"""Group scenarios: missing intervals and corrections to null."""

import json
import tempfile
import unittest
from pathlib import Path

from quality_support import envelope, write_snapshot
from test_quality_replay import evaluate, logical, make_config

from streaming.quality.contracts import decode_wrapper_payload
from streaming.quality_replay import FAULT_TYPES, build_replay


class ScenarioCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.dir = Path(self.tmp.name)
        self.snapshot = self.dir / "snapshot.jsonl"
        write_snapshot(self.snapshot)
        self.lines = self.snapshot.read_bytes().split(b"\n")[:-1]
        self.baseline = [evaluate(line) for line in self.lines]

    def build(self, name="run", scenarios=("missing_interval", "correction_to_null"), **kwargs):
        config = make_config(group_scenarios=list(scenarios), **kwargs)
        out = self.dir / name
        summary = build_replay(self.snapshot, out, config=config)
        wrappers = [json.loads(line) for line in (out / "replay.jsonl").read_text().splitlines()]
        manifest = json.loads((out / "manifest.json").read_text())
        decisions = {w["delivery_id"]: evaluate(decode_wrapper_payload(w)) for w in wrappers}
        return summary, wrappers, manifest, decisions

    def event(self, record_number):
        return self.baseline[record_number - 1]["event_id"]


class MissingIntervalTests(ScenarioCase):
    def test_every_delivery_of_a_selected_group_is_omitted_and_recorded(self):
        summary, wrappers, manifest, _ = self.build(scenarios=["missing_interval"], faults=["numeric_string"], group_fraction=0.2)
        omitted = manifest["omitted_records"]
        self.assertEqual(len(omitted), summary["omitted_groups"])
        self.assertEqual(round(0.2 * len(self.lines)), len(omitted))  # one record per group here
        emitted = {d["source_record_ref"]["record_number"] for d in manifest["deliveries"]}
        for group in omitted:
            self.assertEqual(group["scenario"], "missing_interval")
            for number in group["record_numbers"]:
                self.assertNotIn(number, emitted)
        self.assertEqual(len(wrappers) + summary["omitted_records"], len(self.lines))
        self.assertEqual(manifest["config"]["group_scenarios"], ["missing_interval"])

    def test_a_group_with_a_revision_loses_all_its_deliveries(self):
        original = self.lines[0]
        revision = json.loads(original)
        revision.update(collected_at="2026-01-01T09:00:00Z", ingested_at="2026-01-01T09:01:00Z")
        # The same hour delivered twice: the original and a (hash-inconsistent) revision.
        snapshot = self.dir / "two.jsonl"
        snapshot.write_bytes(original + b"\n" + json.dumps(revision).encode() + b"\n")
        out = self.dir / "two"
        build_replay(snapshot, out, config=make_config(faults=["numeric_string"], fraction=0, group_scenarios=["missing_interval"], group_fraction=1.0))
        manifest = json.loads((out / "manifest.json").read_text())
        self.assertEqual(manifest["omitted_records"][0]["record_numbers"], [1, 2])
        self.assertEqual(manifest["deliveries"], [])
        self.assertEqual((out / "replay.jsonl").read_text(), "")

    def test_selection_is_deterministic_and_seed_dependent(self):
        a = self.build("a", scenarios=["missing_interval"], profile="mixed-v1", faults=["numeric_string", "missing_value"], group_fraction=0.2)[2]
        b = self.build("b", scenarios=["missing_interval"], profile="mixed-v1", faults=["numeric_string", "missing_value"], group_fraction=0.2)[2]
        c = self.build("c", scenarios=["missing_interval"], profile="mixed-v1", faults=["numeric_string", "missing_value"], group_fraction=0.2, seed=7)[2]
        self.assertEqual(a["omitted_records"], b["omitted_records"])
        self.assertNotEqual(a["omitted_records"], c["omitted_records"])

    def test_omitted_groups_are_reserved_from_row_faults(self):
        _, _, manifest, _ = self.build(scenarios=["missing_interval"], profile="mixed-v1", faults=list(FAULT_TYPES), fraction=1.0, group_fraction=0.25)
        omitted = {n for g in manifest["omitted_records"] for n in g["record_numbers"]}
        self.assertTrue(omitted)
        self.assertFalse(omitted & {d["source_record_ref"]["record_number"] for d in manifest["deliveries"]})
        self.assertEqual(manifest["ineligible_selected"], [])

    def test_group_fraction_zero_omits_nothing(self):
        _, wrappers, manifest, _ = self.build(scenarios=["missing_interval"], profile="single-fault", faults=["numeric_string"], group_fraction=0.0)
        self.assertEqual(manifest["omitted_records"], [])
        self.assertEqual(len(wrappers), len(self.lines))


class CorrectionToNullTests(ScenarioCase):
    def test_correction_is_a_later_revision_that_removes_the_usable_value(self):
        summary, wrappers, manifest, decisions = self.build(scenarios=["correction_to_null"], profile="single-fault", faults=["numeric_string"], group_fraction=0.2)
        corrections = [d for d in manifest["deliveries"] if d["role"] == "correction"]
        self.assertTrue(corrections)
        self.assertEqual(summary["fault_counts"]["correction_to_null"], len(corrections))
        # Released after everything it corrects.
        self.assertEqual(manifest["deliveries"][-len(corrections):], corrections)
        primaries = {d["delivery_id"]: d for d in manifest["deliveries"]}
        for correction in corrections:
            original = self.baseline[correction["source_record_ref"]["record_number"] - 1]
            decision = decisions[correction["delivery_id"]]
            self.assertEqual(decision["event_id"], original["event_id"])
            self.assertEqual(decision["quality_flags"], ["missing_value"])
            self.assertGreater(decision["collected_at"], original["collected_at"])
            self.assertEqual(decision["revision_hash"], decision["claimed_revision_hash"])
            self.assertNotEqual(decision["revision_hash"], original["revision_hash"])
            self.assertIn(correction["expected"]["supersedes"], primaries)
            self.assertIsNone(correction["duplicate_of"])
        final = logical(decisions.values())
        for correction in corrections:
            self.assertFalse(final[self.event(correction["source_record_ref"]["record_number"])][0])

    def test_final_result_does_not_depend_on_delivery_order(self):
        _, _, _, decisions = self.build(scenarios=["correction_to_null"], profile="single-fault", faults=["numeric_string"], group_fraction=0.3)
        values = list(decisions.values())
        self.assertEqual(logical(values), logical(values[::-1]))

    def test_corrected_groups_are_reserved_and_never_share_a_group_with_a_gap(self):
        _, _, manifest, _ = self.build(profile="mixed-v1", faults=list(FAULT_TYPES), fraction=1.0, group_fraction=0.25)
        corrected = {d["source_record_ref"]["record_number"] for d in manifest["deliveries"] if d["role"] == "correction"}
        omitted = {n for g in manifest["omitted_records"] for n in g["record_numbers"]}
        self.assertTrue(corrected and omitted)
        self.assertFalse(corrected & omitted)
        for delivery in manifest["deliveries"]:
            if delivery["source_record_ref"]["record_number"] in corrected and delivery["role"] != "correction":
                self.assertIsNone(delivery["fault"])  # no compound fault on a corrected group

    def test_values_that_are_already_unusable_are_not_selected(self):
        lines = [json.dumps(envelope(hour=hour, value=value)).encode() for hour, value in enumerate((None, -3, 12))]
        snapshot = self.dir / "mixed.jsonl"
        snapshot.write_bytes(b"\n".join(lines) + b"\n")
        out = self.dir / "mixed"
        build_replay(snapshot, out, config=make_config(faults=["numeric_string"], fraction=0, group_scenarios=["correction_to_null"], group_fraction=1.0))
        manifest = json.loads((out / "manifest.json").read_text())
        corrected = [d for d in manifest["deliveries"] if d["role"] == "correction"]
        self.assertEqual([d["source_record_ref"]["record_number"] for d in corrected], [3])


class ConfigurationTests(ScenarioCase):
    def test_flatline_unknown_duplicate_and_control_combinations_are_rejected(self):
        out = self.dir / "bad"
        cases = {
            "flatline": make_config("single-fault", ["numeric_string"], group_scenarios=["flatline"]),
            "unknown": make_config("single-fault", ["numeric_string"], group_scenarios=["nope"]),
            "repeated": make_config("single-fault", ["numeric_string"], group_scenarios=["missing_interval", "missing_interval"]),
            "control": make_config("control", group_scenarios=["missing_interval"]),
            "fraction": make_config("single-fault", ["numeric_string"], group_scenarios=["missing_interval"], group_fraction=2),
            "bool fraction": make_config("single-fault", ["numeric_string"], group_scenarios=["missing_interval"], group_fraction=True),
        }
        for name, config in cases.items():
            with self.subTest(name), self.assertRaises(ValueError):
                build_replay(self.snapshot, out, config=config)
        self.assertFalse(out.exists())

    def test_wrappers_still_carry_no_scenario_labels(self):
        self.build(profile="mixed-v1", faults=list(FAULT_TYPES), fraction=0.5, group_fraction=0.2)
        text = (self.dir / "run" / "replay.jsonl").read_text()
        for word in ("missing_interval", "correction_to_null", "correction", "omitted", "supersedes"):
            self.assertNotIn(word, text)


if __name__ == "__main__":
    unittest.main()
