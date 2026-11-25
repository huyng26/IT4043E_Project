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


class InjectorVersionTests(unittest.TestCase):
    """injector-v2 also starts unit faults/corrections from real OpenAQ spellings."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.dir = Path(self.tmp.name)
        self.snapshot = self.dir / "openaq.jsonl"
        rows = [
            envelope(station=s, sensor=s, hour=h, value=10 + s + h, unit=unit)
            for s, unit in ((1, "µg/m³"), (2, "μg/m³"), (3, "µg/m³"))
            for h in range(12)
        ]
        self.snapshot.write_text("".join(json.dumps(r) + "\n" for r in rows))

    def build(self, name, version=None, **config):
        experiment = {} if version is None else {"injector_version": version}
        out = self.dir / name
        summary = build_replay(self.snapshot, out, config=make_config(**config, **experiment))
        manifest = json.loads((out / "manifest.json").read_text())
        return summary, manifest

    def test_v1_finds_nothing_to_corrupt_in_alias_units_and_v2_does(self):
        for fault in ("supported_unit_variation", "unsupported_unit"):
            with self.subTest(fault=fault):
                v1, _ = self.build(f"{fault}-v1", "injector-v1", faults=[fault], fraction=0.5)
                v2, manifest = self.build(f"{fault}-v2", "injector-v2", faults=[fault], fraction=0.5)
                self.assertEqual(v1["fault_counts"], {})
                self.assertEqual(v2["fault_counts"], {fault: 18})
                self.assertEqual(manifest["injector_version"], "injector-v2")

    def test_v2_corrections_reach_alias_unit_groups(self):
        args = dict(faults=["numeric_string"], fraction=0, group_scenarios=["correction_to_null"], group_fraction=0.25)
        v1, _ = self.build("corr-v1", "injector-v1", **args)
        v2, _ = self.build("corr-v2", "injector-v2", **args)
        self.assertEqual(v1["fault_counts"], {})
        self.assertEqual(v2["fault_counts"], {"correction_to_null": 9})

    def test_v2_alias_faults_are_handled_as_the_manifest_expects(self):
        _, manifest = self.build("mixed", "injector-v2", profile="mixed-v1",
                                 faults=["supported_unit_variation", "unsupported_unit"], fraction=1.0)
        replay = (self.dir / "mixed" / "replay.jsonl").read_text().splitlines()
        wrappers = {json.loads(l)["delivery_id"]: json.loads(l) for l in replay}
        for delivery in manifest["deliveries"]:
            if delivery["fault"] is None:
                continue
            decision = evaluate(decode_wrapper_payload(wrappers[delivery["delivery_id"]]))
            expected = delivery["expected"]
            self.assertEqual(decision["disposition"], "accepted")
            self.assertTrue(set(expected["added_repair_actions"]) <= set(decision["repair_actions"]))
            self.assertTrue(set(expected["added_quality_flags"]) <= set(decision["quality_flags"]))

    def test_missing_version_means_v1_and_unknown_versions_are_refused(self):
        default, manifest = self.build("default", faults=["unsupported_unit"], fraction=0.5)
        self.assertEqual(manifest["injector_version"], "injector-v1")
        self.assertEqual(default["fault_counts"], {})
        v1, _ = self.build("explicit", "injector-v1", faults=["unsupported_unit"], fraction=0.5)
        self.assertEqual(default["run_id"], v1["run_id"])  # v1 runs stay reproducible
        v2, _ = self.build("v2", "injector-v2", faults=["unsupported_unit"], fraction=0.5)
        self.assertNotEqual(v1["run_id"], v2["run_id"])
        with self.assertRaises(ValueError):
            self.build("bad", "injector-v3", faults=["unsupported_unit"])
