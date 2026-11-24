"""Deterministic fault generation, checked end to end against the shared rules."""

import json
import random
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from quality_support import write_snapshot

from streaming.quality.contracts import (
    WRAPPER_FIELDS,
    decode_wrapper_payload,
    sha256_hex,
    validate_manifest,
)
from streaming.quality.rules import (
    RULE_VERSION,
    evaluate_payload,
    select_current_decisions,
)
from streaming.quality_replay import (
    FAULT_TYPES,
    build_replay,
    publish_replay,
)

ROOT = Path(__file__).resolve().parents[1]
NON_DESTRUCTIVE = (
    "duplicate_delivery",
    "supported_unit_variation",
    "numeric_string",
    "equivalent_timezone",
    "delayed_delivery",
)
DESTRUCTIVE = ("missing_value", "negative_value", "malformed_payload", "unsupported_unit")


def make_config(profile="single-fault", faults=(), fraction=0.25, seed=4043, **experiment):
    return {
        "schema_version": 1,
        "enabled": True,
        "rule_version": RULE_VERSION,
        "experiment": {
            "seed": seed,
            "profile": profile,
            "selected_input_fraction": fraction,
            "fault_types": list(faults),
            "group_scenarios": [],
            "topic": "quality_replay_raw",
            **experiment,
        },
    }


def evaluate(payload):
    return evaluate_payload(payload, source_topic="air_quality_raw", rule_version=RULE_VERSION)


def logical(decisions):
    """Final logical result: current observations, independent of delivery order."""
    return {
        d["event_id"]: (
            d["usable"],
            tuple(d["quality_flags"]),
            None if d["value_ug_m3"] is None else round(d["value_ug_m3"], 9),
        )
        for d in select_current_decisions(decisions)
    }


class ReplayCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.dir = Path(self.tmp.name)
        self.snapshot = self.dir / "snapshot.jsonl"
        write_snapshot(self.snapshot)
        self.snapshot_lines = self.snapshot.read_bytes().split(b"\n")[:-1]
        self.baseline = [evaluate(line) for line in self.snapshot_lines]

    def build(self, name="run", **kwargs):
        out = self.dir / name
        summary = build_replay(self.snapshot, out, config=make_config(**kwargs))
        wrappers = [json.loads(line) for line in (out / "replay.jsonl").read_text().splitlines()]
        manifest = json.loads((out / "manifest.json").read_text())
        return summary, wrappers, manifest, out

    def replayed(self, wrappers):
        """Process exactly what a consumer would see: wrapper -> bytes -> rules."""
        return {w["delivery_id"]: evaluate(decode_wrapper_payload(w)) for w in wrappers}


class ControlAndDeterminismTests(ReplayCase):
    def test_control_replays_snapshot_bytes_unchanged(self):
        summary, wrappers, manifest, _ = self.build(profile="control")
        self.assertEqual(summary["fault_counts"], {})
        self.assertEqual(
            [decode_wrapper_payload(w) for w in wrappers], self.snapshot_lines
        )
        validate_manifest(manifest)
        self.assertEqual(manifest["snapshot"]["sha256"], sha256_hex(self.snapshot.read_bytes()))
        self.assertEqual(logical(self.replayed(wrappers).values()), logical(self.baseline))

    def test_same_inputs_give_byte_identical_outputs_and_seed_changes_selection(self):
        faults = ["numeric_string", "equivalent_timezone"]
        a = self.build("a", profile="mixed-v1", faults=faults, fraction=0.3)[3]
        b = self.build("b", profile="mixed-v1", faults=faults, fraction=0.3)[3]
        c = self.build("c", profile="mixed-v1", faults=faults, fraction=0.3, seed=7)[3]
        for name in ("replay.jsonl", "manifest.json"):
            self.assertEqual((a / name).read_bytes(), (b / name).read_bytes())
        self.assertNotEqual((a / "replay.jsonl").read_bytes(), (c / "replay.jsonl").read_bytes())

    def test_outputs_are_never_overwritten_and_snapshot_is_untouched(self):
        before = self.snapshot.read_bytes()
        _, _, _, out = self.build(profile="control")
        self.assertEqual(self.snapshot.read_bytes(), before)
        with self.assertRaises(FileExistsError):
            build_replay(self.snapshot, out, config=make_config(profile="control"))

    def test_invalid_or_unimplemented_configuration_is_rejected(self):
        out = self.dir / "bad"
        cases = {
            "disabled": {**make_config(profile="control"), "enabled": False},
            "group scenarios": make_config("single-fault", ["missing_value"], group_scenarios=["flatline"]),
            "unknown fault": make_config("single-fault", ["nope"]),
            "two single faults": make_config("single-fault", ["missing_value", "numeric_string"]),
            "one mixed fault": make_config("mixed-v1", ["missing_value"]),
            "bad fraction": make_config("control", fraction=1.5),
            "bool seed": make_config("control", seed=True),
            "wrong topic": make_config("control", topic="air_quality_raw"),
        }
        for name, config in cases.items():
            with self.subTest(name), self.assertRaises(ValueError):
                build_replay(self.snapshot, out, config=config)
        self.assertFalse(out.exists())

    def test_publication_is_still_an_explicit_placeholder(self):
        with self.assertRaises(NotImplementedError):
            publish_replay(self.dir / "replay.jsonl", bootstrap_servers="localhost:9092")


class SingleFaultTests(ReplayCase):
    def baseline_event(self, delivery):
        return self.baseline[delivery["source_record_ref"]["record_number"] - 1]["event_id"]

    def test_every_fault_is_handled_as_the_manifest_expects(self):
        for fault in FAULT_TYPES:
            with self.subTest(fault=fault):
                summary, wrappers, manifest, _ = self.build(fault, faults=[fault])
                self.assertGreater(summary["fault_counts"][fault], 0)
                decisions = self.replayed(wrappers)
                for delivery in manifest["deliveries"]:
                    expected = delivery["expected"]
                    if delivery["fault"] is None:
                        continue
                    decision = decisions[delivery["delivery_id"]]
                    self.assertEqual(decision["disposition"], expected["disposition"])
                    if expected["disposition"] == "quarantined":
                        self.assertEqual(decision["reason"], expected["reason"])
                        continue
                    for action in expected.get("added_repair_actions", []):
                        self.assertIn(action, decision["repair_actions"])
                    for flag in expected.get("added_quality_flags", []):
                        self.assertIn(flag, decision["quality_flags"])
                    if expected.get("identity_preserved", True):
                        self.assertEqual(decision["event_id"], self.baseline_event(delivery))

    def test_non_destructive_faults_leave_the_final_logical_result_unchanged(self):
        expected = logical(self.baseline)
        for fault in NON_DESTRUCTIVE:
            with self.subTest(fault=fault):
                _, wrappers, _, _ = self.build(fault, faults=[fault])
                self.assertEqual(logical(self.replayed(wrappers).values()), expected)

    def test_logical_result_is_independent_of_delivery_order(self):
        _, wrappers, _, _ = self.build(
            "mixed", profile="mixed-v1", faults=list(NON_DESTRUCTIVE), fraction=0.5
        )
        decisions = list(self.replayed(wrappers).values())
        expected = logical(decisions)
        for seed in range(5):
            shuffled = decisions[:]
            random.Random(seed).shuffle(shuffled)
            self.assertEqual(logical(shuffled), expected)
        self.assertEqual(expected, logical(self.baseline))

    def test_destructive_faults_change_exactly_the_selected_observations(self):
        before = logical(self.baseline)
        for fault in DESTRUCTIVE:
            with self.subTest(fault=fault):
                _, wrappers, manifest, _ = self.build(fault, faults=[fault])
                touched = {self.baseline_event(d) for d in manifest["deliveries"] if d["fault"]}
                after = logical(self.replayed(wrappers).values())
                differing = {event for event in before if after.get(event) != before[event]}
                self.assertEqual(differing, touched)
                self.assertTrue(touched)
                for event in touched:
                    self.assertFalse(after.get(event, (False,))[0])  # never silently usable

    def test_duplicates_are_distinct_deliveries_with_identical_bytes(self):
        _, wrappers, manifest, _ = self.build("dup", faults=["duplicate_delivery"])
        by_id = {d["delivery_id"]: d for d in manifest["deliveries"]}
        duplicates = [d for d in manifest["deliveries"] if d["role"] == "intentional_duplicate"]
        self.assertTrue(duplicates)
        for duplicate in duplicates:
            primary = by_id[duplicate["duplicate_of"]]
            self.assertNotEqual(duplicate["delivery_id"], primary["delivery_id"])
            self.assertEqual(duplicate["emitted_sha256"], primary["emitted_sha256"])
            self.assertEqual(duplicate["release_index"], primary["release_index"] + 1)
        self.assertEqual(len(wrappers), len(self.snapshot_lines) + len(duplicates))

    def test_delayed_deliveries_are_released_after_all_newer_event_times(self):
        _, _, manifest, _ = self.build("late", faults=["delayed_delivery"])
        delayed = [d for d in manifest["deliveries"] if d["fault"] == "delayed_delivery"]
        self.assertTrue(delayed)
        tail = manifest["deliveries"][-len(delayed):]
        self.assertEqual(tail, delayed)
        self.assertTrue(any(d["mutation"]["released_after_newer_event_times"] for d in delayed))
        # Original collection metadata is untouched: bytes are the baseline bytes.
        for delivery in delayed:
            self.assertEqual(delivery["emitted_sha256"], delivery["original_sha256"])

    def test_unit_variation_stays_within_the_documented_tolerance(self):
        _, wrappers, manifest, _ = self.build("units", faults=["supported_unit_variation"])
        decisions = self.replayed(wrappers)
        checked = 0
        for delivery in manifest["deliveries"]:
            if delivery["fault"]:
                baseline = self.baseline[delivery["source_record_ref"]["record_number"] - 1]
                decision = decisions[delivery["delivery_id"]]
                self.assertEqual(decision["received_unit"], "mg/m3")
                self.assertAlmostEqual(decision["value_ug_m3"], baseline["value_ug_m3"], delta=1e-9)
                self.assertEqual(decision["revision_hash"], decision["claimed_revision_hash"])
                checked += 1
        self.assertGreater(checked, 0)


class MixedProfileTests(ReplayCase):
    def test_mixed_profile_selects_the_configured_fraction_and_balances_faults(self):
        summary, _, manifest, _ = self.build("mixed", profile="mixed-v1", faults=list(FAULT_TYPES), fraction=0.2)
        counts = summary["fault_counts"]
        self.assertEqual(sum(counts.values()), round(0.2 * len(self.snapshot_lines)))
        self.assertEqual(set(counts), set(FAULT_TYPES))
        self.assertLessEqual(max(counts.values()) - min(counts.values()), 2)
        self.assertEqual(manifest["ineligible_selected"], [])

    def test_wrappers_contain_no_fault_labels_values_or_expectations(self):
        _, wrappers, manifest, out = self.build("leak", profile="mixed-v1", faults=list(FAULT_TYPES), fraction=0.5)
        text = (out / "replay.jsonl").read_text()
        for wrapper in wrappers:
            self.assertEqual(set(wrapper), set(WRAPPER_FIELDS))
            self.assertIsNone(wrapper["published_at"])
        for word in (*FAULT_TYPES, "expected", "mutation", "added_repair", "baseline"):
            self.assertNotIn(word, text)
        self.assertIn("duplicate_delivery", json.dumps(manifest))  # labels live only here

    def test_release_schedule_and_delivery_ids_are_stable_and_unique(self):
        _, wrappers, manifest, _ = self.build("ids", profile="mixed-v1", faults=list(FAULT_TYPES), fraction=0.5)
        ids = [w["delivery_id"] for w in wrappers]
        self.assertEqual(len(ids), len(set(ids)))
        self.assertEqual([w["scheduled_release_index"] for w in wrappers], list(range(len(wrappers))))
        self.assertEqual([d["delivery_id"] for d in manifest["deliveries"]], ids)


class NaturalDefectTests(unittest.TestCase):
    def test_natural_defects_pass_through_and_are_not_selected_for_mutation(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp = Path(tmp)
            clean = tmp / "clean.jsonl"
            write_snapshot(clean, stations=1, hours=10)
            lines = clean.read_bytes().split(b"\n")[:-1]
            noisy = [b"{broken", b"", *lines[:5], b"   ", lines[5].replace(b"\n", b"") + b"\r", *lines[6:]]
            snapshot = tmp / "noisy.jsonl"
            snapshot.write_bytes(b"\n".join(noisy) + b"\n")
            out = tmp / "out"
            summary = build_replay(snapshot, out, config=make_config("mixed-v1", list(FAULT_TYPES), fraction=1.0))
            manifest = json.loads((out / "manifest.json").read_text())
            self.assertEqual(manifest["input_record_count"], 11)  # blank lines are not deliveries
            broken = manifest["deliveries"][0]
            self.assertIsNone(broken["fault"])
            self.assertEqual(broken["source_record_ref"]["record_number"], 1)
            self.assertEqual(broken["emitted_sha256"], sha256_hex(b"{broken"))
            self.assertGreater(summary["emitted_deliveries"], 0)


class CommandLineTests(unittest.TestCase):
    def run_cli(self, *args, cwd=ROOT):
        return subprocess.run(
            [sys.executable, "-m", "streaming.quality_replay", *args],
            cwd=cwd, capture_output=True, text=True,
        )

    def test_build_command_writes_a_run_and_reports_a_summary(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp = Path(tmp)
            write_snapshot(tmp / "s.jsonl")
            (tmp / "config.json").write_text(json.dumps(make_config("single-fault", ["numeric_string"])))
            result = self.run_cli(
                "build", "--config", str(tmp / "config.json"),
                "--snapshot", str(tmp / "s.jsonl"), "--output-dir", str(tmp / "run"),
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            summary = json.loads(result.stdout)
            self.assertGreater(summary["fault_counts"]["numeric_string"], 0)
            manifest = json.loads((tmp / "run" / "manifest.json").read_text())
            self.assertRegex(manifest["code_revision"] or "0" * 40, r"^[0-9a-f]{40}$")

    def test_inactive_example_config_cannot_run(self):
        with tempfile.TemporaryDirectory() as tmp:
            write_snapshot(Path(tmp) / "s.jsonl")
            result = self.run_cli(
                "build", "--config", str(ROOT / "config" / "quality.example.json"),
                "--snapshot", str(Path(tmp) / "s.jsonl"), "--output-dir", str(Path(tmp) / "run"),
            )
            self.assertEqual(result.returncode, 2)
            self.assertIn("not enabled", result.stderr)
            self.assertFalse((Path(tmp) / "run").exists())


if __name__ == "__main__":
    unittest.main()
