"""Evaluator: scoring, accounting, and that it fails when the processor is wrong."""

import ast
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from quality_support import encode, envelope, write_catalog, write_snapshot
from test_quality_replay import make_config

from streaming.quality.local_run import (
    load_results,
    process_replay,
    process_snapshot,
    save_results,
)
from streaming.quality.rules import RULE_VERSION
from streaming.quality_evaluation import evaluate_run
from streaming.quality_replay import FAULT_TYPES, build_replay

ROOT = Path(__file__).resolve().parents[1]
DESTRUCTIVE = ("missing_value", "negative_value", "malformed_payload", "unsupported_unit")
PIPELINE = (  # processing code that must never see answers
    "rules.py", "local_run.py", "coverage.py", "profiling.py", "contracts.py", "dispatcher.py",
)


class EvalCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.dir = Path(self.tmp.name)
        self.snapshot = self.dir / "snapshot.jsonl"
        self.catalog = self.dir / "catalog.json"
        write_snapshot(self.snapshot)
        write_catalog(self.catalog, stations=3)
        self.count = 0

    def prepare(self, name="run", snapshot=None, **config):
        snapshot = snapshot or self.snapshot
        config.setdefault("catalog_path", str(self.catalog))
        out = self.dir / name
        build_replay(snapshot, out, config=make_config(**config))
        save_results(process_snapshot(snapshot, rule_version=RULE_VERSION), out / "baseline.json")
        save_results(process_replay(out / "replay.jsonl", rule_version=RULE_VERSION), out / "experiment.json")
        return {
            "manifest": out / "manifest.json",
            "baseline": out / "baseline.json",
            "experiment": out / "experiment.json",
            "out": out,
        }

    def evaluate(self, run, *, catalog=True, **overrides):
        self.count += 1
        return evaluate_run(
            manifest_path=overrides.get("manifest", run["manifest"]),
            baseline_results_path=overrides.get("baseline", run["baseline"]),
            experiment_results_path=overrides.get("experiment", run["experiment"]),
            output_directory=self.dir / f"report-{self.count}",
            catalog_path=self.catalog if catalog else None,
        )

    def tamper(self, path, edit, name="tampered.json"):
        """Copy a results file after letting ``edit(rows, doc)`` change its decisions."""
        document = json.loads(Path(path).read_text())
        edit({row["key"]: row["decision"] for row in document["decisions"]}, document)
        target = self.dir / f"{self.count}-{name}"
        target.write_text(json.dumps(document))
        return target

    @staticmethod
    def labeled(run, fault):
        manifest = json.loads(run["manifest"].read_text())
        return [d for d in manifest["deliveries"] if d["fault"] == fault]


class ScoringTests(EvalCase):
    def test_control_run_passes_and_nothing_is_lost_or_flagged(self):
        report = self.evaluate(self.prepare(profile="control"))
        self.assertTrue(report["passed"], report["checks"])
        self.assertEqual(report["detection"], {})
        self.assertEqual(report["valid_control"]["false_positives"], 0)
        self.assertEqual(report["valid_control"]["clean_unlabeled_deliveries"], 72)
        self.assertEqual(report["coverage"]["lost_usable_sensor_hours"], 0)
        self.assertIsNone(report["repair_correctness"]["overall"])  # nothing to repair
        self.assertEqual(report["coverage"]["experiment"]["completeness"], 1.0)

    def test_every_single_fault_is_scored_and_its_losses_are_exactly_its_own(self):
        for fault in FAULT_TYPES:
            with self.subTest(fault=fault):
                run = self.prepare(fault, faults=[fault])
                report = self.evaluate(run)
                labeled = len(self.labeled(run, fault))
                self.assertTrue(report["passed"], report["checks"])
                entry = report["detection"][fault]
                self.assertEqual(entry["labeled"], labeled)
                if fault == "delayed_delivery":
                    self.assertEqual(entry["detection"], "not_applicable")
                else:
                    self.assertEqual((entry["precision"], entry["recall"]), (1.0, 1.0))
                    self.assertEqual(entry["true_positive"], labeled)
                lost = report["coverage"]["lost_usable_sensor_hours"]
                self.assertEqual(lost, labeled if fault in DESTRUCTIVE else 0)
                self.assertEqual(report["coverage"]["unexplained_loss_count"], 0)

    def test_repairs_are_scored_against_the_baseline_value(self):
        run = self.prepare(profile="mixed-v1", faults=["numeric_string", "supported_unit_variation", "equivalent_timezone"], fraction=0.5)
        repairs = self.evaluate(run)["repair_correctness"]
        self.assertEqual(repairs["overall"], 1.0)
        self.assertEqual(set(repairs["by_fault"]), {"numeric_string", "supported_unit_variation", "equivalent_timezone"})
        self.assertEqual(repairs["tolerance_ug_m3"], 1e-9)
        self.assertTrue(all(r["wrong"] == 0 and r["not_repaired"] == 0 for r in repairs["by_fault"].values()))

    def test_mixed_run_with_group_scenarios_explains_every_lost_sensor_hour(self):
        run = self.prepare(
            profile="mixed-v1", faults=list(FAULT_TYPES), fraction=0.4,
            group_scenarios=["missing_interval", "correction_to_null"], group_fraction=0.1,
        )
        manifest = json.loads(run["manifest"].read_text())
        report = self.evaluate(run)
        self.assertTrue(report["passed"], report["checks"])
        removed = sum(1 for d in manifest["deliveries"] if (d["expected"] or {}).get("added_quality_flags")
                      or (d["expected"] or {}).get("disposition") == "quarantined")
        removed += sum(len(o["record_numbers"]) for o in manifest["omitted_records"])
        coverage = report["coverage"]
        self.assertEqual(coverage["lost_usable_sensor_hours"], removed)
        self.assertEqual(sum(coverage["lost_by_fault"].values()), removed)
        self.assertEqual(coverage["usable_sensor_hours_experiment"], 72 - removed)
        self.assertEqual(coverage["experiment"]["completeness"], (72 - removed) / 72)
        gap = report["gap_detection"]
        self.assertEqual((gap["precision"], gap["recall"]), (1.0, 1.0))
        self.assertEqual(gap["true_positive"], len(manifest["omitted_records"]))
        self.assertEqual(report["logical_invariance"]["corrections"]["effective"],
                         len(self.labeled(run, "correction_to_null")))
        self.assertEqual(report["aggregate_effect"]["recoverable_mismatches"], 0)
        self.assertTrue(report["logical_invariance"]["order_invariant"])

    def test_accounting_matches_planned_deliveries(self):
        run = self.prepare(profile="mixed-v1", faults=list(FAULT_TYPES), fraction=0.5)
        accounting = self.evaluate(run)["accounting"]
        manifest = json.loads(run["manifest"].read_text())
        self.assertTrue(accounting["balanced"])
        self.assertEqual(accounting["planned_emitted"], manifest["emitted_delivery_count"])
        self.assertEqual(accounting["accepted"] + accounting["quarantined"], accounting["planned_emitted"])
        self.assertEqual(accounting["quarantined"], len(self.labeled(run, "malformed_payload")))

    def test_gap_detection_needs_a_catalog_and_says_so(self):
        run = self.prepare(faults=["missing_value"], group_scenarios=["missing_interval"], group_fraction=0.1)
        report = self.evaluate(run, catalog=False)
        self.assertEqual(report["gap_detection"]["status"], "not_assessed")
        self.assertEqual(report["coverage"]["expected_source"], "baseline_observations")
        self.assertGreater(report["gap_detection"]["labeled_groups"], 0)

    def test_naturally_absent_sensor_hours_are_not_false_gap_detections(self):
        write_catalog(self.catalog, stations=4)  # a fourth sensor never reported
        run = self.prepare(faults=["numeric_string"], group_scenarios=["missing_interval"], group_fraction=0.1)
        report = self.evaluate(run)
        gap = report["gap_detection"]
        self.assertEqual(gap["natural_missing_sensor_hours"], 24)
        self.assertEqual(gap["false_positive"], 0)
        self.assertEqual(gap["recall"], 1.0)
        self.assertEqual(report["coverage"]["baseline"]["completeness"], 0.75)

    def test_natural_defects_are_reported_separately_and_never_scored(self):
        lines = [
            encode(envelope(hour=0)), encode(envelope(hour=0)),  # natural repeat
            encode(envelope(hour=1, value=-4)),  # natural flag
            b"{truncated",  # natural quarantine
            *[encode(envelope(hour=h)) for h in range(2, 12)],
        ]
        snapshot = self.dir / "noisy.jsonl"
        snapshot.write_bytes(b"\n".join(lines) + b"\n")
        run = self.prepare("noisy", snapshot=snapshot, profile="mixed-v1", faults=["numeric_string", "missing_value"], fraction=1.0, catalog_path=None)
        report = self.evaluate(run, catalog=False)
        self.assertTrue(report["passed"], report["checks"])
        natural = report["natural_defects"]
        self.assertEqual(natural["quarantined"], 1)
        self.assertEqual(natural["repeat_deliveries"], 1)
        self.assertEqual(natural["quality_flags"], {"negative_concentration": 1})
        self.assertEqual(report["valid_control"]["false_positives"], 0)
        self.assertLess(report["valid_control"]["clean_unlabeled_deliveries"], report["valid_control"]["unlabeled_deliveries"] + 1)

    def test_report_is_deterministic_and_never_overwrites(self):
        run = self.prepare(profile="mixed-v1", faults=list(FAULT_TYPES), fraction=0.4)
        first = self.evaluate(run)
        second = self.evaluate(run)
        self.assertEqual(first, second)
        self.assertEqual(
            (self.dir / "report-1" / "evaluation.json").read_bytes(),
            (self.dir / "report-2" / "evaluation.json").read_bytes(),
        )
        with self.assertRaises(FileExistsError):
            evaluate_run(
                manifest_path=run["manifest"], baseline_results_path=run["baseline"],
                experiment_results_path=run["experiment"], output_directory=self.dir / "report-1",
            )
        json.dumps(first, allow_nan=False)
        self.assertIn("timeliness", first["not_assessed"])


class BrokenProcessorTests(EvalCase):
    """The evaluator must fail when the processing result is wrong."""

    def first(self, run, fault):
        return self.labeled(run, fault)[0]["delivery_id"]

    def test_a_wrong_repair_is_counted_and_fails_the_run(self):
        run = self.prepare(faults=["numeric_string"])
        victim = self.first(run, "numeric_string")
        broken = self.tamper(run["experiment"], lambda rows, _: rows[victim].update(
            value_ug_m3=rows[victim]["value_ug_m3"] + 1))
        report = self.evaluate(run, experiment=broken)
        self.assertFalse(report["passed"])
        self.assertEqual(report["repair_correctness"]["by_fault"]["numeric_string"]["wrong"], 1)
        self.assertFalse(report["checks"]["all_repairs_correct"])
        self.assertEqual(report["examples"]["wrong_repairs"][0]["delivery_id"], victim)

    def test_a_missing_repair_action_lowers_recall_and_leaves_precision_undefined(self):
        run = self.prepare(faults=["numeric_string"], fraction=0.1)
        ids = {d["delivery_id"] for d in self.labeled(run, "numeric_string")}

        def drop(rows, _):
            for key in ids:
                rows[key]["repair_actions"] = []

        report = self.evaluate(run, experiment=self.tamper(run["experiment"], drop))
        entry = report["detection"]["numeric_string"]
        self.assertEqual((entry["true_positive"], entry["false_negative"]), (0, len(ids)))
        self.assertEqual(entry["recall"], 0.0)
        self.assertIsNone(entry["precision"])  # no positive calls: undefined, not 1.0
        self.assertEqual(report["repair_correctness"]["by_fault"]["numeric_string"]["not_repaired"], len(ids))
        self.assertFalse(report["passed"])

    def test_flagging_a_clean_delivery_is_a_false_positive(self):
        run = self.prepare(profile="control")

        def flag(rows, _):
            victim = next(iter(rows.values()))
            victim.update(quality_flags=["unsupported_unit"], usable=False)

        report = self.evaluate(run, experiment=self.tamper(run["experiment"], flag))
        self.assertEqual(report["valid_control"]["false_positives"], 1)
        self.assertEqual(report["valid_control"]["false_positive_rate"], 1 / 72)
        self.assertFalse(report["checks"]["no_false_positives_on_clean_deliveries"])
        self.assertFalse(report["checks"]["unlabeled_deliveries_match_baseline"])
        self.assertEqual(report["coverage"]["lost_usable_sensor_hours"], 1)
        self.assertEqual(report["coverage"]["unexplained_loss_count"], 1)  # nobody caused it
        self.assertFalse(report["passed"])

    def test_a_flag_on_an_unlabeled_delivery_counts_against_that_fault_type(self):
        run = self.prepare(faults=["unsupported_unit"], fraction=0.1)
        labeled = {d["delivery_id"] for d in self.labeled(run, "unsupported_unit")}
        manifest = json.loads(run["manifest"].read_text())
        bystander = next(d["delivery_id"] for d in manifest["deliveries"] if d["delivery_id"] not in labeled)

        def flag(rows, _):
            rows[bystander].update(quality_flags=["unsupported_unit"], usable=False)

        report = self.evaluate(run, experiment=self.tamper(run["experiment"], flag))
        entry = report["detection"]["unsupported_unit"]
        self.assertEqual(entry["false_positive"], 1)
        self.assertLess(entry["precision"], 1.0)
        self.assertEqual(entry["recall"], 1.0)

    def test_a_destructive_fault_left_usable_is_caught(self):
        run = self.prepare(faults=["missing_value"], fraction=0.1)
        victim = self.first(run, "missing_value")
        number = next(d["source_record_ref"]["record_number"] for d in self.labeled(run, "missing_value"))
        baseline = {r["key"]: r["decision"] for r in json.loads(run["baseline"].read_text())["decisions"]}

        def restore(rows, _):
            rows[victim].clear()
            rows[victim].update(baseline[number])

        report = self.evaluate(run, experiment=self.tamper(run["experiment"], restore))
        self.assertEqual(report["logical_invariance"]["silently_usable_after_destructive_fault"], 1)
        self.assertFalse(report["checks"]["destructive_faults_never_silently_usable"])
        self.assertLess(report["detection"]["missing_value"]["recall"], 1.0)
        self.assertEqual(report["logical_invariance"]["expected_change_not_observed"], 1)
        self.assertFalse(report["passed"])

    def test_an_ignored_correction_is_reported(self):
        run = self.prepare(faults=["numeric_string"], fraction=0, group_scenarios=["correction_to_null"], group_fraction=0.1)
        correction = self.labeled(run, "correction_to_null")[0]
        number = correction["source_record_ref"]["record_number"]
        baseline = {r["key"]: r["decision"] for r in json.loads(run["baseline"].read_text())["decisions"]}

        def ignore(rows, _):
            rows[correction["delivery_id"]].clear()
            rows[correction["delivery_id"]].update(baseline[number])

        report = self.evaluate(run, experiment=self.tamper(run["experiment"], ignore))
        corrections = report["logical_invariance"]["corrections"]
        self.assertLess(corrections["effective"], corrections["total"])
        self.assertFalse(report["checks"]["corrections_effective"])
        self.assertFalse(report["passed"])

    def test_an_unexpected_value_change_is_a_logical_and_aggregate_failure(self):
        run = self.prepare(faults=["numeric_string"], fraction=0.1)
        manifest = json.loads(run["manifest"].read_text())
        victim = next(d["delivery_id"] for d in manifest["deliveries"] if d["fault"] is None)
        broken = self.tamper(run["experiment"], lambda rows, _: rows[victim].update(
            value_ug_m3=rows[victim]["value_ug_m3"] + 5))
        report = self.evaluate(run, experiment=broken)
        self.assertEqual(report["logical_invariance"]["unexpected_change_count"], 1)
        self.assertEqual(report["aggregate_effect"]["recoverable_mismatches"], 1)
        self.assertFalse(report["passed"])

    def test_missing_and_extra_decisions_unbalance_the_accounting(self):
        run = self.prepare(faults=["numeric_string"])
        manifest = json.loads(run["manifest"].read_text())
        dropped = manifest["deliveries"][0]["delivery_id"]

        def remove(rows, document):
            document["decisions"] = [r for r in document["decisions"] if r["key"] != dropped]

        report = self.evaluate(run, experiment=self.tamper(run["experiment"], remove))
        self.assertFalse(report["accounting"]["balanced"])
        self.assertEqual(report["accounting"]["missing_decisions"], [dropped])
        self.assertFalse(report["passed"])

        def add(rows, document):
            document["decisions"].append({"key": "ghost", "decision": document["decisions"][0]["decision"]})

        report = self.evaluate(run, experiment=self.tamper(run["experiment"], add, "extra.json"))
        self.assertEqual(report["accounting"]["unexpected_decisions"], ["ghost"])
        self.assertFalse(report["accounting"]["balanced"])


class IncompatibleInputTests(EvalCase):
    def setUp(self):
        super().setUp()
        self.run = self.prepare(faults=["numeric_string"])

    def test_results_that_do_not_belong_together_are_rejected(self):
        def rule(_, document):
            document["rule_version"] = "rules-v0"

        def snapshot(_, document):
            document["input"]["sha256"] = "0" * 64

        def other_run(_, document):
            document["run_id"] = "someone-elses-run"

        def short(_, document):
            document["decisions"] = document["decisions"][:5]

        for name, key, edit in (
            ("rule version", "baseline", rule),
            ("snapshot", "baseline", snapshot),
            ("other run", "experiment", other_run),
            ("missing records", "baseline", short),
        ):
            with self.subTest(name), self.assertRaises(ValueError):
                self.evaluate(self.run, **{key: self.tamper(self.run[key], edit, f"{key}.json")})

    def test_kind_swap_and_catalog_mismatch_are_rejected(self):
        with self.assertRaises(ValueError):
            self.evaluate(self.run, baseline=self.run["experiment"])
        with self.assertRaises(ValueError):
            load_results(self.run["baseline"], kind="experiment")
        write_catalog(self.catalog, stations=5)  # different from the frozen catalog
        with self.assertRaises(ValueError):
            self.evaluate(self.run)


class ProcessingIsolationTests(EvalCase):
    def test_processing_modules_never_import_the_evaluator_or_the_injector(self):
        for name in PIPELINE:
            tree = ast.parse((ROOT / "streaming" / "quality" / name).read_text())
            imported = {
                alias.name for node in ast.walk(tree) if isinstance(node, ast.Import) for alias in node.names
            } | {node.module or "" for node in ast.walk(tree) if isinstance(node, ast.ImportFrom)}
            for module in imported:
                self.assertNotIn("quality_evaluation", module, name)
                self.assertNotIn("quality_replay", module, name)

    def test_saved_processing_results_contain_no_fault_labels_or_expectations(self):
        run = self.prepare(profile="mixed-v1", faults=list(FAULT_TYPES), fraction=0.5,
                           group_scenarios=["missing_interval", "correction_to_null"])
        for key in ("baseline", "experiment"):
            text = run[key].read_text()
            # Exact quoted labels: "numeric_string_parsed" is a legitimate repair action,
            # and "missing_value"/"unsupported_unit" are legitimate quality-flag names.
            injector_only = set(FAULT_TYPES) - {"missing_value", "unsupported_unit"}
            for word in (*sorted(injector_only), "missing_interval", "correction_to_null", "expected"):
                self.assertNotIn(f'"{word}"', text, key)
            self.assertNotIn("added_repair", text, key)


class LocalRunTests(EvalCase):
    def test_invalid_wrappers_duplicates_and_clobbering_are_refused(self):
        run = self.prepare(profile="control")
        replay = run["out"] / "replay.jsonl"
        bad = self.dir / "bad.jsonl"
        bad.write_text(replay.read_text() + '{"not": "a wrapper"}\n')
        with self.assertRaisesRegex(ValueError, "not a valid wrapper"):
            process_replay(bad, rule_version=RULE_VERSION)
        first = replay.read_text().splitlines()[0]
        doubled = self.dir / "doubled.jsonl"
        doubled.write_text(first + "\n" + first + "\n")
        with self.assertRaisesRegex(ValueError, "duplicate delivery"):
            process_replay(doubled, rule_version=RULE_VERSION)
        with self.assertRaises(FileExistsError):
            save_results(process_snapshot(self.snapshot, rule_version=RULE_VERSION), run["baseline"])
        with self.assertRaises(ValueError):
            process_snapshot(self.snapshot, rule_version="rules-v0")

    def test_malformed_results_files_are_rejected(self):
        run = self.prepare(profile="control")
        document = json.loads(run["baseline"].read_text())
        cases = {
            "not json": "{",
            "schema": json.dumps({**document, "schema_version": 9}),
            "dup key": json.dumps({**document, "decisions": document["decisions"][:1] * 2}),
            "row shape": json.dumps({**document, "decisions": [{"key": 1}]}),
            "bad decision": json.dumps({**document, "decisions": [{"key": 1, "decision": {}}]}),
        }
        for name, text in cases.items():
            path = self.dir / f"{name}.json"
            path.write_text(text)
            with self.subTest(name), self.assertRaises(ValueError):
                load_results(path, kind="baseline")


class CommandLineTests(EvalCase):
    def cli(self, *args):
        return subprocess.run(
            [sys.executable, "-m", "streaming.quality_evaluation", *args],
            cwd=ROOT, capture_output=True, text=True,
        )

    def test_run_command_processes_evaluates_and_exits_zero(self):
        out = self.dir / "cli"
        build_replay(self.snapshot, out, config=make_config(
            profile="mixed-v1", faults=list(FAULT_TYPES), fraction=0.4, catalog_path=str(self.catalog)))
        result = self.cli("run", "--run-dir", str(out), "--snapshot", str(self.snapshot), "--catalog", str(self.catalog))
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertTrue(json.loads(result.stdout)["passed"])
        for name in ("baseline_results.json", "experiment_results.json", "evaluation/evaluation.json"):
            self.assertTrue((out / name).exists(), name)
        again = self.cli("run", "--run-dir", str(out), "--snapshot", str(self.snapshot))
        self.assertEqual(again.returncode, 2)  # never overwrites saved results

    def test_stepwise_commands_exit_one_when_a_check_fails_and_two_on_errors(self):
        run = self.prepare(faults=["numeric_string"])
        broken = self.tamper(
            run["experiment"],
            lambda rows, doc: rows[self.labeled(run, "numeric_string")[0]["delivery_id"]].update(value_ug_m3=-1.0),
        )
        failing = self.cli("evaluate", "--manifest", str(run["manifest"]), "--baseline", str(run["baseline"]),
                           "--experiment", str(broken), "--output-dir", str(self.dir / "cli-fail"))
        self.assertEqual(failing.returncode, 1, failing.stderr)
        self.assertFalse(json.loads(failing.stdout)["passed"])
        mismatched = self.cli("evaluate", "--manifest", str(run["manifest"]), "--baseline", str(run["experiment"]),
                              "--experiment", str(run["experiment"]), "--output-dir", str(self.dir / "cli-bad"))
        self.assertEqual(mismatched.returncode, 2)
        processed = self.cli("process-baseline", "--snapshot", str(self.snapshot), "--output", str(self.dir / "b.json"))
        self.assertEqual(processed.returncode, 0, processed.stderr)


if __name__ == "__main__":
    unittest.main()
