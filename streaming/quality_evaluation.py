"""Evaluator-only comparison of a corrupted run with its unmodified baseline.

This module reads the evaluator manifest (fault labels and expected outcomes), so
it must never be imported by the quality rules, a dispatcher or executor code. The
processing side produces *results files* without seeing a manifest
(``streaming.quality.local_run``); this module compares them::

    python3 -m streaming.quality_evaluation run --run-dir runs/<run> \\
        --snapshot data/frozen.jsonl [--catalog catalog.json]

The report is deterministic: the same inputs give byte-identical JSON. It scores
detection, repair correctness, valid-control false positives, logical invariance,
coverage loss and aggregate effect. Lost data is reported, never reconstructed from
the manifest. Timeliness and forecast robustness need the Kafka/Spark path and are
listed as not assessed.
"""

import argparse
import json
import math
import random
import sys
from collections import Counter
from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Any

from models import utc_time
from streaming.quality.contracts import sha256_hex, validate_manifest
from streaming.quality.coverage import expected_sensor_hours, parse_catalog, summarize_coverage
from streaming.quality.local_run import (
    load_results,
    process_replay,
    process_snapshot,
    save_results,
)
from streaming.quality.rules import RULE_VERSION, select_current_decisions

EVALUATION_VERSION = "evaluation-v1"
VALUE_TOLERANCE = 1e-9  # ug/m3; the documented fixture tolerance
ORDER_CHECKS = 5
MAX_EXAMPLES = 5
REPORT_FILE = "evaluation.json"
BASELINE_RESULTS_FILE = "baseline_results.json"
EXPERIMENT_RESULTS_FILE = "experiment_results.json"
NOT_ASSESSED = {
    "timeliness": "needs publish/process timestamps from the Kafka and Spark path",
    "forecast_robustness": "needs the forecast pipeline, which does not exist yet",
    "restart_recovery": "needs a stoppable consumer; this run replays a file locally",
}


# --- Small helpers --------------------------------------------------------


def _rate(numerator: int, denominator: int) -> float | None:
    """A ratio, or None when its denominator is zero (undefined, not zero)."""
    return None if denominator == 0 else numerator / denominator


def _signals(decision: Mapping[str, Any]) -> set[str]:
    if decision["disposition"] == "quarantined":
        return {"quarantined"}
    return {f"flag:{f}" for f in decision["quality_flags"]} | {
        f"action:{a}" for a in decision["repair_actions"]
    }


def _expected_signals(delivery: Mapping[str, Any]) -> set[str]:
    expected = delivery["expected"]
    if not expected:
        return set()
    if delivery["role"] == "intentional_duplicate":
        return {"repeat"}
    if expected.get("disposition") == "quarantined":
        return {"quarantined"}
    return {f"flag:{f}" for f in expected.get("added_quality_flags", [])} | {
        f"action:{a}" for a in expected.get("added_repair_actions", [])
    }


def _destructive(delivery: Mapping[str, Any]) -> bool:
    expected = delivery["expected"] or {}
    return expected.get("disposition") == "quarantined" or bool(expected.get("added_quality_flags"))


def _hour(decision: Mapping[str, Any]):
    return utc_time(decision["period_start"], "period_start")


def _sensor_hour(decision: Mapping[str, Any]):
    return decision["station_id"], decision["sensor_id"], _hour(decision)


def _fingerprint(decision: Mapping[str, Any]) -> tuple:
    return (
        decision["disposition"],
        decision["reason"],
        decision["event_id"],
        decision["revision_hash"],
        tuple(decision["quality_flags"]),
        tuple(decision["repair_actions"]),
        decision["usable"],
        decision["value_ug_m3"],
    )


def _same_value(a: float | None, b: float | None) -> bool:
    if a is None or b is None:
        return a is None and b is None
    return abs(a - b) <= VALUE_TOLERANCE


def _logical(current: Iterable[Mapping[str, Any]]) -> dict[str, tuple]:
    return {
        str(d["event_id"]): (bool(d["usable"]), tuple(d["quality_flags"]), d["value_ug_m3"])
        for d in current
    }


def _logical_differences(a: Mapping[str, tuple], b: Mapping[str, tuple]) -> set[str]:
    different = set()
    for event in a.keys() | b.keys():
        left, right = a.get(event), b.get(event)
        if left is None or right is None:
            different.add(event)
        elif left[:2] != right[:2] or not _same_value(left[2], right[2]):
            different.add(event)
    return different


def _hourly_means(decisions: Iterable[Mapping[str, Any]], exclude: frozenset[str] = frozenset()):
    """Mean of usable current values per station-hour, as the live aggregate does."""
    groups: dict[tuple, list[float]] = {}
    for d in decisions:
        if d["usable"] and d["event_id"] not in exclude:
            groups.setdefault((d["station_id"], _hour(d)), []).append(d["value_ug_m3"])
    return {key: math.fsum(v / len(values) for v in values) for key, values in groups.items()}


def _bounded(examples: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return examples[:MAX_EXAMPLES]


# --- Evaluation -----------------------------------------------------------


def _check_compatibility(manifest, baseline, experiment) -> None:
    versions = {manifest["rule_version"], baseline["rule_version"], experiment["rule_version"]}
    if len(versions) != 1:
        raise ValueError(f"rule versions differ across manifest and results: {sorted(versions)}")
    if baseline["input"]["sha256"] != manifest["snapshot"]["sha256"]:
        raise ValueError("baseline results were not produced from the manifest's snapshot")
    if experiment["decisions"] and experiment["run_id"] != manifest["run_id"]:
        raise ValueError("experiment results belong to a different run")


def _load_catalog(catalog_path: Path | None, manifest) -> list[dict[str, object]] | None:
    if catalog_path is None:
        return None
    data = Path(catalog_path).read_bytes()
    recorded = (manifest.get("catalog") or {}).get("sha256")
    if recorded is not None and recorded != sha256_hex(data):
        raise ValueError("catalog does not match the catalog frozen in the manifest")
    return parse_catalog(json.loads(data.decode("utf-8")))


def evaluate_run(
    *,
    manifest_path: Path,
    baseline_results_path: Path,
    experiment_results_path: Path,
    output_directory: Path,
    catalog_path: Path | None = None,
) -> dict[str, Any]:
    """Compare results against the baseline and save ``evaluation.json``.

    Raises ``ValueError`` when the inputs do not belong together (different rule
    version, snapshot or run). Missing or unexpected deliveries do not raise: they
    are accounting findings in the report.
    """
    manifest: Any = validate_manifest(json.loads(Path(manifest_path).read_text(encoding="utf-8")))
    baseline: Any = load_results(baseline_results_path, kind="baseline")
    experiment: Any = load_results(experiment_results_path, kind="experiment")
    _check_compatibility(manifest, baseline, experiment)
    sensors = _load_catalog(catalog_path, manifest)

    base: dict[int, dict[str, Any]] = {row["key"]: row["decision"] for row in baseline["decisions"]}
    exp: dict[str, dict[str, Any]] = {row["key"]: row["decision"] for row in experiment["decisions"]}
    all_deliveries = manifest["deliveries"]
    for delivery in all_deliveries:
        if delivery["source_record_ref"]["record_number"] not in base:
            raise ValueError("baseline results do not cover every record in the manifest")
    deliveries = [d for d in all_deliveries if d["delivery_id"] in exp]

    def record(delivery) -> int:
        return delivery["source_record_ref"]["record_number"]

    # Repeat deliveries: a revision already seen earlier in release (or file) order.
    seen: set[str] = set()
    experiment_repeat: dict[str, bool] = {}
    for delivery in deliveries:
        decision = exp[delivery["delivery_id"]]
        revision = decision["revision_hash"]
        experiment_repeat[delivery["delivery_id"]] = revision is not None and revision in seen
        if revision is not None:
            seen.add(revision)
    seen = set()
    baseline_repeat: dict[int, bool] = {}
    for number in sorted(base):
        revision = base[number]["revision_hash"]
        baseline_repeat[number] = revision is not None and revision in seen
        if revision is not None:
            seen.add(revision)

    def observed(delivery) -> set[str]:
        found = _signals(exp[delivery["delivery_id"]])
        return found | {"repeat"} if experiment_repeat[delivery["delivery_id"]] else found

    def baseline_observed(delivery) -> set[str]:
        number = record(delivery)
        found = _signals(base[number])
        return found | {"repeat"} if baseline_repeat[number] else found

    # --- Delivery accounting and natural defects
    accepted = sum(1 for d in exp.values() if d["disposition"] == "accepted")
    quarantined = len(exp) - accepted
    manifest_ids = [d["delivery_id"] for d in all_deliveries]
    missing_ids = [i for i in manifest_ids if i not in exp]
    unexpected_ids = sorted(set(exp) - set(manifest_ids))
    accounting = {
        "planned_emitted": len(all_deliveries),
        "terminal_decisions": len(exp),
        "accepted": accepted,
        "quarantined": quarantined,
        "missing_decisions": missing_ids[:MAX_EXAMPLES * 4],
        "unexpected_decisions": unexpected_ids[:MAX_EXAMPLES * 4],
        "omitted_records": sum(len(o["record_numbers"]) for o in manifest["omitted_records"]),
        "input_records": manifest["input_record_count"],
        "by_role": dict(sorted(Counter(d["role"] for d in all_deliveries).items())),
        "by_fault": dict(sorted(Counter(d["fault"] for d in all_deliveries if d["fault"]).items())),
        "balanced": not missing_ids and not unexpected_ids and accepted + quarantined == len(all_deliveries),
    }
    natural = {
        "baseline_records": len(base),
        "quarantined": sum(1 for d in base.values() if d["disposition"] == "quarantined"),
        "quarantine_reasons": dict(
            sorted(Counter(d["reason"] for d in base.values() if d["reason"]).items())
        ),
        "quality_flags": dict(
            sorted(Counter(f for d in base.values() for f in d["quality_flags"]).items())
        ),
        "repeat_deliveries": sum(baseline_repeat.values()),
    }

    # --- Detection precision/recall per fault type
    fault_signals: dict[str, set[str]] = {}
    for delivery in all_deliveries:
        if delivery["fault"]:
            fault_signals.setdefault(delivery["fault"], set()).update(_expected_signals(delivery))
    detection: dict[str, dict[str, Any]] = {}
    false_positive_examples: list[dict[str, Any]] = []
    for fault in sorted({d["fault"] for d in all_deliveries if d["fault"]}):
        labeled = [d for d in deliveries if d["fault"] == fault]
        signal = fault_signals[fault]
        if not signal:
            detection[fault] = {
                "labeled": len(labeled),
                "detection": "not_applicable",
                "reason": "no processing signal is expected; scored by logical invariance",
            }
            continue
        tp = sum(1 for d in labeled if signal <= observed(d))
        fp = 0
        for d in deliveries:
            if d["fault"] == fault:
                continue
            if signal <= observed(d) and not signal <= baseline_observed(d) and not signal <= _expected_signals(d):
                fp += 1
                false_positive_examples.append(
                    {"delivery_id": d["delivery_id"], "fault_scored": fault, "signals": sorted(observed(d))}
                )
        fn = len(labeled) - tp
        entry = {
            "labeled": len(labeled),
            "signal": sorted(signal),
            "true_positive": tp,
            "false_negative": fn,
            "false_positive": fp,
            "precision": _rate(tp, tp + fp),
            "recall": _rate(tp, tp + fn),
        }
        if signal == {"quarantined"}:
            entry["reason_matches"] = sum(
                1
                for d in labeled
                if exp[d["delivery_id"]]["reason"] == (d["expected"] or {}).get("reason")
            )
        detection[fault] = entry

    # --- Valid-control false positives and unlabeled drift
    unlabeled = [d for d in deliveries if d["fault"] is None]
    clean = [
        d
        for d in unlabeled
        if base[record(d)]["disposition"] == "accepted"
        and not _signals(base[record(d)])
        and not baseline_repeat[record(d)]
    ]
    control_fp = [d for d in clean if observed(d)]
    drifted = [d for d in unlabeled if _fingerprint(exp[d["delivery_id"]]) != _fingerprint(base[record(d)])]
    valid_control = {
        "unlabeled_deliveries": len(unlabeled),
        "clean_unlabeled_deliveries": len(clean),
        "false_positives": len(control_fp),
        "false_positive_rate": _rate(len(control_fp), len(clean)),
        "unlabeled_changed_vs_baseline": len(drifted),
    }

    # --- Repair correctness against the baseline reference
    repairs: dict[str, dict[str, Any]] = {}
    repair_examples: list[dict[str, Any]] = []
    wrong_examples: list[dict[str, Any]] = []
    repairable = sorted({d["fault"] for d in deliveries if (d["expected"] or {}).get("added_repair_actions")})
    for fault in repairable:
        counts = Counter()
        for d in (x for x in deliveries if x["fault"] == fault):
            reference, got = base[record(d)], exp[d["delivery_id"]]
            if reference["disposition"] != "accepted":
                counts["no_valid_reference"] += 1
                continue
            counts["total"] += 1
            wanted = set((d["expected"] or {}).get("added_repair_actions", []))
            if got["disposition"] != "accepted" or not wanted <= set(got["repair_actions"]):
                counts["not_repaired"] += 1
                continue
            correct = (
                got["event_id"] == reference["event_id"]
                and got["period_start"] == reference["period_start"]
                and got["period_end"] == reference["period_end"]
                and got["usable"] == reference["usable"]
                and _same_value(got["value_ug_m3"], reference["value_ug_m3"])
            )
            counts["correct" if correct else "wrong"] += 1
            sample = {
                "delivery_id": d["delivery_id"],
                "fault": fault,
                "received_value": got["received_value"],
                "received_unit": got["received_unit"],
                "repair_actions": got["repair_actions"],
                "repaired_value_ug_m3": got["value_ug_m3"],
                "baseline_value_ug_m3": reference["value_ug_m3"],
            }
            (repair_examples if correct else wrong_examples).append(sample)
        scored = counts["total"]
        repairs[fault] = {
            "total": scored,
            "correct": counts["correct"],
            "wrong": counts["wrong"],
            "not_repaired": counts["not_repaired"],
            "no_valid_reference": counts["no_valid_reference"],
            "correctness": _rate(counts["correct"], scored),
        }
    repair_total = sum(r["total"] for r in repairs.values())
    repair_correct = sum(r["correct"] for r in repairs.values())

    # --- Events the faults were meant to make unusable
    touched: dict[str, set[str]] = {}
    for d in deliveries:
        if _destructive(d) and base[record(d)]["event_id"]:
            touched.setdefault(base[record(d)]["event_id"], set()).add(d["fault"])
    omitted_keys = set()
    for omission in manifest["omitted_records"]:
        group = omission["group"]
        omitted_keys.add((group["station_id"], group["sensor_id"], utc_time(group["hour"], "hour")))
        for number in omission["record_numbers"]:
            if number in base and base[number]["event_id"]:
                touched.setdefault(base[number]["event_id"], set()).add(omission["scenario"])

    base_current = select_current_decisions(base.values())
    exp_current = select_current_decisions(exp.values())
    base_logical, exp_logical = _logical(base_current), _logical(exp_current)
    changed = _logical_differences(base_logical, exp_logical)
    unexpected_changes = sorted(changed - touched.keys())
    shuffler = random.Random(manifest["seed"])
    order_stable = True
    for _ in range(ORDER_CHECKS):
        shuffled = list(exp.values())
        shuffler.shuffle(shuffled)
        if _logical_differences(exp_logical, _logical(select_current_decisions(shuffled))):
            order_stable = False
    silently_usable = sorted(
        e for e in touched if e in exp_logical and exp_logical[e][0]
    )
    correction_total = correction_effective = 0
    for d in deliveries:
        if d["role"] != "correction":
            continue
        correction_total += 1
        event = base[record(d)]["event_id"]
        current = next((c for c in exp_current if c["event_id"] == event), None)
        if current and current["revision_hash"] == exp[d["delivery_id"]]["revision_hash"] and not current["usable"]:
            correction_effective += 1
    invariance = {
        "events_in_baseline": len(base_logical),
        "events_in_experiment": len(exp_logical),
        "events_expected_to_change": len(touched),
        "events_changed": len(changed),
        "unexpected_changes": unexpected_changes[:MAX_EXAMPLES * 4],
        "unexpected_change_count": len(unexpected_changes),
        "expected_change_not_observed": len(touched.keys() - changed),
        "silently_usable_after_destructive_fault": len(silently_usable),
        "order_checks": ORDER_CHECKS,
        "order_invariant": order_stable,
        "corrections": {"total": correction_total, "effective": correction_effective},
    }

    # --- Coverage, loss and gap detection
    base_keys_usable = {_sensor_hour(d) for d in base_current if d["usable"]}
    exp_keys_usable = {_sensor_hour(d) for d in exp_current if d["usable"]}
    base_received = {_sensor_hour(d) for d in base_current}
    exp_received = {_sensor_hour(d) for d in exp_current}
    if sensors is not None and base_current:
        window_start = min(_hour(d) for d in base_current)
        window_end = max(utc_time(d["period_end"], "period_end") for d in base_current)
        expected = expected_sensor_hours(sensors, window_start=window_start, window_end=window_end)
        expected_source = "catalog"
    else:
        expected = [{"station_id": s, "sensor_id": x, "hour": h} for s, x, h in sorted(base_received)]
        expected_source = "baseline_observations"
    key_faults: dict[tuple, set[str]] = {}
    for event, faults in touched.items():
        decision = next((c for c in base_current if c["event_id"] == event), None)
        if decision is not None:
            key_faults.setdefault(_sensor_hour(decision), set()).update(faults)
    lost = base_keys_usable - exp_keys_usable
    loss_by_fault: Counter = Counter()
    for key in lost & key_faults.keys():
        for fault in sorted(key_faults[key]):
            loss_by_fault[fault] += 1
    unexplained = sorted(lost - key_faults.keys())
    coverage: dict[str, Any] = {
        "expected_source": expected_source,
        "baseline": summarize_coverage(base_current, expected_sensor_hours=expected),
        "experiment": summarize_coverage(exp_current, expected_sensor_hours=expected),
        "usable_sensor_hours_baseline": len(base_keys_usable),
        "usable_sensor_hours_experiment": len(exp_keys_usable),
        "lost_usable_sensor_hours": len(lost),
        "lost_by_fault": dict(sorted(loss_by_fault.items())),
        "unexplained_loss": [
            {"station_id": s, "sensor_id": x, "hour": h.isoformat()} for s, x, h in unexplained[:MAX_EXAMPLES]
        ],
        "unexplained_loss_count": len(unexplained),
        "unexpected_gain_count": len(exp_keys_usable - base_keys_usable),
    }
    if sensors is None:
        gap: dict[str, Any] = {
            "status": "not_assessed",
            "reason": "gap detection needs a catalog that establishes expected observations",
            "labeled_groups": len(omitted_keys),
        }
    else:
        expected_set = {(r["station_id"], r["sensor_id"], r["hour"]) for r in expected}
        labeled_in = omitted_keys & expected_set
        missing_now = expected_set - exp_received
        natural_missing = expected_set - base_received
        gap_tp = len(labeled_in & missing_now)
        gap_fn = len(labeled_in - missing_now)
        gap_fp = len(missing_now - omitted_keys - natural_missing - key_faults.keys())
        gap = {
            "status": "assessed",
            "labeled_groups": len(omitted_keys),
            "labeled_not_expected": len(omitted_keys - expected_set),
            "true_positive": gap_tp,
            "false_negative": gap_fn,
            "false_positive": gap_fp,
            "precision": _rate(gap_tp, gap_tp + gap_fp),
            "recall": _rate(gap_tp, gap_tp + gap_fn),
            "natural_missing_sensor_hours": len(natural_missing),
        }

    # --- Aggregate effect on station-hour means
    base_hourly = _hourly_means(base_current)
    exp_hourly = _hourly_means(exp_current)
    retained = _hourly_means(base_current, frozenset(touched))
    affected_hours = {
        (c["station_id"], _hour(c)) for c in base_current if c["event_id"] in touched
    }
    recoverable = set(base_hourly) - affected_hours
    errors, lost_unexpectedly = [], 0
    for key in recoverable:
        if key in exp_hourly:
            errors.append(abs(exp_hourly[key] - base_hourly[key]))
        else:
            lost_unexpectedly += 1
    affected_mismatch = sum(
        1
        for key in affected_hours
        if not _same_value(retained.get(key), exp_hourly.get(key))
    )
    aggregate = {
        "definition": "mean of usable current value_ug_m3 per (station_id, hour)",
        "baseline_hours": len(base_hourly),
        "recoverable_hours": len(recoverable),
        "recoverable_max_abs_error": max(errors, default=None),
        "recoverable_mean_abs_error": None if not errors else math.fsum(errors) / len(errors),
        "recoverable_mismatches": sum(1 for e in errors if e > VALUE_TOLERANCE) + lost_unexpectedly,
        "affected_hours": len(affected_hours),
        "affected_hours_lost": sum(1 for k in affected_hours if k in base_hourly and k not in exp_hourly),
        "affected_mismatches_vs_expected_subset": affected_mismatch,
        "unexpected_hours": len(set(exp_hourly) - set(base_hourly)),
    }

    # --- Verdict and examples
    checks: dict[str, bool | None] = {
        "delivery_accounting_balanced": accounting["balanced"],
        "no_false_positives_on_clean_deliveries": not control_fp,
        "unlabeled_deliveries_match_baseline": not drifted,
        "all_repairs_correct": None if repair_total == 0 else repair_correct == repair_total,
        "no_unexpected_logical_changes": not unexpected_changes,
        "destructive_faults_never_silently_usable": not silently_usable,
        "order_invariant": order_stable,
        "corrections_effective": None if correction_total == 0 else correction_effective == correction_total,
        "all_coverage_loss_explained": not unexplained and not coverage["unexpected_gain_count"],
        "aggregate_matches_baseline_where_recoverable": aggregate["recoverable_mismatches"] == 0
        and affected_mismatch == 0,
    }
    quarantine_examples = [
        {
            "delivery_id": d["delivery_id"],
            "fault": d["fault"],
            "reason": exp[d["delivery_id"]]["reason"],
            "detail": exp[d["delivery_id"]]["detail"],
        }
        for d in deliveries
        if exp[d["delivery_id"]]["disposition"] == "quarantined"
    ]
    report = {
        "evaluation_version": EVALUATION_VERSION,
        "run": {
            "run_id": manifest["run_id"],
            "profile": manifest["profile"],
            "seed": manifest["seed"],
            "rule_version": manifest["rule_version"],
            "injector_version": manifest["injector_version"],
            "code_revision": manifest["code_revision"],
            "snapshot_sha256": manifest["snapshot"]["sha256"],
            "catalog_sha256": (manifest.get("catalog") or {}).get("sha256"),
            "config": manifest["config"],
        },
        "inputs": {
            "manifest_sha256": sha256_hex(Path(manifest_path).read_bytes()),
            "baseline_results_sha256": sha256_hex(Path(baseline_results_path).read_bytes()),
            "experiment_results_sha256": sha256_hex(Path(experiment_results_path).read_bytes()),
            "catalog_used": sensors is not None,
        },
        "checks": checks,
        "passed": all(value for value in checks.values() if value is not None),
        "accounting": accounting,
        "natural_defects": natural,
        "detection": detection,
        "valid_control": valid_control,
        "repair_correctness": {
            "tolerance_ug_m3": VALUE_TOLERANCE,
            "overall": _rate(repair_correct, repair_total),
            "by_fault": repairs,
        },
        "logical_invariance": invariance,
        "coverage": coverage,
        "gap_detection": gap,
        "aggregate_effect": aggregate,
        "not_assessed": NOT_ASSESSED,
        "examples": {
            "repairs": _bounded(repair_examples),
            "wrong_repairs": _bounded(wrong_examples),
            "quarantined": _bounded(quarantine_examples),
            "false_positives": _bounded(
                false_positive_examples + [{"delivery_id": d["delivery_id"], "fault_scored": None} for d in control_fp]
            ),
        },
    }
    output_directory = Path(output_directory)
    output_directory.mkdir(parents=True, exist_ok=True)
    with (output_directory / REPORT_FILE).open("x", encoding="utf-8") as handle:
        handle.write(json.dumps(report, indent=2, sort_keys=True, allow_nan=False) + "\n")
    return report


# --- Command line ---------------------------------------------------------


def _run(run_dir: Path, snapshot: Path, catalog: Path | None) -> dict[str, Any]:
    manifest: Any = validate_manifest(json.loads((run_dir / "manifest.json").read_text(encoding="utf-8")))
    rule_version = str(manifest["rule_version"])
    save_results(process_snapshot(snapshot, rule_version=rule_version), run_dir / BASELINE_RESULTS_FILE)
    save_results(process_replay(run_dir / "replay.jsonl", rule_version=rule_version), run_dir / EXPERIMENT_RESULTS_FILE)
    return evaluate_run(
        manifest_path=run_dir / "manifest.json",
        baseline_results_path=run_dir / BASELINE_RESULTS_FILE,
        experiment_results_path=run_dir / EXPERIMENT_RESULTS_FILE,
        output_directory=run_dir / "evaluation",
        catalog_path=catalog,
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    commands = parser.add_subparsers(dest="command", required=True)
    base = commands.add_parser("process-baseline", help="decide every snapshot line (no manifest)")
    base.add_argument("--snapshot", type=Path, required=True)
    base.add_argument("--output", type=Path, required=True)
    base.add_argument("--rule-version", default=RULE_VERSION)
    replay = commands.add_parser("process-replay", help="decide every wrapper (no manifest)")
    replay.add_argument("--replay", type=Path, required=True)
    replay.add_argument("--output", type=Path, required=True)
    replay.add_argument("--rule-version", default=RULE_VERSION)
    evaluate = commands.add_parser("evaluate", help="score saved results against a manifest")
    evaluate.add_argument("--manifest", type=Path, required=True)
    evaluate.add_argument("--baseline", type=Path, required=True)
    evaluate.add_argument("--experiment", type=Path, required=True)
    evaluate.add_argument("--output-dir", type=Path, required=True)
    evaluate.add_argument("--catalog", type=Path)
    run = commands.add_parser("run", help="process baseline and replay, then evaluate")
    run.add_argument("--run-dir", type=Path, required=True, help="directory written by quality_replay build")
    run.add_argument("--snapshot", type=Path, required=True)
    run.add_argument("--catalog", type=Path)
    args = parser.parse_args(argv)
    try:
        if args.command == "process-baseline":
            save_results(process_snapshot(args.snapshot, rule_version=args.rule_version), args.output)
            print(f"wrote {args.output}")
            return 0
        if args.command == "process-replay":
            save_results(process_replay(args.replay, rule_version=args.rule_version), args.output)
            print(f"wrote {args.output}")
            return 0
        if args.command == "evaluate":
            report = evaluate_run(
                manifest_path=args.manifest,
                baseline_results_path=args.baseline,
                experiment_results_path=args.experiment,
                output_directory=args.output_dir,
                catalog_path=args.catalog,
            )
        else:
            report = _run(args.run_dir, args.snapshot, args.catalog)
    except (OSError, ValueError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 2
    print(json.dumps({"passed": report["passed"], "checks": report["checks"]}, indent=2, sort_keys=True))
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
