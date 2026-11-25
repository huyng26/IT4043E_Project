"""Local, manifest-free processing of a snapshot or a replay into saved decisions.

This is what a consumer sees: received bytes go through the shared rules and every
delivery gets exactly one terminal decision. Nothing here imports or reads an
evaluator manifest, so expected answers cannot leak into processing. The saved
results files are the only input the evaluator needs besides the manifest.

Decisions made by the Kafka/Spark path are read back from the experiment database
into the same document with ``results_from_store``, and ``transport_summary``
measures what only real transport can show (ACKed records processed, redelivery,
latency). Neither function reads a manifest.

Results document::

    {"schema_version": 1, "kind": "baseline" | "experiment", "rule_version": ...,
     "input": {"sha256": ..., "deliveries": n}, "run_id": ... | null,
     "decisions": [{"key": <record number | delivery id>, "decision": {...}}]}
"""

import json
import math
from collections import Counter
from collections.abc import Iterable, Mapping
from datetime import datetime
from pathlib import Path
from typing import Any

from models import utc_time

from streaming.quality.contracts import (
    decode_wrapper_payload,
    sha256_hex,
    validate_decision,
    validate_wrapper,
)
from streaming.quality.rules import evaluate_payload

RESULTS_SCHEMA_VERSION = 1
SOURCE_TOPIC = "air_quality_raw"
RESULT_KINDS = ("baseline", "experiment")


def read_snapshot_lines(path: Path) -> tuple[str, list[tuple[int, bytes]]]:
    """Return the snapshot digest and its non-blank lines as ``(line_number, bytes)``.

    Line numbers are 1-based positions in the file and stay stable when blank lines
    are skipped. A trailing carriage return is not part of a delivery.
    """
    data = Path(path).read_bytes()
    lines = []
    for number, line in enumerate(data.split(b"\n"), 1):
        raw = line.rstrip(b"\r")
        if raw.strip():
            lines.append((number, raw))
    return sha256_hex(data), lines


def process_snapshot(snapshot_path: Path, *, rule_version: str) -> dict[str, object]:
    """Decide every snapshot line; the key is its record number."""
    digest, lines = read_snapshot_lines(snapshot_path)
    decisions = [
        {
            "key": number,
            "decision": evaluate_payload(raw, source_topic=SOURCE_TOPIC, rule_version=rule_version),
        }
        for number, raw in lines
    ]
    return {
        "schema_version": RESULTS_SCHEMA_VERSION,
        "kind": "baseline",
        "rule_version": rule_version,
        "input": {"sha256": digest, "deliveries": len(lines)},
        "run_id": None,
        "decisions": decisions,
    }


def process_replay(replay_path: Path, *, rule_version: str) -> dict[str, object]:
    """Decide every wrapper; the key is its delivery ID.

    A wrapper that fails its transport contract raises: that is a producer defect,
    not a data-quality outcome, and must not be hidden as a quarantine.
    """
    data = Path(replay_path).read_bytes()
    decisions, run_ids = [], set()
    for line_number, line in enumerate(data.split(b"\n"), 1):
        if not line.strip():
            continue
        try:
            wrapper = validate_wrapper(json.loads(line.decode("utf-8")))
        except (ValueError, UnicodeDecodeError) as error:
            raise ValueError(f"replay line {line_number} is not a valid wrapper: {error}") from error
        run_ids.add(wrapper["run_id"])
        decisions.append(
            {
                "key": wrapper["delivery_id"],
                "decision": evaluate_payload(
                    decode_wrapper_payload(wrapper),
                    source_topic=str(wrapper["original_topic"]),
                    rule_version=rule_version,
                ),
            }
        )
    if len(run_ids) > 1:
        raise ValueError("replay mixes several run IDs")
    keys = [row["key"] for row in decisions]
    if len(set(keys)) != len(keys):
        raise ValueError("replay contains duplicate delivery IDs")
    return {
        "schema_version": RESULTS_SCHEMA_VERSION,
        "kind": "experiment",
        "rule_version": rule_version,
        "input": {"sha256": sha256_hex(data), "deliveries": len(decisions)},
        "run_id": next(iter(run_ids), None),
        "decisions": decisions,
    }


def save_results(results: dict[str, object], path: Path) -> None:
    """Write results as canonical JSON; an existing file is never overwritten."""
    text = json.dumps(results, indent=1, sort_keys=True, allow_nan=False) + "\n"
    with Path(path).open("x", encoding="utf-8") as handle:
        handle.write(text)


def load_results(path: Path, *, kind: str) -> dict[str, object]:
    """Read and validate a results document of the expected kind."""
    try:
        results = json.loads(Path(path).read_text(encoding="utf-8"))
    except (ValueError, UnicodeDecodeError) as error:
        raise ValueError(f"{path} is not valid JSON: {error}") from error
    if not isinstance(results, dict) or results.get("schema_version") != RESULTS_SCHEMA_VERSION:
        raise ValueError(f"{path} is not a schema-{RESULTS_SCHEMA_VERSION} results document")
    if results.get("kind") != kind:
        raise ValueError(f"{path} holds {results.get('kind')!r} results, expected {kind!r}")
    rows = results.get("decisions")
    if not isinstance(rows, list):
        raise ValueError(f"{path}: decisions must be a list")
    seen = set()
    for row in rows:
        if not isinstance(row, dict) or set(row) != {"key", "decision"}:
            raise ValueError(f"{path}: malformed decision row")
        if row["key"] in seen:
            raise ValueError(f"{path}: duplicate key {row['key']!r}")
        seen.add(row["key"])
        validate_decision(row["decision"])
    return results


# --- Results from the Kafka/Spark path ------------------------------------

TRANSPORT_SCHEMA_VERSION = 1
MAX_EXAMPLES = 5


def read_replay_wrappers(replay_path: Path) -> tuple[str, list[dict[str, object]]]:
    """Return the replay digest and its validated wrappers in file order."""
    data = Path(replay_path).read_bytes()
    wrappers = []
    for line_number, line in enumerate(data.split(b"\n"), 1):
        if not line.strip():
            continue
        try:
            wrappers.append(validate_wrapper(json.loads(line.decode("utf-8"))))
        except (ValueError, UnicodeDecodeError) as error:
            raise ValueError(f"replay line {line_number} is not a valid wrapper: {error}") from error
    if len({w["run_id"] for w in wrappers}) > 1:
        raise ValueError("replay mixes several run IDs")
    return sha256_hex(data), wrappers


def results_from_store(
    stored: Iterable[Mapping[str, Any]], *, replay_path: Path, rule_version: str
) -> dict[str, object]:
    """Build an experiment results document from stored decision documents.

    ``stored`` are ``quality_decisions`` rows for one run and rule version. The
    document is keyed and ordered like ``process_replay`` output, so the evaluator
    treats the Spark path and the local path identically.
    """
    digest, wrappers = read_replay_wrappers(replay_path)
    run_id = wrappers[0]["run_id"] if wrappers else None
    rows = []
    for document in stored:
        if document["run_id"] != run_id or document["rule_version"] != rule_version:
            raise ValueError("stored decisions belong to a different run or rule version")
        decision = validate_decision(json.loads(document["decision_json"]))
        if decision["rule_version"] != rule_version:
            raise ValueError("stored decision has a different rule version")
        rows.append((document["scheduled_release_index"], document["delivery_id"], decision))
    rows.sort(key=lambda row: (row[0], row[1]))
    keys = [row[1] for row in rows]
    if len(set(keys)) != len(keys):
        raise ValueError("store holds more than one decision for a delivery")
    return {
        "schema_version": RESULTS_SCHEMA_VERSION,
        "kind": "experiment",
        "rule_version": rule_version,
        "input": {"sha256": digest, "deliveries": len(wrappers)},
        "run_id": run_id,
        "decisions": [{"key": key, "decision": decision} for _, key, decision in rows],
    }


def _seconds_quantile(values: list[float], fraction: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, math.ceil(fraction * len(ordered)) - 1)]


def _as_datetime(value: object, field: str) -> datetime:
    if isinstance(value, datetime):
        return value
    return utc_time(value, field)


def transport_summary(
    *,
    replay_path: Path,
    rule_version: str,
    decisions: Iterable[Mapping[str, Any]],
    receipts: Iterable[Mapping[str, Any]],
    rejections: Iterable[Mapping[str, Any]],
    publications: Iterable[Mapping[str, Any]],
) -> dict[str, object]:
    """Measure the transport: every ACKed record processed, redelivery, latency.

    ``publications`` are the logs written by ``quality_replay publish``. Latency is
    ``processed_at - published_at`` per received Kafka record; it includes Spark's
    trigger interval and is not a rule-processing cost.
    """
    digest, wrappers = read_replay_wrappers(replay_path)
    run_id = wrappers[0]["run_id"] if wrappers else None
    planned = [w["delivery_id"] for w in wrappers]
    decided = [d["delivery_id"] for d in decisions]
    receipts = list(receipts)
    rejections = list(rejections)
    publications = [p for p in publications if p.get("run_id") == run_id]

    per_delivery = Counter(r["delivery_id"] for r in receipts)
    received_offsets = {(r["topic"], r["partition"], r["offset"]) for r in receipts}
    rejected_offsets = {(r["topic"], r["partition"], r["offset"]) for r in rejections}
    acked = {
        (p["topic"], ack["partition"], ack["offset"])
        for p in publications
        for ack in p.get("acks", [])
    }
    unprocessed = sorted(acked - received_offsets - rejected_offsets)
    publication_of = {
        (p["topic"], ack["partition"], ack["offset"]): p["publication"]
        for p in publications
        for ack in p.get("acks", [])
    }
    latencies, by_publication = [], {}
    for receipt in receipts:
        if receipt.get("published_at") is None:
            continue
        delta = _as_datetime(receipt["processed_at"], "processed_at") - _as_datetime(
            receipt["published_at"], "published_at"
        )
        latencies.append(delta.total_seconds())
        number = publication_of.get((receipt["topic"], receipt["partition"], receipt["offset"]))
        by_publication.setdefault(str(number), []).append(delta.total_seconds())
    decided_set = set(decided)
    missing = [i for i in planned if i not in decided_set]
    unexpected = sorted(decided_set - set(planned))
    reasons = Counter(r["reason"] for r in rejections)
    summary = {
        "schema_version": TRANSPORT_SCHEMA_VERSION,
        "run_id": run_id,
        "rule_version": rule_version,
        "replay_sha256": digest,
        "publications": len(publications),
        "failed_publications": sum(1 for p in publications if p.get("error")),
        "acknowledged_records": len(acked),
        "planned_deliveries": len(planned),
        "decided_deliveries": len(decided_set),
        "duplicate_decisions": len(decided) - len(decided_set),
        "missing_deliveries": len(missing),
        "missing_examples": missing[:MAX_EXAMPLES],
        "unexpected_deliveries": len(unexpected),
        "received_records": len(receipts),
        "redelivered_deliveries": sum(1 for count in per_delivery.values() if count > 1),
        "max_records_per_delivery": max(per_delivery.values(), default=0),
        "acknowledged_not_processed": len(unprocessed),
        "acknowledged_not_processed_examples": [list(o) for o in unprocessed[:MAX_EXAMPLES]],
        "rejections_by_reason": dict(sorted(reasons.items())),
        "latency_seconds": {
            "definition": "processed_at - published_at per received Kafka record",
            "count": len(latencies),
            "p50": _seconds_quantile(latencies, 0.5),
            "p95": _seconds_quantile(latencies, 0.95),
            "max": max(latencies, default=None),
            "by_publication": {
                number: {
                    "count": len(values),
                    "p50": _seconds_quantile(values, 0.5),
                    "p95": _seconds_quantile(values, 0.95),
                    "max": max(values),
                }
                for number, values in sorted(by_publication.items())
            },
        },
    }
    summary["checks"] = {
        "every_planned_delivery_decided": not missing and not unexpected,
        "one_decision_per_delivery": summary["duplicate_decisions"] == 0,
        "every_acknowledged_record_processed": None if not acked else not unprocessed,
        "no_conflicting_redeliveries": reasons["conflicting_redelivery"] == 0,
        "no_invalid_wrappers": reasons["invalid_wrapper"] == 0 and reasons["wrong_topic"] == 0,
    }
    return summary
