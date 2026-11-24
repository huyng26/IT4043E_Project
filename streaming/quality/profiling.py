"""Profile frozen source input before any faults are injected.

The profile separates structurally malformed deliveries from quality flags and
from unknown cadence. It reads the snapshot (and catalog) without rewriting them
and makes no API request. The report is deterministic: it carries checksums and
counts but no wall-clock time.
"""

import hashlib
import json
from collections import Counter
from pathlib import Path

from models import timestamp, utc_time
from streaming.quality.contracts import sha256_hex
from streaming.quality.coverage import (
    expected_sensor_hours,
    parse_catalog,
    summarize_coverage,
)
from streaming.quality.rules import RULE_VERSION, evaluate_payload, select_current_decisions

PROFILE_SCHEMA_VERSION = 1
SOURCE_TOPIC = "air_quality_raw"


def _counts(counter: Counter) -> dict[str, int]:
    return {str(key): counter[key] for key in sorted(counter, key=str)}


def _value_class(value: float | None) -> str:
    if value is None:
        return "null"
    return "zero" if value == 0 else "negative" if value < 0 else "positive"


def profile_snapshot(
    snapshot_path: Path,
    *,
    catalog_path: Path | None = None,
    rule_version: str = RULE_VERSION,
) -> dict[str, object]:
    """Report natural defects, units, periods, duplicates and known coverage.

    ``catalog_path`` supplies the known inventory/cadence (see
    ``streaming.quality.coverage``). Without it, expected coverage is unknown and
    reported as ``None``. Blank lines are skipped; every other line is a delivery.
    """
    snapshot_path = Path(snapshot_path)
    snapshot_hash = hashlib.sha256()
    decisions, snapshot_bytes = [], 0
    with snapshot_path.open("rb") as handle:
        for line in handle:
            snapshot_hash.update(line)
            snapshot_bytes += len(line)
            payload = line.rstrip(b"\r\n")
            if payload.strip():
                decisions.append(
                    evaluate_payload(payload, source_topic=SOURCE_TOPIC, rule_version=rule_version)
                )

    accepted = [d for d in decisions if d["disposition"] == "accepted"]
    current = select_current_decisions(accepted)
    reasons = Counter(d["reason"] for d in decisions if d["disposition"] == "quarantined")
    flags = Counter(flag for d in accepted for flag in d["quality_flags"])
    actions = Counter(action for d in accepted for action in d["repair_actions"])
    units = Counter(d["received_unit"] for d in accepted)
    periods = Counter(
        int((utc_time(d["period_end"], "period_end") - utc_time(d["period_start"], "period_start")).total_seconds())
        for d in accepted
    )
    values = Counter(_value_class(d["value_ug_m3"]) for d in accepted)
    revisions = {(d["event_id"], d["revision_hash"]) for d in accepted}
    per_event = Counter(event for event, _ in revisions)

    starts: dict[tuple, set] = {}
    for d in current:
        starts.setdefault((d["station_id"], d["sensor_id"]), set()).add(utc_time(d["period_start"], "period_start"))
    intervals = Counter(
        int((later - earlier).total_seconds())
        for times in starts.values()
        for earlier, later in zip(sorted(times), sorted(times)[1:])
    )

    first_start = min((utc_time(d["period_start"], "period_start") for d in accepted), default=None)
    last_end = max((utc_time(d["period_end"], "period_end") for d in accepted), default=None)

    catalog_report, sensors, expected = None, [], []
    if catalog_path is not None:
        catalog_bytes = Path(catalog_path).read_bytes()
        sensors = parse_catalog(json.loads(catalog_bytes))
        known = [s for s in sensors if s["expected_period_seconds"] is not None]
        catalog_report = {
            "sha256": sha256_hex(catalog_bytes),
            "sensors": len(sensors),
            "sensors_with_known_cadence": len(known),
        }
        if first_start is not None and last_end is not None:
            expected = expected_sensor_hours(sensors, window_start=first_start, window_end=last_end)
    observed = set(starts)
    inventory = {(s["station_id"], s["sensor_id"]) for s in sensors}

    return {
        "schema_version": PROFILE_SCHEMA_VERSION,
        "rule_version": rule_version,
        "snapshot": {
            "file_name": snapshot_path.name,
            "sha256": snapshot_hash.hexdigest(),
            "bytes": snapshot_bytes,
            "deliveries": len(decisions),
        },
        "catalog": catalog_report,
        "deliveries": {
            "accepted": len(accepted),
            "quarantined": len(decisions) - len(accepted),
            "quarantine_reasons": _counts(reasons),
            "quality_flags": _counts(flags),
            "repair_actions": _counts(actions),
        },
        "units": _counts(units),
        "reporting_period_seconds": _counts(periods),
        "normalized_values": _counts(values),
        "observations": {
            "unique_events": len(per_event),
            "unique_revisions": len(revisions),
            "duplicate_deliveries": len(accepted) - len(revisions),
            "events_with_multiple_revisions": sum(count > 1 for count in per_event.values()),
            "current": len(current),
            "usable": sum(bool(d["usable"]) for d in current),
        },
        "time_range": {
            "first_period_start": None if first_start is None else timestamp(first_start),
            "last_period_end": None if last_end is None else timestamp(last_end),
        },
        "sensors": {
            "observed": len(observed),
            "observed_not_in_catalog": len(observed - inventory) if catalog_path is not None else None,
            "catalog_without_observations": len(inventory - observed) if catalog_path is not None else None,
        },
        "observed_interval_seconds": _counts(intervals),
        "coverage": summarize_coverage(current, expected_sensor_hours=expected),
    }
