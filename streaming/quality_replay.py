"""Bounded deterministic fault injector and replay builder.

``build`` reads a frozen JSONL snapshot, selects deliveries with a stable hash of
the seed, the input record and the fault name (never Python's ``hash()``), writes
processor-facing wrappers to ``replay.jsonl`` and the evaluator-only
``manifest.json``, and never touches the snapshot, the network, Kafka or a
database. Publication to the experiment Kafka topic is a separate command::

    python3 -m streaming.quality_replay build --config quality.json \\
        --snapshot data/frozen.jsonl --output-dir runs/<run>
    python3 -m streaming.quality_replay publish --run-dir runs/<run>

Wrappers carry only transport fields. Fault labels, original values and expected
outcomes live exclusively in the manifest. Expected outcomes are written by hand
for each fault, not derived by running the processing rules.

Group scenarios operate on sensor/hour groups chosen before row mutations, and
those groups are reserved from the row faults: ``missing_interval`` omits every
delivery of a group and ``correction_to_null`` adds a later revision that sets the
group's current value to null. Flatlines are out of scope and are rejected
explicitly rather than ignored.
"""

import argparse
import hashlib
import json
import math
import subprocess
import sys
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from models import MeasurementEvent, canonical_json, digest, timestamp, utc_time
from streaming.quality.contracts import (
    EXPERIMENT_DATASET_KIND,
    MANIFEST_SCHEMA_VERSION,
    REPLAY_TOPIC,
    build_wrapper,
    sha256_hex,
    validate_manifest,
    validate_wrapper,
)
from streaming.quality.local_run import read_snapshot_lines
from streaming.quality.rules import SUPPORTED_RULE_VERSIONS

INJECTOR_VERSION = "injector-v1"  # used when a config names no version
INJECTOR_VERSIONS = ("injector-v1", "injector-v2")
# Spellings of ug/m3 a fault may start from. v1 only knew the canonical one, so on
# real OpenAQ rows ("µg/m³") the unit faults and correction_to_null never fired.
# v2 adds the micro-sign and Greek-mu aliases the shared rules already accept.
CONCENTRATION_UNITS = {
    "injector-v1": frozenset({"ug/m3"}),
    "injector-v2": frozenset({"ug/m3", "µg/m³", "μg/m³"}),
}
UNIT_POLICY_VERSION = "units-v1"
TIMEZONE_POLICY_VERSION = "timezone-v1"
SOURCE_TOPIC = "air_quality_raw"
REPLAY_FILE = "replay.jsonl"
MANIFEST_FILE = "manifest.json"

FAULT_TYPES = (
    "duplicate_delivery",
    "supported_unit_variation",
    "numeric_string",
    "missing_value",
    "negative_value",
    "equivalent_timezone",
    "malformed_payload",
    "unsupported_unit",
    "delayed_delivery",
)
PROFILES = ("control", "single-fault", "mixed-v1")
GROUP_SCENARIOS = ("missing_interval", "correction_to_null")
DEFERRED_SCENARIOS = ("flatline",)  # needs anomaly warnings, which are deferred
DEFAULT_GROUP_FRACTION = 0.1
_CORRECTION_LAG = timedelta(hours=1)
_HOUR = timedelta(hours=1)
_REPLAY_OFFSET = timezone(timedelta(hours=7))
_TIME_FIELDS = ("period_start", "period_end", "collected_at", "ingested_at")


@dataclass(frozen=True)
class _Input:
    number: int  # 1-based line number in the snapshot
    raw: bytes  # exact line bytes without the line terminator
    sha256: str
    envelope: dict[str, Any] | None  # a valid PM2.5 envelope, else None


@dataclass(frozen=True)
class _Mutation:
    """How one selected record changes; ``payload`` None keeps the original bytes."""

    payload: bytes | None
    params: dict[str, Any]
    expected: dict[str, Any]


def stable_score(seed: int, record_key: str, label: str) -> int:
    """Process-independent 64-bit selection score."""
    material = canonical_json([seed, record_key, label]).encode()
    return int.from_bytes(hashlib.sha256(material).digest()[:8], "big")


# --- Fault implementations ------------------------------------------------


def _json_bytes(envelope: Mapping[str, Any]) -> bytes:
    return canonical_json(envelope).encode("utf-8")


def _with_refreshed_hash(envelope: dict[str, Any]) -> dict[str, Any]:
    """Recompute the claimed revision hash for a consistent content mutation."""
    if "revision_hash" in envelope:
        envelope["revision_hash"] = MeasurementEvent.from_dict(envelope).revision_hash
    return envelope


def _expected(disposition="accepted", *, actions=(), flags=(), reason=None, identity=True):
    return {
        "disposition": disposition,
        "reason": reason,
        "added_repair_actions": list(actions),
        "added_quality_flags": list(flags),
        "identity_preserved": identity,
    }


def _is_number(value: object) -> bool:
    return type(value) in (int, float)


def _unit_variation(record: _Input, seed: int, units: frozenset[str]) -> _Mutation | None:
    envelope = dict(record.envelope or {})
    if not _is_number(envelope.get("value")) or envelope.get("unit") not in units:
        return None
    envelope.update(value=envelope["value"] / 1000, unit="mg/m3")
    return _Mutation(
        _json_bytes(_with_refreshed_hash(envelope)),
        {"unit": "mg/m3", "value_scale": 0.001},
        _expected(actions=["unit_converted_to_ug_m3"]),
    )


def _numeric_string(record: _Input, seed: int) -> _Mutation | None:
    envelope = dict(record.envelope or {})
    if not _is_number(envelope.get("value")):
        return None
    # The claimed hash stays the numeric baseline's: the repair must round-trip.
    envelope["value"] = f" {json.dumps(envelope['value'])} "
    return _Mutation(
        _json_bytes(envelope), {"format": "padded_json_number"}, _expected(actions=["numeric_string_parsed"])
    )


def _missing_value(record: _Input, seed: int) -> _Mutation | None:
    envelope = dict(record.envelope or {})
    if envelope.get("value") is None:
        return None
    envelope["value"] = None
    return _Mutation(
        _json_bytes(_with_refreshed_hash(envelope)), {"value": None}, _expected(flags=["missing_value"])
    )


def _negative_value(record: _Input, seed: int) -> _Mutation | None:
    envelope = dict(record.envelope or {})
    if not _is_number(envelope.get("value")) or envelope["value"] <= 0:
        return None
    envelope["value"] = -envelope["value"]
    return _Mutation(
        _json_bytes(_with_refreshed_hash(envelope)),
        {"negated": True},
        _expected(flags=["negative_concentration"]),
    )


def _equivalent_timezone(record: _Input, seed: int) -> _Mutation | None:
    envelope = dict(record.envelope or {})
    for field in _TIME_FIELDS:
        envelope[field] = utc_time(envelope[field], field).astimezone(_REPLAY_OFFSET).isoformat()
    return _Mutation(
        _json_bytes(envelope), {"utc_offset": "+07:00"}, _expected(actions=["timestamp_normalized_to_utc"])
    )


def _malformed_payload(record: _Input, seed: int) -> _Mutation | None:
    variant = ("truncated_json", "missing_station_id")[
        stable_score(seed, f"{record.number}:{record.sha256}", "malformed_payload:variant") % 2
    ]
    if variant == "truncated_json":
        payload = record.raw[: max(1, len(record.raw) // 2)]
        reason = "invalid_json"
    else:
        envelope = dict(record.envelope or {})
        del envelope["station_id"]
        payload, reason = _json_bytes(envelope), "missing_required_field"
    return _Mutation(
        payload,
        {"variant": variant},
        _expected("quarantined", reason=reason, identity=False),
    )


def _unsupported_unit(record: _Input, seed: int, units: frozenset[str]) -> _Mutation | None:
    envelope = dict(record.envelope or {})
    if envelope.get("unit") not in units:
        return None
    envelope["unit"] = "ppm"
    return _Mutation(
        _json_bytes(_with_refreshed_hash(envelope)), {"unit": "ppm"}, _expected(flags=["unsupported_unit"])
    )


# Payload faults; duplicate and delayed deliveries change release, not content.
_PAYLOAD_FAULTS = {
    "supported_unit_variation": _unit_variation,
    "numeric_string": _numeric_string,
    "missing_value": _missing_value,
    "negative_value": _negative_value,
    "equivalent_timezone": _equivalent_timezone,
    "malformed_payload": _malformed_payload,
    "unsupported_unit": _unsupported_unit,
}
_UNIT_FAULTS = frozenset({"supported_unit_variation", "unsupported_unit"})


def _plan_mutation(fault: str, record: _Input, seed: int, units: frozenset[str]) -> _Mutation | None:
    if record.envelope is None:
        return None
    if fault == "duplicate_delivery":
        return _Mutation(None, {}, {"disposition": "accepted", "contributes_to_aggregate": False})
    if fault == "delayed_delivery":
        return _Mutation(None, {}, {"disposition": "accepted", "logical_result_unchanged": True})
    if fault in _UNIT_FAULTS:
        return _PAYLOAD_FAULTS[fault](record, seed, units)
    return _PAYLOAD_FAULTS[fault](record, seed)


# --- Input, configuration and planning ------------------------------------


def _read_snapshot(path: Path) -> tuple[str, list[_Input]]:
    snapshot_sha, lines = read_snapshot_lines(path)
    records = []
    for number, raw in lines:
        envelope = None
        try:
            parsed = json.loads(raw.decode("utf-8"))
            if isinstance(parsed, dict):
                MeasurementEvent.from_dict(parsed)  # only valid PM2.5 envelopes are mutable
                envelope = parsed
        except (ValueError, OverflowError):
            pass
        records.append(_Input(number, raw, sha256_hex(raw), envelope))
    return snapshot_sha, records


def _validate_config(config: Mapping[str, Any]) -> dict[str, Any]:
    if config.get("enabled") is not True:
        raise ValueError("quality experiment configuration is not enabled")
    if config.get("schema_version") != 1:
        raise ValueError("quality configuration schema_version must be 1")
    if config.get("rule_version") not in SUPPORTED_RULE_VERSIONS:
        raise ValueError("unsupported rule_version")
    experiment = config.get("experiment")
    if not isinstance(experiment, Mapping):
        raise ValueError("experiment configuration is required")
    seed = experiment.get("seed")
    if type(seed) is not int:
        raise ValueError("seed must be an integer")
    profile = experiment.get("profile")
    if profile not in PROFILES:
        raise ValueError(f"profile must be one of {PROFILES}")
    fraction = experiment.get("selected_input_fraction")
    if not isinstance(fraction, (int, float)) or isinstance(fraction, bool) or not 0 <= fraction <= 1:
        raise ValueError("selected_input_fraction must be between 0 and 1")
    scenarios = list(experiment.get("group_scenarios") or [])
    deferred = sorted(set(scenarios) & set(DEFERRED_SCENARIOS))
    if deferred:
        raise ValueError("group scenarios are not implemented: " + ", ".join(deferred))
    unknown_scenarios = sorted(set(map(str, scenarios)) - set(GROUP_SCENARIOS))
    if unknown_scenarios or len(set(scenarios)) != len(scenarios):
        raise ValueError("group_scenarios must be unique known names: " + ", ".join(unknown_scenarios))
    group_fraction = experiment.get("group_fraction", DEFAULT_GROUP_FRACTION)
    if (
        not isinstance(group_fraction, (int, float))
        or isinstance(group_fraction, bool)
        or not 0 <= group_fraction <= 1
    ):
        raise ValueError("group_fraction must be between 0 and 1")
    injector_version = experiment.get("injector_version", INJECTOR_VERSION)
    if injector_version not in INJECTOR_VERSIONS:
        raise ValueError(f"injector_version must be one of {INJECTOR_VERSIONS}")
    if experiment.get("topic", REPLAY_TOPIC) != REPLAY_TOPIC:
        raise ValueError(f"topic must be {REPLAY_TOPIC}")
    faults = list(experiment.get("fault_types") or [])
    unknown = sorted(set(faults) - set(FAULT_TYPES))
    if unknown or len(set(faults)) != len(faults):
        raise ValueError("fault_types must be unique known fault names: " + ", ".join(unknown))
    if profile == "control":
        if scenarios:
            raise ValueError("the control profile cannot include group scenarios")
        faults = []
    elif profile == "single-fault" and len(faults) != 1:
        raise ValueError("single-fault profile requires exactly one fault type")
    elif profile == "mixed-v1" and len(faults) < 2:
        raise ValueError("mixed-v1 profile requires at least two fault types")
    return {
        "rule_version": config["rule_version"],
        "seed": seed,
        "profile": profile,
        "fraction": fraction,
        "faults": faults,
        "scenarios": scenarios,
        "group_fraction": group_fraction,
        "injector_version": injector_version,
        "run_id": experiment.get("run_id"),
        "catalog_path": experiment.get("catalog_path"),
        "code_revision": config.get("code_revision"),
    }


def _select_count(fraction: float, pool: int) -> int:
    return min(pool, math.floor(fraction * pool + 0.5))


def _hour_group(record: _Input) -> tuple[str, str, str] | None:
    """Sensor/hour group of a valid hourly PM2.5 record, else None."""
    if record.envelope is None:
        return None
    start = utc_time(record.envelope["period_start"], "period_start")
    end = utc_time(record.envelope["period_end"], "period_end")
    if end - start != _HOUR or start != start.replace(minute=0, second=0, microsecond=0):
        return None
    return record.envelope["station_id"], record.envelope["sensor_id"], timestamp(start)


def _current_record(records: list[_Input]) -> _Input:
    return max(
        records,
        key=lambda r: (
            utc_time(r.envelope["collected_at"], "collected_at"),
            r.envelope.get("revision_hash") or "",
        ),
    )


def _can_lose_usable_value(record: _Input, units: frozenset[str]) -> bool:
    """Hand-written check that a null correction would remove a usable value."""
    envelope = record.envelope or {}
    value = envelope.get("value")
    return (
        _is_number(value)
        and value >= 0
        and envelope.get("unit") in units
        and not envelope.get("source_flags")
    )


def _plan_groups(records: list[_Input], settings: dict[str, Any]):
    """Choose sensor/hour groups before row mutation.

    Returns ``(omitted, corrected, reserved)``: omitted group -> its records,
    corrected group -> the record being corrected, and the reserved record numbers
    that row faults must leave alone.
    """
    scenarios, seed, fraction = settings["scenarios"], settings["seed"], settings["group_fraction"]
    units = CONCENTRATION_UNITS[settings["injector_version"]]
    groups: dict[tuple[str, str, str], list[_Input]] = {}
    for record in records:
        key = _hour_group(record)
        if key is not None:
            groups.setdefault(key, []).append(record)

    def ranked(keys, label):
        return sorted(keys, key=lambda k: (stable_score(seed, canonical_json(list(k)), label), k))

    omitted: dict[tuple[str, str, str], list[_Input]] = {}
    corrected: dict[tuple[str, str, str], _Input] = {}
    if "missing_interval" in scenarios:
        chosen = ranked(groups, "missing_interval")[: _select_count(fraction, len(groups))]
        omitted = {key: groups[key] for key in sorted(chosen)}
    if "correction_to_null" in scenarios:
        eligible = [
            key
            for key in groups
            if key not in omitted and _can_lose_usable_value(_current_record(groups[key]), units)
        ]
        chosen = ranked(eligible, "correction_to_null")[: _select_count(fraction, len(eligible))]
        corrected = {key: _current_record(groups[key]) for key in sorted(chosen)}
    reserved = {r.number for rows in omitted.values() for r in rows}
    reserved |= {r.number for key in corrected for r in groups[key]}
    return omitted, corrected, reserved


def _correction_envelope(record: _Input) -> dict[str, Any]:
    envelope = dict(record.envelope or {})
    collected = utc_time(envelope["collected_at"], "collected_at") + _CORRECTION_LAG
    envelope.update(
        value=None,
        collected_at=timestamp(collected),
        ingested_at=timestamp(collected + timedelta(minutes=1)),
    )
    return _with_refreshed_hash(envelope)


def _plan(records: list[_Input], settings: dict[str, Any]):
    """Return ``({record_number: (fault, mutation)}, ineligible_selected)``."""
    seed, faults, fraction = settings["seed"], settings["faults"], settings["fraction"]
    units = CONCENTRATION_UNITS[settings["injector_version"]]
    plan: dict[int, tuple[str, _Mutation]] = {}
    ineligible: list[dict[str, Any]] = []
    if not faults:
        return plan, ineligible

    def score(record, label):
        return stable_score(seed, f"{record.number}:{record.sha256}", label), record.number

    if settings["profile"] == "single-fault":
        fault = faults[0]
        pool = []
        for record in records:
            mutation = _plan_mutation(fault, record, seed, units)
            if mutation is not None:
                pool.append((record, mutation))
        pool.sort(key=lambda item: score(item[0], fault))
        for record, mutation in pool[: _select_count(fraction, len(pool))]:
            plan[record.number] = fault, mutation
        return plan, ineligible

    pool = sorted((r for r in records if r.envelope is not None), key=lambda r: score(r, "select"))
    pointer = 0
    for record in pool[: _select_count(fraction, len(pool))]:
        # Balanced round-robin; skip faults this record cannot carry.
        for offset in range(len(faults)):
            fault = faults[(pointer + offset) % len(faults)]
            mutation = _plan_mutation(fault, record, seed, units)
            if mutation is not None:
                plan[record.number] = fault, mutation
                pointer = (pointer + offset + 1) % len(faults)
                break
        else:
            ineligible.append({"record_number": record.number, "faults_tried": list(faults)})
    return plan, ineligible


def _derive_run_id(settings: dict[str, Any], snapshot_sha: str) -> str:
    if settings["run_id"]:
        return str(settings["run_id"])
    return "qr-" + digest(
        [
            snapshot_sha,
            settings["seed"],
            settings["profile"],
            settings["faults"],
            settings["fraction"],
            settings["scenarios"],
            settings["group_fraction"],
            settings["rule_version"],
            settings["injector_version"],
        ]
    )[:16]


# --- Public API -----------------------------------------------------------


def build_replay(
    snapshot_path: Path,
    output_directory: Path,
    *,
    config: Mapping[str, object],
) -> dict[str, object]:
    """Write deterministic processor-facing wrappers and a separate manifest.

    The snapshot is read-only and outputs are created exclusively, so an existing
    run is never overwritten. Control runs replay the snapshot bytes unchanged.
    """
    settings = _validate_config(config)
    snapshot_sha, records = _read_snapshot(snapshot_path)
    run_id = _derive_run_id(settings, snapshot_sha)
    seed = settings["seed"]
    omitted, corrected, reserved = _plan_groups(records, settings)
    plan, ineligible = _plan([r for r in records if r.number not in reserved], settings)
    omitted_numbers = {r.number for rows in omitted.values() for r in rows}
    correcting = {record.number: key for key, record in corrected.items()}

    newest_end = max(
        (utc_time(r.envelope["period_end"], "period_end") for r in records if r.envelope),
        default=None,
    )
    emitted: list[dict[str, Any]] = []
    delayed: list[dict[str, Any]] = []
    corrections: list[dict[str, Any]] = []
    for record in records:
        if record.number in omitted_numbers:
            continue  # missing_interval: no delivery at all for this sensor/hour
        fault, mutation = plan.get(record.number, (None, None))
        ref = {"snapshot_sha256": snapshot_sha, "record_number": record.number}
        payload = record.raw if mutation is None or mutation.payload is None else mutation.payload
        primary_id = digest([run_id, record.number, "primary"])[:32]
        key = record.envelope["station_id"] if record.envelope else None
        entry = {
            "delivery_id": primary_id,
            "role": "primary",
            "source_record_ref": ref,
            "original_key": key,
            "payload": payload,
            "original_sha256": record.sha256,
            "fault": None,
            "mutation": None,
            "duplicate_of": None,
            "expected": None,
        }
        if fault in _PAYLOAD_FAULTS:
            entry.update(fault=fault, mutation=mutation.params, expected=mutation.expected)
        if fault == "delayed_delivery":
            lag = (newest_end - utc_time(record.envelope["period_end"], "period_end")).total_seconds()
            entry.update(
                fault=fault,
                mutation={"released_after_newer_event_times": lag > 0, "event_time_lag_hours": lag / 3600},
                expected=mutation.expected,
            )
            delayed.append(entry)
        else:
            emitted.append(entry)
        if record.number in correcting:
            corrections.append(
                {
                    **entry,
                    "delivery_id": digest([run_id, record.number, "correction"])[:32],
                    "role": "correction",
                    "payload": _json_bytes(_correction_envelope(record)),
                    "fault": "correction_to_null",
                    "mutation": {"value": None, "collected_at_lag_hours": _CORRECTION_LAG.total_seconds() / 3600},
                    "duplicate_of": None,
                    "expected": {
                        **_expected(flags=["missing_value"]),
                        "supersedes": primary_id,
                        "removes_usable_contribution": True,
                    },
                }
            )
        if fault == "duplicate_delivery":
            emitted.append(
                {
                    **entry,
                    "delivery_id": digest([run_id, record.number, "intentional_duplicate"])[:32],
                    "role": "intentional_duplicate",
                    "fault": fault,
                    "mutation": {"identical_payload": True},
                    "duplicate_of": primary_id,
                    "expected": mutation.expected,
                }
            )
    emitted.extend(delayed)
    emitted.extend(corrections)  # a correction is released after everything it corrects

    wrappers, deliveries = [], []
    for index, entry in enumerate(emitted):
        wrappers.append(
            build_wrapper(
                run_id=run_id,
                delivery_id=entry["delivery_id"],
                original_topic=SOURCE_TOPIC,
                original_key=entry["original_key"],
                source_record_ref=entry["source_record_ref"],
                payload=entry["payload"],
                scheduled_release_index=index,
            )
        )
        deliveries.append(
            {
                "delivery_id": entry["delivery_id"],
                "release_index": index,
                "role": entry["role"],
                "source_record_ref": entry["source_record_ref"],
                "fault": entry["fault"],
                "mutation": entry["mutation"],
                "original_sha256": entry["original_sha256"],
                "emitted_sha256": sha256_hex(entry["payload"]),
                "duplicate_of": entry["duplicate_of"],
                "expected": entry["expected"],
            }
        )
    fault_counts: dict[str, int] = {}
    for delivery in deliveries:
        if delivery["fault"]:
            fault_counts[delivery["fault"]] = fault_counts.get(delivery["fault"], 0) + 1

    catalog = None
    if settings["catalog_path"]:
        catalog = {"sha256": sha256_hex(Path(settings["catalog_path"]).read_bytes())}
    manifest = validate_manifest(
        {
            "schema_version": MANIFEST_SCHEMA_VERSION,
            "run_id": run_id,
            "dataset_kind": EXPERIMENT_DATASET_KIND,
            "injector_version": settings["injector_version"],
            "seed": seed,
            "profile": settings["profile"],
            "rule_version": settings["rule_version"],
            "code_revision": settings["code_revision"],
            "snapshot": {"sha256": snapshot_sha, "file_name": Path(snapshot_path).name},
            "catalog": catalog,
            "policies": {"units": UNIT_POLICY_VERSION, "timezone": TIMEZONE_POLICY_VERSION},
            "config": {
                "fault_types": settings["faults"],
                "selected_input_fraction": settings["fraction"],
                "group_scenarios": settings["scenarios"],
                "group_fraction": settings["group_fraction"],
            },
            "input_record_count": len(records),
            "emitted_delivery_count": len(deliveries),
            "fault_counts": dict(sorted(fault_counts.items())),
            "ineligible_selected": ineligible,
            "omitted_records": [
                {
                    "scenario": "missing_interval",
                    "group": {"station_id": key[0], "sensor_id": key[1], "hour": key[2]},
                    "record_numbers": [r.number for r in rows],
                    "source_record_refs": [
                        {"snapshot_sha256": snapshot_sha, "record_number": r.number} for r in rows
                    ],
                }
                for key, rows in omitted.items()
            ],
            "deliveries": deliveries,
        }
    )

    output_directory = Path(output_directory)
    output_directory.mkdir(parents=True, exist_ok=True)
    replay_path, manifest_path = output_directory / REPLAY_FILE, output_directory / MANIFEST_FILE
    for path in (replay_path, manifest_path):
        if path.exists():
            raise FileExistsError(f"refusing to overwrite existing run output: {path}")
    replay_text = "".join(canonical_json(wrapper) + "\n" for wrapper in wrappers)
    with replay_path.open("x", encoding="utf-8") as handle:
        handle.write(replay_text)
    with manifest_path.open("x", encoding="utf-8") as handle:
        handle.write(json.dumps(manifest, indent=2, sort_keys=True, allow_nan=False) + "\n")
    return {
        "run_id": run_id,
        "replay_path": str(replay_path),
        "manifest_path": str(manifest_path),
        "replay_sha256": sha256_hex(replay_text.encode("utf-8")),
        "input_records": len(records),
        "emitted_deliveries": len(deliveries),
        "fault_counts": dict(sorted(fault_counts.items())),
        "ineligible_selected": len(ineligible),
        "omitted_groups": len(omitted),
        "omitted_records": len(omitted_numbers),
    }


PUBLICATIONS_DIR = "publications"
PUBLISH_ATTEMPTS = 3


def _ack_field(metadata: Any, name: str) -> Any:
    return metadata[name] if isinstance(metadata, Mapping) else getattr(metadata, name)


def _published_now() -> str:
    return timestamp(datetime.now(timezone.utc))


def publish_replay(
    run_path: Path,
    *,
    bootstrap_servers: str | None = None,
    topic: str = REPLAY_TOPIC,
    manager_factory: Any = None,
    clock: Any = _published_now,
    max_attempts: int = PUBLISH_ATTEMPTS,
) -> dict[str, Any]:
    """Publish a built run's wrappers to the experiment topic and log broker ACKs.

    ``run_path`` is a run directory (or its ``replay.jsonl``). Wrappers are sent in
    ``scheduled_release_index`` order, one at a time, each waiting for its ACK, so
    the release schedule holds per partition. ``published_at`` is stamped once per
    wrapper; a failed send is retried with the same bytes. Delivery IDs never
    change, so publishing the same run again is a repeated-publication experiment,
    not new data. Each call writes ``publications/publication-NNN.json`` (never
    overwritten); a failed call still writes its log before raising.

    The topic is created by ``kafka-init`` (compose), not here; only
    ``quality_replay*`` topics are accepted so live topics cannot be targeted.
    """
    if not isinstance(topic, str) or not topic.startswith(REPLAY_TOPIC):
        raise ValueError(f"replay wrappers may only be published to {REPLAY_TOPIC}* topics")
    if type(max_attempts) is not int or max_attempts < 1:
        raise ValueError("max_attempts must be a positive integer")
    run_path = Path(run_path)
    replay_path = run_path / REPLAY_FILE if run_path.is_dir() else run_path
    data = replay_path.read_bytes()
    wrappers = []
    for line_number, line in enumerate(data.split(b"\n"), 1):
        if not line.strip():
            continue
        try:
            wrappers.append(validate_wrapper(json.loads(line.decode("utf-8"))))
        except (ValueError, UnicodeDecodeError) as error:
            raise ValueError(f"replay line {line_number} is not a valid wrapper: {error}") from error
    if not wrappers:
        raise ValueError("replay contains no wrappers")
    run_ids = {w["run_id"] for w in wrappers}
    if len(run_ids) != 1:
        raise ValueError("replay mixes several run IDs")
    indexes = [w["scheduled_release_index"] for w in wrappers]
    if len(set(indexes)) != len(indexes):
        raise ValueError("scheduled_release_index values must be unique")
    if len({w["delivery_id"] for w in wrappers}) != len(wrappers):
        raise ValueError("replay contains duplicate delivery IDs")
    wrappers.sort(key=lambda w: w["scheduled_release_index"])

    log_directory = replay_path.parent / PUBLICATIONS_DIR
    log_directory.mkdir(exist_ok=True)
    number = len(list(log_directory.glob("publication-*.json"))) + 1
    log: dict[str, Any] = {
        "schema_version": 1,
        "publication": number,
        "run_id": next(iter(run_ids)),
        "replay_sha256": sha256_hex(data),
        "topic": topic,
        "started_at": clock(),
        "finished_at": None,
        "planned": len(wrappers),
        "acknowledged": 0,
        "retried_sends": 0,
        "error": None,
        "acks": [],
    }
    if manager_factory is None:
        from streaming.kafka import KafkaManager

        manager_factory = KafkaManager
    try:
        with manager_factory(bootstrap_servers) as manager:
            for wrapper in wrappers:
                message = validate_wrapper({**wrapper, "published_at": clock()})
                key = message["original_key"] or message["delivery_id"]
                for attempt in range(1, max_attempts + 1):
                    try:
                        metadata = manager.publish(topic, key=key, value=message)
                        break
                    except Exception as error:  # broker/transport failure: same bytes again
                        if attempt == max_attempts:
                            raise RuntimeError(
                                f"delivery {message['delivery_id']} failed after "
                                f"{attempt} attempts: {error}"
                            ) from error
                        log["retried_sends"] += 1
                log["acks"].append(
                    {
                        "delivery_id": message["delivery_id"],
                        "scheduled_release_index": message["scheduled_release_index"],
                        "published_at": message["published_at"],
                        "attempts": attempt,
                        "partition": _ack_field(metadata, "partition"),
                        "offset": _ack_field(metadata, "offset"),
                    }
                )
                log["acknowledged"] += 1
    except Exception as error:
        log["error"] = str(error)
        raise
    finally:
        log["finished_at"] = clock()
        path = log_directory / f"publication-{number:03d}.json"
        with path.open("x", encoding="utf-8") as handle:
            handle.write(json.dumps(log, indent=2, sort_keys=True) + "\n")
    return {key: log[key] for key in ("publication", "run_id", "topic", "planned", "acknowledged", "retried_sends")} | {
        "log_path": str(path)
    }


def _code_revision() -> str | None:
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=Path(__file__).resolve().parents[1],
            capture_output=True,
            text=True,
            timeout=10,
            check=True,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    return result.stdout.strip() or None


def main(argv: list[str] | None = None) -> int:
    """Run ``build`` or ``publish``."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    commands = parser.add_subparsers(dest="command", required=True)
    build = commands.add_parser("build", help="write replay wrappers and the evaluator manifest")
    build.add_argument("--config", type=Path, required=True, help="explicitly enabled quality JSON config")
    build.add_argument("--snapshot", type=Path, help="frozen JSONL (default: experiment.snapshot_path)")
    build.add_argument("--output-dir", type=Path, help="new run directory (default: experiment.output_directory)")
    publish = commands.add_parser("publish", help="send a built run's wrappers to the experiment Kafka topic")
    publish.add_argument("--run-dir", type=Path, required=True, help="directory written by build")
    publish.add_argument("--bootstrap-servers", help="default: KAFKA_BOOTSTRAP_SERVERS or 127.0.0.1:9092")
    publish.add_argument("--topic", default=REPLAY_TOPIC)
    args = parser.parse_args(argv)
    try:
        if args.command == "publish":
            summary = publish_replay(
                args.run_dir, bootstrap_servers=args.bootstrap_servers, topic=args.topic
            )
        else:
            config = json.loads(args.config.read_text(encoding="utf-8"))
            experiment = config.get("experiment") or {}
            snapshot = args.snapshot or experiment.get("snapshot_path")
            output = args.output_dir or experiment.get("output_directory")
            if not snapshot or not output:
                parser.error("--snapshot and --output-dir are required unless set in the config")
            config = {**config, "code_revision": config.get("code_revision") or _code_revision()}
            summary = build_replay(Path(snapshot), Path(output), config=config)
    except (OSError, ValueError, RuntimeError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 2
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
