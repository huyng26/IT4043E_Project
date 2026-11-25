"""Versioned replay-wrapper, quality-decision and evaluator-manifest contracts.

Three shapes live here and must stay separate:

* the **wrapper** is processor-facing transport for one emitted delivery. It may
  carry malformed inner bytes but never original values, fault labels or
  expected outcomes;
* the **decision** is the deterministic, serializable outcome of applying the
  shared rules to one received payload;
* the **manifest** is evaluator-only. Executors and quality rules must never
  read it (see ``streaming.quality_evaluation``).

Source envelopes (schema version 1) and observation/revision ordering are owned
by ``models`` and ``streaming.measurements`` and are not redefined here.
"""

import base64
import binascii
import hashlib
import re
from collections.abc import Mapping, Sequence

from models import utc_time

REPLAY_TOPIC = "quality_replay_raw"
EXPERIMENT_DATASET_KIND = "controlled_corruption"

WRAPPER_SCHEMA_VERSION = 1
DECISION_SCHEMA_VERSION = 1
MANIFEST_SCHEMA_VERSION = 1

# The first experiment replays detailed PM2.5 events only.
SUPPORTED_ORIGINAL_TOPICS = ("air_quality_raw",)

WRAPPER_FIELDS = (
    "schema_version",
    "run_id",
    "delivery_id",
    "dataset_kind",
    "original_topic",
    "original_key",
    "source_record_ref",
    "payload_base64",
    "payload_sha256",
    "scheduled_release_index",
    "published_at",
)
SOURCE_RECORD_REF_FIELDS = ("snapshot_sha256", "record_number")

DISPOSITIONS = ("accepted", "quarantined")

# Structural failures. A quarantined delivery has no trustworthy identity, so it
# cannot retract an existing observation.
QUARANTINE_REASONS = (
    "invalid_utf8",
    "invalid_json",
    "not_an_object",
    "collector_rejected",
    "missing_required_field",
    "missing_timezone",
    "unparseable_value",
    "integrity_mismatch",
    "invalid_envelope",
)

# Representation repairs and normalizations; every one is recorded, in this order.
REPAIR_ACTIONS = (
    "numeric_string_parsed",
    "timestamp_normalized_to_utc",
    "unit_alias_normalized",
    "unit_converted_to_ug_m3",
)

DECISION_FIELDS = (
    "schema_version",
    "rule_version",
    "disposition",
    "reason",
    "detail",
    "received_sha256",
    "event_id",
    "revision_hash",
    "claimed_revision_hash",
    "station_id",
    "sensor_id",
    "period_start",
    "period_end",
    "collected_at",
    "ingested_at",
    "received_value",
    "received_unit",
    "value_ug_m3",
    "quality_flags",
    "usable",
    "repair_actions",
    "warnings",
)
_IDENTITY_FIELDS = (
    "event_id",
    "revision_hash",
    "station_id",
    "sensor_id",
    "period_start",
    "period_end",
    "collected_at",
    "ingested_at",
)

MANIFEST_REQUIRED_KEYS = (
    "schema_version",
    "run_id",
    "dataset_kind",
    "injector_version",
    "seed",
    "profile",
    "rule_version",
    "code_revision",
    "snapshot",
    "catalog",
    "policies",
    "config",
    "input_record_count",
    "emitted_delivery_count",
    "fault_counts",
    "ineligible_selected",
    "omitted_records",
    "deliveries",
)
MANIFEST_DELIVERY_KEYS = (
    "delivery_id",
    "release_index",
    "role",
    "source_record_ref",
    "fault",
    "mutation",
    "original_sha256",
    "emitted_sha256",
    "duplicate_of",
    "expected",
)

_SHA256 = re.compile(r"[0-9a-f]{64}")


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _is_int(value: object) -> bool:
    return type(value) is int


def _nonempty_string(value: object) -> bool:
    return isinstance(value, str) and bool(value.strip())


def _is_sha256(value: object) -> bool:
    return isinstance(value, str) and _SHA256.fullmatch(value) is not None


# --- Replay wrapper -------------------------------------------------------


def build_wrapper(
    *,
    run_id: str,
    delivery_id: str,
    original_topic: str,
    original_key: str | None,
    source_record_ref: Mapping[str, object],
    payload: bytes,
    scheduled_release_index: int,
) -> dict[str, object]:
    """Wrap exact payload bytes for one planned delivery; ``published_at`` is unset."""
    wrapper = {
        "schema_version": WRAPPER_SCHEMA_VERSION,
        "run_id": run_id,
        "delivery_id": delivery_id,
        "dataset_kind": EXPERIMENT_DATASET_KIND,
        "original_topic": original_topic,
        "original_key": original_key,
        "source_record_ref": dict(source_record_ref),
        "payload_base64": base64.b64encode(payload).decode("ascii"),
        "payload_sha256": sha256_hex(payload),
        "scheduled_release_index": scheduled_release_index,
        "published_at": None,
    }
    return validate_wrapper(wrapper)


def validate_wrapper(wrapper: object) -> dict[str, object]:
    """Return the wrapper if it is exactly the documented transport shape.

    Unknown fields are rejected so original values, fault labels or repair hints
    cannot be smuggled to a processor. The payload bytes are not interpreted.
    """
    if not isinstance(wrapper, dict):
        raise ValueError("wrapper must be a JSON object")
    missing = set(WRAPPER_FIELDS) - wrapper.keys()
    if missing:
        raise ValueError("missing wrapper fields: " + ", ".join(sorted(missing)))
    unknown = wrapper.keys() - set(WRAPPER_FIELDS)
    if unknown:
        raise ValueError(
            "unexpected wrapper fields: " + ", ".join(sorted(map(str, unknown)))
        )
    if not _is_int(wrapper["schema_version"]) or (
        wrapper["schema_version"] != WRAPPER_SCHEMA_VERSION
    ):
        raise ValueError(f"wrapper schema_version must be {WRAPPER_SCHEMA_VERSION}")
    for field in ("run_id", "delivery_id"):
        if not _nonempty_string(wrapper[field]):
            raise ValueError(f"{field} must be a nonempty string")
    if wrapper["dataset_kind"] != EXPERIMENT_DATASET_KIND:
        raise ValueError(f"dataset_kind must be {EXPERIMENT_DATASET_KIND}")
    if wrapper["original_topic"] not in SUPPORTED_ORIGINAL_TOPICS:
        raise ValueError("original_topic is not a supported source topic")
    if wrapper["original_key"] is not None and not isinstance(
        wrapper["original_key"], str
    ):
        raise ValueError("original_key must be a string or null")
    ref = wrapper["source_record_ref"]
    if not isinstance(ref, dict) or set(ref) != set(SOURCE_RECORD_REF_FIELDS):
        raise ValueError(
            "source_record_ref must contain exactly "
            + ", ".join(SOURCE_RECORD_REF_FIELDS)
        )
    if not _is_sha256(ref["snapshot_sha256"]):
        raise ValueError("source_record_ref.snapshot_sha256 must be a SHA-256 digest")
    if not _is_int(ref["record_number"]) or ref["record_number"] < 1:
        raise ValueError("source_record_ref.record_number must be a positive integer")
    payload = _decode_base64(wrapper["payload_base64"])
    if not _is_sha256(wrapper["payload_sha256"]) or (
        wrapper["payload_sha256"] != sha256_hex(payload)
    ):
        raise ValueError("payload_sha256 does not match the decoded payload")
    if not _is_int(wrapper["scheduled_release_index"]) or (
        wrapper["scheduled_release_index"] < 0
    ):
        raise ValueError("scheduled_release_index must be a nonnegative integer")
    if wrapper["published_at"] is not None:
        utc_time(wrapper["published_at"], "published_at")
    return wrapper


def _decode_base64(value: object) -> bytes:
    if not isinstance(value, str):
        raise ValueError("payload_base64 must be a base64 string")
    try:
        return base64.b64decode(value, validate=True)
    except (binascii.Error, ValueError) as error:
        raise ValueError("payload_base64 is not valid base64") from error


def decode_wrapper_payload(wrapper: object) -> bytes:
    """Validate the wrapper and return the exact inner bytes (possibly malformed)."""
    return _decode_base64(validate_wrapper(wrapper)["payload_base64"])


# --- Quality decisions ----------------------------------------------------


def experiment_decision_key(
    run_id: str, delivery_id: str, rule_version: str
) -> tuple[str, str, str]:
    """Retry-safe key for a planned experiment delivery."""
    return run_id, delivery_id, rule_version


def live_decision_key(
    topic: str, partition: int, offset: int, rule_version: str
) -> tuple[str, int, int, str]:
    """Retry-safe key for a live Kafka transport record."""
    return topic, partition, offset, rule_version


def quarantined_decision(
    *, rule_version: str, received_sha256: str, reason: str, detail: str
) -> dict[str, object]:
    """Terminal decision for input without a trustworthy identity."""
    if reason not in QUARANTINE_REASONS:
        raise ValueError(f"unknown quarantine reason: {reason}")
    decision: dict[str, object] = {field: None for field in DECISION_FIELDS}
    decision.update(
        schema_version=DECISION_SCHEMA_VERSION,
        rule_version=rule_version,
        disposition="quarantined",
        reason=reason,
        detail=detail,
        received_sha256=received_sha256,
        quality_flags=[],
        usable=False,
        repair_actions=[],
        warnings=[],
    )
    return decision


def accepted_decision(
    *,
    rule_version: str,
    received_sha256: str,
    event_id: str,
    revision_hash: str,
    claimed_revision_hash: str | None,
    station_id: str,
    sensor_id: str,
    period_start: str,
    period_end: str,
    collected_at: str,
    ingested_at: str,
    received_value: object,
    received_unit: str,
    value_ug_m3: float | None,
    quality_flags: Sequence[str],
    repair_actions: Sequence[str],
) -> dict[str, object]:
    """Terminal decision for an identifiable revision; flags do not quarantine it."""
    decision = {
        "schema_version": DECISION_SCHEMA_VERSION,
        "rule_version": rule_version,
        "disposition": "accepted",
        "reason": None,
        "detail": None,
        "received_sha256": received_sha256,
        "event_id": event_id,
        "revision_hash": revision_hash,
        "claimed_revision_hash": claimed_revision_hash,
        "station_id": station_id,
        "sensor_id": sensor_id,
        "period_start": period_start,
        "period_end": period_end,
        "collected_at": collected_at,
        "ingested_at": ingested_at,
        "received_value": received_value,
        "received_unit": received_unit,
        "value_ug_m3": value_ug_m3,
        "quality_flags": list(quality_flags),
        "usable": not quality_flags,
        "repair_actions": list(repair_actions),
        "warnings": [],
    }
    return validate_decision(decision)


def validate_decision(decision: object) -> dict[str, object]:
    """Check the decision shape and the invariants other components rely on."""
    if not isinstance(decision, dict):
        raise ValueError("decision must be an object")
    if set(decision) != set(DECISION_FIELDS):
        raise ValueError("decision fields do not match the contract")
    if decision["schema_version"] != DECISION_SCHEMA_VERSION:
        raise ValueError("unsupported decision schema_version")
    if not _nonempty_string(decision["rule_version"]):
        raise ValueError("rule_version must be a nonempty string")
    if not _is_sha256(decision["received_sha256"]):
        raise ValueError("received_sha256 must be a SHA-256 digest")
    for field in ("quality_flags", "repair_actions", "warnings"):
        values = decision[field]
        if not isinstance(values, list) or any(not isinstance(v, str) for v in values):
            raise ValueError(f"{field} must be a list of strings")
    if any(action not in REPAIR_ACTIONS for action in decision["repair_actions"]):
        raise ValueError("unknown repair action")
    if not isinstance(decision["usable"], bool):
        raise ValueError("usable must be a boolean")
    disposition = decision["disposition"]
    if disposition == "accepted":
        if decision["reason"] is not None:
            raise ValueError("accepted decisions have no quarantine reason")
        if any(not _nonempty_string(decision[f]) for f in _IDENTITY_FIELDS):
            raise ValueError("accepted decisions require a complete identity")
        if decision["usable"] != (not decision["quality_flags"]):
            raise ValueError("usable must equal the absence of quality flags")
    elif disposition == "quarantined":
        if decision["reason"] not in QUARANTINE_REASONS:
            raise ValueError("quarantined decisions require a known reason")
        if any(decision[f] is not None for f in _IDENTITY_FIELDS):
            raise ValueError("quarantined decisions must not carry an identity")
        if decision["usable"] or decision["quality_flags"] or decision["repair_actions"]:
            raise ValueError("quarantined decisions have no usable value or repairs")
    else:
        raise ValueError(f"disposition must be one of {DISPOSITIONS}")
    return decision


# --- Evaluator manifest ---------------------------------------------------


def validate_manifest(manifest: object) -> dict[str, object]:
    """Check the evaluator manifest's top-level and per-delivery shape."""
    if not isinstance(manifest, dict):
        raise ValueError("manifest must be a JSON object")
    missing = set(MANIFEST_REQUIRED_KEYS) - manifest.keys()
    if missing:
        raise ValueError("missing manifest keys: " + ", ".join(sorted(missing)))
    if manifest["schema_version"] != MANIFEST_SCHEMA_VERSION:
        raise ValueError("unsupported manifest schema_version")
    if manifest["dataset_kind"] != EXPERIMENT_DATASET_KIND:
        raise ValueError(f"dataset_kind must be {EXPERIMENT_DATASET_KIND}")
    deliveries = manifest["deliveries"]
    if not isinstance(deliveries, list):
        raise ValueError("deliveries must be a list")
    if manifest["emitted_delivery_count"] != len(deliveries):
        raise ValueError("emitted_delivery_count does not match deliveries")
    seen = set()
    for index, delivery in enumerate(deliveries):
        if not isinstance(delivery, dict) or set(delivery) != set(MANIFEST_DELIVERY_KEYS):
            raise ValueError(f"deliveries[{index}] does not match the contract")
        if delivery["delivery_id"] in seen:
            raise ValueError("delivery IDs must be unique")
        seen.add(delivery["delivery_id"])
        if delivery["release_index"] != index:
            raise ValueError("deliveries must be listed in release order")
    return manifest
