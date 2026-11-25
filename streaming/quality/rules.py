"""Pure quality rules shared by local replay, future Spark speed and batch paths.

``evaluate_payload`` turns one received payload into one deterministic decision.
It repairs only representation (numeric strings, equivalent UTC offsets, supported
unit aliases) and never guesses identities, timestamps, flags or missing values.
Everything else is delegated to ``models`` and ``streaming.measurements`` so the
observation identity, revision ordering and flag semantics stay single-sourced.

Nothing here reads a manifest, a source snapshot or the network.
"""

import json
import math
import re
from collections.abc import Iterable, Mapping
from dataclasses import fields
from datetime import datetime
from typing import Any

from models import MeasurementEvent, digest, timestamp, utc_time
from streaming.measurements import parse_measurement
from streaming.quality.contracts import (
    REPAIR_ACTIONS,
    SUPPORTED_ORIGINAL_TOPICS,
    accepted_decision,
    quarantined_decision,
    sha256_hex,
)

RULE_VERSION = "rules-v1"
SUPPORTED_RULE_VERSIONS = frozenset({RULE_VERSION})

_TIME_FIELDS = ("period_start", "period_end", "collected_at", "ingested_at")
_REQUIRED_FIELDS = tuple(field.name for field in fields(MeasurementEvent))
# JSON number grammar only. Signs other than "-", bare/trailing decimal points,
# separators, hex, "NaN"/"Infinity" and empty text are ambiguous and not repaired.
_JSON_NUMBER = re.compile(r"-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?(?:[eE][+-]?[0-9]+)?")
_STRING_NUMBER_PADDING = " \t\r\n"
_UNIT_ACTIONS = {
    "µg/m³": "unit_alias_normalized",
    "μg/m³": "unit_alias_normalized",
    "mg/m3": "unit_converted_to_ug_m3",
}


class _Quarantine(Exception):
    def __init__(self, reason: str, detail: str):
        super().__init__(detail)
        self.reason = reason
        self.detail = detail


def _reject_constant(value: str) -> None:
    raise ValueError(f"nonstandard JSON number: {value}")


def _repair_value(value: Any) -> tuple[int | float | None, bool]:
    """Return ``(number_or_null, repaired)`` or quarantine an unparseable value.

    A string is repaired to exactly what ``json.loads`` would have produced for its
    trimmed literal, so a stringified baseline value round-trips to the same
    revision hash.
    """
    number: Any
    if value is None:
        return None, False
    if isinstance(value, str):
        text = value.strip(_STRING_NUMBER_PADDING)
        if _JSON_NUMBER.fullmatch(text) is None:
            raise _Quarantine("unparseable_value", "value is not an unambiguous number")
        try:
            number = json.loads(text)
        except ValueError as error:  # e.g. an integer beyond the digit limit
            raise _Quarantine("unparseable_value", str(error)) from error
        repaired = True
    elif type(value) in (int, float):
        number, repaired = value, False
    else:
        raise _Quarantine("unparseable_value", "value must be a number, string or null")
    try:
        finite = math.isfinite(number)
    except OverflowError:  # an integer too large for a float
        finite = False
    if not finite:
        raise _Quarantine("unparseable_value", "value must be finite")
    return number, repaired


def _normalized_timestamps(raw: Mapping[str, object]) -> bool:
    """Return whether any timestamp used a non-canonical UTC representation."""
    changed = False
    for field in _TIME_FIELDS:
        text = raw[field]
        if not isinstance(text, str):
            raise _Quarantine(
                "invalid_envelope", f"{field} must be a timezone-aware ISO timestamp"
            )
        try:
            parsed = datetime.fromisoformat(text)
        except ValueError as error:
            raise _Quarantine(
                "invalid_envelope", f"{field} must be a timezone-aware ISO timestamp"
            ) from error
        if parsed.tzinfo is None:
            raise _Quarantine("missing_timezone", f"{field} must include a timezone")
        changed = changed or timestamp(utc_time(text, field)) != text
    return changed


def evaluate_payload(
    payload: bytes, *, source_topic: str, rule_version: str
) -> dict[str, object]:
    """Parse, repair the representation of, and classify one received payload.

    Structural failures return a quarantine decision; quality problems (null or
    negative values, unsupported units, source flags, unsupported periods) are
    flags on an accepted decision so a later revision can still supersede an
    earlier usable one. Programming or configuration errors raise. Cross-record
    revision selection and aggregation belong to the caller; see
    ``select_current_decisions``.
    """
    if source_topic not in SUPPORTED_ORIGINAL_TOPICS:
        raise ValueError(f"unsupported source topic: {source_topic}")
    if rule_version not in SUPPORTED_RULE_VERSIONS:
        raise ValueError(f"unsupported rule version: {rule_version}")
    if not isinstance(payload, (bytes, bytearray)):
        raise TypeError("payload must be bytes")
    payload = bytes(payload)
    received = sha256_hex(payload)
    try:
        return _evaluate(payload, received, rule_version)
    except _Quarantine as quarantine:
        return quarantined_decision(
            rule_version=rule_version,
            received_sha256=received,
            reason=quarantine.reason,
            detail=quarantine.detail,
        )


def _evaluate(payload: bytes, received: str, rule_version: str) -> dict[str, object]:
    try:
        text = payload.decode("utf-8")
    except UnicodeDecodeError as error:
        raise _Quarantine("invalid_utf8", str(error)) from error
    try:
        raw = json.loads(text, parse_constant=_reject_constant)
    except (ValueError, RecursionError) as error:
        raise _Quarantine("invalid_json", str(error)) from error
    if not isinstance(raw, dict):
        raise _Quarantine("not_an_object", "event must be a JSON object")
    if raw.get("status") == "rejected":
        raise _Quarantine(
            "collector_rejected", str(raw.get("error", "invalid source data"))
        )
    missing = [name for name in _REQUIRED_FIELDS if name not in raw]
    if missing:
        raise _Quarantine(
            "missing_required_field", "missing required fields: " + ", ".join(sorted(missing))
        )
    pollutant = raw["pollutant"]
    other_pollutant = isinstance(pollutant, str) and pollutant.strip() and pollutant != "pm25"

    actions = set()
    value, repaired = _repair_value(raw["value"])
    if repaired:
        actions.add("numeric_string_parsed")
    if _normalized_timestamps(raw):
        actions.add("timestamp_normalized_to_utc")

    # Other pollutants share the envelope's structure but not the PM2.5 value
    # rules, so validate the structure as PM2.5 and keep their own identity.
    candidate = {**raw, "value": value, "pollutant": "pm25" if other_pollutant else pollutant}
    try:
        measurement = parse_measurement(candidate)
    except (ValueError, OverflowError) as error:
        raise _Quarantine("invalid_envelope", str(error)) from error

    if other_pollutant:
        event_id, revision_hash = _observation_hashes({**candidate, "pollutant": pollutant})
        flags = ["unsupported_pollutant"]
        if value is None:
            flags.append("missing_value")
        if raw["source_flags"]:
            flags.append("source_flagged")
        value_ug_m3 = None
    else:
        event_id, revision_hash = measurement.event_id, measurement.revision_hash
        flags, value_ug_m3 = list(measurement.quality_flags), measurement.value_ug_m3
        if value_ug_m3 is not None and raw["unit"] in _UNIT_ACTIONS:
            actions.add(_UNIT_ACTIONS[raw["unit"]])

    # Claimed hashes must match the content after representation repair; this
    # catches tampering without rejecting an exactly round-tripped repair.
    if "event_id" in raw and raw["event_id"] != event_id:
        raise _Quarantine("integrity_mismatch", "event_id does not match the payload")
    claimed = raw.get("revision_hash")
    if "revision_hash" in raw and claimed != revision_hash:
        raise _Quarantine(
            "integrity_mismatch", "revision_hash does not match the payload"
        )

    return accepted_decision(
        rule_version=rule_version,
        received_sha256=received,
        event_id=event_id,
        revision_hash=revision_hash,
        claimed_revision_hash=claimed if isinstance(claimed, str) else None,
        station_id=measurement.station_id,
        sensor_id=measurement.sensor_id,
        period_start=timestamp(measurement.period_start),
        period_end=timestamp(measurement.period_end),
        collected_at=timestamp(measurement.collected_at),
        ingested_at=timestamp(measurement.ingested_at),
        received_value=raw["value"],
        received_unit=raw["unit"],
        value_ug_m3=value_ug_m3,
        quality_flags=flags,
        repair_actions=[a for a in REPAIR_ACTIONS if a in actions],
    )


def _observation_hashes(envelope: Mapping[str, Any]) -> tuple[str, str]:
    """Identity and revision hashes with the same fields as ``MeasurementEvent``.

    Used for pollutants that ``MeasurementEvent`` does not model, so their claimed
    hashes are checked exactly like the live path checks them.
    """
    identity = {
        "source": envelope["source"],
        "station_id": envelope["station_id"],
        "sensor_id": envelope["sensor_id"],
        "pollutant": envelope["pollutant"],
        "period_start": timestamp(utc_time(envelope["period_start"], "period_start")),
        "period_end": timestamp(utc_time(envelope["period_end"], "period_end")),
    }
    revision = {
        **identity,
        "value": envelope["value"],
        "unit": envelope["unit"],
        "source_flags": list(envelope["source_flags"]),
        "provider_payload": envelope["provider_payload"],
    }
    return digest(identity), digest(revision)


def select_current_decisions(
    decisions: Iterable[Mapping[str, object]],
) -> list[dict[str, object]]:
    """Select the current accepted revision per logical observation.

    Mirrors ``streaming.measurements.current_measurements``: greatest
    ``(collected_at, revision_hash)``, then ``ingested_at`` to pick the
    representative delivery. Selection happens *before* usability filtering, so a
    newer null/negative/flagged revision removes an older usable value. Quarantined
    decisions have no trustworthy identity and never retract anything.
    """
    latest: dict[str, tuple[tuple, dict[str, object]]] = {}
    for decision in decisions:
        if decision["disposition"] != "accepted":
            continue
        event_id = str(decision["event_id"])
        order = (
            utc_time(decision["collected_at"], "collected_at"),
            decision["revision_hash"],
            utc_time(decision["ingested_at"], "ingested_at"),
        )
        previous = latest.get(event_id)
        if previous is None or order > previous[0]:
            latest[event_id] = order, dict(decision)
    return [latest[key][1] for key in sorted(latest)]
