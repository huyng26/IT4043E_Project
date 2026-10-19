"""Initial PM2.5 rules, independent of infrastructure and network access."""

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import hashlib
import json
import math


def canonical_json(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def digest(value: object) -> str:
    return hashlib.sha256(canonical_json(value).encode()).hexdigest()


def utc_time(value: object, field: str) -> datetime:
    if not isinstance(value, str):
        raise ValueError(f"{field} must be a timezone-aware ISO timestamp")
    try:
        result = datetime.fromisoformat(value)
    except ValueError as error:
        raise ValueError(f"{field} must be a timezone-aware ISO timestamp") from error
    if result.tzinfo is None:
        raise ValueError(f"{field} must include a timezone")
    return result.astimezone(timezone.utc)


def timestamp(value: datetime) -> str:
    return value.isoformat().replace("+00:00", "Z")


@dataclass(frozen=True)
class Measurement:
    event_id: str
    revision_hash: str
    station_id: str
    sensor_id: str
    period_start: datetime
    period_end: datetime
    collected_at: datetime
    ingested_at: datetime
    value_ug_m3: float | None
    quality_flags: tuple[str, ...]
    raw: dict

    @property
    def order(self) -> tuple[datetime, str]:
        # Replays must retain the original collected_at, not their replay time.
        return self.collected_at, self.revision_hash

    def as_dict(self) -> dict:
        return {
            "event_id": self.event_id,
            "revision_hash": self.revision_hash,
            "value_ug_m3": self.value_ug_m3,
            "quality_flags": list(self.quality_flags),
            "raw": self.raw,
        }


def parse_measurement(raw: object) -> Measurement:
    """Parse the internal demo envelope, not an OpenAQ API response."""
    if not isinstance(raw, dict):
        raise ValueError("record must be a JSON object")
    if type(raw.get("schema_version")) is not int or raw["schema_version"] != 1:
        raise ValueError("schema_version must be 1")
    for field in ("source", "station_id", "sensor_id", "pollutant", "unit"):
        if not isinstance(raw.get(field), str) or not raw[field].strip():
            raise ValueError(f"{field} must be a nonempty string")
    if not raw["station_id"].startswith(raw["source"] + ":"):
        raise ValueError("station_id must be qualified with source:")
    if raw["pollutant"] != "pm25":
        raise ValueError("only pm25 is supported initially")
    if not isinstance(raw.get("provider_payload"), dict):
        raise ValueError("provider_payload must retain the original source object")
    flags = raw.get("source_flags")
    if not isinstance(flags, list) or any(not isinstance(flag, str) for flag in flags):
        raise ValueError("source_flags must be an explicit list of strings")
    if "value" not in raw:
        raise ValueError("value is required; use null for missing readings")
    value = raw["value"]
    if value is not None and (
        type(value) not in (int, float) or not math.isfinite(value)
    ):
        raise ValueError("value must be a finite number or null")
    start = utc_time(raw.get("period_start"), "period_start")
    end = utc_time(raw.get("period_end"), "period_end")
    collected = utc_time(raw.get("collected_at"), "collected_at")
    ingested = utc_time(raw.get("ingested_at"), "ingested_at")
    if end <= start:
        raise ValueError("period_end must follow period_start")
    if collected < end or ingested < collected:
        raise ValueError("require period_end <= collected_at <= ingested_at")

    identity = {
        field: raw[field] for field in ("source", "station_id", "sensor_id", "pollutant")
    }
    identity.update(period_start=timestamp(start), period_end=timestamp(end))
    # Collection and ingestion times describe delivery, not a payload revision.
    revision = {
        **identity,
        "value": value,
        "unit": raw["unit"],
        "source_flags": flags,
        "provider_payload": raw["provider_payload"],
    }
    quality = []
    factors = {"ug/m3": 1.0, "µg/m³": 1.0, "μg/m³": 1.0, "mg/m3": 1000.0}
    factor = factors.get(raw["unit"])
    if factor is None:
        quality.append("unsupported_unit")
    if value is None:
        quality.append("missing_value")
    elif value < 0:
        quality.append("negative_concentration")
    if flags:
        quality.append("source_flagged")
    if end - start != timedelta(hours=1) or start != start.replace(
        minute=0, second=0, microsecond=0
    ):
        quality.append("unsupported_reporting_period")
    normalized = None if value is None or factor is None else value * factor
    if normalized is not None and not math.isfinite(normalized):
        quality.append("nonfinite_concentration")
        normalized = None
    return Measurement(
        digest(identity), digest(revision), raw["station_id"], raw["sensor_id"],
        start, end, collected, ingested, normalized, tuple(quality), raw,
    )


def current_measurements(measurements: list[Measurement]) -> list[Measurement]:
    latest = {}
    for measurement in measurements:
        previous = latest.get(measurement.event_id)
        if previous is None or (measurement.order, measurement.ingested_at) > (
            previous.order, previous.ingested_at
        ):
            latest[measurement.event_id] = measurement
    return [latest[key] for key in sorted(latest)]


def hourly_metrics(current: list[Measurement]) -> list[dict]:
    """Mean of usable hourly sensor readings; no coverage or AQI claim."""
    groups = {}
    for measurement in current:
        if not measurement.quality_flags:
            key = measurement.station_id, measurement.period_start
            groups.setdefault(key, []).append(measurement.value_ug_m3)
    return [
        {
            "station_id": station,
            "pollutant": "pm25",
            "hour": timestamp(hour),
            "mean": math.fsum(value / len(values) for value in values),
            "unit": "ug/m3",
            "sample_count": len(values),
            "status": "provisional",
        }
        for (station, hour), values in sorted(groups.items())
    ]
