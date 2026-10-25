"""Shared data contracts for collectors, processing, and future serving code.

MeasurementEvent validates the source envelope. Measurement and HourlyMetric
describe processing outputs. Quality decisions belong in streaming/measurements.py.
"""

from dataclasses import dataclass, fields
from datetime import datetime, timezone
import hashlib
import json
import math
from typing import Literal, TypeAlias


JSONScalar: TypeAlias = str | int | float | bool | None
JSONValue: TypeAlias = JSONScalar | list["JSONValue"] | dict[str, "JSONValue"]


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
class MeasurementEvent:
    """Version 1 source envelope; negative/missing values remain representable.

    Construct from a JSON object with from_dict(), or pass typed values directly.
    Both paths enforce the same structural and temporal constraints.
    """

    schema_version: int
    source: str
    station_id: str
    sensor_id: str
    pollutant: Literal["pm25"]
    period_start: datetime
    period_end: datetime
    collected_at: datetime
    ingested_at: datetime
    value: float | int | None
    unit: str
    source_flags: tuple[str, ...]
    provider_payload: dict[str, JSONValue]

    def __post_init__(self) -> None:
        if type(self.schema_version) is not int or self.schema_version != 1:
            raise ValueError("schema_version must be 1")
        for field in ("source", "station_id", "sensor_id", "pollutant", "unit"):
            value = getattr(self, field)
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"{field} must be a nonempty string")
        if not self.station_id.startswith(self.source + ":"):
            raise ValueError("station_id must be qualified with source:")
        if self.pollutant != "pm25":
            raise ValueError("only pm25 is supported initially")
        if not isinstance(self.provider_payload, dict):
            raise ValueError("provider_payload must retain the original source object")
        try:
            canonical_json(self.provider_payload)
        except (TypeError, ValueError, OverflowError) as error:
            raise ValueError("provider_payload must contain finite JSON values") from error
        if not isinstance(self.source_flags, tuple) or any(
            not isinstance(flag, str) for flag in self.source_flags
        ):
            raise ValueError("source_flags must be a tuple of strings")
        if self.value is not None:
            if type(self.value) not in (int, float):
                raise ValueError("value must be a finite number or null")
            try:
                finite = math.isfinite(self.value)
            except OverflowError:
                finite = False
            if not finite:
                raise ValueError("value must be a finite number or null")
        for field in ("period_start", "period_end", "collected_at", "ingested_at"):
            value = getattr(self, field)
            if not isinstance(value, datetime) or value.utcoffset() is None:
                raise ValueError(f"{field} must be a timezone-aware datetime")
            object.__setattr__(self, field, value.astimezone(timezone.utc))
        if self.period_end <= self.period_start:
            raise ValueError("period_end must follow period_start")
        if self.collected_at < self.period_end or self.ingested_at < self.collected_at:
            raise ValueError("require period_end <= collected_at <= ingested_at")

    @classmethod
    def from_dict(cls, raw: object) -> "MeasurementEvent":
        if not isinstance(raw, dict):
            raise ValueError("record must be a JSON object")
        missing = {field.name for field in fields(cls)} - raw.keys()
        if missing:
            raise ValueError("missing required fields: " + ", ".join(sorted(missing)))
        if not isinstance(raw["source_flags"], list):
            raise ValueError("source_flags must be an explicit list of strings")
        values = {field.name: raw[field.name] for field in fields(cls)}
        for field in ("period_start", "period_end", "collected_at", "ingested_at"):
            values[field] = utc_time(values[field], field)
        values["source_flags"] = tuple(values["source_flags"])
        return cls(**values)

    def as_dict(self) -> dict[str, JSONValue]:
        """Serialize the defined envelope fields using UTC timestamps."""
        return {
            "schema_version": self.schema_version,
            "source": self.source,
            "station_id": self.station_id,
            "sensor_id": self.sensor_id,
            "pollutant": self.pollutant,
            "period_start": timestamp(self.period_start),
            "period_end": timestamp(self.period_end),
            "collected_at": timestamp(self.collected_at),
            "ingested_at": timestamp(self.ingested_at),
            "value": self.value,
            "unit": self.unit,
            "source_flags": list(self.source_flags),
            "provider_payload": self.provider_payload,
        }

    def _identity(self) -> dict[str, str]:
        return {
            "source": self.source,
            "station_id": self.station_id,
            "sensor_id": self.sensor_id,
            "pollutant": self.pollutant,
            "period_start": timestamp(self.period_start),
            "period_end": timestamp(self.period_end),
        }

    @property
    def event_id(self) -> str:
        return digest(self._identity())

    @property
    def revision_hash(self) -> str:
        return digest({
            **self._identity(),
            "value": self.value,
            "unit": self.unit,
            "source_flags": list(self.source_flags),
            "provider_payload": self.provider_payload,
        })


@dataclass(frozen=True)
class Measurement:
    """A processed revision, including quality and its original envelope."""

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
    raw: dict[str, JSONValue]

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


@dataclass(frozen=True)
class HourlyMetric:
    """Provisional mean of usable hourly sensor readings, without coverage claims."""

    station_id: str
    hour: datetime
    mean: float
    sample_count: int
    pollutant: Literal["pm25"] = "pm25"
    unit: Literal["ug/m3"] = "ug/m3"
    status: Literal["provisional"] = "provisional"

    def as_dict(self) -> dict:
        return {
            "station_id": self.station_id,
            "pollutant": self.pollutant,
            "hour": timestamp(self.hour),
            "mean": self.mean,
            "unit": self.unit,
            "sample_count": self.sample_count,
            "status": self.status,
        }
