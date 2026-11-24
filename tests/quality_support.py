"""Deterministic test inputs for the quality modules (not a test module)."""

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

from models import MeasurementEvent, canonical_json, timestamp

START = datetime(2026, 1, 1, tzinfo=timezone.utc)


def envelope(
    *,
    station: int = 1,
    sensor: int = 1,
    hour: int = 0,
    value: float | None = 40,
    unit: str = "ug/m3",
    hashes: bool = True,
    **changes,
):
    """A valid PM2.5 envelope; hashes are computed unless ``hashes`` is false."""
    period_start = START + timedelta(hours=hour)
    result = {
        "schema_version": 1,
        "source": "demo",
        "station_id": f"demo:station-{station}",
        "sensor_id": f"sensor-{sensor}",
        "pollutant": "pm25",
        "period_start": timestamp(period_start),
        "period_end": timestamp(period_start + timedelta(hours=1)),
        "collected_at": timestamp(period_start + timedelta(hours=1, minutes=5)),
        "ingested_at": timestamp(period_start + timedelta(hours=1, minutes=6)),
        "value": value,
        "unit": unit,
        "source_flags": [],
        "provider_payload": {"value": value, "synthetic": True},
    }
    result.update(changes)
    if hashes:
        event = MeasurementEvent.from_dict(result)
        result.update(event_id=event.event_id, revision_hash=event.revision_hash)
    return result


def encode(value) -> bytes:
    return canonical_json(value).encode("utf-8")


def write_snapshot(path: Path, *, stations=3, hours=24) -> list[dict]:
    """Write a clean baseline with varied positive values and one zero per sensor."""
    records = []
    for station in range(1, stations + 1):
        for hour in range(hours):
            value = 0 if hour == 5 else 10 + station * 7 + (hour * 3) % 29 + hour / 4
            records.append(
                envelope(station=station, sensor=station, hour=hour, value=value)
            )
    records.sort(key=lambda r: (r["period_start"], r["station_id"]))
    path.write_text(
        "".join(json.dumps(record) + "\n" for record in records), encoding="utf-8"
    )
    return records


def write_catalog(path: Path, *, stations=3, cadence=3600, **extra) -> None:
    sensors = [
        {
            "station_id": f"demo:station-{n}",
            "sensor_id": f"sensor-{n}",
            "expected_period_seconds": cadence,
            "active_from": None,
            "active_to": None,
            **extra,
        }
        for n in range(1, stations + 1)
    ]
    path.write_text(
        json.dumps({"schema_version": 1, "sensors": sensors}), encoding="utf-8"
    )
