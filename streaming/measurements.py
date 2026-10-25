"""Initial PM2.5 rules, independent of infrastructure and network access."""

from datetime import timedelta
import math
from typing import cast

from models import HourlyMetric, JSONValue, Measurement, MeasurementEvent


def parse_measurement(raw: object) -> Measurement:
    """Validate the shared input model, then apply PM2.5 quality rules."""
    event = MeasurementEvent.from_dict(raw)
    quality = []
    factors = {"ug/m3": 1.0, "µg/m³": 1.0, "μg/m³": 1.0, "mg/m3": 1000.0}
    factor = factors.get(event.unit)
    if factor is None:
        quality.append("unsupported_unit")
    if event.value is None:
        quality.append("missing_value")
    elif event.value < 0:
        quality.append("negative_concentration")
    if event.source_flags:
        quality.append("source_flagged")
    if event.period_end - event.period_start != timedelta(hours=1) or (
        event.period_start != event.period_start.replace(minute=0, second=0, microsecond=0)
    ):
        quality.append("unsupported_reporting_period")
    normalized = None if event.value is None or factor is None else event.value * factor
    if normalized is not None and not math.isfinite(normalized):
        quality.append("nonfinite_concentration")
        normalized = None
    return Measurement(
        event_id=event.event_id,
        revision_hash=event.revision_hash,
        station_id=event.station_id,
        sensor_id=event.sensor_id,
        period_start=event.period_start,
        period_end=event.period_end,
        collected_at=event.collected_at,
        ingested_at=event.ingested_at,
        value_ug_m3=normalized,
        quality_flags=tuple(quality),
        raw=cast(dict[str, JSONValue], raw),
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


def hourly_metrics(current: list[Measurement]) -> list[HourlyMetric]:
    """Mean of usable hourly sensor readings; no coverage or AQI claim."""
    groups = {}
    for measurement in current:
        if not measurement.quality_flags:
            key = measurement.station_id, measurement.period_start
            groups.setdefault(key, []).append(measurement.value_ug_m3)
    return [
        HourlyMetric(
            station_id=station,
            hour=hour,
            mean=math.fsum(value / len(values) for value in values),
            sample_count=len(values),
        )
        for (station, hour), values in sorted(groups.items())
    ]
