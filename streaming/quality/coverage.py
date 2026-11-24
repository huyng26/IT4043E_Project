"""Cadence-aware sensor-hour coverage after current-revision selection.

Expectations come only from a known inventory and cadence, never from a fault
manifest. Where no denominator is known, completeness is ``None`` rather than a
claim of full coverage. Duplicate deliveries cannot improve coverage because
observations are compared as distinct sensor-hours.

Catalog file (frozen alongside a snapshot)::

    {"schema_version": 1, "sensors": [
        {"station_id": "openaq:1", "sensor_id": "10",
         "expected_period_seconds": 3600,
         "active_from": null, "active_to": null}]}

``expected_period_seconds: null`` means unknown cadence and yields no expected
hours. Only hourly cadence is supported until sub-hourly processing rules exist.
"""

from collections.abc import Iterable, Mapping
from datetime import datetime, timedelta
from typing import Any

from models import timestamp, utc_time

CATALOG_SCHEMA_VERSION = 1
SUPPORTED_CADENCE_SECONDS = 3600
DEFAULT_MAX_EXPECTED_SENSOR_HOURS = 1_000_000
DEFAULT_MAX_EXAMPLES = 20

_HOUR = timedelta(hours=1)
_CATALOG_SENSOR_FIELDS = (
    "station_id",
    "sensor_id",
    "expected_period_seconds",
    "active_from",
    "active_to",
)


def _instant(value: object, field: str) -> datetime:
    return value if isinstance(value, datetime) else utc_time(value, field)


def parse_catalog(catalog: object) -> list[dict[str, object]]:
    """Validate a catalog object and return its sensors with parsed activity bounds."""
    if not isinstance(catalog, dict) or catalog.get("schema_version") != CATALOG_SCHEMA_VERSION:
        raise ValueError(f"catalog must be an object with schema_version {CATALOG_SCHEMA_VERSION}")
    sensors = catalog.get("sensors")
    if not isinstance(sensors, list):
        raise ValueError("catalog.sensors must be a list")
    parsed, seen = [], set()
    for sensor in sensors:
        if not isinstance(sensor, dict) or set(sensor) != set(_CATALOG_SENSOR_FIELDS):
            raise ValueError("catalog sensors must contain exactly " + ", ".join(_CATALOG_SENSOR_FIELDS))
        for field in ("station_id", "sensor_id"):
            if not isinstance(sensor[field], str) or not sensor[field]:
                raise ValueError(f"catalog {field} must be a nonempty string")
        period = sensor["expected_period_seconds"]
        if period is not None and (type(period) is not int or period <= 0):
            raise ValueError("expected_period_seconds must be a positive integer or null")
        key = sensor["station_id"], sensor["sensor_id"]
        if key in seen:
            raise ValueError(f"duplicate catalog sensor: {key}")
        seen.add(key)
        parsed.append(
            {
                **sensor,
                "active_from": None if sensor["active_from"] is None else _instant(sensor["active_from"], "active_from"),
                "active_to": None if sensor["active_to"] is None else _instant(sensor["active_to"], "active_to"),
            }
        )
    return parsed


def expected_sensor_hours(
    sensors: Iterable[Mapping[str, Any]],
    *,
    window_start: datetime,
    window_end: datetime,
    max_expected: int = DEFAULT_MAX_EXPECTED_SENSOR_HOURS,
) -> list[dict[str, object]]:
    """Expected hourly observations for sensors with a known hourly cadence.

    A sensor-hour is expected when the whole hour lies inside both the window and
    the sensor's known activity bounds. Sensors with unknown or non-hourly cadence
    contribute nothing: their completeness is unknown, not zero.
    """
    expected = []
    for sensor in sensors:
        if sensor["expected_period_seconds"] != SUPPORTED_CADENCE_SECONDS:
            continue
        start = max(window_start, sensor["active_from"] or window_start)
        end = min(window_end, sensor["active_to"] or window_end)
        hour = start.replace(minute=0, second=0, microsecond=0)
        if hour < start:
            hour += _HOUR
        while hour + _HOUR <= end:
            expected.append(
                {"station_id": sensor["station_id"], "sensor_id": sensor["sensor_id"], "hour": hour}
            )
            if len(expected) > max_expected:
                raise ValueError(f"more than {max_expected} expected sensor-hours; narrow the window")
            hour += _HOUR
    return expected


def _key(row: Mapping[str, object], hour_field: str) -> tuple[str, str, datetime]:
    return str(row["station_id"]), str(row["sensor_id"]), _instant(row[hour_field], hour_field)


def _describe(key: tuple[str, str, datetime]) -> dict[str, str]:
    return {"station_id": key[0], "sensor_id": key[1], "hour": timestamp(key[2])}


def summarize_coverage(
    current_observations: Iterable[Mapping[str, object]],
    *,
    expected_sensor_hours: Iterable[Mapping[str, object]],
    max_examples: int = DEFAULT_MAX_EXAMPLES,
) -> dict[str, object]:
    """Compare expected, received, usable and missing sensor-hours.

    ``current_observations`` are already-selected revisions with ``station_id``,
    ``sensor_id``, ``period_start`` and ``usable``. An observation is received for
    the sensor-hour starting at its ``period_start``; it counts as usable only if
    its quality allows it (zero is valid; a missing concentration is not).
    ``expected_sensor_hours`` rows carry ``station_id``, ``sensor_id`` and ``hour``.
    """
    observed: dict[tuple[str, str, datetime], bool] = {}
    for row in current_observations:
        key = _key(row, "period_start")
        observed[key] = observed.get(key, False) or bool(row["usable"])
    expected = {_key(row, "hour") for row in expected_sensor_hours}

    report: dict[str, object] = {
        "observed_sensor_hours": len(observed),
        "usable_sensor_hours": sum(observed.values()),
        "expected_sensor_hours": None,
        "received_expected_sensor_hours": None,
        "usable_expected_sensor_hours": None,
        "unusable_sensor_hours": None,
        "missing_sensor_hours": None,
        "unexpected_sensor_hours": None,
        "completeness": None,
        "missing_examples": [],
    }
    if not expected:
        return report
    received = expected & observed.keys()
    usable = {key for key in received if observed[key]}
    missing = sorted(expected - received)
    report.update(
        expected_sensor_hours=len(expected),
        received_expected_sensor_hours=len(received),
        usable_expected_sensor_hours=len(usable),
        unusable_sensor_hours=len(received) - len(usable),
        missing_sensor_hours=len(missing),
        unexpected_sensor_hours=len(observed.keys() - expected),
        completeness=len(usable) / len(expected),
        missing_examples=[_describe(key) for key in missing[:max_examples]],
    )
    return report
