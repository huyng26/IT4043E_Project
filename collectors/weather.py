"""Modeled Open-Meteo weather with explicit model and recorded fetch vintage."""

import math

from collectors.storage import now
from streaming.measurements import digest, timestamp, utc_time

BASE_URL = "https://api.open-meteo.com/v1/forecast"
VARIABLES = (
    "temperature_2m",
    "relative_humidity_2m",
    "surface_pressure",
    "wind_speed_10m",
    "wind_direction_10m",
    "precipitation",
)


def fetch_weather(http, location, settings):
    return http.get(
        BASE_URL,
        {
            "latitude": location["latitude"],
            "longitude": location["longitude"],
            "hourly": ",".join(VARIABLES),
            "timezone": "GMT",
            "timeformat": "iso8601",
            "models": settings["model"],
            "past_days": settings["past_days"],
            "forecast_days": settings["forecast_days"],
            "wind_speed_unit": "ms",
        },
        context={"kind": "weather", "location": location, "model": settings["model"]},
        request_units=max(
            1, math.ceil((settings["past_days"] + settings["forecast_days"]) / 14)
        ),
    )


def adapt_weather(response, location, model):
    payload = response.payload
    if not isinstance(payload, dict) or payload.get("utc_offset_seconds") != 0:
        raise ValueError("Open-Meteo response must use UTC")
    hourly, units = payload.get("hourly"), payload.get("hourly_units")
    if not isinstance(hourly, dict) or not isinstance(units, dict):
        raise ValueError("Open-Meteo hourly data and units required")
    times = hourly.get("time")
    if not isinstance(times, list) or not times:
        raise ValueError("Open-Meteo hourly time array must be nonempty")
    for variable in VARIABLES:
        if (
            not isinstance(hourly.get(variable), list)
            or len(hourly[variable]) != len(times)
            or not isinstance(units.get(variable), str)
        ):
            raise ValueError(f"Open-Meteo hourly array/units mismatch: {variable}")
    seen = set()
    envelopes = []
    metadata = {key: value for key, value in payload.items() if key != "hourly"}
    for index, time_value in enumerate(times):
        if not isinstance(time_value, str):
            raise ValueError("Open-Meteo time must be a string")
        valid = utc_time(
            time_value + "Z" if len(time_value) in (16, 19) else time_value,
            "weather time",
        )
        if valid.minute or valid.second or valid.microsecond or valid in seen:
            raise ValueError("Open-Meteo times must be unique UTC hours")
        seen.add(valid)
        values = {variable: hourly[variable][index] for variable in VARIABLES}
        for value in values.values():
            if value is not None and (
                type(value) not in (int, float) or not math.isfinite(value)
            ):
                raise ValueError("weather values must be finite numbers or null")
        provider_payload = {
            "metadata": metadata,
            "hourly": {"time": time_value, **values},
        }
        identity = {
            "source": "open-meteo",
            "location_id": location["location_id"],
            "valid_time": timestamp(valid),
            "model": model,
        }
        envelope = {
            "schema_version": 1,
            **identity,
            "event_id": digest(identity),
            "station_ids": location["station_ids"],
            "data_kind": "modeled_weather",
            "requested_coordinates": {
                "latitude": location["latitude"],
                "longitude": location["longitude"],
            },
            "values": values,
            "units": {name: units[name] for name in VARIABLES},
            "quality_flags": [
                f"missing:{name}" for name, value in values.items() if value is None
            ],
            "issue_time": None,
            "available_at": timestamp(response.collected_at),
            "availability_basis": "recorded_fetch_time",
            "collected_at": timestamp(response.collected_at),
            "ingested_at": timestamp(now()),
            "provider_payload": provider_payload,
        }
        # Fetch vintage is separate from a stable location/hour/model identity.
        envelope["revision_hash"] = digest(
            {
                "values": values,
                "units": envelope["units"],
                "provider_payload": provider_payload,
            }
        )
        envelope["vintage_id"] = digest(
            {
                **identity,
                "available_at": envelope["available_at"],
                "revision_hash": envelope["revision_hash"],
            }
        )
        envelopes.append(envelope)
    return envelopes
