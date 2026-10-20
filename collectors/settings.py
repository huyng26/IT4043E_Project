"""Validated global collection policy; credentials live only in the environment."""

import json
import math
import os
from pathlib import Path

from dotenv import load_dotenv


def load_settings(path=None):
    load_dotenv()
    config = json.loads(
        Path(
            path or os.getenv("COLLECTOR_CONFIG", "config/collectors.json")
        ).read_text()
    )
    if config.get("schema_version") != 2 or config.get("scope") != "global":
        raise ValueError("collector config must use schema_version 2 and scope global")
    aq, weather, scheduler = config["openaq"], config["weather"], config["scheduler"]
    for section, fields in (
        (
            aq,
            (
                "catalog_interval_seconds",
                "latest_interval_seconds",
                "latest_lookback_hours",
                "reconcile_interval_seconds",
                "worker_count",
                "bootstrap_hours",
                "lookback_hours",
                "window_hours",
                "history_days",
            ),
        ),
        (weather, ("poll_seconds", "worker_count", "forecast_days")),
        (scheduler, ("idle_seconds", "lease_seconds", "retry_seconds")),
    ):
        for field in fields:
            if type(section[field]) is not int or section[field] < 1:
                raise ValueError(f"{field} must be a positive integer")
    if type(aq["page_size"]) is not int or not 1 <= aq["page_size"] <= 1000:
        raise ValueError("page_size must be 1..1000")
    if scheduler["lease_seconds"] < 30:
        raise ValueError("lease_seconds must be at least 30")
    if (
        type(weather["past_days"]) is not int
        or not 0 <= weather["past_days"] <= 92
        or weather["forecast_days"] > 16
    ):
        raise ValueError("weather past_days must be 0..92, forecast_days 1..16")
    if (
        type(weather["coordinate_precision"]) is not int
        or not 0 <= weather["coordinate_precision"] <= 6
    ):
        raise ValueError("coordinate_precision must be 0..6")
    if type(weather["enabled"]) is not bool:
        raise ValueError("weather.enabled must be boolean")
    if (
        not isinstance(weather["model"], str)
        or not weather["model"]
        or "," in weather["model"]
    ):
        raise ValueError("configure one explicit weather model")
    for location in weather.get("locations", []):
        coordinates(location["latitude"], location["longitude"])
        if not isinstance(location["location_id"], str) or not location["location_id"]:
            raise ValueError("manual weather locations need a location_id")
    return config


def coordinates(latitude, longitude):
    if any(
        type(value) not in (int, float) or not math.isfinite(value)
        for value in (latitude, longitude)
    ):
        raise ValueError("coordinates must be finite numbers")
    if not -90 <= latitude <= 90 or not -180 <= longitude <= 180:
        raise ValueError("coordinates out of range")
