"""Bounded polling/backfill with progress committed after all durable writes."""

import logging
import math
from datetime import timedelta

from collectors.openaq import adapt_measurement
from collectors.storage import now, rejected_envelope
from collectors.weather import adapt_weather, fetch_weather
from streaming.measurements import digest, timestamp, utc_time

LOGGER = logging.getLogger(__name__)


def stage_latest(store, response, parameter):
    if not isinstance(response.payload, dict) or not isinstance(
        response.payload.get("results"), list
    ):
        store.enqueue(
            response,
            "rejected",
            "air_quality_latest",
            "openaq:unknown",
            rejected_envelope(
                response.payload, response, ValueError("invalid latest response")
            ),
        )
        store.finish_response(response.request_id)
        return
    for index, payload in enumerate(response.payload["results"]):
        try:
            if not isinstance(payload, dict):
                raise ValueError("latest row must be an object")
            if (
                type(payload.get("sensorsId")) is not int
                or type(payload.get("locationsId")) is not int
            ):
                raise ValueError("latest row requires numeric sensor/location IDs")
            value = payload.get("value")
            if "value" not in payload or (
                value is not None
                and (type(value) not in (int, float) or not math.isfinite(value))
            ):
                raise ValueError("latest value must be finite or null")
            observed = utc_time(payload["datetime"]["utc"], "latest.datetime")
            if observed > response.collected_at:
                raise ValueError("latest observation is in the future")
            changed = store.save_latest_snapshot(
                payload, parameter, response.collected_at, response.request_id
            )
            job_id = f"openaq:sensor:{payload['sensorsId']}"
            store.ensure_job(
                job_id,
                "openaq",
                "sensor",
                {
                    "sensor_id": payload["sensorsId"],
                    "location_id": payload["locationsId"],
                },
            )
            if changed:
                store.request_job(job_id)
                store.mark_latest_dispatched(f"openaq:{payload['sensorsId']}", payload)
            if store.get_station(f"openaq:{payload['locationsId']}") is None:
                store.ensure_job(
                    f"openaq:station:{payload['locationsId']}",
                    "openaq",
                    "station",
                    {"location_id": payload["locationsId"]},
                    priority=5,
                )
            envelope = {
                "schema_version": 1,
                "source": "openaq",
                "record_type": "latest_snapshot",
                "station_id": f"openaq:{payload['locationsId']}",
                "sensor_id": str(payload["sensorsId"]),
                "parameter": parameter["name"],
                "unit": parameter.get("units"),
                "observed_at": timestamp(observed),
                "value": value,
                "collected_at": timestamp(response.collected_at),
                "ingested_at": timestamp(now()),
                "provider_payload": payload,
                "raw_response_id": response.request_id,
            }
            identity = {
                field: envelope[field]
                for field in (
                    "source",
                    "record_type",
                    "station_id",
                    "sensor_id",
                    "parameter",
                    "observed_at",
                )
            }
            envelope["event_id"] = digest(identity)
            envelope["revision_hash"] = digest(
                {
                    **identity,
                    "value": value,
                    "unit": envelope["unit"],
                    "provider_payload": payload,
                }
            )
            key = envelope["station_id"]
        except (ValueError, KeyError, TypeError) as error:
            envelope = rejected_envelope(payload, response, error)
            key = "openaq:unknown"
        store.enqueue(response, index, "air_quality_latest", key, envelope)
    store.finish_response(response.request_id)


def stage_measurements(store, page, sensor):
    rejected = 0
    for index, payload in enumerate(page.payload["results"]):
        try:
            envelope = adapt_measurement(payload, sensor, page.collected_at)
        except ValueError as error:
            envelope = rejected_envelope(
                payload,
                page,
                error,
                station_id=f"openaq:{sensor['location_id']}",
                sensor_id=str(sensor["sensor_id"]),
            )
            rejected += 1
        envelope["raw_response_id"] = page.request_id
        if envelope.get("status") != "rejected":
            store.save_measurement(envelope)
        store.enqueue(
            page, index, "air_quality_raw", f"openaq:{sensor['location_id']}", envelope
        )
    store.finish_response(page.request_id)
    return len(page.payload["results"]), rejected


def stage_weather(store, response, location, model):
    try:
        envelopes = adapt_weather(response, location, model)
    except ValueError as error:
        store.enqueue(
            response,
            "rejected",
            "weather_raw",
            location["location_id"],
            rejected_envelope(
                response.payload, response, error, location_id=location["location_id"]
            ),
        )
        store.finish_response(response.request_id)
        raise
    for index, envelope in enumerate(envelopes):
        envelope["raw_response_id"] = response.request_id
        store.save_weather(envelope)
        store.enqueue(response, index, "weather_raw", location["location_id"], envelope)
    store.finish_response(response.request_id)
    return envelopes


def recover_pending(store, source=None):
    """Rebuild the outbox from captured responses with the original provenance."""
    recovered = 0
    for page in store.pending_responses(source):
        context = page.context
        if context["kind"] == "openaq":
            sensor = context["sensor"]
            if isinstance(page.payload, dict) and isinstance(
                page.payload.get("results"), list
            ):
                stage_measurements(store, page, sensor)
            else:
                store.enqueue(
                    page,
                    "rejected",
                    "air_quality_raw",
                    f"openaq:{sensor['location_id']}",
                    rejected_envelope(
                        page.payload,
                        page,
                        ValueError("invalid OpenAQ response"),
                        station_id=f"openaq:{sensor['location_id']}",
                    ),
                )
                store.finish_response(page.request_id)
        elif context["kind"] == "weather":
            try:
                stage_weather(store, page, context["location"], context["model"])
            except ValueError:
                # The rejected response was durably enqueued; a fresh poll can proceed.
                pass
        elif context["kind"] == "openaq_latest":
            stage_latest(store, page, context["parameter"])
        else:
            raise ValueError(
                "unknown captured response context; manual inspection required"
            )
        recovered += 1
    return recovered


def collect_sensor(
    client, store, sensor, start, end, window_hours=24, progress_key=None
):
    key = progress_key or f"openaq:live:{sensor['sensor_id']}"
    counts = {"records": 0, "rejected": 0, "windows": 0}
    while start < end:
        stop = min(end, start + timedelta(hours=window_hours))
        for page in client.measurements(
            sensor["sensor_id"],
            start,
            stop,
            context={"kind": "openaq", "sensor": sensor},
        ):
            records, rejected = stage_measurements(store, page, sensor)
            counts["records"] += records
            counts["rejected"] += rejected
        # A failed page, write, or pagination check cannot acknowledge this window.
        counts["windows"] += 1
        store.complete(
            key, stop, sensor_id=sensor["sensor_id"], window_start=start, **counts
        )
        start = stop
    return counts


def backfill_openaq(client, store, settings, start, end):
    if not settings["sensors"]:
        raise ValueError("explicit sensor list required for bounded backfill")
    if start >= end or end > now():
        raise ValueError("backfill requires start < end <= current UTC time")
    results = {}
    for sensor in settings["sensors"]:
        # A separate cursor for each explicitly bounded historical job.
        key = f"openaq:backfill:{sensor['sensor_id']}:{start.isoformat()}:{end.isoformat()}"
        previous = store.progress(key)
        cursor = max(start, previous["completed_until"]) if previous else start
        results[str(sensor["sensor_id"])] = collect_sensor(
            client,
            store,
            sensor,
            cursor,
            end,
            settings["window_hours"],
            key,
        )
    return results


def poll_weather(http, store, settings):
    results, failures = {}, []
    for location in settings["locations"]:
        try:
            previous = store.progress(f"weather:{location['location_id']}")
            fetch_settings = dict(settings)
            if previous:
                missing_days = math.ceil(
                    (now() - previous["completed_until"]).total_seconds() / 86400
                )
                fetch_settings["past_days"] = max(
                    settings["past_days"], missing_days + 1
                )
                if fetch_settings["past_days"] > 92:
                    raise ValueError(
                        "weather gap exceeds forecast API history; archive backfill required"
                    )
            response = fetch_weather(http, location, fetch_settings)
            envelopes = stage_weather(store, response, location, settings["model"])
            store.complete(
                f"weather:{location['location_id']}",
                response.collected_at,
                records=len(envelopes),
                model=settings["model"],
                latest_valid_time=max(
                    utc_time(row["valid_time"], "valid_time") for row in envelopes
                ),
            )
            results[location["location_id"]] = len(envelopes)
        except Exception:
            LOGGER.exception(
                "Weather location %s failed; other locations continue",
                location["location_id"],
            )
            failures.append(location["location_id"])
    if failures:
        raise RuntimeError(
            f"weather failed locations: {failures}; durable progress retained"
        )
    return results
