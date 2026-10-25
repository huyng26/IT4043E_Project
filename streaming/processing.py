"""Executor-side validation and MongoDB projections for Kafka micro-batches."""

import json
import math
from datetime import UTC

from database.documents import measurement_document
from models import canonical_json, digest, timestamp, utc_time

TOPICS = ("air_quality_raw", "weather_raw", "air_quality_latest", "source_catalog")


def reject_constant(value):
    raise ValueError(f"nonstandard JSON number: {value}")


def validate_weather(envelope):
    if envelope.get("source") != "open-meteo":
        raise ValueError("weather source must be open-meteo")
    for field in ("location_id", "model"):
        if not isinstance(envelope.get(field), str) or not envelope[field]:
            raise ValueError(f"weather {field} must be a nonempty string")
    valid = utc_time(envelope["valid_time"], "valid_time")
    available = utc_time(envelope["available_at"], "available_at")
    collected = utc_time(envelope["collected_at"], "collected_at")
    ingested = utc_time(envelope["ingested_at"], "ingested_at")
    if (
        valid.minute
        or valid.second
        or valid.microsecond
        or not available <= collected <= ingested
    ):
        raise ValueError(
            "weather requires hourly time and available <= collected <= ingested"
        )
    values, units = envelope["values"], envelope["units"]
    if not isinstance(values, dict) or not values or not isinstance(units, dict):
        raise ValueError("weather values and units must be objects")
    for name, value in values.items():
        if not isinstance(units.get(name), str) or not units[name]:
            raise ValueError("weather variables require units")
        if value is not None and (
            type(value) not in (int, float) or not math.isfinite(value)
        ):
            raise ValueError("weather values must be finite numbers or null")
    if not isinstance(envelope["provider_payload"], dict):
        raise TypeError("weather provider_payload must be an object")
    identity = {
        "source": "open-meteo",
        "location_id": envelope["location_id"],
        "valid_time": timestamp(valid),
        "model": envelope["model"],
    }
    revision = digest(
        {
            "values": values,
            "units": units,
            "provider_payload": envelope["provider_payload"],
        }
    )
    vintage = digest(
        {
            **identity,
            "available_at": envelope["available_at"],
            "revision_hash": revision,
        }
    )
    if (
        envelope.get("event_id"),
        envelope.get("revision_hash"),
        envelope.get("vintage_id"),
    ) != (
        digest(identity),
        revision,
        vintage,
    ):
        raise ValueError("weather identity/revision/vintage does not match its payload")
    envelope["quality_flags"] = [
        f"missing:{name}" for name, value in values.items() if value is None
    ]


def validate_snapshot(envelope):
    payload = envelope["provider_payload"]
    if not isinstance(payload, dict) or any(
        type(payload.get(field)) is not int for field in ("sensorsId", "locationsId")
    ):
        raise ValueError("latest snapshot requires numeric sensor/location IDs")
    if envelope["source"] != "openaq" or envelope["record_type"] != "latest_snapshot":
        raise ValueError("invalid latest snapshot type/source")
    if envelope["station_id"] != f"openaq:{payload['locationsId']}" or envelope[
        "sensor_id"
    ] != str(payload["sensorsId"]):
        raise ValueError("snapshot identity does not match provider payload")
    value = envelope["value"]
    if value is not None and (
        type(value) not in (int, float) or not math.isfinite(value)
    ):
        raise ValueError("latest value must be finite or null")
    if value != payload["value"] or utc_time(
        envelope["observed_at"], "observed_at"
    ) != utc_time(payload["datetime"]["utc"], "datetime"):
        raise ValueError("snapshot value/time does not match provider payload")
    if (
        not utc_time(envelope["observed_at"], "observed_at")
        <= utc_time(envelope["collected_at"], "collected_at")
        <= utc_time(envelope["ingested_at"], "ingested_at")
    ):
        raise ValueError("snapshot requires observed <= collected <= ingested")
    for field in ("parameter", "unit"):
        if not isinstance(envelope[field], str) or not envelope[field]:
            raise ValueError(f"snapshot {field} must be a nonempty string")
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
    revision = digest(
        {
            **identity,
            "value": value,
            "unit": envelope["unit"],
            "provider_payload": payload,
        }
    )
    if (
        envelope.get("event_id") != digest(identity)
        or envelope.get("revision_hash") != revision
    ):
        raise ValueError("snapshot identity/revision does not match its payload")


def validate_catalog(envelope):
    payload = envelope["provider_payload"]
    if not isinstance(payload, dict):
        raise TypeError("catalog payload must be an object")
    if not utc_time(envelope["collected_at"], "collected_at") <= utc_time(
        envelope["ingested_at"], "ingested_at"
    ):
        raise ValueError("catalog requires collected <= ingested")
    kind = envelope["record_type"]
    if kind == "parameter_metadata":
        if type(payload["id"]) is not int or not isinstance(payload["name"], str):
            raise ValueError("invalid parameter metadata")
    elif kind == "station_metadata":
        if type(payload["id"]) is not int or not isinstance(
            payload.get("sensors", []), list
        ):
            raise ValueError("invalid station metadata")
        if payload.get("country") is not None and not isinstance(
            payload["country"], dict
        ):
            raise ValueError("station country must be an object or null")
        for sensor in payload.get("sensors", []):
            if type(sensor["id"]) is not int or not isinstance(
                sensor["parameter"]["name"], str
            ):
                raise ValueError("invalid sensor metadata")
        precision = envelope["coordinate_precision"]
        if (
            type(precision) is not int
            or not 0 <= precision <= 6
            or type(envelope["weather_enabled"]) is not bool
        ):
            raise ValueError("invalid weather mapping policy")
    elif kind == "weather_location_metadata":
        if not isinstance(payload["location_id"], str) or not payload["location_id"]:
            raise ValueError("invalid weather location ID")
        for field, bound in (("latitude", 90), ("longitude", 180)):
            value = payload[field]
            if (
                type(value) not in (int, float)
                or not math.isfinite(value)
                or not -bound <= value <= bound
            ):
                raise ValueError("invalid weather coordinates")
    else:
        raise ValueError("unknown catalog record type")


def prepare_record(record):
    raw = record["value"]
    if isinstance(raw, (bytes, bytearray)):
        raw = raw.decode("utf-8")
    envelope = json.loads(raw, parse_constant=reject_constant)
    if not isinstance(envelope, dict):
        raise TypeError("event must be a JSON object")
    canonical_json(envelope)
    if (
        type(envelope.get("schema_version")) is not int
        or envelope["schema_version"] != 1
    ):
        raise ValueError("event schema_version must be 1")
    if envelope.get("status") == "rejected":
        raise ValueError(
            "collector rejected input: "
            + str(envelope.get("error", "invalid source data"))
        )
    topic = record["topic"]
    if topic == "air_quality_raw":
        if envelope.get("source") != "openaq":
            raise ValueError("air-quality source must be openaq")
        measurement_document(envelope)
    elif topic == "weather_raw":
        validate_weather(envelope)
    elif topic == "air_quality_latest":
        validate_snapshot(envelope)
    elif topic == "source_catalog":
        validate_catalog(envelope)
    else:
        raise ValueError("unsupported Kafka topic")
    envelope["kafka"] = {
        field: record[field] for field in ("topic", "partition", "offset")
    }
    return envelope


class EventProcessor:
    """Apply prepared events; storage failures propagate so Spark retries the batch."""

    def __init__(self, repository):
        self.repository = repository

    def process(self, record):
        try:
            envelope = prepare_record(record)
        except (ValueError, KeyError, TypeError, UnicodeError, OverflowError) as error:
            self.repository.record_stream_rejection(record, str(error))
            return
        # Never classify a failed database write as malformed input.
        topic = record["topic"]
        if topic == "air_quality_raw":
            self.repository.save_measurement(envelope)
        elif topic == "weather_raw":
            self.repository.save_weather(envelope)
        elif topic == "air_quality_latest":
            self.repository.save_latest_snapshot(
                envelope["provider_payload"],
                {"name": envelope["parameter"], "units": envelope["unit"]},
                utc_time(envelope["collected_at"], "collected_at"),
                envelope.get("raw_response_id"),
                track_dispatch=False,
            )
        else:
            payload = envelope["provider_payload"]
            kind = envelope["record_type"]
            if kind == "station_metadata":
                station = self.repository.save_station(payload)
                if envelope["weather_enabled"]:
                    self.repository.save_weather_location(
                        station, envelope["coordinate_precision"]
                    )
            elif kind == "parameter_metadata":
                self.repository.save_parameter(payload)
            else:
                self.repository.save_manual_weather_location(payload)


def process_partition(records, *, uri, database):
    """One Mongo client per executor partition; no driver-side record collection."""
    from database import MongoDB

    iterator = iter(records)
    first = next(iterator, None)
    if first is None:
        return
    with MongoDB(uri, database) as repository:
        processor = EventProcessor(repository)
        for row in _with_first(first, iterator):
            record = row.asDict() if hasattr(row, "asDict") else dict(row)
            broker_time = record.get("timestamp")
            if broker_time is not None and broker_time.tzinfo is None:
                record["timestamp"] = broker_time.replace(tzinfo=UTC)
            processor.process(record)


def _with_first(first, iterator):
    yield first
    yield from iterator
