"""Pure transformations from provider/envelope objects to MongoDB documents."""

import math
import unicodedata
from datetime import UTC, datetime

from streaming.measurements import digest, parse_measurement, utc_time


def now():
    return datetime.now(UTC)


def order_time(value):
    # BSON dates have millisecond precision; this string preserves ordering below 1 ms.
    return value.astimezone(UTC).isoformat(timespec="microseconds")


def geo_point(coordinates):
    if not isinstance(coordinates, dict):
        return None
    lat, lon = coordinates.get("latitude"), coordinates.get("longitude")
    if any(
        type(value) not in (int, float) or not math.isfinite(value)
        for value in (lat, lon)
    ):
        return None
    if not -90 <= lat <= 90 or not -180 <= lon <= 180:
        return None
    return {"type": "Point", "coordinates": [lon, lat]}


def city_document(station):
    country = (station.get("country") or {}).get("code")
    locality = station.get("locality")
    if not isinstance(locality, str) or not locality.strip() or not country:
        return None
    normalized = " ".join(unicodedata.normalize("NFKC", locality).casefold().split())
    return {
        "_id": "openaq:" + digest([country, normalized]),
        "schema_version": 2,
        "source": "openaq",
        "country_code": country,
        "name": locality.strip(),
        "normalized_name": normalized,
        "identity_kind": "source_locality_label",
        "administrative_id": None,
        "updated_at": now(),
    }


def measurement_document(envelope):
    if (
        type(envelope.get("schema_version")) is not int
        or envelope["schema_version"] != 1
    ):
        raise ValueError("measurement envelope schema_version must be 1")
    for field in ("source", "station_id", "sensor_id", "pollutant", "unit"):
        if not isinstance(envelope.get(field), str) or not envelope[field].strip():
            raise ValueError(f"{field} must be a nonempty string")
    if not envelope["station_id"].startswith(envelope["source"] + ":"):
        raise ValueError("station_id must be source-qualified")
    value = envelope.get("value")
    if "value" not in envelope or (
        value is not None
        and (type(value) not in (int, float) or not math.isfinite(value))
    ):
        raise ValueError("measurement value must be finite or null")
    if not isinstance(envelope.get("provider_payload"), dict):
        raise ValueError("provider_payload must be an object")
    flags = envelope.get("source_flags")
    if not isinstance(flags, list) or any(not isinstance(flag, str) for flag in flags):
        raise ValueError("source_flags must be an explicit string list")
    start = utc_time(envelope["period_start"], "period_start")
    end = utc_time(envelope["period_end"], "period_end")
    collected = utc_time(envelope["collected_at"], "collected_at")
    ingested = utc_time(envelope["ingested_at"], "ingested_at")
    if not start < end <= collected <= ingested:
        raise ValueError("require start < end <= collected_at <= ingested_at")
    identity = {
        field: envelope[field]
        for field in ("source", "station_id", "sensor_id", "pollutant")
    }
    identity.update(
        period_start=start.isoformat().replace("+00:00", "Z"),
        period_end=end.isoformat().replace("+00:00", "Z"),
    )
    revision = {
        **identity,
        **{
            field: envelope[field]
            for field in ("value", "unit", "source_flags", "provider_payload")
        },
    }
    if envelope.get("event_id") != digest(identity) or envelope.get(
        "revision_hash"
    ) != digest(revision):
        raise ValueError(
            "measurement identity/revision hash does not match its payload"
        )
    sensor_id = f"{envelope['source']}:{envelope['sensor_id']}"
    normalized, quality = None, []
    if envelope["pollutant"] == "pm25":
        parsed = parse_measurement(envelope)
        normalized, quality = parsed.value_ug_m3, list(parsed.quality_flags)
    else:
        if envelope["value"] is None:
            quality.append("missing_value")
        if envelope["source_flags"]:
            quality.append("source_flagged")
    return {
        "_id": envelope["event_id"],
        "schema_version": 2,
        "event_id": envelope["event_id"],
        "revision_hash": envelope["revision_hash"],
        "source": envelope["source"],
        "station_id": envelope["station_id"],
        "sensor_id": sensor_id,
        "parameter": envelope["pollutant"],
        "period_start": start,
        "period_end": end,
        "value": envelope["value"],
        "unit": envelope["unit"],
        "value_ug_m3": normalized,
        "quality_flags": quality,
        "collected_at": collected,
        "ingested_at": ingested,
        "revision_order": order_time(collected) + ":" + envelope["revision_hash"],
        "source_flags": envelope["source_flags"],
        "provider_payload": envelope["provider_payload"],
        "raw_response_id": envelope.get("raw_response_id"),
        "kafka": envelope.get("kafka"),
    }
