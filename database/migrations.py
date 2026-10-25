"""Requeue legacy input through Kafka; never project around Spark."""

from database.documents import now
from models import timestamp

TOPICS = ("air_quality_raw", "weather_raw", "air_quality_latest", "source_catalog")


def migrate_envelopes(
    source, destination, *, coordinate_precision=2, weather_enabled=True
):
    """Copy into the collector outbox without deleting or resetting legacy records."""
    counts = {"stations_queued": 0, "records_queued": 0, "skipped": 0}
    for row in source.iter_catalog_stations():
        payload = row.get("provider_payload", row.get("metadata"))
        if not isinstance(payload, dict):
            counts["skipped"] += 1
            continue
        collected = timestamp(row.get("last_seen_at", now()))
        envelope = {
            "schema_version": 1,
            "source": "collector",
            "record_type": "station_metadata",
            "provider_payload": payload,
            "collected_at": collected,
            "ingested_at": collected,
            "coordinate_precision": coordinate_precision,
            "weather_enabled": weather_enabled,
        }
        destination.enqueue(
            "migration:station:" + row["_id"],
            0,
            "source_catalog",
            str(payload["id"]),
            envelope,
        )
        counts["stations_queued"] += 1
    for row in source.iter_outbox_envelopes():
        if row.get("topic") not in TOPICS:
            counts["skipped"] += 1
            continue
        destination.enqueue(
            "migration:outbox:" + row["_id"],
            0,
            row["topic"],
            row["key"],
            row["envelope"],
        )
        counts["records_queued"] += 1
    return counts
