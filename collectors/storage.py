"""Collector facade. All MongoDB queries live in database.MongoDB."""

import json
import os
import time

from database import MongoDB
from database.documents import now
from streaming.measurements import digest, timestamp, utc_time
from streaming.replay import reject_constant


def wait(seconds, stop_event=None):
    deadline = time.monotonic() + max(0, seconds)
    while (remaining := deadline - time.monotonic()) > 0:
        if stop_event is None:
            time.sleep(min(remaining, 30))
        elif stop_event.wait(min(remaining, 30)):
            raise InterruptedError("collector stopped")


class CollectorStore:
    """Collector control state and outbox, isolated from Spark's serving database."""

    def __init__(self, uri=None, database=None, repository=None):
        state_database = database or os.getenv(
            "MONGO_COLLECTOR_DATABASE",
            os.getenv("MONGO_DATABASE", "urban_air_quality") + "_collector",
        )
        if repository is None and state_database == os.getenv(
            "MONGO_DATABASE", "urban_air_quality"
        ):
            raise ValueError("collector state and serving databases must be different")
        self.mongodb = (
            repository if repository is not None else MongoDB(uri, state_database)
        )
        self.stop_event = None

    def __getattr__(self, name):
        return getattr(self.mongodb, name)

    def close(self):
        self.mongodb.close()

    def capture(self, response):
        self.mongodb.capture_response(response)

    def pending_responses(self, source=None):
        from collectors.http import Response

        for row in self.mongodb.pending_responses(source):
            try:
                payload = json.loads(row["body"], parse_constant=reject_constant)
                digest(payload)
            except ValueError:
                payload = row["body"]
            yield Response(
                row["source"],
                row["url"],
                row["params"],
                payload,
                row["body"],
                row["headers"],
                utc_time(
                    row.get("collected_at_iso", timestamp(row["collected_at"])),
                    "collected_at",
                ),
                row["_id"],
                row["context"],
            )

    def enqueue(self, response, index, topic, key, envelope):
        self.mongodb.enqueue(response.request_id, index, topic, key, envelope)

    def enqueue_catalog(self, record_type, payload, **metadata):
        collected_at = timestamp(now())
        envelope = {
            "schema_version": 1,
            "source": "collector",
            "record_type": record_type,
            "provider_payload": payload,
            "collected_at": collected_at,
            "ingested_at": collected_at,
            **metadata,
        }
        key = str(payload.get("id", payload.get("location_id", record_type)))
        self.mongodb.enqueue(
            "catalog:" + digest(envelope), 0, "source_catalog", key, envelope
        )

    def remember_latest_snapshot(self, payload, parameter, collected_at, response_id):
        return self.mongodb.save_latest_snapshot(
            payload, parameter, collected_at, response_id, collection="collector_latest"
        )

    def mark_latest_dispatched(self, sensor_id, payload):
        self.mongodb.mark_latest_dispatched(
            sensor_id, payload, collection="collector_latest"
        )

    def save_measurement(self, *_args, **_kwargs):
        raise RuntimeError("measurement projections must be written by Spark")

    def save_weather(self, *_args, **_kwargs):
        raise RuntimeError("weather projections must be written by Spark")

    def save_latest_snapshot(self, *_args, **_kwargs):
        raise RuntimeError("serving snapshots must be written by Spark")

    def reserve_request(
        self, scope, minute_limit=60, hour_limit=2000, day_limit=None, units=1
    ):
        limits = [(60, minute_limit), (3600, hour_limit)]
        if day_limit:
            limits.append((86400, day_limit))
        while True:
            if self.stop_event is not None and self.stop_event.is_set():
                raise InterruptedError("collector stopped")
            delay = self.mongodb.reserve_budget(
                scope, limits, units=units, current=now()
            )
            if delay is None:
                return
            if delay:
                wait(delay, self.stop_event)

    def defer_requests(self, scope, seconds):
        self.mongodb.defer_budget(scope, seconds)

    def publish(self, producer, limit=1000):
        sent = 0
        for row in self.mongodb.pending_outbox(limit):
            if self.stop_event is not None and self.stop_event.is_set():
                break
            metadata = producer.send(
                row["topic"], key=row["key"], value=row["envelope"]
            ).get(timeout=30)
            self.mongodb.acknowledge_outbox(
                row["_id"], metadata.topic, metadata.partition, metadata.offset
            )
            sent += 1
        return sent


Store = CollectorStore


def rejected_envelope(payload, response, error, **identity):
    return {
        "schema_version": 1,
        "source": response.source,
        **identity,
        "status": "rejected",
        "error": str(error),
        "collected_at": timestamp(response.collected_at),
        "ingested_at": timestamp(now()),
        "provider_payload": payload,
        "raw_response_id": response.request_id,
    }
