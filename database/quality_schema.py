"""Validators and indexes for the experiment-only quality collections.

These definitions are applied by ``database.MongoDB.initialize_quality`` to a
separate experiment database, never to the live serving or collector database.
``database.MongoDB`` stays the only persistence boundary; this module holds no
client and does no I/O.

Collections (quality schema version 1):

``quality_decisions``
    One terminal decision per ``(run_id, delivery_id, rule_version)``. The decision
    is stored as canonical JSON (``decision_json``) so it round-trips exactly,
    including integers BSON cannot hold; a few fields are copied out for queries.
``quality_receipts``
    One row per Kafka record (topic, partition, offset) that carried a delivery.
    A Spark retry rewrites the same row; a second publication adds new rows. This
    is what timeliness and repeated-publication checks are measured from.
``quality_rejections``
    Kafka records whose *wrapper* broke the transport contract, or that conflict
    with an earlier delivery with the same ID. Raw bytes are kept. Malformed
    *inner* payloads are not rejections: they get a quarantine decision.
"""

import os
from collections.abc import Mapping

QUALITY_SCHEMA_VERSION = 1
QUALITY_COLLECTIONS = ("quality_decisions", "quality_receipts", "quality_rejections")
REJECTION_REASONS = ("invalid_wrapper", "wrong_topic", "conflicting_redelivery")
# Collections that only exist in a live serving or collector database.
LIVE_MARKER_COLLECTIONS = ("measurements", "collector_outbox", "collector_jobs")


def _validator(required: list[str], properties: Mapping[str, object]) -> dict[str, object]:
    return {
        "$jsonSchema": {
            "bsonType": "object",
            "required": ["_id", "schema_version", *required],
            "properties": {
                "_id": {"bsonType": "string"},
                "schema_version": {"enum": [QUALITY_SCHEMA_VERSION]},
                **properties,
            },
        }
    }


_STRING = {"bsonType": "string"}
_DATE = {"bsonType": "date"}

VALIDATORS = {
    "quality_decisions": _validator(
        [
            "run_id",
            "delivery_id",
            "rule_version",
            "payload_sha256",
            "scheduled_release_index",
            "disposition",
            "decision_json",
            "first_processed_at",
            "first_receipt_id",
        ],
        {
            "run_id": _STRING,
            "delivery_id": _STRING,
            "rule_version": _STRING,
            "payload_sha256": _STRING,
            "scheduled_release_index": {"bsonType": ["int", "long"]},
            "disposition": {"enum": ["accepted", "quarantined"]},
            "decision_json": _STRING,
            "first_processed_at": _DATE,
        },
    ),
    "quality_receipts": _validator(
        ["run_id", "delivery_id", "rule_version", "topic", "partition", "offset", "processed_at"],
        {
            "run_id": _STRING,
            "delivery_id": _STRING,
            "topic": _STRING,
            "partition": {"bsonType": ["int", "long"]},
            "offset": {"bsonType": ["int", "long"]},
            "processed_at": _DATE,
        },
    ),
    "quality_rejections": _validator(
        ["run_id", "rule_version", "topic", "partition", "offset", "reason", "processed_at"],
        {
            "reason": {"enum": list(REJECTION_REASONS)},
            "processed_at": _DATE,
        },
    ),
}

# Names are stable so initialization can be rerun safely.
INDEXES = {
    "quality_decisions": [
        (
            [("run_id", 1), ("rule_version", 1), ("scheduled_release_index", 1), ("_id", 1)],
            {"name": "run_release"},
        ),
    ],
    "quality_receipts": [
        ([("run_id", 1), ("delivery_id", 1)], {"name": "run_delivery"}),
    ],
    "quality_rejections": [
        ([("run_id", 1), ("reason", 1)], {"name": "run_reason"}),
    ],
}


def quality_schema_definitions() -> dict[str, object]:
    """Return the versioned validators and indexes for the quality collections."""
    return {"version": QUALITY_SCHEMA_VERSION, "validators": VALIDATORS, "indexes": INDEXES}


def check_experiment_database(
    name: object, environ: Mapping[str, str] | None = None
) -> str:
    """Return ``name`` if it is a dedicated experiment database name.

    Refuses the live serving and collector databases (as configured, plus their
    defaults) so a replay can never write next to real monitoring data.
    """
    environ = os.environ if environ is None else environ
    if not isinstance(name, str) or not name.strip():
        raise ValueError("an explicit experiment database name is required")
    live = environ.get("MONGO_DATABASE", "urban_air_quality")
    collector = environ.get("MONGO_COLLECTOR_DATABASE", live + "_collector")
    forbidden = {live, collector, "urban_air_quality", "urban_air_quality_collector"}
    if name in forbidden:
        raise ValueError(
            f"{name!r} is a live database; use a separate experiment database"
        )
    return name
