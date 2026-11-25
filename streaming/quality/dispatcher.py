"""Experiment-only consumer logic for ``quality_replay_raw`` Kafka records.

The Kafka-side counterpart of ``streaming.quality.local_run``: each record's
wrapper is checked, its inner bytes go through the shared rules, and the decision
is stored in a separate experiment database through ``database.MongoDB``.

* The replay topic is never added to ``streaming.processing.TOPICS``; the live
  consumer and the live serving database never see a wrapper.
* Nothing here reads the evaluator manifest or imports the evaluator/injector.
* Writes are retry-safe. A decision is keyed by ``(run_id, delivery_id,
  rule_version)`` and the first one is kept; a receipt is keyed by Kafka
  topic/partition/offset. A Spark retry rewrites nothing, and a second publication
  of the same run adds receipts without adding decisions.
* A wrapper that breaks the transport contract is a rejection with its raw bytes
  kept, not a quarantine decision: without a valid wrapper there is no trustworthy
  delivery ID. Wrappers of a *different* run are skipped and counted, because they
  belong to another experiment, not to this one.
* Database failures propagate so Spark retries the batch.
"""

import json
from collections import Counter
from collections.abc import Callable, Iterable, Mapping
from datetime import UTC, datetime
from typing import Any

from models import canonical_json, digest
from streaming.quality.contracts import (
    REPLAY_TOPIC,
    decode_wrapper_payload,
    validate_wrapper,
)
from streaming.quality.rules import SUPPORTED_RULE_VERSIONS, evaluate_payload

QUALITY_SCHEMA_VERSION = 1  # mirrors database.quality_schema without importing pymongo
OUTCOMES = ("decided", "redelivered", "rejected", "other_run")


class Rejected(Exception):
    def __init__(self, reason: str, detail: str):
        super().__init__(detail)
        self.reason = reason
        self.detail = detail


def _now() -> datetime:
    return datetime.now(UTC)


def _bytes(value: object) -> bytes | None:
    if value is None:
        return None
    if isinstance(value, (bytes, bytearray, memoryview)):
        return bytes(value)
    if isinstance(value, str):
        return value.encode("utf-8")
    raise TypeError("Kafka key/value must be bytes, str or null")


def _reject_constant(value: str) -> None:
    raise ValueError(f"nonstandard JSON number: {value}")


def decode_record(record: Mapping[str, Any]) -> dict[str, Any]:
    """Return the validated wrapper of one Kafka record, or raise ``Rejected``."""
    if record.get("topic") != REPLAY_TOPIC:
        raise Rejected("wrong_topic", f"expected {REPLAY_TOPIC}, got {record.get('topic')!r}")
    raw = _bytes(record.get("value"))
    if raw is None:
        raise Rejected("invalid_wrapper", "record has no value")
    try:
        wrapper = json.loads(raw.decode("utf-8"), parse_constant=_reject_constant)
        return validate_wrapper(wrapper)
    except (UnicodeDecodeError, ValueError, RecursionError) as error:
        raise Rejected("invalid_wrapper", str(error)) from error


def receipt_id(record: Mapping[str, Any], rule_version: str) -> str:
    """Stable ID of one Kafka record (topic/partition/offset) under a rule version."""
    return digest([record.get("topic"), record.get("partition"), record.get("offset"), rule_version])


def decision_document(
    wrapper: Mapping[str, Any],
    decision: Mapping[str, Any],
    *,
    processed_at: datetime,
    first_receipt_id: str,
) -> dict[str, Any]:
    rule_version = str(decision["rule_version"])
    return {
        "_id": digest([wrapper["run_id"], wrapper["delivery_id"], rule_version]),
        "schema_version": QUALITY_SCHEMA_VERSION,
        "kind": "experiment",
        "run_id": wrapper["run_id"],
        "delivery_id": wrapper["delivery_id"],
        "rule_version": rule_version,
        "original_topic": wrapper["original_topic"],
        "original_key": wrapper["original_key"],
        "source_record_ref": dict(wrapper["source_record_ref"]),
        "payload_sha256": wrapper["payload_sha256"],
        "scheduled_release_index": wrapper["scheduled_release_index"],
        "disposition": decision["disposition"],
        "reason": decision["reason"],
        "usable": decision["usable"],
        "event_id": decision["event_id"],
        "decision_json": canonical_json(decision),
        "first_processed_at": processed_at,
        "first_receipt_id": first_receipt_id,
    }


def receipt_document(
    record: Mapping[str, Any],
    wrapper: Mapping[str, Any],
    *,
    rule_version: str,
    processed_at: datetime,
) -> dict[str, Any]:
    return {
        "_id": receipt_id(record, rule_version),
        "schema_version": QUALITY_SCHEMA_VERSION,
        "run_id": wrapper["run_id"],
        "delivery_id": wrapper["delivery_id"],
        "rule_version": rule_version,
        "payload_sha256": wrapper["payload_sha256"],
        "topic": record["topic"],
        "partition": record["partition"],
        "offset": record["offset"],
        "broker_timestamp": record.get("timestamp"),
        "published_at": wrapper["published_at"],
        "processed_at": processed_at,
    }


def rejection_document(
    record: Mapping[str, Any],
    *,
    run_id: str,
    rule_version: str,
    reason: str,
    detail: str,
    processed_at: datetime,
    delivery_id: str | None = None,
) -> dict[str, Any]:
    return {
        "_id": receipt_id(record, rule_version),
        "schema_version": QUALITY_SCHEMA_VERSION,
        "run_id": run_id,
        "rule_version": rule_version,
        "delivery_id": delivery_id,
        "topic": record.get("topic"),
        "partition": record.get("partition"),
        "offset": record.get("offset"),
        "broker_timestamp": record.get("timestamp"),
        "key": _bytes(record.get("key")),
        "value": _bytes(record.get("value")),
        "reason": reason,
        "detail": detail,
        "processed_at": processed_at,
    }


class ReplayDispatcher:
    """Decide and store each replay record for one run and rule version."""

    def __init__(
        self,
        repository: Any,
        *,
        run_id: str,
        rule_version: str,
        clock: Callable[[], datetime] = _now,
    ):
        if not isinstance(run_id, str) or not run_id.strip():
            raise ValueError("run_id must be a nonempty string")
        if rule_version not in SUPPORTED_RULE_VERSIONS:
            raise ValueError(f"unsupported rule version: {rule_version}")
        self.repository = repository
        self.run_id = run_id
        self.rule_version = rule_version
        self.clock = clock

    def _reject(self, record, reason, detail, delivery_id=None) -> str:
        self.repository.record_quality_rejection(
            rejection_document(
                record,
                run_id=self.run_id,
                rule_version=self.rule_version,
                reason=reason,
                detail=detail,
                processed_at=self.clock(),
                delivery_id=delivery_id,
            )
        )
        return "rejected"

    def process(self, record: Mapping[str, Any]) -> str:
        """Handle one Kafka record and return its outcome (one of ``OUTCOMES``)."""
        try:
            wrapper = decode_record(record)
        except Rejected as rejected:
            return self._reject(record, rejected.reason, rejected.detail)
        if wrapper["run_id"] != self.run_id:
            return "other_run"
        decision = evaluate_payload(
            decode_wrapper_payload(wrapper),
            source_topic=str(wrapper["original_topic"]),
            rule_version=self.rule_version,
        )
        processed_at = self.clock()
        this_receipt = receipt_id(record, self.rule_version)
        stored = self.repository.save_quality_decision(
            decision_document(
                wrapper, decision, processed_at=processed_at, first_receipt_id=this_receipt
            )
        )
        if stored["payload_sha256"] != wrapper["payload_sha256"]:
            return self._reject(
                record,
                "conflicting_redelivery",
                "delivery ID was already decided for different payload bytes",
                delivery_id=str(wrapper["delivery_id"]),
            )
        self.repository.save_quality_receipt(
            receipt_document(
                record, wrapper, rule_version=self.rule_version, processed_at=processed_at
            )
        )
        # A Spark retry of the first record is still "decided"; another Kafka
        # record carrying the same delivery is a redelivery.
        return "decided" if stored["first_receipt_id"] == this_receipt else "redelivered"


def _normalized(row: Any) -> dict[str, Any]:
    record = row.asDict() if hasattr(row, "asDict") else dict(row)
    broker_time = record.get("timestamp")
    if isinstance(broker_time, datetime) and broker_time.tzinfo is None:
        record["timestamp"] = broker_time.replace(tzinfo=UTC)
    return record


def dispatch_records(records: Iterable[Any], dispatcher: ReplayDispatcher) -> dict[str, int]:
    counts = Counter({outcome: 0 for outcome in OUTCOMES})
    for row in records:
        counts[dispatcher.process(_normalized(row))] += 1
    return dict(counts)


def process_replay_partition(
    records: Iterable[Any],
    *,
    mongo_uri: str,
    experiment_database: str,
    run_id: str,
    rule_version: str,
    repository_factory: Callable[[str, str], Any] | None = None,
) -> dict[str, int]:
    """Process one Spark partition with one repository; return outcome counts.

    ``experiment_database`` must not be a live database. ``repository_factory``
    exists for tests; Spark uses ``database.MongoDB``.
    """
    iterator = iter(records)
    first = next(iterator, None)
    if first is None:
        return {outcome: 0 for outcome in OUTCOMES}
    if repository_factory is None:
        from database import MongoDB
        from database.quality_schema import check_experiment_database

        check_experiment_database(experiment_database)
        repository_factory = MongoDB

    def rows():
        yield first
        yield from iterator

    with repository_factory(mongo_uri, experiment_database) as repository:
        dispatcher = ReplayDispatcher(repository, run_id=run_id, rule_version=rule_version)
        return dispatch_records(rows(), dispatcher)
