"""Step 4 without services: publication, dispatch, storage semantics and grading.

A fake Kafka producer assigns partitions/offsets and a fake repository mirrors
``database.MongoDB``'s insert-once semantics, so the whole path
build -> publish -> dispatch -> export -> evaluate runs in-process.
"""

import ast
import json
import tempfile
import unittest
from datetime import UTC, datetime, timedelta
from pathlib import Path

from quality_support import write_catalog, write_snapshot
from test_quality_replay import make_config

from models import canonical_json
from streaming.quality.contracts import REPLAY_TOPIC
from streaming.quality.dispatcher import (
    ReplayDispatcher,
    dispatch_records,
    process_replay_partition,
)
from streaming.quality.local_run import (
    process_replay,
    process_snapshot,
    results_from_store,
    save_results,
    transport_summary,
)
from streaming.quality.rules import RULE_VERSION
from streaming.quality_evaluation import evaluate_run, export_store
from streaming.quality_replay import FAULT_TYPES, build_replay, publish_replay

ROOT = Path(__file__).resolve().parents[1]

try:
    import pymongo  # noqa: F401
except ImportError:
    pymongo = None


class FakeKafka:
    """Shared broker state: three partitions, per-partition offsets."""

    def __init__(self, fail_first=0):
        self.records = []
        self.offsets = [0, 0, 0]
        self.fail_first = fail_first
        self.attempts = []

    def manager(self, servers):
        return FakeManager(self)


class FakeManager:
    def __init__(self, kafka):
        self.kafka = kafka

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False

    def publish(self, topic, *, key, value):
        payload = canonical_json(value).encode("utf-8")  # what KafkaManager sends
        self.kafka.attempts.append(payload)
        if self.kafka.fail_first:
            self.kafka.fail_first -= 1
            raise ConnectionError("broker unavailable")
        partition = sum(key.encode()) % 3
        offset = self.kafka.offsets[partition]
        self.kafka.offsets[partition] += 1
        self.kafka.records.append(
            {
                "topic": topic,
                "partition": partition,
                "offset": offset,
                "timestamp": datetime(2026, 1, 2, tzinfo=UTC).replace(tzinfo=None),
                "key": key.encode(),
                "value": payload,
            }
        )
        return {"topic": topic, "partition": partition, "offset": offset}


class FakeStore:
    """In-memory stand-in for the quality methods of ``database.MongoDB``."""

    def __init__(self):
        self.decisions, self.receipts, self.rejections = {}, {}, {}
        self.closed = False

    def __call__(self, uri, database):  # repository_factory
        self.opened = (uri, database)
        return self

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.closed = True

    @staticmethod
    def _insert_once(table, document):
        json.dumps(document, default=str)  # stays serializable
        table.setdefault(document["_id"], dict(document))
        return table[document["_id"]]

    def save_quality_decision(self, document):
        return self._insert_once(self.decisions, document)

    def save_quality_receipt(self, document):
        return self._insert_once(self.receipts, document)

    def record_quality_rejection(self, document):
        return self._insert_once(self.rejections, document)

    def _rows(self, table, run_id, rule_version):
        return [r for r in table.values() if (r["run_id"], r["rule_version"]) == (run_id, rule_version)]

    def iter_quality_decisions(self, run_id, rule_version):
        rows = self._rows(self.decisions, run_id, rule_version)
        return sorted(rows, key=lambda r: (r["scheduled_release_index"], r["_id"]))

    def iter_quality_receipts(self, run_id, rule_version):
        return self._rows(self.receipts, run_id, rule_version)

    def iter_quality_rejections(self, run_id, rule_version):
        return self._rows(self.rejections, run_id, rule_version)


class Clock:
    def __init__(self):
        self.now = datetime(2026, 1, 2, tzinfo=UTC)

    def __call__(self):
        self.now += timedelta(milliseconds=250)
        return self.now

    def text(self):
        return self().isoformat().replace("+00:00", "Z")


class TransportCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.dir = Path(self.tmp.name)
        self.snapshot = self.dir / "snapshot.jsonl"
        self.catalog = self.dir / "catalog.json"
        write_snapshot(self.snapshot)
        write_catalog(self.catalog, stations=3)
        self.kafka = FakeKafka()
        self.store = FakeStore()
        self.clock = Clock()

    def build(self, name="run", **config):
        config.setdefault("profile", "mixed-v1")
        config.setdefault("faults", list(FAULT_TYPES))
        config.setdefault("fraction", 0.5)
        config.setdefault("group_scenarios", ["missing_interval", "correction_to_null"])
        config.setdefault("catalog_path", str(self.catalog))
        run = self.dir / name
        summary = build_replay(self.snapshot, run, config=make_config(**config))
        return run, summary["run_id"]

    def publish(self, run, **kwargs):
        return publish_replay(
            run, manager_factory=self.kafka.manager, clock=self.clock.text, **kwargs
        )

    def dispatcher(self, run_id):
        return ReplayDispatcher(self.store, run_id=run_id, rule_version=RULE_VERSION, clock=self.clock)


class PublicationTests(TransportCase):
    def test_wrappers_are_sent_in_release_order_with_acks_logged(self):
        run, run_id = self.build()
        summary = self.publish(run)
        sent = [json.loads(r["value"]) for r in self.kafka.records]
        self.assertEqual(
            [w["scheduled_release_index"] for w in sent], sorted(w["scheduled_release_index"] for w in sent)
        )
        self.assertTrue(all(r["topic"] == REPLAY_TOPIC for r in self.kafka.records))
        self.assertTrue(all(w["published_at"] for w in sent))
        log = json.loads(Path(summary["log_path"]).read_text())
        self.assertEqual(log["run_id"], run_id)
        self.assertEqual(log["acknowledged"], log["planned"])
        self.assertEqual(len(log["acks"]), len(sent))
        self.assertIsNone(log["error"])
        # Delivery IDs and inner payloads are exactly the built ones.
        built = [json.loads(l) for l in (run / "replay.jsonl").read_text().splitlines()]
        strip = lambda w: {k: v for k, v in w.items() if k != "published_at"}
        self.assertEqual(
            sorted(map(canonical_json, map(strip, sent))), sorted(map(canonical_json, map(strip, built)))
        )

    def test_release_schedule_is_honored_even_if_the_file_order_differs(self):
        run, _ = self.build()
        replay = run / "replay.jsonl"
        lines = replay.read_text().splitlines()
        replay.write_text("\n".join(reversed(lines)) + "\n")
        self.publish(run)
        sent = [json.loads(r["value"])["scheduled_release_index"] for r in self.kafka.records]
        self.assertEqual(sent, sorted(sent))
        self.assertEqual(len(sent), len(lines))

    def test_a_failed_send_is_retried_with_identical_bytes(self):
        run, _ = self.build()
        self.kafka.fail_first = 2
        summary = self.publish(run)
        self.assertEqual(summary["retried_sends"], 2)
        self.assertEqual(self.kafka.attempts[0], self.kafka.attempts[1])
        self.assertEqual(self.kafka.attempts[1], self.kafka.attempts[2])

    def test_exhausted_retries_raise_and_still_write_the_log(self):
        run, _ = self.build()
        self.kafka.fail_first = 10
        with self.assertRaises(RuntimeError):
            self.publish(run, max_attempts=2)
        log = json.loads((run / "publications" / "publication-001.json").read_text())
        self.assertIn("failed after 2 attempts", log["error"])
        self.assertEqual(log["acknowledged"], 0)

    def test_repeated_publication_gets_a_new_log_and_live_topics_are_refused(self):
        run, _ = self.build()
        self.publish(run)
        second = self.publish(run)
        self.assertEqual(second["publication"], 2)
        self.assertTrue((run / "publications" / "publication-002.json").exists())
        for topic in ("air_quality_raw", "weather_raw", "source_catalog", ""):
            with self.subTest(topic=topic), self.assertRaises(ValueError):
                self.publish(run, topic=topic)


class DispatchTests(TransportCase):
    def setUp(self):
        super().setUp()
        self.run_dir, self.run_id = self.build()
        self.publish(self.run_dir)

    def test_stored_decisions_equal_local_processing(self):
        counts = dispatch_records(self.kafka.records, self.dispatcher(self.run_id))
        self.assertEqual(counts["decided"], len(self.kafka.records))
        self.assertEqual(counts["rejected"] + counts["redelivered"] + counts["other_run"], 0)
        local = process_replay(self.run_dir / "replay.jsonl", rule_version=RULE_VERSION)
        stored = results_from_store(
            self.store.iter_quality_decisions(self.run_id, RULE_VERSION),
            replay_path=self.run_dir / "replay.jsonl",
            rule_version=RULE_VERSION,
        )
        self.assertEqual(
            {r["key"]: r["decision"] for r in stored["decisions"]},
            {r["key"]: r["decision"] for r in local["decisions"]},
        )
        self.assertEqual(stored["input"], local["input"])

    def test_spark_retry_of_a_batch_changes_nothing(self):
        dispatcher = self.dispatcher(self.run_id)
        dispatch_records(self.kafka.records, dispatcher)
        before = (dict(self.store.decisions), dict(self.store.receipts))
        again = dispatch_records(self.kafka.records, dispatcher)
        self.assertEqual(again["decided"], len(self.kafka.records))  # same records: still first
        self.assertEqual((self.store.decisions, self.store.receipts), before)

    def test_republication_adds_receipts_but_no_decisions(self):
        self.publish(self.run_dir)
        counts = dispatch_records(self.kafka.records, self.dispatcher(self.run_id))
        planned = len(self.kafka.records) // 2
        self.assertEqual((counts["decided"], counts["redelivered"]), (planned, planned))
        self.assertEqual(len(self.store.decisions), planned)
        self.assertEqual(len(self.store.receipts), 2 * planned)

    def test_other_runs_are_skipped_and_bad_wrappers_rejected_with_bytes(self):
        good = self.kafka.records[0]
        bad = [
            {**good, "offset": 900, "value": b"{not json"},
            {**good, "offset": 901, "value": canonical_json({"run_id": self.run_id}).encode()},
            {**good, "offset": 902, "topic": "air_quality_raw"},
            {**good, "offset": 903, "value": None},
        ]
        other = json.loads(good["value"])
        other["run_id"] = "someone-else"
        counts = dispatch_records(
            bad + [{**good, "offset": 904, "value": canonical_json(other).encode()}],
            self.dispatcher(self.run_id),
        )
        self.assertEqual((counts["rejected"], counts["other_run"], counts["decided"]), (4, 1, 0))
        reasons = sorted(r["reason"] for r in self.store.rejections.values())
        self.assertEqual(reasons, ["invalid_wrapper", "invalid_wrapper", "invalid_wrapper", "wrong_topic"])
        kept = {r["offset"]: r["value"] for r in self.store.rejections.values()}
        self.assertEqual(kept[900], b"{not json")
        self.assertFalse(self.store.decisions)

    def test_same_delivery_id_with_different_bytes_is_a_conflict(self):
        dispatcher = self.dispatcher(self.run_id)
        first = self.kafka.records[0]
        dispatcher.process(first)
        changed = json.loads(first["value"])
        other = json.loads(next(r["value"] for r in self.kafka.records[1:]))
        changed["payload_base64"], changed["payload_sha256"] = other["payload_base64"], other["payload_sha256"]
        outcome = dispatcher.process({**first, "offset": 999, "value": canonical_json(changed).encode()})
        self.assertEqual(outcome, "rejected")
        (rejection,) = self.store.rejections.values()
        self.assertEqual(rejection["reason"], "conflicting_redelivery")
        self.assertEqual(rejection["delivery_id"], changed["delivery_id"])
        self.assertEqual(len(self.store.receipts), 1)

    def test_partition_adapter_uses_the_factory_and_handles_empty_partitions(self):
        counts = process_replay_partition(
            iter(self.kafka.records[:3]),
            mongo_uri="mongodb://x",
            experiment_database="exp",
            run_id=self.run_id,
            rule_version=RULE_VERSION,
            repository_factory=self.store,
        )
        self.assertEqual(counts["decided"], 3)
        self.assertEqual(self.store.opened, ("mongodb://x", "exp"))
        self.assertTrue(self.store.closed)
        fresh = FakeStore()
        empty = process_replay_partition(
            iter([]), mongo_uri="u", experiment_database="exp", run_id=self.run_id,
            rule_version=RULE_VERSION, repository_factory=fresh,
        )
        self.assertEqual(sum(empty.values()), 0)
        self.assertFalse(hasattr(fresh, "opened"))

    def test_naive_broker_timestamps_are_stored_as_utc(self):
        dispatch_records(self.kafka.records[:1], self.dispatcher(self.run_id))
        (receipt,) = self.store.receipts.values()
        self.assertEqual(receipt["broker_timestamp"].tzinfo, UTC)

    def test_configuration_errors_raise(self):
        with self.assertRaises(ValueError):
            ReplayDispatcher(self.store, run_id="", rule_version=RULE_VERSION)
        with self.assertRaises(ValueError):
            ReplayDispatcher(self.store, run_id="r", rule_version="rules-v0")


class GradingTests(TransportCase):
    """Export from the store, then grade with the unchanged evaluator."""

    def grade(self, run, name="spark"):
        out = run / name
        transport = export_store(self.store, run, out)
        save_results(process_snapshot(self.snapshot, rule_version=RULE_VERSION), out / "baseline_results.json")
        report = evaluate_run(
            manifest_path=run / "manifest.json",
            baseline_results_path=out / "baseline_results.json",
            experiment_results_path=out / "experiment_results.json",
            output_directory=out / "evaluation",
            catalog_path=self.catalog,
            transport_path=out / "transport.json",
        )
        return transport, report

    def test_full_path_passes_with_transport_checks_and_timeliness(self):
        run, run_id = self.build()
        self.publish(run)
        dispatch_records(self.kafka.records, self.dispatcher(run_id))
        transport, report = self.grade(run)
        self.assertTrue(report["passed"], report["checks"])
        for name, value in transport["checks"].items():
            self.assertTrue(value, name)
            self.assertTrue(report["checks"][f"transport_{name}"])
        self.assertNotIn("timeliness", report["not_assessed"])
        self.assertIn("restart_recovery", report["not_assessed"])
        latency = transport["latency_seconds"]
        self.assertEqual(latency["count"], len(self.kafka.records))
        self.assertGreater(latency["p50"], 0)

    def test_repeated_publication_still_passes_and_is_counted(self):
        run, run_id = self.build()
        self.publish(run)
        self.publish(run)
        dispatch_records(self.kafka.records, self.dispatcher(run_id))
        transport, report = self.grade(run)
        planned = transport["planned_deliveries"]
        self.assertTrue(report["passed"], report["checks"])
        self.assertEqual(transport["publications"], 2)
        self.assertEqual(transport["received_records"], 2 * planned)
        self.assertEqual(transport["redelivered_deliveries"], planned)
        self.assertEqual(transport["decided_deliveries"], planned)
        by_publication = transport["latency_seconds"]["by_publication"]
        self.assertEqual(sorted(by_publication), ["1", "2"])
        self.assertEqual([v["count"] for v in by_publication.values()], [planned, planned])

    def test_records_lost_in_transport_fail_the_run(self):
        run, run_id = self.build()
        self.publish(run)
        dispatch_records(self.kafka.records[:-2], self.dispatcher(run_id))  # consumer missed two
        transport, report = self.grade(run)
        self.assertFalse(report["passed"])
        self.assertEqual(transport["acknowledged_not_processed"], 2)
        self.assertEqual(transport["missing_deliveries"], 2)
        self.assertFalse(report["checks"]["transport_every_acknowledged_record_processed"])
        self.assertFalse(report["checks"]["delivery_accounting_balanced"])

    def test_invalid_wrappers_fail_the_transport_check(self):
        run, run_id = self.build()
        self.publish(run)
        records = self.kafka.records + [{**self.kafka.records[0], "offset": 777, "value": b"x"}]
        dispatch_records(records, self.dispatcher(run_id))
        transport, report = self.grade(run)
        self.assertEqual(transport["rejections_by_reason"], {"invalid_wrapper": 1})
        self.assertFalse(report["checks"]["transport_no_invalid_wrappers"])
        self.assertFalse(report["passed"])

    def test_transport_summary_from_another_run_is_refused(self):
        run, run_id = self.build()
        self.publish(run)
        dispatch_records(self.kafka.records, self.dispatcher(run_id))
        out = run / "spark"
        export_store(self.store, run, out)
        save_results(process_snapshot(self.snapshot, rule_version=RULE_VERSION), out / "baseline_results.json")
        transport = json.loads((out / "transport.json").read_text())
        transport["run_id"] = "another"
        forged = self.dir / "forged.json"
        forged.write_text(json.dumps(transport))
        with self.assertRaises(ValueError):
            evaluate_run(
                manifest_path=run / "manifest.json",
                baseline_results_path=out / "baseline_results.json",
                experiment_results_path=out / "experiment_results.json",
                output_directory=self.dir / "eval",
                transport_path=forged,
            )

    def test_summary_without_publication_logs_does_not_claim_ack_coverage(self):
        run, run_id = self.build()
        self.publish(run)
        dispatch_records(self.kafka.records, self.dispatcher(run_id))
        summary = transport_summary(
            replay_path=run / "replay.jsonl",
            rule_version=RULE_VERSION,
            decisions=self.store.iter_quality_decisions(run_id, RULE_VERSION),
            receipts=self.store.iter_quality_receipts(run_id, RULE_VERSION),
            rejections=[],
            publications=[],
        )
        self.assertIsNone(summary["checks"]["every_acknowledged_record_processed"])


class IsolationTests(unittest.TestCase):
    def test_quality_spark_job_never_imports_the_evaluator_or_injector(self):
        tree = ast.parse((ROOT / "streaming" / "quality_spark.py").read_text())
        imported = {a.name for n in ast.walk(tree) if isinstance(n, ast.Import) for a in n.names}
        imported |= {n.module or "" for n in ast.walk(tree) if isinstance(n, ast.ImportFrom)}
        for module in imported:
            self.assertNotIn("quality_evaluation", module)
            self.assertNotIn("quality_replay", module)

    def test_live_consumer_does_not_subscribe_to_the_replay_topic(self):
        source = (ROOT / "streaming" / "processing.py").read_text()
        self.assertNotIn(REPLAY_TOPIC, source)

    def test_compose_keeps_the_quality_job_out_of_the_live_profiles(self):
        text = (ROOT / "compose.yaml").read_text()
        block = text[text.index("  quality-spark:"):]
        self.assertIn('profiles: ["quality"]', block.split("\n\n")[0])
        self.assertIn("--topic quality_replay_raw", text)


@unittest.skipIf(pymongo is None, "pymongo is not installed")
class ExperimentDatabaseGuardTests(unittest.TestCase):
    def test_live_database_names_are_refused(self):
        from database.quality_schema import check_experiment_database

        env = {"MONGO_DATABASE": "serving", "MONGO_COLLECTOR_DATABASE": "collector"}
        for name in ("serving", "collector", "urban_air_quality", "urban_air_quality_collector", "", None):
            with self.subTest(name=name), self.assertRaises(ValueError):
                check_experiment_database(name, env)
        self.assertEqual(check_experiment_database("urban_air_quality_quality", env), "urban_air_quality_quality")

    def test_schema_definitions_cover_every_collection(self):
        from database.quality_schema import QUALITY_COLLECTIONS, quality_schema_definitions

        definitions = quality_schema_definitions()
        self.assertEqual(set(definitions["validators"]), set(QUALITY_COLLECTIONS))
        self.assertEqual(set(definitions["indexes"]), set(QUALITY_COLLECTIONS))

    def test_dispatcher_documents_satisfy_the_required_fields(self):
        from database.quality_schema import VALIDATORS

        case = TransportCase("run")
        case.setUp()
        self.addCleanup(case.tmp.cleanup)
        run, run_id = case.build()
        case.publish(run)
        case.kafka.records.append({**case.kafka.records[0], "offset": 555, "value": b"x"})
        dispatch_records(case.kafka.records, case.dispatcher(run_id))
        for collection, table in (
            ("quality_decisions", case.store.decisions),
            ("quality_receipts", case.store.receipts),
            ("quality_rejections", case.store.rejections),
        ):
            required = VALIDATORS[collection]["$jsonSchema"]["required"]
            for document in table.values():
                self.assertFalse(set(required) - set(document), collection)

    def test_quality_spark_refuses_live_targets(self):
        try:
            from streaming.quality_spark import parser, validate_args
        except ImportError as error:  # pragma: no cover - needs streaming deps
            self.skipTest(str(error))
        env = {"MONGO_DATABASE": "urban_air_quality"}
        ok = parser().parse_args(["--run-id", "r1", "--database", "exp"])
        self.assertEqual(validate_args(ok, env), "data/checkpoints/quality/r1")
        for argv in (
            ["--run-id", "r1", "--database", "urban_air_quality"],
            ["--run-id", "r1"],
            ["--run-id", "r1", "--database", "exp", "--topic", "air_quality_raw"],
            ["--run-id", "r1", "--database", "exp", "--checkpoint", "data/checkpoints/spark-mongo"],
            ["--run-id", "a/b", "--database", "exp"],
        ):
            with self.subTest(argv=argv), self.assertRaises(ValueError):
                validate_args(parser().parse_args(argv), env)


if __name__ == "__main__":
    unittest.main()
