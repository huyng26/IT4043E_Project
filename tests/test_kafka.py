"""Offline checks for the Kafka producer wrapper and its serializers."""

from types import SimpleNamespace
import unittest
from unittest.mock import patch

from streaming.kafka import KafkaManager


class FakeFuture:
    def __init__(self, result):
        self.result = result
        self.timeout = None

    def get(self, timeout):
        self.timeout = timeout
        return self.result


class FakeProducer:
    def __init__(self, **options):
        self.options = options
        self.sent = []
        self.flushed = []
        self.closed = []
        self.future = FakeFuture({"topic": "events", "partition": 1, "offset": 7})

    def send(self, topic, *, key, value):
        self.sent.append((topic, key, value))
        return self.future

    def flush(self, *, timeout):
        self.flushed.append(timeout)

    def close(self, *, timeout):
        self.closed.append(timeout)


class KafkaManagerTests(unittest.TestCase):
    def make_manager(self, servers="broker-a:9092, broker-b:9092,,"):
        self.producers = []

        def producer_factory(**options):
            producer = FakeProducer(**options)
            self.producers.append(producer)
            return producer

        kafka_module = SimpleNamespace(KafkaProducer=producer_factory)
        with patch.dict("sys.modules", {"kafka": kafka_module}):
            manager = KafkaManager(servers)
        return manager, self.producers[0]

    def test_bootstrap_servers_and_serializers_are_configured(self):
        manager, producer = self.make_manager()
        self.addCleanup(manager.close)

        self.assertEqual(
            producer.options["bootstrap_servers"], ["broker-a:9092", "broker-b:9092"]
        )
        self.assertEqual(producer.options["key_serializer"]("openaq:7"), b"openaq:7")
        self.assertEqual(
            producer.options["value_serializer"]({"z": 1, "a": [True]}),
            b'{"a":[true],"z":1}',
        )
        self.assertEqual(producer.options["acks"], "all")

    def test_publish_waits_for_ack_and_returns_metadata(self):
        manager, producer = self.make_manager()

        result = manager.publish(
            "events", key="openaq:7", value={"value": 12}, timeout=4
        )

        self.assertEqual(result, {"topic": "events", "partition": 1, "offset": 7})
        self.assertEqual(producer.sent, [("events", "openaq:7", {"value": 12})])
        self.assertEqual(producer.future.timeout, 4)

    def test_context_manager_closes_producer_once_and_rejects_later_use(self):
        manager, producer = self.make_manager()

        with manager as active:
            self.assertIs(active, manager)
            manager.flush(timeout=3)
        manager.close(timeout=9)

        self.assertEqual(producer.flushed, [3])
        self.assertEqual(producer.closed, [5])
        with self.assertRaisesRegex(RuntimeError, "closed"):
            manager.send("events", value={})
        with self.assertRaisesRegex(RuntimeError, "closed"):
            manager.flush()

    def test_empty_bootstrap_list_is_rejected_before_loading_kafka(self):
        with self.assertRaisesRegex(ValueError, "at least one broker"):
            KafkaManager(" , ")


if __name__ == "__main__":
    unittest.main()
