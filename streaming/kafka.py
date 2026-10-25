"""Kafka producer lifecycle and serialization shared by event publishers."""

import os

from models import canonical_json


class KafkaManager:
    """Own one producer; use as a context manager to close it after publication.

    ``send`` returns a Kafka future. Call ``get`` on it, or use ``publish``,
    before committing delivery state in another system.
    """

    def __init__(self, bootstrap_servers: str | list[str] | None = None):
        servers = (
            os.getenv("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092")
            if bootstrap_servers is None
            else bootstrap_servers
        )
        if isinstance(servers, str):
            servers = servers.split(",")
        self.bootstrap_servers = [
            server.strip() for server in servers if server.strip()
        ]
        if not self.bootstrap_servers:
            raise ValueError("KAFKA_BOOTSTRAP_SERVERS must contain at least one broker")

        # Keep kafka-python optional for local replay and measurement processing.
        from kafka import KafkaProducer

        self._producer = KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            key_serializer=lambda key: key.encode("utf-8"),
            value_serializer=lambda value: canonical_json(value).encode("utf-8"),
            acks="all",
            retries=5,
            max_in_flight_requests_per_connection=1,
            request_timeout_ms=30000,
            max_block_ms=10000,
        )
        self._closed = False

    def __enter__(self):
        self._check_open()
        return self

    def __exit__(self, *_):
        self.close()

    def _check_open(self):
        if self._closed:
            raise RuntimeError("KafkaManager is closed")

    def send(self, topic, *, key=None, value=None):
        """Queue a record and return its acknowledgement future."""
        self._check_open()
        return self._producer.send(topic, key=key, value=value)

    def publish(self, topic, *, key=None, value=None, timeout=30):
        """Wait for the broker ACK and return topic/partition/offset metadata."""
        return self.send(topic, key=key, value=value).get(timeout=timeout)

    def flush(self, timeout=30):
        self._check_open()
        self._producer.flush(timeout=timeout)

    def close(self, timeout=5):
        """Close the producer, allowing a bounded wait for outstanding records."""
        if not self._closed:
            try:
                self._producer.close(timeout=timeout)
            finally:
                self._closed = True
