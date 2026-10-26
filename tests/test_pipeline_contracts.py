"""Collector envelopes must pass the Spark-side validators after Kafka JSON transit."""

from datetime import UTC, datetime, timedelta
import json
import unittest

try:
    import pymongo  # noqa: F401
except ImportError as error:
    raise unittest.SkipTest(
        f"collector dependencies missing ({error.name}); "
        'install with: pip install -e ".[collectors]"'
    ) from None

from collectors.http import Response
from collectors.openaq import adapt_measurement
from collectors.service import stage_latest, stage_measurements
from collectors.storage import CollectorStore, rejected_envelope
from collectors.weather import VARIABLES, adapt_weather
from models import canonical_json
from streaming.processing import EventProcessor, prepare_record


COLLECTED = datetime(2026, 1, 1, 6, tzinfo=UTC)
SENSOR = {"sensor_id": 42, "location_id": 7}
PARAMETER = {"id": 2, "name": "pm25", "units": "µg/m³"}


def response(payload, source="openaq"):
    return Response(
        source, "https://example.test", {}, payload, json.dumps(payload), {},
        COLLECTED, "request-1",
    )


def measurement_row(value=12.5):
    return {
        "value": value,
        "flagInfo": {"hasFlags": False},
        "parameter": PARAMETER,
        "period": {
            "datetimeFrom": {"utc": "2026-01-01T00:00:00Z"},
            "datetimeTo": {"utc": "2026-01-01T01:00:00Z"},
        },
    }


def latest_row(value=8.0):
    return {
        "sensorsId": 42,
        "locationsId": 7,
        "value": value,
        "datetime": {"utc": "2026-01-01T05:00:00Z", "local": "2026-01-01T12:00:00+07:00"},
        "coordinates": {"latitude": 21.0, "longitude": 105.8},
    }


def weather_payload(times=("2026-01-01T00:00", "2026-01-01T01:00")):
    hourly = {"time": list(times)}
    for index, variable in enumerate(VARIABLES):
        hourly[variable] = [float(index)] * len(times)
    hourly["precipitation"][-1] = None
    return {
        "latitude": 21.0,
        "longitude": 105.75,
        "utc_offset_seconds": 0,
        "hourly_units": {variable: "unit" for variable in VARIABLES},
        "hourly": hourly,
    }


def kafka_record(topic, envelope, offset=0):
    """Mirror the producer serializer and the Spark Kafka row shape."""
    return {
        "topic": topic,
        "partition": 0,
        "offset": offset,
        "value": canonical_json(envelope).encode("utf-8"),
    }


class RecordingRepository:
    """Stands in for both the collector database and the serving database."""

    def __init__(self):
        self.calls = []

    def __getattr__(self, name):
        def record(*args, **kwargs):
            self.calls.append((name, args, kwargs))
            if name == "save_station":
                return {"_id": "openaq:" + str(args[0]["id"])}
            if name == "remember_latest_snapshot":
                return True
            if name == "get_station":
                return {"_id": args[0]}
            return None

        return record

    def named(self, name):
        return [call for call in self.calls if call[0] == name]


class StagingStore(CollectorStore):
    def __init__(self):
        super().__init__(repository=RecordingRepository())

    def remember_latest_snapshot(self, *_args):
        return True

    def enqueued(self):
        return [
            {"topic": args[2], "key": args[3], "envelope": args[4]}
            for _name, args, _kwargs in self.mongodb.named("enqueue")
        ]


class PipelineContractTests(unittest.TestCase):
    def process(self, topic, envelope):
        repository = RecordingRepository()
        EventProcessor(repository).process(kafka_record(topic, envelope, offset=9))
        return repository

    def assert_accepted(self, topic, envelope, saved):
        repository = self.process(topic, envelope)
        self.assertEqual(repository.named("record_stream_rejection"), [])
        self.assertEqual(len(repository.named(saved)), 1)
        return repository.named(saved)[0]

    def test_measurement_from_collector_is_accepted(self):
        store = StagingStore()
        stage_measurements(store, response({"results": [measurement_row()]}), SENSOR)
        (queued,) = store.enqueued()
        self.assertEqual((queued["topic"], queued["key"]), ("air_quality_raw", "openaq:7"))
        _, (envelope,), _ = self.assert_accepted(
            "air_quality_raw", queued["envelope"], "save_measurement"
        )
        self.assertEqual(envelope["raw_response_id"], "request-1")
        self.assertEqual(
            envelope["kafka"], {"topic": "air_quality_raw", "partition": 0, "offset": 9}
        )

    def test_tampered_measurement_is_rejected_not_saved(self):
        envelope = adapt_measurement(measurement_row(), SENSOR, COLLECTED, COLLECTED)
        envelope["value"] = 99
        repository = self.process("air_quality_raw", envelope)
        self.assertEqual(repository.named("save_measurement"), [])
        (_, (_record, reason), _) = repository.named("record_stream_rejection")[0]
        self.assertIn("identity/revision", reason)

    def test_collector_rejection_reaches_rejection_store(self):
        store = StagingStore()
        stage_measurements(store, response({"results": [{"value": 1}]}), SENSOR)
        (queued,) = store.enqueued()
        self.assertEqual(queued["envelope"]["status"], "rejected")
        repository = self.process("air_quality_raw", queued["envelope"])
        (_, (_record, reason), _) = repository.named("record_stream_rejection")[0]
        self.assertTrue(reason.startswith("collector rejected input:"))

    def test_latest_snapshot_from_collector_is_accepted(self):
        store = StagingStore()
        stage_latest(store, response({"results": [latest_row()]}), PARAMETER)
        (queued,) = store.enqueued()
        self.assertEqual(queued["topic"], "air_quality_latest")
        _, args, kwargs = self.assert_accepted(
            "air_quality_latest", queued["envelope"], "save_latest_snapshot"
        )
        self.assertEqual(args[1], {"name": "pm25", "units": "µg/m³"})
        self.assertEqual(kwargs, {"track_dispatch": False})

    def test_future_latest_snapshot_is_rejected_by_collector(self):
        row = latest_row()
        row["datetime"]["utc"] = (COLLECTED + timedelta(hours=1)).isoformat()
        store = StagingStore()
        stage_latest(store, response({"results": [row]}), PARAMETER)
        (queued,) = store.enqueued()
        self.assertEqual(queued["envelope"]["status"], "rejected")

    def test_weather_from_collector_is_accepted_with_missing_flags(self):
        location = {
            "location_id": "grid:21.00,105.75",
            "station_ids": ["openaq:7"],
            "latitude": 21.0,
            "longitude": 105.75,
        }
        envelopes = adapt_weather(
            response(weather_payload(), "open-meteo"), location, "gfs_seamless"
        )
        self.assertEqual(len(envelopes), 2)
        self.assertNotEqual(envelopes[0]["event_id"], envelopes[1]["event_id"])
        _, (saved,), _ = self.assert_accepted("weather_raw", envelopes[-1], "save_weather")
        self.assertEqual(saved["quality_flags"], ["missing:precipitation"])

    def test_weather_rejects_duplicate_or_local_hours(self):
        location = {"location_id": "x", "station_ids": [], "latitude": 0, "longitude": 0}
        duplicate = weather_payload(("2026-01-01T00:00", "2026-01-01T00:00"))
        local = {**weather_payload(), "utc_offset_seconds": 25200}
        for payload in (duplicate, local):
            with self.subTest(payload=payload), self.assertRaises(ValueError):
                adapt_weather(response(payload, "open-meteo"), location, "gfs_seamless")

    def test_catalog_station_routes_weather_mapping(self):
        store = StagingStore()
        station = {
            "id": 7,
            "country": {"code": "VN"},
            "sensors": [{"id": 42, "parameter": {"name": "pm25"}}],
        }
        store.enqueue_catalog(
            "station_metadata", station, weather_enabled=True, coordinate_precision=2
        )
        store.enqueue_catalog("parameter_metadata", PARAMETER)
        station_envelope, parameter_envelope = (
            queued["envelope"] for queued in store.enqueued()
        )
        repository = self.process("source_catalog", station_envelope)
        self.assertEqual(repository.named("record_stream_rejection"), [])
        self.assertEqual(
            [call[0] for call in repository.calls],
            ["save_station", "save_weather_location"],
        )
        self.assertEqual(repository.calls[1][1], ({"_id": "openaq:7"}, 2))
        self.assert_accepted("source_catalog", parameter_envelope, "save_parameter")

    def test_malformed_kafka_values_are_rejected(self):
        envelope = adapt_measurement(measurement_row(), SENSOR, COLLECTED, COLLECTED)
        cases = {
            "invalid utf-8": b"\xff",
            "nonstandard number": b'{"value": NaN}',
            "not an object": b"[]",
            "wrong schema": canonical_json({**envelope, "schema_version": 2}).encode(),
        }
        for label, value in cases.items():
            with self.subTest(label):
                repository = RecordingRepository()
                record = {"topic": "air_quality_raw", "partition": 0, "offset": 1, "value": value}
                EventProcessor(repository).process(record)
                self.assertEqual(len(repository.named("record_stream_rejection")), 1)
                self.assertEqual(repository.named("save_measurement"), [])

    def test_unknown_topic_is_rejected(self):
        envelope = adapt_measurement(measurement_row(), SENSOR, COLLECTED, COLLECTED)
        repository = self.process("unknown_topic", envelope)
        (_, (_record, reason), _) = repository.named("record_stream_rejection")[0]
        self.assertEqual(reason, "unsupported Kafka topic")

    def test_storage_failure_propagates_for_spark_retry(self):
        class FailingRepository(RecordingRepository):
            def save_measurement(self, _envelope):
                raise ConnectionError("mongodb unavailable")

        envelope = adapt_measurement(measurement_row(), SENSOR, COLLECTED, COLLECTED)
        repository = FailingRepository()
        with self.assertRaises(ConnectionError):
            EventProcessor(repository).process(kafka_record("air_quality_raw", envelope))
        self.assertEqual(repository.named("record_stream_rejection"), [])

    def test_rejected_envelope_keeps_provenance(self):
        envelope = rejected_envelope({"raw": True}, response({}), ValueError("bad"), station_id="openaq:7")
        self.assertEqual(envelope["status"], "rejected")
        self.assertEqual(envelope["error"], "bad")
        self.assertEqual(envelope["raw_response_id"], "request-1")
        self.assertEqual(envelope["collected_at"], "2026-01-01T06:00:00Z")
        self.assertEqual(envelope["station_id"], "openaq:7")


if __name__ == "__main__":
    unittest.main()
