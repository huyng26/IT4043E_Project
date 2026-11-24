"""Cadence-aware coverage and frozen-snapshot profiling."""

import json
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from quality_support import START, encode, envelope, write_catalog, write_snapshot

from streaming.quality.coverage import (
    expected_sensor_hours,
    parse_catalog,
    summarize_coverage,
)
from streaming.quality.profiling import profile_snapshot

UTC = timezone.utc


def obs(hour, *, station="s1", sensor="x", usable=True, end_minutes=60):
    start = START + timedelta(hours=hour)
    return {
        "station_id": station, "sensor_id": sensor,
        "period_start": start, "period_end": start + timedelta(minutes=end_minutes),
        "usable": usable,
    }


def expect(hours, *, station="s1", sensor="x"):
    return [
        {"station_id": station, "sensor_id": sensor, "hour": START + timedelta(hours=h)}
        for h in hours
    ]


class CoverageTests(unittest.TestCase):
    def test_counts_expected_received_usable_and_missing(self):
        report = summarize_coverage(
            [obs(0), obs(1, usable=False), obs(5)],  # hour 5 is not expected
            expected_sensor_hours=expect(range(4)),
        )
        self.assertEqual(report["expected_sensor_hours"], 4)
        self.assertEqual(report["received_expected_sensor_hours"], 2)
        self.assertEqual(report["usable_expected_sensor_hours"], 1)
        self.assertEqual(report["unusable_sensor_hours"], 1)
        self.assertEqual(report["missing_sensor_hours"], 2)
        self.assertEqual(report["unexpected_sensor_hours"], 1)
        self.assertEqual(report["completeness"], 0.25)
        self.assertEqual(
            [row["hour"] for row in report["missing_examples"]],
            ["2026-01-01T02:00:00Z", "2026-01-01T03:00:00Z"],
        )

    def test_unknown_expectations_leave_completeness_null_not_complete(self):
        report = summarize_coverage([obs(0), obs(1)], expected_sensor_hours=[])
        self.assertIsNone(report["expected_sensor_hours"])
        self.assertIsNone(report["completeness"])
        self.assertIsNone(report["missing_sensor_hours"])
        self.assertEqual(report["observed_sensor_hours"], 2)

    def test_duplicates_cannot_improve_completeness(self):
        once = summarize_coverage([obs(0)], expected_sensor_hours=expect(range(2)))
        many = summarize_coverage([obs(0)] * 5, expected_sensor_hours=expect(range(2)))
        self.assertEqual(once, many)

    def test_string_timestamps_with_offsets_match_utc_hours(self):
        row = {**obs(0), "period_start": "2026-01-01T07:00:00+07:00"}
        report = summarize_coverage([row], expected_sensor_hours=expect([0]))
        self.assertEqual(report["usable_expected_sensor_hours"], 1)

    def test_sensors_are_distinguished_and_output_is_deterministic(self):
        rows = [obs(0, sensor="a"), obs(0, sensor="b", usable=False)]
        wanted = expect([0], sensor="a") + expect([0], sensor="b") + expect([0], sensor="c")
        first = summarize_coverage(rows, expected_sensor_hours=wanted)
        self.assertEqual(first["missing_examples"][0]["sensor_id"], "c")
        self.assertEqual(first, summarize_coverage(rows[::-1], expected_sensor_hours=wanted[::-1]))


class CatalogTests(unittest.TestCase):
    def sensors(self, **changes):
        sensor = {
            "station_id": "s1", "sensor_id": "x", "expected_period_seconds": 3600,
            "active_from": None, "active_to": None, **changes,
        }
        return parse_catalog({"schema_version": 1, "sensors": [sensor]})

    def test_expected_hours_follow_window_and_activity_bounds(self):
        window = dict(window_start=START, window_end=START + timedelta(hours=6))
        self.assertEqual(len(expected_sensor_hours(self.sensors(), **window)), 6)
        bounded = self.sensors(
            active_from="2026-01-01T02:00:00Z", active_to="2026-01-01T05:00:00Z"
        )
        hours = [row["hour"] for row in expected_sensor_hours(bounded, **window)]
        self.assertEqual(hours, [START + timedelta(hours=h) for h in (2, 3, 4)])

    def test_partial_hours_are_not_expected(self):
        rows = expected_sensor_hours(
            self.sensors(),
            window_start=START + timedelta(minutes=30),
            window_end=START + timedelta(hours=3, minutes=30),
        )
        self.assertEqual([r["hour"] for r in rows], [START + timedelta(hours=h) for h in (1, 2)])

    def test_unknown_and_non_hourly_cadence_expect_nothing(self):
        window = dict(window_start=START, window_end=START + timedelta(hours=6))
        for cadence in (None, 900, 7200):
            with self.subTest(cadence=cadence):
                self.assertEqual(
                    expected_sensor_hours(self.sensors(expected_period_seconds=cadence), **window), []
                )

    def test_runaway_windows_are_bounded(self):
        with self.assertRaises(ValueError):
            expected_sensor_hours(
                self.sensors(), window_start=START, window_end=START + timedelta(hours=50), max_expected=10
            )

    def test_catalog_validation(self):
        good = {"station_id": "s", "sensor_id": "x", "expected_period_seconds": 3600,
                "active_from": None, "active_to": None}
        for name, catalog in {
            "not object": [],
            "version": {"schema_version": 2, "sensors": []},
            "sensors type": {"schema_version": 1, "sensors": {}},
            "extra field": {"schema_version": 1, "sensors": [{**good, "extra": 1}]},
            "bad cadence": {"schema_version": 1, "sensors": [{**good, "expected_period_seconds": 0}]},
            "bool cadence": {"schema_version": 1, "sensors": [{**good, "expected_period_seconds": True}]},
            "empty id": {"schema_version": 1, "sensors": [{**good, "sensor_id": ""}]},
            "duplicate": {"schema_version": 1, "sensors": [good, good]},
        }.items():
            with self.subTest(name), self.assertRaises(ValueError):
                parse_catalog(catalog)


class ProfilingTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.dir = Path(self.tmp.name)

    def write(self, name, lines):
        path = self.dir / name
        path.write_bytes(b"".join(line + b"\n" for line in lines))
        return path

    def test_clean_snapshot_with_a_silent_sensor(self):
        snapshot = self.dir / "clean.jsonl"
        write_snapshot(snapshot, stations=3, hours=24)
        write_catalog(self.dir / "cat.json", stations=4)
        before = snapshot.read_bytes()
        report = profile_snapshot(snapshot, catalog_path=self.dir / "cat.json")
        self.assertEqual(snapshot.read_bytes(), before)  # input is never rewritten
        self.assertEqual(report["snapshot"]["deliveries"], 72)
        self.assertEqual(report["deliveries"]["quarantined"], 0)
        self.assertEqual(report["deliveries"]["quality_flags"], {})
        self.assertEqual(report["units"], {"ug/m3": 72})
        self.assertEqual(report["normalized_values"], {"positive": 69, "zero": 3})
        self.assertEqual(report["observed_interval_seconds"], {"3600": 69})
        self.assertEqual(report["sensors"], {
            "observed": 3, "observed_not_in_catalog": 0, "catalog_without_observations": 1,
        })
        coverage = report["coverage"]
        self.assertEqual(coverage["expected_sensor_hours"], 96)
        self.assertEqual(coverage["missing_sensor_hours"], 24)
        self.assertEqual(coverage["completeness"], 0.75)
        self.assertEqual(report["catalog"]["sensors_with_known_cadence"], 4)

    def test_natural_defects_are_separated_by_kind(self):
        lines = [
            encode(envelope(hour=0)),
            encode(envelope(hour=0)),  # repeat delivery
            encode(envelope(hour=1, value=0.035, unit="mg/m3")),  # supported conversion
            encode(envelope(hour=2, value=-5)),  # flagged, still a delivery
            encode(envelope(hour=3, value=None)),
            encode({**envelope(hour=4), "value": "unknown"}),  # unparseable -> quarantine
            b"{truncated",  # malformed -> quarantine
            encode(envelope(hour=0, value=41, collected_at="2026-01-01T03:00:00Z",
                            ingested_at="2026-01-01T03:01:00Z")),  # correction
            encode(envelope(hour=5, value=12, unit="ppm")),
        ]
        report = profile_snapshot(self.write("noisy.jsonl", lines))
        self.assertEqual(report["deliveries"]["accepted"], 7)
        self.assertEqual(report["deliveries"]["quarantined"], 2)
        self.assertEqual(report["deliveries"]["quarantine_reasons"],
                         {"invalid_json": 1, "unparseable_value": 1})
        self.assertEqual(report["deliveries"]["quality_flags"], {
            "missing_value": 1, "negative_concentration": 1, "unsupported_unit": 1,
        })
        self.assertEqual(report["deliveries"]["repair_actions"], {"unit_converted_to_ug_m3": 1})
        # Hour 4 is quarantined, so five events; hour 0 has two revisions (a
        # correction) plus one repeat delivery; only the corrected hour 0 and the
        # converted hour 1 remain usable.
        self.assertEqual(report["observations"], {
            "unique_events": 5, "unique_revisions": 6, "duplicate_deliveries": 1,
            "events_with_multiple_revisions": 1, "current": 5, "usable": 2,
        })
        self.assertEqual(report["units"], {"mg/m3": 1, "ppm": 1, "ug/m3": 5})
        self.assertIsNone(report["catalog"])
        self.assertIsNone(report["coverage"]["expected_sensor_hours"])  # unknown, not complete
        self.assertIsNone(report["coverage"]["completeness"])

    def test_profile_is_deterministic_serializable_and_handles_empty_input(self):
        snapshot = self.write("one.jsonl", [encode(envelope())])
        first = profile_snapshot(snapshot)
        self.assertEqual(first, profile_snapshot(snapshot))
        json.dumps(first, allow_nan=False)
        empty = profile_snapshot(self.write("empty.jsonl", []))
        self.assertEqual(empty["snapshot"]["deliveries"], 0)
        self.assertIsNone(empty["time_range"]["first_period_start"])

    def test_catalog_is_consulted_for_unknown_cadence(self):
        snapshot = self.dir / "s.jsonl"
        write_snapshot(snapshot, stations=1, hours=3)
        write_catalog(self.dir / "cat.json", stations=1, cadence=None)
        report = profile_snapshot(snapshot, catalog_path=self.dir / "cat.json")
        self.assertEqual(report["catalog"]["sensors_with_known_cadence"], 0)
        self.assertIsNone(report["coverage"]["completeness"])


if __name__ == "__main__":
    unittest.main()
