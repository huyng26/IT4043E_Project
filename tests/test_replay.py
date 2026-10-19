"""Regression checks for the first local monitoring slice."""

from copy import deepcopy
import itertools
import json
from pathlib import Path
import subprocess
import sys
import unittest

from streaming.measurements import parse_measurement
from streaming.replay import replay


ROOT = Path(__file__).resolve().parents[1]
FIXTURE = ROOT / "data" / "sample_measurements.jsonl"


def event(**changes):
    with FIXTURE.open(encoding="utf-8") as lines:
        result = json.loads(next(lines))
    result.update(changes)
    return result


def run_records(*records):
    return replay(json.dumps(record) for record in records)


class ReplayTests(unittest.TestCase):
    def test_fixture_runs_through_cli(self):
        process = subprocess.run(
            [sys.executable, "-m", "streaming.replay", str(FIXTURE)],
            cwd=ROOT, check=True, capture_output=True, text=True,
        )
        result = json.loads(process.stdout)
        self.assertEqual(result["summary"], {
            "input_records": 10,
            "parsed_records": 10,
            "rejected_records": 0,
            "unique_revisions": 8,
            "duplicate_deliveries": 2,
            "current_observations": 7,
            "usable_observations": 3,
        })
        self.assertEqual([row["mean"] for row in result["hourly_metrics"]], [42, 35, 0])

    def test_correction_and_late_replay_are_order_independent(self):
        original = event()
        corrected = event(
            value=42, provider_payload={"value": 42},
            collected_at="2026-01-01T01:20:00Z", ingested_at="2026-01-01T01:21:00Z",
        )
        late_replay = event(ingested_at="2026-01-02T01:06:00Z")
        expected = run_records(original, corrected, late_replay)
        for ordered in itertools.permutations([original, corrected, late_replay]):
            self.assertEqual(run_records(*ordered), expected)
        self.assertEqual(expected["hourly_metrics"][0]["mean"], 42)
        self.assertEqual(expected["hourly_metrics"][0]["sample_count"], 1)
        self.assertEqual(len(expected["measurement_revisions"]), 2)

    def test_repeated_delivery_does_not_change_projections(self):
        with FIXTURE.open(encoding="utf-8") as handle:
            lines = list(handle)
        once = replay(lines)
        twice = replay(lines + lines)
        for field in ("hourly_metrics", "current_measurements", "measurement_revisions"):
            self.assertEqual(once[field], twice[field])

    def test_new_unusable_revision_removes_old_contribution(self):
        corrected = event(
            value=-1, collected_at="2026-01-01T01:20:00Z",
            ingested_at="2026-01-01T01:21:00Z",
        )
        result = run_records(event(), corrected)
        self.assertEqual(result["hourly_metrics"], [])
        self.assertEqual(result["current_measurements"][0]["quality_flags"], ["negative_concentration"])
        self.assertEqual(len(result["measurement_revisions"]), 2)

    def test_missing_zero_unit_conversion_and_source_flags(self):
        for changes, expected, flags in [
            ({"value": None}, None, ["missing_value"]),
            ({"value": 0}, 0, []),
            ({"value": 0.035, "unit": "mg/m3"}, 35, []),
            ({"unit": "µg/m³"}, 40, []),
            ({"unit": "ppm"}, None, ["unsupported_unit"]),
            ({"source_flags": ["suspect"]}, 40, ["source_flagged"]),
        ]:
            with self.subTest(changes=changes):
                result = parse_measurement(event(**changes))
                self.assertEqual(result.value_ug_m3, expected)
                self.assertEqual(list(result.quality_flags), flags)

    def test_equivalent_timezones_have_same_identity(self):
        original = parse_measurement(event())
        offset = parse_measurement(event(
            period_start="2026-01-01T07:00:00+07:00",
            period_end="2026-01-01T08:00:00+07:00",
        ))
        self.assertEqual(offset.event_id, original.event_id)
        self.assertEqual(offset.revision_hash, original.revision_hash)

    def test_subhourly_period_is_visible_but_not_aggregated(self):
        result = run_records(event(period_end="2026-01-01T00:30:00Z"))
        self.assertEqual(result["hourly_metrics"], [])
        self.assertEqual(result["current_measurements"][0]["quality_flags"], ["unsupported_reporting_period"])

    def test_malformed_records_are_preserved_without_stopping_replay(self):
        valid = json.dumps(event())
        result = replay(["{bad JSON\n", "[]", valid, '{"value":NaN}'])
        self.assertEqual(result["summary"]["rejected_records"], 3)
        self.assertEqual(result["rejected_records"][0]["raw_line"], "{bad JSON")
        self.assertEqual(result["hourly_metrics"][0]["mean"], 40)

    def test_invalid_contract_fields_are_rejected(self):
        for changes in [
            {"value": True}, {"value": float("inf")}, {"schema_version": True},
            {"period_start": "2026-01-01T00:00:00"},
            {"period_end": "2026-01-01T00:00:00Z"},
            {"collected_at": "2026-01-01T00:30:00Z"},
            {"ingested_at": "2026-01-01T01:00:00Z"},
            {"source_flags": "suspect"}, {"provider_payload": None},
            {"station_id": "station-1"},
        ]:
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                parse_measurement(event(**changes))

    def test_same_collection_time_has_deterministic_tiebreak(self):
        records = [event(), event(value=42)]
        forward = run_records(*records)
        backward = run_records(*reversed(records))
        self.assertEqual(forward, backward)

    def test_station_mean_uses_each_sensor_once(self):
        first = event()
        second = event(sensor_id="sensor-2", value=20)
        result = run_records(first, deepcopy(first), second)
        self.assertEqual(result["hourly_metrics"][0]["mean"], 30)
        self.assertEqual(result["hourly_metrics"][0]["sample_count"], 2)


if __name__ == "__main__":
    unittest.main()
