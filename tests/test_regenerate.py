"""Re-deriving air_quality_raw envelopes from captured OpenAQ responses."""

import io
import json
import unittest
from datetime import UTC, datetime

try:
    from collectors.regenerate import (
        STALE_FLAG,
        regenerate,
        regenerate_response,
        repair_outbox,
        repaired_envelope,
    )
except ImportError:  # collector dependencies (pymongo) are optional
    regenerate = regenerate_response = repair_outbox = repaired_envelope = None
    STALE_FLAG = None

from models import canonical_json, digest


def measurement(value=12.5, parameter="pm25", units="µg/m³", flag_info=None, hour=0):
    return {
        "value": value,
        "parameter": {"id": 2, "name": parameter, "units": units},
        "period": {
            "datetimeFrom": {"utc": f"2026-10-25T{hour:02d}:00:00Z"},
            "datetimeTo": {"utc": f"2026-10-25T{hour + 1:02d}:00:00Z"},
        },
        "flagInfo": {"hasFlags": False} if flag_info is None else flag_info,
        "coverage": {"percentComplete": 100},
    }


def raw_row(results, request_id="req-1", body=None):
    return {
        "_id": request_id,
        "source": "openaq",
        "body": body if body is not None else json.dumps({"meta": {}, "results": results}),
        "collected_at": datetime(2026, 10, 26, 1, 0, tzinfo=UTC),
        "collected_at_iso": "2026-10-26T01:00:00.123456Z",
        "context": {"kind": "openaq", "sensor": {"sensor_id": 7, "location_id": 50}},
    }


@unittest.skipIf(regenerate is None, "collector dependencies are not installed")
class RegenerateTests(unittest.TestCase):
    def test_has_flags_false_is_no_longer_a_source_flag(self):
        (envelope,) = regenerate_response(raw_row([measurement()]))
        self.assertEqual(envelope["source_flags"], [])
        self.assertEqual(envelope["raw_response_id"], "req-1")
        self.assertEqual(envelope["ingested_at"], envelope["collected_at"])
        self.assertEqual(envelope["collected_at"], "2026-10-26T01:00:00.123456Z")

    def test_real_flags_are_kept(self):
        flagged = {"hasFlags": True, "flags": ["calibration"]}
        (envelope,) = regenerate_response(raw_row([measurement(flag_info=flagged)]))
        self.assertEqual(envelope["source_flags"], [f"openaq:flagInfo:{digest(flagged)}"])

    def test_identity_is_unchanged_and_other_pollutants_are_hashed(self):
        pm25, no2 = regenerate_response(raw_row([measurement(), measurement(parameter="no2", units="ppb")]))
        expected = digest(
            {
                "source": "openaq",
                "station_id": "openaq:50",
                "sensor_id": "7",
                "pollutant": "pm25",
                "period_start": "2026-10-25T00:00:00Z",
                "period_end": "2026-10-25T01:00:00Z",
            }
        )
        self.assertEqual(pm25["event_id"], expected)
        self.assertEqual(no2["pollutant"], "no2")
        self.assertTrue(no2["event_id"] and no2["revision_hash"])

    def test_bad_rows_and_pages_become_rejected_envelopes(self):
        broken = measurement()
        del broken["value"]
        (rejected,) = regenerate_response(raw_row([broken]))
        self.assertEqual(rejected["status"], "rejected")
        self.assertEqual(rejected["sensor_id"], "7")
        self.assertEqual(rejected["provider_payload"], broken)
        (page,) = regenerate_response(raw_row(None, body="<html>gateway timeout</html>"))
        self.assertEqual((page["status"], page["error"]), ("rejected", "invalid OpenAQ response"))

    def test_output_is_deterministic_and_filterable(self):
        rows = [
            raw_row([measurement(hour=h), measurement(parameter="no2", units="ppb", hour=h)], f"req-{h}")
            for h in range(3)
        ]
        first, second = io.BytesIO(), io.BytesIO()
        report = regenerate(rows, first)
        self.assertEqual(regenerate(rows, second), report)
        self.assertEqual(first.getvalue(), second.getvalue())
        self.assertEqual(report["envelopes"], 6)
        self.assertEqual(report["source_flagged"], 0)
        self.assertEqual(report["flaginfo_has_flags_false"], 6)
        only = io.BytesIO()
        filtered = regenerate(rows, only, pollutants={"pm25"})
        self.assertEqual(filtered["by_pollutant"], {"pm25": 3})
        lines = only.getvalue().decode().splitlines()
        self.assertTrue(all(json.loads(line)["pollutant"] == "pm25" for line in lines))
        self.assertEqual(lines[0], canonical_json(json.loads(lines[0])))


def stale(envelope):
    """What the pre-5bf84c9 collector staged: the explicit negative became a flag."""
    from collectors.openaq import adapt_measurement  # noqa: F401  (deps present)
    from models import MeasurementEvent

    old = dict(envelope, source_flags=[STALE_FLAG])
    if old["pollutant"] == "pm25":
        old["revision_hash"] = MeasurementEvent.from_dict(old).revision_hash
    else:
        identity = {f: old[f] for f in ("source", "station_id", "sensor_id", "pollutant", "period_start", "period_end")}
        old["revision_hash"] = digest(
            {**identity, **{f: old[f] for f in ("value", "unit", "source_flags", "provider_payload")}}
        )
    old["ingested_at"] = "2026-10-26T01:00:05.000001Z"
    return old


class FakeOutbox:
    def __init__(self, rows):
        self.rows = {row["_id"]: row for row in rows}
        self.calls = 0

    def iter_outbox_with_flag(self, topic, flag):
        return [
            dict(r) for r in self.rows.values()
            if r["topic"] == topic and r["published_at"] is None and flag in r["envelope"].get("source_flags", [])
        ]

    def replace_unpublished_envelopes(self, replacements, *, repair):
        self.calls += 1
        changed = 0
        for identity, old_revision, envelope in replacements:
            row = self.rows[identity]
            if row["published_at"] is None and row["revision_hash"] == old_revision:
                row.update(envelope=envelope, revision_hash=envelope["revision_hash"], repair=repair)
                changed += 1
        return changed


@unittest.skipIf(regenerate is None, "collector dependencies are not installed")
class RepairOutboxTests(unittest.TestCase):
    def setUp(self):
        fresh = regenerate_response(
            raw_row([measurement(), measurement(parameter="no2", units="ppb", hour=1),
                     measurement(flag_info={"hasFlags": True}, hour=2)])
        )
        self.fresh = fresh
        rows = []
        for index, envelope in enumerate(fresh):
            old = stale(envelope) if index < 2 else envelope
            rows.append({"_id": f"row-{index}", "topic": "air_quality_raw", "published_at": None,
                         "revision_hash": old["revision_hash"], "envelope": old})
        self.store = FakeOutbox(rows)

    def test_stale_rows_get_the_current_adapter_output_with_original_times(self):
        report = repair_outbox(self.store)
        self.assertEqual((report["stale_rows"], report["replaced"]), (2, 2))
        for index in (0, 1):
            row = self.store.rows[f"row-{index}"]
            self.assertEqual(row["envelope"]["source_flags"], [])
            self.assertEqual(row["envelope"]["event_id"], self.fresh[index]["event_id"])
            self.assertEqual(row["envelope"]["revision_hash"], self.fresh[index]["revision_hash"])
            self.assertEqual(row["envelope"]["ingested_at"], "2026-10-26T01:00:05.000001Z")
            self.assertEqual(row["envelope"]["raw_response_id"], "req-1")
        self.assertNotIn("repair", self.store.rows["row-2"])  # a genuinely flagged row is untouched

    def test_dry_run_changes_nothing_and_a_rerun_finds_nothing(self):
        before = json.dumps(self.store.rows, sort_keys=True)
        report = repair_outbox(self.store, dry_run=True)
        self.assertEqual((report["would_replace"], report["replaced"]), (2, 0))
        self.assertEqual(json.dumps(self.store.rows, sort_keys=True), before)
        repair_outbox(self.store)
        self.assertEqual(repair_outbox(self.store)["stale_rows"], 0)

    def test_published_or_concurrently_changed_rows_are_not_replaced(self):
        read = self.store.iter_outbox_with_flag

        def read_then_change(topic, flag):
            rows = read(topic, flag)
            # After the repair read them: one row is published, one is rewritten.
            self.store.rows["row-0"]["published_at"] = "2026-10-27T00:00:00Z"
            self.store.rows["row-1"]["revision_hash"] = "changed-meanwhile"
            return rows

        self.store.iter_outbox_with_flag = read_then_change
        report = repair_outbox(self.store)
        self.assertEqual((report["would_replace"], report["replaced"]), (2, 0))
        self.assertNotIn("repair", self.store.rows["row-0"])
        self.assertNotIn("repair", self.store.rows["row-1"])

    def test_published_rows_are_not_selected(self):
        self.store.rows["row-0"]["published_at"] = "2026-10-27T00:00:00Z"
        self.assertEqual(repair_outbox(self.store)["stale_rows"], 1)

    def test_rows_whose_identity_would_change_are_skipped(self):
        envelope = stale(self.fresh[0])
        envelope["event_id"] = "0" * 64
        self.assertIsNone(repaired_envelope(envelope))
        self.assertIsNone(repaired_envelope({**envelope, "station_id": "other:1"}))


if __name__ == "__main__":
    unittest.main()
