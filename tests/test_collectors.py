"""Offline checks for OpenAQ pagination/adaptation, HTTP retries and settings."""

from copy import deepcopy
from datetime import UTC, datetime, timedelta
from email.utils import format_datetime
import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock

try:
    import dotenv  # noqa: F401
    import pymongo  # noqa: F401
    import requests
except ImportError as error:
    raise unittest.SkipTest(
        f"collector dependencies missing ({error.name}); "
        'install with: pip install -e ".[collectors]"'
    ) from None

from collectors import http
from collectors.http import HTTPClient, SourceHTTPError, reset_seconds
from collectors.openaq import OpenAQ, adapt_measurement, coverage_report
from collectors.settings import load_settings
from models import MeasurementEvent
from streaming.measurements import parse_measurement


ROOT = Path(__file__).resolve().parents[1]
SENSOR = {"sensor_id": 42, "location_id": 7}
COLLECTED = datetime(2026, 1, 1, 6, tzinfo=UTC)


def openaq_row(hour=0, value=12.5, name="pm25", units="µg/m³", **changes):
    start = datetime(2026, 1, 1, hour, tzinfo=UTC)
    row = {
        "value": value,
        "flagInfo": {"hasFlags": False},
        "parameter": {"id": 2, "name": name, "units": units},
        "period": {
            "label": "raw",
            "interval": "01:00:00",
            "datetimeFrom": {"utc": start.isoformat().replace("+00:00", "Z")},
            "datetimeTo": {
                "utc": (start + timedelta(hours=1)).isoformat().replace("+00:00", "Z")
            },
        },
        "coverage": {"expectedCount": 1, "observedCount": 1},
    }
    row.update(changes)
    return row


class FakePage:
    def __init__(self, payload):
        self.payload = payload


class FakeHTTP:
    def __init__(self, *payloads):
        self.payloads = list(payloads)
        self.calls = []

    def get(self, url, params, context=None):
        self.calls.append((url, dict(params), context))
        return FakePage(self.payloads.pop(0))


def page(number, rows, found=None, limit=2):
    return {"meta": {"page": number, "limit": limit, "found": found}, "results": rows}


class PaginationTests(unittest.TestCase):
    def test_stops_when_reported_total_is_reached(self):
        client = FakeHTTP(page(1, [{"id": 1}, {"id": 2}], 3), page(2, [{"id": 3}], 3))
        pages = list(OpenAQ(client, page_size=2).pages("locations"))
        self.assertEqual(len(pages), 2)
        self.assertEqual([call[1]["page"] for call in client.calls], [1, 2])
        self.assertEqual(client.calls[0][1]["limit"], 2)

    def test_inexact_total_continues_until_short_page(self):
        client = FakeHTTP(
            page(1, [{"id": 1}, {"id": 2}], ">1000"),
            page(2, [{"id": 3}, {"id": 4}], ">1000"),
            page(3, [], ">1000"),
        )
        self.assertEqual(len(list(OpenAQ(client, page_size=2).pages("x"))), 3)

    def test_repeated_page_is_rejected(self):
        rows = [{"id": 1}, {"id": 2}]
        client = FakeHTTP(page(1, rows, ">1000"), page(2, rows, ">1000"))
        with self.assertRaisesRegex(ValueError, "repeated a page"):
            list(OpenAQ(client, page_size=2).pages("x"))

    def test_wrong_page_number_is_rejected(self):
        client = FakeHTTP(page(2, [{"id": 1}]))
        with self.assertRaisesRegex(ValueError, "wrong page"):
            list(OpenAQ(client, page_size=2).pages("x"))

    def test_short_page_before_reported_total_is_rejected(self):
        client = FakeHTTP(page(1, [{"id": 1}], 5))
        with self.assertRaisesRegex(ValueError, "incomplete"):
            list(OpenAQ(client, page_size=2).pages("x"))

    def test_missing_results_array_is_rejected(self):
        client = FakeHTTP({"meta": {"page": 1}})
        with self.assertRaisesRegex(ValueError, "results array"):
            list(OpenAQ(client).pages("x"))

    def test_resume_from_checkpointed_page(self):
        client = FakeHTTP(page(3, [{"id": 5}], 5))
        pages = OpenAQ(client, page_size=2).pages("x", start_page=3)
        self.assertEqual(len(list(pages)), 1)
        self.assertEqual(client.calls[0][1]["page"], 3)


class AdaptMeasurementTests(unittest.TestCase):
    def test_pm25_identity_matches_shared_model(self):
        envelope = adapt_measurement(openaq_row(), SENSOR, COLLECTED, COLLECTED)
        event = MeasurementEvent.from_dict(envelope)
        self.assertEqual(envelope["station_id"], "openaq:7")
        self.assertEqual(envelope["sensor_id"], "42")
        self.assertEqual(envelope["event_id"], event.event_id)
        self.assertEqual(envelope["revision_hash"], event.revision_hash)
        self.assertEqual(envelope["provider_payload"], openaq_row())

    def test_explicit_no_flags_keeps_reading_usable(self):
        envelope = adapt_measurement(openaq_row(), SENSOR, COLLECTED, COLLECTED)
        self.assertEqual(envelope["source_flags"], [])
        self.assertEqual(parse_measurement(envelope).quality_flags, ())

    def test_reported_flags_are_retained(self):
        for flag_info in ({"hasFlags": True}, {"unexpected": "shape"}):
            with self.subTest(flag_info=flag_info):
                row = openaq_row(flagInfo=flag_info)
                envelope = adapt_measurement(row, SENSOR, COLLECTED, COLLECTED)
                self.assertEqual(len(envelope["source_flags"]), 1)
                self.assertTrue(envelope["source_flags"][0].startswith("openaq:flagInfo:"))
                self.assertIn(
                    "source_flagged", parse_measurement(envelope).quality_flags
                )

    def test_flag_list_is_retained(self):
        row = openaq_row(flags=[{"label": "spike"}])
        envelope = adapt_measurement(row, SENSOR, COLLECTED, COLLECTED)
        self.assertTrue(envelope["source_flags"][0].startswith("openaq:flags:"))

    def test_other_parameters_get_identity_without_pm25_rules(self):
        row = openaq_row(name="no2", units="ppm", value=0.02)
        envelope = adapt_measurement(row, SENSOR, COLLECTED, COLLECTED)
        self.assertEqual(envelope["pollutant"], "no2")
        self.assertEqual(len(envelope["event_id"]), 64)
        changed = adapt_measurement(
            openaq_row(name="no2", units="ppm", value=0.03), SENSOR, COLLECTED, COLLECTED
        )
        self.assertEqual(changed["event_id"], envelope["event_id"])
        self.assertNotEqual(changed["revision_hash"], envelope["revision_hash"])

    def test_invalid_rows_raise_value_error(self):
        missing_value = openaq_row()
        del missing_value["value"]
        missing_period_end = openaq_row()
        del missing_period_end["period"]["datetimeTo"]
        cases = {
            "not an object": [],
            "missing parameter": openaq_row(parameter=None),
            "missing value": missing_value,
            "incomplete period": missing_period_end,
            "nonfinite other parameter": openaq_row(name="o3", value=float("inf")),
            "collected before period end": openaq_row(hour=6),
        }
        for label, row in cases.items():
            with self.subTest(label), self.assertRaises(ValueError):
                adapt_measurement(row, SENSOR, COLLECTED, COLLECTED)


class CoverageReportTests(unittest.TestCase):
    def test_counts_distinct_usable_hours(self):
        start, end = COLLECTED - timedelta(hours=4), COLLECTED
        envelopes = [
            adapt_measurement(openaq_row(hour=2), SENSOR, COLLECTED, COLLECTED),
            # A repeated poll of the same hour must not count twice.
            adapt_measurement(openaq_row(hour=2), SENSOR, COLLECTED, COLLECTED),
            adapt_measurement(openaq_row(hour=3, value=None), SENSOR, COLLECTED, COLLECTED),
        ]
        report = coverage_report(envelopes, start, end, rejected=1)
        self.assertEqual(report["expected_hourly_periods"], 4)
        self.assertEqual(report["observed_hourly_periods"], 2)
        self.assertEqual(report["usable_hourly_periods"], 1)
        self.assertEqual(report["usable_coverage_percent"], 25)
        self.assertEqual(report["quality_flags"], {"missing_value": 1})
        self.assertEqual(report["rejected_records"], 1)
        self.assertEqual(
            report["missing_hours"], ["2026-01-01T04:00:00Z", "2026-01-01T05:00:00Z"]
        )


class ResetSecondsTests(unittest.TestCase):
    def test_header_forms(self):
        current = datetime(2026, 1, 1, tzinfo=UTC)
        with mock.patch.object(http, "now", return_value=current):
            self.assertEqual(reset_seconds("12"), 12)
            self.assertEqual(reset_seconds("-3"), 0)
            self.assertEqual(reset_seconds(str(current.timestamp() + 30)), 30)
            later = format_datetime(current + timedelta(seconds=90), usegmt=True)
            self.assertEqual(reset_seconds(later), 90)
            self.assertEqual(reset_seconds("not a date"), 60)
            self.assertEqual(reset_seconds(None), 60)


class FakeResult:
    def __init__(self, status_code, text="{}", headers=None):
        self.status_code, self.text, self.headers = status_code, text, headers or {}


class FakeSession:
    def __init__(self, *results):
        self.results = list(results)
        self.calls = 0

    def get(self, *_args, **_kwargs):
        self.calls += 1
        result = self.results.pop(0)
        if isinstance(result, Exception):
            raise result
        return result

    def close(self):
        pass


class FakeBudgetStore:
    stop_event = None

    def __init__(self):
        self.reserved, self.deferred, self.captured = 0, [], []

    def reserve_request(self, *_args, **_kwargs):
        self.reserved += 1

    def defer_requests(self, scope, seconds):
        self.deferred.append(seconds)

    def capture(self, response):
        self.captured.append(response.body)


@mock.patch.object(http, "wait")
class HTTPClientTests(unittest.TestCase):
    def client(self, *results, attempts=3, api_key="secret"):
        store = FakeBudgetStore()
        session = FakeSession(*results)
        return HTTPClient("openaq", store, api_key, attempts, session), store, session

    def test_retries_transient_errors_then_captures_response(self, wait):
        client, store, session = self.client(
            FakeResult(503),
            requests.ConnectionError(),
            FakeResult(200, '{"results": []}', {"X-RateLimit-Used": "1"}),
        )
        response = client.get("https://example.test", {"page": 1})
        self.assertEqual(response.payload, {"results": []})
        self.assertEqual(response.headers, {"x-ratelimit-used": "1"})
        self.assertEqual((session.calls, store.reserved, wait.call_count), (3, 3, 2))
        self.assertEqual(store.captured, ['{"results": []}'])
        self.assertNotIn("secret", client.scope)

    def test_rate_limit_defers_shared_budget(self, _wait):
        client, store, _ = self.client(
            FakeResult(429, headers={"Retry-After": "120"}), FakeResult(200)
        )
        client.get("https://example.test", {})
        self.assertEqual(store.deferred, [120])

    def test_auth_failure_names_the_key(self, _wait):
        client, store, _ = self.client(FakeResult(401))
        with self.assertRaisesRegex(SourceHTTPError, "OPENAQ_API_KEY") as caught:
            client.get("https://example.test", {})
        self.assertEqual(caught.exception.status_code, 401)
        self.assertEqual(store.captured, [])

    def test_network_failure_after_last_attempt(self, _wait):
        client, _, _ = self.client(
            requests.Timeout(), requests.Timeout(), attempts=2
        )
        with self.assertRaisesRegex(RuntimeError, "network request failed"):
            client.get("https://example.test", {})

    def test_invalid_json_is_captured_before_failing(self, _wait):
        client, store, _ = self.client(FakeResult(200, '{"value": NaN}'))
        with self.assertRaisesRegex(ValueError, "invalid JSON"):
            client.get("https://example.test", {})
        self.assertEqual(store.captured, ['{"value": NaN}'])


class SettingsTests(unittest.TestCase):
    def setUp(self):
        self.config = json.loads((ROOT / "config" / "collectors.json").read_text())

    def load(self, config):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "collectors.json"
            path.write_text(json.dumps(config))
            return load_settings(path)

    def test_shipped_config_is_valid(self):
        self.assertEqual(self.load(self.config)["scope"], "global")

    def test_invalid_values_are_rejected(self):
        changes = {
            "schema version": ("schema_version", None, 1),
            "page size": ("openaq", "page_size", 1001),
            "worker count": ("openaq", "worker_count", 0),
            "boolean interval": ("weather", "poll_seconds", True),
            "short lease": ("scheduler", "lease_seconds", 10),
            "past days": ("weather", "past_days", 93),
            "forecast days": ("weather", "forecast_days", 17),
            "multiple models": ("weather", "model", "gfs_seamless,icon_seamless"),
            "coordinates": (
                "weather",
                "locations",
                [{"location_id": "x", "latitude": 91, "longitude": 0}],
            ),
        }
        for label, (section, field, value) in changes.items():
            config = deepcopy(self.config)
            if field is None:
                config[section] = value
            else:
                config[section][field] = value
            with self.subTest(label), self.assertRaises(ValueError):
                self.load(config)


if __name__ == "__main__":
    unittest.main()
