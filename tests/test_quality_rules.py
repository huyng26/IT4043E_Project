"""Shared quality rules: repairs, quarantine, integrity and revision selection."""

import json
import unittest
from pathlib import Path

from quality_support import encode, envelope

from streaming.measurements import current_measurements, parse_measurement
from streaming.quality.contracts import validate_decision
from streaming.quality.rules import (
    RULE_VERSION,
    evaluate_payload,
    select_current_decisions,
)

FIXTURE = Path(__file__).resolve().parents[1] / "data" / "sample_measurements.jsonl"


def evaluate(payload, topic="air_quality_raw", version=RULE_VERSION):
    decision = evaluate_payload(payload, source_topic=topic, rule_version=version)
    validate_decision(decision)
    return decision


def evaluate_envelope(**kwargs):
    return evaluate(encode(envelope(**kwargs)))


class FixtureParityTests(unittest.TestCase):
    """The shared rules must agree with the existing local replay semantics."""

    def setUp(self):
        text = FIXTURE.read_text(encoding="utf-8")
        self.lines = [line.encode() for line in text.splitlines()]

    def test_flags_values_and_identities_match_parse_measurement(self):
        for line in self.lines:
            decision = evaluate(line)
            expected = parse_measurement(json.loads(line))
            self.assertEqual(decision["disposition"], "accepted")
            self.assertEqual(decision["event_id"], expected.event_id)
            self.assertEqual(decision["revision_hash"], expected.revision_hash)
            self.assertEqual(decision["value_ug_m3"], expected.value_ug_m3)
            self.assertEqual(tuple(decision["quality_flags"]), expected.quality_flags)

    def test_current_selection_matches_current_measurements(self):
        decisions = [evaluate(line) for line in self.lines]
        expected = current_measurements([parse_measurement(json.loads(l)) for l in self.lines])
        current = select_current_decisions(decisions)
        self.assertEqual(
            [(d["event_id"], d["revision_hash"], d["usable"]) for d in current],
            [(m.event_id, m.revision_hash, not m.quality_flags) for m in expected],
        )


class RepairTests(unittest.TestCase):
    def test_clean_record_has_no_actions_and_is_usable(self):
        decision = evaluate_envelope()
        self.assertEqual(decision["repair_actions"], [])
        self.assertEqual(decision["quality_flags"], [])
        self.assertTrue(decision["usable"])
        self.assertEqual(decision["value_ug_m3"], 40)
        self.assertEqual(decision["claimed_revision_hash"], decision["revision_hash"])

    def test_numeric_strings_round_trip_to_the_baseline_revision(self):
        for value in (40, 40.0, 35.5, 0, -5, 1e-05, 1e22):
            with self.subTest(value=value):
                baseline = evaluate_envelope(value=value)
                text = " " + json.dumps(value) + " "
                # Claimed hashes stay those of the numeric baseline.
                changed = envelope(value=value)
                changed["value"] = text
                decision = evaluate(encode(changed))
                self.assertEqual(decision["disposition"], "accepted")
                self.assertEqual(decision["repair_actions"], ["numeric_string_parsed"])
                self.assertEqual(decision["received_value"], text)
                self.assertEqual(decision["revision_hash"], baseline["revision_hash"])
                self.assertEqual(decision["value_ug_m3"], baseline["value_ug_m3"])
                self.assertEqual(decision["quality_flags"], baseline["quality_flags"])

    def test_ambiguous_or_unparseable_values_are_quarantined_never_zero(self):
        for bad in ("unknown", "", "   ", "1,5", "NaN", "inf", "0x10", "+5", ".5", "5.", "1e999", "١٢", True, [1], {}):
            with self.subTest(value=bad):
                raw = envelope(hashes=False)
                raw["value"] = bad
                decision = evaluate(encode(raw))
                self.assertEqual(decision["disposition"], "quarantined")
                self.assertEqual(decision["reason"], "unparseable_value")
                self.assertIsNone(decision["event_id"])
                self.assertIsNone(decision["value_ug_m3"])

    def test_unit_aliases_and_conversion_are_recorded_and_value_preserved(self):
        cases = [
            ("µg/m³", 35, "unit_alias_normalized", 35),
            ("μg/m³", 35, "unit_alias_normalized", 35),
            ("mg/m3", 0.035, "unit_converted_to_ug_m3", 35),
        ]
        for unit, value, action, normalized in cases:
            with self.subTest(unit=unit):
                decision = evaluate_envelope(value=value, unit=unit)
                self.assertEqual(decision["repair_actions"], [action])
                self.assertAlmostEqual(decision["value_ug_m3"], normalized, delta=1e-9)
                self.assertEqual(decision["received_unit"], unit)
                self.assertEqual(decision["received_value"], value)
                self.assertEqual(decision["quality_flags"], [])

    def test_null_value_has_no_conversion_action(self):
        decision = evaluate_envelope(value=None, unit="mg/m3")
        self.assertEqual(decision["repair_actions"], [])
        self.assertEqual(decision["quality_flags"], ["missing_value"])
        self.assertIsNone(decision["value_ug_m3"])

    def test_unsupported_unit_is_flagged_not_converted_or_quarantined(self):
        decision = evaluate_envelope(value=12, unit="ppm")
        self.assertEqual(decision["disposition"], "accepted")
        self.assertEqual(decision["quality_flags"], ["unsupported_unit"])
        self.assertEqual(decision["repair_actions"], [])
        self.assertIsNone(decision["value_ug_m3"])
        self.assertFalse(decision["usable"])

    def test_equivalent_utc_offsets_keep_identity_and_record_the_action(self):
        baseline = evaluate_envelope()
        shifted = envelope()
        for field in ("period_start", "period_end", "collected_at", "ingested_at"):
            hour = int(shifted[field][11:13]) + 7
            shifted[field] = f"{shifted[field][:11]}{hour:02d}{shifted[field][13:-1]}+07:00"
        decision = evaluate(encode(shifted))
        self.assertEqual(decision["repair_actions"], ["timestamp_normalized_to_utc"])
        self.assertEqual(decision["event_id"], baseline["event_id"])
        self.assertEqual(decision["revision_hash"], baseline["revision_hash"])
        self.assertEqual(decision["period_start"], baseline["period_start"])

    def test_timezone_free_timestamp_is_quarantined_not_assumed_utc(self):
        raw = envelope(hashes=False)
        raw["period_start"] = "2026-01-01T00:00:00"
        decision = evaluate(encode(raw))
        self.assertEqual(decision["reason"], "missing_timezone")

    def test_zero_is_a_valid_usable_value(self):
        decision = evaluate_envelope(value=0)
        self.assertTrue(decision["usable"])
        self.assertEqual(decision["value_ug_m3"], 0)

    def test_negative_and_flagged_values_are_visible_but_not_usable(self):
        negative = evaluate_envelope(value=-5)
        self.assertEqual(negative["quality_flags"], ["negative_concentration"])
        self.assertFalse(negative["usable"])
        flagged = evaluate_envelope(source_flags=["suspect"])
        self.assertEqual(flagged["quality_flags"], ["source_flagged"])


class QuarantineTests(unittest.TestCase):
    def assertQuarantined(self, payload, reason):
        decision = evaluate(payload)
        self.assertEqual(decision["disposition"], "quarantined")
        self.assertEqual(decision["reason"], reason)
        for field in ("event_id", "revision_hash", "station_id", "collected_at"):
            self.assertIsNone(decision[field])
        return decision

    def test_structural_failures(self):
        good = encode(envelope())
        missing_station = envelope()
        del missing_station["station_id"]
        collector_rejected = {**envelope(hashes=False), "status": "rejected", "error": "bad"}
        self.assertQuarantined(good[: len(good) // 2], "invalid_json")
        self.assertQuarantined(b"\xff\xfe" + good, "invalid_utf8")
        self.assertQuarantined(b"[1, 2]", "not_an_object")
        self.assertQuarantined(b"", "invalid_json")
        self.assertQuarantined(b'{"value": NaN}', "invalid_json")
        self.assertQuarantined(encode(missing_station), "missing_required_field")
        self.assertQuarantined(encode(collector_rejected), "collector_rejected")
        self.assertQuarantined(encode(envelope(pollutant="no2", hashes=False)), "unsupported_pollutant")
        self.assertQuarantined(encode(envelope(schema_version=2, hashes=False)), "invalid_envelope")
        self.assertQuarantined(
            encode(envelope(period_end="2026-01-01T00:00:00Z", hashes=False)), "invalid_envelope"
        )

    def test_deeply_nested_json_is_quarantined_not_raised(self):
        self.assertQuarantined(b"[" * 100000, "invalid_json")

    def test_received_bytes_hash_is_kept_for_malformed_input(self):
        decision = self.assertQuarantined(b"{truncated", "invalid_json")
        import hashlib

        self.assertEqual(decision["received_sha256"], hashlib.sha256(b"{truncated").hexdigest())

    def test_tampered_claimed_hashes_are_quarantined(self):
        tampered = envelope()
        tampered["value"] = 41  # content changed, claimed revision_hash did not
        self.assertQuarantined(encode(tampered), "integrity_mismatch")
        wrong_event = {**envelope(), "event_id": "0" * 64}
        self.assertQuarantined(encode(wrong_event), "integrity_mismatch")

    def test_configuration_errors_raise_instead_of_quarantining(self):
        with self.assertRaises(ValueError):
            evaluate_payload(b"{}", source_topic="weather_raw", rule_version=RULE_VERSION)
        with self.assertRaises(ValueError):
            evaluate_payload(b"{}", source_topic="air_quality_raw", rule_version="unknown")
        with self.assertRaises(TypeError):
            evaluate_payload("{}", source_topic="air_quality_raw", rule_version=RULE_VERSION)


class SelectionTests(unittest.TestCase):
    def test_correction_to_null_removes_the_earlier_usable_value(self):
        first = evaluate_envelope(value=40)
        correction = evaluate(
            encode(
                envelope(
                    value=None,
                    collected_at="2026-01-01T02:00:00Z",
                    ingested_at="2026-01-01T02:01:00Z",
                )
            )
        )
        current = select_current_decisions([first, correction])
        self.assertEqual(len(current), 1)
        self.assertEqual(current[0]["revision_hash"], correction["revision_hash"])
        self.assertFalse(current[0]["usable"])

    def test_repeat_delivery_and_order_do_not_change_the_current_revision(self):
        first = evaluate_envelope(value=40)
        newer = evaluate(
            encode(envelope(value=42, collected_at="2026-01-01T02:00:00Z", ingested_at="2026-01-01T02:01:00Z"))
        )
        for order in ([first, first, newer], [newer, first, first], [first, newer, newer]):
            current = select_current_decisions(order)
            self.assertEqual([d["revision_hash"] for d in current], [newer["revision_hash"]])

    def test_quarantined_decisions_never_retract_observations(self):
        first = evaluate_envelope(value=40)
        broken = evaluate(b"{truncated")
        self.assertEqual(select_current_decisions([first, broken]), [first])


if __name__ == "__main__":
    unittest.main()
