"""Wrapper, decision and manifest contract validation."""

import base64
import copy
import unittest

from streaming.quality.contracts import (
    WRAPPER_FIELDS,
    accepted_decision,
    build_wrapper,
    decode_wrapper_payload,
    experiment_decision_key,
    live_decision_key,
    quarantined_decision,
    sha256_hex,
    validate_decision,
    validate_manifest,
    validate_wrapper,
)

REF = {"snapshot_sha256": "a" * 64, "record_number": 3}


def wrapper(payload=b'{"x": 1}', **changes):
    base = build_wrapper(
        run_id="run-1",
        delivery_id="d-1",
        original_topic="air_quality_raw",
        original_key="demo:station-1",
        source_record_ref=REF,
        payload=payload,
        scheduled_release_index=0,
    )
    return {**base, **changes}


class WrapperTests(unittest.TestCase):
    def test_round_trip_preserves_exact_bytes_including_malformed_ones(self):
        for payload in (b'{"x": 1}', b'{"truncated', b"\xff\xfe\x00 not utf-8", b""):
            with self.subTest(payload=payload):
                built = wrapper(payload)
                self.assertEqual(set(built), set(WRAPPER_FIELDS))
                self.assertEqual(decode_wrapper_payload(built), payload)
                self.assertEqual(built["payload_sha256"], sha256_hex(payload))
                self.assertIsNone(built["published_at"])

    def test_extra_fields_are_rejected_so_nothing_can_leak_to_a_processor(self):
        for leak in ("fault", "expected", "original_value", "repair_hint"):
            with self.subTest(leak=leak), self.assertRaisesRegex(ValueError, "unexpected wrapper fields"):
                validate_wrapper({**wrapper(), leak: "x"})

    def test_invalid_wrappers(self):
        good = wrapper()
        missing = {k: v for k, v in good.items() if k != "run_id"}
        cases = {
            "not an object": [good],
            "missing field": missing,
            "schema version": {**good, "schema_version": 2},
            "bool schema version": {**good, "schema_version": True},
            "empty run": {**good, "run_id": " "},
            "dataset kind": {**good, "dataset_kind": "live"},
            "topic": {**good, "original_topic": "weather_raw"},
            "key type": {**good, "original_key": 5},
            "ref shape": {**good, "source_record_ref": {"snapshot_sha256": "a" * 64}},
            "ref digest": {**good, "source_record_ref": {**REF, "snapshot_sha256": "XYZ"}},
            "ref number": {**good, "source_record_ref": {**REF, "record_number": 0}},
            "base64 alphabet": {**good, "payload_base64": "@@@@"},
            "base64 padding": {**good, "payload_base64": base64.b64encode(b"abc").decode()[:-1]},
            "base64 type": {**good, "payload_base64": 5},
            "sha mismatch": {**good, "payload_sha256": "0" * 64},
            "bool release": {**good, "scheduled_release_index": True},
            "negative release": {**good, "scheduled_release_index": -1},
            "naive published_at": {**good, "published_at": "2026-01-01T00:00:00"},
        }
        for name, value in cases.items():
            with self.subTest(name), self.assertRaises(ValueError):
                validate_wrapper(value)

    def test_published_at_must_be_timezone_aware_when_present(self):
        validate_wrapper(wrapper(published_at="2026-01-01T00:00:00Z"))


class DecisionTests(unittest.TestCase):
    ACCEPTED = dict(
        rule_version="rules-v1",
        received_sha256="b" * 64,
        event_id="e" * 64,
        revision_hash="r" * 64,
        claimed_revision_hash=None,
        station_id="demo:station-1",
        sensor_id="sensor-1",
        period_start="2026-01-01T00:00:00Z",
        period_end="2026-01-01T01:00:00Z",
        collected_at="2026-01-01T01:05:00Z",
        ingested_at="2026-01-01T01:06:00Z",
        received_value=40,
        received_unit="ug/m3",
        value_ug_m3=40.0,
        quality_flags=[],
        repair_actions=[],
    )

    def test_accepted_and_quarantined_builders_satisfy_the_contract(self):
        accepted = accepted_decision(**self.ACCEPTED)
        self.assertTrue(accepted["usable"])
        flagged = accepted_decision(**{**self.ACCEPTED, "quality_flags": ["missing_value"]})
        self.assertFalse(flagged["usable"])
        quarantined = quarantined_decision(
            rule_version="rules-v1", received_sha256="b" * 64, reason="invalid_json", detail="x"
        )
        self.assertIsNone(quarantined["event_id"])
        for decision in (accepted, flagged, quarantined):
            validate_decision(decision)

    def test_invariants_are_enforced(self):
        accepted = accepted_decision(**self.ACCEPTED)
        quarantined = quarantined_decision(
            rule_version="rules-v1", received_sha256="b" * 64, reason="invalid_json", detail="x"
        )

        def changed(base, **changes):
            return {**copy.deepcopy(base), **changes}

        cases = {
            "unknown field": changed(accepted, extra=1),
            "usable with flags": changed(accepted, quality_flags=["missing_value"]),
            "unknown action": changed(accepted, repair_actions=["guess"]),
            "accepted without identity": changed(accepted, event_id=None),
            "accepted with reason": changed(accepted, reason="invalid_json"),
            "bad disposition": changed(accepted, disposition="repaired"),
            "bad digest": changed(accepted, received_sha256="x"),
            "quarantined with identity": changed(quarantined, event_id="e" * 64),
            "quarantined usable": changed(quarantined, usable=True),
            "quarantined without reason": changed(quarantined, reason=None),
        }
        for name, decision in cases.items():
            with self.subTest(name), self.assertRaises(ValueError):
                validate_decision(decision)
        with self.assertRaises(ValueError):
            quarantined_decision(rule_version="v", received_sha256="b" * 64, reason="made_up", detail="")

    def test_retry_safe_keys_distinguish_experiment_and_live_transport(self):
        self.assertEqual(experiment_decision_key("r", "d", "v"), ("r", "d", "v"))
        self.assertEqual(live_decision_key("t", 1, 2, "v"), ("t", 1, 2, "v"))


class ManifestTests(unittest.TestCase):
    def manifest(self, deliveries=2):
        rows = [
            {
                "delivery_id": f"d{i}", "release_index": i, "role": "primary",
                "source_record_ref": REF, "fault": None, "mutation": None,
                "original_sha256": "a" * 64, "emitted_sha256": "a" * 64,
                "duplicate_of": None, "expected": None,
            }
            for i in range(deliveries)
        ]
        return {
            "schema_version": 1, "run_id": "r", "dataset_kind": "controlled_corruption",
            "injector_version": "i", "seed": 1, "profile": "control", "rule_version": "v",
            "code_revision": None, "snapshot": {}, "catalog": None, "policies": {},
            "config": {}, "input_record_count": deliveries, "emitted_delivery_count": deliveries,
            "fault_counts": {}, "ineligible_selected": [], "omitted_records": [], "deliveries": rows,
        }

    def test_valid_manifest_passes_and_shape_errors_fail(self):
        validate_manifest(self.manifest())
        wrong_count = {**self.manifest(), "emitted_delivery_count": 5}
        duplicate = self.manifest()
        duplicate["deliveries"][1]["delivery_id"] = "d0"
        unordered = self.manifest()
        unordered["deliveries"][1]["release_index"] = 7
        no_key = {k: v for k, v in self.manifest().items() if k != "seed"}
        for name, value in {
            "count": wrong_count, "duplicate": duplicate, "order": unordered,
            "missing key": no_key, "kind": {**self.manifest(), "dataset_kind": "live"},
        }.items():
            with self.subTest(name), self.assertRaises(ValueError):
                validate_manifest(value)


if __name__ == "__main__":
    unittest.main()
