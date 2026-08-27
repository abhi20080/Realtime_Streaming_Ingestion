import json
import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "flink"))

from logic import (  # noqa: E402
    INVALID_JSON,
    INVALID_TIMESTAMP,
    INVALID_TYPE,
    LONG_MAX,
    LONG_MIN,
    MAX_ERROR_DETAIL_LENGTH,
    MISSING_FIELD,
    build_dlq_record,
    calculate_lateness,
    event_time_ms_for_watermark,
    validate_event,
)


def valid_payload():
    return {
        "schema_version": 1,
        "producer_run_id": "run-123",
        "producer_sequence": 42,
        "produced_at": "2026-08-23T12:00:05.123Z",
        "event_id": "event-123",
        "tenant_id": "tenant_hot",
        "device_id": "device_00042",
        "metric_name": "cpu_usage",
        "metric_value": 37.5,
        "event_time": "2026-08-23T12:00:00.000Z",
        "region": "blr",
        "firmware": "10.8.0",
        "injected_duplicate": False,
        "injected_late": True,
    }


class ValidationTests(unittest.TestCase):
    def test_valid_event_is_typed_and_utc(self):
        result = validate_event(json.dumps(valid_payload()))
        self.assertTrue(result.is_valid)
        self.assertEqual(result.event.producer_sequence, 42)
        self.assertEqual(result.event.metric_value, 37.5)
        self.assertEqual(result.event.produced_at.tzinfo, timezone.utc)
        self.assertTrue(result.event.injected_late)

    def test_invalid_json(self):
        self.assertEqual(validate_event('{"broken":').error_category, INVALID_JSON)

    def test_missing_fields_are_reported_together(self):
        payload = valid_payload()
        del payload["event_id"]
        del payload["region"]
        result = validate_event(json.dumps(payload))
        self.assertEqual(result.error_category, MISSING_FIELD)
        self.assertIn("event_id", result.error_detail)
        self.assertIn("region", result.error_detail)

    def test_boolean_is_not_an_integer_sequence(self):
        payload = valid_payload()
        payload["producer_sequence"] = True
        self.assertEqual(
            validate_event(json.dumps(payload)).error_category, INVALID_TYPE
        )

    def test_nonfinite_metric_is_invalid_type(self):
        payload = valid_payload()
        payload["metric_value"] = float("inf")
        self.assertEqual(
            validate_event(json.dumps(payload)).error_category, INVALID_TYPE
        )

    def test_sink_incompatible_integer_ranges_are_rejected(self):
        payload = valid_payload()
        payload["schema_version"] = 65_536
        self.assertEqual(
            validate_event(json.dumps(payload)).error_category, INVALID_TYPE
        )

        payload = valid_payload()
        payload["producer_sequence"] = LONG_MAX + 1
        self.assertEqual(
            validate_event(json.dumps(payload)).error_category, INVALID_TYPE
        )

    def test_timestamp_without_timezone_is_rejected(self):
        payload = valid_payload()
        payload["event_time"] = "2026-08-23T12:00:00.000"
        self.assertEqual(
            validate_event(json.dumps(payload)).error_category, INVALID_TIMESTAMP
        )

    def test_clickhouse_datetime64_range_is_enforced(self):
        for out_of_range in (
            "1899-12-31T23:59:59.999Z",
            "2300-01-01T00:00:00.000Z",
        ):
            payload = valid_payload()
            payload["event_time"] = out_of_range
            result = validate_event(json.dumps(payload))
            self.assertEqual(result.error_category, INVALID_TIMESTAMP)
            self.assertIn("ClickHouse DateTime64(3) range", result.error_detail)

    def test_missing_error_detail_is_bounded(self):
        result = validate_event("{}")
        self.assertLessEqual(len(result.error_detail), MAX_ERROR_DETAIL_LENGTH)


class EventTimeTests(unittest.TestCase):
    def test_source_watermark_timestamp_uses_only_valid_event_time(self):
        payload = valid_payload()
        expected = int(
            datetime.fromisoformat(payload["event_time"].replace("Z", "+00:00")).timestamp()
            * 1_000
        )

        self.assertEqual(event_time_ms_for_watermark(json.dumps(payload)), expected)
        self.assertEqual(event_time_ms_for_watermark('{"broken":'), 0)

    def test_sentinel_and_none_are_not_late(self):
        self.assertEqual(calculate_lateness(1_000, LONG_MIN), (False, 0))
        self.assertEqual(calculate_lateness(1_000, None), (False, 0))

    def test_event_after_watermark_is_not_late(self):
        self.assertEqual(calculate_lateness(2_001, 2_000), (False, 0))

    def test_event_at_watermark_is_late_with_zero_lateness(self):
        self.assertEqual(calculate_lateness(2_000, 2_000), (True, 0))

    def test_event_before_watermark_records_lateness(self):
        self.assertEqual(calculate_lateness(1_250, 2_000), (True, 750))


class DlqTests(unittest.TestCase):
    def test_envelope_preserves_raw_payload_and_context(self):
        raw = '{"lab-data":"exact value"}'
        serialized = build_dlq_record(
            raw,
            INVALID_TYPE,
            "wrong type",
            "telemetry.raw",
            failed_at=datetime(2026, 8, 23, 12, 0, tzinfo=timezone.utc),
        )
        envelope = json.loads(serialized)
        self.assertEqual(envelope["raw_payload"], raw)
        self.assertEqual(envelope["error_category"], "invalid_type")
        self.assertEqual(envelope["source_topic"], "telemetry.raw")
        self.assertEqual(envelope["pipeline_version"], "1.0.0")
        self.assertEqual(envelope["failed_at"], "2026-08-23T12:00:00.000Z")

    def test_unknown_category_is_rejected(self):
        with self.assertRaises(ValueError):
            build_dlq_record("raw", "event-id-123", "bad", "telemetry.raw")


if __name__ == "__main__":
    unittest.main()
