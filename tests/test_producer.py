from __future__ import annotations

import io
import json
import random
from datetime import UTC, datetime

import pytest

from producer.producer import (
    EVENT_FIELDS,
    LATE_MIN_SECONDS,
    EventFactory,
    KafkaStatisticsObserver,
    PeriodicSummary,
    ProducerStats,
    WorkloadGenerator,
    emit_json_log,
    extract_tx_retries,
    format_utc,
    nonnegative_int,
    positive_float,
    probability,
    snapshot_log_fields,
)


FIXED_NOW = datetime(2026, 8, 23, 12, 0, 0, tzinfo=UTC)


def parse_timestamp(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def test_original_event_has_exact_flat_schema_and_truthful_late_flag() -> None:
    factory = EventFactory(run_id="run-test", rng=random.Random(7), clock=lambda: FIXED_NOW)

    event = factory.create_original(late_rate=1.0, hot_tenant_rate=1.0)
    age = (parse_timestamp(event["produced_at"]) - parse_timestamp(event["event_time"])).total_seconds()

    assert set(event) == EVENT_FIELDS
    assert event["schema_version"] == 1
    assert event["producer_run_id"] == "run-test"
    assert event["producer_sequence"] == 1
    assert event["tenant_id"] == "tenant-hot"
    assert event["injected_duplicate"] is False
    assert event["injected_late"] is True
    assert age >= LATE_MIN_SECONDS
    assert isinstance(event["metric_value"], float)


def test_non_late_event_is_only_slightly_backdated() -> None:
    factory = EventFactory(rng=random.Random(11), clock=lambda: FIXED_NOW)
    event = factory.create_original(late_rate=0.0, hot_tenant_rate=0.0)
    age = (parse_timestamp(event["produced_at"]) - parse_timestamp(event["event_time"])).total_seconds()

    assert event["injected_late"] is False
    assert 0.0 <= age <= 2.0


def test_duplicate_preserves_business_data_and_refreshes_delivery_metadata() -> None:
    factory = EventFactory(run_id="run-test", rng=random.Random(3), clock=lambda: FIXED_NOW)
    original = factory.create_original(late_rate=1.0, hot_tenant_rate=0.0)

    duplicate = factory.create_duplicate(original)

    preserved = EVENT_FIELDS - {"producer_sequence", "produced_at", "injected_duplicate"}
    assert {field: duplicate[field] for field in preserved} == {
        field: original[field] for field in preserved
    }
    assert duplicate["producer_sequence"] == original["producer_sequence"] + 1
    assert duplicate["produced_at"] != original["produced_at"]
    assert duplicate["injected_duplicate"] is True
    assert original["injected_duplicate"] is False


def test_seeded_event_ids_are_repeatable_within_but_unique_across_runs() -> None:
    first = EventFactory(run_id="run-a", rng=random.Random(23), clock=lambda: FIXED_NOW)
    same = EventFactory(run_id="run-a", rng=random.Random(23), clock=lambda: FIXED_NOW)
    other = EventFactory(run_id="run-b", rng=random.Random(23), clock=lambda: FIXED_NOW)

    first_id = first.create_original(late_rate=0, hot_tenant_rate=0)["event_id"]
    same_id = same.create_original(late_rate=0, hot_tenant_rate=0)["event_id"]
    other_id = other.create_original(late_rate=0, hot_tenant_rate=0)["event_id"]

    assert first_id == same_id
    assert first_id != other_id


def test_workload_can_emit_malformed_original_and_valid_duplicate() -> None:
    factory = EventFactory(run_id="run-test", rng=random.Random(17), clock=lambda: FIXED_NOW)
    workload = WorkloadGenerator(
        factory,
        duplicate_rate=1.0,
        late_rate=1.0,
        hot_tenant_rate=1.0,
        bad_json_rate=1.0,
        trace_sample_rate=1.0,
        rng=random.Random(19),
    )

    original, duplicate = workload.next_batch()

    assert original.malformed is True
    with pytest.raises(json.JSONDecodeError):
        json.loads(original.value)
    assert duplicate.malformed is False
    decoded_duplicate = json.loads(duplicate.value)
    assert decoded_duplicate["event_id"] == original.event["event_id"]
    assert decoded_duplicate["producer_sequence"] == 2
    assert decoded_duplicate["injected_duplicate"] is True
    assert original.key == b"tenant-hot"
    assert duplicate.key == original.key
    assert original.trace_delivery is True
    assert duplicate.trace_delivery is True


@pytest.mark.parametrize("value", ["-0.1", "1.1", "not-a-number"])
def test_probability_rejects_invalid_values(value: str) -> None:
    with pytest.raises(Exception):
        probability(value)


@pytest.mark.parametrize("value", ["0", "-1", "nan", "inf", "not-a-number"])
def test_positive_float_rejects_non_positive_values(value: str) -> None:
    with pytest.raises(Exception):
        positive_float(value)


def test_nonnegative_count_accepts_forever_sentinel_and_rejects_negative() -> None:
    assert nonnegative_int("0") == 0
    with pytest.raises(Exception):
        nonnegative_int("-1")


def test_stats_reconcile_partitions_offsets_failures_and_undelivered() -> None:
    stats = ProducerStats()
    for size in (100, 110, 120, 130):
        stats.record_attempt(size)
    stats.record_ack(partition=1, offset=8, latency_ms=4.0)
    stats.record_ack(partition=1, offset=5, latency_ms=8.0)
    stats.record_failure()
    stats.record_undelivered(1)
    stats.observe_tx_retries(7)

    snapshot = stats.snapshot()
    fields = snapshot_log_fields(snapshot)

    assert snapshot.reconciled is True
    assert snapshot.pending == 0
    assert fields["attempted"] == 4
    assert fields["attempted_bytes"] == 460
    assert fields["partition_counts"] == {"1": 2}
    assert fields["offset_ranges"] == {"1": {"min": 5, "max": 8}}
    assert fields["ack_latency_avg_ms"] == 6.0
    assert fields["ack_latency_max_ms"] == 8.0
    assert fields["tx_retries"] == 7


def test_librdkafka_statistics_callback_aggregates_broker_retries() -> None:
    payload = json.dumps(
        {
            "name": "producer-test",
            "brokers": {
                "kafka:19092/1": {"txretries": 3},
                "kafka-2:19092/2": {"txretries": 4},
            },
        }
    )
    stats = ProducerStats()

    assert extract_tx_retries(payload) == 7
    assert KafkaStatisticsObserver(stats)(payload) == 0
    assert snapshot_log_fields(stats.snapshot())["tx_retries"] == 7


def test_periodic_summary_reports_interval_throughput_and_queue_depth() -> None:
    stats = ProducerStats()
    stats.record_attempt(10)
    stats.record_ack(partition=0, offset=2, latency_ms=1.0)
    reporter = PeriodicSummary(5.0, started_at=100.0)

    assert reporter.due(104.99) is False
    assert reporter.due(105.0) is True
    summary = reporter.build(stats.snapshot(), now=105.0, queue_depth=3)

    assert summary["interval_attempted"] == 1
    assert summary["attempted_per_second"] == 0.2
    assert summary["acked_per_second"] == 0.2
    assert summary["queue_depth"] == 3


def test_json_logger_emits_one_parseable_line() -> None:
    output = io.StringIO()
    emit_json_log(
        "producer_summary", stream=output, timestamp=FIXED_NOW, attempted=5, queue_depth=2
    )

    assert output.getvalue().count("\n") == 1
    assert json.loads(output.getvalue()) == {
        "attempted": 5,
        "event": "producer_summary",
        "level": "INFO",
        "queue_depth": 2,
        "timestamp": format_utc(FIXED_NOW),
    }
