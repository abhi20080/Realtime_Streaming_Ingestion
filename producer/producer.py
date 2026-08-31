#!/usr/bin/env python3
"""Observable telemetry generator for the Kafka/Flink/ClickHouse lab.

Kafka is imported only by :func:`run`; event generation and accounting remain
fast, deterministic, and unit-testable without a broker or native client.

Read from :func:`main` at the bottom: ``run`` wires together the Kafka-free
workload generator, delivery accounting, and the Kafka client.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import random
import signal
import sys
import threading
import time
import uuid
from collections import Counter
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, TextIO


# Event contract and runtime defaults.
SCHEMA_VERSION = 1
DEFAULT_BOOTSTRAP_SERVERS = "kafka:19092"
DEFAULT_TOPIC = "telemetry.raw"
LATE_MIN_SECONDS = 15.0
LATE_MAX_SECONDS = 300.0
NORMAL_EVENT_JITTER_SECONDS = 2.0

EVENT_FIELDS = frozenset(
    {
        "schema_version", "producer_run_id", "producer_sequence", "produced_at",
        "event_id", "tenant_id", "device_id", "metric_name", "metric_value",
        "event_time", "region", "firmware", "injected_duplicate", "injected_late",
    }
)
METRICS = (
    ("temperature_c", 18.0, 42.0),
    ("humidity_pct", 20.0, 90.0),
    ("power_w", 25.0, 2_500.0),
    ("vibration_mm_s", 0.0, 18.0),
)
REGIONS = ("ap-south", "eu-west", "us-east")
FIRMWARES = ("1.8.4", "1.9.0", "2.0.1")
_LOG_LOCK = threading.Lock()


# Shared parsing and structured logging helpers.
def utc_now() -> datetime:
    return datetime.now(UTC)


def format_utc(value: datetime) -> str:
    if value.tzinfo is None:
        raise ValueError("timestamp must be timezone-aware")
    return value.astimezone(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")


def probability(value: str | float) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError) as exc:
        raise argparse.ArgumentTypeError("must be a number between 0 and 1") from exc
    if not 0.0 <= parsed <= 1.0:
        raise argparse.ArgumentTypeError("must be between 0 and 1 inclusive")
    return parsed


def positive_float(value: str | float) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError) as exc:
        raise argparse.ArgumentTypeError("must be a positive number") from exc
    if not math.isfinite(parsed) or parsed <= 0.0:
        raise argparse.ArgumentTypeError("must be a finite number greater than 0")
    return parsed


def nonnegative_int(value: str | int) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise argparse.ArgumentTypeError("must be a non-negative integer") from exc
    if parsed < 0:
        raise argparse.ArgumentTypeError("must be 0 or greater")
    return parsed


def emit_json_log(
    event: str,
    *,
    level: str = "INFO",
    stream: TextIO | None = None,
    timestamp: datetime | None = None,
    **fields: Any,
) -> None:
    """Write exactly one compact JSON object per log entry."""

    record = {
        "timestamp": format_utc(timestamp or utc_now()),
        "level": level,
        "event": event,
        **fields,
    }
    line = json.dumps(record, separators=(",", ":"), sort_keys=True, default=str)
    destination = stream or sys.stdout
    with _LOG_LOCK:
        destination.write(line + "\n")
        destination.flush()


# Kafka-free event and anomaly generation.
class EventFactory:
    """Create original events and faithful, visibly refreshed duplicates."""

    def __init__(
        self,
        *,
        run_id: str | None = None,
        rng: random.Random | None = None,
        clock: Callable[[], datetime] = utc_now,
    ) -> None:
        self.run_id = run_id or str(uuid.uuid4())
        self.rng = rng or random.Random()
        self.clock = clock
        self._sequence = 0
        self._last_produced_at: datetime | None = None

    @property
    def sequence(self) -> int:
        return self._sequence

    def _next_sequence(self) -> int:
        self._sequence += 1
        return self._sequence

    def _next_produced_at(self) -> datetime:
        value = self.clock()
        if value.tzinfo is None:
            raise ValueError("clock must return a timezone-aware datetime")
        value = value.astimezone(UTC)
        # Duplicate records are often created inside one clock tick. A monotonic
        # microsecond makes the refreshed produced_at visible during inspection.
        if self._last_produced_at is not None and value <= self._last_produced_at:
            value = self._last_produced_at + timedelta(microseconds=1)
        self._last_produced_at = value
        return value

    def create_original(self, *, late_rate: float, hot_tenant_rate: float) -> dict[str, Any]:
        late_rate = probability(late_rate)
        hot_tenant_rate = probability(hot_tenant_rate)
        produced_at = self._next_produced_at()
        injected_late = self.rng.random() < late_rate
        age_seconds = self.rng.uniform(
            LATE_MIN_SECONDS if injected_late else 0.0,
            LATE_MAX_SECONDS if injected_late else NORMAL_EVENT_JITTER_SECONDS,
        )
        tenant_id = (
            "tenant-hot"
            if self.rng.random() < hot_tenant_rate
            else f"tenant-{self.rng.randint(1, 20):03d}"
        )
        metric_name, minimum, maximum = self.rng.choice(METRICS)
        return {
            "schema_version": SCHEMA_VERSION,
            "producer_run_id": self.run_id,
            "producer_sequence": self._next_sequence(),
            "produced_at": format_utc(produced_at),
            # Include the run namespace so a deterministic seed remains
            # repeatable within one run without colliding across later runs.
            "event_id": str(
                uuid.uuid5(
                    uuid.NAMESPACE_URL,
                    f"{self.run_id}:{self.rng.getrandbits(128):032x}",
                )
            ),
            "tenant_id": tenant_id,
            "device_id": f"device-{self.rng.randint(1, 2_000):04d}",
            "metric_name": metric_name,
            "metric_value": round(self.rng.uniform(minimum, maximum), 3),
            "event_time": format_utc(produced_at - timedelta(seconds=age_seconds)),
            "region": self.rng.choice(REGIONS),
            "firmware": self.rng.choice(FIRMWARES),
            "injected_duplicate": False,
            "injected_late": injected_late,
        }

    def create_duplicate(self, original: Mapping[str, Any]) -> dict[str, Any]:
        missing = EVENT_FIELDS.difference(original)
        if missing:
            raise ValueError(f"cannot duplicate event missing fields: {sorted(missing)}")
        duplicate = dict(original)
        duplicate["producer_sequence"] = self._next_sequence()
        duplicate["produced_at"] = format_utc(self._next_produced_at())
        duplicate["injected_duplicate"] = True
        return duplicate


def encode_event(event: Mapping[str, Any]) -> bytes:
    return json.dumps(event, separators=(",", ":"), sort_keys=True).encode()


def make_malformed_payload(event: Mapping[str, Any]) -> bytes:
    """Truncate a real event so DLQ inspection still shows useful context."""

    encoded = encode_event(event)
    return encoded[:-1] if encoded.endswith(b"}") else b'{"malformed":'


@dataclass(frozen=True)
class OutboundMessage:
    key: bytes
    value: bytes
    event: Mapping[str, Any]
    malformed: bool
    trace_delivery: bool


class WorkloadGenerator:
    """Apply anomaly policies independently around a testable event factory."""

    def __init__(
        self,
        event_factory: EventFactory,
        *,
        duplicate_rate: float,
        late_rate: float,
        hot_tenant_rate: float,
        bad_json_rate: float,
        trace_sample_rate: float,
        rng: random.Random | None = None,
    ) -> None:
        self.event_factory = event_factory
        self.duplicate_rate = probability(duplicate_rate)
        self.late_rate = probability(late_rate)
        self.hot_tenant_rate = probability(hot_tenant_rate)
        self.bad_json_rate = probability(bad_json_rate)
        self.trace_sample_rate = probability(trace_sample_rate)
        self.rng = rng or random.Random()

    def _outbound(self, event: Mapping[str, Any], malformed: bool) -> OutboundMessage:
        return OutboundMessage(
            # Tenant keys deliberately make the hot-tenant workload visible as
            # Kafka partition skew; duplicates retain the same tenant/key.
            key=str(event["tenant_id"]).encode(),
            value=make_malformed_payload(event) if malformed else encode_event(event),
            event=event,
            malformed=malformed,
            trace_delivery=self.rng.random() < self.trace_sample_rate,
        )

    def next_batch(self) -> list[OutboundMessage]:
        """Return one original plus an optional duplicate.

        Bad JSON applies only to originals; duplicates remain valid so duplicate
        and DLQ experiments stay independently understandable.
        """

        original = self.event_factory.create_original(
            late_rate=self.late_rate, hot_tenant_rate=self.hot_tenant_rate
        )
        batch = [self._outbound(original, self.rng.random() < self.bad_json_rate)]
        if self.rng.random() < self.duplicate_rate:
            batch.append(self._outbound(self.event_factory.create_duplicate(original), False))
        return batch


# Delivery accounting shared with Kafka callbacks.
@dataclass(frozen=True)
class StatsSnapshot:
    attempted: int
    attempted_bytes: int
    acked: int
    failed: int
    undelivered: int
    partition_counts: dict[int, int]
    offset_ranges: dict[int, tuple[int, int]]
    ack_latency_total_ms: float
    ack_latency_max_ms: float
    tx_retries: int

    @property
    def pending(self) -> int:
        return max(0, self.attempted - self.acked - self.failed - self.undelivered)

    @property
    def reconciled(self) -> bool:
        return self.attempted == self.acked + self.failed + self.undelivered


class ProducerStats:
    """Thread-safe counters shared with native delivery callbacks."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._attempted = 0
        self._attempted_bytes = 0
        self._acked = 0
        self._failed = 0
        self._undelivered = 0
        self._partition_counts: Counter[int] = Counter()
        self._offset_ranges: dict[int, tuple[int, int]] = {}
        self._ack_latency_total_ms = 0.0
        self._ack_latency_max_ms = 0.0
        self._tx_retries = 0

    def record_attempt(self, payload_bytes: int) -> None:
        with self._lock:
            self._attempted += 1
            self._attempted_bytes += payload_bytes

    def record_ack(self, partition: int, offset: int, latency_ms: float) -> None:
        with self._lock:
            self._acked += 1
            self._partition_counts[partition] += 1
            low, high = self._offset_ranges.get(partition, (offset, offset))
            self._offset_ranges[partition] = (min(low, offset), max(high, offset))
            self._ack_latency_total_ms += latency_ms
            self._ack_latency_max_ms = max(self._ack_latency_max_ms, latency_ms)

    def record_failure(self) -> None:
        with self._lock:
            self._failed += 1

    def record_undelivered(self, count: int) -> None:
        if count < 0:
            raise ValueError("undelivered count cannot be negative")
        with self._lock:
            self._undelivered += count

    def observe_tx_retries(self, count: int) -> None:
        """Retain the largest cumulative retry count reported by librdkafka."""

        if count < 0:
            raise ValueError("retry count cannot be negative")
        with self._lock:
            self._tx_retries = max(self._tx_retries, count)

    def record_kafka_statistics(self, statistics_json: str) -> int:
        """Update retry counters when librdkafka invokes ``stats_cb``."""

        try:
            self.observe_tx_retries(extract_tx_retries(statistics_json))
        except (TypeError, ValueError) as exc:
            emit_json_log(
                "kafka_statistics_parse_failure",
                level="WARN",
                error_type=type(exc).__name__,
                error=str(exc),
            )
        return 0

    def snapshot(self) -> StatsSnapshot:
        with self._lock:
            return StatsSnapshot(
                attempted=self._attempted,
                attempted_bytes=self._attempted_bytes,
                acked=self._acked,
                failed=self._failed,
                undelivered=self._undelivered,
                partition_counts=dict(self._partition_counts),
                offset_ranges=dict(self._offset_ranges),
                ack_latency_total_ms=self._ack_latency_total_ms,
                ack_latency_max_ms=self._ack_latency_max_ms,
                tx_retries=self._tx_retries,
            )


def snapshot_log_fields(snapshot: StatsSnapshot) -> dict[str, Any]:
    average = snapshot.ack_latency_total_ms / snapshot.acked if snapshot.acked else 0.0
    return {
        "attempted": snapshot.attempted,
        "attempted_bytes": snapshot.attempted_bytes,
        "acked": snapshot.acked,
        "failed": snapshot.failed,
        "undelivered": snapshot.undelivered,
        "pending": snapshot.pending,
        "tx_retries": snapshot.tx_retries,
        "partition_counts": {str(k): v for k, v in sorted(snapshot.partition_counts.items())},
        "offset_ranges": {
            str(k): {"min": v[0], "max": v[1]} for k, v in sorted(snapshot.offset_ranges.items())
        },
        "ack_latency_avg_ms": round(average, 3),
        "ack_latency_max_ms": round(snapshot.ack_latency_max_ms, 3),
    }


class PeriodicSummary:
    def __init__(self, interval_seconds: float, *, started_at: float) -> None:
        if interval_seconds <= 0:
            raise ValueError("interval_seconds must be positive")
        self.interval_seconds = interval_seconds
        self.started_at = started_at
        self.last_at = started_at
        self.last_attempted = 0
        self.last_acked = 0
        self.last_failed = 0

    def due(self, now: float) -> bool:
        return now - self.last_at >= self.interval_seconds

    def build(self, snapshot: StatsSnapshot, *, now: float, queue_depth: int) -> dict[str, Any]:
        interval = max(now - self.last_at, 1e-9)
        attempted = snapshot.attempted - self.last_attempted
        acked = snapshot.acked - self.last_acked
        failed = snapshot.failed - self.last_failed
        fields = snapshot_log_fields(snapshot)
        fields.update(
            elapsed_seconds=round(max(now - self.started_at, 0.0), 3),
            interval_seconds=round(interval, 3),
            interval_attempted=attempted,
            interval_acked=acked,
            interval_failed=failed,
            attempted_per_second=round(attempted / interval, 3),
            acked_per_second=round(acked / interval, 3),
            queue_depth=queue_depth,
        )
        self.last_at = now
        self.last_attempted, self.last_acked, self.last_failed = (
            snapshot.attempted, snapshot.acked, snapshot.failed
        )
        return fields


# Kafka callback helpers.
def extract_tx_retries(statistics: str | Mapping[str, Any]) -> int:
    """Aggregate per-broker ``txretries`` from librdkafka statistics JSON."""

    parsed = json.loads(statistics) if isinstance(statistics, str) else statistics
    if not isinstance(parsed, Mapping):
        raise ValueError("Kafka statistics must be a JSON object")
    # Some client builds expose a global counter. Prefer it to avoid counting
    # the same retry once globally and again under its broker.
    global_count = parsed.get("txretries")
    if isinstance(global_count, (int, float)) and not isinstance(global_count, bool):
        return max(0, int(global_count))
    brokers = parsed.get("brokers", {})
    if not isinstance(brokers, Mapping):
        return 0

    total = 0
    for broker in brokers.values():
        if not isinstance(broker, Mapping):
            continue
        count = broker.get("txretries", 0)
        if isinstance(count, (int, float)) and not isinstance(count, bool):
            total += max(0, int(count))
    return total


def _safe_error_value(error: Any, method_name: str) -> Any:
    method = getattr(error, method_name, None)
    try:
        return method() if callable(method) else None
    except Exception:  # pragma: no cover - defensive around native objects
        return None


# CLI and producer loop. Start with main() at the bottom of this section.
def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Publish observable telemetry to Kafka")
    parser.add_argument(
        "--bootstrap-servers",
        default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", DEFAULT_BOOTSTRAP_SERVERS),
    )
    parser.add_argument("--topic", default=os.getenv("KAFKA_TOPIC", DEFAULT_TOPIC))
    parser.add_argument("--rate", type=positive_float, default=10.0, help="original events/second")
    parser.add_argument("--count", type=nonnegative_int, default=0,
                        help="original events; duplicates are additional; 0 runs forever")
    parser.add_argument("--duplicate-rate", type=probability, default=0.02)
    parser.add_argument("--late-rate", type=probability, default=0.05)
    parser.add_argument("--hot-tenant-rate", type=probability, default=0.35)
    parser.add_argument("--bad-json-rate", type=probability, default=0.01)
    parser.add_argument("--trace-sample-rate", type=probability, default=0.01)
    parser.add_argument(
        "--stats-interval-seconds", "--stats-interval", dest="stats_interval_seconds",
        type=positive_float, default=float(os.getenv("PRODUCER_STATS_INTERVAL_SECONDS", "5")),
    )
    parser.add_argument("--flush-timeout-seconds", type=positive_float, default=15.0)
    parser.add_argument("--seed", type=int, default=None)
    parser.add_argument("--run-id", default=None)
    return parser


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = build_parser()
    args = parser.parse_args(argv)
    if not args.bootstrap_servers.strip():
        parser.error("--bootstrap-servers cannot be empty")
    if not args.topic.strip():
        parser.error("--topic cannot be empty")
    if args.run_id is not None and not args.run_id.strip():
        parser.error("--run-id cannot be empty")
    return args


def _enqueue(
    producer: Any,
    topic: str,
    outbound: OutboundMessage,
    stats: ProducerStats,
    stop_event: threading.Event,
) -> None:
    stats.record_attempt(len(outbound.value))
    enqueued_at = time.monotonic()

    def delivery_callback(error: Any, message: Any) -> None:
        latency_ms = max(0.0, (time.monotonic() - enqueued_at) * 1_000)
        common = {
            "event_id": outbound.event["event_id"],
            "producer_sequence": outbound.event["producer_sequence"],
            "malformed": outbound.malformed,
            "latency_ms": round(latency_ms, 3),
        }
        if error is not None:
            stats.record_failure()
            emit_json_log(
                "delivery_failure",
                level="ERROR",
                stage="delivery_callback",
                kafka_error=str(error),
                kafka_error_code=_safe_error_value(error, "code"),
                kafka_error_name=_safe_error_value(error, "name"),
                retriable=_safe_error_value(error, "retriable"),
                fatal=_safe_error_value(error, "fatal"),
                **common,
            )
            return

        partition = int(message.partition())
        offset = int(message.offset())
        stats.record_ack(partition, offset, latency_ms)
        if outbound.trace_delivery:
            emit_json_log(
                "delivery_ack",
                topic=message.topic(),
                partition=partition,
                offset=offset,
                **common,
            )

    while not stop_event.is_set():
        try:
            producer.produce(
                topic=topic,
                key=outbound.key,
                value=outbound.value,
                on_delivery=delivery_callback,
            )
            return
        except BufferError:
            producer.poll(0.1)
        except Exception as exc:
            stats.record_failure()
            emit_json_log(
                "delivery_failure", level="ERROR", stage="enqueue",
                error_type=type(exc).__name__, error=str(exc),
                event_id=outbound.event["event_id"],
                producer_sequence=outbound.event["producer_sequence"], malformed=outbound.malformed,
            )
            return
    stats.record_failure()
    emit_json_log(
        "delivery_failure", level="WARN", stage="enqueue_cancelled",
        error="shutdown requested before enqueue", event_id=outbound.event["event_id"],
        producer_sequence=outbound.event["producer_sequence"], malformed=outbound.malformed,
    )


def run(args: argparse.Namespace) -> int:
    try:
        from confluent_kafka import Producer
    except ImportError:
        emit_json_log(
            "producer_startup_failure",
            level="ERROR",
            error="confluent-kafka is not installed",
        )
        return 2

    run_id = args.run_id or str(uuid.uuid4())
    seed = args.seed if args.seed is not None else random.SystemRandom().randrange(2**63)
    factory = EventFactory(run_id=run_id, rng=random.Random(seed))
    # Keep event values and anomaly decisions deterministic but independent.
    workload = WorkloadGenerator(
        factory, duplicate_rate=args.duplicate_rate, late_rate=args.late_rate,
        hot_tenant_rate=args.hot_tenant_rate, bad_json_rate=args.bad_json_rate,
        trace_sample_rate=args.trace_sample_rate, rng=random.Random(seed ^ 0x5DEECE66D),
    )
    stats = ProducerStats()
    stop_event = threading.Event()
    kafka_config = {
        "bootstrap.servers": args.bootstrap_servers,
        # Keep the Kafka client dimension bounded across repeated lab runs.
        # producer_run_id remains available in payloads and structured logs.
        "client.id": "telemetry-producer",
        "acks": "all",
        "enable.idempotence": True,
        "retries": 2_147_483_647,
        "delivery.timeout.ms": 120_000,
        "request.timeout.ms": 30_000,
        "linger.ms": 5,
        "statistics.interval.ms": max(1_000, int(args.stats_interval_seconds * 1_000)),
        "stats_cb": stats.record_kafka_statistics,
    }
    producer = Producer(kafka_config)
    received_signal: str | None = None

    def request_stop(signum: int, _frame: Any) -> None:
        nonlocal received_signal
        received_signal = signal.Signals(signum).name
        stop_event.set()

    previous_handlers = {
        signum: signal.signal(signum, request_stop)
        for signum in (signal.SIGINT, signal.SIGTERM)
    }
    started_at = time.monotonic()
    reporter = PeriodicSummary(args.stats_interval_seconds, started_at=started_at)
    emit_json_log(
        "producer_startup", producer_run_id=run_id, seed=seed,
        bootstrap_servers=args.bootstrap_servers, topic=args.topic,
        rate=args.rate, count=args.count,
        count_semantics="original_events; duplicates_are_additional_records",
        duplicate_rate=args.duplicate_rate, late_rate=args.late_rate,
        hot_tenant_rate=args.hot_tenant_rate, bad_json_rate=args.bad_json_rate,
        trace_sample_rate=args.trace_sample_rate,
        stats_interval_seconds=args.stats_interval_seconds,
        kafka_client_id=kafka_config["client.id"], kafka_acks="all",
        kafka_idempotence=True,
    )

    originals_generated = 0
    shutdown_reason = "count_completed"
    next_original_at = started_at
    try:
        while not stop_event.is_set() and (args.count == 0 or originals_generated < args.count):
            while not stop_event.is_set():
                remaining = next_original_at - time.monotonic()
                if remaining <= 0:
                    break
                producer.poll(min(remaining, 0.1))
            if stop_event.is_set():
                break
            for outbound in workload.next_batch():
                _enqueue(producer, args.topic, outbound, stats, stop_event)
            originals_generated += 1
            producer.poll(0)
            now = time.monotonic()
            if reporter.due(now):
                emit_json_log(
                    "producer_summary", producer_run_id=run_id,
                    originals_generated=originals_generated,
                    **reporter.build(stats.snapshot(), now=now, queue_depth=len(producer)),
                )
            next_original_at += 1.0 / args.rate
        if stop_event.is_set():
            shutdown_reason = (
                f"signal_{received_signal}"
                if received_signal
                else "stop_requested"
            )
    except Exception as exc:
        shutdown_reason = "unhandled_exception"
        emit_json_log(
            "producer_runtime_failure",
            level="ERROR",
            error_type=type(exc).__name__,
            error=str(exc),
        )
    finally:
        emit_json_log(
            "producer_flush_started", producer_run_id=run_id, queue_depth=len(producer),
            timeout_seconds=args.flush_timeout_seconds,
        )
        remaining = int(producer.flush(args.flush_timeout_seconds))
        if remaining:
            stats.record_undelivered(remaining)
        final_snapshot = stats.snapshot()
        emit_json_log(
            "producer_final", level="INFO" if final_snapshot.reconciled else "ERROR",
            producer_run_id=run_id, shutdown_reason=shutdown_reason,
            originals_generated=originals_generated,
            elapsed_seconds=round(max(time.monotonic() - started_at, 0.0), 3),
            reconciled=final_snapshot.reconciled, **snapshot_log_fields(final_snapshot),
        )
        for signum, previous in previous_handlers.items():
            signal.signal(signum, previous)

    succeeded = (
        shutdown_reason != "unhandled_exception"
        and final_snapshot.failed == 0
        and final_snapshot.undelivered == 0
    )
    return 0 if succeeded else 1


def main(argv: Sequence[str] | None = None) -> int:
    return run(parse_args(argv))


if __name__ == "__main__":
    raise SystemExit(main())
