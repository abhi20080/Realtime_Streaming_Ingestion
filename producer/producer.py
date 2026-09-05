#!/usr/bin/env python3
"""Publish the lab workload to Kafka. Start with main() and run().

workload.py owns event generation; delivery.py owns delivery accounting.
Kafka is imported only inside run(), so --help and unit tests need no broker.
"""

from __future__ import annotations

import argparse
import math
import os
import random
import signal
import threading
import time
import uuid
from collections.abc import Mapping, Sequence
from typing import Any

if __package__:
    from .delivery import PeriodicSummary, ProducerStats, StatsSnapshot, snapshot_log_fields
    from .logging_utils import emit_json_log
    from .workload import EventFactory, OutboundMessage, WorkloadGenerator, probability
else:  # Keep python producer.py and the existing Docker entry point working.
    from delivery import PeriodicSummary, ProducerStats, StatsSnapshot, snapshot_log_fields
    from logging_utils import emit_json_log
    from workload import EventFactory, OutboundMessage, WorkloadGenerator, probability

DEFAULT_BOOTSTRAP_SERVERS = "kafka:19092"
DEFAULT_TOPIC = "telemetry.raw"


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


def _build_workload(
    args: argparse.Namespace,
    *,
    run_id: str,
    seed: int,
) -> WorkloadGenerator:
    factory = EventFactory(run_id=run_id, rng=random.Random(seed))
    # Keep event values and anomaly decisions deterministic but independent.
    return WorkloadGenerator(
        factory,
        duplicate_rate=args.duplicate_rate,
        late_rate=args.late_rate,
        hot_tenant_rate=args.hot_tenant_rate,
        bad_json_rate=args.bad_json_rate,
        trace_sample_rate=args.trace_sample_rate,
        rng=random.Random(seed ^ 0x5DEECE66D),
    )


def _build_kafka_config(
    args: argparse.Namespace,
    stats: ProducerStats,
) -> dict[str, Any]:
    return {
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


def _log_producer_startup(
    args: argparse.Namespace,
    *,
    run_id: str,
    seed: int,
    kafka_config: Mapping[str, Any],
) -> None:
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


def _wait_for_next_original(
    producer: Any,
    stop_event: threading.Event,
    next_original_at: float,
) -> bool:
    while not stop_event.is_set():
        remaining = next_original_at - time.monotonic()
        if remaining <= 0:
            return True
        producer.poll(min(remaining, 0.1))
    return False


def _publish_workload(
    *,
    args: argparse.Namespace,
    producer: Any,
    workload: WorkloadGenerator,
    stats: ProducerStats,
    stop_event: threading.Event,
    reporter: PeriodicSummary,
    run_id: str,
    started_at: float,
) -> int:
    originals_generated = 0
    next_original_at = started_at
    while not stop_event.is_set() and (
        args.count == 0 or originals_generated < args.count
    ):
        if not _wait_for_next_original(producer, stop_event, next_original_at):
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
    return originals_generated


def _finalize_producer_run(
    *,
    args: argparse.Namespace,
    producer: Any,
    stats: ProducerStats,
    run_id: str,
    shutdown_reason: str,
    originals_generated: int,
    started_at: float,
) -> StatsSnapshot:
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
    return final_snapshot


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
    workload = _build_workload(args, run_id=run_id, seed=seed)
    stats = ProducerStats()
    stop_event = threading.Event()
    kafka_config = _build_kafka_config(args, stats)
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
    _log_producer_startup(
        args,
        run_id=run_id,
        seed=seed,
        kafka_config=kafka_config,
    )

    originals_generated = 0
    shutdown_reason = "count_completed"
    try:
        originals_generated = _publish_workload(
            args=args,
            producer=producer,
            workload=workload,
            stats=stats,
            stop_event=stop_event,
            reporter=reporter,
            run_id=run_id,
            started_at=started_at,
        )
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
        final_snapshot = _finalize_producer_run(
            args=args,
            producer=producer,
            stats=stats,
            run_id=run_id,
            shutdown_reason=shutdown_reason,
            originals_generated=originals_generated,
            started_at=started_at,
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
