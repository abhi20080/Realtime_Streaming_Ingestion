"""Account for Kafka delivery and summarize throughput without a Kafka client."""

from __future__ import annotations

import json
import threading
from collections import Counter
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

if __package__:
    from .logging_utils import emit_json_log
else:
    from logging_utils import emit_json_log


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


