"""Create telemetry and inject controlled anomalies without a Kafka client."""

from __future__ import annotations

import argparse
import json
import random
import uuid
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any


SCHEMA_VERSION = 1
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


