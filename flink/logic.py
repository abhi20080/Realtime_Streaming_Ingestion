"""Pure validation, DLQ, and event-time helpers (no PyFlink dependency)."""

from __future__ import annotations

import json
import math
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Mapping, Optional


PIPELINE_VERSION = "1.0.0"
LONG_MIN = -(1 << 63)
LONG_MAX = (1 << 63) - 1
UINT16_MAX = (1 << 16) - 1
MAX_ERROR_DETAIL_LENGTH = 256
CLICKHOUSE_DATETIME64_MIN = datetime(1900, 1, 1, tzinfo=timezone.utc)
CLICKHOUSE_DATETIME64_MAX = datetime(
    2299, 12, 31, 23, 59, 59, 999000, tzinfo=timezone.utc
)
INVALID_JSON = "invalid_json"
MISSING_FIELD = "missing_field"
INVALID_TYPE = "invalid_type"
INVALID_TIMESTAMP = "invalid_timestamp"
ERROR_CATEGORIES = (INVALID_JSON, MISSING_FIELD, INVALID_TYPE, INVALID_TIMESTAMP)
REQUIRED_FIELDS = (
    "schema_version", "producer_run_id", "producer_sequence", "produced_at",
    "event_id", "tenant_id", "device_id", "metric_name", "metric_value",
    "event_time", "region", "firmware", "injected_duplicate", "injected_late",
)


@dataclass(frozen=True)
class ValidatedEvent:
    schema_version: int
    producer_run_id: str
    producer_sequence: int
    produced_at: datetime
    event_id: str
    tenant_id: str
    device_id: str
    metric_name: str
    metric_value: float
    event_time: datetime
    region: str
    firmware: str
    injected_duplicate: bool
    injected_late: bool

    @property
    def produced_at_ms(self) -> int:
        return int(self.produced_at.timestamp() * 1_000)

    @property
    def event_time_ms(self) -> int:
        return int(self.event_time.timestamp() * 1_000)


@dataclass(frozen=True)
class ValidationResult:
    event: Optional[ValidatedEvent] = None
    error_category: Optional[str] = None
    error_detail: Optional[str] = None

    @property
    def is_valid(self) -> bool:
        return self.event is not None


class _ValidationIssue(Exception):
    def __init__(self, category: str, detail: str) -> None:
        super().__init__(detail)
        self.category = category
        self.detail = bounded_error_detail(detail)


def bounded_error_detail(detail: object) -> str:
    single_line = " ".join(str(detail).splitlines()).strip()
    if len(single_line) <= MAX_ERROR_DETAIL_LENGTH:
        return single_line
    return single_line[: MAX_ERROR_DETAIL_LENGTH - 3] + "..."


def _nonempty_string(payload: Mapping[str, Any], field: str) -> str:
    value = payload[field]
    if not isinstance(value, str) or not value.strip():
        raise _ValidationIssue(INVALID_TYPE, f"{field} must be a non-empty string")
    return value


def _timestamp(payload: Mapping[str, Any], field: str) -> datetime:
    value = payload[field]
    if not isinstance(value, str):
        raise _ValidationIssue(INVALID_TIMESTAMP, f"{field} must be an ISO-8601 string")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (ValueError, OverflowError) as exc:
        raise _ValidationIssue(
            INVALID_TIMESTAMP, f"{field} is not a valid ISO-8601 timestamp: {exc}"
        ) from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise _ValidationIssue(
            INVALID_TIMESTAMP, f"{field} must include a UTC offset or Z suffix"
        )
    try:
        normalized = parsed.astimezone(timezone.utc)
    except (ValueError, OverflowError) as exc:
        raise _ValidationIssue(
            INVALID_TIMESTAMP, f"{field} cannot be represented in UTC: {exc}"
        ) from exc
    if not CLICKHOUSE_DATETIME64_MIN <= normalized <= CLICKHOUSE_DATETIME64_MAX:
        raise _ValidationIssue(
            INVALID_TIMESTAMP,
            f"{field} is outside ClickHouse DateTime64(3) range "
            "1900-01-01 through 2299-12-31 UTC",
        )
    return normalized


def validate_event(raw_payload: str) -> ValidationResult:
    """Validate one Kafka value using only four bounded error categories."""
    try:
        payload = json.loads(raw_payload)
    except (json.JSONDecodeError, UnicodeError) as exc:
        return ValidationResult(
            error_category=INVALID_JSON,
            error_detail=bounded_error_detail(f"JSON decoding failed: {exc}"),
        )
    if not isinstance(payload, dict):
        return ValidationResult(
            error_category=INVALID_TYPE,
            error_detail="top-level JSON value must be an object",
        )
    missing = [field for field in REQUIRED_FIELDS if field not in payload]
    if missing:
        return ValidationResult(
            error_category=MISSING_FIELD,
            error_detail=bounded_error_detail(
                "missing required field(s): " + ", ".join(missing)
            ),
        )

    try:
        schema_version = payload["schema_version"]
        if (isinstance(schema_version, bool) or not isinstance(schema_version, int)
                or not 1 <= schema_version <= UINT16_MAX):
            raise _ValidationIssue(
                INVALID_TYPE,
                f"schema_version must be an integer from 1 through {UINT16_MAX}",
            )
        sequence = payload["producer_sequence"]
        if (isinstance(sequence, bool) or not isinstance(sequence, int)
                or not 0 <= sequence <= LONG_MAX):
            raise _ValidationIssue(
                INVALID_TYPE,
                f"producer_sequence must be an integer from 0 through {LONG_MAX}",
            )
        metric_value = payload["metric_value"]
        if isinstance(metric_value, bool) or not isinstance(metric_value, (int, float)):
            raise _ValidationIssue(INVALID_TYPE, "metric_value must be numeric")
        metric_value = float(metric_value)
        if not math.isfinite(metric_value):
            raise _ValidationIssue(INVALID_TYPE, "metric_value must be finite")
        for field in ("injected_duplicate", "injected_late"):
            if not isinstance(payload[field], bool):
                raise _ValidationIssue(INVALID_TYPE, f"{field} must be boolean")

        event = ValidatedEvent(
            schema_version=schema_version,
            producer_run_id=_nonempty_string(payload, "producer_run_id"),
            producer_sequence=sequence,
            produced_at=_timestamp(payload, "produced_at"),
            event_id=_nonempty_string(payload, "event_id"),
            tenant_id=_nonempty_string(payload, "tenant_id"),
            device_id=_nonempty_string(payload, "device_id"),
            metric_name=_nonempty_string(payload, "metric_name"),
            metric_value=metric_value,
            event_time=_timestamp(payload, "event_time"),
            region=_nonempty_string(payload, "region"),
            firmware=_nonempty_string(payload, "firmware"),
            injected_duplicate=payload["injected_duplicate"],
            injected_late=payload["injected_late"],
        )
    except _ValidationIssue as exc:
        return ValidationResult(error_category=exc.category, error_detail=exc.detail)
    return ValidationResult(event=event)


def event_time_ms_for_watermark(raw_payload: str) -> int:
    """Extract valid event time without letting a rejected record advance time.

    Kafka source watermark generation needs a timestamp for every raw value.
    Epoch zero is safely behind all lab events and therefore has no effect once
    a valid record has established the split's maximum timestamp.
    """
    result = validate_event(raw_payload)
    return result.event.event_time_ms if result.is_valid else 0


def normalize_watermark_ms(watermark_ms: Optional[int]) -> Optional[int]:
    """Turn Flink's pre-first-watermark Long.MIN_VALUE sentinel into None."""
    if watermark_ms is None or watermark_ms <= LONG_MIN:
        return None
    return int(watermark_ms)


def calculate_lateness(
    event_time_ms: int, watermark_ms: Optional[int]
) -> tuple[bool, int]:
    """Return late-at-arrival state and lateness in milliseconds."""
    normalized = normalize_watermark_ms(watermark_ms)
    if normalized is None or event_time_ms > normalized:
        return False, 0
    return True, max(0, normalized - int(event_time_ms))


def utc_iso_millis(value: datetime) -> str:
    if value.tzinfo is None or value.utcoffset() is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat(
        timespec="milliseconds"
    ).replace("+00:00", "Z")


def build_dlq_record(
    raw_payload: str,
    error_category: str,
    error_detail: str,
    source_topic: str,
    *,
    pipeline_version: str = PIPELINE_VERSION,
    failed_at: Optional[datetime] = None,
) -> str:
    """Create a stable DLQ envelope while preserving the raw string payload."""
    if error_category not in ERROR_CATEGORIES:
        raise ValueError(f"unbounded error category: {error_category}")
    envelope = {
        "raw_payload": raw_payload,
        "error_category": error_category,
        "error_detail": bounded_error_detail(error_detail),
        "source_topic": source_topic,
        "pipeline_version": pipeline_version,
        "failed_at": utc_iso_millis(failed_at or datetime.now(timezone.utc)),
    }
    return json.dumps(envelope, separators=(",", ":"), ensure_ascii=False)
