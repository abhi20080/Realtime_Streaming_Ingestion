"""Map validated events to Flink rows and the ClickHouse INSERT contract.

The final parsed field, event_time_ms, is internal to Flink. Observation replaces
it with four audit fields. ClickHouse supplies clickhouse_ingested_at itself.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime, timezone
from typing import Any

from logic import REQUIRED_FIELDS, ValidatedEvent


PARSED_FIELDS = (*REQUIRED_FIELDS, "event_time_ms")
SINK_FIELDS = (*REQUIRED_FIELDS,
    "flink_processed_at", "watermark_at_arrival", "is_late_at_flink", "lateness_ms",
)
INSERT_SQL = (
    "INSERT INTO perfmon.telemetry_events (" + ", ".join(SINK_FIELDS) + ") "
    "VALUES (" + ", ".join("?" for _ in SINK_FIELDS) + ")"
)


def to_flink_sql_timestamp(value: datetime) -> datetime:
    """Represent a UTC instant in Flink's timezone-free SQL timestamp type."""
    return value.astimezone(timezone.utc).replace(tzinfo=None)


def event_to_row_values(event: ValidatedEvent) -> tuple[Any, ...]:
    """Keep this explicit tuple in PARSED_FIELDS order; contract tests check it."""
    return (
        event.schema_version,
        event.producer_run_id,
        event.producer_sequence,
        to_flink_sql_timestamp(event.produced_at),
        event.event_id,
        event.tenant_id,
        event.device_id,
        event.metric_name,
        event.metric_value,
        to_flink_sql_timestamp(event.event_time),
        event.region,
        event.firmware,
        event.injected_duplicate,
        event.injected_late,
        event.event_time_ms,
    )


def sink_row_values(
    parsed_values: Sequence[Any],
    processed_at: datetime,
    watermark_at_arrival: datetime | None,
    is_late: bool,
    lateness_ms: int,
) -> tuple[Any, ...]:
    """Replace the internal millisecond timestamp with persisted observations."""
    return (
        *parsed_values[:-1],
        processed_at,
        watermark_at_arrival,
        is_late,
        lateness_ms,
    )
