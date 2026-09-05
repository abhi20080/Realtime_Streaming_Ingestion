"""Check boundaries between real sample data, Python rows, Flink, and SQL."""

import ast
import json
import re
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path

from logic import calculate_lateness, validate_event
from row_mapping import (
    INSERT_SQL, PARSED_FIELDS, SINK_FIELDS, event_to_row_values, sink_row_values,
)

ROOT = Path(__file__).resolve().parents[1]


def example_event():
    guide = (ROOT / "docs/EVENT_FLOW.md").read_text()
    payload = guide.split("```json\n", 1)[1].split("```", 1)[0]
    result = validate_event(payload)
    assert result.is_valid, result.error_detail
    return result.event


def test_documented_event_maps_every_value_to_its_named_column():
    event = example_event()
    values = event_to_row_values(event)
    actual = dict(zip(PARSED_FIELDS, values, strict=True))
    expected = asdict(event)
    expected.update(
        produced_at=datetime(2026, 8, 23, 12, 0, 5, 123000),
        event_time=datetime(2026, 8, 23, 11, 59, 30),
        event_time_ms=int(event.event_time.timestamp() * 1000),
    )
    assert actual == expected
    assert event.produced_at.tzinfo == timezone.utc
    assert actual["produced_at"].tzinfo is None


def test_non_utc_input_is_normalized_before_row_conversion():
    payload = asdict(example_event())
    payload["produced_at"] = "2026-08-23T17:30:05.123+05:30"
    payload["event_time"] = "2026-08-23T17:29:30+05:30"
    event = validate_event(json.dumps(payload)).event
    assert event_to_row_values(event) == event_to_row_values(example_event())


def test_observation_replaces_internal_timestamp_and_matches_insert_order():
    event = example_event()
    watermark = datetime(2026, 8, 23, 11, 59, 40)
    watermark_ms = int(watermark.replace(tzinfo=timezone.utc).timestamp() * 1000)
    late, lateness = calculate_lateness(event.event_time_ms, watermark_ms)
    processed_at = datetime(2026, 8, 23, 12, 0, 5, 500000)
    values = sink_row_values(event_to_row_values(event), processed_at, watermark, late, lateness)
    columns = tuple(INSERT_SQL.split("(", 1)[1].split(")", 1)[0].split(", "))
    actual = dict(zip(columns, values, strict=True))
    expected = asdict(event)
    expected.update(
        produced_at=event.produced_at.replace(tzinfo=None),
        event_time=event.event_time.replace(tzinfo=None),
        flink_processed_at=processed_at,
        watermark_at_arrival=watermark,
        is_late_at_flink=True,
        lateness_ms=10_000,
    )
    assert actual == expected
    assert columns == SINK_FIELDS
    assert INSERT_SQL.count("?") == len(values)


def test_before_first_watermark_persists_null_and_zero_lateness():
    event = example_event()
    late, lateness = calculate_lateness(event.event_time_ms, None)
    values = sink_row_values(event_to_row_values(event), datetime(2026, 8, 23, 12), None, late, lateness)
    assert values[-3:] == (None, False, 0)


def test_insert_columns_match_clickhouse_ddl_except_server_generated_clock():
    ddl = (ROOT / "clickhouse/init.sql").read_text()
    table = ddl.split("CREATE TABLE IF NOT EXISTS perfmon.telemetry_events", 1)[1]
    table = table.split("ENGINE =", 1)[0]
    columns = tuple(re.findall(r"^    (\w+)\s+\w+", table, flags=re.MULTILINE))
    assert columns == (*SINK_FIELDS, "clickhouse_ingested_at")
    assert "clickhouse_ingested_at DateTime64(3, 'UTC') DEFAULT now64(3)" in table


def test_flink_schema_names_follow_the_row_contract():
    # Inspect schema declarations structurally; the host need not install PyFlink.
    tree = ast.parse((ROOT / "flink/job.py").read_text())
    assignments = {
        node.targets[0].id: node.value for node in tree.body
        if isinstance(node, ast.Assign) and isinstance(node.targets[0], ast.Name)
    }
    parsed = tuple(ast.literal_eval(item.elts[0]) for item in assignments["PARSED_SCHEMA"].elts)
    observations = assignments["SINK_SCHEMA"].right
    sink = parsed[:-1] + tuple(ast.literal_eval(item.elts[0]) for item in observations.elts)
    assert parsed == PARSED_FIELDS
    assert sink == SINK_FIELDS
