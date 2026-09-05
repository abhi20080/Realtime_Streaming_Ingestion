# Follow one record from JSON to ClickHouse

Start with the [quiet baseline](LEARNING.md#1-establish-a-quiet-baseline), then
read this example alongside `flink/job.py:main`. The values below illustrate a
late record; actual run IDs and timestamps come from the producer.

## 1. Kafka carries a JSON value and a tenant key

`producer/workload.py:EventFactory` creates the event. `WorkloadGenerator`
encodes the value and uses `tenant_id` as the Kafka key. The runtime in
`producer/producer.py` sends it and records acknowledgements.

```json
{
  "schema_version": 1,
  "producer_run_id": "example-run",
  "producer_sequence": 42,
  "produced_at": "2026-08-23T12:00:05.123Z",
  "event_id": "example-event",
  "tenant_id": "tenant-001",
  "device_id": "device-0042",
  "metric_name": "temperature_c",
  "metric_value": 27.5,
  "event_time": "2026-08-23T11:59:30.000Z",
  "region": "ap-south",
  "firmware": "1.9.0",
  "injected_duplicate": false,
  "injected_late": true
}
```

`event_id` identifies the logical event. The pair `producer_run_id` and
`producer_sequence` identifies a send. An intentional duplicate keeps the event
ID and business fields but receives a new sequence and production timestamp.

## 2. Validation produces typed Python data

`flink/logic.py:validate_event` checks the payload and returns a
`ValidatedEvent`. Timestamp strings become timezone-aware UTC `datetime`
objects; flags remain booleans and the metric becomes a finite float. The event
time also has a derived millisecond representation for Flink's timestamp API.

A rejected value goes to `telemetry.dlq`, with the original payload, error
category, and context. It never reaches the main ClickHouse table.

Timestamp extraction at the Kafka source also uses validation. The later
validation operator validates again to route rows and count outcomes. Keeping
watermarks at the source lets Flink track idleness separately for Kafka splits.

## 3. The parsed Flink row follows an explicit order

`flink/row_mapping.py:event_to_row_values` builds the values in `PARSED_FIELDS`
order. `flink/job.py:PARSED_SCHEMA` supplies their Flink types. The timestamp
adapter removes timezone information only after conversion to UTC, as required
by Flink's SQL timestamp representation.

| Position (zero-based) | Field | Example / representation |
|---|---|---|
| 0 | `schema_version` | `1` |
| 1 | `producer_run_id` | `example-run` |
| 2 | `producer_sequence` | `42` |
| 3 | `produced_at` | UTC SQL timestamp, `12:00:05.123` |
| 4 | `event_id` | `example-event` |
| 5 | `tenant_id` | `tenant-001` |
| 6 | `device_id` | `device-0042` |
| 7 | `metric_name` | `temperature_c` |
| 8 | `metric_value` | `27.5` |
| 9 | `event_time` | UTC SQL timestamp, `11:59:30.000` |
| 10 | `region` | `ap-south` |
| 11 | `firmware` | `1.9.0` |
| 12 | `injected_duplicate` | `false` |
| 13 | `injected_late` | `true` |
| 14 | `event_time_ms` | Internal integer milliseconds since the Unix epoch |

The optional `keyBy` operator counts records by tenant and passes this row
through unchanged. Its managed count is not a ClickHouse column.

## 4. Observation replaces the internal field with audit values

Suppose this record arrives with a watermark of `11:59:40.000`, and Flink
processes it at `12:00:05.500`. `ObserveLatenessFunction` compares event time
with the current watermark, then `sink_row_values` constructs the destination
row. Positions 0–13 stay the same; the remaining fields are:

| Position | Field | Value |
|---|---|---|
| 14 | `flink_processed_at` | `2026-08-23 12:00:05.500` |
| 15 | `watermark_at_arrival` | `2026-08-23 11:59:40.000` |
| 16 | `is_late_at_flink` | `true` |
| 17 | `lateness_ms` | `10000` |

Before the first watermark, `watermark_at_arrival` is `NULL`, observed lateness
is false, and `lateness_ms` is zero. A record exactly at the watermark is marked
late with zero milliseconds of lateness. Late records are observed and stored;
this pipeline does not drop them or perform window aggregation.

`injected_late` describes the producer's choice. `is_late_at_flink` describes
what Flink actually observed relative to its watermark. They need not agree.

## 5. JDBC inserts 18 values; ClickHouse adds its own clock

`row_mapping.py:INSERT_SQL` derives its columns and placeholders from
`SINK_FIELDS`, in the same order as the sink row. `flink/job.py:SINK_SCHEMA`
supplies the corresponding Flink types. The JDBC adapter binds these values.
The table in `clickhouse/init.sql` supplies `clickhouse_ingested_at` using
`now64(3)`; that field is intentionally absent from the INSERT.

If ingestion happens at `12:00:06.000`, the latency views can distinguish:

- Event age: `produced_at - event_time` = 35.123 seconds.
- Kafka and Flink delay: `flink_processed_at - produced_at` = 377 milliseconds.
- Sink delay: `clickhouse_ingested_at - flink_processed_at` = 500 milliseconds.
- End-to-end ingestion delay: `clickhouse_ingested_at - produced_at` = 877 milliseconds.

The main sink is at-least-once. Recovery can insert the same send again; compare
its event ID and send identity using the [replay exercise](LEARNING.md#7-separate-intentional-duplicates-from-replay-duplicates).

## When adding a field

Update event generation and validation first, then the parsed row values and
Flink schema. For a persisted field, also update the sink contract and
ClickHouse DDL. Run the row-contract tests and live smoke check described in
[TESTING.md](../TESTING.md); keep operator UIDs and state names stable during
changes intended to preserve savepoint compatibility.
