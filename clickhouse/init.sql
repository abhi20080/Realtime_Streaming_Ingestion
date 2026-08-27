CREATE DATABASE IF NOT EXISTS perfmon;

CREATE TABLE IF NOT EXISTS perfmon.telemetry_events
(
    schema_version UInt16,
    producer_run_id String,
    producer_sequence UInt64,
    produced_at DateTime64(3, 'UTC'),
    event_id String,
    tenant_id LowCardinality(String),
    device_id String,
    metric_name LowCardinality(String),
    metric_value Float64,
    event_time DateTime64(3, 'UTC'),
    region LowCardinality(String),
    firmware LowCardinality(String),
    injected_duplicate Bool,
    injected_late Bool,
    flink_processed_at DateTime64(3, 'UTC'),
    watermark_at_arrival Nullable(DateTime64(3, 'UTC')),
    is_late_at_flink Bool,
    lateness_ms UInt64,
    clickhouse_ingested_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_time)
ORDER BY
(
    tenant_id,
    metric_name,
    event_time,
    device_id,
    event_id,
    producer_run_id,
    producer_sequence
);

CREATE VIEW IF NOT EXISTS perfmon.v_pipeline_health AS
SELECT
    count() AS total_rows,
    uniqExact(event_id) AS unique_event_ids,
    count() - uniqExact(event_id) AS duplicate_rows,
    min(event_time) AS earliest_event_time,
    max(event_time) AS latest_event_time,
    max(produced_at) AS latest_produced_at,
    max(flink_processed_at) AS latest_flink_processed_at,
    max(clickhouse_ingested_at) AS latest_clickhouse_ingested_at,
    countIf(injected_late) AS injected_late_rows,
    countIf(is_late_at_flink) AS observed_late_rows
FROM perfmon.telemetry_events;

CREATE VIEW IF NOT EXISTS perfmon.v_throughput_per_minute AS
SELECT
    toStartOfMinute(clickhouse_ingested_at) AS minute,
    count() AS rows_received,
    uniqExact(event_id) AS unique_event_ids,
    count() - uniqExact(event_id) AS duplicate_rows,
    countIf(injected_late) AS injected_late_rows,
    countIf(is_late_at_flink) AS observed_late_rows
FROM perfmon.telemetry_events
GROUP BY minute;

CREATE VIEW IF NOT EXISTS perfmon.v_latency_quantiles AS
SELECT
    toStartOfMinute(clickhouse_ingested_at) AS minute,
    quantileTDigest(0.50)(dateDiff('millisecond', event_time, produced_at))
        AS event_age_p50_ms,
    quantileTDigest(0.95)(dateDiff('millisecond', event_time, produced_at))
        AS event_age_p95_ms,
    quantileTDigest(0.99)(dateDiff('millisecond', event_time, produced_at))
        AS event_age_p99_ms,
    quantileTDigest(0.50)(dateDiff('millisecond', produced_at, flink_processed_at))
        AS kafka_flink_p50_ms,
    quantileTDigest(0.95)(dateDiff('millisecond', produced_at, flink_processed_at))
        AS kafka_flink_p95_ms,
    quantileTDigest(0.99)(dateDiff('millisecond', produced_at, flink_processed_at))
        AS kafka_flink_p99_ms,
    quantileTDigest(0.50)(dateDiff('millisecond', flink_processed_at, clickhouse_ingested_at))
        AS sink_p50_ms,
    quantileTDigest(0.95)(dateDiff('millisecond', flink_processed_at, clickhouse_ingested_at))
        AS sink_p95_ms,
    quantileTDigest(0.99)(dateDiff('millisecond', flink_processed_at, clickhouse_ingested_at))
        AS sink_p99_ms,
    quantileTDigest(0.50)(dateDiff('millisecond', produced_at, clickhouse_ingested_at))
        AS end_to_end_p50_ms,
    quantileTDigest(0.95)(dateDiff('millisecond', produced_at, clickhouse_ingested_at))
        AS end_to_end_p95_ms,
    quantileTDigest(0.99)(dateDiff('millisecond', produced_at, clickhouse_ingested_at))
        AS end_to_end_p99_ms
FROM perfmon.telemetry_events
GROUP BY minute;

CREATE VIEW IF NOT EXISTS perfmon.v_data_quality AS
SELECT
    count() AS total_rows,
    uniqExact(event_id) AS unique_event_ids,
    count() - uniqExact(event_id) AS duplicate_rows,
    countIf(injected_duplicate) AS injected_duplicate_rows,
    countIf(injected_late) AS injected_late_rows,
    countIf(is_late_at_flink) AS observed_late_rows,
    countIf(injected_late AND is_late_at_flink) AS injected_and_observed_late_rows,
    countIf(injected_late AND NOT is_late_at_flink) AS injected_not_observed_late_rows,
    countIf(NOT injected_late AND is_late_at_flink) AS unexpected_observed_late_rows
FROM perfmon.telemetry_events;

CREATE USER IF NOT EXISTS observer
IDENTIFIED WITH sha256_password BY 'observer';

GRANT SELECT ON perfmon.* TO observer;
GRANT SELECT ON system.* TO observer;
