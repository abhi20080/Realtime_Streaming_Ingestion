"""Assemble the observable Kafka -> PyFlink -> ClickHouse job.

Start with :func:`main` near the bottom to see the graph in execution order.
The per-record Python logic that does not require PyFlink lives in ``logic.py``.
"""

from __future__ import annotations

import json
import logging
import os
import random
import sys
from datetime import datetime, timezone

from pyflink.common import (
    Duration,
    Row,
    Types,
    WatermarkStrategy,
)
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream import (
    CheckpointingMode,
    ExternalizedCheckpointCleanup,
    KeyedProcessFunction,
    ProcessFunction,
    StreamExecutionEnvironment,
)
from pyflink.datastream.output_tag import OutputTag
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.datastream.connectors.jdbc import (
    JdbcConnectionOptions,
    JdbcExecutionOptions,
    JdbcSink,
)
from pyflink.datastream.connectors.kafka import (
    DeliveryGuarantee,
    KafkaOffsetsInitializer,
    KafkaRecordSerializationSchema,
    KafkaSink,
    KafkaSource,
)

from jdbc_compat import build_compatible_jdbc_sink
from row_mapping import INSERT_SQL, event_to_row_values, sink_row_values

from logic import (
    ERROR_CATEGORIES,
    PIPELINE_VERSION,
    build_dlq_record,
    calculate_lateness,
    event_time_ms_for_watermark,
    is_decimal_milestone,
    normalize_watermark_ms,
    keyby_lab_enabled,
    validate_event,
)


# Runtime configuration and row schemas.
JOB_NAME = "kafka-flink-clickhouse-monitoring"
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:19092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "telemetry.raw")
KAFKA_DLQ_TOPIC = os.getenv("KAFKA_DLQ_TOPIC", "telemetry.dlq")
KAFKA_GROUP_ID = os.getenv(
    "KAFKA_GROUP_ID", "telemetry-flink-monitoring-v1"
)
CLICKHOUSE_URL = os.getenv(
    "CLICKHOUSE_JDBC_URL", "jdbc:clickhouse://clickhouse:8123/perfmon"
)
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "flink")
CLICKHOUSE_PASSWORD = os.environ["CLICKHOUSE_PASSWORD"]
PARALLELISM = int(os.getenv("FLINK_PARALLELISM", "4"))
OUT_OF_ORDER_SECONDS = int(os.getenv("OUT_OF_ORDER_SECONDS", "10"))
WATERMARK_IDLE_SECONDS = int(os.getenv("WATERMARK_IDLE_SECONDS", "30"))
CHECKPOINT_INTERVAL_MS = int(os.getenv("CHECKPOINT_INTERVAL_MS", "10000"))
PARSE_ERROR_LOG_SAMPLE_RATE = float(
    os.getenv("PARSE_ERROR_LOG_SAMPLE_RATE", "0.05")
)
ENABLE_KEYBY_LAB = keyby_lab_enabled(os.environ)


PARSED_SCHEMA = [
    ("schema_version", Types.INT()),
    ("producer_run_id", Types.STRING()),
    ("producer_sequence", Types.LONG()),
    ("produced_at", Types.SQL_TIMESTAMP()),
    ("event_id", Types.STRING()),
    ("tenant_id", Types.STRING()),
    ("device_id", Types.STRING()),
    ("metric_name", Types.STRING()),
    ("metric_value", Types.DOUBLE()),
    ("event_time", Types.SQL_TIMESTAMP()),
    ("region", Types.STRING()),
    ("firmware", Types.STRING()),
    ("injected_duplicate", Types.BOOLEAN()),
    ("injected_late", Types.BOOLEAN()),
    ("event_time_ms", Types.LONG()),
]
PARSED_FIELDS = [name for name, _field_type in PARSED_SCHEMA]
PARSED_TYPE = Types.ROW_NAMED(
    PARSED_FIELDS,
    [field_type for _name, field_type in PARSED_SCHEMA],
)
TENANT_ID_INDEX = PARSED_FIELDS.index("tenant_id")
EVENT_TIME_MS_INDEX = PARSED_FIELDS.index("event_time_ms")
HOT_TENANT_ID = "tenant-hot"

SINK_SCHEMA = PARSED_SCHEMA[:-1] + [
    ("flink_processed_at", Types.SQL_TIMESTAMP()),
    ("watermark_at_arrival", Types.SQL_TIMESTAMP()),
    ("is_late_at_flink", Types.BOOLEAN()),
    ("lateness_ms", Types.LONG()),
]
SINK_TYPE = Types.ROW_NAMED(
    [name for name, _field_type in SINK_SCHEMA],
    [field_type for _name, field_type in SINK_SCHEMA],
)
INVALID_OUTPUT = OutputTag("invalid-telemetry", Types.STRING())


# Structured operator logging.
def configure_logging() -> logging.Logger:
    logger = logging.getLogger("telemetry-pipeline")
    logger.setLevel(os.getenv("LOG_LEVEL", "INFO").upper())
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(logging.Formatter("%(message)s"))
        logger.addHandler(handler)
    logger.propagate = False
    return logger


LOGGER = configure_logging()


def log_json(level: int, message: str, **fields) -> None:
    LOGGER.log(
        level,
        json.dumps(
            {
                "timestamp": datetime.now(timezone.utc).isoformat(
                    timespec="milliseconds"
                ).replace("+00:00", "Z"),
                "level": logging.getLevelName(level),
                "component": "flink-job",
                "message": message,
                **fields,
            },
            separators=(",", ":"),
            default=str,
        ),
    )


# Per-record Flink operators.
class ParseValidateFunction(ProcessFunction):
    """Validate records, emit valid typed rows, and side-output DLQ JSON."""

    def open(self, runtime_context) -> None:
        metric_group = runtime_context.get_metrics_group()
        self.records_seen = metric_group.counter("lab_records_seen")
        self.records_valid = metric_group.counter("lab_records_valid")
        self.records_invalid = metric_group.counter("lab_records_invalid")
        self.category_counters = {
            category: metric_group.counter(f"lab_invalid_{category}")
            for category in ERROR_CATEGORIES
        }

    def process_element(self, raw_payload, ctx):
        self.records_seen.inc()
        result = validate_event(raw_payload)
        if not result.is_valid:
            self.records_invalid.inc()
            self.category_counters[result.error_category].inc()
            if random.random() < PARSE_ERROR_LOG_SAMPLE_RATE:
                log_json(
                    logging.WARNING,
                    "invalid_record_routed_to_dlq",
                    error_category=result.error_category,
                    error_detail=result.error_detail,
                    raw_payload_length=len(raw_payload),
                    raw_payload_preview=raw_payload[:160],
                    source_topic=KAFKA_TOPIC,
                )
            yield (
                INVALID_OUTPUT,
                build_dlq_record(
                    raw_payload,
                    result.error_category,
                    result.error_detail,
                    KAFKA_TOPIC,
                ),
            )
            return

        self.records_valid.inc()
        yield Row(*event_to_row_values(result.event))


class RawEventTimestampAssigner(TimestampAssigner):
    def extract_timestamp(self, value, record_timestamp: int) -> int:
        return event_time_ms_for_watermark(value)


class TenantKeyedStateFunction(KeyedProcessFunction):
    """Count valid records per tenant while preserving the original row."""

    def open(self, runtime_context) -> None:
        self.tenant_record_count = runtime_context.get_state(
            ValueStateDescriptor("tenant-record-count", Types.LONG())
        )
        metric_group = runtime_context.get_metrics_group()
        self.keyed_records = metric_group.counter("lab_keyed_records")
        self.hot_tenant_state_count = 0
        metric_group.gauge(
            "lab_hot_tenant_state_count",
            lambda: self.hot_tenant_state_count,
        )
        self.subtask_index = runtime_context.get_index_of_this_subtask()
        self.attempt_number = runtime_context.get_attempt_number()

    def process_element(self, value, ctx):
        tenant_id = str(ctx.get_current_key())
        current_count = self.tenant_record_count.value()
        next_count = (int(current_count) if current_count is not None else 0) + 1
        self.tenant_record_count.update(next_count)
        self.keyed_records.inc()

        if tenant_id == HOT_TENANT_ID:
            # Every function instance publishes the gauge, but only the one that
            # owns tenant-hot ever reports a positive value.
            self.hot_tenant_state_count = next_count
            # Restrict logarithmic milestone logs to this one lab key. Logging
            # count=1 for every possible tenant would scale with key cardinality.
            if is_decimal_milestone(next_count):
                log_json(
                    logging.INFO,
                    "keyed_state_milestone",
                    tenant_id=tenant_id,
                    tenant_record_count=next_count,
                    state_name="tenant-record-count",
                    subtask_index=self.subtask_index,
                    attempt_number=self.attempt_number,
                )

        yield value


class ObserveLatenessFunction(ProcessFunction):
    """Capture the current watermark at arrival before writing to ClickHouse."""

    def open(self, runtime_context) -> None:
        self.records_late = runtime_context.get_metrics_group().counter(
            "lab_records_late"
        )

    def process_element(self, value, ctx):
        event_time_ms = int(value[EVENT_TIME_MS_INDEX])
        watermark_ms = normalize_watermark_ms(
            ctx.timer_service().current_watermark()
        )
        is_late, lateness_ms = calculate_lateness(event_time_ms, watermark_ms)
        if is_late:
            self.records_late.inc()

        processed_at = datetime.now(timezone.utc).replace(tzinfo=None)
        watermark_at_arrival = None
        if watermark_ms is not None:
            watermark_at_arrival = datetime.fromtimestamp(
                watermark_ms / 1_000, tz=timezone.utc
            ).replace(tzinfo=None)

        yield Row(*sink_row_values(
            value, processed_at, watermark_at_arrival, is_late, lateness_ms
        ))


# External sources and sinks.
def build_source() -> KafkaSource:
    return (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BOOTSTRAP)
        .set_topics(KAFKA_TOPIC)
        .set_group_id(KAFKA_GROUP_ID)
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .set_property("commit.offsets.on.checkpoint", "true")
        .build()
    )


def build_dlq_sink() -> KafkaSink:
    serializer = (
        KafkaRecordSerializationSchema.builder()
        .set_topic(KAFKA_DLQ_TOPIC)
        .set_value_serialization_schema(SimpleStringSchema())
        .build()
    )
    return (
        KafkaSink.builder()
        .set_bootstrap_servers(KAFKA_BOOTSTRAP)
        .set_record_serializer(serializer)
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .build()
    )


def build_clickhouse_sink() -> JdbcSink:
    connection_options = (
        JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
        .with_url(CLICKHOUSE_URL)
        .with_driver_name("com.clickhouse.jdbc.ClickHouseDriver")
        .with_user_name(CLICKHOUSE_USER)
        .with_password(CLICKHOUSE_PASSWORD)
        .build()
    )
    execution_options = (
        JdbcExecutionOptions.builder()
        .with_batch_size(int(os.getenv("JDBC_BATCH_SIZE", "1000")))
        .with_batch_interval_ms(int(os.getenv("JDBC_BATCH_INTERVAL_MS", "1000")))
        .with_max_retries(int(os.getenv("JDBC_MAX_RETRIES", "5")))
        .build()
    )
    return build_compatible_jdbc_sink(
        INSERT_SQL, SINK_TYPE, execution_options, connection_options
    )


# Job graph assembly. Read this function first.
def main() -> None:
    if PARALLELISM < 1 or OUT_OF_ORDER_SECONDS < 0 or WATERMARK_IDLE_SECONDS < 1:
        raise ValueError("parallelism/idleness must be positive; out-of-order >= 0")
    if not 0.0 <= PARSE_ERROR_LOG_SAMPLE_RATE <= 1.0:
        raise ValueError("PARSE_ERROR_LOG_SAMPLE_RATE must be between 0 and 1")

    log_json(
        logging.INFO,
        "job_starting",
        job_name=JOB_NAME,
        pipeline_version=PIPELINE_VERSION,
        kafka_bootstrap_servers=KAFKA_BOOTSTRAP,
        source_topic=KAFKA_TOPIC,
        dlq_topic=KAFKA_DLQ_TOPIC,
        consumer_group=KAFKA_GROUP_ID,
        clickhouse_jdbc_url=CLICKHOUSE_URL,
        parallelism=PARALLELISM,
        checkpoint_interval_ms=CHECKPOINT_INTERVAL_MS,
        delivery_guarantee="at_least_once",
        out_of_order_seconds=OUT_OF_ORDER_SECONDS,
        watermark_idle_seconds=WATERMARK_IDLE_SECONDS,
        parse_error_log_sample_rate=PARSE_ERROR_LOG_SAMPLE_RATE,
        keyby_lab_enabled=ENABLE_KEYBY_LAB,
    )

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(PARALLELISM)
    # Flink state/source positions are checkpointed exactly once. The JDBC
    # sink remains at-least-once because its ClickHouse transaction cannot be
    # atomically coordinated with a Flink checkpoint.
    env.enable_checkpointing(
        CHECKPOINT_INTERVAL_MS, CheckpointingMode.EXACTLY_ONCE
    )
    checkpoint_config = env.get_checkpoint_config()
    checkpoint_config.set_min_pause_between_checkpoints(2_000)
    checkpoint_config.set_checkpoint_timeout(60_000)
    checkpoint_config.set_max_concurrent_checkpoints(1)
    checkpoint_config.enable_externalized_checkpoints(
        ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
    )

    # Assign event time at the FLIP-27 source. KafkaSource can then track
    # watermark/idleness per Kafka split; doing this after parsing would only
    # track an entire downstream subtask that may own multiple partitions.
    source_watermark_strategy = (
        WatermarkStrategy.for_bounded_out_of_orderness(
            Duration.of_seconds(OUT_OF_ORDER_SECONDS)
        )
        .with_timestamp_assigner(RawEventTimestampAssigner())
        .with_idleness(Duration.of_seconds(WATERMARK_IDLE_SECONDS))
    )
    raw_stream = (
        env.from_source(
            build_source(),
            source_watermark_strategy,
            "kafka-source-telemetry-raw-with-watermarks",
        )
        .uid("kafka-source-telemetry-raw")
    )

    parsed_stream = (
        raw_stream.process(ParseValidateFunction(), output_type=PARSED_TYPE)
        .name("parse-and-validate-telemetry")
        .uid("parse-and-validate-telemetry")
    )
    invalid_stream = parsed_stream.get_side_output(INVALID_OUTPUT)
    (
        invalid_stream.sink_to(build_dlq_sink())
        .name("kafka-sink-telemetry-dlq")
        .uid("kafka-sink-telemetry-dlq")
    )

    valid_stream = parsed_stream
    if ENABLE_KEYBY_LAB:
        valid_stream = (
            parsed_stream.key_by(
                lambda value: value[TENANT_ID_INDEX],
                key_type=Types.STRING(),
            )
            .process(TenantKeyedStateFunction(), output_type=PARSED_TYPE)
            .name("count-records-by-tenant")
            .uid("count-records-by-tenant")
        )

    enriched_stream = (
        valid_stream.process(
            ObserveLatenessFunction(), output_type=SINK_TYPE
        )
        .name("observe-watermark-and-lateness")
        .uid("observe-watermark-and-lateness")
    )
    (
        enriched_stream.add_sink(build_clickhouse_sink())
        .name("clickhouse-jdbc-at-least-once-sink")
        .uid("clickhouse-jdbc-at-least-once-sink")
    )

    env.execute(JOB_NAME)


if __name__ == "__main__":
    main()
