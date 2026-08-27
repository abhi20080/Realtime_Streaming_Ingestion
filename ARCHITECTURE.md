# Architecture and Signal Model

## Data path

```text
producer
  │ keyed JSON: tenant_id
  ▼
telemetry.raw (6 Kafka partitions)
  │
  ▼
Flink KafkaSource ─▶ validate ─┬─▶ event-time + watermark inspection ─▶ JDBC ─▶ ClickHouse
                              │
                              └─▶ telemetry.dlq
```

Kafka is the durable buffer, Flink owns processing progress in checkpoint state,
and ClickHouse is the analytical destination. The lab intentionally does not
claim end-to-end exactly-once: the JDBC sink cannot atomically commit a
ClickHouse insert with a Flink checkpoint.

## Control and observation paths

```text
Kafka JMX ─────┐
Flink metrics ─┼─▶ Prometheus ─▶ Grafana
ClickHouse ────┘

ClickHouse tables/views ─────────▶ Grafana ClickHouse datasource

Docker JSON logs ─▶ Alloy ─▶ Loki ─▶ Grafana (optional profile)
```

The dashboards intentionally combine system metrics with data-level SQL:
metrics explain runtime pressure, while persisted timestamps and flags explain
what happened to individual records.

All published ports bind to `127.0.0.1`; the learning credentials and control
APIs are not intended for a shared or production network. The optional Alloy
profile mounts the Docker socket with a read-only filesystem flag, as required
for container discovery. Docker's socket API itself is privileged even through
a read-only mount, so enable that profile only on a trusted development host.

## Signal selection

| Question | Primary signal | Reason |
|---|---|---|
| Is Kafka accepting traffic? | Broker rates/errors | Cheap continuous health signal |
| Which partition is hot? | Producer ack summary + offsets | Connects keys to actual partitions |
| Is Flink falling behind? | `pendingRecords` and current offsets | Measures backlog at the source |
| Can Flink recover? | Checkpoint status/duration | Checkpoint state is recovery truth |
| Is an operator saturated? | busy/backpressured/idle time | Separates compute from downstream pressure |
| Was an event late? | watermark-at-arrival audit columns | Lateness is record- and watermark-relative |
| Why was data rejected? | DLQ record | Full replayable failure context |
| Is ClickHouse ingest healthy? | insert/parts/merge metrics | Shows storage-side pressure |
| Where is latency accumulating? | four persisted stage clocks | Separates event age, processing, and sink delay |
| What happened during a failure? | sampled structured logs | Detailed context without per-event noise |

## Cardinality rules

- Metrics and Loki labels contain only bounded dimensions such as service,
  operator, topic, partition, and error category.
- Flink reporter configuration explicitly drops runtime-generated job, task,
  attempt, operator, and TaskManager IDs; readable names and subtask indexes
  remain available for learning queries.
- Event IDs, producer run IDs, tenant IDs, devices, raw payloads, offsets, and
  exception messages stay in logs or event/DLQ data—not metric labels.
- The producer logs periodic aggregates and sampled delivery traces instead of
  logging every acknowledgement.

## Delivery semantics

- Producer acknowledgement means Kafka accepted a record; it does not mean
  Flink processed it or ClickHouse stored it.
- Flink checkpoints provide consistent source/operator recovery.
- Kafka committed offsets are monitoring progress published on completed
  checkpoints; Flink checkpoint state remains authoritative for recovery.
- Main JDBC and DLQ Kafka sinks are at-least-once. Replays are observable by
  comparing `event_id`, `producer_run_id`, and `producer_sequence`.
