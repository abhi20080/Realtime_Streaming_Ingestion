# Kafka → PyFlink → ClickHouse Monitoring Lab

A local, learning-first streaming system where the data path and the operational
signals are equally important:

```text
                               ┌──────────── Prometheus ────────────┐
                               │ Kafka · Flink · ClickHouse metrics │
                               └────────────────┬───────────────────┘
                                                │
Python producer ──▶ Kafka ──▶ PyFlink ──▶ ClickHouse ──▶ Grafana
                         │          │              │          ▲
                         │          └─▶ Kafka DLQ  └── SQL ───┘
                         │
                         └── structured container logs ──▶ Alloy ──▶ Loki
                                                              (optional)
```

The project deliberately generates duplicates, late events, partition skew,
and malformed JSON. It then gives you enough timestamps, metrics, logs, and
queries to explain exactly what each anomaly did.

This is a standalone sibling of the simpler baseline lab. It uses different
host ports, its own Compose project, and independent volumes, so both projects
can run at the same time.

## How to read this repository

Start with the executable entry points near the bottom of each Python file.
They show the runtime order; the classes and helpers above them support that
order.

```text
producer/producer.py
  main → run → WorkloadGenerator.next_batch → Kafka telemetry.raw

flink/job.py
  main → KafkaSource → ParseValidateFunction
                       ├─ invalid → Kafka telemetry.dlq
                       └─ valid → optional TenantKeyedStateFunction
                                  → ObserveLatenessFunction → ClickHouse
```

Use this map to keep record processing, lab control, and verification separate:

| Path | Start here | Responsibility |
|---|---|---|
| Record processing | [`producer/producer.py`](producer/producer.py), then [`flink/job.py`](flink/job.py) | Generate records and assemble the live Flink graph. |
| Pure transformation logic | [`flink/logic.py`](flink/logic.py) | Validate payloads, calculate event-time results, and build DLQ records without requiring PyFlink. |
| Lab control | [`Makefile`](Makefile), then [`scripts/keyby_lab.py`](scripts/keyby_lab.py) | Start the lab and switch or inspect the optional `keyBy` experiment. This code never processes records. |
| Storage model | [`clickhouse/init.sql`](clickhouse/init.sql) | Define the destination table and the SQL views used by Grafana. |
| Verification | [`scripts/smoke.sh`](scripts/smoke.sh), then [`tests/`](tests/) | Reconcile one live run end to end and check isolated pure logic/configuration. |

[`ARCHITECTURE.md`](ARCHITECTURE.md) explains why the boundaries and delivery
semantics exist. [`TESTING.md`](TESTING.md) explains the three verification
levels and which checks mutate the running lab.


## Requirements

- Docker Desktop or Docker Engine with Docker Compose v2.
- Python 3.11+ for the host-side smoke test; add
  [uv](https://docs.astral.sh/uv/) for validation and unit tests. The normal
  runtime commands themselves only need Docker.
- Approximately 8–10 GB of Docker memory for the full metrics stack. Loki and
  Alloy are optional.

The Flink containers run as `linux/amd64`. PyFlink 1.20.5 does not publish the
required Linux ARM64 wheel, so Docker Desktop uses its compatibility layer on
Apple Silicon.

## Quick start

```bash
cp .env.example .env
make deploy
make produce-small
make observe
```

Open:

| Interface | URL | Purpose |
|---|---|---|
| Kafka Console | http://localhost:18080 | Topics, keys, partitions, offsets, consumer groups |
| Flink UI | http://localhost:18081 | Job graph, checkpoints, backpressure, exceptions |
| Grafana | http://localhost:13000 | Provisioned dashboards; default `admin` / `admin` |
| Prometheus | http://localhost:19090 | Raw metrics, targets, PromQL |
| ClickHouse HTTP | http://localhost:18123 | Server HTTP endpoint |

Grafana is provisioned with Prometheus and a read-only ClickHouse user. The
first startup can take an extra minute while Grafana installs the pinned
ClickHouse datasource plugin.

## Generate an observable workload

```bash
make produce-observable
```

That sends 100,000 original events plus roughly 5,000 duplicate sends with:

- 5% intentional duplicates;
- 10% intentionally backdated event times;
- 80% traffic for `tenant-hot`;
- 0.5% malformed JSON;
- sampled Kafka delivery traces.

Every producer run has a UUID and monotonically increasing sequence number.
Duplicates preserve `event_id` and the business fields but receive a new
sequence number and `produced_at`, making intentional duplicates distinguishable
from recovery duplicates.

## Event timeline

Each valid ClickHouse row includes four clocks:

| Column | Meaning |
|---|---|
| `event_time` | Simulated device/source time; may intentionally be old |
| `produced_at` | When the producer attempted this send |
| `flink_processed_at` | When the record passed Flink validation/lateness inspection |
| `clickhouse_ingested_at` | ClickHouse server time when the row was inserted |

The derived views separate:

- event age: `produced_at - event_time`;
- Kafka + Flink delay: `flink_processed_at - produced_at`;
- sink delay: `clickhouse_ingested_at - flink_processed_at`;
- end-to-end ingestion delay: `clickhouse_ingested_at - produced_at`.

Run the same queries Grafana uses:

```bash
make latency
```

## Invalid records and the DLQ

Flink classifies invalid records using a bounded set of categories:

- `invalid_json`
- `missing_field`
- `invalid_type`
- `invalid_timestamp`

Invalid records do not enter the main ClickHouse table. They are written to the
explicitly created `telemetry.dlq` topic with their raw payload and failure
context:

```bash
make dlq
```

## Logs

The producer and Python operators emit one-line JSON. Normal operation logs
summaries rather than every event; `--trace-sample-rate` enables sampled Kafka
acknowledgements with partition and offset. Producer summaries include queue
depth, delivery latency, partition counts, offset ranges, failures,
undelivered records, and librdkafka transmit retries.

Follow a service directly:

```bash
make logs SERVICE=taskmanager
make logs SERVICE=kafka
```

To collect all Compose logs in Loki:

```bash
make up-logs
```

Then open Grafana Explore or Grafana's Logs Drilldown. Alloy labels only stable
container/service fields. Event IDs, payloads, tenants, and devices remain JSON
fields instead of labels to avoid high-cardinality indexes.

## Metrics sources

Prometheus scrapes every five seconds:

- Kafka JMX Exporter at http://localhost:19404/metrics
- Flink JobManager at http://localhost:19249/metrics
- Flink TaskManager at http://localhost:19250/metrics
- ClickHouse at http://localhost:19363/metrics
- Prometheus itself

Useful Flink signals include current and committed Kafka offsets, pending
records, source event-time lag, operator records in/out, busy/backpressured
time, checkpoint duration/failures, and restarts. Custom `lab_*` metrics show
validation and observed lateness.

## Common commands

```bash
make help                   # List every supported target and its purpose
make deploy                 # Build, start, initialize, and submit the core lab
make build                  # Build Kafka/JMX, Flink, and producer images
make up                     # Start the core pipeline and metrics UI services
make up-observability       # Start Prometheus/Grafana and required dependencies
make up-logs                # Start optional Loki/Alloy log collection
make stop-logs              # Stop Loki/Alloy while keeping the core lab running
make init                   # Explicitly create telemetry.raw and telemetry.dlq
make submit                 # Submit the named PyFlink streaming job
make produce-small          # Run a short anomaly workload
make produce-observable     # Run a longer dashboard-friendly workload
make keyby-on               # Switch the running job to the optional keyBy graph
make produce-keyby          # Generate the controlled hot-tenant workload
make observe-keyby          # Print graph, state, and shuffle evidence
make verify-keyby           # Check the live keyBy experiment without changing it
make keyby-off              # Return to the normal graph from a savepoint
make status                 # Compose service status
make lag                    # Partition-level Flink consumer lag
make checkpoints            # Flink REST job overview
make validate               # Compose, YAML, and dashboard syntax
make test                   # Fast unit tests
make smoke                  # Build-independent live integration smoke test
make teardown               # Stop everything and preserve named volumes
make stop                   # Stop services and preserve volumes
make reset                  # Delete this lab's containers and volumes
```

`make deploy` is safe to run again: topic creation is idempotent and it skips
Flink submission when `kafka-flink-clickhouse-monitoring` is already running.
Optional Loki/Alloy collection remains a separate `make up-logs` choice.
Use `make stop-logs` to reverse that choice without stopping Grafana or the
core pipeline; the Loki/Alloy containers and named volumes are preserved for a
later restart.
`make teardown` stops the entire Compose project but preserves its named data
volumes; use `make reset` only when you intentionally want a clean slate.

`make reset` cannot affect the baseline lab because the Compose projects and
volumes are independently named.

Run `make smoke` without another producer writing concurrently. It uses a
deterministic anomaly workload and verifies producer acknowledgements, raw/DLQ
offsets, per-run ClickHouse counts, stage timestamps, latency views, all five
Prometheus targets, both Grafana datasource health checks, and all four
provisioned dashboards, including a representative live query from each.

## Version pins

| Component | Version |
|---|---:|
| Apache Kafka | 3.9.2 |
| Redpanda Console | 3.10.0 |
| Apache Flink / PyFlink | 1.20.5 |
| Kafka connector | 3.4.0-1.20 |
| JDBC connector | 3.3.0-1.20 |
| ClickHouse JDBC | 0.9.7 |
| ClickHouse | 25.8 |
| Prometheus | 3.14.0 |
| Grafana | 13.1.3 |
| Grafana ClickHouse datasource | 4.20.0 |
| JMX Exporter | 1.6.0 |
| Loki | 3.7.3 |
| Alloy | 1.18.1 |

## Troubleshooting

If the Flink build fails on Apple Silicon, confirm Docker Desktop's amd64
emulation is enabled and rebuild:

```bash
docker compose build --no-cache jobmanager
```

If a dashboard is empty, inspect Prometheus targets first:

```text
http://localhost:19090/targets
```

For application data panels, confirm ClickHouse has rows and Grafana's
`ClickHouse` datasource health check succeeds:

```bash
docker compose exec clickhouse clickhouse-client \
  --user flink --password flink \
  --query 'SELECT count() FROM perfmon.telemetry_events'
```

If the job is running but committed offsets look old, remember that Flink only
commits Kafka offsets after completed checkpoints. Its checkpoint state—not the
broker's committed offset—is the source of truth for recovery.
