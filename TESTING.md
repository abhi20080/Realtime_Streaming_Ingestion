# Verification Guide

The lab has three verification levels. Run them in order so configuration
errors are separated from runtime and failure-recovery behavior.

## 1. Static and unit checks

```bash
make validate
make test
```

These checks parse Compose, Kafka JMX Exporter, Loki, Prometheus, Grafana
provisioning, and dashboard assets, then exercise event generation, anomaly
flags, sequencing, producer statistics, bounded validation errors, DLQ
envelopes, and lateness arithmetic.

## 2. Live reconciliation smoke test

Do not run another producer at the same time.

```bash
make build
make smoke
```

The deterministic workload contains valid, duplicate, late, hot-tenant, and
malformed records. The script asserts all of the following:

- producer attempts equal Kafka acknowledgements and raw-topic offset growth;
- valid ClickHouse rows plus DLQ offset growth equal Kafka input;
- intentional duplicate, injected-late, observed-late, and hot-tenant rows exist;
- processing-stage timestamps are ordered and watermark observations exist;
- the latency view has recent quantiles;
- all five Prometheus targets are healthy;
- Flink custom metrics are present without volatile runtime-ID labels;
- both required Grafana datasources are healthy, four dashboards exist, and a
  representative live query used by each dashboard returns data.

Inspect the per-run identifier printed at the end if an assertion fails. The
same identifier appears in producer logs and every valid ClickHouse row.

## 3. Recovery tests

These tests intentionally interrupt services. Keep Grafana, the Flink UI,
`make lag`, and service logs visible while they run.

### TaskManager recovery

1. Start an unbounded producer using the command in Exercise 5 of
   [LEARNING.md](LEARNING.md).
2. Wait for at least two successful checkpoints.
3. Run `docker compose kill taskmanager`, then
   `docker compose up -d taskmanager`.
4. Confirm a restart, temporary lag growth, catch-up, and a new completed
   checkpoint in the Flink UI.
5. Compare `total_rows`, `unique_event_ids`, and duplicate sequence numbers in
   `perfmon.v_pipeline_health` and Exercise 7's query.

### ClickHouse outage and backpressure

1. Keep an unbounded producer running.
2. Run `docker compose stop clickhouse` for 30–60 seconds.
3. Confirm JDBC retries, rising pending records/lag, checkpoint pressure, and
   backpressured time.
4. Run `docker compose up -d clickhouse` and confirm the backlog drains and
   ClickHouse insert/part activity spikes before settling.

### Sibling-project isolation

From the original repository and this repository, start both Compose projects:

```bash
(cd ../kafka-flink-clickhouse-lab && docker compose up -d)
docker compose up -d
docker ps --format 'table {{.Names}}\t{{.Ports}}'
```

Confirm both Kafka consoles, Flink UIs, ClickHouse endpoints, and this lab's
Prometheus/Grafana endpoints are reachable. Compose project names, host ports,
networks, container names, and volumes must be distinct.

### Reproducibility from empty state

`make reset` removes only this Compose project's containers and volumes. Then
repeat the full build and smoke test:

```bash
make reset
make build
make smoke
```

The second smoke result should reconcile independently of the first run.
