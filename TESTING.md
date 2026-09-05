# Verification Guide

The lab has three verification levels. Run them in order so configuration
errors are separated from runtime and failure-recovery behavior.

## 1. Static and unit checks

```bash
make credentials
make complexity
make validate
make test
```

The complexity gate runs Ruff C901 across all Python code, including tests, and
rejects any function or method above 8 without honoring inline suppressions.
`make validate` includes that gate, then parses Compose, Kafka JMX Exporter,
Loki, Prometheus, Grafana provisioning, and dashboard assets. The unit tests
exercise event generation, anomaly flags, sequencing, producer lifecycle and
statistics, bounded validation errors, DLQ envelopes, and lateness arithmetic. Row-contract tests also check the complete mapping
from a validated event through Flink row order to JDBC columns and ClickHouse
DDL. Command tests exercise Make targets using a fake Docker executable, so
refactoring their implementation does not require Docker or a running lab.

## 2. Live reconciliation smoke test

The Flink image build first constructs both pipeline graphs against the actual
PyFlink and Java connector jars, without contacting Kafka or ClickHouse. This
checks the JDBC Sink V2 bridge, serializers, and checkpoint-retention API that
the host's pure-Python tests cannot exercise. Run it independently with:

```bash
docker compose run --rm --no-deps jobmanager python /opt/flink/usrlib/check_runtime.py
```

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

1. Start an unbounded producer in a separate terminal:

   ```bash
   docker compose --profile tools run --rm producer \
     --rate 1000 --count 0 \
     --duplicate-rate 0 --late-rate 0 --bad-json-rate 0
   ```

2. Wait for at least two successful checkpoints.
3. Run `docker compose kill taskmanager`, then
   `docker compose up -d taskmanager`.
4. Confirm a restart, temporary lag growth, catch-up, and a new completed
   checkpoint in the Flink UI.
5. Compare `total_rows`, `unique_event_ids`, and duplicate sequence numbers in
   `perfmon.v_pipeline_health` and the
   [replay comparison query](docs/LEARNING.md#7-separate-intentional-duplicates-from-replay-duplicates).

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

## Upgrade validation (2026-09-05)

The pinned Flink 2.2.1 / Kafka 4.3.1 / ClickHouse 26.3.30.9 stack was tested
from empty project volumes on Docker Desktop, using amd64 Flink emulation.

| Check | Observed result |
|---|---|
| Unit tests, complexity, configuration | 93 tests passed; all static checks passed |
| JVM graph construction | Baseline and keyBy graphs passed during image build |
| Fresh-stack smoke | 2,097 inputs = 2,076 valid rows + 21 DLQ records |
| Monitoring | 5/5 targets, 4/4 dashboards, both required datasource health checks passed |
| Keyed workload | 20,000 rows; tenant-hot state matched 18,978 rows |
| Keyed savepoint restoration | 1,000 additional tenant-hot events advanced saved state to 19,978 |
| TaskManager crash during production | All 12,000 unique records arrived; 12,002 rows including replay; checkpoint restored |
| ClickHouse outage (35 seconds) | All 12,000 unique records arrived; 12,000 rows; checkpoints resumed |
| Optional logs | Native Loki/Alloy configuration checks passed; live project logs queried from Loki |
| Return to baseline | keyBy-off restored successfully; final smoke reconciled 2,097 inputs again; four tasks running |

Flink 2.2.1 emits repeated `pendingCommittables` metric-registration warnings
for Sink V2 committers. Its
[committable collector](https://github.com/apache/flink/blob/release-2.2.1/flink-runtime/src/main/java/org/apache/flink/streaming/runtime/operators/sink/committables/CommittableCollector.java)
registers the gauge again when copying checkpoint state. The original metric
remained exposed, and reconciliation, recovery, and the lab's dashboard checks
passed. This upstream logging issue remains visible; it has not been suppressed
or patched in the Flink distribution. These results establish the tested lab
scenarios, not compatibility with old checkpoints or every upstream feature.
