# Operations Reference

Start with the [quick start](../README.md#quick-start). This page covers routine
commands and operational behavior; [guided experiments](LEARNING.md) teach the concepts.

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
make produce-baseline       # Run 1,000 valid events without injected anomalies
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
make credentials            # Generate local credentials in .env
make rotate-credentials     # Rotate ClickHouse and Grafana credentials
make complexity             # Enforce Python cyclomatic complexity <= 8
make validate               # Complexity, Compose, YAML, and dashboard checks
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

