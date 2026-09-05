SHELL := /bin/bash
COMPOSE := docker compose
UV := uv
PYTHON_DEV := $(UV) run --no-project --with pytest==8.4.2 --with PyYAML==6.0.2 --with ruff==0.16.5
CLICKHOUSE_CLIENT := $(COMPOSE) exec -T clickhouse sh -ec 'exec clickhouse-client --user "$$CLICKHOUSE_USER" --password "$$CLICKHOUSE_PASSWORD" "$$@"' sh
KEYBY_LAB := python3 scripts/keyby_lab.py
CREDENTIALS := python3 scripts/credentials.py
JMX_EXPORTER_JAR := kafka/jmx_prometheus_javaagent-1.6.0.jar
JMX_EXPORTER_URL := https://github.com/prometheus/jmx_exporter/releases/download/1.6.0/jmx_prometheus_javaagent-1.6.0.jar

.PHONY: build up up-observability up-logs init submit produce-baseline produce-small produce-observable \
	status observe lag dlq latency checkpoints logs complexity validate test smoke stop reset \
	deploy teardown stop-logs keyby-savepoint keyby-rebuild keyby-submit \
	keyby-on keyby-off flink-reset produce-keyby observe-keyby verify-keyby \
	keyby-logs credentials rotate-credentials kafka-agent help

# Compose parses every service environment, so create ignored local credentials
# before any target that invokes Compose or the keyBy control script.
build up up-observability up-logs init submit produce-baseline produce-small produce-observable \
	status observe lag dlq latency checkpoints logs validate smoke stop reset \
	deploy teardown stop-logs keyby-savepoint keyby-rebuild keyby-submit \
	keyby-on keyby-off flink-reset produce-keyby observe-keyby verify-keyby \
	keyby-logs: | .env

.env:
	@$(CREDENTIALS) generate

credentials: ## Generate strong local passwords in the ignored .env file.
	@$(CREDENTIALS) generate

rotate-credentials: ## Rotate ClickHouse/Grafana passwords without deleting volumes.
	@$(CREDENTIALS) rotate
	$(COMPOSE) up -d --force-recreate --wait --wait-timeout 180 \
		clickhouse jobmanager taskmanager grafana
	@$(MAKE) --no-print-directory submit

# Core lab lifecycle and regular workloads.
build: kafka-agent ## Build Kafka/JMX, PyFlink, and producer images.
	$(COMPOSE) --profile tools build

kafka-agent: $(JMX_EXPORTER_JAR) ## Download and cache the Kafka JMX agent on the host.

$(JMX_EXPORTER_JAR):
	@set -eu; \
		tmp=$$(mktemp "$@.tmp.XXXXXX"); \
		trap 'rm -f "$$tmp"' EXIT; \
		curl --fail --location --show-error --connect-timeout 20 --max-time 300 \
			--retry 3 --retry-delay 2 --retry-max-time 1200 \
			--output "$$tmp" "$(JMX_EXPORTER_URL)"; \
		test -s "$$tmp"; \
		mv "$$tmp" "$@"

up: ## Start the pipeline, Prometheus, and Grafana.
	$(COMPOSE) up -d kafka kafka-ui clickhouse jobmanager taskmanager prometheus grafana

up-observability: ## Start Prometheus/Grafana and required dependencies.
	$(COMPOSE) up -d prometheus grafana

up-logs: ## Start optional Loki/Alloy log collection and Grafana.
	$(COMPOSE) --profile logs up -d loki alloy grafana

init: ## Explicitly create and describe both Kafka topics.
	$(COMPOSE) run --rm kafka-init

submit: ## Submit the named PyFlink job (check for an active job first).
	$(COMPOSE) exec -T jobmanager \
		flink run -d -py /opt/flink/usrlib/job.py

produce-baseline: ## Generate 1,000 valid events with no injected anomalies.
	$(COMPOSE) --profile tools run --rm producer \
		--rate 100 --count 1000 --seed 7 \
		--duplicate-rate 0 --late-rate 0 --hot-tenant-rate 0 \
		--bad-json-rate 0 --trace-sample-rate 0

produce-small: ## Generate a short valid anomaly workload.
	$(COMPOSE) --profile tools run --rm producer \
		--rate 100 --count 10000 \
		--duplicate-rate 0.03 \
		--late-rate 0.03 \
		--hot-tenant-rate 0.70 \
		--bad-json-rate 0

produce-observable: ## Generate a large dashboard-friendly anomaly workload.
	$(COMPOSE) --profile tools run --rm producer \
		--rate 500 --count 100000 \
		--duplicate-rate 0.05 \
		--late-rate 0.10 \
		--hot-tenant-rate 0.80 \
		--bad-json-rate 0.005 \
		--trace-sample-rate 0.001

# Optional keyBy experiment. The control script owns safe savepoint transitions.
keyby-on: ## Safely switch the sole running baseline job to the keyBy pipeline.
	@$(KEYBY_LAB) keyby-on

keyby-off: ## Safely switch the sole running keyBy job to the normal pipeline.
	@$(KEYBY_LAB) keyby-off

flink-reset: ## Safely restart the current Flink job mode from a drained savepoint.
	@$(KEYBY_LAB) flink-reset

keyby-savepoint: ## Drain the one running lab job into a canonical savepoint.
	@$(KEYBY_LAB) savepoint

keyby-rebuild: ## Safely rebuild/recreate only idle Flink services; retain volumes.
	@$(KEYBY_LAB) rebuild

keyby-submit: ## Restore the keyBy-enabled job; usage: make keyby-submit SAVEPOINT=file:/...
	@test -n "$(strip $(SAVEPOINT))" || (echo "Usage: make keyby-submit SAVEPOINT=file:/opt/flink/savepoints/savepoint-..."; exit 2)
	@$(KEYBY_LAB) submit --savepoint "$(SAVEPOINT)"

produce-keyby: ## Generate 20k valid events with a 95% tenant-hot workload.
	$(COMPOSE) --profile tools run --rm producer \
		--rate 500 --count 20000 \
		--duplicate-rate 0 \
		--late-rate 0 \
		--hot-tenant-rate 0.95 \
		--bad-json-rate 0 \
		--trace-sample-rate 0

observe-keyby: ## Print the live keyBy graph, per-subtask metrics, and shuffle rates.
	@$(KEYBY_LAB) observe

verify-keyby: ## Opt in to the read-only live Chapter 8 acceptance check.
	@$(KEYBY_LAB) verify

keyby-logs: ## Print a bounded tail of keyed-state milestones.
	@$(KEYBY_LAB) logs

# Inspection and operational queries.
status: ## Show this Compose project's service status.
	$(COMPOSE) ps

observe: status ## Show endpoints, consumer lag, health, and latency.
	@echo
	@echo "Kafka Console:  http://localhost:18080"
	@echo "Flink UI:       http://localhost:18081"
	@echo "Grafana:        http://localhost:13000  (credentials in .env)"
	@echo "Prometheus:     http://localhost:19090"
	@echo "ClickHouse:     http://localhost:18123"
	@echo "Alloy (logs):   http://localhost:22345"
	@echo
	@$(MAKE) --no-print-directory lag || true
	@$(MAKE) --no-print-directory latency || true

lag: ## Show partition-level lag for the Flink consumer group.
	$(COMPOSE) exec -T -e KAFKA_OPTS= kafka \
		/opt/kafka/bin/kafka-consumer-groups.sh \
		--bootstrap-server kafka:19092 \
		--group telemetry-flink-monitoring-v1 \
		--describe

dlq: ## Read up to 20 records from the beginning of the DLQ.
	$(COMPOSE) exec -T -e KAFKA_OPTS= kafka \
		/opt/kafka/bin/kafka-console-consumer.sh \
		--bootstrap-server kafka:19092 \
		--topic telemetry.dlq \
		--from-beginning \
		--max-messages 20 \
		--timeout-ms 5000 || true

latency: ## Query pipeline health and stage-latency views.
	$(CLICKHOUSE_CLIENT) \
		--query "SELECT * FROM perfmon.v_pipeline_health FORMAT Vertical"
	$(CLICKHOUSE_CLIENT) \
		--query "SELECT * FROM perfmon.v_latency_quantiles FORMAT Vertical"

checkpoints: ## Query the Flink jobs overview endpoint.
	@curl -fsS http://localhost:18081/jobs/overview
	@echo

logs: ## Follow logs; usage: make logs SERVICE=taskmanager.
	@test -n "$(SERVICE)" || (echo "Usage: make logs SERVICE=jobmanager"; exit 2)
	$(COMPOSE) logs --tail=200 -f $(SERVICE)

# Static checks, live verification, and shutdown.
complexity: ## Enforce cyclomatic complexity of 8 or less across all Python code.
	$(PYTHON_DEV) ruff check --select C901 --ignore-noqa .

validate: complexity ## Validate complexity, Compose, YAML, provisioning, and dashboards.
	$(COMPOSE) config --quiet
	$(UV) run --no-project --with PyYAML==6.0.2 python scripts/validate_configs.py

test: ## Run the unit-test suite.
	$(PYTHON_DEV) pytest

smoke: ## Run the live anomaly/reconciliation smoke test.
	bash scripts/smoke.sh

stop: ## Stop services while preserving named volumes.
	$(COMPOSE) --profile logs down

reset: ## DESTRUCTIVE: stop services and delete this lab's volumes.
	$(COMPOSE) --profile logs down -v --remove-orphans

deploy: ## Build, start, initialize, and idempotently submit the core lab.
	@$(MAKE) --no-print-directory build
	@$(MAKE) --no-print-directory up
	@$(MAKE) --no-print-directory init
	@if $(COMPOSE) exec -T jobmanager flink list -r 2>/dev/null | grep -Fq 'kafka-flink-clickhouse-monitoring'; then \
		echo "Flink job kafka-flink-clickhouse-monitoring is already running."; \
	else \
		$(MAKE) --no-print-directory submit; \
	fi

teardown: ## Stop the entire lab while preserving named volumes.
	@$(MAKE) --no-print-directory stop

stop-logs: ## Stop Loki and Alloy while keeping the core lab running.
	$(COMPOSE) --profile logs stop alloy loki

help: ## Show available targets and their purpose.
	@awk 'BEGIN {FS = ":.*## "; printf "Usage: make <target>\n\nTargets:\n"} /^[a-zA-Z0-9_-]+:.*## / {printf "  %-22s %s\n", $$1, $$2}' $(MAKEFILE_LIST)
