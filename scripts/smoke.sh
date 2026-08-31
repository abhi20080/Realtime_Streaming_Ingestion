#!/usr/bin/env bash
set -euo pipefail

compose=(docker compose)
grafana_user=""
grafana_password=""
clickhouse_user=""
clickhouse_password=""
smoke_log=$(mktemp -t telemetry-monitoring-smoke.XXXXXX)
trap 'rm -f "$smoke_log"' EXIT

# Shared probes used by the linear smoke workflow below.
wait_for_http() {
  local name=$1
  local url=$2
  local attempts=${3:-90}

  for ((i = 1; i <= attempts; i++)); do
    if curl -fsS "$url" >/dev/null 2>&1; then
      echo "$name is ready"
      return 0
    fi
    sleep 2
  done

  echo "$name did not become ready: $url" >&2
  return 1
}

kafka_offset_sum() {
  local topic=$1
  "${compose[@]}" exec -T -e KAFKA_OPTS= kafka \
    /opt/kafka/bin/kafka-get-offsets.sh \
    --bootstrap-server kafka:19092 --topic "$topic" 2>/dev/null \
    | awk -F: '{total += $3} END {print total + 0}'
}

clickhouse_scalar() {
  local query=$1
  "${compose[@]}" exec -T clickhouse clickhouse-client \
    --user "$clickhouse_user" --password "$clickhouse_password" \
    --query "$query" 2>/dev/null | tr -d '[:space:]'
}

# Start the stack and capture its effective runtime configuration.
echo "Starting the monitored lab..."
"${compose[@]}" up -d kafka kafka-ui clickhouse jobmanager taskmanager prometheus grafana
"${compose[@]}" run --rm kafka-init

wait_for_http "Flink" "http://localhost:18081/overview"
wait_for_http "Prometheus" "http://localhost:19090/-/ready"
wait_for_http "Grafana" "http://localhost:13000/api/health"

# Compose reads .env itself; discover the effective in-container values so
# overrides work even when they were not exported into this host shell.
clickhouse_user=$("${compose[@]}" exec -T clickhouse sh -c 'printf %s "$CLICKHOUSE_USER"')
clickhouse_password=$("${compose[@]}" exec -T clickhouse sh -c 'printf %s "$CLICKHOUSE_PASSWORD"')
grafana_user=$("${compose[@]}" exec -T grafana sh -c 'printf %s "$GF_SECURITY_ADMIN_USER"')
grafana_password=$("${compose[@]}" exec -T grafana sh -c 'printf %s "$GF_SECURITY_ADMIN_PASSWORD"')

if ! "${compose[@]}" exec -T jobmanager flink list -r \
  | grep -q kafka-flink-clickhouse-monitoring; then
  "${compose[@]}" exec -T jobmanager flink run -d -py /opt/flink/usrlib/job.py
fi

raw_before=$(kafka_offset_sum telemetry.raw)
dlq_before=$(kafka_offset_sum telemetry.dlq)
rows_before=$(clickhouse_scalar "SELECT count() FROM perfmon.telemetry_events")

# Produce one run and reconcile it across Kafka, Flink, and ClickHouse.
echo "Producing a deterministic workload with every anomaly enabled..."
"${compose[@]}" --profile tools run --rm producer \
  --rate 200 --count 2000 --seed 20260823 \
  --duplicate-rate 0.05 \
  --late-rate 0.10 \
  --hot-tenant-rate 0.80 \
  --bad-json-rate 0.01 \
  --trace-sample-rate 0.01 | tee "$smoke_log"

read -r producer_run_id attempted acked failed undelivered reconciled < <(
  python3 - "$smoke_log" <<'PY'
import json
import sys

final = None
for line in open(sys.argv[1], encoding="utf-8"):
    try:
        candidate = json.loads(line)
    except json.JSONDecodeError:
        continue
    if candidate.get("event") == "producer_final":
        final = candidate
if final is None:
    raise SystemExit("producer_final JSON log was not found")
print(
    final["producer_run_id"], final["attempted"], final["acked"],
    final["failed"], final["undelivered"], str(final["reconciled"]).lower()
)
PY
)

if ((attempted == 0 || acked != attempted || failed != 0 || undelivered != 0)) \
  || [[ "$reconciled" != "true" ]]; then
  echo "Producer delivery accounting did not reconcile" >&2
  exit 1
fi

echo "Waiting for Flink, the DLQ, and ClickHouse to reconcile this producer run..."
run_rows=0
dlq_after=$dlq_before
raw_after=$raw_before
for ((i = 1; i <= 90; i++)); do
  raw_after=$(kafka_offset_sum telemetry.raw)
  dlq_after=$(kafka_offset_sum telemetry.dlq)
  run_rows=$(clickhouse_scalar \
    "SELECT count() FROM perfmon.telemetry_events WHERE producer_run_id = '$producer_run_id'")
  if ((raw_after - raw_before == attempted \
       && run_rows + dlq_after - dlq_before == attempted)); then
    break
  fi
  sleep 2
done

raw_delta=$((raw_after - raw_before))
dlq_delta=$((dlq_after - dlq_before))
rows_after=$(clickhouse_scalar "SELECT count() FROM perfmon.telemetry_events")
row_delta=$((rows_after - rows_before))

if ((raw_delta != attempted)); then
  echo "Kafka accepted $raw_delta records but producer acknowledged $attempted" >&2
  exit 1
fi
if ((run_rows + dlq_delta != attempted)); then
  echo "Pipeline mismatch: valid=$run_rows dlq=$dlq_delta input=$attempted" >&2
  exit 1
fi

# Verify the persisted anomaly and event-time semantics.
IFS=$'\t' read -r duplicate_rows injected_late_rows observed_late_rows hot_rows \
  watermark_rows reversed_timeline_rows < <(
  "${compose[@]}" exec -T clickhouse clickhouse-client \
    --user "$clickhouse_user" --password "$clickhouse_password" \
    --format TSVRaw --query "
      SELECT
        countIf(injected_duplicate),
        countIf(injected_late),
        countIf(is_late_at_flink),
        countIf(tenant_id = 'tenant-hot'),
        countIf(watermark_at_arrival IS NOT NULL),
        countIf(flink_processed_at < produced_at
          OR clickhouse_ingested_at < flink_processed_at)
      FROM perfmon.telemetry_events
      WHERE producer_run_id = '$producer_run_id'"
)

if ((duplicate_rows == 0 || injected_late_rows == 0 || observed_late_rows == 0 \
     || hot_rows == 0 || watermark_rows == 0)); then
  echo "Expected duplicate, late, hot-tenant, and watermark observations" >&2
  exit 1
fi
if ((reversed_timeline_rows != 0)); then
  echo "Found rows with reversed processing-stage timestamps" >&2
  exit 1
fi

latency_minutes=$(clickhouse_scalar \
  "SELECT count() FROM perfmon.v_latency_quantiles WHERE minute >= now() - INTERVAL 10 MINUTE")
if ((latency_minutes == 0)); then
  echo "Latency quantile view returned no recent data" >&2
  exit 1
fi

# Verify that every observability surface has live data.
targets_summary=""
for ((i = 1; i <= 30; i++)); do
  targets_summary=$(curl -fsS http://localhost:19090/api/v1/targets | python3 -c '
import json, sys
targets = json.load(sys.stdin)["data"]["activeTargets"]
expected = {"prometheus", "kafka", "flink-jobmanager", "flink-taskmanager", "clickhouse"}
healthy = {target["labels"].get("job") for target in targets if target["health"] == "up"}
print(len(targets), sum(target["health"] == "up" for target in targets), len(expected - healthy))
')
  read -r targets_total targets_up targets_missing <<<"$targets_summary"
  if ((targets_total == 5 && targets_up == 5 && targets_missing == 0)); then
    break
  fi
  sleep 2
done
if [[ "$targets_summary" != "5 5 0" ]]; then
  echo "Prometheus targets are not all healthy: $targets_summary" >&2
  exit 1
fi

dashboard_count=0
for ((i = 1; i <= 45; i++)); do
  dashboard_count=$(curl -fsS -u "$grafana_user:$grafana_password" \
    'http://localhost:13000/api/search?type=dash-db' \
    | python3 -c 'import json,sys; print(len(json.load(sys.stdin)))')
  if ((dashboard_count >= 4)); then
    break
  fi
  sleep 2
done
if ((dashboard_count < 4)); then
  echo "Grafana did not provision all four dashboards" >&2
  exit 1
fi

# Exercise one representative metric used by each dashboard. This catches
# syntactically valid dashboards whose metric names do not match the pinned
# exporter versions.
for dashboard in pipeline kafka flink clickhouse; do
  case "$dashboard" in
    pipeline) dashboard_query='sum(up{job=~"kafka|flink-.+|clickhouse"})' ;;
    kafka) dashboard_query='kafka_log_log_size{topic=~"telemetry\\.(raw|dlq)"}' ;;
    flink) dashboard_query='flink_taskmanager_job_task_operator_numRecordsIn' ;;
    clickhouse) dashboard_query='ClickHouseProfileEvents_InsertedRows' ;;
  esac
  result_count=$(curl -fsSG \
    --data-urlencode "query=$dashboard_query" \
    http://localhost:19090/api/v1/query \
    | python3 -c '
import json, sys
response = json.load(sys.stdin)
print(len(response.get("data", {}).get("result", [])))
')
  if ((result_count == 0)); then
    echo "Dashboard $dashboard representative query returned no data" >&2
    exit 1
  fi
done

read -r custom_metric_count volatile_label_count < <(curl -fsSG \
  --data-urlencode 'query=flink_taskmanager_job_task_operator_lab_records_valid' \
  http://localhost:19090/api/v1/query \
  | python3 -c '
import json, sys
response = json.load(sys.stdin)
result = response.get("data", {}).get("result", [])
forbidden = {
    "job_id", "task_id", "task_attempt_id", "task_attempt_num",
    "operator_id", "tm_id",
}
leaky = sum(any(label in item.get("metric", {}) for label in forbidden) for item in result)
print(len(result), leaky)
')
if ((custom_metric_count == 0)); then
  echo "Flink bounded-cardinality custom metrics returned no data" >&2
  exit 1
fi
if ((volatile_label_count != 0)); then
  echo "Flink custom metrics include volatile runtime-ID labels" >&2
  exit 1
fi

for datasource in prometheus clickhouse; do
  health=""
  for ((i = 1; i <= 30; i++)); do
    health=$(curl -fsS -u "$grafana_user:$grafana_password" \
      "http://localhost:13000/api/datasources/uid/$datasource/health" 2>/dev/null \
      | python3 -c 'import json,sys; print(json.load(sys.stdin).get("status", ""))' \
      || true)
    if [[ "$health" == "OK" ]]; then
      break
    fi
    sleep 2
  done
  if [[ "$health" != "OK" ]]; then
    echo "Grafana datasource $datasource is unhealthy: $health" >&2
    exit 1
  fi
done

echo "Smoke test passed"
echo "  producer_run_id=$producer_run_id"
echo "  input=$attempted valid=$run_rows dlq=$dlq_delta clickhouse_total_delta=$row_delta"
echo "  intentional_duplicates=$duplicate_rows injected_late=$injected_late_rows observed_late=$observed_late_rows"
echo "  prometheus_targets=5/5 grafana_dashboards=$dashboard_count dashboard_data=4/4"
