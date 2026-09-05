#!/usr/bin/env bash
set -euo pipefail

: "${CLICKHOUSE_USER:?CLICKHOUSE_USER is required}"
: "${CLICKHOUSE_PASSWORD:?CLICKHOUSE_PASSWORD is required}"
: "${CLICKHOUSE_OBSERVER_USER:?CLICKHOUSE_OBSERVER_USER is required}"
: "${CLICKHOUSE_OBSERVER_PASSWORD:?CLICKHOUSE_OBSERVER_PASSWORD is required}"

if [[ ! "$CLICKHOUSE_OBSERVER_USER" =~ ^[A-Za-z_][A-Za-z0-9_]*$ ]]; then
  echo "CLICKHOUSE_OBSERVER_USER must be a simple identifier" >&2
  exit 2
fi

observer_identifier="\`$CLICKHOUSE_OBSERVER_USER\`"
client=(
  clickhouse-client
  --user "$CLICKHOUSE_USER"
  --password "$CLICKHOUSE_PASSWORD"
)

"${client[@]}" \
  --param_observer_password "$CLICKHOUSE_OBSERVER_PASSWORD" \
  --query "
    CREATE USER IF NOT EXISTS ${observer_identifier}
    IDENTIFIED WITH sha256_password BY {observer_password:String}
  "
"${client[@]}" --query "GRANT SELECT ON perfmon.* TO ${observer_identifier}"
"${client[@]}" --query "GRANT SELECT ON system.* TO ${observer_identifier}"
