#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

ENV_FILE="${ENV_FILE:-$ROOT_DIR/.env}"
BASE_URL="${BASE_URL:-http://localhost:38018}"
FRONTEND_URL="${FRONTEND_URL:-http://localhost:35173}"
FE_BOOTSTRAP_URL="${FE_BOOTSTRAP_URL:-http://localhost:38030/api/bootstrap}"
BE_HEALTH_URL="${BE_HEALTH_URL:-http://localhost:38040/api/health}"
NO_PROXY_VALUE="${NO_PROXY:-localhost,127.0.0.1}"

read_env_value() {
  python3 - "$ENV_FILE" "$1" <<'PY'
from pathlib import Path
import sys

env_path = Path(sys.argv[1])
target = sys.argv[2]

if not env_path.exists():
    raise SystemExit(1)

for raw_line in env_path.read_text(encoding="utf-8").splitlines():
    line = raw_line.strip()
    if not line or line.startswith("#") or "=" not in line:
        continue
    key, value = line.split("=", 1)
    if key.strip() == target:
        print(value.strip().strip("'").strip('"'))
        raise SystemExit(0)

raise SystemExit(1)
PY
}

wait_for_url() {
  local url="$1"
  local expected_status="${2:-200}"
  local attempts="${3:-60}"
  local sleep_seconds="${4:-2}"
  local body_file
  body_file="$(mktemp)"
  trap 'rm -f "$body_file"' RETURN

  for ((i=1; i<=attempts; i++)); do
    local status
    status="$(NO_PROXY="$NO_PROXY_VALUE" curl -sS -o "$body_file" -w "%{http_code}" "$url" || true)"
    if [[ "$status" == "$expected_status" ]]; then
      return 0
    fi
    sleep "$sleep_seconds"
  done

  echo "URL check failed: $url expected $expected_status" >&2
  cat "$body_file" >&2 || true
  return 1
}

assert_status() {
  local url="$1"
  local expected_status="$2"
  shift 2
  local body_file
  body_file="$(mktemp)"
  trap 'rm -f "$body_file"' RETURN
  local status
  status="$(NO_PROXY="$NO_PROXY_VALUE" curl -sS -o "$body_file" -w "%{http_code}" "$@" "$url" || true)"
  if [[ "$status" != "$expected_status" ]]; then
    echo "Unexpected status for $url: got $status expected $expected_status" >&2
    cat "$body_file" >&2 || true
    return 1
  fi
}

if [[ ! -f "$ENV_FILE" ]]; then
  echo "Missing env file: $ENV_FILE" >&2
  exit 1
fi

SMATRIX_API_KEY="$(read_env_value SMATRIX_API_KEY)"
if [[ -z "$SMATRIX_API_KEY" ]]; then
  echo "SMATRIX_API_KEY is missing in $ENV_FILE" >&2
  exit 1
fi

echo "==> Reconciling docker compose services"
docker compose up -d

echo "==> Waiting for FE / BE / API / Frontend"
wait_for_url "$FE_BOOTSTRAP_URL" 200
wait_for_url "$BE_HEALTH_URL" 200
wait_for_url "$BASE_URL/api/health" 200
wait_for_url "$FRONTEND_URL" 200

echo "==> Verifying auth behavior"
assert_status "$BASE_URL/api/query/history" 401
assert_status "$BASE_URL/api/query/history" 200 -H "X-API-Key: $SMATRIX_API_KEY"

echo "==> Verifying required system tables"
docker exec smatrix-api python - <<'PY'
from db import doris_client

required = {
    "_sys_datasources",
    "_sys_sync_tasks",
    "_sys_table_metadata",
    "_sys_table_registry",
    "_sys_query_history",
    "_sys_table_agents",
    "_sys_field_catalog",
    "_sys_table_relationships",
}
rows = doris_client.execute_query("SHOW TABLES")
tables = set()
for row in rows:
    tables.update(row.values())
missing = sorted(required - tables)
if missing:
    raise SystemExit(f"missing system tables: {missing}")
print("system tables ready")
PY

echo "==> docker compose ps"
docker compose ps

echo "Smoke docker checks passed"
