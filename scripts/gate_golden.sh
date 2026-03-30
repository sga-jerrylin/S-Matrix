#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

ENV_FILE="${ENV_FILE:-$ROOT_DIR/.env}"
BASE_URL="${BASE_URL:-http://host.docker.internal:38018}"
HOST_BASE_URL="${HOST_BASE_URL:-http://localhost:38018}"
CASES_PATH="${CASES_PATH:-/app/tests/golden_queries.json}"
MIN_PASS_RATE="${GOLDEN_MIN_PASS_RATE:-0.8}"
MIN_PASSED="${GOLDEN_MIN_PASSED:-4}"
SUMMARY_FILE="${SUMMARY_FILE:-$ROOT_DIR/.reports/golden_summary.json}"

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

if [[ ! -f "$ENV_FILE" ]]; then
  echo "Missing env file: $ENV_FILE" >&2
  exit 1
fi

API_KEY="${LIVE_API_KEY:-$(read_env_value SMATRIX_API_KEY)}"
if [[ -z "$API_KEY" ]]; then
  echo "SMATRIX_API_KEY is missing in $ENV_FILE" >&2
  exit 1
fi

mkdir -p "$(dirname "$SUMMARY_FILE")"

table_registry_count="$(
  NO_PROXY=localhost,127.0.0.1 curl -sS \
    -H "X-API-Key: $API_KEY" \
    "$HOST_BASE_URL/api/table-registry" \
    | python3 -c 'import json,sys; print(json.load(sys.stdin).get("count", 0))'
)"

if [[ "$table_registry_count" == "0" ]]; then
  echo "Golden gate precondition failed: /api/table-registry is empty." >&2
  echo "Load and register at least one business table before running golden acceptance." >&2
  exit 2
fi

docker run --rm \
  -w /app \
  -e PYTHONPATH=/app \
  -v "$ROOT_DIR/doris-api:/app" \
  -v "$(dirname "$SUMMARY_FILE"):/reports" \
  smatrix-api-tdd \
  python tests/run_golden.py \
    --base-url "$BASE_URL" \
    --api-key "$API_KEY" \
    --cases "$CASES_PATH" \
    --min-pass-rate "$MIN_PASS_RATE" \
    --min-passed "$MIN_PASSED" \
    --summary-file "/reports/$(basename "$SUMMARY_FILE")" \
    --verbose
