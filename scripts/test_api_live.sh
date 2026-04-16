#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

ENV_FILE="${ENV_FILE:-$ROOT_DIR/.env}"
LIVE_BASE_URL="${LIVE_BASE_URL:-http://host.docker.internal:38018}"

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

LIVE_API_KEY="${LIVE_API_KEY:-$(read_env_value SMATRIX_API_KEY)}"
if [[ -z "$LIVE_API_KEY" ]]; then
  echo "SMATRIX_API_KEY is missing in $ENV_FILE" >&2
  exit 1
fi

docker run --rm \
  -w /app \
  -e PYTHONPATH=/app \
  -e RUN_LIVE_INTEGRATION=1 \
  -e LIVE_BASE_URL="$LIVE_BASE_URL" \
  -e LIVE_API_KEY="$LIVE_API_KEY" \
  -e RUN_LIVE_NATURAL_QUERY="${RUN_LIVE_NATURAL_QUERY:-0}" \
  -e LIVE_NATURAL_QUERY="${LIVE_NATURAL_QUERY:-广州有多少机构？}" \
  -v "$ROOT_DIR/doris-api:/app" \
  smatrix-api-tdd \
  pytest tests/test_live_api_integration.py -q
