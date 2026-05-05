#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT_DIR"

CANONICAL_BACKEND_HOST="smatrix-be"
CANONICAL_BACKEND_PORT="9050"

detect_python() {
  if command -v python3 >/dev/null 2>&1; then
    echo "python3"
  elif command -v python >/dev/null 2>&1; then
    echo "python"
  else
    echo "python3"
  fi
}

PYTHON_BIN="${PYTHON_BIN:-$(detect_python)}"

wait_for_container_health() {
  local service="$1"
  local attempts="${2:-120}"
  local sleep_seconds="${3:-5}"
  local status

  for ((i=1; i<=attempts; i++)); do
    status="$(docker inspect --format='{{if .State.Health}}{{.State.Health.Status}}{{else}}unknown{{end}}' "$service" 2>/dev/null || echo "missing")"
    case "$status" in
      healthy)
        return 0
        ;;
      unhealthy)
        echo "Service is unhealthy: $service" >&2
        docker compose logs --no-color "$service" >&2 || true
        return 1
        ;;
    esac
    sleep "$sleep_seconds"
  done

  echo "Timed out waiting for healthy service: $service" >&2
  return 1
}

wait_for_fe_mysql_ready() {
  local attempts="${1:-60}"
  local sleep_seconds="${2:-2}"

  for ((i=1; i<=attempts; i++)); do
    if docker compose exec -T smatrix-fe mysql -hsmatrix-fe -P9030 -uroot -e "SELECT 1;" >/dev/null 2>&1; then
      return 0
    fi
    sleep "$sleep_seconds"
  done

  echo "Timed out waiting for FE MySQL endpoint readiness (smatrix-fe:9030 inside smatrix-fe)" >&2
  return 1
}

show_backends_raw() {
  docker compose exec -T smatrix-fe mysql \
    -hsmatrix-fe -P9030 -uroot \
    --batch --skip-column-names \
    -e "SHOW BACKENDS;"
}

print_recovery_hint() {
  echo "Recovery steps:" >&2
  echo "  1) ./init.sh --reset --yes" >&2
  echo "  2) ./init.sh" >&2
}

ensure_single_canonical_backend() {
  local backend_rows="$1"
  local backend_count
  local backend_host
  local heartbeat_port
  local alive

  backend_count="$(printf '%s\n' "$backend_rows" | sed '/^\s*$/d' | wc -l | tr -d ' ')"
  if [[ "$backend_count" -gt 1 ]]; then
    echo "Detected duplicate Doris backends. Expected exactly one backend (${CANONICAL_BACKEND_HOST}:${CANONICAL_BACKEND_PORT})." >&2
    printf '%s\n' "$backend_rows" >&2
    print_recovery_hint
    return 1
  fi

  if [[ "$backend_count" -eq 0 ]]; then
    return 2
  fi

  backend_host="$(printf '%s\n' "$backend_rows" | awk -F $'\t' 'NR==1 { print $2 }')"
  heartbeat_port="$(printf '%s\n' "$backend_rows" | awk -F $'\t' 'NR==1 { print $3 }')"
  alive="$(printf '%s\n' "$backend_rows" | awk -F $'\t' 'NR==1 { print tolower($10) }')"
  if [[ "$backend_host" != "$CANONICAL_BACKEND_HOST" || "$heartbeat_port" != "$CANONICAL_BACKEND_PORT" ]]; then
    echo "Detected stale Doris backend: ${backend_host}:${heartbeat_port}. Expected ${CANONICAL_BACKEND_HOST}:${CANONICAL_BACKEND_PORT}." >&2
    printf '%s\n' "$backend_rows" >&2
    print_recovery_hint
    return 1
  fi
  if [[ "$alive" != "true" ]]; then
    echo "Canonical Doris backend is not alive: ${backend_host}:${heartbeat_port} (Alive=${alive})." >&2
    printf '%s\n' "$backend_rows" >&2
    print_recovery_hint
    return 1
  fi

  return 0
}

ensure_backend_registered() {
  local attempts="${1:-20}"
  local sleep_seconds="${2:-2}"
  local backend_rows

  for ((i=1; i<=attempts; i++)); do
    if ! backend_rows="$(show_backends_raw 2>/dev/null)"; then
      sleep "$sleep_seconds"
      continue
    fi

    if ensure_single_canonical_backend "$backend_rows"; then
      echo "Backend already canonical: ${CANONICAL_BACKEND_HOST}:${CANONICAL_BACKEND_PORT}"
      return 0
    fi

    local ensure_status=$?
    if [[ "$ensure_status" -eq 1 ]]; then
      return 1
    fi

    if docker compose exec -T smatrix-fe mysql -hsmatrix-fe -P9030 -uroot -e "ALTER SYSTEM ADD BACKEND '${CANONICAL_BACKEND_HOST}:${CANONICAL_BACKEND_PORT}';" >/dev/null 2>&1; then
      echo "Registered ${CANONICAL_BACKEND_HOST}:${CANONICAL_BACKEND_PORT}"
      sleep "$sleep_seconds"
      if backend_rows="$(show_backends_raw 2>/dev/null)" && ensure_single_canonical_backend "$backend_rows"; then
        return 0
      fi
      echo "Backend registration succeeded but canonical backend validation failed immediately after registration." >&2
      if [[ -n "${backend_rows:-}" ]]; then
        printf '%s\n' "$backend_rows" >&2
      fi
      print_recovery_hint
      return 1
    fi

    sleep "$sleep_seconds"
  done

  echo "Failed to ensure backend ${CANONICAL_BACKEND_HOST}:${CANONICAL_BACKEND_PORT} within retry window" >&2
  return 1
}

echo "========================================"
echo "  S-Matrix runtime update"
echo "========================================"
echo ""

echo "[1/4] Pulling latest code..."
if ! git pull --ff-only; then
  echo "[warn] git pull failed, continuing with the current checkout"
fi

echo ""
echo "[2/4] Rebuilding and starting services..."
docker compose up -d --build --remove-orphans smatrix-fe smatrix-be || {
  exit_code=$?
  echo "docker compose up for FE/BE returned exit code ${exit_code}; continuing with explicit health checks." >&2
}

echo ""
echo "[3/4] Enforcing Doris single-backend canonical state..."
wait_for_container_health smatrix-fe
wait_for_container_health smatrix-be
wait_for_fe_mysql_ready
ensure_backend_registered

echo ""
echo "[3.5/4] Starting API and frontend after Doris is stable..."
docker compose up -d smatrix-api smatrix-frontend

echo ""
echo "[4/4] Running runtime smoke checks..."
"$PYTHON_BIN" doris-api/dc.py smoke

echo ""
echo "========================================"
echo "  Update complete"
echo "========================================"
echo ""
docker compose ps
echo ""
echo "Access URLs:"
echo "  - Web UI:    http://localhost:35173"
echo "  - API:       http://localhost:38018"
echo "  - Health:    http://localhost:38018/api/health"
