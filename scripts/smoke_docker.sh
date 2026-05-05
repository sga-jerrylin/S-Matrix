#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

SKIP_UP=false

for arg in "$@"; do
  case "$arg" in
    --skip-up|--no-up)
      SKIP_UP=true
      ;;
    *)
      echo "Unknown argument: $arg" >&2
      exit 2
      ;;
  esac
done

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

if [[ "$SKIP_UP" == true ]]; then
  exec "$PYTHON_BIN" doris-api/dc.py smoke
fi

exec "$PYTHON_BIN" doris-api/dc.py smoke --compose-up
