#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
ENV_FILE="${IPO_DATA_BATCH_ENV:-"$ROOT_DIR/scripts/local/ipo_data_batch.env"}"
if [[ -f "$ENV_FILE" ]]; then
  set -a
  # shellcheck disable=SC1090
  source "$ENV_FILE"
  set +a
fi

cd "$ROOT_DIR"

PYTHON_BIN="${PYTHON_BIN:-python3}"
DART_BIN="${DART_BIN:-dart}"

"$PYTHON_BIN" -m pip install --upgrade pip
"$PYTHON_BIN" -m pip install playwright
"$PYTHON_BIN" -m playwright install chromium
"$DART_BIN" pub get
