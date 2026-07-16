#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
STORE_ROOT="${GRAPH_LOCAL_OBJECT_ROOT:-$(mktemp -d)}"
DB_BASE="${GRAPH_DB_BASE:-graph-fence-$(date +%s)-$$}"
TIMEOUT_SECONDS="${GRAPH_WORKER_TIMEOUT:-240}"
FEATURE_ARGS=(--features chaos-harness)
SIGNAL_DIR="$(mktemp -d)"
INCUMBENT_PID=""

cleanup() {
  if [[ -z "${GRAPH_LOCAL_OBJECT_ROOT:-}" ]]; then
    rm -rf "$STORE_ROOT"
  fi
  rm -rf "$SIGNAL_DIR"
  if [[ -n "$INCUMBENT_PID" ]]; then
    kill "$INCUMBENT_PID" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-${XDG_CACHE_HOME:-$HOME/.cache}/dgraph-target}"
STORE_ARG="local:$STORE_ROOT"
DATA_PATH="$DB_BASE/data"

run_worker() {
  timeout "$TIMEOUT_SECONDS" cargo run "${FEATURE_ARGS[@]}" --example fence_worker -- "$@"
}

cd "$ROOT"
mkdir -p "$STORE_ROOT"

echo "graph SlateDB fence takeover: store=$STORE_ROOT data=$DATA_PATH"
run_worker incumbent "$STORE_ARG" "$DATA_PATH" "$SIGNAL_DIR" &
INCUMBENT_PID=$!
run_worker takeover "$STORE_ARG" "$DATA_PATH" "$SIGNAL_DIR"
wait "$INCUMBENT_PID"
INCUMBENT_PID=""
run_worker reader "$STORE_ARG" "$DATA_PATH" "$SIGNAL_DIR"
echo "graph fence takeover passed"
