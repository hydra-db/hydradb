#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
STORE_ROOT="${PHASE0_LOCAL_OBJECT_ROOT:-$(mktemp -d)}"
DB_BASE="${PHASE0_DB_BASE:-phase0-fence-$(date +%s)-$$}"
TIMEOUT_SECONDS="${PHASE0_WORKER_TIMEOUT:-240}"
LEASE_TTL_MS="${PHASE0_LEASE_TTL_MS:-5000}"
FEATURE_ARGS=(--features chaos-harness)

cleanup() {
  if [[ -z "${PHASE0_LOCAL_OBJECT_ROOT:-}" ]]; then
    rm -rf "$STORE_ROOT"
  fi
}
trap cleanup EXIT

export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-${XDG_CACHE_HOME:-$HOME/.cache}/dgraph-phase0-target}"
export PHASE0_LEASE_TTL_MS="$LEASE_TTL_MS"

STORE_ARG="local:$STORE_ROOT"
DATA_PATH="$DB_BASE/data"
CONTROL_PATH="$DB_BASE/control"

run_worker() {
  timeout "$TIMEOUT_SECONDS" cargo run "${FEATURE_ARGS[@]}" --example phase0_fence_worker -- "$@"
}

cd "$ROOT"
mkdir -p "$STORE_ROOT"

echo "phase0 fence takeover: store=$STORE_ROOT data=$DATA_PATH control=$CONTROL_PATH ttl_ms=$LEASE_TTL_MS"
run_worker init "$STORE_ARG" "$DATA_PATH" "$CONTROL_PATH"
sleep "$(awk "BEGIN { printf \"%.3f\", (($LEASE_TTL_MS + 150) / 1000) }")"
run_worker takeover "$STORE_ARG" "$DATA_PATH" "$CONTROL_PATH"
run_worker stale-probe "$STORE_ARG" "$DATA_PATH" "$CONTROL_PATH"
run_worker reader "$STORE_ARG" "$DATA_PATH" "$CONTROL_PATH"
echo "phase0 fence takeover passed"
