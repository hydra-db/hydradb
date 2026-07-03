#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
STORE_ROOT="${PHASE0_LOCAL_OBJECT_ROOT:-$(mktemp -d)}"
DB_BASE="${PHASE0_DB_BASE:-phase0-multiprocess-$(date +%s)-$$}"
OPS="${PHASE0_STRESS_OPS:-2000}"
KILL_OPS="${PHASE0_KILL_OPS:-$OPS}"
TIMEOUT_SECONDS="${PHASE0_WORKER_TIMEOUT:-240}"
FEATURE_ARGS=()

cleanup() {
  if [[ -z "${PHASE0_LOCAL_OBJECT_ROOT:-}" ]]; then
    rm -rf "$STORE_ROOT"
  fi
}
trap cleanup EXIT

if [[ "${PHASE0_GRAPHBLAS:-0}" == "1" ]]; then
  FEATURE_ARGS=(--features graphblas)
  export PHASE0_MATRIX_KERNEL="${PHASE0_MATRIX_KERNEL:-graphblas}"
fi

export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-/home/vishal/.cache/dgraph-phase0-target}"
STORE_ARG="local:$STORE_ROOT"

run_worker_for() {
  local cell_id="$1"
  local src_id="$2"
  shift 2
  PHASE0_CELL_ID="$cell_id" PHASE0_SRC_ID="$src_id" \
    timeout "$TIMEOUT_SECONDS" cargo run "${FEATURE_ARGS[@]}" --example phase0_stress_worker -- "$@"
}

cd "$ROOT"
mkdir -p "$STORE_ROOT"

echo "phase0 multiprocess stress: store=$STORE_ROOT db_base=$DB_BASE ops=$OPS"

run_worker_for reddit-home 1 writer "$STORE_ARG" "$DB_BASE/reddit-home" node-a "$OPS" 100000 &
pid_a=$!
run_worker_for reddit-search 2 writer "$STORE_ARG" "$DB_BASE/reddit-search" node-b "$OPS" 200000 &
pid_b=$!
run_worker_for reddit-ads 3 writer "$STORE_ARG" "$DB_BASE/reddit-ads" node-c "$OPS" 300000 &
pid_c=$!
wait "$pid_a"
wait "$pid_b"
wait "$pid_c"

run_worker_for reddit-home 1 artifact "$STORE_ARG" "$DB_BASE/reddit-home" node-a 1 1
run_worker_for reddit-search 2 artifact "$STORE_ARG" "$DB_BASE/reddit-search" node-b 1 1
run_worker_for reddit-ads 3 artifact "$STORE_ARG" "$DB_BASE/reddit-ads" node-c 1 1

run_worker_for reddit-home 1 reader "$STORE_ARG" "$DB_BASE/reddit-home" reader-a 1 1
run_worker_for reddit-search 2 reader "$STORE_ARG" "$DB_BASE/reddit-search" reader-b 1 2
run_worker_for reddit-ads 3 reader "$STORE_ARG" "$DB_BASE/reddit-ads" reader-c 1 3

echo "phase0 kill/restart injection"
run_worker_for reddit-kill 9 writer "$STORE_ARG" "$DB_BASE/reddit-kill" node-kill "$KILL_OPS" 900000 &
kill_pid=$!
sleep "${PHASE0_KILL_AFTER_SECONDS:-2}"
kill -9 "$kill_pid" >/dev/null 2>&1 || true
wait "$kill_pid" >/dev/null 2>&1 || true
run_worker_for reddit-kill 9 writer "$STORE_ARG" "$DB_BASE/reddit-kill" node-kill "$KILL_OPS" 900000
run_worker_for reddit-kill 9 artifact "$STORE_ARG" "$DB_BASE/reddit-kill" node-kill 1 1
run_worker_for reddit-kill 9 reader "$STORE_ARG" "$DB_BASE/reddit-kill" reader-kill 1 9

echo "phase0 multiprocess stress passed"
