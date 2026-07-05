#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
STORE_ROOT="${PHASE0_LOCAL_OBJECT_ROOT:-$(mktemp -d)}"
DB_BASE="${PHASE0_DB_BASE:-phase0-multiprocess-$(date +%s)-$$}"
OPS="${PHASE0_STRESS_OPS:-2000}"
KILL_OPS="${PHASE0_KILL_OPS:-$OPS}"
TIMEOUT_SECONDS="${PHASE0_WORKER_TIMEOUT:-240}"
KILL_AFTER_SECONDS="${PHASE0_KILL_AFTER_SECONDS:-1}"
VERIFY_HOPS="${PHASE0_VERIFY_HOPS:-3}"
VERIFY_ROOTS="${PHASE0_VERIFY_ROOTS:-8}"
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

export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-${XDG_CACHE_HOME:-$HOME/.cache}/dgraph-phase0-target}"
export PHASE0_OP_DELAY_MICROS="${PHASE0_OP_DELAY_MICROS:-1000}"
export PHASE0_BULK_CHUNK="${PHASE0_BULK_CHUNK:-50}"
export PHASE0_WRITER_BATCH="${PHASE0_WRITER_BATCH:-1024}"
export PHASE0_SEGMENT_CHUNK="${PHASE0_SEGMENT_CHUNK:-128}"
export PHASE0_VERIFY_HOPS="$VERIFY_HOPS"
export PHASE0_VERIFY_ROOTS="$VERIFY_ROOTS"
WORKER_BIN="$CARGO_TARGET_DIR/debug/examples/phase0_stress_worker"
STORE_ARG="local:$STORE_ROOT"

run_worker_for() {
  local cell_id="$1"
  local src_id="$2"
  shift 2
  PHASE0_CELL_ID="$cell_id" PHASE0_SRC_ID="$src_id" PHASE0_INDEX_POLICY="${PHASE0_INDEX_POLICY:-}" \
    timeout "$TIMEOUT_SECONDS" "$WORKER_BIN" "$@"
}

start_worker_group_for() {
  local cell_id="$1"
  local src_id="$2"
  shift 2
  setsid env PHASE0_CELL_ID="$cell_id" PHASE0_SRC_ID="$src_id" PHASE0_INDEX_POLICY="${PHASE0_INDEX_POLICY:-}" \
    timeout "$TIMEOUT_SECONDS" "$WORKER_BIN" "$@" &
  RUN_WORKER_PID="$!"
}

verify_cell() {
  local cell_id="$1"
  local src_id="$2"
  run_worker_for "$cell_id" "$src_id" verify "$STORE_ARG" "$DB_BASE/$cell_id" "verify-$cell_id" 1 1
}

kill_mode_then_recover() {
  local cell_id="$1"
  local src_id="$2"
  local mode="$3"
  local recover_mode="${4:-$mode}"
  local start="${5:-900000}"
  echo "phase0 kill/recover: cell=$cell_id mode=$mode recover=$recover_mode"
  start_worker_group_for "$cell_id" "$src_id" "$mode" "$STORE_ARG" "$DB_BASE/$cell_id" "kill-$mode" "$KILL_OPS" "$start"
  kill_pid="$RUN_WORKER_PID"
  sleep "$KILL_AFTER_SECONDS"
  kill -9 -- "-$kill_pid" >/dev/null 2>&1 || true
  wait "$kill_pid" >/dev/null 2>&1 || true
  run_worker_for "$cell_id" "$src_id" "$recover_mode" "$STORE_ARG" "$DB_BASE/$cell_id" "recover-$mode" "$KILL_OPS" "$start"
  verify_cell "$cell_id" "$src_id"
}

cd "$ROOT"
mkdir -p "$STORE_ROOT"
cargo build "${FEATURE_ARGS[@]}" --example phase0_stress_worker >/dev/null

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
verify_cell reddit-home 1
verify_cell reddit-search 2
verify_cell reddit-ads 3

echo "phase0 kill/restart injection"
kill_mode_then_recover reddit-kill-batch 9 batch batch 900000

run_worker_for reddit-kill-matrix 10 batch "$STORE_ARG" "$DB_BASE/reddit-kill-matrix" seed-matrix "$KILL_OPS" 1000000
kill_mode_then_recover reddit-kill-matrix 10 matrix matrix 1000000

run_worker_for reddit-kill-supernode 11 batch "$STORE_ARG" "$DB_BASE/reddit-kill-supernode" seed-supernode "$KILL_OPS" 1100000
kill_mode_then_recover reddit-kill-supernode 11 supernode supernode 1100000

run_worker_for reddit-kill-rollup 12 batch "$STORE_ARG" "$DB_BASE/reddit-kill-rollup" seed-rollup "$KILL_OPS" 1200000
kill_mode_then_recover reddit-kill-rollup 12 rollup rollup 1200000

run_worker_for reddit-kill-delta-gc 13 batch "$STORE_ARG" "$DB_BASE/reddit-kill-delta-gc" seed-delta-gc "$KILL_OPS" 1300000
run_worker_for reddit-kill-delta-gc 13 rollup "$STORE_ARG" "$DB_BASE/reddit-kill-delta-gc" seed-delta-gc-rollup 1 1
kill_mode_then_recover reddit-kill-delta-gc 13 delta-gc delta-gc 1300000

PHASE0_INDEX_POLICY=outbound-only kill_mode_then_recover reddit-kill-segment 15 segment segment 1500000
PHASE0_INDEX_POLICY=outbound-only run_worker_for reddit-kill-segment 15 segment-delete "$STORE_ARG" "$DB_BASE/reddit-kill-segment" seed-segment-delete "$KILL_OPS" 1500000
PHASE0_INDEX_POLICY=outbound-only kill_mode_then_recover reddit-kill-segment 15 segment-compact segment-compact 1500000
PHASE0_INDEX_POLICY=outbound-only kill_mode_then_recover reddit-kill-segment-gc 16 segment segment 1600000
PHASE0_INDEX_POLICY=outbound-only run_worker_for reddit-kill-segment-gc 16 segment-delete "$STORE_ARG" "$DB_BASE/reddit-kill-segment-gc" seed-segment-gc-delete "$KILL_OPS" 1600000
PHASE0_INDEX_POLICY=outbound-only kill_mode_then_recover reddit-kill-segment-gc 16 segment-gc segment-gc 1600000

PHASE0_VERIFY_HOPS="$VERIFY_HOPS" PHASE0_VERIFY_ROOTS="$VERIFY_ROOTS" scripts/phase0_fence_takeover.sh

echo "phase0 multiprocess stress passed"
