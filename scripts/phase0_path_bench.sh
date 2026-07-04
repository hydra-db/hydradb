#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RESULTS="${PHASE0_BENCH_RESULTS:-$ROOT/bench-results/phase0_path_bench_full.csv}"
LOG="${PHASE0_BENCH_LOG:-$ROOT/bench-results/phase0_path_bench_full.log}"
FANOUTS="${PHASE0_BENCH_FANOUTS:-50,100,1000,10000,50000,100000}"
HOPS="${PHASE0_BENCH_HOPS:-1,3,5,10,12}"
DATA_HOPS="${PHASE0_BENCH_DATA_HOPS:-12}"
HOT_ITERS="${PHASE0_BENCH_HOT_ITERS:-5}"
WRITE_SAMPLES="${PHASE0_BENCH_WRITE_SAMPLES:-32}"
WRITE_MICROBATCH_SIZE="${PHASE0_BENCH_WRITE_MICROBATCH_SIZE:-1024}"
WRITE_MICROBATCH_COUNT="${PHASE0_BENCH_WRITE_MICROBATCH_COUNT:-3}"
FEATURE_ARGS=()

if [[ "${PHASE0_GRAPHBLAS:-1}" == "1" ]]; then
  FEATURE_ARGS=(--features graphblas)
  export PHASE0_MATRIX_KERNEL="${PHASE0_MATRIX_KERNEL:-graphblas}"
fi

export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-${XDG_CACHE_HOME:-$HOME/.cache}/dgraph-phase0-target}"
mkdir -p "$(dirname "$RESULTS")" "$(dirname "$LOG")"

if [[ "${PHASE0_BENCH_APPEND:-0}" != "1" ]]; then
  rm -f "$RESULTS" "$LOG"
fi

cd "$ROOT"
IFS=',' read -ra fanout_list <<< "$FANOUTS"
for fanout in "${fanout_list[@]}"; do
  tmp="$(mktemp)"
  echo "phase0 path bench fanout=$fanout hops=$HOPS data_hops=$DATA_HOPS hot_iters=$HOT_ITERS write_samples=$WRITE_SAMPLES write_microbatch_size=$WRITE_MICROBATCH_SIZE write_microbatch_count=$WRITE_MICROBATCH_COUNT" | tee -a "$LOG" >&2
  PHASE0_BENCH_FANOUTS="$fanout" \
    PHASE0_BENCH_HOPS="$HOPS" \
    PHASE0_BENCH_DATA_HOPS="$DATA_HOPS" \
    PHASE0_BENCH_HOT_ITERS="$HOT_ITERS" \
    PHASE0_BENCH_WRITE_SAMPLES="$WRITE_SAMPLES" \
    PHASE0_BENCH_WRITE_MICROBATCH_SIZE="$WRITE_MICROBATCH_SIZE" \
    PHASE0_BENCH_WRITE_MICROBATCH_COUNT="$WRITE_MICROBATCH_COUNT" \
    cargo run --release "${FEATURE_ARGS[@]}" --example phase0_path_bench > "$tmp" 2>> "$LOG"
  if [[ ! -s "$RESULTS" ]]; then
    cat "$tmp" >> "$RESULTS"
  else
    tail -n +2 "$tmp" >> "$RESULTS"
  fi
  rm -f "$tmp"
done
