#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RESULTS="${PHASE2_QUERY_BENCH_RESULTS:-$ROOT/bench-results/phase2_query_bench_full.csv}"
LOG="${PHASE2_QUERY_BENCH_LOG:-$ROOT/bench-results/phase2_query_bench_full.log}"
FANOUTS="${PHASE2_QUERY_BENCH_FANOUTS:-50,100,1000,5000,10000}"
HOPS="${PHASE2_QUERY_BENCH_HOPS:-1,5,10,15,20}"
DATA_HOPS="${PHASE2_QUERY_BENCH_DATA_HOPS:-20}"
HOT_ITERS="${PHASE2_QUERY_BENCH_HOT_ITERS:-9}"
CONCURRENCY="${PHASE2_QUERY_BENCH_CONCURRENCY:-8}"
CONCURRENT_ITERS="${PHASE2_QUERY_BENCH_CONCURRENT_ITERS:-16}"
PAGE_SIZE="${PHASE2_QUERY_BENCH_PAGE_SIZE:-64}"
FEATURES="${PHASE2_QUERY_BENCH_FEATURES:-opencypher,graphblas}"

export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-${XDG_CACHE_HOME:-$HOME/.cache}/dgraph-phase2-target}"
mkdir -p "$(dirname "$RESULTS")" "$(dirname "$LOG")"

if [[ "${PHASE2_QUERY_BENCH_APPEND:-0}" != "1" ]]; then
  rm -f "$RESULTS" "$LOG"
fi

cd "$ROOT"
IFS=',' read -ra fanout_list <<< "$FANOUTS"
for fanout in "${fanout_list[@]}"; do
  tmp="$(mktemp)"
  echo "phase2 query bench fanout=$fanout hops=$HOPS data_hops=$DATA_HOPS hot_iters=$HOT_ITERS concurrency=$CONCURRENCY concurrent_iters=$CONCURRENT_ITERS page_size=$PAGE_SIZE features=$FEATURES" | tee -a "$LOG" >&2
  PHASE2_QUERY_BENCH_FANOUTS="$fanout" \
    PHASE2_QUERY_BENCH_HOPS="$HOPS" \
    PHASE2_QUERY_BENCH_DATA_HOPS="$DATA_HOPS" \
    PHASE2_QUERY_BENCH_HOT_ITERS="$HOT_ITERS" \
    PHASE2_QUERY_BENCH_CONCURRENCY="$CONCURRENCY" \
    PHASE2_QUERY_BENCH_CONCURRENT_ITERS="$CONCURRENT_ITERS" \
    PHASE2_QUERY_BENCH_PAGE_SIZE="$PAGE_SIZE" \
    cargo run --release --features "$FEATURES" --example phase2_query_bench > "$tmp" 2>> "$LOG"
  if [[ ! -s "$RESULTS" ]]; then
    cat "$tmp" >> "$RESULTS"
  else
    tail -n +2 "$tmp" >> "$RESULTS"
  fi
  rm -f "$tmp"
done
