#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
RESULTS="${GRAPH_QUERY_MEMORY_PROFILE_RESULTS:-$ROOT/../../bench-results/query_memory_profile.csv}"
LOG="${GRAPH_QUERY_MEMORY_PROFILE_LOG:-$ROOT/../../bench-results/query_memory_profile.log}"
WORK_ROOT="${GRAPH_QUERY_MEMORY_PROFILE_ROOT:-$ROOT/../../bench-results/query-memory-profile-root}"
FANOUTS="${GRAPH_QUERY_MEMORY_PROFILE_FANOUTS:-50,100,1000,5000,10000}"
HOPS="${GRAPH_QUERY_MEMORY_PROFILE_HOPS:-20}"
DATA_HOPS="${GRAPH_QUERY_MEMORY_PROFILE_DATA_HOPS:-20}"
CONCURRENCY_LIST="${GRAPH_QUERY_MEMORY_PROFILE_CONCURRENCY:-1,8,32,64}"
WORKLOADS="${GRAPH_QUERY_MEMORY_PROFILE_WORKLOADS:-one-hop-page,count,page}"
FEATURES="${GRAPH_QUERY_MEMORY_PROFILE_FEATURES:-opencypher}"
RUNTIME="${GRAPH_QUERY_MEMORY_PROFILE_RUNTIME:-current-thread}"
PAGE_SIZE="${GRAPH_QUERY_MEMORY_PROFILE_PAGE_SIZE:-64}"
BULK_CHUNK_SIZE="${GRAPH_QUERY_MEMORY_PROFILE_BULK_CHUNK_SIZE:-10000}"
KEEP_ROOT="${GRAPH_QUERY_MEMORY_PROFILE_KEEP_ROOT:-0}"

mkdir -p "$(dirname "$RESULTS")" "$(dirname "$LOG")" "$WORK_ROOT"
rm -f "$RESULTS" "$LOG"
if [[ "$KEEP_ROOT" != "1" ]]; then
  rm -rf "$WORK_ROOT"
  mkdir -p "$WORK_ROOT"
fi

header='kind,object_backend,fanout,hops,edges,query_shape,page_size,build_ms,build_rss_mib,cold_samples,cold_open_query_p50_us,cold_open_query_p95_us,cold_open_query_p99_us,cold_open_query_mean_us,cold_query_p50_us,cold_query_p95_us,cold_query_p99_us,cold_query_mean_us,cold_peak_rss_mib,warm_us,warm_rss_mib,hot_p50_us,hot_p95_us,hot_p99_us,hot_mean_us,hot_qps,hot_peak_rss_mib,concurrency,concurrent_queries,concurrent_p50_us,concurrent_p95_us,concurrent_p99_us,concurrent_mean_us,concurrent_qps,concurrent_peak_rss_mib,rows,concurrent_rows,has_next,cold_cache_hydrations,warm_cache_hits,warm_cache_misses,optimizer_plan'
printf '%s\n' "$header" > "$RESULTS"

export MALLOC_ARENA_MAX="${MALLOC_ARENA_MAX:-1}"
export MALLOC_TRIM_THRESHOLD_="${MALLOC_TRIM_THRESHOLD_:-1}"
export GRAPH_TRIM_MEMORY_AFTER_HYDRATION="${GRAPH_TRIM_MEMORY_AFTER_HYDRATION:-1}"
export GRAPH_COMPILED_KERNEL="${GRAPH_COMPILED_KERNEL:-compact}"
export GRAPH_QUERY_BENCH_RUNTIME="$RUNTIME"
export GRAPH_QUERY_BENCH_HOPS="$HOPS"
export GRAPH_QUERY_BENCH_DATA_HOPS="$DATA_HOPS"
export GRAPH_QUERY_BENCH_WORKLOADS="$WORKLOADS"
export GRAPH_QUERY_BENCH_PAGE_SIZE="$PAGE_SIZE"
export GRAPH_QUERY_BENCH_BULK_CHUNK_SIZE="$BULK_CHUNK_SIZE"
export GRAPH_QUERY_BENCH_DISK_CACHE_BYTES="${GRAPH_QUERY_BENCH_DISK_CACHE_BYTES:-0}"
export GRAPH_QUERY_BENCH_MAX_GRAPHBLAS_MATRICES="${GRAPH_QUERY_BENCH_MAX_GRAPHBLAS_MATRICES:-0}"
export GRAPH_QUERY_BENCH_MAX_MATRIX_ADJACENCIES="${GRAPH_QUERY_BENCH_MAX_MATRIX_ADJACENCIES:-0}"
export GRAPH_QUERY_BENCH_COLD_ITERS="${GRAPH_QUERY_BENCH_COLD_ITERS:-3}"
export GRAPH_QUERY_BENCH_HOT_ITERS="${GRAPH_QUERY_BENCH_HOT_ITERS:-5}"
export GRAPH_QUERY_BENCH_CONCURRENT_ITERS="${GRAPH_QUERY_BENCH_CONCURRENT_ITERS:-8}"

IFS=',' read -r -a fanout_values <<< "$FANOUTS"
IFS=',' read -r -a concurrency_values <<< "$CONCURRENCY_LIST"

cd "$ROOT"
for fanout in "${fanout_values[@]}"; do
  fanout="${fanout//[[:space:]]/}"
  [[ -n "$fanout" ]] || continue
  run_root="$WORK_ROOT/fanout-$fanout"
  run_id="memory-profile-$fanout"
  echo "=== build fanout=$fanout ===" | tee -a "$LOG"
  GRAPH_QUERY_BENCH_ROOT="$run_root" \
    GRAPH_QUERY_BENCH_RUN_ID="$run_id" \
    GRAPH_QUERY_BENCH_MODE=build \
    GRAPH_QUERY_BENCH_FANOUTS="$fanout" \
    cargo run --release --features "$FEATURES" --example query_bench >> "$LOG" 2>&1

  for concurrency in "${concurrency_values[@]}"; do
    concurrency="${concurrency//[[:space:]]/}"
    [[ -n "$concurrency" ]] || continue
    echo "=== query fanout=$fanout concurrency=$concurrency ===" | tee -a "$LOG"
    GRAPH_QUERY_BENCH_ROOT="$run_root" \
      GRAPH_QUERY_BENCH_RUN_ID="$run_id" \
      GRAPH_QUERY_BENCH_MODE=query \
      GRAPH_QUERY_BENCH_FANOUTS="$fanout" \
      GRAPH_QUERY_BENCH_CONCURRENCY="$concurrency" \
      cargo run --release --features "$FEATURES" --example query_bench | tail -n +2 >> "$RESULTS"
  done
done

if [[ "$KEEP_ROOT" != "1" ]]; then
  rm -rf "$WORK_ROOT"
fi

echo "wrote results=$RESULTS log=$LOG"
