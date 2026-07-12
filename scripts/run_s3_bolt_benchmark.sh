#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

ROOT="${GRAPH_S3_BOLT_BENCH_ROOT:-/tmp/sgk-s3-bolt-bench}"
if [[ "$ROOT" != /tmp/sgk-s3-bolt-bench ]]; then
  echo "GRAPH_S3_BOLT_BENCH_ROOT must be /tmp/sgk-s3-bolt-bench" >&2
  exit 2
fi
FANOUTS="${GRAPH_S3_BOLT_BENCH_FANOUTS:-30 50 100 1000 5000 10000}"
COLD_SAMPLES="${GRAPH_S3_BOLT_BENCH_COLD_SAMPLES:-3}"
SKIP_MUTATIONS="${GRAPH_S3_BOLT_BENCH_SKIP_MUTATIONS:-false}"
BUCKET="${AWS_BUCKET:-graph-benchmark}"
REGION="${AWS_REGION:-us-east-1}"
PREFIX="${GRAPH_S3_BOLT_BENCH_PREFIX:-codex/s3-bolt-$(date +%Y%m%d-%H%M%S)}"
VENV="${GRAPH_S3_BOLT_BENCH_VENV:-/tmp/sgk-bolt-bench-venv}"
CLEANUP_S3="${GRAPH_S3_BOLT_BENCH_CLEANUP_S3:-true}"

if [[ ! -x "$VENV/bin/python" ]]; then
  python3 -m venv "$VENV"
  "$VENV/bin/pip" install 'neo4j>=6,<7'
fi

eval "$(aws configure export-credentials --format env)"
export CLOUD_PROVIDER=aws AWS_BUCKET="$BUCKET" AWS_REGION="$REGION"

rm -rf -- "$ROOT"
mkdir -p "$ROOT/results"
printf '%s\n' "$PREFIX" >"$ROOT/prefix"
LATENCY_CSV="$ROOT/results/raw-latency.csv"
THROUGHPUT_CSV="$ROOT/results/raw-throughput.csv"
SUMMARY_CSV="$ROOT/results/summary.csv"
SERVER_PID=""

stop_server() {
  if [[ -n "$SERVER_PID" ]]; then
    touch "$ROOT/stop"
    for _ in $(seq 1 600); do
      if ! kill -0 "$SERVER_PID" 2>/dev/null; then
        wait "$SERVER_PID" || true
        SERVER_PID=""
        return
      fi
      sleep 0.1
    done
    kill -TERM "$SERVER_PID" 2>/dev/null || true
    wait "$SERVER_PID" 2>/dev/null || true
    SERVER_PID=""
  fi
}

cleanup() {
  stop_server
}
trap cleanup EXIT

start_server() {
  local fanout="$1"
  local seed="$2"
  local cache_dir="$ROOT/cache-$fanout"
  rm -f "$ROOT/ready" "$ROOT/stop"
  mkdir -p "$cache_dir"
  GRAPH_BENCH_PREFIX="$PREFIX/fanout-$fanout" \
  GRAPH_BENCH_FANOUT="$fanout" \
  GRAPH_DATA_CACHE_DIR="$cache_dir" \
  GRAPH_BENCH_READY_FILE="$ROOT/ready" \
  GRAPH_BENCH_STOP_FILE="$ROOT/stop" \
  GRAPH_BENCH_SEED="$seed" \
  GRAPH_BOLT_ADDR=127.0.0.1:17687 \
  target/release/examples/s3_bolt_benchmark_server \
    >"$ROOT/server-$fanout.log" 2>&1 &
  SERVER_PID=$!
  for _ in $(seq 1 18000); do
    if [[ -s "$ROOT/ready" ]]; then
      return
    fi
    if ! kill -0 "$SERVER_PID" 2>/dev/null; then
      cat "$ROOT/server-$fanout.log" >&2
      return 1
    fi
    sleep 0.1
  done
  echo "server did not become ready for fanout $fanout" >&2
  cat "$ROOT/server-$fanout.log" >&2
  return 1
}

for fanout in $FANOUTS; do
  echo "stage=seed fanout=$fanout prefix=$PREFIX/fanout-$fanout" >&2
  rm -rf -- "$ROOT/cache-$fanout"
  start_server "$fanout" true
  stop_server

  for hop in 1 3 5 10; do
    for sample in $(seq 1 "$COLD_SAMPLES"); do
      echo "stage=cold fanout=$fanout hop=$hop sample=$sample/$COLD_SAMPLES" >&2
      rm -rf -- "$ROOT/cache-$fanout"
      start_server "$fanout" false
      "$VENV/bin/python" scripts/s3_bolt_driver_benchmark.py cold \
        --fanout "$fanout" \
        --hop "$hop" \
        --latency-csv "$LATENCY_CSV" \
        --throughput-csv "$THROUGHPUT_CSV"
      stop_server
    done
  done

  echo "stage=hot-write-delete fanout=$fanout" >&2
  start_server "$fanout" false
  HOT_ARGS=()
  if [[ "$SKIP_MUTATIONS" == true ]]; then
    HOT_ARGS+=(--skip-mutations)
  fi
  "$VENV/bin/python" scripts/s3_bolt_driver_benchmark.py hot \
    --fanout "$fanout" \
    --latency-csv "$LATENCY_CSV" \
    --throughput-csv "$THROUGHPUT_CSV" \
    "${HOT_ARGS[@]}"
  stop_server
done

"$VENV/bin/python" scripts/s3_bolt_driver_benchmark.py summarize \
  --latency-csv "$LATENCY_CSV" \
  --throughput-csv "$THROUGHPUT_CSV" \
  --summary-csv "$SUMMARY_CSV"

echo "benchmark-summary=$SUMMARY_CSV" >&2
if [[ "$CLEANUP_S3" == true ]]; then
  aws s3 rm "s3://$BUCKET/$PREFIX" --recursive >/dev/null
  echo "removed-s3-prefix=s3://$BUCKET/$PREFIX" >&2
fi
