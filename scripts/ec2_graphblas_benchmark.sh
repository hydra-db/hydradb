#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

DEGREE="${GRAPH_BENCH_DEGREE:-30}"
HOPS="${GRAPH_BENCH_HOPS:-1,3,5,10}"
WARMUP="${GRAPH_BENCH_WARMUP:-10}"
SAMPLES="${GRAPH_BENCH_SAMPLES:-100}"
CONCURRENCY="${GRAPH_BENCH_CONCURRENCY:-8}"
OPERATIONS_PER_WORKER="${GRAPH_BENCH_OPERATIONS_PER_WORKER:-50}"
GRAPHBLAS_THREADS="${GRAPH_BENCH_GRAPHBLAS_THREADS:-}"
PORT="${GRAPH_BENCH_PORT:-17687}"
BUCKET="${AWS_BUCKET_NAME:-graph-benchmark}"
REGION="${AWS_DEFAULT_REGION:-us-east-1}"
IMAGE="${GRAPH_BENCH_IMAGE:-turbolay-graphblas-benchmark:local}"
TOKEN="s3-bolt-benchmark-secret-32-chars"
RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)-$$"
PREFIX="${GRAPH_BENCH_PREFIX:-turbolay/benchmarks/graphblas-degree-${DEGREE}/${RUN_ID}}"
RESULT_DIR="${GRAPH_BENCH_RESULT_DIR:-$ROOT_DIR/bench-results/ec2-graphblas-${RUN_ID}}"
VENV="${GRAPH_BENCH_VENV:-$HOME/.cache/turbolay-graphblas-benchmark-venv}"
RESULT_DIR="$(realpath -m "$RESULT_DIR")"
VENV="$(realpath -m "$VENV")"
CONTAINER="turbolay-graphblas-bench-${RUN_ID,,}"
SEED_CONTAINER="${CONTAINER}-seed"
QUERY_CONTAINER="${CONTAINER}-query"
IFS=',' read -r -a HOP_VALUES <<<"$HOPS"
EXPECTED_GRAPHBLAS_TASKS=$((
  ${#HOP_VALUES[@]} * (WARMUP + SAMPLES + CONCURRENCY * OPERATIONS_PER_WORKER)
))

case "$PREFIX" in
  turbolay/benchmarks/*) ;;
  *) echo "GRAPH_BENCH_PREFIX must remain under turbolay/benchmarks/" >&2; exit 2 ;;
esac
[[ "$DEGREE" =~ ^[1-9][0-9]*$ ]] || { echo "degree must be positive" >&2; exit 2; }
[[ "$PORT" =~ ^[1-9][0-9]*$ ]] || { echo "port must be positive" >&2; exit 2; }
command -v python3 >/dev/null
command -v aws >/dev/null

AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-$(aws configure get aws_access_key_id)}"
AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-$(aws configure get aws_secret_access_key)}"
AWS_SESSION_TOKEN="${AWS_SESSION_TOKEN:-$(aws configure get aws_session_token || true)}"
[[ -n "$AWS_ACCESS_KEY_ID" && -n "$AWS_SECRET_ACCESS_KEY" ]] || {
  echo "AWS credentials could not be resolved from the environment or AWS CLI profile." >&2
  exit 2
}
export AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN
AWS_CREDENTIAL_ARGS=(-e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY)
if [[ -n "$AWS_SESSION_TOKEN" ]]; then
  AWS_CREDENTIAL_ARGS+=(-e AWS_SESSION_TOKEN)
fi

GRAPHBLAS_THREAD_ARGS=()
if [[ -n "$GRAPHBLAS_THREADS" ]]; then
  [[ "$GRAPHBLAS_THREADS" =~ ^[1-9][0-9]*$ ]] || {
    echo "GRAPH_BENCH_GRAPHBLAS_THREADS must be a positive integer" >&2
    exit 2
  }
  GRAPHBLAS_THREAD_ARGS=(-e "OMP_NUM_THREADS=$GRAPHBLAS_THREADS")
fi

if docker info >/dev/null 2>&1; then
  DOCKER=(docker)
elif sudo -n docker info >/dev/null 2>&1; then
  DOCKER=(sudo -n docker)
else
  echo "Docker is unavailable to the current user and passwordless sudo." >&2
  exit 2
fi

mkdir -p "$RESULT_DIR/seed-cache" "$RESULT_DIR/query-cache"
chmod 700 "$RESULT_DIR"
rm -f \
  "$RESULT_DIR/seed-ready" "$RESULT_DIR/seed-stop" "$RESULT_DIR/seed_metrics.json" \
  "$RESULT_DIR/ready" "$RESULT_DIR/stop" "$RESULT_DIR/server_metrics.json"

SERVER_PID=""
ACTIVE_CONTAINER=""
cleanup() {
  touch "$RESULT_DIR/seed-stop" 2>/dev/null || true
  touch "$RESULT_DIR/stop" 2>/dev/null || true
  if [[ -n "$SERVER_PID" ]] && kill -0 "$SERVER_PID" 2>/dev/null; then
    "${DOCKER[@]}" stop --time 10 "$ACTIVE_CONTAINER" >/dev/null 2>&1 || true
    wait "$SERVER_PID" 2>/dev/null || true
  fi
}
trap cleanup EXIT INT TERM

if [[ "${GRAPH_BENCH_SKIP_BUILD:-0}" == "1" ]]; then
  "${DOCKER[@]}" image inspect "$IMAGE" >/dev/null
  echo "Reusing benchmark image $IMAGE"
else
  echo "Building benchmark-only image target..."
  "${DOCKER[@]}" build --target graphblas-benchmark -t "$IMAGE" .
fi

echo "Seeding one canonical graph into isolated real-S3 storage..."
"${DOCKER[@]}" run --rm \
  --name "$SEED_CONTAINER" \
  --network host \
  --user "$(id -u):$(id -g)" \
  -e HOME=/tmp \
  -e CLOUD_PROVIDER=aws \
  -e AWS_BUCKET_NAME="$BUCKET" \
  -e AWS_DEFAULT_REGION="$REGION" \
  "${AWS_CREDENTIAL_ARGS[@]}" \
  "${GRAPHBLAS_THREAD_ARGS[@]}" \
  -e GRAPH_BENCH_FANOUT="$DEGREE" \
  -e GRAPH_BENCH_PREFIX="$PREFIX" \
  -e GRAPH_DATA_CACHE_DIR=/state/seed-cache \
  -e GRAPH_BENCH_READY_FILE=/state/seed-ready \
  -e GRAPH_BENCH_STOP_FILE=/state/seed-stop \
  -e GRAPH_BENCH_METRICS_FILE=/state/seed_metrics.json \
  -e GRAPH_BENCH_EXPECTED_GRAPHBLAS_TASKS=0 \
  -e GRAPH_BENCH_SEED=true \
  -e GRAPH_BOLT_ADDR="127.0.0.1:$PORT" \
  -v "$RESULT_DIR:/state" \
  "$IMAGE" >"$RESULT_DIR/seed.log" 2>&1 &
SERVER_PID=$!
ACTIVE_CONTAINER="$SEED_CONTAINER"

for _ in $(seq 1 900); do
  [[ -s "$RESULT_DIR/seed-ready" ]] && break
  if ! kill -0 "$SERVER_PID" 2>/dev/null; then
    cat "$RESULT_DIR/seed.log" >&2
    exit 1
  fi
  sleep 1
done
[[ -s "$RESULT_DIR/seed-ready" ]] || { echo "benchmark seeder did not become ready" >&2; exit 1; }
touch "$RESULT_DIR/seed-stop"
if ! wait "$SERVER_PID"; then
  SERVER_PID=""
  cat "$RESULT_DIR/seed.log" >&2
  exit 1
fi
SERVER_PID=""
ACTIVE_CONTAINER=""

echo "Starting a clean query process from S3 ground truth..."
"${DOCKER[@]}" run --rm \
  --name "$QUERY_CONTAINER" \
  --network host \
  --user "$(id -u):$(id -g)" \
  -e HOME=/tmp \
  -e CLOUD_PROVIDER=aws \
  -e AWS_BUCKET_NAME="$BUCKET" \
  -e AWS_DEFAULT_REGION="$REGION" \
  "${AWS_CREDENTIAL_ARGS[@]}" \
  "${GRAPHBLAS_THREAD_ARGS[@]}" \
  -e GRAPH_BENCH_FANOUT="$DEGREE" \
  -e GRAPH_BENCH_PREFIX="$PREFIX" \
  -e GRAPH_DATA_CACHE_DIR=/state/query-cache \
  -e GRAPH_BENCH_READY_FILE=/state/ready \
  -e GRAPH_BENCH_STOP_FILE=/state/stop \
  -e GRAPH_BENCH_METRICS_FILE=/state/server_metrics.json \
  -e GRAPH_BENCH_EXPECTED_GRAPHBLAS_TASKS="$EXPECTED_GRAPHBLAS_TASKS" \
  -e GRAPH_BENCH_SEED=false \
  -e GRAPH_BOLT_ADDR="127.0.0.1:$PORT" \
  -v "$RESULT_DIR:/state" \
  "$IMAGE" >"$RESULT_DIR/server.log" 2>&1 &
SERVER_PID=$!
ACTIVE_CONTAINER="$QUERY_CONTAINER"

for _ in $(seq 1 900); do
  [[ -s "$RESULT_DIR/ready" ]] && break
  if ! kill -0 "$SERVER_PID" 2>/dev/null; then
    cat "$RESULT_DIR/server.log" >&2
    exit 1
  fi
  sleep 1
done
[[ -s "$RESULT_DIR/ready" ]] || { echo "benchmark query server did not become ready" >&2; exit 1; }

if [[ ! -x "$VENV/bin/python" ]]; then
  python3 -m venv "$VENV"
fi
"$VENV/bin/python" -m pip install --quiet --disable-pip-version-check "neo4j==6.2.0"

echo "Running official Neo4j driver on EC2 loopback..."
"$VENV/bin/python" scripts/bolt_graphblas_client.py \
  --uri "bolt://127.0.0.1:$PORT" \
  --token "$TOKEN" \
  --degree "$DEGREE" \
  --hops "$HOPS" \
  --warmup "$WARMUP" \
  --samples "$SAMPLES" \
  --concurrency "$CONCURRENCY" \
  --operations-per-worker "$OPERATIONS_PER_WORKER" \
  --output-dir "$RESULT_DIR"

touch "$RESULT_DIR/stop"
if ! wait "$SERVER_PID"; then
  SERVER_PID=""
  cat "$RESULT_DIR/server.log" >&2
  exit 1
fi
SERVER_PID=""
ACTIVE_CONTAINER=""

aws s3 ls "s3://$BUCKET/$PREFIX/" \
  --recursive \
  --summarize \
  --region "$REGION" >"$RESULT_DIR/s3_listing.txt"

"$VENV/bin/python" - "$RESULT_DIR" <<'PY'
import json
import pathlib
import sys

root = pathlib.Path(sys.argv[1])
client = json.loads((root / "client_metrics.json").read_text())
server = json.loads((root / "server_metrics.json").read_text())
storage_lines = (root / "s3_listing.txt").read_text().splitlines()
storage = {}
for line in storage_lines:
    if line.startswith("Total Objects:"):
        storage["object_count"] = int(line.split(":", 1)[1].strip())
    elif line.startswith("   Total Size:"):
        storage["total_bytes"] = int(line.split(":", 1)[1].strip())
if set(storage) != {"object_count", "total_bytes"}:
    raise SystemExit("could not parse S3 storage summary")
(root / "s3_storage.json").write_text(json.dumps(storage, indent=2) + "\n")
errors = []
if not server.get("graphblas_compiled"):
    errors.append("server was not compiled with GraphBLAS")
if server.get("verified_matrix_artifacts") != 1:
    errors.append("the canonical edge type did not have one exact-epoch adjacency image")
if server.get("graph_compute_tasks", 0) < client["query_calls"]:
    errors.append(
        f"only {server.get('graph_compute_tasks', 0)} GraphBLAS tasks were observed "
        f"for {client['query_calls']} Bolt queries"
    )
if server.get("query_rows_failed"):
    errors.append(f"server recorded {server['query_rows_failed']} failed row queries")
if errors:
    raise SystemExit("GraphBLAS verification failed: " + "; ".join(errors))
print(
    "GraphBLAS verified: "
    f"artifacts={server['verified_matrix_artifacts']} "
    f"tasks={server['graph_compute_tasks']} "
    f"queries={client['query_calls']} "
    f"compute_us={server['graph_compute_duration_us']}"
)
cache = server.get("cache_resident_bytes", {})
print(
    "Memory verified: "
    f"rss_kib={server.get('process_rss_kib')} "
    f"peak_rss_kib={server.get('process_peak_rss_kib')} "
    f"graph_cache_bytes={cache.get('total')}"
)
PY
rm -f "$RESULT_DIR/s3_listing.txt"

if [[ "${GRAPH_BENCH_KEEP_S3_DATA:-0}" != "1" ]]; then
  echo "Removing isolated benchmark prefix s3://$BUCKET/$PREFIX"
  aws s3 rm "s3://$BUCKET/$PREFIX" --recursive --only-show-errors || \
    echo "warning: benchmark S3 cleanup failed; retained prefix s3://$BUCKET/$PREFIX" >&2
fi

echo "Results: $RESULT_DIR/results.csv"
echo "Server verification: $RESULT_DIR/server_metrics.json"
echo "S3 storage: $RESULT_DIR/s3_storage.json"
