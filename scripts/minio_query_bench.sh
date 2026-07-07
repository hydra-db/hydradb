#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
NAME="${GRAPH_QUERY_MINIO_NAME:-dgraph-query-bench-minio}"
NETWORK="${GRAPH_QUERY_MINIO_NETWORK:-dgraph-query-bench-net}"
PORT="${GRAPH_QUERY_MINIO_PORT:-19012}"
ACCESS_KEY="${GRAPH_QUERY_MINIO_ACCESS_KEY:-query$(date +%s)$$}"
SECRET_KEY="${GRAPH_QUERY_MINIO_SECRET_KEY:-query-secret-$(date +%s)-$$}"
MINIO_IMAGE="${GRAPH_QUERY_MINIO_IMAGE:-minio/minio:RELEASE.2025-07-23T15-54-02Z}"
MC_IMAGE="${GRAPH_QUERY_MC_IMAGE:-minio/mc:RELEASE.2025-04-16T18-13-26Z}"
BUCKET="${GRAPH_QUERY_MINIO_BUCKET:-query-bench-$(date +%s)-$$}"
ENV_FILE="$(mktemp)"

cleanup() {
  rm -f "$ENV_FILE"
  if [[ "${GRAPH_QUERY_KEEP_MINIO:-0}" != "1" ]]; then
    docker rm -f "$NAME" >/dev/null 2>&1 || true
    docker network rm "$NETWORK" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

if ! command -v docker >/dev/null 2>&1; then
  echo "docker is required for the Query engine MinIO query benchmark" >&2
  exit 1
fi

docker rm -f "$NAME" >/dev/null 2>&1 || true
docker network rm "$NETWORK" >/dev/null 2>&1 || true
docker network create "$NETWORK" >/dev/null
docker run \
  --detach \
  --name "$NAME" \
  --network "$NETWORK" \
  --publish "127.0.0.1:$PORT:9000" \
  --env "MINIO_ROOT_USER=$ACCESS_KEY" \
  --env "MINIO_ROOT_PASSWORD=$SECRET_KEY" \
  "$MINIO_IMAGE" server /data >/dev/null

for _ in {1..60}; do
  if docker run --rm --network "$NETWORK" --entrypoint /bin/sh "$MC_IMAGE" \
    -c "mc alias set local 'http://$NAME:9000' '$ACCESS_KEY' '$SECRET_KEY' >/dev/null" >/dev/null 2>&1; then
    break
  fi
  sleep 1
done

docker run --rm --network "$NETWORK" --entrypoint /bin/sh "$MC_IMAGE" \
  -c "mc alias set local 'http://$NAME:9000' '$ACCESS_KEY' '$SECRET_KEY' >/dev/null && mc mb --ignore-existing 'local/$BUCKET'" >/dev/null

cat >"$ENV_FILE" <<ENV
CLOUD_PROVIDER=aws
AWS_ACCESS_KEY_ID=$ACCESS_KEY
AWS_SECRET_ACCESS_KEY=$SECRET_KEY
AWS_DEFAULT_REGION=us-east-1
AWS_ENDPOINT=http://127.0.0.1:$PORT
AWS_BUCKET=$BUCKET
AWS_ALLOW_HTTP=true
AWS_VIRTUAL_HOSTED_STYLE_REQUEST=false
ENV

export GRAPH_QUERY_BENCH_OBJECT_ENV="$ENV_FILE"
export GRAPH_QUERY_BENCH_RESULTS="${GRAPH_QUERY_BENCH_RESULTS:-$ROOT/bench-results/query_bench_minio.csv}"
export GRAPH_QUERY_BENCH_LOG="${GRAPH_QUERY_BENCH_LOG:-$ROOT/bench-results/query_bench_minio.log}"
export GRAPH_QUERY_BENCH_FANOUTS="${GRAPH_QUERY_BENCH_FANOUTS:-50,100,1000,5000,10000}"
export GRAPH_QUERY_BENCH_HOPS="${GRAPH_QUERY_BENCH_HOPS:-1,5,10,15,20}"
export GRAPH_QUERY_BENCH_DATA_HOPS="${GRAPH_QUERY_BENCH_DATA_HOPS:-20}"
export GRAPH_QUERY_BENCH_COLD_ITERS="${GRAPH_QUERY_BENCH_COLD_ITERS:-5}"
export GRAPH_QUERY_BENCH_HOT_ITERS="${GRAPH_QUERY_BENCH_HOT_ITERS:-9}"
export GRAPH_QUERY_BENCH_INDEX_POLICY="${GRAPH_QUERY_BENCH_INDEX_POLICY:-outbound-only}"
export GRAPH_QUERY_BENCH_BULK_CHUNK_SIZE="${GRAPH_QUERY_BENCH_BULK_CHUNK_SIZE:-10000}"
export GRAPH_QUERY_BENCH_WORKLOADS="${GRAPH_QUERY_BENCH_WORKLOADS:-all}"
export GRAPH_QUERY_BENCH_MODE="${GRAPH_QUERY_BENCH_MODE:-full}"
export GRAPH_QUERY_BENCH_MAX_GRAPHBLAS_MATRICES="${GRAPH_QUERY_BENCH_MAX_GRAPHBLAS_MATRICES:-1}"
export GRAPH_QUERY_BENCH_MAX_MATRIX_ADJACENCIES="${GRAPH_QUERY_BENCH_MAX_MATRIX_ADJACENCIES:-0}"
export GRAPH_QUERY_BENCH_MAX_REACHABILITY_RESULTS="${GRAPH_QUERY_BENCH_MAX_REACHABILITY_RESULTS:-0}"
export GRAPH_QUERY_BENCH_RUNTIME="${GRAPH_QUERY_BENCH_RUNTIME:-multi-thread}"
export GRAPH_QUERY_BENCH_RUNTIME_WORKERS="${GRAPH_QUERY_BENCH_RUNTIME_WORKERS:-}"
export GRAPH_QUERY_BENCH_CONCURRENCY="${GRAPH_QUERY_BENCH_CONCURRENCY:-8}"
export GRAPH_QUERY_BENCH_CONCURRENT_ITERS="${GRAPH_QUERY_BENCH_CONCURRENT_ITERS:-16}"
export GRAPH_QUERY_BENCH_PAGE_SIZE="${GRAPH_QUERY_BENCH_PAGE_SIZE:-64}"

echo "query MinIO query bench: bucket=$BUCKET endpoint=http://127.0.0.1:$PORT results=$GRAPH_QUERY_BENCH_RESULTS log=$GRAPH_QUERY_BENCH_LOG"
"$ROOT/scripts/query_bench.sh"

docker run --rm --network "$NETWORK" --entrypoint /bin/sh "$MC_IMAGE" \
  -c "mc alias set local 'http://$NAME:9000' '$ACCESS_KEY' '$SECRET_KEY' >/dev/null && mc du 'local/$BUCKET'" || true
