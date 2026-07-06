#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
NAME="${PHASE2_MINIO_NAME:-dgraph-phase2-query-bench-minio}"
NETWORK="${PHASE2_MINIO_NETWORK:-dgraph-phase2-query-bench-net}"
PORT="${PHASE2_MINIO_PORT:-19012}"
ACCESS_KEY="${PHASE2_MINIO_ACCESS_KEY:-phase2$(date +%s)$$}"
SECRET_KEY="${PHASE2_MINIO_SECRET_KEY:-phase2-secret-$(date +%s)-$$}"
MINIO_IMAGE="${PHASE2_MINIO_IMAGE:-minio/minio:RELEASE.2025-07-23T15-54-02Z}"
MC_IMAGE="${PHASE2_MC_IMAGE:-minio/mc:RELEASE.2025-04-16T18-13-26Z}"
BUCKET="${PHASE2_MINIO_BUCKET:-phase2-query-bench-$(date +%s)-$$}"
ENV_FILE="$(mktemp)"

cleanup() {
  rm -f "$ENV_FILE"
  if [[ "${PHASE2_KEEP_MINIO:-0}" != "1" ]]; then
    docker rm -f "$NAME" >/dev/null 2>&1 || true
    docker network rm "$NETWORK" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

if ! command -v docker >/dev/null 2>&1; then
  echo "docker is required for the Phase 2 MinIO query benchmark" >&2
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

export PHASE2_QUERY_BENCH_OBJECT_ENV="$ENV_FILE"
export PHASE2_QUERY_BENCH_RESULTS="${PHASE2_QUERY_BENCH_RESULTS:-$ROOT/bench-results/phase2_query_bench_minio.csv}"
export PHASE2_QUERY_BENCH_LOG="${PHASE2_QUERY_BENCH_LOG:-$ROOT/bench-results/phase2_query_bench_minio.log}"
export PHASE2_QUERY_BENCH_FANOUTS="${PHASE2_QUERY_BENCH_FANOUTS:-50,100,1000,5000,10000}"
export PHASE2_QUERY_BENCH_HOPS="${PHASE2_QUERY_BENCH_HOPS:-1,5,10,15,20}"
export PHASE2_QUERY_BENCH_DATA_HOPS="${PHASE2_QUERY_BENCH_DATA_HOPS:-20}"
export PHASE2_QUERY_BENCH_HOT_ITERS="${PHASE2_QUERY_BENCH_HOT_ITERS:-9}"
export PHASE2_QUERY_BENCH_CONCURRENCY="${PHASE2_QUERY_BENCH_CONCURRENCY:-8}"
export PHASE2_QUERY_BENCH_CONCURRENT_ITERS="${PHASE2_QUERY_BENCH_CONCURRENT_ITERS:-16}"
export PHASE2_QUERY_BENCH_PAGE_SIZE="${PHASE2_QUERY_BENCH_PAGE_SIZE:-64}"

echo "phase2 MinIO query bench: bucket=$BUCKET endpoint=http://127.0.0.1:$PORT results=$PHASE2_QUERY_BENCH_RESULTS log=$PHASE2_QUERY_BENCH_LOG"
"$ROOT/scripts/phase2_query_bench.sh"

docker run --rm --network "$NETWORK" --entrypoint /bin/sh "$MC_IMAGE" \
  -c "mc alias set local 'http://$NAME:9000' '$ACCESS_KEY' '$SECRET_KEY' >/dev/null && mc du 'local/$BUCKET'" || true
