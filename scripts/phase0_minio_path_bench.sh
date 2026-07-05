#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
NAME="${PHASE0_MINIO_NAME:-dgraph-phase0-bench-minio}"
NETWORK="${PHASE0_MINIO_NETWORK:-dgraph-phase0-bench-net}"
PORT="${PHASE0_MINIO_PORT:-19002}"
ACCESS_KEY="${PHASE0_MINIO_ACCESS_KEY:-phase0$(date +%s)$$}"
SECRET_KEY="${PHASE0_MINIO_SECRET_KEY:-phase0-secret-$(date +%s)-$$}"
MINIO_IMAGE="${PHASE0_MINIO_IMAGE:-minio/minio:RELEASE.2025-07-23T15-54-02Z}"
MC_IMAGE="${PHASE0_MC_IMAGE:-minio/mc:RELEASE.2025-04-16T18-13-26Z}"
BUCKET="${PHASE0_MINIO_BUCKET:-phase0-bench-$(date +%s)-$$}"
ENV_FILE="$(mktemp)"

cleanup() {
  rm -f "$ENV_FILE"
  if [[ "${PHASE0_KEEP_MINIO:-0}" != "1" ]]; then
    docker rm -f "$NAME" >/dev/null 2>&1 || true
    docker network rm "$NETWORK" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

if ! command -v docker >/dev/null 2>&1; then
  echo "docker is required for the Phase 0 MinIO benchmark" >&2
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

export PHASE0_BENCH_OBJECT_ENV="$ENV_FILE"
export PHASE0_BENCH_RESULTS="${PHASE0_BENCH_RESULTS:-$ROOT/bench-results/phase0_path_bench_minio.csv}"
export PHASE0_BENCH_LOG="${PHASE0_BENCH_LOG:-$ROOT/bench-results/phase0_path_bench_minio.log}"
export PHASE0_BENCH_FANOUTS="${PHASE0_BENCH_FANOUTS:-50,100,1000,10000,50000,100000}"
export PHASE0_BENCH_HOPS="${PHASE0_BENCH_HOPS:-1,3,5,10,12}"
export PHASE0_BENCH_DATA_HOPS="${PHASE0_BENCH_DATA_HOPS:-12}"
export PHASE0_BENCH_HOT_ITERS="${PHASE0_BENCH_HOT_ITERS:-5}"
export PHASE0_BENCH_WRITE_SAMPLES="${PHASE0_BENCH_WRITE_SAMPLES:-32}"
export PHASE0_BENCH_WRITE_MICROBATCH_SIZE="${PHASE0_BENCH_WRITE_MICROBATCH_SIZE:-1024}"
export PHASE0_BENCH_WRITE_MICROBATCH_COUNT="${PHASE0_BENCH_WRITE_MICROBATCH_COUNT:-3}"

echo "phase0 MinIO path bench: bucket=$BUCKET endpoint=http://127.0.0.1:$PORT results=$PHASE0_BENCH_RESULTS log=$PHASE0_BENCH_LOG"
"$ROOT/scripts/phase0_path_bench.sh"

docker run --rm --network "$NETWORK" --entrypoint /bin/sh "$MC_IMAGE" \
  -c "mc alias set local 'http://$NAME:9000' '$ACCESS_KEY' '$SECRET_KEY' >/dev/null && mc du 'local/$BUCKET'" || true
