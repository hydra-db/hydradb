#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
NAME="${PHASE0_MINIO_NAME:-dgraph-phase0-chaos-minio}"
NETWORK="${PHASE0_MINIO_NETWORK:-dgraph-phase0-chaos-net}"
PORT="${PHASE0_MINIO_PORT:-19001}"
ACCESS_KEY="${PHASE0_MINIO_ACCESS_KEY:-minioadmin}"
SECRET_KEY="${PHASE0_MINIO_SECRET_KEY:-minioadmin}"
BUCKET="${PHASE0_MINIO_BUCKET:-phase0-chaos-$(date +%s)-$$}"
DB_PATH="${PHASE0_DB_PATH:-phase0-minio-chaos-$BUCKET}"
OPS="${PHASE0_STRESS_OPS:-5000}"
TIMEOUT_SECONDS="${PHASE0_WORKER_TIMEOUT:-300}"
ENV_FILE="$(mktemp)"
FEATURE_ARGS=()

cleanup() {
  rm -f "$ENV_FILE"
  docker rm -f "$NAME" >/dev/null 2>&1 || true
  docker network rm "$NETWORK" >/dev/null 2>&1 || true
}
trap cleanup EXIT

if ! command -v docker >/dev/null 2>&1; then
  echo "docker is required for the Phase 0 MinIO chaos harness" >&2
  exit 1
fi

if [[ "${PHASE0_GRAPHBLAS:-0}" == "1" ]]; then
  FEATURE_ARGS=(--features graphblas)
  export PHASE0_MATRIX_KERNEL="${PHASE0_MATRIX_KERNEL:-graphblas}"
fi

export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-/home/vishal/.cache/dgraph-phase0-target}"
export PHASE0_OP_DELAY_MICROS="${PHASE0_OP_DELAY_MICROS:-1000}"

docker rm -f "$NAME" >/dev/null 2>&1 || true
docker network rm "$NETWORK" >/dev/null 2>&1 || true
docker network create "$NETWORK" >/dev/null
docker run \
  --detach \
  --name "$NAME" \
  --network "$NETWORK" \
  --publish "$PORT:9000" \
  --env "MINIO_ROOT_USER=$ACCESS_KEY" \
  --env "MINIO_ROOT_PASSWORD=$SECRET_KEY" \
  minio/minio:latest server /data >/dev/null

for _ in {1..60}; do
  if docker run --rm --network "$NETWORK" --entrypoint /bin/sh minio/mc:latest \
    -c "mc alias set local 'http://$NAME:9000' '$ACCESS_KEY' '$SECRET_KEY' >/dev/null" >/dev/null 2>&1; then
    break
  fi
  sleep 1
done

docker run --rm --network "$NETWORK" --entrypoint /bin/sh minio/mc:latest \
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

run_worker_for() {
  local cell_id="$1"
  local src_id="$2"
  shift 2
  PHASE0_CELL_ID="$cell_id" PHASE0_SRC_ID="$src_id" \
    timeout "$TIMEOUT_SECONDS" cargo run "${FEATURE_ARGS[@]}" --example phase0_stress_worker -- "$@"
}

cd "$ROOT"
echo "phase0 MinIO chaos: bucket=$BUCKET db=$DB_PATH ops=$OPS"

run_worker_for reddit-home 1 writer "$ENV_FILE" "$DB_PATH" node-a "$OPS" 100000 &
writer_pid=$!
sleep "${PHASE0_PAUSE_AFTER_SECONDS:-2}"
docker pause "$NAME" >/dev/null
sleep "${PHASE0_PAUSE_SECONDS:-3}"
docker unpause "$NAME" >/dev/null
wait "$writer_pid" || true

run_worker_for reddit-home 1 writer "$ENV_FILE" "$DB_PATH" node-a "$OPS" 100000
run_worker_for reddit-home 1 artifact "$ENV_FILE" "$DB_PATH" node-a 1 1

docker restart "$NAME" >/dev/null
run_worker_for reddit-home 1 reader "$ENV_FILE" "$DB_PATH" reader-empty-cache 1 1

docker run --rm --network "$NETWORK" --entrypoint /bin/sh minio/mc:latest \
  -c "mc alias set local 'http://$NAME:9000' '$ACCESS_KEY' '$SECRET_KEY' >/dev/null && mc find 'local/$BUCKET' --name '*' --maxdepth 4" | sed -n '1,80p'

echo "phase0 MinIO chaos passed"
