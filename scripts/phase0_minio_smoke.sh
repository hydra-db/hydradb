#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
NAME="${PHASE0_MINIO_NAME:-dgraph-phase0-minio}"
NETWORK="${PHASE0_MINIO_NETWORK:-dgraph-phase0-net}"
PORT="${PHASE0_MINIO_PORT:-19000}"
ACCESS_KEY="${PHASE0_MINIO_ACCESS_KEY:-minioadmin}"
SECRET_KEY="${PHASE0_MINIO_SECRET_KEY:-minioadmin}"
BUCKET="${PHASE0_MINIO_BUCKET:-phase0-graph-$(date +%s)-$$}"
DB_PATH="${PHASE0_DB_PATH:-phase0-minio-smoke-$BUCKET}"
ENV_FILE="$(mktemp)"

cleanup() {
  rm -f "$ENV_FILE"
  docker rm -f "$NAME" >/dev/null 2>&1 || true
  docker network rm "$NETWORK" >/dev/null 2>&1 || true
}
trap cleanup EXIT

if ! command -v docker >/dev/null 2>&1; then
  echo "docker is required for the Phase 0 MinIO smoke" >&2
  exit 1
fi

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

features=()
if [[ "${PHASE0_GRAPHBLAS:-0}" == "1" ]]; then
  features=(--features graphblas)
  export PHASE0_MATRIX_KERNEL="${PHASE0_MATRIX_KERNEL:-graphblas}"
fi

cd "$ROOT"
cargo run "${features[@]}" --example phase0_object_store_smoke -- "$ENV_FILE" "$DB_PATH"
docker run --rm --network "$NETWORK" --entrypoint /bin/sh minio/mc:latest \
  -c "mc alias set local 'http://$NAME:9000' '$ACCESS_KEY' '$SECRET_KEY' >/dev/null && mc find 'local/$BUCKET' --name '*' --maxdepth 4" | sed -n '1,40p'
