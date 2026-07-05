#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
NAME="${PHASE0_MINIO_NAME:-dgraph-phase0-fence-minio}"
NETWORK="${PHASE0_MINIO_NETWORK:-dgraph-phase0-fence-net}"
PORT="${PHASE0_MINIO_PORT:-19003}"
ACCESS_KEY="${PHASE0_MINIO_ACCESS_KEY:-phase0$(date +%s)$$}"
SECRET_KEY="${PHASE0_MINIO_SECRET_KEY:-phase0-secret-$(date +%s)-$$}"
MINIO_IMAGE="${PHASE0_MINIO_IMAGE:-minio/minio:RELEASE.2025-07-23T15-54-02Z}"
MC_IMAGE="${PHASE0_MC_IMAGE:-minio/mc:RELEASE.2025-04-16T18-13-26Z}"
BUCKET="${PHASE0_MINIO_BUCKET:-phase0-fence-$(date +%s)-$$}"
DB_BASE="${PHASE0_DB_BASE:-phase0-minio-fence-$BUCKET}"
TIMEOUT_SECONDS="${PHASE0_WORKER_TIMEOUT:-300}"
LEASE_TTL_MS="${PHASE0_LEASE_TTL_MS:-5000}"
ENV_FILE="$(mktemp)"
FEATURE_ARGS=(--features chaos-harness)

cleanup() {
  rm -f "$ENV_FILE"
  docker rm -f "$NAME" >/dev/null 2>&1 || true
  docker network rm "$NETWORK" >/dev/null 2>&1 || true
}
trap cleanup EXIT

if ! command -v docker >/dev/null 2>&1; then
  echo "docker is required for the Phase 0 MinIO fence takeover harness" >&2
  exit 1
fi

export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-${XDG_CACHE_HOME:-$HOME/.cache}/dgraph-phase0-target}"
export PHASE0_LEASE_TTL_MS="$LEASE_TTL_MS"

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

run_worker() {
  timeout "$TIMEOUT_SECONDS" cargo run "${FEATURE_ARGS[@]}" --example phase0_fence_worker -- "$@"
}

cd "$ROOT"
DATA_PATH="$DB_BASE/data"
CONTROL_PATH="$DB_BASE/control"

echo "phase0 MinIO fence takeover: bucket=$BUCKET data=$DATA_PATH control=$CONTROL_PATH ttl_ms=$LEASE_TTL_MS"
run_worker init "$ENV_FILE" "$DATA_PATH" "$CONTROL_PATH"
sleep "$(awk "BEGIN { printf \"%.3f\", (($LEASE_TTL_MS + 150) / 1000) }")"
run_worker takeover "$ENV_FILE" "$DATA_PATH" "$CONTROL_PATH"
docker restart "$NAME" >/dev/null
run_worker stale-probe "$ENV_FILE" "$DATA_PATH" "$CONTROL_PATH"
run_worker reader "$ENV_FILE" "$DATA_PATH" "$CONTROL_PATH"

docker run --rm --network "$NETWORK" --entrypoint /bin/sh "$MC_IMAGE" \
  -c "mc alias set local 'http://$NAME:9000' '$ACCESS_KEY' '$SECRET_KEY' >/dev/null && mc find 'local/$BUCKET' --name '*' --maxdepth 4" | sed -n '1,80p'

echo "phase0 MinIO fence takeover passed"
