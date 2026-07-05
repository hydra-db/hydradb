#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
NAME="${PHASE0_MINIO_NAME:-dgraph-phase0-write-minio}"
NETWORK="${PHASE0_MINIO_NETWORK:-dgraph-phase0-write-net}"
PORT="${PHASE0_MINIO_PORT:-19003}"
ACCESS_KEY="${PHASE0_MINIO_ACCESS_KEY:-phase0$(date +%s)$$}"
SECRET_KEY="${PHASE0_MINIO_SECRET_KEY:-phase0-secret-$(date +%s)-$$}"
MINIO_IMAGE="${PHASE0_MINIO_IMAGE:-minio/minio:RELEASE.2025-07-23T15-54-02Z}"
MC_IMAGE="${PHASE0_MC_IMAGE:-minio/mc:RELEASE.2025-04-16T18-13-26Z}"
BUCKET="${PHASE0_MINIO_BUCKET:-phase0-write-$(date +%s)-$$}"
ENV_FILE="$(mktemp)"
RESULTS="${PHASE0_WRITE_PROFILE_RESULTS:-$ROOT/bench-results/phase0_write_profile_minio.csv}"
MODES="${PHASE0_WRITE_PROFILE_MODES:-log bulk ingest log-materialize log-drain}"
INDEX_POLICY="${PHASE0_WRITE_PROFILE_INDEX_POLICY:-full}"
AWAIT_DURABLE="${PHASE0_WRITE_PROFILE_AWAIT_DURABLE:-true}"

cleanup() {
  rm -f "$ENV_FILE"
  if [[ "${PHASE0_KEEP_MINIO:-0}" != "1" ]]; then
    docker rm -f "$NAME" >/dev/null 2>&1 || true
    docker network rm "$NETWORK" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

if ! command -v docker >/dev/null 2>&1; then
  echo "docker is required for the Phase 0 MinIO write profile" >&2
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

mkdir -p "$(dirname "$RESULTS")"
: >"$RESULTS"
first=1
echo "phase0 MinIO write profile: bucket=$BUCKET endpoint=http://127.0.0.1:$PORT results=$RESULTS modes=$MODES index_policy=$INDEX_POLICY await_durable=$AWAIT_DURABLE"
for mode in $MODES; do
  echo "phase0 MinIO write profile: mode=$mode" >&2
  output="$(
    cd "$ROOT"
    env \
      PHASE0_WRITE_PROFILE_OBJECT_ENV="$ENV_FILE" \
      PHASE0_WRITE_PROFILE_MODE="$mode" \
      PHASE0_WRITE_PROFILE_INDEX_POLICY="$INDEX_POLICY" \
      PHASE0_WRITE_PROFILE_AWAIT_DURABLE="$AWAIT_DURABLE" \
      cargo run --release --locked --example phase0_write_profile
  )"
  if [[ "$first" == "1" ]]; then
    printf '%s\n' "$output" >>"$RESULTS"
    first=0
  else
    printf '%s\n' "$output" | tail -n +2 >>"$RESULTS"
  fi
done

cat "$RESULTS"
docker run --rm --network "$NETWORK" --entrypoint /bin/sh "$MC_IMAGE" \
  -c "mc alias set local 'http://$NAME:9000' '$ACCESS_KEY' '$SECRET_KEY' >/dev/null && mc du 'local/$BUCKET'" || true
