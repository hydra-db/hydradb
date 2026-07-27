#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
NAME="${GRAPH_MINIO_NAME:-dgraph-fence-minio}"
NETWORK="${GRAPH_MINIO_NETWORK:-dgraph-fence-net}"
PORT="${GRAPH_MINIO_PORT:-19003}"
ACCESS_KEY="${GRAPH_MINIO_ACCESS_KEY:-graph$(date +%s)$$}"
SECRET_KEY="${GRAPH_MINIO_SECRET_KEY:-graph-secret-$(date +%s)-$$}"
MINIO_IMAGE="${GRAPH_MINIO_IMAGE:-minio/minio:RELEASE.2025-07-23T15-54-02Z}"
MC_IMAGE="${GRAPH_MC_IMAGE:-minio/mc:RELEASE.2025-04-16T18-13-26Z}"
BUCKET="${GRAPH_MINIO_BUCKET:-graph-fence-$(date +%s)-$$}"
DB_BASE="${GRAPH_DB_BASE:-graph-minio-fence-$BUCKET}"
TIMEOUT_SECONDS="${GRAPH_WORKER_TIMEOUT:-300}"
ENV_FILE="$(mktemp)"
SIGNAL_DIR="$(mktemp -d)"
INCUMBENT_PID=""
FEATURE_ARGS=(--features chaos-harness)

cleanup() {
  rm -f "$ENV_FILE"
  rm -rf "$SIGNAL_DIR"
  if [[ -n "$INCUMBENT_PID" ]]; then
    kill "$INCUMBENT_PID" >/dev/null 2>&1 || true
  fi
  docker rm -f "$NAME" >/dev/null 2>&1 || true
  docker network rm "$NETWORK" >/dev/null 2>&1 || true
}
trap cleanup EXIT

if ! command -v docker >/dev/null 2>&1; then
  echo "docker is required for the MinIO fence takeover harness" >&2
  exit 1
fi

export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-${XDG_CACHE_HOME:-$HOME/.cache}/dgraph-target}"
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

run_with_timeout() {
  if command -v timeout >/dev/null 2>&1; then
    timeout "$TIMEOUT_SECONDS" "$@"
    return
  fi
  if command -v gtimeout >/dev/null 2>&1; then
    gtimeout "$TIMEOUT_SECONDS" "$@"
    return
  fi

  "$@" &
  local worker_pid=$!
  (
    sleep "$TIMEOUT_SECONDS"
    kill -TERM "$worker_pid" >/dev/null 2>&1 || true
  ) &
  local watchdog_pid=$!
  local status=0
  wait "$worker_pid" || status=$?
  kill "$watchdog_pid" >/dev/null 2>&1 || true
  wait "$watchdog_pid" 2>/dev/null || true
  return "$status"
}

run_worker() {
  run_with_timeout cargo run "${FEATURE_ARGS[@]}" --example fence_worker -- "$@"
}

cd "$ROOT"
DATA_PATH="$DB_BASE/data"

echo "graph MinIO SlateDB fence takeover: bucket=$BUCKET data=$DATA_PATH"
run_worker incumbent "$ENV_FILE" "$DATA_PATH" "$SIGNAL_DIR" &
INCUMBENT_PID=$!
run_worker takeover "$ENV_FILE" "$DATA_PATH" "$SIGNAL_DIR"
wait "$INCUMBENT_PID"
INCUMBENT_PID=""
docker restart "$NAME" >/dev/null
run_worker reader "$ENV_FILE" "$DATA_PATH" "$SIGNAL_DIR"

docker run --rm --network "$NETWORK" --entrypoint /bin/sh "$MC_IMAGE" \
  -c "mc alias set local 'http://$NAME:9000' '$ACCESS_KEY' '$SECRET_KEY' >/dev/null && mc find 'local/$BUCKET' --name '*' --maxdepth 4" | sed -n '1,80p'

echo "graph MinIO fence takeover passed"
