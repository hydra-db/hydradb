#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUN_ID="${GRAPH_MINIO_MBT_RUN_ID:-$(date +%Y%m%d-%H%M%S)-$$}"
NAME="${GRAPH_MINIO_NAME:-dgraph-mbt-minio-$RUN_ID}"
NETWORK="${GRAPH_MINIO_NETWORK:-dgraph-mbt-net-$RUN_ID}"
PORT="${GRAPH_MINIO_PORT:-19004}"
ACCESS_KEY="${GRAPH_MINIO_ACCESS_KEY:-graphmbt${RUN_ID//-/}}"
SECRET_KEY="${GRAPH_MINIO_SECRET_KEY:-graph-mbt-secret-$RUN_ID}"
MINIO_IMAGE="${GRAPH_MINIO_IMAGE:-minio/minio:RELEASE.2025-07-23T15-54-02Z}"
MC_IMAGE="${GRAPH_MC_IMAGE:-minio/mc:RELEASE.2025-04-16T18-13-26Z}"
BUCKET="${GRAPH_MINIO_BUCKET:-graph-mbt-${RUN_ID}}"
PREFIX="${GRAPH_MBT_PREFIX:-formal-mbt/$RUN_ID}"
ARTIFACT_DIR="${GRAPH_MINIO_MBT_ARTIFACT_DIR:-$ROOT/target/minio-mbt/$RUN_ID}"
DATA_DIR="$ARTIFACT_DIR/data"
ENV_FILE="$ARTIFACT_DIR/minio.env"
SUCCESS=false

mkdir -p "$DATA_DIR"

mc() {
  docker run --rm --network "$NETWORK" --entrypoint /bin/sh "$MC_IMAGE" \
    -c "mc alias set local 'http://$NAME:9000' '$ACCESS_KEY' '$SECRET_KEY' >/dev/null && $*"
}

retain_failure_artifacts() {
  docker logs "$NAME" >"$ARTIFACT_DIR/minio.log" 2>&1 || true
  mc "mc find 'local/$BUCKET/$PREFIX' --name '*'" >"$ARTIFACT_DIR/objects.txt" 2>&1 || true
  cat <<EOF >&2
MinIO MBT failed. Retained artifacts:
  config: $ENV_FILE
  MinIO log: $ARTIFACT_DIR/minio.log
  object list: $ARTIFACT_DIR/objects.txt
  data: $DATA_DIR
  bucket/prefix: $BUCKET/$PREFIX
EOF
}

cleanup() {
  status=$?
  if [[ "$SUCCESS" != true ]]; then
    retain_failure_artifacts
  else
    mc "mc rm --recursive --force 'local/$BUCKET/$PREFIX'" >/dev/null 2>&1 || true
    mc "mc rb --force 'local/$BUCKET'" >/dev/null 2>&1 || true
    rm -rf "$ARTIFACT_DIR"
  fi
  docker rm -f "$NAME" >/dev/null 2>&1 || true
  docker network rm "$NETWORK" >/dev/null 2>&1 || true
  exit "$status"
}
trap cleanup EXIT

if ! command -v docker >/dev/null 2>&1; then
  echo "docker is required for the MinIO MBT replay" >&2
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
  --volume "$DATA_DIR:/data" \
  --env "MINIO_ROOT_USER=$ACCESS_KEY" \
  --env "MINIO_ROOT_PASSWORD=$SECRET_KEY" \
  "$MINIO_IMAGE" server /data >/dev/null

ready=false
for _ in {1..60}; do
  if mc "true" >/dev/null 2>&1; then
    ready=true
    break
  fi
  sleep 1
done
if [[ "$ready" != true ]]; then
  echo "MinIO did not become ready within 60 seconds" >&2
  exit 1
fi

mc "mc mb --ignore-existing 'local/$BUCKET'" >/dev/null
cat >"$ENV_FILE" <<EOF
CLOUD_PROVIDER=aws
AWS_ACCESS_KEY_ID=$ACCESS_KEY
AWS_SECRET_ACCESS_KEY=$SECRET_KEY
AWS_DEFAULT_REGION=us-east-1
AWS_ENDPOINT=http://127.0.0.1:$PORT
AWS_BUCKET=$BUCKET
AWS_ALLOW_HTTP=true
AWS_VIRTUAL_HOSTED_STYLE_REQUEST=false
EOF

# SlateDB's dotenv loading intentionally leaves exported values alone, so make
# this process agree with the generated config even if the caller has AWS vars.
export CLOUD_PROVIDER=aws
export AWS_ACCESS_KEY_ID="$ACCESS_KEY"
export AWS_SECRET_ACCESS_KEY="$SECRET_KEY"
export AWS_DEFAULT_REGION=us-east-1
export AWS_ENDPOINT="http://127.0.0.1:$PORT"
export AWS_BUCKET="$BUCKET"
export AWS_ALLOW_HTTP=true
export AWS_VIRTUAL_HOSTED_STYLE_REQUEST=false
export GRAPH_MBT_BACKEND=minio
export GRAPH_MBT_S3_ENV_FILE="$ENV_FILE"
export GRAPH_MBT_PREFIX="$PREFIX"

run_adapter() {
  local test_name="$1"
  echo "MinIO MBT: $test_name (bucket=$BUCKET prefix=$PREFIX)"
  cargo test --locked --test "$test_name" -- --test-threads=1 2>&1 | tee "$ARTIFACT_DIR/$test_name.log"
}

cd "$ROOT"
run_adapter formal_mbt
run_adapter formal_mbt_m2
run_adapter formal_mbt_m3
run_adapter formal_mbt_m4
run_adapter formal_mbt_m5
run_adapter formal_mbt_p2

SUCCESS=true
echo "MinIO MBT passed: bucket=$BUCKET prefix=$PREFIX"
