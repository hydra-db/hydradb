#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
NAME="${GRAPH_MINIO_NAME:-dgraph-chaos-minio}"
NETWORK="${GRAPH_MINIO_NETWORK:-dgraph-chaos-net}"
PORT="${GRAPH_MINIO_PORT:-19001}"
ACCESS_KEY="${GRAPH_MINIO_ACCESS_KEY:-graph$(date +%s)$$}"
SECRET_KEY="${GRAPH_MINIO_SECRET_KEY:-graph-secret-$(date +%s)-$$}"
MINIO_IMAGE="${GRAPH_MINIO_IMAGE:-minio/minio:RELEASE.2025-07-23T15-54-02Z}"
MC_IMAGE="${GRAPH_MC_IMAGE:-minio/mc:RELEASE.2025-04-16T18-13-26Z}"
BUCKET="${GRAPH_MINIO_BUCKET:-graph-chaos-$(date +%s)-$$}"
DB_PATH="${GRAPH_DB_PATH:-graph-minio-chaos-$BUCKET}"
OPS="${GRAPH_STRESS_OPS:-5000}"
TIMEOUT_SECONDS="${GRAPH_WORKER_TIMEOUT:-300}"
KILL_OPS="${GRAPH_KILL_OPS:-$OPS}"
KILL_AFTER_SECONDS="${GRAPH_KILL_AFTER_SECONDS:-1}"
VERIFY_HOPS="${GRAPH_VERIFY_HOPS:-3}"
VERIFY_ROOTS="${GRAPH_VERIFY_ROOTS:-8}"
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

if [[ "${GRAPH_ENABLE_GRAPHBLAS:-0}" == "1" ]]; then
  FEATURE_ARGS=(--features graphblas)
  export GRAPH_MATRIX_KERNEL="${GRAPH_MATRIX_KERNEL:-graphblas}"
fi

export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-${XDG_CACHE_HOME:-$HOME/.cache}/dgraph-target}"
export GRAPH_OP_DELAY_MICROS="${GRAPH_OP_DELAY_MICROS:-1000}"
export GRAPH_BULK_CHUNK="${GRAPH_BULK_CHUNK:-50}"
export GRAPH_WRITER_BATCH="${GRAPH_WRITER_BATCH:-1024}"
export GRAPH_SEGMENT_CHUNK="${GRAPH_SEGMENT_CHUNK:-128}"
export GRAPH_VERIFY_HOPS="$VERIFY_HOPS"
export GRAPH_VERIFY_ROOTS="$VERIFY_ROOTS"
WORKER_BIN="$CARGO_TARGET_DIR/debug/examples/stress_worker"

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

run_worker_for() {
  local cell_id="$1"
  local src_id="$2"
  shift 2
  GRAPH_CELL_ID="$cell_id" GRAPH_SRC_ID="$src_id" GRAPH_INDEX_POLICY="${GRAPH_INDEX_POLICY:-}" \
    timeout "$TIMEOUT_SECONDS" "$WORKER_BIN" "$@"
}

start_worker_group_for() {
  local cell_id="$1"
  local src_id="$2"
  shift 2
  setsid env GRAPH_CELL_ID="$cell_id" GRAPH_SRC_ID="$src_id" GRAPH_INDEX_POLICY="${GRAPH_INDEX_POLICY:-}" \
    timeout "$TIMEOUT_SECONDS" "$WORKER_BIN" "$@" &
  RUN_WORKER_PID="$!"
}

verify_cell() {
  local cell_id="$1"
  local src_id="$2"
  run_worker_for "$cell_id" "$src_id" verify "$ENV_FILE" "$DB_PATH" "verify-$cell_id" 1 1
}

kill_mode_then_recover() {
  local cell_id="$1"
  local src_id="$2"
  local mode="$3"
  local recover_mode="${4:-$mode}"
  local start="${5:-900000}"
  echo "graph MinIO kill/recover: cell=$cell_id mode=$mode recover=$recover_mode"
  start_worker_group_for "$cell_id" "$src_id" "$mode" "$ENV_FILE" "$DB_PATH" "kill-$mode" "$KILL_OPS" "$start"
  kill_pid="$RUN_WORKER_PID"
  sleep "$KILL_AFTER_SECONDS"
  kill -9 -- "-$kill_pid" >/dev/null 2>&1 || true
  wait "$kill_pid" >/dev/null 2>&1 || true
  run_worker_for "$cell_id" "$src_id" "$recover_mode" "$ENV_FILE" "$DB_PATH" "recover-$mode" "$KILL_OPS" "$start"
  verify_cell "$cell_id" "$src_id"
}

pause_mode_then_recover() {
  local cell_id="$1"
  local src_id="$2"
  local mode="$3"
  local recover_mode="${4:-$mode}"
  local start="${5:-900000}"
  echo "graph MinIO pause/recover: cell=$cell_id mode=$mode recover=$recover_mode"
  run_worker_for "$cell_id" "$src_id" "$mode" "$ENV_FILE" "$DB_PATH" "pause-$mode" "$KILL_OPS" "$start" &
  local pid="$!"
  sleep "${GRAPH_PAUSE_AFTER_SECONDS:-2}"
  docker pause "$NAME" >/dev/null
  sleep "${GRAPH_PAUSE_SECONDS:-3}"
  docker unpause "$NAME" >/dev/null
  wait "$pid" || true
  run_worker_for "$cell_id" "$src_id" "$recover_mode" "$ENV_FILE" "$DB_PATH" "recover-pause-$mode" "$KILL_OPS" "$start"
  verify_cell "$cell_id" "$src_id"
}

cd "$ROOT"
cargo build "${FEATURE_ARGS[@]}" --example stress_worker >/dev/null
echo "graph MinIO chaos: bucket=$BUCKET db=$DB_PATH ops=$OPS"

pause_mode_then_recover reddit-home 1 writer writer 100000
run_worker_for reddit-home 1 rollup "$ENV_FILE" "$DB_PATH" node-a 1 1
verify_cell reddit-home 1

kill_mode_then_recover reddit-kill-batch 9 batch batch 900000

run_worker_for reddit-kill-matrix 10 batch "$ENV_FILE" "$DB_PATH" seed-matrix "$KILL_OPS" 1000000
kill_mode_then_recover reddit-kill-matrix 10 matrix matrix 1000000

run_worker_for reddit-kill-supernode 11 batch "$ENV_FILE" "$DB_PATH" seed-supernode "$KILL_OPS" 1100000
kill_mode_then_recover reddit-kill-supernode 11 supernode supernode 1100000

run_worker_for reddit-kill-rollup 12 batch "$ENV_FILE" "$DB_PATH" seed-rollup "$KILL_OPS" 1200000
kill_mode_then_recover reddit-kill-rollup 12 rollup rollup 1200000

run_worker_for reddit-kill-delta-gc 13 batch "$ENV_FILE" "$DB_PATH" seed-delta-gc "$KILL_OPS" 1300000
run_worker_for reddit-kill-delta-gc 13 rollup "$ENV_FILE" "$DB_PATH" seed-delta-gc-rollup 1 1
kill_mode_then_recover reddit-kill-delta-gc 13 delta-gc delta-gc 1300000

GRAPH_INDEX_POLICY=outbound-only kill_mode_then_recover reddit-kill-segment 15 segment segment 1500000
GRAPH_INDEX_POLICY=outbound-only run_worker_for reddit-kill-segment 15 segment-delete "$ENV_FILE" "$DB_PATH" seed-segment-delete "$KILL_OPS" 1500000
GRAPH_INDEX_POLICY=outbound-only kill_mode_then_recover reddit-kill-segment 15 segment-compact segment-compact 1500000
GRAPH_INDEX_POLICY=outbound-only kill_mode_then_recover reddit-kill-segment-gc 16 segment segment 1600000
GRAPH_INDEX_POLICY=outbound-only run_worker_for reddit-kill-segment-gc 16 segment-delete "$ENV_FILE" "$DB_PATH" seed-segment-gc-delete "$KILL_OPS" 1600000
GRAPH_INDEX_POLICY=outbound-only kill_mode_then_recover reddit-kill-segment-gc 16 segment-gc segment-gc 1600000

run_worker_for reddit-pause-rollup 14 batch "$ENV_FILE" "$DB_PATH" seed-pause-rollup "$KILL_OPS" 1400000
pause_mode_then_recover reddit-pause-rollup 14 rollup rollup 1400000

docker restart "$NAME" >/dev/null
run_worker_for reddit-home 1 reader "$ENV_FILE" "$DB_PATH" reader-empty-cache 1 1
verify_cell reddit-home 1
verify_cell reddit-kill-batch 9
verify_cell reddit-kill-matrix 10
verify_cell reddit-kill-supernode 11
verify_cell reddit-kill-rollup 12
verify_cell reddit-kill-delta-gc 13
GRAPH_INDEX_POLICY=outbound-only verify_cell reddit-kill-segment 15
GRAPH_INDEX_POLICY=outbound-only verify_cell reddit-kill-segment-gc 16
verify_cell reddit-pause-rollup 14

scripts/minio_fence_takeover.sh

docker run --rm --network "$NETWORK" --entrypoint /bin/sh "$MC_IMAGE" \
  -c "mc alias set local 'http://$NAME:9000' '$ACCESS_KEY' '$SECRET_KEY' >/dev/null && mc find 'local/$BUCKET' --name '*' --maxdepth 4" | sed -n '1,80p'

echo "graph MinIO chaos passed"
