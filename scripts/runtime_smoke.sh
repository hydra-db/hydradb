#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

PYTHON="${PYTHON:-python3}"

cargo build --locked --features server-runtime --bin graph-controller --bin graph-node

ROOT="${GRAPH_RUNTIME_SMOKE_ROOT:-/tmp/sgk-runtime-smoke}"
if [[ "$ROOT" != /tmp/sgk-runtime-smoke ]]; then
  echo "GRAPH_RUNTIME_SMOKE_ROOT must be /tmp/sgk-runtime-smoke" >&2
  exit 2
fi
rm -rf -- "$ROOT"
mkdir -p "$ROOT/store" "$ROOT/controller-cache-a" "$ROOT/controller-cache-b" "$ROOT/node-data-cache"

TOKEN="runtime-smoke-auth-token-32-characters-long"
printf '%s\n' "$TOKEN" >"$ROOT/auth-token"

export CLOUD_PROVIDER=local
export LOCAL_PATH="$ROOT/store"
export GRAPH_NAMESPACE=smoke
export GRAPH_ID=default
export GRAPH_CELL_ID=cell-0
export GRAPH_CELLS=cell-0
export GRAPH_CONTROL_PATH=control
export GRAPH_DATA_PATH=data
export GRAPH_ALLOW_PLAINTEXT=true
export GRAPH_INTERNAL_ALLOW_PLAINTEXT=true
export GRAPH_AUTH_TOKEN_FILE="$ROOT/auth-token"
export GRAPH_LEASE_TTL_MS=10000
export GRAPH_LEASE_RENEW_INTERVAL_MS=1000
export GRAPH_SHARD_REFRESH_INTERVAL_MS=500
export GRAPH_HEARTBEAT_TTL_MS=5000
export GRAPH_CONTROLLER_INTERVAL_MS=250
export GRAPH_CONTROL_CACHE_BYTES=16777216
export GRAPH_DATA_CACHE_BYTES=67108864
export GRAPH_RUNTIME_LEASE_TTL_MS=2000
export GRAPH_RUNTIME_LEASE_RENEW_INTERVAL_MS=250
export GRAPH_CONTROL_RPC_ENDPOINT=127.0.0.1:19443
export GRAPH_CONTROL_RPC_SERVER_NAME=localhost
export GRAPH_BOLT_NODE_ADDRESSES=node-0=127.0.0.1:17687
export RUST_LOG=warn

start_controller() {
  local cache_dir="$1"
  local log_file="$2"
  GRAPH_CONTROL_RPC_ADDR=127.0.0.1:19443 \
  GRAPH_ADMIN_ADDR=127.0.0.1:19090 \
  GRAPH_CONTROL_CACHE_DIR="$cache_dir" \
  target/debug/graph-controller >"$log_file" 2>&1 &
  controller_pid=$!
}

wait_ready() {
  local url="$1"
  local pid="$2"
  local log_file="$3"
  for _ in $(seq 1 160); do
    if curl -fsS "$url" >/dev/null 2>&1; then
      return 0
    fi
    if ! kill -0 "$pid" 2>/dev/null; then
      cat "$log_file" >&2
      return 1
    fi
    sleep 0.25
  done
  cat "$log_file" >&2
  return 1
}

controller_pid=""
node_pid=""
cleanup() {
  for pid in "${node_pid:-}" "${controller_pid:-}"; do
    if [[ -n "$pid" ]]; then
      kill -TERM "$pid" 2>/dev/null || true
      wait "$pid" 2>/dev/null || true
    fi
  done
}
trap cleanup EXIT

start_controller "$ROOT/controller-cache-a" "$ROOT/controller-a.log"
wait_ready http://127.0.0.1:19090/readyz "$controller_pid" "$ROOT/controller-a.log"

GRAPH_NODE_ID=node-0 \
GRAPH_BOLT_ADDR=127.0.0.1:17687 \
GRAPH_HTTP_ADDR=127.0.0.1:18443 \
GRAPH_ADMIN_ADDR=127.0.0.1:19091 \
GRAPH_ADVERTISED_BOLT_ADDR=127.0.0.1:17687 \
GRAPH_DATA_CACHE_DIR="$ROOT/node-data-cache" \
target/debug/graph-node >"$ROOT/node.log" 2>&1 &
node_pid=$!
wait_ready http://127.0.0.1:19091/readyz "$node_pid" "$ROOT/node.log"

kill -TERM "$controller_pid"
wait "$controller_pid"
controller_pid=""
start_controller "$ROOT/controller-cache-b" "$ROOT/controller-b.log"
wait_ready http://127.0.0.1:19090/readyz "$controller_pid" "$ROOT/controller-b.log"
sleep 1.5
curl -fsS http://127.0.0.1:19091/readyz >/dev/null
curl -fsS http://127.0.0.1:19091/metrics | grep -q graph_runtime_ready

if ! "$PYTHON" -c 'import neo4j' >/dev/null 2>&1; then
  echo "python neo4j package is required for runtime smoke" >&2
  exit 2
fi
GRAPH_RUNTIME_SMOKE_TOKEN="$TOKEN" "$PYTHON" - <<'PY'
import os
from neo4j import GraphDatabase

with GraphDatabase.driver(
    "bolt://127.0.0.1:17687",
    auth=("neo4j", os.environ["GRAPH_RUNTIME_SMOKE_TOKEN"]),
) as driver:
    driver.verify_connectivity()
    with driver.session(database="default") as session:
        session.run("CREATE (a {id: 1})-[:FOLLOWS]->(b {id: 2})").consume()
        row = session.run(
            "MATCH (a {id: 1})-[:FOLLOWS]->(b) RETURN b.id AS id"
        ).single(strict=True)
        assert row["id"] == 2, row
PY

http_result="$(curl -fsS -X POST \
  http://127.0.0.1:18443/v1/graphs/default/query \
  -H "Authorization: Bearer $TOKEN" \
  -H 'X-Graph-Namespace: smoke' \
  -H 'Content-Type: application/json' \
  --data '{"cell_id":"cell-0","query":"MATCH (a {id: 1})-[:FOLLOWS]->(b) RETURN b.id AS id"}')"
grep -q '"value":2' <<<"$http_result"

kill -TERM "$node_pid"
wait "$node_pid"
node_pid=""
kill -TERM "$controller_pid"
wait "$controller_pid"
controller_pid=""
echo runtime-smoke-ok
