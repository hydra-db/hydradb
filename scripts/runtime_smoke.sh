#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

PYTHON="${PYTHON:-python3}"

# This script calls cargo directly, so it never inherits the justfile's exports.
# graph-node's async query futures exceed the default thread stack: without this
# the node builds, serves /readyz, and then aborts with a stack overflow on the
# first query. Matches justfile:18. An already-exported value wins.
export RUST_MIN_STACK="${RUST_MIN_STACK:-33554432}"

cargo build --locked --features server-runtime --bin graph-node

ROOT="${GRAPH_RUNTIME_SMOKE_ROOT:-/tmp/sgk-runtime-smoke}"
if [[ "$ROOT" != /tmp/sgk-runtime-smoke ]]; then
  echo "GRAPH_RUNTIME_SMOKE_ROOT must be /tmp/sgk-runtime-smoke" >&2
  exit 2
fi
rm -rf -- "$ROOT"
mkdir -p "$ROOT/store" "$ROOT/node-data-cache"

TOKEN="runtime-smoke-auth-token-32-characters-long"
printf '%s\n' "$TOKEN" >"$ROOT/auth-token"

export CLOUD_PROVIDER=local
export LOCAL_PATH="$ROOT/store"
export GRAPH_NAMESPACE=smoke
export GRAPH_ID=default
export GRAPH_CELL_ID=cell-0
export GRAPH_CELLS=cell-0
export GRAPH_DATA_PATH=data
export GRAPH_ALLOW_PLAINTEXT=true
export GRAPH_AUTH_TOKEN_FILE="$ROOT/auth-token"
export GRAPH_DATA_CACHE_BYTES=67108864
export GRAPH_BOLT_NODE_ADDRESSES=node-0=127.0.0.1:17687
export GRAPH_NODE_ID=node-0
export GRAPH_BOLT_ADDR=127.0.0.1:17687
export GRAPH_HTTP_ADDR=127.0.0.1:18443
export GRAPH_ADMIN_ADDR=127.0.0.1:19091
export GRAPH_ADVERTISED_BOLT_ADDR=127.0.0.1:17687
export GRAPH_DATA_CACHE_DIR="$ROOT/node-data-cache"
export RUST_LOG=warn

node_pid=""
cleanup() {
  if [[ -n "${node_pid:-}" ]]; then
    kill -TERM "$node_pid" 2>/dev/null || true
    wait "$node_pid" 2>/dev/null || true
  fi
}
trap cleanup EXIT

target/debug/graph-node >"$ROOT/node.log" 2>&1 &
node_pid=$!
for _ in $(seq 1 160); do
  if curl -fsS http://127.0.0.1:19091/readyz >/dev/null 2>&1; then
    break
  fi
  if ! kill -0 "$node_pid" 2>/dev/null; then
    cat "$ROOT/node.log" >&2
    exit 1
  fi
  sleep 0.25
done
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

    scoped = {
        "default.scope1.dGVuYW50LWE.Y29sbGVjdGlvbi1h": 3,
        "default.scope1.dGVuYW50LWE.Y29sbGVjdGlvbi1i": 4,
    }
    for database, destination in scoped.items():
        with driver.session(database=database) as session:
            session.run(
                "CREATE (a {id: 1})-[:FOLLOWS]->(b {id: $destination})",
                destination=destination,
            ).consume()
    for database, destination in scoped.items():
        with driver.session(database=database) as session:
            rows = list(session.run(
                "MATCH (a {id: 1})-[:FOLLOWS]->(b) RETURN b.id AS id"
            ))
            assert [row["id"] for row in rows] == [destination], (database, rows)
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
echo runtime-smoke-ok
