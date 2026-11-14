#!/usr/bin/env bash
# Compare the legacy and experimental routes through real Bolt and HTTP nodes.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
PYTHON="${PYTHON:-python3}"
MODE="${GRAPH_CYPHER_ENGINE_AB_MODE:-correctness}"
ITERATIONS="${GRAPH_CYPHER_ENGINE_AB_ITERATIONS:-1}"
RUN_ROOT="${GRAPH_CYPHER_ENGINE_AB_ROOT:-/tmp/hydradb-cypher-engine-ab}"
RESULTS_ROOT="${GRAPH_CYPHER_ENGINE_AB_RESULTS:-$PROJECT_ROOT/bench-results/cypher_engine_ab}"
TOKEN="cypher-engine-ab-auth-token-32-characters-long"

case "$MODE" in correctness|benchmark) ;; *) echo "GRAPH_CYPHER_ENGINE_AB_MODE must be correctness or benchmark" >&2; exit 2 ;; esac
[[ "$ITERATIONS" =~ ^[1-9][0-9]*$ ]] || { echo "GRAPH_CYPHER_ENGINE_AB_ITERATIONS must be positive" >&2; exit 2; }
[[ "$RUN_ROOT" == /tmp/hydradb-cypher-engine-ab ]] || { echo "GRAPH_CYPHER_ENGINE_AB_ROOT must be /tmp/hydradb-cypher-engine-ab" >&2; exit 2; }

# This script calls cargo itself.  Mirror justfile's native exports so direct
# invocation is safe on macOS as well as when called through `just`.
export RUST_MIN_STACK="${RUST_MIN_STACK:-33554432}"
if [[ "$(uname -s)" == Darwin ]]; then
  brew_prefix="$(brew --prefix 2>/dev/null || printf '%s' /opt/homebrew)"
  export BINDGEN_EXTRA_CLANG_ARGS="${BINDGEN_EXTRA_CLANG_ARGS:--I${brew_prefix}/include}"
  export LIBRARY_PATH="${LIBRARY_PATH:-${brew_prefix}/lib}"
fi

if ! "$PYTHON" -c 'import neo4j' >/dev/null 2>&1; then
  echo "Python package neo4j is required (set PYTHON to a venv interpreter)." >&2
  exit 2
fi

cd "$PROJECT_ROOT"
build_profile=debug
build_args=(build --locked --features server-runtime,experimental-cypher-engine --bin graph-node)
if [[ "$MODE" == benchmark ]]; then
  build_profile=release
  build_args+=(--release)
fi
cargo "${build_args[@]}"
# Developers may configure Cargo's target directory globally.  Ask Cargo for
# the resolved location instead of assuming the checkout owns target/.
node_binary="$(cargo metadata --no-deps --format-version=1 | "$PYTHON" -c 'import json, sys; print(json.load(sys.stdin)["target_directory"])')/$build_profile/graph-node"
[[ -x "$node_binary" ]] || { echo "graph-node build produced no executable: $node_binary" >&2; exit 1; }

rm -rf -- "$RUN_ROOT"
mkdir -p "$RUN_ROOT/store" "$RUN_ROOT/legacy-cache" "$RUN_ROOT/experimental-cache" "$RESULTS_ROOT"
run_dir="$(mktemp -d "$RESULTS_ROOT/run.XXXXXX")"
printf '%s\n' "$TOKEN" >"$RUN_ROOT/auth-token"

node_pid=""
cleanup() {
  if [[ -n "$node_pid" ]]; then
    kill -TERM "$node_pid" 2>/dev/null || true
    wait "$node_pid" 2>/dev/null || true
  fi
}
trap cleanup EXIT

start_node() {
  local route="$1" cache_dir="$2"
  export CLOUD_PROVIDER=local LOCAL_PATH="$RUN_ROOT/store"
  export GRAPH_NAMESPACE=cypher-ab GRAPH_ID=default GRAPH_CELL_ID=cell-0 GRAPH_CELLS=cell-0 GRAPH_DATA_PATH=data
  export GRAPH_ALLOW_PLAINTEXT=true GRAPH_AUTH_TOKEN_FILE="$RUN_ROOT/auth-token"
  export GRAPH_DATA_CACHE_BYTES=67108864 GRAPH_DATA_CACHE_DIR="$cache_dir"
  export GRAPH_NODE_ID=node-0 GRAPH_BOLT_ADDR=127.0.0.1:17687 GRAPH_ADVERTISED_BOLT_ADDR=127.0.0.1:17687
  export GRAPH_BOLT_NODE_ADDRESSES=node-0=127.0.0.1:17687 GRAPH_HTTP_ADDR=127.0.0.1:18443 GRAPH_ADMIN_ADDR=127.0.0.1:19091
  # Raise to `hydradb::shard=debug` to get one "experimental cypher stages"
  # line per request (lower/snapshot/statistics/plan/execute microseconds) in
  # the experimental node log, or `hydradb::shard=trace` for per-operator
  # lines too.
  export GRAPH_CYPHER_ENGINE="$route" RUST_LOG="${GRAPH_CYPHER_ENGINE_AB_RUST_LOG:-warn}"
  "$node_binary" >"$RUN_ROOT/$route-node.log" 2>&1 &
  node_pid="$!"
  for _ in $(seq 1 160); do
    curl -fsS http://127.0.0.1:19091/readyz >/dev/null 2>&1 && return
    if ! kill -0 "$node_pid" 2>/dev/null; then cat "$RUN_ROOT/$route-node.log" >&2; return 1; fi
    sleep 0.25
  done
  echo "$route node did not become ready" >&2
  return 1
}

stop_node() {
  kill -TERM "$node_pid"
  wait "$node_pid"
  node_pid=""
}

capture_metrics() {
  curl -fsS http://127.0.0.1:19091/metrics >"$1"
}

run_route() {
  local route="$1" cache_dir="$2"
  start_node "$route" "$cache_dir"
  if [[ "$route" == legacy ]]; then
    GRAPH_CYPHER_ENGINE_AB_TOKEN="$TOKEN" "$PYTHON" "$SCRIPT_DIR/cypher_engine_ab_client.py" seed
  fi
  capture_metrics "$run_dir/$route.metrics.before.prom"
  run_args=(run --mode "$route" --iterations "$ITERATIONS" --output "$run_dir/$route.json")
  if [[ "$MODE" == benchmark ]]; then run_args+=(--warmup); fi
  GRAPH_CYPHER_ENGINE_AB_TOKEN="$TOKEN" "$PYTHON" "$SCRIPT_DIR/cypher_engine_ab_client.py" "${run_args[@]}"
  if [[ "$route" == experimental ]]; then
    GRAPH_CYPHER_ENGINE_AB_TOKEN="$TOKEN" "$PYTHON" "$SCRIPT_DIR/cypher_engine_ab_client.py" explain \
      --mode experimental --output "$run_dir/experimental.explain.json"
    GRAPH_CYPHER_ENGINE_AB_TOKEN="$TOKEN" "$PYTHON" "$SCRIPT_DIR/cypher_compat_protocol_regressions.py" \
      --output "$run_dir/experimental.compat.json"
  fi
  capture_metrics "$run_dir/$route.metrics.after.prom"
  stop_node
}

run_route legacy "$RUN_ROOT/legacy-cache"
run_route experimental "$RUN_ROOT/experimental-cache"
"$PYTHON" "$SCRIPT_DIR/cypher_engine_ab_client.py" compare --legacy "$run_dir/legacy.json" \
  --experimental "$run_dir/experimental.json" --output "$run_dir/comparison.json"

# Report the universal query/storage metrics and the experimental physical
# operator requests. The assertions make observability part of this live gate:
# a route can no longer execute correctly while silently emitting zeroes.
"$PYTHON" - "$run_dir" <<'PY'
import json
import sys
from pathlib import Path

run_dir = Path(sys.argv[1])
names = (
    "graph_storage_get_requests",
    "graph_storage_scan_requests",
    "graph_query_rows_duration_microseconds_count",
    "graph_query_rows_duration_microseconds_sum",
    "graph_query_experimental_property_seek_requests",
    "graph_query_experimental_relationship_expand_requests",
    "graph_query_experimental_ordered_property_scan_requests",
)

def snapshot(path):
    values = {name: 0.0 for name in names}
    for line in path.read_text().splitlines():
        for name in names:
            if line.startswith(name + "{") or line.startswith(name + " "):
                values[name] += float(line.rsplit(" ", 1)[1])
    return values

report = {"unavailable": []}
for route in ("legacy", "experimental"):
    before = snapshot(run_dir / f"{route}.metrics.before.prom")
    after = snapshot(run_dir / f"{route}.metrics.after.prom")
    report[route] = {name: after[name] - before[name] for name in names}

for name in (
    "graph_query_rows_duration_microseconds_count",
    "graph_query_experimental_property_seek_requests",
    "graph_query_experimental_relationship_expand_requests",
    "graph_query_experimental_ordered_property_scan_requests",
):
    if report["experimental"][name] <= 0:
        raise SystemExit(f"experimental route emitted no {name}")
for name in (
    "graph_query_experimental_property_seek_requests",
    "graph_query_experimental_relationship_expand_requests",
    "graph_query_experimental_ordered_property_scan_requests",
):
    if report["legacy"][name] != 0:
        raise SystemExit(f"legacy route unexpectedly emitted {name}")
(run_dir / "metrics.json").write_text(json.dumps(report, indent=2) + "\n")
PY

echo "cypher-engine-ab-ok mode=$MODE iterations=$ITERATIONS results=$run_dir"
