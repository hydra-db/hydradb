#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
IMAGE="${NEO4J_BENCH_IMAGE:-neo4j:2026.06.0}"
PORT="${NEO4J_BENCH_PORT:-17688}"
PASSWORD="${NEO4J_BENCH_PASSWORD:-benchmark-secret}"
DEGREE="${NEO4J_BENCH_DEGREE:-1000}"
HOPS="${NEO4J_BENCH_HOPS:-1,3,5,10}"
MAX_HOP="${NEO4J_BENCH_MAX_HOP:-10}"
REPEATS="${NEO4J_BENCH_REPEATS:-3}"
WARMUP="${NEO4J_BENCH_WARMUP:-10}"
SAMPLES="${NEO4J_BENCH_SAMPLES:-100}"
CONCURRENCY="${NEO4J_BENCH_CONCURRENCY:-8}"
OPERATIONS_PER_WORKER="${NEO4J_BENCH_OPERATIONS_PER_WORKER:-50}"
HEAP="${NEO4J_BENCH_HEAP:-512m}"
PAGE_CACHE="${NEO4J_BENCH_PAGE_CACHE:-512m}"
OUTPUT_ROOT="${NEO4J_BENCH_OUTPUT_ROOT:-$HOME/neo4j-exact-hop-degree-${DEGREE}}"
VENV="${NEO4J_BENCH_VENV:-$HOME/.cache/turbolay-graphblas-benchmark-venv}"

container=""
cleanup() {
  if [[ -n "$container" ]]; then
    docker rm -f "$container" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT INT TERM

if [[ ! -x "$VENV/bin/python" ]]; then
  python3 -m venv "$VENV"
  "$VENV/bin/pip" install --quiet --upgrade pip 'neo4j>=6,<7'
fi

seed_graph() {
  local path="(root)"
  local hop offset
  for ((hop = 1; hop <= MAX_HOP; hop++)); do
    offset=$((hop * 1000000))
    path+="-[:BENCH]->(:BenchNode {id: ${offset} + branch + 1})"
  done

  docker exec "$container" cypher-shell -u neo4j -p "$PASSWORD" \
    "CREATE CONSTRAINT bench_node_id IF NOT EXISTS FOR (n:BenchNode) REQUIRE n.id IS UNIQUE" \
    >/dev/null
  docker exec "$container" cypher-shell -u neo4j -p "$PASSWORD" \
    "CREATE (:BenchNode {id: 1})" >/dev/null
  docker exec "$container" cypher-shell -u neo4j -p "$PASSWORD" \
    "UNWIND range(0, $((DEGREE - 1))) AS branch MATCH (root:BenchNode {id: 1}) CREATE ${path} RETURN count(*) AS branches" \
    >/dev/null
  docker exec "$container" cypher-shell -u neo4j -p "$PASSWORD" \
    "MATCH ()-[r:BENCH]->() RETURN count(r) AS edges" \
    | tail -n 1 | grep -Fx "$((DEGREE * MAX_HOP))" >/dev/null
}

wait_until_ready() {
  local attempt
  for attempt in $(seq 1 90); do
    if docker exec "$container" cypher-shell -u neo4j -p "$PASSWORD" \
      "RETURN 1" >/dev/null 2>&1; then
      return 0
    fi
    sleep 2
  done
  docker logs "$container" >&2 || true
  return 1
}

mkdir -p "$OUTPUT_ROOT"
for round in $(seq 1 "$REPEATS"); do
  container="neo4j-exact-hop-bench-${round}"
  docker rm -f "$container" >/dev/null 2>&1 || true
  docker run -d --name "$container" \
    -p "127.0.0.1:${PORT}:7687" \
    -e "NEO4J_AUTH=neo4j/${PASSWORD}" \
    -e "NEO4J_server_memory_heap_initial__size=${HEAP}" \
    -e "NEO4J_server_memory_heap_max__size=${HEAP}" \
    -e "NEO4J_server_memory_pagecache_size=${PAGE_CACHE}" \
    "$IMAGE" >/dev/null
  wait_until_ready
  seed_graph

  round_dir="${OUTPUT_ROOT}/round-${round}"
  rm -rf "$round_dir"
  mkdir -p "$round_dir"
  "$VENV/bin/python" "$ROOT/scripts/bolt_graphblas_client.py" \
    --uri "bolt://127.0.0.1:${PORT}" \
    --token "$PASSWORD" \
    --database neo4j \
    --source-label BenchNode \
    --degree "$DEGREE" \
    --hops "$HOPS" \
    --warmup "$WARMUP" \
    --samples "$SAMPLES" \
    --concurrency "$CONCURRENCY" \
    --operations-per-worker "$OPERATIONS_PER_WORKER" \
    --output-dir "$round_dir" \
    | tee "$round_dir/client.log"
  docker stats --no-stream --format '{{json .}}' "$container" >"$round_dir/docker-stats.json"
  docker exec "$container" cypher-shell -u neo4j -p "$PASSWORD" \
    "CALL dbms.components() YIELD name, versions, edition RETURN name, versions[0] AS version, edition" \
    >"$round_dir/server-version.txt"
  docker rm -f "$container" >/dev/null
  container=""
done

"$VENV/bin/python" - "$OUTPUT_ROOT" <<'PY'
import csv
import statistics
import sys
from pathlib import Path

root = Path(sys.argv[1])
rows = []
for result in sorted(root.glob("round-*/results.csv")):
    with result.open(newline="", encoding="utf-8") as handle:
        rows.extend(csv.DictReader(handle))

print("median_of_rounds kind hop p50_us p95_us p99_us mean_us qps")
for kind in ("latency", "throughput"):
    for hop in sorted({int(row["hops"]) for row in rows}):
        selected = [row for row in rows if row["kind"] == kind and int(row["hops"]) == hop]
        values = [
            statistics.median(float(row[column]) for row in selected)
            for column in ("p50_us", "p95_us", "p99_us", "mean_us", "qps")
        ]
        print(kind, hop, *(f"{value:.3f}" for value in values))
PY
