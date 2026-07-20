#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
IMAGE="${NEO4J_BENCH_IMAGE:-neo4j:2026.06.0}"
PORT="${NEO4J_BENCH_PORT:-17688}"
PASSWORD="${NEO4J_BENCH_PASSWORD:-benchmark-secret}"
DEGREE="${NEO4J_BENCH_DEGREE:-1000}"
HOPS="${NEO4J_BENCH_HOPS:-1,3,5,10}"
MAX_HOP_INPUT="${NEO4J_BENCH_MAX_HOP:-}"
REPEATS="${NEO4J_BENCH_REPEATS:-3}"
WARMUP="${NEO4J_BENCH_WARMUP:-10}"
SAMPLES="${NEO4J_BENCH_SAMPLES:-100}"
CONCURRENCY="${NEO4J_BENCH_CONCURRENCY:-8}"
OPERATIONS_PER_WORKER="${NEO4J_BENCH_OPERATIONS_PER_WORKER:-50}"
HEAP="${NEO4J_BENCH_HEAP:-512m}"
PAGE_CACHE="${NEO4J_BENCH_PAGE_CACHE:-512m}"
OUTPUT_ROOT="${NEO4J_BENCH_OUTPUT_ROOT:-$HOME/neo4j-exact-hop-degree-${DEGREE}}"
VENV="${NEO4J_BENCH_VENV:-$HOME/.cache/turbolay-graphblas-benchmark-venv}"

if ! validated_dimensions="$(
  python3 - \
    "$PORT" "$DEGREE" "$HOPS" "$MAX_HOP_INPUT" "$REPEATS" "$WARMUP" \
    "$SAMPLES" "$CONCURRENCY" "$OPERATIONS_PER_WORKER" 2>&1 <<'PY'
import sys

(
    port_raw,
    degree_raw,
    hops_raw,
    max_hop_raw,
    repeats_raw,
    warmup_raw,
    samples_raw,
    concurrency_raw,
    operations_raw,
) = sys.argv[1:]


def decimal(name: str, raw: str, *, allow_zero: bool = False) -> int:
    if not raw.isascii() or not raw.isdecimal():
        raise SystemExit(f"{name} must be an unsigned decimal integer")
    value = int(raw)
    if value < (0 if allow_zero else 1):
        qualifier = "non-negative" if allow_zero else "positive"
        raise SystemExit(f"{name} must be {qualifier}")
    return value


port = decimal("NEO4J_BENCH_PORT", port_raw)
if port > 65535:
    raise SystemExit("NEO4J_BENCH_PORT must not exceed 65535")
degree = decimal("NEO4J_BENCH_DEGREE", degree_raw)
requested_hops = [
    decimal("NEO4J_BENCH_HOPS", value.strip())
    for value in hops_raw.split(",")
    if value.strip()
]
if not requested_hops:
    raise SystemExit("NEO4J_BENCH_HOPS must contain at least one hop")
if any(hop > 32 for hop in requested_hops):
    raise SystemExit("NEO4J_BENCH_HOPS values must not exceed 32")

max_hop = max(requested_hops)
if max_hop_raw:
    max_hop = decimal("NEO4J_BENCH_MAX_HOP", max_hop_raw)
    if max_hop > 32:
        raise SystemExit("NEO4J_BENCH_MAX_HOP must not exceed 32")
    if max(requested_hops) > max_hop:
        raise SystemExit(
            "NEO4J_BENCH_MAX_HOP must be at least the deepest requested hop"
        )

decimal("NEO4J_BENCH_REPEATS", repeats_raw)
decimal("NEO4J_BENCH_WARMUP", warmup_raw, allow_zero=True)
decimal("NEO4J_BENCH_SAMPLES", samples_raw)
decimal("NEO4J_BENCH_CONCURRENCY", concurrency_raw)
decimal("NEO4J_BENCH_OPERATIONS_PER_WORKER", operations_raw)

stride = degree + 1
largest_vertex_id = max_hop * stride + degree
if largest_vertex_id > (1 << 63) - 1:
    raise SystemExit("configured degree and hop depth exceed Neo4j's signed integer IDs")

print(max_hop, stride)
PY
)"; then
  printf '%s\n' "$validated_dimensions" >&2
  exit 2
fi
read -r MAX_HOP ID_STRIDE <<<"$validated_dimensions"

if [[ "${NEO4J_BENCH_VALIDATE_ONLY:-0}" == "1" ]]; then
  printf 'validated max_hop=%s id_stride=%s\n' "$MAX_HOP" "$ID_STRIDE"
  exit 0
fi

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
    offset=$((hop * ID_STRIDE))
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
