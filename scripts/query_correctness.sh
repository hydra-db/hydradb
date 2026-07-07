#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

FEATURES="${GRAPH_QUERY_CORRECTNESS_FEATURES:-opencypher}"
RESULTS="${GRAPH_QUERY_CORRECTNESS_RESULTS:-bench-results/query_correctness.csv}"
LOG="${GRAPH_QUERY_CORRECTNESS_LOG:-bench-results/query_correctness.log}"
TARGET_DIR="${GRAPH_QUERY_CORRECTNESS_TARGET_DIR:-${CARGO_TARGET_DIR:-$HOME/.cache/dgraph-query-target}}"

mkdir -p "$(dirname "$RESULTS")" "$(dirname "$LOG")"

echo "query correctness: features=$FEATURES results=$RESULTS log=$LOG" >&2
CARGO_TARGET_DIR="$TARGET_DIR" cargo run --release --features "$FEATURES" --example query_correctness \
    >"$RESULTS" \
    2>"$LOG"
