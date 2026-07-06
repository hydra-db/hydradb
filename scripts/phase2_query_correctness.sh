#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

FEATURES="${PHASE2_QUERY_CORRECTNESS_FEATURES:-opencypher}"
RESULTS="${PHASE2_QUERY_CORRECTNESS_RESULTS:-bench-results/phase2_query_correctness.csv}"
LOG="${PHASE2_QUERY_CORRECTNESS_LOG:-bench-results/phase2_query_correctness.log}"
TARGET_DIR="${PHASE2_QUERY_CORRECTNESS_TARGET_DIR:-${CARGO_TARGET_DIR:-$HOME/.cache/dgraph-phase2-target}}"

mkdir -p "$(dirname "$RESULTS")" "$(dirname "$LOG")"

echo "phase2 query correctness: features=$FEATURES results=$RESULTS log=$LOG" >&2
CARGO_TARGET_DIR="$TARGET_DIR" cargo run --release --features "$FEATURES" --example phase2_query_correctness \
    >"$RESULTS" \
    2>"$LOG"
