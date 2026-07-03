#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-/home/vishal/.cache/dgraph-phase0-target}"

cd "$ROOT"
cargo test --quiet

if ldconfig -p 2>/dev/null | grep -qi 'libgraphblas'; then
  cargo test --quiet --features graphblas
else
  echo "skipping graphblas feature tests because libgraphblas is not installed"
fi
