#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-${XDG_CACHE_HOME:-$HOME/.cache}/dgraph-target}"

cd "$ROOT"
cargo fmt --all --check
cargo clippy --locked --all-targets -- -D warnings
cargo clippy --locked --all-targets --features chaos-harness -- -D warnings
if pkg-config --exists cypher-parser; then
  cargo clippy --locked --all-targets --features opencypher -- -D warnings
fi
cargo test --quiet --locked --lib
cargo test --quiet --locked --features chaos-harness --lib
cargo check --quiet --locked --examples
cargo check --quiet --locked --examples --features chaos-harness
if pkg-config --exists cypher-parser; then
  cargo test --quiet --locked --features opencypher --lib
fi

if ldconfig -p 2>/dev/null | grep -qi 'libgraphblas'; then
  cargo test --quiet --locked --features graphblas --lib
  cargo check --quiet --locked --examples --features graphblas
  if pkg-config --exists cypher-parser; then
    cargo test --quiet --locked --features opencypher,graphblas --lib
    cargo check --quiet --locked --examples --features opencypher,graphblas
  fi
else
  echo "skipping graphblas feature tests because libgraphblas is not installed"
fi
