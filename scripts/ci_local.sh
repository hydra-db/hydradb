#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-${XDG_CACHE_HOME:-$HOME/.cache}/turbolay-target}"

if ! command -v just >/dev/null 2>&1; then
  echo "just is required; install it with 'cargo install just --locked'" >&2
  exit 1
fi

cd "$ROOT"
exec just ci
