set shell := ["bash", "-eu", "-c"]

# Show available recipes.
default:
    @just --list

# Show available recipes.
help:
    @just --list

# Format Rust code.
fmt:
    cargo fmt

# Check Rust formatting.
fmt-check:
    cargo fmt --check

# Check all default-feature targets.
check:
    cargo check --all-targets

# Check examples with GraphBLAS enabled.
check-examples-graphblas:
    cargo check --examples --features graphblas

# Run default library tests.
test:
    cargo test --lib

# Run library tests with GraphBLAS enabled.
test-graphblas:
    cargo test --features graphblas --lib

# Verify native libraries required by Rust FFI crates.
native-check:
    #!/usr/bin/env bash
    set -euo pipefail
    pkg-config --exists cypher-parser
    if command -v ldconfig >/dev/null 2>&1; then
      ldconfig -p | grep -qi libgraphblas
    elif command -v brew >/dev/null 2>&1; then
      test -f "$(brew --prefix suite-sparse)/lib/libgraphblas.dylib"
    else
      test -f /opt/homebrew/lib/libgraphblas.dylib || test -f /usr/local/lib/libgraphblas.dylib
    fi

# Run the local CI-equivalent check set.
ci: native-check fmt-check test test-graphblas check-examples-graphblas

# Run the local object-store smoke test.
smoke:
    cargo run --example phase0_object_store_smoke

# Run the local object-store smoke test with GraphBLAS enabled.
smoke-graphblas:
    PHASE0_MATRIX_KERNEL=graphblas cargo run --features graphblas --example phase0_object_store_smoke

# Run the path/supernode benchmark harness.
bench:
    scripts/phase0_path_bench.sh

# Run the path/supernode benchmark with the Rust sparse kernel.
bench-rust:
    PHASE0_GRAPHBLAS=0 PHASE0_MATRIX_KERNEL=rust scripts/phase0_path_bench.sh

# Run local multiprocess stress against the local filesystem object store.
stress:
    scripts/phase0_multiprocess_stress.sh

# Run MinIO smoke test. Requires Docker.
minio-smoke:
    scripts/phase0_minio_smoke.sh

# Run MinIO chaos test. Requires Docker.
minio-chaos:
    scripts/phase0_minio_chaos.sh

# Refresh the pinned SlateDB Git dependency in Cargo.lock.
update-slatedb:
    cargo update -p slatedb
