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
    cargo check --locked --all-targets

# Check default-feature examples.
check-examples:
    cargo check --locked --examples

# Check examples with GraphBLAS enabled.
check-examples-graphblas:
    cargo check --locked --examples --features graphblas

# Check examples with native parser and GraphBLAS enabled.
check-examples-native:
    cargo check --locked --examples --features opencypher,graphblas

# Check the feature-gated hard-fence chaos harness.
check-examples-chaos:
    cargo check --locked --examples --features chaos-harness

# Run default library tests.
test:
    cargo test --locked --lib

# Run library tests with OpenCypher enabled.
test-opencypher:
    cargo test --locked --features opencypher --lib

# Run library tests with GraphBLAS enabled.
test-graphblas:
    cargo test --locked --features graphblas --lib

# Run library tests with all native features enabled.
test-native:
    cargo test --locked --features opencypher,graphblas --lib

# Run library tests with the feature-gated hard-fence harness enabled.
test-chaos:
    cargo test --locked --features chaos-harness --lib

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
ci: native-check fmt-check check test test-opencypher test-graphblas test-native test-chaos check-examples check-examples-native check-examples-chaos

# Run the local object-store smoke test.
smoke:
    cargo run --example object_store_smoke

# Run the local object-store smoke test with GraphBLAS enabled.
smoke-graphblas:
    GRAPH_MATRIX_KERNEL=graphblas cargo run --features graphblas --example object_store_smoke

# Run the path/supernode benchmark harness.
bench:
    bash scripts/path_bench.sh

# Run the path/supernode benchmark with the Rust sparse kernel.
bench-rust:
    GRAPH_BENCH_GRAPHBLAS=0 GRAPH_MATRIX_KERNEL=rust bash scripts/path_bench.sh

# Run local multiprocess stress against the local filesystem object store.
stress:
    bash scripts/multiprocess_stress.sh

# Run hard write-fence takeover proof against the local filesystem object store.
fence:
    bash scripts/fence_takeover.sh

# Run MinIO smoke test. Requires Docker.
minio-smoke:
    bash scripts/minio_smoke.sh

# Run path/supernode benchmarks against MinIO. Requires Docker.
minio-bench:
    bash scripts/minio_path_bench.sh

# Run Query engine Cypher query benchmarks.
query-bench:
    bash scripts/query_bench.sh

# Run low-memory query/build/concurrency profiling.
query-memory-profile:
    bash scripts/query_memory_profile.sh

# Run Query engine exact query correctness benchmark.
query-correctness:
    bash scripts/query_correctness.sh

# Run Query engine Cypher query benchmarks against MinIO. Requires Docker.
minio-query-bench:
    bash scripts/minio_query_bench.sh

# Run Query engine exact query correctness benchmark against MinIO. Requires Docker.
minio-query-correctness:
    bash scripts/minio_query_correctness.sh

# Run MinIO chaos test. Requires Docker.
minio-chaos:
    bash scripts/minio_chaos.sh

# Run hard write-fence takeover proof against MinIO. Requires Docker.
minio-fence:
    bash scripts/minio_fence_takeover.sh

# Refresh the pinned SlateDB Git dependency in Cargo.lock.
update-slatedb:
    cargo update -p slatedb
