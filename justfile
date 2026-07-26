set shell := ["bash", "-eu", "-c"]

# The build environment every recipe below needs, so that `just <recipe>` works
# on a clean checkout without anyone rediscovering these three by bisecting a
# link error. An already-exported value always wins.
#
# Homebrew's prefix is macOS only: `opencypher` (and therefore `server-runtime`)
# runs bindgen against libcypher-parser, and `graphblas` links libgraphblas.
# Neither is on the default search path there. CI is Linux and installs both
# from apt, which is why nothing in `ci.yml` sets them.
brew_prefix := if os() == "macos" { shell("brew --prefix 2>/dev/null || echo /opt/homebrew") } else { "" }
export BINDGEN_EXTRA_CLANG_ARGS := env_var_or_default("BINDGEN_EXTRA_CLANG_ARGS", if os() == "macos" { "-I" + brew_prefix + "/include" } else { "" })
export LIBRARY_PATH := env_var_or_default("LIBRARY_PATH", if os() == "macos" { brew_prefix + "/lib" } else { "" })

# Every platform, and matching `ci.yml`'s OpenCypher test jobs exactly. Without
# it `cypher_relationship_properties_are_indexed_mutable_and_snapshot_safe`
# overflows the 2 MiB default test-thread stack and aborts the whole run with
# SIGABRT, which reads like a crash in the code rather than a missing knob.
export RUST_MIN_STACK := env_var_or_default("RUST_MIN_STACK", "8388608")

# Show available recipes.
default:
    @just --list

# Show available recipes.
help:
    @just --list

# Format Rust code.
fmt:
    cargo fmt --all

# Check Rust formatting.
fmt-check:
    cargo fmt --all --check

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

# The library under this feature set is covered by test-native; this recipe is
# the graph-node binary's own config, publisher and heartbeat tests.
# Test the production runtime configuration, as `ci.yml` does.
test-server-runtime:
    cargo test --locked --features server-runtime --bin graph-node

# Lint and test the workspace members. Every other recipe here is bare, so it
# selects the root package only.
test-placement:
    cargo clippy --locked --all-targets -p turbolay-placement -- -D warnings
    cargo test --locked -p turbolay-placement

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
ci: native-check fmt-check check test-placement test test-opencypher test-graphblas test-native test-chaos test-server-runtime check-examples check-examples-native check-examples-chaos

# Run the local object-store smoke test.
smoke:
    cargo run --example object_store_smoke

# Run the local object-store smoke test with GraphBLAS enabled.
smoke-graphblas:
    GRAPH_MATRIX_KERNEL=graphblas cargo run --features graphblas --example object_store_smoke

# Run local multiprocess stress against the local filesystem object store.
stress:
    bash scripts/multiprocess_stress.sh

# Run hard write-fence takeover proof against the local filesystem object store.
fence:
    bash scripts/fence_takeover.sh

# Run MinIO smoke test. Requires Docker.
minio-smoke:
    bash scripts/minio_smoke.sh

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

# Replay all six Quint Connect adapters against isolated MinIO paths. Requires Docker.
minio-mbt:
    bash scripts/minio_mbt.sh

# Refresh the pinned SlateDB Git dependency in Cargo.lock.
update-slatedb:
    cargo update -p slatedb
