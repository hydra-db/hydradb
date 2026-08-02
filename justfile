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

# OpenCypher's async query futures exceed the 2 MiB default test-thread stack.
# Keep enough headroom for different compiler profiles and platforms so the
# suite reports assertion failures rather than aborting with SIGABRT.
export RUST_MIN_STACK := env_var_or_default("RUST_MIN_STACK", "33554432")

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

# `ci.yml` lints the root package six times, once per shipped feature
# combination, every one of them `--all-targets ... -D warnings`. None of the six
# subsumes another: a lint only fires inside the cfg arms its feature set
# compiles, so the `cfg(not(feature = ...))` arms are visible only to the default
# run and the Bolt and HTTP paths only to the last three. The two
# workspace members are linted inside test-placement and test-telemetry instead,
# because they need an explicit `-p`.
#
# These are `clippy`, not `check`, and `--all-targets` covers examples and tests,
# so each strictly subsumes the same-feature `check`/`check-examples*` recipe
# below — which is why `ci` runs these in their place rather than in addition to
# them. The check recipes stay defined because plans under `docs/` call them by
# name and because they are the faster inner loop.
# Lint the default feature set, as `ci.yml` does.
clippy:
    cargo clippy --locked --all-targets -- -D warnings

# Lint the feature-gated hard-fence chaos harness.
clippy-chaos:
    cargo clippy --locked --all-targets --features chaos-harness -- -D warnings

# Lint the native OpenCypher parser feature set.
clippy-opencypher:
    cargo clippy --locked --all-targets --features opencypher -- -D warnings

# Lint every native feature at once.
clippy-native:
    cargo clippy --locked --all-targets --features opencypher,query-transport,query-transport-tls,query-service-discovery,public-client-protocols -- -D warnings

# Lint the public Bolt and HTTP client protocols.
clippy-client-protocols:
    cargo clippy --locked --all-targets --features public-client-protocols -- -D warnings

# Lint the production node and indexer runtimes.
clippy-runtime:
    cargo clippy --locked --all-targets --features server-runtime,indexer-runtime -- -D warnings

# `default = []`, so this covers the `cfg(not(feature = ...))` arms and nothing
# else — it is a real shipped configuration (`ci.yml` gates it on both Linux and
# macOS), but it is a small slice of the crate. Pair it with check-all-features;
# neither subsumes the other.
#
# It currently reports `AtomicDurationHistogram::{bucket_index, record_micros,
# record}` as dead. That is true *of this feature set* — every caller sits in
# `src/shard/query.rs` behind opencypher — and false of the crate. The fix is a
# cfg on the impl in `src/core/histogram.rs`, not a flag here.
#
# Leaving it standing stopped being free on 2026-07-27: `ci.yml` lints this same
# feature set with `-D warnings`, and `just clippy` now does too, so what reads as
# a tolerated warning here is a hard build failure there. It is a warning in this
# recipe only because `cargo check` does not promote it.
# Check all default-feature targets.
check:
    cargo check --locked --all-targets

# The widest compile surface in one line, so a feature that only *this* recipe
# reaches — the root package's `otlp` is reached by nothing else in `ci`, since
# test-telemetry enables the *crate's* otlp under `-p turbolay-telemetry` and
# never the root switch the binaries' cfg arms read — cannot rot unnoticed.
# `--all-features` rather than an enumerated list on purpose: an enumerated list
# silently stops covering the next feature added to Cargo.toml.
# Check every target with every feature enabled.
check-all-features:
    cargo check --locked --all-targets --all-features

# `client-api` is the shared client stack that `bolt-server` and `http-api` both
# imply, and until this recipe existed nothing built it alone — not `ci`, not
# `ci.yml`. Every path that reached it dragged a wire protocol in too, so an item
# gated `#[cfg(any(feature = "bolt-server", feature = "http-api"))]` could be
# called from `client-api` code and still compile everywhere anyone looked.
# `QueryResultSet::estimated_resident_bytes` was exactly that, and
# `--features client-api` failed E0599 on it until 2026-07-27.
#
# `--lib`, not `--all-targets`, and not by preference: `src/client/mod.rs`'s
# `#[cfg(test)]` `ClientTestTlsBundle` names `tokio_rustls` with no feature gate,
# so the lib-test target of this configuration does not build either. That is a
# second instance of the same bug and wants a gate on the helper; until it has
# one, widening this line would only make the recipe fail for an unrelated
# reason. Widen it when that lands.
#
# `check` rather than `clippy -D warnings` for the same reason: this
# configuration is the only one in which `PreparedClientQuery::columns` and
# `ClientQueryService::release_server_cursor` are dead — both are reached only
# from `src/client/bolt.rs` — so a `-D warnings` line here would fail on two
# missing cfgs in `src/client/service.rs` rather than on the compile break this
# recipe exists to catch. Promote it once those are gated.
# Check the shared client stack with no wire protocol above it.
check-client-api:
    cargo check --locked --features client-api --lib

# `ci.yml`'s "Check standalone Bolt server". check-all-features cannot stand in
# for it: with `http-api` also on, anything this configuration is missing gets
# supplied by the other protocol and the gap stays hidden.
# Check the standalone Bolt server feature set.
check-bolt-server:
    cargo check --locked --all-targets --features bolt-server

# Check default-feature examples.
check-examples:
    cargo check --locked --examples

# Check examples with the native OpenCypher parser enabled.
check-examples-native:
    cargo check --locked --examples --features opencypher

# Check the feature-gated hard-fence chaos harness.
check-examples-chaos:
    cargo check --locked --examples --features chaos-harness

# Run default library tests.
test *args:
    cargo test --locked --lib {{args}}

# Run library tests with OpenCypher enabled.
test-opencypher:
    cargo test --locked --features opencypher --lib

# `--all-targets`, not `--lib`, and it is load-bearing: `--lib` builds the client
# stack as a plain dependency, so `src/client/service/tests.rs` never sees
# `cfg(test)` and its tests silently do not exist. Mirrors `ci.yml`'s "Test full
# native feature set" line exactly.
# Run all targets with every native feature enabled.
test-native:
    cargo test --locked --all-targets --features opencypher,query-transport,query-transport-tls,query-service-discovery,public-client-protocols

# The Bolt and HTTP surfaces without query-service-discovery, which is how an
# embedder that brings its own routing builds them. test-native cannot catch a
# `use reqwest::…` that leaked into the shared client path; this can.
# Run all targets with the public Bolt and HTTP client protocols.
test-client-protocols:
    cargo test --locked --all-targets --features public-client-protocols

# Run library tests with the feature-gated hard-fence harness enabled.
test-chaos:
    cargo test --locked --features chaos-harness --lib

# The library under this feature set is covered by test-native; this recipe is
# the graph-node binary's own config, publisher and heartbeat tests.
# Test the production runtime configuration, as `ci.yml` does.
test-server-runtime:
    cargo test --locked --features server-runtime --bin graph-node

# `indexer-runtime` was reached only by compile-only recipes until this existed —
# clippy-runtime lints it and check-all-features builds it, so the binary's seven
# tests were compiled by `ci` and then never run. This is the only line in `ci`
# that *executes* indexer code: the scope-discovery test and the six that pin the
# `/metrics` rendering, including the cell and edge-type dimensions and the
# `MAX_DIMENSIONS` cap. `--bin graph-indexer` because the binary declares
# `required-features = ["indexer-runtime"]`, so no `--lib` or `--all-targets` line
# anywhere else builds its test target.
# Test the indexer runtime configuration.
test-indexer:
    cargo test --locked --features indexer-runtime --bin graph-indexer

# The same gap test-indexer closed, one binary over: test-server-runtime omits
# `otlp` and check-all-features only compiles it, so the graph-node binary's OTLP
# export tests were built by `ci` and never run. They are the only tests that
# prove an instrument reaches a collector rather than merely registering — an
# observable instrument whose callback the SDK never invokes is silent, and
# silence is indistinguishable from a counter that is genuinely zero. That is
# the failure mode M1 shipped and `aa53595` was written to close, so it is worth
# a recipe of its own rather than a feature added to the line above: the OTLP
# tests bind a loopback socket and wait on a real export interval, and that
# belongs in a recipe an operator can run alone.
# Test the graph-node binary's OTLP metric export.
test-node-otlp:
    cargo test --locked --features server-runtime,otlp --bin graph-node

# Every other recipe here is bare, so it selects the root package only; a
# workspace member needs its own explicit `-p` line or it is never built.
# Lint and test the placement crate.
test-placement:
    cargo clippy --locked --all-targets -p turbolay-placement -- -D warnings
    cargo test --locked -p turbolay-placement

# The telemetry crate's OTLP-only modules — the sampler, the exporter wiring and
# the log bridge — are behind an off-by-default feature, so neither `just check`
# nor `just test` reaches them. Without `--features otlp` the sampler is not even
# compiled.
# Lint and test the telemetry crate with OTLP export enabled.
test-telemetry:
    cargo clippy --locked --all-targets -p turbolay-telemetry --features otlp -- -D warnings
    cargo test --locked -p turbolay-telemetry --features otlp

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

# Ordered as `ci.yml` orders it: format, then every lint, then the compile-only
# configurations, then the tests. Lints first because they are the cheapest way
# to fail and because until 2026-07-27 this recipe ran *no* clippy at all on the
# root package — only the two `-p` lines inside test-placement and
# test-telemetry — so a green `just ci` did not imply a green CI, which is the
# one thing it exists to mean.
#
# The clippy-* recipes are `--all-targets`, so they subsume check, check-examples,
# check-examples-native and check-examples-chaos feature-for-feature. Those four
# are deliberately absent from this list rather than deleted: running both halves
# would pay for the same compile twice, once through rustc and once through
# clippy-driver, for no extra coverage.
# Run the local CI-equivalent check set.
ci: native-check fmt-check clippy clippy-chaos clippy-opencypher clippy-native clippy-client-protocols clippy-runtime test-placement test-telemetry check-all-features check-client-api check-bolt-server test test-opencypher test-native test-client-protocols test-chaos test-server-runtime test-indexer test-node-otlp

# Run the local object-store smoke test.
smoke:
    cargo run --example object_store_smoke

# SuiteSparse is the default kernel now that the cargo feature is gone, so this
# differs from `smoke` only by pinning it — which is the point, since `smoke`
# inherits whatever GRAPH_MATRIX_KERNEL the caller's shell already exports.
# `example/object_store_smoke.rs` also accepts `compact` and `rust`.
# Run the local object-store smoke test pinned to the SuiteSparse kernel.
smoke-graphblas:
    GRAPH_MATRIX_KERNEL=graphblas cargo run --example object_store_smoke

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
