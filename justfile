set shell := ["bash", "-eu", "-c"]

# The build environment every recipe below needs, so that `just <recipe>` works
# on a clean checkout without anyone rediscovering these three by bisecting a
# link error. An already-exported value always wins.
#
# Homebrew's prefix is macOS only: `opencypher` (and therefore `server-runtime`)
# runs bindgen against libcypher-parser, and `graphblas` links libgraphblas.
# Neither is on the default search path there. CI is Linux and installs both
# from apt, which is why nothing in `ci.yml` sets them.
brew_prefix := `if command -v brew >/dev/null 2>&1; then brew --prefix; elif [ "$(uname -s)" = Darwin ]; then printf /opt/homebrew; fi`
export BINDGEN_EXTRA_CLANG_ARGS := env_var_or_default("BINDGEN_EXTRA_CLANG_ARGS", if os() == "macos" { "-I" + brew_prefix + "/include" } else { "" })
export LIBRARY_PATH := env_var_or_default("LIBRARY_PATH", if os() == "macos" { brew_prefix + "/lib" } else { "" })

# OpenCypher's async query futures exceed the 2 MiB default test-thread stack.
# Keep enough headroom for different compiler profiles and platforms so the
# suite reports assertion failures rather than aborting with SIGABRT.
export RUST_MIN_STACK := env_var_or_default("RUST_MIN_STACK", "33554432")
target_directory := `cargo metadata --no-deps --format-version 1 | python3 -c 'import json,sys; print(json.load(sys.stdin)["target_directory"])'`

# Show available recipes.
default:
    @just --list

# Show available recipes.
help:
    @just --list

# Build the node, indexer, and stdio MCP launcher together.
build-mcp:
    cargo build --locked -p hydradb -p hydradb-mcp --features server-runtime,indexer-runtime --bin graph-node --bin graph-indexer --bin hydradb-mcp

# Run the local node, indexer, and MCP server. Data survives restarts.
mcp-local data_dir="/tmp/turbolay-mcp": build-mcp
    "{{target_directory}}/debug/hydradb-mcp" --local {{quote(data_dir)}}


# Lint the MCP adapter and local launcher.
clippy-mcp:
    cargo clippy --locked --all-targets -p hydradb-mcp -- -D warnings

# Build the node with optional failed-query reporting.
build-mcp-query-telemetry:
    cargo build --locked -p hydradb -p hydradb-mcp --features server-runtime,indexer-runtime,query-telemetry --bin graph-node --bin graph-indexer --bin hydradb-mcp

mcp-local-query-telemetry data_dir="/tmp/turbolay-mcp": build-mcp-query-telemetry
    "{{target_directory}}/debug/hydradb-mcp" --local {{quote(data_dir)}}

clippy-query-telemetry-runtime:
    cargo clippy --locked --all-targets --features server-runtime,indexer-runtime,query-telemetry -- -D warnings

test-query-telemetry:
    cargo test --locked --lib --features server-runtime,query-telemetry client::failure_reporting::tests
    cargo test --locked -p hydradb-query-telemetry
    cargo test --locked -p hydradb-query-telemetry --features receiver --bin query-telemetry-receiver

# Build the independent Axum receiver for failed-query reports.
build-query-telemetry:
    cargo build --locked -p hydradb-query-telemetry --features receiver --bin query-telemetry-receiver

query-telemetry-receiver: build-query-telemetry
    cargo run --locked -p hydradb-query-telemetry --features receiver --bin query-telemetry-receiver

clippy-query-telemetry:
    cargo clippy --locked -p hydradb-query-telemetry --features receiver --all-targets -- -D warnings

# Lint and test the usage-report format shared by GraphNode and the receiver.
clippy-usage-report:
    cargo clippy --locked -p hydradb-usage-report -p hydradb-pg-usage-report --all-targets -- -D warnings

test-usage-report:
    cargo test --locked -p hydradb-usage-report -p hydradb-pg-usage-report

# GraphNode's side of the usage report: config, collection, privacy, and the
# first-query stamps in the query service.
test-usage-report-node:
    cargo test --locked --features server-runtime --bin graph-node -- usage_report config::tests::usage
    cargo test --locked --lib --features server-runtime -- client::service::tests::first_query_outcomes client::service::tests::a_prepare_failure_stamps_the_first_failure


test-pg-graph *args:
    cargo test --locked -p hydradb-pg-graph-core -p hydradb-pg-graph-csr -p hydradb-pg-graph-query -p hydradb-pg-usage-report {{args}}

check-pg-graph:
    cargo clippy --locked -p hydradb-pg-graph-core -p hydradb-pg-graph-csr -p hydradb-pg-graph-query -p hydradb-pg-usage-report --all-targets -- -D warnings

pg-build pg="16":
    cd crates/pg-hydradb && PGRX_PG_CONFIG_PATH="$(pg_config --bindir)/pg_config" cargo build --release --locked --no-default-features --features pg{{pg}}

pg-check pg="16":
    cd crates/pg-hydradb && cargo fmt --all --check
    cd crates/pg-hydradb && PGRX_PG_CONFIG_PATH="$(pg_config --bindir)/pg_config" cargo clippy --locked --no-default-features --features pg{{pg}} -- -D warnings

pg-fmt:
    cd crates/pg-hydradb && cargo fmt --all

pg-install pg="16": (pg-build pg)
    install -m 755 crates/pg-hydradb/target/release/libpg_hydradb.so "$(pg_config --pkglibdir)/pg_hydradb.so"
    install -m 644 crates/pg-hydradb/pg_hydradb.control crates/pg-hydradb/sql/pg_hydradb--0.1.0.sql "$(pg_config --sharedir)/extension/"

pg-test:
    bash scripts/test_pg_hydradb.sh

pg-lock:
    cargo update --workspace
    cd crates/pg-hydradb && cargo generate-lockfile

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

# Lint the storage-neutral DataFusion graph engine and provider contracts.
clippy-datafusion:
    cargo clippy --locked --all-targets -p hydradb-graph-types -p hydradb-graph-provider -p hydradb-graph-datafusion -p hydradb-cypher-df -- -D warnings
    cargo clippy --locked --all-targets -p hydradb-graph-slate --features datafusion-cypher-engine -- -D warnings

clippy-graph-slate:
    cargo clippy --locked --all-targets -p hydradb-graph-slate -- -D warnings
    cargo clippy --locked --all-targets -p hydradb-graph-slate --all-features -- -D warnings

# Lint the production runtime with both query engines compiled together.
clippy-datafusion-runtime:
    cargo clippy --locked --all-targets --features server-runtime,experimental-cypher-engine,datafusion-cypher-engine -- -D warnings

# Check the DataFusion graph engine and its root integration.
check-datafusion:
    cargo check --locked --all-targets -p hydradb-graph-types -p hydradb-graph-provider -p hydradb-graph-datafusion -p hydradb-cypher-df
    cargo check --locked --all-targets --features datafusion-cypher-engine


# Test the root integration for the DataFusion Cypher route.
test-datafusion-root *args:
    cargo test --locked --lib --features datafusion-cypher-engine {{args}}

test-datafusion-node:
    cargo test --locked --features server-runtime,experimental-cypher-engine,datafusion-cypher-engine --bin graph-node



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
# test-telemetry enables the *crate's* otlp under `-p hydradb-telemetry` and
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
    cargo test --locked --lib -p hydradb -p hydradb-graph-slate {{args}}

# Run library tests with OpenCypher enabled.
test-opencypher:
    cargo test --locked --features opencypher --lib -p hydradb -p hydradb-graph-slate

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

# Build the graph-node binary with the DataFusion Cypher route enabled. This is
# intentionally a build-only harness recipe: the default runtime and indexer
# builds remain unchanged, while integration tests can launch this exact binary
# with GRAPH_CYPHER_ENGINE=datafusion.
build-datafusion-node:
    cargo build --locked --features server-runtime,datafusion-cypher-engine --bin graph-node

# Build the companion indexer binary for the isolated runtime fixture without
# executing its tests. This keeps service provisioning on just recipes while
# leaving the indexer product code untouched.
build-indexer-runtime:
    cargo build --locked --features indexer-runtime --bin graph-indexer

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
# The publication protocol crate: lease-owned, forward-only publication of
# derived objects. Pure object-store logic, tested against `InMemory`.
test-object-publication:
    cargo clippy --locked --all-targets -p hydradb-object-publication -- -D warnings
    cargo test --locked -p hydradb-object-publication

test-placement:
    cargo clippy --locked --all-targets -p hydradb-placement -- -D warnings
    cargo test --locked -p hydradb-placement
    cargo clippy --locked --all-targets -p hydradb-placement --no-default-features -- -D warnings
    cargo test --locked -p hydradb-placement --no-default-features

proxy_crates := "-p hydradb-scope -p hydradb-protocol -p hydradb-bolt-server -p turbolay-proxy-types -p turbolay-proxy-topology -p turbolay-proxy-core -p turbolay-proxy-bolt -p turbolay-proxy-http -p turbolay-proxy"

# The turbolay proxy and the shared crates it links. No native libraries and
# no docker, so it runs without the native toolchain. `hydradb-protocol` is
# linted without default features too, the way the kernel builds it.
# Lint and test the turbolay proxy.
test-proxy:
    cargo clippy --locked --all-targets {{proxy_crates}} -- -D warnings
    cargo clippy --locked --all-targets -p hydradb-protocol --no-default-features -- -D warnings
    cargo test --locked {{proxy_crates}}

# kube and BoringSSL are only in the image build; this is the one place they
# are compiled outside it.
# Lint and test the proxy's k8s replica source and the image feature set.
test-proxy-image:
    cargo clippy --locked --all-targets -p turbolay-proxy-k8s -- -D warnings
    cargo test --locked -p turbolay-proxy-k8s
    cargo clippy --locked -p turbolay-proxy --features k8s,boringssl -- -D warnings





# Both feature sets, since the kernel builds the scope crate with `serde` only
# under `query-transport`.
# Lint and test the scope crate.
test-scope:
    cargo clippy --locked --all-targets -p hydradb-scope -- -D warnings
    cargo clippy --locked --all-targets -p hydradb-scope --features serde -- -D warnings
    cargo test --locked -p hydradb-scope --features serde

# The telemetry crate's OTLP-only modules — the sampler, the exporter wiring and
# the log bridge — are behind an off-by-default feature, so neither `just check`
# nor `just test` reaches them. Without `--features otlp` the sampler is not even
# compiled.
# Lint and test the telemetry crate with OTLP export enabled.
test-telemetry:
    cargo clippy --locked --all-targets -p hydradb-telemetry --features otlp -- -D warnings
    cargo test --locked -p hydradb-telemetry --features otlp

# Generate the Neo4j grammar, lint the frontend facade, and execute its parser
# tests. This member is deliberately separate from the root package, so no bare
# root `cargo test` reaches it.
test-cypher-ast:
    cargo clippy --locked --all-targets -p hydradb-cypher-ast -- -D warnings
    cargo test --locked -p hydradb-cypher-ast

test-cypher: test-cypher-ast
    cargo clippy --locked --all-targets -p hydradb-cypher-parser-antlr -- -D warnings
    cargo test --locked -p hydradb-cypher-parser-antlr

# Validate the engine-independent harness and the complete pinned inventory.
tck-harness corpus:
    cargo test --locked -p hydradb-cypher-tck-harness
    TCK_FEATURES={{quote(corpus)}} cargo test --locked -p hydradb-cypher-tck-harness pinned_step_inventory -- --ignored --exact
    python3 query-language/shared/scripts/validate_cypher_test_ledger.py
    python3 -m unittest query-language/shared/scripts/test_cypher_tck_scripts.py

# Run checked-in HydraDB declarative cases through both production adapters.
tck-local:
    cargo test --locked --features opencypher,experimental-cypher-engine --test cypher_tck_conformance -- --nocapture

# Execute every expanded official TCK case through one production adapter.
tck-run engine corpus report:
    test {{quote(engine)}} = legacy -o {{quote(engine)}} = experimental
    python3 query-language/shared/scripts/run_cypher_tck.py {{quote(engine)}} {{quote(corpus)}} {{quote(report)}}

# Require identical shard inventories, four verdict levels, and exact policy.
tck-aggregate shards output:
    python3 query-language/shared/scripts/aggregate_cypher_tck.py {{quote(shards)}} {{quote(output)}}

# Check the generated parser without compiling the graph application.
check-cypher-parser *args:
    cargo check --locked -p hydradb-cypher-parser-antlr {{args}}

# Lower the shared Cypher AST into semantic graph IR and select a KV plan.
test-graph-plan:
    cargo clippy --locked --all-targets -p hydradb-graph-plan -- -D warnings
    cargo test --locked -p hydradb-graph-plan

# Prepare and execute selected graph plans in the isolated Cypher engine.
test-cypher-engine *args:
    cargo clippy --locked --all-targets -p hydradb-cypher-engine -- -D warnings
    cargo test --locked -p hydradb-cypher-engine {{args}}

# Opt-in parser/planner/GraphShard route, including Bolt/HTTP preparation and
# graph-node runtime switch tests. The default runtime remains legacy.
test-experimental-cypher:
    cargo clippy --locked --all-targets --features server-runtime,experimental-cypher-engine -- -D warnings
    cargo test --locked --all-targets --features server-runtime,experimental-cypher-engine





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
ci: clippy-usage-report test-usage-report test-usage-report-node test-neo4j-oracle native-check fmt-check clippy clippy-graph-slate clippy-chaos clippy-opencypher clippy-native clippy-client-protocols clippy-runtime clippy-mcp clippy-query-telemetry clippy-query-telemetry-runtime test-query-telemetry test-placement test-scope test-proxy test-telemetry test-cypher test-graph-plan test-cypher-engine check-all-features check-client-api check-bolt-server test test-opencypher test-native test-client-protocols test-chaos test-server-runtime test-indexer test-node-otlp












# Build one node containing both routes, seed through legacy Bolt, then compare
# the two routes through real Bolt and HTTP against the same immutable store.
# Requires a Python interpreter with the neo4j package (set PYTHON if needed).
cypher-engine-ab:
    bash query-language/shared/scripts/cypher_engine_ab.sh

# Bounded protocol benchmark: 10 iterations per corpus query/transport/route.
# Results are timestamped below ignored bench-results/cypher_engine_ab/.
cypher-engine-ab-bench:
    GRAPH_CYPHER_ENGINE_AB_MODE=benchmark GRAPH_CYPHER_ENGINE_AB_ITERATIONS=10 bash query-language/shared/scripts/cypher_engine_ab.sh

# Fast offline coverage for the normalization and fixed supported corpus.
test-cypher-engine-ab:
    python3 query-language/shared/scripts/cypher_engine_ab_client.py self-test
    python3 query-language/shared/scripts/cypher_compat_protocol_regressions.py --self-test


# Render the legacy read parity matrix to Markdown for review.
parity-matrix:
    python3 query-language/cypher/scripts/render_parity_matrix.py

# Check the legacy read parity matrix renders cleanly and the Neo4j oracle
# passes its self-test. No engine build needed.
test-parity-matrix:
    python3 query-language/cypher/scripts/render_parity_matrix.py --check
    python3 query-language/cypher/scripts/neo4j_parity_oracle.py self-test

# Offline checks of the Neo4j oracle: value normalisation, verdicts, and that
# the matrix and query-language/cypher/parity/neo4j-divergences.json load. No Neo4j needed.
test-neo4j-oracle:
    python3 query-language/cypher/scripts/neo4j_parity_oracle.py self-test
    python3 query-language/cypher/scripts/test_neo4j_oracle_server.py

# Hold every parity matrix row to what Neo4j returns for the same query on the
# same graph. Starts a pinned local Neo4j in Docker; needs the neo4j Python
# package (set PYTHON to a venv interpreter). Extra args go to the oracle, e.g.
# `just neo4j-oracle --jsonl /tmp/oracle.jsonl --only return_star`.
neo4j-oracle *args:
    bash query-language/cypher/scripts/neo4j_oracle_server.sh run {{args}}








# Refresh the pinned SlateDB Git dependency in Cargo.lock.
update-slatedb:
    cargo update -p slatedb

# Fast, pure-Rust validation for the FalkorDB DUMP decoder.
test-falkordb-rdb:
    cargo test --locked -p falkordb-rdb

check-falkordb-rdb:
    cargo check --locked -p falkordb-rdb

clippy-falkordb-rdb:
    cargo clippy --locked -p falkordb-rdb --all-targets -- -D warnings

refresh-falkordb-rdb-lock:
    cargo check -p falkordb-rdb

inspect-falkordb-rdb dump:
    cargo run --locked -p falkordb-rdb --bin inspect -- "{{dump}}"

scan-falkordb-rdb root workers="":
    cargo run --locked --release -p falkordb-rdb --bin scan -- "{{root}}" {{workers}}

test-falkor-dump-import:
    cargo test --locked -p falkor-dump-import

check-falkor-dump-import:
    cargo check --locked -p falkor-dump-import

clippy-falkor-dump-import:
    cargo clippy --locked -p falkor-dump-import --all-targets -- -D warnings

refresh-falkor-dump-import-lock:
    cargo check -p falkor-dump-import

plan-falkor-dump-import root *args:
    cargo run --locked --release -p falkor-dump-import --bin plan -- "{{root}}" {{args}}

build-falkor-dump-import:
    cargo build --locked --release -p falkor-dump-import --bin import

import-falkor-dump snapshot *args:
    cargo run --locked --release -p falkor-dump-import --bin import -- --snapshot-root "{{snapshot}}" {{args}}

test-external-graph-index-verifier:
    cargo test --locked current_graph_verifier_reads_published_external_graph_index -- --nocapture

test-offline-relationship-merge-index:
    cargo test --locked offline_relationship_merge_index_installation_validates_and_publishes -- --nocapture

