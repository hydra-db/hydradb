FROM ubuntu:24.04 AS build-base

ARG RUST_VERSION=1.91.0
ARG CARGO_CHEF_VERSION=0.1.78
ENV DEBIAN_FRONTEND=noninteractive \
    CARGO_HOME=/usr/local/cargo \
    RUSTUP_HOME=/usr/local/rustup \
    PATH=/usr/local/cargo/bin:${PATH}

RUN apt-get update && apt-get install -y --no-install-recommends \
      build-essential ca-certificates clang cmake curl libclang-dev \
      libcypher-parser-dev libgraphblas-dev pkg-config && \
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | \
      sh -s -- -y --profile minimal --default-toolchain "${RUST_VERSION}" && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /workspace

# cargo-chef is what lets the ~400 dependency crates cache independently of our
# own source. It is installed in this stage, which changes only when the apt set
# or the toolchain does, so it is paid for once and never on a source change.
RUN cargo install cargo-chef --locked --version "${CARGO_CHEF_VERSION}"

# `prepare` reduces the tree to a dependency manifest. Its output is a function
# of Cargo.toml, Cargo.lock and the crate layout only, so editing src/ leaves
# recipe.json byte-identical and the cook layers below stay valid. This stage
# still re-runs on every source change; it takes seconds and produces no build.
FROM build-base AS planner

COPY . .
RUN cargo chef prepare --recipe-path recipe.json

FROM build-base AS builder

# cook must be handed the same profile and feature set as the build below it.
# A mismatch is not an error — cargo simply rebuilds what does not match, which
# silently undoes the dependency/application split this whole stage exists for.
COPY --from=planner /workspace/recipe.json recipe.json
RUN cargo chef cook --locked --release \
      --features server-runtime,indexer-runtime,otlp \
      --recipe-path recipe.json

COPY . .

# `otlp` is off by default in Cargo.toml, and every OTLP path — the trace
# bridge, the log appender, the observable-counter registration — compiles to an
# inert stub without it. Omitting it here produced an image whose OTLP export
# was absent rather than misconfigured: no error, no endpoint, nothing on the
# wire. It must stay on this line for OTEL_EXPORTER_OTLP_ENDPOINT to mean
# anything at runtime.
RUN cargo build --locked --release --features server-runtime,indexer-runtime,otlp \
      --bin graph-node --bin graph-indexer && \
    strip target/release/graph-node target/release/graph-indexer

FROM build-base AS benchmark-builder

# A different feature set to the builder stage above, so this cooks and caches
# its own dependency layer rather than sharing one.
COPY --from=planner /workspace/recipe.json recipe.json
RUN cargo chef cook --locked --release --features server-runtime \
      --recipe-path recipe.json

COPY . .
RUN cargo build --locked --release --features server-runtime \
      --example s3_bolt_benchmark_server && \
    strip target/release/examples/s3_bolt_benchmark_server

FROM ubuntu:24.04 AS runtime-base

# `graph-node` sizes its own worker stacks, so this default is for everything
# on this stage that does not: `s3-bolt-benchmark-server` links the same query
# engine and still takes the size from the environment, and it is what any
# thread the runtime did not spawn gets.
ENV DEBIAN_FRONTEND=noninteractive \
    RUST_LOG=info \
    RUST_MIN_STACK=33554432

RUN apt-get update && apt-get install -y --no-install-recommends \
      ca-certificates libcypher-parser-dev libgraphblas-dev && \
    rm -rf /var/lib/apt/lists/* && \
    groupadd --gid 10001 graph && \
    useradd --uid 10001 --gid graph --no-create-home --shell /usr/sbin/nologin graph && \
    mkdir -p /var/cache/slatedb /tmp/graph && \
    chown -R graph:graph /var/cache/slatedb /tmp/graph

FROM runtime-base AS graphblas-benchmark

COPY --from=benchmark-builder \
  /workspace/target/release/examples/s3_bolt_benchmark_server \
  /usr/local/bin/s3-bolt-benchmark-server

USER 10001:10001
ENTRYPOINT ["/usr/local/bin/s3-bolt-benchmark-server"]

FROM runtime-base AS runtime

COPY --from=builder /workspace/target/release/graph-node /usr/local/bin/graph-node
COPY --from=builder /workspace/target/release/graph-indexer /usr/local/bin/graph-indexer

USER 10001:10001
EXPOSE 7687 8443 9090 9443
ENTRYPOINT ["/usr/local/bin/graph-node"]
