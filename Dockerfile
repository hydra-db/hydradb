FROM ubuntu:24.04 AS chef

ARG RUST_VERSION=1.99.0
ARG CARGO_CHEF_VERSION=0.1.77
ENV DEBIAN_FRONTEND=noninteractive \
    CARGO_HOME=/usr/local/cargo \
    RUSTUP_HOME=/usr/local/rustup \
    RUSTUP_TOOLCHAIN=${RUST_VERSION} \
    PATH=/usr/local/cargo/bin:${PATH}

RUN apt-get update && apt-get install -y --no-install-recommends \
      build-essential ca-certificates clang cmake curl libclang-dev \
      libcypher-parser-dev libffi-dev libgraphblas-dev pkg-config && \
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | \
      sh -s -- -y --profile minimal --default-toolchain "${RUST_VERSION}" && \
    cargo install --locked --version "${CARGO_CHEF_VERSION}" cargo-chef && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /workspace

# LAGraph has no Ubuntu package, so it is built here, statically, against the
# chef image's libgraphblas-dev. The stage depends on nothing but the script,
# so a Rust change never rebuilds it, and the builders copy out only the
# archive and header: the runtime image gains no new library.
FROM chef AS lagraph

COPY scripts/install_lagraph.sh /tmp/install_lagraph.sh
RUN bash /tmp/install_lagraph.sh /opt/lagraph

FROM chef AS planner

COPY . .
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder

COPY --from=lagraph /opt/lagraph /opt/lagraph
ENV LAGRAPH_LIB_DIR=/opt/lagraph/lib
COPY --from=planner /workspace/recipe.json recipe.json
RUN cargo chef cook --locked --release --recipe-path recipe.json \
      --features server-runtime,indexer-runtime,experimental-cypher-engine,datafusion-cypher-engine,otlp \
      --bin graph-node --bin graph-indexer

COPY . .

# The commit is passed in rather than read from the tree: `.dockerignore`
# excludes `.git`, so `crates/telemetry/build.rs` has no repository to
# interrogate here and would stamp `unknown` into every image. CI supplies both
# from `container.yml`/`release.yml`; a local `docker build` without them
# produces an image that says so on its first log line.
#
# Keep these below the dependency-only `cargo chef cook` layer: the build stamp
# changes every commit, but it must invalidate only HydraDB's own compilation,
# not the locked third-party dependency graph.
ARG GIT_SHA=""
ARG GIT_BRANCH=""
ENV GIT_SHA=${GIT_SHA} \
    GIT_BRANCH=${GIT_BRANCH}

# `otlp` is off by default in Cargo.toml, and every OTLP path — the trace
# bridge, the log appender, the observable-counter registration — compiles to an
# inert stub without it. Omitting it here produced an image whose OTLP export
# was absent rather than misconfigured: no error, no endpoint, nothing on the
# wire. It must stay on this line for OTEL_EXPORTER_OTLP_ENDPOINT to mean
# anything at runtime.
RUN cargo build --locked --release \
      --features server-runtime,indexer-runtime,experimental-cypher-engine,datafusion-cypher-engine,otlp \
      --bin graph-node --bin graph-indexer && \
    cargo build --locked --release \
      -p falkor-dump-import --bin import && \
    strip --strip-debug target/release/graph-node && \
    strip target/release/graph-indexer \
      target/release/import

# The turbolay proxy. It links none of the native graph libraries,
# so its image is small and its build never touches graph-node's dependency
# graph beyond the shared lockfile. BoringSSL builds from source here, which is
# what the chef image's cmake and libclang are for. boring-sys applies its
# patches to the BoringSSL sources through git, so this stage adds git.
FROM chef AS proxy-builder

RUN apt-get update && apt-get install -y --no-install-recommends git && \
    rm -rf /var/lib/apt/lists/*

COPY --from=planner /workspace/recipe.json recipe.json
RUN cargo chef cook --locked --release --recipe-path recipe.json \
      -p turbolay-proxy --features k8s,boringssl

COPY . .
RUN cargo build --locked --release -p turbolay-proxy --features k8s,boringssl && \
    strip target/release/turbolay-proxy

FROM ubuntu:24.04 AS proxy

ARG GIT_SHA=""
LABEL org.opencontainers.image.revision="${GIT_SHA}"
ENV DEBIAN_FRONTEND=noninteractive \
    RUST_LOG=info

RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && \
    rm -rf /var/lib/apt/lists/* && \
    groupadd --gid 10001 graph && \
    useradd --uid 10001 --gid graph --no-create-home --shell /usr/sbin/nologin graph

COPY --from=proxy-builder /workspace/target/release/turbolay-proxy /usr/local/bin/turbolay-proxy

USER 10001:10001
EXPOSE 7687 8443 9090
ENTRYPOINT ["/usr/local/bin/turbolay-proxy"]

# The usage-report receiver behind telemetry.hydradb.com. HydraDB runs one
# copy for every install, so it is not part of charts/hydradb. Like the proxy it
# links none of the native graph libraries.
FROM chef AS telemetry-receiver-builder

COPY --from=planner /workspace/recipe.json recipe.json
RUN cargo chef cook --locked --release --recipe-path recipe.json \
      -p hydradb-query-telemetry --features receiver

COPY . .
RUN cargo build --locked --release -p hydradb-query-telemetry --features receiver \
      --bin query-telemetry-receiver && \
    strip target/release/query-telemetry-receiver

FROM ubuntu:24.04 AS telemetry-receiver

ARG GIT_SHA=""
LABEL org.opencontainers.image.revision="${GIT_SHA}"
# The binary's own default is 127.0.0.1, which nothing outside the container
# can reach.
ENV DEBIAN_FRONTEND=noninteractive \
    QUERY_TELEMETRY_ADDR=0.0.0.0:18500

RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && \
    rm -rf /var/lib/apt/lists/* && \
    groupadd --gid 10001 graph && \
    useradd --uid 10001 --gid graph --no-create-home --shell /usr/sbin/nologin graph

COPY --from=telemetry-receiver-builder \
  /workspace/target/release/query-telemetry-receiver \
  /usr/local/bin/query-telemetry-receiver

USER 10001:10001
EXPOSE 18500
ENTRYPOINT ["/usr/local/bin/query-telemetry-receiver"]

FROM ubuntu:24.04 AS runtime-base

ENV DEBIAN_FRONTEND=noninteractive \
    RUST_LOG=info

RUN apt-get update && apt-get install -y --no-install-recommends \
      ca-certificates libcypher-parser-dev libgraphblas-dev && \
    rm -rf /var/lib/apt/lists/* && \
    groupadd --gid 10001 graph && \
    useradd --uid 10001 --gid graph --no-create-home --shell /usr/sbin/nologin graph && \
    mkdir -p /var/cache/slatedb /tmp/graph && \
    chown -R graph:graph /var/cache/slatedb /tmp/graph

FROM runtime-base AS runtime

# The same value the binaries carry, in the place `docker inspect` and every
# registry UI already look for it. Redundant on purpose: a label answers
# "what is in this image" without running it, which is the only question you
# can ask about an image that will not start.
ARG GIT_SHA=""
LABEL org.opencontainers.image.revision="${GIT_SHA}"

COPY --from=builder /workspace/target/release/graph-node /usr/local/bin/graph-node
COPY --from=builder /workspace/target/release/graph-indexer /usr/local/bin/graph-indexer
COPY --from=builder /workspace/target/release/import /usr/local/bin/falkor-dump-import

USER 10001:10001
EXPOSE 7687 8443 9090 9443
ENTRYPOINT ["/usr/local/bin/graph-node"]
