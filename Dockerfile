FROM ubuntu:24.04 AS builder

ARG RUST_VERSION=1.91.0
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
COPY . .
RUN cargo build --locked --release --features server-runtime \
      --bin graph-node --bin graph-controller && \
    strip target/release/graph-node target/release/graph-controller

FROM ubuntu:24.04

ENV DEBIAN_FRONTEND=noninteractive \
    RUST_LOG=info

RUN apt-get update && apt-get install -y --no-install-recommends \
      ca-certificates libcypher-parser-dev libgraphblas-dev && \
    rm -rf /var/lib/apt/lists/* && \
    groupadd --gid 10001 graph && \
    useradd --uid 10001 --gid graph --no-create-home --shell /usr/sbin/nologin graph && \
    mkdir -p /var/cache/slatedb /tmp/graph && \
    chown -R graph:graph /var/cache/slatedb /tmp/graph

COPY --from=builder /workspace/target/release/graph-node /usr/local/bin/graph-node
COPY --from=builder /workspace/target/release/graph-controller /usr/local/bin/graph-controller

USER 10001:10001
EXPOSE 7687 8443 9090 9443
ENTRYPOINT ["/usr/local/bin/graph-node"]
