# SlateDB Graph Kernel

This repository contains the Phase 0 graph kernel prototype for a stateless,
object-store-backed graph database. SlateDB is the durable KV/object-store layer;
this crate adds graph layout, write routing, edge mutation batching, artifact
builders, supernode indexes, sparse traversal kernels, and the early Cypher
front-end.

The crate is intentionally kept outside the SlateDB source tree so it can track
upstream SlateDB without carrying a long-lived fork. The tested SlateDB revision
is pinned in `Cargo.toml` and `Cargo.lock`.

## Requirements

- Rust stable
- Linux build tools: `build-essential`, `clang`, `libclang-dev`, `cmake`, and
  `pkg-config`
- Optional GraphBLAS acceleration: SuiteSparse GraphBLAS development headers
  and library, normally `libgraphblas-dev` on Ubuntu/Debian

On Ubuntu or WSL:

```bash
sudo apt-get update
sudo apt-get install -y build-essential clang libclang-dev cmake pkg-config libgraphblas-dev
```

## Clone And Test

```bash
git clone https://github.com/usecortex/slatedb-graph-kernel.git
cd slatedb-graph-kernel
cargo test --lib
cargo test --features graphblas --lib
cargo check --examples --features graphblas
```

The `graphblas` Cargo feature enables the crate's native FFI path:
`src/sparse_kernel.rs` links directly with `libgraphblas` through
`#[link(name = "graphblas")]`. There is no Rust GraphBLAS crate dependency.

## SlateDB Dependency

SlateDB is fetched from GitHub:

```toml
slatedb = { git = "https://github.com/slatedb/slatedb.git", rev = "a6e169dc1e143fa72a0aa916a9b23cf29b3656b4", default-features = false, features = ["aws", "foyer"] }
```

To move to a newer upstream SlateDB, update the `rev` in `Cargo.toml`, then run:

```bash
cargo update -p slatedb
cargo test --lib
cargo test --features graphblas --lib
```

## Useful Commands

Run the local object-store smoke test:

```bash
cargo run --example phase0_object_store_smoke
```

Run the path/supernode benchmark with GraphBLAS:

```bash
PHASE0_GRAPHBLAS=1 scripts/phase0_path_bench.sh
```

Run local multiprocess stress against the local filesystem object store:

```bash
scripts/phase0_multiprocess_stress.sh
```

Run MinIO smoke or chaos checks when Docker is available:

```bash
scripts/phase0_minio_smoke.sh
scripts/phase0_minio_chaos.sh
```

Generated benchmark files are ignored under `bench-results/`.

## Current Scope

This is still a Phase 0 kernel. It is meant for correctness, layout, traversal,
supernode, and object-store experiments, not as a finished database server. The
main production boundary is the graph layer above SlateDB: routing, leases,
rollups, artifact publication, and query execution policy must continue to be
validated under real multi-node and S3 failure modes.
