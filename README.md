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
- Optional task runner: `just`
- Linux build tools: `build-essential`, `clang`, `libclang-dev`, `cmake`, and
  `pkg-config`
- OpenCypher parser headers and library: `libcypher-parser-dev`
- Optional GraphBLAS acceleration: SuiteSparse GraphBLAS development headers
  and library, normally `libgraphblas-dev` on Ubuntu/Debian

On Ubuntu or WSL:

```bash
sudo apt-get update
sudo apt-get install -y build-essential clang libclang-dev cmake pkg-config libcypher-parser-dev libgraphblas-dev
cargo install just --locked
```

On macOS with Homebrew:

```bash
xcode-select --install
brew install rustup-init just cmake pkg-config llvm suite-sparse
brew install cleishm/neo4j/libcypher-parser
rustup-init
```

Open a new shell after `rustup-init`, then make Homebrew's native libraries
visible to `pkg-config` and the linker:

```bash
export PKG_CONFIG_PATH="$(brew --prefix libcypher-parser)/lib/pkgconfig:${PKG_CONFIG_PATH:-}"
export LIBRARY_PATH="$(brew --prefix suite-sparse)/lib:${LIBRARY_PATH:-}"
export DYLD_FALLBACK_LIBRARY_PATH="$(brew --prefix suite-sparse)/lib:${DYLD_FALLBACK_LIBRARY_PATH:-}"

pkg-config --exists cypher-parser
test -f "$(brew --prefix suite-sparse)/lib/libgraphblas.dylib"
```

The default feature set builds the storage kernel without native parser or
GraphBLAS dependencies. The `opencypher` feature needs `libcypher-parser`. The
`graphblas` feature needs SuiteSparse GraphBLAS, which Homebrew provides through
`suite-sparse`. The `chaos-harness` feature exposes only the hard-fence test
worker used by local/MinIO takeover scripts; it is not part of the normal
application API.

## Clone And Test

```bash
git clone https://github.com/usecortex/slatedb-graph-kernel.git
cd slatedb-graph-kernel
cargo test --lib
cargo test --features opencypher --lib
cargo test --features opencypher,graphblas --lib
cargo check --examples --features opencypher,graphblas
```

Or, with `just`:

```bash
just ci
```

The `graphblas` Cargo feature enables the crate's native FFI path:
`src/sparse_kernel.rs` links directly with `libgraphblas` through
`#[link(name = "graphblas")]`. There is no Rust GraphBLAS crate dependency.

The Cypher front-end is behind the `opencypher` Cargo feature. It uses
`libcypher-parser-sys`, which links against the native `cypher-parser` system
library through `pkg-config`.

The default feature set is checked on Ubuntu and macOS in CI. The native
OpenCypher and GraphBLAS feature set is checked on Ubuntu where the system
packages are installed by the workflow.

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

## Store Format

Graph shards write a durable `graph/meta/format_version` record when opened by a
writer. Missing format metadata is treated as a legacy Phase 0 store and is
accepted so existing prototype data can still be read. Newer or otherwise
unsupported format versions fail closed during `GraphShard::open`.

## Observability

The crate emits structured `tracing` events for store-format initialization,
rollup publication, delta/artifact GC, lease acquisition, lease renewal issues,
and failover. Applications should install their own tracing subscriber and
export those events with their normal metrics/log pipeline.

## Useful Commands

List recipes:

```bash
just
```

Run the local object-store smoke test:

```bash
just smoke
```

Run the path/supernode benchmark with GraphBLAS:

```bash
just bench
```

Run the same path/supernode benchmark against a Docker MinIO object store:

```bash
just minio-bench
```

Run local multiprocess stress against the local filesystem object store:

```bash
just stress
```

Run the hard write-fence takeover proof against the local filesystem object
store:

```bash
just fence
```

Run MinIO smoke or chaos checks when Docker is available:

```bash
just minio-smoke
just minio-chaos
just minio-fence
```

Generated benchmark files are ignored under `bench-results/`. The MinIO path
benchmark writes `bench-results/phase0_path_bench_minio.csv` and a matching log
by default.

## Current Scope

This is still a Phase 0 kernel. It is meant for correctness, layout, traversal,
supernode, and object-store experiments, not as a finished database server. The
main production boundary is the graph layer above SlateDB: routing, leases,
rollups, artifact publication, and query execution policy must continue to be
validated under real multi-node and S3 failure modes.
