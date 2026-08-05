# SlateDB Graph Kernel

`slatedb-graph-kernel` is a Rust graph storage and query kernel built on top of
SlateDB. The object store is the durable source of truth, while local SSD/NVMe
and memory are cache layers controlled through SlateDB settings and graph-layer
cache policy.

The crate is intentionally kept outside the SlateDB repository. SlateDB stays an
upstream dependency pinned in `Cargo.toml`/`Cargo.lock`. SlateDB owns writer
fencing, WAL durability, storage snapshots, compaction, and object-store
coordination; this crate owns the graph model, topology artifacts, traversal
kernels, query planner, and operational harnesses.

## What Is Implemented

- Durable edge writes, deletes, bulk import, trusted segment append,
  idempotency records, and degree/index maintenance.
- One SlateDB writer and any number of `DbReader` processes per graph store.
  SlateDB writer epochs, WAL barriers, and serializable transactions provide
  fencing and commit ordering.
- Controllerless stateless nodes: every node can read every configured cell;
  write requests lazily open a cached writer and rely on SlateDB for safety.
- Query-scoped `DbSnapshot`/`DbReaderSnapshot` reads, immutable graph-index
  publication, bounded generation GC, and full graph export/verifier digests.
- Canonical outbound adjacency segments for high-throughput ingestion and one
  content-addressed CSC graph-index format for sparse traversal.
- Compute-compute separation: graph nodes serve reads and canonical writes;
  independent `graph-indexer` workers build CSC generations asynchronously.
- Causal reads stay entirely on the local reader/cache path. Strong reads first
  refresh SlateDB from object storage, then pin one snapshot. Both modes combine
  an indexed base with the committed WAL tail when indexing lags.
- Sparse traversal through the Rust kernel by default, with optional SuiteSparse
  GraphBLAS FFI for compiled CSC traversal.
- OpenCypher row query execution behind the `opencypher` feature, including
  parsing through `libcypher-parser`, row result sets, pages/cursors,
  cancellation, limits, stats, and optimizer passes.
- Optional TCP query transport with required auth by default, optional TLS/mTLS,
  static directory discovery, and Kubernetes/Consul/etcd discovery helpers.
- Public Bolt 5.1-5.4 over TLS for Neo4j-driver compatibility and an HTTPS
  query API with typed JSON or streaming NDJSON responses. Both adapters share
  one scoped authentication, authorization, quota, cancellation, bookmark, and
  deadline service.
- Local filesystem, MinIO, and S3-compatible object-store workflows through
  SlateDB/object-store configuration.

## Repository Layout

```text
src/core/        configuration, error types, public model types, cache policy
src/shard/       GraphShard lifecycle, writes, reads, query execution, maintenance
src/engine/      immutable graph indexes, GC, routed multi-reader runtime
src/query/       OpenCypher lowering, algebra, optimizer, transport, TCK parser
src/client/      shared public query service plus Bolt/TLS and HTTPS adapters
src/sparse_kernel.rs
                 Rust sparse traversal and optional SuiteSparse GraphBLAS FFI
examples/        smoke, stress, correctness, benchmark, and profiling binaries
scripts/         local, MinIO, query, write, stress, and chaos harnesses
charts/turbolay/ production Helm chart for graph nodes and indexer workers
architecture.md  high-level system design and low-level component flows
```

## Requirements

Base build:

- Rust stable 1.91 or newer
- `pkg-config`, `cmake`, `clang`, and normal C/C++ build tools
- Optional: `just` for recipe shortcuts

Native query/traversal features:

- `opencypher`: native `libcypher-parser`
SuiteSparse GraphBLAS is **not** a feature — it is always linked, because it is
the kernel we run in production, so its development headers and library are
required for a plain `cargo build`. To run traversals on a pure-Rust kernel
instead, switch at runtime; no rebuild is needed.

`GRAPH_SPARSE_KERNEL` selects one of `adjacency` (uncompiled BFS), `compact`
(compiled flat CSC, no C) or `suitesparse` (the default). **It is read by the
`graph-node` binary only** — embedders set `GraphCachePolicy::sparse_kernel`
directly, and it has no effect on `cargo test`. The older
`GRAPH_COMPILED_KERNEL=compact` changes the *default* that policy field starts
at, so it does apply to the library and its tests; an explicit
`GRAPH_SPARSE_KERNEL`, or a policy set in code, always wins over it.

Note that `adjacency` is a capability downgrade, not just a slower path. It has
no count or window pushdown, and it routes queries through the storage frontier,
which enforces `max_query_scan_edges` / `max_query_intermediate_rows` /
`max_query_result_vertices` — limits the compiled path does not apply. A large
traversal that succeeds on `suitesparse` can fail outright on `adjacency`.
- `query-transport-tls`: Rustls dependencies are pulled by Cargo; you provide
  certificates/configuration in the embedding service

Ubuntu or WSL:

```bash
sudo apt-get update
sudo apt-get install -y build-essential clang libclang-dev cmake pkg-config libcypher-parser-dev libgraphblas-dev
cargo install just --locked
```

macOS with Homebrew:

```bash
xcode-select --install
brew install rustup-init just cmake pkg-config llvm suite-sparse
brew install cleishm/neo4j/libcypher-parser
rustup-init
```

Then open a new shell and make `libcypher-parser` visible:

```bash
export PKG_CONFIG_PATH="$(brew --prefix libcypher-parser)/lib/pkgconfig:${PKG_CONFIG_PATH:-}"

pkg-config --exists cypher-parser
test -f "$(brew --prefix suite-sparse)/lib/libgraphblas.dylib"
```

You do **not** need to export `LIBRARY_PATH` or `RUSTFLAGS="-L ..."` for
SuiteSparse. `build.rs` resolves the GraphBLAS link-search path itself, in this
order: `GRAPHBLAS_LIB_DIR`, then `pkg-config --libs-only-L GraphBLAS`, then
`brew --prefix suite-sparse`/lib on macOS. It emits nothing when none resolve,
which is the correct behaviour on Debian/Ubuntu where `libgraphblas-dev` lands
on the default linker path.

## Build And Test

```bash
git clone https://github.com/usecortex/slatedb-graph-kernel.git
cd slatedb-graph-kernel

cargo test --locked --lib
GRAPH_COMPILED_KERNEL=compact cargo test --locked --lib   # pure-Rust compiled kernel
cargo test --locked --features opencypher --lib
cargo check --locked --examples --features opencypher
cargo test --locked --all-targets --features public-client-protocols
```

With `just`:

```bash
just
just ci
```

CI runs default and native feature checks on Ubuntu. macOS CI validates the
default and chaos-harness feature sets, so native `opencypher,graphblas` support
on macOS should be checked locally when those libraries are installed.

## Kubernetes Deployment

The production Helm chart deploys graph nodes, services, network policies,
disruption budgets, object-store configuration, and optional cert-manager,
External Secrets, and ServiceMonitor resources. It does not deploy a writable
separate graph controller database.

For a public single-node K3s evaluation host with Docker, Helm, `kubectl`, the
AWS CLI, and OpenSSL already installed, the deployment helper builds the current
checkout, provisions auth and TLS secrets, imports the image into K3s, and
installs the chart:

```bash
TURBOLAY_S3_BUCKET=graph-benchmark ./scripts/deploy_single_node_k3s.sh
```

On EC2 the public DNS name and IP are discovered automatically. Elsewhere, set
`TURBOLAY_PUBLIC_HOST`. The script prints the Bolt and HTTPS endpoints and keeps
the generated client token under `~/.config/turbolay-single-node/`.

```bash
helm upgrade --install turbolay charts/turbolay \
  --namespace turbolay \
  --create-namespace \
  --values charts/turbolay/examples/values-eks.yaml \
  --atomic \
  --timeout 15m
```

Copy the EKS example before use and replace its account, IAM role, DNS, issuer,
bucket, and image values. See `charts/turbolay/README.md` for TLS, authentication,
cache storage, upgrade, and verification details. Helm is the single supported
Kubernetes deployment source; environment values live in `hydradb-argocd`.

## Feature Flags

| Feature | Purpose |
|---|---|
| default | Storage kernel, graph APIs, Rust sparse traversal, no native parser/GraphBLAS |
| `opencypher` | Enables OpenCypher parsing and row query execution through `libcypher-parser-sys` |
| `graphblas` | Enables direct FFI to `libgraphblas` from `src/sparse_kernel.rs` |
| `chaos-harness` | Builds the hard-fence worker used by stress/failover scripts |
| `query-transport` | Enables TCP query client/server types and serde row frames |
| `query-transport-tls` | Adds TLS/mTLS config provider support for TCP query transport |
| `query-service-discovery` | Adds Kubernetes EndpointSlice, Consul, and etcd discovery helpers |
| `client-api` | Enables the protocol-independent authenticated client query service |
| `bolt-server` | Adds the Bolt 5.1-5.4 server and requires TLS unless plaintext is explicitly enabled |
| `http-api` | Adds the HTTPS typed JSON and NDJSON query API |
| `public-client-protocols` | Enables both Bolt and HTTPS public adapters |
| `server-runtime` | Builds the production graph-node service and native query stack |
| `indexer-runtime` | Builds the independent graph-indexer worker and admin server |

The GraphBLAS path does not depend on a Rust GraphBLAS crate. It links directly
to the system library with `#[link(name = "graphblas")]`.

## Object Store And Cache Model

The object store is the durable graph store. For local development use
`local_object_store(path)`. For MinIO/S3-compatible storage use
`object_store_from_env(...)`, which delegates to SlateDB's object-store loader.
The MinIO scripts generate env files containing values such as:

```text
CLOUD_PROVIDER=aws
AWS_ACCESS_KEY_ID=...
AWS_SECRET_ACCESS_KEY=...
AWS_REGION=us-east-1
AWS_DEFAULT_REGION=us-east-1
AWS_ENDPOINT=http://127.0.0.1:19000
AWS_BUCKET=...
AWS_ALLOW_HTTP=true
AWS_VIRTUAL_HOSTED_STYLE_REQUEST=false
```

Caching is two-layer:

- SlateDB object-store cache: configured through `GraphCacheConfig`, including
  disk cache directory, cache size, cache-on-put, and SST preload.
- Graph-layer memory cache: configured through `GraphCachePolicy`, including
  matrix adjacencies, compiled GraphBLAS matrices, parsed row queries, and
  hydration concurrency. `GraphMemoryConfig` independently caps compiled
  matrices and all relationship-result cache payloads by resident bytes;
  oversized query results remain correct but are not retained.

Derived topology is not written back into SlateDB. Indexer workers publish
immutable CSC objects under `_graph_index/<cell>/<edge-type>/generations/` and
atomically advance a small `current` pointer. Query nodes discover that pointer,
hydrate the selected generation through the normal object-store/NVMe cache, and
retain only bounded compiled matrices in memory. Generation GC retains the
configured number of previous objects and never downloads CSC payloads merely
to identify their sequence.

## Falkor S3 Import

Falkor JSONL exports can be imported directly from the same object store used by
the graph kernel. The importer accepts manifests with either a legacy `graph`
field or the Falkor export identity fields `org_id` plus `tenant_id`.

Expected source prefix layout:

```text
<source-prefix>/manifest.json
<source-prefix>/nodes.jsonl
<source-prefix>/edges.jsonl
```

Run an S3 import by pointing `object_store_from_env` at the destination bucket
and passing the export object prefix:

```bash
export CLOUD_PROVIDER=aws
export AWS_BUCKET=graph-benchmark
export AWS_REGION=us-east-1
export AWS_DEFAULT_REGION=us-east-1

cargo run --features json-properties --example falkor_import -- \
  --source-prefix 2026-07-08/gjnh5kebnw/7gezp2vebo \
  --db-path __slatedb_graph_kernel/imports/gjnh5kebnw/7gezp2vebo \
  --cell-id 7gezp2vebo \
  --duplicate-policy preserve \
  --build-artifacts
```

`preserve` is the only supported duplicate policy because Falkor exports can be
multigraphs. Relationship ids from Falkor are stored as graph relationship ids,
and non-integral JSON numbers such as timestamps are stored as float properties.

After import, reopen the graph from S3 and run a row query:

```bash
cargo run --features opencypher --example cypher_query -- \
  --db-path __slatedb_graph_kernel/imports/gjnh5kebnw/7gezp2vebo \
  --cell-id 7gezp2vebo \
  --query "MATCH (u)-[r:RELATES]->(v) RETURN count(*) AS total"
```

Benchmark the same imported S3 data across cold, warm, and hot cache paths:

```bash
cargo run --features opencypher --example falkor_query_bench -- \
  --db-path __slatedb_graph_kernel/imports/gjnh5kebnw/7gezp2vebo \
  --cell-id 7gezp2vebo \
  --query "MATCH (u {id: 11})-[r:RELATES]->(v {id: 10}) RETURN r.raw_relation AS raw" \
  --cache-dir target/slatedb-graph-s3-cache \
  --cold-iters 3 \
  --warm-iters 3 \
  --hot-iters 50
```

The benchmark opens a fresh reader without disk cache for `cold`, reopens with a
seeded SlateDB disk cache for `warm`, and reuses one open reader for `hot` so
graph-layer memory caches are active. Set `--cold-iters 0`, `--warm-iters 0`,
or `--hot-iters 0` to skip a cache phase during focused investigations.

## Write And Read APIs

The main embedding type is `GraphShard`. It can be opened as a read shard, a
standalone writer, or through a routed `GraphNode`/`RoutedGraphCluster`.

Common write paths:

- `write_edge`, `write_edge_with_vertex_metadata`, `write_edge_with_full_metadata`
- `delete_edge`
- `write_edge_mutations_batch`
- `ingest_edge_mutations`
- `bulk_import_edges`, `bulk_import_edges_chunked`
- `bulk_append_edges_trusted_chunked`
- `bulk_append_out_adjacency_segment_trusted`

Common read and matrix paths:

- `edge_exists`, `out_neighbors`, `out_degree`
- `snapshot` (a stable current SlateDB snapshot); `snapshot_at` validates current bookmarks and rejects detached historical replay
- `build_adjacency_image`, `build_matrix_tiles`
- `matrix_reachable`, `matrix_reachable_with_kernel`, `direct_snapshot_reachable`
- `delete_graph_artifacts_before`
- `export_live_graph_digest`, `verify_current_graph`

Minimal local example:

```rust
use slatedb_graph_kernel::{local_object_store, EdgeMutation, GraphShard, Result};

#[tokio::main]
async fn main() -> Result<()> {
    let store = local_object_store("target/local-graph-store")?;
    let shard = GraphShard::open_standalone_writer("demo-db", store).await?;

    shard
        .write_edge(EdgeMutation {
            cell_id: "reddit-home".to_string(),
            edge_type: "FOLLOWS".to_string(),
            src: 1,
            dst: 2,
            idempotency_key: "follow-1-2".to_string(),
        })
        .await?;

    let epoch = shard.current_epoch("reddit-home").await?;
    let neighbors = shard
        .out_neighbors_at("reddit-home", "FOLLOWS", 1, epoch)
        .await?;

    assert_eq!(neighbors, vec![2]);
    shard.close().await
}
```

## Query Engine

OpenCypher support is enabled with `--features opencypher`. The row engine
returns `QueryResultSet` or `QueryResultPage` and is available on `GraphShard`,
`RoutedGraphCluster`, and the optional TCP transport.

Currently supported query shapes include:

- `MATCH ... RETURN` over node patterns and directed typed relationships.
- Multi-edge path patterns in one `MATCH`.
- `OPTIONAL MATCH`.
- `UNION` and `UNION ALL`, preserving `UNION ALL` leg order.
- Bounded variable-length relationships such as `[:FOLLOWS*1..20]`.
- Node labels, node properties, relationship properties, and `id` constraints.
- Property values support unsigned integers, floats, booleans, and strings.
  JSON import helpers preserve non-negative JSON integers as integers and
  floating JSON numbers as native floats.
- `WHERE` boolean combinations over property/id comparisons with `=`, `<>`,
  `<`, `>`, `<=`, and `>=`.
- `ORDER BY`, `SKIP`, `LIMIT`, aliases, and parameter values.
- `DISTINCT` row projection and deduplication before windowing.
- Aggregates: `count(*)`, `count(expr)`, `sum(expr)`, `avg(expr)`,
  and `collect(expr)`.
- Mutations: `CREATE` edge patterns, `MERGE` edge patterns, relationship and
  vertex `DELETE`, `DETACH DELETE`, and `SET`/`REMOVE` labels and properties.

Known query limits:

- Unbounded variable-length paths are rejected; provide an explicit max hop.
- Undirected relationships are rejected.
- `RETURN *` is rejected.
- `WITH` is pass-through only; it must keep every in-scope binding unchanged.
- Native row execution is materialized for many plans; page APIs and the graph
  kernel fast path avoid materializing the hottest bounded reachability cases.

Example:

```rust
use slatedb_graph_kernel::{QueryContext, QueryValue};

let rows = shard
    .execute_cypher_rows(
        QueryContext::new("reddit-home", "query-1").with_timeout_ms(30_000),
        "MATCH (u {id: 1})-[:FOLLOWS*1..5]->(v) \
         RETURN v.id AS id ORDER BY id LIMIT 10",
    )
    .await?;

for row in rows.rows {
    if let [QueryValue::VertexId(id)] = row.values.as_slice() {
        println!("{id}");
    }
}
```

### Native path procedures

`algo.SPpaths` returns bounded paths between two vertex ids. `algo.SSpaths`
returns bounded paths starting at one vertex id. `algo.MSpaths` resolves many
source and optional target vertices through a property index and evaluates all
of them in one request. All three execute against one pinned SlateDB snapshot
and use the current compiled GraphBLAS CSC plus its WAL tail when an index is
available.

```cypher
CALL algo.SPpaths({
  sourceNode: $source,
  targetNode: $target,
  relTypes: ['RELATES'],
  relDirection: 'both',
  maxLen: 3,
  weightProp: 'weight',
  costProp: 'cost',
  maxCost: 10,
  pathCount: 5
})
YIELD path, pathWeight, pathCost
RETURN path, pathWeight, pathCost
```

`sourceNode` and `targetNode` are non-negative vertex ids. Scalar options may
be literals or parameters. `pathCount: 0` returns every path tied at the
minimum weight; a positive value returns at most that many paths in ascending
weight, cost, hop-count, and deterministic topology order. Paths are simple
(a vertex is not revisited), `maxLen` is capped by the runtime traversal limit,
and query edge, intermediate-row, result, byte, cancellation, and timeout
budgets remain enforced. Bolt clients receive a native Bolt `PATH`, including
the original direction of relationships traversed in either direction.

Use `algo.MSpaths` to replace client-side path-query fan-out. Selector values
must resolve through the named property index. With `pairwise: true`, duplicate
self and symmetric source/target pairs are omitted. `pathCount` is enforced per
source/target pair and `resultLimit` bounds the complete response. For
unweighted pairwise reads, `fairRelationshipVariants: true` selects up to
`pathCount` structural paths and then returns every concrete parallel-edge
combination for those paths within `resultLimit`. Variants are admitted in
round-robin structural-path order so one highly connected pair cannot consume
the response budget before other selected paths contribute a result.

```cypher
CALL algo.MSpaths({
  sourceLabel: 'Entity',
  sourceProperty: 'name',
  sourceValues: ['alpha', 'beta', 'gamma'],
  targetValues: ['alpha', 'beta', 'gamma'],
  pairwise: true,
  relTypes: ['RELATES'],
  relDirection: 'both',
  maxLen: 3,
  pathCount: 5,
  fairRelationshipVariants: true,
  resultLimit: 100
})
YIELD path
RETURN path
```

## Public Client Protocols

Enable both public adapters with `--features public-client-protocols`. Create a
single `ClientQueryService` over the routed cluster, then pass clones of that
service to `ClientBoltServer` and `ClientHttpServer`. The shared service:

- classifies OpenCypher before authorization so read grants cannot run writes;
- enforces graph and hierarchical namespace grants;
- applies global and namespace concurrency limits;
- caps query size, parameter count, page size, and total stream runtime;
- pins paged reads to one SlateDB storage sequence and emits scoped bookmarks;
- supports cancellation without letting query ids cross principals or scopes.

There are exactly two read-consistency modes:

- `causal` is the default hot path. It uses the node's current durable reader
  view and refreshes only when a supplied bookmark requires a newer sequence.
- `strong` refreshes the SlateDB reader from object storage before pinning the
  query snapshot, so it observes every durable write committed before that
  refresh completed. It intentionally pays the object-store freshness check.

HTTPS accepts `"consistency": "causal"` or `"strong"` in the query body. Bolt
accepts the same value in RUN metadata as `consistency`, or as
`tx_metadata["turbolay.consistency"]`. Mutation queries reject `strong` because
write acknowledgement already defines their durable commit point.

Bolt supports `HELLO`, `LOGON`, `LOGOFF`, auto-commit `RUN`, bounded or complete
`PULL`/`DISCARD`, `RESET`, `GOODBYE`, `ROUTE`, and telemetry acknowledgement.
Explicit `BEGIN`, `COMMIT`, and `ROLLBACK` are rejected until cross-query
transaction semantics exist. TLS is required by default. Configure
`ObjectStoreBoltRoutingTableProvider` advertises every node for reads and one
stable preferred node for writes. This preserves writer/cache locality only;
direct writes to another promotable node remain safe because SlateDB owns the
writer fence. If no routing provider or complete static routing table is
configured, `ROUTE` fails closed; direct `bolt://` sessions continue to work.

The HTTPS API exposes:

```text
POST /v1/graphs/{graph_id}/query
POST /v1/graphs/{graph_id}/queries/{query_id}/cancel
GET  /healthz
```

Query requests require `Authorization: Bearer ...` and, by default, an
`x-graph-namespace` header. Send `Accept: application/x-ndjson` to stream a
header, typed row records, and a final bookmark summary across bounded backend
cursor pages. HTTPS and Bolt plaintext modes are available only through methods
whose names explicitly begin with `insecure_`.

## Optimizer And Stats

The row query optimizer uses both structural heuristics and persisted stats.
It can select vertex label/property indexes, edge property indexes, full scans,
bound expands, reverse expands, expand-into, graph-kernel reachability, and hash
join shortcuts.

Stats APIs include:

- `refresh_edge_type_query_stats`
- `refresh_vertex_label_query_stats`
- `refresh_vertex_property_query_stats`
- `refresh_edge_property_query_stats`
- `refresh_vertex_property_histogram_query_stats`
- `refresh_edge_property_histogram_query_stats`
- `start_query_stats_refresh_job`

Stats refresh scans run outside write locks and publish with snapshot
revalidation.

## Useful Commands

```bash
just smoke                    # local object-store smoke
just smoke-graphblas          # local smoke with GraphBLAS traversal
just stress                   # local multiprocess stress and recovery checks
just fence                    # local stale-writer/fence takeover proof
just query-bench              # OpenCypher hot/warm/cold query benchmark
just query-correctness        # exact query correctness checks
just query-memory-profile     # low-memory query/build/concurrency profile
just minio-smoke              # Docker MinIO smoke
just minio-chaos              # Docker MinIO chaos
just minio-fence              # MinIO fence takeover proof
just minio-query-bench        # MinIO query benchmark
just minio-query-correctness  # MinIO query correctness checks
```

Benchmark and correctness outputs are written under `bench-results/`.

To enable GraphBLAS for query benchmarks on machines with native GraphBLAS:

```bash
GRAPH_QUERY_BENCH_FEATURES=opencypher,graphblas \
GRAPH_QUERY_BENCH_MAX_GRAPHBLAS_MATRICES=1 \
just query-bench
```

## Storage And Concurrency Model

The production data path follows SlateDB's single-writer, multi-reader model:

1. Every node lazily opens a `DbReader` for every configured cell and keeps only
   disposable memory/NVMe caches locally.
2. A write-routed node lazily opens and caches the SlateDB writer. SlateDB's
   writer epoch and WAL barrier fence any competing or stale writer.
3. Every query pins one `DbSnapshot` or `DbReaderSnapshot`, so canonical records
   and indexes come from exactly one durable SlateDB storage sequence.
4. Bookmarks are SlateDB commit sequences. A reader explicitly refreshes and
   waits until its durable sequence reaches the bookmark before serving data.
5. Indexer workers pin a durable SlateDB reader snapshot, build one immutable
   CSC generation, and publish its base sequence plus exact WAL cursor through
   an object-store compare-and-swap pointer.
6. Query nodes never rebuild a full topology index. They run GraphBLAS over the
   discovered base and overlay committed topology changes from the SlateDB WAL
   through the pinned query sequence. If the required WAL has already been
   compacted away, the query uses bounded source-scoped canonical reads.
7. Bolt routing uses one preferred writer address as soft cache affinity and all
   nodes as readers. It is not a correctness or ownership database.
8. Public pagination uses bounded server-held result cursors, so continuation
   pages never replay a query or publish graph-owned retention records.

This keeps S3 as the durable source of truth, local SSD/NVMe as SlateDB's block
cache, and memory for bounded query plans and compiled GraphBLAS matrices.

## Production Boundary

The repository contains the deployable graph-node service, storage/query engine,
public protocols, chart, stress tools, and verification paths. Production
promotion still requires environment-specific evidence rather than more storage
coordination code:

- long-running multi-node and real-S3 soak tests under throttling, latency,
  timeout, restart, and membership-change faults;
- dashboards and alerts for SlateDB fencing, object-store errors, query latency,
  memory budgets, graph-index lag, WAL-tail size, and rejected work;
- an explicit OpenCypher compatibility policy backed by larger TCK reports;
- deployment certificate rotation, secret management, backup/restore drills,
  and tested rollback procedures.

The safest way to evaluate a new environment is: run default tests, native
feature tests, local smoke, MinIO smoke, query correctness, stress, and then a
long soak using the same object store and cache settings intended for deployment.
