# SlateDB Graph Kernel

`slatedb-graph-kernel` is a Rust graph storage and query kernel built on top of
SlateDB. The object store is the durable source of truth, while local SSD/NVMe
and memory are cache layers controlled through SlateDB settings and graph-layer
cache policy.

The crate is intentionally kept outside the SlateDB repository. SlateDB stays an
upstream dependency pinned in `Cargo.toml`/`Cargo.lock`, and this crate owns the
graph model, write fencing, artifact layout, traversal kernels, query planner,
and operational harnesses.

## What Is Implemented

- Durable edge writes, deletes, bulk import, trusted segment append, mutation
  logs, idempotency records, and degree/index maintenance.
- Per-cell hard write fencing with object-store lock records, lease generation
  checks, and SlateDB transactional retries.
- Routed multi-cell clusters, graph nodes with lease renewal, placement metadata,
  control-plane watermarks, failover helpers, and repair paths.
- Snapshot reads, read leases, retention checks, rollup, delta GC, artifact GC,
  and full graph export/verifier digests.
- Posting chunks, matrix tiles, supernode chunk indexes, paginated supernode
  scans, supernode membership checks, and intersection helpers.
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
src/engine/      artifacts, supernodes, rollup/GC, cluster/control-plane helpers
src/query/       OpenCypher lowering, algebra, optimizer, transport, TCK parser
src/client/      shared public query service plus Bolt/TLS and HTTPS adapters
src/sparse_kernel.rs
                 Rust sparse traversal and optional SuiteSparse GraphBLAS FFI
examples/        smoke, stress, correctness, benchmark, and profiling binaries
scripts/         local, MinIO, query, write, stress, and chaos harnesses
charts/turbolay/ production Helm chart for graph nodes and controllers
```

## Requirements

Base build:

- Rust stable 1.91 or newer
- `pkg-config`, `cmake`, `clang`, and normal C/C++ build tools
- Optional: `just` for recipe shortcuts

Native query/traversal features:

- `opencypher`: native `libcypher-parser`
- `graphblas`: SuiteSparse GraphBLAS development headers and library
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

Then open a new shell and make the native libraries visible when using native
features:

```bash
export PKG_CONFIG_PATH="$(brew --prefix libcypher-parser)/lib/pkgconfig:${PKG_CONFIG_PATH:-}"
export LIBRARY_PATH="$(brew --prefix suite-sparse)/lib:${LIBRARY_PATH:-}"
export DYLD_FALLBACK_LIBRARY_PATH="$(brew --prefix suite-sparse)/lib:${DYLD_FALLBACK_LIBRARY_PATH:-}"

pkg-config --exists cypher-parser
test -f "$(brew --prefix suite-sparse)/lib/libgraphblas.dylib"
```

## Build And Test

```bash
git clone https://github.com/usecortex/slatedb-graph-kernel.git
cd slatedb-graph-kernel

cargo test --locked --lib
cargo test --locked --features opencypher --lib
cargo test --locked --features graphblas --lib
cargo test --locked --features opencypher,graphblas --lib
cargo check --locked --examples --features opencypher,graphblas
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

The production Helm chart deploys graph nodes, controller candidates, services,
RBAC, network policies, disruption budgets, object-store configuration, and
optional cert-manager, External Secrets, and ServiceMonitor resources.

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
  matrix artifacts, matrix adjacencies, compiled GraphBLAS matrices, parsed
  row queries, reachability results, supernode groups, posting chunks, and
  hydration concurrency.

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
- `append_edge_mutation_log`
- `bulk_import_edges`, `bulk_import_edges_chunked`
- `bulk_append_edges_trusted_chunked`
- `bulk_append_supernode_segment_trusted`

Common read/artifact paths:

- `edge_exists`, `out_neighbors`, `out_degree`
- `snapshot`, `snapshot_at`
- `build_posting_chunks`, `build_matrix_tiles`, `build_supernode_groups`
- `matrix_reachable`, `matrix_reachable_with_kernel`, `posting_reachable`
- `supernode_page`, `supernode_degree`, `supernode_edge_exists`,
  `supernode_intersection`
- `rollup_artifacts`, `delete_deltas_through_rollup`,
  `delete_graph_artifacts_before`
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

## Public Client Protocols

Enable both public adapters with `--features public-client-protocols`. Create a
single `ClientQueryService` over the routed cluster, then pass clones of that
service to `ClientBoltServer` and `ClientHttpServer`. The shared service:

- classifies OpenCypher before authorization so read grants cannot run writes;
- enforces graph and hierarchical namespace grants;
- applies global and namespace concurrency limits;
- caps query size, parameter count, page size, and total stream runtime;
- pins paged reads to one graph epoch and emits scoped graph bookmarks;
- supports cancellation without letting query ids cross principals or scopes.

Bolt supports `HELLO`, `LOGON`, `LOGOFF`, auto-commit `RUN`, bounded or complete
`PULL`/`DISCARD`, `RESET`, `GOODBYE`, `ROUTE`, and telemetry acknowledgement.
Explicit `BEGIN`, `COMMIT`, and `ROLLBACK` are rejected until cross-query
transaction semantics exist. TLS is required by default. Configure
`ControllerBoltRoutingTableProvider` with public Bolt addresses to derive read
and route endpoints from live controller heartbeats and the write endpoint from
the current unexpired cell lease. If no routing provider or complete static
routing table is configured, `ROUTE` fails closed; direct `bolt://` sessions
continue to work.

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
just bench                    # path/supernode benchmark
just bench-rust               # same benchmark with Rust sparse traversal
just query-bench              # OpenCypher hot/warm/cold query benchmark
just query-correctness        # exact query correctness checks
just query-memory-profile     # low-memory query/build/concurrency profile
just minio-smoke              # Docker MinIO smoke
just minio-chaos              # Docker MinIO chaos
just minio-fence              # MinIO fence takeover proof
just minio-bench              # MinIO path/supernode benchmark
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

## Production Boundary

This crate is a kernel/library, not a complete hosted database service. It now
contains the major storage, query, routing, fencing, controller, artifact,
stress, and verification pieces needed for an object-store graph database, but
production use still requires an embedding service and operational validation
around it:

- deployment-specific rollout policy and service lifecycle integration around
  `GraphControlPlane`, `GraphClusterControllerHandle`, and `ManagedGraphNode`;
- production metrics export, dashboards, alerts, tenant quotas, and
  backpressure policy choices;
- long-running multi-process and real S3 soak tests under throttling, latency,
  timeout, and restart faults;
- complete compatibility policy for the OpenCypher subset, including larger TCK
  reports and documented skip reasons;
- security integration for the TCP transport, including real certificate
  rotation and secret management.

The safest way to evaluate a new environment is: run default tests, native
feature tests, local smoke, MinIO smoke, query correctness, stress, and then a
long soak using the same object store and cache settings intended for deployment.
