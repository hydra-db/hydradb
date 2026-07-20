#import "../template.typ": term, why, srcblock, figcap, accent, muted
#import "@preview/fletcher:0.5.7" as fletcher: diagram, node, edge
#import "../vendor/bookly/src/themes/reader.typ": reader-colors

= Architecture

Chapter 0 gave you the vocabulary. This chapter shows how the code is organized, which
build contains which parts, and how the pieces connect into one running system. The aim is
that when you open the tree you know which file to look in and why it exists.

We move from the outside in. First the build shapes (Cargo features), then the module map,
then the single type that ties storage and graph together (`GraphShard`), then how an
incoming query name is resolved down to storage keys, how one process becomes a cluster of
symmetric nodes, and finally where the separate out-of-process indexer fits.

== One crate, many builds

TurboLay is a single Rust crate. It is not a workspace of many crates. Instead it uses Cargo
features to include or exclude large slices of itself, so the same source can build as a
small embedded library or as a full network server.

#term("Cargo feature")[
  A named, optional slice of a crate. Code guarded by `#[cfg(feature = "x")]` only compiles
  when feature `x` is turned on. Features can depend on other features, so turning one on can
  pull in a whole stack. This lets one crate ship many configurations.
]

The feature list is a tower. Each higher feature turns on the ones below it:

#srcblock("Cargo.toml, [features]")[```toml
default = []
opencypher = ["dep:libcypher-parser-sys"]
graphblas = []
query-transport = ["opencypher", "dep:serde", "dep:subtle", "json-properties", ...]
query-transport-tls = ["query-transport", "dep:tokio-rustls"]
query-service-discovery = ["query-transport", "dep:base64", "dep:reqwest"]
client-api = ["query-transport"]
bolt-server = ["client-api", "query-transport-tls", "dep:boltr"]
http-api = ["client-api", "query-transport-tls", "dep:axum", "dep:axum-server"]
public-client-protocols = ["bolt-server", "http-api"]
server-runtime = ["public-client-protocols", "graphblas", "query-service-discovery", ...]
indexer-runtime = ["dep:axum", "dep:tracing-subscriber", "tokio/net"]
```]

Read from the bottom up:

- With *no features* you get the embedded engine: the graph model, keys, codec, the shard,
  the storage stack, writes, reads by direct API, and garbage collection. No query language,
  no network.
- `opencypher` adds the Cypher query language (it pulls in a native parser,
  `libcypher-parser-sys`).
- `query-transport` adds the internal query network protocol and the types needed to send a
  query between nodes. `client-api` builds the shared client service on top.
- `bolt-server` and `http-api` add the two public front doors. `public-client-protocols`
  turns on both.
- `server-runtime` is the full deployable build of the *data node*: both front doors, the
  GraphBLAS traversal backend, service discovery for finding other nodes, and logging. This is
  what the `graph-node` binary compiles with.
- `indexer-runtime` is a deliberately *lean, parallel* build — not part of the tower above. It
  pulls in only an HTTP admin surface, logging, and Tokio networking (no front doors, no
  GraphBLAS, no discovery). It is what the separate `graph-indexer` binary compiles with, and
  it exists so the out-of-process index builder (Section 1.8) stays small and cheap to run.

#figure(
  diagram(
    node-stroke: 0.6pt,
    spacing: (0pt, 0.72cm),
    node((0, 0), [`server-runtime` → `graph-node` (full data node: both front doors, GraphBLAS, discovery)], fill: rgb("#e9fce9"), width: 11.5cm),
    node((0, 1), [`public-client-protocols` = `bolt-server` + `http-api`], fill: rgb("#eef4ff"), width: 11.5cm),
    node((0, 2), [`client-api` (shared `ClientQueryService`)], fill: rgb("#eef4ff"), width: 11.5cm),
    node((0, 3), [`query-transport` (query wire protocol, auth, quotas)], fill: rgb("#eef4ff"), width: 11.5cm),
    node((0, 4), [`opencypher` (Cypher parser and planner)], fill: rgb("#fff8e6"), width: 11.5cm),
    node((0, 5), [default (embedded engine: model, keys, codec, shard, storage)], fill: rgb("#f6f8fa"), width: 11.5cm),
    node((0, 6), [`indexer-runtime` → `graph-indexer` (lean, separate: admin HTTP + engine, no front doors)], fill: rgb("#f3e9fc"), width: 11.5cm),
  ),
  caption: none,
)
#figcap[The feature tower. A build is a horizontal slice: everything from some level down. The deployable *data node* (`graph-node`) builds with `server-runtime`; the two example servers in `examples/` build with `bolt-server`. `indexer-runtime` (bottom, shaded) is not part of the tower — it is a separate lean slice over the embedded engine that produces the `graph-indexer` binary.]

#why[
  A single crate with features, rather than many small crates, keeps the internal types
  shared without a web of crate boundaries and version bumps. The cost is that you must read
  the `#[cfg(...)]` attributes to know whether a given item exists in your build. When you
  cannot find where a `pub use` symbol comes from, check whether it is behind a feature.
]

== The module map

The crate root, `src/lib.rs`, declares the modules and re-exports the public API. Every
module lives under `src/`. Grouped by job:

#table(
  columns: (auto, 1fr),
  inset: 5pt,
  align: (left + top, left + top),
  stroke: 0.4pt + rgb("#d0d7de"),
  [*Area*], [*What lives there*],
  [`core/`], [The foundations: `state.rs` (the `GraphShard` type and write authority), `model.rs` (vertex, edge, relationship, property types), `config.rs` (open options and the SlateDB open functions), `namespace.rs` (namespaces, graph ids, scopes), `snapshot.rs`, `cache.rs`, `metrics.rs`, `error.rs`, `write_batch.rs`.],
  [`keys.rs`], [Every storage key builder. One function per key shape. 316 lines of pure key formatting.],
  [`codec.rs`], [Every value encoder and decoder. Hand-written, versioned byte formats. 1543 lines.],
  [`shard/`], [The behavior of `GraphShard`, split by concern: `write.rs` (5108 lines), `query.rs` (8389 lines), `query_optimizer.rs`, `lifecycle.rs`, `maintenance.rs`, and `topology_tail.rs` (the recent-WAL overlay for reads, Section 1.7).],
  [`engine.rs` + `engine/`], [Everything above a single shard: `cluster.rs` (the `GraphCluster` / `RoutedGraphCluster` containers and the `ObjectStoreNodeDirectory` that routes them), `artifact_build.rs` (building durable adjacency images), `index_store.rs` (the durable CSC *index generations* and their manifest/GC, Section 1.7), `artifact_gc.rs` (pruning old artifacts), `matrix_cache.rs` (hydrating artifacts into the in-memory matrix caches), `traversal.rs`, and `verify.rs`. There is no `artifact_refresh.rs`: index building now runs out-of-process (Section 1.8).],
  [`query/`], [The query language and distribution: `opencypher.rs` (parse Cypher into the engine's own plan), `algebra.rs` (the plan types), `coordination.rs` (the query network protocol and distributed execution).],
  [`client/`], [The public front doors: `service.rs` (the shared `ClientQueryService`), `bolt.rs` (the Bolt server), `http.rs` (the HTTPS server).],
  [`sparse_kernel.rs`], [Sparse-matrix traversal, either in pure Rust or through the GraphBLAS C library.],
  [`placement.rs`], [Experiments in how vertex ids map to cells for locality.],
  [`bin/`], [Two executables: `graph-node` (the data node, `server-runtime`) and `graph-indexer` (the out-of-process index builder, `indexer-runtime`).],
)

The line counts tell you where the real work is. `shard/query.rs` and `shard/write.rs`
together are over thirteen thousand lines. Those two files are the subject of the read and
write chapters.

== GraphShard: the type that ties it together

#term("GraphShard")[
  The central type of the engine. One `GraphShard` is one open handle to one cell's data on
  the object store, plus everything needed to read and write it: the SlateDB storage handle,
  the configured limits and policies, the concurrency controls, and all the in-memory caches.
  Nearly every operation in the engine is a method on `GraphShard`.
]

Its fields fall into four groups. Storage and configuration first:

#srcblock("src/core/state.rs:33-50")[```rust
pub struct GraphShard {
    pub(crate) db: GraphStore,
    pub(crate) limits: GraphLimits,
    pub(crate) cache_policy: GraphCachePolicy,
    // ... cache and operation metrics, then the concurrency gates
    pub(crate) index_policy: GraphIndexPolicy,
    pub(crate) await_durable_writes: bool,
    pub(crate) write_authority: GraphWriteAuthority,
    pub(crate) local_write_guard: Arc<Mutex<()>>,
    pub(crate) local_artifact_guard: Arc<Mutex<()>>,
    pub(crate) writer_lanes: Vec<Mutex<()>>,
```]

`db` is the `GraphStore` from Chapter 0. Where the durable data lives (`object_store`,
`store_path`) now lives *inside* `GraphStore` rather than on the shard. `write_authority` and
`writer_lanes` are the write-authority and lane machinery from Section 0.10 and 0.11;
`write_authority` is one of `ReadOnly`, `Promotable`, or `Writer` (Section 1.6).

Second, the concurrency gates. Each is a semaphore that bounds how many of one kind of
expensive operation can run at once:

#srcblock("src/core/state.rs:39-44")[```rust
    pub(crate) hydration_gate: Arc<Semaphore>,
    #[cfg(feature = "graphblas")]
    pub(crate) matrix_compilation_gate: Arc<Semaphore>,
    pub(crate) graph_write_gate: Arc<Semaphore>,
    pub(crate) artifact_build_gate: Arc<Semaphore>,
    pub(crate) gc_gate: Arc<Semaphore>,
```]

#term("Semaphore (as used here)")[
  A counter that hands out a fixed number of permits. A task must take a permit before doing
  the guarded work and returns it when done. If no permit is free the task waits. TurboLay
  uses one semaphore per class of heavy work (loading data into memory, compiling a matrix,
  writing, building artifacts, garbage collecting) so that, for example, a burst of writes
  cannot starve reads of memory.
]

Third, the caches. There are many, one per kind of computed result, and each is a
`BoundedGraphCache` (the subject of the caching chapter):

#srcblock("src/core/state.rs:51-69")[```rust
    pub(crate) matrix_artifact_cache:
        Mutex<BoundedGraphCache<MatrixCacheKey, engine::MatrixArtifact>>,
    pub(crate) graph_index_generations:
        Mutex<BTreeMap<MatrixCacheKey, engine::GraphIndexGeneration>>,
    pub(crate) matrix_cache: Mutex<BoundedGraphCache<MatrixCacheKey, Arc<MatrixAdjacency>>>,
    pub(crate) graphblas_cache:
        Mutex<BoundedGraphCache<MatrixCacheKey, Arc<sparse_kernel::CompiledGraphBlasMatrix>>>,
    #[cfg(feature = "opencypher")]
    pub(crate) parsed_row_query_cache:
        Mutex<BoundedGraphCache<ParsedRowQueryCacheKey, ParsedRowQuery>>,
    #[cfg(feature = "opencypher")]
    pub(crate) relationship_rows_cache:
        Mutex<BoundedGraphCache<RelationshipRowsCacheKey, RelationshipRowsCacheValue>>,
    #[cfg(feature = "opencypher")]
    pub(crate) source_relationship_rows_cache:
        Mutex<BoundedGraphCache<SourceRelationshipRowsCacheKey, Arc<Vec<VertexId>>>>,
    #[cfg(feature = "opencypher")]
    pub(crate) relationship_property_rows_cache:
        Mutex<BoundedGraphCache<RelationshipPropertyRowsCacheKey, RelationshipRowsCacheValue>>,
```]

The traversal-acceleration state sits at the top: `matrix_artifact_cache` holds durable
artifact metadata, `graph_index_generations` is the in-memory registry of the current durable
*index generation* per `(cell, edge type)` (Section 1.7), and `matrix_cache` /
`graphblas_cache` hold the hydrated adjacency and the compiled GraphBLAS matrix. The rest are
per-read result caches behind the query language.

The `#[cfg(feature = "opencypher")]` on some caches is the feature tower showing through:
a build without the query language does not carry the query-result caches.

The single-writer rule used to be an enum: `GraphStore` was either a `Writer(Db)` or a
`Reader(DbReader)`. After the graph-kernel resync it is instead a small handle that opens
its writer and reader *lazily* and caches them:

#srcblock("src/core/state.rs:72-87")[```rust
#[derive(Clone)]
pub(crate) struct GraphStore { inner: Arc<GraphStoreInner> }

struct GraphStoreInner {
    path: Path,
    object_store: Arc<dyn ObjectStore>,
    // ... cache / memory / durability config ...
    writer: StdRwLock<Option<Db>>,           // opened on first write
    reader: AsyncRwLock<Option<Arc<DbReader>>>,
    writer_open_gate: Mutex<()>,
    reader_open_gate: Mutex<()>,
}
```]

`writer()` no longer matches a variant; it returns the cached SlateDB writer, or fails if one
was never promoted — so a node that is not allowed to write still cannot:

#srcblock("src/core/state.rs:183-185")[```rust
pub(crate) fn writer(&self) -> Result<Db> {
    self.open_writer().ok_or(GraphError::ReadOnlyShardStorage)
}
```]

The `Writer` / `Reader` distinction did not disappear — it moved to the *snapshot* level
(`GraphStorageSnapshot::Writer` / `Reader`, Chapter 2) — while *who may write* is now governed
by the shard's `write_authority` and the lazy promotion in Section 1.6.

== The behavior is split by concern

`GraphShard` is one struct, but its methods are spread across the `shard/` module so that
each file holds one concern. The module simply lists them:

#srcblock("src/shard.rs:3-10")[```rust
mod lifecycle;
mod maintenance;
mod query;
#[cfg(feature = "opencypher")]
mod query_optimizer;
#[cfg(feature = "graphblas")]
pub(crate) mod topology_tail;
mod write;
```]

- `lifecycle.rs`: opening, closing, format-version checks, and *promoting* a reader shard to a
  writer on demand (Section 1.6).
- `write.rs`: everything that mutates (the write chapter).
- `query.rs`: everything that reads and answers queries (the read chapter).
- `query_optimizer.rs`: choosing how to run a query using stored statistics.
- `topology_tail.rs`: overlaying edges written *since* the current index generation onto reads,
  so a query sees a consistent picture even when the indexer lags (Section 1.7).
- `maintenance.rs`: background upkeep, including the garbage collection in the delete chapter.

Each file has its own `impl GraphShard { ... }` block. Rust allows a type's methods to be
defined in several `impl` blocks across files, and TurboLay uses that to keep each file
focused. When you look for a method, pick the file by concern.

== From a client name to storage keys

A client does not know about cells or epochs. It connects and names a database. The server
turns that name into a concrete target, and the target carries the `cell_id` that every
storage key needs. This resolution is worth following closely because it connects the
outward identity (Chapter 0, Section 0.9) to the storage keys (Section 0.7).

The target is a scope plus a cell id:

#srcblock("src/client/service.rs:38-42")[```rust
pub struct ClientQueryTarget {
    pub scope: GraphScope,
    pub cell_id: String,
}
```]

The mapping from a database name to a target goes through a trait, so a deployment can plug
in its own policy:

#srcblock("src/client/service.rs:52-54")[```rust
pub trait ClientDatabaseResolver: Send + Sync {
    fn resolve_database(&self, database: Option<&str>) -> Result<ClientQueryTarget>;
}
```]

The shipped implementation is a static table from name to target, with an optional default
name used when the client does not specify one:

#srcblock("src/client/service.rs:96-124 (abridged)")[```rust
impl ClientDatabaseResolver for StaticClientDatabaseResolver {
    fn resolve_database(&self, database: Option<&str>) -> Result<ClientQueryTarget> {
        let database = match database {
            Some(database) => validate_database_name(database.to_string())?,
            None => self.default_database.clone().ok_or_else(|| /* no default */ )?,
        };
        self.targets.get(&database).cloned().ok_or_else(|| /* unknown database */ )
    }
}
```]

A single-graph deployment wires exactly one name to one target with `single`:

#srcblock("src/client/service.rs:88-93")[```rust
pub fn single(database: impl Into<String>, target: ClientQueryTarget) -> Result<Self> {
    let database = validate_database_name(database.into())?;
    Self::new()
        .with_database(database.clone(), target)?
        .with_default_database(database)
}
```]

So the full journey of a request name is a chain of translations:

#figure(
  diagram(
    node-stroke: 0.6pt,
    node-fill: rgb("#eef4ff"),
    spacing: (0pt, 0.7cm),
    node((0, 0), [Bolt `db` field / HTTPS `x-graph-namespace` (a name like `"default"`)], width: 12cm),
    edge((0, 0), (0, 1), "->", [`ClientDatabaseResolver::resolve_database`]),
    node((0, 1), [`ClientQueryTarget { scope: GraphScope, cell_id }`], width: 12cm),
    edge((0, 1), (0, 2), "->", [`cell_id`]),
    node((0, 2), [`cell_prefix(cell_id)` = `cell/<cell_id>/`], width: 12cm),
    edge((0, 2), (0, 3), "->", [key builders in `keys.rs`]),
    node((0, 3), [concrete storage keys read from / written to SlateDB], fill: rgb("#e9fce9"), width: 12cm),
  ),
  caption: none,
)
#figcap[Name resolution. The client picks a database name; the resolver maps it to a scope and a cell id; the cell id becomes the storage-key prefix. The client never sees cell ids or epochs.]

#why[
  Keeping the name-to-cell mapping behind a trait means the same engine serves a single local
  graph in a test and a multi-tenant fleet in production without changing the query path. In
  the deployable binary the resolver is built from environment variables that name one cell;
  a larger deployment registers many names, one per graph.
]

== One process, or a cluster

Everything so far describes a single shard. TurboLay also runs as a cluster, and the cluster
is *symmetric*: there is no controller node, no leases, and no failover loop. Every data node
runs the same `graph-node` binary and holds a `RoutedGraphCluster`. What changed with the
graph-kernel resync is the ownership model: a node no longer owns a *disjoint slice* of the
cells. Instead every node opens *every* cell in a shared directory as a reader, and any node
allowed to write *lazily* opens a cached writer for a cell the first time it writes it.

`GraphCluster` is the simplest container: one scope, several shards held in a map keyed by
cell id, with no distribution at all. It is what the indexer opens (Section 1.8):

#srcblock("src/engine.rs:68-71")[```rust
pub struct GraphCluster {
    scope: GraphScope,
    shards: BTreeMap<String, GraphShard>,
}
```]

`RoutedGraphCluster` adds the fleet view. It carries the node's own id, the
`ObjectStoreNodeDirectory` shared by the whole fleet, the shards this node holds open, and a
single `promotable` flag saying whether this node may become a writer. There is no placement
map, no per-cell owner, no leases, and no control-plane handles:

#srcblock("src/engine.rs:78-89")[```rust
pub struct ObjectStoreNodeDirectory {
    cells: BTreeSet<String>,
    nodes: BTreeSet<String>,
}

pub struct RoutedGraphCluster {
    scope: GraphScope,
    local_node_id: String,
    directory: ObjectStoreNodeDirectory,
    shards: BTreeMap<String, Arc<GraphShard>>,
    promotable: bool,
}
```]

#term("ObjectStoreNodeDirectory")[
  A serializable directory of the fleet: the set of `cells` that exist and the set of `nodes`
  that participate. It replaced `ShardPlacement`. Crucially it does *not* map a cell to an
  owner — it just lists what exists. At open time a node validates that it is in `nodes`, then
  opens a shard for *every* cell in `cells` (`src/engine/cluster.rs`, `open_at_path`). Every
  node therefore materializes every cell as a reader; there is no ownership to negotiate and no
  failover, because there is nothing to fail over.
]

Because every node holds every cell, routing a *read* is trivial: any node can serve it
locally. A *write* is the only asymmetry. Writes are gated by the `promotable` flag and, on a
promotable node, lazily promote a cached SlateDB writer for the target cell before mutating:

#srcblock("src/engine/cluster.rs:375-396 (abridged)")[```rust
pub(crate) async fn ensure_local_writer(&self, cell_id: &str) -> Result<()> {
    if !self.promotable {
        return Err(GraphError::WriteRequiresWriter { operation: "routed_write", cell_id: ... });
    }
    let shard = self.shards.get(cell_id).ok_or(GraphError::UnknownShard { ... })?;
    shard.promote_to_writer(cell_id, "routed_write").await
}

pub async fn write_edge(&self, mutation: EdgeMutation) -> Result<CommitResult> {
    let shard = self.shard(&mutation.cell_id)?;
    self.ensure_local_writer(&mutation.cell_id).await?;   // lazily opens the writer
    shard.write_edge(mutation).await
}
```]

Single-writer safety does not come from a placement table; it rests on the sole SlateDB writer
handle plus the object-store cell write lock (Chapter 0). A second node that tries to promote
the same cell is fenced at the storage layer, so `promotable` can be true on more than one
node without corrupting a cell — at most one promotion wins. The old `graph-controller` binary
and its controller loop no longer exist.

#figure(
  diagram(
    node-stroke: 0.6pt,
    spacing: (1.5cm, 1.0cm),
    node((0, 0), [`graph-node` A\ reads all cells\ (writer for cell 1)], fill: rgb("#eef4ff"), width: 3.6cm),
    node((1, 0), [`graph-node` B\ reads all cells\ (writer for cell 3)], fill: rgb("#eef4ff"), width: 3.6cm),
    node((2, 0), [`graph-indexer`\ builds index\ generations], fill: rgb("#f3e9fc"), width: 3.6cm),
    node((1, 1.6), [Object store (durable graph data + CSC index generations)], fill: rgb("#e9fce9"), width: 11cm),
    edge((0, 0), (1, 1.6), "<->"),
    edge((1, 0), (1, 1.6), "<->"),
    edge((2, 0), (1, 1.6), "<->"),
    edge((0, 0), (1, 0), "<->", [same `ObjectStoreNodeDirectory`], stroke: 0.5pt + muted),
  ),
  caption: none,
)
#figcap[Cluster topology after the resync. Data nodes are identical `graph-node` processes that all read every cell over the shared object store; a write lazily promotes that node to the cell's single writer. A separate, stateless `graph-indexer` builds the durable CSC index generations off to the side (Section 1.8). All parties share one `ObjectStoreNodeDirectory`; there is no controller and no static per-cell owner.]

== Traversal acceleration: index generations

The engine answers multi-hop traversals directly from the adjacency keys, but for large,
stable neighborhoods that is expensive to redo on every query. TurboLay's one acceleration
mechanism is the *index generation*: a durable, immutable, content-addressed image of one
cell's adjacency for one edge type — a GraphBLAS CSC matrix plus a manifest — written to the
object store and hydrated into memory on demand. There are no supernodes, posting chunks, or
reachability caches; this is the whole of it. What changed with the resync is *who* builds it
(a separate process, Section 1.8) and that it is now an immutable generation rather than a
mutable, per-node-refreshed artifact.

#term("Index generation")[
  A durable snapshot of one cell's adjacency for one edge type, built at a fixed storage
  sequence (`base_sequence`) and identified by the SHA-256 of its CSC payload (`generation`).
  It is a cache of structure, not a source of truth: canonical edges still live in the
  adjacency keys, and any edges written *after* `base_sequence` are supplied at query time by
  the WAL-tail overlay laid over the generation.
]

#srcblock("src/engine/index_store.rs:11-20")[```rust
pub struct GraphIndexGeneration {
    pub cell_id: String,
    pub edge_type: String,
    pub base_sequence: StorageSequence,  // the snapshot it was built at
    pub last_wal_id: u64,                // last WAL entry folded in
    pub edge_count: u64,
    pub checksum: u64,
    pub generation: String,              // sha256 hex of the CSC payload
}
```]

Four steps move a generation through its life cycle. All the durable logic lives in
`engine/index_store.rs`; the read-time overlay lives in `shard/topology_tail.rs`:

- *Discover* — `dirty_graph_index_edge_types(cell_id)` scans the "dirty" matrix edge-type
  markers written by the write path and returns each dirty `(edge_type, sequence)`.
- *Build & publish* — `build_graph_index(cell_id, edge_type)` takes an artifact-build permit,
  snapshots at the current `base_sequence`, folds the canonical adjacency into a GraphBLAS CSC
  matrix, checksums it, names the generation by its SHA-256, and publishes the manifest with a
  compare-and-set retry loop (`INDEX_PUBLISH_ATTEMPTS = 8`). Publication is atomic: a reader
  sees either the old generation or the new one, never a torn write.
- *Hydrate & overlay* — `matrix_cache.rs` is read-through: on a miss it hydrates the current
  generation's CSC into the shard's matrix caches (taking the `matrix_compilation_gate` for the
  compiled form). Because a read pins a sequence that may be *ahead* of the generation's
  `base_sequence`, `topology_tail_since` replays the WAL entries written since the generation
  and overlays them, so the answer reflects the pinned read even when the indexer lags.
- *Collect* — `gc_graph_index_generations(cell_id, edge_type, retain_previous)` lists the
  generations under the prefix and deletes those older than the current one beyond the retained
  count.

#figure(
  diagram(
    node-stroke: 0.5pt,
    spacing: (4mm, 9mm),
    node((0, 0), text(size: 7.5pt)[writes →\ dirty markers], fill: reader-colors.surface_soft, stroke: reader-colors.border, corner-radius: 3pt, width: 2.4cm),
    edge((0, 0), (1, 0), "->", stroke: reader-colors.muted),
    node((1, 0), text(size: 7.5pt)[`graph-indexer`\ (separate process,\ Section 1.8)], fill: reader-colors.info_soft, stroke: reader-colors.info, corner-radius: 3pt, width: 2.7cm),
    edge((1, 0), (2, 0), "->", stroke: reader-colors.muted),
    node((2, 0), text(size: 7.5pt)[`build_graph_index`\ → publish manifest], fill: reader-colors.surface_soft, stroke: reader-colors.border, corner-radius: 3pt, width: 2.7cm),
    edge((2, 0), (3, 0), "->", stroke: reader-colors.muted),
    node((3, 0), text(size: 7.5pt)[immutable generation:\ GraphBLAS CSC + manifest\ (object store)], fill: reader-colors.purple_soft, stroke: reader-colors.purple, corner-radius: 3pt, width: 3cm),
    edge((3, 0), (4, 0), "->", stroke: reader-colors.muted, label: text(size: 7pt, fill: reader-colors.muted)[read]),
    node((4, 0), text(size: 7.5pt)[hydrate + WAL-tail\ overlay (read)], fill: reader-colors.ok_soft, stroke: reader-colors.ok, corner-radius: 3pt, width: 2.7cm),
    edge((3, 0), (3, 1), "->", stroke: (dash: "dashed", paint: reader-colors.muted)),
    node((3, 1), text(size: 7.5pt)[`gc_graph_index_generations`\ prunes old base sequences], fill: reader-colors.warn_soft, stroke: reader-colors.warn, corner-radius: 3pt, width: 3.2cm),
  ),
  caption: none,
)<fig-ch01-artifact-lifecycle>
#figcap[The index-generation life cycle. Writes only mark edge-types dirty. A separate `graph-indexer` process (Section 1.8) builds and publishes an immutable, content-addressed CSC generation; a read hydrates the current generation and overlays the WAL tail written since it — so the object store stays the single source of truth, with `gc_graph_index_generations` pruning superseded base sequences off to the side.]

The matrix caches are keyed by `(cell_id, edge_type, base_sequence)` and, unlike the per-read
result caches, are *not* invalidated by a write. Their `base_sequence` deliberately lags the
current read sequence: a read hydrates the current generation and overlays any newer edges from
the WAL tail at query time. So a fresh write does not evict a generation — it simply adds a few
overlay edges — and stale generations are removed only by a newer generation superseding them or
by GC pruning them. By default the hydrated-adjacency cache is off (`max_matrix_adjacencies =
0`); only the compiled GraphBLAS matrix is cached.

#why[
  Decoupling the durable generation's sequence from the read sequence is what lets acceleration
  stay cheap under a steady write load. If the matrix cache were keyed by the exact read
  sequence, every write would invalidate it and force a rebuild. Instead the generation is
  rebuilt out-of-process on a timer, and reads pay only for the small WAL-tail overlay since the
  generation was built. Making the generation immutable and content-addressed also means a
  half-written build can never be observed — a reader only ever adopts a fully published
  manifest.
]

== The out-of-process indexer

Building an index generation is CPU- and memory-heavy, and it does not need the front doors or
service discovery. So it was pulled out of the data node entirely into a second binary,
`graph-indexer`, built with the lean `indexer-runtime` feature (Section 1.1). Nothing on the
read/write path builds generations anymore; the data node only *consumes* them.

`graph-indexer` is a small loop. It reads its configuration from the environment — the data
path, the cells to index, a poll interval (`GRAPH_INDEXER_INTERVAL_MS`, default 5000), how many
previous generations to retain (`GRAPH_INDEXER_RETAIN_PREVIOUS`, default 1), and an admin
address (`GRAPH_INDEXER_ADMIN_ADDR`, default `0.0.0.0:9091`) — opens a *read-side*
`GraphCluster` over those cells, and exposes an admin HTTP server with `/livez`, `/readyz`, and
a Prometheus `/metrics` endpoint. Each cycle, for every cell:

+ `refresh_storage_sequence` to catch up the reader to the latest durable write;
+ `dirty_graph_index_edge_types` to find which edge types have drifted;
+ for each dirty edge type, compare the current generation's `base_sequence` against the dirty
  sequence and, if the generation is stale, `build_graph_index` to publish a fresh one;
+ `gc_graph_index_generations` to prune superseded generations beyond the retained count.

#why[
  Separating the builder from the data node is compute–compute separation: index building can
  be scaled, scheduled, and rate-limited independently of query serving, and a runaway build
  cannot starve reads on a data node. Because a generation is immutable and published
  atomically, the indexer can run as several replicas or be restarted mid-build without a data
  node ever seeing a partial index — the worst case is simply that the current generation lags,
  which the WAL-tail overlay already covers.
]

In Kubernetes this is a literal deployment split: the data node runs as a `StatefulSet`
(`charts/turbolay/templates/node-statefulset.yaml`) while the indexer runs as a stateless
`Deployment` (`charts/turbolay/templates/indexer-deployment.yaml`) with its own
`indexer.replicaCount`, poll interval, and retention, all defaulted in `values.yaml`.

== Concurrency: gates and write lanes

Two mechanisms keep a busy node from tearing itself apart, and both were visible on the
`GraphShard` struct.

The gates (Section 1.3) bound how many heavy operations of each kind run at once. The write
lanes solve a different problem: throughput of small writes. Rather than one lock over all
writes, TurboLay spreads writes across a fixed number of lanes:

#srcblock("src/lib.rs:178")[```rust
pub(crate) const GRAPH_WRITE_LANES: usize = 64;
```]

`writer_lanes: Vec<Mutex<()>>` on the shard holds those 64 lightweight locks. A write picks a
lane (by hashing what it touches), so two writes to unrelated vertices take different lanes
and proceed in parallel, while two writes that could conflict take the same lane and
serialize. The write chapter shows the lane selection in detail.

== How a request travels, end to end

Pulling the chapter together, here is the path of one client query, with pointers to where
each step is covered in depth:

+ A driver connects over *Bolt* or *HTTPS* and authenticates. The server layer is
  `client/bolt.rs` or `client/http.rs` (read chapter).
+ The driver names a *database*. `ClientDatabaseResolver` turns it into a
  `ClientQueryTarget` carrying the `GraphScope` and `cell_id` (Section 1.5).
+ In a cluster, *any* node can serve the read, because every node opens every cell in the
  shared `ObjectStoreNodeDirectory` (Section 1.6). There is no owner to route to.
+ The `ClientQueryService` opens a *SlateDB snapshot* to pin the read, parses the *Cypher*
  into the engine's plan (`query/opencypher.rs`), and runs it against the local `GraphShard`
  (`shard/query.rs`). This is the whole read chapter.
+ For a write, the node must be `promotable`: it *lazily opens (and caches) a SlateDB writer*
  for the cell, takes the *cell write lock*, then commits through a *write batch* that advances
  the storage sequence and updates the edge, adjacency, index, and degree keys. This is the
  write chapter.
+ A delete *hard-removes* the relationship rows and *soft-deletes* the structural edge at a
  new sequence; later a background pass on the *gc gate* physically removes superseded data.
  This is the delete chapter.
+ Throughout, the *caches* on the shard absorb repeated work: per-read caches keyed by the
  read sequence (a write advances the sequence, so the next read simply misses), and matrix
  caches keyed by a deliberately lagging generation `base_sequence` with the WAL tail overlaid.
  This is the caching chapter.

With the shape of the system in place, the next chapter follows a read from the Bolt socket
all the way down to the adjacency keys and back.
