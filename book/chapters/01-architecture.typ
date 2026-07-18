#import "../template.typ": term, why, srcblock, figcap, accent, muted
#import "@preview/fletcher:0.5.7" as fletcher: diagram, node, edge

= Architecture

Chapter 0 gave you the vocabulary. This chapter shows how the code is organized, which
build contains which parts, and how the pieces connect into one running system. The aim is
that when you open the tree you know which file to look in and why it exists.

We move from the outside in. First the build shapes (Cargo features), then the module map,
then the single type that ties storage and graph together (`GraphShard`), then how an
incoming query name is resolved down to storage keys, and finally how one process becomes a
cluster.

== One crate, many builds

turbolay is a single Rust crate. It is not a workspace of many crates. Instead it uses Cargo
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
query-transport = ["opencypher", "dep:serde", "dep:sha2", "dep:subtle", "json-properties", ...]
query-transport-tls = ["query-transport", "dep:tokio-rustls"]
query-service-discovery = ["query-transport", "dep:base64", "dep:reqwest"]
client-api = ["query-transport"]
bolt-server = ["client-api", "query-transport-tls", "dep:boltr"]
http-api = ["client-api", "query-transport-tls", "dep:axum", "dep:axum-server"]
public-client-protocols = ["bolt-server", "http-api"]
server-runtime = ["public-client-protocols", "graphblas", "query-service-discovery", ...]
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
- `server-runtime` is the full deployable build: both front doors, the GraphBLAS traversal
  backend, service discovery for finding other nodes, and logging.

#figure(
  diagram(
    node-stroke: 0.6pt,
    spacing: (0pt, 0.72cm),
    node((0, 0), [`server-runtime` (full node: both front doors, GraphBLAS, discovery)], fill: rgb("#e9fce9"), width: 11.5cm),
    node((0, 1), [`public-client-protocols` = `bolt-server` + `http-api`], fill: rgb("#eef4ff"), width: 11.5cm),
    node((0, 2), [`client-api` (shared `ClientQueryService`)], fill: rgb("#eef4ff"), width: 11.5cm),
    node((0, 3), [`query-transport` (query wire protocol, auth, quotas)], fill: rgb("#eef4ff"), width: 11.5cm),
    node((0, 4), [`opencypher` (Cypher parser and planner)], fill: rgb("#fff8e6"), width: 11.5cm),
    node((0, 5), [default (embedded engine: model, keys, codec, shard, storage)], fill: rgb("#f6f8fa"), width: 11.5cm),
  ),
  caption: none,
)
#figcap[The feature tower. A build is a horizontal slice: everything from some level down. The two example servers in `examples/` build with `bolt-server`; the deployable binaries build with `server-runtime`.]

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
  [`core/`], [The foundations: `state.rs` (the `GraphShard` type and write fencing), `model.rs` (vertex, edge, relationship, property types), `config.rs` (open options and the SlateDB open functions), `namespace.rs` (namespaces, graph ids, scopes), `snapshot.rs`, `cache.rs`, `metrics.rs`, `error.rs`, `write_batch.rs`.],
  [`keys.rs`], [Every storage key builder. One function per key shape. 663 lines of pure key formatting.],
  [`codec.rs`], [Every value encoder and decoder. Hand-written, versioned byte formats. 2369 lines.],
  [`shard/`], [The behavior of `GraphShard`, split by concern: `write.rs` (5667 lines), `query.rs` (9091 lines), `query_optimizer.rs`, `lifecycle.rs`, `maintenance.rs`.],
  [`engine.rs` + `engine/`], [Everything above a single shard: `cluster.rs` (routing many shards), `control_plane.rs` and `controller.rs` (leases, placement, failover), `artifact_build.rs` and `supernode.rs` (precomputed traversal structures), `traversal.rs`, `verify.rs`, and the control-plane RPC transport.],
  [`query/`], [The query language and distribution: `opencypher.rs` (parse Cypher into the engine's own plan), `algebra.rs` (the plan types), `coordination.rs` (the query network protocol and distributed execution, 4468 lines).],
  [`client/`], [The public front doors: `service.rs` (the shared `ClientQueryService`), `bolt.rs` (the Bolt server), `http.rs` (the HTTPS server).],
  [`sparse_kernel.rs`], [Sparse-matrix traversal, either in pure Rust or through the GraphBLAS C library.],
  [`placement.rs`], [Experiments in how vertex ids map to cells for locality.],
  [`bin/`], [The two executables: `graph-node` and `graph-controller`.],
)

The line counts tell you where the real work is. `shard/query.rs` and `shard/write.rs`
together are over fourteen thousand lines. Those two files are the subject of the read and
write chapters.

== GraphShard: the type that ties it together

#term("GraphShard")[
  The central type of the engine. One `GraphShard` is one open handle to one cell's data on
  the object store, plus everything needed to read and write it: the SlateDB storage handle,
  the configured limits and policies, the concurrency controls, and all the in-memory caches.
  Nearly every operation in the engine is a method on `GraphShard`.
]

Its fields fall into four groups. Storage and configuration first:

#srcblock("src/core/state.rs:33-52")[```rust
pub struct GraphShard {
    pub(crate) db: GraphStore,
    pub(crate) object_store: Arc<dyn ObjectStore>,
    pub(crate) store_path: Path,
    pub(crate) limits: GraphLimits,
    pub(crate) cache_policy: GraphCachePolicy,
    pub(crate) retention_policy: GraphRetentionPolicy,
    // ...
    pub(crate) index_policy: GraphIndexPolicy,
    pub(crate) await_durable_writes: bool,
    pub(crate) write_authority: GraphWriteAuthority,
    pub(crate) writer_lanes: Vec<Mutex<()>>,
```]

`db` is the `GraphStore` from Chapter 0, either a `Writer(Db)` or a `Reader(DbReader)`.
`object_store` and `store_path` say where the durable data is. `write_authority` and
`writer_lanes` are the write-fencing and lane machinery from Section 0.10 and 0.11.

Second, the concurrency gates. Each is a semaphore that bounds how many of one kind of
expensive operation can run at once:

#srcblock("src/core/state.rs:44-48")[```rust
    pub(crate) hydration_gate: Arc<Semaphore>,
    pub(crate) matrix_compilation_gate: Arc<Semaphore>,
    pub(crate) graph_write_gate: Arc<Semaphore>,
    pub(crate) artifact_build_gate: Arc<Semaphore>,
    pub(crate) gc_gate: Arc<Semaphore>,
```]

#term("Semaphore (as used here)")[
  A counter that hands out a fixed number of permits. A task must take a permit before doing
  the guarded work and returns it when done. If no permit is free the task waits. turbolay
  uses one semaphore per class of heavy work (loading data into memory, compiling a matrix,
  writing, building artifacts, garbage collecting) so that, for example, a burst of writes
  cannot starve reads of memory.
]

Third, the caches. There are many, one per kind of computed result, and each is a
`BoundedGraphCache` (the subject of the caching chapter):

#srcblock("src/core/state.rs:53-78 (abridged)")[```rust
    pub(crate) matrix_artifact_cache: Mutex<BoundedGraphCache<MatrixCacheKey, MatrixArtifact>>,
    pub(crate) matrix_cache: Mutex<BoundedGraphCache<MatrixCacheKey, Arc<MatrixAdjacency>>>,
    #[cfg(feature = "opencypher")]
    pub(crate) reachability_cache:
        Mutex<BoundedGraphCache<ReachabilityCacheKey, ReachabilityCacheValue>>,
    pub(crate) supernode_group_cache:
        Mutex<BoundedGraphCache<SupernodeCacheKey, SupernodeGroup>>,
    pub(crate) posting_chunk_cache:
        Mutex<BoundedGraphCache<PostingChunkCacheKey, PostingChunk>>,
    // ... several more, some gated behind the opencypher feature
```]

The `#[cfg(feature = "opencypher")]` on some caches is the feature tower showing through:
a build without the query language does not carry the query-result caches.

The `Writer` / `Reader` split on `db` is the mechanism behind the single-writer rule. A
read-only shard literally cannot call a write method, because the storage handle refuses:

#srcblock("src/core/state.rs:87-93")[```rust
impl GraphStore {
    pub(crate) fn writer(&self) -> Result<&Db> {
        match self {
            Self::Writer(db) => Ok(db),
            Self::Reader(_) => Err(GraphError::ReadOnlyShardStorage),
        }
    }
```]

== The behavior is split by concern

`GraphShard` is one struct, but its methods are spread across the `shard/` module so that
each file holds one concern. The module simply lists them:

#srcblock("src/shard.rs:3-8")[```rust
mod lifecycle;
mod maintenance;
mod query;
mod query_optimizer;
mod write;
```]

- `lifecycle.rs`: opening, closing, and format-version checks for a shard.
- `write.rs`: everything that mutates (the write chapter).
- `query.rs`: everything that reads and answers queries (the read chapter).
- `query_optimizer.rs`: choosing how to run a query using stored statistics.
- `maintenance.rs`: background upkeep, including the garbage collection in the delete chapter.

Each file has its own `impl GraphShard { ... }` block. Rust allows a type's methods to be
defined in several `impl` blocks across files, and turbolay uses that to keep each file
focused. When you look for a method, pick the file by concern.

== From a client name to storage keys

A client does not know about cells or epochs. It connects and names a database. The server
turns that name into a concrete target, and the target carries the `cell_id` that every
storage key needs. This resolution is worth following closely because it connects the
outward identity (Chapter 0, Section 0.9) to the storage keys (Section 0.7).

The target is a scope plus a cell id:

#srcblock("src/client/service.rs:34-46")[```rust
pub struct ClientQueryTarget {
    pub scope: GraphScope,
    pub cell_id: String,
}
```]

The mapping from a database name to a target goes through a trait, so a deployment can plug
in its own policy:

#srcblock("src/client/service.rs:48-50")[```rust
pub trait ClientDatabaseResolver: Send + Sync {
    fn resolve_database(&self, database: Option<&str>) -> Result<ClientQueryTarget>;
}
```]

The shipped implementation is a static table from name to target, with an optional default
name used when the client does not specify one:

#srcblock("src/client/service.rs:92-111")[```rust
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

#srcblock("src/client/service.rs:84-89")[```rust
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

Everything so far describes a single shard. turbolay also runs as a cluster of nodes, each
owning some shards, coordinated by a control plane. The types climb in scope.

`GraphCluster` is the simplest: one scope, several shards held in a map keyed by cell id:

#srcblock("src/engine.rs:156-159")[```rust
pub struct GraphCluster {
    scope: GraphScope,
    shards: BTreeMap<String, GraphShard>,
}
```]

`RoutedGraphCluster` adds distribution. It knows which node owns which shard (the
`placement`), which shards this node currently holds a lease on, and which cells have been
revoked from it:

#srcblock("src/engine.rs:281-293")[```rust
pub struct RoutedGraphCluster {
    scope: GraphScope,
    base_path: String,
    local_node_id: String,
    placement: ShardPlacement,
    object_store: Arc<dyn ObjectStore>,
    options: GraphOpenOptions,
    memory: GraphMemoryConfig,
    shards: BTreeMap<String, GraphShard>,
    leases: Arc<RwLock<BTreeMap<String, ShardLease>>>,
    shard_db_handles: Arc<RwLock<BTreeMap<String, GraphStore>>>,
    revoked_cells: Arc<RwLock<BTreeSet<String>>>,
}
```]

#term("Control plane")[
  The part of a distributed system that decides who does what, as opposed to the data plane
  that does the work. In turbolay the control plane hands out shard ownership leases, records
  which node owns which shard, advances safety watermarks, and fails a shard over to a new
  node when its owner stops renewing its lease. It is not on the read or write hot path; it
  runs alongside.
]

`GraphControlPlane` owns the control-plane state on the object store. `GraphNode` is a live
data-plane node: a `RoutedGraphCluster` plus a client to the control plane, a lease renewer,
and a heartbeat. `ManagedGraphNode` wraps a `GraphNode` with the background task that
refreshes shard assignments.

#srcblock("src/engine.rs:295-306")[```rust
pub struct GraphNode {
    cluster: RoutedGraphCluster,
    control: Arc<dyn GraphControlClient>,
    lease_renewer: LeaseRenewalHandle,
    heartbeat: NodeHeartbeatHandle,
}

pub struct ManagedGraphNode {
    node: Arc<TokioRwLock<Option<GraphNode>>>,
    shard_refresher: ShardRefreshHandle,
    metrics: Arc<GraphNodeMaintenanceMetrics>,
}
```]

The two binaries map onto these types. `graph-controller` runs the controller loop that
drives a `GraphControlPlane` (assign shards, detect dead nodes, fail over). `graph-node`
runs a `ManagedGraphNode` and serves Bolt and HTTPS on top of it.

#figure(
  diagram(
    node-stroke: 0.6pt,
    spacing: (1.4cm, 1.0cm),
    node((1, 0), [`graph-controller`\ (control plane)], fill: rgb("#fff8e6"), width: 4cm),
    node((0, 2), [`graph-node` A\ owns cells 1, 2], fill: rgb("#eef4ff"), width: 3.6cm),
    node((2, 2), [`graph-node` B\ owns cell 3], fill: rgb("#eef4ff"), width: 3.6cm),
    node((1, 3.2), [Object store (durable graph + control state)], fill: rgb("#e9fce9"), width: 8cm),
    edge((1, 0), (0, 2), "<->", [lease + heartbeat]),
    edge((1, 0), (2, 2), "<->", [lease + heartbeat]),
    edge((0, 2), (1, 3.2), "->"),
    edge((2, 2), (1, 3.2), "->"),
    edge((1, 0), (1, 3.2), "->", [placement,\ watermarks], stroke: 0.5pt + muted),
  ),
  caption: none,
)
#figcap[Cluster topology. The controller assigns cells to nodes with leases and watches heartbeats. Each node writes its cells' data to the shared object store. If node A stops renewing, the controller reassigns cells 1 and 2 to another node.]

== Concurrency: gates and write lanes

Two mechanisms keep a busy node from tearing itself apart, and both were visible on the
`GraphShard` struct.

The gates (Section 1.3) bound how many heavy operations of each kind run at once. The write
lanes solve a different problem: throughput of small writes. Rather than one lock over all
writes, turbolay spreads writes across a fixed number of lanes:

#srcblock("src/lib.rs:303")[```rust
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
+ In a cluster, the query is routed to the *node that owns that cell* using the placement
  from the control plane (Section 1.6). On a single node this step is a no-op.
+ The `ClientQueryService` pins an *epoch*, parses the *Cypher* into the engine's plan
  (`query/opencypher.rs`), and runs it against the owning `GraphShard` (`shard/query.rs`).
  This is the whole read chapter.
+ For a write, the shard first checks its *write fence*, then commits through a *write batch*
  that advances the epoch and updates the edge, adjacency, index, and degree keys. This is
  the write chapter.
+ A delete writes *tombstones* and later a background pass on the *gc gate* physically
  removes old data. This is the delete chapter.
+ Throughout, the *caches* on the shard absorb repeated work, keyed by epoch so they never
  serve stale data. This is the caching chapter.

With the shape of the system in place, the next chapter follows a read from the Bolt socket
all the way down to the adjacency keys and back.
