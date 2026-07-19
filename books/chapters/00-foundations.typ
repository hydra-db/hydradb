#import "../template.typ": term, why, srcblock, figcap, accent, muted
#import "@preview/fletcher:0.5.7" as fletcher: diagram, node, edge

= Foundations

This chapter builds the vocabulary the rest of the book uses. If you already know what
a log-structured merge tree is, what an object store is, and what a property graph is,
you can skim. Everything here is defined from first principles, then tied to the place in
the code where TurboLay relies on it.

TurboLay is a graph database engine. A graph database stores data as points connected by
links, and answers questions about how those points connect. What makes TurboLay unusual
is where it keeps that data. Instead of writing to a local disk, it keeps the durable copy
of the graph in an object store such as Amazon S3, and it reaches that object store through
a storage library called SlateDB. To understand any later chapter you first need to
understand that storage stack, so we start there and work up to the graph.

== A key-value store

#term("Key-value store (KV store)")[
  A database whose entire interface is: put a value under a key, get the value back by its
  key, delete a key, and scan a range of keys in sorted order. Keys and values are just
  bytes. There are no tables, no columns, and no query language. It is the simplest useful
  storage abstraction.
]

Every higher-level data model can be encoded on top of a key-value store if you design the
keys carefully. TurboLay does exactly this: the whole graph, every vertex, every edge, and
every index, is a set of key-value pairs. The engine never sees a disk block or an S3
object directly. It calls four operations: `get`, `put` (through a write batch), `scan` over
a key prefix, and `delete`. You can see these calls threaded through `GraphStore`, the thin
wrapper TurboLay puts over its storage handle:

#srcblock("src/core/state.rs:81-131")[```rust
pub(crate) enum GraphStore {
    Writer(Db),
    Reader(Arc<DbReader>),
}

impl GraphStore {
    pub(crate) async fn get_with_options(
        &self, key: &[u8], options: &ReadOptions,
    ) -> std::result::Result<Option<Bytes>, slatedb::Error> { /* ... */ }

    pub(crate) async fn scan_prefix_with_options(
        &self, prefix: &[u8], start_suffix: Option<Vec<u8>>, options: &ScanOptions,
    ) -> std::result::Result<slatedb::DbIterator, slatedb::Error> { /* ... */ }
}
```]

The two variants, `Writer(Db)` and `Reader(Arc<DbReader>)`, are the two ways TurboLay can
be attached to storage. We come back to that split in Section 0.10. For now notice that the
only verbs are get and scan. That is the whole storage contract.

== The log-structured merge tree

A key-value store still has to live on real hardware, and the layout it chooses on that
hardware decides whether it is fast to write, fast to read, or neither. TurboLay's storage
library uses a log-structured merge tree.

#term("Log-structured merge tree (LSM tree)")[
  A storage design tuned for fast writes. New writes are appended to an in-memory table
  (the memtable) and to a write-ahead log for durability. When the memtable fills, it is
  frozen and flushed to storage as an immutable sorted file. Files accumulate in levels;
  a background process called compaction merges smaller files into larger ones and throws
  away overwritten or deleted keys. Reads check the memtable first, then the files from
  newest to oldest.
]

#term("Write-ahead log (WAL)")[
  An append-only record of every write, written before the write is acknowledged. If the
  process crashes, the log is replayed on restart so no acknowledged write is lost.
]

The important consequence for TurboLay is that an LSM tree never updates a value in place.
A write is an append. A delete is also an append: it writes a small marker called a
tombstone that says "this key is gone", and the real removal happens later during
compaction.

#term("Tombstone")[
  A marker written to record that a key was deleted. Until compaction runs, the old value
  and its tombstone both physically exist; a read that finds the tombstone reports the key
  as absent. Tombstones matter a great deal in the delete chapter, because TurboLay layers
  its own graph-level tombstones on top of the storage engine's.
]

#why[
  An append-only design is what makes it safe to keep the durable copy on an object store.
  Object stores do not let you edit part of an object, but they are happy to accept new
  objects. An LSM tree only ever produces new immutable files, which is exactly the shape
  an object store rewards.
]

== The object store

#term("Object store")[
  A storage service that holds named blobs of bytes, called objects, grouped into buckets.
  You can put a whole object, get a whole object, and list objects by prefix. You cannot
  edit an object in place or append to it; to change one byte you upload a new object.
  Amazon S3 is the best-known example. Objects are cheap, effectively unlimited, and
  durable, but each request has high latency compared to a local disk.
]

TurboLay treats the object store as the single source of truth for the graph. Local disk
and memory are only caches in front of it. The engine is generic over the object store
through the `ObjectStore` trait, so the same code runs against S3, against MinIO (an
S3-compatible server you can run locally), or against the local filesystem. The helpers that
build one are exported at the crate root:

#srcblock("src/lib.rs:83-84")[```rust
pub use engine::{
    local_object_store, object_store_from_env, ArtifactGcResult,
    // ...
};
```]

`local_object_store` points at a directory on disk for development, and
`object_store_from_env` reads credentials and a bucket name from environment variables for
a real deployment. Both hand back an `Arc<dyn ObjectStore>`, which is stored directly on the
shard (Section 0.10).

== SlateDB

#term("SlateDB")[
  An embedded key-value storage library that implements an LSM tree whose files live in an
  object store. "Embedded" means it is a Rust library you call in-process, not a server you
  connect to over the network. SlateDB gives TurboLay durability, sorted scans, atomic write
  batches, and transactions, all backed by the object store. TurboLay pins it as an upstream
  dependency and builds the entire graph on top of it.
]

SlateDB provides two handles, and they line up with the two `GraphStore` variants from
Section 0.1:

- `Db`: a read-write handle. It owns the memtable and can commit write batches and
  transactions.
- `DbReader`: a read-only handle. It can open the same object-store data for reading without
  taking write ownership, which lets many readers share one writer's data.

TurboLay opens them in `open_graph_db` and `open_graph_reader`:

#srcblock("src/core/config.rs:316-344")[```rust
pub(crate) async fn open_graph_db(
    path: impl Into<Path>,
    object_store: Arc<dyn ObjectStore>,
    cache: &GraphCacheConfig,
    storage_memory: &GraphStorageMemoryConfig,
    durability: &GraphDurabilityConfig,
) -> Result<Db> {
    let mut settings = Settings::default();
    cache.apply_to_settings(&mut settings);
    storage_memory.apply_to_settings(&mut settings)?;
    durability.apply_to_settings(&mut settings);
    Ok(Db::builder(path, object_store).with_settings(settings).build().await?)
}

pub(crate) async fn open_graph_reader(
    path: impl Into<Path>,
    object_store: Arc<dyn ObjectStore>,
    cache: &GraphCacheConfig,
) -> Result<DbReader> { /* builds a DbReader */ }
```]

Everything you configure at open time (how much memory the memtable may use, how large the
first level of files is, how aggressively to fsync) is applied to SlateDB `Settings` here.
The caching knobs are the subject of the last chapter of this book.

Putting the stack together, a request travels down through these layers and the durable
bytes live at the bottom:

#figure(
  diagram(
    node-stroke: 0.6pt,
    node-fill: rgb("#eef4ff"),
    spacing: (0pt, 0.9cm),
    node((0, 0), [Client (Bolt / HTTPS)], width: 6.4cm),
    edge((0, 0), (0, 1), "->"),
    node((0, 1), [TurboLay engine (`GraphShard`)\ graph model, keys, query], width: 6.4cm),
    edge((0, 1), (0, 2), "->", [`get` / `put` / `scan`]),
    node((0, 2), [SlateDB (LSM tree, write batches)], width: 6.4cm),
    edge((0, 2), (0, 3), "->", [immutable objects]),
    node((0, 3), [Object store (S3 / MinIO / filesystem)], fill: rgb("#e9fce9"), width: 6.4cm),
  ),
  caption: none,
)
#figcap[The storage stack. Each layer only talks to the one below it. The durable copy of the graph is at the bottom, in the object store.]

== The property graph data model

Now the data. TurboLay stores a property graph.

#term("Property graph")[
  A data model with three ingredients. Vertices (also called nodes) are the things.
  Edges are directed links from one vertex to another, each with a type such as `RELATES`.
  Properties are named values attached to a vertex or an edge, for example a name or a
  timestamp. A label is a category attached to a vertex, for example `Entity` or `Source`.
]

TurboLay identifies a vertex by a plain 64-bit integer. It also carries two distinct 64-bit
sequence numbers, and keeping them apart is the single most important idea in this book. Both
are colloquially called "epochs", but they are different things:

#srcblock("src/lib.rs:143-150")[```rust
pub type VertexId = u64;
/// SlateDB's sequence number for a committed storage snapshot.
pub type StorageSequence = u64;

/// Monotonic cursor for topology changes consumed by asynchronous matrix builds.
/// This is not a second storage MVCC system; canonical record visibility belongs
/// to SlateDB snapshots.
pub type TopologySequence = u64;
```]

A `StorageSequence` is SlateDB's own snapshot sequence: it is the mechanism that pins a
consistent view of the data for a read, and it is the real multi-version machinery
(Section 0.7). A `TopologySequence` is something else entirely: it is a monotonic cursor over
topology changes (edges added and removed) that drives the asynchronous rebuilding of matrix
artifacts used to accelerate traversal. As its own doc comment insists, it is *not* a second
storage MVCC system; canonical record visibility always belongs to SlateDB snapshots. When
graph records below stamp an `epoch`, that field is a `TopologySequence`, a topology cursor,
not a read snapshot.

An edge in its most basic form is just the pair of vertex ids, the edge type, and the
topology sequence at which it was written:

#srcblock("src/core/model.rs:63-69")[```rust
pub struct EdgeRecord {
    pub cell_id: String,
    pub edge_type: String,
    pub src: VertexId,
    pub dst: VertexId,
    pub epoch: TopologySequence,
}
```]

Property values are a small closed set of types. Note that TurboLay stores integers,
signed integers, booleans, floats, and strings, and nothing else:

#srcblock("src/core/model.rs:147-153")[```rust
pub enum VertexPropertyValue {
    Integer(u64),
    SignedInteger(i64),
    Bool(bool),
    Float(QueryFloat),
    String(String),
}
```]

#term("Relationship")[
  TurboLay draws a distinction between an edge and a relationship. An edge is the bare
  structural link used for traversal. A relationship is a richer object: it carries its own
  identity (a `RelationshipId`, another `u64`) and a bag of properties (`EdgeMetadata`), and
  more than one relationship can sit between the same pair of vertices. When the graph needs
  to say "these two entities are related, here are the details, and here is one specific
  such fact", that is a relationship.
]

#srcblock("src/core/model.rs:5, 95-103")[```rust
pub type RelationshipId = u64;

pub struct RelationshipRecord {
    pub cell_id: String,
    pub edge_type: String,
    pub src: VertexId,
    pub dst: VertexId,
    pub relationship_id: RelationshipId,
    pub epoch: TopologySequence,
    pub metadata: EdgeMetadata,
}
```]

#why[
  Splitting the cheap structural edge from the heavier relationship keeps traversal fast.
  A traversal that only needs to know "who is connected to whom" reads the compact edge and
  adjacency keys and never pays to load properties. Code that needs the details reads the
  relationship records separately. The write chapter shows both being written together.
]

== How the graph becomes keys: cells

Every field above (`cell_id`, `edge_type`, `src`, `dst`) shows up again as part of a
storage key. This is where the graph meets the key-value store.

#term("Cell")[
  TurboLay's unit of partitioning. A cell is a slice of the key space that holds one logical
  graph's data. Every key that belongs to a cell begins with the prefix `cell/<cell_id>/`.
  A cell is how one TurboLay deployment can hold many independent graphs (for different
  tenants, for example) in the same object store without their keys colliding.
]

The prefix is produced by one small function, and every other key builder is built on top
of it:

#srcblock("src/keys.rs:3-5")[```rust
pub fn cell_prefix(cell_id: &str) -> String {
    format!("cell/{cell_id}/")
}
```]

An edge is stored under a canonical key, and also under two adjacency-index keys so the
engine can scan a vertex's outgoing and incoming neighbors in sorted order. The vertex ids
are formatted as 20-digit zero-padded numbers (`{src:020}`) so that a lexicographic scan of
the keys visits them in numeric order:

#srcblock("src/keys.rs:51-61")[```rust
pub fn edge(cell_id: &str, edge_type: &str, src: VertexId, dst: VertexId) -> String {
    format!("cell/{cell_id}/edge/{edge_type}/{src:020}/{dst:020}")
}

pub fn out_edge(cell_id: &str, edge_type: &str, src: VertexId, dst: VertexId) -> String {
    format!("cell/{cell_id}/e/out/{edge_type}/{src:020}/{dst:020}")
}

pub fn in_edge(cell_id: &str, edge_type: &str, dst: VertexId, src: VertexId) -> String {
    format!("cell/{cell_id}/e/in/{edge_type}/{dst:020}/{src:020}")
}
```]

Reading a key left to right tells you exactly what it is and where it lives:

#figure(
  diagram(
    node-stroke: 0.5pt,
    node-fill: rgb("#f6f8fa"),
    spacing: (0.1cm, 1.1cm),
    node((0, 0), raw("cell/"), width: 1.7cm),
    node((1, 0), raw("acme/"), width: 1.7cm),
    node((2, 0), raw("e/out/"), width: 1.9cm),
    node((3, 0), raw("RELATES/"), width: 2.3cm),
    node((4, 0), raw("...0042/"), width: 2.1cm),
    node((5, 0), raw("...0099"), width: 2.1cm),
    node((0, 1), text(size: 7.5pt)[cell\ namespace], width: 1.7cm, stroke: none, fill: none),
    node((1, 1), text(size: 7.5pt)[cell id], width: 1.7cm, stroke: none, fill: none),
    node((2, 1), text(size: 7.5pt)[outgoing\ adjacency], width: 1.9cm, stroke: none, fill: none),
    node((3, 1), text(size: 7.5pt)[edge type], width: 2.3cm, stroke: none, fill: none),
    node((4, 1), text(size: 7.5pt)[source\ vertex], width: 2.1cm, stroke: none, fill: none),
    node((5, 1), text(size: 7.5pt)[dest\ vertex], width: 2.1cm, stroke: none, fill: none),
    edge((0, 0), (0, 1), "-", stroke: 0.3pt + muted),
    edge((1, 0), (1, 1), "-", stroke: 0.3pt + muted),
    edge((2, 0), (2, 1), "-", stroke: 0.3pt + muted),
    edge((3, 0), (3, 1), "-", stroke: 0.3pt + muted),
    edge((4, 0), (4, 1), "-", stroke: 0.3pt + muted),
    edge((5, 0), (5, 1), "-", stroke: 0.3pt + muted),
  ),
  caption: none,
)
#figcap[Anatomy of an outgoing-adjacency key for edge 42 to 99 of type RELATES in cell "acme". A prefix scan of `cell/acme/e/out/RELATES/00...0042/` returns every out-neighbor of vertex 42 in id order.]

== Snapshots, topology cursors, and multi-version concurrency

Because the storage engine only appends, TurboLay can keep old versions of the graph around
and let readers pick a consistent version. But it is important to be precise about *which*
sequence number does that job. The snapshot that gives a reader a stable view is SlateDB's,
tracked by a `StorageSequence`. The `TopologySequence` stamped on graph records is a separate
concern and does not, by itself, pin a read.

#term("Multi-version concurrency control (MVCC)")[
  A technique where the store keeps multiple versions of data so that readers and writers do
  not block each other. A reader works from a fixed snapshot version while a writer creates a
  newer version alongside it. In TurboLay this is SlateDB's job: a read opens a SlateDB
  snapshot (a `DbSnapshot` pinned at a `StorageSequence`) and every key it touches is served
  from that snapshot, so the read sees a single consistent version of the graph regardless of
  concurrent writes. Record visibility belongs to the SlateDB snapshot, full stop.
]

#term("Topology sequence")[
  The `TopologySequence` stamped on an `EdgeRecord` or `RelationshipRecord` (colloquially "the
  epoch") is a monotonic cursor over topology changes for a cell. It records the order in
  which structural edges came and went, and it is what feeds the asynchronous rebuilding of
  matrix artifacts that accelerate traversal. It is explicitly *not* a second read-snapshot
  mechanism: a reader does not pick a topology sequence to time-travel the graph; it opens a
  SlateDB snapshot. The current cursor of a cell is stored under
  `cell/<cell_id>/meta/last_epoch`.
]

You already saw this `epoch` field on `EdgeRecord` and `RelationshipRecord`. The keys that
track a cell's current topology cursor are defined next to the edge keys. The word "epoch"
survives in these key names for historical reasons; read it as "topology cursor":

#srcblock("src/keys.rs:19-29")[```rust
pub fn last_epoch(cell_id: &str) -> String {
    format!("cell/{cell_id}/meta/last_epoch")
}

pub fn mutation_log_epoch(cell_id: &str) -> String {
    format!("cell/{cell_id}/meta/mutation_log_epoch")
}
```]

The two sequences are the backbone of the read, write, and delete chapters. Reads open a
SlateDB snapshot and bind it to the cell's current topology cursor; writes advance the cursor
as they mutate topology; and deletes rely on the cursor to decide which topology deltas are
old enough to physically remove.

== The identity hierarchy: namespaces, graphs, and scopes

A cell id is an internal storage key. The outward-facing identity of a graph is richer,
because TurboLay is multi-tenant.

#term("Namespace")[
  A named tenant boundary. Namespaces can nest to form a path, for example a company and
  then a team inside it. A `NamespaceId` is a single validated name; a `NamespacePath` is the
  ordered list of names from the root tenant down to the leaf. The nesting depth is capped.
]

#srcblock("src/core/namespace.rs:5-7")[```rust
pub const DEFAULT_NAMESPACE_ID: &str = "default";
pub const DEFAULT_GRAPH_ID: &str = "default";
pub const MAX_NAMESPACE_DEPTH: usize = 8;
```]

#term("Graph id and graph scope")[
  Within a namespace there can be several named graphs, each identified by a `GraphId`. A
  `GraphScope` is the full coordinate of one graph: which namespace path it is in, and which
  graph id inside that path. It prints as `<namespace>/graphs/<graph_id>`.
]

#srcblock("src/core/namespace.rs:226-248")[```rust
pub struct GraphScope {
    pub namespace: NamespacePath,
    pub graph_id: GraphId,
}

impl GraphScope {
    pub fn tenant(namespace_id: NamespaceId, graph_id: GraphId) -> Self {
        Self::new(NamespacePath::root(namespace_id), graph_id)
    }
}
```]

The relationship between these ideas is a chain. A client picks a logical database name.
The server maps that name to a `GraphScope` plus a `cell_id`. The `cell_id` is the prefix
that all the storage keys carry. The architecture chapter follows this resolution end to
end; for now hold the mental model that the outside world speaks in namespaces and graph
ids, and the storage layer speaks in cell ids.

== Writer, reader, and the cell write lock

Section 0.4 said the object store is the single source of truth. That raises a hazard: if
two processes both believe they are the writer for the same cell, they can corrupt it. Only
one writer per cell may exist at a time. TurboLay enforces this with a distributed cell write
lock held in the object store itself.

#term("Cell write lock")[
  A single record in the object store that names the current legitimate writer of a cell.
  Rather than a generation counter, the lock is an *owner token* plus a *TTL*: a writer
  acquires the lock by creating the record with a freshly minted `owner_token` and an
  `expires_at_ms` set one TTL into the future; it must renew before that expiry or the lock is
  considered abandoned and another process may take it. Every write checks that the record's
  `owner_token` still matches and the lock is `Active`. This is how TurboLay guarantees a
  single writer even though the storage is shared and remote. There is no durable "write
  fence" and no generation number.
]

The lock lives at a well-known object path per cell, and its record is a small
line-oriented value tagged `graph-cell-write-lock-v1`:

#srcblock("src/shard/lifecycle.rs:239-246")[```rust
pub(crate) fn cell_write_lock_path(&self, cell_id: &str) -> Path {
    let db_path = if self.store_path.as_ref().is_empty() {
        "__root__"
    } else {
        self.store_path.as_ref()
    };
    Path::from_iter(["__slatedb_graph_kernel", "write_locks", db_path, cell_id])
}
```]

#srcblock("src/core/state.rs:486-494")[```rust
pub(crate) struct CellWriteLockRecord {
    pub(crate) cell_id: String,
    pub(crate) operation: String,
    pub(crate) owner_token: String,
    pub(crate) created_ms: u64,
    pub(crate) expires_at_ms: u64,
    pub(crate) state: CellWriteLockState,   // Active | Released
}
```]

Acquisition and release are two functions in `src/core/state.rs`:
`acquire_distributed_write_lock` mints an owner token and creates the record (retrying if
another owner already holds an unexpired lock), and `release_cell_write_lock` flips the
record's state to `Released`. The constants that bound the lock are defined at the crate root:

#srcblock("src/lib.rs:188-191")[```rust
pub(crate) const GRAPH_WRITE_LANES: usize = 64;
pub(crate) const GRAPH_CELL_WRITE_LOCK_MAX_ATTEMPTS: usize = 256;
pub(crate) const GRAPH_CELL_WRITE_LOCK_BACKOFF_MS: u64 = 2;
pub(crate) const GRAPH_CELL_WRITE_LOCK_TTL_MS: u64 = 5 * 60 * 1000;
```]

`GRAPH_CELL_WRITE_LOCK_TTL_MS` is the five-minute lock lifetime after which an un-renewed
lock may be taken over. `GRAPH_WRITE_LANES` is a separate concern: within the one legitimate
writer, work is spread across 64 lanes so that writes to different vertices do not serialize
behind each other. The write chapter dissects both.

== Bolt and the client surface

Finally, how a program talks to TurboLay from outside the process.

#term("Bolt")[
  The binary network protocol used by the Neo4j family of graph databases and their client
  drivers. A Bolt client opens a connection, authenticates, sends a query with parameters,
  and pulls back rows. TurboLay speaks Bolt so that existing Neo4j drivers, in Python, Go,
  and other languages, can connect to it without a custom client. It also exposes an HTTPS
  query API for the same purpose.
]

The Bolt and HTTPS servers, and the shared service layer behind them, are the top of the
crate's public surface:

#srcblock("src/lib.rs:24-36")[```rust
#[cfg(feature = "bolt-server")]
pub use client::bolt::{
    BoltRoutingServer, BoltRoutingTable, BoltRoutingTableProvider, BoltServerConfig,
    BoltServerHandle, ClientBoltServer, RendezvousBoltRoutingTableProvider,
};
#[cfg(feature = "http-api")]
pub use client::http::{ClientHttpServer, HttpQueryServerConfig, HttpQueryServerHandle};
#[cfg(feature = "client-api")]
pub use client::service::{ /* ClientQueryService and friends */ };
```]

Those `#[cfg(feature = ...)]` lines are worth noticing now. TurboLay is one crate with many
optional pieces gated behind Cargo features. A build might include only the embedded engine,
or add the query language, or add the Bolt server on top. The architecture chapter maps the
feature tower so you know which code is present in which build.

== Vocabulary recap

You now have every term the later chapters lean on:

- *Key-value store*: put, get, scan by prefix, delete. The only storage contract.
- *LSM tree, WAL, tombstone*: append-only storage that never edits in place.
- *Object store*: durable, remote, whole-object storage; the source of truth.
- *SlateDB*: the embedded LSM library that puts the key-value store on the object store.
- *Property graph, vertex, edge, relationship, property, label*: the data model.
- *Cell*: the `cell/<cell_id>/` key prefix that partitions one graph from another.
- *Storage sequence, topology sequence, MVCC*: the SlateDB snapshot that gives readers a
  stable version, versus the topology cursor stamped on graph records that drives matrix builds.
- *Namespace, graph id, graph scope*: the multi-tenant identity that maps to a cell.
- *Cell write lock*: the owner-token + TTL record in the object store that guarantees a single
  writer over shared remote storage.
- *Bolt*: the wire protocol that lets standard graph drivers connect.

With these in hand, the next chapter lays out how the code is organized and how these pieces
are wired into the one type that ties them together, the `GraphShard`.
