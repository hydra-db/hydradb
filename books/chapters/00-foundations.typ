#import "../template.typ": custom-box, srcblock, accent, muted
#import "../vendor/bookly/src/themes/reader.typ": reader-colors
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

#custom-box(title: [Term — Key-value store (KV store)], icon: "info")[
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

#srcblock("src/core/state.rs:248-270")[```rust
impl GraphStore {
    pub(crate) async fn get_with_options(
        &self, key: &[u8], options: &ReadOptions,
    ) -> Result<Option<Bytes>> { /* ... */ }

    pub(crate) async fn scan_prefix_with_options(
        &self, prefix: &[u8], start_suffix: Option<Vec<u8>>, options: &ScanOptions,
    ) -> Result<slatedb::DbIterator> { /* ... */ }
}
```]

Notice that the only verbs are get and scan. That is the whole storage contract. What sits
behind those two methods — a writer handle, a read-only handle, or a pinned snapshot — is a
question we defer to Section 0.10.

== The log-structured merge tree

A key-value store still has to live on real hardware, and the layout it chooses on that
hardware decides whether it is fast to write, fast to read, or neither. TurboLay's storage
library uses a log-structured merge tree.

#custom-box(title: [Term — Log-structured merge tree (LSM tree)], icon: "info")[
  A storage design tuned for fast writes. New writes are appended to an in-memory table
  (the memtable) and to a write-ahead log for durability. When the memtable fills, it is
  frozen and flushed to storage as an immutable sorted file. Files accumulate in levels;
  a background process called compaction merges smaller files into larger ones and throws
  away overwritten or deleted keys. Reads check the memtable first, then the files from
  newest to oldest.
]

#custom-box(title: [Term — Write-ahead log (WAL)], icon: "info")[
  An append-only record of every write, written before the write is acknowledged. If the
  process crashes, the log is replayed on restart so no acknowledged write is lost.
]

The important consequence for TurboLay is that an LSM tree never updates a value in place.
A write is an append. A delete is also an append: it writes a small marker called a
tombstone that says "this key is gone", and the real removal happens later during
compaction.

#custom-box(title: [Term — Tombstone], icon: "info")[
  A marker written to record that a key was deleted. Until compaction runs, the old value
  and its tombstone both physically exist; a read that finds the tombstone reports the key
  as absent. Tombstones matter a great deal in the delete chapter, because TurboLay layers
  its own graph-level tombstones on top of the storage engine's.
]

#custom-box(title: [Why], icon: "tip")[
  An append-only design is what makes it safe to keep the durable copy on an object store.
  Object stores do not let you edit part of an object, but they are happy to accept new
  objects. An LSM tree only ever produces new immutable files, which is exactly the shape
  an object store rewards.
]

== The object store

#custom-box(title: [Term — Object store], icon: "info")[
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

#srcblock("src/lib.rs:79-84")[```rust
pub use engine::{
    local_object_store, object_store_from_env, ArtifactGcResult,
    // ...
};
```]

`local_object_store` points at a directory on disk for development, and
`object_store_from_env` reads credentials and a bucket name from environment variables for
a real deployment. Both hand back an `Arc<dyn ObjectStore>`, which is held inside the
`GraphStore` handle rather than on the shard itself (Section 0.10).

== SlateDB

#custom-box(title: [Term — SlateDB], icon: "info")[
  An embedded key-value storage library that implements an LSM tree whose files live in an
  object store. "Embedded" means it is a Rust library you call in-process, not a server you
  connect to over the network. SlateDB gives TurboLay durability, sorted scans, atomic write
  batches, and transactions, all backed by the object store. TurboLay pins it as an upstream
  dependency and builds the entire graph on top of it.
]

SlateDB provides two handles, and TurboLay may hold either or both:

- `Db`: a read-write handle. It owns the memtable and can commit write batches and
  transactions.
- `DbReader`: a read-only handle. It can open the same object-store data for reading without
  taking write ownership, which lets many readers share one writer's data.

TurboLay opens them in `open_graph_db` and `open_graph_reader`:

#srcblock("src/core/config.rs:313-342")[```rust
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
    crossing-fill: reader-colors.paper,
    node-stroke: 0.6pt + reader-colors.info,
    node-fill: reader-colors.info_soft,
    edge-stroke: reader-colors.muted,
    spacing: (0pt, 0.9cm),
    node((0, 0), [Client (Bolt / HTTPS)], width: 6.4cm),
    edge((0, 0), (0, 1), "->"),
    node((0, 1), [TurboLay engine (`GraphShard`)\ graph model, keys, query], width: 6.4cm),
    edge((0, 1), (0, 2), "->", text(size: 8pt, fill: reader-colors.muted)[`get` / `put` / `scan`]),
    node((0, 2), [SlateDB (LSM tree, write batches)], width: 6.4cm),
    edge((0, 2), (0, 3), "->", text(size: 8pt, fill: reader-colors.muted)[immutable objects]),
    node((0, 3), [Object store (S3 / MinIO / filesystem)], fill: reader-colors.ok_soft, stroke: 0.6pt + reader-colors.ok, width: 6.4cm),
  ),
  caption: [The storage stack: each layer only talks to the one below it, and the
    durable copy of the graph is at the bottom, in the object store.],
) <fig-found-storage-stack>

== The property graph data model

Now the data. TurboLay stores a property graph.

#custom-box(title: [Term — Property graph], icon: "info")[
  A data model with three ingredients. Vertices (also called nodes) are the things.
  Edges are directed links from one vertex to another, each with a type such as `RELATES`.
  Properties are named values attached to a vertex or an edge, for example a name or a
  timestamp. A label is a category attached to a vertex, for example `Entity` or `Source`.
]

TurboLay identifies a vertex by a plain 64-bit integer, and it carries exactly one 64-bit
sequence type. There is no second clock:

#srcblock("src/lib.rs:138-140")[```rust
pub type VertexId = u64;
/// SlateDB's sequence number for a committed storage snapshot.
pub type StorageSequence = u64;
```]

A `StorageSequence` is SlateDB's own snapshot sequence. It is the mechanism that pins a
consistent view of the data for a read, and it is the *only* multi-version machinery in the
system (Section 0.7). The word "epoch" survives colloquially and in a handful of field names,
but wherever you meet it, it means a `StorageSequence` and nothing else.

#custom-box(title: [Why], icon: "tip")[
  An earlier edition of TurboLay carried a second sequence, a "topology cursor", alongside the
  storage sequence, so that traversal acceleration could advance on its own clock. Two clocks
  meant two notions of "when", and every read had to reconcile them. The graph-kernel resync
  removed the second clock outright: acceleration structures are now stamped with the same
  `StorageSequence` as everything else, so there is only ever one answer to "as of when?".
  If you find `TopologySequence` in older prose or in your own memory of this codebase, it does
  not exist — the type has zero occurrences in `src/`.
]

An edge in its most basic form is just the pair of vertex ids and the edge type. The record
carries no sequence field at all, because when the edge became visible is a property of the
snapshot you read it through, not of the row:

#srcblock("src/core/model.rs:61-66")[```rust
pub struct EdgeRecord {
    pub cell_id: String,
    pub edge_type: String,
    pub src: VertexId,
    pub dst: VertexId,
}
```]

Property values are a small closed set of types. Note that TurboLay stores integers,
signed integers, booleans, floats, and strings, and nothing else:

#srcblock("src/core/model.rs:143-149")[```rust
pub enum VertexPropertyValue {
    Integer(u64),
    SignedInteger(i64),
    Bool(bool),
    Float(QueryFloat),
    String(String),
}
```]

#custom-box(title: [Term — Relationship], icon: "info")[
  TurboLay draws a distinction between an edge and a relationship. An edge is the bare
  structural link used for traversal. A relationship is a richer object: it carries its own
  identity (a `RelationshipId`, another `u64`) and a bag of properties (`EdgeMetadata`), and
  more than one relationship can sit between the same pair of vertices. When the graph needs
  to say "these two entities are related, here are the details, and here is one specific
  such fact", that is a relationship.
]

#srcblock("src/core/model.rs:5, 92-99")[```rust
pub type RelationshipId = u64;

pub struct RelationshipRecord {
    pub cell_id: String,
    pub edge_type: String,
    pub src: VertexId,
    pub dst: VertexId,
    pub relationship_id: RelationshipId,
    pub metadata: EdgeMetadata,
}
```]

Like `EdgeRecord`, a stored relationship carries no sequence of its own. A sequence appears
only where a caller genuinely needs to *name a point in time* — for instance the result handed
back after a write, so the caller can say "read at least as recent as this":

#srcblock("src/core/model.rs:101-104")[```rust
pub struct RelationshipCreateResult {
    pub epoch: StorageSequence,
    pub relationship_id: RelationshipId,
    // ...
}
```]

That returned `epoch` is the seed of the bookmark mechanism in Section 0.8.

#custom-box(title: [Why], icon: "tip")[
  Splitting the cheap structural edge from the heavier relationship keeps traversal fast.
  A traversal that only needs to know "who is connected to whom" reads the compact edge and
  adjacency keys and never pays to load properties. Code that needs the details reads the
  relationship records separately. The write chapter shows both being written together.
]

== How the graph becomes keys: cells

Every field above (`cell_id`, `edge_type`, `src`, `dst`) shows up again as part of a
storage key. This is where the graph meets the key-value store.

#custom-box(title: [Term — Cell], icon: "info")[
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

The whole inventory of key shapes fits in one 316-line file with no logic in it, only
formatting. It is worth knowing what the families are before we look at any one of them,
because the list is short and it tells you what the engine actually stores:

#figure(
  table(
    columns: (auto, 1.25fr),
    inset: 6pt,
    align: (left + top, left + top),
    stroke: 0.4pt + reader-colors.border,
    fill: (_, row) => if row == 0 { reader-colors.surface_soft },
    table.header(
      text(fill: reader-colors.text)[*Key family*],
      text(fill: reader-colors.text)[*What it holds*],
    ),
    [`cell/<id>/edge/…`], [The canonical edge record, one per structural edge.],
    [`cell/<id>/e/out/…`\ `cell/<id>/e/in/…`], [The two adjacency indexes, so out-neighbors and in-neighbors of a vertex are a single sorted prefix scan.],
    [`cell/<id>/seg/out/…`\ `cell/<id>/seg/tomb/out/…`], [Compacted adjacency *segments* and their tombstones, for vertices with enough edges to be worth packing into one value.],
    [`cell/<id>/cnt/out/…`\ `cell/<id>/cnt/in/…`], [Degree counters, so the planner can ask how many neighbors a vertex has without scanning them.],
    [`cell/<id>/emeta/…`\ `cell/<id>/rel/…`\ `cell/<id>/rel_id/…`\ `cell/<id>/rprop_idx/…`], [Edge metadata, the richer relationship records, the relationship-id index, and the relationship-property indexes.],
    [`cell/<id>/vertex/…`\ `cell/<id>/vlabel/…`\ `cell/<id>/vprop_idx/…`], [Vertex records, the label index, and the vertex-property indexes.],
    [`cell/<id>/qstats/…`], [Cardinality statistics the query optimizer reads: per-label, per-property, and per-edge-type counts and histograms.],
    [`cell/<id>/meta/…`], [A small amount of per-cell bookkeeping: `last_relationship_id`, `matrix_dirty/<edge_type>`, `adjacency_generation/<edge_type>`.],
    [`cell/<id>/idem/…`], [Idempotency receipts, so a retried write is recognized rather than repeated.],
    [`graph/drop/…`], [Markers for dropping a whole cell, deliberately placed *outside* the cell prefix so they survive the deletion of everything inside it.],
  ),
  caption: [The complete key vocabulary of `src/keys.rs`. Everything the engine stores falls
    into one of these families; notice that none of them is a log, a queue, or a journal,
    because the delta and mutation-log subsystem that once occupied that role was removed in
    the graph-kernel resync.],
) <tab-found-key-families>

#custom-box(title: [Why], icon: "tip")[
  The absence is as informative as the presence. There is no `last_epoch` key, because nothing
  needs to record "where the graph is now" — a SlateDB snapshot already answers that. There is
  no outbox or mutation-log key family, because writes are not journalled for later
  materialization; they are committed once, in one transaction. What remains of asynchronous
  work is a single dirty *flag* per edge type, `meta/matrix_dirty/<edge_type>`
  (`src/keys.rs:23-29`), which says only "this edge type changed, someone should rebuild its
  index". A flag is not a log: it carries no history to replay and nothing to garbage-collect.
]

An edge is stored under a canonical key, and also under two adjacency-index keys so the
engine can scan a vertex's outgoing and incoming neighbors in sorted order. The vertex ids
are formatted as 20-digit zero-padded numbers (`{src:020}`) so that a lexicographic scan of
the keys visits them in numeric order:

#srcblock("src/keys.rs:39-49")[```rust
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
    crossing-fill: reader-colors.paper,
    node-stroke: 0.5pt + reader-colors.border,
    node-fill: reader-colors.surface_soft,
    spacing: (0.1cm, 1.1cm),
    node((0, 0), raw("cell/"), width: 1.7cm),
    node((1, 0), raw("acme/"), width: 1.7cm),
    node((2, 0), raw("e/out/"), width: 1.9cm),
    node((3, 0), raw("RELATES/"), width: 2.3cm),
    node((4, 0), raw("...0042/"), width: 2.1cm),
    node((5, 0), raw("...0099"), width: 2.1cm),
    node((0, 1), text(size: 7.5pt, hyphenate: false)[cell\ namespace], width: 1.7cm, stroke: none, fill: none),
    node((1, 1), text(size: 7.5pt)[cell id], width: 1.7cm, stroke: none, fill: none),
    node((2, 1), text(size: 7.5pt, hyphenate: false)[outgoing\ adjacency], width: 1.9cm, stroke: none, fill: none),
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
  caption: [Anatomy of an outgoing-adjacency key for edge 42 to 99 of type RELATES in
    cell "acme": a prefix scan of `cell/acme/e/out/RELATES/00...0042/` returns every
    out-neighbor of vertex 42 in id order.],
) <fig-found-key-anatomy>

== Snapshots and multi-version concurrency

Because the storage engine only appends, several versions of a key can exist at once, and a
reader has to decide which one it means. Consider a query that scans ten thousand adjacency
keys while a writer is busy adding edges. If the scan simply read whatever was current at each
step, it would see the first half of the graph as it was before a write and the second half as
it was after — a picture that never existed at any single moment. The fix is to fix the
version once, at the start, and hold it.

#custom-box(title: [Term — Multi-version concurrency control (MVCC)], icon: "info")[
  A technique where the store keeps multiple versions of data so that readers and writers do
  not block each other. A reader works from a fixed snapshot version while a writer creates a
  newer version alongside it. In TurboLay this is entirely SlateDB's job: a read opens a
  SlateDB snapshot pinned at a `StorageSequence`, and every key it touches is served from that
  snapshot, so the read sees one consistent version of the graph regardless of concurrent
  writes. Record visibility belongs to the SlateDB snapshot, full stop.
]

#custom-box(title: [Term — Read epoch], icon: "info")[
  The `StorageSequence` a particular query is executing against — the sequence number of the
  snapshot it pinned. It is not chosen by the reader and not stored anywhere; it is simply
  read off the snapshot with `snapshot.seq()` and carried on the query context for the life of
  the query, so that every cache lookup and every index decision inside that query is made
  against the same number.
]

The whole mechanism is a dozen lines at the top of the row-query path. A query with no
client-supplied epoch opens a snapshot, takes its sequence as the read epoch, binds the two
together, and runs the entire query scoped under that snapshot:

#srcblock("src/shard/query.rs:461-478")[```rust
let result = if context.read_epoch.is_none() {
    let snapshot = if context.uses_refreshed_reader() {
        self.db.reader_snapshot().await
    } else {
        self.db.snapshot().await
    };
    match snapshot {
        Ok(snapshot) => {
            let read_epoch = snapshot.seq();
            let context = context.with_validated_storage_read_epoch(read_epoch, read_epoch);
            GraphStore::scope_snapshot(
                snapshot,
                self.execute_parsed_opencypher_rows_inner(context, query),
            )
            .await
        }
        Err(err) => Err(err),
    }
```]

Three details in that fragment carry the whole design. `snapshot.seq()` is where the epoch
comes from — the engine never allocates or increments one, and never reads a "current epoch"
key, because none exists. `with_validated_storage_read_epoch(read_epoch, read_epoch)`
(`src/query/algebra.rs:258-270`) passes the *same* number twice: once as the epoch the query
runs at and once as the storage snapshot that justifies it. They are the same number because
there is only one sequence. And `scope_snapshot` installs the snapshot in task-local state so
that every `get` and `scan` deeper in the call stack is served from it without having to
thread a handle through every function.

The choice between `db.snapshot()` and `db.reader_snapshot()` is the one branch here, and it
belongs to the next section.

#custom-box(title: [Why], icon: "tip")[
  Passing a client-supplied historical epoch is refused outright, with the message
  #emph[historical graph epochs are not storage snapshots; execute against a current SlateDB
  snapshot] (`src/shard/query.rs:450-456`). This is worth reading as a design statement rather
  than a limitation. A `StorageSequence` from the past names a snapshot SlateDB may already
  have compacted away; honouring it would mean either retaining every version forever or
  silently serving something else. TurboLay declines to pretend. Time travel is not offered;
  *catching up* to a known point, which is a different and cheaper promise, is — and that is
  Section 0.8.
]

#figure(
  diagram(
    crossing-fill: reader-colors.paper,
    spacing: (9mm, 9mm),
    node-stroke: 0.6pt + reader-colors.border,
    node-inset: 7pt,
    node-corner-radius: 3pt,
    node((0, 0), text(fill: reader-colors.text, size: 8.5pt, weight: "bold")[`db.snapshot()`],
      fill: reader-colors.info_soft, stroke: reader-colors.info, width: 34mm),
    edge((0, 0), (1, 0), "->", text(fill: reader-colors.muted, size: 7.5pt)[`.seq()`],
      stroke: reader-colors.muted),
    node((1, 0), align(center)[
      #text(fill: reader-colors.text, size: 8.5pt, weight: "bold")[read epoch #emph[S]] \
      #text(fill: reader-colors.muted, size: 7pt)[a `StorageSequence`]
    ], fill: reader-colors.ok_soft, stroke: reader-colors.ok, width: 34mm),
    edge((1, 0), (1, 1), "->",
      text(fill: reader-colors.muted, size: 7pt)[`with_validated_storage_read_epoch(S, S)`],
      stroke: reader-colors.muted, label-side: right),
    node((1, 1), align(center)[
      #text(fill: reader-colors.text, size: 8pt)[every record read at #emph[S]] \
      #text(fill: reader-colors.text, size: 8pt)[every cache keyed on #emph[S]] \
      #text(fill: reader-colors.text, size: 8pt)[every index measured against #emph[S]]
    ], fill: reader-colors.surface_soft, width: 62mm),
    node((0, 1), text(fill: reader-colors.muted, size: 7.5pt, style: "italic")[one axis,\ one answer],
      stroke: none, fill: none, width: 30mm),
  ),
  caption: [The single-sequence model that replaced the old two-sequence one: a read pins a
    SlateDB snapshot, reads its sequence #emph[S], and binds #emph[S] as both the read epoch
    and the storage snapshot that justifies it. Record visibility, cache keys, and index
    freshness are all measured on that one axis, so there is never a second notion of
    "as of when".],
) <fig-found-one-sequence>

== Causal and strong reads

A snapshot makes one query coherent with itself. It says nothing about how two queries relate,
and that gap is where the most common surprise in a distributed database lives.

Picture the simplest possible sequence. A client writes an edge, gets an acknowledgement, and
immediately runs a query that should return it. If the second request lands on a different
node — or even on the same node holding a slightly older reader handle — the snapshot it pins
may predate the write. The client did everything right and still cannot see its own writing.
Nothing is corrupt; the second read is merely *earlier* than the first write, and no amount of
snapshot discipline within that one query can fix it.

The obvious fix is to make every read wait for the newest possible state. That is correct and
it is expensive: it turns every read into a round trip to establish what "newest" currently
means, whether the client needed the guarantee or not. TurboLay instead offers two named
levels and makes the cheaper one the default.

#custom-box(title: [Term — Read consistency level], icon: "info")[
  The choice a client makes about how fresh a read must be. TurboLay offers exactly two, and
  the request carries one of them:

  - *Causal* — the default. The read is guaranteed to include everything the client has
    already been told is durable, and nothing weaker. It does not promise to see writes made
    by other clients in the meantime.
  - *Strong* — the read is refreshed against the latest durable state of the cell before it
    executes, so it includes writes by anyone that had committed before the request began.
]

#srcblock("src/client/service.rs:269-273")[```rust
pub enum ClientReadConsistency {
    Causal,   // #[default]
    Strong,
}
```]

The mechanism that makes causal reads work is a small token the client carries between
requests.

#custom-box(title: [Term — Bookmark], icon: "info")[
  An opaque token naming a point in one cell's history that the client has already observed. A
  `ClientBookmark` is nothing more than a query target plus a `StorageSequence`, encoded as a
  printable string so it can travel through a driver that knows nothing about TurboLay. A
  client that passes a bookmark with its next request is saying: *do not answer me from a
  state older than this one.*
]

#srcblock("src/client/service.rs:130-148")[```rust
pub struct ClientBookmark {
    pub target: ClientQueryTarget,
    pub epoch: StorageSequence,
}

impl ClientBookmark {
    pub fn encode(&self) -> String {
        format!(
            "sgk:1:{}:{}:{}:{}",
            hex_encode(self.target.scope.namespace.to_string().as_bytes()),
            hex_encode(self.target.scope.graph_id.as_str().as_bytes()),
            hex_encode(self.target.cell_id.as_bytes()),
            self.epoch
        )
    }
}
```]

Every result carries a bookmark back out — `ClientQueryResult { read_epoch, bookmark, .. }`
(`src/client/service.rs:340-344`) — so the client never has to construct one. It receives a
bookmark from each response and hands the latest one back on the next request, and the loop
closes: what it saw last time is the floor for what it sees next time.

The two levels are then two small pieces of code. A causal read honours the bookmark by
waiting for the cell's durable sequence to reach it, and refuses rather than silently serving
something older:

#srcblock("src/client/service.rs:719-741 (abridged)")[```rust
pub async fn ensure_bookmark(&self, bookmark: &ClientBookmark) -> Result<()> {
    let current_sequence = self.inner.client
        .wait_for_storage_sequence(
            &bookmark.target.scope, &bookmark.target.cell_id, bookmark.epoch,
        ).await?
        .ok_or_else(|| /* backend cannot prove bookmark durability */)?;
    if current_sequence < bookmark.epoch {
        return Err(GraphError::SnapshotAhead { /* ... */ });
    }
    Ok(())
}
```]

A strong read does not wait for a number the client supplied; it goes and finds the current
one. `refresh_strong_read` (`src/client/service.rs:1380-1403`) returns immediately unless the
request asked for `Strong`, rejects strong *writes* as meaningless, and otherwise refreshes the
cell's durable frontier before the query runs. It also flips one bit on the query context —
`context.with_refreshed_reader()` (`src/client/service.rs:1676-1678`) — and that bit is exactly
the branch you already saw in Section 0.7: it is what makes the query choose
`db.reader_snapshot()` over `db.snapshot()`, forcing a fresh reader rather than reusing a
cached one.

#custom-box(title: [Why], icon: "tip")[
  Causal is the default (`src/client/service.rs:270`) because it is the level that matches what
  users actually expect. Almost every complaint about eventual consistency is really a
  complaint about *read-your-writes*: people are untroubled by not seeing a stranger's edit and
  very troubled by not seeing their own. A bookmark buys read-your-writes for the cost of
  passing a short string, with no coordination when the state is already fresh enough. Strong
  remains available for the cases that genuinely need someone else's writes, and it charges for
  itself honestly, at the point of use.
]

This section defines the vocabulary; the read-path chapter owns the mechanism and follows a
bookmark through the transport, the coordinator, and the shard.

== The identity hierarchy: namespaces, graphs, and scopes

A cell id is an internal storage key. The outward-facing identity of a graph is richer,
because TurboLay is multi-tenant.

#custom-box(title: [Term — Namespace], icon: "info")[
  A named tenant boundary. Namespaces can nest to form a path, for example a company and
  then a team inside it. A `NamespaceId` is a single validated name; a `NamespacePath` is the
  ordered list of names from the root tenant down to the leaf. The nesting depth is capped.
]

#srcblock("src/core/namespace.rs:5-7")[```rust
pub const DEFAULT_NAMESPACE_ID: &str = "default";
pub const DEFAULT_GRAPH_ID: &str = "default";
pub const MAX_NAMESPACE_DEPTH: usize = 8;
```]

#custom-box(title: [Term — Graph id and graph scope], icon: "info")[
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

== Writer, reader, and SlateDB manifest fencing

Section 0.3 said the object store is the single source of truth. That raises a hazard: if two
processes both believe they are the writer for the same cell, they can corrupt it. Only one
writer per cell may exist at a time.

The tempting answer is to build a lock in the object store — a record naming the current
owner, with a lease that must be renewed. TurboLay used to do exactly that, and the reason it
no longer does is instructive. A lease-based lock is only as good as the clock that measures
it: an owner that pauses for longer than its lease (a long garbage-collection pause, a stalled
network call) is not *told* it has lost ownership. It wakes up believing it still holds the
lock and writes. Meanwhile a second process, having watched the lease expire, has legitimately
taken over. Both are convinced they are the writer, which is precisely the state the lock was
meant to prevent.

TurboLay now delegates the problem to the layer that can actually solve it.

#custom-box(title: [Term — SlateDB manifest fencing], icon: "info")[
  SlateDB records the identity of the current writer in its own manifest in the object store.
  When a new writer opens the database it advances that record, which *fences* every earlier
  writer: the old handle is not asked to stand down, it is simply invalidated, and the next
  operation it attempts fails with `Closed(Fenced)`. The guarantee does not depend on a clock
  or on the fenced process being responsive, because it is enforced at the point of the write
  rather than by agreement beforehand. TurboLay holds no lock record of its own; there is no
  owner token, no TTL, and no lease.
]

A node checks its standing by asking SlateDB to re-read the manifest, and reacts to being
fenced by throwing away the handle it can no longer use:

#srcblock("src/core/state.rs:187-203")[```rust
pub(crate) async fn refresh_writer_fence(&self) -> Result<()> {
    let _open_guard = self.inner.writer_open_gate.lock().await;
    let writer = self.open_writer().ok_or(GraphError::ReadOnlyShardStorage)?;
    match writer.refresh_manifest().await {
        Ok(()) => Ok(()),
        Err(err) if matches!(err.kind(), ErrorKind::Closed(CloseReason::Fenced)) => {
            *self.inner.writer.write()... = None;
            Err(err.into())
        }
        Err(err) => Err(err.into()),
    }
}
```]

Dropping the cached handle is the whole of the local response. There is no lock to release and
no state to unwind, because the node never owned anything a peer needs handed back.

This works because `GraphStore` no longer *is* a writer or a reader; it is a handle that opens
either one lazily and caches it. Its inner state holds two slots, both initially empty:

#srcblock("src/core/state.rs:72-87")[```rust
#[derive(Clone)]
pub(crate) struct GraphStore { inner: Arc<GraphStoreInner> }

struct GraphStoreInner {
    path: Path,
    object_store: Arc<dyn ObjectStore>,
    // ... cache / memory / durability configuration ...
    writer: StdRwLock<Option<Db>>,
    reader: AsyncRwLock<Option<Arc<DbReader>>>,
    writer_open_gate: Mutex<()>,
    reader_open_gate: Mutex<()>,
}
```]

An empty `writer` slot is not a permission; it merely means no writer has been opened yet.
Permission is a separate, explicit property of the shard.

#custom-box(title: [Term — Write authority], icon: "info")[
  What a node is *allowed* to do with a cell, independent of what it currently holds open.
  There are three levels: `ReadOnly` may never write; `Promotable` may become the writer if it
  needs to; `Writer` already is one. The distinction between `ReadOnly` and `Promotable` is
  policy — whether this node is a candidate for the writer role at all — while the distinction
  between `Promotable` and `Writer` is merely whether the handle has been opened yet.
]

#srcblock("src/core/state.rs:471-476")[```rust
pub(crate) enum GraphWriteAuthority {
    ReadOnly,
    Promotable,
    Writer,
}
```]

Every write begins by consulting it. `ReadOnly` is refused outright; the other two fall
through to the same call, which returns the writer handle or fails if none can be had:

#srcblock("src/shard/lifecycle.rs:404-418")[```rust
pub(crate) fn ensure_write_authority(
    &self, cell_id: &str, operation: &'static str,
) -> Result<()> {
    match &self.write_authority {
        GraphWriteAuthority::ReadOnly => Err(GraphError::WriteRequiresWriter {
            operation, cell_id: cell_id.to_string(),
        }),
        GraphWriteAuthority::Promotable | GraphWriteAuthority::Writer => {
            self.db.writer().map(|_| ())
        }
    }
}
```]

A `Promotable` node becomes a writer by calling `promote_to_writer`
(`src/shard/lifecycle.rs:420-434`), which re-checks that the node is not `ReadOnly` and then
asks the store to open a writer. `GraphStore::promote_writer` (`src/core/state.rs:204-227`)
does the opening under `writer_open_gate` with a double check, so that many concurrent
requests to promote result in exactly one open database.

#custom-box(title: [Term — Write lane], icon: "info")[
  One of 64 mutexes (`GRAPH_WRITE_LANES`, `src/lib.rs:178`) that a cell's writes are
  distributed across by cell id. Lanes are a *throughput* device, not a correctness one: they
  keep unrelated writes inside the one legitimate writer from serializing behind each other.
  They say nothing about which process may write.
]

Notice what `write_edge` (`src/shard/write.rs:2354-2384`) does at its start: it validates its
arguments, calls `ensure_write_authority`, takes a write permit and a writer lane, and enters
a retry loop over a serializable transaction. It acquires no lock, waits on no lease, and
contacts no peer. Correctness against a competing process is not established here at all — it
is established by SlateDB, when the transaction tries to commit and the manifest says whether
this writer is still the writer. The write chapter takes those three tiers apart in detail.

== Bolt and the client surface

Finally, how a program talks to TurboLay from outside the process.

#custom-box(title: [Term — Bolt], icon: "info")[
  The binary network protocol used by the Neo4j family of graph databases and their client
  drivers. A Bolt client opens a connection, authenticates, sends a query with parameters,
  and pulls back rows. TurboLay speaks Bolt so that existing Neo4j drivers, in Python, Go,
  and other languages, can connect to it without a custom client. It also exposes an HTTPS
  query API for the same purpose.
]

The Bolt and HTTPS servers, and the shared service layer behind them, are the top of the
crate's public surface:

#srcblock("src/lib.rs:24-37")[```rust
#[cfg(feature = "bolt-server")]
pub use client::bolt::{
    BoltRoutingServer, BoltRoutingTable, BoltRoutingTableProvider, BoltServerConfig,
    BoltServerHandle, ClientBoltServer, ObjectStoreBoltRoutingTableProvider,
};
#[cfg(feature = "http-api")]
pub use client::http::{ClientHttpServer, HttpQueryServerConfig, HttpQueryServerHandle};
#[cfg(feature = "client-api")]
pub use client::service::{
    ClientBookmark, ClientDatabaseResolver, /* ... */ ClientQueryService,
    ClientQueryTarget, ClientReadConsistency, StaticClientDatabaseResolver,
};
```]

`ClientBookmark` and `ClientReadConsistency` in that last list are the causal- and strong-read
vocabulary from Section 0.8, exported at the crate root because they are part of the contract
a client program writes against. Note also that the Bolt routing-table provider is
`ObjectStoreBoltRoutingTableProvider`: routing is answered from a directory in the object
store, not from a hash of the database name, which matters when the architecture chapter
explains why every node can open every cell.

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
- *Storage sequence, read epoch, MVCC*: the one and only sequence in the system. A read pins a
  SlateDB snapshot, takes its sequence as the read epoch, and measures record visibility, cache
  keys, and index freshness against that single number.
- *Causal read, strong read, bookmark*: the two consistency levels a client may ask for, and
  the token that carries "what I have already seen" from one request to the next.
- *Namespace, graph id, graph scope*: the multi-tenant identity that maps to a cell.
- *SlateDB manifest fencing, write authority, write lane*: how exactly one writer per cell is
  guaranteed — the manifest invalidates a superseded writer, `GraphWriteAuthority` says which
  nodes may hold one, and lanes spread that one writer's work for throughput.
- *Bolt*: the wire protocol that lets standard graph drivers connect.

Two of these are worth restating as negatives, because the previous edition of this book said
otherwise and the wrong version is memorable. There is no second sequence: no topology cursor,
no `TopologySequence`, no delta log measured against one. And there is no lock: no cell write
lock, no owner token, no lease, and no TTL — single-writer safety comes from SlateDB's
manifest, not from a record TurboLay maintains.

With these in hand, the next chapter lays out how the code is organized and how these pieces
are wired into the one type that ties them together, the `GraphShard`.
