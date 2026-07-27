#import "../book/vendor/bookly/src/bookly.typ": *
#import "../book/template.typ": term, why, srcblock, figcap, accent, muted

= The Graph Must Survive the Compute Node

Imagine a graph node serving a busy application. It has parsed queries in
memory, recently read graph structures on its local disk, and several active
requests halfway through execution. Then the machine disappears.

The process is gone. Its memory is gone. Its local disk may be gone. A new
process starts somewhere else with no useful local state.

What does it mean for the replacement to serve *the same graph*?

This is not initially a question about graph algorithms. It is a question
about what must survive a machine failure, what may be rebuilt, and who is
allowed to change the durable state while machines are being replaced.

The architecture becomes easier to understand when we solve those problems in
order.

== Problem 1: a fast machine is not a durable graph

A graph server usually has two kinds of state mixed together:

- facts: vertices, edges, properties, versions, and deletion markers;
- conveniences: parsed queries, indexes in memory, hydrated adjacency, and
  recently fetched storage blocks.

If both kinds live only in the process, the graph disappears with the process.
If both kinds live only on a remote object store, every query repeatedly pays
the cost of finding and decoding remote data.

So the first design decision is a distinction:

#boxeq[
  *Durable state answers what the graph is. Compute state makes answering
  faster.*
]

#term("Durable state")[
  Records that must remain available after a compute process, its memory, and
  its local disk disappear. In turbolay, the durable records are stored through
  SlateDB on an object store such as S3, MinIO, or a local object-store
  implementation.
]

#term("Compute state")[
  Data held by a running node to execute work efficiently: open database
  handles, semaphores, writer lanes, parsed plans, hydrated graph structures,
  and caches. It can be discarded and reconstructed from durable state.
]

This distinction gives us a test for every field and every optimization:

#figure(
  table(
    columns: (1.2fr, 1fr, 1.5fr),
    inset: 8pt,
    align: (left + top, left + top, left + top),
    table.header([*Question*], [*If the answer is durable*], [*If the answer is compute-local*]),
    [Does its loss change the meaning of the graph?], [It belongs in the object store.], [It must be rebuilt or treated as a miss.],
    [Can a replacement node reconstruct it?], [It is part of the recovery boundary.], [It is part of the warm-up cost.],
    [Can it be stale without changing correctness?], [It needs versioning or a transaction.], [It is a cache or an execution aid.],
  ),
  caption: [The durability test separates truth from acceleration.],
)

The separation does not mean the compute node is stateless. An active node
holds leases, locks, open handles, and work in progress. It means those things
are not the only copy of the graph's meaning.

== Problem 2: remote truth alone is too slow to query

Suppose every edge lookup is a fresh object-store operation. A simple
neighborhood query may need many key reads. A traversal may need to hydrate
the same adjacency repeatedly. A Cypher query may parse the same text again
and again.

The obvious response is caching. But caching introduces a dangerous question:
what happens when a cache is wrong or empty?

The safe answer is that a cache miss may cost time, never truth. turbolay has
two acceleration layers:

#figure(
  table(
    columns: (1.4fr, 1.3fr, 1.4fr),
    inset: 8pt,
    align: (left + top, left + top, left + top),
    table.header([*Layer*], [*Stores*], [*A miss means*]),
    [SlateDB object-store cache], [Raw storage blocks, configured through `GraphCacheConfig`], [Fetch blocks again from the object store],
    [Graph-layer caches], [Artifacts, adjacency, parsed queries, traversal results, and supernode data], [Rebuild or reload the derived value],
  ),
  caption: [Both layers reduce work; neither becomes the source of graph truth.],
)

The lower layer is configured when the graph database opens. The upper layer
is owned by `GraphShard` and bounded by graph cache policy. The source code
keeps these concerns visible: storage settings live in `src/core/config.rs`,
cache structures live in `src/core/cache.rs`, and the shard owns the cache
instances in `src/core/state.rs`.

#srcblock("src/core/state.rs:33-78 (abridged)")[```rust
pub struct GraphShard {
    pub(crate) db: GraphStore,
    pub(crate) object_store: Arc<dyn ObjectStore>,
    pub(crate) store_path: Path,
    pub(crate) cache_policy: GraphCachePolicy,
    pub(crate) hydration_gate: Arc<Semaphore>,
    pub(crate) graph_write_gate: Arc<Semaphore>,
    pub(crate) writer_lanes: Vec<Mutex<()>>,
    // matrix, traversal, query, posting, and supernode caches ...
}
```]

`GraphShard` is therefore not merely a cache and not merely a database
handle. It is the compute-side object that combines durable access, query
execution, maintenance, concurrency limits, and disposable acceleration.

#why[
  Keeping truth in the object store makes replacement possible. Keeping
  derived data near the process makes repeated reads practical. The two goals
  conflict only when a cache is allowed to become authoritative.
]

== Problem 3: one giant graph creates one giant coordination problem

Now consider writes. A single edge insertion is not one physical record. The
engine may need to update the canonical edge, forward and reverse access
paths, degree counters, metadata indexes, an epoch, an outbox delta, and an
idempotency result.

If every graph write shares one global lock, unrelated parts of the graph
block one another. If every writer updates records independently, readers can
observe a half-written edge.

We need a unit that is both:

1. small enough to distribute;
2. large enough to be an atomic write boundary.

That unit is a *cell*.

#term("Cell")[
  An addressable graph partition. In the current implementation, a cell is
  the unit of storage path, placement, lease, epoch, and write authority. The
  public APIs carry `cell_id` through mutations, snapshots, and query
  contexts.
]

The storage layout makes the boundary concrete. A cluster opens a cell below a
base path, conceptually:

```text
object store
└── base_path
    ├── cell_id_a  -> SlateDB database for cell A
    ├── cell_id_b  -> SlateDB database for cell B
    └── cell_id_c  -> SlateDB database for cell C
```

The exact object-store encoding is handled by SlateDB and the key helpers, but
the design consequence is simple: cell A's durable records do not require
cell B's writer to participate in the same transaction.

#figure(
  table(
    columns: (1.2fr, 1fr, 1.5fr),
    inset: 8pt,
    align: (left + top, left + top, left + top),
    table.header([*Without cells*], [*With cells*], [*What improves*]),
    [One shared write boundary], [One write boundary per cell], [Unrelated writes can progress independently],
    [One placement decision for the whole graph], [Placement per cell], [Ownership can move in smaller units],
    [One failure domain], [Failure and recovery per cell], [A replacement can reopen only its assignments],
    [One global epoch assumption], [Epochs named within a cell], [Snapshot reasoning stays local and explicit],
  ),
  caption: [Partitioning is useful because it partitions coordination, not just data.],
)

Cell isolation has a precise limit: it does not create a transaction across
cells. A mutation that touches two cells needs an application-level protocol;
the kernel's atomicity guarantee is cell-local.

== Problem 4: a write needs one visible version

Even inside one cell, a reader needs a stable answer while writes continue.
Consider this sequence:

1. a reader begins while the graph contains edge `1 -> 2`;
2. a writer deletes that edge;
3. the reader asks for the edge's reverse index;
4. the reader asks for its degree.

If each read uses the newest state independently, one logical query can combine
values from before and after the deletion. The result is not a snapshot of any
real graph state.

The solution is to name the graph state being read.

#term("Epoch")[
  A monotonically increasing version number within a cell. A mutation is
  committed at a new epoch, and a snapshot read uses a chosen epoch as its
  visibility boundary.
]

The read rule is:

#boxeq[
  *A read at epoch N sees the durable state that was committed at or before N,
  not whichever individual key happens to be newest when it is read.*
]

`GraphSnapshot` carries both the cell identity and the read epoch. Its methods
such as `edge_exists`, `out_neighbors`, `out_degree`, and matrix traversal all
pass that same epoch into the shard (`src/core/snapshot.rs`). This is how one
query keeps one version of the world in view.

The write side needs the matching guarantee. The logical edge mutation and its
related records are committed in one serializable SlateDB transaction. The
caller receives a `CommitResult` containing the epoch at which the mutation
became visible (`src/core/model.rs`).

#srcblock("src/core/model.rs:55-71, 275-279")[```rust
pub struct EdgeMutation {
    pub cell_id: String,
    pub edge_type: String,
    pub src: VertexId,
    pub dst: VertexId,
    pub idempotency_key: String,
}

pub struct CommitResult {
    pub epoch: GraphEpoch,
    pub already_existed: bool,
}
```]

The important idea is not the field layout. It is the pairing:

- writes publish a complete logical change at one epoch;
- reads ask for one epoch and use it consistently.

Without both halves, version numbers are decoration rather than concurrency
control.

== Problem 5: complete history is correct but too expensive

Epochs let us define a correct snapshot, but a new reader should not have to
replay the entire life of a cell from its first write.

Suppose a cell has one million historical edge changes. A query at epoch
`1,000,100` needs the answer at that point, not a million-step archaeology
project.

The engine therefore separates canonical records from derived read
structures.

#term("Canonical record")[
  A durable record that represents graph meaning directly: an edge, metadata,
  an index entry, a degree counter, a deletion marker, or a versioned delta.
]

#term("Artifact")[
  A durable, derived read structure built from canonical records. An artifact
  records the `base_epoch` it represents and can be combined with later delta
  records to answer a newer snapshot.
]

The read calculation becomes:

#boxeq[
  *state at read epoch = artifact at base epoch + ordered changes after the
  base through the read epoch*
]

For example:

#figure(
  table(
    columns: (1fr, 0.5fr, 1.2fr, 0.5fr, 1fr),
    inset: 8pt,
    align: center,
    [matrix base at 100], [`+`], [deltas 101 through 107], [`=`], [answer at 107],
  ),
  caption: [A lagging artifact is safe when the missing interval is replayed in order.],
)

The artifact is not a second definition of the graph. It is a shortcut for
reconstructing a defined version of the graph. If an artifact is partial or
not published as a complete unit, the read path must ignore it.

The current engine exposes several forms of this shortcut, including matrix
artifacts, posting chunks, and supernode groups. The traversal code chooses an
appropriate backend, hydrates the required structure, and applies the delta
overlay needed to reach the requested epoch (`src/engine/artifact_build.rs`,
`src/engine/traversal.rs`, and `src/engine/supernode.rs`).

#why[
  This design gives maintenance room to improve future reads without changing
  what an edge means. Canonical records preserve meaning; artifacts trade
  storage and build work for faster reconstruction.
]

== Problem 6: multiple nodes can all reach the same object store

Putting data in shared storage makes replacement possible, but it also means
two processes can reach the same cell. A stale process may still have network
access after it should have stopped writing.

There are three different questions here, and they need three different
answers:

1. Which node should serve a cell?
2. Which node is currently allowed to write it?
3. How does the data path reject a writer that has become stale?

The control plane answers the first two. The data plane enforces the third.

#term("Placement")[
  A mapping from `cell_id` to an owning node. `ShardPlacement` can be fixed or
  produced using rendezvous hashing. Placement says where a cell should run;
  it does not, by itself, prove that a process is still allowed to write.
]

#term("Lease")[
  A time-bounded ownership record for a cell. A `ShardLease` carries the cell,
  owner node, lease token, and expiry time. The control plane persists and
  renews it.
]

#term("Fence")[
  A durable data-path record binding writes to a particular owner and lease
  token. A transaction validates the fence before it commits, so an older
  owner cannot write merely because it still has an open connection.
]

The sequence is:

#figure(
  table(
    columns: (1.4fr, 0.35fr, 1.6fr, 0.35fr, 1.5fr),
    inset: 8pt,
    align: center,
    [control plane], [`→`], [placement and lease for cell A], [`→`], [node owning cell A],
    [node], [`→`], [durable write fence with lease token], [`→`], [cell-local transaction],
  ),
  caption: [Control-plane ownership becomes a data-plane write condition.],
)

In the current code, `GraphControlPlane` stores placement and lease metadata.
`GraphNode` opens the cells assigned to its node ID and renews leases in the
background. `GraphShard` checks its local write authority, validates the
durable fence inside the write transaction, and also uses a cross-process cell
write lock (`src/engine/control_plane.rs`, `src/engine/cluster.rs`,
`src/shard/lifecycle.rs`).

These protections overlap because they cover different failures:

#figure(
  table(
    columns: (1.2fr, 1.5fr),
    inset: 8pt,
    align: (left + top, left + top),
    table.header([*Mechanism*], [*Problem it handles*]),
    [Local write authority], [A read-only shard or a process without a live lease tries to write],
    [Writer lane], [Two tasks in one process race for the same cell],
    [Object-store cell lock], [Two processes attempt the same cell concurrently],
    [Durable write fence], [An old lease holder reaches the object store after takeover],
    [Serializable transaction], [Related records must commit together and conflicts must be retryable],
  ),
  caption: [Ownership, serialization, fencing, and atomicity are separate jobs.],
)

== The complete mental model

We can now assemble the architecture without relying on a slogan:

#figure(
  table(
    columns: (1.1fr, 1.3fr, 1.5fr),
    inset: 8pt,
    align: (left + top, left + top, left + top),
    table.header([*Layer*], [*What it owns*], [*What it must never assume*]),
    [Client and query API], [Cypher requests, parameters, pages, and protocol details], [That a request is automatically global across cells],
    [Compute node], [Query execution, traversal, maintenance, locks, leases, and caches], [That its local memory is authoritative],
    [Cell boundary], [Placement, write ownership, epochs, and cell-local atomicity], [That two cells commit as one transaction],
    [SlateDB], [Key-value transactions, reads, scans, and durable records], [That a derived artifact replaces canonical truth],
    [Object store], [The durable storage substrate shared across replacements and nodes], [That reachability alone grants write authority],
  ),
  caption: [The architecture is a set of boundaries, each with a specific responsibility.],
)

The resulting request lifecycle is concrete:

1. A client sends a query or mutation to a compute node.
2. The node identifies the target cell and obtains a read or write context.
3. A read pins an epoch, then loads canonical data or a valid artifact and
   applies the required versioned changes.
4. A write checks authority, serializes within the cell, and commits all of its
   related records in one transaction at a new epoch.
5. A cache may shorten any of these steps, but a miss falls back to durable
   state.
6. If the node disappears, another node can reopen the cell from the same
   object-store path and reconstruct its compute state.

This is the central intuition for turbolay:

#boxeq[
  *Make the durable state complete enough to rebuild, the cell boundary small
  enough to coordinate, and every acceleration layer disposable enough to
  lose.*
]

== What this architecture guarantees—and what it does not

The design guarantees a useful, bounded contract:

- durable graph state is not dependent on one process's memory;
- reads can name and preserve a cell-local snapshot epoch;
- one cell-local mutation commits its related durable records atomically;
- stale leased writers are rejected by the data-path fence;
- derived artifacts and caches can be rebuilt from durable state;
- cells can be placed and served by different compute nodes.

It does not claim more than the code provides:

- compute is replaceable, not completely stateless;
- the kernel does not provide a transparent global Cypher planner for arbitrary
  cross-cell queries;
- a distributed request does not automatically acquire one global snapshot;
- there is no multi-cell atomic commit protocol;
- artifacts and caches are accelerators, not a new durable source of truth.

Those limits are not footnotes. They are part of the architecture's shape.
Once the boundaries are explicit, the implementation details—keys, leases,
transactions, snapshots, artifacts, and caches—have somewhere precise to fit.

== Revision notes

Use these notes to reconstruct the chapter's argument quickly.

=== The ideas to remember

- *Truth and acceleration are different.* Durable graph meaning belongs in
  SlateDB on the object store. Memory, local disk, hydrated structures, parsed
  plans, and caches may disappear and be rebuilt.
- *A cell is the coordination boundary.* Storage paths, placement, leases,
  epochs, and atomic writes are cell-local. Partitioning the graph therefore
  partitions coordination as well as data.
- *A snapshot needs one named version.* A write publishes all related records
  at one new epoch; a read carries one epoch through every lookup. Using the
  latest value independently for each key does not produce a coherent view.
- *Canonical records define the graph.* Artifacts summarize a base epoch and
  replay later deltas to reach the requested epoch. They improve read speed but
  never become a second source of truth.
- *Write safety has several layers.* Placement routes work, a lease grants
  temporary ownership, a durable fence rejects a stale owner, locks serialize
  writers, and the transaction makes related record changes atomic.
- *Replaceable compute is not stateless compute.* A live node has useful local
  state, but no irreplaceable graph meaning lives only there.

=== The compact equations

#figure(
  table(
    columns: (1.25fr, 1.75fr),
    inset: 7pt,
    align: (left + top, left + top),
    table.header([*Question*], [*Revision answer*]),
    [What is a snapshot at epoch N?], [One consistent view of records committed at or before N.],
    [How is a newer read reconstructed?], [Artifact at `base_epoch` + ordered deltas through `read_epoch`.],
    [What survives node loss?], [Canonical durable state in the shared object store.],
    [What may be lost safely?], [Caches, hydrated data, parsed plans, open handles, and other compute state.],
    [What is atomic?], [One logical mutation and its related records within one cell.],
    [What is not promised?], [A global snapshot, transparent cross-cell planning, or multi-cell atomic commit.],
  ),
  caption: [The chapter's invariants in revision form.],
)

=== A quick correctness test

When evaluating a new feature, ask:

1. If the compute node vanishes, can durable state reconstruct the answer?
2. Does every read use one cell and one explicit epoch consistently?
3. Can a cache miss change only latency, never graph meaning?
4. Are all records for one logical mutation committed together?
5. Can an expired owner still pass the durable fence?
6. Does the design accidentally assume atomicity or one snapshot across cells?

#boxeq[
  *Durable truth makes recovery possible; cell-local epochs and transactions
  make reads coherent; disposable acceleration makes the recovered system
  practical.*
]
