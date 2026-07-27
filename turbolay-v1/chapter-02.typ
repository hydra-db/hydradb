#import "../book/vendor/bookly/src/bookly.typ": *
#import "../book/template.typ": term, why, srcblock, figcap, accent, muted

= A Graph Has Cells

The first chapter established a durability boundary: the object store keeps
the graph's meaning, while compute nodes keep the machinery that makes the
meaning useful.

That boundary creates a practical question. If several compute nodes can reach
the same object store, how do we decide which node should open which part of
the graph? More importantly, how do we stop two nodes from believing that
they are both allowed to write the same data?

We need to solve four problems:

1. give data a stable address;
2. divide the graph into manageable ownership units;
3. map those units to compute nodes;
4. make ownership expire safely when a node fails.

turbolay solves these with four different concepts: *scope*, *cell*,
*placement*, and *lease*. They are related, but they are not interchangeable.

== Problem 1: “the graph” is not a sufficient address

Imagine two customers both use a graph called `default`. If the storage path
is simply:

```text
graphs/default
```

then the name does not tell us whose graph it is. We could put the customer ID
into every application key, but that makes isolation depend on every caller
remembering to add the prefix correctly.

The storage address should express the ownership hierarchy before graph data
is read or written.

#term("Namespace")[
  A validated identity component used to separate tenants or organizational
  scopes. Namespaces may be nested, but they are not the same thing as graph
  cells and they do not decide which compute node owns a cell.
]

#term("Graph scope")[
  The pair of a namespace path and a graph ID. A scope identifies one logical
  graph within the namespace hierarchy. In the code it is represented by
  `GraphScope` in `src/core/namespace.rs`.
]

The current path construction is explicit:

#srcblock("src/core/namespace.rs:226-258")[```rust
pub struct GraphScope {
    pub namespace: NamespacePath,
    pub graph_id: GraphId,
}

pub fn scoped_store_path(&self, base_path: &str) -> String {
    let scoped_path = format!(
        "{}/graphs/{}",
        self.namespace.storage_suffix(),
        self.graph_id
    );
    format!("{base_path}/{scoped_path}")
}
```]

For a nested namespace, the resulting shape is conceptually:

```text
base_path/
└── namespaces/acme
    └── subnamespaces/analytics
        └── graphs/recommendations
```

The path answers “which logical graph is this?” It does not yet answer “which
node may write it?” That is a separate concern.

The implementation validates namespace IDs and graph IDs as path components.
An empty namespace path is rejected, and namespace depth is bounded by
`MAX_NAMESPACE_DEPTH` (currently eight). These checks are not cosmetic: a
component that can contain arbitrary separators or an unbounded hierarchy can
turn an identity field into an accidental path language.

== Problem 2: a whole graph is too large a coordination unit

Suppose the graph has one storage path and one writer boundary. A write to an
unrelated customer, region, or workload still competes for the same process
and the same coordination point. Recovery has the same shape: one replacement
must reopen and manage the entire graph.

We could split only the compute process, but then every process still needs a
shared answer to questions such as:

- which records belong to this process?
- which process may allocate the next epoch for those records?
- which process should a query contact?
- which unit should move during failover?

The answer must be visible in the data model, not inferred from process
memory. That unit is the cell.

#term("Cell")[
  The unit of graph storage opened by a shard, placement assigned by the
  control plane, lease granted to a node, and write authority checked by the
  data path. A cell carries its own cell-local epochs and write boundary.
]

The distinction between scope and cell is easiest to see as a hierarchy:

#figure(
  table(
    columns: (1.5fr, 0.35fr, 1.5fr, 0.35fr, 1.5fr),
    inset: 8pt,
    align: center,
    [namespace path], [`+`], [graph ID], [`=`], [graph scope],
    [graph scope], [`+`], [cell ID], [`=`], [addressable storage unit],
    [cell ID], [`+`], [node ID], [`=`], [placement assignment],
  ),
  caption: [Identity, storage partitioning, and runtime ownership are separate layers.],
)

The cell ID is a graph-kernel boundary. It is not automatically a tenant ID,
namespace ID, or graph ID. An embedding service may choose one cell per
tenant, many cells per tenant, or a workload-specific partitioning scheme.
The kernel uses the cell ID it is given and enforces its boundaries; it does
not silently invent a tenancy policy.

== Problem 3: the storage path must match the cell boundary

Once we decide that a cell is the ownership unit, opening a cell must lead to
one unambiguous SlateDB path. Otherwise two nodes could agree on the cell ID
but open different physical databases, or two cell IDs could accidentally share
one database.

`GraphCluster::open_cells_scoped` first builds the scope path, then appends each
validated cell ID:

#srcblock("src/engine/cluster.rs:816-891 (abridged)")[```rust
let store_path = scope.scoped_store_path(&base_path.into());

for cell_id in &cell_ids {
    validate_component("cell_id", cell_id)?;
}

for cell_id in cell_ids {
    let path = format!("{store_path}/{cell_id}");
    let shard = GraphShard::open(path, Arc::clone(&object_store)).await?;
    shards.insert(cell_id, shard);
}
```]

The conceptual address is therefore:

```text
base path
└── namespace path
    └── graph ID
        ├── cell A -> one SlateDB database
        ├── cell B -> one SlateDB database
        └── cell C -> one SlateDB database
```

This gives us a useful recovery property. Reopening cell B does not require
reconstructing cell A's in-memory state first. A replacement needs the same
object store, the same scope, and the same cell path.

#why[
  The cell is not merely a label attached to a query. It is part of the
  physical address, so the ownership boundary and the durable storage boundary
  have the same shape.
]

== Problem 4: deciding ownership by convention is not enough

With three cells and two nodes, we need an explicit mapping:

```text
cell-a -> node-1
cell-b -> node-2
cell-c -> node-1
```

If each node independently guesses its cells, a configuration mismatch can
produce two bad outcomes:

- both nodes open the same cell and attempt to write it;
- neither node opens the cell, so it becomes unavailable even though its data
  still exists.

`ShardPlacement` makes the mapping a value that can be validated, persisted,
loaded, and passed into a routed cluster.

#srcblock("src/engine.rs:277-281; src/engine/cluster.rs:912-1000 (abridged)")[```rust
pub struct ShardPlacement {
    owners: BTreeMap<String, String>,
}

impl ShardPlacement {
    pub fn fixed(
        assignments: impl IntoIterator<Item = (impl Into<String>, impl Into<String>)>,
    ) -> Result<Self>;

    pub fn rendezvous(
        cell_ids: impl IntoIterator<Item = impl Into<String>>,
        node_ids: impl IntoIterator<Item = impl Into<String>>,
    ) -> Result<Self>;

    pub fn owner(&self, cell_id: &str) -> Result<&str>;
    pub fn cells_for_node(&self, node_id: &str) -> Result<Vec<String>>;
}
```]

There are two ways to construct the mapping:

- *fixed placement* accepts explicit cell-to-node assignments;
- *rendezvous placement* scores each cell against the available node IDs and
  selects an owner.

The rendezvous function is not a query planner and it is not a replication
protocol. It answers one narrow question: which node is the primary owner for
this cell under the current node set?

The placement object also provides the inverse operation, `cells_for_node`.
That inverse is important because a node should open the cells assigned to it,
not scan the entire object store and guess which paths it ought to serve.

== Problem 5: opening a database is not the same as owning it

A process may have permission to read a cell without having permission to
write it. The code represents this distinction in the shard's storage and
authority state.

There are three useful opening modes:

#figure(
  table(
    columns: (1.25fr, 1.25fr, 1.6fr),
    inset: 8pt,
    align: (left + top, left + top, left + top),
    table.header([*Mode*], [*Storage handle*], [*Meaning*]),
    [`GraphShard::open`], [`GraphStore::Reader`], [Can read durable state; writes fail as read-only],
    [`open_standalone_writer`], [`GraphStore::Writer`], [Can write without a cluster lease; useful for local or single-process operation],
    [`open_leased_writer`], [`GraphStore::Writer`], [Can write only while the local leased authority is valid],
  ),
  caption: [A readable database handle does not imply write ownership.],
)

This distinction prevents a common mistake in distributed storage design:
assuming that because a process can open an object-store path, it may mutate
the records behind it.

The routed cluster uses placement to select its local cells. A node with ID
`node-1` opens only the cells for which `placement.cells_for_node("node-1")`
returns an assignment. It retains those shards locally so query and write
operations do not need to rediscover the mapping for every call.

The local shard still checks authority at operation time. Placement is a
routing decision; authority is a write decision.

== Problem 6: a crashed node can look alive to the object store

Now consider a failure timeline:

1. `node-old` owns cell A.
2. Its process stops renewing its lease, but its network connection remains
   usable for a while.
3. The control plane lets `node-new` take over after the lease expires.
4. `node-old` wakes up and tries to commit a write to cell A.

The object store cannot infer that `node-old` is stale from the TCP connection
alone. It needs a durable generation of ownership that the write transaction
can check.

#term("Lease token")[
  A monotonically advancing generation number attached to cell ownership. A
  renewal extends the expiry of the current lease; a later acquisition uses a
  newer token so an old owner cannot be confused with the new owner.
]

The current lease record is deliberately small:

#srcblock("src/engine.rs:321-328")[```rust
pub struct ShardLease {
    pub cell_id: String,
    pub owner_node_id: String,
    pub lease_token: u64,
    pub expires_at_ms: u64,
}
```]

The control plane persists placement and lease metadata in its own scoped
SlateDB database (`src/engine/control_plane.rs`). A `GraphNode` obtains leases
for its assigned cells, renews them in the background, and reports heartbeat
state. A managed node also refreshes its local shard set as ownership changes.

The lease answers “who should own this cell right now?” It still does not by
itself protect the final data commit.

== Problem 7: expiry must become a data-path rejection

The stale-writer timeline is solved by connecting control-plane ownership to a
durable write fence.

The owner installs a record for the cell containing its node ID and lease
token. A leased write then validates that the fence still matches the active
local lease inside the transaction that changes graph data.

#figure(
  table(
    columns: (1.5fr, 0.4fr, 1.7fr, 0.4fr, 1.6fr),
    inset: 8pt,
    align: center,
    [control plane], [`→`], [lease token 8 for node-new], [`→`], [new durable fence],
    [node-old write], [`→`], [validate fence], [`→`], [reject token 7],
  ),
  caption: [A control-plane takeover becomes a data-plane write rejection.],
)

The check must happen inside the transaction, not only before it. A process
can pass an in-memory check and then lose its lease before the commit. The
transactional check closes that race.

There is another coordination layer as well: the object-store cell write lock.
It serializes processes that are both trying to perform a write transaction on
the same cell. The fence and the lock solve different problems:

- the lock prevents concurrent writers from overlapping normally;
- the fence prevents a stale writer from committing after ownership changes.

#srcblock("src/shard/lifecycle.rs:579-610, 695-735 (shape)")[```rust
ensure_write_authority(cell_id, operation)?;

// Before a leased transaction commits:
validate_write_fence_txn(&txn, cell_id, operation).await?;
```]

#why[
  A lease is a control-plane fact. A fence is the durable condition that makes
  the fact matter to the data plane. Keeping both lets the system fail closed:
  stale ownership is rejected even when the stale process can still reach the
  object store.
]

== Problem 8: ownership changes without copying the graph

If a cell moves from `node-old` to `node-new`, the durable data should not need
to be copied from one node's local disk to another. Local disk is a cache, not
the identity of the graph.

The movement is therefore a control sequence:

1. publish or compute a new placement;
2. wait for the previous ownership to expire or be released safely;
3. acquire a newer lease for the target node;
4. install the target's write fence;
5. open the same scoped cell path on the target node;
6. stop serving the cell on the old node;
7. let the target warm its caches from durable state.

#figure(
  table(
    columns: (1.4fr, 0.45fr, 1.4fr, 0.45fr, 1.4fr),
    inset: 8pt,
    align: center,
    [shared object store], [`→`], [node-new opens cell path], [`→`], [rebuilds compute state],
    [placement], [`→`], [lease token advances], [`→`], [old writer is fenced],
  ),
  caption: [Failover changes ownership and compute state, not the durable identity of the cell.],
)

The new node may be cold at first. It may need to fetch blocks, parse queries,
hydrate adjacency, or build an artifact. Those are performance costs. They are
not correctness failures, because the source needed to reconstruct them is
still in the object store.

This is why the system can have a local disk cache without making the local
disk part of the graph's identity.

== The complete ownership model

The four concepts now fit together:

#figure(
  table(
    columns: (1.2fr, 1.4fr, 1.5fr),
    inset: 8pt,
    align: (left + top, left + top, left + top),
    table.header([*Concept*], [*Question answered*], [*Current implementation*]),
    [Graph scope], [Which logical graph is this?], [`GraphScope` and `NamespacePath`],
    [Cell], [Which independently coordinated storage unit?], [`cell_id` and one cell path],
    [Placement], [Which node should serve the cell?], [`ShardPlacement`],
    [Lease], [Which node may write it now?], [`ShardLease` and control-plane metadata],
    [Fence], [Can this transaction prove that authority is current?], [Durable write fence checked by the shard],
  ),
  caption: [Each layer removes one ambiguity; no layer replaces the others.],
)

For a request targeting cell A, the reasoning is consequently:

1. Resolve the graph scope to find the correct durable hierarchy.
2. Resolve cell A to find the exact SlateDB path.
3. Consult placement to find the serving node.
4. For a write, require a live local lease and matching durable fence.
5. Execute the cell-local read or transaction.
6. If the node disappears, reopen the same path under a new compute process.

The design has a deliberate stopping point. Placement and leases do not make
an arbitrary multi-cell mutation atomic. A routed read may coordinate several
cell legs, but each cell retains its own storage and epoch boundary. A global
transaction would require another protocol—one that this kernel does not
pretend to provide.

#boxeq[
  *A scope identifies the graph, a cell identifies the ownership unit,
  placement identifies the intended node, and a lease plus fence identifies
  the writer that is allowed to act now.*
]

That chain is the practical meaning of “replaceable compute.” The replacement
does not inherit the old process's memory or local disk. It inherits a durable
address, obtains current ownership, and rebuilds everything else.

== Revision notes

Use these notes to recall how a logical graph becomes a safely owned storage
unit.

=== The identity and ownership chain

#figure(
  table(
    columns: (1fr, 1.4fr, 1.6fr),
    inset: 7pt,
    align: (left + top, left + top, left + top),
    table.header([*Concept*], [*Remember it as*], [*It does not answer*]),
    [Graph scope], [The namespace path plus graph ID], [Which node serves a cell],
    [Cell], [The independently stored and coordinated unit], [Which node owns it now],
    [Placement], [The intended cell-to-node mapping], [Whether that node's authority is still live],
    [Lease], [Time-bounded write ownership with a generation token], [Whether the transaction presented the current token],
    [Fence], [The durable proof checked by the write transaction], [How a multi-cell transaction should commit],
  ),
  caption: [Each concept answers one question; none is a synonym for another.],
)

=== The ideas to remember

- *The durable address mirrors the isolation hierarchy.* A namespace and graph
  ID form the graph scope; appending a validated cell ID selects one SlateDB
  database.
- *A cell is not automatically a tenant.* The embedding service chooses how
  tenants or workloads map to cells. The kernel enforces the cell boundary it
  receives.
- *Opening a path is not write authority.* A shard may be read-only, a
  standalone writer, or a leased writer. Object-store reachability alone never
  proves ownership.
- *Placement and authority are separate.* Placement tells a request where to
  go. The lease tells the node whether it may write now.
- *The lease token distinguishes ownership generations.* Renewal extends the
  current generation; takeover advances the token so an older owner can be
  identified.
- *The fence check belongs inside the transaction.* An in-memory lease check
  can race with expiry. Transactional validation prevents a stale owner from
  committing after takeover.
- *Failover moves authority, not graph data.* The new node opens the same
  object-store path and rebuilds disposable compute state. Cold caches affect
  latency, not correctness.

=== Failover in seven lines

1. Change or recompute placement.
2. Let the old lease expire or release it safely.
3. Give the target node a newer lease token.
4. Install the matching durable write fence.
5. Open the same scoped cell path on the target.
6. Stop the old node from serving the cell.
7. Rebuild caches and other compute-local state from durable storage.

The ordering matters: opening the data early is harmless for reads, but a
writer must not act until its current authority is established and fenced.

=== Common confusions

- *Scope is identity; cell is partitioning.*
- *Placement is routing; lease is temporary authority.*
- *Lease is a control-plane fact; fence is its data-plane enforcement.*
- *A write lock serializes contenders; a fence rejects an obsolete owner.*
- *Cell-local safety does not imply multi-cell atomicity or a global epoch.*

#boxeq[
  *A replacement node needs the same durable address and newer authority—not
  the failed node's memory, disk, or caches.*
]
