#import "../book/vendor/bookly/src/bookly.typ": *
#import "../book/template.typ": term, why, srcblock, figcap, accent, muted

= Reads Name the World They See

Imagine a query that begins when cell A is at epoch 100. It reads the outgoing
neighbors of vertex 1. Before it asks for their properties, a writer commits
epoch 101 and deletes one of those neighbors.

If the second lookup simply asks for the newest value, the query combines two
different versions of the graph. It may return a row that never existed at any
single moment.

Chapter 3 showed how a writer publishes one complete change at one new epoch.
The read side must now keep one epoch in view from its first storage access to
its last result row.

== Problem 1: “latest” can move during a read

A query often performs many physical reads. It may scan adjacency, hydrate
metadata, test a predicate, expand another hop, and fetch a later page. If each
operation independently resolves “latest,” concurrent writes can move the
meaning between those steps.

turbolay fixes the version before doing the work.

#term("Read epoch")[
  The cell-local epoch that bounds a read. Records and changes newer than this
  epoch are invisible to that read, even if they commit before the read
  finishes.
]

#term("Graph snapshot")[
  A handle containing a cell ID and one read epoch. Its graph operations pass
  that same epoch to the shard, preserving one cell-local view across several
  calls.
]

`GraphSnapshot` makes the pairing explicit:

#srcblock("src/core/snapshot.rs:5-31 (abridged)")[```rust
pub struct GraphSnapshot<'a> {
    pub(crate) shard: &'a GraphShard,
    pub(crate) cell_id: String,
    pub(crate) read_epoch: GraphEpoch,
}

impl<'a> GraphSnapshot<'a> {
    pub fn read_epoch(&self) -> GraphEpoch { self.read_epoch }

    pub async fn out_neighbors(
        &self,
        edge_type: &str,
        src: VertexId,
    ) -> Result<Vec<VertexId>> {
        self.shard
            .out_neighbors_at(&self.cell_id, edge_type, src, self.read_epoch)
            .await
    }
}
```]

`snapshot` pins the current cell epoch. `snapshot_at` accepts an explicit
historical epoch but rejects a value newer than the cell's current state
(`src/shard/lifecycle.rs`). A requested future epoch is not a promise to wait;
it is an invalid snapshot.

#boxeq[
  *A coherent read asks every graph question of the same cell at the same
  epoch.*
]

This guarantee is cell-local. A request reading cell A at epoch 100 and cell B
at epoch 80 carries two explicit versions, not one global database epoch.

== Problem 2: replaying the whole history is correct but impractical

An epoch tells us which world to reconstruct. A naive implementation could
start from an empty graph and replay every mutation through that epoch. The
answer would be well defined, but its cost would grow with the entire age of
the cell.

turbolay builds durable artifacts that summarize an earlier state.

#term("Matrix artifact")[
  A durable, derived representation of all edges of one type in a cell at a
  particular `base_epoch`. Its published manifest identifies the artifact as
  a complete base that readers may hydrate.
]

#term("Delta overlay")[
  The ordered plus and minus changes after an artifact's base epoch and through
  the requested read epoch. Applying the overlay advances the older base to
  the exact requested snapshot.
]

The read equation is:

#boxeq[
  *answer at read_epoch = newest valid artifact at base_epoch + ordered deltas
  after base_epoch through read_epoch*
]

#figure(
  table(
    columns: (1.1fr, 0.4fr, 1.25fr, 0.4fr, 1.1fr),
    inset: 8pt,
    align: center,
    [artifact at 100], [`+`], [deltas 101 through 107], [`=`], [snapshot at 107],
  ),
  caption: [A lagging base is correct when the missing interval is explicit and ordered.],
)

`latest_matrix_artifact` does not select the newest artifact unconditionally.
It selects the newest artifact whose `base_epoch` is no greater than the
requested `read_epoch`:

#srcblock("src/engine/artifact_build.rs:962-1002 (abridged)")[```rust
pub async fn latest_matrix_artifact(
    &self,
    cell_id: &str,
    edge_type: &str,
    read_epoch: GraphEpoch,
) -> Result<Option<MatrixArtifact>> {
    if let Some(cached) = self.matrix_artifact_cache.lock().await.get_latest_by(
        |key, _| key.cell_id == cell_id
            && key.edge_type == edge_type
            && key.base_epoch <= read_epoch,
        |key, _| key.base_epoch,
    ) {
        return Ok(Some(cached));
    }
    // Scan published manifests and choose the newest eligible base.
    // ...
}
```]

An artifact from epoch 110 cannot answer a read at 107 by “subtracting” later
changes unless the engine has a proven reverse history. turbolay instead
chooses a base at or before the requested epoch and moves forward.

== Problem 3: a derived artifact must be published coherently

Building a matrix is not one small write. The builder scans graph state,
partitions rows into tiles, may produce a persisted CSC representation, and
writes a manifest. A process can fail after writing some pieces.

Readers therefore discover artifacts through their manifest, not by treating
any tile-shaped object as complete. The builder publishes the pieces and then
the record that names the coherent base. Partial or abandoned pieces are not a
second graph definition.

#term("Artifact manifest")[
  The durable publication record for a derived artifact. It names the cell,
  edge type, base epoch, and artifact metadata needed by a reader. The manifest
  is the discoverable proof that the build reached its publication point.
]

This creates a safe maintenance contract:

#figure(
  table(
    columns: (1.4fr, 1.55fr),
    inset: 8pt,
    align: (left + top, left + top),
    table.header([*Builder state*], [*Reader behavior*]),
    [No eligible manifest], [Use an older artifact or reconstruct from deltas],
    [Tiles exist but publication did not finish], [Ignore the incomplete build],
    [Manifest published at epoch N], [May hydrate it for reads at epoch N or later],
    [Newer artifact published], [Older snapshots may still select the older eligible base],
  ),
  caption: [Publication, not mere object existence, makes an artifact readable.],
)

Artifact construction requires write authority and participates in the cell's
maintenance limits (`src/engine/artifact_build.rs`). It improves later reads,
but it is not allowed to revise canonical edge meaning.

#why[
  A derived structure becomes safe when the reader has a binary discovery
  rule: either a complete manifest exists or it does not. Making readers infer
  completeness from a collection of remote objects would turn every process
  crash into an ambiguous graph version.
]

== Problem 4: plus and minus must be applied in epoch order

Suppose an edge is added at epoch 101, removed at 103, and added again at 106.
Looking only for the presence of any plus record or any minus record cannot
answer a read at epoch 105 or 107.

The overlay must preserve ordering and enforce both ends of its interval.

#srcblock("src/engine.rs:1642-1665 (abridged)")[```rust
pub(crate) fn apply_delta_overlay(
    adjacency: &mut BTreeMap<VertexId, BTreeSet<VertexId>>,
    deltas: Vec<DeltaRecord>,
    base_epoch: GraphEpoch,
    read_epoch: GraphEpoch,
) -> u64 {
    let mut applied = 0;
    for delta in deltas {
        if delta.edge.epoch <= base_epoch || delta.edge.epoch > read_epoch {
            continue;
        }
        applied += 1;
        match delta.kind {
            DeltaKind::Plus => {
                adjacency.entry(delta.edge.src).or_default().insert(delta.edge.dst);
            }
            DeltaKind::Minus => {
                if let Some(row) = adjacency.get_mut(&delta.edge.src) {
                    row.remove(&delta.edge.dst);
                }
            }
        }
    }
    applied
}
```]

The interval is open at the base because the artifact already includes state
through `base_epoch`. It is closed at the read epoch because changes committed
at that version belong to the requested snapshot.

The same visibility rule appears in the general `edges_at` path. It hydrates
the eligible base into an edge map, then inserts plus deltas and removes minus
deltas through the read epoch (`src/shard/query.rs`).

#figure(
  table(
    columns: (0.8fr, 1fr, 1.2fr),
    inset: 8pt,
    align: (center, center, left + top),
    table.header([*Epoch*], [*Change to 1 → 2*], [*State after replay*]),
    [100], [artifact contains no edge], [absent],
    [101], [plus], [present],
    [103], [minus], [absent],
    [106], [plus], [present],
  ),
  caption: [Deltas are operations in a versioned sequence, not unordered evidence.],
)

== Problem 5: a current read and a historical read have different costs

At the tip of the graph, the engine may use canonical keys, segments,
supernode materializations, or a recent artifact plus a short overlay. A
historical read has fewer freedoms: every chosen record must be valid at its
older epoch.

The engine exposes specialized reads such as `edge_exists_at`,
`out_neighbors_at`, `out_degree_at`, matrix traversal, and supernode pages.
Their implementations can choose the cheapest eligible representation, but
the representation cannot change the snapshot contract.

This separation is important:

- *logical plan*: find neighbors or test an edge at epoch N;
- *physical plan*: point key, posting chunk, supernode group, matrix artifact,
  or scan plus overlay;
- *correctness rule*: every physical input must describe state no newer than N,
  with all required changes through N applied.

An accelerator may be absent, cold, or too new. Those conditions change the
amount of work, not the answer.

== Problem 6: garbage collection can erase a permitted snapshot

Artifacts and deltas accumulate. Keeping every version forever makes storage
and scans grow without bound. Deleting old history too aggressively makes an
active snapshot impossible to reconstruct.

The safe deletion question is not simply “how old is this delta?” It is:

#boxeq[
  *After deletion, does every still-permitted read epoch retain at least one
  complete base and the changes needed to reach it?*
]

turbolay combines retention policy, rollup state, GC watermarks, and active
read leases.

#term("Read lease")[
  A durable, time-bounded record that protects an active read epoch from
  maintenance which would otherwise remove required history. It is separate
  from the write lease: it protects reconstruction, not write ownership.
]

#term("Delta GC watermark")[
  The epoch through which delta history has been compacted into a verified
  replacement base and may be removed subject to retention and active-reader
  checks.
]

Pinning an epoch publishes the read lease before returning the snapshot:

#srcblock("src/shard/query.rs:6728-6753 (abridged)")[```rust
pub(crate) async fn pin_current_read_epoch(
    &self,
    cell_id: &str,
    operation: &'static str,
) -> Result<GraphEpoch> {
    self.ensure_cell_readable(cell_id, operation).await?;
    let read_epoch = self.read_counter(&keys::last_epoch(cell_id)).await?;
    self.publish_read_lease(cell_id, read_epoch).await?;
    self.ensure_cell_readable(cell_id, operation).await?;
    Ok(read_epoch)
}
```]

The readable checks around lease publication close a drop race: a snapshot
must not be handed out for a cell that moved into its drop lifecycle during
pinning.

Delta GC refuses to compact past its calculated safe epoch. It also requires a
matrix artifact at exactly the proposed compaction epoch before advancing the
watermark (`src/shard/maintenance.rs`). Deleting the log before proving the
replacement base would destroy the only reconstruction path.

Read leases expire so abandoned clients cannot retain history forever. The
retention configuration controls the lease TTL, the minimum retained epochs,
and bounds on lease scanning. This is a policy boundary: historical reads are
supported within retained history, not for every epoch that has ever existed.

== Problem 7: pagination and caches must keep the same snapshot

A page cursor is another long read. If page one uses epoch 100 and page two
silently switches to 101, rows can be duplicated, skipped, or reordered by a
concurrent mutation. The service therefore pins the epoch before paging and
carries it in the query context.

Content-dependent caches follow the same rule. Their keys include a
`read_epoch` or an artifact `base_epoch`. A cached reachability result for
epoch 100 cannot satisfy a lookup for epoch 101 because it is stored under a
different key.

The parsed-query cache is the deliberate exception. Parsing depends on query
text, not graph contents, so its key does not need a graph epoch.

#why[
  Versioned cache keys turn invalidation into lookup identity. A write creates
  a new epoch; a read at that epoch constructs a different key and misses old
  results automatically. Old snapshot entries can remain cached without being
  mistaken for current truth.
]

== The complete read model

A cell-local read now has a precise lifecycle:

1. Resolve the target graph scope and cell.
2. Pin the current epoch or validate an explicit historical epoch.
3. Publish a read lease when retention protection is enabled.
4. Choose the newest complete artifact whose base is not newer than the read.
5. Hydrate the base or fall back to canonical reconstruction.
6. Apply ordered plus and minus deltas after the base through the read epoch.
7. Carry the epoch through traversal, metadata access, caching, and paging.
8. Let retention and GC reclaim history only behind a safe replacement base.

#figure(
  table(
    columns: (1.1fr, 1.4fr, 1.5fr),
    inset: 8pt,
    align: (left + top, left + top, left + top),
    table.header([*Layer*], [*Version it names*], [*Correctness condition*]),
    [Snapshot], [`read_epoch`], [Every logical lookup keeps the same epoch],
    [Artifact], [`base_epoch`], [Complete, published, and not newer than the read],
    [Delta overlay], [`base_epoch < epoch <= read_epoch`], [Ordered changes close the exact gap],
    [Read lease], [Oldest protected active epoch], [GC cannot erase required reconstruction history],
    [Cache entry], [`read_epoch` or `base_epoch`], [A different snapshot cannot match the key],
  ),
  caption: [Read coherence is one epoch carried through every representation.],
)

The central intuition is:

#boxeq[
  *Artifacts make old state cheap to start from; deltas make it exact; the read
  epoch makes every participating layer agree on which world is being built.*
]

== What the read path guarantees—and what it does not

The design guarantees:

- one snapshot handle carries one explicit cell-local read epoch;
- an artifact newer than the requested epoch is not selected;
- ordered overlay reconstructs changes after the base through the read epoch;
- partial unpublished artifacts are not treated as complete bases;
- active read leases constrain delta and artifact garbage collection;
- epoch-dependent cache entries cannot satisfy a different snapshot key.

It does not guarantee:

- one global epoch across cells;
- unlimited historical retention;
- that every historical read has the same latency as a current read;
- that an artifact is always available or warm;
- that a caller may request an epoch newer than the current cell and wait for it;
- that a distributed plan automatically negotiates a global snapshot.

== Revision notes

=== The ideas to remember

- *Pin before reading.* Resolving “latest” separately for each key does not
  produce a snapshot.
- *Start behind and move forward.* Choose the newest artifact at or before the
  read epoch, then apply the exact missing delta interval.
- *Publication proves completeness.* Tiles without a manifest are not a valid
  graph base.
- *Overlay order matters.* Plus and minus records describe a sequence of graph
  states.
- *Read leases protect reconstruction.* They prevent maintenance from deleting
  history still needed by an active snapshot.
- *Retention is explicit.* The engine supports historical reads inside a
  configured window, not an eternal archive by implication.
- *The epoch reaches caches and cursors.* Paging and acceleration must preserve
  the same version as storage reads.

=== A quick correctness test

1. Is the read epoch fixed before the first graph-dependent operation?
2. Can any chosen artifact be newer than the requested snapshot?
3. Are deltas after `base_epoch` through `read_epoch` applied in order?
4. Can a partial build be discovered without its publication manifest?
5. Can GC delete the only base-plus-delta route for an active read lease?
6. Does every content-dependent cache or cursor carry its version?
7. Does any claim accidentally turn cell-local epochs into a global clock?

#boxeq[
  *A snapshot is not one stored object. It is a proof that every object used to
  answer the query belongs to one named version of the cell.*
]
