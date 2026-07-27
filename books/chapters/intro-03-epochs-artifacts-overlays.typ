#import "../vendor/bookly/src/bookly.typ": *
#import "@preview/fletcher:0.5.7" as fletcher: diagram, node, edge

= Reads Name the World They See

A matrix artifact takes time to build. While it is being built, edges keep
arriving. If a query blindly uses the newest matrix it can find, it may answer
from yesterday's structure and silently omit today's edge.

TurboLay makes both sides explicit: the query names a `read_epoch`; every index
generation names a `base_sequence`. Both are values of the *same* type, and there
is only one such type in the engine — `StorageSequence` (`src/lib.rs`), which is
SlateDB's sequence number for a committed storage snapshot.

That "only one" is worth pausing on, because an earlier design had two clocks: a
storage sequence for record visibility and a separate topology counter for
acceleration. Two clocks mean a seam, and a seam means a way for a read to be
coherent on one axis and stale on the other. The engine now reads its epoch
straight out of the snapshot it already pinned, and binds it as both bounds of the
same value (`src/shard/query.rs`):

```rust
let context = context.with_validated_storage_read_epoch(read_epoch, read_epoch);
```

The same argument twice is the whole point: there is nothing left to disagree.

#boxeq[
  *answer at read_epoch = index generation at base_sequence + the WAL tail
  through read_epoch*
]

== Canonical truth and derived accelerators

Canonical edge and metadata records are the source of graph meaning. Builders
derive several durable query structures:

- matrix tiles for sparse traversal;
- compact CSC chunks for GraphBLAS hydration;
- manifests that publish a coherent artifact epoch.

These are durable accelerators, not competing truths. An unpublished or partial
artifact is ignored. A published manifest identifies the complete base.

== Overlay closes the time gap

`latest_matrix_artifact` (`src/engine/artifact_build.rs`) selects the newest
artifact whose base is not newer than the requested read epoch. For traversal, the
shard then closes the remaining interval with `topology_tail_since`
(`src/shard/topology_tail.rs`) — but not by replaying a log TurboLay wrote. There
is no such log. It reads SlateDB's own *write-ahead log*, walking the WAL files
from the generation's `last_wal_id` up to the newest durable one and collecting
the edges that changed.

#custom-box(title: [Term — WAL-tail overlay], icon: "info")[
  The gap between what an index generation knows and what the read must see, read
  directly out of SlateDB's write-ahead log rather than from any TurboLay-maintained
  delta record. Because the WAL is already there for durability, the overlay costs no
  extra write on the write path — the accelerator is reconciled by *reading* the same
  bytes durability already required.
]

#figure(
  table(
    columns: (1fr, 0.45fr, 1fr, 0.45fr, 1fr),
    align: center,
    inset: 7pt,
    [generation at 100], [`+`], [WAL tail 101…107], [`=`], [answer at 107],
  ),
  caption: [The index generation is allowed to lag because the exact missing interval is
    read back out of the write-ahead log.],
) <tab-artifact-overlay>

#figure(
  diagram(
    spacing: (4mm, 9mm),
    node-stroke: 0.5pt,
    crossing-fill: reader-colors.paper,
    // async refresh job — dashed edge over the base artifact
    edge((0, -1), (5, -1), "->", stroke: (paint: reader-colors.warn, dash: "dashed"), label: text(size: 8pt, fill: reader-colors.warn)[out-of-process indexer rebuilds the base], label-side: center),
    // base index generation (wide, durable)
    node(enclose: ((0, 0), (1, 0)), [index generation — `base_sequence 100`], fill: reader-colors.purple_soft, stroke: reader-colors.purple, corner-radius: 3pt, inset: 8pt),
    // WAL tail entries
    node((2, 0), text(size: 8pt)[WAL 101], fill: reader-colors.info_soft, stroke: reader-colors.info, corner-radius: 2pt, inset: 4pt),
    node((3, 0), text(size: 8pt)[WAL 102], fill: reader-colors.info_soft, stroke: reader-colors.info, corner-radius: 2pt, inset: 4pt),
    node((4, 0), text(size: 8pt)[…], fill: reader-colors.info_soft, stroke: reader-colors.info, corner-radius: 2pt, inset: 4pt),
    node((5, 0), text(size: 8pt)[WAL 107], fill: reader-colors.info_soft, stroke: reader-colors.info, corner-radius: 2pt, inset: 4pt),
    // read epoch marker
    node((6, 0), text(size: 8pt)[`read_epoch 107`], fill: reader-colors.warn_soft, stroke: reader-colors.primary_active, corner-radius: 2pt, inset: 5pt),
    // epoch axis
    edge((-0.3, 0.9), (6.4, 0.9), "->", stroke: reader-colors.muted, label: text(size: 8pt, fill: reader-colors.muted)[epoch], label-side: center),
    // summary bracket node
    node((3.0, 1.8), text(size: 8pt)[`generation(100) + WAL tail (100..107] = answer @ 107`], fill: reader-colors.ok_soft, stroke: reader-colors.ok, corner-radius: 3pt, inset: 7pt),
  ),
  caption: [The index generation is allowed to lag because the exact missing interval is read
    back out of SlateDB's write-ahead log; a separate out-of-process indexer rebuilds the base
    asynchronously.],
) <fig-intro03-overlay>

Read it left to right along the epoch axis. The durable generation only knows the
world up to its base sequence (100), so a read at epoch 107 walks the WAL files
covering `(100, 107]` and folds the edges it finds there on top. The dashed arrow is
the out-of-process indexer that will later publish a fresher generation — which is
why the base is allowed to lag.

There is an honest limit here, and the code states it rather than hiding it.
`topology_tail_since` returns `Unavailable` in two cases: when the pinned snapshot
has moved out from under the read, and when the WAL files it needs are already gone.
In either case the query does not fail and does not answer from a stale base — it
falls back to reading adjacency straight from the snapshot, trading the accelerator's
speed for the canonical path's certainty. The accelerator is always optional; the
canonical records are always sufficient.

#info-box(title: [Status: planned, not in the current tree])[The Roaring
compression described next is future work. It is not on this branch: the
current hydrated adjacency row is a `BTreeSet`
(`MatrixAdjacency = BTreeMap<VertexId, BTreeSet<VertexId>>`), there is no
`Roaring` type in `src/`, and `roaring` is not yet a dependency.]

A planned Roaring change would live after hydration: each matrix row would become
a compressed `RoaringTreemap` in place of today's `BTreeSet`. Because it would
touch only the in-memory hydrated row, it would not alter matrix-tile or CSC
bytes in the object store, so old artifacts would remain readable.

== Snapshots make long reads coherent

`snapshot_at` (`src/shard/lifecycle.rs`) pins a read epoch. Pagination, index
lookup and the WAL-tail overlay all use that version rather than drifting to the
newest epoch midway through the query. Coherence for an in-flight query comes from
the SlateDB `DbSnapshot` it holds for the duration of the result — every key it
reads is served from that one revision.

What `snapshot_at` will *not* do is travel backwards. An epoch ahead of the cell's
current one is rejected as `SnapshotAhead`; an epoch merely *behind* it is rejected
too, with the plain explanation that "historical graph epochs are not SlateDB
snapshots". Only the current sequence is openable.

#custom-box(title: [Why], icon: "tip")[
  Refusing historical reads looks like a lost feature, and it is — deliberately. A
  SlateDB snapshot is a live handle on a storage revision, not an archive you can
  reconstruct on request. Pretending otherwise would mean either keeping every
  superseded version forever or quietly serving an approximation, and the second is
  the kind of silent wrongness this chapter exists to rule out. A read that wants to
  be causally after a specific write asks for it with a *bookmark* instead, which
  says "wait until you are at least this current" rather than "go back in time".
]

There are no read leases, and garbage collection does not consult active readers.
It trusts the caller's epoch and prunes by `retain`
(`delete_graph_artifacts_before`, `src/engine/artifact_gc.rs`). There is no
watermark record to check against.

#let seq-tag(body) = text(size: 7.5pt, fill: reader-colors.muted, weight: "bold", body)
#figure(
  diagram(
    spacing: (7mm, 9mm),
    node-stroke: 0.6pt,
    crossing-fill: reader-colors.paper,
    node-inset: 7pt,
    node-corner-radius: 3pt,
    // The one clock — SlateDB defines it
    node((0, 0), seq-tag[SlateDB\ defines it], stroke: none, fill: none),
    node((2, 0), align(center)[
      #text(size: 9pt, weight: "bold")[`db.snapshot()` → StorageSequence *S*] \
      #text(size: 8pt)[one consistent view of *every* key · MVCC record visibility]
    ], fill: reader-colors.info_soft, stroke: reader-colors.info, width: 88mm),
    // the binding — the same value is used as both bounds
    edge((2, 0), (2, 1), "->", stroke: reader-colors.primary_active + 0.8pt, label-side: right,
      label: text(size: 7.5pt, fill: reader-colors.primary_active)[take `read_epoch` = `snapshot.seq()`]),
    node((2, 1), align(center)[
      #text(size: 8.5pt)[`with_validated_storage_read_epoch(`*S*`, `*S*`)`] \
      #text(size: 7.5pt, fill: reader-colors.muted)[the same value twice — there is no second cursor to disagree]
    ], fill: reader-colors.info_soft, stroke: reader-colors.info, width: 88mm),
    // The acceleration axis — TurboLay consumes the same S
    node((0, 3), seq-tag[TurboLay\ consumes it], stroke: none, fill: none),
    node((1, 3), align(center)[#text(size: 8pt)[index generation] \ #text(size: 8pt, weight: "bold")[`base_sequence` *B*]],
      fill: reader-colors.purple_soft, stroke: reader-colors.purple),
    node((2, 3), align(center)[#text(size: 8pt)[WAL tail] \ #text(size: 8pt, weight: "bold")[(*B* … *S*\]]],
      fill: reader-colors.info_soft, stroke: reader-colors.info),
    node((3, 3), align(center)[#text(size: 8pt)[read view] \ #text(size: 8pt, weight: "bold")[@ *S*]],
      fill: reader-colors.ok_soft, stroke: reader-colors.ok),
    edge((1, 3), (2, 3), "->", stroke: reader-colors.muted, label-fill: none, label-side: center,
      label: text(size: 7.5pt, fill: reader-colors.muted)[base lags]),
    edge((2, 3), (3, 3), "->", stroke: reader-colors.muted, label-fill: none, label-side: center,
      label: text(size: 7.5pt, fill: reader-colors.muted)[fold the tail on]),
    // the SAME S from the top anchors the acceleration axis
    edge((2, 1), (3, 3), "-->", stroke: (paint: reader-colors.primary_active, dash: "dotted"), bend: -20deg, label-fill: none, label-side: left,
      label: text(size: 7pt, fill: reader-colors.primary_active)[same #emph[S]]),
  ),
  caption: [One sequence, two consumers: a read pins a SlateDB snapshot, takes its sequence
    *S* as the read epoch, and the very same *S* is what the index generation's lagging base
    is measured against — so there is no second clock that could drift out of step with
    record visibility.],
) <fig-intro03-two-sequences>

How to read it: the top row is SlateDB's job — opening a `DbSnapshot` fixes what every key
looks like, which is the only thing that makes a long read coherent. The bottom row is
TurboLay's job — the index generation may lag at `base_sequence B`, and the missing interval
up to `S` is read out of the WAL. The dotted line is the punchline, and it is the part worth
remembering: the bottom row is measured against the *same* `S` the top row pinned. An earlier
design gave acceleration its own counter, which meant a read could be coherent about records
and stale about structure at the same moment. Collapsing to one sequence removes that failure
mode by construction rather than by discipline.

#note[
  Epoch consistency is cell-local. A distributed query can give each cell leg
  an explicit epoch, but the coordinator does not negotiate one global epoch
  across unrelated cells.
]

== Maintenance has a proof obligation

A newer base may replace an older one when `build_adjacency_image`
(`src/engine/artifact_build.rs`) publishes a fresher artifact. That rebuild does
not run on the write path, and it does not run in the data node at all: a separate
`graph-indexer` process (`src/bin/graph-indexer.rs`) scans the "dirty" edge-type
markers a write leaves behind and publishes a fresh index generation when one is
due. That is precisely why the base is allowed to lag `read_epoch` — the missing
interval is read from the WAL at read time.

The write path's entire contribution to this is one marker. A write does not build
an index, does not schedule a build, and does not wait for one; it records that an
edge type has moved and commits. Everything else happens in another process, on its
own clock, and can fail without the write noticing.

Artifact GC keeps versions still reachable by a permitted read epoch. The invariant
is not “never delete”; it is “never delete the only route to a permitted read
epoch.” Because the canonical records are always sufficient on their own, the worst
outcome of getting that wrong is a slower read, not a wrong one.

Once a coherent adjacency is available, the query engine still has to turn a
question into traversal work. That is the next layer.
