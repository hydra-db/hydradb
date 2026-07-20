#import "../vendor/bookly/src/bookly.typ": *
#import "@preview/fletcher:0.5.7" as fletcher: diagram, node, edge

= Reads Name the World They See

A matrix artifact takes time to build. While it is being built, edges keep
arriving. If a query blindly uses the newest matrix it can find, it may answer
from yesterday's structure and silently omit today's edge.

TurboLay makes both sides explicit: the query names a `read_epoch`; every
artifact names a `base_epoch`. These epochs are cursors on the
`TopologySequence` — the monotonic topology-change counter that names artifact
and delta versions. It is not the mechanism that owns read visibility: that
belongs to the `StorageSequence`, the SlateDB snapshot sequence a query pins for
the duration of its result. The two travel together, but only the SlateDB
snapshot makes a read coherent.

#boxeq[
  *answer at read_epoch = artifact at base_epoch + ordered deltas through
  read_epoch*
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

`latest_matrix_artifact` selects the newest artifact whose base is not newer
than the requested read epoch. The shard hydrates it, then
`apply_delta_overlay` applies plus and minus records in the interval
`(base_epoch, read_epoch]`.

#figure(
  table(
    columns: (1fr, 0.45fr, 1fr, 0.45fr, 1fr),
    align: center,
    inset: 7pt,
    [matrix at 100], [`+`], [deltas 101…107], [`=`], [answer at 107],
  ),
  caption: [The artifact is allowed to lag because the exact missing interval is replayed.],
) <tab-artifact-overlay>

#figure(
  diagram(
    spacing: (4mm, 9mm),
    node-stroke: 0.5pt,
    crossing-fill: reader-colors.paper,
    // async refresh job — dashed edge over the base artifact
    edge((0, -1), (5, -1), "->", stroke: (paint: reader-colors.warn, dash: "dashed"), label: text(size: 8pt, fill: reader-colors.warn)[async refresh job rebuilds the base], label-side: center),
    // base matrix artifact (wide, durable)
    node(enclose: ((0, 0), (1, 0)), [matrix artifact — `base_epoch 100`], fill: reader-colors.purple_soft, stroke: reader-colors.purple, corner-radius: 3pt, inset: 8pt),
    // delta ticks
    node((2, 0), text(size: 8pt)[Δ101], fill: reader-colors.info_soft, stroke: reader-colors.info, corner-radius: 2pt, inset: 4pt),
    node((3, 0), text(size: 8pt)[Δ102], fill: reader-colors.info_soft, stroke: reader-colors.info, corner-radius: 2pt, inset: 4pt),
    node((4, 0), text(size: 8pt)[…], fill: reader-colors.info_soft, stroke: reader-colors.info, corner-radius: 2pt, inset: 4pt),
    node((5, 0), text(size: 8pt)[Δ107], fill: reader-colors.info_soft, stroke: reader-colors.info, corner-radius: 2pt, inset: 4pt),
    // read epoch marker
    node((6, 0), text(size: 8pt)[`read_epoch 107`], fill: reader-colors.warn_soft, stroke: reader-colors.primary_active, corner-radius: 2pt, inset: 5pt),
    // epoch axis
    edge((-0.3, 0.9), (6.4, 0.9), "->", stroke: reader-colors.muted, label: text(size: 8pt, fill: reader-colors.muted)[epoch], label-side: center),
    // summary bracket node
    node((3.0, 1.8), text(size: 8pt)[`artifact(100) + deltas(101..107] = answer @ 107`], fill: reader-colors.ok_soft, stroke: reader-colors.ok, corner-radius: 3pt, inset: 7pt),
  ),
  caption: [The artifact is allowed to lag because the exact missing interval of deltas is
    replayed on top; a background job rebuilds the base asynchronously.],
) <fig-intro03-overlay>

Read it left to right along the epoch axis. The durable artifact only knows the
world up to its base epoch (100), so a read at epoch 107 replays exactly the deltas
in `(100, 107]` on top of it. The dashed arrow is the background job that will later
fold those deltas into a fresher artifact — which is why the base is allowed to lag.

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

`snapshot_at` pins a read epoch. Historical reads, pagination, artifact lookup,
and delta application use that version rather than drifting to the newest
epoch midway through the query. Coherence for an in-flight query comes from the
SlateDB `DbSnapshot` it holds for the duration of the result — every key it
reads is served from that one revision. There are no read leases. GC no longer
consults active readers: it writes a `delta_gc_watermark` before deleting and
then trusts the caller's epoch, so a read that asks for an epoch below the
watermark is refused with `SnapshotExpired` rather than being silently served
partial history.

#let seq-tag(body) = text(size: 7.5pt, fill: reader-colors.muted, weight: "bold", body)
#figure(
  diagram(
    spacing: (7mm, 9mm),
    node-stroke: 0.6pt,
    crossing-fill: reader-colors.paper,
    node-inset: 7pt,
    node-corner-radius: 3pt,
    // Lane A — SlateDB owns record visibility
    node((0, 0), seq-tag[SlateDB\ owns this], stroke: none, fill: none),
    node((2, 0), align(center)[
      #text(size: 9pt, weight: "bold")[`db.snapshot()` → StorageSequence *S*] \
      #text(size: 8pt)[one consistent view of *every* key · MVCC record visibility]
    ], fill: reader-colors.info_soft, stroke: reader-colors.info, width: 88mm),
    // the seam — the app cursor is read from inside the SlateDB snapshot
    edge((2, 0), (2, 1), "->", stroke: reader-colors.primary_active + 0.8pt, label-side: right,
      label: text(size: 7.5pt, fill: reader-colors.primary_active)[read `meta/last_epoch` from #emph[inside] snapshot #emph[S]]),
    node((2, 1), align(center)[
      #text(size: 8.5pt)[`read_epoch` *E* = `meta/last_epoch` (TopologySequence)] \
      #text(size: 7.5pt, fill: reader-colors.muted)[bound by `with_validated_storage_read_epoch(E, S)`]
    ], fill: reader-colors.info_soft, stroke: reader-colors.info, width: 88mm),
    // Lane B — TurboLay owns the topology / acceleration axis
    node((0, 3), seq-tag[TurboLay\ owns this], stroke: none, fill: none),
    node((1, 3), align(center)[#text(size: 8pt)[`delta_gc_watermark` *W*] \ #text(size: 7pt, fill: reader-colors.muted)[reads below → `SnapshotExpired`]],
      fill: reader-colors.warn_soft, stroke: (paint: reader-colors.warn, dash: "dashed")),
    node((2, 3), align(center)[#text(size: 8pt)[matrix artifact] \ #text(size: 8pt, weight: "bold")[`base_epoch` *B*]],
      fill: reader-colors.purple_soft, stroke: reader-colors.purple),
    node((3, 3), align(center)[#text(size: 8pt)[read view] \ #text(size: 8pt, weight: "bold")[@ epoch *E*]],
      fill: reader-colors.ok_soft, stroke: reader-colors.ok),
    edge((1, 3), (2, 3), "--", stroke: reader-colors.muted),
    edge((2, 3), (3, 3), "->", stroke: reader-colors.muted, label-fill: none, label-side: center,
      label: text(size: 7.5pt, fill: reader-colors.muted)[replay deltas after #emph[B] up to #emph[E]]),
    // the SAME E from lane A anchors the topology axis
    edge((2, 1), (3, 3), "-->", stroke: (paint: reader-colors.primary_active, dash: "dotted"), bend: -20deg, label-fill: none, label-side: left,
      label: text(size: 7pt, fill: reader-colors.primary_active)[same #emph[E]]),
  ),
  caption: [The two sequences, and the seam that binds them: a read pins a SlateDB snapshot
    (record visibility), and from inside it reads the topology cursor E that the matrix
    accelerator is measured against.],
) <fig-intro03-two-sequences>

How to read it: the top lane is SlateDB's job — opening a `DbSnapshot` fixes what every key
looks like, which is the only thing that makes a long read coherent. The bottom lane is
TurboLay's job — the artifact may lag at `base_epoch B`, the missing interval up to `E` is
replayed, and reads below `delta_gc_watermark W` are refused. The dotted seam is the punchline:
`E` is read #emph[from inside] the snapshot, so the topology cursor never becomes a second
visibility clock.

#note[
  Epoch consistency is cell-local. A distributed query can give each cell leg
  an explicit epoch, but the coordinator does not negotiate one global epoch
  across unrelated cells.
]

== Maintenance has a proof obligation

A newer base may replace an older one when `build_adjacency_image`
(`src/engine/artifact_build.rs`) publishes a fresher artifact. That rebuild does
not run on the write path: the background matrix-artifact refresh job
(`src/engine/artifact_refresh.rs`) scans "dirty" edge-type markers and rebuilds
an artifact asynchronously when it is due, which is precisely why the artifact is
allowed to lag `read_epoch` — the missing interval is replayed from deltas at
read time. Delta GC may delete history only after the watermark has advanced past
it. Artifact GC keeps versions still reachable by a permitted read epoch. The
invariant is not “never delete”; it is “never delete the only route to a
permitted read epoch.”

Once a coherent adjacency is available, the query engine still has to turn a
question into traversal work. That is the next layer.
