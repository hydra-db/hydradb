#import "../vendor/bookly/src/bookly.typ": *

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
