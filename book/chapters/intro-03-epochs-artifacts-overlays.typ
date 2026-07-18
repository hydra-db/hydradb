#import "../vendor/bookly/src/bookly.typ": *

= Reads Name the World They See

A matrix artifact takes time to build. While it is being built, edges keep
arriving. If a query blindly uses the newest matrix it can find, it may answer
from yesterday's structure and silently omit today's edge.

turbolay makes both sides explicit: the query names a `read_epoch`; every
artifact names a `base_epoch`.

#boxeq[
  *answer at read_epoch = artifact at base_epoch + ordered deltas through
  read_epoch*
]

== Canonical truth and derived accelerators

Canonical edge and metadata records are the source of graph meaning. Builders
derive several durable query structures:

- matrix tiles for sparse traversal;
- compact CSC chunks for GraphBLAS hydration;
- posting chunks for bounded neighbor access;
- supernode groups and chunk indexes for very high-degree vertices;
- manifests that publish a coherent artifact epoch.

These are durable accelerators, not competing truths. An unpublished or partial
artifact is ignored. A published manifest identifies the complete base.

#info-box(title: [Known review finding])[The current-epoch supernode-group
builder scans live edge keys but omits segment-backed membership. The failing
segment-supernode parity tests document the resulting truncated groups. Matrix
artifact construction already scans both sources; the supernode builder still
needs the same row source. See
`docs/impl/2026-07-08-graphblas-branch-bug-report-and-proposed-fixes.md`, Bug 1.]

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

The Roaring change in this branch lives after hydration: each matrix row becomes
a compressed `RoaringTreemap`. It does not alter matrix-tile or CSC bytes in the
object store, so old artifacts remain readable.

== Snapshots make long reads coherent

`snapshot_at` pins a read epoch. Historical reads, pagination, artifact lookup,
and delta application use that version rather than drifting to the newest
epoch midway through the query. Read leases prevent GC from deleting history
still needed by an active snapshot.

#note[
  Epoch consistency is cell-local. A distributed query can give each cell leg
  an explicit epoch, but the coordinator does not negotiate one global epoch
  across unrelated cells.
]

== Maintenance has a proof obligation

Rollup may replace an older base with a newer one. Delta GC may delete history
only after a safe rollup watermark and retention/read-lease checks. Artifact GC
keeps versions needed by snapshots. The invariant is not “never delete”; it is
“never delete the only route to a permitted read epoch.”

Once a coherent adjacency is available, the query engine still has to turn a
question into traversal work. That is the next layer.
