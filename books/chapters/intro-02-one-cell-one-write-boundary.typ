#import "../vendor/bookly/src/bookly.typ": *
#import "@preview/fletcher:0.5.7" as fletcher: diagram, node, edge

= One Cell, One Write Boundary

“Insert edge 1 → 2” sounds like one fact. Physically it fans out into several
records: the canonical edge, reverse access, degree, metadata indexes, an epoch,
a delta or outbox record, and an idempotency result. If half land, the graph can
contradict itself.

TurboLay therefore makes the cell-local transaction the visibility boundary.

#boxeq[
  *One logical mutation becomes one durable SlateDB transaction at one new
  cell epoch.*
]

== The guards before the transaction

The public `write_edge` path in `src/shard/write.rs` crosses several gates:

#figure(
  table(
    columns: (0.35fr, 1fr, 1.8fr),
    inset: 6pt,
    table.header([*Order*], [*Guard*], [*Failure prevented*]),
    [1], [write authority], [read-only or non-owner process writes],
    [2], [backpressure permit], [unbounded concurrent mutation work],
    [3], [writer lane], [same-process cell races],
    [4], [object-store cell lock], [cross-process writers overlap],
    [5], [drop-guard + writer authority], [writes to a dropping cell, or from a read-only or non-writer process],
    [6], [serializable transaction], [partial fan-out and conflicting epochs],
  ),
  caption: [The write path narrows authority before it changes durable state.],
) <tab-write-guards>

#figure(
  diagram(
    spacing: (12mm, 7mm),
    node-stroke: 0.5pt,
    crossing-fill: reader-colors.paper,
    {
      let guard(pos, w, body) = node(
        pos, text(size: 8pt, fill: reader-colors.text, body),
        fill: reader-colors.surface_soft, stroke: reader-colors.border,
        shape: fletcher.shapes.rect, corner-radius: 3pt, width: w,
      )
      guard((0, 0), 62mm, [writer authority])
      guard((0, 1), 56mm, [backpressure permit])
      guard((0, 2), 50mm, [writer lane])
      guard((0, 3), 46mm, [cell write lock])
      guard((0, 4), 42mm, [drop-guard + authority])
      guard((0, 5), 38mm, [serializable transaction])
      edge((0, 0), (0, 1), "->", stroke: reader-colors.muted)
      edge((0, 1), (0, 2), "->", stroke: reader-colors.muted)
      edge((0, 2), (0, 3), "->", stroke: reader-colors.muted)
      edge((0, 3), (0, 4), "->", stroke: reader-colors.muted)
      edge((0, 4), (0, 5), "->", stroke: reader-colors.muted)
      edge((0, 5), (0, 6), "->", stroke: reader-colors.muted)
      node(
        (0, 6), text(size: 8pt, fill: reader-colors.text, [durable commit \@ new epoch]),
        fill: reader-colors.ok_soft, stroke: reader-colors.ok,
        shape: fletcher.shapes.rect, corner-radius: 3pt, width: 46mm,
      )
      node(
        (2, 5), text(size: 8pt, fill: reader-colors.text, [insert edge 1#sym.arrow.r 2]),
        fill: reader-colors.info_soft, stroke: reader-colors.info,
        shape: fletcher.shapes.rect, corner-radius: 3pt, width: 30mm,
      )
      let rec(pos, body) = node(
        pos, text(size: 8pt, fill: reader-colors.text, body),
        fill: reader-colors.info_soft, stroke: reader-colors.info,
        shape: fletcher.shapes.rect, corner-radius: 3pt, width: 30mm,
      )
      rec((4, 2), [canonical edge])
      rec((4, 3), [reverse])
      rec((4, 4), [degree])
      rec((4, 5), [index])
      rec((4, 6), [epoch])
      rec((4, 7), [delta / outbox])
      rec((4, 8), [idempotency])
      edge((2, 5), (4, 2), "->", stroke: reader-colors.muted)
      edge((2, 5), (4, 3), "->", stroke: reader-colors.muted)
      edge((2, 5), (4, 4), "->", stroke: reader-colors.muted)
      edge((2, 5), (4, 5), "->", stroke: reader-colors.muted)
      edge((2, 5), (4, 6), "->", stroke: reader-colors.muted)
      edge((2, 5), (4, 7), "->", stroke: reader-colors.muted)
      edge((2, 5), (4, 8), "->", stroke: reader-colors.muted)
      node((4, 1), text(size: 8pt, fill: reader-colors.muted, [one txn, one epoch]), stroke: none)
    },
  ),
  caption: [Each guard narrows authority; one logical mutation fans out into many
  records inside a single serializable transaction at one new epoch.],
) <fig-intro02-funnel>

Read the left column downward: each guard is narrower than the one above it, so
authority is progressively reduced until a single serializable transaction is all
that remains. The right column is what that one transaction actually writes —
one logical mutation fanning out into many records, all published together at one
new epoch.

The layers overlap deliberately. The local lane is cheap. The object-store cell
write lock coordinates processes. The sole SlateDB writer handle and the cell
write lock together reject a stale or zombie writer. SlateDB provides atomicity
and conflict detection for the actual records.

== Epoch and idempotency are part of the write

Inside the serializable transaction the writer reads `last_epoch`, allocates
the next epoch, checks the idempotency key, inspects existing edge state, and
builds the full batch. The idempotency record binds the caller's key to the
original mutation and result. Replaying the same request returns the same
logical outcome; reusing its key for another edge is rejected.

The durable commit publishes the edge and the version that names its visibility
together. A caller never observes “edge written, epoch missing.”

== The boundary stops at the cell

`RoutedGraphCluster::write_edge` routes only to a cell owned by the local node
per its static placement, and requires `Writer` authority — which in turn
requires this process to hold the sole SlateDB writer handle
(`src/engine/cluster.rs`). It does not forward a write over the query TCP
transport. A mutation for a remotely owned cell is rejected, leaving remote
routing to the embedding service.

#info-box(title: [Built boundary])[Writes are sharded and fenced per cell. A
single atomic transaction spanning two cells is not built.]

The write has now named a new epoch. The read side uses that name to decide
which world it is allowed to see.
