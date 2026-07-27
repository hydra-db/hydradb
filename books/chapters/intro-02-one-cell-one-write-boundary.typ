#import "../vendor/bookly/src/bookly.typ": *
#import "@preview/fletcher:0.5.7" as fletcher: diagram, node, edge

= One Cell, One Write Boundary

“Insert edge 1 → 2” sounds like one fact. Physically it fans out into several
records: the canonical outbound edge, the reverse-direction record, the degree
counters on both endpoints, whatever metadata and property indexes the mutation
carries, a dirty marker telling the indexer this edge type moved, and an
idempotency result. If half land, the graph can contradict itself.

TurboLay therefore makes the cell-local transaction the visibility boundary.

#boxeq[
  *One logical mutation becomes one durable SlateDB transaction committing at a
  single storage sequence.*
]

== The guards before the transaction

The public `write_edge` path (`src/shard/write.rs`) crosses several gates before
it reaches durable state. None of them is a lock in the object store:

#figure(
  table(
    columns: (0.35fr, 1fr, 1.8fr),
    inset: 6pt,
    table.header([*Order*], [*Guard*], [*Failure prevented*]),
    [1], [write authority (`ensure_write_authority`)], [a read-only process mutating the cell],
    [2], [write permit (`acquire_graph_write_permit`)], [unbounded concurrent mutation work],
    [3], [per-cell writer lane], [same-process races on one cell],
    [4], [manifest fence refresh (`refresh_writer_fence`)], [a superseded writer committing after a newer one took over],
    [5], [serializable snapshot transaction], [partial fan-out and concurrent conflicting writes],
    [6], [drop marker + authority re-check (`validate_write_fence_txn`)], [writes into a cell that is being dropped],
  ),
  caption: [The write path narrows authority before it changes durable state; the
  cross-process guarantee at step 4 is SlateDB's own manifest fencing, not a
  lock record.],
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
      guard((0, 0), 62mm, [write authority])
      guard((0, 1), 58mm, [write permit])
      guard((0, 2), 54mm, [per-cell writer lane])
      guard((0, 3), 50mm, [manifest fence refresh])
      guard((0, 4), 46mm, [serializable transaction])
      guard((0, 5), 42mm, [drop marker check])
      edge((0, 0), (0, 1), "->", stroke: reader-colors.muted)
      edge((0, 1), (0, 2), "->", stroke: reader-colors.muted)
      edge((0, 2), (0, 3), "->", stroke: reader-colors.muted)
      edge((0, 3), (0, 4), "->", stroke: reader-colors.muted)
      edge((0, 4), (0, 5), "->", stroke: reader-colors.muted)
      edge((0, 5), (0, 6), "->", stroke: reader-colors.muted)
      node(
        (0, 6), text(size: 8pt, fill: reader-colors.text, [durable commit \@ sequence]),
        fill: reader-colors.ok_soft, stroke: reader-colors.ok,
        shape: fletcher.shapes.rect, corner-radius: 3pt, width: 46mm,
      )
      node(
        (2, 4.5), text(size: 8pt, fill: reader-colors.text, [insert edge 1#sym.arrow.r 2]),
        fill: reader-colors.info_soft, stroke: reader-colors.info,
        shape: fletcher.shapes.rect, corner-radius: 3pt, width: 30mm,
      )
      let rec(pos, body) = node(
        pos, text(size: 8pt, fill: reader-colors.text, body),
        fill: reader-colors.info_soft, stroke: reader-colors.info,
        shape: fletcher.shapes.rect, corner-radius: 3pt, width: 30mm,
      )
      rec((4, 2), [canonical edge])
      rec((4, 3), [reverse edge])
      rec((4, 4), [degree counters])
      rec((4, 5), [metadata index])
      rec((4, 6), [dirty marker])
      rec((4, 7), [idempotency])
      edge((2, 4.5), (4, 2), "->", stroke: reader-colors.muted)
      edge((2, 4.5), (4, 3), "->", stroke: reader-colors.muted)
      edge((2, 4.5), (4, 4), "->", stroke: reader-colors.muted)
      edge((2, 4.5), (4, 5), "->", stroke: reader-colors.muted)
      edge((2, 4.5), (4, 6), "->", stroke: reader-colors.muted)
      edge((2, 4.5), (4, 7), "->", stroke: reader-colors.muted)
      node((4, 1), text(size: 8pt, fill: reader-colors.muted, [one txn, one sequence]), stroke: none)
    },
  ),
  caption: [Each guard narrows authority — and the cross-process guard is a
  manifest fence refresh, not a lock — while one logical mutation fans out into
  many records committed together at a single storage sequence; the dirty marker
  is the only thing the write owes the traversal index.],
) <fig-intro02-funnel>

Read the left column downward: each guard is narrower than the one above it, so
authority is progressively reduced until a single serializable transaction is all
that remains. The right column is what that one transaction actually writes —
one logical mutation fanning out into many records, all published together at
the sequence the commit lands on. There is no delta row, no outbox row and no
mutation log; the fan-out ends where the transaction ends. Traversal
acceleration is decoupled instead of queued: the write marks the edge type dirty
(`keys::matrix_dirty`), an out-of-process indexer rebuilds an immutable index
generation from that hint, and readers close the remaining lag with the WAL-tail
overlay (`topology_tail_since`, `src/shard/topology_tail.rs`). The write itself
never waits for any of that.

The layers overlap deliberately, and each one answers a different question. The
writer lane is cheap and only orders writers inside this process. Cross-process
safety comes from SlateDB itself: before every attempt the shard refreshes the
writer's manifest (`refresh_writer_fence`, `src/core/state.rs`), and if a newer
writer has taken the cell the refresh comes back fenced, the cached writer
handle is dropped, and the write fails rather than committing behind the new
owner's back. SlateDB then provides atomicity and conflict detection for the
records themselves, and a conflicting transaction is simply retried — the loop
in `write_edge` re-runs `write_edge_txn` on a retryable conflict.

== Epoch and idempotency are part of the write

Inside the serializable transaction the writer checks the idempotency key first
and returns the recorded result unchanged if this is a replay. Only then does it
read the epoch — and it does not allocate one. It takes the transaction's own
sequence number, `let current_epoch = txn.seqnum();`
(`write_edge_txn_locked_with_metadata`, `src/shard/write.rs`), inspects existing
edge state at that sequence, stamps the new records with the next sequence, and
builds the full batch. The idempotency record binds the caller's key to the
original mutation and result. Replaying the same request returns the same
logical outcome; reusing its key for another edge is rejected.

#custom-box(title: [Term — Epoch], icon: "info")[
  An epoch is a SlateDB storage sequence number, nothing more. It is not a
  counter TurboLay maintains in a key of its own: there is no epoch key to read
  and no epoch to allocate, so a write cannot lose a race for one.
]

The durable commit publishes the records and the sequence that names their
visibility together. A caller never observes “edge written, version missing.”

== The boundary stops at the cell

`RoutedGraphCluster::write_edge` (`src/engine/cluster.rs`) routes only to a cell
this node already opened from the object-store node directory; a cell it did not
open is refused with `UnknownShard`. The node must also be promotable — a
read-only node is refused with `WriteRequiresWriter` — and `ensure_local_writer`
then promotes it to the cell's SlateDB writer lazily, on the first write that
needs one. There is no owner map to consult and no write forwarded over the
query TCP transport, so remote routing stays with the embedding service.

#info-box(title: [Built boundary])[Writes are sharded per cell and fenced by
SlateDB's manifest. A single atomic transaction spanning two cells is not built.]

The write has now landed at a storage sequence. The read side uses that sequence
to decide which world it is allowed to see.
