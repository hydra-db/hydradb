#import "../vendor/bookly/src/bookly.typ": *

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
