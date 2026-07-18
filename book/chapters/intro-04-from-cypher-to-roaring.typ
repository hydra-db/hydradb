#import "../vendor/bookly/src/bookly.typ": *

= From Cypher to Roaring Rows

A Cypher query begins as text. The storage layer understands keys, epochs,
edges, indexes, and artifacts. The interesting work is the bridge between
them: choosing an access path without changing the query's meaning.

== The local execution pipeline

With the `opencypher` feature enabled, a cell-local query moves through:

#figure(
  table(
    columns: (1fr, 0.25fr, 1fr, 0.25fr, 1fr),
    align: center,
    inset: 7pt,
    [Cypher parser], [`→`], [row plan + optimizer], [`→`], [GraphShard executor],
    [bindings and predicates], [`→`], [physical access path], [`→`], [rows/page/count],
  ),
  caption: [Parsing describes the question; optimization chooses how the cell answers it.],
) <tab-query-pipeline>

The executor can use point reads, label or property indexes, neighbor expansion,
edge-exists checks, posting artifacts, matrix artifacts, and specialized
streaming paths. Variable-length reachability crosses the sparse-kernel seam.

== Why Roaring belongs at the seam

Before this branch, hydrated matrix adjacency was
`BTreeMap<VertexId, BTreeSet<VertexId>>`. Each destination was a separate tree
node with allocator and pointer overhead. Graph rows are sorted sets of `u64`,
which is precisely the shape `RoaringTreemap` compresses.

The current type is:

```rust
type MatrixAdjacency = BTreeMap<VertexId, RoaringTreemap>;
```

The outer map still finds a source row. The row itself is now a compressed,
deduplicated, ordered bitmap. Iteration still yields ascending vertex IDs, so
CSC construction, deterministic result ordering, and the Rust kernel retain
their behavior.

#boxeq[
  *Keep the durable artifact stable; compress the hydrated row where compute
  pays the memory bill.*
]

== Rust sparse and GraphBLAS are two backends

The default build uses the Rust sparse kernel. With the `graphblas` feature,
the same adjacency or persisted CSC can compile into a SuiteSparse GraphBLAS
matrix. GraphBLAS is an optional traversal backend, not the durable graph model
and not a requirement for ordinary graph operations.

Roaring does not guarantee every query becomes faster. Tiny sparse rows can
have less to gain, and the current Rust BFS still uses `BTreeSet` frontiers.
The immediate claim is narrower: cached matrix rows are compressed and provide
a better set-algebra substrate. Quantified latency and RSS claims require the
benchmark chapter that has not yet been written.

One cell can now answer one query. The final architecture question is how many
cells participate in one request.
