#import "../vendor/bookly/src/bookly.typ": *

= From Cypher to Roaring Rows

A Cypher query begins as text. The storage layer understands keys, epochs,
edges, indexes, and artifacts. The interesting work is the bridge between
them: choosing an access path without changing the query's meaning.

#info-box(title: [Status: planned])[Roaring matrix rows are a proposed
optimization, *not yet in the current tree*. On this branch the hydrated
adjacency is still `BTreeMap<VertexId, BTreeSet<VertexId>>` (`src/lib.rs`), there
is no `Roaring` symbol in `src/`, and `roaring` is not a Cargo dependency. The
Cypher-to-access-path pipeline below is real and shipped; the Roaring payoff
is future work. Read every Roaring claim here as a design intent, not behavior
you can observe today.]

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
edge-exists checks, matrix artifacts, and specialized streaming paths.
Variable-length reachability crosses the sparse-kernel seam.

== Why Roaring would belong at the seam

Today, hydrated matrix adjacency is
`BTreeMap<VertexId, BTreeSet<VertexId>>`. Each destination is a separate tree
node with allocator and pointer overhead. Graph rows are sorted sets of `u64`,
which is precisely the shape `RoaringTreemap` compresses.

The current type is:

```rust
type MatrixAdjacency = BTreeMap<VertexId, BTreeSet<VertexId>>;
```

The planned change swaps the inner row for a compressed bitmap at the same
hydration seam:

```rust
// planned
type MatrixAdjacency = BTreeMap<VertexId, RoaringTreemap>;
```

The outer map would still find a source row. The row itself would become a
compressed, deduplicated, ordered bitmap. Iteration would still yield ascending
vertex IDs, so CSC construction, deterministic result ordering, and the Rust
kernel would retain their behavior.

#boxeq[
  *Keep the durable artifact stable; compress the hydrated row where compute
  pays the memory bill.*
]

== Rust sparse and GraphBLAS are two backends

The default build uses the Rust sparse kernel. With the `graphblas` feature,
the same adjacency or persisted CSC can compile into a SuiteSparse GraphBLAS
matrix. GraphBLAS is an optional traversal backend, not the durable graph model
and not a requirement for ordinary graph operations.

Roaring would not guarantee every query becomes faster. Tiny sparse rows have
less to gain, and the current Rust BFS still uses `BTreeSet` frontiers. The
narrower claim is that cached matrix rows would be compressed and provide a
better set-algebra substrate. Quantified latency and RSS claims require the
benchmark chapter that has not yet been written.

One cell can now answer one query. The final architecture question is how many
cells participate in one request.
