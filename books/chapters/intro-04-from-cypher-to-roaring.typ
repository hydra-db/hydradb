#import "../vendor/bookly/src/bookly.typ": *
#import "@preview/fletcher:0.5.7" as fletcher: diagram, node, edge

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

#figure(
  diagram(
    spacing: (9mm, 12mm),
    node-stroke: 0.5pt,
    {
      // pipeline (left to right)
      let pipe = (
        ((0, 0), [Cypher text]),
        ((1, 0), [parse\ (`ParsedCypher`)]),
        ((2, 0), [row plan +\ optimizer]),
        ((3, 0), [GraphShard\ executor]),
        ((4, 0), [rows / page /\ count]),
      )
      for (pos, body) in pipe {
        node(pos, text(size: 8pt)[#body], fill: reader-colors.surface_soft,
          stroke: reader-colors.border, shape: fletcher.shapes.rect,
          corner-radius: 3pt, width: 2.4cm)
      }
      edge((0, 0), (1, 0), "->", stroke: reader-colors.muted)
      edge((1, 0), (2, 0), "->", stroke: reader-colors.muted)
      edge((2, 0), (3, 0), "->", stroke: reader-colors.muted)
      edge((3, 0), (4, 0), "->", stroke: reader-colors.muted)

      // access-path fan-out below the executor
      let access = (
        ((0.5, 1), [point reads]),
        ((1.5, 1), [label/property\ index]),
        ((2.5, 1), [neighbor\ expansion]),
        ((3.5, 1), [matrix\ artifacts]),
      )
      for (pos, body) in access {
        node(pos, text(size: 8pt)[#body], fill: reader-colors.info_soft,
          stroke: reader-colors.info, shape: fletcher.shapes.rect,
          corner-radius: 3pt, width: 2.3cm)
        edge((3, 0), pos, "->", stroke: reader-colors.muted)
      }

      // sparse-kernel seam and its two backends
      node((2, 2), text(size: 8pt)[sparse kernel], fill: reader-colors.surface_soft,
        stroke: reader-colors.border, shape: fletcher.shapes.rect,
        corner-radius: 3pt, width: 2.4cm)
      edge((2.5, 1), (2, 2), "->", stroke: reader-colors.muted)
      edge((3.5, 1), (2, 2), "->", stroke: reader-colors.muted)

      node((1, 3), text(size: 8pt)[Rust sparse\ (default)], fill: reader-colors.surface_soft,
        stroke: reader-colors.border, shape: fletcher.shapes.rect,
        corner-radius: 3pt, width: 2.4cm)
      node((3, 3), text(size: 8pt)[GraphBLAS\ (optional)], fill: reader-colors.surface_soft,
        stroke: reader-colors.border, shape: fletcher.shapes.rect,
        corner-radius: 3pt, width: 2.4cm)
      edge((2, 2), (1, 3), "->", stroke: reader-colors.muted)
      edge((2, 2), (3, 3), "->", stroke: reader-colors.muted)

      // PLANNED node at the hydration seam
      node((4.5, 2), text(size: 8pt)[Roaring rows\ (planned)], fill: reader-colors.warn_soft,
        stroke: (paint: reader-colors.warn, dash: "dashed"),
        shape: fletcher.shapes.rect, corner-radius: 3pt, width: 2.4cm)
      edge((3.5, 1), (4.5, 2), "-->", stroke: (paint: reader-colors.warn, dash: "dashed"))
    },
  ),
  caption: [Parsing describes the question; the optimizer chooses a physical
    access path. Roaring row compression is a planned optimization at the
    hydration seam.],
) <fig-intro04-pipeline>

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
