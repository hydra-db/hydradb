#import "../vendor/bookly/src/bookly.typ": *

= Roaring Matrix Rows

#info-box(title: [Implementation scope])[This chapter documents the Roaring
change on `feat/2026-07-13`. It changes compute-local hydrated adjacency, not
the durable object-store format.]

== The type boundary

The old aliases in `src/lib.rs` and `src/sparse_kernel.rs` represented an
adjacency as:

```rust
BTreeMap<VertexId, BTreeSet<VertexId>>
```

They now use:

```rust
BTreeMap<VertexId, RoaringTreemap>
```

`VertexId` is `u64`, so `RoaringTreemap` is used rather than the 32-bit
`RoaringBitmap`. The new tests deliberately include IDs above `u32::MAX` to
pin that requirement.

== What stays byte-for-byte compatible

The durable types have not changed:

#figure(
  table(
    columns: (1.3fr, 1fr, 1fr),
    inset: 6pt,
    table.header([*Layer*], [*Representation*], [*Changed?*]),
    [canonical edge/index keys], [SlateDB records], [no],
    [matrix tile rows], [`BTreeMap<u64, Vec<u64>>`], [no],
    [persisted CSC], [vertex/pointer/index chunks], [no],
    [posting and supernode chunks], [`Vec<u64>`], [no],
    [hydrated matrix row], [`RoaringTreemap`], [*yes*],
    [Rust/GraphBLAS adjacency input], [Roaring rows], [*yes*],
  ),
  caption: [Roaring is introduced after hydration, preserving stored artifacts.],
) <tab-roaring-boundary>

No migration is required. A process can open artifacts written by the previous
commit, decode their vector rows, and extend a Roaring row during hydration in
`src/engine/supernode.rs`.

== Iterator changes are correctness changes

`BTreeSet::iter()` yields `&u64`; a `RoaringTreemap` iterator yields `u64`.
Porting therefore required removing reference dereferences in:

- CSC construction;
- sparse expansion;
- graph vertex-dictionary construction;
- reconstruction of `EdgeRecord`s from hydrated adjacency.

Delta removal also changed from `row.remove(&dst)` to `row.remove(dst)`.
Those small type changes are exactly where a silent high-ID truncation or
ordering regression could hide, so the tests cover sorted deduplication,
values above `u32::MAX`, Rust traversal, and plus/minus overlay.

== What Roaring buys now

- one compressed row rather than one tree allocation per destination;
- natural deduplication and ascending iteration;
- efficient future union, intersection, and difference operations;
- a lower-memory cache candidate for dense and high-degree rows.

The outer `BTreeMap` remains because matrix hydration and source-row lookup are
still organized by source vertex. A later measurement may justify a different
outer structure, but that is independent of row compression.

== What it does not buy automatically

The current Rust traversal uses `BTreeSet` for its frontier and accumulated
result. It iterates each Roaring row into that frontier. Therefore the change
does not by itself prove lower query latency. Tiny rows may see little benefit,
and cache limits count entries rather than bytes.

#boxeq[
  *The implemented win is a compressed adjacency cache. The performance win
  remains a benchmark result, not a premise.*
]
