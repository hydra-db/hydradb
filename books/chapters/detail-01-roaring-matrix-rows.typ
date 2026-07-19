#import "../vendor/bookly/src/bookly.typ": *

= Roaring Matrix Rows

#info-box(title: [Status: PLANNED])[Roaring matrix rows are a *proposed*
optimization. They are *not* in the current tree: adjacency hydrates today as
`BTreeMap<VertexId, BTreeSet<VertexId>>`. This chapter describes the intended
change. When adopted it would touch only compute-local hydrated adjacency, not
the durable object-store format.]

== The type boundary

The current aliases in `src/lib.rs` and `src/sparse_kernel.rs` represent an
adjacency as:

```rust
BTreeMap<VertexId, BTreeSet<VertexId>>
```

Under the planned change they would instead use:

```rust
BTreeMap<VertexId, RoaringTreemap>
```

Design note: `VertexId` is `u64`, so the plan calls for `RoaringTreemap` rather
than the 32-bit `RoaringBitmap`. Tests for the change would deliberately include
IDs above `u32::MAX` to pin that requirement.

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
    [hydrated matrix row], [`BTreeSet<VertexId>` today; `RoaringTreemap` planned],
      [*planned*],
    [Rust/GraphBLAS adjacency input], [Roaring rows (planned)], [*planned*],
  ),
  caption: [Roaring would be introduced after hydration, preserving stored artifacts.],
) <tab-roaring-boundary>

No migration would be required. A process can already open artifacts written by
a previous commit and decode their vector rows; under the plan it would extend a
Roaring row during hydration. Hydration lives in `src/engine/matrix_cache.rs`
(`load_matrix_adjacency`) — the old `src/engine/supernode.rs` has been deleted —
so the port would touch `matrix_cache.rs`.

== What the port will involve

`BTreeSet::iter()` yields `&u64`; a `RoaringTreemap` iterator yields `u64`.
Porting will therefore require removing reference dereferences in:

- CSC construction;
- sparse expansion;
- graph vertex-dictionary construction;
- reconstruction of `EdgeRecord`s from hydrated adjacency.

Delta removal will also change from `row.remove(&dst)` to `row.remove(dst)`.
Those small type changes are exactly where a silent high-ID truncation or
ordering regression could hide, so the tests for the change should cover sorted
deduplication, values above `u32::MAX`, Rust traversal, and plus/minus overlay.

== What Roaring would buy

- one compressed row rather than one tree allocation per destination;
- natural deduplication and ascending iteration;
- efficient future union, intersection, and difference operations;
- a lower-memory cache candidate for dense and high-degree rows.

The outer `BTreeMap` would remain because matrix hydration and source-row lookup
are still organized by source vertex. A later measurement may justify a different
outer structure, but that is independent of row compression.

== What it would not buy automatically

The current Rust traversal uses `BTreeSet` for its frontier and accumulated
result. It would iterate each Roaring row into that frontier. Therefore the
change would not by itself prove lower query latency. Tiny rows may see little
benefit, and cache limits count entries rather than bytes.

#boxeq[
  *The proposed win is a compressed adjacency cache. The performance win would
  remain a benchmark result, not a premise.*
]
