#import "../vendor/bookly/src/bookly.typ": *
#import "@preview/fletcher:0.5.7" as fletcher: diagram, node, edge

= A Graph Has Cells

One giant graph path gives every writer the same contention point and every
query the same failure domain. TurboLay instead gives the graph an addressable
unit small enough to move: a *cell*.

#boxeq[
  *A cell is the unit of storage path, storage sequence, and write authority.*
]

== The path is part of the boundary

`GraphCluster` opens a cell at `base_path/cell_id` (`src/engine/cluster.rs`).
That path is a separate SlateDB database over the shared object store: its own
manifest, its own write-ahead log, its own compacted SSTs, and therefore its own
sequence numbers and its own writer fencing. Two cells share the bucket and
nothing else.

#figure(
  table(
    columns: (1.1fr, 1fr, 1.15fr),
    inset: 7pt,
    table.header([*Object-store path*], [*Durable state at that path*], [*Role on a node that opened it*]),
    [`graph/reddit-home`], [manifest, WAL, SSTs], [reader; writer only after promotion],
    [`graph/reddit-ads`], [manifest, WAL, SSTs], [reader; writer only after promotion],
    [`graph/archive`], [manifest, WAL, SSTs], [reader; writer only after promotion],
  ),
  caption: [Each cell is an independent SlateDB database under the shared base path, so
    a cell's log, snapshots, and fencing belong to it alone; the local role is the same
    on every node, because every node opens every cell.],
) <tab-cell-paths>

The term matters. The current APIs consistently carry `cell_id`; they do not
implement a general tenant or namespace abstraction above it. An embedding
service may decide that one tenant owns one cell, several cells, or a whole
base path. The kernel does not make that policy choice.

== Every node opens every cell; only the write is asymmetric

Where a cell lives is not a routing decision, because nothing routes. The fleet
view is a plain object-store *directory*: `ObjectStoreNodeDirectory { cells,
nodes }` (`src/engine.rs`), two sorted sets of names. It records what *exists* —
which cell ids belong to this graph, which node ids belong to the fleet — and
records nothing at all about who owns what. There is no owner map, no lease, no
revoked-cell list, and no control-plane process holding any of it.

`open_at_path` (`src/engine/cluster.rs`) turns that directory into a running
node in two steps. First it refuses to start when the local node id is absent
from the directory's node set, returning `GraphError::CorruptValue` on the key
`directory/node/<node_id>` with the reason "local node is not present in the
object-store node directory". Then it iterates every entry of `directory.cells()`
and opens a `GraphShard` at `base_path/<cell_id>` for each one. There is no owned
subset to select: `local_cells` on the resulting `RoutedGraphCluster` returns
every cell the directory lists.

The consequence is worth stating plainly. Any node can serve any read for any
cell, so a read needs no ownership lookup before it can begin.

#figure(
  diagram(
    spacing: (10mm, 7mm),
    node-stroke: 0.5pt + reader-colors.border,
    crossing-fill: reader-colors.paper,
    node-corner-radius: 3pt,
    node((0, 1), text(fill: reader-colors.text, size: 8pt)[
      `ObjectStoreNodeDirectory`\
      cells: home · ads · archive\
      nodes: a · b · c\
      _every node opens all three_
    ], fill: reader-colors.info_soft, stroke: reader-colors.info, width: 4.2cm),
    node((1, 0), text(fill: reader-colors.text, size: 8pt)[
      `node-a`\ `promotable`\ promoted writer
    ], fill: reader-colors.ok_soft, stroke: reader-colors.ok, width: 3.4cm),
    node((1, 1), text(fill: reader-colors.text, size: 8pt)[
      `node-b`\ `promotable`\ reader today
    ], fill: reader-colors.surface_soft, width: 3.4cm),
    node((1, 2), text(fill: reader-colors.text, size: 8pt)[
      `node-c`\ not promotable
    ], fill: reader-colors.surface_soft, width: 3.4cm),
    node((2, 1), text(fill: reader-colors.text, size: 8pt)[
      `graph/reddit-home`\ one SlateDB database\ manifest fencing admits\ one writer
    ], fill: reader-colors.purple_soft, stroke: reader-colors.purple, width: 4.0cm),
    edge((0, 1), (1, 0), "->", stroke: reader-colors.muted),
    edge((0, 1), (1, 1), "->", stroke: reader-colors.muted),
    edge((0, 1), (1, 2), "->", stroke: reader-colors.muted),
    edge((1, 0), (2, 1), "->", text(fill: reader-colors.muted, size: 7pt)[writes], stroke: 0.8pt + reader-colors.ok, label-side: left),
    edge((1, 1), (2, 1), "->", text(fill: reader-colors.muted, size: 7pt)[reads · may promote], stroke: reader-colors.muted),
    edge((1, 2), (2, 1), "->", text(fill: reader-colors.muted, size: 7pt)[reads only], stroke: reader-colors.muted, label-side: right),
  ),
  caption: [The directory on the left lists what exists, not who owns it, so all three nodes
    open the same cells and any of them can serve any read; only one cell of the three is drawn,
    and the other two look identical. The single asymmetry is on the right: `node-a` has promoted
    a SlateDB writer, `node-b` may promote one later, `node-c` is refused with
    `WriteRequiresWriter`, and it is SlateDB's manifest fencing — not an owner record — that
    keeps two promotions from both committing (`src/engine/cluster.rs`, `src/core/state.rs`).],
) <fig-intro01-symmetric-cells>

The write is the only place the fleet is not symmetric, and it is decided by one
boolean. A `RoutedGraphCluster` carries a single `promotable` flag (`src/engine.rs`).
`ensure_local_writer` (`src/engine/cluster.rs`) refuses immediately with
`GraphError::WriteRequiresWriter` when that flag is false; when it is true, the
shard lazily promotes a cached SlateDB writer for the addressed cell. Nothing
about that decision is per-cell, and nothing about it is written down durably.

#custom-box(title: [Term — Writer promotion], icon: "info")[
  A node does not open a SlateDB writer for a cell when it starts; it opens a
  reader. The first write routed to a promotable node promotes that cell's
  handle — `promote_to_writer` (`src/shard/lifecycle.rs`) calls
  `GraphStore::promote_writer` (`src/core/state.rs`), which opens the database
  under a gate and caches the handle. Promotion is lazy, per cell, and leaves no
  record anywhere else in the system.
]

So what stops two promoted writers from both committing? Three tiers, none of
which is a lock TurboLay holds:

1. *Authority.* `ensure_write_authority` (`src/shard/lifecycle.rs`) matches on
  `GraphWriteAuthority { ReadOnly, Promotable, Writer }` (`src/core/state.rs`).
  `ReadOnly` is refused with `WriteRequiresWriter`; the other two fall through to
  the SlateDB writer handle.
2. *SlateDB manifest fencing.* `refresh_writer_fence` (`src/core/state.rs`) calls
  `writer.refresh_manifest()`, and `acquire_local_write_guard`
  (`src/shard/lifecycle.rs`) invokes it before every local write. If SlateDB
  answers with a `Closed(Fenced)` close reason, the shard drops its cached writer
  handle and the error propagates to the caller. A newer writer fences an older
  one through SlateDB's own manifest.
3. *The serializable-snapshot transaction.* `write_edge` (`src/shard/write.rs`)
  takes no lock at all: an authority check, a write permit, a per-cell writer-lane
  mutex, then a retry loop over one serializable-snapshot transaction.

#custom-box(title: [Why], icon: "tip")[
  Because fencing lives in the storage layer, `promotable` can safely be true on
  more than one node at once. Two nodes may race to promote; at most one of them
  can go on committing, because SlateDB fences the loser at its next manifest
  refresh. That is why TurboLay needs no owner map, no lease renewal, and no
  control plane to be safe — the only thing an operator has to get right is the
  directory listing, and getting it wrong stops a node from starting rather than
  corrupting a cell.
]

== What cell isolation buys

- Every node reads every cell, so read capacity grows by adding nodes and no read
  pays for an ownership lookup.
- Each cell has its own log, its own snapshots, and its own fencing, so a writer
  that loses its fence loses it for that cell alone.
- Maintenance, index generations, snapshots, and GC all progress per cell.
- A query that spans cells is still bounded scatter/gather over cells with a
  coordinator merge — but there is no owner to resolve before the legs can run.

What it does *not* buy is a transaction across cells. No two-phase commit or
cross-cell constraint protocol exists. That limitation becomes clear when we
walk one write in the next chapter, which makes the cell-local transaction the
visibility boundary.
