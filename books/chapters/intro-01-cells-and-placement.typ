#import "../vendor/bookly/src/bookly.typ": *
#import "@preview/fletcher:0.5.7" as fletcher: diagram, node, edge

= A Graph Has Cells

One giant graph path gives every writer the same lock and every query the same
failure domain. TurboLay instead gives the graph an addressable unit small
enough to move: a *cell*.

#boxeq[
  *A cell is the unit of storage path, placement, epoch, and write authority.*
]

== The path is part of the boundary

`GraphCluster` opens a cell at `base_path/cell_id`
(`src/engine/cluster.rs`). That path is a separate SlateDB database over the
shared object store. A routed cluster holds only the shards assigned to its
local node.

#figure(
  table(
    columns: (1.1fr, 1fr, 1fr),
    inset: 7pt,
    table.header([*Object-store path*], [*Placed owner*], [*Local role*]),
    [`graph/reddit-home`], [`node-a`], [sole writer + reader],
    [`graph/reddit-ads`], [`node-b`], [sole writer + reader],
    [`graph/archive`], [`node-a`], [sole writer + reader],
  ),
  caption: [Placement maps cells to compute nodes; paths keep their SlateDB state separate.],
) <tab-cell-placement>

The term matters. The current APIs consistently carry `cell_id`; they do not
implement a general tenant or namespace abstraction above it. An embedding
service may decide that one tenant owns one cell, several cells, or a whole
base path. The kernel does not make that policy choice.

== Placement says where; a lock says who may write

`ShardPlacement` maps each cell to one node (`src/engine/cluster.rs`).
Assignments may be `fixed` or built with `rendezvous` hashing, and the mapping
is decided at startup. There is no control-plane database and no lease store —
placement is static configuration, not a coordinated, renewing grant.

#figure(
  diagram(
    spacing: (13mm, 14mm),
    node-stroke: 0.5pt,
    node-corner-radius: 3pt,
    // compute nodes (top row)
    node((0.5, 0), text(size: 8pt)[`node-a`], fill: reader-colors.surface_soft, stroke: reader-colors.border),
    node((2.5, 0), text(size: 8pt)[`node-b`], fill: reader-colors.surface_soft, stroke: reader-colors.border),
    // durable object-store band enclosing the three cells
    node(
      enclose: ((0, 2), (1.5, 2), (3, 2)),
      inset: 12pt,
      fill: none,
      stroke: (paint: reader-colors.purple, dash: "dotted"),
      snap: -1,
    ),
    node((-0.55, 1.45), text(size: 7pt, fill: reader-colors.muted)[durable object store], stroke: none, fill: none),
    // cells (bottom row) — separate SlateDB paths
    node((0, 2), text(size: 8pt)[`graph/reddit-home`], fill: reader-colors.purple_soft, stroke: reader-colors.purple),
    node((1.5, 2), text(size: 8pt)[`graph/reddit-ads`], fill: reader-colors.purple_soft, stroke: reader-colors.purple),
    node((3, 2), text(size: 8pt)[`graph/archive`], fill: reader-colors.purple_soft, stroke: reader-colors.purple),
    // placement edges: home -> a, archive -> a, ads -> b
    edge((0, 2), (0.5, 0), "->", stroke: reader-colors.muted, label: text(size: 7pt, fill: reader-colors.muted)[ShardPlacement], label-pos: 0.6),
    edge((3, 2), (0.5, 0), "->", stroke: reader-colors.muted),
    edge((1.5, 2), (2.5, 0), "->", stroke: reader-colors.muted, label: text(size: 7pt, fill: reader-colors.muted)[ShardPlacement], label-pos: 0.6),
    // owner badge
    node((3.55, 0.9), text(size: 7pt)[sole writer +\ cell write lock], fill: reader-colors.ok_soft, stroke: reader-colors.ok),
  ),
  caption: [Placement (fixed or rendezvous-hashed) maps each cell to one owning node; paths keep each cell's SlateDB state separate.],
) <fig-intro01-placement>

Read the map bottom-up. Each cell is its own SlateDB path on the shared object
store; placement simply assigns each path to one owning node. The green badge marks
where that node holds the two things that make it the writer — the sole SlateDB
writer handle and the cell write lock.

When a node starts, it:

1. opens a `RoutedGraphCluster`;
2. selects the cells its node ID owns under the placement;
3. opens a `GraphShard` at each owned cell path.

Placement alone cannot stop an old process, and TurboLay does not use a lease
generation to fence it out. Write ownership rests on two mechanisms instead:
SlateDB's sole-writer handle (only one process may hold the database's writer),
and the object-store *cell write lock* — an owner-token record with a TTL
(`acquire_cell_write_lock` / `release_cell_write_lock`). A writer whose token
is stale is rejected before it can mutate the cell.

== What cell isolation buys

- Different nodes can own and compute over different cells.
- A stale owner is rejected by the cell write lock before it can mutate its
  former cell.
- A query coordinator can route each leg to the cell's owner.
- Maintenance, artifacts, snapshots, and GC can progress per cell.

What it does *not* buy is a transaction across cells. No two-phase commit or
cross-cell constraint protocol exists. That limitation becomes clear when we
walk one write in the next chapter.
