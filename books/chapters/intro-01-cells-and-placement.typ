#import "../vendor/bookly/src/bookly.typ": *

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
