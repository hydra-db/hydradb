#import "../vendor/bookly/src/bookly.typ": *

= A Graph Has Cells

One giant graph path gives every writer the same lock and every query the same
failure domain. turbolay instead gives the graph an addressable unit small
enough to move: a *cell*.

#boxeq[
  *A cell is the unit of storage path, placement, lease, epoch, and write
  authority.*
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
    [`graph/reddit-home`], [`node-a`], [leased writer + reader],
    [`graph/reddit-ads`], [`node-b`], [leased writer + reader],
    [`graph/archive`], [`node-a`], [leased writer + reader],
  ),
  caption: [Placement maps cells to compute nodes; paths keep their SlateDB state separate.],
) <tab-cell-placement>

The term matters. The current APIs consistently carry `cell_id`; they do not
implement a general tenant or namespace abstraction above it. An embedding
service may decide that one tenant owns one cell, several cells, or a whole
base path. The kernel does not make that policy choice.

== Placement says where; a lease says who may write

`ShardPlacement` maps each cell to one node. Assignments may be fixed or built
with rendezvous hashing. `GraphControlPlane` persists placement and leases in
its own SlateDB database (`src/engine/control_plane.rs`).

When `GraphNode` starts, it:

1. loads placement;
2. selects the cells owned by its node ID;
3. acquires one lease per owned cell;
4. opens a leased `GraphShard` at each cell path;
5. installs a data-plane write fence;
6. renews the leases in the background.

Placement alone cannot stop an old process. The lease token is monotonically
advanced, and the installed fence binds data writes to that generation.

== What cell isolation buys

- Different nodes can own and compute over different cells.
- A stale owner is rejected before it can mutate its former cell.
- A query coordinator can route each leg to the cell's owner.
- Maintenance, artifacts, snapshots, and GC can progress per cell.

What it does *not* buy is a transaction across cells. No two-phase commit or
cross-cell constraint protocol exists. That limitation becomes clear when we
walk one write in the next chapter.
