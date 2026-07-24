#import "../vendor/bookly/src/bookly.typ": *
#import "@preview/fletcher:0.5.7" as fletcher: diagram, node, edge

= The `write_edge` Path

The public entry point is `GraphShard::write_edge` in `src/shard/write.rs`.
Its input is an `EdgeMutation`: cell, edge type, source, destination, and an
idempotency key. Its output is a `CommitResult` containing the commit epoch and
whether the edge already existed.

== Phase 1: reject the wrong writer cheaply

Before opening a transaction, `write_edge` validates the cell, edge type and
idempotency key, then calls `ensure_write_authority` (`src/shard/lifecycle.rs`).
That method matches on a three-variant `GraphWriteAuthority` (`src/core/state.rs`):
a `ReadOnly` shard is refused outright with `WriteRequiresWriter`, while
`Promotable` and `Writer` both fall through to `self.db.writer()`, which fails
unless this process holds the database's writer handle. The routed cluster makes
the same decision one level up: `RoutedGraphCluster::write_edge`
(`src/engine/cluster.rs`) calls `ensure_local_writer`, which refuses a
non-promotable node and otherwise promotes this shard to writer lazily. There is
no owner map to consult, no lease, and no lease expiry.

Next it acquires a graph-write permit from the shard's admission gate
(`acquire_graph_write_permit`) and locks the cell's hashed writer lane
(`self.writer_lane(&mutation.cell_id).lock().await`). Both are process-local
controls: the permit is backpressure, and the lane is cheap serialization so that
two tasks inside *this* process do not contend on the same cell. Neither is
visible to any other process, and neither protects anything across the fleet.

== Phase 2: refresh the SlateDB manifest fence

Nothing on this path takes a lock. What makes the write safe against a second
process is SlateDB's own manifest fencing, and `write_edge_txn` reaches it
through `acquire_local_write_guard` (`src/shard/lifecycle.rs`), which does two
things: it takes the shard's process-local write guard, and it calls
`self.db.refresh_writer_fence()`.

`refresh_writer_fence` (`src/core/state.rs`) asks SlateDB to refresh the
manifest. If another process has since opened this database as a writer, the
refresh fails with `ErrorKind::Closed(CloseReason::Fenced)`; the shard drops its
cached writer handle and propagates the error, so the superseded process never
reaches a commit. That is the entire cross-process story. There is no owner
token, no TTL, no lock object in the object store, and nothing to reclaim.

#custom-box(title: [Term — Manifest fencing], icon: "info")[
  SlateDB records the current writer in the database manifest. Opening a new
  writer claims a newer writer epoch, and the previous writer learns it has been
  superseded the next time it refreshes the manifest — it is closed rather than
  allowed to commit. The guarantee is a negative one: a fenced writer cannot
  write. It needs no coordination service and no separate lock record.
]

The guard `acquire_local_write_guard` returns is process-local, and
`finish_local_write` (`src/core/state.rs`) merely drops it and returns the
transaction's result unchanged — its `release` is a no-op.

== Phase 3: transact against one snapshot

The fenced writer begins a `SerializableSnapshot` transaction.
`validate_write_fence_txn` (`src/shard/lifecycle.rs`) is a drop-guard plus an
authority re-check, not a token fence: it reads the cell's drop and drop-pending
markers and rejects a dropped or drop-pending cell with `CellDropped`, then calls
`ensure_write_authority` again. It reads no durable fence and no lease token;
there is no such record.

The transaction then performs the logical work:

#figure(
  table(
    columns: (0.35fr, 1.7fr, 1.4fr),
    inset: 6pt,
    table.header([*Step*], [*Read or decision*], [*Write produced*]),
    [1], [idempotency lookup], [return prior result, or reserve the key],
    [2], [`txn.seqnum()`], [nothing — the commit epoch is that sequence plus one],
    [3], [canonical and segment edge state], [insert vs already existed],
    [4], [vertex and edge metadata merge], [metadata rows and property indexes, only if changed],
    [5], [this edge type is now stale], [`matrix_dirty` and `adjacency_generation` markers],
    [6], [existing degree counters], [forward/reverse edge rows and degree counters],
    [7], [request fingerprint], [idempotency result],
  ),
  caption: [One logical edge mutation fans out inside one serializable transaction.],
) <tab-write-fanout>

Two rows deserve a note. The epoch in step 2 is not allocated and not read from a
key: the transaction reports its own snapshot sequence with `txn.seqnum()`, and
the commit epoch is that number plus one — the same sequence `commit_txn_strict`
hands to SlateDB (`src/codec.rs`). And step 5 is the only thing the write does on
behalf of traversal. There is no delta row, no outbox row and no mutation-log
entry; `mark_adjacency_dirty_txn` (`src/shard/write.rs`) writes two small markers
that say this edge type's traversal index is stale, and stops there.

#custom-box(title: [Why], icon: "tip")[
  Rebuilding a traversal index inside the write transaction would put a
  whole-edge-type cost on the latency of a single edge. Marking the edge type
  dirty costs one key. The out-of-process indexer (`src/bin/graph-indexer.rs`)
  picks the marker up and rebuilds an immutable CSC index generation, and a
  reader whose snapshot has moved past that generation closes the remaining gap
  itself with the WAL-tail overlay (`topology_tail_since`,
  `src/shard/topology_tail.rs`) — falling back to snapshot adjacency when the
  tail is unavailable.
]

#figure(
  diagram(
    spacing: (5mm, 7mm),
    node-stroke: 0.5pt + reader-colors.border,
    crossing-fill: reader-colors.paper,
    node-corner-radius: 3pt,
    // Phase flow, left to right
    node((0, 0), text(size: 8pt, fill: reader-colors.text, hyphenate: false)[*Phase 1*\ refuse the wrong\ writer\ authority · permit\ · writer lane],
      fill: reader-colors.surface_soft, width: 32mm),
    edge((0, 0), (1, 0), "->", stroke: reader-colors.muted),
    node((1, 0), text(size: 8pt, fill: reader-colors.text, hyphenate: false)[*Phase 2*\ refresh SlateDB\ manifest fence],
      fill: reader-colors.info_soft, width: 28mm),
    edge((1, 0), (2, 0), "->", stroke: reader-colors.muted),
    node((2, 0), text(size: 8pt, fill: reader-colors.text, hyphenate: false)[*Phase 3 — serializable txn*\ drop-guard · idempotency\ seqnum · edge state\ metadata · dirty marker\ degree · idem result],
      fill: reader-colors.info_soft, stroke: reader-colors.info, width: 46mm, height: 28mm),
    edge((2, 0), (3, 0), "->", stroke: reader-colors.muted),
    node((3, 0), text(size: 8pt, fill: reader-colors.text, hyphenate: false)[*Phase 4*\ retry or\ report],
      fill: reader-colors.surface_soft, width: 24mm),
    // a retryable conflict restarts from the manifest refresh
    edge((3, 0), (3, -0.95), (1, -0.95), (1, 0), "->",
      text(size: 7.5pt, fill: reader-colors.muted)[retryable conflict],
      stroke: (dash: "dashed", paint: reader-colors.muted)),
    // commit divider between before / after states
    edge((2.55, -0.4), (2.55, 1.35), stroke: reader-colors.ok + 1.4pt,
      label: text(size: 7.5pt, fill: reader-colors.ok)[commit], label-side: left),
    node((1.2, 0.95), text(size: 7.5pt, fill: reader-colors.text)[before: nothing visible],
      fill: reader-colors.ok_soft, stroke: none, width: 34mm),
    node((3.05, 0.95), text(size: 7.5pt, fill: reader-colors.text, hyphenate: false)[after: edge, epoch, indexes,\ degree, dirty marker, idem\ visible together],
      fill: reader-colors.ok_soft, stroke: none, width: 38mm),
  ),
  caption: [No lock is taken and none is released: phase 2 only refreshes SlateDB's
    manifest fence, phases 2 and 3 re-run from that refresh when the transaction
    hits a retryable conflict, and the green divider marks the single moment at
    which every record the mutation touched becomes visible at once.],
) <fig-detail02-phases>

`commit_txn_strict` (`src/codec.rs`) commits with an explicit sequence and the
shard's `await_durable_writes` setting. A shard opened with any write authority
refuses to open at all when that setting is false (`open_internal`,
`src/shard/lifecycle.rs`), so an acknowledged write has always reached remote
durability.

== Phase 4: retry or report

Phases 2 and 3 run inside a retry loop in `write_edge`, bounded by
`GRAPH_TXN_MAX_RETRIES` (32, `src/lib.rs`). `is_retryable_write_conflict`
(`src/core/state.rs`) admits exactly one shape of failure: a SlateDB error whose
kind is `ErrorKind::Transaction` or `ErrorKind::Invalid`. Such an attempt
increments the write-retry counter, yields the task, and starts again from the
manifest refresh. Any other outcome is returned as-is, and exhausting the loop
yields `RetryExhausted`. A success increments the write-commit counter and
returns the new epoch.

Nothing is released on the way out: the write permit, the writer lane and the
local write guard drop with their scopes, and there was never a durable lock to
clean up, so no cleanup failure can mask the original error.

== Idempotency is semantic, not merely transport-level

The idempotency key is bound to the mutation fingerprint. Replaying the same
edge request returns the recorded result. Reusing the key for a different edge is
corruption from the API's perspective: `ensure_idempotent_edge` (`src/codec.rs`)
compares the stored cell, edge type, source and destination against the request
and returns `IdempotencyConflict` when they differ.

An already-existing edge can legitimately return without incrementing degree,
but the idempotency record still distinguishes a cached acknowledgement from a
fresh logical decision.

== Failure boundaries

#boxeq[
  *Before commit: no logical mutation is visible. After commit: the edge rows,
  the commit epoch, the indexes, the degree counters, the dirty marker, and the
  idempotency record become visible together.*
]

This guarantee is cell-local. A caller that needs two cells changed together
must provide a higher-level protocol; the kernel intentionally exposes no
cross-cell atomic write.
