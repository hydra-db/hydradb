#import "../vendor/bookly/src/bookly.typ": *
#import "@preview/fletcher:0.5.7" as fletcher: diagram, node, edge

= The `write_edge` Path

The public entry point is `GraphShard::write_edge` in `src/shard/write.rs`.
Its input is an `EdgeMutation`: cell, edge type, source, destination, and an
idempotency key. Its output is a `CommitResult` containing the commit epoch and
whether the edge already existed.

== Phase 1: reject the wrong writer cheaply

Before opening a transaction, the method validates components and calls
`ensure_write_authority`. A read-only shard fails. A writer-authority shard must
hold the sole SlateDB writer handle: `ensure_write_authority` calls
`self.db.writer()`, which fails unless this process owns the database's only
writer. The routed cluster performs the same owner and writer-authority check
before reaching the shard. There are no leases and no lease expiry.

Next it acquires a graph-write semaphore and a hashed writer lane. These are
process-local controls: backpressure plus low-cost serialization.

== Phase 2: acquire the cross-process cell write lock

`write_edge_txn` creates a cell write-lock path containing the database path
and cell ID. Conditional object-store writes establish an owner token and TTL.
A stale lock can be reclaimed; release verifies the token so an expired writer
cannot delete its successor's lock.

The lock is broader than the SlateDB transaction. It protects the read/decide/
commit protocol across independent processes using the same object store.

== Phase 3: transact against one snapshot

The locked path begins a `SerializableSnapshot` transaction.
`validate_write_fence_txn` acts as a drop-guard: it checks the cell-drop markers
(rejecting a dropped or drop-pending cell) and then calls `ensure_write_authority`
to reconfirm writer authority. It reads no durable fence and no lease token; there
is no such record.

The transaction then performs the logical work:

#figure(
  table(
    columns: (0.35fr, 1.7fr, 1.4fr),
    inset: 6pt,
    table.header([*Step*], [*Read or decision*], [*Write produced*]),
    [1], [idempotency lookup], [return prior result or reserve key],
    [2], [`last_epoch`], [next cell epoch],
    [3], [canonical/segment edge state], [insert vs already existed],
    [4], [existing metadata/index state], [forward/reverse/index updates],
    [5], [degree and mutation semantics], [degree counters],
    [6], [new logical edge], [delta/outbox and mutation log],
    [7], [request fingerprint], [idempotency result],
  ),
  caption: [One logical edge mutation fans out inside one serializable transaction.],
) <tab-write-fanout>

#figure(
  diagram(
    spacing: (5mm, 7mm),
    node-stroke: 0.5pt,
    crossing-fill: reader-colors.paper,
    node-corner-radius: 3pt,
    // Phase flow, left to right
    node((0, 0), text(size: 8pt)[*Phase 1*\ reject wrong writer\ authority · semaphore · lane],
      fill: reader-colors.surface_soft, stroke: reader-colors.border, width: 34mm),
    edge((0, 0), (1, 0), "->", stroke: reader-colors.muted),
    node((1, 0), text(size: 8pt)[*Phase 2*\ acquire cell\ write lock\ (cross-process)],
      fill: reader-colors.surface_soft, stroke: reader-colors.border, width: 30mm),
    edge((1, 0), (2, 0), "->", stroke: reader-colors.muted),
    node((2, 0), text(size: 8pt)[*Phase 3 — serializable txn*\ drop-guard + authority · idempotency\ · epoch · edge state · indexes\ · degree · delta/outbox · idem result],
      fill: reader-colors.info_soft, stroke: reader-colors.info, width: 58mm, height: 26mm),
    edge((2, 0), (3, 0), "->", stroke: reader-colors.muted),
    node((3, 0), text(size: 8pt)[*Phase 4*\ retry on\ conflict / report],
      fill: reader-colors.surface_soft, stroke: reader-colors.border, width: 28mm),
    edge((3, 0), (4, 0), "->", stroke: reader-colors.muted),
    node((4, 0), text(size: 8pt)[release lock],
      fill: reader-colors.surface_soft, stroke: reader-colors.border, width: 24mm),
    // commit divider between before / after states
    edge((2.55, -0.55), (2.55, 1.35), stroke: reader-colors.ok + 1.4pt,
      label: text(size: 7.5pt, fill: reader-colors.ok)[commit], label-side: left),
    node((2, 0.95), text(size: 7.5pt)[before: nothing visible],
      fill: reader-colors.ok_soft, stroke: none),
    node((3.4, 0.95), text(size: 7.5pt)[after: edge + epoch + indexes\ + delta + idem visible together],
      fill: reader-colors.ok_soft, stroke: none),
  ),
  caption: [One logical edge mutation fans out inside one serializable transaction;
    nothing is visible before commit, everything after.],
) <fig-detail02-phases>

`commit_txn_strict` uses durable write options. Write-authoritative shards reject
configuration that would acknowledge before remote-visible metadata is durable.

== Phase 4: retry or report

Retryable conflicts — a SlateDB transaction conflict (`Slate(Transaction)`) or a
`CellWriteConflict` — yield the task and restart, up to the configured retry
limit; any other outcome is returned as-is. A success increments write metrics
and returns the new epoch.

The cell write lock is released after the transaction result is known. The
release path preserves the original error if cleanup also fails.

== Idempotency is semantic, not merely transport-level

The idempotency key is bound to the mutation fingerprint. Replaying the same
edge request returns the recorded result. Reusing the key for a different edge
is corruption from the API's perspective and is rejected.

An already-existing edge can legitimately return without incrementing degree,
but the idempotency record still distinguishes a cached acknowledgement from a
fresh logical decision.

== Failure boundaries

#boxeq[
  *Before commit: no logical mutation is visible. After commit: edge, epoch,
  indexes, delta, and idempotency become visible together.*
]

This guarantee is cell-local. A caller that needs two cells changed together
must provide a higher-level protocol; the kernel intentionally exposes no
cross-cell atomic write.
