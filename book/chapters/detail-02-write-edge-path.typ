#import "../vendor/bookly/src/bookly.typ": *

= The `write_edge` Path

The public entry point is `GraphShard::write_edge` in `src/shard/write.rs`.
Its input is an `EdgeMutation`: cell, edge type, source, destination, and an
idempotency key. Its output is a `CommitResult` containing the commit epoch and
whether the edge already existed.

== Phase 1: reject the wrong writer cheaply

Before opening a transaction, the method validates components and calls
`ensure_write_authority`. A read-only shard fails. A leased shard must find a
non-expired lease owned by its local node. The routed cluster performs the same
owner and active-lease check before reaching the shard.

Next it acquires a graph-write semaphore and a hashed writer lane. These are
process-local controls: backpressure plus low-cost serialization.

== Phase 2: acquire the cross-process lock

`write_edge_txn` creates an object-store lock path containing the database path
and cell ID. Conditional object-store writes establish an owner token and TTL.
A stale lock can be reclaimed; release verifies the token so an expired writer
cannot delete its successor's lock.

The lock is broader than the SlateDB transaction. It protects the read/decide/
commit protocol across independent processes using the same object store.

== Phase 3: transact against one snapshot

The locked path begins a `SerializableSnapshot` transaction. For a leased
writer, `validate_write_fence_txn` reads the durable fence and checks cell,
owner, lease token, and expiry.

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

`commit_txn_strict` uses durable write options. Write-authoritative shards reject
configuration that would acknowledge before remote-visible metadata is durable.

== Phase 4: retry or report

Retryable transaction conflicts yield and restart, up to the configured retry
limit. A stale lease is counted separately and returned immediately. A success
increments write metrics and returns the new epoch.

The object-store lock is released after the transaction result is known. The
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
