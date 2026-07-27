---
id: BFG-007
title: Remote bookmark proof and read-only freshness are unspecified
status: blocked
severity: P2
classification: freshness-contract-gap
introduced_or_first_bad_commit: 215bed9-and-e875387
fix_commit: pending
model:
  - quint-models/turbolay/m2_snapshot_read.qnt
  - quint-models/turbolay/m4_placement_fence.qnt
current_verified_commit: f45662c
date_opened: 2026-07-18
date_verified: null
tags: [bugs, bookmarks, readers, replicas, freshness, quint]
---

# BFG-007: bookmark proof and read-only freshness

## Status

Freshness contract is **approved as read-your-writes after bookmark** (see Review
decision). Current state is partial: the local read path already enforces RYW and
the typed proof-unavailable error already exists
(`GraphError::UnsupportedQuery { feature: "backend cannot prove bookmark
durability" }`, `src/client/service.rs:701-704`). Two gaps remain — the **remote**
path cannot prove the epoch (it inherits `current_graph_epoch = Ok(None)` and so
returns the typed error rather than a satisfied bookmark), and a **checkpoint-pinned
reader has no refresh/staleness bound**. Remains `blocked`: the reader-refresh half
may require a SlateDB dependency bump (the pinned `DbReader` revision does not
appear to expose a refresh primitive), and full remote RYW needs a versioned
wire-protocol change. Both are deferred to a separately-reviewed PR (see
Implementation plan).

## Intended behavior

At minimum, a bookmark must not regress within one principal/scope and must be either provably satisfied or return a typed unsupported/proof-unavailable error. Any read-only freshness guarantee must state whether it is none, bounded staleness, monotonic reads, or read-your-writes after a bookmark.

## Reproduction to add

Use a remote client to write, retain its bookmark, and read through a separate client/reader. Record the epoch-proof response and reader visibility before and after reopen/refresh. Repeat after a routed-owner takeover.

## Impact

Applications can assume a causal read guarantee that the remote path cannot prove, or indefinitely read an old checkpoint without an explicit contract.

## Formal coverage and next step

M2 checks bookmark monotonicity and M4 checks safe ownership transfer. Neither claims freshness. Their bounded checks pass as safety-only evidence. Implement a model/MBT action only after the selected remote epoch and reader-refresh contract is approved.

## Review decision

Reviewed 2026-07-18 (vyom@hydradb.com). **Decision: read-your-writes after
bookmark.** A reader presenting a valid bookmark must observe at least its own
prior writes. On the remote path the epoch must be provably satisfied, or the
call must return a typed proof-unavailable / unsupported error rather than
silently serving a stale view. A bookmark must never regress within one
principal/scope. Checkpoint-pinned readers must expose a refresh path so this
guarantee holds after reopen.

**Consequence — a confirmed contract gap with a scoped, partly external fix.** The
current safety-only models (M2 bookmark monotonicity, M4 safe fencing) do not
provide read-your-writes. Delivering the approved contract requires a source
change on the remote/bookmark path plus a new M2 freshness action/witness and a
matching adapter step. Scope is below.

## Implementation plan (deferred to reviewed PR)

Scoped 2026-07-18 from a read-only source survey. Deliver in two independent
sub-fixes; the first is small and unblocks the guaranteed-or-typed-error contract,
the second is the harder remote/reader work.

**Already in place (no change):** `ClientBookmark { target, epoch }`
(`src/client/service.rs:129-153`); validation via `validate_bookmark`
(`service.rs:1330-1341`) → `ensure_bookmark` (`service.rs:695-713`), which already
returns the typed `UnsupportedQuery` when the backend cannot prove durability
(`service.rs:701-704`) and `SnapshotAhead` when `current_epoch < bookmark.epoch`
(`service.rs:705-711`); local RYW because `RoutedGraphCluster::current_graph_epoch`
returns `Some(shard.current_epoch())` (`src/query/coordination.rs:3298-3310`).

**Sub-fix 1 — typed-error contract hardening (SMALL).** The remote client
`TcpQueryCellClient` (`coordination.rs:2102-2219`) does not override
`current_graph_epoch`, so it inherits the `Ok(None)` default
(`coordination.rs:1606-1612`) and remote bookmarks fall to the typed error. Make
this an *explicit documented contract* rather than incidental: assert in tests
that a remote bookmark either succeeds or returns the typed proof-unavailable
error, and document RYW-after-bookmark as local-guaranteed / remote-typed-error.
Add an M2 freshness action `readYourWritesAfterBookmark` (a read pinned to
`>= sessionBookmark` succeeds) plus a `bookmarkProofUnavailableIsTyped` witness,
and a matching M2 adapter step. No wire or storage change.

**Sub-fix 2 — real remote proof + reader refresh (MEDIUM→LARGE, partly external).**
- *Remote epoch proof:* add a `CurrentEpoch { scope, cell_id }` variant to the
  versioned `QueryTransportRequest`/`QueryTransportResponse`
  (`coordination.rs:3559-3593`, note `QUERY_TRANSPORT_VERSION` at
  `coordination.rs:2109`), override `current_graph_epoch` in `TcpQueryCellClient`
  to issue it, and dispatch it server-side near the existing shard-epoch handler
  (`coordination.rs:3050-3103`). Additive but crosses the process boundary and the
  versioned protocol — treat as a compat-sensitive change.
- *Reader refresh:* the read-only `DbReader` (`GraphStore::Reader`,
  `src/core/state.rs:72`; opened in `open_graph_reader` /
  `apply_to_reader_options`, `src/core/config.rs:110-123,327-338`) sets no manifest
  poll interval and exposes no `DbSnapshot` (`state.rs:136-145`). Add a bounded
  manifest poll interval and a "refresh reader until `epoch >= bookmark` else typed
  staleness error" loop in `ensure_bookmark`. **Blocker:** the pinned SlateDB
  revision does not appear to expose a reader-refresh primitive (see comments at
  `state.rs:141-143`, `src/core/snapshot.rs:37,82,118,184`,
  `src/shard/lifecycle.rs:588,633`); if confirmed, this needs a SlateDB dependency
  bump or a reader-reopen strategy — the reason this record stays `blocked`.

Re-run on landing: `quint typecheck`/`quint test` for M2, `cargo test --test
formal_mbt_m2` (InMemory) and `just minio-mbt`, plus `just minio-fence` for the
ownership/fencing interaction, and `cargo clippy --all-targets -D warnings`. Then
transition `blocked` → `fixed-pending-review`.
