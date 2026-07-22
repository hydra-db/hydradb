---
id: BFG-013
title: Compiled-index generation ahead of the pinned read snapshot
status: not-a-bug
severity: P2
classification: guarded-by-callers
introduced_or_first_bad_commit: n/a
fix_commit: none
affected_range: present at 36a38a6 (Turbolay-V3.5)
model:
  intended: none-yet (candidate: M2 snapshot read family)
  fault: none-yet
current_verified_commit: 36a38a6
date_opened: 2026-07-22
date_verified: 2026-07-22
tags: [bugs, graph-index, snapshots, read-epoch, not-a-bug]
---

# BFG-013: compiled generation ahead of the pinned read snapshot

## Status

**Not a bug through any reachable caller.** The unsafe branch exists, but every
path that could reach it filters first. A guard test is committed so a relaxed
filter fails loudly rather than silently reopening this.

## The suspected bug

`topology_tail_since` returns an empty overlay when
`generation.base_sequence >= read_sequence` (`src/shard/topology_tail.rs:38`).
Equality is sound — the base already covers the read epoch. The `>` case is
not: it would merge a CSC built at a **newer** sequence than the snapshot the
read is pinned to, so an edge deleted in `(read_epoch, base_sequence]` would
vanish from a read that should still see it, and an edge created in that window
would appear early. That is the "stale under strong" class, and the indexer
publishes asynchronously, so a generation genuinely can race ahead of a
long-lived reader's pinned epoch.

## Why it cannot be reached

Two independent filters, both on the only path into that line:

1. **Generation selection is exact.** `graph_index_generation_at`
   (`src/engine/index_store.rs:194-207`) looks up the in-memory cache by
   `MatrixCacheKey::new(cell_id, edge_type, base_sequence)` and, on a miss,
   filters discovery with `generation.base_sequence == base_sequence`. It can
   only ever return a generation whose `base_sequence` equals the requested
   `base_epoch`.
2. **The requested epoch is itself bounded.** Every caller passes
   `artifact.base_epoch` from `latest_matrix_artifact(cell, type, read_epoch)`
   (`src/shard/query.rs:5033`, `:5105`, `:5186`), which selects artifacts at or
   below `read_epoch`.
3. **The refresh branch re-filters.** When the cached matrix is missing and the
   code retries against a newer generation, it requires
   `latest.base_sequence <= read_epoch` before adopting it
   (`src/shard/query.rs:5281`), and the admission-rejection retry path applies
   the same bound.

`compiled_graphblas_query_snapshot` additionally returns `None` outright when
`storage_snapshot.seq() != read_epoch` (`src/shard/query.rs:5268-5271`), so a
mismatched snapshot never reaches the tail at all.

## Guard test

`compiled_graph_index_generation_never_exceeds_the_read_epoch`, in
`src/tests.rs`, `#[cfg(feature = "graphblas")]`, not `#[ignore]`-marked.

Seeds a cell, records a `pinned_read_epoch`, then advances the store and
publishes a generation strictly ahead of it — asserting
`generation.base_sequence > pinned_read_epoch` so the setup cannot silently
degenerate. It then asserts that `graph_index_generation_at` at the pinned
epoch either returns nothing or returns a generation at or below that epoch.
It passes.

## What would change this verdict

Relaxing either filter — an inexact generation lookup, or a caller passing an
unbounded `base_epoch` — reopens this immediately. The guard test is written
against `graph_index_generation_at` directly, so it fails on the selection
change rather than waiting for a traversal to produce a wrong answer.

## Fix directions (not applied, not warranted)

None. If defence in depth is wanted, `topology_tail_since` could return
`Unavailable` rather than an empty overlay when
`generation.base_sequence > read_sequence`, converting an unreachable silent
wrong answer into an unreachable clean fallback. That is hardening, not a fix.
