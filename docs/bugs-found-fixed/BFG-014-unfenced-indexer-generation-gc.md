---
id: BFG-014
title: The async indexer is unfenced, and its generation GC takes no reader lease
status: not-a-bug
severity: P2
classification: self-fenced-by-monotonicity
introduced_or_first_bad_commit: n/a
fix_commit: none
affected_range: present at 36a38a6 (Turbolay-V3.5)
model:
  intended: none-yet (candidate: M3 artifact/GC family)
  fault: none-yet
current_verified_commit: 36a38a6
date_opened: 2026-07-22
date_verified: 2026-07-22
tags: [bugs, indexer, fencing, gc, jepsen, not-a-bug]
---

# BFG-014: unfenced indexer generation GC

## Status

**Not a data-loss bug.** The unfenced-ness is real; it is harmless because
manifest monotonicity supplies the ordering guarantee a fence would. Two guard
tests are committed. Jepsen was not required — the interleaving is fully
expressible in-process.

## The suspected bug, and the part of it that was wrong

Accurate in the original claim:

- The indexer opens as a **reader** (`src/bin/graph-indexer.rs:138`) and
  publishes and deletes `_graph_index` state purely through object-store calls,
  so SlateDB writer fencing genuinely does not apply to it.
- Publish is protected only by a monotonic CAS
  (`src/engine/index_store.rs:281-286`).

**Wrong:** that generation GC "has no guard at all". It has one — not
lease-shaped, but sufficient. `gc_graph_index_generations`
(`src/engine/index_store.rs:210-232`) reads the manifest fresh at the top of
the call and only queues a generation whose `base_sequence` is **strictly less
than** that `current.base_sequence`. `retain_previous` is a second, weaker
bound layered on top.

## Why a paused or zombie indexer cannot delete a live generation

The strict `<` against a freshly-read `current`, combined with manifest
monotonicity, is a self-fence:

1. `publish_graph_index` refuses any proposal not strictly ahead in
   `(base_sequence, last_wal_id)` (`src/engine/index_store.rs:281-286`), so the
   published manifest never moves backwards.
2. A GC caller therefore cannot observe a `current` **newer** than the real
   one. A stale zombie can only read an **older** manifest, giving a
   **smaller** `current.base_sequence`, so it deletes a strict subset of what a
   live GC would delete. Staleness makes a zombie more conservative, never more
   destructive.
3. The generation named by the live manifest is consequently never a delete
   candidate for any caller, at any degree of staleness — including under S3
   eventual consistency.
4. The dangling-manifest inverse closes the same way: once a generation at
   sequence `S` is deletable, `current > S` permanently, so a resumed zombie's
   re-publish of `S` is always refused by the CAS and it returns the
   already-published newer manifest instead.

## What a reader actually observes

A **superseded** generation genuinely can be deleted under a reader mid-fetch —
that is exactly what `retain_previous` bounds. That path is clean availability,
not loss:

- `graph_index_csc` (`src/engine/index_store.rs:159-168`) maps `NotFound` to
  `Ok(None)` at **both** the `get` and the `bytes` step, so a delete landing
  between manifest resolution and object fetch, or mid-body, is a clean miss
  rather than a truncated CSC.
- `cached_graphblas_matrix` (`src/engine/matrix_cache.rs:84-87`) turns that
  into `forget_graph_index_generation` plus `Ok(None)`.
- `compiled_graphblas_query_snapshot` (`src/shard/query.rs:5273-5288`) retries
  once against a newer generation bounded by `latest.base_sequence <=
  read_epoch`, else returns `Ok(None)`.
- Every consumer of that `Ok(None)` falls back to storage truth, never to an
  empty answer (`src/engine/traversal.rs:69-94`, `src/shard/query.rs:4866-4881`
  and `:4957-4971`). Both callers of `cached_graphblas_matrix` are inside
  `compiled_graphblas_query_snapshot`.

Worst reachable outcome: a slower query. Already covered by the pre-existing
`graph_index_query_recovers_when_gc_removes_its_selected_generation`.

## Guard tests (both pass, neither `#[ignore]`-marked)

1. `unfenced_index_gc_never_deletes_the_generation_the_current_manifest_names`
   — three unfenced reader-mode shards standing in for indexer replicas. Six
   rounds of publish → live reader resolves `current` → zombie runs
   `gc_graph_index_generations(..., 0)` at retention `0`, stricter than the
   binary's default `1` → reader completes its fetch. Then four rounds running
   publish and two aggressive GCs concurrently via `tokio::join!`, asserting the
   published manifest never dangles. Includes a non-vacuity assertion that the
   zombie GC really does delete objects.
2. `reader_holding_a_gc_deleted_index_generation_sees_a_clean_miss_not_lost_edges`
   — a reader caches a generation while current, two newer generations land, GC
   deletes it, and the test asserts `Ok(None)` rather than `CorruptValue` from
   both `graph_index_csc` and `cached_graphblas_matrix`, and that the full edge
   set still reads back. Deliberately avoids the compiled SuiteSparse kernel.

## What would change this verdict

Any change that lets the manifest move backwards, or that makes GC decide
against a cached rather than freshly-read `current`, destroys the self-fence
and reopens this as a genuine loss risk. The JEP-001 §8 gap stands on its own
merits — the indexer has still never been exercised under a pause nemesis, and
doing so remains worthwhile for reasons other than this suspect.

## Fix directions (not applied, not warranted)

None. Explicit fencing or a reader lease would be redundant with the
monotonicity argument above.
