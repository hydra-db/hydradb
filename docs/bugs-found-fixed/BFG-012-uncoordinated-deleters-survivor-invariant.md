---
id: BFG-012
title: Three uncoordinated deleters have no shared "one readable copy survives" invariant
status: not-a-bug
severity: P2
classification: gc-coordination-risk
introduced_or_first_bad_commit: n/a
fix_commit: none
affected_range: present at 36a38a6 (Turbolay-V3.5)
model:
  intended: none-yet (candidate: M3 artifact/GC family)
  fault: none-yet
current_verified_commit: 36a38a6
date_opened: 2026-07-22
date_verified: 2026-07-22
tags: [bugs, gc, compaction, artifacts, availability, not-a-bug]
---

# BFG-012: three uncoordinated deleters, no survivor invariant

## Status

**Not a data-loss bug.** Reproduction attempt passes; kept as a regression
guard. A residual **availability** risk is real and recorded below — it is not
the loss the suspect predicted, so this is not filed as a confirmed bug, but it
should not be forgotten either.

## The suspected bug

Three independent code paths delete durable state, each checking a different
survivor in a different store, and nothing enforces a shared invariant that at
least one readable copy of a committed epoch survives:

- **Segment compaction** (`src/shard/maintenance.rs:126-143`) deletes raw
  adjacency segments after checking only that a matrix artifact exists at
  exactly `compacted_through_epoch`.
- **Artifact GC** (`src/engine/artifact_gc.rs:28`) deletes artifacts below a
  **caller-trusted** `keep_epoch`, with no check that anything newer survives,
  no reader lease and no fence.
- **Index-generation GC** (`src/engine/index_store.rs:226-232`,
  `retain_previous` defaults to 1) deletes `_graph_index` files.

The predicted failure was that sequencing them destroys the last readable copy
of a committed edge, leaving a reader with either a hard `CorruptValue`
(`src/engine/matrix_cache.rs:26-29`) or a fallback onto a store another GC has
already emptied.

## Reproduction attempt (passes)

`committed_edges_stay_readable_after_compaction_then_artifact_gc`, in
`src/tests.rs`, not `#[ignore]`-marked.

Creates a proper subset of live edges via two segment appends and one
acknowledged delete, so the surviving set is `[2, 4]` and never trivially equal
to everything written. Then, through the public API: builds a matrix artifact
at `base_epoch`; runs `compact_out_adjacency_segments` through that epoch;
asserts the live set is intact; runs
`delete_graph_artifacts_before(cell, type, base_epoch + 1)` — a caller-trusted
`keep_epoch` above the only artifact that exists — and asserts `deleted_keys > 0`
so the test cannot pass vacuously. Committed edges still read back as `[2, 4]`.

## Why it does not lose data

Compaction does not merely delete. It reconstructs the live edge set through
the tombstone filter and **writes it back as a new compacted segment**
(`src/shard/maintenance.rs:189-229`), then updates the degree counter. Live
current reads therefore never depend on the artifact surviving — the compacted
segment is itself a complete readable copy. Artifact GC deleting the only
artifact removes a derived acceleration structure, not the last copy of the
data.

## Residual availability risk (real, not filed as loss)

The caller-trusted `keep_epoch` still has no guard that anything newer exists.
Deleting the artifact at `base_epoch` makes a **subsequent** compaction at that
epoch hard-error: `compact_out_adjacency_segments` requires a matrix artifact
at exactly `compacted_through_epoch` and returns `CorruptValue` when it is
absent (`src/shard/maintenance.rs:126-143`). That is an operator-visible
failure, not silent loss, and it is recoverable by rebuilding the artifact.

## What would change this verdict

A representation where compaction does **not** write back a self-sufficient
compacted segment — for example if artifact-backed epochs became the only copy
for some edge class — would reopen the loss question immediately. The guard
test is deliberately written against the public API so it keeps holding the
line if compaction's write-back behaviour ever changes.

## Fix directions (not applied)

Not warranted for loss. If the availability risk is worth closing: make
`delete_graph_artifacts_before` refuse a `keep_epoch` that would leave no
artifact at or above the newest compacted epoch, rather than trusting the
caller.
