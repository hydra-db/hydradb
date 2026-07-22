---
id: BFG-009
title: Epoch-scoped reads compose over unpinned live state, so later writes rewrite the answer
status: open
severity: P0
classification: confirmed-bug
introduced_or_first_bad_commit: pending-bisect
fix_commit: none
affected_range: present at a43ec61 (Turbolay-V3.5)
model:
  intended: quint-models/turbolay/m2_epoch_scoped_read.qnt
  fault: quint-models/turbolay/m2_epoch_scoped_read_buggy.qnt
  scenarios: quint-models/turbolay/m2_epoch_scoped_read_test.qnt
current_verified_commit: a43ec61
date_opened: 2026-07-22
date_verified: null
tags: [bugs, read-epoch, segments, tombstones, mvcc, data-loss, regression]
---

# BFG-009: epoch-scoped reads compose over unpinned live state

## Summary

The visibility rule for segment adjacency — `segment.storage_sequence <=
read_epoch`, tombstone applies when `tombstone_epoch <= read_epoch` — is only
sound when evaluated against storage state pinned at `read_epoch`. Several read
paths evaluate it against **live (or later-pinned) state** instead. Two writer
behaviors then destroy the history the rule depends on:

1. A segment re-append **deletes the tombstone key** of a prior acknowledged
   delete (`src/shard/write.rs:4279`).
2. The point-edge branch of the composition has **no epoch filter at all**
   (`src/engine/artifact_build.rs:497-506`; same shape in
   `out_neighbors_in_storage_snapshot`, `src/shard/query.rs:135-142`).

Result: an epoch-scoped read is not a pure function of its epoch. An
acknowledged delete can be retroactively erased from the epoch at which it was
acknowledged, and a read can return edges committed after its epoch. Under
ordinary concurrency this contradicts acknowledged history on **17% of
current-epoch reads** in the reproduction below.

## Three proven expressions (all e2e, `src/tests.rs`, run with `--ignored`)

All three tests encode the intended contract and fail at `a43ec61`.

### 1. Acknowledged delete retroactively erased (deterministic)

`epoch_scoped_read_is_stable_after_segment_reinsert_clears_the_tombstone`

Segment-append `1->2` (epoch 1); `delete_edge` acknowledged at epoch 2;
`edge_exists_at(1,2, epoch=2)` correctly answers `false`. A later re-append
(epoch 3) deletes the tombstone key. The **same call with the same epoch** now
answers `true`:

```text
read at epoch 2 changed its answer after an unrelated later re-insert: the
acknowledged delete at epoch 2 has been retroactively erased
```

### 2. Read returns data from the future of its epoch (deterministic)

`epoch_scoped_read_excludes_edges_committed_after_the_requested_epoch`

Segment edge committed at epoch 1, point edge committed at epoch 2.
`out_neighbors_at(..., epoch=1)` returns both:

```text
assertion `left == right` failed: read at epoch 1 returned an edge committed
later at epoch 2
  left: [2, 3]
 right: [2]
```

This is the BFG-002 shape resurfacing at the shard `_at` APIs
(`edge_exists_at` / `out_neighbors_at` / `out_degree_at` / `edges_at`,
`src/shard/maintenance.rs:14-57`, `src/shard/query.rs:6012`): the epoch is
accepted unvalidated and answered from live state.

### 3. Current-epoch reads contradict acknowledged history (stress, 4s)

`current_epoch_reads_match_acknowledged_history_under_concurrent_reinserts`

One writer cycles acknowledged `delete_edge` / segment re-append on a single
edge, recording each acknowledged (epoch, state). A reader derives
`current_epoch()` and immediately asks `edge_exists_at` at that epoch — the
exact `query_read_epoch`-then-read composition the server paths use. At
`a43ec61`:

```text
681 current-epoch reads contradicted acknowledged history; first:
(read_epoch, expected, observed) = Some((6, false, true))
```

A read anchored at epoch 6 observed an edge whose delete was acknowledged *at*
epoch 6. This is a read-your-writes violation for the deleting session and a
Jepsen `set-full` resurrection (present → acknowledged-absent → present).

## Root cause

Epoch-scoped visibility is evaluated over storage state that is not pinned at
`read_epoch`, while writers destroy the history the evaluation needs:

- `edges_at` → `canonical_adjacency_at` → `current_matrix_rows`
  (`src/engine/artifact_build.rs:488-532`): three separate live prefix scans
  (point edges unfiltered; tombstones and segments epoch-filtered), no snapshot
  pinning at all when called outside `scope_snapshot`.
- Segment re-append deletes tombstone keys (`src/shard/write.rs:4279`);
  segment compaction deletes tombstones and rewrites segment sequences
  (`src/shard/maintenance.rs:207-229`). Both are correct for *current* reads
  but erase the record that epoch-scoped evaluation over newer state requires.

## Blast radius beyond the `_at` APIs

- **Kernel/streaming page fast paths** (`src/shard/query.rs:4475`, `:4607`)
  derive `read_epoch` from a dropped snapshot (`query_read_epoch`,
  `src/shard/query.rs:4495`) and their storage fallback takes its **own fresh
  snapshot** (`reachable_from_storage_frontier`, `src/shard/query.rs:4894`)
  with no `seq == read_epoch` guard. Any delete + re-append (or point write)
  committing between the two lines produces the same torn composition inside a
  single page. (The compiled path *does* guard with `seq() != read_epoch →
  fallback` — `src/shard/query.rs:5268` — but the fallback it lands on is the
  unguarded one.)
- **`validate_cell_edge_type`** (`src/shard/query.rs:6059`) reads
  `current_epoch` then `edges_at` unpinned — its repair report is subject to
  the same race and can report false degree mismatches.
- The main rows path (`execute_parsed_opencypher_rows`,
  `src/shard/query.rs:461-476`) is **safe**: it pins one snapshot via
  `scope_snapshot` and derives `read_epoch` from that snapshot, so live scans
  resolve against the pinned state.

## Not implicated (checked and ruled out)

- Forced-sequence commits: `commit_txn_strict` pins the commit sequence to
  `seqnum()+1` (`src/codec.rs:143-159`), so epochs equal real commit sequences.
- Segment scan early-`break`: segment keys zero-pad the sequence
  (`src/keys.rs:67-74`), so lexicographic order is numeric order.
- Segment compaction is atomic and preserves out-of-range tombstones and
  live-at-epoch semantics (`src/shard/maintenance.rs:189-248`).
- DETACH DELETE scans segments as well as point edges
  (`src/shard/write.rs:739-773`).
- Indexer builds are consistent: `build_graph_index` runs
  `canonical_adjacency_at(base_sequence)` inside `scope_snapshot` at
  `base_sequence == snapshot.seq()`, which neutralizes both unpinned branches.

## Fix directions (not applied)

1. Pin every epoch-scoped read to a storage snapshot whose `seq() ==
   read_epoch`, or reject (`SnapshotAhead` / `UnsupportedQuery`) as
   `snapshot_at` already does (`src/shard/lifecycle.rs:620-639`). The `_at`
   APIs currently promise history the storage layer deliberately does not keep.
2. In the kernel/streaming page paths, derive `read_epoch` from the same
   snapshot the traversal uses (take the snapshot first, use its `seq()`),
   mirroring `execute_parsed_opencypher_rows`.
3. If historical epoch answers are a real requirement, stop destroying
   history: tombstone keys must not be deleted on re-append (write a
   supersede record instead), and point edges need an epoch-carrying record.
   This is a much larger change; option 1+2 restores correctness without it.

## Validation protocol

Per `validation-protocol.md`: bisect for `introduced_or_first_bad_commit` is
pending; the three regressions above must pass at the fix commit and the
`--ignored` markers must then be removed.

### Formal model (written 2026-07-22)

`m2_epoch_scoped_read.qnt` encodes the intended contract — every append, point
write, and tombstone is retained, so any epoch can be reconstructed — and holds
all three invariants:

- `epochScopedReadIsStable` — re-reading an already-answered epoch gives the
  same answer, however many commits landed at later epochs.
- `readNeverSeesTheFuture` — the answer equals the state reconstructed at the
  read epoch.
- `readMatchesAcknowledgedHistory` — a read anchored at an acknowledged epoch
  agrees with that acknowledgement.

```text
quint run m2_epoch_scoped_read.qnt --invariant=allSafety --max-steps=16 --max-samples=50000
[ok] No violation found
```

All four witnesses are reachable (same run, with `--witnesses`), so the
invariants are not holding vacuously:

```text
deleteAcknowledgedReached   witnessed in 49903 trace(s) out of 50000 (99.81%)
reappendAfterDeleteReached  witnessed in 41679 trace(s) out of 50000 (83.36%)
rereadAfterReappendReached  witnessed in 36530 trace(s) out of 50000 (73.06%)
pointEdgeAfterReadReached   witnessed in 43300 trace(s) out of 50000 (86.60%)
```

`m2_epoch_scoped_read_buggy.qnt` models the two implementation behaviors — the
re-append deleting preceding tombstones (`write.rs:4279`) and the unfiltered
point-edge branch (`artifact_build.rs:497-506`). It violates **two of the
three** invariants:

```text
quint run m2_epoch_scoped_read_buggy.qnt --invariant=epochScopedReadIsStable
[violation] Found an issue                     <-- expression 1
quint run m2_epoch_scoped_read_buggy.qnt --invariant=readNeverSeesTheFuture
[violation] Found an issue                     <-- expression 2
quint run m2_epoch_scoped_read_buggy.qnt --invariant=readMatchesAcknowledgedHistory
[ok] No violation found                        <-- expression 3 NOT discriminated
```

The third result is not a defect in the e2e evidence and not a weakening of the
report; it is a limit of this module. In the model a read is **atomic**: the
epoch is derived and the state is evaluated in a single action. Every write
action advances `epoch` and sets `ackedEpoch` to the new epoch, so
`observedAt == ackedEpoch` is only ever tested against the state that produced
the acknowledgement, and it always agrees (checked to `--max-steps=20
--max-samples=200000`). The third e2e repro fails for the reason the model
cannot express — `query_read_epoch` and the read are *separate* steps, and a
writer commits between them. Discriminating expression 3 formally requires a
module with a non-atomic read (derive-epoch and read-at-epoch as two actions);
that model has not been written.

The shortest counterexample to expression 1 is the create → delete → re-append
interleaving, at five steps (`--max-steps=4` finds none):

```text
quint run m2_epoch_scoped_read_buggy.qnt --invariant=epochScopedReadIsStable \
  --max-steps=5 --max-samples=20000 --verbosity=3 --seed=0x4

[State 1] appendSegment       epoch=1  segmentEpochs=Set(1)   tombstoneEpochs=Set()
[State 2] deleteEdge          epoch=2                         tombstoneEpochs=Set(2)
[State 3] readAtCurrentEpoch  observedAt=2 observed=false     tombstoneEpochs=Set(2)
[State 4] appendSegment       epoch=3  segmentEpochs=Set(1,3)
                                       tombstoneEpochs=Set()   <-- delete destroyed
[State 5] rereadFirstEpoch    observedAt=2 observed=true       <-- answer flipped
```

`m2_epoch_scoped_read_test.qnt` holds four deterministic scenarios mirroring the
three e2e repros; all pass against the intended module (`quint test`).
