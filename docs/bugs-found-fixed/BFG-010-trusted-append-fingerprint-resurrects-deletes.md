---
id: BFG-010
title: Trusted segment append resurrects an acknowledged delete when an already-fingerprinted import is replayed under a fresh key
status: open
severity: P1
classification: confirmed-bug
introduced_or_first_bad_commit: pending-bisect
fix_commit: none
affected_range: present at 36a38a6 (Turbolay-V3.5)
model: pending
current_verified_commit: 36a38a6
date_opened: 2026-07-22
date_verified: null
tags: [bugs, bulk-import, idempotency, fingerprint, segments, tombstones, data-loss, lost-delete]
---

# BFG-010: trusted-append fingerprint resurrects acknowledged deletes

## Summary

`bulk_append_out_adjacency_segment_trusted` recognises a replayed import two
ways: an exact **idempotency key** (`src/shard/write.rs:4223-4226`) and, failing
that, a **content fingerprint** over `(cell_id, edge_type, edges)`
(`src/shard/write.rs:4227-4228`, `src/codec.rs:1208-1229`, `:1325-1336`). The
fingerprint short-circuit only fires when `all_edges_still_exist`
(`src/shard/write.rs:4233-4242`). When some of the fingerprinted edges have
since been deleted, that guard fails and control falls through to the insert
path, which **deletes the segment tombstone key** for every destination it
inserts (`src/shard/write.rs:4279`).

So the store recognises the work as already accepted — the fingerprint record
is present — and reacts by *redoing* it, undoing the acknowledged
`delete_edge` in between. A replay of an import the store already accepted
silently resurrects a deleted edge. In Jepsen terms this is a lost delete /
`set-full` resurrection: present → acknowledged-absent → present, with no
client operation between the delete and the resurrection other than a retry.

The status is `open`: **no fix is applied**, and one contract question is
deliberately left for the user (see [Open contract
question](#open-contract-question-not-resolved-here)).

## Reproduction

Both tests live in `src/tests.rs` and were run at `36a38a6`.

### 1. Re-keyed replay resurrects the delete (deterministic, `#[ignore]`, FAILS by design)

`trusted_segment_reimport_under_a_fresh_key_preserves_an_acknowledged_delete`
(`src/tests.rs:12218-12271`). Segment-append `1->2` under key `import-run-1`;
`delete_edge` acknowledged; `edge_exists` correctly answers `false`; the same
content is then replayed under key `import-run-2`.

```console
$ cargo test --lib -- --ignored trusted_segment_reimport_under_a_fresh_key
running 1 test
test tests::trusted_segment_reimport_under_a_fresh_key_preserves_an_acknowledged_delete ... FAILED

---- tests::trusted_segment_reimport_under_a_fresh_key_preserves_an_acknowledged_delete stdout ----
thread '...' panicked at src/tests.rs:12265:5:
a re-keyed replay of an already-fingerprinted import resurrected an acknowledged delete: replay reported inserted=1 already_existed=0

test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 92 filtered out; finished in 0.06s
```

`inserted=1` is the second half of the damage: the caller is told one *new*
edge was appended, so nothing in the return value distinguishes a replay from
genuine work.

### 2. Same-key retry is a no-op (boundary, PASSES)

`trusted_segment_reimport_under_the_same_key_is_a_noop_after_a_delete`
(`src/tests.rs:12276-12311`). Identical sequence, identical idempotency key on
the retry.

```console
$ cargo test --lib -- trusted_segment_reimport_under_the_same_key
running 1 test
test tests::trusted_segment_reimport_under_the_same_key_is_a_noop_after_a_delete ... ok

test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 92 filtered out; finished in 0.01s
```

This test pins the hazard boundary. The idempotency-key path replays its stored
record (`src/shard/write.rs:4224-4226`) *before* anything touches storage, so an
identical retry cannot resurrect anything. The hazard is specifically the
**re-keyed** replay — and, as the next section shows, it is the fingerprint
guard that decides to treat that replay as new work.

## Root cause

`bulk_append_out_adjacency_segment_trusted_txn_locked`
(`src/shard/write.rs:4207-4304`), in order:

1. `src/shard/write.rs:4223-4226` — exact idempotency key hit ⇒ replay the
   stored `BulkImportResult`, touch nothing. This is the path test 2 takes.
2. `src/shard/write.rs:4227-4228` — read the content-fingerprint key
   `…segment-import-fp-{edge_type}-{src}/{fingerprint}`
   (`src/codec.rs:1325-1336`). The fingerprint covers only `cell_id`,
   `edge_type` and the edge list (`src/codec.rs:1208-1229`) — never the
   idempotency key — so it is exactly a "have I already done this content?"
   marker.
3. `src/shard/write.rs:4230-4232` — `existing` = destinations currently visible
   for `src`, tombstones applied.
4. `src/shard/write.rs:4233-4242` — fingerprint hit **and**
   `all_edges_still_exist` ⇒ replay the stored result
   (`decode_bulk_import_fingerprint_idempotency`, `src/codec.rs:997-1025`).
   When an edge was deleted in between, `all_edges_still_exist` is false and
   this branch is skipped. **This is the defect: the one case where the replay
   would change observable state is the one case the guard lets through.**
5. `src/shard/write.rs:4243-4247` — `inserted_dsts` = destinations *not*
   currently visible. After a delete this is precisely the deleted set.
6. `src/shard/write.rs:4276-4284` — for each inserted destination,
   `txn.delete(keys::out_segment_tombstone(...))` (`:4279`), then write a new
   segment record at `end_epoch` and bump `degree_out` (`:4285-4291`).

The tombstone is what made the delete visible: `edge_exists` reads the
tombstone epoch and applies `segment_edge_visible` per segment
(`src/shard/query.rs:93-119`), and `delete_edge` records the delete by writing
that key (`src/shard/write.rs:3341`). Deleting the key does not merely hide the
delete at the current epoch — it erases it at **every** epoch, which is why this
finding also appears as behavior 1 of [BFG-009](BFG-009-epoch-scoped-read-unpinned-composition.md).
BFG-009 uses that erasure to show epoch-scoped reads are unstable; BFG-010 is
the narrower and simpler claim that a **current** read flips back to `true`.

Step 6 is correct for a *new* append intent — a caller asking to append `1->2`
again should get `1->2` back. It is wrong for a *replay*. The store's only
means of telling the two apart are the idempotency key and the fingerprint, and
the fingerprint abdicates exactly when it matters.

## Blast radius

**Reachability.** The vulnerable transaction body has exactly one entry point:
the public `GraphShard::bulk_append_out_adjacency_segment_trusted`
(`src/shard/write.rs:3506-3573`), whose `idempotency_key` is caller-supplied
per `(src, dsts)` batch. `segment_import_fingerprint_key` has no other caller
(`src/codec.rs:1325`, used only at `src/shard/write.rs:4227`).

**Correction to the initial triage.** The chunked import APIs do synthesize
fresh per-chunk keys, but they do **not** route into this code:

- `src/shard/write.rs:4145` and `:4163` are inside
  `bulk_import_edges_chunked_with_options`, which calls
  `bulk_import_edges_with_options` → `bulk_import_edges_txn_locked`
  (`src/shard/write.rs:4331-4486`). That body keys only on the idempotency key
  (`:4348-4351`), has **no** content-fingerprint key, and never deletes a
  tombstone.
- `src/shard/write.rs:2780` and `:2800` are inside `delete_edges_batch_chunked`
  — a delete path, not an import path.
- `bulk_append_edges_trusted_chunked` / `bulk_append_edges_trusted_bounded`
  (`src/shard/write.rs:3466-3504`) likewise fan out to
  `bulk_import_edges_*`, not to the segment append.

So the `-chunk-{chunk_id:020}` key idiom is not itself a caller of this bug. It
matters as evidence of the *pattern*: this codebase's own convention is to
derive per-batch idempotency keys from batch position, and batch position is
not stable across a retry that batches differently.

**Callers that follow that pattern into the vulnerable API.** Both in-tree
callers key by position:

- `examples/stress_worker.rs:186-200` — key
  `{node_id}-segment-{src}-{start}-{ops}-{chunk_start}-{chunk_end}`, where the
  chunk bounds come from `GRAPH_SEGMENT_CHUNK`.
- `examples/write_profile.rs:311-322` — key
  `write-profile-segment-trusted-{phase}-{batch}`, where `batch` indexes a
  `batch_size`-sized window.

Re-running either with a different chunk size re-keys the same logical work.
Any external client that imports adjacency this way — the intended use of a
"trusted" bulk API — has the same exposure whenever a retry, a resharded
import job, or a changed batch size re-keys previously accepted content.

**Two shapes, one root.** Only the first is reproduced here:

1. *Same content, new key* (reproduced above). Fingerprint hits,
   `all_edges_still_exist` fails, insert path runs, tombstone deleted.
2. *Overlapping-but-different content, new key* (not reproduced; follows from
   the code at `src/shard/write.rs:4243-4247` and `:4279` having no replay
   guard at all). A rebatched retry whose chunk merely *contains* the deleted
   destination never reaches the fingerprint check — its fingerprint differs —
   and re-inserts it directly. This shape is wider than shape 1, and neither
   key can distinguish it from a legitimate new append.

**Damage is not symmetric with the delete.** `delete_edge` on a segment edge
also decrements `degree_out` (`src/shard/write.rs:3312-3314`), deletes the
relationship rows for the structural edge (`:3326`), and clears edge metadata
(`:3327-3340`). The trusted append writes back only the segment record, the
degree counter, and the dirty marker (`src/shard/write.rs:4276-4291`). By code
reading — this part is **not** covered by a test — the resurrected edge
therefore returns without the relationship rows and edge properties the delete
removed, so the graph can end up with a structural edge whose relationship
projection is gone.

## Not implicated (checked and ruled out)

- **`bulk_import_edges` / the point-edge import path.** No fingerprint key
  (`src/shard/write.rs:4348-4351` keys only on the idempotency key) and no
  tombstone deletion anywhere in `bulk_import_edges_txn_locked`
  (`src/shard/write.rs:4331-4486`). It cannot express this bug.
- **Relationship import.** `import_relationships_batch_txn`
  (`src/shard/write.rs:1363-1411`) computes a fingerprint
  (`relationship_import_fingerprint`, `src/codec.rs:1231`) but stores it only
  *inside* the idempotency record for conflict detection; there is no
  content-addressed second key and therefore no second short-circuit. Same for
  `create_relationship` (`src/shard/write.rs:1831-1839`, `:2073`).
- **Fingerprint collision.** Not the mechanism here. The fingerprint *hit* in
  the repro is a true hit on identical content; the decode even re-checks the
  stored fingerprint and raises `IdempotencyConflict` on mismatch
  (`src/codec.rs:1013-1018`).
- **The idempotency short-circuit itself.** Test 2 shows it is correct and
  sufficient for identical retries.
- **Degree accounting.** The delete decrements (`src/shard/write.rs:3312-3314`)
  and the resurrecting append increments (`:4285-4291`), so `degree_out` stays
  consistent *with the resurrection*. The counter does not detect the fault and
  is not separately corrupted by it.
- **Duplicate suppression within one call.** `dsts` is sorted and deduped
  before the fingerprint is computed (`src/shard/write.rs:3531-3534`), so the
  fingerprint is stable under caller-side ordering and duplication. Re-keying,
  not re-ordering, is what breaks it.

## Open contract question (not resolved here)

It is possible that "trusted append ensures presence" is the *intended*
contract — that a trusted append is a declaration of desired state ("these
edges shall exist") rather than an idempotent unit of work, in which case
overriding a delete is by design.

This document does not resolve that. It records the evidence on both sides and
flags it for the user:

- **Undocumented either way.** `README.md:298` lists
  `bulk_append_out_adjacency_segment_trusted` among the write paths with no
  statement of its delete-interaction semantics, and there is no other
  contract text for it in `docs/` or `books/`. Nothing in the repository says a
  trusted append is allowed to override an acknowledged delete.
- **Evidence the resurrection is unintended.** The idempotency path already
  treats an identical retry as a no-op *even after a delete* — that is exactly
  what the passing boundary test pins. If the contract were "ensure presence",
  the same-key retry in test 2 would be the inconsistent case and would be
  expected to restore the edge. Instead the same content produces two different
  outcomes depending only on the key it arrives under, which is a difference no
  documented contract explains.
- **Evidence the fingerprint is meant to be a replay guard.** It is keyed by
  content and stores a `BulkImportResult` to replay, and its result is returned
  verbatim on a hit (`src/shard/write.rs:4236-4241`) — that is the shape of a
  deduplication mechanism, not of a presence-assertion mechanism.

Until the user rules, the classification stands as `confirmed-bug` on the
narrow ground that identical input yields different observable outcomes based
only on the idempotency key, which no documented contract sanctions.

## Fix directions (not applied)

1. **Make the fingerprint hit terminal.** If the fingerprint record exists for
   this exact content, replay it and return, dropping the
   `all_edges_still_exist` condition (`src/shard/write.rs:4233-4242`). Smallest
   change; fixes shape 1 only, and only if the contract answer is "a trusted
   append is an idempotent unit of work".
2. **Stop the append from destroying delete history.** Do not
   `txn.delete(...out_segment_tombstone...)` at `src/shard/write.rs:4279`;
   instead let the newly appended segment out-rank the tombstone by sequence,
   which is what `segment_edge_visible` already computes
   (`src/shard/query.rs:114-118`). This fixes both shapes for current reads and
   is also fix direction 3 of BFG-009. It is the larger change — every consumer
   of the tombstone key (compaction, `src/shard/maintenance.rs:165`; artifact
   build, `src/engine/artifact_build.rs:509-560`) has to agree on the new
   supersede rule.
3. **Separate the two intents in the API.** If "ensure presence" is a real
   requirement, give it its own entry point and make the replay-guarded append
   refuse to cross a delete (a typed error, the way `snapshot_at` refuses an
   unsupported epoch, `src/shard/lifecycle.rs:620-639`), so a client that
   re-keys a retry gets an error instead of silent resurrection.
4. **Independently of the above, document the contract** on
   `bulk_append_out_adjacency_segment_trusted` and state whether an idempotency
   key is required to be stable across retries of the same logical import. The
   present behavior is only safe under a stability requirement that is nowhere
   stated.

## Validation protocol

Per [validation-protocol.md](validation-protocol.md): `introduced_or_first_bad_commit`
is pending bisect. At the fix commit, the `#[ignore]` marker on
`trusted_segment_reimport_under_a_fresh_key_preserves_an_acknowledged_delete`
must be removed and both tests must pass. No Quint model has been written for
this finding yet (`model: pending`); the natural home is the M1 cell-write
family, since the property is about write-side idempotency rather than read
epochs.
