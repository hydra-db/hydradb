---
id: BFG-003
title: Relationship MERGE identity scope is ambiguous
status: reproducing
severity: P1-P2
classification: identity-contract-gap
introduced_or_first_bad_commit: d01e32e
fix_commit: pending
model:
  - quint-models/turbolay/m1_cell_write.qnt
  - quint-models/turbolay/m5_public_commands.qnt
current_verified_commit: f45662c
date_opened: 2026-07-18
date_verified: null
tags: [bugs, relationships, merge, identity, quint]
---

# BFG-003: relationship MERGE identity scope

## Status

Identity scope is **approved as endpoint-scoped** (see Review decision). The
current implementation diverges: it keys relationship identity on the external ID
alone and rejects a same-ID/different-endpoint import with
`GraphError::IdempotencyConflict`, which the M5 adapter `reject_ambiguous`
reproduces today. This is now a confirmed contract violation with a scoped source
fix; the fix is deferred to a separately-reviewed PR (see Implementation plan).
Until then the model action and adapter intentionally still encode the pre-fix
rejection so the MBT suite stays green against unmodified source.

## Intended behavior

One external relationship identity must have one documented scope. If it is cell-global, a second endpoint using that ID must reject. If it is endpoint-scoped, both endpoint records may exist and batch coalescing must use that composite key. Silent aliasing or accidental pre-lookup conflict is never permitted.

## Reproduction to add

Submit one batch containing two relationship `MERGE` rows with the same external `id` and different `(src, dst)` endpoints. Record whether the current implementation rejects, aliases, or creates both through direct batch, Cypher, HTTP, and Bolt entry points.

## Impact

An ambiguous identity can reject a valid batch, update the wrong relationship, or make retry/idempotency behavior depend on ingestion path.

## Formal coverage and next step

M1 supplies atomic identity/idempotency context; M5's `rejectAmbiguousRelationshipId` makes the provisional no-silent-alias rule executable. Its deterministic scenario and six-step bounded check pass. This is not source conformance until the identity scope is approved and the Rust adapter executes both endpoint cases.

## Review decision

Reviewed 2026-07-18 (vyom@hydradb.com). **Decision: endpoint-scoped external
relationship IDs.** Relationship identity is the composite key
`(relationship_id, src, dst)`. The same external ID at two different endpoints
(e.g. `id=7` on `A→B` and `id=7` on `A→C`) yields two distinct relationship
records that both persist; batch coalescing must key on the composite. A retry
updates only the record matching its endpoints. Silent aliasing of two distinct
endpoints onto one record remains forbidden.

**Consequence — reclassified from `blocked` to a confirmed contract violation.**
The current implementation instead keys relationship identity on the external ID
alone: importing `id=7` at a new endpoint fails with
`GraphError::IdempotencyConflict` (proven by the M5 adapter `reject_ambiguous`).
That directly contradicts the approved endpoint-scoped contract, so the code, the
M5 `rejectAmbiguousRelationshipId` action, and its `ambiguousRelationshipNeverAliases`
witness must all be revised to the endpoint-scoped semantics together. Source-fix
scope and status transition are tracked below.

## Implementation plan (deferred to reviewed PR)

Scoped 2026-07-18 from a read-only source survey. **Size: medium**, contained:
the primary relationship record key is already composite
(`cell/{cell}/rel/{edge_type}/{src:020}/{dst:020}/{relationship_id:020}`,
`src/keys.rs:129-137`) and the import fingerprint already includes `src`, `dst`,
and `id` (`relationship_import_fingerprint`, `src/codec.rs:1505-1508`), so no
data migration of primary records or fingerprints is needed. The **only** enforcer
of id-alone uniqueness is the `rel_id` reverse index
(`keys::relationship_id`, `src/keys.rs:157-159` →
`cell/{cell}/rel_id/{relationship_id:020}`), which is referenced **exclusively in
`src/shard/write.rs`** — no read/query/MERGE/adjacency path resolves a
relationship by external ID (reads use the `rel/` composite prefix via
`source_relationship_id_bindings_at`, `src/shard/query.rs:906`; MERGE already
resolves endpoint-scoped via `relationship_ids_for_edge_property_txn`,
`write.rs:1476-1508`). That is why the change is contained.

Coordinated edit sites, all in `src/shard/write.rs` unless noted:

1. **Batch coalescing** — `coalesce_relationship_imports` (`write.rs:5284-5306`).
   Change the dedup map key from `RelationshipId` to the composite
   `(RelationshipId, src, dst)` so same-ID/different-endpoint rows in one batch
   coexist instead of returning `IdempotencyConflict` at `write.rs:5294`.
2. **Import existence check** — `import_relationships_batch_txn_locked`
   (`write.rs:1519-1599`). Stop consulting the id-alone reverse index
   (`id_key` branch, `write.rs:1527-1549`); decide existence from the composite
   `rel_key` only (the `None => read rel_key` branch at `write.rs:1550-1556`
   already does the right thing). The endpoint-mismatch conflict at
   `write.rs:1568-1578` becomes unreachable for imports and is removed.
3. **`rel_id` reverse index** (`keys.rs:157-159`; writes at `write.rs:1763-1773`;
   reads at `write.rs:1527`, `1978`, `5492`; deletes at `write.rs:2396`, `5765`).
   One external ID can now map to multiple primary records, so either drop the
   index entirely or make it composite (`rel_id/{id}/{src}/{dst}`) if any consumer
   still needs an id→record lookup.
4. **CREATE auto-id allocator** — `next_available_relationship_id_txn`
   (`write.rs:5480-5499`) uses the reverse index to skip used ids. If the index
   is dropped/reshaped, give it an alternate uniqueness source (composite reverse
   index, or rely on the monotonic `last_relationship_id` counter).
5. **Delete cleanup** — `delete_relationship_txn_locked` (`write.rs:2326-2415`).
   The primary delete already keys on the full `(src,dst,id)` composite
   (`write.rs:2353-2359`) and is correct for siblings, **but** it unconditionally
   removes the id-alone reverse index at `write.rs:2396`, which would clobber a
   sibling's pointer. Make this composite-aware or remove it with the index. Same
   for `delete_relationships_for_structural_edge_txn` (`write.rs:5765`).

**Correctness traps to verify:** no id reuse after the allocator change; no
dangling/clobbered reverse-index pointers across sibling delete; MERGE and
parallel-relationship behavior unchanged.

Formal + adapter changes to land in the same PR:
- Replace M5 `rejectAmbiguousRelationshipId` with an
  `createEndpointScopedRelationship` action (same ID `7`, endpoint `dst=3`,
  distinct live record) and replace the `ambiguousRelationshipNeverAliases`
  witness with an endpoint-scoped-identity invariant. Add a
  `m5_public_commands_buggy.qnt` preserving the id-alone rejection so `quint run`
  still yields the counterexample (per `validation-protocol.md`).
- Rewrite the M5 adapter `reject_ambiguous` (`tests/formal_mbt_m5.rs:180-198`) to
  assert the second endpoint import **succeeds** and both records persist.
- Historical reproduction: worktree at `d01e32e` (or first-bad after bisect),
  replay the model, confirm the violation; then confirm absent at the fix commit.
- Re-run: `quint typecheck`/`quint test` for M5, `cargo test --test formal_mbt_m5`
  (InMemory) and `just minio-mbt`, plus `cargo clippy --all-targets -D warnings`.

On landing, transition `reproducing` → `fixed-pending-review` with the fix commit,
historical worktree result, and current MBT output.
