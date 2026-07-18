---
id: BFG-003
title: Relationship MERGE identity scope is ambiguous
status: not-a-bug
severity: P1-P2
classification: identity-idempotency-contract
introduced_or_first_bad_commit: d01e32e
fix_commit: none
model:
  - quint-models/turbolay/m1_cell_write.qnt
  - quint-models/turbolay/m5_public_commands.qnt
current_verified_commit: f45662c
date_opened: 2026-07-18
date_verified: 2026-07-18
tags: [bugs, relationships, merge, identity, idempotency, quint]
---

# BFG-003: relationship MERGE identity scope

## Status

Resolved as `not-a-bug`: the current behavior is the intended design. The external
`relationship_id` is a **client-supplied idempotency / dedup key** on the bulk
and direct import path — not an endpoint-scoped label and not the interactive
relationship identity. Importing the same ID at a different endpoint reuses that
dedup key for a different edge, so returning `GraphError::IdempotencyConflict` is
correct. The M5 `rejectAmbiguousRelationshipId` action and the adapter
`reject_ambiguous` already encode this; model, adapter, and source agree.

## Intended behavior (approved contract)

Two distinct layers, deliberately different:

- **Interactive Cypher / Bolt:** relationships are pattern/property-addressed,
  exactly like FalkorDB. The internal `RelationshipId` is **auto-assigned** by the
  engine (`next_available_relationship_id_txn`, `src/shard/write.rs:1445,1491`);
  clients never supply or see it. `(A)-[:KNOWS]->(B)` and `(A)-[:KNOWS]->(C)` are
  simply two relationships, as in any Cypher engine.
- **Bulk / direct import** (`import_relationships_batch` / `RelationshipMutation`,
  and the UNWIND `relationship_id_field` mapping, `src/query/opencypher.rs:936`):
  the client may supply an external `relationship_id` that is a **per-cell
  idempotency / dedup key**. Importing the same ID with identical endpoints is
  idempotent (`already_existed`); importing the same ID with **different**
  endpoints is a client error — reusing a dedup key for a different edge — and
  returns `IdempotencyConflict`.

## Why this is intended, not a bug

- **It is not exposing an internal.** FalkorDB/Cypher never accept or expose a
  client-chosen relationship id; identity there is the pattern (endpoints + type).
  Turbolay matches that on the interactive path. The external id is a
  Turbolay-specific *ingestion* affordance, confined to the bulk/direct layer.
- **The id earns its place.** It exists for two jobs pattern-addressing cannot do
  at the ingestion layer: (1) **idempotent mirroring** of a source system's edge
  ids so replays/partial retries are safe, and (2) **addressing one of several
  parallel relationships** (same type between the same two nodes), where
  `(src, dst, type)` is not unique and `delete_relationship`/update need a stable
  handle keyed on the id.
- **Reuse-with-different-content must conflict.** That is the entire point of an
  idempotency key. Silently aliasing the id onto the first edge, or creating a
  second record under the same id, would break the dedup guarantee. Rejecting is
  the only behavior consistent with the key's purpose.

## Earlier decision reversed

A provisional "endpoint-scoped" choice was recorded earlier on 2026-07-18. On
review of FalkorDB/Cypher semantics and the id's actual role, it was reconsidered:
endpoint-scoped would have demoted the id to a per-endpoint label and required a
kernel change. Instead the id stays a per-cell idempotency key, the current
rejection is correct, and **no source, model, or adapter change is needed.**

## Impact

None (intended). Clients using the bulk/direct import path must treat the external
`relationship_id` as a unique dedup key per cell; reusing it for a different edge
is rejected by design. Interactive Cypher/Bolt clients are unaffected — they never
supply the id.

## Formal coverage

M1 supplies atomic identity/idempotency context; M5's
`rejectAmbiguousRelationshipId` is now the **approved contract** (not provisional):
a same-ID/different-endpoint import returns `rejected`, and
`ambiguousRelationshipNeverAliases` holds. Its deterministic scenario and
six-step bounded check pass, and the M5 adapter replays the `IdempotencyConflict`
against both InMemory and MinIO. Model, adapter, and source agree.

## Review decision

Reviewed 2026-07-18 (vyom@hydradb.com). **Decision: the external
`relationship_id` is a client-supplied per-cell idempotency / dedup key on the
bulk/direct import path — not an endpoint-scoped or cell-global public identity.**
A same-ID/different-endpoint import correctly returns `IdempotencyConflict`; the
interactive path remains pattern-addressed with auto-assigned internal ids
(FalkorDB-like). This reverses the earlier provisional endpoint-scoped choice.
Status transition: `blocked` → `not-a-bug`. No source, model, or adapter change.
