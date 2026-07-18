---
id: BFG-003
title: Relationship MERGE identity scope is ambiguous
status: blocked
severity: P1-P2
classification: identity-contract-gap
introduced_or_first_bad_commit: d01e32e
fix_commit: none
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

Blocked on a public identity decision. The analysis shows a mismatch between endpoint-scoped relationship lookup and batch coalescing by external relationship ID; it does not establish which behavior the API intends.

## Intended behavior

One external relationship identity must have one documented scope. If it is cell-global, a second endpoint using that ID must reject. If it is endpoint-scoped, both endpoint records may exist and batch coalescing must use that composite key. Silent aliasing or accidental pre-lookup conflict is never permitted.

## Reproduction to add

Submit one batch containing two relationship `MERGE` rows with the same external `id` and different `(src, dst)` endpoints. Record whether the current implementation rejects, aliases, or creates both through direct batch, Cypher, HTTP, and Bolt entry points.

## Impact

An ambiguous identity can reject a valid batch, update the wrong relationship, or make retry/idempotency behavior depend on ingestion path.

## Formal coverage and next step

M1 supplies atomic identity/idempotency context; M5's `rejectAmbiguousRelationshipId` makes the provisional no-silent-alias rule executable. Its deterministic scenario and six-step bounded check pass. This is not source conformance until the identity scope is approved and the Rust adapter executes both endpoint cases.

## Review decision

Required: choose cell-global versus endpoint-scoped external relationship IDs.
