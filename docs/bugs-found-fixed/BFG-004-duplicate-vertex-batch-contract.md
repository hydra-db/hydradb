---
id: BFG-004
title: Duplicate vertex batch rows have an undocumented conflict policy
status: not-a-bug
severity: P2
classification: batch-semantics-contract-gap
introduced_or_first_bad_commit: ea7ec2c
fix_commit: none
model:
  - quint-models/turbolay/m1_cell_write.qnt
  - quint-models/turbolay/m5_public_commands.qnt
current_verified_commit: f45662c
date_opened: 2026-07-18
date_verified: 2026-07-18
tags: [bugs, batch, vertices, semantics, quint]
---

# BFG-004: duplicate vertex batch conflict policy

## Status

Resolved as `not-a-bug`: the observed behavior is the approved public contract.
V2 coalesces duplicate vertex IDs with equal values and rejects the whole batch
atomically when their mutable property values conflict. This was reviewed and
confirmed as intentional rather than a defect.

## Intended behavior

The public batch API must state one deterministic rule for duplicate vertex rows: reject conflicts atomically, first-row-wins, last-row-wins, or a defined merge. The chosen rule must be identical across direct ingestion and public Cypher/batch adapters where they claim equivalent semantics.

## Reproduction to add

Send a two-row batch with the same vertex ID and equal values, then repeat with conflicting values. Capture mutation result, durable vertex properties, epoch, and idempotent retry result for each entry point.

## Impact

Without a documented rule, clients can accidentally depend on input ordering or receive whole-batch rejection while expecting upsert behavior.

## Formal coverage and next step

M5's `rejectConflictingDuplicateVertex` is now the approved contract, not a
provisional model. Its deterministic scenario and six-step bounded check pass,
and the M5 Rust adapter (`reject_duplicate`) replays the atomic rejection
against both InMemory and MinIO. No further model, adapter, or source change is
required; the contract and implementation agree.

## Review decision

Reviewed 2026-07-18 (vyom@hydradb.com). **Decision: reject conflicts
atomically** — duplicate vertex rows with equal values coalesce; any conflicting
mutable property value rejects the whole batch atomically (no arbitrary winner,
no last-row-wins). This matches the current implementation. Status transition:
`blocked` → `not-a-bug`.
