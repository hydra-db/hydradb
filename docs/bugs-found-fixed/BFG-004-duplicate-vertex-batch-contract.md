---
id: BFG-004
title: Duplicate vertex batch rows have an undocumented conflict policy
status: blocked
severity: P2
classification: batch-semantics-contract-gap
introduced_or_first_bad_commit: ea7ec2c
fix_commit: none
model:
  - quint-models/turbolay/m1_cell_write.qnt
  - quint-models/turbolay/m5_public_commands.qnt
current_verified_commit: f45662c
date_opened: 2026-07-18
date_verified: null
tags: [bugs, batch, vertices, semantics, quint]
---

# BFG-004: duplicate vertex batch conflict policy

## Status

Blocked on API-contract review. V2 coalesces duplicate vertex IDs, but rejects a batch when their mutable property values conflict. That is a valid possible contract, not automatically a defect.

## Intended behavior

The public batch API must state one deterministic rule for duplicate vertex rows: reject conflicts atomically, first-row-wins, last-row-wins, or a defined merge. The chosen rule must be identical across direct ingestion and public Cypher/batch adapters where they claim equivalent semantics.

## Reproduction to add

Send a two-row batch with the same vertex ID and equal values, then repeat with conflicting values. Capture mutation result, durable vertex properties, epoch, and idempotent retry result for each entry point.

## Impact

Without a documented rule, clients can accidentally depend on input ordering or receive whole-batch rejection while expecting upsert behavior.

## Formal coverage and next step

M5's `rejectConflictingDuplicateVertex` is an explicit provisional model of the observed behavior. Its scenario and six-step bounded check pass. Rust MBT must change with the approved contract rather than treating this provisional rejection as an implementation proof.

## Review decision

Required: approve the conflict-rejection rule or choose a different merge rule.
