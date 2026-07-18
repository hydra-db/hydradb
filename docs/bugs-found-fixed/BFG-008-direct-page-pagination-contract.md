---
id: BFG-008
title: Direct page and batch tokens do not pin a cross-request snapshot
status: not-a-bug
severity: P2
classification: pagination-contract-gap
introduced_or_first_bad_commit: e875387bf121292c316f6c81d5a3d3e5fdce7d04
fix_commit: none
model:
  - quint-models/turbolay/m2_snapshot_read.qnt
  - quint-models/turbolay/m5_public_commands.qnt
current_verified_commit: b1709ea
date_opened: 2026-07-18
date_verified: 2026-07-18
tags: [bugs, pagination, cursor, snapshots, quint]
---

# BFG-008: direct page pagination contract

## Status

Resolved as `not-a-bug`: the split is the approved public contract.
`QueryCursorToken` contains an offset but no storage snapshot identity. The
client-service cursor is **stable** because it materializes the complete result;
lower-level direct, TCP/distributed, and batch page calls are **best-effort**:
each page is an independent current read and may skip, duplicate, or reorder rows
after an intervening mutation. This two-tier contract was reviewed and confirmed
as intentional.

## Intended behavior

Choose one public contract: **stable pagination**, which carries a resumable snapshot/materialized cursor and returns disjoint ordered pages from one result; or **best-effort pagination**, which documents that pages are independent current reads and may skip/duplicate rows after a mutation. The BFG-001 per-page scope is required by either choice, but does not make two requests one snapshot.

## Reproduction to add

Read page one, insert/delete/reinsert rows before its offset, then read page two through direct shard, routed/TCP, distributed, and batch paths. Compare the combined rows with both the first and second snapshot to demonstrate the declared contract.

## Impact

Offset pagination can duplicate, skip, or reorder graph results across a concurrent mutation, surprising clients that treat a page token as a cursor.

## Formal coverage and next step

M2's materialized cursor is the stable service-cursor contract; its
`directPageUsesOneRequestView` invariant already encodes best-effort direct
offset pagination (a direct page observes one committed view and is deliberately
not required to equal a later request's view). M5's `openMaterializedCursor`
holds the stable boundary. The M2 adapter replays both paths. Model, adapter,
and implementation agree with the approved contract; no further change is
required beyond documenting best-effort semantics for clients.

## Review decision

Reviewed 2026-07-18 (vyom@hydradb.com). **Decision: best-effort offset
pagination** for direct, TCP/distributed, and batch page tokens — pages are
independent current reads and may skip/duplicate/reorder rows across a
concurrent mutation. The materialized client-service cursor remains stable. This
matches the current implementation and must be documented as a client-facing
contract. Status transition: `blocked` → `not-a-bug`.
