---
id: BFG-008
title: Direct page and batch tokens do not pin a cross-request snapshot
status: blocked
severity: P2
classification: pagination-contract-gap
introduced_or_first_bad_commit: e875387bf121292c316f6c81d5a3d3e5fdce7d04
fix_commit: none
model:
  - quint-models/turbolay/m2_snapshot_read.qnt
  - quint-models/turbolay/m5_public_commands.qnt
current_verified_commit: b1709ea
date_opened: 2026-07-18
date_verified: null
tags: [bugs, pagination, cursor, snapshots, quint]
---

# BFG-008: direct page pagination contract

## Status

Blocked on API choice. `QueryCursorToken` contains an offset but no storage snapshot identity. The client-service cursor is safe because it materializes the complete result; lower-level direct, TCP/distributed, and batch page calls may rerun a later offset against newer data.

## Intended behavior

Choose one public contract: **stable pagination**, which carries a resumable snapshot/materialized cursor and returns disjoint ordered pages from one result; or **best-effort pagination**, which documents that pages are independent current reads and may skip/duplicate rows after a mutation. The BFG-001 per-page scope is required by either choice, but does not make two requests one snapshot.

## Reproduction to add

Read page one, insert/delete/reinsert rows before its offset, then read page two through direct shard, routed/TCP, distributed, and batch paths. Compare the combined rows with both the first and second snapshot to demonstrate the declared contract.

## Impact

Offset pagination can duplicate, skip, or reorder graph results across a concurrent mutation, surprising clients that treat a page token as a cursor.

## Formal coverage and next step

M2's cursor is the target stable semantics for materialized service cursors; M5 makes that boundary explicit. Their checks do not assert a promise for direct offset tokens. Update the model and MBT driver after the public choice.

## Review decision

Required: approve stable direct pagination or explicitly best-effort offset pagination, including all transport and batch entry points.
