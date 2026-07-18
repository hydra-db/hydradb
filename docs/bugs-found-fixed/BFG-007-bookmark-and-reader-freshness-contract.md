---
id: BFG-007
title: Remote bookmark proof and read-only freshness are unspecified
status: blocked
severity: P2
classification: freshness-contract-gap
introduced_or_first_bad_commit: 215bed9-and-e875387
fix_commit: none
model:
  - quint-models/turbolay/m2_snapshot_read.qnt
  - quint-models/turbolay/m4_placement_fence.qnt
current_verified_commit: f45662c
date_opened: 2026-07-18
date_verified: null
tags: [bugs, bookmarks, readers, replicas, freshness, quint]
---

# BFG-007: bookmark proof and read-only freshness

## Status

Blocked on product contract. The analysis found that remote clients may not provide the epoch proof used by bookmark validation, and a checkpoint-pinned reader has no stated refresh or staleness guarantee.

## Intended behavior

At minimum, a bookmark must not regress within one principal/scope and must be either provably satisfied or return a typed unsupported/proof-unavailable error. Any read-only freshness guarantee must state whether it is none, bounded staleness, monotonic reads, or read-your-writes after a bookmark.

## Reproduction to add

Use a remote client to write, retain its bookmark, and read through a separate client/reader. Record the epoch-proof response and reader visibility before and after reopen/refresh. Repeat after a routed-owner takeover.

## Impact

Applications can assume a causal read guarantee that the remote path cannot prove, or indefinitely read an old checkpoint without an explicit contract.

## Formal coverage and next step

M2 checks bookmark monotonicity and M4 checks safe ownership transfer. Neither claims freshness. Their bounded checks pass as safety-only evidence. Implement a model/MBT action only after the selected remote epoch and reader-refresh contract is approved.

## Review decision

Required: choose the remote bookmark error/success contract and the read-only freshness SLA, if any.
