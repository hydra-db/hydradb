---
id: BFG-005
title: Canonical and outbound-segment adjacency paths may diverge
status: discovered
severity: P2
classification: lifecycle-parity-risk
introduced_or_first_bad_commit: pending-bisect
fix_commit: none
model:
  - quint-models/turbolay/m1_cell_write.qnt
  - quint-models/turbolay/m3_artifact_gc.qnt
current_verified_commit: f45662c
date_opened: 2026-07-18
date_verified: null
tags: [bugs, adjacency, artifacts, reopen, quint]
---

# BFG-005: adjacency representation lifecycle parity

## Status

Discovered as a source-review risk. Canonical edge records and trusted outbound-only segments use different write/delete/reopen/artifact paths; no divergence has yet been reproduced.

## Intended behavior

For the same logical mutation history, canonical adjacency and any trusted segment representation expose the same edge existence, ordered neighbors, degrees, direct traversal result, and matrix result after reopen/refresh.

## Reproduction to add

Exercise create → delete → reinsert → reopen → artifact refresh for both representations. Compare direct snapshot reads and matrix traversal after every phase, then repeat with an interrupted refresh.

## Impact

Representation-specific lifecycle drift can produce missing or stale neighbors and matrix answers even when canonical records are correct.

## Formal coverage and next step

M1 models a mutation's normalized projection. M3 requires artifact queries to equal canonical state and rejects stale publication. Both pass their bounded checks, but a representation-selecting Rust/MinIO trace is still required.

## Review decision

Pending reproduction and bisect; do not classify as fixed or as a confirmed implementation bug before a mismatch is observed.
