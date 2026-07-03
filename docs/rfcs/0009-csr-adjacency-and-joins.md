---
title: "RFC 0009: CSR Adjacency & Worst-Case-Optimal Joins"
status: planned (stub — flesh out when triggered)
date: 2026-07-03T00:00:00Z
related:
  - 0000-rfc-index.md
  - 0005-posting-list-substrate.md
  - 0007-opencypher-read-path.md
---

# RFC 0009: CSR Adjacency & Worst-Case-Optimal Joins

**Status:** planned stub. The optimization path deferred from D3/Q1. Fleshed out only when RFC 0017 Phase-3 real-S3 baselines show posting-list traversal / first-hop latency over budget.

## Summary (to expand)
Add a materialized CSR (sorted src → contiguous partner slice) as an **opt-in secondary adjacency layout** for hot predicates, plus worst-case-optimal (leapfrog triejoin) multi-way traversal on top of it. The default roaring posting-list model (RFC 0005) is untouched; CSR is an additive `format`-tag variant.

## Will contain
- CSR generation format as `PostingValue format = 3` (offsets/partners), built by a materializer from the roaring adjacency; per-predicate opt-in.
- The per-part min/max/card skip metadata (already reserved in RFC 0005 `PartRef`) exploited for range-skipping.
- Leapfrog triejoin over sorted partner slices for multi-hop/cyclic patterns (AGM-bounded), added to the RFC 0007 planner as an alternative physical operator.
- Ordered adjacency (the Q4 deferral): composite-edge-key / sortkey variant for natively-ordered edges (weight/timestamp) without materialize-then-sort.
- Cold-open + first-hop latency budget on real S3; before/after matrix vs the RFC 0017 baseline.

## Trigger
RFC 0017 Phase 3 shows traversal or first-hop latency over budget (hard gate).
