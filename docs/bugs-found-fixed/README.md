---
title: Turbolay bug inventory from the V1 to V2 commit analysis
status: review-required
date: 2026-07-18
analysis_range: Turbolay-V1...Turbolay-V2
analysis_head: 94c6dbec708a4f17e47f7df846883b0474e914ca
source_commit: 31aa558a34812f36fa14f978c43f4fdaf31ee64d
tags:
  - bugs
  - chaos-testing
  - formal-methods
  - quint
---

# Turbolay V1 to V2 bug inventory

This is the short, source-backed intake from
`turbolay-v2-commit-analysis/`. It deliberately distinguishes a demonstrated
correctness bug from a review risk or an incomplete public contract. Only the
first two rows are concrete bugs demonstrated by the analysis's control-flow
evidence; the remaining rows are high-value candidates to model and either
reproduce or close by a documented contract.

| ID | Finding | Classification | First bad/finding commit | Current state | Quint family |
|---|---|---|---|---|---|
| BFG-001 | Paged graph-kernel/streaming fast paths run before the snapshot-scoped fallback, so one page can mix storage versions while a writer commits. | **Confirmed bug** | Suspected introduction: `e875387` (bounded paged reads) | Open; current page entry still dispatches fast paths before complete-read snapshot scope. | M2 snapshot read |
| BFG-002 | A direct paged query with an unvalidated historical `read_epoch` can take a fast path and return current data, whereas the fallback rejects it. | **Confirmed bug** | Suspected introduction: `e875387` | Open; public page entry has no equivalent pre-dispatch validation. | M2 snapshot read |
| BFG-003 | Relationship `MERGE` coalesces by external ID while lookup is endpoint-scoped, so same-ID/different-endpoint rows can conflict before lookup. | Risk / identity-contract gap | Exposed by `d01e32e`; exact first bad commit pending bisect | Open; requires an explicit scope decision and a repro. | M1 cell write, M5 public commands |
| BFG-004 | Duplicate vertex rows are coalesced; conflicting property values reject the whole batch rather than last-row-wins. | Observable batch-semantics gap | `ea7ec2c` | Needs API contract confirmation; not yet a defect. | M1 cell write, M5 public commands |
| BFG-005 | Canonical adjacency and trusted outbound-only segments use different delete/reopen/artifact paths. | Lifecycle-parity risk | Pending bisect | Chaos-covered risk; no divergence is claimed yet. | M1 cell write, M3 artifact/GC |
| BFG-006 | Artifact build, generation/dirty-marker clear, and topology write may race; a stale build must never publish or clear a newer generation. | Concurrency risk | Present in V2 analysis; exact origin pending bisect | Open proof/repro task. | M3 artifact/GC |
| BFG-007 | Remote bookmark proof and long-lived read-only reader freshness lack a complete validated contract. | Availability/freshness contract gap | `215bed9` and `e875387` are relevant | Open design work; no bounded staleness or remote read-your-writes guarantee is claimed. | M2 snapshot read, M4 placement/fence |
| BFG-008 | Lower-level page and batch APIs can re-execute a later page after intervening writes; unlike the client-service cursor, they do not retain a stable result. | Pagination-contract risk | Suspected introduction: `e875387` | Open; requires a stable-page contract or explicit best-effort semantics. | M2 snapshot read, M5 public commands |

## V1 baseline and historical reproduction

`Turbolay-V1` is the architectural baseline: it used graph-level controller/
lease coordination and historical graph reconstruction, whereas V2 moved
writer fencing and read snapshots to SlateDB. It is not automatically the
right reproduction target for every finding. For example, BFG-001, BFG-002,
and BFG-008 arise from V2's paged-read implementation and should be reproduced
against the first V2 commit that contains that path, then shown absent at its
fix commit. The validation record must name both commits explicitly.

The analysis sources are:

- `book/turbolay-v2-commit-analysis.typ` — executive analysis and the combined
  “Key bugs and review risks” table.
- `turbolay-v2-commit-analysis/review-findings.md` — detailed evidence and
  suggested tests.
- `turbolay-v2-commit-analysis/commit-timeline.md` — V1 to V2 commit map.

See [validation-protocol.md](validation-protocol.md) for the required
model-to-history validation record before any item is called fixed.
