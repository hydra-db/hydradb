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
| [BFG-001](BFG-001-paged-fast-path-snapshot-scope.md) | Paged graph-kernel/streaming fast paths run before the snapshot-scoped fallback, so one page can mix storage versions while a writer commits. | **Confirmed bug** | Suspected introduction: `e875387` (bounded paged reads) | Candidate fix `b1709ea` scopes each page before dispatch; historical forced-interleaving replay remains. | M2 snapshot read |
| [BFG-002](BFG-002-unvalidated-historical-page-epoch.md) | A direct paged query with an unvalidated historical `read_epoch` can take a fast path and return current data, whereas the fallback rejects it. | **Confirmed bug** | Suspected introduction: `e875387` | `fixed-pending-review` at `b1709ea`; historical repro fails and current regression passes. | M2 snapshot read |
| [BFG-003](BFG-003-relationship-merge-identity-scope.md) | Relationship `MERGE` coalesces by external ID while lookup is endpoint-scoped, so same-ID/different-endpoint rows can conflict before lookup. | Intended idempotency contract | Exposed by `d01e32e` | **not-a-bug** (2026-07-18): external `relationship_id` is a client-supplied per-cell idempotency/dedup key; same-id/different-endpoint `IdempotencyConflict` is intended; matches implementation. | M1 cell write, M5 public commands |
| [BFG-004](BFG-004-duplicate-vertex-batch-contract.md) | Duplicate vertex rows are coalesced; conflicting property values reject the whole batch rather than last-row-wins. | Approved batch-semantics contract | `ea7ec2c` | **not-a-bug** (2026-07-18): atomic reject-on-conflict is the intended contract; matches implementation. | M1 cell write, M5 public commands |
| [BFG-005](BFG-005-adjacency-representation-lifecycle-parity.md) | Canonical adjacency and trusted outbound-only segments use different delete/reopen/artifact paths. | Lifecycle-parity risk | Pending bisect | Discovered; no divergence is claimed yet. | M1 cell write, M3 artifact/GC |
| [BFG-006](BFG-006-artifact-generation-race.md) | Artifact build, generation/dirty-marker clear, and topology write may race; a stale build must never publish or clear a newer generation. | Concurrency risk | Present in V2 analysis; exact origin pending bisect | Discovered; requires a forced race repro. | M3 artifact/GC |
| [BFG-007](BFG-007-bookmark-and-reader-freshness-contract.md) | Remote bookmark proof and long-lived read-only reader freshness lack a complete validated contract. | Confirmed freshness gap | `215bed9` and `e875387` are relevant | **blocked** (2026-07-18): approved read-your-writes; local met + typed error exists; remote proof + reader refresh scoped (refresh may need a SlateDB bump). | M2 snapshot read, M4 placement/fence |
| [BFG-008](BFG-008-direct-page-pagination-contract.md) | Lower-level page and batch APIs can re-execute a later page after intervening writes; unlike the client-service cursor, they do not retain a stable result. | Approved pagination contract | Suspected introduction: `e875387` | **not-a-bug** (2026-07-18): best-effort direct pages, stable service cursor; matches implementation. | M2 snapshot read, M5 public commands |
| [BFG-009](BFG-009-epoch-scoped-read-unpinned-composition.md) | Epoch-scoped reads compose over unpinned live state; segment re-append deletes tombstone keys and the point-edge branch is unfiltered, so acknowledged deletes are retroactively erased and reads return post-epoch data — 17% of current-epoch reads contradict acknowledged history under concurrent re-inserts. | **Confirmed bug** (three failing e2e regressions at `a43ec61`, `--ignored` in `src/tests.rs`) | Pending bisect | Open (2026-07-22); fix directions recorded, none applied. | M2 snapshot read |
| [BFG-010](BFG-010-trusted-append-fingerprint-resurrects-deletes.md) | Trusted segment append short-circuits on a content fingerprint only while every fingerprinted edge still exists; once one has been deleted it falls through to the insert path and deletes the segment tombstone, so replaying an already-accepted import under a fresh idempotency key resurrects an acknowledged delete. | **Confirmed bug** (one failing e2e repro at `36a38a6`, `--ignored` in `src/tests.rs`; the same-key boundary test passes and pins the hazard) | Pending bisect | Open (2026-07-22); fix directions recorded, none applied. One contract question flagged for the user. | M1 cell write (model pending) |

**Found is not fixed.** This directory records findings; the folder name covers
both halves of the lifecycle. Read the per-record frontmatter, not the folder
name: a finding is fixed only when `status` is `fixed-pending-review` or
`fixed` *and* `fix_commit` names a SHA. As of 2026-07-22 exactly one record
(BFG-002) carries a fix commit; every other record has `fix_commit: none` or
`pending` and no fix applied. See
[validation-protocol.md](validation-protocol.md#found-versus-fixed-how-to-read-the-metadata)
for the field-by-field legend.

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
