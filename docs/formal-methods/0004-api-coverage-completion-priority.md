---
title: "Formal Methods 0004: Remaining API Coverage and MBT Completion Priority"
status: implementation-in-progress
date: 2026-07-18
depends_on:
  - 0002-turbolay-quint-specification-plan.md
  - 0003-turbolay-quint-verification-evidence.md
tags: [quint, mbt, api-coverage, priority, jepsen]
---

# Remaining API coverage and MBT completion priority

## Purpose

This is the implementation order for the API surface left out of the first
Quint slice. Ranking uses correctness impact and expected read/write-path use,
not production usage telemetry. Each row requires a Quint action/invariant,
at least one deterministic scenario, a Rust conformance test, and—where
applicable—a Quint Connect trace replay.

## Contract decisions used for this completion pass

These defaults preserve the current safest observable behavior. They are
explicit so a later product decision can deliberately change both model and
implementation rather than leaving an accidental contract.

| Finding | Completion default |
|---|---|
| BFG-003 | An external relationship ID is cell-global. A different endpoint using it is rejected; no silent alias is legal. |
| BFG-004 | Conflicting duplicate vertex rows reject the whole atomic batch; equal duplicates coalesce. |
| BFG-007 | Bookmark safety is monotonic/provable-or-error. Read-only handles have no freshness or read-your-writes promise. |
| BFG-008 | Low-level offset page calls are explicitly best-effort across requests. A server-materialized cursor is stable. |

## Ranked implementation groups

### P0 — hot-path correctness

| Rank | Surface | Quint/MBT completion target |
|---:|---|---|
| 1 | Relationship `MERGE` identity scope | Model cell-global identity, same-ID/different-endpoint rejection, retry result, and Cypher/direct conformance. |
| 2 | Batch mutation and duplicate vertex conflicts | Model equal coalescing, conflicting atomic rejection, durable result, and idempotent retry. |
| 3 | Direct paged query contract | Model a per-request snapshot and explicit mutation-between-pages best-effort behavior; separately prove materialized cursor stability. |
| 4 | Metadata `SET` / `REMOVE` | Model metadata update/removal as an atomic record/index projection that does not change structural adjacency. |

#### P0 completion evidence

The first kernel-contract pass is implemented on `Turbolay-V3`: M2/M5 contain
the actions and deterministic witnesses; each has passed a 10,000-sample,
12-step Quint simulation and a six-step Apalache check. The focused Rust tests
are named `formal_p0_*` in `src/tests.rs`; relationship/duplicate testing runs
with default features, and direct pagination/metadata testing runs with
`--features opencypher`.

M1 is the first complete Quint Connect driver. M2 and M5 still need their
full action adapters before this group can be called MBT-complete; this is an
explicit remaining completion-gate item rather than an unrecorded gap.

### P1 — severe but less frequent or asynchronous paths

| Rank | Surface | Quint/MBT completion target |
|---:|---|---|
| 5 | Relationship delete with parallel relationships | Delete one relationship without removing a shared structural edge; remove it only after the final relationship. |
| 6 | Artifact publication/generation race | Pause-after-base, topology write, stale publish rejection, delta retention, and direct/matrix equivalence. |
| 7 | Remote bookmark proof and reader freshness | Typed proof-or-error result, monotonic bookmark, and an explicit safety-only lagging-reader model. |
| 8 | Bulk/trusted import unit and retry | Model stated atomic batch units, failure between units, and trusted-input precondition rejection. |

#### P1 rank 5 completion evidence

M5 now models two relationship records sharing one structural edge and the
two required deletion transitions. Its parallel-relationship witness, 10,000
sample simulation, and six-step Apalache run pass. The default-feature Rust
test `formal_p1_parallel_relationship_delete_preserves_edge_until_final_relationship`
verifies that the first delete leaves the edge and its degree intact, while the
second removes both. P1 ranks 6–8 remain pending their adapter and fault-model
work.

P1 rank 6 is covered by M3's stale-publication, retained-reader, and
matrix/direct-equivalence actions. P1 rank 7 is covered by M2's monotone
bookmark model and M4's durable-fence safety model; it intentionally does not
promise reader freshness. Both models passed their deterministic, simulation,
and bounded checks on this branch. Their Rust MBT adapters remain pending.

### P2 — lower-frequency destructive or unsupported operations

| Rank | Surface | Quint/MBT completion target |
|---:|---|---|
| 9 | `snapshot_at`, future epoch, and cursor cancellation | Current-only snapshot success, typed future/historical rejection, and cancelled cursor no-result behavior. |
| 10 | `DELETE`, `DETACH DELETE`, and cell drop | Vertex delete rejection/behavior, detach removal of all incident relationships, and no post-drop writes. |

## Completion gates

1. Extend M1–M5 with the named action, state projection, safety invariant, and
   action witness; use a fault model only where it distinguishes an actual bad
   transition.
2. Add a deterministic `*_test.qnt` scenario and a focused Rust test using
   the public kernel/service API.
3. Add each stable action to the Quint Connect driver. The driver compares the
   full normalized public projection after every trace transition.
4. Run the expanded model through Quint simulation and the six-step Apalache
   gate; generate an ITF trace for every MBT-enabled family.
5. Add corresponding Jepsen generators/nemeses only after the kernel-level
   contract and Rust replay pass.

## Deferred work

The completion default for BFG-008 does not promise repeatable direct offset
pagination. If stable direct pagination is later required, replace the token
with a snapshot-bearing or server-materialized cursor and revise M2/M5 before
changing client behavior. Likewise, a read-only freshness SLA requires a
separate graph-watermark/change-log protocol; it must not be inferred from the
safety-only M2/M4 models.
