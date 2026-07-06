---
title: "turbolay — FalkorDB-style GraphBLAS layer: experiment sketch"
date: 2026-07-05
kind: design-note (pre-RFC; feeds RFC 0009 when its trigger fires)
status: recorded for later — NOT v0 scope
related:
  - ../dgraph-alignment.md
  - ../goals.md
  - ../rfcs/0009-csr-adjacency-and-joins.md
  - ../rfcs/0017-observability-and-metrics.md
---

# FalkorDB-style GraphBLAS on top of turbolay

**The idea:** read posting lists from S3, materialize them as sparse boolean
matrices in reader RAM, and execute traversals FalkorDB-style — a hop is a
masked sparse matrix-vector multiply (frontier × Aᵖ), a k-hop is k
multiplies, an intersection is an element-wise op.

**One reframe first.** This accelerates **reads/traversals, not writes**. The
write path is already a thin durable batch append and stays untouched — in
fact that's the design's main virtue: FalkorDB needs delta matrices
*precisely because* mutating CSR in place is expensive; by keeping matrices
off the writer entirely, turbolay's write latency is protected by
construction. What "load into RAM" buys is the read side: zero S3 round trips
per hop once warm, and batch frontier expansion instead of N+1 point-gets.

## Why the architecture is already shaped for this

Verified against the FalkorDB checkout (`../FalkorDB`):
`Delta_Matrix` = main `GrB_Matrix` **M** + `delta_plus` (pending adds) +
`delta_minus` (pending deletes) + a transposed twin
(`src/graph/delta_matrix/delta_matrix.h:110-112`), with queries reading
M ⊕ DP ⊖ DM and a periodic sync folding deltas into M.

That is **exactly turbolay's read plan** (RFC 0001/0007): materialized state
to watermark `W` + changelog tail `(W, latest]` overlaid at query time +
periodic advance of `W`. Mapping:

| FalkorDB | turbolay equivalent |
|---|---|
| `M` (main matrix) | matrix snapshot built from posting lists at watermark `W` |
| `delta_plus` / `delta_minus` | changelog tail `(W, latest]` (UpsertEdge/DeleteEdge records) |
| `Delta_Matrix_sync` (fold deltas) | rebuild/advance snapshot to a newer `W` |
| transposed twin | built from `EdgeIn` (already materialized, D10) |
| one matrix per relation type + label matrices | one matrix per `pred_id` (predicate sharding is already the unit) + label bitmaps |

Other things already in place: dense u64 uids (D5 — matrices need integer
indices; SuiteSparse hypersparse handles 2^60 dims, so uid-as-index works
even with holes), unconditional reverse projections (transpose for free),
SlateDB O(1) checkpoints (consistent point-in-time scan to build a snapshot
without blocking reads), and the stateless-reader rule (`goals.md`: the
matrix is a killable RAM cache, never durable state — object-native holds).

## What would need to change

**Nothing durable.** No keyspace change, no format change, no writer change.
S3 posting lists stay the source of truth. The changes are all reader-side:

1. **Snapshot builder** — per-predicate scan of `EdgeOut` (+ parts) →
   `GrB_Matrix` per `pred_id`; label matrices from node records; built from a
   SlateDB checkpoint at watermark `W`.
2. **Executor seam** (the one thing to keep clean *now*, in M2/M3): hop
   execution behind a single surface —
   `expand(frontier, pred, dir) -> frontier` — with the posting-list
   implementation (point-gets + roaring) as the default backend and the
   matrix implementation as an alternative. Var-length = repeated expand with
   a visited-mask difference. This is RFC 0009's adjacency surface; GraphBLAS
   is an alternative materialization *behind* it, not a new architecture.
3. **Tail overlay** — apply changelog records `(W, latest]` as DP/DM masks at
   query time (bounded by lag, same correctness argument as the index plan).
4. **Library choice** (experiment decides): SuiteSparse:GraphBLAS via FFI
   (what FalkorDB uses — mature masks/semirings, C build complexity) vs
   Rust-native CSR + roaring hybrid (no FFI; enough for boolean semirings,
   which is all v0 traversal needs). Start the spike with SuiteSparse to get
   the ceiling, then decide if a Rust subset reaches it.

## How to run the experiment (when triggered)

- **Gate:** RFC 0017 Phase 3 — real-S3 baselines for the posting-list
  executor must exist first. No baseline, no experiment (D12; LocalStack
  forbidden for this).
- **Shape:** additive spike on a reader — build matrices for the RAG-KG
  shadow dataset, run the same query set through both backends.
- **Correctness harness:** shadow set-diff, both backends against each other
  (must be 0 diff at equal `W`+tail) and against FalkorDB itself (harness
  exists in `../turbolay-poc/shadow/`, 0 missing / ≤5% extra).
- **Fold results into RFC 0009** — this note is input to that RFC, not a
  parallel track. Trigger unchanged: traversal latency measured too high on
  real S3.

## Key things to measure (against the posting-list executor)

| Metric | Question it answers |
|---|---|
| `query_phase{hop, frontier_expand}` p50/p99, cold + warm, per backend | The headline: does SpMV beat batched point-gets, and when? |
| Hop latency **as a function of frontier size** | The break-even curve — SpMV should win on large frontiers, lose on anchor-heavy point lookups. If RAG-KG queries stay small-frontier, matrices may never pay off; that is a valid outcome. |
| `objstore_requests_total` per query, warm | N+1 elimination — should drop to ~0 for matrix-backend hops |
| Snapshot build: wall time, S3 GETs, bytes scanned per predicate | Cold-start cost; how expensive is `W` advancement |
| RSS: bytes/edge in matrix vs roaring posting cache | Memory budget at 1–10M nodes (should be comfortable; verify) |
| Tail-overlay cost vs lag (entries applied per query, DP/DM nnz) | The freshness tax; how often sync must run |
| **Guardrail:** writer commit p99 | Must not move at all — the writer is untouched by design |
| **Guardrail:** shadow set-diff | 0 diff between backends at equal freshness |
| EdgeProp point-get overhead in faceted traversals (both backends) | Ties into `dgraph-alignment.md` §4b — matrices don't carry facets either; the companion-read cost is backend-independent |

## What this is not

- Not a v0 workstream. Nothing in Wave 4 or M2/M3 depends on it.
- Not a durable-format change — CSR-as-durable-format (the namidb path)
  remains rejected; this is a RAM materialization over the existing keys.
- Not a multi-writer or consistency change — tokens, watermark, and tail
  semantics are identical across backends.
