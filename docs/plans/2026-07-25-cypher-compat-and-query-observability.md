---
title: Cypher compatibility inventory and query observability
status: draft-for-review
date: 2026-07-25
branch: Turbolay-V3.5
base_commit: ea942ec
tags:
  - opencypher
  - query-optimizer
  - observability
  - staging
---

# Cypher compatibility inventory and query observability

Goal: find out which production Cypher Turbolay can actually run, stop staging
deletes from failing at the 30s wall, and make the next query regression
diagnosable from logs instead of from a code read.

This plan deliberately does **not** fix the query planner. Predicate pushdown,
the cost-model fallback inversion, and the per-backend query split are separate
work that should not start until the inventory says what is actually broken.

## Sources

The analysis this plan rests on is not written down anywhere else yet; the
following are the files it was derived from.

**Turbolay (this repo)**

- `src/shard/query_optimizer.rs` — `best_row_node_access` (:391) and
  `best_row_edge_access` (:459) hold the entire index-selection policy.
  `EdgePropertyIndex` is only ever considered from `edge.properties`, the inline
  map on the relationship pattern (:522). No WHERE-predicate pushdown exists.
  `FullEdgeScan` + `RowQueryOptimizerPass::FullScanFallback` is the fallthrough
  (:565). `explain_row_query_plan_with_stats` (:19) already builds a complete
  `RowQueryPlan`.
- `src/query/opencypher.rs` — the lowering. `row_aggregate_function` (:2257)
  recognises exactly one function, `count`. `parse_opencypher_row_query` (:263)
  and `parse_opencypher_mutation_query_with_parameters` (:274) are the public
  entry points, both re-exported from `src/lib.rs:132`.
- `src/client/service.rs` — `effective_runtime_limit_ms` (:764) **rejects**
  rather than clamps a client timeout above the server cap.
  `client_query_runtime_exceeded` (:2115) is the error in the staging log.
- `src/client/bolt.rs` — `remaining_bolt_runtime_ms` (:1318) recomputes the
  budget per PULL, which is why the error reports 29999 and not 30000.
- `src/bin/graph_node/config.rs:203` — `GRAPH_MAX_QUERY_RUNTIME_MS`, default
  30_000.
- `charts/turbolay/templates/configmap.yaml:52` ← `charts/turbolay/values.yaml:151`
  (`runtime.maxQueryRuntimeMs`). Delivered by `envFrom: configMapRef`, so it
  does not appear in the StatefulSet's inline `env`.
- `src/query/corpus.rs` and `examples/opencypher_tck_report.rs` — the existing
  parse-a-corpus-and-emit-JSON harness shape to copy for the inventory.

**cortex-ingestion** (`../../cortex-ingestion`)

- `utils/graph/mutate.py` — `delete_source_from_kg` and its eight step helpers.
  `_delete_relationships_by_chunk_ids` (:355) carries a long docstring
  explaining a FalkorDB `GRAPH.EXPLAIN` v4.2.1 tuning — anonymous `()` endpoints
  plus a post-`LIMIT` `startNode`/`endNode` guard — that is exactly the shape
  Turbolay cannot index. `_DEFAULT_STEP_TIMEOUT_SECONDS = 120.0` (:22),
  `_DELETE_CHUNK_BATCH_SIZE = 500` (:26), `_DELETE_EDGE_BATCH_SIZE = 1000` (:32).
  The failing log line is `logger.error("Error deleting source ...")` at :346.
- `core/db/graph/turbolay.py` — `execute_query` (:449). `query_timeout_seconds`
  feeds `asyncio.wait_for` only (:479); it is never set as a Bolt transaction
  timeout, so the server always applies its own default.
- `core/db/graph/factor.py:13` — backend selection. Only the driver is swapped;
  the Cypher text in `utils/graph/` is shared across both backends.
- `config/settings.py:218` — `TURBOLAY_QUERY_TIMEOUT_SECONDS = 150`.

**hydradb-application** (`../../hydradb-application`)

- `application/internal/platform/falkordb/indexes.go` — `baseIndexQueries` (:35)
  and `CreateIndexes` (:88), which no-ops on `backendTurbolay`. This is the
  correct behaviour: Turbolay has no `CREATE INDEX` DDL and indexes every
  property on write (`src/shard/write.rs:4711`).
- `application/internal/platform/falkordb/turbolay_*.go` — the per-backend query
  set. `turbolay_mutations.go:139` is the correctly-shaped Turbolay delete, and
  the model for what cortex-ingestion is missing.
- `application/internal/platform/falkordb/delete.go:66` — the FalkorDB-only
  delete, with the PRO-1064 comment describing the same planner trap from the
  FalkorDB side.

**Staging**

- Namespace `turbolay-staging`: 3 × `turbolay-staging-node`, 1 × indexer, all
  Running, no restarts. Logs carry only slatedb checkpoint/GC lines — there is
  no query-level logging at any level, which is the gap Step 3 closes.
- `../../hydradb-argocd/infra/environments/staging/turbolay/values.yaml` — the
  staging Helm values.

## The failure being explained

```
Error deleting source 693cdeab... after 30.28s:
{neo4j_code: Neo.ClientError.Transaction.Terminated}
{message: client_query_runtime exceeded query timeout after 29999 ms; limit is 29999 ms}
```

`29999` is Turbolay's own budget, not a client setting: 30_000 default minus the
millisecond consumed before the first PULL. The client's 120s step timeout and
150s driver timeout are both client-side only and never reach the server.

What we know: some query in `delete_source_from_kg` ran for the full server
budget. What we do **not** know is which one, because nothing logs it. The
prime suspect is a `FullEdgeScan` fallthrough, but
`_delete_relationships_by_chunk_ids` — the most obvious candidate — calls
`startNode`/`endNode`, which the lowering does not recognise, so it should be
rejected outright rather than time out. Either it never reaches Turbolay in
that form, or a different step is the culprit. Step 1 settles this.

## Step 1 — Cypher parse inventory

Cheapest step, highest information. May show the ingestion delete path has never
worked on Turbolay at all, which would reorder everything below.

1. Extract every Cypher string literal shipped to the graph from both callers:
   - `../../cortex-ingestion/utils/graph/*.py` — `execute_query(query=...)` and
     the f-string variants.
   - `../../hydradb-application/application/internal/platform/falkordb/*.go` —
     both the FalkorDB set and the `turbolay_*.go` set, kept separate so the
     comparison is meaningful.
   Store them as a JSON corpus (id, source file, line, backend the caller
   intends, query text, parameter names) under `docs/` or `bench/`, not inline
   in the harness — the corpus is data and will be regenerated.
2. Add `examples/cypher_compat_report.rs`, modelled on
   `examples/opencypher_tck_report.rs`. For each entry call
   `parse_opencypher_row_query_with_parameters` or
   `parse_opencypher_mutation_query_with_parameters` (both public from
   `src/lib.rs:132`) and classify: **parses**, **rejected**
   (`GraphError::UnsupportedQuery`, capture `dialect` + `feature` verbatim), or
   **parse error**.
3. Emit JSON plus a markdown table grouped by rejection reason, so the output is
   a work-list — "these 6 queries need `startNode`/`endNode`, these 3 need
   parameterised `LIMIT`" — rather than a pass/fail number.

Parse-only, no cluster and no seeded data. This runs as a plain `cargo run
--example`.

**Done when** every production query is classified, and each rejection reason is
either an open Turbolay gap with a named missing feature or a caller-side query
that needs a Turbolay-specific rewrite.

## Step 2 — Timeout stopgap

One line, and only worth doing to buy room for Step 3.

- `../../hydradb-argocd/infra/environments/staging/turbolay/values.yaml`, add
  `runtime.maxQueryRuntimeMs: 120000` to match the callers' 120s step budget.
  The key already flows through `charts/turbolay/templates/configmap.yaml:52`;
  no chart or code change is needed.

Caveats to state in the PR rather than discover later:

- This converts a fast failure into a slow one. It fixes nothing.
- Per-cell write concurrency is unverified. If writes serialise per cell, a
  120s scan blocks that tenant's writes for 120s instead of 30s. **Check this
  before merging** — if writes do serialise, cap the bump at 60s.
- It does not make the client timeout authoritative. Doing that properly means
  `core/db/graph/turbolay.py` setting a Bolt transaction timeout, and
  `effective_runtime_limit_ms` (`src/client/service.rs:764`) clamping instead of
  rejecting — otherwise a 120s client request against a 30s server becomes a
  hard `AdmissionRejected`. Both are out of scope here; note them as follow-ups.

**Done when** staging runs at the raised budget and we know whether deletes
complete or still wall — which is itself a useful signal.

## Step 3 — EXPLAIN and slow-query logging

The planner already produces everything needed. `RowQueryPlan` simply never
crosses into `src/client/` — `grep -rn "RowQueryPlan" src/client/` returns
nothing.

### 3a. Slow-query log

> **Superseded.** `docs/plans/2026-07-26-otel-telemetry-crate.md` §3 adopts this
> rule verbatim and owns its implementation, inside the `query.plan` span of the
> read-path span tree. It is kept here for context; do not implement it twice.
> Step 3b below is **not** superseded and stays owned by this plan.

Lower risk, ship first. At query completion in `src/shard/query.rs`, where
`explain_row_query_plan_with_stats` is already called (:408), emit a structured
`WARN` when either condition holds:

- elapsed exceeds a threshold (new `runtime.slowQueryMs`, default ~1000), or
- the chosen plan contains `RowQueryOptimizerPass::FullScanFallback`,
  `RowQueryAccess::FullEdgeScan`, or `RowQueryAccess::AllVertexScan` —
  regardless of elapsed time.

The second condition matters more than the first: it catches a bad plan on a
small graph *before* it becomes a timeout on a large one, which is the exact
failure mode staging just hit.

Fields: cell id, elapsed ms, access path per pattern, optimizer passes,
estimated vs actual cardinality, and a redacted query fingerprint. **Do not log
parameter values** — these carry tenant data. Log the query shape with
parameters as placeholders.

### 3b. EXPLAIN over Bolt

Accept a leading `EXPLAIN` in `src/client/bolt.rs`, plan without executing, and
return the `RowQueryPlan` as a result row. Plumbing only:
`src/shard/query_optimizer.rs:19` → `src/client/service.rs` →
`src/client/bolt.rs`. `RowQueryPlan`, `RowQueryPlanPattern`, and
`RowQueryOptimizerPass` are already `serde`-derived behind the
`query-transport` feature (`src/query/algebra.rs:802`), so the wire format is
mostly free.

This is what makes the Step 1 work-list verifiable at runtime, and what any
future "why is this slow" question starts from.

**Done when** a slow or full-scanning query in staging produces a log line
naming the access path, and `EXPLAIN <query>` over Bolt returns a plan.

## Explicitly out of scope

Deferred until Step 1 reports, listed so they are not silently forgotten:

- **Predicate pushdown** — `WHERE x.prop = $v` and `WHERE x.prop IN $list` into
  index access, surviving a `WITH` boundary. Three features, not one.
- **Cost-model fallback inversion** — vertex-property fallback estimate is `8`
  (`query_optimizer.rs:417`), edge-property is `16` (:540), so a cold cell with
  no stats rates a tenant-wide `tenant_id` seek cheaper than a chunk-scoped
  `chunk_id` edge seek. Two constants; outsized impact; affects the *correctly
  shaped* Go query in `turbolay_mutations.go:139`.
- **Per-backend query set in cortex-ingestion**, mirroring `turbolay_*.go`.
  This is the structural fix — one shared Cypher string for two planners is why
  the bug exists and why it will recur.
- **Stats lifecycle** — who publishes `query_stats_*`, what invalidates them,
  and whether staging cells have any. Without stats everything plans on the
  fallback constants above, forever.
- **`latest_snapshot` gate** — the whole edge-property-index block sits inside
  `if latest_snapshot` (`query_optimizer.rs:522`), so a non-current-epoch read
  can never use an edge index.
- **LIMIT pushdown** — `WITH r LIMIT 1000` bounds the delete, not the scan, and
  the slice loop re-scans from the top each iteration.
- **`max_query_index_candidates`** (250k, `src/shard/query.rs:4772`) aborts
  rather than degrades.
- **Partial-delete garbage** — `delete_source_from_kg` has no transaction across
  its eight steps, so a mid-way timeout leaves orphans that make the next
  attempt slower.
