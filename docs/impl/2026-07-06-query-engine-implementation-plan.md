---
title: "turbolay Query Engine — Implementation Plan (M2 + M3)"
status: draft
date: 2026-07-06
author: vishal
related:
  - ../rfcs/0001-strong-consistency-model.md
  - ../rfcs/0006-index-framework.md
  - ../rfcs/0007-opencypher-read-path.md
  - ../rfcs/0008-http-service-and-fleet.md
  - ../plan.md
  - 2026-07-06-review-and-bench-summary.md
---

# turbolay Query Engine — Implementation Plan (M2 + M3)

## 0. Purpose & scope

Take turbolay from "M1 write path + storage primitives, no way to query" to a
**working read query engine**: declare indexes, plan and execute the openCypher
read subset (RFC 0007 §2), serve it over HTTP through a reader fleet, and
shadow-verify it against FalkorDB.

This plan implements **RFC 0006** (index framework), **RFC 0007** (openCypher
read subset, planner, read path), and **RFC 0008** (HTTP service + fleet), on top
of the **RFC 0001** consistency contract. It is the concrete build sequence for
milestones **M2** (index framework + read path) and **M3** (Cypher
planner/executor + HTTP fleet).

Out of scope (deferred, unchanged): Cypher writes (RFC 0011), full openCypher /
aggregations / `WITH` / subqueries (RFC 0013), CSR / WCOJ joins (RFC 0009),
bitpacked frames (RFC 0010), fulltext/vector/geo indexes (RFC 0015).

## 1. Current state (verified 2026-07-06)

**Exists (M0/M1):**
- Keyspace + order-preserving encodings (`src/serde/`), posting-list substrate
  (`src/posting.rs`, `src/posting_ops.rs`), merge operator (`src/merge.rs`),
  UID/xid allocation (`src/ids.rs`), node/edge/changelog codecs (`src/value.rs`),
  single-writer write path (`src/write.rs`), per-namespace storage wrapper
  (`src/storage.rs`), observability spine (`src/obs.rs`).
- Read-side primitives already usable by a query engine:
  - `posting_ops::neighbors(storage, uid, pred, dir, &deleted_nodes)` — one hop,
    folds in deleted-node/edge subtraction (RFC 0005 read path).
  - `Writer::{get_node, lookup_uid, edge_props, degree, neighbors, storage, schema_id}`.
  - `GraphStorage::{get, scan_record_type, subscribe_durable}`.

**Missing (this plan builds it):**
- **No query engine at all** — no planner, no executor, no read path, no Cypher.
  Every `src/` mention of "planner"/"query" is a comment pointing here (e.g.
  `write.rs:313`, `write.rs:1828`).
- **No secondary indexes** — value/label/count indexes and the `IndexAm` trait
  do not exist; only the reverse projection (`EdgeIn`) is materialized.
- **No reader role** — everything runs through the RW `Writer`; there is no
  `DbReader`-backed read-only handle, no freshness gate, no HTTP service.
- **`decypher` is vendored but unwired** (zero uses in `src/`; finding L5).
- **No batched multi-get** — `GraphStorage::get` is single-key; `neighbors()` is
  called one uid at a time. This is the measured bottleneck (§2).

**Prerequisite fixes discovered in review/bench (must land before/with M2):**
- **M2-finding (torn Split read):** `neighbors()` reads a `Split` manifest then
  its parts across separate `.get()`s with no pinned snapshot — a concurrent
  reader can observe a manifest naming a part a racing split already rewrote.
  Must bind manifest+part reads to one snapshot before the concurrent read path
  lands (`docs/impl/2026-07-06-implementation-review-findings.md` M2).
- **M3-finding (`maybe_rollup` unwired):** deleted-edge bitmaps grow unbounded;
  wire rollup into a maintenance path so tail/subtraction cost stays bounded.
- **H1 (`intern()` cache poisoning):** fix before the write path feeds indexes,
  or a partially-aborted write can leave an index referencing an unpersisted id.

## 2. Benchmark-driven design constraint (measured, not assumed)

The 2026-07-06 hop × supernode-degree sweep vs FalkorDB (same in-memory tier,
same data) measured turbolay's hand-written traversals at **4–14× slower than
FalkorDB**, widening with degree. Accuracy was **240/240 exact** — correct but
slow. The bottleneck is **one storage point-read per frontier node, no batching,
no adjacency/node cache** (RFC 0007 §8 N+1). Implications this plan bakes in:

1. **Batched neighbor reads are a first-class primitive, not an afterthought**
   (WS-A2). The executor must expand a frontier with `⌈N/batch⌉` multi-gets, not
   `N` serial gets. This is the single biggest lever (RFC 0007 §8.1).
2. **A read-side cache** (node blobs + hot adjacency) is in-scope for M2/M3, not
   deferred — the bench shows uncached per-node reads dominate.
3. The FalkorDB shadow-test harness now **exists and works** (`bench/`,
   `scratchpad/*.py` from the bench session): reuse it as the M2/M3 correctness
   gate. Note the **`RESULTSET_SIZE=-1`** trap (FalkorDB truncates its own output
   at 10k by default) — the verify tooling must set it.

## 3. Target architecture (layer cake)

```
                 HTTP plane (axum)   ── RFC 0008, WS-F
  role=reader  ─────────────────────────────────────────
        │  Cypher text
        ▼
   decypher parse ── full-grammar AST ── WS-C
        │  lower (NameResolver = SchemaCache)          → unsupported_cypher / malformed_cypher
        ▼
   PlanNode IR (ScanBy*/Expand/VarExpand/Filter/Intersect/Union/Difference/Project/Sort/Limit) ── WS-C
        │
        ▼
   Planner (anchor = smallest live index; hops; var-length BFS; smallest-first) ── WS-D
        │  bound plan
        ▼
   Executor / read path ── WS-E
     freshness gate (session token) ─┐
     index+adjacency to W  ──────────┤ roaring set algebra (RFC 0005)
     deleted subtraction   ──────────┤ batched multi-get (WS-A2)
     changelog tail (W,latest] ──────┤ + read cache (WS-A3)
     merge → fetch Node[uid] → sort/skip/limit
        │
        ▼
   GraphRead (read-only trait) over DbReader ── WS-A1
        │
        ▼
   SlateDB / S3   (writer maintains indexes synchronously — WS-B in write.rs)
```

## 4. Workstreams

Each deliverable lists: **new/changed files**, **what**, **acceptance**. `[dep: …]`
marks prerequisites.

---

### WS-A — Reader storage foundation

Goal: a read-only handle with the primitives the executor needs. **Blocks
everything** (planner/executor read through it).

**A1. `GraphRead` trait + `DbReader`-backed reader** — `src/read/mod.rs`,
`src/read/reader.rs`; extend `src/storage.rs`.
- Define `trait GraphRead` = the read surface the executor consumes:
  `get(key) -> Option<Bytes>`, `multi_get(keys) -> Vec<Option<Bytes>>`,
  `scan(range)`, `neighbors(...)`, `get_node(uid)`, `lookup_uid(xid)`,
  `subscribe_durable()`, plus `schema()` access. Both the `Writer` (for
  read-your-writes tests) and the new reader implement it.
- Add `GraphReader` opening the namespace via `common::create_storage_read`
  (`vendor/common/src/storage/factory.rs:400` — `DbReader`, unfenced,
  manifest-polling) with the **same merge operator registered** (else
  merge-operand reads fail — see `storage.rs:34` note).
- **Reader-side schema cache:** the reader rebuilds `SchemaCache` from stored
  `SchemaId` records at open — but the writer keeps interning new names after
  that. On a name-resolution **miss**, the reader must re-scan the schema
  keyspace once before concluding; a label/pred/prop that truly doesn't exist
  resolves to the **empty set** (a `MATCH` on an unknown label matches nothing —
  not an error), mirroring Cypher semantics.
- **Acceptance:** open a namespace written by a `Writer`, read a node back
  through `GraphReader` (RFC 0004 acceptance #3 shape, but via the reader path);
  a query naming a predicate interned *after* reader open still resolves
  (schema-refresh-on-miss regression).

**A2. Batched multi-get + batched `neighbors`** — `src/storage.rs`,
`src/posting_ops.rs`.
- `GraphStorage::multi_get(keys: Vec<Bytes>) -> Vec<Option<Bytes>>`. **Verified:
  neither `common::StorageRead` nor SlateDB exposes a native batch-get** — so
  this is a new helper: a **bounded-concurrency fan-out** over `get()`
  (`futures::stream::buffer_unordered(k)`, k ≈ 32–64, tunable), which converts N
  serial awaits into ⌈N/k⌉ overlapped rounds served by SlateDB's block cache +
  bloom filters. (If profiling shows per-call overhead dominating, adding a real
  multi-get to the *vendored* common is in-bounds — it is our fork, not SlateDB.)
- `posting_ops::neighbors_batch(reader, frontier: &RoaringTreemap, pred, dir,
  &deleted_nodes) -> RoaringTreemap`: build all `EdgeOut/EdgeIn[u][pred]` keys
  for `u ∈ frontier`, resolve them via `multi_get`, roaring-union the decoded
  posting lists (folding in deleted subtraction). `Split` postings contribute
  their part keys to the same batch (second round for parts discovered from
  manifests).
- **Acceptance:** a frontier expansion issues **O(1) logical batch rounds, not
  O(N) sequential awaits**, asserted via the RFC 0017 N+1 fan-out counter
  (`obs.rs`); result equals the serial `neighbors()` loop over a random graph.
  [dep: A1]

**A3. Snapshot-consistent reads + read cache** — `src/storage.rs`,
`src/read/cache.rs`.
- Bind a query to **one storage snapshot** so a `Split` manifest and its parts
  are read consistently (fixes the M2 torn-split-read finding). **Verified: the
  substrate already exists** — `common::Storage::snapshot() -> Arc<dyn
  StorageSnapshot>` (`vendor/common/src/storage/mod.rs:437`, SlateDB `DbSnapshot`
  underneath); the work is plumbing it through `GraphStorage`/`GraphRead` and
  making `neighbors()`/`neighbors_batch` read manifest+parts through one
  snapshot handle.
- `ReadCache`: a bounded cache (foyer, already a dep) of decoded `NodeRecord`
  blobs and hot adjacency posting lists, keyed by uid/adjacency-key, scoped to a
  reader (invalidated on manifest advance). Directly targets the bench's
  uncached-per-node cost.
- **Acceptance:** torn-split-read regression (concurrent split during a scan
  returns consistent members, no `key is missing` error); cache hit path
  measured in `obs`.  [dep: A1, A2]

---

### WS-B — Index framework (RFC 0006)

Goal: declarable secondary indexes, maintained synchronously by the writer,
backfilled asynchronously, consulted by the planner. **Blocks the planner's
anchor selection.**

**B1. `IndexAm` trait + registry + watermark** — `src/index/mod.rs`,
`src/index/registry.rs`.
- `trait IndexAm { extract(change,&schema)->Vec<IndexMutation>;
  apply(&muts,&mut batch); supports(&PredicateIR)->Option<AccessPlan>;
  execute(&AccessPlan,&dyn GraphRead)->RoaringTreemap }` (RFC 0006).
- Registry persisted at `m/index/{id}` (RFC 0003 `SchemaEntry.directives.index`);
  per-index watermark `m/wm/{id}`; state machine
  `creating→backfilling→live→dropping`.
- **Acceptance:** register/list/drop an index; watermark read/write round-trips.

**B2. Value index** — `src/index/value.rs`. Tokenizers `exact`/`hash`/`int`/
`float` (RFC 0006 table); `Index[key_id][token] → uids`; range via
order-preserving `int`/`float` token key-range scan; `hash` sets the re-fetch
flag on its `AccessPlan`.
- **Acceptance:** RFC 0006 acceptance #1 (=, IN, range vs oracle, DateTime→epoch
  range across boundaries), #2 (lossy `hash` re-fetch).

**B3. Label index** — `src/index/label.rs`. `LabelIndex[label_id] → uids`; node
upsert adds/removes uid from each label set.
- **Acceptance:** RFC 0006 #3 (label ∩ range = oracle).

**B4. Count index** — `src/index/count.rs`. `Count[pred][dir][degree] → uids`;
move uid between degree buckets on edge add/delete (writer knows old/new degree
from roaring `len()`). Supersedes M1's simplified degree-Meta counter for query
use.
- **Acceptance:** RFC 0006 #4 (bucket moves; degree predicate = oracle).

**B5. Synchronous write-path maintenance wire-in** — `src/write.rs`.
- In the atomic write batch (RFC 0004 step 4), for every **live** index touching
  the change, run `extract→apply` so the index commits with the data
  (`watermark == latest_seq`, no steady-state lag). Reuses the existing
  single-`Vec<RecordOp>` fan-out.
- **Acceptance:** upsert → immediately query via index (read-your-writes); index
  entry present at the write's seq.  [dep: B1–B4; H1 fix]

**B6. Backfill builder** — `src/index/backfill.rs`. Async loop scans existing
nodes/edges (`scan_record_type`), emits `extract→apply` in batches, advances
`m/wm/{id}`; idempotent replay (set union/diff) for crash-safe restart; flips to
`live` at catch-up; `dropping` range-deletes the index prefix.
- **Acceptance:** RFC 0006 #5 (backfilling index correct via index-to-watermark +
  tail; idempotent mid-backfill restart; `live` transition empties the tail), #7
  (drop drains keyspace, no dangling postings — RFC 0017 invariant counter).

---

### WS-C — Predicate IR + Cypher frontend (RFC 0007 §3, Amendment A1)

Goal: Cypher text → validated `PlanNode` IR. **Storage-independent — can be
built in parallel with WS-A/B** (the decypher track was always meant to be
front-loaded, RFC 0007 §13).

**C1. `PlanNode` IR + `PropPredicate`** — `src/query/ir.rs`. Exactly RFC 0007 §3:
`ScanByLabel`, `ScanByValueIndex{access: Point|Range|InSet}`, `Expand`,
`VarExpand`, `Filter`, `Intersect`/`Union`/`Difference`, `Project`, `Sort`,
`Limit`; literals are RFC 0004 `TypedValue` (imported, not redefined — Q23c).

**C2. Wire in `decypher`** — re-enable the `Cargo.toml` dep (pinned
`=0.2.0-alpha.6`), `src/query/parse.rs`. Parse full openCypher grammar → typed
AST. `malformed_cypher` = a decypher parse error (surface its `miette`
diagnostic).
- **Acceptance:** valid subset queries parse; garbage → `malformed_cypher`.

**C3. Lowering AST → IR with the subset gate** — `src/query/lower.rs`.
- Inject a `NameResolver` (label/pred/prop name → interned id) — mock map in
  tests, `SchemaCache` in production (Q23b). Property maps `{p:v}` lower to
  `Filter`/`ScanByValueIndex` equality; `<>` → `Difference{pos, neg: Point}`;
  `IN` → `Union` of type-scoped points; `date(...)` → epoch `int`.
- **Query parameters:** RFC 0008's data plane is `{cypher, params, consistency}`
  — lowering substitutes `$param` references from the `params` map into
  `TypedValue` literals **before** planning (type-checked at substitution;
  missing param → `malformed_cypher`-family error). Parameters are v0 scope:
  clients must not string-interpolate values into Cypher.
- Out-of-v0 constructs (`WITH`, aggregations>`count(*)`, subqueries, path vars,
  `OPTIONAL MATCH`, `shortestPath`, map projections, any write clause) →
  `unsupported_cypher{construct, see:"RFC 001x"}`; `*0..n` → rejected.
- **Acceptance:** RFC 0007 test #1 (grammar accept/reject table; assert exact
  `unsupported_cypher` vs `malformed_cypher` variant).  [dep: C1, C2]

---

### WS-D — Planner (RFC 0007 §5)

Goal: `PlanNode` IR + live index registry → a bound, ordered plan (or typed
error). Selectivity-driven, roaring `len()` as the one statistic.

**D1. Anchor selection** — `src/query/plan/anchor.rs`. Per variable, collect
access paths via `IndexAm::supports()`; estimate cardinality via roaring `len()`
(one `get`+`len` for point/label; union card for range); bind the
**minimum-cardinality** anchor. `supports()==None` → `unindexed_property` unless
`brute_force=true` under `brute_force_max_nodes` (default 100k). [dep: WS-B, WS-C]

**D2. Hop + direction planning** — `src/query/plan/hops.rs`. Each fixed hop →
`Expand` (`EdgeOut` forward / `EdgeIn` reverse / union of both for undirected
`Dir::Both`); intersect far endpoint with its filter/label set; **reverse the
pattern** to anchor on the smaller side when beneficial (reverse adjacency is
materialized). Multi-way AND → `Intersect` smallest-first by `len()`.

**D3. Variable-length planning** — `src/query/plan/varlen.rs`. `VarExpand{min,max}`
= bounded BFS bookkeeping; clamp `max` to `bfs_depth_cap` (default 5); `max >
cap` → `bfs_depth_exceeded` (reject, never silently truncate); `*` = `1..cap`.
- **Acceptance:** RFC 0007 tests #2 (anchor picks smaller + reverses), #3/#4/#5
  planning shapes (validated end-to-end in WS-E).

---

### WS-E — Executor + read path (RFC 0007 §6, RFC 0001)

Goal: run a bound plan under the freshness-gated, watermark + changelog-tail
model. **The correctness core.**

**E0. Design decision first: row bindings over set semantics.** The IR is
set-based — each `PlanNode` yields **one roaring set per variable** — but
`RETURN a.name, b.content` needs **(a, b) row pairs**, and `Expand` (a
union-of-neighbors) discards the src→dst pairing. RFC 0007 §6.g hand-waves this
("the surviving `f` set … carried alongside the BFS"); this plan must own it:
- **Chosen approach — back-expansion at materialize time:** run the whole plan
  set-based (cheap, roaring), then reconstruct only the pairs the `RETURN`
  clause needs by **re-expanding the final hop(s) per retained uid** against
  the already-filtered sets. This is exactly what the bench executors do by
  hand (`bench/src/queries.rs::messages_by_frontier` iterates the final person
  frontier and reads each one's `HAS_CREATOR` postings) and it was verified
  correct 240/240 vs FalkorDB. Cost: one extra batched expansion of the final
  frontier — bounded, and only for variables actually returned.
- The alternative (a full binding-tuple table threaded through every operator,
  Neo4j-style rows) is rejected for v0: it forfeits the roaring set-algebra
  advantage on intermediate hops and reintroduces per-path blowup that set
  semantics deliberately avoids.
- **Consequence (client-visible, document in WS-F):** v0 returns **distinct
  bindings**, not per-path row multiplicity — a non-`DISTINCT` Cypher `MATCH`
  that reaches the same `(a, b)` via 3 paths returns 1 row here. Shadow-tests
  therefore compare `RETURN DISTINCT` sets (proved apples-to-apples in the
  2026-07-06 accuracy run). Path multiplicity arrives with path variables in
  RFC 0013.

**E1. Freshness gate** — `src/query/exec/gate.rs`. No token → proceed; `{session:T}`
→ block on `DbReader::subscribe()` until `durable_seq >= T` (bounded wait →
retryable `reader_behind`); `{strict:true}` → bounded by `manifest_poll_interval`.
Yields the query's `latest = m/latest_seq`.  [dep: A1]

**E2. Index/adjacency phase (roaring)** — `src/query/exec/mod.rs`. Walk the bound
plan: anchor scan, each `Expand`/`VarExpand` via **batched** `neighbors_batch`
(WS-A2), `Filter`/`Intersect`/`Union`/`Difference` in roaring. Track
`W = min(watermark)` over used indexes/projections.  [dep: A2, WS-D]

**E3. Deleted subtraction + var-length BFS dedup** — subtract `m/deleted_nodes`
(read **once/query, cached** — WS-A3) and deleted-edge bitmaps at every hop and
on the final set; BFS keeps a cumulative `visited` bitmap (roaring difference)
so each uid expands at most once (terminates on cycles, `O(reachable)` not
`O(paths)`).

**E4. Changelog-tail overlay** — `src/query/exec/tail.rs`. When `W < latest`
(a backfilling index), scan `Log[(W, latest]]`, materialize named nodes/edges
from base KV (current to `latest`), **re-evaluate the full pattern**, and merge
`(candidates − deleted) ∪ tail_matches` (disjoint by `W`, no dedupe). Bound by
`tail_max_entries` (5000): block up to `tail_wait_timeout` re-checking
watermarks, else retryable `index_behind`.
- **Acceptance:** RFC 0007 test #6 (tail overlay over a traversal = caught-up
  oracle; deleted endpoint removed), #10 (`index_behind` at the bound).

**E5. Fetch + Project + Sort + Skip + Limit** — `src/query/exec/materialize.rs`.
Reconstruct multi-variable rows via **back-expansion** (E0), then for surviving
uids **batched** fetch `Node[uid]` (WS-A2), project items
(`Var`/`Var.prop`/optional `count(*)`), **materialize-then-sort** (Q4) over the
bounded row set, then `SKIP`/`LIMIT`. `LIMIT` without `ORDER BY` truncates in
ascending-uid order before fetching all blobs.
- **Acceptance:** RFC 0007 tests #3/#4/#5 vs oracle, #7 (unindexed error/brute-
  force), #8 (ORDER BY/SKIP/LIMIT incl. DESC + ties), #9 (N+1 batching asserted
  via fan-out counter + filter push-down shrinks frontier).

---

### WS-F — HTTP service + reader fleet (RFC 0008)

Goal: expose the engine. `src/service/` (axum), `src/main.rs` (`--role
{writer,reader}`).
- **Data plane:** `POST /query {cypher, params, consistency}`; JSON upsert
  endpoints wrapping the M1 `Writer` (writer role only). API docs state the E0
  row contract explicitly: results are **distinct bindings** (no per-path
  multiplicity until RFC 0013).
- **Admin plane:** `create_index`/`drop_index`/`list_indexes` (WS-B), namespace
  lifecycle.
- **Error taxonomy → HTTP:** `reader_behind`/`index_behind` → 503;
  `unindexed_property`/`bfs_depth_exceeded`/`malformed_cypher` → 400;
  `unsupported_cypher` → 501. Response carries `latest_seq` (session token).
- **Fleet:** same binary; `--role reader` serves `GraphReader` against the same
  prefix, scales independently; writer role registers write routes.
- **Acceptance:** end-to-end HTTP query returns rows + token; reader serves a
  query with a writer's token after the freshness gate; role routing correct.
  [dep: WS-E]

---

### WS-G — Validation & benchmarking

Goal: prove correctness and measure the perf work landed.
- **Shadow-test vs FalkorDB** using the existing `bench/` harness + the
  `verify`/accuracy tooling built 2026-07-06 — but now driving **real Cypher
  through the engine** instead of hand-written traversals. Reuse the hop ×
  supernode-degree matrix; **set `RESULTSET_SIZE=-1`** (the 10k-cap trap). Target
  RFC 0000 D12: **0 missing / ≤5% extra** rows vs FalkorDB.
- **Re-run the hop × degree latency matrix** through the engine and compare to
  both FalkorDB and the M1 hand-written baseline, to quantify the batched-read +
  cache win (WS-A2/A3) against the measured 4–14× gap.
- **Real-S3 gate (D12/RFC 0017):** the N+1 mitigations must be benchmarked on
  real S3, never LocalStack-only, before any CSR/WCOJ optimization (RFC 0009) is
  justified.

## 5. Sequencing & milestones

Critical path and parallelism:

```
        ┌─ WS-C (IR + decypher + lowering)  ── parallelizable now, storage-independent
        │
WS-A1 ──┼─ WS-A2 ── WS-A3            (reader + batched get + snapshot/cache)
        │
        └─ WS-B1..B6 (index framework)      [needs A1; writer wire-in needs H1 fix]
                     │
      WS-C + WS-B ──▶ WS-D (planner) ──▶ WS-E (executor/read path) ──▶ WS-F (HTTP/fleet) ──▶ WS-G (validate)
```

- **M2 = WS-A + WS-B + WS-E-core** (index framework + read path with
  freshness/watermark/tail merge), validated with **programmatically-built
  `PlanNode` IR** (no Cypher yet) against the FalkorDB oracle. Exit criterion:
  RFC 0006 acceptance 1–7 + RFC 0001 tail-merge test pass; the RFC 0017 metric
  matrix is populated.
- **M3 = WS-C + WS-D + WS-F** (Cypher frontend, planner, HTTP fleet) on top of
  M2's read path. Exit criterion: RFC 0007 tests 1–10 pass; end-to-end HTTP
  Cypher; FalkorDB shadow-test green (0 missing / ≤5% extra) on real S3.

Suggested order: **(1)** prerequisite fixes (H1, M2 torn-split, wire
`maybe_rollup`) → **(2)** WS-A1/A2 + WS-C in parallel → **(3)** WS-B → **(4)**
WS-A3 → **(5)** WS-D → **(6)** WS-E → **(7)** WS-F → **(8)** WS-G.

## 6. Prerequisite fixes (land first)

| # | Fix | Why it blocks | Source |
|---|-----|---------------|--------|
| H1 | `intern()` cache poisoning on aborted write | write path feeds indexes; a poisoned id in an index is corruption | review findings H1 |
| M2 | torn `Split` manifest/part read → pin a snapshot | concurrent reader path (WS-A3) is unsafe without it | review findings M2 |
| M3 | wire `maybe_rollup` into maintenance | deleted-edge bitmaps grow unbounded → tail/subtraction cost unbounded | review findings M3 |
| — | batched multi-get on `GraphStorage` | executor N+1 mitigation impossible without it | bench §2 |
| — | CI (`ci.yml`: fmt/clippy/test) | protect a query engine landing across many files | review findings M4 |

## 7. Testing strategy

- **Unit / property:** IR lowering accept-reject table (C3) including `$param`
  substitution and missing/mistyped params; planner anchor/hop ordering (WS-D);
  roaring set-algebra vs `BTreeSet` oracle (extends existing
  `tests/posting_props.rs`); row back-expansion (E0) reconstructs exactly the
  pairs a naive per-path oracle produces after `DISTINCT`.
- **Correctness vs oracle:** small-random-graph oracles for fixed/reverse/undirected
  hops, var-length BFS on cyclic graphs, multi-pattern join (RFC 0007 tests 3–5).
- **Consistency:** freshness gate + tail-merge over a traversal with a lagging
  watermark (RFC 0007 #6, RFC 0001 #3); `reader_behind`/`index_behind`.
- **Shadow-test vs FalkorDB:** the `bench/` harness — the accuracy tooling proved
  240/240 exact-match feasible; reuse it as the engine's acceptance gate
  (`RESULTSET_SIZE=-1`, DISTINCT set-diff, size-only for huge sets).
- **Perf regression:** the hop × degree latency matrix through the engine, gated
  on real S3 (RFC 0017), tracking the N+1 fan-out and cache-hit counters.

## 8. Risks & open questions

- **`decypher` is a bus-factor-1 alpha with an unstable AST** (RFC 0007 §13.3a).
  Mitigation already decided: pin `=0.2.0-alpha.6`, re-run lowering tests on every
  bump, keep lowering the only coupling surface, fork if abandoned (permissive
  license). First spike: lower from typed AST vs HIR — pick the more stable.
- **Read cache invalidation** (WS-A3) must key off manifest advance; a stale
  cache under a moving writer would violate the freshness gate. Snapshot-scoped
  caching is the safe default.
- **`W = min(watermark)` with several backfilling indexes** could make the tail
  large; `tail_max_entries` + `index_behind` is the loud-failure valve, but a
  big multi-index backfill needs an operational story (stagger backfills).
- **Storage primitives (RESOLVED by inspection 2026-07-06):**
  `Storage::snapshot()` exists in the vendored common (`storage/mod.rs:437`) —
  A3 is plumbing, not substrate work. A native multi-get does **not** exist —
  A2 starts as bounded-concurrency fan-out; only promote it into the vendored
  fork if profiles demand it.
- **Row-multiplicity semantics** (E0): v0 returns distinct bindings, not
  per-path rows. Clients porting non-`DISTINCT` Cypher from Neo4j/FalkorDB may
  see fewer rows. Must be documented at the API surface (WS-F) and enforced in
  shadow-tests (`RETURN DISTINCT` comparison). If a customer workload genuinely
  needs path multiplicity, that pulls RFC 0013 path-variables forward — flag
  early.
- **Effort (rough, sequential):** WS-A ≈ 1–1.5 wk · WS-B ≈ 2–3 wk (largest:
  four index kinds + backfill state machine) · WS-C ≈ 1–2 wk (decypher AST
  spike is the unknown) · WS-D ≈ 1 wk · WS-E ≈ 2–3 wk (correctness core +
  oracle tests) · WS-F ≈ 1 wk · WS-G ≈ 1 wk. With WS-C parallel to WS-A/B:
  **~8–11 weeks** single-developer, M2 exit ≈ week 5–6.

## 9. Definition of done

- Declare an `int`/`exact`/label/count index via the admin API; it backfills,
  goes `live`, and is maintained synchronously thereafter.
- `POST /query` with an openCypher read-subset query returns correct rows +
  a session token, through a reader that gated on that token.
- Variable-length + multi-hop + filtered patterns match a FalkorDB oracle
  (0 missing / ≤5% extra) on the RAG-KG workload at 1–10M nodes.
- N+1 mitigations (batched reads, filter push-down, cache) measured on **real
  S3** and recorded in the RFC 0017 ledger; the M1→engine latency gap vs the
  hand-written baseline and FalkorDB is quantified.
- All bounds fail loudly (`bfs_depth_exceeded`, `index_behind`,
  `unindexed_property`), never silently.
