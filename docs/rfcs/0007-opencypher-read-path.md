---
title: "RFC 0007: openCypher Read Subset, Planner & Read Path"
status: draft
date: 2026-07-03T00:00:00Z
related:
  - 0000-rfc-index.md
  - 0001-strong-consistency-model.md
  - 0004-graph-data-model-and-write-path.md
  - 0005-posting-list-substrate.md
  - 0006-index-framework.md
  - 0008-http-service-and-fleet.md
---

# RFC 0007: openCypher Read Subset, Planner & Read Path

## Summary

turbolay's v0 query surface is a **read-only subset of openCypher**: `MATCH` node and relationship patterns (including variable-length `-[:REL*min..max]->` hops), a `WHERE` clause with boolean and comparison operators, and `RETURN` / `ORDER BY` / `SKIP` / `LIMIT`. Writes are **not** Cypher in v0 — they are the JSON upsert API (RFC 0004), and Cypher `CREATE`/`MERGE`/`SET`/`DELETE` are deferred to RFC 0011 (D7, Q3). This RFC owns the grammar, how it lowers, how it is planned, and how it executes correctly on top of the freshness-gated read model.

The Cypher text never touches execution directly. It parses into a small internal **predicate IR** (§3); the planner and executor consume only the IR. This is the seam (D7) that lets a later full-Cypher/GQL frontend with aggregations, `WITH`, and subqueries (RFC 0013) be a *frontend swap*, not a rewrite of the traversal engine.

The traversal engine itself is **roaring set algebra** (RFC 0005): every hop is an adjacency posting-list read, every filter is an index posting-list read, and everything is combined with roaring AND/OR/NOT applied **smallest-first by cardinality**. Execution is the freshness-gate → index/adjacency-to-watermark → changelog-tail-overlay → merge algorithm from RFC 0001, extended here to graph patterns and multi-hop traversal.

Correctness first: v0 evaluates the pattern exactly over every candidate. No cost-based join reordering beyond selectivity-driven anchor/frontier choice, no CSR/WCOJ leapfrog joins (RFC 0009), no top-k pruning.

## Decision

1. **Grammar**: locked per §2 — read-only `MATCH` / `WHERE` / `RETURN` / `ORDER BY` / `SKIP` / `LIMIT`, fixed and `*min..max` variable-length hops (Q17). Everything else (`WITH`, aggregations beyond `count(*)`, subqueries, path variables, `OPTIONAL MATCH`, `shortestPath`, map projections) is out of v0 and rejected with a precise error → RFC 0013.
2. **Predicate IR**: a typed Rust enum of physical-ish operators (§3) is the only thing the planner and executor see. The Cypher parser is the only frontend-specific code; RFC 0013's frontend is a second, parallel producer of the same IR.
3. **Planner**: anchor = the most selective `live` index (by roaring `len()` / index cardinality); each hop = an adjacency posting-list read (RFC 0005 `neighbors()`) roaring-intersected with any filter sets; multi-way AND smallest-first; variable-length = bounded BFS with a per-query depth cap and uid dedup (§5). A predicate with no supporting index → `supports()` returns `None` → `unindexed_property` error unless `brute_force=true` (Q15).
4. **Read path**: freshness gate on the session token (RFC 0001) → run index/adjacency access valid to `W = min(watermark over used indexes)` → scan the changelog tail `Log[(W, latest]]` and evaluate the *full pattern* on materialized tail nodes/edges → merge `(index/adjacency candidates − deleted bitmaps) ∪ tail matches` → fetch surviving `Node[uid]` blobs → `ORDER BY` / `SKIP` / `LIMIT` in the executor (§6).
5. **Bounds**: a variable-length traversal is capped at `bfs_depth_cap` (config, default 5); the changelog tail is bounded by `tail_max_entries` with a bounded wait then a retryable error (§7).

## Motivation

RFC 0000 (D7) locks v0 to an openCypher read subset plus JSON writes and defers the full language to RFC 0013, deliberately: standing up a complete Cypher/GQL planner before the posting-list traversal engine underneath it has run on real S3 would be solving the wrong problem first (RFC 0000 correct-first ledger). The architectural risk in that plan is the same one the sister FTS project's RFC 0007 names: if the parser, planner, and executor are one undifferentiated blob, "swap the frontend later" becomes a rewrite. This RFC makes the swap real by construction — define the IR now, prove the Cypher subset is a thin lowering onto it, and keep every operator below the IR frontend-agnostic.

The second reason this RFC exists is that RFC 0001 states the consistency *contract* (freshness gate, watermark, changelog-tail overlay, `reader_behind`) and RFC 0005/0006 state the *primitives* (roaring set algebra, `neighbors()`, `IndexAm::supports/execute`), but nothing yet says how a **multi-hop graph pattern** is planned and executed over them — what the anchor is, how a k-hop BFS frontier is expanded and intersected, how the tail overlay applies to a *traversal* rather than a single-index filter, and what happens at the depth cap and the tail bound. Those are load-bearing correctness questions and this RFC answers them.

## 2. The openCypher read subset

### 2.1 Grammar (v0)

```
Query       := Match+ Where? Return
Match       := 'MATCH' Pattern (',' Pattern)*
Pattern     := NodePat (Rel NodePat)*
NodePat     := '(' Var? (':' Label)? PropMap? ')'
Rel         := '-' RelDetail? '->'            // outgoing
             | '<-' RelDetail? '-'            // incoming
             | '-' RelDetail? '-'             // undirected (both projections)
RelDetail   := '[' Var? (':' RelType)? VarLen? ']'
VarLen      := '*' (Int ('..' Int?)?)?        // *  *2  *1..3  *2..  (Q17)
PropMap     := '{' (Key ':' Literal (',' Key ':' Literal)*)? '}'
Where       := 'WHERE' BoolExpr
BoolExpr    := BoolExpr 'AND' BoolExpr
             | BoolExpr 'OR'  BoolExpr
             | 'NOT' BoolExpr
             | '(' BoolExpr ')'
             | Comparison | LabelPred | Var '.' Key 'IS NOT NULL'
Comparison  := Var '.' Key Op Literal
             | Var '.' Key 'IN' '[' Literal (',' Literal)* ']'
Op          := '=' | '<>' | '<' | '<=' | '>' | '>='
LabelPred   := Var ':' Label
Return      := 'RETURN' RetItem (',' RetItem)* OrderBy? Skip? Limit?
RetItem     := Var | Var '.' Key | 'count(*)'          // count(*) optional in v0 (§2.3)
OrderBy     := 'ORDER BY' (Var '.' Key ('ASC'|'DESC')?)+ ...
Skip        := 'SKIP' Int
Limit       := 'LIMIT' Int
```

Multiple `MATCH` clauses and comma-separated patterns within one `MATCH` are supported; they share variable bindings and are executed as a conjunction of patterns joined on their shared variables (an intersection of the uid sets each pattern binds to that variable). Property maps `{prop: v}` on a node pattern are sugar for equality predicates on those properties, evaluated exactly like a `WHERE` `=` (they lower to the same `Filter` IR).

### 2.2 Relationship patterns

- `(a)-[:KNOWS]->(b)` — a forward hop; access path `EdgeOut[a][KNOWS]` (RFC 0004/0005).
- `(a)<-[:KNOWS]-(b)` — a reverse hop; access path `EdgeIn[a][KNOWS]`, the materialized in-projection (RFC 0006, Q16). Reverse traversal is a symmetric posting-list read, not a scan.
- `(a)-[:KNOWS]-(b)` — undirected; the union of both projections.
- `(a)-[:KNOWS*1..3]->(b)` — a **variable-length** path of 1 to 3 `KNOWS` hops (Q17), executed as bounded BFS (§5). `*` alone means `1..bfs_depth_cap`; `*2` means exactly 2; `*2..` means 2 to the cap. `min = 0` (`*0..n`, reflexive) is **rejected in v0** (it introduces the anchor itself as a zero-length match and complicates dedup) → RFC 0013.

### 2.3 Explicitly out of v0 (→ RFC 0013)

Rejected at parse time with `unsupported_cypher` (recognized construct, deferred) — distinct from `malformed_cypher` (never valid):

- `WITH` (query chaining / horizontal composition) and everything it enables (post-aggregation filtering, mid-query projection).
- **Aggregations beyond `count(*)`**: `collect`, `sum`, `avg`, `min`, `max`, `count(expr)`, `DISTINCT` aggregation. `count(*)` on the final result set is the *only* aggregate in v0, and even it is optional (a build-time flag) — a planner that ships without it rejects `count(*)` as `unsupported_cypher` too, since it is a materialize-then-count over the bounded result, not a streaming aggregate.
- **Subqueries** (`CALL { ... }`, `EXISTS { ... }`, pattern predicates in `WHERE`).
- **Path variables** (`p = (a)-[:R]->(b)`) and path functions (`length(p)`, `nodes(p)`, `relationships(p)`).
- `OPTIONAL MATCH` (left-outer semantics), `shortestPath` / `allShortestPaths`.
- **Map projections** (`RETURN a { .name, .age }`) and list comprehensions.
- Any Cypher **write** clause (`CREATE`, `MERGE`, `SET`, `REMOVE`, `DELETE`, `DETACH DELETE`) → the JSON upsert API today (RFC 0004), Cypher writes → RFC 0011.

These are recognized-but-deferred so client authors get "come back after RFC 0013" rather than "this is malformed." Anything the parser has never heard of is `malformed_cypher`.

## 3. Predicate IR — the frontend swap seam (D7)

The Cypher AST lowers to a tree of physical-ish operators. This is the *only* input to the planner and executor; nothing downstream ever looks at Cypher again.

```rust
/// Physical-ish IR the executor walks. Each node yields a roaring set of uids
/// (or, for Project/Sort/Limit, a materialized row set at the end).
pub enum PlanNode {
    /// All node uids carrying a label — LabelIndex[label_id] (RFC 0006).
    ScanByLabel { label: LabelId },

    /// Value-index access on a scalar property (RFC 0006 §"operator → access").
    ScanByValueIndex {
        prop: PropId,
        access: ValueAccess,       // Point{val} | Range{lo: Bound, hi: Bound} | InSet{vals}
    },

    /// One hop from a bound frontier set along a predicate/direction.
    /// Reads EdgeOut/EdgeIn posting lists (RFC 0005 neighbors()).
    Expand { input: Box<PlanNode>, pred: PredId, dir: Dir },   // Dir = Out | In | Both

    /// Bounded variable-length expansion: BFS from `input`, min..max hops,
    /// uid dedup across levels, capped at bfs_depth_cap (§5).
    VarExpand { input: Box<PlanNode>, pred: PredId, dir: Dir, min: u32, max: u32 },

    /// Restrict a bound set by a predicate on the SAME variable.
    /// Resolved to an index set (intersect) or, if unindexed, a materialize+recheck.
    Filter { input: Box<PlanNode>, pred: PropPredicate },

    /// Roaring set combination over sets bound to the SAME variable.
    Intersect(Vec<PlanNode>),     // AND / multi-pattern join on a shared var — smallest-first
    Union(Vec<PlanNode>),         // OR / IN
    Difference { pos: Box<PlanNode>, neg: Box<PlanNode> },     // NOT / deleted subtraction

    /// Terminal operators (run once, on the materialized surviving rows).
    Project { input: Box<PlanNode>, items: Vec<RetItem> },     // Var | Var.prop | count(*)
    Sort    { input: Box<PlanNode>, keys: Vec<SortKey> },      // ORDER BY (materialize-then-sort, Q4)
    Limit   { input: Box<PlanNode>, skip: u64, limit: Option<u64> },
}

pub enum PropPredicate {
    Cmp { prop: PropId, op: CmpOp, val: TypedValue },   // = <> < <= > >=
    InSet { prop: PropId, vals: Vec<TypedValue> },
    IsNotNull { prop: PropId },
}
```

- `ScanByValueIndex` folds `=`/`IN`/range into one node with an `access` discriminant, exactly as RFC 0006's operator→access mapping produces (`Point`→one `get`, `InSet`→union, `Range`→bounded key-range scan). `<>` lowers to `Difference{ pos: <bound set>, neg: ScanByValueIndex Point }` — it is a negated point, never a standalone scan.
- `Expand`/`VarExpand` are the graph-native operators the FTS-project IR never needed; they read adjacency posting lists (RFC 0005) rather than value indexes.
- `Sort` is the executor-side materialize-then-sort mandated by Q4 (no ordered adjacency in v0); it consumes the *bounded* surviving row set, never the whole graph.

**The swap contract**: the planner and executor may never branch on "was this Cypher or GQL." If a future construct can't be expressed as a `PlanNode`, it belongs to the RFC 0013 frontend growing the IR deliberately (a new node type, e.g. `Aggregate`, `Optional`), not to the source syntax leaking through. Aggregations, `WITH`, and subqueries are exactly such deliberate IR growth — new terminal/pipeline nodes consumed by the same executor.

## 4. Type semantics

Property values carry a runtime type tag and are indexed under it (RFC 0006 tokenizers: `exact`/`hash` for strings, order-preserving `int`/`float`, DateTime→epoch-`int`). A query literal matches only values of its own type; there is no cross-type coercion (`5` and `"5"` never compare equal or order together). `date('1990-01-01')` is a v0 literal constructor that lowers to an epoch-millis `int` — so a `DateTime`/`Date` range predicate is an ordinary `int` range scan (RFC 0006). `IN` is element-wise type-scoped: `x.p IN [1, '1']` is `Union(Point(int:1), Point(string:'1'))`.

## 5. Planner

Input: the `PlanNode` tree from lowering plus the `live` index registry (`m/index/{id}`, RFC 0006). Output: a bound plan, or a typed error. The planner is selectivity-driven, not cost-model-driven — v0 uses roaring cardinality as the one statistic.

### 5.1 Anchor selection — most selective index

For each pattern, the planner picks the **starting variable** whose bound set is smallest, so the traversal drives off the fewest uids:

1. For each variable, collect the access paths available from its label predicate and its property predicates by calling `IndexAm::supports()` per predicate (RFC 0006). A `Point`/`InSet` on a `live` value index, a label lookup, or a count-index lookup each yields a candidate anchor set.
2. Estimate each candidate's cardinality from roaring `len()` (a point/label lookup is one `get` + `len()`; a range is the union cardinality over spanned tokens — cheap because roaring `len()` is O(1) per part). Pick the **minimum-cardinality** anchor.
3. Bind the anchor as a `ScanByLabel` / `ScanByValueIndex`. If a variable has *both* a label and a value predicate, anchor on whichever is smaller and keep the other as a `Filter` to intersect after (or intersect both up front — `Intersect` smallest-first, RFC 0005).

A predicate whose `supports()` returns `None` (no live index on that property) is **not** an anchor and **not** silently scanned: by default the whole query errors `unindexed_property` (Q15, RFC 0006's error shape). With `brute_force=true` and the namespace under the brute-force node-count guardrail, that predicate becomes a `Filter` that materializes candidate `Node[uid]` blobs and rechecks the property in memory (small namespaces only — §5.4).

### 5.2 Hop planning

Each fixed hop is an `Expand`: read the frontier's adjacency posting lists (`EdgeOut[u][pred]` forward, `EdgeIn[u][pred]` reverse — RFC 0005 `neighbors()`), roaring-union them into the next frontier, then roaring-**intersect** with any filter/label set constraining the far endpoint. If the far endpoint has its own selective predicate, the planner may **reverse the pattern** and anchor there instead (expand `In` where it wrote `Out`), because reverse adjacency is materialized symmetrically (Q16) — it picks the direction that starts from the smaller anchor.

Multi-way `AND` on a shared variable (multiple patterns, or a label plus several property predicates) is an `Intersect` applied **smallest-first by `len()`** (RFC 0005): the smallest set drives, each subsequent intersection can only shrink it, and roaring intersections are cheapest when the smaller set leads.

### 5.3 Variable-length planning (Q17)

`VarExpand{pred, dir, min, max}` is bounded BFS:

- Level 0 = the bound input frontier (anchor). For each level `d` from 1 to `max`, expand every uid in the current frontier along `(pred, dir)` via batched `neighbors()` reads (§8), roaring-union the results into the next frontier, and **subtract the cumulative `visited` bitmap** (roaring difference) so each uid is expanded at most once across the whole traversal — this is the uid dedup that makes BFS terminate on cyclic graphs and bounds work to `O(reachable uids)`, not `O(paths)`.
- Uids reached at any level `d` with `min <= d <= max` are unioned into the **result frontier**. (v0 returns the *set of reachable endpoints*, not enumerated paths — path variables are RFC 0013, §2.3.)
- `max` is clamped to `bfs_depth_cap` (config, default 5). A pattern requesting `*1..100` on a namespace capped at 5 is handled per §7 (reject or truncate — decided there), not silently run to 100.

### 5.4 Unindexed predicate

`supports()` → `None` → by default the planner returns `unindexed_property` (RFC 0006):

```json
{ "error": "unindexed_property", "property": "height",
  "hint": "declare an index or pass brute_force=true", "retryable": false }
```

With `brute_force=true` **and** the namespace's live node count under `brute_force_max_nodes` (default 100,000): the predicate becomes a materialize-and-recheck `Filter` over the current frontier (not a full-graph scan unless the *anchor* itself is unindexed, in which case the alive node universe is the frontier — `MATCH (n) WHERE n.height > 5` with no index). Over the guardrail, brute force is refused even with the flag — a hard cap, not a soft warning (matching the sister project's posture). This is the exact Q15 contract.

## 6. Read path / execution

Execution integrates RFC 0001's freshness-gate + index-watermark + changelog-tail plan, extended to traversal. All phases run on a `DbReader` (RFC 0008 reader role).

### 6.a Freshness gate (RFC 0001)

- No `consistency` → proceed (bounded-stale).
- `{"session": T}` → block on `DbReader::subscribe()` until replayed `durable_seq >= T` (RFC 0001's `wait_for_session`); timeout → retryable `reader_behind`. This is the only phase that can block on reader replay, and it issues no S3 read itself.
- `{"strict": true}` → RFC 0001 mode 3, bounded by `manifest_poll_interval`.

The gate yields this query's `latest = m/latest_seq` in the reader's replayed state (`>= T`), which is the tail phase's upper bound.

### 6.b Index / adjacency phase (valid to `W`)

Run the bound plan against indexes and adjacency posting lists. Every index/reverse projection is read **up to its watermark**; live indexes are current (`watermark == latest_seq`, RFC 0006), backfilling ones may lag. Let `W = min(watermark over every index/adjacency projection the plan uses)`. Anchor scan, each `Expand`/`VarExpand` hop, and every `Filter`/`Intersect`/`Union`/`Difference` compose here, entirely in roaring (RFC 0005). In steady state every used index is live, so `W = latest` and the tail below is empty — the common case pays nothing for the overlay.

### 6.c Deleted-set subtraction

The frontier at every hop, and the final candidate set, has the **deleted-node bitmap** (`m/deleted_nodes`, roaring, read **once per query and cached**, current to `latest` because it is written synchronously in the write batch — RFC 0001/0004) and the relevant **deleted-edge bitmaps** (`Meta["deleted_edges"/pred/anchor]`, RFC 0005) subtracted. `neighbors()` already folds both in (RFC 0005 read-path pseudocode), so tombstoned endpoints never enter a frontier and can't propagate through subsequent hops.

### 6.d Changelog-tail phase (RFC 0001)

Scan `Log[(W, latest]]`. For each entry — `AddEdge` / `DeleteEdge` / node upsert / node delete (RFC 0004 changelog schema) — **materialize** the named nodes/edges directly from the base KV records (`Node[uid]`, `EdgeOut`/`EdgeIn` — guaranteed present by the token, RFC 0001) and **evaluate the full pattern** on them: the same traversal + predicate logic the executor runs in §6.b, just fed from tail-materialized nodes/edges instead of from an index. A tail `AddEdge` that extends a traversal path is followed through the base adjacency records (which *are* current to `latest`); a tail node upsert re-checks the `WHERE` predicates on the live blob; a delete contributes to the subtraction, never a match. The tail is bounded by `tail_max_entries` (§7).

Because live indexes have `W = latest`, the tail is non-empty **only** when the query uses a backfilling index (RFC 0006) — the overlay is the mechanism that hides backfill lag, not a steady-state cost.

### 6.e Merge

```
candidates = ( index / adjacency candidates  −  deleted_nodes − deleted_edges )  ∪  tail_matches
```

The subtraction removes uids an index still lists but a later tail entry deleted/superseded; `tail_matches` adds uids whose index/adjacency entry hasn't been written yet. The two sides are disjoint by the seq boundary `W` (RFC 0001), so the union needs no dedupe pass.

### 6.f Fetch, then ORDER BY / SKIP / LIMIT

For every surviving uid bound to a `RETURN` variable, fetch its `Node[uid]` blob (RFC 0004), deserialize, and project the requested items (`Var` → the node, `Var.prop` → one property, `count(*)` → the cardinality if enabled). **`ORDER BY` is a materialize-then-sort in the executor** over this bounded result set (Q4 — no ordered adjacency in v0), then `SKIP`/`LIMIT` slice it. Sorting the *final bounded set* (already limited by anchor selectivity and the traversal cap) is cheap; it never sorts a full adjacency list. If the query has a `LIMIT` but no `ORDER BY`, order is stable-but-implementation-defined (ascending uid, which the roaring iteration already produces) and `LIMIT` can truncate before fetching all blobs.

### 6.g Worked example (end to end)

```cypher
MATCH (p:Person)-[:KNOWS*1..2]->(f)
WHERE f.birthdate < date('1990-01-01')
RETURN f.name
ORDER BY f.name
LIMIT 10
```

with a `Person` label index, an `int` value index on `birthdate` (both live), and session token `T = 200`.

1. **Gate**: `wait_for_session(reader, 200)` → reader replayed to `durable_seq = 200`. `latest = 200`.
2. **Anchor**: two candidate anchors — `ScanByLabel{Person}` (say `len = 1M`) and, if it constrained a variable directly, a `birthdate` range. The far endpoint `f` has the selective range predicate, so the planner considers **reversing**: anchor on `f` via the `birthdate` range and expand `KNOWS` in reverse. Suppose `birthdate < 1990-01-01` yields `len = 120K` — smaller than `Person`'s 1M, so the planner anchors on the range and plans `VarExpand{pred: KNOWS, dir: In, 1, 2}` from it, intersecting the reached set with `ScanByLabel{Person}` to bind `p`. (If the range were the *larger* set, it would anchor on `Person` and apply the range as a `Filter` on `f` after the forward expansion — same result, different drive order.)
3. **Range scan** (`ScanByValueIndex{birthdate, Range{hi: date('1990-01-01') exclusive}}`): bounded key-range scan `Index[birthdate][ ..sortable(epoch(1990-01-01)) )`, roaring-union the spanned tokens → `f`-candidate set `F₀` (`W` for this index = 200, live).
4. **Var-length BFS** (`dir = In`, 1..2, from `F₀`): level 1 = `⋃ neighbors(f, KNOWS, In)` over `f ∈ F₀`, minus `visited`; level 2 = expand level 1's frontier, minus `visited`. Union of levels 1–2 = the reachable `p`-side set `P`. Each `neighbors()` folds in `deleted_nodes`/`deleted_edges` (§6.c); `visited` dedup bounds the work.
5. **Label intersect**: `Intersect([P, ScanByLabel{Person}])` smallest-first → the `p` uids that are actually `Person` and reach an `f` in `F₀` within 1–2 `KNOWS` hops. The surviving `f` set (the endpoints that anchored a valid path) is what `RETURN` binds — carried alongside the BFS so each retained `f` is one that had ≥1 qualifying `Person` predecessor.
6. **`W` and tail**: both indexes live → `W = 200 = latest` → tail `Log[(200, 200]]` empty → merge is a no-op. (Had the `birthdate` index been mid-backfill at `W = 197`, the tail `Log[(197, 200]]` would materialize the 3 tail entries, re-run the range + reverse-BFS pattern on the live nodes/edges, and union the matches.)
7. **Deleted subtraction**: `candidates − deleted_nodes − deleted_edges` (already applied per-hop; re-applied to the final set for any node tombstoned after the anchor scan).
8. **Fetch + sort + limit**: fetch `Node[f]` for each surviving `f`, project `f.name`, **materialize-then-sort** ascending by `name` (Q4), `SKIP 0`, `LIMIT 10` → the 10 lexicographically-first names. Response carries `latest_seq = 200`.

## 7. Bounds: depth cap and tail bound

**Variable-length depth cap.** `bfs_depth_cap` (config, default 5) bounds any `VarExpand`. A pattern whose requested `max` **exceeds** the cap is **rejected** with a typed error rather than silently truncated — truncating would return a subset of the true answer with no signal, the exact silent-cliff failure Q15/RFC 0006 rejects for unindexed scans:

```json
{ "error": "bfs_depth_exceeded", "requested": 100, "max": 5, "retryable": false }
```

A query that wants deeper traversal must raise the namespace cap explicitly (admin config) — a deliberate act, visible in metrics (RFC 0017 per-hop frontier sizes), never a per-query surprise. `*` (unbounded) is defined as `1..bfs_depth_cap`, so it is always in-bounds by construction.

**Tail bound.** The changelog tail is bounded by `tail_max_entries` (default 5,000 — an order of magnitude above the "backfill lag is one batch" expectation). At the bound, do **not** truncate and evaluate a partial pattern (that violates exactness). Instead, mirroring the sister project's `index_behind`: block up to `tail_wait_timeout` (default 2s) re-checking the used indexes' watermarks; if a watermark advances enough that a re-scoped tail `(new_W, latest]` fits, re-run the index phase at the higher `new_W` (less tail work) and proceed; if the wait times out still over the bound, return a **retryable** error:

```json
{ "error": "index_behind", "retryable": true, "required_watermark": W_needed, "current_watermark": W_have }
```

This is `reader_behind`'s shape one layer down (index-builder lag, not reader-replay lag). It essentially never fires in steady state because live indexes keep `W = latest` and the tail empty; it exists to fail loudly during a large backfill rather than serve an incorrect traversal. RFC 0008 lifts both errors into HTTP status (`reader_behind`/`index_behind` → 503, `unindexed_property`/`bfs_depth_exceeded`/`malformed_cypher` → 400, `unsupported_cypher` → 501).

## 8. The N+1 problem and mitigations

Graph traversal's dominant cost is **N+1**: after fetching a frontier of `N` uids, the naive next hop issues `N` separate adjacency reads — one round-trip per node — and on S3 each cold read is a network round-trip (the "cold first-hop S3 latency is the real enemy" framing of RFC 0000 D12 / RFC 0017). Left unmitigated, a 2-hop traversal over a 10K frontier is 10K sequential `get`s. The fundamentals framing: traversal cost is dominated by the *number of adjacency lookups*, not the CPU of set math, so the planner optimizes for **fewer, larger, filtered** reads. v0's mitigations:

1. **Batch neighbor reads.** A frontier expansion issues a **multi-get** over the frontier's adjacency keys (`EdgeOut[u][pred]` / `EdgeIn[u][pred]` for all `u` in the frontier) rather than a serial loop — one batched round-trip served by SlateDB's cache + bloom filters, then roaring-union the returned posting lists. This is the single biggest lever; `neighbors()` (RFC 0005) is the per-key primitive, and the executor drives it in batches. (Instrumented as N+1 fan-out in RFC 0017.)
2. **Push filters down before expanding the next frontier.** Intersect the frontier with any far-endpoint label/property set *before* expanding again, so hop `d+1` starts from the smallest possible frontier. A `WHERE` predicate on an intermediate variable is applied at that hop, not deferred to the end — fewer uids to expand is fewer adjacency reads.
3. **Frontier-size heuristics — expand the smaller side first.** When a pattern can be driven from either endpoint (both have anchors, or one has a selective filter), the planner starts from the smaller anchor and, for each hop, chooses the direction (`Out` vs `In`, both materialized per Q16) that keeps the frontier smaller. Multi-way intersections are smallest-first (RFC 0005). This is the planner's whole cost model in v0: minimize frontier cardinality at each step using roaring `len()`.
4. **Deleted-node bitmap read once per query, cached.** `m/deleted_nodes` is a per-namespace roaring bitmap; it is read a single time at query start and reused for every hop's subtraction (§6.c), never re-fetched per node. Deleted-edge bitmaps are read alongside their adjacency key in the same batch.

These are v0-shaped (correct, bounded, measurable) rather than the deeper CSR/WCOJ locality wins deferred to RFC 0009 — but they turn the dominant N sequential reads into `⌈N / batch⌉` batched reads over a filter-shrunk frontier, which is what makes k-hop RAG-KG traversal viable on S3 at the 1–10M node target (D12).

## 9. Deferred

- **Ordered adjacency (Q4)** — `ORDER BY` on an edge property is materialize-then-sort over the bounded result (§6.f), never a native pre-sorted adjacency read. Composite-edge-key / sortkey ordered adjacency is RFC 0009 (recorded in RFC 0005 §"Deferred: ordered adjacency").
- **Aggregations, `WITH`, subqueries, path variables, `OPTIONAL MATCH`, `shortestPath`, map projections** → full openCypher, RFC 0013, consuming this same IR (new `Aggregate`/`Optional`/`Path` nodes — a frontend + IR-growth change, not an executor rewrite).
- **CSR adjacency + WCOJ / leapfrog joins** → RFC 0009, gated on RFC 0017 traversal-latency baselines on real S3. The `Expand`/`Intersect` operators are the seam a worst-case-optimal join implementation slots behind.
- **Cypher writes** (`CREATE`/`MERGE`/`SET`/`DELETE`) → RFC 0011; v0 writes are the JSON upsert API (RFC 0004).
- **Path enumeration / `min = 0` reflexive var-length** → RFC 0013 (v0 `VarExpand` returns the reachable endpoint *set*, `min >= 1`).

## 10. Alternatives considered

**Ship full openCypher (or GQL) in v0.** One language for the product's life; aggregations and projections immediately. Rejected: pulls a general Cypher planner in before the posting-list traversal engine has run on real S3 — a decision in the dark, against RFC 0000's correct-first posture — and front-loads scope (aggregation pipelines, path algebra) v0 doesn't need. Recorded as the RFC 0013 swap, which the predicate IR (§3) makes a frontend change.

**No variable-length paths in v0** (fixed hops only, var-length later). Rejected (Q17): k-hop neighborhoods are the core RAG-KG query (D12); without them the subset is too thin to be useful. Bounded BFS + a depth cap + uid dedup makes them safe (§5/§7).

**Native ordered adjacency for `ORDER BY` on edge props** (composite-edge-key). Rejected for v0 (Q4): it is the opposite storage model (one key per edge, tiny-object pressure) and multiplies key count by edge count. Materialize-then-sort over the bounded result is correct and cheap at v0 scale; native ordering is RFC 0009 when profiles demand it.

**Auto-brute-force unindexed predicates.** Rejected as default (Q15): a silent full-scan cliff as a namespace grows from 10K to 5M nodes, with no signal. Kept as the guardrailed opt-in `brute_force=true` under `brute_force_max_nodes`.

**Cost-based join reordering with statistics.** Rejected for v0: roaring `len()` selectivity (anchor = smallest set, hops smallest-first) is a good-enough heuristic at 1–10M nodes and needs no histogram/cardinality-estimation machinery. A real optimizer is a later concern, unblocked once RFC 0017 says the heuristic mis-orders on real workloads.

## 11. Tests required

1. **Grammar accept/reject** — table-driven: every pattern/`WHERE`/`RETURN` shape in §2 (accept); `WITH`, `collect`, subquery, path variable, `OPTIONAL MATCH`, map projection (reject `unsupported_cypher`, assert the exact variant); a Cypher write clause (reject `unsupported_cypher`, pointing at RFC 0011); garbage (reject `malformed_cypher`); `*0..n` (reject).
2. **Anchor selection** — a pattern with two candidate anchors of known cardinality; assert the planner binds the smaller and (where beneficial) reverses the traversal direction to start from it.
3. **Fixed + reverse hop correctness vs oracle** — random small graphs; forward, reverse, and undirected hops; assert the bound set equals a naive adjacency-walk oracle.
4. **Variable-length BFS** — `*1..k` on cyclic graphs; assert reachable-endpoint set matches an oracle, that uid dedup prevents reprocessing, and that `max > bfs_depth_cap` errors `bfs_depth_exceeded`.
5. **Multi-pattern join** — two comma-patterns sharing a variable; assert the result is the smallest-first roaring intersection of the per-pattern bindings.
6. **Tail-merge over a traversal** (extends RFC 0001 test 3) — hold an index/reverse-projection watermark behind a write; run a traversal with the write's token; assert the tail overlay recovers the edge/node and the result matches the fully-caught-up oracle; assert deleted subtraction removes a tombstoned endpoint.
7. **Unindexed predicate** — errors `unindexed_property` by default; succeeds with `brute_force=true` under the guardrail; refused over the guardrail.
8. **ORDER BY / SKIP / LIMIT** — materialize-then-sort correctness on the bounded result, including `DESC` and ties (stable order), and `LIMIT` without `ORDER BY` truncating in ascending-uid order.
9. **N+1 batching** — assert a frontier expansion issues one batched multi-get, not N serial gets (via the RFC 0017 fan-out counter), and that filter push-down shrinks the frontier before the next hop.
10. **`index_behind` at the tail bound** — cap `tail_max_entries` low, exceed it with a lagging watermark; assert the query blocks then succeeds if the watermark advances, else returns retryable `index_behind`.

## 12. Final contract

- The v0 query surface is a **read-only openCypher subset**: `MATCH` node/relationship patterns (fixed and `*min..max` variable-length, `min >= 1`), `WHERE` with `AND`/`OR`/`NOT` and `= <> < <= > >= IN` plus label/property predicates, `RETURN` (nodes, node properties, optional `count(*)`), `ORDER BY`, `SKIP`, `LIMIT`. Writes are the JSON upsert API (RFC 0004); Cypher writes are RFC 0011. `WITH`, aggregations beyond `count(*)`, subqueries, path variables, `OPTIONAL MATCH`, `shortestPath`, and map projections are `unsupported_cypher` → RFC 0013.
- Cypher lowers to a **predicate IR** (`ScanByLabel`, `ScanByValueIndex`, `Expand`, `VarExpand`, `Filter`, `Intersect`/`Union`/`Difference`, `Project`, `Sort`, `Limit`) that is the only thing the planner and executor consume; RFC 0013's frontend targets the same IR — a frontend swap, not a rewrite.
- The planner anchors on the **most selective live index** (roaring `len()`), plans each hop as an adjacency posting-list read intersected with filter sets, orders multi-way AND **smallest-first**, and runs variable-length paths as **bounded BFS with a depth cap + uid dedup** (Q17). An unindexed predicate errors `unindexed_property` unless `brute_force=true` under the node-count guardrail (Q15).
- Execution is: freshness gate (RFC 0001) → index/adjacency access valid to `W = min(watermark over used indexes)`, all combined in roaring (RFC 0005) with deleted-node/edge subtraction → changelog-tail scan `Log[(W, latest]]` re-evaluating the full pattern on materialized base nodes/edges → merge `(candidates − deleted) ∪ tail matches` → fetch `Node[uid]` blobs → `ORDER BY`/`SKIP`/`LIMIT` in the executor (materialize-then-sort per Q4). Live indexes keep `W = latest`, so the tail is empty in steady state and index lag is invisible.
- N+1 — the dominant traversal cost — is mitigated by batched multi-get neighbor reads, filter push-down before the next frontier, smaller-side-first frontier heuristics, and a once-per-query cached deleted-node bitmap. CSR/WCOJ locality wins are deferred to RFC 0009.
- Bounds fail loudly, never silently: a var-length request over `bfs_depth_cap` errors `bfs_depth_exceeded`; a tail over `tail_max_entries` blocks then returns retryable `index_behind`. Correctness is never traded for latency in v0.
