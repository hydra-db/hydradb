---
title: "RFC 0006: Index Framework & Value / Reverse / Count Indexes"
status: draft
date: 2026-07-03T00:00:00Z
related:
  - 0000-rfc-index.md
  - 0003-keyspace-and-encoding.md
  - 0004-graph-data-model-and-write-path.md
  - 0005-posting-list-substrate.md
  - 0007-opencypher-read-path.md
---

# RFC 0006: Index Framework & Value / Reverse / Count Indexes

## Summary

A monolithic node blob (RFC 0004) stores a node but cannot be *scanned* by property. Everything a query filters or anchors on — a property value, a label, a degree, the reverse direction of an edge — is served by a **secondary index**, and in turbolay every index is a **posting list** (RFC 0005): a roaring set of node UIDs keyed by a token. This RFC decides the extension framework that owns indexes (`IndexAm`), the registry + watermark + backfill lifecycle, and the three concrete v0 index kinds (value, label, count) plus the already-materialized reverse projection.

This is the Dgraph insight — "an index entry is nothing more than a posting list whose key encodes a token instead of a source UID" (`ch15-dgraph-indexing`) — made concrete on the roaring substrate, with the Postgres access-method extension shape (`IndexAm`) so new index types plug in without touching the core.

## Storage vs indexing (answers "how do I filter on a property?")

Storage and indexing are **separate concerns**:

- A node's `birthdate` lives in the `Node[uid]` blob — for retrieval / `RETURN`.
- *If an index is declared on `birthdate`*, the writer **also** maintains a value-index posting list, so the property is searchable.

Filtering on an **unindexed** property is an error by default (Q15) — no silent full scans:

```json
{ "error": "unindexed_property", "property": "height",
  "hint": "declare an index or pass brute_force=true", "retryable": false }
```

with an opt-in `brute_force=true` that materializes and filters (RFC 0007), meant for small namespaces only.

## The `IndexAm` trait

Every index kind is an extension implementing one trait; the core knows only nodes, edges, the changelog, and this surface (RFC 0002 escape hatch).

```rust
trait IndexAm {
    /// From a changelog record, produce the (token, uid, add|del) mutations this index needs.
    fn extract(&self, change: &ChangeRecord, schema: &Schema) -> Vec<IndexMutation>;
    /// Apply mutations to this index's posting lists within the caller's WriteBatch.
    fn apply(&self, muts: &[IndexMutation], batch: &mut WriteBatch);
    /// Which predicate-IR shapes can this index answer? (consulted by the planner)
    fn supports(&self, pred: &PredicateIR) -> Option<AccessPlan>;
    /// Execute an AccessPlan → a roaring set of candidate node uids.
    fn execute(&self, plan: &AccessPlan, reader: &dyn GraphRead) -> RoaringTreemap;
}
```

`extract`/`apply` are the write side, `supports`/`execute` the read side. `supports()` returning `None` for a predicate is what makes the planner (RFC 0007) fall back to error-or-brute-force. All four operate over the RFC 0005 `PostingList` type — an `IndexAm` never re-implements set storage.

## Registry & lifecycle

### Declaration (Q-new = explicit)
Indexes are declared explicitly via the admin/REST API (RFC 0008), e.g. `create_index(label="Person", property="birthdate", tokenizer=int)`. The declaration is persisted in the schema keyspace (RFC 0003 `SchemaEntry.directives.index`) and registered as `m/index/{id}`. There is no auto-indexing in v0.

> **Future (noted per user):** (i) an openCypher `CREATE INDEX FOR (n:Person) ON (n.birthdate)` / `DROP INDEX` frontend onto this same registry — a frontend change riding the Cypher-write surface (RFC 0011), not new machinery; (ii) user-driven field selection generalized (the admin API is that input today; later also namespace config or Cypher DDL). Both reuse the registry + builder below unchanged.

### Steady-state maintenance is synchronous (single-writer simplification)
Because there is one writer and writes are atomic (D2), a **live** index is maintained **in the same `WriteBatch` as the data** (RFC 0004 write path step 4): the writer runs `extract`→`apply` for every live index touching the change, so the index commits atomically with the node/edge and is **never behind** (`watermark == latest_seq`). This is simpler and stronger than the sister FTS project's always-async build: no steady-state lag, no tail scan needed for live indexes.

### Backfill is the only async case
Creating an index on **existing** data needs a scan. A new index enters a state machine and backfills asynchronously; only during backfill does its watermark lag:

```
creating → backfilling → live → (dropping → gone)
```

- `backfilling`: a build loop scans existing nodes/edges, emits `extract`→`apply` mutations in batches, and advances `m/wm/{id}` to the last seq it has incorporated. Idempotent replay (re-applying a batch is a set union/difference, associative) makes crash-restart safe.
- Reads against a `backfilling` index use it up to `W = m/wm/{id}` and cover `(W, latest]` with the changelog tail (RFC 0001/0007) — so a half-built index never returns wrong answers, it just leans more on the tail.
- `live`: caught up; switches to synchronous in-batch maintenance; watermark tracks `latest_seq`.
- `dropping`: the index's keyspace is drained (range-delete under its prefix); the read path stops consulting it first.

Async decoupling of *live* maintenance (moving it off the write batch, like the sister project) is a deferred option if write-batch latency from many indexes shows up in RFC 0017 — the watermark machinery already supports it.

## The v0 index kinds

All four are `IndexAm`s over roaring posting lists. Keys per RFC 0003.

### 1. Value index — `Index[key_id][token] → node uids`
Node scalar properties (`key_id = prop_id`). Tokenizers (Q14 = A):

| Tokenizer | Value types | Token | Serves | Lossy? |
|---|---|---|---|---|
| `exact` | String | `terminated_bytes(value)` | `=`, `IN` | no |
| `hash` | String (long) | 8-byte fingerprint | `=`, `IN` (compact keys for long strings) | **yes** → re-fetch |
| `int` | Int, **DateTime** (epoch i64) | `sortable_i64` (BE) | `=`, `IN`, `< > <= >=` | no |
| `float` | Float | `sortable_f64` (BE) | `=`, `IN`, `< > <= >=` | no |

- **Range queries** work because `int`/`float` tokens are order-preserving (RFC 0003): `p.birthdate < D` → a bounded key-range scan `Index[birthdate_prop_id][ ..sortable(epoch(D)) )`, roaring-unioning the posting lists it spans.
- **DateTime** is indexed as epoch-millis via the `int` tokenizer in v0 — so "born before a date" is an ordinary int range scan. (Dgraph-style datetime granularity buckets — `year`/`month`/`day` — are deferred, see below.)
- **`hash` is lossy** (non-injective): `supports()` marks its `AccessPlan` re-fetch, and the executor re-checks the real property on candidate nodes. `exact`/`int`/`float` are non-lossy — no re-fetch. (This is Dgraph's `IsLossy()` contract; only `hash` needs it in v0.)
- A multi-valued (`list`) property indexes each element under its own token (array-contains-value falls out).

### 2. Label index — `LabelIndex[label_id] → node uids`
`MATCH (p:Person)` needs "all nodes with label Person." A node upsert adds its uid to each of its labels' posting lists; label removal is a difference. Anchoring or filtering by label is a roaring lookup, intersected with other predicate sets. (Implemented as a value-index variant with `key_id = label_id` in the label id-space; equivalently a dedicated tag — RFC 0003 keeps label ids in their own space.)

### 3. Count index — `Count[pred_id][dir][degree] → node uids`
Nodes with exactly `degree` edges on a predicate/direction. Maintained by moving a uid between degree buckets on edge add/delete (the writer knows old/new degree from the posting cardinality — roaring `len()` is O(1)). Serves degree predicates (`WHERE size((p)-[:KNOWS]->()) = 5`) without scanning adjacency. Dgraph's `@count`.

### 4. Reverse projection — `EdgeIn[dst][pred]` (already materialized)
Not a separate index type: reverse adjacency is materialized unconditionally as the in-projection (RFC 0004, Q16), so "who points at me on `pred`" is a symmetric posting-list read. Listed here because the planner treats it as the access path for reverse-direction hops.

## Operator → access mapping (what the planner consults)

`supports()` returns, per predicate-IR node:

| Predicate | Index / access | Roaring op |
|---|---|---|
| `prop = v` | value-index point lookup | one `get` |
| `prop IN [..]` | value-index multi-lookup | union |
| `prop < / <= / > / >= v` | value-index bounded range scan (`int`/`float`) | union over spanned tokens |
| `:Label` | label-index lookup | one `get` |
| `size((n)-[:p]->()) = k` | count-index lookup | one `get` |
| `(a)-[:p]->(b)` forward hop | `EdgeOut[a][p]` | membership |
| `(a)<-[:p]-(b)` reverse hop | `EdgeIn[a][p]` | membership |
| unindexed `prop <op> v` | `None` → error or `brute_force` | materialize + filter |

Combinations: `AND` → intersect (smallest-first, RFC 0005), `OR` → union, `NOT` → difference. All on roaring.

## Read consistency tie-in

The read path (RFC 0007) gates on the session token (RFC 0001), then: use each index up to its watermark (`latest_seq` for live indexes, `m/wm/{id}` for backfilling ones), take `W = min` over the indexes a query uses, scan the changelog tail `(W, latest]`, re-evaluate the pattern on materialized tail nodes/edges, and merge. Live indexes contribute `W = latest_seq`, so in steady state the tail is empty and the merge is a no-op — index lag only exists while an index is backfilling.

## Deferred

- **`term` / `trigram` / `fulltext`** tokenizers (word-match, substring, BM25) → fulltext index extension, RFC 0015. v0 text filtering is `exact`/`hash` equality only (a v0 consequence of Q14).
- **Datetime granularity buckets** (`year`/`month`/`day`/`hour`) → later value-tokenizer options; v0 uses epoch-`int`.
- **`geo` (S2), `vector` (ANN)** → their own `IndexAm` extensions.
- **Composite / multi-property indexes** → later; v0 composes single-property indexes via roaring AND.
- **Cypher `CREATE INDEX` DDL** and generalized user-driven field selection → RFC 0011 frontend on this registry.

## Alternatives considered

- **Properties as predicates (Dgraph literal).** Dgraph makes every scalar property a predicate/posting list, so indexing is uniform. We chose the monolithic node blob (RFC 0004, Q6) for cheap whole-node reads, which costs an explicit extract-into-value-index step — the standard secondary-index tradeoff. Accepted.
- **Auto-index every scalar property.** Zero-config filtering, but every property pays index write-amp + storage whether queried or not. Rejected for v0 (Q-new = A); explicit declaration only.
- **Always-async index build (sister FTS project).** Decouples index build from writes but makes every index lag. We maintain live indexes synchronously in the write batch (single-writer atomicity makes it free of coordination) and use the watermark/tail machinery only for backfill. Async live build remains available if write latency demands it.

## Acceptance

1. **Value index correctness**: declare `int` index; upsert nodes; assert `=`, `IN`, and range scans return exactly the oracle set; assert DateTime-as-epoch range ("before date") is correct across boundaries.
2. **Lossy `hash` re-fetch**: two distinct strings colliding to the same `hash` token are disambiguated by the re-fetch step.
3. **Label + intersection**: `MATCH (p:Person) WHERE p.age > 30` = label-set ∩ range-set equals the oracle.
4. **Count index**: adds/deletes move a uid between degree buckets; degree predicate matches the oracle.
5. **Backfill safety**: create an index on existing data; while `backfilling`, queries (with a session token) return correct results via index-to-watermark + changelog tail; assert idempotent restart mid-backfill; assert `live` transition makes the tail empty.
6. **Unindexed → error**: a filter on an undeclared property errors by default and succeeds with `brute_force=true`.
7. **Drop**: dropping an index drains its keyspace and the planner stops using it; no dangling posting lists (RFC 0017 invariant counter).

## Final contract

- Every index is a roaring posting list of node UIDs behind the `IndexAm` trait (`extract`/`apply`/`supports`/`execute`); the core interprets only nodes, edges, the changelog, and this trait.
- v0 index kinds: value (`exact`/`hash`/`int`/`float`, DateTime→epoch-int, ranges via order-preserving scan), label, count; reverse is the materialized in-projection.
- Indexes are declared explicitly (admin API); live indexes are maintained synchronously in the write batch (no steady-state lag); only a newly-created index backfills asynchronously with a watermark, covered by the changelog tail.
- Filtering on an unindexed property errors by default, opt-in `brute_force` for small namespaces.
- Cypher `CREATE INDEX` DDL, user-driven field selection, and richer tokenizers (`term`/`trigram`/`fulltext`/`geo`/`vector`) are deferred behind the same registry + trait.
