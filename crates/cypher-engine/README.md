# hydradb-cypher-engine

The isolated, experimental Cypher execution pipeline:

```text
Cypher 25 text
      ↓
parser-independent AST
      ↓
GraphLogicalPlan
      ↓
GraphPhysicalPlan
      ↓
GraphRead session
      ↓
query rows
```

This crate does not depend on the root `hydradb` package, SlateDB, or
libcypher. It also owns its experimental logical and physical plan types rather
than extending the older `hydradb-graph-plan` proof of concept consumed by
production `GraphShard`. New operators can therefore evolve here without
changing the current query path.

`GraphStorage::begin_read` creates one consistent `GraphRead` session for the
complete physical-plan execution. A future root-side adapter can map that
contract to `GraphShard` without making this crate depend on production storage
internals. Property seeks are semantic operators: a backend must return
cross-numeric matches such as integer `1` for floating `1.0`, regardless of its
exact KV encoding.

Vertex reads use one physical `VertexScan` leaf. The query planner passes a
`VertexScanRequest` containing the binding, label predicates, safely movable
filters, full-record output requirement, and an optional ordered window to
`SlateDbDataSource`. The provider prepares access using the coherent statistics
view, before execution opens a reader. Its private strategies cover ID/property
points, property presence, ordered windows, label prefixes, inventory reads,
and guarded access with bounded fallbacks.

Intrinsic vertex-identity collections use one batched `id-points` request.
The provider recognizes request-independent `n.id IN [...]` predicates and
complete OR trees of equality predicates on the same vertex identity. It
deduplicates and orders valid `u64` candidates and intersects conjunctive
identity collections. Mixed bindings, row-dependent values, incomplete OR
trees, and fallible conjunctions retain their filter evaluation boundaries.

`PreparedVertexScan` owns that strategy and derives its row estimate, ordering,
residual predicates, accepted-window flag, and possible inventory-scan flag.
The query planner removes filter/sort operators only when the provider accepts
the complete ordered window. Ordinary scans retain residual filtering in the
engine. Provider execution owns adaptive equality probes, guarded fallback,
ordered residual filtering, boundary ties, and hydration through the existing
`GraphRead` and request-local identity map. The output contract currently
requires complete vertex records; it does not promise projection pruning.

EXPLAIN shows `VertexScan(provider=slatedb, access=..., ...)`. Profiles use
`VertexScan` as the operator name and report the selected access separately.
Provider fallbacks are diagnostic details of that operator, not child physical
operators. `may_scan_inventory` describes prepared possibilities;
`ExecutionProfile::ran_full_scan` records inventory reads actually executed.
Plan shapes continue to redact literals, list cardinality, and derived limits.
These names and plan hashes intentionally change from the old seek/scan names.

The provider is a concrete engine-side implementation of the existing storage
contract, not a new SlateDB handle or an open plugin interface. This keeps
access selection, execution, estimates, and diagnostics together so a new
storage strategy does not require matching variants throughout the generic
planner. It preserves the existing row executor and has no DataFusion dependency.

`CypherEngine::prepare` exposes the AST, logical plan, and physical plan.
`CypherEngine::execute_prepared` executes that exact physical artifact without
re-planning, so `PreparedQuery::explain` and execution cannot diverge.

Storage-backed callers can use `CypherEngine::lower_with_lists` to parse and
lower once, load an immutable statistics view asynchronously, then call
`LogicalQuery::plan_with_provider(&SlateDbDataSource::new(&statistics))`.
`plan_with_statistics` remains a convenience for that same provider. Scalar and list parameters remain bound
across that boundary. The existing `prepare` and `prepare_with_lists` APIs
remain storage-free and use unknown statistics by default.

The HydraDB experimental query route plans with the per-scope statistics
object graph-indexer publishes beside the store (`GRAPH_PLANNER_STATISTICS`):
label and relationship-type counts, and per-property totals, distinct counts,
top values and ranges. The node reads it at most once per TTL and serves the
planner from memory; without one, a query plans with unknown statistics.
`EXPLAIN` uses the same statistics-aware planner as execution; client
authorization and column preparation remain storage-free.

Missing statistics and records from a future sequence or timestamp remain
unknown. Older records are estimates, never grounds to omit query results.
Bloom misses affect cost only and cannot suppress execution, so a stale filter
cannot create a false-negative query result. Total vertex and untyped
relationship counts remain unknown because no corresponding persisted record
exists. Valid escaped Cypher identifiers outside the storage-key alphabet
cannot have persisted statistics, so their statistics remain unknown without
failing query preparation.

The physical planner propagates cardinalities through filters, expansions,
unions, joins, and windows. Exact frequencies, histogram estimates, Bloom
membership, label intersections, and relationship counts choose between label
scans, property indexes, relationship-property seeks, and anchored expansion.
Relationship-property access compares against the persisted degree of the
actual source-label population, so rare hubs and rare low-degree sources do not
share an invented degree estimate. A missing expansion record stays unknown;
the database-wide relationship count is used only as a cardinality upper bound,
not to choose between those access paths.
For multi-label scans it selects the smallest single-label anchor while the
executor enforces the complete label predicate. Ordered-index/LIMIT pushdown
and range-distribution buckets remain separate physical-operator work.

The current relationship slice supports one directed hop in either direction,
optional relationship bindings, type filters, target labels, and relationship
property predicates. `GraphRead::expand_relationships` is a batched operator:
the engine validates direction and type, removes duplicate copies of the same
relationship identity, preserves distinct parallel relationships, hydrates
targets once, and emits deterministic `(target, relationship)` order.

Run its conformance slice from the repository root:

```bash
just test-cypher-engine
```
