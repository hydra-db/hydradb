#import "../book/vendor/bookly/src/bookly.typ": *
#import "../book/template.typ": term, why, srcblock, figcap, accent, muted

= From Cypher to Sparse Rows

A client sends text:

```cypher
MATCH (a)-[:FOLLOWS*1..3]->(b)
WHERE id(a) = 1
RETURN id(b)
```

The object store does not understand variables, patterns, predicates, or hop
ranges. It stores keys, values, manifests, and deltas. Between the query text
and those records, turbolay must decide what the request means, whether it is
supported, which access path can answer it, and which traversal kernel should
perform the expansion.

This chapter follows that bridge. Chapter 4 fixed the world at a read epoch;
the query engine must preserve that world while changing representations.

== Problem 1: accepting syntax is not the same as supporting semantics

Cypher is a large language. A parser can recognize more syntax than a graph
kernel knows how to execute correctly. Treating every successfully parsed
statement as executable would move unsupported behavior into deeper code,
where failure is harder to explain and partial execution is more dangerous.

turbolay separates parsing from lowering.

#term("Parsing")[
  Turning query text into a syntax tree that represents the language's clauses
  and expressions.
]

#term("Lowering")[
  Translating the general syntax tree into turbolay's smaller internal query
  representation. Unsupported constructs are rejected during this step so
  execution receives only shapes it implements.
]

The entry point makes the two phases visible:

#srcblock("src/query/opencypher.rs:216-222")[```rust
pub fn parse_opencypher_row_query_with_parameters(
    query: &str,
    parameters: &BTreeMap<String, VertexPropertyValue>,
) -> Result<ParsedRowQuery> {
    let parsed = ParsedCypher::parse(query)?;
    parsed.lower_row_query(parameters)
}
```]

The lowered row query is deliberately finite. It contains patterns, match
groups, union arms, a predicate, projections, ordering, a result window,
columns, and distinctness.

#srcblock("src/query/opencypher.rs:14-27")[```rust
pub struct ParsedRowQuery {
    pub patterns: Vec<RowPattern>,
    pub pattern_groups: Vec<RowMatchGroup>,
    pub union_arms: Vec<ParsedRowQuery>,
    pub union_all: bool,
    pub predicate: Option<RowPredicate>,
    pub projections: Vec<RowProjection>,
    pub order_by: Vec<RowSort>,
    pub window: QueryWindow,
    pub columns: Vec<QueryColumn>,
    pub distinct: bool,
}
```]

This is a safety boundary, not merely a compiler organization choice. Once a
query is lowered, the executor can match a closed set of pattern, predicate,
and projection variants instead of guessing what an arbitrary syntax node
means.

#boxeq[
  *Parse broadly enough to understand the request; lower narrowly enough to
  execute only behavior the kernel can prove.*
]

== Problem 2: repeated text should not repeat pure work

Parsing and lowering a parameter-free query produce the same internal form
regardless of the current graph epoch. Repeating that work on every request
wastes CPU without improving correctness.

The parsed-row-query cache is keyed by the query string. Parameterized queries
are re-lowered with their values, while parameter-free queries can reuse the
cached `ParsedRowQuery` (`src/shard/query.rs`).

This cache is unusual because it does not include a graph epoch. That is safe:
the parse result describes the question, not its answer. Result caches,
adjacency caches, and traversal caches do depend on graph state and therefore
carry an epoch or artifact base in their keys.

#figure(
  table(
    columns: (1.35fr, 1.25fr, 1.4fr),
    inset: 8pt,
    align: (left + top, left + top, left + top),
    table.header([*Cached value*], [*Depends on graph contents?*], [*Version in key?*]),
    [Lowered query], [No], [No; query text is sufficient],
    [Reachability result], [Yes], [`read_epoch`],
    [Hydrated matrix adjacency], [Yes], [`base_epoch`],
    [Relationship rows], [Yes], [`read_epoch`],
  ),
  caption: [Cache identity follows semantic dependency, not a uniform convention.],
)

== Problem 3: one pattern can have several valid access paths

Consider the edge pattern `(a)-[:FOLLOWS]->(b)`. Its cheapest execution depends
on what is already known:

- if `a` is bound, expand outgoing neighbors;
- if `b` is bound, expand incoming neighbors;
- if both are bound, test one edge;
- if a selective property is indexed, begin from that index;
- if neither endpoint is known, scan the edge type as a last resort.

#term("Access path")[
  The physical method chosen to satisfy a logical pattern: point lookup,
  adjacency expansion, reverse expansion, property index, artifact-backed
  traversal, or scan.
]

turbolay's row optimizer evaluates candidates using estimated cardinality and
a stable priority. Per-cell query statistics provide counts and selectivity
hints; stale statistics are penalized so an old estimate does not look
artificially cheap (`src/shard/query_optimizer.rs`).

#term("Cardinality estimate")[
  A prediction of how many rows an access path will produce. Smaller
  intermediate results usually mean less storage work, less memory, and fewer
  later predicate evaluations.
]

#figure(
  table(
    columns: (1.25fr, 1.3fr, 1.45fr),
    inset: 8pt,
    align: (left + top, left + top, left + top),
    table.header([*Known bindings*], [*Likely access path*], [*Work avoided*]),
    [Source only], [Outgoing neighbor expansion], [Full edge-type scan],
    [Destination only], [Incoming neighbor expansion], [Forward scan and filtering],
    [Both endpoints], [Edge-exists point check], [Materializing either adjacency list],
    [Selective indexed property], [Property-index lookup], [Testing the property on every row],
    [No useful binding or index], [Bounded scan], [No cheaper supported path exists],
  ),
  caption: [The logical edge stays the same while its cheapest physical route changes.],
)

#why[
  Optimization is allowed to change cost, not meaning. Every candidate still
  receives the same cell ID and read epoch, so choosing a point lookup instead
  of a matrix scan cannot move the query to a different snapshot.
]

== Problem 4: a query needs budgets as well as a plan

Even a valid plan can be operationally dangerous. An unanchored pattern can
scan a large edge type. A broad variable-length expansion can visit an
explosive number of edges. A result without a practical window can consume
memory long after the client has stopped reading.

The execution context therefore carries limits and cancellation state.
`QueryContext` includes the scope, cell, idempotency identity, optional read
epoch, result window, parameters, and runtime controls. Deep loops use a query
budget to check cancellation, elapsed time, and configured scan limits.

#term("Query budget")[
  The execution limits carried into storage scans and traversal loops. A
  budget turns cancellation, maximum runtime, scan limits, and result limits
  into checks inside long-running work rather than advice at the API boundary.
]

Budgets are part of correctness at the service boundary. Without them, one
syntactically valid query can monopolize shared compute and make unrelated
cells unavailable.

The result window also travels through planning. `SKIP`, `LIMIT`, and paging
are applied under a fixed epoch so a later page does not drift into a newer
graph version.

== Problem 5: row matching must meet versioned storage

After planning, a bound edge source becomes a neighbor read. The executor asks
for neighbors at the pinned epoch and turns each destination into a candidate
binding.

#srcblock("src/shard/query.rs:3095-3105 (abridged)")[```rust
for src in sources {
    budget.check("cypher_edge_sources")?;
    let neighbors = self
        .out_neighbors_at_for_query(
            cell_id,
            &edge.edge_type,
            src,
            read_epoch,
            budget,
        )
        .await?;
    scanned_edges = scanned_edges.saturating_add(neighbors.len() as u64);
    for dst in neighbors {
        self.push_matching_edge_row(edge, src, dst, &mut state).await?;
    }
}
```]

This is the seam between the logical plan and Chapter 4's reconstruction
rules. The neighbor method may use canonical records, an eligible artifact,
segments, scoped delta indexes, or a specialized fast path. Its output must
still be the adjacency at `read_epoch`.

Candidate bindings then pass through predicates and projections. Predicates
form a tree of comparisons and Boolean operators. Projections produce vertex
IDs, properties, counts, aggregates, or lists. The final stage applies
distinctness, ordering, and the requested window before returning rows.

#figure(
  table(
    columns: (1fr, 0.3fr, 1fr, 0.3fr, 1fr),
    inset: 8pt,
    align: center,
    [Cypher text], [`→`], [lowered row query], [`→`], [access choice],
    [versioned graph reads], [`→`], [candidate bindings], [`→`], [filter and project],
    [result window], [`→`], [rows or page], [], [],
  ),
  caption: [Each stage narrows representation while the read epoch stays fixed.],
)

== Problem 6: multiple hops need a graph-shaped execution model

One-hop expansion can scan one adjacency row. A bounded path such as
`*1..3` repeatedly expands a frontier. Expressing that as nested generic row
loops is possible, but the operation is naturally sparse graph algebra.

#term("Sparse adjacency")[
  A map from a source vertex to the set of its destination vertices. It stores
  only present edges rather than a dense matrix containing mostly zeros.
]

In the current implementation the hydrated adjacency type is:

#srcblock("src/lib.rs:156-159")[```rust
pub type VertexId = u64;
pub type GraphEpoch = u64;
pub(crate) type MatrixAdjacency =
    BTreeMap<VertexId, BTreeSet<VertexId>>;
```]

The outer map locates a source row. The inner ordered set deduplicates
destinations and gives deterministic iteration. This is compute-local state;
the durable graph remains SlateDB records and artifact bytes.

This representation can evolve independently of the durable format. In this
branch the source uses `BTreeSet` rows and contains no Roaring dependency. A
future compressed-row implementation would still need to preserve `u64`
vertex IDs, deduplication, deterministic iteration, and delta-overlay
semantics.

#boxeq[
  *The sparse row is an execution representation, not the durable definition
  of an edge.*
]

== Problem 7: traversal needs a general backend and optional acceleration

A bounded reachability request begins with one or more start vertices and
repeatedly expands their outgoing rows. The pure-Rust sparse kernel keeps a
frontier, a seen set, and the accumulated result.

#term("Frontier")[
  The vertices discovered at the current hop. Expanding their adjacency rows
  produces the next frontier; already-seen vertices are removed to prevent
  repeated work.
]

turbolay exposes two backend names:

#srcblock("src/sparse_kernel.rs:14-18")[```rust
pub enum SparseKernelBackend {
    RustSparse,
    SuiteSparseGraphBlas,
}
```]

The Rust backend is the ordinary, available path. With the `graphblas` feature,
the engine can compile or hydrate a SuiteSparse GraphBLAS matrix and perform
supported expansion there. GraphBLAS is an optional physical backend, not a
requirement for storing or reading the graph.

The engine chooses among several levels of acceleration:

#figure(
  table(
    columns: (1.35fr, 1.35fr, 1.35fr),
    inset: 8pt,
    align: (left + top, left + top, left + top),
    table.header([*Available structure*], [*Execution*], [*Fallback condition*]),
    [Exact-epoch compiled matrix], [Use the compiled sparse backend], [Feature or exact artifact unavailable],
    [Older matrix artifact], [Hydrate, overlay deltas, then expand], [No eligible published artifact],
    [Posting or canonical data], [Build/scan the required adjacency and expand], [Always the truth-preserving base fallback],
    [Supernode materialization], [Serve bounded high-degree neighbor work in pages], [Vertex not materialized or snapshot mismatch],
  ),
  caption: [Traversal acceleration is a ladder; every rung preserves the same epoch.],
)

The exact-epoch compiled path avoids overlay work. The general artifact path
hydrates a base and applies changes through the read epoch before traversal.
If neither is available, the engine reconstructs from durable graph records.

No backend is permitted to answer merely because it is fast. Its artifact
epoch, direction, edge type, and cell must match the request.

== Problem 8: high-degree vertices need a bounded interface

A vertex with millions of neighbors is not operationally equivalent to an
ordinary row. Returning the entire adjacency in one allocation can exceed
memory and result limits even when the query asks for one hop.

#term("Supernode")[
  A vertex whose degree is high enough to justify materialized, paged neighbor
  structures. turbolay represents these with supernode groups and posting
  chunks that can be hydrated and overlaid at a requested epoch.
]

The supernode path changes the unit of work from “load every neighbor” to
“load a bounded page or test a focused operation.” It supports degree reads,
page retrieval, edge existence, and intersection without forcing every caller
through one enormous vector.

Supernode structures remain derived. A missing group or cache entry must fall
back to other durable paths. Materialization cannot be the only record of an
edge.

== The complete local query model

One cell-local read query now has the following shape:

1. Classify the statement as a read or write and authorize the action.
2. Pin the cell read epoch.
3. Parse Cypher and lower only supported constructs.
4. Reuse a cached parse only when its semantic inputs match.
5. Choose access paths using bindings, indexes, and bounded statistics.
6. Carry budgets into scans and traversal loops.
7. Reconstruct every graph-dependent input at the pinned epoch.
8. Use row execution, sparse traversal, or a specialized path as appropriate.
9. Apply predicates, aggregates, ordering, distinctness, and the result window.
10. Return rows or pages while preserving the same snapshot.

#figure(
  table(
    columns: (1.15fr, 1.4fr, 1.45fr),
    inset: 8pt,
    align: (left + top, left + top, left + top),
    table.header([*Boundary*], [*Question answered*], [*Must remain unchanged*]),
    [Lowering], [Can this query shape be executed?], [Cypher semantics],
    [Optimization], [Which valid access path is cheapest?], [Bindings and read epoch],
    [Storage seam], [Which records reconstruct the pattern?], [Cell-local snapshot],
    [Sparse kernel], [How are repeated hops expanded?], [Edge type, direction, hop bounds],
    [Projection], [Which values become result columns?], [Predicate and aggregation meaning],
  ),
  caption: [Query execution changes representation repeatedly without changing the question.],
)

The chapter's central claim is:

#boxeq[
  *A query plan is correct when every optimization can be removed and the
  slower durable path still produces the same rows at the same epoch.*
]

== What the local query engine guarantees—and what it does not

The current engine provides:

- explicit rejection of unsupported lowered Cypher shapes;
- cell-local row patterns, predicates, projections, aggregates, ordering, and
  result windows for implemented forms;
- access selection from bindings, indexes, statistics, and specialized paths;
- bounded runtime, scan, cancellation, and result checks;
- pure-Rust sparse traversal, with optional GraphBLAS acceleration;
- epoch-preserving artifact, delta, posting, and supernode reads.

It does not imply:

- complete Cypher language compatibility;
- unlimited or unbounded variable-length traversal;
- that GraphBLAS is enabled in every build;
- that the current hydrated row type is Roaring-compressed;
- that every query uses a matrix rather than canonical records or indexes;
- that a local planner automatically becomes a distributed planner.

== Revision notes

=== The ideas to remember

- *Lowering is the support boundary.* Parsing a construct does not prove the
  kernel can execute it.
- *Logical patterns admit several physical paths.* Bindings and statistics
  choose cost; they do not change meaning.
- *Budgets belong inside loops.* Cancellation and limits must reach scans and
  frontier expansion.
- *Every graph read keeps the pinned epoch.* Point reads, indexes, artifacts,
  postings, and matrices are interchangeable only when version-correct.
- *Sparse rows are compute state.* The current source uses
  `BTreeMap<u64, BTreeSet<u64>>`; durable formats remain separate.
- *GraphBLAS is optional acceleration.* The Rust sparse backend preserves a
  general execution path.
- *Supernodes need bounded operations.* Pages and intersections avoid turning
  one high-degree row into an unbounded allocation.

=== A quick correctness test

1. Is an unsupported syntax shape rejected before execution begins?
2. Can the chosen access path return a different binding set than the fallback?
3. Does every graph-dependent operation receive the same read epoch?
4. Are cancellation and scan limits checked inside long-running work?
5. Does the optional backend have a truth-preserving Rust or storage fallback?
6. Can a high-degree result be paged or otherwise bounded?
7. Is a compute-local representation being mistaken for a durable format?

#boxeq[
  *Cypher becomes executable by narrowing: text to supported meaning, meaning
  to a bounded plan, and the plan to versioned graph operations.*
]
