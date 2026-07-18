#import "../book/vendor/bookly/src/bookly.typ": *
#import "../book/template.typ": term, why, srcblock, figcap, accent, muted

= A Cost Map for Graph Queries

A graph question is not easy or hard merely because its Cypher text is short.
Its cost depends on how much of the storage hierarchy it crosses, whether it
starts from a selective key, how much intermediate state it creates, and
whether the final answer can be merged from independent cell-local answers.

This chapter gives an application-facing cost map for a hierarchy such as:

```text
tenant
└── sub-tenant
    ├── document A
    │   ├── graph 1
    │   └── graph 2
    └── document B
        └── graph 3
```

The examples assume that documents and graphs are mapped deliberately onto
`GraphScope` and cells. The precise mapping is application policy; turbolay
does not discover it from a Cypher pattern.

== The six questions that predict query cost

Before assigning a difficulty label, ask six questions.

#figure(
  table(
    columns: (1.05fr, 1.45fr, 1.45fr),
    inset: 8pt,
    align: (left + top, left + top, left + top),
    table.header([*Axis*], [*Cheaper end*], [*More expensive end*]),
    [Scope fan-out], [One known cell], [Every graph and cell in a sub-tenant],
    [Starting point], [Known vertex or exact indexed value], [Unbound pattern or broad predicate],
    [Storage access], [Point or adjacency-prefix lookup], [Scan, hydrate, then filter],
    [Traversal], [One hop from a bound vertex], [Wide or repeated multi-hop frontier],
    [Result operator], [Bounded rows or a local counter], [Global sort, distinct, grouping, or collection],
    [Snapshot], [One pinned cell epoch], [A cross-cell view requiring shared temporal meaning],
  ),
  caption: [Query difficulty is the product of scope, selectivity, expansion, and merge work.],
)

#term("Easy query")[
  A query routed to one known cell with a selective starting key and an access
  path that returns bounded work. Its answer requires no global merge.
]

#term("Medium query")[
  A query that remains cell-local or has small explicit fan-out, but performs
  a scan, metadata hydration, bounded traversal, grouping, or sorting.
]

#term("Hard query")[
  A query whose cost grows with graphs, cells, frontier width, or materialized
  intermediate rows, especially when correctness requires a global merge.
]

#term("Not native today")[
  A useful query shape for which the current kernel has no automatic planner
  or coordinator operation. The application may implement an explicit
  protocol, but a single submitted Cypher query does not provide the result.
]

The labels describe execution shape, not business importance. A hard query can
be worthwhile, and an easy query can still be expensive when it returns a
supernode with millions of neighbors.

== The locality ladder

The most useful first classification is the smallest scope that contains the
complete answer.

#figure(
  table(
    columns: (1.1fr, 1.25fr, 1.55fr),
    inset: 8pt,
    align: (left + top, left + top, left + top),
    table.header([*Answer lives in*], [*Typical execution*], [*Expected class*]),
    [One record], [Point lookup by vertex or edge identity], [Easy],
    [One adjacency row], [Outgoing or incoming prefix, degree counter], [Easy unless it is a supernode],
    [One cell], [Index lookup, bounded scan, local aggregate, or local traversal], [Easy to medium],
    [Several cells], [Explicit concurrent query legs plus merge], [Medium to hard],
    [Several graphs in a document], [Application enumerates graph scopes and cells], [Hard unless the set is small and bounded],
    [A whole sub-tenant], [Catalog lookup, fan-out, and global reduction], [Hard or not native today],
  ),
  caption: [Every step up the locality ladder adds routing and merge obligations.],
)

`GraphScope` names one namespace path and one graph. A `QueryContext` then
names one cell inside that scope. This means a cell-local executor does not
interpret “this document” or “this sub-tenant” as an automatic fan-out rule.

#srcblock("src/query/algebra.rs:89-101 (abridged)")[```rust
pub struct QueryContext {
    pub scope: GraphScope,
    pub cell_id: String,
    pub read_epoch: Option<GraphEpoch>,
    pub result_window: QueryWindow,
    // parameters, timeout, cancellation, and request identity ...
}
```]

#boxeq[
  *A business scope becomes a cheap query scope only when the placement model
  keeps the required answer in a small, known set of cells.*
]

== Easy paths: selective and cell-local

The easiest paths already know both the graph scope and the cell.

=== Point identity

Questions such as these are naturally selective:

```text
Does entity 42 exist here?
Does 42 -[MENTIONS]-> 91 exist here?
What metadata belongs to entity 42?
```

A bound edge can become an edge-existence lookup. A bound source can become an
outgoing adjacency lookup. A bound destination is cheap when the reverse index
is available; otherwise the engine may need a broader forward-edge scan.

=== One-hop adjacency and degree

These are also direct cell-local shapes:

```cypher
MATCH (e)-[:MENTIONS]->(x)
WHERE id(e) = 42
RETURN id(x)
```

```text
How many MENTIONS edges leave entity 42?
```

The first follows an adjacency prefix. The second can use the maintained
degree counter instead of scanning every edge. The physical keys are aligned
with those questions:

```text
cell/{cell}/e/out/{type}/{src}/{dst}
cell/{cell}/e/in/{type}/{dst}/{src}
cell/{cell}/cnt/out/{type}/{src}
cell/{cell}/cnt/in/{type}/{dst}
```

A high-degree vertex changes the operational cost. Supernode pages and
posting structures keep the interface bounded, but asking for every neighbor
still produces every neighbor.

=== Exact indexed entity selection

Finding entities by an exact label or property value can use the label and
property indexes:

```cypher
MATCH (e:Person {country: "IN"})
RETURN id(e)
LIMIT 100
```

This stays easy when the equality value is selective and the result window is
small. The index does not make a common value selective; ten million matches
still mean ten million candidates unless the query stops early safely.

== Medium paths: bounded work with reconstruction

Medium queries have a known boundary but do more than one focused lookup.

=== All entities in one document

This query is easy only when the document maps to one known cell and “entity”
has a maintained label or exact property index:

```cypher
MATCH (e:Entity)
RETURN id(e)
```

If one document contains several graph scopes or cells, the application must
issue one leg per cell and union the rows. The operation moves from easy to
medium as fan-out grows. Pagination must preserve a cursor and read epoch for
every participating leg.

=== Cell-local range predicates

The row engine supports ordered comparisons on compatible numeric or string
values:

```cypher
MATCH (e:Entity)
WHERE e.score >= 50 AND e.score < 80
RETURN id(e), e.score
```

The important distinction is semantic support versus an ordered range access
path. Current property indexes are strongest for exact encoded values. A range
predicate may therefore begin from a label or another equality constraint,
hydrate candidates, and filter them. It is medium when the candidate set is
bounded and hard when it approaches a full cell scan.

=== Bounded local aggregation

The local row engine implements `count`, `sum`, `avg`, and `collect`, including
group keys. A focused aggregate can be reasonable:

```cypher
MATCH (e:Entity {document_id: "doc-7"})
RETURN e.kind, count(*)
```

Aggregation is not automatically a counter lookup. General grouping builds
state proportional to the input rows and number of groups. `collect` also
retains the collected values, so it needs a result and memory budget.

=== Anchored bounded multi-hop traversal

An anchored traversal such as this has a finite frontier and one cell-local
snapshot:

```cypher
MATCH (a)-[:RELATED_TO*1..3]->(b)
WHERE id(a) = 42
RETURN id(b)
```

The general Rust sparse backend expands adjacency rows. When the `graphblas`
feature and a compatible matrix are available, SuiteSparse GraphBLAS can
accelerate supported range expansion. An older artifact may first be hydrated
and advanced with ordered deltas to the requested epoch.

#boxeq[
  *GraphBLAS reduces local frontier-expansion cost; it does not remove cell
  boundaries, discover document placement, or provide a global snapshot.*
]

Traversal difficulty grows with the number of edges visited, not merely with
the maximum hop count. Three hops through low-degree data may be small; three
hops from a dense hub may touch most of the cell.

The current variable-length query fast path also requires a fixed source ID.
An unanchored `*1..N` pattern should therefore be classified as unsupported,
not advertised as a slow but general traversal.

== Hard paths: broad scope or global reduction

=== All entities in a sub-tenant

This request first needs a catalog answer: which documents, graphs, and cells
belong to the sub-tenant? The graph query coordinator does not discover that
set from Cypher. After enumeration, the application can run explicit legs
concurrently and `UNION ALL` compatible rows.

The result is hard when the scope is large because it requires:

1. catalog enumeration and placement routing;
2. one pinned snapshot per cell;
3. fan-out and failure handling;
4. duplicate policy across graphs;
5. global pagination or ordering state.

Equal epoch numbers in different cells do not create one sub-tenant snapshot.

=== Every relationship for an entity across a sub-tenant

Within a known cell, outgoing and incoming relationship lookup is direct. At
sub-tenant scope, the answer is hard unless an external directory maps the
entity to the cells that may contain its incident edges.

Without that directory, the safe plan is scatter/gather across every relevant
cell. If the same logical entity or relationship is copied into several
graphs, the coordinator also needs an application-defined identity and
deduplication rule.

This is the central locality question for partitioning:

#boxeq[
  *Partitioning writes by document is excellent for document-local queries,
  but a sub-tenant-wide entity query needs a routing index or broad fan-out.*
]

=== Global range, sort, and pagination

A range filter across many cells requires local filtering followed by a
global merge. A correct top-$k$ query cannot generally take an arbitrary
`LIMIT k` independently from each cell unless every leg orders by the same
key and the coordinator performs a k-way merge under stable cursors.

The present distributed merge vocabulary does not provide global order,
distinctness, or range-aware cursor merging. Materializing all rows and
sorting in the application may work for a bounded administrative query, but
it is not a scalable general read path.

=== Distributed aggregation

Local aggregates are implemented, but the distributed coordinator currently
merges rows with `UNION ALL` or one two-leg inner join. It does not perform a
distributed aggregate.

Some aggregates are algebraically mergeable when the application supplies the
second phase:

```text
global count = sum(local counts)
global sum   = sum(local sums)
global avg   = sum(local sums) / sum(local counts)
```

Grouped aggregates require merging equal group keys. `collect`, global
distinct, percentiles, and top-$k$ require more coordinator state and explicit
memory limits.

=== Cross-cell multi-hop traversal

A traversal becomes distributed when a frontier vertex in cell A owns edges
in cell B. Correct execution must route the new frontier, deduplicate visited
vertices, repeat the expansion, and carry a defined snapshot vector through
every hop.

The current distributed coordinator runs independent terminal legs and then
unions or joins their completed rows. It does not redistribute an intermediate
frontier for another graph expansion. GraphBLAS operates behind a cell-local
leg and therefore does not solve this coordination problem.

== What is not native today

The following should not be presented as automatic single-query capabilities:

- namespace or sub-tenant discovery from an arbitrary Cypher statement;
- automatic decomposition of one Cypher query across all matching cells;
- a global snapshot across those cells;
- distributed grouping, aggregation, distinctness, or total ordering;
- iterative cross-cell multi-hop frontier routing;
- a general ordered property-range index and distributed range cursor;
- cross-graph entity deduplication without an application identity policy.

These operations can be built above the kernel using catalogs, routing
indexes, explicit query legs, and purpose-specific reducers. Their absence
from the automatic planner is different from their being impossible at the
application layer.

== A query map for the document hierarchy

#figure(
  table(
    columns: (1.5fr, 0.75fr, 1.25fr, 1.3fr),
    inset: 7pt,
    align: (left + top, center + top, left + top, left + top),
    table.header([*Question*], [*Class*], [*Best path*], [*Main risk*]),
    [Entity by ID in one known graph], [Easy], [Point/index lookup in one cell], [Wrong placement information],
    [Neighbors or degree of a bound entity], [Easy], [Adjacency prefix or counter], [Supernode-sized result],
    [All labeled entities in one cell], [Easy/medium], [Label index plus bounded page], [Low-selectivity label],
    [All entities in one document], [Medium], [Explicit union over its known cells], [Per-leg paging state],
    [Property range in one cell], [Medium/hard], [Selective anchor then filter], [Broad candidate scan],
    [Bounded multi-hop in one cell], [Medium/hard], [Sparse rows or GraphBLAS], [Frontier explosion],
    [All entities in a sub-tenant], [Hard], [Catalog fan-out plus union], [No global snapshot/order],
    [All incident relationships across sub-tenant], [Hard], [Entity-to-cell routing index], [Scatter and deduplication],
    [Sub-tenant aggregate], [Hard/not native], [Local partials plus custom reducer], [Coordinator memory and semantics],
    [Cross-cell multi-hop], [Not native], [Iterative distributed traversal protocol], [Frontier routing and snapshot vector],
  ),
  caption: [The same logical question changes class when its placement boundary changes.],
)

== Design guidance

For a tenant, sub-tenant, document, and graph hierarchy:

1. Keep document-local data colocated when document reads dominate.
2. Maintain a catalog from sub-tenant to documents, graphs, and cells; the
   graph kernel is not that discovery catalog.
3. Maintain an entity-to-cell directory if sub-tenant-wide incident-edge
   queries are frequent.
4. Use exact label and property indexes for common selective entry points.
5. Treat range access as a separate index requirement; comparison syntax alone
   does not guarantee range-efficient storage access.
6. Precompute or incrementally maintain frequent sub-tenant aggregates rather
   than scanning every graph on demand.
7. Keep multi-hop traversal inside a cell when possible. If cross-cell hops are
   required, design frontier routing and snapshot semantics explicitly.
8. Require limits, timeouts, and stable per-cell cursors on every broad query.

#why[
  The best partition boundary is not the smallest writable unit in isolation.
  It is the smallest unit that preserves the important read neighborhoods.
  More cells increase write concurrency, but every relationship cut by a cell
  boundary becomes routing work for reads.
]

== Revision notes

=== The ideas to remember

- *Locality dominates syntax.* A short sub-tenant question can be harder than
  a longer cell-local Cypher query.
- *Anchors dominate scans.* IDs and exact indexes keep candidate sets small.
- *Degree is easier than enumeration.* A maintained counter can answer a count
  without returning every neighbor.
- *GraphBLAS is local acceleration.* It speeds supported sparse expansion but
  does not create a distributed planner.
- *Distributed merge is intentionally small.* Current coordination provides
  explicit legs, `UNION ALL`, and one inner-join shape—not global analytics.
- *Partitioning trades writes for reads.* Small cells reduce write contention;
  colocated neighborhoods reduce query fan-out.

=== A quick classification test

1. Can the router name every required graph and cell without scanning a catalog?
2. Does the query begin from an ID or selective exact index?
3. Is every traversal frontier contained in one cell?
4. Can the answer be returned without materializing all candidates?
5. Does the coordinator support the required global merge?
6. Is one cell-local snapshot sufficient for the business meaning?

#boxeq[
  *Easy queries preserve locality. Hard queries cross placement boundaries or
  require global state. The schema and cell map decide which class dominates.*
]
