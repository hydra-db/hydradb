# Postgres graph ports and traversal

These crates have no dependency on the root HydraDB engine, SlateDB, a native
graph library or a Postgres runtime.

- `hydradb-pg-graph-core` owns the synchronous `GraphRead` trait and owned
  vertex/edge models.
- `hydradb-pg-graph-csr` builds immutable outgoing/incoming CSR adjacency from
  a snapshot. Every physical relationship retains its ID and type. Duplicate
  vertex/edge IDs and dangling endpoints fail the build.
- `hydradb-pg-graph-query` implements the traversals as free functions over any
  `GraphRead`, and the path search over a `CsrGraph`, whose
  `CsrGraph::incident` reads any vertex's edges, since they are all in memory.

## Representation contract

Vertex IDs and edge IDs are unique within their own domains. Vertex order is
`(sort_key, id)`, using Rust string ordering. Edges are ordered by source vertex
order, target vertex order, then edge ID. `GraphRead::vertices` and
`loaded_edges` must use those orders, except in a graph whose `ordered` is
false: it loads sort keys only on demand, so its `loaded_edges` returns edges by
edge ID, and the traversals sort a layer by these orders only when a limit cuts
it. Both-direction adjacency emits a self-loop once.
All endpoints exist. Missing lookup IDs return `None`, and missing adjacency
IDs yield an empty iterator. Reads take `&self` and never change the graph; a
graph that loads on demand loads only in `load_edges` and `sort_by_order`, which
take `&mut self`. `sort_by_order` sorts vertices by sort key, ties broken in the
graph's `(label, key)` order, loading what that needs; it is the only way a
traversal orders vertices of such a graph, so it never reads a sort key it did
not load. A traversal reads edges only through `GraphRead::expand`,
which loads a layer's whole frontier in one batch and returns an `Expansion`
to read it: `loaded_edges` takes a token only `expand` can make, and a vertex
whose edges were not loaded is a `GraphError`, never a panic.

The ports use static dispatch, without `Send`, `Sync` or async requirements.
An adapter may therefore stay on its Postgres backend thread. They do not
establish MVCC visibility or authorization: the snapshot source must exclude
ineligible vertices and edges before traversal.

## Query semantics

Seeds are validated, sorted and deduplicated. Missing IDs, empty lists,
over-limit input lists and invalid configurations are errors, not truncations.
Input list limits apply before deduplication.

`reachable` returns seeds at depth zero and their union of reachable vertices
at minimum hop distance. Results are ordered by depth, then vertex order.
`nearby_edges` returns each physical edge once at one plus the minimum depth
of a walked endpoint permitted by the direction. Results are ordered by depth,
source vertex order, target vertex order and edge ID. The optional edge type
filter applies before any expansion.

## Budgets and interruption

The default depth is 5; its hard ceiling is 255. The vertex and edge defaults
and hard ceilings are 20,000 and 5,000. The seed list default is 32, with a
hard ceiling of 128. The adjacency-work default is 1,000,000 visits
with a hard ceiling of 10,000,000. The result-byte default is 8 MiB with a hard
ceiling of 64 MiB. Zero edge, work and byte budgets are supported; the vertex
and input-list budgets must be positive.

Work counts every examined adjacency entry, even when its type is filtered
out. Both-direction traversal can examine an edge from each endpoint.
Vertices count accepted BFS visits; nearby-edge queries do not enqueue or count
endpoints at the final edge depth because they will not be expanded.
Edge budgets count returned edges.

The byte budget counts retained result payload: `size_of::<ReachedVertex>()`
or `size_of::<ReachedEdge>()` per item. It is not a JSON wire-size limit or
an exact allocator/process-memory limit. Graph storage, container capacity,
query metadata and bounded BFS working sets are excluded. Working candidate
sets are independently bounded by the vertex/edge caps.

Every stop reason is explicit in `truncated`. For reachable queries, depth
means a nonempty boundary was reached, not proof of further eligible neighbors:
adjacency of max-depth vertices is never opened. Nearby-edge queries complete
through the requested edge depth without a depth marker. Vertex/edge caps retain complete shallow layers and
a sorted prefix of the final layer. They finish scanning that layer, subject
to the work and cancellation limits, using bounded candidate sets. Work or
cancellation can leave a partial final layer based on the entries examined.

`native_paths`, first ported from HydraDB's `algo.SPpaths`, `algo.SSpaths`
and `algo.MSpaths` (`src/shard/path_procedure.rs`), lists simple paths over
structural edges `(kind, source, target)`, breadth first, then expands each
path into its parallel relationships and selects by length, vertex IDs and
relationship. Its options are snake_case: `edge_kinds` (empty follows every
kind), `direction`, `max_depth` (default 16, ceiling 255), `paths_per_pair`
(default 1; 0 keeps every shortest path), `max_paths`, `unordered_pairs`,
`balance_parallel_edges` and the work limits. With targets it makes one
breadth-first pass, back from the targets: that pass scans every vertex within
`max_depth - 1` hops of a target, so it sees every relationship a path can use,
and the successor lists come from it. Without targets it walks out from the
sources. The parallel relationships are recorded as the pass scans each vertex,
so expansion reads no lists of its own and each examined relationship counts
once against `max_edge_visits`. With targets and `paths_per_pair` 1 it lists
no paths: one breadth-first walk over vertices per source finds a shortest
path to each target, through the lowest-id relationship of each structural
edge, so its cost grows with the region, not with the number of paths. The
graph is unweighted: there are no weight or cost options, and paths carry no
weight or cost. Over-budget work is a `LimitExceeded` error and cancellation is
`Cancelled`, not a truncation. `tests/paths.rs` ports HydraDB's procedure tests
and compares random graphs against a brute-force oracle.

`Checkpoint::is_cancelled` runs at query entry, frontier vertices, every
adjacency visit and path reconstruction. A `FnMut() -> bool` can implement it
directly, for example by checking Postgres interrupts. A cancelled query
returns partial results with `cancelled`; a host callback may instead raise
the host's cancellation error.

Run `just test-pg-graph` and `just check-pg-graph` from the repository root.
