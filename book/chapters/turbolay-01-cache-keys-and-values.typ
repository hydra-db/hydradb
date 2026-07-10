````typst
== The cache keys and values

The cache layer uses three different keys because it caches three fundamentally
different things:

- a precomputed adjacency matrix for a cell;
- a parsed openCypher query;
- the result of a specific reachability traversal.

The rule behind all three is simple: a cache key must contain every input that
can change the answer. Anything that does not affect the answer does not belong
in the key.

=== `MatrixCacheKey`: one cached adjacency matrix

`MatrixCacheKey` identifies the precomputed connectivity structure for one
locality cell, one edge type, and one graph version:

```rust
pub struct MatrixCacheKey {
    pub cell_id: String,
    pub edge_type: String,
    pub base_epoch: GraphEpoch,
}
````

#table(
columns: (auto, auto, 1fr),
align: (left, left, left),
stroke: none,
inset: (x: 7pt, y: 4pt),
table.hline(),
table.header([*Field*], [*Type*], [*Purpose*]),
table.hline(),
[`cell_id`],
[`String`],
[Identifies the locality cell whose vertices are covered by the matrix.
Matrices are built per cell rather than for the entire graph.],

[`edge_type`],
[`String`],
[Identifies which relationship type the matrix represents, such as
`FOLLOWS` or `PURCHASED`. Each edge type forms a different connectivity
structure and therefore needs a separate matrix.],

[`base_epoch`],
[`GraphEpoch` (`u64`)],
[Identifies the graph version from which the matrix was built. When the
graph moves to a new epoch, lookups naturally use a different key. The old
matrix therefore stops receiving normal lookups, although it may still be
usable as a base onto which newer deltas are applied.],
table.hline(),
)

The key is deliberately coarse-grained. It does not contain a source vertex,
hop count, pagination window, or individual query shape. One matrix is shared
by every traversal that operates over the same cell, edge type, and base
epoch.

#boxeq[
`MatrixCacheKey` identifies reusable graph structure, not the answer to one
traversal.
]

=== `ParsedRowQueryCacheKey`: one parsed query

`ParsedRowQueryCacheKey` exists only when the `opencypher` feature is enabled.
It identifies the parsed representation of a query string:

```rust
pub struct ParsedRowQueryCacheKey {
    pub query: String,
}
```

#table(
columns: (auto, auto, 1fr),
align: (left, left, left),
stroke: none,
inset: (x: 7pt, y: 4pt),
table.hline(),
table.header([*Field*], [*Type*], [*Purpose*]),
table.hline(),
[`query`],
[`String`],
[Stores the exact query text supplied to the parser. Parsing depends only
on the input characters, not on the graph contents, cell, epoch, or source
vertex. The same query text therefore maps to the same parsed
representation.],
table.hline(),
)

The graph epoch is intentionally absent. A graph mutation may change the
result of a query, but it does not change how the same query text is parsed.

#note[
This remains correct only while parsing is independent of external context.
If parsing later depends on a language version, parser configuration,
feature set, schema, or session option, those inputs must also become part
of the key.
]

=== `ReachabilityCacheKey`: one exact traversal question

`ReachabilityCacheKey` identifies the cached answer to a bounded reachability
query:

```rust
pub struct ReachabilityCacheKey {
    pub cell_id: String,
    pub edge_type: String,
    pub src: VertexId,
    pub min_hops: u8,
    pub max_hops: u8,
    pub read_epoch: GraphEpoch,
    pub window: Option<ReachabilityCacheWindow>,
}
```

Conceptually, it answers a question of the form:

#boxeq[
Starting from vertex `src`, inside cell `cell_id`, which vertices are
reachable through edges of type `edge_type` using between `min_hops` and
`max_hops`, when reading graph snapshot `read_epoch`?
]

This is the cache used for variable-length patterns such as:

```cypher
(a)-[:FOLLOWS*1..2]->(b)
```

#table(
columns: (auto, auto, 1fr),
align: (left, left, left),
stroke: none,
inset: (x: 7pt, y: 4pt),
table.hline(),
table.header([*Field*], [*Type*], [*Purpose*]),
table.hline(),
[`cell_id`],
[`String`],
[Identifies the locality cell in which the traversal runs. Keeping it in
the key also allows cache management code to group or evict entries
belonging to one cell.],

[`edge_type`],
[`String`],
[Identifies the relationship type followed by the traversal. Traversing
`FOLLOWS` and traversing `PURCHASED` are different questions even when
every other field matches.],

[`src`],
[`VertexId` (`u64`)],
[The starting vertex from which reachability is computed.],

[`min_hops`],
[`u8`],
[The minimum accepted path length. In `*1..2`, this is `1`.],

[`max_hops`],
[`u8`],
[The maximum accepted path length. In `*1..2`, this is `2`. Using `u8`
also gives the representation a maximum expressible hop count of `255`.],

[`read_epoch`],
[`GraphEpoch`],
[The graph snapshot from which the answer was calculated. A write advances
the graph epoch, so later reads generate different keys and do not reuse
answers from an older snapshot.],

[`window`],
[`Option<ReachabilityCacheWindow>`],
[`None` means the key represents the complete result. `Some(window)` means
it represents one ordered and paginated slice. The complete answer and a
page of that answer are stored separately because they are different
returned values.],
table.hline(),
)

Unlike `MatrixCacheKey`, this key is fine-grained. Two traversals share a cache
entry only when every input that affects their returned answer is identical.

=== `ReachabilityCacheWindow`: pagination and ordering

`ReachabilityCacheWindow` captures the result-window inputs that can change
which vertices are returned:

```rust
pub struct ReachabilityCacheWindow {
    pub skip: u64,
    pub limit: Option<usize>,
    pub ascending: bool,
}
```

#table(
columns: (auto, auto, 1fr),
align: (left, left, left),
stroke: none,
inset: (x: 7pt, y: 4pt),
table.hline(),
table.header([*Field*], [*Type*], [*Purpose*]),
table.hline(),
[`skip`],
[`u64`],
[The number of ordered results omitted before returning the page. This is
the cache-key equivalent of `SKIP 10`.],

[`limit`],
[`Option<usize>`],
[The maximum number of vertices returned by the page. `None` represents an
unbounded result and is distinct from every finite limit.],

[`ascending`],
[`bool`],
[The ordering direction used before applying `skip` and `limit`. Ascending
and descending windows can contain different vertices and must therefore
use different keys.],
table.hline(),
)

The type contains only small scalar values, so it can derive `Copy`. It is
embedded in `ReachabilityCacheKey` only when the cached value represents a
windowed answer.

#note[
Pagination is deterministic only when the underlying result has a stable
ordering. The `ascending` flag records the direction, but the implementation
must also define what value is being ordered, such as vertex id. If future
queries can order by different fields, the ordering expression must also
become part of the cache key.
]

=== `ReachabilityCacheValue`: the cached answer

`ReachabilityCacheValue` is stored underneath a
`ReachabilityCacheKey`:

```rust
pub struct ReachabilityCacheValue {
    pub vertices: Option<Arc<Vec<VertexId>>>,
    pub count: u64,
    pub edge_visits: u64,
}
```

#table(
columns: (auto, auto, 1fr),
align: (left, left, left),
stroke: none,
inset: (x: 7pt, y: 4pt),
table.hline(),
table.header([*Field*], [*Type*], [*Purpose*]),
table.hline(),
[`vertices`],
[`Option<Arc<Vec<VertexId>>>`],
[Contains the reachable vertices when the traversal materialized them.
`Arc` allows concurrent readers to share the same vector without copying
it. Cloning the value increments the reference count rather than cloning
every vertex. The vector can also remain alive after cache eviction while
another reader still holds an `Arc`. `None` represents an entry that stores
only a count.],

[`count`],
[`u64`],
[Stores the number of reachable vertices. It is retained even when
`vertices` is present so count-oriented consumers can read the cardinality
directly without scanning or cloning the list.],

[`edge_visits`],
[`u64`],
[Records how many graph edges the original traversal examined. This is
recomputation-cost metadata rather than part of the logical answer. It can
feed metrics and an eviction policy that prefers keeping expensive
traversal results over cheap ones.],
table.hline(),
)

`vertices` and `count` deliberately overlap. The vector is needed when the
caller requests identities, while the scalar count allows count-only queries
to avoid touching the larger allocation.

The `count_only` constructor can therefore produce an entry with:

```rust
ReachabilityCacheValue {
    vertices: None,
    count,
    edge_visits,
}
```

A fully materialized result stores both the shared vector and its count:

```rust
ReachabilityCacheValue {
    vertices: Some(Arc::new(vertices)),
    count,
    edge_visits,
}
```

#note[
A count-only entry cannot satisfy a later request for the vertex list. A
materialized entry can satisfy both list and count requests, provided both
request forms map to compatible cache lookup rules.
]

=== Epochs as logical invalidation

Both `MatrixCacheKey` and `ReachabilityCacheKey` include a graph epoch:

```text
write changes graph
        |
        v
epoch advances
        |
        v
new reads construct different keys
        |
        v
old entries stop matching
```

The write path does not need to synchronously locate and delete every cached
matrix or traversal answer. Advancing the epoch makes entries from the old
snapshot unreachable through normal lookups.

This is logical invalidation rather than immediate physical deletion. Old
entries can remain allocated until ordinary eviction or cleanup removes them.

#warning-box[
Epoch-based keys prevent stale cache hits only when every graph mutation
that can affect the answer advances the relevant epoch, and every cache
lookup uses the correct read epoch. Missing either rule can make an old
answer appear valid.
]

=== The one-line mental model

#boxeq[
A key contains every input that can change the cached result. A value
contains the result itself plus the metadata needed to share it, measure
its saved work, and decide when it should be evicted.
]

For the three cache families:

* `MatrixCacheKey` is *place + edge type + graph version*.
* `ReachabilityCacheKey` is *place + edge type + source + hop bounds + graph
  version + result window*.
* `ParsedRowQueryCacheKey` is only the exact query text because parsing is
  currently independent of graph state.

The epoch fields provide self-invalidation: after the graph changes, readers
move to new keys rather than reusing answers computed from an older snapshot.

```
```
