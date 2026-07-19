#import "../template.typ": term, why, srcblock, figcap, accent, muted
#import "@preview/fletcher:0.5.7" as fletcher: diagram, node, edge

= The Read Path

This chapter follows one read from the moment a driver sends a query to the moment rows come
back. It is the longest chapter because reading is where most of the engine's code lives:
`src/shard/query.rs` alone is about nine thousand lines. We take it in stages and quote the
real functions at each step.

Keep one idea in mind the whole way through, because it is the thread that ties the chapter
together: a read is bound to a single SlateDB snapshot when it reaches the shard, the
topology epoch recorded inside that snapshot is read back out, and that one epoch is carried
into every storage access, every visibility check, and every cache key. That is how the
engine gives a reader a stable snapshot while writers keep advancing the graph. Note the two
distinct cursors this involves: a `StorageSequence` (SlateDB's snapshot sequence, the real
MVCC mechanism) and a `TopologySequence` (the topology-change epoch that feeds matrix builds
and stamps records); the chapter says "epoch" colloquially but they are not the same thing.

== The journey of a read

Before the detail, here is the whole path. Each box is a stage, and each has its own section
below.

#figure(
  diagram(
    node-stroke: 0.6pt,
    node-fill: rgb("#eef4ff"),
    spacing: (0pt, 0.66cm),
    node((0, 0), [driver sends Bolt `RUN`/`PULL` or HTTPS `POST` (Section 2.2)], width: 12.5cm),
    edge((0, 0), (0, 1), "->"),
    node((0, 1), [`ClientQueryService`: authorize, classify read/write, reject client epochs, bookmark (2.3)], width: 12.5cm),
    edge((0, 1), (0, 2), "->"),
    node((0, 2), [parse Cypher into `ParsedRowQuery` (2.4)], width: 12.5cm),
    edge((0, 2), (0, 3), "->"),
    node((0, 3), [optimize: pick an access strategy from statistics (2.5)], width: 12.5cm),
    edge((0, 3), (0, 4), "->"),
    node((0, 4), [execute in `shard/query.rs`: patterns to scans (2.6, 2.7)], width: 12.5cm),
    edge((0, 4), (0, 5), "->"),
    node((0, 5), [MVCC merge at the read epoch, or traversal kernel (2.8, 2.11)], fill: rgb("#fff8e6"), width: 12.5cm),
    edge((0, 5), (0, 6), "->"),
    node((0, 6), [apply `WHERE` / `RETURN`, build `QueryResultSet`, page back (2.9, 2.10)], fill: rgb("#e9fce9"), width: 12.5cm),
  ),
  caption: none,
)
#figcap[The read path end to end. The yellow stage is where the epoch snapshot is applied; the green stage is what the client receives.]

The overall call chain in code is:

Bolt `RUN`/`PULL` or HTTP `POST` #sym.arrow.r `ClientQueryService::execute_page` / `execute_rows`
#sym.arrow.r `QueryCellClient::execute_cypher_rows{,_page}` #sym.arrow.r
`GraphShard::execute_opencypher_rows{,_page}` #sym.arrow.r parse #sym.arrow.r optimize
#sym.arrow.r execute #sym.arrow.r MVCC merge or traversal #sym.arrow.r assemble `QueryResultSet`.

== Entry: Bolt and HTTPS

A read enters through one of the two front doors from Chapter 1.

The Bolt server is a state machine, `run_bolt_protocol` (`src/client/bolt.rs:645`), that
reacts to `(state, message)` pairs. A `RUN` message (a query) is accepted only in the
`Ready` state. It prepares the request, replies with the result column names, and moves to
`Streaming`, where `PULL` messages pull rows back. The step that turns the wire message into
an engine request is `prepare_bolt_run`, and that is where the database name becomes a
concrete target:

#srcblock("src/client/bolt.rs:1082-1089")[```rust
let database = selected_bolt_database(session, context, &extra)?;
let target = context
    .database_resolver
    .resolve_database(Some(&database))
    .map_err(graph_error_to_bolt)?;
// ...
let mut request = bolt_query_request(target.clone(), query_id.clone(), query, parameters, &extra)?;
```]

This is the resolution chain from Chapter 1, Section 1.5, happening for real: the Bolt `db`
field goes through `ClientDatabaseResolver` and comes out as a `ClientQueryTarget` carrying
the scope and cell id.

#term("Auto-commit query")[
  A query that is its own transaction: it runs and commits by itself, with no surrounding
  `BEGIN` / `COMMIT`. TurboLay only supports auto-commit queries. An explicit transaction is
  refused.
]

The refusal is explicit. In the `Ready` state a `Begin`, `Commit`, or `Rollback` message
fails the connection immediately:

#srcblock("src/client/bolt.rs:999-1005")[```rust
(BoltState::Ready,
 ClientMessage::Begin { .. } | ClientMessage::Commit | ClientMessage::Rollback) => {
    send_bolt_failure(&mut writer, &explicit_transactions_unsupported()).await?;
    state = BoltState::Failed;
}
```]

The HTTPS door is simpler. The router binds `POST /v1/graphs/{graph_id}/query` to
`execute_query` (`src/client/http.rs:174-197`), which authenticates, builds the target,
converts the JSON parameters, and calls the same service method the Bolt path uses. It reads
an optional `bookmark` from the body, but a `read_epoch` in the body is *rejected*, not
honored (`src/client/http.rs:464`): "read_epoch is not a storage snapshot selector; use
bookmark for causal reads". A client cannot ask for a historical topology epoch as a query
snapshot; it carries a bookmark for causal reads instead. Both doors converge on
`ClientQueryService`.

== The service layer

#term("ClientQueryService")[
  The shared layer behind both front doors. It does everything that is common to a query
  regardless of protocol: validate the request, decide whether it reads or writes, check the
  caller is allowed, reject any client-supplied epoch and manage bookmarks, enforce quotas
  and timeouts, register a cancellation handle, and hand the query to the shard that owns the
  cell. It does *not* pin the read epoch itself; that happens down in the shard.
]

The full-result entry point is `execute_rows`; the paged entry point is `execute_page`
(used by both Bolt `PULL` and HTTPS streaming). `execute_rows` runs a fixed sequence
(`src/client/service.rs:705-747`): validate the request, normalize the runtime limit,
authorize, register a cancellation token, then run under a timeout and concurrency permits.

Two of those steps decide the whole character of the read.

*Read or write.* The service parses the query and inspects it to classify access, then
authorizes the caller for that action on that scope:

#srcblock("src/client/service.rs:1062-1073")[```rust
let action = match classify_opencypher_query_access(&request.query)? {
    OpenCypherQueryAccess::Read => QueryTransportAction::Read,
    OpenCypherQueryAccess::Write => QueryTransportAction::Write,
};
self.authorize_scope(session, &request.target.scope, action)?;
```]

`classify_opencypher_query_access` (`src/query/opencypher.rs:306`) parses the query and looks
for write clauses; if it finds any it is a write, otherwise a read. This matters here because
the epoch handling below differs for reads.

*No client epochs; leave the snapshot to the shard.* This is where the service's role in
snapshot reads changed. A client may *not* hand in a `read_epoch`: historical topology epochs
are not query snapshots. Both `execute_rows` (`src/client/service.rs:753`) and the unpaged
start of `execute_page` (`:846`) reject a supplied epoch outright:

#srcblock("src/client/service.rs:753-758")[```rust
if request.read_epoch.is_some() {
    return Err(GraphError::UnsupportedQuery {
        dialect: "ClientProtocol",
        feature: "historical graph epochs are not client query snapshots; use a bookmark for causal reads"
            .to_string(),
    });
}
```]

So for a read the service does the opposite of pinning: it clears the epoch and lets the
shard choose the snapshot. It sets `context.read_epoch = None` (`:803`), runs the query, then
reads the epoch the shard actually used back out of the result and turns it into a bookmark:

#srcblock("src/client/service.rs:803-818 (abridged)")[```rust
if action == QueryTransportAction::Read {
    context.read_epoch = None;
    context.max_result_bytes = Some(self.inner.config.max_cursor_buffer_bytes);
}
let result = /* execute_cypher_rows / execute_batch */;
let read_epoch = result_read_epoch(&result, action)?;   // the epoch the shard used
let bookmark = self.bookmark_after(&request, action, read_epoch).await?;
```]

`result_read_epoch` (`:1565`) pulls `result.read_epoch` off the returned `QueryResultSet` (and
errors if a read failed to report one), and `bookmark_after` (`:1343`) mints a fresh bookmark
at that epoch. On the paged path the same happens once, up front, in `prepare_page_request`,
so that a cursor stays on the same snapshot across all its pages even if writes land in
between.

#term("Bookmark")[
  A small token a client can carry between queries that says "I have already seen the graph
  up to epoch N". When the client sends it back, the service refuses to run the next read
  until the cell's current epoch is at least N. This gives read-your-writes consistency: a
  client that just wrote will not accidentally read an older replica that has not caught up.
]

`validate_bookmark` and `ensure_bookmark` (`src/client/service.rs:1330`, `:695`) implement
that check, rejecting the read if `current_epoch < bookmark.epoch`. After a successful read
the service hands back a fresh bookmark at the epoch it used.

The request the shard finally receives is wrapped in a `QueryContext`, which carries a *None*
epoch for a client read and everything else execution needs:

#srcblock("src/query/algebra.rs:166-180")[```rust
pub struct QueryContext {
    pub scope: GraphScope,
    pub cell_id: String,
    pub idempotency_key: String,
    pub read_epoch: Option<TopologySequence>,
    pub result_window: QueryWindow,
    pub parameters: BTreeMap<String, VertexPropertyValue>,
    pub max_runtime_ms: Option<u64>,
    pub max_result_bytes: Option<u64>,
    // cancellation_token, validated_read ...
}
```]

The `validated_read` field is where the real snapshot ends up: it holds the epoch and storage
sequence the shard bound the read to, set by `with_validated_storage_read_epoch`
(`src/query/algebra.rs:243`) and read back through `validated_read_epoch` (`:258`).

*The pin the service skipped happens here.* When a read arrives with `read_epoch == None`, the
shard opens a SlateDB snapshot, reads the cell's current topology epoch *from inside that
snapshot*, binds the two together, and runs the whole query scoped to that snapshot
(`src/shard/query.rs:288-315`):

#srcblock("src/shard/query.rs:288-306 (abridged)")[```rust
if context.read_epoch.is_none() {
    if let Ok(Some(snapshot)) = self.db.snapshot().await {
        let key = keys::last_epoch(&context.cell_id);
        let read_epoch = match snapshot
            .get_with_options(key.as_bytes(), &remote_read_options()).await {
            Ok(Some(value)) => decode_u64(&key, &value)?,
            Ok(None) => 0,
            Err(err) => return Err(err.into()),
        };
        let context = context.with_validated_storage_read_epoch(read_epoch, snapshot.seq());
        return GraphStore::scope_snapshot(snapshot, Box::pin(/* run the query */)).await;
    }
}
```]

Two things are bound together here, and they are different types. `snapshot.seq()` is a
`StorageSequence` --- SlateDB's own snapshot number, the actual read-consistency mechanism.
`read_epoch`, read from `keys::last_epoch(cell_id)`, is a `TopologySequence` --- the
topology-change cursor that matrix builds and record stamps use. Reading `last_epoch` from
*inside* the snapshot is what makes the two agree: the epoch the query filters by is exactly
the topology state that this storage snapshot froze. `GraphStore::scope_snapshot`
(`src/core/state.rs:147`) then installs the snapshot for the duration of the query so every
storage access underneath sees the same frozen bytes; the borrowed and owned forms are
`GraphSnapshot` / `OwnedGraphSnapshot` (`src/core/snapshot.rs:9,16`). This is where the "one
epoch everywhere" thread becomes concrete.

== Parsing Cypher into the engine's plan

The shard turns the query string into the engine's own plan type before doing anything with
storage. The entry function is `parse_opencypher_row_query_with_parameters`:

#srcblock("src/query/opencypher.rs:267-272")[```rust
pub fn parse_opencypher_row_query_with_parameters(
    query: &str,
    parameters: &BTreeMap<String, VertexPropertyValue>,
) -> Result<ParsedRowQuery> {
    with_parsed_cypher(query, |parsed| parsed.lower_row_query(parameters))
}
```]

`with_parsed_cypher` (`src/query/opencypher.rs:315`) runs a C parser (`libcypher-parser`,
reached through FFI) to get a raw syntax tree --- reusing one from a thread-local cache of
`ParsedCypher` values when it can --- and hands that tree to the closure. The closure,
`lower_row_query`, is TurboLay's own work: it walks the tree and lowers it into
`ParsedRowQuery`, rejecting anything the engine does not implement. The sibling entry points
(`lower_mutation_query`, `query_access`) go through the same `with_parsed_cypher` wrapper, so
they all share one parse.

#term("Lowering")[
  Translating a general syntax tree into a smaller, stricter internal form that the engine
  knows how to run. TurboLay's lowering is where unsupported Cypher is turned away, so the
  execution code below it can assume a narrow, well-formed plan.
]

`ParsedRowQuery` is that internal form. It is a flat description of one row query: the
patterns to match, an optional filter, the projections to return, ordering, a result window,
and the output column names:

#srcblock("src/query/opencypher.rs:14-26")[```rust
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

The supporting types are small closed enums, all in `opencypher.rs`. A `RowPattern` is either
a `Node` or an `Edge` (`:69`). A `RowEdgePattern` carries an optional `hop_range: Option<(u8, u8)>`
(`:82`), which is how a bounded variable-length path such as `-[:RELATES*1..3]-` is
represented; the traversal section returns to it. A `RowProjection` is a `NodeId`, a
`Property`, `CountAll`, or an `Aggregate` (`:100`), and `RowAggregateFunction` is `Count`,
`Sum`, `Avg`, or `Collect` (`:116`). A `RowPredicate` is a tree of `Compare`, `And`, `Or`,
and `Not` (`:183`).

*The parse cache.* Parsing is pure work that depends only on the query text, so TurboLay
caches the parsed form, but only when the query has no parameters (a parameterized query is
cheap to re-lower and the parameter values must be folded in):

#srcblock("src/shard/query.rs:258-278 (abridged)")[```rust
if !parameters.is_empty() {
    return parse_opencypher_row_query_with_parameters(query, parameters);
}
let key = ParsedRowQueryCacheKey::new(query);
if let Some(parsed) = self.parsed_row_query_cache.lock().await.get(&key) {
    self.cache_metrics.record_hit(GraphCacheKind::ParsedRowQuery);
    return Ok(parsed);
}
let parsed = parse_opencypher_row_query_with_parameters(query, parameters)?;
self.parsed_row_query_cache.lock().await.insert(key, parsed.clone(), /* ... */);
```]

This cache key holds only the query string. It is the one read cache that does not carry an
epoch, because a parse result does not depend on graph contents.

== The optimizer

Before scanning, the engine chooses how to reach each pattern. For row queries the choice is
made lazily as patterns are matched, not as a separate up-front pass. The decision function
is `best_row_edge_access` (`src/shard/query_optimizer.rs:459`), which returns a
`RowQueryAccess` describing the cheapest way to satisfy one edge pattern.

#term("Access path")[
  The concrete method chosen to satisfy a pattern. For an edge, the choices include expanding
  from a known source using the outgoing adjacency index, expanding backward using the
  incoming index, checking a single edge's existence, using an edge-property index, walking a
  variable-length range, or, as a last resort, scanning every edge of that type.
]

The choice is cost-based. Each candidate is given an estimated cardinality (how many rows it
will produce), and the engine picks the smallest, breaking ties by a fixed priority. The
estimates come from persisted statistics:

#term("Query statistics (qstats)")[
  Counts the engine keeps about the graph so the optimizer can guess selectivity: how many
  edges of a type exist, how many distinct values a property has, how common the most common
  value is. They are stored in the cell (as `QueryStatsRecord`) and refreshed in the
  background. If a statistic is stale, its cost estimate is inflated so the optimizer treats
  it with suspicion.
]

`query_stats_estimate` (`src/shard/query_optimizer.rs:579`) reads a `QueryStatsRecord` and
`stats_record_cost_estimate` multiplies the estimate by four when the record is stale. A
`QueryStatsRecord` (`src/query/algebra.rs:366`) holds `count`, `read_epoch`,
`refreshed_at_ms`, `distinct_values`, `total_values`, and `most_common_count`, and offers an
`equality_estimate()` for the selectivity of a property equality.

#why[
  Statistics are stored per cell and stamped with the epoch they were computed at, rather than
  recomputed per query, because the graph lives on a remote object store where counting is
  expensive. Inflating stale estimates is a safety valve: a wrong-but-cheap-looking plan is
  worse than a plan the optimizer already distrusts, so staleness pushes the optimizer toward
  more conservative access paths.
]

== Execution: from a pattern to a scan

Execution begins at `execute_opencypher_rows` (`src/shard/query.rs:105`), which ties parse
and run together:

#srcblock("src/shard/query.rs:105-117")[```rust
pub async fn execute_opencypher_rows(
    &self, context: QueryContext, query: &str,
) -> Result<QueryResultSet> {
    let parsed = self
        .parsed_opencypher_row_query(&context.cell_id, query, &context.parameters)
        .await?;
    let context = merge_opencypher_window(context, parsed.window)?;
    self.execute_parsed_opencypher_rows(context, parsed).await
}
```]

From there `execute_parsed_opencypher_rows_inner` (`:317`) builds a `QueryBudget` (the
runtime-limit and cancellation guard that every deep loop checks), resolves the read epoch
one more time with `query_read_epoch`, and routes to either the union path or
`execute_single_opencypher_rows` (`:413`).

`execute_single_opencypher_rows` tries a few fast paths first (a pure traversal query, a
relationship-count query, a relationship-rows query) before falling back to generic pattern
matching. We follow the generic path, because the fast paths are specializations of it.

Generic matching is `match_row_patterns` (`:2499`), which for each edge pattern with a known
source scans that source's neighbors. This is the exact point where the graph model becomes
storage access:

#srcblock("src/shard/query.rs:3095-3105")[```rust
for src in sources {
    budget.check("cypher_edge_sources")?;
    let neighbors = self
        .out_neighbors_at_for_query(cell_id, &edge.edge_type, src, read_epoch, budget)
        .await?;
    scanned_edges = scanned_edges.saturating_add(neighbors.len() as u64);
    self.ensure_query_scan_edges("cypher_edge_neighbor_scan", scanned_edges)?;
    for dst in neighbors { self.push_matching_edge_row(edge, src, dst, &mut state).await?; }
}
```]

`out_neighbors_at_for_query` (`:5902`) is the representative neighbor scan. Notice that it
does not scan raw keys itself; it asks the MVCC merge for all edges of the type at the read
epoch, then keeps the ones leaving `src`:

#srcblock("src/shard/query.rs:5902-5924")[```rust
async fn out_neighbors_at_for_query(
    &self, cell_id: &str, edge_type: &str, src: VertexId,
    read_epoch: TopologySequence, budget: &QueryBudget,
) -> Result<Vec<VertexId>> {
    let mut neighbors = Vec::new();
    for edge in self.edges_at_with_budget(cell_id, edge_type, read_epoch, Some(budget)).await? {
        budget.check("query_out_neighbors_scan")?;
        if edge.src == src { neighbors.push(edge.dst); }
    }
    neighbors.sort_unstable();
    Ok(neighbors)
}
```]

The reverse direction uses `in_neighbors_at_for_query`, and a pattern with no bound endpoint
falls back to a full edge scan. All three build their keys from the functions in `keys.rs`
(`out_prefix`, `in_prefix`, `out_edge`, and so on from Chapter 0, Section 0.7) and read them
through two thin wrappers in `src/shard/maintenance.rs`: `read_remote` for a point `get`, and
`scan_remote_prefix` for a prefix `scan`. Those wrappers set the storage read options:

#srcblock("src/codec.rs:204-215")[```rust
pub(crate) fn remote_read_options() -> ReadOptions {
    ReadOptions { durability_filter: DurabilityLevel::Remote, ..Default::default() }
}

pub(crate) fn remote_scan_options() -> ScanOptions {
    ScanOptions::default()
        .with_durability_filter(DurabilityLevel::Remote)
        .with_cache_blocks(false)
}
```]

#term("Durability filter")[
  A read option that tells SlateDB which data is allowed to satisfy the read. `Remote` means
  only data that has reached the object store durably counts; data still sitting in a local
  memtable is ignored. TurboLay reads at `Remote` so that every reader, including a read-only
  `DbReader` on another machine, sees the same durable snapshot.
]

== MVCC on read: the two-layer merge

`out_neighbors_at_for_query` leaned on `edges_at_with_budget`. That function is where the
epoch snapshot is actually constructed, and it is worth reading in full because it is the
core of the read path. It builds the set of live edges of a type as of the read epoch by
merging two layers: a compacted base and a short replay of deltas.

#srcblock("src/shard/query.rs:6671-6741 (abridged)")[```rust
async fn edges_at_with_budget(
    &self, cell_id: &str, edge_type: &str, read_epoch: TopologySequence, budget: Option<&QueryBudget>,
) -> Result<Vec<EdgeRecord>> {
    let mut edges = std::collections::BTreeMap::new();
    // Layer 1: canonical base = newest matrix artifact with base_epoch <= read_epoch.
    let base_epoch = if let Some(artifact) =
        self.latest_matrix_artifact(cell_id, edge_type, read_epoch).await?
    {
        let adjacency = self.cached_matrix_adjacency(cell_id, edge_type, artifact.base_epoch).await?;
        for (src, dsts) in adjacency.iter() {
            for dst in dsts {
                edges.insert((*src, *dst), EdgeRecord { /* ... */ epoch: artifact.base_epoch });
            }
        }
        artifact.base_epoch
    } else { 0 };
    // Layer 2: deltas between the base and the read epoch, applied in order.
    for delta in self
        .deltas_between_with_budget(cell_id, edge_type, base_epoch, read_epoch, budget).await?
    {
        let key = (delta.edge.src, delta.edge.dst);
        match delta.kind {
            DeltaKind::Plus => { edges.insert(key, delta.edge); }
            DeltaKind::Minus => { edges.remove(&key); }
        }
    }
    Ok(edges.into_values().collect())
}
```]

The two helper calls here now live in dedicated files: `latest_matrix_artifact` is in
`src/engine/artifact_build.rs`, and `cached_matrix_adjacency` is in the new
`src/engine/matrix_cache.rs:4`, which read-through hydrates a matrix artifact and caches the
adjacency keyed by its `base_epoch`. That cache subsystem gets its own treatment in the
caching chapter; here it is enough to know the base layer comes from it.

The two layers, newest-understanding-last:

#term("Matrix artifact")[
  A precomputed, compacted snapshot of all edges of one type in a cell, as of some base
  epoch. It is the canonical base layer of a read: instead of replaying every write from the
  beginning of time, the engine starts from the newest artifact whose base epoch is at or
  before the read epoch. Artifacts are built in the background (write and delete chapters).
]

#term("Delta record")[
  A single edge change since the base: a `Plus` (edge added) or a `Minus` (edge removed),
  stamped with the epoch it happened at. Reads replay the deltas from the artifact's base
  epoch up to the read epoch, applying `Plus` as an insert and `Minus` as a remove, so the
  merged set is exactly the graph as of the read epoch.
]

`edges_at_with_budget` stops there: it never scans segments. Segments --- compacted runs of
adjacency materialized past the delta stage but not yet folded into an artifact --- belong to
the neighbor-scan and traversal paths instead (`out_segment_edge_record_at`,
`scan_out_segments_for_src_at`), not to this generic edge merge. Section 2.11 returns to them
when it reaches the traversal adjacency.

The epoch filtering is uniform across both layers and is the mechanism behind snapshot
isolation:

- The base artifact is chosen with `base_epoch <= read_epoch`.
- `deltas_between_with_budget` returns deltas in epoch order and stops as soon as it sees a
  record with `epoch > read_epoch`, and skips records at or before the base.

#figure(
  diagram(
    node-stroke: 0.6pt,
    spacing: (0.5cm, 0.75cm),
    node((0, 0), [Matrix artifact\ (`base_epoch <= read_epoch`)], fill: rgb("#e9fce9"), width: 4.6cm),
    node((1.6, 0), [+ Deltas\ (`base < epoch <= read_epoch`)], fill: rgb("#fff8e6"), width: 4.6cm),
    edge((0, 0), (1.6, 0), "->"),
    node((0.8, 1), [merged live edge set as of `read_epoch`], fill: rgb("#f6f8fa"), width: 7cm),
    edge((0, 0), (0.8, 1), "->"),
    edge((1.6, 0), (0.8, 1), "->"),
  ),
  caption: none,
)
#figcap[The MVCC merge: a compacted base artifact plus a short delta replay. Everything newer than the read epoch is filtered out at both layers, so the reader sees one consistent snapshot regardless of concurrent writes.]

#why[
  Layering a compacted base under a short replay of recent deltas is the classic
  log-structured trade-off applied to a graph. Writes stay cheap because they only append a
  delta. Reads stay cheap because they start from a compacted artifact and only replay a small
  tail. The epoch cutoff on each layer is what lets many readers at different epochs share the
  same underlying files.
]

An important optimization threads through the code: a read whose epoch equals the cell's
current epoch (`latest_snapshot = read_epoch == current_epoch`) can take cheaper "read the
tip" paths, while a historical read uses the epoch-filtered variants. You will see the
`latest_snapshot` flag computed in many functions for exactly this reason.

== WHERE and RETURN

Once patterns produce candidate rows (each row a set of bound vertices with hydrated
properties), the filter and the projection run over them.

The filter is `row_predicate_matches` (`src/shard/query.rs:8194`), a direct recursive walk of
the `RowPredicate` tree:

#srcblock("src/shard/query.rs:8194-8209 (shape)")[```rust
RowPredicate::Compare { left, op, right } =>
    compare_row_values(eval_row_expression(row, left)?, *op, eval_row_expression(row, right)?)?,
RowPredicate::And(l, r) => matches(l) && matches(r),
RowPredicate::Or(l, r)  => matches(l) || matches(r),
RowPredicate::Not(inner) => !matches(inner),
```]

`eval_row_expression` (`:8212`) resolves each side to a `NodeId`, a `Property`, or a
`Literal`. A property that is not present resolves to a "missing" value, which makes any
comparison against it false.

The projection has two modes. Without aggregates, `project_binding_row` (`:8329`) maps each
`RowProjection` straight to a `QueryValue` (a node id becomes `QueryValue::VertexId`, a
property becomes `QueryValue::Property`, absent becomes `QueryValue::Null`). With aggregates,
`aggregate_projected_rows` (`:8517`) groups rows by the non-aggregate projections (the group
key is the vector of those `QueryValue`s) and folds each group through an accumulator:
`CountAll`, `CountExpression`, `Sum`, `Avg`, or `Collect`.

== Assembling and paging the result

The terminal step is `finish_projected_rows` (`src/shard/query.rs:4471`): it applies
`DISTINCT` deduplication, then `ORDER BY`, then the `SKIP` / `LIMIT` window, enforcing the
configured maximum result size along the way, and returns a `QueryResultSet`.

The result types were introduced in Chapter 1; here they are the actual output:

#srcblock("src/query/algebra.rs:423-461")[```rust
pub enum QueryValue {
    Null, VertexId(VertexId), Count(u64), Bool(bool),
    Float(QueryFloat), Property(VertexPropertyValue), List(Vec<QueryValue>),
}
pub struct QueryRow { pub values: Vec<QueryValue> }
pub struct QueryResultSet { pub columns: Vec<QueryColumn>, pub rows: Vec<QueryRow> }
```]

For the paged path the shard returns a `QueryResultPage` with a `next_cursor:
Option<QueryCursorToken>` (a simple row offset). The Bolt server drives paging with `PULL`,
turning each `QueryValue` into the matching Bolt wire value on the way out. Because the
snapshot epoch is fixed once when the cursor is created (in `prepare_page_request`), every
page of a cursor reads the same snapshot.

== Traversal: multi-hop reads

A bounded variable-length pattern such as `MATCH (a)-[:RELATES*1..3]->(b)` cannot be answered
by one adjacency scan. These reads take a separate path built around a sparse-matrix kernel.

A row query is eligible for the traversal path only when it is a single anchored edge pattern
with a hop range, no filter, no union, and a simple projection. `graph_kernel_row_query_request`
(`src/shard/query.rs:7013`) recognizes that shape and hands off to
`try_execute_graph_kernel_row_query` (`:504`), which calls one of the reachability functions.

#term("Reachability")[
  The set of vertices you can reach from a starting vertex by following a given edge type
  between a minimum and maximum number of hops. It is computed by breadth-first expansion:
  start with the source, take all neighbors, take their neighbors, and so on, up to the hop
  limit, collecting everything seen.
]

`reachable_vertices_in_hop_range_at` (`src/shard/query.rs:4887`) is the representative
function. It validates the hop range, tries a compiled fast path, and otherwise builds a
working adjacency and runs the kernel. There is no reachability cache in this path --- it
either hits a compiled artifact or rebuilds the adjacency for the read:

#srcblock("src/shard/query.rs:4903-4927 (abridged)")[```rust
if let Some(result) = self.reachable_vertices_with_compiled_graph_kernel(/* ... */).await? {
    return Ok(result);
}
let (adjacency, delta_records_applied) =
    self.reachable_query_adjacency_at(cell_id, edge_type, read_epoch, budget).await?;
let traversal = crate::sparse_kernel::expand_range(
    &adjacency, &[src], min_hops, max_hops, SparseKernelBackend::RustSparse,
)?;
```]

`reachable_query_adjacency_at` (`:5336`) builds the working adjacency from the same base and
delta layers as `edges_at_with_budget` --- the newest matrix artifact with `base_epoch <=
read_epoch`, then `apply_delta_overlay` up to the read epoch --- only shaped as an adjacency
map instead of an edge list. Segments enter on the neighbor-scan side of the same family
(`out_segment_edge_record_at`, `scan_out_segments_for_src_at`), overlaid the same way.

#term("Sparse kernel")[
  The component that does the breadth-first expansion. "Sparse" because a graph's adjacency,
  written as a matrix, is almost all zeros, so it is stored and traversed as lists of
  neighbors rather than a dense grid. TurboLay has two backends: a pure-Rust one and one that
  calls the SuiteSparse GraphBLAS C library when the `graphblas` feature is on.
]

The backend is chosen by an enum:

#srcblock("src/sparse_kernel.rs:14-18")[```rust
pub enum SparseKernelBackend {
    RustSparse,
    SuiteSparseGraphBlas,
}
```]

The Rust backend is a plain frontier BFS over a `BTreeMap` of neighbor sets, counting the
edges it visits and excluding the start vertices from the result:

#srcblock("src/sparse_kernel.rs:221-250 (abridged)")[```rust
fn expand_rust(adjacency: &Adjacency, starts: &[VertexId], hops: u8) -> SparseTraversal {
    let start_set: BTreeSet<_> = starts.iter().copied().collect();
    let mut frontier = start_set.clone();
    let mut seen = start_set.clone();
    for _ in 0..hops {
        let mut next = BTreeSet::new();
        for src in &frontier {
            if let Some(neighbors) = adjacency.get(src) {
                for dst in neighbors { if seen.insert(*dst) { next.insert(*dst); } }
            }
        }
        frontier = next;
        if frontier.is_empty() { break; }
    }
    // vertices = seen minus the start set
}
```]

*Precomputed artifacts versus live scans.* Traversal chooses between three levels of
precomputation, from fastest to most general (`src/engine/traversal.rs`):

+ *Compiled GraphBLAS matrix*, used only when the artifact's base epoch equals the read epoch
  exactly (`reachable_vertices_with_compiled_graph_kernel`, `src/shard/query.rs:4999`). When
  the snapshot lines up with a compiled artifact, expansion runs as sparse-matrix
  multiplication with no delta replay.
+ *Matrix artifact plus delta overlay*, the general path in `matrix_reachable_with_kernel`
  (`src/engine/traversal.rs:23`): hydrate the artifact's adjacency, overlay the deltas up to
  the read epoch, then expand.
+ *Direct snapshot expansion*, `direct_snapshot_reachable` (`src/engine/traversal.rs:138`),
  which live-scans edges from the snapshot when there is no usable artifact.

The decision looks like this:

#figure(
  diagram(
    node-stroke: 0.55pt,
    spacing: (0.6cm, 0.72cm),
    node((0, 1), [reachability request\ at `read_epoch`], fill: rgb("#eef4ff"), width: 4.4cm),
    edge((0, 1), (1.5, 1), "->", []),
    node((1.5, 1), [artifact `base_epoch`\ `== read_epoch`?], fill: rgb("#fff8e6"), width: 4.4cm),
    edge((1.5, 1), (1.5, 0), "->", [yes]),
    node((1.5, 0), [compiled GraphBLAS\ matrix multiply], fill: rgb("#e9fce9"), width: 4.4cm),
    edge((1.5, 1), (3, 1), "->", [no]),
    node((3, 1), [hydrate artifact +\ overlay deltas, BFS\ (or direct scan)], fill: rgb("#e9fce9"), width: 4.6cm),
  ),
  caption: none,
)
#figcap[How a multi-hop read picks its engine. The exact-epoch compiled path is fastest; the artifact-plus-overlay path is the general fallback; with no usable artifact the engine expands directly from the snapshot.]

== Read-side caching, in brief

A read consults several caches on the shard, and the caching chapter covers their internals.
The single fact that matters for correctness here is that every cache key which depends on
graph contents embeds an epoch. The matrix caches --- hydrated in `src/engine/matrix_cache.rs`
--- are keyed by the artifact's base epoch:

#srcblock("src/lib.rs:153-159")[```rust
pub(crate) struct MatrixCacheKey {
    pub(crate) cell_id: String,
    pub(crate) edge_type: String,
    pub(crate) base_epoch: TopologySequence,
}
```]

The epoch's role differs by cache. Per-read caches (the parsed-query cache aside, the
relationship-rows and property-rows caches) embed the `read_epoch` itself, so a cached entry
from one epoch can never be served to a read at a different epoch --- a write advances the
epoch and the next read simply misses. The matrix caches instead key on a `base_epoch` that
deliberately lags the read epoch, with the delta log overlaid at read time; a write does not
invalidate them, and GC prunes them by `retain`. Either way, a cached entry is only ever
returned for a snapshot it is correct for. The only content cache without an epoch is the
parsed-query cache from Section 2.4, because a parse result does not depend on graph contents.

== Recap: the epoch invariant

Trace the epoch through the chapter and the read path becomes one idea:

+ The service does *not* pin an epoch. It rejects any client-supplied `read_epoch` and sets
  `context.read_epoch = None` (`service.rs:753`, `:803`), leaving the choice to the shard.
+ The shard pins it: it opens a SlateDB snapshot, reads `keys::last_epoch(cell_id)` from
  inside that snapshot, binds the epoch to `snapshot.seq()` with
  `with_validated_storage_read_epoch`, and scopes the query under it (`query.rs:288-315`). The
  epoch it used is reported back out for the client's bookmark.
+ It is validated, not by `query_read_epoch` (which does no rejection) but by
  `validate_executable_query_plan` (`query.rs:1832`), which raises `SnapshotAhead` if a
  `read_epoch` is newer than the cell's current epoch.
+ Every storage read is filtered by it: base artifacts with `base_epoch <= read_epoch` and
  deltas with `epoch > read_epoch` dropped (and, on the traversal/neighbor path, segments cut
  off past it).
+ Every content-dependent cache key embeds an epoch.

The result is that a read presents one consistent snapshot across matrix artifacts, deltas,
segments, traversal kernels, and caches, no matter what writers are doing at the same time.
The next chapter turns to those writers and shows how an epoch is advanced in the first place.
