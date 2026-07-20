#import "../template.typ": custom-box, srcblock, accent, muted
#import "@preview/fletcher:0.5.7" as fletcher: diagram, node, edge
#import "../vendor/bookly/src/themes/reader.typ": reader-colors

= The Read Path

This chapter follows one read from the moment a driver sends a query to the moment rows come
back. It is the longest chapter because reading is where most of the engine's code lives:
`src/shard/query.rs` alone is 8389 lines. We take it in stages and quote the real functions at
each step.

Keep one idea in mind the whole way through, because it is the thread that ties the chapter
together: a read is bound to a single SlateDB snapshot when it reaches the shard, the snapshot's
own sequence number is read straight back out of it, and that one number — the *read epoch* of
Section 0.7 — is carried into every storage access, every index decision, and every cache key.
There is exactly one sequence type in the system, `StorageSequence` (`src/lib.rs:138-140`), so
"the epoch" and "the storage snapshot" are never two things that have to be reconciled. They are
the same number, and the code says so by passing it twice.

That single-axis design is what makes the rest of the chapter tractable. It also creates two
questions a snapshot alone cannot answer, and each gets its own section: *which* snapshot a
client is entitled to (Section 2.4, causal and strong reads), and what to do when a precomputed
index was built at an older sequence than the one the read pinned (Section 2.8, the WAL-tail
overlay).

== The journey of a read

Before the detail, here is the whole path. Each box is a stage, and each has its own section
below.

#figure(
  diagram(
    crossing-fill: reader-colors.paper,
    node-stroke: 0.6pt + reader-colors.info,
    node-fill: reader-colors.info_soft,
    edge-stroke: reader-colors.muted,
    spacing: (0pt, 0.66cm),
    node((0, 0), text(fill: reader-colors.text)[driver sends Bolt `RUN`/`PULL` or HTTPS `POST` (Section 2.2)], width: 13.6cm),
    edge((0, 0), (0, 1), "->", stroke: reader-colors.muted),
    node((0, 1), text(fill: reader-colors.text, hyphenate: false)[`ClientQueryService`: authorize, classify read/write, refuse client epochs (2.3)], width: 13.6cm),
    edge((0, 1), (0, 2), "->", stroke: reader-colors.muted),
    node((0, 2), text(fill: reader-colors.text, hyphenate: false)[honour the bookmark, or refresh for a strong read (2.4)], width: 13.6cm),
    edge((0, 2), (0, 3), "->", stroke: reader-colors.muted),
    node((0, 3), text(fill: reader-colors.text)[parse Cypher into `ParsedRowQuery` (2.5); optimize (2.6)], width: 13.6cm),
    edge((0, 3), (0, 4), "->", stroke: reader-colors.muted),
    node((0, 4), text(fill: reader-colors.text, hyphenate: false)[shard pins one SlateDB snapshot; patterns become scans (2.7)], fill: reader-colors.warn_soft, stroke: 0.6pt + reader-colors.warn, width: 13.6cm),
    edge((0, 4), (0, 5), "->", stroke: reader-colors.muted),
    node((0, 5), text(fill: reader-colors.text, hyphenate: false)[index generation + WAL-tail overlay, or traversal kernel (2.8, 2.11)], fill: reader-colors.purple_soft, stroke: 0.6pt + reader-colors.purple, width: 13.6cm),
    edge((0, 5), (0, 6), "->", stroke: reader-colors.muted),
    node((0, 6), text(fill: reader-colors.text, hyphenate: false)[apply `WHERE` / `RETURN`, build `QueryResultSet`, page back (2.9, 2.10)], fill: reader-colors.ok_soft, stroke: 0.6pt + reader-colors.ok, width: 13.6cm),
  ),
  caption: [The read path end to end: each box is a stage with its own section below. The amber
    stage is where the read epoch is pinned and every later stage inherits it; the purple stage
    is the acceleration layer, which is the only place a read consults something other than the
    canonical keys; the green stage is what the client receives.],
) <fig-read-journey>

The overall call chain in code is:

Bolt `RUN`/`PULL` or HTTP `POST` #sym.arrow.r `ClientQueryService::execute_rows` / `execute_page`
#sym.arrow.r `QueryCellClient::execute_cypher_rows{,_page}` #sym.arrow.r
`GraphShard::execute_opencypher_rows{,_page}` #sym.arrow.r pin a snapshot #sym.arrow.r parse
#sym.arrow.r optimize #sym.arrow.r execute #sym.arrow.r assemble `QueryResultSet`.

== Entry: Bolt and HTTPS

A read enters through one of the two front doors from Chapter 1.

The Bolt server is a state machine, `run_bolt_protocol` (`src/client/bolt.rs:646`), that reacts
to `(state, message)` pairs. A `RUN` message (a query) is accepted only in the `Ready` state. It
prepares the request, replies with the result column names, and moves to `Streaming`, where
`PULL` messages pull rows back. The step that turns the wire message into an engine request is
`prepare_bolt_run`, and that is where the database name becomes a concrete target — and where a
driver's bookmarks become an engine bookmark:

#srcblock("src/client/bolt.rs:1103-1120")[```rust
let database = selected_bolt_database(session, context, &extra)?;
let target = context
    .database_resolver
    .resolve_database(Some(&database))
    .map_err(graph_error_to_bolt)?;
// ...
let mut request =
    bolt_query_request(target.clone(), query_id.clone(), query, parameters, &extra)?;
// ...
if let Some(bookmark) = highest_matching_bookmark(&target, bolt_bookmarks_from_extra(&extra)?)? {
    request = request.after_bookmark(bookmark);
}
```]

This is the resolution chain from Chapter 1, Section 1.5, happening for real: the Bolt `db` field
goes through `ClientDatabaseResolver` and comes out as a `ClientQueryTarget` carrying the scope
and cell id. The bookmark handling on the last two lines is standard Neo4j driver behaviour —
drivers already collect bookmarks from every response and resend them — so TurboLay gets
read-your-writes from an unmodified driver. Section 2.4 takes that apart.

#custom-box(title: [Term — Auto-commit query], icon: "info")[
  A query that is its own transaction: it runs and commits by itself, with no surrounding
  `BEGIN` / `COMMIT`. TurboLay only supports auto-commit queries. An explicit transaction is
  refused.
]

The refusal is explicit. In the `Ready` state a `Begin`, `Commit`, or `Rollback` message fails
the connection immediately:

#srcblock("src/client/bolt.rs:1006-1012")[```rust
(
    BoltState::Ready,
    ClientMessage::Begin { .. } | ClientMessage::Commit | ClientMessage::Rollback,
) => {
    send_bolt_failure(&mut writer, &explicit_transactions_unsupported()).await?;
    state = BoltState::Failed;
}
```]

The HTTPS door is simpler. The router binds `POST /v1/graphs/{graph_id}/query` to
`execute_query` (`src/client/http.rs`), which authenticates, builds the target, converts the JSON
parameters, and calls the same service method the Bolt path uses. It reads an optional
`consistency` and an optional `bookmark` from the body, but a `read_epoch` in the body is
*rejected*, not honoured:

#srcblock("src/client/http.rs:466-481")[```rust
if let Some(consistency) = body.consistency {
    request = request.with_consistency(consistency);
}
if body.read_epoch.is_some() {
    return Err(HttpApiError::from_graph(GraphError::UnsupportedQuery {
        dialect: "HTTP",
        feature: "read_epoch is not a storage snapshot selector; use bookmark for causal reads"
            .to_string(),
    }));
}
if let Some(bookmark) = body.bookmark {
    request = request
        .after_bookmark(ClientBookmark::parse(&bookmark).map_err(HttpApiError::from_graph)?);
}
```]

Both doors therefore offer a client exactly the same two knobs — a consistency level and a
bookmark — and neither offers a way to name a snapshot directly. Both converge on
`ClientQueryService`.

== The service layer

#custom-box(title: [Term — ClientQueryService], icon: "info")[
  The shared layer behind both front doors. It does everything that is common to a query
  regardless of protocol: validate the request, decide whether it reads or writes, check the
  caller is allowed, refuse any client-supplied epoch, honour bookmarks and consistency levels,
  enforce quotas and timeouts, register a cancellation handle, and hand the query to the cell's
  shard. It does *not* pin the read epoch itself; that happens down in the shard.
]

The full-result entry point is `execute_rows` (`src/client/service.rs:776`); the paged entry
point is `execute_page` (`:871`), used by both Bolt `PULL` and HTTPS streaming. `execute_rows`
runs a fixed sequence: refuse a client epoch, validate the request, normalize the runtime limit,
authorize, resolve any `UNWIND` batch, register a cancellation token, then run the body under a
timeout and concurrency permits.

Three of those steps decide the whole character of the read.

*Read or write.* The service parses the query, classifies its access, and authorizes the caller
for that action on that scope in one step:

#srcblock("src/client/service.rs:1339-1350")[```rust
fn authorize_query(
    &self, session: &ClientQuerySession, request: &ClientQueryRequest,
) -> Result<QueryTransportAction> {
    let action = match classify_opencypher_query_access(&request.query)? {
        OpenCypherQueryAccess::Read => QueryTransportAction::Read,
        OpenCypherQueryAccess::Write => QueryTransportAction::Write,
    };
    self.authorize_scope(session, &request.target.scope, action)?;
    Ok(action)
}
```]

`classify_opencypher_query_access` (`src/query/opencypher.rs:306`) parses the query and looks for
write clauses; if it finds any it is a write, otherwise a read. The classification matters twice
below: only a read gets its epoch cleared, and only a read may ask for strong consistency.

*No client epochs.* A client may not hand in a `read_epoch`, and the refusal is the very first
statement in `execute_rows`:

#srcblock("src/client/service.rs:781-787")[```rust
if request.read_epoch.is_some() {
    return Err(GraphError::UnsupportedQuery {
        dialect: "ClientProtocol",
        feature: "historical graph epochs are not client query snapshots; use a bookmark for causal reads"
            .to_string(),
    });
}
```]

`execute_page` carries the same guard, but only when there is no cursor (`:871-878`): a
continuation page is a different situation, and Section 2.10 explains why it does not need one.

*Clear the epoch and let the shard choose.* For a read the service does the opposite of pinning.
It builds a `QueryContext`, forces `read_epoch` to `None`, runs the query, and then reads back
out of the result *which* snapshot the shard actually used:

#srcblock("src/client/service.rs:824-851 (abridged)")[```rust
self.validate_bookmark(&request).await?;
self.refresh_strong_read(&request, action).await?;
let mut context = query_context(&request, scalar_parameters.clone(), cancellation_token.clone());
if action == QueryTransportAction::Read {
    context.read_epoch = None;
    context.max_result_bytes = Some(self.inner.config.max_cursor_buffer_bytes);
}
let result = /* execute_batch / execute_cypher_rows */;
let read_epoch = result_read_epoch(&result, action)?;
let storage_sequence = result_storage_sequence(&result, action)?;
let bookmark = self.bookmark_after(&request, action, storage_sequence).await?;
```]

`result_read_epoch` and `result_storage_sequence` (`src/client/service.rs:1627-1656`) pull the two
fields off the returned `QueryResultSet` and *fail loudly* — `CorruptValue` — if a read came back
without them. A read that cannot say which snapshot answered it is treated as a bug, not as a
missing optional. `bookmark_after` (`:1405-1418`) then mints the bookmark the client will carry
to its next request.

The request the shard finally receives is a `QueryContext` whose epoch field is `None` and whose
sequence type is the only one there is:

#srcblock("src/query/algebra.rs:166-182")[```rust
pub struct QueryContext {
    pub scope: GraphScope,
    pub cell_id: String,
    pub idempotency_key: String,
    pub read_epoch: Option<StorageSequence>,
    pub result_window: QueryWindow,
    pub parameters: BTreeMap<String, VertexPropertyValue>,
    pub max_runtime_ms: Option<u64>,
    pub max_result_bytes: Option<u64>,
    refreshed_reader: bool,
    // cancellation_token, validated_read ...
}
```]

Two private fields do the real work. `refreshed_reader` is the one bit a strong read sets
(Section 2.4). `validated_read` is where the pin ends up: it records the cell, the epoch, and the
storage snapshot that justifies it, and it is written only by
`with_validated_storage_read_epoch` (`src/query/algebra.rs:258-270`) and read back through
`validated_read_epoch` (`:273`) and `validated_storage_sequence` (`:282`). Because
`validated_read_epoch` re-checks that the recorded cell and epoch still match the context, an
epoch that was set some other way cannot masquerade as a validated one.

*The pin the service skipped happens here.* The shard's entry point refuses an unvalidated epoch
outright, then, when the epoch is `None`, opens one SlateDB snapshot, takes its sequence, binds
the two together, and scopes the whole query under it:

#srcblock("src/shard/query.rs:450-479 (abridged)")[```rust
if context.read_epoch.is_some() && context.validated_read_epoch().is_none() {
    return Err(GraphError::UnsupportedQuery {
        dialect: "OpenCypher",
        feature: "historical graph epochs are not storage snapshots; execute against a current SlateDB snapshot"
            .to_string(),
    });
}
let result = if context.read_epoch.is_none() {
    let snapshot = if context.uses_refreshed_reader() {
        self.db.reader_snapshot().await
    } else {
        self.db.snapshot().await
    };
    match snapshot {
        Ok(snapshot) => {
            let read_epoch = snapshot.seq();
            let context = context.with_validated_storage_read_epoch(read_epoch, read_epoch);
            GraphStore::scope_snapshot(
                snapshot,
                self.execute_parsed_opencypher_rows_inner(context, query),
            )
            .await
        }
        Err(err) => Err(err),
    }
```]

Four details in that fragment carry the design.

+ `snapshot.seq()` is where the epoch comes from. The engine never allocates one, never
  increments one, and never reads a "current epoch" key — no such key exists.
+ `with_validated_storage_read_epoch(read_epoch, read_epoch)` passes the same number twice, once
  as the epoch the query runs at and once as the storage snapshot that justifies it. They are the
  same number because there is only one sequence.
+ The branch on `uses_refreshed_reader()` is the entire runtime cost of strong consistency:
  `db.reader_snapshot()` (`src/core/state.rs:337-343`) always opens a fresh reader snapshot,
  whereas `db.snapshot()` (`:318-334`) will happily reuse a snapshot already installed in
  task-local state, or the writer's own.
+ `GraphStore::scope_snapshot` (`src/core/state.rs:376-383`) installs the snapshot in a task-local
  so every `get` and `scan` deeper in the call stack is served from it without threading a handle
  through every function. That task-local is also why the *nested* `db.snapshot()` calls you will
  meet in Section 2.11 return the same snapshot rather than a newer one.

#custom-box(title: [Why], icon: "tip")[
  Refusing a client-supplied epoch looks unhelpful until you ask what honouring one would mean. A
  `StorageSequence` from the past names a SlateDB snapshot that may already have been compacted
  away; serving it would require either retaining every version forever or quietly answering from
  some other state. TurboLay declines both. What clients actually want in practice is not time
  travel but a *floor* — "at least as fresh as what I already saw" — and a floor is cheap to
  honour and always satisfiable by waiting. That is the bookmark, and it is the next section.
]

#figure(
  diagram(
    crossing-fill: reader-colors.paper,
    node-stroke: 0.6pt,
    spacing: (14mm, 10mm),
    node((0.5, 0), text(fill: reader-colors.text, size: 8.5pt, hyphenate: false)[client request\ (bookmark + consistency,\ never an epoch)],
      fill: reader-colors.surface_soft, stroke: reader-colors.border, corner-radius: 3pt, width: 5.0cm),
    edge((0.5, 0), (0.5, 1), "->", stroke: reader-colors.muted),
    node((0.5, 1), text(fill: reader-colors.text, size: 8.5pt, hyphenate: false)[service: refuse `read_epoch`,\ set `context.read_epoch = None`],
      fill: reader-colors.surface_soft, stroke: reader-colors.border, corner-radius: 3pt, width: 5.6cm),
    edge((0.5, 1), (0.5, 2), "->", stroke: reader-colors.muted),
    node((0.5, 2), text(fill: reader-colors.text, size: 8.5pt, hyphenate: false)[shard opens one snapshot\ `read_epoch = snapshot.seq()`],
      fill: reader-colors.info_soft, stroke: reader-colors.info, corner-radius: 3pt, width: 5.6cm),
    edge((0.5, 2), (0.5, 3), "->",
      text(fill: reader-colors.muted, size: 7pt)[`with_validated_storage_read_epoch(S, S)`],
      stroke: reader-colors.muted, label-side: right),
    node((0.5, 3), align(center)[
      #text(fill: reader-colors.text, size: 8pt)[every canonical key read at #emph[S]] \
      #text(fill: reader-colors.text, size: 8pt)[every index generation judged against #emph[S]] \
      #text(fill: reader-colors.text, size: 8pt)[every content cache keyed on #emph[S]]
    ], fill: reader-colors.ok_soft, stroke: reader-colors.ok, corner-radius: 3pt, width: 7.4cm),
    edge((0.5, 3), (0.5, 4), "->", text(fill: reader-colors.muted, size: 7pt)[`result_storage_sequence`], stroke: reader-colors.muted, label-side: right),
    node((0.5, 4), text(fill: reader-colors.text, size: 8.5pt, hyphenate: false)[rows + a bookmark naming #emph[S]],
      fill: reader-colors.surface_soft, stroke: reader-colors.border, corner-radius: 3pt, width: 5.6cm),
    node((-0.62, 3), text(fill: reader-colors.muted, size: 7.5pt, style: "italic")[scoped under\ one snapshot],
      stroke: none, fill: none, width: 2.6cm),
    node(enclose: ((-0.62, 3), (0.5, 2), (0.5, 3)),
      stroke: (paint: reader-colors.muted, dash: "dotted"), fill: none,
      inset: 11pt, corner-radius: 5pt),
  ),
  caption: [One read, one snapshot, one number: the client may name a floor but never a snapshot;
    the service clears the epoch; the shard opens a single SlateDB snapshot, takes its sequence
    #emph[S], and binds #emph[S] as both the read epoch and the storage snapshot that justifies
    it. The dotted region is the part of the query that runs inside `scope_snapshot`, and the
    same #emph[S] leaves again as the client's next bookmark.],
) <fig-read-one-snapshot>

== Causal reads and bookmarks

A snapshot makes one query coherent with itself. It says nothing about how two *consecutive*
queries relate, and that gap is where the most common surprise in a distributed database lives.
Chapter 0, Section 0.8 named the two levels; this section is the mechanism.

The failure is easy to reproduce. Every node in a TurboLay cluster opens every cell as a reader
(Section 1.6), and a reader's view of the object store advances only when it refreshes. So a
client can write an edge on node A, be told the write is durable, send its next query to node B,
and have node B pin a snapshot that predates the write. Nothing is corrupt. The second read is
simply *earlier* than the first write, and no amount of snapshot discipline inside that one query
can fix it.

The blunt fix — make every read refresh to the newest durable state — is correct and expensive:
it turns every read into a round trip to the object store whether the client needed it or not.
TurboLay instead names two levels and defaults to the cheap one:

#srcblock("src/client/service.rs:269-273")[```rust
pub enum ClientReadConsistency {
    #[default]
    Causal,
    Strong,
}
```]

The level is chosen per request with `with_consistency` (`:328-331`) or the `strong()` shortcut
(`:333-336`), and both front doors expose it: HTTPS as a `consistency` body field
(`src/client/http.rs:466-468`), Bolt as either a `consistency` key in the `RUN` metadata or
`turbolay.consistency` inside `tx_metadata`, with a protocol error if the two disagree
(`src/client/bolt.rs:1156-1180`).

=== The bookmark

#custom-box(title: [Term — Bookmark], icon: "info")[
  An opaque token naming a point in one cell's history that the client has already observed. A
  `ClientBookmark` is a query target plus a `StorageSequence`, encoded as a printable string so it
  can travel through a driver that knows nothing about TurboLay. Sending one with a request means:
  *do not answer me from a state older than this.*
]

#srcblock("src/client/service.rs:130-148")[```rust
pub struct ClientBookmark {
    pub target: ClientQueryTarget,
    pub epoch: StorageSequence,
}

impl ClientBookmark {
    pub fn encode(&self) -> String {
        format!(
            "sgk:1:{}:{}:{}:{}",
            hex_encode(self.target.scope.namespace.to_string().as_bytes()),
            hex_encode(self.target.scope.graph_id.as_str().as_bytes()),
            hex_encode(self.target.cell_id.as_bytes()),
            self.epoch
        )
    }
}
```]

The encoding is worth a second look because it is doing more than carrying a number. Hex-encoding
the namespace, graph id, and cell id makes the token self-describing and unambiguous under a
`:` separator, and the leading `sgk:1:` is a version tag. The consequence is that a bookmark is
*bound to one cell*: presenting it against a different target is rejected rather than reinterpreted
(`validate_bookmark`, `src/client/service.rs:1367-1378`, raises `GraphScopeMismatch`), and the Bolt
door rejects it at the protocol level with Neo4j's own
`Neo.ClientError.Transaction.InvalidBookmark` (`src/client/bolt/values.rs:16-21`).

The client never constructs one. Every read result carries a fresh bookmark back out:

#srcblock("src/client/service.rs:340-345")[```rust
pub struct ClientQueryResult {
    pub query_id: String,
    pub result: QueryResultSet,
    pub read_epoch: Option<StorageSequence>,
    pub bookmark: Option<ClientBookmark>,
}
```]

and `bookmark_after` (`src/client/service.rs:1405-1418`) mints it from the sequence the read
actually used, or — for a *write*, which reports no read epoch — from the cell's current durable
sequence:

#srcblock("src/client/service.rs:1405-1418")[```rust
async fn bookmark_after(
    &self, request: &ClientQueryRequest, action: QueryTransportAction,
    read_storage_sequence: Option<StorageSequence>,
) -> Result<Option<ClientBookmark>> {
    let sequence = if action == QueryTransportAction::Read {
        read_storage_sequence
    } else {
        self.inner.client
            .current_storage_sequence(&request.target.scope, &request.target.cell_id).await?
    };
    Ok(sequence.map(|sequence| ClientBookmark::new(request.target.clone(), sequence)))
}
```]

That write branch is the load-bearing one. It is what makes *read-your-writes* work: the client's
`CREATE` returns a bookmark naming the sequence at which the write is durable, and the next read
carrying that bookmark cannot be served from anything older. A Bolt driver holding several
bookmarks sends them all, and `highest_matching_bookmark` (`src/client/bolt/values.rs:9-30`) keeps
the largest epoch after checking every one belongs to this target.

=== Honouring a causal read

A causal read waits for the cell to reach the bookmark's sequence, and refuses rather than
silently serving something older:

#srcblock("src/client/service.rs:719-741")[```rust
pub async fn ensure_bookmark(&self, bookmark: &ClientBookmark) -> Result<()> {
    let current_sequence = self.inner.client
        .wait_for_storage_sequence(
            &bookmark.target.scope, &bookmark.target.cell_id, bookmark.epoch,
        ).await?
        .ok_or_else(|| GraphError::UnsupportedQuery {
            dialect: "ClientProtocol",
            feature: "backend cannot prove bookmark durability".to_string(),
        })?;
    if current_sequence < bookmark.epoch {
        return Err(GraphError::SnapshotAhead { /* cell_id, read_epoch, current_epoch */ });
    }
    Ok(())
}
```]

The waiting is real, bounded, and happens in the shard. `wait_for_storage_sequence`
(`src/shard/lifecycle.rs:590-617`) checks the durable sequence, returns immediately if it already
meets the floor, and otherwise polls `refresh_durable_reader` every 10 ms until the query's own
runtime limit (default 30 s) expires, at which point it raises `SnapshotAhead`:

#srcblock("src/shard/lifecycle.rs:596-616 (abridged)")[```rust
let current = self.db.durable_sequence().await?;
if current >= minimum { return Ok(current); }
let deadline = std::time::Instant::now() + Duration::from_millis(
    self.limits.max_query_runtime_ms.unwrap_or(30_000).max(1));
loop {
    let current = self.db.refresh_durable_reader().await?;
    if current >= minimum { return Ok(current); }
    if std::time::Instant::now() >= deadline {
        return Err(GraphError::SnapshotAhead { /* ... */ });
    }
    tokio::time::sleep(Duration::from_millis(10)).await;
}
```]

Note the shape of the fast path: when the reader is already caught up — which is the common case
— `ensure_bookmark` costs one in-memory read of `writer.status().durable_seq` or
`reader.status().durable_seq` (`src/core/state.rs:346-351`) and no object-store traffic at all.
Causal consistency is free when it is already satisfied. It only costs something when it is
actually doing work.

=== Honouring a strong read

A strong read does not wait for a number the client supplied; it goes and finds the current one:

#srcblock("src/client/service.rs:1380-1402 (abridged)")[```rust
async fn refresh_strong_read(
    &self, request: &ClientQueryRequest, action: QueryTransportAction,
) -> Result<()> {
    if request.consistency != ClientReadConsistency::Strong { return Ok(()); }
    if action != QueryTransportAction::Read {
        return Err(GraphError::UnsupportedQuery {
            dialect: "ClientProtocol",
            feature: "strong consistency applies only to read queries".to_string(),
        });
    }
    self.inner.client
        .refresh_storage_sequence(&request.target.scope, &request.target.cell_id).await?
        .ok_or_else(|| /* backend cannot refresh the latest durable SlateDB frontier */)?;
    Ok(())
}
```]

Three things happen there. Strong is a no-op unless asked for, so the branch costs nothing on the
default path. A strong *write* is rejected as meaningless rather than quietly ignored — a write
establishes its own point in the sequence, so asking it to be strongly consistent is a category
error. And `refresh_storage_sequence` (`src/shard/lifecycle.rs:583-588`) calls
`refresh_durable_reader` (`src/core/state.rs:353-357`), which forces the `DbReader` to re-read
the object store and pick up everything committed by any writer, anywhere.

Refreshing the reader is only half of it, because the query has not started yet and might still
pin a stale snapshot. The other half is one bit set on the context:

#srcblock("src/client/service.rs:1659-1678 (abridged)")[```rust
fn query_context(/* ... */) -> QueryContext {
    let mut context = QueryContext::new(&request.target.cell_id, &request.query_id)
        // scope, parameters, cancellation token ...
    if request.consistency == ClientReadConsistency::Strong {
        context = context.with_refreshed_reader();
    }
    context
}
```]

`with_refreshed_reader` (`src/query/algebra.rs:242-246`) sets the private `refreshed_reader` flag,
and `uses_refreshed_reader` (`:248`) is read in exactly one place: the snapshot branch in
Section 2.3. That is how the request-level word "strong" becomes the storage-level choice of
`db.reader_snapshot()` over `db.snapshot()` — and, in particular, how it defeats the task-local
snapshot reuse inside `db.snapshot()` that would otherwise hand back a snapshot pinned earlier in
the same task.

#figure(
  diagram(
    crossing-fill: reader-colors.paper,
    spacing: (3.1cm, 0.85cm),
    node-stroke: none,
    node((0, 0), text(weight: "bold", fill: reader-colors.text)[Client]),
    node((1, 0), text(weight: "bold", fill: reader-colors.text)[node A (writer)]),
    node((2, 0), text(weight: "bold", fill: reader-colors.text)[node B (reader)]),
    edge((0, 0), (0, 7), stroke: (dash: "dotted", paint: reader-colors.border)),
    edge((1, 0), (1, 7), stroke: (dash: "dotted", paint: reader-colors.border)),
    edge((2, 0), (2, 7), stroke: (dash: "dotted", paint: reader-colors.border)),
    edge((0, 1), (1, 1), "->", text(fill: reader-colors.muted, size: 8pt)[`CREATE` edge],
      stroke: reader-colors.muted),
    node((1, 2), text(fill: reader-colors.text, size: 8pt)[commit durable at #emph[S]],
      fill: reader-colors.ok_soft, stroke: 0.5pt + reader-colors.border, inset: 5pt),
    edge((1, 3), (0, 3), "->", text(fill: reader-colors.muted, size: 8pt)[bookmark #emph[S]],
      stroke: reader-colors.muted),
    edge((0, 4), (2, 4), "->", text(fill: reader-colors.muted, size: 8pt)[`MATCH` + bookmark #emph[S]],
      stroke: reader-colors.muted),
    node((2, 5), text(fill: reader-colors.text, size: 8pt, hyphenate: false)[`ensure_bookmark`:\ durable seq #sym.lt #emph[S], so\ refresh and re-check],
      fill: reader-colors.warn_soft, stroke: 0.5pt + reader-colors.border, inset: 5pt),
    node((2, 6), text(fill: reader-colors.text, size: 8pt, hyphenate: false)[pin snapshot at #sym.gt.eq #emph[S];\ the write is visible],
      fill: reader-colors.ok_soft, stroke: 0.5pt + reader-colors.border, inset: 5pt),
    edge((2, 7), (0, 7), "->", text(fill: reader-colors.muted, size: 8pt)[rows + bookmark #sym.gt.eq #emph[S]],
      stroke: 0.7pt + reader-colors.ok),
  ),
  caption: [A causal read across two nodes: the write returns a bookmark naming the sequence
    #emph[S] at which it became durable, and the follow-up read on a different node blocks in
    `ensure_bookmark` until its own reader has caught up to #emph[S] before pinning a snapshot.
    Had node B already been at or past #emph[S] the amber step would have been a single in-memory
    comparison with no object-store traffic; had it never caught up within the query's runtime
    limit, the read would have failed with `SnapshotAhead` rather than returning stale rows.],
) <fig-read-causal-bookmark>

#custom-box(title: [Why], icon: "tip")[
  Causal is the default because it matches what users actually complain about. Almost every
  grievance filed against "eventual consistency" is really a grievance about read-your-writes:
  people are untroubled by not seeing a stranger's edit and very troubled by not seeing their own.
  A bookmark buys read-your-writes for the price of a short string on the wire and, in the common
  case, zero coordination. Strong stays available for the genuinely different requirement — *see
  everyone's* writes — and charges for itself at the point of use, in one extra reader refresh,
  rather than taxing every read in the system.
]

#custom-box(title: [Term — SnapshotAhead], icon: "info")[
  The error raised when a required sequence is ahead of what the cell can prove durable: by
  `ensure_bookmark` when a bookmark cannot be reached within the query's runtime limit
  (`src/client/service.rs:731-737`), and by `wait_for_storage_sequence` when the polling deadline
  expires (`src/shard/lifecycle.rs:610-615`). It is a *timeout on catching up*, not a claim that
  the client is wrong, and retrying it is reasonable.
]

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

`with_parsed_cypher` (`src/query/opencypher.rs:315`) runs a C parser (`libcypher-parser`, reached
through FFI) to get a raw syntax tree — reusing one from a thread-local cache of `ParsedCypher`
values when it can — and hands that tree to the closure. The closure, `lower_row_query`, is
TurboLay's own work: it walks the tree and lowers it into `ParsedRowQuery`, rejecting anything the
engine does not implement. The sibling entry points (`lower_mutation_query`, `query_access`) go
through the same `with_parsed_cypher` wrapper, so they all share one parse.

#custom-box(title: [Term — Lowering], icon: "info")[
  Translating a general syntax tree into a smaller, stricter internal form that the engine knows
  how to run. TurboLay's lowering is where unsupported Cypher is turned away, so the execution
  code below it can assume a narrow, well-formed plan.
]

`ParsedRowQuery` is that internal form. It is a flat description of one row query: the patterns to
match, an optional filter, the projections to return, ordering, a result window, and the output
column names:

#srcblock("src/query/opencypher.rs:23-35")[```rust
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

The supporting types are small closed enums, all in `opencypher.rs`. A `RowPattern` is either a
`Node` or an `Edge` (`:117`). A `RowEdgePattern` (`:130`) carries an optional
`hop_range: Option<(u8, u8)>`, which is how a bounded variable-length path such as
`-[:RELATES*1..3]-` is represented; the traversal section returns to it. A `RowProjection` is a
`NodeId`, a `Property`, `CountAll`, or an `Aggregate` (`:148`), and `RowAggregateFunction` is
`Count`, `Sum`, `Avg`, or `Collect` (`:164`). A `RowPredicate` is a tree of `Compare`, `And`,
`Or`, and `Not` (`:231`).

*The parse cache.* Parsing is pure work that depends only on the query text, so TurboLay caches
the parsed form, but only when the query has no parameters (a parameterized query is cheap to
re-lower and the parameter values must be folded in):

#srcblock("src/shard/query.rs:414-442 (abridged)")[```rust
async fn parsed_opencypher_row_query(
    &self, cell_id: &str, query: &str,
    parameters: &BTreeMap<String, VertexPropertyValue>,
) -> Result<ParsedRowQuery> {
    if !parameters.is_empty() {
        return parse_opencypher_row_query_with_parameters(query, parameters);
    }
    let key = ParsedRowQueryCacheKey::new(query);
    if let Some(parsed) = self.parsed_row_query_cache.lock().await.get(&key) {
        self.cache_metrics.record_hit(GraphCacheKind::ParsedRowQuery);
        return Ok(parsed);
    }
    self.cache_metrics.record_miss(GraphCacheKind::ParsedRowQuery);
    let parsed = parse_opencypher_row_query_with_parameters(query, parameters)?;
    self.parsed_row_query_cache.lock().await.insert(
        key, parsed.clone(), cell_id.to_string(), false, &self.cache_metrics,
    );
    Ok(parsed)
}
```]

This cache key holds only the query string. It is the one read cache that does not carry an epoch,
because a parse result does not depend on graph contents.

== The optimizer

Before scanning, the engine chooses how to reach each pattern. For row queries the choice is made
lazily as patterns are matched, not as a separate up-front pass. The decision function is
`best_row_edge_access` (`src/shard/query_optimizer.rs:459`), wrapped by
`best_row_edge_access_with_stats` (`:369`), which returns a `RowQueryAccess` describing the
cheapest way to satisfy one edge pattern.

#custom-box(title: [Term — Access path], icon: "info")[
  The concrete method chosen to satisfy a pattern. For an edge, the choices include expanding from
  a known source using the outgoing adjacency index, expanding backward using the incoming index,
  checking a single edge's existence, using an edge-property index, walking a variable-length
  range, or, as a last resort, scanning every edge of that type.
]

The choice is cost-based. Each candidate is given an estimated cardinality (how many rows it will
produce), and the engine picks the smallest, breaking ties by a fixed priority. The estimates come
from persisted statistics:

#custom-box(title: [Term — Query statistics (qstats)], icon: "info")[
  Counts the engine keeps about the graph so the optimizer can guess selectivity: how many edges
  of a type exist, how many distinct values a property has, how common the most common value is.
  They are stored in the cell under `cell/<id>/qstats/…` (as `QueryStatsRecord`) and refreshed by
  a background job. If a statistic is stale, its cost estimate is inflated so the optimizer treats
  it with suspicion.
]

`query_stats_estimate` (`src/shard/query_optimizer.rs:579`) reads a `QueryStatsRecord` and
`stats_record_cost_estimate` (`:623`) multiplies the estimate by four when the record is stale. A
`QueryStatsRecord` (`src/query/algebra.rs:393`) holds `count`, `read_epoch`, `refreshed_at_ms`,
`distinct_values`, `total_values`, and `most_common_count`, and offers an `equality_estimate()`
for the selectivity of a property equality. Note that its `read_epoch` field is a
`StorageSequence` like every other epoch in the system: a statistic records the snapshot it was
computed against, which is exactly what makes "is this stale?" a well-posed question.

#custom-box(title: [Why], icon: "tip")[
  Statistics are stored per cell and stamped with the sequence they were computed at, rather than
  recomputed per query, because the graph lives on a remote object store where counting is
  expensive. Inflating stale estimates is a safety valve: a wrong-but-cheap-looking plan is worse
  than a plan the optimizer already distrusts, so staleness pushes the optimizer toward more
  conservative access paths.
]

== Execution: from a pattern to a scan

Execution begins at `execute_opencypher_rows` (`src/shard/query.rs:262`), which ties parse and run
together, and lands in `execute_parsed_opencypher_rows` — the snapshot-pinning function from
Section 2.3. Inside the snapshot, `execute_parsed_opencypher_rows_inner` (`:506`) sets up the
guard rails and resolves the epoch one last time:

#srcblock("src/shard/query.rs:512-521")[```rust
let budget = QueryBudget::new(
    context.max_runtime_ms.or(self.limits.max_query_runtime_ms),
    context.cancellation_token.clone(),
)
.with_max_result_bytes(context.max_result_bytes);
budget.check("cypher_rows")?;
let storage_sequence = context.validated_storage_sequence();
let read_epoch = self.query_read_epoch(&context).await?;
```]

#custom-box(title: [Term — QueryBudget], icon: "info")[
  The per-query guard that every deep loop consults. It carries the runtime deadline, the
  cancellation token, and the result-byte ceiling, and `budget.check("…")` is called with a
  named site — `cypher_edge_sources`, `graph_index_wal_entry`, `query_out_neighbors_scan` — so
  that a query killed by its budget says *where* it was killed. Passing the budget explicitly,
  rather than checking a global, is why an expensive traversal can be aborted mid-expansion.
]

`query_read_epoch` (`:1821-1826`) is deliberately trivial: it returns the validated epoch if there
is one, and otherwise falls back to `current_epoch`. It does no validation of its own — the
rejection already happened at the entry point, and the *plan*-level check lives in
`validate_executable_query_plan` (`:1797-1818`), which raises `SnapshotAhead` for an epoch beyond
the cell's current sequence and rejects any other mismatch with *"stale query plans are not pinned
SlateDB snapshots"*. And `current_epoch` (`:5983-5987`) is now a one-liner that says the whole
story of the resync:

#srcblock("src/shard/query.rs:5983-5987")[```rust
pub async fn current_epoch(&self, cell_id: &str) -> Result<StorageSequence> {
    validate_component("cell_id", cell_id)?;
    self.ensure_cell_readable(cell_id, "current_epoch").await?;
    Ok(self.db.snapshot().await?.seq())
}
```]

"The current epoch of a cell" is not a stored value that anyone maintains. It is whatever sequence
a snapshot opened right now reports.

From there the query routes to the union path or to `execute_single_opencypher_rows` (`:599`),
which tries four fast paths in order — the graph-kernel traversal path (Section 2.11), a
source-relationship-id path, a relationship-count path, and a relationship-rows path — before
falling back to generic pattern matching. We follow the generic path, because the fast paths are
specializations of it.

Generic matching is `match_row_patterns` (`:2628`), which for each edge pattern with a known source
scans that source's neighbours. This is the exact point where the graph model becomes storage
access:

#srcblock("src/shard/query.rs:3296-3307")[```rust
for src in sources {
    budget.check("cypher_edge_sources")?;
    let neighbors = self
        .out_neighbors_at_for_query(cell_id, &edge.edge_type, src, read_epoch, budget)
        .await?;
    scanned_edges = scanned_edges.saturating_add(neighbors.len() as u64);
    self.ensure_query_scan_edges("cypher_edge_neighbor_scan", scanned_edges)?;
    for dst in neighbors {
        self.push_matching_edge_row(edge, src, dst, &mut state).await?;
    }
}
```]

`out_neighbors_at_for_query` (`:5689-5711`) is the representative neighbour scan, and it is
strikingly plain — it asks for every edge of the type as of the read epoch and keeps the ones
leaving `src`:

#srcblock("src/shard/query.rs:5689-5711 (abridged)")[```rust
async fn out_neighbors_at_for_query(
    &self, cell_id: &str, edge_type: &str, src: VertexId,
    read_epoch: StorageSequence, budget: &QueryBudget,
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

#custom-box(title: [Why], icon: "tip")[
  Filtering an all-edges list down to one source looks wasteful, and it is: this path is
  #sym.Theta of the number of edges of the type, per source vertex, not #sym.Theta of the source's
  degree. It is honest to say so. The generic row path exists to be *correct for every pattern
  shape*, and the shapes that matter for performance — anchored traversals with a hop range,
  relationship lookups, counts — are intercepted by the fast paths in
  `execute_single_opencypher_rows` before they ever reach here. The budget is what keeps the
  general case from running away: `max_query_scan_edges` is enforced inside the scan, so a query
  that would degenerate is rejected rather than served slowly.
]

The reverse direction uses `in_neighbors_at_for_query` (`:5935`), and a pattern with no bound
endpoint falls back to a full edge scan. All of them build their keys from the functions in
`keys.rs` (`out_prefix`, `in_prefix`, `out_edge`, and so on from Chapter 0, Section 0.6) and read
them through two thin wrappers in `src/shard/maintenance.rs`: `read_remote` for a point `get`, and
`scan_remote_prefix` for a prefix `scan`. Those wrappers set the storage read options:

#srcblock("src/codec.rs:162-174")[```rust
pub(crate) fn remote_read_options() -> ReadOptions {
    ReadOptions { durability_filter: DurabilityLevel::Remote, ..Default::default() }
}

pub(crate) fn remote_scan_options() -> ScanOptions {
    ScanOptions::default()
        .with_durability_filter(DurabilityLevel::Remote)
        .with_cache_blocks(false)
}
```]

#custom-box(title: [Term — Durability filter], icon: "info")[
  A read option that tells SlateDB which data is allowed to satisfy the read. `Remote` means only
  data that has reached the object store durably counts; data still sitting in a local memtable is
  ignored. TurboLay reads at `Remote` so that every reader, including a read-only `DbReader` on
  another machine, sees the same durable snapshot — which is also what makes the durable sequence
  a meaningful thing for a bookmark to name.
]

== Index generations and the WAL-tail overlay

Everything so far reads canonical keys under the pinned snapshot. Nothing needs reconciling,
because the snapshot *is* the version. This section is about the one place where that is not the
whole story: traversal acceleration, where the engine consults a precomputed structure that was
built at some *earlier* sequence and must be reconciled with the sequence the read pinned.

First, what the generic path does, so the contrast is clear. `edges_at_with_budget`
(`src/shard/query.rs:6022-6056`) delegates to `canonical_adjacency_at`
(`src/engine/artifact_build.rs:534-549`) and flattens the result:

#srcblock("src/shard/query.rs:6022-6042 (abridged)")[```rust
async fn edges_at_with_budget(
    &self, cell_id: &str, edge_type: &str,
    read_epoch: StorageSequence, budget: Option<&QueryBudget>,
) -> Result<Vec<EdgeRecord>> {
    self.ensure_cell_readable(cell_id, "edges_at").await?;
    let adjacency = self.canonical_adjacency_at(cell_id, edge_type, read_epoch).await?;
    let mut edges = Vec::new();
    for (src, destinations) in adjacency {
        for dst in destinations {
            edges.push(EdgeRecord { cell_id: /* .. */, edge_type: /* .. */, src, dst });
            /* ensure_limit("query_edges_at_canonical", …, max_query_scan_edges) */
        }
    }
    Ok(edges)
}
```]

`canonical_adjacency_at` in turn calls `current_matrix_rows`
(`src/engine/artifact_build.rs:488-532`), which scans the `e/out/<type>/` adjacency prefix, then
scans the compacted `seg/out/<type>/` segments, dropping any segment whose `storage_sequence`
exceeds the read epoch and any destination covered by an `seg/tomb/out/` tombstone at or before
it. There is no artifact, no index, and nothing to overlay: this is the canonical truth, read
under the snapshot. `EdgeRecord` itself carries no sequence field — when an edge became visible is
a property of the snapshot you read it through, not of the row (Chapter 0, Section 0.5).

#custom-box(title: [Why], icon: "tip")[
  A previous edition of this book described reading as a *two-layer merge*: a compacted base plus
  a replay of delta records in `(base, read]`, applying `Plus` as an insert and `Minus` as a
  remove. That subsystem — the delta log, the outbox, the mutation log, `DeltaKind`, and the delta
  GC watermark — was deleted wholesale in the graph-kernel resync. Nothing replays a log at read
  time. If you remember TurboLay working that way, that memory is of a system that no longer
  exists; the mechanism below is what took its place, and it differs in kind, not just in detail.
]

=== The gap the overlay closes

Section 1.7 introduced the *index generation*: an immutable, content-addressed GraphBLAS CSC image
of one cell's adjacency for one edge type, built out-of-process and published to the object store.

#srcblock("src/engine/index_store.rs:11-20")[```rust
pub struct GraphIndexGeneration {
    pub cell_id: String,
    pub edge_type: String,
    pub base_sequence: StorageSequence,  // the snapshot it was built at
    pub last_wal_id: u64,                // last WAL entry folded in
    pub edge_count: u64,
    pub checksum: u64,
    pub generation: String,              // sha256 hex of the CSC payload
}
```]

A generation is built at `base_sequence` #emph[B]. A read pins `read_epoch` #emph[S]. Because the
indexer runs on a timer in a separate process (default `GRAPH_INDEXER_INTERVAL_MS=5000`), #emph[S]
is normally *ahead* of #emph[B], and everything written in between is invisible to the generation.
Serving the traversal from the generation alone would silently answer an older question than the
one the client asked.

Three responses are possible and TurboLay uses all three, in order of preference: if the gap is
empty, use the generation directly; if the gap is small and readable, close it exactly; if it
cannot be closed, do not use the generation at all.

#custom-box(title: [Term — WAL-tail overlay], icon: "info")[
  The set of edges whose existence changed between an index generation's `base_sequence` and the
  read epoch, computed by reading the SlateDB write-ahead-log files written since the generation
  and then resolving each affected pair against the pinned snapshot. It is applied *on top of* the
  compiled matrix during expansion, so a traversal answers at the read epoch even though the
  matrix it walks was built earlier. It is a *repair*, not a source of truth: the snapshot decides
  what is true, and the WAL only says where to look.
]

=== `topology_tail_since`, line by line

The whole mechanism is one function, `src/shard/topology_tail.rs:28-96`. It is worth reading in
four pieces, because each is a distinct decision.

*Piece one: the three ways to answer without reading anything.*

#srcblock("src/shard/topology_tail.rs:28-44")[```rust
pub(crate) async fn topology_tail_since(
    &self,
    generation: &crate::GraphIndexGeneration,
    snapshot: &GraphStorageSnapshot,
    read_sequence: StorageSequence,
    budget: &QueryBudget,
) -> Result<GraphTopologyTail> {
    if snapshot.seq() != read_sequence {
        return Ok(GraphTopologyTail::Unavailable);
    }
    if generation.base_sequence >= read_sequence {
        return Ok(GraphTopologyTail::Complete(GraphTopologyOverlay::default()));
    }
    let last_wal_id = self.db.last_durable_wal_id().await?;
    if generation.last_wal_id >= last_wal_id {
        return Ok(GraphTopologyTail::Complete(GraphTopologyOverlay::default()));
    }
```]

The first guard is a consistency check, not an optimization: the overlay is only meaningful
against the very snapshot the read pinned, so if the snapshot handed in disagrees with the read
sequence the function declines rather than guessing. The second and third are the happy paths — a
generation already at or past the read, or one that has already folded in every durable WAL file —
and both return an *empty* overlay, which is a positive answer meaning "nothing changed", not a
failure.

*Piece two: read the WAL tail, and give up cleanly if you cannot.*

#srcblock("src/shard/topology_tail.rs:46-60")[```rust
let mut affected = BTreeSet::new();
let wal_reader = self.db.wal_reader();
for wal_id in generation.last_wal_id.saturating_add(1)..=last_wal_id {
    budget.check("graph_index_wal_file")?;
    let mut entries = match wal_reader.get(wal_id).iterator().await {
        Ok(entries) => entries,
        Err(error) => {
            tracing::debug!(
                wal_id, error = %error,
                "graph index WAL tail is unavailable; using snapshot adjacency"
            );
            return Ok(GraphTopologyTail::Unavailable);
        }
    };
```]

WAL files are garbage-collected by SlateDB once they are compacted into SSTs. If the indexer has
fallen far enough behind that the files it would need are gone, the read cannot reconstruct the
tail. `Unavailable` is the honest answer, and the log line says exactly what will happen instead.
This degradation path is a designed behaviour, not a bug to be hidden: correctness never depends
on the WAL being present, only performance does.

*Piece three: the WAL is a change-detector, not a value source.*

#srcblock("src/shard/topology_tail.rs:61-79 (abridged)")[```rust
while let Some(entry) = entries.next().await? {
    budget.check("graph_index_wal_entry")?;
    if entry.seq <= generation.base_sequence || entry.seq > read_sequence {
        continue;
    }
    collect_topology_entry(
        &entry.key, &entry.value,
        &generation.cell_id, &generation.edge_type, &mut affected,
    )?;
    ensure_limit(
        "graph_index_wal_affected_edges",
        affected.len() as u64, self.limits.max_query_scan_edges,
    )?;
}
```]

Entries outside `(base_sequence, read_sequence]` are skipped, so the overlay is bounded by the
same interval on both ends. `collect_topology_entry` (`:98-137`) then recognizes exactly three key
shapes and, crucially, records only the `(src, dst)` *pair*, never the value:

- `cell/<id>/e/out/<type>/<src>/<dst>` — a direct adjacency write;
- `cell/<id>/seg/tomb/out/<type>/<src>/<dst>` — a segment tombstone;
- `cell/<id>/seg/out/<type>/…` — a compacted segment, whose decoded `destinations` all become
  affected pairs.

Keys belonging to another cell or another edge type fall through the match arms and are ignored.

*Piece four: resolve every affected pair against the snapshot.*

#srcblock("src/shard/topology_tail.rs:81-96")[```rust
let mut overlay = GraphTopologyOverlay::default();
for (src, dst) in affected {
    budget.check("graph_index_wal_resolve_edge")?;
    let exists = self
        .edge_exists_in_storage_snapshot(
            snapshot, &generation.cell_id, &generation.edge_type, src, dst, read_sequence,
        )
        .await?;
    overlay.set(src, dst, exists);
}
Ok(GraphTopologyTail::Complete(overlay))
```]

This is the design's central move, and it is what makes the overlay so much simpler than a delta
log. The WAL is used only to answer *which pairs might have changed*. The truth about each one is
then read from the pinned snapshot, which already resolves the whole history — creations,
deletions, segment compaction, re-creation — to a single boolean. An edge added and removed inside
the interval resolves to `false` with no ordering logic; a pair touched five times costs one
lookup. The overlay is a `BTreeMap<VertexId, BTreeMap<VertexId, bool>>`
(`src/shard/topology_tail.rs:5-7`), and a `true` means "add this edge to the matrix's answer",
a `false` means "remove it".

#figure(
  diagram(
    crossing-fill: reader-colors.paper,
    node-stroke: 0.55pt,
    spacing: (16mm, 15mm),
    node((0, 0), text(fill: reader-colors.text, size: 7.5pt, hyphenate: false)[index generation\ built at #emph[B]\ (CSC matrix)],
      fill: reader-colors.purple_soft, stroke: reader-colors.purple, corner-radius: 3pt, width: 3.2cm),
    edge((0, 0), (1, 0), "->", text(fill: reader-colors.muted, size: 7pt)[hydrate],
      stroke: reader-colors.muted, label-side: left),
    node((1, 0), text(fill: reader-colors.text, size: 7.5pt, hyphenate: false)[compiled matrix\ answers at #emph[B]],
      fill: reader-colors.info_soft, stroke: reader-colors.info, corner-radius: 3pt, width: 3.2cm),
    edge((1, 0), (2, 0), "->", text(fill: reader-colors.muted, size: 7pt)[apply overlay],
      stroke: reader-colors.muted, label-side: left),
    node((2, 0), text(fill: reader-colors.text, size: 7.5pt, hyphenate: false)[traversal answers\ at #emph[S]],
      fill: reader-colors.ok_soft, stroke: reader-colors.ok, corner-radius: 3pt, width: 3.2cm),
    node((0, 1), text(fill: reader-colors.text, size: 7.5pt, hyphenate: false)[WAL files written\ after #emph[B]: which\ pairs changed?],
      fill: reader-colors.warn_soft, stroke: reader-colors.warn, corner-radius: 3pt, width: 3.2cm),
    edge((0, 1), (1, 1), "->", text(fill: reader-colors.muted, size: 7pt)[pairs],
      stroke: reader-colors.muted, label-side: right),
    node((1, 1), text(fill: reader-colors.text, size: 7.5pt, hyphenate: false)[pinned snapshot at #emph[S]:\ does each pair exist?],
      fill: reader-colors.info_soft, stroke: reader-colors.info, corner-radius: 3pt, width: 3.2cm),
    edge((1, 1), (1, 0), "->", text(fill: reader-colors.muted, size: 7pt)[overlay],
      stroke: reader-colors.muted, label-side: right),
    node((2, 1), text(fill: reader-colors.bad, size: 7.5pt, hyphenate: false)[WAL gone #sym.arrow.r\ `Unavailable` #sym.arrow.r\ snapshot adjacency],
      fill: none, stroke: (dash: "dashed", paint: reader-colors.bad), corner-radius: 3pt, width: 3.2cm),
    edge((1, 1), (2, 1), "-->", stroke: (dash: "dashed", paint: reader-colors.bad)),
  ),
  caption: [The WAL-tail overlay: the compiled matrix answers as of the generation's base sequence
    #emph[B], and the WAL files written since #emph[B] are read only to learn *which* vertex pairs
    might have changed. Each such pair is then resolved against the snapshot pinned at #emph[S],
    so the overlay records a plain exists/does-not-exist verdict and the expansion answers at
    #emph[S]. The dashed branch is the honest degradation: if those WAL files have already been
    compacted away, `topology_tail_since` returns `Unavailable` and the query abandons the
    accelerated path entirely rather than answering from a stale matrix.],
) <fig-read-wal-overlay>

=== Expanding through the overlay

The overlay is not merged into the matrix — that would mean rebuilding it. It is applied during
expansion, one hop at a time, by `expand_range_with_overlay`
(`src/shard/topology_tail.rs:140-197`). Each hop expands the frontier through the compiled
GraphBLAS matrix, then patches the result: destinations marked `true` are inserted, and a
destination marked `false` is removed *only if no other vertex in the frontier still reaches it* —
a check that consults the overlay first and falls back to
`compiled_graphblas_contains_edge` for pairs the overlay says nothing about. Getting that second
condition right is what stops one deleted edge from wrongly pruning a vertex that remains
reachable by another.

#custom-box(title: [Why], icon: "tip")[
  Compare the two designs on the same question: *what does a read pay per write that landed after
  the acceleration structure was built?* Under a delta log it paid a replay — ordered, per-edge,
  and growing with the number of changes, with a garbage-collection watermark to keep the log from
  growing without bound, and a whole class of bugs around a reader whose base had been collected.
  Under the overlay it pays one point lookup per *distinct pair touched*, against a snapshot it
  already holds open, with no ordering, no log to retain, and no watermark. The cost is a
  dependency on SlateDB's WAL still being present — and when it is not, the answer is a clean
  `Unavailable` and a fallback, rather than a wrong answer.
]

== WHERE and RETURN

Once patterns produce candidate rows (each row a set of bound vertices with hydrated properties),
the filter and the projection run over them.

The filter is `row_predicate_matches` (`src/shard/query.rs:7485`), a direct recursive walk of the
`RowPredicate` tree:

#srcblock("src/shard/query.rs:7485-7509 (shape)")[```rust
RowPredicate::Compare { left, op, right } =>
    compare_row_values(eval_row_expression(row, left)?, *op, eval_row_expression(row, right)?)?,
RowPredicate::And(l, r) => matches(l) && matches(r),
RowPredicate::Or(l, r)  => matches(l) || matches(r),
RowPredicate::Not(inner) => !matches(inner),
```]

`eval_row_expression` (`:7511`) resolves each side to a `NodeId`, a `Property`, or a `Literal`. A
property that is not present resolves to a "missing" value, which makes any comparison against it
false.

The projection has two modes. Without aggregates, `project_binding_row` (`:7628`) maps each
`RowProjection` straight to a `QueryValue` (a node id becomes `QueryValue::VertexId`, a property
becomes `QueryValue::Property`, absent becomes `QueryValue::Null`). With aggregates,
`aggregate_projected_rows` (`:7816`) groups rows by the non-aggregate projections (the group key is
the vector of those `QueryValue`s) and folds each group through an accumulator: `CountAll`,
`CountExpression`, `Sum`, `Avg`, or `Collect`.

Both run entirely in memory over rows already produced. Neither touches storage, which is why
neither needs the read epoch.

== Assembling and paging the result

The terminal step is `finish_projected_rows` (`src/shard/query.rs:4353`): it applies `DISTINCT`
deduplication, then `ORDER BY`, then the `SKIP` / `LIMIT` window, enforcing the configured maximum
result size along the way, and returns a `QueryResultSet`.

The result types were introduced in Chapter 1; here they are the actual output:

#srcblock("src/query/algebra.rs:549-614 (abridged)")[```rust
pub enum QueryValue {
    Null, VertexId(VertexId), Count(u64), Bool(bool),
    Float(QueryFloat), Property(VertexPropertyValue), List(Vec<QueryValue>),
}
pub struct QueryRow { pub values: Vec<QueryValue> }
pub struct QueryResultSet {
    pub columns: Vec<QueryColumn>,
    pub rows: Vec<QueryRow>,
    pub read_epoch: Option<StorageSequence>,
    pub storage_sequence: Option<StorageSequence>,
}
```]

Those last two fields are how the shard reports back which snapshot answered — the values
`result_read_epoch` and `result_storage_sequence` in Section 2.3 insist on, and the seed of the
client's next bookmark. They are stamped at the end of
`execute_parsed_opencypher_rows_inner` (`src/shard/query.rs:542-546`) with
`result.with_read_epoch(read_epoch)` and `with_storage_sequence(sequence)`.

*Paging is buffered, not re-executed.* This is worth being precise about, because the obvious
design — re-run the query at a pinned epoch for each page — is not what happens. On the first
page of a read, `execute_prepared_page` clears the epoch exactly like `execute_rows` does, executes
the *complete* query under one snapshot, and hands the whole result set to `start_server_cursor`:

#srcblock("src/client/service.rs:940-967 (abridged)")[```rust
if action == QueryTransportAction::Read {
    // The client-visible topology watermark is not a
    // storage snapshot selector. Shard execution creates
    // one SlateDB DbSnapshot for this complete result.
    context.read_epoch = None;
    context.max_result_bytes = Some(self.inner.config.max_cursor_buffer_bytes);
    self.refresh_strong_read(&request, action).await?;
    let result = /* execute the whole query */;
    let read_epoch = result_read_epoch(&result, action)?;
    let storage_sequence = result_storage_sequence(&result, action)?;
    let bookmark = self.bookmark_after(&request, action, storage_sequence).await?;
    return self
        .start_server_cursor(session, &request, result, read_epoch, bookmark, page_size)
        .await;
}
```]

Every subsequent `PULL` goes to `continue_server_cursor` (`:1171`), which finds the buffered cursor
by token, re-checks that it belongs to this session, target, query text, and parameters, and slices
off the next page. So a cursor is trivially snapshot-stable: there is only ever one execution, and
the pages are slices of its output. The costs are equally explicit — the whole result is resident
in memory, bounded by `max_cursor_buffer_bytes` (checked against the running total in
`start_server_cursor`, `:1108-1119`), and cursors expire. That is also why `execute_page` only
refuses a client `read_epoch` when there is *no* cursor: a continuation is not choosing a snapshot,
it is reading from one that was already chosen.

The Bolt server drives paging with `PULL`, turning each `QueryValue` into the matching Bolt wire
value on the way out; the HTTPS server drives the same cursor for its NDJSON streaming response.

== Traversal: multi-hop reads

A bounded variable-length pattern such as `MATCH (a)-[:RELATES*1..3]->(b)` cannot be answered by
one adjacency scan. These reads take a separate path built around a sparse-matrix kernel, and it
is here — and only here — that the index generation and the WAL-tail overlay of Section 2.8 are
used.

A row query is eligible for the traversal path only when it is a single anchored edge pattern with
a hop range and a shape the kernel can answer. `graph_kernel_row_query_request`
(`src/shard/query.rs:6410`) recognizes that shape and `try_execute_graph_kernel_row_query` (`:690`)
dispatches to one of the reachability functions; if the shape does not match, it returns `None`
and the generic path of Section 2.7 takes over.

#custom-box(title: [Term — Reachability], icon: "info")[
  The set of vertices you can reach from a starting vertex by following a given edge type between
  a minimum and maximum number of hops. It is computed by breadth-first expansion: start with the
  source, take all neighbours, take their neighbours, and so on, up to the hop limit, collecting
  everything seen at or after the minimum hop count.
]

`reachable_vertices_in_hop_range_at` (`src/shard/query.rs:4843-4881`) is the representative
function, and it is a two-way choice, not a three-way one:

#srcblock("src/shard/query.rs:4852-4880 (abridged)")[```rust
let (min_hops, max_hops) = self.validate_reachable_hop_request(
    "cypher_match_reachable", cell_id, edge_type, hop_range,
)?;
if let Some(result) = self
    .reachable_vertices_with_compiled_graph_kernel(
        cell_id, edge_type, src, hop_range, read_epoch, budget,
    )
    .await?
{
    return Ok(result);
}
let traversal = self
    .reachable_from_storage_frontier(
        cell_id, edge_type, src, (min_hops, max_hops), read_epoch, budget,
    )
    .await?;
self.operation_metrics.query_rust_sparse_fallbacks.fetch_add(1, Ordering::Relaxed);
Ok((traversal.vertices, traversal.edge_visits))
```]

Either the compiled path can serve this read, or it cannot — and the fallback is a plain
frontier BFS straight over the snapshot, counted in a metric so you can see how often the
acceleration is missing.

*The accelerated path.* `reachable_vertices_with_compiled_graph_kernel` (`:5008-5060`) finds the
newest artifact at or before the read epoch with `latest_matrix_artifact`
(`src/engine/artifact_build.rs:578`, which itself is now a thin adapter over
`discover_graph_index`), then asks `compiled_graphblas_query_snapshot` (`:5249-5335`) for a
compiled matrix and, if needed, an overlay. That function is where all the conditions live:

+ Look up the generation for this `(cell, edge type, base epoch)` with
  `graph_index_generation_at`; if there is none, fall through to the plain compiled-matrix lookup.
+ Open a snapshot and bail out immediately if `storage_snapshot.seq() != read_epoch` — under
  `scope_snapshot` this returns the query's own pinned snapshot, so a mismatch means something is
  out of step and the accelerated path declines.
+ Hydrate the compiled matrix with `cached_graphblas_matrix` (`src/engine/matrix_cache.rs:47`),
  which on a miss loads the CSC payload through `graphblas_csc` (`src/engine/matrix_cache.rs:211`),
  verifies its checksum against the manifest, and compiles it.
+ If `generation.base_sequence >= read_epoch`, return the matrix with no overlay.
+ Otherwise call `topology_tail_since`. `Complete(overlay)` returns the matrix plus the overlay;
  `Unavailable` returns `None`, which sends the caller to the storage-frontier fallback.

There is one more wrinkle worth knowing, because it explains a retry loop that otherwise looks
odd. If the overlay grows past `max_query_scan_edges` the tail fails with
`AdmissionRejected { operation: "graph_index_wal_affected_edges", .. }`; rather than giving up,
the code calls `discover_graph_index` once to see whether the indexer has since published a
*newer* generation — one whose base sequence is closer to the read — and retries with it. The loop
runs at most twice, which the code asserts with an `unreachable!` on the third pass. A lagging
indexer therefore degrades to "try the fresher index, then fall back", never to a hang.

#custom-box(title: [Term — Sparse kernel], icon: "info")[
  The component that does the breadth-first expansion. "Sparse" because a graph's adjacency,
  written as a matrix, is almost all zeros, so it is stored and traversed as lists of neighbours
  rather than a dense grid. TurboLay has two backends: a pure-Rust one and one that calls the
  SuiteSparse GraphBLAS C library when the `graphblas` feature is on.
]

#srcblock("src/sparse_kernel.rs:14-18")[```rust
pub enum SparseKernelBackend {
    RustSparse,
    SuiteSparseGraphBlas,
}
```]

The compiled path always reports `SuiteSparseGraphBlas`; `expand_range_with_overlay` says so
explicitly when it builds its `SparseTraversal` (`src/shard/topology_tail.rs:192-196`). The
fallback reports `RustSparse`.

*The fallback path.* `reachable_from_storage_frontier` (`src/shard/query.rs:4883-4934`) is a plain
frontier BFS with no precomputation at all. It opens a snapshot, and for each hop asks
`out_neighbors_in_storage_snapshot` (`:123`) for each frontier vertex — a prefix scan of that
vertex's `e/out/` keys, plus its segments minus its tombstones, all read from the snapshot — while
enforcing three separate limits: `max_query_scan_edges` on edges visited,
`max_query_intermediate_rows` on the frontier size, and `max_query_result_vertices` on the
accumulated result. It is slower than the matrix path by a wide margin, and it is always correct,
which is the right way round.

#figure(
  diagram(
    crossing-fill: reader-colors.paper,
    node-stroke: 0.55pt + reader-colors.border,
    edge-stroke: reader-colors.muted,
    spacing: (17mm, 15mm),
    node((0, 1), text(fill: reader-colors.text, size: 8pt, hyphenate: false)[reachability request\ pinned at #emph[S]],
      fill: reader-colors.info_soft, stroke: 0.55pt + reader-colors.info, width: 3.4cm),
    edge((0, 1), (1, 1), "->", stroke: reader-colors.muted),
    node((1, 1), text(fill: reader-colors.text, size: 8pt, hyphenate: false)[index generation\ at #emph[B] #sym.lt.eq #emph[S]?],
      fill: reader-colors.warn_soft, stroke: 0.55pt + reader-colors.warn, width: 3.4cm),
    edge((1, 1), (1, 0), "->", text(fill: reader-colors.muted, size: 7.5pt)[#emph[B] #sym.eq.not #emph[S]],
      stroke: reader-colors.muted, label-side: left),
    node((1, 0), text(fill: reader-colors.text, size: 8pt, hyphenate: false)[WAL tail after #emph[B]\ #sym.arrow.r overlay],
      fill: reader-colors.purple_soft, stroke: 0.55pt + reader-colors.purple, width: 3.4cm),
    edge((1, 0), (2, 0), "->", stroke: reader-colors.muted),
    node((2, 0), text(fill: reader-colors.text, size: 8pt, hyphenate: false)[GraphBLAS expansion\ + overlay per hop],
      fill: reader-colors.ok_soft, stroke: 0.55pt + reader-colors.ok, width: 3.6cm),
    edge((1, 1), (2, 1), "->", text(fill: reader-colors.muted, size: 7.5pt)[#emph[B] #sym.eq #emph[S]],
      stroke: reader-colors.muted, label-side: left),
    node((2, 1), text(fill: reader-colors.text, size: 8pt, hyphenate: false)[GraphBLAS expansion,\ no overlay],
      fill: reader-colors.ok_soft, stroke: 0.55pt + reader-colors.ok, width: 3.6cm),
    edge((1, 1), (1, 2), "->", text(fill: reader-colors.bad, size: 7.5pt)[none, or\ tail unavailable],
      stroke: (dash: "dashed", paint: reader-colors.bad), label-side: right),
    node((1, 2), text(fill: reader-colors.text, size: 8pt, hyphenate: false)[`reachable_from_storage_frontier`:\ BFS over snapshot adjacency],
      fill: reader-colors.surface_soft, stroke: 0.55pt + reader-colors.border, width: 4.8cm),
  ),
  caption: [How a multi-hop read picks its engine: with an index generation whose base sequence
    already equals the pinned read #emph[S] the expansion is pure sparse-matrix multiplication;
    with an older generation the WAL tail is turned into an overlay and applied hop by hop; and
    with no usable generation — or a WAL tail that can no longer be read — the dashed path leads
    to a plain breadth-first search over the snapshot's own adjacency keys, which is slower but
    needs nothing precomputed.],
) <fig-read-traversal-choice>

== Read-side caching, in brief

A read consults several caches on the shard, and Chapter 5 covers their internals and their
bounds. The single fact that matters for correctness here is that every cache key which depends
on graph contents embeds a `StorageSequence`, and there are exactly two regimes.

#srcblock("src/lib.rs:144-149")[```rust
pub(crate) struct MatrixCacheKey {
    pub(crate) cell_id: String,
    pub(crate) edge_type: String,
    pub(crate) base_epoch: StorageSequence,
}
```]

*Per-read caches* — the relationship-rows and relationship-property-rows caches — embed the
`read_epoch` itself. A cached entry from one epoch can never be served to a read at a different
epoch, so a write advances the sequence and the next read simply misses; nothing is ever
invalidated, and the LRU ages the dead entry out.

*Matrix caches* — the hydrated adjacency and the compiled GraphBLAS matrix — key on a
`base_epoch` that *deliberately lags* the read epoch, because that is the generation's
`base_sequence`. A write does not invalidate them, and it must not: if it did, every write would
force a rebuild of the acceleration structure. The lag is closed at read time by the WAL-tail
overlay of Section 2.8.

The parsed-query cache from Section 2.5 is the only content cache with no sequence in its key,
because a parse result does not depend on graph contents at all.

== Recap: the one-sequence invariant

Trace the sequence through the chapter and the read path becomes one idea.

+ *The client cannot name a snapshot.* Both front doors refuse a `read_epoch`
  (`src/client/http.rs:469-475`, `src/client/service.rs:781-787`). What a client may supply is a
  *floor* (a bookmark) or a *level* (`Causal` / `Strong`).
+ *The service clears the epoch and enforces the floor.* `context.read_epoch = None`
  (`service.rs:832`), after `validate_bookmark` has waited for the cell to reach the bookmark
  (`:1367`, `:719`) and `refresh_strong_read` has refreshed the reader if the request asked for it
  (`:1380`).
+ *The shard pins it.* One SlateDB snapshot — `reader_snapshot()` for a strong read,
  `snapshot()` otherwise — `read_epoch = snapshot.seq()`, then
  `with_validated_storage_read_epoch(read_epoch, read_epoch)` and `scope_snapshot`
  (`query.rs:461-476`). The same number is the epoch and the storage sequence, because there is
  only one sequence type in the system (`src/lib.rs:138-140`).
+ *Every canonical read inherits it.* Adjacency scans, segments, and tombstones are all read
  through the pinned snapshot, and `EdgeRecord` carries no sequence of its own because visibility
  is a property of the snapshot, not of the row.
+ *The one lagging structure is repaired, not trusted.* An index generation built at #emph[B] is
  reconciled to the read at #emph[S] by `topology_tail_since`
  (`src/shard/topology_tail.rs:28-96`), which uses the WAL only to find changed pairs and the
  snapshot to decide their truth — or returns `Unavailable` and steps aside.
+ *Every content-dependent cache key embeds a sequence*, either the read epoch or the
  generation's base sequence.
+ *The sequence leaves again as a bookmark*, so the client's next read has a floor
  (`bookmark_after`, `service.rs:1405-1418`).

The result is that a read presents one consistent snapshot across adjacency keys, segments, index
generations, traversal kernels, and caches, no matter what writers are doing at the same time —
and that "as of when?" has exactly one answer, all the way down.

The next chapter turns to those writers, and to the question this chapter has quietly assumed away:
how a sequence advances in the first place, and what guarantees only one process may advance it.
