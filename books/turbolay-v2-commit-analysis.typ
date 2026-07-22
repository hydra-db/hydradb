#set document(
  title: "Turbolay V2 commit analysis",
  author: "Code review working summary",
)
#set page(paper: "a4", margin: (x: 1.7cm, y: 1.55cm))
#set text(size: 9.5pt, font: "Avenir Next")
#set par(leading: 0.58em, justify: true)
#show heading.where(level: 1): set text(size: 17pt, weight: "bold")
#show heading.where(level: 2): set text(size: 12pt, weight: "bold")
#show heading.where(level: 3): set text(size: 10.5pt, weight: "bold")

= Turbolay V2 commit analysis

*Review focus:* graph read/write paths, storage consistency, ingestion semantics, and changes that deserve code review. Helm and delivery changes are intentionally treated as context only.

== Executive summary

The key difference from `Turbolay-V1` is a change in ownership of correctness:

- V1 had a graph-level distributed controller/lease model and reconstructed historical graph state by replaying metadata and topology changes.
- V2 makes SlateDB the storage coordination boundary. It uses SlateDB writer fencing, durable transactions, storage snapshots, and current-state records. Graph topology still has epoch-tagged deltas, but those deltas primarily support adjacency reads and matrix/artifact refresh rather than a general historical graph view.

In practical terms, V2 is moving from *“the graph epoch is the read snapshot”* to *“a SlateDB snapshot is the read snapshot; the graph epoch is a causal/topology watermark.”* That is the central mental-model change to validate.

The other major code change is an ingestion/write-path consolidation. OpenCypher `UNWIND` batches are resolved into explicit batch operations, routed to one shard transaction, coalesced, made idempotent, and committed with current records, indexes, topology deltas, outbox entries, and artifact-dirty markers.

== Compared range and commit map

The comparison is `refs/heads/Turbolay-V1...refs/heads/Turbolay-V2`, from merge base `e2f1977` through V2 `2374a76`. The range is large: 133 changed files, approximately 12.7k insertions and 28.1k deletions. Most of the deletion volume is the old controller/runtime and test migration, not a small feature patch.

#table(
  columns: (1.25fr, 2.2fr, 4.55fr),
  inset: 5pt,
  stroke: .5pt + luma(75%),
  [*Commit*], [*Theme*], [*Why it matters to the code review*],
  [`ea7ec2c`], [Batch ingestion], [Adds vertex metadata batches and labeled relationship import operations from `UNWIND`; introduces the batch boundary that later commits refine.],
  [`d96bd1b`], [Cypher MERGE semantics], [Makes batch upserts behave like Cypher `MERGE`/`SET` rather than an accidental replace or duplicate insert.],
  [`348191a`], [CREATE semantics], [Preserves multigraph behavior: repeated `CREATE` operations allocate distinct relationship identities while sharing structural adjacency.],
  [`4872eac`], [Relationship MERGE], [Adds idempotent relationship merge batches and makes the relationship identity/property-index contract reviewable.],
  [`d01e32e`], [Unified identity], [Converges relationship `CREATE` and `MERGE` on the same relationship record/index model.],
  [`215bed9`], [SlateDB-native runtime], [Removes the graph controller/supernode runtime and makes local shards, SlateDB fencing, transactions, snapshots, and current-state records the primary model.],
  [`e875387`], [Bounded paged reads], [Adds bounded page execution and aligns client bookmarks with the new snapshot model; this is the highest-risk read-path area.],
  [`494ea76`, `74571e0`, `8f5245f`], [Follow-ups], [Feature-gates result sizing for supported clients and exposes the snapshot epoch in the Bolt smoke path.],
)

== New mental model

The V2 path is best understood as two related timelines:

#table(
  columns: (1.5fr, 3fr, 3.5fr),
  inset: 5pt,
  stroke: .5pt + luma(75%),
  [*Timeline*], [*What advances it*], [*What it is used for*],
  [Storage sequence], [SlateDB transaction commit / snapshot sequence], [Repeatable reads, writer fencing, remote durability, and the actual version seen by a read.],
  [Topology sequence], [Structural edge/vertex mutation], [Graph-facing watermark, topology delta selection, adjacency overlays, and artifact refresh.],
  [Client bookmark], [A durable topology watermark returned by the service], [Causal readiness: `ensure_bookmark` waits until the backend is at least as advanced as the bookmark. It does not select an old storage snapshot.],
)

The write/read flow is therefore:

#box(
  width: 100%,
  fill: luma(97%),
  inset: 8pt,
  radius: 3pt,
  [
  *Write:* OpenCypher parser → `QueryBatchOperation` → routed shard → serializable SlateDB transaction → current records/indexes + topology delta/outbox + dirty marker.

  *Read:* client request/bookmark → shard query → one SlateDB `DbSnapshot` for a complete result → query operator or bounded fast path → materialized rows/server cursor.
  ],
)

The important consequence is that a function named `_at(epoch)` should not automatically be read as a historical database read. In V2, the epoch can be the topology watermark used to overlay deltas on a current/artifact representation. The storage snapshot is what makes ordinary metadata and index reads consistent.

== Write path: changes to review

=== 1. Batch ingestion is now a first-class operation

`src/query/opencypher.rs` recognizes `UNWIND $rows AS row MERGE ...` and relationship import/create/merge patterns. `src/client/service.rs` resolves the rows into typed `QueryBatchVertex` and `QueryBatchRelationship` values. `src/query/coordination.rs` then calls shard batch methods instead of executing each row as an independent request.

The shard methods in `src/shard/write.rs` (roughly lines 81–229) establish the transaction boundary. They validate the cell, acquire the write permit/lane and cell lock, coalesce input, retry serializable conflicts, and commit one bounded batch. This is a material correctness and throughput improvement: the batch has one conflict/idempotency boundary and does not expose partially applied rows.

Review questions:

- Is the promised batch atomicity documented at the client API boundary, including failure/retry behavior?
- Are batch size limits applied before all potentially expensive row resolution and again at the shard boundary?
- Do retry metrics and idempotency responses distinguish a replay from a newly committed batch?

=== 2. Vertex metadata batches coalesce duplicate identities

`coalesce_vertex_metadata_updates` in `src/shard/write.rs:5187–5212` groups rows by vertex ID, unions labels, and combines properties. The same property with different values is rejected as a conflict.

That is a deliberate semantic choice, not an implementation detail. A repeated vertex in one `UNWIND` input is no longer “last row wins.” Review or add tests for duplicate IDs with identical values, disjoint properties, repeated labels, and conflicting values. The user-facing contract should say whether conflicting rows are rejected, because this is observable Cypher behavior.

=== 3. Relationship CREATE and MERGE now share one identity model

The common transaction path is `import_relationships_batch_txn_locked` in `src/shard/write.rs:1381–1600`.

- `CREATE` (`create_always`) allocates a fresh relationship ID for every input relationship. This preserves parallel relationships in a multigraph.
- `MERGE` (`update_existing_metadata`) treats the relationship `id` property as the identity. The resolver in `src/query/coordination.rs` supplies it as an integer, and the shard looks it up through the relationship-property index, restricted to the same source and destination (`src/shard/write.rs:1461–1507`, `5502–5558`).
- An existing match updates metadata through `merge_edge_metadata`; a missing match receives a fresh internal relationship ID.
- Existing relationship records, relationship-ID pointers, property indexes, relationship counts, structural adjacency, deltas, and outbox records are updated in the same serializable transaction.

This is the core write-path payoff of commits `348191a`, `4872eac`, and `d01e32e`: structural adjacency and relationship identity are separate. Several relationship records may sit on one `(src, dst)` structural edge; deleting the last relationship removes the structural edge (`src/shard/write.rs:2352–2418`).

Review questions:

- Is `id` explicitly immutable in the parser and API? A MERGE identity changing via `SET` must not silently create a second relationship.
- If the property index contains multiple matching records, is updating all matches the intended Cypher behavior? The current implementation resolves all matching IDs.
- `coalesce_relationship_imports` groups non-CREATE input by `relationship_id` alone (`src/shard/write.rs:5284–5305`), while lookup identity includes edge type and endpoint pair. Two MERGE rows with the same external ID but different endpoints therefore conflict before endpoint-scoped lookup. Confirm whether the external ID is cell-global or endpoint-scoped, and test duplicate rows with different mutable properties.
- Are idempotency keys and relationship IDs bound strongly enough that a replay with a different endpoint, edge type, or payload cannot mutate a different record?
- Are property-index deletion and insertion symmetric for metadata updates and relationship deletes?

=== 4. Structural adjacency has two write forms

Normal edge mutations write canonical adjacency records plus scoped delta/outbox entries. Trusted bulk ingestion can write outbound-only adjacency segments (`src/shard/write.rs:3717–3765`). Structural deletion writes a minus delta, degrees, canonical edge/index cleanup, and artifact dirty/generation markers (`src/shard/write.rs:5583–5737`).

This split is likely intentional for bulk loading, but it is a high-value invariant to review. The segment representation is constrained to outbound-only policy, while normal reads and deletes also understand canonical edges. Add parity tests for segment-only data followed by delete, reinsert, duplicate import, artifact refresh, and reopen-from-object-store.

=== 5. Metadata and topology now have different invalidation costs

Vertex/edge metadata updates write current metadata and metadata indexes but do not dirty adjacency. Structural mutations write topology deltas and advance `adjacency_generation`/`matrix_dirty`. This is a good separation: a property-only update should not rebuild a graph matrix. It also means every query operator must read metadata from the same storage snapshot as the topology view it combines with.

== Read path: changes to review

=== 1. Ordinary complete reads pin a SlateDB snapshot

`execute_parsed_opencypher_rows` in `src/shard/query.rs:424–475` rejects unvalidated historical graph epochs. For a current read, it obtains `self.db.snapshot()`, reads `last_epoch` from that snapshot, marks the context with `with_validated_storage_read_epoch`, and scopes the inner query through `GraphStore::scope_snapshot`.

`GraphStore` in `src/core/state.rs:69–155` routes `get` and `scan_prefix` through the task-local active snapshot. This is subtle but central: lower-level helpers do not receive a snapshot argument; they inherit it from async task scope. The reader variant relies on its checkpoint/manifest because this SlateDB revision does not expose a `DbSnapshot` handle for `DbReader`.

=== 2. Client bookmarks are causal watermarks, not historical snapshots

`ClientQueryService::ensure_bookmark` (`src/client/service.rs:695–712`) verifies that the current graph epoch has reached the bookmark. For a client read, the service intentionally clears `context.read_epoch` and relies on shard execution to create one current SlateDB snapshot (`src/client/service.rs:908–925`).

The server cursor then materializes the complete result and stores remaining rows in memory (`src/client/service.rs:1063–1131`). Continuing a cursor is stable because it consumes those materialized rows; the cursor is not a live database cursor and does not retain a storage snapshot.

=== 3. Paged fast paths appear to bypass the snapshot scope

This is the most important review finding. `execute_opencypher_rows_page` tries the graph-kernel and streaming page paths before falling back to `execute_parsed_opencypher_rows` (`src/shard/query.rs:265–344`). The two fast paths call `query_read_epoch` directly (`4472–4497` and `4604–4638`). The streaming path then calls `out_neighbors_window_at` (`4642–4651`), whose lower-level reads use the ordinary `GraphStore` path, not the task-local snapshot created by the fallback.

As written, a normal paged query can therefore observe a different storage version from the complete-read path if a write occurs while page materialization is in progress. The graph-kernel path also needs the same proof because it can combine artifacts, deltas, and current metadata under the epoch selected before the fallback would have pinned storage.

Suggested review action: acquire and scope the `DbSnapshot` around *all* page execution paths, or make the fast paths accept and consistently use an explicit snapshot handle. Add a concurrent writer/read test that forces a page to span a commit and verifies that the page is internally consistent.

There is a second pagination boundary outside the client service. `QueryResultPage` does not carry a storage snapshot, and direct `QueryCellClient`, TCP, distributed, and batch-page callers may execute page 2 as a fresh query. The client service is safer because it materializes the full result into a server cursor (`src/client/service.rs:1063–1131`), but lower-level page APIs can skip or duplicate rows after an intervening mutation. `RoutedGraphCluster::execute_batch` reports a current epoch but does not obviously scope one matching `DbSnapshot` around batch neighbor reads (`src/query/coordination.rs:3031–3090`).

=== 4. Direct paged historical reads may silently become current reads

The complete path explicitly rejects `context.read_epoch` when it is not a validated storage read epoch (`src/shard/query.rs:430–436`). `execute_opencypher_rows_page` has no equivalent check before trying its fast paths (`265–321`). `query_read_epoch` returns a validated epoch when present, otherwise it calls `current_epoch` (`1803–1808`), so an unvalidated historical epoch can be ignored by a fast path and return current data. A query that misses both fast-path patterns falls through and is rejected, creating inconsistent behavior based only on query shape.

Suggested review action: reject unvalidated `context.read_epoch` at the public paged entry point, before parser/operator dispatch, and add direct shard tests for both graph-kernel and streaming page shapes.

=== 5. Artifact reads need a consistency proof

The matrix/artifact layer is no longer a controller-managed graph snapshot. It uses per-edge-type dirty markers, generations, current-state image builds, and topology delta overlays. Review `src/engine/artifact_refresh.rs:107–261` and `src/shard/query.rs:5147–5277` for the invariant that an artifact build either sees a stable storage snapshot or aborts/retries on a concurrent topology generation change. Metadata-only epochs should not trigger a matrix rebuild, but topology changes must not be omitted from the overlay or dirty-marker clear.

=== 6. Remote bookmark and reader freshness contracts need validation

`ensure_bookmark` asks `QueryCellClient::current_graph_epoch` for proof of durability (`src/client/service.rs:695–712`). The trait default can return `None`, and the TCP client path does not obviously provide an epoch RPC. A bookmark returned by a remote client may therefore fail on the next request with “backend cannot prove bookmark durability.” Add a remote round-trip test or change the validation contract.

The `GraphStore::Reader` path is checkpoint/manifest-pinned and does not expose a per-query `DbSnapshot` (`src/core/state.rs:136–145`). Review how long-lived read-only routed nodes refresh their checkpoint; without a refresh/reopen path they may serve stale records and stale watermarks.

== Key bugs and review risks

#table(
  columns: (0.85fr, 1.1fr, 3.1fr, 3.1fr),
  inset: 5pt,
  stroke: .5pt + luma(75%),
  [*Priority*], [*Area*], [*Finding*], [*Review action*],
  [P1], [Paged reads], [Fast page paths run before snapshot-scoped fallback and can read live storage while a write commits.], [Pin/scope one snapshot around every page operator; add concurrent consistency test.],
  [P1], [Epoch API], [Direct paged calls can ignore an unvalidated historical `read_epoch` on fast paths, while fallback calls reject it.], [Reject at `execute_opencypher_rows_page` entry; test both fast paths.],
  [P1/P2], [MERGE identity], [Relationship MERGE relies on integer property `id`, while batch coalescing is keyed only by relationship ID even though lookup is endpoint-scoped.], [Define identity scope; test same ID across endpoints and duplicate mutable-property rows.],
  [P2], [Batch semantics], [Duplicate vertex IDs are coalesced; conflicting property values fail the whole batch.], [Document contract and test conflict/merge cases.],
  [P2], [Direct pagination], [Lower-level page/batch APIs may re-execute later pages against newer state; only client server cursors materialize the full result.], [Test mutate-between-pages behavior or carry a snapshot/cursor contract.],
  [P2], [Adjacency forms], [Canonical edges and outbound-only segments have different lifecycle paths.], [Test delete/reinsert/refresh/reopen parity across both representations.],
  [P2], [Artifact refresh], [Artifact build/dirty-marker clearing must not race a topology generation change.], [Verify snapshot/generation fencing and retry behavior under concurrent writes.],
  [P2], [Remote/read-only clients], [Bookmark proof may be unavailable remotely; long-lived readers may remain checkpoint-stale.], [Add epoch round-trip tests and define reader refresh semantics.],
)

== Review checklist

#enum(
  [Read snapshot is acquired before graph-kernel and streaming page dispatch.],
  [Unvalidated historical epochs are rejected consistently for complete, paged, direct-shard, and client calls.],
  [Bookmark checks are understood as causal readiness, not repeatable historical reads.],
  [A complete result and its server cursor cannot mix storage versions.],
  [Direct page and batch APIs define what happens when data changes between page requests.],
  [Vertex batch coalescing, conflicting values, labels, and idempotency are covered.],
  [CREATE always allocates distinct relationship identities when the structural edge already exists.],
  [MERGE matches only the intended immutable identity and updates the intended record set.],
  [Relationship property indexes, relationship counts, structural edges, degrees, deltas, and outbox entries remain transactionally aligned.],
  [Segment-only and canonical adjacency paths have equivalent delete/reopen/artifact behavior.],
  [Metadata-only writes leave adjacency generation and matrix refresh state unchanged.],
  [Remote bookmark round-trips and long-lived read-only refresh behavior are covered.],
)

== Validation performed

`cargo test --locked --all-targets` passes locally: 91 library tests plus example targets. The OpenCypher-enabled all-target suite also passes: 171 library tests plus example targets. On this macOS setup, the installed parser package needs its include directory supplied to the old bindgen build with `PKG_CONFIG_PATH=/opt/homebrew/opt/libcypher-parser/lib/pkgconfig` and `BINDGEN_EXTRA_CLANG_ARGS=-I/opt/homebrew/opt/libcypher-parser/include`.

The full commit inventory, evidence notes, and suggested test matrix are in `turbolay-v2-commit-analysis/` beside this file.

== Bottom line

V2 is not primarily “V1 with a new deployment.” It is a storage/runtime rewrite with a new read contract and a more explicit batch write contract. The highest-value review work is to prove that the new snapshot model survives the optimized paged read paths, then to lock down the new batch and relationship identity semantics. If those invariants hold, the V2 architecture is simpler operationally and has a cleaner separation between durable storage coordination, current graph records, topology overlays, and derived adjacency artifacts.
