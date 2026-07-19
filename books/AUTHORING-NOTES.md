# Authoring notes — the two new TurboLay books

These two books are ports of the older `book/` editions, corrected against the
**current** source (HEAD `8e2b6e4`, branch `Turbolay-V3`). Every chapter agent
must anchor to the facts and terminology below so the two books stay mutually
consistent.

## The one refactor that changed almost everything

Since the old books' pinned commit `b67d457`, a single refactor —
*"Adopt SlateDB-native graph runtime"* (`215bed9`, follow-up `e875387`) — deleted
TurboLay's bespoke coordination layer and re-based it on SlateDB primitives plus a
new async matrix-artifact pipeline. Deleted: `control_plane.rs`, `controller.rs`,
`control_transport.rs`, `control_metadata.rs`, `control_client.rs`, `supernode.rs`,
`bin/graph-controller.rs`. Added: `engine/matrix_cache.rs`, `engine/artifact_refresh.rs`,
`engine/artifact_gc.rs`. `engine.rs` shrank from 2777 to ~200 core lines.

## The 7 canonical corrections (apply everywhere)

1. **Two sequences, not one "epoch."** The type `GraphEpoch` is GONE (zero
   occurrences in `src/`). It split into:
   - `StorageSequence` (`src/lib.rs:145`) — SlateDB's snapshot sequence; this is the
     REAL read-consistency / MVCC mechanism.
   - `TopologySequence` (`src/lib.rs:150`) — a monotonic *topology-change cursor* that
     feeds asynchronous matrix builds. Its own doc comment says it is "not a second
     storage MVCC system; canonical record visibility belongs to SlateDB snapshots."
   The word "epoch" survives colloquially and in key names (`meta/last_epoch`,
   `meta/mutation_log_epoch`) and struct fields (`EdgeRecord.epoch: TopologySequence`),
   so keep the word — but stop equating it with a distinct MVCC type.

2. **Reads are pinned by a SlateDB snapshot; clients carry bookmarks, not epochs.**
   The service no longer pins `current_graph_epoch`. A client-supplied `read_epoch`
   is *rejected* ("historical graph epochs are not client query snapshots; use a
   bookmark for causal reads", `src/client/service.rs:753/846`). The shard pins by
   opening `self.db.snapshot()`, reading `keys::last_epoch(cell_id)` from *inside*
   that snapshot, and binding them via `with_validated_storage_read_epoch(read_epoch,
   snapshot.seq())`, scoping the whole query under that snapshot
   (`src/shard/query.rs:288-315`). The epoch used is reported back out for the
   bookmark (`result_read_epoch`, `bookmark_after`).

3. **No control plane, no leases, no write fences.** `GraphControlPlane`,
   `graph-controller` binary, `ShardLease`, `GraphWriteFence`, lease tokens, the
   `write_fence` key builder, `StaleShardLease`/`WriteRequiresLease` errors — ALL
   deleted. Single-writer now rests on three things: (a) the sole SlateDB writer
   handle — `ensure_write_authority` for a `Writer` calls `self.db.writer()`
   (`src/shard/lifecycle.rs:454`), which fails unless this process holds the DB's
   only writer; (b) the object-store **cell write lock** (owner-token + TTL,
   `acquire_cell_write_lock`/`release_cell_write_lock`, record `graph-cell-write-lock-v1`
   at object path `__slatedb_graph_kernel/write_locks/<db>/<cell>`); (c) the
   serializable-snapshot SlateDB transaction. `GraphWriteAuthority` is now just
   `{ ReadOnly, Writer }` (`src/core/state.rs:242`). `validate_write_fence_txn`
   (`src/shard/lifecycle.rs:458`) only checks cell-drop markers + writer authority —
   it is a drop-guard, NOT a token fence.

4. **The cluster is symmetric.** Every process is a `graph-node` (single binary;
   `graph-controller` is gone) holding a `RoutedGraphCluster`
   (`src/engine.rs:86-93`: `scope, local_node_id, placement, shards, writable,
   maintenance_metrics` — no `leases`, no `revoked_cells`). Cell ownership is a
   static `ShardPlacement` (`src/engine/cluster.rs:101`) — `fixed` or
   `rendezvous`-hashed, decided at startup. No heartbeats, no failover loop, no
   watermark-advancing controller.

5. **Supernodes are gone entirely.** No supernode subsystem, cache, posting chunks,
   reachability cache, or supernode pinning anywhere in `src/`. Traversal
   acceleration is **matrix artifacts only**. Remove all supernode / posting-chunk /
   reachability-cache material.

6. **New acceleration machinery to document.** Three new files replace the old story:
   - `engine/matrix_cache.rs` — read-through hydration. On miss, take a hydration
     permit / `matrix_compilation_gate`, load the artifact (tiles or the newer
     GraphBLAS CSC on-disk format), insert sized + pinned. `cached_matrix_adjacency`
     / `cached_graphblas_matrix`.
   - `engine/artifact_refresh.rs` — a background Tokio job
     (`start_matrix_artifact_refresh_job`) that scans "dirty" matrix edge-type markers
     and rebuilds a matrix artifact when *due* per `MatrixArtifactRefreshPolicy
     { interval, max_dirty_age, min_epoch_lag, tile_size, max_edge_types_per_cycle }`.
     This is the concrete "rollup" — the word "rollup" appears nowhere in `src/`;
     the builder is `build_adjacency_image` (`engine/artifact_build.rs`).
   - `engine/artifact_gc.rs` — deletes artifact keys with `base_epoch < keep_epoch`
     and prunes the three matrix caches via `retain` (no safe-epoch gate; trusts
     caller's epoch).
   **Two cache-correctness regimes:** per-read caches (parsed query, relationship-rows,
   source/property rows) are keyed by `read_epoch` → a write advances the epoch, next
   read misses, LRU ages the old entry out (the old "no invalidation" story holds
   HERE). Matrix caches are keyed by a `base_epoch` that *deliberately lags*
   `read_epoch` (`MatrixCacheKey { cell_id, edge_type, base_epoch }`,
   `get_latest_by base_epoch <= read_epoch`), with the delta log overlaid at read
   time — so a write does NOT invalidate them; GC prunes them via `retain`.
   Default note: hydrated-adjacency cache is OFF by default
   (`max_matrix_adjacencies = 0`); only the compiled GraphBLAS matrix is cached
   (64 entries / 128 MiB). `GraphCacheKind` has 6 variants:
   `MatrixArtifact, MatrixAdjacency, GraphBlas, ParsedRowQuery, RelationshipRows,
   RelationshipPropertyRows` (`src/core/metrics.rs:49`). Shard cache fields number
   seven (`src/core/state.rs:50-66`), including the new `source_relationship_rows_cache`.

7. **GC safety machinery removed; relationships hard-deleted.** No read leases, no
   retention policy, no computed safe-epoch — GC trusts the caller's epoch. The
   watermark → `SnapshotExpired` contract SURVIVES (`delete_deltas_through_matrix`
   writes `delta_gc_watermark` first, then deletes; reads below it are refused).
   `delete_deltas_through_rollup` was renamed `delete_deltas_through_matrix`
   (`src/shard/maintenance.rs:14`). Relationships are now **physically deleted**, not
   tombstoned — `delete_relationships_for_structural_edge_txn` (`src/shard/write.rs`)
   `txn.delete`s the relationship + id + property indexes + count. The structural
   *edge* is still soft-deleted (a `Minus` delta at a new epoch); the relationships
   riding on it are hard-removed. Don't over-generalize "everything is a soft delete."

## Terminology rules (keep both books consistent)

- Product name is **TurboLay** (capital L) in prose/titles; the crate/repo stays
  `slatedb-graph-kernel` in code. The old books wrote "turbolay" lowercase — new
  books use "TurboLay".
- Use "cell" for the isolation/ownership unit (never "namespace" unless the API does).
- "epoch" is fine colloquially; the *type* is `TopologySequence` (topology) or
  `StorageSequence` (storage snapshot). Never write `GraphEpoch`.
- Say "cell write lock" / "distributed write lock", never "write fence" as a
  durable record. `validate_write_fence_txn` is a drop-guard + authority check.
- Traversal acceleration = "matrix artifacts", never "supernodes".
- Distributed queries = bounded scatter/gather (placement-aware legs + concurrent
  execution + coordinator merge). Not a global planner or global snapshot.

## Roaring policy (per the book owner's decision)

The Roaring matrix-row work is NOT on this branch: `MatrixAdjacency` is
`BTreeMap<VertexId, BTreeSet<VertexId>>` (`src/lib.rs:152`), there is no `Roaring`
symbol in `src/`, and `roaring` is not a Cargo dependency. **Keep** the Roaring
chapters/paragraphs (`intro-04`, `detail-01`, the `intro-03` Roaring note) but mark
them explicitly as PLANNED / FUTURE work that is not yet in this branch — use a
clearly-labelled callout at the top ("Status: planned, not in the current tree") and
change present-tense "is" claims to future/conditional ("would", "is planned to").
Correct any statement that says the current hydrated type is `RoaringTreemap`; the
current type is `BTreeSet`.

## Citations

Line numbers drift — verify each `file:line` you cite against current source before
writing it. Prefer citing a function/type name plus file over a bare line number.
The old books' line numbers are mostly stale; re-derive them.
