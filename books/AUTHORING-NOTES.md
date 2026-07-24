# Authoring notes — the TurboLay books

These notes are the binding brief for every chapter agent working in `books/`
(`inside`, `conceptual`, `quint`). They are corrected against the **current**
source at HEAD `1f5b06d`, branch `Turbolay-V3-vishal`, after the *graph kernel
resync* merge (`4b4665f`) and after `Turbolay-V3` was merged into the branch
carrying the dynamic-scope work (§10 below). Every `file:line` below was opened
and verified at that HEAD.

> **If this file and a chapter disagree, this file wins — but re-verify anyway.**
> Line numbers drift. Prefer citing a function/type name plus a file over a bare
> line number, and open the file before you write the number.

---

## The merge that invalidated the previous edition

The resync deleted three whole subsystems the older books were built on. Any
prose you inherit that mentions them is wrong, not merely stale:

1. **The two-sequence model is gone.** There is no `TopologySequence` — zero
   occurrences in `src/`. `src/lib.rs:140` declares exactly one sequence type:
   `pub type StorageSequence = u64;` ("SlateDB's sequence number for a committed
   storage snapshot"). There is no separate topology cursor and no second MVCC
   system.
2. **The object-store cell write lock is gone.** `acquire_cell_write_lock`,
   `release_cell_write_lock`, `acquire_distributed_write_lock`, the
   `__slatedb_graph_kernel/write_locks/...` object path and
   `GraphError::CellWriteConflict` are all zero-occurrence. Fencing is now
   SlateDB's own manifest fencing.
3. **The delta / outbox / mutation-log subsystem is gone.** See the dedicated
   section below — this is the single most load-bearing deletion.

Also deleted: `src/engine/artifact_refresh.rs` and its
`start_matrix_artifact_refresh_job` / `MatrixArtifactRefreshPolicy`. Index
building is now **out-of-process**. `src/engine/` today contains exactly:
`artifact_build.rs`, `artifact_gc.rs`, `cluster.rs`, `index_store.rs`,
`matrix_cache.rs`, `scope_directory.rs`, `traversal.rs`, `verify.rs`. The
binaries are `src/bin/graph-node.rs` and `src/bin/graph-indexer.rs`.

---

## The canonical corrections (apply everywhere)

### 1. One sequence: `StorageSequence`. The epoch is `txn.seqnum()`.

`src/lib.rs:140` is the only sequence type. A write does **not** allocate,
read, or increment an epoch key; it reads the epoch straight off the SlateDB
transaction: `let current_epoch = txn.seqnum();` (`src/shard/write.rs:643`, and
~10 further sites in that file).

`MatrixCacheKey { cell_id, edge_type, base_epoch: StorageSequence }`
(`src/lib.rs:144-149`).

Keep the word "epoch" — it survives colloquially and in field names — but it now
means *a SlateDB storage sequence number*, nothing else. **Never write
`TopologySequence` or `GraphEpoch`.** Any field the old books typed
`TopologySequence` is `StorageSequence` today.

### 2. Reads pin a SlateDB snapshot; clients carry bookmarks, not epochs.

A read with no client-supplied epoch opens `self.db.snapshot()` — or
`self.db.reader_snapshot()` when the request wants a refreshed reader
(`src/core/state.rs:318` and `:337`) — takes `read_epoch = snapshot.seq()`, binds
it via `context.with_validated_storage_read_epoch(read_epoch, read_epoch)`, and
scopes the whole query under that snapshot (`src/shard/query.rs:461-476`).

There is **no `keys::last_epoch` read**; that key builder no longer exists. A
client-supplied historical epoch is rejected outright: *"historical graph epochs
are not storage snapshots; execute against a current SlateDB snapshot"*
(`src/shard/query.rs:450-456`) and, at the client edge, *"historical graph epochs
are not client query snapshots; use a bookmark for causal reads"*
(`src/client/service.rs:784`, `:881`).

**Causal vs strong reads are first-class and were absent from the old books.**
`ClientReadConsistency { Causal, Strong }` (`src/client/service.rs:269-272`),
default `Causal` (`:289`), selectable via `with_consistency` (`:328`) and a
`Strong` shortcut (`:334`); the strong path branches at `:1385` and `:1674`.
`inside/02-read-path` owns this mechanism; `inside/00-foundations` defines the
vocabulary.

### 3. Single-writer rests on three tiers — none of them a cell lock.

- **Tier 1, authority.** `ensure_write_authority` (`src/shard/lifecycle.rs:404-418`)
  matches on a **three-variant** `GraphWriteAuthority { ReadOnly, Promotable,
  Writer }` (`src/core/state.rs:472-476`). `ReadOnly` is refused with
  `WriteRequiresWriter`; `Promotable` and `Writer` both fall through to
  `self.db.writer()`. A `Promotable` node becomes a writer lazily via
  `promote_to_writer` (`src/shard/lifecycle.rs:420-434`) →
  `GraphStore::promote_writer` (`src/core/state.rs:204-227`), which opens the DB
  under a gate and caches the handle.
- **Tier 2, SlateDB manifest fencing** — this replaces the old cell write lock.
  `refresh_writer_fence` (`src/core/state.rs:187-203`) calls
  `writer.refresh_manifest()`; on
  `ErrorKind::Closed(CloseReason::Fenced)` it drops the cached writer handle and
  propagates the error. A newer writer fences the old one through SlateDB's own
  manifest, not through an owner-token record in the object store.
- **Tier 3, the serializable-snapshot transaction.** Unchanged and still the
  correctness backstop.

`write_edge` (`src/shard/write.rs:2354-2384`) takes **no lock**: authority check →
write permit → per-cell `writer_lane` mutex → retry loop over `write_edge_txn`.
`validate_write_fence_txn` (`src/shard/lifecycle.rs:436`) is a drop-guard plus
authority check, not a token fence.

**Where the fence is actually refreshed (verified at HEAD).** The summary above
skips a step, and chapter agents have concluded from it that the fence is never
refreshed per-write. It is. `write_edge_txn` calls `acquire_local_write_guard`
(`src/shard/lifecycle.rs:253-263`), which takes the process-local guard **and**
calls `self.db.refresh_writer_fence()` — so the manifest refresh happens before
*every* attempt inside the retry loop, not once at open. That call is the real
cross-process safety step, and it sits at exactly the position the deleted cell
write lock used to occupy.

**Never write "cell write lock" or "distributed write lock" as a current
mechanism.** Say "SlateDB manifest fencing" (plus "writer promotion" where
relevant).

### 4. There is no delta / outbox / mutation log. At all.

This is the deletion most likely to survive unnoticed in inherited prose,
because the old books used it as a general explanatory device.

Deleted key builders (`src/keys.rs` is now 316 lines): `last_epoch`,
`mutation_log_*`, `outbox*`, `delta_*`, `owner_delta`, `pair_delta`,
`delta_gc_watermark`. Deleted functions: `append_edge_mutation_log`,
`materialize_edge_mutation_log` (`write.rs`, now 5108 lines); `deltas_between`,
`deltas_between_with_budget`, `deltas_since`, `outbox_since` (`query.rs`, now
8389 lines). `DeltaKind` — and therefore `DeltaKind::Plus` / `::Minus` — has
zero occurrences.

**Correction (verified at HEAD).** An earlier revision of this file listed
`query_read_epoch`, `current_epoch` and `edges_at_with_budget` as deleted. They
are **live** — write them as such:

- `query_read_epoch` — `src/shard/query.rs:1821`
- `current_epoch` — `src/shard/query.rs:5983`, now simply `db.snapshot().seq()`
- `edges_at_with_budget` — `src/shard/query.rs:6022`, now delegating to
  `canonical_adjacency_at` (`src/engine/artifact_build.rs:534`) →
  `current_matrix_rows` (`:488`), a plain canonical-key plus segment/tombstone
  scan. The generic row path never touches an index generation; only traversal
  does.
`GraphError::SnapshotExpired` is still declared (`src/core/error.rs:87`) but is
never constructed: dead code, do not build an argument on it.

Consequences you must honour: **no two-phase "append then materialize" write**,
**no delta replay at read time**, **no delta GC watermark**, and **no
"a delete is an append"** framing. A soft delete is a record state, not a delta
row.

What replaces it: a matrix edge-type is marked dirty
(`keys::matrix_dirty` / `matrix_dirty_prefix`, `src/keys.rs:23-29`), an
out-of-process indexer rebuilds an immutable content-addressed generation, and
readers close the remaining lag with a **WAL-tail overlay**.

### 5. Index generations + WAL-tail overlay replace the refresh job.

`GraphIndexGeneration { cell_id, edge_type, base_sequence, last_wal_id,
edge_count, checksum, generation }` (`src/engine/index_store.rs:12-20`), published
under the manifest magic `turbolay-index-current-v1` with CSC payload magic
`turbolay-index-csc-v1` (`index_store.rs:7-8`). Discovery, publication and
`gc_graph_index_generations(cell_id, edge_type, retain_previous)`
(`index_store.rs:210`) all live there; dirty edge types come from
`dirty_graph_index_edge_types` (`index_store.rs:23`).

The reader closes the gap between a generation's `base_sequence` and the read
sequence with `topology_tail_since` (`src/shard/topology_tail.rs:28-60`, called
from `src/shard/query.rs:5293`). It returns `Complete` when the generation is
already current, and `Unavailable` when the snapshot moved or the WAL files have
gone — in which case the query falls back to snapshot adjacency. **This
degradation path is honest and should be documented, not hidden.**

The builder runs in `src/bin/graph-indexer.rs`: env-configured, defaults
`GRAPH_INDEXER_INTERVAL_MS=5000`, `GRAPH_INDEXER_RETAIN_PREVIOUS=1`,
`GRAPH_INDEXER_ADMIN_ADDR=0.0.0.0:9091` (`graph-indexer.rs:58-64`). Say
"out-of-process indexer", never "background refresh job".

### 6. The cluster is symmetric, and there is no placement map.

`ShardPlacement` and rendezvous/fixed hashing are gone — zero occurrences. Cell
location is an object-store directory: `ObjectStoreNodeDirectory { cells, nodes }`
(`src/engine.rs:78-81`), read from `directory/cells`, `directory/nodes` and
`directory/cell/<id>` keys (`src/engine/cluster.rs:112-133`).
`RoutedGraphCluster { scope, local_node_id, directory, shards, promotable }`
(`src/engine.rs:83-89`) — no `placement`, no `leases`, no `revoked_cells`.
`open_at_path` (`cluster.rs:271`) refuses to start if the local node is absent
from the directory (`cluster.rs:283-287`) and then opens every cell the directory
lists (`cluster.rs:289`).

So: **there is no owner map to consult.** Distributed queries are still bounded
scatter/gather, but do not describe a coordinator "looking up the owner of a
cell in `ShardPlacement`".

### 7. Caching: two regimes, and one of them changed.

`GraphCacheKind` still has exactly 6 variants: `MatrixArtifact, MatrixAdjacency,
GraphBlas, ParsedRowQuery, RelationshipRows, RelationshipPropertyRows`
(`src/core/metrics.rs:50-57`).

Defaults (`src/core/metrics.rs:20-37`): `max_matrix_artifacts = 1_024`,
`max_matrix_adjacencies = 0` (the hydrated-adjacency cache is **off** by
default), `max_graphblas_matrices = 64`, `max_parsed_row_queries = 4_096`,
`max_relationship_row_sets = 1_024`, `max_relationship_property_row_sets = 4_096`,
`max_entries_per_cell = Some(8_192)`, `pin_matrix_min_edges = 1_000_000`,
`max_concurrent_hydrations = 16`.

- **Per-read caches** (parsed query, relationship rows, property rows) are keyed
  by `read_epoch`. A write moves the sequence, the next read misses, LRU ages the
  old entry out. The "no invalidation" story still holds here.
- **Matrix caches** are keyed by a `base_epoch` that deliberately lags the read
  sequence. The lag is closed by the **WAL-tail overlay** (§5) — *not* by
  replaying a delta log. Rewrite any "overlay the deltas" sentence accordingly.

`engine/artifact_gc.rs` still prunes by `base_epoch` and `retain`; it trusts the
caller's epoch (no computed safe-epoch, no read leases).

### 8. Supernodes are gone; Roaring is still not on this branch.

Zero occurrences of `supernode`, `Roaring`, or `roaring` in `src/`; `roaring` is
not a Cargo dependency. Traversal acceleration is matrix artifacts / GraphBLAS
CSC only. The hydrated type is `MatrixAdjacency = BTreeMap<VertexId,
BTreeSet<VertexId>>` (`src/lib.rs:142`) — `BTreeSet`, not `RoaringTreemap`.

**Keep** the Roaring material (`intro-04`, `detail-01`, the `intro-03` Roaring
note) but label it explicitly as PLANNED / FUTURE with a status callout at the
top ("Status: planned, not in the current tree") and phrase claims as "would" /
"is planned to", never "is".

### 9. Deletes are mostly real deletes. (corrected — verified at HEAD)

`delete_relationships_for_structural_edge_txn` (`src/shard/write.rs`) issues real
`txn.delete`s for the relationship, its id index, its property indexes and the
count.

**Correction.** An earlier revision of this file said "the structural *edge* is
still soft-deleted". That is wrong on the canonical path.
`delete_structural_edge_txn` (`src/shard/write.rs:4921`) issues real `txn.delete`s
on `keys::out_edge` and the reverse/metadata rows, and only adjusts the degree
counters with `txn.put`. The soft delete lives on the **segment** path:
`keys::out_segment_tombstone` (`src/keys.rs:89`), written when the edge being
deleted lives inside a packed segment that cannot be edited in place.

So the accurate sentence is: canonical rows and relationships are physically
removed; a segment-resident edge leaves a tombstone instead. Do not write
"everything is a soft delete", do not write "the structural edge is soft-deleted"
without the segment qualifier, and do not describe any of it as a `Minus` delta (§4).

### 10. Graph scopes are dynamic. One process serves many tenants. (verified at HEAD)

This landed **after** every chapter was last written (`b5c933b`, `3f80551`,
2026-07-21) and is invisible to any prose you inherit. All of it is verified at
`1f5b06d`.

**There are three cluster containers, not two.** `GraphCluster` (one scope, many
shards) → `RoutedGraphCluster` (adds the fleet view; §6 above) →
`ScopedRoutedGraphCluster` (`src/engine.rs:101-115`, impl
`src/engine/cluster.rs:762-968`), which holds a `BTreeMap<GraphScope,
RoutedGraphCluster>` and is **what `graph-node` actually constructs**
(`src/bin/graph-node.rs:44`). Do not describe the node as opening one scope.

- `cluster_for_scope` (`cluster.rs:847-907`) is the whole mechanism: validate →
  LRU hit → evict if full → open + register.
- `validate_scope` (`:816-828`) requires a matching `graph_id` and a namespace
  **descendant of** `root_namespace`; otherwise `GraphError::GraphScopeMismatch`.
  This is the tenant-isolation guard — nothing else enforces it.
- Eviction only considers entries with `Arc::strong_count == 1` (nobody holding
  it). If every slot is in use the request **fails** with
  `GraphError::AdmissionRejected { operation: "open_graph_scopes" }`. It does
  **not** block or queue — say so.
- `max_open_scopes` comes from `GRAPH_MAX_OPEN_SCOPES`, **default 8**
  (`src/bin/graph_node/config.rs:181`). Zero is rejected at construction.
- `options_for_scope` (`:830-845`) gives each scope a private cache dir under
  `scopes/<namespace…>/graphs/<id>` **and divides `object_store_cache_bytes` by
  `max_open_scopes`** — by the capacity, not the live count. Test:
  `scoped_clusters_partition_the_local_slate_cache_budget` (`:990`).
- Metrics gained a scope dimension: `ScopedGraphShardRuntimeMetrics { scope,
  shard }` (`src/engine.rs:126-130`), built by `local_shard_runtime_metrics`
  (`:922-944`).

**The scope directory is discovery, not authority.**
`ObjectStoreGraphScopeDirectory` (`src/engine/scope_directory.rs`): `register`
(`:40`) writes a marker with `PutMode::Create` and treats `AlreadyExists` as
success (idempotent); `list` (`:54`) enumerates them. Nothing consults it to
authorize a request — `validate_scope` does that.

**The deployed resolver is hierarchical, not static.** `graph-node` wires
`HierarchicalClientDatabaseResolver` (`src/bin/graph-node.rs:90`,
`src/client/service.rs:61-127`), **not** `StaticClientDatabaseResolver`. The
database name *encodes* the tenant path: `<base>.scope1.<tenant>.<sub_tenant>`,
components base64url-encoded without padding (`encode_database_scope_id`,
`:130-140`), absent sub-tenant written as the sentinel `_`,
`SCOPED_DATABASE_VERSION = "scope1"` (`:58`). A name equal to the base database
resolves to the root target unchanged. Graph id and cell id are inherited from
the root target; **only the namespace path deepens — tenants are separated by
namespace, not by cell.**

**The indexer sweeps all scopes.** `graph-indexer` calls
`scope_directory.list()` every cycle (`src/bin/graph-indexer.rs:132`), skips
scopes with no data via `scope_has_data` (`:167-187`), and opens *and closes* a
`GraphCluster` per scope per cycle — it caches nothing between cycles, unlike the
data node. Failures are collected per scope, not fatal to the sweep. Any prose
saying it "opens a read-side `GraphCluster` over those cells" and stops there is
now incomplete.

### 11. Known-wrong sections — RESOLVED for `inside` and `conceptual`

All entries below were fixed in the deleted-subsystem sweep. Both books now pass a
grep for `ShardPlacement`, `rendezvous`, `cell write lock`, `TopologySequence`,
`last_epoch`, `delta_*`, `outbox`, `mutation log`, `artifact_refresh` and
`supernode` with **only negations remaining** ("there is no…"), in prose and
inside diagrams. Kept here as the record of what was wrong and where, because the
`quint` book has not been swept and may still inherit any of it.

Resolved:

- **`05-caching.typ` §5.8 "Read-through hydration and background refresh"**
  (~lines 470-500) documents `src/engine/artifact_refresh.rs` and
  `start_matrix_artifact_refresh_job` as live. **That file was deleted** (§ the
  resync section above). The section needs a rewrite onto index generations and
  the out-of-process indexer, and it still contains one `TopologySequence`
  (`min_epoch_lag`) that survives only because the surrounding section is being
  replaced wholesale rather than patched.
- **`intro-01-cells-and-placement.typ`** is still on the pre-resync edition
  end to end: `ShardPlacement`, `rendezvous` hashing, and the object-store *cell
  write lock* with an owner token and TTL, in both prose and the
  `<fig-intro01-placement>` diagram. It cannot be patched — the figure encodes
  the deleted model. Rewrite the chapter and the figure together in Wave 4.
- **`intro-03-epochs-artifacts-overlays.typ`** asserts `TopologySequence` as a
  live type at `:12` ("the monotonic topology-change counter") and again at
  `:123` *inside a diagram node*, alongside `meta/last_epoch` — a key builder
  that no longer exists. Same treatment as `intro-01`: rewrite with the figure,
  do not patch the prose around a diagram that still teaches the old model.

A useful invariant while working: in the `inside` book every surviving mention of
a deleted subsystem is a **negation** ("there is no…", "if you find… it does
not"), and that is correct and should stay. In the `conceptual` book they are
still **assertions**. That difference is the whole remaining gap between the two
books.

---

## Structural decisions in force (set by the book owner)

1. **The conceptual book is being restored to full turbolay-v1 depth.** An
   earlier pass compressed its chapters to roughly 107 lines each; that
   compression is being **reversed**. Target the v1 shape: roughly 500 lines per
   chapter, built on the `Problem N` ladder (declarative-claim title → opening
   scenario → numbered problem sections, each running failure → why the obvious
   fix falls short → mechanism → invariant → synthesis → honest boundary →
   revision notes). See `skills/chapter-framing/SKILL.md`. Do not re-compress.

2. **Chapter revision order is `00-foundations` FIRST, then 02 / 03 / 04.**
   `00-foundations` defines the vocabulary — `StorageSequence`, writer promotion
   and manifest fencing, causal vs strong reads — that the later chapters
   consume. Revising 02 or 03 first means inventing terminology that 00 will
   later contradict. `01-architecture` has already been rewritten for the resync
   and its §1.1–1.10 numbering is **frozen**; other chapters point into it.

3. **All three books converge on Bookly's `custom-box`.** The `template.typ`
   helpers `#term` / `#why` / `#srcblock` are being retired; a separate pass is
   converting the `inside` book's remaining calls. Write `custom-box` in new and
   revised content. See `skills/writing-content/SKILL.md` for the exact forms —
   and note that the callout accent colour is derived from `icon:`, so **never
   pass a `color:` argument**.

---

## Terminology rules (keep all three books consistent)

- Product name is **TurboLay** (capital L) in prose and titles; the crate/repo
  stays `slatedb-graph-kernel` in code.
- Use "cell" for the isolation/ownership unit (never "namespace" unless the API
  does).
- "epoch" is fine colloquially; the *type* is always `StorageSequence`. Never
  `TopologySequence`, never `GraphEpoch`.
- Say "SlateDB manifest fencing" and "writer promotion". Never "cell write lock",
  "distributed write lock", "lease", or "write fence" as a durable record.
- Say "out-of-process indexer" and "index generation". Never "refresh job" or
  "rollup" (the word "rollup" appears nowhere in `src/`).
- Say "WAL-tail overlay". Never "delta replay", "outbox", or "mutation log".
- Traversal acceleration = "matrix artifacts" / GraphBLAS CSC, never
  "supernodes".
- Distributed queries = bounded scatter/gather (directory-aware legs, concurrent
  execution, coordinator merge). Not a global planner, not a global snapshot,
  and not an owner lookup.
- Say "graph scope" for the tenant coordinate (`<namespace>/graphs/<graph_id>`)
  and keep "cell" for the isolation/ownership unit — they are different axes and
  a node holds many scopes, each with the same cells. Never say a node "is
  configured for a scope"; scopes are opened on demand and evicted under a
  capacity bound. Never say a full node "waits" or "queues" for a scope slot: it
  rejects with `AdmissionRejected`.

## Citations

Line numbers drift — verify every `file:line` against current source before you
write it. Prefer a function/type name plus file over a bare line number. Line
numbers inherited from the older editions are mostly stale; re-derive them. Do
not trust this file's numbers blindly either: open the file.
