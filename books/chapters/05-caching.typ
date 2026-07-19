#import "../template.typ": term, why, srcblock, figcap, accent, muted
#import "@preview/fletcher:0.5.7" as fletcher: diagram, node, edge
#import "../vendor/bookly/src/themes/reader.typ": reader-colors

= Caching

The read chapter kept meeting caches and deferring them to here. This chapter is that
explanation. It matters more in TurboLay than in a database backed by local disk, because
TurboLay's source of truth is a remote object store where every miss can cost a network round
trip. Caching is not a nicety here; it is what makes the engine usable.

There are two layers of cache, and they are easy to confuse, so the chapter separates them
first and then goes deep on each. Underneath sits a byte cache that SlateDB manages, holding
raw storage blocks so the engine does not re-fetch them from the object store. Above it sits a
set of caches the engine manages itself, holding computed results such as parsed queries,
relationship rows, and matrix artifacts, so the engine does not recompute them. The code is in
`src/core/cache.rs` for the engine caches, `src/core/config.rs` and `src/core/metrics.rs` for
their configuration, and `src/shard/lifecycle.rs` for where they are built.

== Two layers

#figure(
  diagram(
    node-stroke: 0.6pt,
    spacing: (0pt, 0.7cm),
    node((0, 0), [engine result caches (this chapter, `BoundedGraphCache`)\ parsed queries, relationship rows, matrix artifacts / GraphBLAS, ...], fill: rgb("#eef4ff"), width: 12cm),
    edge((0, 0), (0, 1), "->", [miss: recompute]),
    node((0, 1), [SlateDB object-store block cache (foyer, on local disk)\ raw storage blocks], fill: rgb("#fff8e6"), width: 12cm),
    edge((0, 1), (0, 2), "->", [miss: fetch object]),
    node((0, 2), [object store (S3 / MinIO / filesystem)], fill: rgb("#e9fce9"), width: 12cm),
  ),
  caption: none,
)
#figcap[The two cache layers. The upper layer avoids recomputation; the lower layer avoids re-fetching bytes. A read consults the upper layer first, and only a full miss reaches the object store.]

== The object-store block cache

The lower layer is not TurboLay's own code. It is a feature of SlateDB that TurboLay turns on
and configures.

#term("Block cache")[
  A cache of raw storage blocks (chunks of an SST file) kept close to the process so repeated
  reads do not re-fetch them from slow storage. SlateDB keeps its block cache on local disk in
  front of the object store, so a warmed process reads most blocks from local disk instead of
  over the network.
]

TurboLay pins SlateDB with the `foyer` feature (`Cargo.toml:105`).

#term("foyer")[
  A hybrid caching library (memory plus disk) that SlateDB uses to implement its object-store
  cache. TurboLay does not call foyer directly; it enables it through the SlateDB feature and
  configures a cache directory and a size budget. From TurboLay's point of view foyer is the
  machinery that makes the disk block cache work.
]

TurboLay's control over this layer is the small `GraphCacheConfig`:

#srcblock("src/core/config.rs:59-64")[```rust
pub struct GraphCacheConfig {
    pub object_store_cache_dir: Option<PathBuf>,
    pub object_store_cache_bytes: Option<usize>,
    pub object_store_cache_puts: bool,
    pub preload_sst_on_startup: bool,
}
```]

These four fields are translated into SlateDB's settings when the database is opened:

#srcblock("src/core/config.rs:95-108")[```rust
fn apply_to_settings(&self, settings: &mut Settings) {
    if let Some(cache_dir) = &self.object_store_cache_dir {
        settings.object_store_cache_options.root_folder = Some(cache_dir.clone());
    }
    if let Some(max_cache_size_bytes) = self.object_store_cache_bytes {
        settings.object_store_cache_options.max_cache_size_bytes = Some(max_cache_size_bytes);
    }
    settings.object_store_cache_options.cache_puts = self.object_store_cache_puts;
    if self.preload_sst_on_startup {
        settings.object_store_cache_options.preload_disk_cache_on_startup =
            Some(PreloadLevel::AllSst);
    }
}
```]

Two options are worth calling out. `cache_puts` decides whether data the process writes is also
placed into the local cache, so a writer can read back what it just wrote from disk rather than
the object store. Notice that the read-only reader path sets it to `false`
(`apply_to_reader_options`, `config.rs:117`), because a pure reader has nothing to put.
`preload_sst_on_startup` warms the cache by loading all SST files at startup, trading a slower
start for a faster first query.

#why[
  Putting the block cache on disk rather than only in memory suits an object-store backend. The
  working set of a large graph will not fit in memory, but it can fit on a local SSD, and even a
  local SSD read is far cheaper than an S3 GET. Preloading exists because the first query after a
  cold start would otherwise pay the full object-store latency for every block it touches.
]

== The engine's result caches

The upper layer is TurboLay's own. There are seven cache fields on the `GraphShard`
(Chapter 1), covering six kinds of computed result. Each is a `BoundedGraphCache` behind a
mutex. The kinds are enumerated for metrics:

#srcblock("src/core/metrics.rs:49-57")[```rust
pub enum GraphCacheKind {
    MatrixArtifact,
    MatrixAdjacency,
    GraphBlas,
    ParsedRowQuery,
    RelationshipRows,
    RelationshipPropertyRows,
}
```]

The shard holds seven caches because the relationship-rows kind is split across two fields — a
destination-pair cache and a newer one-hop source cache. The full set of fields
(`src/core/state.rs:50-66`):

#table(
  columns: (auto, 1fr),
  inset: 5pt,
  align: (left + top, left + top),
  stroke: 0.4pt + rgb("#d0d7de"),
  [*Cache*], [*What it saves and when it is consulted*],
  [`parsed_row_query_cache`], [The lowered form of a Cypher string, so a repeated query is not re-parsed. Consulted at the start of execution (read chapter, parse stage). The only cache whose key has no epoch.],
  [`matrix_artifact_cache` / `matrix_cache` / `graphblas_cache`], [A matrix artifact's manifest, its hydrated `MatrixAdjacency`, and its compiled GraphBLAS matrix. All three are keyed by `MatrixCacheKey { cell_id, edge_type, base_epoch }`. They form the base layer of the MVCC merge, so caching them saves rebuilding the base on every read.],
  [`relationship_rows_cache` / `relationship_property_rows_cache`], [Relationship records for a source-destination pair (optionally by property) at a read epoch.],
  [`source_relationship_rows_cache`], [The newer one-hop cache: for a single source vertex, its list of destinations (`Arc<Vec<VertexId>>`), keyed by `SourceRelationshipRowsCacheKey`. Serves neighbor expansion without re-reading the relationship keyspace.],
)

Every one of these caches shares the same implementation, `BoundedGraphCache`, so understand it
once and you understand all seven.

== Inside BoundedGraphCache

#term("Least-recently-used (LRU)")[
  An eviction policy: when the cache is full, throw out the entry that has gone longest without
  being read. It approximates "keep what is likely to be used again". TurboLay implements LRU
  with a logical clock that ticks on every access, stamping each entry with its last-access
  time.
]

The cache is a map from key to an entry that records the value, its owning tenant, whether it is
pinned, its last-access time, and how many bytes it occupies:

#srcblock("src/core/cache.rs:190-206")[```rust
struct CacheEntry<V> {
    value: V,
    tenant: String,
    pinned: bool,
    last_access: u64,
    resident_bytes: usize,
}

pub(crate) struct BoundedGraphCache<K, V> {
    max_entries: usize,
    max_resident_bytes: usize,
    resident_bytes: usize,
    max_entries_per_tenant: Option<usize>,
    clock: u64,
    entries: BTreeMap<K, CacheEntry<V>>,
    tenant_entries: BTreeMap<String, usize>,
}
```]

A read bumps the clock and stamps the entry, which is what makes eviction LRU:

#srcblock("src/core/cache.rs:241-246")[```rust
pub(crate) fn get(&mut self, key: &K) -> Option<V> {
    self.clock = self.clock.saturating_add(1);
    let entry = self.entries.get_mut(key)?;
    entry.last_access = self.clock;
    Some(entry.value.clone())
}
```]

An insert enforces two independent budgets after adding the entry: a per-tenant entry quota and
a total limit that is both an entry count and a byte count. Eviction picks the least-recently
used entry that is eligible:

#srcblock("src/core/cache.rs:344-375 (abridged)")[```rust
fn enforce_total_limit(&mut self, metrics: &GraphCacheMetrics) {
    while self.entries.len() > self.max_entries || self.resident_bytes > self.max_resident_bytes {
        if self.evict_one(None, false, metrics).is_none()
            && self.evict_one(None, true, metrics).is_none()
        {
            break;
        }
    }
}

fn evict_one(&mut self, tenant: Option<&str>, allow_pinned: bool, metrics: &GraphCacheMetrics) -> Option<()> {
    let key = self.entries.iter()
        .filter(|(_, entry)| /* tenant matches */ && (allow_pinned || !entry.pinned))
        .min_by_key(|(_, entry)| entry.last_access)   // least recently used
        .map(|(key, _)| key.clone())?;
    self.remove(&key);
    metrics.evictions.fetch_add(1, Ordering::Relaxed);
    Some(())
}
```]

Two details in that eviction loop are important. First, it tries `evict_one(..., false)` before
`evict_one(..., true)`: it evicts unpinned entries first and only touches pinned ones if it has
no choice. Second, it enforces the byte budget the same way as the entry budget, so a cache full
of large values evicts on size, and a cache full of small values evicts on count. A single value
larger than the whole byte budget is simply not retained; the tests confirm an oversized entry
inserts as `None` and leaves the cache empty (`cache.rs:416-425`).

For the artifact caches there is a specialized read that picks the best entry rather than an
exact key, `get_latest_by`, which selects the entry with the highest score (the newest base
epoch) among those matching a predicate:

#srcblock("src/core/cache.rs:248-260")[```rust
pub(crate) fn get_latest_by(
    &mut self,
    mut predicate: impl FnMut(&K, &V) -> bool,
    mut score: impl FnMut(&K, &V) -> TopologySequence,
) -> Option<V> {
    let key = self.entries.iter()
        .filter(|(key, entry)| predicate(key, &entry.value))
        .max_by_key(|(key, entry)| score(key, &entry.value))
        .map(|(key, _)| key.clone())?;
    self.get(&key)
}
```]

This is how a read finds "the newest cached matrix artifact whose base epoch is at or below my
read epoch" without knowing the exact base epoch in advance. The score is a `TopologySequence` —
the topology-change cursor, not a distinct MVCC type — and the predicate does the
`base_epoch <= read_epoch` test (`latest_matrix_artifact`, `src/engine/artifact_build.rs:607-617`).

== Two budgets: entries and bytes

Each cache is constructed with two limits that come from two different config structs. The entry
counts and the per-tenant quota come from `GraphCachePolicy`; the byte budgets come from
`GraphMemoryConfig`. You can see both feeding the constructors:

#srcblock("src/shard/lifecycle.rs:174-186")[```rust
matrix_artifact_cache: Mutex::new(BoundedGraphCache::new(
    cache_policy.max_matrix_artifacts,
    tenant_quota,
)),
matrix_cache: Mutex::new(BoundedGraphCache::new_with_byte_limit(
    cache_policy.max_matrix_adjacencies,
    tenant_quota,
    memory.max_matrix_adjacency_bytes,
)),
graphblas_cache: Mutex::new(BoundedGraphCache::new_with_byte_limit(
    cache_policy.max_graphblas_matrices,
    tenant_quota,
    memory.max_graphblas_bytes,
)),
```]

`GraphCachePolicy` (`metrics.rs:5`) sets the entry caps and the per-cell tenant quota; its
defaults give, for example, 1,024 matrix artifacts, 64 GraphBLAS matrices, 4,096 parsed row
queries, 1,024 relationship-row sets, and a per-cell quota of 8,192 entries (`metrics.rs:20-37`).
The small caches use only an entry cap; the ones that hold large values (GraphBLAS matrices,
relationship rows, source relationship rows, relationship property rows) also carry a byte budget
from `GraphMemoryConfig` (`config.rs:137-149`). The GraphBLAS byte budget defaults to 128 MiB, the
relationship-rows budgets to 8-16 MiB; a `low_memory` profile (`config.rs:168`) shrinks them for
constrained deployments.

One default is worth pausing on. The hydrated-adjacency cache is *off by default*:
`max_matrix_adjacencies = 0` (`metrics.rs:24`) and `max_matrix_adjacency_bytes = 0`
(`config.rs:154`). A cache built with `max_entries == 0` early-returns on every insert, so nothing
is retained. In the default configuration only the *compiled GraphBLAS matrix* is cached (64
entries / 128 MiB); the intermediate `MatrixAdjacency` map is hydrated when needed and dropped.
Enabling the adjacency cache is an opt-in for workloads that want the map form resident.

#why[
  Two budgets exist because entries and bytes fail differently. A cache of parsed queries could
  hold millions of tiny entries, so it needs a count cap. A cache of compiled GraphBLAS matrices
  could blow out memory with a handful of huge entries, so it needs a byte cap. Bounding only one
  would leave the other failure mode open, so the caches that can hit both bound both.
]

== Pinning the expensive results

Some cached values are far more expensive to rebuild than others. A matrix artifact for a graph
with millions of edges took real work to compute and load. TurboLay pins those so they survive
eviction unless the cache is truly out of room.

#term("Pinning")[
  Marking a cache entry as protected so it is only evicted when there is no unpinned entry left
  to drop. TurboLay pins the entries that are most costly to recompute: large matrix artifacts
  and the compiled GraphBLAS matrices built from them. The eviction loop always drops unpinned
  entries first.
]

The decision is a single threshold on edge count:

#srcblock("src/core/metrics.rs:44-46")[```rust
pub(crate) fn pin_matrix_artifact(&self, artifact: &engine::MatrixArtifact) -> bool {
    artifact.edge_count >= self.pin_matrix_min_edges          // default 1_000_000
}
```]

There is no supernode pinning any more — the supernode subsystem is gone, and with it
`pin_supernode_group`. The `pin` flag is decided and applied at insert time in
`src/engine/matrix_cache.rs`: when the hydration path inserts a hydrated adjacency or a compiled
GraphBLAS matrix, it passes `edge_count >= pin_matrix_min_edges` as the `pinned` argument to
`insert_sized`. So a million-edge artifact is pinned by default; smaller results are cheap enough
to rebuild that letting them age out is fine.

== Why the caches are correct: two regimes

A cache is dangerous when it can serve stale data. TurboLay's result caches do not, but they
achieve it in two different ways, and the difference matters. There is a common temptation to say
"every key embeds an epoch, so a write makes a new key and old entries just age out — there is no
invalidation code because there is nothing to invalidate." That story is true, but only for one
family of caches. The matrix caches work differently and *are* explicitly pruned. Separating the
two regimes is the point of this section.

#figure(
  diagram(
    node-stroke: 0.55pt,
    spacing: (0.7cm, 0.7cm),
    // LEFT: per-read caches — self-invalidating.
    node((0, 0), text(size: 8pt)[*per-read caches* \ keyed by `read_epoch`], fill: reader-colors.info_soft, stroke: reader-colors.info, width: 4.7cm),
    edge((0, 0), (0, 1), "->", stroke: reader-colors.muted),
    node((0, 1), text(size: 8pt)[write advances epoch], fill: reader-colors.info_soft, stroke: reader-colors.info, width: 4.7cm),
    edge((0, 1), (0, 2), "->", stroke: reader-colors.muted),
    node((0, 2), text(size: 8pt)[new key → miss], fill: reader-colors.info_soft, stroke: reader-colors.info, width: 4.7cm),
    edge((0, 2), (0, 3), "->", stroke: reader-colors.muted),
    node((0, 3), text(size: 8pt)[old entry ages out (LRU)], fill: reader-colors.info_soft, stroke: reader-colors.info, width: 4.7cm),
    node((0, 4), text(size: 7.5pt, fill: reader-colors.muted)[self-invalidating; no invalidation code], stroke: none, width: 4.7cm),

    // RIGHT: matrix caches — explicitly GC'd.
    node((1, 0), text(size: 8pt)[*matrix caches* \ keyed by lagging `base_epoch`], fill: reader-colors.purple_soft, stroke: reader-colors.purple, width: 4.7cm),
    edge((1, 0), (1, 1), "->", stroke: reader-colors.muted),
    node((1, 1), text(size: 8pt)[read picks `base_epoch <= read_epoch`], fill: reader-colors.purple_soft, stroke: reader-colors.purple, width: 4.7cm),
    edge((1, 1), (1, 2), "->", stroke: reader-colors.muted),
    node((1, 2), text(size: 8pt)[overlay deltas; write does not invalidate], fill: reader-colors.purple_soft, stroke: reader-colors.purple, width: 4.7cm),
    edge((1, 2), (1, 3), "->", stroke: reader-colors.muted),
    node((1, 3), text(size: 8pt)[`artifact_gc` prunes via `retain`], fill: reader-colors.purple_soft, stroke: reader-colors.purple, width: 4.7cm),
    node((1, 4), text(size: 7.5pt, fill: reader-colors.muted)[explicitly invalidated by GC], stroke: none, width: 4.7cm),

    // BOTTOM: read-through hydration fed by the background refresh job.
    node((0, 5), text(size: 8pt)[background refresh job], fill: reader-colors.surface_soft, stroke: reader-colors.border, width: 4.7cm),
    edge((0, 5), (1, 5), "->", stroke: reader-colors.muted, label: text(size: 7.5pt, fill: reader-colors.muted)[feeds]),
    node((1, 5), text(size: 8pt)[read-through hydration \ permit → load artifact → insert pinned], fill: reader-colors.ok_soft, stroke: reader-colors.ok, width: 4.7cm),
  ),
  caption: none,
) <fig-ch05-two-regimes>
#figcap[The two cache-correctness regimes. On the left, per-read caches embed `read_epoch`, so a write just mints new keys and the stale entries age out under LRU — the "no invalidation code" thesis holds exactly here. On the right, the matrix caches reuse a deliberately lagging `base_epoch` across many read epochs (a read overlays deltas on it), so a write does *not* self-invalidate them; they need real, GC-driven eviction via `artifact_gc`'s `retain`, with entries hydrated read-through and fed by the background refresh job.]

=== Regime one: per-read caches, epoch-keyed, self-invalidating

The parsed-query, relationship-rows, source-relationship-rows, and relationship-property-rows
caches are the classic epoch-keyed design.

#term("Epoch-keyed invalidation")[
  The technique of putting the read epoch into the cache key, so that a new version is a new key
  rather than an overwrite of an old one. There is no explicit "invalidate this entry on write"
  step. A write advances the epoch; reads at the new epoch use new keys and miss the cache;
  entries for old epochs are never served to a new-epoch read and simply age out under LRU.
]

Each of these content-dependent keys embeds `read_epoch` (`RelationshipRowsCacheKey`,
`SourceRelationshipRowsCacheKey`, etc., in `src/core/cache.rs` and `src/lib.rs`). Put that
together with the write and delete chapters:

+ A write or delete advances the cell's topology cursor and appends a delta at the new epoch.
+ The next read pins the new epoch, so it builds cache keys with the new epoch.
+ Those keys are not in the cache, so the read misses and computes fresh, correct results.
+ The old-epoch entries are still in the cache but can never match a new-epoch key, so they are
  never served incorrectly. They sit idle and are eventually evicted as least-recently-used.

The parsed-query cache is the one deliberate exception whose key is only the query string,
because a parse result does not depend on graph contents. For this whole family, the "no
invalidation code because there is nothing to invalidate" thesis holds exactly: a write touches
none of these caches.

#figure(
  diagram(
    node-stroke: 0.55pt,
    spacing: (0.6cm, 0.75cm),
    node((0, 0), [read at epoch 5\ key `(..., 5)`], fill: rgb("#eef4ff"), width: 3.6cm),
    edge((0, 0), (1, 0), "->", [hit]),
    node((1, 0), [cached value\ for epoch 5], fill: rgb("#e9fce9"), width: 3.6cm),
    node((0, 1), [write advances\ epoch to 6], fill: rgb("#fff8e6"), width: 3.6cm),
    edge((0, 1), (0, 2), "->"),
    node((0, 2), [read at epoch 6\ key `(..., 6)`], fill: rgb("#eef4ff"), width: 3.6cm),
    edge((0, 2), (1, 2), "->", [miss: recompute]),
    node((1, 2), [fresh value\ for epoch 6], fill: rgb("#e9fce9"), width: 3.6cm),
    node((2, 1), [epoch-5 entry\ still cached,\ never served to\ an epoch-6 read], fill: rgb("#f6f8fa"), width: 3.6cm),
  ),
  caption: none,
)
#figcap[Epoch-keyed invalidation for the per-read caches. A write does not touch the cache at all. The new epoch produces new keys, so the reader misses old entries automatically and the stale ones age out.]

=== Regime two: matrix caches, base-epoch keyed, deliberately lagging

The three matrix caches (`matrix_artifact_cache`, `matrix_cache`, `graphblas_cache`) do *not*
work this way, and it is a common mistake to assume they do. Their key is not the read epoch. It
is a `base_epoch` that deliberately lags the read epoch:

#srcblock("src/lib.rs:155-159")[```rust
pub(crate) struct MatrixCacheKey {
    pub(crate) cell_id: String,
    pub(crate) edge_type: String,
    pub(crate) base_epoch: TopologySequence,
}
```]

A matrix artifact is a periodically-rebuilt snapshot of the adjacency at some past epoch. A read
at epoch $N+1$ does *not* build a fresh artifact keyed at $N+1$. Instead it asks
`get_latest_by` for the newest cached artifact whose `base_epoch <= read_epoch`
(`latest_matrix_artifact`, `src/engine/artifact_build.rs:607-639`) and then *merges the delta log*
recorded since that base epoch on top of it. The base is old on purpose; correctness comes from
overlaying the deltas, not from the base being current.

The consequence is the opposite of regime one: a write at $N+1$ does *not* invalidate the base-$N$
artifact, and a new epoch does *not* automatically miss. The base-$N$ artifact stays valid and
keeps being reused, with an ever-growing delta overlay, until a newer artifact is built. So these
caches need real invalidation, and they have it. Garbage collection prunes them explicitly with
`retain`:

#srcblock("src/engine/artifact_gc.rs:56-64")[```rust
self.matrix_artifact_cache.lock().await.retain(|key, _| {
    key.cell_id != cell_id || key.edge_type != edge_type || key.base_epoch >= keep_epoch
});
self.matrix_cache.lock().await.retain(/* same predicate */);
self.graphblas_cache.lock().await.retain(/* same predicate */);
```]

`delete_graph_artifacts_before` deletes the on-disk artifact keys with `base_epoch < keep_epoch`
and then drops the corresponding cache entries for that cell and edge type. This is the invalidation
step that regime one does not need.

#why[
  The write and delete chapters never mentioned invalidating the per-read caches because there
  genuinely is nothing to invalidate there: a stale entry has a key no current read will ever
  construct. But do not over-generalize that to the whole engine. The matrix caches trade freshness
  of the *base* for the amortized cost of rebuilding it — the base lags, the delta log carries the
  recent writes, and an explicit GC-time `retain` is what eventually reclaims superseded bases. Two
  regimes, two correctness arguments: self-invalidating for per-read results, base-epoch plus
  delta-merge plus GC-time prune for the matrix layer.
]

== Read-through hydration and background refresh

Two engine files hold the machinery behind the matrix caches, and they explain both how an entry
gets into the cache and why its `base_epoch` lags the write epoch.

*Read-through hydration* lives in `src/engine/matrix_cache.rs`. On a matrix-cache miss the engine
does not block indefinitely or hydrate unboundedly. It acquires a hydration permit
(`acquire_hydration_permit`, bounded by `max_concurrent_hydrations`, default 16) for the adjacency
path, or the `matrix_compilation_gate` semaphore for the compiled-GraphBLAS path, then loads the
artifact from the object store — either the tiled on-disk format (`load_matrix_adjacency` walks the
matrix tiles) or the newer GraphBLAS CSC format (`graphblas_csc`, recognized by a `GRAPHBLAS_CSC_MAGIC`
header and checksum) — and finally calls `insert_sized(..., pinned)` to place the sized, possibly
pinned entry into the cache. `cached_matrix_adjacency` and `cached_graphblas_matrix` are the two
entry points; both hydrate on miss and return an `Arc` so the compiled matrix is shared, not copied.

*Background refresh* lives in `src/engine/artifact_refresh.rs`. A Tokio job
(`start_matrix_artifact_refresh_job`) periodically scans each cell's "dirty" matrix edge-type
markers and rebuilds an artifact when one is *due* under the policy:

#srcblock("src/engine.rs:96-102")[```rust
pub struct MatrixArtifactRefreshPolicy {
    pub interval: Duration,
    pub max_dirty_age: Duration,
    pub min_epoch_lag: TopologySequence,
    pub tile_size: u64,
    pub max_edge_types_per_cycle: usize,
}
```]

Each cycle collects the dirty `(cell, edge_type)` markers, and for each candidate rebuilds only
when the accumulated change is worth it — the epoch lag has reached `min_epoch_lag`, or the marker
has aged past `max_dirty_age` — building at most `max_edge_types_per_cycle` artifacts per pass with
`build_adjacency_image` (`src/engine/artifact_build.rs`). This is exactly *why* matrix artifacts
lag the write epoch: they are rebuilt asynchronously off dirty markers, not synchronously on every
write. The delta-merge at read time is what closes the gap between a lagging base and a current
read.

== Multi-tenancy and metrics

Because one process serves many cells, the caches are shared, and a single busy tenant could
otherwise crowd everyone else out. The per-tenant quota, `max_entries_per_cell` (default 8,192),
bounds how many entries any one cell may hold, enforced by `enforce_tenant_quota`
(`cache.rs:328`) which evicts within the offending tenant before falling back to a global
eviction. Every cache operation is counted through `GraphCacheMetrics` (hits, misses, insertions,
evictions, pinned insertions, tenant-quota rejections) and exposed per kind through
`GraphCacheKind`, so operators can see hit rates and whether a cache is thrashing.

== Recap, and the end of the tour

The caches are the last piece of the machine, and they close the loop the whole book has traced:

- The object store is the durable, slow source of truth (Chapter 0).
- SlateDB's foyer-backed block cache keeps recently touched bytes on local disk so the engine
  rarely re-fetches from the object store.
- Above it, seven `BoundedGraphCache` instances keep computed results (parsed queries,
  relationship rows, matrix artifacts, and their compiled GraphBLAS matrices) so the read path
  rarely recomputes.
- Both budgets, entries and bytes, are bounded; the expensive matrix results are pinned; a
  per-tenant quota keeps cells fair.
- Correctness comes in two regimes: the per-read caches are epoch-keyed and self-invalidating (a
  write just produces new keys), while the matrix caches key on a deliberately lagging base epoch,
  merge the delta log at read time, and are pruned explicitly by garbage collection.

You now have the whole engine end to end. The foundations gave you the vocabulary and the storage
stack. The architecture chapter showed how the code is organized around `GraphShard`. The read
chapter followed a query down to the adjacency keys and back, pinned to one epoch. The write
chapter showed how that epoch and its deltas are created under a three-tier single-writer
guarantee. The delete chapter showed deletes as epoch-stamped soft deletes and the garbage
collection that eventually reclaims them without disturbing live readers. And this chapter showed
the caches that make all of it fast while the epoch discipline keeps them honest.

From here the best next step is the code itself. Open `src/shard/query.rs` and `src/shard/write.rs`
with this book beside you, and the thousands of lines should now read as elaborations of the paths
you have already walked.
