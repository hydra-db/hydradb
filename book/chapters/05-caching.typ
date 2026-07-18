#import "../template.typ": term, why, srcblock, figcap, accent, muted
#import "@preview/fletcher:0.5.7" as fletcher: diagram, node, edge

= Caching

The read chapter kept meeting caches and deferring them to here. This chapter is that
explanation. It matters more in turbolay than in a database backed by local disk, because
turbolay's source of truth is a remote object store where every miss can cost a network round
trip. Caching is not a nicety here; it is what makes the engine usable.

There are two layers of cache, and they are easy to confuse, so the chapter separates them
first and then goes deep on each. Underneath sits a byte cache that SlateDB manages, holding
raw storage blocks so the engine does not re-fetch them from the object store. Above it sits a
set of caches the engine manages itself, holding computed results such as parsed queries,
reachability sets, and matrix artifacts, so the engine does not recompute them. The code is in
`src/core/cache.rs` for the engine caches, `src/core/config.rs` and `src/core/metrics.rs` for
their configuration, and `src/shard/lifecycle.rs` for where they are built.

== Two layers

#figure(
  diagram(
    node-stroke: 0.6pt,
    spacing: (0pt, 0.7cm),
    node((0, 0), [engine result caches (this chapter, `BoundedGraphCache`)\ parsed queries, reachability, matrix artifacts, supernodes, ...], fill: rgb("#eef4ff"), width: 12cm),
    edge((0, 0), (0, 1), "->", [miss: recompute]),
    node((0, 1), [SlateDB object-store block cache (foyer, on local disk)\ raw storage blocks], fill: rgb("#fff8e6"), width: 12cm),
    edge((0, 1), (0, 2), "->", [miss: fetch object]),
    node((0, 2), [object store (S3 / MinIO / filesystem)], fill: rgb("#e9fce9"), width: 12cm),
  ),
  caption: none,
)
#figcap[The two cache layers. The upper layer avoids recomputation; the lower layer avoids re-fetching bytes. A read consults the upper layer first, and only a full miss reaches the object store.]

== The object-store block cache

The lower layer is not turbolay's own code. It is a feature of SlateDB that turbolay turns on
and configures.

#term("Block cache")[
  A cache of raw storage blocks (chunks of an SST file) kept close to the process so repeated
  reads do not re-fetch them from slow storage. SlateDB keeps its block cache on local disk in
  front of the object store, so a warmed process reads most blocks from local disk instead of
  over the network.
]

turbolay pins SlateDB with the `foyer` feature (`Cargo.toml:110`).

#term("foyer")[
  A hybrid caching library (memory plus disk) that SlateDB uses to implement its object-store
  cache. turbolay does not call foyer directly; it enables it through the SlateDB feature and
  configures a cache directory and a size budget. From turbolay's point of view foyer is the
  machinery that makes the disk block cache work.
]

turbolay's control over this layer is the small `GraphCacheConfig`:

#srcblock("src/core/config.rs:76-81")[```rust
pub struct GraphCacheConfig {
    pub object_store_cache_dir: Option<PathBuf>,
    pub object_store_cache_bytes: Option<usize>,
    pub object_store_cache_puts: bool,
    pub preload_sst_on_startup: bool,
}
```]

These four fields are translated into SlateDB's settings when the database is opened:

#srcblock("src/core/config.rs:112-125")[```rust
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
(`apply_to_reader_options`, `config.rs:134`), because a pure reader has nothing to put.
`preload_sst_on_startup` warms the cache by loading all SST files at startup, trading a slower
start for a faster first query.

#why[
  Putting the block cache on disk rather than only in memory suits an object-store backend. The
  working set of a large graph will not fit in memory, but it can fit on a local SSD, and even a
  local SSD read is far cheaper than an S3 GET. Preloading exists because the first query after a
  cold start would otherwise pay the full object-store latency for every block it touches.
]

== The engine's result caches

The upper layer is turbolay's own, and there are ten of them, one per kind of computed result.
They live as fields on the `GraphShard` (Chapter 1). Each is a `BoundedGraphCache` behind a
mutex. The kinds are enumerated for metrics:

#srcblock("src/core/metrics.rs:67-79")[```rust
pub enum GraphCacheKind {
    MatrixArtifact,
    MatrixAdjacency,
    GraphBlas,
    ParsedRowQuery,
    ReachabilityResult,
    RelationshipRows,
    RelationshipPropertyRows,
    SupernodeGroup,
    PostingChunk,
    MaterializedSupernode,
}
```]

Mapping them back to the read chapter:

#table(
  columns: (auto, 1fr),
  inset: 5pt,
  align: (left + top, left + top),
  stroke: 0.4pt + rgb("#d0d7de"),
  [*Cache*], [*What it saves and when it is consulted*],
  [`ParsedRowQuery`], [The lowered form of a Cypher string, so a repeated query is not re-parsed. Consulted at the start of execution (read chapter, parse stage). The only cache whose key has no epoch.],
  [`ReachabilityResult`], [The vertices reachable in a hop range from a source at an epoch. Consulted by the traversal path.],
  [`MatrixArtifact` / `MatrixAdjacency` / `GraphBlas`], [A matrix artifact's manifest, its hydrated adjacency, and its compiled GraphBLAS form. These are the base layer of the MVCC merge, so caching them saves rebuilding the base on every read.],
  [`RelationshipRows` / `RelationshipPropertyRows`], [Relationship records for a source-destination pair (optionally by property) at an epoch.],
  [`SupernodeGroup` / `PostingChunk` / `MaterializedSupernode`], [The precomputed neighbor structures for high-degree vertices, so a one-hop query on a supernode is served from memory.],
)

Every one of these caches shares the same implementation, `BoundedGraphCache`, so understand it
once and you understand all ten.

== Inside BoundedGraphCache

#term("Least-recently-used (LRU)")[
  An eviction policy: when the cache is full, throw out the entry that has gone longest without
  being read. It approximates "keep what is likely to be used again". turbolay implements LRU
  with a logical clock that ticks on every access, stamping each entry with its last-access
  time.
]

The cache is a map from key to an entry that records the value, its owning tenant, whether it is
pinned, its last-access time, and how many bytes it occupies:

#srcblock("src/core/cache.rs:182-198")[```rust
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

#srcblock("src/core/cache.rs:233-238")[```rust
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

#srcblock("src/core/cache.rs:336-367 (abridged)")[```rust
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
inserts as `None` and leaves the cache empty (`cache.rs:408-418`).

For the artifact caches there is a specialized read that picks the best entry rather than an
exact key, `get_latest_by`, which selects the entry with the highest score (the newest base
epoch) among those matching a predicate:

#srcblock("src/core/cache.rs:240-252")[```rust
pub(crate) fn get_latest_by(
    &mut self,
    mut predicate: impl FnMut(&K, &V) -> bool,
    mut score: impl FnMut(&K, &V) -> GraphEpoch,
) -> Option<V> {
    let key = self.entries.iter()
        .filter(|(key, entry)| predicate(key, &entry.value))
        .max_by_key(|(key, entry)| score(key, &entry.value))
        .map(|(key, _)| key.clone())?;
    self.get(&key)
}
```]

This is how a read finds "the newest cached matrix artifact whose base epoch is at or below my
read epoch" without knowing the exact base epoch in advance.

== Two budgets: entries and bytes

Each cache is constructed with two limits that come from two different config structs. The entry
counts and the per-tenant quota come from `GraphCachePolicy`; the byte budgets come from
`GraphMemoryConfig`. You can see both feeding the constructors:

#srcblock("src/shard/lifecycle.rs:266-278")[```rust
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
defaults give, for example, 1,024 matrix artifacts, 512 reachability results, 16,384 posting
chunks, and a per-cell quota of 8,192 entries (`metrics.rs:27-50`). The small caches use only an
entry cap; the ones that hold large values (matrix adjacency, GraphBLAS, posting chunks,
materialized supernodes) also carry a byte budget from `GraphMemoryConfig`, whose defaults are
64 to 128 MiB each (`config.rs:164-175`). A `low_memory` profile (`config.rs:178`) shrinks those
budgets for constrained deployments.

#why[
  Two budgets exist because entries and bytes fail differently. A cache of parsed queries could
  hold millions of tiny entries, so it needs a count cap. A cache of hydrated adjacency could
  blow out memory with a handful of huge entries, so it needs a byte cap. Bounding only one would
  leave the other failure mode open, so the caches that can hit both bound both.
]

== Pinning the expensive results

Some cached values are far more expensive to rebuild than others. A matrix artifact for a graph
with millions of edges, or the posting for a supernode with tens of thousands of neighbors, took
real work to compute. turbolay pins those so they survive eviction unless the cache is truly out
of room.

#term("Pinning")[
  Marking a cache entry as protected so it is only evicted when there is no unpinned entry left
  to drop. turbolay pins the entries that are most costly to recompute: large matrix artifacts
  and high-degree supernodes. The eviction loop always drops unpinned entries first.
]

The decision is a simple threshold on size or degree:

#srcblock("src/core/metrics.rs:58-64")[```rust
pub(crate) fn pin_matrix_artifact(&self, artifact: &engine::MatrixArtifact) -> bool {
    artifact.edge_count >= self.pin_matrix_min_edges          // default 1_000_000
}

pub(crate) fn pin_supernode_group(&self, group: &engine::SupernodeGroup) -> bool {
    group.degree >= self.pin_supernode_min_degree             // default 10_000
}
```]

So a million-edge artifact and a ten-thousand-degree supernode are pinned by default. Smaller
results are cheap enough to recompute that letting them age out is fine.

== Why the caches are always correct

This is the most important section of the chapter, and it is short, because the design does the
work. A cache is dangerous when it can serve stale data. turbolay's result caches cannot, and the
reason is the epoch discipline from every earlier chapter.

#term("Epoch-keyed invalidation")[
  The technique of putting the version (epoch) into the cache key, so that a new version is a new
  key rather than an overwrite of an old one. There is no explicit "invalidate this entry on
  write" step. A write advances the epoch; reads at the new epoch use new keys and miss the
  cache; entries for old epochs are never served to a new-epoch read and simply age out under
  LRU.
]

Recall that every content-dependent cache key embeds an epoch: `ReachabilityCacheKey` carries
`read_epoch`, the relationship-rows keys carry `read_epoch`, and the artifact and supernode keys
carry `base_epoch` (Chapter 2, Section on read-side caching; the key structs are in `cache.rs`
and `lib.rs`). Put that together with the write and delete chapters:

+ A write or delete advances the cell's epoch and appends a delta at the new epoch.
+ The next read pins the new epoch, so it builds cache keys with the new epoch.
+ Those keys are not in the cache, so the read misses and computes fresh, correct results.
+ The old-epoch entries are still in the cache but can never match a new-epoch key, so they are
  never served incorrectly. They sit idle and are eventually evicted as least-recently-used.

There is one deliberate exception, the parsed-query cache, whose key is only the query string
because a parse result does not depend on graph contents. Everything that does depend on contents
is epoch-keyed.

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
#figcap[Epoch-keyed invalidation. A write does not touch the cache at all. The new epoch produces new keys, so the reader misses old entries automatically and the stale ones age out.]

#why[
  This is why the write and delete chapters never mentioned invalidating caches: there is no
  invalidation code, because there is nothing to invalidate. Explicit invalidation is a classic
  source of bugs, a write forgetting to clear one of ten caches. turbolay sidesteps the whole
  class of bug by making staleness unrepresentable: a stale entry has a key no current read will
  ever construct. It also plays perfectly with garbage collection, because once GC removes the
  history below a watermark, no read is allowed below that epoch anyway, so the corresponding old
  cache entries are already unreachable.
]

== Multi-tenancy and metrics

Because one process serves many cells, the caches are shared, and a single busy tenant could
otherwise crowd everyone else out. The per-tenant quota, `max_entries_per_cell` (default 8,192),
bounds how many entries any one cell may hold, enforced by `enforce_tenant_quota`
(`cache.rs:320`) which evicts within the offending tenant before falling back to a global
eviction. Every cache operation is counted through `GraphCacheMetrics` (hits, misses, insertions,
evictions, pinned insertions, tenant-quota rejections) and exposed per kind through
`GraphCacheKind`, so operators can see hit rates and whether a cache is thrashing.

== Recap, and the end of the tour

The caches are the last piece of the machine, and they close the loop the whole book has traced:

- The object store is the durable, slow source of truth (Chapter 0).
- SlateDB's foyer-backed block cache keeps recently touched bytes on local disk so the engine
  rarely re-fetches from the object store.
- Above it, ten `BoundedGraphCache` instances keep computed results (parsed queries, reachability
  sets, matrix artifacts, supernode postings) so the read path rarely recomputes.
- Both budgets, entries and bytes, are bounded; the expensive results are pinned; a per-tenant
  quota keeps cells fair.
- Correctness is free, because every content-dependent key embeds an epoch, so a write simply
  produces new keys and old entries age out without any invalidation step.

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
