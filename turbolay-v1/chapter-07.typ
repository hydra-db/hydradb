#import "../book/vendor/bookly/src/bookly.typ": *
#import "../book/template.typ": term, why, srcblock, figcap, accent, muted

= Everything Fast Is Allowed to Disappear

The architecture is now correct without a warm compute node. Durable records
survive replacement, cell-local transactions publish complete changes,
snapshots reconstruct named epochs, and explicit distributed legs preserve
their local guarantees.

But a correct graph that fetches and decodes remote state for every repeated
question is not yet a practical graph database.

Caching closes the performance loop. It also creates the book's final
correctness question: how can a node remember expensive work without making
that memory part of the graph's truth?

== Problem 1: object-store latency appears at two different layers

A repeated query can waste work in two distinct ways:

1. SlateDB may fetch the same remote storage blocks again.
2. turbolay may decode or recompute the same graph structure again.

One cache cannot efficiently solve both. Raw blocks and hydrated adjacency
have different keys, sizes, lifetimes, and eviction policies.

turbolay therefore has two cache layers.

#figure(
  table(
    columns: (1.2fr, 1.4fr, 1.45fr),
    inset: 8pt,
    align: (left + top, left + top, left + top),
    table.header([*Layer*], [*Stores*], [*Miss behavior*]),
    [Graph result caches], [Parsed plans, artifacts, hydrated adjacency, reachability, relationship rows, postings, supernodes], [Reload or recompute from durable graph state],
    [SlateDB object-store cache], [Raw storage and SST blocks near the process], [Fetch bytes from the object store],
    [Object store], [Durable database records and artifacts], [Authoritative fallback],
  ),
  caption: [The upper cache avoids graph work; the lower cache avoids remote byte transfer.],
)

#term("Block cache")[
  A local cache of raw storage blocks used by SlateDB. It reduces repeated
  object-store reads without understanding edges, epochs, or query plans.
]

#term("Graph result cache")[
  A turbolay-owned cache of decoded or computed graph values. Its entries know
  graph identities such as cell, edge type, vertex, read epoch, or artifact
  base epoch.
]

#boxeq[
  *Block caching remembers bytes; graph caching remembers interpretations of
  those bytes.*
]

== Problem 2: the local disk must remain disposable

SlateDB is built with its `foyer` feature, and `GraphCacheConfig` passes a
cache directory, byte limit, put policy, and optional SST preload into the
storage settings.

#srcblock("src/core/config.rs:76-82")[```rust
pub struct GraphCacheConfig {
    pub object_store_cache_dir: Option<PathBuf>,
    pub object_store_cache_bytes: Option<usize>,
    pub object_store_cache_puts: bool,
    pub preload_sst_on_startup: bool,
}
```]

Preloading trades startup work for lower first-query latency. Caching puts can
let a writer reuse locally available blocks. A read-only database handle
disables cache puts because it does not produce writes.

None of these options changes the durable address of a cell. If the cache
directory is deleted, the next reads fetch blocks again from the object store.
If a replacement node uses a different disk, it opens the same scoped database
path and begins cold.

#why[
  The easiest test for a safe local disk cache is destructive: erase it. If the
  graph's meaning changes, the directory was not a cache. If only warm-up and
  latency change, it remains on the acceleration side of the architecture.
]

Local disk is therefore useful but never inherited as ownership. Chapter 2's
failover sequence moves placement and leases, not cache files.

== Problem 3: computed graph values can consume unbounded memory

The graph layer caches several kinds of results:

- matrix manifests and hydrated adjacency;
- optional compiled GraphBLAS matrices;
- parsed row queries;
- reachability results;
- relationship and relationship-property rows;
- supernode groups, posting chunks, and materialized supernodes.

These values vary enormously. A parsed query may be small. A hydrated matrix
or high-degree supernode may occupy megabytes. Limiting only the number of
entries lets a few large values exhaust memory; limiting only bytes lets a
huge number of tiny entries consume bookkeeping and lookup cost.

`BoundedGraphCache` supports both limits:

#srcblock("src/core/cache.rs:182-201")[```rust
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

#term("Resident-byte budget")[
  The maximum estimated memory retained by a cache. It complements the entry
  count because a small number of graph values can still be very large.
]

The cache policy controls entry caps, pinning thresholds, per-cell quotas, and
hydration concurrency. `GraphMemoryConfig` controls byte budgets for the large
representations and offers a smaller `low_memory` profile.

#figure(
  table(
    columns: (1.2fr, 1.3fr, 1.45fr),
    inset: 8pt,
    align: (left + top, left + top, left + top),
    table.header([*Resource risk*], [*Control*], [*Example*]),
    [Too many small values], [Entry limit], [Parsed queries or relationship result sets],
    [A few huge values], [Resident-byte limit], [Hydrated adjacency or GraphBLAS matrices],
    [One cell crowds out others], [Per-cell entry quota], [A hot tenant generating many distinct keys],
    [Too many simultaneous rebuilds], [Hydration and compilation semaphores], [Cold-start artifact hydration],
  ),
  caption: [Capacity is bounded along count, bytes, fairness, and concurrent work.],
)

== Problem 4: eviction should preserve the most useful work

When a cache exceeds its budget, it must choose what to forget. turbolay uses
a logical access clock to approximate least-recently-used eviction.

#term("Least recently used (LRU)")[
  An eviction policy that removes the eligible entry whose last access is
  oldest. It favors values showing recent reuse without requiring the engine
  to predict the future.
]

A `get` advances the clock and stamps the entry:

#srcblock("src/core/cache.rs:229-238")[```rust
pub(crate) fn get(&mut self, key: &K) -> Option<V> {
    self.clock = self.clock.saturating_add(1);
    let entry = self.entries.get_mut(key)?;
    entry.last_access = self.clock;
    Some(entry.value.clone())
}
```]

Some results are expensive enough to deserve additional protection. Matrix
artifacts above an edge-count threshold and supernode groups above a degree
threshold can be pinned. Eviction first looks for an unpinned LRU entry; only
when no such entry remains may it evict pinned data.

#term("Pinned cache entry")[
  An entry protected from ordinary eviction because rebuilding it is expected
  to be unusually expensive. Pinning is a preference, not infinite capacity:
  pinned entries may still be evicted when budgets cannot otherwise be met.
]

This is an important limit. “Pinned” does not mean durable, authoritative, or
guaranteed resident. It only changes eviction order.

== Problem 5: shared caches need fairness between cells

One `GraphShard` process may serve work for many cells over time, and the cache
implementation tags entries with an owning tenant string. Without a quota, a
single cell issuing many distinct queries can evict every other cell's working
set.

`max_entries_per_cell` bounds retained entries for one cell. When a tenant
exceeds its quota, eviction first targets that tenant before enforcing the
global limit. The default policy sets the quota to 8,192 entries, but the
configuration—not the number—is the architectural point.

Fairness is enforced by admission and eviction, then observed through cache
metrics: hits, misses, insertions, evictions, pinned insertions, and quota
rejections.

#why[
  A global LRU answers “which entry is coldest?” but not “who consumed the
  cache?” A per-cell quota prevents one workload's key diversity from turning
  every other workload into a permanent cold start.
]

The quota counts entries rather than guaranteeing equal bytes per cell. Large
values remain governed by the cache's global byte budget. Production policy
may need stronger tenant-aware byte accounting if workloads demand it; the
current implementation should not be described as full memory isolation.

== Problem 6: stale cache entries must never become current truth

The classic cache invalidation approach tells every mutation path to clear
every derived value that might have changed. turbolay has many mutation paths
and many graph caches. Missing one invalidation would create a correctness bug.

Epoch-keyed caches avoid that dependency.

#term("Epoch-keyed cache")[
  A cache whose content-dependent key includes the `read_epoch` or artifact
  `base_epoch`. A newer graph version produces a different key, so an old
  entry cannot match a current lookup.
]

The sequence is:

1. A read at epoch 20 caches reachability under a key containing 20.
2. A write publishes epoch 21.
3. A read at epoch 21 constructs a key containing 21.
4. The epoch-20 result does not match, so the read recomputes or reloads.
5. The old entry remains valid for permitted epoch-20 reads and eventually
   ages out under LRU.

#figure(
  table(
    columns: (1.1fr, 0.35fr, 1.2fr, 0.35fr, 1.2fr),
    inset: 8pt,
    align: center,
    [read at 20], [`→`], [cache key `(…, 20)`], [`→`], [epoch-20 value],
    [write at 21], [`→`], [new key space], [`→`], [old value cannot match],
  ),
  caption: [Advancing the epoch changes cache identity without a global invalidation sweep.],
)

#boxeq[
  *A stale cache entry is safe when no current read can construct its key.*
]

The parsed-query cache remains the exception because parsing does not depend
on graph content. Matrix manifest and hydration caches key by base epoch;
reachability and row-result caches key by read epoch.

Epoch identity does not remove the need for retention awareness. If history
for epoch 20 is no longer permitted, callers should not be able to create new
epoch-20 work merely because a cache happens to retain an old result. The
storage/read admission rules remain authoritative.

== Problem 7: a cache stampede can defeat a correct cache

After failover, many requests may miss the same cold matrix or supernode at
once. If every request independently hydrates it, the node multiplies object
store traffic and memory pressure precisely when it is least prepared.

The shard bounds concurrent hydration and matrix compilation using semaphores.
Writes, artifact builds, and GC have their own gates. This separates resource
classes so a burst in one kind of work is visible and constrained.

#term("Cache stampede")[
  Many concurrent misses for the same or similarly expensive data, causing
  duplicate reconstruction work and a spike in downstream load.
]

The current gates bound concurrency but are not a universal per-key
single-flight mechanism. Several admitted requests can still perform related
work. The guarantee is bounded pressure, not perfect deduplication of every
miss.

Preloading the disk cache and prewarming selected artifacts can reduce the cold
window, but they move work into startup. Operators must choose whether fast
readiness or fast first-query latency matters more for a deployment.

== Problem 8: recovery should restore truth before speed

Now return to the failure that opened Chapter 1. A compute node disappears.
The replacement has no parsed plans, no hydrated matrices, no reachability
results, and perhaps an empty local disk cache.

Recovery proceeds in two phases:

1. *Correctness recovery*: resolve placement, obtain current authority when a
   writer is needed, open the same object-store cell path, read durable epochs,
   manifests, canonical records, and deltas.
2. *Performance recovery*: fetch blocks, hydrate graph structures, rebuild
   caches, compile optional matrices, and learn the new working set.

#figure(
  table(
    columns: (1.2fr, 1.45fr, 1.4fr),
    inset: 8pt,
    align: (left + top, left + top, left + top),
    table.header([*Recovery item*], [*Source*], [*If initially absent*]),
    [Graph meaning], [Canonical SlateDB records in object storage], [Cannot serve the graph correctly],
    [Current ownership], [Placement, lease, and durable fence], [Writer must remain disabled],
    [Snapshot history], [Artifacts, deltas, retention metadata], [Historical reads may be impossible],
    [Raw block cache], [Refetched object-store bytes], [Higher I/O latency],
    [Graph result caches], [Rehydration and recomputation], [Higher CPU and query latency],
  ),
  caption: [Durable mechanisms restore service; caches restore pace.],
)

This ordering is the operational meaning of replaceable compute. The node may
be slow while cold, but it is not allowed to invent state from an incomplete
cache or treat cached ownership as current authority.

== The complete performance model

For a repeated query on a warm node:

1. Reuse the lowered plan if its pure query-text key matches.
2. Look for an epoch-correct graph result or artifact.
3. On a graph-cache miss, hydrate or compute under bounded concurrency.
4. Let SlateDB satisfy repeated storage blocks from its local cache when
   available.
5. Fetch missing durable bytes from the object store.
6. Insert the derived result subject to cell quota, entry count, byte budget,
   pinning policy, and LRU eviction.
7. Record hits, misses, insertions, evictions, and pressure signals.

#figure(
  table(
    columns: (1.15fr, 1.35fr, 1.5fr),
    inset: 8pt,
    align: (left + top, left + top, left + top),
    table.header([*Question*], [*Mechanism*], [*Invariant*]),
    [Can the value be reused?], [Semantic cache key with epoch where required], [A hit belongs to the requested version],
    [Can it fit?], [Entry and resident-byte budgets], [Memory remains bounded],
    [Can one cell dominate?], [Per-cell quota], [Shared cache has basic fairness],
    [What leaves first?], [LRU with pinned preference], [Recent and expensive work is favored],
    [What happens on loss?], [Durable fallback and bounded rehydration], [Miss changes latency, never graph meaning],
  ),
  caption: [Caching is safe when reuse, capacity, fairness, and fallback are all explicit.],
)

The final architectural claim is:

#boxeq[
  *Every acceleration layer must be bounded while present, correct when reused,
  and harmless when lost.*
]

== What the cache architecture guarantees—and what it does not

The implementation provides:

- a configurable SlateDB object-store block cache;
- bounded graph result caches for plans, artifacts, traversal, relationships,
  postings, and supernodes;
- entry-count and resident-byte limits where representations require them;
- per-cell entry quotas and LRU-style eviction;
- preferential retention of expensive pinned artifacts and supernodes;
- epoch-aware keys for graph-content-dependent values;
- metrics and concurrency gates for cache and hydration pressure.

It does not promise:

- warm latency immediately after failover;
- that local disk or memory survives node replacement;
- that pinned entries can never be evicted;
- perfect per-cell byte isolation;
- that every concurrent miss is deduplicated into one reconstruction;
- that a cache entry can override retention, lease, fence, or durable storage
  rules.

== The book's complete mental model

The seven chapters now form one chain:

1. Durable object storage keeps graph meaning independent of one compute node.
2. Scopes and cells make identity, placement, and ownership explicit.
3. One authorized cell-local transaction publishes a complete mutation.
4. A pinned epoch, eligible artifact, and ordered delta overlay reconstruct one
   coherent snapshot.
5. Cypher lowering and bounded physical plans turn that snapshot into local
   graph operations.
6. Explicit cell legs distribute known work without inventing a global
   transaction.
7. Disposable caches make repeated execution practical without becoming
   authoritative.

#figure(
  table(
    columns: (1.05fr, 1.35fr, 1.55fr),
    inset: 8pt,
    align: (left + top, left + top, left + top),
    table.header([*Boundary*], [*What crosses it*], [*Promise*]),
    [Object store], [Canonical records and published artifacts], [Survives compute loss],
    [Cell], [Ownership, epoch, lock, fence, transaction], [Local coordination and atomicity],
    [Snapshot], [Artifact base plus bounded overlay], [One named cell version],
    [Query engine], [Supported lowered plan and budgets], [Equivalent physical paths],
    [Coordinator], [Explicit named cell legs], [Bounded scatter/gather],
    [Cache], [Versioned derived values and raw blocks], [Acceleration that may disappear],
  ),
  caption: [Each boundary has one job and a deliberate stopping point.],
)

turbolay is therefore not “a graph kept in S3” as a slogan. It is a set of
proof obligations around remote durability:

- truth must outlive compute;
- ownership must expire and be fenced;
- one mutation must publish all of its meaning together;
- one read must name and reconstruct one version;
- every optimizer and cache must have a correct durable fallback;
- distribution must not claim a global guarantee it has not coordinated.

== Revision notes

=== The ideas to remember

- *There are two cache layers.* SlateDB caches bytes; turbolay caches decoded
  and computed graph values.
- *Local disk remains disposable.* Losing it causes remote reads, not graph
  loss.
- *Count and bytes are separate limits.* Small entries and huge values create
  different memory failures.
- *Pinning changes preference, not truth.* A pinned artifact is still a cache
  entry and may be rebuilt.
- *Epoch keys avoid stale reuse.* New graph versions construct new keys rather
  than relying on every writer to clear every cache.
- *Fairness and hydration are bounded.* Per-cell quotas and semaphores constrain
  shared-node pressure.
- *Recovery restores truth before speed.* Ownership and durable state come
  first; warm caches follow.

=== A quick correctness test

1. If this cache is erased, can durable state reconstruct its value?
2. Does its key include every semantic input, including the epoch when needed?
3. Are both entry count and large-value memory bounded appropriately?
4. Can one cell crowd all other cells out?
5. Does pinning remain subordinate to hard capacity limits?
6. Are cold misses admitted under bounded hydration work?
7. Can any cached value bypass current ownership, retention, or snapshot checks?

#boxeq[
  *The graph survives because truth is durable, scales because coordination is
  cell-local, stays coherent because versions are named, and becomes fast
  because everything expensive may be remembered—but nothing remembered is
  allowed to become truth.*
]
