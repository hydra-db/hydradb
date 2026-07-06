---
title: "turbolay — Graph Database on S3"
status: draft
date: 2026-07-03T00:00:00Z
related:
  - goals.md
  - rfcs/0000-rfc-index.md
---

# turbolay — Graph Database on S3 — Design & Build Plan

## 1. What we're building

An **object-native property-graph database** where **S3 is the storage layer and we are the compute layer**. It reimplements Dgraph's proven graph-on-KV storage model (posting lists, predicate sharding, indexes-as-posting-lists) **in Rust on top of [SlateDB](https://slatedb.io)** — a log-structured KV store whose WAL, SSTs, and manifest all live on object storage.

The bet: Dgraph already proved you can build a scalable graph engine where *the value at each key is a compressed sorted set of vertex IDs (a posting list) and a graph edge is just a member of one of those sets*. Dgraph runs that model on Badger + Raft + a Zero timestamp oracle. We keep the model and **delete the distributed-consensus half** by making SlateDB (single-writer, object-native, self-fencing) do the heavy lifting.

Two user-facing surfaces:

1. **Graph writes**: upsert nodes (with labels + arbitrary properties) and typed directed edges (with properties), via a simple JSON API. Every durable write returns a monotonic session token.
2. **openCypher reads**: a scoped read subset — `MATCH` node/edge patterns, `WHERE` filters, fixed and variable-length hops (`-[:REL*1..3]->`), `RETURN` / `ORDER BY` / `LIMIT` — planned onto posting-list intersections and adjacency scans.

### Non-goals (v0)

- **Multi-writer per namespace.** Single writer per namespace is a core simplifying invariant — it is what lets us delete Dgraph's Zero oracle and conflict OCC.
- **Cypher writes** (`CREATE` / `MERGE` / `SET` / `DELETE`). Writes are a JSON upsert API in v0; Cypher mutation is a later RFC.
- **Distributed query execution / cross-namespace traversal.**
- **CSR adjacency, WAND/leapfrog joins, bitpacked posting frames.** Designed-for (block-max metadata written from day one) but not built — deferred to the optimization ledger.
- **Vector / geo / full-text indexes.** The index framework should make them addable; not in v0 scope.

### Scale target

POC-grade: a **RAG knowledge graph** (`Source → Chunk → Entity`, `RELATES` edges), **1–10M nodes per namespace**, correctness and clean architecture first ("earned complexity"). Correctness is shadow-tested against FalkorDB with set-diff. The posting-list model scales past 100M with the deferred optimizations; we don't tune for it yet.

## 2. Design principles

1. **Object-native, disaggregated.** All durable state lives on S3. Compute nodes are stateless caches over it and can be killed/replaced at will.
2. **Namespace = shard = tenant = graph.** One S3 prefix per namespace, one writer per namespace. No cross-namespace coordination. Multi-tenancy and isolation for free.
3. **SlateDB is the substrate; CAS-on-manifest is the only coordination primitive.** SlateDB's manifest writer-epoch (an S3 conditional PUT) fences zombie writers. No Raft, no ZooKeeper, no DynamoDB, no external lock service. This is exactly what replaces Dgraph's Zero oracle + Raft groups.
4. **Write path / read path split.** Writes durably commit node/edge/changelog keys in one atomic `WriteBatch`; secondary indexes and reverse adjacency are maintained by the same writer, but reads never depend on an index being caught up. Reads = **posting-lists/indexes up to a watermark + materialized changelog tail (watermark → now), merged.** Strong consistency always; index lag never observable.
5. **Extension-first index framework (Postgres access-method model).** The engine core knows about nodes, edges, a changelog, and an `IndexAm` trait. Every index type (value/eq, range, reverse, count, later fulltext/vector/geo) is an extension that owns its keyspace, its build function, and its query operators. New index types plug in without touching the core; the read path is uniformly "index + changelog tail."
6. **Port the model, not the plumbing.** Dgraph's `x/keys` (order-preserving key layout) and `posting` (list model, predicate sharding, split/rollup) are Badger- and Raft-agnostic and port almost verbatim. We keep that *model* but use **roaring** for the encoding and set algebra — Dgraph's `codec` (UidPack) and `algo/uidlist` (sorted-UID intersect/merge/difference) are *replaced*, not ported (RFC 0005), as are Badger's value-log/MVCC and Zero's oracle.

## 3. Substrate decision: SlateDB (v0.14.1)

We build on SlateDB rather than rolling our own LSM-on-S3 (the path namidb and turbolay-v0 took — both built a graph-aware SST format, which is precisely the work we avoid). Full rationale is RFC 0002; the mapping of our requirements to SlateDB features:

| Our requirement | SlateDB feature (v0.14.1) |
|---|---|
| Everything on S3 | Zero-disk architecture: WAL SSTs, L0, sorted runs, manifest all under one S3 prefix (`object_store` crate; S3/GCS/Azure/MinIO/local) |
| Single writer + zombie fencing via CAS | Manifest `writer_epoch` CAS'd on open; a fenced writer fails all ops with `CloseReason::Fenced`. Native conditional PUTs — no external coordinator |
| Adjacency = one KV value; fast point read | Ordered byte keys + `get` served from memtable / foyer block cache / bloom-filtered SST. This is the exact contract Dgraph relies on from Badger |
| Predicate-ordered range scans / index scans | `scan` / `scan_prefix` (ascending or descending); `DbIterator::seek` for block-skipping intersection |
| Atomic multi-key edge + index + changelog write | `WriteBatch` — the whole batch commits at one sequence number |
| Degree counters, deleted-bitmaps, adjacency-set append without RMW races | `MergeOperator` (associative), applied lazily at read/flush/compaction. Route by record-type (opendata's pattern) |
| Stateless reader fleet reading posting-lists + changelog tail | `DbReader`: read-only open from another process; polls the manifest (`manifest_poll_interval`) and replays newer WAL — principle #4 already implemented at the KV layer |
| Session token / read-your-writes | `WriteHandle::seqnum()` + `WriteOptions.seqnum` (inject our own logical seq) + `DbReader::subscribe()`/`durable_seq` gate |
| Compaction off the write path | Embedded, standalone, or distributed CAS-claimed workers |
| Cheap graph snapshots / branch-and-query | O(1) checkpoints + clones (fork a namespace at a point in time) |
| Low-latency WAL | Separate `wal_object_store` → WAL on S3 Express One Zone (~5–10ms) while bulk SSTs live on S3 Standard |

### Constraints we accept (permanent v0 inputs, not open questions)

- **Bytewise-only key ordering.** SlateDB has no custom comparators. Mitigated by order-preserving key encodings (RFC 0003) — big-endian ints, sign-flipped floats, escaped/terminated variable-length components.
- **Pre-1.0 API churn.** Storage format is stable across adjacent versions; the Rust API is not. Isolated behind opendata's `common::Storage` wrapper (never touch `slatedb::Db` directly).
- **No key-value separation.** Large values are rewritten whole by compaction. v0 caps a node's total property size and rejects oversize writes (RFC 0004); spill-to-raw-S3 is a backlog item. Note this is exactly why Dgraph splits posting lists at 512 KiB — we keep that.
- **Non-associative posting maintenance is single-writer RMW, not merge.** Posting-list split and rollup aren't associative, so the single writer applies them as read-modify-write. `MergeOperator` is reserved for genuinely associative state (deleted-bitmap union, degree counters, ordered-set append).

## 4. Graph → KV mapping (the core, from Dgraph)

The entire storage model reduces to one sentence: **turbolay is a predicate-sharded KV store where the value at each key is a compressed sorted set of UIDs (a posting list), and a graph edge is a member of one of those sets.**

### 4.1 Identity

- Nodes and edges have **internal `u64` UIDs**, allocated crash-safely by `common::SequenceAllocator` (block-reserved, monotonic across restarts). Internal u64s are what make roaring compression and cheap set math work; UUIDs would destroy them (namidb hit exactly this and paid for it).
- Users address nodes by an **external id** (`xid`, an arbitrary string). A mapping index `xid → uid` (itself a posting list / KV entry) translates. Dgraph does exactly this.

### 4.2 Keys (RFC 0003)

Order-preserving, big-endian, built on opendata's `common::serde` toolkit (`KeyPrefix` 2-byte subsystem+version, `terminated_bytes` for variable-length components, `var_u64`, `sortable`). Subsystem byte `GRAPH = 0x05`. A record-type tag distinguishes:

```
Node:        [GRAPH|ver] [type=Node]     [uid:8]                     -> node record (labels + property blob)
Edge (out):  [GRAPH|ver] [type=EdgeOut]  [src_uid:8][pred][sortkey]  -> posting list of dst uids
Edge (in):   [GRAPH|ver] [type=EdgeIn]   [dst_uid:8][pred][sortkey]  -> posting list of src uids
Value index: [GRAPH|ver] [type=Index]    [pred][token]              -> posting list of node uids
Reverse idx: (EdgeIn is the reverse projection; materialized, not lazy)
Count index: [GRAPH|ver] [type=Count]    [pred][degree:4]           -> posting list of node uids
Xid map:     [GRAPH|ver] [type=Xid]      [xid_terminated]           -> uid:8
Changelog:   [GRAPH|ver] [type=Log]      [seq: var_u64]             -> change record
Meta:        [GRAPH|ver] [type=Meta]     ["latest_seq" | counters]  -> value
```

All edges from one `(src, predicate)` share one key; their destination UIDs are packed together in the value — one posting list. This is what makes an edge cost ~1–2 bytes amortized.

### 4.3 Posting lists (RFC 0005)

- Value = a roaring **`RoaringTreemap`** (64-bit) of member UIDs; set ops (AND/OR/NOT, cardinality, min/max) come free from the library. Members that carry a scalar value or edge properties (facets) store them in a companion `EdgeProp` record (RFC 0004); a plain UID edge with no properties lives *only* in the set. This **replaces Dgraph's UidPack codec and `algo/uidlist` engine** with roaring; UidPack/CSR stay a deferred format behind the value's `format` tag (RFC 0009/0010).
- **Split at 512 KiB**: a supernode's posting list is bin-split at the median-cardinality pivot into multiple part-keys. On S3 this is *simpler* than Badger — new parts are just new keys, no in-place rewrite (per the fundamentals book's ch18 synthesis).
- **CSR-ready**: the value carries a 1-byte `format` tag and per-part min/max/card skip metadata from day one, so a future UidPack/CSR/leapfrog-join RFC is an additive format bump.

### 4.4 Bidirectional storage

Every edge is written twice — `EdgeOut` keyed by source, `EdgeIn` keyed by destination — in the same atomic `WriteBatch`. Reverse traversal ("who points at me?") is then a symmetric posting-list read, not an O(E) scan. 2× write amplification is the accepted, standard trade (JanusGraph, namidb, Dgraph `@reverse` all do it; we make it unconditional).

### 4.5 Indexes are posting lists too (RFC 0006)

- **Value/eq index**: tokenize a property value (`exact`, `term`, `hash`, order-preserving `int`/`float`) → `Index[pred][token]` → posting list of node UIDs. Range predicates (`>`, `<`) become key-range scans because numeric tokens are order-preserving.
- **Count index**: `Count[pred][degree]` → nodes with exactly that out-degree; degree filters skip list scans.
- **Lossy tokens** (e.g. `term`) set a re-fetch flag: the planner materializes candidates and re-checks the real value.

## 5. Consistency (RFC 0001)

Single writer per namespace + SlateDB's durable WAL gives us strong consistency without a coordinator.

- Every durable write returns a **session token** = our own logical seq. The writer keeps a `next_seq` counter, writes `m/latest_seq` into every `WriteBatch`, and can inject it as SlateDB's `WriteOptions.seqnum`. Single-writer monotonicity makes this safe.
- A reader **must not answer a query carrying token T** until its replayed durable state shows `m/latest_seq >= T` (gate via `DbReader::subscribe()` on `durable_seq`). Guarantees read-your-writes across a stateless reader fleet.
- **No token** → bounded staleness (up to `manifest_poll_interval`). **Strict mode** → bounded by poll interval / reader reopen until SlateDB grows a public refresh API (we won't patch SlateDB).
- **Index lag is invisible.** Reads use posting-lists/indexes up to `W = min(watermark over used indexes)`, then scan the changelog tail `(W, latest]` and evaluate the full pattern on materialized nodes/edges, then merge. Same plan as the sister FTS project; proven.

We do **not** need Dgraph's Zero oracle, `start_ts`/`commit_ts` assignment, conflict-key OCC, or version-in-key MVCC — those exist only to serialize *concurrent distributed writers*. One writer per namespace serializes writes by construction.

## 6. Reader / writer separation (RFC 0008)

Four layers, following opendata:
1. **Storage**: writer = `common::SlateDbStorage` (fenced `Db`); reader = `SlateDbStorageReader` (unfenced `DbReader`, manifest-polling). Many readers coexist with one writer.
2. **Trait**: read code takes `Arc<dyn GraphRead>`; write code takes the RW `GraphDb`.
3. **Service**: `GraphDb` (RW, single writer) vs `GraphDbReader` (RO), sharing a `GraphRead` trait.
4. **Deployment**: same binary, `--role writer` registers write routes, `--role reader` serves a fleet against the same S3 prefix. Readers scale independently of the writer (goal #2).

## 7. Query surface (RFC 0007)

v0 = an **openCypher read subset** compiled to posting-list operations:

- `MATCH (a:Label {prop: v})-[:REL*1..k]->(b) WHERE b.x > 3 RETURN b.y ORDER BY b.z LIMIT n`
- Node anchor by label/property → value-index posting-list lookup. Each hop → adjacency posting-list read + sorted-UID intersection with any filter posting lists (`AND`=intersect, `OR`=merge, `NOT`=difference — Dgraph's `algo`, ported). Variable-length paths → bounded BFS over adjacency reads, dedup by UID.
- The **N+1 problem** is the dominant cost; mitigations: batch neighbor reads, push filters down before expanding the next frontier, apply the most selective anchor first.
- A small internal **predicate IR** sits between the Cypher frontend and the executor, so a later full-Cypher or GQL frontend is a frontend swap, not a rewrite.

## 8. Correct-first ledger (deferred, with triggers)

| Deferred | v0 does instead | Picked up when |
|---|---|---|
| CSR adjacency + WCOJ/leapfrog joins | posting-list reads + sorted-UID intersect | traversal latency measured too high on real S3 |
| Bitpacked posting frames, UidPack | roaring posting lists | posting decode shows up in profiles |
| Block-max WAND / MAXSCORE | exact term-at-a-time intersect | headers written from day one (RFC 0005) |
| Vacuum / dead-UID purge from posting lists | deleted-bitmap filtering (roaring, merge-union) | bitmap cardinality grows |
| Cypher writes (CREATE/MERGE/SET) | JSON upsert API | after read path is proven |
| Full openCypher (WITH, aggregation, subqueries) | scoped read subset | product need; IR swap |
| Locality-aware partitioning | hash / uid order + block cache | cold first-hop latency measured bad |
| Standalone/distributed compaction | embedded in writer | config flip, no RFC |
| Oversized-node spill to raw S3 | reject over the cap | cap proves wrong on real workloads |

## 9. Prior art (what we're standing on / diverging from)

- **Dgraph** — the model we port (posting lists, predicate sharding, indexes-as-posting-lists, split/rollup). We use roaring for the encoding/set-algebra, **not** its UidPack codec or `algo/uidlist` engine (RFC 0005). We drop its Raft/Zero/Badger-MVCC.
- **The fundamentals book** (`../fundamentals-of-graph`) — ch13–18 give the posting-list model and an explicit Dgraph→S3 "Borrow / Rework / Avoid" synthesis. The two S3-specific failure modes to design against: **tombstone storms** and **tiny-object storms** (never one S3 object per edge — posting lists live inside SlateDB SSTs holding many edges).
- **opendata** (`../../2026-06/opendata`) — the `common` substrate we reuse (serde toolkit, StorageBuilder, merge-operator framework, SequenceAllocator, reader/writer split, RFC template, test harness). We add `SUBSYSTEM GRAPH=0x05`.
- **namidb / turbolay-v0** — prior graph-on-S3 attempts that chose **CSR** over posting lists. Their lesson we heed: benchmark against **real S3, not LocalStack** (LocalStack hid a ~10× cold regression). Their divergence from us: they built their own SST format; we let SlateDB own it. CSR remains our documented optimization path, not our v0.

## 10. Build milestones

- **M0** — substrate wrapper on `common`, keyspace + order-preserving encoding (RFC 0003), UID/xid allocation, property tests.
- **M1** — node/edge write path: atomic `WriteBatch` (node + out/in edges + changelog + `m/latest_seq`), posting-list encode/split (RFC 0004/0005), session token, crash-recovery test.
- **M2** — index framework + value/reverse/count indexes (RFC 0006); read path with index-watermark + changelog-tail merge; consistency tests.
- **M3** — openCypher read subset + planner (RFC 0007); HTTP service + reader/writer fleet (RFC 0008); shadow-test vs FalkorDB.
- **Observability (RFC 0017)** spans M0–M3: instrumented object store, phase timers, invariant counters; gates any optimization RFC.

**Parallel track (front-loaded during M1) — Cypher frontend.** The openCypher **parser + AST + lowering-to-IR** is storage-independent (it stops at the predicate IR seam, RFC 0007 §3), so it is built alongside M1 rather than waiting for M3. Per Q23 (RFC 0007 §13) the parser covers the **full** openCypher grammar; the v0 subset is enforced at lowering (`unsupported_cypher` for out-of-v0 constructs), which removes the parser from RFC 0013 and makes the full-Cypher swap a lowering+executor change. Only the planner/executor *below* the IR wait on M1/M2. Parser build (Q23a) is resolved: **adopt `decypher`**, vendored as a local sibling crate (`../decypher`, cloned with upstream remote, path-dep'd, modifiable locally) for its full-grammar AST + `miette` diagnostics — vendoring absorbs the alpha/unstable-AST risk. Our **lowering** (decypher AST/HIR → predicate IR) is the only coupling surface and is where the v0 subset gate lives (`unsupported_cypher`).

The binding specifications are the RFCs in `docs/rfcs/`, starting at `0000-rfc-index.md`. This document is the narrative overview; where the two disagree, the RFCs win.
