---
title: "turbolay — Open Decisions Register"
status: awaiting-decisions
date: 2026-07-03T00:00:00Z
related:
  - rfcs/0000-rfc-index.md
---

# Open Decisions Register

Every design fork where I picked a **best-judgment default** while drafting, plus the ones still open. Nothing here is final — the RFCs currently reflect the "Recommended" column, but each is a live choice. **Please rule on each (confirm / veto / discuss); the dependent RFCs update.**

Format per decision: **the question**, the options, my **recommendation + why**, and **what it touches**. Grouped by theme. Decisions already written into RFCs are marked `[in RFC]`; genuinely still-open ones are marked `[OPEN]`.

Answer inline (e.g. "Q1: option A, but ..."), or we walk them live.

## Decisions log
- 2026-07-03 — **Q1 = A** (posting-lists on SlateDB, CSR-ready).
- 2026-07-03 — **Q2 = A** (roaring bitmaps; skip UidPack + algo port; unify with deleted-uid bitmaps).
- 2026-07-03 — **Q3 = A** (openCypher read subset + JSON writes; Cypher writes → RFC 0011).
- 2026-07-03 — **Q17 = A** (variable-length paths via bounded BFS + depth cap + uid dedup).
- 2026-07-03 — **Q9 = A** (per-(anchor,pred) deleted-edge roaring bitmap, subtract at read, fold at rollup).
- 2026-07-03 — **Q10 = A** (size-adaptive add: roaring-union merge small, RMW at 512 KiB split).
- 2026-07-03 — **Q4 = defer** (pure posting-list, no sortkey; ordered adjacency in a dedicated "Deferred" section of RFC 0005 → RFC 0009).
- 2026-07-03 — **Q5 = confirmed** (dense internal u64 UIDs + external xid mapping).
- 2026-07-03 — **Q6 = confirmed** (monolithic node blob; property filtering via declared secondary value indexes — see RFC 0006).
- 2026-07-03 — **Q7 = confirmed** (bincode; rkyv fast-follow behind NodeCodec).
- 2026-07-03 — **Q8 = confirmed** (tombstone-and-filter + explicit DETACH DELETE).
- 2026-07-03 — **Q14 = A** (v0 tokenizers: exact, int, float, hash; datetime → int/epoch; term/trigram/fulltext/geo/vector deferred).
- 2026-07-03 — **Q16 = A** (always materialize out+in projections).
- Convention: for pure "confirm" items I proceed on the recommendation unless flagged (user: "yes unless stated otherwise").
- 2026-07-03 — **Q15 = A** (filter on non-indexed property → error by default + opt-in `brute_force` flag).
- 2026-07-03 — **Q-new (index declaration) = A** (explicit declaration via admin/REST API; writer backfills). FUTURE: (i) openCypher `CREATE INDEX FOR (n:Label) ON (n.prop)` / `DROP INDEX` as a frontend on the same registry (rides RFC 0011); (ii) user-driven field selection (user specifies fields to index — the admin API is that input today; later also via Cypher DDL or namespace config). Note both in RFC 0006 future section.
- 2026-07-03 — **Confirms taken (no objection):** Q11 session-token default, Q12 own logical seq, Q13 single-writer/no-MVCC, Q18 depend on opendata/common, Q19 one DB per namespace, Q20 single S3 store for WAL+data in v0, Q21 RAG-KG / 1–10M / correctness-first, Q22 benchmark on real S3.
- **All 22 decisions now settled.** Remaining work: draft RFC 0006, 0007, 0008; reconcile UidPack→roaring wording in 0000/0003.

---

## A. Architecture & storage core

### Q1 — Adjacency storage model `[in RFC 0000 D3, 0005 pending]`
How is a node's edge list physically stored?
- **(A) Posting-lists on SlateDB, CSR-ready** ← *recommend*. Adjacency = one KV value; neighbor read = one `get` served by SlateDB's cache + bloom filters (dgraph's contract on badger). Reserve block-header space so CSR is additive later. Fastest; matches your brief; lets SlateDB do the heavy lifting.
- (B) CSR materialized now (namidb/turbolay-v0 path) — better traversal locality + WCOJ joins, but you build a graph-aware materializer; bigger v0; contradicts "let SlateDB do the lifting."
- (C) Posting-lists, no CSR seams — simplest now, bigger change to add CSR later.
- **Why A**: your brief explicitly wants the dgraph shape with SlateDB doing the work. Prior art chose CSR *because they built their own SST format* — the exact lift we avoid. CSR stays the documented optimization (RFC 0009).
- **Touches**: RFC 0005 (the whole thing), 0007 read path, 0009.

### Q2 — Posting-list encoding `[OPEN — 0005]`
How is the sorted uid set inside a posting value stored, and how are AND/OR/NOT done?
- **(A) Roaring bitmaps (Treemap, 64-bit)** ← *recommend*. opendata already uses roaring for sets; intersect/union/difference come free from the library. **Skips porting dgraph's UidPack codec AND the intersection algos** — much less code, correctness-first.
- (B) Port dgraph UidPack (256-uid groupvarint blocks) + its intersect/merge/difference with block-skipping — battle-tested, best compression, block-max-ready; more porting now.
- (C) Plain sorted varint deltas + hand-written merge-joins — minimal code, you own the correctness.
- **Why A**: biggest work-saver in the whole design. Roaring is purpose-built for compressed sorted-int sets with fast boolean ops — literally graph adjacency + traversal intersection. UidPack's edge (compression %, block-max/WAND) only matters at the scale/latency we've deferred. We can swap to UidPack in the CSR/perf RFC if profiles demand it.
- **Risk to weigh**: roaring's 64-bit Treemap is a bit heavier than a dense-block UidPack; the deleted-uid bitmaps are already roaring, so this unifies the representation.
- **Touches**: RFC 0005, 0006 (index posting lists), 0007 (intersection).

### Q3 — Name encoding in keys `[in RFC 0003]`
Label/predicate/property names in keys: **(A) intern to u32 ids** (schema-preserving, compact keys, tiny cached schema table) ← *recommend* vs **(B) inline strings** (dgraph-style, no schema layer, bloats keys).
- **Why A**: fundamentals book + opendata both say schema-free "can double storage." Interning is a cache-hot lookup. Veto if you'd rather keep the dgraph-literal layout for fidelity.
- **Touches**: RFC 0003, 0004 (schema keyspace).

### Q4 — Sortkey / ordered adjacency `[in RFC 0003 — deferred]`
Pure posting-list model (**no** sortkey in the adjacency key; order by dst uid; `ORDER BY` on edge props = materialize-then-sort) ← *recommend* vs composite-edge-key (sortkey in key → natively-ordered adjacency, but one key per edge → tiny-object pressure).
- **Why pure**: composite-edge-key is the opposite storage model and multiplies key count by edge count. Ordered adjacency deferred to RFC 0009.
- **Touches**: RFC 0003, 0005, 0007 (ORDER BY).

---

## B. Data model & write path

### Q5 — UID model `[in RFC 0004/0005 D5]`
**(A) Dense internal u64 UIDs + external `xid` string mapping** ← *recommend* vs (B) UUIDs as ids.
- **Why A**: dense u64s are required for delta/offset compression and cheap set math; namidb's retro documents that UUID ids forced binary-searched vectors instead. Users still address by arbitrary `xid` (mapped `xid → uid`).
- **Touches**: RFC 0004 (allocation, xid index), 0005 (compression).

### Q6 — Node property layout `[in RFC 0004]`
**(A) Monolithic blob** (`Node[uid] → all props`; one get, whole-record update) ← *recommend* vs (B) wide-column split (`Node[uid][prop_id] → value`; cheap partial updates, per-property scans, more keys).
- **Why A**: simpler, one get, cross-property atomicity; fits read-heavy RAG-KG. Wide-column deferred.
- **Touches**: RFC 0004.

### Q7 — Node serialization codec `[in RFC 0004]`
**(A) bincode** (opendata house codec, unblocks M1 now) ← *recommend*, with **rkyv** (zero-copy reads) as a measured fast-follow behind a `NodeCodec` trait. (B) rkyv from day one. (C) CBOR.
- **Why A**: bincode is already proven in `common`; codec is behind a trait so the choice isn't load-bearing.
- **Touches**: RFC 0004.

### Q8 — Node delete semantics `[in RFC 0004]`
**(A) Tombstone-and-filter** (add uid to deleted-node roaring bitmap, filter at read, physical purge = vacuum RFC 0012; O(1) regardless of degree) ← *recommend*, with explicit `DETACH DELETE` for eager. (B) Eager cascade by default (remove all incident edges immediately — O(degree), supernode-hostile).
- **Why A**: keeps deletes degree-independent (supernode-safe); reads pay a bitmap subtraction.
- **Touches**: RFC 0004, 0005 (read filtering), 0012 (vacuum).

### Q9 — Edge delete mechanism `[OPEN — 0005]`
**(A) Per-`(anchor,pred)` deleted-edge roaring bitmap** (merge-union, subtract at read, fold in at rollup) ← *recommend* vs (B) immediate RMW of the posting list (read/remove/write — fine for small lists, expensive for supernodes) vs (C) size-adaptive (RMW small, bitmap large).
- **Why A**: uniform, supernode-safe, mirrors dgraph's Del-posting + rollup. (C) is a possible optimization.
- **Touches**: RFC 0005.

### Q10 — Edge add mechanism `[OPEN — 0005]`
**(A) Size-adaptive** — small list: associative merge-append (fast, no read); grows past a threshold or needs dedup/split: single-writer RMW ← *recommend*. (B) Always RMW. (C) Always merge-append.
- **Why A**: merge-append avoids a read on the hot path; RMW is needed for split at 512 KiB and for dedup. Single-writer makes RMW safe.
- **Touches**: RFC 0005.

---

## C. Consistency & sequencing

### Q11 — Consistency default `[in RFC 0001 D4]`
**(A) Session-token default** (write returns logical seq; reader gates `durable_seq >= token`; strong read-your-writes; no-token = bounded stale; strict = opt-in) ← *recommend* vs (B) strict-by-default (every query refreshes from S3 — simplest model, adds S3 latency to every read).
- **Why A**: read-your-writes without an S3 round-trip per query; matches the sister FTS project.
- **Touches**: RFC 0001, 0007, 0008.

### Q12 — Session token source `[in RFC 0004 D4, amends 0001]`
**(A) turbolay's own logical seq** (`m/latest_seq`, injected as SlateDB `WriteOptions.seqnum`) ← *recommend* vs (B) use SlateDB's internal `WriteHandle.seqnum()` directly.
- **Why A**: keeps the token meaningful across any future format change and is fully our protocol on public APIs (D1). Single-writer monotonicity makes injecting our seq safe.
- **Touches**: RFC 0001, 0004.

### Q13 — Multi-writer / concurrency `[in RFC 0000 D2]`
**(A) Single writer per namespace, no MVCC/OCC/oracle** ← *recommend* (this is what deletes all of dgraph's Zero/Raft/conflict machinery) vs (B) allow concurrent writers (needs the coordination we're trying to avoid).
- **Why A**: the core simplifying invariant; multi-writer is backlog RFC 0016.
- **Touches**: everything (it's the foundational assumption).

---

## D. Indexing & query

### Q14 — v0 index tokenizers `[OPEN — 0006]`
Which value-index tokenizers ship in v0? Candidates (dgraph set): `exact` (equality), `int`/`float` (order-preserving, range), `term` (word-split, lossy), `hash` (non-lossy equality on long strings). `trigram`/`fulltext`/`geo`/`vector` = later.
- *Recommend*: **`exact` + `int` + `float` + `hash`** in v0 (covers `= / IN / < / >` filters and the RAG-KG anchor patterns); add `term` if text filtering is needed early.
- **Touches**: RFC 0006, 0007 (which WHERE predicates are indexable).

### Q15 — Unindexed predicate behavior `[OPEN — 0006/0007]`
A query filters on a non-indexed property: **(A) error by default, opt-in brute-force flag for small namespaces** ← *recommend* vs (B) always brute-force scan vs (C) auto-create the index.
- **Why A**: predictable cost; no silent full scans. Matches sister project.
- **Touches**: RFC 0006, 0007.

### Q16 — Reverse edges `[in RFC 0004 D10]`
**(A) Always materialize both out + in projections** (unconditional bidirectional storage; 2× edge write-amp) ← *recommend* vs (B) opt-in `@reverse` per predicate (dgraph default; saves write-amp, but reverse traversal needs the directive).
- **Why A**: reverse traversal is table-stakes for KG queries; unconditional is simplest and correct. Opt-in is a write-amp optimization if it hurts.
- **Touches**: RFC 0004, 0006.

### Q17 — Variable-length paths in v0 Cypher `[OPEN — 0007]`
Support `-[:REL*min..max]->` in the v0 read subset? **(A) Yes, bounded BFS with a hop cap + uid dedup** ← *recommend* (KG queries need k-hop) vs (B) fixed-length hops only in v0, var-length later.
- **Why A**: k-hop neighborhoods are the core RAG-KG query; without them the subset is too thin. Cap depth (config) to bound cost.
- **Touches**: RFC 0007.

---

## E. Reuse, ops, workload

### Q18 — Code reuse `[in RFC 0000 D8]`
**(A) Depend on `opendata/common`** (serde toolkit, StorageBuilder, merge framework, SequenceAllocator, reader/writer split) ← *recommend* vs (B) vendor selected modules into turbolay vs (C) standalone reimplementation on slatedb directly.
- **Why A**: fastest; `common` already solves order-preserving keys, storage wrapping, merge routing, id allocation, and the reader/writer split. Coupling risk: turbolay tracks that repo's evolution.
- **Consideration**: is `opendata` available as a path/git dependency from this repo, and are you OK coupling to it? If not, (B) vendor.
- **Touches**: whole implementation; RFC 0003 builds on `common::serde`.

### Q19 — One SlateDB DB per namespace `[in RFC 0003]`
**(A) One DB (manifest + poller) per namespace** ← *recommend* (isolation, independent compaction, atomic namespace drop) vs (B) shared DB with a namespace key-prefix (fewer manifests, but shared compaction/cache and no free drop).
- **Why A**: namespace = shard = tenant; matches turbopuffer/namidb; manifest overhead acceptable at POC tenant counts. Revisit (lazy open / shared readers) only past POC scale.
- **Touches**: RFC 0003, 0008.

### Q20 — WAL placement `[OPEN — ops]`
**(A) Single object store for WAL + data (S3 Standard)** ← *recommend for v0* vs (B) separate `wal_object_store` on S3 Express One Zone (~5–10ms durable writes vs ~50–100ms) for lower write latency at extra cost/complexity.
- **Why A for v0**: simpler; measure write-ack latency first (RFC 0017), flip to (B) as a config change if the WAL PUT is the bottleneck. Not an RFC — a config note.
- **Touches**: ops config; RFC 0002 mentions the option.

### Q21 — Workload & scale target `[in RFC 0000 D12]`
**(A) RAG knowledge graph** (Source/Chunk/Entity/RELATES), 1–10M nodes/namespace, correctness-first, shadow-test vs FalkorDB ← *recommend* vs (B) general property graph / LDBC-SNB, 10–100M, deeper traversals & heavier supernodes.
- **Why A**: matches the cortex/turbolay-POC lineage and right-sizes supernode/traversal effort. Keep the model general; revisit scale once something runs.
- **Touches**: right-sizing across all RFCs; RFC 0017 benchmark targets.

### Q22 — Benchmarking substrate `[in RFC 0017 D12]`
**(A) Benchmark on real S3 (+ InMemory/local for unit tests), never LocalStack-only** ← *recommend, hard rule* vs (B) LocalStack/MinIO acceptable for perf numbers.
- **Why A**: namidb found LocalStack hid a ~10× cold-read regression; cold first-hop S3 latency is the real enemy. Unit/integration tests use `object_store::memory::InMemory` or a temp dir; perf claims require real S3.
- **Touches**: RFC 0017, CI/bench setup.

---

## Status of drafting

Drafted (reflecting the recommendations above): `plan.md`, RFC 0000, 0001, 0002, 0003, 0004, 0017.
Paused pending these decisions: RFC 0005 (posting-list substrate — gated on Q1/Q2/Q9/Q10), 0006 (indexes — Q14/Q15/Q16), 0007 (Cypher read path — Q17), 0008 (HTTP/fleet).
