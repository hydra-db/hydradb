---
title: "RFC 0000: RFC Index & Roadmap"
status: draft
date: 2026-07-03T00:00:00Z
related:
  - ../plan.md
  - 0001-strong-consistency-model.md
---

# RFC 0000: RFC Index & Roadmap

## Purpose

Master index of every RFC needed to take turbolay — a **property-graph database on S3, Dgraph's storage model reimplemented in Rust on SlateDB** — from plan to running system.

- **v0** = a correct, observable, end-to-end graph database: node/edge store, value/reverse/count indexes, an openCypher read subset, a JSON write API, a reader fleet, an HTTP service. Milestones M0–M3.
- **Optimizations are deliberately deferred** until the running v0 system produces measurable numbers on **real S3** (see the "Correct-first ledger"). CSR adjacency, WCOJ/leapfrog joins, and bitpacked frames become later RFCs and run on data, not intuition.

The one-sentence architecture: *turbolay is a predicate-sharded KV store on SlateDB where the value at each key is a compressed sorted set of UIDs (a posting list), and a graph edge is a member of one of those sets — with Dgraph's Raft/Zero/MVCC machinery deleted because we have exactly one writer per namespace.*

## Locked decisions (2026-07-03)

Best-judgment defaults; each is vetoable — flag it and the dependent RFCs update.

| # | Decision | Consequence |
|---|---|---|
| D1 | **Substrate = SlateDB v0.14.1, unmodified.** No fork, no upstream patch on the critical path. | RFC 0002 is a one-page decision record. Anything SlateDB doesn't expose is built as an application-level protocol on public APIs, never a patch. |
| D2 | **Single writer per namespace.** No Raft, no Zero timestamp oracle, no conflict-key OCC, no version-in-key MVCC. SlateDB manifest writer-epoch fences zombies. | The entire distributed half of Dgraph is deleted (RFC 0004 §"what we drop"). Writes serialize by construction; a single logical seq is authoritative. |
| D3 | **Adjacency = Dgraph-style posting lists on SlateDB** (roaring `RoaringTreemap` sets, split at 512 KiB). Designed **CSR-ready** — a `format` tag + per-part min/max/card skip metadata written from day one. | RFC 0005 owns the encoding + split/rollup. UidPack/CSR, WCOJ/leapfrog joins, and bitpacked frames are deferred behind the format tag but non-blocking (seam exists). |
| D4 | **Consistency = session token via our own logical seq** (`m/latest_seq`, optionally injected as SlateDB `WriteOptions.seqnum`). Reads = posting-lists/indexes to a watermark + changelog-tail overlay, merged. | RFC 0001 (amended by RFC 0004 §seq). Index lag is invisible to callers. No external coordinator. |
| D5 | **Internal `u64` UIDs + external-id (`xid`) mapping.** UIDs allocated crash-safely via `common::SequenceAllocator`. | Roaring posting-list compression and cheap set math need dense-ish u64s (UUIDs would destroy them — namidb's documented mistake). RFC 0004 owns allocation + the `xid → uid` index. |
| D6 | **Indexes / reverse / count are posting lists**, keyed by order-preserving token / object-uid / degree. Lossy tokens carry a re-fetch flag. | RFC 0006. Range predicates become key-range scans (order-preserving numeric tokens). Reverse adjacency is materialized (`EdgeIn`), not lazy. |
| D7 | **Query surface v0 = openCypher READ subset** (`MATCH`/`WHERE`/`RETURN`/`ORDER BY`/`LIMIT`, fixed + variable-length hops) + **JSON upsert writes**. Cypher writes deferred. | RFC 0007 specifies the subset, the planner, and a swappable predicate IR so a later full-Cypher/GQL frontend is a frontend change. |
| D8 | **Reuse `opendata/common`** (serde toolkit, `StorageBuilder`, merge-operator-by-record-type, `SequenceAllocator`, reader/writer split, RFC template, test harness). Subsystem byte `GRAPH = 0x05`. | RFC 0003 builds the keyspace on `common::serde`. We never touch `slatedb::Db` directly. |
| D9 | **Reader/writer node separation** via SlateDB `DbReader`; same binary, `--role {writer,reader}`. | Readers scale independently (goal #2). RFC 0008 owns the fleet + role routing. |
| D10 | **Property-graph model** (nodes + labels + property blob, typed directed edges + properties); **bidirectional storage** (out + in projections, one atomic batch). Value-log subsumed by SlateDB. Oversize node → reject in v0. | RFC 0004. No Badger value-log reimplementation; spill-to-raw-S3 is backlog. |
| D11 | **`MergeOperator` only for associative state** — degree counters (i32 sum), deleted-UID roaring bitmap (union), ordered-set append. Split/rollup done by single-writer RMW. | RFC 0002 §constraints; RFC 0005 owns the RMW paths, RFC 0006 the merge routing. |
| D12 | **Workload = RAG knowledge graph** (Source/Chunk/Entity/RELATES), 1–10M nodes/namespace, correctness-first. **Benchmark on real S3.** Shadow-test vs FalkorDB (0 missing / ≤5% extra). | Right-sizes supernode/traversal effort. RFC 0017 forbids LocalStack-only benchmarking (namidb hid a ~10× cold regression that way). |

## RFC catalog

### v0 — correct running system (M0–M3)

| RFC | Title | Unblocks | Status |
|---|---|---|---|
| 0001 | Strong consistency model | M1, M3 | drafted 2026-07-03; amended by 0004 (D4) |
| 0002 | Substrate decision record: SlateDB | everything | drafted 2026-07-03 |
| 0003 | Keyspace layout & order-preserving encoding | M0 | drafted 2026-07-03 |
| 0004 | Graph data model, write path & UID allocation | M1 | drafted 2026-07-03 |
| 0005 | Posting-list substrate (roaring, split, rollup) | M1/M2 | drafted 2026-07-03 |
| 0006 | Index framework & value/reverse/count indexes | M2 | drafted 2026-07-03 |
| 0007 | openCypher read subset, planner & read path | M2/M3 | drafted 2026-07-03 |
| 0008 | HTTP service, reader/writer fleet & error taxonomy | M3 | drafted 2026-07-03 |
| 0017 | Observability: SLIs, metrics & bottleneck ledger | spans M0–M3; gates optimizations | drafted 2026-07-03 |

### RFC 0001 — Strong consistency model
- **Decides**: session-token default (logical seq); reader freshness gate on `DbReader::subscribe()`/`durable_seq`; no-token bounded staleness; strict mode bounded by `manifest_poll_interval` / reader reopen; the index-watermark + changelog-tail query plan that hides index lag; failure semantics (`reader_behind` retryable, zombie-writer fencing).
- **Explicitly drops** Dgraph's Zero oracle, `start_ts`/`commit_ts`, conflict OCC, linearizable-read timestamp waiting — single-writer makes them unnecessary.

### RFC 0002 — Substrate decision record: SlateDB
- **Decides** D1, D2, D11 formally. Records why we build on SlateDB rather than a self-owned graph-aware SST format (the namidb / turbolay-v0 path): SlateDB owns LSM/compaction/caching/fencing/S3, so the graph model ports on top without reimplementing storage. Records accepted constraints (bytewise ordering, pre-1.0 churn, no KV separation → node size cap, RMW posting maintenance) and the per-index escape hatch.

### RFC 0003 — Keyspace layout & order-preserving encoding
- **Decides**: subsystem `GRAPH = 0x05`; record-type tags (`SchemaName`/`SchemaId`, `Node`, `EdgeOut`, `EdgeIn`, `EdgePart`, `Index`, `Count`, `Xid`, `Log`, `Meta`); name interning to `u32` ids (schema-preserving); exact key layouts; order-preserving encoding of every key component via `common::serde` (`terminated_bytes` escape/terminator, big-endian ints/uids, sign-flip floats via `sortable`, fixed-BE changelog seq); the merge-operator dispatch table. Edge **sortkey** / ordered adjacency is **deferred** (Q4) — v0 is the pure posting-list model ordered by dst uid.
- **Acceptance**: property tests proving `encoded byte order == logical order` for every type and boundary (embedded `0x00`, ±0.0, empty strings, UID ordering, xid prefixes).

### RFC 0004 — Graph data model, write path & UID allocation
- **Decides**: the property-graph model (node = labels + property blob or wide-column split; edge = typed directed with optional properties/facets); the `DirectedEdge`→KV fan-out (node record + `EdgeOut` + `EdgeIn` + affected indexes + changelog + `m/latest_seq`, all in one `WriteBatch`); UID allocation via `SequenceAllocator` + the `xid → uid` mapping and its recovery; the changelog record schema (op, uids, predicate, before/after); node property serialization (codec behind a trait); node size cap (reject oversize); the D4 seq protocol (amends RFC 0001).
- **Explicitly drops** Dgraph's delta-then-rollup-on-timestamp: the single writer applies posting-list edits as RMW; "rollup" maps onto SlateDB compaction.
- **Acceptance**: crash-recovery (kill writer mid-batch, reopen, invariants + counters hold), zombie-writer fencing, read-your-writes-across-reader.

### RFC 0005 — Posting-list substrate
- **Decides**: posting values are **roaring `RoaringTreemap`** (64-bit) sets carrying a 1-byte `format` tag (the CSR-ready seam); the `Single` vs `Split` value shape (plain membership in the set; valued/faceted edges keep their properties in a companion `EdgeProp` record); split at 512 KiB via median-cardinality bin-split into part-keys (on S3: new keys, no in-place rewrite); the ascending-UID iteration invariant that set algebra relies on; **per-part min/max/card skip metadata** written from day one; set algebra (AND/OR/NOT/difference/cardinality) from roaring, **replacing Dgraph's UidPack `codec` and `algo/uidlist` intersection engine**. UidPack/CSR remain a deferred format behind the tag (RFC 0009/0010).
- **Per D11**: split/rollup = single-writer RMW; ordered-set append = associative merge operand.
- **Acceptance**: split/merge exercised from day one (single part until first split); correctness under update/delete via deleted bitmap; intersection equivalence tests vs a naïve `BTreeSet` oracle.

### RFC 0006 — Index framework & value/reverse/count indexes
- **Decides**: the `IndexAm` trait (`extract`/`apply`/`supports`/`execute`); index registry + per-index watermark commit protocol (data + `m/wm/{id}` in one batch, idempotent replay); backfill state machine (`creating → backfilling → live → dropping`); the concrete v0 indexes — value/eq (tokenizers: `exact`, `term`, `hash`, order-preserving `int`/`float`), reverse (materialized `EdgeIn`), count (`degree`); lossy-token re-fetch; operator→access mapping (`=`→lookup, `IN`→union, ranges→bounded scan, degree filter→count lookup).
- **Planner tie-in**: `supports()` is what the RFC 0007 planner consults per predicate; unindexed predicate → error by default, opt-in brute-force for small namespaces.

### RFC 0007 — openCypher read subset, planner & read path
- **Decides**: the supported Cypher grammar subset (node/edge patterns, labels, property maps, `WHERE` with `AND`/`OR`/`NOT` and comparison ops, fixed + `*min..max` variable-length paths, `RETURN`/`ORDER BY`/`SKIP`/`LIMIT`); lowering to a predicate IR; the planner (anchor selection = most selective index; hop = adjacency read + sorted-UID intersect; var-length = bounded BFS with UID dedup); the read path (freshness gate → index/adjacency to `W = min(watermark)` → changelog tail `(W, latest]` evaluated on materialized nodes/edges → merge); N+1 mitigations (batch neighbor reads, filter push-down, frontier-size heuristics); behavior at the var-length/tail bound.
- **IR swap path**: a later full-Cypher/GQL/aggregation frontend consumes the same IR — frontend change, not rewrite.

### RFC 0008 — HTTP service, reader/writer fleet & error taxonomy
- **Decides**: axum service (opendata `server/` shape); `--role {writer,reader}` routing; two surfaces — data plane (upsert nodes/edges, delete, Cypher query = `{cypher, params, consistency}`) and admin plane (create/drop index, namespace lifecycle); error taxonomy (`reader_behind` retryable, malformed-Cypher, unindexed-path-without-brute-force, oversize-node); auth posture (v0: trusted network); namespace creation implicit on first write.

### RFC 0017 — Observability: SLIs, metrics & the bottleneck ledger
- **Decides**: an instrumented `ObjectStore` wrapper as S3 latency/cost ground truth; phase timers at the seams RFCs 0004–0007 name (write-batch steps, index build-tick, query phases: gate / anchor / per-hop / tail-merge); the metric inventory (latest seq, per-index watermark lag, changelog-tail entries scanned, deleted-bitmap cardinality, posting split counts, per-hop frontier sizes, N+1 fan-out, reader-behind rate); should-be-zero invariant counters; slow-query log + `debug: true` per-query stats.
- **Binding on optimization RFCs**: **benchmark on real S3, never LocalStack-only** (D12). Every correct-first deferral maps to a primary metric (must move) + guardrail (must not regress); baselines required before an optimization merges.

### Backlog / post-v0 (brief stubs written 2026-07-03; flesh out when triggered)

Each below exists as a **brief stub RFC** (summary + "will contain" + trigger) in `docs/rfcs/`; expand on its trigger.

| RFC | Title | Trigger |
|---|---|---|
| 0009 | CSR adjacency + WCOJ/leapfrog joins | traversal latency measured too high on real S3 (gated on 0017 baselines) |
| 0010 | Bitpacked posting frames + block-max WAND | posting decode / intersection dominates profiles |
| 0011 | Cypher write surface (CREATE/MERGE/SET/DELETE) | read path proven end-to-end |
| 0012 | Vacuum, dead-UID purge & GC | deleted-bitmap cardinality growth |
| 0013 | Full openCypher (WITH, aggregation, subqueries, path fns) | product need; consumes 0007's IR |
| 0014 | Oversized-node spill to raw S3 objects | node size cap rejected by real workloads |
| 0015 | Fulltext / vector / geo index extensions | product need; each an `IndexAm` |
| 0016 | Multi-writer / namespace leasing control plane | outgrowing static writer assignment |

## Correct-first ledger

| Deferred | v0 does instead | Picked up by |
|---|---|---|
| CSR adjacency, WCOJ/leapfrog joins | posting-list reads + sorted-UID intersect | 0009 |
| Bitpacked frames, UidPack, block-max WAND | roaring posting lists, exact intersect | 0010 (format tag reserved day one per 0005) |
| Cypher writes | JSON upsert API | 0011 |
| Vacuum / dead-UID purge | deleted-bitmap filtering (roaring merge-union) | 0012 |
| Full openCypher surface | scoped read subset + IR | 0013 |
| Oversized-node spill | reject over cap | 0014 |
| Locality-aware partitioning | uid-order + block cache | measured; noted in 0017 |
| Standalone/distributed compaction | embedded in writer | config flip, no RFC |
| Dgraph Zero oracle / conflict OCC / MVCC-in-key | single writer + logical seq + changelog tail | never (deleted by D2) |

## Drafting order (critical path)

1. **0002** — one page, closes the substrate question on paper. Unblocked.
2. **0001 + 0003** — the consistency contract and the keyspace/encoding foundation. 0001 is largely portable from the sister FTS project; 0003 builds on `common::serde`. Unblocked.
3. **0004 + 0005** — the M1 write path and posting-list core (the port from Dgraph `codec`/`posting`/`x`). 0004 carries the D4 seq amendment.
4. **0006** — the index framework and the three v0 indexes.
5. **0007** — the openCypher read subset and read path.
6. **0008** — thin; falls out of 0007 + 0001 + the opendata `server/` shape.
7. **0017** — spine lands with M0/M1; the matrix is an M2 exit criterion; benchmark-grade gates any optimization RFC.

## Open questions

Defaults adopted 2026-07-03 (D1–D12), vetoable:

- **D3 (pivotal)**: posting-lists-on-SlateDB vs CSR. Prior art (namidb, turbolay-v0) chose CSR but built their own SST format; we choose posting-lists-on-SlateDB to let SlateDB do the heavy lifting and match the Dgraph brief, with CSR as an additive optimization (0009). *Veto reopens the substrate model.*
- **D7**: openCypher read subset + JSON writes for v0; Cypher writes to 0011. *Confirm the exact grammar subset when 0007 is drafted.*
- **D8**: reuse `opendata/common` as a dependency vs vendor selected modules vs standalone reimplementation. Default: reuse. *Confirm the coupling before M0.*
- **D12**: RAG-KG workload / 1–10M scale / correctness-first. *Confirm the primary workload; it right-sizes supernode + traversal effort.*

## Amendments (post-draft decisions)

Additive record of decisions taken after the initial draft. The catalog and locked-decision tables above are left intact; these amend specific rows.

- **2026-07-03 — Q23 (amends D7; narrows RFC 0013).** The openCypher **frontend** (lexer + parser + full-grammar AST) is front-loaded as a parallel track during **M1**, ahead of the planner/executor. The parser accepts the **full** openCypher grammar; the v0 subset boundary is enforced at **lowering** (AST → predicate IR), which emits `unsupported_cypher` for out-of-v0 constructs — `malformed_cypher` stays strictly syntactic. The v0 *executable* surface is unchanged (still D7's read subset). **Consequence:** RFC 0013's scope drops the parser (→ IR growth + lowering + executor only). Parser implementation (Q23a) **resolved 2026-07-03: adopt `decypher`, vendored as a local sibling crate** (`../decypher`, cloned with upstream remote retained, path-dep'd from turbolay, modifiable locally) — its var-length + full-grammar AST and `miette` diagnostics, with vendoring absorbing the alpha/unstable-AST risk. Our lowering (decypher AST/HIR → predicate IR) is the coupling surface and the subset gate. Full record: RFC 0007 §13; `docs/open-decisions.md` Q23.
