---
title: "turbolay — Implementation Plan (M0–M3)"
date: 2026-07-03
kind: implementation
---

# Implementation Plan (M0–M3)

Maps the RFC set to concrete milestones. Correctness-first; optimizations deferred to the backlog RFCs (0009–0016) and gated on real-S3 numbers (RFC 0017). Each milestone ends with its RFC acceptance tests green.

## M0 — Storage foundation  → RFC 0003, 0004 §UID
- Reuse `opendata/common` (confirm dep vs vendor); add `GRAPH=0x05`.
- `serde/` keyspace: record tags + key builders/parsers (inverse pairs) on `common::serde`.
- UID + schema-id allocation over `SequenceAllocator`; `xid → uid`; recovery.
- Schema interning (name↔u32) cached in memory, durable in `SchemaName`/`SchemaId`.
- **Exit:** RFC 0003 property tests (byte order == logical order for every type/boundary); id monotonicity across a simulated restart.
- **Deliverable of record:** `turbolay/src/serde/`, `ids.rs`, `schema.rs`, `storage.rs`.

## M1 — Write path + posting lists  → RFC 0004, 0005
- Node record (bincode blob) + `NodeCodec` trait; node size cap (reject oversize).
- Write path: `UpsertNode`/`UpsertEdge`/`DeleteNode`/`DeleteEdge` → one atomic `WriteBatch` (node + `EdgeOut` + `EdgeIn` + changelog + `m/latest_seq`).
- Posting substrate: `PostingValue` (roaring `RoaringTreemap`, `format` tag, Single/Split), size-adaptive add (merge-union small / RMW at 512 KiB split), deleted-node & deleted-edge bitmaps, rollup.
- Logical-seq protocol (`m/latest_seq`, injected `WriteOptions.seqnum`), recovery scan.
- Merge operator routing (RFC 0003 dispatch): roaring union + i64 counters.
- **Exit:** RFC 0004 (crash-recovery, fencing, RYW, atomic fan-out) + RFC 0005 (set-algebra vs BTreeSet oracle, split lifecycle, delete correctness) tests.

## M2 — Indexes + read consistency  → RFC 0006, 0001
- `IndexAm` trait + registry (`m/index/{id}`) + watermark (`m/wm/{id}`) + backfill state machine.
- v0 indexes: value (`exact`/`hash`/`int`/`float`, date→epoch-int, range scans), label, count; reverse = materialized `EdgeIn`.
- Synchronous in-batch maintenance for live indexes; async backfill for new-on-existing.
- Reader freshness gate (`durable_seq >= token`) + index-watermark + changelog-tail merge.
- **Exit:** RFC 0006 (value/label/count/backfill/lossy re-fetch/unindexed-error) + RFC 0001 (read-your-writes, index-lag-hidden-by-tail, delete correctness) tests.

## M3 — Query + service  → RFC 0007, 0008
- openCypher read subset: parser → predicate IR → planner (anchor by selectivity, per-hop adjacency read + roaring intersect, bounded-BFS var-length) → executor (gate → index → tail merge → fetch → sort/skip/limit).
- axum service (opendata `server/` shape): data plane (upsert/delete/query) + admin plane (index create/drop, namespace); `--role {writer,reader}`; error taxonomy; health/ready/metrics.
- **Exit:** RFC 0007 (grammar accept/reject, worked traversals, var-length, tail merge, bounds) + RFC 0008 (endpoint round-trips, error taxonomy, fenced writer, reader_behind) tests.
- **Validation:** shadow-test the RAG-KG queries vs FalkorDB (0 missing / ≤5% extra), on **real S3**.

## Cross-cutting — Observability  → RFC 0017 (spans M0–M3)
- Phase 0 (M0/M1): instrumented ObjectStore, write-path timers, invariant counters.
- Phase 1 (M2 exit): the metric matrix. Phase 2 (M3): fleet/HTTP/slow-query log.
- Phase 3 (before any optimization RFC): benchmark-grade on real S3 — the hard gate on 0009+.

## Then: backlog RFCs (flesh out + build when triggered)
0009 CSR/WCOJ · 0010 bitpacked frames · 0011 Cypher writes · 0012 vacuum/GC · 0013 full Cypher · 0014 node spill · 0015 fulltext/vector/geo · 0016 multi-writer. See `docs/rfcs/0009`–`0016` stubs.

## Standing risks (track in scratchpad)
- Cold first-hop S3 latency (the real enemy — measure early, real S3).
- Roaring Treemap serialization stability + size vs UidPack (spike).
- opendata `common` reachability/coupling (dep vs vendor).
- Write-batch latency with many synchronous indexes (may push live-index build async).
