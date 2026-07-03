---
title: "RFC 0015: Fulltext / Vector / Geo Index Extensions"
status: planned (stub — flesh out when triggered)
date: 2026-07-03T00:00:00Z
related:
  - 0000-rfc-index.md
  - 0006-index-framework.md
  - 0010-bitpacked-frames-and-block-max.md
---

# RFC 0015: Fulltext / Vector / Geo Index Extensions

**Status:** planned stub. Fleshed out per product need; each is a new `IndexAm` (RFC 0006) owning its keyspace behind the trait — no core change.

## Summary (to expand)
New index-kind extensions beyond the v0 value/label/count set: fulltext (BM25), vector (ANN), and geo (S2), each plugging into the RFC 0006 framework.

## Will contain
- **Fulltext**: `term`/analyzer tokenizer, positional postings, BM25 (borrowing block-max/fieldnorm from RFC 0010); `trigram` for substring/`CONTAINS`. Wire a `rank_by` surface into the query path.
- **Vector**: ANN index (e.g. HNSW/IVF) as an `IndexAm`; k-NN query operator.
- **Geo**: S2-cell tokenizer + range/containment predicates.
- Per-extension keyspace, build/backfill, and query-surface additions; the sister FTS-on-S3 project (RFC 0006 posting-blocks) is the fulltext reference.

## Trigger
Product need per index kind (text search, similarity, geospatial).
