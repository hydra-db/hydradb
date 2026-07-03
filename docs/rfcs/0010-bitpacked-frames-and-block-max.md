---
title: "RFC 0010: Bitpacked Posting Frames & Block-Max WAND"
status: planned (stub — flesh out when triggered)
date: 2026-07-03T00:00:00Z
related:
  - 0000-rfc-index.md
  - 0005-posting-list-substrate.md
  - 0015-search-index-extensions.md
---

# RFC 0010: Bitpacked Posting Frames & Block-Max WAND

**Status:** planned stub. Fleshed out when posting decode / intersection dominates profiles (RFC 0017).

## Summary (to expand)
Introduce a UidPack-style **bitpacked frame encoding** as `PostingValue format = 2`, an alternative to roaring for postings where decode/intersection is the bottleneck, plus block-max metadata for WAND/MAXSCORE skipping (primarily for ranked fulltext).

## Will contain
- Bitpacking crate integration; 256-uid frames with block headers (first-uid, count, max-weight).
- `format`-tag dual-read so bitpacked and roaring postings coexist (the CSR-ready seam from RFC 0005).
- When to prefer bitpacked vs roaring (measured, per posting kind).
- Block-max WAND/MAXSCORE for ranked fulltext scoring (ties to RFC 0015).

## Trigger
Posting decode or intersection dominates query profiles on real S3 (RFC 0017).
