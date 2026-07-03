---
title: "RFC 0012: Vacuum, Dead-UID Purge & GC"
status: planned (stub — flesh out when triggered)
date: 2026-07-03T00:00:00Z
related:
  - 0000-rfc-index.md
  - 0004-graph-data-model-and-write-path.md
  - 0005-posting-list-substrate.md
---

# RFC 0012: Vacuum, Dead-UID Purge & GC

**Status:** planned stub. Fleshed out when deleted-bitmap cardinality growth is observed (RFC 0017).

## Summary (to expand)
Physically reclaim what tombstone-and-filter (RFC 0004) and deleted-edge bitmaps (RFC 0005) leave behind: purge dead nodes/edges, fold deleted bitmaps into posting lists, and GC orphaned posting parts and index entries.

## Will contain
- Vacuum loop (single-writer RMW): fold `deleted_nodes` / `deleted_edges` bitmaps into the sets and clear them; re-merge posting parts on shrink.
- Purge a deleted node's `Node` record, `Xid` mapping, index tokens, and incident `EdgeOut`/`EdgeIn` entries.
- Snapshot-retention horizon (min manifest version any live reader is pinned to) so vacuum never resurrects a row a reader can still see; interaction with SlateDB compaction/GC + checkpoints.
- Cardinality-based trigger; guardrails against tombstone storms.

## Trigger
Deleted-bitmap cardinality growth (RFC 0017 metric).
