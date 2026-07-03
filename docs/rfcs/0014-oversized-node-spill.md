---
title: "RFC 0014: Oversized-Node Spill to Raw S3 Objects"
status: planned (stub — flesh out when triggered)
date: 2026-07-03T00:00:00Z
related:
  - 0000-rfc-index.md
  - 0004-graph-data-model-and-write-path.md
---

# RFC 0014: Oversized-Node Spill to Raw S3 Objects

**Status:** planned stub. Fleshed out only if the node size cap (RFC 0004) is rejected by real workloads.

## Summary (to expand)
For nodes whose encoded `NodeRecord` exceeds the size cap, spill large property blobs to raw S3 objects referenced from the node record — working around SlateDB's lack of key-value separation (RFC 0002 constraint) without rewriting huge values through compaction.

## Will contain
- Spill threshold + reference format in the `NodeRecord`; write-path and read-path indirection.
- Consistency of the spill reference within the atomic write.
- GC of spilled objects (ties to RFC 0012 vacuum).

## Trigger
The v0 node size cap (default 1 MiB) rejected by real workloads (RFC 0017 `oversize_node` rate).
