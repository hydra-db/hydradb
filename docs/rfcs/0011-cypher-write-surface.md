---
title: "RFC 0011: openCypher Write Surface"
status: planned (stub — flesh out when triggered)
date: 2026-07-03T00:00:00Z
related:
  - 0000-rfc-index.md
  - 0004-graph-data-model-and-write-path.md
  - 0006-index-framework.md
  - 0007-opencypher-read-path.md
---

# RFC 0011: openCypher Write Surface

**Status:** planned stub. Fleshed out once the read path (RFC 0007) is proven end-to-end.

## Summary (to expand)
Add Cypher mutation — `CREATE` / `MERGE` / `SET` / `REMOVE` / `DELETE` / `DETACH DELETE`, and index DDL `CREATE INDEX` / `DROP INDEX` — as a **frontend onto the existing JSON write path (RFC 0004) and index registry (RFC 0006)**. No new storage machinery.

## Will contain
- Mutation grammar; lowering to the RFC 0004 `UpsertNode`/`UpsertEdge`/`DeleteNode`/`DeleteEdge` ops.
- `MERGE` (match-or-create) semantics under single-writer serialization; `SET`/`REMOVE` property and label.
- Multi-statement write = one atomic `WriteBatch`; interaction with the session-token protocol.
- **`CREATE INDEX FOR (n:Label) ON (n.prop)` / `DROP INDEX`** onto the RFC 0006 registry (the user-requested Cypher DDL; also the generalized user-driven field-selection surface).
- Error taxonomy additions; extends the RFC 0007 predicate IR with write ops.

## Trigger
Read path proven end-to-end (M3 complete).
