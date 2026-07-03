---
title: "RFC 0013: Full openCypher (Pipelines, Aggregation, Subqueries)"
status: planned (stub — flesh out when triggered)
date: 2026-07-03T00:00:00Z
related:
  - 0000-rfc-index.md
  - 0007-opencypher-read-path.md
---

# RFC 0013: Full openCypher

**Status:** planned stub. Fleshed out on product need; consumes the RFC 0007 predicate IR (frontend/executor extension, not a rewrite).

> **Scope amendment (2026-07-03, per RFC 0007 §13 / Q23):** the Cypher **parser + AST is no longer owned by this RFC** — the full openCypher grammar is parsed by a front-loaded frontend built during M1 (RFC 0007 Amendment A1). What this RFC accepts today as `unsupported_cypher` is already *parsed* into a complete AST; the frontend just declines to *lower* it. RFC 0013's remaining scope therefore narrows to **IR growth + lowering rules + executor support** — no parser work. Constructs are recognized (not `malformed_cypher`) the day the parser lands; they become executable when this RFC adds their `PlanNode`(s) and lowering.

## Summary (to expand)
Extend the v0 read subset to the constructs RFC 0007 rejects as `unsupported_cypher`: `WITH` pipelines, aggregations beyond `count(*)`, `DISTINCT`, subqueries, `OPTIONAL MATCH`, path variables, `shortestPath`, and map projections.

## Will contain
- `WITH` pipeline semantics (intermediate projections/aggregation between MATCH stages).
- Streaming/hash aggregations (`collect`/`sum`/`avg`/`min`/`max`/`count(expr)`, `DISTINCT`).
- `OPTIONAL MATCH` (left-join semantics); path binding + `shortestPath`; subqueries.
- The predicate-IR extensions required; optional DataFusion feasibility note for a SQL/analytical frontend.

## Trigger
Product need for the full surface; the RFC 0007 IR is the swap point.
