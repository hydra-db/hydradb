---
title: "RFC 0016: Multi-Writer / Namespace Leasing Control Plane"
status: planned (stub — flesh out when triggered)
date: 2026-07-03T00:00:00Z
related:
  - 0000-rfc-index.md
  - 0001-strong-consistency-model.md
  - 0002-substrate-decision-slatedb.md
---

# RFC 0016: Multi-Writer / Namespace Leasing Control Plane

**Status:** planned stub. Fleshed out when static per-namespace writer assignment is outgrown. **This is the big one** — it reopens the single-writer invariant (D2) that lets us delete Dgraph's Zero/OCC/MVCC.

## Summary (to expand)
Relax the single-writer-per-namespace invariant via a leasing control plane and/or intra-namespace sharding — carefully, since D2 is what the whole simplified design rests on.

## Will contain
- Writer lease acquisition/renewal (CAS-based, on top of SlateDB fencing); namespace → writer assignment.
- Options for scaling writes without full multi-writer: (a) namespace sharding with one writer per shard; (b) predicate-group sharding (Dgraph's model) with per-group writers; (c) true concurrent writers — which reintroduces some conflict machinery (what, exactly, and where it lives without a Zero oracle).
- Migration from static assignment; failover; interaction with the session-token protocol (RFC 0001) across writers.

## Trigger
Outgrowing static per-namespace writer assignment (write throughput ceiling of a single writer).
