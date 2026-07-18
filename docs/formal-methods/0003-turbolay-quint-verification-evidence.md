---
title: "Formal Methods 0003: Turbolay Quint Verification Evidence and Handoff"
status: implementation-in-progress
date: 2026-07-18
depends_on:
  - 0001-quint-jepsen-testing-objective.md
  - 0002-turbolay-quint-specification-plan.md
tags: [quint, apalache, mbt, jepsen, verification]
---

# Turbolay Quint verification evidence and handoff

## What is now executable

All five planned model families typecheck. Their 15 deterministic scenarios
pass, and each main model's randomized Quint simulation found no invariant
violation while reaching its named actions. Apalache then bounded-checked every
main model through six transitions using `quint verify` and
`mise exec java@21.0.2`; all five runs returned `NoError`.

| Family | Safety boundary | Deterministic scenarios | Apalache bound | MBT trace |
|---|---|---:|---:|---|
| M1 | atomic edge projection, idempotency, writer fencing | 3 | 6 | generated |
| M2 | page snapshot scope, historical epoch rejection, bookmarks | 3 | 6 | generated |
| M3 | artifact generation fence, matrix equivalence, reader retention | 3 | 6 | pending |
| M4 | placement disagreement and durable writer fence | 3 | 6 | pending |
| M5 | command normalization, relationship identity, batch semantics | 4 | 6 | pending |

The generated M1/M2 Informal Trace Format files are under `target/formal/` and
are ignored by Git. They include `mbt::actionTaken`; this proves trace
generation, not a replay against Rust.

## Evidence boundary

These checks are intentionally finite. A six-step Apalache result means that
all transitions in the stated, small abstraction passed up to that bound. It
does not prove arbitrary graph sizes, arbitrary S3 failure behavior, or the
Rust implementation. The next layer binds that abstraction to code; the final
layer binds it to deployed processes.

The bug records make this distinction explicit:

| Finding | Current evidence | Remaining gate |
|---|---|---|
| BFG-001 | fault model counterexample; current page-entry scope fix; M2 bounded check | force a writer commit inside historical/current graph-kernel and streaming page operators, then replay in Rust MBT |
| BFG-002 | fault model counterexample; historical `e875387` test returns current row; current regression rejects; M2 bounded check | Rust MBT replay and review |
| BFG-003/BFG-004 | M1/M5 model makes the identity/batch choice explicit | approve relationship-ID scope and duplicate-row public contract before calling either a defect or fixed |
| BFG-005/BFG-006 | M1/M3 check normalized write and stale-build safety | implementation trace adapter plus concurrent artifact test |
| BFG-007 | M2/M4 model safety only | decide remote bookmark/read-only freshness guarantee |
| BFG-008 | deliberately open direct-page contract | approve best-effort behavior or add a snapshot-bearing/materialized direct cursor |

## Rust MBT adapter scope

Add a test-only `quint-connect`/ITF driver only after resolving its version
against this repository's Rust toolchain. The driver must invoke public kernel
APIs and compare a normalized projection after every trace action; it must not
read private SlateDB keys as its oracle.

| Family | Rust action binding | Public normalization after each action |
|---|---|---|
| M1 | `GraphShard::open_standalone_writer`, `write_edge`, `delete_edge`, retry, writer reopen | edge existence, neighbors, degree, current epoch, idempotency outcome |
| M2 | `snapshot`, `edge_exists_at`, `out_neighbors_at`, `out_degree_at`, `execute_cypher_rows_page` | one snapshot's edge/neighbor/degree projection; typed historical error; page rows |
| M3 | artifact build/refresh, direct and matrix reachability, maintenance GC | direct traversal equals matrix-plus-delta traversal; publication generation; retained read succeeds |
| M4 | fenced owned shard/routed cluster open and replacement writer | one accepted writer, monotone epoch, fresh reader sees committed prefix |
| M5 | Cypher `CREATE`/`MERGE`/`DELETE`/batch plus service cursor calls | normalized rows, relationship identity, batch outcome, materialized cursor rows |

The first implementation order is M1, then M2. It should run a local
in-memory/object-store trace first, retain the input ITF plus observed
projection on failure, and rerun the same corpus against MinIO. M3–M5 follow
only after their required public semantic decisions are approved.

## Jepsen handoff

Jepsen must use the same operation vocabulary and end-state oracle rather than
trying to execute Quint actions directly.

| Campaign | Generator operations | Nemesis | Checker / postcondition | Model link |
|---|---|---|---|---|
| Write atomicity | create/delete edge, retry same key, conflicting-key reuse | response timeout, client-node partition, kill/restart | per-cell linearizability for accepted operations; fresh `verify_current_graph` and digest | M1 |
| Snapshot pages | one-shot query; page one then mutation then page two; historical epoch request | pause writer during page, request timeout | page is internally one snapshot; historical request errors; direct-page across-request result follows approved BFG-008 contract | M2 |
| Ownership | routed writes with old/new owner identities | SIGSTOP old owner, membership partition, replacement start, heal | no accepted zombie write; replacement prefix contains prior commits | M4/M1 |
| Artifacts and GC | topology writes, direct/matrix reachability, maintenance requests | pause/kill builder, concurrent writes, MinIO latency/restart | direct equals matrix at a fresh snapshot; retained snapshots remain readable | M3 |
| API semantics | relationship MERGE, duplicate vertex batch, service cursor | client retry/cancel and process restart | only approved identity/batch contract is accepted; service cursor rows remain materialized | M5/M2 |

Run no-fault histories first, then one nemesis class at a time. Preserve
Jepsen history, node/object-store logs, object prefix, and a fresh-reader graph
digest. Minimize every failure and add it either as a `BFG-*` record or as a
documented unsupported-contract result.

## Reproducible commands

Run all long commands in tmux pane `pson:10.2`:

```bash
mise exec -- quint test quint-models/turbolay/m1_cell_write_test.qnt \
  --main m1_cell_write_test --match '.*Test$'

mise exec java@21.0.2 -- mise exec -- quint verify \
  quint-models/turbolay/m1_cell_write.qnt --main m1_cell_write \
  --invariant epochNeverRegresses,edgeProjectionConsistent,\
deltaMatchesLatestTopologyChange,createIdempotencyExact,\
deleteIdempotencyExact,oneEffectiveWriter,zombieWriteRejected --max-steps 6

mise exec -- quint run quint-models/turbolay/m1_cell_write.qnt \
  --main m1_cell_write --mbt --max-steps 8 \
  --out-itf target/formal/m1-cell-write-mbt.itf.json
```

## Review requested

1. Approve or change the BFG-003 relationship external-ID scope.
2. Approve whether BFG-004's conflict rejection is the public batch contract.
3. Choose BFG-008: stable direct pagination or explicitly best-effort offset
   pagination.
4. Confirm that M1 then M2 is the desired Rust MBT implementation order.
