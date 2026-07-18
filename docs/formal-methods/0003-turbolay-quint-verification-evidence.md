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

All five planned model families and their three focused submodels typecheck.
Their 31 deterministic scenarios pass, and each main model's randomized Quint
simulation found no invariant violation while reaching its named actions.
Apalache then bounded-checked every main model through six transitions using
`quint verify` and `mise exec java@21.0.2`; all runs returned `NoError`.

| Family | Safety boundary | Deterministic scenarios | Apalache bound | Rust MBT status |
|---|---|---:|---:|---|
| M1 | atomic edge projection, idempotency, writer fencing | 4 | 6 | 24 seeded public-API traces pass |
| M1b | chunked bulk import, durable prefix, retry | 3 | 6 | focused public-API conformance test |
| M2 | page snapshot scope, historical epoch rejection, bookmarks | 4 | 6 | direct-page conformance test; driver pending |
| M2b | current-only `snapshot_at`, typed rejection, cancelled page | 3 | 6 | 24 seeded OpenCypher public-API traces pass |
| M3 | artifact generation fence, matrix equivalence, reader retention | 4 | 6 | 10k simulation + bounded check; driver pending |
| M4 | placement disagreement and durable writer fence | 3 | 6 | 10k simulation + bounded check; driver pending |
| M5 | command normalization, relationship identity, batch semantics | 7 | 6 | P0/P1 command conformance tests; driver pending |
| M5b | `DELETE`, `DETACH DELETE`, cell-drop fence | 3 | 6 | 24 seeded public-API traces pass |

The generated M1/M2 Informal Trace Format files are under `target/formal/` and
are ignored by Git. M1 is replayed by `tests/formal_mbt.rs`; M2b and M5b are
replayed by `tests/formal_mbt_p2.rs`. `quint-connect` generates 24
deterministic-seed simulation traces per driver, calls only public APIs, and
compares normalized state after every step. The named `quint test` witnesses
remain Quint-only because their ITF output omits `mbt::actionTaken`, which the
current adapter requires for action dispatch.

### P0 completion evidence (2026-07-18)

M2 now states BFG-008 precisely: a low-level offset page captures one request
view, while a materialized cursor remains stable. M5 now states metadata set
and removal as an atomic projection that cannot remove structural adjacency.
Both model families passed 10,000 twelve-step simulation traces and a
six-transition Apalache check using their `allSafety` predicates.

The focused kernel conformance tests are:

| Rank | Rust test | Contract checked |
|---:|---|---|
| 1–2 | `formal_p0_relationship_identity_and_duplicate_vertex_batch_are_atomic` | cell-global external relationship ID; equal duplicate coalescing; conflict has no epoch change |
| 3 | `formal_p0_direct_offset_pagination_is_best_effort_across_requests` | insertion before an offset may repeat a row on the later direct request |
| 4 | `formal_p0_edge_metadata_set_and_clear_preserve_structural_adjacency` | property visibility changes while edge existence and degree remain stable |

These are public-kernel conformance tests, not a claim that M2/M5 already
have full Quint Connect action adapters. M1 is the current complete adapter;
M2 and M5 are the next bindings.

### P1 artifact and reader-safety evidence (2026-07-18)

M3's aggregate predicate covers stale publication rejection, active-read
retention, and matrix/direct equivalence. M4's covers the durable writer fence,
monotone committed prefix, and zombie-writer rejection. Both passed their
deterministic suites, 10,000 twelve-step simulation traces, and six-step
Apalache checks. This proves the stated finite abstractions, not a read-only
freshness SLA: the documented BFG-007 contract remains safety/proof-or-error
only.

### P1 bulk import evidence (2026-07-18)

The M1b submodel represents the documented 2+2+1 chunk boundaries for a
five-edge import. It makes an interruption after the first durable chunk
explicit and requires retry to preserve the committed prefix. Its three
witnesses, 10,000-sample simulation, and six-step Apalache check pass. The
default-feature Rust test
`formal_p1_chunked_bulk_import_is_idempotent_by_durable_chunk` validates a
real chunked import, invalid chunk rejection, reordered retry, stable epoch,
and final public neighbors.

### P2 snapshot and destructive-operation evidence (2026-07-18)

M2b and M5b each passed their three deterministic scenarios, 10,000 twelve-step
Quint simulation, and six-step Apalache check. The default-feature tests
`formal_p2_snapshot_at_is_current_only` and
`formal_p2_delete_detach_delete_and_drop_are_fenced` pass. With OpenCypher,
`formal_p2_cancelled_cursor_page_returns_no_rows` also passes. Together these
check current-only storage snapshot admission, future/historical typed errors,
cancelled page no-result behavior, detach cascade, and the post-drop write
fence. `formal_mbt_p2.rs` then replays 24 seeded M5b destructive-operation
traces under default features and 24 seeded M2b snapshot/cancellation traces
with OpenCypher. Each driver normalizes only public outcomes, snapshots, and
degree projections after every Quint action.

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
| BFG-003/BFG-004 | M1/M5 model makes the identity/batch choice explicit; M1 Rust MBT verifies structural-edge retries | implement the approved relationship-ID and duplicate-row contract in the M5 driver |
| BFG-005/BFG-006 | M1/M3 check normalized write and stale-build safety | implementation trace adapter plus concurrent artifact test |
| BFG-007 | M2/M4 model safety only | decide remote bookmark/read-only freshness guarantee |
| BFG-008 | deliberately open direct-page contract | approve best-effort behavior or add a snapshot-bearing/materialized direct cursor |

## Rust MBT adapter scope

The test-only `quint-connect` 0.1.2 driver is resolved in the workspace's
locked development dependencies. It invokes public kernel APIs and compares a
normalized projection after every trace action; it does not read private
SlateDB keys as its oracle.

| Family | Rust action binding | Public normalization after each action |
|---|---|---|
| M1 | **implemented:** `GraphShard::open_standalone_writer`, `write_edge`, `delete_edge`, retry, close/reopen | edge existence, degree, current epoch, recorded idempotency outcome |
| M2 | `snapshot`, `edge_exists_at`, `out_neighbors_at`, `out_degree_at`, `execute_cypher_rows_page` | one snapshot's edge/neighbor/degree projection; typed historical error; page rows |
| M3 | artifact build/refresh, direct and matrix reachability, maintenance GC | direct traversal equals matrix-plus-delta traversal; publication generation; retained read succeeds |
| M4 | fenced owned shard/routed cluster open and replacement writer | one accepted writer, monotone epoch, fresh reader sees committed prefix |
| M5 | Cypher `CREATE`/`MERGE`/`DELETE`/batch plus service cursor calls | normalized rows, relationship identity, batch outcome, materialized cursor rows |
| M2b | **implemented:** `snapshot_at`, OpenCypher page/cancellation | snapshot epoch/error class; a cancelled request yields no page |
| M5b | **implemented:** vertex deletion and `drop_cell` | incident-edge degree projection; post-drop `CellDropped` |

The next implementation order is the broader M2, then M5 P0 semantics. Every
driver runs first against a local in-memory object store, retains the input
trace plus observed projection on failure, and later replays the same corpus
against MinIO. M3–M5 remain subject to their required public semantic
decisions.

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
  --invariant allSafety --max-steps 6

mise exec -- quint run quint-models/turbolay/m1_cell_write.qnt \
  --main m1_cell_write --mbt --max-steps 8 \
  --out-itf target/formal/m1-cell-write-mbt.itf.json

# Rust replay of action-labelled Quint simulation traces.
mise exec -- cargo test --locked --test formal_mbt -- --test-threads=1
mise exec -- cargo test --locked --test formal_mbt_p2 -- --test-threads=1
```

## Review requested

1. Approve or change the BFG-003 relationship external-ID scope.
2. Approve whether BFG-004's conflict rejection is the public batch contract.
3. Choose BFG-008: stable direct pagination or explicitly best-effort offset
   pagination.
4. Confirm that the broader M2 then M5 P0 adapters are the desired next Rust
   MBT implementation order after M1/M2b/M5b.
