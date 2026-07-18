---
title: "Formal Methods 0005: Next Steps and Completion Gates"
status: active
date: 2026-07-18
branch: Turbolay-V3
depends_on:
  - 0002-turbolay-quint-specification-plan.md
  - 0003-turbolay-quint-verification-evidence.md
  - 0004-api-coverage-completion-priority.md
tags: [quint, apalache, mbt, jepsen, minio, ci]
---

# Next steps and completion gates

## Current state

The top-ten API contracts have Quint models, deterministic witnesses, bounded
Apalache checks, and focused Rust conformance tests. Rust MBT replay is wired
for the six default adapter binaries: M1, M2, M3, M4, M5, and P2. The P2
binary covers M5b by default and covers M2b cancellation when built with
OpenCypher. These checks currently use an in-memory object store for fast
deterministic feedback; this document does not claim MinIO/S3 or Jepsen
completion.

All formal-methods and test work described here remains on `Turbolay-V3`.

## Ordered work

### 1. Contract review

Review and approve the explicit defaults before expanding distributed tests:

| Finding | Decision required |
|---|---|
| BFG-003 | Confirm whether external relationship IDs are cell-global. |
| BFG-004 | Confirm that conflicting duplicate vertex rows reject atomically. |
| BFG-007 | Confirm proof-or-error bookmark safety and whether a freshness SLA is required. |
| BFG-008 | Confirm best-effort direct offsets versus a stable snapshot-bearing cursor. |

If a decision changes, update the corresponding Quint action, witness, Rust
adapter, and bug record together.

### 2. M3 and M4 Rust MBT adapters — complete for finite `InMemory` replay

Public-API Quint Connect drivers are now wired for:

- M3 artifact publication, stale-publication rejection, retained owned readers,
  GC gated by a published artifact, and direct/matrix equivalence.
- M4 placement disagreement, durable writer fencing, takeover, committed-prefix
  monotonicity, and zombie-writer rejection through the previously opened old
  writer handle.

Each driver compares a normalized public projection after every action and
retains the failing seed, action trace, and observed state. The observable
scope is still finite seeded replay on `InMemory`; S3-compatible storage and
Jepsen remain later gates.

### 3. Replay against S3-compatible storage

Run the complete MBT corpus against MinIO or the configured S3-compatible
backend after the in-memory runs pass. Preserve the same seeds so failures can
be compared across storage backends.

Required evidence:

- object-store endpoint and configuration;
- Quint seed and action trace;
- Rust observed projection and error;
- relevant object prefix and service logs;
- whether the failure reproduces with `InMemory`.

Long-running Quint, Apalache, and Cargo commands must run in tmux pane
`pson:10.2`.

### 4. Run Jepsen campaigns

Use the same operation vocabulary as the Quint models:

| Campaign | Main operations | Nemesis | Checker |
|---|---|---|---|
| Write atomicity | create/delete, retries, conflicting keys | timeout, partition, restart | accepted-operation linearizability and fresh graph digest |
| Snapshot pages | open/read/page, mutation between pages | writer pause, query timeout | pinned-page safety and approved BFG-008 behavior |
| Ownership | routed writes and takeover | old-owner pause, membership partition | no zombie commits and monotone committed prefix |
| Artifacts/GC | topology writes, direct/matrix reads | builder pause/kill, S3 latency | retained reads and direct/matrix equivalence |
| API semantics | relationship merge, duplicate batch, cursor cancel | retry, restart, cancellation | approved identity, batch, and cursor contracts |

Start with no-fault histories, then introduce one nemesis class at a time.
Minimize every failure before classifying it.

### 5. Record and regress bugs

For every minimized Jepsen or formal counterexample, add a file under
`docs/bugs-found-fixed/` with YAML metadata containing:

- bug ID and status (`found`, `fixed`, `regression`, or `accepted-contract`);
- first failing commit and fixing commit;
- affected API/model and reproducible seed or history;
- impact and intended behavior;
- Quint witness and Jepsen history references.

Fixed bugs must retain a regression witness on the V3 branch.

### 6. Add CI gates

The CI pipeline should run, in increasing cost order:

1. Quint typechecks and deterministic witnesses.
2. Six-step Apalache checks for each aggregate safety invariant.
3. Rust unit/conformance tests and all Quint Connect adapters.
4. A bounded MinIO MBT corpus.
5. Scheduled Jepsen campaigns with retained histories and artifacts.

CI failures must report the model, seed, action trace, commit, and storage
backend. A passing in-memory check must not be treated as an S3/Jepsen pass.

## Completion definition

This workstream is complete when:

- the contract decisions above are approved;
- M3 and M4 have Rust MBT adapters (satisfied for finite `InMemory` replay);
- all adapters pass against both `InMemory` and MinIO/S3;
- each Jepsen campaign has a passing baseline and documented failure policy;
- every discovered bug has a minimized witness and a regression record; and
- CI runs the required formal and distributed checks on `Turbolay-V3`.

## Review gate

Please review the contract table first. After approval, implement M3/M4 MBT
adapters, then schedule the MinIO and Jepsen campaigns in that order.
