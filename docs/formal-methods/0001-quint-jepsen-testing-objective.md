---
title: "Formal Methods 0001: Turbolay Quint and Jepsen Testing Objective"
status: proposed
date: 2026-07-18
owners:
  - Turbolay storage and query maintainers
depends_on:
  - 0002-turbolay-quint-specification-plan.md
---

# Formal Methods 0001: Turbolay Quint and Jepsen Testing Objective

## Decision requested

Approve the system contract, API scope, and verification sequence in this document
and in [0002](0002-turbolay-quint-specification-plan.md). Approval authorizes
creation of the Quint models, deterministic Quint tests, ITF traces, and the
local Rust model-based-test (MBT) harness. It does **not** silently authorize a
change to production semantics.

## Objective

Establish evidence that Turbolay preserves graph correctness when its
S3/SlateDB-backed state is mutated, read, recovered, and maintained under the
failure modes the design claims to tolerate. The evidence is deliberately
layered:

1. **Quint** makes the intended state transitions and safety properties small,
   explicit, reviewable, and executable.
2. **Apalache through `quint verify`** gives bounded exhaustive checks for the
   few highest-value properties once the models have been debugged with Quint
   simulations.
3. **Rust MBT** replays generated Informal Trace Format (ITF) action traces
   against Turbolay's public kernel API, showing that the real implementation
   refines the checked model for a bounded domain.
4. **Jepsen** executes the public service against real processes and a
   S3-compatible store, injecting process, network, and ownership faults that
   a model cannot reproduce.

The result is not a proof of S3, SlateDB, Kubernetes, or an unbounded graph.
It is a precise safety contract for Turbolay's own protocol, tests that can
find violations of that contract, and operational histories that exercise it
in the deployed architecture.

## System contract being tested

The following are the intended, testable claims for the current repository.

| Area | Contract under test | Evidence in the implementation |
|---|---|---|
| Per-cell writes | A successful graph mutation is a durable, serializable SlateDB transaction. It either updates every canonical and derived record required by that mutation or none. | `src/shard/write.rs` begins `SerializableSnapshot` transactions, validates the fence, writes canonical/index/degree/delta state, and commits. |
| Writer ownership | A cell has one effective writer. Rendezvous placement is advisory; SlateDB writer epoch and WAL durability are the stale-writer authority. | [storage and ownership model](../../README.md#storage-and-ownership-model), `src/engine/cluster.rs`, `src/shard/lifecycle.rs`. |
| Retry safety | Repeating an accepted mutation with its idempotency key returns the original result and does not apply another logical mutation. A conflicting re-use of a key is rejected. | `src/shard/write.rs` idempotency records and decode paths. |
| Current reads | A one-shot graph query runs on one `DbSnapshot`; it must not combine canonical adjacency, metadata, and indexes from different storage sequences. | `src/shard/lifecycle.rs::snapshot`, `src/shard/query.rs`. |
| Historical reads | `snapshot_at` accepts only the currently supported storage snapshot semantics; it must reject future and unavailable historical graph epochs rather than fabricate a result. | `src/shard/lifecycle.rs::snapshot_at`. |
| Async matrix path | An asynchronous matrix artifact is an optimization, not a second source of truth. A traversal over base epoch `B` plus deltas `(B,S]` must equal direct canonical-snapshot traversal at `S`. | [storage and ownership model](../../README.md#storage-and-ownership-model), `src/engine/artifact_build.rs`, `src/engine/traversal.rs`. |
| Maintenance | Delta and artifact GC may reclaim only data that no published artifact or active read still requires. | `src/shard/maintenance.rs`, `src/engine/artifact_gc.rs`. |
| Read replicas | Read-only opens are safe. No bounded-staleness, cache-coherence, or read-your-writes guarantee is claimed for separately deployed replicas until the graph-watermark/change-log protocol is implemented and specified. | `docs/discord/2026-07-09-slatedb-reader-sync.md`. |

The key boundary is important: write throughput scales by **cells**, not by
concurrent writers to one hot cell. Independent read-replica deployment and
coherence are design work, not a completed production contract.

## Explicit non-goals and assumptions

The models use a shared-state abstraction: a durable SlateDB transaction is one
atomic state update. They do not model byte encoding, LSM compaction, S3's
implementation, Rust scheduling, TLS, parsing, or Kubernetes internals.

This is a deliberate abstraction, not an assertion that those layers are
unimportant. Each layer has a more appropriate test technique:

| Concern | Technique outside these Quint models |
|---|---|
| Key encoding, codecs, limits, malformed data | Rust unit and property tests |
| SlateDB atomic commit, object visibility, writer fencing | Upstream SlateDB contract plus Turbolay integration and Jepsen tests |
| Real S3 retries, latency, throttling, credentials | MinIO fault tests followed by AWS S3 soak tests |
| OpenCypher grammar and compatibility | Existing query correctness/TCK work |
| Throughput, cache sizing, and cost | Benchmarks and operational load tests |

The current public surface also has no cross-query, cross-cell transaction
contract: explicit Bolt `BEGIN`/`COMMIT`/`ROLLBACK` are rejected. Jepsen and
MBT must therefore not report a failure merely because a multi-cell operation
is not serializable as one global transaction.

## API coverage matrix

The goal is semantic coverage, rather than one formal model per Rust method.
Equivalent entry points must refine the same logical transition.

| Semantic operation | APIs to bind or exercise | Formal oracle | Jepsen history |
|---|---|---|---|
| Create/upsert one edge | `write_edge`; Cypher `CREATE`/`MERGE` | Atomic edge, adjacency, degree, topology delta, and idempotency update | Per-edge register/set with competing writers and retries |
| Delete one edge | `delete_edge`; Cypher relationship `DELETE` | Removal, degree change, tombstone/delta, replay-safe idempotency | Create/delete races and timeout retry |
| Batch and ingestion | `write_edge_mutations_batch`, `write_edges_batch`, `ingest_edge_mutations`, `bulk_import_edges`, trusted append APIs | All-or-nothing for each stated atomic unit; deterministic chunk boundaries where the API intentionally chunks | Kill/restart and timeout during a batch; compare accepted units only |
| Relationships and metadata | `create_relationship*`, `delete_relationship`, vertex/edge metadata setters, Cypher `SET`/`REMOVE` | Relationship identity is unique; endpoints and metadata remain mutually consistent | Concurrent create/delete/update with snapshot query oracle |
| Vertex removal | `delete_vertex`, `detach_delete_vertex`, Cypher `DELETE` / `DETACH DELETE` | Ordinary delete is rejected or leaves stated state; detach removal leaves no incident canonical edge | Concurrent incident-edge writes, delete, retry, and recovery |
| Snapshot reads | `snapshot`, `edge_exists_at`, `out_neighbors_at`, `out_degree_at`, OpenCypher query execution | Query is evaluated from one pinned logical snapshot | Reads interleaved with writes; linearizability checker only for explicitly atomic operations |
| Cursor and bookmark reads | public HTTPS/Bolt pages, cancellation, bookmarks | Pages are from the pinned cursor snapshot; bookmark never regresses within one scope/principal | Fetch page 1, mutate, fetch later pages, retry/cancel |
| Routing and takeover | `RoutedGraphCluster::open_fenced_owned*`, write routing | A post-fence zombie cannot commit; a successful replacement writer sees all prior durable commits | Pause old owner, start replacement, partition clients/store, then recover |
| Matrix artifacts | `build_adjacency_image`, `build_matrix_tiles`, `matrix_reachable*`, `direct_snapshot_reachable` | `matrix(B) + deltas(B,S] == direct(S)` | Writes/deletes concurrent with build and query |
| GC and verification | `delete_deltas_through_matrix`, `delete_graph_artifacts_before`, `verify_current_graph`, `export_live_graph_digest` | Reclamation preserves every required published/readable state | Kill during maintenance; fresh-reader digest and verifier at test end |

Trusted import/append APIs are deliberately included but have narrower caller
preconditions. Their MBT domain will first verify the graph invariants they are
responsible for, then separately test documented rejection behavior for invalid
input. It will not pretend that a caller violating a `trusted` API's stated
precondition is a normal transaction client.

## Properties that must become executable

Each property below will be named in a Quint module, exercised by a deterministic
`*_test.qnt` scenario, and checked by simulation. The selected safety
properties will later be checked exhaustively to a stated small bound with
Apalache.

1. **Atomic graph mutation.** A committed edge/relationship mutation has a
   matching canonical record, required endpoint metadata, adjacency posting,
   optional reverse posting, degree count, topology delta/outbox record, and
   idempotency outcome. No partial projection is observable.
2. **Epoch and fence safety.** Commit epochs for a cell increase strictly. A
   writer whose epoch is no longer current cannot make a durable mutation or
   issue a successful outcome.
3. **Ambiguous-result recovery and idempotency.** If a transaction commits but
   its response is lost, retrying the same idempotency key yields the same
   logical result without a duplicate edge, degree increment, or delta.
4. **Delete and detach correctness.** A deleted edge is absent from all
   canonical/read indexes. `DETACH DELETE` leaves no live incident relationship
   or degree contribution.
5. **Snapshot consistency.** Every modeled query result is produced from one
   storage sequence. Future/unavailable snapshots fail rather than mix state.
6. **Artifact equivalence and safe publication.** A published matrix manifest
   names a complete base artifact. Base-plus-delta traversal equals direct
   canonical traversal at the query snapshot.
7. **GC safety.** GC cannot remove an artifact or delta that a published
   manifest or active snapshot needs.
8. **Placement is not authority.** Divergent membership views may produce
   competing candidates, but the durable writer fence determines the one
   effective writer.

## Verification pipeline

### 1. Quint modeling and deterministic scenarios

Use plain Quint in shared-state style. Each model has a small finite domain,
named actions, state-transition witnesses, and separate `*_test.qnt` scenario
files. `quint typecheck` and `quint test` are required after every edit.

`quint run` is then used with all safety invariants and state-change witnesses.
No counterexample in a sampled simulation is reported as simulation evidence,
not a proof.

### 2. Bounded model checking

After a model is stable, use `quint verify` / Apalache for the safety properties
listed in the implementation plan. Every bounded-proof report records the model
version, module instance, constants, invariant list, max steps, tool versions,
and result.

Quint is available through Mise (`quint 0.32.0`). The current Apalache launcher
cannot start because the host lacks a Java runtime. Installing or exposing a
supported JRE is a prerequisite for this stage; it is not a reason to skip the
earlier Quint, ITF, MBT, or Jepsen stages.

### 3. Rust model-based testing

Generate finite action traces with:

```bash
mise exec -- quint run quint-models/turbolay/<model>.qnt \
  --main <model>Analysis --mbt --out-itf target/formal/<trace>.itf.json
```

The Rust adapter will use `quint-connect` plus the `itf` crate to decode each
trace and map every named Quint action to a real Turbolay call. After each
action it reads a normalized graph projection through the kernel API and
compares it with the Quint state projection. The first adapter runs against a
fresh local object store; the same trace corpus then runs on MinIO.

This distinguishes the components correctly: Quint produces action traces,
`quint-connect`/ITF is the Rust MBT bridge, and Apalache is the bounded model
checker. They complement one another rather than being interchangeable.

### 4. Jepsen

Run Jepsen after the per-cell model and local MBT harness are trusted. The
initial topology is three Turbolay writer-capable nodes with stable identities,
one or more test cells, MinIO, and Jepsen client workers. A custom test-only
read-only endpoint is needed before testing separate readers because the
production graph-node/Helm deployment does not currently expose one.

The staged Jepsen campaigns are:

1. No faults: small per-edge register/set histories and single-cell
   linearizability checks.
2. Ambiguous client outcomes: request timeouts and client-to-node partitions;
   retry identical idempotency keys.
3. Process faults: kill, restart, and SIGSTOP/SIGCONT writers while mutations,
   batches, and query cursors run.
4. Storage faults: node-to-MinIO loss/latency/timeout, MinIO restart, then a
   real-S3 soak with service-appropriate fault injection.
5. Ownership faults: old/new owner overlap and membership disagreement;
   require stale-writer fencing and eventual recovery.
6. Maintenance faults: build/GC concurrent with writes and direct-vs-matrix
   comparison.

Every Jepsen run retains the history, configuration, process logs, object-store
prefix, and a post-run fresh-reader `verify_current_graph` plus graph digest.
Failures are minimized and become deterministic Quint/MBT scenarios whenever
their logical cause can be expressed in the bounded model.

## Completion criteria

The formal-methods goal is complete only when all of the following are true:

- The approved Quint modules and scenario tests in 0002 exist, typecheck, and
  have named state-change witnesses.
- Required simulation runs report no counterexample for their safety invariants
  at their documented bounds; witnesses are reached.
- The selected Apalache proofs complete after a working JRE is available, or an
  explicit environment block and reproducible command/output are recorded.
- ITF traces cover every MBT-bound action, and the Rust adapter passes them on
  local storage and MinIO.
- The scoped Jepsen campaigns run with their stated checkers and nemeses; any
  violation is triaged as an implementation bug, a specification bug requiring
  approval, or an unsupported-contract finding.
- CI has a fast Quint typecheck/test/simulation gate. Longer MBT, Apalache, and
  Jepsen jobs are scheduled with preserved artifacts.

## Approval questions

1. Is the listed per-cell contract the desired public correctness contract?
2. Should the first implementation include all five proposed model families,
   including placement and matrix/GC, or stop after write and snapshot models?
3. Is a local-store then MinIO MBT progression acceptable before the Jepsen
   infrastructure is introduced?
