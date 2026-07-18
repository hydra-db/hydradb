---
title: "Formal Methods 0002: Turbolay Quint Specification and MBT Plan"
status: proposed
date: 2026-07-18
related:
  - 0001-quint-jepsen-testing-objective.md
  - ../discord/2026-07-09-slatedb-reader-sync.md
---

# Formal Methods 0002: Turbolay Quint Specification and MBT Plan

## Decision requested

Approve the model shapes, abstraction boundary, action names, properties,
deterministic scenarios, and verification gates below. Once approved, these
become the source-of-truth plan for the Quint files and the Rust MBT adapter.
An invariant will never be weakened merely to make an implementation test pass;
a semantic change requires a proposed spec change and renewed approval.

## Modeling decisions

### Plain shared-state Quint, not Choreo

Turbolay's relevant actors—writer, read-only handle, artifact builder, GC, and
placement candidates—coordinate through shared durable SlateDB/S3 state. They
do not implement a Turbolay-owned message protocol. The models will therefore
use **plain Quint** with a durable `Store` record and small per-actor records;
there is no invented message soup or Choreo layer.

An optional future reader-notification protocol would be different: if it
introduces Turbolay-owned pub/sub messages, it needs its own message-passing
model and a separate Choreo-vs-plain decision. This plan does not claim that
protocol exists today.

### Logical graph state, not physical object bytes

One Quint write action models one successful strict-durability SlateDB
transaction. The model represents graph facts, indexes, degree counters,
idempotency outcomes, topology deltas, manifests, and GC eligibility—not S3
objects, SSTs, WAL bytes, codecs, or compaction mechanics. A crash and lost
response can occur around a durable transition, which is the failure boundary
needed to test recovery and idempotency.

The starting finite domains are two cells, two writer identities, three
vertices, two edge types, three idempotency keys, and one matrix artifact
family. Domains may grow only when necessary to expose a new interleaving;
every Apalache run records its bound.

### Model structure and source correspondence

The files below are created only after approval.

```text
quint-models/
  turbolay/
    README.md
    m1_cell_write.qnt
    m1_cell_write_test.qnt
    m2_snapshot_read.qnt
    m2_snapshot_read_test.qnt
    m3_artifact_gc.qnt
    m3_artifact_gc_test.qnt
    m4_placement_fence.qnt
    m4_placement_fence_test.qnt
    m5_public_commands.qnt
    m5_public_commands_test.qnt
    traces/                    # generated, ignored except curated regressions
```

Main modules contain types, state, named actions, `init`, `step`, invariants,
and witnesses. Test modules import their associated main module and contain
only deterministic `run ...Test` scenarios. The `Test` suffix is required so
`quint test` does not silently skip them.

| Model family | Primary implementation correspondence | Why it is separate |
|---|---|---|
| M1 cell write | `src/shard/lifecycle.rs`, `src/shard/write.rs`, `src/core/state.rs` | Atomic durable mutation, epoch, idempotency, and recovery are the foundational state machine. |
| M2 snapshot read | `src/shard/lifecycle.rs`, `src/shard/query.rs`, `src/client/service` | Snapshot pinning and cursor/bookmark behavior need a read-specific state model. |
| M3 artifact and GC | `src/engine/artifact_build.rs`, `src/engine/traversal.rs`, `src/engine/artifact_gc.rs`, `src/shard/maintenance.rs` | Async publication and reclamation are independent from canonical transaction correctness. |
| M4 placement and fence | `src/engine/cluster.rs`, `src/shard/lifecycle.rs` | Placement may disagree while the durable fence remains authoritative. |
| M5 public commands | `src/shard/query.rs`, `src/client/service`, Bolt/HTTP adapters | Maps supported Cypher mutation/read command classes to the M1/M2 graph semantics without trying to model parsing. |

## M1: per-cell durable graph write

### State sketch

```quint
type Edge = { edgeType: str, src: int, dst: int }
type Relationship = { id: int, edge: Edge }
type MutationOutcome = { epoch: int, changed: bool, fingerprint: str }
type Store = {
  epoch:             str -> int,             // cell -> latest committed epoch
  liveEdges:         str -> Set[Edge],
  relationships:     str -> Set[Relationship],
  outAdjacency:      (str, str, int) -> Set[int],
  reverseAdjacency:  (str, str, int) -> Set[int],
  outDegree:         (str, str, int) -> int,
  topologyDeltas:    str -> Set[(int, Edge)],
  idempotency:       (str, str) -> MutationOutcome,
  dropped:           Set[str],
}
type Writer = { epoch: int, live: bool }

var store: Store
var writers: str -> Writer
var lastAction: str
```

The concrete implementation uses richer records and optional indexes. The
model's normalized projection is intentionally smaller: every state field must
participate in a load-bearing invariant or a future public-read oracle.

### Named actions

| Action | Real semantic transition | Required outcome |
|---|---|---|
| `openWriter(writer, cell)` | acquire/open fenced writer | writer learns a current effective epoch or cannot become live |
| `createEdge(writer, cell, edge, key)` | `write_edge` / CREATE | atomically insert canonical edge, adjacency, degree, delta, idempotency outcome, and next epoch |
| `deleteEdge(writer, cell, edge, key)` | `delete_edge` / relationship DELETE | atomically remove the modeled projections and record replay-safe outcome |
| `createRelationship(...)` | relationship-ID create | relationship id is not duplicated and endpoint projections agree |
| `detachDeleteVertex(...)` | detach vertex delete | removes every modeled incident relationship before returning success |
| `commitThenLoseReply(...)` | timeout after durable commit | performs the durable transition while recording an unknown client outcome |
| `retryIdempotent(...)` | retry same request/key | returns the stored outcome and does not advance epoch or duplicate state |
| `retryConflicting(...)` | same key, different fingerprint | is disabled/rejected and changes no durable state |
| `crashWriter(writer)` | process loss | writer becomes non-live; durable store does not roll back |
| `takeOver(writer, cell)` | replacement opens cell | replacement receives a higher effective epoch |
| `zombieWrite(...)` | stale process attempts a write | is disabled and makes no state change |
| `dropCell(...)` | cell deletion | marks the cell dropped; later writes are disabled |

`step` chooses among all enabled actions. Each action assigns every state
variable and updates `lastAction`, so a witness proves that a particular
transition—not merely a coincidentally similar state—was exercised.

### Invariants and witnesses

| Name | Predicate | Primary bug class |
|---|---|---|
| `epochStrictlyAdvances` | every accepted new mutation advances that cell's epoch exactly once | reused/lost epoch |
| `edgeProjectionConsistent` | live edge, adjacency, reverse index when enabled, and degree are mutually consistent | partial write/index drift |
| `relationshipProjectionConsistent` | every live relationship has one canonical id and consistent endpoint projections | duplicate/half-created relationship |
| `idempotencyExact` | each key maps to one fingerprint/outcome; retry does not add state | duplicate write after timeout |
| `deltaMatchesCanonicalChange` | every successful topology change has one corresponding modeled delta | matrix lag/missing update |
| `fencedWriterCannotCommit` | a non-current writer cannot alter the store | zombie writer after takeover |
| `droppedCellIsImmutable` | dropped cells never accept modeled writes | deleted-cell resurrection |
| `createEdgeReached`, etc. | `lastAction == <action>` | dead action / missing model coverage |

### Deterministic scenarios

- Create → delete → retry delete: replay result is stable and degrees never go
  negative.
- Commit then lose reply → crash → new writer → retry: exactly one logical
  edge and one epoch increment exist.
- Old writer pauses → replacement takes over → old writer attempts mutation:
  old action is blocked and replacement's data remains intact.
- Two incident edges → `detachDeleteVertex`: no edge is observable through
  canonical or adjacency projection.
- Same idempotency key with a changed edge/fingerprint: conflict is rejected.

### Apalache targets

Run bounded proofs for `edgeProjectionConsistent`, `idempotencyExact`, and
`fencedWriterCannotCommit`, first at 6 then 8 steps. The model must remain
Apalache-friendly: finite enumerations, no recursion, no unbounded containers,
and no polymorphic `None` inside a single operator.

## M2: snapshot, query, page, and bookmark semantics

### State and boundary

M2 represents each write as a durable `GraphView` at its committed epoch and a
snapshot as `{ cell, storageSeq, graphView }`. A query reads only the view
captured by its snapshot; it never invokes the M1 current-state oracle midway
through evaluation.

The current `snapshot_at` behavior is modelled honestly: future epochs and
unsupported historical graph epochs are errors. The model does **not** assume
that arbitrary old graph epochs can be reconstructed.

### Named actions

| Action | Real correspondence | Property |
|---|---|---|
| `commitWrite` | M1 projection handoff | creates a new current view |
| `openSnapshot(cell)` | `GraphShard::snapshot` | pins one current sequence/view |
| `requestSnapshotAt(cell, epoch)` | `snapshot_at` | succeeds only for supported current state; otherwise records typed rejection |
| `readEdge(snapshot, edge)` | `edge_exists_at` | matches pinned view |
| `readNeighbors(snapshot, source)` | `out_neighbors_at` | matches pinned view and deterministic set order |
| `readDegree(snapshot, source)` | `out_degree_at` | equals pinned adjacency cardinality |
| `startCursor(snapshot, query)` | public query service | creates cursor with snapshot epoch |
| `fetchCursorPage(cursor)` | Bolt/HTTPS page continuation | returns disjoint ordered slice from that cursor's pinned result |
| `cancelCursor(cursor)` | public cancellation | later fetch is rejected and has no graph effect |
| `advanceSessionBookmark` | scoped public bookmark | does not regress within one client/scope |

### Invariants and tests

- `snapshotReadsAreCoherent`: edge, neighbors, and degree returned for a
  snapshot derive from one `GraphView`.
- `futureSnapshotsRejected`: no read result is emitted for a future epoch.
- `cursorPinnedToSnapshot`: concatenating cursor pages equals its original
  pinned query result even when writes occur after page one.
- `cursorPagesPartitionResult`: page rows are ordered, non-overlapping, and
  complete on exhaustion.
- `bookmarkMonotone`: a session bookmark never decreases for a scope.
- `readOnlyHandleHasNoWriteAction`: read-only actors have no enabled transition
  that changes durable graph state or writer epoch.

Read replicas receive only a safety-only model at this stage: a lagging reader
may return an earlier complete view, but never a view that was not durably
committed or one that combines epochs. A future freshness guarantee requires
approval of the graph-watermark/change-log protocol described in the reader
sync document.

## M3: matrix artifact publication and GC

### State sketch and actions

M3 tracks canonical edges by epoch, immutable candidate artifacts, published
manifests, active snapshot references, and retained delta intervals. It models
the following actions:

- `writeTopologyChange`: append canonical epoch and delta.
- `startArtifactBuild(B)`: pins an existing base epoch.
- `uploadArtifactPart`: makes a candidate incomplete/complete without publishing
  it.
- `publishManifest(B)`: enabled only when the named candidate is complete and
  the builder is current for the cell.
- `queryMatrix(S)`: evaluates base `B` plus deltas `(B,S]`.
- `queryDirect(S)`: evaluates canonical graph at `S`.
- `beginRead(S)` / `endRead`: records active reader retention.
- `gcDeltas` / `gcArtifacts`: remove only unreferenced, no-longer-needed data.
- `fenceBuilder`: prevents stale builders from publishing.

### Properties

- `publishedArtifactComplete`: every published manifest names a complete,
  base-consistent candidate.
- `matrixEqualsDirect`: for all modeled reachability requests,
  `matrix(B) + deltas(B,S] == direct(S)`.
- `manifestRetainsRequiredDeltas`: a published manifest's required interval is
  not collected.
- `activeSnapshotRetainsRequiredData`: an active read's base/deltas are not
  collected.
- `staleBuilderCannotPublish`: fencing a builder prevents its later manifest
  publication.

The crucial deterministic test builds an artifact at `B`, applies a create and
delete through `S`, interleaves publication/GC attempts, and checks direct and
matrix traversal equivalence at every legal ordering.

## M4: routing, membership, and durable fence

M4 deliberately separates **candidate owner** from **effective writer**. It
models nodes with local membership views, a deterministic rendezvous choice,
and the durable epoch held by the store.

| Action | Meaning |
|---|---|
| `changeMembershipView(node, members)` | local Kubernetes/discovery view changes |
| `choosePlacement(node, cell)` | node computes a candidate rendezvous owner |
| `openAsCandidate(node, cell)` | node tries to become the durable writer |
| `commitAsOwner(node, cell)` | succeeds only with current durable epoch |
| `partitionNode` / `recoverNode` | candidate's local knowledge can lag or be absent |
| `closeOwner` | clean owner shutdown |

Properties:

- `atMostOneEffectiveWriter`: two nodes cannot both have an enabled durable
  write transition for a cell.
- `commitsRespectDurableEpoch`: a commit from a losing/stale candidate is
  rejected or linearized before the winner's fence.
- `placementDisagreementIsSafe`: membership disagreement can harm availability
  but cannot create two effective writers.
- `takeoverPreservesCommittedPrefix`: a replacement begins from the complete
  durable state of accepted predecessor commits.

## M5: public command conformance

M5 is a thin refinement model rather than a parser model. It maps a finite
subset of supported public commands to M1/M2 logical operations:

| Command class | Model operation |
|---|---|
| Cypher `CREATE` edge | `createEdge` |
| Cypher `MERGE` edge | idempotent logical create with stated existing-edge outcome |
| Cypher relationship `DELETE` | `deleteEdge` or `deleteRelationship` |
| Cypher `DELETE` / `DETACH DELETE` vertex | vertex removal transition |
| Cypher `SET` / `REMOVE` | metadata projection update |
| `MATCH ... RETURN`, bounded reachability | snapshot query oracle |
| Bolt/HTTP pagination and cancellation | M2 cursor transition |

M5 does not parse Cypher grammar or reproduce optimizer choices. Its adapter
executes the actual text/API calls and compares their normalized output with
the command model. Parser and broad compatibility testing stay in existing
query tests and TCK work.

## MBT adapter plan

### Trace format and generated data

Each main module has an analysis instance with concrete finite constants. Run
Quint with `--mbt --out-itf` to produce traces. Curated regression traces are
committed only when they represent a discovered bug or a particularly valuable
boundary; routine generated traces stay under `target/formal/`.

Each ITF action contains a stable name and simple scalar/record arguments.
Avoid making implementation-private storage keys part of the trace format. This
keeps the MBT interface stable across internal refactors while still binding
the externally visible semantics.

### Rust harness phases

1. Add test-only `quint-connect` and `itf` dependencies after verifying their
   resolved versions against the current Rust toolchain.
2. Add a trace runner that creates an isolated local object store and a
   `GraphShard::open_standalone_writer`/read-only handle for every trace.
3. Implement M1 action adapters one at a time. After every action, derive a
   normalized projection using graph APIs—edge existence, neighbors, degrees,
   relationship records, epoch, and verifier digest—and compare to the model
   projection.
4. Repeat the trace corpus against MinIO. The adapter must not inspect private
   SlateDB files as its oracle.
5. Add M2, M3, M4, and M5 adapters incrementally, each guarded by its model's
   Quint tests and simulation properties.

The harness records the source trace, action number, graph scope/cell, input,
result, normalized actual state, expected model state, and the object-store
prefix. A mismatch must be replayable from that artifact alone.

## Commands and gates

All shell and Quint commands are executed from the requested tmux pane
`pson:10.2`.

| Gate | Command shape | Required result |
|---|---|---|
| Parse and typecheck | `mise exec -- quint typecheck quint-models/turbolay/m1_cell_write.qnt` | Exit 0 |
| Scenario tests | `mise exec -- quint test quint-models/turbolay/m1_cell_write_test.qnt --main m1_cell_write_test --match '.*Test$'` | Every named scenario executes and passes |
| Simulation | `mise exec -- quint run <model>.qnt --main <model>Analysis --invariants <...> --witnesses <...> --max-steps <n>` | No invariant counterexample; every witness reached |
| MBT trace | `mise exec -- quint run <model>.qnt --main <model>Analysis --mbt --out-itf target/formal/<name>.itf.json` | Valid trace with action coverage |
| Rust replay | `cargo test --locked --test formal_mbt_<family>` | Actual normalized state refines trace state |
| Bounded proof | `mise exec -- quint verify <model>.qnt --main <model>Analysis --invariants <...> --max-steps <n>` | No counterexample for documented finite bound |

The exact generated-test target and Cargo feature names are decided in the MBT
implementation step after checking the current test layout and resolved
`quint-connect` API. This avoids committing a speculative library interface in
the plan.

## Implementation order and stop conditions

1. Create M1 types, one successful edge creation, its witness, and
   `edgeProjectionConsistent`. Typecheck and test it before adding recovery.
2. Add M1 delete, idempotency, loss/retry, fencing, relationship, and vertex
   scenarios. Do not begin M2 until M1's simulation gates are green.
3. Implement M2 snapshot and cursor semantics. Do not state a freshness SLA for
   read replicas.
4. Implement M3 matrix/GC with direct-traversal oracle.
5. Implement M4 placement/fence, then M5 public-command refinement.
6. Generate ITF for M1 and bind the first Rust MBT adapter. Extend MBT only
   after every preceding model is stable.
7. Enable targeted Apalache proofs once a JRE is available and model simulation
   has already found/fixed its errors.
8. Build the Jepsen harness and transfer the same operation vocabulary,
   idempotency rules, end-state verifier, and known model counterexamples.

Stop and return to planning if any invariant fails, an action is unreachable,
the implementation needs a behavior not represented by the approved model, or
the test exposes a previously unstated consistency guarantee. Do not solve
such a problem by weakening the model or by adding hidden assumptions.

## Review checklist

- [ ] The M1 normalized projection includes every derived record whose
      inconsistency could change a graph read.
- [ ] Every API in the objective's matrix maps to a model operation or has a
      documented non-goal/test technique.
- [ ] Every action has a witness and a deterministic scenario where practical.
- [ ] Every stated invariant can be evaluated from model state without an
      undefined map lookup or implementation-private byte encoding.
- [ ] Apalache targets are finite, non-recursive, and have explicit bounds.
- [ ] ITF action names/arguments are stable semantic vocabulary, not Rust
      implementation details.
- [ ] MBT compares public normalized state after every action and preserves
      replay artifacts.
- [ ] Reader-replica freshness is not claimed before its protocol is designed.
