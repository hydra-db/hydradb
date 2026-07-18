#import "../vendor/bookly/src/bookly.typ": *
#import "@preview/fletcher:0.5.7" as fletcher: diagram, node, edge

= Reading your first real model: the write path

The previous chapter taught you Quint from zero: `module`, `var`, `action`, `all`,
`any`, primed variables, and `val`. You can now read those keywords the way you read
`for` and `if`. This chapter puts that reading skill to work on a real artifact from the
turbolay repository. By the end you will be able to open
`quint-models/turbolay/m1_cell_write.qnt`, read it top to bottom, and say, for each line,
_what claim about the write path it is making_ and _where in the Rust that claim lives_.

We are not going to write a model here, and we are not going to run one. We are going to
_read_ one. That is a distinct and underrated skill. A formal model is only useful if a
human can check that it says what it is supposed to say; a model nobody can read is a
second program to get wrong, not a check on the first. So we take `m1_cell_write.qnt`
slowly, one guard at a time, and keep asking the same two questions: what does this
transition assert, and does the real `src/shard/write.rs` actually behave that way?

Keep the write-path chapter within reach. Every action in this model is a shadow of
something concrete there: `write_edge`, `delete_edge`, the write fence, the idempotency
record, the epoch counter. The model's whole job is to be a small, exhaustively-checkable
statement of the contract that the 5,700-line `write.rs` is supposed to honor.

== What the model is a model of

Before the first line of Quint, fix the boundary. The write-path chapter described a
mutation as a five-layer envelope: validate and check the in-memory lease, take a global
write permit, lock the cell's write lane, enter a bounded retry loop, take an object-store
lock, and only then open a SlateDB transaction that checks the fence, advances the epoch,
writes a fan-out of keys, and commits atomically. That is a lot of moving parts. A model
that reproduced all of them would be as hard to trust as the code.

So the model does not reproduce them. It collapses the entire durable transaction into a
single atomic state update.

#custom-box(title: [Term — Shared-state abstraction], icon: "info", color: purple)[
  A modeling style in which the whole system is one shared collection of variables, and
  every operation is an atomic transition that reads some of them and writes some of them
  in one indivisible step. There are no threads, no messages in flight, no partial writes.
  The durable state _is_ the model's variables, and a committed transaction _is_ one
  assignment to them.
]

This is exactly the abstraction the formal-methods objective document commits to. Its
"Explicit non-goals" section states the boundary in one sentence,
`0001-quint-jepsen-testing-objective.md:67-69`:

```
The models use a shared-state abstraction: a durable SlateDB transaction is one
atomic state update. They do not model byte encoding, LSM compaction, S3's
implementation, Rust scheduling, TLS, parsing, or Kubernetes internals.
```

#custom-box(title: [Why], icon: "tip", color: rgb("#c99700"))[
  Why throw away so much? Because each discarded layer has a _better_ test technique than a
  state-machine model, and mixing them in would only dilute the one thing this model is
  good at. The same document pairs every excluded concern with its proper tool: key
  encoding and codecs go to Rust property tests; SlateDB's atomic commit and fencing are an
  upstream contract exercised by integration and Jepsen tests; real S3 latency and
  throttling go to MinIO fault tests and an AWS soak. What is left for Quint is the
  _protocol logic_: given that a transaction commits atomically, are the graph's projections
  always mutually consistent, do epochs behave, is a retry exactly-once, can a fenced writer
  ever commit? Those are questions about the shape of the transitions, not about bytes, and
  a small exhaustively-checkable state machine answers them far better than any amount of
  integration testing can.
]

The payoff of the abstraction is that when `createEdge` fires, the edge's adjacency, its
degree count, its topology delta, its idempotency record, and the epoch counter all move
in the same indivisible step. The model cannot even _express_ a state where the adjacency
key exists but its degree counter does not, because there is no moment between the two
assignments. That is precisely the "no partial projection is observable" property from the
objective's property list, made structurally impossible rather than merely tested for.

== The header and the variable block: the state under test

Open the file. The first eight lines are a comment, and they are the most important
comment in the model because they tell you what the variables mean, `m1_cell_write.qnt:1-9`:

```
// -*- mode: quint -*-
//
// M1 — durable per-cell mutation, idempotency, and writer-fence contract.
//
// This deliberately models one structural edge. `edgePresent`, `outDegree`,
// and `deltaEpoch` are the normalized projections which must commit together.
// A real Turbolay mutation has richer records and indexes, but any successful
// mutation must refine this atomic transition.
module m1_cell_write {
```

Two phrases carry the whole design. "One structural edge": the model tracks the fate of a
single edge, not a graph. "The normalized projections which must commit together": the
handful of variables below are not the graph, they are the _consequences_ a successful
mutation must produce, reduced to their essence.

#custom-box(title: [Term — Projection], icon: "info", color: purple)[
  A single derived view of a mutation's effect, reduced to the one fact this model needs to
  check. A real `write_edge` writes an out-adjacency key, an in-adjacency key, two degree
  counters, an outbox delta, owner and pair delta indexes, an idempotency record, and the
  epoch counter. This model keeps `edgePresent` (does the adjacency exist), `outDegree`
  (the degree count), and `deltaEpoch` (which epoch the latest topology delta names). Each
  is a projection of the same physical write.
]

Here is the full variable block, `m1_cell_write.qnt:10-23`. Read it as the schema of the
durable state:

```
  var epoch: int
  var previousEpoch: int
  var edgePresent: bool
  var outDegree: int
  var deltaEpoch: int
  var createRecorded: bool
  var deleteRecorded: bool
  var createOutcomeEpoch: int
  var deleteOutcomeEpoch: int
  var unknownReply: bool
  var activeWriter: int
  var writer1Live: bool
  var writer2Live: bool
  var lastAction: str
```

Sort those fourteen variables into three groups and the model becomes legible.

_The three graph projections_, named in the header comment:

- `edgePresent` — does the one edge exist. This stands in for the `out_edge` / `in_edge`
  adjacency keys and the canonical `edge` record from the write chapter (`keys.rs:51-61`).
- `outDegree` — the source vertex's outgoing degree, either 0 or 1 for one edge. This is
  the `cnt/out` degree counter that lets a reader learn a degree without scanning.
- `deltaEpoch` — the epoch of the most recent topology change, standing in for the `outbox`
  delta record the read path replays.

_The epoch and idempotency bookkeeping_:

- `epoch` and `previousEpoch` — the current cell epoch and its value one step ago. Keeping
  the previous value lets a safety predicate assert the epoch never regresses. This is the
  `cell/<cell_id>/meta/last_epoch` counter.
- `createRecorded`, `deleteRecorded` — whether a create (or delete) idempotency record
  exists for this key. These are the `idem/create/<key>` and `idem/delete/<key>` records.
- `createOutcomeEpoch`, `deleteOutcomeEpoch` — the epoch stored inside that idempotency
  record, i.e. the result a retry replays.
- `unknownReply` — the model's representation of the ambiguous-outcome hazard: the write
  committed durably but the caller never learned it did.

_The writer-fence bookkeeping_:

- `activeWriter` — which writer currently owns the cell: 0 for none, 1 for the original
  owner, 2 for a replacement after takeover. This is the identity named in the write fence.
- `writer1Live`, `writer2Live` — whether each writer process is still up.
- `lastAction` — the name of the action that produced the current state. This is a modeling
  convenience: it lets test scenarios and reachability witnesses say "the last thing that
  happened was a takeover" without adding a real state bit.

That is the entire state space. Fourteen variables, most of them booleans or small
integers. This is the whole point of the abstraction: the state is small enough that a
model checker can walk _every_ reachable configuration, and small enough that you can hold
it in your head while you read the transitions.

== `init`: where every run begins

`init` is the model's constructor. It pins every variable to a starting value, so there is
exactly one initial state and every behavior the checker explores begins here,
`m1_cell_write.qnt:25-40`:

```
  action init: bool = all {
    epoch' = 0,
    previousEpoch' = 0,
    edgePresent' = false,
    outDegree' = 0,
    deltaEpoch' = 0,
    createRecorded' = false,
    deleteRecorded' = false,
    createOutcomeEpoch' = 0,
    deleteOutcomeEpoch' = 0,
    unknownReply' = false,
    activeWriter' = 0,
    writer1Live' = false,
    writer2Live' = false,
    lastAction' = "init",
  }
```

Read it as a sentence: a fresh cell has epoch 0, no edge, degree 0, no delta, no recorded
outcomes, no ambiguity, and _no writer at all_ (`activeWriter' = 0`, both writers not
live). This matches the real engine exactly. The write chapter noted that an absent
`last_epoch` counter reads as zero, so "a fresh cell starts at epoch 0 and its first write
commits at epoch 1." The model bakes that in: nothing has happened yet, and the first thing
that _can_ happen is that a writer opens.

Notice the shape. `init` is `all { ... }`, a conjunction of primed-variable assignments,
one per variable. Every action in this model has that same skeleton: a block of guards
(unprimed facts that must hold for the action to fire) followed by a complete set of primed
assignments (the next state). When you read an action, your eyes should split it into those
two halves. `init` is the degenerate case with no guards — it is always enabled, but only at
the very start.

== The actions, guard by guard

Now the heart of the model. Each action is one operation on the write path. We read them in
roughly the order a real cell lives through: acquire a writer, create, survive an ambiguous
outcome, retry safely, delete, then crash and be fenced. For every action, find the guards
first, then the assignments, then the tie to `write.rs` and to the objective's contract.

=== `openWriter1`: acquiring the writer

Nothing can be written until someone owns the cell. `openWriter1` is that acquisition,
`m1_cell_write.qnt:42-58`:

```
  action openWriter1: bool = all {
    activeWriter == 0,
    epoch' = epoch,
    previousEpoch' = epoch,
    edgePresent' = edgePresent,
    outDegree' = outDegree,
    deltaEpoch' = deltaEpoch,
    createRecorded' = createRecorded,
    deleteRecorded' = deleteRecorded,
    createOutcomeEpoch' = createOutcomeEpoch,
    deleteOutcomeEpoch' = deleteOutcomeEpoch,
    unknownReply' = unknownReply,
    activeWriter' = 1,
    writer1Live' = true,
    writer2Live' = writer2Live,
    lastAction' = "openWriter1",
  }
```

The single guard is `activeWriter == 0`: you can only take ownership of a cell that has no
active writer. Everything else is a frame condition — `edgePresent' = edgePresent`,
`epoch' = epoch`, and so on — the Quint idiom for "this variable does not change." Only
three variables actually move: `activeWriter` becomes 1, `writer1Live` becomes true, and
`previousEpoch` is snapped to the current `epoch` (a bookkeeping move that keeps the
never-regresses predicate honest across this no-op-on-the-graph step).

This is the model's version of turbolay acquiring the write fence and lease before touching
data. In the objective's contract table, this is the "Writer ownership" row: "A cell has
one effective writer." The guard `activeWriter == 0` is how the model refuses a second
concurrent owner. Note what is _abstracted away_: the real acquisition is three tiers deep
(the in-memory `GraphWriteAuthority`, the persisted `write_fence`, the object-store CAS
lock), but from the protocol's point of view the only fact that matters is "ownership moved
from nobody to writer 1," and that is one guard plus one assignment.

=== `createEdge`: the atomic mutation

Here is the transition the whole model exists to justify. Its own comment states the claim,
`m1_cell_write.qnt:60-81`:

```
  // CREATE commits its current edge projection, topology delta, and outcome
  // as one durable state transition.
  action createEdge: bool = all {
    activeWriter == 1,
    writer1Live,
    not(edgePresent),
    not(createRecorded),
    epoch' = epoch + 1,
    previousEpoch' = epoch,
    edgePresent' = true,
    outDegree' = 1,
    deltaEpoch' = epoch + 1,
    createRecorded' = true,
    deleteRecorded' = deleteRecorded,
    createOutcomeEpoch' = epoch + 1,
    deleteOutcomeEpoch' = deleteOutcomeEpoch,
    unknownReply' = false,
    activeWriter' = activeWriter,
    writer1Live' = writer1Live,
    writer2Live' = writer2Live,
    lastAction' = "createEdge",
  }
```

The four guards are the preconditions of a real create. `activeWriter == 1` and
`writer1Live`: you must be the live owner. `not(edgePresent)`: the edge must not already be
there (an existing edge is the redundant-write case that the write chapter said commits
_without_ bumping the epoch — this action is only the genuine insert). `not(createRecorded)`:
no create idempotency record yet, so this is the first time this logical write is applied.

Now the assignments, and this is where the abstraction earns its keep. In one atomic step:

- `epoch' = epoch + 1` — the version advances by exactly one. The write chapter:
  "It reads `last_epoch`, adds one, and writes the new epoch ... in the same transaction as
  the data."
- `edgePresent' = true`, `outDegree' = 1` — the adjacency and the degree counter both move,
  together. No reader can ever catch one without the other.
- `deltaEpoch' = epoch + 1` — the topology delta is stamped with the new epoch, exactly the
  `outbox` record at that epoch that the read merge replays.
- `createRecorded' = true`, `createOutcomeEpoch' = epoch + 1` — the idempotency record is
  written _in the same step_, recording that this write happened and what epoch it produced.
- `unknownReply' = false` — the caller received the acknowledgement.

Line this up against `write.rs`. The real transaction writes `out_edge`, `in_edge`, the
degree counters, the `outbox` delta, the delta indexes, the idempotency record, and
`last_epoch` in one `commit_txn_strict` call (`write.rs:2608-2649`). The model keeps one
projection of each — `edgePresent`, `outDegree`, `deltaEpoch`, `createRecorded` /
`createOutcomeEpoch`, `epoch` — and moves them all in one `all { ... }`. This is what the
header meant by "any successful mutation must refine this atomic transition": the real code
is allowed to do far _more_, but it must never do _less_ or do it non-atomically.

This is the objective's property 1, "Atomic graph mutation," reduced to something a checker
can verify by construction: "A committed edge mutation has a matching canonical record ...
adjacency posting ... degree count ... topology delta ... and idempotency outcome. No
partial projection is observable."

=== `commitThenLoseReply`: the ambiguous outcome

Real systems do not get to assume the caller heard the answer. A transaction can commit
durably and _then_ the acknowledgement is lost — the network drops, the client times out,
the writer crashes a millisecond after the object store accepted the write. The caller is
left not knowing whether its write took. The model gives that hazard its own action,
`m1_cell_write.qnt:83-103`:

```
  // The write committed but the caller did not receive its acknowledgement.
  action commitThenLoseReply: bool = all {
    activeWriter == 1,
    writer1Live,
    not(edgePresent),
    not(createRecorded),
    epoch' = epoch + 1,
    previousEpoch' = epoch,
    edgePresent' = true,
    outDegree' = 1,
    deltaEpoch' = epoch + 1,
    createRecorded' = true,
    deleteRecorded' = deleteRecorded,
    createOutcomeEpoch' = epoch + 1,
    deleteOutcomeEpoch' = deleteOutcomeEpoch,
    unknownReply' = true,
    activeWriter' = activeWriter,
    writer1Live' = writer1Live,
    writer2Live' = writer2Live,
    lastAction' = "commitThenLoseReply",
  }
```

Compare it line by line against `createEdge` and you will find them _identical_ except for
one assignment: `unknownReply' = true` instead of `false`. That is the entire content of the
hazard. The durable effect is the same — the edge is present, the degree is 1, the epoch
advanced, the idempotency record is written. What differs is only the caller's knowledge.
The write happened; the reply did not.

#custom-box(title: [Why], icon: "tip", color: rgb("#c99700"))[
  Why model the lost reply as a separate action rather than a flag on `createEdge`? Because
  it makes the checker enumerate _both_ worlds. In some behaviors the caller learns the
  outcome (`createEdge`); in some it does not (`commitThenLoseReply`). By splitting them into
  two enabled transitions, the model forces exploration of what happens next in each. The
  interesting one is the second: a caller who does not know whether its write landed will
  _retry_. The next action is what makes that retry safe.
]

=== `retryCreate`: idempotency, the exactly-once promise

A caller who lost the reply retries the same logical write with the same idempotency key.
The contract says the retry must return the original result and must _not_ apply a second
mutation. Here is that promise as a transition, `m1_cell_write.qnt:105-123`:

```
  // A matching create retry returns its recorded outcome and does not write.
  action retryCreate: bool = all {
    createRecorded,
    activeWriter != 0,
    epoch' = epoch,
    previousEpoch' = epoch,
    edgePresent' = edgePresent,
    outDegree' = outDegree,
    deltaEpoch' = deltaEpoch,
    createRecorded' = createRecorded,
    deleteRecorded' = deleteRecorded,
    createOutcomeEpoch' = createOutcomeEpoch,
    deleteOutcomeEpoch' = deleteOutcomeEpoch,
    unknownReply' = false,
    activeWriter' = activeWriter,
    writer1Live' = writer1Live,
    writer2Live' = writer2Live,
    lastAction' = "retryCreate",
  }
```

The guards: `createRecorded` (there must be a prior create outcome to replay) and
`activeWriter != 0` (some writer must be active to serve the retry — notice it does _not_
require `writer1Live` specifically, so a replacement writer can serve the retry too). Now
look at the assignments: _every graph projection frames unchanged_. `epoch' = epoch`,
`edgePresent' = edgePresent`, `outDegree' = outDegree`, `deltaEpoch' = deltaEpoch`. The one
meaningful move is `unknownReply' = false`: the retry _resolves_ the ambiguity by handing
back the recorded outcome. No new edge, no degree increment, no delta, and crucially _the
epoch does not advance_.

That last fact — epoch unchanged — is the operational signature of idempotency. The write
chapter's idempotency term put it exactly: "A retry with the same key is detected and
returns the original result instead of applying the change twice." In the real code the
transaction "checks for a prior result ... as its first read after the fence. If it is
present, the write returns the stored result without re-applying." The model is that check,
distilled: if `createRecorded`, replay, do not mutate.

This is the objective's property 3, "Ambiguous-result recovery and idempotency": "If a
transaction commits but its response is lost, retrying the same idempotency key yields the
same logical result without a duplicate edge, degree increment, or delta." Read
`commitThenLoseReply` immediately followed by `retryCreate` as one story and you see the
whole promise enacted: the write lands, the reply is lost, the retry finds the record and
returns the same epoch without touching the graph.

=== `rejectConflictingRetry`: a reused key is not a free pass

Idempotency keys are powerful, which makes them dangerous if misused. If a caller reuses a
key that already named one mutation but now asks for a _different_ mutation, the engine must
refuse — otherwise the key would silently mask a real, different write. The model has an
action for the rejection, `m1_cell_write.qnt:125-143`:

```
  // A reused idempotency key with a different mutation is rejected.
  action rejectConflictingRetry: bool = all {
    createRecorded,
    activeWriter != 0,
    epoch' = epoch,
    previousEpoch' = epoch,
    edgePresent' = edgePresent,
    outDegree' = outDegree,
    deltaEpoch' = deltaEpoch,
    createRecorded' = createRecorded,
    deleteRecorded' = deleteRecorded,
    createOutcomeEpoch' = createOutcomeEpoch,
    deleteOutcomeEpoch' = deleteOutcomeEpoch,
    unknownReply' = unknownReply,
    activeWriter' = activeWriter,
    writer1Live' = writer1Live,
    writer2Live' = writer2Live,
    lastAction' = "rejectConflictingRetry",
  }
```

Same guards as `retryCreate`, and every graph projection frames unchanged again — a
rejection must not mutate anything. The distinction from `retryCreate` is subtle and worth
pausing on: `retryCreate` set `unknownReply' = false` (it resolved the caller's question),
whereas `rejectConflictingRetry` sets `unknownReply' = unknownReply` (it leaves the caller's
ambiguity untouched) and records itself only in `lastAction`. A rejection is an outcome the
caller observes as an error, not as a resolved write. The important safety fact is what it
does _not_ do: it does not advance the epoch, create an edge, or overwrite the recorded
outcome. This is the second half of the objective's "Retry safety" contract row: "A
conflicting re-use of a key is rejected."

=== `deleteEdge` and `retryDelete`: the mirror image

Delete is create's reflection, and its two actions mirror `createEdge` and `retryCreate`
almost line for line. First the delete itself, `m1_cell_write.qnt:145-164`:

```
  action deleteEdge: bool = all {
    activeWriter == 1,
    writer1Live,
    edgePresent,
    not(deleteRecorded),
    epoch' = epoch + 1,
    previousEpoch' = epoch,
    edgePresent' = false,
    outDegree' = 0,
    deltaEpoch' = epoch + 1,
    createRecorded' = createRecorded,
    deleteRecorded' = true,
    createOutcomeEpoch' = createOutcomeEpoch,
    deleteOutcomeEpoch' = epoch + 1,
    unknownReply' = false,
    activeWriter' = activeWriter,
    writer1Live' = writer1Live,
    writer2Live' = writer2Live,
    lastAction' = "deleteEdge",
  }
```

The guards flip the create's: it requires `edgePresent` (you can only delete an edge that
exists) and `not(deleteRecorded)` (first delete of this key). The assignments flip too:
`edgePresent' = false`, `outDegree' = 0`. But the epoch still _advances_ — a delete is a
topology change, so `epoch' = epoch + 1` and `deltaEpoch' = epoch + 1`, stamping a new delta.
In the real engine this is the `write_edge` mirror the write chapter described: "a delete
does the mirror image: it removes the adjacency and canonical keys, decrements the degree
counters, and writes an `outbox` record with `DeltaKind::Minus`." Here the `Minus` delta is
implicit in `deltaEpoch` advancing while `edgePresent` drops to false. The delete records
its own idempotency outcome (`deleteRecorded' = true`, `deleteOutcomeEpoch' = epoch + 1`),
kept separate from the create's, because the real code uses distinct operation namespaces
(`create` vs `delete`) for its idempotency records.

And its retry, identical in spirit to `retryCreate`, `m1_cell_write.qnt:166-183`:

```
  action retryDelete: bool = all {
    deleteRecorded,
    activeWriter != 0,
    epoch' = epoch,
    previousEpoch' = epoch,
    edgePresent' = edgePresent,
    outDegree' = outDegree,
    deltaEpoch' = deltaEpoch,
    createRecorded' = createRecorded,
    deleteRecorded' = deleteRecorded,
    createOutcomeEpoch' = createOutcomeEpoch,
    deleteOutcomeEpoch' = deleteOutcomeEpoch,
    unknownReply' = false,
    activeWriter' = activeWriter,
    writer1Live' = writer1Live,
    writer2Live' = writer2Live,
    lastAction' = "retryDelete",
  }
```

Guarded by `deleteRecorded`, frames every projection, does not advance the epoch. A repeated
delete of an already-deleted edge is a no-op that replays the recorded outcome — the delete
side of exactly-once.

=== `crashWriter1`, `takeOverWriter2`, `rejectZombieWrite`: the fence

The last three actions are the reason the objective document uses the word "fence" so
often, and they are best read as a single three-step story: the owner dies, a replacement
takes over, and the dead owner — should it wake up — is refused.

#custom-box(title: [Term — Write fence and the zombie writer], icon: "info", color: purple)[
  A write fence is the durable record that names the one legitimate writer of a cell and a
  token that increases each time ownership changes. A _zombie writer_ is a former owner that
  believes it is still the writer — it was paused, or partitioned, and never learned it was
  replaced. The fence exists so that when the zombie finally tries to write, its stale token
  no longer matches and the write is rejected. The zombie must be observable as a rejected
  attempt, never as a second commit.
]

First, the crash. The owner simply goes away, `m1_cell_write.qnt:185-202`:

```
  action crashWriter1: bool = all {
    activeWriter == 1,
    writer1Live,
    epoch' = epoch,
    previousEpoch' = epoch,
    edgePresent' = edgePresent,
    outDegree' = outDegree,
    deltaEpoch' = deltaEpoch,
    createRecorded' = createRecorded,
    deleteRecorded' = deleteRecorded,
    createOutcomeEpoch' = createOutcomeEpoch,
    deleteOutcomeEpoch' = deleteOutcomeEpoch,
    unknownReply' = unknownReply,
    activeWriter' = 0,
    writer1Live' = false,
    writer2Live' = writer2Live,
    lastAction' = "crashWriter1",
  }
```

Guarded on writer 1 being the live owner. It changes only ownership: `activeWriter' = 0`,
`writer1Live' = false`. The graph is left exactly as it was — a crash does not roll anything
back, because everything already committed is durable in the object store. This is the model
saying "the process is gone but its writes survive."

Next, takeover. A replacement claims the cell, but only under a strict precondition,
`m1_cell_write.qnt:204-222`:

```
  action takeOverWriter2: bool = all {
    activeWriter == 0,
    not(writer1Live),
    lastAction == "crashWriter1",
    epoch' = epoch,
    previousEpoch' = epoch,
    edgePresent' = edgePresent,
    outDegree' = outDegree,
    deltaEpoch' = deltaEpoch,
    createRecorded' = createRecorded,
    deleteRecorded' = deleteRecorded,
    createOutcomeEpoch' = createOutcomeEpoch,
    deleteOutcomeEpoch' = deleteOutcomeEpoch,
    unknownReply' = unknownReply,
    activeWriter' = 2,
    writer1Live' = false,
    writer2Live' = true,
    lastAction' = "takeOverWriter2",
  }
```

Three guards: there must be no active writer (`activeWriter == 0`), writer 1 must be down
(`not(writer1Live)`), and the immediately preceding action must have been the crash
(`lastAction == "crashWriter1"`). That last guard is a modeling device to keep the scenario
tight — takeover happens right after a crash — and it ensures writer 2 never appears
alongside a still-live writer 1. The result: `activeWriter' = 2`, `writer2Live' = true`,
writer 1 stays dead. In the real system this is `open_fenced_owned*` bumping the lease token
so the fence now names node 2. Note the graph projections all frame unchanged: the objective
requires that "a successful replacement writer sees all prior durable commits," and here the
replacement inherits `edgePresent`, `epoch`, and every outcome exactly as writer 1 left them.

Finally, the zombie is refused. Its comment states the guarantee in one breath,
`m1_cell_write.qnt:224-243`:

```
  // A fenced former writer is explicitly observable as a rejected attempt,
  // never as a second commit.
  action rejectZombieWrite: bool = all {
    activeWriter == 2,
    not(writer1Live),
    epoch' = epoch,
    previousEpoch' = epoch,
    edgePresent' = edgePresent,
    outDegree' = outDegree,
    deltaEpoch' = deltaEpoch,
    createRecorded' = createRecorded,
    deleteRecorded' = deleteRecorded,
    createOutcomeEpoch' = createOutcomeEpoch,
    deleteOutcomeEpoch' = deleteOutcomeEpoch,
    unknownReply' = unknownReply,
    activeWriter' = activeWriter,
    writer1Live' = writer1Live,
    writer2Live' = writer2Live,
    lastAction' = "rejectZombieWrite",
  }
```

Guarded on writer 2 being the active owner and writer 1 being down — i.e. a takeover has
happened and now the old writer's attempt arrives. Read the assignments: _every single
variable frames unchanged_ except `lastAction`. That is the entire meaning of the action.
The zombie's write does nothing. It advances no epoch, writes no edge, records no outcome.
It only leaves a trace in `lastAction` that a rejection was observed. This is the objective's
property 2, "Epoch and fence safety": "A writer whose epoch is no longer current cannot make
a durable mutation or issue a successful outcome," and property 8, "Placement is not
authority ... the durable writer fence determines the one effective writer."

#custom-box(title: [Why], icon: "tip", color: rgb("#c99700"))[
  Notice there is no action in this model that lets a fenced writer actually commit. That is
  deliberate: a successful zombie commit would be a _bug_, and this model encodes the correct
  system, in which such a transition does not exist. The safety predicate below even names a
  `lastAction == "zombieWrite"` state that no action here can ever produce — a placeholder for
  the violation the correct model refuses to reach. The next chapter's deliberately-buggy
  twin model adds exactly such a bad transition, so the checker has something to catch.
]

== `step`: composing the actions into nondeterministic choice

Fourteen variables and eleven actions do not yet make a system. Something has to say "at
each moment, any enabled action may fire." That is `step`, `m1_cell_write.qnt:245-256`:

```
  action step: bool = any {
    openWriter1,
    createEdge,
    commitThenLoseReply,
    retryCreate,
    rejectConflictingRetry,
    deleteEdge,
    retryDelete,
    crashWriter1,
    takeOverWriter2,
    rejectZombieWrite,
  }
```

Where an action body is `all { ... }` (a conjunction — every guard and every assignment must
hold), `step` is `any { ... }` (a disjunction — take any _one_ of these transitions whose
guards are currently satisfied). This one keyword is what turns a pile of actions into a
state machine. At every state, Quint asks which of the ten listed actions are enabled (their
guards hold), and the next state may be the result of any one of them.

#custom-box(title: [Why], icon: "tip", color: rgb("#c99700"))[
  This is why the abstraction is worth the discipline. Because `step` is a nondeterministic
  choice and each action is small, a model checker can explore _every reachable interleaving_
  of these operations: create then crash then takeover then zombie; open, lose a reply, retry,
  delete, retry-delete; every order the guards permit. The real write path has threads,
  retries, and network timing that you could never enumerate. Collapsed to this shared-state
  machine, the same protocol has a state space small enough to walk exhaustively, and any
  safety property that holds across the whole walk holds for _all_ those interleavings at once.
]

When you later run this model, the tool repeatedly applies `step` from `init`, and every
sequence of `step`s is one behavior it checks. The deterministic `*_test.qnt` scenarios you
will meet in a later chapter are just _named, fixed_ sequences — `init.then(openWriter1)
.then(commitThenLoseReply).then(retryCreate)` — pinning down one specific path through this
same `step` for a human to read. We are not covering that `run` / `then` / `expect` syntax
here; only note that those scenarios exercise exactly the actions you have just read.

== A picture of the reachable states

It helps to see the shape `step` traces out. The create-and-fence storyline looks like this:

#figure(
  diagram(
    node-stroke: 0.6pt,
    node-fill: rgb("#eef4ff"),
    spacing: (1.9cm, 1.0cm),
    node((0, 0), [`init`\ no writer], width: 2.5cm),
    node((1, 0), [writer 1\ owns cell], fill: rgb("#eef4ff"), width: 2.5cm),
    node((2, 0), [edge present\ epoch += 1], fill: rgb("#e9fce9"), width: 2.5cm),
    node((2, 1), [committed,\ reply lost], fill: rgb("#fff8e6"), width: 2.5cm),
    node((1, 1), [writer 1\ crashed], width: 2.5cm),
    node((2, 2), [writer 2\ took over], fill: rgb("#eef4ff"), width: 2.5cm),
    node((0, 2), [zombie write\ rejected], fill: rgb("#ffecec"), width: 2.5cm),
    edge((0, 0), (1, 0), "->", [`openWriter1`]),
    edge((1, 0), (2, 0), "->", [`createEdge`]),
    edge((1, 0), (2, 1), "->", [`commitThenLoseReply`], stroke: 0.5pt + luma(60%)),
    edge((2, 1), (2, 0), "->", [`retryCreate`], stroke: 0.5pt + luma(60%)),
    edge((2, 0), (1, 1), "->", [`crashWriter1`]),
    edge((1, 1), (2, 2), "->", [`takeOverWriter2`]),
    edge((2, 2), (0, 2), "->", [`rejectZombieWrite`], stroke: 0.5pt + rgb("#1d90d0")),
  ),
  caption: [One storyline through `step`: acquire, create (or commit-then-lose-reply and retry), crash, take over, and reject the zombie. Every arrow is one action firing because its guards held; the graph projections carry across crash and takeover untouched.],
)

The diagram is one path; the checker explores all of them. Delete, retry-delete, and
conflicting-retry rejections branch off the same states wherever their guards permit.

== What comes next: the predicates that judge these transitions

The file does not end at `step`. Below it sit two more groups of `val` definitions that we
have leaned on informally throughout this chapter but deliberately have _not_ opened.

The first group are the safety predicates. `epochNeverRegresses` asserts `epoch >=
previousEpoch`. `edgeProjectionConsistent` asserts the edge and its degree agree
(`edgePresent and outDegree == 1`, or neither). `oneEffectiveWriter` asserts writers 1 and 2
are never both live and the active writer is the live one. `createIdempotencyExact` and
`deleteIdempotencyExact` pin the recorded outcome epochs to a valid range. They are gathered
into one `allSafety` conjunction — the single invariant that must hold in _every_ reachable
state. These are the executable form of the objective's property list you have been mapping
each action to; the difference is that here they are checked _against every state `step` can
reach_, not just asserted in prose.

The second group are the reachability witnesses: `lostReplyReached`, `takeoverReached`,
`zombieRejectionReached`, and their siblings, each true when `lastAction` names the
corresponding action. Their job is the opposite of a safety property. A safety property must
_always_ hold; a witness must be _reachable at least once_, and it exists to prove the model
is not trivially safe by simply never doing anything interesting. If `zombieRejectionReached`
could never become true, the fence guarantee would be vacuous.

That pairing — invariants that must always hold, witnesses that must be reachable — plus the
deliberately-buggy twin model that _violates_ them so we can watch the checker produce a
counterexample, is the entire subject of the next chapter. You now have the skill it builds
on: you can open `m1_cell_write.qnt` and read every transition as a precise, checkable claim
about turbolay's write path. Next, we make the checker judge those claims.
