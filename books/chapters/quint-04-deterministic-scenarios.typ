#import "../vendor/bookly/src/bookly.typ": *
#import "@preview/fletcher:0.5.7" as fletcher: diagram, node, edge

// LEARNING GOAL: the reader can read AND write a deterministic scenario in a
// *_test.qnt file — a named, fixed sequence init.then(a).then(b)... that pins
// one path through the nondeterministic step, with expect checkpoints asserting
// state at chosen points — and can explain how these auditable one-path stories
// complement exhaustive invariant checking and later drive the Rust MBT harness.
// GROUND IN:
//   quint-models/turbolay/m1_cell_write_test.qnt  (primary teaching vehicle)
//   quint-models/turbolay/m2_snapshot_read_test.qnt  (one contrast: scenarios live in the sibling)
//   quint-models/turbolay/m1_cell_write.qnt  (the actions the scenarios walk)
//   docs/formal-methods/0002-turbolay-quint-specification-plan.md  (the quint test command, gates)

= Deterministic scenarios: the one path a human can read

The previous chapter handed the model to a machine and let it walk. You wrote a
safety predicate, gathered it into `allSafety`, and the checker explored every
reachable interleaving of `step`, either returning "no counterexample" or
handing you a concrete trace that breaks the invariant. That is an extraordinary
thing to be able to do: one command judges every story at once. But it leaves a
gap that has nothing to do with rigor and everything to do with people.

Ask yourself a plain question. When `quint run` reports that `allSafety` holds
across the whole state space, what did _you_, the human, read? A verdict. A
boolean and, on a bad day, a machine-generated trace you did not choose. You did
not get to say, in your own words, "here is the specific story I care about most
— the write whose reply was lost and then retried — and here is exactly what the
epoch should be at each step." An invariant check is a proof about all paths; it
is not a story you can point a new engineer at and say "read this, it is the
contract." This chapter is about writing that story down.

== The problem: the checker proved it, but can you read it?

Return to the most important behavior in the write-path model: a create commits
durably, its acknowledgement is lost, and the anxious client retries with the
same key. The whole point of idempotency is that the retry returns the original
result and advances nothing. In the previous chapter that promise was enforced
_indirectly_ — it fell out of `allSafety` holding over every interleaving,
including the ones that happen to walk `commitThenLoseReply` then `retryCreate`.
The checker certainly exercised that path. But it was one anonymous thread among
thousands, and nowhere in the model did a human write "this is the lost-reply
story, and after the retry the epoch must still be 1."

That sentence — a named, fixed sequence of actions with the expected state
spelled out at each step — is a deterministic scenario. It is the second half of
how turbolay is verified, and it complements the exhaustive check rather than
replacing it.

#custom-box(title: [Term — Deterministic scenario], icon: "info")[
  A single, named walk through a model: a fixed sequence of actions,
  `init.then(actionA).then(actionB)...`, that pins exactly one path through the
  otherwise-nondeterministic `step`, together with checkpoints that assert what
  the state must be at chosen points along that path. Where `step`'s `any { ... }`
  lets the checker take _any_ enabled action, a scenario names _the_ action to
  take at each moment, so the run is reproducible and reads top to bottom like a
  short story a person can audit.
]

The two techniques answer two different questions, and the difference is worth
stating flatly before we write any Quint.

#figure(
  table(
    columns: (1fr, 1.15fr, 1.15fr),
    align: (left, left, left),
    stroke: 0.5pt + reader-colors.border,
    fill: (_, row) => if row == 0 { reader-colors.surface_soft },
    inset: 7pt,
    table.header(
      text(fill: reader-colors.text)[*Question*],
      text(fill: reader-colors.text)[*Exhaustive invariant check*],
      text(fill: reader-colors.text)[*Deterministic scenario*],
    ),
    [Who chooses the path?], [The checker — it walks _all_ of them], [You — one fixed sequence, by name],
    [What does it prove?], [A property holds on every reachable state], [This one story reaches this exact state],
    [Who reads it?], [A machine; a human reads only the verdict], [A human reads the whole story top to bottom],
    [Coverage], [Every interleaving `step` allows], [Exactly one path; nothing about the others],
    [Command], [`quint run --invariant`], [`quint test`],
  ),
  caption: [The two halves of the same verification. The previous chapter's invariant check walks every story and returns a verdict; a scenario is one story, written by a human, that a machine replays. They are complements: the check gives coverage, the scenario gives an auditable, precise statement of intent that later drives the Rust tests.],
) <tab-ch5-check-vs-scenario>

== `run`, `then`, `expect`: the three moves

Everything you need to read and write a scenario lives in three operators, and
all three appear in one small file. Open
`quint-models/turbolay/m1_cell_write_test.qnt`. It is a separate module that
imports every name from the model you already know, `m1_cell_write_test.qnt:1-3`:

```
// -*- mode: quint -*-
module m1_cell_write_test {
  import m1_cell_write.* from "./m1_cell_write"
```

That `import ... .*` is what lets the scenarios below say `openWriter1` and
`epoch` directly, as if they were writing inside the model. Now the first
operator.

#custom-box(title: [Term — `run`], icon: "info")[
  A named, executable scenario. `run <name> = <expr>` binds a name to a fixed
  sequence built from `init`, `then`, and `expect`. Unlike a `val` (a value) or
  an `action` (a single transition), a `run` is a whole _behavior_: a starting
  state and an ordered list of moves. The `quint test` command finds every
  `run` and executes it, passing if the sequence completes and every checkpoint
  holds, failing the moment one does not.
]

The second operator, `then`, is how a scenario advances. Reading
`a.then(b)` as English: start from the behavior `a`, and from the state it ends
in, take action `b`. It is the deterministic cousin of `step`. Where `step`
offered `any` enabled action and let the checker pick, `then` names the single
action to fire next. There is one rule that matters: the named action must be
_enabled_ — its guards must hold in the current state — or the run fails right
there with "action is not enabled." That failure is a feature. If you write
`.then(createEdge)` at a point where no writer owns the cell, the scenario
refuses to proceed, and it has just told you your story is impossible.

The third operator, `expect`, is how a scenario checks its work without moving.

#custom-box(title: [Term — `expect` (a checkpoint)], icon: "info")[
  `b.expect(pred)` runs the behavior `b`, then evaluates the boolean `pred` in
  the resulting state. If `pred` is true the state is passed along unchanged and
  the scenario continues; if it is false the run fails at that line. An `expect`
  never advances the state and never fires an action — it only inspects. Chaining
  `.expect(p).expect(q)` checks both `p` and `q` against the _same_ state, the
  one left by the last `then`.
]

Hold the distinction firmly, because it is the whole grammar: `then` is a verb
that changes the world, `expect` is a question that reads it. A scenario is an
alternation of the two — do something, check something, do the next thing, check
again.

#custom-box(title: [Why], icon: "tip")[
  Why have `expect` at all, when the previous chapter's invariants already assert
  facts about state? Because they assert them _everywhere_, unconditionally. An
  invariant like `epochNeverRegresses` must hold in every reachable state, so it
  can only say things that are always true. A scenario checkpoint is local and
  specific: after _this_ particular retry, in _this_ story, the epoch must be
  exactly `1` — a concrete number that would be meaningless as a global
  invariant, because most states have other epochs. `expect` lets a human nail
  the precise value at a chosen moment, which is exactly what makes the scenario
  readable as a contract. (Quint also offers `assert` as an action you can
  sequence with `then`; these scenario files use `expect` throughout as their
  checkpoint, so that is the one to learn here.)
]

== Reading a scenario: the lost reply, retried once

Now read a whole scenario as one story. This is the lost-reply behavior, written
down by name, `m1_cell_write_test.qnt:5-14`:

```
  run lostReplyRetryIsExactlyOnceTest =
    init
      .then(openWriter1)
      .then(commitThenLoseReply)
      .expect(edgePresent)
      .expect(epoch == 1)
      .then(retryCreate)
      .expect(edgePresent)
      .expect(epoch == 1)
      .expect(outDegree == 1)
```

Read it top to bottom the way you would read a paragraph. It begins at `init` —
the fresh cell from the model, epoch 0, no writer, no edge. Then `openWriter1`
fires, and writer 1 owns the cell. Then `commitThenLoseReply` fires: the create
commits durably — the edge is present, the epoch advanced to 1 — but the caller
never hears the acknowledgement. At that checkpoint the scenario asserts exactly
that: `edgePresent` is true and `epoch == 1`. Then the anxious client retries
with the same key: `retryCreate`. And here is the entire promise of idempotency,
spelled out for a human to see: after the retry, `edgePresent` is still true,
`epoch` is _still_ 1 — it did not advance a second time — and `outDegree` is 1,
not 2. One edge, one epoch bump, no matter that the client asked twice.

Compare that to how you met the same behavior in the write-path model. There it
was an anonymous path the checker happened to walk. Here it has a name you can
say out loud, `lostReplyRetryIsExactlyOnceTest`, and every intermediate fact is
written down. If someone later changes `retryCreate` so that it accidentally
advanced the epoch, this scenario fails on the line `.expect(epoch == 1)` and
names the exact step — where an invariant failure would hand you a trace to
decode. The scenario _is_ the decoded trace, authored in advance.

That is the picture to hold: a scenario is a single bold thread pulled out of the
branching tree from the previous chapter, with a checkmark pinned to each state
it passes through.

#figure(
  diagram(
    crossing-fill: reader-colors.paper,
    node-stroke: 0.6pt + reader-colors.border,
    node-outset: 0pt,
    spacing: (3.0cm, 1.25cm),
    // Faded forks: the interleavings the exhaustive checker walks but this scenario does not.
    node((-1.4, 2), text(fill: reader-colors.muted, size: 8pt, hyphenate: false)[edge present,\ reply delivered], fill: reader-colors.surface_soft.transparentize(20%), width: 2.4cm),
    node((-1.4, 3), text(fill: reader-colors.muted, size: 8pt)[writer 1\ crashed], fill: reader-colors.surface_soft.transparentize(20%), width: 2.3cm),
    // The bold spine: the one named path this scenario walks.
    node((0, 0), text(fill: reader-colors.text)[`init`\ no writer], fill: reader-colors.surface_soft, width: 2.7cm),
    node((0, 1), text(fill: reader-colors.text)[writer 1\ owns cell], fill: reader-colors.info_soft, width: 2.7cm),
    node((0, 2), text(fill: reader-colors.text)[committed,\ reply lost], fill: reader-colors.warn_soft, width: 2.7cm),
    node((0, 3), text(fill: reader-colors.text)[ambiguity\ resolved], fill: reader-colors.ok_soft, width: 2.7cm),
    // Checkpoint badges: the expect() assertions nailing the state at each step.
    node((1.55, 2), text(fill: reader-colors.text, size: 6.5pt, hyphenate: false)[✓ `expect edgePresent`\ ✓ `expect epoch == 1`], fill: reader-colors.ok_soft, stroke: (paint: reader-colors.ok, thickness: 0.8pt), corner-radius: 4pt, width: 3.15cm),
    node((1.55, 3), text(fill: reader-colors.text, size: 6.5pt, hyphenate: false)[✓ `expect epoch == 1`\ ✓ `expect outDegree == 1`], fill: reader-colors.ok_soft, stroke: (paint: reader-colors.ok, thickness: 0.8pt), corner-radius: 4pt, width: 3.15cm),
    // Bold spine edges = the .then(...) moves.
    edge((0, 0), (0, 1), "->", text(fill: reader-colors.info, size: 7pt)[`.then(openWriter1)`], stroke: 1.3pt + reader-colors.info, label-side: right),
    edge((0, 1), (0, 2), "->", text(fill: reader-colors.info, size: 7pt)[`.then(commitThenLoseReply)`], stroke: 1.3pt + reader-colors.info, label-side: right),
    edge((0, 2), (0, 3), "->", text(fill: reader-colors.info, size: 7pt)[`.then(retryCreate)`], stroke: 1.3pt + reader-colors.info, label-side: right),
    // Faded fork edges: other actions step's any{...} allows here.
    edge((0, 1), (-1.4, 2), "->", text(fill: reader-colors.muted, size: 6.5pt)[`createEdge`], stroke: (thickness: 0.6pt, paint: reader-colors.muted, dash: "dotted"), label-side: left, label-pos: 0.7),
    edge((0, 2), (-1.4, 3), "->", text(fill: reader-colors.muted, size: 6.5pt)[`crashWriter1`], stroke: (thickness: 0.6pt, paint: reader-colors.muted, dash: "dotted"), label-side: left, label-pos: 0.7),
    // Checkpoint attach lines: an expect reads the state, it does not advance it.
    edge((0, 2), (1.55, 2), "--", stroke: (thickness: 0.7pt, paint: reader-colors.ok, dash: "dotted")),
    edge((0, 3), (1.55, 3), "--", stroke: (thickness: 0.7pt, paint: reader-colors.ok, dash: "dotted")),
  ),
  caption: [The `lostReplyRetryIsExactlyOnceTest` scenario as one bold walk through the write-path model's state tree. Each *bold* arrow is a `.then(...)` move naming the single action to fire; the faded dotted forks are the _other_ transitions `step`'s `any { ... }` allows from the same states, which the exhaustive checker walks but this scenario deliberately does not. The green badges are the `expect` checkpoints: they read the state a step lands in — they never advance it — and pin exact facts, most tellingly `epoch == 1` _after_ the retry, the operational signature of idempotency.],
) <fig-ch5-scenario-path>

Set this figure beside the reachable-states picture from the write-path chapter
and the relationship is exact. That earlier figure drew the whole tree with one
bold spine and faded forks; a scenario _is_ that bold spine, extracted, named,
and annotated with the checkpoints that say what must be true at each node. The
exhaustive check fills in the entire tree; the scenario pulls out one thread and
hands it to a reader.

== Three more stories, each pinning one contract

The rest of the file is three more scenarios, and reading them is now just
reading. Each is a fixed path that ends by nailing a specific fact, and together
they cover the delete side, the fence, and the conflict rejection you read
action-by-action in the write-path chapter. The delete-retry story,
`m1_cell_write_test.qnt:16-25`, drives a create then a delete, checkpoints that
the edge is gone and the epoch reached 2, then retries the delete and checks the
epoch _stayed_ 2 — delete's half of exactly-once:

```
  run deleteRetryDoesNotAdvanceEpochTest =
    init
      .then(openWriter1)
      .then(createEdge)
      .then(deleteEdge)
      .expect(not(edgePresent))
      .expect(epoch == 2)
      .then(retryDelete)
      .expect(epoch == 2)
      .expect(outDegree == 0)
```

The fence story, `m1_cell_write_test.qnt:27-36`, walks the three-step takeover
you read as a single narrative before — create, crash, hand over to writer 2,
then let the zombie try — and checkpoints that after the rejected zombie write
the active writer is 2, the edge writer 1 created survives, and the epoch is
still 1. The zombie changed nothing:

```
  run staleWriterIsFencedAfterTakeoverTest =
    init
      .then(openWriter1)
      .then(createEdge)
      .then(crashWriter1)
      .then(takeOverWriter2)
      .then(rejectZombieWrite)
      .expect(activeWriter == 2)
      .expect(edgePresent)
      .expect(epoch == 1)
```

And the conflict story, `m1_cell_write_test.qnt:38-45`, creates an edge and then
reuses its idempotency key for a _different_ mutation, checkpointing that the
edge is untouched, the epoch did not move, and `lastAction` records the
rejection. A reused key with a changed intent is refused, not silently applied.

Notice what every one of these scenarios shares: it is a straight line, no
branching, ending in a cluster of `expect`s that read like an assertion of
intent. The table below is the whole file at a glance — four stories, four
contracts.

#figure(
  table(
    columns: (1.35fr, 1.3fr, 1.35fr),
    align: (left, left, left),
    stroke: 0.5pt + reader-colors.border,
    fill: (_, row) => if row == 0 { reader-colors.surface_soft },
    inset: 7pt,
    table.header(
      text(fill: reader-colors.text)[*Scenario (`run`)*],
      text(fill: reader-colors.text)[*The path it walks*],
      text(fill: reader-colors.text)[*The contract it pins*],
    ),
    [`lostReplyRetryIsExactlyOnceTest`], [open, commit-then-lose-reply, retry], [retry returns the outcome; `epoch` stays 1, one edge],
    [`deleteRetryDoesNotAdvanceEpochTest`], [open, create, delete, retry-delete], [delete replay is a no-op; `epoch` stays 2],
    [`staleWriterIsFencedAfterTakeoverTest`], [open, create, crash, take over, zombie], [fenced writer commits nothing; edge and epoch survive],
    [`conflictingRetryIsRejectedWithoutMutationTest`], [open, create, reuse key for a different write], [conflicting key is rejected; state unchanged],
  ),
  caption: [The four deterministic scenarios in `m1_cell_write_test.qnt`. Each is one fixed path through the write-path model, and each ends by nailing the exact state that path must reach. Read straight down, they are the write-path contract told as four short, auditable stories — the human-readable counterpart to the machine's exhaustive walk.],
) <tab-ch5-scenarios>

== Where scenarios live: the sibling file, not the model

You may have noticed the file is `m1_cell_write_test.qnt`, a _separate_ module
from `m1_cell_write.qnt`. That separation is deliberate and consistent across the
whole model suite. The main model holds the state, the actions, `step`, the
invariants, and the witnesses — and no scenarios at all. The scenarios live only
in the `_test.qnt` sibling, which imports the model. The snapshot-read model
states the rule in its test file's header, `m2_snapshot_read_test.qnt:1-3`:

```
// -*- mode: quint -*-
// Deterministic M2 contract scenarios. Main model remains scenario-free.
module m2_snapshot_read_test {
```

"Main model remains scenario-free" is the design in one line. The model is the
specification — the space of all allowed behaviors — and it should not be
cluttered with particular walks through itself. The scenarios are examples _of_
that specification, and they belong in their own file so the model stays a clean
statement of the contract while the tests accumulate stories against it. The
specification plan makes the naming a hard requirement: test modules "contain
only deterministic `run ...Test` scenarios," and the `Test` suffix on each run is
mandatory so that `quint test` does not silently skip them
(`0002-turbolay-quint-specification-plan.md:74-76`).

== Running them: `quint test`

A scenario is executable, so run it. The command that drives every `run` in a
test module is `quint test`, and the specification plan pins its exact shape
(`0002-turbolay-quint-specification-plan.md:339`):

```
mise exec -- quint test quint-models/turbolay/m1_cell_write_test.qnt \
  --main m1_cell_write_test --match '.*Test$'
```

Read the flags. `--main m1_cell_write_test` names the test module to run.
`--match '.*Test$'` selects every `run` whose name ends in `Test` — which is why
the suffix is required. Quint executes each selected scenario as a fixed
sequence: it starts at `init`, fires each `then` action in order (failing if one
is not enabled), and evaluates each `expect` (failing if one is false). A passing
run means the story is possible _and_ every checkpoint held. Because there is no
nondeterminism — every action is named — the result is completely reproducible:
`quint test` does not sample or search, it replays.

#custom-box(title: [Why], icon: "tip")[
  Why keep these fast, deterministic scenarios when the exhaustive invariant
  check already covers strictly more paths? Two reasons. First, they are the
  documentation that cannot rot: `lostReplyRetryIsExactlyOnceTest` says, in
  runnable Quint, exactly what the lost-reply contract is, and if the model ever
  drifts from that intent the named test fails on a named line — far easier to
  diagnose than an invariant counterexample. Second, and this is where the whole
  effort is heading, these exact scenarios become the seed traces for the Rust
  model-based tests. The same fixed sequence of action names that `quint test`
  replays against the model is later replayed against the _real kernel_, its
  state projection compared after every step. A scenario written once is checked
  twice: against the specification here, and against the implementation later.
]

== What a scenario proves, and what it does not

Be honest about the boundary, because it is the flip side of the scenario's
value. A deterministic scenario tests exactly _one_ path. The lost-reply scenario
proves that its specific story — open, commit-then-lose-reply, retry — behaves
correctly and reaches the exact state its checkpoints demand. It
proves _nothing_ about the story where the reply is lost twice, or where a crash
lands between the commit and the retry, or where a second writer is involved.
Those are different paths, and this scenario never walks them. Its precision is
also its limit: it is a single, auditable thread, and a single thread is not
coverage.

That is precisely why scenarios do not stand alone. The exhaustive invariant
check from the previous chapter is what covers the interleavings a human would
never think to enumerate — every order the guards permit, thousands of them,
judged against `allSafety` at once. And for the strongest "all paths" guarantee
within a finite bound, the Apalache proof (a later chapter) turns the bounded
search into a symbolic one. The division of labor is clean: scenarios give you
precise, readable, replayable statements of the stories you care about most; the
exhaustive check and the proof give you coverage over the stories you did not
think to write. Neither is a substitute for the other, and turbolay uses both.

You can now read every `run` in a `_test.qnt` file as a fixed walk through a
model — `init.then(...)` for the moves, `.expect(...)` for the checkpoints — and
you can write one yourself: name a story, drive it action by action, and pin the
state that story must reach. The next chapter, a tour of the eight model
families, uses that reading skill at scale, opening each `mN_*.qnt` model and its
scenario sibling in turn so you can see the same grammar describe snapshots and
cursors, artifact builds and garbage collection, placement and the durable
fence. Further out, the Rust model-based-testing chapter takes exactly the
scenarios you just read and replays them against the running kernel, comparing
the real engine's state to the model's after every step — the moment these
human-readable stories become executable checks on the code itself.
