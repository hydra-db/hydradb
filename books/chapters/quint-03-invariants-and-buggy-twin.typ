#import "../vendor/bookly/src/bookly.typ": *
#import "@preview/fletcher:0.5.7" as fletcher: diagram, node, edge

// LEARNING GOAL: The reader can read the safety predicates and reachability
// witnesses at the bottom of a Quint model, state precisely how they differ
// (an invariant must hold in EVERY reachable state; a witness must be reachable
// in AT LEAST ONE), run them with `quint run --invariant`, and understand the
// buggy-twin method: a deliberately-broken copy whose bad action VIOLATES an
// invariant so the checker emits a counterexample trace, proving the invariant
// has teeth and is not vacuously true.
// GROUND IN: quint-models/turbolay/m1_cell_write.qnt (predicates 258-289),
//   quint-models/turbolay/m2_snapshot_read.qnt, m2_snapshot_read_buggy.qnt,
//   docs/formal-methods/0001-...md, 0003-...md.

= Invariants, witnesses, and the deliberately-broken twin

The previous chapter read the write-path model, `m1_cell_write.qnt`, from its header down
to `step`, and stopped there on purpose. Below `step` sit two more blocks of one-line `val`
definitions, and we leaned on them informally the whole way through — "the epoch never
regresses," "the fence guarantee," "the witness proves the model does something interesting"
— without ever opening them. This chapter opens them.

That is the entire job of Act II's first chapter, because those blocks are where a model
stops _describing_ turbolay and starts _judging_ it. Everything above `step` is a machine
that moves; the predicates below `step` are the questions you ask about every state that
machine can reach. Get them right and a model checker will find a protocol bug you could
stare at for a week. Get them subtly wrong — write a check that is always true no matter what
the system does — and the checker will cheerfully report "no violation found" about a
property that was never really being tested. This chapter is about telling those two
situations apart.

We will do three things. First, name the two kinds of claim a model makes and see both at the
bottom of `m1_cell_write.qnt`. Second, run one and watch it pass. Third — and this is the
heart — meet the _buggy twin_: a deliberately-broken copy of a model whose whole reason to
exist is to make the checker fail, so you can see that the invariant it violates was doing
real work.

== Two kinds of claim a model makes

Return to the bottom of `m1_cell_write.qnt`. There are two groups of definitions, and they
pull in opposite directions. The first group asserts things that must be true _forever_; the
second asserts things that must be possible _at all_. Learn to see the difference and you can
read the contract of any model in this repository from its last page alone.

Here is the first group, the safety predicates, `m1_cell_write.qnt:258-278`:

```
  val epochNeverRegresses: bool = epoch >= previousEpoch
  val edgeProjectionConsistent: bool =
    (edgePresent and outDegree == 1) or (not(edgePresent) and outDegree == 0)
  val deltaMatchesLatestTopologyChange: bool = deltaEpoch == epoch
  val createIdempotencyExact: bool =
    not(createRecorded) or all { createOutcomeEpoch > 0, createOutcomeEpoch <= epoch }
  val deleteIdempotencyExact: bool =
    not(deleteRecorded) or all { deleteOutcomeEpoch > 0, deleteOutcomeEpoch <= epoch }
  val oneEffectiveWriter: bool =
    not(writer1Live and writer2Live) and
      ((activeWriter == 1) implies writer1Live) and
      ((activeWriter == 2) implies writer2Live)
  val zombieWriteRejected: bool = not(lastAction == "zombieWrite")
  val allSafety: bool =
    epochNeverRegresses and
      edgeProjectionConsistent and
      deltaMatchesLatestTopologyChange and
      createIdempotencyExact and
      deleteIdempotencyExact and
      oneEffectiveWriter and
      zombieWriteRejected
```

Read each one as a sentence about a single state. `epochNeverRegresses`: in this state, the
epoch is at least what it was one step ago. `edgeProjectionConsistent`: the edge and its
degree agree — either the edge is present and the degree is 1, or the edge is absent and the
degree is 0, never one without the other. `oneEffectiveWriter`: the two writers are never
both live, and whichever writer is named as active is in fact the live one. None of these
mentions a sequence of steps or a history; each is a yes-or-no question you could ask about a
photograph of the fourteen variables. That is what makes them _invariants_.

#custom-box(title: [Term — Invariant (safety predicate)], icon: "info", color: purple)[
  A predicate over a single state — a boolean expression of the model's variables — that must
  evaluate to true in _every state the model can reach_ from `init` by any sequence of
  `step`s. It says "nothing bad is ever the case." An invariant is violated the moment the
  checker finds even one reachable state where it is false. In Quint you state an invariant as
  an ordinary `val` of type `bool`; what makes it an invariant is that you _check it against
  every reachable state_, which the next section shows how to do.
]

The last line, `allSafety`, is just the conjunction of all seven. Bundling them lets you check
the whole safety contract with one name: `allSafety` holds in a state exactly when every
individual predicate does. The objective document you have been mapping actions to lists these
same guarantees in prose — "commit epochs for a cell increase strictly," "a writer whose epoch
is no longer current cannot make a durable mutation" (`0001-quint-jepsen-testing-objective.md`,
the properties list) — and `allSafety` is that prose made executable and checkable against
every state at once.

Now the second group, four lines further down, `m1_cell_write.qnt:280-289`:

```
  val lostReplyReached: bool = lastAction == "commitThenLoseReply"
  val takeoverReached: bool = lastAction == "takeOverWriter2"
  val zombieRejectionReached: bool = lastAction == "rejectZombieWrite"
  val openWriter1Reached: bool = lastAction == "openWriter1"
  val createEdgeReached: bool = lastAction == "createEdge"
  ...
```

These look almost identical — one-line `val`s of type `bool` — but their job is the exact
opposite. `zombieRejectionReached` is true in a state whose last action was the zombie
rejection. We do not want this to be true _always_; a model where every state had just
rejected a zombie would be absurd. We want it to be true _somewhere_: there must exist at
least one reachable state in which it holds, which proves the model can actually get a zombie
rejected. That is a _witness_.

#custom-box(title: [Term — Reachability witness], icon: "info", color: purple)[
  A predicate that must be true in _at least one_ reachable state. It witnesses that some
  interesting situation is actually achievable — that a particular action can fire, that a
  particular corner of the state space is not dead code. Where an invariant is a claim of the
  form "for all reachable states, P," a witness is a claim of the form "there exists a
  reachable state where Q." The checker reports a witness _satisfied_ when it finds such a
  state, and _unreached_ if it never does across the whole simulation.
]

The two claims are duals, and the cleanest way to hold them in your head is side by side.

#figure(
  table(
    columns: (0.9fr, 1.05fr, 1.05fr),
    align: (left, left, left),
    stroke: 0.5pt + reader-colors.border,
    fill: (_, row) => if row == 0 { reader-colors.surface_soft },
    inset: 7pt,
    table.header(
      text(fill: reader-colors.text)[],
      text(fill: reader-colors.text)[*Invariant (safety)*],
      text(fill: reader-colors.text)[*Witness (reachability)*],
    ),
    [Shape of the claim], [for _all_ reachable states, P holds], [there _exists_ a reachable state where Q holds],
    [In plain words], [nothing bad ever happens], [this good thing can actually happen],
    [Checker reports a problem when], [it finds _one_ state where P is false], [it finds _no_ state where Q is ever true],
    [A counterexample is], [a trace ending in the bad state], [(none) — failure is silent absence],
    [Example in `m1`], [`allSafety`], [`zombieRejectionReached`],
  ),
  caption: [The two kinds of claim at the bottom of every turbolay model. They are logical
    duals: an invariant is a universal statement the checker tries to _refute_ with a single
    bad state; a witness is an existential statement the checker tries to _confirm_ by finding
    one good state. A model needs both — the invariants keep it honest, the witnesses keep it
    from being trivially honest.],
) <tab-ch4-invariant-witness>

Why does a model need both? Because an invariant on its own can be passed by cheating. This is
the subtlest and most important idea in the chapter, so it gets its own section — but you can
already feel the shape of it. If `oneEffectiveWriter` must hold in every reachable state, one
guaranteed way to satisfy it is to build a model that never lets a _second_ writer appear at
all. Every state trivially has one effective writer, the invariant passes, and the check
proved nothing about the takeover logic because the takeover never happened. The witnesses are
the guardrail against exactly that.

== Running an invariant: `quint run --invariant`

An invariant is inert until something checks it against reachable states. The tool that does
so by _simulation_ is `quint run`. You point it at a model, name an invariant, and it
repeatedly starts at `init` and walks random sequences of `step` — many separate sample runs,
each up to a step bound — evaluating the invariant at every state it lands in. If it ever finds
a state where the invariant is false, it stops and prints the trace that led there. If it
never does, it reports that no violation was found within the bound it explored.

Here is the safety contract of the whole write-path model, checked by simulation. The
verification evidence document runs exactly this shape — ten thousand samples of twelve steps
each (`0003-turbolay-quint-verification-evidence.md`, the P0 completion evidence):

```bash
mise exec -- quint run quint-models/turbolay/m1_cell_write.qnt \
  --main m1_cell_write --invariant allSafety \
  --max-samples 10000 --max-steps 12
```

`--invariant allSafety` names the predicate to check at every state; `--max-steps 12` bounds
how long each sampled behavior may be; `--max-samples 10000` is how many separate behaviors to
try. Run it and the model reports success:

```
[ok] No violation found (56ms at 178571 traces/second).
Trace length statistics: max=13, min=13, average=13.00
```

Ten thousand random walks through the write path, each up to twelve `step`s long, and in every
single state along every single walk, all seven safety predicates held at once. That is real
evidence. It is also, and this matters, _not a proof_.

#custom-box(title: [Term — Bounded simulation], icon: "info", color: purple)[
  `quint run` is a _randomized, bounded simulator_. It samples a finite number of behaviors, each
  of finite length, and checks the invariant at each state it happens to visit. It does not
  enumerate every reachable state; it draws a large but incomplete sample of them. A clean
  `quint run` therefore means "no counterexample was found in the region I explored," which is
  strong evidence for a small model but is categorically weaker than "no counterexample
  exists." A run that ends `[ok] No violation found` has not proven the property; it has failed
  to disprove it, thoroughly.
]

This candour is the house style, and the objective document states it in the same breath as
the command: a run with no counterexample is "reported as simulation evidence, not a proof"
(`0001-quint-jepsen-testing-objective.md`, the verification-pipeline section). The gap gets
closed — partially — two chapters on, when the Apalache checker re-examines the same invariants
_exhaustively_ up to a fixed number of steps: not a sample of behaviors but every behavior of
that length, symbolically, so a clean result at bound six genuinely means "no counterexample
of six steps or fewer exists." Even that is bounded; a proof over an unbounded graph and
unbounded time is beyond every tool in this book, and we will keep saying so. For now, hold the
honest claim: a passing `quint run` is a strong, cheap, fast filter, and the next thing we need
is a way to be sure that filter can actually catch something.

== The vacuity trap: how a green check can be a lie

Here is the failure mode that should keep a specification author up at night. Suppose you write
an invariant, run the checker, and it passes. What have you learned? You want to conclude "the
system never does the bad thing." But there is a second, poisonous explanation for a passing
check: the bad thing was never _possible_ in your model to begin with, so of course no state
violated the invariant, and the check told you nothing.

#custom-box(title: [Why], icon: "tip", color: rgb("#c99700"))[
  A predicate is _vacuously true_ when it holds only because its dangerous case never arises.
  `zombieWriteRejected` in `m1_cell_write.qnt:270` reads `not(lastAction == "zombieWrite")` — it
  is false exactly when the last action was a successful zombie write. But look back at the
  actions in the previous chapter: there _is no action_ named `zombieWrite`. No transition in
  the correct model can ever set `lastAction` to that string, so `zombieWriteRejected` is true
  in every reachable state — not because the fence heroically stops zombies, but because the
  model contains no way to even attempt the forbidden commit. The predicate names a bad state
  the correct model simply cannot reach. On its own, that invariant proves nothing. It is a
  placeholder, waiting for a model that _can_ reach the bad state, so we can confirm the
  invariant would catch it.
]

This is why the witnesses exist, and it is why they are not optional decoration. `takeoverReached`
being satisfied tells you the model genuinely performed a takeover before you trusted
`oneEffectiveWriter` across it. `lostReplyReached` being satisfied tells you the ambiguous-outcome
branch was actually explored before you trusted the idempotency predicates. A witness converts a
silent assumption — "surely the interesting case happened" — into a checked fact. But witnesses
guard against a _dead action_; they cannot, by themselves, prove that an invariant would notice
a genuinely bad transition if one were added. For that, you need to add the bad transition and
watch the invariant fail. You need a buggy twin.

== The buggy twin: a broken copy built to fail

The technique is disarmingly direct. Take a model whose invariant passes. Make a copy. Into the
copy, deliberately introduce the exact bug the invariant is supposed to forbid — a single bad
action that reaches the forbidden state. Then run the checker on the copy. If the invariant has
teeth, the checker must now _fail_, and hand you a concrete trace of the violation. If instead
the buggy copy still passes, your invariant was vacuous all along, and you have just caught it.

#custom-box(title: [Term — Buggy twin], icon: "info", color: purple)[
  A deliberately-broken copy of a model, identical to the intended one except that it adds (or
  un-forbids) a transition the correct model refuses to make — the specific defect an invariant
  is meant to rule out. Its purpose is _negative_: a correct model must pass its invariants, and
  its buggy twin must _fail_ the corresponding one. The pair is a controlled experiment. The
  twin proves the invariant is falsifiable — that it discriminates the bug from the intended
  behavior — which is exactly the assurance a green check on the correct model cannot give you
  by itself.
]

The turbolay repository ships a worked example of this pair for the read path, and it is small
enough to read whole. To understand what its bug violates, you need one idea from the intended
read model, `m2_snapshot_read.qnt`, and only one.

=== Just enough of the intended snapshot contract

The read model is about serving _pages_ of query results to a client without lying to it while
the graph changes underneath. Its central object is a snapshot.

#custom-box(title: [Term — Snapshot], icon: "info", color: purple)[
  One immutable view of the graph, captured at a single committed epoch. Once a snapshot is
  taken, later writes advance the _current_ graph but must not alter what the snapshot shows.
  A reader that pins a snapshot and then reads pages from it sees a stable, self-consistent
  graph even as the world moves on — the read is isolated from concurrent writes because it is
  answered entirely from the frozen view, never from live storage.
]

The intended model captures this in one sentence of Quint. Its `openSnapshot` action is
commented "the only point at which a current storage view is captured," and it copies the
current view into the snapshot, `m2_snapshot_read.qnt:57-58`:

```
  action openSnapshot: bool = all {
    not(cursorOpen),
    snapshot' = current,
    snapshotOpen' = true,
    ...
```

From then on, every page the intended model returns is drawn from the pinned view, never from
`current`. The safety predicate that enforces this in the full model is `returnedPageMatchesCursor`
(`m2_snapshot_read.qnt:258-265`): if a page was returned, its row must be exactly the row from
the cursor's pinned snapshot. And notice the intended model's discipline about the forbidden
outcome — it even contains an action `returnUnvalidatedHistoricalPage` whose body ends in a bare
`false` (`m2_snapshot_read.qnt:174-196`), a guard that can never hold, so the action is
permanently disabled. That is the read path's version of the write path's missing zombie-commit
edge: the correct model makes the bad transition _structurally impossible_, and an invariant
alone can never tell you whether that wall is load-bearing or decorative. So we knock a hole in
it on purpose.

=== The twin, and its two bad actions

The buggy twin, `m2_snapshot_read_buggy.qnt`, is a stripped-down copy carrying the same essential
invariant in simplified form — `pageMatchesSnapshot`, "if a page was returned, it equals the
snapshot" (`m2_snapshot_read_buggy.qnt:86`) — plus two deliberately wrong actions. Its own header
says why they are there: they "deliberately model two paths identified in the V2 analysis," and
"`quint run` must violate both invariants" (`m2_snapshot_read_buggy.qnt:3-9`). Each bad action
reproduces one real, catalogued defect.

The first is BFG-001, `m2_snapshot_read_buggy.qnt:55-65`:

```
  // BUG BFG-001: the page path reads the current live view, not `snapshot`.
  action materializePageFromLiveStorage: bool = all {
    snapshotOpen,
    ...
    page' = current,
    pageReturned' = true,
    ...
  }
```

Read the one line that matters: `page' = current`. A page is materialized from the _live_
current view even though a snapshot was already open. This is the isolation bug in its purest
form — a reader that pinned a snapshot is handed data from after the snapshot, as if the freeze
never happened. The second bad action, BFG-002, is its sibling: `returnUnvalidatedHistoricalFromCurrent`
(`m2_snapshot_read_buggy.qnt:67-77`) answers a historical request that no snapshot ever validated
by, again, returning `current` — data the caller was never entitled to see. The intended model
_rejects_ that request; the twin _serves_ it.

#figure(
  table(
    columns: (1fr, 0.5fr, 1.3fr),
    align: (left, center, left),
    stroke: 0.5pt + reader-colors.border,
    fill: (_, row) => if row == 0 { reader-colors.surface_soft },
    inset: 7pt,
    table.header(
      text(fill: reader-colors.text)[*Bad action in the twin*],
      text(fill: reader-colors.text)[*Bug*],
      text(fill: reader-colors.text)[*Invariant it violates*],
    ),
    [`materializePageFromLiveStorage`], [BFG-001], [`pageMatchesSnapshot` — the page equals `current`, not the frozen `snapshot`],
    [`returnUnvalidatedHistoricalFromCurrent`], [BFG-002], [`pageMatchesSnapshot` and `invalidHistoricalNeverReturns` — an unvalidated request is served live data],
  ),
  caption: [The two deliberately-wrong actions in `m2_snapshot_read_buggy.qnt` and the intended
    invariants each is built to break. Both reduce to the same one-character mistake — reading
    `current` where the intended model reads the pinned `snapshot` — which is exactly why the
    isolation invariant is the right thing to be checking.],
) <tab-ch4-bad-actions>

=== Running the twin, and watching it fail

Now the payoff. Point `quint run` at the twin and ask it to check the isolation invariant:

```bash
mise exec -- quint run quint-models/turbolay/m2_snapshot_read_buggy.qnt \
  --main m2_snapshot_read_buggy --invariant pageMatchesSnapshot --max-steps 8
```

Where the correct model's `allSafety` reported `[ok] No violation found`, the twin reports the
opposite, and — crucially — it does not just say "false." It hands you the shortest sequence of
actions that reaches a state where `pageMatchesSnapshot` is false:

```
[State 0]
{ current: { epoch: 1, rows: [1, 2] }, snapshot: { epoch: 0, rows: [] },
  snapshotOpen: false, page: { epoch: 0, rows: [] }, pageReturned: false,
  invalidHistoricalReturned: false, lastAction: "init" }

[State 1]
{ current: { epoch: 1, rows: [1, 2] }, snapshot: { epoch: 0, rows: [] },
  snapshotOpen: false, page: { epoch: 1, rows: [1, 2] }, pageReturned: true,
  invalidHistoricalReturned: true,
  lastAction: "returnUnvalidatedHistoricalFromCurrent" }

[violation] Found an issue (26ms at 1038 traces/second).
error: Invariant violated
```

Read the trace as the story it is. State 0 is `init`: current graph at epoch 1 holding rows
`[1, 2]`, no snapshot taken, so `snapshot` is the empty view at epoch 0, and no page returned.
State 1 fires the bad action: it returns a page equal to `current` — `page` is now
`{ epoch: 1, rows: [1, 2] }` — while `snapshot` is still the empty view. Ask the invariant:
`pageMatchesSnapshot` is `not(pageReturned) or page == snapshot`. A page _was_ returned, and
`{epoch: 1, rows: [1,2]}` does not equal `{epoch: 0, rows: []}`. False. Violation.

The checker found the one-step path through BFG-002 first, because it is the shortest — a single
bad action from `init` already breaks the invariant. The BFG-001 path is a step longer and just
as real: open a snapshot, commit a write so `current` moves ahead of the frozen `snapshot`, then
`materializePageFromLiveStorage` returns the newer `current` as a page while the snapshot still
names the old view — `page` at epoch 2, `snapshot` at epoch 1, not equal, violation. Either way,
the experiment succeeded: the invariant that sat there looking harmless in the correct model
turned out to have real teeth, because the moment we added a transition that could reach the
forbidden state, it caught it and produced a trace naming the exact culprit.

#custom-box(title: [Why], icon: "tip", color: rgb("#c99700"))[
  A counterexample trace is the most useful thing a checker produces, and it is worth seeing why
  the buggy twin earns one where prose review cannot. A human reading `pageMatchesSnapshot` can
  agree it _looks_ right and still not know whether any reachable state can falsify it. The twin
  turns that open question into a mechanical one: introduce the defect, and the checker either
  produces a concrete counterexample — a specific, replayable sequence of actions and the state
  it lands in — or it does not, in which case the invariant was vacuous and you have found a
  hole in your own specification instead of in the code. The counterexample is also directly
  reusable: the deterministic-scenarios chapter turns exactly this kind of trace into a pinned,
  named regression test.
]

== A picture of the frontier the bug crosses

The write-path chapter drew the model as a tree of reachable states with a _shaded region_ where
`allSafety` holds and a forbidden state _outside_ it that no arrow could reach. The buggy twin is
best understood as one edit to that picture: it adds the arrow. The bug is not a bad _state_ — the
state "a page that disagrees with its snapshot" was always describable. The bug is the
_transition_ that makes that state reachable, the edge that crosses out of the safe region.

#figure(
  diagram(
    node-stroke: 0.6pt + reader-colors.border,
    node-outset: 0pt,
    spacing: (2.7cm, 1.2cm),
    // Shaded safe region: states where pageMatchesSnapshot holds. Declared
    // first so it sits behind the nodes. It encloses the spine and the correct
    // page node, but NOT the forbidden node.
    node(
      enclose: ((0, 0), (0, 1), (0, 2), (-1.5, 3)),
      inset: 14pt,
      corner-radius: 7pt,
      stroke: (dash: "dashed", paint: reader-colors.ok, thickness: 0.8pt),
      fill: reader-colors.ok_soft.transparentize(60%),
    ),
    // The shared spine: both models walk this, and it stays inside the region.
    node((0, 0), text(fill: reader-colors.text)[`init`\ current epoch 1], fill: reader-colors.surface_soft, width: 2.7cm),
    node((0, 1), text(fill: reader-colors.text)[snapshot open\ snapshot epoch 1], fill: reader-colors.info_soft, width: 2.7cm),
    node((0, 2), text(fill: reader-colors.text, hyphenate: false)[write commits\ current epoch 2,\ snapshot epoch 1], fill: reader-colors.surface_soft, width: 2.9cm),
    // Correct model's page node: inside the region, safe.
    node((-1.5, 3), text(fill: reader-colors.text, hyphenate: false)[page = snapshot\ rows ✓], fill: reader-colors.ok_soft, width: 2.5cm),
    // The forbidden state: outside the region.
    node((1.6, 3), text(fill: reader-colors.text, hyphenate: false)[*page = live\ current epoch 2*\ ≠ snapshot epoch 1], fill: reader-colors.bad_soft, width: 2.7cm, stroke: (dash: "dashed", paint: reader-colors.bad, thickness: 1pt)),
    // Spine edges (bold, both models walk them).
    edge((0, 0), (0, 1), "->", text(fill: reader-colors.info, size: 7.5pt)[`openSnapshot`], stroke: 1.3pt + reader-colors.info, label-side: right),
    edge((0, 1), (0, 2), "->", text(fill: reader-colors.info, size: 7.5pt)[`commitAppend`], stroke: 1.3pt + reader-colors.info, label-side: right),
    // Correct model's page edge: stays inside the region.
    edge((0, 2), (-1.5, 3), "->", text(fill: reader-colors.muted, size: 7pt)[intended model:\ read the snapshot], stroke: reader-colors.muted, label-side: left, label-pos: 0.55),
    // The BUG edge: crosses out of the safe region into the forbidden state.
    edge((0, 2), (1.6, 3), "->", text(fill: reader-colors.bad, size: 7pt)[*buggy twin:*\ `materializePageFromLiveStorage`], stroke: 1.4pt + reader-colors.bad, label-side: right, label-pos: 0.52),
  ),
  caption: [The buggy twin as one edit to the write-path picture. Both models share the bold
    spine — open a snapshot, commit a write so `current` moves ahead of the frozen `snapshot` —
    and both stay inside the shaded region where `pageMatchesSnapshot` holds. The intended model's
    page path reads the pinned snapshot and lands _inside_ the region (left, safe). The twin adds
    one red transition, `materializePageFromLiveStorage`, that reads live `current` instead and
    crosses _out_ of the region into a state the invariant forbids. The bug is the edge, not the
    state: the forbidden state was always describable; the twin is the arrow that reaches it, and
    `quint run` reports that arrow as a counterexample trace.],
) <fig-ch4-counterexample>

That single red edge is the whole method in one stroke. The correct model has no such arrow — its
page path lands in the safe region and its forbidden action is guarded by a bare `false` — so the
invariant is passed but untested. The twin supplies the arrow, the invariant fails, and now you
know the wall was load-bearing.

The same grammar draws the other half of the pairing, the witnesses. If an invariant is "the
shaded region is never breached," a witness is "this particular state inside the region is
actually reached" — a place the model must be able to stand, marked below with a star.

#figure(
  diagram(
    node-stroke: 0.6pt + reader-colors.border,
    node-outset: 0pt,
    spacing: (2.5cm, 1.15cm),
    // Shaded safe region enclosing the reachable states, not the forbidden one.
    node(
      enclose: ((0, 0), (0, 1), (0, 2), (1.5, 2), (0, 3), (0, 4)),
      inset: 13pt,
      corner-radius: 7pt,
      stroke: (dash: "dashed", paint: reader-colors.ok, thickness: 0.8pt),
      fill: reader-colors.ok_soft.transparentize(60%),
    ),
    node((0, 0), text(fill: reader-colors.text)[`init`\ no writer], fill: reader-colors.surface_soft, width: 2.5cm),
    node((0, 1), text(fill: reader-colors.text)[writer 1\ owns cell], fill: reader-colors.info_soft, width: 2.5cm),
    node((0, 2), text(fill: reader-colors.text)[edge present\ epoch += 1], fill: reader-colors.ok_soft, width: 2.5cm),
    // Witness states, marked with a star.
    node((1.5, 2), text(fill: reader-colors.text)[★ committed,\ reply lost], fill: reader-colors.warn_soft, width: 2.5cm),
    node((0, 3), text(fill: reader-colors.text)[★ writer 2\ took over], fill: reader-colors.info_soft, width: 2.5cm),
    node((0, 4), text(fill: reader-colors.text)[★ zombie write\ rejected], fill: reader-colors.ok_soft, width: 2.5cm),
    // Forbidden state outside the region, reachable by no arrow.
    node((2.75, 4), text(fill: reader-colors.text, hyphenate: false)[*zombie COMMITS*\ (no such action)], fill: reader-colors.bad_soft, width: 2.5cm, stroke: (dash: "dashed", paint: reader-colors.bad, thickness: 1pt)),
    edge((0, 0), (0, 1), "->", text(fill: reader-colors.muted, size: 7pt)[`openWriter1`], stroke: reader-colors.muted),
    edge((0, 1), (0, 2), "->", text(fill: reader-colors.muted, size: 7pt)[`createEdge`], stroke: reader-colors.muted),
    edge((0, 1), (1.5, 2), "->", text(fill: reader-colors.muted, size: 6.5pt)[`commitThenLoseReply`], stroke: (thickness: 0.6pt, paint: reader-colors.muted, dash: "dotted"), label-side: left, label-pos: 0.38),
    edge((0, 2), (0, 3), "->", text(fill: reader-colors.muted, size: 7pt)[crash, take over], stroke: reader-colors.muted),
    edge((0, 3), (0, 4), "->", text(fill: reader-colors.muted, size: 7pt)[`rejectZombieWrite`], stroke: reader-colors.muted),
    // The transition that does not exist.
    edge((0, 4), (2.75, 4), "-->", text(fill: reader-colors.bad, size: 6.5pt)[✗ no such\ transition], stroke: (thickness: 0.8pt, paint: reader-colors.bad, dash: "dashed"), label-pos: 0.55),
  ),
  caption: [The invariant and the witnesses on one map of `m1_cell_write`. The invariant is the
    shaded region: `allSafety` holds in _every_ state inside it. The witnesses are the ★ states —
    `lostReplyReached`, `takeoverReached`, `zombieRejectionReached` — each of which must be
    _reachable at least once_, proving the model actually exercises the lost-reply, takeover, and
    fencing paths rather than passing its invariants by avoiding them. The forbidden "zombie
    commits" state sits outside the region with no arrow reaching it; the buggy twin of the
    previous figure is what happens when someone draws that arrow.],
) <fig-ch4-witnesses>

== What the method proves, and what it does not

Be precise about what you now have. A correct model whose `allSafety` passes `quint run`, whose
witnesses are all reached, and whose buggy twin _fails_ the corresponding invariant gives you
three distinct assurances: the intended behavior satisfies the contract across a large sample of
behaviors; the model genuinely exercises its interesting cases rather than dodging them; and the
invariant is falsifiable — it can tell the bug from the intended behavior. That is a great deal
more than an English specification and a code review can offer, and it is why every bug in the
`0003` evidence document that could be expressed in the bounded model has a model counterexample
attached to it (`0003-turbolay-quint-verification-evidence.md`, the evidence-boundary table lists
BFG-001 and BFG-002 with "fault model counterexample").

It is still not a proof. `quint run` sampled behaviors; it did not enumerate them. The buggy twin
demonstrated that _one_ invariant catches _one_ class of defect; it says nothing about defects
nobody thought to model as a twin. And the whole exercise lives inside the shared-state
abstraction of the previous chapter, which deliberately discards byte encoding, storage
mechanics, and scheduling. The evidence document states the ceiling plainly: a bounded check
"does not prove arbitrary graph sizes, arbitrary S3 failure behavior, or the Rust implementation"
(`0003-turbolay-quint-verification-evidence.md`, the evidence-boundary section). What the buggy
twin buys is confidence that the checks themselves are not hollow — and that is precisely the
confidence a green light most needs and least advertises.

You can now read the last page of any turbolay model and say, for each `val`, whether it is an
invariant that must always hold or a witness that must be reachable; run it with
`quint run --invariant`; and, given a buggy twin, predict that the checker will produce a
counterexample and read the trace it returns. The next chapter, on _deterministic scenarios_,
picks up exactly where the counterexample left off: it shows how a specific trace — a fixed
`run`/`then`/`expect` sequence — pins one named path through a model so a human can read it and a
test can replay it, turning both the passing storyline and the buggy twin's counterexample into
permanent, checkable scenarios. After that, the _model gallery_ chapter reads all eight turbolay
model families end to end, and every one of them ends in the same two blocks you can now read on
sight: the invariants that must always hold, and the witnesses that must be reached.
