#import "../vendor/bookly/src/bookly.typ": *
#import "@preview/fletcher:0.5.7" as fletcher: diagram, node, edge

= Quint from zero

The previous chapter argued that turbolay's hardest guarantees, single-writer fencing,
snapshot isolation, exactly-once retries, are the kind of thing an English sentence can
describe but cannot enforce. This chapter introduces the tool the rest of this part uses to
turn those sentences into something a machine can check.

#custom-box(title: [Term — Quint], icon: "info")[
  An executable specification language for state machines. You write down the states a system
  can be in and the steps it may take between them, and Quint can then run the specification
  like a program, generate example executions, and, with a backend checker, prove that a
  property holds across every reachable state. It is a modern, programmer-friendly surface over
  the same ideas as TLA+.
]

The goal of this chapter is small and concrete. By the end you will be able to read a tiny
Quint model, understand every keyword in it, and run it yourself from the command line. We do
not touch turbolay's real models until the very last section, and even then only to point at
lines you will already be able to read. Everything in between is built on one deliberately
tiny toy: a coin-operated turnstile. It is small enough to hold in your head and complete
enough to actually run.

== Why write a specification at all

Start with the problem the specification solves. Here is a sentence from turbolay's write
contract, in plain English:

#quote(block: true)[
  "Once a create has been committed, retrying the same create must not write again; it returns
  the outcome that was already recorded."
]

That sentence is clear enough to a human, and completely inert. Nothing checks it. If a
future refactor makes a retry advance the epoch a second time, the sentence still sits in the
document, still reads correctly, and is now a lie. English prose specifications rot silently
because there is no mechanical link between the words and the behavior.

#custom-box(title: [Term — State machine], icon: "info")[
  A description of a system as a set of possible states plus the rules for moving between
  them. It has a starting state and a transition rule that says, given the current state, which
  next states are allowed. Almost any stateful system, a protocol, a database, a UI, can be
  modeled as one.
]

An executable specification fixes the rot by making the specification a program. You describe
the system as a state machine and hand it to Quint. Now the sentence
above is not prose; it is a property you can evaluate against actual executions of the model.
If the model can reach a state where a retry wrote twice, Quint hands you the exact sequence
of steps that got there.

#custom-box(title: [Why], icon: "tip")[
  An English spec is read by people and enforced by nobody. An executable spec is read by
  people #emph[and] run by a machine. The moment the model and the claimed property disagree,
  the tool produces a concrete counterexample, a specific trace of steps, rather than leaving
  the contradiction latent in a paragraph. That counterexample is the entire value: it turns
  "I think this is correct" into "here is the sequence of events that breaks it."
]

A Quint specification of a state machine has exactly three moving parts, and the rest of this
chapter is those three parts and nothing more:

- the *state*: a handful of variables that together capture everything the system remembers;
- an *`init`*: the one starting state;
- a *`step`*: the rule for how the state is allowed to change.

#figure(
  diagram(
    crossing-fill: reader-colors.paper,
    node-stroke: 0.6pt + reader-colors.border,
    spacing: (1.6cm, 1.25cm),
    node((0, 0), text(fill: reader-colors.muted, size: 8pt)[start], stroke: none),
    node((0, 1), text(fill: reader-colors.text)[*State*\ the `var`s — everything\ the machine remembers], fill: reader-colors.info_soft, width: 4.6cm),
    edge((0, 0), (0, 1), "->", text(fill: reader-colors.muted, size: 8pt)[`init` sets the first values], stroke: reader-colors.muted),
    edge((0, 1), (0, 1), "->", text(fill: reader-colors.muted, size: 8pt)[`step`: fire any one enabled action], bend: 135deg, stroke: reader-colors.muted),
  ),
  caption: [The three moving parts, and how they relate. The *state* is the variables; `init` sets their first values; `step` is a self-transition that carries the state to a next state by firing any one enabled action. Everything in this chapter is one of these three.],
) <fig-quint01-three-parts>

Let us build all three for the turnstile.

== The system: a coin turnstile

Picture a subway turnstile. It is either *locked* or *unlocked*. Insert a coin into a locked
turnstile and it unlocks. Push through an unlocked turnstile and it locks again behind you.
Pushing a locked turnstile does nothing, it just sits there. That is the whole machine, and
it is a genuine two-state gate, the same shape as many real protocols.

#figure(
  diagram(
    crossing-fill: reader-colors.paper,
    node-stroke: 0.6pt + reader-colors.border,
    node-fill: reader-colors.info_soft,
    spacing: (3.2cm, 1.4cm),
    node((0, 0), text(fill: reader-colors.text)[locked], width: 2.4cm, shape: fletcher.shapes.pill),
    node((1, 0), text(fill: reader-colors.text)[unlocked], width: 2.4cm, shape: fletcher.shapes.pill, fill: reader-colors.ok_soft),
    edge((0, 0), (1, 0), "->", text(fill: reader-colors.text)[`insertCoin`], bend: 30deg, stroke: reader-colors.muted),
    edge((1, 0), (0, 0), "->", text(fill: reader-colors.text)[`push`], bend: 30deg, stroke: reader-colors.muted),
    edge((0, 0), (0, 0), "->", text(fill: reader-colors.muted)[`push` (no-op)], bend: 130deg, stroke: reader-colors.muted),
  ),
  caption: [The turnstile as a state machine. Two states, and three kinds of step. A coin unlocks; a push through an unlocked gate re-locks it; a push against a locked gate changes nothing.],
) <fig-quint01-turnstile>

We will also make the turnstile count, so that the model has some data in it and not just a
single flag. It keeps a running tally of coins taken and passes made. That gives us a real
record to model and a real numeric property to check at the end ("you can never take more
passes than you paid coins for").

== A module and its state

Every Quint file is a `module`. Inside it, the state of the machine is declared with `var`.

#custom-box(title: [Term — State variable], icon: "info")[
  A named piece of the system's memory, declared with `var`. The values of all the state
  variables together are the current state of the machine. Nothing outside the state variables
  persists between steps; if the machine needs to remember something, it must live in a `var`.
]

Our turnstile remembers three things: whether it is locked, its running meter of coins and
passes, and the name of the last step it took (a convenience we use for readable output). We
will introduce the meter as a small record type. The model lives in `toy/turnstile.qnt`:

```rust
module turnstile {
  // A record type: a value with named fields, each with its own type.
  type Meter = { coins: int, passes: int }

  var locked: bool     // is the gate currently locked?
  var meter: Meter     // running tally of coins taken and passes made
  var lastAction: str  // name of the most recent step, for readable traces
}
```

Two things to notice. First, `type Meter = { coins: int, passes: int }` declares a
#emph[record]: a single value carrying named fields, exactly like the `GraphView = { epoch:
int, rows: List[int] }` you will meet in turbolay's real snapshot model. A `Meter` value is
written `{ coins: 0, passes: 0 }`, and you read a field out of one with a dot: `meter.coins`.

Second, the state variables are only #emph[declared] here, not given values. A `var` with no
value is like an uninitialized field: the machine is not in a valid state until `init` fills
every variable in. We do that next.

=== Constants and derived values: `pure val`

Not everything in a model changes over time. Some values are fixed once and never move, a
capacity, a starting configuration, a constant. Those are #emph[not] state; putting them in a
`var` would be a mistake. Quint spells a constant `pure val`.

```rust
  // A constant. Computed once, never part of the changing state.
  pure val startLocked: bool = true
```

The word `pure` is a promise: the value depends on nothing but other pure values, never on a
state variable, so it is the same in every state. In turbolay's snapshot model you will see
`pure val initialRows: List[int] = List(1, 2)` used the same way, to name a fixed starting
value that `init` then installs. Keep the distinction sharp in your head:

- `var` is memory. It changes as the machine steps.
- `pure val` is a constant. It is fixed for the whole run.

== `init`: the starting state

The machine needs somewhere to begin. That is `init`, and it is our first action.

#custom-box(title: [Term — action], icon: "info")[
  A named rule describing a state transition, declared `action name: bool = all { ... }`. It
  evaluates to a boolean: `true` means "this transition is allowed and here is the resulting
  next state," `false` means "this transition is not possible right now." `init` is the special
  action that describes the starting state.
]

```rust
  action init: bool = all {
    locked' = startLocked,
    meter' = { coins: 0, passes: 0 },
    lastAction' = "init",
  }
```

Read it top to bottom. `init` sets the gate locked, zeroes the meter, and records that the
last action was `"init"`. Every one of the three state variables gets a value. That is not
optional: `init` must pin down the complete starting state, so it must assign #emph[every]
`var` the module declares. Leave one out and the machine would begin in a half-defined state,
which Quint does not permit.

=== Primed variables: naming the next state

The odd part is the apostrophe. `locked'`, not `locked`.

#custom-box(title: [Term — Primed variable], icon: "info")[
  Inside an action, `x` means the value of state variable `x` in the #emph[current] state, and
  `x'` (pronounced "x prime") means its value in the #emph[next] state, after the transition.
  An assignment `x' = e` says "in the next state, `x` takes the value `e`." An action defines a
  transition precisely by giving the primed value of every state variable.
]

So `locked' = startLocked` does not mean "set locked" the way an imperative program would. It
is a #emph[claim about the next state]: after this transition, `locked` equals `startLocked`.
The unprimed names on the right-hand side still refer to the current state, which is what lets
an action describe a change in terms of what came before, as we are about to see with the
meter.

=== `all { ... }` is a conjunction, not a sequence

One more subtlety hides in that `all { ... }` block. It looks like a list of statements
executed in order. It is not. `all { ... }` is a logical #emph[and]: every line must hold for
the whole action to hold. The lines are joined by conjunction, not run in sequence, so their
order does not matter and there is no notion of one line happening "before" another. In
`init` all three lines are assignments, so the effect is simply "the next state has `locked =
true` and `meter = {0,0}` and `lastAction = "init"`, all at once." When an action mixes
assignments with plain conditions, that same "all of these must hold" reading is what makes
guards work, which is the next idea.

== The steps: actions with guards

Now the interesting transitions. Inserting a coin is only meaningful when the gate is locked;
it takes a coin and unlocks. Here it is.

```rust
  action insertCoin: bool = all {
    locked,                                              // guard
    locked' = false,                                     // now unlocked
    meter' = { coins: meter.coins + 1, passes: meter.passes },
    lastAction' = "insertCoin",
  }
```

The first line, `locked`, is different in kind from the other three. It has no prime and no
assignment. It is just a boolean expression over the current state, and because `all { ... }`
requires every line to hold, it acts as a guard.

#custom-box(title: [Term — guard / precondition], icon: "info")[
  A plain boolean line inside an action's `all { ... }` block, with no primed variable. It is a
  condition on the current state that must be true for the action to be enabled. If the guard is
  false, the whole `all { ... }` is false, so the action simply cannot happen from this state.
  Guards are how a model says "this step is only possible when...".
]

The reading is: `insertCoin` is possible only when `locked` is true; and when it happens, the
gate becomes unlocked and the coin count goes up by one. Notice the right-hand side of the
meter assignment: `meter.coins + 1` reads the #emph[current] coin count (unprimed) and the
primed `meter'` installs the incremented record as the next state. Current on the right,
primed on the left, that is the universal shape.

Pushing through is the mirror image. It is only possible when the gate is #emph[un]locked, and
it re-locks the gate and counts a pass.

```rust
  action push: bool = all {
    not(locked),                                         // guard: only when open
    locked' = true,                                      // re-locks behind you
    meter' = { coins: meter.coins, passes: meter.passes + 1 },
    lastAction' = "push",
  }
```

And what about shoving a #emph[locked] turnstile? Physically it does not budge. We model that
as an explicit no-op step: it is enabled only when locked, and it leaves every variable
unchanged.

```rust
  action pushWhenLocked: bool = all {
    locked,                                              // guard: gate is shut
    locked' = locked,                                    // unchanged
    meter' = meter,                                      // unchanged
    lastAction' = "pushWhenLocked",
  }
```

Look closely at how "unchanged" is spelled: `locked' = locked` and `meter' = meter`. There is
no implicit "leave it as it was" in Quint. Every action must give a next value to every state
variable, so even a variable that does not change must be assigned its own current value. This
is the same discipline you saw in turbolay's `m1_cell_write.qnt`, where an action that only
opens a writer still carries every unrelated variable forward with lines like `epoch' = epoch`
and `edgePresent' = edgePresent`. It looks verbose, and it is, but it makes every transition
total and unambiguous: reading one action tells you the fate of the entire state.

#custom-box(title: [Why], icon: "tip")[
  Forcing every action to assign every variable removes a whole class of specification bug: the
  variable you "forgot" to think about. In an imperative program, an omitted update just leaves
  the old value silently. In a state machine used for verification, silence is dangerous, an
  unstated next value would make the transition ambiguous. So Quint makes you say it. If a step
  genuinely leaves `x` alone, you write `x' = x` and the intent is on the page.
]

== `step`: how the machine moves, and nondeterminism

We have three possible transitions: `insertCoin`, `push`, `pushWhenLocked`. Something has to
say which one the machine takes. That something is `step`, and its definition is where a new
and important idea enters.

```rust
  action step: bool = any {
    insertCoin,
    push,
    pushWhenLocked,
  }
```

Where `init` used `all { ... }`, `step` uses `any { ... }`. If `all` is a logical and, `any`
is a logical #emph[or]: the machine takes a step if #emph[any] one of the listed actions can
fire. From a given state, Quint looks at which of the three actions have a satisfied guard,
and the step is a nondeterministic choice among exactly those.

#custom-box(title: [Term — Nondeterminism], icon: "info")[
  A model is nondeterministic when, from one state, more than one next step is allowed and the
  model does not say which is taken. `any { a, b, c }` expresses this directly: whichever of the
  enabled actions could happen, may happen. Nondeterminism is not vagueness; it is coverage. One
  nondeterministic model stands in for every possible sequence of choices at once.
]

Trace it from the start. After `init` the gate is locked. Which actions are enabled? `push`
needs `not(locked)`, so its guard is false, it is out. `insertCoin` needs `locked`, true, so
it is in. `pushWhenLocked` needs `locked`, also true, so it is in too. So from the locked
start state the machine may nondeterministically either take a coin or be shoved uselessly.
Take the coin, and now the gate is unlocked: `insertCoin` and `pushWhenLocked` fall out
(their `locked` guard is now false), and only `push` is enabled. Guards, in other words, are
what shape the nondeterminism: at every state the set of enabled actions is exactly those
whose guards hold.

#custom-box(title: [Why], icon: "tip")[
  Nondeterminism is the reason a single tiny model can say something about a whole messy
  reality. A real turnstile is used by thousands of people in an order nobody controls, coins
  and pushes interleaved every possible way. We do not want to write one model per possible
  order; there are astronomically many. Instead `any { ... }` says "any enabled action may fire
  next," and a checker can then explore #emph[all] the interleavings the model permits. This is
  exactly why turbolay's models end their transition with `action step: bool = any { ... }`
  listing every writer action, crash, and takeover: one `any` block stands for every order in
  which those events could really occur.
]

== Properties: a first `val`

We have a complete, runnable machine. But a machine that merely runs tells us nothing; we want
to state and check things that should always be true of it. The first taste of that is a named
predicate written with `val`.

#custom-box(title: [Term — State predicate (`val ... : bool`)], icon: "info")[
  A named boolean expression over the current state. `val p: bool = ...` gives a name to a
  claim about any state the machine can be in. Evaluated in a given state it is simply true or
  false. Used as a property to check, it asserts that the claim holds in every reachable state.
]

Here are two. The first is the obvious sanity check, a count is never negative. The second is
the real safety claim of a turnstile: you cannot get more passes out of it than the coins you
put in.

```rust
  val coinsNeverNegative: bool = meter.coins >= 0

  val neverMorePassesThanCoins: bool = meter.passes <= meter.coins
```

Convince yourself `neverMorePassesThanCoins` really holds. Every `push` is guarded by
`not(locked)`, and the only way to become unlocked is an `insertCoin`, which raised `coins`.
So each pass is "paid for" by a coin that came before it, and `passes` can never overtake
`coins`. That is precisely the kind of invariant a checker can confirm holds across every
reachable state, not just the handful we traced by hand.

This is only a first taste. A later chapter treats properties in full: invariants that must
never be violated, witnesses that must be reachable, and how a checker searches for
counterexamples. Here the point is narrower, that a `val ... : bool` is just a named claim
about the state, written in ordinary boolean expressions over the same variables the actions
use. In turbolay's models these accumulate into a single `val allSafety: bool = ...` that
conjoins every safety property of the contract; you have already seen its shape in
`m1_cell_write.qnt`.

== The whole toy, in one piece

Here is the complete module. It is valid Quint: `init` assigns every variable, every action
assigns every primed variable, `step` is an `any { ... }` over the actions, and every guard is
a plain boolean. You can type this into a file and run it.

```rust
// A coin-operated turnstile as a two-state machine.
module turnstile {
  type Meter = { coins: int, passes: int }

  pure val startLocked: bool = true

  var locked: bool
  var meter: Meter
  var lastAction: str

  action init: bool = all {
    locked' = startLocked,
    meter' = { coins: 0, passes: 0 },
    lastAction' = "init",
  }

  // A coin unlocks a locked gate and is counted.
  action insertCoin: bool = all {
    locked,
    locked' = false,
    meter' = { coins: meter.coins + 1, passes: meter.passes },
    lastAction' = "insertCoin",
  }

  // Pushing through an open gate re-locks it and counts a pass.
  action push: bool = all {
    not(locked),
    locked' = true,
    meter' = { coins: meter.coins, passes: meter.passes + 1 },
    lastAction' = "push",
  }

  // Shoving a locked gate does nothing.
  action pushWhenLocked: bool = all {
    locked,
    locked' = locked,
    meter' = meter,
    lastAction' = "pushWhenLocked",
  }

  action step: bool = any {
    insertCoin,
    push,
    pushWhenLocked,
  }

  val coinsNeverNegative: bool = meter.coins >= 0
  val neverMorePassesThanCoins: bool = meter.passes <= meter.coins
}
```

Forty lines, and it is a genuine, checkable model of a system. Everything you need to read
turbolay's models is on this page: `module`, `var`, `pure val`, a record `type`, `action ...:
bool = all { ... }` with guards and primed assignments, `init`, `step` as `any { ... }`, and a
`val ... : bool` property.

== Running it yourself

A specification you cannot run is just prose with punctuation. Quint is executable, and
turbolay drives it through Mise, the tool-version manager, so every command is prefixed
`mise exec -- quint ...`. Save the module above as `turnstile.qnt` and try these three.

=== Typecheck it

First, does it even make sense? `typecheck` catches the mechanical errors, a misspelled
variable, a missing assignment, a type mismatch, before you try to run anything.

```bash
mise exec -- quint typecheck turnstile.qnt
```

If a variable went unassigned in some action, or you compared an `int` to a `str`, this is
where you would hear about it. A clean typecheck is the price of entry; turbolay's CI
typechecks every model in the directory this way.

=== Run it

`run` executes the machine: it starts from `init` and then repeatedly takes a `step`, making a
random nondeterministic choice among the enabled actions at each point, and prints the
resulting trace of states.

```bash
mise exec -- quint run turnstile.qnt --max-steps 8
```

You will see the state evolve, `locked` flipping, `meter.coins` and `meter.passes` climbing,
`lastAction` naming each move. Run it a few times and the nondeterminism shows itself: because
`step` is an `any { ... }`, different runs take different paths through the machine. This is
the cheapest way to sanity-check that your model does what you think.

=== Explore it interactively

For poking at pieces by hand there is a REPL. `mise exec -- quint repl` drops you into a
session where you can load the module and evaluate expressions, individual actions, guards,
`val` predicates, one at a time. It is the fastest way to answer "is this guard true here?"
without writing a whole run.

=== What comes later

The three commands above are the whole toolkit for this chapter. The rest of this part reaches
for three more, each introduced properly in its own chapter. One sentence each, as a map:

- `mise exec -- quint test` runs #emph[deterministic] scenarios, a fixed sequence of steps with
  assertions after each, rather than random exploration. You already glimpsed the syntax in
  turbolay's tests: `run someTest = init.then(openWriter1).then(createEdge).expect(edgePresent)`.
  We teach `run` / `then` / `expect` in full later; for now just know it exists.
- `mise exec -- quint verify` hands the model to Apalache, a bounded model checker, to
  #emph[prove] a property like `--invariant allSafety` holds across every state up to some depth,
  rather than sampling a few runs.
- `mise exec -- quint run --mbt` emits an action-labelled trace as a file, the raw material for
  model-based testing, where the same steps are later replayed against the real Rust code.

Do not worry about these yet. They all stand on the exact foundation you just built: a module,
its state, `init`, `step`, and properties.

== You can almost read turbolay already

To close, look at a few lines from turbolay's real write model. Do not study it; just notice
how little of it is now unfamiliar.

```rust
module m1_cell_write {
  var epoch: int
  var previousEpoch: int
  var edgePresent: bool
  // ... more state ...

  action createEdge: bool = all {
    activeWriter == 1,
    writer1Live,
    not(edgePresent),
    not(createRecorded),
    epoch' = epoch + 1,
    previousEpoch' = epoch,
    edgePresent' = true,
    // ... every other variable assigned ...
    lastAction' = "createEdge",
  }

  action step: bool = any {
    openWriter1,
    createEdge,
    // ... more actions ...
  }

  val epochNeverRegresses: bool = epoch >= previousEpoch
}
```

Every construct here is one you now know. `var epoch: int` is a state variable. `createEdge`
is an `action ...: bool = all { ... }`; its first four lines, `activeWriter == 1`,
`writer1Live`, `not(edgePresent)`, `not(createRecorded)`, are guards, the preconditions under
which a create is allowed. The primed lines below them, `epoch' = epoch + 1` and the rest,
define the next state, and note the same discipline as the toy: `previousEpoch' = epoch` saves
the old epoch, and every other variable is assigned too. `step` is an `any { ... }` over all
the writer actions, the nondeterministic choice that lets one model cover every order of
creates, deletes, crashes, and takeovers. And `val epochNeverRegresses: bool = epoch >=
previousEpoch` is a state predicate, a named safety claim, exactly like
`neverMorePassesThanCoins` on our turnstile.

The only thing separating this model from the toy is domain vocabulary, epochs, writers,
fences, not new language machinery. The next chapter reads `m1_cell_write.qnt` in full and
explains what each of those variables means and why the contract is shaped the way it is. You
have the grammar; now we bring in the words.
