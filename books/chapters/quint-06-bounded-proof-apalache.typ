#import "../vendor/bookly/src/bookly.typ": *
#import "@preview/fletcher:0.5.7" as fletcher: diagram, node, edge

// LEARNING GOAL: the reader can state precisely how `quint run` (randomized,
// bounded SIMULATION — a sample of behaviors) differs from `quint verify` /
// Apalache (BOUNDED EXHAUSTIVE symbolic model checking — every behavior up to a
// fixed step bound, proven free of counterexamples), invoke `quint verify` for
// turbolay's high-value safety invariants, read a bounded-proof evidence
// record, and say exactly what a bounded exhaustive check does and does NOT
// establish (all paths <= N steps: yes; unbounded depth / unbounded graph /
// S3-SlateDB internals: no).
// GROUND IN:
//   docs/formal-methods/0001-quint-jepsen-testing-objective.md  (layer 2; bounded model checking subsection; simulation-not-a-proof)
//   docs/formal-methods/0002-turbolay-quint-specification-plan.md  (Apalache targets; bounded-proof command; Apalache-friendly requirements)
//   docs/formal-methods/0003-turbolay-quint-verification-evidence.md  (recorded six-step NoError evidence; evidence boundary; reproducible command)
//   quint-models/turbolay/m1_cell_write.qnt  (allSafety, the invariant that gets the bounded-exhaustive treatment)

= Bounded proof: `quint verify` and Apalache

The gallery chapter closed on a promise and a doubt. The promise was that every turbolay
model ends in the same two blocks — invariants that must always hold, witnesses that must be
reached — and that you can now read any of them on sight. The doubt was sharper. When you ran
the write-path model's `allSafety` through `quint run` you got `[ok] No violation found` after
ten thousand random walks of twelve steps each, and the invariants-and-buggy-twin chapter was
careful to call that what it is: strong evidence, and _not a proof_. Ten thousand walks is a
large sample of the write path's behaviors. It is still a sample. This chapter is about the
tool that stops sampling and starts enumerating — bounded, symbolic, exhaustive — and about
being exact, to the last word, on what that buys and what it does not.

This is the payoff the earlier chapter pointed at directly: the gap "gets closed — partially —
when the Apalache checker re-examines the same invariants exhaustively up to a fixed number of
steps." We are here now. The invariant is the same `allSafety` you already know from the
write-path model; nothing about the property changes. What changes is the _quality of the
answer_ the tool gives about it.

== The gap a sample leaves

Recall exactly what `quint run` does, because the whole chapter turns on its one limitation.
`quint run` is a randomized, bounded simulator: it starts at `init`, walks a random sequence
of `step`s up to a length bound, checks the invariant at each state it lands in, and repeats
that for as many separate samples as you ask. The write-path evidence ran it at ten thousand
samples of twelve steps (`0003-turbolay-quint-verification-evidence.md`, the P0 completion
evidence). Every state on every walk satisfied all seven safety predicates. That is genuine,
cheap, fast evidence — and it is silent about every behavior the dice never rolled.

Here is the failure mode that silence hides. Suppose the write path had a bug that shows up on
exactly one interleaving: open a writer, commit-then-lose-reply, crash, take over, and _only_
if a conflicting retry arrives at precisely that point does an invariant break. If that path is
one arrangement among millions of length-twelve behaviors, a random sample of ten thousand can
walk right past it ten thousand times and report `[ok]`. The check was not wrong; it was
incomplete. A clean `quint run` means "no counterexample in the region I happened to explore,"
which is categorically weaker than "no counterexample exists." The objective document states
this in the same breath as the command itself: a run with no counterexample is "reported as
simulation evidence, not a proof" (`0001-quint-jepsen-testing-objective.md:151-152`).

So the question this chapter answers is precise. Not "how do we test more paths?" — sampling
more paths only ever samples more paths. The question is: how do we replace "the paths we
walked" with _all_ paths, at least up to some honest, stated length? That requires a different
kind of tool.

== What "exhaustive to a bound" means

The move is to stop walking paths one at a time and instead ask a single question about all of
them at once: _is there any behavior of at most N steps, starting from `init`, that reaches a
state where the invariant is false?_ If the answer is a rigorous "no," then no such
counterexample exists — not "was not sampled," but does not exist — among every behavior of
length N or shorter. That is a bounded exhaustive check.

#custom-box(title: [Term — Bounded exhaustive model checking], icon: "info")[
  A check that examines _every_ behavior of a model up to a fixed number of transitions, N,
  and reports whether any of them violates a stated invariant. Unlike simulation, it does not
  sample: within the bound N it leaves no behavior unexamined, so a clean result means "no
  counterexample of N steps or fewer exists," not merely "none was found." Unlike an unbounded
  proof, it says nothing about behaviors longer than N. It is exhaustive in breadth (all paths)
  but bounded in depth (only up to N steps).
]

The tool that does this for Quint is Apalache, reached through the `quint verify` command.
Crucially, it does not do the enumeration by brute force — walking a tree with millions of
leaves would be as hopeless as sampling is incomplete. It works _symbolically_.

#custom-box(title: [Term — Symbolic model checking], icon: "info")[
  Instead of executing behaviors one concrete state at a time, a symbolic checker encodes
  "some behavior of length N reaches a bad state" as a single logical formula over all the
  model's variables at every step, and hands that formula to an SMT solver — a decision engine
  that determines whether any assignment of values makes it true. If the solver finds one, that
  assignment _is_ a counterexample trace. If it proves none can exist (the formula is
  unsatisfiable), the invariant holds across every behavior of length N at once, without any
  behavior ever being run individually. Apalache is the symbolic engine `quint verify` drives.
]

The difference is best held as one picture over the same tree of states. `quint run` walks a
few bold threads and leaves the rest of the tree dark. `quint verify` fills the entire tree
down to a solid frontier at depth N and proves the safe region un-breached everywhere above it.
And beyond that frontier the tree keeps going, into a depth no tool in this book reaches.

#figure(
  kind: image,
  supplement: [Figure],
  grid(
    columns: 3,
    column-gutter: 0.5cm,
    row-gutter: 0.5cm,
    align: center + horizon,
    // --- column headers ---
    text(fill: reader-colors.text, size: 8.5pt)[*`quint run`*\ #text(fill: reader-colors.muted, size: 7pt)[randomized simulation]],
    text(fill: reader-colors.text, size: 8.5pt)[*`quint verify`*\ #text(fill: reader-colors.muted, size: 7pt)[bounded exhaustive]],
    text(fill: reader-colors.text, size: 8.5pt)[*beyond the bound*\ #text(fill: reader-colors.muted, size: 7pt)[unbounded, unknown]],
    // ===================== PANEL A: sampling =====================
    diagram(
      crossing-fill: reader-colors.paper,
      node-outset: 0pt,
      spacing: (0.62cm, 0.66cm),
      // faint nodes (unsampled)
      node((-0.5, 2), [], width: 0.34cm, height: 0.34cm, inset: 0pt, corner-radius: 2pt, fill: reader-colors.surface_soft.transparentize(50%), stroke: 0.4pt + reader-colors.border),
      node((0.5, 2), [], width: 0.34cm, height: 0.34cm, inset: 0pt, corner-radius: 2pt, fill: reader-colors.surface_soft.transparentize(50%), stroke: 0.4pt + reader-colors.border),
      node((-0.5, 3), [], width: 0.34cm, height: 0.34cm, inset: 0pt, corner-radius: 2pt, fill: reader-colors.surface_soft.transparentize(50%), stroke: 0.4pt + reader-colors.border),
      node((0.5, 3), [], width: 0.34cm, height: 0.34cm, inset: 0pt, corner-radius: 2pt, fill: reader-colors.surface_soft.transparentize(50%), stroke: 0.4pt + reader-colors.border),
      node((1.5, 3), [], width: 0.34cm, height: 0.34cm, inset: 0pt, corner-radius: 2pt, fill: reader-colors.surface_soft.transparentize(50%), stroke: 0.4pt + reader-colors.border),
      // sampled (bold) nodes
      node((0, 0), [], width: 0.38cm, height: 0.38cm, inset: 0pt, corner-radius: 2pt, fill: reader-colors.info_soft, stroke: 1pt + reader-colors.info),
      node((-1, 1), [], width: 0.38cm, height: 0.38cm, inset: 0pt, corner-radius: 2pt, fill: reader-colors.info_soft, stroke: 1pt + reader-colors.info),
      node((1, 1), [], width: 0.38cm, height: 0.38cm, inset: 0pt, corner-radius: 2pt, fill: reader-colors.info_soft, stroke: 1pt + reader-colors.info),
      node((-1.5, 2), [], width: 0.38cm, height: 0.38cm, inset: 0pt, corner-radius: 2pt, fill: reader-colors.info_soft, stroke: 1pt + reader-colors.info),
      node((1.5, 2), [], width: 0.38cm, height: 0.38cm, inset: 0pt, corner-radius: 2pt, fill: reader-colors.info_soft, stroke: 1pt + reader-colors.info),
      node((-1.5, 3), [], width: 0.38cm, height: 0.38cm, inset: 0pt, corner-radius: 2pt, fill: reader-colors.info_soft, stroke: 1pt + reader-colors.info),
      // faint edges
      edge((-1, 1), (-0.5, 2), stroke: (dash: "dotted", paint: reader-colors.muted, thickness: 0.4pt)),
      edge((1, 1), (0.5, 2), stroke: (dash: "dotted", paint: reader-colors.muted, thickness: 0.4pt)),
      edge((-0.5, 2), (-0.5, 3), stroke: (dash: "dotted", paint: reader-colors.muted, thickness: 0.4pt)),
      edge((0.5, 2), (0.5, 3), stroke: (dash: "dotted", paint: reader-colors.muted, thickness: 0.4pt)),
      edge((1.5, 2), (1.5, 3), stroke: (dash: "dotted", paint: reader-colors.muted, thickness: 0.4pt)),
      // bold sampled edges
      edge((0, 0), (-1, 1), stroke: 1.2pt + reader-colors.info),
      edge((0, 0), (1, 1), stroke: 1.2pt + reader-colors.info),
      edge((-1, 1), (-1.5, 2), stroke: 1.2pt + reader-colors.info),
      edge((1, 1), (1.5, 2), stroke: 1.2pt + reader-colors.info),
      edge((-1.5, 2), (-1.5, 3), stroke: 1.2pt + reader-colors.info),
    ),
    // ===================== PANEL B: bounded exhaustive =====================
    diagram(
      crossing-fill: reader-colors.paper,
      node-outset: 0pt,
      spacing: (0.62cm, 0.66cm),
      // shaded proven-safe region, behind the nodes
      node(
        enclose: ((0, 0), (-1.5, 2), (1.5, 2), (-1.5, 3), (1.5, 3)),
        inset: 7pt, corner-radius: 5pt,
        stroke: (dash: "dashed", paint: reader-colors.ok, thickness: 0.7pt),
        fill: reader-colors.ok_soft.transparentize(55%),
      ),
      // every node up to the frontier, filled
      node((0, 0), [], width: 0.38cm, height: 0.38cm, inset: 0pt, corner-radius: 2pt, fill: reader-colors.ok_soft, stroke: 0.5pt + reader-colors.border),
      node((-1, 1), [], width: 0.38cm, height: 0.38cm, inset: 0pt, corner-radius: 2pt, fill: reader-colors.ok_soft, stroke: 0.5pt + reader-colors.border),
      node((1, 1), [], width: 0.38cm, height: 0.38cm, inset: 0pt, corner-radius: 2pt, fill: reader-colors.ok_soft, stroke: 0.5pt + reader-colors.border),
      node((-1.5, 2), [], width: 0.38cm, height: 0.38cm, inset: 0pt, corner-radius: 2pt, fill: reader-colors.ok_soft, stroke: 0.5pt + reader-colors.border),
      node((-0.5, 2), [], width: 0.38cm, height: 0.38cm, inset: 0pt, corner-radius: 2pt, fill: reader-colors.ok_soft, stroke: 0.5pt + reader-colors.border),
      node((0.5, 2), [], width: 0.38cm, height: 0.38cm, inset: 0pt, corner-radius: 2pt, fill: reader-colors.ok_soft, stroke: 0.5pt + reader-colors.border),
      node((1.5, 2), [], width: 0.38cm, height: 0.38cm, inset: 0pt, corner-radius: 2pt, fill: reader-colors.ok_soft, stroke: 0.5pt + reader-colors.border),
      node((-1.5, 3), [], width: 0.38cm, height: 0.38cm, inset: 0pt, corner-radius: 2pt, fill: reader-colors.ok_soft, stroke: 0.5pt + reader-colors.border),
      node((-0.5, 3), [], width: 0.38cm, height: 0.38cm, inset: 0pt, corner-radius: 2pt, fill: reader-colors.ok_soft, stroke: 0.5pt + reader-colors.border),
      node((0.5, 3), [], width: 0.38cm, height: 0.38cm, inset: 0pt, corner-radius: 2pt, fill: reader-colors.ok_soft, stroke: 0.5pt + reader-colors.border),
      node((1.5, 3), [], width: 0.38cm, height: 0.38cm, inset: 0pt, corner-radius: 2pt, fill: reader-colors.ok_soft, stroke: 0.5pt + reader-colors.border),
      // all tree edges, solid
      edge((0, 0), (-1, 1), stroke: 0.5pt + reader-colors.muted),
      edge((0, 0), (1, 1), stroke: 0.5pt + reader-colors.muted),
      edge((-1, 1), (-1.5, 2), stroke: 0.5pt + reader-colors.muted),
      edge((-1, 1), (-0.5, 2), stroke: 0.5pt + reader-colors.muted),
      edge((1, 1), (0.5, 2), stroke: 0.5pt + reader-colors.muted),
      edge((1, 1), (1.5, 2), stroke: 0.5pt + reader-colors.muted),
      edge((-1.5, 2), (-1.5, 3), stroke: 0.5pt + reader-colors.muted),
      edge((-0.5, 2), (-0.5, 3), stroke: 0.5pt + reader-colors.muted),
      edge((0.5, 2), (0.5, 3), stroke: 0.5pt + reader-colors.muted),
      edge((1.5, 2), (1.5, 3), stroke: 0.5pt + reader-colors.muted),
      // the solid proven frontier / wall at depth N
      edge((-1.95, 3.62), (1.95, 3.62), stroke: 2.6pt + reader-colors.ok),
      node((0, 4.1), text(fill: reader-colors.ok, size: 6.5pt)[depth N — proven frontier]),
      // faint unknown beyond the wall
      node((-0.7, 4.7), [], width: 0.3cm, height: 0.3cm, inset: 0pt, corner-radius: 2pt, fill: reader-colors.surface_soft.transparentize(65%), stroke: 0.4pt + reader-colors.border),
      node((0.7, 4.7), [], width: 0.3cm, height: 0.3cm, inset: 0pt, corner-radius: 2pt, fill: reader-colors.surface_soft.transparentize(65%), stroke: 0.4pt + reader-colors.border),
    ),
    // ===================== PANEL C: unbounded / unknown =====================
    diagram(
      crossing-fill: reader-colors.paper,
      node-outset: 0pt,
      spacing: (0.62cm, 0.66cm),
      // small proven cap, shaded
      node(
        enclose: ((0, 0), (-0.8, 0.9), (0.8, 0.9)),
        inset: 6pt, corner-radius: 5pt,
        stroke: (dash: "dashed", paint: reader-colors.ok, thickness: 0.7pt),
        fill: reader-colors.ok_soft.transparentize(55%),
      ),
      node((0, 0), [], width: 0.36cm, height: 0.36cm, inset: 0pt, corner-radius: 2pt, fill: reader-colors.ok_soft, stroke: 0.5pt + reader-colors.border),
      node((-0.8, 0.9), [], width: 0.36cm, height: 0.36cm, inset: 0pt, corner-radius: 2pt, fill: reader-colors.ok_soft, stroke: 0.5pt + reader-colors.border),
      node((0.8, 0.9), [], width: 0.36cm, height: 0.36cm, inset: 0pt, corner-radius: 2pt, fill: reader-colors.ok_soft, stroke: 0.5pt + reader-colors.border),
      edge((0, 0), (-0.8, 0.9), stroke: 0.5pt + reader-colors.muted),
      edge((0, 0), (0.8, 0.9), stroke: 0.5pt + reader-colors.muted),
      // the same solid wall
      edge((-1.75, 1.5), (1.75, 1.5), stroke: 2.6pt + reader-colors.ok),
      node((0, 1.86), text(fill: reader-colors.ok, size: 6.5pt)[depth N]),
      // the unbounded fan below, fading
      node((-1.2, 2.5), [], width: 0.3cm, height: 0.3cm, inset: 0pt, corner-radius: 2pt, fill: reader-colors.surface_soft.transparentize(60%), stroke: 0.4pt + reader-colors.border),
      node((0, 2.5), [], width: 0.3cm, height: 0.3cm, inset: 0pt, corner-radius: 2pt, fill: reader-colors.surface_soft.transparentize(60%), stroke: 0.4pt + reader-colors.border),
      node((1.2, 2.5), [], width: 0.3cm, height: 0.3cm, inset: 0pt, corner-radius: 2pt, fill: reader-colors.surface_soft.transparentize(60%), stroke: 0.4pt + reader-colors.border),
      node((-1.7, 3.3), [], width: 0.26cm, height: 0.26cm, inset: 0pt, corner-radius: 2pt, fill: reader-colors.surface_soft.transparentize(78%), stroke: 0.3pt + reader-colors.border),
      node((-0.55, 3.3), [], width: 0.26cm, height: 0.26cm, inset: 0pt, corner-radius: 2pt, fill: reader-colors.surface_soft.transparentize(78%), stroke: 0.3pt + reader-colors.border),
      node((0.55, 3.3), [], width: 0.26cm, height: 0.26cm, inset: 0pt, corner-radius: 2pt, fill: reader-colors.surface_soft.transparentize(78%), stroke: 0.3pt + reader-colors.border),
      node((1.7, 3.3), [], width: 0.26cm, height: 0.26cm, inset: 0pt, corner-radius: 2pt, fill: reader-colors.surface_soft.transparentize(78%), stroke: 0.3pt + reader-colors.border),
      edge((-0.8, 0.9), (-1.2, 2.5), stroke: (dash: "dotted", paint: reader-colors.muted, thickness: 0.4pt)),
      edge((0, 0), (0, 2.5), stroke: (dash: "dotted", paint: reader-colors.muted, thickness: 0.4pt)),
      edge((0.8, 0.9), (1.2, 2.5), stroke: (dash: "dotted", paint: reader-colors.muted, thickness: 0.4pt)),
      node((0, 4.0), text(fill: reader-colors.muted, size: 6.5pt)[unbounded depth —\ beyond every tool here]),
    ),
  ),
  caption: [The same state tree under three regimes. *Left* — `quint run` walks a handful of
    bold sampled threads to various depths and leaves the rest of the tree dark: it checks the
    paths it happened to walk. *Middle* — `quint verify` (Apalache) fills the _entire_ tree down
    to a solid frontier at depth N: every path of N steps or fewer, proven inside the shaded
    safe region, with the depth-N frontier drawn as a solid wall. *Right* — the honest gap: the
    tree does not stop at N. Below the wall it continues without bound — unbounded depth,
    unbounded graph, S3 and SlateDB internals — and no tool in this book reaches there. The
    trees are schematic; turbolay's recorded bound is six transitions
    (`0003-turbolay-quint-verification-evidence.md:17-19`).],
) <fig-ch7-three-regimes>

The same contrast, stated as a table, is the thing to memorize. Both tools take the identical
model and the identical `allSafety` invariant; they differ only in what they promise about the
answer.

#figure(
  table(
    columns: (0.82fr, 1.1fr, 1.1fr),
    align: (left, left, left),
    stroke: 0.5pt + reader-colors.border,
    fill: (_, row) => if row == 0 { reader-colors.surface_soft },
    inset: 7pt,
    table.header(
      text(fill: reader-colors.text)[],
      text(fill: reader-colors.text)[*`quint run` (simulation)*],
      text(fill: reader-colors.text)[*`quint verify` (Apalache)*],
    ),
    [What it explores], [a random _sample_ of behaviors], [_every_ behavior of at most N steps],
    [How], [walks `max-samples` paths, each up to `max-steps`, at random], [encodes "does a ≤ N-step violation exist?" as one formula, solved symbolically by an SMT engine],
    [A clean result means], [no counterexample in the paths it walked], [no counterexample of ≤ N steps _exists_],
    [Cost], [milliseconds; run constantly as a fast filter], [seconds to minutes; run on stable models for chosen invariants],
    [Honest name for it], [simulation evidence, not a proof], [a bounded proof — still bounded at N],
    [Recorded for turbolay], [10,000 samples × 12 steps, no violation], [6 transitions, `NoError`, all eight model families],
  ),
  caption: [`quint run` versus `quint verify` on the very same model and invariant. The tools
    are complements, not rivals: you debug a model cheaply with simulation until it is stable,
    then spend the exhaustive check on the few highest-value properties
    (`0001-quint-jepsen-testing-objective.md:29-33`). The turbolay rows are the recorded numbers
    from the verification evidence — ten-thousand-sample simulation
    (`0003-turbolay-quint-verification-evidence.md:50-51`) and a six-transition Apalache check
    that returned `NoError` for every model family (`0003-turbolay-quint-verification-evidence.md:17-30`).],
) <tab-ch7-run-vs-verify>

The layering is deliberate and stated as the objective's second evidence layer: "Apalache
through `quint verify` gives bounded exhaustive checks for the few highest-value properties once
the models have been debugged with Quint simulations"
(`0001-quint-jepsen-testing-objective.md:29-33`). Simulation is the cheap net you drag
constantly; the exhaustive check is the expensive, decisive verdict you reserve for the
properties that matter most, on models that simulation has already shaken the obvious bugs out
of.

== Running it: `quint verify`

The command that drives Apalache is `quint verify`. Its general shape, from the specification
plan's gate table, names the model, its main module, the invariants to check, and — the number
that makes it a _bounded_ check — the step bound (`0002-turbolay-quint-specification-plan.md:343`):

```bash
mise exec -- quint verify <model>.qnt --main <model> \
  --invariant <name> --max-steps <n>
```

`--max-steps <n>` is the bound N: how many transitions deep the exhaustive enumeration reaches.
`--invariant <name>` is the property proven at every state within that depth. For the write-path
model, the property is the one you already know — `allSafety`, the conjunction of all seven
safety predicates gathered at the bottom of the model (`m1_cell_write.qnt:271-278`). Apalache
does not get a new, weaker, or Apalache-special invariant; it re-checks the same `allSafety` the
simulator checked, only exhaustively to the bound instead of by sampling.

There is one wrinkle, and it is worth stating because it is the difference between the command
working and failing on the machine this was run on. On macOS the plain `quint verify` can trip
over the Java launcher stub before it ever reaches Apalache, so the recorded invocation wraps it
in an explicit runtime. This is the exact command that produced turbolay's bounded evidence for
the write path (`0003-turbolay-quint-verification-evidence.md:178-180`):

```bash
mise exec java@21.0.2 -- mise exec -- quint verify \
  quint-models/turbolay/m1_cell_write.qnt --main m1_cell_write \
  --invariant allSafety --max-steps 6
```

#custom-box(title: [Why], icon: "tip")[
  Why the doubled `mise exec` and the pinned `java@21.0.2`? Apalache is a JVM tool that Quint
  bundles and shells out to. The outer `mise exec java@21.0.2 --` provisions a real Java 21
  runtime and puts it on the path; the inner `mise exec -- quint verify ...` then runs Quint,
  which finds that Java and launches the bundled Apalache with it. The objective document spells
  out the reason: this "resolves the macOS Java launcher stub and runs the Apalache bundled by
  Quint" (`0001-quint-jepsen-testing-objective.md:161-163`). Without it, the launcher stub that
  ships with the OS intercepts the call and the check never starts. It is plumbing, not
  cryptography — but a bounded proof you cannot invoke is no proof at all.
]

A check this powerful is not free, and it does not accept just any model. A symbolic engine has
to turn the whole model into a finite logical formula, which means the model must be finite and
tame in specific ways.

#custom-box(title: [Term — Apalache-friendly model], icon: "info")[
  A model an SMT-backed checker can encode as a finite formula: finite enumerations, no
  recursion, no unbounded containers, and explicit bounds on every domain. The specification
  plan requires turbolay's Apalache targets to be exactly this — "finite enumerations, no
  recursion, no unbounded containers" (`0002-turbolay-quint-specification-plan.md:165-166`) —
  and the review checklist restates it as a gate: "Apalache targets are finite, non-recursive,
  and have explicit bounds" (`0002-turbolay-quint-specification-plan.md:381`). This is _why_ the
  write-path model was built from fourteen small variables over two writers and one edge rather
  than an unbounded graph: not to make the model cute, but to keep it inside the region a
  symbolic checker can actually decide.
]

The plan is specific about which properties earn the exhaustive treatment. It does not run
Apalache on everything; it names the load-bearing safety properties and a modest bound: "Run
bounded proofs for `edgeProjectionConsistent`, `idempotencyExact`, and `fencedWriterCannotCommit`,
first at 6 then 8 steps" (`0002-turbolay-quint-specification-plan.md:163-164`). Those three are
the write path's atomic-projection, exactly-once, and fencing guarantees — the same claims the
write-path chapter mapped action by action — and `allSafety` bundles them so a single
`quint verify` proves them together to the bound.

== Reading a bounded-proof evidence record

A bounded proof is only as trustworthy as its record. "It passed" is worthless without the
bound it passed to, the exact model checked, the constants, and the tool versions — because
every one of those changes what the green light means. The objective document makes the record
a requirement: "Every bounded-proof report records the model version, module instance,
constants, invariant list, max steps, tool versions, and result"
(`0001-quint-jepsen-testing-objective.md:156-159`), and the specification plan reinforces that
"every Apalache run records its bound" (`0002-turbolay-quint-specification-plan.md:48-49`). The
bound is not a footnote; it is half the claim.

Here is what the recorded turbolay result actually says. Apalache "bounded-checked every main
model through six transitions using `quint verify` and `mise exec java@21.0.2`; all runs
returned `NoError`" (`0003-turbolay-quint-verification-evidence.md:17-19`). Read every field of
that record deliberately, because each one bounds the claim:

#figure(
  table(
    columns: (0.9fr, 1.4fr),
    align: (left, left),
    stroke: 0.5pt + reader-colors.border,
    fill: (_, row) => if row == 0 { reader-colors.surface_soft },
    inset: 7pt,
    table.header(
      text(fill: reader-colors.text)[*Field of the record*],
      text(fill: reader-colors.text)[*What it was, and why it bounds the claim*],
    ),
    [Model / module], [each main model, e.g. `m1_cell_write`, checked on its own `--main` module — the claim is about that abstraction, no wider],
    [Invariant], [`allSafety` — the seven-predicate conjunction (`m1_cell_write.qnt:271-278`), the same one simulation checked],
    [Max steps (the bound)], [6 transitions — the claim covers every behavior of length ≤ 6, and says nothing about length 7],
    [Tool runtime], [`quint verify` driving bundled Apalache under `mise exec java@21.0.2`],
    [Result], [`NoError` — no counterexample of ≤ 6 steps exists for `allSafety`, for every one of the eight model families],
  ),
  caption: [The bounded-proof record for turbolay's models, field by field
    (`0003-turbolay-quint-verification-evidence.md:17-30`). `NoError` at six transitions is a
    real, strong result — it upgrades the write path's `allSafety` from "no violation sampled"
    to "no violation of six steps or fewer exists." Every field is load-bearing: change the
    bound, the module, or the invariant and you have proven a different, smaller thing. The
    record exists so no reader can quietly inflate a six-step check into an unbounded one.],
) <tab-ch7-evidence-record>

That is a genuine upgrade over the simulation result. For the write path, `[ok] No violation
found` across ten thousand samples became `NoError` across _every_ behavior of six steps or
fewer — the story of open, create, lose-reply, retry, crash, take over, reject the zombie, in
every order the guards permit, all of it enumerated, none of it sampled. The one-in-a-million
interleaving that a random walk could miss is, at six steps, no longer possible to miss.

== The wall is still a wall

Now the honesty, because this is the chapter where it matters most, and the book's voice is to
say the limit plainly and more than once. A bounded exhaustive check is stronger than
simulation. It is still bounded. `NoError` at six transitions proves that no behavior of _six
steps or fewer_ breaks `allSafety`. It does not prove that no behavior of seven steps does. The
solid wall in the figure is exactly six deep, and everything below it — the whole unbounded
continuation of the tree — is untouched by this result.

The evidence document states the ceiling in the same words the figure draws: "A six-step
Apalache result means that all transitions in the stated, small abstraction passed up to that
bound. It does not prove arbitrary graph sizes, arbitrary S3 failure behavior, or the Rust
implementation" (`0003-turbolay-quint-verification-evidence.md:110-115`). Three separate limits
live in that sentence, and they compound with the abstraction the write-path chapter already
committed to:

- *Bounded depth.* The proof reaches six steps, not all steps. A protocol bug that first
  appears on the seventh transition is outside this result. Raising the bound to eight (as the
  plan's Apalache targets propose) pushes the wall deeper; it never removes it.
- *Bounded breadth.* The model is two writers, one edge, a handful of idempotency keys — the
  finite domains the shared-state abstraction fixed on purpose. `NoError` says nothing about a
  million-edge graph; it says the _protocol logic_ holds on the small domain that exposes the
  interesting interleavings.
- *Bounded scope.* The whole exercise lives inside the shared-state abstraction, where "a
  durable SlateDB transaction is one atomic state update"
  (`0001-quint-jepsen-testing-objective.md:67-69`). It models graph facts and epochs, not byte
  encoding, LSM compaction, S3's implementation, Rust scheduling, or Kubernetes internals. Those
  are real, and they are simply not what a symbolic check of this model can reach.

#custom-box(title: [Why], icon: "tip")[
  Why keep saying "still bounded" when the result is genuinely strong? Because the most
  dangerous thing a green check can do is invite a claim larger than it earned. A reader who
  hears "Apalache proved it" and drops the qualifier will believe turbolay is proven correct for
  every graph, every failure, and every depth — none of which this establishes. The precise
  claim is smaller and more useful: for the fixed small domain, the highest-value safety
  invariants have _no counterexample of six steps or fewer_. Stated that exactly, it is a result
  you can stand behind. Inflated, it is a liability. The evidence record with its explicit bound
  exists precisely so the claim cannot be quietly inflated.
]

So the honest summary is this. `quint run` samples behaviors and finds bugs cheaply; `quint
verify` enumerates every behavior up to a stated bound and turns "not found" into "does not
exist within N." Both operate on the same models and the same invariants, and neither reaches
past the shared-state abstraction to the real bytes, the real store, or an unbounded graph. That
frontier is where the last two layers of the evidence pipeline take over — and they are the
subject of the chapters that close this book.

The first is the model-based-testing chapter, where a Rust harness replays each model's
generated action traces against a live turbolay `GraphShard`, calling the real public kernel API
and comparing the actual graph projection to the model's state after every action — the layer
that binds this checked abstraction to the code that must refine it "for a bounded domain"
(`0001-quint-jepsen-testing-objective.md:34-37`). The second is the closing chapter on the whole
assurance stack, where Jepsen drives the deployed service against real processes and an
S3-compatible store, injecting the process, network, and ownership faults "that a model cannot
reproduce" (`0001-quint-jepsen-testing-objective.md:38-40`). Apalache proved the protocol to a
bound; those layers are how the bound gets pushed outward into the running system.
