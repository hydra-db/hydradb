#import "../vendor/bookly/src/bookly.typ": *
#import "@preview/fletcher:0.5.7" as fletcher: diagram, node, edge

// LEARNING GOAL: leave the reader knowing exactly what "verified" means for
// turbolay — the layered assurance stack, what each layer catches, what it
// costs, where its guarantee stops, and that it establishes a precise safety
// contract for turbolay's own protocol, not a proof of S3/SlateDB/Kubernetes
// or an unbounded graph. Jepsen is named as the honest, deferred next layer.
// GROUND IN: docs/formal-methods/0001-quint-jepsen-testing-objective.md
// (layered list lines 29-44, non-goals 67-80, Apalache JRE gate line 224);
// docs/formal-methods/0005-next-steps-and-completion-gates.md;
// docs/formal-methods/0006-minio-mbt-handoff.md (MBT status + evidence boundary).

= The assurance stack: what "verified" does and does not mean

We started this book with a small, ordinary disaster: a write that committed and
whose acknowledgement was then lost, leaving a caller who could not tell whether its
change had landed. From there the chapters added one instrument at a time. The
correctness-problem chapter argued that examples alone can never close such a gap.
The two Quint-from-zero and first-model chapters taught you to read a state machine
and to see the write path as a handful of projections that must commit together. The
invariants-and-buggy-twin chapter turned those projections into safety predicates and
watched a deliberately broken model surrender a counterexample. The
deterministic-scenarios chapter pinned single storylines with `run`, `then`, and
`expect`. The model-gallery chapter spread the same discipline across all eight model
families. The bounded-proof chapter handed the highest-value properties to Apalache.
And the model-based-testing chapter replayed generated traces against the real kernel,
first on an in-memory store and then on a local S3-compatible one.

Eight instruments. This closing chapter sets them side by side, because the moment
someone says turbolay is "verified" the word does almost no work on its own. Verified
_against what_? To _what depth_? And — the question this whole book has been quietly
building toward — _where does the guarantee stop_? The honest answer is not a single
proof. It is a stack of layers, each catching a different class of error at a different
cost, and each with an edge past which it says nothing at all.

#custom-box(title: [Term — Assurance stack], icon: "info")[
  A deliberately layered set of independent techniques, ordered so that each one covers
  the blind spot of the one below it. Lower layers are cheaper and reason exhaustively
  over a small abstract model of the protocol; higher layers are costlier and exercise
  the real system over a few concrete histories. No single layer is complete on its
  own. What the stack establishes is not one guarantee but a _map_: for each class of
  error, which layer catches it, and where the checked region ends.
]

== Four instruments over one protocol

The formal-methods objective states the stack in four numbered points and is explicit
that the layering is a choice, not an accident,
`0001-quint-jepsen-testing-objective.md:29-44`. Quint makes the intended transitions
and safety properties small, explicit, reviewable, and executable. Apalache, reached
through `quint verify`, gives bounded exhaustive checks for the few highest-value
properties once a model has been debugged with simulation. Rust model-based testing
replays generated action traces against turbolay's public kernel API, showing the real
implementation refines the checked model over a bounded domain. Jepsen — the layer
this book defers — would execute the public service against real processes and an
S3-compatible store, injecting the process, network, and ownership faults a model
cannot reproduce.

Under those four sits a base the same objective assigns explicitly: the concerns the
Quint models _exclude on purpose_, each handed to the technique that suits it better,
`0001-quint-jepsen-testing-objective.md:74-80`. Key encoding, codecs, and malformed
input go to Rust unit and property tests. SlateDB's atomic commit and writer fencing
are an upstream contract exercised by integration tests. Real S3 latency and
throttling go to MinIO fault tests and an eventual soak. These are not the protocol's
job to prove; they are the ground the protocol model stands on. Draw all five as a
stack and the shape of the whole effort appears at once.

#figure(
  stack(dir: ttb, spacing: 10pt,
  diagram(
    crossing-fill: reader-colors.paper,
    node-stroke: 0.7pt + reader-colors.border,
    node-outset: 0pt,
    spacing: (1cm, 0.55cm),
    // Outer boundary: a dashed muted enclosure marking the edge of what the stack checks.
    node(
      enclose: ((0, 0), (0, 1), (0, 2), (0, 3), (0, 4)),
      inset: 12pt,
      corner-radius: 9pt,
      stroke: (dash: "dashed", paint: reader-colors.muted, thickness: 0.8pt),
      fill: none,
    ),
    // The five bands, top (closest to reality) to bottom (abstract, exhaustive).
    node((0, 0), align(left, block(width: 100%)[
      #text(fill: reader-colors.text, weight: "bold", size: 9.5pt)[Jepsen — real cluster under faults #text(size: 7.5pt, fill: reader-colors.muted)[(FUTURE — not yet run)]]
      #v(2pt)
      #text(fill: reader-colors.text, size: 8pt)[Checks: safety on a live cluster under process, network, and ownership faults.]
      #v(1.5pt)
      #text(fill: reader-colors.muted, size: 7.5pt)[Stops at: — deferred; no operational histories exist yet.]
    ]), fill: reader-colors.muted.transparentize(85%), stroke: (dash: "dashed", paint: reader-colors.muted, thickness: 0.9pt), width: 11.5cm, inset: 9pt),
    node((0, 1), align(left, block(width: 100%)[
      #text(fill: reader-colors.text, weight: "bold", size: 9.5pt)[Rust model-based testing — real kernel refines the model]
      #v(2pt)
      #text(fill: reader-colors.text, size: 8pt)[Checks: the live kernel's observed projection matches the model after every replayed action.]
      #v(1.5pt)
      #text(fill: reader-colors.muted, size: 7.5pt)[Stops at: a finite seeded corpus, no injected faults; local MinIO is not production S3.]
    ]), fill: reader-colors.ok_soft, width: 11.5cm, inset: 9pt),
    node((0, 2), align(left, block(width: 100%)[
      #text(fill: reader-colors.text, weight: "bold", size: 9.5pt)[Apalache bounded proof (`quint verify`)]
      #v(2pt)
      #text(fill: reader-colors.text, size: 8pt)[Checks: no safety violation on _any_ behavior up to a small step bound.]
      #v(1.5pt)
      #text(fill: reader-colors.muted, size: 7.5pt)[Stops at: the step bound and finite constants; silent beyond N steps.]
    ]), fill: reader-colors.ok_soft, width: 11.5cm, inset: 9pt),
    node((0, 3), align(left, block(width: 100%)[
      #text(fill: reader-colors.text, weight: "bold", size: 9.5pt)[Quint simulation (`quint run`)]
      #v(2pt)
      #text(fill: reader-colors.text, size: 8pt)[Checks: the named invariants along many _sampled_ interleavings; witnesses are reached.]
      #v(1.5pt)
      #text(fill: reader-colors.muted, size: 7.5pt)[Stops at: sampling, not exhaustion — a rare interleaving can go unvisited.]
    ]), fill: reader-colors.info_soft, width: 11.5cm, inset: 9pt),
    node((0, 4), align(left, block(width: 100%)[
      #text(fill: reader-colors.text, weight: "bold", size: 9.5pt)[Property & integration tests — the excluded concerns]
      #v(2pt)
      #text(fill: reader-colors.text, size: 8pt)[Checks: codecs and encoding, SlateDB's commit/fence contract, real S3 latency.]
      #v(1.5pt)
      #text(fill: reader-colors.muted, size: 7.5pt)[Stops at: single components — says nothing about protocol-level interleavings.]
    ]), fill: reader-colors.surface_soft, width: 11.5cm, inset: 9pt),
    // The axis of the stack.
    edge((-6.7, 4.3), (-6.7, -0.3), "->", stroke: reader-colors.muted),
    node((-6.25, 2), rotate(-90deg, reflow: true, text(fill: reader-colors.muted, size: 8pt)[realism · cost · fault coverage increase upward]), stroke: none, fill: none, inset: 0pt),
  ),
  // Beyond the boundary: what no layer of the stack claims.
  align(center, text(fill: reader-colors.muted, size: 8pt, style: "italic")[Beyond this boundary — not modeled, not proven: an unbounded graph, unbounded\ time, and the internals of S3, SlateDB, and Kubernetes.]),
  ),
  caption: [The assurance stack, bottom to top. The base layer checks the concerns the Quint models deliberately exclude, each with its own tool. Above it, three layers reason about turbolay's protocol: simulation _samples_ interleavings, Apalache proves them _exhaustive to a small bound_, and model-based testing shows the _real kernel_ refines the checked model over a bounded corpus. The top band, Jepsen, is dashed because it is future work — no operational histories exist yet. Moving up the stack means moving closer to reality at higher cost and broader fault coverage; moving down means cheaper, more exhaustive reasoning over a smaller abstraction. The dashed enclosure is the edge of what is checked at all; the caption beneath it names what lies outside every layer.],
) <fig-ch9-stack>

#custom-box(title: [Why], icon: "tip")[
  Why layer at all, instead of pushing one technique as far as it will go? Because each
  technique's strength is exactly another's blind spot, and the trade is unavoidable.
  Exhaustive checking is only possible over a _small abstract_ state space — the moment
  you model real bytes, real threads, and real object storage, the space explodes and
  nothing can walk it. Running the _real system_ is only possible over a _handful of
  concrete histories_ — you cannot execute every interleaving on hardware. So the low
  layers buy exhaustiveness by abstracting reality away, and the high layers buy realism
  by giving up exhaustiveness. Neither is a defect; stacked, each layer's guarantee
  starts where the layer below it went blind.
]

== Reading the stack, layer by layer

Follow the stack upward and watch the guarantee change character at every step. The
base layer is where the excluded concerns live, and each is genuinely tested — just not
here, and not by a state machine. The Quint-simulation layer takes the small protocol
model and walks _sampled_ paths through it: `quint run` fires the nondeterministic
`step` from `init` over and over, checking every safety invariant on each visited state
and confirming the reachability witnesses fire. The objective is careful with the word
for what this earns you: "No counterexample in a sampled simulation is reported as
simulation evidence, not a proof," `0001-quint-jepsen-testing-objective.md:152-153`.
Sampling can miss a rare interleaving; it is fast, and it is where models are debugged
before anything more expensive touches them.

The Apalache layer closes that particular gap. Reached through `quint verify`, it asks
whether _any_ behavior up to a fixed number of steps can violate a property, and
answers by construction rather than by sampling. Where simulation walks some paths, a
bounded proof walks all of them — up to the bound. The catch is in that clause: the
guarantee holds to a stated small step count and finite constants, and says nothing
beyond it. This is exactly why the completion criteria record the Apalache results as
_bounded checks_ contingent on a working runtime, "The selected Apalache proofs complete
after a working JRE is available, or an explicit environment block and reproducible
command/output are recorded," `0001-quint-jepsen-testing-objective.md:224`. A bounded
proof is a wall across every path of length N — not a proof for all time.

The Rust model-based-testing layer changes the subject from the model to the code. It
does not check the model against itself; it checks the _implementation_ against the
model.

#custom-box(title: [Term — Refinement], icon: "info")[
  The relation that makes a model-based test meaningful. The real implementation
  _refines_ a model when every behavior the implementation exhibits corresponds to some
  behavior the model allows — the implementation may do more (richer records, real
  indexes, extra internal steps), but it must never do something the model forbids. When
  refinement holds, a safety property proved of the small model constrains the large
  program: the code cannot reach a bad state the model ruled out, because every one of
  its behaviors maps to a good one in the model.
]

Concretely, the adapter decodes a generated action trace, maps each named Quint action
to a real kernel call, and after every step compares a normalized projection read back
through the public API against the model's projection. When they match across the whole
corpus, the code refined the model on those traces. The current evidence is real but
bounded in a precise way: all six default adapters pass against both the in-memory store
and a pinned local MinIO, `0006-minio-mbt-handoff.md:38-41`, and the handoff states the
limit plainly — "A passing local MinIO replay proves only this finite corpus against
that pinned S3-compatible image and configuration. It does not prove arbitrary S3
provider behavior, S3 outage handling, performance, CI execution, or Jepsen
process-level fault tolerance," `0006-minio-mbt-handoff.md:45-47`. A finite corpus,
one process, no injected faults, and a local store that is not yet production S3.

The top layer, Jepsen, is where the injected faults would live: kill and restart writers
mid-mutation, partition clients from nodes, overlap an old and a new owner, throttle the
object store. It is the only layer that would produce histories of the deployed
architecture under the failures a model cannot reproduce. It is also the layer this book
does not claim: the objective sequences it last, "after the per-cell model and local MBT
harness are trusted," `0001-quint-jepsen-testing-objective.md:188-189`, and the current
handoffs list its baselines as pending work, `0006-minio-mbt-handoff.md:85-87`. The
honest status of the stack today is four layers standing and one designed but not yet
built.

#figure(
  table(
    columns: (0.95fr, 1.5fr, 1.45fr, 1.45fr, 0.85fr),
    align: (left, left, left, left, left),
    stroke: 0.5pt + reader-colors.border,
    fill: (_, row) => if row == 0 { reader-colors.surface_soft },
    inset: 6pt,
    table.header(
      text(fill: reader-colors.text, size: 8pt)[*Layer*],
      text(fill: reader-colors.text, size: 8pt)[*What it catches*],
      text(fill: reader-colors.text, size: 8pt)[*Guarantee*],
      text(fill: reader-colors.text, size: 8pt)[*Boundary*],
      text(fill: reader-colors.text, size: 8pt)[*Status*],
    ),
    text(size: 8pt)[Property & integration tests], text(size: 8pt)[Codec, encoding, and malformed-input bugs; SlateDB commit/fence; real S3 latency], text(size: 8pt)[Each excluded concern is checked by its most fitting technique], text(size: 8pt)[Single components; assumes SlateDB commits atomically], text(size: 8pt)[Established],
    text(size: 8pt)[Quint simulation], text(size: 8pt)[Invariant violations along sampled protocol interleavings], text(size: 8pt)[No counterexample in the sampled behaviors; witnesses reached], text(size: 8pt)[Sampling, not exhaustion; finite domain], text(size: 8pt)[Done],
    text(size: 8pt)[Apalache bounded proof], text(size: 8pt)[Any safety violation on any path within the step bound], text(size: 8pt)[No violation exists on _any_ behavior up to N steps], text(size: 8pt)[Bounded depth and constants; silent beyond N], text(size: 8pt)[Done (bounded)],
    text(size: 8pt)[Rust model-based testing], text(size: 8pt)[Divergence between the model's projection and the real kernel's state], text(size: 8pt)[The implementation refines the model on the replayed corpus], text(size: 8pt)[Finite corpus, no faults; local MinIO is not production S3], text(size: 8pt)[Done (InMemory + MinIO); prod-S3 pending],
    text(size: 8pt)[Jepsen], text(size: 8pt)[Safety under real process, network, and ownership faults], text(size: 8pt)[Operational histories the deployed system produces under faults], text(size: 8pt)[— not yet run], text(size: 8pt)[Future],
  ),
  caption: [The same five layers as a contract: what each catches, the guarantee it earns, where that guarantee stops, and its status today. Read the _Boundary_ column as the real content — it is the list of things a reader must not assume the word "verified" covers.],
) <tab-ch9-layers>

== One state space, four lenses

The book has carried a single picture since the write-path chapter: the model as the
tree of states reachable from `init`, with a shaded region where the safety invariant
holds and a forbidden state outside it that no transition reaches. Each layer of the
stack is a different way of looking at _that same picture_. Simulation walks a bold
sampled thread through the region. Apalache fills the region in, proving no path of
length N escapes it. Model-based testing re-walks a thread with the real kernel instead
of the model, checking that the code stays inside the same region. And Jepsen would draw
an outer world around the whole thing — the real cluster, with faults that live outside
any state the model can express.

#figure(
  diagram(
    crossing-fill: reader-colors.paper,
    node-stroke: 0.6pt + reader-colors.border,
    node-outset: 0pt,
    spacing: (2.3cm, 1.35cm),
    // Jepsen's outer world: the real cluster and faults, outside any modeled state.
    node(
      enclose: ((-0.35, -0.55), (0, 0), (0, 1), (0, 2), (1, 1), (1.55, 1)),
      inset: 16pt,
      corner-radius: 12pt,
      stroke: (dash: "dashed", paint: reader-colors.muted, thickness: 0.9pt),
      fill: reader-colors.muted.transparentize(90%),
    ),
    node((1.15, -0.5), text(fill: reader-colors.muted, size: 7.5pt, style: "italic")[Jepsen (future): real cluster + faults], stroke: none, fill: none),
    // Apalache's proven region: every path up to the bound stays inside.
    node(
      enclose: ((0, 0), (0, 1), (0, 2), (1, 1)),
      inset: 12pt,
      corner-radius: 8pt,
      stroke: (dash: "dashed", paint: reader-colors.ok, thickness: 0.8pt),
      fill: reader-colors.ok_soft.transparentize(60%),
    ),
    // Protocol states.
    node((0, 0), text(fill: reader-colors.text, size: 8pt)[`init`], fill: reader-colors.surface_soft, width: 2cm),
    node((0, 1), text(fill: reader-colors.text, size: 8pt)[edge\ committed], fill: reader-colors.info_soft, width: 2cm),
    node((1, 1), text(fill: reader-colors.text, size: 8pt)[reply\ lost], fill: reader-colors.warn_soft, width: 2cm),
    node((0, 2), text(fill: reader-colors.text, size: 8pt)[zombie\ rejected], fill: reader-colors.ok_soft, width: 2cm),
    // Simulation / MBT bold thread through the region.
    edge((0, 0), (0, 1), "->", stroke: 1.3pt + reader-colors.info),
    edge((0, 1), (0, 2), "->", stroke: 1.3pt + reader-colors.info),
    // A sampled fork simulation also walks.
    edge((0, 1), (1, 1), "->", stroke: (thickness: 0.6pt, paint: reader-colors.muted, dash: "dotted")),
    // Labels for the lenses.
    node((-1.15, 1), text(fill: reader-colors.info, size: 7.5pt)[simulation\ + MBT\ walk this\ thread], stroke: none, fill: none),
    node((0.62, 2.02), text(fill: reader-colors.ok, size: 7.5pt)[Apalache: no path ≤ N escapes], stroke: none, fill: none),
  ),
  caption: [The book's state-space picture, seen through each layer at once. The bold thread is what simulation samples and what model-based testing re-walks with the real kernel. The green dashed region is what Apalache proves: no behavior up to N steps leaves it. The outer grey world is Jepsen's — the real cluster under faults, which lives outside every state the model can name, and which the stack does not yet reach.],
) <fig-ch9-lenses>

== The outer boundary

Every layer has an edge, and the objective states the outermost one in a single sentence
that this book has been earning the right to quote. The abstraction that made all of it
possible — treating a durable SlateDB transaction as one atomic state update — is also
its boundary: the models "do not model byte encoding, LSM compaction, S3's
implementation, Rust scheduling, TLS, parsing, or Kubernetes internals,"
`0001-quint-jepsen-testing-objective.md:67-69`. Nothing in the stack proves S3 is
correct, that SlateDB commits are truly atomic, that Kubernetes schedules the way the
model assumes, or that a graph of unbounded size over unbounded time behaves like the
small finite one that was checked. Those are assumptions the stack _rests on_, tested
where they can be tested, and taken as contracts where they cannot.

That is not a weakness to apologize for; it is the whole reason the guarantee is worth
anything. A claim that named no boundary would be untrustworthy precisely because it
claimed too much. The objective draws the line in one breath,
`0001-quint-jepsen-testing-objective.md:41-44`:

#boxeq[
  Not a proof of S3, SlateDB, Kubernetes, or an unbounded graph — but a precise safety
  contract for turbolay's own protocol, tests that can find violations of that contract,
  and operational histories that exercise it in the deployed architecture.
]

Read that as three things, each supplied by part of the stack. The _precise safety
contract_ is the Quint models and their invariants, proven exhaustive to a bound by
Apalache. The _tests that find violations_ are the deterministic scenarios and the
model-based replay against the real kernel. The _operational histories_ are Jepsen's —
the part still to come.

== The honest bottom line

So when someone says turbolay is "verified," here is the sentence that survives scrutiny.
Turbolay's per-cell protocol — atomic mutation, strictly increasing epochs, writer
fencing, exactly-once retries, snapshot-consistent reads, artifact equivalence, and
GC safety — is written as a small, reviewable state machine; its highest-value safety
properties are checked exhaustively to a small step bound; and the real kernel has been
shown to refine that machine over a finite trace corpus, on an in-memory store and on a
pinned local S3-compatible one. That is a strong, specific claim, and it is bounded on
every side: sampled where it is not proven, proven only to a depth, refined only over a
corpus, and resting on components it assumes rather than establishes.

What remains is the layer that trades the last of the abstraction for the last of the
realism. Jepsen would take this same protocol vocabulary to a live three-node cluster
against a real object store and inject the faults a model cannot reproduce — killing
writers mid-batch, partitioning clients, overlapping owners, throttling storage — and
keep the histories, the digests, and a fresh-reader verification at the end. Its
baselines are designed and pending, not run. When they run, every minimized failure that
can be expressed in the bounded model becomes a new Quint scenario, and the lowest layer
of the stack grows to catch it forever after. That is the shape of the thing this book
set out to build: not a single proof that ends the conversation, but a layered set of
instruments honest about where each one stops — and a clear, standing invitation for the
next fault to teach the model something it did not yet know.
