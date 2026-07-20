#import "../vendor/bookly/src/bookly.typ": *
#import "@preview/fletcher:0.5.7" as fletcher: diagram, node, edge

= The correctness problem

Everything up to here has described what turbolay does: how a graph becomes keys, how a write
fences out rival writers, how a read pins an epoch, how deltas fold into matrix artifacts. This
chapter asks a harder question. How do we know any of it is actually correct? Not correct on the
happy path, where one client writes and one client reads and nothing fails, but correct when the
network drops an acknowledgement, when a writer is killed mid-transaction, when a second process
takes over a cell it believes is orphaned, and when a client retries an operation whose outcome it
never learned. turbolay keeps its only durable copy on a remote object store and allows exactly one
writer per cell over that shared, remote storage. Those two facts make correctness under failure the
whole game, and they make it very hard to test.

This part of the book is about the machinery we built to answer the question honestly. It is a
standalone story: you do not need to have read every earlier chapter in detail, only to hold the
mental model that turbolay is a single-writer, S3-backed graph engine that must survive crashes,
retries, and ownership takeover. Over the next chapters we teach a small formal language called
Quint, use it to write down what turbolay is supposed to do, and then test the real engine against
that written-down intent. This first chapter does none of that. It only argues why the ordinary
tools, the unit test and the integration test, run out before they reach the interesting cases, and
what a layered approach buys instead. We also draw the boundary carefully, because the most
dishonest thing a verification effort can do is claim more than it proved.

== A write that may or may not have happened

Start with a concrete story, because the whole problem is visible in a single unlucky exchange.

A client asks turbolay to create one edge. The engine does everything right. It proves it is the
legitimate writer for the cell, opens a serializable transaction, advances the epoch, writes the
canonical edge record and every index and degree key that edge implies, and commits the transaction
to the object store. The bytes are durable. The edge exists. Then, on the way back to the client,
the acknowledgement is lost: a connection resets, a load balancer times out, the client's process is
paused just long enough for its own deadline to fire. The write succeeded. The client does not know
it succeeded.

Now put yourself in the client's position. You issued a mutation and heard nothing. Did it happen?
You cannot tell from where you stand, so you do the only sensible thing: you retry. You send the
same create again. And here is the hazard. If the engine treats your retry as a brand-new request,
you now have two edges where you meant one, or two degree increments, or two topology deltas feeding
the background artifact builder. A silent, dropped network packet has corrupted the graph. The
correct behavior is that the retry, carrying the same idempotency key, returns the original result
and applies nothing further, so that the ambiguous outcome collapses back to exactly one edge no
matter how many times the anxious client asks again.

#figure(
  diagram(
    crossing-fill: reader-colors.paper,
    spacing: (3.0cm, 0.85cm),
    node-stroke: none,
    node((0, 0), text(weight: "bold", fill: reader-colors.text)[Client]),
    node((1, 0), text(weight: "bold", fill: reader-colors.text)[turbolay]),
    edge((0, 0), (0, 6), stroke: (dash: "dotted", paint: reader-colors.border)),
    edge((1, 0), (1, 6), stroke: (dash: "dotted", paint: reader-colors.border)),
    edge((0, 1), (1, 1), "->", text(fill: reader-colors.muted, size: 8pt)[create edge, key `K`], stroke: reader-colors.muted),
    node((1, 2), text(fill: reader-colors.text, size: 8pt)[commit durable · epoch += 1], fill: reader-colors.ok_soft, stroke: 0.5pt + reader-colors.border, inset: 5pt),
    edge((1, 3), (0, 3), "-->", text(fill: reader-colors.bad, size: 8pt)[acknowledgement lost], stroke: (dash: "dashed", paint: reader-colors.bad)),
    edge((0, 4), (1, 4), "->", text(fill: reader-colors.muted, size: 8pt)[retry, same key `K`], stroke: reader-colors.muted),
    node((1, 5), text(fill: reader-colors.text, size: 8pt)[recognise `K` · write nothing], fill: reader-colors.warn_soft, stroke: 0.5pt + reader-colors.border, inset: 5pt),
    edge((1, 6), (0, 6), "->", text(fill: reader-colors.muted, size: 8pt)[original result — one edge], stroke: 0.7pt + reader-colors.ok),
  ),
  caption: [The unlucky exchange. The write is durable, but its acknowledgement never reaches the client. A correct retry, carrying the same idempotency key `K`, returns the recorded outcome and writes nothing, so the ambiguous result collapses to exactly one edge. The task of this part is to gain confidence that turbolay behaves like this in every ordering, not just this one.],
) <fig-lost-reply>

#custom-box(title: [Why], icon: "tip")[
  This exact hazard is a first-class property in the models, listed as "ambiguous-result recovery
  and idempotency": if a transaction commits but its response is lost, retrying the same idempotency
  key must yield the same logical result without a duplicate edge, degree increment, or delta. It is
  not an edge case bolted on afterward. It is one of the correctness claims the whole verification
  effort exists to check.
]

So the design is supposed to handle this. The question this chapter is really about is not whether
the design is right, but how you would ever gain confidence that it is. Sit with what it would take
to write a normal test for the story above. You would need to drive a real write to the exact point
where the transaction has committed but the reply has not yet been delivered, freeze the world
there, inject precisely the failure that loses the acknowledgement, then resume the client, have it
time out, have it retry with the same key, and finally assert that the graph contains one edge and
not two. That is already a delicate test to write, and it captures a single interleaving. The commit
could instead be lost before it is durable, in which case the retry must actually perform the write.
The takeover could happen in the same window, so that a different process fields the retry. The retry
could race a concurrent delete of the same edge. Each of these is a different ordering of the same
handful of events, and each demands its own correct answer.

The trouble is that there are not a handful of orderings. There are thousands, and most of them are
states an ordinary test can never even reach, because a test drives the system from the outside and
cannot pause it between two internal steps to slip a fault into the gap.

== Why examples run out

An example test proves one thing: that on the specific inputs and the specific timing the test
happened to produce, the system did the right thing. That is genuinely valuable, and turbolay has
many such tests. But the space we need to cover here is not a list of inputs. It is a space of
_interleavings_: every way that concurrent operations and failures can be ordered relative to each
other. And that space is combinatorial.

Consider only a few moving parts, each with a few choices. A write can be in one of several
stages: validated, permitted, committed-but-unacknowledged, acknowledged. A failure can strike, or
not, at each stage: the process is killed, the store call times out, a takeover fences the writer, the
client's deadline fires. A retry can arrive early or late. A concurrent operation, a delete, a read, a
second write, can be interleaved at any point. Multiply these together and the number of distinct
scenarios does not grow by addition; it explodes. Ten independent binary choices are already a
thousand orderings. The real system has far more than ten. No engineer can sit down and enumerate
them as example tests, and no test suite you could run in reasonable time would sample enough of them
to be convincing. The interesting bugs live in the orderings nobody thought to write down.

There is a standard move out of this trap, and it is old and well proven. Instead of trying to
enumerate the scenarios, you write down, once and precisely, what the system is _supposed_ to do, in
a form small enough that a tool can explore the scenarios for you. This is the domain of formal
methods.

#custom-box(title: [Term — Formal methods], icon: "info")[
  The practice of describing a system, and the properties it must satisfy, in a precise mathematical
  language, so that the description can be analyzed by a tool rather than only read by a human.
  Because the description is exact and executable, a machine can search through system behaviors
  looking for one that breaks a stated property, something a prose specification and a reviewer's
  attention cannot do.
]

The heart of the technique is to stop thinking about the code and start thinking about a _model_ of
it.

#custom-box(title: [Term — Model], icon: "info")[
  A small, deliberately simplified description of a system as a state machine: a notion of what
  counts as the system's state, a set of named actions that carry it from one state to the next, and
  a starting state. The model is not the code. It keeps the behavior that matters for the property
  under study, an edge either exists or does not, an epoch either advanced or did not, a writer is
  either fenced or current, and throws away everything else, such as byte layouts and thread
  scheduling. A good model is small enough to reason about exhaustively and faithful enough that its
  conclusions still say something true about the real system.
]

A model on its own only describes behavior. To catch bugs you also have to state, separately and
just as precisely, what must never go wrong.

#custom-box(title: [Term — Safety property], icon: "info")[
  A claim that something bad never happens: at every reachable state, some condition holds. "The
  graph never contains two edges from one committed create," "a fenced writer never commits," "a
  historical read never mixes state from two storage sequences." A safety property is violated the
  moment you can exhibit a single reachable state where the condition is false, which is exactly the
  kind of witness a tool can hunt for.
]

#custom-box(title: [Term — Invariant], icon: "info")[
  A safety property phrased as a predicate on a single state that must hold for _every_ state the
  model can reach. Checking a model reduces to a mechanical question: starting from the initial
  state and applying actions in every possible order, can the model ever reach a state where the
  invariant is false? If yes, the tool hands you the exact sequence of actions that gets there,
  which is the counterexample you could never have written by hand.
]

This reframes the correctness problem into something a machine can attack. Rather than asking a
human to imagine the thousand interleavings, you write a small state machine that captures turbolay's
intended protocol, you write the invariants it must preserve, and you let a tool walk the reachable
states looking for a violation. When it finds one, it does not merely say "a bug exists"; it prints
the precise ordering of actions that triggers it, which you can then turn into a permanent, concrete
regression test. The dropped-acknowledgement story stops being a scenario you hope you remembered to
test and becomes a state the model reaches on its own.

== Layers of evidence

A model buys precision, but a model can also lie. It can be internally consistent and still fail to
describe the real turbolay, either because it is too abstract to be meaningful or because the code
drifted away from it. And even a faithful, thoroughly checked model says nothing about how three real
processes behave when you kill one and partition another from its object store. No single technique
covers the whole distance from "the intended protocol is self-consistent" to "the deployed system
survives faults." So the design does not rely on one. It stacks four layers, each catching a class of
error the layer below cannot.

#figure(
  diagram(
    crossing-fill: reader-colors.paper,
    node-stroke: 0.6pt + reader-colors.border,
    node-fill: reader-colors.info_soft,
    spacing: (0pt, 0.7cm),
    node((0, 0), text(fill: reader-colors.text)[*Quint model*\ small, explicit, executable state machine and its safety properties], width: 12.5cm),
    edge((0, 0), (0, 1), "->", text(fill: reader-colors.muted)[debugged by simulation], stroke: reader-colors.muted),
    node((0, 1), text(fill: reader-colors.text)[*Apalache, via `quint verify`*\ bounded exhaustive check of the highest-value properties], width: 12.5cm),
    edge((0, 1), (0, 2), "->", text(fill: reader-colors.muted)[action traces (ITF)], stroke: reader-colors.muted),
    node((0, 2), text(fill: reader-colors.text)[*Rust model-based testing*\ replay traces against turbolay's public API; the real code must refine the model], fill: reader-colors.ok_soft, width: 12.5cm),
    edge((0, 2), (0, 3), "-->", stroke: (dash: "dashed", paint: reader-colors.muted), text(fill: reader-colors.muted)[deferred, out of scope here]),
    node((0, 3), text(fill: reader-colors.muted)[_Jepsen_\ real processes and an S3-compatible store under injected faults], stroke: (dash: "dashed", paint: reader-colors.muted), fill: none, width: 12.5cm),
  ),
  caption: [The layered-evidence design from the testing objective. Each layer establishes something the layer above it cannot. This part of the book pursues the first three; Jepsen is named here as the intended fourth layer and is out of scope for these chapters.],
) <fig-layers-of-evidence>

Read the layers from the top down, because that is the order the evidence is built in.

The first layer is the _Quint model_ itself. Quint is a language for writing exactly the kind of
small, explicit state machine defined above: named actions, an explicit notion of state, and named
safety properties, all in a form you can execute. Its immediate value is not proof but clarity. To
write the model you are forced to make turbolay's intended transitions concrete and reviewable, small
enough that a human can hold the whole protocol in their head and a maintainer can argue about
whether a given rule is what the system should really do. Quint can also _simulate_ the model, running
many randomized sequences of actions and watching whether any of them trips an invariant. Simulation
is cheap and finds shallow bugs quickly, but it is sampling. A simulation that finds no violation is
evidence the model is not obviously broken; it is not a proof that no violation exists.

For that you climb to the second layer, _bounded model checking_.

#custom-box(title: [Term — Bounded model checking], icon: "info")[
  Exhaustively checking every behavior of a model up to a fixed limit, typically a maximum number of
  actions, or steps. Unlike simulation, which samples paths at random, bounded checking examines all
  of them within the bound. If a safety property can be violated in that many steps, the checker is
  guaranteed to find it; if it reports no violation, you know the property holds for every behavior
  up to the bound, and nothing beyond it. turbolay reaches this layer through Quint's `verify`
  command, which hands the model to a checker called Apalache.
]

Bounded checking turns "we tried a lot of orderings" into "we tried _all_ orderings, up to this many
steps." That is a real strengthening, and it is where the sharpest confidence in the protocol comes
from. But notice the words "up to this many steps." The exhaustiveness is real and it is bounded, and
we will be very careful in a moment about what that bound does and does not license you to claim.

The first two layers reason only about the model. They can prove the intended protocol is
self-consistent, and they cannot detect that the real Rust code does something the model never
described. Closing that gap is the third layer, _model-based testing_.

#custom-box(title: [Term — Model-based testing (MBT)], icon: "info")[
  Using the model as an oracle for the real implementation. The checked model generates sequences of
  actions; each action is mapped to a genuine call against the real system; and after every step the
  implementation's observable state is compared against the model's. When they disagree, either the
  code has a bug or the model does. Because the traces come from the model rather than a human's
  imagination, MBT drives the implementation through the same awkward orderings the checker explored,
  including the ones no one would have written by hand.
]

In turbolay this layer replays the action traces the model produces, encoded in a portable trace
format, against the engine's real public API, and checks after each action that the real graph's
state matches the model's. When the real code and the model agree across a large corpus of generated
traces, you have evidence that the implementation _refines_ the model, that within the tested domain
the code does what the checked specification says. This is the layer that finally connects the tidy
abstraction to the actual bytes and functions.

The fourth layer, _Jepsen_, would drive the fully deployed service, real processes against a real
S3-compatible store, and inject the process, network, and ownership faults a model cannot reproduce.
It is the intended culmination of the design, and it is deliberately out of scope for this part of
the book; we mention it only so the picture is complete. The chapters ahead pursue the first three
layers: the model, the bounded check, and the model-based test.

== What is actually being claimed

Layers and techniques are the how. Before going further it is worth stating plainly the _what_: the
specific claims about turbolay that all of this machinery exists to establish. The testing objective
lays them out as a contract, and the intuition behind each is more useful here than the exact wording.

The central promise is _atomic, durable, per-cell mutation_. A successful graph mutation is one
serializable, durable transaction against the cell's storage. It updates every canonical and derived
record that mutation implies, the edge itself, its adjacency postings, its degree counts, its
topology delta, its idempotency record, or it updates none of them. There is no observable state in
which half of an edge exists.

Around that promise sit the guarantees that make it safe under failure. There is _one effective
writer_ per cell: placement and cluster membership may nominate a candidate, but they are only
advice; the durable write fence, backed by the storage engine's writer epoch, is the sole authority
over who may commit. A process that has been fenced out cannot make a durable change, even if it
still believes it is the writer. There is _retry and idempotency safety_, the resolution of the
dropped-acknowledgement story: a repeated mutation carrying its idempotency key returns the original
result and applies nothing new, while a conflicting reuse of a key is rejected. There is
_snapshot-consistent reading_: a single query is answered from one pinned storage snapshot and never
stitches together adjacency, metadata, and indexes from different points in time; a request for a
future or unavailable historical epoch fails cleanly rather than fabricating an answer by mixing
state.

Then there are the guarantees that keep turbolay's optimizations from becoming second sources of
truth. The asynchronous matrix artifact is only an accelerator: a traversal over a base artifact plus
the deltas layered on top of it must equal a direct traversal of the canonical snapshot at the same
epoch. Garbage collection is constrained by _GC safety_: maintenance may reclaim only data that no
published artifact and no active read still needs. And underneath all of it, _placement is not
authority_, restated because it is the crux of correct takeover: divergent views of cluster membership
may produce competing writer candidates, but only the durable fence decides which one is real.

#custom-box(title: [Why], icon: "tip")[
  These claims are scoped to _one cell_ on purpose. turbolay's public surface offers no cross-query,
  cross-cell transaction: an explicit multi-statement Bolt transaction is rejected outright. Write
  throughput is meant to scale by adding cells, not by admitting concurrent writers to a single hot
  cell. That boundary is a deliberate design choice, and it matters for the tests: neither the
  model-based tests nor a future Jepsen run should report a failure merely because an operation
  spanning several cells is not serializable as one global transaction. It was never promised to be.
]

That is the contract. Everything in the following chapters is ultimately about making these
sentences executable, so a tool can search for a way to break them and, when it cannot within its
bound, when the real code matches the model across the trace corpus, you have earned some real
confidence.

== The honest boundary

Here is the part that is easy to skip and most important not to. A verification effort is only as
trustworthy as its account of what it did _not_ establish. It would be a small and satisfying lie to
say "turbolay is proved correct." It is not, and the design documents are emphatic about this. What
you get from these three layers is precise and genuinely useful, but its edges are sharp, and you
should be able to see them.

Start with what the model deliberately does not describe. The models work in a _shared-state
abstraction_: a durable storage transaction is treated as one atomic state update, full stop. They
say nothing about byte encoding, about how the log-structured storage compacts files, about how S3 is
actually implemented, about how Rust schedules threads, about TLS, about query parsing, or about
Kubernetes internals. This is not an oversight and not a claim that those layers are unimportant. It
is a division of labor. Each of those concerns has a technique better suited to it than a protocol
model: key encoding and malformed input are the province of Rust unit and property tests; the
storage engine's atomic-commit and fencing guarantees are an upstream contract exercised by
integration and, eventually, Jepsen tests; real S3 retries, latency, and throttling belong to fault
tests against a local S3-compatible store and then soak tests against the real thing; query-language
compatibility has its own conformance work; throughput and cost belong to benchmarks. The model
abstracts these away precisely so that it can say something sharp about the one thing it is
responsible for, the protocol, without drowning in the things it is not.

Now the subtler boundary, the one around the word "proved." When the bounded checker examines a model
through six transitions and reports no error, it is stating something exact: every behavior of that
_small, stated abstraction_, up to six steps, satisfies the checked safety properties. Read the
qualifiers, because every one of them is load-bearing. _Small abstraction_: the check is about the
model, not the code. _Up to six steps_: behaviors that only go wrong on the seventh action are
outside what was examined. And the domain is finite, a handful of vertices, a bounded set of
operations, not an unbounded graph. "Six transitions checked" is emphatically not "proved correct for
all graphs of all sizes under all failures." It is a strong statement about a deliberately small
world.

#custom-box(title: [Why], icon: "tip")[
  Why bounded, then, if it is not a full proof? Because bugs in concurrent, failure-prone protocols
  are overwhelmingly _shallow_: they show up in short sequences of a few operations and a
  well-placed fault, exactly the sequences a small bounded check enumerates exhaustively. A bounded
  check is a favorable trade, complete coverage of the short behaviors where the real bugs cluster,
  in exchange for honesty about the long and large ones it did not touch. The alternative,
  unbounded proof over arbitrary graphs, is dramatically harder and, for this class of protocol,
  usually not where the defects are hiding.
]

Each layer extends the boundary a little and moves it somewhere new, and it is worth being precise
about what each one adds and what it still leaves open. The Quint model and its simulations establish
that the intended protocol is expressible and not obviously self-contradictory, and no more; a passing
simulation is a sample, not a proof. The bounded Apalache check upgrades a chosen set of properties
to exhaustive coverage within a small step bound and a finite domain, and no further; it proves
nothing about larger graphs, longer histories, or arbitrary storage failures. The model-based tests
bind that abstraction to the real Rust code, showing the implementation refines the checked model for
the bounded domain the traces cover, and they run first against a local in-memory object store and
then against a local S3-compatible store, but they do not thereby establish behavior on real S3, and
they are not a substitute for the deployed-process fault testing that Jepsen would provide. The final
step, binding the abstraction to real processes under real faults, is the deferred fourth layer.

So state the result in the words the design uses. What this effort produces is not a proof of S3, of
the storage library, of Kubernetes, or of an unbounded graph. It is a _precise safety contract for
turbolay's own protocol_, plus tests that can find violations of that contract, plus, eventually,
operational histories that exercise it in the deployed architecture. That is a more modest claim than
"proved correct," and it is a far more valuable one than "we ran the tests and they passed," because
you can see exactly where its authority ends. When a later chapter reports that a model passed a
six-step check and that twenty-four generated traces replayed cleanly against the real engine, you
will know precisely what has been shown and precisely what has not, and you will be able to weigh it
without being sold anything.

== What the next chapters do

With the problem framed and the boundary drawn, the rest of this part is constructive. The next
chapter teaches Quint from zero. It does not touch turbolay at all; instead it builds a tiny model of
something familiar, so you learn the language, how state is declared, how actions are written, how an
invariant is stated and checked, without also having to carry the graph engine's complexity. By the
end of it the notation on the page will read as ordinary code rather than mathematics.

The chapter after that opens the first real turbolay model and reads it closely: the model of a
single cell's write path, the one that makes the atomic-mutation, epoch-and-fence, and
dropped-acknowledgement properties from this chapter executable. From there the remaining chapters
follow the layers upward, into bounded checking with Apalache and then into replaying the generated
traces against the real engine, always keeping the boundary from this chapter in view. The through
line never changes: write down what turbolay must do, small and exact; let a tool search for a way to
break it; then hold the real code to the same standard. The dropped acknowledgement we opened with is
where that search begins.
