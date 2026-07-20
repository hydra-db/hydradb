#import "../vendor/bookly/src/bookly.typ": *
#import "@preview/fletcher:0.5.7" as fletcher: diagram, node, edge

// LEARNING GOAL: the reader understands model-based testing as the bridge from
// the abstract Quint model to the real Rust kernel — Quint generates ITF action
// traces, a Rust harness replays each step by driving turbolay's public
// GraphShard API, and after each step reads the real durable state and asserts
// it REFINES the model's state projection. The reader can run the tests, knows
// the six adapters default to InMemory and swap to MinIO via env / just, and
// understands what this establishes (real code refines the checked model for a
// bounded domain) and what it does not (not a proof over all inputs; a MinIO
// pass proves only that corpus against one pinned image).
// GROUND IN:
//   tests/formal_mbt.rs                (the M1 MBT harness — struct/fields/methods)
//   tests/support/mbt_backend.rs       (backend factory: InMemory default, minio env vars)
//   tests/formal_mbt_m2.rs .._p2.rs    (the other five adapters)
//   justfile                           (the minio-mbt recipe)
//   docs/formal-methods/0001-quint-jepsen-testing-objective.md  (layer 3; components note)
//   docs/formal-methods/0006-minio-mbt-handoff.md               (completed; evidence boundary)
//   docs/formal-methods/0002-turbolay-quint-specification-plan.md:342  (cargo test gate)

= Model-based testing: replaying traces against the real kernel

Every chapter up to now has been about the _model_. You learned to read
`m1_cell_write.qnt` as a precise statement of the write-path contract; you
watched an exhaustive checker walk every interleaving of its `step` and judge
`allSafety` on all of them; the bounded-proof chapter turned that walk into a
symbolic argument over a finite horizon. All of that establishes one thing, and
it is a large thing: _the model is correct_. Given the model's rules, no
reachable state violates the contract.

But stop and ask the question that should have been nagging you since the very
first model. The model is a fourteen-variable state machine written in Quint.
The real turbolay write path is thousands of lines of Rust that opens SlateDB
transactions, checks a durable fence, writes a fan-out of keys, and commits
atomically against an object store. Proving the _model_ correct says nothing, on
its own, about whether the _code_ does what the model says. A perfect proof about
the wrong program is worthless. So how do we tie the two together? How do we get
evidence that the real `GraphShard` — the actual public kernel your service
calls — behaves the way the checked model promises?

That is the question this chapter answers, and the answer has a name.

#custom-box(title: [Term — Model-based testing (MBT)], icon: "info", color: purple)[
  A testing technique in which the _test cases are generated from a model_
  rather than written by hand. The model produces sequences of actions; a
  harness executes each action against the real system; after each step the
  harness compares the real system's observable state against the state the
  model predicts. Where an ordinary unit test asserts one hand-picked outcome,
  model-based testing lets the model author thousands of action sequences and
  checks the implementation against the model on every one of them.
]

Model-based testing is the third layer of turbolay's evidence stack. The
objective document names it in one sentence, and it is worth reading exactly,
because every word is load-bearing (`0001-quint-jepsen-testing-objective.md:34-36`):
"Rust MBT replays generated Informal Trace Format (ITF) action traces against
Turbolay's public kernel API, showing that the real implementation refines the
checked model for a bounded domain." Unpack it slowly. _Generated_ traces: Quint
produces them, no human writes them. _ITF_: a concrete file format the traces
travel in. _Public kernel API_: the harness only ever calls what a real client
could call. _Refines_: the relationship being checked, defined below. _Bounded
domain_: the honest limit, held until the end of the chapter.

== The problem, made concrete: two state machines, one claim

Picture the two things side by side. On the left is the Quint model: when
`createEdge` fires, the model assigns `edgePresent' = true`, `outDegree' = 1`,
`epoch' = epoch + 1`, and stamps the idempotency record — one atomic update to
fourteen variables. On the right is the real kernel: when your service calls
`GraphShard::write_edge`, turbolay opens a serializable SlateDB transaction,
validates the fence, writes the adjacency keys, the degree counters, the topology
delta, the idempotency record, and the epoch counter, and commits. Two very
different machines. The claim the whole verification effort rests on is that the
second _refines_ the first.

#custom-box(title: [Term — Refinement], icon: "info", color: purple)[
  One system _refines_ another when everything the first does is permitted by the
  second — the concrete implementation is an allowed behavior of the abstract
  model. For turbolay this means: reduce the real durable state to the same
  normalized projection the model tracks (does the edge exist, what is the
  degree, what epoch are we at), and after every action that projection must
  equal the value the model computed. The real code is free to do vastly _more_
  underneath — more keys, more indexes, real bytes on S3 — but it must never
  produce a normalized state the model would forbid.
]

Refinement is exactly the word the write-path model's own header reached for.
`m1_cell_write.qnt:6-8` says the projections "must commit together" and "any
successful Turbolay mutation must refine this atomic transition." Model-based
testing is how that sentence stops being an aspiration in a comment and becomes a
check that runs. The model says what the projection must be; the harness reads
what the real projection _is_; refinement holds when, step after step, they
match.

#custom-box(title: [Why], icon: "tip", color: rgb("#c99700"))[
  Why generate the traces from the model instead of writing test cases by hand?
  Because the model has already been proven to explore states a human would never
  think to enumerate — reply lost then retried then crashed then fenced, in every
  order the guards permit. Hand-written integration tests exercise the paths their
  author imagined. Model-generated traces exercise the paths the _checker_ found
  interesting, including the awkward interleavings that break real systems. The
  work of thinking up adversarial sequences was already done when the model was
  checked; MBT reuses it, pointing that same generated variety at the real code.
]

== The refinement loop, end to end

Before any Rust, hold the whole mechanism as one picture. It is a pipeline with a
loop in the middle. Quint runs the model in a mode that emits action traces; each
trace is a file; a Rust bridge decodes the file and, for every step in it, drives
the real kernel, reads the real durable state back, and compares that to the
model's projection for the same step. A match lets the replay advance to the next
step; a mismatch fails the test with the exact diverging step in hand. The same
generated corpus is then run a second time against a different storage backend.

#figure(
  diagram(
    node-stroke: 0.6pt + reader-colors.border,
    node-outset: 0pt,
    spacing: (2.5cm, 1.15cm),
    // The abstract / model side.
    node((0, 0), text(fill: reader-colors.text, hyphenate: false)[Quint model\ `m1_cell_write.qnt`], fill: reader-colors.info_soft, width: 3.4cm),
    node((0, 1), text(fill: reader-colors.text, hyphenate: false)[ITF action trace\ `init` → `openWriter1` → `createEdge` → …], fill: reader-colors.info_soft, width: 4.0cm),
    node((0, 2), text(fill: reader-colors.text, hyphenate: false)[`quint-connect` bridge\ decode + `switch!` each step], fill: reader-colors.surface_soft, width: 4.0cm),
    // The real-kernel side.
    node((0, 3), text(fill: reader-colors.text, hyphenate: false)[drive real `GraphShard`\ `write_edge` / `delete_edge` / …], fill: reader-colors.ok_soft, width: 4.0cm),
    node((2.15, 3.5), text(fill: reader-colors.text, hyphenate: false)[durable object store\ InMemory · MinIO], fill: reader-colors.surface_soft, width: 2.9cm),
    node((0, 4), text(fill: reader-colors.text, hyphenate: false)[read real durable state\ `current_epoch`, `edge_exists`, `out_degree`], fill: reader-colors.surface_soft, width: 4.3cm),
    // The prominent compare / verdict node.
    node((0, 5), text(fill: reader-colors.text, hyphenate: false)[*compare* real state\ vs model `State` projection], fill: reader-colors.warn_soft, stroke: (paint: reader-colors.info, thickness: 1.2pt), corner-radius: 5pt, width: 4.3cm),
    node((-1.35, 6), text(fill: reader-colors.text)[✓ refines\ take next step], fill: reader-colors.ok_soft, width: 2.7cm),
    node((1.4, 6), text(fill: reader-colors.text)[✗ divergence\ test fails], fill: reader-colors.bad_soft, stroke: (dash: "dashed", paint: reader-colors.bad, thickness: 1pt), width: 2.7cm),
    // Pipeline edges.
    edge((0, 0), (0, 1), "->", text(fill: reader-colors.muted, size: 7.5pt)[`quint run --mbt`], stroke: reader-colors.muted, label-side: right),
    edge((0, 1), (0, 2), "->", text(fill: reader-colors.muted, size: 7.5pt)[`.itf.json`], stroke: reader-colors.muted, label-side: right),
    edge((0, 2), (0, 3), "->", text(fill: reader-colors.muted, size: 7.5pt)[map action → API call], stroke: reader-colors.muted, label-side: right),
    edge((0, 3), (2.15, 3.5), "->", text(fill: reader-colors.muted, size: 7pt)[write], stroke: reader-colors.muted),
    edge((2.15, 3.5), (0, 4), "->", text(fill: reader-colors.muted, size: 7pt)[read], stroke: reader-colors.muted),
    edge((0, 4), (0, 5), "->", stroke: reader-colors.muted),
    edge((0, 5), (-1.35, 6), "->", text(fill: reader-colors.muted, size: 7pt)[equal], stroke: reader-colors.muted, label-side: left),
    edge((0, 5), (1.4, 6), "->", text(fill: reader-colors.muted, size: 7pt)[differ], stroke: reader-colors.muted, label-side: right),
    // The loop back, routed down the left margin so it stays clear of the spine.
    edge((-1.35, 6), (-1.35, 3), (0, 3), "-->", text(fill: reader-colors.info, size: 7pt)[next trace step], stroke: (paint: reader-colors.info, dash: "dashed")),
  ),
  caption: [The MBT refinement loop. Quint runs the checked model to emit an _ITF action trace_ (top, abstract, in the model's blue); the `quint-connect` bridge decodes it and, for each step, drives the real `GraphShard` kernel (green — the actual turbolay code) against a durable object store, reads the real normalized state back, and compares it to the model's `State` projection at the prominent verdict node. Equal states advance the replay one step (the dashed loop); a divergence fails the test, naming the exact step. The store node carries the backend swap: the same corpus runs first against process-local `InMemory`, then against S3-compatible `MinIO`.],
) <fig-ch8-refinement-loop>

Notice which nodes are which colour, because the colour _is_ the argument. The
blue nodes are the abstract model — the same model the earlier chapters checked.
The green node is the real kernel, the code that ships. The whole point of the
loop is to make those two agree at the verdict node, over and over, on traces the
model itself invented.

#custom-box(title: [Term — Informal Trace Format (ITF)], icon: "info", color: purple)[
  A JSON file format for _traces_ — sequences of states and the actions between
  them — emitted by Quint (and other Apalache-family tools). When `quint run` is
  asked for model-based testing output it writes each generated behavior as an
  `.itf.json` file: an ordered list of steps, each recording the action name that
  fired and the values of the model's variables afterward. ITF is the neutral
  wire format that carries a Quint-generated trace out of the model and into the
  Rust harness that replays it.
]

The objective document is careful to keep the pieces distinct, and so should you:
"Quint produces action traces, `quint-connect`/ITF is the Rust MBT bridge, and
Apalache is the bounded model checker. They complement one another rather than
being interchangeable" (`0001-quint-jepsen-testing-objective.md:183-185`). ITF is
not a checker and not a model; it is the format the trace travels in. Apalache
proves; MBT replays. Do not confuse the two.

== The harness, grounded: `tests/formal_mbt.rs`

Everything in the picture is a real file. The M1 harness is
`tests/formal_mbt.rs`, and its header states its whole discipline in two
sentences (`formal_mbt.rs:1-6`): the harness state "deliberately mirrors the
public, durable write projection in `m1_cell_write.qnt`," and "every non-crash
action reads the real graph state after executing its public API call." Read that
as the refinement contract restated in Rust: mirror the model's projection, and
after each action go read the _real_ state.

The bridge is a small crate. The harness pulls its vocabulary from
`quint_connect` and the kernel from `slatedb_graph_kernel`
(`formal_mbt.rs:13-16`):

```rust
use quint_connect::{quint_run, switch, Driver, Result, State, Step};
use slatedb_graph_kernel::{CommitResult, EdgeMutation, GraphError, GraphShard};
```

Five names carry the whole design. `quint_run` is an attribute macro that
generates the traces and runs the replay. `Step` is one decoded ITF step.
`switch!` dispatches on the step's action name. `Driver` is the trait your harness
implements to drive the real system, and `State` is the trait that reads the real
state back for comparison. On the other line, `GraphShard` is turbolay's real
public kernel handle, and `EdgeMutation` / `CommitResult` / `GraphError` are its
real request and result types — the exact ones a production caller uses.

=== The state projection: ITF fields become a Rust struct

The comparison needs a shape to compare. That shape is `M1State`, and it is a
one-to-one image of the model's durable projection (`formal_mbt.rs:21-38`):

```rust
#[derive(Debug, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
struct M1State {
    epoch: i64,
    previous_epoch: i64,
    edge_present: bool,
    out_degree: i64,
    delta_epoch: i64,
    create_recorded: bool,
    delete_recorded: bool,
    // … create/delete outcome epochs, unknown_reply, the writer fence fields …
    last_action: String,
}
```

Every field is a Quint variable you already know from the write-path model:
`epoch`, `edgePresent`, `outDegree`, `deltaEpoch`, `createRecorded`,
`lastAction`, and the rest. Two derives do the connecting work. `Deserialize`
with `#[serde(rename_all = "camelCase")]` is what lets serde read the ITF trace's
`edgePresent` field straight into the Rust `edge_present`, so the model's
`camelCase` variable names line up with Rust's `snake_case` fields automatically.
`PartialEq` is what makes the comparison a single `==`: refinement, at each step,
is literally `real_state == model_state`.

That equality has two sides, and they come from two different places. The
model's side is deserialized from the ITF trace. The real side is produced by the
`State` trait, whose one method reads the live driver (`formal_mbt.rs:40-58`):

```rust
impl State<M1Driver> for M1State {
    fn from_driver(driver: &M1Driver) -> Result<Self> {
        Ok(Self {
            epoch: driver.last_epoch,
            edge_present: driver.last_edge_present,
            out_degree: driver.last_out_degree,
            // … every other field, pulled from what the real kernel last reported …
            last_action: driver.last_action.clone(),
        })
    }
}
```

`from_driver` is the "read the real normalized durable state" node in the figure.
It assembles an `M1State` out of what the real `GraphShard` most recently told the
driver. The framework then compares that against the ITF step's state, and a
difference on any field is a refinement violation.

=== Driving the real kernel: the action-to-API loop closes here

The write-path chapter left you a table mapping each Quint
action to the real `GraphShard` call it abstracts — `createEdge` to `write_edge`,
`openWriter1` to opening a standalone writer, `rejectZombieWrite` to a fenced
former-writer handle. That table was a promise. This is where it becomes
executable. The `Driver` trait's `step` method is exactly that table, written as a
`switch!` (`formal_mbt.rs:114-128`):

```rust
fn step(&mut self, step: &Step) -> Result {
    switch!(step {
        init => { self.init()?; },
        openWriter1 => { self.open_writer1()?; },
        createEdge => { self.create_edge(false)?; },
        commitThenLoseReply => { self.create_edge(true)?; },
        retryCreate => { self.retry_create()?; },
        rejectConflictingRetry => { self.reject_conflicting_retry()?; },
        deleteEdge => { self.delete_edge()?; },
        retryDelete => { self.retry_delete()?; },
        crashWriter1 => { self.crash_writer1()?; },
        takeOverWriter2 => { self.take_over_writer2()?; },
        rejectZombieWrite => { self.reject_zombie_write()?; },
    })
}
```

Read it against the model's `step` from the write-path chapter and they are the
same list of action names — but where the model's `step` was an `any { ... }` that
let the checker _choose_ a transition, this `switch!` _dispatches_ on the action
name the ITF trace already chose. The trace decides; the harness executes. And
each arm calls the real kernel. `openWriter1` opens a real writer through
`GraphShard::open_standalone_writer` against the store (`formal_mbt.rs:286-293`).
`createEdge` builds a real `EdgeMutation` and calls `write_edge`
(`formal_mbt.rs:168-182`). The two create arms differ only in a boolean —
`commitThenLoseReply` is `create_edge(true)`, modelling the lost acknowledgement
by remembering the reply was not delivered — exactly the one-assignment difference
you saw between `createEdge` and `commitThenLoseReply` in the model.

The fence story is the one worth watching, because it uses the real kernel to
prove the real kernel refuses a zombie. `crash_writer1` takes the open writer and
_closes_ it, stashing the closed handle as a `zombie_writer` rather than dropping
it (`formal_mbt.rs:243-251`). `take_over_writer2` opens a fresh writer against the
same storage. Then `reject_zombie_write` reaches for that retained dead handle and
tries to write through it (`formal_mbt.rs:262-279`):

```rust
let result = self.runtime.block_on(zombie.write_edge(EdgeMutation {
    // … src 9, dst 10, idempotency_key "m1-zombie" …
}));
if result.is_ok() {
    bail!("M1 former writer committed after replacement writer takeover");
}
```

This is not a mock. It is the real former-writer `GraphShard`, holding a stale
fence token, being told to commit — and the test _fails loudly_ if it succeeds.
The model's `rejectZombieWrite` action, whose every variable framed unchanged, is
here enforced against the actual durable fence in the actual object store.

=== Reading the real state back: the oracle

After each write arm, the harness must learn what the real store now holds, so it
can be compared. That read-back is `refresh_projection`, and it queries the real
`GraphShard` through its public read API (`formal_mbt.rs:303-316`):

```rust
let (epoch, edge_present, out_degree) = self.runtime.block_on(async {
    Ok::<_, GraphError>((
        writer.current_epoch(CELL).await?,
        writer.edge_exists(CELL, EDGE_TYPE, 1, 2).await?,
        writer.out_degree(CELL, EDGE_TYPE, 1).await?,
    ))
})?;
```

These three calls — `current_epoch`, `edge_exists`, `out_degree` — are the
normalized oracle. They ask the real engine the same three questions the model's
`epoch`, `edgePresent`, and `outDegree` variables answer, and nothing about the
private byte layout underneath. That restraint is deliberate and is the P2
adapters' stated rule too: their drivers "deliberately retain only normalized
public observations ... they do not inspect private object-store keys"
(`tests/formal_mbt_p2.rs:3-6`). Refinement is checked at the public surface, so a
pass means the _contract_ holds, not that two implementations happen to share
private internals.

=== Generating and bounding the traces

One thing remains: where do the traces come from, and how many? The `quint_run`
attribute on the test function answers both (`formal_mbt.rs:340-346`):

```rust
#[quint_run(
    spec = "quint-models/turbolay/m1_cell_write.qnt",
    main = "m1_cell_write",
    max_samples = 24,
    max_steps = 10,
    seed = "20260718"
)]
fn m1_randomized_public_write_trace_refines_quint() -> impl Driver {
    M1Driver::default()
}
```

The macro names the model to run (`spec`, `main`), asks Quint for `max_samples`
generated traces of up to `max_steps` steps each, and fixes a `seed` so the corpus
is _reproducible_ — the same twenty-four traces every run, not a fresh random draw.
The function body just hands back a fresh `Driver`. At test time the macro runs
Quint to emit the ITF traces, then for each trace replays every step through your
`Driver::step` and compares `State::from_driver` against the trace after each one.
Those two numbers, `24` and `10`, are the entire size of the domain this test
covers — a fact we return to when we are honest about boundaries.

== The two backends: same corpus, two stores

The refinement loop is indifferent to _where_ the real kernel keeps its bytes,
which is precisely what makes the backend swap valuable. The identical trace
corpus runs first against a fast in-process store, then against a real
S3-compatible one, and the choice is made entirely by environment, in one shared
factory: `tests/support/mbt_backend.rs`. Its header states the default plainly
(`mbt_backend.rs:3-5`): "The default is deliberately process-local `InMemory`.
Set `GRAPH_MBT_BACKEND=minio` and `GRAPH_MBT_S3_ENV_FILE=/path/to/minio.env` to
load the S3-compatible backend through SlateDB's normal AWS configuration." The
factory reads `GRAPH_MBT_BACKEND`, defaulting to `"memory"`, and hands each replay
a unique, path-safe graph location so replays never collide
(`mbt_backend.rs:33-70`).

#figure(
  table(
    columns: (0.7fr, 1.25fr, 1.25fr),
    align: (left, left, left),
    stroke: 0.5pt + reader-colors.border,
    fill: (_, row) => if row == 0 { reader-colors.surface_soft },
    inset: 7pt,
    table.header(
      text(fill: reader-colors.text)[*Aspect*],
      text(fill: reader-colors.text)[*InMemory (default)*],
      text(fill: reader-colors.text)[*MinIO (S3-compatible)*],
    ),
    [What it exercises], [Kernel protocol logic against a process-local store], [The same logic against a real S3 API: network, CAS, durability],
    [Isolation], [Fresh `InMemory` store, unique graph path per replay], [Disposable bucket under `GRAPH_MBT_PREFIX`, unique path per replay],
    [Selected by], [nothing — it is the default], [`GRAPH_MBT_BACKEND=minio` + `GRAPH_MBT_S3_ENV_FILE`],
    [Command], [`cargo test --locked --test formal_mbt_<family>`], [`just minio-mbt`],
    [Speed], [Fast; the everyday gate], [Slower; needs Docker and a pinned image],
  ),
  caption: [The two MBT backends. Both replay the _same_ seeded trace corpus through the _same_ adapters and the same normalized oracle; only the durable store underneath changes. `InMemory` is the fast default that runs on every check; `MinIO` re-runs the identical corpus against a real S3-compatible API to exercise the storage layer the in-memory store abstracts away.],
) <tab-ch8-backends>

Running the fast default needs no setup. The specification plan pins the gate's
shape (`0002-turbolay-quint-specification-plan.md:342`): `cargo test --locked
--test formal_mbt_<family>`, where `<family>` is `formal_mbt` for M1 and
`formal_mbt_m2` through `formal_mbt_p2` for the rest. There are six adapters in
all — M1, M2, M3, M4, M5, and P2 — each a `tests/formal_mbt*.rs` file that replays
its own model's traces, and all six share the one backend factory.

The MinIO run is a single recipe. The `justfile` keeps it to one line
(`justfile:124-125`):

```
minio-mbt:
    bash scripts/minio_mbt.sh
```

That script, per the MinIO handoff, "starts the pinned local MinIO and `mc`
images, creates a disposable bucket, replays the six adapters serially with their
unchanged seeds, and removes resources on success"
(`0006-minio-mbt-handoff.md:31-33`). On failure it does the opposite of tidy up:
it _retains_ the generated configuration, per-adapter Cargo output, the MinIO log,
the object list, and the bucket and prefix under `target/minio-mbt/`, so a
backend-specific failure can be reproduced (`0006-minio-mbt-handoff.md:34-36`).
The recorded result is that all six adapters pass against both the default
`InMemory` store and local MinIO (`0006-minio-mbt-handoff.md:37-40`).

#custom-box(title: [Why], icon: "tip", color: rgb("#c99700"))[
  Why bother running the identical corpus twice? Because `InMemory` and MinIO test
  different things through the same lens. The in-memory store makes the kernel's
  _protocol logic_ fast to check on every commit — the atomic projection, the
  idempotent retry, the fence. But it is not S3: it does not exercise the real
  conditional-write and durability behavior the fence actually depends on in
  production. Re-running the exact same traces against a real S3-compatible API
  keeps the model, the adapters, and the oracle fixed and swaps in the storage
  reality the in-memory store abstracted away. Same questions, harder substrate.
]

== What this establishes, and what it does not

Be exact about what you now have, because the value and the limit are two sides of
the same coin. What model-based testing establishes is strong and concrete: the
_real_ `GraphShard` kernel, driven through its _public_ API, produces a normalized
durable state that _refines_ the model the earlier chapters checked — not in the
abstract, but on a specific corpus of model-generated traces, verified step by
step, and re-verified against a real S3-compatible store. This is the bridge the
whole effort needed. The proof about the model now has a tested tie to the code.

What it does _not_ establish follows from the two small numbers in the
`quint_run` attribute. `max_samples = 24`, `max_steps = 10`: the corpus is finite.
Model-based testing checks the real code against the model on _that_ bounded set
of traces, seeded for reproducibility. It is not a proof over all inputs — a
create sequence of length eleven, or the twenty-fifth trace the seed never drew,
is simply not covered. That coverage-over-all-paths guarantee, within a finite
horizon, is the bounded-proof chapter's job, not this one's; the two are
complements, as the objective document insists they must be kept.

The MinIO result carries its own boundary, and the handoff states it in as many
words: "A passing local MinIO replay proves only this finite corpus against that
pinned S3-compatible image and configuration. It does not prove arbitrary S3
provider behavior, S3 outage handling, performance, CI execution, or Jepsen
process-level fault tolerance. It must not be reported as Jepsen evidence"
(`0006-minio-mbt-handoff.md:44-47`). A green MinIO run is evidence about one
pinned image running one seeded corpus. It is not evidence about production S3, and
it is not a distributed-systems fault test.

That last exclusion points precisely at what is left. Model-based testing drives
the real kernel, but it drives it in one process, against a store that does not
crash mid-write, partition, or reorder under real network faults. Those failures —
processes killed at the worst moment, ownership contested across a partition,
clocks and messages misbehaving — are exactly what a model cannot reproduce and a
harness in one process cannot inject. Establishing that the real deployed system
survives them is a different kind of test entirely.

You can now run these tests and read what they prove. `cargo test --test
formal_mbt` replays the model-generated M1 corpus against the real kernel on the
fast in-memory store; `just minio-mbt` re-runs all six adapters against a real
S3-compatible one. Either way, the loop is the same: the model invents the trace,
the harness drives the real `GraphShard`, the oracle reads the real state, and
refinement is checked step by step. The closing chapter steps back from the
individual layers to the whole assurance stack — how the Quint check, the bounded
proof, and this model-based replay compose into one argument, and where the
deferred layer, injecting real process and network faults against the deployed
service, picks up the guarantees model-based testing deliberately leaves on the
table.
