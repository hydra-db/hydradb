#import "../vendor/bookly/src/bookly.typ": *
#import "@preview/fletcher:0.5.7" as fletcher: diagram, node, edge

// LEARNING GOAL: the reader can name all eight turbolay Quint model families,
// say in a sentence what subsystem each verifies and its headline safety
// property, see how together they cover the system, and know honestly what the
// suite does not model.
// GROUND IN: quint-models/turbolay/{m1_cell_write, m1_bulk_import,
//   m2_snapshot_read, m2_snapshot_lifecycle, m3_artifact_gc, m4_placement_fence,
//   m5_destructive_lifecycle, m5_public_commands}.qnt;
//   docs/formal-methods/0002-turbolay-quint-specification-plan.md;
//   docs/formal-methods/0004-api-coverage-completion-priority.md.

= The model gallery: mapping the whole verified surface

You have now read one model the slow way. You can open `m1_cell_write.qnt`, split each
action into its guards and its assignments, and say what claim about the write path every
line makes. The invariants-and-witnesses chapter then showed you how a checker _judges_ those
claims, and how a deliberately-broken twin produces the counterexample that proves the check
has teeth. The deterministic-scenarios chapter pinned specific stories through the machine so a
human can read one behavior end to end.

All of that was about a single model. turbolay is more than its write path. It admits bulk
imports, serves paginated snapshot reads, publishes immutable artifacts and later collects
them, decides which node in a cluster is allowed to write, executes destructive commands, and
answers a public command surface. If only the write path were verified, the word "verified"
would be doing dishonest work. So this chapter pulls all the way back. It is a _map_, not a
re-derivation: you already know how to read a model, and here you learn the shape of the whole
suite — the eight model families, what each one covers, and why turbolay's engineers drew the
boundaries exactly where they did.

By the end you should be able to point at any part of the system and name the model that
verifies it — or say, honestly, that no Quint model does and why that concern belongs to a
different kind of test.

== Why eight models and not one

Here is a question the previous chapters never had to answer. If the whole point of a model is
that a checker can walk _every_ reachable state, why not write one big model of all of turbolay
and check everything at once? One model, one `allSafety`, one verdict.

The answer is the reason this is a gallery and not a single canvas.

#custom-box(title: [Term — Model family], icon: "info", color: purple)[
  A model family is one self-contained Quint module that fixes a single abstraction boundary:
  one subsystem, its own small set of state variables, its own named actions, and its own
  `allSafety` conjunction and reachability witnesses. It is deliberately not the whole system.
  Each of the eight `.qnt` files under `quint-models/turbolay/` is one family, chosen so that
  its state space is small enough to walk exhaustively and its invariants read as one coherent
  contract.
]

A single model of everything would fail on two counts. First, state-space size: the write
path's model already carries fourteen variables, and the checker's work grows explosively as
you multiply subsystems together. A monolith of writes _and_ reads _and_ artifacts _and_
placement would be too large to walk exhaustively, which throws away the one advantage a model
had over integration testing. Second, and more important, it would be unreadable — and an
unreadable model is just a second program to get wrong, not a check on the first.

#custom-box(title: [Why], icon: "tip", color: rgb("#c99700"))[
  The specification plan makes the separation an explicit design rule, and pairs every family
  with the reason it stands alone
  (`0002-turbolay-quint-specification-plan.md:77-83`): the atomic durable mutation is "the
  foundational state machine"; snapshot pinning "needs a read-specific state model"; async
  artifact publication and reclamation "are independent from canonical transaction
  correctness"; placement "may disagree while the durable fence remains authoritative"; and the
  public-command model maps supported commands onto the M1/M2 semantics "without trying to model
  parsing." Each boundary is drawn where one subsystem's contract can be stated and checked
  without dragging in another's. The models compose in your _head_ — a write commits, and a
  later snapshot read pins that committed view — but the checker never has to compose them, so
  each stays small and each stays legible.
]

The eight families are not an arbitrary carving. Read together they trace the path a piece of
data takes through the system: a command arrives, a fenced writer commits it (one cell at a
time, or in bulk), the change becomes a topology delta and eventually an immutable artifact, a
reader opens a snapshot and pages through it, and destructive commands and cluster membership
sit at the edges. The next figure draws that whole surface at once, with each model laid over
the part it verifies.

== The coverage map

Think of turbolay as a few subsystems stacked on one durable substrate. Commands enter at the
top. Writes and reads are the two paths through the middle. Artifacts and garbage collection
sit underneath the write path, deriving immutable snapshots of the topology. Placement and the
writer fence stand to the side, deciding _who_ may write. And every one of these rests on the
durable object store — SlateDB on S3 — whose bytes the suite deliberately never opens.

Each of the eight models is a badge pinned over the subsystem whose contract it verifies. Read
the picture as a covering: where a badge sits, a checked `allSafety` guards that part of the
system; the store band at the bottom is shaded neutral because no Quint model reaches inside it.

#figure(
  block(width: 100%)[
    #diagram(
      node-outset: 0pt,
      node-stroke: none,
      spacing: (1.0cm, 1.0cm),

      // ---- region backgrounds (drawn first, behind the badges) ----
      // Command surface (info)
      node(enclose: ((1, 0), (4, 0), (2, -0.55)), inset: 9pt, corner-radius: 8pt,
        stroke: (dash: "dashed", paint: reader-colors.info, thickness: 0.7pt),
        fill: reader-colors.info_soft.transparentize(55%)),
      // Placement & fence (bad)
      node(enclose: ((0, 0.5), (0, 1)), inset: 8pt, corner-radius: 8pt,
        stroke: (dash: "dashed", paint: reader-colors.bad, thickness: 0.7pt),
        fill: reader-colors.bad_soft.transparentize(55%)),
      // Write path (ok)
      node(enclose: ((1, 1), (2, 1), (1, 0.5)), inset: 8pt, corner-radius: 8pt,
        stroke: (dash: "dashed", paint: reader-colors.ok, thickness: 0.7pt),
        fill: reader-colors.ok_soft.transparentize(55%)),
      // Read path (purple)
      node(enclose: ((3, 1), (4, 1), (3, 0.5)), inset: 8pt, corner-radius: 8pt,
        stroke: (dash: "dashed", paint: reader-colors.purple, thickness: 0.7pt),
        fill: reader-colors.purple_soft.transparentize(50%)),
      // Artifacts & GC (warn)
      node(enclose: ((1, 2), (2, 2), (2, 1.6)), inset: 8pt, corner-radius: 8pt,
        stroke: (dash: "dashed", paint: reader-colors.warn, thickness: 0.7pt),
        fill: reader-colors.warn_soft.transparentize(50%)),

      // ---- subsystem titles ----
      node((2, -0.5), text(fill: reader-colors.muted, size: 8pt)[*Command surface*]),
      node((1, 0.5), text(fill: reader-colors.muted, size: 7.5pt)[*Write path*]),
      node((3, 0.5), text(fill: reader-colors.muted, size: 7.5pt)[*Read path*]),
      node((0, 0.5), text(fill: reader-colors.muted, size: 7pt)[*Placement\ & fence*]),
      node((2, 1.6), text(fill: reader-colors.muted, size: 7.5pt)[*Artifacts & GC*]),

      // ---- model badges ----
      node((1, 0), text(fill: reader-colors.text, size: 7.5pt)[*M5*\ #text(size: 6.5pt)[public cmds]], fill: reader-colors.info_soft, width: 1.7cm, inset: 5pt, corner-radius: 6pt, stroke: 0.7pt + reader-colors.info),
      node((4, 0), text(fill: reader-colors.text, size: 7.5pt)[*M5b*\ #text(size: 6.5pt)[destructive]], fill: reader-colors.info_soft, width: 1.7cm, inset: 5pt, corner-radius: 6pt, stroke: 0.7pt + reader-colors.info),
      node((1, 1), text(fill: reader-colors.text, size: 7.5pt)[*M1*\ #text(size: 6.5pt)[single cell]], fill: reader-colors.ok_soft, width: 1.7cm, inset: 5pt, corner-radius: 6pt, stroke: 0.7pt + reader-colors.ok),
      node((2, 1), text(fill: reader-colors.text, size: 7.5pt)[*M1b*\ #text(size: 6.5pt)[bulk import]], fill: reader-colors.ok_soft, width: 1.7cm, inset: 5pt, corner-radius: 6pt, stroke: 0.7pt + reader-colors.ok),
      node((3, 1), text(fill: reader-colors.text, size: 7.5pt)[*M2*\ #text(size: 6.5pt)[cursor pin]], fill: reader-colors.purple_soft, width: 1.7cm, inset: 5pt, corner-radius: 6pt, stroke: 0.7pt + reader-colors.purple),
      node((4, 1), text(fill: reader-colors.text, size: 7.5pt)[*M2b*\ #text(size: 6.5pt)[lifecycle]], fill: reader-colors.purple_soft, width: 1.7cm, inset: 5pt, corner-radius: 6pt, stroke: 0.7pt + reader-colors.purple),
      node((0, 1), text(fill: reader-colors.text, size: 7.5pt)[*M4*\ #text(size: 6.5pt)[fence]], fill: reader-colors.bad_soft, width: 1.55cm, inset: 5pt, corner-radius: 6pt, stroke: 0.7pt + reader-colors.bad),
      node((1, 2), text(fill: reader-colors.text, size: 7.5pt)[*M3*\ #text(size: 6.5pt)[GC]], fill: reader-colors.warn_soft, width: 1.55cm, inset: 5pt, corner-radius: 6pt, stroke: 0.7pt + reader-colors.warn),

      // ---- flow edges (muted, never black) ----
      edge((1, 0), (1, 0.5), "->", text(fill: reader-colors.muted, size: 6.5pt)[writes], stroke: reader-colors.muted, label-pos: 0.5, label-side: right),
      edge((4, 0), (4, 1), "->", text(fill: reader-colors.muted, size: 6.5pt)[reads], stroke: reader-colors.muted, label-pos: 0.4, label-side: right),
      edge((0, 1), (1, 1), "->", text(fill: reader-colors.muted, size: 6.5pt)[fences], stroke: reader-colors.muted, label-pos: 0.5, label-side: left),
      edge((2, 1), (1, 2), "->", text(fill: reader-colors.muted, size: 6.5pt)[deltas], stroke: reader-colors.muted, label-pos: 0.3, label-side: left),
    )
    #v(5pt)
    #block(width: 100%, fill: reader-colors.surface_soft, inset: 8pt, radius: 5pt,
      stroke: 0.5pt + reader-colors.border)[
      #align(center)[#text(fill: reader-colors.text, size: 8.5pt)[*Durable object store* — SlateDB on S3 · #text(fill: reader-colors.muted)[every subsystem above commits to and reads from it]\ #text(size: 7.5pt, fill: reader-colors.muted)[bytes · LSM compaction · S3 internals · TLS · Kubernetes — _not modeled by Quint_]]]
    ]
  ],
  caption: [The eight Quint model families as a covering of turbolay. Each colored badge is a
    model, pinned over the subsystem whose contract it verifies; a dashed region groups the
    models that guard one part of the system. Commands enter at the top; the write path and read
    path run through the middle; placement and the writer fence decide who may write; artifacts
    and GC derive immutable topology. Everything rests on the durable object store band beneath,
    whose bytes (compaction, S3, TLS, Kubernetes) no Quint model opens — that band is shaded
    neutral because it is intentionally outside the verified surface.],
) <fig-ch6-coverage-map>

Two things about that picture are worth saying out loud. It is a _covering_: every subsystem in
the flow carries at least one badge, so there is no part of the protocol logic that goes
unchecked. And it is an _honest_ covering: the store band is deliberately bare, because the
suite makes no claim about bytes. The rest of this chapter walks the badges one at a time, then
returns to that bare band to say precisely what is left out and why.

== The eight families, one at a time

We walk them in the order data flows: the two write models, the two read models, artifacts,
placement, then the two command-surface models. Each entry names the subsystem, states the
headline safety property in one breath, and grounds it in the model's own `allSafety`. You have
already met the machinery — actions, guards, `allSafety`, witnesses — so these are short.

=== M1 — the per-cell write path

This is the model you read line by line: `m1_cell_write.qnt`, durable per-cell mutation,
idempotency, and the writer fence. Its headline property is that a fenced former writer can
never commit a second time, and that every accepted mutation moves its edge, degree, delta,
idempotency record, and epoch as one atomic, exactly-once step. It is the foundational state
machine every other write refines, and the reading chapter covers it in full — we point back to
it rather than repeat it here. Hold it as the center of the map: the seven other families are
what you get by moving outward from a single durable write.

=== M1b — chunked bulk import

A single user request can carry many edges. Rather than admit them as one enormous transaction,
turbolay breaks the request into chunks and commits each chunk durably before starting the
next. `m1_bulk_import.qnt` models exactly one such request — five edges admitted as chunks of
two, two, and one (`m1_bulk_import.qnt:3-8`).

The property that matters is atomicity _of the unit_, not of the whole request. A chunk is
durable all-or-nothing, and if the client dies between chunks, only the completed prefix
survives — the model never invents a partial third edge. That is `chunkPrefixIsAtomic`, which
pins each `(completedChunks, insertedEdges, epoch)` triple to exactly one legal combination
(`m1_bulk_import.qnt:127-131`), paired with `retryDoesNotCreateDuplicates`
(`m1_bulk_import.qnt:133-134`) so that replaying a completed chunk or the whole request adds no
edges. The `failBetweenChunks` action is the failure boundary made explicit
(`m1_bulk_import.qnt:44-55`): progress is lost, but the durable prefix and the epoch do not
move.

=== M2 — the snapshot read path

Reads are the mirror of writes, and they have their own hazard: a query that begins, then a
write lands mid-flight, and the query must not see it. `m2_snapshot_read.qnt` is the intended
snapshot, cursor, and bookmark contract — the semantics behind the read-path findings the
formal work catalogues as BFG-001, BFG-002, and BFG-008 (`m2_snapshot_read.qnt:3-8`).

Its headline is cursor stability: a cursor stays bound to the exact graph view it captured when
it opened, and no page it returns ever reflects a later write. That is `cursorPinnedToSnapshot`
(`m2_snapshot_read.qnt:254-256`) together with `returnedPageMatchesCursor`
(`m2_snapshot_read.qnt:260-265`), which forces every returned row to come from the pinned
cursor rather than from live storage. The invariants-and-buggy-twin chapter already used this
model's broken twin to watch a checker catch a page that leaked a concurrent commit, so we send
you there for the bug story and only record the headline here.

=== M2b — snapshot lifecycle

`m2_snapshot_read` assumes a snapshot exists; `m2_snapshot_lifecycle.qnt` governs when one may
be _created_ and how it _ends_. turbolay exposes a storage snapshot only at the current graph
epoch: a future epoch has no committed state, and an older graph epoch is not a SlateDB snapshot
(`m2_snapshot_lifecycle.qnt:3-8`).

The headline is admission plus terminal cancellation. `openedSnapshotIsCurrent` says any open
snapshot's epoch equals the current epoch (`m2_snapshot_lifecycle.qnt:133-134`); future and
historical requests become typed rejections that return no page. And once a cursor is
cancelled, it never yields another page — `cancelledCursorNeverReturnsPage`
(`m2_snapshot_lifecycle.qnt:143-144`) — which makes cancellation genuinely terminal rather than
merely advisory.

=== M3 — artifacts, topology, and GC

Behind the read path, turbolay periodically freezes the topology into an immutable artifact so
that reachability queries do not have to replay every delta from the beginning. Building and
collecting those artifacts runs asynchronously alongside live writes, which is its own race.
`m3_artifact_gc.qnt` treats `canonicalReachable` — the direct traversal result — as the source
of truth, and holds every artifact to it (`m3_artifact_gc.qnt:3-8`).

Three properties carry the contract. Publication is legal only if no topology write advanced the
dirty generation after the builder copied its source: `staleBuilderCannotPublish`
(`m3_artifact_gc.qnt:263-264`). Any answer served from an artifact plus deltas must equal the
canonical traversal: `matrixEqualsCanonical` (`m3_artifact_gc.qnt:266-267`). And collection
never removes history a live read still needs: `activeReadRetainsHistory`
(`m3_artifact_gc.qnt:265`). Together they say an artifact is never stale, never wrong, and never
collected out from under a reader.

=== M4 — placement and the writer fence

Who is allowed to write? In a cluster, membership views can disagree — two nodes can each
believe they should own a cell. `m4_placement_fence.qnt` separates that local disagreement from
authority: competing candidates are allowed, but only acquiring the durable fence creates an
effective writer or commits state (`m4_placement_fence.qnt:3-5`).

The headline is that placement is not authority. `atMostOneEffectiveWriter` forbids two live
writers at once (`m4_placement_fence.qnt:188`); `activeWriterMatchesFence` ties the live writer
to whichever identity the durable fence names (`m4_placement_fence.qnt:191-193`); and
`staleWriterCannotCommit` refuses a commit from a node the fence has already superseded
(`m4_placement_fence.qnt:194`). This is the cluster-level restatement of the same fence
guarantee you met inside the single-cell write model, now with two nodes and a partition.

=== M5b — destructive commands

Deletes are where a graph store is easiest to corrupt, because a careless delete can strand
dangling edges. `m5_destructive_lifecycle.qnt` fixes the `DELETE`, `DETACH DELETE`, and cell-drop
contract (`m5_destructive_lifecycle.qnt:3-8`).

Its headline has two halves. A plain vertex delete is refused while incident edges still exist —
`rejectedDeleteDidNotMutate` guarantees the rejection changes nothing
(`m5_destructive_lifecycle.qnt:82-87`). And `DETACH DELETE` removes the vertex and every
incident edge as one atomic operation — `detachDeleteRemovesAllIncidentState` leaves no vertex
and zero incident edges behind (`m5_destructive_lifecycle.qnt:88-92`). Dropping a cell then
fences the namespace so no later write can resurrect state in it.

=== M5 — the public command surface

Finally, the surface a client actually talks to. `m5_public_commands.qnt` is a small refinement
model: it does not parse Cypher, but treats each action as the normalized behavior of one
supported command class that a model-based-testing adapter invokes (`m5_public_commands.qnt:3-11`).

#custom-box(title: [Term — Refinement model], icon: "info", color: purple)[
  A refinement model does not re-derive a subsystem from scratch; it maps a higher-level
  interface down onto semantics an existing model already checks. M5 takes public commands —
  `CREATE`, `MERGE`, `SET`, pagination, relationship delete — and expresses each as the M1/M2
  graph behavior it must produce, so the command surface is checked _against_ the write and read
  contracts rather than re-proving them. It deliberately stops at the normalized behavior; the
  Cypher grammar and the query optimizer are somebody else's tests.
]

Its headline is identity stability: a relationship keeps one immutable identity and is never
silently aliased onto a different edge. `relationshipIdentityIsStable` pins the identity and
endpoint projections whenever a relationship is present (`m5_public_commands.qnt:333-340`), and
`ambiguousRelationshipNeverAliases` guarantees that reusing an external ID at a different
endpoint is rejected rather than quietly merged (`m5_public_commands.qnt:347-352`) — the
approved BFG-003 contract. A companion invariant, `metadataRequiresStructuralEdge`, keeps
`SET` and `REMOVE` metadata honest by requiring a structural edge to hang it on
(`m5_public_commands.qnt:353-354`).

That is the whole gallery. The table below is the map in one place: each family, the subsystem
it verifies, its headline property, and the real turbolay code it abstracts.

#figure(
  table(
    columns: (0.42fr, 1.02fr, 1.78fr, 1.35fr),
    align: (left, left, left, left),
    stroke: 0.5pt + reader-colors.border,
    fill: (_, row) => if row == 0 { reader-colors.surface_soft },
    inset: 6pt,
    table.header(
      text(fill: reader-colors.text)[*Model*],
      text(fill: reader-colors.text)[*Subsystem*],
      text(fill: reader-colors.text)[*Headline safety property*],
      text(fill: reader-colors.text)[*Real turbolay code*],
    ),
    [`M1`], [Per-cell write path], [A fenced former writer can never commit twice; every mutation is atomic and exactly-once], [`write.rs`, `lifecycle.rs`],
    [`M1b`], [Chunked bulk import], [A completed chunk is durable as a unit; a retry adds no duplicate edges], [bulk-import path; `formal_p1_*` test],
    [`M2`], [Snapshot read path], [A cursor stays pinned to the view it captured; no page reflects a later write], [`query.rs`, client service],
    [`M2b`], [Snapshot lifecycle], [A snapshot opens only at the current epoch; a cancelled cursor never returns a page], [`snapshot_at`; `formal_p2_*` test],
    [`M3`], [Artifacts, topology & GC], [A stale builder cannot publish; every matrix read equals the canonical answer; GC never drops referenced history], [`artifact_build.rs`, `traversal.rs`, `artifact_gc.rs`],
    [`M4`], [Placement & writer fence], [At most one effective writer; only fence acquisition commits state], [`cluster.rs`, `lifecycle.rs`],
    [`M5b`], [Destructive commands], [Plain vertex delete refused while edges are incident; `DETACH DELETE` removes vertex and edges atomically], [`DELETE` / `DETACH DELETE` / drop; `formal_p2_*`],
    [`M5`], [Public command surface], [A relationship identity is stable and never silently aliased; metadata needs a structural edge], [`query.rs`, Bolt / HTTP adapters],
  ),
  caption: [The eight turbolay Quint model families. The subsystem column matches the badges in
    the coverage map; the headline column is each model's `allSafety` in one sentence; the code
    column lists the real correspondence (files under `src/shard` and `src/engine`, and the
    focused Rust conformance tests) recorded in the specification and API-coverage plans
    (`0002-turbolay-quint-specification-plan.md:77-83`).],
) <tab-ch6-model-gallery>

== What the suite does not model

A map is only trustworthy if it marks its own edges. The coverage map's store band was left bare
on purpose, and this is where we say exactly what that means. The formal-methods objective
commits to a shared-state abstraction and then lists what it throws away: byte encoding, LSM
compaction, S3's implementation, Rust scheduling, TLS, parsing, and Kubernetes internals. Those
are not oversights. They are concerns with _better_ tools than a state machine.

#custom-box(title: [Term — Protocol-logic core], icon: "info", color: purple)[
  The protocol-logic core is the part of turbolay's correctness that is about the _shape_ of its
  transitions — which operations are enabled, what they commit atomically, what they refuse, how
  epochs and fences order them — rather than about bytes, timing, or syntax. It is exactly the
  part a small exhaustively-checkable state machine can judge better than any amount of
  integration testing, and it is exactly what these eight Quint families cover.
]

Everything the suite omits has a home elsewhere. Key encoding, codecs, and roaring-bitmap layout
go to Rust property tests. SlateDB's atomic commit and its fencing primitive are an upstream
contract exercised by integration tests and, later, by Jepsen. Real S3 latency, throttling, and
partial failures go to MinIO fault tests and a cloud soak. Cypher grammar and optimizer choices
stay in the query and compatibility tests — M5 stops at the normalized behavior precisely so it
does not pretend to be a parser. And Kubernetes scheduling is below M4's abstraction: the model
takes "membership can disagree" as a given and checks that disagreement cannot create two
writers, without modeling how the disagreement arose.

#custom-box(title: [Why], icon: "tip", color: rgb("#c99700"))[
  The suite is also honest about where its coverage is intentionally _shallow_. Read-only handles
  carry no freshness or read-your-writes promise — the bookmark contract is monotonic-or-error
  and nothing more, and a real freshness guarantee would need a separate change-log protocol that
  does not exist yet (`0004-api-coverage-completion-priority.md:31,66`). Low-level direct offset
  pagination is explicitly best-effort across requests; only the server-materialized cursor is
  stable, which is why M2's `directPageUsesOneRequestView` asks so little of a direct page
  (`m2_snapshot_read.qnt:280-285`). Marking these shallow spots is part of the map, not an
  apology for it: a reader who knows a guarantee is only safety-only will not lean on a freshness
  it was never promised.
]

So the honest summary is this. These eight families are the protocol-logic core, checked by
Quint. They are a covering of the write path, bulk import, snapshot reads, snapshot lifecycle,
artifacts and GC, placement and fencing, destructive commands, and the public command surface.
They are not a covering of bytes, timing, or syntax, and they say so.

== Where this leaves us

You can now stand in front of turbolay and place any concern. A lost write reply, a retried
bulk chunk, a cursor that must not see a concurrent commit, a stale artifact, two nodes fighting
over a cell, a dangling-edge delete, a reused relationship ID — each has a named model, a
subsystem, and a one-sentence guarantee, and you can find it on the coverage map. Just as
important, you can name what has _no_ Quint model and say which other kind of test owns it.

That closes the middle of this book. We have built intuition, learned to read a model, watched a
checker judge one and catch its broken twin, walked deterministic scenarios, and now mapped the
whole verified surface. Two questions remain, and they are the subject of the final part. First:
`quint run` walks a huge sample of behaviors, but a sample is not a proof — what does it take to
turn "no counterexample found in ten thousand runs" into "no counterexample exists within a
bound"? That is the bounded-proof chapter, where Apalache and `quint verify` replace sampling
with an exhaustive symbolic check. Second: a model is only a promise about the code until
something forces the code to keep it — what does it take to drive these exact eight models
against the running turbolay kernel and fail the build when reality diverges? That is the
model-based-testing chapter, where the Rust harness replays each model's traces against a live
`GraphShard` and compares state after every action. The map you now hold is what both of those
chapters make real.
