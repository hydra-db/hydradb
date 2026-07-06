#import "../vendor/bookly/src/bookly.typ": *

= The Writer/Reader Split

The freshness gate said the *reader* waits until it has caught up. All through Part
I we have said "the writer" and "the readers" as if they were different machines.
They are — and seeing why closes the whole story.

The answer to "who is the reader?" is: *a stateless cache over shared S3*. That
phrase is the last brick in the arch.

== The data is in S3; a reader is a lens on it

All the *authoritative* state — the log and the compacted tables — lives once, in
the object store, one copy per namespace. A reader holds nothing durable: a cache,
a replay cursor, and the fold logic. Everything it knows is reconstructible from S3.

So a reader is *disposable*. Lose one and a survivor serves the same bytes, because
the bytes are in S3, not in the reader. Add ten and they share the one copy —
nothing is replicated. The graph exists once, in the object store; a reader is an
instrument pointed at it, focusing (caching, folding the log to a position) but
holding no original. Smash the lens, grab another; the thing being viewed never
moved.

#note[
  "Stateless" does not mean *contentless*. A reader has a warm cache and a replay
  position and a bounded lag — that is what makes it fast. It is stateless in the
  sense that matters: it holds no data that isn't recoverable from S3.
]

== Replaying the log

What is a reader *doing*, moment to moment? SlateDB provides a read-only handle — a
`DbReader` — that polls the manifest on an interval and replays newly-durable data,
advancing its own `durable_seq`. A reader is forever *chasing the writer's latest
durable state*, catching up one poll at a time.

And this is where the token from last chapter clicks into place. Because the writer
injects its logical `seq` as SlateDB's durable sequence number, a reader's replay
position is measured in the *same units* as a session token — so the gate's
comparison, `durable_seq >= token`, is exact. The one piece of all this that is
built today is precisely that subscription: a channel that reports a reader's
`durable_seq` as it advances.

== Same binary, two roles

The design is one process type with a flag. `--role writer` opens the single fenced,
writable database for a namespace and serves writes; `--role reader` opens a
read-only `DbReader` and serves reads only. One writer per namespace, as many
readers as you like — the same binary, the flag deciding which routes it exposes.

== Reads scale, writes don't

A read never touches the writer. It hits a reader, which answers from its cache and
S3. Add readers and read throughput grows; the writer is untouched, because readers
*poll S3 — they do not call the writer*. There is no coordination to add.

This is the deliberate counterweight to the write ceiling from chapter I·6. The two
bounds are the *same decision* seen from opposite sides: capping the namespace to
one writer is what let us delete all the coordination machinery, and keeping the one
durable copy on shared S3 is what lets reads scale without copying anything.

The contrast with Dgraph is the sharpest way to feel it:

#figure(
  kind: image,
  supplement: [Figure],
  block(inset: 4pt)[
    #set text(size: 8.8pt)
    #table(
      columns: (auto, 1fr, 1fr),
      align: (left, left, left),
      stroke: none,
      inset: (x: 8pt, y: 5pt),
      table.hline(),
      table.header([], [*Dgraph replica*], [*turbolay reader*]),
      table.hline(),
      [Holds the data?], [yes — a full copy on local disk], [no — a cache over the one S3 copy],
      [Adding a node], [replicate the whole dataset (Raft)], [replicate *nothing*; warm a cache],
      [Losing a node], [lose a copy; rebuild it elsewhere], [lose a cache; a survivor serves],
      [Coordinated by], [Raft apply-index + the timestamp oracle], [a replay position + a client token],
      table.hline(),
    )
  ],
  caption: [Dgraph scales reads by replicating the *data* (stateful Raft replicas
    holding copies); turbolay scales reads by replicating *compute* over one shared
    S3 copy (stateless caches). That difference is storage/compute separation.],
) <fig-fleet>

== Where Part I lands

Stand back, and the three moves of Part I turn out to be one idea. The log lives on
S3. Exactly one writer appends to it. Readers are stateless caches that replay it.
Put together, *storage is durable and shared, while compute is cheap and
disposable* — and the two split cleanly across the S3 boundary.

That is the whole architecture, and now every word of the one-sentence version from
the opening is earned:

#boxeq[
  Dgraph's storage model, on S3 — its distributed half deleted, because one writer
  per namespace makes a single sequence authoritative *by construction*, and S3
  makes the one durable copy shareable, so readers never need their own.
]

The write ceiling and the read elasticity are not two facts; they are one decision
seen twice. Everything Dgraph runs a cluster to *manufacture* — a global clock,
conflict detection, versioned keys, replicated consensus — turbolay *deletes*, and
buys back the one thing it still needs, a consistent read while writes proceed, from
a replay position and a bounded tail scan.

#note[
  Honest status: most of this chapter is the road ahead. The reader fleet, the
  `--role` flag, the HTTP service, and the freshness gate itself are the design in
  RFC 0008 (milestone M3); the single built piece today is the durable-sequence
  subscription that the gate will consume. The architecture is settled; the service
  around it is still being built.
]

Part I set out to build intuition — enough of it that you could stand at a
whiteboard and explain turbolay, and want to. If the mental model is now yours — a
graph as sets of UIDs, a database as a log folded on S3, one writer deleting a world
of coordination — then it has done its job.

*Part II* takes every idea in this track down to its bytes: the codecs, the exact
key layouts, the trait signatures, the merge operators. Same architecture, now in
the form the person building it needs. The intuition was the hard part. The rest is
engineering.
