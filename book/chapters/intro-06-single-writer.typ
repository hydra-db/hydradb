#import "../vendor/bookly/src/bookly.typ": *

= What One Writer Deletes

For five chapters we treated "one writer per namespace" as a convenience — the
thing that makes a read-modify-write safe, that gives the sequence one clean
lineage. It is time to see it for what it really is: the most consequential
decision in the project, and the reason turbolay is a fraction of the size of the
system it is modelled on.

Because here is the truth about Dgraph — most of it is not about graphs.

== The machinery Dgraph runs to stay consistent

To be a correct distributed graph database, Dgraph runs four heavy subsystems
underneath the storage model turbolay borrows:

- *Raft*, per predicate group. Every write is replicated to a majority of the
  group's nodes before it is acknowledged — buying durability, instant failover to
  a hot follower, and a single agreed order within the group.
- *Zero*, the control plane, really three jobs in one: a *timestamp oracle* that
  hands every transaction a globally-ordered start and commit timestamp; a *tablet
  directory* deciding which machine owns which predicate, and moving hot ones; and
  a *UID lease* doling out id ranges.
- *MVCC.* Every version of every posting is stamped with the timestamp that wrote
  it, so a reader can hold a stable snapshot while writers commit newer versions.
- *Conflict detection.* At commit, a transaction's touched keys are checked against
  everyone else's; if two overlapped, one is aborted and must retry.

Stare at that list. Not one of those subsystems is about *storing a graph*. Raft,
the oracle, the versions, the conflict checks — every one exists to referee
*concurrency*: the machinery a system needs when many writers touch the same data
at once and must be stopped from corrupting each other.

== The one decision

turbolay makes a single decision, and the whole apparatus falls away.

#boxeq[
  *One writer per namespace.* With one writer, the writes are already in an order —
  the order the writer performed them. The single agreed sequence that Dgraph
  spends Raft, an oracle, versioned postings, and conflict detection to
  *manufacture*, turbolay gets for free, by construction.
]

Think of the log as a ledger. Dgraph's ledger is written by many accountants at
once, so it needs synchronized clocks, version columns, and a supervisor who voids
clashing entries. turbolay's ledger is written by *one hand*: entry #super[n+1]
follows entry #super[n] because the same hand wrote both, in that order —
`seq = max + 1`, chosen locally, in memory. No clock to synchronize, no version
column, no supervisor.

== Going through the deletions

Take the subsystems one at a time; each deletion has its own reason.

*The timestamp oracle* exists to invent an order across writers that cannot see
each other — two machines committing at the same instant have no agreed "which came
first," so they both ask a central clock. With one writer there is nothing to
invent. The oracle refereed a race that no longer runs.

*Conflict detection* is the sharpest deletion, because nothing replaces it — it is
rendered *meaningless*. It aborts a transaction when a concurrent one touched the
same keys. With one writer there is no concurrent transaction; the precondition for
a conflict cannot occur. There is nothing to detect and nothing to abort.

*MVCC's versions* let a reader hold a snapshot while writers commit newer versions —
the timestamps keep the versions apart. One writer means two versions of a key
never compete, so each key holds exactly one live value and the version stamps
vanish from the encoding. (Read consistency does not vanish — it *moves*. Where it
goes is the last thread of this chapter.)

*Raft* did three jobs, and turbolay re-sources each without it: durability and
replication are S3's — a write is durable when its object lands, eleven-nines
replicated by the store; order is the one writer's `seq`; and failover, the one job
that genuinely still needs *something*, is handled by the single scrap of
coordination that survives, below. *Zero's* other two jobs go the same way — the
UID lease becomes a local counter (one writer owns the whole id space alone), and
the tablet directory becomes nothing at all, because a namespace *is* one database
on one writer: there is no predicate to place and no hot tablet to move. A supernode
is handled inside the one keyspace by splitting its posting into new keys, not by
relocating it across machines.

#figure(
  kind: image,
  supplement: [Figure],
  block(inset: 4pt)[
    #set text(size: 8.6pt)
    #table(
      columns: (auto, 1fr, 1fr),
      align: (left, left, left),
      stroke: none,
      inset: (x: 7pt, y: 4pt),
      table.hline(),
      table.header([*Deleted from Dgraph*], [*Why one writer makes it needless*], [*What turbolay uses instead*]),
      table.hline(),
      [Raft consensus], [S3 is already durable + replicated; order comes from one writer], [S3 durability + `seq = max+1` + the fence],
      [Zero timestamp oracle], [the writer's own order *is* the order; nothing to invent], [local `seq`, injected as the durable seqnum],
      [Zero tablets + UID lease], [one namespace = one database; nothing to place or lease], [one DB per namespace; a local UID counter],
      [MVCC versions-in-key], [no write-write races; one live value per key], [single version; consistency moves to the read path],
      [Conflict detection (OCC)], [no concurrent transaction can exist], [*nothing* — the case cannot occur],
      table.hline(),
    )
  ],
  caption: [The deletion ledger. Every row is coordination machinery for *many
    writers*; each is deleted by having *one*. Only the last column's final entry
    is truly "nothing" — a mechanism not replaced but rendered impossible.],
) <fig-deletion>

== The one thing that survives

Delete all of it and exactly one grain of coordination remains — and it is *not*
consensus.

When a writer opens a namespace it stamps its identity — an *epoch* — into the
manifest with a conditional write. If a second writer starts, it stamps a higher
epoch. The first writer finds out only when it next tries to commit: its conditional
write fails, its handle closes, it can no longer write.

#boxeq[
  One conditional write, on one object. No election, no quorum, no votes. This is
  the minimum viable coordination — the entire residue of Dgraph's Raft-and-Zero
  stack.
]

What is it *for*? Single-writer deletes conflicts by *assumption*; the fence
*enforces the assumption*. It guarantees that even during a messy redeploy, where an
old writer and a new one briefly overlap, only one of them can commit — so the
sequence can never fork. It does not elect a leader or keep a hot standby. It only
makes sure a zombie writer cannot corrupt the one true lineage.

== What it costs

This is a subtraction, and subtractions have a price. turbolay names three, and I
will not soften them.

- *A write ceiling.* Every write to a namespace goes through one machine, one at a
  time; there is no writing a namespace in parallel. It is a single-lane bridge with
  a gate — order is just arrival order, collisions never happen, and the price is
  throughput. To move more traffic you build more bridges (more namespaces), not
  more lanes.
- *No multi-statement transactions.* turbolay gives complete atomicity *within* one
  request's fan-out — the whole edge, both directions, the changelog, at one `seq`,
  all-or-nothing. But there is no `BEGIN … COMMIT` spanning several round-trips,
  reading and writing and aborting late on conflict. Dgraph has that; turbolay
  deliberately does not.
- *Failover is restart-and-replay, not instant.* A crashed writer means a new one
  opens, claims the epoch, and rebuilds its counters from S3 before it serves.
  Dgraph's hot followers take over in a heartbeat; turbolay pauses.

And a precision that matters, because it is the easiest thing to overclaim: this
does *not* make turbolay linearizable or serializable. What it offers is
*read-your-writes* — write, receive a token, and a read carrying that token waits
until it can honour it — plus bounded staleness. That is a real, useful contract. It
is not Dgraph's snapshot isolation, and the book will not pretend it is.

You get scale back by sharding on the *namespace*: one writer per graph, many
graphs, write throughput growing as you add them — while reads scale independently
on a stateless reader fleet that never touches the writer. The target workload,
knowledge graphs of a few million nodes each, is chosen to sit under one writer's
ceiling. And many-writers is deferred, not foreclosed: a written-down future with a
named trigger, for the day a single graph outgrows one writer.

#boxeq[
  turbolay did not out-engineer Dgraph's consensus. It *changed the problem* so
  consensus is not needed — and paid with a write ceiling, no cross-request
  transactions, and slower failover.
]

== Next: the structures that lag

One deletion left a debt. MVCC's versions were how a read stayed consistent while
writes kept coming; we deleted them and said the consistency "moves to the read
path." It does — and it leans on an idea we have not built yet.

A real query rarely walks raw adjacency alone. It asks for "entities *named* Ada,"
"chunks with *more than five* mentions" — filters on properties and degrees. Those
need *indexes*. And indexes turn out to be just more posting lists — with one new,
faintly alarming property: they are allowed to *lag* behind the log.

#question-box[
  If an index can trail the latest writes, how can a read that uses it ever be
  correct? And what *are* these indexes, when everything so far has been a set of
  UIDs? The first answer is the consistency trick that replaces everything we
  deleted in this chapter — but it depends on the second. Indexes first.
]

That is the next chapter.
