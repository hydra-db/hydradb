#import "../vendor/bookly/src/bookly.typ": *

= The Substrate: SlateDB on S3

For three chapters we have leaned on a thing called SlateDB without saying what it
is. We said compaction rewrites whole values, that a split is just a new key, that
one writer commits one atomic batch — all promissory notes against a substrate we
never described. Time to describe it, because the choice of substrate *is* the
project. The name says so: graph, on S3.

The bargain at the center of turbolay is a refusal to build a storage engine:

#boxeq[
  Rent the hard, generic half — an LSM tree that lives on S3 — and spend the whole
  budget on the graph half instead.
]

== SlateDB is a boring LSM on S3

SlateDB is an ordered key #sym.arrow.r value store — `get`, `put`, `scan`, `merge`,
nothing graph-aware — built as a log-structured merge tree. You already know the
shape of an LSM: writes land in an in-memory table, get flushed to immutable sorted
files, and background *compaction* merges those files to keep reads fast. SlateDB's
one twist is where the files live:

#info-box(title: none)[
  Every persistent structure — the write-ahead log, every sorted table, the
  manifest that lists them — is an *S3 object*. There is no local disk in the
  picture. The database is a *bucket*.
]

That single move cashes the mental model from the very first chapter — *a durable
log on S3, folded by compaction into queryable state.* SlateDB's write-ahead log is
that durable log; its compaction is that fold; S3 is the disk. turbolay's job is
only to decide what bytes go in and how they are keyed.

#note[
  One correction to your local-LSM instinct up front: SlateDB does *not* PUT an
  object per write. Writes are batched into a WAL object every ~100 ms, and a
  durable write waits for that one object to land. Durability latency is roughly
  one flush interval, not one write.
]

== Why put a database on object storage

S3 is a strange choice for a database's disk, and the trade is stark. What you gain
is enormous: eleven nines of durability, elastic and effectively unbounded capacity,
storage fully separated from compute (so readers are stateless and disposable), and
a per-gigabyte price nothing else touches — with no disks, no replication, no fsck
to operate. What you pay is latency and a meter: a cold read is a network GET of
*tens of milliseconds*, you can never patch bytes in place, and you are billed *per
request*.

#tip-box[
  On S3 the scarce resource is *requests, not bytes*. Storage is nearly free;
  every GET and PUT is on the bill. The enemy the whole system is tuned against is
  *cold first-hop latency* — the query that misses the cache and has to wait on S3.
]

Why is that a good trade *here*? Because the target workload — a RAG knowledge
graph — is read-heavy, correctness-first, and latency-tolerant: it would rather be
cheap, durable, and elastic than shave milliseconds. An LSM already pushes work off
the write path, which suits batched-durable writes on slow storage, and reads are
served from cache with S3 as the cold backstop. You rent a maintained, S3-native
engine and spend the saved effort on the graph.

== What changes when the disk is S3

Carry three corrections into the rest of the book, or the cost model will mislead
you:

- *One table is one whole object.* No `fsync`, no page cache, no patching a file in
  place. Growth is always *additive* — new objects, old ones dereferenced and
  garbage-collected later.
- *The cache and the bloom filter are load-bearing, not optimizations.* On local
  disk a cache miss is a page fault; on S3 it is a network round-trip. A bloom
  filter that lets a lookup skip an entire table object is skipping a whole GET.
- *Compaction re-PUTs whole values.* This is the fact that bites next.

== Rent the engine, port the graph

The division of labor is the reason turbolay is small. SlateDB owns the hard generic
machinery; turbolay ports only the graph-specific layer on top; and a whole category
of Dgraph code is simply *deleted*.

#figure(
  kind: image,
  supplement: [Figure],
  block(width: 100%)[
    #let layer(fill, body) = box(width: 100%, inset: 8pt, radius: 3pt,
      fill: fill, stroke: 0.5pt + luma(55%), body)
    #set text(size: 9pt)
    #stack(dir: ttb, spacing: 5pt,
      layer(rgb("#f7e5c2"))[*turbolay* — the graph half: key layout, roaring posting
        lists, node blobs, the changelog #h(1fr) _ported / built_],
      layer(rgb("#eceff2"))[*`common::Storage`* — one thin wrapper; the rest of the
        code never touches SlateDB directly],
      layer(rgb("#d3e3f7"))[*SlateDB* — LSM on object storage: WAL, memtable, sorted
        tables, compaction, bloom + block cache, writer fence #h(1fr) _rented,
        unmodified_],
      layer(rgb("#cfe9d5"))[*S3* — durable, elastic object storage: the actual bytes],
    )
  ],
  caption: [The stack. turbolay ports the graph layer onto a rented, unmodified
    SlateDB, and reaches it only through one `common::Storage` wrapper — never the
    raw SlateDB API. Below the wrapper, nothing is graph-aware.],
) <fig-stack>

The one-wrapper rule matters more than it looks: SlateDB is pre-1.0 and its Rust API
will churn, so the entire codebase talks to `common::Storage` and never to SlateDB
directly. An upstream API break is then contained to one module instead of scattered
across the graph engine. And what turbolay ports from Dgraph — the key layout, the
posting-list model — moves over nearly verbatim, because it was always agnostic to
Badger and Raft underneath.

#figure(
  kind: image,
  supplement: [Figure],
  block(inset: 4pt)[
    #set text(size: 8.6pt)
    #table(
      columns: (1fr, 1fr, 1fr),
      align: (left, left, left),
      stroke: none,
      inset: (x: 7pt, y: 4pt),
      table.hline(),
      table.header([*SlateDB owns*], [*turbolay ports*], [*turbolay deletes*]),
      table.hline(),
      [LSM: WAL, memtable, tables], [Order-preserving key layout], [Badger's *value log*],
      [Compaction, space/read-amp], [Posting model, split, rollup], [UidPack codec #sym.arrow.r roaring],
      [Block cache, bloom filters], [Node blob + write fan-out], [`algo/uidlist` intersect #sym.arrow.r roaring],
      [Manifest, *writer fence*], [Logical `seq` + consistency], [Raft, Zero oracle, MVCC #super[†]],
      table.hline(),
    )
    #set text(size: 8pt)
    #super[†] deleted by having one writer — the subject of a later chapter.
  ],
  caption: [Owns, ports, deletes. The rightmost column is the payoff of building on
    an LSM store and having a single writer.],
) <fig-division>

== No value log

Here is the single most consequential thing SlateDB does *not* do, and the reason
for the caps from the last chapter.

Dgraph runs on Badger, which uses *key–value separation* (the WiscKey design): large
values are pulled out into a separate append-only *value log*, and the LSM tree
stores only small *pointers* to them. Compaction then shuffles cheap pointers
instead of rewriting fat values. It is a real, useful piece of machinery — and
building or porting it is real work.

SlateDB deliberately does none of it. Values are stored *inline*, right next to
their keys in the sorted table. The consequence is blunt:

#warning-box[
  With no value separation, *compaction rewrites the whole value every time it
  touches the row.* A 900 KiB posting compacted through three levels is copied,
  in full, three times. A large value is not just big — it is repeatedly, expensively
  re-copied.
]

This is exactly why the two limits from the last chapter exist. A node is *capped*
at 1 MiB and oversize writes are rejected, so a giant blob never enters the rewrite
loop. Adjacency is *split* at 512 KiB into new part-keys, so a supernode's edges
grow as new objects rather than one ever-rewritten value. Both caps are turbolay's
own policy against compaction cost — SlateDB itself would happily store values up to
4 GiB.

And the headline: where Dgraph *builds* a value log, turbolay *deletes* it. There is
nothing to port and nothing to garbage-collect, because there is no second log —
just inline values, a size cap, and a posting split, all cheap on S3 because a new
part is just a new key.

== One writer, one fence

turbolay allows exactly one writer per namespace, and the entire enforcement is one
field in the manifest.

The manifest — the object that lists the current tables — carries a `writer_epoch`.
When a writer opens the database it bumps that epoch and writes the manifest back
with a *conditional* update: the write succeeds only if the manifest is still the
version it read. A second writer that starts up bumps the epoch again. Now the first
writer is stale, and it finds out the next time it tries to commit: its conditional
manifest update fails, its handle closes as *fenced*, and it can no longer write.

#boxeq[
  One conditional write on one S3 object decides which writer is real. No election,
  no quorum, no ZooKeeper, no lock service.
]

That is the whole coordination substrate. It guarantees a single authoritative
sequence of writes per namespace — so the `seq` a write returns belongs to one
lineage and can never be forked by a zombie writer assigning the same number twice.

#note[
  Honest detail: the fence is *detect-on-next-write*, not admission control.
  SlateDB does not stop a second writer from opening; it lets the loser run until
  its next commit fails. There is a bounded window where a deposed writer holds a
  live handle but cannot durably commit. Fencing protects the *log*, not the
  loser's feelings.
]

This one fence is also the thread the rest of the design pulls on. Having exactly
one writer is what lets turbolay delete Raft, the timestamp oracle, and MVCC
versioning outright — but that is a big enough idea to be its own chapter, two
chapters from now.

== Next: follow a write down

We have the substrate: a boring LSM whose disk is S3, rented unmodified, reached
through one wrapper, fenced to a single writer. We know a write becomes one atomic
batch that lands as an S3 object. But we have never actually *followed* one — what
does a single "add this edge" turn into on the way down?

#question-box[
  When a user upserts one edge, what exactly gets written, in one atomic batch? We
  said an edge is stored twice, that UIDs come from an allocator, that a changelog
  entry is recorded and a sequence number handed back. How do all of those become
  *one* write that either wholly happens or wholly does not?
]

That is the write path, and it is the next chapter.
