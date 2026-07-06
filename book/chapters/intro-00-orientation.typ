#import "../vendor/bookly/src/bookly.typ": *

= The One Sentence

You already know what a graph database is. You have seen Neo4j chase pointers
through a native store, Dgraph shard a trillion edges across a Raft cluster,
FalkorDB turn a traversal into a matrix multiply. Every one of them *owns its
storage*: bespoke file formats, page layouts, a compaction thread, a team.

#question-box[
  Now take all of that away. You are given exactly two things: an *S3 bucket* —
  a dumb store that maps a key to a blob of bytes — and one rule, that *only one
  machine may write at a time*. No custom disk format. No cluster consensus. No
  storage team. Could you still build a real, correct, queryable property-graph
  database? And if you could — how small would it be?
]

turbolay is the answer, and it fits in one sentence:

#boxeq[
  *turbolay is Dgraph's storage model, reimplemented on SlateDB, with the entire
  distributed half deleted — because there is exactly one writer per namespace.*
]

Read that as three moves, and the whole book is their consequences.

- *Keep Dgraph's idea.* Dgraph already proved the hard thing — that you can run a
  scalable property graph on top of a plain ordered key-value store, with nothing
  graph-aware in the storage layer at all. We keep that idea and almost all of its
  machinery.
- *Rent the substrate.* We do not write a storage engine. SlateDB — an LSM tree
  that lives on S3 — owns compaction, caching, and the object lifecycle. We hand
  it the generic, hard half and port only the graph-specific half on top.
- *Delete the distributed half.* Dgraph is mostly a distributed system: Raft, the
  Zero timestamp oracle, MVCC versioning, conflict resolution. *One writer per
  namespace makes all of it unnecessary.* Writes serialize by construction, so
  most of Dgraph's code simply evaporates.

#note[
  One writer is the lever the whole design pulls on: it deletes hard problems
  rather than solving them.
]

That is the mental model. The rest of the book is two tracks through it. *Part I,
the track you are reading,* builds the intuition from first principles — enough to
explain turbolay at a whiteboard. *Part II* takes each idea down to its bytes,
code, and RFC, for the reader who is going to build it.

We start with the single idea Dgraph's storage model reduces to once you strip
everything else away: a graph is just *sets of numbers*.
