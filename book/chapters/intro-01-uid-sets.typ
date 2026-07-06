#import "../vendor/bookly/src/bookly.typ": *
#import "@preview/fletcher:0.5.8" as fletcher: diagram, node, edge

= Everything Is a UID Set

Here is our graph. It is small enough to hold in your head, and we will carry it
through the entire book:

#info-box(title: [The cast])[
  A knowledge graph built from a research paper.
  - `source_1` — the paper, "paper-alpha".
  - `chunk_10`, `chunk_11` — two passages cut from it.
  - `entity_7` — *Ada Lovelace*. `entity_19` — the *Analytical Engine*.
    `entity_88` — *Charles Babbage*.

  And the facts between them: `source_1` *has* both chunks; `chunk_10` *mentions*
  Ada and the Engine; `chunk_11` *mentions* the Engine and Babbage; Ada *relates
  to* Babbage; the Engine *relates to* Babbage.
]

#figure(
  diagram(
    node-stroke: 0.9pt,
    node-inset: 5pt,
    spacing: (13mm, 7mm),

    node((0, 1), align(center)[*1*\ #text(0.6em, raw("source_1"))], name: <src>,
      shape: circle, fill: rgb("#cfe9d5"), stroke: rgb("#2f8a4f") + 0.9pt),

    node((1.5, 0), align(center)[*2*\ #text(0.6em, raw("chunk_10"))], name: <c10>,
      shape: circle, fill: rgb("#d3e3f7"), stroke: rgb("#3b6fb0") + 0.9pt),
    node((1.5, 2), align(center)[*3*\ #text(0.6em, raw("chunk_11"))], name: <c11>,
      shape: circle, fill: rgb("#d3e3f7"), stroke: rgb("#3b6fb0") + 0.9pt),

    node((3.1, -0.4), align(center)[*4*\ #text(0.6em, raw("Ada"))], name: <ada>,
      shape: circle, fill: rgb("#f7e5c2"), stroke: rgb("#c8791f") + 0.9pt),
    node((3.9, 1), align(center)[*5*\ #text(0.6em, raw("Engine"))], name: <eng>,
      shape: circle, fill: rgb("#f7e5c2"), stroke: rgb("#c8791f") + 0.9pt),
    node((3.1, 2.4), align(center)[*6*\ #text(0.6em, raw("Babbage"))], name: <bab>,
      shape: circle, fill: rgb("#f7e5c2"), stroke: rgb("#c8791f") + 0.9pt),

    // HAS_CHUNK
    edge(<src>, <c10>, "-|>", stroke: rgb("#8a8f98") + 1pt),
    edge(<src>, <c11>, "-|>", stroke: rgb("#8a8f98") + 1pt),
    // MENTIONS
    edge(<c10>, <ada>, "-|>", stroke: rgb("#3b6fb0") + 1pt),
    edge(<c10>, <eng>, "-|>", stroke: rgb("#3b6fb0") + 1pt),
    edge(<c11>, <eng>, "-|>", stroke: rgb("#3b6fb0") + 1pt),
    edge(<c11>, <bab>, "-|>", stroke: rgb("#3b6fb0") + 1pt),
    // RELATES
    edge(<ada>, <bab>, "-|>", stroke: rgb("#c8791f") + 1pt),
    edge(<eng>, <bab>, "-|>", stroke: rgb("#c8791f") + 1pt),
  ),
  caption: [The cast as a property graph — six nodes, their UIDs, and the facts
    between them. #box(baseline: 0.15em, line(length: 1.1em, stroke: rgb("#8a8f98") + 1.4pt))~HAS\_CHUNK
    #h(0.5em) #box(baseline: 0.15em, line(length: 1.1em, stroke: rgb("#3b6fb0") + 1.4pt))~MENTIONS
    #h(0.5em) #box(baseline: 0.15em, line(length: 1.1em, stroke: rgb("#c8791f") + 1.4pt))~RELATES],
) <fig-cast>

This is a tangle of arrows between people, passages, and a paper. Our only storage
is an S3 bucket, which is a *flat dictionary*: you hand it a key, it hands you back
a blob. It knows nothing about arrows.

#question-box[
  How do you fold a tangle of relationships into a flat key #sym.arrow.r blob
  dictionary — so that a real question, like *"which entities does `chunk_10`
  mention that also relate to Babbage?"*, is answered by a couple of fast lookups
  instead of a crawl over the whole graph?
]

The rest of this chapter is one idea, reused until it covers the whole system.

== Three ways to store adjacency

The famous graph engines answer that question differently. Ask each the same
thing: *what is a single hop — following one arrow — physically?*

- *Neo4j — a pointer chase.* Every node record stores the disk address of its
  first relationship; every relationship stores the address of the next. A hop is
  a pointer dereference: read an address, jump there. No search, no lookup —
  "index-free adjacency," and fast on one machine.
- *FalkorDB — a matrix multiply.* Adjacency is a sparse boolean matrix $A$, where
  $A[i,j] = 1$ means "$i$ points to $j$." A hop advances the whole current set of
  nodes at once, as a matrix multiply (the linear algebra of GraphBLAS).
- *Dgraph, and so turbolay — a set intersection.* A node's neighbors are stored
  as a plain *sorted set of numbers*. A hop reads a set; combining hops is set
  arithmetic.

#note[
  One word each: *dereference*, *multiply*, *intersect*.
]

turbolay takes the third option. The reason it can comes down to one decision at
the bottom of the system.

== Number everything

Give every node a number — a 64-bit unsigned integer, its *UID*. Nothing clever,
just a dense count:

#info-box(title: none)[
  #set align(center)
  `source_1`#sym.arrow.r *1* #h(1em) `chunk_10`#sym.arrow.r *2* #h(1em)
  `chunk_11`#sym.arrow.r *3* #h(1em)
  Ada#sym.arrow.r *4* #h(1em) Engine#sym.arrow.r *5* #h(1em) Babbage#sym.arrow.r *6*
]

Once nodes are numbers, a node's neighbors along one kind of edge are just a *set
of numbers* — an ordinary blob you can park at a key. So we store adjacency like
this:

#boxeq[
  #set align(center)
  key: `EdgeOut[ chunk_10 ][ MENTIONS ]` #h(1.2em)#sym.arrow.r#h(1.2em)
  value: #raw("{ 4, 5 }")
]

The key names *whose* edges and *which kind* — subject `chunk_10`, predicate
`MENTIONS`. The value is the set of everyone on the other end. This stored set is
a *posting list*, a term we inherit from Dgraph. Picture it as a *sorted shelf of
numbered ID cards*: `4`, then `5`, in order. The graph is now a dictionary of
shelves.

#tip-box[
  We store every edge *twice*, once each way. Alongside
  `EdgeOut[chunk_10][MENTIONS] = {4,5}` sits `EdgeIn[Ada][MENTIONS] = {2,...}`, the
  set of chunks that mention Ada. One arrow, two shelves — so you can walk in
  either direction at the same cost. Storage is cheap; inverting an edge at query
  time is not.
]

== An edge is set membership

In most databases an edge is a *record*: a row with a source, a target, maybe an
id, that you fetch. turbolay has no such record. The edge `chunk_10 -MENTIONS->
Ada` is stored *nowhere as an edge*. It exists only as a fact about a set:

#boxeq[
  *`4` #sym.in `EdgeOut[chunk_10][MENTIONS]`.*
  The edge is the membership. Present means the edge exists; absent means it does
  not.
]

Deleting the edge does not find and erase a record. It drops `4` into a separate
*tombstone* set, and every read subtracts the tombstones (covered later). A delete
costs the same whether the node has three neighbors or three million.

== Queries are set algebra

If neighbors are sets, questions are arithmetic on sets. There are three
operations, each with a job.

Start with expansion — following an edge outward from a frontier of nodes. Take
both chunks, `{2, 3}`, and expand `MENTIONS`:

#info-box(title: none)[
  `neighbors(chunk_10) = {4, 5}` #h(0.6em) and #h(0.6em) `neighbors(chunk_11) = {5, 6}` \
  frontier after the hop #sym.arrow.r `{4,5}` #sym.union `{5,6}` = *`{4, 5, 6}`* \
  #emph[(Ada, the Engine, and Babbage — everyone either chunk mentions)]
]

Expanding a frontier is a *union*: a forward walk unions neighbor shelves, it does
not intersect them. Intersection appears when you *constrain* instead of expand —
"entities mentioned by `chunk_10` *and* `chunk_11`":

#info-box(title: none)[
  `{4,5}` #sym.inter `{5,6}` = *`{5}`* #h(1em) #emph[(only the Engine is in both)]
]

Same two shelves, opposite operator — the shape of the question picks which. The
third operator, subtraction, is exclusion: the deleted-set removal above, or a
`WHERE NOT`. That is the whole vocabulary:

#boxeq[
  #set align(center)
  #sym.union #h(0.3em) *expands* a frontier #h(1.5em)
  #sym.inter #h(0.3em) *constrains* it #h(1.5em)
  #sym.minus #h(0.3em) *excludes* from it
]

So our motivating question — "entities `chunk_10` mentions that also relate to
Babbage" — is: take `MENTIONS[chunk_10] = {4,5}`, take `RELATES_in[6] = {4,5}`
(things that relate to Babbage), and intersect: `{4,5}`. Ada and the Engine. Two
shelf reads and one set operation. No crawl.

== Indexes and deletes are sets too

A set is not only how we store adjacency. It is how we store almost everything
worth querying:

- *Indexes are sets.* "Which entities are named 'Ada'?" is a shelf too:
  `Index[name]["ada"] = {4}`. A value lookup is the same machine as an edge walk.
- *Reverse edges are sets.* `EdgeIn` is not a special structure — it is another
  posting list, the backward shelf from the tip-box above.
- *Deletes are sets.* The tombstones you subtract are a set of UIDs.
- *Counts lean on sets.* A node's degree is how many cards are on its shelf.

So the query engine is not five subsystems. It is one:

#boxeq[
  *Fetch some UID sets. Combine them with #sym.union, #sym.inter, #sym.minus. That
  is the graph engine.*
]

This is why turbolay needs no graph-native storage engine: a dictionary of sets
and three operators is something an S3 bucket can hold.

== Where the set model breaks down

The set model has three limits. Each points to a later chapter, not a flaw in the
idea:

#warning-box[
  - *Edges with values.* A set holds bare numbers, so it has nowhere to write
    "`since: 2019`" on a relationship. Valued and faceted edges keep their
    properties in a companion record beside the set, which still holds only the
    membership.
  - *Parallel edges.* A set holds each number once, so it cannot record *two*
    separate `KNOWS` edges between the same pair. A true multigraph on one
    predicate is not expressible as one set.
  - *Order that isn't UID order.* The shelf is sorted by UID and nothing else.
    "This node's edges, heaviest-first" cannot be a set read; it needs a different
    layout, which turbolay defers.

  And a boundary for the larger claim: "everything is a set" holds for the graph's
  *topology, indexes, and tombstones*. A node's actual *properties* — Ada's name,
  the paper's title — are ordinary blobs, not sets. A later chapter covers where
  those live.
]

One more difference from Neo4j: reading a shelf is a *storage lookup*, not a
pointer dereference. Every hop is a `get` against SlateDB — possibly a read from
S3 — never a memory jump. turbolay trades away Neo4j's in-memory pointer chase for
a model that shards, survives on object storage, and computes in set algebra.
Whether that trade pays off, the rest of the book measures rather than asserts.

== Next: how to store a set

The whole database now rests on one primitive: a set of UIDs. So the most
important engineering decision in turbolay is how you store a set of numbers.

#question-box[
  A celebrity node can have ten million neighbors — a set of ten million integers.
  You may need to intersect it with another such set in milliseconds, and it has
  to sit cheaply in an S3 object. How do you store ten million numbers so the set
  is small *and* intersects without unpacking it? And why does it matter that the
  numbers we handed out are *dense* — 1, 2, 3, 4, 5, 6 — rather than random?
]

The next chapter answers both, and the answer comes with a sharp constraint.
