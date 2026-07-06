#import "../vendor/bookly/src/bookly.typ": *

= Nodes, Edges, and the Keyspace

The last two chapters built one half of the system: adjacency is sets of UIDs,
stored as roaring bitmaps. But a graph is not only its wiring. Ada has a *name*;
a chunk has *text*; the paper has a *title*. Those are not sets — they are blobs.
And a real graph is not one set but *millions*: one per node, per edge type, per
direction, plus the external-id lookups and the schema.

Everything — the millions of sets, the property blobs, the lookups, the schema —
lives in the *same* flat key #sym.arrow.r blob store, SlateDB. This chapter is how
it all fits, and what a key looks like.

== A node is one blob

A node record holds exactly three things (`src/value.rs`):

#info-box(title: [The node record])[
  *`NodeRecord`* = *labels* (which kinds it is: `Entity`, `Chunk`) + *properties*
  (a map of name #sym.arrow.r value: `name = "Ada Lovelace"`) + its *xid* (the
  external string the user knows it by: `"Ada"`).
]

No edges, no degree, no version stamp — adjacency lives in separate records, and
the node knows nothing about it. Crucially, the whole record is stored *monolithic*:
one value, at one key, `Node[uid]`. Reading Ada's name is one `get` of `Node[4]`
that returns her labels, all her properties, and her xid together. "Ada's name" is
never a separate lookup from "Ada's labels."

This is a real choice, and the alternatives are instructive:

- *Neo4j* keeps properties in a separate store, reached by a pointer from the node
  record and chained as a linked list — a property read is a pointer chase.
- *Dgraph* makes every property its own posting-list key; a node's five properties
  are five keys.
- *turbolay* puts all of them in one blob. Five properties, one `get`.

The one blob wins for a read-heavy workload — one fetch returns the node — and
gives cross-property atomicity for free, since the whole record commits and rewrites
as a unit. It pays for that when you update a single property: the *entire* node is
rewritten. That price is the reason for a limit we will meet shortly.

== One flat, sorted keyspace

SlateDB is a single ordered key #sym.arrow.r value store. turbolay fits the whole
graph into it by making the *first byte of every key a record-type tag*. That tag
sorts each kind of record into its own contiguous band of the keyspace:

#figure(
  kind: image,
  supplement: [Figure],
  block(inset: 4pt)[
    #set text(size: 8.8pt)
    #table(
      columns: (auto, auto, auto),
      align: (left, left, left),
      stroke: none,
      inset: (x: 8pt, y: 4pt),
      table.hline(),
      table.header([*Record*], [*Key*], [*Example from the cast*]),
      table.hline(),
      [`Node`],    [`Node[uid]`],            [`Node[4]` #sym.arrow.r labels+props+xid for Ada],
      [`EdgeOut`], [`EdgeOut[src][pred]`],   [`EdgeOut[2][MENTIONS]` #sym.arrow.r `{4, 5}`],
      [`EdgeIn`],  [`EdgeIn[dst][pred]`],    [`EdgeIn[4][MENTIONS]` #sym.arrow.r `{2, 3}`],
      [`Xid`],     [`Xid[string]`],          [`Xid["Ada"]` #sym.arrow.r `4`],
      [`Log`],     [`Log[seq]`],             [`Log[7]` #sym.arrow.r "edge added: chunk_10 #box[MENTIONS] Ada"],
      [`Meta`],    [`Meta[key]`],            [`Meta["latest_seq"]` #sym.arrow.r `7`],
      table.hline(),
    )
  ],
  caption: [One store, tagged by record type. Each tag is a contiguous band, sorted
    by the rest of the key. Adjacency, properties, lookups, and the log all live
    here. (Index records get their own tag when we add indexes.)],
) <fig-keyspace>

Look at the two edge rows, because they are the whole trick. The `EdgeOut` key is
*subject-first*: `EdgeOut[src][pred]`. So `chunk_10`'s MENTIONS is one exact key —
one `get` returns `{4, 5}`. And a node's *entire* neighborhood, across every edge
type, is one prefix scan of `EdgeOut[chunk_10][…]`, because all of its predicates
sort together under its UID.

#tip-box[
  Predicates and labels are not stored as strings in these keys. `MENTIONS` is
  interned to a small integer id the first time it is seen, and every key carries
  the 4-byte id, not the word. It keeps keys compact and fixed-width — and the
  mapping itself is just two more records in the same keyspace.
]

#note[
  Dgraph puts the *predicate* first in its keys, which clusters one edge-type
  across all nodes — the unit it shards across a cluster. turbolay has one writer
  per namespace, so it never shards; it puts the *subject* first instead, for
  single-node locality.
]

== Both directions, always

The reverse row, `EdgeIn[4][MENTIONS] = {2, 3}`, answers "everything that mentions
Ada" — and it is exactly as cheap as the forward hop, one `get`. That is not free:
every edge is written *twice*, forward and backward, in the same atomic batch.

turbolay pays that on every edge, unconditionally. Dgraph makes reverse edges
opt-in (a schema directive), optimizing for one-directional scans. turbolay's bet
is the opposite: for a knowledge graph you traverse both ways constantly — "what
does this chunk mention" and "what mentions this entity" — so both directions
should cost the same, with no forethought required. Storage is cheap; a reverse
traversal that has to be reconstructed at query time is not.

== Two limits: reject versus split

The monolithic node blob and the roaring adjacency each have a size limit — but the
two limits behave in *opposite* ways, and the difference is worth burning in.

#figure(
  kind: image,
  supplement: [Figure],
  block(inset: 4pt)[
    #set text(size: 9pt)
    #table(
      columns: (auto, 1fr, 1fr),
      align: (left, left, left),
      stroke: none,
      inset: (x: 8pt, y: 5pt),
      table.hline(),
      table.header([], [*Node property blob*], [*Adjacency posting*]),
      table.hline(),
      [Limit],    [*1 MiB*],                          [*512 KiB* (serialized set)],
      [On breach],[*reject* the write (`oversize_node`)], [*bin-split* into part-keys],
      [Why],      [one value, rewritten whole by every update and compaction],
                  [new parts are new keys — no in-place rewrite],
      [Result],   [a giant property blob is refused], [a supernode's edges grow without bound],
      table.hline(),
    )
  ],
  caption: [Two size limits, opposite policies. A node with a 900 KiB blob *and*
    fifty million edges is legal: the blob fits, the edges split.],
) <fig-limits>

Why the asymmetry? A node value is rewritten *in full* every time you touch it —
SlateDB does not separate large values out, so a 50 MiB node would be re-copied by
every compaction pass. Capping it and rejecting oversize bounds that cost (spilling
huge nodes to raw S3 objects is a deliberate someday, not v0). Adjacency has no such
problem: when a posting crosses 512 KiB it is split into new part-keys, and on
object storage a new key is just a new object — nothing is rewritten in place. So a
supernode's edges are fine; a bloated property blob is not.

== Keys that sort themselves

One detail makes the whole keyspace work, and it is easy to miss: SlateDB compares
keys as *raw bytes*, with no notion of what they mean. So every logical order
turbolay needs — UIDs ascending (roaring depends on it), numbers in range order,
the log in sequence order — has to be baked into the bytes themselves.

That is why UIDs and ids go into keys *big-endian* (byte order then matches numeric
order), and why strings get an escape-and-terminate treatment so that `"Ada"` sorts
before `"Adam"` even with more key bytes following. Get this wrong and posting
intersection returns nonsense and range scans read garbage; get it right, and the
dumb byte-ordered store gives you every ordering the graph engine relies on, for
free. The exact encodings are Part II's business; the intuition is simply: *the
key's bytes are engineered so that byte order is meaning order.*

== Next: what is this substrate?

We now have the whole graph laid into one flat, sorted, byte-ordered store: nodes as
blobs, adjacency as sets, both directions, all under one keyspace. But we have kept
leaning on claims about the thing underneath — "compaction rewrites the whole
value," "a new part is just a new key," "one writer, one atomic batch" — without
ever saying what that thing *is*.

#question-box[
  What is SlateDB, really? Why build a graph database on an *LSM tree that lives on
  S3* — and what, exactly, does having a single writer let us throw away? The last
  three chapters kept cashing cheques against a substrate we have not yet described.
]

That substrate, and the bargain at the center of the whole project, is the next
chapter.
