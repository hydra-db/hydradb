#import "../vendor/bookly/src/bookly.typ": *

// Cast colors for the xid->uid figure (reused across the book).
#let src_fill = rgb("#cfe9d5")
#let src_line = rgb("#2f8a4f")
#let chk_fill = rgb("#d3e3f7")
#let chk_line = rgb("#3b6fb0")
#let ent_fill = rgb("#f7e5c2")
#let ent_line = rgb("#c8791f")
#let castbox(f, l, body) = box(inset: (x: 5pt, y: 2.5pt), radius: 3pt,
  fill: f, stroke: l + 0.6pt, text(size: 9pt, body))

= Node & Schema Records: The Blob at `Node[uid]`

#info-box(title: [Learning goals])[
  Read this chapter if you want to know:
  - why a node's scalar properties live *inline*, in one value, when Dgraph
    makes every property its own posting-list key — and what that buys and
    costs;
  - why the node codec is hand-rolled little-endian rather than `bincode`,
    even though RFC 0004's own example says `bincode`;
  - how a label, predicate, or property name becomes a dense `u32` id, and why
    the three name-spaces are interned independently;
  - the exact byte layout of a `NodeRecord`, tag by tag, field by field;
  - why an oversize node is *rejected* while an oversize adjacency is *split*
    — the opposite policy on almost the same threshold.
]

Last chapter closed on a promise: a uid is real, durable, and stable, but it
is only ever an *address*. `resolve_or_create_xid_batched` hands back a
`u64` and nothing else — no name, no label, no properties. Whatever Ada
*is*, as opposed to merely the number `4`, has to live somewhere else. It
lives at exactly one key: `Node[4]`.

#question-box[
  What is actually stored at `Node[4]`? Chapter I·3 already sketched the
  shape from a distance — labels, properties, xid, one blob. This chapter
  opens that blob to the byte. Why does turbolay pack an entire node into one
  monolithic value when Dgraph, its direct ancestor, gives every property its
  own posting-list key? Why write a hand-rolled codec for that blob instead
  of reaching for `bincode`, the same house serializer RFC 0004's own example
  names? And why does that blob get *rejected outright* past 1 MiB, when an
  oversize adjacency posting is quietly *split* instead — the opposite
  policy on almost the same size?
]

== Monolithic, not wide-column

Start with what Dgraph does, because turbolay's node record only makes sense
as a departure from it. In Dgraph, a node's scalar properties are posting
lists exactly like its edges: `name` is a predicate, and `<uid, name,
"Ada Lovelace">` lives in the `name` predicate's own tablet, next to every
other node's `name` value. Reconstructing "everything about Ada" means
touching every predicate she has a value on — a fan-out across as many keys
as she has properties, each a separate lookup.

turbolay does not do this. A node's labels, its scalar properties, and its
xid back-reference are packed into *one* value — `NodeRecord` — behind
*one* key, `Node[uid]` (`src/value.rs:122-133`):

```
pub struct NodeRecord {
    pub labels: Vec<LabelId>,             // sorted, caller invariant
    pub props: BTreeMap<PropId, TypedValue>,
    pub xid: String,
}
```

Ada's five properties, her two labels, her xid — all of it comes back from
a single `get(Node[4])`. There is no fan-out to assemble a node, because a
node was never split up in the first place.

#figure(
  kind: image,
  supplement: [Figure],
  block(inset: 8pt)[
    #set text(size: 9pt)
    #grid(columns: (1fr, 1fr), column-gutter: 18pt,
      block(inset: 6pt, radius: 4pt, stroke: chk_line + 0.6pt, fill: rgb("#f7f8fa"))[
        *Dgraph — wide-column*
        #v(4pt)
        `name[4] -> "Ada Lovelace"` #linebreak()
        `title[4] -> "mathematician"` #linebreak()
        `born[4]  -> -1815`
        #v(4pt)
        #text(fill: rgb("#8a8f98"))[3 keys, 3 tablets, 3 `get`s to know Ada]
      ],
      block(inset: 6pt, radius: 4pt, stroke: ent_line + 0.6pt, fill: rgb("#f7f8fa"))[
        *turbolay — monolithic*
        #v(4pt)
        `Node[4] -> { labels: [Entity],`#linebreak()
        #h(1.4em) `props: { name: "Ada Lovelace",`#linebreak()
        #h(2.8em) `title: "mathematician", born: -1815 },`#linebreak()
        #h(1.4em) `xid: "entity_7" }`
        #v(4pt)
        #text(fill: rgb("#8a8f98"))[1 key, 1 tablet, 1 `get` to know Ada]
      ],
    )
  ],
  caption: [Dgraph makes each scalar property its own posting-list key,
    sharing the predicate's tablet with every other node's value on that
    predicate; a node read is a fan-out. turbolay inlines labels, properties,
    and xid into one `NodeRecord` at `Node[uid]` (`src/value.rs:122-133`); a
    node read is one point `get`. Edges are the one thing turbolay does
    *not* inline — `EdgeOut`/`EdgeIn` posting lists (II·1) and the
    single-copy `EdgeProp` companion (`src/serde/record_tag.rs:59`) stay
    separate keys, because their whole value is a set that many other
    lookups need to intersect against.],
) <fig-monolithic-vs-widecolumn>

The trade is real and worth stating plainly, not just for the win column.
One `get` returning the whole node is a genuine win for a read-heavy
workload — the RAG-KG case this book has followed all along, where a query
resolves an entity and wants its name, its type, and its source in the same
breath. The cost lands on the write side: any single property change is a
*whole-record rewrite*. Add one property to a node with forty others, and
all forty travel again. Dgraph's per-property posting lists sidestep that
exact cost — a `name` update touches only the `name` tablet. RFC 0004 calls
the wide-column split the "write-heavy alternative" and defers it outright
(`docs/rfcs/0004-graph-data-model-and-write-path.md` §"Alternatives
considered", D12); turbolay's v0 bet is that RAG-KG ingestion is read-heavy
enough, and nodes small enough, that the rewrite cost is the cheaper problem
to have.

Notice, too, what stays *outside* the blob: edges. `EdgeOut[uid][pred]` and
`EdgeIn[uid][pred]` are their own posting-list keys (II·1), and a
faceted/valued edge's properties get their own single-copy `EdgeProp[src][pred][dst]`
record (`src/serde/record_tag.rs:59`) rather than living inside either
endpoint's `NodeRecord`. That is not an inconsistency — it is the same
design principle cutting the other way. An edge set is read by many
different queries fanning out from many different anchors, and it needs to
support set algebra (union, intersect, subtract) against *other* edge sets.
A node's own properties are read exactly one way: "give me everything about
this node." Inline what is read as a whole; keep separate what is read as a
set.

#boxeq[
  A node is one value: one `get` returns the whole node. The price is that
  any change rewrites all of it — which is exactly why the 1 MiB cap
  *rejects* rather than splits (more on that split-vs-reject asymmetry
  below).
]

== Why hand-rolled, not `bincode`

Last chapter's closing question-box asked this directly, so it deserves a
straight answer before anything else: RFC 0004's own worked example writes
`NodeRecord`'s v0 codec as `bincode` — "the opendata house codec... already
proven across `common`" (§"Node record — monolithic blob (v0)"). The code
does not do that. `V0NodeCodec` is a hand-rolled, fail-closed,
little-endian encoder (`src/value.rs:143-203`), and `bincode` is not a
dependency anywhere in this fork — not in `Cargo.toml`, not transitively.
The module doc says so without hedging (`src/value.rs:1-11`).

Why depart from the RFC's own suggestion? Three reasons, and they are the
same three reasons every codec in this crate — `PostingValue` (II·1),
`SchemaEntry` (below), `NodeRecord`, `EdgeProps`, `ChangeRecord` — is
hand-rolled instead of derived:

- *An explicit, stable on-wire format.* `bincode`'s wire format is an
  implementation detail of the `bincode` crate and its derive macros; a
  struct-field reorder or a `bincode` major-version bump can silently change
  what's already on disk. A hand-rolled codec's byte layout is the one thing
  written down in this module — offsets, tag bytes, LE integers — and
  nothing outside this file can move it without a deliberate edit.
- *Fail-closed, not panic-open.* Every decode path here rejects truncation,
  trailing bytes, and unknown tags with an `Err`, never a panic
  (`src/value.rs:195-200`, `243-247`, `363-367`; the adversarial-bytes test
  at `src/value.rs:696-707` throws raw garbage at both `V0NodeCodec::decode`
  and `ChangeRecord::decode` and asserts neither one panics). A derived
  `bincode::deserialize` on untrusted or corrupted bytes does not give you
  that guarantee for free.
- *One house rule, uniformly applied, zero external dependency.* Keys are
  big-endian (so byte order is uid/id order); values are little-endian,
  everywhere, without exception (`src/value.rs:17`, `src/schema.rs:22`).
  A hand-rolled codec can enforce that rule byte by byte; a general-purpose
  serializer has its own conventions to fight.

None of this forecloses anything. `NodeRecord`'s serialization sits behind
a `NodeCodec` trait precisely so the choice is swappable
(`src/value.rs:135-141`):

```
pub trait NodeCodec {
    fn encode(record: &NodeRecord) -> Result<Bytes>;
    fn decode(bytes: &[u8]) -> Result<NodeRecord>;
}
```

RFC 0004 flags a zero-copy `rkyv` codec as a measured fast-follow, adopted
only if a real read-path profile pays for it (§"Alternatives considered").
The trait is the seam that swap would go through; nothing about today's v0
callers would need to change. Where the RFC and the code disagree on which
codec ships first, the code wins, and the honest story is simpler than
either extreme: not "bincode, as designed," not "a bespoke format we're
attached to" — a hand-rolled encoder chosen for explicitness and fail-closed
safety, kept swappable, and not yet worth swapping.

#note[
  The same "code wins" note applies one level down. RFC 0003's schema
  keyspace table states the `SchemaName` record's value as `id: u32 (BE)`
  (`docs/rfcs/0003-keyspace-and-encoding.md` §"Schema keyspace"), and
  `src/serde/record_tag.rs:20`'s own doc table repeats the claim. The write
  path does not do that: `intern` encodes the id little-endian
  (`encode_u32_le`, `src/write.rs:655, 837-839`), matching the crate's
  values-are-LE house rule rather than the RFC's BE claim. It is a harmless
  divergence today for an unusual reason — nothing in this codebase actually
  *decodes* a `SchemaName` value yet. `intern` resolves names through the
  in-memory `SchemaCache` (`src/write.rs:619-621`), and that cache is
  rebuilt at open by scanning `SchemaId` records alone
  (`src/schema.rs:295-307`), never `SchemaName`. The record is written,
  correctly LE by the house rule, and simply not yet read by anything.
]

== Names become dense `u32`

A `NodeRecord`'s `labels` field is `Vec<LabelId>` and its `props` key is
`PropId` — dense 32-bit integers, not strings. Chapter II·2 already minted
these: `label`, `pred`, and `prop` are three of the five counters on
`GraphAllocators` (`src/ids.rs:74-85`). This chapter is where those counters
turn into *names* a schema can look up in either direction.

`schema.rs`'s module doc states the shape as two record types
(`src/schema.rs:10-15`):

```
SchemaName [kind][name]  -> u32 id       (name -> id, consulted on write)
SchemaId   [kind][id]    -> SchemaEntry  (id -> name + directives, on read)
```

`kind` is one of three values — `Label`, `Predicate`, `PropertyKey`
(`src/serde/mod.rs:180, 194-196`) — and each kind interns *independently*.
That is deliberate: a label and a predicate can share a name (`"Person"` the
label, `"Person"` the predicate on some other schema) without colliding,
because they live in disjoint `(kind, name)` and `(kind, id)` spaces. A test
in `schema.rs` checks exactly this — the same string, two kinds, two
different ids, no cross-talk (`src/schema.rs:485-503`).

#figure(
  kind: image,
  supplement: [Figure],
  block(inset: 8pt)[
    #set text(size: 9pt)
    #align(center)[
      #grid(columns: (auto, auto, auto, 1fr), column-gutter: 14pt, row-gutter: 8pt,
        align: (right, center, left, left),
        [`Label` space],    [#sym.arrow.r], [`"Entity"` #sym.arrow.r `1`], [dense per-kind counter],
        [`Predicate` space],[#sym.arrow.r], [`"MENTIONS"` #sym.arrow.r `1`], [independent — also starts at 1],
        [`PropertyKey` space],[#sym.arrow.r], [`"name"` #sym.arrow.r `1`], [independent — a third space],
      )
    ]
  ],
  caption: [Three interning spaces, one per `SchemaKind`
    (`src/serde/mod.rs:180-197`). Nothing stops all three from independently
    allocating id `1` to their first name — they are different `(kind, id)`
    keys, so there is nothing to collide.],
) <fig-schema-kinds>

What lives *at* an id is a `SchemaEntry` (`src/schema.rs:150-157`): the name
itself (round-tripped, so `SchemaId` alone is enough to answer "what is id
`7`"), a `ValType` for predicates that carry a scalar (`None` for pure
edges/labels), and `Directives` — whether this name gets a value index, a
reverse projection, a count index, list semantics (RFC 0006). The write
path's v0 defaults are narrow: a predicate always gets `reverse: true` (RFC
0003 D10 — the `EdgeIn` projection is always materialized), and nothing
else is turned on by default (`src/write.rs:641-652`) — value/count indexes
are a schema-authoring feature this write path doesn't expose yet (M2, RFC
0006).

The `SchemaEntry` codec is the same hand-rolled, fail-closed, LE shape as
everything else in this chapter: `encode_utf8(name)`, one byte for
`value_type`, one byte each for `reverse`/`count`/`list`, then an
array-count-prefixed list of tokenizer bytes (`src/schema.rs:159-178`), and
`decode` is its exact inverse, rejecting truncation, an unknown
value-type/tokenizer/boolean byte, or trailing bytes
(`src/schema.rs:185-216`; round-tripped by `src/schema.rs:325-450`).

Two records are the durable authority; a third structure, `SchemaCache`, is
the accelerator that makes name resolution I/O-free on the hot path — a
bimap per kind, `name -> id` and `id -> entry`, entirely in memory
(`src/schema.rs:246-264`). It is never itself the source of truth: on open,
`SchemaCache::rebuild_from_storage` scans every `SchemaId` record and
rebuilds both directions from scratch (`src/schema.rs:295-307`). Lose the
cache and you lose nothing but a warm start.

Why intern at all, rather than just storing `"MENTIONS"` in every key and
value that needs it? The `schema.rs` module doc gives the reason directly:
"the keyspace stores fixed-width ids instead of repeating name strings for
every vertex" (`src/schema.rs:4-8`). A `u32` is 4 bytes, always; a name is
however many bytes it is, repeated once per node that carries it, once per
adjacency key that uses it. Dgraph made the same call for exactly the same
reason — RFC 0003 cites the `fundamentals-of-graph` guidance that
schema-free encoding "can easily double storage size because property key
strings are repeated for every vertex" (`docs/rfcs/0003-keyspace-and-encoding.md`
§"Schema keyspace"). turbolay keeps that call; the departure is only in
which name-spaces are independent and how the value bytes are laid out.

== The byte layout of a node

Put the last two sections together on the running cast. Ada is uid `4`,
label `Entity`, one property `name = "Ada Lovelace"`, external id
`entity_7` (Chapter I·3, `src/serde/mod.rs`). Her key is built by
`node_key` (`src/serde/keys.rs:131-135`) — the 3-byte head plus her uid,
big-endian:

#figure(
  kind: image,
  supplement: [Figure],
  block(inset: 6pt)[
    #set text(size: 8.5pt)
    #table(
      columns: (auto, auto, auto, 1fr),
      align: (left, right, left, left),
      stroke: none,
      inset: (x: 7pt, y: 3.5pt),
      table.hline(),
      table.header([*Part*], [*Offset*], [*Bytes*], [*Field*]),
      table.hline(),
      [key], [`0`],    [`05`],                       [`SUBSYSTEM` (`src/serde/mod.rs:39`)],
      [key], [`1`],    [`01`],                       [`KEY_VERSION` (`src/serde/mod.rs:42`)],
      [key], [`2`],    [`30`],                       [`Node` tag byte — nibble `0x3` high, reserved `0x0` low (`src/serde/record_tag.rs:22,40`)],
      [key], [`3–10`], [`00 00 00 00 00 00 00 04`],  [uid `4`, big-endian (`src/serde/keys.rs:131-135`)],
      table.hline(),
    )
  ],
  caption: [`Node[4]` — Ada's key. Same 3-byte head as every graph record
    (`src/serde/mod.rs:39-42`); only the tag byte and the tail differ by
    record type.],
) <fig-node-key>

Her value is the interesting part — a `NodeRecord` run through
`V0NodeCodec::encode_with_cap` (`src/value.rs:149-169`). The field *order*
and *widths* below are exactly what the codec writes, machine-verifiable
from `src/value.rs`; the two interned ids are shown symbolically, because
which small integer the interner actually assigned to `"Entity"` and
`"name"` depends on allocation order at write time, not on anything this
codec fixes:

#figure(
  kind: image,
  supplement: [Figure],
  block(inset: 6pt)[
    #set text(size: 8.5pt)
    #table(
      columns: (auto, auto, auto, 1fr),
      align: (left, right, left, left),
      stroke: none,
      inset: (x: 7pt, y: 3.5pt),
      table.hline(),
      table.header([*Offset*], [*Bytes (LE)*], [*Width*], [*Field*]),
      table.hline(),
      [`0–1`],   [`01 00`],                 [`u16`], [label count = 1 (`encode_array_count`, `common/src/serde/encoding.rs:139-144`)],
      [`2–5`],   [`L? L? L? L?`],           [`u32`], [`LabelId` for `"Entity"` — illustrative interned id],
      [`6–7`],   [`01 00`],                 [`u16`], [prop count = 1],
      [`8–11`],  [`P? P? P? P?`],           [`u32`], [`PropId` for `"name"` — illustrative interned id],
      [`12`],    [`04`],                    [`u8`],  [`TypedValue` tag = `STRING` (`src/value.rs:40`)],
      [`13–16`], [`0c 00 00 00`],           [`u32`], [string length = 12 bytes (`write_blob`, `src/value.rs:404-407`)],
      [`17–28`], [`"Ada Lovelace"`],        [12 B],  [UTF-8 payload],
      [`29–32`], [`08 00 00 00`],           [`u32`], [xid length = 8 bytes],
      [`33–40`], [`"entity_7"`],            [8 B],   [xid UTF-8 payload],
      table.hline(),
    )
  ],
  caption: [`Node[4]`'s value, 41 bytes total. Structure — field order, every
    width, every tag byte — is fixed by `V0NodeCodec::encode_with_cap`
    (`src/value.rs:149-169`) and machine-verified against it; `L?`/`P?` mark
    the two bytes whose *value* depends on interning order, not on the
    codec. `decode` (`src/value.rs:177-203`) is the exact inverse and rejects
    truncated or trailing bytes (`src/value.rs:195-200`; the property tests
    at `src/value.rs:565-581` cut every prefix and append one stray byte,
    asserting both fail).],
) <fig-noderecord-bytes>

Every `TypedValue` in the crate shares the tag byte in row `12` above
(`src/value.rs:36-42`):

#figure(
  kind: image,
  supplement: [Figure],
  block(inset: 6pt)[
    #set text(size: 9pt)
    #table(
      columns: (auto, auto, 1fr),
      align: (center, left, left),
      stroke: none,
      inset: (x: 9pt, y: 3.5pt),
      table.hline(),
      table.header([*Tag*], [*Variant*], [*Payload*]),
      table.hline(),
      [`0`], [`Null`],     [none],
      [`1`], [`Bool`],     [1 byte, `0`/`1`],
      [`2`], [`Int`],      [8 bytes, `i64` LE],
      [`3`], [`Float`],    [8 bytes, `f64` LE bit pattern],
      [`4`], [`String`],   [`u32` LE length + UTF-8 bytes],
      [`5`], [`Bytes`],    [`u32` LE length + raw bytes],
      [`6`], [`DateTime`], [8 bytes, `i64` LE epoch-millis (RFC 0006 §"DateTime is indexed via the `int` tokenizer")],
      table.hline(),
    )
  ],
  caption: [The seven `TypedValue` tags (`src/value.rs:36-42, 60-116`). The
    same seven appear inside a `NodeRecord`'s `props`, an `EdgeProps`
    companion record, and a `ChangeRecord`'s before/after `value` field — one
    scalar encoding, reused everywhere a scalar needs to go on the wire.],
) <fig-typedvalue-tags>

A property change never touches only its own bytes. Because the whole
record is one value, changing Ada's `name` re-serializes her labels, every
other property, and her xid right along with it — the write-amplification
half of the trade Section 1 named. `V0NodeCodec::decode`'s own trailing-
bytes check (`src/value.rs:195-200`) is what makes that whole-record rewrite
*safe* to trust on the way back: a truncated or corrupted `Node[uid]` value
fails loudly, never silently returns half a node.

== The record-tag map

`Node` (`0x3`) is one of twelve record types sharing the same 3-byte head
and the same high-nibble tag scheme. `src/serde/record_tag.rs`'s own doc
comment is the full map (`src/serde/record_tag.rs:20-31`); reproduced here
because it is the one table worth having at hand for the rest of Part II:

#figure(
  kind: image,
  supplement: [Figure],
  block(inset: 6pt)[
    #set text(size: 8.5pt)
    #table(
      columns: (auto, auto, auto, 1fr),
      align: (center, left, left, left),
      stroke: none,
      inset: (x: 7pt, y: 3.5pt),
      table.hline(),
      table.header([*Tag*], [*Type*], [*Key tail*], [*Value*]),
      table.hline(),
      [`0x1`], [`SchemaName`], [`[kind:1][name: terminated]`],                [`id: u32`],
      [`0x2`], [`SchemaId`],   [`[kind:1][id: u32 BE]`],                      [`SchemaEntry`],
      [`0x3`], [`Node`],       [`[uid: u64 BE]`],                             [`NodeRecord`],
      [`0x4`], [`EdgeOut`],    [`[src: u64 BE][pred: u32 BE]`],               [posting list],
      [`0x5`], [`EdgeIn`],     [`[dst: u64 BE][pred: u32 BE]`],               [posting list],
      [`0x6`], [`EdgePart`],   [`[dir:1][anchor:8][pred:4][start:8]`],        [posting part],
      [`0x7`], [`Index`],      [`[pred: u32 BE][token: order-preserving]`],   [posting list],
      [`0x8`], [`Count`],      [`[pred: u32 BE][dir:1][degree: u32 BE]`],     [posting list],
      [`0x9`], [`Xid`],        [`[xid: terminated]`],                        [`uid: u64 BE`],
      [`0xA`], [`Log`],        [`[seq: u64 BE]`],                            [`ChangeRecord`],
      [`0xB`], [`Meta`],       [`[meta_key: bytes]`],                        [scalar / bitmap / counter],
      [`0xC`], [`EdgeProp`],   [`[src: u64 BE][pred: u32 BE][dst: u64 BE]`],  [`EdgeProps`],
      table.hline(),
    )
  ],
  caption: [Every graph record type, high-nibble tag, reserved low nibble
    always `0x0` in v0 (`src/serde/record_tag.rs:1-60`). `from_tag_byte`
    rejects a nonzero reserved nibble and an unknown high nibble alike
    (`src/serde/record_tag.rs:99-111`; round-tripped and adversarially
    tested at `src/serde/record_tag.rs:133-160`). This is the whole
    keyspace's dispatch table — the next chapter is how these tags sit
    together in one ordered SlateDB namespace.],
) <fig-record-tags>

Two things worth noticing from the whole table at once, not just Node's
row. First, `Node`, `EdgeOut`/`EdgeIn`, and `EdgeProp` are the only three
record families whose key embeds a `uid` at all — schema and index records
key off interned ids and tokens, never raw node identity. Second, `Node`
and `EdgeProp` are the only two families this chapter and II·1 have covered
whose *values* are themselves structured records rather than a bare posting
list or scalar; `SchemaEntry` (this chapter) is the third.

== Reject a node, split an adjacency

Here is the asymmetry the opening question-box promised. Two size limits,
almost the same order of magnitude, opposite policies:

- A `NodeRecord` past `DEFAULT_NODE_SIZE_CAP` — 1 MiB, `1024 * 1024`
  (`src/value.rs:30`) — is *rejected*. `V0NodeCodec::encode_with_cap`
  checks the encoded length against the cap and returns
  `Err(Error::value("oversize_node: ..."))` before the caller ever builds a
  batch (`src/value.rs:162-167`). `upsert_node` runs this check *before*
  queuing any op — an oversize node aborts with nothing written at all, not
  even the xid mapping that would otherwise be free
  (`src/write.rs:314-317, 346-358`; the acceptance test at
  `src/write.rs:924-945` upserts a 2 MiB property and then asserts
  `lookup_uid` still resolves to `None` — the rejection is total, not
  partial).
- An adjacency posting past `SPLIT_THRESHOLD` — 512 KiB, `512 * 1024`
  (`src/posting_ops.rs:55`, taught in full in II·1) — is *split*: the base
  key becomes a manifest of `PartRef`s, and the members move out to
  `EdgePart` keys. Nothing is rejected; the set just stops living behind one
  key.

Same author, same file family, same order-of-magnitude threshold, and the
two policies point in opposite directions. The reason is not inconsistency
— it is that a node and an adjacency posting have different *meanings* as
values, and only one of those meanings survives being cut in half.

An adjacency posting is a *set*. `{4, 5}` split into `{4}` and `{5}` is
still, provably, `{4} #sym.union {5}`, and `union_parts` (`src/posting.rs:143-149`)
folds them back with no information lost and no order to preserve — that is
exactly what a `RoaringTreemap`'s bisection (`bin_split`,
`src/posting_ops.rs:301-329`) exploits. A `NodeRecord` is not a set; it is
one indivisible fact — "everything Ada is." There is no lossless way to
"split" that fact in half and have either half still mean anything on its
own; a `NodeRecord` cut at the midpoint is not two smaller nodes, it is
garbage. So the only sound move past the cap is refusal.

#figure(
  kind: image,
  supplement: [Figure],
  block(inset: 8pt)[
    #set text(size: 9pt)
    #table(
      columns: (auto, 1fr, 1fr),
      align: (left, left, left),
      stroke: none,
      inset: (x: 9pt, y: 4pt),
      table.hline(),
      table.header([], [*`Node[uid]` (1 MiB)*], [*Adjacency posting (512 KiB)*]),
      table.hline(),
      [Value's meaning],       [one indivisible fact — "this node"],       [a set of uids — order-independent membership],
      [Can it be halved and stay correct?], [no — half a node is not a smaller node], [yes — a set is the union of its parts],
      [Past the cap],          [*reject*, `oversize_node` (`src/value.rs:162-167`)], [*split* into `EdgePart` keys (`src/posting_ops.rs:301-329`)],
      [What the caller sees],  [the whole upsert aborts, nothing written],  [nothing — same logical set, more keys underneath],
      table.hline(),
    )
  ],
  caption: [The reject-vs-split asymmetry, restated as the property that
    actually decides it: whether the value can be losslessly halved. Both
    caps exist for the same underlying reason — SlateDB has no key-value
    separation (RFC 0002), so a large value is rewritten *whole* by
    compaction and by every update; both caps bound how large that
    whole-value rewrite can get.],
) <fig-reject-vs-split>

#boxeq[
  Split what is a set; reject what is a fact. A `RoaringTreemap` can be
  bisected and reunioned losslessly, so an oversize adjacency becomes more
  keys. A `NodeRecord` has no half that still means anything, so an oversize
  node becomes an error.
]

#note[
  *Built vs. design.* *Built (M1), on real SlateDB:* `NodeRecord` and
  `V0NodeCodec` (encode/decode, the 1 MiB cap, the oversize-rejects-nothing
  guarantee — `src/value.rs`, `src/write.rs`); every `TypedValue` variant,
  round-tripped and fuzzed against truncation/trailing bytes
  (`src/value.rs:494-707`); `EdgeProps`' single-copy encode/decode
  (`src/value.rs:210-250`), reusing the exact same scalar codec as a node's
  own `props`; `SchemaEntry` and the two-record (`SchemaName`/`SchemaId`)
  interning model with three independent kind-spaces, plus the read-through
  `SchemaCache` bimap rebuilt from `SchemaId` on open (`src/schema.rs`). All
  of the above are exercised in-process and, for the node/write path, against
  the SlateDB acceptance tier (`src/write.rs` tests).

  *Design, not built:* `Directives.index`/`.count` exist as fields — a
  `SchemaEntry` can *carry* the request for a value index or a count index
  today — but nothing builds or maintains those indexes yet; only the
  reverse (`EdgeIn`) projection is a live index at M1 (RFC 0006, M2
  territory). The `NodeCodec` trait's swap to a zero-copy `rkyv` codec is a
  named fast-follow (RFC 0004 §"Alternatives considered"), not started.
  And a `SchemaName` record's value, while written, is not decoded by
  anything in this crate yet — see the note above; a future reader that
  wants the "on write" fast path RFC 0003 describes would need to fix its
  BE/LE claim first.
]

== Next: how these tags share one keyspace

Every record this chapter and the two before it introduced — `Node`,
`Xid`, `EdgeOut`/`EdgeIn`, `SchemaName`/`SchemaId` — shares the same 3-byte
head and sits, ultimately, in one SlateDB instance behind one `Storage`
trait. That is not an accident of convenience; it is the whole reason a
graph traversal here is "just" a sequence of ordered-key lookups instead of
a distributed protocol.

#question-box[
  How do twelve record types, each with its own key shape, coexist in one
  ordered keyspace without colliding or fighting each other for scan
  ranges? What does `GRAPH = 0x05` actually gate, and what does
  `common::Storage` hand turbolay for free — versus what turbolay's own
  `serde` module has to build on top of it to make big-endian uids and
  order-preserving tokens actually sort the way a range scan needs them to?
]

That is `src/storage.rs` and `src/serde/`, and it is the next chapter.
