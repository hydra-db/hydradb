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

= Substrate & Keyspace: One Ordered Store, Twelve Record Types

#info-box(title: [Learning goals])[
  Read this chapter if you want to know:
  - why SlateDB having *no custom comparators* is the single fact the entire
    `serde` module exists to work around;
  - what the 3-byte head `[0x05][0x01][tag]` actually gates, and why
    `GRAPH = 0x05` is defined locally instead of upstreamed into `common`;
  - what `common::Storage` hands turbolay for free versus what turbolay's own
    key encodings must build on top of it;
  - the four order-preserving token encodings that turn a range predicate
    into one bounded key-range scan;
  - why an adjacency read is "just" a range scan over one anchor's prefix —
    and why that makes turbolay *subject-major*, not predicate-sharded.
]

II·3 closed with a question this chapter has to answer in full: twelve record
types — `SchemaName`, `SchemaId`, `Node`, `EdgeOut`, `EdgeIn`, `EdgePart`,
`Index`, `Count`, `Xid`, `Log`, `Meta`, `EdgeProp` — each with its own key
shape, all sitting in *one* SlateDB instance behind *one* `Storage` handle.
Nothing separates them: no per-type table, no column family, no namespace
within the namespace. They share the same flat, ordered byte space, and nine
chapters of this book have quietly depended on that sharing working — every
`get`, every `scan_record_type`, every one-hop `neighbors` call has assumed
that a `Node` key and an `EdgeOut` key can coexist without one's scan
accidentally sweeping up the other, and that a range of `EdgeOut` keys comes
back in exactly the order a traversal needs.

#question-box[
  How do twelve record types, each with its own key shape, coexist in one
  ordered keyspace without colliding or fighting each other for scan ranges?
  What does `GRAPH = 0x05` actually gate, and what does `common::Storage`
  hand turbolay for free — versus what turbolay's own `serde` module has to
  build on top of it to make big-endian uids and order-preserving tokens
  actually sort the way a range scan needs them to?
]

The short version: coexistence is a 3-byte head, and sorting-right is
everything this chapter's byte encodings buy on top of a store that will
never do it for you. Start with the constraint that makes both necessary.

== No comparators: order lives in the bytes

Here is the one fact `src/serde/mod.rs`'s module doc states first, before
anything else, because everything downstream is a consequence of it:
"SlateDB is a plain ordered-byte-key store with *no custom comparators* —
key order is unsigned lexicographic byte order, full stop" (`src/serde/mod.rs:3-7`).
RocksDB lets you register a comparator function and change what "sorted"
means for a whole column family. Badger, Dgraph's own engine, does the same
for its own tables. SlateDB does not offer that knob at all — every key,
in every keyspace, sorts by raw unsigned byte comparison, and that is the
entire ordering vocabulary available.

That sounds like a limitation. It is actually the reason the rest of this
chapter's machinery exists, and it is worth sitting with the alternative
for a moment. If SlateDB *did* let turbolay plug in "compare these two keys
as big-endian uids" or "compare these two tokens as signed floats," the
`serde` module could be a thin set of struct definitions and the comparator
would do the sorting work. It doesn't. So every ordering turbolay's read
path depends on — uid order for posting-list set algebra, numeric order for
a range predicate, sequence order for the changelog tail, one anchor's edges
clustering together for a cheap one-hop read — has to already be true of the
*raw bytes*, before SlateDB ever looks at them. There is no second chance to
fix it at compare time.

`src/serde/mod.rs`'s own diagram states the shape every graph key commits to,
independent of record type (`src/serde/mod.rs:9-15`):

```
| KeyPrefix (2B) | RecordTag (1B) | order-preserving tail... |
  ^ subsystem=0x05                  ^ record-type-specific
    version=0x01
```

Two bytes that never vary, one byte that names the record type, and then a
tail whose entire job is to make byte order equal the order the record type
needs. This is turbolay's incarnation of Dgraph's own `x/keys` layout — kept,
not replaced (AGENTS.md, "Kept from Dgraph"). Dgraph needed order-preserving
keys for exactly the same reason: Badger has no custom comparators either.
The one house rule that governs every tail from here on, stated without
qualification at the top of the module: *keys are big-endian, values are
little-endian* (`src/serde/mod.rs:24`).

#boxeq[
  SlateDB sorts raw bytes and offers no comparator, so turbolay makes the
  bytes sort right: big-endian fixed-width for numeric order, terminated
  escaping for strings, sign-flipped numerics for ranges.
]

== The three-byte head

The two bytes that never vary are the coexistence trick. `SUBSYSTEM = 0x05`
and `KEY_VERSION = 0x01` (`src/serde/mod.rs:39-42`) are followed by one tag
byte that names the record type, and `KEY_HEAD_LEN` fixes the whole head at
3 bytes (`src/serde/mod.rs:45`). Every key builder in `src/serde/keys.rs`
starts here — `head()` writes exactly these three bytes before a single
tail byte is appended (`src/serde/keys.rs:29-35`).

`SUBSYSTEM` is not turbolay's own invention; it is one slot in a shared
registry opendata already runs across its subsystems. `common`'s own
registry names four others — `TIMESERIES = 0x01`, `VECTOR = 0x02`,
`LOG = 0x03`, `KEYVALUE = 0x04` (`vendor/common/src/serde/subsystem.rs:10-20`)
— and stops there; `0x05` is not registered upstream at all. turbolay claims
it locally instead of adding a fifth constant to the shared crate:
"`GRAPH = 0x05`... defined locally here rather than in
`common::serde::subsystem` so turbolay does not have to mutate the shared
crate; upstreaming the registration is a follow-up" (`src/serde/mod.rs:32-38`).
That is a real, if small, form of debt — the registry's own `name()` lookup
does not know graph keys exist (`vendor/common/src/serde/subsystem.rs:22-31`)
— but it costs nothing today: nothing else stores anything at `0x05`, so
there is no collision to have.

#figure(
  kind: image,
  supplement: [Figure],
  block(inset: 8pt)[
    #set text(size: 9pt)
    #align(center)[
      #stack(dir: ltr, spacing: 0pt,
        box(width: 90pt, height: 34pt, fill: rgb("#e3ecf7"), stroke: chk_line + 0.6pt,
          radius: (left: 4pt), align(center + horizon)[`05`#linebreak()#text(size: 7.5pt, fill: rgb("#5a6472"))[SUBSYSTEM]]),
        box(width: 90pt, height: 34pt, fill: rgb("#f2ede2"), stroke: ent_line + 0.6pt,
          align(center + horizon)[`01`#linebreak()#text(size: 7.5pt, fill: rgb("#5a6472"))[KEY_VERSION]]),
        box(width: 110pt, height: 34pt, fill: rgb("#e7efe9"), stroke: src_line + 0.6pt,
          radius: (right: 4pt), align(center + horizon)[`tag` (e.g. `30`)#linebreak()#text(size: 7.5pt, fill: rgb("#5a6472"))[RECORD TYPE]]),
      )
    ]
    #v(10pt)
    #align(center)[#text(size: 8.5pt, fill: rgb("#8a8f98"))[the 3-byte head, `KEY_HEAD_LEN = 3` — identical on every graph key]]
    #v(10pt)
    #table(
      columns: (auto, auto, 1fr),
      align: (center, left, left),
      stroke: none,
      inset: (x: 8pt, y: 3.5pt),
      table.hline(),
      table.header([*Subsystem byte*], [*Owner*], [*Registered where*]),
      table.hline(),
      [`0x01`], [Timeseries], [`common::serde::subsystem::TIMESERIES`],
      [`0x02`], [Vector], [`common::serde::subsystem::VECTOR`],
      [`0x03`], [Log],       [`common::serde::subsystem::LOG`],
      [`0x04`], [KeyValue],  [`common::serde::subsystem::KEYVALUE`],
      [`0x05`], [*Graph (turbolay)*], [`src/serde/mod.rs:39` — local, not upstreamed],
      table.hline(),
    )
  ],
  caption: [The 3-byte head shared by every graph key (`src/serde/mod.rs:39-45`,
    `src/serde/keys.rs:29-35`), and the subsystem registry it draws from
    (`vendor/common/src/serde/subsystem.rs:10-20`). `0x01`–`0x04` are shared
    opendata subsystems living in other SlateDB instances entirely — the byte
    only has to be disjoint if two subsystems ever shared one physical store,
    which they don't (one DB per namespace, below). `0x05` carves out
    turbolay's slice of the registry without a shared-crate edit.],
) <fig-three-byte-head>

The tag byte is where the twelve-way split actually happens. `RecordType`
puts its discriminant in the tag's high nibble and reserves the low nibble
at `0x0` in v0 — `Node`'s tag byte is `0x30`, `EdgeOut`'s is `0x40`,
`EdgeIn`'s is `0x50`, and so on through all twelve (`src/serde/record_tag.rs:34-60`,
`tag_byte()` at `62-73`). II·3 already built the full tag map (`<fig-record-tags>`
there) — this chapter's job is not to repeat it but to explain what owning a
tag byte *buys*: `record_type_range(rt)` is nothing more than `BytesRange::prefix`
over that record type's 3-byte head (`src/serde/keys.rs:432-434`), which is
exactly what `GraphStorage::scan_record_type` calls to answer "every `Node`
record" without touching an `EdgeOut` or a `Log` key even once
(`src/storage.rs:132-139`). Twelve record types don't fight over scan ranges
because each one owns a disjoint, contiguous slice of the keyspace — the tag
byte *is* the partition, and it costs one byte per key to keep it that way.

== One wrapper, one database, one namespace

`GraphStorage` is where the coexistence story turns from "the keys don't
collide" into "there is one physical thing they live in." It is
deliberately thin: one field, an `Arc<dyn Storage>`, cloneable so every
caller shares the same underlying database (`src/storage.rs:26-28`). RFC
0003 calls the shape "one SlateDB database per namespace"
(`src/storage.rs:1-2`), and RFC 0002 is where the *reason* lives: exactly
one writer per namespace (D2, `src/storage.rs:5-6`), so there is no second
writer to isolate from, no second tenant's keys that need a different
physical store, and no reason to shard the keyspace across databases the
way Dgraph shards tablets across Alpha groups. `docs/plan.md` costs this
plainly too: "one DB (manifest + poller) per namespace... acceptable at POC
tenant counts" (`docs/plan.md:48`) — a real, named tradeoff, not a free
lunch, just one this book's scale doesn't yet have to pay for.

`GraphStorage::open` is short, and every line in it is doing one of two
jobs: wiring the merge operator, or getting between SlateDB and the network
(`src/storage.rs:43-62`). The `SlateDb` config path builds the real object
store and wraps it in `InstrumentedObjectStore` *before* `StorageBuilder`
ever sees it (`src/storage.rs:36-51`) — RFC 0017's observability spine
counting, timing, and byte-metering every request SlateDB itself issues
against S3, with no fork of SlateDB required to do it (`src/obs.rs:1-25`).
Then `StorageSemantics::new().with_merge_operator(GraphMergeOperator)` is
attached before `build()` returns (`src/storage.rs:55-59`) — every reader
and compactor that ever opens this namespace has to register the identical
operator, or a stored merge operand simply fails to resolve
(`src/storage.rs:31-35`). That operator, and what it actually dispatches by
record tag, is II·5's chapter; here it only matters that `GraphStorage` is
the one place it gets registered, once, for the whole namespace.

#figure(
  kind: image,
  supplement: [Figure],
  block(inset: 10pt)[
    #set text(size: 8.5pt)
    #let layer(f, l, title, body) = block(width: 100%, inset: 7pt, radius: 4pt,
      fill: f, stroke: l + 0.7pt)[*#title* #linebreak() #body]
    #let arrow = align(center)[#text(size: 13pt, fill: rgb("#8a8f98"))[#sym.arrow.b]]
    #layer(rgb("#f7f8fa"), rgb("#8a8f98"), [turbolay write / read path],
      [`upsert_node`, `upsert_edge`, `neighbors` — `src/write.rs`, `src/posting_ops.rs`])
    #arrow
    #layer(chk_fill, chk_line, [`GraphStorage` — one `Arc<dyn Storage>` per namespace],
      [`get`, `apply`/`apply_with_options`, `subscribe_durable`, `scan_record_type`, `flush` — `src/storage.rs:26-145`])
    #arrow
    #layer(ent_fill, ent_line, [`common::Storage` / `StorageBuilder`],
      [ordered `get`/`scan`/atomic `apply`; `GraphMergeOperator` registration; seqnum injection so durable seq == logical seq; the durable-watermark channel — `src/storage.rs:43-125`])
    #arrow
    #layer(rgb("#f2ede2"), rgb("#c8791f"), [`InstrumentedObjectStore`],
      [wraps the real object store *before* SlateDB sees it — every S3 request SlateDB itself makes is counted/timed, no SlateDB fork — `src/storage.rs:36-51`, `src/obs.rs`])
    #v(2pt)
    #align(center)[#text(size: 7.5pt, fill: rgb("#b23b3b"))[#sym.dash.em churn-isolation boundary: nothing above this line calls `slatedb::Db` directly #sym.dash.em]]
    #v(2pt)
    #arrow
    #layer(rgb("#eceff3"), rgb("#5a6472"), [SlateDB 0.14.x],
      [LSM tree, compaction, WAL, manifest — the pre-1.0 API this whole stack exists to not touch directly])
    #arrow
    #layer(rgb("#e7efe9"), src_line, [Object store],
      [S3 / GCS / MinIO / local — one bucket-prefix per namespace])
  ],
  caption: [The write/read path never calls `slatedb::Db`. Every hop goes
    through `common::Storage`, which isolates SlateDB's pre-1.0 API churn —
    "storage format is stable across adjacent versions; the Rust API is not"
    (`docs/plan.md:65`). `GraphStorage` (`src/storage.rs`) is turbolay's own
    thin seam on top: one handle per namespace, the merge operator, and
    nothing else.],
) <fig-storage-stack>

Read the stack top to bottom as a division of labor, because that is the
honest way to answer "what does `common::Storage` hand turbolay for free."
Ordered byte keys, a point `get`, a range `scan`, an atomic multi-key
`apply` (a `WriteBatch` at one seq), the merge-operator registration and
resolution machinery, `subscribe_durable`'s freshness-gate channel, and the
object-store abstraction across S3/GCS/MinIO/local — all of that is
`common`'s. `apply_with_options` is the sharpest example of the free lunch:
it commits with `await_durable: true` and an *injected* `seqnum` equal to
turbolay's own logical sequence, so the two counters never have a chance to
diverge (`src/storage.rs:107-118`). What `common::Storage` does *not* know,
and cannot be asked to know, is what any of those ordered bytes *mean* —
that byte 3 through 10 of a key is a uid, that it should be big-endian, that
an index token needs its sign bit flipped before it will sort like the
number it represents. That is entirely `crate::serde`'s job, and it is the
whole reason this module exists as more than a re-export of `common`. Two
named constraints, both from `docs/plan.md`'s own list of "permanent v0
inputs, not open questions" (`docs/plan.md:62-67`): *bytewise-only key
ordering*, mitigated by the encodings this chapter is about, and *pre-1.0
API churn*, mitigated by never letting anything above `GraphStorage` import
`slatedb` at all.

== Encodings that sort

Fixed-width components are the easy half, and II·2 and II·3 already used
them without dwelling on the reason: `Uid` is a `u64` written big-endian,
`PredId`/`LabelId`/`PropId` are `u32` written big-endian, because "byte
order equals numeric order — the contract the posting-list set algebra
relies on" (`src/serde/mod.rs:47-54`). `src/serde/keys.rs`'s own test says
this in code, not just in a comment: for a fixed record type, `node_key(1)
< node_key(2) < node_key(u32::MAX as u64 + 1)`
(`src/serde/keys.rs:628-636`) — a property the test checks across the u32/u64
boundary specifically, because that boundary is exactly where a naive
encoding (say, a variable-length varint) would quietly break the contract.

Variable-length components — xids, schema names, an `exact` index token —
cannot be fixed-width, so they need a different trick to keep "shorter
sorts before longer" true even when more key bytes follow. `terminated_bytes`
is that trick: escape `0x00` as `0x01 0x01`, escape `0x01` as `0x01 0x02`,
leave every other byte alone, and terminate the whole thing with a bare
`0x00` (`vendor/common/src/serde/terminated_bytes.rs:1-14`; the same rule
restated at `src/serde/token.rs:42-44`). The terminator is what makes
`"a" < "ab"` hold at the byte level and not just at the string level — with
no terminator, encoded `"a"` would literally be a byte-for-byte prefix of
encoded `"ab"`, which happens to sort correctly for *those* two strings but
breaks the moment a third key component follows either one. `keys.rs`'s own
round-trip test feeds an xid containing the terminator and escape bytes
themselves back through the encoder and gets it back exactly
(`src/serde/keys.rs:560-567`) — the escaping exists precisely so that a
user's xid is never forced to avoid two specific byte values.

Numeric index tokens need a third trick, because neither "big-endian" nor
"terminated" is enough on its own to make a *signed* or *floating-point*
value sort correctly. `token_int`/`token_float` reuse
`common::serde::sortable`'s sign-flipping encoders and then write the
result big-endian (`src/serde/token.rs:69-71, 90-92`): XOR an `i64` with its
sign-bit mask so negative numbers, which two's-complement stores with a
leading `1`, end up with a leading `0` and sort first
(`vendor/common/src/serde/sortable.rs:52-60`). Three concrete inputs make
the trick visible rather than asserted:

#figure(
  kind: image,
  supplement: [Figure],
  block(inset: 8pt)[
    #set text(size: 8.5pt)
    #table(
      columns: (auto, auto, 1fr),
      align: (right, left, left),
      stroke: none,
      inset: (x: 9pt, y: 3.5pt),
      table.hline(),
      table.header([*`i64` value*], [*sortable `u64` (hex)*], [*`token_int` bytes, big-endian*]),
      table.hline(),
      [`-5`],  [`0x7FFFFFFFFFFFFFFB`], [`7f ff ff ff ff ff ff fb`],
      [`0`],   [`0x8000000000000000`], [`80 00 00 00 00 00 00 00`],
      [`42`],  [`0x800000000000002A`], [`80 00 00 00 00 00 00 2a`],
      table.hline(),
    )
  ],
  caption: [`encode_i64_sortable(v) = (v as u64) ^ 0x8000_0000_0000_0000`
    (`vendor/common/src/serde/sortable.rs:59`), applied by hand to three
    values and written big-endian as `token_int` does
    (`src/serde/token.rs:69-71`). The sign-flip pulls every negative value's
    top bit down to `0` and every non-negative value's top bit up to `1`, so
    `-5`'s token sorts strictly before `0`'s, which sorts strictly before
    `42`'s — exactly the numeric order, now equal to byte order. `token_float`
    does the analogous flip for `f64`, with `NaN` sorting last by policy
    (`src/serde/token.rs:86-89`, tested at `src/serde/token.rs:224-235`).],
) <fig-sortable-int-example>

`hash` is the deliberate odd one out, and naming it as an exception rather
than hiding it is the point. It folds an arbitrary value down to a fixed
8-byte FNV-1a-64 digest (`src/serde/token.rs:134-141`) — fast, fixed-width,
and completely unordered: two values one apart numerically can hash to
digests on opposite ends of the byte space. It answers `=` and set
membership, never `<`/`>`, and because it is lossy — the original value
cannot be recovered from the digest — the planner has to flag it for a
re-fetch, re-checking every index hit against the real value before
trusting it (RFC 0006, `src/serde/token.rs:121-133`). The module's own doc
table is the whole vocabulary in one place (`src/serde/token.rs:11-16`):

#figure(
  kind: image,
  supplement: [Figure],
  block(inset: 8pt)[
    #set text(size: 9pt)
    #table(
      columns: (auto, auto, auto, 1fr),
      align: (left, center, center, left),
      stroke: none,
      inset: (x: 9pt, y: 4pt),
      table.hline(),
      table.header([*Tokenizer*], [*Order-preserving?*], [*Fidelity*], [*What it enables*]),
      table.hline(),
      [`exact` (`token_exact`, `src/serde/token.rs:45-47`)], [yes], [full], [equality *and* range on strings/bytes — `>`/`<` over an `exact` predicate is one bounded scan],
      [`int` (`token_int`, `:69-71`)],                      [yes], [full], [signed-integer range predicates — `>= lo <= hi` becomes `index_token_range(pred, lo, hi)` (`src/serde/keys.rs:457-459`)],
      [`float` (`token_float`, `:90-92`)],                  [yes (`NaN` last)], [full], [floating-point range predicates; `NaN` never falls inside a finite range],
      [`hash` (`token_hash`, `:134-141`)],                  [*no* — equality only], [lossy], [cheap fixed-width equality on values not worth ordering; planner sets a re-fetch flag],
      table.hline(),
    )
  ],
  caption: [The four order-preserving index-token encodings (`src/serde/token.rs:11-16`).
    Every one produces the *value* half of an `Index` key
    (`index_key(pred, token)`, `src/serde/keys.rs:209-214`) — the predicate id
    comes first, the token second, so a whole predicate's index is one
    contiguous scan (`index_pred_range`, `src/serde/keys.rs:448-452`) and a
    bounded token sub-range within it is `index_token_range`
    (`src/serde/keys.rs:457-459`), the exact encoding of a `>=`/`<=` range
    predicate RFC 0006's planner will consume.],
) <fig-token-table>

Every one of these encodings earns its keep the same way: it turns a
question the read path wants to ask — "give me everything in order," "give
me everything between these two bounds" — into a `BytesRange` SlateDB can
answer with one bounded scan, no post-filtering, no in-memory re-sort.

== Prefix clustering, and why a hop is a range scan

Put the head and the tail together on one record type and the payoff is
concrete. `edge_key(dir, anchor, pred)` writes the 3-byte head, then the
*anchor* uid big-endian, then the *predicate* id big-endian
(`src/serde/keys.rs:152-157`) — anchor first, predicate second. That
ordering choice, more than any other single fact in this chapter, is what
makes a one-hop read cheap.

Take five edges from the running cast (HANDOFF's dataset): `source_1`
(uid 1) `HAS_CHUNK` {2, 3}; `chunk_10` (uid 2) `MENTIONS` {4, 5}; `chunk_11`
(uid 3) `MENTIONS` {5, 6}; `Ada` (uid 4) `RELATES` {6}; `Engine` (uid 5)
`RELATES` {6}. Predicate ids below are illustrative — interning order isn't
fixed by this design, exactly as II·3 flagged for label/prop ids — but the
*shape* of the ordering is exact:

#let rowfill = (src_fill, chk_fill, chk_fill, ent_fill, ent_fill)
#figure(
  kind: image,
  supplement: [Figure],
  block(inset: 8pt)[
    #set text(size: 8.5pt)
    #table(
      columns: (auto, auto, auto, auto),
      align: (left, left, left, left),
      stroke: none,
      inset: (x: 8pt, y: 3.5pt),
      fill: (x, y) => if y == 0 { none } else { rowfill.at(y - 1) },
      table.hline(),
      table.header([*Key, sorted*], [*Anchor*], [*Pred*], [*Members*]),
      table.hline(),
      [`EdgeOut[1][HAS_CHUNK]`], [`source_1` (`1`)], [`HAS_CHUNK`], [`{2, 3}`],
      [`EdgeOut[2][MENTIONS]`],  [`chunk_10` (`2`)], [`MENTIONS`],  [`{4, 5}`],
      [`EdgeOut[3][MENTIONS]`],  [`chunk_11` (`3`)], [`MENTIONS`],  [`{5, 6}`],
      [`EdgeOut[4][RELATES]`],  [`Ada` (`4`)],       [`RELATES`],  [`{6}`],
      [`EdgeOut[5][RELATES]`],  [`Engine` (`5`)],    [`RELATES`],  [`{6}`],
      table.hline(),
    )
  ],
  caption: [Five `EdgeOut` keys in SlateDB sort order — which is exactly
    anchor-uid order, because `anchor` is the *first* variable byte after
    the head (`src/serde/keys.rs:152-157`). Rows 2 and 3 both carry
    `MENTIONS`, but they are not adjacent to each other in this ordering —
    row 3 (`chunk_11`) sorts where `3` sorts, full stop, whatever predicate
    it carries. Anchor dominates; predicate is a tiebreaker within one
    anchor's own block.],
) <fig-prefix-clustering>

Two things this table is built to show. First: if `chunk_10` carried a
*second* out-predicate, its key would sit immediately next to
`EdgeOut[2][MENTIONS]` — before `chunk_11` begins — because both share the
same 8-byte anchor prefix and only the trailing predicate id differs. One
node's whole out-adjacency, across every predicate it has, is one
contiguous `BytesRange` (`head(EdgeOut) ++ anchor`, an `adjacency_part_range`-style
prefix), and that is precisely what makes a one-hop "everything `chunk_10`
points to" a single bounded scan instead of a fan-out across as many keys
as `chunk_10` has predicates.

Second, and worth stating as plainly as the HANDOFF corrections do: this is
*not* predicate sharding. Two `MENTIONS` edges (rows 2 and 3 above) do not
cluster with each other at all — they land wherever their anchors happen to
sort, three uids apart in this toy dataset, arbitrarily far apart in a real
one. Dgraph shards by predicate: every `MENTIONS` triple across the whole
graph lives in one predicate's tablet, because Dgraph's read pattern is
"give me this predicate, wherever its subjects are." turbolay's read
pattern is the opposite — "give me this node, whatever its predicates are"
— so the key is anchor-major, or *subject-major*, and a predicate's edges
are scattered across the uid space by design, not by accident. There is no
sharding here at all in the Dgraph sense; there is one writer, one ordered
store, and a key layout chosen for the query turbolay actually runs.

`Index` keys make the contrast concrete by doing the *opposite* thing on
purpose. `index_key(pred, token)` writes predicate first, token second
(`src/serde/keys.rs:209-214`), so a whole predicate's index — every node
that has *any* value on it — is the one contiguous range
`index_pred_range(pred)` (`src/serde/keys.rs:448-452`), and `keys.rs`'s own
test confirms it: the same token under two different predicates sorts by
predicate first, and predicate 5's whole range excludes predicate 6's
key entirely (`src/serde/keys.rs:638-651`). `EdgeOut`/`EdgeIn` cluster by
*node*; `Index` clusters by *predicate*. Neither is more "correct" — each
record type's key order is chosen for the one query that type exists to
answer, and SlateDB's bytewise-only ordering is exactly permissive enough to
let every record type pick its own axis, as long as each stays inside its
own tag byte.

#boxeq[
  A one-hop read is a range scan over one anchor's prefix — not a fan-out,
  not a join, and not sharded by predicate at all. Subject-major key order
  is the whole trick; the tag byte is what lets a different record type
  (`Index`) cluster by predicate instead, in the very same keyspace, without
  the two ever colliding.
]

#note[
  *Built vs. design.* *Built (M1), on real SlateDB:* `GraphStorage` end to
  end — one `Arc<dyn Storage>` per namespace, `GraphMergeOperator`
  registration, `InstrumentedObjectStore` wrapping the real object store
  before SlateDB sees it, seqnum injection so durable seq equals logical seq,
  `subscribe_durable`'s watch channel (`src/storage.rs`, round-tripped
  against both the in-memory and SlateDB backends). The 3-byte head, every
  key builder/parser and its exact inverse, and the scan-range helpers
  (`record_type_range`, `adjacency_part_range`, `index_pred_range`,
  `index_token_range`, `log_range`) — all fail-closed on truncation and
  trailing bytes, all round-tripped (`src/serde/keys.rs`). All four token
  encodings — `exact`, `int`, `float`, `hash` — implemented, round-tripped,
  and property-tested for order (`src/serde/token.rs`).

  *Design, not built:* the token encodings exist and are tested in
  isolation, but nothing yet *maintains* an `Index` keyspace over them —
  RFC 0006's value/count indexes are M2 territory, so `token_int`/`token_float`/`token_hash`
  are proven correct with no live caller yet, the same status II·3 gave
  `Directives.index`/`.count`. Only the reverse `EdgeIn` projection is a
  live index today. Multi-namespace or cross-namespace access is out of
  scope entirely — `GraphStorage` is deliberately single-namespace, and
  RFC 0003's "one DB per namespace" cost (a manifest and poller per tenant)
  is accepted, not yet revisited. And where `docs/plan.md`'s own one-sentence
  summary calls turbolay "a predicate-sharded KV store" (`docs/plan.md:71`),
  the key layout this chapter just walked through says otherwise — `EdgeOut`/`EdgeIn`
  are anchor-major, there is one writer and one database per namespace, and
  nothing in the code shards a predicate across anything. Where the plan
  doc's own loose language and the actual key encoding disagree, the code —
  and this chapter — side with the code.
]

== Next: fanning one upsert into many keys

Every key this chapter built sorts right and lands in a keyspace where
twelve record types coexist without collision. But no single call this book
has described yet actually writes more than one of them at once, and a real
upsert never touches just one. Adding one `MENTIONS` edge means a `Node`
read, an `EdgeOut` put or merge, an `EdgeIn` put or merge on the other
endpoint, possibly an `EdgeProp` companion, a `Log` entry, and a `latest_seq`
bump — and RFC 0004's whole promise is that all of it commits together, at
one seq, or none of it does.

#question-box[
  How does one upsert actually become the single atomic `WriteBatch` that
  promise requires? Which of those keys are plain `put`s, which are
  associative merge operands the `GraphMergeOperator` resolves later, and
  which — like a posting-list split or rollup — are deliberately *not*
  merges at all, because the operation isn't associative and only a single
  writer doing a read-modify-write can do it safely? And how does the merge
  operator, given nothing but a key's bytes, know which of those three
  buckets any given operand belongs in?
]

That is `src/merge.rs`, and it is the next chapter.
