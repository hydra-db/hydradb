#import "../vendor/bookly/src/bookly.typ": *

= The Posting Value: A Set, on the Wire

#info-box(title: [Learning goals])[
  Read this chapter if you want to know:
  - how a `RoaringTreemap` becomes the *exact bytes* stored at an adjacency key;
  - why a one-byte *format tag* lets turbolay change codecs (CSR, bitpacked
    frames) with no data migration;
  - when and how a supernode's posting *splits* at the 512 KiB threshold;
  - which set operations (#sym.union #sym.inter #sym.minus) are actually
    exercised today, and which are still the planner's job.
]

Part I left this promise dangling: "the exact encodings are Part II's business."
Time to collect on it. `EdgeOut[chunk_10][MENTIONS]` resolves to a `RoaringTreemap`
holding `{4, 5}` — a real Rust struct, with a `BTreeMap` of 32-bit containers
inside it. SlateDB does not store Rust structs. It stores `Bytes`. Somewhere,
something has to turn one into the other, and it has to do it in a way that a
reader — possibly a reader compiled a year later, possibly running a smarter
codec than roaring — can still make sense of.

#question-box[
  You own the on-wire format for every adjacency and index value in the
  database. If you serialize the `RoaringTreemap` and nothing else, you have
  frozen roaring into the keyspace forever — the day you want a CSR layout for
  hot predicates (RFC 0009) or a bitpacked frame for cold ones (RFC 0010), you
  are stuck dual-reading every historical key by *guessing* its shape. And a
  single node's posting can still grow past whatever you consider a
  reasonable single-value size. What do you put around the raw roaring bytes
  so that both problems have an answer, and a corrupt or unknown byte is
  *rejected*, never silently misread?
]

That wrapper is `PostingValue` (`src/posting.rs:90-95`). This chapter is its
byte layout, why each byte earns its place, and what the type still owes RFC
0005's read-side promises.

== The type behind the shelf

`PostingValue` is deliberately thin — two fields, no cleverness:

```
pub struct PostingValue {
    pub format: u8,
    pub kind: PostingKind,   // Single(RoaringTreemap) | Split(Vec<PartRef>)
}
```

`format` names the codec; `kind` names the shape. Everything else — the
roaring bytes themselves, the split manifest's part references — is payload
behind those two tags. One module-level house rule governs every integer
inside it: *keys are big-endian, values are little-endian* (`src/posting.rs:13`,
`src/value.rs:17`). Part I's keyspace chapter told you why keys are
big-endian — so byte order is UID order. Values get no such requirement, so
turbolay just picked LE and applied it uniformly, `PartRef`'s fields included.

== The byte layout, exactly

Take the running edge, `EdgeOut[chunk_10][MENTIONS] = {4, 5}`, still small
enough to be a `Single` posting. `PostingValue::serialize` (`src/posting.rs:160-182`)
writes a 2-byte header, then hands the roaring set to its own
`serialize_into` untouched:

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
      table.header([*Layer*], [*Offset*], [*Bytes (LE)*], [*Field*]),
      table.hline(),
      [`PostingValue`], [`0`],     [`01`],                    [`format` = `FORMAT_ROARING_V1`],
      [`PostingValue`], [`1`],     [`00`],                    [`kind` = `KIND_SINGLE`],
      [`RoaringTreemap`], [`2–9`],  [`01 00 00 00 00 00 00 00`], [bucket count = 1 (`u64`) — one high-32 bucket touched],
      [`RoaringTreemap`], [`10–13`],[`00 00 00 00`],            [bucket key = 0 (`u32`) — `4` and `5` share high bits `0`],
      [`RoaringBitmap`], [`14–17`],[`3a 30 00 00`],            [cookie `12346`, no-run-container (`u32`)],
      [`RoaringBitmap`], [`18–21`],[`01 00 00 00`],            [container count = 1 (`u32`)],
      [`RoaringBitmap`], [`22–23`],[`00 00`],                  [container key = 0 (`u16`) — shared high-16 of the low-32],
      [`RoaringBitmap`], [`24–25`],[`01 00`],                  [cardinality − 1 = 1 #sym.arrow.r 2 members (`u16`)],
      [`RoaringBitmap`], [`26–29`],[`10 00 00 00`],            [container data offset = 16 (`u32`)],
      [array store],     [`30–31`],[`04 00`],                  [member `4` (`u16`)],
      [array store],     [`32–33`],[`05 00`],                  [member `5` (`u16`)],
      table.hline(),
    )
  ],
  caption: [The full 34 bytes stored at `EdgeOut[chunk_10][MENTIONS]`. The
    first two bytes are turbolay's own header (`src/posting.rs:151-159`); every
    byte after offset 2 is `roaring 0.10`'s own on-disk format — a `RoaringTreemap`
    wraps one `u32` bucket key per touched high-32 range around a nested
    32-bit `RoaringBitmap`, itself the interoperable Roaring format shared with
    the Java/Go/C++ implementations. `PostingValue::serialized_len()` reports
    this as `34` exactly (`src/posting.rs:184-192`) — the number the split
    threshold below actually checks.],
) <fig-postingvalue-bytes>

Nothing in that table is guesswork: it is what `RoaringTreemap::serialize_into`
and `RoaringBitmap::serialize_into` actually write for a two-element set whose
values fit one array-store container (`roaring` crate, `treemap/serialization.rs`,
`bitmap/serialization.rs`). turbolay adds exactly two bytes around it. A
`Split` value's body looks different — no roaring bytes at all, just a
manifest — and the next section is why.

#boxeq[
  Byte 0 says *which codec*; byte 1 says *inline set, or a manifest of
  parts*. Everything after those two bytes is untouched payload — either
  roaring's own serialization, or a list of `PartRef`s. Decode the header
  first, or not at all.
]

== The format tag: a codec change without a migration

`FORMAT_ROARING_V1` is `1` (`src/posting.rs:21-23`). That is the *only* value
ever written today — `PostingValue::single` and `PostingValue::split` both
hard-code it. But the byte exists, is checked on every read, and is already
carrying meaning nothing writes yet:

- `2` is reserved for a bitpacked frame encoding — RFC 0010's UidPack-style
  256-uid blocks with block-max metadata, for postings where roaring's
  decode or intersection cost dominates a real profile.
- `3` is reserved for CSR — RFC 0009's materialized offsets/partners layout,
  for hot predicates where a leapfrog triejoin wants contiguous partner
  slices instead of a compressed bitmap.
- Anything else is not a future format; it is corruption. `deserialize`
  checks `format != FORMAT_ROARING_V1` and returns `Err`, never guesses
  (`src/posting.rs:204-209`).

That third bullet is where the design earns its keep: it is a strict
gain over both the systems it stands between. Neo4j's fixed relationship
record has no version tag at all — a store-format change is a full-database
upgrade pass. A schema or codec change on a large sharded Vitess/PlanetScale
fleet needs an online-migration tool (`gh-ost`-style shadow tables, cutover)
precisely because there is no per-row escape hatch. turbolay's reader checks
one byte and either knows the codec or refuses the value — so the day RFC
0009 or 0010 lands, old roaring-v1 keys and new CSR/bitpacked keys coexist in
the same keyspace, decoded by the same reader, with zero keys rewritten to
make room for the new format. RFC 0005 calls this property "CSR-ready"
(§"Why roaring, not UidPack"); the cost was one write-only byte, paid from
day zero.

#note[
  The `PartRef` records introduced below carry the same seam forward:
  `min_uid`/`max_uid`/`card` are metadata a CSR reader would use to skip whole
  parts by range without decoding them (RFC 0009). They are written and
  checked today for an unrelated reason — routing adds and detecting
  oversize parts — and simply *also* happen to be exactly what RFC 0009
  needs later. Nothing about them changes when that RFC ships.
]

== Splitting a supernode: the manifest and its parts

A `Single` posting that serializes past `SPLIT_THRESHOLD` — `512 * 1024`
bytes, `src/posting_ops.rs:55` — stops being one value. The base key becomes a
`Split` manifest, and the members move to `EdgePart[dir][anchor][pred][start_uid]`
keys, one per part:

```
pub struct PartRef {
    pub start_uid: u64,  // also the part's key
    pub min_uid: u64,    // RoaringTreemap::min(), O(1)
    pub max_uid: u64,    // RoaringTreemap::max(), O(1)
    pub card: u32,       // RoaringTreemap::len(), O(1)
}
```

Four fields, 28 bytes on the wire (`8+8+8+4`, `src/posting.rs:28-30`) — no
roaring internals, just enough to route an add and skip a part at read time
without opening it. The base key's `Split` body is `part_count: u32` followed
by that many 28-byte `PartRef`s back to back (`src/posting.rs:171-181`); for
two parts that is `2 + 4 + 2*28 = 62` bytes, regardless of how many millions
of members those two parts hold between them — the manifest is the thing
you actually pay to read on a whole-set fetch, not the parts.

`bin_split` (`src/posting_ops.rs:301-329`) builds those parts by bisecting the
set at its median member — `set.select(len/2)`, one of roaring's O(log) rank
operations — into a low and high half, recursing on each until every part's
`serialized_len()` is back under threshold. `maybe_split` is a deliberate,
single-writer read-modify-write, never a merge operand (RFC 0005
§"Splitting supernodes"; Part I's write-path chapter already told you it
"rides outside the atomic batch, carries no sequence number" — this is what
it actually rewrites). A part that later crosses threshold on its own gets
flattened into fresh siblings by `reconcile_oversized_parts`
(`src/posting_ops.rs:436-501`), never nested — an `EdgePart` value is a
`Single`, full stop; `crate::merge` panics if one ever decodes as `Split`.

Reading a split posting is the manifest's whole reason to exist: `neighbors`
(`src/posting_ops.rs:219-246`) fetches the base key, and only for a `Split`
kind does it fan out to every named part and fold them with
`PostingValue::union_parts` (`src/posting.rs:143-149`) — a plain `RoaringTreemap`
`|=` loop, order-independent. A query that only needs a uid range would
instead skip any part whose `[min_uid, max_uid]` doesn't overlap it — the
seam RFC 0009 exploits, not yet wired into the one-hop read path.

== The merge operand's hidden requirement

RFC 0005 describes the fast-add path in one line: `batch.merge(key,
roaring_singleton(neighbor))`. The code does something slightly different,
and the gap between the two is worth knowing because it was found the hard
way. `crate::merge`'s module doc (`src/merge.rs:27-70`) explains: real SlateDB's
`MergeOperatorIterator` resolves a run of merge entries in batches of up to
100, and — even for a single batch — makes one further `merge_batch(key,
base_value, results)` call to fold those intermediate results against the
real stored value. That means a `merge_batch` call's own *no-existing-value*
output must be directly re-usable as an *operand* in a later call to the same
function: SlateDB has no separate "partial" vs "full" merge.

For the `Meta` deleted-bitmap keys, bare `RoaringTreemap` bytes satisfy this
trivially — operand, stored value, and output are all the same bare encoding.
The adjacency fast-add path did not, originally: operands were bare roaring
bytes, but the *stored* value (and the merge's own output) was a full
`PostingValue::single(..)` with its 2-byte header. The very first write to a
fresh key hit the re-feed case, and that 2-byte header got mis-decoded as
roaring cookie bytes — a panic, caught by the SlateDB acceptance tests (in
`tests/slatedb_acceptance.rs`) precisely because the in-memory test backend's
simpler merge dispatch never re-feeds its own output and so never exercised
this path. The fix (`src/posting_ops.rs:149-177`, `add`): the operand is now a
full `PostingValue::single(singleton)` too, so decoding a stored value and
decoding an operand are the same function. Where RFC 0005's prose and the
code disagree on the operand's exact bytes, the code — and the crash that
shaped it — is the authority.

== Set algebra on the stored form

Everything above only matters because the value has to support the three
operators Part I built the whole model on: #sym.union, #sym.inter, #sym.minus.
On a `RoaringTreemap`, those are `a | b`, `a & b`, `a - b` — library calls,
not code turbolay wrote. What's actually exercised at M1 is narrower than the
full vocabulary:

- *Union* — two places. `union_parts` folds a `Split` posting's parts back
  into one whole-set read (`src/posting.rs:143-149`), a plain `|=` loop. And
  the merge operator's fast-add path (previous section) is itself a union —
  singleton operands folded into the stored set as SlateDB resolves them.
- *Difference* — every one-hop read subtracts twice: the per-`(anchor,pred)`
  deleted-edge bitmap, then the whole-namespace deleted-node bitmap
  (`src/posting_ops.rs:219-246`, matching RFC 0005's `neighbors` pseudocode
  exactly). Rollup (`src/posting_ops.rs:529-598`) is the same subtraction run
  once and folded permanently, when a posting's tombstones exceed 25% of its
  live members (`ROLLUP_DELETED_RATIO`, `src/posting_ops.rs:60`) — Dgraph's
  `IncrRollup`, minus the MVCC version stack it used to collapse.
- *Intersection* — roaring gives it for free (`a & b`), and RFC 0005 lists it
  as the core traversal op, with `len()` feeding the planner's smallest-set-first
  selectivity. Nothing in this crate calls it yet.

#note[
  *Built vs. design.* *Built, on real SlateDB:* `PostingValue` serialize/deserialize for both
  `Single` and `Split` (`src/posting.rs`, round-trip + fail-closed adversarial
  tests); size-adaptive `add` via merge, including the operand fix above; and
  one-hop `neighbors` (union of parts, minus deleted edges, minus deleted
  nodes) — both exercised against the SlateDB acceptance tier
  (`tests/slatedb_acceptance.rs`), where the operand bug was actually caught.
  `maybe_split`/`reconcile_oversized_parts` and `maybe_rollup` run as
  single-writer RMWs today, driven by the real merge operator but so far only
  against the in-memory backend (`src/posting_ops.rs` tests), not yet in that
  acceptance tier.

  *Design, not yet written:* format `2` (bitpacked, RFC 0010) and `3` (CSR,
  RFC 0009) are reserved and rejected on read, never produced. Range-filtered
  part reads (skip parts outside a query's uid range) are possible with
  today's `PartRef` metadata but not implemented — every current whole-set
  read touches every part. And the *query-level* set algebra — intersecting
  or subtracting two independently-fetched postings to answer a multi-hop
  pattern — is RFC 0007's planner, M2/M3. What exists today is one anchor's
  one-hop resolve, not the executor that chains several of them with
  #sym.inter and #sym.minus.
]

== Next: minting the numbers inside the set

Every member of `{4, 5}` above is a plain `u64` this chapter trusted blindly.
But a `RoaringTreemap` is only as cheap as Part I's density bargain — and
that bargain depends on those integers being handed out *right*: dense,
monotonic, and never reused, even across a crash mid-write.

#question-box[
  What actually stops two concurrent writes from minting the same `u64` for
  two different external ids? And if the allocator hands out ids in blocks
  to avoid a per-uid fsync, what happens to the unused tail of a block when
  the writer crashes before using it — does turbolay leak uids forever, or
  is a gap in the dense range something it can tolerate by design?
]

That is `src/ids.rs`, and it is the next chapter.
