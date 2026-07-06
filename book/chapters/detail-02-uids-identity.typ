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

= UIDs & Identity: Minting the Numbers

#info-box(title: [Learning goals])[
  Read this chapter if you want to know:
  - how turbolay hands out a dense `u64` uid without an fsync per id, and why the
    unit of durability is a *block*, not a uid;
  - what a crash does to a half-used block — and why the *gap* it leaves is
    something the design welcomes rather than repairs;
  - why there is no race to allocate a uid *inside* a namespace, and what deleting
    Dgraph's Zero bought and cost to get there;
  - the exact `Xid -> uid` record, and how a new uid and its mapping commit
    together so a crash never resurrects a dangling id.
]

Last chapter closed on a promise and a worry. The promise: every member of
`{4, 5}` is a plain dense `u64`, and the whole density bargain of Chapter I·2
rests on those integers being handed out *right*. The worry, stated as a
question: what stops two writes from minting the same `u64` for two different
external ids — and if the allocator reserves ids in blocks to dodge a per-uid
fsync, what happens to the unused tail of a block when the writer crashes
mid-block? Does turbolay leak uids forever, or is a gap in the dense range
something it can live with?

The short answers, which the rest of this chapter earns: nothing *races*,
because there is exactly one writer per namespace and allocation is a serial
in-memory bump. And yes, a crash leaks the tail of a block — *by design* —
because the durable unit is the block, and roaring never asked for a
*contiguous* range, only a *dense* one. This is `src/ids.rs`, and it is short,
which is the point: the distributed machinery Dgraph needed for exactly this
job is gone.

== One writer, five counters, no lock

Dgraph mints uids from *Zero* — a dedicated Raft group whose job is to lease
blocks of the uid space (and transaction timestamps) to the Alpha servers that
actually write. Zero exists because Dgraph has *many* writers across *many*
shards, and two of them must never mint the same uid. A lease is a small
distributed transaction: an Alpha asks Zero for the next block, Zero commits the
grant through Raft, the Alpha writes from its leased range.

turbolay has one writer per namespace. That single fact deletes Zero entirely.
There is no second party to coordinate with, so there is no lease, no Raft round
trip, no oracle. Allocation is an `&mut self` method on an in-memory struct
(`GraphAllocators`, `src/ids.rs:74-85`), owned by the one writer and not shared
across threads (`src/ids.rs:70-73`). Handing out the next uid is a field
increment.

That struct bundles five *independent* counters — the five id-spaces one
namespace owns — each its own `SequenceAllocator` (`src/ids.rs:74-85`):

#figure(
  kind: image,
  supplement: [Figure],
  block(inset: 6pt)[
    #set text(size: 9pt)
    #table(
      columns: (auto, auto, 1fr),
      align: (left, left, left),
      stroke: none,
      inset: (x: 9pt, y: 3.5pt),
      table.hline(),
      table.header([*Counter*], [*Width*], [*What it numbers*]),
      table.hline(),
      [`uid`],   [`u64`], [dense node/edge uids — the numbers inside every set],
      [`pred`],  [`u32`], [interned predicate names (`MENTIONS`, `HAS_CHUNK`, …)],
      [`label`], [`u32`], [interned node labels (`Chunk`, `Entity`, …)],
      [`prop`],  [`u32`], [interned property keys (`name`, `text`, …)],
      [`seq`],   [`u64`], [changelog sequence numbers],
      table.hline(),
    )
  ],
  caption: [The five id-spaces of one namespace (`src/ids.rs:74-85`). Each is a
    separate `SequenceAllocator` with its own `Meta` key (`seq/uid`, `seq/pred`,
    `seq/label`, `seq/prop`, `seq/log`; `src/ids.rs:43-51`) and advances
    independently — allocating uids never moves the `pred` counter. The interned
    `u32` spaces cap at ~4 billion distinct names; crossing `u32::MAX` is a
    `debug_assert!`, not a supported condition (`src/ids.rs:59-66`).],
) <fig-allocators>

Everything below is told through the `uid` counter, the `u64` one. The other
four behave identically; only their width and their `Meta` key differ.

== Persist the block, not the id

A uid must survive a crash, and it must never be reused — reusing one would
resurrect a tombstoned node's edges under a new identity. The obvious way to
guarantee that is to persist the counter after every allocation. That is one
durable write per uid, and on S3-backed storage a durable write is the single
most expensive thing you can do. Bulk-loading a million nodes would mean a
million fsyncs *just for the numbering*.

So the allocator does not persist per uid. It persists per *block*. A
`SequenceAllocator` holds a reserved range in memory — a `SeqBlock` of
`base_sequence` and `block_size`, default `4096` (`vendor/common/src/sequence.rs:54`,
`serde/seq_block.rs`) — and hands ids out of it with a bare increment, no I/O:

```
pub fn allocate(&mut self, count: u64) -> (u64, Option<Record>) {
    let remaining = self.block.remaining();
    if remaining >= count {                       // block has room:
        let base = self.block.next_sequence;      //   just bump the cursor
        self.block.next_sequence += count;
        return (base, None);                      //   nothing to persist
    }
    // block exhausted: reserve the next one and hand back its record
    let (new_block, record) = self.init_next_block(count - remaining);
    ...
    (base, Some(record))
}
```

The shape of the return value is the whole idea (`src/ids.rs:103-111`,
`vendor/common/src/sequence.rs:175-200`). Allocating a uid returns
`(Uid, Option<Record>)`. The `Uid` is always real and usable. The `Record` is
`None` on the common path — the block still had room, and nothing needs to hit
storage. It is `Some` only when this allocation *crossed a block boundary*, and
then the `Record` is the new 16-byte `SeqBlock` reservation the caller must
persist. One durable write buys 4,096 uids.

And the caller never pays even that as a separate round trip. The `Record` gets
*folded into the write the caller was already making* — the node mutation, the
`Xid -> uid` mapping, the changelog entry — and rides to storage in the same
atomic batch (`src/ids.rs:11-18`, `src/write.rs:697-699`). Reserving the next
4,096 uids is free on the write you had to do anyway.

#boxeq[
  Allocation is an in-memory bump; only a *block boundary* costs a write, and
  that write folds into the batch you were already committing. The durable unit
  is the block — never the uid.
]

== What a crash does to the tail

Here is the subtle part, and it is exactly the worry from last chapter. The
durable state is *block-granular*. The persisted `SeqBlock` records that the
range `[base, base + 4096)` is reserved. It does *not* record how far into that
range the cursor got. There is no durable "next uid" — only a durable "this
block is mine."

So on restart, recovery cannot know whether the writer had consumed one uid of
the block or four thousand. It makes the only safe assumption: *the whole block
is spent.* `load` reads the `SeqBlock` and sets the next sequence to
`base + block_size` — the block's end — so the very next allocation reserves a
*fresh* block past it (`vendor/common/src/sequence.rs:110-119`; `remaining()`
is immediately `0`, forcing a new reservation):

#figure(
  kind: image,
  supplement: [Figure],
  block(inset: 8pt)[
    #set text(size: 9pt)
    #let slot(f, l, w, body) = box(width: w, height: 20pt, radius: 2pt,
      fill: f, stroke: l + 0.6pt, align(center + horizon, text(size: 8.5pt, body)))
    #align(center)[
      *Before the crash* — block `[0, 4096)` reserved, cursor at 6
      #v(5pt)
      #stack(dir: ltr, spacing: 0pt,
        slot(rgb("#e7efe9"), rgb("#2f8a4f"), 150pt, [used: uids `1..6`  (the cast)]),
        slot(rgb("#f2ede2"), rgb("#c8791f"), 230pt, [reserved, unused: `7 .. 4095`]),
      )
      #v(2pt)
      #box(width: 380pt, align(right)[#text(size: 8pt, fill: rgb("#8a8f98"))[persisted `SeqBlock{ base: 0, size: 4096 }` — no cursor]])
      #v(12pt)
      #text(fill: rgb("#b23b3b"))[#sym.arrow.b.double *crash, then restart*]
      #v(12pt)
      *After recovery* — resume at `base + size = 4096`
      #v(5pt)
      #stack(dir: ltr, spacing: 0pt,
        slot(rgb("#f6ecec"), rgb("#b23b3b"), 230pt, [`7 .. 4095` — #text(style: "italic")[leaked forever]]),
        slot(rgb("#e3ecf7"), rgb("#3b6fb0"), 150pt, [next block: uids `4096 ..`]),
      )
    ]
  ],
  caption: [A crash after minting six uids of a 4,096-wide block. The durable
    `SeqBlock` names the *range*, not the cursor, so recovery conservatively
    skips to the block's end (`vendor/common/src/sequence.rs:110-119`). Uids
    `7..4095` are never handed out — a gap of up to `block_size - 1` per crash.
    The uid range stays *monotonic and reuse-free*; it is simply no longer
    *contiguous*.],
) <fig-leaked-tail>

That gap is the price of the amortization, and the design pays it gladly. Recall
the density bargain from Chapter I·2: roaring compresses the *clustering of the
universe*, not the count of values. It stays small when uids fall into a few
well-filled chunks — and a hole of 4,000 numbers inside a 65,536-wide chunk
barely dents the fill. I·2's own warning box said it outright: "deleted nodes
and abandoned id blocks leave gaps, which roaring tolerates fine. The enemy is
2^64 spread, not the occasional hole." A crash leaks at most `block_size - 1`
uids; over a `u64` space you could crash every second for the age of the
universe and never notice. *Dense-ish is the contract; contiguous was never
promised.*

This is the same trade every database sequence cache makes. MySQL's
`AUTO_INCREMENT` and Postgres's `CACHE n` sequences both reserve a range in
memory and abandon its tail on restart — which is why the expert reader has
seen auto-increment ids skip after a crash. turbolay's `SeqBlock` is that cache,
made durable and folded into the write batch. Gaps in a sequence are a normal,
well-understood cost; turbolay just leans into them because roaring makes them
genuinely free.

#note[
  The raw counter starts at `0`, and `Uid(0)` is reserved as the `NIL` sentinel
  (`src/ids.rs:20-25`). Whether a namespace ever *stores* uid 0 is a write-path
  policy call, not the allocator's concern — it stays dense and simple. The
  running cast is numbered `1..6` for readability; nothing in `ids.rs` offsets
  the sequence to guarantee that.
]

== The xid to uid mapping

Users do not hold uids. They hold *external ids* — `"source_1"`, `"chunk_10"`,
`"Ada"` — arbitrary strings from their own world. Internally turbolay works only
in dense `u64`. Something has to translate, and translate *stably*: the same
xid must always resolve to the same uid, forever, or a user's second write to
`"Ada"` would land on a different node than the first.

That translation is one record type: `Xid`, the forward map `xid -> uid`. The
key wraps the external string; the value is the uid as a fixed `u64`
*big-endian* (`src/ids.rs:199`, `src/serde/keys.rs:254`). Here is the pair for
the first cast member, byte for byte:

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
      [key], [`0`],     [`05`],                          [`SUBSYSTEM` (`src/serde/mod.rs:39`)],
      [key], [`1`],     [`01`],                          [`KEY_VERSION`],
      [key], [`2`],     [`90`],                          [`Xid` tag byte — nibble `0x9` in the *high* nibble, reserved `0x0` low (`src/serde/record_tag.rs:52,69`)],
      [key], [`3–10`],  [`73 6f 75 72 63 65 5f 31`],     [`"source_1"`, ASCII],
      [key], [`11`],    [`00`],                          [terminator (`terminated_bytes`, `src/serde/keys.rs:256`)],
      [value], [`0–7`], [`00 00 00 00 00 00 00 01`],     [uid `1` as `u64` big-endian],
      table.hline(),
    )
  ],
  caption: [The `Xid` record for `source_1 -> 1`. The key is the 3-byte head
    plus the external string, `0x00`-terminated (embedded `0x00`/`0x01` bytes
    are escaped, so any string is legal). The value is the uid big-endian, so
    byte order equals numeric order (RFC 0003). The whole running cast is six
    such records.],
) <fig-xid-record>

The rest of the cast is the same shape — six forward mappings, dense values
1 through 6:

#figure(
  kind: image,
  supplement: [Figure],
  block(inset: 8pt)[
    #set text(size: 9.5pt)
    #align(center)[
      #grid(columns: (auto, auto, auto), column-gutter: 22pt, row-gutter: 7pt,
        castbox(src_fill, src_line, [`source_1`]), [#sym.arrow.r], castbox(src_fill, src_line, [`1`]),
        castbox(chk_fill, chk_line, [`chunk_10`]), [#sym.arrow.r], castbox(chk_fill, chk_line, [`2`]),
        castbox(chk_fill, chk_line, [`chunk_11`]), [#sym.arrow.r], castbox(chk_fill, chk_line, [`3`]),
        castbox(ent_fill, ent_line, [`Ada`]),      [#sym.arrow.r], castbox(ent_fill, ent_line, [`4`]),
        castbox(ent_fill, ent_line, [`Engine`]),   [#sym.arrow.r], castbox(ent_fill, ent_line, [`5`]),
        castbox(ent_fill, ent_line, [`Babbage`]),  [#sym.arrow.r], castbox(ent_fill, ent_line, [`6`]),
      )
    ]
  ],
  caption: [The running namespace's `Xid` table: arbitrary external strings on
    the left, dense uids on the right. The external namespace is as sparse or
    random as the user likes; the internal one is dense by construction.],
) <fig-xid-cast>

Notice what is *not* here: there is no reverse `uid -> xid` record. Turbolay
never needs a second index for it, because the uid's `NodeRecord` already
carries its xid as a field — projecting a result back to its external id is a
read of the node, not a separate lookup (RFC 0004 §"xid → uid"). The next
chapter opens that record; here it is enough to know the back-reference lives
inside it.

== Resolve-or-create, and why the uid can't dangle

The write path resolves an xid through one function,
`resolve_or_create_xid_batched` (`src/ids.rs:178-208`). Its logic is four lines
of intent:

```
fn resolve_or_create(xid) -> (uid, ops):
    if let Some(rec) = get(Xid[xid]) {           // fast path: already mapped
        return (u64::from_be(rec.value), [])     //   nothing to persist
    }
    let (uid, block) = alloc.allocate_uid()      // first sight: in-memory bump
    ops = [ block?, Put(Xid[xid], uid.be()) ]    // mapping (+ block if crossed)
    return (uid, ops)
```

Two paths. If the xid already maps, the function reads its uid and returns an
*empty* op list — there is nothing new to write, and resolution is idempotent by
construction (`src/ids.rs:184-194`). If the xid is new, it allocates a uid — the
in-memory bump, which may or may not cross a block boundary — and returns the
uid together with the *ops the caller must persist*: the `Xid -> uid` mapping,
and, only if the allocation crossed a boundary, the `SeqBlock` reservation
(`src/ids.rs:196-207`).

The function itself never writes to storage. That is deliberate, and it is where
the "no dangling id" guarantee lives. If resolving an xid eagerly persisted the
mapping in its own round trip, a crash between *that* write and the rest of the
write — the node record, the changelog — would leave a uid half-born: a mapping
pointing at a node that does not exist, or worse, a uid handed out but with its
block reservation not yet durable. So the ops are handed *back*, and the write
path folds them into the one atomic batch it commits at a single seq
(`src/write.rs:326-329`, `src/write.rs:718-727`). The uid, its xid mapping, its
block reservation, and the data that uses it all become durable together, or
none of them do. A new uid and its identity commit as one fact.

#figure(
  kind: image,
  supplement: [Figure],
  block(inset: 6pt)[
    #set text(size: 9pt)
    #table(
      columns: (auto, 1fr),
      align: (left, left),
      stroke: none,
      inset: (x: 9pt, y: 4pt),
      table.hline(),
      table.header([*In the one atomic batch*], [*Why it must be here*]),
      table.hline(),
      [`Put Xid[chunk_10] = 2`],        [the identity — resolve must be stable across restarts],
      [`Put seq/uid = SeqBlock{..}`],   [only if this uid crossed a block boundary — reserve it durably],
      [`Put Node[2] = {..}`],           [the node the uid names],
      [`Put Log[seq] = change`],        [the changelog entry at this seq],
      [`Put latest_seq = seq`],         [the durable high-water mark],
      table.hline(),
    )
  ],
  caption: [`upsert` folds the resolve ops into the same batch as the mutation
    and commits all-or-nothing at one seq (`src/write.rs:326-329, 718-727`). A
    crash either leaves every row present or none — never a uid without its
    mapping, never a mapping without its block reservation.],
) <fig-atomic-batch>

The single-writer assumption is what makes the four-line `resolve_or_create`
correct without a lock. In a multi-writer store, "check if `Xid[xid]` exists,
and if not allocate" is a classic race — two writers both miss, both allocate,
both claim the string — and you would need `INSERT ... ON CONFLICT`, a uid
oracle, or an optimistic retry to close it. turbolay has none of these because
the premise removes the race: within a namespace the writes are *serial*, run by
one writer, so the check and the allocate are never interleaved with another
allocation (RFC 0004 §"xid → uid"). This is the payoff of the whole
architecture, seen at the smallest scale. There is no concurrency to defend
against inside a namespace, so there is no concurrency-control code here at all.

#note[
  *Built vs. design.* *Built (M1), on real SlateDB:* the block-reserved
  `SequenceAllocator` for all five id-spaces, monotonic and reuse-free across
  restarts (`src/ids.rs`, `vendor/common/src/sequence.rs`; the round-trip and
  post-restart-no-reuse tests in `src/ids.rs:262-509`); `resolve_or_create_xid_batched`
  wired into `upsert_node`/`upsert_edge` and committed in one atomic batch
  (`src/write.rs`). *A note on the record, not a claim:* RFC 0004 §"UID
  allocation" describes exactly this block scheme, so here the code and the RFC
  agree — but RFC 0004's `NodeRecord` example says the node value is `bincode`,
  and the code is a hand-rolled little-endian codec; where they diverge, that is
  the *next* chapter's business, and the code wins. *Design, not built:*
  multi-writer allocation (RFC 0016) — which would resurrect a coordinator very
  much like Zero — is a future stub; today the single-writer premise is load-bearing.
]

== Next: what actually lives at `Node[uid]`

A uid is now a real, durable, dense number, and its xid resolves to it and back.
But a uid is just an *address*. Chapter I·2 admitted the other half of a graph:
a node's actual substance — Ada's name, a chunk's text, its labels — is not a
set at all. It is a blob. And that blob sits at exactly one key: `Node[uid]`.

#question-box[
  What is stored at `Node[2]`? Turbolay packs labels, properties, and the xid
  back-reference into *one monolithic value* behind a hand-rolled, fail-closed
  codec — not `bincode`, whatever RFC 0004's example says. Why write your own
  encoder instead of reaching for the house serializer? And why does a node
  that serializes past *1 MiB* get *rejected outright*, when an adjacency
  posting past *512 KiB* is quietly *split* instead — the exact opposite
  policy on almost the same threshold?
]

That is the node record, and it is the next chapter.
