#import "../vendor/bookly/src/bookly.typ": *

// Small visual helpers for the roaring container figure.
#let bcell(on) = box(width: 7pt, height: 7pt, radius: 1pt,
  fill: if on { rgb("#3b6fb0") } else { white },
  stroke: rgb("#9aa0a8") + 0.4pt)
#let acell(n) = box(inset: (x: 4pt, y: 2pt), radius: 2pt,
  fill: rgb("#d3e3f7"), stroke: rgb("#3b6fb0") + 0.5pt, raw(n))

= Roaring Bitmaps and Dense UIDs

The whole database rests on one primitive: a set of UIDs. Last chapter we never
asked what a set *costs*. Now we have to, because the sets get large.

Take a celebrity node with ten million neighbors. As a plain sorted list of 64-bit
integers, that one node's adjacency is:

#boxeq[
  10,000,000 #sym.times 8 bytes = *80 MB* — for a single node's edges.
]

Two problems, both fatal. It is 80 MB sitting in an S3 object you must fetch to
read anything. And to intersect it with another set — the move behind every query
in the last chapter — you would decompress both to flat lists and merge them. We
need the opposite: a set that is *small on disk* and can be *intersected without
ever unpacking it*. A *roaring bitmap* gives us both — if we hold up one end of a
bargain. This chapter is the bargain.

== A set as a map of chunks

A roaring bitmap is not one compressed blob. It is a *map keyed by the high bits of
the numbers*, where each entry holds one *chunk* of the set.

Split every UID into a high part and a low 16 bits. The high part picks a *chunk*;
the low 16 bits are a position *inside* it. One chunk therefore covers a fixed
window of 65,536 consecutive values, and the set stores only the chunks it actually
touches. Each chunk is kept in whichever of two shapes is smaller:

- *Array chunk* — a sorted list of the positions present, 2 bytes each. Used when
  a chunk holds few values (up to 4,096).
- *Bitmap chunk* — one flat block of 65,536 bits (8 KiB), one bit per possible
  position. Used above 4,096 values. It costs the same 8 KiB whether it holds four
  thousand values or all sixty-five thousand — so a *full* chunk works out to about
  *0.125 bytes per value*.

The 4,096 crossover is not arbitrary: 4,096 #sym.times 2 bytes = 8 KiB, the
bitmap's fixed size. Roaring keeps whichever is smaller, per chunk, on its own.

#figure(
  kind: image,
  supplement: [Figure],
  block(inset: 6pt)[
    #set text(size: 9pt)
    #align(center)[
      UID `70_004` #h(0.6em) #sym.arrow.r #h(0.6em)
      #box(inset: 4pt, radius: 2pt, fill: rgb("#eceff2"))[high bits #sym.arrow.r chunk *1*]
      #h(0.4em)
      #box(inset: 4pt, radius: 2pt, fill: rgb("#eceff2"))[low 16 #sym.arrow.r position *4 468*]
    ]
    #v(9pt)
    #grid(columns: (1fr, 1fr), gutter: 16pt,
      align(center)[
        *Array chunk* — sparse \
        #v(3pt)
        #stack(dir: ltr, spacing: 4pt, acell("3"), acell("4468"), acell("51002")) \
        #v(3pt)
        #text(0.85em)[a few positions, 2 bytes each]
      ],
      align(center)[
        *Bitmap chunk* — dense \
        #v(3pt)
        #stack(dir: ltr, spacing: 1.5pt,
          ..(1,1,0,1,1,1,0,0,1,1,1,0,1,1,1,1,0,1,1,1).map(b => bcell(b == 1))) \
        #v(3pt)
        #text(0.85em)[one flat block, 8 KiB, any fill]
      ],
    )
  ],
  caption: [A roaring set splits each UID into a chunk key and a low-16 position,
    then stores each chunk as an array (when sparse) or a flat bitmap (when dense).
    A full bitmap chunk costs ~0.125 bytes per value.],
) <fig-roaring>

A picture for it: read the high bits as a *building number* and the low bits as an
*apartment*. A dense set is "everyone in three buildings" — three full rosters. A
scattered set is "one person in each of a million buildings" — a million
near-empty rosters, each with its own letterhead. Same people, wildly different
paperwork. turbolay stores every posting list as a 64-bit roaring bitmap
(`RoaringTreemap`, `src/posting.rs`).

== Set algebra without unpacking

Now the payoff. To intersect two roaring sets, first line up their chunk keys —
both maps are sorted — and *only compare chunks that share a key*. Chunks on one
side with no partner on the other are skipped untouched, one step each. For the
chunks that do overlap, the work runs in the chunk's own shape:

- bitmap #sym.inter bitmap — a thousand machine words AND-ed together, no
  per-value branching.
- array #sym.inter array — a merge of two short sorted lists.
- array #sym.inter bitmap — test each array value's bit directly.

The result is assembled chunk by chunk, and *the flat list of UIDs is never built*.
A celebrity's ten-million-neighbor set intersected against a three-element query set
touches three chunks and ignores the rest.

#boxeq[
  Two sets meet chunk-to-chunk, in compressed form. This is why roaring replaces
  not just Dgraph's storage codec but its whole intersection engine.
]

In the last chapter's terms, #sym.union / #sym.inter / #sym.minus over *shelves* are
now the same operations over *chunk maps*, done without decompressing. One honest
caveat: this is fast, not O(1) — the cost is proportional to the shared chunks, and
to the smaller of the two sets. Only a set's size, smallest, and largest members
are truly free; roaring tracks those as it goes, and a later chapter reuses them to
skip whole posting parts.

== Why the UIDs must be dense

Here is the bargain. Roaring compresses *the clustering of the universe*, not the
sheer count of values. It is small only when the numbers fall into a few, well-
filled chunks. How many numbers there are barely matters; *where they land* is
everything.

And that is entirely ours to control, because we assign the UIDs. Hand them out as
a dense count — 1, 2, 3, 4, … — and ten million of them occupy only about 150
chunks, each a full bitmap at 0.125 bytes per value:

#figure(
  table(
    columns: (auto, auto),
    align: (left, right),
    stroke: none,
    inset: (x: 10pt, y: 5pt),
    table.hline(),
    table.header([*Ten million neighbor UIDs*], [*Size*]),
    table.hline(),
    [Plain sorted list of `u64`], [80 MB],
    [Roaring, *dense* UIDs (`1, 2, 3, …`)], [#sym.approx 1.2 MB],
    [Roaring, *random* 64-bit ids], [#sym.approx 180 MB · ballpark],
    table.hline(),
  ),
  caption: [The same ten million values in every row — only the id *scheme*
    changes. Dense ids turn 80 MB into ~1 MB; random ids turn it into something
    *larger* than the naive list. Figures are order-of-magnitude.],
) <tab-density>

The 80 MB list becomes about *1 MB*. But flip the id scheme and the compression
inverts into *expansion*: ten million *random* 64-bit ids scatter roughly one per
chunk across the high bits, so you keep ten million near-empty chunks, each paying
its own framing — and the total climbs *past* the naive 80 MB, toward ~180 MB.
Roaring is not magic compression; it is compression that exploits a density we
deliberately engineer.

This mistake has a paper trail. An earlier system keyed its nodes by 128-bit
UUIDs; its own storage format admits it could never beat sixteen bytes per
neighbor, because the low bits of a UUID are uniform noise and delta math extracts
nothing from noise. Same graph, 128#sym.times the bytes — bought entirely by the
choice of id (`docs/rfcs/0000`, D5).

#warning-box[
  *Dense does not mean clustered.* A dense id space keeps the whole *universe*
  compact — a handful of high-bit groups instead of the full range of 2^64. It does
  *not* mean a given node's neighbors sit next to each other; a neighbor set is
  still an arbitrary subset. Roaring wins because the universe is small, not because
  neighbors are contiguous. And "dense" is dense-*ish*: deleted nodes and abandoned
  id blocks leave gaps, which roaring tolerates fine. The enemy is 2^64 spread, not
  the occasional hole.
]

== Dense inside, friendly outside

If UIDs must be a dense internal count, what about the ids users actually hold —
`"chunk_10"`, a URL, a UUID from their own system? Those stay untouched, on the
outside. A user addresses a node by an arbitrary string *xid*; turbolay keeps a
small `xid → uid` index and works internally in dense `u64` (`docs/rfcs/0004`). The
external namespace can be as sparse or random as the user likes; the internal one
stays dense by construction.

The UIDs come from a monotonic allocator (`src/ids.rs`) that hands out ids in
pre-reserved blocks and persists its counter in the *same write batch* as the data
that uses them — so ids are monotonic across crashes and never reused, though a
crash mid-block leaves a harmless gap.

== What we traded away: UidPack

turbolay did not invent storing adjacency as integer sets — Dgraph did, and it
already had a codec for the job: *UidPack*, which cuts the sorted UIDs into 256-id
blocks and delta-encodes each with group varint. On dense sorted data UidPack is
actually about 13% *smaller* than roaring, and it carries scoring metadata
(block-max) that roaring lacks. We replaced it anyway, for reasons that are the
theme of this chapter:

- *One type, every job.* The same roaring bitmap stores adjacency, index matches,
  and the deleted-UID sets. Adding an edge is a set union; deleting one is a set
  difference. UidPack is a storage format; roaring is storage *and* algebra.
- *Free, tested boolean ops.* UidPack needs a hand-written block-skipping
  merge-join to intersect (Dgraph's `algo/uidlist`); roaring's chunk arithmetic
  *is* that engine, in a library.
- *No codec to port.*

We give up that 13% and block-max scoring — deliberately, correctness first. And we
keep the door open: every posting value begins with a one-byte *format* tag
(`src/posting.rs`). Today it is always `roaring`. The values for `UidPack` and `CSR`
are reserved from day one, so those encodings can return for the workloads that
need them without rewriting a single existing key — and a reader rejects any format
it does not know rather than guess.

== Next: millions of sets, and the things that aren't sets

We can now store one set cheaply and intersect two sets fast. But a graph is not
one set — it is *millions* of them: one per node, per edge type, per direction, plus
an index for every queryable property. And the last chapter admitted the other
half: a node's actual *properties* — Ada's name, a chunk's text — are not sets at
all. They are blobs.

#question-box[
  How do you lay millions of these sets, plus the property blobs that aren't sets,
  into one flat key #sym.arrow.r blob store — so that "`chunk_10`'s MENTIONS" is one
  lookup, "everything that mentions Ada" is one lookup the *other* way, and the
  whole store still reads back in sorted order? What does a *key* actually look
  like?
]

That is the graph model, and it is the next chapter.
