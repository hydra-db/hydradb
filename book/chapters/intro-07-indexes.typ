#import "../vendor/bookly/src/bookly.typ": *

= Indexes Are Posting Lists Too

The last chapter left a query stranded. "Find the entities *named* Ada." "Find
chunks with *more than five* mentions." Everything we have built answers questions
about *connections* — who points to whom. Neither of those is a connection question:
one is about a *value*, the other about a *degree*. Raw adjacency cannot answer
them.

The answer is an *index*. And the reveal of this chapter is that an index is not a
new kind of thing at all.

#boxeq[
  An index is a posting list. Only the meaning of the key changes.
]

== The same machine, a different key

Adjacency stored a set of neighbor UIDs under the key `EdgeOut[anchor][pred]`. An
index stores a set of *node* UIDs under a key whose tail is a *token* or a *degree*
instead of a source node. Same roaring set, same set algebra — a different
question:

#figure(
  kind: image,
  supplement: [Figure],
  block(inset: 4pt)[
    #set text(size: 8.6pt)
    #table(
      columns: (auto, auto, 1fr),
      align: (left, left, left),
      stroke: none,
      inset: (x: 7pt, y: 4pt),
      table.hline(),
      table.header([*Index*], [*Key*], [*Answers, on the cast*]),
      table.hline(),
      [Value], [`Index[pred][token]`], [`Index[name][token("Ada")]` #sym.arrow.r `{4}` — who is named Ada],
      [Count], [`Count[pred][dir][degree]`], [`Count[MENTIONS][in][2]` #sym.arrow.r nodes mentioned by *exactly* 2 chunks],
      [Reverse], [`EdgeIn[dst][pred]`], [`EdgeIn[4][MENTIONS]` #sym.arrow.r `{2, 3}` — who mentions Ada],
      table.hline(),
    )
  ],
  caption: [Three indexes, one substrate. Each value is a roaring set of node UIDs;
    only the key's tail changes what the set means. Reverse is already live — it is
    the `EdgeIn` we built in the write path.],
) <fig-indexes>

That last row matters: we have *already built an index* and did not call it one. The
reverse projection `EdgeIn`, written unconditionally in the same batch as every edge,
is exactly a materialized index — the always-on one. The value and count indexes are
the same machine, declared per property rather than always on.

#note[
  Honest status: of the three, only the reverse index is live today. The value and
  count indexes are the designed framework — the next milestone's work. What
  follows describes that framework; where a query would use a value or count index,
  read "will," not "does."
]

== Tokenizers: turning a value into a key

A value index needs the *value* — `"Ada Lovelace"`, `42`, `3.14` — turned into the
token bytes of a key. That is a *tokenizer*, and its one job is to make the bytes
*sort correctly*, because the whole keyspace is ordered by raw bytes (recall I·3).

#figure(
  kind: image,
  supplement: [Figure],
  block(inset: 4pt)[
    #set text(size: 8.6pt)
    #table(
      columns: (auto, 1fr, auto, auto),
      align: (left, left, center, center),
      stroke: none,
      inset: (x: 7pt, y: 4pt),
      table.hline(),
      table.header([*Tokenizer*], [*Token*], [*Ranges?*], [*Lossy?*]),
      table.hline(),
      [`exact`], [the whole value, escaped and terminated], [yes (strings)], [no],
      [`int`], [sign-flipped 8-byte big-endian], [*yes*], [no],
      [`float`], [order-preserving 8-byte encoding], [*yes*], [no],
      [`hash`], [a compact fixed-width digest of a long value], [no], [*yes*],
      table.hline(),
    )
  ],
  caption: [Tokenizers. The order-preserving ones (`int`, `float`, `exact`) make a
    range predicate a key-range *scan*; `hash` keeps keys small on long strings but
    can collide.],
) <fig-tokenizers>

The order-preserving trick is what turns comparisons into scans. Because `int`
tokens sort in numeric order, `age > 30` is not a lookup per candidate value — it is
*one contiguous key-range scan* from `token(30)` onward. A range becomes a walk.

`hash` is the exception, and it is honest about the cost. A digest is small and
fixed-width — good for indexing long strings — but two different values can collide
to the same digest. So a hash lookup returns a *candidate* set, not an answer set,
and the reader must re-check each candidate against its real `Node` record. A
*re-fetch* flag marks exactly this. The order-preserving tokenizers are exact; only
`hash` pays the re-fetch tax.

== A query picks an access path

With indexes in place, a filter chooses how to read:

- `prop = v` #sym.arrow.r one lookup on `Index[prop][token(v)]`.
- `prop IN [...]` #sym.arrow.r a *union* of those lookups.
- `prop < / > / <=` (numeric) #sym.arrow.r a bounded key-range *scan*.
- `degree = k` #sym.arrow.r one lookup on the count index.
- combine them with `AND` #sym.arrow.r intersect, `OR` #sym.arrow.r union, `NOT`
  #sym.arrow.r difference — the same roaring algebra from chapter I·1.

And one firm rule: if *no* index supports a predicate, the query *errors* by default.

#warning-box[
  turbolay will not silently full-scan. On object storage a full scan is not a slow
  query — it is a flood of GET requests and a bill to match. An unindexed filter is
  refused unless the caller explicitly opts into brute force on a small namespace.
  Indexes are not an optimization here; they are the admission ticket.
]

== Indexes can lag — and the watermark says how much

Here is the idea the whole next chapter turns on, and it needs stating precisely.

An index is maintained *inside the write batch*. When an edge changes a node's
degree, the count moves in the very same atomic write that added the edge; when a
node is created, its value-index entries go in that batch too. So a *live* index is
exactly current — it never trails the log. (This is why `EdgeIn` never lags: it
rides the same batch as `EdgeOut`.)

Lag appears in exactly one situation: when you *declare a new index over data that
already exists*. You cannot index a million existing nodes in an instant. The index
must be *backfilled* — built from scratch by replaying the changelog from the
beginning, folding each past write into the new posting. While that backfill runs,
the index is behind the log.

How far behind? *Exactly* knowable — because each index carries a *watermark*:

#boxeq[
  An index's *watermark* is the highest `seq` it has folded in. It is a promise:
  "this index reflects every write up to seq #super[W], and nothing after."
]

A `name` index backfilling at watermark 197 while the log stands at 200 is precisely
three writes behind — and you know *which* three (the changelog entries 198, 199,
200). The watermark is committed *with* the index data, in one atomic batch, so a
crash mid-backfill simply resumes, and the watermark can never claim more than is
durably folded in. The index moves through a small lifecycle as it goes — *creating,
backfilling, live, dropping* — and only in *backfilling* does its watermark trail
`latest_seq`.

== Next: hiding the lag

Now every piece is on the table. Adjacency and indexes — fast, but a freshly-declared
one can trail while it backfills. The changelog — every write, in order, at
`Log[seq]`. And the watermark — a single number saying exactly how far each index
has caught up.

A read that trusted a lagging index *alone* would miss the newest writes and answer
stale. And we deleted MVCC's versions in the last chapter precisely so we are *not*
carrying per-key history to paper over that. So the question that has been pending
for two chapters finally comes due:

#question-box[
  If a read may touch an index that is behind the log, how can its answer ever be
  correct — never missing a fresh write, never returning a stale one? The answer is
  a single move: read the fast structures up to the watermark, read the raw log
  *after* it, and merge. It is the trick that pays back everything chapter I·6
  deleted — and getting it exactly right is the most careful machinery in the
  system.
]

That is the read path, and it is the next chapter.
