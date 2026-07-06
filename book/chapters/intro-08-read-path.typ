#import "../vendor/bookly/src/bookly.typ": *

= The Read Path and the Consistency Trick

An index can be behind the log. We said so last chapter, and then asked the
question that has been pending since we deleted MVCC: if a read touches an index
that has not caught up, how can its answer be correct?

Here is the whole trick in one line. The rest of the chapter is why it works.

#boxeq[
  result = ( indexed state up to *W* #sym.minus deleted ) #h(0.4em)#sym.union#h(0.4em)
  ( the log tail after *W*, re-evaluated )

  #v(0.3em)
  #text(0.9em)[where *W* is how far the query's *stalest* index has caught up.]
]

== The base layer is the truth; indexes are a cache

Recall the very first equation of the book: a durable log on S3, folded by
compaction into queryable state. That split is the key to reading.

The *base layer* — the node blobs, both edge projections, the changelog, the
deleted bitmaps — is written synchronously in every atomic batch, so it is always
current. The *indexes* are a fold built on top of it, and a freshly-declared one
can lag while it backfills. So indexes are a *cache* over the base layer. And the
rule for a stale cache is old: trust it where it is fresh, and fall back to the
source of truth where it is not.

That is exactly the equation. The query trusts the fast indexed structures only as
far as they admit — up to their watermark *W* — and for everything after *W* it
goes back to the log, the source of truth, and re-derives the answer.

#note[
  This is why the token matters so precisely. A session token proves the reader has
  replayed the *base layer* up to `latest` — every node, edge, and log entry is
  there. It proves *nothing* about whether the indexes have caught up. The whole
  trick lives in that asymmetry: re-derive the stale part from the layer the token
  guarantees.
]

== Why nothing is missed, and nothing is stale

The equation is exactly correct, in both directions, and the argument is short.

*No write is missed.* Every durable write has a sequence number. The watermark *W*
splits them cleanly: a write at `seq <= W` is, by definition of the watermark,
already folded into the indexed state the query reads; a write at
`W < seq <= latest` is, by definition, a changelog entry in the tail range
`(W, latest]`, so the tail scan catches it. The two ranges *partition* every
sequence up to `latest` at the boundary *W* — nothing can fall through the gap.

*No answer is stale.* Deletes are subtracted: the tombstone bitmaps are written in
the same batch as the write, so they are current to `latest` and removed directly,
no lag. And the tail does not replay the log's *payload* — it *re-evaluates the
query* against the *live base records*. A property changed at the last second is
re-checked against the current node blob, so the tail both *adds* newly-matching
UIDs and *removes* newly-non-matching ones. The answer reflects current truth, not
the index's stale snapshot.

#boxeq[
  Index lag is invisible to callers. A session-token read never returns a stale
  traversal — even when every secondary index is behind — because the part the
  indexes missed is re-derived from base records the token guarantees are present.
]

== The freshness gate: read-your-writes

One step comes before all of that. A write returns its sequence number as a
*token*; the caller keeps the highest token it has seen and presents it on the next
read. The reader then *gates*: it blocks until its own replay position,
`durable_seq`, has reached the token, and only then answers.

Because the token's number *is* the write's durable sequence number, the comparison
`durable_seq >= token` is exact. That single gate delivers *read-your-writes*
across a fleet of *stateless* readers — you can write to the one writer and
immediately read your own write from any reader, because the reader waits until it
has caught up. If it cannot catch up in time, it returns `reader_behind`, which is
*retryable*: try another reader, or fall back to the always-fresh writer.

== Why the trick is usually free

*W* is the *minimum* watermark over the indexes a query touches — because the query
is only as fresh as its stalest structure. And that gives the payoff its final,
important property:

#tip-box[
  In steady state, every index a query touches is *live*, so its watermark equals
  `latest`, so `W = latest`, so the tail range `(latest, latest]` is *empty*. The
  tail scan reads nothing; the merge is a no-op. The overlay costs *nothing* on a
  normal read. It is a backfill-hider, dormant until a new index opens a gap — then
  it fills exactly that gap and closes again.
]

== Walk the trick

Watch it work once. A reverse index is mid-backfill, three writes behind, and a
query needs an edge it has not yet folded in.

#figure(
  kind: image,
  supplement: [Figure],
  block(inset: 5pt)[
    #set text(size: 9pt)
    *Query:* "which chunks mention Ada?", with token `T = 200`. \
    *State at the reader:* `latest = 200`; value index `wm = 200` (live); reverse
    index `wm = 197` (backfilling). The edge `chunk_42 -MENTIONS-> Ada` was written
    at `seq 199`.
    #v(4pt)
    #line(length: 100%, stroke: 0.4pt + luma(70%))
    #v(3pt)
    1. *Gate.* `durable_seq = 200 >= 200` #sym.arrow.r serve. `latest = 200`.
    2. *W* = min(200 value, *197* reverse) = *197*.
    3. *Anchor* Ada via the live value index (fine — current to 200).
    4. *Reverse hop, to W = 197.* The reverse view through 197 does *not* contain
       the seq-199 edge #sym.arrow.r the indexed candidates *miss* `chunk_42`.
    5. *Tail scan* `Log[(197, 200]]` = entries 198, 199, 200. Entry 199 =
       "`chunk_42 -MENTIONS-> Ada` added."
    6. *Materialize + re-evaluate.* Read `chunk_42` and its `EdgeOut` base record
       (both present — replayed through 200); the pattern matches.
    7. *Merge.* (reverse candidates #sym.minus deleted) #sym.union {`chunk_42`}
       #sym.arrow.r *`chunk_42` is returned.*
  ],
  caption: [The tail recovers the exact edge the reverse index had not folded in
    yet. The index lagged by three writes; the tail re-derived those three from the
    base layer. The caller never sees the lag.],
) <fig-trick>

== What kind of consistency this is

It is worth being exact about what this buys, because it is easy to overclaim. What
turbolay offers is *read-your-writes* and *monotonic reads* — strong, useful, and
*not* the same as linearizable or serializable.

#figure(
  kind: image,
  supplement: [Figure],
  block(inset: 4pt)[
    #set text(size: 8.8pt)
    #table(
      columns: (auto, 1fr),
      align: (left, left),
      stroke: none,
      inset: (x: 8pt, y: 5pt),
      table.hline(),
      table.header([*Mode*], [*Guarantee — stated honestly*]),
      table.hline(),
      [No token], [bounded staleness: the reader may lag the writer by up to one poll interval],
      [Session (default)], [read-your-writes + monotonic reads across the fleet; blocks until caught up to your token],
      [Strict], [freshest state the reader can reach — but bounded by one poll interval, *not* linearizable],
      table.hline(),
    )
  ],
  caption: [The three consistency modes. Strict is "freshest within one poll
    interval," not linearizable — there is no forced-refresh in the substrate, and
    we do not fork it to add one.],
) <fig-modes>

So the thing MVCC used to provide — a consistent read while writes proceed — is
rebuilt from three cheap parts: a replay position (`durable_seq`), a minimum
watermark, and a bounded tail re-scan. No versions in any key. That is the whole
payback for the deletion of the previous chapter.

#note[
  Honest status: of this entire read path, exactly *one* piece is built today — the
  one-hop `neighbors` read, which resolves a posting and subtracts the deleted
  bitmaps (`src/posting_ops.rs`). The freshness gate, the watermark math, the
  changelog-tail overlay, the planner, and the openCypher executor are the *design*
  in RFCs 0001 and 0007 (the M2/M3 milestones). The Cypher parser is vendored but
  not yet wired in. This chapter describes the mechanism; the code is one primitive
  and the plan around it.
]

== Next: who is "the reader"?

The gate said the *reader* waits until it has caught up. Throughout the book we have
spoken of "the writer" and "the readers" as if they were different machines. They
are — and that separation is the last idea of Part I.

#question-box[
  If readers are stateless caches that replay the log from S3, then a reader is
  disposable: lose one and a survivor serves; add ten and reads scale ten-fold,
  none of them touching the single writer. What does it take for compute to be that
  cheap — and what is the reader actually doing as it "replays the log"?
]

That is the writer/reader split, and it closes Part I.
