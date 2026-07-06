#import "../vendor/bookly/src/bookly.typ": *

= The Write Path

A user does the smallest possible thing: adds one edge. `chunk_10` mentions Ada.
One arrow. Follow it down through everything we have built — the keyspace, the
posting lists, the substrate — and watch how much has to happen, and how all of it
happens *at once*.

== One edge, many records

That single edge does not become one write. It becomes a *fan-out* — a batch of
records, because an edge touches the endpoints, the schema, both adjacency
directions, the degree counts, and the log. Here is the whole batch for adding
`chunk_10 -MENTIONS-> Ada` when both nodes and the predicate are brand new:

#figure(
  kind: raw,
  supplement: [Listing],
  block(inset: 4pt)[
    #set text(size: 8.6pt)
    ```
    BATCH — one atomic apply, seq = 1

      endpoints     Put    Xid["chunk_10"] -> 2       Put   Node[2]  (stub)
                    Put    Xid["Ada"]      -> 4       Put   Node[4]  (stub)
      intern pred   Put    SchemaName["MENTIONS"] -> 0
                    Put    SchemaId[0] -> {name:"MENTIONS", reverse:true}
      the edge x2   Merge  EdgeOut[2][0] += 4         Merge EdgeIn[4][0] += 2
      degree        Merge  count[out, 2] += 1         Merge count[in, 4] += 1
      changelog     Put    Log[1] -> "edge added: 2 -MENTIONS-> 4"
      the token     Put    Meta["latest_seq"] -> 1
    ─────────────────────────────────────────────────────────────
      commit; return token = 1
      (after commit, unlogged)   maybe_split(EdgeOut[2][0]), maybe_split(EdgeIn[4][0])
    ```
  ],
  caption: [One upsert fans out into one atomic batch. Nine records for a
    first-of-its-kind edge; a steady-state re-add of an edge between existing nodes
    on a known predicate collapses to the four middle rows plus the log and token.],
) <fig-fanout>

Read it top to bottom and every record earns its place. The two endpoints do not
exist yet, so each gets a dense UID from the allocator, an `Xid` mapping so the user
can find it again, and a stub `Node` record. The predicate `MENTIONS` has never been
seen, so it is interned to the id `0` and its two schema records are written. Then
the edge itself, *twice* — `EdgeOut` and `EdgeIn` — and a `+1` on each endpoint's
degree. Finally the changelog line and the new sequence number.

#note[
  One detail elided: the UID, predicate, and sequence allocators each hand out ids
  in pre-reserved blocks, and every so often a write also carries a tiny record that
  reserves the next block. It rides in the same batch, so a crash can never hand the
  same id to two different things.
]

== All or nothing

That whole batch is handed to SlateDB as *one* atomic write. This is the guarantee
I·4 promised, and now it earns its keep:

#boxeq[
  The batch commits *entirely* or *not at all*, at one sequence number. There is no
  partial state to repair.
]

Picture the alternative — the fan-out as separate writes, with a crash in the
middle. You could land `EdgeOut[2][0] += 4` but not `EdgeIn[4][0] += 2`: a *torn
edge*. "What does chunk_10 mention?" would say Ada; "who mentions Ada?" would not
say chunk_10. The graph would silently disagree with itself. Or you could write the
`Log` line and the `latest_seq` for a node whose records never landed — a token
pointing at nothing.

Atomicity forbids all of it. A reader never sees a node without its edge
projections, never a changelog entry naming records that are not there. Either the
edge is wholly there, at seq 1, or the namespace looks exactly as it did before.

== Upsert is merge, not overwrite

Adding a *property* to a node does not overwrite the node. The writer reads the
current `NodeRecord`, *merges* the change in, re-encodes the whole blob, and writes
it back:

- new *labels* are unioned in (v0 upsert only adds; removing a label is a later
  feature);
- new *properties* overlay by key — the new value wins, the untouched ones stay;
- the *xid* is never changed — it is the handle the node is found by.

And the 1 MiB cap is checked *before* anything is queued. An oversize node is
rejected with nothing written at all — not even its `Xid` mapping, not a bump to the
sequence number. The write simply did not happen.

== Delete is a tombstone

Here is the move that makes deletes cheap. Deleting an edge does *not* rewrite the
posting to remove a member — for a node with ten million edges, that would rewrite
the whole ten-million-element value. Instead the writer adds the UID to a small
*deleted* set:

#info-box(title: none)[
  `delete_edge(chunk_10, MENTIONS, Ada)` writes a handful of tiny records: tombstone
  `4` into `deleted_edges` (both directions), decrement the two degree counts, drop
  the edge's facet record. The million-element posting is *never touched*. Deleting
  one edge from a supernode costs the same as deleting one from a leaf.
]

Reads reconcile it by subtraction: a one-hop read is *posting #sym.minus deleted
edges #sym.minus deleted nodes*, so the tombstoned member never surfaces even though
the raw set still contains it. The tombstones are physically folded out only later,
in a background rollup, once enough of them accumulate — a deliberate someday, not
part of this write.

== Two ways to write: merge, and rewrite

Look back at the batch and notice two different verbs: `Put` and `Merge`. They mark
a real distinction the whole write path turns on.

Some combinations are *associative* — the order does not matter. Adding UID `4` and
then UID `7` to a set is the same set either way; two `+1`s on a counter make `+2`
regardless of order. For these, the writer emits a `Merge` and lets SlateDB fold the
operands together lazily — no need to read the current value first. That is why
adding an edge never reads the posting on the hot path.

Other operations are *not* associative and need the current value in hand: rewriting
a whole node blob, or splitting a 512 KiB posting into parts. These are done as a
*read-modify-write* by the writer. And this is the quiet payoff of the whole design:

#boxeq[
  A read-modify-write is safe with no locks and no coordinator — because there is
  exactly *one* writer. Nobody can clobber the value between the read and the write.
]

- *Merge* — an associative combine; skip the read, fold lazily (set union, counter sum).
- *Put* — last-write-wins on a whole value (a node blob, a schema record, the token).
- *Read-modify-write* — a non-associative reshape only the single writer may do (the split).

#note[
  The 512 KiB split is a *physical* reorganization — it rearranges bytes without
  changing the logical set of members. So it rides *outside* the atomic batch,
  carries no sequence number, and writes no changelog line. If it is lost to a crash,
  the next add to that key simply re-checks and redoes it.
]

== The sequence number

The last two records in the batch are the changelog line and `latest_seq`, and they
share one number: the *sequence number*, `seq = 1`. The writer picks it as one past
the current maximum, stamps it into the changelog, records it as the namespace's new
`latest_seq`, hands it to SlateDB as the write's durable sequence number, and returns
it to the caller.

That returned number is a *token*. It is the caller's receipt: "your write is
durable at seq 1." Because turbolay's own sequence number *is* SlateDB's durable
sequence number, a later read can be told "do not answer me until you have caught up
to seq 1" — and mean it precisely. That is the thread the read path pulls on, two
chapters from now.

== Next: what single-writer deletes

Twice now the design has leaned on the same fact — a read-modify-write is safe
because there is one writer; the sequence is one clean lineage because there is one
writer. We have treated "one writer per namespace" as a convenience. It is far more
than that.

#question-box[
  Dgraph is mostly a *distributed* database: Raft consensus, a Zero server handing
  out transaction timestamps, multi-version concurrency control, conflict detection.
  turbolay has *none* of it. How much of a graph database is machinery that exists
  only to coordinate multiple writers — and what happens to all of it the moment you
  decide there is just one?
]

That accounting — the largest deletion in the project — is the next chapter.
