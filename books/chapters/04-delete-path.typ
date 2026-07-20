#import "../template.typ": custom-box, srcblock, accent, muted
#import "@preview/fletcher:0.5.7" as fletcher: diagram, node, edge
#import "../vendor/bookly/src/themes/reader.typ": reader-colors

= The Delete Path

The write chapter showed how data arrives. This one shows how it leaves, and the first thing
to say is that it leaves more completely than you might expect from a system built on
append-only storage.

There is a tempting line of reasoning here, and an earlier edition of TurboLay followed it:
the object store cannot edit bytes in place, therefore the engine cannot really remove
anything, therefore every delete must be a marker written at a new epoch and every read must
subtract those markers as it goes. That reasoning has a hidden assumption — that the graph
engine is the layer responsible for versioning. It is not. SlateDB is (Chapter 0, Section
0.7). Once record visibility belongs entirely to the SlateDB snapshot, the graph engine is
free to issue an ordinary `txn.delete`, because "ordinary delete" already means "invisible to
snapshots after this commit, still visible to snapshots before it". The LSM tree writes its
own tombstone and compacts it away on its own schedule, and the graph never has to know.

So the shape of this chapter is: a delete is a delete. `txn.delete` on the adjacency keys,
`txn.delete` on the relationship rows. There is exactly one place where that does not work —
an edge packed inside a compacted adjacency segment, where the thing to be removed is not a
key but a *value inside* a key — and that one place is the only graph-level tombstone left in
the system. The rest of the chapter follows the delete outward: how it reaches readers at
three different layers, what it leaves behind, and which of the three collectors that reclaim
that residue actually runs on its own.

The code is `src/shard/write.rs` for the foreground deletes, `src/shard/maintenance.rs`,
`src/engine/artifact_gc.rs` and `src/engine/index_store.rs` for the collectors.

== A delete is a delete

Start with the outcome, because it is short. Deleting a materialized edge removes three keys
and rewrites two counters:

#srcblock("src/shard/write.rs:3400-3421 (abridged)")[```rust
txn.delete(canonical_key.as_bytes())?;                          // cell/<id>/edge/…
txn.delete(keys::out_edge(cell, edge_type, src, dst).as_bytes())?;  // cell/<id>/e/out/…
txn.delete(keys::in_edge(cell, edge_type, dst, src).as_bytes())?;   // cell/<id>/e/in/…
txn.put(out_degree_key.as_bytes(), encode_u64(out_degree))?;
if let Some((in_degree_key, in_degree)) = in_degree {
    txn.put(in_degree_key.as_bytes(), encode_u64(in_degree))?;
}
txn.put(idem_key.as_bytes(), encode_delete_idempotency(mutation, &result))?;
commit_txn_strict(txn, self.await_durable_writes).await?;
```]

No marker record is written for that edge. No log is appended. After this transaction commits
there is no key anywhere in the cell that says "an edge used to be here". The edge is simply
absent, in the same sense that an edge which was never created is absent.

#custom-box(title: [Why], icon: "tip")[
  A snapshot read is still correct across this delete, and it is correct for a reason that
  costs the graph engine nothing. A query pins a SlateDB snapshot at sequence $S$ before it
  starts (Section 0.7). If the delete commits at a sequence above $S$, the snapshot at $S$
  continues to serve the old value of `cell/<id>/e/out/…` — SlateDB keeps both versions and
  resolves which one a reader sees from the sequence, exactly as it does for an overwrite.
  The engine does not need a second versioning scheme layered on top, and the previous edition
  of this book was describing one it no longer has. The historical-epoch refusal in Section 0.7
  is the honest boundary on this: you can read *as of a live snapshot*, and you cannot ask for
  a sequence SlateDB has already compacted past.
]

The envelope around that transaction is the standard write envelope from the write chapter,
and `delete_edge` (`src/shard/write.rs:3184-3214`) is worth reading beside `write_edge` because
the two are structurally identical: validate the components, `ensure_write_authority`, take a
write permit, lock the cell's `writer_lane`, then a bounded retry loop over a serializable
transaction. There is no lock and no lease. The transaction itself opens at
`IsolationLevel::SerializableSnapshot` and calls `validate_write_fence_txn`
(`src/shard/lifecycle.rs:436-455`) before touching anything — which, as Section 4.7 shows, is
also what makes dropping a whole cell safe.

Two other things happen in every successful delete and are easy to miss.

The first is that the delete marks the edge type dirty:

#srcblock("src/shard/write.rs:5002-5017")[```rust
fn mark_adjacency_dirty_txn(
    txn: &DbTransaction, cell_id: &str, edge_type: &str, epoch: StorageSequence,
) -> Result<()> {
    txn.put(keys::matrix_dirty(cell_id, edge_type).as_bytes(), encode_u64(epoch))?;
    txn.put(keys::adjacency_generation(cell_id, edge_type).as_bytes(), encode_u64(epoch))?;
    Ok(())
}
```]

That is the *entire* asynchronous consequence of a delete: two flags, `meta/matrix_dirty/<edge_type>`
and `meta/adjacency_generation/<edge_type>` (`src/keys.rs:23-33`). The out-of-process indexer
polls the first of them and rebuilds the edge type's index generation (Chapter 1, Section 1.8).
A flag is not a queue — it carries no history, records no operation, and has nothing to
garbage-collect. Deleting a thousand edges and deleting one leave the same two keys behind.

The second is that the epoch on the result is derived, not allocated:

#srcblock("src/codec.rs:128-135")[```rust
pub(crate) async fn next_epoch_txn(txn: &DbTransaction, cell_id: &str) -> Result<StorageSequence> {
    txn.seqnum()
        .checked_add(1)
        .ok_or_else(|| GraphError::CorruptValue { /* storage sequence overflow */ })
}
```]

`DeleteResult { epoch, deleted }` (`src/core/model.rs:282-285`) hands that number back so the
caller can turn it into a bookmark (Section 0.8) and read its own delete. Nothing writes it to
storage.

== The two branches of `delete_edge`

`delete_edge_txn_locked` (`src/shard/write.rs:3225`) branches on one question, asked once: does
this edge have a live outgoing-adjacency row?

#srcblock("src/shard/write.rs:3252")[```rust
let Some(existing) = read_txn_remote(&txn, &edge_key).await? else {
    // ... no e/out row: the edge may still live inside a compacted segment ...
};
decode_edge_record(&edge_key, &existing)?;   // ... it has a row: delete the keys.
```]

The `else` arm is where the interesting case lives. Recall from the write chapter that a
vertex with a large out-degree can have its adjacency packed into *segments*: a run of
destinations encoded into the value of a single `cell/<id>/seg/out/<type>/<src>/<seq>/<id>`
key (Section 0.6). A segment is one opaque encoded blob. Removing one destination from it
would mean decoding the blob, re-encoding it without that destination, and rewriting the key —
a read-modify-write over a potentially large value, on the foreground delete path, for the sake
of one edge.

So the delete does not touch the segment. It writes a tombstone next to it.

#custom-box(title: [Term — Segment tombstone], icon: "info")[
  A key recording that one destination inside one source vertex's compacted segments is
  deleted, and at which `StorageSequence`. The key is
  `cell/<cell>/seg/tomb/out/<edge_type>/<src>/<dst>` (`src/keys.rs:89-96`) and its value is the
  epoch. It is the only graph-level deletion marker TurboLay still writes, and it exists for a
  narrow structural reason: the thing being deleted is not a key, it is a member of an encoded
  value, and the storage engine's own tombstones only work at key granularity.
]

The segment branch establishes that a segment-resident edge really is there and really is not
already tombstoned, and only then commits the marker:

#srcblock("src/shard/write.rs:3253-3350 (abridged)")[```rust
let current_epoch = txn.seqnum();
let segment_edge = self.out_segment_edge_record_at(cell, et, src, dst, current_epoch).await?;
let Some((segment_sequence, _)) = segment_edge else {
    // nothing here at all — record a no-op receipt and commit
    let result = DeleteResult { epoch: current_epoch, deleted: false };
    txn.put(idem_key.as_bytes(), encode_delete_idempotency(mutation, &result))?;
    commit_txn_strict(txn, self.await_durable_writes).await?;
    return Ok(result);
};
let tombstone_key = keys::out_segment_tombstone(cell, et, src, dst);
if let Some(value) = read_txn_remote(&txn, &tombstone_key).await? {
    let tombstone_epoch = decode_u64(&tombstone_key, &value)?;
    if !segment_edge_visible(segment_sequence, Some(tombstone_epoch)) {
        /* already deleted — another no-op receipt */
    }
}
let epoch = current_epoch.checked_add(1)...?;
mark_adjacency_dirty_txn(&txn, cell, et, epoch)?;
delete_relationships_for_structural_edge_txn(&txn, mutation, current_epoch).await?;
txn.put(tombstone_key.as_bytes(), encode_u64(epoch))?;
txn.put(out_degree_key.as_bytes(), encode_u64(out_degree))?;
```]

Note what the tombstone branch does *not* do: it does not decrement an in-degree or delete an
`e/in` key, because the segment representation is outgoing-only. And note the no-op paths.
Deleting an edge that does not exist, or one that a previous delete already tombstoned, is not
an error: the transaction still commits, writing an idempotency receipt carrying
`deleted: false`. A client that retries a delete after a lost acknowledgement gets the same
answer back rather than a second, differently-stamped delete.

#figure(
  diagram(
    crossing-fill: reader-colors.paper,
    node-stroke: 0.6pt + reader-colors.border,
    node-corner-radius: 3pt,
    node-inset: 6pt,
    spacing: (5mm, 10mm),
    node((0.5, 0), text(fill: reader-colors.text, size: 8.5pt, hyphenate: false)[`delete_edge_txn_locked`\ #text(size: 7.5pt, fill: reader-colors.muted)[is there a live `e/out` row?]],
      fill: reader-colors.info_soft, stroke: reader-colors.info, width: 5.6cm),
    edge((0.5, 0), (0, 1), "->", stroke: reader-colors.muted),
    edge((0.5, 0), (1, 1), "->", stroke: reader-colors.muted),
    node((0, 1), text(fill: reader-colors.text, size: 8pt, hyphenate: false)[#text(fill: reader-colors.muted, size: 7.5pt)[yes — materialized]\ *delete the keys*\ `txn.delete` on `edge`, `e/out`, `e/in`;\ both degrees decremented],
      fill: reader-colors.ok_soft, stroke: reader-colors.ok, width: 5.8cm),
    node((1, 1), text(fill: reader-colors.text, size: 8pt, hyphenate: false)[#text(fill: reader-colors.muted, size: 7.5pt)[no — segment-resident]\ *write one tombstone*\ `seg/tomb/out/<et>/<src>/<dst> = epoch`;\ segment untouched;\ out-degree decremented],
      fill: reader-colors.warn_soft, stroke: reader-colors.warn, width: 5.8cm),
    edge((0, 1), (0.5, 2), "->", stroke: reader-colors.muted),
    edge((1, 1), (0.5, 2), "->", stroke: reader-colors.muted),
    node((0.5, 2), text(fill: reader-colors.text, size: 8pt, hyphenate: false)[both branches: relationships hard-deleted · `matrix_dirty` flag set · idempotency receipt · one serializable commit],
      fill: reader-colors.surface_soft, stroke: reader-colors.border, width: 11.6cm),
  ),
  caption: [The one branch in `delete_edge_txn_locked` (`src/shard/write.rs:3225`), and why it
    exists. A materialized edge is removed with ordinary `txn.delete`s, because SlateDB's own
    snapshot versioning already keeps it visible to readers pinned before the commit. An edge
    packed inside a compacted adjacency segment cannot be removed that way — it is a member of
    an encoded value, not a key of its own — so it gets the single surviving graph-level
    marker, a segment tombstone stamped with the delete's epoch. Everything below the fork is
    common to both paths.],
) <fig-ch04-delete-branches>

Deleting a whole vertex is built on this primitive rather than replacing it. `delete_vertex`
and `detach_delete_vertex` (`src/shard/write.rs:422-440`) both route into
`delete_vertex_with_options`, whose transaction (`delete_vertex_txn_locked`,
`src/shard/write.rs:496`) scans the vertex's incident edges and relationships, calls
`delete_edge_txn_locked` and `delete_relationship_txn_locked` on each in turn, and returns a
`VertexDeleteResult { epoch, vertex_deleted, incident_edges_deleted, relationships_deleted }`
(`src/core/model.rs:297-302`). The counts are how a caller learns that "detach delete" actually
detached something.

== The relationships riding on the edge

Chapter 0 distinguished the cheap structural *edge* from the richer *relationship* that carries
its own id and properties. Deletes treat the two differently, and this is the one asymmetry in
the chapter worth memorizing.

Both branches above call `delete_relationships_for_structural_edge_txn`
(`src/shard/write.rs:5019-5065`), which is unambiguous about what it does:

#srcblock("src/shard/write.rs:5025-5054 (abridged)")[```rust
let relationships = live_relationships_for_edge_txn(txn, cell, et, src, dst, read_epoch).await?;
for record in &relationships {
    txn.delete(keys::relationship(cell, et, src, dst, record.relationship_id).as_bytes())?;
    txn.delete(keys::relationship_id(cell, record.relationship_id).as_bytes())?;
    delete_relationship_property_indexes_txn(txn, record, &record.metadata)?;
}
txn.delete(keys::relationship_count(cell, et, src, dst).as_bytes())?;
```]

Every relationship record, its entry in the global relationship-id index, every one of its
property-index entries (`src/shard/write.rs:5067-5087` walks
`metadata.properties` and deletes one `rprop_idx` key per property), and the count — all removed
in the same transaction that removes the edge. A relationship never outlives the structural
edge that carries it, and there is no reconciliation pass that would have to notice if it did.

The traffic runs the other way as well. Deleting the *last* relationship between a pair of
vertices takes the structural edge with it. `delete_relationship_txn_locked`
(`src/shard/write.rs:2253`) counts what remains and, if nothing does, calls
`delete_structural_edge_txn` (`src/shard/write.rs:4921-5000`):

#srcblock("src/shard/write.rs:2338-2340")[```rust
if !other_live_relationships {
    delete_structural_edge_txn(self, &txn, mutation, epoch).await?;
}
```]

`delete_structural_edge_txn` is the materialized branch of Section 4.2 factored out: it returns
immediately if there is no `e/out` row, otherwise marks the edge type dirty, decrements the
degrees, clears any edge metadata and its property indexes, and deletes `edge`, `e/out`, and —
when the reverse index is enabled — `e/in`. It has no segment branch, which is the honest
boundary: relationship deletion can retire a materialized structural edge, but it will not
tombstone a segment-resident one.

#custom-box(title: [Why], icon: "tip")[
  The previous edition of this book generalized these two behaviours into a single rule and got
  it backwards, describing the structural edge as soft-deleted and the relationships as the
  exception. There is no soft/hard split any more, because there is no soft delete: both are
  ordinary `txn.delete`s and both are invisible from the next snapshot onward. What remains is a
  *containment* rule, and it runs in both directions — a relationship cannot outlive its edge,
  and an edge does not outlive its last relationship. One transaction enforces both, so no
  intermediate state where a relationship dangles is ever committed, let alone observable.
]

== How a delete reaches a reader

A deleted edge has to disappear from three quite different structures, and it disappears from
each by a different mechanism. Following one delete through all three is the clearest way to
see why the tombstone is confined to the middle layer.

*The adjacency keys.* Nothing to do. `out_neighbors` (`src/shard/query.rs:5372-5404`) prefix-scans
`cell/<id>/e/out/<type>/<src>/` and decodes what it finds. A deleted row is not in the snapshot,
so it is not in the result. Readers pinned before the delete scan an older snapshot and still
find it.

*The compacted segments.* Here the row *is* still there — inside the segment value — so the read
has to consult the tombstone. The comparison is one line:

#srcblock("src/codec.rs:1506-1511")[```rust
pub(crate) fn segment_edge_visible(
    edge_epoch: StorageSequence,
    tombstone_epoch: Option<StorageSequence>,
) -> bool {
    tombstone_epoch.is_none_or(|epoch| edge_epoch > epoch)
}
```]

An edge in a segment written at sequence $E$ is visible unless a tombstone exists at some
sequence $T >= E$. Strict inequality is what lets an edge be deleted, re-created, and packed
into a *newer* segment without the stale tombstone suppressing it. The same scan appears in the
per-source path (`scan_out_segment_tombstones_for_src_at`, `src/shard/query.rs:5793`), the
whole-edge-type path (`out_segment_tombstones_at`, `:5830`), and the single-edge probe
(`out_segment_tombstone_epoch_at`, `:5777`) — the reader loads the relevant tombstones into a
`BTreeMap` once and filters the segment destinations through it.

*The index generation.* The GraphBLAS CSC matrix the traversal path uses (Chapter 1, Section
1.7) was built by a separate process at some `base_sequence` that may predate the delete
entirely. It is immutable, so the delete cannot edit it. Instead the reader closes the gap with
the WAL-tail overlay — and the overlay carries deletions, not just insertions:

#srcblock("src/shard/topology_tail.rs:4-12")[```rust
pub(crate) struct GraphTopologyOverlay {
    states: BTreeMap<VertexId, BTreeMap<VertexId, bool>>,   // (src, dst) -> exists
}

pub(crate) enum GraphTopologyTail {
    Complete(GraphTopologyOverlay),
    Unavailable,
}
```]

That `bool` is the delete path's presence in the read path. `topology_tail_since`
(`src/shard/topology_tail.rs:28-97`) walks the WAL files written after the generation's
`last_wal_id`, collects every edge the tail *touched*, and then re-resolves each one against the
pinned snapshot, recording `exists: false` for the ones the delete removed. Crucially,
`collect_topology_entry` (`:100-136`) matches both key shapes this chapter produces:

#srcblock("src/shard/topology_tail.rs:110-120")[```rust
["cell", key_cell, "e", "out", key_type, src, dst] if ... => {
    affected.insert((parse_u64(key, src, "src")?, parse_u64(key, dst, "dst")?));
}
["cell", key_cell, "seg", "tomb", "out", key_type, src, dst] if ... => {
    affected.insert((parse_u64(key, src, "src")?, parse_u64(key, dst, "dst")?));
}
```]

So a hard-deleted `e/out` key and a newly written segment tombstone both enter the overlay by
the same route, and `expand_range_with_overlay` (`:139`) then subtracts them from each hop of a
traversal — removing a destination only when no other vertex in the frontier still reaches it.

#custom-box(title: [Why], icon: "tip")[
  The overlay is where the delete path pays for the indexer being out-of-process. An index
  generation is immutable and content-addressed precisely so that a reader can never observe a
  half-built one; the price is that it is also *stale*, and a delete committed a second ago is
  still present in it. Overlaying the WAL tail is the cheap way to be correct anyway: the
  reader pays only for the edges touched since the generation was built, and it re-checks each
  one against its own snapshot rather than trusting the WAL entry's opinion of the outcome.
]

The degradation is honest and worth stating plainly, because it is the failure mode you will
meet in production. `topology_tail_since` returns `Unavailable` in two cases: when the snapshot
handed to it does not match the read sequence (`:35-37`), and when a WAL file it needs can no
longer be opened (`:50-59`, which logs "graph index WAL tail is unavailable; using snapshot
adjacency"). In both, the query abandons the accelerated path and answers from the snapshot
adjacency keys instead. That is slower, and it is always correct, because the adjacency keys are
the source of truth and the index generation never was.

#figure(
  diagram(
    crossing-fill: reader-colors.paper,
    node-stroke: 0.6pt + reader-colors.border,
    node-corner-radius: 3pt,
    node-inset: 6pt,
    spacing: (7mm, 6mm),
    node((0, 0), text(fill: reader-colors.text, size: 8pt, hyphenate: false)[adjacency keys\ `e/out` · `e/in`],
      fill: reader-colors.surface_soft, width: 4.2cm),
    node((1, 0), text(fill: reader-colors.text, size: 8pt, hyphenate: false)[*the key is gone*\ the snapshot simply\ does not contain it],
      fill: reader-colors.ok_soft, stroke: reader-colors.ok, width: 6.6cm),
    edge((0, 0), (1, 0), "->", stroke: reader-colors.muted),
    node((0, 1), text(fill: reader-colors.text, size: 8pt, hyphenate: false)[compacted segments\ `seg/out`],
      fill: reader-colors.surface_soft, width: 4.2cm),
    node((1, 1), text(fill: reader-colors.text, size: 8pt, hyphenate: false)[*tombstone compared*\ `segment_edge_visible(E, T)`\ hidden when $T >= E$],
      fill: reader-colors.warn_soft, stroke: reader-colors.warn, width: 6.6cm),
    edge((0, 1), (1, 1), "->", stroke: reader-colors.muted),
    node((0, 2), text(fill: reader-colors.text, size: 8pt, hyphenate: false)[index generation\ (immutable CSC)],
      fill: reader-colors.purple_soft, stroke: reader-colors.purple, width: 4.2cm),
    node((1, 2), text(fill: reader-colors.text, size: 8pt, hyphenate: false)[*overlay says `false`*\ WAL tail re-resolved\ against the pinned snapshot],
      fill: reader-colors.info_soft, stroke: reader-colors.info, width: 6.6cm),
    edge((0, 2), (1, 2), "->", stroke: reader-colors.muted),
    node((1, 3), text(fill: reader-colors.muted, size: 7.5pt, style: "italic", hyphenate: false)[if the tail is `Unavailable`: fall back to snapshot adjacency],
      stroke: (dash: "dashed", paint: reader-colors.muted), fill: none, width: 6.6cm),
    edge((1, 2), (1, 3), "-->", stroke: (dash: "dashed", paint: reader-colors.muted)),
  ),
  caption: [One delete, three structures, three different mechanisms — and only the middle one
    needs a marker. The adjacency keys need nothing because SlateDB's snapshot already hides a
    deleted key from later readers and shows it to earlier ones. The compacted segments need a
    tombstone because the edge is a member of an encoded value rather than a key. The immutable
    index generation needs the WAL-tail overlay, which carries `exists: false` for deletes just
    as it carries `true` for inserts; when that tail cannot be read the query degrades to the
    snapshot adjacency keys, which are the source of truth in every case.],
) <fig-ch04-delete-visibility>

== What is not here

Two mechanisms that a reader of the previous edition — or of most LSM-backed graph systems —
would expect at this point do not exist, and it is worth naming them rather than leaving their
absence to be inferred.

*There is no delete log.* No outbox, no mutation log, no delta stream, and no `DeltaKind::Minus`.
The entire delta subsystem was removed in the graph-kernel resync; `DeltaKind` has zero
occurrences in `src/`, and with it went the key builders `outbox*`, `delta_*`, `owner_delta`,
`pair_delta` and `last_epoch`. A delete is not appended anywhere and is not replayed at read
time. What Section 4.1 showed is all of it.

*There is no GC watermark, and the error that guarded it is dead code.* The old design carried
a per-edge-type `delta_gc_watermark` key recording how far history had been collected, and
refused any read below it with `GraphError::SnapshotExpired`. The key builder is gone. The
error variant, however, is still *declared*:

#srcblock("src/core/error.rs:87")[```rust
    SnapshotExpired {
```]

and is never constructed anywhere in `src/`. Nothing raises it; nothing can catch it in
practice. It is a leftover declaration, and this book flags it as such rather than reasoning
from its presence — a variant that exists in the type system but not in the control flow will
mislead you about what the system guarantees.

#custom-box(title: [Why], icon: "tip")[
  Both absences follow from the same move. Once record visibility is delegated wholly to the
  SlateDB snapshot, the graph engine no longer owns a history, and a subsystem that owns no
  history needs neither a log to replay nor a watermark to bound how much of it survives. The
  guarantee that used to require a watermark — "a read is never served from history that has
  already been collected" — is now provided by SlateDB refusing to open a snapshot at a
  compacted-away sequence, which is the refusal you already met in Section 0.7. The engine did
  not weaken the promise; it stopped re-implementing it.
]

== Reclaiming space: three collectors

Deletes and index builds still leave residue: segment values holding destinations nobody can
see, tombstones that have outlived the segments they suppressed, superseded matrix artifacts,
and superseded index generations. Three separate routines reclaim it. They share no scheduler,
no watermark, and no notion of a safe epoch — each takes a boundary from its caller and trusts
it.

#custom-box(title: [Term — Garbage collection (as used here)], icon: "info")[
  Physically removing storage that no live reader needs. In TurboLay this is always
  caller-driven: every collector takes an explicit boundary parameter (`compacted_through_epoch`,
  `keep_epoch`, `retain_previous`) and does exactly what it is told. There is no retention
  policy, no computed safe epoch, and no read lease anywhere in the current tree. All three take
  the shard's `gc_gate` permit, so collection cannot crowd out reads and writes.
]

*Segment compaction* — `compact_out_adjacency_segments` (`src/shard/maintenance.rs:59`) — is the
one that closes the loop opened in Section 4.2. For one `(cell, edge_type, src)` it reads every
segment at or below `compacted_through_epoch`, filters the destinations through
`segment_edge_visible` against the tombstones (`:189-204`), writes a single fresh segment holding
only the survivors, and deletes both the source segments and the tombstones it has just honoured:

#srcblock("src/shard/maintenance.rs:207-229 (abridged)")[```rust
for (key, _) in &source_segments { batch.delete(key.as_bytes()); }
for (_, (epoch, key)) in tombstones {
    if epoch <= compacted_through_epoch {
        batch.delete(key.as_bytes());
        deleted_tombstone_keys = deleted_tombstone_keys.saturating_add(1);
    }
}
if !compacted_destinations.is_empty() {
    batch.put(keys::out_segment(cell, et, src, compacted_through_epoch,
        &format!("compact-{idempotency_key}")), encode_out_edge_segment_records(&compacted_destinations));
}
```]

A tombstone is only deleted alongside the segment it was suppressing, in one batch — which is
what keeps the deletion durable once the marker is gone. The routine has one guard, and it is a
strict one: a matrix artifact must already exist at *exactly* `compacted_through_epoch`
(`src/shard/maintenance.rs:126-143`), and a `compacted_through_epoch` ahead of the cell's current
epoch is rejected with `SnapshotAhead` (`:119-125`). It reports a
`SegmentCompactionResult { compacted_through_epoch, source_segments, deleted_segment_keys,
deleted_tombstone_keys, input_edges, output_edges }` (`src/core/model.rs:313-320`).

*Artifact GC* — `delete_graph_artifacts_before` (`src/engine/artifact_gc.rs:4`) — prunes the
durable matrix artifacts. It scans four key prefixes (`graph_artifact_gc_prefixes`,
`src/engine.rs:860-867`: the matrix manifest, the matrix payload, and the two GraphBLAS CSC
prefixes), deletes every key whose `base_epoch` is strictly below `keep_epoch`, and flushes in
batches of `GRAPH_ARTIFACT_GC_BATCH_KEYS` (512, `src/engine.rs:884`). It then prunes memory to
match, which is the part worth reading:

#srcblock("src/engine/artifact_gc.rs:56-64")[```rust
self.matrix_artifact_cache.lock().await.retain(|key, _| {
    key.cell_id != cell_id || key.edge_type != edge_type || key.base_epoch >= keep_epoch
});
self.matrix_cache.lock().await.retain(/* same predicate */);
self.graphblas_cache.lock().await.retain(/* same predicate */);
```]

All three matrix caches are swept with the same predicate, so no in-memory hydration survives an
artifact its durable bytes no longer back. The result is
`ArtifactGcResult { deleted_keys, retained_keys }` (`src/engine.rs:101-104`).

*Index-generation GC* — `gc_graph_index_generations` (`src/engine/index_store.rs:210-236`) —
works on the object store rather than the key space. It finds the current generation, lists every
published generation under the prefix, keeps the newest `retain_previous` of those older than
current, and deletes the rest by object path. It is guarded by construction: if there is no
current generation it returns `0` and deletes nothing, so it can never orphan the generation
readers are using.

#custom-box(title: [Why], icon: "tip")[
  Of the three, only index-generation GC has an in-tree scheduler: `graph-indexer` calls it at
  the end of every cycle (`src/bin/graph-indexer.rs:185`) with the retention its environment
  configures (`GRAPH_INDEXER_RETAIN_PREVIOUS`, default 1 — Chapter 1, Section 1.8). Segment
  compaction and artifact GC have *no caller anywhere in `src/`*; they are operator-invoked APIs,
  exercised in the tree only by `examples/stress_worker.rs:247`. That is a real gap and this book
  will not paper over it: an unattended deployment reclaims stale index generations automatically
  and accumulates segment tombstones and superseded matrix artifacts indefinitely. The
  correctness argument is unaffected — every collector's boundary is supplied by its caller, so a
  collector that is never called simply never collects — but the operational one is that
  compaction is currently something you schedule, not something that happens.
]

#figure(
  diagram(
    crossing-fill: reader-colors.paper,
    node-stroke: 0.6pt + reader-colors.border,
    node-corner-radius: 3pt,
    node-inset: 6pt,
    spacing: (5mm, 7mm),
    node((0, 0), text(fill: reader-colors.text, size: 8pt, weight: "bold", hyphenate: false)[`compact_out_adjacency_`\ `segments`],
      fill: reader-colors.warn_soft, stroke: reader-colors.warn, width: 3.9cm),
    node((1, 0), text(fill: reader-colors.text, size: 8pt, weight: "bold", hyphenate: false)[`delete_graph_`\ `artifacts_before`],
      fill: reader-colors.warn_soft, stroke: reader-colors.warn, width: 3.9cm),
    node((2, 0), text(fill: reader-colors.text, size: 8pt, weight: "bold", hyphenate: false)[`gc_graph_index_`\ `generations`],
      fill: reader-colors.ok_soft, stroke: reader-colors.ok, width: 3.9cm),
    node((0, 1), text(fill: reader-colors.text, size: 7.5pt, hyphenate: false)[segments + the tombstones\ they suppressed\ #text(fill: reader-colors.muted)[bound: `compacted_through_epoch`]],
      fill: reader-colors.surface_soft, width: 3.9cm),
    node((1, 1), text(fill: reader-colors.text, size: 7.5pt, hyphenate: false)[matrix artifact keys\ + the three caches\ #text(fill: reader-colors.muted)[bound: `keep_epoch`]],
      fill: reader-colors.surface_soft, width: 3.9cm),
    node((2, 1), text(fill: reader-colors.text, size: 7.5pt, hyphenate: false)[published CSC generations\ in the object store\ #text(fill: reader-colors.muted)[bound: `retain_previous`]],
      fill: reader-colors.surface_soft, width: 3.9cm),
    edge((0, 0), (0, 1), "->", stroke: reader-colors.muted),
    edge((1, 0), (1, 1), "->", stroke: reader-colors.muted),
    edge((2, 0), (2, 1), "->", stroke: reader-colors.muted),
    node((0, 2), text(fill: reader-colors.muted, size: 7.5pt, style: "italic")[no in-tree caller],
      stroke: (dash: "dashed", paint: reader-colors.muted), fill: none, width: 3.9cm),
    node((1, 2), text(fill: reader-colors.muted, size: 7.5pt, style: "italic")[no in-tree caller],
      stroke: (dash: "dashed", paint: reader-colors.muted), fill: none, width: 3.9cm),
    node((2, 2), text(fill: reader-colors.text, size: 7.5pt, hyphenate: false)[called every cycle by\ `graph-indexer`],
      fill: reader-colors.info_soft, stroke: reader-colors.info, width: 3.9cm),
    edge((0, 1), (0, 2), "-->", stroke: (dash: "dashed", paint: reader-colors.muted)),
    edge((1, 1), (1, 2), "-->", stroke: (dash: "dashed", paint: reader-colors.muted)),
    edge((2, 1), (2, 2), "->", stroke: reader-colors.muted),
  ),
  caption: [The three collectors, what each one owns, and which of them actually runs. Every
    boundary in the middle row is a parameter supplied by the caller — there is no computed safe
    epoch and no watermark anywhere in this row. The bottom row is the operational reality: only
    index-generation GC is scheduled, by `graph-indexer` (`src/bin/graph-indexer.rs:185`), while
    segment compaction and artifact GC are operator-invoked APIs with no caller in `src/`, so an
    unattended deployment accumulates tombstones and superseded artifacts.],
) <fig-ch04-reclaim>

== Dropping a whole graph

The largest delete removes an entire cell. `drop_cell` (`src/shard/write.rs:783`) takes the same
envelope as every other write — authority check, write permit, writer lane, bounded retry — and
the work happens in `drop_cell_locked` (`src/shard/write.rs:821`). What makes it interesting is
that it cannot be one transaction: a cell may hold millions of keys, so the scan-and-delete has
to be chunked, and a chunked deletion needs protection against a concurrent writer refilling
what it has already emptied.

The protection is a marker written *before* the deletion starts, in its own committed
transaction:

#srcblock("src/shard/write.rs:854-862 (abridged)")[```rust
let marker_epoch = match read_txn_remote(&txn, &pending_marker_key).await? {
    Some(value) => decode_u64(&pending_marker_key, &value)?,
    None => {
        let epoch = txn.seqnum().saturating_add(1);
        txn.put(pending_marker_key.as_bytes(), encode_u64(epoch))?;
        epoch
    }
};
commit_txn_strict(txn, self.await_durable_writes).await?;
```]

That is phase one of three. Phase two scans `cell_prefix(cell_id)` and deletes everything it
finds in batches of `GRAPH_MAINTENANCE_BATCH_KEYS` (512, `src/lib.rs:177`), each batch its own
fence-validated serializable transaction (`flush_drop_cell_batch`, `src/shard/write.rs:916-938`),
renewing the local write guard around each flush so a long drop does not look like a stall.
Phase three commits the *final* marker, removes the pending one, and records the idempotency
receipt.

The two markers are the entire concurrency story, and they work because of one line in the
write fence every other operation passes through:

#srcblock("src/shard/lifecycle.rs:442-453")[```rust
if operation != "drop_cell" {
    let drop_marker = keys::cell_drop_marker(cell_id);
    let pending_drop_marker = keys::cell_drop_pending_marker(cell_id);
    if read_txn_remote(txn, &drop_marker).await?.is_some()
        || read_txn_remote(txn, &pending_drop_marker).await?.is_some()
    {
        return Err(GraphError::CellDropped { operation, cell_id: cell_id.to_string() });
    }
}
```]

Once the pending marker is durable, every write to that cell — including every delete in this
chapter — fails with `CellDropped` before it can touch a key, while `drop_cell` itself is
exempted by name so its own batches proceed. The cell is sealed for the whole of the scan, not
just for one transaction.

Both markers live at `graph/drop/…` (`src/keys.rs:7-17`), *outside* the `cell/<cell_id>/` prefix.
That placement is deliberate and load-bearing: the phase-two scan deletes the cell prefix
wholesale, so a marker stored inside it would delete itself midway and unseal the cell it was
protecting. The result is `GraphCellDropResult { marker_epoch, deleted_keys, batches,
already_dropped }` (`src/core/model.rs:305-310`); the `already_dropped` flag is how a re-issued
drop reports that it found the final marker and did nothing.

#custom-box(title: [Why], icon: "tip")[
  Splitting the marker in two — pending during the scan, final afterwards — separates "this cell
  is being torn down" from "this cell is gone". A crash between phases leaves the pending marker
  behind, which is exactly the right failure mode: the cell stays sealed rather than reopening
  half-emptied, and a re-issued `drop_cell` picks up the same `marker_epoch` from the pending
  marker (`src/shard/write.rs:854-856`) and resumes the scan rather than starting a new drop with
  a new epoch. It is the same instinct as the segment tombstone one section up — when you cannot
  make the change atomic, make the *incomplete* state a state the rest of the system already
  knows how to refuse.
]

== Recap: the life of a deleted edge

Trace one edge out of the graph:

+ `delete_edge` runs the ordinary write envelope and opens a serializable transaction
  (`src/shard/write.rs:3184`).
+ If the edge has a live `e/out` row, the transaction *deletes* `edge`, `e/out` and `e/in`
  outright and decrements the degree counters. If it only exists inside a compacted segment, the
  transaction instead writes one `seg/tomb/out/…` tombstone stamped with the new epoch.
+ Either way, the relationships riding on that edge — records, id index, property indexes,
  count — are deleted in the same transaction, and the edge type is flagged
  `meta/matrix_dirty/<edge_type>`.
+ From the next snapshot onward the edge is gone: absent from the adjacency scan, filtered out of
  the segments by `segment_edge_visible`, and carried into the traversal path as `exists: false`
  in the WAL-tail overlay. Readers pinned at an earlier `StorageSequence` still see it, because
  SlateDB still has that version.
+ The out-of-process indexer notices the dirty flag and publishes a new index generation without
  the edge (Chapter 1, Section 1.8), then prunes superseded generations.
+ If — and only if — an operator invokes it, `compact_out_adjacency_segments` rewrites the
  segment without the destination and deletes the tombstone in the same batch, and
  `delete_graph_artifacts_before` prunes the artifacts and the matrix caches beneath a
  `keep_epoch` it is given.

The through-line of this chapter is a subtraction. Deleting the delta log, the GC watermark and
the soft-delete framing did not cost the system a guarantee; it revealed that SlateDB's snapshot
was already providing the guarantee twice. What is left is a delete that is a delete, one
tombstone confined to the one representation that genuinely cannot express absence, and three
collectors that do precisely what their caller asks and nothing more. The next chapter turns to
the caches sitting in front of all of this, and how they stay correct while the sequence advances
underneath them.
