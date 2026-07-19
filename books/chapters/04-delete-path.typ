#import "../template.typ": term, why, srcblock, figcap, accent, muted
#import "@preview/fletcher:0.5.7" as fletcher: diagram, node, edge
#import "../vendor/bookly/src/themes/reader.typ": reader-colors

= The Delete Path

Deleting from TurboLay is not the opposite of writing. It is another kind of writing. Because
the durable store only appends (Chapter 0), a delete cannot reach into the object store and
erase bytes. Instead it records that something is gone, at a specific epoch, and the physical
removal happens much later in a background job once no reader can still need the old data.

This chapter has two halves. The first is the foreground delete: what happens when you delete
an edge, a relationship, or a whole graph. The second is garbage collection: how the tombstones
and old deltas that deletes (and writes) leave behind are eventually removed, and the careful
epoch bookkeeping that keeps that removal from pulling data out from under a live reader.

The code lives in `src/shard/write.rs` for the foreground deletes and `src/shard/maintenance.rs`
plus `src/engine/` for the garbage collection.

== A delete is an append

Recall the delta model from the read and write chapters. The set of live edges at an epoch is a
matrix artifact base, plus a replay of delta records up to that epoch, where a `Plus` adds an
edge and a `Minus` removes it (`edges_at_with_budget`, read chapter). A delete, then, is just a
`Minus` delta appended at a new epoch.

#srcblock("src/core/model.rs:455-464")[```rust
pub enum DeltaKind {
    Plus,
    Minus,
}

pub struct DeltaRecord {
    pub kind: DeltaKind,
    pub edge: EdgeRecord,
}
```]

#term("Soft delete")[
  Recording that data is gone without physically removing it yet. The data and a deletion
  marker coexist; reads treat the data as absent, and a later cleanup pass does the physical
  removal. TurboLay deletes are soft deletes: the `Minus` delta and the segment tombstone are
  the markers, and the delta and artifact garbage collectors are the cleanup.
]

#why[
  Soft deletes are what make snapshot reads work. A reader pinned at an old epoch must still see
  an edge that a newer epoch deleted. If a delete erased the edge immediately, that reader's
  snapshot would be corrupted. By recording the delete as a `Minus` at a new epoch, the edge
  stays visible to reads below that epoch and disappears only for reads at or above it. The
  same mechanism that gives writers cheap appends gives readers stable history.
]

== Deleting an edge

`delete_edge` (`src/shard/write.rs:3338`) has the exact shape of `write_edge` from the last
chapter: validate, take a write permit, lock the cell's write lane, then a bounded retry loop
around `delete_edge_txn`, which takes the object-store lock and runs `delete_edge_txn_locked`.
The interesting part is that transaction, and it splits into two branches depending on where
the edge currently lives.

=== Branch one: a materialized edge

If the edge has a live adjacency row (`out_edge`), it is materialized. The delete removes the
structural keys, decrements the degree counters, and appends a `Minus` delta:

#srcblock("src/shard/write.rs:3534-3640 (abridged)")[```rust
decode_edge_record(&edge_key, &existing)?;
let epoch = next_epoch_txn(&txn, &mutation.cell_id).await?;
let delta = DeltaRecord { kind: DeltaKind::Minus, edge: record.clone() };
let delta_value = encode_delta_record(&delta);
put_scoped_delta_indexes_txn(&txn, &delta)?;
// degree counters decremented with saturating_sub(1) ...
delete_relationships_for_structural_edge_txn(&txn, mutation, epoch.saturating_sub(1)).await?;
txn.put(keys::last_epoch(&mutation.cell_id).as_bytes(), encode_u64(epoch))?;
txn.delete(canonical_key.as_bytes())?;
txn.delete(keys::out_edge(cell, et, src, dst).as_bytes())?;
txn.delete(keys::in_edge(cell, et, dst, src).as_bytes())?;
txn.put(out_degree_key.as_bytes(), encode_u64(out_degree))?;
// in_degree if reverse index ...
txn.put(keys::outbox(cell, epoch, DeltaKind::Minus, et, src, dst).as_bytes(), &delta_value)?;
txn.put(idem_key.as_bytes(), encode_delete_idempotency(mutation, &result))?;
commit_txn_strict(txn, self.await_durable_writes).await?;
```]

Compare this to the edge insert from the write chapter. The physical keys (`edge`, `out_edge`,
`in_edge`) are `delete`d here rather than `put`. But the delete still `put`s a delta: an
`outbox` record with `DeltaKind::Minus` at the new epoch, plus the scoped `owner_delta` and
`pair_delta` indexes with `Minus`. That `Minus` is what a historical read replays to make the
edge disappear at this epoch and above.

Deleting the adjacency rows directly is safe here even though the store is append-only, because
those rows describe only the current tip. A read at an older epoch does not rebuild the tip from
the adjacency rows; it rebuilds from the matrix artifact plus the delta replay, and the `Minus`
delta is what it needs. The adjacency rows are a current-tip convenience, and the delete keeps
them honest by removing them.

=== Branch two: a segment-resident edge

If there is no `out_edge` row, the edge may still exist inside a compacted segment (Chapter 3,
Section 3.7). You cannot delete a row that is not there, so the delete instead writes a segment
tombstone:

#srcblock("src/shard/write.rs:3512-3530 (abridged)")[```rust
txn.put(tombstone_key.as_bytes(), encode_u64(epoch))?;    // out_segment_tombstone at new epoch
txn.put(out_degree_key.as_bytes(), encode_u64(out_degree))?;
txn.put(
    keys::outbox(cell, epoch, DeltaKind::Minus, et, src, dst).as_bytes(),
    &delta_value,
)?;
txn.put(idem_key.as_bytes(), encode_delete_idempotency(mutation, &result))?;
commit_txn_strict(txn, self.await_durable_writes).await?;
```]

#term("Tombstone")[
  A marker that a specific thing is deleted as of an epoch, used when the thing cannot simply be
  removed because it lives in immutable compacted storage. TurboLay has segment tombstones for
  edges inside segments (`cell/<cell>/seg/tomb/out/<et>/<src>/<dst>`, `keys.rs:110`). A read
  checks the tombstone's epoch: the segment edge is visible only while no tombstone at or before
  the read epoch exists.
]

The read side honors this through `segment_edge_visible` and `out_segment_tombstone_epoch_at`
(read chapter): a segment edge counts as present only if there is no tombstone with
`epoch <= read_epoch`. So the tombstone, like the `Minus` delta, is a soft delete stamped with
an epoch.

Both branches also call `delete_relationships_for_structural_edge_txn` (`src/shard/write.rs`).
When the structural edge between two entities is deleted, any relationships riding on it are
physically removed in the same transaction: for each one, `txn.delete` erases the relationship,
its id record, its property indexes, and the relationship count, so a relationship never outlives
the edge that carries it. Note the asymmetry. The structural edge itself is still soft-deleted --
a `Minus` delta or a segment tombstone at a new epoch, exactly as above -- but the relationships
riding on it are hard-removed, not tombstoned. Not every delete in TurboLay is a soft delete.

#figure(
  stack(
    dir: ttb,
    spacing: 8mm,
    align(left)[#text(size: 9pt, weight: "bold", fill: reader-colors.ink)[Part 1 -- one delete txn @ epoch E]],
    diagram(
      node-stroke: 0.6pt,
      spacing: (11mm, 9mm),
      node((1, 0), text(size: 8pt)[delete txn @ epoch `E`], fill: reader-colors.surface_soft, stroke: reader-colors.border, corner-radius: 3pt, width: 4cm),
      edge((1, 0), (0, 1), "->", stroke: reader-colors.muted, label: text(size: 7pt, fill: reader-colors.muted)[structural edge]),
      edge((1, 0), (2, 1), "->", stroke: reader-colors.muted, label: text(size: 7pt, fill: reader-colors.muted)[relationships on it]),
      node((0, 1), text(size: 8pt)[edge: `Minus` delta / tombstone\ *soft* -- readable at old epochs], fill: reader-colors.info_soft, stroke: reader-colors.info, corner-radius: 3pt, width: 5.2cm),
      node((2, 1), text(size: 8pt)[`txn.delete` id + indexes + count\ *hard* -- gone now], fill: reader-colors.bad_soft, stroke: reader-colors.bad, corner-radius: 3pt, width: 5.2cm),
    ),
    align(left)[#text(size: 9pt, weight: "bold", fill: reader-colors.ink)[Part 2 -- GC contract]],
    diagram(
      node-stroke: 0.6pt,
      spacing: (9mm, 8mm),
      node((0, 0), text(size: 7pt, fill: reader-colors.muted)[epoch 0], stroke: none, fill: none),
      node((4, 0), text(size: 7pt, fill: reader-colors.muted)[now], stroke: none, fill: none),
      edge((0, 0), (4, 0), "->", stroke: reader-colors.muted),
      node((2, -0.55), text(size: 7pt, fill: reader-colors.primary_active)[`delta_gc_watermark`], stroke: none, fill: none),
      edge((2, -0.18), (2, 0.18), "-", stroke: 1.6pt + reader-colors.primary_active),
      node((1, 1.2), text(size: 8pt)[raise watermark], fill: reader-colors.ok_soft, stroke: reader-colors.ok, corner-radius: 3pt, width: 3.2cm),
      node((3, 1.2), text(size: 8pt)[delete deltas below it], fill: reader-colors.surface_soft, stroke: reader-colors.border, corner-radius: 3pt, width: 3.8cm),
      edge((1, 1.2), (3, 1.2), "->", stroke: reader-colors.muted, label: text(size: 7pt, fill: reader-colors.muted)[then]),
      node((0.7, 2.5), text(size: 8pt)[read @ epoch < watermark], fill: reader-colors.surface_soft, stroke: reader-colors.border, corner-radius: 3pt, width: 4cm),
      node((3, 2.5), text(size: 8pt)[`SnapshotExpired`], fill: reader-colors.warn_soft, stroke: reader-colors.warn, corner-radius: 3pt, width: 3.4cm),
      edge((0.7, 2.5), (3, 2.5), "->", stroke: reader-colors.muted),
    ),
  ),
  caption: none,
) <fig-ch04-delete-watermark>
#figcap[The delete asymmetry and the GC/read watermark contract. Read Part 1 left-to-right: one delete txn splits in two -- the structural edge is soft-deleted (a `Minus` delta or tombstone at a new epoch, still readable at old epochs), while the relationships riding on it are hard-deleted in the same txn. Part 2 is the epoch axis: GC raises `delta_gc_watermark` *before* deleting the deltas beneath it, so a read that starts below the watermark is refused with `SnapshotExpired` rather than served a half-collected history.]

Not every delete is soft, then, and GC never deletes the only route to a still-permitted read
epoch. The edge is versioned but the relationships it carries are physically removed; and because
GC advances the watermark first, anything older than the watermark is refused outright rather than
silently reconstructed wrong.

#figure(
  diagram(
    node-stroke: 0.6pt,
    spacing: (0.7cm, 0.8cm),
    node((1, 0), [`delete_edge` at new `epoch`], fill: rgb("#fff8e6"), width: 5cm),
    edge((1, 0), (0, 1), "->", [has `e/out` row]),
    edge((1, 0), (2, 1), "->", [only in a segment]),
    node((0, 1), [delete `edge`, `e/out`, `e/in`;\ decrement degree;\ put `Minus` delta], fill: rgb("#eef4ff"), width: 5cm),
    node((2, 1), [put segment tombstone;\ decrement degree;\ put `Minus` delta], fill: rgb("#eef4ff"), width: 5cm),
    edge((0, 1), (1, 2), "->"),
    edge((2, 1), (1, 2), "->"),
    node((1, 2), [edge invisible to reads at `epoch` and above,\ still visible below it], fill: rgb("#e9fce9"), width: 7.5cm),
  ),
  caption: none,
)
#figcap[The two delete branches. Either way the outcome is a Minus (or tombstone) at a new epoch, so the edge disappears for new reads while staying in the history old readers depend on.]

Deleting a whole vertex (`delete_vertex`, `detach_delete_vertex`, `write.rs:435`) is built on
the same primitive: it finds the vertex's incident edges and relationships and tombstones or
deletes each, returning a `VertexDeleteResult` with the counts.

== The problem garbage collection solves

Every write appends a delta, and every delete appends another delta or a tombstone. Left alone,
a cell's delta history grows without bound, and reads get slower because the merge replays a
longer and longer tail. Garbage collection reclaims that space, but it cannot simply delete old
records, because a reader pinned at an old epoch might still need them.

#term("Garbage collection (GC)")[
  Reclaiming storage that is no longer needed. In TurboLay, GC physically removes delta records,
  tombstones, and superseded artifacts that predate a boundary epoch supplied by the caller. GC
  trusts that epoch -- there is no computed safe-epoch, retention policy, or read lease in the
  current tree. The one invariant GC enforces itself is the watermark contract below, which keeps
  a read from ever starting inside history that has already been collected.
]

The boundary is a single per-edge-type watermark that records how far history has already been
compacted away. GC raises the watermark to its target epoch and then deletes everything below it.

== The GC watermark

#term("Delta GC watermark")[
  A per-edge-type epoch recorded at `cell/<cell>/meta/delta_gc/<edge_type>` (`keys.rs:469`). It
  marks the oldest epoch whose deltas still exist. Reads below the watermark are refused with a
  snapshot-expired error, because the deltas needed to reconstruct that snapshot have been
  collected. GC advances the watermark and then deletes everything below it.
]

The read side already enforces this: `deltas_between` returns a snapshot-expired error when a
read tries to start below the watermark (read chapter). So the watermark is the contract between
GC and reads. GC may only advance it to an epoch that is provably safe.

GC does not compute that boundary. The caller passes a target epoch, and the only thing
`delete_deltas_through_matrix` checks before touching the watermark is that a compacted matrix
artifact already exists at exactly that epoch (see below). There is no safe-epoch calculation, no
retention window, and no read lease in the current tree; those layers were removed. What survives
is the watermark contract, and it is enough: because reads below the watermark are refused, GC
can raise the watermark to the caller's target epoch and delete underneath it without corrupting
any read that is still allowed to start.

#figure(
  diagram(
    node-stroke: 0.5pt,
    spacing: (0.35cm, 0.7cm),
    node((0, 0), [epoch 0], stroke: none, fill: none),
    node((4.6, 0), [current epoch], stroke: none, fill: none),
    node((0, 1), [collected\ (below watermark)], fill: rgb("#f0d0d0"), width: 3.4cm),
    node((1.6, 1), [collectable\ (below target)], fill: rgb("#fff8e6"), width: 3.4cm),
    node((3.2, 1), [live\ (at or above target)], fill: rgb("#e9fce9"), width: 3.6cm),
    node((1.6, 2), [watermark], fill: none, stroke: none),
    node((3.2, 2), [target epoch], fill: none, stroke: none),
    edge((1.6, 1), (1.6, 2), "-", stroke: 0.4pt + muted),
    edge((3.2, 1), (3.2, 2), "-", stroke: 0.4pt + muted),
  ),
  caption: none,
)
#figcap[The GC boundary along the epoch axis. GC raises the watermark rightward to the caller's target epoch, then deletes the deltas below it. Reads that try to start below the watermark are refused, so nothing a live read still needs is ever collected.]

== Matrix refresh, then delta GC

Removing deltas is only safe once the information in them has been folded into a compacted
artifact. That folding is what this book calls rollup.

#term([Matrix refresh (\"rollup\")])[
  Compacting recent history into a more permanent form: folding delta records and segments into
  a new matrix artifact at a higher base epoch. "Rollup" is a teaching word -- it appears nowhere
  in the source. The builder is `build_adjacency_image` (`src/engine/artifact_build.rs`), and it
  is driven by a real background job, `start_matrix_artifact_refresh_job`
  (`src/engine/artifact_refresh.rs`), which scans dirty edge-type markers and rebuilds an artifact
  when one is due per its refresh policy. After a refresh produces an artifact at epoch E, a read
  at any epoch at or above E can start from that artifact and no longer needs the deltas below E,
  which makes those deltas collectable. This is distinct from the mutation-log materialize of
  Chapter 3: materialize turns the append log into deltas, a refresh turns deltas into artifacts.
]

Unlike earlier drafts of this design, the refresh loop is shipping code, not future work:
`graph-node` starts it at boot (`src/bin/graph-node.rs`).

Delta GC is therefore gated on a refresh having already produced an artifact at the target epoch.
`delete_deltas_through_matrix` (`src/shard/maintenance.rs:14`) enforces exactly that -- its only
guard is that a matrix artifact exists at exactly `compact_through_epoch`. It does not compute a
safe epoch or consult a retention policy; it trusts the caller's target epoch, raises the
watermark, then deletes the old delta records:

#srcblock("src/shard/maintenance.rs:27-56 (abridged)")[```rust
let Some(artifact) = self.latest_matrix_artifact(cell_id, edge_type, compact_through_epoch).await?
else {
    return Err(/* "cannot compact deltas without a matrix artifact" */);
};
if artifact.base_epoch != compact_through_epoch {
    return Err(/* "latest matrix artifact is at epoch N, expected ..." */);
}
let mut watermark_batch = GraphWriteBatch::new();
watermark_batch.put(keys::delta_gc_watermark(cell_id, edge_type), encode_u64(compact_through_epoch));
self.write_graph_batch_strict_with_cell_lock(cell_id, "delete_deltas_through_matrix", watermark_batch).await?;
```]

After the watermark is raised, it deletes every kind of delta record below it: the `outbox`
deltas and outbox batches, the `delta/plus` and `delta/minus` streams, and the scoped
`delta_owner` and `delta_pair` indexes for both kinds (`maintenance.rs:62-112`). The deletes are
batched so a large history does not build one enormous transaction:

#srcblock("src/shard/maintenance.rs:259-277")[```rust
async fn flush_delta_gc_batch(
    &self, cell_id: &str, batch: &mut GraphWriteBatch, pending_deletes: &mut usize,
) -> Result<()> {
    if *pending_deletes == 0 { return Ok(()); }
    let batch_to_write = std::mem::replace(batch, GraphWriteBatch::new());
    self.write_graph_batch_strict_with_cell_lock(cell_id, "delete_deltas_through_matrix", batch_to_write).await?;
    *pending_deletes = 0;
    Ok(())
}
```]

The flush fires whenever `pending_deletes` reaches `GRAPH_DELTA_GC_BATCH_KEYS` (512, `lib.rs:187`).
The order matters: the watermark is raised first, then the deltas are removed. If the process
dies midway, the watermark already forbids reads that would need the half-removed deltas, so
there is no window where a read could see an inconsistent history.

== Artifact garbage collection

A refresh also leaves behind superseded artifacts. Once a newer matrix artifact at a higher base
epoch exists, the older ones are dead weight. `delete_graph_artifacts_before`
(`src/engine/artifact_gc.rs:4`) removes them:

#srcblock("src/engine/artifact_gc.rs:20-64 (abridged)")[```rust
for prefix in graph_artifact_gc_prefixes(cell_id, edge_type) {
    let mut iter = self.scan_remote_prefix(&prefix).await?;
    while let Some(kv) = iter.next().await? {
        let Some(base_epoch) = graph_artifact_epoch_from_key(&key)? else {
            result.retained_keys += 1; continue;
        };
        if base_epoch < keep_epoch {
            batch.delete(key.as_bytes());
            // flush in batches of GRAPH_ARTIFACT_GC_BATCH_KEYS ...
        } else {
            result.retained_keys += 1;
        }
    }
}
// then prune the three matrix caches to match:
self.matrix_artifact_cache.lock().await.retain(|k, _| k.base_epoch >= keep_epoch /* + cell/type */);
// ... matrix_cache and graphblas_cache the same way ...
```]

Like the delta path, this has no safe-keep computation and no read lease. It deletes every
artifact key whose `base_epoch` is strictly below the caller's `keep_epoch`, then prunes the
three matrix caches -- `matrix_artifact_cache`, `matrix_cache`, and `graphblas_cache` -- with a
`retain` that drops any entry for this cell and edge type below `keep_epoch`. It trusts the caller
to pass an epoch that no live read needs.

== Dropping a whole graph

The largest delete removes an entire cell. `drop_cell` (`src/shard/write.rs:803`) follows the
usual write envelope (permit, lane lock, retry, object-store lock) and then, in
`drop_cell_locked`, writes the cell drop marker and scans the whole `cell/<cell_id>/` prefix,
deleting every key in batches:

#srcblock("src/shard/write.rs (drop_cell_locked, abridged)")[```rust
let marker_key = keys::cell_drop_marker(cell_id);
// ... write the drop marker ...
let mut iter = self.scan_remote_prefix(&keys::cell_prefix(cell_id)).await?;
// ... accumulate deletes, flush_drop_cell_batch, count deleted_keys and batches ...
```]

The drop marker is what makes this safe against concurrent writers. Recall from the write
chapter that `validate_write_fence_txn` refuses any write to a cell whose drop marker or
pending-drop marker is present. So once the marker is written, no new data can enter the cell
while it is being torn down, and the scan-and-delete can proceed to completion. The result is a
`GraphCellDropResult` reporting how many keys and batches were removed.

== Recap: the life of a deleted edge

Trace one edge from deletion to disappearance:

+ `delete_edge` appends a `Minus` delta (or a segment tombstone) at a new epoch and removes the
  current-tip adjacency rows. The edge is now invisible to reads at that epoch and above.
+ Reads below that epoch still see the edge, because they replay only up to their own epoch.
+ A background matrix refresh folds history into a matrix artifact at some epoch E, so reads at
  or above E no longer need the deltas below E.
+ `delete_deltas_through_matrix` confirms a matrix artifact exists at exactly E, raises the
  watermark to E, and physically deletes the `Minus` delta and every other delta below E.
+ `delete_graph_artifacts_before` removes the now-superseded old artifacts.

The delete is only truly gone in step four, once the watermark forbids any read that would have
needed it -- the caller is trusted to advance to an epoch no live read is below. This is
the same append-only, epoch-stamped discipline the write and read chapters showed, now closing
the loop by reclaiming what those chapters produced. The final chapter turns to the caches that
sit in front of all of this and how they, too, stay correct as epochs advance and data is
collected.
