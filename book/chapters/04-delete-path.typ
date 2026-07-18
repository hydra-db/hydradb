#import "../template.typ": term, why, srcblock, figcap, accent, muted
#import "@preview/fletcher:0.5.7" as fletcher: diagram, node, edge

= The Delete Path

Deleting from turbolay is not the opposite of writing. It is another kind of writing. Because
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

#srcblock("src/core/model.rs:449-458")[```rust
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
  removal. turbolay deletes are soft deletes: the `Minus` delta and the segment tombstone are
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

`delete_edge` (`src/shard/write.rs:3089`) has the exact shape of `write_edge` from the last
chapter: validate, take a write permit, lock the cell's write lane, then a bounded retry loop
around `delete_edge_txn`, which takes the object-store lock and runs `delete_edge_txn_locked`.
The interesting part is that transaction, and it splits into two branches depending on where
the edge currently lives.

=== Branch one: a materialized edge

If the edge has a live adjacency row (`out_edge`), it is materialized. The delete removes the
structural keys, decrements the degree counters, and appends a `Minus` delta:

#srcblock("src/shard/write.rs:3297-3401 (abridged)")[```rust
decode_edge_record(&edge_key, &existing)?;
let epoch = next_epoch_txn(&txn, &mutation.cell_id).await?;
let delta = DeltaRecord { kind: DeltaKind::Minus, edge: record.clone() };
let delta_value = encode_delta_record(&delta);
put_scoped_delta_indexes_txn(&txn, &delta)?;
// degree counters decremented with saturating_sub(1) ...
tombstone_relationships_for_structural_edge_delete_txn(&txn, mutation, epoch, /* ... */).await?;
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

#srcblock("src/shard/write.rs:3275-3294 (abridged)")[```rust
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
  removed because it lives in immutable compacted storage. turbolay has segment tombstones for
  edges inside segments (`cell/<cell>/seg/tomb/out/<et>/<src>/<dst>`, `keys.rs:104`) and
  relationship tombstones for relationships (`cell/<cell>/rel_tomb/...`, `keys.rs:189`). A read
  checks the tombstone's epoch: the segment edge is visible only while no tombstone at or before
  the read epoch exists.
]

The read side honors this through `segment_edge_visible` and `out_segment_tombstone_epoch_at`
(read chapter): a segment edge counts as present only if there is no tombstone with
`epoch <= read_epoch`. So the tombstone, like the `Minus` delta, is a soft delete stamped with
an epoch.

Both branches also call `tombstone_relationships_for_structural_edge_delete_txn`. When the
structural edge between two entities is deleted, any relationships riding on it are tombstoned
in the same transaction, so a relationship never outlives the edge that carries it.

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

Deleting a whole vertex (`delete_vertex`, `detach_delete_vertex`, `write.rs:331`) is built on
the same primitive: it finds the vertex's incident edges and relationships and tombstones or
deletes each, returning a `VertexDeleteResult` with the counts.

== The problem garbage collection solves

Every write appends a delta, and every delete appends another delta or a tombstone. Left alone,
a cell's delta history grows without bound, and reads get slower because the merge replays a
longer and longer tail. Garbage collection reclaims that space, but it cannot simply delete old
records, because a reader pinned at an old epoch might still need them.

#term("Garbage collection (GC)")[
  Reclaiming storage that is no longer needed. In turbolay, GC physically removes delta records,
  tombstones, and superseded artifacts that predate a safety boundary. The whole difficulty is
  computing that boundary correctly, so that GC never removes anything a current or possible
  future read at an allowed epoch could still require.
]

The boundary is captured in two ideas: a per-edge-type watermark that records how far history
has already been compacted away, and a safe epoch that GC is allowed to advance the watermark
up to.

== The GC watermark

#term("Delta GC watermark")[
  A per-edge-type epoch recorded at `cell/<cell>/meta/delta_gc/<edge_type>` (`keys.rs:661`). It
  marks the oldest epoch whose deltas still exist. Reads below the watermark are refused with a
  snapshot-expired error, because the deltas needed to reconstruct that snapshot have been
  collected. GC advances the watermark and then deletes everything below it.
]

The read side already enforces this: `deltas_between` returns a snapshot-expired error when a
read tries to start below the watermark (read chapter). So the watermark is the contract between
GC and reads. GC may only advance it to an epoch that is provably safe.

That safe epoch is computed by `delta_gc_safe_epoch`:

#srcblock("src/shard/lifecycle.rs:859-873")[```rust
pub(crate) async fn delta_gc_safe_epoch(
    &self, cell_id: &str, edge_type: &str,
) -> Result<GraphEpoch> {
    let current_epoch = self.current_epoch(cell_id).await?;
    let retained_safe_epoch =
        current_epoch.saturating_sub(self.retention_policy.min_retained_epochs);
    if self.min_active_read_epoch(cell_id).await?.is_some() {
        let watermark = self.delta_gc_watermark(cell_id, edge_type).await?;
        Ok(retained_safe_epoch.min(watermark))
    } else {
        Ok(retained_safe_epoch)
    }
}
```]

Two forces bound it.

#term("Retention policy")[
  Configuration that says how much recent history to keep no matter what. Its
  `min_retained_epochs` keeps at least that many epochs below the current one uncollected, so
  time-travel reads within that window always work. GC will not advance past
  `current_epoch - min_retained_epochs`.
]

#term("Read lease")[
  A record a reader publishes to announce "I am reading at epoch N, do not collect below it". It
  lives under `cell/<cell>/read_lease/<id>` (`keys.rs:27`) and has a time-to-live. While any
  live lease exists, GC must not advance past the oldest leased epoch, so an in-flight query is
  never starved of the deltas it needs.
]

The read-lease floor is computed by `min_active_read_epoch`, which scans the lease keys, deletes
expired ones as it goes, and returns the minimum epoch of the live leases:

#srcblock("src/shard/lifecycle.rs:790-798 (head)")[```rust
pub(crate) async fn min_active_read_epoch(&self, cell_id: &str) -> Result<Option<GraphEpoch>> {
    if self.retention_policy.read_lease_ttl_ms == 0 {
        return Ok(None);
    }
    let now_ms = graph_now_millis();
    let prefix = keys::read_lease_prefix(cell_id);
    let mut iter = self.scan_remote_prefix(&prefix).await?;
    // ... walk leases, delete expired, track the minimum live epoch ...
}
```]

Put together: the safe epoch is `current - min_retained_epochs`, and when any reader holds a
lease it is additionally capped at the current watermark so an active read window is never
collected past.

#figure(
  diagram(
    node-stroke: 0.5pt,
    spacing: (0.35cm, 0.7cm),
    node((0, 0), [epoch 0], stroke: none, fill: none),
    node((4.6, 0), [current epoch], stroke: none, fill: none),
    node((0, 1), [collected\ (below watermark)], fill: rgb("#f0d0d0"), width: 3.4cm),
    node((1.6, 1), [collectable\ (safe to GC)], fill: rgb("#fff8e6"), width: 3.4cm),
    node((3.2, 1), [retained\ (`min_retained_epochs`)], fill: rgb("#e9fce9"), width: 3.6cm),
    node((1.6, 2), [watermark], fill: none, stroke: none),
    node((3.2, 2), [safe epoch], fill: none, stroke: none),
    edge((1.6, 1), (1.6, 2), "-", stroke: 0.4pt + muted),
    edge((3.2, 1), (3.2, 2), "-", stroke: 0.4pt + muted),
  ),
  caption: none,
)
#figcap[The GC safety boundary along the epoch axis. GC advances the watermark rightward toward the safe epoch, never past it. An active read lease pulls the safe epoch left so a live query's snapshot survives.]

== Rollup, then delta GC

Removing deltas is only safe once the information in them has been folded into a compacted
artifact. That folding is rollup.

#term("Rollup")[
  Compacting recent history into a more permanent form: folding delta records and segments into
  a new matrix artifact at a higher base epoch. After a rollup produces an artifact at epoch E,
  a read at any epoch at or above E can start from that artifact and no longer needs the deltas
  below E, which makes those deltas collectable. Rollup is distinct from the mutation-log
  materialize of Chapter 3: materialize turns the append log into deltas, rollup turns deltas
  into artifacts.
]

Delta GC is therefore gated on a rollup having already produced an artifact at the target epoch.
`delete_deltas_through_rollup` (`src/shard/maintenance.rs:14`) enforces exactly that, then
advances the watermark and deletes the old delta records:

#srcblock("src/shard/maintenance.rs:27-65 (abridged)")[```rust
let safe_epoch = self.delta_gc_safe_epoch(cell_id, edge_type).await?;
if compact_through_epoch > safe_epoch {
    return Err(self.record_retention_reject(/* ... */));      // never past the safe epoch
}
let Some(artifact) = self.latest_matrix_artifact(cell_id, edge_type, compact_through_epoch).await?
else {
    return Err(/* "cannot compact deltas without a matrix rollup artifact" */);
};
if artifact.base_epoch != compact_through_epoch {
    return Err(/* artifact must be exactly at the target epoch */);
}
let mut watermark_batch = GraphWriteBatch::new();
watermark_batch.put(keys::delta_gc_watermark(cell_id, edge_type), encode_u64(compact_through_epoch));
self.write_graph_batch_strict_with_cell_lock(cell_id, "delete_deltas_through_rollup", watermark_batch).await?;
```]

After the watermark is raised, it deletes every kind of delta record below it: the `outbox`
deltas and outbox batches, the `delta/plus` and `delta/minus` streams, and the scoped
`delta_owner` and `delta_pair` indexes for both kinds (`maintenance.rs:71-121`). The deletes are
batched so a large history does not build one enormous transaction:

#srcblock("src/shard/maintenance.rs:268-285")[```rust
async fn flush_delta_gc_batch(
    &self, cell_id: &str, batch: &mut GraphWriteBatch, pending_deletes: &mut usize,
) -> Result<()> {
    if *pending_deletes == 0 { return Ok(()); }
    let batch_to_write = std::mem::replace(batch, GraphWriteBatch::new());
    self.write_graph_batch_strict_with_cell_lock(cell_id, "delete_deltas_through_rollup", batch_to_write).await?;
    *pending_deletes = 0;
    Ok(())
}
```]

The flush fires whenever `pending_deletes` reaches `GRAPH_DELTA_GC_BATCH_KEYS` (512, `lib.rs:302`).
The order matters: the watermark is raised first, then the deltas are removed. If the process
dies midway, the watermark already forbids reads that would need the half-removed deltas, so
there is no window where a read could see an inconsistent history.

== Artifact garbage collection

Rollup also leaves behind superseded artifacts. Once a newer matrix artifact at a higher base
epoch exists, the older ones below the safe boundary are dead weight.
`delete_graph_artifacts_before` (`src/engine/supernode.rs:366`) removes them:

#srcblock("src/engine/supernode.rs:379-402 (abridged)")[```rust
let safe_keep_epoch = self.artifact_gc_safe_keep_epoch(cell_id, edge_type).await?;
if keep_epoch > safe_keep_epoch {
    return Err(self.record_retention_reject(/* ... */));
}
for prefix in graph_artifact_gc_prefixes(cell_id, edge_type) {
    let mut iter = self.scan_remote_prefix(&prefix).await?;
    while let Some(kv) = iter.next().await? {
        let Some(base_epoch) = graph_artifact_epoch_from_key(&key)? else {
            result.retained_keys += 1; continue;
        };
        if base_epoch < keep_epoch {
            batch.delete(key.as_bytes());
            // flush in batches of GRAPH_ARTIFACT_GC_BATCH_KEYS ...
        }
    }
}
```]

Its safe boundary, `artifact_gc_safe_keep_epoch` (`lifecycle.rs:875`), is the mirror of the
delta one: if any read lease is active it keeps everything (returns 1), otherwise it keeps the
artifact that a retention-window read would land on and collects everything strictly older. The
matrix artifacts, posting chunks, and supernode groups all share this path.

== Dropping a whole graph

The largest delete removes an entire cell. `drop_cell` (`src/shard/write.rs:718`) follows the
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
+ A background rollup folds history into a matrix artifact at some epoch E, so reads at or above
  E no longer need the deltas below E.
+ `delete_deltas_through_rollup` confirms E is at or below the safe epoch (respecting retention
  and any read lease), raises the watermark to E, and physically deletes the `Minus` delta and
  every other delta below E.
+ `delete_graph_artifacts_before` removes the now-superseded old artifacts.

The delete is only truly gone in step four, and only once no reader can still need it. This is
the same append-only, epoch-stamped discipline the write and read chapters showed, now closing
the loop by reclaiming what those chapters produced. The final chapter turns to the caches that
sit in front of all of this and how they, too, stay correct as epochs advance and data is
collected.
