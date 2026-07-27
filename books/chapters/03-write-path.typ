#import "../template.typ": custom-box, srcblock, accent, muted
#import "../vendor/bookly/src/themes/reader.typ": reader-colors
#import "@preview/fletcher:0.5.7" as fletcher: diagram, node, edge

= The Write Path

The read chapter consumed a read epoch as if it already existed. This chapter shows where it
comes from. A write to TurboLay is not a single `put`. It is a guarded sequence: confirm this
node is allowed to write, take a global permit and a per-cell lane, re-read SlateDB's manifest
to confirm this process is still the writer, open a serializable transaction, check the cell is
not being dropped, check the write has not already happened, write the edge and every index key
it implies, and commit atomically to the object store. We follow that sequence for one edge,
then widen to batches, lanes, and idempotency.

The main file is `src/shard/write.rs`, 5,108 lines. The single-writer machinery lives in
`src/core/state.rs` and `src/shard/lifecycle.rs`; the key vocabulary it writes into is
`src/keys.rs`, 316 lines.

#custom-box(title: [Why], icon: "tip")[
  One thing this chapter does *not* describe is a queue. An earlier edition of TurboLay split a
  write into an append to a mutation log and a later background materialization, and stamped
  every change into an outbox of delta rows for readers to replay. None of that survives: the
  key builders (`last_epoch`, `mutation_log_*`, `outbox*`, `delta_*`) and the functions
  (`append_edge_mutation_log`, `materialize_edge_mutation_log`) are gone from the tree. A write
  is committed once, in one transaction, and the only asynchronous work it schedules is a
  single dirty flag per edge type. If you remember a two-phase write here, that memory is of a
  system that no longer exists.
]

== The shape of every write

Every foreground mutation, whether it writes one edge or a thousand, passes through the same
nested layers. Keep this envelope in mind; the sections below open each layer in turn.

#figure(
  diagram(
    crossing-fill: reader-colors.paper,
    node-stroke: 0.6pt + reader-colors.info,
    node-fill: reader-colors.info_soft,
    edge-stroke: reader-colors.muted,
    spacing: (0pt, 0.62cm),
    node((0, 0), text(fill: reader-colors.text, size: 9pt)[1. validate + `ensure_write_authority` (may this node write?)], width: 12.5cm),
    edge((0, 0), (0, 1), "->", stroke: reader-colors.muted),
    node((0, 1), text(fill: reader-colors.text, size: 9pt)[2. `acquire_graph_write_permit` (global write semaphore)], width: 12.5cm),
    edge((0, 1), (0, 2), "->", stroke: reader-colors.muted),
    node((0, 2), text(fill: reader-colors.text, size: 9pt)[3. `writer_lane(cell).lock()` (one of 64 in-process mutexes)], width: 12.5cm),
    edge((0, 2), (0, 3), "->", stroke: reader-colors.muted),
    node((0, 3), text(fill: reader-colors.text, size: 9pt)[4. retry loop, up to `GRAPH_TXN_MAX_RETRIES` = 32],
      fill: reader-colors.warn_soft, stroke: 0.6pt + reader-colors.warn, width: 12.5cm),
    edge((0, 3), (0, 4), "->", stroke: reader-colors.muted),
    node((0, 4), text(fill: reader-colors.text, size: 9pt)[5. `acquire_local_write_guard` #sym.arrow.r `refresh_writer_fence` (am I still the writer?)],
      fill: reader-colors.purple_soft, stroke: 0.6pt + reader-colors.purple, width: 12.5cm),
    edge((0, 4), (0, 5), "->", stroke: reader-colors.muted),
    node((0, 5), text(fill: reader-colors.text, size: 9pt)[6. `begin` #sym.arrow.r `validate_write_fence_txn` #sym.arrow.r reads/puts #sym.arrow.r `commit_txn_strict`],
      fill: reader-colors.ok_soft, stroke: 0.6pt + reader-colors.ok, width: 12.5cm),
  ),
  caption: [The write envelope, top to bottom as `write_edge` (`src/shard/write.rs:2354-2384`)
    and `write_edge_txn` (`:2386-2392`) execute it. Layers 1 to 3 decide whether and when this
    process may proceed, layer 5 asks SlateDB's manifest whether this process is still the
    legitimate writer, and layer 6 makes the change atomic. Notice that no layer takes a lock
    in the object store: nothing here waits on a peer.],
) <fig-write-envelope>

#custom-box(title: [Term — Mutation], icon: "info")[
  A single graph change: add an edge, delete an edge, create a relationship, set metadata.
  TurboLay represents the common case as an `EdgeMutation` (`src/core/model.rs:52-58`), which
  carries the cell id, edge type, source and destination vertex ids, and an idempotency key
  (Section 3.8).
]

== Write entry points

The public mutating methods on `GraphShard` all live in `write.rs`. The ones you will meet
most often:

- `write_edge` (`write.rs:2354`): the primary single-edge upsert. This is what a Cypher `MERGE`
  of an edge becomes.
- `create_relationship` (`write.rs:1783`): create a relationship (a Cypher `CREATE`), with its
  own id and properties. Variants add vertex metadata (`:1797`) or full metadata (`:1812`).
- `delete_edge` (`write.rs:3184`) and `delete_relationship` (`write.rs:2197`).
- `write_edge_mutations_batch` (`write.rs:3635`): the primary atomic multi-edge batch.
- `ingest_edge_mutations` (`write.rs:3738`): chunks a large stream into batches, bounded by
  `max_bulk_import_edges`.
- `bulk_import_edges` (`write.rs:3432`) and `bulk_append_edges_trusted` (`write.rs:3449`):
  high-volume import paths.

A Cypher write reaches these from the query layer. `execute_opencypher`
(`src/shard/query.rs:228`) parses the statement as a mutation; a patternless `MERGE`/`CREATE`
is dispatched by `execute_patternless_mutation` (`query.rs:1280`), which maps a single edge
`MERGE` to `write_edge`, a single `CREATE` to `create_relationship`, and multiple creates to a
batch. The idempotency key is derived deterministically from the query context:

#srcblock("src/shard/query.rs:1434-1437")[```rust
idempotency_key: format!(
    "{}.merge.{}.{}.{}",
    context.idempotency_key, edge_type, src, dst
),
```]

so the same logical query retried produces the same key. The `CREATE` path builds the same
shape with a `.create.` infix and a zero-padded ordinal (`query.rs:1340-1343`).

== The single-writer guarantee

Chapter 0 said only one writer per cell may exist, and named the mechanism that enforces it:
SlateDB manifest fencing. TurboLay does not rest the promise on that one mechanism alone.
Three independent things line up behind every commit — the node's *write authority*, the
*manifest fence* checked immediately before the transaction, and the *serializable transaction*
itself — and a write that satisfies all three cannot race another writer.

The three are not redundant. They answer three different questions, and each is blind to the
other two: *may this node write at all?*, *is this process still the writer as of right now?*,
and *did anything this write depends on change underneath it?*

#let tier-title(t) = text(size: 8pt, weight: "bold", fill: reader-colors.text)[#t]
#let defend(t) = text(size: 7pt, fill: reader-colors.muted, style: "italic")[#t]
#figure(
  diagram(
    crossing-fill: reader-colors.paper,
    node-stroke: 0.6pt,
    node-shape: fletcher.shapes.rect,
    spacing: (1.0cm, 1.8cm),
    // Three concentric tiers, all centered on (0,0): outer -> inner.
    node((0, 0), box(width: 9.6cm, height: 6.6cm, inset: 6pt, align(top + center, stack(spacing: 4pt,
      tier-title[Tier 1 · write authority],
      tier-title[`ReadOnly` / `Promotable` / `Writer`]))),
      fill: reader-colors.surface_soft, stroke: reader-colors.border, corner-radius: 4pt, name: <t1>),
    node((0, 0), box(width: 7.6cm, height: 4.6cm, inset: 6pt, align(top + center, stack(spacing: 4pt,
      tier-title[Tier 2 · SlateDB manifest fencing],
      tier-title[`refresh_manifest()` · no lock, no lease]))),
      fill: reader-colors.purple_soft, stroke: reader-colors.purple, corner-radius: 4pt, name: <t2>),
    node((0, 0), box(width: 5.9cm, height: 2.4cm, inset: 6pt, align(top + center, stack(spacing: 5pt,
      tier-title[Tier 3 · serializable txn],
      text(size: 8pt, weight: "bold", fill: reader-colors.ok)[drop-guard · conflict detection · commit]))),
      fill: reader-colors.ok_soft, stroke: reader-colors.ok, corner-radius: 4pt, name: <t3>),
    // "defends against" side labels, one per tier.
    node((1, -1), defend[defends against:\ a read-only node\ writing at all], stroke: none, fill: none, name: <d1>),
    node((1, 0), defend[defends against:\ a superseded writer\ process], stroke: none, fill: none, name: <d2>),
    node((1, 1), defend[defends against:\ a concurrent change\ to what was read], stroke: none, fill: none, name: <d3>),
    edge(<t1>, <d1>, "-", stroke: (paint: reader-colors.muted, dash: "dashed")),
    edge(<t2>, <d2>, "-", stroke: (paint: reader-colors.muted, dash: "dashed")),
    edge(<t3>, <d3>, "-", stroke: (paint: reader-colors.muted, dash: "dashed")),
  ),
  caption: [The three tiers behind single-writer atomicity, nested from the outermost policy
    check to the innermost commit. There is no cell write lock, no owner token, and no lease:
    the middle tier is SlateDB's own manifest, which invalidates a superseded writer rather
    than asking it to stand down, and the innermost serializable commit is what makes the
    multi-key edge write atomic within the cell.],
) <fig-ch03-tiers>

=== Tier 1: write authority

Before anything else, `ensure_write_authority` checks that this node is allowed to write at
all. The authority is a property of how the shard was opened, and it has three levels, not two:

#srcblock("src/core/state.rs:472-476")[```rust
pub(crate) enum GraphWriteAuthority {
    ReadOnly,
    Promotable,
    Writer,
}
```]

A `ReadOnly` shard can never write; the call fails immediately with
`GraphError::WriteRequiresWriter`. `Promotable` and `Writer` both fall through to the same
call, which resolves the role the only way that actually matters:

#srcblock("src/shard/lifecycle.rs:404-418")[```rust
pub(crate) fn ensure_write_authority(
    &self,
    cell_id: &str,
    operation: &'static str,
) -> Result<()> {
    match &self.write_authority {
        GraphWriteAuthority::ReadOnly => Err(GraphError::WriteRequiresWriter {
            operation,
            cell_id: cell_id.to_string(),
        }),
        GraphWriteAuthority::Promotable | GraphWriteAuthority::Writer => {
            self.db.writer().map(|_| ())
        }
    }
}
```]

`self.db.writer()` returns the cached SlateDB writer handle, or
`GraphError::ReadOnlyShardStorage` if none has been opened (`src/core/state.rs:183-185`). Note
what the two live variants have in common and where they differ. Both are *permitted* to write;
the difference is only whether a writer handle has been opened yet. That is why the `match` arm
is shared: authority is policy, and the handle is a resource.

One more check rides inside the transaction. `validate_write_fence_txn` — despite the
historical name, not a token fence — first refuses to touch a cell that is being dropped, then
re-confirms authority:

#srcblock("src/shard/lifecycle.rs:436-455")[```rust
pub(crate) async fn validate_write_fence_txn(
    &self, txn: &DbTransaction, cell_id: &str, operation: &'static str,
) -> Result<()> {
    if operation != "drop_cell" {
        let drop_marker = keys::cell_drop_marker(cell_id);
        let pending_drop_marker = keys::cell_drop_pending_marker(cell_id);
        if read_txn_remote(txn, &drop_marker).await?.is_some()
            || read_txn_remote(txn, &pending_drop_marker).await?.is_some()
        {
            return Err(GraphError::CellDropped { operation, cell_id: cell_id.to_string() });
        }
    }
    self.ensure_write_authority(cell_id, operation)
}
```]

It is a drop-guard, not an ownership arbiter. `read_txn_remote` calls `txn.mark_read(...)`
before it reads (`src/codec.rs:3-8`), so the drop markers *participate in conflict detection*
(Section 3.4). A concurrent `drop_cell` that commits first therefore forces this write to abort
rather than resurrect a cell someone is tearing down. The non-transactional sibling,
`validate_write_fence` (`src/shard/lifecycle.rs:457-470`), refreshes the manifest fence and then
runs the same check inside a throwaway transaction; maintenance paths use it as a precondition.

=== Tier 2: SlateDB manifest fencing and writer promotion

Write authority says this node *may* write. It cannot say whether some *other* process has since
opened the same cell and taken the writer role away. That question is answered one layer down,
and TurboLay does not answer it itself.

#custom-box(title: [Term — SlateDB manifest fencing], icon: "info")[
  SlateDB records the current writer's identity in its own manifest in the object store. Opening
  a new writer advances that record, which *fences* every earlier writer: the superseded handle
  is not asked to stand down and is not notified, it is simply invalidated, and its next
  operation fails with `Closed(Fenced)`. The guarantee holds without a clock and without the
  fenced process being responsive, because it is enforced where the write lands rather than by
  agreement beforehand. TurboLay stores no lock object, no owner token, and no TTL of its own.
]

The whole of TurboLay's participation is one method that asks SlateDB to re-read the manifest,
and reacts to being fenced by throwing away the handle it can no longer use:

#srcblock("src/core/state.rs:187-203")[```rust
pub(crate) async fn refresh_writer_fence(&self) -> Result<()> {
    let _open_guard = self.inner.writer_open_gate.lock().await;
    let writer = self.open_writer().ok_or(GraphError::ReadOnlyShardStorage)?;
    match writer.refresh_manifest().await {
        Ok(()) => Ok(()),
        Err(err) if matches!(err.kind(), ErrorKind::Closed(CloseReason::Fenced)) => {
            *self.inner.writer.write()... = None;
            Err(err.into())
        }
        Err(err) => Err(err.into()),
    }
}
```]

Dropping the cached handle is the entire local response. There is nothing to release and no
state to unwind, because this node never held anything a peer needs handed back. The error
propagates to the caller as a failed write.

That method is called from exactly the place the old cell write lock used to be taken: the
guard acquired immediately before the transaction opens.

#srcblock("src/shard/lifecycle.rs:253-262")[```rust
pub(crate) async fn acquire_local_write_guard(
    &self, cell_id: &str, operation: &'static str,
) -> Result<LocalWriteGuard> {
    validate_component("cell_id", cell_id)?;
    validate_component("operation", operation)?;
    let guard = LocalWriteGuard::new(Arc::clone(&self.local_write_guard).lock_owned().await);
    self.db.refresh_writer_fence().await?;
    Ok(guard)
}
```]

Two things happen there, and it is worth separating them. The `local_write_guard` mutex
serializes mutating work *within this process* — one write body at a time, whatever cell it
targets. The `refresh_writer_fence` call is the cross-process check, and it is the one that
carries correctness. `write_edge_txn` takes the guard, runs the body, and hands it to
`finish_local_write` (`src/core/state.rs:509-512`), whose only remaining job is to drop it:

#srcblock("src/shard/write.rs:2386-2392")[```rust
pub(crate) async fn write_edge_txn(&self, mutation: &EdgeMutation) -> Result<CommitResult> {
    let lock = self
        .acquire_local_write_guard(&mutation.cell_id, "write_edge")
        .await?;
    let result = self.write_edge_txn_locked(mutation).await;
    finish_local_write(lock, result).await
}
```]

`LocalWriteGuard::renew` and `::release` are both `Ok(())` with no body
(`src/core/state.rs:501-507`). They are the vestigial shape of a lease that no longer exists,
kept only so the call sites did not have to change; there is nothing to renew because nothing
expires.

The other half of tier 2 is how a node that is merely `Promotable` acquires the writer role in
the first place. It does so lazily, on demand:

#srcblock("src/shard/lifecycle.rs:420-434")[```rust
pub(crate) async fn promote_to_writer(
    &self, cell_id: &str, operation: &'static str,
) -> Result<()> {
    validate_component("cell_id", cell_id)?;
    if matches!(&self.write_authority, GraphWriteAuthority::ReadOnly) {
        return Err(GraphError::WriteRequiresWriter { operation, cell_id: cell_id.to_string() });
    }
    self.db.promote_writer().await?;
    Ok(())
}
```]

`GraphStore::promote_writer` (`src/core/state.rs:204-227`) is a double-checked open: it returns
`Ok(false)` immediately if a writer is already cached, takes `writer_open_gate`, checks again,
and only then calls `open_graph_db` and caches the handle. Many concurrent requests to promote
therefore produce exactly one open database, and the return value tells the caller whether *it*
was the one that did the opening.

#custom-box(title: [Term — Writer promotion], icon: "info")[
  The act of a `Promotable` node opening a SlateDB writer for a cell it was until then only
  reading. Promotion needs no coordination with the previous writer and sends it no message:
  opening the database advances the manifest, and that alone fences whoever held it before. The
  loser learns of its demotion the next time it touches storage, not before.
]

#custom-box(title: [Why], icon: "tip")[
  The mechanism this replaced was an object-store lock: a record naming the owner, with a TTL,
  taken by compare-and-set and released on completion. It failed for the reason every
  lease-based lock fails. An owner that pauses longer than its lease — a long garbage-collection
  pause, a stalled network call — is never *told* it has lost ownership. It wakes up believing
  it still holds the lock and writes, while a second process that watched the lease expire has
  legitimately taken over. Both are convinced they are the writer, which is exactly the state
  the lock existed to prevent. Manifest fencing does not have that failure mode, because the
  paused process is not trusted to notice anything: its writes are rejected at the storage layer
  whether or not it has realised it was superseded.
]

=== Tier 3: the serializable transaction

The third guarantee is the transaction itself. Every write runs as one SlateDB transaction at
serializable-snapshot isolation, and every key it reads through `read_txn_remote` — the drop
markers, the idempotency record, the degree counters, the existing edge row — is marked as read
and participates in conflict detection. If any of them changed under the write before it
commits, the commit is rejected and the write retries. The next section opens this layer in
full.

#custom-box(title: [Why], icon: "tip")[
  The three tiers cover gaps the others cannot. Write authority (Tier 1) is a local policy
  decision and is worth nothing against a peer, but it makes a misconfigured read-only node fail
  loudly and immediately rather than at commit time. Manifest fencing (Tier 2) is the only tier
  that can adjudicate between two processes, and it needs neither of the others to be correct.
  The serializable transaction (Tier 3) is blind to processes entirely but is the only tier that
  can see *what* was read: it is what makes the multi-key fan-out of Section 3.6 atomic, and
  what stops a write from committing on top of a cell someone dropped a moment ago. Correctness
  against a competing writer rests on Tier 2; correctness of the write's own contents rests on
  Tier 3; Tier 1 is there so the common misconfiguration produces a clear error.
]

== The transaction and the retry loop

Inside the guard, the actual change runs as one SlateDB transaction.

#custom-box(title: [Term — Serializable snapshot isolation], icon: "info")[
  A transaction guarantee: the transaction sees a consistent snapshot, and if any key it read
  was changed by another committed transaction before it commits, its own commit is rejected.
  It is optimistic, meaning it does not lock rows up front; it detects conflicts at commit time
  and aborts. TurboLay opens every write transaction at this level and marks every key it reads
  so the drop-guard check, the idempotency probe, and the counter reads all participate.
]

The transaction is opened on the writer handle and immediately runs the drop-guard:

#srcblock("src/shard/write.rs:2551-2557")[```rust
let txn = self
    .db
    .writer()?
    .begin(IsolationLevel::SerializableSnapshot)
    .await?;
self.validate_write_fence_txn(&txn, &mutation.cell_id, operation)
    .await?;
```]

and is committed by `commit_txn_strict`, which forwards the `await_durable` flag into SlateDB's
`WriteOptions` (`src/codec.rs:137-160`).

Because a serializable transaction can abort under contention, the public method wraps it in a
bounded retry loop. This is `write_edge` in full, and it is the template every writer follows:

#srcblock("src/shard/write.rs:2354-2384 (abridged)")[```rust
pub async fn write_edge(&self, mutation: EdgeMutation) -> Result<CommitResult> {
    validate_component("cell_id", &mutation.cell_id)?;
    validate_component("edge_type", &mutation.edge_type)?;
    validate_component("idempotency_key", &mutation.idempotency_key)?;
    self.ensure_write_authority(&mutation.cell_id, "write_edge")?;

    let _permit = self.acquire_graph_write_permit("write_edge").await?;
    let _writer = self.writer_lane(&mutation.cell_id).lock().await;
    for attempt in 0..GRAPH_TXN_MAX_RETRIES {
        match self.write_edge_txn(&mutation).await {
            Err(err)
                if is_retryable_write_conflict(&err)
                    && attempt + 1 < GRAPH_TXN_MAX_RETRIES =>
            {
                /* self.operation_metrics.write_retries += 1 */
                tokio::task::yield_now().await;
            }
            Ok(result) => {
                /* self.operation_metrics.write_commits += 1 */
                return Ok(result);
            }
            result => return result,
        }
    }
    Err(GraphError::RetryExhausted {
        operation: "graph transaction",
        attempts: GRAPH_TXN_MAX_RETRIES,
    })
}
```]

Read that against the envelope figure: authority, permit, lane, retry — and no lock anywhere.
Authority is checked once, up front, so a `ReadOnly` shard fails before it ever takes a permit.

Inside the loop, only some errors are worth retrying, and the predicate is now a single
`matches!`:

#srcblock("src/core/state.rs:514-516")[```rust
pub(crate) fn is_retryable_write_conflict(err: &GraphError) -> bool {
    matches!(err, GraphError::Slate(err)
        if matches!(err.kind(), ErrorKind::Transaction | ErrorKind::Invalid))
}
```]

Two SlateDB error kinds, and nothing else. There is no `CellWriteConflict` variant to retry on —
that error was removed with the lock it described. Note in particular what is *not* retryable:
`ErrorKind::Closed(CloseReason::Fenced)`. Being fenced is not contention, it is a change of
management, and retrying would be an attempt to write with a handle SlateDB has already
invalidated. Every other error — a validation failure, a dropped cell, a missing writer role —
falls through the `match`'s catch-all arm and returns immediately, because retrying a write that
is invalid or not permitted is pointless.

== Where the epoch comes from

The epoch is the version stamp the whole read path depends on. In an earlier edition, allocating
it was a step in the write: read a `last_epoch` counter, add one, write it back. That counter no
longer exists — `keys::last_epoch` was removed along with the rest of the delta machinery — and
neither does the step. The write does not allocate an epoch at all. It *reads* one, off the
transaction:

#srcblock("src/shard/write.rs:2564")[```rust
let current_epoch = txn.seqnum();
```]

That single line appears at the top of essentially every transaction body in the file — sixteen
sites, including `write.rs:643` in the vertex-metadata path, `:1909` in the relationship create,
and `:3253` in the delete. It is SlateDB's own sequence number for the snapshot the transaction
is reading at: a `StorageSequence`, the one and only sequence in the system (Chapter 0,
Section 0.7).

The epoch a write *returns* is that number plus one — the sequence its own commit will land at:

#srcblock("src/shard/write.rs:2615-2624")[```rust
let epoch = current_epoch
    .checked_add(1)
    .ok_or_else(|| GraphError::CorruptValue {
        key: "storage_sequence".to_string(),
        reason: "epoch overflow".to_string(),
    })?;
let result = CommitResult {
    epoch,
    already_existed: existing_edge_epoch.is_some(),
};
```]

Notice the `key:` field of that error. It is the literal string `"storage_sequence"`, not a key
name, because there is no key to name. The same arithmetic appears inside
`commit_txn_strict_with_sequence` (`src/codec.rs:143-153`), which computes the committed
sequence the same way and hands it back to callers that need it.

#custom-box(title: [Why], icon: "tip")[
  Deriving the epoch instead of allocating it removes an entire class of problem. A counter key
  is a key every concurrent write must read and write, which makes it the hottest conflict point
  in the cell — two writes that touch unrelated vertices still collide on it, and every one of
  those collisions costs a serializable abort and a retry. A `txn.seqnum()` is free, is already
  consistent with the snapshot the transaction is reading, and cannot disagree with the sequence
  the commit actually lands at. It also removes the possibility of the counter and the data
  disagreeing after a partial failure, because there is no longer anything that could disagree.
]

Three consequences are worth stating explicitly.

First, a redundant write does not move the version. If the edge already exists and no metadata
changed, the write commits the idempotency record and returns `CommitResult { epoch:
existing_epoch, already_existed: true }` without taking the `checked_add` path at all
(`write.rs:2600-2612`). Second, "the epoch of an existing edge" is not stored on the edge:
`edge_epoch_at_txn` (`src/codec.rs:34-53`) returns the *read* epoch for a live adjacency row,
and only a segment-resident edge carries a sequence of its own. The `EdgeRecord` has no epoch
field (Chapter 0, Section 0.5). Third, a batch does not need to reserve a contiguous run of
epochs, because it never allocated them: every edge in one batch transaction shares the one
sequence that transaction commits at.

Relationship creates do still allocate from a real counter, but it is an identity counter, not a
clock: `cell/<cell_id>/meta/last_relationship_id` (`src/keys.rs:19-21`), read, incremented past
any id already in use, and written back in the same transaction (`write.rs:1926-1968`).

== What keys a single edge write touches

Here is the payoff. When `write_edge` inserts a genuinely new edge, it writes this exact set of
keys, in one transaction (`src/shard/write.rs:2665-2708`):

#srcblock("src/shard/write.rs:2665-2707 (abridged)")[```rust
mark_adjacency_dirty_txn(&txn, cell, et, epoch)?;                  // matrix_dirty + adjacency_generation
let out_degree = read_counter_txn(&txn, &out_degree_key).await? + 1;
// ... in_degree likewise, only if the reverse index is enabled ...
txn.put(keys::out_edge(cell, et, src, dst).as_bytes(), &edge_value)?;
if self.writes_reverse_index() {
    txn.put(keys::in_edge(cell, et, dst, src).as_bytes(), &edge_value)?;
}
txn.put(out_degree_key.as_bytes(), encode_u64(out_degree))?;
if let Some((in_degree_key, in_degree)) = in_degree {
    txn.put(in_degree_key.as_bytes(), encode_u64(in_degree))?;
}
txn.put(idem_key.as_bytes(), encode_commit_idempotency(mutation, &result))?;
commit_txn_strict(txn, self.await_durable_writes).await?;
```]

Laid out, one logical edge fans out into a handful of physical keys, each serving a different
read:

#figure(
  table(
    columns: (auto, auto, 1fr),
    inset: 5pt,
    align: (left + top, left + top, left + top),
    stroke: 0.4pt + reader-colors.border,
    fill: (_, row) => if row == 0 { reader-colors.surface_soft },
    table.header(
      text(fill: reader-colors.text)[*Key builder*],
      text(fill: reader-colors.text)[*Shape*],
      text(fill: reader-colors.text)[*Read it serves*],
    ),
    [`out_edge` (`keys.rs:43`)], [`.../e/out/{et}/{src}/{dst}`], [scan a vertex's outgoing neighbors],
    [`in_edge` (`keys.rs:47`)], [`.../e/in/{et}/{dst}/{src}`], [scan a vertex's incoming neighbors; written only when the reverse index is enabled],
    [`degree_out` (`keys.rs:158`)\ `degree_in` (`keys.rs:162`)], [`.../cnt/out/{et}/{src}`\ `.../cnt/in/{et}/{dst}`], [read a vertex's degree without scanning it],
    [`matrix_dirty` (`keys.rs:27`)], [`.../meta/matrix_dirty/{et}`], [tells the out-of-process indexer this edge type has drifted],
    [`adjacency_generation` (`keys.rs:31`)], [`.../meta/adjacency_generation/{et}`], [the sequence at which this edge type last changed],
    [`idempotency` (`keys.rs:35`)], [`.../idem/{op}/{key}`], [detect and short-circuit a retried write],
  ),
  caption: [Every key one new-edge `write_edge` transaction writes. There is no `last_epoch`
    key because the epoch is read from the transaction (Section 3.5), and no `outbox` or delta
    key because the delta subsystem was removed; what took their place is the two-byte-cheap
    dirty marker in the fourth row.],
) <tab-ch03-write-keys>

The two markers in the middle of that table are the whole of what a write hands to asynchronous
work, and they are written together by one small helper:

#srcblock("src/shard/write.rs:5002-5017")[```rust
fn mark_adjacency_dirty_txn(
    txn: &DbTransaction,
    cell_id: &str,
    edge_type: &str,
    epoch: StorageSequence,
) -> Result<()> {
    txn.put(
        keys::matrix_dirty(cell_id, edge_type).as_bytes(),
        encode_u64(epoch),
    )?;
    txn.put(
        keys::adjacency_generation(cell_id, edge_type).as_bytes(),
        encode_u64(epoch),
    )?;
    Ok(())
}
```]

#custom-box(title: [Term — Dirty marker], icon: "info")[
  A single key per `(cell, edge type)` holding the sequence at which that edge type last
  changed: `cell/<cell_id>/meta/matrix_dirty/<edge_type>` (`src/keys.rs:23-29`). It is a
  *flag*, not a log. It carries no history, so nothing accumulates and nothing has to be
  garbage-collected; overwriting it a million times leaves one key. The out-of-process indexer
  scans the `matrix_dirty/` prefix with `dirty_graph_index_edge_types`
  (`src/engine/index_store.rs:23-45`) to decide which edge types need a fresh index generation.
]

#custom-box(title: [Why], icon: "tip")[
  This is the load-bearing difference between this edition and the last one. The old write wrote
  an `outbox` row *per edge*, plus two scoped delta index rows, so that readers could replay
  history forward from a matrix artifact's base epoch. That made every write more expensive,
  made the read path a merge, and created a body of data whose only purpose was to be collected
  later. The dirty marker does the same job — *tell someone this changed* — in one key that is
  overwritten rather than appended, and the reader closes the residual gap between an index
  generation's `base_sequence` and its read epoch with the WAL-tail overlay instead
  (Chapter 1, Section 1.7; the read chapter has the mechanism).
]

#figure(
  diagram(
    crossing-fill: reader-colors.paper,
    node-stroke: 0.55pt + reader-colors.border,
    edge-stroke: reader-colors.muted,
    spacing: (0.9cm, 0.95cm),
    node((1, 0), text(fill: reader-colors.text, size: 9pt)[one `write_edge` transaction],
      fill: reader-colors.warn_soft, stroke: 0.55pt + reader-colors.warn, width: 5cm),
    node((0, 1), text(fill: reader-colors.text, size: 8.5pt)[`e/out`, `e/in`\ adjacency],
      fill: reader-colors.info_soft, stroke: 0.55pt + reader-colors.info, width: 3.4cm),
    node((1, 1), text(fill: reader-colors.text, size: 8.5pt)[`cnt/out`, `cnt/in`\ degree],
      fill: reader-colors.info_soft, stroke: 0.55pt + reader-colors.info, width: 3.4cm),
    node((2, 1), text(fill: reader-colors.text, size: 8.5pt)[`meta/matrix_dirty`\ dirty marker],
      fill: reader-colors.purple_soft, stroke: 0.55pt + reader-colors.purple, width: 3.4cm),
    node((1, 2), text(fill: reader-colors.text, size: 8.5pt)[`idem`\ idempotency receipt],
      fill: reader-colors.ok_soft, stroke: 0.55pt + reader-colors.ok, width: 3.4cm),
    edge((1, 0), (0, 1), "->", stroke: reader-colors.muted),
    edge((1, 0), (1, 1), "->", stroke: reader-colors.muted),
    edge((1, 0), (2, 1), "->", stroke: reader-colors.muted),
    edge((1, 0), (1, 2), "->", stroke: reader-colors.muted),
    node((3, 1), text(fill: reader-colors.muted, size: 7.5pt, style: "italic")[read later by\ the indexer],
      stroke: none, fill: none, width: 2.6cm),
  ),
  caption: [One edge, a handful of keys, one transaction: all of them commit atomically, so a
    reader never sees an adjacency entry without its matching degree count, and the indexer
    never sees a dirty marker for a change that did not land. The purple marker is the only
    output consumed by another process.],
) <fig-write-key-fanout>

A relationship create adds more on top of the structural edge keys: the `rel/...` record, a
`rel_id/...` pointer back to it, a `rel_count/...` counter, and the relationship-property
indexes (`src/shard/write.rs:2037-2062`). A delete does the mirror image — it removes the
adjacency rows, decrements the degree counters, hard-deletes the relationships riding on the
edge, and marks the edge type dirty at the new sequence (`write.rs:3299-3310`). For an edge that
lives only inside a compacted adjacency segment rather than as an `e/out` row, the delete
instead writes a segment tombstone. The delete chapter takes that apart.

== Write lanes and concurrency

Two mechanisms from Chapter 1 keep writes from colliding inside one process. Neither is a
correctness device; both exist for throughput.

The 64 write lanes (`GRAPH_WRITE_LANES`, `src/lib.rs:178`) are plain mutexes. A write picks its
lane by hashing the cell id, so all writes to one cell take the same lane and serialize, while
writes to different cells run in parallel:

#srcblock("src/codec.rs:1338-1345")[```rust
pub(crate) fn writer_lane_index(cell_id: &str) -> usize {
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;      // FNV-1a offset basis
    for byte in cell_id.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    (hash as usize) % GRAPH_WRITE_LANES
}
```]

The shard-side lookup is a single line: `writer_lane` indexes
`self.writer_lanes` with that hash (`src/shard/lifecycle.rs:249-251`).

#custom-box(title: [Why], icon: "tip")[
  Serializing same-cell writes in memory, before they reach the transaction, is what keeps
  SlateDB serialization aborts rare. Two writes to the same cell would very likely conflict at
  commit, so the lane makes them queue cheaply on a mutex instead of both doing the work and one
  of them aborting and retrying. Writes to different cells never share a lane, so unrelated
  tenants do not slow each other down. Be clear about what this is *not*: a lane says nothing
  about which process may write. Two processes each hold their own set of 64 lanes and neither
  can see the other's. Only Tier 2 arbitrates between them.
]

Above the lanes sits `graph_write_gate`, a semaphore that bounds the total number of writes in
flight across all cells. `acquire_graph_write_permit` (`src/shard/lifecycle.rs:351-360`)
increments a `write_attempts` metric and then waits on it via `acquire_operation_permit`
(`:385-402`), which counts a `backpressure_waits` whenever the gate is already exhausted. The
order is always permit first, then lane lock, so a flood of writes waits on the global gate
rather than piling up while holding lane mutexes.

#custom-box(title: [Term — Backpressure], icon: "info")[
  Slowing down producers when the system is saturated, rather than accepting unbounded work and
  falling over. TurboLay's write gate is backpressure: when the permit limit is reached, new
  writes wait, and the wait is counted as a metric so operators can see the system is at its
  write ceiling.
]

== Durability and idempotency

Two properties make writes safe to retry and safe to trust.

*Durability.* The `await_durable_writes` flag, set from the open options, is threaded into every
commit through `commit_txn_strict` (`src/codec.rs:137-141`), which passes it to
`commit_txn_strict_with_sequence` where it becomes SlateDB's `WriteOptions { await_durable }`
(`:143-160`). When it is on, the commit does not return until the data has reached the object
store durably — which is what makes the returned epoch safe to hand a client as a bookmark
(Chapter 0, Section 0.8). Reads inside a write transaction go through `read_txn_remote`
(`src/codec.rs:3-8`), which uses the `Remote` durability filter, so a writer reads its own
already-durable state and never unflushed data.

*Idempotency.* Every mutation carries an idempotency key, and the transaction probes for a prior
result under `cell/<cell_id>/idem/<operation>/<key>` (`keys::idempotency`, `src/keys.rs:35-37`)
as its first read after the drop-guard. If it is present, the write decodes and returns the
stored result without re-applying anything (`write.rs:2560-2562`).

#custom-box(title: [Term — Idempotency key], icon: "info")[
  A caller-supplied identifier for a logical write. TurboLay records, per key, that the write
  already happened and what it returned. A retry with the same key is detected and returns the
  original result instead of applying the change twice. This is what makes the retry loop of
  Section 3.4 and at-least-once delivery from an upstream ingestion pipeline safe: the same edge
  can be submitted many times and land once.
]

The receipt is written in the same transaction as the data — it is the last `put` before
`commit_txn_strict` in the excerpt above — so it is impossible to apply the change without also
recording that it happened, or to record it without applying the change. Different write paths
use different operation namespaces, which keeps their receipts from colliding: `create`,
`delete`, `vertex-delete`, `bulk-import`, `segment-import`, and `relationship-import` all appear
as the `<operation>` component in `write.rs`. A batch additionally rejects duplicate keys within
itself before it starts.

== Recap: one edge, one transaction

Follow one `MERGE` edge through the whole chapter:

+ `write_edge` validates its arguments and calls `ensure_write_authority`: a `ReadOnly` node
  fails here, a `Promotable` or `Writer` node continues.
+ It takes a global write permit, then the cell's write lane. Both are throughput devices.
+ It enters a retry loop, up to 32 attempts.
+ Each attempt takes the local write guard, which asks SlateDB to refresh the manifest. If this
  process has been fenced by a newer writer, the cached handle is dropped and the write fails —
  it is not retried, because being fenced is not contention.
+ It opens a serializable transaction, confirms the cell is not being dropped, and re-checks
  authority. Both drop markers are marked as read, so a concurrent `drop_cell` aborts this write.
+ It reads the idempotency receipt and returns the stored result if this write already happened.
+ It reads `txn.seqnum()` as the current epoch and derives the commit epoch as that plus one. It
  allocates nothing and writes no counter.
+ It writes the adjacency rows, the degree counters, the `matrix_dirty` and
  `adjacency_generation` markers, and the idempotency receipt.
+ It commits atomically, waiting for object-store durability, and drops the guard.

The epoch it returns is a `StorageSequence` — the same number a reader pins as its read epoch,
and the same number a client carries forward in a bookmark. That is where write and read meet:
not at a delta stream, but at a single sequence. The one thing the write leaves behind for
someone else is a dirty marker, which the out-of-process indexer (Chapter 1, Section 1.8) turns
into a fresh index generation on its own schedule.

The next chapter follows the other kind of change, a delete, and the garbage collection that
eventually removes the tombstones and superseded generations this one creates.
