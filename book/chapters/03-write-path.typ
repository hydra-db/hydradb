#import "../template.typ": term, why, srcblock, figcap, accent, muted
#import "@preview/fletcher:0.5.7" as fletcher: diagram, node, edge

= The Write Path

The read chapter consumed epochs and delta records as if they already existed. This chapter
shows how they are born. A write to turbolay is not a single `put`. It is a carefully guarded
sequence: prove you are the only writer, open a transaction, check you have not been fenced
out, advance the epoch, write the edge and all its index keys, and commit atomically to the
object store. We follow that sequence for one edge, then widen to batches, bulk import, and
the background job that compacts writes into the artifacts the read path relies on.

The main file is `src/shard/write.rs`, about 5,700 lines. The fencing lives in
`src/core/state.rs` and `src/shard/lifecycle.rs`, and the compaction job is in
`src/engine/artifact_build.rs`.

== The shape of every write

Every foreground mutation, whether it writes one edge or a thousand, passes through the same
five nested layers. Keep this envelope in mind; the sections below open each layer in turn.

#figure(
  diagram(
    node-stroke: 0.6pt,
    node-fill: rgb("#eef4ff"),
    spacing: (0pt, 0.62cm),
    node((0, 0), [1. validate + `ensure_write_authority` (in-memory lease check)], width: 12.5cm),
    edge((0, 0), (0, 1), "->"),
    node((0, 1), [2. `acquire_graph_write_permit` (global write semaphore)], width: 12.5cm),
    edge((0, 1), (0, 2), "->"),
    node((0, 2), [3. `writer_lane(cell).lock()` (per-cell in-process mutex)], width: 12.5cm),
    edge((0, 2), (0, 3), "->"),
    node((0, 3), [4. retry loop, up to `GRAPH_TXN_MAX_RETRIES` = 32], fill: rgb("#fff8e6"), width: 12.5cm),
    edge((0, 3), (0, 4), "->"),
    node((0, 4), [5. `acquire_cell_write_lock` (object-store CAS) then the transaction], fill: rgb("#e9fce9"), width: 12.5cm),
    edge((0, 4), (0, 5), "->"),
    node((0, 5), [`begin` #sym.arrow.r `validate_write_fence_txn` #sym.arrow.r reads/puts #sym.arrow.r `commit_txn_strict`], fill: rgb("#e9fce9"), width: 12.5cm),
  ),
  caption: none,
)
#figcap[The write envelope. Layers 1 to 3 keep writers from colliding in the first place; layers 4 and 5 make the actual change safe and atomic even when they do.]

#term("Mutation")[
  A single graph change: add an edge, delete an edge, create a relationship, set metadata.
  turbolay represents the common case as an `EdgeMutation` (`src/core/model.rs:55`), which
  carries the cell id, edge type, source and destination vertex ids, and an idempotency key
  (Section 3.9).
]

== Write entry points

The public mutating methods on `GraphShard` all live in `write.rs`. The ones you will meet
most often:

- `write_edge` (`write.rs:2257`): the primary single-edge upsert. This is what a Cypher
  `MERGE` of an edge becomes.
- `create_relationship` (`write.rs:1588`): create a relationship (a Cypher `CREATE`), with
  its own id and properties.
- `delete_edge` (`write.rs:3089`) and `delete_relationship` (`write.rs:2059`).
- `write_edge_mutations_batch` (`write.rs:3626`): the primary atomic multi-edge batch.
- `ingest_edge_mutations` (`write.rs:3708`): chunks a large stream into batches (default 1024
  edges each).
- `bulk_import_edges` (`write.rs:3411`) and `bulk_append_edges_trusted` (`write.rs:3428`):
  high-volume import paths.
- `append_edge_mutation_log` (`write.rs:3778`) and `materialize_edge_mutation_log`
  (`write.rs:3919`): the two halves of the append-then-materialize path (Section 3.7).

A Cypher write reaches these from the query layer. `execute_opencypher` (`query.rs:71`) parses
the statement as a mutation; a patternless `MERGE`/`CREATE` is dispatched by
`execute_patternless_mutation` (`query.rs:978`), which maps a single edge `MERGE` to
`write_edge`, a single `CREATE` to `create_relationship`, and multiple creates to
`write_edge_mutations_batch`. The idempotency key is derived deterministically from the query
context, for example `format!("{}.merge.{}.{}.{}", context.idempotency_key, edge_type, src, dst)`
(`query.rs:1132`), so the same logical query retried produces the same key.

== The single-writer guarantee

Chapter 0 said only one writer per cell may exist. turbolay does not trust a single mechanism
for this. It layers three, and a write must satisfy all of them.

=== Tier 1: the in-memory lease

Before anything else, `ensure_write_authority` (`lifecycle.rs:579`) checks this process is
allowed to write the cell at all. The authority is one of three states:

#srcblock("src/core/state.rs:216-224")[```rust
pub(crate) enum GraphWriteAuthority {
    ReadOnly,
    Standalone,
    Leased {
        local_node_id: String,
        leases: Arc<RwLock<BTreeMap<String, engine::ShardLease>>>,
    },
}
```]

`ReadOnly` can never write. `Standalone` (a single embedded process) always can. `Leased` (a
node in a cluster) can write a cell only while it holds an unexpired lease for it whose owner
is this node, otherwise the write fails with `StaleShardLease`.

=== Tier 2: the persisted write fence

The in-memory lease is not enough, because another process might believe it holds the lease
too. The durable arbiter is the write fence, a record stored in the object store under a
per-cell key.

#term("Write fence")[
  A small record naming the current legitimate writer of a cell and its lease token, a number
  that increases each time ownership changes. A writer reads the fence inside its transaction
  and checks that the fence still names it with the same token. If another node has taken
  over, the token will have moved and the check fails.
]

#srcblock("src/core/state.rs:226-232")[```rust
pub(crate) struct GraphWriteFence {
    pub(crate) cell_id: String,
    pub(crate) owner_node_id: String,
    pub(crate) lease_token: u64,
    pub(crate) expires_at_ms: u64,
}
```]

The fence lives at `cell/<cell_id>/meta/write_fence` (`keys.rs:19`). It is checked by
`validate_write_fence_txn` inside every write transaction. The check first refuses to write a
cell that is being dropped, then reads the fence and compares it to the active lease:

#srcblock("src/shard/lifecycle.rs:716-735")[```rust
let key = keys::write_fence(cell_id);
let Some(value) = read_txn_remote(txn, &key).await? else {
    return Err(GraphError::WriteRequiresLease { operation, cell_id: cell_id.to_string() });
};
let fence = decode_write_fence(&key, &value)?;
if fence.cell_id == cell_id
    && fence.owner_node_id == lease.owner_node_id
    && fence.lease_token == lease.lease_token
{
    Ok(())
} else {
    Err(GraphError::StaleShardLease {
        cell_id: cell_id.to_string(),
        node_id: lease.owner_node_id,
        lease_token: lease.lease_token,
    })
}
```]

Because this fence read happens inside a serializable transaction (Section 3.4), a concurrent
writer that bumps the fence forces this transaction to abort. A stale writer cannot slip a
write in between reading and committing.

=== Tier 3: the object-store lock

The third tier stops two writers from even entering the transaction at once. It is a lock
file in the object store, taken with a create-if-absent operation.

#term("Compare-and-set (CAS)")[
  An atomic storage operation that only succeeds if the target is in an expected state: create
  an object only if it does not exist, or overwrite it only if its version tag matches. It is
  the one primitive an object store gives you to coordinate writers, and turbolay builds its
  locks and fences on it.
]

`acquire_distributed_write_lock` (`state.rs:547`) loops trying to create the lock object.
`PutMode::Create` succeeds only if no lock exists; on `AlreadyExists` it tries to reclaim the
lock but only if the existing one has expired, otherwise it backs off and retries:

#srcblock("src/core/state.rs:562-609 (abridged)")[```rust
for attempt in 0..GRAPH_CELL_WRITE_LOCK_MAX_ATTEMPTS {   // 256 attempts
    let now_ms = graph_now_millis();
    let payload = encode_cell_write_lock_record(/* owner_token, now, now + ttl, Active */);
    match object_store.put_opts(&path, payload.into(), PutMode::Create.into()).await {
        Ok(_) => return Ok(CellWriteLock { object_store, path, owner_token, ttl_ms }),
        Err(AlreadyExists { .. }) => {
            if let Some(lock) = try_reclaim_distributed_write_lock(/* only if expired */).await? {
                return Ok(lock);
            }
            tokio::time::sleep(Duration::from_millis(GRAPH_CELL_WRITE_LOCK_BACKOFF_MS)).await;
        }
        Err(err) => return Err(err.into()),
    }
}
```]

The lease lifetime is `GRAPH_CELL_WRITE_LOCK_TTL_MS`, five minutes (`lib.rs:306`). The lock is
released after the write through `release_cell_write_lock` (`state.rs:785`), which returns the
body's result and drops the lock together.

#why[
  Three tiers look redundant, but each covers a gap the others cannot. The in-memory lease is
  instant but only knows about this process. The object-store lock coordinates across
  processes but its expiry-based reclaim could, in principle, let a paused old owner and a new
  owner briefly overlap. The transactional fence closes that last gap: even if two writers
  hold what they think is the lock, only the one whose token matches the fence can commit, and
  the other's transaction aborts. Correctness rests on the fence; the lock and lease are there
  to make conflicts rare and cheap.
]

== The transaction and the retry loop

Inside the lock, the actual change runs as one SlateDB transaction.

#term("Serializable snapshot isolation")[
  A transaction guarantee: the transaction sees a consistent snapshot, and if any key it read
  was changed by another committed transaction before it commits, its own commit is rejected.
  It is optimistic, meaning it does not lock rows up front; it detects conflicts at commit
  time and aborts. turbolay opens every write transaction at this level and marks the keys it
  reads so the fence check and counter reads participate in conflict detection.
]

The transaction is opened with `IsolationLevel::SerializableSnapshot`, and committed by one
small helper that threads the durability setting through:

#srcblock("src/codec.rs:195-202")[```rust
pub(crate) async fn commit_txn_strict(txn: DbTransaction, await_durable: bool) -> Result<()> {
    let options = WriteOptions { await_durable, ..Default::default() };
    txn.commit_with_options(&options).await?;
    Ok(())
}
```]

Because a serializable transaction can abort under contention, the public method wraps the
transaction in a bounded retry loop. This is `write_edge` in full, and it is the template
every writer follows:

#srcblock("src/shard/write.rs:2257-2294")[```rust
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
                if is_retryable_write_conflict(&err) && attempt + 1 < GRAPH_TXN_MAX_RETRIES => {
                self.operation_metrics.write_retries.fetch_add(1, Ordering::Relaxed);
                tokio::task::yield_now().await;
            }
            Err(err @ GraphError::StaleShardLease { .. }) => {
                self.operation_metrics.stale_write_rejects.fetch_add(1, Ordering::Relaxed);
                return Err(err);
            }
            Ok(result) => {
                self.operation_metrics.write_commits.fetch_add(1, Ordering::Relaxed);
                return Ok(result);
            }
            result => return result,
        }
    }
    Err(GraphError::RetryExhausted { operation: "graph transaction", attempts: GRAPH_TXN_MAX_RETRIES })
}
```]

Only some errors are worth retrying. `is_retryable_write_conflict` (`state.rs:796`) retries a
SlateDB serialization abort and object-store lock contention, but a `StaleShardLease` is
returned immediately, because retrying a write you are no longer allowed to make is pointless.

== Advancing the epoch

The epoch is the version stamp the whole read path depends on. A write allocates the next
epoch by reading the counter, adding one, and writing it back in the same transaction as the
data, so the epoch and the data it stamps are committed together or not at all:

#srcblock("src/shard/write.rs:2536-2549")[```rust
let epoch = current_epoch
    .checked_add(1)
    .ok_or_else(|| GraphError::CorruptValue {
        key: keys::last_epoch(&mutation.cell_id),
        reason: "epoch overflow".to_string(),
    })?;
let result = CommitResult { epoch, already_existed: existing_edge_epoch.is_some() };
txn.put(keys::last_epoch(&mutation.cell_id).as_bytes(), encode_u64(epoch))?;
```]

Two details matter. First, if the edge already exists and nothing changed, the write commits
without bumping the epoch, so a redundant write does not inflate the version. Second, a batch
allocates a contiguous run of epochs: it reads the counter once, gives each newly inserted
edge the next epoch, and writes the counter once at the end only if it moved
(`write.rs:4289`). Relationships additionally allocate from a second counter,
`cell/<cell_id>/meta/last_relationship_id` (`keys.rs:35`), in the same transaction.

Counters are stored as big-endian `u64` (`encode_u64`, `codec.rs:260`), and an absent counter
reads as zero, so a fresh cell starts at epoch 0 and its first write commits at epoch 1.

== What keys a single edge write touches

Here is the payoff, the write-side counterpart to the read chapter's three-layer merge. When
`write_edge` inserts a new edge it writes this exact set of keys in one transaction
(`write.rs:2596-2649`):

#srcblock("src/shard/write.rs:2608-2649 (abridged)")[```rust
put_scoped_delta_indexes_txn(&txn, &delta)?;                       // owner + pair delta indexes
// out adjacency
txn.put(keys::out_edge(cell, et, src, dst).as_bytes(), &edge_value)?;
if self.writes_reverse_index() {                                  // in adjacency
    txn.put(keys::in_edge(cell, et, dst, src).as_bytes(), &edge_value)?;
}
txn.put(out_degree_key.as_bytes(), encode_u64(out_degree))?;      // degree counters
if let Some((in_degree_key, in_degree)) = in_degree {
    txn.put(in_degree_key.as_bytes(), encode_u64(in_degree))?;
}
txn.put(keys::outbox(cell, epoch, DeltaKind::Plus, et, src, dst).as_bytes(), &delta_value)?;
txn.put(idem_key.as_bytes(), encode_commit_idempotency(mutation, &result))?;
commit_txn_strict(txn, self.await_durable_writes).await?;
```]

Laid out, one logical edge fans out into several physical keys, each serving a different read:

#table(
  columns: (auto, auto, 1fr),
  inset: 5pt,
  align: (left + top, left + top, left + top),
  stroke: 0.4pt + rgb("#d0d7de"),
  [*Key builder*], [*Shape*], [*Read it serves*],
  [`out_edge` (`keys.rs:55`)], [`.../e/out/{et}/{src}/{dst}`], [scan a vertex's outgoing neighbors],
  [`in_edge` (`keys.rs:59`)], [`.../e/in/{et}/{dst}/{src}`], [scan a vertex's incoming neighbors],
  [`degree_out` / `degree_in`], [`.../cnt/out/{et}/{src}`], [read a vertex's degree without scanning],
  [`outbox` (`keys.rs:254`)], [`.../outbox/{epoch}/{kind}/{et}/{src}/{dst}`], [the delta stream the read merge replays],
  [`owner_delta` (`keys.rs:611`)], [`.../delta_owner/{kind}/{et}/{dir}/{owner}/{epoch}/{nbr}`], [find one vertex's recent changes fast],
  [`pair_delta` (`keys.rs:647`)], [`.../delta_pair/{kind}/{et}/{src}/{dst}/{epoch}`], [find changes to one specific edge],
  [`idempotency` (`keys.rs:47`)], [`.../idem/{op}/{key}`], [detect and short-circuit a retried write],
  [`last_epoch` (`keys.rs:31`)], [`.../meta/last_epoch`], [the version stamp for the whole cell],
)

#figure(
  diagram(
    node-stroke: 0.55pt,
    spacing: (0.7cm, 0.85cm),
    node((1.5, 0), [one `write_edge` transaction], fill: rgb("#fff8e6"), width: 5cm),
    node((0, 1), [`e/out`, `e/in`\ adjacency], fill: rgb("#eef4ff"), width: 3cm),
    node((1, 1), [`cnt/out`, `cnt/in`\ degree], fill: rgb("#eef4ff"), width: 3cm),
    node((2, 1), [`outbox`\ delta], fill: rgb("#eef4ff"), width: 3cm),
    node((3, 1), [`delta_owner`,\ `delta_pair`], fill: rgb("#eef4ff"), width: 3cm),
    node((0.5, 2), [`idem`\ idempotency], fill: rgb("#e9fce9"), width: 3cm),
    node((2.5, 2), [`last_epoch`\ counter], fill: rgb("#e9fce9"), width: 3cm),
    edge((1.5, 0), (0, 1), "->"),
    edge((1.5, 0), (1, 1), "->"),
    edge((1.5, 0), (2, 1), "->"),
    edge((1.5, 0), (3, 1), "->"),
    edge((1.5, 0), (0.5, 2), "->"),
    edge((1.5, 0), (2.5, 2), "->"),
  ),
  caption: none,
)
#figcap[One edge, many keys, one transaction. All of these commit atomically, so a reader never sees an adjacency entry without its matching delta or degree count.]

A relationship create adds the `rel/...` record, a `rel_id/...` pointer, a `rel_count/...`
counter, and a metadata delta on top of the structural edge keys. A delete does the mirror
image: it removes the adjacency and canonical keys, decrements the degree counters, and writes
an `outbox` record with `DeltaKind::Minus` so the read merge removes the edge at that epoch.
For an edge that lives only in a compacted segment rather than as an `e/out` row, the delete
instead writes a segment tombstone. The delete chapter takes that apart.

== Append then materialize

Writing all those keys per edge is fine for interactive writes but expensive for high-volume
ingestion. turbolay offers a cheaper path that splits a write into two phases.

#term("Append-then-materialize")[
  A two-phase write. Phase one appends the raw change to a log cheaply, touching almost no
  index keys. Phase two, running in the background, reads the log and folds the changes into
  the real adjacency, degree, and delta keys in batches. It trades a short delay before the
  write is queryable for much higher write throughput.
]

Phase one is `append_edge_mutation_log` (`write.rs:3778`). It bumps a separate log counter,
`cell/<cell_id>/meta/mutation_log_epoch` (`keys.rs:39`), stores the raw batch under a
`mutation_log/...` key, and writes nothing else:

#srcblock("src/shard/write.rs:3903-3911 (abridged)")[```rust
txn.put(keys::mutation_log_entry(cell_id, log_epoch, batch_id).as_bytes(),
        encode_edge_mutation_log_batch(&batch))?;
txn.put(keys::mutation_log_epoch(cell_id).as_bytes(), encode_u64(log_epoch))?;
txn.put(idem_key.as_bytes(), encode_mutation_log_append_idempotency(/* ... */))?;
```]

Phase two is `materialize_edge_mutation_log` (`write.rs:3919`). It scans the log forward from a
materialized watermark, `cell/<cell_id>/meta/mutation_log_materialized_epoch` (`keys.rs:43`),
gathers up to `GRAPH_MUTATION_LOG_MATERIALIZE_TXN_EDGES` (512, `lib.rs:310`) edges into a
microbatch, and runs them through the normal batch writer. Crucially, it advances the
materialized watermark in the same transaction as the edges it materializes, so the log can
never be double-applied.

That produces the `outbox` deltas. A further background job compacts deltas into the larger
structures the read path prefers. `build_matrix_tiles` (`artifact_build.rs:419`) validates the
fence, reads the current graph with `edges_at` (the same merge the read path uses), builds the
tiled matrix and a GraphBLAS matrix, buffers them into a `GraphWriteBatch`, and publishes the
manifest under a cell lock. The result is a matrix artifact at a base epoch, exactly the
canonical base layer the read chapter started from.

#figure(
  diagram(
    node-stroke: 0.6pt,
    node-fill: rgb("#eef4ff"),
    spacing: (0pt, 0.62cm),
    node((0, 0), [`append_edge_mutation_log`: raw batch + log counter only], width: 12cm),
    edge((0, 0), (0, 1), "->", [background]),
    node((0, 1), [`materialize_edge_mutation_log`: fold into `e/out`, `cnt`, `outbox` deltas], width: 12cm),
    edge((0, 1), (0, 2), "->", [background]),
    node((0, 2), [`build_matrix_tiles`: compact deltas + segments into a matrix artifact], fill: rgb("#e9fce9"), width: 12cm),
    edge((0, 2), (0, 3), "->"),
    node((0, 3), [read path: artifact base + delta overlay at the read epoch], fill: rgb("#fff8e6"), width: 12cm),
  ),
  caption: none,
)
#figcap[The write-to-read pipeline. Each stage is cheaper to run often and produces the input the next stage compacts, ending in the artifacts the read merge starts from.]

== Write lanes and concurrency

Two mechanisms from Chapter 1 keep writes from colliding inside one process.

The 64 write lanes are per-cell mutexes. A write picks its lane by hashing the cell id, so all
writes to one cell take the same lane and serialize, while writes to different cells run in
parallel:

#srcblock("src/codec.rs:1642-1649")[```rust
pub(crate) fn writer_lane_index(cell_id: &str) -> usize {
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;      // FNV-1a offset basis
    for byte in cell_id.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    (hash as usize) % GRAPH_WRITE_LANES
}
```]

#why[
  Serializing writes to the same cell in memory, before they reach the transaction, is what
  keeps SlateDB serialization aborts rare. Two writes to the same cell would almost certainly
  conflict at commit, so the lane makes them queue cheaply on a mutex instead of both doing the
  work and having one abort and retry. Writes to different cells never share a lane, so
  unrelated tenants do not slow each other down.
]

Above the lanes sits `graph_write_gate`, a semaphore that bounds the total number of writes in
flight across all cells (`acquire_graph_write_permit`, `lifecycle.rs:527`). The order is always
permit first, then lane lock, so a flood of writes waits on the global gate rather than piling
up holding lane mutexes.

#term("Backpressure")[
  Slowing down producers when the system is saturated, rather than accepting unbounded work and
  falling over. turbolay's write gate is backpressure: when the permit limit is reached, new
  writes wait, and the wait is counted as a metric so operators can see the system is at its
  write ceiling.
]

== Durability and idempotency

Two properties make writes safe to retry and safe to trust.

*Durability.* The `await_durable_writes` flag (set from the open options) is threaded into
every commit through `commit_txn_strict`. When it is on, the commit does not return until the
data has reached the object store durably. Reads inside a write transaction use the `Remote`
durability filter, so a writer reads its own already-durable state, never unflushed data.

*Idempotency.* Every mutation carries an idempotency key, and the transaction checks for a
prior result under `cell/<cell_id>/idem/<operation>/<key>` as its first read after the fence.
If it is present, the write returns the stored result without re-applying:

#term("Idempotency key")[
  A caller-supplied identifier for a logical write. turbolay records, per key, that the write
  already happened and what it returned. A retry with the same key is detected and returns the
  original result instead of applying the change twice. This is what makes the retry loop and
  at-least-once delivery from `cortex-ingestion` safe: the same edge can be submitted many
  times and land once.
]

The idempotency record is written in the same transaction as the data, so it is impossible to
apply the change without also recording that it happened. Different write paths use different
operation namespaces (`create`, `delete`, `relationship-create`, `mutation-log`,
`segment-import`), and a batch additionally rejects duplicate keys within itself before it
starts.

== Recap: the birth of an epoch

Follow one `MERGE` edge through the whole chapter:

+ `write_edge` validates, takes a global write permit, and locks the cell's write lane.
+ It retries, up to 32 times, a transaction that first takes the object-store lock.
+ Inside the transaction it checks the write fence, aborting if another node has taken over.
+ It checks the idempotency record and returns early if this write already happened.
+ It reads `last_epoch`, adds one, and writes the new epoch, the adjacency keys, the degree
  counters, the `outbox` delta, and the scoped delta indexes.
+ It commits atomically, waiting for object-store durability, and releases the lock.

The `outbox` delta it wrote is exactly what the read path replays over a matrix artifact, and
the epoch it allocated is exactly what a reader pins. Write and read meet at the delta stream
and the epoch counter. The next chapter follows the other kind of change, a delete, and the
background garbage collection that eventually removes the tombstones and old deltas this
chapter created.
