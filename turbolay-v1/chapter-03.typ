#import "../book/vendor/bookly/src/bookly.typ": *
#import "../book/template.typ": term, why, srcblock, figcap, accent, muted

= One Cell, One Write Boundary

Suppose a client asks turbolay to insert one edge:

```text
1 -[FOLLOWS]-> 2
```

The request sounds indivisible. Either vertex 1 follows vertex 2 or it does
not. The storage work is not indivisible by default. The engine may need to
write outgoing and incoming access paths, update degree counters, record a
versioned delta, advance the cell epoch, and remember the request's
idempotency result.

If only some of those records become durable, the graph can disagree with
itself. A neighbor scan may find the edge while the degree says zero. A retry
may create a second effect. A snapshot may see the edge without the epoch that
explains when it appeared.

Chapter 2 established who may write a cell. This chapter asks the next
question: once the correct writer has been identified, how does one logical
change become one durable fact?

The answer is the cell-local write boundary.

== Problem 1: one graph fact becomes many storage records

A graph exposes several ways to ask about the same relationship:

- Does edge `1 -> 2` exist?
- Which vertices does vertex 1 follow?
- Which vertices follow vertex 2?
- What is the out-degree of vertex 1?
- Which changes occurred after an older artifact was built?

Serving each question efficiently requires different physical records. The
outgoing edge key supports a scan by source. The reverse key supports a scan
by destination. Counters avoid scanning merely to compute a degree. Delta
records let a snapshot advance an older artifact to a newer epoch.

These records are different access paths to one logical fact. They must not
be allowed to acquire separate meanings.

#term("Logical mutation")[
  One requested change to graph meaning, such as inserting an edge, deleting
  an edge, creating a relationship, or changing metadata. A logical mutation
  may fan out into several physical key-value changes.
]

#term("Write boundary")[
  The set of changes that become visible together or do not become visible at
  all. In turbolay the implemented atomic boundary is one SlateDB transaction
  within one cell.
]

For a new edge, the physical fan-out has this shape:

#figure(
  table(
    columns: (1.25fr, 1.25fr, 1.6fr),
    inset: 8pt,
    align: (left + top, left + top, left + top),
    table.header([*Record*], [*Purpose*], [*Failure if published alone*]),
    [Outgoing adjacency], [Find neighbors from the source], [The edge exists in one direction only],
    [Incoming adjacency], [Find neighbors from the destination], [Reverse traversal disagrees with forward traversal],
    [Degree counters], [Answer degree without a scan], [Counts disagree with adjacency],
    [Outbox and delta indexes], [Reconstruct later snapshots], [Artifact overlays miss the change],
    [Last epoch], [Name the new cell version], [The change has no coherent visibility point],
    [Idempotency result], [Recognize a retried request], [A retry may apply work again],
  ),
  caption: [One edge has several representations, but only one meaning.],
)

The transaction is what turns that list back into one fact:

#boxeq[
  *One logical mutation becomes one durable SlateDB transaction at one new
  cell epoch.*
]

== Problem 2: reaching the transaction is not permission to commit

Chapter 2 separated placement, lease, fence, and lock. The write path now puts
those mechanisms in a deliberate order. A request becomes more expensive only
as it proves that it is allowed to continue.

#figure(
  table(
    columns: (0.45fr, 1.35fr, 1.7fr),
    inset: 8pt,
    align: (center, left + top, left + top),
    table.header([*Order*], [*Guard*], [*What it prevents*]),
    [1], [Local write authority], [A read-only shard or expired local lease starts a write],
    [2], [Write permit], [Unbounded mutation work exhausts the node],
    [3], [Writer lane], [Tasks in one process race on the same cell],
    [4], [Object-store cell lock], [Processes normally overlap their cell transactions],
    [5], [Transactional fence check], [An obsolete lease holder commits after takeover],
    [6], [Serializable transaction], [The mutation publishes partially or loses a conflict],
  ),
  caption: [The write envelope narrows concurrency and authority before durable state changes.],
)

The public `write_edge` method shows the outer portion of that envelope.
It validates the mutation, checks authority, takes a bounded write permit,
serializes the cell in the local process, and retries only conflicts that may
succeed on another attempt.

#srcblock("src/shard/write.rs:2257-2294 (abridged)")[```rust
pub async fn write_edge(&self, mutation: EdgeMutation) -> Result<CommitResult> {
    validate_component("cell_id", &mutation.cell_id)?;
    validate_component("edge_type", &mutation.edge_type)?;
    validate_component("idempotency_key", &mutation.idempotency_key)?;
    self.ensure_write_authority(&mutation.cell_id, "write_edge")?;

    let _permit = self.acquire_graph_write_permit("write_edge").await?;
    let _writer = self.writer_lane(&mutation.cell_id).lock().await;
    for attempt in 0..GRAPH_TXN_MAX_RETRIES {
        match self.write_edge_txn(&mutation).await {
            Err(err) if is_retryable_write_conflict(&err)
                && attempt + 1 < GRAPH_TXN_MAX_RETRIES =>
            {
                tokio::task::yield_now().await;
            }
            Ok(result) => return Ok(result),
            result => return result,
        }
    }
    Err(GraphError::RetryExhausted { /* ... */ })
}
```]

The current retry limit is 32. A serialization abort or cell-lock conflict is
retryable because the writer may still have authority. A stale lease is not:
waiting does not make an obsolete owner legitimate again.

#why[
  The guards are not six versions of the same lock. Authority rejects the
  wrong actor. Backpressure bounds admitted work. Lanes and the object-store
  lock reduce contention. The fence rejects an old ownership generation. The
  transaction makes the related data changes atomic. Removing one layer
  changes a different property.
]

== Problem 3: “one writer” still needs conflict detection

It is tempting to think that a lease and a lock make transaction isolation
unnecessary. They make conflicts less common, but correctness cannot depend
on their timing being perfect.

A process can pause. A cell lock can expire. Ownership can move while the old
process still has network access. Two operations can also touch shared
metadata such as the cell epoch.

turbolay therefore performs graph writes using a serializable SlateDB
transaction.

#term("Serializable transaction")[
  A transaction that reads a consistent view and detects relevant concurrent
  changes before committing. If another committed transaction invalidates
  what this transaction read, the commit aborts instead of silently combining
  incompatible states.
]

The fence validation happens *inside* this transaction. The leased writer
reads the durable fence and compares its cell, owner, and lease token with the
local lease (`src/shard/lifecycle.rs`). If takeover changes the fence before
the graph transaction commits, conflict detection prevents the old
transaction from quietly publishing behind the new owner.

The commit helper also carries the durability choice into SlateDB:

#srcblock("src/codec.rs:195-202")[```rust
pub(crate) async fn commit_txn_strict(
    txn: DbTransaction,
    await_durable: bool,
) -> Result<()> {
    let options = WriteOptions {
        await_durable,
        ..Default::default()
    };
    txn.commit_with_options(&options).await?;
    Ok(())
}
```]

Graph writer opens reject relaxed durability. The reason is architectural:
the process must not release cross-process coordination while a newer epoch,
degree counter, or idempotency record is still only locally visible
(`src/shard/lifecycle.rs`).

The three cross-process mechanisms now have distinct roles:

#figure(
  table(
    columns: (1.2fr, 1.5fr, 1.2fr),
    inset: 8pt,
    align: (left + top, left + top, left + top),
    table.header([*Mechanism*], [*Question*], [*Failure response*]),
    [Lease], [Should this node still own the cell?], [Reject locally when authority is absent or expired],
    [Cell lock], [Is another process already performing cell write work?], [Wait, reclaim an expired lock, or report contention],
    [Fence in transaction], [Does this commit present the current ownership generation?], [Abort the stale writer],
  ),
  caption: [The lease guides admission, the lock reduces overlap, and the fence protects commit.],
)

== Problem 4: the mutation must publish its own version

A snapshot reader needs to know when an edge became visible. It cannot infer
that reliably from wall-clock time: clocks differ, requests overlap, and a
timestamp does not make related keys atomic.

The writer instead allocates a cell-local epoch inside the same transaction as
the graph data. It reads `last_epoch`, advances it with overflow checking, and
writes the new counter alongside the mutation.

#srcblock("src/shard/write.rs:2536-2549 (abridged)")[```rust
let epoch = current_epoch
    .checked_add(1)
    .ok_or_else(|| GraphError::CorruptValue {
        key: keys::last_epoch(&mutation.cell_id),
        reason: "epoch overflow".to_string(),
    })?;
let result = CommitResult {
    epoch,
    already_existed: existing_edge_epoch.is_some(),
};
txn.put(
    keys::last_epoch(&mutation.cell_id).as_bytes(),
    encode_u64(epoch),
)?;
```]

This ordering creates two guarantees together:

- if the mutation commits, its visibility epoch commits with it;
- if the transaction aborts, neither the mutation nor its proposed epoch is
  published.

The epoch is local to the cell. Epoch 42 in cell A and epoch 42 in cell B do
not name one shared global state. A distributed caller that needs a coherent
multi-cell view must carry an explicit epoch vector or use another protocol;
the write path does not manufacture a global transaction.

#term("Epoch allocation")[
  Advancing the cell's durable `last_epoch` counter and assigning the result to
  a committed logical change. Allocation happens inside the data transaction,
  so the version and the change have the same fate.
]

An edge that already exists may return without advancing the epoch when there
is no new graph state to publish. A batch can allocate a contiguous run of
epochs within one transaction. These are optimizations of the same rule: an
epoch names a durable change, not merely an attempted request.

== Problem 5: retries must not create a second mutation

Object-store writes and network requests have an unavoidable ambiguous case:
the server may commit successfully and the response may be lost. The client
then cannot tell whether it should repeat the request.

Blindly repeating `CREATE` or a counter update can apply the logical operation
twice. Refusing all retries makes transient failures unnecessarily permanent.

turbolay makes the request identity part of the transaction.

#term("Idempotency key")[
  A caller-supplied identity for one logical operation. The transaction stores
  the mutation and its result under that identity. A compatible retry returns
  the recorded result rather than applying the mutation again.
]

`EdgeMutation` makes the requirement visible at the API boundary:

#srcblock("src/core/model.rs:55-71; 275-279")[```rust
pub struct EdgeMutation {
    pub cell_id: String,
    pub edge_type: String,
    pub src: VertexId,
    pub dst: VertexId,
    pub idempotency_key: String,
}

pub struct CommitResult {
    pub epoch: GraphEpoch,
    pub already_existed: bool,
}
```]

Inside the transaction, the writer first checks whether that key already has
a result. If so, it verifies that the recorded request is compatible and
returns the original outcome. If not, it writes the idempotency result in the
same commit as the graph records.

That atomic placement closes both dangerous gaps:

#figure(
  table(
    columns: (1.45fr, 1.45fr),
    inset: 8pt,
    align: (left + top, left + top),
    table.header([*Unsafe split*], [*Atomic outcome*]),
    [Graph change commits, idempotency record does not], [Impossible: both are in one transaction],
    [Idempotency record commits, graph change does not], [Impossible: both are in one transaction],
    [Response is lost after commit], [Retry finds and returns the stored result],
    [Same key is reused for a different mutation], [Reject the incompatible reuse],
  ),
  caption: [Idempotency resolves an ambiguous response without weakening exactly-once logical effect.],
)

Idempotency is scoped and operation-specific. It is not a global deduplication
service, and it cannot rescue a caller that generates a fresh key for every
retry. The caller and kernel share the contract: one logical request keeps one
stable idempotency key.

== Problem 6: every physical representation must cross together

Once authority, epoch allocation, and idempotency have been established, the
transaction can publish the edge representations.

For the common edge insertion, the write includes the forward adjacency, the
optional reverse adjacency, degree counters, an outbox delta, scoped delta
indexes, the epoch counter, and the idempotency result. The implementation is
spread across helpers, but the commit site makes the boundary explicit:

#srcblock("src/shard/write.rs:2596-2651 (abridged)")[```rust
put_scoped_delta_indexes_txn(&txn, &delta)?;

txn.put(keys::out_edge(cell, et, src, dst).as_bytes(), &edge_value)?;
if self.writes_reverse_index() {
    txn.put(keys::in_edge(cell, et, dst, src).as_bytes(), &edge_value)?;
}
txn.put(out_degree_key.as_bytes(), encode_u64(out_degree))?;
if let Some((in_degree_key, in_degree)) = in_degree {
    txn.put(in_degree_key.as_bytes(), encode_u64(in_degree))?;
}
txn.put(keys::outbox(cell, epoch, DeltaKind::Plus, et, src, dst).as_bytes(),
        &delta_value)?;
txn.put(idem_key.as_bytes(), encode_commit_idempotency(mutation, &result))?;

commit_txn_strict(txn, self.await_durable_writes).await?;
```]

Each key supports a later read path:

#figure(
  table(
    columns: (1.25fr, 1.4fr, 1.5fr),
    inset: 8pt,
    align: (left + top, left + top, left + top),
    table.header([*Key family*], [*Example shape*], [*Consumer*]),
    [`out_edge`], [`.../e/out/{type}/{src}/{dst}`], [Outgoing neighbor and edge-existence reads],
    [`in_edge`], [`.../e/in/{type}/{dst}/{src}`], [Incoming neighbor reads],
    [`degree_out`, `degree_in`], [`.../cnt/{dir}/{type}/{vertex}`], [Degree reads],
    [`outbox`], [`.../outbox/{epoch}/{kind}/...`], [Delta replay and maintenance],
    [`delta_owner`, `delta_pair`], [`.../delta_owner/...`, `.../delta_pair/...`], [Scoped overlay lookup],
    [`idempotency`], [`.../idem/{operation}/{key}`], [Request retry],
    [`last_epoch`], [`.../meta/last_epoch`], [Snapshot selection],
  ),
  caption: [Physical duplication serves access paths; the transaction prevents semantic duplication.],
)

The outbox deserves special attention. It is not incidental audit logging. It
is the durable bridge between the new canonical state and readers using an
older artifact. A read at the new epoch can start from an earlier base and
apply this delta to reconstruct the state that the transaction published.

#boxeq[
  *The epoch tells a reader which version exists; the delta tells it how to
  reach that version from an older base.*
]

The next chapter can therefore explain snapshot reads without inventing
history after the fact. The write transaction has already created the history
record and the version that bounds it.

== Problem 7: concurrency should queue before it conflicts

Serializable transactions make conflicts safe, but aborting completed work is
still expensive. Two writes to the same cell will both touch `last_epoch`, so
allowing them to race freely would create predictable serialization failures.

The local writer lane moves that competition earlier. The shard owns 64 mutex
lanes. A stable hash maps a cell ID to a lane, so the same cell always queues
on the same mutex.

#srcblock("src/codec.rs:1642-1649")[```rust
pub(crate) fn writer_lane_index(cell_id: &str) -> usize {
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    for byte in cell_id.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    (hash as usize) % GRAPH_WRITE_LANES
}
```]

The lane is a concurrency optimization, not the durable correctness boundary.
Two different cell IDs can hash to the same lane and wait unnecessarily, but
they do not become one transaction. Two different processes have separate
lane arrays, so the object-store lock and fence are still required.

Above the lanes, the graph write semaphore bounds total write work across the
shard. Its purpose is backpressure: at saturation, new work waits rather than
creating unbounded transactions, buffers, and retries.

#term("Backpressure")[
  A deliberate limit that makes producers wait when the system is saturated.
  It protects latency and memory by bounding admitted work rather than letting
  an overload grow without limit.
]

The resulting concurrency model is layered:

- writes to the same cell queue locally and serialize durably;
- writes to different cells may proceed independently, subject to the global
  write permit and occasional lane-hash collisions;
- writes from different processes coordinate through the object-store lock;
- any stale leased writer that reaches commit is rejected by the fence.

This is why the cell is more than a storage prefix. It determines the epoch
counter, ownership generation, local serialization lane, distributed lock,
transactional fence, and atomicity claim.

== Problem 8: a batch changes throughput, not the boundary

Writing one edge per transaction is easy to reason about but expensive for
bulk ingestion. turbolay therefore provides batch and import paths that
amortize coordination and commit overhead.

A batch does not weaken the rules. It still checks authority, takes the write
permit and cell lock, validates the fence inside the transaction, records
idempotency information, advances cell-local epochs, and commits durably. The
difference is that several edge changes share the envelope.

#figure(
  table(
    columns: (1.2fr, 1.35fr, 1.55fr),
    inset: 8pt,
    align: (left + top, left + top, left + top),
    table.header([*Path*], [*Optimization*], [*Atomicity to remember*]),
    [Single-edge write], [Small, direct mutation], [One edge fan-out in one cell transaction],
    [Atomic edge batch], [Amortize one transaction across several changes], [The accepted batch commits within one cell],
    [Chunked ingestion], [Split a large stream into bounded batches], [Each chunk is a separate transaction],
    [Append then materialize], [Append raw mutation work before expanding indexes], [Append and later materialization are separate durable phases],
  ),
  caption: [Throughput paths preserve cell-local safety but may introduce more than one commit boundary.],
)

The distinction matters when reporting partial progress. A single atomic batch
has one success or failure. A stream divided into ten chunks can commit the
first six before the seventh fails. The caller must use returned progress and
stable idempotency keys to resume safely; “bulk” does not mean “one enormous
transaction.”

The append-then-materialize path has another explicit trade-off. Appending a
mutation log entry can be cheaper than writing every adjacency and index key
immediately, but the change is not queryable through the normal materialized
paths until background work folds it in. That is a throughput and visibility
choice, not a new source of graph truth.

== The complete write model

We can now follow one successful `write_edge` without treating the method as a
black box:

1. Validate the cell, edge type, and idempotency components.
2. Reject the request if this shard lacks current write authority.
3. Acquire bounded node capacity and the cell's local writer lane.
4. Acquire the cell's object-store write lock.
5. Open a serializable transaction and validate the durable fence.
6. Return a compatible recorded result if the idempotency key already exists.
7. Read the current edge state, degree values, and `last_epoch` needed by the
   mutation.
8. Allocate a new epoch only when a new graph state will be published.
9. Write adjacency, counters, delta records, epoch, and idempotency result.
10. Commit durably; on a retryable conflict, repeat within the bounded retry
    policy.

Every step belongs to one of three questions:

#figure(
  table(
    columns: (1.1fr, 1.45fr, 1.5fr),
    inset: 8pt,
    align: (left + top, left + top, left + top),
    table.header([*Question*], [*Mechanisms*], [*Required outcome*]),
    [Who may act?], [Authority, lease, lock, fence], [Only the current cell writer can commit],
    [What changes?], [Serializable transaction and physical key fan-out], [All representations preserve one graph meaning],
    [How is it named and retried?], [Epoch, delta, idempotency result], [Readers and retrying callers recover the same outcome],
  ),
  caption: [A correct write combines authority, atomic meaning, and repeatable identity.],
)

The chapter's central claim can now be stated precisely:

#boxeq[
  *A cell is a write boundary because authority, graph meaning, epoch, delta,
  and retry result cross the durable boundary in one transaction.*
]

The lock alone does not create that property. Neither does the lease, the
epoch, or the transaction in isolation. It emerges from their composition and
stops exactly where the transaction stops: at the cell.

== What this write path guarantees—and what it does not

For the normal materialized write paths, the design guarantees:

- a read-only or stale leased shard cannot legitimately commit a cell write;
- related physical records for one accepted cell-local transaction publish
  atomically;
- the cell epoch and its versioned delta are committed with the graph change;
- a stable idempotency key can return the original result after an ambiguous
  retry;
- predictable same-cell contention queues before reaching SlateDB in the
  common single-process case;
- serialization conflicts are bounded and retried only when retry may help.

It does not promise:

- an atomic mutation spanning two cells;
- a global epoch shared by every cell;
- exactly-once behavior when the caller changes the idempotency key on retry;
- that every bulk stream is one transaction;
- that a mutation-log append is immediately visible through materialized graph
  indexes;
- that local lanes or the object-store lock can replace the transactional
  fence.

These limits keep the guarantee useful. “Atomic writes” without naming the
cell and the chosen ingestion path would be too broad to reason about.

== Revision notes

Use these notes to reconstruct the write path from the outside in.

=== The ideas to remember

- *One fact has several physical representations.* Adjacency, reverse access,
  degree, delta, epoch, and idempotency records serve different reads but must
  preserve one graph meaning.
- *Admission and commit are different checks.* Local authority rejects cheaply;
  the durable fence is checked inside the transaction so takeover can invalidate
  a stale commit.
- *The cell owns the version counter.* A mutation and its new epoch commit
  together. Epochs from different cells do not form a global clock.
- *Idempotency is transactional.* The graph change and its recorded result
  have the same fate, allowing an ambiguous network retry to recover the
  original outcome.
- *Lanes prevent waste; transactions preserve correctness.* The writer lane
  makes predictable conflicts queue early. Serializable conflict detection
  remains the final protection.
- *Bulk paths have explicit commit boundaries.* An atomic batch is one cell
  transaction; chunked ingestion and append-then-materialize contain several
  durable phases.
- *The guarantee stops at the cell.* Multi-cell atomicity needs another
  protocol and is not implied by routed access or shared object storage.

=== The write envelope in one table

#figure(
  table(
    columns: (1.25fr, 1.75fr),
    inset: 7pt,
    align: (left + top, left + top),
    table.header([*Stage*], [*Revision answer*]),
    [Authority], [Does this process hold the right kind of current write authority?],
    [Backpressure], [Is there bounded capacity to admit the work?],
    [Serialization], [Do the local lane and cell lock prevent ordinary overlap?],
    [Fencing], [Does the transaction present the current lease generation?],
    [Identity], [Has this idempotency key already produced a result?],
    [Versioning], [Which new cell epoch will name the change?],
    [Atomic publish], [Do all graph records, the delta, epoch, and retry result commit together?],
  ),
  caption: [The write path is a proof assembled from the outside inward.],
)

=== A quick correctness test

When adding a new mutation path, ask:

1. Does it reject missing or stale write authority before expensive work?
2. Does it validate the durable fence inside its transaction?
3. Are all physical representations of the logical change in that transaction?
4. Does it allocate and publish the epoch with the data it versions?
5. Can a lost response be retried with the same idempotency key?
6. If it batches or stages work, are the actual commit boundaries explicit?
7. Does any claim accidentally extend atomicity across cells?

#boxeq[
  *The writer is correct not when it has put every key, but when no observer can
  distinguish those keys from one authorized, versioned, retryable graph
  change.*
]
