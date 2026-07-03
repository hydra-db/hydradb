---
title: "RFC 0004: Graph Data Model, Write Path & UID Allocation"
status: draft
date: 2026-07-03T00:00:00Z
related:
  - 0000-rfc-index.md
  - 0001-strong-consistency-model.md
  - 0003-keyspace-and-encoding.md
  - 0005-posting-list-substrate.md
  - 0006-index-framework.md
---

# RFC 0004: Graph Data Model, Write Path & UID Allocation

## Summary

This RFC decides turbolay's **property-graph data model**, how a write becomes an atomic set of KV mutations, how internal UIDs are allocated crash-safely, and the logical-sequence protocol that backs session-token consistency (the D4 amendment to RFC 0001).

The shape is Dgraph's mutation path (`worker/mutation.go`, `posting/index.go`) with the distributed half removed: no `LocalCache` staging for a distributed commit, no Zero-assigned `commit_ts`, no delta-then-rollup-on-timestamp. One writer per namespace applies each write as **one atomic `WriteBatch`** and moves on.

## Data model

A **property graph**:

- **Node**: an internal `u64` UID, a set of **labels** (interned `label_id`s), and **properties** (a map `prop_id → TypedValue`). Every node also has an external id (`xid`, an arbitrary user string) used to address it.
- **Edge**: a typed directed relationship `(src_uid) --pred--> (dst_uid)`, optionally carrying **edge properties** (facets). `pred` is an interned `pred_id`.
- **TypedValue**: `Null | Bool | Int(i64) | Float(f64) | String | Bytes | DateTime` (the value types the index tokenizers understand, RFC 0006). Arrays are modeled as `list` predicates (multi-value), per Dgraph.

### Node record — monolithic blob (v0)

```
Node[uid] -> NodeRecord {
  labels: Vec<u32>,             // sorted label_ids
  props:  Map<u32, TypedValue>, // prop_id -> value
  xid:    String,               // external id (also indexed via Xid[xid] -> uid)
}
```

v0 stores the whole node as one value (**monolithic**): one `get` returns all properties, updates are a whole-record rewrite, and there is cross-property atomicity for free. The fundamentals-of-graph trade-off table calls this the right default for read-heavy workloads; the **wide-column split** (`Node[uid][prop_id] → value`, cheap partial updates, per-property scans) is the write-heavy alternative and is a deferred option, not v0. The node-size cap (below) bounds the rewrite cost.

**Serialization** sits behind a `NodeCodec` trait. v0 default = **bincode** (the opendata house codec, little-endian values, already proven across `common`); **rkyv** (zero-copy archived reads) is a documented fast-follow spike, adopted only if it pays for itself on the read path — same posture as the sister FTS project, resolved toward the pragmatic reuse default (D8). The codec is behind the trait either way, so the choice is not load-bearing.

## UID allocation (D5)

Internal ids are dense `u64`s so roaring compression and cheap set math work (RFC 0005) — the namidb retro is explicit that public UUID node-ids destroy offset/delta math. We allocate them crash-safely with `common::SequenceAllocator`:

- The allocator hands out monotonic ids from pre-reserved `SeqBlock`s (default block 4096) to amortize the durability write. When a block is exhausted it returns a new `SeqBlock` record the writer persists **in the same `WriteBatch`** as the data using those ids.
- On restart, `load` resumes at `base + block_size` — monotonic across crashes even with unused ids in the abandoned block. No id is ever reused (reuse would resurrect a tombstoned node's edges).
- Separate id-spaces: node/edge UIDs, and the schema id-spaces (`label_id`, `pred_id`, `prop_id`) from RFC 0003. Each is an independent allocator.

### xid → uid resolution

Users write and query by `xid`. Resolution on the write path:

```
fn resolve_or_create(xid) -> uid:
    if let Some(uid) = get(Xid[xid]) { return uid }      // existing
    let uid = uid_alloc.next()                            // new
    batch.put(Xid[xid], uid)                              // mapping, same batch
    batch.put(Node[uid], NodeRecord{ xid, .. })
    uid
```

Single-writer serialization makes this lookup-then-allocate race-free — no `INSERT ... ON CONFLICT`, no oracle. `uid → xid` for result projection is read straight from the `NodeRecord`.

## Write path

A write request is a batch of **operations**: `UpsertNode`, `UpsertEdge`, `DeleteNode`, `DeleteEdge`. The writer lowers the whole request into **one atomic `WriteBatch`** committed via `Storage::apply` / `db.write` (all-or-nothing at one SlateDB seq), then acks with the session token.

### UpsertNode(xid, labels, props)

1. `uid = resolve_or_create(xid)` (may add `Xid[xid]` + `Node[uid]` to the batch).
2. Intern any new `label_id`/`prop_id`; add `SchemaName`/`SchemaId` records to the batch (RFC 0003).
3. Read the prior `NodeRecord` (if any); compute the merged record; `batch.put(Node[uid], encode(record))`.
4. **Index maintenance** (RFC 0006): for each indexed property whose value changed, delete old-value tokens and add new-value tokens (`batch` puts/merges on `Index[pred_id][token]` posting lists). This is Dgraph's `addIndexMutations` (delete old, add new).
5. Append the changelog record and bump `m/latest_seq` (below).

### UpsertEdge(src_xid, pred, dst_xid, edge_props?)

1. Resolve `src_uid`, `dst_uid` (each may create a node).
2. Intern `pred_id`.
3. **Out projection**: add `dst_uid` to `EdgeOut[src_uid][pred_id]`'s posting list.
4. **In projection**: add `src_uid` to `EdgeIn[dst_uid][pred_id]`'s posting list. (Bidirectional storage is unconditional — D10.)
5. Edge properties, if any, go in the posting's side-array keyed by `dst_uid` (out) / `src_uid` (in) — RFC 0005.
6. **Count index** (RFC 0006): if the predicate is `count`-directed, move the anchor between degree buckets (`Count[pred_id][old_deg] −= src`, `Count[pred_id][new_deg] += src`).
7. Changelog + `m/latest_seq`.

Steps 3–4 are where "add to a posting list" happens; RFC 0005 decides whether an add is an associative **merge-append** (fast path, resolved into the pack lazily) or a single-writer **RMW** (for lists needing dedup/split). Both land in the same batch.

### DeleteNode(xid)

v0 uses **tombstone-and-filter**, not eager cascade:

1. `uid = get(Xid[xid])`; if absent, no-op.
2. `batch.merge(Meta["deleted_nodes"], roaring_singleton(uid))` — union into the deleted-node bitmap.
3. Changelog + `m/latest_seq`.

At read time, any `uid ∈ deleted_nodes` is filtered from every posting-list result **and** from node lookups, so incident edges in either direction vanish without touching their posting lists (the anchor or the neighbor is filtered). Physical purge of the node record, its `Xid`, its index tokens, and its incident edges is deferred to **vacuum (RFC 0012)**. This keeps a delete O(1) regardless of degree — critical for supernodes — at the cost of a deleted-bitmap subtraction on reads (RFC 0005) and eventual vacuum. `DETACH DELETE`-style eager edge removal is available but not the default.

### DeleteEdge(src_xid, pred, dst_xid)

1. Resolve uids and `pred_id`.
2. `batch.merge(Meta["deleted_edges"/pred_id/src_uid], roaring_singleton(dst_uid))` (out) and the symmetric in-projection tombstone.
3. Count-index decrement if applicable.
4. Changelog + `m/latest_seq`.

Reads subtract the per-`(anchor,pred)` deleted-edge bitmap from the posting list; rollup (RFC 0005) folds the tombstones in and physically removes them via RMW. This mirrors Dgraph's `Del` posting + rollup, minus the timestamps.

## Logical sequence protocol (amends RFC 0001)

turbolay's session token is **its own logical seq**, not SlateDB's internal seqnum (RFC 0002 consequence (a)). The writer owns a monotonic `next_seq`:

- `Meta["latest_seq"] -> u64` holds the highest committed logical seq.
- Every write batch appends `Log[seq] -> ChangeRecord` and `batch.put(Meta["latest_seq"], seq)`, and commits with `WriteOptions { await_durable: true, seqnum: seq }` — injecting our seq as SlateDB's seq so the two never diverge (SlateDB requires the injected seqnum to strictly increase, which single-writer monotonicity guarantees).
- **Recovery on open**: read `Meta["latest_seq"]` and each `SequenceAllocator` block record; resume `next_seq` and the uid/schema allocators from there. A batch that was mid-flight when the writer died either committed atomically (SlateDB WAL) or did not — there is no partial batch.

The reader freshness gate (`durable_seq >= token`) and the index-watermark + changelog-tail read plan are RFC 0001; this RFC only fixes where `latest_seq` and the changelog live.

### ChangeRecord schema

The changelog must carry enough to (a) let a lagging read re-evaluate a pattern on tail entries and (b) let an index backfill/rebuild replay deterministically:

```
Log[seq] -> ChangeRecord {
  seq: u64,
  op:  UpsertNode | UpsertEdge | DeleteNode | DeleteEdge,
  subject_uid: u64,
  pred_id: Option<u32>,
  object_uid: Option<u64>,        // edge target
  value: Option<TypedValue>,      // scalar property, before/after captured by op
  label_delta: Option<...>,       // labels added/removed
}
```

The tail scan (RFC 0007) reads `Log[(W, latest]]`, reconstructs the affected nodes/edges, and evaluates the query pattern directly on them — so an index that is only current to `W` never hides a recent write.

## Node size cap

v0 caps a node's encoded `NodeRecord` at a configurable limit (default **1 MiB**) and **rejects** oversize upserts with `oversize_node` (RFC 0008 error taxonomy). This exists because SlateDB has no key-value separation (RFC 0002 constraint): a large value is rewritten whole by compaction and by every property update. Spill-to-raw-S3-objects is backlog RFC 0014, triggered only if the cap proves wrong on real workloads. (Note this is the *node* cap; a supernode's *adjacency* is bounded instead by the 512 KiB posting-list split — RFC 0005.)

## What we drop from Dgraph

| Dgraph mechanism | turbolay | why |
|---|---|---|
| `LocalCache` per-txn staging + distributed commit | one atomic `WriteBatch` | single writer; no cross-node commit |
| Zero-assigned `start_ts`/`commit_ts` | own `next_seq` (`m/latest_seq`) | no oracle (D2) |
| Conflict-key OCC (`GetConflictKey`, abort-on-overlap) | nothing | one writer never conflicts with itself |
| `BitDeltaPosting` written, `IncrRollup` folds on a timer | direct RMW / merge-append; rollup = compaction | no MVCC versions to collapse |
| `WithNumVersionsToKeep(MaxInt32)` version-in-key | single logical version per key | reads don't merge version stacks |

The *upper* layers Dgraph built on Badger — the triple→KV fan-out, the out/in/index/count projections, the posting model — port directly; only the timestamp/version/consensus plumbing is deleted.

## Acceptance

1. **Crash-recovery**: kill the writer mid-batch (fault-inject via `common::FailingStorage::fail_apply`), reopen, assert: no partial batch, `next_seq`/uid-counter/schema resume correctly, out/in projections symmetric, no changelog gap.
2. **Zombie-writer fencing**: open a second writer on the same prefix; assert the first fails with `CloseReason::Fenced` and the second continues the seq lineage (RFC 0001).
3. **Read-your-writes across a reader**: upsert edge, capture token, query a stale `DbReader` with the token, assert it waits/replays and returns the edge (RFC 0001 test 1).
4. **Delete correctness**: delete a node/edge, query with the returned token, assert the tombstoned uid is filtered in both directions and the count index reflects it.
5. **xid stability**: re-upsert an existing `xid`, assert the same `uid` and no duplicate `Xid`/`Node` records; assert monotonic uid allocation survives a restart with a half-used block.
6. **Atomic fan-out**: assert an `UpsertEdge` that also creates both endpoints commits node records, xid mappings, both projections, count updates, changelog, and `latest_seq` in exactly one SlateDB seq (no intermediate visible state).

## Alternatives considered

- **Wide-column node split** (`Node[uid][prop_id]`). Cheaper partial updates and per-property scans, higher key count and no cross-property atomicity. Deferred; monolithic blob is simpler and fits read-heavy RAG-KG (D12).
- **Eager cascade delete** (remove all incident edges on node delete). Correct but O(degree) — a supernode delete becomes a huge batch. Rejected as default in favor of tombstone-and-filter + vacuum; offered as explicit `DETACH DELETE`.
- **Add-via-merge only vs RMW only** for posting maintenance. Deferred to RFC 0005, which splits the decision by list size (merge-append fast path, RMW for split/dedup).
- **rkyv as the v0 node codec.** Zero-copy reads are attractive, but bincode is already proven in `common` and unblocks M1 immediately; rkyv is a measured fast-follow behind `NodeCodec`.

## Final contract

- Property-graph model: nodes (labels + property blob + xid), typed directed edges (+ facets); UIDs are dense `u64` from `SequenceAllocator`; users address by `xid` via the `Xid → uid` index.
- Every write is one atomic `WriteBatch`: node/edge records + both projections + affected indexes + count + changelog + `m/latest_seq`, committed with `seqnum = latest_seq`.
- Deletes are tombstone-and-filter (O(1), degree-independent); physical purge is vacuum (RFC 0012).
- The logical-seq protocol (`m/latest_seq`, injected seqnum, recovery scan) is the authoritative session token — amends RFC 0001.
- Node size capped (reject oversize); Dgraph's oracle/OCC/MVCC-rollup machinery is dropped, its triple→KV projection model is kept.
