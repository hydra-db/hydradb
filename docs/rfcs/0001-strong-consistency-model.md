---
title: "RFC 0001: Strong Consistency Model"
status: draft
date: 2026-07-03T00:00:00Z
related:
  - ../plan.md
  - 0000-rfc-index.md
  - 0002-substrate-decision-slatedb.md
  - 0004-graph-data-model-and-write-path.md
  - 0006-index-framework.md
  - 0007-opencypher-read-path.md
  - 0008-http-service-and-fleet.md
---

# RFC 0001: Strong Consistency Model

## Summary

turbolay exposes strong read semantics on top of SlateDB/S3 without forcing every graph query to synchronously round-trip to S3.

The whole contract rests on one structural fact: **there is exactly one writer per namespace** (D2). Writes therefore serialize by construction, and a single monotonic logical sequence is authoritative. This is precisely the property Dgraph spends a Zero timestamp oracle, conflict-key OCC, and version-in-key MVCC to *manufacture* across concurrent distributed writers — we get it for free and delete all of that machinery (see [Alternatives](#alternatives-considered)).

Default consistency is **session-token consistency**:

- every durable write returns a monotonically increasing sequence token — turbolay's own logical `seq`, carried in the batch as `m/latest_seq`;
- a reader must not answer a query carrying token `T` until it has replayed durable state through at least `T`;
- this guarantees read-your-writes across a stateless reader fleet, for graph traversals and index lookups alike.

For callers that need globally freshest reads, support **strict mode** as a per-query opt-in, bounded by the manifest poll interval (or a reader reopen) until SlateDB grows a public forced-refresh API — which, per D1, **we will not patch in**.

## Decision

Use **session tokens by default**. Add **strict mode** as an opt-in query flag.

Do not make every query strict by default.

Do not introduce any timestamp oracle, commit-timestamp assignment, conflict detection, or MVCC-in-key versioning. Single-writer-per-namespace makes all of them unnecessary; SlateDB's manifest writer-epoch handles the one remaining hazard (a zombie writer).

## Motivation

turbolay uses S3/object storage as durable state and stateless compute nodes as readers (D9). A reader can lag the writer briefly because it learns about new durable WAL/SST files by polling SlateDB's manifest (`DbReaderOptions.manifest_poll_interval`) and replaying WAL SSTs into local immutable memtables.

We need a consistency contract that gives application-correct behavior while avoiding unnecessary object-store round trips.

Most graph-application flows require this:

> After I upsert a node/edge, I can immediately query it back — including via a traversal or an indexed lookup that touches it.

They usually do not require this on every query:

> Before answering, check S3 for every write from every client globally.

Session tokens satisfy the first requirement with lower steady-state cost. The graph twist is that a write does not just land a KV value — it fans out into node record, `EdgeOut`, `EdgeIn`, affected value/reverse/count indexes, and the changelog, all in one atomic `WriteBatch` (RFC 0004). The token proves the *durable KV state* is replayed through `T`; it deliberately does **not** require every secondary index to have caught up. Preserving correctness across that gap is the crux of this RFC ([Query correctness with index / adjacency lag](#query-correctness-with-index--adjacency-lag)).

## Consistency modes

### 1. No token

Query request:

```json
{
  "cypher": "MATCH (e:Entity {name: 'rust'})<-[:MENTIONS]-(c:Chunk) RETURN c"
}
```

Guarantee:

- bounded staleness;
- reader may be behind the writer by up to approximately `manifest_poll_interval`;
- suitable for public/analytics graph reads, browse pages, recommendation fan-out where a few-hundred-ms lag is acceptable.

### 2. Session token, default strong mode

Write response:

```json
{
  "ok": true,
  "seq": 105
}
```

Query request:

```json
{
  "cypher": "MATCH (e:Entity {name: 'rust'})<-[:MENTIONS]-(c:Chunk) RETURN c",
  "consistency": {
    "session": 105
  }
}
```

Guarantee:

- reader must wait until its replayed durable sequence is `>= 105`;
- the query sees the caller's write — the new node, both edge projections, and any index/adjacency updates recovered via the tail plan — and all earlier writes in the namespace's single lineage;
- steady state costs zero extra S3 reads if the reader is already caught up.

This is the default consistency model for clients that pass the latest token they have observed.

### 3. Strict mode

Query request:

```json
{
  "cypher": "MATCH (e:Entity {name: 'rust'})<-[:MENTIONS]-(c:Chunk) RETURN c",
  "consistency": {
    "strict": true
  }
}
```

Guarantee:

- before serving, the reader advances to the freshest durable state it can obtain from object storage;
- the query sees all writes visible from that refresh;
- higher latency, bounded by `manifest_poll_interval` (or a reader reopen — see [Strict mode implementation note](#strict-mode-implementation-note)).

Strict mode is for admin reads, correctness-sensitive tests, migrations, and cross-namespace reconciliation where global freshness matters more than latency.

## API shape

Write response:

```json
{
  "ok": true,
  "seq": 105
}
```

Query consistency object:

```json
{
  "consistency": {
    "session": 105
  }
}
```

or:

```json
{
  "consistency": {
    "strict": true
  }
}
```

If both are supplied, strict mode wins and must also satisfy the session token (i.e. the reader must be at least as fresh as `T`, and additionally refresh).

Every query response includes the latest sequence observed by the serving node:

```json
{
  "results": [],
  "latest_seq": 112
}
```

Clients keep the max `seq` / `latest_seq` they observe and send it on later queries when they need read-your-writes. This is a purely client-side monotonic counter — no server-side session state, no coordinator (see [Alternatives](#alternatives-considered)).

## SlateDB support

SlateDB provides the primitives needed for session-token consistency. Nothing here requires a fork or an upstream patch (D1).

### Durable writes

turbolay's single writer per namespace assigns the logical sequence itself. It keeps a `next_seq` counter (recovered on open from `m/latest_seq`), stamps `m/latest_seq = next_seq` into every `WriteBatch` alongside the node/edge/index/changelog keys, and commits durably:

```rust
// One atomic batch: node record + EdgeOut + EdgeIn + touched indexes
// + changelog entry + the meta seq. All commit at one SlateDB seqnum.
batch.put(meta_latest_seq_key(), &encode_u64(logical_seq));

let handle = db.write_with_options(batch, &WriteOptions {
    await_durable: true,
    seqnum: Some(logical_seq),   // inject our logical seq as SlateDB's seqnum
    ..Default::default()
}).await?;

let session_token = logical_seq;   // == handle.seqnum()
```

`WriteOptions::default()` has `await_durable: true`, so the future resolves only after the batch is durably persisted through SlateDB's WAL path (on S3, or S3 Express One Zone via a separate `wal_object_store`). Because turbolay is single-writer-per-namespace, assigning strictly increasing `seqnum` values in the writer is safe — there is no second writer to collide with, so SlateDB's injected `seqnum` and our `m/latest_seq` stay in lockstep by construction (D4/D5).

Two representations of the same number, kept equal on purpose:

- `m/latest_seq` — a **durable KV value** in the batch. This is what a reader observes by *replaying* the batch, and it is what the reader-freshness gate and the changelog-tail plan key off. It survives independent of any SlateDB API detail.
- SlateDB `WriteOptions.seqnum` — the same value injected as SlateDB's internal sequence number, so `DbReader`'s `durable_seq` advances in the same units as our tokens. This lets the gate below compare `durable_seq >= T` directly.

### Reader catch-up and the freshness gate

SlateDB `DbReader` is a read-only handle for separate reader processes (D9). It:

- polls the manifest on `DbReaderOptions.manifest_poll_interval`;
- replays newer WAL SSTs into local immutable memtables;
- exposes current progress via `reader.subscribe() -> watch::Receiver<DbStatus>`, where `DbStatus.durable_seq` advances as flushed WAL is polled and replayed.

turbolay gates queries on that watch channel:

```rust
async fn wait_for_session(
    reader: &DbReader,
    token: u64,
    deadline: Instant,
) -> Result<(), ReaderBehind> {
    let mut rx = reader.subscribe();          // watch::Receiver<DbStatus>

    loop {
        let current = rx.borrow().durable_seq;
        if current >= token {
            return Ok(());
        }
        // Wait for the next manifest poll / WAL replay to advance durable_seq,
        // or give up if we cannot catch up in time.
        tokio::select! {
            r = rx.changed() => { r.map_err(|_| ReaderBehind::closed())?; }
            _ = tokio::time::sleep_until(deadline) => {
                return Err(ReaderBehind {
                    required_seq: token,
                    current_seq: rx.borrow().durable_seq,
                });
            }
        }
    }
}
```

Query execution then becomes:

```rust
if let Some(token) = request.consistency.session {
    wait_for_session(&reader, token, Instant::now() + gate_timeout).await?;
}
// From here, the reader's replayed durable KV state is guaranteed >= token.
execute_graph_query(&reader, request).await?;
```

The gate is the *only* place a session-token query can block, and it blocks on nothing but the reader's own WAL replay progress — no S3 read is issued by the gate itself.

## Query correctness with index / adjacency lag

This is the graph-specific crux, and the one place the token's guarantee must be handled with care.

The session token proves the reader has **replayed durable KV state through sequence `T`**. Concretely, that means the base records committed by every batch up to `T` are present: node records, `EdgeOut` posting lists, `EdgeIn` posting lists, the changelog entries, and `m/latest_seq`. It does **not** prove that every *secondary index* (value/eq, reverse, count — RFC 0006) has been rebuilt through `T`. Index maintenance advances a per-index watermark `m/wm/{id}`, and that watermark can trail `latest_seq` — either because index build ticks lag, or because an index is mid-backfill.

turbolay preserves correctness with the same index-watermark + changelog-tail plan the read path uses everywhere (RFC 0007, referencing the watermarks defined in RFC 0006):

1. Resolve the query plan; note which indexes / adjacency projections it will use.
2. Read those indexes / posting lists **up to** their watermarks. Let `W = min(watermark over used indexes)`.
3. Scan the **changelog tail** for entries with `W < seq <= latest`, where `latest = m/latest_seq` in the reader's replayed state (`>= T`, guaranteed by the gate).
4. **Materialize** the nodes/edges named by those tail entries directly from the base KV records (which the token *does* guarantee are present) and **evaluate the full graph pattern** on them — the same predicate/traversal logic the executor runs, just fed from tail-materialized nodes and edges instead of from an index.
5. Merge:

   ```
   result  =  ( index / adjacency candidates  −  deleted-UID bitmap )  ∪  tail matches
   ```

   The deleted-UID bitmap (a roaring bitmap maintained by associative merge, D11) removes UIDs that an index still lists but that a later tail entry deleted or superseded; the tail matches add UIDs whose index entry hasn't been written yet.

Because step 4 reads only base records — which the token guarantees — index lag is **invisible** to callers. A session-token query never returns a stale traversal result, even when every secondary index is behind.

### Worked example: a reverse-edge index lagging behind an edge write

Suppose the RAG graph gets a new mention edge, and a query traverses it in reverse before the reverse-adjacency maintenance has caught up.

State at the reader after the freshness gate passes:

```text
reader durable_seq (m/latest_seq)  = 200      # gate guaranteed >= query token 200
EdgeIn (reverse) watermark m/wm/rev = 197
value-index watermark m/wm/name     = 200
query session token                 = 200
```

The write at `seq = 199` created edge `(Chunk#42) -[:MENTIONS]-> (Entity#7)`. Its `EdgeOut` and `EdgeIn` **base** posting lists were committed in that batch and are replayed (durable_seq is 200). But the reverse-*index* maintenance watermark is only at 197, so a naive reverse lookup keyed off the index would miss it.

Query: `MATCH (e:Entity {name:'rust'})<-[:MENTIONS]-(c:Chunk) RETURN c`, with `Entity#7` being the `name:'rust'` node.

- `W = min(197 for the reverse projection, 200 for the value index) = 197`.
- Anchor `Entity#7` via the value index (current to 200 — fine).
- Reverse hop uses the `EdgeIn` projection up to `W = 197`; that view does **not** yet include the `seq 199` edge.
- Tail scan: changelog entries `198, 199, 200`. Entry `199` is `AddEdge{src: Chunk#42, pred: MENTIONS, dst: Entity#7}`.
- Materialize: read `Chunk#42` and the `(Chunk#42, MENTIONS)` `EdgeOut` base record (both present — replayed through 200), confirm the edge targets `Entity#7`, evaluate the reverse pattern directly → `Chunk#42` is a match.
- Merge: index-derived reverse candidates ∪ `{Chunk#42}`, minus any deleted UIDs. `Chunk#42` is returned.

The tail scan recovers the exact edge the reverse index hasn't indexed yet. The same mechanism covers a **value index** lagging an updated property (e.g. an `Entity.name` changed at `seq 199` but the `name` token index only current to 197): the tail entry carries before/after values, so materialization re-evaluates the `{name: 'rust'}` predicate on the live node record and both adds newly-matching and removes newly-non-matching UIDs.

## Strict mode implementation note

Session-token mode is supported cleanly by current SlateDB `DbReader` APIs (`subscribe()` / `durable_seq`) with no patch (D1).

Strict per-query mode wants a *forced* refresh — advance the reader to the freshest durable state *now*, not at the next poll. SlateDB v0.14.1 does not expose a public `DbReader::refresh()` / `refresh_manifest()`. Per D1 we will **not** fork or patch SlateDB to add one. That leaves three application-level options, in preference order:

1. **Short `manifest_poll_interval` on strict-serving readers.** Bounds staleness tightly; not a true forced refresh, but requires no SlateDB change. Strict mode's guarantee is stated as "bounded by the poll interval" for exactly this reason.
2. **Reader reopen.** Open a fresh `DbReader` (which reads the latest manifest on open) for the strict query. Correct and genuinely fresh, but too expensive for anything but rare admin/migration reads.
3. **Upstream a manual refresh API.** If SlateDB later grows a public `DbReader` refresh, strict mode switches to it transparently and the poll-interval bound goes away. This is a *future* SlateDB capability, not a turbolay patch.

Until (3) exists, strict mode is implemented via (1) with (2) available for admin paths, and is documented as "freshest within one poll interval," not "linearizable."

## Failure and safety properties

### Writer crash after write ack

If a write returned with `await_durable: true`, the token corresponds to durable WAL state. `m/latest_seq`, the node/edge base records, and the changelog entry are all durable together (one atomic batch). A reader — or a restarted writer — later replays them from object storage. The token remains honorable.

### Writer crash before write ack

No token is returned. The client must retry. No read guarantee is promised for an unacknowledged write; the partial batch either did not commit or is not observable. Because the batch is atomic, a reader never sees a node without its edge projections or without its changelog entry.

### Zombie writer — fencing via SlateDB manifest writer-epoch

This is the single hazard that single-writer-per-namespace does not eliminate by construction: a writer that was superseded (network partition, slow GC pause, redeploy overlap) but does not yet know it, still issuing writes.

turbolay does **not** solve this with leader election. Dgraph uses Raft to elect a leader and reject writes from stale leaders; we delete Raft (D2). Instead, SlateDB's manifest carries a **writer epoch**, CAS'd on open via an S3 conditional PUT. When a new writer opens the namespace, it bumps the epoch; the deposed writer's next manifest-touching operation fails the CAS and the `Db` returns `CloseReason::Fenced`, after which all further operations on that handle error. A fenced writer therefore *cannot* commit into the active lineage.

The consistency consequence: **every token issued by a successful, durable write belongs to the one valid sequence lineage.** There is no fork in the sequence, so `m/latest_seq` is globally monotonic per namespace and tokens are totally ordered. This is what makes a single logical seq authoritative — the property Dgraph needs Raft ordering + a timestamp oracle to guarantee across a replica group, we get from one CAS'd epoch.

### Reader timeout while waiting for a session token

If a reader cannot catch up to a supplied token within `gate_timeout`, `wait_for_session` returns a retryable error:

```json
{
  "error": "reader_behind",
  "retryable": true,
  "required_seq": 105,
  "current_seq": 101
}
```

The load balancer / client may retry another reader (which may already be caught up) or, as a last-resort fallback, route to the writer node, which always has the freshest state locally. This taxonomy is owned by RFC 0008.

## Tests required

1. **Read-your-writes across a reader node**
   - upsert a node + edge on the writer; capture the returned `seq`;
   - issue a traversal query to a deliberately stale reader with `consistency.session = seq`;
   - assert the reader waits / replays and returns the new node and edge.

2. **No-token bounded staleness**
   - upsert a node; query a stale reader without a token;
   - allow either stale or fresh result; after one `manifest_poll_interval`, assert the fresh result appears.

3. **Reverse-index lag hidden by tail scan** (the worked example)
   - add an edge; hold the `EdgeIn` reverse-index watermark behind the write seq;
   - run a reverse traversal with the write's session token;
   - assert the edge is returned via changelog-tail materialization, not via the (stale) index.

4. **Value-index lag hidden by tail scan**
   - update an indexed property; hold the value-index watermark behind the write seq;
   - query the property with the returned token;
   - assert the newly-matching UID is added and the newly-non-matching UID is removed via the tail overlay.

5. **Delete / update correctness**
   - delete an edge / node, or supersede a property, then query with the returned token;
   - assert the stale UID is filtered by the deleted-UID bitmap and the new state is visible.

6. **Zombie-writer fencing**
   - open a second writer on the same namespace (bumping the epoch);
   - assert the first writer's next write fails with `CloseReason::Fenced` and issues no token;
   - assert `m/latest_seq` never regresses or forks across the handover.

7. **Reader-behind error**
   - supply a token beyond what a reader can reach within `gate_timeout`;
   - assert a retryable `reader_behind` error with `required_seq` / `current_seq`.

8. **Strict mode**
   - with a short poll interval (or forced reader reopen), assert a strict query observes a write within the bounded window even without an explicit token.

## Alternatives considered

The dominant alternative is **Dgraph's own consistency stack**, which we adopt the *storage model* from (RFC 0004/0005/0006) but delete the *concurrency-control half* of. Each piece exists to serialize **concurrent distributed writers**; turbolay has exactly **one writer per namespace** (D2), so writes serialize by construction and a single logical seq is authoritative. What we drop, and why:

### Dropped: Zero timestamp oracle

Dgraph runs a central "Zero" service that hands out a monotonic `start_ts` at transaction begin and a `commit_ts` at commit, ordering all transactions across the cluster.

- **Why it exists:** to impose a global order on writes originating from many Alpha nodes concurrently.
- **Why we drop it:** our single writer *is* the order. `next_seq` in that one writer produces a strictly monotonic sequence with no coordination. A network service to assign timestamps would be pure overhead and a new failure domain. **Rejected.**

### Dropped: conflict-key OCC (optimistic concurrency control)

Dgraph tracks the key-set each transaction touched and, at commit, aborts if it overlaps a concurrently-committed transaction's key-set.

- **Why it exists:** to detect write-write conflicts between transactions racing on the same posting list.
- **Why we drop it:** two writes to the same `(src, predicate)` posting list from a single writer are already strictly ordered — the writer applies them one after another as read-modify-write (D11). There is no concurrent transaction to conflict with, so there is nothing to detect and nothing to abort. **Rejected.**

### Dropped: version-in-key MVCC

Dgraph encodes `start_ts` / `commit_ts` into each posting and keeps multiple versions per key, so a reader at `start_ts` iterates a snapshot-isolated view.

- **Why it exists:** to give concurrent readers a consistent snapshot while writers commit new versions.
- **Why we drop it:** snapshot isolation across concurrent writers is a non-problem here. Our reader consistency comes from **replay position** (`durable_seq` / `m/latest_seq`) plus the **changelog-tail overlay**, not from per-posting version stamps. SlateDB's LSM already provides the last-writer-wins point-in-time view of the KV base; we layer freshness on top with the seq gate. Version-in-key would bloat every posting and complicate the posting encoding for no benefit. **Rejected.**

### Dropped: linearizable reads via Raft ordering + WaitForTs

Dgraph serves a linearizable read by routing it through the Raft log and having the serving Alpha wait until it has applied up to a required timestamp (`WaitForTs`).

- **Why it exists:** to make a read on any replica reflect all commits ordered before it, despite replica lag.
- **Why we keep the *shape* but drop the *mechanism*:** our reader-freshness gate (`wait_for_session` on `durable_seq >= T`) is the direct analogue of `WaitForTs` — but it waits on SlateDB WAL replay position, not on a Raft apply index, and the order it waits into is the single writer's seq, not a Raft log. No consensus, no leader, no quorum. The zombie-writer hazard that Raft's leader election otherwise covers is handled by SlateDB's manifest writer-epoch fencing instead. So we keep read-your-writes and monotonic reads; we drop Raft entirely. **Raft rejected; the WaitForTs *idea* survives as the seq gate.**

Beyond the Dgraph-derived pieces, three turbolay-level alternatives:

### Strict by default

Every reader refreshes from object storage before every query.

- **Pros:** simplest user-facing model; globally fresh reads by default.
- **Cons:** adds S3 latency and cost to *every* query, including cheap point lookups and hot traversals; wastes work for the common read-your-writes flow where a session token is enough; makes low-latency graph reads much harder. **Rejected for default; kept as opt-in strict mode.**

### Writer-routed reads only

Route all strong reads to the single writer node, which always has the freshest local state.

- **Pros:** trivially strong; the writer's memtable is always latest.
- **Cons:** kills read scaling — the whole point of the stateless reader fleet (D9, goal #2) is that reads scale independently of the one writer; funneling strong reads to the writer recreates a hot spot. **Rejected except as the last-resort fallback for a `reader_behind` error.**

### External coordinator (DynamoDB / etcd / …)

Track the latest global sequence in an external strongly-consistent store.

- **Pros:** an explicit, easily-queried global freshness point.
- **Cons:** violates the S3-only coordination principle (D1/D2, plan §2.3) — CAS-on-manifest is our *only* coordination primitive; more infrastructure and more failure modes; and it is unnecessary, because SlateDB already exposes durable sequence progress (`durable_seq`) and WAL replay, and `m/latest_seq` already lives durably in-band. **Rejected.**

## Final contract

- Writes are acknowledged only after durable SlateDB WAL persistence (`await_durable: true`); the whole graph fan-out (node + `EdgeOut` + `EdgeIn` + touched indexes + changelog + `m/latest_seq`) commits in one atomic `WriteBatch`.
- Write responses return a session token = turbolay's logical `seq` (= `m/latest_seq` = injected SlateDB `seqnum`).
- Queries with `consistency.session = T` wait until the serving reader has replayed durable state through `T` (`DbReader::subscribe()` → `durable_seq >= T`), then serve.
- Queries without a token are bounded-stale (up to `manifest_poll_interval`).
- Queries with `consistency.strict = true` serve after advancing to the freshest durable state the reader can reach, bounded by the poll interval (or a reader reopen) until SlateDB exposes a public refresh API — which we will not patch in (D1).
- Secondary index / reverse-adjacency lag is invisible to callers, handled by the index-watermark + changelog-tail materialize-and-merge plan (RFC 0006/0007).
- Per-namespace sequence monotonicity — the property that makes a single logical seq authoritative — is guaranteed by single-writer-per-namespace (D2) plus SlateDB manifest writer-epoch fencing of zombie writers (`CloseReason::Fenced`).
- No timestamp oracle, no conflict OCC, no version-in-key MVCC, no Raft, no external coordinator. All deleted because turbolay has one writer per namespace.
