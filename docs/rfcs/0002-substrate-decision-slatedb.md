---
title: "RFC 0002: Substrate Decision Record — SlateDB"
status: draft
date: 2026-07-03T00:00:00Z
related:
  - 0000-rfc-index.md
  - 0001-strong-consistency-model.md
  - ../plan.md
---

# RFC 0002: Substrate Decision Record — SlateDB

## Summary

This closes the substrate question on paper: **SlateDB v0.14.1 is committed as turbolay's storage substrate, used strictly unmodified, and the graph model is built as an application layer *on top of* it rather than as a self-owned graph-aware SST format.** This RFC does not re-derive the graph→KV mapping — see `../plan.md` §4 and RFC 0005 — it formalizes D1, D2, and D11 from `0000-rfc-index.md`, argues the graph-specific twist (build-on-top vs build-your-own SST), records the accepted constraints of running SlateDB unmodified, and pins down the engineering consequences.

## Decision

SlateDB v0.14.1 is the substrate for all of turbolay (D1). It is used **strictly unmodified**: no fork, no upstream patch on the critical path. Anything turbolay needs that SlateDB does not already expose is built as an application-level protocol on public SlateDB APIs, never as a change to SlateDB itself.

Coordination is **single-writer-per-namespace** (D2). SlateDB's manifest `writer_epoch` — CAS'd on open, fencing any zombie writer with `CloseReason::Fenced` — is the *only* coordination primitive. This replaces Dgraph's Raft groups and Zero timestamp oracle outright; there is no external lock service, no ZooKeeper, no DynamoDB.

`MergeOperator` is reserved for **associative state only** (D11): degree counters (i32 sum), the deleted-UID roaring bitmap (union), and ordered-set append. Non-associative posting maintenance — split and rollup — is applied by the single writer as read-modify-write over the block keyspace, never as a merge operand.

### The graph-specific decision: build on SlateDB, not a graph-aware SST format

turbolay is the third graph-on-S3 attempt in this lineage. The two priors — **namidb** and **turbolay-v0** — both made the *opposite* call: each built its **own LSM/SST format on object storage**, graph-aware from the SST layer up. namidb rejected SlateDB explicitly — "SlateDB is a KV store; we need graph-aware SSTs… we borrow its protocol shape and reimplement with a graph-aware SST format" — and materialized adjacency as CSR inside that custom format.

turbolay reverses that decision, and the reversal *is* the point of the brief. The argument is Dgraph's existence proof: **Dgraph already runs a scalable property-graph engine on a plain ordered KV store (Badger).** In that model there is nothing graph-aware in the storage layer at all —

- a **posting list is just a KV value** (a compressed sorted set of UIDs), and a graph edge is a member of one of those sets;
- an **adjacency read is just a `get`** on a `(src, predicate)` key, served by the store's block cache and bloom filters;
- an **index probe is just another `get`**, and a range predicate is just a `scan` over order-preserving tokens.

None of that needs a comparator, a column, or a graph-shaped block layout in the SST. It needs exactly what a good ordered-KV/LSM store already gives you: ordered byte keys, point `get`, prefix `scan`, atomic multi-key batches, and a fast cache. So we let **SlateDB own the hard, generic half — LSM structure, compaction, block caching, bloom filters, writer-epoch fencing, and the entire S3 object lifecycle** — and we port only the graph-specific half on top: Dgraph's `x/keys` (order-preserving layout) and `posting` (list model, predicate sharding, split/rollup). Its `codec` (UidPack) and `algo` (sorted-UID intersect/merge/difference) are **replaced by roaring** (RFC 0005), not ported. What we do port is Badger- and Raft-agnostic and moves over nearly verbatim.

This is strictly the faster path to a correct v0. Building a graph-aware SST format means owning — and debugging on real S3 — compaction, caching, bloom filters, manifest CAS, and crash recovery, all *before* writing a line of graph logic. namidb and turbolay-v0 paid exactly that tax (and one of them hid a ~10× cold-read regression behind LocalStack while paying it). turbolay spends that budget on the graph model instead and inherits a maintained, S3-native storage engine for free. This is the literal reading of the brief: *let SlateDB do the heavy lifting.*

**Considered and deferred — CSR / self-materialized SST (RFC 0009).** The alternative is not wrong, it is *premature*. Compressed-sparse-row adjacency and a bespoke SST can beat posting-list-on-KV on traversal-bound workloads — that is precisely why namidb reached for it. turbolay treats CSR as an **additive optimization, not the v0 substrate**: posting-list blocks carry block-max/skip metadata (first-UID, count) from day one (RFC 0005), so a later CSR/leapfrog-join RFC is a bolt-on materialization behind the same `IndexAm`/adjacency surface, gated on real-S3 traversal numbers (RFC 0017), not intuition. We keep CSR as the documented optimization path; we do not pay for a self-owned SST format to get v0 running.

## Constraints accepted

From `../plan.md` §3 — permanent design inputs for v0, not open questions. Each is the price of running SlateDB unmodified, and each has a named owner:

- **Bytewise-only key ordering.** SlateDB has no custom comparators. Mitigated by order-preserving key encodings — big-endian ints, sign-flipped floats, escaped/terminated variable-length components — owned by RFC 0003, built on opendata's `common::serde`.
- **Pre-1.0 API churn.** SlateDB's storage format is stable across adjacent versions; its Rust API is not. Isolated behind opendata's `common::Storage` wrapper (M0 deliverable). We **never touch `slatedb::Db` directly** (D8) — an API break is contained to one wrapper module.
- **No key-value separation.** Large values are rewritten whole by compaction. v0 caps a node's total property size and **rejects oversize writes** (RFC 0004); spill-to-raw-S3 is backlog (RFC 0014), triggered only if the cap proves wrong on real workloads. This constraint is also *exactly why we keep Dgraph's 512 KiB posting-list split* (RFC 0005): a supernode's adjacency must never become one giant value that compaction rewrites in full — it is bin-split into part-keys, which on S3 are just new keys, no in-place rewrite.
- **Per-namespace manifest overhead.** One DB (manifest + poller) per namespace — the "namespace = shard = tenant = graph" invariant. Acceptable at POC tenant counts; revisit (lazy open, shared reader processes) only if tenant count grows past POC scale.
- **Non-associative posting maintenance via single-writer RMW, not merge (D11).** Posting-list split and rollup are not associative, so the single writer applies them as read-modify-write over the block's keyspace. `MergeOperator` is reserved for genuinely associative state only: the deleted-UID roaring-bitmap union, degree counters (i32 sum), and ordered-set append. Single-writer-per-namespace is what makes RMW safe without a coordinator — there is no concurrent writer to lose an update to.

## Consequences

D1 — "SlateDB used strictly unmodified: no fork, no upstream patch on the critical path" — is a hard constraint, not an aspiration. Two consequences follow directly, and both are built entirely on public SlateDB APIs (`WriteBatch`, `Db`, `DbReader`) with zero SlateDB changes:

**(a) Session tokens are turbolay's own logical seq, via an `m/latest_seq` key protocol — not SlateDB's internal seqnum.** The naive design would use `WriteHandle::seqnum()` directly as the session token. Instead the writer maintains its own `next_seq` counter and writes `m/latest_seq` into every `WriteBatch` (optionally injected as SlateDB's `WriteOptions.seqnum`); a reader's freshness gate replays durable state and checks `m/latest_seq >= T` before answering a query carrying token `T`. This is an application-level protocol on public `WriteBatch`/`Db`/`DbReader` APIs — single-writer-per-namespace monotonicity (D2) is what makes the plain put safe, so it requires no coordination and no SlateDB change. Full protocol detail is owned by RFC 0001, amended by RFC 0004's seq section (D4).

**(b) Strict mode is bounded, not a true forced refresh, until upstream grows a public refresh API on its own.** A `DbReader` polls the manifest at `manifest_poll_interval` and replays newer WAL; the available freshness levers are therefore a short poll interval (bounds staleness, not a true refresh) or an explicit fresh `DbReader` reopen per strict query (correct, but too expensive to be the default). D1 forecloses the fourth option — patching SlateDB ourselves to add a public `DbReader::refresh()`. So in v0, "strict mode" means *bounded-by-`manifest_poll_interval` staleness, or an explicit reader reopen* where a harder guarantee is required — documented as such, not as a true global-freshness guarantee. `../plan.md` §12 carries a public `DbReader` manual-refresh API as an **upstream ask, not a blocking dependency**: v0 ships on the bounded semantics regardless of whether upstream ever adds it. This is owned by RFC 0001.

Everything turbolay needs beyond point/scan/batch is likewise application-level on public APIs: `MergeOperator` routing by record-type for associative state (D11), `DbReader` poll/subscribe for the reader fleet (RFC 0008), O(1) checkpoints and clones for branch-and-query snapshots, and a separate `wal_object_store` to put the WAL on low-latency storage (e.g. S3 Express One Zone) while bulk SSTs live on S3 Standard. None of these require a fork.

## Escape hatch

Every index extension owns its keyspace layout entirely behind the `IndexAm` trait (`x/{index_id}/…`, opaque to the core — RFC 0006). The core interprets only **nodes, edges, the changelog, and the trait surface**; it never interprets index bytes. Consequence: migrating one index type — or, in the limit, the whole engine — off SlateDB later is a per-extension rewrite of `extract`/`apply`/`execute` against a new backend, not a redesign of the node/edge store, the changelog, or the query planner.

Caveat, and it is the exact lesson of the priors: **this escape hatch is cheapest when the destination is another ordered-KV/LSM substrate.** Because turbolay leans on prefix `scan`, block RMW, and `WriteBatch` atomicity, retargeting to a *structurally different* backend — a columnar or CSR-materialized store, the namidb / turbolay-v0 shape — has no clean drop-in even behind the trait: those primitives have no direct analog, so that particular rewrite is closer to a redesign of the affected extension's internals than a backend swap. The trait boundary contains the blast radius to one extension; it does not promise the rewrite is small. (This cuts both ways with the CSR decision above: adopting CSR later is additive precisely because it materializes *alongside* the KV posting lists rather than replacing the substrate.)

## Final contract

- Substrate is **SlateDB v0.14.1, used unmodified**: no fork, no upstream patch on turbolay's critical path (D1). Anything SlateDB doesn't expose is an application-level protocol on public APIs.
- The graph model is built **on top of** SlateDB — SlateDB owns LSM/compaction/caching/bloom-filters/fencing/S3; turbolay ports Dgraph's `codec`/`x`/`posting`/`algo` on top. A **self-owned graph-aware SST format** (the namidb / turbolay-v0 path) is **not** pursued for v0; **CSR remains the additive optimization path (RFC 0009)**, gated on real-S3 numbers.
- Coordination is **single-writer-per-namespace**, fenced by SlateDB's manifest `writer_epoch` CAS (D2). No Raft, no Zero oracle, no external lock service.
- `MergeOperator` is used **only for associative state** — degree counters (i32 sum), deleted-UID roaring bitmap (union), ordered-set append; **split/rollup is single-writer RMW** (D11).
- turbolay's session token is its **own logical seq via `m/latest_seq`** (D4), not SlateDB's internal seqnum; full detail in RFC 0001, amended by RFC 0004.
- **Strict consistency mode is bounded** by `manifest_poll_interval`, or implemented via reader reopen — not a true forced refresh — pending a public SlateDB refresh API that turbolay will not build itself (owned by RFC 0001).
- Accepted constraints — **bytewise ordering (RFC 0003), pre-1.0 API churn (behind `common::Storage`), no KV separation → node size cap (RFC 0004) and the 512 KiB posting split (RFC 0005), per-namespace manifest overhead, RMW posting maintenance (D11)** — are permanent v0 design inputs.
- Each `IndexAm` extension owns its keyspace; that boundary is the migration escape hatch for both individual indexes and the engine as a whole, cheapest when the target is another ordered-KV/LSM substrate and closer to a redesign for a columnar/CSR backend.
