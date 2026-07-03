---
title: "turbolay — References & Source Map"
date: 2026-07-03
kind: reference
---

# References & Source Map

Every external repo/doc turbolay draws on, where it lives, and **what to read it for**. Share this with anyone (or any agent) picking up the work — it's the orientation map behind the whole RFC set.

## This project
- **turbolay** — `/Users/abhishek/hydradb/graphdb-on-s3/turbolay` — the graph DB we're building. RFCs in `docs/rfcs/`, narrative in `docs/plan.md`, decisions in `docs/open-decisions.md`, implementation notes in `docs/impl/`.

## Substrate — SlateDB (we build ON this, unmodified)
- **`/Users/abhishek/hydradb/graphdb-on-s3/slatedb`** — Rust LSM KV store on object storage. **v0.14.1.** Used strictly unmodified (RFC 0002).
- Read for the public API we code against:
  - `src/lib.rs` — public exports · `src/ops.rs` — all read/write/metadata traits & signatures
  - `src/db.rs` (`Db`, `WriteHandle`) · `src/db_reader.rs` (`DbReader`, poll/subscribe) · `src/db_transaction.rs`
  - `src/merge_operator.rs` (associative merge — degree counters, roaring unions) · `src/batch.rs` (`WriteBatch`)
  - `src/config.rs` (`Settings`, `DbReaderOptions`, `manifest_poll_interval`) · `src/types.rs` · `src/db_iter.rs` · `src/bytes_range.rs`
  - `rfcs/` — SlateDB's own design docs (manifest, compaction, merge-operator, transactions)
- Key facts: bytewise-only key order (no custom comparators); `WriteOptions.seqnum` lets us inject our own logical seq; `DbStatus.durable_seq` + `subscribe()` for the read-your-writes gate; single-writer fencing via manifest `writer_epoch` → `CloseReason::Fenced`.

## Reusable substrate — opendata (we depend on / mirror `common`)
- **`/Users/abhishek/hydradb/2026-06/opendata`** — Rust multi-service repo on SlateDB (keyvalue/log/queue/timeseries/vector). We add a `graph` service in the same shape and reuse `common`.
- Read for the toolkit we steal (D8):
  - `common/src/serde/` — **the crown jewel**: `key_prefix.rs` (2-byte subsystem+version), `subsystem.rs` (registry — we add `GRAPH=0x05`), `record_tag.rs`, `terminated_bytes.rs` (order-preserving var-length), `varint.rs`, `sortable.rs` (i64/f64 sign-flip), `seq_block.rs`, `encoding.rs`
  - `common/src/storage/` — `mod.rs` (Storage traits), `slate.rs` (SlateDB adapter + durable-watermark bridge), `factory.rs` (`StorageBuilder`, object-store dispatch, foyer cache), `in_memory.rs` (test fake)
  - `common/src/sequence.rs` (`SequenceAllocator` — crash-safe u64 ids) · `common/src/coordinator/` (write coordinator, epoch watcher, view) · `common/src/bytes.rs` (`BytesRange`, `lex_increment`)
  - `vector/src/storage/merge_operator.rs` — the "route merge by record-type" pattern to copy
  - `log/` — closest service to imitate (composite keys, seq, reader/writer split); `log/src/server/http.rs` (axum shape)
  - `rfcs/0000-template.md` (RFC template) · `AGENTS.md` / `CONTRIBUTING.md` / `PROMPT.md` (house conventions: `bytes::Bytes`, keys BE / values LE, given/when/then + `should_` tests, `cargo fmt` + clippy `-D warnings`)
  - `macros/` — `#[opendata_macros::storage_test]` test fixture

## Model source — Dgraph (we port the model, not the plumbing)
- **`/Users/abhishek/hydradb/graphdb-on-s3/dgraph`** — Go graph DB on Badger + Raft + Zero oracle.
- Read for the graph→KV mapping we adapt (RFC 0003/0004/0005/0006):
  - `codec/codec.go` — UidPack (we **replace** with roaring, but the block/split ideas transfer)
  - `x/keys.go` — order-preserving key layout (`DataKey`/`IndexKey`/`ReverseKey`/`CountKey`, `ParsedKey`) — port the layout
  - `posting/list.go` — posting-list model, `pack` vs `postings`, 512 KiB split (`binSplit`), rollup · `posting/index.go` — triple→KV fan-out (data/reverse/index/count) · `posting/mvcc.go` — delta/rollup (we simplify)
  - `algo/uidlist.go` — sorted-UID intersect/merge/difference (we **replace** with roaring)
  - `protos/pb.proto` — `PostingList`, `Posting`, `DirectedEdge`, `SchemaUpdate`
  - What we DELETE: `worker/draft.go` + raftwal (Raft), `posting/oracle.go` + Zero (timestamp oracle/OCC), Badger value-log. All exist only for multi-writer distribution.

## Concept source — Fundamentals of Graph (the "why")
- **`/Users/abhishek/hydradb/graphdb-on-s3/fundamentals-of-graph`** — 18-chapter Typst book, "KV-Backed Property Graphs."
- Read `chapters/`: `ch04a-s3-framings.typ` (five "S3 as source of truth" framings), `ch13-dgraph-posting.typ` (posting-list model), `ch14/15` (storage/indexing), `ch16` (query/intersection), `ch17` (txn — what we drop), **`ch18-dgraph-vs-s3.typ`** (the Dgraph→S3 Borrow/Rework/Avoid synthesis — the intellectual backbone). Key rules: "key design is query design", "never one S3 object per edge", design against **tombstone storms + tiny-object storms**.

## House-style source — fts-on-s3 (sister project)
- **`/Users/abhishek/hydradb/2026-06/fts-on-s3`** — FTS-on-S3 on SlateDB. Same team, same house style we mirror.
- Read `docs/rfcs/` (0000 index, 0001 consistency, 0002 substrate, 0003 keyspace, 0006 posting-blocks, 0007 query, 0017 observability) and `docs/plan.md` — the RFC format, locked-decisions table, correct-first ledger.

## Prior graph-on-S3 attempts (prior art — read before re-deciding)
- **namidb** — `/Users/abhishek/hydradb/graphdb-on-s3/namidb` — most developed prior attempt; built its OWN LSM/SST format (rejected SlateDB-as-substrate) + CSR adjacency. Read `docs/rfc/001-storage-engine.md` ("why not SlateDB"), `002-sst-format.md`, `018-csr-adjacency.md`, `024-wcoj.md`, `027-compaction`. Lesson we heed: **benchmark on real S3, not LocalStack** (hid a ~10× cold regression).
- **turbolay-v0** — `/Users/abhishek/hydradb/graphdb-on-s3/turbolay-v0` — S3-native graph on the turbopuffer substrate; per-partition CSR + WAL delta-graph. Read `docs/impl/2026-05-11-*-csr-and-delta-graph.md` (why the tpuf posting-list hybrid does NOT port to graph adjacency), `docs/impl/research/*` (substrate + FalkorDB traversal research).
- **turbolay-poc** — `/Users/abhishek/hydradb/graphdb-on-s3/turbolay-poc` — Python ClickHouse POC (RAG KG Source/Chunk/Entity/RELATES). Read `src/turbolay/indexer/precompute.py` (bounded-BFS path precompute), `ch/ddl.py` (projection-based adjacency), `shadow/` (the FalkorDB set-diff correctness harness — 0 missing / ≤5% extra).
- **lance-graphdb-experiment** — `/Users/abhishek/hydradb/graphdb-on-s3/lance-graphdb-experiment` — Lance/DataFusion join-based traversal bench (columnar edge tables, no adjacency structure). Reference only.

## Convergent lessons from prior art (baked into our decisions)
- CAS-on-manifest as the only coordinator (SlateDB gives this free) · single-writer + epoch fencing · WAL-tail cut-point for read-your-writes · materialize forward+reverse · cache keyed by immutable version · shadow-test vs FalkorDB · **real-S3 benchmarking only**.
