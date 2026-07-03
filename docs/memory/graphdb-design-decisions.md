---
name: graphdb-design-decisions
description: turbolay = graph DB on S3 (dgraph-on-SlateDB); locked v0 design decisions & RFC roadmap
metadata: 
  node_type: memory
  type: project
  originSessionId: e861d2c5-f32d-4d4a-853a-980ee68976d1
---

**turbolay** (`/Users/abhishek/hydradb/graphdb-on-s3/turbolay`) = openCypher graph DB on S3, dgraph's storage model reimplemented in Rust on **SlateDB** (KV does the heavy lifting; no badger, no raft). RFCs live in `docs/rfcs/`, mirroring the house style of `/Users/abhishek/hydradb/2026-06/fts-on-s3/docs/rfcs` (locked-decisions table, per-RFC "Decides" bullets, correct-first ledger). Reuse substrate from `/Users/abhishek/hydradb/2026-06/opendata` (`common/` crate).

**Why:** User's brief — object-native graph DB, reader/writer separation, strong consistency on S3, Rust, openCypher, SlateDB as KV. "Scope so we can implement faster; push optimizations as documented follow-ups."

Locked v0 decisions (best-judgment defaults 2026-07-03; user was away, can veto):
- D1 Substrate = SlateDB v0.14.1 unmodified.
- D2 Single-writer-per-namespace; NO raft/Zero-oracle/conflict-OCC/version-in-key-MVCC (all dgraph machinery deleted — it existed only for multi-writer). Manifest-epoch fencing.
- D3 Adjacency = dgraph posting-lists (UidPack: 256-uid blocks, base+groupvarint deltas) on SlateDB, **CSR-ready** (block-max metadata day one). CSR/WCOJ/bitpacking deferred.
- D4 Consistency = session token via own logical seq `m/latest_seq` (SlateDB `WriteOptions.seqnum`); reads = adjacency/index up to watermark + changelog-tail overlay merged. Strict mode bounded by poll interval.
- D5 Internal u64 UIDs + external-id→uid mapping (xid), via `common::SequenceAllocator`. Required for UidPack compression.
- D6 Indexes/reverse/count = posting lists keyed by order-preserving token/object/degree; lossy tokens re-fetch.
- D7 Query = openCypher READ subset (MATCH/WHERE/RETURN, fixed+var-length hops) + JSON upsert writes. Cypher writes (CREATE/MERGE/SET) deferred.
- D8 Reuse opendata `common` (serde: KeyPrefix+subsystem+terminated_bytes+var_u64+sortable; StorageBuilder; merge-op-by-record-type; SequenceAllocator; reader/writer split). Add SUBSYSTEM GRAPH=0x05.
- D9 Reader/writer node separation via SlateDB `DbReader`; same binary, RW vs RO backends.
- D10 Property-graph model (nodes+labels+props, typed directed edges+props); bidirectional storage (out+in). Value-log subsumed by SlateDB. Oversize property blob → reject v0 (spill deferred).
- D11 MergeOperator only for associative state (degree counter i32 sum, deleted roaring bitmap union, adjacency-set append). Split/rollup via single-writer RMW.
- D12 Workload = RAG knowledge graph (Source/Chunk/Entity/RELATES), 1–10M nodes/ns, correctness-first, shadow-test vs FalkorDB. Benchmark on REAL S3 (LocalStack hid 10× regression in namidb).

RFC roadmap (ALL DRAFTED 2026-07-03, in docs/rfcs/): 0000 index · 0001 consistency · 0002 substrate · 0003 keyspace/encoding · 0004 data-model+write-path+UID-alloc · 0005 posting-list substrate (roaring) · 0006 indexes · 0007 opencypher-read-path · 0008 http-service-and-fleet · 0017 observability. Plus docs/plan.md (narrative) + docs/open-decisions.md (22-decision register, all settled). Q2 changed UidPack→roaring after some docs written — reconciled. Error taxonomy canonical in 0008 (reader_behind/index_behind 503; unindexed_property/malformed_cypher/bfs_depth_exceeded 400; unsupported_cypher 501; oversize_node 413; fenced_writer 503). M0 is now implemented (see [[m0-storage-foundation-built]]); Q2 uses roaring for postings, but roaring only enters the codebase at M1. Prior art: namidb & turbolay-v0 chose CSR (but built own SST format); see [[max-3-subagents]] for delegation cap.
