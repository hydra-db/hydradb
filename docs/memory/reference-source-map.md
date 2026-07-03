---
name: reference-source-map
description: "Local paths to all repos turbolay draws on (slatedb, opendata, dgraph, fundamentals, prior attempts)"
metadata: 
  node_type: memory
  type: reference
  originSessionId: e861d2c5-f32d-4d4a-853a-980ee68976d1
---

Source repos for the turbolay graph-DB work (full annotated map: `turbolay/docs/impl/2026-07-03-references-and-source-map.md`). See [[graphdb-design-decisions]].

- **SlateDB** (substrate, v0.14.1, unmodified): `/Users/abhishek/hydradb/graphdb-on-s3/slatedb` — API in `src/lib.rs`, `src/ops.rs`, `src/db_reader.rs`, `src/merge_operator.rs`, `src/batch.rs`, `src/config.rs`.
- **opendata** (reuse `common`): `/Users/abhishek/hydradb/2026-06/opendata` — `common/src/serde/` (key toolkit; add GRAPH=0x05), `common/src/storage/`, `common/src/sequence.rs`, `vector/src/storage/merge_operator.rs`, `log/` (service to imitate), `rfcs/0000-template.md`, `AGENTS.md`.
- **Dgraph** (model to port): `/Users/abhishek/hydradb/graphdb-on-s3/dgraph` — `codec/codec.go`, `x/keys.go`, `posting/{list,index,mvcc}.go`, `algo/uidlist.go`, `protos/pb.proto`.
- **Fundamentals of Graph** (concepts): `/Users/abhishek/hydradb/graphdb-on-s3/fundamentals-of-graph` — `chapters/ch04a-s3-framings.typ`, `ch13-18` (esp. `ch18-dgraph-vs-s3.typ`).
- **fts-on-s3** (RFC house style, sister project): `/Users/abhishek/hydradb/2026-06/fts-on-s3` — `docs/rfcs/`, `docs/plan.md`.
- **Prior graph-on-S3 attempts**: `namidb` (own SST format + CSR; `docs/rfc/001,002,018,024,027`), `turbolay-v0` (tpuf substrate + CSR/delta; `docs/impl/`), `turbolay-poc` (Python ClickHouse RAG-KG + FalkorDB shadow harness `shadow/`), `lance-graphdb-experiment` — all under `/Users/abhishek/hydradb/graphdb-on-s3/`.
