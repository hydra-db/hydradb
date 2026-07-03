---
name: m0-storage-foundation-built
description: turbolay M0 (keyspace + encodings + id allocation) is implemented, tested, green
metadata:
  node_type: memory
  type: project
  originSessionId: e861d2c5-f32d-4d4a-853a-980ee68976d1
---

M0 storage foundation of **turbolay** is built and green as of 2026-07-03 (RFC 0003 + RFC 0004 §UID). First real code in the crate. Binding spec was `docs/impl/2026-07-03-m0-handoff.md`; decisions-of-record appended to `docs/impl/2026-07-03-scratchpad.md`.

**What exists** (`src/`): `serde/{mod,record_tag,keys,token}.rs` (keyspace: `SUBSYSTEM GRAPH=0x05`, `RecordType` 0x1–0xB, all key builders/parsers as inverse pairs, order-preserving index tokens), `ids.rs` (`GraphAllocators` over `common::SequenceAllocator` — 5 spaces uid/pred/label/prop/log-seq on `Meta["seq/*"]`; `resolve_or_create_xid`), `schema.rs` (`SchemaEntry` LE codec + `SchemaCache` + `rebuild_from_storage`), `merge.rs` (`GraphMergeOperator` **stub**), `storage.rs` (`GraphStorage::open/in_memory`), `error.rs`, `telemetry.rs` (tracing init). Tests: `tests/keyspace_props.rs` (north-star proptests). **Gates:** cargo test 62 unit + 14 proptest green; clippy `-D warnings` clean; fmt clean; `cargo run` smoke ok.

**Why (non-obvious choices):**
- `common` was a **path dep** into the sibling opendata workspace in M0 — **SUPERSEDED in M1**: it is now a vendored fork at `vendor/common`, see [[m1-vendored-common-and-seqnum]].
- `GRAPH=0x05` defined **locally** in `serde/mod.rs`, NOT upstreamed into `common::serde::subsystem` (avoid mutating shared crate; upstream later).
- Merge op is a **stub** (newest-wins + warn); associative roaring-union/i64-counter merges are M1, so **`roaring` is not a dep yet**. Full dispatch table lives in `merge.rs` rustdoc.
- `hash` tokenizer = inline FNV-1a-64 (stable, version-independent). Xid/SchemaName **values are BE** per RFC 0003 table (exception to values-LE house rule); SchemaEntry value is LE.

**How to apply / next:** M1 = write path + posting lists (RFC 0004/0005): add `roaring`, fill the merge operator (route by `keys::record_type_of`), `value.rs` (NodeRecord/PostingValue/ChangeRecord), atomic WriteBatch, logical-seq protocol. Reader/compactor must register the SAME merge operator. See [[graphdb-design-decisions]].
