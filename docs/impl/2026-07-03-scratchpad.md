---
title: "turbolay — Implementation Scratchpad"
date: 2026-07-03
kind: scratchpad
---

# Implementation Scratchpad

Running notes, open implementation questions, and spikes to run. Not a spec — the RFCs are the spec. Append freely; promote resolved items into the relevant RFC.

## Spikes to run (before/early M0–M1)
- [ ] **opendata `common` reachability** — is it a path/git dep from turbolay, or do we vendor `serde/` + `storage/` + `sequence.rs` + the merge framework? (Decides Cargo.toml.) → M0 task 0.
- [ ] **Roaring crate + serialization** — pick the Rust roaring crate; confirm `RoaringTreemap` (64-bit) portable serialize/deserialize is stable across versions and cheap enough; measure serialized size vs a UidPack-style baseline on realistic adjacency (validates Q2). Note the chosen format in RFC 0005.
- [ ] **bincode vs rkyv for NodeRecord** — bincode is the M1 default (Q7); timebox an rkyv zero-copy spike only if node-read shows up in profiles. Codec stays behind `NodeCodec`.
- [ ] **Real-S3 bench harness** — stand up the RFC 0017 Phase 3 harness against real S3 (not LocalStack) early enough to catch cold-first-hop regressions (namidb's ~10× lesson). InMemory/local for correctness only.
- [x] **`decypher` audit (Q23a — parser build) — DONE 2026-07-03.** Findings: license EUPL-1.2 **OR MIT OR Apache-2.0** (✅ take MIT/Apache); diagnostics rowan CST + `miette` + spans + error-resilient (✅ excellent); recency v0.2.0-**alpha**.6 May 2026, 118 commits, ~4.7k dl/mo (🟡 early); **bus factor 1** (1 star/1 fork/single maintainer, 🔴); **AST explicitly "unstable until 0.2.0"** (🔴 lowering churns per alpha bump); **var-length `*1..3`** — API exposes `ast::pattern::{Quantifier (min/max hops), RangeLiteral, RelationshipPattern, RelationshipDirection}` → **supported** (✅ resolves the Q17 must-have; confirm end-to-end parse in the first spike). Also present: `sema_scope`/`sema_with_scope`/`hir_lower` tests → real scope analysis + an HIR layer. Broader finding: **no production-grade parser-*only* full-openCypher Rust crate exists** — `ocg`/`gqlite` are full engines (fight our IR/planner), `open-cypher` is pest/PEG (generic errors), libcypher-parser is C FFI (no maintained Rust binding). **Resolution (user decision 2026-07-03): ADOPT `decypher`, pinned at `=0.2.0-alpha.6`**, `miette` feature on. Rationale user accepted: full-grammar AST + best-in-class diagnostics now, worth the alpha/bus-factor-1 risk. Mitigations to hold: pin the exact version (unstable AST — upgrade deliberately, re-run lowering tests each bump); permissive MIT/Apache arm means we can fork if abandoned; our lowering is the only coupling surface. Subset gate still lives in *lowering* (RFC 0007 §13): decypher parses the full grammar → our lowering emits `unsupported_cypher` for out-of-v0 constructs; `malformed_cypher` = decypher parse error (surface its miette diagnostics). Open sub-question for the first spike: **lower from decypher's typed AST (`ast::query::Query`) or its HIR** — pick whichever is the more stable/ergonomic source.
  - **VENDORED + VERIFIED 2026-07-03.** Cloned to `/Users/abhishek/hydradb/graphdb-on-s3/decypher` (sibling of turbolay) at tag **`v0.2.0-alpha.6`**, upstream `origin` remote retained for rebasing. Single crate, `edition = "2024"`, `rust-version = "1.94"`. Builds clean with `cargo +1.96.0 build --features miette`; **all 71 `roundtrip` tests pass** incl. `rt_variable_length_path`, `rt_unbounded_variable_length`, `rt_with_pipeline`, `rt_unwind`, `rt_union(_all)`, `rt_xor_operator` → full-grammar coverage confirmed. Deps light: `thiserror`, `ryu`, `rowan 0.16`, `unicode-ident` (+ optional `serde`/`miette`); default feature `hir`. Rich internals: `parser/{grammar,lexer}` (hand-written), typed `ast/*`, rowan CST `syntax/ast`, **full `hir/{lower,arena,binding}` + `sema/{scope,resolve,aggregation}` + `recover`** — the HIR (bindings/scopes already resolved) is the **likely lowering seam** (turbolay lowering: decypher HIR → `PlanNode` IR). **Coordination: adopting decypher raises turbolay's MSRV to ≥1.94** — bump the default toolchain (currently 1.91.1) or add a `rust-toolchain.toml` pin (1.95/1.96 installed) before path-depping. Wire-in (path dep + turbolay workspace-ify) is deferred to the frontend track's first task, not done yet.

## Open implementation questions
- [ ] Split threshold check: how does the writer know a `Single` posting crossed 512 KiB without serializing every add? (periodic size check vs merge-resolved size sampling — RFC 0005 left it "observe via merge-resolved size or periodic check"; pick a concrete trigger).
- [ ] Deleted-node bitmap scope: one per-namespace `Meta["deleted_nodes"]` roaring — confirm read-path caches it per query and that its size stays sane before vacuum (RFC 0012 trigger). Same for per-(anchor,pred) deleted-edge bitmaps.
- [ ] Count-index maintenance: moving a uid between degree buckets on every edge add/delete is 2 posting writes per edge — measure the write-amp; consider deferring count-index maintenance behind a directive.
- [ ] Changelog tail scan: exact `ChangeRecord` fields needed to re-evaluate a *traversal* pattern (not just a filter) on tail nodes/edges — validate against RFC 0007's worked example.
- [ ] Schema id allocation vs data in the same batch: ensure an unknown name interned mid-batch can't collide with a concurrent... (moot — single writer, but assert in a test).
- [ ] `WriteOptions.seqnum` injection: confirm SlateDB accepts our monotonic logical seq and that it must strictly exceed the current max (single-writer guarantees it) — integration test.

## Decisions settled (pointers)
- All 22 forks: `docs/open-decisions.md` (Decisions log). Locked table: RFC 0000 D1–D12.
- Q1 posting-lists CSR-ready · Q2 roaring · Q3 read-subset+JSON writes · Q4 no sortkey (deferred) · Q5 u64+xid · Q6 monolithic blob · Q7 bincode · Q8 tombstone-and-filter · Q9 deleted-edge bitmap · Q10 size-adaptive add · Q14 exact/int/float/hash · Q15 unindexed→error · Q16 reverse always · Q17 var-length BFS.
- **Q23 full-parser-now** (Cypher frontend, RFC 0007 §13): parse full openCypher during M1, subset gate at lowering; **Q23a parser impl = ADOPT `decypher`** (pinned `=0.2.0-alpha.6`, miette on; user decision 2026-07-03 accepting alpha risk; var-length confirmed via `ast::pattern::Quantifier`; lower from its AST/HIR to our IR); Q23b `NameResolver` seam confirmed; Q23c import RFC 0004 `TypedValue`.

## TODO checklist (M0) — DONE 2026-07-03
- [x] Confirm dep vs vendor for `common`; wire Cargo.toml. → **path dep**: `common = { path = "../../2026-06/opendata/common", package = "opendata-common" }`. Resolves cleanly (common's `workspace = true` deps resolve against the opendata workspace it lives in). No vendoring needed.
- [x] `serde/record_tag.rs` + round-trip test. → `RecordType` (SchemaName..Meta, 0x1..0xB), high-nibble tag via `common::RecordTag`, `from_tag_byte` validates reserved low nibble == 0.
- [x] `serde/keys.rs` builders/parsers (inverse pairs) for all record types + scan-range helpers (`record_type_range`, `adjacency_part_range`, `index_pred_range`, `index_token_range`, `log_range`).
- [x] First failing proptest → now passing: `tests/keyspace_props.rs` proves `Index[pred][token]` order == logical order for int/float/exact + all boundaries (the north star). Plus UID ordering, xid ordering, range totality, round-trips.
- [x] `ids.rs` — `GraphAllocators` (5 spaces: uid/pred/label/prop/log-seq on `Meta["seq/*"]` keys) + `resolve_or_create_xid` + restart-monotonicity test.
- [x] `schema.rs` — `SchemaEntry` LE codec + `SchemaCache` (per-kind bimap) + `rebuild_from_storage`.
- [x] `storage.rs` over `common::Storage` (`GraphStorage::open/in_memory`); `merge.rs` = `GraphMergeOperator` stub registered on the writer.
- **Gate status:** `cargo test` 62 unit + 14 proptest green; `cargo clippy --all-targets -- -D warnings` clean; `cargo fmt --check` clean; `cargo run` smoke ok (alloc uid, resolve xid, read back).

## M0 decisions of record (deviations from handoff — intentional)
- **`GRAPH = 0x05` defined locally** in `src/serde/mod.rs` (`SUBSYSTEM`), NOT added to `common::serde::subsystem`, to avoid mutating the shared crate from this repo. Upstreaming the registration is a follow-up.
- **Merge operator is a stub** (newest-wins + `tracing::warn!` if ever invoked; full RFC 0003 dispatch table reproduced in `merge.rs` rustdoc). The associative roaring-union / i64-counter merges are M1 — so **`roaring` is NOT yet a dependency**. Pick the roaring crate + confirm `RoaringTreemap` serialization stability at the top of M1 (the open spike still stands).
- **Hash tokenizer** = inline **FNV-1a-64**, 8 bytes BE, equality-only (lossy → planner re-fetch flag). Chosen for a stable, dependency-version-independent digest.
- **Xid / SchemaName values are big-endian** (uid u64 BE / id u32 BE) per the RFC 0003 record table, even though the general house rule is values-LE. `SchemaEntry` values are LE.
- Module layout follows the handoff: `src/serde/{mod,record_tag,keys,token}.rs`, `ids.rs`, `schema.rs`, `merge.rs`, `storage.rs`, `error.rs`, `telemetry.rs`. `value.rs` (NodeRecord/PostingValue/ChangeRecord) deferred to M1 — those are write-path value types, not M0 keyspace.

## Watch-outs (from prior art)
- Never one S3 object per edge; postings live inside SlateDB SSTs (fundamentals ch18).
- Design against tombstone storms + tiny-object storms (the two S3-specific failure modes).
- Cache keyed by immutable manifest/generation version → no invalidation protocol.
