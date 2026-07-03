---
name: m1-vendored-common-and-seqnum
description: M1 vendored common as a workspace fork + added WriteOptions.seqnum injection (RFC 0004 logical-seq)
metadata: 
  node_type: memory
  type: project
  originSessionId: 210365a4-9709-46cf-ac3a-719c9d5bdaa3
---

M1 pre-flight is done and committed (post 2026-07-03). Two blockers resolved
before `write.rs`:

**Roaring spike** — `roaring = "0.10"` (workspace pin; resolves 0.10.12) added;
`tests/roaring_spike.rs` confirms portable serde round-trips, ascending
iteration, 2.5–10× compression under a naive 8B/uid baseline. No risk. Confirms
RFC 0005 Q2.

**Vendoring + seqnum injection** — user chose to **copy `common` into the repo as
a fork** (reverses M0's no-vendoring; [[m0-storage-foundation-built]]) so the
storage layer can be modified freely. Turbolay is now a **Cargo workspace**:
- `vendor/common` = fork of `opendata-common` 0.1.17 (lib `common`).
- `vendor/macros` = fork of `opendata-macros` (the `storage_test` test macro; so
  common's own 249-test suite compiles).
- Root `Cargo.toml` has `[workspace.dependencies]` (single source of dep versions,
  pinned to opendata's at fork time); both members inherit via `.workspace = true`.
- Dropped common's `varint` bench.

**Logical-seq decision = Option A (inject), now implemented.** Added `seqnum: u64`
to `common::WriteOptions` (**default 0 = auto-generate, a no-op for existing
callers**). SlateDB's `WriteOptions.seqnum` already supports user injection
(`slatedb/src/config.rs:467`, `batch_write.rs:205`; strictly-increasing or
`InvalidSequenceNumber`). Forwarded at the 3 `SlateDbStorage` write sites and
honored by `InMemoryStorage::assign_seqnum`. So turbolay's logical seq == SlateDB
durable seq: **one clock** for the session token + reader freshness gate. Decision
record: `docs/impl/2026-07-03-m1-spike-and-seqnum-decision.md`.

**Why (non-obvious):** the fork is what lets us touch the storage substrate
(seqnum injection) without mutating the shared sibling crate that vector/timeseries
depend on. The one-clock design is RFC 0004's final contract.

**Carry into write.rs (next):** commit each batch with `WriteOptions {
await_durable: true, seqnum: logical_seq }`; seed logical seq ≥1 and resume above
the durable max on recovery (single-writer monotonic `next_seq` gives strict
increase). Fault-injection for crash tests = `common::storage::in_memory::
FailingStorage` (`test-utils` feature) — drive the **`apply`** path (`merge` has
no fail slot). Deletes are `RecordOp::Delete(Bytes)` (no DeleteRecordOp).
`resolve_or_create_xid` must be refactored to **return** RecordOps for the batch
instead of its own eager `storage.put`. Build order = handoff deliverables 1→6
(posting.rs → value.rs → merge.rs → posting_ops → write.rs → recovery). See
[[graphdb-design-decisions]].
