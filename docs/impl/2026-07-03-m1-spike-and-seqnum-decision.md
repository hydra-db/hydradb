---
title: "turbolay — M1 Pre-flight: Roaring Spike + Logical-Seq Decision"
date: 2026-07-03
kind: spike-report + decision-record
status: awaiting sign-off on the seqnum decision
---

# M1 Pre-flight — spike results and the one decision that gates `write.rs`

The M1 handoff named two things to resolve *before* deliverable #1: the **roaring
spike** and the **`WriteOptions.seqnum` injection decision** (the top standing
risk). Both are now investigated with evidence. This doc is the reviewable
output. **One item needs your sign-off** (§2, the seqnum decision) because it
touches the shared `common` crate; everything else is settled.

---

## 1. Roaring spike — ✅ resolved, no risk

**Question:** is `RoaringTreemap` the right posting substrate (RFC 0005 Q2), and
is its portable serialize/deserialize stable, cheap, and ascending-ordered?

**What I did:** added `roaring = "0.10"` (matches the opendata workspace pin at
`opendata/Cargo.toml:45`; resolves to `roaring 0.10.12`) and wrote
`tests/roaring_spike.rs` — a throwaway harness that round-trips the portable
format, asserts ascending iteration, exercises the set algebra, and measures
serialized size against a naïve 8-bytes-per-uid ("UidPack strawman") baseline.

**Results** (all green, `cargo test --test roaring_spike`):

| adjacency shape | cardinality | roaring bytes | naïve 8B/uid | ratio |
|---|---:|---:|---:|---:|
| dense `0..10000` | 10 000 | 8 220 | 80 000 | **0.103** |
| sparse scatter over 100M | 10 000 | 32 204 | 80 000 | **0.403** |
| clustered runs | 50 062 | 100 464 | 400 496 | **0.251** |

- Portable `serialize_into` / `deserialize_from` round-trips **identically**,
  including across the 32-bit bucket boundary (`u32::MAX`, `u32::MAX+1`, `u64::MAX`).
- Iteration is **strictly ascending** — the RFC 0003 ordering contract the whole
  read path leans on.
- Set algebra (`&`, `|`, `-`, `len`, `min`, `max`) behaves; the full
  set-algebra-vs-`BTreeSet`-oracle proptest (RFC 0005 acceptance #2) is
  deliverable #1's first test.

**Verdict:** roaring is confirmed as the one posting representation everywhere.
Compression beats the naïve baseline by 2.5×–10× on realistic shapes; even
worst-case sparse-random stays at 0.4×. The `format` tag (RFC 0005) keeps
UidPack/CSR an additive change if RFC 0017 ever shows a hot spot. Committed as
`adb77b4`.

---

## 2. Logical-seq protocol — the decision that shapes the token/gate contract

**Question (RFC 0004 §"Logical sequence protocol"):** turbolay's session token is
its *logical* seq. RFC 0004's design commits each batch with
`WriteOptions { await_durable: true, seqnum: latest_seq }` so the logical seq and
SlateDB's durable seq are *the same number* — which makes the RFC 0001 reader
gate (`durable_seq >= token`) trivially correct. The handoff flagged that
`common::WriteOptions` has no `seqnum` field, and left the call open: **(A)**
extend `common` to forward an injected seqnum, or **(B)** decouple and let the
token be SlateDB's own seq.

### New evidence (this is what unblocks the decision)

The handoff assumed seqnum injection might not be supported downstream. **It is.**

- **SlateDB already supports user-injected seqnums.** `slatedb::WriteOptions` has
  a `seqnum: u64` field (`slatedb/src/config.rs:467`): *"when non-zero, the
  provided value is used instead of the internally generated sequence number; the
  value must be strictly greater than the current maximum sequence number or the
  write fails with `InvalidSequenceNumber`."* The write path honours it
  (`batch_write.rs:205`: `if options.seqnum > 0 { reject if <= last_seq;
  advance_last_seq(seqnum) } else { oracle.next_seq() }`) and upstream ships tests
  for it (`test_user_defined_seqnum`, `..._rejects_lower_value`).
- **The only missing link is in `common`.** The `SlateDbStorage` adapter builds
  `SlateDbWriteOptions { await_durable, ..default() }` at three sites
  (`common/src/storage/slate.rs:365, 388, 411`), leaving `seqnum: 0`
  (auto-generate). `common::WriteOptions` (`common/src/storage/mod.rs:139`) has
  only `await_durable`.
- **`WriteResult { seqnum: u64 }` already returns the assigned seq**, and
  `subscribe_durable() -> watch::Receiver<u64>` already exposes the durable
  watermark in that same space — so option B is *also* fully wired today.

### The two options, concretely

**Option A — inject (RFC 0004's design). One clock.**
- Change to `common` (additive, backward-compatible): add `seqnum: u64` to
  `WriteOptions` (default `0`), forward it into `slate_options.seqnum` at the 3
  adapter sites. Default `0` ⇒ **every existing caller (vector, timeseries, …) is
  byte-for-byte unchanged.**
- turbolay commits each batch with `seqnum = logical_seq`. Logical seq **is** the
  durable seq. Session token = logical seq. Reader gate `subscribe_durable() >=
  token` is correct with the token as-is; the token also correlates directly to
  `Log[seq]` for the changelog-tail scan.
- Requirement: the injected seq must strictly exceed SlateDB's current max at all
  times. Single-writer monotonic `next_seq` guarantees this **provided** we seed
  the logical seq above SlateDB's initial max on a fresh DB (start at ≥1) and
  resume above the durable max on recovery. Both fall out of the RFC 0004 recovery
  scan (`Meta["latest_seq"]` + allocator reload).

**Option B — decouple. Two clocks.**
- No change to `common`. Logical seq lives only in `Meta["latest_seq"]` / `Log[seq]`
  (dense, gap-free — good for deterministic changelog replay). Session token =
  `WriteResult.seqnum` (SlateDB's own). Reader gate uses that token.
- Cost: two independent clocks. The client's token no longer correlates to a
  `Log[seq]` entry; the reader holds a durable-space freshness token *and* a
  logical-space index watermark `W`. They're used for orthogonal jobs (gate =
  "is it physically durable", tail-scan = "what changed since my index
  watermark"), so it's *correct*, but it's two things to reason about instead of
  one.

### Recommendation: **Option A (inject).**

1. The `common` change is ~4 lines, additive, and provably behavior-preserving
   for every other consumer (default `0`). Blast radius is effectively nil.
2. One clock is the entire point of RFC 0004's logical-seq design: the token is
   meaningful, the reader gate is a one-liner, and there's no logical↔durable
   translation layer to get subtly wrong.
3. SlateDB supports it first-class and tests it upstream — we're using a
   sanctioned feature, not bending the substrate.
4. It is what RFC 0004's *final contract* already specifies ("committed with
   `seqnum = latest_seq`"). B would be a deviation from a locked RFC.

**The one risk to close with a test:** whether SlateDB advances the oracle seq for
any *non-user* write (compaction/checkpoint bookkeeping) *between* our injected
writes — which wouldn't break strict-increase but could break the logical==durable
*identity*. Mitigation: we inject on every write and `advance_last_seq` to our
value, and resume from the durable max on recovery. RFC 0004 acceptance #6
(atomic fan-out in exactly one seq) plus a targeted "injected seq survives a
compaction cycle" test cover it. Low risk; upstream's own tests suggest the
oracle only advances on user writes.

### Why this needs your nod

Option A edits the shared `common` crate that the sibling opendata projects
depend on. It's additive and safe, and it's the RFC's design — but I don't want to
mutate shared code on my own authority. **If you approve A, I'll make the `common`
change first (with a test proving default-`0` is a no-op for existing callers),
then build `write.rs` on the one-clock contract.** If you'd rather not touch
`common` at all in M1, I'll take B and keep the two-clock contract behind the
token abstraction so we can collapse to A later without a client-visible change.

---

## 3. What's queued behind this decision

Both spikes are done; the roaring dep is in and committed. The decision above is
the only thing gating `write.rs`. Once it's settled, the build order is the
handoff's deliverables 1→6:

1. `posting.rs` — `PostingValue` + the set-algebra-vs-oracle proptest (north star).
2. `value.rs` — `NodeRecord`/`NodeCodec`/`ChangeRecord`/`TypedValue`, 1 MiB cap.
3. `merge.rs` — fill the stub (roaring union + i64 sum + fail-closed).
4. `posting_ops` — add/delete/split/rollup + `neighbors`.
5. `write.rs` — the 4 ops → one atomic batch (built on the §2 contract).
6. recovery-on-open + the RFC 0004/0005 acceptance suites.

Execution: I own the coupled core (posting/merge/write); the independent leaves
(`value.rs`/`TypedValue`, posting-ops tests, recovery tests) fan out to ≤5
subagents; commit as I go; subagent review at the end.

### Integration notes already banked for the build

- **`resolve_or_create_xid` must move into the batch.** M0's version does its own
  eager `storage.put` (`ids.rs:157`). RFC 0004 needs the xid mapping + node record
  + seq-block record to commit atomically with the write — refactor it to *return*
  `RecordOp`s the caller folds into the request batch.
- **Deletes are `RecordOp::Delete(Bytes)`** — there is no `DeleteRecordOp` type.
- **Fault injection = `common::storage::in_memory::FailingStorage`** (`test-utils`
  feature), `fail_apply` / `fail_apply_once`. ⚠️ `merge_with_options` is **not**
  gated by a fail slot — crash-recovery tests must drive the **`apply`** path
  (which is what the write path uses anyway).
- **Register the same `GraphMergeOperator` on every reader/compactor** — pass it
  via `StorageSemantics` to `create_storage_read`, or merge-operand reads fail.
