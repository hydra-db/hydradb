---
title: "turbolay — M1 Gap-Closure Plan (Wave 4, finalized)"
date: 2026-07-05
kind: plan
status: implemented 2026-07-06 (commits 859e4a0, 6c32e2c, e20334d, 3091ebd)
---

# M1 Gap-Closure Plan (Wave 4) — finalized

> **Implemented 2026-07-06.** All three workstreams landed and were integrated,
> reviewed, and committed on `main` with the full gauntlet green
> (`cargo test` + `clippy --all-targets -D warnings` + `fmt --check`):
>
> - **A — `EdgeProp` companion record** → `859e4a0 M1 D7: EdgeProp companion record`.
> - **B — SlateDB acceptance tier** → `e20334d M1: SlateDB acceptance tier`,
>   preceded by `6c32e2c M1: fix adjacency merge operand format for real
>   SlateDB` — B's honest exercise of the *real* backend surfaced a genuine
>   pre-existing merge-operator bug the in-memory backend had masked (adjacency
>   fast-add operands were bare `RoaringTreemap` while output/stored form was a
>   header-framed `PostingValue`; SlateDB's `MergeOperatorIterator` re-feeds a
>   no-existing-value output back as an operand, so the two formats must match).
>   Committed separately as the correctness fix it is.
> - **C — RFC 0017 Phase 0 observability spine** → `3091ebd M1: RFC 0017 Phase 0
>   observability spine`. Invariant counters wired honestly: full 9-kind §3.7
>   taxonomy registered, only `merge_rejected` wired (the merge fail-closed
>   panics — the one genuine M1 site); every other kind register-only with its
>   candidate M2 site, no fabricated checks.
>
> **M1 is now fully closed, including the RFC 0017 Phase 0 debt.** What remains
> below the fold is unchanged: **M2** (index framework, RFC 0006) is next, and
> **RFC 0017 Phase 3** (real-S3 benchmark harness) is still the hard gate before
> any optimization RFC (0009/0010) — tracked, not part of this wave.

M1 write path is green (commits `440145b` int#1, `dc256ac` D3, `4b1abb0` D4,
`7bb05d6` D5/D6; 127 lib + 35 integration tests, clippy/fmt clean). This plan
closes the remaining honest M1 gaps. Revised 2026-07-05 after a code-verified
review: every claim below was checked against `src/`, `vendor/common`,
slatedb-0.14.1 source, and RFCs 0004/0005/0017 — the open decisions of the
first draft are now **resolved** inline.

## The gaps being closed

| Gap | Why it's open | RFC |
|---|---|---|
| **A. Valued/faceted edges** — `EdgeProp` companion record | Edges are plain set membership today; the `EdgeProp` tag was never allocated. RAG-KG `RELATES` edges carry properties. | 0005 acc #6 |
| **B. Backend-dependent acceptance tests** — fencing, durable gate, crash-recovery | In-memory `common` can't fence, never drives the durable-seq watch, and can't truncate a batch. All three need the real SlateDB backend. | 0004 acc #1/#2/#3 |
| **C. RFC 0017 Phase 0 observability** — instrumented ObjectStore, write-path timers, invariant counters | Only the tracing-subscriber scaffold exists (`telemetry.rs`, 35 lines); `tracing::` fires in exactly 3 places, all `main.rs`. RFC 0017 §8 says Phase 0 "lands with M0/M1 code" — it hasn't. | 0017 Phase 0 |

A and B are **independent** (disjoint files) and run as two parallel Sonnet
agents. **C runs after A** — both touch `write.rs` (A adds the EdgeProp
fan-out, C instruments the fan-out steps), and C should instrument the final
shape including EdgeProp. I integrate + verify + commit after each.

---

## Workstream A — `EdgeProp` companion record (valued edges)

**Design:** valued/faceted edges keep their properties in a separate companion
record, not inline in the posting. Adjacency stays pure set membership; the
property blob is a `Put` record keyed by edge identity.

**New record type:** `EdgeProp = 0xC` — verified next free tag
(`record_tag.rs:141`: `0xC..=0xF` unassigned; `Meta = 0xB`).

**Key layout (A1 — resolved):** `[src: u64 BE][pred: u32 BE][dst: u64 BE]` →
bincode-encoded `BTreeMap<PropId, TypedValue>` (A2 — resolved: BTreeMap, dedup
+ deterministic order, consistent with `NodeRecord.props`). Stored **once per
edge identity**, no direction byte.

> **This amends RFC 0005.** RFC 0005 §"Edge facets" (line 49) specifies
> `EdgeProp[dir][anchor_uid][pred_id][neighbor_uid]` — per-projection, two
> copies. We deliberately diverge: a single copy avoids double-write and
> update anomalies between projections. The trade, named: facets are point
> lookups from either projection (flip to `(src,pred,dst)` from the
> in-projection), but an in-direction *prefix scan* over facets is off the
> table. The RFC 0005 text edit is part of this workstream.

**ChangeRecord (A3 — resolved: no wire-format change).**
`ChangeRecord.value` is `Option<TypedValue>` — a single scalar, documented
"for node ops" (`value.rs:266`) — and `ChangeOp` has no valued-edge variant.
It **cannot** carry a props map, and the changelog format is replayed by M2's
changelog-tail merge + index backfill, so changing it is not a throwaway
clause. Decision: `upsert_edge_with_props` emits the **existing** `UpsertEdge`
change record unchanged; `(subject_uid, pred_id, object_uid)` identifies the
edge, and since `EdgeProp` is Put-only, a replayer at seq N reads the
companion record directly. Zero format risk for M1; revisit only if an M2
consumer proves it needs inline props.

**Files touched:**
- `src/serde/record_tag.rs` — `EdgeProp = 0xC`: enum variant, `from_tag_byte`, `ALL`, rustdoc tag table.
- `src/serde/keys.rs` — `edge_prop_key(src, pred, dst)` + `parse_edge_prop_key` inverse pair + `edge_prop_range(src, pred)` prefix scan (out-projection facet scan).
- `src/value.rs` — `EdgeProps` container + codec (reusing `TypedValue`).
- `src/write.rs` — new `upsert_edge_with_props(src_xid, pred, dst_xid, props)`: writes the `EdgeProp` `Put` alongside the existing `EdgeOut`/`EdgeIn` fan-out in the **same batch**. **`upsert_edge` signature stays intact** (back-compat; B and all callers keep compiling). `delete_edge` additionally emits `RecordOp::Delete(edge_prop_key)` in the same batch (blind delete of a Put-only key is safe). Read side: `edge_props(src, pred, dst) -> Option<BTreeMap<PropId, TypedValue>>` — **a raw point-get, documented as such**: it does not consult tombstones.
- `src/merge.rs` — add `EdgeProp` to the **fail-closed** non-associative panic arm (`merge.rs:123` block; it's Put-only, must never receive a merge operand).
- `docs/rfcs/0005-posting-list-substrate.md` — amend §"Edge facets" key shape (see above).
- `docs/rfcs/0012-vacuum-and-gc.md` (stub) — one line added to scope: **vacuum must reap `EdgeProp` records for tombstoned edges/nodes**.

**Semantics pinned (so the agent doesn't improvise):**
- Plain `upsert_edge` never touches the companion — re-upserting a valued edge **preserves** its props.
- `upsert_edge_with_props` with an **empty map** behaves as plain `upsert_edge` — writes no companion (keeps RFC 0005 acc #6's "a plain edge writes no EdgeProp" clean).
- `delete_node` does **not** cascade (tombstone model, RFC 0004 rejects eager cascade) — EdgeProp records of incident edges stay live-but-unreachable until vacuum (RFC 0012). Documented, not fixed here.

**Tests (RFC 0005 acc #6):**
- Round-trip: write valued edge → read props back.
- Atomicity: props commit in the one edge batch (same seq).
- `delete_edge` removes the companion.
- Plain re-upsert of a valued edge preserves props; empty-map upsert writes no companion; plain edge writes no companion.
- `edge_prop_key` inverse-pair round-trip + ordering (keyspace proptests).

---

## Workstream B — SlateDB-backed acceptance tier

**Goal:** new `tests/slatedb_acceptance.rs` running the three RFC 0004
acceptance items unreachable on the in-memory backend, against a real
SlateDB-backed `GraphStorage`.

**Backend (B1 — resolved: tempdir, memory is impossible).**
`create_object_store` constructs a **fresh** `object_store::memory::InMemory`
on every call (`vendor/common/src/storage/factory.rs:332`) and
`StorageBuilder` bakes it in with no injection seam — two opens with
`ObjectStoreConfig::InMemory` get two disconnected empty stores. Fencing and
crash-recovery both require two opens over the *same* store, so:
`StorageConfig::SlateDb` over `ObjectStoreConfig::Local` on a **`tempfile`
tempdir**. `tempfile` dev-dep is **required** (workspace already ships it via
vendor/common's dev-deps). This is NOT LocalStack and does not violate RFC
0017 D12 — SlateDB correctness over a local store, not S3 benchmarking. Tests
use `#[tokio::test(flavor = "multi_thread")]` (SlateDB background tasks).

**Fenced-error surface (B2 — resolved: extend the vendored fork).**
slatedb 0.14.1 has the typed error (`SlateDBError::Fenced`,
`CloseReason::Fenced` — its `error.rs:115,423`), but `common` flattens every
error to `StorageError::Storage(String)` via `Display`
(`storage/mod.rs:170-175`; `from_storage` throughout `slate.rs`). RFC 0004
acc #2 says "assert `CloseReason::Fenced`" — unreachable through today's API.
Decision: add a typed **`StorageError::Fenced`** variant to the vendored fork
(its stated purpose per Cargo.toml: "so turbolay can extend the storage
layer"), mapped in `slate.rs`'s error conversion wherever the slatedb error is
`Fenced`/closed-with-`CloseReason::Fenced`. ~15 lines + its unit test. No
string matching against another crate's `Display` output.

**Durability premises the tests may assume (verified):**
- `Writer::commit` uses `await_durable: true` (`write.rs:506`) — **every
  returned seq is already WAL-durable**. Consequence for #3: committed data
  survives drop by construction. Consequence for #2: the fence surfaces
  deterministically on writer1's next commit (it awaits the WAL), not
  asynchronously.
- `close()` is an explicit async fn (`slate.rs:265`), **not** run on Drop —
  so "drop without close" is a legitimate crash simulation.

**Files touched:**
- `tests/slatedb_acceptance.rs` — new, the whole tier.
- `vendor/common/src/storage/mod.rs` + `slate.rs` — the typed `Fenced` variant (B2).
- `Cargo.toml` — `tempfile` dev-dep.
- `src/storage.rs` — only if a minimal helper is genuinely needed; prefer none.

**Tests:**
1. **Durable gate / RYW (0004 #3)** — write, `subscribe_durable()`, assert the
   watch advances to `>= token` and the write is visible. The writer-side
   assert is near-tautological given `await_durable: true` — the *value* here
   is the subscriber view (the gate a reader will use).
2. **Zombie-writer fencing (0004 #2)** — writer1 over the tempdir + write;
   writer2 over the same path (new epoch); assert writer1's next commit fails
   with **`StorageError::Fenced`** (typed, per B2) and writer2 continues the
   seq lineage from persisted `latest_seq` (mind the +1 seeding,
   `write.rs:484-489`; recovery-on-open from `7bb05d6` handles it — assert
   monotonicity across the handover).
3. **Crash-recovery (0004 #1)** — write via writer1, **drop without close**,
   reopen a fresh Writer on the same path, assert committed data +
   `latest_seq` + allocators recovered and seqs stay monotonic. (Valid
   because commit awaits durability — state this in the test doc-comment.
   The existing `FailingStorage` fault-injection test covers mid-batch
   truncation; this tier adds real WAL replay + manifest + allocator
   recovery from persisted state.)

**Honesty rule stands:** if any item can't be reached via `common`'s public
API even after B2, implement what's possible and report the precise gap — do
not fabricate a pass.

---

## Workstream C — RFC 0017 Phase 0 (observability spine) — runs after A

**Scope is exactly RFC 0017 §8 Phase 0**, nothing more: `metrics` facade
wiring, instrumented ObjectStore wrapper, write-path fan-out timers,
`turbolay_latest_seq`, error/outcome counters, invariant counters, naming/
cardinality rules. Phase 1 (metric matrix) stays an M2 exit criterion; the
Prometheus exporter lands with the HTTP plane (M3, RFC 0008) — Phase 0 is
recording sites only, exporter-agnostic by design (RFC 0017 principle #2).

**Files touched:**
- `vendor/common/src/storage/factory.rs` — add a `StorageBuilder::with_object_store(Arc<dyn ObjectStore>)` override so a caller can supply a (wrapped) store instead of the config-built one. Small seam, fork is ours. *(Side benefit: gives tests a shared-in-memory-store option later; B still uses tempdir to exercise the real config path.)*
- `src/obs.rs` — new: the instrumented `ObjectStore` wrapper (delegates to inner, times every op) emitting `turbolay_objstore_request_duration_seconds{op}` / `_requests_total{op, outcome}` / `_bytes{op, direction}` (RFC 0017 §3.1); metric name constants; cardinality rules doc-comment (labels: op/phase/outcome enums only — never UIDs, xids, tokens, seqs).
- `src/storage.rs` — `GraphStorage::open` builds the store via the seam, wrapped with the instrumented wrapper.
- `src/write.rs` — a `WriteStats` struct threaded through the fan-out, **emitted once at completion** (RFC 0017 principle #4): phase timers with §3.2's verbatim labels (`encode_node|encode_out|encode_in|index_fanout|batch_commit|latest_seq` — `index_fanout` will be ~0 until M2, recorded anyway so the taxonomy is stable) + `turbolay_latest_seq` gauge on commit.
- `src/telemetry.rs` — unchanged scaffold; add facade init for tests (a no-op recorder is fine — the `metrics` crate defaults to no-op when no recorder is installed, so library code just records).
- `Cargo.toml` — `metrics` dependency (workspace already has it via `common`'s `metrics_recorder`).

**Invariant counters (§3.7) — honest M1 subset.** Register
`turbolay_invariant_violations_total{kind}` with the full kind taxonomy, but
only wire the kinds M1 code paths can actually observe: `xid_uid_miss` (xid
resolves to a uid with no node record — the lookup path), `changelog_gap`
(detected during recovery-on-open's scan), and the merge fail-closed panic
path (count before panicking). `projection_asymmetry` and
`orphaned_index_entry` need a checker/read path — M2. Do **not** fake checks
to make counters move.

**Explicitly out of scope for C:** query phase timers (no read path yet),
slow-query log / `debug: true` (M3), the `m/obs_heartbeat` key (Phase 2,
needs an RFC 0003 keyspace registration first), the real-S3 benchmark harness
(Phase 3 — see "After Wave 4").

---

## Parallelization & file-conflict matrix

| File | A | B | C |
|---|---|---|---|
| `record_tag.rs`, `keys.rs`, `value.rs`, `merge.rs` | ✏️ | — | — |
| `write.rs` | ✏️ | — | ✏️ **(conflict → C after A)** |
| `tests/slatedb_acceptance.rs` (new) | — | ✏️ | — |
| `vendor/common` (`mod.rs`/`slate.rs` vs `factory.rs`) | — | ✏️ Fenced variant | ✏️ store seam (disjoint files, but C is sequential anyway) |
| `src/obs.rs` (new), `src/storage.rs`, `telemetry.rs` | — | — | ✏️ |
| `Cargo.toml` | — | tempfile | metrics |
| `docs/rfcs/0005`, `docs/rfcs/0012` (amendments) | ✏️ | — | — |

## Sequencing

1. **Spawn A ∥ B** (two Sonnet agents; ≤3-subagent cap respected). Each must
   end `cargo test` + `cargo clippy --all-targets -- -D warnings` +
   `cargo fmt --check` clean; **no commits** — I verify + commit each
   (`M1 D7: EdgeProp companion record`, `M1: SlateDB acceptance tier`).
   Watch for Wave 3's failure classes: unused imports under `-D warnings`;
   any SlateDB test that can hang → bounded monitor on test runs.
2. **Spawn C after A is committed** (one agent, same green-tree bar; commit
   `M1: RFC 0017 Phase 0 observability spine`).
3. Update the impl scratchpad: M1 fully closed **including** the RFC 0017
   Phase 0 debt; note what remains below.

## After Wave 4

- M1 closed → **M2** (RFC 0006/0001): `IndexAm` trait + registry + watermark,
  value/label/count indexes, reader freshness gate + changelog-tail merge.
  (M2 must also wire `index_fanout` timing + Phase 1 metrics per RFC 0017.)
- **RFC 0017 Phase 3 (real-S3 benchmark harness) is still outstanding** and
  remains the hard gate before any optimization RFC (0009/0010) starts —
  tracked, not part of this wave.
