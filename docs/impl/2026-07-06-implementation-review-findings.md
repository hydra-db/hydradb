# turbolay M1 implementation review — findings (2026-07-06)

Findings-only review (no fixes) ahead of the FalkorDB benchmark. Four sonnet
reviewers, two waves of two:

- **R1** storage-model correctness (posting / posting_ops / merge)
- **R2** write path & crash recovery (write / ids / value / serde)
- **R3** vendored-fork deltas + plumbing/hygiene (vendor/common, obs, error, storage)
- **R4** adversarial hazards + test-coverage (whole src/ + tests/)

Every finding below was **re-verified by the orchestrator against the code**
(file:line read) before landing here. Fixes are a separate, approved step.

## Bench-blocking? — NO

The benchmark reads via `posting_ops::neighbors` → `resolve_whole_set` → `merge`.
R1 traced every `RecordOp::Merge` producer and confirmed the operand-format
asymmetry that bit in commit 6c32e2c (adjacency = `PostingValue`-framed vs Meta
bitmaps = bare `RoaringTreemap`) is **fully contained** — no other leak. The
`neighbors()` algorithm (resolve → subtract `deleted_edges` → subtract
`deleted_nodes`) matches RFC 0005 exactly. **No confirmed correctness bug in the
read/merge/posting path is reachable by the benchmark's single-writer,
load-then-query workload.** The bench numbers will not be corrupted by a
read-path defect. Two items below are *bench-relevant for the S3 tier* (perf
noise / ingest robustness), flagged inline: R3-1 and R4-1.

---

## HIGH

### H1 — `intern()` poisons the schema cache on an aborted write (R2-1) — confirmed
**`src/write.rs:618-663` (`intern`), 338 + 348-358 (`upsert_node` order).**
`intern` mutates the in-memory `SchemaCache` (line 661) and queues its
`SchemaName`/`SchemaId` `Put`s *before* the caller's batch commits; on a later
cache **hit** it returns the id and emits **no ops** (lines 619-621). But
`upsert_node` interns prop names (line 338) *before* the oversize-node cap check
that aborts the whole op committing nothing (lines 348-358). Sequence:
1. `upsert_node("doc:huge", {"blob": <2 MiB>})` — `intern` allocates `PropId(N)`,
   writes the cache, queues (never-committed) schema `Put`s; cap check aborts.
2. `upsert_node("doc:small", {"blob": 5})` — cache **hit** → returns `N`, no
   schema ops → commits a `Node` referencing `PropId(N)` whose `SchemaName`/
   `SchemaId` records were **never persisted**.
After restart, `SchemaCache::rebuild_from_storage` scans `SchemaId` records; `N`
is absent → the name is unresolvable, and a fresh writer can re-intern `"blob"`
to a different id. Violates RFC 0004 acceptance #1 ("no partial batch"): cache
state escapes the commit boundary and is never rolled back. Contrast
`ids::resolve_or_create_xid_batched`, which re-checks storage every call and is
safe under the identical failure shape. *Not bench-blocking* (the bench writes
no oversize nodes), but a genuine write-path atomicity bug.

### H2 — post-commit `maybe_split` error is returned as if the write failed (R4-1) — confirmed
**Status:** fixed in `9a8bef4` with a regression that injects a post-commit
split-read failure and verifies `upsert_edge` returns the committed seq.

**`src/write.rs:491-498`.** `upsert_edge_inner` commits the logical write (durable
`seq` obtained), then runs `maybe_split` on both endpoints with `?` propagating
any error straight out — contradicting the function's own doc ("A failure here
does not un-commit the logical write above, and is safe to retry/ignore"). A
transient storage error in the post-commit split RMW makes `upsert_edge` return
`Err` even though the edge is live. `bump_degree` is documented **non-idempotent**,
so a caller that retries the "failed" write skews the degree counter. No test
covers this (`FailingStorage` only targets the primary batch).
**Bench-relevant (S3 tier):** on real S3, a transient split-RMW hiccup would
surface as a spurious ingest failure; the Part B batched-ingest path must decide
split handling deliberately (log-and-continue, not propagate).

### H3 — `InstrumentedObjectStore` omits `list_with_offset` and `rename_opts` (R3-1) — confirmed
**Status:** fixed in `6ef15d7` with a regression that fails if the wrapper falls
back to `list()`+client filtering or `copy_opts()`+`delete`.

**`src/obs.rs:207-284`.** The wrapper overrides 7 `ObjectStore` methods but not
`list_with_offset` or `rename_opts`, so both fall through to the trait defaults
(client-side `list()`+filter; `copy_opts`+`delete`). SlateDB's `RetryingObjectStore`
calls both directly on the inner store, so they reach turbolay's wrapper
undecomposed. Impact: (a) on S3/GCS/Azure every offset listing (compaction /
manifest-recovery SST scans) degrades to an unbounded prefix list + client
filter — a perf cliff at scale; (b) on local disk, `rename` loses filesystem
atomicity in favor of copy+delete, changing crash-safety in exactly the tier
`tests/slatedb_acceptance.rs` exercises. `object_store`'s trait doc and SlateDB's
own private `InstrumentedObjectStore` both forward all methods.
**Bench-relevant (S3 tier):** may inflate S3 cold/load/compaction latency in a
way attributable to the wrapper, not the engine — note as a caveat, consider
fixing before Tier 3.

---

## MEDIUM

### M1 — no compare-and-swap outside the changelog seq; concurrent writers duplicate uids (R2-2, R3-2) — plausible
**`src/ids.rs:198-207`, `vendor/common/src/sequence.rs:202-214`, `src/error.rs:73-77`.**
Only the injected changelog `seqnum` is monotonicity-checked. Xid/Node/adjacency/
schema/block-reservation writes are plain `Put`/`Merge`. Two live `Writer`
sessions on one namespace can mint the same uid for different xids (last-write
-wins, silent loss). **Mitigation:** real SlateDB single-writer fencing covers
this and *is* tested — `should_fence_a_zombie_writer_on_next_commit`
(`tests/slatedb_acceptance.rs:157`). The residual gap is defense-in-depth only:
`error.rs:73-77` collapses `StorageError::Fenced` into `Error::Storage(String)`,
so the typed `Fenced` variant the fork added has **zero consumers** and
`write.rs` has no fencing-aware reopen. Not bench-blocking (single writer).

### M2 — torn read between a `Split` manifest and its parts (R1-2) — plausible, dormant
**`src/posting_ops.rs:219-291` + `src/storage.rs:97-100`.** `GraphStorage::get`
pins no per-call snapshot; `neighbors()` reads the manifest then one `.get()` per
part sequentially. Since `write.rs` runs `maybe_split` after *every* `upsert_edge`,
a future concurrent reader could observe a manifest naming a part key that a
racing split already rewrote → spurious `Error::encoding("... key is missing")`.
Dormant today (no concurrent read path yet; the bench is load-then-query,
single-threaded) — must bind manifest+part reads to one snapshot before the M2/M3
read path lands.

### M3 — `maybe_rollup` is designed but never wired into the write path (R4-2) — confirmed
**`src/posting_ops.rs:600-607` called only from its own tests.** `write.rs` calls
`maybe_split` after every edge but never `maybe_rollup`. `Meta["deleted_edges"]`
bitmaps grow monotonically under delete churn with no compaction trigger;
`neighbors()`'s per-read subtraction cost grows unbounded, and postings that
shrink never re-consolidate. Claimed-vs-actual RFC 0005 gap. Not bench-blocking
(no deletes in the bench).

### M4 — no CI configuration in the repo (R3-4) — confirmed
No `.github/` at all; the fmt/clippy/test "gauntlet" is manual-only. For a
4-member workspace with a vendored fork under active modification, an untested
regression can reach `main` unblocked. Recommend a minimal `ci.yml`.

---

## LOW

- **L1 — `find_target_part` panics on a zero-part `Split` (R1-1 / R4-3) — confirmed.**
  `src/posting_ops.rs:123-133`: `.unwrap_or(&parts[0])` eagerly indexes `parts[0]`,
  so an empty-parts `Split` panics in **release too** (the `debug_assert!` only
  improves the debug message). Root cause: `PostingValue::deserialize` accepts
  `part_count = 0` as well-formed (`src/posting.rs`). Reachable only via corrupt
  storage; inconsistent with sibling `read_part_set` which returns `Err`. Not the
  deliberate merge fail-closed panic.
- **L2 — duplicated Meta constants (R1).** `META_DELETED_NODES` /
  `META_DELETED_EDGES_PREFIX` hand-duplicated in `src/posting_ops.rs:62-63` **and**
  `src/serde/keys.rs:328-329`; nothing enforces agreement — an edit to one
  reclassifies a Meta sub-key and trips the merge fail-closed panic.
- **L3 — stale "fencing untestable" comment (R3-3 / R4-5).** `src/write.rs:1398-1414`
  claims fencing is wholly untested; false since `slate.rs`'s `Fenced` unit test
  and `slatedb_acceptance.rs:157` landed. Update/remove.
- **L4 — node cap checked after full encode (R2-3).** `src/value.rs:149-169`
  builds the whole `BytesMut` before the `> cap` check; a large transient
  allocation precedes rejection. Minor.
- **L5 — `decypher` is a declared dep with zero uses (R3-5).** `Cargo.toml`; pulls
  ~20 transitive crates for M3 pre-wiring not yet consumed. Defer or feature-gate.
- **L6 — release fail-open on id/degree overflow (R4-6, R4-7).** `ids::to_u32`
  (`src/ids.rs:59-66`) and `merge_i64_sum` (`src/merge.rs:249-258`) wrap silently
  in release (panic in debug) — inconsistent with the codebase's fail-closed
  posture. Impractical cardinalities; flagged for consistency.
- **L7 — delete_node stale-read surface (R4-4).** `delete_node` filters neither
  `EdgeProp` companions nor degree counters, so `edge_props()`/`degree()` disagree
  with `neighbors()` for a tombstoned endpoint. EdgeProp orphan is documented
  (RFC 0012 scope); the degree-counter half is an undocumented extension.

## Verified sound (ruled out)
Operand-format asymmetry fully contained (R1); codecs fail closed on
truncation/unknown-tag/trailing bytes (R2); key & token ordering order-preserving
and proptest-verified (R2); atomic fan-out happy+failure paths
`FailingStorage`-tested (R2); EdgeProp keyed by full edge identity, immune to
split/rollup (R2); vendor fork delta is minimal and purely additive vs pristine
`opendata-common` 0.1.17, 5 files, rebasable (R3); seqnum forwarding + `Fenced`
mapping + `with_object_store` seam all correct (R3); all `merge.rs` panics are the
intended fail-closed design, not bugs (R4).
