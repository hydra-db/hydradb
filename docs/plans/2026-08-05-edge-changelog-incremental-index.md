---
title: Edge changelog (xlog) — incremental index without fallback
status: step-1-complete
date: 2026-08-05
branch: feat/xlog-incremental-index
base_commit: 6ff5203
tags:
  - graph-indexer
  - incremental-index
  - slatedb
---

# Edge changelog (xlog): the definitive incremental index

## TL;DR

The incremental graph index build regressed on staging because it derives its delta by re-reading SlateDB's WAL, which is fragmented into one tiny S3 object per ~100 ms of wall-clock time — so the cost of finding a 15-edge delta is thousands of GET requests, priced by elapsed time rather than by delta size. The fix is to stop reconstructing the delta after the fact and instead **record it as ordinary data at commit time**: every transaction that changes topology also writes one small key per changed edge, stamped with its exact commit sequence, into the same keyspace, same WriteBatch, and same WAL object the mutation already pays for. Compaction then folds these entries into the same large sorted SSTs that make the full scan fast, so "which edges of type T changed since the last build" becomes a single bounded range scan: O(delta) bytes in a handful of block reads, independent of write cadence, with no fallback path. This is the same pattern production databases use for exactly this problem — Postgres logical replication (commit-ordered change stream + slot-tracked retention), Debezium's transactional outbox (change record written in the same transaction as the data), and Neo4j (indexes updated as commands inside the data transaction's commit) — prior-art mapping and references at the end of the document.

## Scope and phasing

This document specifies **Phase 1** in full and **Phase 2** to design-review depth. They share one architecture — Neon's split between a change feed and layered storage — and Phase 1 is a strict prerequisite for Phase 2; none of its code is throwaway.

- **Phase 1 — the xlog (implement now).** Capture the delta at commit time; derive it at build time with one bounded range scan. This kills the request-bound regression: everything the build *reads* becomes O(delta). The artifact *write* (CSC re-encode + PUT) stays O(graph).
- **Phase 2 — delta-shaped artifacts (implement after Phase 1's scale benches gate).** Publish compact delta artifacts between periodic full CSC images — Neon's delta-layer/image-layer split applied to our generations — making the artifact write O(delta) too, with the O(graph) re-materialization amortized across many cycles. Specified in the "Phase 2" section below.

## Sources

- `interactive/incremental-build-cost.html` — the full diagnosis of the staging regression ("Requests, Not Bytes"): why the WAL-tail walk is priced per *request* while the full scan is priced per *byte*, the measured ledger, the break-even calculator, and the L0–L3 ladder this plan supersedes.
- `src/engine/index_store.rs:198` — `build_graph_index_incremental`, the function whose delta *source* this plan replaces. The overlay application, re-encode, and content-addressed publish all stay exactly as they are.
- `src/codec.rs:143` — `commit_txn_strict_with_sequence`, the commit path whose pinned sequence number is the correctness hinge of this design.
- Memory: `turbolay-project-context.md` §"Staging regression + fix (2026-08-05)".

## The problem, with numbers

Staging telemetry dates the regression precisely. Before the incremental deploy, `artifact.build` averaged 0.4–1.1 s per build (Jul 29 – Aug 2). After it: 27.0 s average on Aug 4 (max 274 s) and 30.9 s on Aug 5 (max 397 s), with daily span volume up ~10× (12–24 k → 160–216 k). Local benchmarks showed the opposite because local load was heavy and fast: many edges per 100 ms flush window means few, fat WAL files. Staging's trickle workload means the same delta is scattered across thousands of near-empty files. Same delta, 20× the price — the cost driver is *file count*, and file count is `elapsed_time / flush_interval`, not delta size.

Two approaches are on the table to be replaced, and both deserve to be:

- **The always-full build (today's deployed default, `GRAPH_INDEXER_BUILD_MODE=full`)** re-scans every canonical edge of every edge type of every registered scope, every cycle. Correct and cache-friendly, but O(graph) per cycle forever: a 10 M-edge graph that gained 40 edges still pays a 10 M-edge scan.
- **The WAL-tail incremental with cost-gated fallback (PR #23)** is a safety valve, not a solution. The walk costs one round trip per WAL file in the span, so precisely the workloads that need incremental most are the ones where the gate fires and we fall back to full. A feature that degrades to "off" under load isn't the feature.

The root defect both share: **the delta is not stored anywhere that can be read cheaply.** The WAL is a physical redo log owned by SlateDB — fragmented by time, shared by all 8 edge types, GC'd on a 60-second fuse. Asking it "which edges of type T changed since sequence B?" costs hundreds of GETs to answer a question it was never designed to answer.

**Why segment compaction doesn't already solve this.** Segments and their compaction (`maintenance.rs:209`) operate one layer below the problem, on the *canonical store*: they pack many edges into few keys and merge those runs, which makes the full scan's O(G) cheaper — but they are deliberately topology-preserving and sequence-invisible, so they cannot answer "which edges changed since sequence B". Worse for any diff-based alternative: compaction rewrites the physical layout *without* changing the logical edge set, so diffing storage state across builds is structurally broken — which is exactly why the change feed must be a *logical* changelog written at commit time, not something inferred from storage. And segments do nothing for the artifact side: the builder still derives, re-encodes, and uploads one full CSC per cycle regardless of how the canonical edges are packed underneath. The relationship runs the other way — Phase 2's base-image + delta-artifact chain is segment compaction's *shape* (small runs folded into a big run on a policy) applied at the artifact layer, which is why it should feel familiar.

## What the xlog stores, exactly

The xlog is not a marker, a pointer, or a flag — it **is the delta, stored as data**. One record per logical edge change, holding everything the index builder needs to patch the previous CSC matrix: which edge type, which edge, at what commit sequence, and whether the edge exists after that commit.

```
key   = cell/{cell}/xlog/{edge_type}/{seq:020}/{src:020}/{dst:020}
value = 0x01  → edge (src → dst) EXISTS after this commit   (insert, or re-insert over a delete)
        0x00  → edge (src → dst) is GONE after this commit  (delete)
```

A worked example. A transaction commits at sequence 4812 inside cell `acme-prod`, inserting FOLLOWS edge 42→9107 and deleting FOLLOWS edge 42→77. It writes, in that same transaction:

```
cell/acme-prod/xlog/FOLLOWS/00000000000000004812/00000000000000000042/00000000000000009107  →  0x01
cell/acme-prod/xlog/FOLLOWS/00000000000000004812/00000000000000000042/00000000000000000077  →  0x00
```

Each record is ~70 bytes of key + 1 byte of value. The three fields that carry information are all in the key, deliberately:

1. **`{edge_type}` before `{seq}`** — all changes for one edge type are one contiguous key run. Each of the 8 edge types sharing a scope DB scans only its own changes; the WAL walk re-downloaded the whole span once per type.
2. **`{seq:020}` zero-padded** — lexicographic order equals numeric order equals commit order. "Changes in `(B_prev, B]`" is the literal key range `xlog/{T}/{B_prev+1:020}` … `xlog/{T}/{B+1:020}`: one bounded range scan, no filtering, no post-sort.
3. **`{src}/{dst}` after `{seq}`** — each change is its own key, so nothing overwrites. This is the exact opposite of the dirty marker (see below), whose single overwritten key is *why* it can't store a delta.

The **value is final state, not operation**. We do not store "insert" / "delete" ops that must be replayed in order; we store what is true after the commit. Within a scanned range, the last entry in sequence order wins per `(src, dst)`, which makes overlay application idempotent and order-insensitive — the same contract `build_graph_index_incremental` already applies overlays under, and the same last-write-wins-per-key model Kafka log compaction uses. It also eliminates the per-edge existence-resolution point reads the WAL path needed, which were themselves request-bound.

**Deliberately out of scope: properties.** Edge and vertex properties live under their own key families (`emeta/`, `eprop_idx/`, `rprop_idx/`) and are not part of the CSC matrix. Property-only updates log nothing. The xlog records only what the index consumes — structural `(src, dst, exists)` per edge type. If a future index consumes properties, that index gets its own log family; this one stays minimal by design.

## Where the bytes physically go

Nothing in this plan creates files, talks to S3 directly, or adds requests to the write path. Every piece rides SlateDB's existing pipeline, which works like this for **every** key TurboLay writes today:

```
txn.put(key, value)      → appended to an in-memory WriteBatch (no I/O)
txn.commit()             → batch appended to the current WAL buffer
        every ~100 ms    → SlateDB flushes ONE WAL object to S3 containing every commit
                           from that window (time-based, not size-based — the root of
                           the staging file blowup)
        memtable full    → flushed as ONE L0 SST (large, sorted, indexed)
        background       → L0s compacted into deeper, even larger sorted runs
reads / scans            → merged view over memtable + SSTs, through the block cache;
                           big sequential ranges are cheap
```

**The dirty marker today** is an ordinary key in this same keyspace — `cell/{cell}/meta/matrix_dirty/{edge_type}` → epoch (8 bytes), one key per (cell, edge type), overwritten on every mutation, newest value wins. That is exactly why it "stores no data": it is a *cell*, not a *log*. It can say "FOLLOWS changed, most recently at epoch 4812" but never *which* edges changed, because each write destroys the previous one. The indexer polls it to decide *whether* to rebuild; it has nothing to say about *what* to rebuild.

**The xlog is the same mechanism with a key shape that refuses to overwrite.** Because the sequence and the edge are part of the key, every change is preserved as its own entry. Same keyspace, same WriteBatch, same WAL object, same memtable, same SSTs, same block cache — the dirty marker grown a tail.

**File-count math, since this was the original sin:** SlateDB cuts WAL files on the clock, not on payload. A commit carrying 7 keys and a commit carrying 9 land in the *same* WAL object; object count is `elapsed_time / flush_interval` either way. So the xlog adds **zero files** and **zero requests** — only bytes: ~72 per edge change, next to the ~300+ bytes the change already writes (canonical edge, reverse edge, degree counters, dirty marker, idempotency record). Compaction then folds the entries into the same large sorted SSTs the full scan reads. That is the entire trick: **the delta ends up stored in the shape that made the full scan fast.**

## How the builder consumes it

`build_graph_index_incremental` keeps its exact shape; only the `topology_tail_since` call is replaced:

```text
snapshot  = pin at base_sequence B            (unchanged)
previous  = current_graph_index()             (unchanged)
low_water = read cell/{cell}/meta/xlog_low/{edge_type}
if previous.base_sequence + 1 < low_water:
    → bootstrap full build                    (coverage breach — see GC)
delta = snapshot.scan(xlog/{T}/{prev_B+1:020} .. xlog/{T}/{B+1:020})
        last entry per (src, dst) wins        (already sequence-sorted)
apply delta to previous CSC's adjacency       (unchanged code)
re-encode, content-address, publish           (unchanged code)
```

The byte-identical oracle stands: a full rebuild at the same snapshot must produce the same payload hash. The existing equivalence test keeps working unmodified because the overlay contract is unchanged.

## Correctness

### The sequence stamp is exact — enforced by the engine, not by convention

A stamped log is only sound if stamp order equals visibility order. An entry stamped *below* its real commit sequence can land at or below a previous build's `base_sequence` while its commit is above it — excluded from that build's range and from every later one: a permanently lost edge. An entry stamped *above* its commit is picked up one generation late — not lost, but it breaks the byte-identical oracle.

TurboLay already closes this. `commit_txn_strict_with_sequence` (`src/codec.rs:143`) pins the commit sequence via `WriteOptions { seqnum: txn.seqnum() + 1 }`, and `next_epoch_txn` (`src/codec.rs:128`) hands write paths exactly `txn.seqnum() + 1` as `epoch`. On the engine side, `batch_write.rs:211` in the pinned slatedb fork commits at *exactly* the requested sequence or returns `InvalidSequenceNumber` — it can never silently assign a different one, and it rejects any sequence ≤ the current max, so sequences are strictly monotonic:

```rust
let commit_seq = if options.seqnum > 0 {
    let current = self.oracle.last_seq();
    if options.seqnum <= current {
        return Err(SlateDBError::InvalidSequenceNumber { provided, current });
    }
    self.oracle.advance_last_seq(options.seqnum);
    options.seqnum
} else { self.oracle.next_seq() };
```

So the epoch stamped into an xlog key **is** the sequence at which that transaction becomes visible — exactly, or the write errors. This is the same identity the existing segment-visibility logic already relies on: `artifact_build.rs:494` compares `segment.storage_sequence > base_epoch` where one side is an epoch from `next_epoch_txn` and the other is `snapshot.seq()`, proving epochs and snapshot sequences share one number space. No new invariant is invented; the xlog rides an existing one.

Three more invariants, each checked in source rather than assumed:

- **The bounded range scan needs no new plumbing.** `state.rs:259` — `GraphStorageSnapshot::scan_prefix_with_options(prefix, subrange, options)` already accepts a sub-range within a prefix, implemented for both the Writer and Reader arms. The delta scan is a call, not a feature.
- **The full build filters purely by snapshot visibility.** `artifact_build.rs:474` scans the canonical prefix with *no* epoch predicate — correctness comes from the pinned snapshot alone. The xlog scan inherits the identical mechanism, which is why an incremental and a full build at the same snapshot see the same changes.
- **WAL replay is a shared cost of both paths, not a differentiator.** The indexer reads through a `DbReader` (`cluster.rs:55`), and `db_reader.rs:628` folds new WAL files into a retained `imm_memtable`, skipping entries already held — each WAL file is downloaded once per refresh for the whole database and reused by every read. The old `topology_tail_since` downloaded the same span itself, per build, per edge type, and discarded the parse; the xlog scan reads whatever the snapshot already materialized and issues no per-WAL-file requests of its own.

### The chokepoint: no mutation site can be missed, by construction

The classic failure mode of a changelog is a missed mutation site producing a silently wrong index. Instead of patching 13 sites and hoping, the plan makes the compiler enforce coverage. `mark_adjacency_dirty_txn(txn, cell_id, edge_type, epoch)` is already called adjacent to **every** topology mutation in the codebase — it must be, or dirty tracking would already miss rebuilds. There are exactly 11 call sites, all in `src/shard/write.rs`. The plan changes its signature to require the edge delta and deletes the old function:

```rust
fn mark_topology_change_txn(
    txn: &DbTransaction,
    cell_id: &str,
    edge_type: &str,
    epoch: StorageSequence,
    changes: &[(VertexId, VertexId, bool)],   // (src, dst, exists-after)
) -> Result<()>
```

It writes the dirty marker + adjacency generation (as today) **and** one xlog entry per change. Any existing or future site that marks the adjacency dirty is forced, at compile time, to state which edges changed. A site that doesn't mark dirty at all is a bug the indexer already has today — and the equivalence property test (below) hunts those.

Site-by-site semantics, all verified in source:

| Site (`write.rs`) | Delta logged |
|---|---|
| `import_relationships_batch` :1871 | insert `(src, dst, true)` per structural edge |
| `create_relationship` :2191 | insert, single |
| `write_edge` :2891 | insert, single |
| `delete_edge_mutations_batch` :3334 | delete `(src, dst, false)` per mutation |
| `delete_edge` :3576, :3631 | delete, single (canonical-delete and tombstone arms) |
| `write_edge_mutations_batch` :4284 | insert, single per mutation |
| `bulk_append_segment_trusted` :4562 | insert per `inserted_dsts` entry (the edges packed into the segment value) |
| `bulk_import_edges` :4708 / :4749 | insert per inserted edge — log once, at :4749, with the full `inserted_edges` list (:4708 sits in the per-edge loop and would double-log) |
| `delete_structural_edge` :5424 | delete, single |

Two sites intentionally log **nothing**. Segment compaction (`maintenance.rs:209–228`) merges segments and drops tombstones without changing the logical edge set — topology-preserving by contract, and the property test includes compaction in its mutation mix to prove it. `drop_cell` (`write.rs:1011/1077`) is a wholesale prefix delete of `cell/{cell}/`; the xlog and its low-water key live under that prefix, so they are deleted with everything else, and the next build finds no previous generation and bootstraps.

## GC and the low-water mark

`cell/{cell}/meta/xlog_low/{edge_type}` holds the lowest sequence whose entries are still retained (the exclusive coverage floor). As implemented (this deviates from the first draft, which had the builder GC inside the build — see below for why that was wrong):

1. **The floor is written by the writer, in the write path.** The first logged change for a `(cell, edge_type)` sets `xlog_low` to its own epoch inside the same transaction, so coverage provably begins at the first entry. A per-process cache skips the check once the key's presence has been *read back* — a pending put is never cached, so a rolled-back transaction cannot strand the floor.
2. **Coverage check at build start** (`xlog_delta_since`): `previous.base_sequence + 1 >= xlog_low` means the range is fully retained; a missing floor is `Uninitialized` and a floor above the range is `CoverageGap` — both one-time bootstraps, never errors.
3. **GC is a separate pass, never part of a build.** `gc_topology_changelog(cell, edge_type)` deletes entries ≤ the current published generation's base and advances the floor, capped at 100 k deletes per pass (a capped pass parks the floor *at* the last touched sequence and resumes next time). Builds stay strictly read-only: a build that GC'd would commit a write, advancing the very sequence it just published against — which both breaks the byte-identical oracle (the equivalence test compares an incremental and a full build at the same sequence) and produces a perpetual build-GC-build churn loop. Found the hard way; the oracle test caught it.
4. **GC is best-effort and writer-gated.** The indexer's per-edge-type cleanup step (`artifact.xlog_gc`, next to the existing generation GC) calls it every cycle; on a reader-mode shard (the deployed indexer) it returns `Ok(0)` without writing, and retention falls to whichever process holds the SlateDB writer. An empty pass writes nothing — not even a floor advance — so idle cycles never commit. GC also *repairs* an absent floor at the current epoch (a manual purge deletes the key out from under the write path's cache; repair converges in one pass instead of degrading to full builds until a writer restart).

Retention is one build cycle per edge type wherever GC can run; where it cannot (reader-only indexer, writer node not yet wired), entries accumulate at ~72 bytes per change until the writer-side call is integrated — a flagged rollout item, not a correctness risk, since the floor guards coverage either way. There is **no dependency on SlateDB WAL GC timing at all** — the 60-second `min_age` fuse that killed the WAL-tail chain at 5 M-edge scale is simply irrelevant to this path.

## Prior art: this is how the serious databases do it

The design was cross-checked against systems that solved this exact problem in production — Postgres logical replication (commit-ordered change stream addressed by LSN, per-consumer retention floors: our commit sequence = LSN, our `xlog_low` = a replication slot with one consumer), Debezium's transactional outbox (the change record commits inside the same transaction as the data, killing the dual-write problem — the xlog is an outbox whose consumer is the index builder), and Kafka log compaction (last-write-wins per key — why our values are final state, not operations). The two closest analogues deserve fuller treatment:

**Neo4j — indexes are updated inside the data transaction's commit.** Neo4j's commit process converts a transaction into a sequence of commands — data records, count-store updates, *and index updates* — that are appended to one transaction log entry and applied together, which is how its indexes stay transactionally consistent with the graph instead of being rebuilt by scans. We can't update the CSC artifact synchronously at commit (it's an immutable content-addressed blob rebuilt out-of-band), so we do the next-closest thing: capture the index-relevant facts transactionally at commit, apply them asynchronously. Same consistency source (the committing transaction), decoupled application.

**Neon — the endgame for the follow-on.** Neon's pageserver ingests WAL and organizes storage as *delta layers* (changes per key-range × LSN-range) plus *image layers* (full materialization of a key range at one LSN), reconstructing any page as nearest image + deltas on top, with background compaction re-materializing images so delta chains stay short. Map the vocabulary: our CSC generation is an image layer; our xlog is the raw feed a delta layer is built from; "bootstrap full build" is image materialization. Neon is the proof that the architecture scales — and it shows exactly what our Phase 2 looks like: publish *delta artifacts* between periodic full CSC images so the O(graph) re-encode/PUT (the one cost this plan does not remove) also becomes O(delta), with image re-materialization as a background job. This plan is the prerequisite for that: the xlog is the delta feed.

So to the question "does the xlog actually ensure the delta is stored, or do we need something better": the xlog **is** the stored delta — the actual `(edge_type, src, dst, exists)` facts keyed by commit sequence, not a pointer to where they might be reconstructed from — and the pattern is the one Postgres, Debezium, and Neo4j converged on independently. The "something better" (delta-shaped artifacts, à la Neon's layer map) is not an alternative to the xlog; it is built *on top of* it, as Phase 2.

## What "full build" becomes

Not a fallback — a **bootstrap**, reached only when there is genuinely no prior state to be incremental against:
Hi 
| Condition | Why full is correct, not a regression |
|---|---|
| No previous generation | First build of a new scope/edge type — nothing to patch |
| `xlog_low` above coverage | xlog feature just enabled, or state purged — one-time |
| Previous CSC payload GC'd | Prior state physically gone — one-time |

There is **no cost-based decline**: the scan is O(delta) by construction, so "the delta is recoverable but too expensive to recover" cannot arise. `max_wal_tail_files` and the WAL-tail path remain only as the read path's query-time mechanism (unchanged) — the builder stops using them.

## Cost accounting

**Write amplification (the price):** one xlog put per logical edge change — ~70-byte key + 1-byte value, in the same WriteBatch/WAL append the mutation already pays for. No extra requests at write time, no extra commits. Entries live ~one build cycle then are deleted; steady-state storage is O(delta-per-cycle) per edge type, not O(history).

**Build cost (the payoff)**, at staging shape (15-edge delta, ~2,000-file WAL span, 8 edge types):

| Path | Requests | Useful edges / request |
|---|---|---|
| WAL-tail walk (regressed) | ~2,000 GETs × 8 types | 0.002–0.1 |
| Full rebuild (today's default) | tens of GETs, O(graph) bytes | 10³–10⁴, but scans 100% of the graph every cycle |
| **xlog scan** | **~1 block read per type, usually cache-hit** | **≈ the delta itself** |

### What stays O(graph) — stated plainly

The xlog removes the request-bound pathology completely. It does **not** make the whole build O(delta), and this plan does not pretend otherwise. Per build, with G = graph edges and D = delta edges:

| Stage | Full build | xlog incremental |
|---|---|---|
| Derive what changed | — (scans everything) | **O(D)**, ~1 block read, usually RAM |
| Read prior state | scan G canonical records, ~100–150 B/edge, many GETs | GET one CSC payload, ~8–12 B/edge, **1 GET** |
| Parse | `from_utf8_lossy` + `decode_edge_record` per edge | typed read of packed arrays |
| Apply changes | — | O(D) |
| Re-encode CSC | O(G) | O(G) |
| PUT payload | O(G) bytes | O(G) bytes |

**Two kinds of O(G) — only one of them is expensive.** The O(G) the full build pays is O(G) in *requests and parsing*: scanning the canonical keyspace reads ~100–150 B per edge (every record carries its full string key) across many GETs, then runs `from_utf8_lossy` + `decode_edge_record` once per edge to re-derive the graph's structure from key strings. The O(G) that remains in Phase 1 is O(G) in *sequential bytes*: the prior CSC is one packed blob (~8–12 B per edge, two integer arrays), fetched in one GET (or zero, via the matrix cache), patched at D points, and streamed back out — a memory-bandwidth operation, tens of milliseconds for a 10 M-edge graph. Nothing re-derives structure; the graph's knowledge is already sitting in the previous CSC in final form. The reason re-encode cannot be made O(D) in place is the format itself: CSC is packed, so inserting one edge into a column shifts every later byte of `row_idx` and bumps every later `col_ptr` — making the *write* O(D) requires changing the published format, which is precisely Phase 2.

| | Expensive O(G) — full build, removed by Phase 1 | Cheap O(G) — remains in Phase 1 |
|---|---|---|
| What is read | every canonical edge record in SlateDB | one packed CSC blob |
| Bytes per edge | ~100–150 (full string key per record) | ~8–12 (packed integer arrays) |
| Requests | many GETs across the keyspace | 1 GET (0 with matrix cache) |
| Per-edge CPU | `from_utf8_lossy` + `decode_edge_record` | none — memcpy with D patch points |
| Scales with | request latency × keyspace size | memory bandwidth |

So the win is: requests collapse to a handful, bytes read drop roughly 10× (compact CSC arrays instead of fully-keyed edge records — the key `cell/{cell}/e/out/{T}/{src:020}/{dst:020}` alone is ~60 B before the value), and per-edge string parsing disappears. Re-encode and PUT remain O(G) and are common to both paths. The literal form of the original bet ("1 M-edge graph, 100 k delta, therefore ~10% of the work") is reached only by also making the *artifact* delta-shaped — layered generations, base image + deltas, periodically compacted, exactly Neon's delta-layer/image-layer split. That is Phase 2, specified next. A complementary cheap win is keeping the prior CSC in the matrix cache across cycles, which removes the one remaining O(G) read as well.

**End-to-end wall clock at staging shape** (~1 M edges, 15-edge delta):

| | Today (always-full) | Phase 1 (xlog) | Phase 2 (+ delta artifacts) |
|---|---|---|---|
| Find the delta | re-scans everything | ~1 cached block read | same |
| Read prior state | — | 1 GET, or RAM via matrix cache | in RAM |
| Encode + upload | full CSC | full CSC — tens of ms + 1 PUT | delta blob only |
| **Cycle wall-clock** | **~30 s** | **~1 s** | **~ms** |

Phase 1 alone recovers most of the win at this scale, because the expensive O(G) is what dominated. Phase 2 is what keeps that true at 100 M edges, where the CSC blob is ~1 GB and cheap-O(G) stops being cheap — per-cycle PUT bytes and re-encode time become real again.

Framed honestly: **Phase 1 makes incremental cost proportional to the delta for everything the build reads; Phase 2 does the same for what it writes.**

## Phase 2: delta-shaped artifacts (the Neon layer model)

Phase 1 leaves exactly one O(G) cost: every cycle still re-encodes and PUTs the full CSC payload even when D ≪ G. Phase 2 removes it by restructuring the published artifact the way Neon structures pageserver storage — a generation stops being one full image and becomes a **chain: one base image plus a short run of delta artifacts.**

- **Base image** — today's CSC payload unchanged: a full materialization of the adjacency at sequence B₀. (Neon: image layer.)
- **Delta artifact** — a compact sorted list of `(src, dst, exists)` covering `(Bₖ₋₁, Bₖ]`: the deduplicated output of the xlog scan, published as its own content-addressed blob. Encode and PUT are O(D). (Neon: delta layer.)
- **Steady-state cycle** — scan the xlog, publish one delta artifact, update the generation manifest to `base + [d₁ … dₖ]`. No CSC re-encode, no O(G) PUT.
- **Consumer** — the graph node loads base + deltas and applies the overlays in memory before handing the matrix to GraphBLAS. This is the same overlay contract `build_graph_index_incremental` applies today, moved to load time; read amplification is bounded by the chain length K.
- **Re-materialization** — when the chain reaches K deltas, or Σ|deltas| exceeds a set fraction of the base, the builder folds base + deltas into a new base image and truncates the chain. That is literally today's full-build code run every Kth cycle instead of every cycle — Neon's background image-layer creation, amortizing the O(G) cost. Per-cycle amortized cost becomes O(D + G/K).
- **Crash safety** — unchanged model: every blob is content-addressed and immutable, and the manifest update remains the single atomic publish point, exactly as generation publish works today.

Phase 2 design decisions to settle when we get there (explicitly *not* blockers for Phase 1): the K / size-ratio compaction policy; whether readers apply deltas eagerly at load or keep the layered form resident; how the matrix cache keys chains vs images; GC of superseded chains. What makes Phase 1 the prerequisite: a delta artifact is exactly the xlog scan's output given a durable home — without commit-time capture there is nothing O(delta) to publish.

## Test and measurement plan

1. **Unit tests** — xlog key ordering, range bounds (inclusive/exclusive ends), last-wins resolution, low-water advance, GC deletes exactly the scanned range.
2. **Equivalence property test** — randomized sequences over *all* public write APIs (single/batch inserts, deletes, relationship imports, bulk imports, segment appends, segment compaction, vertex detach-delete), asserting after every burst that the incremental build's payload hash equals a full rebuild's at the same snapshot. This is the missed-site detector: any mutation path that changes topology without logging breaks the hash.
3. **Existing tests** — the current equivalence + decline tests keep passing; decline-reason logging updated to the bootstrap taxonomy.
4. **Scale benches, the definitive answer** — extend the `examples/wal_tail_trickle_bench.rs` shape: seed 1 M–5 M edges × 2 types, trickle deltas, measure xlog-incremental vs full on (a) in-memory, (b) MinIO, (c) real S3 (`hydradb-local-turbolay`). Success criteria: **zero fallbacks** at every scale, incremental wall-clock beats full and stays ~flat as seed size grows while full grows linearly. This is the graph to put in the design note.

## Rollout

1. Land the xlog writes — they are inert data until a builder reads them.
2. First indexer cycle after deploy: bootstrap full per edge type (the one-time coverage bootstrap; `xlog_low` is set by the first post-deploy mutation).
3. Every subsequent cycle: incremental, unconditionally.
4. PR #23's gate + concurrency stay merged as the read path's own hygiene and as a backstop for pre-xlog generations during the transition; the builder no longer walks WAL at all once xlog covers it.
5. **Open item — writer-side GC wiring.** The deployed indexer is reader-mode, so its `artifact.xlog_gc` step no-ops and nothing deletes consumed entries in production yet (~72 B per edge change accumulates; harmless at staging write rates). The writing node needs a periodic `gc_topology_changelog(cell, edge_type)` call — same shape as its segment-compaction maintenance — before this matters at scale.

## Implementation status (2026-08-07, Phase 1 landed on `feat/xlog-incremental-index`)

- `src/keys.rs` — `xlog_entry`, `xlog_type_prefix`, `xlog_low_water`.
- `src/shard/xlog.rs` — new module: `xlog_delta_since` (coverage check + bounded range scan + last-wins overlay), `gc_topology_changelog` (writer-gated, capped, floor-repairing), key parsing.
- `src/shard/write.rs` — `mark_adjacency_dirty_txn` replaced by `mark_topology_change_txn(…, changes: &[(VertexId, VertexId, bool)])`; all 11 sites converted per the table above; the old signature no longer exists, so the compiler enforces the chokepoint.
- `src/engine/index_store.rs` — `build_graph_index_incremental` derives its delta from `xlog_delta_since`; the WAL-tail call, the cost gate, and the `AdmissionRejected` decline are gone from the builder; declines are the bootstrap taxonomy only.
- `src/bin/graph-indexer.rs` — `artifact.xlog_gc` span in the per-edge-type cleanup step, best-effort.
- Tests: the byte-identical oracle passes on the xlog path; `xlog_incremental_matches_full_over_random_mutation_mix` (randomized inserts/deletes/bulk imports/segment appends across two edge types, GC interleaved, segment compaction at the end); GC lifecycle and purge-bootstrap-recovery tests; the two obsolete WAL-tail decline tests rewritten as their inverses (`oversized_delta_no_longer_declines_the_incremental_build`, `incremental_build_ignores_the_wal_span_cap`). Full lib suite: 184 passing.

## Open questions for discussion

1. **Idempotent re-imports that change nothing** currently still mark dirty in some paths (e.g. re-import marks dirty with the old epoch when `changed` is false). Logging zero changes there is correct and free — but worth deciding whether those sites should mark dirty at all.
2. **xlog for in-edges:** the CSC is built from out-adjacency only, so the plan logs out-edges only. If a future index consumes in-edges, mirror entries under `xlog-in/` at that point rather than paying double now.
3. **Crash between publish and GC:** publish succeeds, GC deletes + low-water advance don't commit → next build re-scans a slightly larger range and re-deletes. Idempotent and self-healing because values are final-state (last-wins); no action needed. Agreed?
4. **Very large single commits** (bulk import of 1 M edges in one txn) write 1 M xlog entries in that txn's WriteBatch — the same order of work as the edges themselves, which SlateDB already carries. Any limit worth adding, or let `max_artifact_build_edges` continue to govern?

## Prior-art references

- Postgres logical decoding concepts (slots, LSN ordering): https://www.postgresql.org/docs/current/logicaldecoding-explanation.html
- Replication-slot retention semantics (`confirmed_flush_lsn` vs `restart_lsn`): https://www.morling.dev/blog/postgres-replication-slots-confirmed-flush-lsn-vs-restart-lsn/
- Debezium, the transactional outbox pattern: https://debezium.io/blog/2019/02/19/reliable-microservices-data-exchange-with-the-outbox-pattern/
- Neo4j commit process (index updates as commands in the data transaction): https://neo4j.com/developer/kb/neo4j-commit-process-explained/ and https://neo4j.com/docs/operations-manual/current/database-internals/transaction-logs/
- Neon storage engine (delta layers, image layers, GetPage@LSN): https://neon.com/blog/get-page-at-lsn and https://github.com/neondatabase/neon/blob/main/docs/pageserver-storage.md
