# xlog incremental builds — commit-time changelog benchmark

- **Date:** 2026-08-07
- **Branch:** `feat/xlog-incremental-index` (PR #25, ticket PRO-1469); design in `docs/plans/2026-08-05-edge-changelog-incremental-index.md`
- **Harness:** `examples/wal_tail_trickle_bench.rs` (unchanged from PR #23), release build
- **Machine:** Apple Silicon macOS; MinIO in Docker (shared instance, port 9002); real S3 `hydradb-local-turbolay` (us-east-1), client ~300–800 ms from the bucket — absolute real-S3 times are inflated vs an in-cluster indexer; ratios and outcomes are what transfer
- **Method:** seed N edges per type across 2 edge types sharing one database, publish full baselines, then trickle single-edge durable commits so every commit cuts its own WAL file(s) — the staging shape — then time `build_graph_index_auto` (now the xlog path) and a full rebuild at the same durable sequence. "Span" is the WAL-file gap the old walk paid one GET per file for; the xlog build never reads it.

## Results

| environment | seed/type | span (WAL files) | delta | incremental (cold) | incremental (2nd type) | full, same seq | speedup vs full |
|:------------|----------:|-----------------:|------:|-------------------:|-----------------------:|---------------:|:---------------:|
| in-memory   | 20 k      | 400              | 200   | 62 ms              | 6 ms                   | 43 ms          | ~1× |
| in-memory   | 2 M       | 1,000            | 500   | 413 ms             | 367 ms                 | 2,680 ms       | **6.5×** |
| MinIO       | 200 k     | 600              | 300   | 225 ms             | 136 ms                 | 2,323 ms       | **10× / 17×** |
| real S3     | 100 k     | 500              | 250   | 11,578 ms          | 6,670 ms               | 281,575 ms     | **24× / 42×** |

## The staging replay — the operating loop, not a one-shot build

`examples/staging_replay_bench.rs` recreates what actually degraded on staging: 8 edge types
sharing one scope database, an indexer cycle building every type every round, and per cycle
320 single-commit writes — ~215 to one hot type, **exactly 15 edges per quiet type** spread
across the same window — so every quiet type's build faces the staging signature precisely:
a 15-edge delta under a ~330-file WAL span it had no part in creating. The xlog GC runs each
cycle as the indexer's cleanup step does, and one quiet type per cycle is re-verified against
a full rebuild at the same sequence. Run twice with identical parameters (25 k seed/type):
once on **real S3** (`hydradb-local-turbolay`; each cycle's trickle is ~9 minutes of genuine
elapsed time and 320 real durable commits) and once on MinIO to isolate the xlog's own cost
from S3 request latency.

**Real S3** — cycle 0 runs the old way (full builds), cycles 1–3 run the xlog path on
identical workloads, so both columns of the before/after come from the same run:

| cycle | trickle | quiet-type builds (15-edge delta) | hot type (215) | cycle total | verify | GC reclaimed |
|------:|--------:|:----------------------------------|---------------:|------------:|:-------|-------------:|
| 0 (bootstrap, full builds) | 9.5 min | 73–195 s each | 195 s | **18.3 min** | AGREE (full 71 s) | 200,320 (seed) |
| 1 | 9.0 min | **5.1–6.0 s each** | 8.7 s | 2.2 min | AGREE (full 72 s) | 320 |
| 2 | 8.9 min | **2.9–5.6 s each** | 7.5 s | 2.0 min | AGREE (full 71 s) | 320 |
| 3 | 8.9 min | **3.0–5.4 s each** | 8.7 s | 2.0 min | AGREE (full 72 s) | 320 |

The 18.3-minute full-build cycle lands inside staging's regressed 15–40-minute range —
the "before" reproduced live. The remaining seconds in the "after" are the shared
reader-refresh over the new WAL span, paid once per cycle by either path (the 72 s verify
full pays it too) and amortized across all 8 edge types.

**MinIO, same harness** — the xlog's own cost with request latency removed:

| cycle | span | quiet-type builds (15-edge delta) | hot type (215) | full 8-type cycle | verify | GC reclaimed |
|------:|-----:|:----------------------------------|---------------:|------------------:|:-------|-------------:|
| 1 | 328 | **17–21 ms each** | 241 ms | **0.69 s** | AGREE | 320 |
| 2 | 328 | **18–23 ms each** | 242 ms | **0.70 s** | AGREE | 320 |
| 3 | 328 | **17–28 ms each** | 77 ms | **0.55 s** | AGREE | 320 |

Zero fallbacks after the planned bootstrap in both runs; every delta count exact; every
cycle byte-identical to full. **Cross-store determinism:** each cycle's content-addressed
generation hash is identical between the MinIO and real-S3 runs (cycle-1 `dae74af53bb2`,
cycle-2 `9beee71433f1`, cycle-3 `86e5f9d7453a`) — same graph in, same artifact out,
regardless of object store. The old path's measured cost for this shape (~50 ms per span
file, from the staging traces) is ~16 s per quiet type per cycle, which compounded into
staging's 15–40-minute cycles.

**Caveat, itself a finding:** attempts to seed the replay at 50–100 k edges/type against
real S3 reproducibly wedged the fork's write pipeline mid-seed — writers parked on
compaction backpressure while the compactor loops refreshing its fenced transactional
state object after a GC boundary write; alive-but-silent, no errors, same family as the
staging indexer stall, now locally reproducible with stack samples. A pre-xlog binary
shows the same collapse in crawl form (40–60 s per WAL file vs 3 s healthy), so the
mechanism predates the xlog; the xlog's added write volume deepens it under sustained
bulk load. Tracked separately from the xlog work; the replay seeds below the threshold.

Every run reported `[incremental, N delta edges]` with N exactly the trickled count — zero
fallbacks, zero declines, exact deltas. There is no cost gate left to fire:
`incremental_build_ignores_the_wal_span_cap` runs with `max_wal_tail_files = 0` and stays
incremental.

## Against the old path, same bucket

PR #23 measured the WAL-tail walk on this exact bucket over a ~1,000-file span: the serial
walk **did not finish in 10 minutes**; the gated 16-way walk took **478 s**; a full rebuild
**290 s**. The xlog build over a 500-file span on the same bucket: **11.6 s** — and span size
no longer appears in the cost model at all, including spans WAL GC has already collected (the
condition that permanently broke the tail chain at 5 M-edge scale in the 2026-08-03 runs).

## Reading the numbers

- The residual real-S3 incremental seconds are dominated by `refresh_durable_reader` folding
  the new WAL span into the retained memtable — a shared cost of both paths, paid once per
  database per refresh and amortized across all 8 edge types in production. The warm
  second-type column shows the shared part amortizing.
- The in-memory 2 M row isolates the CPU-side win: the full build parses every canonical edge
  record (`from_utf8_lossy` + `decode_edge_record`, ~100–150 B/edge with full string keys);
  the xlog path reads the prior CSC as packed arrays and patches D entries.
- Deliberately not claimed: CSC re-encode and payload PUT remain O(graph) in both paths.
  Making the artifact itself delta-shaped is Phase 2 in the plan doc, fed directly by the
  xlog scan's output.
- Correctness at these sequences is asserted by the suite, not eyeballed: the byte-identical
  content-address oracle and `xlog_incremental_matches_full_over_random_mutation_mix`
  (randomized writes/deletes/bulk imports/segment appends across two edge types, GC
  interleaved, segment compaction) compare payload hashes between incremental and full
  builds at the same snapshot.

## Comparison to `2026-08-03-incremental-index-build.md`

That document benchmarked the WAL-tail incremental path and recorded its two structural
failures: cost proportional to WAL file count (the staging regression) and permanent
fallback once WAL GC's 60-second `min_age` collected a span (observed at 5 M/MinIO). The
xlog path has neither failure mode by construction — the delta is durable first-class data
with its own retention (`meta/xlog_low` + `gc_topology_changelog`), decoupled from WAL GC
entirely.
