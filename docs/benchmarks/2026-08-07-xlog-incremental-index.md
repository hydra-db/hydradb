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
