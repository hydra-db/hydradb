# Incremental index builds — before/after benchmark

- **Date:** 2026-08-03
- **Harness:** `examples/incremental_index_bench.rs` (committed on this branch), release build
- **Machine:** Apple Silicon macOS; MinIO in Docker (`minio/minio`, port 19100) as the real object store
- **Method:** seed N edges at out-degree ~8, publish a full baseline generation, then per cycle
  apply a 100-edge delta (99 adds + 1 delete), build through `build_graph_index_auto` (the
  incremental path the indexer takes under `GRAPH_INDEXER_BUILD_MODE=incremental`), then run the
  old full rebuild at the same durable sequence and compare wall time. `delta_edges` and
  `full_edges` are exactly what the `graph_indexer_incremental_delta_edges` and
  `graph_indexer_full_build_edges` Prometheus counters export.

## Headline: previous (full rebuild) vs current (incremental patch)

Per-cycle averages. "Previous" is the only path that existed before this branch: a full
canonical-adjacency scan for every dirty edge type, however small the change.

| edges | object store | previous: full rebuild | current: incremental | wall speedup | edges touched per build |
|------:|:-------------|-----------------------:|---------------------:|:------------:|:------------------------|
| 200k  | in-memory    | 254 ms                 | 176 ms               | 1.4x         | 100 vs 200k (2,005x fewer) |
| 200k  | MinIO        | 263 ms                 | 520 ms               | **0.5x — slower** | 100 vs 200k (2,003x fewer) |
| 1M    | in-memory    | 1,346 ms               | 914 ms               | 1.5x         | 100 vs 1M (10,003x fewer) |
| 1M    | MinIO        | 10,458 ms              | 2,747 ms             | **3.8x**     | 100 vs 1M (10,003x fewer) |
| 2M    | in-memory    | 2,862 ms               | 1,918 ms             | 1.5x         | 100 vs 2M (20,002x fewer) |
| 2M    | MinIO        | 23,404 ms              | 5,550 ms             | **4.2x**     | 100 vs 2M (20,002x fewer) |
| 5M    | in-memory    | 7,699 ms               | 5,560 ms             | 1.4x         | 100 vs 5M (50,002x fewer) |
| 5M    | MinIO        | ~74,000 ms             | *fell back to full*  | —            | see WAL-retention finding below |

Two shapes fall out of the table:

- **Against a real object store the win grows with the graph.** The full rebuild scans the whole
  canonical keyspace, paying object-store reads proportional to N every cycle; the incremental
  path reads one previous CSC payload plus the WAL tail. At 1M edges that is 3.8x, at 2M it is
  4.2x, and the gap keeps widening.
- **Below the block-cache scale the trade inverts.** A 200k-edge graph fits SlateDB's foyer block
  cache, so the "full scan" runs at memory speed while the incremental path still decodes,
  patches, and re-encodes the whole previous CSC payload. This is exactly why
  `GRAPH_INDEXER_INCREMENTAL_MIN_EDGES` (default 250,000) exists: in incremental mode, edge
  types below the floor deliberately keep the full path.

The work counters are scale-invariant either way: the incremental path applies exactly the
delta (100 edges/cycle here) regardless of graph size, which is what
`graph_indexer_incremental_delta_edges` vs `graph_indexer_full_build_edges` shows in
production.

## Per-cycle detail

### 200,000 edges — in-memory (10 cycles)

| cycle | delta_edges | full_edges | incr_ms | full_ms | speedup |
|------:|------------:|-----------:|--------:|--------:|--------:|
| 0 | 100 | 200,098 | 195 | 257 | 1.3x |
| 1 | 100 | 200,196 | 174 | 249 | 1.4x |
| 2 | 100 | 200,294 | 173 | 251 | 1.5x |
| 3 | 100 | 200,392 | 173 | 250 | 1.4x |
| 4 | 100 | 200,490 | 170 | 252 | 1.5x |
| 5 | 100 | 200,588 | 174 | 256 | 1.5x |
| 6 | 100 | 200,686 | 183 | 254 | 1.4x |
| 7 | 100 | 200,784 | 175 | 251 | 1.4x |
| 8 | 100 | 200,882 | 174 | 254 | 1.5x |
| 9 | 100 | 200,980 | 169 | 263 | 1.6x |

Totals: incremental 1,760 ms vs full 2,537 ms (1.4x); 1,000 delta edges applied vs 2,005,390
scanned (2,005x fewer).

### 200,000 edges — MinIO (5 cycles) — the regression case

| cycle | delta_edges | full_edges | incr_ms | full_ms | speedup |
|------:|------------:|-----------:|--------:|--------:|--------:|
| 0 | 100 | 200,098 | 711 | 263 | 0.4x |
| 1 | 100 | 200,196 | 482 | 269 | 0.6x |
| 2 | 100 | 200,294 | 509 | 265 | 0.5x |
| 3 | 100 | 200,392 | 436 | 259 | 0.6x |
| 4 | 100 | 200,490 | 461 | 261 | 0.6x |

Totals: incremental 2,599 ms vs full 1,317 ms (0.5x — the full scan wins). This is the case the
`GRAPH_INDEXER_INCREMENTAL_MIN_EDGES` floor removes: at 200k < 250k the indexer takes the full
path even in incremental mode.

### 1,000,000 edges — in-memory (5 cycles)

| cycle | delta_edges | full_edges | incr_ms | full_ms | speedup |
|------:|------------:|-----------:|--------:|--------:|--------:|
| 0 | 100 | 1,000,098 | 1,020 | 1,432 | 1.4x |
| 1 | 100 | 1,000,196 | 883 | 1,309 | 1.5x |
| 2 | 100 | 1,000,294 | 891 | 1,328 | 1.5x |
| 3 | 100 | 1,000,392 | 881 | 1,318 | 1.5x |
| 4 | 100 | 1,000,490 | 896 | 1,341 | 1.5x |

Totals: incremental 4,571 ms vs full 6,728 ms (1.5x); 500 delta edges vs 5,001,470 scanned
(10,003x fewer).

### 1,000,000 edges — MinIO (5 cycles)

| cycle | delta_edges | full_edges | incr_ms | full_ms | speedup |
|------:|------------:|-----------:|--------:|--------:|--------:|
| 0 | 100 | 1,000,098 | 2,867 | 10,628 | 3.7x |
| 1 | 100 | 1,000,196 | 2,549 | 10,005 | 3.9x |
| 2 | 100 | 1,000,294 | 2,818 | 10,788 | 3.8x |
| 3 | 100 | 1,000,392 | 2,921 | 10,404 | 3.6x |
| 4 | 100 | 1,000,490 | 2,578 | 10,467 | 4.1x |

Totals: incremental 13,733 ms vs full 52,292 ms (3.8x); 500 delta edges vs 5,001,470 scanned
(10,003x fewer).

### 2,000,000 edges — in-memory (3 cycles)

| cycle | delta_edges | full_edges | incr_ms | full_ms | speedup |
|------:|------------:|-----------:|--------:|--------:|--------:|
| 0 | 100 | 2,000,098 | 2,074 | 2,883 | 1.4x |
| 1 | 100 | 2,000,196 | 1,894 | 2,716 | 1.4x |
| 2 | 100 | 2,000,294 | 1,785 | 2,986 | 1.7x |

Totals: incremental 5,753 ms vs full 8,585 ms (1.5x); 300 delta edges vs 6,000,588 scanned
(20,002x fewer).

### 2,000,000 edges — MinIO (3 cycles)

| cycle | delta_edges | full_edges | incr_ms | full_ms | speedup |
|------:|------------:|-----------:|--------:|--------:|--------:|
| 0 | 100 | 2,000,098 | 6,121 | 23,866 | 3.9x |
| 1 | 100 | 2,000,196 | 5,003 | 23,776 | 4.8x |
| 2 | 100 | 2,000,294 | 5,525 | 22,571 | 4.1x |

Totals: incremental 16,649 ms vs full 70,213 ms (4.2x); 300 delta edges vs 6,000,588 scanned
(20,002x fewer). Baseline full build at this scale: 21.9 s — every cycle, forever, under the
previous behavior.

### 5,000,000 edges — in-memory (3 cycles)

| cycle | delta_edges | full_edges | incr_ms | full_ms | speedup |
|------:|------------:|-----------:|--------:|--------:|--------:|
| 0 | 100 | 5,000,098 | 5,612 | 7,879 | 1.4x |
| 1 | 100 | 5,000,196 | 5,878 | 7,755 | 1.3x |
| 2 | 100 | 5,000,294 | 5,191 | 7,463 | 1.4x |

Totals: incremental 16,681 ms vs full 23,097 ms (1.4x); 300 delta edges vs 15,000,588 scanned
(50,002x fewer).

### 5,000,000 edges — MinIO (3 cycles) — the WAL-retention finding

At this scale the incremental path **never ran**: `build_graph_index_auto` declined to the full
path on all three cycles (baseline full build: 74.3 s; each cycle's full build ~60-75 s). The
fallback itself is the designed behavior — correctness was never at risk, and in the indexer
binary these cycles would surface as `graph_indexer_incremental_fallbacks` increments — but the
*reason* is an operational interaction worth pinning down:

SlateDB's WAL garbage collector retains WAL files for only **60 seconds** past the compacted
boundary by default (`min_age = "60s"` in the upstream GC options — versus 86,400 s for every
other object class). The incremental build needs the unbroken WAL chain
`previous.last_wal_id + 1 ..= current`; `topology_tail_since` returns `Unavailable` the moment
any file in that range has been collected. At 5M edges on MinIO, a full build takes ~74 s —
longer than the WAL retention — so by the time each incremental attempt ran, the head of its
tail chain was already collected, and the decline became self-sustaining: every fallback full
build out-lasted the retention window again.

This did not bite at 2M (full build 22 s < 60 s) and would not bite a steadily-polling indexer
(default 5 s interval keeps the chain fresh). The exposure is: an edge type whose builds are
spaced longer than the WAL retention — indexer downtime, a very bursty tenant, or a first
build after enabling incremental mode at a scale where the full build itself exceeds 60 s.
The system degrades to exactly the old behavior and says so in the fallback counter.

**Mitigation for deployments running incremental mode at large scale:** raise SlateDB's WAL GC
`min_age` above the worst-case full-build time plus the poll interval (e.g. 300 s). The graph
kernel does not currently expose that knob through `GraphStorageConfig` — exposing it is a
small follow-up. The phase-2 direction (delta-shaped generations diffing against the previous
*payload* rather than the WAL) would remove the dependency on WAL retention entirely.

## What bounds the incremental path

The incremental build's wall time is dominated by decode → patch → re-encode of the previous
CSC payload — O(N) work even for a 100-edge delta. That is why the in-memory speedup plateaus
around 1.5x while the MinIO speedup keeps growing (the full scan's object-store reads grow with
N; the single-payload read does not). A follow-up that makes generations delta-shaped (patch
files against a base payload, compacted periodically) would cut the O(N) re-encode and is the
natural phase 2; this harness gives it a baseline to beat.

## Backward compatibility

The incremental path changes how a generation is *produced*, not what a generation *is*. Both
paths end at the same `encode_graph_index_csc` payload encoding, the same content-addressed
object name, and the same manifest publish — a reader cannot tell which path built a
generation, and nothing on the read path changed. The default mode is `full`, so unconfigured
deployments behave exactly as before; rollback is just an old binary building normal
generations again, and every non-viable case (no previous generation, unavailable or oversized
tail, below the floor) declines into the old full scan.

The equivalence is byte-for-byte, and it is engineered rather than incidental: the patch path
replicates the full build's empty-source normalization so a delete that empties a source
produces the identical CSC bytes (and therefore the identical SHA-256 generation name) a full
build would. The equivalence test pins this against the content-addressed payload objects on
the store, not the manifest, so a divergence cannot hide behind publish arbitration.

## Default-mode recommendation

`GRAPH_INDEXER_BUILD_MODE` stays `full` by default in this PR. The incremental path is opt-in
per deployment, guarded three ways when enabled: the size floor keeps small graphs on the full
path, every non-viable tail declines to a full build (counted by
`graph_indexer_incremental_fallbacks`), and equivalence is pinned by tests that compare the
content-addressed payload objects, not just manifests. After a production soak with those
counters flat, flipping the default to `incremental` is a one-line change.
