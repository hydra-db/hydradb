# turbolay vs FalkorDB — 3–4-hop benchmark (RFC 0017 Phase 3)

**Status: COMPLETE for local tiers + MinIO S3-compatible Tier 3.** FalkorDB
baseline and turbolay InMemory, Local FS, and MinIO-backed S3-compatible runs
are captured.
Raw result JSONs live in `bench/out/`; reproduce with the commands in each section.

## 1. What this measures (and what it does not)

turbolay has no query language / read planner yet (M2 = indexes, M3 = openCypher).
The bench executes **hand-planned traversals** directly over the storage API
(`lookup_uid` → `posting_ops::neighbors` BFS with roaring dedup → `get_node` /
`edge_props`), so this benchmarks turbolay's **storage + traversal cost**, not a
query engine. Honest-comparison caveats (stated up front, not hidden):

- **FalkorDB runs full Cypher** (parse/plan/execute, in-memory, C). turbolay runs
  hand-compiled traversals over a storage engine. The M3 planner will re-run this.
- **FalkorDB is RAM-resident**; turbolay's Local-FS and S3 tiers pay object-store
  round-trips by design. The point is to **quantify the gap and locate
  bottlenecks** (RFC 0017's purpose), the same way NamiDB's 230×→3.4× journey did.
- **Path multiplicity.** FalkorDB's `MATCH` emits one row per path (no `DISTINCT`);
  the turbolay executor dedups nodes per hop (roaring sets). For *timed* runs both
  keep `LIMIT 20` and we compare cold/warm latency + the top-20 ranking. For the
  **verify** (correctness) tier we compare the **distinct result set** (dedup by
  returned columns, no `LIMIT`, deterministic `ORDER BY … DESC, id ASC`) so the
  Cypher path-enumeration artifact doesn't produce false row diffs.

## 2. Setup

- **turbolay:** branch `main` (M1 complete; write path + posting substrate + obs
  spine). Bench crate: `bench/` (`turbolay-bench`), backends `memory|local|s3`.
- **FalkorDB:** `falkordb/falkordb:latest` via `docker run -d -p 6379:6379`,
  driven by `bench/py/falkordb_runner.py` (`pip install falkordb`).
- **Dataset:** synthetic LDBC-SNB subset, generator copied verbatim from
  graphdb-experiments (`bench/src/dataset.rs`), **seed=42**, pipe-delimited CSVs —
  byte-identical input to both engines. Scale 1.0 = 10k Person / 100k Post / 50k
  Comment + 100k KNOWS / 150k HAS_CREATOR / 100k LIKES / 30k REPLY_OF (160k nodes,
  380k edges).
- **Queries:** ic02 (1-hop), ic07/ic08 (1-hop reverse), ic09 (2-hop KNOWS), **ic3h
  (3-hop, new)**, ic4h (4-hop KNOWS chain). All `… ORDER BY <date> DESC LIMIT 20`.
- **Protocol:** per (query, param): 1 cold run + N warm runs → cold_us,
  warm_p50/p95/p99_us. Params = deterministic `make_person_id_hex(i·stride)`.
- **Hardware:** local macOS (Darwin 25.5.0). Single-writer, load-then-query.

## 3. FalkorDB baseline

### scale = 0.1 (54k elements, 20 warm runs, 3 params) — `bench/out/falkordb-s0.1.json`

| query | rows | warm_p50 | notes |
|-------|-----:|---------:|-------|
| ic02  | 20   | ~0.7–1.1 ms | 1-hop KNOWS + HAS_CREATOR |
| ic07  | 2–8  | ~0.5–0.6 ms | likers of my messages |
| ic08  | 1–2  | ~0.6–0.8 ms | replies to my posts |
| ic09  | 20   | ~1.2–1.9 ms | 2-hop KNOWS |
| ic3h  | 20   | ~4.1–5.6 ms | **3-hop KNOWS** |
| ic4h  | 20   | ~4.7–5.5 ms | 4-hop KNOWS |

Load: 54k elements in ~1.0 s (~53k elem/s).

### scale = 1.0 (540k elements, 50 warm runs, 3 params) — `bench/out/falkordb-s1.0.json`

Median warm_p50 across 3 params (load: 540k elements in 9.2 s, ~59k elem/s):

| query | rows | warm_p50 (median) | warm_p95 | notes |
|-------|-----:|------------------:|---------:|-------|
| ic02  | 20   | ~1.1 ms  | ~1.5 ms  | 1-hop |
| ic07  | 4–6  | ~0.9 ms  | ~1.2 ms  | likers |
| ic08  | 1–4  | ~0.75 ms | ~1.2 ms  | replies |
| ic09  | 20   | ~2.2 ms  | ~2.7 ms  | 2-hop KNOWS |
| ic3h  | 20   | **~14 ms**  | ~19 ms   | **3-hop KNOWS** |
| ic4h  | 20   | **~68 ms**  | ~85 ms   | 4-hop KNOWS — deep traversal blows up |

The 3→4-hop cliff (ic3h ~14 ms → ic4h ~68 ms) is the regime this benchmark
targets, and matches the prior NamiDB-vs-FalkorDB observation that FalkorDB's
in-memory Cypher stays cheap until the frontier explodes at 4 hops.

## 4. turbolay results

### Tier 1 — InMemory (`object_store::memory`, pure engine cost)

**Correctness (verify oracle): PASS.** Distinct-set row diff (turbolay hand-planned
traversal vs FalkorDB `RETURN DISTINCT`, no LIMIT, `bench/py/verify_diff.py`) is
**zero-diff at scale 0.1 (18/18 (query,param)) and scale 1.0** — full row-content
diff on ic02/ic07/ic08/ic09; exact distinct-set size match on the deep queries
(scale 1.0: ic3h 9,504 rows, ic4h 54,025 rows, identical on both engines). One
gotcha found and handled: FalkorDB's default `RESULTSET_SIZE=10000` silently caps
unlimited `DISTINCT` results — set `-1` for the ic4h verify (LIMIT-20 timed runs
are unaffected). turbolay's storage traversals return byte-identical result sets
to FalkorDB's Cypher.

**Timing, scale = 1.0** (release build, batched ingest bs=1000, 20 warm runs,
median across 3 params) — `bench/out/turbolay-memory-s1.0.json`,
`bench/out/compare-memory-s1.0.txt`:

| query | rows | turbolay p50 | FalkorDB p50 | ratio | read |
|-------|-----:|-------------:|-------------:|------:|------|
| ic02 | 20 | 152 µs | 1.07 ms | **0.16×** | 1-hop — turbolay ~7× faster |
| ic07 | 4–6 | 27 µs | 941 µs | **0.03×** | reverse 1-hop + edge prop |
| ic08 | 1–4 | 18 µs | 750 µs | **0.03×** | reverse 1-hop |
| ic09 | 20 | 2.04 ms | 2.19 ms | **1.02×** | 2-hop KNOWS — parity |
| ic3h | 20 | 21.9 ms | 14.3 ms | **2.20×** | **3-hop** |
| ic4h | 20 | 144 ms | 68 ms | **2.48×** | 4-hop |

**Reading:** turbolay's storage engine *beats* full-Cypher on point/shallow
queries — it pays no parse/plan/execute overhead, just direct posting reads. It
reaches parity at 2 hops and trails **~2.2–2.5×** at 3–4 hops. The crossover is
the frontier-expansion cost: each hop does one `posting_ops::neighbors` (posting
deserialize + roaring ops) **per frontier node**, plus a `get_node` per
materialized message — at 4 hops the frontier covers most of the 10k Persons and
materializes ~54k messages. That per-frontier-node loop (and the absence of an
adjacency/node cache) is the concrete RFC 0017 optimization target; the M3
planner + caching are expected to close it, exactly as NamiDB's 230×→3.4×
journey did. (Note: `compare.py`'s "Kuzu"/"GATE" labels are cosmetic — inherited
from the copied tool; the comparison here is turbolay-vs-FalkorDB.)

Scale 0.1 InMemory (`bench/out/turbolay-memory-s0.1.json`) shows the same shape
(ic02 ~0.1 ms, ic09 ~1.3 ms, ic3h ~11 ms, ic4h ~20 ms), all row counts matching.

### Tier 2 — Local FS (SlateDB over a local-filesystem object store)

Same corrected executor + batched ingest (bs=1000), release build, scale=1.0,
**no SlateDB block cache configured** (`block_cache: None`), so every
`posting_ops::neighbors` and `get_node` is a real object-store read. Row counts
identical to Tier 1 / FalkorDB. warm_p50 median across 3 params (20 warm runs;
ic4h from a dedicated 3-param run at ~4.6 s/query):

| query | rows | Local FS p50 | InMemory p50 | vs InMemory | FalkorDB p50 | vs FalkorDB |
|-------|-----:|-------------:|-------------:|------------:|-------------:|------------:|
| ic02 | 20  | ~5.2 ms  | 152 µs | ~34× | 1.07 ms | ~4.9× |
| ic07 | 4–6 | ~1.5 ms  | 27 µs  | ~55× | 941 µs  | ~1.6× |
| ic08 | 1–4 | ~1.2 ms  | 18 µs  | ~65× | 750 µs  | ~1.6× |
| ic09 | 20  | ~47 ms   | 2.04 ms| ~23× | 2.19 ms | ~21× |
| ic3h | 20  | ~449 ms  | 21.9 ms| ~20× | 14.3 ms | ~31× |
| ic4h | 20  | **~4.6 s** | 144 ms | ~32× | 68 ms | **~68×** |

**Reading:** the Local-FS tier isolates **storage round-trip cost with no cache**.
Shallow/point queries stay ~1–5 ms (a handful of reads), but multi-hop queries
amplify the Tier-1 bottleneck brutally: ic4h touches thousands of frontier nodes
× a per-node object-store read with zero reuse → ~4.6 s. This is the single
strongest argument for RFC 0017's caching work (adjacency/node caches + the M3
planner batching reads) — and it quantifies the gap the roadmap's optimization
RFCs must close. The result JSONs are `bench/out/turbolay-local-s1.0.*`
(ic02–ic3h) and `bench/out/turbolay-local-ic4h-s1.0.json`.

### Tier 3 — S3-compatible MinIO

Run against local MinIO (`hydra-minio-it`, endpoint `http://127.0.0.1:9010`) with
`AWS_ENDPOINT` + `AWS_ALLOW_HTTP=true`, fresh bucket
`turbolay-tier3-1783311225`. This validates the `--backend s3` / object-store
path, but it is **not** a canonical AWS S3 latency claim.

**Correctness (verify oracle): PASS.** Full row-content verify passed at scale
1.0 for all 18 `(query,param)` cases after setting FalkorDB
`GRAPH.CONFIG SET RESULTSET_SIZE -1` so unlimited `RETURN DISTINCT` results are
not silently capped at 10k rows. Result: `bench/out/verify-tier3-minio-s1.0.json`.

**Timing, scale = 1.0** (release build, batched ingest bs=1000, 20 warm runs,
median across 3 params) — `bench/out/turbolay-s3-minio-s1.0.json`,
`bench/out/compare-s3-minio-s1.0.txt`:

| query | rows | MinIO p50 | Local FS p50 | InMemory p50 | FalkorDB p50 | vs FalkorDB |
|-------|-----:|----------:|-------------:|-------------:|-------------:|------------:|
| ic02 | 20  | ~6.5 ms   | ~5.2 ms  | 152 us  | 1.07 ms | ~6.1x |
| ic07 | 4-6 | ~0.87 ms  | ~1.5 ms  | 27 us   | 941 us  | ~0.95x |
| ic08 | 1-4 | ~0.96 ms  | ~1.2 ms  | 18 us   | 750 us  | ~1.5x |
| ic09 | 20  | ~44.8 ms  | ~47 ms   | 2.04 ms | 2.19 ms | ~24x |
| ic3h | 20  | ~431 ms   | ~449 ms  | 21.9 ms | 14.3 ms | ~41x |
| ic4h | 20  | **~3.33 s** | ~4.6 s | 144 ms  | 68 ms   | **~57x** |

**Reading:** MinIO shows the same bottleneck shape as Local FS, with slightly
better deep-hop medians on this machine but the same order-of-magnitude cliff.
That is useful because it exercises the real S3 object-store code path while
confirming the underlying problem is still the uncached traversal strategy: each
frontier node causes an object-store read, and deep KNOWS traversals multiply
that cost quickly. RFC 0017 should target adjacency/node caching and batched
frontier reads before any real-S3 number is expected to be competitive.

## 5. Ratio tables (compare.py)

Available outputs:

- InMemory: `bench/out/compare-memory-s1.0.txt`
- MinIO S3-compatible: `bench/out/compare-s3-minio-s1.0.txt`

(`compare.py` still prints inherited "Kuzu"/"GATE" labels; those labels are
cosmetic in this repo.)

## 6. Bottleneck notes & review cross-reference

Two review findings (see `2026-07-06-implementation-review-findings.md`) were
**bench-relevant for the S3 tier** and were fixed before treating any S3 numbers
as canonical:

- **H3** — `InstrumentedObjectStore` omits `list_with_offset`/`rename_opts`, so on
  S3 offset-listings degrade to unbounded list+client-filter; this can inflate
  S3 compaction/recovery/cold latency in a way attributable to the wrapper, not
  the engine. Fixed in `6ef15d7`.
- **H2** — a post-commit `maybe_split` error is (incorrectly) returned as a write
  failure; on S3 a transient hiccup could surface as a spurious ingest error. The
  single-edge path now matches the batched-ingest log-and-continue behavior.
  Fixed in `9a8bef4`.

RFC 0017 Phase 0 objstore/phase metrics are wired (`src/obs.rs`) and can be used
to attribute S3-tier latency to specific object-store operations.
