# turbolay read-path optimization — results (2026-07-06)

Fixes for the read-path issues the hop × supernode-degree benchmark exposed
(one storage point-read per frontier node, no batching, no cache). Correctness
was re-verified against FalkorDB after **every** change.

## What changed

**Core (`src/`):**
- `GraphStorage::multi_get` — bounded-concurrency (64) batched point read,
  order-preserving.
- `posting_ops::neighbors` — issues the base-adjacency and deleted-edges reads
  **concurrently** (`join!`) instead of two serial round-trips (R2).
- `posting_ops::neighbors_batch` / `neighbors_each` — expand a whole frontier
  concurrently instead of one serial `await` per node (R1); `_each` keeps the
  anchor→set binding for materialization.
- `Writer::get_nodes` / `edge_props_batch` — batched node / edge-companion
  fetch (R3).
- **`Writer` decoded read cache** (`node_cache` + `neigh_cache`) — decode each
  node blob / resolve each adjacency set **once**, reuse across hops and repeated
  queries (R4/R5). **Cleared on every durable write** (`commit`/`commit_batch`)
  so it never serves stale state; safe under `maybe_split` (a split preserves
  membership, which is what's cached). Cache-aware read methods
  (`get_nodes_cached`, `neighbors_cached`/`_batch_cached`/`_each_cached`).

**Bench (`bench/src/queries.rs`):** the hand-planned executors now drive the
cached/batched primitives, with a per-query `BTreeSet` node-fetch that dedups
decodes.

## Correctness

- **186 unit/integration tests pass.**
- **FalkorDB shadow-test: 240/240 cells exact-match** — verified after the
  batching change *and* again after the cache (byte-identical DISTINCT result
  sets across ic02/07/08/09 × hops 1–5 × 4 hub degrees × 3 anchors).

## Latency (in-memory tier, warm p50, before → after)

Geometric-mean speedup **1.91× across all 80 cells; no regressions.** The
hottest query (ic07, likers) improves most at deep/dense cells:

| query | representative before → after |
|-------|-------------------------------|
| ic07 h5 deg-10000 | 1137 ms → 321 ms (**3.54×**) |
| ic07 h3 deg-10000 | 1005 ms → 308 ms (**3.26×**) |
| ic02 h1 deg-10000 | 366 ms → 124 ms (**2.95×**) |
| ic08 h5 deg-1000  | 772 ms → 236 ms (**3.27×**) |
| ic09 (message-only) | ~1.1–2.1× (fewer repeated decodes to save) |

## Gap to FalkorDB (in-memory, matched datasets/anchors/queries)

Geometric-mean latency ratio turbolay ÷ FalkorDB (>1 = turbolay slower):

| hub degree | before | after |
|-----------|:------:|:-----:|
| 50    | 4.39× | **2.44×** |
| 100   | 4.72× | **2.65×** |
| 1000  | 6.41× | **3.15×** |
| 10000 | 7.45× | **3.64×** |

**The gap roughly halved.** turbolay is now 2.4–3.6× slower than FalkorDB on the
in-memory tier (was 4.4–7.5×), while returning identical answers.

## Notes / honest caveats

- The **in-memory tier is turbolay's hardest comparison** — both engines hold
  everything in RAM, so it's pure CPU (decode) vs FalkorDB's native in-RAM graph.
  The remaining ~2.4–3.6× is decode/clone/lookup overhead of a KV-backed model vs
  a purpose-built in-memory graph.
- **Batching's bigger payoff is on the S3 / local-FS tiers** (turbolay's actual
  object-native target), where each `get` is a real round-trip and `multi_get`
  overlaps `N` reads into `⌈N/64⌉` rounds — not exercised in these in-memory
  numbers, and the next thing to benchmark on real S3 (RFC 0017 gate).
- Further in-memory headroom: true multi-core parallelism (spawn decode across
  cores) and `Arc<NodeRecord>` to drop the clone-on-hit — deferred.
- Raw data: `turbolay-hopdeg-memory-s1.0-deg*.json` (now the optimized numbers),
  `falkordb-hopdeg-s1.0-deg*.json`.
