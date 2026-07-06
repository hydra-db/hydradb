# turbolay vs FalkorDB — hop × supernode-degree sweep (2026-07-06)

Matched comparison: **identical datasets** (seed 42, scale 1.0, hub degrees
50/100/1000/10000), **identical anchors** (the 3 hub persons, indices 0–2), and
**matched queries** — turbolay's generalized executors vs the same MATCH pattern
in Cypher with a `KNOWS*H..H` prefix. Both engines are **in-memory** (turbolay
`--backend memory`; FalkorDB is a Redis module). Warm p50, median across the 3
anchors. turbolay warm=10 runs, FalkorDB warm=5, per-query timeout 15 s.

Raw: `turbolay-hopdeg-memory-s1.0-deg*.json`, `falkordb-hopdeg-s1.0-deg*.json`.

## Headline

FalkorDB completed **every cell with zero timeouts** and is **faster than
turbolay almost everywhere** — geometric-mean latency ratio (turbolay/FalkorDB):

| hub degree | turbolay / FalkorDB (geomean) |
|-----------|-------------------------------|
| 50    | **4.4× slower** |
| 100   | **4.7× slower** |
| 1000  | **6.4× slower** |
| 10000 | **7.5× slower** |

The gap **widens with degree** (4.4× → 7.5×). The only cells turbolay wins are
**hop-1 at deg-50** (~0.8×) — the shallowest, tiniest-frontier case where its
zero parse/plan overhead barely edges out. Everywhere else FalkorDB wins by 3–14×.

## Per-query (turbolay | FalkorDB, ratio T/F)

### ic02 — friends@N → messages
| hops | deg 50 | deg 100 | deg 1000 | deg 10000 |
|--|--|--|--|--|
| 1 | 581us \| 762us (0.8×) | 3.0ms \| 974us (3.1×) | 21.7ms \| 4.3ms (5.0×) | 366.6ms \| 31.8ms (11.5×) |
| 2 | 9.2ms \| 1.8ms (5.1×) | 27.6ms \| 3.5ms (8.0×) | 183.6ms \| 27.8ms (6.6×) | 599.6ms \| 75.8ms (7.9×) |
| 3 | 91.4ms \| 11.9ms (7.7×) | 122.7ms \| 30.8ms (4.0×) | 374.3ms \| 80.1ms (4.7×) | 447.7ms \| 79.9ms (5.6×) |
| 4 | 317.2ms \| 56.9ms (5.6×) | 467.2ms \| 78.7ms (5.9×) | 454.5ms \| 73.0ms (6.2×) | 529.0ms \| 79.0ms (6.7×) |
| 5 | 494.7ms \| 91.7ms (5.4×) | 410.5ms \| 70.9ms (5.8×) | 392.9ms \| 88.7ms (4.4×) | 526.9ms \| 83.7ms (6.3×) |

### ic07 — friends@N → messages → likers (turbolay's worst absolute; hottest query)
| hops | deg 50 | deg 100 | deg 1000 | deg 10000 |
|--|--|--|--|--|
| 1 | 2.5ms \| 953us (2.7×) | 2.6ms \| 1.7ms (1.5×) | 36.6ms \| 10.4ms (3.5×) | 377.9ms \| 59.7ms (6.3×) |
| 2 | 33.3ms \| 3.5ms (9.4×) | 29.9ms \| 10.5ms (2.8×) | 300.8ms \| 55.6ms (5.4×) | 886.2ms \| 133.6ms (6.6×) |
| 3 | 168.2ms \| 22.4ms (7.5×) | 256.4ms \| 52.1ms (4.9×) | 741.9ms \| 120.3ms (6.2×) | 1005.4ms \| 119.9ms (8.4×) |
| 4 | 503.9ms \| 94.9ms (5.3×) | 716.1ms \| 112.1ms (6.4×) | 829.9ms \| 123.4ms (6.7×) | 960.1ms \| 122.0ms (7.9×) |
| 5 | 758.1ms \| 115.7ms (6.6×) | 795.3ms \| 121.1ms (6.6×) | 996.7ms \| 129.3ms (7.7×) | 1137.6ms \| 135.4ms (8.4×) |

### ic08 — friends@N → posts → replies (widest ratios: up to 13.6×)
| hops | deg 50 | deg 100 | deg 1000 | deg 10000 |
|--|--|--|--|--|
| 1 | 625us \| 764us (0.8×) | 2.3ms \| 1.0ms (2.3×) | 40.3ms \| 6.2ms (6.5×) | 259.3ms \| 31.9ms (8.1×) |
| 2 | 8.9ms \| 2.0ms (4.4×) | 24.2ms \| 4.9ms (4.9×) | 255.7ms \| 30.8ms (8.3×) | 674.1ms \| 67.2ms (10.0×) |
| 3 | 90.7ms \| 12.2ms (7.4×) | 206.4ms \| 25.9ms (8.0×) | 705.2ms \| 68.5ms (10.3×) | 733.6ms \| 64.6ms (11.4×) |
| 4 | 484.3ms \| 51.9ms (9.3×) | 578.5ms \| 61.0ms (9.5×) | 852.8ms \| 62.9ms (13.6×) | 659.5ms \| 64.3ms (10.2×) |
| 5 | 665.7ms \| 61.8ms (10.8×) | 588.0ms \| 62.8ms (9.4×) | 771.7ms \| 64.4ms (12.0×) | 760.8ms \| 68.3ms (11.1×) |

### ic09 — friends@N → messages (same executor as ic02 at depth N)
| hops | deg 50 | deg 100 | deg 1000 | deg 10000 |
|--|--|--|--|--|
| 1 | 614us \| 818us (0.8×) | 1.5ms \| 940us (1.6×) | 24.1ms \| 5.9ms (4.1×) | 195.2ms \| 33.1ms (5.9×) |
| 2 | 7.0ms \| 1.8ms (3.9×) | 21.1ms \| 3.9ms (5.4×) | 158.7ms \| 36.0ms (4.4×) | 428.4ms \| 76.3ms (5.6×) |
| 3 | 79.9ms \| 11.8ms (6.8×) | 137.7ms \| 25.2ms (5.5×) | 498.7ms \| 75.8ms (6.6×) | 470.6ms \| 91.9ms (5.1×) |
| 4 | 259.1ms \| 51.9ms (5.0×) | 466.4ms \| 81.5ms (5.7×) | 501.2ms \| 78.4ms (6.4×) | 457.3ms \| 77.0ms (5.9×) |
| 5 | 361.1ms \| 81.0ms (4.5×) | 400.2ms \| 81.7ms (4.9×) | 653.7ms \| 83.6ms (7.8×) | 443.3ms \| 86.3ms (5.1×) |

## Reading this

1. **This flips the repo's earlier headline.** The prior FalkorDB report ("turbolay
   beats full-Cypher 0.03–0.16× on point/shallow queries") held for *natural-hop,
   no-hub, single-anchor* queries where the frontier is tiny and turbolay's only
   edge was skipping parse/plan. Under **supernode + multi-hop stress**, turbolay
   loses that edge and trails 4–14×.
2. **The gap grows with both degree and hops** because turbolay does **one storage
   point-read per frontier node, no batching, no adjacency/node cache** — cost
   scales linearly with frontier size. FalkorDB's mature in-memory engine amortizes
   traversal far better and doesn't explode on `KNOWS*H..H` (its planner dedupes).
   This is precisely the RFC 0017 / RFC 0009 (CSR + batched neighbor reads) target.
3. **Fair caveats:**
   - Both are in-memory. turbolay's memory backend is its *best* case; on its native
     S3/local-FS substrate it would be far slower still (object read per node).
     FalkorDB is not object-native at all — different design point.
   - turbolay is **M1**: no query planner, no read cache, no batched neighbor fetch.
     The comparison measures *engine maturity*, not the architecture's ceiling — the
     optimizations that would close this gap are explicitly deferred (RFC 0009/0010/0017).
   - Semantics differ (turbolay = deduped node-set BFS; FalkorDB = path enumeration),
     but both return the same top-20-by-date rows.
