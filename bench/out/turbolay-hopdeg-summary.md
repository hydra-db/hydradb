# turbolay hop × supernode-degree sweep (2026-07-06)

Latency of `ic02 / ic07 / ic08 / ic09` swept over **KNOWS-prefix hop depth
{1..5}** and **injected hub degree {50, 100, 1000, 10000}**.

## Setup

- **Backend:** in-memory (`--backend memory`) — pure engine cost, no object-store I/O.
  This is the optimistic floor; the local-FS / S3 tiers pay one object read per
  frontier node (RFC 0017 bottleneck).
- **Dataset:** LDBC-shaped, scale 1.0 (10k Person, 100k Post, 50k Comment;
  100k KNOWS, 150k HAS_CREATOR, 100k LIKES, 30k REPLY_OF), seed 42.
- **Supernodes:** 3 hub persons (indices 0–2) generated with total degree ≈ the
  column's `hub_degree` (split ~half outgoing / half incoming KNOWS). The bench
  anchors on those 3 hubs so degree actually bites the traversal.
- **Hop semantics:** every query walks `hops` outgoing KNOWS hops from the anchor
  to build a person frontier, then applies its tail — messages (ic02/ic09),
  likers of those messages (ic07), replies to those posts (ic08). `hops` was
  generalized to all four queries for this sweep (ic02/ic09 are the *same*
  executor at different depths; ic07/ic08 gained a KNOWS prefix ahead of their
  original tail).
- **Timing:** warm p50 of 10 runs, reported as the median across the 3 hub anchors.
  Result rows are LDBC `ORDER BY … DESC LIMIT 20`, so every cell returns 20 rows
  (cardinality is saturated well past 20; this sweep measures latency, not row counts).
- Raw JSON: `turbolay-hopdeg-memory-s1.0-deg{50,100,1000,10000}.json`.

## Warm p50 latency

### ic02 — friends@N → messages
| hops | deg 50 | deg 100 | deg 1000 | deg 10000 |
|------|--------|---------|----------|-----------|
| 1 | 581 µs | 2.97 ms | 21.70 ms | 366.60 ms |
| 2 | 9.23 ms | 27.60 ms | 183.60 ms | 599.57 ms |
| 3 | 91.38 ms | 122.72 ms | 374.27 ms | 447.72 ms |
| 4 | 317.15 ms | 467.15 ms | 454.49 ms | 529.01 ms |
| 5 | 494.67 ms | 410.53 ms | 392.94 ms | 526.93 ms |

### ic07 — friends@N → messages → likers  (most expensive: adds per-LIKES-edge prop lookup)
| hops | deg 50 | deg 100 | deg 1000 | deg 10000 |
|------|--------|---------|----------|-----------|
| 1 | 2.55 ms | 2.59 ms | 36.61 ms | 377.89 ms |
| 2 | 33.29 ms | 29.95 ms | 300.77 ms | 886.17 ms |
| 3 | 168.18 ms | 256.43 ms | 741.91 ms | 1005.41 ms |
| 4 | 503.93 ms | 716.06 ms | 829.88 ms | 960.12 ms |
| 5 | 758.11 ms | 795.28 ms | 996.75 ms | 1137.61 ms |

### ic08 — friends@N → posts → replies
| hops | deg 50 | deg 100 | deg 1000 | deg 10000 |
|------|--------|---------|----------|-----------|
| 1 | 625 µs | 2.30 ms | 40.30 ms | 259.26 ms |
| 2 | 8.90 ms | 24.23 ms | 255.70 ms | 674.12 ms |
| 3 | 90.70 ms | 206.43 ms | 705.18 ms | 733.58 ms |
| 4 | 484.27 ms | 578.51 ms | 852.83 ms | 659.46 ms |
| 5 | 665.73 ms | 587.99 ms | 771.75 ms | 760.78 ms |

### ic09 — friends@N → messages  (same executor as ic02; sanity check)
| hops | deg 50 | deg 100 | deg 1000 | deg 10000 |
|------|--------|---------|----------|-----------|
| 1 | 614 µs | 1.51 ms | 24.08 ms | 195.25 ms |
| 2 | 6.96 ms | 21.13 ms | 158.68 ms | 428.40 ms |
| 3 | 79.94 ms | 137.69 ms | 498.67 ms | 470.62 ms |
| 4 | 259.11 ms | 466.36 ms | 501.23 ms | 457.26 ms |
| 5 | 361.08 ms | 400.19 ms | 653.69 ms | 443.34 ms |

## Findings

1. **Two cost axes, one ceiling.** Hub degree dominates at shallow hops
   (ic02 hop-1: 581 µs → 366 ms, deg 50 → 10000, ≈630×); hop depth dominates at
   low degree (ic02 deg 50: 581 µs → 495 ms, hop 1 → 5, ≈850×). Both converge to a
   **~0.4–1.1 s saturation ceiling** at hop ≥ 4.
2. **The KNOWS graph saturates by hop 4 regardless of degree.** With avg
   out-degree ≈10 over 10k persons, the frontier reaches nearly the whole graph
   by hop 4 from *any* anchor — so the degree columns collapse together at hops
   4–5, and the mild non-monotonicity there (e.g. ic09 deg 1000 hop 5 > deg 10000)
   is saturation-regime jitter, not signal. The degree axis is only meaningful at
   hops 1–2.
3. **ic07 is the hot query** — the extra LIKES-in expansion plus a per-`(fan,
   message)` `edge_props` lookup pushes it to ~1.14 s at hop 5 / deg 10000, ~2×
   the message-only ic02/ic09.
4. **Root cause = one point-read per frontier node, no batching, no cache.**
   `posting_ops::neighbors` is called once per frontier uid; at saturation the
   executor issues hundreds of thousands of in-memory gets per query. This is the
   exact N+1 / no-adjacency-cache bottleneck RFC 0017 names as the primary
   optimization target — and on local-FS/S3 each of those gets is an object-store
   round trip (prior report: ic4h ≈ 4.6 s on local FS), so these memory numbers
   are the best case.
