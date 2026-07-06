# turbolay: M1 review + FalkorDB bench — summary (2026-07-06)

Short handoff. Full detail in the two companion docs:
- `2026-07-06-implementation-review-findings.md` (Part A)
- `2026-07-06-falkordb-bench-report.md` (Parts B/C)

## What was done

- **Part A — implementation review** (4 sonnet reviewers, 2 waves of 2; every
  finding orchestrator-verified against code + RFCs). Follow-up fixes applied
  for H2/H3 before S3-tier benchmarking.
- **Part B — benchmark harness** (`bench/` → `turbolay-bench`): copied the
  dataset generator + FalkorDB/compare runners with attribution; wrote the
  turbolay-side hand-planned traversal executors (ic02/07/08/09 + new **ic3h** +
  ic4h); added a **batched-ingest** write path to `Writer` (first-class, tested);
  added a **distinct-set verify** oracle. Gauntlet green throughout.
- **Part C — runs**: FalkorDB baseline (scale 0.1 + 1.0) + turbolay Tier 1
  (InMemory), Tier 2 (Local FS), and Tier 3 via local MinIO S3-compatible
  object store.

## Correctness (the important part)

**Verify = zero row diffs vs FalkorDB at scale 0.1 and 1.0.** turbolay's
hand-planned traversals return byte-identical distinct result sets to FalkorDB's
Cypher. A real bug was caught by verify-first (executors ignored node `:Post`/
`:Comment` labels -> over-counted ic07/ic08) and fixed before any numbers were
trusted. For Tier 3, full MinIO-path verify passed at scale 1.0 after setting
FalkorDB `RESULTSET_SIZE=-1` to avoid its default 10k unlimited-result cap.

## Results (warm p50, scale 1.0, median of 3 params)

| query | turbolay InMem | FalkorDB | ratio | Local-FS | MinIO S3-compat |
|-------|---------------:|---------:|------:|---------:|----------------:|
| ic02  | 152 us | 1.07 ms | 0.16x | ~5.2 ms | ~6.5 ms |
| ic07  | 27 us  | 941 us  | 0.03x | ~1.5 ms | ~0.87 ms |
| ic08  | 18 us  | 750 us  | 0.03x | ~1.2 ms | ~0.96 ms |
| ic09  | 2.04 ms| 2.19 ms | 1.02x | ~47 ms  | ~44.8 ms |
| ic3h  | 21.9 ms| 14.3 ms | 2.20x | ~449 ms | ~431 ms |
| ic4h  | 144 ms | 68 ms   | 2.48x | ~4.6 s  | ~3.33 s |

**Read:** turbolay's storage engine *beats* full-Cypher on point/shallow queries
(no parse/plan overhead), hits parity at 2-hop, trails ~2.2–2.5× at 3–4 hop. The
Local-FS and MinIO S3-compatible tiers (no cache) show the uncached round-trip
cost exploding on deep hops (ic4h seconds/query). Same bottleneck: one
object-store read **per frontier node**, no adjacency/node cache -- the concrete
RFC 0017 optimization target.

## Review findings (ranked; verified)

- **H1** `intern()` poisons the schema cache on an aborted oversize write (atomicity).
- **H2** post-commit `maybe_split` error returned as write failure
  (S3-ingest-relevant; fixed in `9a8bef4`).
- **H3** `InstrumentedObjectStore` omits `list_with_offset`/`rename_opts`
  (S3-latency-relevant; fixed in `6ef15d7`).
- **M1** no CAS outside changelog seq (mitigated+tested by SlateDB fencing).
- **M2** torn Split-manifest read (dormant until concurrent read path).
- **M3** `maybe_rollup` designed but never wired in.
- **M4** no CI config.
- **L1–L7** empty-parts panic, duplicated Meta constants, stale fencing comment,
  cap-check-after-encode, unused `decypher` dep, release fail-open on overflow,
  `delete_node` stale-read surface.

H2/H3 are now fixed and covered by focused regressions. The remaining findings
are tracked but were not needed to trust Tier 1/Tier 2 numbers.

## State / what's left

- Committed in separate slices: H3 object-store forwarding (`6ef15d7`), H2
  post-commit split handling (`9a8bef4`), batched ingest (`abae0fe`), and the
  benchmark harness (`42f88f3`).
- **Tier 3 (MinIO/S3-compatible): run complete.** Results are in
  `bench/out/turbolay-s3-minio-s1.0.json`,
  `bench/out/compare-s3-minio-s1.0.txt`, and
  `bench/out/verify-tier3-minio-s1.0.json`.
- Still not done: canonical real AWS S3 numbers. The MinIO run validates the S3
  code path but should not be presented as AWS latency.
