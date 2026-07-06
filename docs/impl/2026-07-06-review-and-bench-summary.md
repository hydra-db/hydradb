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
  (InMemory) and Tier 2 (Local FS). Tier 3 (MinIO/S3) **not run** (stopped here).

## Correctness (the important part)

**Verify = zero row diffs vs FalkorDB at scale 0.1 and 1.0.** turbolay's
hand-planned traversals return byte-identical distinct result sets to FalkorDB's
Cypher. A real bug was caught by verify-first (executors ignored node `:Post`/
`:Comment` labels → over-counted ic07/ic08) and fixed before any numbers were
trusted. No bench-blocking correctness bug exists in the read/merge/posting path.

## Results (warm p50, scale 1.0, median of 3 params)

| query | turbolay InMem | FalkorDB | ratio | turbolay Local-FS |
|-------|---------------:|---------:|------:|------------------:|
| ic02  | 152 µs | 1.07 ms | 0.16× | ~5.2 ms |
| ic07  | 27 µs  | 941 µs  | 0.03× | ~1.5 ms |
| ic08  | 18 µs  | 750 µs  | 0.03× | ~1.2 ms |
| ic09  | 2.04 ms| 2.19 ms | 1.02× | ~47 ms  |
| ic3h  | 21.9 ms| 14.3 ms | 2.20× | ~449 ms |
| ic4h  | 144 ms | 68 ms   | 2.48× | ~4.6 s  |

**Read:** turbolay's storage engine *beats* full-Cypher on point/shallow queries
(no parse/plan overhead), hits parity at 2-hop, trails ~2.2–2.5× at 3–4 hop. The
Local-FS tier (no cache) shows the uncached round-trip cost exploding on deep
hops (ic4h ~4.6 s). Both are the same bottleneck: one object-store read **per
frontier node**, no adjacency/node cache — the concrete RFC 0017 optimization
target.

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
- **Tier 3 (MinIO/S3): not run.** Wiring is ready (`--backend s3`,
  `AmazonS3Builder::from_env` honors `AWS_ENDPOINT`/`AWS_ALLOW_HTTP` → MinIO on
  :9010 works with creds + a bucket; real S3 works with your creds).
- Suggested next: run Tier 3 now that H2/H3 are fixed, then refresh the S3
  section of the full bench report.
