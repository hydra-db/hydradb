# turbolay pending work (2026-07-06)

This is the current remaining-work list after the M1 review, H2/H3 fixes,
batched ingest, and the FalkorDB/InMemory/Local-FS/MinIO benchmark runs.

## Highest priority

1. **H1: fix `intern()` schema-cache poisoning on aborted writes.**
   An oversized write can abort after mutating the in-memory schema cache. This
   is the remaining high-severity correctness finding from the review.

2. **Bind split-manifest and part reads to a single snapshot.**
   The torn-read risk between a `Split` manifest and its parts is dormant while
   the read path is narrow, but it must be resolved before a real concurrent
   reader/query path relies on split postings.

3. **Fix zero-part `Split` handling.**
   `find_target_part` can panic on an empty split manifest. This is a low-level
   defensive correctness fix and should be small.

## Query and product surface

1. **Implement the general query engine.**
   There is no arbitrary Cypher/openCypher query engine yet. The benchmark
   currently uses hand-planned executors for fixed query shapes only:
   `ic02`, `ic07`, `ic08`, `ic09`, `ic3h`, and `ic4h`.

2. **Wire parser/lowering/planner/executor.**
   `decypher` is present as a dependency, but the production lowering, planner,
   and execution path are still pending.

3. **Build the HTTP service and reader/writer fleet.**
   The storage/write path exists, but the service plane and reader fleet are
   still M3 work.

## Indexes and read path

1. **Build the M2 index framework.**
   Value indexes, count indexes, and the read planner integration are still
   pending.

2. **Add snapshot-aware reader behavior.**
   The future read path needs clear snapshot semantics across manifest reads,
   posting parts, index state, and changelog tail handling.

3. **Resolve future concurrency story.**
   The no-CAS-outside-changelog-seq finding is mitigated by SlateDB fencing in
   the current single-writer model, but the story needs to be made explicit
   before broader concurrency is introduced.

## Performance and observability

1. **Capture canonical real AWS S3 numbers.**
   The MinIO S3-compatible run is complete and useful for exercising the S3
   code path, but it is not a canonical AWS S3 latency baseline.

2. **Add adjacency/node caching.**
   Benchmarks show the same bottleneck in Local FS and MinIO: one object-store
   read per frontier node, with no reuse. This is the main RFC 0017/RFC 0009
   optimization target.

3. **Batch frontier reads.**
   Deep traversals need to avoid N+1 object-store access patterns across large
   frontiers.

4. **Complete the RFC 0017 query metric matrix.**
   Pending metrics include query phase timers, per-hop frontier sizes,
   N+1 fan-out counters, block-cache hit rate, adjacency parts read, and
   cold-vs-warm first-hop breakdowns.

5. **Measure/tune block cache behavior.**
   Current Local FS and MinIO benchmark results intentionally expose the
   uncached path. Cache configuration and measured cache-hit behavior remain
   open.

## Review cleanup

1. **Wire `maybe_rollup`.**
   Rollup is designed but not connected to the write path.

2. **Add CI.**
   There is no CI config in the repo.

3. **Clean up duplicated Meta constants.**

4. **Update stale fencing comments.**

5. **Check node-size caps before full encode.**

6. **Remove or feature-gate unused `decypher` dependency until wired.**

7. **Make id/degree overflow fail closed in release builds.**

8. **Close the `delete_node` stale-read surface.**

## Already done

- H2 fixed: post-commit split errors no longer surface as failed writes after a
  successful durable commit.
- H3 fixed: object-store wrapper forwards `list_with_offset` and `rename_opts`.
- Batched ingest write path implemented and tested.
- Benchmark harness implemented.
- FalkorDB baseline captured.
- turbolay InMemory, Local FS, and MinIO S3-compatible benchmark tiers captured.
- Distinct-set verify oracle passes for the benchmarked scale-1 dataset.
