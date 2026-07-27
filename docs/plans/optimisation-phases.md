---
title: GraphBLAS optimisation phases
status: draft-for-review
date: 2026-07-18
branch: Turbolay-V3.5
base_commit: 0a15b8d
tags:
  - graphblas
  - performance
  - sparse-kernel
---

# GraphBLAS optimisation phases

Phased plan to measure and improve CPU performance of the SuiteSparse:GraphBLAS
traversal path used by turbolay, and to keep correctness locked down while doing
it. Each phase is written to be handed to a single Opus subagent with no other
context — read **Shared context** first, then the phase you were assigned.

**Decisions already made (do not relitigate):**
- Benchmarks exercise the **GraphBLAS backend only**. The pure-Rust compact CSC
  kernel is out of scope; never set `GRAPH_COMPILED_KERNEL=compact` in any bench.
- Concurrency work (shared read-only matrix, replica redesign, concurrency
  benchmarks) is **deferred** — see Backlog. Do not add it to any phase.
- Also deferred: non-blocking `GrB_init`, JIT enablement, thread tuning,
  backend-size heuristic. See Backlog.

**Execution rules:**
- One phase per agent; **max two agents running at any time**.
- One git commit per phase, in the repo the phase changes. Phase 5 explicitly
  must be its own commit, separate from Phase 4 and Phase 6.
- Every code-changing phase must show before/after numbers from the Phase 2
  criterion harness (and Phase 1 macro bench where noted) in its summary.
- Do not push, do not open PRs. Commit locally only.

---

## Shared context

### Repo map

| Repo | Path | Role |
|---|---|---|
| turbolay (`slatedb-graph-kernel`) | `/Users/abhishek/hydradb/graphdb-on-s3/turbolay` | Graph DB on S3/SlateDB. Calls the traversal kernel through `src/sparse_kernel.rs` only. Bench scripts live here. |
| rs-graphblas | `/Users/abhishek/Downloads/experiments/2026-07/rs-graphblas` | Cargo workspace: `crates/graphblas-sys` (hand-written FFI decls), `crates/graphblas` (safe wrapper), `crates/graphblas-codegen` (macros), `crates/graphblas-traversal` (the BFS kernel turbolay uses — **most work happens here**). |

turbolay depends on `graphblas-traversal` by path (turbolay `Cargo.toml:50`),
behind the non-default cargo feature `graphblas`. Bench scripts pass
`--features opencypher,graphblas`.

> **Stale as of 2026-07-27 — read this before acting on the paragraph above.**
> The sparse-kernel consolidation
> (`docs/plans/2026-07-25-sparse-kernel-backend-consolidation.md`, Step 1,
> `f80d9c6`) **deleted the `graphblas` cargo feature** and, with it, the path
> dependency. `Cargo.toml` today declares no `graphblas-traversal` and no
> `graphblas` feature; SuiteSparse is reached through turbolay's own FFI in
> `src/sparse_kernel/graphblas.rs`, declared by `#[link(name = "graphblas")]`
> and linked **unconditionally** — `build.rs` only contributes search paths.
> SuiteSparse is the default kernel and the backend is now selected at runtime
> by `GRAPH_MATRIX_KERNEL` / `SparseKernelBackend`, not at compile time.
>
> So **`--features opencypher,graphblas` no longer exists**: pass
> `--features opencypher` and select the kernel by environment.
> `scripts/query_bench.sh` already defaults to `FEATURES=opencypher`.
>
> This note annotates the flag rather than rewriting the plan. Phases 2–6
> target the `rs-graphblas` repo, whose relationship to turbolay this paragraph
> also describes; whether they are still the right phases is a question for
> whoever picks the plan up, not a citation fix.

### Environment (this Mac)

SuiteSparse comes from Homebrew (`suite-sparse 7.12.2`, GraphBLAS **10.3.1**,
OpenMP linked via `libomp`). The `graphblas-sys` build script needs:

```bash
export GRAPHBLAS_INCLUDE_DIR=/opt/homebrew/opt/suite-sparse/include/suitesparse
export GRAPHBLAS_LIB_DIR=/opt/homebrew/opt/suite-sparse/lib
export DYLD_LIBRARY_PATH="$GRAPHBLAS_LIB_DIR${DYLD_LIBRARY_PATH:+:$DYLD_LIBRARY_PATH}"
```

`scripts/falkor_latency_bench.sh` in turbolay already sets these defaults.
rs-graphblas's own `scripts/env.sh`/`verify.sh` assume a Linux `local/` install
that does not exist on this machine — on macOS just export the vars above and
use `cargo test --workspace` directly.

### Mental model of the kernel

All traversal code is in
`rs-graphblas/crates/graphblas-traversal/src/lib.rs` (~1500 lines, sits on raw
`graphblas-sys` FFI). Line numbers below are approximate anchors; search for the
function names.

- **What turbolay does with it:** builds a CSC adjacency (transposed at import)
  from stored edges, compiles it once into a `CompiledGraphBlasMatrix`, and
  caches it in `GraphShard.graphblas_cache` keyed `(cell_id, edge_type,
  base_epoch)` (`turbolay/src/engine/supernode.rs` `cached_graphblas_matrix`,
  ~:1486). Queries then call `expand` / `expand_range` /
  `expand_range_window` / `expand_range_count` many times against the cached
  matrix. So: **compile is the cold path, expand is the hot path.**
- **The hot loop** (`expand_with_compiled` ~:624, `range_result_vector` ~:899):
  classic push BFS. Per hop it issues ~7 FFI calls:
  1. `frontier_edge_visits_graphblas` (~:1235): `GrB_Vector_clear` +
     `GrB_Vector_eWiseMult_BinaryOp(FIRST_UINT64)` + `GrB_Vector_reduce_UINT64`
     — three calls that exist **only** to compute the `edge_visits` statistic.
  2. `GrB_Vector_new` for the next frontier (fresh allocation every hop).
  3. `GrB_mxv` with semiring `GrB_LOR_LAND_SEMIRING_BOOL` — masked by `seen`
     with `GrB_DESC_SC` (structural complement) in `masked_multiply` (~:1166),
     unmasked in `multiply` (~:1194).
  4. `GrB_Vector_nvals`, then `GrB_Vector_eWiseAdd_BinaryOp(GrB_LOR)` to union
     into the result.
- **Matrix build** (`build_transposed_matrix` ~:1046): allocates
  `vec![true; edge_count]`, calls deprecated `GrB_Matrix_import_BOOL` (which
  **copies** pointers, indices, and values), then `GrB_Matrix_wait(MATERIALIZE)`.
- **Init** (`init` ~:978): `GrB_init(GrB_BLOCKING)`; JIT pinned **off** by
  default (Homebrew clang can't find `omp.h`; `GRAPHBLAS_JIT=on|inherit`
  overrides). Leave both alone — changing them is Backlog.
- **Cost model:** sparse mxv is memory-bandwidth-bound (dominated by streaming
  the index arrays), and each GrB call has a fixed FFI/dispatch cost on the
  order of microseconds. The bench query is 16 hops, so per-hop fixed costs
  multiply by 16. The three levers this plan pulls: touch fewer bytes per edge
  (iso values, 32-bit indices), do less work per output (ANY_PAIR semiring),
  and issue fewer calls per hop (optional stats, scratch reuse — explained in
  Phase 7, implemented later).
- **Compact kernel (out of scope):** `GRAPH_COMPILED_KERNEL=compact` routes
  everything to a pure-Rust CSC BFS inside the same crate
  (`use_compact_csc_kernel` ~:1024). We are not benchmarking or improving it.
  Its one relevant tentacle: `new_from_trusted_compact_u32` (~:489) currently
  **widens u32 arrays to u64** before GraphBLAS import — Phase 6 fixes that.
- **Replicas/Mutex:** `CompiledGraphBlasMatrix` wraps
  `Vec<Mutex<Inner>>` with round-robin dispatch; default replica count is 1.
  Known serialization bottleneck under concurrency — **deferred, do not touch.**

### The macro benchmark (turbolay side)

`scripts/falkor_latency_bench.sh` builds a synthetic fixture (fanout=10000,
48 roots, edge type `USER_FOLLOWS_USER`) and times, via
`examples/falkor_query_bench.rs`:

```
MATCH (u {id: $root})-[:USER_FOLLOWS_USER*1..16]->(v) RETURN v.id ORDER BY v.id LIMIT 64
```

in cold (3 iters, fresh open), warm (3 iters, disk cache), hot (30 iters,
held-open shard) phases, cycling roots so caching can't fake results. Output:
`bench/out/falkor_latency_new.csv` (`query_p50/p95/p99`, `end_to_end_*`, `qps`).
This query routes to `expand_range_window` on the compiled matrix.

**Known bug this plan fixes first:** the script sets
`GRAPH_COMPILED_KERNEL=compact` (line ~37), so all published numbers measured
the pure-Rust kernel, **not** GraphBLAS. Phase 1 flips this.

### Correctness gates (run for every code-changing phase)

1. `cd rs-graphblas && cargo test --workspace` (with the env vars above).
2. `cd turbolay && just ci` — or at minimum `just test-opencypher` and
   `just check-examples`. (Was "its graphblas targets: `cargo test --features
   graphblas` and the graphblas example checks". The feature is gone; so are
   `test-graphblas` and `check-examples-graphblas`, deleted in `425bedf` as
   byte-identical to their siblings once the dead flag came off. SuiteSparse now
   links into every build, so every test target already exercises it.)
3. rs-graphblas discipline: FFI surface changes get a matching C baseline test
   in `graphblas-tests/c/` (see repo README; `scripts/check-constants.sh` guards
   constant drift). Consult `GraphBLAS.h` at
   `$GRAPHBLAS_INCLUDE_DIR/GraphBLAS.h` for any API you touch — do not trust
   memory of the C API.

---

## Phase 1 — Make the macro bench measure GraphBLAS, capture baseline

**Repo:** turbolay. **Type:** script fix + measurement. **Depends on:** nothing.

**Goal:** the FalkorDB-comparison bench actually exercises the SuiteSparse
backend, and we have a baseline CSV to compare every later phase against.

**Steps:**
1. In `scripts/falkor_latency_bench.sh`: remove the hardcoded
   `export GRAPH_COMPILED_KERNEL=compact` (make it opt-in via the caller's
   environment instead, defaulting to unset = GraphBLAS backend). Check
   `scripts/query_memory_profile.sh` sets the same var (~line 33) — leave that
   script alone; this phase only changes the latency bench.
2. Add proof-of-backend to the bench output: `examples/falkor_query_bench.rs` /
   `query_bench.rs` already read cache metrics (`graphblas_hits`/`graphblas_misses`).
   After the hot phase, assert `graphblas_hits > 0` and print which backend
   served the traversal (the `SparseKernelBackend` enum is on every
   `SparseTraversal` result at the `src/sparse_kernel.rs` boundary). Fail loudly
   if the RustSparse fallback served any hot-phase query.
3. Run the turbolay side of the bench (no Docker/Falkor needed):
   `scripts/falkor_latency_bench.sh` — build fixture + run. Copy the result to
   `bench/out/falkor_latency_graphblas_baseline.csv` (do not overwrite existing
   CSVs; they are the historical compact-kernel numbers).
4. Record in the commit message: hot `query_p50/p95` for GraphBLAS backend, and
   note the previously published numbers were the compact kernel.

**Verify:** bench completes; backend assertion passes; baseline CSV exists.
**Commit (turbolay):** `bench: run falkor latency bench on GraphBLAS backend, capture baseline`

---

## Phase 2 — Criterion micro-harness in graphblas-traversal

**Repo:** rs-graphblas. **Type:** new benches. **Depends on:** nothing
(parallel-safe with Phase 1).

**Goal:** a repeatable, statistically sound harness that isolates **compile**
(`CompiledGraphBlasMatrix::new_from_csc`) from **expand**
(`expand_range`, `expand_range_window`) so Phases 4–6 can prove their impact.

**Steps:**
1. Add `criterion` as a dev-dependency of `crates/graphblas-traversal`, with
   `[[bench]] name = "expand" harness = false`.
2. Write `crates/graphblas-traversal/benches/expand.rs`:
   - Deterministic synthetic graphs, seeded inline xorshift/LCG (no `rand`
     dependency): uniform-degree and power-law variants. Sizes: |V| ∈
     {10k, 100k, 1M}, avg out-degree ∈ {4, 16}; plus one fixture mirroring the
     turbolay bench shape (fanout tree, 10k fanout, depth ≥ 2).
   - Bench groups: `compile` (matrix build from CSC), `expand_range` at hops
     {1, 3, 8, 16} from 1 and 48 start vertices, and `expand_range_window`
     (window: LIMIT 64 ascending — mirrors the macro bench). Note
     `expand_range_window`/`expand_range_count` are behind the crate's
     `opencypher` feature — enable it for the bench.
   - GraphBLAS backend only: never set `GRAPH_COMPILED_KERNEL`; assert the
     result's `backend == SparseKernelBackend::SuiteSparseGraphBlas`.
   - Reduce sample count for the 1M-vertex group so a full run stays under
     ~10 minutes.
3. Document the run command at the top of the bench file (env vars from Shared
   context, `cargo bench -p graphblas-traversal --features opencypher`).
4. Run it once and save the criterion baseline:
   `cargo bench ... -- --save-baseline pre-opt`. Later phases compare with
   `--baseline pre-opt`.

**Verify:** `cargo bench` runs green; baseline saved under `target/criterion`.
**Commit (rs-graphblas):** `bench: criterion harness for compile/expand on the GraphBLAS backend`

---

## Phase 3 — Profiling analysis: flamegraph + burble *(Opus agent, analysis doc)*

**Repo:** rs-graphblas (profiling target) + turbolay (doc lands here).
**Type:** investigation; the only code committed is a small profiling example.
**Depends on:** Phase 2 (reuses its graph generators; can start once the bench
file exists).

**Goal:** know where hot-path time actually goes — Rust glue vs. FFI dispatch
vs. which SuiteSparse kernels — before/while landing Phases 4–6. Deliverable is
a written analysis, not code changes.

**Steps:**
1. Add `crates/graphblas-traversal/examples/profile_expand.rs`: builds one
   compiled matrix (turbolay-bench-shaped graph), then loops
   `expand_range_window` (hops 1..16, LIMIT 64) for a duration set by env/argv
   (default ~20s). Reuse Phase 2's generator (extract to a small shared module
   if needed).
2. **CPU profile:** prefer `samply` (`cargo install samply`;
   `samply record --save-only -o profile.json ./target/release/examples/profile_expand`),
   fall back to `cargo flamegraph` (needs `sudo` for dtrace on macOS). Build
   with `--release`. Analyze the profile programmatically (samply's JSON is
   Firefox Profiler format) — extract the top self-time symbols split into:
   libgraphblas symbols (`GB_*`), traversal-crate Rust symbols, allocator
   symbols, everything else.
3. **Kernel-level view:** enable burble for one run — call
   `graphblas_sys::GxB_Global_Option_set_INT32(GxB_BURBLE, 1)` right after init
   (add a temporary hook or an env-gated line in the example; `GxB_BURBLE` is
   bound in `graphblas-sys/src/lib.rs`). Capture stdout: SuiteSparse prints
   which kernel each op selects (factory vs. generic — remember JIT is off),
   sparsity-format transitions (sparse/bitmap/full), and per-op timing.
4. Write `turbolay/docs/optimisations/profiling-analysis.md`:
   - Time split: % in `GB_*` kernels vs. Rust glue vs. allocation; per-hop cost
     breakdown; how much the 3-call `edge_visits` accounting costs; whether
     mxv is bandwidth- or dispatch-dominated at each graph size.
   - Burble findings: kernels chosen, any "generic" (slow-path) kernels hit,
     format switches per hop.
   - Ranked recommendations, explicitly mapped onto Phases 4–7 and Backlog
     items ("confirms/refutes phase N's premise, expected X%").
5. Commit the example in rs-graphblas; commit the doc in turbolay.

**Verify:** doc exists with quantified findings; profile artifacts saved under
the scratchpad or `bench/out/` (not committed if large).
**Commits:** rs-graphblas `bench: profiling example for expand hot loop`;
turbolay `docs: GraphBLAS hot-path profiling analysis`.

---

## Phase 4 — ANY_PAIR semiring

**Repo:** rs-graphblas. **Type:** small perf change. **Depends on:** Phase 2
(for numbers); land before Phase 5.

**Goal:** replace `GrB_LOR_LAND_SEMIRING_BOOL` with `GxB_ANY_PAIR_BOOL` in the
BFS mxv — the canonical LAGraph BFS semiring.

**Mental model:** for reachability we only care *whether* a vertex is reached.
`LOR_LAND` reads both operand values and combines all contributions; `ANY`
short-circuits after the first contributing edge per output vertex, and `PAIR`
returns 1 without reading value arrays at all. For BOOL reachability the result
is bit-identical (`ANY` over all-true contributions is deterministically true),
so this is a pure win, and it compounds with Phase 5 (iso values are never even
loaded).

**Steps:**
1. In `crates/graphblas-traversal/src/lib.rs`, swap the semiring in
   `masked_multiply` (~:1166) and `multiply` (~:1194) to the `ANY_PAIR` BOOL
   semiring. The sys binding is stamped in `graphblas-sys/src/semiring.rs`
   (`GxB_ANY_PAIR` for BOOL/INT8/UINT8) — confirm the exact exported symbol
   name and that `scripts/check-constants.sh` / the API-inventory tooling stays
   green.
2. Run correctness gates (both repos).
3. Criterion: `cargo bench ... -- --baseline pre-opt`; record deltas for
   `expand_range`/`expand_range_window` groups in the commit message.

**Verify:** all tests green; identical traversal results; criterion delta recorded.
**Commit (rs-graphblas):** `perf(traversal): ANY_PAIR_BOOL semiring for BFS expansion`

---

## Phase 5 — Iso-valued, zero-copy matrix build *(separate commit — do not fold into 4 or 6)*

**Repo:** rs-graphblas. **Type:** perf change, cold path + memory.
**Depends on:** Phase 4 landed (so deltas are attributable).

**Goal:** stop copying three arrays and allocating `vec![true; E]` at compile
time; store the adjacency as an **iso-valued** boolean matrix (SuiteSparse
stores a single value for the whole matrix).

**Mental model:** `GrB_Matrix_import_BOOL` (deprecated) deep-copies pointers,
indices, and values. GraphBLAS v10's Container API
(`GxB_Container` + `GxB_load_Matrix_from_Container`, bound in
`graphblas-sys/src/container.rs`; opaque struct fields transcribed in
`graphblas-sys/src/lib.rs` ~:128) moves buffers instead. With **read-only
load** (`GxB_IS_READONLY` handling, as used by the safe wrapper's
`Vector::load_readonly_slice`, `graphblas/src/vector.rs:349`), GraphBLAS
borrows caller memory and never frees it — which means the Rust `Vec`s must be
kept alive as long as the matrix; store them inside
`CompiledGraphBlasMatrixInner` alongside the handle. An iso matrix eliminates
the O(E) values array entirely: less memory, less bandwidth per mxv, and with
Phase 4's PAIR semiring values are never read anyway.

**Steps:**
1. Read the container section of `GraphBLAS.h` (v10.3) carefully — the load
   protocol (which container fields for CSC: `p`, `i`, `x`, `format`,
   `orientation`, `iso`, `jumbled`, dimensions) must come from the header/User
   Guide, not memory. Add a matching C baseline test in `graphblas-tests/c/`
   demonstrating an iso CSC container load + mxv (repo discipline).
2. Rework `build_transposed_matrix` (~:1046) to container-based read-only load
   with `iso = true` (x array length 1, value `true`). Restructure ownership so
   the CSC buffers live in `CompiledGraphBlasMatrixInner`. Note the free-fn
   `expand`/`expand_range` entry points (~:586) build a matrix from a borrowed
   `&GraphBlasCsc` — they either copy into owned buffers or keep the old import
   path; keep it simple and correct, the cached path is what matters.
3. Keep `GrB_Matrix_wait(MATERIALIZE)` after load. Verify with a
   `GxB_Matrix_fprint`-level check in tests that the matrix is iso and CSC.
4. Gates + criterion (`compile` group especially — expect a large drop; expand
   groups should improve modestly from bandwidth). Optionally rerun the Phase 1
   macro bench and note cold-phase movement.

**Verify:** tests green in both repos; no leaks (run the traversal tests under
`leaks`/Instruments or valgrind-on-Linux if convenient — read-only load must
not double-free).
**Commit (rs-graphblas):** `perf(traversal): iso-valued zero-copy container load for adjacency build`

---

## Phase 6 — Native 32-bit indices end-to-end

**Repo:** rs-graphblas (+ small turbolay touch if plumbing requires).
**Type:** perf change, bandwidth. **Depends on:** Phase 5 (container load is
the mechanism that admits 32-bit arrays).

**Goal:** feed turbolay's compact u32 CSC artifacts to GraphBLAS as real 32-bit
index arrays instead of widening them to u64.

**Mental model:** GraphBLAS v10 supports 32-bit integer row/column index
storage natively (its headline feature). Sparse mxv streams the index arrays,
so halving index width ≈ halves the dominant memory traffic. Today
`new_from_trusted_compact_u32` (~:489–509) maps `Vec<u32> → Vec<u64>` before
import — doubling memory on exactly the path designed to be compact. With the
Phase 5 container flow, the `p` and `i` container components can be
`GrB_UINT32` vectors wrapping the u32 buffers directly.

**Steps:**
1. Consult the v10 User Guide / `GraphBLAS.h` on 32/64-bit integer matrices
   (per-matrix integer controls and container component types). C baseline test
   first in `graphblas-tests/c/` (u32 `p`/`i` container load + mxv + extract).
2. Extend the Phase 5 load path to accept u32 pointers/indices; route
   `new_from_trusted_compact_u32` through it with **no widening**. The u64
   `GraphBlasCsc` path stays as-is (or downcasts when values fit — optional,
   only if simple).
3. Check extraction still returns u64 ordinals (`extract_ordinals` ~:1277 uses
   `GrB_Vector_extractTuples_BOOL` with `GrB_Index` arrays — vectors/ordinals
   are unaffected by matrix index width, but verify against the header).
4. Gates + criterion with `--baseline pre-opt`; also rerun the Phase 1 macro
   bench (turbolay's compact artifacts feed this path via
   `cached_graphblas_matrix` → `new_from_trusted_compact_u32`,
   `src/engine/supernode.rs` ~:1506) and record hot p50/p95 vs. the Phase 1
   baseline CSV.

**Verify:** tests green both repos; macro bench delta recorded.
**Commit (rs-graphblas):** `perf(traversal): native 32-bit index load for compact CSC artifacts`

---

## Phase 7 — Explainer doc: per-hop overhead *(doc only, parallel-safe)*

**Repo:** turbolay. **Type:** documentation. **Depends on:** nothing (Phase 3's
numbers enrich it if available — reference them, don't block on them).

**Goal:** `turbolay/docs/optimisations/per-hop-overhead.md` explaining item 5
from the review — what the hot loop pays per hop and how we'd cut it — so the
team can decide when to schedule the implementation.

**Contents to cover (audience: teammates who haven't read the kernel):**
1. Walk the per-hop FFI call sequence in `expand_with_compiled` /
   `range_result_vector` (see Shared context mental model): name each of the ~7
   calls and what it does.
2. The `edge_visits` accounting: three calls per hop
   (`GrB_Vector_clear` + `eWiseMult(FIRST_UINT64)` + `reduce`) that exist only
   to produce a statistic; for the 16-hop LIMIT-64 bench query that's up to 48
   extra FFI ops per query. Proposal: make stats opt-in
   (e.g. `expand_range_with_stats` variant or a flag on the compiled matrix),
   with the default path skipping them.
3. Per-hop `GrB_Vector_new`: fresh allocation each hop; proposal: preallocate
   scratch/output vectors once per call (or per compiled matrix with care) and
   reuse via clear/replace semantics.
4. Fixed FFI/dispatch cost per GrB call (~µs order) vs. actual kernel work —
   why this matters most for deep-hop, small-frontier queries and barely at all
   for huge frontiers. Pull measured numbers from
   `docs/optimisations/profiling-analysis.md` if Phase 3 has landed.
5. Expected impact estimate and a proposed implementation sketch, so this can
   become a future phase.

**Verify:** doc reads standalone; file paths/line anchors correct.
**Commit (turbolay):** `docs: per-hop FFI overhead in the GraphBLAS traversal kernel`

---

## Backlog (noted for future — explicitly not scheduled)

- **Shared read-only matrix / concurrency redesign:** replace
  `Vec<Mutex<Inner>>` + replicas with one materialized matrix shared across
  threads (`unsafe impl Sync` + documented invariant: materialized via
  `GrB_Matrix_wait`, never mutated after build). Pair with a concurrency
  criterion bench (N threads, one matrix). Currently every concurrent query on
  a shard serializes on one Mutex.
- **Non-blocking init:** `GrB_init(GrB_NONBLOCKING)` instead of blocking mode.
- **JIT enablement:** fix the Homebrew toolchain issue by passing
  `-I/opt/homebrew/opt/libomp/include` via `GxB_JIT_C_COMPILER_FLAGS` instead
  of pinning the JIT off; benchmark `GRAPHBLAS_JIT=on`. JIT fuses mask+semiring
  kernels the factory set doesn't cover.
- **Thread tuning:** OpenMP defaults to all cores — right for huge single
  queries, wrong for small latency-critical ones and oversubscribed under
  concurrent load. Benchmark `GxB_NTHREADS=1` vs. default for the server shape.
- **Backend size heuristic (item 8):** pick compact-Rust vs. GraphBLAS per
  matrix by edge count (measure the crossover with the Phase 2 harness) instead
  of the global `GRAPH_COMPILED_KERNEL` env var.
- **Release-profile tuning:** `[profile.release] lto = "thin",
  codegen-units = 1` in both workspaces (mainly helps Rust-side glue).
- **Safety track (from the unsafe review, separate effort):** port
  `graphblas-traversal` onto the safe `graphblas` wrapper ("step 2b");
  lifetime-parameterize `Vector::load_readonly_slice` and `Type`;
  `#![deny(unsafe_op_in_unsafe_fn)]` + `undocumented_unsafe_blocks` lint;
  differential/property tests between backends.
