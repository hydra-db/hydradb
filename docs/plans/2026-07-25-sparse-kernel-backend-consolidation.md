---
title: Sparse kernel backend consolidation
status: step-1-complete
date: 2026-07-25
branch: Turbolay-V3.5
base_commit: 989cc72
head_commit: 73309df
tags:
  - sparse-kernel
  - graphblas
  - build
  - refactor
---

# Sparse kernel backend consolidation

Goal: make SuiteSparse GraphBLAS the default traversal kernel, keep the ability
to switch to a Rust kernel, and get the number of places that encode that choice
down to as close to one as possible.

## What we found first

There are **three** kernels, not two, and they form a ladder rather than a pair
of interchangeable backends. The full capability and cost matrix now lives in
the module docs at `src/sparse_kernel/mod.rs`; the summary is:

| # | Kernel | Compiled? | Contains C? |
|---|--------|-----------|-------------|
| 1 | Adjacency BFS (`expand_rust`) | no | no |
| 2 | Compact CSC BFS (`CompiledCompactCscMatrix`) | yes | no |
| 3 | SuiteSparse GraphBLAS (`expand_with_compiled`) | yes | yes |

Kernel 2 lives in `graphblas.rs` because it shares that module's compiled-matrix
representation, not because it needs SuiteSparse.

Kernels 2 and 3 are capability-identical. Kernel 1 is a strict subset — no count
pushdown, no window pushdown, no `contains_edge`, no matrix caching. So the real
fork in the codebase is **compiled vs uncompiled**, not "Rust vs GraphBLAS", and
dropping to kernel 1 is a capability downgrade rather than merely a slower path.

The choice was encoded three incompatible ways:

| Representation | Bind time | Sites |
|---|---|---|
| `#[cfg(feature = "graphblas")]` | compile | 101 |
| `SparseKernelBackend` enum | runtime | ~20 |
| `use_compact_kernel: bool` | runtime, per-matrix | 5 branches |

A `bool` cannot express a third compiled engine, so adding one meant editing all
five branches. That is the thing worth fixing.

## Step 1 — delete the cargo feature (done)

SuiteSparse ships in production regardless, so the feature bought nothing while
costing 101 conditional sites across 12 files. It was also the only one of the
three representations that could not be changed at runtime.

| Commit | Change |
|---|---|
| `c22aaad` | `sparse_kernel.rs` → `sparse_kernel/mod.rs`, plus the kernel-ladder docs |
| `f80d9c6` | Deleted the `graphblas` feature; 101 conditional sites → 0 |
| `5a40f6a` | Removed the 4 functions the strip made unreachable, and the examples' feature branches |
| `dd7852f` | build.rs: emit link search paths unconditionally |
| `73309df` | GraphBLAS thread count moves from descriptor to global; CI, scripts, README |

Behaviour is unchanged, because `default` already enabled the feature. Kernel 1
stays reachable at runtime via `SparseKernelBackend::RustSparse` and the
`None`-fallthrough in `shard::query`; kernel 2 via `GRAPH_COMPILED_KERNEL=compact`.

### Two bugs surfaced on the way

**build.rs early-returned on `CARGO_FEATURE_GRAPHBLAS`.** Deleting the feature
removed that variable, so the `-L` search path stopped being emitted and linking
failed with `ld: library 'graphblas' not found`. `cargo check` does not link, so
a check-only CI gate would not have caught this.

**GraphBLAS v9 stopped accepting `GxB_NTHREADS` as a descriptor field.**
`GxB_Desc_set_INT32` returns `GrB_INVALID_VALUE` (-3) for it on v10.3.1 —
verified directly against libgraphblas from C with both the old constant (5) and
the current one (7086). It is not a constant mismatch: descriptors no longer
carry a thread count at all, despite the v10 header still listing `GxB_NTHREADS`
under `GrB_Desc_Field`.

`exact_count_descriptor()` built such a descriptor for every compiled matrix, so
`build_compiled_inner` failed outright and **the entire compiled GraphBLAS path
was dead on any SuiteSparse >= v9**. This is pre-existing — it reproduces
identically at `989cc72`. It accounted for all 17 failing lib tests.

The fix moves the thread count to `GrB_Global_set_INT32` at init. Two behaviour
consequences: it is applied only when `GRAPHBLAS_NTHREADS` (or the older
`GRAPHBLAS_EXACT_COUNT_THREADS`) is set, so the default is now GraphBLAS's own
one-thread-per-core rather than the single thread the descriptor asked for; and
it is global rather than scoped to the exact-count path. Scoping it back would
mean adopting `GxB_Context`.

### Verification

| Feature set | Before | After |
|---|---|---|
| default | 83 passed / 17 failed | 100 / 0 |
| `chaos-harness` | — | 100 / 0 |
| `opencypher` | — | 189 / 0 |
| full native, `--all-targets` | — | 16 binaries, all green |

`cargo clippy --locked --all-targets -- -D warnings` passes on the default and
`chaos-harness` feature sets. Regression-checked by building a worktree at
`989cc72` and diffing the failing test-name sets.

## Step 2 — one runtime switch (not started)

1. Replace `SparseKernelBackend` (2 variants) and `use_compact_kernel: bool`
   with a single 3-valued `SparseKernel` enum: `Adjacency`, `CompactCsc`,
   `SuiteSparse`.
2. Add a `sparse_kernel: SparseKernel` field to `GraphCachePolicy`, defaulting
   to `SuiteSparse`, parsed in `bin/graph_node/config.rs` beside
   `max_graphblas_matrices`.
3. Make `CompiledGraphBlasMatrix::new_from_csc` take the kernel as a parameter
   instead of reading `use_compact_csc_kernel()` from the environment. Config is
   read **once**, at matrix-compile time, and baked into the artifact.
4. Have `default_matrix_kernel()` return the configured value. `Adjacency` makes
   the compiled builders skip and `shard::query` return `None`, which the
   existing fallthrough already handles.
5. Report the executed kernel truthfully on `SparseTraversal.backend`. Kernel 2
   currently reports `RustSparse`, indistinguishable from kernel 1, so telemetry
   cannot today tell whether a query ran compiled or uncompiled.
6. Verify by running the suite once per kernel value.

## Step 3 — `CompiledKernel` trait (optional until a fourth engine appears)

Kernels 2 and 3 are capability-identical and already have five one-to-one method
pairs, dispatched today by five `if self.use_compact_kernel` branches. A trait
collapses those to one dispatch, after which adding an engine is one variant,
one `impl`, and one match arm — no call-site edits.

The trait must **not** span kernel 1. It lacks count, window, `contains_edge`
and caching entirely, so a trait over all three would be half `Option`-returning
stubs. Kernel 1 stays the `None`-fallthrough lane.

## Open items

- `docs/plans/optimisation-phases.md` still references `--features graphblas` in
  three places.
- `src/query/opencypher.rs:361` trips `clippy::useless_conversion` locally under
  `--features opencypher`. It is bindgen-type-dependent — the constant is
  already `u32` from Homebrew's libcypher-parser 0.6.2 but may not be against
  Ubuntu's `libcypher-parser-dev` — so removing `.into()` could break CI.
- The `macos-default` CI job had no SuiteSparse install and runs `cargo test`,
  which links. Since `default` already included `graphblas`, that job was
  probably already failing; it now installs `suite-sparse`. Worth checking its
  history.
- Kernel 3's advantage over kernel 2 rests on `mxv` efficiency, not parallelism,
  and has not been benchmarked since the threading fix. Worth measuring before
  treating `SuiteSparse` as the permanent default.
