---
title: Rendezvous placement Phase 1 — implementation review
date: 2026-07-26
branch: Turbolay-V3.5
base_commit: ea942ec
head_commit: efc4c5a
plan: docs/plans/2026-07-25-rendezvous-placement.md
status: commits-1-2-5-complete
tags:
  - routing
  - placement
  - single-writer
  - fencing
  - review
---

# Rendezvous placement Phase 1 — implementation review

Covers `ea942ec..efc4c5a`: six commits, 15 files, ~4150 insertions. Three of the
seven commits in §7.8 of the plan are done. What follows is what shipped, what
changed in the plan while shipping it, and the one real defect found along the
way.

## Where the seven commits stand

| # | Commit | State |
|---|---|---|
| 1 | crate — `heartbeat.rs`, `liveness.rs` | **done** — `475c473` |
| 2 | readiness-gated publisher | **done** — `efc4c5a` |
| 3 | (a) routing names the owner, probe deleted | in progress |
| 4 | (b)+(c) don't-promote rule + `NotALeader` | in progress |
| 5 | (d) fence backoff | **done** — `8fa865f`, reworked by `efc4c5a` |
| 6 | 3a advisory record | not started |
| 7 | regression test | not started |

Nothing has client-visible effect yet. The publisher writes objects nothing
reads; the placement seam is built and tested but not yet wired into either
consumer. Commits 3 and 4 are where that changes, and they are the pair the plan
says must not be separated.

## The defect worth reading about

`8fa865f` shipped a regression, and the shape of the mistake generalises.

Decision 6 takes its three rules from sleet's `supervise_with`, the third being
*retry unconditionally, without re-checking ownership*. That was implemented
literally: a retry loop inside `refresh_writer_fence` that called
`install_writer` directly on a fence.

Two pre-existing tests caught it —
`routed_cluster_uses_slatedb_writer_fencing` and
`second_writer_open_fences_first_writer_instance` — with
`unwrap_err()` on `Ok(CommitResult { epoch: 3 })`. **The fenced writer took the
epoch straight back.** The commit shipped with both failing because only
`cargo test --lib core::state` was run, not the lib suite.

The failing assertion was the smaller half. `install_writer` walks past
`ensure_local_writer`, which is the single place ownership is checked. Commit 4
would have landed its ownership rule and been *silently defeated* by this loop: a
fenced non-owner would re-promote regardless. The one branch the entire plan
exists to add would have been dead on arrival, with every test still green
because commit 4's own tests would have exercised the gate, not the loop beneath
it.

Rule 3 is not portable. Sleet's retry is a **daemon supervisor**: nothing waits
on it, and its reconcile loop cancels the task out of band when ownership moves,
so a blind retry can never outlive the node's claim to the work. This sits on the
**write path**, where no canceller exists.

The fix inverts rule 3 and keeps 1 and 2 exactly. `refresh_writer_fence` drops
the handle, arms a `WriterReopenGate` and returns the error; it never promotes.
`ensure_local_writer` becomes the single promotion gate — ownership first, then
pacing, because a non-owner must be refused outright rather than asked to wait.

The near-miss: the daemon-vs-write-path difference *had* already been noticed,
and was the stated reason for bounding the loop at four attempts. The same
reasoning was simply not carried to rule 3, where it mattered far more. Noticing
a structural difference in one place is not the same as applying it everywhere it
bears.

## Plan changes made while implementing

Four decisions were settled before commit 1 (`a430d12`) and three were revised by
contact with the code.

- **Decision 10 improved.** The original said "pass `now` everywhere". Sleet's
  actual shape is better: collapse the timestamp to a `Duration` age at the LIST
  boundary, and every function below is pure and time-free. It costs one
  parameter on one function instead of many, and it is why the liveness tests are
  plain `#[test]`s with hand-built entries — no store, no clock, no sleeping.
- **Touch point (b) had three arms; it needs four.** "No live owner" (promote)
  and "I have shed my view" (refuse) are opposite answers, and the original
  sketch had no arm for the second. Collapsing them is precisely how decision 7's
  permanent-refusal trap gets built by accident. `CellOwnership` keeps them
  apart, and `LiveView::candidates()` returns `Option` so the promote arm is
  unreachable from a shed view. Consequence: `NotCellWriter`'s owner is
  `Option<String>`.
- **Decision 6 rule 3 inverted**, as above.

## Deviations from the plan, all recorded in it

- The fence retry is **bounded**, where sleet's is unbounded — write path, not
  daemon, so an unbounded loop hangs a client request.
- Placement's module is `liveness.rs`, not `directory.rs`, because the kernel
  already has an `ObjectStoreNodeDirectory` and it is the *other* operand of the
  intersection.
- `src/placement.rs` → `src/locality.rs`. It contained `LocalityCellExtractor`,
  `StorageLayout` and prefix extractors — **no placement logic at all** — and the
  kernel now has a real placement module. The module is private, so nothing
  outside the crate changed. This is decision 12's problem again; the plan chose
  to rename the new thing there only because the old name was a public
  re-export, which was not true here.

## Three latent dependency breakages, all fixed

A pattern worth naming, because it recurred three times and each instance
compiled fine:

| Dependency | Arrived via | Would break when |
|---|---|---|
| `chrono/serde` | feature unification with `object_store` | `object_store` stops enabling it |
| `tokio/test-util` | `fail-parallel`, a slatedb dependency | that transitive path changes |
| `chrono` itself | not available at all | — two agents independently wrote `SystemTime::now().into()` to route around it |

All three now declared explicitly. None changed `Cargo.lock`, which confirms they
were already resolving that way — the declarations are insurance, not fixes.
Versions were also hoisted into `[workspace.dependencies]` (`409ebdc`),
version-neutrally, so a member cannot drift from the kernel — which matters most
for `slatedb`, since placement depends on it solely so `Arc<dyn ObjectStore>`
names the same type on both sides of the crate boundary.

## Verification

125 lib tests, 51 placement, 9 bin, clippy clean on both packages, `cargo fmt
--all --check` clean. The lib suite runs in 0.13s, down from 5.07s before the
commit-5 rework, because the retry loop's sleeps were real.

**Known pre-existing failures, confirmed at `ea942ec` and unrelated to this
work:** `useless_conversion` at `src/query/opencypher.rs:361` under
`--features opencypher` (macOS-specific; the file is untouched since `215bed9`),
and a stack overflow in
`cypher_relationship_properties_are_indexed_mutable_and_snapshot_safe`. Both
verified in a clean worktree rather than accepted on assertion.

**Gaps:**

- The LIST-transport-failure half of "empty vs failed must not share a path" is
  covered in `fault_store.rs`'s own tests, but not in `heartbeat.rs`'s —
  `InMemory` cannot fail, and `FaultStore` is `#[cfg(test)]` inside the placement
  crate so the kernel cannot import it. If that wall is hit a third time,
  promoting it to a `crates/test-support` member is the fix decision 11 already
  anticipates.
- `--features server-runtime` needs `BINDGEN_EXTRA_CLANG_ARGS=-I/opt/homebrew/include`
  and `LIBRARY_PATH=/opt/homebrew/lib` on macOS. CI installs
  `libcypher-parser-dev` via apt and needs neither. Nothing documents the local
  equivalent; it belongs in the `justfile`.

## What is left

Commits 3 and 4 in progress. Then 6 (the advisory record at
`_cell_writers/v1/<cell_id>`, read on exactly two off-path sites and never
consulted to decide a promotion) and 7 (three promotable nodes, one cell,
concurrent writes).

Commit 7 asserts **bounded epoch growth, not zero re-fences** — decision 6.3
permits one re-fence before convergence, so a stricter assertion would fail
correct code.
