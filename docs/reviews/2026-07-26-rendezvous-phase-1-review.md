---
title: Rendezvous placement Phase 1 — implementation review
date: 2026-07-26
branch: Turbolay-V3.5
base_commit: ea942ec
head_commit: b638fd8
plan: docs/plans/2026-07-25-rendezvous-placement.md
status: phase-1-complete
tags:
  - routing
  - placement
  - single-writer
  - fencing
  - review
---

# Rendezvous placement Phase 1 — implementation review

Covers `ea942ec..b638fd8`. **All seven commits of §7.8 are done** and Phase 1 is
complete. What follows is what shipped, what changed in the plan while shipping
it, and the one real defect found along the way.

## Where the seven commits stand

| # | Commit | State |
|---|---|---|
| 1 | crate — `heartbeat.rs`, `liveness.rs` | **done** — `475c473` |
| 2 | readiness-gated publisher | **done** — `efc4c5a` |
| 3 | (a) routing names the owner, probe deleted | **done** — `dfbdf02` |
| 4 | (b)+(c) don't-promote rule + `NotALeader` | **done** — `dfbdf02` |
| 5 | (d) fence backoff | **done** — `8fa865f`, reworked by `efc4c5a` |
| 6 | 3a advisory record | **done** — `dddc1ec` |
| 7 | regression test | **done** — `dddc1ec` |

`dfbdf02` is where behaviour changes. It also pulled in touch point (e), which
was not optional: `graph-node.rs` called `with_readiness_port`, which decision 4
deletes, so the tree could not compile without it — and a routing provider with
no live set would have made commits 3 and 4 a no-op in production.

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

- There is **no fence retry at all**, where sleet's is an unbounded loop — write
  path, not daemon, so the wait is recorded and the client's own retry spends it,
  through the gate that re-derives ownership first.
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

136 lib tests, 303 under `server-runtime`, 67 placement, 11 bin, clippy clean on
both packages including `--features server-runtime,indexer-runtime`, and
`cargo fmt --all --check` clean. All of it through `just`, which now exports the
build environment those feature sets need.

The lib suite runs in 0.14s. It passed through 5.07s and 5.28s on the way, twice
for the same underlying reason — a fenced writer's wait was a real one at the
production 5s. The first time the sleeps were inside the retry loop the
commit-5 rework deleted; the second time they were the gate's own wait, served
honestly by a test using `GraphOpenOptions::default()`. That is what finished
decision 5's wiring: `fence_backoff_interval` is now a field, `Default` is
hand-written because `Duration::default()` is zero and a zero wait would let a
fenced writer re-open immediately, and the fencing tests pace in milliseconds.

**Two "known pre-existing failures" were recorded here, and both were the local
environment rather than defects.** Corrected in the review pass below; neither
survives.

**Gaps:**

- The LIST-transport-failure half of "empty vs failed must not share a path" is
  covered in `fault_store.rs`'s own tests, but not in `heartbeat.rs`'s —
  `InMemory` cannot fail, and `FaultStore` is `#[cfg(test)]` inside the placement
  crate so the kernel cannot import it. If that wall is hit a third time,
  promoting it to a `crates/test-support` member is the fix decision 11 already
  anticipates. **It has been hit twice more since**, so this is now the
  recommended next move — see decision 11 in the plan.
- `--features server-runtime` needs `BINDGEN_EXTRA_CLANG_ARGS=-I/opt/homebrew/include`
  and `LIBRARY_PATH=/opt/homebrew/lib` on macOS. CI installs
  `libcypher-parser-dev` via apt and needs neither. Nothing documents the local
  equivalent; it belongs in the `justfile`. **Fixed in `d3ce0ca`.**

## The regression test, and why it discriminates

`three_nodes_writing_one_cell_at_once_leave_one_writer_and_a_bounded_epoch` is
the incident reproduced: three promotable nodes, one cell, twelve concurrent
writes, all sharing one fleet view. Three details are what make it a test rather
than a demonstration.

- The expected owner is computed independently from `hash::owner` rather than
  observed, so it would fail if ownership were settled by a coin flip.
- It asserts writer *handles* — `db.writer().is_ok()` false on both peers — not
  just the returned error. A refusal that still opened the writer **is** the
  duel, and checking the error alone would miss exactly that.
- The bound is `1 <= epoch <= 2`: one promotion plus one spare, because decision
  6.3 permits a fenced node to re-fence the winner once before converging.
  Asserting a single bump would fail correct code. Observed is 1; the pre-fix
  behaviour reaches 3 after the first of four rounds, so the ceiling still fails
  the old code. 33 runs, no flakes.

## The review pass, `d3ce0ca..b638fd8`

A second read of the shipped code against both plan documents. Three things it
found, and what happened to each.

**Decision 7 was built, tested, and not connected.** `heartbeat_action` and
`is_ready` had no consumer outside `engine::placement`: the publisher gated on
the lifecycle `AtomicBool` alone, and nothing about a shed view touches it. So a
node past the grace window refused every promotion *and kept publishing* —
precisely the permanent-refusal trap the decision, `ViewState::Shed`'s own
docs, and `fault_store.rs`'s module header all describe. Two supporting claims
were false with it: §5's two `FaultStore`-backed rows named tests that do not
exist (`FaultStore`'s twelve tests all test `FaultStore`), and
`engine/placement.rs`'s module docs asserted the wiring. Fixed in `f0a1843` —
`NodeReadiness` is lifecycle **and** placement, read by the publisher and by
`/readyz`.

It is the same shape as the fence defect this review already documents: a rule
correct in the library, enforced by a caller that was never written, with green
tests on both sides of the gap.

**The reverted rule-3 design outlived its revert, in two documents.** The doc
comment above `refresh_writer_fence` still described the retry loop `efc4c5a`
deleted — "waits one heartbeat interval, re-promotes, the retry is
unconditional" — immediately above the paragraph saying the function never
promotes; and §7.6 still described the four-attempt cap. Both now describe the
gate (`b638fd8`).

**The re-open ladder was armed where it could never be spent.** It was armed
only from the non-fence error arm of `refresh_writer_fence`, which leaves the
writer open, and `await_writer_reopen` returns early whenever a writer is open —
so every wait it armed was skipped and the `AdmissionRejected` cap could not
fire. Moved to `promote_writer`, where a re-open actually happens (`b638fd8`).

**Corrections to this document's own verification section.** Neither recorded
failure was a defect. The stack overflow in
`cypher_relationship_properties_are_indexed_mutable_and_snapshot_safe` is a
missing `RUST_MIN_STACK` — `ci.yml` sets 8 MiB on both OpenCypher test jobs and
the `justfile` did not; the test passes at that stack size. The
`useless_conversion` at `opencypher.rs:361` is real on macOS and required on
Linux, because bindgen types the constant per platform; it is now an annotated
`#[allow]` rather than a standing lint failure. With `d3ce0ca` exporting all
three variables, `just ci` is clean on macOS.

## What is left

Phase 2 — cell addressability on the wire — and the §6 follow-ups: forwarding
over `QueryServiceEndpoint` for HTTP clients, and a minimum-tenure rule if
flapping proves painful. Rendezvous moves ownership the instant the live set
changes, so a flapping node reclaims its cells each time it returns, at one
writer open per reclaim.

Four smaller items the review pass raised and did not act on:

- **A shed node answers `ROUTE` with `Neo.ClientError.Statement.InvalidSyntax`**
  (`bolt_config_error` → `UnsupportedQuery`). Decision 7 makes shedding routine,
  and a driver receiving a client error from the routing procedure will not fail
  over to the next router. A transient code is the right answer;
  `bolt/tests.rs`'s `object_store_routing_advertises_nothing_when_no_live_node_is_addressable`
  pins the current one and would change with it.
- **`GRAPH_HEARTBEAT_INTERVAL_MS` / `_TIMEOUT_MS` appear in no chart, manifest
  or doc.** Decision 5 chose config over constants precisely because the cost
  scales with fleet size; operationally they are still constants.
- **No demotion path.** A node that loses ownership keeps its SlateDB handle
  until the new owner's first write fences it.
- **Decision 8's startup window can name a gracefully-removed peer** as a cell's
  owner for up to one `heartbeat_timeout` after a restart, refusing writes with a
  hint pointing at a node that is gone. Bounded, and the right trade against the
  alternative, but the plan argues the decision only from the other direction.
