---
title: Split the CI test job into a feature-set matrix
status: draft-for-review
date: 2026-07-29
branch: feat/telemetry-log-schema
base_commit: 8a4ce1f
tags:
  - ci
  - build-time
---

## Sources

- `.github/workflows/ci.yml` — the `test` job this plan reshapes. Step names
  here quote it verbatim.
- Step timings below come from the last green run of that job on
  `feat/telemetry-log-schema` (2026-07-28T16:38Z), read via
  `gh api repos/:owner/:repo/actions/runs/<id>/jobs`. Re-measure before acting
  on the balance; the shard boundaries are chosen from these numbers.
- `scripts/runtime_smoke.sh:7` and `scripts/bolt_neo4j_driver_smoke.sh:10` —
  the feature each smoke script builds, which is what pins it to a shard.

## Why

`test` is the critical path of every push at **18m18s**. Wall clock for the
whole push is ~18 min; nothing else comes close (docker 10m16s, macOS 4m43s,
TCK 3m38s, helm 14s). Essentially all of it is compile time — 22 sequential
`cargo` invocations sharing one target directory.

Adding `Swatinem/rust-cache` (landed on this branch) removes the cold
dependency build. What it cannot remove is the serialisation: the steps run one
after another even though they have no ordering relationship.

## What already changed

This plan is step 3 of three. The first two are already applied:

1. `Swatinem/rust-cache@v2` on `test`, `macos-default` and the TCK job, with
   `save-if` restricted to `main`.
2. `container.yml` PR builds read `turbolay-linux-amd64` and no longer write a
   per-pull-request cache scope.

Do **not** start this step until a few pushes have landed on `main` with the
cache warm. The shard boundaries below assume dependencies restore rather than
build, and measuring against a cold cache will point at the wrong split.

## Measured baseline

| Step | Time |
|---|---|
| Clippy | 133s |
| Test full native feature set | 151s |
| Test placement crate | 102s |
| Test production runtime configuration | 88s |
| Test public client protocols | 78s |
| Test official Neo4j driver compatibility | 69s |
| Test production runtime lifecycle | 64s |
| Clippy full native feature set | 62s |
| Test default feature set | 58s |
| Clippy production runtime | 55s |
| Check standalone Bolt server | 38s |
| Clippy OpenCypher feature set | 37s |
| Test OpenCypher feature set | 33s |
| Clippy public client protocols | 30s |
| Check indexing runtime | 20s |
| Test chaos harness feature set | 16s |
| Clippy placement crate | 15s |
| Check native feature examples | 9s |
| Clippy chaos harness | 8s |
| Check formatting, Check default examples, Check chaos harness example | 11s |
| **Total** | **~1077s** |

## The grouping rule

Shard on **feature union**, not on step kind. Cargo keys compiled artifacts by
the resolved feature set, so two steps sharing a feature set share their
compiled dependencies; two steps in different unions share nothing and pay the
dependency build twice.

This is why clippy and test for one feature set must stay in the same shard.
Since Rust 1.52 `cargo clippy` drives `clippy-driver` only through
`RUSTC_WORKSPACE_WRAPPER`, so registry dependencies are built by plain `rustc`
and are reused by the following `cargo test`. Splitting a `Clippy X` step away
from its `Test X` step converts that reuse into a second full dependency build.

The corollary is that splitting has a real cost — every shard pays for the
dependency graph its feature union needs. Four shards is the point where added
duplication starts to outweigh added parallelism at these timings.

## Proposed shards

Four matrix legs, each a `just`-style group of the existing step commands, run
under one job with `strategy.matrix`.

### `default` — ~343s

Check formatting; Clippy; Test default feature set; Check default examples;
Clippy chaos harness; Test chaos harness feature set; Check chaos harness
example; Clippy placement crate; Test placement crate.

The placement crate is here because `-p turbolay-placement` resolves a small,
mostly disjoint graph — it does not benefit from sitting next to any particular
feature union, and this is the lightest shard to absorb it.

### `opencypher` — ~292s

Clippy OpenCypher feature set; Test OpenCypher feature set; Check native
feature examples; Clippy full native feature set; Test full native feature set.

`opencypher` is a subset of the full-native union, so the two share most of
their dependency build.

### `protocols` — ~215s

Clippy public client protocols; Test public client protocols; Check standalone
Bolt server; Test official Neo4j driver compatibility.

`bolt_neo4j_driver_smoke.sh:10` runs `cargo run --features bolt-server`, which
is why the driver smoke test sits with the `bolt-server` check rather than with
the runtime shard.

### `runtime` — ~227s

Clippy production runtime; Test production runtime configuration; Check
indexing runtime; Test production runtime lifecycle.

`runtime_smoke.sh:7` builds `--features server-runtime --bin graph-node`, the
same artifact the preceding steps produce.

Longest shard ~343s, plus ~20s apt and ~30–60s cache restore. Expect the job to
land around **6–7 min**, from 18m18s.

## Open questions to settle while implementing

- **Cache budget.** Four shards want four rust-cache keys instead of one. Each
  entry is smaller than today's monolithic one, but the total is untested. The
  repository was at 6.6 GiB of 10 GiB before this work. Measure with
  `gh api repos/:owner/:repo/actions/cache/usage` after the first main build
  and shrink the split to three shards if the entries thrash.
  A single `shared-key` across all four legs is **not** the fix: the four jobs
  race to upload the same key, first write wins, and the surviving entry holds
  only one shard's artifacts.
- **`RUST_MIN_STACK`.** Currently set once at job level (`ci.yml:218`) with a
  comment explaining that the offending test is reachable from every feature
  set that compiles the Cypher engine. It must stay at job level so it applies
  to every leg — do not push it down into individual matrix entries.
- **Native dependencies.** Every shard needs the `apt-get install` step and the
  `pkg-config`/`ldconfig` verification. That is ~14s paid four times instead of
  once; acceptable, but a container image with them preinstalled would remove
  it if the number grows.
- **Required status checks.** Splitting `test` into four legs changes the check
  names. Branch protection on `main` needs updating in the same change, or
  merges will block on a `test` check that no longer reports.

## Not in scope

`macos-default` stays a single job. At 4m43s it is far off the critical path,
and sharding it would spend macOS runner minutes to shorten a job nothing waits
on.
