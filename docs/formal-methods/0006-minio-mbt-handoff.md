---
title: "Formal Methods 0006: MinIO MBT Handoff"
status: active
date: 2026-07-18
branch: Turbolay-V3
depends_on:
  - 0003-turbolay-quint-verification-evidence.md
  - 0005-next-steps-and-completion-gates.md
tags: [quint, mbt, minio, s3, ci, jepsen]
---

# MinIO MBT handoff

## Goal

Bind the existing seeded Quint Connect corpus to an S3-compatible backend
without changing its model actions, seeds, or public normalized oracle.
`InMemory` must remain the fast deterministic default; local MinIO must be an
explicit replay mode with enough retained failure evidence to reproduce a
backend-specific result.

## Completed

- All six default adapter binaries (M1, M2, M3, M4, M5, and P2) use the shared
test-only backend factory in `tests/support/mbt_backend.rs`.
- The default backend remains isolated `InMemory` storage.
- `GRAPH_MBT_BACKEND=minio` plus `GRAPH_MBT_S3_ENV_FILE` selects the
S3-compatible backend after validating AWS provider, endpoint, bucket, and
credentials. Each replay receives a unique safe graph path below
`GRAPH_MBT_PREFIX`.
- `just minio-mbt` starts the pinned local MinIO and `mc` images, creates a
disposable bucket, replays the six adapters serially with their unchanged
seeds, and removes resources on success.
- On failure, the runner retains its generated configuration, per-adapter
Cargo output, MinIO log, object list, mounted data directory, and
bucket/prefix under `target/minio-mbt/`.
- Local evidence: all six adapters pass against both default `InMemory` and
local MinIO. `just minio-fence` additionally proves replacement-writer
commit, incumbent fencing, and fresh-reader verification against the same
pinned MinIO image.

## Evidence boundary

A passing local MinIO replay proves only this finite corpus against that
pinned S3-compatible image and configuration. It does not prove arbitrary S3
provider behavior, S3 outage handling, performance, CI execution, or Jepsen
process-level fault tolerance. It must not be reported as Jepsen evidence.

## Reproduction

```bash
# Default fast corpus.
for test in formal_mbt formal_mbt_m2 formal_mbt_m3 formal_mbt_m4 formal_mbt_m5 formal_mbt_p2; do
  mise exec -- cargo test --locked --test "$test" -- --test-threads=1
done

# Pinned local MinIO corpus and CAS/fence proof.
just minio-mbt
just minio-fence
```

## Pending, in order

1. Record product approval (or intentional changes) for BFG-003, BFG-004,
   BFG-007, and BFG-008; revise model, witness, adapter, and bug record
   together for any changed decision.
2. Add the bounded `just minio-mbt` corpus to CI after the existing
   InMemory MBT gate. Upload retained MinIO failure artifacts and report the
   backend, seed, trace, commit, bucket/prefix, and logs.
3. Replay the same configuration against the intended non-local
   S3-compatible deployment and retain its endpoint/config evidence. A local
   MinIO pass is not a production-S3 pass.
4. Build Jepsen no-fault baselines using the Quint operation vocabulary,
   beginning with write atomicity and ownership/fencing; introduce one
   nemesis class at a time only after each baseline passes.
5. For each minimized formal, MinIO, or Jepsen failure, add a regression
   record under `docs/bugs-found-fixed/` with its seed/history and fixing
   commit.

## Required reading and reference map

Read these before extending or interpreting this work:

| Purpose | Material |
|---|---|
| Formal objective, model boundary, and action vocabulary | `docs/formal-methods/0001-quint-jepsen-testing-objective.md`; `0002-turbolay-quint-specification-plan.md` |
| Executed evidence and explicit evidence limits | `docs/formal-methods/0003-turbolay-quint-verification-evidence.md`; this handoff |
| Contract defaults and completion ordering | `docs/formal-methods/0004-api-coverage-completion-priority.md`; `0005-next-steps-and-completion-gates.md` |
| Quint models, witnesses, and adapter commands | `quint-models/turbolay/README.md`; all `quint-models/turbolay/m*_*.qnt` files |
| Shared backend selection and adapter bindings | `tests/support/mbt_backend.rs`; `tests/formal_mbt*.rs` |
| Local S3 lifecycle and CAS/fence proof | `scripts/minio_mbt.sh`; `scripts/minio_smoke.sh`; `scripts/minio_fence_takeover.sh`; `justfile` |
| Conditional lock behavior and ownership implementation | `src/core/state.rs`; `src/engine/cluster.rs`; `src/shard/lifecycle.rs` |
| CI insertion point and bug-regression format | `.github/workflows/ci.yml`; `docs/bugs-found-fixed/` |

Keep the model vocabulary, adapter actions, MinIO runner, and future Jepsen
generators aligned. A behavior change is not complete until its relevant
Quint action/witness, Rust adapter, MinIO replay, documentation, and bug
record (when applicable) agree.
