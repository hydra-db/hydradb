---
title: Turbolay Quint models
status: active
date: 2026-07-18
scope: Turbolay graph kernel
tags:
  - quint
  - formal-methods
  - model-based-testing
---

# Turbolay Quint models

This directory implements the model families approved in
[`docs/formal-methods/0002-turbolay-quint-specification-plan.md`](../../docs/formal-methods/0002-turbolay-quint-specification-plan.md).

Each `mN_*.qnt` module is the intended contract. Its paired `*_test.qnt` file
contains only deterministic scenario tests. Where a historical bug has a small
implementation-shaped state machine, `*_buggy.qnt` deliberately preserves the
bad transition so `quint run` can produce a counterexample. The buggy module is
evidence that the invariant is discriminating; it is not the intended design.

M2 is first because it covers BFG-001 and BFG-002 directly: a page operator
must be pinned to one snapshot and an unvalidated historical request must be
rejected. Its cursor state is the intended contract for the materialized client
cursor. Direct `QueryResultPage` pagination remains BFG-008: its token carries
only an offset, so its across-request contract is intentionally still open.
M2b makes the separate `snapshot_at` admission and cancellation boundary
executable. M5b captures destructive API behavior without overloading the
relationship-identity command model.

The Rust Quint Connect adapters are finite public-API refinement checks. They
run seeded traces against a local `InMemory` object store, not MinIO/S3, and do
not constitute Jepsen evidence. M3's adapter observes checked-current matrix
artifact publication/rejection, retained owned snapshots, GC through a
published artifact, and direct/matrix reachability equivalence. M4's adapter
observes local placement disagreement, fenced routed-cluster takeover,
committed-prefix monotonicity, and stale-writer rejection through the previously
opened node-1 writer handle.

| Family | Main module | Deterministic tests | Primary findings |
|---|---|---|---|
| M1 | `m1_cell_write.qnt` | `m1_cell_write_test.qnt` | BFG-003, BFG-004, BFG-005, BFG-006 |
| M1b | `m1_bulk_import.qnt` | `m1_bulk_import_test.qnt` | P1 bulk unit/retry contract |
| M2 | `m2_snapshot_read.qnt` | `m2_snapshot_read_test.qnt` | BFG-001, BFG-002, BFG-007, BFG-008 |
| M2b | `m2_snapshot_lifecycle.qnt` | `m2_snapshot_lifecycle_test.qnt` | P2 current-only `snapshot_at`, typed rejection, cancellation |
| M3 | `m3_artifact_gc.qnt` | `m3_artifact_gc_test.qnt` | BFG-005, BFG-006 |
| M4 | `m4_placement_fence.qnt` | `m4_placement_fence_test.qnt` | BFG-007 |
| M5 | `m5_public_commands.qnt` | `m5_public_commands_test.qnt` | BFG-003, BFG-004, BFG-008 |
| M5b | `m5_destructive_lifecycle.qnt` | `m5_destructive_lifecycle_test.qnt` | P2 `DELETE`, `DETACH DELETE`, cell-drop fence |

## Local commands

Run Quint and other long-running formal-methods commands in tmux pane
`pson:10.2`:

```bash
for model in m1_cell_write m1_bulk_import m2_snapshot_read \
             m2_snapshot_lifecycle m3_artifact_gc m4_placement_fence \
             m5_public_commands m5_destructive_lifecycle; do
  mise exec -- quint typecheck "quint-models/turbolay/${model}.qnt"
done

for test in m1_cell_write m1_bulk_import m2_snapshot_read \
            m2_snapshot_lifecycle m3_artifact_gc m4_placement_fence \
            m5_public_commands m5_destructive_lifecycle; do
  mise exec -- quint test "quint-models/turbolay/${test}_test.qnt" \
    --main "${test}_test" --match '.*Test$'
done

# Bounded Apalache check; Java must be selected through Mise on this host.
mise exec java@21.0.2 -- mise exec -- quint verify \
  quint-models/turbolay/m2_snapshot_read.qnt --main m2_snapshot_read \
  --invariant allSafety --max-steps 6

# The Rust drivers replay action-labelled simulation traces through public APIs
# and compare a normalized state projection after every step. M3 and M4 are the
# newly wired adapters for artifact/GC and placement/fencing respectively.
mise exec -- cargo test --locked --test formal_mbt -- --test-threads=1
mise exec -- cargo test --locked --test formal_mbt_m2 -- --test-threads=1
mise exec -- cargo test --locked --test formal_mbt_m3 -- --test-threads=1
mise exec -- cargo test --locked --test formal_mbt_m4 -- --test-threads=1
mise exec -- cargo test --locked --test formal_mbt_m5 -- --test-threads=1

# P2 has two additional public-API trace replayers. M5b is feature-independent;
# M2b also exercises the OpenCypher cancelled-page entry point.
mise exec -- cargo test --locked --test formal_mbt_p2 -- --test-threads=1
TURBOLAY_PKG_PREFIX="$(brew --prefix libcypher-parser)" \
PKG_CONFIG_PATH="$(brew --prefix libcypher-parser)/lib/pkgconfig${PKG_CONFIG_PATH:+:$PKG_CONFIG_PATH}" \
BINDGEN_EXTRA_CLANG_ARGS="-I$(brew --prefix libcypher-parser)/include" \
mise exec -- cargo test --locked --features opencypher --test formal_mbt_p2 -- --test-threads=1

# Produces action names (`mbt::actionTaken`) for inspecting the generated
# simulation trace. `quint test` witnesses are deterministic model checks, but
# their ITF output lacks the action labels required by quint-connect 0.1.2.
mise exec -- quint run quint-models/turbolay/m1_cell_write.qnt \
  --main m1_cell_write --mbt --max-steps 8 \
  --out-itf target/formal/m1-cell-write-mbt.itf.json
```
