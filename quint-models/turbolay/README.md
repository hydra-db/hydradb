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

| Family | Main module | Deterministic tests | Primary findings |
|---|---|---|---|
| M1 | `m1_cell_write.qnt` | `m1_cell_write_test.qnt` | BFG-003, BFG-004, BFG-005, BFG-006 |
| M2 | `m2_snapshot_read.qnt` | `m2_snapshot_read_test.qnt` | BFG-001, BFG-002, BFG-007, BFG-008 |
| M3 | `m3_artifact_gc.qnt` | `m3_artifact_gc_test.qnt` | BFG-005, BFG-006 |
| M4 | `m4_placement_fence.qnt` | `m4_placement_fence_test.qnt` | BFG-007 |
| M5 | `m5_public_commands.qnt` | `m5_public_commands_test.qnt` | BFG-003, BFG-004, BFG-008 |

## Local commands

Run Quint and other long-running formal-methods commands in tmux pane
`pson:10.2`:

```bash
for model in m1_cell_write m2_snapshot_read m3_artifact_gc \
             m4_placement_fence m5_public_commands; do
  mise exec -- quint typecheck "quint-models/turbolay/${model}.qnt"
done

for test in m1_cell_write m2_snapshot_read m3_artifact_gc \
            m4_placement_fence m5_public_commands; do
  mise exec -- quint test "quint-models/turbolay/${test}_test.qnt" \
    --main "${test}_test" --match '.*Test$'
done

# Bounded Apalache check; Java must be selected through Mise on this host.
mise exec java@21.0.2 -- mise exec -- quint verify \
  quint-models/turbolay/m2_snapshot_read.qnt --main m2_snapshot_read \
  --invariant cursorPinnedToSnapshot,returnedPageMatchesCursor,\
invalidHistoricalNeverReturns,bookmarkMonotone --max-steps 6

# The M1 Rust driver replays these action-labelled simulation traces through
# the public GraphShard API and compares its state projection after every step.
mise exec -- cargo test --locked --test formal_mbt -- --test-threads=1

# Produces action names (`mbt::actionTaken`) for inspecting the generated
# simulation trace. `quint test` witnesses are deterministic model checks, but
# their ITF output lacks the action labels required by quint-connect 0.1.2.
mise exec -- quint run quint-models/turbolay/m1_cell_write.qnt \
  --main m1_cell_write --mbt --max-steps 8 \
  --out-itf target/formal/m1-cell-write-mbt.itf.json
```
