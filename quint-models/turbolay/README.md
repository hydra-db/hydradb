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

M2 is first because it covers BFG-001, BFG-002, and BFG-008: page results must
be pinned to one snapshot, unvalidated historical page requests must be
rejected, and cursor pages must not change after concurrent commits.

## Local commands

Run Quint and other long-running formal-methods commands in tmux pane
`pson:10.2`:

```bash
mise exec -- quint typecheck quint-models/turbolay/m2_snapshot_read.qnt
mise exec -- quint test quint-models/turbolay/m2_snapshot_read_test.qnt \
  --main m2_snapshot_read_test --match '.*Test$'
mise exec -- quint run quint-models/turbolay/m2_snapshot_read.qnt \
  --main m2_snapshot_read --invariants cursorPinnedToSnapshot \
  returnedPageMatchesCursor invalidHistoricalNeverReturns bookmarkMonotone \
  --witnesses pageAfterConcurrentCommitReached unvalidatedHistoricalRejected \
  --max-steps 12
```
