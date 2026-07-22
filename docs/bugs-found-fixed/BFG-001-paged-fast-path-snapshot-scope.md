---
id: BFG-001
title: Paged fast paths could read live storage outside a SlateDB snapshot
status: reproducing
severity: P1
classification: confirmed-bug
introduced_or_first_bad_commit: e875387bf121292c316f6c81d5a3d3e5fdce7d04
fix_commit: none
candidate_fix_commit: b1709ea
affected_range: e875387bf121292c316f6c81d5a3d3e5fdce7d04..b1709ea
model:
  intended: quint-models/turbolay/m2_snapshot_read.qnt
  fault: quint-models/turbolay/m2_snapshot_read_buggy.qnt
historical_worktree: /Users/abhishek/hydradb/graphdb-on-s3/turbolay-bfg-001
current_verified_commit: b1709ea
date_opened: 2026-07-18
date_verified: null
tags: [bugs, snapshot, pagination, quint, regression]
---

# BFG-001: page fast paths must execute under one storage snapshot

## Status

`b1709ea` acquires and task-scopes one `DbSnapshot` before graph-kernel or
streaming page dispatch. This is the intended code change, but the record
remains `reproducing` until a historical worktree test forces a writer commit
*during* a page operator and the current test proves that the returned page is
internally from one view.

## Intended behavior

A single `QueryResultPage` is evaluated against exactly one SlateDB storage
snapshot. A concurrent committed write may be visible to a later page request
under the direct-page API's current best-effort contract (BFG-008), but it must
not make one already-running page combine old and new records, topology, or
derived-artifact state.

## Bad behavior and reproduction

At historical commit `e875387`, `execute_opencypher_rows_page` dispatched the
graph-kernel and streaming operators before the complete-read path obtained
and task-scoped a `DbSnapshot`. Their lower-level reads therefore used the
live `GraphStore` path. The source evidence is recorded in
`turbolay-v2-commit-analysis/review-findings.md`, Finding 1.

The fault model has the corresponding forbidden transition: a page reads the
live current view after the snapshot view has been captured. Run it in the
approved tmux pane:

```bash
mise exec -- quint run quint-models/turbolay/m2_snapshot_read_buggy.qnt \
  --main m2_snapshot_read_buggy \
  --invariants pageMatchesSnapshot \
  --witnesses livePageReached --max-steps 8 \
  --out-itf target/formal/m2-bfg-001-buggy.itf.json
```

The run reports an invariant violation. It is a model counterexample, not yet
a historical source-level forced-interleaving reproduction; that distinction
is why this item is not marked fixed.

## Impact

An application can receive a page that never existed as one durable graph
view. That can produce inconsistent rows, adjacency, degrees, or traversal
results during a concurrent write.

## Candidate fix and current validation

`b1709ea` performs the following before page parsing and fast-path dispatch:

1. acquire `self.db.snapshot()`;
2. read `last_epoch` through that exact snapshot;
3. mark the context as a validated storage read; and
4. run the recursive page execution inside `GraphStore::scope_snapshot`.

The intended M2 model has passed its deterministic tests, bounded Quint
simulation, and the six-step Apalache check with `cursorPinnedToSnapshot` and
`returnedPageMatchesCursor`. The remaining validation is a controlled
write/read interleaving for both the graph-kernel and streaming page shapes at
`e875387` and `b1709ea`, followed by Rust MBT replay.

## Review decision

Pending: review the forced-interleaving harness and decide whether direct page
requests remain explicitly best-effort across requests (BFG-008) or gain a
stable snapshot-bearing cursor.
