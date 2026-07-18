---
id: BFG-002
title: Direct paged query could ignore an unvalidated historical epoch
status: fixed-pending-review
severity: P1
classification: confirmed-bug
introduced_or_first_bad_commit: e875387bf121292c316f6c81d5a3d3e5fdce7d04
fix_commit: b1709ea
affected_range: e875387bf121292c316f6c81d5a3d3e5fdce7d04..b1709ea
model:
  intended: quint-models/turbolay/m2_snapshot_read.qnt
  fault: quint-models/turbolay/m2_snapshot_read_buggy.qnt
historical_worktree: /Users/abhishek/hydradb/graphdb-on-s3/turbolay-bfg-001
current_verified_commit: b1709ea
date_opened: 2026-07-18
date_verified: 2026-07-18
tags: [bugs, read-epoch, pagination, quint, regression]
---

# BFG-002: reject an unvalidated historical epoch before page dispatch

## Status

The historical behavior is reproduced at `e875387`; the focused regression
passes at `b1709ea`. Review is required before the status changes to `fixed`.

## Intended behavior

`QueryContext::at_epoch` is a graph/topology watermark, not a caller-owned
SlateDB storage snapshot. A paged OpenCypher request that supplies an epoch
without a matching internal storage-snapshot validation must fail with
`GraphError::UnsupportedQuery` before graph-kernel, streaming, parser, or
fallback dispatch. A current request obtains its validated epoch internally
from the snapshot it uses.

## Bad behavior and reproduction

The page entry at `e875387` tried fast paths before the complete-read path's
validation. `query_read_epoch` then selected the current epoch, so a request
for epoch zero returned the edge committed at epoch one.

In detached worktree
`/Users/abhishek/hydradb/graphdb-on-s3/turbolay-bfg-001` at
`e875387bf121292c316f6c81d5a3d3e5fdce7d04`, a focused temporary regression
test seeded `1-[:FOLLOWS]->2` and requested a one-row page with
`QueryContext::at_epoch(0)`. Executed in `pson:10.2` with the local
`libcypher-parser` include settings, it failed as required:

```text
unvalidated historical paged queries must reject, got Ok(QueryResultPage {
  rows: [QueryRow { values: [VertexId(2)] }], ...
})
```

The fault model independently enables
`returnUnvalidatedHistoricalFromCurrent`; Quint reports violations of both
`invalidHistoricalNeverReturns` and `pageMatchesSnapshot`:

```bash
mise exec -- quint run quint-models/turbolay/m2_snapshot_read_buggy.qnt \
  --main m2_snapshot_read_buggy \
  --invariants pageMatchesSnapshot invalidHistoricalNeverReturns \
  --witnesses livePageReached invalidHistoricalPageReached --max-steps 8 \
  --out-itf target/formal/m2-buggy.itf.json
```

## Impact

The same historical epoch request could return current data for a
fast-path-shaped query but return an error for a fallback-shaped query. That
makes snapshot semantics depend on optimizer/query shape and can silently
violate a caller's read contract.

## Fix and current validation

`b1709ea` adds the pre-dispatch check to
`GraphShard::execute_opencypher_rows_page`. The permanent regression,
`paged_query_rejects_unvalidated_historical_epoch_before_fast_path_dispatch`,
now passes:

```bash
cargo test --locked --features opencypher --lib \
  paged_query_rejects_unvalidated_historical_epoch_before_fast_path_dispatch \
  -- --nocapture
```

The intended model disables the forbidden transition. Its deterministic
scenario `unvalidatedHistoricalPageIsRejectedTest` passes, and bounded Quint
simulation reports no violation of `invalidHistoricalNeverReturns` while
reaching the rejection witness. These are model checks over the stated finite
model, not a claim of an unbounded proof; Apalache and Rust MBT replay remain
required gates in the approved plan.

## Review decision

Pending review of the behavior and evidence. Promote to `fixed` only after
the M2 Apalache/MBT gate is recorded and the public historical-epoch error
contract is accepted.
