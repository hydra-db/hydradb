---
title: Formal bug validation and regression protocol
status: proposed
date: 2026-07-18
scope: Turbolay V1 to V2 findings
tags:
  - bugs
  - quint
  - apalache
  - model-based-testing
  - worktree
---

# Formal bug validation and regression protocol

## Purpose

For every row in the [bug inventory](README.md), turn the intended behavior
into an executable Quint invariant, demonstrate the violating behavior against
the historically affected implementation when possible, and retain a durable
record of the result. A passing current test alone is not enough to call a bug
fixed: the record must show that the model would have caught the old behavior.

## Required workflow

1. **Classify the finding.** Confirm whether it is a demonstrated bug, a
   semantic decision, or a suspected race. Do not promote a review question to
   a “fixed bug” without a concrete reproduction or an approved contract.
2. **Specify the intended behavior.** Add the smallest Quint state/action model
   and a safety invariant that is violated by the bad outcome and holds for the
   intended outcome. Use the corresponding M1–M5 module from Formal Methods
   0002; retain a named witness for the critical action/interleaving.
3. **Identify historical commits.** Use `git log`, `git blame`, and, when
   needed, `git bisect` to record `first_bad_commit` and `fix_commit`. If V1
   predates the feature, use the first relevant V2 commit instead of treating
   V1 as a false historical reproduction target.
4. **Reproduce in an isolated worktree.** Create a detached worktree at the
   first bad commit. Run the same Quint model and a focused Rust regression
   harness adapted only for legitimate API drift. The result must exhibit the
   invariant violation, mismatch, or explicitly unsupported contract.
5. **Validate current code.** Run the model against the current source,
   generate ITF traces with `quint run --mbt --out-itf`, and replay them with
   the Rust MBT adapter. Use MinIO as the second execution environment for any
   object-store-sensitive behavior.
6. **Create one record per finding.** Write
   `docs/bugs-found-fixed/BFG-<nnn>-<slug>.md` using the template below. Link
   the model, test/trace, historical worktree result, fix commit, and current
   verification output.
7. **Request review before status changes.** `fixed` requires review of the
   behavior, historical evidence, and current verification evidence. A model
   discovering a new counterexample creates a new `BFG-*` record with status
   `discovered`, not an untracked note.

## Worktree discipline

Use a separate, detached worktree for every historical reproduction. Keep the
main Turbolay worktree on the active implementation branch and never reset,
checkout, or clean its existing user changes.

```bash
git worktree add --detach ../turbolay-bfg-001 <first-bad-commit>
# Run only the scoped historical model/repro there.
git worktree remove ../turbolay-bfg-001
```

Quint, Apalache, environment setup, and long-running model commands run in
tmux pane `pson:10.2`. Ordinary file operations and focused Git commits may run
directly in the workspace.

## Per-bug record template

```markdown
---
id: BFG-001
title: Short human-readable finding
status: discovered # discovered | reproducing | fixed-pending-review | fixed | not-a-bug | blocked
severity: P1
introduced_or_first_bad_commit: <sha | pending-bisect>
fix_commit: <sha | none | pending>
affected_range: <sha>..<sha>
model: quint-models/turbolay/m2_snapshot_read.qnt
historical_worktree: ../turbolay-bfg-001
current_verified_commit: <sha>
date_opened: YYYY-MM-DD
date_verified: null
tags: [bugs, quint, regression]
---

# BFG-001: Title

## Status

One sentence stating the current evidence, not the desired conclusion.

## Intended behavior

Precise externally visible contract.

## Bad behavior and reproduction

Steps, finite model trace, historical commit/worktree, and observed invariant
violation. Include the smallest reproducible command and preserved ITF trace.

## Impact

Who observes the behavior and what correctness property fails.

## Fix and current validation

Fix commit, code correspondence, Quint simulation/Apalache result, MBT trace
result, and any Jepsen history that exercises the same fault class.

## Review decision

Reviewer, date, and accepted status transition.
```

## Status meanings

| Status | Meaning |
|---|---|
| `discovered` | Evidence identifies a possible defect; no historical replay is complete. |
| `reproducing` | A model and historical worktree reproduction are in progress. |
| `fixed-pending-review` | Old behavior was reproduced and current verification passes, awaiting review. |
| `fixed` | Reviewed historical and current evidence supports the fix. |
| `not-a-bug` | The behavior is an approved API contract; the rationale is recorded. |
| `blocked` | Progress requires an external decision or unavailable environment; the blocker is explicit. |

## Review gate for this inventory

Before implementation, review and approve:

1. The classification and priority of BFG-001 through BFG-008.
2. The intended public pagination contract for BFG-001, BFG-002, and BFG-008.
3. The relationship-ID scope decision for BFG-003.
4. Whether BFG-004 is a defect or the intended batch API behavior.
5. The use of first-relevant V2 commits rather than V1 for features absent in
   V1.
