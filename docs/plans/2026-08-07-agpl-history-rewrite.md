---
title: AGPLv3 as first commit (history rewrite)
status: draft-for-review
date: 2026-08-07
branch: rewrite/agpl-first-commit
base_commit: 17cb1bd
tags:
  - license
  - agpl
  - history-rewrite
  - PRO-1481
---

# AGPLv3 as first commit (PRO-1481)

## Goal

Make the standard **GNU Affero General Public License v3.0** the **first commit** of this repository, then keep the existing `main` history (including merge commits) on top of that root.

## Deliverable already on the remote

| Ref | Purpose |
|---|---|
| `origin/rewrite/agpl-first-commit` | **Cutover tip** — full rewritten history (review with `git log`) |
| this branch / PR | Runbook + `LICENSE` preview for review (normal mergeable PR) |

Inspect the rewritten history:

```bash
git fetch origin rewrite/agpl-first-commit
git log --reverse --oneline origin/rewrite/agpl-first-commit | head -10
git rev-list --count origin/rewrite/agpl-first-commit          # expect 559
git rev-list --count --merges origin/rewrite/agpl-first-commit  # expect 74
git show origin/rewrite/agpl-first-commit:LICENSE | head
```

Expected first commits:

1. `Add GNU Affero General Public License v3.0` (LICENSE only)
2. `M0: storage foundation...` (parent = license root)
3. …rest of history with merges preserved

## Why this PR cannot be the cutover

GitHub cannot open a mergeable PR between histories that share no commits. The rewrite rewrites every SHA, so:

- **Do not** use Merge / Squash / Rebase on a rewrite-to-main PR as the cutover (GitHub rejects unrelated histories anyway).
- **Do** flip `main` to the rewrite tip after review (force-push or default-branch swap).

## Cutover procedure (after approval)

```bash
git fetch origin

# 1. Backup current main
git push origin origin/main:refs/heads/backup/main-before-agpl-pro-1481

# 2. Point main at the rewritten history
git push --force-with-lease origin rewrite/agpl-first-commit:main
```

Optional GitHub UI alternative: change the default branch to `rewrite/agpl-first-commit`, then rename/delete old `main` and rename the rewrite branch to `main`.

## After cutover

1. Tell the team to **re-clone** or hard-reset local branches; old SHAs are obsolete.
2. Long-lived branches (`Turbolay-V3`, feature branches, etc.) still point at **pre-rewrite** history until they are rebased/recreated onto the new root. Out of scope for the first cutover unless needed.
3. Local `git reflog` on machines that previously had old history may still show old SHAs until expired/pruned — this does **not** affect the remote.

## Method used to build `rewrite/agpl-first-commit`

1. Orphan root commit with standard AGPLv3 text as `LICENSE`.
2. `git replace --graft` both historical roots of `main` onto that license commit (preserves dual-lineage / merge topology).
3. `git filter-repo` to materialize grafts permanently (all SHAs change).
4. Inject `LICENSE` into every commit tree so the file remains present at the tip.
5. Backdate the license root slightly before the oldest existing commit so `git log --reverse` lists it first.

## Out of scope for now

- Scrubbing local reflogs
- Rewriting all product/feature branches
- Asking GitHub Support to purge unreachable objects
