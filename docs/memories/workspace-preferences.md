---
title: Workspace execution preferences
status: active
date: 2026-07-18
scope: turbolay
tags:
  - workflow
  - tmux
  - git
  - formal-methods
---

# Workspace execution preferences

- Read and write repository files directly from the workspace; do not route
  ordinary filesystem work through tmux.
- Run ordinary Git inspection, staging, and focused commits directly from the
  workspace. Commit work in small, task-scoped commits and never include
  unrelated dirty or untracked files.
- Use tmux pane `pson:10.2` for Quint, Apalache, environment-variable setup,
  and long-running shell commands.
- Keep generated formal-methods artifacts and verification output reproducible;
  record the command and relevant environment when a run is long-lived or
  depends on external tooling.
