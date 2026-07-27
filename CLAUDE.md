# CLAUDE.md

Guidance for Claude Code when working in this repository.

## Build and test through the justfile

Run builds and tests as `just <recipe>` (`just --list`), not bare `cargo`. The
justfile exports the three environment variables the FFI features need —
`BINDGEN_EXTRA_CLANG_ARGS` and `LIBRARY_PATH` for Homebrew on macOS, and
`RUST_MIN_STACK` on every platform — without which `opencypher`,
`server-runtime` and `graphblas` fail to link or abort on a stack overflow. CI
is Linux and sets them elsewhere, so a bare `cargo` line that works there fails
here.

## Never create artifacts

**Never use the Artifact tool in this repository — not for anything, ever.**

Visual and long-form deliverables are local HTML files written into
`interactive/` with Write/Edit, and nothing else. The user opens them from disk.

- Complete standalone documents: `<!doctype html>`, `<head>` with
  `<meta charset="utf-8">` and a viewport tag. `file://` must work by
  double-clicking.
- Self-contained: no CDN scripts, no external stylesheets, no remote fonts or
  images.
- Light and dark, following `interactive/assets/textbook.css`.
- `interactive/README.md` documents the existing house style.

## Plan documents

Every file in `docs/plans/` is named `YYYY-MM-DD-kebab-case-title.md`, dated the
day the plan was written. The date is part of the name so the directory sorts
chronologically and a stale plan is obvious on sight.

Every markdown file in `docs/plans/` opens with a YAML frontmatter block. Keep it
short — enough metadata to know what a plan is, when it was written, and what
tree it applies to:

```yaml
---
title: Sparse kernel backend consolidation
status: draft-for-review        # draft-for-review | step-N-complete | done | superseded
date: 2026-07-25
branch: Turbolay-V3.5
base_commit: 989cc72            # tree the plan was written against
head_commit: 73309df            # add once the work lands; omit while unstarted
tags:
  - sparse-kernel
  - refactor
---
```

A plan that rests on prior analysis opens with a **Sources** section naming the
files that hold it — design notes under `interactive/`, memory entries, and the
exact paths in any reference repo (`../sleet`, `../tidb-master`). Name the file
and what it holds, not just the repo, so the next session reads instead of
re-deriving.

`docs/plans/2026-07-25-sparse-kernel-backend-consolidation.md` is the reference
example. `optimisation-phases.md` predates the convention and does not conform;
leave it unless asked, since `build.rs:10` references it by name.
