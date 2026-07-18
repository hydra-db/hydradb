# Handoff — "Verifying turbolay" (the Quint part)

## Goal
A standalone, independently-buildable book part that teaches Quint, formal
verification, and model-based testing from zero, in the book's voice
(Socratic/Feynman, problem→answer). Reader arc: build intuition → read all eight
turbolay `.qnt` models by the middle → understand MBT by the end. Scope is
**Quint → Apalache → Rust MBT**; Jepsen is named as a future layer but deferred.

## What is done
- **Act I written, on the real Bookly theme, contrast-fixed, committed** (`5aac8cf`):
  Ch.1 The correctness problem · Ch.2 Quint from zero · Ch.3 Reading the first
  real model (`m1_cell_write`).
- Entry `book/quint.typ` mirrors `main.typ` (reader theme, its own PDF); chapters
  are `book/chapters/quint-0{0,1,2}-*.typ` (the `quint-*` prefix keeps them out
  of `gen-index.sh`). Compiles clean in dark + light.
- Two authoring skills capture the workflow: `book/skills/formatting-chapters`
  (Phase 1: Bookly scaffolding, standardized Term/Why boxes, **theme-aware
  diagram colors**, visualize-don't-tell) and `book/skills/writing-content`
  (Phase 2: the Socratic method, ground every claim in a real file).

## What to read to gain context
- This file, then `book/skills/formatting-chapters` + `book/skills/writing-content`.
- The three existing chapters as the voice/format reference; `book/quint.typ`
  for the entry pattern; `book/chapters/intro-00-replaceable-compute.typ` for
  native Bookly usage.
- Content source of truth: `docs/formal-methods/0001-...md` (objective, contract,
  properties) and `0003-...md` (evidence, boundaries); the models in
  `quint-models/turbolay/*.qnt`; the real code in `src/shard/write.rs` and
  `src/shard/lifecycle.rs`.
- Build: `cd book && typst compile --input mode=dark quint.typ quint.pdf`
  (also test `--input mode=light`; PDFs are gitignored).

## Requirements (apply to every chapter, Act II and III included)
- **Every chapter must carry at least one clear diagram that illustrates what
  that chapter is testing** — the state machine, invariant, counterexample, or
  test flow the prose is about, drawn as a picture, not just described. A chapter
  with no such figure is not done. The gold-standard reference is Ch.3 §3.6
  `<fig-state-space>` (`quint-02-first-model-m1.typ`): the whole model as one
  picture — a bold causal spine (one behavior a test walks), faded forks (the
  other interleavings the checker walks), a shaded region where `allSafety`
  holds, and the forbidden state _outside_ it reached only by a crossed-out "no
  such transition" arrow (the missing edge that _is_ the safety property).
  Reuse that visual grammar so diagrams compound across chapters: nodes = states,
  bold = the sampled path, faded = the unexplored branches, shaded region = the
  invariant, a red frontier-crossing edge = a bug (Act II's buggy twin), a proven
  wall = Apalache, a re-walked bold thread = Rust MBT.

## Next steps
- **Ch.3 diagrams — done** (`71ae358`, `f13f84d`): the §3.6 state-space concept
  figure opens the section; the `step` fan-out and the storyline are reframed in
  prose as zoom-ins of it. `make quint` builds the standalone PDF (dark default).
- **Act II:** invariants, witnesses & the buggy-twin method (`*_buggy.qnt`);
  deterministic scenarios (`run`/`then`/`expect`); the model gallery reading all
  eight `.qnt` families. Scaffold with `formatting-chapters`, then fill with
  `writing-content`.
- **Act III:** Apalache (`quint verify`) and Rust MBT (`tests/formal_mbt*.rs`,
  `just minio-mbt`).

## Anything else
- Always compile **both** modes and eyeball diagram contrast before committing;
  never hardcode diagram colors (edges/arrowheads use `reader-colors.muted`, not
  black/`border`; node fills use `reader-colors.*_soft` with explicit text color).
- Define each term once, in a standalone Term box — never inline mid-sentence,
  never twice.
- Verify quoted `file:line` references against the actual file (line numbers drift).
