# Handoff — "Verifying turbolay" (the Quint part)

## Goal
A standalone, independently-buildable book part that teaches Quint, formal
verification, and model-based testing from zero, in the book's voice
(Socratic/Feynman, problem→answer). Reader arc: build intuition → read all eight
turbolay `.qnt` models by the middle → understand MBT by the end. Scope is
**Quint → Apalache → Rust MBT**; Jepsen is named as a future layer but deferred.

## What is done
- **All three Acts written, committed, compiling clean in dark + light — the book
  is complete (9 chapters, ~81pp).** Every chapter carries at least one diagram in
  the shared visual grammar; all figures were rendered and eyeballed in both modes
  in the full-book context before committing.
  - **Act I** (`5aac8cf`, `71ae358`, `f13f84d`): Ch.1 The correctness problem ·
    Ch.2 Quint from zero · Ch.3 Reading the first real model (`m1_cell_write`),
    incl. the §3.6 `<fig-state-space>` concept figure.
  - **Act II** (`170fa3f`, `e5c4c07`, `c562394`): Ch.4 Invariants, witnesses & the
    buggy twin (`fig-ch4-counterexample` — the red bug edge crossing out of the
    safe region) · Ch.5 Deterministic scenarios `run`/`then`/`expect`
    (`fig-ch5-scenario-path`) · Ch.6 The model gallery / coverage map of all eight
    families (`fig-ch6-coverage-map`).
  - **Act III** (`f78fc4d`, `3cba77b`, `62d44bf`): Ch.7 Bounded proof —
    `quint verify` / Apalache (`fig-ch7-three-regimes`: sampling vs bounded-
    exhaustive vs unbounded) · Ch.8 Rust MBT — ITF replay against the real
    `GraphShard` (`fig-ch8-refinement-loop`) · Ch.9 Epilogue — the assurance stack,
    Jepsen deferred (`fig-ch9-stack`).
- Entry `book/quint.typ` includes `quint-0{0..8}-*.typ` in order (the `quint-*`
  prefix keeps them out of `gen-index.sh`). `make quint` builds the standalone PDF
  (dark by default; `make MODE=light quint` for light). PDFs are gitignored.
- Two authoring skills capture the workflow: `book/skills/formatting-chapters`
  (Phase 1: Bookly scaffolding, standardized Term/Why boxes, **theme-aware
  diagram colors**, visualize-don't-tell) and `book/skills/writing-content`
  (Phase 2: the Socratic method, ground every claim in a real file). The winning
  authoring pattern was one subagent per chapter, each self-verifying compile +
  contrast via a throwaway `_preview-chN.typ` (quint.typ preamble + one include),
  with the orchestrator owning `quint.typ` wiring, label-collision checks, and the
  per-chapter commits.

## What to read to gain context
- This file, then `book/skills/formatting-chapters` + `book/skills/writing-content`.
- The nine chapters (`quint-0{0..8}-*.typ`) as the voice/format reference —
  especially `quint-02` for the `<fig-state-space>` visual grammar every later
  figure reuses; `book/quint.typ` for the entry pattern;
  `book/chapters/intro-00-replaceable-compute.typ` for native Bookly usage.
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
The three-Act arc is complete. What remains is optional polish and future layers:
- **Jepsen chapter (future):** the epilogue names Jepsen as the deferred real-
  cluster fault-injection layer. When Jepsen is actually run and evidence exists,
  it warrants its own chapter (real-process histories, fault schedules) — grounded
  in `docs/formal-methods/0001` §Jepsen and whatever run artifacts land.
- **Polish pass (optional):** a full read-through for cross-chapter voice
  consistency and forward/back references; re-verify quoted `file:line` numbers if
  the models/tests drift; consider a short front-matter preface framing the arc.
- **When editing any chapter,** keep the per-chapter self-verify loop (throwaway
  `_preview-chN.typ`, compile + eyeball both modes) and re-run the label-collision
  check (`grep` figure/table labels across all chapters) before committing.

## Anything else
- Always compile **both** modes and eyeball diagram contrast before committing;
  never hardcode diagram colors (edges/arrowheads use `reader-colors.muted`, not
  black/`border`; node fills use `reader-colors.*_soft` with explicit text color).
- Define each term once, in a standalone Term box — never inline mid-sentence,
  never twice.
- Verify quoted `file:line` references against the actual file (line numbers drift).
