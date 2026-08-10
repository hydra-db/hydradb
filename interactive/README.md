# TurboLay — Interactive Textbooks

Self-contained interactive HTML editions of the TurboLay books, generated from
`books/chapters/*.typ`. No build step, no dependencies — open `index.html` in any
browser (double-click works; `file://` is fine).

## What's here

Two books (the `quint` verification book is intentionally excluded):

| Book | Folder | Audience | Chapters |
|------|--------|----------|----------|
| **TurboLay — A Conceptual Guide** | `conceptual/` | curious CS undergraduate | `intro-00`…`intro-05`, `detail-01`, `detail-02` |
| **Inside TurboLay** | `inside/` | working engineer | `00-foundations`…`05-caching` |

Plus a set of **design notes** — standalone analyses of open problems. These are not
textbooks: no tooltips, and they carry their own inline styles rather than
`assets/textbook.css`. The first two are static; the third carries its own widgets.

| Note | File | Subject |
|------|------|---------|
| One Cell, One Writer | `write-routing-problem.html` | why a write cannot reach the correct single-writer node today |
| Routing the Write | `write-routing-solutions.html` | four ways to place the writer, scored — TiDB/TiKV and sleet as reference |
| Placing the Writer | `write-routing-placement.html` | the landed fix — rendezvous hashing, heartbeat liveness, the don't-promote rule, and why the duel is bounded rather than prevented. Five interactive SVG widgets (W1–W5), self-contained |
| Requests, Not Bytes | `incremental-build-cost.html` | why the incremental index build inverted on staging — request-bound vs data-bound cost anatomy, measured ledger, why the read path never noticed, break-even calculator widget, and the L0–L3 fix ladder with implementation sketches |
| Where the Time Goes | `read-write-path-performance.html` | the read and write paths drawn as they actually run, every finding re-verified against `main @ 8e91fbe`, and the benchmark / flamegraph / Grafana plan to price each one. Defers to `incremental-build-cost.html` on the WAL tail. Three annotated SVG diagrams, static |

## The interactive layer (after the [interactive-textbook skill](https://github.com/alharkan7/skills))

- **Nested tooltips** — dotted terms open a hover tooltip; keep hovering to *pin*
  it, then hover terms *inside* it to drill deeper (Paradox-grand-strategy style).
  Esc closes the chain. Click/Enter works for touch & keyboard. Cycles are
  detected and flagged.
- **Interactive widgets** — every diagram from the printed book is rebuilt as a
  runnable SVG widget (kill a node and watch it rehydrate, drag `read_epoch` and
  watch the WAL tail fold on, step the `write_edge` phases through a fenced
  retry, toggle Roaring bitmap containers, …). They live inside the
  "deeper reading" accordions and re-render with the light/dark theme.
- **Verbatim prose** — body text is reproduced faithfully from the source `.typ`;
  the widgets and tooltips are additive, never a rewrite.

## Files

```
index.html              landing page (both books' tables of contents)
assets/textbook.css     the reader-theme palette (light + dark), all components
assets/textbook.js      tooltip engine, theme toggle, progress bar, widget registry
assets/glossary.js      shared prerequisite-concept definitions (nested)
conceptual/*.html        book 1
inside/*.html            book 2
```

Each chapter page also defines a handful of *page-local* glossary entries inline
(for terms specific to that chapter) which merge onto the shared glossary.

## Regenerating

These pages are derived artifacts. If a source chapter changes, re-run the
conversion for that chapter against `assets/*` and the exemplar
`conceptual/intro-00-replaceable-compute.html`, which defines the house style.
