---
name: formatting-chapters
description: >
  Phase 1 of authoring the turbolay Typst book — scaffolding and layout ONLY.
  Use when creating a new chapter, part, or standalone book entry, or when
  wiring content into the Bookly theme. Sets up the correct imports, the Bookly
  component vocabulary, figures, and code blocks, then leaves TODO(content)
  placeholders for the writing-content skill to fill. Keeps formatting and
  content as two separate phases. Triggers: "new chapter", "new part of the
  book", "scaffold a chapter", "make it match the book theme", "format this
  chapter".
---

# Formatting chapters (Phase 1: layout, not prose)

The turbolay book is a **Typst + Bookly** project under `book/`. This skill sets
up the *skeleton* of a chapter or part so it renders in the real book theme.
It deliberately does **not** write the teaching prose — that is Phase 2, the
`writing-content` skill. Do the two in order and keep them separate: a chapter
you scaffold here should compile and look right while its body is still
placeholders.

## The one rule that was gotten wrong before

**Chapters render through Bookly, not through `template.typ`.** There are two
book systems in `book/`:

- `main.typ` → the real book. Uses `vendor/bookly` (reader theme, Avenir Next
  body, Source Code Pro code, marginalia, highlighted TOC, `#part`). Chapters
  named `intro-*.typ`, `detail-*.typ`, `turbolay-*.typ` are swept into it by
  `scripts/gen-index.sh`.
- `book.typ` + `template.typ` → an older minimal custom template with hand-rolled
  `term`/`why`/`srcblock`/`figcap` helpers. **Do not build new content on this.**
  It does not have the Bookly theme, so its TOC and boxes look wrong.

Any new chapter must import Bookly and use Bookly's components. If you ever find
yourself importing `../template.typ`, stop — that is the wrong foundation.

## Chapter skeleton

Every chapter file begins with exactly:

```typ
#import "../vendor/bookly/src/bookly.typ": *
// Add this line ONLY if the chapter draws box-and-arrow diagrams:
#import "@preview/fletcher:0.5.7" as fletcher: diagram, node, edge

= Chapter Title
```

Headings: `=` chapter (becomes "Chapter N"), `==` section, `===` subsection.
Read `chapters/intro-00-replaceable-compute.typ` as the canonical example of an
idiomatic chapter.

## Standalone part / independent entry

When a body of chapters must be **independently consumable** as its own PDF
(e.g. the "Verifying turbolay" Quint part), create an entry file at `book/<name>.typ`
that mirrors `main.typ` exactly — same `#show: bookly.with(...)`, marginalia,
page/text setup, and front/main-matter — then a single `#part([...])` and the
chapter includes. Use a distinct filename prefix (e.g. `quint-*`) so
`gen-index.sh` does **not** sweep the chapters into the main book. See
`book/quint.typ` for the reference entry. Build with:

```sh
cd book && typst compile --input mode=dark <name>.typ <name>.pdf
```

(The `unknown font family: dejavu serif` warning is harmless — a fallback.)
PDFs are gitignored (`*.pdf`); never commit them.

## Bookly component vocabulary (use these, not custom helpers)

All defined in `vendor/bookly/src/bookly-themes.typ`; available after the
`bookly.typ` import.

| Need | Component |
|---|---|
| Key one-line statement | `#boxeq[ *…* ]` |
| Margin aside / sidenote | `#note[…]` |
| Generic callout box | `#custom-box(title: <content>, icon: <str>)[…]` |
| Stock note / tip / warning / important / question | `#info-box(title:[…])[…]`, `#tip-box[…]`, `#warning-box[…]`, `#important-box[…]`, `#question-box[…]` |
| Figure (diagram or table) | `#figure(<diagram|table>, caption: [<caption>]) <label>` |
| Code | a plain fenced block ` ```rust … ``` ` — Bookly styles raw automatically |

Available `custom-box` icons: `"info"`, `"tip"`, `"alert"`, `"stop"`,
`"report"`, `"question"`.

`custom-box` accepts a `color:` argument, but **do not pass it.** The accent is
derived from the icon name: `reader-box` calls
`_accent-for(c, icon, color)` (`vendor/bookly/src/themes/reader.typ:421`) and
`_accent-for` (`reader.typ:149-165`) resolves all six icons above from a lookup
on `icon`, using `color` only as a fallback for an unrecognized one. A `color:`
on these boxes is silently ignored.

### Standardized semantic boxes for these books

All three books (`inside`, `conceptual`, `quint`) converge on `custom-box`; the
`template.typ` helpers `#term`/`#why`/`#srcblock` are being retired. Map the two
recurring teaching callouts to these exact forms:

- **Term / definition** (first appearance of jargon):
  `#custom-box(title: [Term — <Name>], icon: "info")[ … ]`
- **Why / design rationale**:
  `#custom-box(title: [Why], icon: "tip")[ … ]` — renders green (`c.ok`), which
  is the accepted colour.

### Code excerpts with a source location

Bookly has no "location bar" helper. Name the file inline in code font in the
sentence that introduces the block, then the fenced block:

```typ
The `createEdge` action, `quint-models/turbolay/m1_cell_write.qnt:60-81`:

​```rust
action createEdge: bool = all { ... }
​```
```

Verify line numbers against the actual file before quoting — open it and count.
Line numbers are signposts; keep the quoted definition accurate.

### Figures and diagrams

Use fletcher, put the caption inside the figure, and label it:
`#figure(diagram(...), caption: [ … ]) <fig-id>`. Do not use a separate caption
helper. Available viz libraries (all cached offline): `@preview/fletcher`
(box-and-arrow, state, sequence diagrams), `@preview/cetz` (freeform drawing),
and Typst `table`. Never reference `accent`/`muted` (old custom-template globals).

### Theme-aware diagram colors (MANDATORY — this was gotten wrong once)

The book renders in dark, light, and sepia via `--input mode=…`. **Never
hardcode a fill or text color in a diagram** (e.g. `rgb("#eef4ff")`, `luma(60%)`).
A hardcoded light fill plus the theme's light label text is invisible in dark
mode. Always pull colors from the mode-adaptive `reader-colors` palette (exported
by `bookly.typ`), and set an explicit label text fill so contrast holds in every
mode:

| Role | Fill | Text / stroke |
|---|---|---|
| neutral / structural node | `reader-colors.surface_soft` | text `reader-colors.text` |
| normal / in-progress node | `reader-colors.info_soft` | text `reader-colors.text` |
| success / committed node | `reader-colors.ok_soft` | text `reader-colors.text` |
| warning / ambiguous node | `reader-colors.warn_soft` | text `reader-colors.text` |
| error / rejected node | `reader-colors.bad_soft` | text `reader-colors.text` |
| node border / faint lifeline | — | `reader-colors.border` |
| edge line + arrowhead + label | — | `reader-colors.muted` (NEVER default/black — arrowheads default to black and vanish in dark mode; `border` is too faint for a marker) |
| accent edge (highlight) | — | `reader-colors.info` / `reader-colors.primary` |

Pattern: `node((x,y), text(fill: reader-colors.text)[label], fill: reader-colors.ok_soft, ...)`
and `edge(a, b, "->", text(fill: reader-colors.muted)[label], stroke: reader-colors.border)`.
Table headers: `fill: (_, row) => if row == 0 { reader-colors.surface_soft }` with
`stroke: 0.5pt + reader-colors.border`. Always compile BOTH `--input mode=dark`
and `--input mode=light` and eyeball contrast before declaring done.

### Visualize, don't just tell

Long stretches of prose are a smell. When a point is a flow, a state machine, an
exchange over time, a mapping, or a comparison, prefer a figure or table over a
paragraph. Good defaults per shape:

- **sequence of events / a protocol exchange** → fletcher two-lifeline sequence
  diagram (see `<fig-lost-reply>` in `quint-00`).
- **states and transitions** → fletcher state diagram (see the turnstile in
  `quint-01`, the M1 storyline in `quint-02`).
- **a concept's parts and how they relate** → a small labeled schematic.
- **a mapping (model ↔ code, action ↔ API) or a comparison** → a `table`
  (see `<tab-m1-action-api>` in `quint-02`).

Scaffold the empty `#figure(...)` with a `// TODO(content): …` caption where a
visual belongs, so Phase 2 fills the illustration, not another wall of text.

## Placeholders to hand off to Phase 2

Scaffold the chapter's **structure** — the `=`/`==`/`===` headings in teaching
order, empty boxes/figures where they belong — and drop content placeholders the
writing skill will replace. Example skeleton body:

```typ
== The problem
// TODO(content): Socratic opener — pose the concrete failure, then motivate.

#custom-box(title: [Term — Snapshot], icon: "info")[
  // TODO(content): first-principles definition.
]

== The answer
// TODO(content): walk it, grounded in <file:lines>.
```

Leave a short HTML-style comment at the top of the file listing the chapter's
**learning goal** and the **source files** the content must be grounded in, so
the Phase 2 agent stays focused:

```typ
// LEARNING GOAL: <one sentence>.
// GROUND IN: <docs/…>, <quint-models/…>, <src/…>.
```

## Definition of done for Phase 1

- File imports `bookly.typ` (and fletcher only if needed); no `template.typ`.
- Heading structure is complete and in teaching order.
- Every callout/figure is a real Bookly component, present but possibly empty.
- `TODO(content)` markers and the LEARNING GOAL / GROUND IN header are present.
- The standalone entry (if any) compiles.

Then invoke `writing-content` to fill it.
