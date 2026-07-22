# Diagram style — all three books (conceptual.typ, inside.typ, quint.typ)

> See also `skills/diagrams/SKILL.md` — the worked technique behind these rules,
> extracted from the quint book: copy-pasteable snippets per semantic role, the
> dark-mode failure modes (including the fletcher `crossing-fill: white` bug),
> and a pre-flight checklist.

## Intuition-first rule (read this first)

The goal of every diagram is to build a mental model. So:

- **The caption IS the "how to read it" explanation.** Write a full sentence (sometimes
  two) stating what the reader should conclude from the picture — not a noun-phrase title
  naming it. Do not leave a diagram to speak for itself, and do not split the takeaway
  into a separate block: it goes in `caption:` so it also lands in the List of Figures.
  There is no `#figcap` in current chapters; if you find one, fold it into the caption.
  Canonical example — `chapters/intro-00-replaceable-compute.typ:36`:

  ```typ
  #figure(
    diagram( ... ),
    caption: [Memory and local disk make the graph faster, not true: a replacement node
      rebuilds the same logical state from the same object-store path.],
  ) <fig-intro00-boundary>
  ```
- Prefer a diagram + short explanation over a dense paragraph whenever a concept is
  structural (a boundary, a pipeline, a lifecycle, a merge, a set of tiers).
- One diagram per chapter minimum. All three books get this treatment. The `inside.typ`
  chapters (00–05) already import fletcher and already have some diagrams; ADD or UPGRADE
  so the chapter's **key implementation concept** has an intuition diagram, and make sure
  every diagram has its short explanation. Do not delete correct existing diagrams.
- New diagrams in BOTH books should use the mode-aware `reader-colors` tokens below (the
  inside book is also mode-aware via `template.typ`). Leave existing correct diagrams as-is
  unless you are upgrading them.

---

All diagrams use **fletcher** and **theme-aware colors** so
they render correctly in dark / light / sepia. NEVER hardcode rgb hex for fills or
strokes — the conceptual book's `reader-colors` palette switches with the build
`mode`. Use the semantic tokens below.

## Import (add once per chapter, after the bookly import)

```typ
#import "@preview/fletcher:0.5.7" as fletcher: diagram, node, edge
```

`reader-colors` is already in scope (the chapter imports `../vendor/bookly/src/bookly.typ: *`).

## Palette tokens (mode-aware — use these, not raw hex)

- Neutral box fill: `reader-colors.surface_soft`   · box stroke: `reader-colors.border`
- Text / labels: `reader-colors.text` (emphasis: `reader-colors.ink`) · captions/secondary: `reader-colors.muted`
- Durable / storage layer (object store, artifacts): fill `reader-colors.purple_soft`, stroke `reader-colors.purple`
- Active / commit / success: fill `reader-colors.ok_soft`, stroke `reader-colors.ok`
- Highlight / primary path: stroke `reader-colors.primary_active`, fill `reader-colors.primary_bg` (use sparingly)
- Info / neutral accent: fill `reader-colors.info_soft`, stroke `reader-colors.info`
- PLANNED / future nodes: fill `reader-colors.warn_soft`, stroke `reader-colors.warn`, and add `stroke: (dash: "dashed")`
- Edges: `stroke: reader-colors.muted`, arrowheads default; label text `reader-colors.muted`

## Conventions

- Wrap every diagram in a `#figure(...)` with a `caption:` and a `<label>` so it lands
  in the List of Figures. Use one clear label per chapter, e.g. `<fig-intro00-boundary>`.
  Never `caption: none` — that produces a blank List-of-Figures entry. The label goes
  immediately after the figure's closing paren. This applies to table and code-listing
  figures too.
- Keep node text short (a few words); put the explanation in prose, not the diagram.
- Prefer a readable grid: `diagram(spacing: (10mm, 8mm), node-stroke: 0.5pt, ...)`.
- Node helper pattern:
  ```typ
  node((0,0), [Object store], fill: reader-colors.purple_soft, stroke: reader-colors.purple, shape: fletcher.shapes.rect, corner-radius: 3pt)
  ```
- Edge pattern: `edge((0,0), (1,0), "->", stroke: reader-colors.muted, label: text(size: 8pt, fill: reader-colors.muted)[hydrate])`
- One primary diagram per chapter is required. Place it where it earns the concept
  (usually right after the section that introduces the mechanism), NOT at the very top
  before the problem is set up. Keep any existing tables/figures.
- Sizing: target ~11–14 cm wide so it fits the reader page (25 cm wide, wide outer
  margin). Use `text(size: 8pt..9pt)` inside nodes.

## Build check

After editing, the whole book must still compile:
`typst compile --input mode=dark conceptual.typ conceptual.pdf` (from books/).
Also sanity-check `--input mode=light`. Only the benign "DejaVu Serif" font warning is allowed.
