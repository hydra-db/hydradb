---
name: diagrams
description: >
  The house diagram aesthetic, extracted from the quint book — the most
  dark-mode-robust figures in the repo — so the conceptual and inside books can
  be brought up to the same standard. Covers explicit per-node text fills,
  explicit node strokes, the semantic fill vocabulary (ok/warn/info/purple/bad
  and the dashed-none "deferred" role), failure edges, edge-label typography,
  the sequence-diagram and state-tree idioms, region enclosures, and the
  full-sentence caption rule. Use whenever you add, upgrade, or review a
  #figure/diagram/table in any of the three books. Triggers: "add a diagram",
  "this figure is unreadable in dark mode", "upgrade the figures", "draw the
  state machine", "make a sequence diagram", "fix the caption".
---

# Diagrams (the quint-book aesthetic)

`DIAGRAM-STYLE.md` in `books/` states the rules. This skill is the *worked
technique*: why quint's figures survive dark mode when the older ones do not,
with copy-pasteable snippets. Read `DIAGRAM-STYLE.md` first; this file does not
replace it.

Exemplars, in the order worth reading:

| File | What to steal from it |
|---|---|
| `chapters/quint-00-correctness-problem.typ:47-63` | the sequence-diagram idiom (lifelines, lost reply) |
| `chapters/quint-00-correctness-problem.typ:170-184` | vertical layer stack, `fill: none` deferred layer |
| `chapters/quint-02-first-model-m1.typ:697-735` | state tree: bold spine, faded forks, forbidden state, shaded region |
| `chapters/quint-02-first-model-m1.typ:743-760` | fan-out (one state, four enabled actions) |
| `chapters/quint-02-first-model-m1.typ:796-818` | the table figure convention |
| `chapters/quint-06-bounded-proof-apalache.typ:97-222` | multi-panel figure via `grid` + `kind: image` |
| `chapters/quint-08-assurance-stack.typ:69-127` | rich multi-line band nodes, outer boundary, `stack` + trailing note |
| `chapters/quint-08-assurance-stack.typ:236-273` | nested enclosures = nested scopes of a claim |

The counterexample — do NOT copy: `chapters/intro-00-replaceable-compute.typ:24-35`.
Its nodes are `text(size: 8pt)[…]` with **no `fill:`**, sitting on
`reader-colors.bad_soft` / `purple_soft`. In dark mode the inherited body text
colour is light and the `*_soft` fills are light, so the labels vanish. That
single omission is the difference between the two books.

---

## Rule 1 — every node sets its own text colour

This is the single biggest dark-mode robustness win. Never let a node label
inherit the body text colour: the `*_soft` fills are light-ish in *both* modes,
while inherited body text flips to light in dark mode, and light-on-light is
unreadable.

```typ
// WRONG — inherits body text colour
node((0,0), text(size: 8pt)[committed], fill: reader-colors.ok_soft)

// RIGHT — the label carries its own colour
node((0,0), text(fill: reader-colors.text, size: 8pt)[committed],
     fill: reader-colors.ok_soft, stroke: 0.5pt + reader-colors.border)
```

Every node in `quint-00`, `quint-02`, and `quint-08` does this without
exception (e.g. `quint-02-first-model-m1.typ:711-716`, six consecutive nodes,
each with its own `text(fill: reader-colors.text)`).

Corollary: a *de-emphasised* node states `fill: reader-colors.muted` on its
text rather than lowering opacity on an inherited colour
(`quint-02-first-model-m1.typ:718-719`).

## Rule 2 — every node sets its own stroke

Either per-node `stroke: 0.5pt + reader-colors.border`
(`quint-00-correctness-problem.typ:56`) or once at diagram level
`node-stroke: 0.6pt + reader-colors.border`
(`quint-02-first-model-m1.typ:699`, `quint-08-assurance-stack.typ:72`). Never
leave it default: fletcher's default stroke is black and disappears on a dark
page. Diagram-level is preferred when most nodes share it; per-node overrides
then carry meaning (see the forbidden state, Rule 4).

## Rule 3 — the semantic fill vocabulary

Fills are not decoration; each one is a claim about the node. Keep this mapping
consistent across all three books.

| Role | Fill | Typical stroke | Seen at |
|---|---|---|---|
| committed / durable / success / proven-safe | `reader-colors.ok_soft` | border, or `ok` when the point is the guarantee | `quint-00:56`, `quint-02:713` |
| caution / no-op / ambiguous ("wrote nothing", "reply lost") | `reader-colors.warn_soft` | border | `quint-00:59`, `quint-02:751` |
| neutral in-progress step / active participant | `reader-colors.info_soft` | border | `quint-02:712`, `quint-08:111` |
| structural / inert / `init` / baseline | `reader-colors.surface_soft` | border | `quint-02:711`, `quint-08:118` |
| rejected / error / forbidden | `reader-colors.bad_soft` | `bad`, dashed when forbidden | `quint-02:721` |
| durable / storage layer (object store, artifacts) | `reader-colors.purple_soft` | `reader-colors.purple` | `DIAGRAM-STYLE.md:50` |
| deferred / future / out-of-scope | `fill: none` **or** `muted.transparentize(85%)`, `stroke: (dash: "dashed", paint: reader-colors.muted)`, label text `reader-colors.muted` italic | | `quint-00:181`, `quint-08:90` |

```typ
// committed / success
node((1,2), text(fill: reader-colors.text, size: 8pt)[commit durable · epoch += 1],
     fill: reader-colors.ok_soft, stroke: 0.5pt + reader-colors.border, inset: 5pt)

// caution / no-op
node((1,5), text(fill: reader-colors.text, size: 8pt)[recognise `K` · write nothing],
     fill: reader-colors.warn_soft, stroke: 0.5pt + reader-colors.border, inset: 5pt)

// neutral step
node((0,1), text(fill: reader-colors.text)[writer 1\ owns cell],
     fill: reader-colors.info_soft, width: 2.55cm)

// structural / baseline
node((0,0), text(fill: reader-colors.text)[`init`\ no writer],
     fill: reader-colors.surface_soft, width: 2.55cm)

// durable / storage
node((0.5,1), text(fill: reader-colors.text, size: 8pt)[Object store (SlateDB)],
     fill: reader-colors.purple_soft, stroke: reader-colors.purple,
     shape: fletcher.shapes.rect, corner-radius: 3pt)

// deferred / out of scope — no fill, dashed muted stroke, muted italic label
node((0,3), text(fill: reader-colors.muted)[_Jepsen_\ real processes under injected faults],
     stroke: (dash: "dashed", paint: reader-colors.muted), fill: none, width: 12.5cm)

// forbidden state — reachable-looking but deliberately unreached
node((2.95,4), text(fill: reader-colors.text)[*zombie COMMITS*\ 2nd durable write],
     fill: reader-colors.bad_soft, width: 2.5cm,
     stroke: (dash: "dashed", paint: reader-colors.bad, thickness: 1pt))
```

## Rule 4 — edges carry meaning through dash and paint

```typ
// normal step
edge((0,0), (1,0), "->", text(fill: reader-colors.muted, size: 8pt)[hydrate],
     stroke: reader-colors.muted)

// failure / loss — dashed, painted bad, label ALSO bad
edge((1,3), (0,3), "-->", text(fill: reader-colors.bad, size: 8pt)[acknowledgement lost],
     stroke: (dash: "dashed", paint: reader-colors.bad))

// the transition that does not exist (a guarantee drawn as an absence)
edge((0,4), (2.95,4), "-->", text(fill: reader-colors.bad, size: 7pt)[✗ no such\ transition],
     stroke: (thickness: 0.8pt, paint: reader-colors.bad, dash: "dashed"), label-pos: 0.78)

// the emphasised outcome edge
edge((1,6), (0,6), "->", text(fill: reader-colors.muted, size: 8pt)[original result — one edge],
     stroke: 0.7pt + reader-colors.ok)

// bold spine (the one behaviour being narrated)
edge((0,0), (0,1), "->", text(fill: reader-colors.info, size: 7.5pt)[`openWriter1`],
     stroke: 1.3pt + reader-colors.info, label-side: right)

// faded fork (an alternative the checker also explores)
edge((0,1), (-1.45,2), "->", text(fill: reader-colors.muted, size: 6.5pt)[`commitThenLoseReply`],
     stroke: (thickness: 0.6pt, paint: reader-colors.muted, dash: "dotted"),
     label-side: left, label-pos: 0.82)
```

Edge labels are **always** `text(fill: reader-colors.muted, size: 8pt)[…]` —
7.5pt or 6.5pt when the diagram is dense, `reader-colors.bad` for failures,
`reader-colors.info` for the bold spine. Never rely on fletcher's default label
colour, and never let an arrowhead default to black.

`label-side: left|right` and `label-pos: 0.78..0.82` are the two knobs that stop
labels colliding in fan-outs and long edges (`quint-02:723-732`).

## Rule 5 — the sequence-diagram idiom

Two participants, bold headers, dotted vertical lifelines drawn as one long
edge each, then time flows downward one row per event. Verbatim shape from
`quint-00-correctness-problem.typ:47-63`:

```typ
#figure(
  diagram(
    spacing: (3.0cm, 0.85cm),
    node-stroke: none,
    node((0, 0), text(weight: "bold", fill: reader-colors.text)[Client]),
    node((1, 0), text(weight: "bold", fill: reader-colors.text)[turbolay]),
    edge((0, 0), (0, 6), stroke: (dash: "dotted", paint: reader-colors.border)),
    edge((1, 0), (1, 6), stroke: (dash: "dotted", paint: reader-colors.border)),
    edge((0, 1), (1, 1), "->", text(fill: reader-colors.muted, size: 8pt)[create edge, key `K`],
         stroke: reader-colors.muted),
    node((1, 2), text(fill: reader-colors.text, size: 8pt)[commit durable · epoch += 1],
         fill: reader-colors.ok_soft, stroke: 0.5pt + reader-colors.border, inset: 5pt),
    edge((1, 3), (0, 3), "-->", text(fill: reader-colors.bad, size: 8pt)[acknowledgement lost],
         stroke: (dash: "dashed", paint: reader-colors.bad)),
  ),
  caption: [ … full sentence … ],
) <fig-lost-reply>
```

Notes: `node-stroke: none` at diagram level so the participant headers and the
in-band event boxes are not boxed by accident — the event boxes then re-state
their own `stroke:`. Lifelines use `border` (faint) while message arrows use
`muted` (visible); that contrast is deliberate.

## Rule 6 — region enclosures say "this set of states shares a property"

`node(enclose: (...))` declared **before** the states it wraps, so it paints
behind them:

```typ
node(
  enclose: ((0, 0), (0, 1), (-1.45, 2), (0, 2), (1.45, 2)),
  inset: 13pt, corner-radius: 7pt,
  stroke: (dash: "dashed", paint: reader-colors.ok, thickness: 0.8pt),
  fill: reader-colors.ok_soft.transparentize(60%),
)
```

Conventions: a **green dashed** enclosure = "the invariant holds in here /
proven safe" (`quint-02:703-709`, `quint-06:144-149`, `quint-08:251-257`); a
**muted dashed** enclosure = "the outer edge of what is checked at all"
(`quint-08:77-82`, `quint-08:242-248`). Nest them to show one claim sitting
inside a wider unproven world. Always `transparentize(55–90%)` so the states
stay legible through the wash.

De-emphasis generally is `.transparentize(…)` on a semantic token, never a
hardcoded grey: `surface_soft.transparentize(50%)` for unsampled nodes,
`bad_soft.transparentize(25%)` for a faded fork (`quint-06:114`, `quint-02:719`).

## Rule 7 — table figures

```typ
#figure(
  table(
    columns: (1.15fr, 1.15fr, 1.7fr),
    align: (left, left, left),
    stroke: 0.5pt + reader-colors.border,
    fill: (_, row) => if row == 0 { reader-colors.surface_soft },
    inset: 7pt,
    table.header(
      text(fill: reader-colors.text)[*Quint action*],
      text(fill: reader-colors.text)[*Real turbolay call*],
      text(fill: reader-colors.text)[*What it pins*],
    ),
    [`openWriter1`], [`open_standalone_writer`], [one effective writer acquires the cell],
  ),
  caption: [ … full sentence … ],
) <tab-m1-action-api>
```

Header cells set their own `fill:` on the text exactly like nodes do
(`quint-02:803-807`). In dense tables every body cell is wrapped in
`text(size: 8pt)[…]` too (`quint-08:215-219`).

## Rule 8 — captions and labels

- Full sentence(s) stating what the reader should *conclude*, never a
  noun-phrase title. This is the rule in `DIAGRAM-STYLE.md:7-11`; the quint
  captions are the model — see `quint-02:734`, which explains the bold spine,
  the faded forks, the shaded region, *and* why the arrowless node matters.
- The caption is where "how to read this picture" lives. If the figure has a
  legend-like structure (left/middle/right panels, bands), name each part in the
  caption: `quint-06:213-221`.
- Cite the grounding file inside the caption when the figure asserts a number:
  `(0003-turbolay-quint-verification-evidence.md:17-19)` at `quint-06:220-221`.
- `<label>` goes **immediately after the closing paren**, same line:
  `) <fig-lost-reply>`. Prefix `fig-` for diagrams, `tab-` for tables; include
  the chapter, e.g. `<fig-ch9-stack>`, `<tab-ch7-run-vs-verify>`.
- Never `caption: none`.

## Rule 9 — composite figures

Two escape hatches, both from quint:

- **Multi-panel**: wrap `grid(columns: 3, …)` of separate `diagram()`s in one
  `#figure(kind: image, supplement: [Figure], …)`, with a
  `text(fill: reader-colors.text, size: 8.5pt)[*header*]` row above the panels
  (`quint-06:97-108`). `kind:`/`supplement:` are required because the body is a
  grid, not an image.
- **Diagram plus footnote line**: `stack(dir: ttb, spacing: 10pt, diagram(…),
  align(center, text(fill: reader-colors.muted, size: 8pt, style: "italic")[…]))`
  for a "beyond this boundary" note that belongs to the picture but not to any
  node (`quint-08:69-125`).

Rich band nodes hold several lines with their own sizes — bold 9.5pt title,
8pt body, 7.5pt muted limit — inside `align(left, block(width: 100%)[…])` with
`#v(2pt)` spacers (`quint-08:84-90`).

---

## Dark-mode failure modes, and how to avoid them

> **Colour-token correctness is necessary but not sufficient.** The three
> defects numbered 0, 8, and 9 below all pass a source-level audit cleanly —
> there is no `rgb(...)` to grep for. They are only visible by rendering the
> page in dark mode and looking at it. Never declare a figure done from the
> source alone.

### 0. The fletcher white-label bug (REQUIRED fix on every diagram)

`crossing-fill` defaults to literal `white`
(`fletcher/0.5.7/src/diagram.typ:461`). Separately, an edge's `label-fill:
auto` resolves to `true` for any edge whose `label-side` is `center`, and
`true` then resolves to the diagram's `crossing-fill`
(`fletcher/0.5.7/src/edge.typ:953-955`). Net effect: **every centred edge label
renders as a white box on the dark page.** Nothing in the chapter source
contains a colour literal, so no grep will ever find it.

Set the mode-aware fill explicitly on **every** `diagram(...)` call — not only
the ones with crossing edges:

```typ
diagram(
  crossing-fill: reader-colors.paper,   // REQUIRED — fletcher defaults to white
  spacing: (10mm, 8mm),
  node-stroke: 0.5pt + reader-colors.border,
  ...
)
```

This preserves the label-occlusion benefit (labels still mask the lines they
sit on) and is invisible in light mode, where `paper` is white anyway. Treat it
as boilerplate on the same footing as `node-stroke`. It is already applied
across the conceptual book and is being rolled out to the other two.

1. **Inherited label colour on a `*_soft` fill.** The classic. Light text on a
   light fill in dark mode. → Rule 1: every label gets `text(fill: …)`.
   Counterexample: `chapters/intro-00-replaceable-compute.typ:24-35`.
2. **Default (black) node stroke.** Invisible on a dark page. → Rule 2.
3. **Default (black) arrowheads.** `edge(a, b, "->")` with no `stroke:` paints a
   black head that vanishes in dark mode. Always name
   `stroke: reader-colors.muted` (and note `border` is too faint for an
   arrowhead — use `muted`).
4. **Hardcoded hex or `luma()`.** `rgb("#eef4ff")`, `luma(60%)` do not switch
   with `--input mode=`. → only `reader-colors.*` tokens.
5. **Opacity for de-emphasis instead of a colour token.** A 40%-opacity black
   label is invisible on dark. → use `fill: reader-colors.muted`, and
   `.transparentize()` only on *fills*, never as a substitute for a text colour.
6. **Relying on the caption's inherited colour inside a diagram.** Any text
   drawn *inside* `diagram(...)` — frontier labels, lens annotations — needs its
   own fill too (`quint-06:175`, `quint-08:269-270`). Only the real `caption:`
   is styled by the theme.
7. **Mid-word hyphenation from an undersized node.** A node narrower than its
   label wraps mid-word: a 32mm node broke "serializable transaction" into
   "transac-/tion". Source-invisible. → widen the node, shorten the label, or
   set `hyphenate: false` on the label text as `quint-02:713` does; insert your
   own `\` line break where you want the wrap.
8. **A diagram wider than the text block, spilling into the margins.** Measure
   against the **~17.3 cm text block**, *not* the 25 cm paper width — that trap
   produced a 19.9 cm diagram that nearly touched the paper edge. Target
   11–14 cm; treat 17.3 cm as a hard ceiling, not a target. Sum your node
   `width:` values plus `spacing` to sanity-check before rendering, then confirm
   by looking at the page.

## Pre-flight checklist

Before declaring a figure done:

- [ ] Every `node(...)` label is `text(fill: reader-colors.…)[…]` — grep the
      block for `text(` without `fill:`.
- [ ] **`crossing-fill: reader-colors.paper` is set on every `diagram(...)`**
      — fletcher defaults to white and centred edge labels become white boxes
      in dark mode (`fletcher/0.5.7/src/diagram.typ:461`).
- [ ] Every node has a stroke, from `node-stroke:` or per-node.
- [ ] Every `edge(...)` names a `stroke:`; no default arrowheads.
- [ ] Every edge label is `text(fill: reader-colors.muted, size: 8pt)[…]`
      (or `bad`/`info` where semantics demand).
- [ ] No `rgb("…")`, no `luma(…)`, no bare colour names in the figure.
- [ ] Fills follow the Rule 3 semantic table — a green box means "committed",
      not "I liked green here".
- [ ] Deferred/out-of-scope things are `fill: none` + dashed muted, and the
      caption says they are out of scope.
- [ ] Enclosures are declared before the nodes they wrap and are
      `transparentize`d.
- [ ] Caption is a full sentence stating the takeaway, and names each panel/band
      if the figure has parts.
- [ ] `<label>` on the same line as the closing paren; `fig-`/`tab-` prefix.
- [ ] Width ≈ 11–14 cm, measured against the **17.3 cm text block** (never the
      25 cm paper width); node text 8–9pt.
- [ ] No node is narrow enough to hyphenate its own label mid-word.
- [ ] **Rendered to PNG in dark mode and visually inspected.** A source-level
      audit cannot catch the white-label bug, mid-word hyphenation, or margin
      spill. This step is not optional:
      ```sh
      cd books && typst compile --input mode=dark  quint.typ quint.pdf
      cd books && typst compile --input mode=light quint.typ quint.pdf
      # then render the affected pages and actually look at them:
      cd books && typst compile --input mode=dark --format png --ppi 150 \
        --pages <n> quint.typ /tmp/quint-dark-{p}.png
      ```
      Only the "DejaVu Serif" font warning is acceptable. Check both modes, but
      dark is where the defects live.
