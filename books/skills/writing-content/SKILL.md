---
name: writing-content
description: >
  Phase 2 of authoring the turbolay book — writing the teaching prose into a
  chapter that formatting-chapters has already scaffolded. Encodes the book's
  voice: Socratic problem→answer, Feynman first-principles intuition, assume the
  reader knows nothing, ground every claim in real repo files. Fills
  TODO(content) placeholders WITHOUT changing the Bookly structure or
  components. Use when writing or revising the actual content of a chapter.
  Triggers: "write the chapter", "fill in the content", "write Act …", "flesh
  out this section", "explain this the way the book does".
---

# Writing content (Phase 2: prose, not layout)

Assume a chapter skeleton already exists (from `formatting-chapters`): the
Bookly imports, the heading structure, empty callouts, and `TODO(content)`
markers with a `LEARNING GOAL` / `GROUND IN` header. Your job is to fill the
prose. **Do not restructure, do not swap components, do not touch the theme
wiring.** Content and formatting are separate concerns handled in separate
phases — respect that boundary.

## Read these first, every time

- The chapter's own `LEARNING GOAL` / `GROUND IN` header, and every file it
  names. Never write a claim you have not grounded in a real file.
- `chapters/00-foundations.typ` and `chapters/intro-00-replaceable-compute.typ`
  — the canonical voice. Match their register.
- `skills/chapter-framing/SKILL.md` — the chapter's *argument shape* (the
  declarative-claim title, the opening scenario, the `Problem N` ladder, the
  synthesis / honest-boundary / revision-notes sections) and the prose rhythm
  that carries it, extracted from the v1 book. This skill covers voice and
  grounding; that one covers the skeleton those sentences hang on. If a chapter
  reads like a summary rather than a lesson, the defect is framing, not prose.
- `skills/diagrams/SKILL.md` — before adding or editing any figure.

## The method (this is the whole point)

**Socratic + Feynman, problem before answer.** Build intuition; do not lecture.

1. **Open with a concrete problem, not a definition.** Pose a specific situation
   the reader can feel — a write whose reply is lost, a page that reads the wrong
   snapshot — and ask the question it forces. Then answer it. Every section earns
   its concepts by needing them first.
2. **Assume zero prior knowledge** of the topic being introduced. Define every
   term from first principles the first time it appears, in a Term box. Do not
   assume the reader has read other parts unless the skeleton says so; a
   standalone part must stand alone.
3. **Explain it the way you'd explain it to a smart friend who is new** — plain
   words, short chains of reasoning, no jargon that hasn't been defined, no
   marketing. Prefer "here is the problem, here is why the obvious fix fails,
   here is what turbolay does" over a flat description.
4. **Ground everything in the real code.** Quote actual files with accurate paths
   and line numbers (open the file and count — line numbers drift, so verify).
   Never invent an API name, file path, function, or number. If you are unsure,
   go read it. Paraphrase docs in your own words; quote code.
5. **Make it runnable where you can.** Prefer a small snippet the reader could
   actually type and run (e.g. a toy Quint model, a `mise exec -- quint run …`
   command) over an abstract description. Keep runnable examples correct.
5b. **Show it, don't just tell it.** If a paragraph is describing a sequence of
   events, a set of states and transitions, a concept's parts, or a mapping
   between two things, replace or accompany it with a figure or table. A page
   that is wall-to-wall prose is a defect — break it up with visuals. Use the
   Bookly/fletcher/cetz/table conventions and theme-aware colors from the
   `formatting-chapters` skill; never hardcode diagram colors.
6. **State the learning goal's payoff.** By the end of the chapter the reader
   should be able to do the thing the `LEARNING GOAL` promises. Close by naming
   what the next chapter does, by descriptive name.
7. **Be honest about boundaries.** When a claim has limits (a bounded check is
   not a proof; an abstraction omits real failure modes), say so plainly — that
   candor is part of the book's voice.

## Voice checklist

- Calm, precise, second person occasionally. Justified full paragraphs, not
  bullet dumps (bullets only for genuine lists).
- No hype, no "simply/obviously", no filler.
- Full sentences in callout boxes too.
- Cross-reference other chapters by descriptive name, not by internal codes.

## Filling the scaffold

- Replace each `TODO(content)` with prose; keep the surrounding component.
- Fill Term boxes (`#custom-box(title: [Term — …], icon: "info")`) with
  first-principles definitions; fill Why boxes
  (`#custom-box(title: [Why], icon: "tip")`) with design rationale.
- **Do not pass a `color:` argument.** The accent is derived from `icon:`, not
  from `color:` — see "The callout vocabulary" below.
- Put code inside the existing fenced blocks / figures; add the inline
  `file:line` reference in the introducing sentence.
- Give figures real captions. If a diagram or Term box the skeleton lacks is
  genuinely needed, add it using the SAME Bookly components (see
  `formatting-chapters`) — but do not convert existing ones to anything else.

## The callout vocabulary (settled decision — read before writing a box)

**All three books converge on Bookly's `custom-box`.** This is a decision by the
book owner, not a preference. The `books/template.typ` helpers — `term`, `why`,
`srcblock`, `figcap`, `accent`, `muted` — are being **retired**. A separate
conversion pass is rewriting the `inside` book's remaining `#term` / `#why`
calls into `custom-box`. Do not add new calls to any of them, in any book,
including files that still contain them; if you are revising a section that
already uses `#term`/`#why`, write the replacement prose in `custom-box` form.

The two standardized forms, and the only ones you should write:

```typ
#custom-box(title: [Term — <Name>], icon: "info")[ … ]
#custom-box(title: [Why], icon: "tip")[ … ]
```

**No `color:` argument.** It is dead code. `reader-box`
(`vendor/bookly/src/themes/reader.typ:419-421`) computes its accent with
`let accent = _accent-for(c, icon, color)`, and `_accent-for`
(`reader.typ:149-165`) branches on the *icon name* — `"tip"` → `c.ok`,
`"alert"`/`"stop"` → `c.bad`, `"question"`/`"report"` → `c.purple`, `"info"` →
`c.info` — falling through to `color` only for an icon it does not recognize.
The companion `_soft-for` (`reader.typ:167`) picks the fill the same way. Every
one of the six documented icons is recognized, so `color:` has never had any
effect on these boxes: Why boxes have always rendered **green** (`c.ok`), never
the gold that was written into the old snippets. Green is accepted; the argument
is dropped. Do not reintroduce it — passing a colour that the render silently
ignores is how the last round of drift started.

To change a box's accent, change the `icon:`. That is the only lever.

## What NOT to do

- Do not import `template.typ` or use `term`/`why`/`srcblock`/`figcap`/`accent`/
  `muted` — all three books are converging on `custom-box` (see above).
- Do not pass `color:` to `custom-box`. The accent comes from `icon:`.
- Do not change headings' structure or order, or rename the file.
- Do not compile Typst as part of writing (the format phase / a final check owns
  that); just make sure your Typst is well-formed.
- Do not pad for length. A tight chapter that hits its learning goal beats a long
  one that wanders.

## Definition of done for Phase 2

- No `TODO(content)` markers remain.
- Every term is defined on first use; every non-obvious claim is grounded in a
  named file; every quoted line number is verified.
- The chapter delivers its stated `LEARNING GOAL` and ends by pointing forward.
- Structure and components are exactly as the skeleton left them.
