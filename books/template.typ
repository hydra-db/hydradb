// Shared foundation for ALL THREE TurboLay books (conceptual, inside, quint).
//
// Every book root is just:
//     #import "template.typ": *
//     #show: book.with(title: "...", subtitle: "...")
//     ... #include chapters ...
//
// The visual system is the vendored `bookly` reader theme. This file is the ONE
// place that configures it, so the three books render identically. The callout
// helpers below (term / why / srcblock / figcap) are the shared vocabulary the
// chapters use. Re-exporting bookly's `*` means roots and chapters can reach
// `part`, `note`, `reader-colors`, etc. through this module too.

#import "vendor/bookly/src/bookly.typ": *
#import "vendor/bookly/src/themes/reader.typ": reader-colors
#import "@preview/marginalia:0.3.1" as marginalia

// --------------------------------------------------------------------------
// Shared palette shortcuts (chapters import `accent` / `muted` from here).
// --------------------------------------------------------------------------
// Chapter numbering offset by one so the first chapter renders as 0 (see the
// note in `book` below). Levels below the chapter are passed through unchanged.
// Defensive: the reader theme's chapter title page and the `part` page both call
// this through `counter.display`, sometimes before any chapter has incremented
// the counter, so an empty or already-zero first level must not panic.
#let zero-based-numbering = (..n) => {
  let p = n.pos()
  if p.len() == 0 {
    ""
  } else {
    // "1.1." keeps bookly's trailing period (bookly-environments.typ:21).
    numbering("1.1.", calc.max(p.at(0) - 1, 0), ..p.slice(1))
  }
}

// Figure numbers embed the chapter number, so they need the same offset. This
// mirrors bookly's own `numbering-fig` (bookly.typ:87-94) with h1 - 1.
#let zero-based-figure-numbering = n => {
  let h1 = counter(heading).get().first()
  numbering(states.num-pattern-fig.get(), calc.max(h1 - 1, 0), n)
}

// The chapter opener page prints the number alone after the word "Chapter", so
// it wants a bare "1" pattern — the ToC's trailing period would render as
// "Chapter 1.".
#let zero-based-chapter-label = (..n) => {
  let p = n.pos()
  if p.len() == 0 {
    ""
  } else {
    numbering("1", calc.max(p.at(0) - 1, 0))
  }
}

#let zero-based-equation-numbering = (..n) => {
  let h1 = counter(heading).get().first()
  numbering(states.num-pattern-eq.get(), calc.max(h1 - 1, 0), ..n)
}

#let accent = reader-colors.primary_active
#let muted = reader-colors.muted
#let term-bg = reader-colors.purple_soft
#let term-border = reader-colors.purple
#let why-bg = reader-colors.warn_soft
#let why-border = reader-colors.warn
#let code-bg = reader-colors.code_bg
#let code-bar = reader-colors.border

// --------------------------------------------------------------------------
// Document setup. Wraps `bookly` with the shared HydraDB configuration.
// Call once per book: #show: book.with(title: "...", subtitle: "...").
// --------------------------------------------------------------------------
#let book(title: "", subtitle: "", body) = {
  show: bookly.with(
    author: "HydraDB Team",
    title: title,
    lang: "en",
    theme: reader,
    colors: reader-colors,
    fonts: (
      body: "Avenir Next",
      math: "New Computer Modern Math",
      raw: "Source Code Pro",
    ),
    title-page: reader-title-page(
      subtitle: subtitle,
      series: "HydraDB Technical Architecture Series",
      institution: "HydraDB",
      logo: none,
      cover: none,
    ),
    config-options: (
      open-right: false,
      part-numbering: "1",
    ),
  )

  show: marginalia.setup.with(
    inner: (far: 1.6cm, width: 0cm, sep: 0cm),
    outer: (far: 1cm, width: 4.5cm, sep: 0.6cm),
    top: 1.6cm,
    bottom: 1.6cm,
  )
  set page(width: 25cm, fill: reader-colors.paper)
  set text(size: 10pt, fill: reader-colors.text)

  show: front-matter
  show: main-matter

  // --- Zero-based chapter numbering -------------------------------------
  // The chapter files are named 00-, 01-, ... and every cross-reference in the
  // prose says "Chapter 0" / "Section 1.8". Bookly (and Typst) number from 1,
  // which rendered Foundations as "Chapter 1" and left every reference off by
  // one against the page. Typst counters cannot go negative, so rather than
  // seeding the counter we subtract one at display time, in four places: the
  // headings themselves, figures, tables, and the chapter opener label.
  //
  // These must come AFTER `main-matter`: it applies its own
  // `set heading(numbering: "1.1.")` (bookly-environments.typ:21), which would
  // otherwise win. Figures carry the chapter number too (bookly.typ:87-94 reads
  // the raw heading counter), so they need the same offset or "Figure 1.1"
  // would appear inside "Chapter 0".
  set heading(numbering: zero-based-numbering)
  // Figures and tables need `show ... : set` rules, not a bare `set figure`:
  // bookly installs its own `show figure.where(kind: ...)` rules
  // (bookly.typ:91, 118) and a show rule always beats a set rule.
  show figure.where(kind: image): set figure(numbering: zero-based-figure-numbering)
  show figure.where(kind: table): set figure(numbering: zero-based-figure-numbering)
  set math.equation(numbering: zero-based-equation-numbering)
  // The reader theme's chapter opener page renders
  // `counter(heading).display(states.num-heading.get())` (reader.typ:243), so it
  // needs the offset too or the opener says "Chapter 5" above a chapter the ToC
  // and running head both call 4. This update MUST come after `main-matter`,
  // which resets the state to the plain "1" pattern
  // (bookly-environments.typ:27). Wrapped in `_ => ...` because
  // `state.update(f)` treats a bare function as an updater (new = f(old)).
  states.num-heading.update(_ => zero-based-chapter-label)

  tableofcontents
  listoffigures
  listoftables

  body
}

// --------------------------------------------------------------------------
// Term definition box. Use on first appearance of any jargon.
// --------------------------------------------------------------------------
#let term(name, body) = block(
  width: 100%, fill: term-bg, stroke: (left: 2pt + term-border),
  inset: (x: 10pt, y: 8pt), radius: 2pt, above: 0.9em, below: 0.9em,
)[
  #text(size: 8pt, weight: "bold", fill: term-border)[TERM]
  #h(6pt)
  #text(weight: "bold")[#name]
  #v(-0.3em)
  #body
]

// --------------------------------------------------------------------------
// "Why it is done this way" callout.
// --------------------------------------------------------------------------
#let why(body) = block(
  width: 100%, fill: why-bg, stroke: (left: 2pt + why-border),
  inset: (x: 10pt, y: 8pt), radius: 2pt, above: 0.9em, below: 0.9em,
)[
  #text(size: 8pt, weight: "bold", fill: why-border)[WHY]
  #v(-0.3em)
  #body
]

// --------------------------------------------------------------------------
// Source excerpt with a file-location bar above the code:
//   #srcblock("src/lib.rs:156")[```rust ... ```]
// --------------------------------------------------------------------------
#let srcblock(loc, code) = block(above: 0.8em, below: 0.9em, width: 100%, breakable: true)[
  #block(
    width: 100%, fill: code-bar, inset: (x: 8pt, y: 4pt),
    radius: (top: 3pt), below: 0pt,
  )[#text(size: 8pt, font: "DejaVu Sans Mono", fill: muted)[#loc]]
  #block(
    width: 100%, fill: code-bg, inset: (x: 8pt, y: 6pt),
    radius: (bottom: 3pt), stroke: 0.5pt + code-bar,
  )[#code]
]

// A figure caption for diagrams.
#let figcap(body) = align(center)[#text(size: 9pt, fill: muted, style: "italic")[#body]]
