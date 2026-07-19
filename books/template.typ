// Styling helpers for the turbolay onboarding book.
// Everything visual lives here so the chapter files stay focused on content.

#import "vendor/bookly/src/themes/reader.typ": reader-colors

#let page-bg = reader-colors.paper
#let body-fg = reader-colors.text
#let accent = reader-colors.primary_active
#let accent-soft = reader-colors.surface_soft
#let term-bg = reader-colors.purple_soft
#let term-border = reader-colors.purple
#let why-bg = reader-colors.warn_soft
#let why-border = reader-colors.warn
#let code-bg = reader-colors.code_bg
#let code-bar = reader-colors.border
#let muted = reader-colors.muted

// ---------------------------------------------------------------------------
// Document setup. Call once at the top of book.typ with #show: book.with(...)
// ---------------------------------------------------------------------------
#let book(title: "", subtitle: "", body) = {
  set document(title: title)
  set page(
    paper: "a4",
    margin: (x: 2.4cm, y: 2.6cm),
    numbering: "1",
    number-align: center,
    fill: page-bg,
  )
  set text(font: ("Libertinus Serif", "New Computer Modern", "DejaVu Serif"), size: 10.5pt, fill: body-fg, lang: "en")
  set par(justify: true, leading: 0.62em)
  show raw: set text(font: "DejaVu Sans Mono", size: 8.6pt)

  // Heading styling.
  set heading(numbering: "1.1")
  show heading.where(level: 1): it => {
    pagebreak(weak: true)
    block(above: 0.5em, below: 0.9em)[
      #text(size: 9pt, fill: accent, weight: "bold")[CHAPTER #counter(heading).display("1")]
      #v(-0.4em)
      #text(size: 20pt, weight: "bold")[#it.body]
    ]
  }
  show heading.where(level: 2): it => block(above: 1.1em, below: 0.6em)[
    #text(size: 13.5pt, weight: "bold", fill: body-fg)[#it]
  ]
  show heading.where(level: 3): it => block(above: 0.9em, below: 0.5em)[
    #text(size: 11.5pt, weight: "bold", fill: muted)[#it]
  ]

  // Inline code gets a subtle background.
  show raw.where(block: false): it => box(
    fill: code-bg, inset: (x: 3pt, y: 0pt), outset: (y: 3pt), radius: 2pt,
  )[#it]

  body
}

// ---------------------------------------------------------------------------
// Term definition box. Use on first appearance of any jargon.
// ---------------------------------------------------------------------------
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

// ---------------------------------------------------------------------------
// "Why it is done this way" callout.
// ---------------------------------------------------------------------------
#let why(body) = block(
  width: 100%, fill: why-bg, stroke: (left: 2pt + why-border),
  inset: (x: 10pt, y: 8pt), radius: 2pt, above: 0.9em, below: 0.9em,
)[
  #text(size: 8pt, weight: "bold", fill: why-border)[WHY]
  #v(-0.3em)
  #body
]

// ---------------------------------------------------------------------------
// Source excerpt with a file-location bar above the code.
// Pass the location string and a raw code block:
//   #srcblock("src/lib.rs:156")[```rust ... ```]
// ---------------------------------------------------------------------------
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
