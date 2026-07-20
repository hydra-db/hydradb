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
