#import "@preview/hydra:0.6.2": hydra
#import "@preview/marginalia:0.3.1" as marginalia: wideblock
#import "@preview/showybox:2.0.4": *
#import "@preview/itemize:0.2.0" as el
#import "../bookly-helper.typ": *
#import "../bookly-defaults.typ": *

#let fancy-theme(colors: default-colors, it) = {
  // Headings
  show heading.where(level: 1): it => {
    if not states.open-right.get() {
      pagebreak(weak: true)
    }

    // Reset counters
    reset-counters

    // Heading style
    // place(top)[
    //   #rect(fill: white, width: 1%, height: 1%)
    // ]

    set align(right)
    set underline(stroke: 2pt + colors.secondary, offset: 8pt)

    show: show-if(states.tufte.get(), it => {
      show: wideblock.with(side: "both")
      it
    })

    if it.numbering != none {
      v(5em)
      block[
        #text(counter(heading).display(states.num-heading.get()), size: 4em, fill: colors.primary)
        #v(-3em)
        #text(underline(it.body), size: 1.5em)
      ]
      v(5em)
    } else {
      v(1em)
      text(underline(it.body), size: 1.5em)
      v(2.5em)
    }
  }

  show heading.where(level: 2): it => {
    block(above: 1.5em)[
      #if it.numbering != none {
        text(counter(heading).display(), fill: colors.primary)
        h(0.25em)
      }
      #text(it.body)
      #v(-0.5em)
      #line(stroke: 1.5pt + colors.secondary, length: 100%)
      #v(0.75em)
    ]
  }

  show heading.where(level: 3): it => {
    block[
      #if it.numbering != none {
        text(counter(heading).display(), fill: colors.primary)
        h(0.25em)
      }
      #text(it.body)
      #v(1em)
    ]
  }

  // Lists
  show: el.default-enum-list
  set list(marker: [#text(fill:colors.primary, size: 1.1em)[#sym.bullet]])
  set enum(numbering: n => text(fill:colors.primary)[#n.])

  // Footnotes
  set footnote.entry(separator: line(length: 30% + 0pt, stroke: 1pt + colors.secondary))

  // References
  show ref: set text(fill: colors.primary)

  // Tables
  show table.cell.where(y: 0): set text(weight: "bold", fill: white)
    set table(
    fill: (_, y) => if y == 0 {colors.primary} else if calc.odd(y) { colors.secondary.lighten(60%)},
    stroke: (x, y) => (
      left: if x == 0 or y > 0 { (thickness: 1pt, paint: colors.secondary) } else { (thickness: 1pt, paint: colors.primary) },
      right: (thickness: 1pt, paint: colors.secondary),
      top: if y <= 1 { (thickness: 1pt, paint: colors.secondary) } else { 0pt },
      bottom: (thickness: 1pt, paint: colors.secondary),
    )
  )

  // Outline
  set outline.entry(fill: box(width: 1fr, repeat(gap: 0.25em)[.]))
  show outline.entry: it => {
    show linebreak: none
    if it.element.func() == heading {
      let number = it.prefix()
      let section = it.element.body
      let item = none
      if it.level == 1 {
        block(above: 1.25em, below: 0em)
        v(0.5em)
        item = [#text([*#number*], fill: colors.primary) *#it.inner()*]
      } else if it.level == 2{
        block(above: 1em, below: 0em)
        item = [#h(1em) #text([#number], fill: colors.primary) #it.inner()]
      } else {
        block(above: 1em, below: 0em)
        item = [#h(2em) #text([#number], fill: colors.primary) #it.inner()]
      }
      link(it.element.location(), item)
    } else if it.element.func() == figure {
      block(above: 1.25em, below: 0em)
      v(0.25em)
      link(it.element.location(), [#text([#it.prefix().], fill: colors.primary) #h(0.2em) #it.inner()])
    } else {
      it
    }
  }

  // Page style
  let page-header = context {
    // Pas de header sur les pages d'ouverture de chapitre
    let h1-on-page = query(heading.where(level: 1)).filter(h => h.location().page() == here().page())
    if h1-on-page.len() > 0 { return }

    show linebreak: none
    show: show-if(states.tufte.get(), it => {
      show: wideblock.with(side: "both")
      it
    })
    set text(style: "italic", fill: colors.header)
    if calc.odd(here().page()) {
      align(right, hydra(2))
    } else {
      align(left, hydra(1))
    }
  }

  let page-footer = context {
    let current-page = counter(page).display()
    let page-final = counter(page).final().first()

    set align(center)
    show: wideblock.with(side: "both")
    if states.isfrontmatter.get() {
      [#current-page]
    } else {
      [#current-page/#page-final]
    }
  }

  set page(
    paper: paper-size,
    header: page-header,
    footer: page-footer
  )

  it
}

// Boxes - Definitions
#let custom-box-fancy(title: none, icon: "info", color: rgb(29, 144, 208), body) = showybox(
  title: grid(
    columns: 2,
    align: (left + horizon, right + horizon),
    column-gutter: 0.5em,
    [#color-svg("resources/images/icons/" + icon + ".svg", white)],
    [#title]
  ),
  title-style: (
    boxed-style: (
      anchor: (x: left, y: horizon)
    )
  ),
  frame: (
    title-color: color,
    border-color: color,
    body-color: color.lighten(90%),
    thickness: 1pt
  ),
  align: center
)[
  #body
  #v(0.5em)
]

// Part
#let part-fancy(title) = context {
  states.counter-part.update(i => i + 1)
  set page(
    header: none,
    footer: none,
    numbering: none
  )

  set align(center + horizon)

  if states.open-right.get() {
    pagebreak(weak: true, to:"odd")
  }

  let top-length = 80%
  let mid-length = 35%
  if states.tufte.get() {
    top-length = 86.5%
    mid-length = 38%
  }

  wideblock(side: "both")[
    #line(stroke: 1.75pt + states.colors.get().primary, length: top-length)
    #text(size: 2.5em)[#states.localization.get().part #states.counter-part.display(states.part-numbering.get())]
    #line(stroke: 1.75pt + states.colors.get().primary, length: mid-length)
    #text(size: 3em)[*#title*]
    #line(stroke: 1.75pt + states.colors.get().primary, length: top-length)
  ]

  show heading: none
  heading(numbering: none)[
    #v(1em)
    #box[#text(fill:states.colors.get().primary)[#states.localization.get().part #states.counter-part.display(states.part-numbering.get()) -- #title]]
  ]

  if states.open-right.get() {
    pagebreak(weak: true, to:"odd")
  }
}

// Minitoc
#let minitoc-fancy = context {
  let toc-header = states.localization.get().toc
  block(above: 3.5em)[
    #text([*#toc-header*])
    #v(-0.5em)
  ]

  let miniline = line(stroke: 1.5pt + states.colors.get().secondary, length: 100%)

  miniline
  v(0.5em)
  suboutline(target: heading.where(outlined: true, level: 2))
  miniline
}

#let boxeq-fancy(body) = context _boxeq(stroke: 1pt + states.colors.get().boxeq.darken(35%), fill: states.colors.get().boxeq, radius: 5pt, body)

#let fancy = (theme: fancy-theme, part: part-fancy, minitoc: minitoc-fancy, box: custom-box-fancy, boxeq: boxeq-fancy)
