#import "@preview/showybox:2.0.4": *
#import "@preview/hydra:0.6.2": hydra
#import "@preview/marginalia:0.3.1" as marginalia: wideblock
#import "@preview/itemize:0.2.0" as el
#import "../bookly-helper.typ": *
#import "../bookly-defaults.typ": *

#let pretty-theme(colors: default-colors, it) = {
  // Headings
  show heading.where(level: 1): it => {
    if not states.open-right.get() {
      pagebreak(weak: true)
    }

    // Reset counters
    reset-counters

    // Heading style
    show: show-if(states.tufte.get(), it => {
      show: wideblock.with(side: "both")
      it
    })

    // Ugly hack
    // let dy = if states.tufte.get() {-75%} else {-6%}
    // place(top, dy: dy, rect(stroke: none, fill: white, width: 116%))

    let text-size = 1.6em
    set align(left)
    let type-chapter = if states.isappendix.get() {states.localization.get().appendix} else {states.localization.get().chapter}

    let chapter-title = text(size: text-size)[#it.body]
    if it.numbering != none {
      v(2em)

      block[
        #text(size: 1.25em)[#type-chapter]
        #v(-1em)
        #box(stroke: (top: 1.5pt + colors.primary))[
          #context layout(area => {
            let num-str = text(size: text-size, fill: white)[#counter(heading).display(states.num-heading.get())]

            // Actual width of the left column (number + inset)
            let num-w = measure(box(inset: 0.5em)[#num-str]).width

            // Height of the title rendered at the effective width of the right column (total width - number column width)
            let title-inset = 0.5em
            let title-h = measure(block(width: area.width - num-w)[#chapter-title]).height

            // Total height of the row = title height + top inset + bottom inset
            let row-h = title-h + 2 * title-inset

            // set text(1.6em)
            grid(
              columns: (auto, 1fr),
              align: top + left,
              inset: (0pt, title-inset),
              box(fill: colors.primary, width: num-w, height: row-h, radius: (bottom: 0.4em))[
                #set align(center + horizon)
                #text(fill: white)[#num-str]
              ],
              chapter-title
            )
          })
        ]
      ]
      v(3em)
    } else {
      box(stroke: (top: 1.5pt + states.colors.get().primary))[
        #context layout(area => {
          let bar-w = 1em  // width of the decorative bar (1em content + 2x0.5em inset)
          let title-inset = 0.5em

          // Height of the title rendered at the effective width of the title column (total width - bar width)
          let title-h = measure(block(width: area.width - bar-w)[#chapter-title]).height

          // Total height of the row = title height + top inset + bottom inset
          let row-h = title-h + 2 * title-inset

          grid(
            columns: (auto, 1fr),
            align: (center, left),
            inset: (0pt, title-inset),
            box(
              fill: colors.primary,
              width: bar-w,
              height: row-h,
              radius: (bottom: 0.4em)
            ),
            chapter-title
          )
        })
      ]
      v(1em)
    }
  }

  show heading.where(level: 2): it => {
    block(above: 1.5em)[
      #if it.numbering != none {
        text(counter(heading).display(), fill: colors.primary) + sym.space
      }
      #text(it.body)
      #v(-0.75em)
      #line(stroke: 0.75pt + colors.primary, length: 100%)
      #v(0.75em)
    ]
  }

  show heading.where(level: 3): it => {
    block[
      #if it.numbering != none {
        text(counter(heading).display(), fill: colors.primary) + sym.space
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
  set footnote.entry(separator: none)
  show footnote.entry: it => {
    box(width: 100%, stroke: (top: 0.5pt + colors.primary, left: 0.5pt + colors.primary), inset: 0.5em, radius: (top-left: 0.5em))[#it]
  }

  // References
  show ref: set text(fill: colors.primary)

  // Tables
  show table.cell.where(y: 0): set text(weight: "bold", fill: white)
  set table(
    fill: (_, y) => if y == 0 {colors.primary} ,
    stroke: (_, y) => if y == 0 {(bottom: 0pt)} else {(bottom: 01pt + colors.secondary)}
  )
  show table: it => block(
    stroke: 01pt + colors.primary,
    radius: 1em,
    clip: true
  )[#it]

  // Outline
  set outline.entry(fill: none)
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
    let h1-on-page = query(heading.where(level: 1)).filter(h => h.location().page() == here().page())
    if h1-on-page.len() > 0 { return }

    show linebreak: none

    show: show-if(states.tufte.get(), it => {
      show: wideblock.with(side: "both")
      it
    })

    set text(style: "italic", fill: white)
    if calc.odd(here().page()) {
      align(right)[
        #hydra(2, display: (_, it) => [
          #let head = none
          #if it.numbering != none {
            head = numbering(it.numbering, ..counter(heading).at(it.location())) + " " + it.body
          } else {
            head = it.body
          }
          #box(fill: states.colors.get().primary, inset: 0.5em, radius: (top-left: 2em, bottom-right: 2em))[#head]
        ])
      ]
    } else {
      align(left)[
        #hydra(1, display: (_, it) => [
          #let head = none
          #if it.numbering != none {
            head = numbering(it.numbering, ..counter(heading).at(it.location())) + " " + it.body
          } else {
            head = it.body
          }
          #box(fill: states.colors.get().primary, inset: 0.5em, radius: (bottom-left: 2em, top-right: 2em))[#head]
        ])
      ]
    }
  }

  let page-footer = context {
    let cp = counter(page).get().first()
    let current-page = counter(page).display()
    let dx = 0%
    if states.tufte.get() {
      dx = 15.04%
      if states.alt-margins.get() and calc.odd(here().page()) {
        dx = page.margin.outside + page.margin.inside/1.18
      }
    }
    set align(center)
    show: show-if(states.tufte.get(), it => {
      show: wideblock.with(side: "both")
      it
    })

    grid(
      columns: (1fr, auto, 1fr),
      align: center + horizon,
      [#line(length: 100%, stroke: 0.75pt + states.colors.get().primary)],
      [#box(stroke: 1pt + states.colors.get().primary, inset: 0.3em, radius: 0.25em)[#current-page]],
      [#line(length: 100%, stroke: 0.75pt + states.colors.get().primary)]
    )
    // ]
  }

  set page(
    paper: paper-size,
    header: page-header,
    footer: page-footer
  )

  it
}

// Boxes - Definitions
#let custom-box-pretty(title: none, icon: "info", color: rgb(29, 144, 208), body) = {
  showybox(
    title: box-title(color-svg("resources/images/icons/" + icon + ".svg", color, width: 1em), [*#title*]),
    title-style: (
      color: color,
      sep-thickness: 0pt,
    ),
    frame: (
      title-color: color.lighten(85%),
      border-color: color,
      body-color: none,
      thickness: 0.75pt,
      radius: (top-left: 2em, bottom-right: 2em, rest: 0em),
    ),
    breakable: true
  )[#body]
}

// Part
#let part-pretty(title) = context {
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

  show: show-if(states.tufte.get(), it => {
    show: wideblock.with(side: "both")
    it
  })

  let width = if states.tufte.get() {77.9%} else {90%}

  stack(
    dir: ttb,
    box(fill: states.colors.get().primary, inset: 1em, radius: (top: 2em))[
      #set text(size: 4.5em, fill: white, weight: "bold")
      #states.localization.get().part #states.counter-part.display(states.part-numbering.get())
    ],
    box(width: width, inset: 5em, stroke: 2pt + states.colors.get().primary, radius: 2em)[
    #set text(size: 3em)

    *#title*
  ])

  show heading: none
  heading(numbering: none)[
    #v(1em)
    #box(width: 95%, stroke: (top: 0.5pt + states.colors.get().primary, left: 0.5pt + states.colors.get().primary), inset: 00.5em, radius: (top-left: 0.5em))[#text(fill:states.colors.get().primary)[#states.localization.get().part #states.counter-part.display(states.part-numbering.get()) -- #title]]
  ]

  if states.open-right.get() {
    pagebreak(weak: true, to:"odd")
  }
}

// Minitoc
#let minitoc-pretty = context {
  let toc-header = states.localization.get().toc

  let miniline = line(stroke: 1pt + states.colors.get().primary, length: 100%)

  let header = block[
    #set text(fill: white)
    #box(fill: states.colors.get().primary, inset: 0.5em, radius: (top: 0.25em))[*#toc-header*]
  ]

  let body = block[
    #miniline
    #v(0.5em)
    #suboutline(target: heading.where(outlined: true, level: 2))
    #miniline
  ]

  v(3.5em)
  stack(
    dir: ttb,
    header,
    body
  )
}

#let boxeq-pretty(body) = context _boxeq(stroke: 1pt + states.colors.get().primary, fill: states.colors.get().boxeq.lighten(35%), radius: (top-left: 2em, bottom-right: 2em, rest: 0em), body)

#let pretty = (theme: pretty-theme, part: part-pretty, minitoc: minitoc-pretty, box: custom-box-pretty, boxeq: boxeq-pretty)
