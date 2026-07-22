#import "@preview/hydra:0.6.2": hydra
#import "@preview/marginalia:0.3.1" as marginalia: wideblock
#import "@preview/itemize:0.2.0" as el
#import "../bookly-helper.typ": *
#import "../bookly-defaults.typ": *

#let partial-outline = context {
  let cur-loc = here()

  // Find the next part phantom heading (level 1 with in-outline == true)
  let future-h1 = query(selector(heading.where(level: 1)).after(cur-loc, inclusive: false))
  let next-parts = future-h1.filter(h => states.in-outline.at(h.location()))

  // Build target: all outlined headings in this part, depth 2 max
  let target = if next-parts.len() > 0 {
    selector(heading.where(outlined: true))
      .after(cur-loc, inclusive: false)
      .before(next-parts.first().location(), inclusive: false)
  } else {
    selector(heading.where(outlined: true)).after(cur-loc, inclusive: false)
  }

  box(width: 52%, stroke: (left: 1.5pt + states.colors.get().primary), outset: 0.5em)[#outline(title: none, target: target, depth: 2)]
}

#let obook-theme(colors: default-colors, it) = {
  show heading.where(level: 1): it => context {
      if not states.open-right.get() {
        pagebreak(weak: true)
      }

      // Reset counters
      reset-counters

      // Heading style
      let chap-id = if it.numbering != none {counter(heading).display(states.num-heading.get()) + ". "} else {none}
      let content = grid(
        columns: (auto, 1fr),
        align: top + left,
        [#chap-id], [#it.body]
      )

      show: show-if(states.tufte.get(), it => {
        show: wideblock.with(side: "both")
        it
      })

      // Heading content
      set text(size: 1.25em)
      box(stroke: (right: none, rest:1.5pt + colors.primary), width: 116%, inset: 0.5em, radius: (top-right: 0pt, bottom-right: 0pt, rest: 0.5em))[#content]

      v(1em)
  }

  show heading.where(level: 2): it => {
    block(above: 1.5em)[
      #let text-counter = none
      #let dx = 0pt
      #if it.numbering != none {
        text-counter = text(counter(heading).display(), fill: colors.primary) + sym.space
        if not states.tufte.get() {
          dx = measure(text-counter + sym.space).width
        }
      }

      #move(dx: -dx)[
        #text-counter
        #it.body
      ]

      #v(0.5em)
    ]
  }

  show heading.where(level: 3): it => {
    block[
      #let text-counter = none
      #let dx = 0pt
      #if it.numbering != none {
        text-counter = text(counter(heading).display(), fill: colors.primary) + sym.space
        if not states.tufte.get() {
          dx = measure(text-counter + sym.space).width
        }
      }

      #move(dx: -dx)[
        #text-counter
        #it.body
      ]

      #v(0.5em)
    ]
  }

  // Lists
  show: el.default-enum-list
  set list(marker: [#text(fill:colors.primary, size: 1.1em)[#sym.bullet]])
  set enum(numbering: n => text(fill:colors.primary)[#n.])

  // References
  show ref: set text(fill: colors.primary)

  // Tables
  show table.cell.where(y: 0): set text(weight: "bold")
  set table(
    stroke: (_, y) => if y == 0 {(top: 1pt, bottom: 1pt)} else {none}
  )
  show table: it => block(
    stroke: (x: none, bottom: 1pt),
  )[#it]

  // Outline
  set outline.entry(fill: repeat([.], gap: 0.6em))
  show outline.entry: it => {
    show linebreak: none
    if it.element.func() == heading {
      let number = it.prefix()
      let is-part = states.in-outline.at(it.element.location())
      let item = none
      if it.level == 1 {
        block(above: 1.25em, below: 0em)
        v(0.5em)
        if is-part {
          text([*#it.element.body*], fill: colors.primary)
        } else {
          item = [#text([*#number #it.inner()*], fill: colors.primary)]
        }
      } else if it.level == 2{
        block(above: 1em, below: 0em)
        item = [#text([*#number #it.inner()*])]
      } else {
        block(above: 1em, below: 0em)
        item = [#text([#number]) #it.inner()]
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

  show outline.entry.where(level: 2): it => {
    show repeat: none
    it
  }

  // Page header
let page-header = context {
  // Pas de header sur les pages d'ouverture de chapitre
    let h1-on-page = query(heading.where(level: 1)).filter(h => h.location().page() == here().page())
    if h1-on-page.len() > 0 { return }

    show linebreak: none

    let length = 100%
    let dy = 12%
    if states.tufte.get() {
      dy = 40%
    }

    let current-page = counter(page).display()

    show: show-if(states.tufte.get(), it => {
      show: wideblock.with(side: "both")
      it
    })

    if calc.odd(here().page()) {
      let head = hydra(2, display: (_, it) => [
        #if it.numbering != none {
          numbering(it.numbering, ..counter(heading).at(it.location())) + " " + it.body
        } else {
          it.body
        }
      ])
      grid(
        columns: (1fr, 1fr),
        align: (left, right),
        [#head],
        [#current-page]
      )
      place(dx: 0%, dy: dy)[#line(length: length, stroke: 0.75pt)]
    } else {
      let head = hydra(1, display: (_, it) => [
        #if it.numbering != none {
          counter(heading.where(level:1)).display() + " " + it.body
        } else {
          it.body
        }
      ])

      grid(
        columns: (1fr, 1fr),
        align: (left, right),
        [#current-page],
        [*#head*]
      )
      place(dx: 0%, dy: dy)[#line(length: length, stroke: 0.75pt)]
    }

    v(0.5em)
  }

  let page-footer = context {
    let chapter-heading = query(heading.where(level: 1).before(here())).last()

    if chapter-heading.location().page() == here().page() {
      show: show-if(states.tufte.get(), it => {
        show: wideblock.with(side: "both")
        it
      })
      align(center)[#counter(page).display()]
    }
  }

  set page(
    header: page-header,
    footer: page-footer
  )
  it
}

// Part
#let part-obook(title) = context {
  states.counter-part.update(i => i + 1)
  set page(
    header: none,
    footer: none,
    numbering: none
  )

  if states.open-right.get() {
    pagebreak(weak: true, to:"odd")
  }

  let part-num = text(fill: states.colors.get().primary, size: 10em, weight: "bold")[#states.counter-part.display(states.part-numbering.get())]
  let part-title = text(size: 3em)[*#title*]

  align(top)[
    #show: show-if(states.tufte.get(), it => {
      show: wideblock.with(side: "both")
      it
    })
    #grid(
      columns: (1fr,)*2,
      align: (top + left, top + right),
      [#part-num],
      [#part-title]
    )
  ]

  show heading: none
  states.in-outline.update(true)
  heading(numbering: none)[
    #set text(size: 1.25em)
    #grid(
      columns: (auto, 1fr),
      align: center,
      column-gutter: 0.2em,
      [#box(fill: states.colors.get().primary.lighten(75%), inset: 0.5em)[#states.counter-part.display(states.part-numbering.get())]],
      [
        #set text(fill: white)
        #box(fill: states.colors.get().primary, inset: 0.5em, width: 1fr)[*#title*]
      ]
    )
  ]
  states.in-outline.update(false)

  align(bottom + right)[
    #show: show-if(states.tufte.get(), it => {
      show: wideblock.with(side: "both")
      it
    })
    #partial-outline
  ]

  if states.open-right.get() {
    pagebreak(weak: true, to:"odd")
  }
}

// Minitoc
#let minitoc-obook = context {
  let toc-header = states.localization.get().toc

  let miniline = line(stroke: 1pt + states.colors.get().primary, length: 100%)

  let header = align(right)[
    #set text(fill: states.colors.get().primary)
    #box(fill: states.colors.get().primary.lighten(85%),stroke: states.colors.get().primary, inset: 0.5em, radius: (top: 0.25em))[*#toc-header*]
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

#let custom-box-obook(title: none, icon: "info", color: rgb(29, 144, 208), body) = context {
  // let box-tcontent = box(fill: color.lighten(85%), inset: 0.5em, radius: (top: 0.5em), stroke: (bottom: none, rest: 1pt + color))[
  //    #box-title(color-svg("resources/images/icons/" + icon + ".svg", color, width: 1em), text(fill: color)[*#title*])]

  // let box-tw = measure(box-tcontent).width

  // let box-title = align(right)[
  //   #stack(
  //     dir: ttb,
  //      box-tcontent,
  //      move(dx: -0.5pt, line(length: box-tw - 1pt, stroke: 2pt + color.lighten(85%)))
  //   )
  // ]

  let box-title = move(dy: -0.5em)[#box-title(color-svg("resources/images/icons/" + icon + ".svg", color, width: 1em), text(fill: color)[*#title*])]

  let box-content = block(breakable: true, box(fill: color.lighten(85%), stroke: 1pt + color, width: 100%, inset: (top: 1em, bottom: 1em, rest: 0.5em), radius: 0.5em)[#body])

  stack(
    dir: btt,
    box-content,
    box-title,
  )
}

#let boxeq-obook(body) = context _boxeq(stroke: 1pt + states.colors.get().primary, fill: states.colors.get().boxeq.lighten(30%), radius: 5pt, body)

#let obook = (theme: obook-theme, part: part-obook, minitoc: minitoc-obook, box: custom-box-obook, boxeq: boxeq-obook)
