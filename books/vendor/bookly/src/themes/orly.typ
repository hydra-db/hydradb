#import "@preview/showybox:2.0.4": *
#import "@preview/hydra:0.6.2": hydra, anchor
#import "@preview/marginalia:0.3.1" as marginalia: wideblock
#import "../bookly-helper.typ": *
#import "../bookly-defaults.typ": *

#let orly-theme(colors: default-colors, it) = {
    show heading.where(level:1): it => {
    if not states.open-right.get() {
      pagebreak(weak: true)
    }

    // Reset counters
    reset-counters

    // Heading style
    let type-chapter = if states.isappendix.get() {states.localization.get().appendix} else {states.localization.get().chapter}

    set align(right)

    show: show-if(states.tufte.get(), it => {
      show: wideblock.with(side: "both")
      it
    })

    if it.numbering != none {
      v(1em)
      [
        #text(size: 1em)[#upper[#type-chapter] #counter(heading).display(states.num-heading.get())]
        #v(-0.75em)
        #line(length: 100%, stroke: 0.5pt)
        #v(-0.1em)
        #text(2em)[#it.body]
      ]
    } else {
      [
        #line(length: 100%, stroke: 0.5pt)
        #v(-0.1em)
        #text(2em)[#it.body]
      ]
    }
    v(5em)
  }

  // Tables
  show table.cell.where(y: 0): set text(weight: "bold", fill: white)
  set table(
    fill: (_, y) => if y == 0 {black} ,
    stroke: (_, y) => (
      top: 0pt,
      bottom: 0.75pt
    ),
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
        v(1em)
        item = [*#number #it.inner()*]
      } else if it.level == 2 {
        block(above: 1em, below: 0em)
        item = [#h(1em) #number #it.inner()]
      } else {
        block(above: 1em, below: 0em)
        item = [#h(2em) #number #it.inner()]
      }
      link(it.element.location(), item)
    } else if it.element.func() == figure {
      block(above: 1.25em, below: 0em)
      link(it.element.location(), [#it.prefix(). #h(0.2em) #it.inner()])
    } else {
      it
    }
  }

  // Page style
  let page-footer = context {
    let cp = counter(page).get().first()
    let current-page = counter(page).display()
    let chapter-heading = query(heading.where(level: 1).before(here())).last()
    let hcontent = none

    show: show-if(states.tufte.get(), it => {
      show: wideblock.with(side: "both")
      it
    })

    set text(0.85em, weight: "bold")
    line(length: 100%, stroke: 0.5pt)
    v(-0.5em)

    let inset = 0.5em
    if calc.odd(cp) {
      let curr-box = box(stroke: (left: 0.5pt), inset: inset)[#current-page]
      let hcontent = if chapter-heading.location().page() != here().page() {box(inset: inset, hydra(2))}
      align(right)[#move(dy: -0.7em)[#hcontent #curr-box]]
    } else {
      let curr-box = box(stroke: (right: 0.5pt), inset: inset)[#current-page]
      let hcontent = if chapter-heading.location().page() != here().page() {box(inset: inset, hydra(1))}
      align(left)[#move(dy: -0.7em)[#curr-box #hcontent]]
    }
  }

  set page(
    header: anchor(),
    footer: page-footer,
  )

  it
}

// Boxes - Definitions
#let custom-box-orly(title: none, icon: "info", color: rgb(29, 144, 208), body) = {
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
      thickness: (left: 1.25pt),
      radius: 0pt,
    ),
    breakable: true
  )[#body]
}

// Part
#let part-orly(title) = context {
  states.counter-part.update(i => i + 1)
  set page(
    header: none,
    footer: none,
    numbering: none
  )

  set align(top + right)

  if states.open-right.get() {
    pagebreak(weak: true, to:"odd")
  }

  let part = [
    #text(size: 1.75em)[*#upper[#states.localization.get().part] #states.counter-part.display(states.part-numbering.get())*]
    #v(-0.75em)
    #line(length: 100%, stroke: 0.5pt)
    #v(-2.5em)
    #text(size: 3em)[*#title*]
  ]

  if states.tufte.get() {wideblock(side: "both")[#part]} else {part}

  show heading: none
  heading(numbering: none)[
    #set text(1.15em)
    #v(1em)
    #line(length: 100%, stroke: 0.5pt)
    #v(-0.5em)
    #box[*#upper[#states.localization.get().part] #states.counter-part.display(states.part-numbering.get()) -- #title*]
  ]

  if states.open-right.get() {
    pagebreak(weak: true, to:"odd")
  }
}

#let minitoc-orly = context {
  let miniline = line(stroke: 0.5pt, length: 100%)
  let toc-header = states.localization.get().toc

  block(above: 3.5em)[
    #set align(right)
    #miniline
    #v(-0.5em)
    #text([*#toc-header*])
    #v(0.5em)
  ]

  // miniline
  v(0.5em)
  suboutline(target: heading.where(outlined: true, level: 2))
  miniline
}

#let boxeq-orly(body) = context _boxeq(stroke: 1pt, radius: 5pt, body)

#let orly = (theme: orly-theme, part: part-orly, minitoc: minitoc-orly, box: custom-box-orly, boxeq: boxeq-orly)
