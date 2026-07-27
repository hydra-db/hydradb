#import "@preview/hydra:0.6.2": hydra
#import "@preview/marginalia:0.3.1" as marginalia: wideblock
#import "@preview/itemize:0.2.0" as el
#import "../bookly-helper.typ": *
#import "../bookly-defaults.typ": *

#let reader-mode = sys.inputs.at("mode", default: "light")

// PostHog-inspired reader palettes. The light palette follows DESIGN.md
// directly; sepia is a warmer long-reading variant; dark inverts the same
// cream/olive/yellow system around PostHog's surface-dark color.
#let reader-palettes = (
  light: (
    paper: rgb("#eeefe9"),
    ink: rgb("#23251d"),
    text: rgb("#4d4f46"),
    charcoal: rgb("#33342d"),
    muted: rgb("#6c6e63"),
    ash: rgb("#9b9c92"),
    primary: rgb("#f7a501"),
    primary_bg: rgb("#f7a501"),
    primary_pressed: rgb("#dd9001"),
    primary_active: rgb("#b17816"),
    on_primary: rgb("#23251d"),
    surface: rgb("#fcfcfa"),
    surface_card: white,
    surface_soft: rgb("#e5e7e0"),
    surface_dark: rgb("#23251d"),
    on_dark: white,
    header: rgb("#6c6e63"),
    border: rgb("#bfc1b7"),
    border_soft: rgb("#dcdfd2"),
    table_alt: rgb("#fcfcfa"),
    code_bg: rgb("#23251d"),
    code_text: white,
    inline_code_bg: rgb("#e5e7e0"),
    boxeq: rgb("#fcfcfa"),
    link: rgb("#1078a3"),
    link_blue: rgb("#1d4ed8"),
    info: rgb("#2c84e0"),
    info_soft: rgb("#dceaf6"),
    ok: rgb("#2c8c66"),
    ok_soft: rgb("#d9eddf"),
    bad: rgb("#cd4239"),
    bad_soft: rgb("#f7d6d3"),
    warn: rgb("#b17816"),
    warn_soft: rgb("#f3dfb7"),
    purple: rgb("#7c44a6"),
    purple_soft: rgb("#e7d8ee"),
    secondary: rgb("#e5e7e0"),
  ),
  sepia: (
    paper: rgb("#f4ecd8"),
    ink: rgb("#2f2a22"),
    text: rgb("#51483a"),
    charcoal: rgb("#3d3427"),
    muted: rgb("#746852"),
    ash: rgb("#a79a82"),
    primary: rgb("#e3a21a"),
    primary_bg: rgb("#e3a21a"),
    primary_pressed: rgb("#c88912"),
    primary_active: rgb("#9b6b1f"),
    on_primary: rgb("#2f2a22"),
    surface: rgb("#fbf5e8"),
    surface_card: rgb("#fffaf0"),
    surface_soft: rgb("#e8ddc6"),
    surface_dark: rgb("#2f2a22"),
    on_dark: rgb("#fff8e7"),
    header: rgb("#746852"),
    border: rgb("#cdbf9f"),
    border_soft: rgb("#ded1b4"),
    table_alt: rgb("#fbf5e8"),
    code_bg: rgb("#2f2a22"),
    code_text: rgb("#fff8e7"),
    inline_code_bg: rgb("#e8ddc6"),
    boxeq: rgb("#fbf5e8"),
    link: rgb("#0f6f87"),
    link_blue: rgb("#255caa"),
    info: rgb("#317da8"),
    info_soft: rgb("#d8e6ed"),
    ok: rgb("#3f805f"),
    ok_soft: rgb("#dce9d4"),
    bad: rgb("#a94b43"),
    bad_soft: rgb("#efd4cf"),
    warn: rgb("#9b6b1f"),
    warn_soft: rgb("#ead8ad"),
    purple: rgb("#7b5294"),
    purple_soft: rgb("#e5d8e6"),
    secondary: rgb("#e8ddc6"),
  ),
  dark: (
    paper: rgb("#23251d"),
    ink: rgb("#f4f0df"),
    text: rgb("#dcdfd2"),
    charcoal: rgb("#eeefe9"),
    muted: rgb("#b6b7af"),
    ash: rgb("#8e9087"),
    primary: rgb("#f7a501"),
    primary_bg: rgb("#f7a501"),
    primary_pressed: rgb("#dd9001"),
    primary_active: rgb("#f0c066"),
    on_primary: rgb("#23251d"),
    surface: rgb("#2b2d24"),
    surface_card: rgb("#2c2f26"),
    surface_soft: rgb("#33362c"),
    surface_dark: rgb("#171811"),
    on_dark: white,
    header: rgb("#bfc1b7"),
    border: rgb("#4b4e43"),
    border_soft: rgb("#3a3d33"),
    table_alt: rgb("#2b2e25"),
    code_bg: rgb("#171811"),
    code_text: rgb("#f7f7ef"),
    inline_code_bg: rgb("#33362c"),
    boxeq: rgb("#2b2d24"),
    link: rgb("#7cc7dd"),
    link_blue: rgb("#90b8ff"),
    info: rgb("#7ab8f0"),
    info_soft: rgb("#243647"),
    ok: rgb("#75c596"),
    ok_soft: rgb("#24382b"),
    bad: rgb("#f08b84"),
    bad_soft: rgb("#472a29"),
    warn: rgb("#f0c066"),
    warn_soft: rgb("#44351f"),
    purple: rgb("#d2a3ea"),
    purple_soft: rgb("#392b42"),
    secondary: rgb("#33362c"),
  ),
)

#let reader-colors = reader-palettes.at(reader-mode, default: reader-palettes.light)

#let ok(body) = text(fill: reader-colors.ok, body)
#let bad(body) = text(fill: reader-colors.bad, body)
#let warn(body) = text(fill: reader-colors.warn, body)

#let codeblock(body) = block(
  fill: reader-colors.code_bg,
  stroke: 0.5pt + reader-colors.border,
  inset: (x: 12pt, y: 10pt),
  radius: 6pt,
  width: 100%,
)[
  #set raw(theme: none)
  #text(font: "Source Code Pro", size: 9pt, fill: reader-colors.code_text, body)
]

#let _accent-for(c, icon, fallback) = {
  if icon == "tip" {
    c.ok
  } else if icon == "alert" {
    c.bad
  } else if icon == "stop" {
    c.bad
  } else if icon == "question" {
    c.purple
  } else if icon == "report" {
    c.purple
  } else if icon == "info" {
    c.info
  } else {
    fallback
  }
}

#let _soft-for(c, icon) = {
  if icon == "tip" {
    c.ok_soft
  } else if icon == "alert" {
    c.bad_soft
  } else if icon == "stop" {
    c.bad_soft
  } else if icon == "question" {
    c.purple_soft
  } else if icon == "report" {
    c.purple_soft
  } else if icon == "info" {
    c.info_soft
  } else {
    c.surface
  }
}

#let reader-theme-fn(colors: default-colors, it) = {
  let c = reader-colors + colors

  set page(fill: c.paper)
  set text(fill: c.text)

  // Inline markup colors.
  show link: set text(fill: c.link)
  show ref: set text(fill: c.link)
  show raw.where(block: false): it => box(
    fill: c.inline_code_bg,
    inset: (x: 3pt, y: 1pt),
    outset: (y: 1pt),
    radius: 2pt,
  )[
    #text(font: "Source Code Pro", size: 0.9em, fill: c.ink)[#it]
  ]
  // Block code renders as a dark terminal card. The bare `set text(fill:
  // code_text)` this replaced put white text on the cream page — invisible.
  show raw.where(block: true): it => block(
    fill: c.code_bg,
    stroke: 0.5pt + c.border,
    inset: (x: 12pt, y: 10pt),
    radius: 6pt,
    width: 100%,
    breakable: true,
  )[
    #set text(font: "Source Code Pro", size: 9pt, fill: c.code_text)
    #it
  ]

  // Figure captions must read as captions, not body prose: smaller, muted,
  // italic body, with a bold accent-colored "Figure N.N --" label.
  show figure.caption: it => block(width: 100%, inset: (x: 4pt))[
    #set text(size: 0.85em, fill: c.muted)
    #set par(leading: 0.5em, justify: true)
    #text(weight: 700, fill: c.primary_active, style: "normal")[#it.supplement~#context it.counter.display(it.numbering)#it.separator]#emph(it.body)
  ]

  // Headings: flat PostHog-style canvas, hairlines, and one restrained
  // yellow pill instead of decorative gradients.
  show heading.where(level: 1): it => context {
    if not states.open-right.get() {
      pagebreak(weak: true)
    }

    reset-counters

    let type-chapter = if states.isappendix.get() {states.localization.get().appendix} else {states.localization.get().chapter}

    if it.numbering != none {
      v(4.5em)
      block(width: 100%)[
        #box(
          fill: c.primary_bg,
          inset: (x: 9pt, y: 4pt),
          radius: 999pt,
        )[
          #text(size: 0.8em, weight: 700, fill: c.on_primary)[#type-chapter #counter(heading).display(states.num-heading.get())]
        ]
        #v(1.1em)
        #text(size: 2.35em, weight: 800, fill: c.ink)[#it.body]
        #v(0.9em)
        #line(stroke: 0.75pt + c.border, length: 100%)
      ]
      v(3em)
    } else {
      v(2em)
      block(width: 100%)[
        #text(size: 2.1em, weight: 800, fill: c.ink)[#it.body]
        #v(0.75em)
        #line(stroke: 0.75pt + c.border, length: 100%)
      ]
      v(2em)
    }
  }

  show heading.where(level: 2): it => {
    block(above: 1.7em, below: 0.9em)[
      #if it.numbering != none {
        text(counter(heading).display(), weight: 700, fill: c.muted)
        h(0.35em)
      }
      #text(size: 1.25em, weight: 700, fill: c.ink, it.body)
      #v(0.35em)
      #line(stroke: 0.5pt + c.border_soft, length: 100%)
    ]
  }

  show heading.where(level: 3): it => {
    block(above: 1.15em, below: 0.65em)[
      #if it.numbering != none {
        text(counter(heading).display(), weight: 700, fill: c.muted)
        h(0.35em)
      }
      #text(size: 1.05em, weight: 700, fill: c.ink, it.body)
    ]
  }

  // Tables: flat cards with olive-charcoal headers and soft hairlines.
  show table.cell.where(y: 0): set text(weight: 700, fill: c.on_dark)
  set table(
    fill: (_, y) => if y == 0 {c.surface_dark} else if calc.odd(y) {c.table_alt} else {none},
    stroke: 0.35pt + c.border,
  )

  // Lists
  show: el.default-enum-list
  set list(marker: [#text(fill: c.primary_active, size: 1.1em)[#sym.bullet]])
  set enum(numbering: n => text(fill: c.primary_active, weight: 700)[#n.])

  // Footnotes
  set footnote.entry(separator: line(length: 30% + 0pt, stroke: 0.75pt + c.border))

  // Outline
  set outline.entry(fill: box(width: 1fr, repeat(gap: 0.25em)[#text(fill: c.ash)[.]]))
  show outline.entry: it => {
    show linebreak: none
    if it.element.func() == heading {
      let number = it.prefix()
      let item = none
      if it.level == 1 {
        block(above: 1.25em, below: 0em)
        v(0.5em)
        item = [#text([*#number*], fill: c.primary_active) *#it.inner()*]
      } else if it.level == 2{
        block(above: 1em, below: 0em)
        item = [#h(1em) #text([#number], fill: c.muted) #it.inner()]
      } else {
        block(above: 1em, below: 0em)
        item = [#h(2em) #text([#number], fill: c.muted) #it.inner()]
      }
      link(it.element.location(), item)
    } else if it.element.func() == figure {
      block(above: 1.25em, below: 0em)
      v(0.25em)
      link(it.element.location(), [#text([#it.prefix().], fill: c.primary_active) #h(0.2em) #it.inner()])
    } else {
      it
    }
  }

  // Page style
  let page-header = context {
    show linebreak: none

    show: show-if(states.tufte.get(), it => {
      show: wideblock.with(side: "both")
      it
    })

    set text(style: "normal", size: 0.82em, fill: c.header)
    if calc.odd(here().page()) {
      align(right)[
        #hydra(2, display: (_, it) => [
          #let head = if it.numbering != none {
            numbering(it.numbering, ..counter(heading).at(it.location())) + " " + it.body
          } else {
            it.body
          }

          #grid(
            columns: (1fr, auto),
            column-gutter: 0.5em,
            align: horizon,
            [
              #line(length: 100% - 0.5em, stroke: 0.5pt + c.border)
            ],
            [
              #align(right)[#head]
            ]
          )
        ])
      ]
    } else {
      align(left)[
        #hydra(1, display: (_, it) => [
          #let head = if it.numbering != none {
            counter(heading.where(level:1)).display() + " " + it.body
          } else {
            it.body
          }

          #grid(
            columns: (auto, 1fr),
            column-gutter: 0.5em,
            align: horizon,
            [
              #align(left)[#head]
            ],
            [
              #line(length: 100% - 0.5em, stroke: 0.5pt + c.border)
            ]
          )
        ])
      ]
    }
  }

  let page-footer = context {
    let cp = counter(page).get().first()
    let current-page = counter(page).display()
    set text(fill: c.on_primary, weight: 700, size: 0.85em)
    v(1.5em)

    show: show-if(states.tufte.get(), it => {
      show: wideblock.with(side: "both")
      it
    })

    if calc.odd(cp) {
      set align(right)
      box(inset: (x: 7pt, y: 3pt), fill: c.primary_bg, radius: 999pt)[
        #current-page
      ]
    } else {
      set align(left)
      box(inset: (x: 7pt, y: 3pt), fill: c.primary_bg, radius: 999pt)[
        #current-page
      ]
    }
  }

  set page(
    paper: paper-size,
    fill: c.paper,
    header: page-header,
    footer: page-footer
  )

  it
}

// Boxes - Definitions
#let reader-box(title: none, icon: "info", color: rgb(29, 144, 208), body) = {
  let c = reader-colors
  let accent = _accent-for(c, icon, color)
  let soft = _soft-for(c, icon)

  block(above: 0.85em, below: 0.85em)[
    #box(
      stroke: (left: 2pt + accent, rest: 0.35pt + c.border_soft),
      fill: soft,
      inset: (x: 12pt, y: 10pt),
      radius: 6pt,
      width: 100%,
    )[
      #grid(
        columns: (auto, 1fr),
        column-gutter: 0.65em,
        align: top + left,
        [#color-svg("resources/images/icons/" + icon + ".svg", accent, width: 1.15em)],
        [
          #if title != none {
            text(fill: c.ink, weight: 700)[#title]
            h(0.4em)
          }
          #text(fill: c.text)[#body]
        ],
      )
    ]
  ]
}

// Part
#let reader-part(title) = context {
  states.counter-part.update(i => i + 1)
  let c = reader-colors + states.colors.get()

  set page(
    fill: c.paper,
    header: none,
    footer: none,
    numbering: none
  )

  set align(center + horizon)
  set text(fill: c.text)

  if states.open-right.get() {
    pagebreak(weak: true, to:"odd")
  }

  move[
    #box(
      fill: c.primary_bg,
      inset: (x: 12pt, y: 5pt),
      radius: 999pt,
    )[
      #text(size: 0.9em, weight: 800, fill: c.on_primary)[#states.localization.get().part #states.counter-part.display(states.part-numbering.get())]
    ]
    #v(1.5em)
    #text(size: 3em, weight: 800, fill: c.ink)[#title]
    #v(1em)
    #line(stroke: 1pt + c.border, length: 45%)
  ]

  show heading: none
  heading(numbering: none)[
    #v(1em)
    #box[#text(fill: c.primary_active)[#states.localization.get().part #states.counter-part.display(states.part-numbering.get()) -- #title]]
  ]

  if states.open-right.get() {
    pagebreak(weak: true, to:"odd")
  }
}

#let reader-minitoc = context {
  let c = reader-colors + states.colors.get()
  let toc-header = states.localization.get().toc
  block(above: 3.5em)[
    #text(fill: c.ink, weight: 700)[#toc-header]
    #v(-0.5em)
  ]

  let miniline = line(stroke: 0.75pt + c.border, length: 100%)

  miniline
  v(0.5em)
  suboutline(target: heading.where(outlined: true, level: 2))
  miniline
}

#let reader-boxeq(body) = context {
  let c = reader-colors + states.colors.get()
  _boxeq(stroke: 1pt + c.border, fill: c.boxeq, radius: 6pt, body)
}

#let reader-title-page(
  subtitle: "Book subtitle",
  edition: "First edition",
  institution: "Institution",
  series: "Discipline",
  year: datetime.today().year(),
  cover: none,
  logo: none,
  version-usage: none,
) = context {
  let c = reader-colors + states.colors.get()

  set page(
    paper: paper-size,
    fill: c.paper,
    header: none,
    footer: none,
    margin: (left: 4em, right: 4em, top: 4em, bottom: 4em),
  )
  set text(fill: c.text)

  align(horizon)[
    #box(
      fill: c.primary_bg,
      inset: (x: 12pt, y: 5pt),
      radius: 999pt,
    )[
      #text(fill: c.on_primary, size: 0.9em, weight: 800)[#series]
    ]

    #v(2em)
    #text(size: 3.1em, weight: 800, fill: c.ink)[#states.title.get()]

    #if subtitle != none {
      v(0.9em)
      text(size: 1.5em, fill: c.text)[#subtitle]
    }

    #if edition != none {
      v(0.7em)
      text(size: 1.05em, fill: c.muted)[_ #edition _]
    }

    #v(1em)
    #line(stroke: 1pt + c.border, length: 65%)
    #v(1em)
    #text(size: 1.25em, weight: 600, fill: c.charcoal)[#states.author.get()]
    #v(0.5em)
    #text(size: 1em, fill: c.muted)[#institution]

    #if cover != none {
      v(1.5em)
      align(center)[#cover]
    }
  ]

  set page(
    paper: paper-size,
    fill: c.paper,
    header: none,
    footer: none,
    margin: auto,
  )

  if states.open-right.get() {
    pagebreak(to: "odd")
  }

  align(center + horizon)[
    #text(size: 2.2em, weight: 800, fill: c.ink)[#states.title.get()]

    #if subtitle != none {
      v(0.8em)
      text(size: 1.15em, fill: c.text)[#subtitle]
    }
  ]

  let version-info = if version-usage != none {
    text(size: 0.85em, fill: c.muted)[#version-usage \ #sym.copyright #states.author.get(), #year.]
  } else {
    text(size: 0.85em, fill: c.muted)[#states.localization.get().version-usage \ #sym.copyright #states.author.get(), #year.]
  }

  let height-ver = measure(version-info).height

  if logo != none {
    set image(width: 35%)
    place(bottom + center, dy: -(height-ver + 4em), logo)
  }

  place(bottom)[
    #version-info
  ]
}

#let reader = (theme: reader-theme-fn, part: reader-part, minitoc: reader-minitoc, box: reader-box, boxeq: reader-boxeq)
