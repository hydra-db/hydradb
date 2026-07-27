
#import "@preview/subpar:0.2.2"
#import "@preview/suboutline:0.3.0": *
#import "bookly-defaults.typ": *

// Reset counters
#let reset-counters = context {
  counter(math.equation).update(0)
  counter(figure.where(kind: image)).update(0)
  counter(figure.where(kind: table)).update(0)
  if states.tufte.get(){
    states.sidenotecounter.update(0)
  }
  counter(footnote).update(0)
}

// Conditional set-show
#let show-if(cond, func) = body => if cond { func(body) } else { body }

// Headings
#let headings-on-odd-page(it) = {
  show heading.where(level: 1): it => {
    {
      set page(header: none, footer: none)
      pagebreak(to: "odd")
    }
    it
  }
  it
}

// Equations
#let _boxeq(stroke: none, fill: none, radius: 0pt, inset: 0.6em, body) = context {
  set align(center)
  box(
    stroke: stroke,
    fill: fill,
    radius: radius,
    inset: inset
   )[#body]
}

// Subfigure
#let subfigure = subpar.grid.with(
  gap: 1em,
  numbering: n => {numbering(states.num-pattern-fig.get(), counter(heading).get().first() , n)},
  numbering-sub-ref: (m, n) => {numbering(states.num-pattern-subfig.get(), counter(heading).get().first(), m, n)},
  supplement: fig-supplement,
  show-sub: it => {set figure.caption(position: bottom); it}
)

// Long and short captions for figures or tables
#let ls-caption(long, short) = context if states.in-outline.get() { short } else { long }

// Book title page
#let book-title-page(
  subtitle: "Book subtitle",
  edition: "First edition",
  institution: "Institution",
  series: "Discipline",
  year: datetime.today().year(),
  cover: none,
  logo: none,
  version-usage: none
) = context {
  let header = {
    box(fill: states.colors.get().primary, width: 100%, inset: 1em)[
      #set align(center + horizon)
      #text(fill: white, size: 1.5em)[#strong(delta: 400)[#series]]
    ]
  }

  let footer = {
    box(fill: states.colors.get().primary, width: 100%, inset: 1em)[
      #set align(center + horizon)
      #text(fill: white, size: 1.5em)[#strong(delta: 400)[#institution]]
    ]
  }

  set page(
    paper: paper-size,
    header: header,
    footer: footer,
    margin: (left: 0em, right:0em, top: 4em, bottom: 4em)
  )

  let title-page = context {

    align(horizon)[
      #move(dx: 2em)[
        #line(stroke: 1.5pt + states.colors.get().primary, length: 90%)
        #v(1em)
      ]

      #move(dx: 4em)[
        #text(size: 3em)[*#states.title.get()*]
        #linebreak()

        #if subtitle != none {
          v(0.5em)
          text(size: 1.75em)[#subtitle]
          linebreak()
          v(0.5em)
        }

        #if edition != none {
          v(0.5em)
          text(size: 1.25em)[_ #edition _]
          linebreak()
          v(0.5em)
        }

        #v(0.5em)
        #text(size: 1.5em)[#states.author.get()]
      ]

      #move(dx: 2em)[
        #v(1em)
        #line(stroke: 1.5pt + states.colors.get().primary, length: 90%)
      ]

      #if cover != none {
        v(1.5em)
        align(center)[#cover]
      }
    ]

    set page(
      paper: paper-size,
      header: none,
      footer: none,
      margin: auto
    )

    if states.open-right.get() {
      pagebreak(to: "odd")
    }

    align(center + horizon)[
      #text(size: 3em)[*#states.title.get()*]

      #if subtitle != none {
          v(-1.5em)
          text(size: 1.75em)[#subtitle]
          linebreak()
          v(0.5em)
        }

        #if edition != none {
          v(0.5em)
          text(size: 1.25em)[_ #edition _]
          linebreak()
          v(0.5em)
        }
    ]

    let version-info = if version-usage != none {text(size: 0.85em)[#version-usage \ #sym.copyright #states.author.get(), #year.]} else {text(size: 0.85em)[#states.localization.get().version-usage \ #sym.copyright #states.author.get(), #year.]}

    let height-ver = measure(version-info).height

    if logo != none {
      set image(width: 35%)
      place(bottom + center, dy: -(height-ver + 4em), logo)
    }

    place(bottom)[
      #version-info
    ]
  }

  title-page
}

// Thesis title page
#let thesis-title-page(
  type: "phd",
  school: "School name",
  doctoral-school: "Name of the doctoral school",
  supervisor: ("Supervisor name",),
  cosupervisor: none,
  laboratory: "Laboratory name",
  defense-date: "01 January 1970",
  discipline: "Discipline",
  specialty: "Specialty",
  committee: (:),
  logo: none
) = context {
  set page(
      paper: paper-size,
      header: none,
      footer: none,
      margin: auto
    )

  place(top + left, dx: -16%, dy: -10%,
      rect(fill: states.colors.get().primary, height: 121%, width: 20%)
  )

  let title-page = {
    if logo != none {
      set image(width: 35%)
      place(top + right, dx: 0%, dy: -15%, logo)
    }
    text([#states.localization.get().doctoral-school #h(0.25em) #doctoral-school], size: 1.25em)
    v(0.25em)
    text(school, size: 1.25em)

    v(0.25em)
    text(laboratory, size: 1.25em)
    v(2em)
    if type.contains("phd") {
      text([*#states.localization.get().phd*], size: 1.5em)
    } else {
      text([*#states.localization.get().habilitation*], size: 1.5em)
    }
    v(0.25em)
    text([_ #states.localization.get().authored _ *#states.author.get()*], size: 1.15em)
    v(0.25em)
    text([_ #states.localization.get().defended _ *#defense-date*], size: 1.15em)
    v(0.25em)
    text([_ #states.localization.get().discipline _ *#discipline*], size: 1.1em)
    v(0.15em)
    text([_ #states.localization.get().specialty _ *#specialty*], size: 1.1em)
    v(2em)
    line(stroke: 1.75pt + states.colors.get().primary, length: 104%)
    align(center)[#text(strong(states.title.get()), size: 2em)]
    line(stroke: 1.75pt + states.colors.get().primary, length: 104%)
    v(1em)

    if supervisor != none {
      let n = supervisor.len()
      let dir = none
      if n == 1 {
        if type.contains("phd") {
          dir = states.localization.get().supervisor
      } else {
        dir = states.localization.get().sponsor
      }
      } else {
        if type.contains("phd") {
          dir = states.localization.get().supervisors
        } else {
          dir = states.localization.get().sponsors
        }
      }

      let names-dir = ()
      for director in supervisor {
        names-dir.push(director)
      }

      text(1.15em)[#dir *#names-dir.join(",", last: states.localization.get().and)*]
      v(0.5em)
    }

    if cosupervisor != none {
      let m = cosupervisor.len()
      let codir = none
      if m == 1 {
        if type.contains("phd") {
          codir = states.localization.get().cosupervisor
        } else {
          codir = states.localization.get().cosponsor
        }
      } else {
        if type.contains("phd") {
          codir = states.localization.get().cosupervisors
        } else {
          codir = states.localization.get().cosponsors
        }
      }

      let names-codir = ()
        for codirector in cosupervisor {
          names-codir.push(codirector)
        }

      text(1.15em)[#codir *#names-codir.join(",", last: states.localization.get().and)*]
      v(0.5em)
    }

    if committee.len() > 0 {
      v(1fr)
      align(center)[
        #text([*#states.localization.get().committee*])
        #v(0.5em)
        #set text(size: 0.9em)
        #grid(
          columns: 4,
          column-gutter: 1.5em,
          row-gutter: 1em,
          align: left,
          stroke: none,
          ..for (name, position, affiliation, role) in committee {
            ([*#name*], position, affiliation, role)
          },
        )
      ]

      v(1fr)
    }
  }

  place(dx: 8%, dy: 11%,
    block(
      height: 100%,
      width: 100%,
      breakable: false,
      title-page
    )
  )
}

// Back cover
#let back-cover(abstracts: (), logo: none) = context{
  set page(margin: auto, header: none, footer: none)

  if states.open-right.get() {
    pagebreak(to: "even", weak: true)
  }

  set align(horizon)

  if logo != none {
    grid(columns : logo.len(),
      column-gutter: 1fr,
      ..logo.map((logos) => logos)
    )
  }

  context{
    v(2em)
    align(center)[
      #text([*#states.author.get()*], size: 1.5em, fill: states.colors.get().primary)

      #text([*#states.title.get()*], size: 1.25em)

      #v(1em)
    ]
  }

  for abstract in abstracts {
    context{
      block(
        width: 100%,
        stroke: 1pt + states.colors.get().primary,
        inset: 1em,
        radius: 0.5em,
        below: 2em
      )[
        #text([*#abstract.title * #abstract.text], size: 0.9em)
      ]
    }
  }
}

// Boxes - Utility
#let box-title(a, b) = {
  grid(columns: 2, column-gutter: 0.5em, align: (horizon),
    a,
    b
  )
}

#let colorize(svg, color) = {
  let blk = black.to-hex();
  if svg.contains(blk) {
    svg.replace(blk, color.to-hex())
  } else {
    svg.replace("<svg ", "<svg fill=\""+ color.to-hex() + "\" ")
  }
}

#let color-svg(
  path,
  color,
  ..args,
) = {
  let data = colorize(read(path), color)
  return image(bytes(data), ..args)
}
