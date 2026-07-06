#import "@preview/bookly:4.0.1": *
// #import "../../src/bookly.typ": *

// #show: chapter.with(
//   title: "Algorithms",
//   toc: false
// )

= Algorithms

#lorem(100)

Figure @fig:A is a beautiful typst logo.

#figure(
image("../images/typst-logo.svg", width: 75%),
caption: [#lorem(10)],
) <fig:A>

#figure(
table(
  columns: 3,
  table.header(
    [Substance],
    [Subcritical °C],
    [Supercritical °C],
  ),
  [Hydrochloric Acid],
  [12.0], [92.1],
  [Sodium Myreth Sulfate],
  [16.6], [104],
  [Potassium Hydroxide],
  table.cell(colspan: 2)[24.7],
), caption: [#lorem(2)]
)

== Test

#lorem(100)
