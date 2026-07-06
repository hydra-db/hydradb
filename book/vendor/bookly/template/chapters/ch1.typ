#import "@preview/bookly:4.0.1": *
// #import "../../src/bookly.typ": *

// #show: chapter.with(title: "First chapter", abstract: lorem(100), label: <ch:1>)

= First chapter

#lorem(100)
#minitoc
#pagebreak()

== Goals
#lorem(100)

Equations @eq:1 and @eq:2 are very important.
$
integral_0^1 f(x) dif x = F(1) - F(0) "et voilà"
$ <eq:1>

$
integral_0^1 f(x) dif x = F(1) - F(0) "et voilà"
$ <eq:2>

#lorem(20)
== Code

Figure @fig:1 is a beautiful typst logo.

#figure(
image("../images/typst-logo.svg", width: 75%),
caption: [#ls-caption([#lorem(10)], [#lorem(2)])],
) <fig:1>

Figure @fig:subfig the Typst logo. Figure @b is a Typst logo @Smi21.

#subfigure(
figure(image("../images/typst-logo.svg"), caption: []),
figure(image("../images/typst-logo.svg"), caption: []), <b>,
columns: (1fr, 1fr),
caption: [(a) Left image and (b) Right image],
label: <fig:subfig>,
)

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
  ), caption: [#lorem(4)]
)

== Boxes

#lorem(10)

=== Informations

#info-box[
  #lorem(10)
]

#tip-box[
  #lorem(10)
]

#warning-box[
  #lorem(10)
]

#important-box[
  #lorem(10)
]

#proof-box[
  #lorem(10)
]

#question-box[
  #lorem(10)
]

#lorem(1000)#footnote("This is a footnote")
