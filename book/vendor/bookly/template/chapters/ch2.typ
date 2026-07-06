#import "@preview/bookly:4.0.1": *
// #import "../../src/bookly.typ": *

// #show: chapter.with(title: "Second chapter")

= Second chapter
#minitoc
#pagebreak()

== Goals
#lorem(100)

$
arrow(V)(M slash R_0) = lr((d arrow(O M))/(d t)|)_(R_0) + theta
$

Figure @b2 is an example of a figure with a caption @Jon22.

#subfigure(
figure(image("../images/typst-logo.svg"), caption: []),
figure(image("../images/typst-logo.svg"), caption: []), <b2>,
columns: (1fr, 1fr),
caption: [(a) Left image and (b) Right image],
label: <fig:subfig2>,
)
