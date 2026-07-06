#import "@preview/bookly:4.0.1": *
// #import "../../src/bookly.typ": *

// #show: chapter.with(
//   title: "Foundations",
//   toc: false
// )

= Foundations

#lorem(100)

$
  #boxeq($bold(y)_(k + 1) = bold(C) space.thin bold(x)_(k + 1)$)
$

$
y(x) = f(x)
$<nonum-eq>

Figure @fig:B is an example of a figure with a caption.

#figure(
image("../images/typst-logo.svg", width: 75%),
caption: [#lorem(10)],
) <fig:B>

Figure @b3 is an example of a subfigure.

#subfigure(
figure(image("../images/typst-logo.svg"), caption: []),
figure(image("../images/typst-logo.svg"), caption: []), <b3>,
columns: (1fr, 1fr),
caption: [(a) Left image and (b) Right image],
label: <fig:subfig3>,
)
