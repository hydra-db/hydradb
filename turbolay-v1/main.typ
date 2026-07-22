#import "../book/vendor/bookly/src/bookly.typ": *
#import "@preview/marginalia:0.3.1" as marginalia

#show: bookly.with(
  author: "HydraDB Team",
  title: "turbolay",
  lang: "en",
  theme: reader,
  colors: reader-colors,
  fonts: (
    body: "Avenir Next",
    math: "New Computer Modern Math",
    raw: "Source Code Pro",
  ),
  title-page: reader-title-page(
    subtitle: "An intuition-first guide to an object-store graph kernel",
    series: "HydraDB Technical Architecture Series",
    institution: "HydraDB",
    logo: none,
    cover: none,
  ),
  config-options: (
    open-right: false,
    part-numbering: "1",
  ),
)

#show: marginalia.setup.with(
  inner: (far: 1.6cm, width: 0cm, sep: 0cm),
  outer: (far: 1cm, width: 4.5cm, sep: 0.6cm),
  top: 1.6cm,
  bottom: 1.6cm,
)

#set page(width: 25cm, fill: reader-colors.paper)
#set text(size: 10pt, fill: reader-colors.text)

#show: front-matter
#show: main-matter

#tableofcontents
#listoffigures
#listoftables

#include "chapter-01.typ"
#include "chapter-02.typ"
#include "chapter-03.typ"
#include "chapter-04.typ"
#include "chapter-05.typ"
#include "chapter-06.typ"
#include "chapter-07.typ"
#include "chapter-08.typ"
