#import "vendor/bookly/src/bookly.typ": *
#import "@preview/marginalia:0.3.1" as marginalia

#show: bookly.with(
  author: "HydraDB Team",
  title: "turbolay Design Docs",
  lang: "en",
  theme: reader,
  colors: reader-colors,
  fonts: (
    body: "Avenir Next",
    math: "New Computer Modern Math",
    raw: "Source Code Pro",
  ),
  title-page: reader-title-page(
    subtitle: "RFCs, Implementation Notes, and Mental Models — turbolay (property-graph on S3)",
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

// `#note[...]` sidenotes (chapters/*.typ) render via marginalia; without an
// explicit setup() call its margin column defaults to 1.5cm, which crushes
// prose comments into a one-word-per-line sliver (e.g. chapter "Read Path"
// §7.7). Reserve a real right-margin column and widen the page to pay for it
// out of new space rather than the body column.
#show: marginalia.setup.with(
  inner: (far: 1.6cm, width: 0cm, sep: 0cm),
  outer: (far: 1cm, width: 4.5cm, sep: 0.6cm),
  top: 1.6cm,
  bottom: 1.6cm,
)
#set page(width: 25cm, fill: reader-colors.paper)
#set text(size: 10pt, fill: reader-colors.text)

#show: front-matter

// Title page is auto-generated; no manual front-matter content needed.

#show: main-matter

#tableofcontents
#listoffigures
#listoftables

// The parts and their #include lists below are AUTO-GENERATED from the files
// in chapters/ by scripts/gen-index.sh. Do not add #include lines here by
// hand — add a chapter file and run `make -C book index`.
#include "chapters/_index.typ"
