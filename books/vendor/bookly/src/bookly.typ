// Exported packages
#import "@preview/equate:0.3.3": *
// Internals
#import "bookly-environments.typ": *
#import "bookly-outlines.typ": *
#import "bookly-components.typ": *
#import "bookly-helper.typ": *
#import "bookly-themes.typ": *
#import "bookly-tufte.typ": *

// Template
#let bookly(
  title: "Title",
  author: "Author Name",
  theme: fancy,
  tufte: false,
  logo: none,
  lang: "en",
  fonts: default-fonts,
  colors: default-colors,
  title-page: default-title-page,
  config-options: default-config-options,
  body
) = context {
  // Document's properties
  set document(author: author, title: title)
  states.author.update(author)
  states.title.update(title)
  states.tufte.update(tufte)

  // Book colors
  let book-colors = default-colors + colors
  states.colors.update(book-colors)

  // Book theme
  let bookly-theme = classic + theme
  states.theme.update(bookly-theme)

  // Configuration options
  let book-options = default-config-options + config-options
  states.alt-margins.update(book-options.alt-margins)
  states.open-right.update(book-options.open-right)
  states.part-numbering.update(book-options.part-numbering)

  // Fonts
  set text(font: fonts.body, lang: lang, size: text-size, ligatures: false)

  // Math font
  show math.equation: set text(font: fonts.math, stylistic-set: 1)
  // Unnumbered equations
  show selector(<nonum-eq>): set math.equation(numbering: none)

  // Equations
  show: equate.with(breakable: true, sub-numbering: true)

  // Paragraphs
  set par(justify: true)

  // Localization
  let bookly-lang = if default-language.contains(lang) {
    lang
  } else {
    "en"
  }
  states.localization.update(json("resources/i18n/" + bookly-lang + ".json"))


  // References
  set ref(supplement: none)

  // Citations
  show cite: it => context {
    let c = states.colors.get()
    show regex("\[|\]"): it => text(fill: c.at("text", default: black))[#it]
    it
  }

  // Footnotes
  // show footnote.entry: it => {
  //   [#h(it.indent) #text(fill: book-colors.primary, it.note) #it.note.body]
  // }

  // Outline entries
  set outline(depth: 3)

  // Figures
  let numbering-fig = n => {
      let h1 = counter(heading).get().first()
      numbering(states.num-pattern-fig.get(), h1, n)
  }

  show figure.where(kind: image): set figure(
      supplement: fig-supplement,
      numbering: numbering-fig,
      gap: 1.5em
    )

  set figure.caption(position: top) if tufte
  show: show-if(tufte, it => {
    show figure.caption.where(position: top): note.with(
      alignment: "top",
      counter: none,
      shift: "avoid",
      keep-order: true,
    )
    it
  })
  show figure: set figure.caption(separator: [ -- ])

  // Equations
  let numbering-eq = (..n) => {
    let h1 = counter(heading).get().first()
    numbering(states.num-pattern-eq.get(), h1, ..n)
  }

  set math.equation(numbering: numbering-eq)

  // Tables
  show figure.where(kind: table): set figure(
    numbering: numbering-fig,
  )

  show figure.where(kind: table): it => {
    set figure.caption(position: top)
    it
  }

  // Title page
  if title-page != none {
    title-page
  } else {
    default-title-page
  }

  show: show-if(tufte, it => {
    let marginalia-book = if book-options.alt-margins {true} else {false}

    let m-config = (
    inner: (far: 1.25cm, width: 0cm, sep: 0cm),
    outer: (far: 1.25cm, width: 5cm, sep: 0.5cm),
    book: marginalia-book
    )

    show: marginalia.setup.with(..m-config)
    it
  })


  // show: marginalia.show-frame.with(footer: false)

  // Headings
  show: theme.theme.with(colors: book-colors)
  show: show-if(book-options.open-right, it => {
    show: headings-on-odd-page
    it
  })

  // Unnumbered sections - Thanks to @bluss (Typst universe: How to have headings without numbers in a fluent way?)
  show selector(<nonum-sec>): set heading(numbering: none)

  body
}
