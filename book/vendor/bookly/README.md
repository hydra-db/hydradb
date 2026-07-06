# Book template

[![Generic badge](https://img.shields.io/badge/Version-4.0.1-cornflowerblue.svg)](https://github.com/maucejo/bookly/releases/tag/4.0.1)
[![MIT License](https://img.shields.io/badge/License-MIT-forestgreen)](https://github.com/maucejo/book_template/blob/708d8ca8ac2262fc77df8756835420d728b0d281/LICENSE)
[![User Manual](https://img.shields.io/badge/doc-.pdf-mediumpurple)](https://github.com/maucejo/bookly/blob/708d8ca8ac2262fc77df8756835420d728b0d281/docs/manual.pdf)


The `bookly` template is a Typst package designed for writing academic documents such as theses, French habilitations, or scientific books. It provides a structured format that adheres to academic standards, making it easier for authors to focus on content rather than formatting.

## Basic usage

This section provides the minimal amount of information to get started with the template. For more detailed information, see the [manual](https://github.com/maucejo/bookly/blob/708d8ca8ac2262fc77df8756835420d728b0d281/docs/manual.pdf).

To use the `bookly` template, you need to include the following line at the beginning of your typ file:

```typ
#import "@preview/bookly:4.0.1": *
```

After importing `bookly`, you have to initialize the template by a show rule with the `#bookly()` command.

**Example**
```typ
#show: bookly.with(
  title: "My document",
  author: "Author Name",
  theme: modern,
  lang: "en",
  tufte: false,
  fonts: (
    body: "Lato",
    math: "Lete Sans Math"
  ),
  title-page: book-title-page(
    series: "Typst book series",
    institution: "Typst community",
    logo: image("images/typst-logo.svg"),
    cover: image("images/book-cover.jpg", width: 45%)
  ),
  config-options: (
    open-right: true,
    alt-margins: false
  )
)
```

## Main features

* Themes: `classic`, `modern`, `fancy`, `obook`, `orly`, `pretty`
* Tufte layout powered by `marginalia` package
* Language support: English, Chinese, French, German, Italian, Portuguese, Spanish
* Font customization: Body, math and raw fonts can be customized
* Environments: `front-matter`, `main-matter`, `appendix`, `back-matter`
* Outlines: `tableofcontents`, `listoffigures`, `listoftables`, `minitoc`
* Part and chapter definition: `part`, `chapter`, `chapter-nonum`

> **_Note:_**  The chapters can be also written using the Typst standard markup syntax.

## Helper functions

* Subfigures - based on the `subpar` package
    ```typ
    #subfigure(
        figure(image("image1.png"), caption: []),
        figure(image("image2.png"), caption: []), <b>,
        columns: (1fr, 1fr),
        caption: [Figure title],
        label: <fig:subfig>,
    )
    ```

* Equations
    * Boxed equations
        ```typ
        $
            #boxeq[$p(A|B) prop p(B|A) space p(A)$]
        $
        ```

    * Unnumbered equations
        ```typ
        $
            integral_0^1 f(x) dif x = F(1) - F(0)
        $ <nonum-eq>
        ```

    * Subequation numbering based on the `equate` package

* Information boxes
    * `#info-box` for remarks
    * `#tip-box` for tips
    * `#important-box` for important notes
    * `proof-box` for proofs
    * `question-box` for questions
    * `custom-box` for user defined boxes

* `book-title-page` for defining the title page of a book

* `thesis-title-page` for defining the title page of a thesis

* `back-cover` for defining the back cover of a book

## Dependencies

`bookly` relies on the following packages:

* `marginalia:0.3.1`: for tufte layout.

* `hydra:0.6.2` : for bibliography management.

* `equate:0.3.2` : for advanced equation numbering.

* `itemize:0.2.0"`: for lists and enumerations customization.

* `showybox:2.0.4` : for custom boxes.

* `suboutline:0.3.0` : for mini tables of contents in chapters.

* `subpar:0.2.2` : for subfigures.

## Licence

MIT licensed

Copyright © 2026 Mathieu AUCEJO (maucejo)
