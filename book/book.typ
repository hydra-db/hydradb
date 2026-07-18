#import "template.typ": book, term, why, srcblock, figcap, accent, muted

#show: book.with(
  title: "Inside turbolay",
  subtitle: "A code-level guide to slatedb-graph-kernel",
)

// ------------------------------- Title page -------------------------------
#page(numbering: none)[
  #v(6cm)
  #align(center)[
    #text(size: 34pt, weight: "bold")[Inside turbolay]
    #v(0.2em)
    #text(size: 15pt, fill: muted)[A code-level guide to #raw("slatedb-graph-kernel")]
    #v(2em)
    #line(length: 40%, stroke: 0.5pt + muted)
    #v(1em)
    #text(size: 11pt, fill: muted)[
      How the graph engine is built, and how a read, a write, a\
      delete, and the caches actually work, traced through the source.
    ]
  ]
]

// --------------------------------- Preface --------------------------------
#page(numbering: none)[
  #heading(numbering: none, outlined: false)[How to read this book]
  This book teaches the #raw("slatedb-graph-kernel") codebase (the team calls it
  #emph[turbolay]) by walking through the real source. Every claim points at a file
  and, where it helps, a line. The goal is that after reading it you can open the
  repository and know what a piece of code does, why it is there, and how it fits the
  larger machine.

  The book assumes you can read Rust but does not assume you know graph databases,
  log-structured storage, or object stores. Chapter 0 defines every term the later
  chapters use. When a term first appears it is called out in a purple box. Design
  decisions are called out in yellow boxes.

  Code shown in the book is quoted from the tree at the commit noted in the build
  file. Line numbers drift as the code changes, so treat them as signposts, not
  contracts. Read the surrounding function, not only the quoted lines.

  #v(1em)
  #outline(title: [Contents], indent: auto, depth: 2)
]

#counter(page).update(1)

#include "chapters/00-foundations.typ"
#include "chapters/01-architecture.typ"
#include "chapters/02-read-path.typ"
#include "chapters/03-write-path.typ"
#include "chapters/04-delete-path.typ"
#include "chapters/05-caching.typ"
