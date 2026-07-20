#import "template.typ": *

#show: book.with(
  title: "Inside TurboLay",
  subtitle: "How this is implemented — a code-level guide to slatedb-graph-kernel",
)

// --------------------------------- Preface --------------------------------
#heading(numbering: none, outlined: false)[How to read this book]

This book teaches the #raw("slatedb-graph-kernel") codebase (the team calls it
#emph[TurboLay]) by walking through the real source. Every claim points at a file
and, where it helps, a line. The goal is that after reading it you can open the
repository and know what a piece of code does, why it is there, and how it fits the
larger machine.

The book assumes you can read Rust but does not assume you know graph databases,
log-structured storage, or object stores. Chapter 0 defines every term the later
chapters use. When a term first appears it is called out in a purple box. Design
decisions are called out in yellow boxes.

Code shown in the book is quoted from the tree at the commit noted in the build
file (`README.md`). Line numbers drift as the code changes, so treat them as
signposts, not contracts. Read the surrounding function, not only the quoted
lines.

#include "chapters/00-foundations.typ"
#include "chapters/01-architecture.typ"
#include "chapters/02-read-path.typ"
#include "chapters/03-write-path.typ"
#include "chapters/04-delete-path.typ"
#include "chapters/05-caching.typ"
