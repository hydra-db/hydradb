# Building the book

This directory holds a Typst book, "Inside turbolay", that teaches the
`slatedb-graph-kernel` codebase.

## Requirements

- [Typst](https://github.com/typst/typst) 0.15.0 or newer.
- Network access on the first build, so Typst can download two packages from
  Typst Universe:
  - `@preview/fletcher:0.5.7` (box-and-arrow diagrams)
  - `@preview/cetz:0.3.4` (low-level drawings; pulled in transitively)

  They are cached under `~/.cache/typst/packages/` after the first build, so
  later builds work offline.
- Fonts used: Libertinus Serif (body) and DejaVu Sans Mono (code). Both ship
  with most Typst installs. Any missing font falls back automatically.

## Compile

```sh
# from this directory
typst compile book.typ book.pdf
```

To watch and rebuild on save while editing:

```sh
typst watch book.typ book.pdf
```

## Layout

```
book.typ                 entry point: title page, preface, table of contents, includes
template.typ             styling helpers (term boxes, why callouts, source blocks)
chapters/
  00-foundations.typ     all terminology, defined from first principles
  01-architecture.typ    module map and how the pieces wire together
  02-read-path.typ       how a read/query executes
  03-write-path.typ      how a write commits
  04-delete-path.typ     how a delete and its garbage collection work
  05-caching.typ         the cache layers
```

## Source references

Code excerpts and line numbers are quoted from the tree at commit `b67d457`.
Line numbers drift as the code changes, so treat them as signposts. When a
number looks off, search for the quoted function or type name instead.
