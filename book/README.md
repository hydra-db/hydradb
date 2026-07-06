# turbolay Design Docs (Typst)

A single [Typst](https://typst.app/) book that indexes the design docs for
**turbolay** — HydraDB's property-graph-on-S3 engine (`graphdb-on-s3`). It
gathers the RFCs, implementation notes, and mental models into one narrative,
using a vendored [bookly](https://typst.app/universe/package/bookly) template,
one `main.typ` entry point, and per-topic chapter files.

## Layout

```
book/
  main.typ                 # index entry: config + TOC + includes chapters/_index.typ
  chapters/
    _index.typ             # AUTO-GENERATED #part/#include list (do not edit)
    guide-00-orientation.typ   # one file per doc
    rfc-*.typ
  vendor/bookly/           # vendored template (offline, self-contained)
  scripts/gen-index.sh     # regenerates chapters/_index.typ from the folder
  Makefile
```

## What "automatically updated" means

- **The index (`chapters/_index.typ`).** Typst cannot list a directory, so
  `scripts/gen-index.sh` scans `chapters/` and rewrites the `#part` / `#include`
  list. Add a chapter file (named with a known prefix), run `make index`, and it
  appears in the book — no hand-edit of `main.typ`. `make build` runs `index`
  first, so a new file is never missed.
- **Everything Typst owns.** The table of contents, list of figures/tables,
  chapter/part numbering, and cross-references are regenerated from the actual
  content on every compile.

Naming convention → part: `guide-*` → *Reader's Guide*, `rfc-*` → *RFCs*,
`bug-*` → *Bugs*, `reflection-*` → *Reflections*, anything else → *Other*.
Files are ordered lexically within a part.

## Build

Requires the `typst` CLI (this repo was built with typst 0.15).

```bash
make -C book build      # regenerate index + compile -> book/main.pdf
make -C book watch      # live rebuild while editing
make -C book dark       # dark-theme PDF
```

`*.pdf` is a generated artifact and is gitignored — commit the `.typ` sources.

## Adding a doc

1. Write `book/chapters/<prefix>-<slug>.typ`, starting with
   `#import "../vendor/bookly/src/bookly.typ": *` and a top-level `= Title`.
2. `make -C book build`.
