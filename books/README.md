# TurboLay books

Three books, all describing the current source on `Turbolay-V3`, all authored the
same way (one shared `template.typ`, `vendor/bookly` theme, and `scripts/`):

- **`conceptual.typ` — "TurboLay: A Conceptual Guide"** — mental models: replaceable
  compute, cells, the write boundary, epochs/artifacts/overlays, Cypher execution,
  and bounded distributed queries. Start here to understand *what* TurboLay is.
- **`inside.typ` — "Inside TurboLay: How This Is Implemented"** — a code-level guide
  that traces foundations, architecture, and the read / write / delete / caching
  paths through the real source. For someone who wants to *contribute*.
- **`quint.typ` — "Verifying TurboLay"** — Quint, formal verification, and
  model-based testing (Acts I–III): why correctness is hard here, the models, bounded
  proof with Apalache, and replaying traces against the Rust kernel.

Recommended reading order: **conceptual → inside → quint**.

## Build

Requires Typst 0.15.

```bash
make -C books build        # all three books (dark theme)
make -C books conceptual   # just the conceptual guide
make -C books inside       # just the code-level guide
make -C books quint        # just the verification book
make -C books light        # light theme
```

`scripts/gen-index.sh` regenerates `chapters/_index.typ` for the conceptual guide by
sweeping `intro-*` / `detail-*` chapter files. Never edit `_index.typ` by hand. The
`inside` (`00`–`05`) and `quint` (`quint-00`–`quint-08`) guides list their chapters
explicitly in their root `.typ`; the sweep ignores those prefixes.

## Editing

Read `AUTHORING-NOTES.md` and `DIAGRAM-STYLE.md` before touching a chapter, and the
authoring aids in `skills/` (`writing-content`, `formatting-chapters`). All three
books share one visual language and terminology. Code excerpts and `file:line`
references must be verified against current source before citing.
