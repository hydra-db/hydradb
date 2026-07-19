# TurboLay books

Two books, both describing the current source on `Turbolay-V3` (HEAD `8e2b6e4`):

- **`conceptual.typ` — "TurboLay: A Conceptual Guide"** — mental models: replaceable
  compute, cells, the write boundary, epochs/artifacts/overlays, Cypher execution,
  and bounded distributed queries. Ports the old `book/main.typ` edition, corrected.
- **`inside.typ` — "Inside TurboLay: How This Is Implemented"** — a code-level guide
  that traces foundations, architecture, and the read / write / delete / caching
  paths through the real source. Ports the old `book/book.typ` edition, corrected.

These are new editions in a dedicated `books/` folder. The older editions remain
untouched in `book/`.

## Build

Requires Typst 0.15 (same as `book/`).

```bash
make -C books build        # both books (dark theme)
make -C books conceptual   # just the conceptual guide
make -C books inside        # just the code-level guide
make -C books light         # light theme
```

`scripts/gen-index.sh` regenerates `chapters/_index.typ` for the conceptual guide by
sweeping `intro-*` / `detail-*` chapter files. Never edit `_index.typ` by hand. The
code-level guide lists its `00`–`05` chapters explicitly in `inside.typ`.

## Editing

Read `AUTHORING-NOTES.md` before touching a chapter. It records the current-source
facts, the canonical terminology, and the corrections applied when porting from the
old editions. Code excerpts and line numbers are quoted from HEAD `8e2b6e4`; verify
any `file:line` against current source before citing it.
