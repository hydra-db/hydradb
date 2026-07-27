# Local Bookly fork

This directory is a vendored local copy of `@preview/bookly:4.0.1` from <https://github.com/maucejo/bookly>.

Local changes:

- Added `src/themes/reader.typ` with three reader modes: `light`, `sepia`, and `dark`.
- Exported the new `reader` theme, `reader-colors`, `reader-title-page`, `codeblock`, and semantic color helpers from `src/bookly-themes.typ`.
- Made citation bracket coloring use the active text color in `src/bookly.typ`.

Build examples:

```bash
typst compile main.typ main-light.pdf
typst compile --input mode=sepia main.typ main-sepia.pdf
typst compile --input mode=dark main.typ main-dark.pdf
```

Do not edit Typst's package cache under `~/Library/Caches/typst`; this fork is the repo-local copy used by `main.typ` and chapter files.
