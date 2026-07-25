# CLAUDE.md

Guidance for Claude Code when working in this repository.

## Never create artifacts

**Never use the Artifact tool in this repository — not for anything, ever.**

Visual and long-form deliverables are local HTML files written into
`interactive/` with Write/Edit, and nothing else. The user opens them from disk.

- Complete standalone documents: `<!doctype html>`, `<head>` with
  `<meta charset="utf-8">` and a viewport tag. `file://` must work by
  double-clicking.
- Self-contained: no CDN scripts, no external stylesheets, no remote fonts or
  images.
- Light and dark, following `interactive/assets/textbook.css`.
- `interactive/README.md` documents the existing house style.
