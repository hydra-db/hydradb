# turbolay book

This Typst book describes the implementation on `feat/2026-07-13`: a
SlateDB-backed graph kernel whose durable truth lives in an object store and
whose compute nodes own cells, leases, query execution, and disposable caches.

Build it with Typst 0.15:

```bash
make -C book build
```

`scripts/gen-index.sh` regenerates `chapters/_index.typ`. Add chapters using an
`intro-`, `detail-`, or `turbolay-` filename; never maintain the index manually.

The previous postings-only book remains available in Git history on `main`.
Its assumptions are not silently mixed into this edition.
