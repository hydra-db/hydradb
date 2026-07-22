# Book authoring rules

This book documents the current code, not the removed postings-only kernel.

- Open with a concrete operational problem, then earn the mechanism.
- Separate built behavior from future or production work explicitly.
- Cite current source files for implementation claims.
- Use “cell” for the implemented isolation and ownership unit. Do not call a
  cell a namespace unless the surrounding API defines it that way.
- Describe compute as replaceable, not completely stateless: a writer holds
  SlateDB's writer handle (fenced through the manifest, not a lease) and all
  nodes retain caches and runtime state. There are no leases — the type and the
  key builder were deleted.
- Describe distributed queries as bounded scatter/gather. Do not imply a
  transparent arbitrary distributed Cypher planner or a global snapshot.
- Describe writes as atomic within one cell. Do not imply multi-cell atomicity.
- Roaring is not on this branch at all: zero occurrences in `src/`, not a Cargo
  dependency. Hydrated matrix rows are `BTreeMap<VertexId, BTreeSet<VertexId>>`
  (`src/lib.rs:142`). Keep the Roaring material, but label it PLANNED.
- `AUTHORING-NOTES.md` is the binding brief on what is true at HEAD — read it
  before writing any implementation claim.
- Regenerate the index and compile the PDF after every structural change.
