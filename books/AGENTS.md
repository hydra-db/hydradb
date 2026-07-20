# Book authoring rules

This book documents the current code, not the removed postings-only kernel.

- Open with a concrete operational problem, then earn the mechanism.
- Separate built behavior from future or production work explicitly.
- Cite current source files for implementation claims.
- Use “cell” for the implemented isolation and ownership unit. Do not call a
  cell a namespace unless the surrounding API defines it that way.
- Describe compute as replaceable, not completely stateless: writers retain
  leases and all nodes retain caches and runtime coordination state.
- Describe distributed queries as bounded scatter/gather. Do not imply a
  transparent arbitrary distributed Cypher planner or a global snapshot.
- Describe writes as atomic within one cell. Do not imply multi-cell atomicity.
- Roaring currently compresses hydrated, compute-local matrix rows. It is not
  yet the durable posting format.
- Regenerate the index and compile the PDF after every structural change.
