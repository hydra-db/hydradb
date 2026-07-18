# Book handoff — 2026-07-13 edition

The book was rebuilt for the SlateDB graph-kernel architecture on
`feat/2026-07-13`. The old `main` edition described a different implementation:
one namespace, one writer, binary posting keys, and no complete query engine.

This edition follows the current system:

1. replaceable compute over durable object storage;
2. cells as the placement and write-ownership unit;
3. leases, fencing, and cell-local serializable writes;
4. epoch-pinned reads, artifacts, and delta overlays;
5. local Cypher execution and Roaring-backed matrix rows;
6. bounded distributed-query coordination;
7. implementation deep dives for Roaring, writes, and caches.

The generated PDF is ignored by Git. Run `make -C book build` before handoff.
