#import "../vendor/bookly/src/bookly.typ": *
#import "@preview/fletcher:0.5.7" as fletcher: diagram, node, edge

= The Compute Node Can Disappear

A graph server has just failed. Its memory is gone. Its local SSD may be gone.
The replacement process starts with an empty heap and an empty cache. What must
still exist for the graph to be the same graph?

TurboLay answers with a hard boundary: the object store holds durable truth;
the compute node holds ways to reach it quickly.

#boxeq[
  *The graph survives the machine. Memory and local disk make it faster, but
  they do not make it true.*
]

#figure(
  diagram(
    spacing: (10mm, 14mm),
    node-stroke: 0.5pt,
    crossing-fill: reader-colors.paper,
    node((0, 0), text(size: 8pt)[graph-node A — heap · caches · writer role],
      fill: reader-colors.bad_soft, stroke: (paint: reader-colors.bad, dash: "dashed"),
      shape: fletcher.shapes.rect, corner-radius: 3pt),
    node((1, 0), text(size: 8pt)[graph-node A′ (replacement) — empty heap · cold cache],
      fill: reader-colors.surface_soft, stroke: reader-colors.border,
      shape: fletcher.shapes.rect, corner-radius: 3pt),
    node((0.5, 1), text(size: 8pt)[Object store (SlateDB): edges · epochs · index generations · manifests],
      fill: reader-colors.purple_soft, stroke: reader-colors.purple,
      shape: fletcher.shapes.rect, corner-radius: 3pt),
    edge((0, 0), (0.5, 1), "--}>", stroke: (paint: reader-colors.bad, dash: "dashed"),
      label: text(size: 8pt, fill: reader-colors.muted)[gone]),
    edge((0.5, 1), (1, 0), "->", stroke: reader-colors.muted,
      label: text(size: 8pt, fill: reader-colors.muted)[re-hydrate same path]),
  ),
  caption: [Memory and local disk make the graph faster, not true: a replacement node
    rebuilds the same logical state from the same object-store path.],
) <fig-intro00-boundary>

Read the picture top-down. The two compute nodes are interchangeable: when node A
dies, node A′ starts with a cold heap and re-opens the *same* object-store path, so
the graph it serves is identical. The crash costs cache warmth and in-flight work,
never a fact.

== The one-sentence architecture

TurboLay is an object-store-backed graph kernel that partitions a graph into
cells, admits at most one writer per cell — a guarantee SlateDB's own manifest
fencing enforces, not a lock TurboLay holds — and accelerates snapshot reads
with durable index generations plus disposable compute-local caches.

That sentence has four load-bearing nouns:

- *Object store* — the durable home of SlateDB state.
- *Cell* — the unit of storage path and write ownership.
- *Epoch* — the version named by a mutation or a snapshot read; concretely, a
  SlateDB storage sequence number.
- *Index generation* — derived query structure, immutable and rebuilt out of
  process, built from canonical edges alone.

#figure(
  table(
    columns: (1fr, 0.9fr, 1fr),
    align: center,
    inset: 8pt,
    table.header([*Clients*], [*Compute*], [*Durable storage*]),
    [Cypher and graph APIs], [parser, planner, traversal], [SlateDB on S3],
    [query legs], [matrix and GraphBLAS caches], [edges, epochs, index generations],
    [writes], [writer lanes and writer promotion], [manifest fencing, idempotency],
  ),
  caption: [The object store owns durable state; compute owns execution and acceleration.],
) <tab-architecture-boundary>

== Two cache layers, neither authoritative

SlateDB can place fetched object-store data in a local disk cache. Above it,
`GraphShard` maintains graph-aware memory caches: matrix artifacts, hydrated
adjacency, compiled GraphBLAS matrices, parsed row queries, and relationship-row
caches (relationship, source-relationship, and relationship-property rows)
(`src/core/state.rs`).

A cache miss costs work. It does not change the answer. A replacement node can
open the same object-store path and hydrate the same logical state. Tests such
as `reopened_reader_sees_data_from_object_store` (`src/tests.rs`) exercise that
replacement boundary: a fresh shard opens the same object-store path and serves
the same edges from an empty heap and cache.

#custom-box(title: [Term — Replaceable compute], icon: "info")[
  “Replaceable compute” is more accurate than “stateless compute.” An active
  process holds a writer role, a promoted SlateDB writer handle, semaphores,
  parsed plans, and hydrated matrices. Those are runtime state, but none is the
  sole durable copy of the graph. Losing the writer handle is the interesting
  case, and it is still not a durability event: a replacement node promotes its
  own writer, and SlateDB's manifest fencing is what stops the old one from
  committing afterwards.
]

== Not a separate storage service

The physical boundary is real, but the software boundary is intentionally
unfinished. `GraphShard` directly owns a `slatedb::Db`, an object-store handle,
and its query caches. Query code calls the shard's SlateDB-backed accessors.
There is no independent storage RPC service and no `QueryStorage` trait.

So the honest claim is:

#info-box(title: [Built boundary])[Object-store-backed durable state with
replaceable compute is built. Independently swappable storage and compute
services are not.]

The next chapter names the unit that makes replacement and ownership tractable:
the cell.
