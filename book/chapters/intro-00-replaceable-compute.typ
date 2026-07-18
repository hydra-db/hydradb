#import "../vendor/bookly/src/bookly.typ": *

= The Compute Node Can Disappear

A graph server has just failed. Its memory is gone. Its local SSD may be gone.
The replacement process starts with an empty heap and an empty cache. What must
still exist for the graph to be the same graph?

turbolay answers with a hard boundary: the object store holds durable truth;
the compute node holds ways to reach it quickly.

#boxeq[
  *The graph survives the machine. Memory and local disk make it faster, but
  they do not make it true.*
]

== The one-sentence architecture

turbolay is an object-store-backed graph kernel that partitions a graph into
cells, serializes each cell's writes with leases and epochs, and accelerates
snapshot reads with durable artifacts plus disposable compute-local caches.

That sentence has four load-bearing nouns:

- *Object store* — the durable home of SlateDB state.
- *Cell* — the unit of storage path, placement, lease, and write ownership.
- *Epoch* — the version named by a mutation or a snapshot read.
- *Artifact* — derived query structure built from canonical edges and deltas.

#figure(
  table(
    columns: (1fr, 0.9fr, 1fr),
    align: center,
    inset: 8pt,
    table.header([*Clients*], [*Compute*], [*Durable storage*]),
    [Cypher and graph APIs], [parser, planner, traversal], [SlateDB on S3],
    [query legs], [Roaring and GraphBLAS caches], [edges, epochs, artifacts],
    [writes], [leases and writer lanes], [fences, deltas, idempotency],
  ),
  caption: [The object store owns durable state; compute owns execution and acceleration.],
) <tab-architecture-boundary>

== Two cache layers, neither authoritative

SlateDB can place fetched object-store data in a local disk cache. Above it,
`GraphShard` maintains graph-aware memory caches: matrix manifests, hydrated
adjacency, compiled GraphBLAS matrices, parsed Cypher, reachability results,
posting chunks, and supernode groups (`src/core/state.rs`).

A cache miss costs work. It does not change the answer. A replacement node can
open the same object-store path and hydrate the same logical state. Tests such
as `control_plane_empty_compute_node_replacement_reads_object_store_state`
exercise that replacement boundary.

#note[
  “Replaceable compute” is more accurate than “stateless compute.” An active
  process holds leases, writer lanes, semaphores, parsed plans, and hydrated
  matrices. Those are runtime state, but none is the sole durable copy of the
  graph.
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
