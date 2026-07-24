#import "../vendor/bookly/src/bookly.typ": *
#import "@preview/fletcher:0.5.7" as fletcher: diagram, node, edge

= Distribution with Edges Around It

“Distributed query support” can mean anything from two RPCs to a global
optimizer with repartition joins. TurboLay implements a useful, deliberately
bounded middle: dispatch known query legs to nodes, execute them concurrently,
then merge their rows at a coordinator.

#boxeq[
  *Distributed query = caller-named cell legs + concurrent execution + a
  bounded coordinator merge.*
]

== The caller supplies the legs

`DistributedQueryPlan` contains named legs (`src/query/coordination.rs`). Each leg
carries its own `QueryContext`, cell ID, Cypher string, and optional row estimate.
The coordinator picks a registered `QueryCellClient` for each leg and launches them
all with `join_all`.

The interesting part is how it picks. `client_for_cell` first checks that the cell
*exists* in the object-store node directory — an unknown cell is `UnknownShard` —
and then chooses a client by hashing the cell id:

```rust
let mut hash = 0xcbf29ce484222325_u64;
for byte in cell_id.as_bytes() {
    hash ^= u64::from(*byte);
    hash = hash.wrapping_mul(0x100000001b3);
}
let index = usize::try_from(hash % self.clients.len().max(1) as u64).unwrap_or(0);
self.clients.values().nth(index)
```

That looks like a placement hash, and it is worth being precise about why it is not.
The hash does not discover who *owns* the cell, because nobody does: every node in
the directory opened every cell, so any registered client can answer this leg
correctly. The hash exists so that the same cell tends to land on the same node
across queries, which keeps that node's caches warm. It is cache affinity, not
correctness authority — send the leg to the "wrong" node and you get the same rows,
slightly slower.

#custom-box(title: [Why], icon: "tip")[
  This is the payoff of having no owner map. A routing table that decides
  *correctness* has to be consistent, agreed upon, and updated under failure — that
  is a consensus problem, and it is how the deleted `ShardPlacement` design pulled a
  control plane into the read path. A routing hint that only decides *which cache is
  warm* can be stale, disagreed upon, or wrong, and the worst outcome is a slower
  query. Demoting routing from authority to hint is what let the coordinator become
  this small.
]

#figure(
  table(
    columns: (1fr, 0.25fr, 1fr, 0.25fr, 1fr),
    align: center,
    inset: 7pt,
    [coordinator], [`→`], [node A / cell A], [`→`], [leg A rows],
    [], [`→`], [node B / cell B], [`→`], [leg B rows],
    [merge], [`←`], [UNION ALL or inner join], [`←`], [named leg results],
  ),
  caption: [The coordinator routes explicit cell legs and merges their returned rows.],
) <tab-distributed-query>

#figure(
  diagram(
    spacing: (16mm, 10mm),
    node-stroke: 0.5pt,
    crossing-fill: reader-colors.paper,
    node((0, 1), text(size: 8pt)[coordinator],
      fill: reader-colors.surface_soft, stroke: reader-colors.border,
      shape: fletcher.shapes.rect, corner-radius: 3pt),
    node((1, 0), text(size: 8pt)[node A / cell A],
      fill: reader-colors.purple_soft, stroke: reader-colors.purple,
      shape: fletcher.shapes.rect, corner-radius: 3pt),
    node((1, 2), text(size: 8pt)[node B / cell B],
      fill: reader-colors.purple_soft, stroke: reader-colors.purple,
      shape: fletcher.shapes.rect, corner-radius: 3pt),
    node((2, 1), text(size: 8pt)[merge — UNION ALL / one inner-join],
      fill: reader-colors.ok_soft, stroke: reader-colors.ok,
      shape: fletcher.shapes.rect, corner-radius: 3pt),
    node((1, 3), text(size: 8pt)[reads distribute; writes stay local],
      fill: reader-colors.info_soft, stroke: reader-colors.info,
      shape: fletcher.shapes.rect, corner-radius: 3pt),
    edge((0, 1), (1, 0), "->", stroke: reader-colors.muted,
      label: text(size: 8pt, fill: reader-colors.muted)[dispatch leg (hash = cache affinity)]),
    edge((0, 1), (1, 2), "->", stroke: reader-colors.muted,
      label: text(size: 8pt, fill: reader-colors.muted)[concurrent (join_all)]),
    edge((1, 0), (2, 1), "->", stroke: reader-colors.muted),
    edge((1, 2), (2, 1), "->", stroke: reader-colors.muted),
  ),
  caption: [The coordinator dispatches caller-named cell legs to nodes that can all serve
    them, runs the legs concurrently, and merges the returned rows — no owner lookup, no
    global planner, and no global snapshot.],
) <fig-intro05-scatter>

Read it left to right. The coordinator does not plan the split — the caller already
named the legs — so all it does is choose a node per leg, run the legs concurrently,
and merge what comes back. There is no global planner and no global snapshot anywhere
in the picture. Writes are the exception that proves the shape: a leg that mutates has
to reach a node that is promotable for that cell, so writes do not fan out.

Clients may be in-process implementations — `RoutedGraphCluster` itself
implements `QueryCellClient` — or, with the `query-transport` feature, TCP
clients (`TcpQueryCellClient`). Bearer authentication ships with that base
transport; TLS (`query-transport-tls`) and HTTP service-discovery adapters
(`query-service-discovery`) are separate features that extend it without
changing the leg interface.

== The merge vocabulary is intentionally small

The coordinator currently supports:

- executing the same query over many cell contexts;
- paged requests per cell;
- `UNION ALL` over named leg results;
- one coordinator-side inner join shape;
- cost ordering of inner-join legs by estimated rows.

This is enough for explicit scatter/gather plans. It is not an arbitrary
distributed Cypher compiler. It does not repartition intermediate rows,
push distributed aggregates, or choose cell boundaries from a single global
Cypher statement.

#info-box(title: [Known review finding])[The coordinator-side inner-join merger
keys its build and probe maps on `QueryValue` directly, and `QueryValue::Null`
is an ordinary map key. Two `NULL` join keys therefore compare equal and can
produce spurious joined rows; a correct inner join would skip null keys on both
the build and probe sides. No test currently pins this behaviour, so avoid
coordinator inner joins on nullable keys.]

== Reads distribute further than writes

The query transport exposes cell reads. A write stays on the node that handles it:
that node must be promotable for the cell, and it promotes a SlateDB writer locally
before mutating. Uniqueness of that writer is SlateDB's manifest fencing, not a lock
record. There is no automatic remote write forwarder and no transaction spanning
cells.

That asymmetry is the whole reason reads distribute further than writes. A read is
servable by every node because every node opened every cell, so the coordinator is
free to pick any of them. A write has to reach a node willing to become the writer,
and only one such promotion can survive per cell.

#info-box(title: [Built boundary])[Distributed reads are directory-aware and
concurrent — any node can serve any cell, and the coordinator's choice is a cache
hint. Writes are independently sharded per cell and fenced by SlateDB's manifest.
Neither implies a global distributed snapshot or multi-cell atomic commit.]

== The production boundary

This repository is a kernel/library. A production service must still supply
deployment control, placement rollout policy, failover orchestration, tenant
quotas, observability export, secret rotation, and long S3 fault/latency soaks.
The architecture contains the mechanisms those systems drive; it is not itself
the complete hosted database.
