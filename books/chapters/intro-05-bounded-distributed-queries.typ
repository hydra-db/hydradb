#import "../vendor/bookly/src/bookly.typ": *
#import "@preview/fletcher:0.5.7" as fletcher: diagram, node, edge

= Distribution with Edges Around It

“Distributed query support” can mean anything from two RPCs to a global
optimizer with repartition joins. TurboLay implements a useful, deliberately
bounded middle: route known query legs to cell owners, execute them concurrently,
then merge their rows at a coordinator.

#boxeq[
  *Distributed query = placement-aware cell legs + concurrent execution + a
  bounded coordinator merge.*
]

== The caller supplies the legs

`DistributedQueryPlan` contains named legs. Each leg carries its own
`QueryContext`, cell ID, Cypher string, and optional row estimate. The
coordinator looks up the cell's owner in `ShardPlacement`, selects a registered
`QueryCellClient`, and launches all legs with `join_all`
(`src/query/coordination.rs`).

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
      label: text(size: 8pt, fill: reader-colors.muted)[route leg (ShardPlacement)]),
    edge((0, 1), (1, 2), "->", stroke: reader-colors.muted,
      label: text(size: 8pt, fill: reader-colors.muted)[concurrent (join_all)]),
    edge((1, 0), (2, 1), "->", stroke: reader-colors.muted),
    edge((1, 2), (2, 1), "->", stroke: reader-colors.muted),
  ),
  caption: [The coordinator routes explicit cell legs to their owners, runs them
    concurrently, and merges the returned rows — no global planner or global snapshot.],
) <fig-intro05-scatter>

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

The query transport exposes cell reads. Routed writes remain local-owner
operations protected by the cell write lock. There is no automatic remote write
forwarder and no transaction spanning cell owners.

#info-box(title: [Built boundary])[Distributed reads are placement-aware and
concurrent. Writes are independently sharded per cell. Neither implies a
global distributed snapshot or multi-cell atomic commit.]

== The production boundary

This repository is a kernel/library. A production service must still supply
deployment control, placement rollout policy, failover orchestration, tenant
quotas, observability export, secret rotation, and long S3 fault/latency soaks.
The architecture contains the mechanisms those systems drive; it is not itself
the complete hosted database.
