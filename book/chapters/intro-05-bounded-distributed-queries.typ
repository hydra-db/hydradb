#import "../vendor/bookly/src/bookly.typ": *

= Distribution with Edges Around It

“Distributed query support” can mean anything from two RPCs to a global
optimizer with repartition joins. turbolay implements a useful, deliberately
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

Clients may be in-process implementations or, with `query-transport`, TCP
clients. Optional TLS, bearer authentication, and service-discovery adapters
extend that transport without changing the leg interface.

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

#info-box(title: [Known review finding])[The inner-join merger currently treats
`NULL` keys as equal and can produce spurious rows. The failing
`inner_join_null_keys_do_not_match` test pins this bug; both build and probe
sides must skip null join keys. Until fixed, do not use coordinator inner joins
on nullable keys.]

== Reads distribute further than writes

The query transport exposes cell reads. Routed writes remain local-owner
operations protected by the cell lease. There is no automatic remote write
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
