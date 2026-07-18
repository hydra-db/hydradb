#import "../book/vendor/bookly/src/bookly.typ": *
#import "../book/template.typ": term, why, srcblock, figcap, accent, muted

= Distribution with Edges Around It

Suppose customer data is divided between cell A and cell B. One request needs
rows from both. The phrase “distributed query” may suggest that the client can
send one arbitrary Cypher statement and the database will discover partitions,
move intermediate results, choose distributed joins, and create a global
snapshot.

That is not the contract turbolay implements.

It implements a smaller and useful mechanism: the caller supplies explicit
cell-local query legs, placement routes each leg to its owner, the legs execute
concurrently, and a coordinator performs one of a bounded set of merges.

== Problem 1: the coordinator cannot infer the partitioning policy

Chapter 2 established that the embedding service decides how a logical graph
maps to cells. A cell may represent a tenant, region, workload, or another
application boundary. The kernel receives that boundary; it does not infer it
from arbitrary graph patterns.

A distributed request must therefore name its legs.

#term("Distributed query leg")[
  One named cell-local query. A leg carries its own `QueryContext`, cell ID,
  Cypher string, and optional row estimate. It is independently executable by
  the node that serves that cell.
]

#term("Distributed query plan")[
  A collection of named legs plus a bounded coordinator merge operation. It is
  an explicit scatter/gather description, not a global Cypher syntax tree.
]

The public types keep those responsibilities visible:

#srcblock("src/query/coordination.rs:2566-2639 (abridged)")[```rust
pub struct DistributedQueryLeg {
    pub name: String,
    pub context: QueryContext,
    pub query: String,
    pub estimated_rows: Option<u64>,
}

pub enum DistributedQueryMerge {
    UnionAll,
    InnerJoin(DistributedQueryJoin),
}

pub struct DistributedQueryPlan {
    pub legs: Vec<DistributedQueryLeg>,
    pub merge: DistributedQueryMerge,
}
```]

This API forces an important piece of knowledge to remain explicit: which
part of the request belongs to which cell.

#boxeq[
  *Distribution begins with named cell-local work, not with an assumption that
  one global query can be partitioned automatically.*
]

== Problem 2: a cell ID must become a live query client

A leg names a cell, but execution needs a node connection. The coordinator
uses the `ShardPlacement` from Chapter 2 to resolve the cell's owner, then
looks up a registered `QueryCellClient` for that node.

#term("QueryCellClient")[
  The asynchronous interface through which the coordinator asks a cell owner
  to execute rows, pages, or supported batch operations and to report or pin a
  graph epoch.
]

The client can be implemented in process by a routed cluster or over the
optional query transport. The coordinator does not need different merge code
for the two cases.

#srcblock("src/query/coordination.rs:2848-2858")[```rust
fn client_for_cell(&self, cell_id: &str) -> Result<Arc<dyn QueryCellClient>> {
    let owner = self.placement.owner(cell_id)?;
    self.clients
        .get(owner)
        .cloned()
        .ok_or_else(|| GraphError::CorruptValue {
            key: format!("query/node/{owner}"),
            reason: format!("missing query client for owner node {owner}"),
        })
}
```]

Placement failure is explicit. An unknown cell, missing owner, or unregistered
client fails the request instead of causing the coordinator to guess or scan
all nodes.

#figure(
  table(
    columns: (1fr, 0.35fr, 1.1fr, 0.35fr, 1.1fr),
    inset: 8pt,
    align: center,
    [leg names cell B], [`→`], [placement says node 2], [`→`], [client for node 2],
    [client], [`→`], [cell-local executor], [`→`], [named result rows],
  ),
  caption: [Routing converts explicit cell identity into an owner-specific execution client.],
)

== Problem 3: independent legs should not wait for one another

If cell A and cell B can execute independently, running A to completion before
starting B adds avoidable latency. The coordinator builds one future per leg
and awaits them together using `join_all`.

#srcblock("src/query/coordination.rs:2791-2845 (abridged)")[```rust
pub async fn execute_distributed_query_plan(
    &self,
    plan: DistributedQueryPlan,
) -> Result<DistributedQueryPlanResult> {
    // validate plan and resolve each leg's client ...
    for leg in plan.legs {
        let client = self.client_for_cell(&leg.context.cell_id)?;
        jobs.push(async move {
            let result = client
                .execute_cypher_rows(leg.context, leg.query.as_str())
                .await;
            (leg.name, result)
        });
    }

    for (leg_name, result) in join_all(jobs).await {
        leg_results.insert(leg_name, result?);
    }
    // apply the selected bounded merge ...
}
```]

Concurrency does not erase independence. Each leg retains its own cell ID,
query budget, failure, and read epoch. The coordinator waits for the set and
then merges successful results according to the plan.

The current behavior is fail-fast at collection: if a leg returns an error,
the distributed plan returns an error rather than silently presenting a
partial merged result as complete. This does not imply that already-started
remote work is transactionally rolled back; reads have no cross-node rollback
to perform.

== Problem 4: merging rows needs a deliberately small vocabulary

Once named results return, the coordinator needs to know what combination the
caller intended. turbolay currently supports `UNION ALL` and one two-leg inner
join description.

`UNION ALL` requires matching columns and appends rows in leg order. It does
not remove duplicates.

#figure(
  table(
    columns: (1.25fr, 1.35fr, 1.45fr),
    inset: 8pt,
    align: (left + top, left + top, left + top),
    table.header([*Merge*], [*Coordinator work*], [*Explicit limit*]),
    [`UNION ALL`], [Validate equal columns and append named leg rows], [No global distinct, order, or aggregate],
    [Inner join], [Hash/map the smaller side by one named column and combine matches], [One bounded coordinator-side join shape],
  ),
  caption: [A small merge vocabulary makes coordinator behavior inspectable.],
)

The inner join names left and right legs plus one column from each. The output
prefixes columns with their leg names. The plan may sort legs by
`estimated_rows`, but estimates affect work order rather than the join's
declared sides and columns.

#term("Coordinator merge")[
  Combination of already-materialized leg result sets at the process running
  the distributed plan. It is not repartitioned execution: intermediate rows
  are not redistributed among cell owners for additional graph work.
]

There is a current semantic limitation worth making explicit. The inner-join
implementation indexes `QueryValue` directly and therefore treats two
`QueryValue::Null` keys as equal. Cypher-style nulls ordinarily do not match in
an equality join. Until the implementation skips null build and probe keys,
callers should not use this coordinator join on nullable columns.

That limitation illustrates why the merge vocabulary should stay small until
each shape has precise semantics.

== Problem 5: per-cell snapshots do not create a global snapshot

Each query leg has a `QueryContext` and may carry its own `read_epoch`. The
client interface can fill an absent epoch using the cell's current epoch. This
preserves snapshot correctness *inside* each leg.

It does not make the epochs comparable across cells.

Consider:

```text
leg A -> cell A at epoch 120
leg B -> cell B at epoch 87
```

Those values name two cell histories. They do not prove that the reads
correspond to one simultaneous global moment. Even if both happen to be 120,
the equality is numeric coincidence unless another protocol assigned a shared
meaning.

#figure(
  table(
    columns: (1.2fr, 1.35fr, 1.45fr),
    inset: 8pt,
    align: (left + top, left + top, left + top),
    table.header([*Level*], [*Version guarantee*], [*Not implied*]),
    [One cell leg], [One pinned cell-local epoch], [Agreement with another cell's history],
    [Several explicit legs], [One epoch may be supplied for each leg], [A coordinator-negotiated global snapshot],
    [Merged result], [Rows are combined as returned by their legs], [Serializable execution across cell owners],
  ),
  caption: [Composition preserves each local guarantee but does not silently strengthen it.],
)

#boxeq[
  *A distributed result is a composition of named cell snapshots, not proof of
  one global snapshot.*
]

An application that needs cross-cell causal or snapshot rules must choose and
carry the required per-cell epochs or introduce a higher-level coordination
protocol.

== Problem 6: paging multiplies coordinator state

Returning every row from every cell in one response is not safe for large
scatter/gather queries. The coordinator therefore also supports page requests
per cell. Each request contains a query context and optional cursor; each cell
returns its own page and next cursor.

These cursors are independent. Cell A may be exhausted while cell B has more
pages. A higher-level streaming merge must track the cursor and pinned epoch
for every active leg.

The built page API returns a map of cell pages. It does not claim to implement
a globally ordered, streaming k-way merge. Global order, distinctness, and
aggregation would need explicit coordinator algorithms and memory limits.

== Problem 7: transport capability is not query-planning capability

With the `query-transport` feature, `TcpQueryCellClient` and the query server
carry requests between nodes. Optional features add TLS and service discovery.
The transport configuration supports bearer authentication, mTLS identities,
authorization grants, connection pooling, timeouts, and rotating TLS config.

Those are important production mechanisms, but they do not widen the logical
query contract.

#figure(
  table(
    columns: (1.3fr, 1.3fr, 1.4fr),
    inset: 8pt,
    align: (left + top, left + top, left + top),
    table.header([*Transport capability*], [*What it adds*], [*What it does not add*]),
    [TCP client/server], [Remote execution of a named cell request], [Automatic query decomposition],
    [TLS and authentication], [Confidentiality and caller identity], [A global snapshot],
    [Service discovery], [Node endpoint resolution], [Placement policy or failover correctness],
    [Connection pooling], [Lower repeated connection cost], [Distributed joins or aggregates],
  ),
  caption: [Moving a leg safely is separate from inventing or optimizing that leg.],
)

Service discovery can populate clients for placed nodes, but the placement
itself remains the control-plane answer to cell ownership. A stale or missing
directory entry fails routing; it does not authorize a different node to
serve the cell.

== Problem 8: reads distribute further than writes

The coordinator's query client can reach remote cell reads. Routed graph
writes remain constrained by Chapter 3's owner, lease, fence, lock, and
cell-local transaction.

There is no automatic remote write forwarding that turns a coordinator into a
multi-cell transaction manager. Sending two writes to two owners would still
produce two independent commits and two epochs. A failure between them can
leave one committed and the other absent.

This asymmetry is deliberate:

- a distributed read can merge independently computed rows;
- an atomic distributed write must coordinate commit, abort, recovery, and
  ownership across failure boundaries;
- turbolay implements the first and does not pretend to implement the second.

== The complete distributed query model

An explicit distributed plan follows this lifecycle:

1. The caller divides the request into named, supported cell-local Cypher legs.
2. Each leg carries a complete `QueryContext`, including its cell and optional
   read epoch.
3. Placement resolves each cell to its owner node.
4. The coordinator selects an in-process or transport-backed client.
5. All independent legs begin concurrently.
6. Each owner executes its leg using the local query engine from Chapter 5.
7. The coordinator fails if a required leg fails.
8. Successful named results are combined with `UNION ALL` or the bounded inner
   join shape.

#figure(
  table(
    columns: (1.15fr, 1.4fr, 1.45fr),
    inset: 8pt,
    align: (left + top, left + top, left + top),
    table.header([*Component*], [*Owns*], [*Must not assume*]),
    [Caller or embedding service], [Cell decomposition and intended merge], [That the kernel discovers partitioning],
    [Placement], [Cell-to-node ownership mapping], [That routing creates write authority],
    [Query client], [One remote or local leg execution], [That transport creates a global plan],
    [Cell executor], [Local snapshot and Cypher semantics], [That another cell shares its epoch],
    [Coordinator], [Concurrency and bounded result merge], [That materialized rows are a distributed transaction],
  ),
  caption: [Distribution is explicit ownership of responsibilities, not one magical planner.],
)

The central claim is:

#boxeq[
  *turbolay distributes known work: placement-aware cell legs execute
  concurrently, and the coordinator performs only the merge the plan names.*
]

== What distributed query support guarantees—and what it does not

The current implementation provides:

- explicit named cell legs;
- placement-aware selection of node clients;
- concurrent execution of independent legs;
- in-process and optional TCP-backed query clients;
- per-cell rows and pages;
- `UNION ALL` and one coordinator-side inner join shape;
- optional transport TLS, authentication, and service-discovery mechanisms.

It does not provide:

- automatic decomposition of arbitrary global Cypher;
- repartition joins, distributed aggregates, or global ordering;
- a coordinator-negotiated global snapshot;
- multi-cell atomic writes;
- correct equality-join semantics for nullable keys in the present inner-join
  implementation;
- a complete hosted-database control plane merely because transport exists.

== Revision notes

=== The ideas to remember

- *The caller supplies the legs.* Cell partitioning policy is explicit.
- *Placement routes; it does not plan.* It maps a named cell to its intended
  owner client.
- *Legs execute concurrently but remain independent.* Each has its own context,
  epoch, limits, and failure.
- *The merge vocabulary is bounded.* `UNION ALL` and a small inner join are not
  an arbitrary distributed relational engine.
- *Cell snapshots remain cell-local.* A set of pinned legs does not become a
  global epoch.
- *Transport moves work; it does not invent work.* TLS, discovery, and pooling
  do not expand query semantics.
- *Reads compose more easily than writes.* Multi-cell commit needs a protocol
  absent from this kernel.

=== A quick correctness test

1. Does every leg explicitly identify its cell and query context?
2. Does placement resolve exactly one intended owner client?
3. Can a failed leg be mistaken for an empty successful result?
4. Does the merge validate columns and named join inputs?
5. Are nullable join keys handled with the intended query semantics?
6. Are per-cell epochs being mislabeled as a global snapshot?
7. Does any transport feature accidentally imply multi-cell atomicity?

#boxeq[
  *A bounded distributed system is easier to trust because the edges of its
  promise are visible: explicit legs in, explicit cell results out, and no
  hidden global transaction between them.*
]
