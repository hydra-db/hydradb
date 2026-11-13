//! A lookup whose selective equality sits in `WHERE`, on a cell many tenants
//! share.
//!
//! The source-resolution query names four equalities on one binding: the
//! tenant pair inline in the pattern, and the provider pair in `WHERE`. Only
//! `app_external_id` is selective -- it names one vertex -- but `tenant_id`
//! holds every row in the cell.
//!
//! The two clauses plan as two logical filters over one access, and a stack of
//! filters hides the outer predicate from `probe_replaceable_access`, which
//! looks through a filter only to a bare access. The selective equality
//! therefore never reached the probe and the query stayed anchored on
//! `tenant_id`, walking the whole cell on every execution -- multiple seconds
//! for a single-row answer.
//!
//! Cost is read from `query_property_fetches`, which both engines increment as
//! they load vertex metadata, so the two routes are measured on one number
//! rather than on wall time.

use std::sync::Arc;

use slatedb::object_store::memory::InMemory;

use crate::{CypherEngineMode, GraphLimits, GraphShard, QueryContext, QueryValue, VertexMetadata, VertexPropertyValue};

const CELL: &str = "cell-a";
/// Every source in the cell shares the tenant pair, so anchoring on it is a
/// full walk. Well past the probe's per-candidate allowance.
const TENANT_ROWS: u64 = 4_000;
const PROVIDERS: u64 = 4;

fn source(external_id: &str, provider: &str) -> VertexMetadata {
    VertexMetadata::default()
        .with_label("Source")
        .with_property("tenant_id", VertexPropertyValue::String("t".to_string()))
        .with_property("sub_tenant_id", VertexPropertyValue::String("s".to_string()))
        .with_property("app_provider", VertexPropertyValue::String(provider.to_string()))
        .with_property("app_external_id", VertexPropertyValue::String(external_id.to_string()))
}

async fn fixture() -> GraphShard {
    let shard = GraphShard::open_standalone_writer_with_limits(
        "graph/stacked-filter-probe".to_string(),
        Arc::new(InMemory::new()),
        GraphLimits::default(),
    )
    .await
    .expect("open graph shard");

    let rows = (0..TENANT_ROWS)
        .map(|i| (1 + i, source(&format!("ext-{i:05}"), &format!("p{}", i % PROVIDERS))))
        .collect::<Vec<_>>();
    shard.set_vertex_metadata_batch(CELL, rows).await.expect("seed the shared cell");
    shard
}

/// The shape of a `resolveSourceQuery` client query: the tenant pair
/// inline, the provider pair in `WHERE`.
fn resolve_query(provider: &str, external_id: &str) -> String {
    format!(
        "MATCH (s:Source {{tenant_id: 't', sub_tenant_id: 's'}}) \
         WHERE s.app_provider = '{provider}' AND s.app_external_id = '{external_id}' \
         RETURN s.app_external_id AS external_id"
    )
}

async fn run(shard: &GraphShard, engine: CypherEngineMode, query: &str) -> (Vec<String>, u64) {
    let before = shard.graph_operational_metrics().query_property_fetches;
    let result = shard
        .execute_cypher_rows(QueryContext::new(CELL, "stacked-filter-probe").with_cypher_engine(engine), query)
        .await
        .unwrap_or_else(|error| panic!("{engine:?}: {query}: {error}"));
    let fetched = shard.graph_operational_metrics().query_property_fetches - before;
    let ids = result
        .rows
        .iter()
        .map(|row| match &row.values[0] {
            QueryValue::Property(VertexPropertyValue::String(id)) => id.clone(),
            other => panic!("unexpected external_id: {other:?}"),
        })
        .collect();
    (ids, fetched)
}

/// Loaded vertices a single-row answer may cost. The probe returns the one
/// vertex `app_external_id` names; the allowance leaves room for a small
/// bounded overhead without admitting a walk of the tenant.
const FETCH_BUDGET: u64 = 64;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_selective_where_equality_reaches_the_probe() {
    let shard = fixture().await;
    let mut over_budget = Vec::new();
    let mut report = Vec::new();

    // A hit in each provider slice, and a miss, whose right answer is empty
    // and whose walk is the same length.
    let mut cases = (0..PROVIDERS)
        .map(|i| {
            let id = i * 37 + 11;
            (format!("p{}", id % PROVIDERS), format!("ext-{id:05}"), vec![format!("ext-{id:05}")])
        })
        .collect::<Vec<_>>();
    cases.push(("p0".to_string(), "ext-99999".to_string(), Vec::new()));

    for (provider, external_id, expected) in cases {
        let query = resolve_query(&provider, &external_id);
        let (legacy_rows, legacy_fetched) = run(&shard, CypherEngineMode::Legacy, &query).await;
        let (rows, fetched) = run(&shard, CypherEngineMode::Experimental, &query).await;

        assert_eq!(rows, legacy_rows, "{external_id}: engines disagree");
        assert_eq!(rows, expected, "{external_id}: wrong rows");
        report.push(format!(
            "{external_id:<10} exp fetched={fetched:>5}  legacy fetched={legacy_fetched:>5}"
        ));
        if fetched > FETCH_BUDGET {
            over_budget.push(format!("{external_id}: {fetched}"));
        }
    }

    for line in &report {
        println!("{line}");
    }
    assert!(
        over_budget.is_empty(),
        "a selective WHERE equality still walked the tenant, \
         budget {FETCH_BUDGET} loaded vertices: {over_budget:?}\n{}",
        report.join("\n")
    );
}
