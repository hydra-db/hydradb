//! Relationship-type probe: a rare edge type under a large label anchor.
//!
//! Reproduces a production shape — tens of thousands of
//! `:Entity` vertices and a handful of `:BRIDGE` edges between them, counted
//! with `MATCH (a:Entity)-[r:BRIDGE]->(b:Entity) RETURN count(*)`.
//!
//! The A/B is inside the experimental engine, not across engines. A bound
//! relationship (`-[r:BRIDGE]->`) plans `RelationshipTypeProbe`; an unbound one
//! (`-[:BRIDGE]->`) is a shape the planner declines to probe and plans
//! `ExpandExec`, which is exactly the plan the probe replaced. Same anchor, same
//! type, same direction, same answer, one planner difference. The legacy route
//! runs too, for context.
//!
//! Both plans are asserted before timing, so a rule change that stops emitting
//! the probe fails the run instead of quietly reporting no difference.
//!
//! Run with `cargo run --release --features
//! experimental-cypher-engine --example type_probe_bench -- <anchors>
//! <rare_edges> <iterations> [object-store-env-file]`. Defaults reproduce the
//! observed p50 anchor population against one edge.

use std::sync::Arc;
use std::time::Instant;

use hydradb::{
    object_store_from_env, CypherEngineMode, GraphCacheConfig, GraphOpenOptions, GraphShard, QueryContext, QueryValue, Result,
    VertexMetadata, VertexPropertyValue,
};
use slatedb::object_store::memory::InMemory;
use slatedb::object_store::ObjectStore;

const CELL: &str = "cell-a";
const EDGE_TYPE: &str = "BRIDGE";
/// Plans `RelationshipTypeProbe`: bare label anchor, one type, bound relationship.
const PROBED: &str = "MATCH (a:Entity)-[r:BRIDGE]->(b:Entity) RETURN count(*) AS n";
/// Plans `ExpandExec`: an unbound relationship is a shape the probe declines,
/// so this is the pre-probe plan measured under the same engine.
const EXPANDED: &str = "MATCH (a:Entity)-[:BRIDGE]->(b:Entity) RETURN count(*) AS n";

fn context(mode: CypherEngineMode) -> QueryContext {
    QueryContext::new(CELL, "type-probe-bench").with_cypher_engine(mode)
}

async fn plan_of(shard: &GraphShard, query: &str) -> Result<String> {
    let explain = shard
        .execute_cypher_rows(context(CypherEngineMode::Experimental), &format!("EXPLAIN {query}"))
        .await?;
    match &explain.rows[0].values[0] {
        QueryValue::Property(VertexPropertyValue::String(plan)) => Ok(plan.clone()),
        other => panic!("EXPLAIN returns plan text, got {other:?}"),
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let anchors = std::env::args().nth(1).and_then(|s| s.parse::<u64>().ok()).unwrap_or(20_000);
    let rare_edges = std::env::args().nth(2).and_then(|s| s.parse::<u64>().ok()).unwrap_or(1);
    let iterations = std::env::args().nth(3).and_then(|s| s.parse::<usize>().ok()).unwrap_or(5);
    assert!(anchors >= 2, "need at least two anchor vertices");
    assert!(
        rare_edges >= 1 && rare_edges <= anchors / 2,
        "rare_edges must fit the anchor population"
    );
    let store: Arc<dyn ObjectStore> = match std::env::args().nth(4) {
        Some(env_file) => object_store_from_env(Some(env_file))?,
        None => Arc::new(InMemory::new()),
    };
    let shard_path = format!(
        "bench/type-probe-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock before unix epoch")
            .as_millis()
    );
    let shard = GraphShard::open_standalone_writer(shard_path.as_str(), Arc::clone(&store)).await?;

    // Every anchor carries :Entity, so the label scan the probe replaces has the
    // whole population to sweep.
    shard
        .set_vertex_metadata_batch(
            CELL,
            (1..=anchors).map(|id| {
                (
                    id,
                    VertexMetadata::default()
                        .with_label("Entity")
                        .with_property("entity_id", VertexPropertyValue::String(format!("entity-{id}"))),
                )
            }),
        )
        .await?;
    // The rare edges. Sources are spread across the population rather than
    // clustered, so neither route gets a locality advantage the other lacks.
    let stride = anchors / (rare_edges * 2);
    shard
        .write_edges_batch_chunked(
            CELL,
            EDGE_TYPE,
            (0..rare_edges).map(|index| (1 + index * stride, 1 + index * stride + stride.max(1))),
            "type-probe-seed",
            1_000,
        )
        .await?;
    shard.close().await?;

    let options = GraphOpenOptions::default();
    let options = GraphOpenOptions {
        cache: GraphCacheConfig::disabled(),
        ..options
    };

    // Fail loudly rather than measure the wrong plans.
    let checker = GraphShard::open_with_options(shard_path.as_str(), Arc::clone(&store), options.clone()).await?;
    let probed_plan = plan_of(&checker, PROBED).await?;
    let expanded_plan = plan_of(&checker, EXPANDED).await?;
    assert!(
        probed_plan.contains("RelationshipTypeProbe"),
        "the probed route must plan the probe:\n{probed_plan}"
    );
    assert!(
        !expanded_plan.contains("RelationshipTypeProbe"),
        "the expand route must not plan the probe:\n{expanded_plan}"
    );
    assert!(
        expanded_plan.contains("VertexLabelScan"),
        "the expand route must sweep the anchor label:\n{expanded_plan}"
    );
    checker.close().await?;

    println!("anchors={anchors} rare_edges={rare_edges} iterations={iterations} cache=disabled reader=fresh");
    println!("route,iteration,elapsed_us,count");
    let mut expected: Option<QueryValue> = None;
    for iteration in 0..iterations {
        for (name, query, mode) in [
            ("probe", PROBED, CypherEngineMode::Experimental),
            ("expand", EXPANDED, CypherEngineMode::Experimental),
            ("legacy", PROBED, CypherEngineMode::Legacy),
        ] {
            let reader = GraphShard::open_with_options(shard_path.as_str(), Arc::clone(&store), options.clone()).await?;
            let started = Instant::now();
            let result = reader.execute_cypher_rows(context(mode), query).await?;
            let elapsed = started.elapsed();
            let count = result.rows[0].values[0].clone();
            println!("{name},{iteration},{},{count:?}", elapsed.as_micros());
            // Every route must agree, or the fast one is fast for the wrong reason.
            match &expected {
                Some(expected) => assert_eq!(&count, expected, "{name} disagrees on the count"),
                None => expected = Some(count),
            }
            reader.close().await?;
        }
    }
    Ok(())
}
