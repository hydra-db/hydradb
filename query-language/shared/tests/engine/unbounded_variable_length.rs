//! Unbounded variable-length patterns: `*`, `*2..`.
//!
//! Cypher sets no maximum. A walk still ends, because a path never reuses a
//! relationship; how much work it may do is the query's row budgets' to say,
//! and a walk that exceeds them fails rather than answering short. The matrix
//! row `compat_varlen_unbounded` holds Neo4j's answer.

mod common;

use common::compat::{graph, run, table};
use hydradb_cypher_engine::{CypherEngine, ParameterMap};

#[tokio::test]
async fn an_unbounded_walk_counts_every_trail() {
    // 8 paths up to 4 hops around the diamond-and-cycle, and 2 of 5.
    let result = run("MATCH (a:Node {name: 'a'})-[:R*]->(b) RETURN count(*) AS paths").await;
    assert_eq!(table(&result), rows![["10"]]);
}

#[tokio::test]
async fn a_minimum_alone_leaves_the_maximum_open() {
    let result = run("MATCH (a:Node {name: 'a'})-[rs:R*4..]->(b) RETURN size(rs) AS hops, count(*) AS paths ORDER BY hops ASC").await;
    assert_eq!(table(&result), rows![["4", "2"], ["5", "2"]]);
}

#[tokio::test]
async fn a_maximum_alone_starts_at_one() {
    let result = run("MATCH (a:Node {name: 'a'})-[:R*..2]->(b) RETURN count(*) AS paths").await;
    assert_eq!(table(&result), rows![["4"]]);
}

#[tokio::test]
async fn an_unbounded_undirected_walk_ends_too() {
    let result = run("MATCH (n:Node {name: 'loop'})-[:R*]-(m) RETURN count(*) AS paths").await;
    assert_eq!(table(&result), rows![["1"]]);
}

#[tokio::test]
async fn a_walk_past_the_row_budget_fails_instead_of_answering_short() {
    let mut storage = graph();
    storage.max_intermediate_rows = Some(5);
    let error = CypherEngine::new(storage)
        .execute(
            "MATCH (a:Node {name: 'a'})-[:R*]->(b) RETURN count(*) AS paths",
            &ParameterMap::new(),
        )
        .await
        .expect_err("ten paths exceed a budget of five rows");
    assert!(error.to_string().contains("intermediate"), "{error}");
}

#[tokio::test]
async fn long_narrow_walk_reaches_the_target_without_truncating() {
    use hydradb_cypher_engine::{explain_physical_plan_shape, CypherEngine, ParameterMap, RelationshipRecord, VertexRecord};
    let mut storage = common::MemoryStorage::with_graph(
        (0..=1000).map(|id| {
            VertexRecord::new(id).with_label(if id == 0 {
                "Start"
            } else if id == 1000 {
                "End"
            } else {
                "Middle"
            })
        }),
        (0..1000).map(|id| RelationshipRecord::new(id, id, id + 1, "R")),
    );
    storage.max_intermediate_rows = Some(2);
    let engine = CypherEngine::new(storage);
    let plan = engine
        .prepare("MATCH (a:Start)-[rs:R*]->(b:End) RETURN size(rs) AS hops", &ParameterMap::new())
        .unwrap();
    assert!(plan.explain().contains("hops=1.."), "{}", plan.explain());
    assert!(explain_physical_plan_shape(&plan.physical).contains("hops=?..)"));
    assert_eq!(table(&engine.execute_prepared(&plan).await.unwrap()), rows![["1000"]]);
}
