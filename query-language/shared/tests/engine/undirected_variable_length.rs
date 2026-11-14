//! Undirected variable-length patterns: `-[:R*1..3]-`.
//!
//! One traversal over both orientations with trail semantics keyed on
//! relationship identity. Expectations follow Neo4j 2026.09; the matrix rows
//! `compat_varlen_undirected` and `undirected_variable_length` hold them to
//! Neo4j.

mod common;

use common::compat::{run, table};

#[tokio::test]
async fn both_orientations_compound_at_every_hop() {
    let result = run("MATCH (a:Node {name: 'b'})-[:R*1..2]-(x) RETURN x.name AS name ORDER BY name ASC").await;
    assert_eq!(
        table(&result),
        rows![["\"a\""], ["\"a\""], ["\"c\""], ["\"c\""], ["\"d\""], ["\"d\""]]
    );
}

#[tokio::test]
async fn a_path_may_return_to_its_start_over_different_relationships() {
    // b-a-d-b: a->b walked back, d->a walked back, b->d walked back.
    let result = run("MATCH (b:Node {name: 'b'})-[:R*3..3]-(x) WHERE x.name = 'b' RETURN count(*) AS n").await;
    assert_eq!(table(&result), rows![["2"]]);
}

#[tokio::test]
async fn an_undirected_self_loop_is_one_step() {
    let result = run("MATCH (n:Node {name: 'loop'})-[:R*1..2]-(m) RETURN m.name AS name").await;
    assert_eq!(table(&result), rows![["\"loop\""]]);
}

#[tokio::test]
async fn an_undirected_walk_binds_its_relationships() {
    let result = run("MATCH (a:Node {name: 'b'})-[rs:R*2..2]-(x) RETURN size(rs) AS hops, count(*) AS paths").await;
    assert_eq!(table(&result), rows![["2", "4"]]);
}

#[tokio::test]
async fn merged_directions_share_one_candidate_budget() {
    use hydradb_cypher_engine::{CypherEngine, ParameterMap, RelationshipRecord, ScalarValue, VertexRecord};
    let vertex = |id| {
        VertexRecord::new(id)
            .with_label("Node")
            .with_property("name", ScalarValue::String(format!("n{id}").into()))
    };
    let mut storage = common::MemoryStorage::with_graph(
        [vertex(1), vertex(2)],
        [RelationshipRecord::new(1, 1, 2, "R"), RelationshipRecord::new(2, 2, 1, "R")],
    );
    storage.max_intermediate_rows = Some(1);
    let engine = CypherEngine::new(storage);
    let plan = engine
        .prepare(
            "MATCH (a:Node {name:'n1'})-[:R*1..1]-(b) RETURN count(*) AS n",
            &ParameterMap::new(),
        )
        .unwrap();
    assert!(engine
        .execute_prepared(&plan)
        .await
        .unwrap_err()
        .to_string()
        .contains("cypher_variable_expand_candidates"));
}

#[tokio::test]
async fn self_loop_deduplication_does_not_double_charge_the_budget() {
    use hydradb_cypher_engine::{CypherEngine, ParameterMap, RelationshipRecord, ScalarValue, VertexRecord};
    let mut storage = common::MemoryStorage::with_graph(
        [VertexRecord::new(1)
            .with_label("Node")
            .with_property("name", ScalarValue::String("n1".into()))],
        [RelationshipRecord::new(1, 1, 1, "R")],
    );
    storage.max_intermediate_rows = Some(1);
    let engine = CypherEngine::new(storage);
    let plan = engine
        .prepare(
            "MATCH (a:Node {name:'n1'})-[:R*1..1]-(b) RETURN count(*) AS n",
            &ParameterMap::new(),
        )
        .unwrap();
    assert_eq!(table(&engine.execute_prepared(&plan).await.unwrap()), rows![["1"]]);
}
