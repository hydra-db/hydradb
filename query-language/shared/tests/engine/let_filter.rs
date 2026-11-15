//! GQL's LET and FILTER, as Neo4j accepts them under CYPHER 25.
//!
//! Each case was probed on Neo4j 2026.09; the matrix row `compat_let_filter`
//! holds the shared answer to Neo4j.

mod common;

use common::compat::{refusal, run, table};

#[tokio::test]
async fn let_binds_and_filter_keeps() {
    let result =
        run("MATCH (i:Item) LET doubled = i.score * 2 FILTER doubled > 4 RETURN i.name AS name, doubled ORDER BY doubled ASC").await;
    assert_eq!(table(&result), rows![["\"apple\"", "6"], ["null", "10"]]);
}

#[tokio::test]
async fn filter_where_and_filter_mean_the_same() {
    let plain = run("UNWIND [1, 2, 3] AS x FILTER x > 1 RETURN x").await;
    let spelled = run("UNWIND [1, 2, 3] AS x FILTER WHERE x > 1 RETURN x").await;
    assert_eq!(table(&plain), rows![["2"], ["3"]]);
    assert_eq!(table(&plain), table(&spelled));
}

#[tokio::test]
async fn let_may_open_a_query_and_bind_several_names() {
    let result = run("LET a = 1, b = 'two' RETURN a, b").await;
    assert_eq!(table(&result), rows![["1", "\"two\""]]);
}

#[tokio::test]
async fn a_let_name_reads_like_any_value_later() {
    let result =
        run("MATCH (n:Node) LET first = 'a' MATCH (n)-[:R]->(m) WHERE n.name = first RETURN m.name AS name ORDER BY name ASC").await;
    assert_eq!(table(&result), rows![["\"b\""], ["\"c\""]]);
}

#[tokio::test]
async fn filter_then_aggregate() {
    let result = run("UNWIND [1, 2, 3] AS x FILTER x > 1 RETURN count(*) AS n").await;
    assert_eq!(table(&result), rows![["2"]]);
}

#[tokio::test]
async fn let_bindings_cannot_read_each_other() {
    assert!(refusal("LET a = 1, b = a + 1 RETURN b").await.contains("out-of-scope binding a"));
}

#[tokio::test]
async fn let_cannot_rebind_a_name_in_scope() {
    assert!(refusal("UNWIND [1, 2] AS x LET x = 5 RETURN x").await.contains("more than once"));
}

#[tokio::test]
async fn let_and_filter_cannot_aggregate() {
    assert!(refusal("UNWIND [1, 2] AS x LET c = count(*) RETURN c")
        .await
        .contains("LET cannot aggregate"));
    // A FILTER aggregates only horizontally, over a group variable (GE09).
    assert!(refusal("UNWIND [1, 2] AS x FILTER count(*) > 1 RETURN x")
        .await
        .contains("reads exactly one group variable"));
}

#[tokio::test]
async fn filter_preserves_the_property_seek_access_path() {
    use hydradb_cypher_engine::{CypherEngine, ParameterMap};
    let engine = CypherEngine::new(common::compat::graph());
    let parameters = ParameterMap::new();
    let where_plan = engine
        .prepare("MATCH (i:Item) WHERE i.score = 2 RETURN i.name AS name", &parameters)
        .unwrap();
    let filter_plan = engine
        .prepare("MATCH (i:Item) FILTER i.score = 2 RETURN i.name AS name", &parameters)
        .unwrap();
    assert!(filter_plan.explain().contains("access=property-point"), "{}", filter_plan.explain());
    assert_eq!(where_plan.explain(), filter_plan.explain());
    for (query, operation) in [
        ("MATCH (i:Item) WHERE 1 / 0 = 0 RETURN i", hydradb_cypher_ast::operation::MATCH),
        ("MATCH (i:Item) FILTER 1 / 0 = 0 RETURN i", hydradb_cypher_ast::operation::FILTER),
    ] {
        let error = engine.execute(query, &parameters).await.unwrap_err();
        assert_eq!(error.operation(), operation);
        assert_eq!(error.gql_status(), hydradb_cypher_ast::GqlStatus::DIVISION_BY_ZERO);
    }
    assert_eq!(table(&engine.execute_prepared(&filter_plan).await.unwrap()), rows![["\"carrot\""]]);
}
