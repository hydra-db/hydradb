//! `WITH *` and `RETURN *`.
//!
//! The star stands for every name the query wrote that is in scope, in name
//! order. The matrix rows `return_star`, `compat_return_star_pattern`,
//! `compat_with_star` and `compat_with_star_and_alias` hold the same answers
//! to Neo4j.

mod common;

use common::compat::{columns, refusal, run, table};

#[tokio::test]
async fn return_star_returns_every_written_binding_in_name_order() {
    let result = run("MATCH (i:Item {name: 'banana'})<-[:HAS]-(h:Hub) RETURN *").await;
    assert_eq!(columns(&result), ["h", "i"]);
    assert_eq!(table(&result), rows![["#1010", "#1001"]]);
}

#[tokio::test]
async fn return_star_leaves_out_anonymous_elements_but_keeps_named_relationships() {
    let result = run("MATCH (h:Hub)-[r:HAS]->(:Item {name: 'banana'}) RETURN *").await;
    assert_eq!(columns(&result), ["h", "r"]);
}

#[tokio::test]
async fn return_star_may_add_items() {
    let result = run("MATCH (i:Item {name: 'banana'}) RETURN *, i.score AS score").await;
    assert_eq!(columns(&result), ["i", "score"]);
    assert_eq!(table(&result), rows![["#1001", "1"]]);
}

#[tokio::test]
async fn with_star_carries_everything() {
    let result = run("MATCH (i:Item {name: 'banana'}) WITH * RETURN i.score AS score").await;
    assert_eq!(table(&result), rows![["1"]]);
}

#[tokio::test]
async fn with_star_may_add_computed_items() {
    let result = run("MATCH (i:Item {name: 'banana'}) WITH *, i.score * 10 AS scaled RETURN i.name AS name, scaled").await;
    assert_eq!(table(&result), rows![["\"banana\"", "10"]]);
}

#[tokio::test]
async fn with_star_keeps_a_value_a_value() {
    assert!(refusal("UNWIND [1] AS x WITH * MATCH (x)-[:R]->(y) RETURN y")
        .await
        .contains("holds a value"));
}

#[tokio::test]
async fn with_star_with_nothing_in_scope_is_one_row() {
    let result = run("WITH * RETURN 1 AS one").await;
    assert_eq!(table(&result), rows![["1"]]);
}

#[tokio::test]
async fn return_star_with_nothing_in_scope_is_refused() {
    assert!(refusal("RETURN *").await.contains("no variables in scope"));
}

#[tokio::test]
async fn return_star_after_with_returns_only_what_with_kept() {
    let result = run("MATCH (h:Hub)-[:HAS]->(i:Item) WITH h, count(i) AS items RETURN *").await;
    assert_eq!(columns(&result), ["h", "items"]);
    assert_eq!(table(&result), rows![["#1010", "3"]]);
}

#[tokio::test]
async fn extra_projection_does_not_make_empty_return_star_valid() {
    assert!(refusal("RETURN *, 1 AS answer").await.contains("no variables in scope"));
    assert!(refusal("WITH * RETURN *, 1 AS answer").await.contains("no variables in scope"));
    assert_eq!(table(&run("WITH 2 AS x RETURN *, 1 AS answer").await), rows![["2", "1"]]);
}
