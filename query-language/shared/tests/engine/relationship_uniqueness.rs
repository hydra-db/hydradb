//! No relationship is used twice within one MATCH clause.
//!
//! Cypher's default match mode (GQL's DIFFERENT RELATIONSHIPS) is
//! relationship isomorphism. The matrix rows `compat_*_used_once` hold
//! Neo4j's answers.

mod common;

use common::compat::{run, table};

#[tokio::test]
async fn a_self_loop_is_not_both_hops_of_a_path() {
    let result = run("MATCH (x:Node {name: 'loop'})-[:R]->(y)-[:R]->(z) RETURN count(*) AS n").await;
    assert_eq!(table(&result), rows![["0"]]);
}

#[tokio::test]
async fn uniqueness_spans_the_patterns_of_a_clause() {
    let result = run("MATCH (x:Node {name: 'loop'})-[:R]->(y), (y)-[:R]->(z) RETURN count(*) AS n").await;
    assert_eq!(table(&result), rows![["0"]]);
}

#[tokio::test]
async fn an_undirected_hop_is_not_walked_back() {
    let result = run("MATCH (a:Node {name: 'a'})-[:R]-(b)-[:R]-(c) WHERE c = a RETURN count(*) AS n").await;
    assert_eq!(table(&result), rows![["0"]]);
}

#[tokio::test]
async fn distinct_relationships_still_chain() {
    let result = run("MATCH (a:Node {name: 'a'})-[:R]->(b)-[:R]->(c) RETURN c.name AS name ORDER BY name ASC").await;
    assert_eq!(table(&result), rows![["\"d\""], ["\"d\""]]);
}

#[tokio::test]
async fn separate_match_clauses_may_reuse_a_relationship() {
    // Uniqueness is per clause: two clauses may both walk the self-loop.
    let result = run("MATCH (x:Node {name: 'loop'})-[:R]->(y) MATCH (y)-[:R]->(z) RETURN count(*) AS n").await;
    assert_eq!(table(&result), rows![["1"]]);
}

#[tokio::test]
async fn a_relationship_named_twice_is_one_relationship() {
    let result = run("MATCH (x)-[r:R]->(y), (x)-[r]->(y) WHERE x.name = 'a' RETURN count(*) AS n").await;
    assert_eq!(table(&result), rows![["2"]]);
}

#[tokio::test]
async fn fixed_and_variable_hops_cannot_reuse_a_relationship() {
    for query in [
        "MATCH (a:Node {name:'loop'})-[rs:R*1..1]->(b)-[r:R]->(c) RETURN count(*) AS n",
        "MATCH (a:Node {name:'loop'})-[:R]->(b)-[:R*1..1]->(c) RETURN count(*) AS n",
        "MATCH (a:Node {name:'loop'})-[rs:R*1..1]->(b), (b)-[other:R*1..1]->(c) RETURN count(*) AS n",
    ] {
        assert_eq!(table(&run(query).await), rows![["0"]], "{query}");
    }
    assert_eq!(
        table(&run("MATCH (a:Node {name:'loop'})-[rs:R*1..1]->(b) MATCH (b)-[r:R]->(c) RETURN count(*) AS n").await),
        rows![["1"]]
    );
}
