//! A pattern naming one node at both ends of a hop: `(n)-[:R]->(n)`.
//!
//! The matrix row `compat_self_loop` holds the shared answer to Neo4j.

mod common;

use common::compat::{run, table};

#[tokio::test]
async fn a_repeated_adjacent_node_matches_self_loops() {
    let result = run("MATCH (n)-[:R]->(n) RETURN n.name AS name").await;
    assert_eq!(table(&result), rows![["\"loop\""]]);
}

#[tokio::test]
async fn labels_properties_and_a_relationship_name_still_apply() {
    let result = run("MATCH (n:Node {name: 'loop'})-[r:R]->(n) RETURN n.name AS name, r IS NULL AS missing").await;
    assert_eq!(table(&result), rows![["\"loop\"", "false"]]);
    let none = run("MATCH (n:Item)-[:R]->(n) RETURN n.name AS name").await;
    assert!(table(&none).is_empty());
}

#[tokio::test]
async fn the_path_continues_from_the_repeated_node() {
    // a's only incoming R is from d, which has no self-loop.
    let result = run("MATCH (n:Node {name: 'a'})<-[:R]-(m)-[:R]->(m) RETURN m.name AS name").await;
    assert!(table(&result).is_empty());
}

#[tokio::test]
async fn a_node_predicate_on_the_repeated_node_reads_it() {
    let result = run("MATCH (n)-[:R]->(n WHERE n.name = 'loop') RETURN n.name AS name").await;
    assert_eq!(table(&result), rows![["\"loop\""]]);
}

#[tokio::test]
async fn repeated_variable_endpoint_matches_cycle_trails() {
    assert_eq!(
        table(&run("MATCH (n:Node {name:'a'})-[:R*1..3]->(n) RETURN count(*) AS n").await),
        rows![["2"]]
    );
    assert!(table(&run("MATCH (n:Item)-[:R*1..3]->(n) RETURN n").await).is_empty());
}

#[tokio::test]
async fn repeated_variable_endpoint_zero_hop_preserves_node_constraints() {
    assert_eq!(
        table(&run("MATCH (n:Node {name:'a'})-[:R*0..0]->(n {name:'a'}) RETURN count(*) AS n").await),
        rows![["1"]]
    );
    assert!(table(&run("MATCH (n:Node {name:'a'})-[:R*0..0]->(n {name:'b'}) RETURN n").await).is_empty());
}
