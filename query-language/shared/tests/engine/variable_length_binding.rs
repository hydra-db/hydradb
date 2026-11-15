//! A variable-length relationship bound to a name: `[rs:R*1..3]`.
//!
//! The name holds the path's relationships, in walk order, as a list. The
//! matrix row `compat_varlen_relationship_binding` holds Neo4j's answer.

mod common;

use common::compat::{run, table};

#[tokio::test]
async fn the_name_holds_one_relationship_per_hop() {
    let result = run("MATCH (a:Node {name: 'a'})-[rs:R*2..2]->(d) RETURN d.name AS name, size(rs) AS hops ORDER BY name ASC").await;
    assert_eq!(table(&result), rows![["\"d\"", "2"], ["\"d\"", "2"]]);
}

#[tokio::test]
async fn hop_counts_vary_with_the_path() {
    let result = run("MATCH (a:Node {name: 'a'})-[rs:R*1..3]->(x) RETURN size(rs) AS hops, count(*) AS paths ORDER BY hops ASC").await;
    assert_eq!(table(&result), rows![["1", "2"], ["2", "2"], ["3", "2"]]);
}

#[tokio::test]
async fn a_zero_length_path_binds_an_empty_list() {
    let result = run("MATCH (a:Node {name: 'a'})-[rs:R*0..1]->(x) WHERE x.name = 'a' RETURN size(rs) AS hops").await;
    assert_eq!(table(&result), rows![["0"]]);
}

#[tokio::test]
async fn distinct_paths_bind_distinct_lists() {
    let result = run("MATCH (a:Node {name: 'a'})-[rs:R*2..2]->(d) RETURN count(DISTINCT rs) AS paths").await;
    assert_eq!(table(&result), rows![["2"]]);
}

#[tokio::test]
async fn an_optional_miss_leaves_the_name_null() {
    let result = run("MATCH (h:Hub) OPTIONAL MATCH (h)-[rs:R*1..2]->(x) RETURN h.name AS hub, rs").await;
    assert_eq!(table(&result), rows![["\"hub\"", "null"]]);
}

#[tokio::test]
async fn relationships_unwind_with_their_properties_and_survive_aliases() {
    assert_eq!(
        table(&run("MATCH (h:Hub)-[rs:HAS*1..1]->(i) WITH rs AS edges UNWIND edges AS r RETURN r.weight AS weight ORDER BY weight").await),
        rows![["1"], ["2"], ["3"]]
    );
    assert_eq!(
        table(&run("MATCH (a:Node {name:'a'})-[rs:R*1..1]->(b) UNWIND rs AS r RETURN count(r) AS n").await),
        rows![["2"]]
    );
    let result = run("MATCH (a:Node {name:'a'})-[rs:R*1..1]->(b) UNWIND rs AS r RETURN r").await;
    assert_eq!(table(&result), rows![["rel#4"], ["rel#5"]]);
}

#[tokio::test]
async fn the_relationship_list_can_be_indexed() {
    let result = run("MATCH (a:Node {name:'a'})-[rs:R*1..1]->(b) RETURN rs[0] AS first, rs[-1] AS last, rs[1] AS missing").await;
    assert_eq!(table(&result), rows![["rel#4", "rel#4", "null"], ["rel#5", "rel#5", "null"]]);
}

#[tokio::test]
async fn reused_path_binding_must_match_the_existing_ordered_list() {
    assert_eq!(
        table(&run("MATCH (a:Node {name:'a'})-[rs:R*1..1]->(b) MATCH (d:Node {name:'d'})-[rs:R*1..1]->(e) RETURN count(*) AS n").await),
        rows![["0"]]
    );
    assert_eq!(
        table(&run("MATCH (a:Node {name:'a'})-[rs:R*3..3]->(b) MATCH (d:Node {name:'a'})-[rs:R*1..3]->(e) RETURN count(*) AS n").await),
        rows![["2"]]
    );
}

#[tokio::test]
async fn optional_miss_preserves_an_existing_path_list() {
    assert_eq!(table(&run("MATCH (h:Hub)-[rs:HAS*1..1]->(i) OPTIONAL MATCH (h)-[rs:R*1..1]->(x) WITH rs, x RETURN size(rs) AS hops, x IS NULL AS missing").await), rows![["1", "true"], ["1", "true"], ["1", "true"]]);
}

#[tokio::test]
async fn computed_nested_and_collected_lists_retain_relationship_records() {
    for query in [
        "MATCH (h:Hub)-[rs:HAS*1..1]->(i) WITH [rs] AS paths UNWIND paths AS edges UNWIND edges AS r RETURN r.weight AS weight ORDER BY weight",
        "MATCH (h:Hub)-[rs:HAS*1..1]->(i) WITH [rs] AS paths UNWIND paths AS edges WITH edges AS kept UNWIND kept AS r RETURN r.weight AS weight ORDER BY weight",
        "MATCH (h:Hub)-[r:HAS]->(i) WITH collect(r) AS edges UNWIND edges AS r RETURN r.weight AS weight ORDER BY weight",
        "MATCH (h:Hub)-[rs:HAS*1..1]->(i) WITH coalesce(rs, []) AS edges UNWIND edges AS r RETURN r.weight AS weight ORDER BY weight",
    ] { assert_eq!(table(&run(query).await), rows![["1"], ["2"], ["3"]], "{query}"); }
}
