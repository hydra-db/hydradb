//! Queries that do not open with MATCH: a bare RETURN, an opening WITH, and
//! an opening OPTIONAL MATCH.
//!
//! All three read `SingleRow`, the one row that binds nothing. The matrix
//! rows `compat_return_without_match`, `compat_return_parameter_without_match`
//! and `compat_initial_optional_match_*` hold the same answers to Neo4j.

mod common;

use common::compat::{columns, refusal, run, run_with, table};
use hydradb_cypher_engine::{ListParameterMap, ParameterMap, ScalarValue};

#[tokio::test]
async fn a_bare_return_projects_one_row() {
    let result = run("RETURN 1 AS one, 'x' AS letter, null AS nothing").await;
    assert_eq!(columns(&result), ["one", "letter", "nothing"]);
    assert_eq!(table(&result), rows![["1", "\"x\"", "null"]]);
}

#[tokio::test]
async fn a_bare_return_reads_parameters() {
    let parameters = ParameterMap::from([("value".to_string(), ScalarValue::Integer(41))]);
    let result = run_with("RETURN $value AS value, $value + 1 AS next", &parameters, &ListParameterMap::new()).await;
    assert_eq!(table(&result), rows![["41", "42"]]);
}

#[tokio::test]
async fn a_bare_return_may_aggregate_its_one_row() {
    let result = run("RETURN count(*) AS n, sum(2) AS total").await;
    assert_eq!(table(&result), rows![["1", "2"]]);
}

#[tokio::test]
async fn an_opening_with_binds_values_the_rest_reads() {
    let result = run("WITH 2 AS two, 'a' AS name MATCH (n:Node {name: name}) RETURN n.name AS name, two").await;
    assert_eq!(table(&result), rows![["\"a\"", "2"]]);
}

#[tokio::test]
async fn an_opening_with_may_filter_its_one_row_away() {
    let result = run("WITH 1 AS x WHERE x > 5 RETURN x").await;
    assert!(table(&result).is_empty());
}

#[tokio::test]
async fn an_opening_optional_match_that_misses_yields_one_null_row() {
    let result = run("OPTIONAL MATCH (n:Missing) RETURN n.name AS name").await;
    assert_eq!(table(&result), rows![["null"]]);
}

#[tokio::test]
async fn an_opening_optional_match_that_hits_yields_the_matches() {
    let result = run("OPTIONAL MATCH (n:Node) WHERE n.name < 'c' RETURN n.name AS name ORDER BY name ASC").await;
    assert_eq!(table(&result), rows![["\"a\""], ["\"b\""]]);
}

#[tokio::test]
async fn counting_an_opening_optional_miss_counts_the_row_not_the_binding() {
    let result = run("OPTIONAL MATCH (n:Missing) RETURN count(n) AS bound, count(*) AS rows").await;
    assert_eq!(table(&result), rows![["0", "1"]]);
}

#[tokio::test]
async fn two_opening_optional_misses_still_yield_one_row() {
    let result = run("OPTIONAL MATCH (n:Missing) OPTIONAL MATCH (m:AlsoMissing) RETURN n.name AS n, m.name AS m").await;
    assert_eq!(table(&result), rows![["null", "null"]]);
}

#[tokio::test]
async fn a_null_from_an_opening_optional_eliminates_a_later_required_match() {
    // `n` is null, not absent: the required MATCH on it matches nothing.
    let result = run("OPTIONAL MATCH (n:Missing) MATCH (n)-[:R]->(m) RETURN m.name AS name").await;
    assert!(table(&result).is_empty());
}

#[tokio::test]
async fn a_bare_return_of_an_unbound_name_is_refused() {
    assert!(refusal("RETURN missing.name AS name")
        .await
        .contains("out-of-scope binding missing"));
}

#[tokio::test]
async fn nested_count_star_reads_no_binding() {
    assert_eq!(
        table(&run("RETURN count(*) + 1 AS n, coalesce(count(*), 0) AS count").await),
        rows![["2", "1"]]
    );
}

#[tokio::test]
async fn initial_optional_cannot_read_an_undeclared_name() {
    assert!(refusal("OPTIONAL MATCH (a:Missing) RETURN b.name AS name")
        .await
        .contains("out-of-scope binding b"));
    assert!(refusal("RETURN count(*) + missing AS n")
        .await
        .contains("out-of-scope binding missing"));
}
