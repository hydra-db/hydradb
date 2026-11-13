//! Scalar functions: calls that read one row and return one value.
//!
//! The parity matrix's `compat_*` function rows pin the same answers on a real
//! shard and hold them to Neo4j. These tests pin the semantics underneath:
//! null handling, composition, and that a scalar call never makes a projection
//! group the way an aggregate does.

mod common;

use common::compat::{columns, refusal, run, table};
use hydradb_cypher_engine::{QueryValue, ScalarValue};

#[tokio::test]
async fn coalesce_returns_the_first_argument_that_is_not_null() {
    let result = run("MATCH (i:Item) RETURN coalesce(i.name, 'unnamed') AS name ORDER BY name ASC").await;
    assert_eq!(columns(&result), ["name"]);
    assert_eq!(
        table(&result),
        rows![
            ["\"Durian\""],
            ["\"apple\""],
            ["\"apple\""],
            ["\"banana\""],
            ["\"carrot\""],
            ["\"unnamed\""]
        ]
    );
}

#[tokio::test]
async fn coalesce_of_nothing_but_nulls_is_null() {
    let result = run("MATCH (i:Item {name: 'Durian'}) RETURN coalesce(i.score, i.missing, null) AS value").await;
    assert_eq!(table(&result), rows![["null"]]);
}

#[tokio::test]
async fn coalesce_skips_any_number_of_nulls() {
    let result = run("MATCH (i:Item {name: 'Durian'}) RETURN coalesce(null, i.score, i.kind, 'x') AS value").await;
    assert_eq!(table(&result), rows![["\"fruit\""]]);
}

#[tokio::test]
async fn a_scalar_call_does_not_group_the_projection() {
    // Six items, six rows: an aggregate here would have collapsed them.
    let result = run("MATCH (i:Item) RETURN coalesce(i.score, -1) AS score ORDER BY score ASC").await;
    assert_eq!(table(&result), rows![["-1"], ["0"], ["1"], ["2"], ["3"], ["5"]]);
}

#[tokio::test]
async fn coalesce_filters_in_where() {
    let result = run("MATCH (i:Item) WHERE coalesce(i.name, '') = '' RETURN i.score AS score").await;
    assert_eq!(table(&result), rows![["5"]]);
}

#[tokio::test]
async fn coalesce_wraps_an_aggregate_and_groups_by_its_keys() {
    let result = run("MATCH (i:Item) RETURN i.kind AS kind, coalesce(sum(i.score), 0) AS total, count(*) AS n ORDER BY kind ASC").await;
    assert_eq!(table(&result), rows![["\"fruit\"", "4", "4"], ["\"veg\"", "7", "2"]]);
}

#[tokio::test]
async fn coalesce_over_an_empty_group_still_answers() {
    let result = run("MATCH (i:Item) WHERE i.score > 100 RETURN coalesce(null, 7) AS fallback, count(*) AS n").await;
    assert_eq!(table(&result), rows![["7", "0"]]);
}

#[tokio::test]
async fn coalesce_reads_a_with_projected_value() {
    let result = run("MATCH (i:Item) WITH i.name AS name RETURN coalesce(name, 'zz') AS name ORDER BY name ASC").await;
    assert_eq!(table(&result).len(), 6);
    assert_eq!(table(&result)[5], ["\"zz\""]);
}

#[tokio::test]
async fn function_names_are_case_insensitive() {
    let result = run("MATCH (i:Item {name: 'banana'}) RETURN COALESCE(i.missing, i.score) AS score").await;
    assert_eq!(table(&result), rows![["1"]]);
}

#[tokio::test]
async fn coalesce_needs_an_argument() {
    assert!(refusal("MATCH (i:Item) RETURN coalesce() AS x")
        .await
        .contains("does not take 0 arguments"));
}

#[tokio::test]
async fn distinct_is_refused_outside_an_aggregate() {
    assert!(refusal("MATCH (i:Item) RETURN coalesce(DISTINCT i.name) AS x")
        .await
        .contains("DISTINCT is only valid in an aggregate"));
}

#[tokio::test]
async fn an_unknown_function_is_still_refused_by_name() {
    assert!(refusal("MATCH (i:Item) RETURN frobnicate(i.name) AS x")
        .await
        .contains("function frobnicate is not supported"));
}

#[tokio::test]
async fn coalesce_preserves_a_collected_list() {
    let result = run("MATCH (i:Item) RETURN coalesce(collect(i.name), 'fallback') AS names").await;
    let hydradb_cypher_engine::QueryValue::List(names) = &result.rows[0].values[0] else {
        panic!("expected the collected list");
    };
    assert_eq!(names.len(), 5);
}

#[tokio::test]
async fn coalesce_preserves_an_empty_collection() {
    let result = run("MATCH (i:Missing) RETURN coalesce(collect(i.name), 'fallback') AS names").await;
    assert_eq!(table(&result), rows![["[]"]]);
}

#[tokio::test]
async fn grouped_coalesce_counts_have_the_same_type_as_integer_fallbacks() {
    for (label, count) in [("Item", 6_u64), ("Missing", 0)] {
        let query =
            format!("MATCH (i:{label}) RETURN count(*) AS raw, coalesce(count(*), 0) AS counted, coalesce({count}, count(*)) AS literal");
        let result = run(&query).await;
        assert_eq!(
            result.rows[0].values,
            vec![
                QueryValue::Count(count),
                QueryValue::Scalar(ScalarValue::Integer(i128::from(count))),
                QueryValue::Scalar(ScalarValue::Integer(i128::from(count))),
            ]
        );
    }
}

#[tokio::test]
async fn grouped_coalesce_sorts_count_and_property_branches_numerically() {
    for (direction, expected) in [("ASC", [0, 1, 1, 2, 3, 5]), ("DESC", [5, 3, 2, 1, 1, 0])] {
        let query = format!("MATCH (i:Item) RETURN coalesce(i.score, count(*)) AS value ORDER BY value {direction}");
        let result = run(&query).await;
        assert_eq!(
            result.rows.iter().map(|row| row.values.clone()).collect::<Vec<_>>(),
            expected
                .into_iter()
                .map(|value| vec![QueryValue::Scalar(ScalarValue::Integer(value))])
                .collect::<Vec<_>>()
        );
    }
}

#[tokio::test]
async fn nested_grouped_coalesce_keeps_a_count_as_a_scalar_integer() {
    let result = run("MATCH (i:Item) RETURN coalesce(null, coalesce(count(*), 0)) AS value").await;
    assert_eq!(result.rows[0].values, vec![QueryValue::Scalar(ScalarValue::Integer(6))]);
}
