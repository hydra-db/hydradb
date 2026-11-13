//! CASE, simple and searched.
//!
//! The matrix rows `compat_case_searched` and `compat_case_simple` hold the
//! same answers to Neo4j.

mod common;

use common::compat::{refusal, run, table};

#[tokio::test]
async fn searched_case_takes_the_first_true_branch() {
    let result = run("MATCH (i:Item) RETURN i.name AS name, \
         CASE WHEN i.score >= 3 THEN 'high' WHEN i.score >= 1 THEN 'mid' ELSE 'low' END AS band \
         ORDER BY name ASC, band ASC")
    .await;
    assert_eq!(
        table(&result),
        rows![
            ["\"Durian\"", "\"low\""],
            ["\"apple\"", "\"high\""],
            ["\"apple\"", "\"low\""],
            ["\"banana\"", "\"mid\""],
            ["\"carrot\"", "\"mid\""],
            ["null", "\"high\""],
        ]
    );
}

#[tokio::test]
async fn simple_case_compares_and_groups() {
    let result =
        run("MATCH (i:Item) RETURN CASE i.kind WHEN 'fruit' THEN 1 WHEN 'veg' THEN 2 END AS code, count(*) AS n ORDER BY code ASC").await;
    assert_eq!(table(&result), rows![["1", "4"], ["2", "2"]]);
}

#[tokio::test]
async fn no_branch_and_no_else_is_null() {
    let result = run("MATCH (i:Item {name: 'banana'}) RETURN CASE i.kind WHEN 'veg' THEN 1 END AS code").await;
    assert_eq!(table(&result), rows![["null"]]);
}

#[tokio::test]
async fn a_null_operand_matches_no_when() {
    let result = run("MATCH (i:Item {name: 'Durian'}) RETURN CASE i.score WHEN null THEN 'null' ELSE 'other' END AS seen").await;
    assert_eq!(table(&result), rows![["\"other\""]]);
}

#[tokio::test]
async fn only_the_taken_branch_is_evaluated() {
    // The ELSE would divide by zero; it is never reached.
    let result = run("MATCH (i:Item {name: 'banana'}) RETURN CASE WHEN true THEN 1 ELSE 1 / 0 END AS x").await;
    assert_eq!(table(&result), rows![["1"]]);
}

#[tokio::test]
async fn case_filters_in_where() {
    let result = run("MATCH (i:Item) WHERE CASE WHEN i.score IS NULL THEN false ELSE i.score > 2 END RETURN count(*) AS n").await;
    assert_eq!(table(&result), rows![["2"]]);
}

#[tokio::test]
async fn case_may_wrap_an_aggregate() {
    let result =
        run("MATCH (i:Item) RETURN i.kind AS kind, CASE WHEN count(*) > 3 THEN 'many' ELSE 'few' END AS size ORDER BY kind ASC").await;
    assert_eq!(table(&result), rows![["\"fruit\"", "\"many\""], ["\"veg\"", "\"few\""]]);
}

#[tokio::test]
async fn a_non_boolean_condition_is_refused_at_evaluation() {
    assert!(!refusal("MATCH (i:Item) RETURN CASE WHEN i.name THEN 1 END AS x").await.is_empty());
}

#[tokio::test]
async fn a_boolean_literal_filters_like_a_comparison() {
    assert_eq!(table(&run("MATCH (i:Item) WHERE true RETURN count(*) AS n").await), rows![["6"]]);
    assert_eq!(table(&run("MATCH (i:Item) WHERE false RETURN count(*) AS n").await), rows![["0"]]);
}

#[tokio::test]
async fn simple_case_compares_computed_and_literal_booleans_in_both_directions() {
    for query in [
        "RETURN CASE (1=1) WHEN true THEN 'yes' ELSE 'no' END AS answer",
        "RETURN CASE true WHEN (1=1) THEN 'yes' ELSE 'no' END AS answer",
        "RETURN CASE (1=2) WHEN false THEN 'yes' ELSE 'no' END AS answer",
    ] {
        assert_eq!(table(&run(query).await), rows![["\"yes\""]]);
    }
    assert_eq!(
        table(&run("RETURN CASE (null=1) WHEN null THEN 'bad' ELSE 'unknown' END AS answer").await),
        rows![["\"unknown\""]]
    );
}

#[tokio::test]
async fn escaped_when_is_an_operand_not_a_case_delimiter() {
    assert_eq!(
        table(&run("UNWIND [1] AS `WHEN` RETURN CASE `WHEN` WHEN 1 THEN 'yes' ELSE 'no' END AS answer").await),
        rows![["\"yes\""]]
    );
    assert_eq!(table(&run("WITH 2 AS `CASE` RETURN `CASE` AS answer").await), rows![["2"]]);
}

#[tokio::test]
async fn case_in_hidden_group_order_keeps_its_key() {
    assert_eq!(
        table(&run("UNWIND [2,2,1] AS x RETURN count(*) AS n GROUP BY x ORDER BY CASE WHEN x=1 THEN 0 ELSE 1 END").await),
        rows![["1"], ["2"]]
    );
}
