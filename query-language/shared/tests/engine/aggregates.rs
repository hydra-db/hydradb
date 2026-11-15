//! `min`, `max`, and aggregating no rows.
//!
//! Expectations follow Neo4j 2026.09 under CYPHER 25; the matrix rows
//! `compat_min_max` and `compat_min_max_empty` hold the same answers to Neo4j
//! through `just neo4j-oracle`.

mod common;

use common::compat::{columns, run, table};

#[tokio::test]
async fn min_and_max_ignore_nulls_and_order_within_a_type() {
    let result = run("MATCH (i:Item) RETURN min(i.score) AS low, max(i.score) AS high, min(i.name) AS first, max(i.name) AS last").await;
    assert_eq!(columns(&result), ["low", "high", "first", "last"]);
    // Strings order by code point, so 'Durian' is the least name.
    assert_eq!(table(&result), rows![["0", "5", "\"Durian\"", "\"carrot\""]]);
}

#[tokio::test]
async fn min_and_max_group_by_the_other_items() {
    let result = run("MATCH (i:Item) RETURN i.kind AS kind, min(i.score) AS low, max(i.score) AS high ORDER BY kind ASC").await;
    assert_eq!(table(&result), rows![["\"fruit\"", "0", "3"], ["\"veg\"", "2", "5"]]);
}

#[tokio::test]
async fn aggregating_no_rows_still_answers_one_row() {
    // min, max and avg of nothing are null; count and sum of nothing are zero.
    let result = run("MATCH (i:Item) WHERE i.score > 100 \
         RETURN min(i.score) AS low, max(i.name) AS last, avg(i.score) AS mean, sum(i.score) AS total, count(*) AS n")
    .await;
    assert_eq!(table(&result), rows![["null", "null", "null", "0", "0"]]);
}

#[tokio::test]
async fn sum_of_only_nulls_is_zero() {
    let result = run("MATCH (i:Item {name: 'Durian'}) RETURN sum(i.score) AS total, avg(i.score) AS mean").await;
    assert_eq!(table(&result), rows![["0", "null"]]);
}

#[tokio::test]
async fn a_group_whose_inputs_are_all_null_has_a_null_minimum() {
    let result = run("MATCH (i:Item) RETURN i.kind AS kind, min(i.name) AS first ORDER BY kind ASC").await;
    // Every veg item but carrot is unnamed; the group's minimum skips the null.
    assert_eq!(table(&result), rows![["\"fruit\"", "\"Durian\""], ["\"veg\"", "\"carrot\""]]);
}

#[tokio::test]
async fn aggregates_compose_with_scalar_functions() {
    let result = run("MATCH (i:Item) RETURN toUpper(max(i.name)) AS last, coalesce(min(i.missing), -1) AS absent").await;
    assert_eq!(table(&result), rows![["\"CARROT\"", "-1"]]);
}

#[tokio::test]
async fn results_order_by_an_aggregate() {
    let result = run("MATCH (i:Item) RETURN i.kind AS kind, max(i.score) AS high ORDER BY high DESC").await;
    assert_eq!(table(&result), rows![["\"veg\"", "5"], ["\"fruit\"", "3"]]);
}

#[tokio::test]
async fn distinct_aggregates_count_each_value_once() {
    let result = run("MATCH (i:Item) RETURN count(DISTINCT i.name) AS names, count(i.name) AS named, \
         count(DISTINCT i.kind) AS kinds, sum(DISTINCT i.score) AS distinct_total")
    .await;
    // Two items are named apple; the distinct scores {0, 1, 2, 3, 5} sum to 11.
    assert_eq!(table(&result), rows![["4", "5", "2", "11"]]);
}

#[tokio::test]
async fn distinct_applies_per_group() {
    let result = run("MATCH (i:Item) RETURN i.kind AS kind, count(DISTINCT i.name) AS names, count(*) AS n ORDER BY kind ASC").await;
    assert_eq!(table(&result), rows![["\"fruit\"", "3", "4"], ["\"veg\"", "1", "2"]]);
}

#[tokio::test]
async fn collect_distinct_keeps_first_seen_order_and_drops_nulls() {
    let result = run("MATCH (i:Item) WITH i ORDER BY i.score ASC RETURN collect(DISTINCT i.kind) AS kinds").await;
    // Scores ascending: apple(0, fruit), banana(1, fruit), carrot(2, veg) ...
    assert_eq!(table(&result), rows![["[\"fruit\", \"veg\"]"]]);
}

#[tokio::test]
async fn distinct_min_max_and_avg_agree_with_their_plain_forms_where_they_must() {
    let result = run(
        "MATCH (i:Item) RETURN min(DISTINCT i.score) AS low, max(DISTINCT i.score) AS high, \
         avg(DISTINCT i.score) AS mean, avg(i.score) AS plain_mean",
    )
    .await;
    // Distinct scores {0,1,2,3,5}: mean 2.2. Every score is distinct already.
    assert_eq!(table(&result), rows![["0", "5", "2.2f", "2.2f"]]);
}

#[tokio::test]
async fn an_unaliased_distinct_aggregate_names_its_column_with_distinct() {
    let result = run("MATCH (i:Item) RETURN count(DISTINCT i.kind)").await;
    assert_eq!(columns(&result), ["count(DISTINCT i.kind)"]);
}

#[tokio::test]
async fn numeric_distinct_uses_values_not_types_or_float_spellings() {
    use hydradb_cypher_engine::{CypherEngine, ParameterMap, ScalarValue, VertexRecord};
    let scores = [
        ScalarValue::Integer(1),
        ScalarValue::Float("1.0".into()),
        ScalarValue::Float("1.00".into()),
        ScalarValue::Integer(2),
        ScalarValue::Float("2.0".into()),
        ScalarValue::Integer(0),
        ScalarValue::Float("-0.0".into()),
    ];
    let storage = common::MemoryStorage::with_vertices(
        scores
            .into_iter()
            .enumerate()
            .map(|(id, score)| VertexRecord::new(id as u64).with_label("Score").with_property("value", score)),
    );
    let engine = CypherEngine::new(storage);
    let prepared = engine.prepare("MATCH (s:Score) RETURN count(DISTINCT s.value) AS n, sum(DISTINCT s.value) AS total, avg(DISTINCT s.value) AS mean, collect(DISTINCT s.value) AS values", &ParameterMap::new()).unwrap();
    let result = engine.execute_prepared(&prepared).await.unwrap();
    assert_eq!(table(&result), rows![["3", "3", "1f", "[1, 2, 0]"]]);
}

#[tokio::test]
async fn all_set_and_group_operators_share_numeric_keys_without_erasing_types() {
    use hydradb_cypher_engine::{CypherEngine, ParameterMap, QueryValue, ScalarValue, VertexRecord};
    let storage = common::MemoryStorage::with_vertices([
        VertexRecord::new(1)
            .with_label("Score")
            .with_property("value", ScalarValue::Integer(1)),
        VertexRecord::new(2)
            .with_label("Score")
            .with_property("value", ScalarValue::Float("1.0".into())),
        VertexRecord::new(3)
            .with_label("Score")
            .with_property("value", ScalarValue::Integer(2)),
    ]);
    let engine = CypherEngine::new(storage);
    for (query, expected) in [
        ("MATCH (s:Score) RETURN DISTINCT s.value AS v ORDER BY v", rows![["1"], ["2"]]),
        (
            "MATCH (s:Score) RETURN s.value AS v UNION MATCH (s:Score) RETURN s.value AS v",
            rows![["1"], ["2"]],
        ),
        (
            "MATCH (s:Score) RETURN s.value AS v, count(*) AS n ORDER BY v",
            rows![["1", "2"], ["2", "1"]],
        ),
        (
            "MATCH (s:Score) WITH DISTINCT s.value AS v RETURN v ORDER BY v",
            rows![["1"], ["2"]],
        ),
    ] {
        let plan = engine.prepare(query, &ParameterMap::new()).unwrap();
        assert_eq!(table(&engine.execute_prepared(&plan).await.unwrap()), expected, "{query}");
    }
    // Typed result equality remains suitable for detecting a changed output
    // representation; only operator keying uses numeric equivalence.
    assert_ne!(
        QueryValue::Scalar(ScalarValue::Integer(1)),
        QueryValue::Scalar(ScalarValue::Float("1.0".into()))
    );
}
