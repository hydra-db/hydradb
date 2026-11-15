//! UNWIND and list values.
//!
//! The matrix rows `compat_unwind_*` hold the same answers to Neo4j. Edge
//! semantics below were probed on Neo4j 2026.09 under CYPHER 25: a null list
//! unwinds to no rows, a non-list value to one, and a list element that is
//! null still makes a row.

mod common;

use common::compat::{columns, refusal, run, run_with, table};
use hydradb_cypher_engine::{ListParameterMap, ParameterMap, ScalarValue};

fn list(name: &str, values: &[i128]) -> ListParameterMap {
    ListParameterMap::from([(name.to_string(), values.iter().map(|value| ScalarValue::Integer(*value)).collect())])
}

#[tokio::test]
async fn unwinding_a_list_parameter_makes_a_row_per_element() {
    let result = run_with(
        "UNWIND $values AS value RETURN value",
        &ParameterMap::new(),
        &list("values", &[3, 1, 3]),
    )
    .await;
    assert_eq!(columns(&result), ["value"]);
    assert_eq!(table(&result), rows![["3"], ["1"], ["3"]]);
}

#[tokio::test]
async fn unwinding_a_literal_then_ordering() {
    let result = run("UNWIND [3, 1, 2] AS value RETURN value ORDER BY value DESC").await;
    assert_eq!(table(&result), rows![["3"], ["2"], ["1"]]);
}

#[tokio::test]
async fn null_empty_scalar_and_null_elements() {
    assert!(table(&run("UNWIND null AS x RETURN x").await).is_empty());
    assert!(table(&run("UNWIND [] AS x RETURN x").await).is_empty());
    assert_eq!(table(&run("UNWIND 5 AS x RETURN x").await), rows![["5"]]);
    assert_eq!(table(&run("UNWIND [1, null] AS x RETURN x").await), rows![["1"], ["null"]]);
}

#[tokio::test]
async fn an_unwound_value_correlates_a_following_match() {
    let result = run("UNWIND ['apple', 'carrot'] AS wanted MATCH (i:Item {name: wanted}) RETURN i.score AS score ORDER BY score ASC").await;
    assert_eq!(table(&result), rows![["0"], ["2"], ["3"]]);
}

#[tokio::test]
async fn unwind_after_match_multiplies_rows() {
    let result = run("MATCH (h:Hub) UNWIND [1, 2] AS copy RETURN h.name AS name, copy ORDER BY copy ASC").await;
    assert_eq!(table(&result), rows![["\"hub\"", "1"], ["\"hub\"", "2"]]);
}

#[tokio::test]
async fn two_unwinds_form_a_cross_product() {
    let result = run("UNWIND [1, 2] AS x UNWIND [10, 20] AS y RETURN x, y").await;
    assert_eq!(table(&result), rows![["1", "10"], ["1", "20"], ["2", "10"], ["2", "20"]]);
}

#[tokio::test]
async fn an_unwind_element_can_be_filtered_and_aggregated() {
    let result = run("UNWIND [1, 2, 3, 4] AS x WITH x WHERE x % 2 = 0 RETURN count(*) AS evens, sum(x) AS total").await;
    assert_eq!(table(&result), rows![["2", "6"]]);
}

#[tokio::test]
async fn nested_lists_and_list_values_return_as_lists() {
    let result = run("UNWIND [[1, 2], [3]] AS x RETURN x, size(x) AS n").await;
    assert_eq!(table(&result), rows![["[1, 2]", "2"], ["[3]", "1"]]);
    assert_eq!(table(&run("RETURN [1, 'a', null] AS l").await), rows![["[1, \"a\", null]"]]);
}

#[tokio::test]
async fn list_equality_is_element_wise() {
    let result = run("RETURN [1, 2] = [1, 2] AS same, [1, 2] = [1, 3] AS different, [1] = [1, 2] AS shorter").await;
    assert_eq!(table(&result), rows![["true", "false", "false"]]);
}

#[tokio::test]
async fn an_unwind_cannot_rebind_a_name_in_scope() {
    assert!(refusal("MATCH (i:Item) UNWIND [1] AS i RETURN i").await.contains("cannot rebind i"));
}

#[tokio::test]
async fn an_unwound_value_cannot_be_bound_as_a_node() {
    assert!(refusal("UNWIND [1] AS x MATCH (x)-[:R]->(y) RETURN y")
        .await
        .contains("holds a value"));
}

#[tokio::test]
async fn unwind_is_a_valid_contextual_identifier() {
    assert_eq!(table(&run("MATCH (unwind:Item) RETURN count(unwind) AS n").await), rows![["6"]]);
}

#[tokio::test]
async fn nested_constant_unwind_respects_a_small_output_budget() {
    use hydradb_cypher_engine::{explain_physical_plan_shape, CypherEngine};
    let engine = CypherEngine::new(common::MemoryStorage {
        max_intermediate_rows: Some(10),
        ..common::MemoryStorage::default()
    });
    let query = "UNWIND $large AS x UNWIND [1] AS y RETURN x LIMIT 1";
    let parameters = ParameterMap::new();
    let prepared = engine
        .prepare_with_lists(query, &parameters, &list("large", &(0..100).collect::<Vec<_>>()))
        .unwrap();
    assert_eq!(table(&engine.execute_prepared(&prepared).await.unwrap()), rows![["0"]]);
    let small = engine.prepare_with_lists(query, &parameters, &list("large", &[0])).unwrap();
    assert_eq!(
        explain_physical_plan_shape(&prepared.physical),
        explain_physical_plan_shape(&small.physical)
    );
}

#[tokio::test]
async fn dependent_empty_lists_do_not_hide_later_nonempty_inputs() {
    assert_eq!(
        table(&run("UNWIND [[], [2]] AS xs UNWIND xs AS x RETURN x LIMIT 1").await),
        rows![["2"]]
    );
}

#[tokio::test]
async fn redacted_lists_retain_property_dependencies() {
    use hydradb_cypher_engine::{explain_physical_plan_shape, CypherEngine};
    let engine = CypherEngine::new(common::compat::graph());
    let a = engine
        .prepare("MATCH (i:Item) UNWIND [i.name] AS x RETURN x", &ParameterMap::new())
        .unwrap();
    let b = engine
        .prepare("MATCH (i:Item) UNWIND [i.score] AS x RETURN x", &ParameterMap::new())
        .unwrap();
    assert_ne!(explain_physical_plan_shape(&a.physical), explain_physical_plan_shape(&b.physical));
}

#[tokio::test]
async fn list_relational_comparisons_are_lexicographic_and_three_valued() {
    assert_eq!(
        table(
            &run("RETURN [1]<[2] AS a, [1,null]<[1,2] AS b, [1]<[1,null] AS c, [] < [] AS d, [[1]] < [[2]] AS e, [(1=1)] <= [true] AS f")
                .await
        ),
        rows![["true", "null", "true", "false", "true", "true"]]
    );
    assert_eq!(
        table(&run("UNWIND [[2],[1],[3]] AS x WITH x WHERE x < [3] RETURN x ORDER BY x").await),
        rows![["[1]"], ["[2]"]]
    );
}

#[tokio::test]
async fn list_set_operators_use_numeric_equality_recursively() {
    assert_eq!(
        table(&run("UNWIND [[1],[1.0],[2]] AS x RETURN DISTINCT x ORDER BY x").await),
        rows![["[1]"], ["[2]"]]
    );
    assert_eq!(
        table(&run("UNWIND [1,1.0,2] AS x RETURN x AS key, count(*) AS n ORDER BY key").await),
        rows![["1", "2"], ["2", "1"]]
    );
}
