//! `r.p = $a OR r.p = $b` and `r.p IN $list` seek the relationship property
//! index once per distinct value instead of expanding every relationship of
//! the type. The rows must be exactly those of the expansion it replaces.

mod common;

use std::collections::BTreeSet;

use common::{entity, MemoryStorage};
use hydradb_cypher_engine::{
    CypherEngine, GraphPhysicalPlan, ListParameterMap, ParameterMap, QueryRow, QueryValue, RelationshipRecord, ScalarValue,
};

/// An `Entity` hub `a` (1) with `KNOWS` edges to five people, a second hub
/// (2) with one, and a `LIKES` edge carrying the same property, which no
/// `KNOWS` seek may return. Relationship 13 goes to the same person as 12, so
/// the target is reached by two relationships with different values.
///
/// Every relationship carries the client `id` property, what `r.id` reads,
/// equal to its internal identity, as HydraDB's identified write path stores it.
fn graph() -> MemoryStorage {
    let knows = |id: u64, source, target, weight: ScalarValue| {
        RelationshipRecord::new(id, source, target, "KNOWS")
            .with_property("id", ScalarValue::Integer(i128::from(id)))
            .with_property("weight", weight)
    };
    MemoryStorage::with_graph(
        [
            entity(1, "Entity", "a"),
            entity(2, "Entity", "z"),
            entity(10, "Person", "p10"),
            entity(11, "Person", "p11"),
            entity(12, "Person", "p12"),
            entity(13, "Person", "p13"),
            entity(14, "Robot", "r14"),
        ],
        [
            knows(10, 1, 10, ScalarValue::Integer(1)),
            knows(11, 1, 11, ScalarValue::Integer(2)),
            knows(12, 1, 12, ScalarValue::Integer(3)),
            knows(13, 1, 12, ScalarValue::Integer(2)),
            knows(14, 1, 14, ScalarValue::Integer(2)),
            knows(15, 2, 13, ScalarValue::Float("1.0".into())),
            RelationshipRecord::new(16, 1, 13, "LIKES")
                .with_property("id", ScalarValue::Integer(16))
                .with_property("weight", ScalarValue::Integer(1)),
        ],
    )
}

fn integers(values: &[i128]) -> Vec<ScalarValue> {
    values.iter().copied().map(ScalarValue::Integer).collect()
}

fn prepare(
    query: &str,
    parameters: &ParameterMap,
    lists: &ListParameterMap,
) -> (CypherEngine<MemoryStorage>, hydradb_cypher_engine::PreparedQuery) {
    let engine = CypherEngine::new(graph());
    let prepared = engine
        .prepare_with_lists(query, parameters, lists)
        .unwrap_or_else(|error| panic!("{query:?} failed to prepare: {error}"));
    (engine, prepared)
}

fn explain(query: &str, parameters: &ParameterMap, lists: &ListParameterMap) -> String {
    prepare(query, parameters, lists).1.explain()
}

async fn rows(query: &str, parameters: &ParameterMap, lists: &ListParameterMap) -> Vec<QueryRow> {
    let (engine, prepared) = prepare(query, parameters, lists);
    let mut rows = engine
        .execute_prepared(&prepared)
        .await
        .unwrap_or_else(|error| panic!("{query:?} failed: {error}"))
        .rows;
    rows.sort_by_key(|row| format!("{:?}", row.values));
    rows
}

/// The access under the projection and the Filter above it.
fn access(prepared: &hydradb_cypher_engine::PreparedQuery) -> &GraphPhysicalPlan {
    let GraphPhysicalPlan::Project { input, .. } = &prepared.physical else {
        panic!("expected a projection root");
    };
    let GraphPhysicalPlan::Filter { input, .. } = input.as_ref() else {
        panic!("the predicate must stay above the seek: {}", prepared.explain());
    };
    input
}

/// `a.id, r.id, b.id`.
fn row(&[a, r, b]: &[u64; 3]) -> QueryRow {
    QueryRow {
        values: vec![
            QueryValue::VertexId(a),
            QueryValue::Scalar(ScalarValue::Integer(i128::from(r))),
            QueryValue::VertexId(b),
        ],
    }
}

const SEEK_OR: &str = "MATCH (a:Entity)-[r:KNOWS]->(b) WHERE r.weight = $x OR r.weight = $y RETURN a.id AS a, r.id AS r, b.id AS b";
/// `OR false` keeps the predicate's meaning and defeats the index: the
/// expansion every seek plan below is held to.
const EXPAND_OR: &str =
    "MATCH (a:Entity)-[r:KNOWS]->(b) WHERE (r.weight = $x OR r.weight = $y) OR false RETURN a.id AS a, r.id AS r, b.id AS b";

#[test]
fn an_or_of_equalities_on_one_relationship_property_plans_a_multi_seek() {
    let parameters = ParameterMap::from([("x".into(), ScalarValue::Integer(1)), ("y".into(), ScalarValue::Integer(3))]);
    let (_, prepared) = prepare(SEEK_OR, &parameters, &ListParameterMap::new());
    let GraphPhysicalPlan::RelationshipPropertyMultiSeek {
        property,
        values,
        relationship,
        ..
    } = access(&prepared)
    else {
        panic!("expected a multi-value relationship seek:\n{}", prepared.explain());
    };
    assert_eq!(property.as_str(), "weight");
    assert_eq!(relationship.as_str(), "r");
    assert_eq!(
        values.iter().map(|value| value.value.clone()).collect::<Vec<_>>(),
        integers(&[1, 3])
    );
    let explain = prepared.explain();
    assert!(explain.contains("RelationshipPropertyMultiSeek("), "{explain}");
    assert!(explain.contains("property=weight"), "{explain}");
    assert!(!explain.contains("ExpandExec"), "{explain}");

    let fallback = explain_plain(EXPAND_OR, &parameters);
    assert!(fallback.contains("ExpandExec"), "{fallback}");
    assert!(!fallback.contains("RelationshipProperty"), "{fallback}");
}

fn explain_plain(query: &str, parameters: &ParameterMap) -> String {
    explain(query, parameters, &ListParameterMap::new())
}

#[test]
fn an_in_list_parameter_or_literal_plans_a_multi_seek() {
    let lists = ListParameterMap::from([("weights".into(), integers(&[2, 3]))]);
    let parameter = explain(
        "MATCH (a:Entity)-[r:KNOWS]->(b) WHERE r.weight IN $weights RETURN b",
        &ParameterMap::new(),
        &lists,
    );
    assert!(parameter.contains("RelationshipPropertyMultiSeek("), "{parameter}");
    assert!(parameter.contains("values=["), "{parameter}");

    let literal = explain_plain(
        "MATCH (a:Entity)-[r:KNOWS]->(b) WHERE r.weight IN [3, 1, 2] RETURN b",
        &ParameterMap::new(),
    );
    assert!(literal.contains("RelationshipPropertyMultiSeek("), "{literal}");
    assert!(!literal.contains("ExpandExec"), "{literal}");
}

#[test]
fn other_conjuncts_stay_as_the_filter_over_the_multi_seek() {
    let lists = ListParameterMap::from([("weights".into(), integers(&[2, 3]))]);
    let (_, prepared) = prepare(
        "MATCH (a:Entity)-[r:KNOWS]->(b:Person) WHERE r.weight IN $weights AND b.entity_id <> 'p11' AND a.entity_id = 'a' RETURN b",
        &ParameterMap::new(),
        &lists,
    );
    assert!(
        matches!(access(&prepared), GraphPhysicalPlan::RelationshipPropertyMultiSeek { .. }),
        "{}",
        prepared.explain()
    );
    let explain = prepared.explain();
    assert!(explain.contains("FilterExec"), "{explain}");
    assert!(explain.contains("entity_id"), "{explain}");
}

#[test]
fn duplicate_values_collapse_and_a_single_value_keeps_the_single_seek() {
    // Literal duplicates are one value, so this is the single seek, unchanged.
    let single = explain_plain(
        "MATCH (a:Entity)-[r:KNOWS]->(b) WHERE r.weight IN [2, 2] RETURN b",
        &ParameterMap::new(),
    );
    assert!(single.contains("RelationshipPropertySeek("), "{single}");
    assert!(single.contains("value=2"), "{single}");
    let equality = explain_plain("MATCH (a:Entity)-[r:KNOWS]->(b) WHERE r.weight = 2 RETURN b", &ParameterMap::new());
    assert!(equality.contains("RelationshipPropertySeek("), "{equality}");
    assert!(!equality.contains("MultiSeek"), "{equality}");

    let repeated = explain_plain(
        "MATCH (a:Entity)-[r:KNOWS]->(b) WHERE r.weight = 3 OR r.weight = 2 OR r.weight = 3 RETURN b",
        &ParameterMap::new(),
    );
    assert!(repeated.contains("RelationshipPropertyMultiSeek("), "{repeated}");
    assert!(repeated.contains("values=[2, 3]"), "{repeated}");
}

#[test]
fn shapes_the_multi_seek_leaves_to_the_expansion() {
    for query in [
        // Two properties: neither side alone covers the result.
        "MATCH (a:Entity)-[r:KNOWS]->(b) WHERE r.weight = 1 OR r.since = 2 RETURN b",
        // Two bindings.
        "MATCH (a:Entity)-[r:KNOWS]->(b) WHERE r.weight = 1 OR b.entity_id = 'p10' RETURN b",
        // No relationship type to seek.
        "MATCH (a:Entity)-[r]->(b) WHERE r.weight IN [1, 2] RETURN b",
        // An empty list matches nothing; there is no value to seek.
        "MATCH (a:Entity)-[r:KNOWS]->(b) WHERE r.weight IN [] RETURN b",
    ] {
        let explain = explain_plain(query, &ParameterMap::new());
        assert!(!explain.contains("RelationshipPropertyMultiSeek"), "{query}\n{explain}");
    }
}

#[tokio::test]
async fn the_multi_seek_returns_exactly_the_expansions_rows() {
    let cases: Vec<(i128, i128)> = vec![(1, 3), (2, 3), (2, 9), (7, 9), (1, 2)];
    for (x, y) in cases {
        let parameters = ParameterMap::from([("x".into(), ScalarValue::Integer(x)), ("y".into(), ScalarValue::Integer(y))]);
        let seek = rows(SEEK_OR, &parameters, &ListParameterMap::new()).await;
        let expand = rows(EXPAND_OR, &parameters, &ListParameterMap::new()).await;
        assert_eq!(seek, expand, "weights {x} or {y}");
    }

    // The brute-force answer for one case, so agreement is not agreement on
    // a shared mistake: weight 2 or 3 from either hub.
    let parameters = ParameterMap::from([("x".into(), ScalarValue::Integer(2)), ("y".into(), ScalarValue::Integer(3))]);
    let seek = rows(SEEK_OR, &parameters, &ListParameterMap::new()).await;
    assert_eq!(
        seek,
        vec![row(&[1, 11, 11]), row(&[1, 12, 12]), row(&[1, 13, 12]), row(&[1, 14, 14])]
    );
}

#[tokio::test]
async fn in_list_rows_match_the_expansion_with_endpoint_filters() {
    let lists = ListParameterMap::from([("weights".into(), integers(&[2, 3, 99]))]);
    let seek = "MATCH (a:Entity)-[r:KNOWS]->(b:Person) WHERE r.weight IN $weights AND b.entity_id <> 'p11' \
                RETURN a.id AS a, r.id AS r, b.id AS b";
    let expand = "MATCH (a:Entity)-[r:KNOWS]->(b:Person) WHERE (r.weight IN $weights AND b.entity_id <> 'p11') OR false \
                  RETURN a.id AS a, r.id AS r, b.id AS b";
    assert!(explain(seek, &ParameterMap::new(), &lists).contains("RelationshipPropertyMultiSeek("));
    assert!(explain(expand, &ParameterMap::new(), &lists).contains("ExpandExec"));
    let seek_rows = rows(seek, &ParameterMap::new(), &lists).await;
    // 14 is a Robot and p11 is excluded, which leaves the two edges to 12.
    assert_eq!(seek_rows, vec![row(&[1, 12, 12]), row(&[1, 13, 12])]);
    assert_eq!(seek_rows, rows(expand, &ParameterMap::new(), &lists).await);
}

#[tokio::test]
async fn a_relationship_two_values_match_is_one_row() {
    // `1` and `1.0` are Cypher-equal, so both seeks find relationships 10 and
    // 15. Each must still be one row.
    let lists = ListParameterMap::from([("weights".into(), vec![ScalarValue::Integer(1), ScalarValue::Float("1.0".into())])]);
    let seek = "MATCH (a:Entity)-[r:KNOWS]->(b) WHERE r.weight IN $weights RETURN a.id AS a, r.id AS r, b.id AS b";
    assert!(explain(seek, &ParameterMap::new(), &lists).contains("RelationshipPropertyMultiSeek("));
    let found = rows(seek, &ParameterMap::new(), &lists).await;
    assert_eq!(found, vec![row(&[1, 10, 10]), row(&[2, 15, 13])]);

    // Two parameters bound to one value are one seek and one row.
    let parameters = ParameterMap::from([("x".into(), ScalarValue::Integer(3)), ("y".into(), ScalarValue::Integer(3))]);
    let (engine, prepared) = prepare(SEEK_OR, &parameters, &ListParameterMap::new());
    assert!(
        prepared.explain().contains("RelationshipPropertyMultiSeek("),
        "{}",
        prepared.explain()
    );
    let result = engine.execute_prepared(&prepared).await.expect("execute");
    assert_eq!(result.rows, vec![row(&[1, 12, 12])]);
}

#[tokio::test]
async fn each_distinct_value_is_sought_once() {
    let storage = graph();
    let sought = std::sync::Arc::clone(&storage.relationship_seek_values);
    let expanded = std::sync::Arc::clone(&storage.expand_requests);
    let engine = CypherEngine::new(storage);
    let parameters = ParameterMap::from([("x".into(), ScalarValue::Integer(3)), ("y".into(), ScalarValue::Integer(3))]);
    let lists = ListParameterMap::from([("weights".into(), integers(&[2, 3, 2]))]);
    for (query, parameters, lists) in [
        (SEEK_OR, &parameters, &ListParameterMap::new()),
        (
            "MATCH (a:Entity)-[r:KNOWS]->(b) WHERE r.weight IN $weights RETURN r.id AS r",
            &ParameterMap::new(),
            &lists,
        ),
    ] {
        sought.lock().unwrap().clear();
        let prepared = engine.prepare_with_lists(query, parameters, lists).expect("prepare");
        engine.execute_prepared(&prepared).await.expect("execute");
        let values = sought.lock().unwrap().clone();
        assert_eq!(
            values.len(),
            values.iter().collect::<BTreeSet<_>>().len(),
            "{query}: a value was sought twice: {values:?}"
        );
    }
    assert!(expanded.lock().unwrap().is_empty(), "a seek plan must not expand");
}
