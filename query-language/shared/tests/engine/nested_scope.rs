//! A projection below a result plan, and a UNION below a binding operator.
//!
//! These are the two operators `WITH` needs and the
//! executor refused: it required `Project` exactly at the root and rejected
//! `Union` anywhere below one.
//!
//! The plans are built by hand because lowering cannot produce them yet - a
//! nested projection needs `WITH` as a scope boundary, which is the next PR.
//! Testing the operators first is deliberate: it keeps the executor's
//! behaviour pinned by tests that do not depend on how lowering happens to
//! spell a scope, so the lowering PR can be read as a lowering change.

mod common;

use hydradb_cypher_ast::QueryConjunction;
use std::sync::Arc;

use common::{entity, MemoryStorage};
use hydradb_cypher_ast::{CypherDialect, CypherVersion, Document};
use hydradb_cypher_engine::{
    CypherEngine, GraphLogicalPlan, GraphPhysicalPlan, PhysicalExpression, PhysicalProjection, PreparedQuery, QueryValue, ScalarValue,
    Symbol,
};
use hydradb_cypher_engine::{SlateDbDataSource, UnknownStatistics, VertexScanRequest};

fn storage() -> MemoryStorage {
    MemoryStorage::with_vertices([entity(1, "Entity", "alpha"), entity(2, "Entity", "beta")])
}

fn prepared(physical: GraphPhysicalPlan) -> PreparedQuery {
    PreparedQuery {
        ast: Arc::new(Document::new(CypherDialect::Neo4j(CypherVersion::V25), Vec::new())),
        logical: Arc::new(GraphLogicalPlan::NodeScan {
            binding: Symbol::from("e"),
            labels: Vec::new(),
        }),
        physical,
    }
}

fn project(input: GraphPhysicalPlan, items: Vec<PhysicalProjection>) -> GraphPhysicalPlan {
    GraphPhysicalPlan::Project {
        context: Default::default(),
        operation: hydradb_cypher_ast::operation::RETURN,
        input: Box::new(input),
        items,
        distinct: false,
        post_sort: Vec::new(),
        post_skip: None,
        post_limit: None,
    }
}

fn item(expression: PhysicalExpression, alias: Option<&str>) -> PhysicalProjection {
    PhysicalProjection {
        expression,
        alias: alias.map(Symbol::from),
    }
}

fn binding(name: &str) -> PhysicalExpression {
    PhysicalExpression::Binding(Symbol::from(name))
}

fn property(name: &str, key: &str) -> PhysicalExpression {
    PhysicalExpression::Property {
        binding: Symbol::from(name),
        property: Symbol::from(key),
    }
}

fn scan() -> GraphPhysicalPlan {
    GraphPhysicalPlan::VertexScan {
        scan: SlateDbDataSource::new(&UnknownStatistics).prepare_vertex_scan(VertexScanRequest::new(Symbol::from("e"), Vec::new())),
    }
}

fn strings(result: &hydradb_cypher_engine::QueryResult) -> Vec<String> {
    result
        .rows
        .iter()
        .map(|row| match &row.values[0] {
            QueryValue::Scalar(ScalarValue::String(value)) => value.to_string(),
            other => panic!("expected a string, got {other:?}"),
        })
        .collect()
}

/// An entity projected under a new name keeps its properties.
///
/// This is the case that decides how a nested projection must carry entities.
/// Flattening `e` to its id when binding it to `a` would make `a.entity_id`
/// null, which is how a scope boundary silently loses data.
#[tokio::test]
async fn a_renamed_entity_keeps_its_property_map() {
    let engine = CypherEngine::new(storage());
    let inner = project(scan(), vec![item(binding("e"), Some("a"))]);
    let outer = project(inner, vec![item(property("a", "entity_id"), Some("id"))]);

    let result = engine
        .execute_prepared(&prepared(outer))
        .await
        .expect("a renamed entity is still an entity");
    let mut ids = strings(&result);
    ids.sort();
    assert_eq!(ids, ["alpha", "beta"]);
}

/// A computed value bound by a nested projection is readable by name.
#[tokio::test]
async fn a_bound_scalar_is_a_binding_like_any_other() {
    let engine = CypherEngine::new(storage());
    let inner = project(scan(), vec![item(property("e", "entity_id"), Some("id"))]);
    let outer = project(inner, vec![item(binding("id"), Some("id"))]);

    let result = engine
        .execute_prepared(&prepared(outer))
        .await
        .expect("a bound scalar survives the scope boundary");
    let mut ids = strings(&result);
    ids.sort();
    assert_eq!(ids, ["alpha", "beta"]);
}

/// A nested projection replaces the scope rather than adding to it.
///
/// `e` is gone after a projection that does not carry it, so reading it is
/// null - not the value it had on the other side of the boundary.
#[tokio::test]
async fn a_scope_boundary_drops_what_it_does_not_project() {
    let engine = CypherEngine::new(storage());
    let inner = project(scan(), vec![item(property("e", "entity_id"), Some("id"))]);
    let outer = project(inner, vec![item(property("e", "entity_id"), Some("gone"))]);

    let result = engine
        .execute_prepared(&prepared(outer))
        .await
        .expect("reading a dropped binding is null, not an error");
    assert_eq!(result.rows.len(), 2);
    assert!(result.rows.iter().all(|row| row.values[0] == QueryValue::Null));
}

/// DISTINCT and the window apply to the projected scope.
#[tokio::test]
async fn distinct_and_limit_apply_after_the_projection() {
    let engine = CypherEngine::new(storage());
    // Both vertices project the same constant, so DISTINCT collapses them.
    let inner = GraphPhysicalPlan::Project {
        context: Default::default(),
        operation: hydradb_cypher_ast::operation::RETURN,
        input: Box::new(scan()),
        items: vec![item(
            PhysicalExpression::Value(hydradb_cypher_engine::BoundValue {
                value: ScalarValue::String("same".into()),
                origin: hydradb_cypher_engine::ValueOrigin::Literal,
            }),
            Some("constant"),
        )],
        distinct: true,
        post_sort: Vec::new(),
        post_skip: None,
        post_limit: None,
    };
    let outer = project(inner, vec![item(binding("constant"), Some("value"))]);

    let result = engine
        .execute_prepared(&prepared(outer))
        .await
        .expect("DISTINCT below a result plan collapses duplicate rows");
    assert_eq!(strings(&result), ["same"]);
}

#[tokio::test]
async fn union_all_below_a_binding_operator_concatenates_its_arms() {
    let engine = CypherEngine::new(storage());
    let union = GraphPhysicalPlan::Union {
        arms: vec![scan(), scan()],
        all: true,
        conjunction: QueryConjunction::Union,
    };
    let plan = project(union, vec![item(property("e", "entity_id"), Some("id"))]);

    let result = engine
        .execute_prepared(&prepared(plan))
        .await
        .expect("UNION ALL below a binding operator");
    let mut ids = strings(&result);
    ids.sort();
    assert_eq!(ids, ["alpha", "alpha", "beta", "beta"]);
}

/// GQL §14.2 General Rule 2 below a binding operator: EXCEPT and INTERSECT
/// count each row the arms share, and OTHERWISE keeps the first arm with rows.
#[tokio::test]
async fn except_intersect_and_otherwise_below_a_binding_operator() {
    let engine = CypherEngine::new(storage());
    for (conjunction, all, expected) in [
        (QueryConjunction::Except, true, Vec::<&str>::new()),
        (QueryConjunction::Intersect, true, vec!["alpha", "beta"]),
        (QueryConjunction::Intersect, false, vec!["alpha", "beta"]),
        (QueryConjunction::Otherwise, false, vec!["alpha", "beta"]),
    ] {
        let composite = GraphPhysicalPlan::Union {
            arms: vec![scan(), scan()],
            all,
            conjunction,
        };
        let plan = project(composite, vec![item(property("e", "entity_id"), Some("id"))]);
        let result = engine
            .execute_prepared(&prepared(plan))
            .await
            .expect("a composite below a binding operator");
        let mut ids = strings(&result);
        ids.sort();
        assert_eq!(ids, expected, "{conjunction:?} all={all}");
    }
}

#[tokio::test]
async fn union_below_a_binding_operator_deduplicates_its_arms() {
    let engine = CypherEngine::new(storage());
    let union = GraphPhysicalPlan::Union {
        arms: vec![scan(), scan()],
        all: false,
        conjunction: QueryConjunction::Union,
    };
    let plan = project(union, vec![item(property("e", "entity_id"), Some("id"))]);

    let result = engine
        .execute_prepared(&prepared(plan))
        .await
        .expect("UNION below a binding operator");
    let mut ids = strings(&result);
    ids.sort();
    assert_eq!(ids, ["alpha", "beta"], "identical arms deduplicate");
}
