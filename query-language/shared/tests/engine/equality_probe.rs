//! A `Filter` over a vertex access path must offer its equality conjuncts to
//! the backend's runtime probe, and must produce exactly the rows it would
//! have produced without one.
//!
//! The planner chooses an access path from predicate order, so the anchor can
//! be the broadest property in the `WHERE` clause. These tests build that
//! plan directly — anchored on the broad property, with the narrow one left in
//! the filter — and check both halves of the contract: the probe replaces the
//! candidate set when it answers, and nothing changes when it declines.

mod common;

use std::sync::Arc;

use common::MemoryStorage;
use hydradb_cypher_ast::{CypherDialect, Document};
use hydradb_cypher_engine::{
    BoundValue, CypherEngine, GraphLogicalPlan, GraphPhysicalPlan, PhysicalBinaryOperator, PhysicalExpression, PhysicalProjection,
    PreparedQuery, QueryResult, ScalarValue, Symbol, ValueOrigin, VertexId, VertexRecord,
};
use hydradb_cypher_engine::{SlateDbDataSource, UnknownStatistics, VertexScanRequest};

fn entity(id: VertexId, tenant: &str, sub: &str, kind: &str) -> VertexRecord {
    VertexRecord::new(id)
        .with_label("Entity")
        .with_property("tenant_id", ScalarValue::String(tenant.into()))
        .with_property("sub_tenant_id", ScalarValue::String(sub.into()))
        .with_property("type", ScalarValue::String(kind.into()))
}

/// Ten vertices in tenant `t1`, two of them in sub-tenant `s9`.
///
/// Sub-tenant names are not unique across tenants, and the index is not
/// per-tenant, so a probe on `sub_tenant_id` also returns `s9` rows belonging
/// to another tenant (11) and a vertex carrying the wrong label (12). Both
/// must be rejected after hydration, which is the only thing standing in for
/// the Filter the probe removed.
fn fixture() -> MemoryStorage {
    MemoryStorage::with_vertices(
        (1..=10)
            .map(|id| {
                entity(
                    id,
                    "t1",
                    if id % 5 == 0 { "s9" } else { "s0" },
                    if id % 2 == 0 { "person" } else { "org" },
                )
            })
            .chain([
                entity(11, "t2", "s9", "person"),
                VertexRecord::new(12)
                    .with_label("Other")
                    .with_property("tenant_id", ScalarValue::String("t1".into()))
                    .with_property("sub_tenant_id", ScalarValue::String("s9".into()))
                    .with_property("type", ScalarValue::String("person".into())),
            ]),
    )
}

fn property(binding: &str, property: &str) -> PhysicalExpression {
    PhysicalExpression::Property {
        binding: Symbol::new(binding),
        property: Symbol::new(property),
    }
}

fn literal(value: &str) -> PhysicalExpression {
    PhysicalExpression::Value(BoundValue {
        value: ScalarValue::String(value.into()),
        origin: ValueOrigin::Literal,
    })
}

fn equals(binding: &str, name: &str, value: &str) -> PhysicalExpression {
    PhysicalExpression::Binary {
        left: Box::new(property(binding, name)),
        operator: PhysicalBinaryOperator::Equal,
        right: Box::new(literal(value)),
    }
}

/// The access path `plan_filter` picks for these queries: the seek anchors on
/// the first conjunct it finds, which is whichever property was written first.
fn seek_on_tenant() -> GraphPhysicalPlan {
    let mut request = VertexScanRequest::new(Symbol::new("e"), vec![Symbol::new("Entity")]);
    request.predicates.push(equals("e", "tenant_id", "t1"));
    GraphPhysicalPlan::VertexScan {
        scan: SlateDbDataSource::new(&UnknownStatistics).prepare_vertex_scan(request),
    }
}

fn filtered(predicate: PhysicalExpression) -> GraphPhysicalPlan {
    GraphPhysicalPlan::Filter {
        operation: hydradb_cypher_ast::operation::MATCH,
        input: Box::new(seek_on_tenant()),
        predicate,
    }
}

/// `RETURN e` over whatever plan is handed in. Only `physical` is read by
/// `execute_prepared`, so the AST and logical plan are placeholders.
fn project(input: GraphPhysicalPlan) -> PreparedQuery {
    PreparedQuery {
        ast: Arc::new(Document::new(CypherDialect::OpenCypher, Vec::new())),
        logical: Arc::new(GraphLogicalPlan::NodeScan {
            binding: Symbol::new("e"),
            labels: vec![Symbol::new("Entity")],
        }),
        physical: GraphPhysicalPlan::Project {
            context: Default::default(),
            operation: hydradb_cypher_ast::operation::RETURN,
            input: Box::new(input),
            items: vec![PhysicalProjection {
                expression: PhysicalExpression::Identity(Symbol::new("e")),
                alias: Some(Symbol::new("id")),
            }],
            distinct: false,
            post_sort: Vec::new(),
            post_skip: None,
            post_limit: None,
        },
    }
}

fn both_equalities() -> PhysicalExpression {
    PhysicalExpression::Binary {
        left: Box::new(equals("e", "tenant_id", "t1")),
        operator: PhysicalBinaryOperator::And,
        right: Box::new(equals("e", "sub_tenant_id", "s9")),
    }
}

/// `MATCH (e:Entity {tenant_id: $tenant_id, sub_tenant_id: $sub_tenant_id})`,
/// planned the way `plan_filter` plans it: the seek anchors on the first
/// conjunct and the whole predicate stays in the Filter above it.
fn prepared() -> PreparedQuery {
    project(filtered(both_equalities()))
}

fn hydrated(storage: &MemoryStorage) -> Vec<VertexId> {
    let mut ids = storage
        .hydration_requests
        .lock()
        .unwrap()
        .iter()
        .flatten()
        .copied()
        .collect::<Vec<_>>();
    ids.sort_unstable();
    ids.dedup();
    ids
}

fn row_count(result: &QueryResult) -> usize {
    result.rows.len()
}

#[tokio::test]
async fn probe_replaces_the_broad_anchor_with_the_narrow_property() {
    let mut storage = fixture();
    storage.selective_properties = vec!["sub_tenant_id".to_string()];
    let engine = CypherEngine::new(storage.clone());

    let result = engine.execute_prepared(&prepared()).await.expect("execute");

    // Both conjuncts reached the probe, so it had a real choice to make.
    let requests = storage.probe_requests.lock().unwrap().clone();
    assert_eq!(requests.len(), 1, "probe consulted exactly once");
    let properties = requests[0].iter().map(|candidate| candidate.property.as_str()).collect::<Vec<_>>();
    assert!(properties.contains(&"tenant_id"), "{properties:?}");
    assert!(properties.contains(&"sub_tenant_id"), "{properties:?}");

    // Four vertices carry sub_tenant_id = s9, against ten in the tenant. Only
    // those four were read.
    assert_eq!(hydrated(&storage), vec![5, 10, 11, 12]);
    // Two survive: 11 is another tenant's, 12 is not an Entity.
    assert_eq!(row_count(&result), 2);
}

#[tokio::test]
async fn residual_rejects_rows_the_probe_returned() {
    let mut storage = fixture();
    storage.selective_properties = vec!["sub_tenant_id".to_string()];
    let engine = CypherEngine::new(storage.clone());

    let result = engine.execute_prepared(&prepared()).await.expect("execute");

    // Vertex 11 satisfies the probed conjunct and fails the other one. The
    // probe is allowed to return it; dropping it is the residual's job, and
    // nothing else in the plan would.
    let read = storage.hydration_requests.lock().unwrap().clone();
    assert!(
        read.iter().flatten().any(|id| *id == 11),
        "the probe returned the other tenant's row: {read:?}"
    );
    assert_eq!(row_count(&result), 2, "and the residual rejected it");
}

#[tokio::test]
async fn labels_are_still_checked_when_the_probe_wins() {
    let mut storage = fixture();
    storage.selective_properties = vec!["sub_tenant_id".to_string()];
    let engine = CypherEngine::new(storage.clone());

    let result = engine.execute_prepared(&prepared()).await.expect("execute");

    // Vertex 12 matches every property and is not an Entity. The probe walks a
    // property index, which carries no label, so the label check cannot move.
    let read = storage.hydration_requests.lock().unwrap().clone();
    assert!(read.iter().flatten().any(|id| *id == 12), "{read:?}");
    assert_eq!(row_count(&result), 2);
}

/// The probe result is a superset, so a LIMIT above the Filter must still cut
/// it down — and must do so without hydrating the rows it will not return.
#[tokio::test]
async fn a_limit_above_the_filter_survives_the_probe() {
    let mut storage = fixture();
    storage.selective_properties = vec!["sub_tenant_id".to_string()];
    let engine = CypherEngine::new(storage.clone());

    let plan = project(GraphPhysicalPlan::Limit {
        input: Box::new(filtered(both_equalities())),
        count: BoundValue {
            value: ScalarValue::Integer(1),
            origin: ValueOrigin::Literal,
        },
    });

    let result = engine.execute_prepared(&plan).await.expect("execute");

    assert_eq!(row_count(&result), 1);
    assert!(hydrated(&storage).len() < 4, "stopped early: {:?}", hydrated(&storage));
}

#[tokio::test]
async fn declined_probe_leaves_the_planned_path_untouched() {
    let storage = fixture();
    let engine = CypherEngine::new(storage.clone());

    let result = engine.execute_prepared(&prepared()).await.expect("execute");

    // No property is selective here, so the probe declines and the planned
    // seek runs: every vertex in tenant t1 is read, as it is on main today.
    // Vertex 12 is in t1 and is not an Entity, so the seek returns it too.
    let mut expected = (1..=10).collect::<Vec<_>>();
    expected.push(12);
    assert_eq!(hydrated(&storage), expected);
    assert_eq!(row_count(&result), 2);
}

#[tokio::test]
async fn one_property_alone_is_never_probed() {
    let mut storage = fixture();
    storage.selective_properties = vec!["tenant_id".to_string()];
    let engine = CypherEngine::new(storage.clone());

    let plan = project(filtered(equals("e", "tenant_id", "t1")));

    let result = engine.execute_prepared(&plan).await.expect("execute");

    // The planner already anchored on the only equality, so a probe could
    // only confirm it at the cost of another round trip.
    assert!(storage.probe_requests.lock().unwrap().is_empty());
    assert_eq!(row_count(&result), 10);
}

/// The production listing query: an inline property map plus a WHERE clause
/// lower to two nested Filters (`lower.rs:72` and `lower.rs:484`), so the
/// outer Filter sits over a Filter rather than over an access path. The probe
/// has to reach the inner one.
#[tokio::test]
async fn probe_reaches_the_inner_filter_of_a_nested_pair() {
    let mut storage = fixture();
    storage.selective_properties = vec!["sub_tenant_id".to_string()];
    let engine = CypherEngine::new(storage.clone());

    let plan = project(GraphPhysicalPlan::Filter {
        operation: hydradb_cypher_ast::operation::MATCH,
        input: Box::new(filtered(both_equalities())),
        predicate: equals("e", "type", "person"),
    });

    let result = engine.execute_prepared(&plan).await.expect("execute");

    // Consulted once, at the inner Filter. The outer one sits over a Filter,
    // which is not an access path, so it takes the unchanged path.
    assert_eq!(storage.probe_requests.lock().unwrap().len(), 1);
    assert_eq!(hydrated(&storage), vec![5, 10, 11, 12]);
    // Of the two surviving entities, only vertex 10 is a person.
    assert_eq!(row_count(&result), 1);
}

/// `MATCH (e:Entity {tenant_id: .., sub_tenant_id: .., type: ..})` — three
/// equalities, and `type` is the property legacy's probe picks most often
/// after `sub_tenant_id`.
#[tokio::test]
async fn three_equalities_all_reach_the_probe() {
    let mut storage = fixture();
    storage.selective_properties = vec!["type".to_string()];
    let engine = CypherEngine::new(storage.clone());

    let plan = project(filtered(PhysicalExpression::Binary {
        left: Box::new(both_equalities()),
        operator: PhysicalBinaryOperator::And,
        right: Box::new(equals("e", "type", "person")),
    }));

    let result = engine.execute_prepared(&plan).await.expect("execute");

    let offered = offered_properties(&storage);
    assert_eq!(offered, vec!["sub_tenant_id", "tenant_id", "type"]);
    assert_eq!(row_count(&result), 1);
}

/// A disjunct over two different properties covers neither on its own, so
/// seeding from either would silently drop rows.
#[tokio::test]
async fn a_disjunct_over_two_properties_is_not_a_candidate() {
    let mut storage = fixture();
    storage.selective_properties = vec!["sub_tenant_id".to_string(), "type".to_string()];
    let engine = CypherEngine::new(storage.clone());

    let plan = project(filtered(PhysicalExpression::Binary {
        left: Box::new(equals("e", "sub_tenant_id", "s9")),
        operator: PhysicalBinaryOperator::Or,
        right: Box::new(equals("e", "type", "person")),
    }));

    let result = engine.execute_prepared(&plan).await.expect("execute");

    // One candidate at most can come out of this, so the probe never runs and
    // the planned seek answers it.
    assert!(storage.probe_requests.lock().unwrap().is_empty());
    // Every t1 Entity that is an s9 sub-tenant or a person: 2, 4, 5, 6, 8, 10.
    assert_eq!(row_count(&result), 6);
}

/// The same property on both sides of a disjunct still covers the result, so
/// the union of its values is a candidate — which is also how `IN` arrives.
#[tokio::test]
async fn a_disjunct_on_one_property_offers_the_union() {
    let mut storage = fixture();
    storage.selective_properties = vec!["sub_tenant_id".to_string()];
    let engine = CypherEngine::new(storage.clone());

    let plan = project(filtered(PhysicalExpression::Binary {
        left: Box::new(equals("e", "tenant_id", "t1")),
        operator: PhysicalBinaryOperator::And,
        right: Box::new(PhysicalExpression::Binary {
            left: Box::new(equals("e", "sub_tenant_id", "s9")),
            operator: PhysicalBinaryOperator::Or,
            right: Box::new(equals("e", "sub_tenant_id", "s0")),
        }),
    }));

    let result = engine.execute_prepared(&plan).await.expect("execute");

    let requests = storage.probe_requests.lock().unwrap().clone();
    let sub = requests[0]
        .iter()
        .find(|candidate| candidate.property == "sub_tenant_id")
        .expect("sub_tenant_id offered");
    assert_eq!(sub.values.len(), 2, "both values, not one: {sub:?}");
    // Every t1 Entity is in s0 or s9.
    assert_eq!(row_count(&result), 10);
}

/// An `IN` list is the same candidate set as the equivalent disjunct.
#[tokio::test]
async fn in_list_becomes_one_candidate_with_every_value() {
    let mut storage = fixture();
    storage.selective_properties = vec!["sub_tenant_id".to_string()];
    let engine = CypherEngine::new(storage.clone());

    let plan = project(filtered(PhysicalExpression::Binary {
        left: Box::new(equals("e", "tenant_id", "t1")),
        operator: PhysicalBinaryOperator::And,
        right: Box::new(PhysicalExpression::InList {
            expression: Box::new(property("e", "sub_tenant_id")),
            // NULL is not a seekable key and must not reach the backend.
            values: vec![
                BoundValue {
                    value: ScalarValue::String("s9".into()),
                    origin: ValueOrigin::Literal,
                },
                BoundValue {
                    value: ScalarValue::Null,
                    origin: ValueOrigin::Literal,
                },
            ],
        }),
    }));

    engine.execute_prepared(&plan).await.expect("execute");

    let requests = storage.probe_requests.lock().unwrap().clone();
    let sub = requests[0]
        .iter()
        .find(|candidate| candidate.property == "sub_tenant_id")
        .expect("sub_tenant_id offered");
    assert_eq!(sub.values, vec![ScalarValue::String("s9".into())]);
}

/// An equality on a different binding restricts a different vertex, so it is
/// not a candidate for this access path.
#[tokio::test]
async fn another_bindings_property_is_not_offered() {
    let mut storage = fixture();
    storage.selective_properties = vec!["sub_tenant_id".to_string()];
    let engine = CypherEngine::new(storage.clone());

    let plan = project(filtered(PhysicalExpression::Binary {
        left: Box::new(equals("e", "tenant_id", "t1")),
        operator: PhysicalBinaryOperator::And,
        right: Box::new(equals("other", "sub_tenant_id", "s9")),
    }));

    engine.execute_prepared(&plan).await.expect("execute");

    // Only `e.tenant_id` belongs to this binding, so one property is in play
    // and the probe never runs.
    assert!(storage.probe_requests.lock().unwrap().is_empty());
}

fn offered_properties(storage: &MemoryStorage) -> Vec<String> {
    let mut properties = storage.probe_requests.lock().unwrap()[0]
        .iter()
        .map(|candidate| candidate.property.clone())
        .collect::<Vec<_>>();
    properties.sort();
    properties
}

/// `Some(vec![])` is an answer, not an absence: the backend walked the index
/// and the property matches nothing, so the query has no rows.
///
/// The engine cannot second-guess it, which is what makes producing one
/// wrongly so costly. A backend that cannot encode a value into index keys has
/// to decline with `None`, because an empty walk here is indistinguishable
/// from a property that is selective to zero rows.
#[tokio::test]
async fn an_empty_probe_answer_is_authoritative() {
    let mut storage = fixture();
    storage.selective_properties = vec!["sub_tenant_id".to_string()];
    storage.probe_answers_empty = true;
    let engine = CypherEngine::new(storage.clone());

    let result = engine.execute_prepared(&prepared()).await.expect("execute");

    assert_eq!(row_count(&result), 0);
    assert!(
        hydrated(&storage).is_empty(),
        "the planned seek must not run as well: {:?}",
        hydrated(&storage)
    );
}
