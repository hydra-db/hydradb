//! Capability is decided before storage is touched.
//!
//! Two claims, and the second is the one with teeth:
//! the engine can name why it will not run a plan, and it can do so without
//! opening a read session. `MemoryStorage.read_sessions` counts `begin_read`,
//! so a shape refused with the counter still at zero is a shape that cost no
//! storage - which is the property the ticket asks a test to assert.
//!
//! The unsupported plans here are built by hand because lowering cannot yet
//! produce them: a nested `Project` needs `WITH`, and a binding-level `Union`
//! needs UNION below an arm boundary. Both arrive in later changes, and
//! these tests are what will stop them arriving by accident.

mod common;

use hydradb_cypher_ast::QueryConjunction;
use std::sync::Arc;

use common::{entity, MemoryStorage};
use hydradb_cypher_ast::{CypherDialect, CypherVersion, Document};
use hydradb_cypher_engine::{
    physical_capability, CypherEngine, EngineCapability, GraphLogicalPlan, GraphPhysicalPlan, ParameterMap, PhysicalExpression,
    PhysicalProjection, PreparedQuery, Symbol, NESTED_PROJECT_UNNAMED, RESULT_PLAN_ROOT, UNION_BINDING_MISMATCH, UNION_COLUMN_MISMATCH,
    UNION_EMPTY,
};
use hydradb_cypher_engine::{SlateDbDataSource, UnknownStatistics, VertexScanRequest};

fn document() -> Arc<Document> {
    Arc::new(Document::new(CypherDialect::Neo4j(CypherVersion::V25), Vec::new()))
}

fn scan(binding: &str) -> GraphPhysicalPlan {
    GraphPhysicalPlan::VertexScan {
        scan: SlateDbDataSource::new(&UnknownStatistics).prepare_vertex_scan(VertexScanRequest::new(Symbol::from(binding), Vec::new())),
    }
}

fn project(input: GraphPhysicalPlan, binding: &str, alias: Option<&str>) -> GraphPhysicalPlan {
    GraphPhysicalPlan::Project {
        context: Default::default(),
        operation: hydradb_cypher_ast::operation::RETURN,
        input: Box::new(input),
        items: vec![PhysicalProjection {
            expression: PhysicalExpression::Binding(Symbol::from(binding)),
            alias: alias.map(Symbol::from),
        }],
        distinct: false,
        post_sort: Vec::new(),
        post_skip: None,
        post_limit: None,
    }
}

fn project_items(input: GraphPhysicalPlan, items: Vec<PhysicalProjection>) -> GraphPhysicalPlan {
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

/// A `PreparedQuery` whose logical half is a placeholder.
///
/// These plans have no lowering that produces them, so there is no logical
/// tree to carry. The physical half is what the capability check and the
/// executor both read.
fn prepared(physical: GraphPhysicalPlan) -> PreparedQuery {
    PreparedQuery {
        ast: document(),
        logical: Arc::new(GraphLogicalPlan::NodeScan {
            binding: Symbol::from("e"),
            labels: Vec::new(),
        }),
        physical,
    }
}

fn unsupported_plans() -> Vec<(&'static str, GraphPhysicalPlan)> {
    vec![
        (
            // A nested projection is supported now; one that computes a value
            // and never names it still is not, because nothing downstream
            // could refer to it.
            NESTED_PROJECT_UNNAMED,
            // Nested, because an unnamed computed item at the *root* is fine:
            // there it becomes a column name. Below a result plan there is no
            // column, only a scope, and an unnamed value cannot enter one.
            project(
                project_items(
                    scan("e"),
                    vec![PhysicalProjection {
                        expression: PhysicalExpression::Property {
                            binding: Symbol::from("e"),
                            property: Symbol::from("entity_id"),
                        },
                        alias: None,
                    }],
                ),
                "e",
                None,
            ),
        ),
        (
            UNION_BINDING_MISMATCH,
            project(
                GraphPhysicalPlan::Union {
                    arms: vec![scan("e"), scan("other")],
                    all: true,
                    conjunction: QueryConjunction::Union,
                },
                "e",
                None,
            ),
        ),
        (
            UNION_EMPTY,
            GraphPhysicalPlan::Union {
                arms: Vec::new(),
                all: false,
                conjunction: QueryConjunction::Union,
            },
        ),
        (
            UNION_COLUMN_MISMATCH,
            GraphPhysicalPlan::Union {
                arms: vec![project(scan("e"), "e", Some("left")), project(scan("e"), "e", Some("right"))],
                all: true,
                conjunction: QueryConjunction::Union,
            },
        ),
        (RESULT_PLAN_ROOT, scan("e")),
    ]
}

#[test]
fn every_unsupported_shape_is_named_by_its_own_token() {
    for (kind, plan) in unsupported_plans() {
        assert_eq!(
            physical_capability(&plan).unsupported_kind(),
            Some(kind),
            "plan for {kind} was not reported as that shape"
        );
    }
}

#[tokio::test]
async fn an_unsupported_shape_issues_no_storage_request() {
    for (kind, plan) in unsupported_plans() {
        let storage = MemoryStorage::with_vertices([entity(1, "Entity", "a")]);
        let engine = CypherEngine::new(storage.clone());
        let error = engine
            .execute_prepared(&prepared(plan))
            .await
            .expect_err(&format!("{kind} was expected to be refused"));

        assert_eq!(
            *storage.read_sessions.lock().unwrap(),
            0,
            "{kind} opened a read session before being refused: {error}"
        );
        // The refusal is the capability check's, not an operator's, so it says
        // the same thing the pre-execution answer would have said.
        assert!(!error.to_string().is_empty(), "{kind} produced an empty refusal");
    }
}

#[tokio::test]
async fn a_supported_shape_still_opens_exactly_one_session() {
    let storage = MemoryStorage::with_vertices([entity(1, "Entity", "a")]);
    let engine = CypherEngine::new(storage.clone());
    let plan = project(scan("e"), "e", None);
    assert_eq!(physical_capability(&plan), EngineCapability::Supported);

    engine.execute_prepared(&prepared(plan)).await.expect("a supported plan executes");
    assert_eq!(
        *storage.read_sessions.lock().unwrap(),
        1,
        "the capability check changed how many sessions a supported plan opens"
    );
}

/// Unsupported syntax fails rather than panicking, and fails before storage.
///
/// These go through the whole front end - parse, lower, plan - so they cover
/// the rejections that never reach a physical plan at all. Every one of them
/// is refused today; the list is an assertion that they stay refused *and*
/// stay cheap, not a survey of what happens to be rejected.
#[tokio::test]
async fn unsupported_syntax_fails_without_panicking_or_reading() {
    let queries = [
        "MATCH (e:Entity) RETURN e.entity_id =~ 'a.*' AS hit",
        "MATCH (a:Entity)-[:KNOWS*1..2 {since: 1}]->(b:Person) RETURN b.name AS name",
        "this is not a query",
        "",
        "MATCH (e:Entity",
        "MATCH (e:Entity) RETURN e.entity_id AS id ORDER BY",
    ];
    for query in queries {
        let storage = MemoryStorage::with_vertices([entity(1, "Entity", "a")]);
        let engine = CypherEngine::new(storage.clone());
        // A panic unwinds and fails the test; the assertions are that the call
        // returns an error at all, and that the error cost no storage.
        let error = engine
            .execute(query, &ParameterMap::new())
            .await
            .expect_err(&format!("{query:?} was expected to be refused"));
        assert_eq!(
            *storage.read_sessions.lock().unwrap(),
            0,
            "refusing {query:?} opened a read session: {error}"
        );
    }
}

/// A mismatched UNION is settled by the plan, not by running an arm.
///
/// This is the one case in the current surface syntax where the capability
/// check changes an observable outcome rather than only its timing. The
/// executor used to compare column names *after* executing the first arm, so
/// the refusal arrived having already opened a session and collected rows. The
/// zero below is that fix.
#[tokio::test]
async fn a_union_with_mismatched_columns_is_refused_before_any_arm_runs() {
    let storage = MemoryStorage::with_vertices([entity(1, "Entity", "a")]);
    let engine = CypherEngine::new(storage.clone());
    let error = engine
        .execute(
            "MATCH (e:Entity) RETURN e.entity_id AS id \
             UNION MATCH (p:Person) RETURN p.name AS name",
            &ParameterMap::new(),
        )
        .await
        .expect_err("mismatched UNION columns are refused");

    assert!(error.to_string().contains("same column names"), "unexpected refusal: {error}");
    assert_eq!(*storage.read_sessions.lock().unwrap(), 0);
}
