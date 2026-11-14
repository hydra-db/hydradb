mod common;

use std::sync::Arc;

use common::{entity, MemoryStorage};
use hydradb_cypher_engine::{
    plan_physical, CypherEngine, GraphLogicalPlan, GraphPhysicalPlan, ListParameterMap, LogicalBinaryOperator, LogicalExpression,
    ParameterMap, PhysicalPlanningContext, QueryRow, QueryValue, ScalarValue, Symbol,
};

fn identity_equal(binding: &str, parameter: &str) -> LogicalExpression {
    LogicalExpression::Binary {
        left: Box::new(LogicalExpression::Identity(Symbol::from(binding))),
        operator: LogicalBinaryOperator::Equal,
        right: Box::new(LogicalExpression::Parameter(Symbol::from(parameter))),
    }
}

fn identity_point_count(plan: &GraphPhysicalPlan) -> usize {
    fn find(plan: &GraphPhysicalPlan) -> Option<usize> {
        match plan {
            GraphPhysicalPlan::VertexScan { scan } if scan.access_kind() == "id-points" => {
                Some(scan.properties().estimated_rows.unwrap_or(0) as usize)
            }
            GraphPhysicalPlan::Filter { input, .. } | GraphPhysicalPlan::Project { input, .. } | GraphPhysicalPlan::Sort { input, .. } => {
                find(input)
            }
            GraphPhysicalPlan::NaturalJoin { left, right, .. } => find(left).or_else(|| find(right)),
            _ => None,
        }
    }
    find(plan).unwrap_or_else(|| panic!("expected a plan containing identity points, got {plan:?}"))
}

#[tokio::test]
async fn identity_or_batches_plan_as_direct_points_at_production_sizes() {
    for count in [2_usize, 21, 128] {
        let storage = MemoryStorage::with_vertices((1..=128).map(|id| entity(id, "Entity", &id.to_string())));
        let hydration_requests = Arc::clone(&storage.hydration_requests);
        let engine = CypherEngine::new(storage);
        let parameters = (1..=count)
            .map(|id| (format!("id_{id}"), ScalarValue::Integer(id as i128)))
            .collect::<ParameterMap>();
        let predicate = (1..=count).map(|id| format!("e.id = $id_{id}")).collect::<Vec<_>>().join(" OR ");
        let query = format!("MATCH (e:Entity) WHERE {predicate} RETURN e.id AS id ORDER BY id");

        let prepared = engine.prepare(&query, &parameters).expect("prepare identity OR batch");
        assert_eq!(identity_point_count(&prepared.physical), count);
        assert!(prepared.explain().contains("VertexScan(provider=slatedb, access=id-points"));
        let result = engine.execute_prepared(&prepared).await.expect("execute identity OR batch");
        assert_eq!(result.rows.len(), count);
        assert_eq!(
            hydration_requests.lock().unwrap().as_slice(),
            &[(1..=count as u64).collect::<Vec<_>>()]
        );
    }
}

#[tokio::test]
async fn identity_in_filters_invalid_points_and_preserves_membership_semantics() {
    let storage = MemoryStorage::with_vertices([
        entity(1, "Entity", "one"),
        entity(2, "Entity", "two"),
        entity(3, "Entity", "three"),
        entity(4, "Other", "wrong-label"),
    ]);
    let hydration_requests = Arc::clone(&storage.hydration_requests);
    let engine = CypherEngine::new(storage);
    let lists = ListParameterMap::from([(
        "ids".to_string(),
        vec![
            ScalarValue::Integer(2),
            ScalarValue::String("2".into()),
            ScalarValue::Null,
            ScalarValue::Integer(-1),
            ScalarValue::Integer(2),
            ScalarValue::Integer(4),
            ScalarValue::Integer(99),
        ],
    )]);
    let prepared = engine
        .prepare_with_lists(
            "MATCH (e:Entity) WHERE e.id IN $ids RETURN e.id AS id",
            &ParameterMap::new(),
            &lists,
        )
        .expect("prepare identity IN");

    assert_eq!(identity_point_count(&prepared.physical), 3);
    let result = engine.execute_prepared(&prepared).await.expect("execute identity IN");
    assert_eq!(
        result.rows,
        vec![QueryRow {
            values: vec![QueryValue::VertexId(2)]
        }]
    );
    assert_eq!(hydration_requests.lock().unwrap().as_slice(), &[vec![2, 4, 99]]);
}

#[tokio::test]
async fn identity_points_cover_the_full_u64_domain_only() {
    let storage = MemoryStorage::with_vertices([entity(0, "Entity", "zero"), entity(u64::MAX, "Entity", "maximum")]);
    let hydration_requests = Arc::clone(&storage.hydration_requests);
    let engine = CypherEngine::new(storage);
    let lists = ListParameterMap::from([(
        "ids".to_string(),
        vec![
            ScalarValue::Integer(0),
            ScalarValue::Integer(u64::MAX as i128),
            ScalarValue::Integer(u64::MAX as i128 + 1),
            ScalarValue::Float("0.0".into()),
        ],
    )]);
    let prepared = engine
        .prepare_with_lists(
            "MATCH (e:Entity) WHERE e.id IN $ids RETURN e.id AS id ORDER BY id",
            &ParameterMap::new(),
            &lists,
        )
        .expect("prepare u64 boundary identity IN");

    assert_eq!(identity_point_count(&prepared.physical), 2);
    assert_eq!(
        engine
            .execute_prepared(&prepared)
            .await
            .expect("execute u64 boundary identity IN")
            .rows,
        vec![
            QueryRow {
                values: vec![QueryValue::VertexId(0)]
            },
            QueryRow {
                values: vec![QueryValue::VertexId(u64::MAX)]
            },
        ]
    );
    assert_eq!(hydration_requests.lock().unwrap().as_slice(), &[vec![0, u64::MAX]]);
}

#[tokio::test]
async fn single_identity_equality_preserves_invalid_value_errors() {
    let engine = CypherEngine::new(MemoryStorage::with_vertices([entity(1, "Entity", "one")]));
    for value in [
        ScalarValue::String("1".into()),
        ScalarValue::Integer(-1),
        ScalarValue::Integer(u64::MAX as i128 + 1),
    ] {
        let prepared = engine
            .prepare(
                "MATCH (e:Entity) WHERE e.id = $id RETURN e",
                &ParameterMap::from([("id".to_string(), value)]),
            )
            .expect("prepare invalid single identity equality");
        assert!(prepared.explain().contains("VertexScan(provider=slatedb, access=id-point"));
        let error = engine
            .execute_prepared(&prepared)
            .await
            .expect_err("invalid single identity equality must fail");
        assert!(error.to_string().contains("vertex id"), "{error}");
    }
}

#[tokio::test]
async fn conjunctive_and_stacked_identity_equalities_preserve_invalid_value_errors() {
    let engine = CypherEngine::new(MemoryStorage::with_vertices([entity(1, "Entity", "one")]));
    let parameters = ParameterMap::from([
        ("bad".to_string(), ScalarValue::String("invalid".into())),
        ("good".to_string(), ScalarValue::Integer(1)),
    ]);
    for query in [
        "MATCH (e:Entity) WHERE e.id = $bad AND e.id = $good RETURN e",
        "MATCH (e:Entity {id: $bad}) WHERE e.id = $good RETURN e",
    ] {
        let prepared = engine.prepare(query, &parameters).expect("prepare invalid equality intersection");
        assert!(
            prepared.explain().contains("VertexScan(provider=slatedb, access=id-point"),
            "{query}"
        );
        let result = engine.execute_prepared(&prepared).await;
        assert!(result.is_err(), "{query}: {result:?}");
        let error = result.unwrap_err();
        assert!(error.to_string().contains("vertex id"), "{query}: {error}");
    }
    let prepared = engine
        .prepare("MATCH (e:Entity) WHERE e.id = $good AND e.id = $bad RETURN e", &parameters)
        .expect("prepare valid-first equality intersection");
    assert!(prepared.explain().contains("VertexScan(provider=slatedb, access=id-point"));
    assert!(engine
        .execute_prepared(&prepared)
        .await
        .expect("execute valid-first equality intersection")
        .rows
        .is_empty());
}

#[tokio::test]
async fn outer_invalid_identity_equality_does_not_cross_an_earlier_empty_filter() {
    let mut record = entity(1, "Entity", "one");
    record.properties.insert("rank".to_string(), ScalarValue::Integer(1));
    let engine = CypherEngine::new(MemoryStorage::with_vertices([record]));
    let prepared = engine
        .prepare(
            "MATCH (e:Entity {rank: 0}) WHERE e.id = $bad RETURN e",
            &ParameterMap::from([("bad".to_string(), ScalarValue::String("invalid".into()))]),
        )
        .expect("prepare invalid outer identity equality");
    assert!(engine
        .execute_prepared(&prepared)
        .await
        .expect("earlier empty filter suppresses later invalid equality")
        .rows
        .is_empty());
}

#[tokio::test]
async fn single_identity_equality_keeps_priority_over_a_collected_batch() {
    let engine = CypherEngine::new(MemoryStorage::with_vertices([entity(1, "Entity", "one")]));
    let prepared = engine
        .prepare(
            "MATCH (e:Entity) WHERE e.id = $bad AND e.id IN [1, 2] RETURN e",
            &ParameterMap::from([("bad".to_string(), ScalarValue::String("invalid".into()))]),
        )
        .expect("prepare equality and identity batch");
    assert!(prepared.explain().contains("VertexScan(provider=slatedb, access=id-point"));
    let error = engine
        .execute_prepared(&prepared)
        .await
        .expect_err("single equality retains legacy validation priority");
    assert!(error.to_string().contains("vertex id"), "{error}");
}

#[tokio::test]
async fn request_independent_identity_points_preserve_seeded_match_rows() {
    let storage = MemoryStorage::with_vertices([entity(1, "Entity", "one"), entity(2, "Entity", "two")]);
    let hydration_requests = Arc::clone(&storage.hydration_requests);
    let engine = CypherEngine::new(storage);
    let prepared = engine
        .prepare(
            "UNWIND [10, 20] AS seed MATCH (e:Entity) WHERE e.id IN [1, 2] RETURN seed, e.id AS id ORDER BY seed, id",
            &ParameterMap::new(),
        )
        .expect("prepare seeded identity IN");
    assert_eq!(identity_point_count(&prepared.physical), 2);
    assert_eq!(
        engine.execute_prepared(&prepared).await.expect("execute seeded identity IN").rows,
        vec![
            QueryRow {
                values: vec![QueryValue::Scalar(ScalarValue::Integer(10)), QueryValue::VertexId(1)]
            },
            QueryRow {
                values: vec![QueryValue::Scalar(ScalarValue::Integer(10)), QueryValue::VertexId(2)]
            },
            QueryRow {
                values: vec![QueryValue::Scalar(ScalarValue::Integer(20)), QueryValue::VertexId(1)]
            },
            QueryRow {
                values: vec![QueryValue::Scalar(ScalarValue::Integer(20)), QueryValue::VertexId(2)]
            },
        ]
    );
    assert_eq!(hydration_requests.lock().unwrap().as_slice(), &[vec![1, 2]]);

    for label in ["Entity", "Missing"] {
        let prepared = engine
            .prepare(
                &format!("UNWIND [10] AS seed MATCH (e:{label}) WHERE e.id = $bad RETURN seed, e"),
                &ParameterMap::from([("bad".to_string(), ScalarValue::String("invalid".into()))]),
            )
            .expect("prepare seeded invalid singleton identity");
        assert!(engine
            .execute_prepared(&prepared)
            .await
            .expect("join-local singleton identity validation stays residual")
            .rows
            .is_empty());
    }
}

#[tokio::test]
async fn identity_points_preserve_optional_match_null_extension() {
    let engine = CypherEngine::new(MemoryStorage::with_vertices([entity(1, "Entity", "one")]));
    let prepared = engine
        .prepare(
            "UNWIND [10, 20] AS seed OPTIONAL MATCH (e:Entity) WHERE e.id IN [999] RETURN seed, e.id AS id ORDER BY seed",
            &ParameterMap::new(),
        )
        .expect("prepare optional identity IN");
    assert_eq!(identity_point_count(&prepared.physical), 1);
    assert_eq!(
        engine.execute_prepared(&prepared).await.expect("execute optional identity IN").rows,
        vec![
            QueryRow {
                values: vec![QueryValue::Scalar(ScalarValue::Integer(10)), QueryValue::Null]
            },
            QueryRow {
                values: vec![QueryValue::Scalar(ScalarValue::Integer(20)), QueryValue::Null]
            },
        ]
    );
}

#[tokio::test]
async fn empty_and_conjunctive_identity_collections_keep_stable_point_access() {
    let storage = MemoryStorage::with_vertices([entity(1, "Entity", "one"), entity(2, "Entity", "two"), entity(3, "Entity", "three")]);
    let hydration_requests = Arc::clone(&storage.hydration_requests);
    let engine = CypherEngine::new(storage);

    let empty = ListParameterMap::from([("ids".to_string(), Vec::new())]);
    let prepared = engine
        .prepare_with_lists(
            "MATCH (e:Entity) WHERE e.id IN $ids RETURN e.id AS id",
            &ParameterMap::new(),
            &empty,
        )
        .expect("prepare empty identity IN");
    assert!(identity_point_count(&prepared.physical) == 0);
    assert!(engine
        .execute_prepared(&prepared)
        .await
        .expect("execute empty identity IN")
        .rows
        .is_empty());
    assert!(hydration_requests.lock().unwrap().is_empty());

    let all_invalid = ListParameterMap::from([(
        "ids".to_string(),
        vec![ScalarValue::Null, ScalarValue::String("1".into()), ScalarValue::Integer(-1)],
    )]);
    let prepared = engine
        .prepare_with_lists(
            "MATCH (e:Entity) WHERE e.id IN $ids RETURN e.id AS id",
            &ParameterMap::new(),
            &all_invalid,
        )
        .expect("prepare all-invalid identity IN");
    assert!(identity_point_count(&prepared.physical) == 0);
    assert!(engine
        .execute_prepared(&prepared)
        .await
        .expect("execute all-invalid identity IN")
        .rows
        .is_empty());
    assert!(hydration_requests.lock().unwrap().is_empty());

    let lists = ListParameterMap::from([
        ("left".to_string(), vec![ScalarValue::Integer(1), ScalarValue::Integer(2)]),
        ("right".to_string(), vec![ScalarValue::Integer(2), ScalarValue::Integer(3)]),
    ]);
    let prepared = engine
        .prepare_with_lists(
            "MATCH (e:Entity) WHERE e.id IN $left AND e.id IN $right RETURN e.id AS id",
            &ParameterMap::new(),
            &lists,
        )
        .expect("prepare intersecting identity collections");
    assert_eq!(identity_point_count(&prepared.physical), 1);
    assert_eq!(
        engine
            .execute_prepared(&prepared)
            .await
            .expect("execute intersecting identity collections")
            .rows,
        vec![QueryRow {
            values: vec![QueryValue::VertexId(2)]
        }]
    );
}

#[test]
fn public_planning_boundary_normalizes_manual_identity_or_plans() {
    let logical = GraphLogicalPlan::Filter {
        operation: hydradb_cypher_ast::operation::MATCH,
        input: Box::new(GraphLogicalPlan::NodeScan {
            binding: Symbol::from("e"),
            labels: vec![Symbol::from("Entity")],
        }),
        predicate: LogicalExpression::Binary {
            left: Box::new(identity_equal("e", "a")),
            operator: LogicalBinaryOperator::Or,
            right: Box::new(identity_equal("e", "b")),
        },
    };
    let context = PhysicalPlanningContext::default()
        .with_parameter("a", ScalarValue::Integer(1))
        .with_parameter("b", ScalarValue::Integer(2));

    let physical = plan_physical(&logical, &context).expect("plan manual identity OR");
    assert_eq!(identity_point_count(&physical), 2);
}

#[tokio::test]
async fn identity_in_truth_and_plan_shape_stay_semantic_and_low_cardinality() {
    let storage = MemoryStorage::with_vertices([entity(1, "Entity", "one"), entity(2, "Entity", "two")]);
    let engine = CypherEngine::new(storage);
    let one = ListParameterMap::from([("ids".to_string(), vec![ScalarValue::Integer(1)])]);
    let two = ListParameterMap::from([("ids".to_string(), vec![ScalarValue::Integer(1), ScalarValue::Integer(2)])]);
    let query = "MATCH (e:Entity) RETURN e.id AS id, e.id IN $ids AS hit, NOT (e.id IN $ids) AS miss ORDER BY id";

    let one_prepared = engine
        .prepare_with_lists(query, &ParameterMap::new(), &one)
        .expect("prepare identity membership projection");
    let two_prepared = engine
        .prepare_with_lists(query, &ParameterMap::new(), &two)
        .expect("prepare second identity membership projection");
    assert_eq!(one_prepared.plan_shape(), two_prepared.plan_shape());
    assert!(!one_prepared.plan_shape().contains("values=[1]"));

    assert_eq!(
        engine
            .execute_prepared(&one_prepared)
            .await
            .expect("execute identity membership projection")
            .rows,
        vec![
            QueryRow {
                values: vec![
                    QueryValue::VertexId(1),
                    QueryValue::Scalar(ScalarValue::Boolean(true)),
                    QueryValue::Scalar(ScalarValue::Boolean(false)),
                ],
            },
            QueryRow {
                values: vec![
                    QueryValue::VertexId(2),
                    QueryValue::Scalar(ScalarValue::Boolean(false)),
                    QueryValue::Scalar(ScalarValue::Boolean(true)),
                ],
            },
        ]
    );
}

#[test]
fn mixed_or_and_row_dependent_in_do_not_embed_identity_filters() {
    let engine = CypherEngine::new(MemoryStorage::with_vertices([entity(1, "Entity", "one")]));
    let parameters = ParameterMap::from([
        ("id".to_string(), ScalarValue::Integer(1)),
        ("name".to_string(), ScalarValue::String("one".into())),
    ]);
    let mixed = engine
        .prepare("MATCH (e:Entity) WHERE e.id = $id OR e.entity_id = $name RETURN e", &parameters)
        .expect("prepare mixed identity/property OR");
    assert!(!mixed.explain().contains("VertexScan(provider=slatedb, access=id-points"));

    let dependent = engine
        .prepare(
            "UNWIND [1] AS wanted MATCH (e:Entity) WHERE e.id IN [wanted] RETURN e",
            &ParameterMap::new(),
        )
        .expect_err("row-dependent identity IN remains unsupported rather than becoming a point scan");
    assert!(dependent.to_string().contains("IN list elements must be literals or parameters"));
}

#[test]
fn relationship_id_remains_an_ordinary_stored_property() {
    let engine = CypherEngine::new(MemoryStorage::default());
    let prepared = engine
        .prepare("MATCH (a)-[r:REL]->(b) WHERE r.id IN [1, 2] RETURN r.id", &ParameterMap::new())
        .expect("prepare relationship id property membership");
    assert!(prepared.explain().contains("RelationshipPropertyMultiSeek"));
    assert!(!prepared.explain().contains("VertexScan(provider=slatedb, access=id-points"));
}

#[tokio::test]
async fn unsafe_conjunction_does_not_hide_eager_predicate_errors() {
    let engine = CypherEngine::new(MemoryStorage::with_vertices([entity(1, "Entity", "one")]));
    let prepared = engine
        .prepare("MATCH (e:Entity) WHERE e.id IN [999] AND 1 / 0 = 0 RETURN e", &ParameterMap::new())
        .expect("prepare unsafe conjunction");
    assert!(!prepared.explain().contains("VertexScan(provider=slatedb, access=id-points"));
    let error = engine
        .execute_prepared(&prepared)
        .await
        .expect_err("full predicate must still divide by zero");
    assert!(error.to_string().contains("zero"), "{error}");
}

#[tokio::test]
async fn identity_pushdown_does_not_cross_fallible_inner_filters_or_left_join_branches() {
    let mut entity_record = entity(1, "Entity", "one");
    entity_record.properties.insert("rank".to_string(), ScalarValue::Integer(1));
    let mut other_record = entity(2, "Other", "two");
    other_record.properties.insert("rank".to_string(), ScalarValue::Integer(1));
    let engine = CypherEngine::new(MemoryStorage::with_vertices([entity_record, other_record]));
    for query in [
        "MATCH (e:Entity {rank: 1 / 0}) WHERE e.id IN [999] RETURN e",
        "MATCH (e:Entity), (x:Other {rank: 1 / 0}) WHERE e.id IN [999] RETURN e",
    ] {
        let prepared = engine.prepare(query, &ParameterMap::new()).expect("prepare fallible boundary");
        let error = engine
            .execute_prepared(&prepared)
            .await
            .expect_err("earlier predicate must still divide by zero");
        assert!(error.to_string().contains("zero"), "{query}: {error}");
    }
}
