mod common;

use std::{collections::BTreeMap, sync::Arc};

use common::{entity, MemoryStorage};
use hydradb_cypher_ast::{CypherDialect, CypherVersion, Document};
use hydradb_cypher_engine::{
    CypherEngine, ExpandDirection, ExpandRequest, GraphLogicalPlan, GraphPhysicalPlan, ListParameterMap, ParameterMap, PatternDirection,
    PhysicalExpression, PhysicalProjection, PreparedQuery, QueryColumn, QueryResult, QueryRow, QueryValue, RelationshipRecord, ScalarValue,
    StatisticsProvider, Symbol, VertexRecord,
};
use hydradb_cypher_engine::{SlateDbDataSource, UnknownStatistics, VertexScanRequest};

struct FixedStatistics {
    vertex_count: u64,
}

#[derive(Default)]
struct CostStatistics {
    vertices: BTreeMap<Vec<Symbol>, u64>,
    property_values: BTreeMap<(Symbol, ScalarValue), u64>,
    relationships: BTreeMap<Vec<Symbol>, u64>,
    expansions: BTreeMap<(Vec<Symbol>, Vec<Symbol>, PatternDirection), u64>,
    relationship_property_values: BTreeMap<(Symbol, ScalarValue), u64>,
}

impl StatisticsProvider for CostStatistics {
    fn vertex_count(&self, labels: &[Symbol]) -> Option<u64> {
        let mut labels = labels.to_vec();
        labels.sort();
        labels.dedup();
        self.vertices.get(&labels).copied()
    }

    fn property_value_count(&self, _labels: &[Symbol], property: &Symbol, value: &ScalarValue) -> Option<u64> {
        self.property_values.get(&(property.clone(), value.clone())).copied()
    }

    fn relationship_count(&self, relationship_types: &[Symbol]) -> Option<u64> {
        let mut relationship_types = relationship_types.to_vec();
        relationship_types.sort();
        relationship_types.dedup();
        self.relationships.get(&relationship_types).copied()
    }

    fn relationship_expansion_count(
        &self,
        source_labels: &[Symbol],
        relationship_types: &[Symbol],
        direction: PatternDirection,
    ) -> Option<u64> {
        let mut source_labels = source_labels.to_vec();
        source_labels.sort();
        source_labels.dedup();
        let mut relationship_types = relationship_types.to_vec();
        relationship_types.sort();
        relationship_types.dedup();
        self.expansions.get(&(source_labels, relationship_types, direction)).copied()
    }

    fn relationship_property_value_count(&self, _relationship_types: &[Symbol], property: &Symbol, value: &ScalarValue) -> Option<u64> {
        self.relationship_property_values.get(&(property.clone(), value.clone())).copied()
    }
}

#[tokio::test]
async fn costs_choose_the_rarest_label_and_preserve_the_full_label_predicate() {
    let storage = MemoryStorage::with_vertices([
        VertexRecord::new(1)
            .with_label("Hot")
            .with_label("Rare")
            .with_property("kind", ScalarValue::String("selected".into())),
        VertexRecord::new(2)
            .with_label("Hot")
            .with_property("kind", ScalarValue::String("selected".into())),
        VertexRecord::new(3)
            .with_label("Rare")
            .with_property("kind", ScalarValue::String("other".into())),
    ]);
    let statistics = CostStatistics {
        vertices: BTreeMap::from([
            (vec![Symbol::from("Hot")], 1_000),
            (vec![Symbol::from("Rare")], 2),
            (vec![Symbol::from("Hot"), Symbol::from("Rare")], 1),
        ]),
        property_values: BTreeMap::from([((Symbol::from("kind"), ScalarValue::String("selected".into())), 50)]),
        ..CostStatistics::default()
    };
    let engine = CypherEngine::new(storage);
    let prepared = engine
        .prepare_with_statistics(
            "MATCH (e:Hot:Rare) WHERE e.kind = $kind RETURN e",
            &ParameterMap::from([("kind".to_string(), ScalarValue::String("selected".into()))]),
            &statistics,
        )
        .expect("prepare costed multi-label query");
    assert!(
        prepared
            .explain()
            .contains("VertexScan(provider=slatedb, access=label-prefix, binding=e, labels=[Hot, Rare], label=Rare)"),
        "{}",
        prepared.explain()
    );
    assert_eq!(
        engine.execute_prepared(&prepared).await.unwrap().rows,
        vec![QueryRow {
            values: vec![QueryValue::VertexId(1)]
        }]
    );
}

#[test]
fn exact_property_frequency_drives_the_access_path_and_sort_cardinality() {
    let statistics = CostStatistics {
        vertices: BTreeMap::from([(vec![Symbol::from("Entity")], 1_000)]),
        property_values: BTreeMap::from([((Symbol::from("kind"), ScalarValue::String("rare".into())), 1)]),
        ..CostStatistics::default()
    };
    let prepared = CypherEngine::new(())
        .prepare_with_statistics(
            "MATCH (e:Entity) WHERE e.kind = $kind RETURN e ORDER BY e.id",
            &ParameterMap::from([("kind".to_string(), ScalarValue::String("rare".into()))]),
            &statistics,
        )
        .expect("prepare selective property query");
    let explain = prepared.explain();
    assert!(explain.contains("estimated_input_rows=1"), "{explain}");
    assert!(explain.contains("access=property-point"), "{explain}");
}

#[test]
fn high_degree_sources_do_not_make_broad_expansion_look_cheaper_than_a_seek() {
    let statistics = CostStatistics {
        vertices: BTreeMap::from([(vec![Symbol::from("Hub")], 1), (vec![Symbol::from("Person")], 20_000)]),
        relationships: BTreeMap::from([(vec![Symbol::from("KNOWS")], 10_000)]),
        expansions: BTreeMap::from([(
            (vec![Symbol::from("Hub")], vec![Symbol::from("KNOWS")], PatternDirection::Outgoing),
            10_000,
        )]),
        relationship_property_values: BTreeMap::from([((Symbol::from("weight"), ScalarValue::Integer(7)), 10)]),
        ..CostStatistics::default()
    };
    let prepared = CypherEngine::new(())
        .prepare_with_statistics(
            "MATCH (a:Hub)-[r:KNOWS]->(b:Person) WHERE r.weight = 7 RETURN b",
            &ParameterMap::new(),
            &statistics,
        )
        .expect("prepare a high-degree relationship predicate");
    let explain = prepared.explain();
    assert!(explain.contains("RelationshipPropertySeek"), "{explain}");
}

#[test]
fn selective_low_degree_sources_prefer_anchored_expansion_over_a_global_seek() {
    let statistics = CostStatistics {
        vertices: BTreeMap::from([(vec![Symbol::from("Rare")], 1), (vec![Symbol::from("Person")], 20_000)]),
        relationships: BTreeMap::from([(vec![Symbol::from("KNOWS")], 10_000)]),
        expansions: BTreeMap::from([(
            (vec![Symbol::from("Rare")], vec![Symbol::from("KNOWS")], PatternDirection::Outgoing),
            1,
        )]),
        relationship_property_values: BTreeMap::from([((Symbol::from("weight"), ScalarValue::Integer(7)), 10)]),
        ..CostStatistics::default()
    };
    let prepared = CypherEngine::new(())
        .prepare_with_statistics(
            "MATCH (a:Rare)-[r:KNOWS]->(b:Person) WHERE r.weight = 7 RETURN b",
            &ParameterMap::new(),
            &statistics,
        )
        .expect("prepare a selective low-degree expansion");
    let explain = prepared.explain();
    assert!(explain.contains("ExpandExec("), "{explain}");
    assert!(!explain.contains("RelationshipPropertySeek"), "{explain}");
}

/// A range probe never shows the filter a relationship below its bound. That
/// is sound only when the rest of the predicate cannot fail: evaluation is
/// eager, so `1 / r.timestamp` must still see, and fail on, a zero timestamp.
#[test]
fn a_range_probe_is_planned_only_under_a_predicate_that_cannot_fail() {
    let explain = |predicate: &str| {
        CypherEngine::new(())
            .prepare(
                &format!("MATCH (a:Entity)-[r:RELATES]->(b:Entity) WHERE {predicate} RETURN b"),
                &ParameterMap::new(),
            )
            .unwrap_or_else(|error| panic!("prepare {predicate}: {error}"))
            .explain()
    };
    for total in [
        "r.timestamp >= 30",
        "r.timestamp >= 30 AND (r.superseded_by IS NULL OR r.superseded_by = '')",
    ] {
        let plan = explain(total);
        assert!(plan.contains("RelationshipPropertyRangeProbe"), "{total}: {plan}");
    }
    for fallible in [
        "r.timestamp >= 30 AND 1 / r.timestamp > 0",
        "r.timestamp >= 30 AND r.name STARTS WITH 'a'",
    ] {
        let plan = explain(fallible);
        assert!(!plan.contains("RelationshipPropertyRangeProbe"), "{fallible}: {plan}");
        assert!(plan.contains("ExpandExec("), "{fallible}: {plan}");
    }
}

#[tokio::test]
async fn native_values_survive_scalar_not_equal_filters() {
    let storage = MemoryStorage::with_vertices([
        VertexRecord::new(1).with_property("value", ScalarValue::Date(20_388)),
        VertexRecord::new(2).with_property("value", ScalarValue::Integer(0)),
    ]);
    let engine = CypherEngine::new(storage);
    let prepared = engine
        .prepare("MATCH (n) WHERE n.value <> 0 RETURN n", &ParameterMap::new())
        .expect("prepare native inequality");
    assert!(prepared.explain().contains("access=inventory"), "{}", prepared.explain());
    assert_eq!(
        engine.execute_prepared(&prepared).await.expect("execute native inequality").rows,
        vec![QueryRow {
            values: vec![QueryValue::VertexId(1)]
        }]
    );
}

/// ENG-3254: `'x' IN n.tags` tests membership in a stored list. The list is
/// known only per row, so it is evaluated as a filter rather than planned as a
/// property seek, and a literal right side still plans a multi-seek.
#[tokio::test]
async fn membership_in_a_stored_list_property_filters_each_row() {
    let tags = |values: &[&str]| ScalarValue::List(values.iter().map(|value| ScalarValue::String((*value).into())).collect());
    let storage = MemoryStorage::with_vertices([
        entity(1, "Person", "alice").with_property("tags", tags(&["engineer", "database"])),
        entity(2, "Person", "bob").with_property("tags", tags(&["designer"])),
        entity(3, "Person", "carol").with_property("tags", tags(&[])),
        entity(4, "Person", "dan"),
    ]);
    let engine = CypherEngine::new(storage);
    let ids = |result: QueryResult| result.rows.into_iter().map(|row| row.values).collect::<Vec<_>>();

    for (query, expected) in [
        (
            "MATCH (n:Person) WHERE 'engineer' IN n.tags RETURN n.id AS id",
            vec![vec![QueryValue::VertexId(1)]],
        ),
        (
            "MATCH (n:Person) WHERE 'designer' IN n.tags RETURN n.id AS id",
            vec![vec![QueryValue::VertexId(2)]],
        ),
        // Empty is false and missing is unknown, so neither negation matches dan.
        (
            "MATCH (n:Person) WHERE NOT 'engineer' IN n.tags RETURN n.id AS id ORDER BY id",
            vec![vec![QueryValue::VertexId(2)], vec![QueryValue::VertexId(3)]],
        ),
        (
            "MATCH (n:Person) WITH n, n.tags AS tags WHERE 'database' IN tags RETURN n.id AS id",
            vec![vec![QueryValue::VertexId(1)]],
        ),
    ] {
        let prepared = engine
            .prepare(query, &ParameterMap::new())
            .unwrap_or_else(|error| panic!("{query}: {error}"));
        assert!(
            !prepared.explain().contains("access=property-points"),
            "{query}\n{}",
            prepared.explain()
        );
        assert_eq!(ids(engine.execute_prepared(&prepared).await.unwrap()), expected, "{query}");
    }

    let literal = engine
        .prepare(
            "MATCH (n:Person) WHERE n.entity_id IN ['alice', 'bob'] RETURN n.id AS id",
            &ParameterMap::new(),
        )
        .unwrap();
    assert!(literal.explain().contains("access=property-points"), "{}", literal.explain());

    // A null right side is null in every spelling, as in Neo4j: written
    // literally, computed, or carried through WITH.
    for query in [
        "RETURN 1 IN null AS hit",
        "RETURN 1 IN (NULL = 1) AS hit",
        "WITH null AS tags RETURN 'engineer' IN tags AS hit",
    ] {
        let prepared = engine
            .prepare(query, &ParameterMap::new())
            .unwrap_or_else(|error| panic!("{query}: {error}"));
        let result = engine
            .execute_prepared(&prepared)
            .await
            .unwrap_or_else(|error| panic!("{query}: {error}"));
        assert_eq!(ids(result), vec![vec![QueryValue::Null]], "{query}");
    }
    // A non-null literal that is not a list is still rejected while planning.
    for query in ["RETURN 1 IN true AS hit", "RETURN 1 IN 123 AS hit", "RETURN 1 IN {x: []} AS hit"] {
        assert!(
            engine.prepare(query, &ParameterMap::new()).is_err(),
            "{query} must fail at planning"
        );
    }
}

/// ENG-3254: `list[index]` on literals, stored list properties, parameters
/// and WITH aliases. Out-of-range indexes are null; a non-list target or a
/// non-integer index is an error.
#[tokio::test]
async fn list_element_access_reads_one_element() {
    let tags = |values: &[&str]| ScalarValue::List(values.iter().map(|value| ScalarValue::String((*value).into())).collect());
    let engine = CypherEngine::new(MemoryStorage::with_vertices([
        entity(1, "Person", "alice").with_property("tags", tags(&["engineer", "database"]))
    ]));
    let text = |value: &str| QueryValue::Scalar(ScalarValue::String(value.into()));
    let int = |value: i128| QueryValue::Scalar(ScalarValue::Integer(value));
    let run = |query: &'static str, parameters: ParameterMap| {
        let engine = &engine;
        async move {
            let prepared = engine
                .prepare(query, &parameters)
                .unwrap_or_else(|error| panic!("{query}: {error}"));
            engine
                .execute_prepared(&prepared)
                .await
                .unwrap_or_else(|error| panic!("{query}: {error}"))
                .rows
                .into_iter()
                .map(|row| row.values)
                .collect::<Vec<_>>()
        }
    };

    assert_eq!(run("RETURN [1, 2, 3][0] AS v", ParameterMap::new()).await, vec![vec![int(1)]]);
    assert_eq!(run("RETURN [1, 2, 3][-1] AS v", ParameterMap::new()).await, vec![vec![int(3)]]);
    assert_eq!(run("RETURN [[1]][0][0] AS v", ParameterMap::new()).await, vec![vec![int(1)]]);
    // An unaliased index keeps its written form as the column name.
    let unaliased = engine.prepare("RETURN [[1]][0][0], [1, 2][-1]", &ParameterMap::new()).unwrap();
    assert_eq!(
        unaliased.column_names().unwrap(),
        vec!["[[1]][0][0]".to_string(), "[1, 2][-1]".to_string()]
    );
    // A parameter index keeps its spelling, so the column name does not
    // change with the value the query is run with.
    for i in [0, 1, 7] {
        let prepared = engine
            .prepare(
                "MATCH (n:Person) RETURN n.tags[$i]",
                &ParameterMap::from([("i".to_string(), ScalarValue::Integer(i))]),
            )
            .unwrap();
        assert_eq!(prepared.column_names().unwrap(), vec!["n.tags[$i]".to_string()], "$i = {i}");
    }
    assert_eq!(
        run("RETURN [1, 2, 3][5] AS v", ParameterMap::new()).await,
        vec![vec![QueryValue::Null]]
    );
    assert_eq!(
        run(
            "MATCH (n:Person) RETURN n.tags[0] AS first, n.tags[-1] AS last, n.tags[5] AS missing, n.missing[0] AS absent",
            ParameterMap::new()
        )
        .await,
        vec![vec![text("engineer"), text("database"), QueryValue::Null, QueryValue::Null]]
    );
    assert_eq!(
        run(
            "MATCH (n:Person) WITH n.tags AS tags RETURN tags[$i] AS tag",
            ParameterMap::from([("i".to_string(), ScalarValue::Integer(1))])
        )
        .await,
        vec![vec![text("database")]]
    );
    assert_eq!(
        run(
            "MATCH (n:Person) WHERE n.tags[0] = 'engineer' RETURN n.id AS id",
            ParameterMap::new()
        )
        .await,
        vec![vec![QueryValue::VertexId(1)]]
    );

    for query in [
        "RETURN 123[0] AS v",
        "RETURN 'abc'[0] AS v",
        "RETURN [1, 2][1.5] AS v",
        "RETURN [1, 2]['0'] AS v",
    ] {
        let failed = match engine.prepare(query, &ParameterMap::new()) {
            Err(_) => true,
            Ok(prepared) => engine.execute_prepared(&prepared).await.is_err(),
        };
        assert!(failed, "{query} must fail");
    }
}

impl StatisticsProvider for FixedStatistics {
    fn vertex_count(&self, _labels: &[Symbol]) -> Option<u64> {
        Some(self.vertex_count)
    }
}

#[tokio::test]
async fn prepares_explains_and_executes_the_same_multi_seek() {
    let storage = MemoryStorage::with_vertices([
        entity(1, "Entity", "a"),
        entity(2, "Entity", "b"),
        entity(3, "Entity", "z"),
        entity(4, "Other", "a"),
    ]);
    let engine = CypherEngine::new(storage.clone());
    let parameters = ParameterMap::from([
        ("a".to_string(), ScalarValue::String("a".into())),
        ("b".to_string(), ScalarValue::String("b".into())),
    ]);

    let prepared = engine
        .prepare("MATCH (e:Entity) WHERE e.entity_id = $a OR e.entity_id = $b RETURN e", &parameters)
        .expect("prepare first Cypher 25 query");
    let GraphPhysicalPlan::Project { input, .. } = &prepared.physical else {
        panic!("expected projection root");
    };
    let GraphPhysicalPlan::Filter { input, .. } = input.as_ref() else {
        panic!("expected residual filter");
    };
    assert!(matches!(
        input.as_ref(),
        GraphPhysicalPlan::VertexScan { scan } if scan.access_kind() == "property-points"
    ));
    assert!(prepared.explain().contains("values=[$a, $b]"));

    let result = engine.execute_prepared(&prepared).await.expect("execute exact prepared plan");
    assert_eq!(
        result,
        QueryResult {
            columns: vec![QueryColumn { name: "e".to_string() }],
            rows: vec![
                QueryRow {
                    values: vec![QueryValue::VertexId(1)]
                },
                QueryRow {
                    values: vec![QueryValue::VertexId(2)]
                },
            ],
        }
    );
    assert_eq!(*storage.read_sessions.lock().unwrap(), 1);
    assert_eq!(storage.seek_values.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn duplicate_parameter_values_issue_one_semantic_seek() {
    let storage = MemoryStorage::with_vertices([entity(1, "Entity", "a")]);
    let engine = CypherEngine::new(storage.clone());
    let value = ScalarValue::String("a".into());
    let parameters = ParameterMap::from([("a".to_string(), value.clone()), ("b".to_string(), value)]);
    let result = engine
        .execute("MATCH (e:Entity) WHERE e.entity_id = $a OR e.entity_id = $b RETURN e", &parameters)
        .await
        .expect("execute duplicate-valued parameters");
    assert_eq!(result.rows.len(), 1);
    assert_eq!(storage.seek_values.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn property_seek_and_residual_use_cypher_numeric_equality() {
    let storage = MemoryStorage::with_vertices([
        VertexRecord::new(1)
            .with_label("Entity")
            .with_property("rank", ScalarValue::Float("1.0".into())),
        VertexRecord::new(2)
            .with_label("Entity")
            .with_property("rank", ScalarValue::Float("2.0".into())),
    ]);
    let engine = CypherEngine::new(storage);
    let parameters = ParameterMap::from([("rank".to_string(), ScalarValue::Integer(1))]);
    let result = engine
        .execute("MATCH (e:Entity) WHERE e.rank = $rank RETURN e", &parameters)
        .await
        .expect("execute cross-numeric-type equality");
    assert_eq!(
        result.rows,
        vec![QueryRow {
            values: vec![QueryValue::VertexId(1)]
        }]
    );
}

#[tokio::test]
async fn filter_uses_cypher_three_valued_or() {
    let storage = MemoryStorage::with_vertices([entity(1, "Entity", "a"), entity(2, "Entity", "b")]);
    let engine = CypherEngine::new(storage);
    let parameters = ParameterMap::from([
        ("missing".to_string(), ScalarValue::String("x".into())),
        ("selected".to_string(), ScalarValue::String("b".into())),
    ]);
    let result = engine
        .execute(
            "MATCH (e:Entity) WHERE e.missing = $missing OR e.entity_id = $selected RETURN e",
            &parameters,
        )
        .await
        .expect("execute NULL OR boolean predicate");
    assert_eq!(
        result.rows,
        vec![QueryRow {
            values: vec![QueryValue::VertexId(2)]
        }]
    );
}

#[tokio::test]
async fn unlabeled_match_executes_all_vertex_scan() {
    let storage = MemoryStorage::with_vertices([entity(2, "Entity", "b"), entity(1, "Entity", "a")]);
    let engine = CypherEngine::new(storage);
    let binding = Symbol::from("e");
    let prepared = PreparedQuery {
        ast: Arc::new(Document::new(CypherDialect::Neo4j(CypherVersion::V25), Vec::new())),
        logical: Arc::new(GraphLogicalPlan::Project {
            context: Default::default(),
            operation: hydradb_cypher_ast::operation::RETURN,
            input: Box::new(GraphLogicalPlan::NodeScan {
                binding: binding.clone(),
                labels: Vec::new(),
            }),
            items: Vec::new(),
            distinct: false,
            post_sort: Vec::new(),
            post_skip: None,
            post_limit: None,
        }),
        physical: GraphPhysicalPlan::Project {
            context: Default::default(),
            operation: hydradb_cypher_ast::operation::RETURN,
            input: Box::new(GraphPhysicalPlan::VertexScan {
                scan: SlateDbDataSource::new(&UnknownStatistics).prepare_vertex_scan(VertexScanRequest::new(binding.clone(), Vec::new())),
            }),
            items: vec![PhysicalProjection {
                expression: PhysicalExpression::Binding(binding),
                alias: None,
            }],
            distinct: false,
            post_sort: Vec::new(),
            post_skip: None,
            post_limit: None,
        },
    };
    let result = engine.execute_prepared(&prepared).await.expect("execute all vertex scan");
    assert_eq!(
        result.rows,
        vec![
            QueryRow {
                values: vec![QueryValue::VertexId(1)]
            },
            QueryRow {
                values: vec![QueryValue::VertexId(2)]
            },
        ]
    );
}

#[tokio::test]
async fn outgoing_expand_runs_from_seek_and_canonicalizes_backend_results() {
    let storage = MemoryStorage::with_graph(
        [
            entity(1, "Entity", "a"),
            entity(2, "Person", "b"),
            entity(3, "Person", "c"),
            entity(4, "Other", "d"),
            entity(5, "Entity", "other"),
        ],
        [
            RelationshipRecord::new(20, 1, 3, "KNOWS"),
            RelationshipRecord::new(10, 1, 2, "KNOWS"),
            RelationshipRecord::new(11, 1, 2, "KNOWS"),
            RelationshipRecord::new(12, 1, 4, "KNOWS"),
            RelationshipRecord::new(13, 1, 2, "BLOCKS"),
            RelationshipRecord::new(14, 2, 1, "KNOWS"),
            RelationshipRecord::new(15, 5, 2, "KNOWS"),
        ],
    );
    let engine = CypherEngine::new(storage.clone());
    let parameters = ParameterMap::from([("id".to_string(), ScalarValue::String("a".into()))]);
    let prepared = engine
        .prepare(
            "MATCH (a:Entity)-[r:KNOWS]->(b:Person) WHERE a.entity_id = $id RETURN b",
            &parameters,
        )
        .expect("prepare outgoing expansion");

    let GraphPhysicalPlan::Project { input, .. } = &prepared.physical else {
        panic!("expected projection root");
    };
    let GraphPhysicalPlan::Expand {
        input,
        from,
        relationship,
        relationship_types,
        direction,
        to,
        target_labels,
    } = input.as_ref()
    else {
        panic!("expected expand below projection");
    };
    assert_eq!(from.as_str(), "a");
    assert_eq!(relationship.as_ref().map(Symbol::as_str), Some("r"));
    assert_eq!(relationship_types[0].as_str(), "KNOWS");
    assert_eq!(*direction, PatternDirection::Outgoing);
    assert_eq!(to.as_str(), "b");
    assert_eq!(target_labels[0].as_str(), "Person");
    let GraphPhysicalPlan::Filter { input, .. } = input.as_ref() else {
        panic!("source predicate should be pushed below Expand");
    };
    assert!(matches!(
        input.as_ref(),
        GraphPhysicalPlan::VertexScan { scan } if scan.binding().as_str() == "a" && scan.access_kind() == "property-point"
    ));
    assert!(prepared
        .explain()
        .contains("ExpandExec(from=a, relationship=r, types=[KNOWS], direction=outgoing, to=b, target_labels=[Person])"));

    let result = engine.execute_prepared(&prepared).await.expect("execute outgoing expansion");
    assert_eq!(
        result.rows,
        vec![
            QueryRow {
                values: vec![QueryValue::VertexId(2)]
            },
            QueryRow {
                values: vec![QueryValue::VertexId(2)]
            },
            QueryRow {
                values: vec![QueryValue::VertexId(3)]
            },
        ]
    );
    assert_eq!(*storage.read_sessions.lock().unwrap(), 1);
    assert_eq!(
        *storage.expand_requests.lock().unwrap(),
        vec![ExpandRequest {
            input_vertex_ids: vec![1],
            direction: ExpandDirection::Outgoing,
            relationship_types: vec!["KNOWS".to_string()],
        }]
    );
}

#[tokio::test]
async fn incoming_expand_binds_the_relationship_identity() {
    let storage = MemoryStorage::with_graph(
        [entity(1, "Entity", "a"), entity(2, "Person", "b"), entity(3, "Other", "c")],
        [
            RelationshipRecord::new(30, 2, 1, "KNOWS"),
            RelationshipRecord::new(31, 3, 1, "KNOWS"),
            RelationshipRecord::new(32, 1, 2, "KNOWS"),
        ],
    );
    let engine = CypherEngine::new(storage.clone());
    let parameters = ParameterMap::from([("id".to_string(), ScalarValue::String("a".into()))]);
    let result = engine
        .execute(
            "MATCH (a:Entity)<-[r:KNOWS]-(b:Person) WHERE a.entity_id = $id RETURN r",
            &parameters,
        )
        .await
        .expect("execute incoming expansion");

    assert_eq!(
        result.rows,
        vec![QueryRow {
            values: vec![QueryValue::RelationshipId(30)]
        }]
    );
    assert_eq!(storage.expand_requests.lock().unwrap()[0].direction, ExpandDirection::Incoming);
}

#[tokio::test]
async fn relationship_property_filter_uses_index_seek_and_remains_residual() {
    let storage = MemoryStorage::with_graph(
        [entity(1, "Entity", "a"), entity(2, "Person", "b"), entity(3, "Person", "c")],
        [
            RelationshipRecord::new(40, 1, 2, "KNOWS").with_property("weight", ScalarValue::Integer(1)),
            RelationshipRecord::new(41, 1, 3, "KNOWS").with_property("weight", ScalarValue::Integer(2)),
        ],
    );
    let engine = CypherEngine::new(storage);
    let parameters = ParameterMap::from([
        ("entity".to_string(), ScalarValue::String("a".into())),
        ("weight".to_string(), ScalarValue::Integer(2)),
    ]);
    let prepared = engine
        .prepare(
            "MATCH (a:Entity)-[r:KNOWS]->(b:Person) \
             WHERE a.entity_id = $entity AND r.weight = $weight RETURN b",
            &parameters,
        )
        .expect("prepare relationship-property predicate");
    let GraphPhysicalPlan::Project { input, .. } = &prepared.physical else {
        panic!("expected projection root");
    };
    let GraphPhysicalPlan::Filter { input, .. } = input.as_ref() else {
        panic!("relationship predicate must remain after expansion");
    };
    assert!(matches!(input.as_ref(), GraphPhysicalPlan::RelationshipPropertySeek { .. }));

    let result = engine.execute_prepared(&prepared).await.expect("filter expanded relationships");
    assert_eq!(
        result.rows,
        vec![QueryRow {
            values: vec![QueryValue::VertexId(3)]
        }]
    );
}

#[tokio::test]
async fn multiple_projections_stable_sort_skip_and_limit_match_production_order() {
    let storage = MemoryStorage::with_vertices([
        entity(1, "Entity", "b").with_property("rank", ScalarValue::Integer(2)),
        entity(2, "Entity", "a").with_property("rank", ScalarValue::Integer(2)),
        entity(3, "Entity", "c").with_property("rank", ScalarValue::Integer(1)),
        entity(4, "Entity", "z"),
        entity(5, "Other", "ignored").with_property("rank", ScalarValue::Integer(9)),
    ]);
    let engine = CypherEngine::new(storage);
    let parameters = ParameterMap::from([
        ("skip".to_string(), ScalarValue::Integer(1)),
        ("limit".to_string(), ScalarValue::Integer(2)),
    ]);
    let query = "MATCH (e:Entity) \
                 RETURN e.entity_id AS id, e.rank AS rank \
                 ORDER BY rank DESC, id ASC SKIP $skip LIMIT $limit";
    let prepared = engine
        .prepare_with_statistics(query, &parameters, &FixedStatistics { vertex_count: 4 })
        .expect("prepare projected window query");

    let GraphPhysicalPlan::Project { input, items, .. } = &prepared.physical else {
        panic!("projection root");
    };
    assert_eq!(items.len(), 2);
    let GraphPhysicalPlan::Limit { input, count } = input.as_ref() else {
        panic!("LIMIT follows projection");
    };
    assert_eq!(count.value, ScalarValue::Integer(2));
    let GraphPhysicalPlan::Skip { input, count } = input.as_ref() else {
        panic!("SKIP precedes LIMIT");
    };
    assert_eq!(count.value, ScalarValue::Integer(1));
    let GraphPhysicalPlan::Sort {
        input,
        items,
        estimated_input_rows,
    } = input.as_ref()
    else {
        panic!("ORDER BY precedes windowing");
    };
    assert_eq!(items.len(), 2);
    assert_eq!(*estimated_input_rows, Some(4));
    assert!(matches!(
        input.as_ref(),
        GraphPhysicalPlan::VertexScan { scan } if scan.access_kind() == "label-prefix"
    ));
    let explained = prepared.explain();
    assert!(explained.contains("LimitExec(count=$limit)"));
    assert!(explained.contains("SkipExec(count=$skip)"));
    assert!(explained.contains("SortExec(items=[e.rank DESC, e.entity_id ASC], estimated_input_rows=4)"));

    let result = engine.execute_prepared(&prepared).await.expect("execute projected window query");
    assert_eq!(
        result,
        QueryResult {
            columns: vec![QueryColumn { name: "id".to_string() }, QueryColumn { name: "rank".to_string() },],
            rows: vec![
                QueryRow {
                    values: vec![
                        QueryValue::Scalar(ScalarValue::String("a".into())),
                        QueryValue::Scalar(ScalarValue::Integer(2)),
                    ],
                },
                QueryRow {
                    values: vec![
                        QueryValue::Scalar(ScalarValue::String("b".into())),
                        QueryValue::Scalar(ScalarValue::Integer(2)),
                    ],
                },
            ],
        }
    );
}

#[tokio::test]
async fn limit_bounds_leaf_hydration_and_expand_output_materialization() {
    let storage = MemoryStorage::with_graph(
        [
            entity(1, "Entity", "a"),
            entity(2, "Entity", "b"),
            entity(3, "Entity", "c"),
            entity(10, "Person", "x"),
            entity(11, "Person", "y"),
            entity(12, "Person", "z"),
        ],
        [
            RelationshipRecord::new(100, 1, 10, "KNOWS"),
            RelationshipRecord::new(101, 1, 11, "KNOWS"),
            RelationshipRecord::new(102, 1, 12, "KNOWS"),
        ],
    );
    let engine = CypherEngine::new(storage.clone());

    let vertices = engine
        .execute("MATCH (e:Entity) RETURN e LIMIT 2", &ParameterMap::new())
        .await
        .expect("execute bounded label scan");
    assert_eq!(
        vertices.rows,
        vec![
            QueryRow {
                values: vec![QueryValue::VertexId(1)]
            },
            QueryRow {
                values: vec![QueryValue::VertexId(2)]
            },
        ]
    );
    assert_eq!(
        storage.hydration_requests.lock().unwrap()[0],
        vec![1, 2],
        "LIMIT should keep the label scan from hydrating vertex 3"
    );
    assert_eq!(
        storage.label_scan_limits.lock().unwrap()[0],
        Some(2),
        "LIMIT should reach the storage label scan"
    );

    let expanded = engine
        .execute(
            "MATCH (a:Entity)-[:KNOWS]->(b:Person) WHERE a.entity_id = $id RETURN b LIMIT 1",
            &ParameterMap::from([("id".to_string(), ScalarValue::String("a".into()))]),
        )
        .await
        .expect("execute bounded expansion");
    assert_eq!(
        expanded.rows,
        vec![QueryRow {
            values: vec![QueryValue::VertexId(10)]
        }]
    );
}

#[tokio::test]
async fn expansion_obeys_backend_intermediate_row_limit() {
    let mut storage = MemoryStorage::with_graph(
        [entity(1, "Entity", "a"), entity(2, "Person", "b"), entity(3, "Person", "c")],
        [
            RelationshipRecord::new(10, 1, 2, "KNOWS"),
            RelationshipRecord::new(11, 1, 3, "KNOWS"),
        ],
    );
    storage.max_intermediate_rows = Some(1);
    let engine = CypherEngine::new(storage);
    let error = engine
        .execute(
            "MATCH (a:Entity)-[:KNOWS]->(b:Person) WHERE a.entity_id = $id RETURN b",
            &ParameterMap::from([("id".to_string(), ScalarValue::String("a".into()))]),
        )
        .await
        .expect_err("expansion beyond the configured work limit must fail");
    assert!(error
        .to_string()
        .contains("cypher_expand_relationship_rows exceeded intermediate row limit"));
}

#[test]
fn window_parameters_are_validated_during_physical_planning() {
    let engine = CypherEngine::new(MemoryStorage::default());
    let negative = engine
        .prepare("MATCH (e:Entity) RETURN e SKIP -1", &ParameterMap::new())
        .expect_err("negative SKIP must fail before execution");
    assert!(negative.to_string().contains("SKIP cannot be negative"));

    let parameters = ParameterMap::from([("limit".to_string(), ScalarValue::String("two".into()))]);
    let wrong_type = engine
        .prepare("MATCH (e:Entity) RETURN e LIMIT $limit", &parameters)
        .expect_err("non-integer LIMIT must fail before execution");
    assert!(wrong_type.to_string().contains("LIMIT parameter $limit must be an integer"));
}

#[tokio::test]
async fn nested_aggregate_expressions_execute_in_projection_and_sort() {
    let engine = CypherEngine::new(MemoryStorage::with_vertices([
        entity(1, "Entity", "first").with_property("category", ScalarValue::Integer(10)),
        entity(2, "Entity", "second").with_property("category", ScalarValue::Integer(10)),
        entity(3, "Entity", "third").with_property("category", ScalarValue::Integer(20)),
    ]));

    let result = engine
        .execute(
            "MATCH (e:Entity) RETURN count(*) + 1 AS total ORDER BY count(*) + 1 DESC",
            &ParameterMap::new(),
        )
        .await
        .expect("execute expression containing an aggregate");

    assert_eq!(
        result.rows,
        vec![QueryRow {
            values: vec![QueryValue::Scalar(ScalarValue::Integer(4))],
        }]
    );

    let grouped = engine
        .execute(
            "MATCH (e:Entity) RETURN e.category + count(*) AS total \
             ORDER BY e.category + count(*) ASC",
            &ParameterMap::new(),
        )
        .await
        .expect("execute mixed grouping and aggregate expression");
    assert_eq!(
        grouped.rows,
        vec![
            QueryRow {
                values: vec![QueryValue::Scalar(ScalarValue::Integer(12))],
            },
            QueryRow {
                values: vec![QueryValue::Scalar(ScalarValue::Integer(21))],
            },
        ]
    );

    let empty = engine
        .execute(
            "MATCH (e:Missing) RETURN count(*) + 1 AS total ORDER BY count(*) + 1 DESC",
            &ParameterMap::new(),
        )
        .await
        .expect("execute nested aggregate over an empty input");
    assert_eq!(
        empty.rows,
        vec![QueryRow {
            values: vec![QueryValue::Scalar(ScalarValue::Integer(1))],
        }]
    );
}

#[tokio::test]
async fn legacy_id_inline_properties_and_scalar_predicates_execute_natively() {
    let storage = MemoryStorage::with_vertices([
        entity(1, "Entity", "first")
            .with_property("active", ScalarValue::Boolean(false))
            .with_property("score", ScalarValue::Integer(4))
            .with_property("name", ScalarValue::String("zed".into())),
        entity(2, "Entity", "second")
            .with_property("active", ScalarValue::Boolean(true))
            .with_property("score", ScalarValue::Integer(12))
            .with_property("name", ScalarValue::String("alice".into())),
    ]);
    let hydration_requests = Arc::clone(&storage.hydration_requests);
    let engine = CypherEngine::new(storage);
    let parameters = ParameterMap::from([("id".to_string(), ScalarValue::Integer(2))]);
    let query = "MATCH (e:Entity {id: $id, active: true}) \
                 WHERE e.score >= 10 AND NOT (e.name STARTS WITH 'z') \
                 RETURN e.id AS id, e.name AS name LIMIT 1 + 1";

    let prepared = engine.prepare(query, &parameters).expect("prepare legacy-shaped identity query");
    assert!(prepared.explain().contains("access=id-point"));
    assert!(prepared.explain().contains("STARTS WITH"));
    let result = engine
        .execute_prepared(&prepared)
        .await
        .expect("execute legacy-shaped identity query");

    assert_eq!(
        result.rows,
        vec![QueryRow {
            values: vec![QueryValue::VertexId(2), QueryValue::Scalar(ScalarValue::String("alice".into())),],
        }]
    );
    assert_eq!(hydration_requests.lock().unwrap()[0], vec![2]);
}

#[tokio::test]
async fn inline_relationship_and_target_properties_are_preserved() {
    let storage = MemoryStorage::with_graph(
        [
            entity(1, "Entity", "source"),
            entity(2, "Person", "target"),
            entity(3, "Person", "other"),
        ],
        [
            RelationshipRecord::new(50, 1, 2, "KNOWS")
                .with_property("weight", ScalarValue::Integer(7))
                .with_property("kind", ScalarValue::String("allowed".into())),
            RelationshipRecord::new(51, 1, 3, "KNOWS")
                .with_property("weight", ScalarValue::Integer(7))
                .with_property("kind", ScalarValue::String("blocked".into())),
        ],
    );
    let engine = CypherEngine::new(storage);
    let parameters = ParameterMap::from([
        ("source".to_string(), ScalarValue::Integer(1)),
        ("target".to_string(), ScalarValue::Integer(2)),
    ]);
    let query = "MATCH (a:Entity {id: $source})-[r:KNOWS {weight: 7}]->(b:Person {id: $target}) \
                 WHERE r.kind <> 'blocked' RETURN b.id AS target, r.weight AS weight";

    let result = engine
        .execute(query, &parameters)
        .await
        .expect("execute inline relationship-property query");

    assert_eq!(
        result.rows,
        vec![QueryRow {
            values: vec![QueryValue::VertexId(2), QueryValue::Scalar(ScalarValue::Integer(7)),],
        }]
    );
}

#[tokio::test]
async fn fixed_multi_hop_paths_execute_as_chained_expansions() {
    let storage = MemoryStorage::with_graph(
        [
            entity(1, "Entity", "source"),
            entity(2, "Middle", "middle"),
            entity(3, "Target", "target"),
            entity(4, "Target", "wrong"),
        ],
        [
            RelationshipRecord::new(60, 1, 2, "KNOWS"),
            RelationshipRecord::new(61, 2, 3, "LIKES"),
            RelationshipRecord::new(62, 1, 4, "LIKES"),
        ],
    );
    let engine = CypherEngine::new(storage);
    let parameters = ParameterMap::from([("source".to_string(), ScalarValue::Integer(1))]);
    let query = "MATCH (a:Entity {id: $source})-[:KNOWS]->(b:Middle)-[:LIKES]->(c:Target) \
                 RETURN c.id AS id";

    let prepared = engine.prepare(query, &parameters).expect("prepare fixed multi-hop traversal");
    assert_eq!(prepared.explain().matches("ExpandExec").count(), 2);
    let result = engine.execute_prepared(&prepared).await.expect("execute fixed multi-hop traversal");

    assert_eq!(
        result.rows,
        vec![QueryRow {
            values: vec![QueryValue::VertexId(3)],
        }]
    );
}

#[tokio::test]
async fn multiple_match_comma_joins_and_optional_match_preserve_bindings() {
    let storage = MemoryStorage::with_graph(
        [
            entity(1, "Entity", "source"),
            entity(2, "Person", "shared"),
            entity(3, "Entity", "other"),
        ],
        [
            RelationshipRecord::new(70, 1, 2, "KNOWS"),
            RelationshipRecord::new(71, 3, 2, "LIKES"),
        ],
    );
    let engine = CypherEngine::new(storage);
    let parameters = ParameterMap::from([
        ("source".to_string(), ScalarValue::Integer(1)),
        ("other".to_string(), ScalarValue::Integer(3)),
    ]);

    let sequential = engine
        .execute(
            "MATCH (a:Entity {id: $source}) MATCH (a)-[:KNOWS]->(b:Person) RETURN b.id AS id",
            &parameters,
        )
        .await
        .expect("execute sequential MATCH");
    assert_eq!(
        sequential.rows,
        vec![QueryRow {
            values: vec![QueryValue::VertexId(2)],
        }]
    );

    let joined = engine
        .execute(
            "MATCH (a:Entity {id: $source})-[:KNOWS]->(b:Person), \
             (c:Entity {id: $other})-[:LIKES]->(b) RETURN b.id AS id",
            &parameters,
        )
        .await
        .expect("execute comma-separated natural join");
    assert_eq!(joined.rows, sequential.rows);

    let optional = engine
        .execute(
            "MATCH (a:Entity {id: $source}) OPTIONAL MATCH (a)-[:MISSING]->(b) \
             RETURN a.id AS source, b.id AS missing",
            &parameters,
        )
        .await
        .expect("execute OPTIONAL MATCH");
    assert_eq!(
        optional.rows,
        vec![QueryRow {
            values: vec![QueryValue::VertexId(1), QueryValue::Null],
        }]
    );
}

#[tokio::test]
async fn distinct_grouped_aggregates_ordering_and_windows_match_legacy_semantics() {
    let storage = MemoryStorage::with_vertices([
        entity(1, "Entity", "first")
            .with_property("kind", ScalarValue::String("a".into()))
            .with_property("score", ScalarValue::Integer(10))
            .with_property("name", ScalarValue::String("one".into())),
        entity(2, "Entity", "second")
            .with_property("kind", ScalarValue::String("a".into()))
            .with_property("score", ScalarValue::Integer(20))
            .with_property("name", ScalarValue::String("two".into())),
        entity(3, "Entity", "third")
            .with_property("kind", ScalarValue::String("b".into()))
            .with_property("score", ScalarValue::Integer(7))
            .with_property("name", ScalarValue::String("three".into())),
    ]);
    let engine = CypherEngine::new(storage);

    let distinct = engine
        .execute(
            "MATCH (e:Entity) RETURN DISTINCT e.kind AS kind ORDER BY kind",
            &ParameterMap::new(),
        )
        .await
        .expect("execute DISTINCT projection");
    assert_eq!(
        distinct.rows,
        vec![
            QueryRow {
                values: vec![QueryValue::Scalar(ScalarValue::String("a".into()))],
            },
            QueryRow {
                values: vec![QueryValue::Scalar(ScalarValue::String("b".into()))],
            },
        ]
    );

    let grouped = engine
        .execute(
            "MATCH (e:Entity) RETURN e.kind AS kind, count(*) AS total, \
             sum(e.score) AS score, avg(e.score) AS average, collect(e.name) AS names \
             ORDER BY total DESC, kind ASC SKIP 0 LIMIT 1",
            &ParameterMap::new(),
        )
        .await
        .expect("execute grouped aggregates");
    assert_eq!(
        grouped.rows,
        vec![QueryRow {
            values: vec![
                QueryValue::Scalar(ScalarValue::String("a".into())),
                QueryValue::Count(2),
                QueryValue::Scalar(ScalarValue::Integer(30)),
                QueryValue::Scalar(ScalarValue::Float("15".into())),
                QueryValue::List(vec![
                    QueryValue::Scalar(ScalarValue::String("one".into())),
                    QueryValue::Scalar(ScalarValue::String("two".into())),
                ]),
            ],
        }]
    );
}

#[tokio::test]
async fn bounded_and_exact_variable_length_paths_execute_natively() {
    let storage = MemoryStorage::with_graph(
        [
            entity(1, "Entity", "source"),
            entity(2, "Entity", "middle"),
            entity(3, "Entity", "target"),
        ],
        [
            RelationshipRecord::new(80, 1, 2, "LINK"),
            RelationshipRecord::new(81, 2, 3, "LINK"),
            RelationshipRecord::new(82, 2, 1, "LINK"),
        ],
    );
    let engine = CypherEngine::new(storage);
    let parameters = ParameterMap::from([("source".to_string(), ScalarValue::Integer(1))]);

    let bounded = engine
        .execute(
            "MATCH (a:Entity {id: $source})-[:LINK*1..2]->(b:Entity) \
             RETURN b.id AS id ORDER BY id",
            &parameters,
        )
        .await
        .expect("execute bounded variable-length traversal");
    assert_eq!(
        bounded.rows,
        vec![
            QueryRow {
                values: vec![QueryValue::VertexId(1)],
            },
            QueryRow {
                values: vec![QueryValue::VertexId(2)],
            },
            QueryRow {
                values: vec![QueryValue::VertexId(3)],
            },
        ]
    );

    let exact = engine
        .execute(
            "MATCH (a:Entity {id: $source})-[:LINK*2..2]->(b:Entity) \
             WHERE b.id = 3 RETURN b.id AS id",
            &parameters,
        )
        .await
        .expect("execute exact-length traversal");
    assert_eq!(
        exact.rows,
        vec![QueryRow {
            values: vec![QueryValue::VertexId(3)],
        }]
    );
}

#[tokio::test]
async fn with_narrows_the_scope_to_what_it_projects() {
    let engine = CypherEngine::new(MemoryStorage::with_graph(
        [entity(1, "Entity", "source"), entity(2, "Person", "target")],
        [RelationshipRecord::new(90, 1, 2, "KNOWS")],
    ));
    let parameters = ParameterMap::from([("source".to_string(), ScalarValue::Integer(1))]);
    let result = engine
        .execute(
            "MATCH (a:Entity {id: $source})-[r:KNOWS]->(b:Person) \
             WITH a, r, b RETURN a.id AS source, b.id AS target",
            &parameters,
        )
        .await
        .expect("execute a WITH that carries every binding");
    assert_eq!(result.rows.len(), 1);

    // Dropping bindings is what a scope boundary is for, so this now runs.
    // It used to be refused, because WITH had to be transparent for the plan
    // to be correct without one: the clause never reached lowering, so a WITH
    // that did not pass everything through would silently have widened the
    // scope rather than narrowing it.
    let narrowed = engine
        .execute(
            "MATCH (a:Entity {id: $source})-[r:KNOWS]->(b:Person) \
             WITH a RETURN a.id AS source",
            &parameters,
        )
        .await
        .expect("a WITH may drop bindings");
    assert_eq!(
        narrowed.rows,
        vec![QueryRow {
            values: vec![QueryValue::VertexId(1)],
        }]
    );

    // What it dropped is gone, and reading it says so by name rather than
    // returning a column of nulls.
    let error = engine
        .prepare(
            "MATCH (a:Entity {id: $source})-[r:KNOWS]->(b:Person) \
             WITH a RETURN b.id AS target",
            &parameters,
        )
        .expect_err("a dropped binding is out of scope");
    assert!(error.to_string().contains("out-of-scope binding b"), "unexpected refusal: {error}");

    // Mid-query, a dropped binding reaches the next MATCH as a fresh
    // variable. That is what a scope boundary means, so this now plans.
    engine
        .prepare(
            "MATCH (a:Entity {id: $source})-[r:KNOWS]->(b:Person) \
             WITH a MATCH (b:Person) RETURN b.id AS target",
            &parameters,
        )
        .expect("a WITH boundary lets the next MATCH rebind a dropped name");
}

/// A `get_chunk_ids_by_sub_tenant`-shaped query: a trailing `WITH c LIMIT $n`
/// that also drops `s`. It used to fail with "expected exactly one typed query
/// statement".
#[tokio::test]
async fn trailing_with_limit_bounds_the_return() {
    let engine = CypherEngine::new(MemoryStorage::with_graph(
        [
            entity(1, "Source", "s1"),
            entity(2, "Chunk", "c1"),
            entity(3, "Chunk", "c2"),
            entity(4, "Chunk", "c3"),
        ],
        [
            RelationshipRecord::new(90, 2, 1, "HAS_CHUNK"),
            RelationshipRecord::new(91, 3, 1, "HAS_CHUNK"),
            RelationshipRecord::new(92, 4, 1, "HAS_CHUNK"),
        ],
    ));
    let query = "MATCH (s:Source {id: $source})-[:HAS_CHUNK]-(c:Chunk) \
                 WITH c LIMIT $fetch_limit RETURN c.id AS chunk_id";
    for (limit, expected) in [(2_i128, 2_usize), (10, 3)] {
        let parameters = ParameterMap::from([
            ("source".to_string(), ScalarValue::Integer(1)),
            ("fetch_limit".to_string(), ScalarValue::Integer(limit)),
        ]);
        let result = engine.execute(query, &parameters).await.expect("execute trailing WITH ... LIMIT");
        assert_eq!(result.rows.len(), expected, "LIMIT {limit}");
    }

    let parameters = ParameterMap::from([
        ("source".to_string(), ScalarValue::Integer(1)),
        ("fetch_limit".to_string(), ScalarValue::Integer(2)),
    ]);
    // These four used to be refused, and the reason was the parser's: it folded
    // a trailing `WITH ... LIMIT` into the RETURN window, which is only sound
    // when RETURN is a plain per-row projection. Each of these reads the whole
    // bounded row set, so the fold would have answered a different question.
    //
    // A real scope boundary does not fold. The window rides on the projection
    // and whatever RETURN does runs after it, which is what Cypher says these
    // mean, so they plan now.
    for now_planned in [
        "MATCH (s:Source {id: $source})-[:HAS_CHUNK]-(c:Chunk) \
         WITH c LIMIT $fetch_limit RETURN c.id AS id ORDER BY id",
        "MATCH (s:Source {id: $source})-[:HAS_CHUNK]-(c:Chunk) \
         WITH c LIMIT $fetch_limit RETURN DISTINCT c.id AS id",
        "MATCH (s:Source {id: $source})-[:HAS_CHUNK]-(c:Chunk) \
         WITH c LIMIT $fetch_limit RETURN count(*) AS n",
        "MATCH (s:Source {id: $source})-[:HAS_CHUNK]-(c:Chunk) \
         WITH s, c LIMIT $fetch_limit MATCH (c)-[:HAS_CHUNK]-(s) RETURN c.id AS id",
    ] {
        engine
            .prepare(now_planned, &parameters)
            .unwrap_or_else(|error| panic!("{now_planned} should plan over a scope boundary: {error}"));
    }

    // The window still bounds what RETURN sees: two of the three chunks, then
    // sorted. Folding would have sorted all three and taken two.
    let ordered = engine
        .execute(
            "MATCH (s:Source {id: $source})-[:HAS_CHUNK]-(c:Chunk) \
             WITH c LIMIT $fetch_limit RETURN c.id AS id ORDER BY id",
            &parameters,
        )
        .await
        .expect("a bounded WITH feeding an ordered RETURN");
    assert_eq!(ordered.rows.len(), 2, "the WITH window still bounds the input");

    let counted = engine
        .execute(
            "MATCH (s:Source {id: $source})-[:HAS_CHUNK]-(c:Chunk) \
             WITH c LIMIT $fetch_limit RETURN count(*) AS n",
            &parameters,
        )
        .await
        .expect("a bounded WITH feeding an aggregate");
    assert_eq!(counted.rows.len(), 1);
    assert_eq!(
        format!("{:?}", counted.rows[0].values),
        "[Count(2)]",
        "count sees the bounded rows, not all three"
    );
}

#[tokio::test]
async fn union_and_union_all_compose_compatible_result_arms() {
    let engine = CypherEngine::new(MemoryStorage::with_vertices([
        entity(1, "Entity", "first"),
        entity(2, "Entity", "second"),
    ]));
    let parameters = ParameterMap::from([
        ("first".to_string(), ScalarValue::Integer(1)),
        ("second".to_string(), ScalarValue::Integer(2)),
    ]);
    let union = engine
        .execute(
            "MATCH (n:Entity {id: $first}) RETURN n.id AS id \
             UNION MATCH (n:Entity {id: $first}) RETURN n.id AS id \
             UNION MATCH (n:Entity {id: $second}) RETURN n.id AS id",
            &parameters,
        )
        .await
        .expect("execute UNION");
    assert_eq!(
        union.rows,
        vec![
            QueryRow {
                values: vec![QueryValue::VertexId(1)],
            },
            QueryRow {
                values: vec![QueryValue::VertexId(2)],
            },
        ]
    );

    let union_all = engine
        .execute(
            "MATCH (n:Entity {id: $first}) RETURN n.id AS id \
             UNION ALL MATCH (n:Entity {id: $first}) RETURN n.id AS id",
            &parameters,
        )
        .await
        .expect("execute UNION ALL");
    assert_eq!(
        union_all.rows,
        vec![
            QueryRow {
                values: vec![QueryValue::VertexId(1)],
            },
            QueryRow {
                values: vec![QueryValue::VertexId(1)],
            },
        ]
    );
}
// --- IN predicate -----------------------------------------------------------

/// The whole point of `IN`: it must reach the same multi-seek an OR chain
/// reaches, not degrade into a label scan. `seek_values` records one entry per
/// storage seek, so a scan would show up as zero.
#[tokio::test]
async fn in_list_parameter_uses_the_property_multi_seek() {
    let storage = MemoryStorage::with_vertices([entity(1, "Entity", "a"), entity(2, "Entity", "b"), entity(3, "Entity", "c")]);
    let engine = CypherEngine::new(storage.clone());
    let lists = ListParameterMap::from([(
        "ids".to_string(),
        vec![ScalarValue::String("a".into()), ScalarValue::String("c".into())],
    )]);
    let prepared = engine
        .prepare_with_lists(
            "MATCH (e:Entity) WHERE e.entity_id IN $ids RETURN e.entity_id AS id ORDER BY id",
            &ParameterMap::new(),
            &lists,
        )
        .expect("prepare IN over a list parameter");
    assert!(
        prepared.explain().contains("access=property-points"),
        "IN must plan as a multi-seek, got:\n{}",
        prepared.explain()
    );
    let result = engine.execute_prepared(&prepared).await.expect("execute IN over a list parameter");
    assert_eq!(
        result.rows,
        vec![
            QueryRow {
                values: vec![QueryValue::Scalar(ScalarValue::String("a".into()))]
            },
            QueryRow {
                values: vec![QueryValue::Scalar(ScalarValue::String("c".into()))]
            },
        ]
    );
    assert_eq!(storage.seek_values.lock().unwrap().len(), 2);
}

/// An inline list needs no parameter plumbing at all.
#[tokio::test]
async fn in_inline_list_literal_matches_every_listed_value() {
    let storage = MemoryStorage::with_vertices([entity(1, "Entity", "a"), entity(2, "Entity", "b"), entity(3, "Entity", "c")]);
    let engine = CypherEngine::new(storage);
    let result = engine
        .execute(
            "MATCH (e:Entity) WHERE e.entity_id IN ['a', 'b'] RETURN e.entity_id AS id ORDER BY id",
            &ParameterMap::new(),
        )
        .await
        .expect("execute IN over an inline list");
    assert_eq!(
        result.rows,
        vec![
            QueryRow {
                values: vec![QueryValue::Scalar(ScalarValue::String("a".into()))]
            },
            QueryRow {
                values: vec![QueryValue::Scalar(ScalarValue::String("b".into()))]
            },
        ]
    );
}

/// `x IN []` is false, not an error. Callers page id sets down to empty, and
/// erroring there would be a correctness trap rather than a helpful message.
#[tokio::test]
async fn in_empty_list_returns_no_rows_without_erroring() {
    let storage = MemoryStorage::with_vertices([entity(1, "Entity", "a")]);
    let engine = CypherEngine::new(storage.clone());
    let lists = ListParameterMap::from([("ids".to_string(), Vec::new())]);
    let prepared = engine
        .prepare_with_lists(
            "MATCH (e:Entity) WHERE e.entity_id IN $ids RETURN e.entity_id AS id",
            &ParameterMap::new(),
            &lists,
        )
        .expect("an empty IN list must still prepare");
    let result = engine.execute_prepared(&prepared).await.expect("execute IN over an empty list");
    assert!(result.rows.is_empty());
}

/// A scalar bound where a list was expected is a one-element membership test,
/// so a caller that pages an id set down to a single value keeps working.
#[tokio::test]
async fn in_accepts_a_scalar_parameter_as_a_one_element_test() {
    let storage = MemoryStorage::with_vertices([entity(1, "Entity", "a"), entity(2, "Entity", "b")]);
    let engine = CypherEngine::new(storage);
    let parameters = ParameterMap::from([("ids".to_string(), ScalarValue::String("b".into()))]);
    let result = engine
        .execute("MATCH (e:Entity) WHERE e.entity_id IN $ids RETURN e.entity_id AS id", &parameters)
        .await
        .expect("execute IN over a scalar parameter");
    assert_eq!(
        result.rows,
        vec![QueryRow {
            values: vec![QueryValue::Scalar(ScalarValue::String("b".into()))]
        }]
    );
}

/// Cypher membership is three-valued. A miss against a list holding null is
/// unknown, not false, so `NOT (... IN ...)` must not resurrect the row.
#[tokio::test]
async fn in_with_a_null_candidate_is_unknown_rather_than_false() {
    let storage = MemoryStorage::with_vertices([entity(1, "Entity", "a"), entity(2, "Entity", "z")]);
    let engine = CypherEngine::new(storage);
    let lists = ListParameterMap::from([("ids".to_string(), vec![ScalarValue::String("a".into()), ScalarValue::Null])]);
    let prepared = engine
        .prepare_with_lists(
            "MATCH (e:Entity) WHERE NOT (e.entity_id IN $ids) RETURN e.entity_id AS id",
            &ParameterMap::new(),
            &lists,
        )
        .expect("prepare a negated IN");
    let result = engine.execute_prepared(&prepared).await.expect("execute a negated IN");
    assert!(
        result.rows.is_empty(),
        "a miss against a null-bearing list is unknown, so NOT must not admit the row: {:?}",
        result.rows
    );
}

/// Numeric equality follows the same rule as the rest of the engine: integer 1
/// and float 1.0 are the same value, so the list must not compare by encoding.
#[tokio::test]
async fn in_uses_cypher_numeric_equality_for_candidates() {
    let storage = MemoryStorage::with_vertices([VertexRecord::new(1)
        .with_label("Entity")
        .with_property("rank", ScalarValue::Float("1.0".into()))]);
    let engine = CypherEngine::new(storage);
    let lists = ListParameterMap::from([("ranks".to_string(), vec![ScalarValue::Integer(1)])]);
    let prepared = engine
        .prepare_with_lists(
            "MATCH (e:Entity) WHERE e.rank IN $ranks RETURN e.rank AS rank",
            &ParameterMap::new(),
            &lists,
        )
        .expect("prepare a numeric IN");
    let result = engine.execute_prepared(&prepared).await.expect("execute a numeric IN");
    assert_eq!(result.rows.len(), 1);
}

// --- undirected patterns ----------------------------------------------------

/// The point of an undirected hop: the same query must find the edge whichever
/// way round it was written. Here the only edge points at the anchor, so a
/// directed outgoing expansion would return nothing.
#[tokio::test]
async fn undirected_hop_finds_a_backwards_written_edge() {
    let storage = MemoryStorage::with_graph(
        [entity(1, "Entity", "a"), entity(2, "Entity", "b")],
        [RelationshipRecord::new(10, 2, 1, "KNOWS")],
    );
    let engine = CypherEngine::new(storage);
    let result = engine
        .execute(
            "MATCH (a:Entity {entity_id: 'a'})-[:KNOWS]-(b:Entity) RETURN b.entity_id AS id",
            &ParameterMap::new(),
        )
        .await
        .expect("execute an undirected hop");
    assert_eq!(
        result.rows,
        vec![QueryRow {
            values: vec![QueryValue::Scalar(ScalarValue::String("b".into()))]
        }]
    );
}

/// A self-loop satisfies both orientations of an undirected hop, and both
/// storage expansions return it. It is one relationship, so it must be
/// reported once.
#[tokio::test]
async fn undirected_hop_reports_a_self_loop_once() {
    let storage = MemoryStorage::with_graph([entity(1, "Entity", "a")], [RelationshipRecord::new(10, 1, 1, "KNOWS")]);
    let engine = CypherEngine::new(storage);
    let result = engine
        .execute(
            "MATCH (a:Entity {entity_id: 'a'})-[:KNOWS]-(b:Entity) RETURN b.entity_id AS id",
            &ParameterMap::new(),
        )
        .await
        .expect("execute an undirected self-loop");
    assert_eq!(result.rows.len(), 1, "a self-loop is one row: {:?}", result.rows);
}

/// A reciprocal pair is two distinct relationships between the same nodes, so
/// an undirected hop must report both. This is the case a naive
/// dedupe-by-endpoints would wrongly collapse. Each carries the client `id`
/// property HydraDB's write path stores, which is what `r.id` reads.
#[tokio::test]
async fn undirected_hop_reports_both_relationships_of_a_reciprocal_pair() {
    let storage = MemoryStorage::with_graph(
        [entity(1, "Entity", "a"), entity(2, "Entity", "b")],
        [
            RelationshipRecord::new(10, 1, 2, "KNOWS").with_property("id", ScalarValue::Integer(10)),
            RelationshipRecord::new(11, 2, 1, "KNOWS").with_property("id", ScalarValue::Integer(11)),
        ],
    );
    let engine = CypherEngine::new(storage);
    let result = engine
        .execute(
            "MATCH (a:Entity {entity_id: 'a'})-[r:KNOWS]-(b:Entity) RETURN r.id AS rel ORDER BY rel",
            &ParameterMap::new(),
        )
        .await
        .expect("execute an undirected reciprocal pair");
    assert_eq!(
        result.rows,
        vec![
            QueryRow {
                values: vec![QueryValue::Scalar(ScalarValue::Integer(10))]
            },
            QueryRow {
                values: vec![QueryValue::Scalar(ScalarValue::Integer(11))]
            },
        ]
    );
}

/// The fan-out happens inside pattern matching, so ORDER BY and LIMIT apply
/// once across the merged rows. Were it two union arms, each arm would sort
/// and limit on its own and the combined result would be unsorted.
#[tokio::test]
async fn undirected_ordering_and_limit_apply_once_across_both_orientations() {
    let storage = MemoryStorage::with_graph(
        [entity(1, "Entity", "anchor"), entity(2, "Entity", "b"), entity(3, "Entity", "a")],
        [
            // Reached by the outgoing orientation, sorts last.
            RelationshipRecord::new(10, 1, 2, "KNOWS"),
            // Reached by the incoming orientation, sorts first.
            RelationshipRecord::new(11, 3, 1, "KNOWS"),
        ],
    );
    let engine = CypherEngine::new(storage);
    let result = engine
        .execute(
            "MATCH (a:Entity {entity_id: 'anchor'})-[:KNOWS]-(b:Entity) \
             RETURN b.entity_id AS id ORDER BY id LIMIT 1",
            &ParameterMap::new(),
        )
        .await
        .expect("execute an ordered undirected hop");
    assert_eq!(
        result.rows,
        vec![QueryRow {
            values: vec![QueryValue::Scalar(ScalarValue::String("a".into()))]
        }],
        "the limit must pick the globally first row, not the first arm's"
    );
}

/// EXPLAIN has to describe what executes, so the plan names the undirected
/// direction rather than silently showing one orientation.
#[tokio::test]
async fn undirected_expansion_is_named_in_the_plan() {
    let storage = MemoryStorage::with_graph([entity(1, "Entity", "a")], [RelationshipRecord::new(10, 1, 1, "KNOWS")]);
    let engine = CypherEngine::new(storage);
    let prepared = engine
        .prepare(
            "MATCH (a:Entity {entity_id: 'a'})-[:KNOWS]-(b:Entity) RETURN b.entity_id AS id",
            &ParameterMap::new(),
        )
        .expect("prepare an undirected hop");
    assert!(
        prepared.explain().contains("undirected"),
        "EXPLAIN must name the undirected direction, got:\n{}",
        prepared.explain()
    );
}

/// Undirected variable-length matching walks both orientations inside one
/// traversal. A self-loop, which both orientations return, is one step: Neo4j
/// answers this graph with one row, and a second hop has no unused
/// relationship to take.
#[tokio::test]
async fn variable_length_undirected_traversal_walks_a_self_loop_once() {
    let storage = MemoryStorage::with_graph([entity(1, "Entity", "a")], [RelationshipRecord::new(10, 1, 1, "KNOWS")]);
    let engine = CypherEngine::new(storage);
    let result = engine
        .execute(
            "MATCH (a:Entity {entity_id: 'a'})-[:KNOWS*1..2]-(b:Entity) RETURN b.entity_id AS id",
            &ParameterMap::new(),
        )
        .await
        .expect("undirected variable-length traversal executes");
    assert_eq!(
        result.rows,
        vec![QueryRow {
            values: vec![QueryValue::Scalar(ScalarValue::String("a".into()))],
        }]
    );
}

#[tokio::test]
async fn logical_preparation_preserves_lists_while_waiting_for_statistics() {
    let engine = CypherEngine::new(MemoryStorage::with_vertices([
        entity(1, "Entity", "a"),
        entity(2, "Entity", "b"),
        entity(3, "Entity", "c"),
    ]));
    let parameters = ParameterMap::from([("limit".to_owned(), ScalarValue::Integer(1))]);
    let lists = ListParameterMap::from([(
        "keys".to_owned(),
        vec![ScalarValue::String("b".into()), ScalarValue::String("a".into())],
    )]);
    let query = "MATCH (e:Entity) WHERE e.entity_id IN $keys RETURN e.entity_id AS id ORDER BY id LIMIT $limit";
    let logical = engine.lower_with_lists(query, &parameters, &lists).unwrap();
    let prepared = logical.plan_with_statistics(&FixedStatistics { vertex_count: 3 }).unwrap();
    assert!(prepared.explain().contains("estimated_input_rows=3"));
    assert!(prepared.explain().contains("access=property-points"));
    let expected = engine
        .execute_prepared(&engine.prepare_with_lists(query, &parameters, &lists).unwrap())
        .await
        .unwrap();
    let actual = engine.execute_prepared(&prepared).await.unwrap();
    assert_eq!(actual, expected);
    assert_eq!(actual.rows.len(), 1);
    assert_eq!(actual.rows[0].values, vec![QueryValue::Scalar(ScalarValue::String("a".into()))]);
}
