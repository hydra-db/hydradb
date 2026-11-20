mod common;

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use common::MemoryStorage;
use hydradb_cypher_engine::{
    plan_physical, CypherEngine, EngineResult, ExpandDirection, ListParameterMap, ParameterMap, PreparedQuery, QueryResult, QueryRow,
    QueryValue, RelationshipRecord, ScalarValue, StorageError, VertexRecord,
};

const SEEDS: &str = "MATCH (e:Entity {tenant_id: $tenant, sub_tenant_id: $sub}) WHERE e.entity_id IN $ids";
const PRESENT: &str = "MATCH (e)-[p:PRESENT_IN]->(c:Chunk {tenant_id: $tenant, sub_tenant_id: $sub})";
const SOURCES: &str = "MATCH (c)-[h:HAS_CHUNK]-(s:Source {tenant_id: $tenant, sub_tenant_id: $sub})";
const RAW: &str = "RETURN e.n AS e, c.n AS c, s.n AS s, p.edge AS p, h.edge AS h";
const GLOBAL: &str = "RETURN DISTINCT e.entity_id AS entity_id, s.source_id AS source_id ORDER BY entity_id, source_id LIMIT $cap";

fn node(id: u64, label: &str) -> VertexRecord {
    let key = match label {
        "Entity" => "entity_id",
        "Chunk" => "chunk_id",
        _ => "source_id",
    };
    VertexRecord::new(id)
        .with_label(label)
        .with_property(key, ScalarValue::String(format!("{id:04}").into()))
        .with_property("n", ScalarValue::Integer(i128::from(id)))
        .with_property("tenant_id", ScalarValue::String("tenant-a".into()))
        .with_property("sub_tenant_id", ScalarValue::String("sub-a".into()))
}

fn edge(id: u64, source: u64, target: u64, kind: &str) -> RelationshipRecord {
    RelationshipRecord::new(id, source, target, kind).with_property("edge", ScalarValue::Integer(i128::from(id)))
}

fn fixture() -> MemoryStorage {
    MemoryStorage::with_graph(
        [
            node(1, "Entity"),
            node(2, "Entity"),
            node(3, "Entity"),
            node(4, "Entity").with_property("tenant_id", ScalarValue::String("tenant-b".into())),
            node(5, "Entity").with_property("sub_tenant_id", ScalarValue::String("sub-b".into())),
            node(10, "Chunk"),
            node(11, "Chunk"),
            node(12, "Chunk").with_property("tenant_id", ScalarValue::String("tenant-b".into())),
            node(13, "NotChunk"),
            node(14, "Chunk").with_property("sub_tenant_id", ScalarValue::String("sub-b".into())),
            node(20, "Source"),
            node(21, "Source"),
            node(22, "Source").with_property("tenant_id", ScalarValue::String("tenant-b".into())),
            node(23, "NotSource"),
            node(24, "Source").with_property("sub_tenant_id", ScalarValue::String("sub-b".into())),
        ],
        [
            edge(100, 1, 10, "PRESENT_IN"),
            edge(101, 1, 10, "PRESENT_IN"),
            edge(102, 2, 10, "PRESENT_IN"),
            edge(103, 1, 11, "PRESENT_IN"),
            edge(104, 1, 12, "PRESENT_IN"),
            edge(105, 1, 13, "PRESENT_IN"),
            edge(106, 1, 14, "PRESENT_IN"),
            edge(107, 4, 10, "PRESENT_IN"),
            edge(108, 5, 10, "PRESENT_IN"),
            edge(200, 10, 20, "HAS_CHUNK"),
            edge(201, 21, 10, "HAS_CHUNK"),
            edge(202, 11, 20, "HAS_CHUNK"),
            edge(203, 10, 22, "HAS_CHUNK"),
            edge(204, 10, 23, "HAS_CHUNK"),
            edge(205, 10, 24, "HAS_CHUNK"),
        ],
    )
}

#[derive(Default)]
struct Parameters {
    scalars: ParameterMap,
    lists: ListParameterMap,
}

fn parameters(ids: &[u64], cap: i128) -> Parameters {
    Parameters {
        scalars: ParameterMap::from([
            ("tenant".into(), ScalarValue::String("tenant-a".into())),
            ("sub".into(), ScalarValue::String("sub-a".into())),
            ("cap".into(), ScalarValue::Integer(cap)),
        ]),
        lists: ListParameterMap::from([(
            "ids".into(),
            ids.iter().map(|id| ScalarValue::String(format!("{id:04}").into())).collect(),
        )]),
    }
}

fn query(barriers: bool, output: &str) -> String {
    if barriers {
        format!("{SEEDS} WITH e {PRESENT} WITH e,c,p {SOURCES} {output}")
    } else {
        format!("{SEEDS} {PRESENT} {SOURCES} {output}")
    }
}

async fn execute(storage: &MemoryStorage, source: &str, params: &Parameters) -> EngineResult<QueryResult> {
    let engine = CypherEngine::new(storage.clone());
    let prepared = engine.prepare_with_lists(source, &params.scalars, &params.lists)?;
    engine.execute_prepared(&prepared).await
}

// WITH is no longer a decorrelation barrier. Plan directly when a test needs
// the original correlated reference rather than the rewritten pipeline.
fn prepare_unrewritten(engine: &CypherEngine<MemoryStorage>, source: &str, params: &Parameters) -> EngineResult<PreparedQuery> {
    let lowered = engine.lower_with_lists(source, &params.scalars, &params.lists)?;
    Ok(PreparedQuery {
        ast: lowered.ast.clone(),
        logical: lowered.logical.clone(),
        physical: plan_physical(&lowered.logical, lowered.planning_context())?,
    })
}

async fn run_unrewritten(storage: &MemoryStorage, source: &str, params: &Parameters) -> Vec<QueryRow> {
    let engine = CypherEngine::new(storage.clone());
    let prepared = prepare_unrewritten(&engine, source, params).unwrap();
    engine.execute_prepared(&prepared).await.unwrap().rows
}

async fn run(storage: &MemoryStorage, source: &str, params: &Parameters) -> Vec<QueryRow> {
    execute(storage, source, params)
        .await
        .unwrap_or_else(|error| panic!("{source}: {error}"))
        .rows
}

fn integers(rows: &[QueryRow]) -> Vec<Vec<i128>> {
    let mut rows = rows
        .iter()
        .map(|row| {
            row.values
                .iter()
                .map(|value| match value {
                    QueryValue::Scalar(ScalarValue::Integer(value)) => *value,
                    other => panic!("expected integer, got {other:?}"),
                })
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    rows.sort();
    rows
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Work {
    calls: usize,
    input_ids: usize,
    matching_relationships: usize,
    relationship_records: usize,
    hydration_calls: usize,
    hydration_ids: usize,
}

fn work(storage: &MemoryStorage) -> Work {
    let requests = storage.expand_requests.lock().unwrap();
    let hydration = storage.hydration_requests.lock().unwrap();
    Work {
        calls: requests.len(),
        input_ids: requests.iter().map(|request| request.input_vertex_ids.len()).sum(),
        matching_relationships: requests
            .iter()
            .map(|request| {
                storage
                    .relationships
                    .iter()
                    .filter(|relationship| {
                        (request.relationship_types.is_empty() || request.relationship_types.contains(&relationship.relationship_type))
                            && request.input_vertex_ids.contains(&match request.direction {
                                ExpandDirection::Outgoing => relationship.source,
                                ExpandDirection::Incoming => relationship.target,
                            })
                    })
                    .count()
            })
            .sum(),
        relationship_records: *storage.expanded_relationship_records.lock().unwrap(),
        hydration_calls: hydration.len(),
        hydration_ids: hydration.iter().map(Vec::len).sum(),
    }
}

#[tokio::test]
async fn connected_matches_preserve_raw_bags_shared_parents_and_scope() {
    let params = parameters(&[1, 2, 3, 4, 5], 100);
    let storage = fixture();
    let rows = run(&storage, &query(false, RAW), &params).await;
    assert_eq!(
        integers(&rows),
        vec![
            vec![1, 10, 20, 100, 200],
            vec![1, 10, 20, 101, 200],
            vec![1, 10, 21, 100, 201],
            vec![1, 10, 21, 101, 201],
            vec![1, 11, 20, 103, 202],
            vec![2, 10, 20, 102, 200],
            vec![2, 10, 21, 102, 201],
        ]
    );
    let baseline = fixture();
    assert_eq!(
        integers(&rows),
        integers(&run_unrewritten(&baseline, &query(true, RAW), &params).await)
    );
    let batched = work(&storage);
    let scalar = work(&baseline);
    assert!(batched.input_ids < scalar.input_ids, "{batched:?} vs {scalar:?}");
    assert!(
        batched.matching_relationships < scalar.matching_relationships,
        "{batched:?} vs {scalar:?}"
    );
    assert!(
        batched.relationship_records < scalar.relationship_records,
        "{batched:?} vs {scalar:?}"
    );
    let requests = storage.expand_requests.lock().unwrap();
    assert_eq!(requests.len(), 3, "one source batch and both orientations of the next hop");
    assert_eq!(requests[0].input_vertex_ids, [1, 2, 3]);
    assert!(requests[1..].iter().all(|request| request.input_vertex_ids == [10, 11]));
}

#[tokio::test]
async fn distinct_order_and_cap_apply_globally_after_batching() {
    for cap in [0, 1, 3, 100] {
        let params = parameters(&[2, 1, 2, 4, 5], cap);
        let storage = fixture();
        let rows = run(&storage, &query(false, GLOBAL), &params).await;
        assert_eq!(rows, run_unrewritten(&fixture(), &query(true, GLOBAL), &params).await);
        let expected = [("0001", "0020"), ("0001", "0021"), ("0002", "0020"), ("0002", "0021")];
        let expected = expected
            .into_iter()
            .take(cap as usize)
            .map(|(e, s)| {
                vec![
                    QueryValue::Scalar(ScalarValue::String(e.into())),
                    QueryValue::Scalar(ScalarValue::String(s.into())),
                ]
            })
            .collect::<Vec<_>>();
        assert_eq!(rows.iter().map(|row| row.values.clone()).collect::<Vec<_>>(), expected);
        if cap > 0 {
            let baseline = fixture();
            run_unrewritten(&baseline, &query(true, GLOBAL), &params).await;
            assert!(work(&storage).matching_relationships < work(&baseline).matching_relationships);
        }
    }
}

#[tokio::test]
async fn empty_and_duplicate_membership_inputs_do_not_multiply_rows() {
    let source = query(false, RAW);
    let storage = fixture();
    assert!(run(&storage, &source, &parameters(&[], 100)).await.is_empty());
    assert!(storage.expand_requests.lock().unwrap().is_empty());
    let single = run(&fixture(), &source, &parameters(&[1], 100)).await;
    let duplicates = run(&fixture(), &source, &parameters(&[1, 1, 1], 100)).await;
    assert_eq!(integers(&single), integers(&duplicates));
    assert_eq!(single.len(), 5);
}

#[tokio::test]
async fn incoming_outgoing_undirected_and_self_loops_keep_relationship_identity() {
    let graph = || {
        MemoryStorage::with_graph(
            [node(1, "Entity"), node(2, "Entity")],
            [
                edge(10, 1, 1, "LINK"),
                edge(11, 1, 2, "LINK"),
                edge(12, 2, 1, "LINK"),
                edge(13, 1, 2, "LINK"),
            ],
        )
    };
    for (pattern, expected) in [
        (
            "(e)-[r:LINK]->(t)",
            vec![vec![1, 1, 10], vec![1, 2, 11], vec![1, 2, 13], vec![2, 1, 12]],
        ),
        (
            "(e)<-[r:LINK]-(t)",
            vec![vec![1, 1, 10], vec![1, 2, 12], vec![2, 1, 11], vec![2, 1, 13]],
        ),
        (
            "(e)-[r:LINK]-(t)",
            vec![
                vec![1, 1, 10],
                vec![1, 2, 11],
                vec![1, 2, 12],
                vec![1, 2, 13],
                vec![2, 1, 11],
                vec![2, 1, 12],
                vec![2, 1, 13],
            ],
        ),
    ] {
        let source = format!("{SEEDS} MATCH {pattern} RETURN e.n AS e, t.n AS t, r.edge AS r");
        let storage = graph();
        let params = parameters(&[1, 2], 100);
        let rows = run(&storage, &source, &params).await;
        assert_eq!(integers(&rows), expected, "{source}");
        let scalar = format!("{SEEDS} WITH e MATCH {pattern} RETURN e.n AS e, t.n AS t, r.edge AS r");
        assert_eq!(integers(&rows), integers(&run_unrewritten(&graph(), &scalar, &params).await));
        let requests = storage.expand_requests.lock().unwrap();
        assert_eq!(requests.len(), if pattern == "(e)-[r:LINK]-(t)" { 2 } else { 1 });
        assert!(requests.iter().all(|request| request.input_vertex_ids == [1, 2]));
    }
}

#[tokio::test]
async fn required_match_pipelines_decorrelate_through_filters_paths_and_input_operators() {
    for (source, expected_rows) in [
        (format!("MATCH (e:Entity) {PRESENT} RETURN e.n AS e, c.n AS c"), 6),
        (format!("{SEEDS} WITH e {PRESENT} RETURN e.n AS e, c.n AS c"), 4),
        (
            format!(
                "{SEEDS} MATCH (e)-[:PRESENT_IN*1..1]->(c:Chunk {{tenant_id: $tenant, sub_tenant_id: $sub}}) RETURN e.n AS e, c.n AS c"
            ),
            4,
        ),
        (
            format!("{SEEDS} WITH e ORDER BY e.n LIMIT 2 {PRESENT} RETURN e.n AS e, c.n AS c"),
            4,
        ),
        (format!("{SEEDS} {PRESENT} RETURN e.n AS e, c.n AS c LIMIT 2"), 2),
        (
            format!("{SEEDS} MATCH (e)-[:PRESENT_IN]->(c:Chunk {{n: 10 / e.n}}) RETURN e.n AS e, c.n AS c"),
            2,
        ),
        (format!("{SEEDS} AND 100 / e.n > 0 {PRESENT} RETURN e.n AS e, c.n AS c"), 4),
        (
            format!("{SEEDS} WITH DISTINCT e AS src MATCH (src)-[:PRESENT_IN]->(c:Chunk {{tenant_id: $tenant, sub_tenant_id: $sub}}) RETURN src.n,c.n"),
            4,
        ),
        (
            format!("{SEEDS} UNWIND [$tenant] AS wanted MATCH (e)-[:PRESENT_IN]->(c:Chunk {{tenant_id: wanted}}) RETURN e.n,c.n,wanted"),
            5,
        ),
        (
            format!("{SEEDS} MATCH path=(e)-[:PRESENT_IN]->(c:Chunk {{tenant_id: $tenant, sub_tenant_id: $sub}}) RETURN e.n,c.n,path"),
            4,
        ),
        (
            format!("{SEEDS} MATCH (e)-[:PRESENT_IN*0..2]->(c:Chunk {{tenant_id: $tenant, sub_tenant_id: $sub}}) RETURN e.n,c.n"),
            4,
        ),
        (
            format!("{SEEDS} MATCH (e)-[p:PRESENT_IN {{edge: 100}}]->(c:Chunk {{tenant_id: $tenant, sub_tenant_id: $sub}}) RETURN e.n,c.n,id(p)"),
            1,
        ),
        (
            format!("{SEEDS} MATCH (e)-[:PRESENT_IN]->(c:Chunk WHERE CASE WHEN c.tenant_id = $tenant THEN c.sub_tenant_id ELSE '' END = $sub) RETURN e.n,c.n"),
            4,
        ),
    ] {
        let storage = fixture();
        let params = parameters(&[1, 2, 3], 100);
        let engine = CypherEngine::new(storage.clone());
        let prepared = engine.prepare_with_lists(&source, &params.scalars, &params.lists).unwrap();
        assert!(!prepared.explain().contains("NaturalJoin"), "{source}: {}", prepared.explain());
        let result = engine.execute_prepared(&prepared).await.unwrap();
        let mut rows = result.rows;
        assert_eq!(rows.len(), expected_rows, "{source}: {rows:?}");
        let mut reference = run_unrewritten(&fixture(), &source, &params).await;
        rows.sort_by_cached_key(|row| format!("{row:?}"));
        reference.sort_by_cached_key(|row| format!("{row:?}"));
        assert_eq!(rows, reference, "{source}");
        let requests = storage.expand_requests.lock().unwrap();
        assert!(requests.iter().any(|request| request.input_vertex_ids.len() > 1), "{source}: {requests:?}");
    }
}

#[tokio::test]
async fn target_constraints_decorrelate_without_changing_result_bags() {
    let params = parameters(&[1, 2, 3], 100);
    for (pattern, count) in [
        ("(c:Chunk)", 6),
        ("(c {tenant_id: $tenant, sub_tenant_id: $sub})", 5),
        ("(c:Chunk {tenant_id: $tenant, sub_tenant_id: $sub})", 4),
        ("(c) WHERE c:Chunk AND c.tenant_id = $tenant AND c.sub_tenant_id = $sub", 4),
    ] {
        let storage = fixture();
        let engine = CypherEngine::new(storage.clone());
        let source = format!("{SEEDS} MATCH (e)-[:PRESENT_IN]->{pattern} RETURN e.n AS e,c.n AS c");
        let prepared = engine.prepare_with_lists(&source, &params.scalars, &params.lists).unwrap();
        assert!(!prepared.explain().contains("NaturalJoin"), "{}", prepared.explain());
        let rows = engine.execute_prepared(&prepared).await.unwrap().rows;
        assert_eq!(rows.len(), count, "{source}");
        assert_eq!(
            integers(&rows),
            integers(&run_unrewritten(&fixture(), &source, &params).await),
            "{source}"
        );
        let requests = storage.expand_requests.lock().unwrap();
        assert_eq!(requests.len(), 1, "{source}");
        assert_eq!(requests[0].input_vertex_ids, [1, 2, 3], "{source}");
    }
}

#[tokio::test]
async fn independent_joins_optional_matches_and_subqueries_keep_their_plans() {
    let params = parameters(&[1, 2, 3], 100);
    for (source, count) in [
        (format!("{SEEDS} MATCH (c:Chunk) RETURN e.n,c.n"), 12),
        (
            format!("{SEEDS} OPTIONAL MATCH (e)-[:PRESENT_IN]->(c:Chunk {{tenant_id: $tenant, sub_tenant_id: $sub}}) RETURN e.n,c.n"),
            5,
        ),
        (
            format!(
                "{SEEDS} RETURN e.n,VALUE {{ MATCH (e)-[:PRESENT_IN]->(c:Chunk) MATCH (c)-[:HAS_CHUNK]->(s) RETURN count(s) AS n }} AS n"
            ),
            3,
        ),
        (
            format!("{SEEDS} CALL (e) {{ MATCH (e)-[:PRESENT_IN]->(c:Chunk) RETURN count(c) AS n }} RETURN e.n,n"),
            3,
        ),
    ] {
        let engine = CypherEngine::new(fixture());
        let original = prepare_unrewritten(&engine, &source, &params).unwrap();
        let prepared = engine.prepare_with_lists(&source, &params.scalars, &params.lists).unwrap();
        assert_eq!(prepared.physical, original.physical, "{source}");
        assert_eq!(engine.execute_prepared(&prepared).await.unwrap().rows.len(), count, "{source}");
    }
}

#[tokio::test]
async fn source_labels_and_scope_are_rechecked_on_bound_scan_roots() {
    for constraint in ["e:Chunk", "e {tenant_id: 'tenant-b'}", "e {sub_tenant_id: 'sub-b'}"] {
        let source = format!("{SEEDS} MATCH ({constraint})-[:PRESENT_IN]->(c) RETURN e.n AS e, c.n AS c");
        assert!(run(&fixture(), &source, &parameters(&[1, 2], 100)).await.is_empty(), "{source}");
    }
}

#[tokio::test]
async fn scalar_subquery_keeps_its_correlated_execution() {
    let storage = fixture();
    let source = format!("RETURN VALUE {{ {SEEDS} {PRESENT} RETURN count(c) AS count }} AS count");
    let rows = run(&storage, &source, &parameters(&[1, 2, 3], 100)).await;
    assert_eq!(
        rows,
        vec![QueryRow {
            values: vec![QueryValue::Scalar(ScalarValue::Integer(4))]
        }]
    );
    let requests = storage.expand_requests.lock().unwrap();
    assert!(!requests.is_empty());
    assert!(requests.iter().all(|request| request.input_vertex_ids.len() == 1));
}

#[derive(Debug, thiserror::Error)]
#[error("cancelled during connected expansion")]
struct Cancelled;

#[tokio::test]
async fn connected_expansion_propagates_cancellation_and_row_budget() {
    for target in ["cypher_expand_inputs", "cypher_expand_relationships", "cypher_expand_output_rows"] {
        let observed = Arc::new(Mutex::new(0));
        let seen = Arc::clone(&observed);
        let storage = MemoryStorage {
            checkpoint: Some(Arc::new(move |operation| {
                if operation == target {
                    let mut count = seen.lock().unwrap();
                    *count += 1;
                    if *count == 2 {
                        return Err(StorageError::from_backend(Cancelled));
                    }
                }
                Ok(())
            })),
            ..fixture()
        };
        let error = execute(&storage, &query(false, RAW), &parameters(&[1, 2, 3], 100))
            .await
            .unwrap_err();
        assert!(error.into_backend_error::<Cancelled>().is_ok(), "{target}");
        assert_eq!(*observed.lock().unwrap(), 2, "{target}");
    }
    let storage = MemoryStorage {
        max_intermediate_rows: Some(6),
        ..fixture()
    };
    let error = execute(&storage, &query(false, RAW), &parameters(&[1, 2], 100)).await.unwrap_err();
    assert!(error.to_string().contains("intermediate row limit"), "{error}");
}

#[tokio::test]
async fn fallible_filters_keep_errors_and_empty_left_laziness() {
    let source = format!("{SEEDS} MATCH (e)-[:PRESENT_IN]->(c) WHERE 1 / (c.n - 10) > 0 RETURN c.n AS c");
    let storage = fixture();
    let error = execute(&storage, &source, &parameters(&[1, 2], 100)).await.unwrap_err();
    assert_eq!(error.gql_status(), hydradb_cypher_ast::GqlStatus::DIVISION_BY_ZERO);
    assert!(execute(&storage, &source, &parameters(&[], 100)).await.unwrap().rows.is_empty());
    let scalar = source.replace("MATCH (e)-[:PRESENT_IN]", "WITH e MATCH (e)-[:PRESENT_IN]");
    let error = execute(&fixture(), &scalar, &parameters(&[1, 2], 100)).await.unwrap_err();
    assert_eq!(error.gql_status(), hydradb_cypher_ast::GqlStatus::DIVISION_BY_ZERO);
}

fn benchmark_fixture() -> MemoryStorage {
    let mut vertices = (1..=668).map(|id| node(id, "Entity")).collect::<Vec<_>>();
    let mut relationships = Vec::new();
    for id in 1..=668 {
        for offset in [0, 1] {
            relationships.push(edge(10_000 + id * 2 + offset, id, 1_000 + (id + offset) % 16, "PRESENT_IN"));
        }
    }
    for chunk in 0..16 {
        vertices.push(node(1_000 + chunk, "Chunk"));
        for offset in 0..3 {
            let source = 2_000 + chunk * 3 + offset;
            vertices.push(node(source, "Source"));
            let (from, to) = if offset == 2 {
                (source, 1_000 + chunk)
            } else {
                (1_000 + chunk, source)
            };
            relationships.push(edge(20_000 + chunk * 3 + offset, from, to, "HAS_CHUNK"));
        }
    }
    MemoryStorage::with_graph(vertices, relationships)
}

#[tokio::test]
#[ignore = "in-process in-memory execution benchmark; run explicitly with --ignored --nocapture"]
async fn benchmark_connected_match_batching() {
    let params = parameters(&(1..=668).collect::<Vec<_>>(), 1_000);
    let mut expected = None;
    let mut batched_times = Vec::new();
    let mut scalar_times = Vec::new();
    println!("in-process in-memory execution benchmark: 668 entities, 16 shared chunks, 48 sources; cap=1000");
    for repetition in 0..5 {
        for barriers in if repetition % 2 == 0 { [false, true] } else { [true, false] } {
            let storage = benchmark_fixture();
            let engine = CypherEngine::new(storage.clone());
            let prepared = if barriers {
                prepare_unrewritten(&engine, &query(true, GLOBAL), &params)
            } else {
                engine.prepare_with_lists(&query(false, GLOBAL), &params.scalars, &params.lists)
            }
            .expect("prepare benchmark");
            let start = Instant::now();
            let rows = engine.execute_prepared(&prepared).await.expect("execute benchmark").rows;
            let elapsed = start.elapsed();
            assert_eq!(rows.len(), 1_000);
            if let Some(expected) = &expected {
                assert_eq!(&rows, expected);
            } else {
                expected = Some(rows);
            }
            let work = work(&storage);
            println!(
                "repetition={} mode={} elapsed_ms={:.3} {work:?}",
                repetition + 1,
                if barriers { "scalar-with" } else { "batched" },
                elapsed.as_secs_f64() * 1_000.0
            );
            if barriers {
                scalar_times.push((elapsed, work));
            } else {
                batched_times.push((elapsed, work));
            }
        }
    }
    for ((_, batched), (_, scalar)) in batched_times.iter().zip(&scalar_times) {
        assert!(batched.calls < scalar.calls, "{batched:?} vs {scalar:?}");
        assert!(batched.input_ids < scalar.input_ids, "{batched:?} vs {scalar:?}");
        assert!(
            batched.matching_relationships < scalar.matching_relationships,
            "{batched:?} vs {scalar:?}"
        );
        assert!(
            batched.relationship_records < scalar.relationship_records,
            "{batched:?} vs {scalar:?}"
        );
    }
    let median = |runs: &[(Duration, Work)]| {
        let mut times = runs.iter().map(|(elapsed, _)| *elapsed).collect::<Vec<_>>();
        times.sort();
        times[times.len() / 2].as_secs_f64() * 1_000.0
    };
    println!(
        "median elapsed_ms: batched={:.3} scalar-with={:.3}",
        median(&batched_times),
        median(&scalar_times)
    );
}

#[tokio::test]
async fn linear_expansion_rechecks_a_previously_bound_target() {
    let storage = fixture();
    let source = format!("{SEEDS} MATCH (c:Chunk {{n: 10}}) MATCH (e)-[:PRESENT_IN]->(c) RETURN e.n AS e, c.n AS c");
    let rows = run(&storage, &source, &parameters(&[1, 2, 3], 100)).await;
    assert_eq!(rows.len(), 3);
    assert!(storage
        .expand_requests
        .lock()
        .unwrap()
        .iter()
        .any(|request| request.input_vertex_ids == [1, 2, 3]));
}
