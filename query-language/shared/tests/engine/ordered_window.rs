//! `ORDER BY <string property> [SKIP] LIMIT` over a filtered node scan must
//! become a bounded ordered index walk, and must produce exactly the rows the
//! generic sort/filter plan would.

mod common;

use common::MemoryStorage;
use hydradb_cypher_engine::{CypherEngine, GraphPhysicalPlan, ParameterMap, QueryRow, QueryValue, ScalarValue, VertexRecord};

fn stamped(id: u64, created_at: &str) -> VertexRecord {
    VertexRecord::new(id)
        .with_label("Entity")
        .with_property("created_at", ScalarValue::String(created_at.into()))
}

fn fixture() -> MemoryStorage {
    MemoryStorage::with_vertices([
        stamped(1, "2026-01-01"),
        stamped(2, "2026-01-02"),
        stamped(3, "2026-01-03"),
        // A tie on the sort key: the id tie-break must be honored across the
        // window boundary.
        stamped(4, "2026-01-03"),
        stamped(5, "2026-01-04"),
        stamped(6, "2026-01-05"),
        // Wrong label: carries the property but must never appear.
        VertexRecord::new(7)
            .with_label("Other")
            .with_property("created_at", ScalarValue::String("2026-01-06".into())),
        // Non-string value: the string index never holds it, and the
        // predicate would reject it anyway.
        VertexRecord::new(8)
            .with_label("Entity")
            .with_property("created_at", ScalarValue::Integer(20260107)),
    ])
}

fn ids(rows: &[QueryRow]) -> Vec<u64> {
    rows.iter()
        .map(|row| match row.values.as_slice() {
            [QueryValue::VertexId(id)] => *id,
            other => panic!("unexpected row {other:?}"),
        })
        .collect()
}

fn assert_ordered_scan(plan: &GraphPhysicalPlan, expected_required: usize) {
    let GraphPhysicalPlan::Project { input, .. } = plan else {
        panic!("expected projection root, got {plan:?}");
    };
    let GraphPhysicalPlan::Limit { input, .. } = input.as_ref() else {
        panic!("expected limit under projection, got {input:?}");
    };
    let scan = match input.as_ref() {
        GraphPhysicalPlan::Skip { input, .. } => input.as_ref(),
        other => other,
    };
    let GraphPhysicalPlan::VertexScan { scan } = scan else {
        panic!("expected vertex scan, got {scan:?}");
    };
    assert_eq!(scan.access_kind(), "ordered-property");
    assert!(scan.properties().window_accepted);
    assert_eq!(scan.request().window.as_ref().unwrap().required, expected_required);
}

#[tokio::test]
async fn index_sort_eligibility_respects_null_order_on_every_key() {
    // §16.17 GR1g and IS001: explicit non-default null ordering needs a
    // generic sort, including on a later key. Ties still use the next key.
    let mut storage = fixture();
    // The generic predicate must not evaluate STARTS WITH on the intentional
    // non-string control in the broader index-only fixture.
    storage.vertices.remove(&8);
    let engine = CypherEngine::new(storage);
    for (sort, ordered, expected) in [
        ("n.created_at ASC NULLS LAST, n.id", true, vec![1, 2, 3]),
        ("n.created_at DESC NULLS FIRST, n.id", true, vec![6, 5, 3]),
        ("n.created_at ASC NULLS FIRST, n.id", false, vec![1, 2, 3]),
        ("n.created_at DESC NULLS LAST, n.id", false, vec![6, 5, 3]),
        ("n.created_at DESC, n.id DESC NULLS LAST", false, vec![6, 5, 4]),
    ] {
        let query = format!("MATCH (n:Entity) WHERE n.created_at STARTS WITH '' RETURN n.id AS id ORDER BY {sort} LIMIT 3");
        let prepared = engine.prepare(&query, &ParameterMap::new()).unwrap();
        assert_eq!(prepared.explain().contains("access=ordered-property"), ordered, "{query}");
        assert_eq!(ids(&engine.execute_prepared(&prepared).await.unwrap().rows), expected, "{query}");
    }
}

#[tokio::test]
async fn descending_window_reads_only_the_leading_rows() {
    let storage = fixture();
    let engine = CypherEngine::new(storage.clone());
    let prepared = engine
        .prepare(
            "MATCH (n:Entity) WHERE n.created_at STARTS WITH '' \
             RETURN n.id AS id ORDER BY n.created_at DESC, n.id LIMIT 3",
            &ParameterMap::new(),
        )
        .expect("prepare ordered window");
    assert_ordered_scan(&prepared.physical, 3);
    let explain = prepared.explain();
    assert!(explain.contains("access=ordered-property"), "{explain}");
    assert!(explain.contains("items=[n.created_at DESC, n.id ASC]"), "{explain}");
    assert!(!explain.contains("SortExec"), "{explain}");
    assert!(!explain.contains("FilterExec"), "{explain}");

    let result = engine.execute_prepared(&prepared).await.expect("execute ordered window");
    assert_eq!(ids(&result.rows), vec![6, 5, 3]);

    let requests = storage.ordered_scan_requests.lock().unwrap();
    assert_eq!(requests.len(), 1);
    assert!(!requests[0].ascending);
    assert_eq!(requests[0].required, 3);
    assert_eq!(requests[0].prefix.as_deref(), Some(""));
    assert!(
        storage.hydration_requests.lock().unwrap().is_empty(),
        "the ordered scan returns hydrated rows; no separate hydration pass"
    );
}

#[tokio::test]
async fn tie_on_the_boundary_uses_the_id_tie_break() {
    let storage = fixture();
    let engine = CypherEngine::new(storage.clone());
    // Ascending: 1, 2, then the tie {3, 4} on 2026-01-03. LIMIT 3 must take
    // vertex 3, not 4, and LIMIT 4 must take both.
    let three = engine
        .execute(
            "MATCH (n:Entity) WHERE n.created_at >= '2026' \
             RETURN n.id AS id ORDER BY n.created_at, n.id LIMIT 3",
            &ParameterMap::new(),
        )
        .await
        .expect("execute limit 3");
    assert_eq!(ids(&three.rows), vec![1, 2, 3]);
    let four = engine
        .execute(
            "MATCH (n:Entity) WHERE n.created_at >= '2026' \
             RETURN n.id AS id ORDER BY n.created_at, n.id LIMIT 4",
            &ParameterMap::new(),
        )
        .await
        .expect("execute limit 4");
    assert_eq!(ids(&four.rows), vec![1, 2, 3, 4]);
}

#[tokio::test]
async fn keyset_cursor_becomes_an_exclusive_lower_bound() {
    let storage = fixture();
    let engine = CypherEngine::new(storage.clone());
    let parameters = ParameterMap::from([("cursor".to_string(), ScalarValue::String("2026-01-02".into()))]);
    let prepared = engine
        .prepare(
            "MATCH (n:Entity) WHERE n.created_at > $cursor AND n.created_at STARTS WITH '' \
             RETURN n.id AS id ORDER BY n.created_at, n.id LIMIT 2",
            &parameters,
        )
        .expect("prepare keyset page");
    assert_ordered_scan(&prepared.physical, 2);
    let result = engine.execute_prepared(&prepared).await.expect("execute keyset page");
    assert_eq!(ids(&result.rows), vec![3, 4]);

    let requests = storage.ordered_scan_requests.lock().unwrap();
    let lower = requests[0].lower.as_ref().expect("lower bound pushed down");
    assert_eq!(lower.value, ScalarValue::String("2026-01-02".into()));
    assert!(!lower.inclusive);
}

#[tokio::test]
async fn skip_is_folded_into_the_required_window() {
    let storage = fixture();
    let engine = CypherEngine::new(storage.clone());
    let prepared = engine
        .prepare(
            "MATCH (n:Entity) WHERE n.created_at STARTS WITH '2026' \
             RETURN n.id AS id ORDER BY n.created_at DESC, n.id SKIP 1 LIMIT 2",
            &ParameterMap::new(),
        )
        .expect("prepare skip window");
    assert_ordered_scan(&prepared.physical, 3);
    let result = engine.execute_prepared(&prepared).await.expect("execute skip window");
    assert_eq!(ids(&result.rows), vec![5, 3]);
}

#[tokio::test]
async fn upper_bound_and_prefix_are_pushed_down() {
    let storage = fixture();
    let engine = CypherEngine::new(storage.clone());
    let result = engine
        .execute(
            "MATCH (n:Entity) WHERE n.created_at < '2026-01-04' AND n.created_at STARTS WITH '2026-01' \
             RETURN n.id AS id ORDER BY n.created_at DESC, n.id LIMIT 10",
            &ParameterMap::new(),
        )
        .await
        .expect("execute bounded window");
    assert_eq!(ids(&result.rows), vec![3, 4, 2, 1]);
    let requests = storage.ordered_scan_requests.lock().unwrap();
    assert_eq!(requests[0].prefix.as_deref(), Some("2026-01"));
    let upper = requests[0].upper.as_ref().expect("upper bound pushed down");
    assert_eq!(upper.value, ScalarValue::String("2026-01-04".into()));
    assert!(!upper.inclusive);
}

#[tokio::test]
async fn shapes_the_index_cannot_prove_keep_the_generic_plan() {
    let storage = fixture();
    let engine = CypherEngine::new(storage.clone());
    for query in [
        // Sort key differs from the filtered property.
        "MATCH (n:Entity) WHERE n.created_at STARTS WITH '' RETURN n.id AS id ORDER BY n.id LIMIT 2",
        // OR is not a single index range, and nothing else bounds the walk.
        "MATCH (n:Entity) WHERE n.created_at > 'a' OR n.created_at < 'b' RETURN n.id AS id ORDER BY n.created_at LIMIT 2",
        // The executor does not negate floats, so neither does the planner:
        // a unary minus is not a literal the index can seek to.
        "MATCH (n:Entity) WHERE n.created_at > -2.5 RETURN n.id AS id ORDER BY n.created_at LIMIT 2",
        // A null operand bounds nothing: the walk skips vertices without the
        // property, and null sorts above every value.
        "MATCH (n:Entity) WHERE n.created_at > null RETURN n.id AS id ORDER BY n.created_at LIMIT 2",
        // No LIMIT: nothing bounds the walk.
        "MATCH (n:Entity) WHERE n.created_at STARTS WITH '' RETURN n.id AS id ORDER BY n.created_at",
    ] {
        let prepared = engine.prepare(query, &ParameterMap::new()).expect("prepare generic shape");
        assert!(
            !prepared.explain().contains("access=ordered-property"),
            "{query}\n{}",
            prepared.explain()
        );
    }
    assert!(storage.ordered_scan_requests.lock().unwrap().is_empty());
}

#[tokio::test]
async fn limit_zero_reads_nothing() {
    let storage = fixture();
    let engine = CypherEngine::new(storage.clone());
    let result = engine
        .execute(
            "MATCH (n:Entity) WHERE n.created_at STARTS WITH '' \
             RETURN n.id AS id ORDER BY n.created_at LIMIT 0",
            &ParameterMap::new(),
        )
        .await
        .expect("execute empty window");
    assert!(result.rows.is_empty());
    assert!(storage.ordered_scan_requests.lock().unwrap().is_empty());
}

/// Nothing at plan time separates an equality that names one vertex from one
/// that names a hundred thousand, so the walk starts either way and the
/// runtime settles it. An equality the storage probe can enumerate is one the
/// walk was never going to win, and the probe's answer covers the whole
/// predicate, so the walk is abandoned rather than resumed.
#[tokio::test]
async fn a_walk_the_residual_starves_falls_back_to_a_seek() {
    let mut storage = MemoryStorage::with_vertices((1..=60u64).map(|id| {
        VertexRecord::new(id)
            .with_label("Entity")
            .with_property("created_at", ScalarValue::String(format!("2026-01-{id:02}").into()))
            .with_property(
                "entity_id",
                ScalarValue::String(if id == 47 { "x".into() } else { format!("e{id}").into() }),
            )
    }));
    storage.selective_properties = vec!["entity_id".to_string()];
    let engine = CypherEngine::new(storage.clone());
    let query = "MATCH (n:Entity) WHERE n.created_at STARTS WITH '' AND n.entity_id = 'x' \
                 RETURN n.id AS id ORDER BY n.created_at LIMIT 2";

    let prepared = engine.prepare(query, &ParameterMap::new()).expect("prepare starved window");
    assert!(prepared.explain().contains("access=ordered-property"), "{}", prepared.explain());

    let result = engine.execute(query, &ParameterMap::new()).await.expect("execute starved window");
    assert_eq!(ids(&result.rows), vec![47]);

    let probes = storage.probe_requests.lock().unwrap();
    assert_eq!(probes.len(), 1, "the probe is asked once, not once per page");
    assert!(probes[0].iter().any(|candidate| candidate.property == "entity_id"));

    // The walk stopped short of the vertex it was hunting, which is the point:
    // reaching it by walking is exactly the cost being avoided.
    let walked: usize = storage
        .ordered_scan_requests
        .lock()
        .unwrap()
        .iter()
        .map(|request| request.required)
        .sum();
    assert!(walked < 47, "walk asked for {walked} rows before giving up");
}

/// A residual whose equalities are all broad keeps the walk. The probe
/// declining is the evidence that no seek is narrower than the window.
#[tokio::test]
async fn a_walk_no_seek_can_beat_keeps_going() {
    let storage = MemoryStorage::with_vertices((1..=60u64).map(|id| {
        VertexRecord::new(id)
            .with_label("Entity")
            .with_property("created_at", ScalarValue::String(format!("2026-01-{id:02}").into()))
            .with_property(
                "tenant_id",
                ScalarValue::String(if id >= 50 { "t".into() } else { format!("t{id}").into() }),
            )
    }));
    let engine = CypherEngine::new(storage.clone());
    let result = engine
        .execute(
            "MATCH (n:Entity) WHERE n.created_at STARTS WITH '' AND n.tenant_id = 't' \
             RETURN n.id AS id ORDER BY n.created_at LIMIT 2",
            &ParameterMap::new(),
        )
        .await
        .expect("execute broad window");
    assert_eq!(ids(&result.rows), vec![50, 51]);
    assert!(!storage.probe_requests.lock().unwrap().is_empty());
}

/// The index orders ties by vertex ID, so a tie key on a property can rank a
/// row ahead of one that arrived before it. The walk must read the whole tie
/// group before it stops, or the window closes on arrival order instead of the
/// order the query asked for.
#[tokio::test]
async fn a_property_tie_key_reads_the_whole_tie_group() {
    let storage = MemoryStorage::with_vertices([
        // Same instant, and the entity_id order is the reverse of the ID order.
        stamped(1, "2026-01-01").with_property("entity_id", ScalarValue::String("b".into())),
        stamped(2, "2026-01-01").with_property("entity_id", ScalarValue::String("a".into())),
        stamped(3, "2026-01-02").with_property("entity_id", ScalarValue::String("c".into())),
    ]);
    let engine = CypherEngine::new(storage.clone());
    let prepared = engine
        .prepare(
            "MATCH (n:Entity) WHERE n.created_at STARTS WITH '' \
             RETURN n.id AS id ORDER BY n.created_at, n.entity_id LIMIT 1",
            &ParameterMap::new(),
        )
        .expect("prepare property tie key");
    assert_ordered_scan(&prepared.physical, 1);

    let result = engine.execute_prepared(&prepared).await.expect("execute property tie key");
    assert_eq!(ids(&result.rows), vec![2], "vertex 1 arrives first but sorts second");
    assert!(
        storage.ordered_scan_requests.lock().unwrap().len() > 1,
        "one page cannot have proven the tie group complete"
    );
}

/// Descending is the same argument read the other way: the tie group is still
/// contiguous, and the tie key still orders within it independently.
#[tokio::test]
async fn a_property_tie_key_holds_on_a_descending_walk() {
    let storage = MemoryStorage::with_vertices([
        stamped(1, "2026-01-01").with_property("entity_id", ScalarValue::String("a".into())),
        stamped(2, "2026-01-02").with_property("entity_id", ScalarValue::String("z".into())),
        stamped(3, "2026-01-02").with_property("entity_id", ScalarValue::String("m".into())),
    ]);
    let engine = CypherEngine::new(storage.clone());
    let result = engine
        .execute(
            "MATCH (n:Entity) WHERE n.created_at STARTS WITH '' \
             RETURN n.id AS id ORDER BY n.created_at DESC, n.entity_id LIMIT 1",
            &ParameterMap::new(),
        )
        .await
        .expect("execute descending property tie key");
    assert_eq!(ids(&result.rows), vec![3]);
}

// ---------------------------------------------------------------------------
// Every scalar type. The index walks booleans, then integers and floats merged
// by value, then strings, so a range on a numeric sort key sees exactly what
// the generic filter and sort see - including strings above a lower bound and
// booleans below an upper bound, which rank there.
// ---------------------------------------------------------------------------

fn scored(id: u64, score: Option<ScalarValue>, kind: &str) -> VertexRecord {
    let mut vertex = VertexRecord::new(id)
        .with_label("Entity")
        .with_property("kind", ScalarValue::String(kind.into()));
    if let Some(score) = score {
        vertex = vertex.with_property("score", score);
    }
    if id.is_multiple_of(3) {
        vertex = vertex.with_label("Tagged");
    }
    vertex
}

/// Integers and floats that tie by value (`1`, `1.0`; `0`, `0.0`, `-0.0`),
/// duplicates, negatives, an integer beyond `u64`, infinities and NaN, plus
/// booleans, strings and rows without the key.
fn scored_fixture(page_size: Option<usize>) -> MemoryStorage {
    let float = |text: &str| Some(ScalarValue::Float(text.into()));
    let integer = |value: i128| Some(ScalarValue::Integer(value));
    let scores = [
        integer(1),
        float("1.0"),
        integer(-3),
        float("-0"),
        integer(0),
        float("0.0"),
        float("2.5"),
        integer(2),
        integer(2),
        float("-inf"),
        float("inf"),
        float("NaN"),
        integer(1 << 70),
        integer(-(1 << 40)),
        float("-2.75"),
        Some(ScalarValue::Boolean(true)),
        Some(ScalarValue::Boolean(false)),
        Some(ScalarValue::String("10".into())),
        Some(ScalarValue::String("abc".into())),
        None,
        integer(1),
        float("2.0"),
        integer(7),
        float("6.5"),
    ];
    let mut storage = MemoryStorage::with_vertices(scores.into_iter().enumerate().map(|(index, score)| {
        let id = index as u64 + 1;
        scored(id, score, if id.is_multiple_of(4) { "drop" } else { "keep" })
    }));
    storage.ordered_page_size = page_size;
    storage
}

#[tokio::test]
async fn numeric_windows_match_the_generic_plan_at_every_page_size() {
    let parameters = ParameterMap::from([
        ("int_cursor".to_string(), ScalarValue::Integer(1)),
        ("float_cursor".to_string(), ScalarValue::Float("2.0".into())),
        ("after_id".to_string(), ScalarValue::Integer(2)),
    ]);
    let shapes = [
        // Strict tier: the index proves the whole predicate.
        "WHERE n.score > 0 RETURN n.id AS id ORDER BY n.score, n.id",
        "WHERE n.score >= -3 AND n.score < 2.5 RETURN n.id AS id ORDER BY n.score DESC, n.id",
        "WHERE n.score <= 2 RETURN n.id AS id ORDER BY n.score DESC, n.id DESC",
        "WHERE n.score >= 0.0 AND n.score <= 0 RETURN n.id AS id ORDER BY n.score, n.id DESC",
        "WHERE n.score > $float_cursor RETURN n.id AS id ORDER BY n.score, n.id",
        // A boolean bound: only `true` ranks above it.
        "WHERE n.score > false AND n.score < 0 RETURN n.id AS id ORDER BY n.score, n.id",
        // Residual tier: a bound in the walk direction plus another property.
        "WHERE n.score >= -1 AND n.kind <> 'drop' RETURN n.id AS id ORDER BY n.score, n.id",
        "WHERE n.score < 3 AND n.kind <> 'drop' RETURN n.id AS id ORDER BY n.score DESC, n.id",
        // Keyset continuation on (score, id), from an integer and a float
        // cursor that tie values of the other numeric type.
        "WHERE n.score > $int_cursor OR (n.score = $int_cursor AND n.id > $after_id) \
         RETURN n.id AS id ORDER BY n.score, n.id",
        "WHERE n.score < $float_cursor OR (n.score = $float_cursor AND n.id < $after_id) \
         RETURN n.id AS id ORDER BY n.score DESC, n.id DESC",
        // Empty: nothing lies strictly between the two bounds.
        "WHERE n.score > 7 AND n.score < 7.0 RETURN n.id AS id ORDER BY n.score, n.id",
    ];
    for page_size in [None, Some(1), Some(2), Some(5)] {
        let storage = scored_fixture(page_size);
        for pattern in ["MATCH (n:Entity)", "MATCH (n:Entity:Tagged)"] {
            for shape in shapes {
                let query = format!("{pattern} {shape}");
                for (skip, limit) in [(0, 1), (0, 3), (1, 4), (0, 30)] {
                    let window = if skip == 0 {
                        format!("LIMIT {limit}")
                    } else {
                        format!("SKIP {skip} LIMIT {limit}")
                    };
                    assert_window_matches_generic(&storage, &query, &window, skip, limit, &parameters).await;
                }
            }
        }
    }
}

#[tokio::test]
async fn integer_and_float_ties_break_by_vertex_id() {
    let storage = scored_fixture(Some(1));
    let rows = assert_window_matches_generic(
        &storage,
        "MATCH (n:Entity) WHERE n.score >= 0 AND n.score <= 2 \
         RETURN n.id AS id ORDER BY n.score, n.id",
        "LIMIT 20",
        0,
        20,
        &ParameterMap::new(),
    )
    .await;
    // 0 = {-0.0: 4, 0: 5, 0.0: 6}; 1 = {1: 1, 1.0: 2, 1: 21}; 2 = {2: 8, 9, 2.0: 22}.
    assert_eq!(rows, vec![4, 5, 6, 1, 2, 21, 8, 9, 22]);
}

async fn assert_window_matches_generic(
    storage: &MemoryStorage,
    query: &str,
    window: &str,
    skip: usize,
    limit: usize,
    parameters: &ParameterMap,
) -> Vec<u64> {
    let engine = CypherEngine::new(storage.clone());
    let windowed = format!("{query} {window}");
    let prepared = engine.prepare(&windowed, parameters).expect("prepare windowed query");
    let explain = prepared.explain();
    assert!(explain.contains("access=ordered-property"), "{windowed}\n{explain}");
    assert!(!explain.contains("SortExec"), "{windowed}\n{explain}");
    let actual = engine.execute_prepared(&prepared).await.expect("execute windowed query");
    let generic = engine.prepare(query, parameters).expect("prepare generic query");
    assert!(!generic.explain().contains("access=ordered-property"), "{query}");
    let expected = engine.execute_prepared(&generic).await.expect("execute generic query");
    let expected = ids(&expected.rows).into_iter().skip(skip).take(limit).collect::<Vec<_>>();
    assert_eq!(ids(&actual.rows), expected, "{windowed}");
    expected
}

#[tokio::test]
async fn provider_window_guarantees_match_execution_and_declines_keep_engine_operators() {
    use hydradb_cypher_engine::{PreparedVertexScan, SlateDbDataSource, UnknownStatistics};

    fn scan(plan: &GraphPhysicalPlan) -> &PreparedVertexScan {
        match plan {
            GraphPhysicalPlan::VertexScan { scan } => scan,
            other => scan(other.children().first().expect("a scan below the relational operators")),
        }
    }

    let storage = MemoryStorage::with_vertices(
        (1..=6).map(|id| stamped(id, &format!("2026-01-{id:02}")).with_property("keep", ScalarValue::Boolean(id >= 5))),
    );
    let engine = CypherEngine::new(storage);
    let query = "MATCH (n:Entity) WHERE n.created_at STARTS WITH '2026-private' OR false RETURN n.id AS id";
    let redacted = engine.prepare(query, &ParameterMap::new()).unwrap();
    assert!(!redacted.plan_shape().contains("2026-private"));

    let ordered = engine
        .prepare(
            "MATCH (n:Entity) WHERE n.created_at STARTS WITH '2026' AND n.keep = true \
         RETURN n.id AS id ORDER BY n.created_at LIMIT 2",
            &ParameterMap::new(),
        )
        .unwrap();
    let accepted = scan(&ordered.physical);
    assert_eq!(accepted.access_kind(), "ordered-property");
    assert!(accepted.properties().window_accepted);
    assert_eq!(accepted.properties().ordering, accepted.request().window.as_ref().unwrap().ordering);
    assert!(accepted.properties().residual_predicates.is_empty());
    assert!(ordered.explain().contains("residual=(n.keep = true)"));
    assert!(!ordered.explain().contains("FilterExec"));
    assert!(!ordered.explain().contains("SortExec"));
    // Four earlier index entries fail the residual. They cannot consume the window.
    assert_eq!(ids(&engine.execute_prepared(&ordered).await.unwrap().rows), [5, 6]);

    let mut request = accepted.request().clone();
    request.window.as_mut().unwrap().ordering[0].expression = hydradb_cypher_engine::PhysicalExpression::Identity(request.binding.clone());
    let declined = SlateDbDataSource::new(&UnknownStatistics).prepare_vertex_scan(request.clone());
    assert!(!declined.properties().window_accepted);
    assert!(declined.properties().ordering.is_empty());
    assert_eq!(declined.properties().residual_predicates, request.predicates);

    let generic = engine
        .prepare(
            "MATCH (n:Entity) WHERE n.created_at STARTS WITH '2026' AND n.keep = true \
         RETURN n.id AS id ORDER BY n.id LIMIT 2",
            &ParameterMap::new(),
        )
        .unwrap();
    assert!(generic.explain().contains("FilterExec"));
    assert!(generic.explain().contains("SortExec"));
    assert!(generic.explain().contains("LimitExec"));
    assert_eq!(ids(&engine.execute_prepared(&generic).await.unwrap().rows), [5, 6]);
}

#[test]
fn declined_ordered_rewrites_preserve_filter_error_precedence() {
    let engine = CypherEngine::new(MemoryStorage::default());
    for ordering in ["$sort ASC NULLS FIRST", "$sort DESC NULLS LAST", "n.id"] {
        let query = format!("MATCH (n:Entity) WHERE n.age > $filter RETURN n ORDER BY {ordering} LIMIT 2");
        let error = engine.prepare(&query, &ParameterMap::new()).unwrap_err();
        assert!(error.to_string().contains("missing query parameter $filter"), "{query}: {error}");
        assert_eq!(error.operation(), hydradb_cypher_ast::operation::MATCH, "{query}: {error}");
    }
}
