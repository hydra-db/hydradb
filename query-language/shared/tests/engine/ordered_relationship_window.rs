//! `ORDER BY r.p [SKIP] LIMIT` over a filtered single-type expansion becomes a
//! bounded ordered walk of the relationship property index, and returns
//! exactly the rows the generic expand/filter/sort plan returns.

mod common;

use common::MemoryStorage;
use hydradb_cypher_engine::{CypherEngine, ParameterMap, QueryRow, QueryValue, RelationshipRecord, ScalarValue, VertexRecord};

fn person(id: u64) -> VertexRecord {
    let mut vertex = VertexRecord::new(id).with_label("Person");
    if id.is_multiple_of(3) {
        vertex = vertex.with_label("Tagged");
    }
    vertex
}

/// A `RATED` relationship whose client `id` property, what `r.id` reads,
/// equals its internal identity, as HydraDB's identified write path stores it.
fn rated(id: u64, source: u64, target: u64, score: Option<ScalarValue>) -> RelationshipRecord {
    let mut relationship = RelationshipRecord::new(id, source, target, "RATED").with_property("id", ScalarValue::Integer(i128::from(id)));
    if let Some(score) = score {
        relationship = relationship.with_property("score", score);
    }
    let kind = [Some("keep"), Some("drop"), None][(id % 3) as usize];
    if let Some(kind) = kind {
        relationship = relationship.with_property("kind", ScalarValue::String(kind.into()));
    }
    relationship.with_property("at", ScalarValue::String(format!("2026-03-{:02}", id % 7 + 1).into()))
}

/// Duplicate scores whose ties straddle every small page, integer and float
/// ties (`1`/`1.0`, `0`/`-0.0`), parallel relationships between one pair, a
/// self-loop, relationships whose endpoints fail a label, rows with a null
/// residual property, a relationship without the score, and another type
/// carrying the same property.
fn fixture(page_size: Option<usize>) -> MemoryStorage {
    let mut vertices = (1..=12).map(person).collect::<Vec<_>>();
    vertices.push(VertexRecord::new(50).with_label("Robot"));
    let int = ScalarValue::Integer;
    let float = |text: &str| ScalarValue::Float(text.into());
    let scores = [
        Some(int(3)),
        Some(float("1.0")),
        Some(int(1)),
        Some(int(3)),
        Some(float("2.5")),
        Some(int(-1)),
        Some(int(0)),
        Some(float("-0.0")),
        Some(int(3)),
        None,
        Some(int(7)),
        Some(int(1)),
        Some(float("2.5")),
        Some(int(3)),
        Some(int(5)),
        Some(int(3)),
        Some(int(1)),
        Some(float("6.5")),
        Some(int(3)),
        Some(int(2)),
    ];
    let mut relationships = scores
        .into_iter()
        .enumerate()
        .map(|(index, score)| {
            let id = 100 + index as u64;
            let source = (index as u64 * 5) % 12 + 1;
            let target = (index as u64 * 7 + 3) % 12 + 1;
            rated(id, source, target, score)
        })
        .collect::<Vec<_>>();
    // Parallel relationships between one pair, tying on the score.
    relationships.push(rated(200, 4, 9, Some(int(3))));
    relationships.push(rated(201, 4, 9, Some(int(3))));
    relationships.push(rated(202, 4, 9, Some(float("3.0"))));
    // A self-loop.
    relationships.push(rated(203, 6, 6, Some(int(2))));
    // An endpoint that is not a Person.
    relationships.push(rated(204, 50, 2, Some(int(1))));
    relationships.push(rated(205, 2, 50, Some(int(4))));
    // Another type with the same property.
    relationships.push(RelationshipRecord::new(206, 1, 2, "OTHER").with_property("score", int(2)));
    let mut storage = MemoryStorage::with_graph(vertices, relationships);
    storage.ordered_page_size = page_size;
    storage
}

fn relationship_ids(rows: &[QueryRow]) -> Vec<u64> {
    rows.iter()
        .map(|row| match row.values.as_slice() {
            [QueryValue::Scalar(ScalarValue::Integer(id))] => u64::try_from(*id).expect("relationship id"),
            other => panic!("unexpected row {other:?}"),
        })
        .collect()
}

#[tokio::test]
async fn relationship_index_sort_eligibility_respects_null_order_on_every_key() {
    let edges = [
        rated(1, 1, 2, Some(ScalarValue::Integer(2))),
        rated(2, 1, 2, Some(ScalarValue::Integer(1))),
        rated(3, 1, 2, Some(ScalarValue::Integer(1))).with_property("tie", ScalarValue::Integer(1)),
        rated(4, 1, 2, None),
    ];
    let engine = CypherEngine::new(MemoryStorage::with_graph([person(1), person(2)], edges));
    // §16.17 GR1g: the score ties are resolved by subsequent keys, whose
    // explicit null placement is just as binding as the first key's.
    for (sort, ordered, expected) in [
        ("r.score ASC NULLS LAST, r.id", true, vec![2, 3]),
        ("r.score DESC NULLS FIRST, r.id", true, vec![1, 2]),
        ("r.score ASC NULLS FIRST, r.id", false, vec![2, 3]),
        ("r.score DESC NULLS LAST, r.id", false, vec![1, 2]),
        ("r.score ASC, r.tie ASC NULLS FIRST, r.id", false, vec![2, 3]),
        ("r.score ASC, r.tie ASC NULLS LAST, r.id", true, vec![3, 2]),
    ] {
        let query = format!("MATCH (a:Person)-[r:RATED]->(b:Person) WHERE r.score >= 0 RETURN r.id AS id ORDER BY {sort} LIMIT 2");
        let prepared = engine.prepare(&query, &ParameterMap::new()).unwrap();
        assert_eq!(prepared.explain().contains("OrderedRelationshipPropertyScan"), ordered, "{query}");
        assert_eq!(
            relationship_ids(&engine.execute_prepared(&prepared).await.unwrap().rows),
            expected,
            "{query}"
        );
    }
}

async fn assert_window_matches_generic(
    storage: &MemoryStorage,
    query: &str,
    skip: usize,
    limit: usize,
    parameters: &ParameterMap,
) -> Vec<u64> {
    let engine = CypherEngine::new(storage.clone());
    let window = if skip == 0 {
        format!("LIMIT {limit}")
    } else {
        format!("SKIP {skip} LIMIT {limit}")
    };
    let windowed = format!("{query} {window}");
    let prepared = engine
        .prepare(&windowed, parameters)
        .unwrap_or_else(|error| panic!("prepare {windowed}: {error}"));
    let explain = prepared.explain();
    assert!(explain.contains("OrderedRelationshipPropertyScan"), "{windowed}\n{explain}");
    assert!(!explain.contains("SortExec"), "{windowed}\n{explain}");
    assert!(!explain.contains("ExpandExec"), "{windowed}\n{explain}");
    let actual = engine
        .execute_prepared(&prepared)
        .await
        .unwrap_or_else(|error| panic!("execute {windowed}: {error}"));
    let generic = engine
        .prepare(query, parameters)
        .unwrap_or_else(|error| panic!("prepare {query}: {error}"));
    assert!(!generic.explain().contains("OrderedRelationshipPropertyScan"), "{query}");
    let expected = engine
        .execute_prepared(&generic)
        .await
        .unwrap_or_else(|error| panic!("execute {query}: {error}"));
    let expected = relationship_ids(&expected.rows)
        .into_iter()
        .skip(skip)
        .take(limit)
        .collect::<Vec<_>>();
    assert_eq!(relationship_ids(&actual.rows), expected, "{windowed}");
    expected
}

#[tokio::test]
async fn relationship_windows_match_the_generic_plan_at_every_page_size() {
    let none = ParameterMap::new();
    let keyset = ParameterMap::from([
        ("after_score".to_string(), ScalarValue::Integer(3)),
        ("after_id".to_string(), ScalarValue::Integer(108)),
    ]);
    let float_keyset = ParameterMap::from([
        ("after_score".to_string(), ScalarValue::Float("2.5".into())),
        ("after_id".to_string(), ScalarValue::Integer(112)),
    ]);
    // Every order ends in a total tie-break, so the generic plan's stable
    // order and the walk's index order cannot legitimately differ.
    let cases: &[(&str, &ParameterMap)] = &[
        // Strict tier, ascending and descending.
        (
            "MATCH (a:Person)-[r:RATED]->(b:Person) WHERE r.score >= 0 \
             RETURN r.id AS id ORDER BY r.score, r.id",
            &none,
        ),
        (
            "MATCH (a:Person)-[r:RATED]->(b:Person) WHERE r.score <= 5 \
             RETURN r.id AS id ORDER BY r.score DESC, r.id",
            &none,
        ),
        (
            "MATCH (a:Person)-[r:RATED]->(b:Person) WHERE r.score > -1 AND r.score < 3 \
             RETURN r.id AS id ORDER BY r.score DESC, r.id DESC",
            &none,
        ),
        // Residual on another relationship property, and on endpoints.
        (
            "MATCH (a:Person)-[r:RATED]->(b:Person) WHERE r.score >= 1 AND r.kind <> 'drop' \
             RETURN r.id AS id ORDER BY r.score, r.id DESC",
            &none,
        ),
        (
            "MATCH (a:Person)-[r:RATED]->(b:Person) WHERE r.score <= 7 AND a.id > 3 AND b.id <> 9 \
             RETURN r.id AS id ORDER BY r.score DESC, b.id, r.id",
            &none,
        ),
        // A residual on a sometimes-missing property: NULL rejects.
        (
            "MATCH (a:Person)-[r:RATED]->(b:Person) WHERE r.score >= 0 AND r.kind >= 'k' \
             RETURN r.id AS id ORDER BY r.score, r.id",
            &none,
        ),
        // Keyset continuation from an integer and a float cursor.
        (
            "MATCH (a:Person)-[r:RATED]->(b:Person) \
             WHERE r.score > $after_score OR (r.score = $after_score AND r.id > $after_id) \
             RETURN r.id AS id ORDER BY r.score, r.id",
            &keyset,
        ),
        (
            "MATCH (a:Person)-[r:RATED]->(b:Person) \
             WHERE r.score < $after_score OR (r.score = $after_score AND r.id < $after_id) \
             RETURN r.id AS id ORDER BY r.score DESC, r.id DESC",
            &float_keyset,
        ),
        // Incoming pattern and multi-label endpoints.
        (
            "MATCH (b:Person)<-[r:RATED]-(a:Person) WHERE r.score >= 1 \
             RETURN r.id AS id ORDER BY r.score, a.id, r.id",
            &none,
        ),
        (
            "MATCH (a:Person:Tagged)-[r:RATED]->(b) WHERE r.score <= 6.5 \
             RETURN r.id AS id ORDER BY r.score DESC, r.id",
            &none,
        ),
        // A residual whose only bound is at the far end of the walk: the
        // index still bounds the window, so the walk is planned.
        (
            "MATCH (a:Person)-[r:RATED]->(b:Person) WHERE r.score <= 3 AND r.kind <> 'drop' \
             RETURN r.id AS id ORDER BY r.score, r.id",
            &none,
        ),
        // Empty.
        (
            "MATCH (a:Person)-[r:RATED]->(b:Person) WHERE r.score > 7 \
             RETURN r.id AS id ORDER BY r.score, r.id",
            &none,
        ),
    ];
    for page_size in [None, Some(1), Some(2), Some(5)] {
        let storage = fixture(page_size);
        for (query, parameters) in cases {
            for (skip, limit) in [(0, 1), (0, 3), (2, 4), (0, 7), (5, 100)] {
                assert_window_matches_generic(&storage, query, skip, limit, parameters).await;
            }
        }
    }
}

/// With only `ORDER BY r.p`, ties keep index order: `(source, target, id)`
/// ascending, which is also the generic plan's order for an ascending,
/// outgoing expansion from a scan.
#[tokio::test]
async fn a_single_key_ascending_window_keeps_index_order_in_ties() {
    for page_size in [None, Some(2)] {
        let storage = fixture(page_size);
        for (skip, limit) in [(0, 2), (3, 5), (0, 30)] {
            assert_window_matches_generic(
                &storage,
                "MATCH (a:Person)-[r:RATED]->(b:Person) WHERE r.score >= 0 \
                 RETURN r.id AS id ORDER BY r.score",
                skip,
                limit,
                &ParameterMap::new(),
            )
            .await;
        }
    }
}

/// A 5-row window over 400 relationships reads a few pages and hydrates a
/// handful of endpoints, never the fixture.
#[tokio::test]
async fn a_small_relationship_window_does_not_read_the_fixture() {
    let vertices = (1..=40).map(person).collect::<Vec<_>>();
    let relationships = (0..400_u64).map(|index| {
        RelationshipRecord::new(1_000 + index, index % 40 + 1, (index * 7) % 40 + 1, "RATED")
            .with_property("score", ScalarValue::Integer(i128::from(index % 97)))
    });
    let storage = MemoryStorage::with_graph(vertices, relationships);
    let engine = CypherEngine::new(storage.clone());
    let result = engine
        .execute(
            "MATCH (a:Person)-[r:RATED]->(b:Person) WHERE r.score >= 0 \
             RETURN r.id AS id ORDER BY r.score DESC, r.id LIMIT 5",
            &ParameterMap::new(),
        )
        .await
        .expect("windowed relationship query executes");
    assert_eq!(result.rows.len(), 5);
    let read = *storage.ordered_relationship_scan_rows.lock().unwrap();
    // Scores 96 down to 92 hold 4 relationships each: the window fills in
    // the second value and the walk stops at the end of its tie.
    assert!(read <= 12, "read {read} relationships");
    let hydrated = storage.hydration_requests.lock().unwrap().iter().map(Vec::len).sum::<usize>();
    assert!(hydrated <= 24, "hydrated {hydrated} vertices");
    assert!(storage.expand_requests.lock().unwrap().is_empty());
}

#[tokio::test]
async fn shapes_the_relationship_index_cannot_serve_keep_the_generic_plan() {
    let engine = CypherEngine::new(fixture(None));
    let parameters = ParameterMap::from([("score".to_string(), ScalarValue::Integer(3))]);
    for query in [
        // Undirected: each relationship would be two rows.
        "MATCH (a:Person)-[r:RATED]-(b:Person) WHERE r.score >= 0 RETURN r.id AS id ORDER BY r.score LIMIT 3",
        // Equality anchor: the relationship seek is narrower.
        "MATCH (a:Person)-[r:RATED]->(b:Person) WHERE r.score = $score RETURN r.id AS id ORDER BY r.score LIMIT 3",
        // Nothing proves the key non-null or gives the walk a start point.
        "MATCH (a:Person)-[r:RATED]->(b:Person) WHERE r.kind <> 'drop' RETURN r.id AS id ORDER BY r.score LIMIT 3",
        // An `Or` leaves every term residual, so the index bounds nothing and
        // the walk has nothing to stand on -- a full scan wearing an
        // operator's name. The same rule as the vertex walk.
        "MATCH (a:Person)-[r:RATED]->(b:Person) WHERE r.at STARTS WITH '2026-03' AND r.kind = 'keep' OR r.at STARTS WITH '2026-03-0' RETURN r.id AS id ORDER BY r.at DESC, r.id LIMIT 3",
        // No relationship window at all.
        "MATCH (a:Person)-[r:RATED]->(b:Person) WHERE r.score >= 0 RETURN r.id AS id ORDER BY r.score",
        // Leading key on an endpoint.
        "MATCH (a:Person)-[r:RATED]->(b:Person) WHERE r.score >= 0 RETURN r.id AS id ORDER BY a.id, r.score LIMIT 3",
        // Two hops: the walk would have to join a second expansion.
        "MATCH (a:Person)-[r:RATED]->(b:Person)-[s:RATED]->(c:Person) WHERE r.score >= 0 RETURN r.id AS id ORDER BY r.score LIMIT 3",
        "MATCH (a:Person)-[r:RATED]->(b:Person)-[s:RATED]->(c:Person) WHERE s.score >= 0 RETURN s.id AS id ORDER BY s.score LIMIT 3",
    ] {
        let prepared = engine
            .prepare(query, &parameters)
            .unwrap_or_else(|error| panic!("prepare {query}: {error}"));
        assert!(
            !prepared.explain().contains("OrderedRelationshipPropertyScan"),
            "{query}\n{}",
            prepared.explain()
        );
    }
}

/// A backend without an ordered relationship index fails the query instead
/// of silently widening it into a scan.
#[tokio::test]
async fn a_backend_without_the_index_fails_rather_than_widening() {
    use async_trait::async_trait;
    use hydradb_cypher_engine::{
        ExpandRequest, GraphRead, GraphStorage, OrderedPropertyScanPage, OrderedPropertyScanRequest, ReadRequest, StorageResult, VertexId,
    };

    struct NoRelationshipIndex(MemoryStorage);
    struct NoRelationshipIndexRead<'a>(Box<dyn GraphRead + 'a>);

    #[async_trait]
    impl GraphStorage for NoRelationshipIndex {
        async fn begin_read(&self, request: ReadRequest) -> StorageResult<Box<dyn GraphRead + '_>> {
            Ok(Box::new(NoRelationshipIndexRead(self.0.begin_read(request).await?)))
        }
    }

    // Everything but the defaulted ordered relationship scan is forwarded.
    #[async_trait]
    impl GraphRead for NoRelationshipIndexRead<'_> {
        fn raise_completion_condition(&mut self, status: hydradb_cypher_engine::GqlStatus) {
            self.0.raise_completion_condition(status);
        }

        fn take_completion_conditions(&mut self) -> Vec<hydradb_cypher_engine::GqlStatus> {
            self.0.take_completion_conditions()
        }

        async fn seek_vertices_by_property(&mut self, property: &str, value: &ScalarValue) -> StorageResult<Vec<VertexId>> {
            self.0.seek_vertices_by_property(property, value).await
        }
        async fn scan_vertices_by_property(&mut self, property: &str) -> StorageResult<Vec<VertexId>> {
            self.0.scan_vertices_by_property(property).await
        }
        async fn scan_vertices_by_property_ordered(
            &mut self,
            request: &OrderedPropertyScanRequest,
        ) -> StorageResult<OrderedPropertyScanPage> {
            self.0.scan_vertices_by_property_ordered(request).await
        }
        async fn scan_vertices_by_label(&mut self, label: &str, max_results: Option<usize>) -> StorageResult<Vec<VertexId>> {
            self.0.scan_vertices_by_label(label, max_results).await
        }
        async fn scan_all_vertices(&mut self) -> StorageResult<Vec<VertexId>> {
            self.0.scan_all_vertices().await
        }
        async fn seek_relationships_by_property(
            &mut self,
            relationship_type: &str,
            property: &str,
            value: &ScalarValue,
        ) -> StorageResult<Vec<RelationshipRecord>> {
            self.0.seek_relationships_by_property(relationship_type, property, value).await
        }
        async fn expand_relationships(&mut self, request: &ExpandRequest) -> StorageResult<Vec<RelationshipRecord>> {
            self.0.expand_relationships(request).await
        }
        async fn hydrate_vertices(&mut self, vertex_ids: &[VertexId]) -> StorageResult<Vec<VertexRecord>> {
            self.0.hydrate_vertices(vertex_ids).await
        }
    }

    let storage = fixture(None);
    let expands = std::sync::Arc::clone(&storage.expand_requests);
    let engine = CypherEngine::new(NoRelationshipIndex(storage));
    let error = engine
        .execute(
            "MATCH (a:Person)-[r:RATED]->(b:Person) WHERE r.score >= 0 \
             RETURN r.id AS id ORDER BY r.score LIMIT 3",
            &ParameterMap::new(),
        )
        .await
        .expect_err("no ordered relationship index");
    assert!(
        error
            .to_string()
            .contains("no ordered index over relationship property RATED.score"),
        "{error}"
    );
    assert!(expands.lock().unwrap().is_empty());
}
