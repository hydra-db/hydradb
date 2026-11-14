//! Every legacy combination of MATCH, OPTIONAL MATCH and UNION.
//!
//! Comma-separated patterns, sequential MATCH,
//! OPTIONAL MATCH, UNION and UNION ALL, crossed with projection, aggregation,
//! ordering, DISTINCT and windows. Disconnected patterns and correlated
//! bindings are here too, because those are what the combinations are made of.

mod common;

use common::{entity, MemoryStorage};
use hydradb_cypher_engine::{CypherEngine, ParameterMap, QueryResult, QueryValue, RelationshipRecord, ScalarValue};

/// Two entities, two people, and a two-hop chain that starts at one of them.
///
/// `beta` deliberately has no outgoing KNOWS: it is the row that makes an
/// OPTIONAL MATCH miss, which is the case several tests below turn on.
fn engine() -> CypherEngine<MemoryStorage> {
    CypherEngine::new(MemoryStorage::with_graph(
        [
            entity(1, "Entity", "alpha"),
            entity(2, "Entity", "beta"),
            entity(10, "Person", "bea"),
            entity(11, "Person", "bo"),
        ],
        [
            RelationshipRecord::new(90, 1, 10, "KNOWS"),
            RelationshipRecord::new(91, 10, 11, "KNOWS"),
        ],
    ))
}

async fn run(query: &str) -> QueryResult {
    engine()
        .execute(query, &ParameterMap::new())
        .await
        .unwrap_or_else(|error| panic!("{query:?} failed: {error}"))
}

fn rows(result: &QueryResult) -> Vec<Vec<String>> {
    result
        .rows
        .iter()
        .map(|row| {
            row.values
                .iter()
                .map(|value| match value {
                    QueryValue::Scalar(ScalarValue::String(value)) => value.to_string(),
                    QueryValue::Scalar(ScalarValue::Integer(value)) => value.to_string(),
                    QueryValue::Count(value) => value.to_string(),
                    QueryValue::Null => "null".to_string(),
                    other => panic!("unexpected value {other:?}"),
                })
                .collect()
        })
        .collect()
}

fn column(result: &QueryResult) -> Vec<String> {
    rows(result).into_iter().map(|row| row[0].clone()).collect()
}

#[tokio::test]
async fn comma_separated_patterns_are_a_cartesian_product() {
    let result = run("MATCH (a:Entity), (p:Person) RETURN a.entity_id AS a, p.entity_id AS p \
         ORDER BY a ASC, p ASC")
    .await;
    assert_eq!(
        rows(&result),
        vec![
            vec!["alpha".to_string(), "bea".to_string()],
            vec!["alpha".to_string(), "bo".to_string()],
            vec!["beta".to_string(), "bea".to_string()],
            vec!["beta".to_string(), "bo".to_string()],
        ],
        "two disconnected patterns pair every row with every row"
    );
}

#[tokio::test]
async fn a_sequential_match_correlates_with_the_binding_before_it() {
    let result = run("MATCH (a:Entity) MATCH (a)-[:KNOWS]->(p:Person) RETURN p.entity_id AS id ORDER BY id ASC").await;
    assert_eq!(column(&result), ["bea"], "only alpha has an outgoing KNOWS");
}

#[tokio::test]
async fn an_optional_match_that_misses_keeps_its_row_and_nulls_the_rest() {
    let result = run("MATCH (a:Entity) OPTIONAL MATCH (a)-[:KNOWS]->(p:Person) \
         RETURN a.entity_id AS a, p.entity_id AS p ORDER BY a ASC")
    .await;
    assert_eq!(
        rows(&result),
        vec![
            vec!["alpha".to_string(), "bea".to_string()],
            vec!["beta".to_string(), "null".to_string()],
        ]
    );
}

/// Expanding from a binding an OPTIONAL MATCH never bound yields no rows.
///
/// This used to fail the whole query with "unbound vertex variable p", which
/// describes the plan rather than the data: the optional miss pushes its row
/// onward with `p` unbound, and the next expansion met a row it had no way to
/// read. Cypher propagates the null instead - that row simply produces nothing
/// - and the rows that did bind `p` are unaffected.
#[tokio::test]
async fn an_expansion_from_an_optional_miss_yields_no_rows_rather_than_failing() {
    let result = run("MATCH (a:Entity) OPTIONAL MATCH (a)-[:KNOWS]->(p:Person) \
         MATCH (p)-[:KNOWS]->(q:Person) RETURN q.entity_id AS id ORDER BY id ASC")
    .await;
    assert_eq!(
        column(&result),
        ["bo"],
        "alpha's chain continues; beta's miss drops out instead of erroring"
    );
}

#[tokio::test]
async fn union_all_keeps_duplicates_and_union_removes_them() {
    let all = run("MATCH (a:Entity) RETURN a.entity_id AS id \
         UNION ALL MATCH (b:Entity) RETURN b.entity_id AS id")
    .await;
    let mut ids = column(&all);
    ids.sort();
    assert_eq!(ids, ["alpha", "alpha", "beta", "beta"]);

    let distinct = run("MATCH (a:Entity) RETURN a.entity_id AS id \
         UNION MATCH (b:Entity) RETURN b.entity_id AS id")
    .await;
    let mut ids = column(&distinct);
    ids.sort();
    assert_eq!(ids, ["alpha", "beta"]);
}

#[tokio::test]
async fn union_arms_may_project_different_patterns_under_one_column_name() {
    let result = run("MATCH (a:Entity) RETURN a.entity_id AS id \
         UNION MATCH (p:Person) RETURN p.entity_id AS id")
    .await;
    let mut ids = column(&result);
    ids.sort();
    assert_eq!(ids, ["alpha", "bea", "beta", "bo"]);
}

#[tokio::test]
async fn aggregation_composes_with_ordering_and_a_window() {
    let counted = run("MATCH (a:Entity) RETURN count(a) AS n").await;
    assert_eq!(column(&counted), ["2"]);

    let windowed = run("MATCH (a:Entity) RETURN a.entity_id AS id ORDER BY id DESC SKIP 1 LIMIT 1").await;
    assert_eq!(column(&windowed), ["alpha"]);
}

#[tokio::test]
async fn distinct_composes_with_a_disconnected_pattern() {
    // Every pairing repeats each entity once per person, so DISTINCT on the
    // entity alone collapses the duplication the join introduced.
    let result = run("MATCH (a:Entity), (p:Person) RETURN DISTINCT a.entity_id AS id ORDER BY id ASC").await;
    assert_eq!(column(&result), ["alpha", "beta"]);
}

#[tokio::test]
async fn an_optional_match_composes_with_aggregation() {
    let result = run("MATCH (a:Entity) OPTIONAL MATCH (a)-[:KNOWS]->(p:Person) RETURN count(p) AS n").await;
    assert_eq!(column(&result), ["1"], "count skips the null the miss produced");
}

/// A `UNION` runs its arms side by side on forked reads, and returns exactly
/// what running them one after another returns. The shape is a
/// chunk-candidate source read: one arm per chunk, each a point lookup and a
/// one-hop expansion.
mod concurrent_union_arms {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    use super::common::MemoryStorage;
    use async_trait::async_trait;
    use hydradb_cypher_engine::{
        CypherEngine, ExpandRequest, GraphRead, GraphStorage, OrderedPropertyScanPage, OrderedPropertyScanRequest, ParameterMap,
        QueryResult, QueryRow, QueryValue, ReadRequest, RelationshipRecord, ScalarValue, StorageResult, VertexId, VertexRecord,
    };

    /// A backend whose reads fork, and which records how many vertex seeks are
    /// in flight at once. Each seek yields before answering, as a storage round
    /// trip does, so arms that run side by side overlap there.
    struct ForkingStorage {
        memory: MemoryStorage,
        forks: bool,
        in_flight: Arc<AtomicUsize>,
        peak: Arc<AtomicUsize>,
        forked: Arc<Mutex<usize>>,
        /// A label whose scan raises a warning, as a read that reports a
        /// completion condition does.
        warns_on: Option<&'static str>,
    }

    impl ForkingStorage {
        fn new(memory: MemoryStorage, forks: bool) -> Self {
            Self {
                memory,
                forks,
                in_flight: Arc::default(),
                peak: Arc::default(),
                forked: Arc::default(),
                warns_on: None,
            }
        }

        fn warning_on_label(mut self, label: &'static str) -> Self {
            self.warns_on = Some(label);
            self
        }

        fn read(&self) -> ForkingRead<'_> {
            let inner = futures::executor::block_on(self.memory.begin_read(ReadRequest::default())).expect("memory read");
            ForkingRead { storage: self, inner }
        }
    }

    #[async_trait]
    impl GraphStorage for ForkingStorage {
        async fn begin_read(&self, _request: ReadRequest) -> StorageResult<Box<dyn GraphRead + '_>> {
            Ok(Box::new(self.read()))
        }
    }

    struct ForkingRead<'a> {
        storage: &'a ForkingStorage,
        inner: Box<dyn GraphRead + 'a>,
    }

    #[async_trait]
    impl GraphRead for ForkingRead<'_> {
        fn fork_read(&self) -> Option<Box<dyn GraphRead + '_>> {
            if !self.storage.forks {
                return None;
            }
            *self.storage.forked.lock().unwrap() += 1;
            Some(Box::new(self.storage.read()))
        }

        fn check_intermediate_rows(&self, operation: &'static str, rows: usize) -> StorageResult<()> {
            self.inner.check_intermediate_rows(operation, rows)
        }

        fn max_intermediate_rows(&self) -> Option<usize> {
            self.inner.max_intermediate_rows()
        }

        fn raise_completion_condition(&mut self, status: hydradb_cypher_engine::GqlStatus) {
            self.inner.raise_completion_condition(status);
        }

        fn take_completion_conditions(&mut self) -> Vec<hydradb_cypher_engine::GqlStatus> {
            self.inner.take_completion_conditions()
        }

        async fn seek_vertices_by_property(&mut self, property: &str, value: &ScalarValue) -> StorageResult<Vec<VertexId>> {
            let now = self.storage.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
            self.storage.peak.fetch_max(now, Ordering::SeqCst);
            for _ in 0..4 {
                tokio::task::yield_now().await;
            }
            let result = self.inner.seek_vertices_by_property(property, value).await;
            self.storage.in_flight.fetch_sub(1, Ordering::SeqCst);
            result
        }

        async fn scan_vertices_by_property(&mut self, property: &str) -> StorageResult<Vec<VertexId>> {
            self.inner.scan_vertices_by_property(property).await
        }

        async fn scan_vertices_by_property_ordered(
            &mut self,
            request: &OrderedPropertyScanRequest,
        ) -> StorageResult<OrderedPropertyScanPage> {
            self.inner.scan_vertices_by_property_ordered(request).await
        }

        async fn scan_vertices_by_label(&mut self, label: &str, max_results: Option<usize>) -> StorageResult<Vec<VertexId>> {
            if self.storage.warns_on == Some(label) {
                self.raise_completion_condition(hydradb_cypher_engine::GqlStatus::NULL_VALUE_ELIMINATED_IN_SET_FUNCTION);
            }
            self.inner.scan_vertices_by_label(label, max_results).await
        }

        async fn scan_all_vertices(&mut self) -> StorageResult<Vec<VertexId>> {
            self.inner.scan_all_vertices().await
        }

        async fn seek_relationships_by_property(
            &mut self,
            relationship_type: &str,
            property: &str,
            value: &ScalarValue,
        ) -> StorageResult<Vec<RelationshipRecord>> {
            self.inner.seek_relationships_by_property(relationship_type, property, value).await
        }

        async fn expand_relationships(&mut self, request: &ExpandRequest) -> StorageResult<Vec<RelationshipRecord>> {
            self.inner.expand_relationships(request).await
        }

        async fn hydrate_vertices(&mut self, vertex_ids: &[VertexId]) -> StorageResult<Vec<VertexRecord>> {
            self.inner.hydrate_vertices(vertex_ids).await
        }
    }

    const CHUNKS: u64 = 12;

    /// Chunk `n` belongs to source `n % 3`; chunk 0 has no source at all.
    fn graph() -> MemoryStorage {
        let mut vertices = Vec::new();
        let mut relationships = Vec::new();
        for source in 0..3_u64 {
            vertices.push(
                VertexRecord::new(1_000 + source)
                    .with_label("Source")
                    .with_property("source_id", ScalarValue::String(format!("src-{source}").into())),
            );
        }
        for chunk in 0..CHUNKS {
            vertices.push(
                VertexRecord::new(chunk)
                    .with_label("Chunk")
                    .with_property("chunk_id", ScalarValue::String(format!("chunk-{chunk}").into())),
            );
            if chunk != 0 {
                relationships.push(RelationshipRecord::new(10_000 + chunk, chunk, 1_000 + chunk % 3, "HAS_CHUNK"));
            }
        }
        MemoryStorage::with_graph(vertices, relationships)
    }

    /// One arm per chunk, as the application builds it.
    fn union_query(all: bool) -> (String, ParameterMap) {
        let union = if all { "\nUNION ALL\n" } else { "\nUNION\n" };
        let arms: Vec<String> = (0..CHUNKS)
            .map(|chunk| {
                format!(
                    "MATCH (c:Chunk {{chunk_id: $chunk_id_{chunk}}})-[:HAS_CHUNK]->(s:Source) \
                     RETURN DISTINCT s.source_id AS source_id"
                )
            })
            .collect();
        let mut parameters = ParameterMap::new();
        for chunk in 0..CHUNKS {
            parameters.insert(format!("chunk_id_{chunk}"), ScalarValue::String(format!("chunk-{chunk}").into()));
        }
        (arms.join(union), parameters)
    }

    async fn run(storage: ForkingStorage, all: bool) -> (QueryResult, usize, usize) {
        let peak = Arc::clone(&storage.peak);
        let forked = Arc::clone(&storage.forked);
        let (query, parameters) = union_query(all);
        let result = CypherEngine::new(storage)
            .execute(&query, &parameters)
            .await
            .expect("union executes");
        let forked = *forked.lock().unwrap();
        (result, peak.load(Ordering::SeqCst), forked)
    }

    #[tokio::test]
    async fn union_arms_overlap_on_forked_reads_and_return_the_sequential_rows() {
        for all in [true, false] {
            let (sequential, sequential_peak, _) = run(ForkingStorage::new(graph(), false), all).await;
            let (concurrent, concurrent_peak, forked) = run(ForkingStorage::new(graph(), true), all).await;

            assert_eq!(sequential_peak, 1, "a backend that cannot fork runs the arms in turn");
            assert_eq!(forked, CHUNKS as usize, "one forked read per arm");
            assert!(
                concurrent_peak > 1,
                "UNION{} arms never overlapped: peak {concurrent_peak} seeks in flight",
                if all { " ALL" } else { "" }
            );
            assert_eq!(concurrent.columns, sequential.columns);
            assert_eq!(concurrent.rows, sequential.rows, "UNION{} rows", if all { " ALL" } else { "" });
        }
    }

    #[tokio::test]
    async fn concurrent_union_arms_report_the_row_limit_with_arms_still_after_it() {
        // A UNION ALL below a projection (the binding-level union, which
        // lowering does not produce yet, so built by hand as in
        // nested_scope.rs): five arms of two rows against a limit of four.
        // The third arm crosses it with two arms still to come; the query
        // must fail with the limit, not run short of arms.
        use hydradb_cypher_ast::{CypherDialect, CypherVersion, Document, QueryConjunction};
        use hydradb_cypher_engine::{
            GraphLogicalPlan, GraphPhysicalPlan, PhysicalExpression, PhysicalProjection, PreparedQuery, SlateDbDataSource, Symbol,
            UnknownStatistics, VertexScanRequest,
        };

        let scan = || GraphPhysicalPlan::VertexScan {
            scan: SlateDbDataSource::new(&UnknownStatistics).prepare_vertex_scan(VertexScanRequest::new(Symbol::from("e"), Vec::new())),
        };
        let plan = GraphPhysicalPlan::Project {
            context: Default::default(),
            operation: hydradb_cypher_ast::operation::RETURN,
            input: Box::new(GraphPhysicalPlan::Union {
                arms: (0..5).map(|_| scan()).collect(),
                all: true,
                conjunction: QueryConjunction::Union,
            }),
            items: vec![PhysicalProjection {
                expression: PhysicalExpression::Property {
                    binding: Symbol::from("e"),
                    property: Symbol::from("entity_id"),
                },
                alias: Some(Symbol::from("id")),
            }],
            distinct: false,
            post_sort: Vec::new(),
            post_skip: None,
            post_limit: None,
        };
        let prepared = PreparedQuery {
            ast: Arc::new(Document::new(CypherDialect::Neo4j(CypherVersion::V25), Vec::new())),
            logical: Arc::new(GraphLogicalPlan::NodeScan {
                binding: Symbol::from("e"),
                labels: Vec::new(),
            }),
            physical: plan,
        };
        let mut memory = MemoryStorage::with_vertices([
            super::common::entity(1, "Entity", "alpha"),
            super::common::entity(2, "Entity", "beta"),
        ]);
        memory.max_intermediate_rows = Some(4);
        let storage = ForkingStorage::new(memory, true);
        let forked = Arc::clone(&storage.forked);
        let error = CypherEngine::new(storage)
            .execute_prepared(&prepared)
            .await
            .expect_err("ten rows exceed a limit of four");
        assert!(error.to_string().contains("cypher_union_intermediate_rows"), "{error}");
        assert_eq!(*forked.lock().unwrap(), 5, "the arms ran on forked reads");
    }

    #[tokio::test]
    async fn concurrent_union_arms_keep_the_warnings_of_marks_read_after_them() {
        // A UNION ALL below a projection (built by hand, as above) whose arms
        // each mark their rows with EXISTS. The subquery's warning belongs to
        // the mark until the projection reads it, which happens after the arm
        // has returned; it must still reach the caller, as it does when the
        // arms run in turn.
        use hydradb_cypher_ast::{CypherDialect, CypherVersion, Document, QueryConjunction};
        use hydradb_cypher_engine::{
            GraphLogicalPlan, GraphPhysicalPlan, PhysicalExpression, PhysicalProjection, PreparedQuery, SlateDbDataSource, Symbol,
            UnknownStatistics, VertexScanRequest,
        };

        let scan = |binding: &str, labels: Vec<Symbol>| GraphPhysicalPlan::VertexScan {
            scan: SlateDbDataSource::new(&UnknownStatistics).prepare_vertex_scan(VertexScanRequest::new(Symbol::from(binding), labels)),
        };
        let marked = || GraphPhysicalPlan::Exists {
            input: Box::new(scan("e", Vec::new())),
            subquery: Box::new(scan("w", vec![Symbol::from("Warned")])),
            binding: Symbol::from("m"),
        };
        let prepared = PreparedQuery {
            ast: Arc::new(Document::new(CypherDialect::Neo4j(CypherVersion::V25), Vec::new())),
            logical: Arc::new(GraphLogicalPlan::NodeScan {
                binding: Symbol::from("e"),
                labels: Vec::new(),
            }),
            physical: GraphPhysicalPlan::Project {
                context: Default::default(),
                operation: hydradb_cypher_ast::operation::RETURN,
                input: Box::new(GraphPhysicalPlan::Union {
                    arms: (0..3).map(|_| marked()).collect(),
                    all: true,
                    conjunction: QueryConjunction::Union,
                }),
                items: vec![PhysicalProjection {
                    expression: PhysicalExpression::Binding(Symbol::from("m")),
                    alias: Some(Symbol::from("m")),
                }],
                distinct: false,
                post_sort: Vec::new(),
                post_skip: None,
                post_limit: None,
            },
        };
        let outcome = |forks: bool| {
            let storage = ForkingStorage::new(
                MemoryStorage::with_vertices([
                    super::common::entity(1, "Entity", "alpha"),
                    super::common::entity(2, "Entity", "beta"),
                ]),
                forks,
            )
            .warning_on_label("Warned");
            let forked = Arc::clone(&storage.forked);
            let prepared = &prepared;
            async move {
                let outcome = CypherEngine::new(storage)
                    .execute_prepared_with_status(prepared)
                    .await
                    .expect("marked union executes");
                (outcome, *forked.lock().unwrap())
            }
        };
        let (sequential, _) = outcome(false).await;
        let (concurrent, forked) = outcome(true).await;
        assert_eq!(forked, 3, "the arms ran on forked reads");
        assert!(
            format!("{:?}", sequential.status).contains("01G11"),
            "run in turn, the mark's warning is reported: {:?}",
            sequential.status
        );
        assert_eq!(concurrent.result, sequential.result);
        assert_eq!(
            (&concurrent.status, &concurrent.additional),
            (&sequential.status, &sequential.additional),
            "the warning of a mark read after its arm returned was lost"
        );
    }

    #[tokio::test]
    async fn union_all_keeps_every_arms_rows_in_arm_order() {
        let (result, _, _) = run(ForkingStorage::new(graph(), true), true).await;
        // Chunk 0 has no source; chunks 1..12 each contribute one row, in order.
        let expected: Vec<QueryRow> = (1..CHUNKS)
            .map(|chunk| QueryRow {
                values: vec![QueryValue::Scalar(ScalarValue::String(format!("src-{}", chunk % 3).into()))],
            })
            .collect();
        assert_eq!(result.rows, expected);
    }
}
