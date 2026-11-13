//! Per-operator profiling through the executor's dispatch hooks.
//!
//! The backend here forwards every `GraphRead` call to the shared in-memory
//! harness and charges an `OperatorProfiler`, which is exactly what a
//! production adapter does. What is under test is the executor's half — that
//! every operator invocation is bracketed, nested correctly and attributed its
//! own work — and the profiler's arithmetic.

mod common;

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use async_trait::async_trait;
use common::{entity, MemoryStorage};
use hydradb_cypher_engine::{
    CypherEngine, ExecutionProfile, ExpandRequest, GraphPhysicalPlan, GraphRead, GraphStorage, OperatorProfile, OperatorProfiler,
    OrderedPropertyScanPage, OrderedPropertyScanRequest, ParameterMap, ReadRequest, RelationshipRecord, ScalarValue, StatisticsProvider,
    StorageResult, Symbol, VertexId, VertexRecord,
};

struct ProfilingStorage {
    inner: MemoryStorage,
    profiler: Arc<Mutex<Option<OperatorProfiler>>>,
}

struct ProfilingRead<'a> {
    inner: Box<dyn GraphRead + 'a>,
    profiler: Arc<Mutex<Option<OperatorProfiler>>>,
}

impl ProfilingRead<'_> {
    fn with_profiler(&self, charge: impl FnOnce(&mut OperatorProfiler)) {
        if let Some(profiler) = self.profiler.lock().unwrap().as_mut() {
            charge(profiler);
        }
    }

    fn storage_call(&self, started: Instant) {
        self.with_profiler(|profiler| profiler.record_storage_call(started.elapsed(), 1, 0));
    }
}

#[async_trait]
impl GraphStorage for ProfilingStorage {
    async fn begin_read(&self, request: ReadRequest) -> StorageResult<Box<dyn GraphRead + '_>> {
        Ok(Box::new(ProfilingRead {
            inner: self.inner.begin_read(request).await?,
            profiler: Arc::clone(&self.profiler),
        }))
    }
}

#[async_trait]
impl GraphRead for ProfilingRead<'_> {
    fn raise_completion_condition(&mut self, status: hydradb_cypher_engine::GqlStatus) {
        self.inner.raise_completion_condition(status);
    }

    fn take_completion_conditions(&mut self) -> Vec<hydradb_cypher_engine::GqlStatus> {
        self.inner.take_completion_conditions()
    }

    fn check_intermediate_rows(&self, operation: &'static str, rows: usize) -> StorageResult<()> {
        self.with_profiler(|profiler| profiler.record_retained_rows(rows));
        self.inner.check_intermediate_rows(operation, rows)
    }

    fn record_full_scan(&self) {
        self.with_profiler(OperatorProfiler::record_full_scan);
    }

    fn operator_started(&mut self, plan: &GraphPhysicalPlan) {
        self.with_profiler(|profiler| profiler.operator_started(plan));
    }

    fn operator_finished(&mut self, plan: &GraphPhysicalPlan, rows_out: usize) {
        self.with_profiler(|profiler| profiler.operator_finished(plan, rows_out));
    }

    async fn seek_vertices_by_property(&mut self, property: &str, value: &ScalarValue) -> StorageResult<Vec<VertexId>> {
        let started = Instant::now();
        let result = self.inner.seek_vertices_by_property(property, value).await;
        self.storage_call(started);
        result
    }

    async fn scan_vertices_by_property(&mut self, property: &str) -> StorageResult<Vec<VertexId>> {
        let started = Instant::now();
        let result = self.inner.scan_vertices_by_property(property).await;
        self.storage_call(started);
        result
    }

    async fn scan_vertices_by_property_ordered(&mut self, request: &OrderedPropertyScanRequest) -> StorageResult<OrderedPropertyScanPage> {
        let started = Instant::now();
        let result = self.inner.scan_vertices_by_property_ordered(request).await;
        self.storage_call(started);
        result
    }

    async fn scan_vertices_by_label(&mut self, label: &str, max_results: Option<usize>) -> StorageResult<Vec<VertexId>> {
        let started = Instant::now();
        let result = self.inner.scan_vertices_by_label(label, max_results).await;
        self.storage_call(started);
        result
    }

    async fn scan_all_vertices(&mut self) -> StorageResult<Vec<VertexId>> {
        let started = Instant::now();
        let result = self.inner.scan_all_vertices().await;
        self.storage_call(started);
        result
    }

    async fn seek_relationships_by_property(
        &mut self,
        relationship_type: &str,
        property: &str,
        value: &ScalarValue,
    ) -> StorageResult<Vec<RelationshipRecord>> {
        let started = Instant::now();
        let result = self.inner.seek_relationships_by_property(relationship_type, property, value).await;
        self.storage_call(started);
        result
    }

    async fn expand_relationships(&mut self, request: &ExpandRequest) -> StorageResult<Vec<RelationshipRecord>> {
        let started = Instant::now();
        let result = self.inner.expand_relationships(request).await;
        self.storage_call(started);
        if let Ok(relationships) = &result {
            self.with_profiler(|profiler| {
                profiler.record_scanned_relationships(relationships.len());
            });
        }
        result
    }

    async fn hydrate_vertices(&mut self, vertex_ids: &[VertexId]) -> StorageResult<Vec<VertexRecord>> {
        let started = Instant::now();
        let result = self.inner.hydrate_vertices(vertex_ids).await;
        self.storage_call(started);
        if let Ok(vertices) = &result {
            self.with_profiler(|profiler| profiler.record_hydrated_vertices(vertices.len()));
        }
        result
    }
}

fn graph() -> MemoryStorage {
    let people = (1..=6).map(|id| entity(id, "Person", &format!("p{id}")));
    let knows = [(1, 2), (1, 3), (2, 3), (4, 5)]
        .into_iter()
        .enumerate()
        .map(|(index, (source, target))| RelationshipRecord {
            id: 100 + index as u64,
            source,
            target,
            relationship_type: "KNOWS".to_string(),
            properties: BTreeMap::new(),
        });
    MemoryStorage::with_graph(people, knows)
}

async fn profile(query: &str) -> (ExecutionProfile, usize, GraphPhysicalPlan) {
    profile_with_statistics(query, &hydradb_cypher_engine::UnknownStatistics).await
}

async fn profile_with_statistics(query: &str, statistics: &dyn StatisticsProvider) -> (ExecutionProfile, usize, GraphPhysicalPlan) {
    let profiler = Arc::new(Mutex::new(None));
    let engine = CypherEngine::new(ProfilingStorage {
        inner: graph(),
        profiler: Arc::clone(&profiler),
    });
    let prepared = engine
        .prepare_with_statistics(query, &ParameterMap::new(), statistics)
        .expect("prepares");
    *profiler.lock().unwrap() = Some(OperatorProfiler::new(&prepared.physical));
    let result = engine.execute_prepared(&prepared).await.expect("executes");
    let profile = profiler.lock().unwrap().take().unwrap().finish();
    (profile, result.rows.len(), prepared.physical)
}

fn by_name<'a>(profile: &'a ExecutionProfile, operator: &str) -> &'a OperatorProfile {
    profile
        .operators
        .iter()
        .find(|profile| profile.operator == operator)
        .unwrap_or_else(|| panic!("no {operator} in {profile:#?}"))
}

/// Every operator's input is exactly what its children produced, the root's
/// output is the result, and own time never exceeds inclusive time.
fn assert_consistent(profile: &ExecutionProfile, result_rows: usize) {
    let root = &profile.operators[0];
    assert_eq!(root.parent, None);
    assert_eq!(root.invocations, 1);
    assert_eq!(root.rows_out, result_rows as u64);
    for operator in &profile.operators {
        let children_out: u64 = profile
            .operators
            .iter()
            .filter(|child| child.parent == Some(operator.id))
            .map(|child| child.rows_out)
            .sum();
        assert_eq!(operator.rows_in, children_out, "{operator:#?}");
        assert!(operator.self_elapsed <= operator.elapsed);
        assert!(operator.storage_elapsed <= operator.self_elapsed);
        assert!(operator.peak_retained_rows >= operator.rows_out / operator.invocations.max(1));
    }
}

#[tokio::test]
async fn a_traversal_attributes_storage_hydration_and_edges_to_its_operators() {
    let (profile, rows, _) = profile("MATCH (a:Person)-[:KNOWS]->(b:Person) WHERE a.entity_id = 'p1' RETURN b.entity_id AS id").await;
    assert_eq!(rows, 2);
    assert_consistent(&profile, rows);

    let expand = by_name(&profile, "ExpandExec");
    assert_eq!(expand.invocations, 1);
    assert_eq!(expand.rows_out, 2);
    assert!(expand.scanned_relationships >= 2, "{expand:#?}");
    assert!(expand.storage_requests >= 1);
    // The anchor's own hydration is charged to the anchor, not to the expand
    // or the filter that consume its rows.
    let anchor = by_name(&profile, "VertexScan");
    assert_eq!(anchor.access, Some("property-point"));
    assert_eq!(anchor.rows_out, 1);
    assert!(anchor.hydrated_vertices >= 1, "{profile:#?}");
    assert_eq!(anchor.scanned_relationships, 0);
    let filter = by_name(&profile, "FilterExec");
    assert_eq!(filter.storage_requests, 0);
    assert_eq!(filter.parent, Some(expand.id));
}

#[tokio::test]
async fn a_correlated_input_is_counted_once_per_outer_row() {
    let (profile, rows, _) = profile("MATCH (a:Person) MATCH (a)-[:KNOWS]->(b) RETURN a.entity_id AS a, b.entity_id AS b").await;
    assert_eq!(rows, 4);
    assert_consistent(&profile, rows);
    let join = by_name(&profile, "NaturalJoinExec");
    let left = profile
        .operators
        .iter()
        .find(|operator| operator.parent == Some(join.id))
        .expect("join has a left input");
    let right = profile
        .operators
        .iter()
        .rfind(|operator| operator.parent == Some(join.id))
        .expect("join has a right input");
    assert_eq!(left.invocations, 1);
    assert_eq!(left.rows_out, 6);
    assert_eq!(right.invocations, 6, "one correlated run per outer row");
}

struct Overestimate;

impl StatisticsProvider for Overestimate {
    fn vertex_count(&self, labels: &[Symbol]) -> Option<u64> {
        (!labels.is_empty()).then_some(50_000)
    }
}

#[tokio::test]
async fn an_attached_estimate_names_the_operator_that_missed() {
    let (mut profile, rows, plan) = profile_with_statistics("MATCH (a:Person) RETURN a.entity_id AS id", &Overestimate).await;
    assert_consistent(&profile, rows);
    // Reporting must retain the estimate used to prepare the scan even if a
    // later statistics view no longer has that count.
    profile.attach_estimates(&plan, &hydradb_cypher_engine::UnknownStatistics);
    let scan = by_name(&profile, "VertexScan");
    assert_eq!(scan.access, Some("label-prefix"));
    assert_eq!(scan.estimated_rows, Some(50_000));
    assert_eq!(scan.rows_out, 6);
    let (worst, ratio) = profile.worst_estimate(1_000).expect("a miss above the floor");
    assert_eq!(worst.operator, "VertexScan");
    assert!(ratio > 8_000.0, "{ratio}");
    assert!(profile.worst_estimate(100_000).is_none());
    assert!(profile.hottest().is_some());
}

#[tokio::test]
async fn correlated_seed_reuse_is_not_reported_as_a_full_scan() {
    let (profile, rows, _) = profile("MATCH (a:Person {entity_id: 'p1'}) MATCH (a)-[:KNOWS]->(b:Person) RETURN b.entity_id AS id").await;
    assert_eq!(rows, 2);
    assert!(profile
        .operators
        .iter()
        .any(|operator| operator.access == Some("inventory") && operator.invocations > 0));
    assert!(!profile.ran_full_scan());
}

#[tokio::test]
async fn an_unbound_scan_is_reported_as_a_full_scan() {
    let (profile, rows, _) = profile("MATCH (a) RETURN a.entity_id AS id").await;
    assert!(rows > 0);
    assert!(profile.ran_full_scan());
}
