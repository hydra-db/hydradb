//! A `GraphStorage` that profiles every operator through the executor's
//! profiling hooks, the way a production adapter does, over [`MemoryStorage`].
//!
//! Every `GraphRead` method is forwarded, including the defaulted reporting
//! hooks (`record_retained_rows`, `record_hydrated_vertices`), so the proof
//! counters in these tests are the same numbers `/metrics` would export.

use std::sync::{Arc, Mutex};
use std::time::Instant;

use async_trait::async_trait;
use hydradb_cypher_engine::{
    CypherEngine, ExecutionProfile, ExpandDirection, ExpandNeighbor, ExpandRequest, GraphPhysicalPlan, GraphRead, GraphStorage,
    OperatorProfile, OperatorProfiler, OrderedPropertyScanPage, OrderedPropertyScanRequest, OrderedRelationshipPropertyScanPage,
    OrderedRelationshipPropertyScanRequest, ParameterMap, QueryResult, ReadRequest, RelationshipRecord, ScalarValue,
    StatisticsAccessReport, StorageResult, VertexId, VertexRecord,
};

use super::MemoryStorage;

pub struct ProfilingStorage {
    pub inner: MemoryStorage,
    pub profiler: Arc<Mutex<Option<OperatorProfiler>>>,
}

impl ProfilingStorage {
    pub fn new(inner: MemoryStorage) -> Self {
        Self {
            inner,
            profiler: Arc::new(Mutex::new(None)),
        }
    }
}

pub struct ProfilingRead<'a> {
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
    fn checkpoint(&self, operation: &'static str) -> StorageResult<()> {
        self.inner.checkpoint(operation)
    }

    fn check_intermediate_rows(&self, operation: &'static str, rows: usize) -> StorageResult<()> {
        self.with_profiler(|profiler| profiler.record_retained_rows(rows));
        self.inner.check_intermediate_rows(operation, rows)
    }

    fn max_intermediate_rows(&self) -> Option<usize> {
        self.inner.max_intermediate_rows()
    }

    fn raise_completion_condition(&mut self, status: hydradb_cypher_ast::GqlStatus) {
        self.inner.raise_completion_condition(status);
    }

    fn take_completion_conditions(&mut self) -> Vec<hydradb_cypher_ast::GqlStatus> {
        self.inner.take_completion_conditions()
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

    fn record_retained_rows(&self, rows: usize) {
        self.with_profiler(|profiler| profiler.record_retained_rows(rows));
    }

    fn record_statistics_access(&self, report: &StatisticsAccessReport) {
        self.inner.record_statistics_access(report);
    }

    fn record_hydrated_vertices(&self, vertices: usize) {
        self.with_profiler(|profiler| profiler.record_hydrated_vertices(vertices));
    }

    async fn seek_vertices_by_property(&mut self, property: &str, value: &ScalarValue) -> StorageResult<Vec<VertexId>> {
        let started = Instant::now();
        let result = self.inner.seek_vertices_by_property(property, value).await;
        self.storage_call(started);
        result
    }

    async fn seek_vertices_by_property_values(&mut self, property: &str, values: &[ScalarValue]) -> StorageResult<Vec<VertexId>> {
        let started = Instant::now();
        let result = self.inner.seek_vertices_by_property_values(property, values).await;
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
        if let Ok(page) = &result {
            self.with_profiler(|profiler| profiler.record_hydrated_vertices(page.vertices.len()));
        }
        result
    }

    async fn scan_relationships_by_property_ordered(
        &mut self,
        request: &OrderedRelationshipPropertyScanRequest,
    ) -> StorageResult<OrderedRelationshipPropertyScanPage> {
        let started = Instant::now();
        let result = self.inner.scan_relationships_by_property_ordered(request).await;
        self.storage_call(started);
        if let Ok(page) = &result {
            self.with_profiler(|profiler| profiler.record_scanned_relationships(page.relationships.len()));
        }
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
        if let Ok(relationships) = &result {
            self.with_profiler(|profiler| {
                profiler.record_scanned_relationships(relationships.len());
            });
        }
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

    async fn probe_relationships_by_type(
        &mut self,
        relationship_type: &str,
        max_index_entries: usize,
    ) -> StorageResult<Option<Vec<RelationshipRecord>>> {
        let started = Instant::now();
        let result = self.inner.probe_relationships_by_type(relationship_type, max_index_entries).await;
        self.storage_call(started);
        result
    }

    async fn probe_relationships_by_property_lower_bound(
        &mut self,
        relationship_type: &str,
        property: &str,
        lower: &ScalarValue,
        inclusive: bool,
        max_index_entries: usize,
    ) -> StorageResult<Option<Vec<RelationshipRecord>>> {
        let started = Instant::now();
        let result = self
            .inner
            .probe_relationships_by_property_lower_bound(relationship_type, property, lower, inclusive, max_index_entries)
            .await;
        self.storage_call(started);
        result
    }

    async fn expand_neighbors(&mut self, request: &ExpandRequest) -> StorageResult<Option<Vec<ExpandNeighbor>>> {
        let started = Instant::now();
        let result = self.inner.expand_neighbors(request).await;
        self.storage_call(started);
        result
    }

    async fn expand_relationships_between(
        &mut self,
        direction: ExpandDirection,
        neighbors: &[ExpandNeighbor],
    ) -> StorageResult<Vec<RelationshipRecord>> {
        let started = Instant::now();
        let result = self.inner.expand_relationships_between(direction, neighbors).await;
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

/// Prepare `query`, execute it under a fresh profiler, and return the result
/// with the finished profile and the plan it describes.
pub async fn profile_query(
    storage: MemoryStorage,
    query: &str,
    parameters: &ParameterMap,
) -> (QueryResult, ExecutionProfile, GraphPhysicalPlan) {
    let storage = ProfilingStorage::new(storage);
    let profiler = Arc::clone(&storage.profiler);
    let engine = CypherEngine::new(storage);
    let prepared = engine
        .prepare(query, parameters)
        .unwrap_or_else(|error| panic!("prepare {query}: {error}"));
    *profiler.lock().unwrap() = Some(OperatorProfiler::new(&prepared.physical));
    let result = engine
        .execute_prepared(&prepared)
        .await
        .unwrap_or_else(|error| panic!("execute {query}: {error}"));
    let profile = profiler.lock().unwrap().take().unwrap().finish();
    (result, profile, prepared.physical)
}

/// The single operator of kind `operator` in `profile`.
pub fn operator<'a>(profile: &'a ExecutionProfile, operator: &str) -> &'a OperatorProfile {
    let mut matches = profile.operators.iter().filter(|profile| profile.operator == operator);
    let found = matches.next().unwrap_or_else(|| panic!("no {operator} in {profile:#?}"));
    assert!(matches.next().is_none(), "more than one {operator}");
    found
}
