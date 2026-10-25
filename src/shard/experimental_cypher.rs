use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use async_trait::async_trait;
use futures::{stream, StreamExt as _, TryStreamExt as _};
use hydradb_cypher_engine::{
    CypherEngine, ExpandDirection, ExpandRequest, GraphRead, GraphStorage, OrderedPropertyScanPage,
    OrderedPropertyScanRequest, OrderedScanPosition, PropertyEqualityCandidate, QueryResult,
    QueryValue as EngineQueryValue, ReadRequest, RelationshipRecord as EngineRelationshipRecord,
    ScalarValue, StorageError, StorageResult, VertexRecord as EngineVertexRecord,
};
use tracing::Instrument;

#[cfg(test)]
use crate::query::experimental_cypher::lower_experimental_cypher;
use crate::query::experimental_cypher::{
    experimental_engine_error, experimental_query_error, is_experimental_explain,
    lower_experimental_cypher_timed, property_to_scalar,
};
use crate::QueryFailureReason;

use super::*;

mod observability;

mod statistics;

#[cfg(test)]
mod optional_match_predicate_tests;

#[cfg(test)]
mod union_distinct_tests;
use observability::{PlanIdentity, RequestObservation};

/// Upper bound on the estimated bytes the per-shard statistics memo retains.
pub(crate) const STATISTICS_MEMO_MAX_BYTES: usize = 8 * 1024 * 1024;

/// How many index seeks of one multi-value predicate run at once. Bounded so
/// a large `IN` list cannot fan out into unbounded concurrent object-store
/// reads; matches the statistics loader's read concurrency.
const MULTI_SEEK_CONCURRENCY: usize = 8;

/// Candidate allowance shared out across the equality candidates of a runtime
/// access-path probe's first pass. Matches the legacy row engine's unordered
/// probe, so a property that anchors a query well on one engine anchors it on
/// the other, and catches the common case: legacy's own post-probe fetch count
/// has a median of 40.
const EQUALITY_PROBE_ENTRIES: usize = 256;

/// Allowance for the second pass, run only when every candidate exhausted its
/// first-pass slice.
///
/// Declining at 256 is weak evidence. Split across two candidates that is 128
/// entries each, which legacy's fetch distribution clears about 71% of the
/// time — so a first-pass decline is as likely to mean the slice was too thin
/// as it is to mean every property is broad. One wider pass covers the p99 of
/// 955 and still leaves a bounded ceiling of `EQUALITY_PROBE_ENTRIES` plus
/// this, in index-key reads, against the tens of thousands of whole vertex
/// records the unprobed path hydrates.
///
/// Deliberately not `max_query_index_candidates`, which is what legacy
/// escalates to. The walks race and the first to finish wins, so completing
/// only means *selective* while the allowance stays tight: at legacy's 250,000
/// a 60,458-entry tenant index finishes too, and the probe could hand back the
/// broad property it exists to reject. Legacy can afford that because its
/// escalation is gated on a measured rejection rate; this one is gated only on
/// a decline, so it keeps the allowance meaningful instead.
const EQUALITY_PROBE_ENTRIES_WIDE: usize = 4_096;

pub(crate) async fn execute_experimental_cypher_rows(
    shard: &GraphShard,
    context: QueryContext,
    query: &str,
) -> Result<QueryResultSet> {
    // EXPLAIN prepares and renders but never executes the physical plan. Keep
    // it out of both the row-query lifecycle and operator-request counters so
    // an A/B benchmark has the same denominator on both routes.
    if is_experimental_explain(query) {
        let mut observation = RequestObservation::default();
        return execute_experimental_cypher_rows_inner(shard, context, query, &mut observation)
            .await;
    }

    shard
        .operation_metrics
        .query_rows_started
        .fetch_add(1, Ordering::Relaxed);
    let started = std::time::Instant::now();
    let span = observability::request_span();
    let mut observation = RequestObservation::default();
    let result = execute_experimental_cypher_rows_inner(shard, context, query, &mut observation)
        .instrument(span.clone())
        .await;
    let elapsed = started.elapsed();
    shard.operation_metrics.query_rows_latency.record(elapsed);
    observation.finish(&span, shard, elapsed, &result);
    match &result {
        Ok(result_set) => {
            shard
                .operation_metrics
                .query_rows_completed
                .fetch_add(1, Ordering::Relaxed);
            shard
                .operation_metrics
                .query_rows_returned
                .fetch_add(result_set.rows.len() as u64, Ordering::Relaxed);
        }
        Err(error) => shard.operation_metrics.record_query_rows_failure(error),
    }
    result
}

async fn execute_experimental_cypher_rows_inner(
    shard: &GraphShard,
    context: QueryContext,
    query: &str,
    stages: &mut RequestObservation,
) -> Result<QueryResultSet> {
    validate_component("cell_id", &context.cell_id)?;
    if context.read_epoch.is_some() && context.validated_read_epoch().is_none() {
        return Err(GraphError::UnsupportedQuery {
            reason: QueryFailureReason::InvalidRequest,
            dialect: "Cypher25",
            feature: "historical graph epochs are not storage snapshots; execute against a current SlateDB snapshot"
                .to_string(),
        });
    }

    let budget = QueryBudget::new(
        context.max_runtime_ms.or(shard.limits.max_query_runtime_ms),
        context.cancellation_token.clone(),
    )
    .with_max_result_bytes(context.max_result_bytes);
    budget.check("experimental_cypher_prepare")?;
    // Stage timers, recorded by `RequestObservation::finish` onto the
    // `query.experimental` span and the per-cell stage counters. The one debug
    // event at the end still makes a request decomposable from the node log
    // alone (`RUST_LOG=hydradb=debug`) without a collector.
    let stage_started = std::time::Instant::now();
    let (explain, logical, lowering) = {
        let _span = tracing::info_span!("query.experimental.lower").entered();
        lower_experimental_cypher_timed(query, &context.parameters, &context.list_parameters)?
    };
    stages.lowering = stage_started.elapsed();
    stages.lower_timings = lowering.timings;
    stages.lower_cache_hit = lowering.cache_hit;
    let stage_started = std::time::Instant::now();
    let snapshot = budget
        .read_only_io("experimental_cypher_snapshot", async {
            if context.uses_refreshed_reader() {
                shard.db.reader_snapshot().await
            } else {
                shard.db.snapshot().await
            }
        })
        .await?;
    let read_epoch = snapshot.seq();
    let storage_sequence = read_epoch;
    stages.snapshot = stage_started.elapsed();
    let stage_started = std::time::Instant::now();
    let (prepared, statistics) = GraphStore::scope_snapshot(Arc::clone(&snapshot), async {
        budget
            .read_only_io(
                "experimental_cypher_statistics_cell",
                shard.ensure_cell_readable(&context.cell_id, "experimental_cypher_statistics"),
            )
            .await?;
        let statistics = statistics::SnapshotStatistics::load(
            shard,
            &context.cell_id,
            read_epoch,
            &logical.logical,
            logical.planning_context(),
            &budget,
        )
        .instrument(tracing::info_span!(
            "query.experimental.statistics",
            hydradb.query.stats.requested_records = tracing::field::Empty,
            hydradb.query.stats.available_records = tracing::field::Empty,
            hydradb.query.stats.memo_hits = tracing::field::Empty,
        ))
        .await?;
        stages.statistics = stage_started.elapsed();
        let plan_started = std::time::Instant::now();
        budget.check("experimental_cypher_physical_plan")?;
        let prepared = {
            let _span = tracing::info_span!("query.experimental.plan").entered();
            logical
                .plan_with_statistics(&statistics)
                .map_err(experimental_engine_error)
        };
        stages.plan = plan_started.elapsed();
        prepared.map(|prepared| (prepared, statistics))
    })
    .await?;
    stages.plan_identity = Some(PlanIdentity::of(&prepared));
    stages.work.profile(&prepared.physical);
    if explain {
        return Ok(QueryResultSet::new(
            vec![QueryColumn::new("plan")],
            vec![QueryRow::new(vec![QueryValue::Property(
                VertexPropertyValue::String(prepared.explain()),
            )])],
        )
        .with_read_epoch(read_epoch)
        .with_storage_sequence(storage_sequence));
    }

    let storage = HydraDbCypherStorage {
        shard,
        cell_id: &context.cell_id,
        read_epoch,
        snapshot: Arc::clone(&snapshot),
        budget: budget.clone(),
    };
    let engine = CypherEngine::new(storage);
    let stage_started = std::time::Instant::now();
    let executed = stages
        .work
        .scope(GraphStore::scope_snapshot(
            snapshot,
            engine
                .execute_prepared(&prepared)
                .instrument(tracing::info_span!("query.experimental.execute")),
        ))
        .await;
    stages.execute = stage_started.elapsed();
    stages.collect_profile(&prepared.physical, &statistics);
    let mut result = executed.map_err(experimental_engine_error)?;
    let result_started = std::time::Instant::now();
    let rows_materialized = result.rows.len();
    let skip =
        usize::try_from(context.result_window.skip).map_err(|_| GraphError::AdmissionRejected {
            operation: "experimental_cypher_result_skip",
            actual: context.result_window.skip,
            limit: usize::MAX as u64,
        })?;
    result.rows = result.rows.into_iter().skip(skip).collect();
    if let Some(limit) = context.result_window.limit {
        ensure_limit(
            "experimental_cypher_result_limit",
            limit as u64,
            shard.limits.max_query_result_vertices as u64,
        )?;
        result.rows.truncate(limit);
    } else {
        ensure_limit(
            "experimental_cypher_result_rows",
            result.rows.len() as u64,
            shard.limits.max_query_result_vertices as u64,
        )?;
    }
    // Emitted only once the request's own window and the result-size limits
    // have been applied, so the row count agrees with what the client gets
    // and with the post-window `query_rows_returned` counter. A request that
    // fails those checks reports through the failure counters instead.
    stages.rows_materialized = rows_materialized;
    stages.rows_returned = result.rows.len();
    let converted = engine_result_to_hydradb(result, read_epoch, storage_sequence, &budget);
    stages.result = result_started.elapsed();
    let (parse, lower, bind) = stages.preparation_split();
    tracing::debug!(
        hydradb.query.stage.parse_us = parse.as_micros() as u64,
        hydradb.query.stage.lower_us = lower.as_micros() as u64,
        hydradb.query.stage.bind_us = bind.as_micros() as u64,
        hydradb.query.stage.lower_cache_hit = stages.lower_cache_hit,
        hydradb.query.stage.snapshot_us = stages.snapshot.as_micros() as u64,
        hydradb.query.stage.statistics_us = stages.statistics.as_micros() as u64,
        hydradb.query.stage.plan_us = stages.plan.as_micros() as u64,
        hydradb.query.stage.execute_us = stages.execute.as_micros() as u64,
        hydradb.query.rows_materialized = stages.rows_materialized,
        hydradb.query.rows_returned = stages.rows_returned,
        "experimental cypher stages"
    );
    converted
}

fn elapsed_us(started: std::time::Instant) -> u64 {
    u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX)
}

/// Run one storage operator under a span that records its wall time and
/// output size, and echo the same numbers as a trace event for log-only
/// environments. Operator spans are the unit Workstream 11 asks for; this is
/// the smallest version of it, timing only.
async fn traced_operator<T, F>(
    operator: &'static str,
    span: tracing::Span,
    future: F,
    output_len: impl FnOnce(&T) -> usize,
) -> T
where
    F: std::future::Future<Output = T>,
{
    let started = std::time::Instant::now();
    let storage_before = observability::storage_totals();
    let output = future.instrument(span.clone()).await;
    let elapsed = elapsed_us(started);
    let rows = output_len(&output);
    span.record("hydradb.query.operator.elapsed_us", elapsed);
    span.record("hydradb.query.operator.rows_out", rows);
    observability::record_storage_call(operator, elapsed, rows, storage_before);
    tracing::trace!(
        hydradb.query.operator = operator,
        hydradb.query.operator.elapsed_us = elapsed,
        hydradb.query.operator.rows_out = rows,
        "experimental operator"
    );
    output
}

fn operator_span(operator: &'static str) -> tracing::Span {
    tracing::info_span!(
        "query.experimental.operator",
        hydradb.query.operator = operator,
        hydradb.query.operator.elapsed_us = tracing::field::Empty,
        hydradb.query.operator.rows_out = tracing::field::Empty,
        // Filled by the ordered index walk only: how much of its time was
        // metadata hydration versus the index scan, and how many index
        // entries it touched to produce its window.
        hydradb.query.scan.index_entries = tracing::field::Empty,
        hydradb.query.scan.hydrate_us = tracing::field::Empty,
        // Filled by the equality probe only: which property won its ranking.
        // Declared here because `tracing` drops a `record` for a field a span
        // did not declare, and the shared walk records this one against
        // whatever span is current -- which on this route is this one, not the
        // `query.execute` span that declares it for the legacy engine.
        hydradb.query.runtime_property_index = tracing::field::Empty,
    )
}

struct HydraDbCypherStorage<'a> {
    shard: &'a GraphShard,
    cell_id: &'a str,
    read_epoch: StorageSequence,
    snapshot: Arc<GraphStorageSnapshot>,
    budget: QueryBudget,
}

#[async_trait]
impl GraphStorage for HydraDbCypherStorage<'_> {
    async fn begin_read(&self, _request: ReadRequest) -> StorageResult<Box<dyn GraphRead + '_>> {
        storage_result(self.budget.check("experimental_cypher_begin_read"))?;
        Ok(Box::new(HydraDbCypherRead {
            shard: self.shard,
            cell_id: self.cell_id,
            read_epoch: self.read_epoch,
            snapshot: Arc::clone(&self.snapshot),
            budget: self.budget.clone(),
            structural_relationship_ids: BTreeMap::new(),
            next_structural_relationship_id: u64::MAX,
            scanned_edges: 0,
        }))
    }
}

struct HydraDbCypherRead<'a> {
    shard: &'a GraphShard,
    cell_id: &'a str,
    read_epoch: StorageSequence,
    snapshot: Arc<GraphStorageSnapshot>,
    budget: QueryBudget,
    structural_relationship_ids: BTreeMap<(String, VertexId, VertexId), RelationshipId>,
    next_structural_relationship_id: RelationshipId,
    /// Edges visited by every expansion this read has served, not just the
    /// current one. An undirected hop calls `expand_relationships` once per
    /// storage orientation (`expand_rows` in the engine), so a tally local to
    /// the call would let a query spend `max_query_scan_edges` twice over.
    scanned_edges: u64,
}

#[async_trait]
impl GraphRead for HydraDbCypherRead<'_> {
    fn checkpoint(&self, operation: &'static str) -> StorageResult<()> {
        storage_result(self.budget.check(operation))
    }

    fn operator_started(&mut self, plan: &hydradb_cypher_engine::GraphPhysicalPlan) {
        observability::operator_started(plan);
    }

    fn operator_finished(&mut self, plan: &hydradb_cypher_engine::GraphPhysicalPlan, rows: usize) {
        observability::operator_finished(plan, rows);
    }

    fn record_retained_rows(&self, rows: usize) {
        observability::record_retained_rows(rows);
    }

    fn record_hydrated_vertices(&self, vertices: usize) {
        observability::record_hydrated_vertices(vertices);
    }

    fn check_intermediate_rows(&self, operation: &'static str, rows: usize) -> StorageResult<()> {
        observability::record_intermediate_rows(rows);
        storage_result(self.budget.check(operation))?;
        storage_result(ensure_limit(
            operation,
            rows as u64,
            self.shard.limits.max_query_intermediate_rows as u64,
        ))
    }

    async fn seek_vertices_by_property(
        &mut self,
        property: &str,
        value: &ScalarValue,
    ) -> StorageResult<Vec<VertexId>> {
        traced_operator(
            "seek_vertices_by_property",
            operator_span("seek_vertices_by_property"),
            async {
                let value = scalar_to_property(value)?;
                self.shard
                    .operation_metrics
                    .record_experimental_property_seek();
                storage_result(
                    GraphStore::scope_snapshot(
                        Arc::clone(&self.snapshot),
                        self.shard.scan_vertex_property_index_at(
                            self.cell_id,
                            property,
                            &value,
                            self.read_epoch,
                            &self.budget,
                        ),
                    )
                    .await,
                )
            },
            |result| result.as_ref().map_or(0, Vec::len),
        )
        .await
    }

    async fn seek_vertices_by_property_values(
        &mut self,
        property: &str,
        values: &[ScalarValue],
    ) -> StorageResult<Vec<VertexId>> {
        traced_operator(
            "seek_vertices_by_property_values",
            operator_span("seek_vertices_by_property_values"),
            async {
                let values = values
                    .iter()
                    .map(scalar_to_property)
                    .collect::<StorageResult<Vec<_>>>()?;
                // Same accounting as the single seek: one value is one
                // index request, so a two-value OR still adds two.
                for _ in &values {
                    self.shard
                        .operation_metrics
                        .record_experimental_property_seek();
                }
                let shard = self.shard;
                let cell_id = self.cell_id;
                let read_epoch = self.read_epoch;
                let budget = &self.budget;
                let seeks = stream::iter(values)
                    .map(|value| async move {
                        shard
                            .scan_vertex_property_index_at(
                                cell_id, property, &value, read_epoch, budget,
                            )
                            .await
                    })
                    .buffer_unordered(MULTI_SEEK_CONCURRENCY)
                    .try_collect::<Vec<Vec<VertexId>>>();
                let ids = storage_result(
                    GraphStore::scope_snapshot(Arc::clone(&self.snapshot), seeks).await,
                )?;
                Ok(ids.into_iter().flatten().collect())
            },
            |result| result.as_ref().map_or(0, Vec::len),
        )
        .await
    }

    async fn scan_vertices_by_property(&mut self, property: &str) -> StorageResult<Vec<VertexId>> {
        traced_operator(
            "scan_vertices_by_property",
            operator_span("scan_vertices_by_property"),
            async {
                self.shard
                    .operation_metrics
                    .record_experimental_property_seek();
                storage_result(
                    GraphStore::scope_snapshot(
                        Arc::clone(&self.snapshot),
                        self.shard.scan_vertex_property_values_at(
                            self.cell_id,
                            property,
                            self.read_epoch,
                            &self.budget,
                        ),
                    )
                    .await,
                )
            },
            |result| result.as_ref().map_or(0, Vec::len),
        )
        .await
    }

    async fn scan_vertices_by_property_ordered(
        &mut self,
        request: &OrderedPropertyScanRequest,
    ) -> StorageResult<OrderedPropertyScanPage> {
        traced_operator(
            "scan_vertices_by_property_ordered",
            operator_span("scan_vertices_by_property_ordered"),
            async {
                self.shard
                    .operation_metrics
                    .record_experimental_ordered_property_scan();
                let bound = |bound: &Option<hydradb_cypher_engine::PropertyBound>| {
                    bound.as_ref().and_then(|bound| match &bound.value {
                        ScalarValue::String(value) => Some((value.to_string(), bound.inclusive)),
                        _ => None,
                    })
                };
                let window = super::query::OrderedStringVertexWindow {
                    property: request.property.clone(),
                    labels: request.labels.clone(),
                    ascending: request.ascending,
                    id_ascending: request.id_ascending,
                    prefix: request.prefix.clone(),
                    lower: bound(&request.lower),
                    upper: bound(&request.upper),
                    required: request.required,
                    // The engine speaks property values; the index walk speaks
                    // its key encoding of them.
                    after: request.after.as_ref().map(|after| {
                        (
                            super::query::OrderedStringVertexWindow::encode(&after.value),
                            after.vertex_id,
                        )
                    }),
                    index_order: request.index_order,
                };
                let hydrated = storage_result(
                    GraphStore::scope_snapshot(
                        Arc::clone(&self.snapshot),
                        self.shard.scan_ordered_string_vertex_window_at(
                            self.cell_id,
                            &window,
                            self.read_epoch,
                            &self.budget,
                        ),
                    )
                    .await,
                )?;
                Ok(OrderedPropertyScanPage {
                    vertices: hydrated
                        .vertices
                        .into_iter()
                        .map(|(id, metadata)| EngineVertexRecord {
                            id,
                            labels: metadata.labels,
                            properties: metadata
                                .properties
                                .iter()
                                .map(|(name, value)| (name.clone(), property_to_scalar(value)))
                                .collect(),
                        })
                        .collect(),
                    last: hydrated
                        .last
                        .map(|(encoded, vertex_id)| {
                            crate::codec::decode_string_index_value(
                                "experimental_ordered_scan_position",
                                &encoded,
                            )
                            .map(|value| OrderedScanPosition { value, vertex_id })
                        })
                        .transpose()
                        .map_err(|error| StorageError::new(error.to_string()))?,
                    exhausted: hydrated.exhausted,
                })
            },
            |result: &StorageResult<OrderedPropertyScanPage>| {
                result.as_ref().map_or(0, |page| page.vertices.len())
            },
        )
        .await
    }

    async fn scan_vertices_by_label(
        &mut self,
        label: &str,
        max_results: Option<usize>,
    ) -> StorageResult<Vec<VertexId>> {
        traced_operator(
            "scan_vertices_by_label",
            operator_span("scan_vertices_by_label"),
            async {
                storage_result(
                    GraphStore::scope_snapshot(
                        Arc::clone(&self.snapshot),
                        self.shard.scan_vertex_label_index_at_limited(
                            self.cell_id,
                            label,
                            self.read_epoch,
                            &self.budget,
                            max_results,
                        ),
                    )
                    .await,
                )
            },
            |result| result.as_ref().map_or(0, Vec::len),
        )
        .await
    }

    async fn scan_all_vertices(&mut self) -> StorageResult<Vec<VertexId>> {
        traced_operator(
            "scan_all_vertices",
            operator_span("scan_all_vertices"),
            async {
                storage_result(
                    self.shard
                        .scan_vertex_ids_at(
                            self.snapshot.as_ref(),
                            self.cell_id,
                            self.read_epoch,
                            &self.budget,
                        )
                        .await,
                )
            },
            |result| result.as_ref().map_or(0, Vec::len),
        )
        .await
    }

    async fn seek_relationships_by_property(
        &mut self,
        relationship_type: &str,
        property: &str,
        value: &ScalarValue,
    ) -> StorageResult<Vec<EngineRelationshipRecord>> {
        traced_operator(
            "seek_relationships_by_property",
            operator_span("seek_relationships_by_property"),
            async {
                let value = scalar_to_property(value)?;
                let pairs = storage_result(
                    GraphStore::scope_snapshot(
                        Arc::clone(&self.snapshot),
                        self.shard.scan_edge_property_index_at(
                            self.cell_id,
                            relationship_type,
                            property,
                            &value,
                            self.read_epoch,
                            &self.budget,
                        ),
                    )
                    .await,
                )?;
                let mut pending = Vec::new();
                for (source, target) in pairs {
                    storage_result(
                        self.budget
                            .check("experimental_cypher_relationship_property_seek"),
                    )?;
                    let variants = storage_result(
                        GraphStore::scope_snapshot(
                            Arc::clone(&self.snapshot),
                            self.shard.native_path_relationships_at(
                                self.cell_id,
                                relationship_type,
                                source,
                                target,
                                self.read_epoch,
                                &self.budget,
                            ),
                        )
                        .await,
                    )?;
                    for (id, metadata) in variants {
                        let matches = metadata.properties.get(property).is_some_and(|stored| {
                            hydradb_cypher_engine::scalar_values_equal(
                                &property_to_scalar(stored),
                                &property_to_scalar(&value),
                            )
                        });
                        if matches {
                            pending.push((id, source, target, metadata));
                        }
                    }
                }

                let mut used_ids = pending
                    .iter()
                    .filter_map(|(id, ..)| *id)
                    .chain(self.structural_relationship_ids.values().copied())
                    .collect::<BTreeSet<_>>();
                let mut relationships = Vec::with_capacity(pending.len());
                for (id, source, target, metadata) in pending {
                    let id = match id {
                        Some(id) => id,
                        None => self.structural_relationship_id(
                            (relationship_type.to_string(), source, target),
                            &mut used_ids,
                        )?,
                    };
                    relationships.push(EngineRelationshipRecord {
                        id,
                        source,
                        target,
                        relationship_type: relationship_type.to_string(),
                        properties: metadata
                            .properties
                            .iter()
                            .map(|(name, value)| (name.clone(), property_to_scalar(value)))
                            .collect(),
                    });
                }
                Ok(relationships)
            },
            |result| result.as_ref().map_or(0, Vec::len),
        )
        .await
    }

    async fn expand_relationships(
        &mut self,
        request: &ExpandRequest,
    ) -> StorageResult<Vec<EngineRelationshipRecord>> {
        traced_operator(
            "expand_relationships",
            operator_span("expand_relationships"),
            async {
                if request.relationship_types.is_empty() {
                    return Err(StorageError::new(
                        "relationship expansion without an explicit type is not supported yet",
                    ));
                }
                self.shard
                    .operation_metrics
                    .record_experimental_relationship_expand();
                self.check_intermediate_rows(
                    "experimental_cypher_expand_inputs",
                    request.input_vertex_ids.len(),
                )?;
                let mut pending = Vec::new();
                for input in request.input_vertex_ids.iter().copied() {
                    for edge_type in &request.relationship_types {
                        storage_result(self.budget.check("experimental_cypher_expand"))?;
                        let neighbors = match request.direction {
                            ExpandDirection::Outgoing => storage_result(
                                self.shard
                                    .out_neighbors_in_storage_snapshot_for_query(
                                        self.snapshot.as_ref(),
                                        self.cell_id,
                                        edge_type,
                                        input,
                                        self.read_epoch,
                                        &self.budget,
                                    )
                                    .await,
                            )?,
                            ExpandDirection::Incoming => storage_result(
                                GraphStore::scope_snapshot(
                                    Arc::clone(&self.snapshot),
                                    self.shard.in_neighbors_at_for_query(
                                        self.cell_id,
                                        edge_type,
                                        input,
                                        self.read_epoch,
                                        &self.budget,
                                    ),
                                )
                                .await,
                            )?,
                        };
                        self.scanned_edges =
                            self.scanned_edges.saturating_add(neighbors.len() as u64);
                        storage_result(ensure_limit(
                            "experimental_cypher_expand_scanned_edges",
                            self.scanned_edges,
                            self.shard.limits.max_query_scan_edges,
                        ))?;
                        for neighbor in neighbors {
                            storage_result(
                                self.budget.check("experimental_cypher_expand_neighbor"),
                            )?;
                            let (source, target) = match request.direction {
                                ExpandDirection::Outgoing => (input, neighbor),
                                ExpandDirection::Incoming => (neighbor, input),
                            };
                            let relationships = storage_result(
                                GraphStore::scope_snapshot(
                                    Arc::clone(&self.snapshot),
                                    self.shard.native_path_relationships_at(
                                        self.cell_id,
                                        edge_type,
                                        source,
                                        target,
                                        self.read_epoch,
                                        &self.budget,
                                    ),
                                )
                                .await,
                            )?;
                            for (relationship_id, metadata) in relationships {
                                storage_result(
                                    self.budget.check("experimental_cypher_expand_relationship"),
                                )?;
                                pending.push((
                                    relationship_id,
                                    edge_type.clone(),
                                    source,
                                    target,
                                    metadata,
                                ));
                                self.check_intermediate_rows(
                                    "experimental_cypher_expand_relationships",
                                    pending.len(),
                                )?;
                            }
                        }
                    }
                }

                let mut used_ids = pending
                    .iter()
                    .filter_map(|(id, ..)| *id)
                    .chain(self.structural_relationship_ids.values().copied())
                    .collect::<BTreeSet<_>>();
                let mut relationships = Vec::with_capacity(pending.len());
                for (id, edge_type, source, target, metadata) in pending {
                    storage_result(self.budget.check("experimental_cypher_expand_materialize"))?;
                    let id = match id {
                        Some(id) => id,
                        None => self.structural_relationship_id(
                            (edge_type.clone(), source, target),
                            &mut used_ids,
                        )?,
                    };
                    let properties = metadata
                        .properties
                        .iter()
                        .map(|(name, value)| (name.clone(), property_to_scalar(value)))
                        .collect();
                    relationships.push(EngineRelationshipRecord {
                        id,
                        source,
                        target,
                        relationship_type: edge_type,
                        properties,
                    });
                    self.check_intermediate_rows(
                        "experimental_cypher_expand_materialized_rows",
                        relationships.len(),
                    )?;
                }
                Ok(relationships)
            },
            |result| result.as_ref().map_or(0, Vec::len),
        )
        .await
    }

    async fn probe_selective_equality(
        &mut self,
        candidates: &[PropertyEqualityCandidate],
    ) -> StorageResult<Option<Vec<VertexId>>> {
        traced_operator(
            "probe_selective_equality",
            operator_span("probe_selective_equality"),
            async {
                let mut encoded = Vec::with_capacity(candidates.len());
                for candidate in candidates {
                    let mut keys = Vec::new();
                    // Every value must have index keys or the candidate is
                    // unusable, not narrow. `scalar_property_keys` yields none
                    // for an integer outside the i64/u64 range or a float that
                    // does not parse, and such a value still compares equal to
                    // a stored float under `scalar_values_equal`. Walking the
                    // keys of its siblings would complete, look selective, and
                    // return a subset of the rows it matches.
                    let encodable = candidate.values.iter().all(|value| {
                        let value_keys = statistics::scalar_property_keys(value);
                        let encodable = !value_keys.is_empty();
                        keys.extend(value_keys);
                        encodable
                    });
                    if !encodable {
                        continue;
                    }
                    // The probe rejects a candidate that reaches its own
                    // alternatives cap, so truncating here cannot turn an
                    // incomplete probe into a complete one.
                    keys.truncate(super::query::QUERY_NODE_EQUALITY_PROBE_ALTERNATIVES);
                    encoded.push((candidate.property.clone(), keys));
                }
                let ceiling = self.shard.limits.max_query_intermediate_rows;
                let narrow = EQUALITY_PROBE_ENTRIES.min(ceiling);
                let wide = EQUALITY_PROBE_ENTRIES_WIDE.min(ceiling);
                let mut probed = self.probe_pass(&encoded, narrow).await?;
                // Every candidate exhausted its first-pass slice. That is as
                // likely to be a thin slice as a broad property, so buy one
                // wider pass before handing the planned path back — unless a
                // configured ceiling has already collapsed the two, in which
                // case the second pass would re-walk the same bound.
                if probed.is_none() && wide > narrow {
                    probed = self.probe_pass(&encoded, wide).await?;
                }
                Ok(probed.map(|(_, vertices)| vertices))
            },
            |result: &StorageResult<Option<Vec<VertexId>>>| {
                result
                    .as_ref()
                    .ok()
                    .and_then(|probed| probed.as_ref().map(Vec::len))
                    .unwrap_or(0)
            },
        )
        .await
    }

    async fn hydrate_vertices(
        &mut self,
        vertex_ids: &[VertexId],
    ) -> StorageResult<Vec<EngineVertexRecord>> {
        traced_operator(
            "hydrate_vertices",
            operator_span("hydrate_vertices"),
            async {
                let hydrated = storage_result(
                    GraphStore::scope_snapshot(
                        Arc::clone(&self.snapshot),
                        self.shard.vertex_metadata_batch_at(
                            self.cell_id,
                            vertex_ids,
                            self.read_epoch,
                            &self.budget,
                        ),
                    )
                    .await,
                )?;
                Ok(hydrated
                    .into_iter()
                    .map(|(id, metadata)| EngineVertexRecord {
                        id,
                        labels: metadata.labels,
                        properties: metadata
                            .properties
                            .iter()
                            .map(|(name, value)| (name.clone(), property_to_scalar(value)))
                            .collect(),
                    })
                    .collect())
            },
            |result| result.as_ref().map_or(0, Vec::len),
        )
        .await
    }
}

impl HydraDbCypherRead<'_> {
    /// One bounded ranking pass over already-encoded candidates.
    async fn probe_pass(
        &self,
        encoded: &[(String, Vec<String>)],
        max_probe_entries: usize,
    ) -> StorageResult<Option<(usize, Vec<VertexId>)>> {
        storage_result(
            GraphStore::scope_snapshot(
                Arc::clone(&self.snapshot),
                self.shard.probe_property_index_candidates(
                    self.cell_id,
                    encoded,
                    max_probe_entries,
                    &self.budget,
                ),
            )
            .await,
        )
    }

    fn structural_relationship_id(
        &mut self,
        identity: (String, VertexId, VertexId),
        used_ids: &mut BTreeSet<RelationshipId>,
    ) -> StorageResult<RelationshipId> {
        if let Some(id) = self.structural_relationship_ids.get(&identity) {
            return Ok(*id);
        }
        while used_ids.contains(&self.next_structural_relationship_id) {
            self.next_structural_relationship_id = self
                .next_structural_relationship_id
                .checked_sub(1)
                .ok_or_else(|| StorageError::new("relationship identity space is exhausted"))?;
        }
        let id = self.next_structural_relationship_id;
        used_ids.insert(id);
        self.structural_relationship_ids.insert(identity, id);
        self.next_structural_relationship_id =
            self.next_structural_relationship_id.saturating_sub(1);
        Ok(id)
    }
}

fn engine_result_to_hydradb(
    result: QueryResult,
    read_epoch: StorageSequence,
    storage_sequence: StorageSequence,
    budget: &QueryBudget,
) -> Result<QueryResultSet> {
    let columns = result
        .columns
        .into_iter()
        .map(|column| QueryColumn::new(column.name))
        .collect();
    let mut rows = Vec::with_capacity(result.rows.len());
    for row in result.rows {
        budget.check("experimental_cypher_result")?;
        let values = row
            .values
            .into_iter()
            .map(engine_value_to_hydradb)
            .collect::<Result<Vec<_>>>()?;
        let row = QueryRow::new(values);
        budget.account_result_row(&row)?;
        rows.push(row);
    }
    Ok(QueryResultSet::new(columns, rows)
        .with_read_epoch(read_epoch)
        .with_storage_sequence(storage_sequence))
}

fn engine_value_to_hydradb(value: EngineQueryValue) -> Result<QueryValue> {
    match value {
        EngineQueryValue::Null => Ok(QueryValue::Null),
        EngineQueryValue::VertexId(id) => Ok(QueryValue::VertexId(id)),
        EngineQueryValue::RelationshipId(id) => {
            Ok(QueryValue::Property(VertexPropertyValue::Integer(id)))
        }
        EngineQueryValue::Count(value) => Ok(QueryValue::Count(value)),
        EngineQueryValue::Scalar(value) => scalar_to_query_value(&value),
        EngineQueryValue::List(values) => values
            .into_iter()
            .map(engine_value_to_hydradb)
            .collect::<Result<Vec<_>>>()
            .map(QueryValue::List),
    }
}

fn scalar_to_query_value(value: &ScalarValue) -> Result<QueryValue> {
    match value {
        ScalarValue::Null => Ok(QueryValue::Null),
        ScalarValue::Boolean(value) => Ok(QueryValue::Property(VertexPropertyValue::Bool(*value))),
        ScalarValue::Integer(value) => scalar_integer_to_property(*value).map(QueryValue::Property),
        ScalarValue::Float(value) => {
            let value = value
                .parse::<f64>()
                .map_err(|error| experimental_query_error(QueryFailureReason::Evaluation, error))?;
            if !value.is_finite() {
                return Err(experimental_query_error(
                    QueryFailureReason::Evaluation,
                    "non-finite floating-point values are not supported",
                ));
            }
            Ok(QueryValue::Property(VertexPropertyValue::Float(
                QueryFloat(value),
            )))
        }
        ScalarValue::String(value) => Ok(QueryValue::Property(VertexPropertyValue::String(
            value.to_string(),
        ))),
    }
}

fn scalar_to_property(value: &ScalarValue) -> StorageResult<VertexPropertyValue> {
    match value {
        ScalarValue::Null => Err(StorageError::new("NULL cannot be a property seek key")),
        ScalarValue::Boolean(value) => Ok(VertexPropertyValue::Bool(*value)),
        ScalarValue::Integer(value) => {
            scalar_integer_to_property(*value).map_err(|error| StorageError::new(error.to_string()))
        }
        ScalarValue::Float(value) => {
            let value = value
                .parse::<f64>()
                .map_err(|error| StorageError::new(error.to_string()))?;
            if !value.is_finite() {
                return Err(StorageError::new(
                    "non-finite floating-point values are not supported",
                ));
            }
            Ok(VertexPropertyValue::Float(QueryFloat(value)))
        }
        ScalarValue::String(value) => Ok(VertexPropertyValue::String(value.to_string())),
    }
}

fn scalar_integer_to_property(value: i128) -> Result<VertexPropertyValue> {
    match u64::try_from(value) {
        Ok(value) => Ok(VertexPropertyValue::Integer(value)),
        Err(_) => i64::try_from(value)
            .map(VertexPropertyValue::SignedInteger)
            .map_err(|_| {
                experimental_query_error(
                    QueryFailureReason::Evaluation,
                    "integer is outside HydraDB's i64/u64 range",
                )
            }),
    }
}

fn storage_result<T>(result: Result<T>) -> StorageResult<T> {
    result.map_err(|error| StorageError::new(error.to_string()))
}

#[cfg(test)]
mod tests {
    use slatedb::object_store::memory::InMemory;

    use super::*;

    async fn shard(path: &str) -> GraphShard {
        GraphShard::open_standalone_writer(path, Arc::new(InMemory::new()))
            .await
            .expect("open graph shard")
    }

    async fn shard_with_limits(path: &str, limits: GraphLimits) -> GraphShard {
        GraphShard::open_standalone_writer_with_limits(path, Arc::new(InMemory::new()), limits)
            .await
            .expect("open graph shard with limits")
    }

    #[tokio::test]
    async fn experimental_multiple_match_reuses_the_anchored_binding() {
        let shard = shard("graph/experimental-cypher-correlated-match").await;
        for (id, label) in [(1, "Entity"), (2, "Person")] {
            shard
                .set_vertex_metadata("cell-a", id, VertexMetadata::default().with_label(label))
                .await
                .expect("write vertex metadata");
        }
        shard
            .write_edge(EdgeMutation {
                cell_id: "cell-a".to_string(),
                edge_type: "KNOWS".to_string(),
                src: 1,
                dst: 2,
                idempotency_key: "experimental-correlated-edge".to_string(),
            })
            .await
            .expect("write edge");

        let query = "MATCH (a:Entity {id: $source}) \
                     MATCH (a)-[:KNOWS]->(b:Person) RETURN b.id AS id";
        let result = shard
            .execute_cypher_rows(
                QueryContext::new("cell-a", "experimental-correlated-match")
                    .with_parameter("source", VertexPropertyValue::Integer(1))
                    .with_cypher_engine(CypherEngineMode::Experimental),
                query,
            )
            .await
            .expect("execute correlated MATCH without an all-node scan");
        assert_eq!(
            result.rows,
            vec![QueryRow::new(vec![QueryValue::VertexId(2)])]
        );

        let explain = shard
            .execute_cypher_rows(
                QueryContext::new("cell-a", "experimental-correlated-match-explain")
                    .with_parameter("source", VertexPropertyValue::Integer(1))
                    .with_cypher_engine(CypherEngineMode::Experimental),
                &format!("EXPLAIN {query}"),
            )
            .await
            .expect("explain correlated MATCH");
        let QueryValue::Property(VertexPropertyValue::String(plan)) = &explain.rows[0].values[0]
        else {
            panic!("EXPLAIN returns plan text");
        };
        assert!(plan.contains("VertexIdSeek(binding=a"));
        assert!(plan.contains("ExpandExec(from=a"));
        assert!(!plan.contains("VertexLabelScan(binding=b"));
        assert!(!plan.contains("AllVertexScan(binding=b"));
        shard.close().await.expect("close graph shard");
    }

    #[tokio::test]
    async fn experimental_mode_dispatches_mutations_to_the_transactional_engine() {
        let shard = shard("graph/experimental-cypher-mutation-dispatch").await;
        let output = shard
            .execute_cypher(
                QueryContext::new("cell-a", "experimental-create")
                    .with_cypher_engine(CypherEngineMode::Experimental),
                "CREATE (a:Entity {id: 1})-[:KNOWS]->(b:Person {id: 2})",
            )
            .await
            .expect("execute mutation in experimental mode");
        let QueryOutput::Mutation(mutation) = output else {
            panic!("mutation dispatcher returns mutation metadata");
        };
        assert_eq!(mutation.created_edges, 1);

        let rows = shard
            .execute_cypher_rows(
                QueryContext::new("cell-a", "experimental-read-created")
                    .with_parameter("source", VertexPropertyValue::Integer(1))
                    .with_cypher_engine(CypherEngineMode::Experimental),
                "MATCH (a:Entity {id: $source})-[:KNOWS]->(b:Person) RETURN b.id AS id",
            )
            .await
            .expect("read mutation through experimental row planner");
        assert_eq!(
            rows.rows,
            vec![QueryRow::new(vec![QueryValue::VertexId(2)])]
        );

        let page = shard
            .execute_cypher_rows_page(
                QueryContext::new("cell-a", "experimental-page-create")
                    .with_cypher_engine(CypherEngineMode::Experimental),
                "CREATE (a:Entity {id: 3})-[:LIKES]->(b:Person {id: 4})",
                None,
                128,
            )
            .await
            .expect("execute paged mutation in experimental mode");
        assert!(page.rows.is_empty());
        assert!(page.next_cursor.is_none());

        let page_rows = shard
            .execute_cypher_rows(
                QueryContext::new("cell-a", "experimental-read-page-created")
                    .with_parameter("source", VertexPropertyValue::Integer(3))
                    .with_cypher_engine(CypherEngineMode::Experimental),
                "MATCH (a:Entity {id: $source})-[:LIKES]->(b:Person) RETURN b.id AS id",
            )
            .await
            .expect("read paged mutation through experimental row planner");
        assert_eq!(
            page_rows.rows,
            vec![QueryRow::new(vec![QueryValue::VertexId(4)])]
        );
        shard.close().await.expect("close graph shard");
    }

    #[tokio::test]
    async fn experimental_mode_dispatches_native_path_pages_to_the_path_engine() {
        let shard = shard("graph/experimental-native-path-dispatch").await;
        shard
            .write_edge(EdgeMutation {
                cell_id: "cell-a".to_string(),
                edge_type: "ROUTE".to_string(),
                src: 1,
                dst: 2,
                idempotency_key: "experimental-native-path-edge".to_string(),
            })
            .await
            .expect("write path edge");
        let query = "CALL algo.SSpaths({sourceNode: 1, relTypes: ['ROUTE'], maxLen: 1, \
                     pathCount: 10}) YIELD path RETURN path";

        let page = shard
            .execute_cypher_rows_page(
                QueryContext::new("cell-a", "experimental-native-path-page")
                    .with_cypher_engine(CypherEngineMode::Experimental),
                query,
                None,
                1,
            )
            .await
            .expect("execute native path page in experimental mode");

        assert_eq!(page.rows.len(), 1);
        let QueryValue::Path(path) = &page.rows[0].values[0] else {
            panic!("native path procedure returns a path");
        };
        assert_eq!(path.nodes.last().expect("path target").id, 2);
        assert!(page.next_cursor.is_none());
        shard.close().await.expect("close graph shard");
    }

    #[tokio::test]
    async fn experimental_relationship_property_anchor_uses_the_hydradb_index() {
        let shard = shard("graph/experimental-relationship-property-seek").await;
        for id in [1, 2, 3] {
            shard
                .set_vertex_metadata("cell-a", id, VertexMetadata::default().with_label("Entity"))
                .await
                .expect("write vertex metadata");
        }
        for (destination, chunk_id) in [(2, "wanted"), (3, "other")] {
            shard
                .write_edge(EdgeMutation {
                    cell_id: "cell-a".to_string(),
                    edge_type: "RELATES".to_string(),
                    src: 1,
                    dst: destination,
                    idempotency_key: format!("experimental-property-edge-{destination}"),
                })
                .await
                .expect("write edge");
            shard
                .set_edge_metadata(
                    "cell-a",
                    "RELATES",
                    1,
                    destination,
                    EdgeMetadata::default().with_property(
                        "chunk_id",
                        VertexPropertyValue::String(chunk_id.to_string()),
                    ),
                )
                .await
                .expect("write edge metadata");
        }

        let query = "MATCH (a)-[r:RELATES {chunk_id: $chunk}]->(b) \
                     RETURN a.id AS source, b.id AS destination, r.chunk_id AS chunk";
        let context = QueryContext::new("cell-a", "experimental-relationship-property-seek")
            .with_parameter("chunk", VertexPropertyValue::String("wanted".to_string()))
            .with_cypher_engine(CypherEngineMode::Experimental);
        let result = shard
            .execute_cypher_rows(context, query)
            .await
            .expect("execute relationship-property index seek");
        assert_eq!(
            result.rows,
            vec![QueryRow::new(vec![
                QueryValue::VertexId(1),
                QueryValue::VertexId(2),
                QueryValue::Property(VertexPropertyValue::String("wanted".to_string())),
            ])]
        );

        let explain = shard
            .execute_cypher_rows(
                QueryContext::new("cell-a", "experimental-relationship-property-explain")
                    .with_parameter("chunk", VertexPropertyValue::String("wanted".to_string()))
                    .with_cypher_engine(CypherEngineMode::Experimental),
                &format!("EXPLAIN {query}"),
            )
            .await
            .expect("explain relationship-property index seek");
        let QueryValue::Property(VertexPropertyValue::String(plan)) = &explain.rows[0].values[0]
        else {
            panic!("EXPLAIN returns plan text");
        };
        assert!(plan.contains("RelationshipPropertySeek"));
        assert!(!plan.contains("AllVertexScan"));
        shard.close().await.expect("close graph shard");
    }

    /// The production shape: two equalities pin the vertex, the broad one is
    /// written first, and the planner anchors on whatever it reads first.
    ///
    /// Two hundred entities share the tenant and four share the sub-tenant, so
    /// the property fetch counter separates the two access paths outright: the
    /// planned seek hydrates every candidate in the tenant, and a probe that
    /// ranked the properties hydrates only the sub-tenant's. Disabling the
    /// probe turns the 4 below into 200.
    #[tokio::test]
    async fn experimental_equality_probe_anchors_on_the_narrow_property() {
        let shard = shard("graph/experimental-equality-probe").await;
        for id in 1..=200u64 {
            shard
                .set_vertex_metadata(
                    "cell-a",
                    id,
                    VertexMetadata::default()
                        .with_label("Entity")
                        .with_property("tenant_id", VertexPropertyValue::String("t1".to_string()))
                        .with_property(
                            "sub_tenant_id",
                            VertexPropertyValue::String(
                                if id % 50 == 0 { "s9" } else { "s0" }.to_string(),
                            ),
                        ),
                )
                .await
                .expect("write vertex metadata");
        }

        let before = shard.graph_operational_metrics().query_property_fetches;
        let result = shard
            .execute_cypher_rows(
                QueryContext::new("cell-a", "experimental-equality-probe")
                    .with_parameter("tenant_id", VertexPropertyValue::String("t1".to_string()))
                    .with_parameter(
                        "sub_tenant_id",
                        VertexPropertyValue::String("s9".to_string()),
                    )
                    .with_cypher_engine(CypherEngineMode::Experimental),
                "MATCH (e:Entity {tenant_id: $tenant_id, sub_tenant_id: $sub_tenant_id}) \
                 RETURN e.id AS id ORDER BY id",
            )
            .await
            .expect("execute tenant-scoped match");
        let fetched = shard.graph_operational_metrics().query_property_fetches - before;

        assert_eq!(
            result.rows,
            vec![
                QueryRow::new(vec![QueryValue::VertexId(50)]),
                QueryRow::new(vec![QueryValue::VertexId(100)]),
                QueryRow::new(vec![QueryValue::VertexId(150)]),
                QueryRow::new(vec![QueryValue::VertexId(200)]),
            ]
        );
        assert!(
            fetched < 50,
            "anchored on the broad property: {fetched} fetches for 4 rows"
        );
    }

    /// A page resumes at the value it stopped on and skips what it already
    /// returned, which means deciding, for the run sharing that value, which
    /// vertex IDs are behind the cursor. The index answers that in the
    /// direction it is walked. Ask it in the direction the query ranks by and
    /// the two disagree on every query that sorts ties against the scan: most
    /// of the run is read a second time, surviving rows come back twice, and
    /// the duplicates spend the window that the later matches needed.
    #[tokio::test]
    async fn experimental_ordered_window_resumes_in_the_direction_it_reads() {
        let shard = shard("graph/experimental-ordered-window-opposed-tie").await;
        // Run A holds four entries and only its first two survive the tenant
        // filter, so the page fills, stops on A, and has to come back for more.
        // Run C holds the match that completes the answer.
        for (id, created_at, tenant) in [
            (1u64, "2026-01-01", "t9"),
            (2, "2026-01-01", "t9"),
            (3, "2026-01-01", "t0"),
            (4, "2026-01-01", "t0"),
            (5, "2026-01-02", "t0"),
            (6, "2026-01-02", "t0"),
            (7, "2026-01-03", "t9"),
        ] {
            shard
                .set_vertex_metadata(
                    "cell-a",
                    id,
                    VertexMetadata::default()
                        .with_label("Entity")
                        .with_property(
                            "created_at",
                            VertexPropertyValue::String(created_at.to_string()),
                        )
                        .with_property(
                            "tenant_id",
                            VertexPropertyValue::String(tenant.to_string()),
                        ),
                )
                .await
                .expect("write vertex metadata");
        }

        let result = shard
            .execute_cypher_rows(
                QueryContext::new("cell-a", "experimental-ordered-window-opposed-tie")
                    .with_parameter("tenant_id", VertexPropertyValue::String("t9".to_string()))
                    .with_cypher_engine(CypherEngineMode::Experimental),
                // Ascending walk, descending tie: the scan reads the run 1, 2,
                // 3, 4 while the query wants it 4, 3, 2, 1.
                "MATCH (e:Entity {tenant_id: $tenant_id}) WHERE e.created_at STARTS WITH '2026' \
                 RETURN e.id AS id ORDER BY e.created_at, e.id DESC LIMIT 3",
            )
            .await
            .expect("execute a window whose tie order opposes the walk");
        assert_eq!(
            result.rows,
            vec![
                QueryRow::new(vec![QueryValue::VertexId(2)]),
                QueryRow::new(vec![QueryValue::VertexId(1)]),
                QueryRow::new(vec![QueryValue::VertexId(7)]),
            ],
            "run A ranked descending, then the later match -- each row once"
        );
        shard.close().await.expect("close graph shard");
    }

    /// The bounded page keeps the best `required` entries by the index's own
    /// ranking, which is the value then the vertex ID. That ranking is the
    /// caller's only while the tie key is the vertex ID. Order ties by a
    /// property instead and the entries the page dropped may be the ones the
    /// query wanted, and the cursor has already moved past them.
    #[tokio::test]
    async fn experimental_ordered_window_ranks_a_tie_by_the_querys_own_key() {
        let shard = shard("graph/experimental-ordered-window-property-tie").await;
        for id in 1..=5u64 {
            shard
                .set_vertex_metadata(
                    "cell-a",
                    id,
                    VertexMetadata::default()
                        .with_label("Entity")
                        .with_property(
                            "created_at",
                            VertexPropertyValue::String("2026-01-01".to_string()),
                        )
                        // Reverse of the vertex ID order, so the index's
                        // ranking and the query's disagree on every pair.
                        .with_property(
                            "entity_id",
                            VertexPropertyValue::String(format!("e{}", 6 - id)),
                        ),
                )
                .await
                .expect("write vertex metadata");
        }

        let result = shard
            .execute_cypher_rows(
                QueryContext::new("cell-a", "experimental-ordered-window-property-tie")
                    .with_cypher_engine(CypherEngineMode::Experimental),
                "MATCH (n:Entity) WHERE n.created_at STARTS WITH '2026' \
                 RETURN n.id AS id ORDER BY n.created_at, n.entity_id LIMIT 2",
            )
            .await
            .expect("execute a window whose tie key is a property");
        assert_eq!(
            result.rows,
            vec![
                QueryRow::new(vec![QueryValue::VertexId(5)]),
                QueryRow::new(vec![QueryValue::VertexId(4)]),
            ],
            "e1 and e2 win the tie; the lowest vertex IDs carry the highest entity_ids"
        );
        shard.close().await.expect("close graph shard");
    }

    /// The same dropped-tie hazard reached through the residual rather than the
    /// tie key. Every row shares the sort value, so the whole answer lives in
    /// one tie group, and the rows the tenant filter keeps are the ones the
    /// index ranks last.
    #[tokio::test]
    async fn experimental_ordered_window_keeps_tied_rows_a_residual_still_needs() {
        let shard = shard("graph/experimental-ordered-window-tied-residual").await;
        for id in 1..=6u64 {
            shard
                .set_vertex_metadata(
                    "cell-a",
                    id,
                    VertexMetadata::default()
                        .with_label("Entity")
                        .with_property(
                            "created_at",
                            VertexPropertyValue::String("2026-01-01".to_string()),
                        )
                        .with_property(
                            "tenant_id",
                            VertexPropertyValue::String(
                                if id >= 5 { "t9" } else { "t0" }.to_string(),
                            ),
                        ),
                )
                .await
                .expect("write vertex metadata");
        }

        let result = shard
            .execute_cypher_rows(
                QueryContext::new("cell-a", "experimental-ordered-window-tied-residual")
                    .with_parameter("tenant_id", VertexPropertyValue::String("t9".to_string()))
                    .with_cypher_engine(CypherEngineMode::Experimental),
                "MATCH (e:Entity {tenant_id: $tenant_id}) WHERE e.created_at STARTS WITH '2026' \
                 RETURN e.id AS id ORDER BY e.created_at, e.id LIMIT 2",
            )
            .await
            .expect("execute a tied window behind a residual");
        assert_eq!(
            result.rows,
            vec![
                QueryRow::new(vec![QueryValue::VertexId(5)]),
                QueryRow::new(vec![QueryValue::VertexId(6)]),
            ]
        );
        shard.close().await.expect("close graph shard");
    }

    /// A residual that rejects a whole page is not the index running out. The
    /// walk fills `required` with rows the tenant filter then discards, and if
    /// that page reports itself as the end of the index the query answers with
    /// fewer rows than it asked for -- quietly, since nothing about it is an
    /// error. Only every tenth vertex here survives, and none of them is in the
    /// first page, so the answer is the whole of what paging is for.
    #[tokio::test]
    async fn experimental_ordered_window_pages_past_a_rejecting_residual() {
        let shard = shard("graph/experimental-ordered-window-residual-paging").await;
        for id in 1..=30u64 {
            shard
                .set_vertex_metadata(
                    "cell-a",
                    id,
                    VertexMetadata::default()
                        .with_label("Entity")
                        .with_property(
                            "created_at",
                            VertexPropertyValue::String(format!("2026-01-{id:02}")),
                        )
                        .with_property(
                            "tenant_id",
                            VertexPropertyValue::String(
                                if id % 10 == 0 { "t9" } else { "t0" }.to_string(),
                            ),
                        ),
                )
                .await
                .expect("write vertex metadata");
        }

        let result = shard
            .execute_cypher_rows(
                QueryContext::new("cell-a", "experimental-ordered-window-residual-paging")
                    .with_parameter("tenant_id", VertexPropertyValue::String("t9".to_string()))
                    .with_cypher_engine(CypherEngineMode::Experimental),
                "MATCH (e:Entity {tenant_id: $tenant_id}) WHERE e.created_at STARTS WITH '2026' \
                 RETURN e.id AS id ORDER BY e.created_at LIMIT 3",
            )
            .await
            .expect("execute ordered window over a rejecting residual");
        assert_eq!(
            result.rows,
            vec![
                QueryRow::new(vec![QueryValue::VertexId(10)]),
                QueryRow::new(vec![QueryValue::VertexId(20)]),
                QueryRow::new(vec![QueryValue::VertexId(30)]),
            ]
        );
        shard.close().await.expect("close graph shard");
    }

    /// Each page of a descending walk resumes below where the last one
    /// stopped. SlateDB starts a descending scan at the top of its range, so
    /// before the walk had an end bound every page re-read the index from its
    /// highest key and discarded what earlier pages returned: page `k` read
    /// `k` pages' worth of entries, and a walk a residual starved went
    /// quadratic. Ascending is checked alongside as the baseline it now
    /// matches.
    #[tokio::test]
    async fn an_ordered_walk_reads_each_page_once_in_both_directions() {
        let shard = shard("graph/experimental-ordered-walk-page-cost").await;
        for id in 1..=40u64 {
            shard
                .set_vertex_metadata(
                    "cell-a",
                    id,
                    VertexMetadata::default()
                        .with_label("Entity")
                        .with_property(
                            "created_at",
                            VertexPropertyValue::String(format!("2026-01-{id:02}")),
                        ),
                )
                .await
                .expect("write vertex metadata");
        }
        let snapshot = shard.db.snapshot().await.expect("snapshot");
        let read_epoch = snapshot.seq();
        let budget = QueryBudget::new(None, None);
        for ascending in [true, false] {
            let mut after = None;
            let mut seen = Vec::new();
            for page in 0..5 {
                let window = super::query::OrderedStringVertexWindow {
                    property: "created_at".to_string(),
                    labels: vec!["Entity".to_string()],
                    ascending,
                    id_ascending: true,
                    prefix: Some("2026".to_string()),
                    lower: None,
                    upper: None,
                    required: 4,
                    after: after.clone(),
                    index_order: false,
                };
                let result = GraphStore::scope_snapshot(
                    Arc::clone(&snapshot),
                    shard.scan_ordered_string_vertex_window_at(
                        "cell-a", &window, read_epoch, &budget,
                    ),
                )
                .await
                .expect("scan a page");
                assert_eq!(
                    result.vertices.len(),
                    4,
                    "ascending={ascending} page={page}"
                );
                // Four returned, plus the entry a page resumes on (read again,
                // then skipped) and the one that tells it the page is full: a
                // constant per page. A restarting walk reads 4 * (page + 1)
                // and more, growing with every page.
                assert!(
                    result.index_entries <= 6,
                    "ascending={ascending} page={page} read {} index entries",
                    result.index_entries
                );
                seen.extend(result.vertices.iter().map(|(id, _)| *id));
                after = result.last.clone();
            }
            let expected: Vec<VertexId> = if ascending {
                (1..=20).collect()
            } else {
                (21..=40).rev().collect()
            };
            assert_eq!(seen, expected, "ascending={ascending}");
        }
        shard.close().await.expect("close graph shard");
    }

    /// Resuming *inside* one primary value costs no more than resuming between
    /// two of them. A page of a tie group used to resume on the value alone and
    /// let `already_returned` discard the entries earlier pages had taken, so
    /// page `k` re-read the whole run ahead of it: draining an `n`-row tie group
    /// read on the order of `n^2 / 2` index entries. The walk now seeks past the
    /// vertex ID it stopped on, which is the same position `already_returned`
    /// computes -- so the skip stays as a guard against a stale index rather
    /// than as the mechanism.
    #[tokio::test]
    async fn an_ordered_walk_resumes_inside_a_tie_run_without_rereading_it() {
        let shard = shard("graph/experimental-ordered-walk-tie-run-cost").await;
        for id in 1..=40u64 {
            shard
                .set_vertex_metadata(
                    "cell-a",
                    id,
                    VertexMetadata::default()
                        .with_label("Entity")
                        // One value for all forty, so every page resumes inside
                        // the same run instead of moving on to the next value.
                        .with_property(
                            "created_at",
                            VertexPropertyValue::String("2026-01-01".to_string()),
                        ),
                )
                .await
                .expect("write vertex metadata");
        }
        let snapshot = shard.db.snapshot().await.expect("snapshot");
        let read_epoch = snapshot.seq();
        let budget = QueryBudget::new(None, None);
        for ascending in [true, false] {
            let mut after = None;
            let mut seen = Vec::new();
            for page in 0..5 {
                let window = super::query::OrderedStringVertexWindow {
                    property: "created_at".to_string(),
                    labels: vec!["Entity".to_string()],
                    ascending,
                    id_ascending: true,
                    prefix: Some("2026".to_string()),
                    lower: None,
                    upper: None,
                    required: 4,
                    after: after.clone(),
                    // What the engine asks for whenever it drains a tie group:
                    // index order, and drop nothing the page read.
                    index_order: true,
                };
                let result = GraphStore::scope_snapshot(
                    Arc::clone(&snapshot),
                    shard.scan_ordered_string_vertex_window_at(
                        "cell-a", &window, read_epoch, &budget,
                    ),
                )
                .await
                .expect("scan a page");
                assert_eq!(
                    result.vertices.len(),
                    4,
                    "ascending={ascending} page={page}"
                );
                // Four returned plus the one that tells the page it is full. A
                // walk that resumes by value alone reads 4 * page more than
                // that, growing with every page.
                assert!(
                    result.index_entries <= 6,
                    "ascending={ascending} page={page} read {} index entries",
                    result.index_entries
                );
                seen.extend(result.vertices.iter().map(|(id, _)| *id));
                after = result.last.clone();
            }
            let expected: Vec<VertexId> = if ascending {
                (1..=20).collect()
            } else {
                // A descending walk reads the run downward while `id_ascending`
                // ranks each page's four rows upward -- the disagreement between
                // read order and rank order that a resume has to get right, and
                // the reason the cursor cannot be compared on the rank.
                (0..5u64)
                    .flat_map(|page| {
                        let top = 40 - page * 4;
                        (top - 3)..=top
                    })
                    .collect()
            };
            assert_eq!(seen, expected, "ascending={ascending}");
        }
        shard.close().await.expect("close graph shard");
    }

    #[tokio::test]
    async fn experimental_unlabelled_range_predicate_uses_the_property_index() {
        let shard = shard("graph/experimental-property-range-scan").await;
        for (id, name) in [(1, "alpha"), (2, "beta"), (3, "alpine")] {
            shard
                .set_vertex_metadata(
                    "cell-a",
                    id,
                    VertexMetadata::default()
                        .with_property("name", VertexPropertyValue::String(name.to_string())),
                )
                .await
                .expect("write vertex metadata");
        }
        let query = "MATCH (n) WHERE n.name STARTS WITH $prefix \
                     RETURN n.id AS id ORDER BY id";
        let result = shard
            .execute_cypher_rows(
                QueryContext::new("cell-a", "experimental-property-range")
                    .with_parameter("prefix", VertexPropertyValue::String("al".to_string()))
                    .with_cypher_engine(CypherEngineMode::Experimental),
                query,
            )
            .await
            .expect("execute anchored property scan");
        assert_eq!(
            result.rows,
            vec![
                QueryRow::new(vec![QueryValue::VertexId(1)]),
                QueryRow::new(vec![QueryValue::VertexId(3)]),
            ]
        );

        let explain = shard
            .execute_cypher_rows(
                QueryContext::new("cell-a", "experimental-property-range-explain")
                    .with_parameter("prefix", VertexPropertyValue::String("al".to_string()))
                    .with_cypher_engine(CypherEngineMode::Experimental),
                &format!("EXPLAIN {query}"),
            )
            .await
            .expect("explain anchored property scan");
        let QueryValue::Property(VertexPropertyValue::String(plan)) = &explain.rows[0].values[0]
        else {
            panic!("EXPLAIN returns plan text");
        };
        assert!(plan.contains("VertexPropertyScan"));
        assert!(!plan.contains("AllVertexScan"));
        shard.close().await.expect("close graph shard");
    }

    #[tokio::test]
    async fn experimental_multi_seek_matches_legacy_and_explains_the_executed_plan() {
        let shard = shard("graph/experimental-cypher-multi-seek").await;
        for (id, label, entity_id) in [
            (1, "Entity", "a"),
            (2, "Entity", "b"),
            (3, "Entity", "z"),
            (4, "Other", "a"),
        ] {
            shard
                .set_vertex_metadata(
                    "cell-a",
                    id,
                    VertexMetadata::default().with_label(label).with_property(
                        "entity_id",
                        VertexPropertyValue::String(entity_id.to_string()),
                    ),
                )
                .await
                .expect("write vertex metadata");
        }
        let query = "MATCH (e:Entity) \
                     WHERE e.entity_id = $a OR e.entity_id = $b \
                     RETURN e.entity_id AS id ORDER BY id ASC";
        let parameters = [
            (
                "a".to_string(),
                VertexPropertyValue::String("a".to_string()),
            ),
            (
                "b".to_string(),
                VertexPropertyValue::String("b".to_string()),
            ),
        ];
        let experimental = QueryContext::new("cell-a", "experimental-multi-seek")
            .with_parameters(parameters.clone())
            .with_cypher_engine(CypherEngineMode::Experimental);
        let result = shard
            .execute_cypher_rows(experimental, query)
            .await
            .expect("execute experimental Cypher");
        assert_eq!(result.columns, vec![QueryColumn::new("id")]);
        assert_eq!(
            result.rows,
            vec![
                QueryRow::new(vec![QueryValue::Property(VertexPropertyValue::String(
                    "a".to_string()
                ))]),
                QueryRow::new(vec![QueryValue::Property(VertexPropertyValue::String(
                    "b".to_string()
                ))]),
            ]
        );
        let experimental_metrics = shard.graph_operational_metrics();
        assert_eq!(experimental_metrics.query_rows_started, 1);
        assert_eq!(experimental_metrics.query_rows_completed, 1);
        assert_eq!(experimental_metrics.query_rows_failed, 0);
        assert_eq!(experimental_metrics.query_rows_returned, 2);
        assert_eq!(experimental_metrics.query_rows_latency.count(), 1);
        assert_eq!(
            experimental_metrics.query_experimental_property_seek_requests,
            2
        );
        assert_eq!(
            experimental_metrics.query_experimental_relationship_expand_requests,
            0
        );

        let legacy = shard
            .execute_cypher_rows(
                QueryContext::new("cell-a", "legacy-multi-seek").with_parameters(parameters),
                query,
            )
            .await
            .expect("execute legacy Cypher");
        assert_eq!(result, legacy);

        let before_explain = shard.graph_operational_metrics();

        let explained = shard
            .execute_cypher_rows(
                QueryContext::new("cell-a", "experimental-explain")
                    .with_parameter("a", VertexPropertyValue::String("a".to_string()))
                    .with_parameter("b", VertexPropertyValue::String("b".to_string()))
                    .with_cypher_engine(CypherEngineMode::Experimental),
                &format!("EXPLAIN {query}"),
            )
            .await
            .expect("explain experimental Cypher");
        assert_eq!(explained.columns, vec![QueryColumn::new("plan")]);
        let QueryValue::Property(VertexPropertyValue::String(plan)) = &explained.rows[0].values[0]
        else {
            panic!("EXPLAIN returns plan text");
        };
        assert!(plan.contains("VertexPropertyMultiSeek"));
        assert!(plan.contains("SortExec"));
        let after_explain = shard.graph_operational_metrics();
        assert_eq!(
            after_explain.query_rows_started,
            before_explain.query_rows_started
        );
        assert_eq!(
            after_explain.query_rows_latency.count(),
            before_explain.query_rows_latency.count()
        );
        assert_eq!(
            after_explain.query_experimental_property_seek_requests,
            before_explain.query_experimental_property_seek_requests
        );

        shard.close().await.expect("close graph shard");
    }

    #[tokio::test]
    async fn experimental_unlabelled_match_scans_canonical_vertices() {
        let shard = shard("graph/experimental-unlabelled-match").await;
        for id in [2, 1] {
            shard
                .set_vertex_metadata("cell-a", id, VertexMetadata::default().with_label("Entity"))
                .await
                .expect("write vertex metadata");
        }

        let result = shard
            .execute_cypher_rows(
                QueryContext::new("cell-a", "experimental-unlabelled-match")
                    .with_cypher_engine(CypherEngineMode::Experimental)
                    .with_refreshed_reader(),
                "MATCH (n) RETURN n.id AS id ORDER BY id",
            )
            .await
            .expect("execute unlabelled MATCH");

        assert_eq!(
            result.rows,
            vec![
                QueryRow::new(vec![QueryValue::VertexId(1)]),
                QueryRow::new(vec![QueryValue::VertexId(2)]),
            ]
        );
        shard.close().await.expect("close graph shard");
    }

    #[tokio::test]
    async fn experimental_ordered_window_walks_the_string_index_and_matches_legacy() {
        let shard = shard("graph/experimental-cypher-ordered-window").await;
        // Six Entity rows with a tie on the sort key, one row with the wrong
        // label, and one row whose value is not a string. The ordered walk
        // must skip the last two without ever hydrating the whole population.
        for (vertex_id, label, created_at) in [
            (1, "Entity", Some("2026-01-01")),
            (2, "Entity", Some("2026-01-02")),
            (3, "Entity", Some("2026-01-03")),
            (4, "Entity", Some("2026-01-03")),
            (5, "Entity", Some("2026-01-04")),
            (6, "Entity", Some("2026-01-05")),
            (7, "Other", Some("2026-01-06")),
            (8, "Entity", None),
        ] {
            let mut metadata = VertexMetadata::default().with_label(label);
            metadata = match created_at {
                Some(value) => metadata
                    .with_property("created_at", VertexPropertyValue::String(value.to_string())),
                None => {
                    metadata.with_property("created_at", VertexPropertyValue::Integer(20260107))
                }
            };
            shard
                .set_vertex_metadata("cell-a", vertex_id, metadata)
                .await
                .unwrap();
        }

        let ordered = "MATCH (n:Entity) WHERE n.created_at STARTS WITH '' \
                       RETURN n.id AS id ORDER BY n.created_at DESC, n.id LIMIT 3";
        let keyset =
            "MATCH (n:Entity) WHERE n.created_at > $cursor AND n.created_at STARTS WITH '' \
                      RETURN n.id AS id ORDER BY n.created_at, n.id LIMIT 3";
        let context = |name: &str, mode: CypherEngineMode| {
            QueryContext::new("cell-a", name)
                .with_parameter(
                    "cursor",
                    VertexPropertyValue::String("2026-01-02".to_string()),
                )
                .with_cypher_engine(mode)
        };

        let explained = shard
            .execute_cypher_rows(
                context("ordered-explain", CypherEngineMode::Experimental),
                &format!("EXPLAIN {ordered}"),
            )
            .await
            .expect("explain ordered window");
        let QueryValue::Property(VertexPropertyValue::String(plan)) = &explained.rows[0].values[0]
        else {
            panic!("EXPLAIN returns plan text");
        };
        assert!(plan.contains("OrderedVertexPropertyScan"), "{plan}");
        assert!(!plan.contains("SortExec"), "{plan}");
        assert!(!plan.contains("FilterExec"), "{plan}");

        for (name, query) in [("ordered", ordered), ("keyset", keyset)] {
            let legacy = shard
                .execute_cypher_rows(
                    context(&format!("{name}-legacy"), CypherEngineMode::Legacy),
                    query,
                )
                .await
                .expect("execute legacy ordered window");
            let experimental = shard
                .execute_cypher_rows(
                    context(
                        &format!("{name}-experimental"),
                        CypherEngineMode::Experimental,
                    ),
                    query,
                )
                .await
                .expect("execute experimental ordered window");
            assert_eq!(experimental.columns, legacy.columns, "{name}");
            assert_eq!(experimental.rows, legacy.rows, "{name}");
        }
        let ids = |rows: &[QueryRow]| {
            rows.iter()
                .map(|row| match &row.values[0] {
                    QueryValue::VertexId(id) => *id,
                    other => panic!("unexpected id value {other:?}"),
                })
                .collect::<Vec<_>>()
        };
        let ordered_rows = shard
            .execute_cypher_rows(
                context("ordered-check", CypherEngineMode::Experimental),
                ordered,
            )
            .await
            .unwrap();
        assert_eq!(ids(&ordered_rows.rows), vec![6, 5, 3]);
        let keyset_rows = shard
            .execute_cypher_rows(
                context("keyset-check", CypherEngineMode::Experimental),
                keyset,
            )
            .await
            .unwrap();
        assert_eq!(ids(&keyset_rows.rows), vec![3, 4, 5]);

        let metrics = shard.graph_operational_metrics();
        // Four experimental executions: two parity runs plus two checks. Each
        // is one ordered walk and never falls back to the broad property scan.
        assert_eq!(metrics.query_experimental_ordered_property_scan_requests, 4);
        assert_eq!(metrics.query_experimental_property_seek_requests, 0);
        assert_eq!(metrics.query_experimental_relationship_expand_requests, 0);

        shard.close().await.expect("close graph shard");
    }

    /// A walk that must read a whole tie group -- to rank it by another key,
    /// or to filter it with a residual the index cannot see -- keeps only the
    /// window's best rows while it reads, never the group. Two hundred rows
    /// share one value here against a 32-row intermediate budget; holding the
    /// group, as the walk once did, fails every one of these with an
    /// admission error despite a `LIMIT` of three.
    #[tokio::test]
    async fn experimental_ordered_window_drains_a_large_tie_group_within_the_window() {
        const TIE_ROWS: u64 = 200;
        let shard = shard_with_limits(
            "graph/experimental-cypher-ordered-window-drained-tie",
            GraphLimits {
                max_query_intermediate_rows: 32,
                ..GraphLimits::default()
            },
        )
        .await;
        shard
            .set_vertex_metadata_batch(
                "cell-a",
                (1..=TIE_ROWS).map(|vertex_id| {
                    (
                        vertex_id,
                        VertexMetadata::default()
                            .with_label("Entity")
                            .with_property(
                                "created_at",
                                VertexPropertyValue::String("2026-01-01".to_string()),
                            )
                            // Reverse of vertex-ID order, so the index and the
                            // tie key disagree on every pair.
                            .with_property(
                                "entity_id",
                                VertexPropertyValue::String(format!(
                                    "e{:03}",
                                    TIE_ROWS + 1 - vertex_id
                                )),
                            )
                            .with_property(
                                "tenant_id",
                                VertexPropertyValue::String(
                                    if vertex_id % 7 == 0 { "t1" } else { "t0" }.to_string(),
                                ),
                            ),
                    )
                }),
            )
            .await
            .expect("seed one large tie group");

        for (predicate, order, expected) in [
            // Ties ranked by another property: the whole group is read, the
            // best three kept.
            ("", "n.created_at, n.entity_id", vec![200, 199, 198]),
            // A residual in index order: the first three survivors.
            ("AND n.tenant_id = 't1'", "n.created_at", vec![7, 14, 21]),
            // A residual with ties ranked against the index's ID order.
            (
                "AND n.tenant_id = 't1'",
                "n.created_at, n.id DESC",
                vec![196, 189, 182],
            ),
            // Both at once.
            (
                "AND n.tenant_id = 't1'",
                "n.created_at, n.entity_id",
                vec![196, 189, 182],
            ),
        ] {
            let query = format!(
                "MATCH (n:Entity) WHERE n.created_at STARTS WITH '2026' {predicate} \
                 RETURN n.id AS id ORDER BY {order} LIMIT 3"
            );
            let result = shard
                .execute_cypher_rows(
                    QueryContext::new("cell-a", "experimental-ordered-window-drained-tie")
                        .with_cypher_engine(CypherEngineMode::Experimental),
                    &query,
                )
                .await
                .unwrap_or_else(|error| panic!("{query}: {error}"));
            let actual = result
                .rows
                .iter()
                .map(|row| match row.values[0] {
                    QueryValue::VertexId(id) => id,
                    ref other => panic!("unexpected ID: {other:?}"),
                })
                .collect::<Vec<_>>();
            assert_eq!(actual, expected, "{query}");
        }
        shard.close().await.expect("close graph shard");
    }

    /// A window as large as the intermediate-row budget still fits: the walk
    /// never holds a row past the window, even for a moment. It used to append
    /// the next page's survivor and check the budget before cutting back to
    /// the window, so `LIMIT 8` under an 8-row budget failed as soon as a tie
    /// group -- or one wide page of a residual walk -- ran past eight rows.
    #[tokio::test]
    async fn experimental_ordered_window_as_large_as_the_row_budget_fits() {
        const BUDGET: usize = 8;
        let shard = shard_with_limits(
            "graph/experimental-cypher-ordered-window-at-budget",
            GraphLimits {
                max_query_intermediate_rows: BUDGET,
                ..GraphLimits::default()
            },
        )
        .await;
        shard
            .set_vertex_metadata_batch(
                "cell-a",
                (1..=30u64).map(|vertex_id| {
                    (
                        vertex_id,
                        VertexMetadata::default()
                            .with_label("Entity")
                            .with_property(
                                "created_at",
                                VertexPropertyValue::String("2026-01-01".to_string()),
                            )
                            .with_property(
                                "entity_id",
                                VertexPropertyValue::String(format!("e{:02}", 31 - vertex_id)),
                            )
                            .with_property(
                                "tenant_id",
                                VertexPropertyValue::String(
                                    if vertex_id % 2 == 0 { "t1" } else { "t0" }.to_string(),
                                ),
                            ),
                    )
                }),
            )
            .await
            .expect("seed one tie group spanning several pages");

        for (predicate, order, expected) in [
            // Draining: the tie group spans pages past the window.
            (
                "",
                "n.created_at, n.entity_id",
                (23..=30).rev().collect::<Vec<u64>>(),
            ),
            // Index order under a residual: a page may carry more survivors
            // than the window has room for.
            (
                "AND n.tenant_id = 't1'",
                "n.created_at",
                (1..=8).map(|n| n * 2).collect(),
            ),
        ] {
            let query = format!(
                "MATCH (n:Entity) WHERE n.created_at STARTS WITH '2026' {predicate} \
                 RETURN n.id AS id ORDER BY {order} LIMIT {BUDGET}"
            );
            let result = shard
                .execute_cypher_rows(
                    QueryContext::new("cell-a", "experimental-ordered-window-at-budget")
                        .with_cypher_engine(CypherEngineMode::Experimental),
                    &query,
                )
                .await
                .unwrap_or_else(|error| panic!("{query}: {error}"));
            let actual = result
                .rows
                .iter()
                .map(|row| match row.values[0] {
                    QueryValue::VertexId(id) => id,
                    ref other => panic!("unexpected ID: {other:?}"),
                })
                .collect::<Vec<_>>();
            assert_eq!(actual, expected, "{query}");
        }
        shard.close().await.expect("close graph shard");
    }

    #[tokio::test]
    async fn experimental_ordered_window_bounds_the_boundary_tie() {
        const TIE_ROWS: u64 = 33;
        const INTERMEDIATE_ROW_LIMIT: usize = 32;
        let shard = shard_with_limits(
            "graph/experimental-cypher-ordered-window-large-tie",
            GraphLimits {
                max_query_intermediate_rows: INTERMEDIATE_ROW_LIMIT,
                max_query_index_candidates: TIE_ROWS as usize + 1,
                ..GraphLimits::default()
            },
        )
        .await;
        shard
            .set_vertex_metadata_batch(
                "cell-a",
                (1..=TIE_ROWS).map(|vertex_id| {
                    (
                        vertex_id,
                        VertexMetadata::default()
                            .with_label("Entity")
                            .with_property(
                                "created_at",
                                VertexPropertyValue::String("2026-01-01".to_string()),
                            ),
                    )
                }),
            )
            .await
            .expect("seed one boundary tie");

        for (order, skip, limit, expected) in [
            ("ASC, n.id DESC", 0, 1, vec![33]),
            ("ASC, n.id DESC", 2, 3, vec![31, 30, 29]),
            ("DESC, n.id ASC", 2, 3, vec![3, 4, 5]),
            ("DESC", 0, 1, vec![33]),
            ("ASC", 0, 1, vec![1]),
        ] {
            let result = shard
                .execute_cypher_rows(
                    QueryContext::new("cell-a", "experimental-ordered-window-large-tie")
                        .with_cypher_engine(CypherEngineMode::Experimental),
                    &format!(
                        "MATCH (n:Entity) WHERE n.created_at STARTS WITH '' \
                     RETURN n.id AS id ORDER BY n.created_at {order} SKIP {skip} LIMIT {limit}"
                    ),
                )
                .await
                .expect("retain only the window, not all 33 tied rows");
            let actual = result
                .rows
                .iter()
                .map(|row| match row.values[0] {
                    QueryValue::VertexId(id) => id,
                    ref other => panic!("unexpected ID: {other:?}"),
                })
                .collect::<Vec<_>>();
            assert_eq!(actual, expected);
        }

        let error = shard.execute_cypher_rows(
            QueryContext::new("cell-a", "ordered-window-row-budget")
                .with_cypher_engine(CypherEngineMode::Experimental),
            "MATCH (n:Entity) WHERE n.created_at STARTS WITH '' RETURN n.id AS id ORDER BY n.created_at ASC, n.id DESC LIMIT 33",
        ).await.expect_err("real retained rows must still obey the row budget");
        assert!(
            error.to_string().contains("actual 33 exceeds limit 32"),
            "{error}"
        );

        shard.close().await.expect("close graph shard");
    }

    #[tokio::test]
    async fn experimental_ordered_window_huge_limit_tiny_graph() {
        let shard = shard_with_limits(
            "graph/experimental-cypher-huge-window",
            GraphLimits {
                max_query_intermediate_rows: 8,
                ..GraphLimits::default()
            },
        )
        .await;
        shard
            .set_vertex_metadata_batch(
                "cell-a",
                (1..=5).map(|id| {
                    (
                        id,
                        VertexMetadata::default()
                            .with_label("Entity")
                            .with_property(
                                "created_at",
                                VertexPropertyValue::String("same".into()),
                            ),
                    )
                }),
            )
            .await
            .unwrap();
        for (skip, limit, expected) in [(0, 1_000_000_000, 5), (1_000_000_000, 10, 0), (0, 0, 0)] {
            let result = shard.execute_cypher_rows(
                QueryContext::new("cell-a", "huge-window").with_cypher_engine(CypherEngineMode::Experimental),
                &format!("MATCH (n:Entity) WHERE n.created_at STARTS WITH '' RETURN n.id AS id ORDER BY n.created_at ASC, n.id DESC SKIP {skip} LIMIT {limit}"),
            ).await.expect("reserve only for actual rows");
            assert_eq!(result.rows.len(), expected);
        }
        shard.close().await.unwrap();
    }

    #[tokio::test]
    async fn experimental_ordered_window_discarded_ties_count_against_scan_budget() {
        let shard = shard_with_limits(
            "graph/experimental-cypher-tie-scan-budget",
            GraphLimits {
                max_query_index_candidates: 4,
                ..GraphLimits::default()
            },
        )
        .await;
        shard
            .set_vertex_metadata_batch(
                "cell-a",
                (1..=5).map(|id| {
                    (
                        id,
                        VertexMetadata::default()
                            .with_label("Entity")
                            .with_property(
                                "created_at",
                                VertexPropertyValue::String("same".into()),
                            ),
                    )
                }),
            )
            .await
            .unwrap();
        let error = shard.execute_cypher_rows(
            QueryContext::new("cell-a", "tie-scan-budget").with_cypher_engine(CypherEngineMode::Experimental),
            "MATCH (n:Entity) WHERE n.created_at STARTS WITH '' RETURN n.id AS id ORDER BY n.created_at ASC, n.id DESC LIMIT 1",
        ).await.expect_err("discarding a candidate must not bypass scan accounting");
        assert!(
            error
                .to_string()
                .contains("experimental_ordered_vertex_property_candidates"),
            "{error}"
        );
        shard.close().await.unwrap();
    }

    /// A walk spread over many pages spends one allowance, not one per page.
    ///
    /// `max_query_index_candidates` is what stops a query whose residual the
    /// index cannot prove from reading an entire index. The walk asks for one
    /// page per call and resumes with a cursor, so while the tally lived in the
    /// page it started at zero on every page: no page came close to the limit,
    /// the query as a whole blew past it, and nothing but the runtime deadline
    /// ended the walk. Legacy reads the same query in one pass and fails.
    ///
    /// The residual is `<>` rather than `=` on purpose: an equality would reach
    /// the selective-equality probe, which answers the query by seeking instead
    /// of walking and never reads far enough to charge anything.
    #[tokio::test]
    async fn experimental_ordered_window_counts_index_entries_across_pages() {
        const VERTICES: u64 = 200;
        // Above any single page this walk can ask for -- `LIMIT 1` grows a page
        // to at most 64 entries -- and well below what walking all 200 costs.
        // Only a tally shared by the pages can reach it.
        const CANDIDATE_LIMIT: usize = 100;
        let shard = shard_with_limits(
            "graph/experimental-cypher-ordered-window-paged-candidates",
            GraphLimits {
                max_query_index_candidates: CANDIDATE_LIMIT,
                ..GraphLimits::default()
            },
        )
        .await;
        shard
            .set_vertex_metadata_batch(
                "cell-a",
                (1..=VERTICES).map(|vertex_id| {
                    (
                        vertex_id,
                        VertexMetadata::default()
                            .with_label("Entity")
                            .with_property(
                                "created_at",
                                VertexPropertyValue::String(format!("2026-01-{vertex_id:03}")),
                            )
                            // One tenant, so a residual naming any other rejects
                            // every row the index hands back and the walk keeps
                            // asking for pages until something stops it.
                            .with_property(
                                "tenant_id",
                                VertexPropertyValue::String("t0".to_string()),
                            ),
                    )
                }),
            )
            .await
            .expect("seed a window a residual rejects");

        let query = "MATCH (n:Entity) WHERE n.created_at STARTS WITH '2026' \
                     AND n.tenant_id <> 't0' RETURN n.id AS id ORDER BY n.created_at LIMIT 1";
        let error = shard
            .execute_cypher_rows(
                QueryContext::new("cell-a", "ordered-window-paged-candidates")
                    .with_cypher_engine(CypherEngineMode::Experimental),
                query,
            )
            .await
            .expect_err("a paged walk must not spend the candidate limit once per page");
        assert!(
            error
                .to_string()
                .contains("experimental_ordered_vertex_property_candidates"),
            "{error}"
        );

        // The same walk is not otherwise broken: given the allowance it needs it
        // reads the window out and answers with no rows. Without this the test
        // would pass on any error at all.
        let generous = shard_with_limits(
            "graph/experimental-cypher-ordered-window-paged-candidates-generous",
            GraphLimits::default(),
        )
        .await;
        generous
            .set_vertex_metadata_batch(
                "cell-a",
                (1..=VERTICES).map(|vertex_id| {
                    (
                        vertex_id,
                        VertexMetadata::default()
                            .with_label("Entity")
                            .with_property(
                                "created_at",
                                VertexPropertyValue::String(format!("2026-01-{vertex_id:03}")),
                            )
                            .with_property(
                                "tenant_id",
                                VertexPropertyValue::String("t0".to_string()),
                            ),
                    )
                }),
            )
            .await
            .expect("seed the same window again");
        let result = generous
            .execute_cypher_rows(
                QueryContext::new("cell-a", "ordered-window-paged-candidates-generous")
                    .with_cypher_engine(CypherEngineMode::Experimental),
                query,
            )
            .await
            .expect("the walk itself is sound");
        assert!(result.rows.is_empty(), "{:?}", result.rows);

        shard.close().await.unwrap();
        generous.close().await.unwrap();
    }

    #[tokio::test]
    async fn experimental_route_expands_a_structural_relationship_on_one_snapshot() {
        let shard = shard("graph/experimental-cypher-expand").await;
        shard
            .set_vertex_metadata(
                "cell-a",
                1,
                VertexMetadata::default()
                    .with_label("Entity")
                    .with_property("entity_id", VertexPropertyValue::String("a".to_string())),
            )
            .await
            .unwrap();
        shard
            .set_vertex_metadata(
                "cell-a",
                2,
                VertexMetadata::default()
                    .with_label("Person")
                    .with_property("name", VertexPropertyValue::String("Ada".to_string())),
            )
            .await
            .unwrap();
        shard
            .write_edge(EdgeMutation {
                cell_id: "cell-a".to_string(),
                edge_type: "KNOWS".to_string(),
                src: 1,
                dst: 2,
                idempotency_key: "experimental-expand-edge".to_string(),
            })
            .await
            .unwrap();

        let result = shard
            .execute_cypher_rows(
                QueryContext::new("cell-a", "experimental-expand")
                    .with_parameter("id", VertexPropertyValue::String("a".to_string()))
                    .with_cypher_engine(CypherEngineMode::Experimental),
                "MATCH (a:Entity)-[:KNOWS]->(b:Person) \
                 WHERE a.entity_id = $id RETURN b.name AS name",
            )
            .await
            .expect("execute experimental relationship expansion");
        assert_eq!(result.columns, vec![QueryColumn::new("name")]);
        assert_eq!(
            result.rows,
            vec![QueryRow::new(vec![QueryValue::Property(
                VertexPropertyValue::String("Ada".to_string())
            )])]
        );
        let metrics = shard.graph_operational_metrics();
        assert_eq!(metrics.query_rows_started, 1);
        assert_eq!(metrics.query_rows_completed, 1);
        assert_eq!(metrics.query_rows_failed, 0);
        assert_eq!(metrics.query_rows_returned, 1);
        assert_eq!(metrics.query_rows_latency.count(), 1);
        assert_eq!(metrics.query_experimental_property_seek_requests, 1);
        assert_eq!(metrics.query_experimental_relationship_expand_requests, 1);

        shard.close().await.expect("close graph shard");
    }

    #[tokio::test]
    async fn experimental_outgoing_expand_stops_at_storage_scan_work_limit() {
        let shard = shard_with_limits(
            "graph/experimental-cypher-expand-work-limit",
            GraphLimits {
                max_query_scan_edges: 2,
                ..GraphLimits::default()
            },
        )
        .await;
        shard
            .set_vertex_metadata(
                "cell-a",
                1,
                VertexMetadata::default()
                    .with_label("Entity")
                    .with_property("entity_id", VertexPropertyValue::String("a".to_string())),
            )
            .await
            .unwrap();
        for target in 2..=4 {
            shard
                .set_vertex_metadata(
                    "cell-a",
                    target,
                    VertexMetadata::default().with_label("Person"),
                )
                .await
                .unwrap();
            shard
                .write_edge(EdgeMutation {
                    cell_id: "cell-a".to_string(),
                    edge_type: "KNOWS".to_string(),
                    src: 1,
                    dst: target,
                    idempotency_key: format!("experimental-work-limit-{target}"),
                })
                .await
                .unwrap();
        }

        let error = shard
            .execute_cypher_rows(
                QueryContext::new("cell-a", "experimental-expand-work-limit")
                    .with_parameter("id", VertexPropertyValue::String("a".to_string()))
                    .with_cypher_engine(CypherEngineMode::Experimental),
                "MATCH (a:Entity)-[:KNOWS]->(b:Person) \
                 WHERE a.entity_id = $id RETURN b",
            )
            .await
            .expect_err("the outgoing prefix scan must obey max_query_scan_edges");
        assert!(
            error
                .to_string()
                .contains("query_out_neighbors_storage_records"),
            "unexpected error: {error}"
        );

        let metrics = shard.graph_operational_metrics();
        assert_eq!(metrics.query_rows_started, 1);
        assert_eq!(metrics.query_rows_completed, 0);
        assert_eq!(metrics.query_rows_failed, 1);

        shard.close().await.expect("close graph shard");
    }

    /// An undirected hop runs as two storage orientations, so the two share
    /// one scan allowance: three edges out and three back is six visits
    /// against a limit of four, and the second orientation crosses the line.
    /// With a tally local to each `expand_relationships` call, three passed
    /// twice and the query scanned six under a limit of four.
    #[tokio::test]
    async fn an_undirected_hop_spends_one_scan_allowance_across_both_orientations() {
        let shard = shard_with_limits(
            "graph/experimental-cypher-undirected-scan-allowance",
            GraphLimits {
                max_query_scan_edges: 4,
                ..GraphLimits::default()
            },
        )
        .await;
        shard
            .set_vertex_metadata(
                "cell-a",
                1,
                VertexMetadata::default()
                    .with_label("Entity")
                    .with_property("entity_id", VertexPropertyValue::String("a".to_string())),
            )
            .await
            .unwrap();
        for target in 2..=4 {
            shard
                .set_vertex_metadata(
                    "cell-a",
                    target,
                    VertexMetadata::default().with_label("Person"),
                )
                .await
                .unwrap();
            shard
                .write_edge(EdgeMutation {
                    cell_id: "cell-a".to_string(),
                    edge_type: "KNOWS".to_string(),
                    src: 1,
                    dst: target,
                    idempotency_key: format!("undirected-allowance-out-{target}"),
                })
                .await
                .unwrap();
            shard
                .write_edge(EdgeMutation {
                    cell_id: "cell-a".to_string(),
                    edge_type: "KNOWS".to_string(),
                    src: target,
                    dst: 1,
                    idempotency_key: format!("undirected-allowance-in-{target}"),
                })
                .await
                .unwrap();
        }

        let error = shard
            .execute_cypher_rows(
                QueryContext::new("cell-a", "experimental-undirected-allowance")
                    .with_parameter("id", VertexPropertyValue::String("a".to_string()))
                    .with_cypher_engine(CypherEngineMode::Experimental),
                "MATCH (a:Entity)-[:KNOWS]-(b:Person) WHERE a.entity_id = $id RETURN b",
            )
            .await
            .expect_err("both orientations together must obey max_query_scan_edges");
        assert!(
            error
                .to_string()
                .contains("experimental_cypher_expand_scanned_edges"),
            "unexpected error: {error}"
        );

        shard.close().await.expect("close graph shard");
    }

    #[tokio::test]
    async fn experimental_execution_failures_finish_the_query_metric_lifecycle() {
        let shard = shard("graph/experimental-cypher-metrics-failure").await;
        let error = shard
            .execute_cypher_rows(
                QueryContext::new("cell-a", "experimental-metrics-failure")
                    .with_cypher_engine(CypherEngineMode::Experimental),
                "MATCH (a {id: 1})-[r]->(e) RETURN e",
            )
            .await
            .expect_err("untyped relationship expansion remains unsupported");
        assert!(
            error.to_string().contains("relationship"),
            "unexpected execution error: {error}"
        );

        let metrics = shard.graph_operational_metrics();
        assert_eq!(metrics.query_rows_started, 1);
        assert_eq!(metrics.query_rows_completed, 0);
        assert_eq!(metrics.query_rows_failed, 1);
        assert_eq!(metrics.query_rows_failed_by_class.iter().sum::<u64>(), 1);
        assert_eq!(metrics.query_rows_returned, 0);
        assert_eq!(metrics.query_rows_latency.count(), 1);

        shard.close().await.expect("close graph shard");
    }

    /// Production query (fingerprint `0469bd3696a49813`) that both engines
    /// rejected: Cypher 25 fell back to an untyped statement at `IS NULL`, and
    /// the legacy lowering had no arm for it. A missing property and a stored
    /// empty string both count as "not superseded"; a stored id does not.
    #[tokio::test]
    async fn is_null_predicate_matches_legacy_on_the_superseded_relationship_query() {
        let shard = shard("graph/experimental-is-null-superseded").await;
        for (id, name) in [(1, "alpha"), (2, "beta"), (3, "gamma"), (4, "delta")] {
            shard
                .set_vertex_metadata(
                    "cell-a",
                    id,
                    VertexMetadata::default()
                        .with_label("Entity")
                        .with_property("name", VertexPropertyValue::String(name.to_string())),
                )
                .await
                .expect("write vertex metadata");
        }
        for (src, dst, timestamp, relationship_id, superseded_by) in [
            (1, 2, 30, "r1", None),
            (1, 3, 20, "r2", Some("")),
            (2, 3, 10, "r3", Some("r9")),
            (3, 4, 40, "r4", None),
        ] {
            shard
                .write_edge(EdgeMutation {
                    cell_id: "cell-a".to_string(),
                    edge_type: "RELATES".to_string(),
                    src,
                    dst,
                    idempotency_key: format!("is-null-edge-{relationship_id}"),
                })
                .await
                .expect("write edge");
            let mut metadata = EdgeMetadata::default()
                .with_property("timestamp", VertexPropertyValue::Integer(timestamp))
                .with_property(
                    "relationship_id",
                    VertexPropertyValue::String(relationship_id.to_string()),
                )
                .with_property(
                    "chunk_id",
                    VertexPropertyValue::String(format!("chunk-{relationship_id}")),
                );
            if let Some(superseded_by) = superseded_by {
                metadata = metadata.with_property(
                    "superseded_by",
                    VertexPropertyValue::String(superseded_by.to_string()),
                );
            }
            shard
                .set_edge_metadata("cell-a", "RELATES", src, dst, metadata)
                .await
                .expect("write edge metadata");
        }

        let query = "MATCH (src:Entity)-[r:RELATES]->(tgt:Entity)
WHERE r.timestamp >= 0 AND (r.superseded_by IS NULL OR r.superseded_by = '')
RETURN r.chunk_id AS chunk_id,
       src.name AS source_name, src.type AS source_type,
       src.namespace AS source_namespace, src.identifier AS source_identifier,
       src.entity_id AS source_entity_id,
       tgt.name AS target_name, tgt.type AS target_type,
       tgt.namespace AS target_namespace, tgt.identifier AS target_identifier,
       tgt.entity_id AS target_entity_id,
       r.canonical_relation AS canonical_relation, r.raw_relation AS raw_relation,
       r.chunk_id AS rel_chunk_id, r.metadata AS metadata,
       r.relationship_id AS relationship_id, r.superseded_by AS superseded_by,
       r.timestamp AS timestamp
ORDER BY r.timestamp DESC, r.relationship_id ASC LIMIT $limit";
        let relationship_ids = |result: &crate::QueryResultSet| {
            result
                .rows
                .iter()
                .map(|row| row.values[15].clone())
                .collect::<Vec<_>>()
        };
        let expected = ["r4", "r1", "r2"]
            .map(|id| QueryValue::Property(VertexPropertyValue::String(id.to_string())));

        let mut results = Vec::new();
        for engine in [CypherEngineMode::Legacy, CypherEngineMode::Experimental] {
            let result = shard
                .execute_cypher_rows(
                    QueryContext::new("cell-a", "is-null-superseded")
                        .with_parameter("limit", VertexPropertyValue::Integer(10))
                        .with_cypher_engine(engine),
                    query,
                )
                .await
                .unwrap_or_else(|error| panic!("{engine:?} rejected IS NULL: {error}"));
            assert_eq!(relationship_ids(&result), expected, "{engine:?}");
            results.push(result);
        }
        assert_eq!(results[0].columns, results[1].columns);
        assert_eq!(results[0].rows, results[1].rows);

        // The negated form, which the same parsers dropped the same way.
        let not_null = "MATCH (src:Entity)-[r:RELATES]->(tgt:Entity) \
                        WHERE r.superseded_by IS NOT NULL \
                        RETURN r.relationship_id AS id ORDER BY id";
        for engine in [CypherEngineMode::Legacy, CypherEngineMode::Experimental] {
            let result = shard
                .execute_cypher_rows(
                    QueryContext::new("cell-a", "is-not-null-superseded")
                        .with_cypher_engine(engine),
                    not_null,
                )
                .await
                .unwrap_or_else(|error| panic!("{engine:?} rejected IS NOT NULL: {error}"));
            assert_eq!(
                result.rows,
                ["r2", "r3"]
                    .map(|id| QueryRow::new(vec![QueryValue::Property(
                        VertexPropertyValue::String(id.to_string())
                    )]))
                    .to_vec(),
                "{engine:?}"
            );
        }

        // A NULL literal operand: constant true under IS NULL, constant false
        // under IS NOT NULL, and unknown -- so no rows -- under `=`.
        for (predicate, expected) in [
            ("NULL IS NULL", vec!["r1", "r2", "r3", "r4"]),
            ("NULL IS NOT NULL", Vec::new()),
            ("r.superseded_by = NULL", Vec::new()),
        ] {
            let query = format!(
                "MATCH (src:Entity)-[r:RELATES]->(tgt:Entity) WHERE {predicate} \
                 RETURN r.relationship_id AS id ORDER BY id"
            );
            for engine in [CypherEngineMode::Legacy, CypherEngineMode::Experimental] {
                let result = shard
                    .execute_cypher_rows(
                        QueryContext::new("cell-a", "null-literal").with_cypher_engine(engine),
                        &query,
                    )
                    .await
                    .unwrap_or_else(|error| panic!("{engine:?} rejected {predicate}: {error}"));
                assert_eq!(
                    result.rows,
                    expected
                        .iter()
                        .map(|id| QueryRow::new(vec![QueryValue::Property(
                            VertexPropertyValue::String(id.to_string())
                        )]))
                        .collect::<Vec<_>>(),
                    "{engine:?}: {predicate}"
                );
            }
        }
        shard.close().await.expect("close graph shard");
    }

    /// A name no pattern declares is not a null binding: it is a query that
    /// cannot mean what it says. Reading it as null would make `IS NULL`
    /// match every row and `IS NOT NULL` match none, silently -- which is how
    /// a typo becomes a wrong answer instead of an error.
    #[tokio::test]
    async fn an_undeclared_binding_is_rejected_by_both_engines() {
        let shard = shard("graph/undeclared-binding-null-predicate").await;
        shard
            .set_vertex_metadata("cell-a", 1, VertexMetadata::default().with_label("Entity"))
            .await
            .expect("write vertex metadata");

        for predicate in ["typo IS NULL", "typo IS NOT NULL"] {
            for engine in [CypherEngineMode::Experimental, CypherEngineMode::Legacy] {
                let error = shard
                    .execute_cypher_rows(
                        QueryContext::new("cell-a", "undeclared-binding")
                            .with_cypher_engine(engine),
                        &format!("MATCH (a:Entity) WHERE {predicate} RETURN a.id AS id"),
                    )
                    .await
                    .expect_err("an undeclared binding is not a null binding");
                assert!(
                    error.to_string().contains("unbound variable typo"),
                    "{engine:?}: {predicate}: {error}"
                );
            }
        }

        // A property of an undeclared binding was already refused by both,
        // each in its own words; only the bare binding read as null.
        for engine in [CypherEngineMode::Experimental, CypherEngineMode::Legacy] {
            shard
                .execute_cypher_rows(
                    QueryContext::new("cell-a", "undeclared-binding-property")
                        .with_cypher_engine(engine),
                    "MATCH (a:Entity) WHERE typo.name = 'a' RETURN a.id AS id",
                )
                .await
                .expect_err("a property of an undeclared binding is refused");
        }

        // The binding the query does declare still answers, on both engines.
        for engine in [CypherEngineMode::Experimental, CypherEngineMode::Legacy] {
            let result = shard
                .execute_cypher_rows(
                    QueryContext::new("cell-a", "declared-binding").with_cypher_engine(engine),
                    "MATCH (a:Entity) WHERE a IS NOT NULL RETURN a.id AS id",
                )
                .await
                .unwrap_or_else(|error| panic!("{engine:?} rejected a declared binding: {error}"));
            assert_eq!(
                result.rows,
                vec![QueryRow::new(vec![QueryValue::VertexId(1)])]
            );
        }
        shard.close().await.expect("close graph shard");
    }

    #[tokio::test]
    async fn is_null_on_a_bound_relationship_matches_both_engines() {
        let shard = shard("graph/experimental-is-null-relationship-binding").await;
        for id in [1, 2] {
            shard
                .set_vertex_metadata("cell-a", id, VertexMetadata::default().with_label("Entity"))
                .await
                .expect("write vertex metadata");
        }
        shard
            .write_edge(EdgeMutation {
                cell_id: "cell-a".to_string(),
                edge_type: "RELATES".to_string(),
                src: 1,
                dst: 2,
                idempotency_key: "is-null-relationship-binding".to_string(),
            })
            .await
            .expect("write relationship");

        for (predicate, expected) in [
            (
                "r IS NOT NULL",
                vec![QueryRow::new(vec![QueryValue::VertexId(2)])],
            ),
            ("r IS NULL", Vec::new()),
        ] {
            let query = format!(
                "MATCH (a:Entity)-[r:RELATES]->(b:Entity) WHERE {predicate} RETURN b.id AS id"
            );
            for engine in [CypherEngineMode::Experimental, CypherEngineMode::Legacy] {
                let result = shard
                    .execute_cypher_rows(
                        QueryContext::new("cell-a", "is-null-relationship-binding")
                            .with_cypher_engine(engine),
                        &query,
                    )
                    .await
                    .unwrap_or_else(|error| panic!("{engine:?} rejected {predicate}: {error}"));
                assert_eq!(result.rows, expected, "{engine:?}: {predicate}");
            }
        }
        shard.close().await.expect("close graph shard");
    }
}
