use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use async_trait::async_trait;
use futures::{stream, StreamExt as _, TryStreamExt as _};
use hydradb_cypher_engine::{
    BoundedVertexIds, CypherEngine, ExpandDirection, ExpandNeighbor, ExpandRequest, GraphRead,
    GraphStorage, OrderedPropertyScanPage, OrderedPropertyScanRequest,
    OrderedRelationshipPropertyScanPage, OrderedRelationshipPropertyScanRequest, OrderedScanCursor,
    OrderedScanPosition, PropertyEqualityCandidate, QueryResult, QueryValue as EngineQueryValue,
    ReadRequest, RelationshipRecord as EngineRelationshipRecord, ScalarValue,
    StatisticsAccessReport, StatisticsProvider, StorageError, StorageResult,
    VertexRecord as EngineVertexRecord,
};
use tracing::Instrument;

use crate::query::experimental_cypher::{
    experimental_engine_error, experimental_query_error, is_experimental_explain,
    lower_experimental_cypher_timed, property_into_scalar, property_to_scalar,
};
use crate::QueryFailureReason;

use super::*;

mod observability;
mod ordered_relationship_walk;
mod ordered_walk;
mod scope_statistics;
mod statistics;

use observability::{PlanIdentity, RequestObservation};
pub(crate) use scope_statistics::{
    PlannerStatisticsCache, PlannerStatisticsHintFailure, PlannerStatisticsHints,
};

/// Upper bound on the estimated bytes the per-shard statistics memo retains.
pub(crate) const STATISTICS_MEMO_MAX_BYTES: usize = 8 * 1024 * 1024;

/// How many index seeks of one multi-value predicate run at once. Bounded so
/// a large `IN` list cannot fan out into unbounded concurrent object-store
/// reads; matches the statistics loader's read concurrency.
const MULTI_SEEK_CONCURRENCY: usize = 8;

/// Bound object-store reads during relationship expansion. `buffered` keeps
/// the original input order, which also keeps structural relationship IDs
/// stable for queries that return them.
const RELATIONSHIP_EXPAND_CONCURRENCY: usize = 16;

/// Candidate allowance shared out across the equality candidates of a runtime
/// access-path probe's first pass. Matches the legacy row engine's unordered
/// probe, so a property that anchors a query well on one engine anchors it on
/// the other, and catches the common case: legacy's own post-probe fetch count
/// has a median of 40.
const EQUALITY_PROBE_ENTRIES: usize = 256;

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
            feature: "historical graph epochs are not storage snapshots; execute against a current SlateDB snapshot".to_string(),
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

    // Capability, before anything reads. The guarantee has two tiers, and it is
    // worth being exact about which applies to what:
    //
    //   * A shape this check settles has read nothing at all. No snapshot, no
    //     statistics, zero storage requests of either kind.
    //   * A shape only the physical check settles -- one that depends on the
    //     operators the planner chose -- costs a snapshot and the statistics
    //     point lookups planning needed, and nothing more. It never executes,
    //     so it issues no scan.
    //
    // We want the answer "after lowering and planning, before storage
    // execution begins", which is what the second tier is: planning needs
    // statistics, and statistics are point lookups. Both tiers are pinned by
    // tests below, because "no storage request" read alone would over-claim the
    // second one.
    let capability = logical.capability();
    if let Some(kind) = capability.unsupported_kind() {
        // The token, not the query text: bounded cardinality, no literals.
        // Turning this into a counter is future work; a span field costs
        // nothing until somebody is looking.
        tracing::debug!(
            hydradb.query.experimental.unsupported = kind,
            "experimental engine cannot plan this shape"
        );
    }
    capability
        .into_result()
        .map_err(experimental_engine_error)?;

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
        // With the flag on, the indexer's counts plan this query and the
        // per-query statistics reads never happen: an unpublished, corrupt or
        // unreadable object plans with no statistics at all rather than
        // falling back to point lookups. With the flag off, planning is
        // exactly what it was before they existed.
        let planner_statistics = shard
            .planner_statistics_for_query(&context.cell_id, &budget, explain)
            .await;
        stages.planner_statistics = planner_statistics.as_deref().map(|statistics| {
            (
                statistics.published.storage_sequence,
                statistics.published.built_at_ms,
            )
        });
        let statistics: Arc<dyn StatisticsProvider> = match planner_statistics {
            Some(statistics) => statistics,
            None if shard.planner_statistics.enabled => {
                Arc::new(hydradb_cypher_engine::UnknownStatistics)
            }
            None => Arc::new(
                statistics::SnapshotStatistics::load(
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
                .await?,
            ),
        };
        stages.statistics = stage_started.elapsed();
        let plan_started = std::time::Instant::now();
        budget.check("experimental_cypher_physical_plan")?;
        let prepared = {
            let _span = tracing::info_span!("query.experimental.plan").entered();
            logical
                .plan_with_statistics(statistics.as_ref())
                .map_err(experimental_engine_error)
        };
        stages.plan = plan_started.elapsed();
        prepared.map(|prepared| (prepared, statistics))
    })
    .await?;
    // The second half of the capability answer: what only the chosen physical
    // operators settle. Still before `HydraDbCypherStorage` exists, so a
    // refusal here costs planning and nothing more.
    prepared
        .capability()
        .into_result()
        .map_err(experimental_engine_error)?;
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
    stages.collect_profile(&prepared.physical, statistics.as_ref());
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

    fn record_full_scan(&self) {
        observability::record_full_scan();
    }

    fn record_retained_rows(&self, rows: usize) {
        observability::record_retained_rows(rows);
    }

    fn max_intermediate_rows(&self) -> Option<usize> {
        Some(self.shard.limits.max_query_intermediate_rows)
    }

    fn record_hydrated_vertices(&self, vertices: usize) {
        observability::record_hydrated_vertices(vertices);
    }

    fn record_statistics_access(&self, report: &StatisticsAccessReport) {
        observability::record_statistics_access(report);
    }

    fn max_query_index_candidates(&self) -> Option<usize> {
        Some(self.shard.limits.max_query_index_candidates)
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
                        let ids = shard
                            .scan_vertex_property_index_at(
                                cell_id, property, &value, read_epoch, budget,
                            )
                            .await?;
                        // `scan_vertex_property_index_at` bounds one value's
                        // own candidates. Nothing bounded their union, so an
                        // `IN` list of n values could read n times the ceiling
                        // a single seek is held to -- and the list's own limit
                        // was all that capped n. Charged to the query's shared
                        // tally instead, which is what the legacy route does
                        // by counting the whole candidate set at once.
                        //
                        // Charged as each seek lands rather than after the
                        // batch, so `try_collect` abandons the rest instead of
                        // paying for every value and failing on the total.
                        shard.charge_query_index_candidates(
                            "cypher_in_list_index_candidates",
                            budget,
                            ids.len() as u64,
                        )?;
                        Ok(ids)
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
                let page = storage_result(
                    GraphStore::scope_snapshot(
                        Arc::clone(&self.snapshot),
                        self.shard.scan_ordered_vertex_property_page_at(
                            self.cell_id,
                            request,
                            self.read_epoch,
                            &self.budget,
                        ),
                    )
                    .await,
                )?;
                Ok(OrderedPropertyScanPage {
                    vertices: page
                        .rows
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
                    // The walk already speaks property values, so unlike the
                    // string-only path this needs no key decoding.
                    last: page
                        .last
                        .map(|(value, vertex_id)| OrderedScanPosition { value, vertex_id }),
                    exhausted: page.exhausted,
                    index_entries: Some(page.index_entries),
                })
            },
            |result: &StorageResult<OrderedPropertyScanPage>| {
                result.as_ref().map_or(0, |page| page.vertices.len())
            },
        )
        .await
    }

    async fn scan_relationships_by_property_ordered(
        &mut self,
        request: &OrderedRelationshipPropertyScanRequest,
    ) -> StorageResult<OrderedRelationshipPropertyScanPage> {
        traced_operator(
            "scan_relationships_by_property_ordered",
            operator_span("scan_relationships_by_property_ordered"),
            async {
                let page = storage_result(
                    GraphStore::scope_snapshot(
                        Arc::clone(&self.snapshot),
                        self.shard.scan_ordered_relationship_property_page_at(
                            self.cell_id,
                            request,
                            self.read_epoch,
                            &self.budget,
                        ),
                    )
                    .await,
                )?;
                // Structural edges get the same session-stable identities
                // expansion gives them.
                let mut used_ids = page
                    .rows
                    .iter()
                    .filter_map(|row| row.relationship_id)
                    .chain(self.structural_relationship_ids.values().copied())
                    .collect::<BTreeSet<_>>();
                let mut rows = Vec::with_capacity(page.rows.len());
                for row in page.rows {
                    let id = match row.relationship_id {
                        Some(id) => id,
                        None => self.structural_relationship_id(
                            (request.relationship_type.clone(), row.src, row.dst),
                            &mut used_ids,
                        )?,
                    };
                    rows.push(EngineRelationshipRecord {
                        id,
                        source: row.src,
                        target: row.dst,
                        relationship_type: request.relationship_type.clone(),
                        properties: row
                            .metadata
                            .properties
                            .into_iter()
                            .map(|(name, value)| (name, property_into_scalar(value)))
                            .collect(),
                    });
                }
                Ok(OrderedRelationshipPropertyScanPage {
                    relationships: rows,
                    last: page.last.map(OrderedScanCursor),
                    exhausted: page.exhausted,
                })
            },
            |result: &StorageResult<OrderedRelationshipPropertyScanPage>| {
                result.as_ref().map_or(0, |page| page.relationships.len())
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

    /// The guarded seek of a statistics-chosen equality. Every value walks
    /// its index drawing on one allowance of `max_index_entries`, so the
    /// values between them read at most that many entries, and the first
    /// value to find the allowance spent declines the whole seek without
    /// waiting for the rest.
    ///
    /// Like the probe, these reads are not charged to the query's shared
    /// candidate tally: a declined guard hands over to the plan chosen
    /// without statistics, which must still be able to spend the whole
    /// allowance it has today. The limit is capped at
    /// `max_query_index_candidates`, the most a single seek may read, so a
    /// large estimate declines where a plain seek would have failed.
    async fn seek_vertices_by_property_values_bounded(
        &mut self,
        property: &str,
        values: &[ScalarValue],
        max_index_entries: usize,
    ) -> StorageResult<BoundedVertexIds> {
        traced_operator(
            "seek_vertices_by_property_values_bounded",
            operator_span("seek_vertices_by_property_values_bounded"),
            async {
                // A value the index cannot hold -- NaN, an infinity, an
                // integer outside i64/u64 -- declines rather than failing:
                // the fallback's Filter decides it, as it does today.
                let Ok(values) = values
                    .iter()
                    .map(scalar_to_property)
                    .collect::<StorageResult<Vec<_>>>()
                else {
                    return Ok(BoundedVertexIds::declined(0));
                };
                for _ in &values {
                    self.shard
                        .operation_metrics
                        .record_experimental_property_seek();
                }
                let limit = max_index_entries.min(self.shard.limits.max_query_index_candidates);
                let allowance = std::sync::atomic::AtomicUsize::new(limit);
                let refused = std::sync::atomic::AtomicUsize::new(0);
                let shard = self.shard;
                let cell_id = self.cell_id;
                let budget = &self.budget;
                let allowance = &allowance;
                let refused = &refused;
                let seeks = async move {
                    let mut seeks = stream::iter(values)
                        .map(|value| async move {
                            shard
                                .try_scan_vertex_property_value_within(
                                    cell_id,
                                    property,
                                    &value,
                                    budget,
                                    allowance,
                                    refused,
                                    super::query::QUERY_NODE_EQUALITY_PROBE_ALTERNATIVES,
                                )
                                .await
                        })
                        .buffer_unordered(MULTI_SEEK_CONCURRENCY);
                    let mut ids = BTreeSet::new();
                    while let Some(seek) = seeks.next().await {
                        // One value past the allowance is the decline; the
                        // values still in flight are dropped unread.
                        let Some(seek) = seek? else {
                            return Ok(None);
                        };
                        ids.extend(seek);
                    }
                    Ok(Some(ids))
                };
                let ids = storage_result(
                    GraphStore::scope_snapshot(Arc::clone(&self.snapshot), seeks).await,
                )?;
                // The entries taken from the allowance, plus every read entry
                // the spent allowance refused: concurrent seeks can each read
                // one before finding it spent.
                let taken = (limit - allowance.load(std::sync::atomic::Ordering::Relaxed)) as u64;
                let refused = refused.load(std::sync::atomic::Ordering::Relaxed) as u64;
                Ok(match ids {
                    Some(ids) => BoundedVertexIds {
                        ids: Some(ids.into_iter().collect()),
                        index_entries: taken,
                    },
                    None => BoundedVertexIds::declined(taken.saturating_add(refused)),
                })
            },
            |result: &StorageResult<BoundedVertexIds>| {
                result
                    .as_ref()
                    .ok()
                    .and_then(|bounded| bounded.ids.as_ref().map(Vec::len))
                    .unwrap_or(0)
            },
        )
        .await
    }

    /// The guarded scan of a statistics-chosen label. Capped one short of
    /// `max_query_index_candidates`, because the label walk fails the query
    /// past that ceiling and a guard must decline instead.
    async fn scan_vertices_by_label_bounded(
        &mut self,
        label: &str,
        max_index_entries: usize,
    ) -> StorageResult<BoundedVertexIds> {
        let limit = max_index_entries.min(
            self.shard
                .limits
                .max_query_index_candidates
                .saturating_sub(1),
        );
        let ids = self
            .scan_vertices_by_label(label, Some(limit.saturating_add(1)))
            .await?;
        Ok(BoundedVertexIds::within(ids, limit))
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
                self.seeked_relationship_records(relationship_type, pending)
            },
            |result| result.as_ref().map_or(0, Vec::len),
        )
        .await
    }

    /// The single seek for several values at once: every value's index
    /// entries are read concurrently, and each endpoint pair any of them
    /// reached has its relationships read once, however many values led to
    /// it. A variant is kept when its stored value equals any of `values`.
    async fn seek_relationships_by_property_values(
        &mut self,
        relationship_type: &str,
        property: &str,
        values: &[ScalarValue],
    ) -> StorageResult<Vec<EngineRelationshipRecord>> {
        traced_operator(
            "seek_relationships_by_property_values",
            operator_span("seek_relationships_by_property_values"),
            async {
                let values = values
                    .iter()
                    .map(scalar_to_property)
                    .collect::<StorageResult<Vec<_>>>()?;
                // Compared as the single seek compares: through the stored
                // encoding, so `1` and `1.0` stay equal.
                let wanted = values.iter().map(property_to_scalar).collect::<Vec<_>>();
                let shard = self.shard;
                let cell_id = self.cell_id;
                let read_epoch = self.read_epoch;
                let budget = &self.budget;
                let index_reads = stream::iter(values)
                    .map(|value| async move {
                        let pairs = shard
                            .scan_edge_property_index_at(
                                cell_id,
                                relationship_type,
                                property,
                                &value,
                                read_epoch,
                                budget,
                            )
                            .await?;
                        // Each value's own read is bounded where it is made;
                        // their union is not, so it is charged to the query's
                        // shared tally as the vertex multi-seek charges its.
                        shard.charge_query_index_candidates(
                            "cypher_relationship_in_list_index_candidates",
                            budget,
                            pairs.len() as u64,
                        )?;
                        Ok(pairs)
                    })
                    .buffer_unordered(MULTI_SEEK_CONCURRENCY)
                    .try_collect::<Vec<Vec<(VertexId, VertexId)>>>();
                let pairs = storage_result(
                    GraphStore::scope_snapshot(Arc::clone(&self.snapshot), index_reads).await,
                )?
                .into_iter()
                .flatten()
                .collect::<BTreeSet<_>>();
                let variant_reads = stream::iter(pairs)
                    .map(|(source, target)| async move {
                        budget.check("experimental_cypher_relationship_property_seek")?;
                        let variants = shard
                            .native_path_relationships_at(
                                cell_id,
                                relationship_type,
                                source,
                                target,
                                read_epoch,
                                budget,
                            )
                            .await?;
                        Ok((source, target, variants))
                    })
                    .buffered(RELATIONSHIP_EXPAND_CONCURRENCY)
                    .try_collect::<Vec<_>>();
                let pairs = storage_result(
                    GraphStore::scope_snapshot(Arc::clone(&self.snapshot), variant_reads).await,
                )?;
                let mut pending = Vec::new();
                for (source, target, variants) in pairs {
                    for (id, metadata) in variants {
                        let matches = metadata.properties.get(property).is_some_and(|stored| {
                            let stored = property_to_scalar(stored);
                            wanted.iter().any(|value| {
                                hydradb_cypher_engine::scalar_values_equal(&stored, value)
                            })
                        });
                        if matches {
                            pending.push((id, source, target, metadata));
                        }
                    }
                }
                self.seeked_relationship_records(relationship_type, pending)
            },
            |result| result.as_ref().map_or(0, Vec::len),
        )
        .await
    }

    async fn probe_relationships_by_property_lower_bound(
        &mut self,
        relationship_type: &str,
        property: &str,
        lower: &ScalarValue,
        inclusive: bool,
        max_index_entries: usize,
    ) -> StorageResult<Option<Vec<EngineRelationshipRecord>>> {
        let ScalarValue::Integer(lower) = *lower else {
            return Ok(None);
        };
        traced_operator(
            "probe_relationships_by_property_lower_bound",
            operator_span("probe_relationships_by_property_lower_bound"),
            async {
                let pairs = storage_result(
                    GraphStore::scope_snapshot(
                        Arc::clone(&self.snapshot),
                        self.shard.try_scan_edge_property_lower_bound_at(
                            super::query::RelationshipLowerBoundProbe {
                                cell_id: self.cell_id,
                                edge_type: relationship_type,
                                property,
                                lower,
                                read_epoch: self.read_epoch,
                                budget: &self.budget,
                                max_entries: max_index_entries,
                            },
                        ),
                    )
                    .await,
                )?;
                let Some(pairs) = pairs else {
                    return Ok(None);
                };
                // Each candidate pair is an edge the expansion would have
                // visited, so it draws on the same per-read budget. A probe
                // that declines is not charged: its index walk is bounded by
                // `max_index_entries`, and the fallback charges its own reads.
                self.scanned_edges = self.scanned_edges.saturating_add(pairs.len() as u64);
                storage_result(ensure_limit(
                    "experimental_cypher_range_probe_scanned_edges",
                    self.scanned_edges,
                    self.shard.limits.max_query_scan_edges,
                ))?;
                let shard = self.shard;
                let cell_id = self.cell_id;
                let read_epoch = self.read_epoch;
                let budget = self.budget.clone();
                let snapshot = Arc::clone(&self.snapshot);
                let mut variant_reads = stream::iter(pairs)
                    .map(|(source, target)| {
                        let budget = budget.clone();
                        let snapshot = Arc::clone(&snapshot);
                        async move {
                            storage_result(budget.check("experimental_cypher_range_probe_pair"))?;
                            let variants = storage_result(
                                GraphStore::scope_snapshot(
                                    snapshot,
                                    shard.native_path_relationships_at(
                                        cell_id,
                                        relationship_type,
                                        source,
                                        target,
                                        read_epoch,
                                        &budget,
                                    ),
                                )
                                .await,
                            )?;
                            Ok((source, target, variants))
                        }
                    })
                    .buffered(RELATIONSHIP_EXPAND_CONCURRENCY);
                // An index entry names an endpoint pair, not a relationship,
                // so a pair's other variants arrive too. Those that cannot
                // satisfy the bound are dropped before they count toward the
                // row limit; the full predicate still runs over the rest.
                // Their identities stay reserved so structural IDs do not
                // depend on which variants the bound kept.
                let mut pending = Vec::new();
                let mut used_ids = self
                    .structural_relationship_ids
                    .values()
                    .copied()
                    .collect::<BTreeSet<_>>();
                while let Some((source, target, variants)) = variant_reads.try_next().await? {
                    for (id, metadata) in variants {
                        used_ids.extend(id);
                        if !may_satisfy_lower_bound(
                            metadata.properties.get(property),
                            lower,
                            inclusive,
                        ) {
                            continue;
                        }
                        pending.push((id, source, target, metadata));
                        self.check_intermediate_rows(
                            "experimental_cypher_range_probe_relationships",
                            pending.len(),
                        )?;
                    }
                }
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
                Ok(Some(relationships))
            },
            |result| {
                result
                    .as_ref()
                    .ok()
                    .and_then(Option::as_ref)
                    .map_or(0, Vec::len)
            },
        )
        .await
    }

    async fn probe_relationships_by_type(
        &mut self,
        relationship_type: &str,
        max_index_entries: usize,
    ) -> StorageResult<Option<Vec<EngineRelationshipRecord>>> {
        traced_operator(
            "probe_relationships_by_type",
            operator_span("probe_relationships_by_type"),
            async {
                let pairs = storage_result(
                    GraphStore::scope_snapshot(
                        Arc::clone(&self.snapshot),
                        self.shard
                            .try_scan_edge_type_at(super::query::RelationshipTypeProbe {
                                cell_id: self.cell_id,
                                edge_type: relationship_type,
                                read_epoch: self.read_epoch,
                                budget: &self.budget,
                                max_entries: max_index_entries,
                            }),
                    )
                    .await,
                )?;
                let Some(pairs) = pairs else {
                    return Ok(None);
                };
                // The pairs draw on the same per-read budget the expansion
                // spends, but unlike the range probe's candidates they are not
                // "edges the expansion would have visited": this reads a whole
                // type, while the expansion reads only edges incident to its
                // anchor. A selective anchor over a broader type therefore costs
                // the probe more than the plan it replaces.
                //
                // So exceeding the budget here means the probe is not viable,
                // not that the query is inadmissible. Decline and let the
                // expansion charge what it actually visits. Failing instead
                // would reject a query the fallback admits, which is the one
                // thing an access path chosen on a guess must never do. A
                // declined probe stays uncharged, as ever.
                let charged = self.scanned_edges.saturating_add(pairs.len() as u64);
                if charged > self.shard.limits.max_query_scan_edges {
                    return Ok(None);
                }
                self.scanned_edges = charged;
                let shard = self.shard;
                let cell_id = self.cell_id;
                let read_epoch = self.read_epoch;
                let budget = self.budget.clone();
                let snapshot = Arc::clone(&self.snapshot);
                let mut variant_reads = stream::iter(pairs)
                    .map(|(source, target)| {
                        let budget = budget.clone();
                        let snapshot = Arc::clone(&snapshot);
                        async move {
                            storage_result(budget.check("experimental_cypher_type_probe_pair"))?;
                            let variants = storage_result(
                                GraphStore::scope_snapshot(
                                    snapshot,
                                    shard.native_path_relationships_at(
                                        cell_id,
                                        relationship_type,
                                        source,
                                        target,
                                        read_epoch,
                                        &budget,
                                    ),
                                )
                                .await,
                            )?;
                            Ok((source, target, variants))
                        }
                    })
                    .buffered(RELATIONSHIP_EXPAND_CONCURRENCY);
                // A pair names an edge, not a relationship. Every variant of it
                // is a row `MATCH ()-[r:T]->()` must bind, so parallel
                // relationships are expanded here rather than counted once —
                // the same reason `expand_relationships` resolves each pair.
                let mut pending = Vec::new();
                let mut used_ids = self
                    .structural_relationship_ids
                    .values()
                    .copied()
                    .collect::<BTreeSet<_>>();
                while let Some((source, target, variants)) = variant_reads.try_next().await? {
                    for (id, metadata) in variants {
                        used_ids.extend(id);
                        pending.push((id, source, target, metadata));
                        self.check_intermediate_rows(
                            "experimental_cypher_type_probe_relationships",
                            pending.len(),
                        )?;
                    }
                }
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
                Ok(Some(relationships))
            },
            |result| {
                result
                    .as_ref()
                    .ok()
                    .and_then(Option::as_ref)
                    .map_or(0, Vec::len)
            },
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
                let neighbors = self.list_expand_neighbors(request).await?;
                self.read_expand_relationships(request.direction, &neighbors)
                    .await
            },
            |result| result.as_ref().map_or(0, Vec::len),
        )
        .await
    }

    /// The adjacency half of [`Self::expand_relationships`], for a windowed
    /// expansion that reads relationship records only for the neighbours its
    /// `LIMIT` still needs.
    async fn expand_neighbors(
        &mut self,
        request: &ExpandRequest,
    ) -> StorageResult<Option<Vec<ExpandNeighbor>>> {
        traced_operator(
            "expand_neighbors",
            operator_span("expand_neighbors"),
            async { self.list_expand_neighbors(request).await.map(Some) },
            |result| {
                result
                    .as_ref()
                    .ok()
                    .and_then(Option::as_ref)
                    .map_or(0, Vec::len)
            },
        )
        .await
    }

    async fn expand_relationships_between(
        &mut self,
        direction: ExpandDirection,
        neighbors: &[ExpandNeighbor],
    ) -> StorageResult<Vec<EngineRelationshipRecord>> {
        traced_operator(
            "expand_relationships_between",
            operator_span("expand_relationships_between"),
            self.read_expand_relationships(direction, neighbors),
            |result| result.as_ref().map_or(0, Vec::len),
        )
        .await
    }

    async fn probe_selective_equality(
        &mut self,
        candidates: &[PropertyEqualityCandidate],
    ) -> StorageResult<Option<Vec<VertexId>>> {
        let started = std::time::Instant::now();
        let probed = self.probe_selective_equality_traced(candidates).await;
        observability::record_probe(elapsed_us(started), matches!(probed, Ok(Some(_))));
        probed
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
                        self.shard.existing_vertex_metadata_batch_at(
                            self.cell_id,
                            vertex_ids,
                            self.read_epoch,
                            &self.budget,
                        ),
                    )
                    .await,
                )?;
                Ok(hydrated
                    .map(|(id, metadata)| EngineVertexRecord {
                        id,
                        labels: metadata.labels,
                        properties: metadata
                            .properties
                            .into_iter()
                            .map(|(name, value)| (name, property_into_scalar(value)))
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
    /// The neighbours each input reaches over each requested type: one
    /// adjacency read per `(input, type)`, charged to the read's scanned-edge
    /// allowance. Neighbours come back input by input, type by type, in the
    /// order the adjacency stores them.
    async fn list_expand_neighbors(
        &mut self,
        request: &ExpandRequest,
    ) -> StorageResult<Vec<ExpandNeighbor>> {
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
        let shard = self.shard;
        let cell_id = self.cell_id;
        let read_epoch = self.read_epoch;
        let budget = self.budget.clone();
        let snapshot = Arc::clone(&self.snapshot);
        let direction = request.direction;
        // Not an intermediate-row count: the input vertex check above
        // bounds this work, and a sparse multi-type hop must not be
        // rejected before any neighbour is read. Each (vertex, type)
        // pair is derived from its index as the read starts, so only
        // the reads in flight exist at once. A borrowing `flat_map`
        // would do the same but fails the `Send` bound `async_trait`
        // requires of this future.
        let inputs = &request.input_vertex_ids;
        let edge_types = &request.relationship_types;
        let mut neighbor_reads = stream::iter(0..inputs.len() * edge_types.len())
            .map(|pair| {
                let input = inputs[pair / edge_types.len()];
                let edge_type = edge_types[pair % edge_types.len()].clone();
                let budget = budget.clone();
                let snapshot = Arc::clone(&snapshot);
                async move {
                    storage_result(budget.check("experimental_cypher_expand"))?;
                    let neighbors = match direction {
                        ExpandDirection::Outgoing => storage_result(
                            shard
                                .out_neighbors_in_storage_snapshot_for_query(
                                    snapshot.as_ref(),
                                    cell_id,
                                    &edge_type,
                                    input,
                                    read_epoch,
                                    &budget,
                                )
                                .await,
                        )?,
                        ExpandDirection::Incoming => storage_result(
                            GraphStore::scope_snapshot(
                                snapshot,
                                shard.in_neighbors_at_for_query(
                                    cell_id, &edge_type, input, read_epoch, &budget,
                                ),
                            )
                            .await,
                        )?,
                    };
                    Ok((input, edge_type, neighbors))
                }
            })
            .buffered(RELATIONSHIP_EXPAND_CONCURRENCY);
        let mut listed = Vec::new();
        while let Some((input, edge_type, neighbors)) = neighbor_reads.try_next().await? {
            self.scanned_edges = self.scanned_edges.saturating_add(neighbors.len() as u64);
            storage_result(ensure_limit(
                "experimental_cypher_expand_scanned_edges",
                self.scanned_edges,
                self.shard.limits.max_query_scan_edges,
            ))?;
            listed.extend(neighbors.into_iter().map(|neighbor| ExpandNeighbor {
                input,
                neighbor,
                relationship_type: edge_type.clone(),
            }));
        }
        Ok(listed)
    }

    /// The relationship records between each listed input and neighbour, in
    /// listing order -- which keeps the structural relationship IDs assigned
    /// below stable for a given listing.
    async fn read_expand_relationships(
        &mut self,
        direction: ExpandDirection,
        neighbors: &[ExpandNeighbor],
    ) -> StorageResult<Vec<EngineRelationshipRecord>> {
        let shard = self.shard;
        let cell_id = self.cell_id;
        let read_epoch = self.read_epoch;
        let budget = self.budget.clone();
        let snapshot = Arc::clone(&self.snapshot);
        // Indexed rather than borrowed item by item, for the same `Send`
        // reason as the neighbour reads.
        let mut relationship_reads = stream::iter(0..neighbors.len())
            .map(|index| {
                let neighbor = &neighbors[index];
                let budget = budget.clone();
                let snapshot = Arc::clone(&snapshot);
                async move {
                    storage_result(budget.check("experimental_cypher_expand_neighbor"))?;
                    let (source, target) = match direction {
                        ExpandDirection::Outgoing => (neighbor.input, neighbor.neighbor),
                        ExpandDirection::Incoming => (neighbor.neighbor, neighbor.input),
                    };
                    let relationships = storage_result(
                        GraphStore::scope_snapshot(
                            snapshot,
                            shard.native_path_relationships_at(
                                cell_id,
                                &neighbor.relationship_type,
                                source,
                                target,
                                read_epoch,
                                &budget,
                            ),
                        )
                        .await,
                    )?;
                    Ok((
                        source,
                        target,
                        neighbor.relationship_type.as_str(),
                        relationships,
                    ))
                }
            })
            .buffered(RELATIONSHIP_EXPAND_CONCURRENCY);
        let mut pending = Vec::new();
        while let Some((source, target, edge_type, relationships)) =
            relationship_reads.try_next().await?
        {
            for (relationship_id, metadata) in relationships {
                storage_result(self.budget.check("experimental_cypher_expand_relationship"))?;
                pending.push((
                    relationship_id,
                    edge_type.to_string(),
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
    }

    /// Engine records for the relationships a property seek kept, giving each
    /// structural edge -- which has no stored identity -- the read's synthetic
    /// one.
    fn seeked_relationship_records(
        &mut self,
        relationship_type: &str,
        pending: Vec<(Option<RelationshipId>, VertexId, VertexId, EdgeMetadata)>,
    ) -> StorageResult<Vec<EngineRelationshipRecord>> {
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
    }

    /// The runtime equality probe, under its operator span. Wrapped by the
    /// trait method, which times it for the request's access-path record.
    async fn probe_selective_equality_traced(
        &self,
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
                // Second pass, run only when every candidate exhausted the
                // first. Declining at 256 -- 128 each across two candidates --
                // is weak evidence: legacy's own post-probe fetch distribution
                // clears that about 71% of the time, so a decline is as likely
                // to mean the slice was thin as to mean every property is
                // broad.
                //
                // This escalates to `max_query_index_candidates`, the same
                // ceiling legacy reaches, rather than a tighter constant of its
                // own. A tight allowance was previously the thing keeping a
                // completed probe *meaningful*, because a completed probe was
                // answered by hydrating every vertex it returned: a broad
                // property that finished anyway cost tens of thousands of whole
                // records. It no longer is. A seed past
                // `ORDERED_SEED_HYDRATE_MAX` is handed to the ordered walk as
                // `allowed` and spends index-key reads, not metadata reads, so
                // a wide winner is bounded work rather than a regression --
                // while the tenant-sized populations a tight allowance rejected
                // outright are exactly the ones this path has to serve. The
                // walks still race, so a narrower property still wins when
                // there is one.
                //
                // These reads are deliberately not charged to
                // `scanned_index_candidates`, so a query can spend this
                // allowance and then the ordered walk's. Reviewers read that as
                // a leak; it is parity. Legacy's ordered path escalates to the
                // same ceiling (`NodeEqualityProbe::Ordered(
                // self.limits.max_query_index_candidates)` in
                // `super::query::GraphShard::try_execute_ordered_string_vertex_rows_query`)
                // and has no query-wide tally to charge at all --
                // `scanned_index_candidates` is this engine's own addition, and
                // the ordered walk is its only caller. Charging the probe would
                // make this route stricter than the one it has to match and
                // would fail queries legacy serves, which is not allowed.
                //
                // The real gap is that an overflowed probe discards its work and
                // falls back to the planned walk with no signal, so a decline
                // after eight entries and one after the whole allowance look
                // identical from outside. The fix for that is a counter, not
                // enforcement, and it is not in this change.
                let wide = self.shard.limits.max_query_index_candidates.min(ceiling);
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
        ScalarValue::Integer(value) => scalar_integer_to_property(*value).map_err(storage_error),
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
    result.map_err(storage_error)
}

/// Hand a storage failure to the engine with the `GraphError` attached, so
/// `experimental_engine_error` returns that error itself -- a timeout, an
/// exhausted budget, a corrupt record -- with the class and client code the
/// legacy route gives it, rather than the "not supported yet" a bare message
/// becomes.
fn storage_error(error: GraphError) -> StorageError {
    StorageError::from_backend(error)
}

/// Whether a relationship's property might pass an integer lower bound that
/// is one AND term of its predicate. Only definite failures return `false`:
/// a missing property compares as null, which no AND can turn true, and an
/// integer compares exactly. Floats, strings and booleans are kept for the
/// engine's own comparison, which the full predicate applies afterwards.
fn may_satisfy_lower_bound(
    value: Option<&VertexPropertyValue>,
    lower: i128,
    inclusive: bool,
) -> bool {
    let value = match value {
        None => return false,
        Some(VertexPropertyValue::Integer(value)) => i128::from(*value),
        Some(VertexPropertyValue::SignedInteger(value)) => i128::from(*value),
        Some(_) => return true,
    };
    if inclusive {
        value >= lower
    } else {
        value > lower
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use slatedb::object_store::memory::InMemory;

    #[tokio::test]
    async fn vertex_identity_in_matches_equalities() {
        let shard = GraphShard::open_standalone_writer(
            "graph/experimental-vertex-identity-in",
            Arc::new(InMemory::new()),
        )
        .await
        .expect("open graph shard");
        for id in [1, 2, 3] {
            shard
                .set_vertex_metadata("cell-a", id, VertexMetadata::default().with_label("Entity"))
                .await
                .expect("write vertex metadata");
        }

        let expected = vec![
            QueryRow::new(vec![QueryValue::VertexId(1)]),
            QueryRow::new(vec![QueryValue::VertexId(3)]),
        ];
        for (query, description) in [
            (
                "MATCH (n:Entity) WHERE n.id = 1 OR n.id = 3 RETURN n.id AS id ORDER BY id",
                "equalities",
            ),
            (
                "MATCH (n:Entity) WHERE n.id IN [1, 3] RETURN n.id AS id ORDER BY id",
                "IN list",
            ),
        ] {
            let result = shard
                .execute_cypher_rows(
                    QueryContext::new("cell-a", "vertex-identity-in")
                        .with_cypher_engine(CypherEngineMode::Experimental)
                        .with_refreshed_reader(),
                    query,
                )
                .await
                .expect("execute identity query");
            assert_eq!(result.rows, expected, "{description}");
        }
        shard.close().await.expect("close graph shard");
    }
}
