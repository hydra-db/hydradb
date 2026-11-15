use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex};

use async_trait::async_trait;
use futures::{stream, StreamExt as _, TryStreamExt as _};
use hydradb_cypher_engine::{
    BoundedVertexIds, CypherEngine, ExpandDirection, ExpandNeighbor, ExpandRequest, GraphRead, GraphStorage, OrderedPropertyEntryPage,
    OrderedPropertyScanPage, OrderedPropertyScanRequest, OrderedRelationshipPropertyScanPage, OrderedRelationshipPropertyScanRequest,
    OrderedScanCursor, OrderedScanPosition, PropertyEqualityCandidate, PropertyPointSet, QueryResult, QueryValue as EngineQueryValue,
    ReadRequest, RelationshipRecord as EngineRelationshipRecord, ScalarValue, StatisticsAccessReport, StatisticsProvider, StorageError,
    StorageResult, VertexRecord as EngineVertexRecord, VertexStorageScanPage, VertexStorageScanRequest,
};
use tracing::Instrument;

use crate::query::experimental_cypher::{
    experimental_engine_error, experimental_query_error, gql_status_report, is_experimental_explain,
    lower_experimental_cypher_with_id_properties, property_into_scalar, property_to_scalar,
};
use crate::QueryFailureReason;

use super::*;
use crate::shard::query::{IndexedRelationshipBatch, IndexedRelationshipPairMemo};

mod observability;
mod ordered_relationship_walk;
mod ordered_walk;
mod scope_statistics;

use observability::{PlanIdentity, RequestObservation};
pub(crate) use scope_statistics::{PlannerStatisticsCache, PlannerStatisticsHintFailure, PlannerStatisticsHints};

/// How many index seeks of one multi-value predicate run at once. Bounded so
/// a large `IN` list cannot fan out into unbounded concurrent object-store
/// reads; matches the statistics loader's read concurrency.
const MULTI_SEEK_CONCURRENCY: usize = 8;

/// Bound object-store reads during relationship expansion. `buffered` keeps
/// the original input order, which also keeps structural relationship IDs
/// stable for queries that return them.
const RELATIONSHIP_EXPAND_CONCURRENCY: usize = 16;

/// Listed out-neighbours of one source, over one type, from which a whole
/// expansion reads the source's relationships with two prefix scans instead of
/// three reads per edge. Below it the per-edge reads, sixteen at a time, are as
/// quick and read nothing the hop does not need.
const SOURCE_BATCH_MIN_NEIGHBORS: usize = 8;

/// Candidate allowance shared out across the equality candidates of a runtime
/// access-path probe's first pass. Matches the legacy row engine's unordered
/// probe, so a property that anchors a query well on one engine anchors it on
/// the other, and catches the common case: legacy's own post-probe fetch count
/// has a median of 40.
const EQUALITY_PROBE_ENTRIES: usize = 256;

pub(crate) async fn execute_experimental_cypher_rows(shard: &GraphShard, context: QueryContext, query: &str) -> Result<QueryResultSet> {
    // EXPLAIN prepares and renders but never executes the physical plan. Keep
    // it out of both the row-query lifecycle and operator-request counters so
    // an A/B benchmark has the same denominator on both routes.
    if is_experimental_explain(query) {
        let mut observation = RequestObservation::default();
        return execute_experimental_cypher_rows_inner(shard, context, query, &mut observation).await;
    }

    shard.operation_metrics.query_rows_started.fetch_add(1, Ordering::Relaxed);
    let started = std::time::Instant::now();
    let span = observability::request_span();
    let mut observation = RequestObservation::default();
    let remote_io = crate::engine::remote_io::RemoteIoWindow::open();
    let result = execute_experimental_cypher_rows_inner(shard, context, query, &mut observation)
        .instrument(span.clone())
        .await;
    remote_io.record(&span);
    let elapsed = started.elapsed();
    shard.operation_metrics.query_rows_latency.record(elapsed);
    observation.finish(&span, shard, elapsed, &result);
    match &result {
        Ok(result_set) => {
            shard.operation_metrics.query_rows_completed.fetch_add(1, Ordering::Relaxed);
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
    let id_properties = shard.uses_id_properties().await?;
    let (explain, logical, lowering) = {
        let _span = tracing::info_span!("query.experimental.lower").entered();
        lower_experimental_cypher_with_id_properties(
            query,
            &context.parameters,
            &context.list_parameters,
            context.session_user.as_deref(),
            id_properties,
        )?
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
    // The capability check asks for the answer "after lowering and planning, before storage
    // execution begins", which is what the second tier is: planning needs
    // statistics, and statistics are point lookups. Both tiers are pinned by
    // tests below, because "no storage request" read alone would over-claim the
    // second one.
    let capability = logical.capability();
    if let Some(kind) = capability.unsupported_kind() {
        // The token, not the query text: bounded cardinality, no literals.
        // Turning this into a counter is separate work; a span field costs
        // nothing until somebody is looking.
        tracing::debug!(
            hydradb.query.experimental.unsupported = kind,
            "experimental engine cannot plan this shape"
        );
    }
    capability.into_result().map_err(experimental_engine_error)?;

    let stage_started = std::time::Instant::now();
    let snapshot = budget
        .read_only_io("experimental_cypher_snapshot", async {
            if context.uses_refreshed_reader() {
                shard.storage.strong_snapshot().await
            } else {
                shard.storage.snapshot().await
            }
        })
        .await?;
    let read_epoch = snapshot.seq();
    let storage_sequence = read_epoch;
    stages.snapshot = stage_started.elapsed();
    let stage_started = std::time::Instant::now();
    let (prepared, statistics) = SlateGraphStore::scope_snapshot(Arc::clone(&snapshot), async {
        budget
            .read_only_io(
                "experimental_cypher_statistics_cell",
                shard.ensure_cell_readable(&context.cell_id, "experimental_cypher_statistics"),
            )
            .await?;
        // The indexer's published counts plan this query. An unpublished,
        // corrupt or unreadable object, or the flag off, plans with no
        // statistics at all.
        let planner_statistics = shard.planner_statistics_for_query(&context.cell_id, &budget, explain).await;
        stages.planner_statistics = planner_statistics
            .as_deref()
            .map(|statistics| (statistics.published.storage_sequence, statistics.published.built_at_ms));
        let statistics: Arc<dyn StatisticsProvider> = match planner_statistics {
            Some(statistics) => statistics,
            None => Arc::new(hydradb_cypher_engine::UnknownStatistics),
        };
        stages.statistics = stage_started.elapsed();
        let plan_started = std::time::Instant::now();
        budget.check("experimental_cypher_physical_plan")?;
        let prepared = {
            let _span = tracing::info_span!("query.experimental.plan").entered();
            logical
                .plan_with_provider(&hydradb_cypher_engine::SlateDbDataSource::new(statistics.as_ref()))
                .map_err(experimental_engine_error)
        };
        stages.plan = plan_started.elapsed();
        prepared.map(|prepared| (prepared, statistics))
    })
    .await?;
    // The second half of the capability answer: what only the chosen physical
    // operators settle. Still before `HydraDbCypherStorage` exists, so a
    // refusal here costs planning and nothing more.
    prepared.capability().into_result().map_err(experimental_engine_error)?;
    stages.plan_identity = Some(PlanIdentity::of(&prepared));
    stages.work.profile(&prepared.physical);
    if explain {
        return Ok(QueryResultSet::new(
            vec![QueryColumn::new("plan")],
            vec![QueryRow::new(vec![QueryValue::Property(VertexPropertyValue::String(
                prepared.explain(),
            ))])],
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
        .scope(SlateGraphStore::scope_snapshot(
            snapshot,
            engine
                .execute_prepared_with_status(&prepared)
                .instrument(tracing::info_span!("query.experimental.execute")),
        ))
        .await;
    stages.execute = stage_started.elapsed();
    stages.collect_profile(&prepared.physical, statistics.as_ref());
    let outcome = executed.map_err(experimental_engine_error)?;
    let result_started = std::time::Instant::now();
    let rows_materialized = outcome.result.rows.len();
    let skip = usize::try_from(context.result_window.skip).map_err(|_| GraphError::AdmissionRejected {
        operation: "experimental_cypher_result_skip",
        actual: context.result_window.skip,
        limit: usize::MAX as u64,
    })?;
    if let Some(limit) = context.result_window.limit {
        ensure_limit(
            "experimental_cypher_result_limit",
            limit as u64,
            shard.limits.max_query_result_vertices as u64,
        )?;
    }
    // The request's window cuts the result before its status is chosen, so a
    // window that leaves no rows reports no data rather than success.
    let outcome = outcome.windowed(skip, context.result_window.limit);
    if context.result_window.limit.is_none() {
        ensure_limit(
            "experimental_cypher_result_rows",
            outcome.result.rows.len() as u64,
            shard.limits.max_query_result_vertices as u64,
        )?;
    }
    // The GQL-status objects (ISO/IEC 39075 §4.9) ride on the result set to
    // the client transports, primary first.
    let statuses: Vec<crate::GqlStatusReport> = std::iter::once(&outcome.status)
        .chain(&outcome.additional)
        .map(gql_status_report)
        .collect();
    let result = outcome.result;
    // Emitted only once the request's own window and the result-size limits
    // have been applied, so the row count agrees with what the client gets
    // and with the post-window `query_rows_returned` counter. A request that
    // fails those checks reports through the failure counters instead.
    stages.rows_materialized = rows_materialized;
    stages.rows_returned = result.rows.len();
    let converted = engine_result_to_hydradb(result, read_epoch, storage_sequence, &budget).map(|set| set.with_statuses(statuses));
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
async fn traced_operator<T, F>(operator: &'static str, span: tracing::Span, future: F, output_len: impl FnOnce(&T) -> usize) -> T
where
    F: std::future::Future<Output = T>,
{
    let started = std::time::Instant::now();
    // The call's reads are counted in a scope of its own. A delta of the
    // request-wide totals would charge this call with whatever a concurrently
    // running `UNION` arm read while it waited.
    let call_reads = Arc::new(crate::core::state::ReadAccounting::default());
    let output = call_reads.scope(future.instrument(span.clone())).await;
    call_reads.absorb_into_enclosing();
    let elapsed = elapsed_us(started);
    let rows = output_len(&output);
    span.record("hydradb.query.operator.elapsed_us", elapsed);
    span.record("hydradb.query.operator.rows_out", rows);
    observability::record_storage_call(operator, elapsed, rows, call_reads.totals());
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
        // Filled by the ordered index walk only: how much of its time went
        // to verifying entries against storage (metadata hydration, or the
        // membership point gets of an entry walk) versus the index scan, and
        // how many index entries it touched to produce its window.
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
            structural: Arc::new(StdMutex::new(StructuralRelationshipIds::default())),
            scanned_edges: Arc::new(AtomicU64::new(0)),
            completion_conditions: Vec::new(),
            write_back_present: Arc::default(),
            forked: false,
        }))
    }
}

/// Identities handed to relationships the store keeps no id for, stable for
/// the whole query: every read forked from one `begin_read` shares them, so
/// two `UNION` arms that meet the same structural edge report the same id.
struct StructuralRelationshipIds {
    ids: BTreeMap<(String, VertexId, VertexId), RelationshipId>,
    allocated: BTreeSet<RelationshipId>,
    next: RelationshipId,
}

impl Default for StructuralRelationshipIds {
    fn default() -> Self {
        Self {
            ids: BTreeMap::new(),
            allocated: BTreeSet::new(),
            next: RelationshipId::MAX,
        }
    }
}

/// Where a forked read's structural ids start: `[2^63, 2^63 + 2^62)`, the
/// bottom of the generated range, below the counter's part of it (which counts
/// down from `u64::MAX`). No stored relationship can carry one; see
/// [`crate::GENERATED_RELATIONSHIP_ID_FLOOR`].
const FORKED_STRUCTURAL_ID_BASE: RelationshipId = crate::GENERATED_RELATIONSHIP_ID_FLOOR;
const FORKED_STRUCTURAL_ID_SPAN: RelationshipId = 1 << 62;

/// A structural id that is a function of the edge, for reads that run side by
/// side. The counter hands ids out in the order reads reach it, which for
/// concurrent `UNION` arms is the order they happen to finish in, so the same
/// query could number the same edges differently from run to run. FNV-1a over
/// the identity is the same in every run and every process.
fn forked_structural_relationship_id(identity: &(String, VertexId, VertexId)) -> RelationshipId {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    let (relationship_type, source, target) = identity;
    for byte in relationship_type
        .as_bytes()
        .iter()
        .copied()
        .chain([0xff])
        .chain(source.to_le_bytes())
        .chain(target.to_le_bytes())
    {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    FORKED_STRUCTURAL_ID_BASE + hash % FORKED_STRUCTURAL_ID_SPAN
}

struct HydraDbCypherRead<'a> {
    shard: &'a GraphShard,
    cell_id: &'a str,
    read_epoch: StorageSequence,
    snapshot: Arc<GraphStorageSnapshot>,
    budget: QueryBudget,
    structural: Arc<StdMutex<StructuralRelationshipIds>>,
    /// A read forked for a concurrently executed branch; see
    /// [`forked_structural_relationship_id`].
    // All UNION arms read the same immutable cell snapshot.
    write_back_present: Arc<tokio::sync::OnceCell<bool>>,
    forked: bool,
    /// Edges visited by every expansion this read has served, not just the
    /// current one. An undirected hop calls `expand_relationships` once per
    /// storage orientation (`expand_rows` in the engine), so a tally local to
    /// the call would let a query spend `max_query_scan_edges` twice over.
    scanned_edges: Arc<AtomicU64>,
    /// GQL completion conditions the engine raised on this read, such as a
    /// null eliminated by an aggregate.
    completion_conditions: Vec<hydradb_cypher_engine::GqlStatus>,
}

#[async_trait]
impl GraphRead for HydraDbCypherRead<'_> {
    async fn scan_vertex_storage(&mut self, request: &VertexStorageScanRequest) -> StorageResult<VertexStorageScanPage> {
        match request {
            VertexStorageScanRequest::IdPoints { ids } => {
                if ids.is_empty() {
                    return Ok(VertexStorageScanPage::Records(Vec::new()));
                }
                Ok(VertexStorageScanPage::Records(self.hydrate_vertices(ids).await?))
            }
            VertexStorageScanRequest::PropertyPoints { property, points } => {
                let ids = match points {
                    PropertyPointSet::Single(value) => self.seek_vertices_by_property(property, value).await?,
                    PropertyPointSet::Multiple(values) => self.seek_vertices_by_property_values(property, values).await?,
                };
                Ok(VertexStorageScanPage::Ids(ids))
            }
            VertexStorageScanRequest::PropertyPresence { property } => {
                Ok(VertexStorageScanPage::Ids(self.scan_vertices_by_property(property).await?))
            }
            VertexStorageScanRequest::OrderedProperty(request) => Ok(VertexStorageScanPage::Ordered(
                self.scan_vertices_by_property_ordered(request).await?,
            )),
            VertexStorageScanRequest::LabelPrefix { label, max_results } => {
                Ok(VertexStorageScanPage::Ids(self.scan_vertices_by_label(label, *max_results).await?))
            }
            VertexStorageScanRequest::AllVertices => Ok(VertexStorageScanPage::Ids(self.scan_all_vertices().await?)),
        }
    }

    fn raise_completion_condition(&mut self, status: hydradb_cypher_engine::GqlStatus) {
        self.completion_conditions.push(status);
    }

    /// A sibling read for a concurrently executed plan branch: the same
    /// snapshot, epoch and budget, and the same query-wide scanned-edge total
    /// and structural relationship ids, so running `UNION` arms side by side
    /// neither multiplies a per-query limit nor renumbers a relationship.
    /// Completion conditions are its own; the engine hands them back.
    fn fork_read(&self) -> Option<Box<dyn GraphRead + '_>> {
        Some(Box::new(HydraDbCypherRead {
            shard: self.shard,
            cell_id: self.cell_id,
            read_epoch: self.read_epoch,
            snapshot: Arc::clone(&self.snapshot),
            budget: self.budget.clone(),
            structural: Arc::clone(&self.structural),
            scanned_edges: Arc::clone(&self.scanned_edges),
            completion_conditions: Vec::new(),
            write_back_present: Arc::clone(&self.write_back_present),
            forked: true,
        }))
    }

    fn take_completion_conditions(&mut self) -> Vec<hydradb_cypher_engine::GqlStatus> {
        std::mem::take(&mut self.completion_conditions)
    }

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

    async fn seek_vertices_by_property(&mut self, property: &str, value: &ScalarValue) -> StorageResult<Vec<VertexId>> {
        traced_operator(
            "seek_vertices_by_property",
            operator_span("seek_vertices_by_property"),
            async {
                let value = scalar_to_property(value)?;
                self.shard.operation_metrics.record_experimental_property_seek();
                storage_result(
                    SlateGraphStore::scope_snapshot(
                        Arc::clone(&self.snapshot),
                        self.shard
                            .scan_vertex_property_index_at(self.cell_id, property, &value, self.read_epoch, &self.budget),
                    )
                    .await,
                )
            },
            |result| result.as_ref().map_or(0, Vec::len),
        )
        .await
    }

    async fn seek_vertices_by_property_values(&mut self, property: &str, values: &[ScalarValue]) -> StorageResult<Vec<VertexId>> {
        traced_operator(
            "seek_vertices_by_property_values",
            operator_span("seek_vertices_by_property_values"),
            async {
                let values = values.iter().map(scalar_to_property).collect::<StorageResult<Vec<_>>>()?;
                // Same accounting as the single seek: one value is one
                // index request, so a two-value OR still adds two.
                for _ in &values {
                    self.shard.operation_metrics.record_experimental_property_seek();
                }
                let shard = self.shard;
                let cell_id = self.cell_id;
                let read_epoch = self.read_epoch;
                let budget = &self.budget;
                let seeks = stream::iter(values)
                    .map(|value| async move {
                        let ids = shard
                            .scan_vertex_property_index_at(cell_id, property, &value, read_epoch, budget)
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
                        shard.charge_query_index_candidates("cypher_in_list_index_candidates", budget, ids.len() as u64)?;
                        Ok(ids)
                    })
                    .buffer_unordered(MULTI_SEEK_CONCURRENCY)
                    .try_collect::<Vec<Vec<VertexId>>>();
                let ids = storage_result(SlateGraphStore::scope_snapshot(Arc::clone(&self.snapshot), seeks).await)?;
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
                self.shard.operation_metrics.record_experimental_property_seek();
                storage_result(
                    SlateGraphStore::scope_snapshot(
                        Arc::clone(&self.snapshot),
                        self.shard
                            .scan_vertex_property_values_at(self.cell_id, property, self.read_epoch, &self.budget),
                    )
                    .await,
                )
            },
            |result| result.as_ref().map_or(0, Vec::len),
        )
        .await
    }

    async fn scan_vertices_by_property_ordered(&mut self, request: &OrderedPropertyScanRequest) -> StorageResult<OrderedPropertyScanPage> {
        traced_operator(
            "scan_vertices_by_property_ordered",
            operator_span("scan_vertices_by_property_ordered"),
            async {
                self.shard.operation_metrics.record_experimental_ordered_property_scan();
                let page = storage_result(
                    SlateGraphStore::scope_snapshot(
                        Arc::clone(&self.snapshot),
                        self.shard
                            .scan_ordered_vertex_property_page_at(self.cell_id, request, self.read_epoch, &self.budget),
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
                    last: page.last.map(|(value, vertex_id)| OrderedScanPosition { value, vertex_id }),
                    exhausted: page.exhausted,
                    index_entries: Some(page.index_entries),
                })
            },
            |result: &StorageResult<OrderedPropertyScanPage>| result.as_ref().map_or(0, |page| page.vertices.len()),
        )
        .await
    }

    /// The same walk with no record read: existence and labels are proven
    /// from index keys, so a page costs one point get per entry per label
    /// (or per entry, unlabelled) instead of the record and its overlay.
    /// Reported under its own operator name, which keeps it out of the
    /// profiler's hydrated-vertex count: nothing here is hydrated.
    async fn scan_vertex_entries_by_property_ordered(
        &mut self,
        request: &OrderedPropertyScanRequest,
    ) -> StorageResult<OrderedPropertyEntryPage> {
        traced_operator(
            "scan_vertex_entries_by_property_ordered",
            operator_span("scan_vertex_entries_by_property_ordered"),
            async {
                self.shard.operation_metrics.record_experimental_ordered_property_scan();
                let page = storage_result(
                    SlateGraphStore::scope_snapshot(
                        Arc::clone(&self.snapshot),
                        self.shard
                            .scan_ordered_vertex_property_entries_at(self.cell_id, request, self.read_epoch, &self.budget),
                    )
                    .await,
                )?;
                Ok(OrderedPropertyEntryPage {
                    entries: page.rows,
                    last: page.last.map(|(value, vertex_id)| OrderedScanPosition { value, vertex_id }),
                    exhausted: page.exhausted,
                    index_entries: Some(page.index_entries),
                })
            },
            |result: &StorageResult<OrderedPropertyEntryPage>| result.as_ref().map_or(0, |page| page.entries.len()),
        )
        .await
    }

    /// The entry walk again, charged to `max_query_scan_edges` rather than
    /// `max_query_index_candidates`: a count keeps no entry, so what bounds
    /// it is what it reads, and the candidate cap would refuse a count over
    /// more entries than any query may hold.
    async fn scan_vertex_entries_by_property_for_count(
        &mut self,
        request: &OrderedPropertyScanRequest,
    ) -> StorageResult<Option<OrderedPropertyEntryPage>> {
        traced_operator(
            "scan_vertex_entries_by_property_for_count",
            operator_span("scan_vertex_entries_by_property_for_count"),
            async {
                self.shard.operation_metrics.record_experimental_ordered_property_scan();
                let page = storage_result(
                    SlateGraphStore::scope_snapshot(
                        Arc::clone(&self.snapshot),
                        self.shard
                            .scan_ordered_vertex_property_counted_entries_at(self.cell_id, request, self.read_epoch, &self.budget),
                    )
                    .await,
                )?;
                Ok(Some(OrderedPropertyEntryPage {
                    entries: page.rows,
                    last: page.last.map(|(value, vertex_id)| OrderedScanPosition { value, vertex_id }),
                    exhausted: page.exhausted,
                    index_entries: Some(page.index_entries),
                }))
            },
            |result: &StorageResult<Option<OrderedPropertyEntryPage>>| {
                result.as_ref().map_or(0, |page| page.as_ref().map_or(0, |page| page.entries.len()))
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
                    SlateGraphStore::scope_snapshot(
                        Arc::clone(&self.snapshot),
                        self.shard
                            .scan_ordered_relationship_property_page_at(self.cell_id, request, self.read_epoch, &self.budget),
                    )
                    .await,
                )?;
                // Structural edges get the same session-stable identities
                // expansion gives them.
                let mut used_ids = page
                    .rows
                    .iter()
                    .filter_map(|row| row.relationship_id)
                    .chain(self.structural_relationship_ids_in_use())
                    .collect::<BTreeSet<_>>();
                let mut rows = Vec::with_capacity(page.rows.len());
                for row in page.rows {
                    let id = match row.relationship_id {
                        Some(id) => id,
                        None => self.structural_relationship_id((request.relationship_type.clone(), row.src, row.dst), &mut used_ids)?,
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
            |result: &StorageResult<OrderedRelationshipPropertyScanPage>| result.as_ref().map_or(0, |page| page.relationships.len()),
        )
        .await
    }

    async fn scan_vertices_by_label(&mut self, label: &str, max_results: Option<usize>) -> StorageResult<Vec<VertexId>> {
        traced_operator(
            "scan_vertices_by_label",
            operator_span("scan_vertices_by_label"),
            async {
                storage_result(
                    SlateGraphStore::scope_snapshot(
                        Arc::clone(&self.snapshot),
                        self.shard
                            .scan_vertex_label_index_at_limited(self.cell_id, label, self.read_epoch, &self.budget, max_results),
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
                let Ok(values) = values.iter().map(scalar_to_property).collect::<StorageResult<Vec<_>>>() else {
                    return Ok(BoundedVertexIds::declined(0));
                };
                for _ in &values {
                    self.shard.operation_metrics.record_experimental_property_seek();
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
                let ids = storage_result(SlateGraphStore::scope_snapshot(Arc::clone(&self.snapshot), seeks).await)?;
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
    async fn scan_vertices_by_label_bounded(&mut self, label: &str, max_index_entries: usize) -> StorageResult<BoundedVertexIds> {
        let limit = max_index_entries.min(self.shard.limits.max_query_index_candidates.saturating_sub(1));
        let ids = self.scan_vertices_by_label(label, Some(limit.saturating_add(1))).await?;
        Ok(BoundedVertexIds::within(ids, limit))
    }

    async fn scan_all_vertices(&mut self) -> StorageResult<Vec<VertexId>> {
        traced_operator(
            "scan_all_vertices",
            operator_span("scan_all_vertices"),
            async {
                storage_result(
                    self.shard
                        .scan_vertex_ids_at(self.snapshot.as_ref(), self.cell_id, self.read_epoch, &self.budget)
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
                let pending = self
                    .seeked_relationship_variants(relationship_type, property, std::slice::from_ref(&value))
                    .await?;
                self.seeked_relationship_records(relationship_type, pending)
            },
            |result| result.as_ref().map_or(0, Vec::len),
        )
        .await
    }

    /// The single seek for several values at once: see
    /// [`Self::seeked_relationship_variants`]. A relationship is kept when its
    /// value equals any of `values`.
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
                let values = values.iter().map(scalar_to_property).collect::<StorageResult<Vec<_>>>()?;
                let pending = self.seeked_relationship_variants(relationship_type, property, &values).await?;
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
                    SlateGraphStore::scope_snapshot(
                        Arc::clone(&self.snapshot),
                        self.shard
                            .try_scan_edge_property_lower_bound_at(super::query::RelationshipLowerBoundProbe {
                                cell_id: self.cell_id,
                                edge_type: relationship_type,
                                property,
                                lower,
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
                // Each candidate pair is an edge the expansion would have
                // visited, so it draws on the same per-read budget. A probe
                // that declines is not charged: its index walk is bounded by
                // `max_index_entries`, and the fallback charges its own reads.
                let scanned = self.charge_scanned_edges(pairs.len());
                storage_result(ensure_limit(
                    "experimental_cypher_range_probe_scanned_edges",
                    scanned,
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
                                SlateGraphStore::scope_snapshot(
                                    snapshot,
                                    shard.native_path_relationships_at(cell_id, relationship_type, source, target, read_epoch, &budget),
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
                let mut used_ids = self.structural_relationship_ids_in_use().collect::<BTreeSet<_>>();
                while let Some((source, target, variants)) = variant_reads.try_next().await? {
                    for (id, metadata) in variants {
                        used_ids.extend(id);
                        if !may_satisfy_lower_bound(metadata.properties.get(property), lower, inclusive) {
                            continue;
                        }
                        pending.push((id, source, target, metadata));
                        self.check_intermediate_rows("experimental_cypher_range_probe_relationships", pending.len())?;
                    }
                }
                let mut relationships = Vec::with_capacity(pending.len());
                for (id, source, target, metadata) in pending {
                    let id = match id {
                        Some(id) => id,
                        None => self.structural_relationship_id((relationship_type.to_string(), source, target), &mut used_ids)?,
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
            |result| result.as_ref().ok().and_then(Option::as_ref).map_or(0, Vec::len),
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
                    SlateGraphStore::scope_snapshot(
                        Arc::clone(&self.snapshot),
                        self.shard.try_scan_edge_type_at(super::query::RelationshipTypeProbe {
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
                if !self.try_charge_scanned_edges(pairs.len(), self.shard.limits.max_query_scan_edges) {
                    return Ok(None);
                }
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
                                SlateGraphStore::scope_snapshot(
                                    snapshot,
                                    shard.native_path_relationships_at(cell_id, relationship_type, source, target, read_epoch, &budget),
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
                let mut used_ids = self.structural_relationship_ids_in_use().collect::<BTreeSet<_>>();
                while let Some((source, target, variants)) = variant_reads.try_next().await? {
                    for (id, metadata) in variants {
                        used_ids.extend(id);
                        pending.push((id, source, target, metadata));
                        self.check_intermediate_rows("experimental_cypher_type_probe_relationships", pending.len())?;
                    }
                }
                let mut relationships = Vec::with_capacity(pending.len());
                for (id, source, target, metadata) in pending {
                    let id = match id {
                        Some(id) => id,
                        None => self.structural_relationship_id((relationship_type.to_string(), source, target), &mut used_ids)?,
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
            |result| result.as_ref().ok().and_then(Option::as_ref).map_or(0, Vec::len),
        )
        .await
    }

    async fn expand_relationships(&mut self, request: &ExpandRequest) -> StorageResult<Vec<EngineRelationshipRecord>> {
        traced_operator(
            "expand_relationships",
            operator_span("expand_relationships"),
            async {
                let neighbors = self.list_expand_neighbors(request).await?;
                self.read_expand_relationships(request.direction, &neighbors, true).await
            },
            |result| result.as_ref().map_or(0, Vec::len),
        )
        .await
    }

    /// The adjacency half of [`Self::expand_relationships`], for a windowed
    /// expansion that reads relationship records only for the neighbours its
    /// `LIMIT` still needs.
    async fn expand_neighbors(&mut self, request: &ExpandRequest) -> StorageResult<Option<Vec<ExpandNeighbor>>> {
        traced_operator(
            "expand_neighbors",
            operator_span("expand_neighbors"),
            async { self.list_expand_neighbors(request).await.map(Some) },
            |result| result.as_ref().ok().and_then(Option::as_ref).map_or(0, Vec::len),
        )
        .await
    }

    async fn outgoing_neighbor_page(
        &mut self,
        input: VertexId,
        edge_type: &str,
        after: Option<VertexId>,
        size: usize,
    ) -> StorageResult<Option<Vec<VertexId>>> {
        traced_operator(
            "outgoing_neighbor_page",
            operator_span("outgoing_neighbor_page"),
            async {
                let result = storage_result(
                    self.shard
                        .out_neighbor_page(&self.snapshot, self.cell_id, edge_type, input, after, size, &self.budget)
                        .await,
                )?;
                if let Some(page) = &result {
                    if after.is_none() {
                        self.shard.operation_metrics.record_experimental_relationship_expand();
                    }
                    let scanned = self.charge_scanned_edges(page.len());
                    storage_result(ensure_limit(
                        "experimental_cypher_expand_scanned_edges",
                        scanned,
                        self.shard.limits.max_query_scan_edges,
                    ))?;
                }
                Ok(result)
            },
            |result| result.as_ref().ok().and_then(Option::as_ref).map_or(0, Vec::len),
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
            self.read_expand_relationships(direction, neighbors, false),
            |result| result.as_ref().map_or(0, Vec::len),
        )
        .await
    }

    async fn probe_selective_equality(&mut self, candidates: &[PropertyEqualityCandidate]) -> StorageResult<Option<Vec<VertexId>>> {
        let started = std::time::Instant::now();
        let probed = self.probe_selective_equality_traced(candidates).await;
        observability::record_probe(elapsed_us(started), matches!(probed, Ok(Some(_))));
        probed
    }

    async fn hydrate_vertices(&mut self, vertex_ids: &[VertexId]) -> StorageResult<Vec<EngineVertexRecord>> {
        traced_operator(
            "hydrate_vertices",
            operator_span("hydrate_vertices"),
            async {
                let hydrated = storage_result(
                    SlateGraphStore::scope_snapshot(Arc::clone(&self.snapshot), async {
                        // A single isolated point lookup already needs just one overlay seek.
                        let has_overlays = if vertex_ids.len() > 1 || (self.forked && !vertex_ids.is_empty()) {
                            *self
                                .write_back_present
                                .get_or_init(|| async {
                                    // An inconclusive probe leaves the ordinary per-vertex read in charge
                                    // of reporting errors for the vertices this query actually touches.
                                    self.shard
                                        .cell_has_write_back_values(self.cell_id, &self.budget)
                                        .await
                                        .unwrap_or(true)
                                })
                                .await
                        } else {
                            self.write_back_present.get().copied().unwrap_or(true)
                        };
                        self.shard
                            .read_existing_vertices(self.cell_id, vertex_ids, &self.budget, has_overlays)
                            .await
                    })
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
    async fn list_expand_neighbors(&mut self, request: &ExpandRequest) -> StorageResult<Vec<ExpandNeighbor>> {
        if request.relationship_types.is_empty() {
            return Err(StorageError::new(
                "relationship expansion without an explicit type is not supported yet",
            ));
        }
        self.shard.operation_metrics.record_experimental_relationship_expand();
        self.check_intermediate_rows("experimental_cypher_expand_inputs", request.input_vertex_ids.len())?;
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
                            SlateGraphStore::scope_snapshot(
                                snapshot,
                                shard.in_neighbors_at_for_query(cell_id, &edge_type, input, read_epoch, &budget),
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
            let scanned = self.charge_scanned_edges(neighbors.len());
            storage_result(ensure_limit(
                "experimental_cypher_expand_scanned_edges",
                scanned,
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
    ///
    /// `whole_listing` says `neighbors` is every neighbour the listing found,
    /// as in a full expansion. Then an outgoing source with many neighbours is
    /// read with two prefix scans (`native_path_relationships_from_source_at`).
    /// A `LIMIT` window passes a few neighbours of what may be a hub, where a
    /// whole-source scan would read far more than the window needs, so it
    /// keeps the per-edge reads.
    async fn read_expand_relationships(
        &mut self,
        direction: ExpandDirection,
        neighbors: &[ExpandNeighbor],
        whole_listing: bool,
    ) -> StorageResult<Vec<EngineRelationshipRecord>> {
        let shard = self.shard;
        let cell_id = self.cell_id;
        let read_epoch = self.read_epoch;
        let budget = self.budget.clone();
        let snapshot = Arc::clone(&self.snapshot);
        let mut answered: Vec<Option<super::query::NativePathRows>> = vec![None; neighbors.len()];
        // Rows held so far, checked as each read lands, as the per-edge
        // stream always did, so an expansion over the allowance stops before
        // it holds the rest.
        let mut retained = 0_usize;
        if whole_listing && direction == ExpandDirection::Outgoing {
            let mut sources: BTreeMap<(VertexId, &str), Vec<usize>> = BTreeMap::new();
            for (index, neighbor) in neighbors.iter().enumerate() {
                sources
                    .entry((neighbor.input, neighbor.relationship_type.as_str()))
                    .or_default()
                    .push(index);
            }
            sources.retain(|_, indices| indices.len() >= SOURCE_BATCH_MIN_NEIGHBORS);
            let groups = sources.into_iter().collect::<Vec<_>>();
            let mut source_reads = stream::iter(0..groups.len())
                .map(|group| {
                    let ((src, edge_type), indices) = &groups[group];
                    let dsts = indices.iter().map(|&index| neighbors[index].neighbor).collect::<Vec<_>>();
                    let budget = budget.clone();
                    let snapshot = Arc::clone(&snapshot);
                    async move {
                        storage_result(budget.check("experimental_cypher_expand_source"))?;
                        let rows = storage_result(
                            SlateGraphStore::scope_snapshot(
                                snapshot,
                                shard.native_path_relationships_from_source_at(cell_id, edge_type, *src, &dsts, read_epoch, &budget),
                            )
                            .await,
                        )?;
                        Ok::<_, StorageError>((group, rows))
                    }
                })
                .buffered(RELATIONSHIP_EXPAND_CONCURRENCY);
            while let Some((group, rows)) = source_reads.try_next().await? {
                // `None`: a historical epoch, which keeps the per-edge reads.
                if let Some(rows) = rows {
                    for (&index, rows) in groups[group].1.iter().zip(rows) {
                        retained = retained.saturating_add(rows.len());
                        self.check_intermediate_rows("experimental_cypher_expand_relationships", retained)?;
                        answered[index] = Some(rows);
                    }
                }
            }
        }
        // The windowed executor supplies pairs sorted by (input, neighbour).
        // Batch just one typed destination at a time and bound each seek range.
        if direction == ExpandDirection::Incoming && read_epoch == snapshot.seq() {
            let mut offset = 0;
            for group in neighbors.chunk_by(|a, b| a.input == b.input && a.relationship_type == b.relationship_type) {
                for batch in group.chunks(64) {
                    if batch.len() >= SOURCE_BATCH_MIN_NEIGHBORS && batch.windows(2).all(|pair| pair[0].neighbor < pair[1].neighbor) {
                        let sources = batch.iter().map(|edge| edge.neighbor).collect::<Vec<_>>();
                        let rows = storage_result(
                            shard
                                .listed_incoming_relationship_rows(
                                    &snapshot,
                                    cell_id,
                                    &batch[0].relationship_type,
                                    batch[0].input,
                                    &sources,
                                    &budget,
                                )
                                .await,
                        )?;
                        for (slot, rows) in answered[offset..].iter_mut().zip(rows) {
                            retained = retained.saturating_add(rows.len());
                            self.check_intermediate_rows("experimental_cypher_expand_relationships", retained)?;
                            *slot = Some(rows);
                        }
                    }
                    offset += batch.len();
                }
            }
        }
        let remaining = (0..neighbors.len()).filter(|&index| answered[index].is_none()).collect::<Vec<_>>();
        // Indexed rather than borrowed item by item, for the same `Send`
        // reason as the neighbour reads.
        let mut relationship_reads = stream::iter(0..remaining.len())
            .map(|position| {
                let index = remaining[position];
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
                        SlateGraphStore::scope_snapshot(
                            snapshot,
                            shard.native_path_relationships_of_listed_edge_at(
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
                    Ok::<_, StorageError>((index, relationships))
                }
            })
            .buffered(RELATIONSHIP_EXPAND_CONCURRENCY);
        while let Some((index, relationships)) = relationship_reads.try_next().await? {
            retained = retained.saturating_add(relationships.len());
            self.check_intermediate_rows("experimental_cypher_expand_relationships", retained)?;
            answered[index] = Some(relationships);
        }
        // In listing order whichever way each pair was read, which keeps the
        // structural relationship IDs assigned below stable for a listing.
        let mut pending = Vec::new();
        for (neighbor, relationships) in neighbors.iter().zip(answered) {
            let (source, target) = match direction {
                ExpandDirection::Outgoing => (neighbor.input, neighbor.neighbor),
                ExpandDirection::Incoming => (neighbor.neighbor, neighbor.input),
            };
            let edge_type = neighbor.relationship_type.as_str();
            for (relationship_id, metadata) in relationships.expect("every listed pair was read") {
                storage_result(self.budget.check("experimental_cypher_expand_relationship"))?;
                pending.push((relationship_id, edge_type.to_string(), source, target, metadata));
                self.check_intermediate_rows("experimental_cypher_expand_relationships", pending.len())?;
            }
        }

        let mut used_ids = pending
            .iter()
            .filter_map(|(id, ..)| *id)
            .chain(self.structural_relationship_ids_in_use())
            .collect::<BTreeSet<_>>();
        let mut relationships = Vec::with_capacity(pending.len());
        for (id, edge_type, source, target, metadata) in pending {
            storage_result(self.budget.check("experimental_cypher_expand_materialize"))?;
            let id = match id {
                Some(id) => id,
                None => self.structural_relationship_id((edge_type.clone(), source, target), &mut used_ids)?,
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
            self.check_intermediate_rows("experimental_cypher_expand_materialized_rows", relationships.len())?;
        }
        Ok(relationships)
    }

    /// The relationships whose `property` equals one of `values`, as an
    /// equality seek keeps them, with every storage read made concurrently.
    ///
    /// Every value's index entries are read first. A relationship-index entry
    /// carries its relationship's id, so that relationship is one point read
    /// (`indexed_relationship_metadata_batch_at`, which also checks the
    /// relationship's own value against the entry). An endpoint pair the
    /// structural index names instead reads every relationship on the pair,
    /// since each may inherit the pair's properties; a relationship-index
    /// entry on such a pair goes with that read. Reading every pair one at a
    /// time cost one storage round trip per match: 11 s for a 67-match seek on
    /// a large production scope.
    async fn seeked_relationship_variants(
        &mut self,
        relationship_type: &str,
        property: &str,
        values: &[VertexPropertyValue],
    ) -> StorageResult<Vec<(Option<RelationshipId>, VertexId, VertexId, EdgeMetadata)>> {
        // Compared through the stored encoding, so `1` and `1.0` stay equal.
        let wanted = values.iter().map(property_to_scalar).collect::<Vec<_>>();
        let shard = self.shard;
        let cell_id = self.cell_id;
        let read_epoch = self.read_epoch;
        let budget = &self.budget;
        let several = values.len() > 1;
        let index_reads = stream::iter(values.iter().cloned())
            .map(|value| async move {
                let entries = shard
                    .scan_relationship_property_entries_at(cell_id, relationship_type, property, &value, budget)
                    .await?;
                if several {
                    // Each value's own read is bounded where it is made; their
                    // union is not, so it is charged to the query's shared
                    // tally as the vertex multi-seek charges its.
                    let pairs = entries
                        .structural_pairs
                        .iter()
                        .copied()
                        .chain(entries.identified.iter().map(|(src, dst, ..)| (*src, *dst)))
                        .collect::<BTreeSet<_>>();
                    shard.charge_query_index_candidates("cypher_relationship_in_list_index_candidates", budget, pairs.len() as u64)?;
                }
                Ok(entries)
            })
            .buffer_unordered(MULTI_SEEK_CONCURRENCY)
            .try_collect::<Vec<_>>();
        let per_value = storage_result(SlateGraphStore::scope_snapshot(Arc::clone(&self.snapshot), index_reads).await)?;
        let structural_pairs = per_value
            .iter()
            .flat_map(|entries| entries.structural_pairs.iter().copied())
            .collect::<BTreeSet<_>>();
        // Every (relationship, value) entry is kept until the batch checks it
        // against the record: a relationship can have entries for several
        // requested values, a stale one among them, and only the record
        // knows which is live. Matches are deduplicated below.
        let identified = per_value
            .into_iter()
            .flat_map(|entries| entries.identified)
            .filter(|(src, dst, ..)| !structural_pairs.contains(&(*src, *dst)))
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();

        let pair_reads = stream::iter(structural_pairs)
            .map(|(source, target)| async move {
                budget.check("experimental_cypher_relationship_property_seek")?;
                let variants = shard
                    .native_path_relationships_at(cell_id, relationship_type, source, target, read_epoch, budget)
                    .await?;
                Ok((source, target, variants))
            })
            .buffered(RELATIONSHIP_EXPAND_CONCURRENCY)
            .try_collect::<Vec<_>>();
        let pairs = storage_result(SlateGraphStore::scope_snapshot(Arc::clone(&self.snapshot), pair_reads).await)?;
        let identified_metadata = if identified.is_empty() {
            Vec::new()
        } else {
            let mut memo = IndexedRelationshipPairMemo::default();
            storage_result(
                SlateGraphStore::scope_snapshot(
                    Arc::clone(&self.snapshot),
                    shard.indexed_relationship_metadata_batch_at(IndexedRelationshipBatch {
                        cell_id,
                        edge_type: relationship_type,
                        property,
                        candidates: &identified,
                        read_epoch,
                        inherit_structural: true,
                        budget,
                        memo: &mut memo,
                    }),
                )
                .await,
            )?
        };

        let matches = |metadata: &EdgeMetadata| {
            metadata.properties.get(property).is_some_and(|stored| {
                let stored = property_to_scalar(stored);
                wanted
                    .iter()
                    .any(|value| hydradb_cypher_engine::scalar_values_equal(&stored, value))
            })
        };
        let mut pending = Vec::new();
        for (source, target, variants) in pairs {
            for (id, metadata) in variants {
                if matches(&metadata) {
                    pending.push((id, source, target, metadata));
                }
            }
        }
        let mut kept = BTreeSet::new();
        for ((source, target, id, _), metadata) in identified.into_iter().zip(identified_metadata) {
            if let Some(metadata) = metadata.filter(|metadata| matches(metadata)) {
                if kept.insert((source, target, id)) {
                    pending.push((Some(id), source, target, metadata));
                }
            }
        }
        // The pair-by-pair read's order: by endpoints, then relationship id.
        pending.sort_by_key(|(id, source, target, _)| (*source, *target, *id));
        Ok(pending)
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
            .chain(self.structural_relationship_ids_in_use())
            .collect::<BTreeSet<_>>();
        let mut relationships = Vec::with_capacity(pending.len());
        for (id, source, target, metadata) in pending {
            let id = match id {
                Some(id) => id,
                None => self.structural_relationship_id((relationship_type.to_string(), source, target), &mut used_ids)?,
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
    async fn probe_selective_equality_traced(&self, candidates: &[PropertyEqualityCandidate]) -> StorageResult<Option<Vec<VertexId>>> {
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
                        let value_keys = scope_statistics::scalar_property_keys(value);
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
                // would fail queries legacy serves, which the parity requirement rules out.
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
                result.as_ref().ok().and_then(|probed| probed.as_ref().map(Vec::len)).unwrap_or(0)
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
            SlateGraphStore::scope_snapshot(
                Arc::clone(&self.snapshot),
                self.shard
                    .probe_property_index_candidates(self.cell_id, encoded, max_probe_entries, &self.budget),
            )
            .await,
        )
    }

    fn structural_relationship_id(
        &mut self,
        identity: (String, VertexId, VertexId),
        used_ids: &mut BTreeSet<RelationshipId>,
    ) -> StorageResult<RelationshipId> {
        let mut structural = self.structural.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(id) = structural.ids.get(&identity) {
            return Ok(*id);
        }
        if self.forked {
            // A collision moves on deterministically; it would need two
            // edges of one query to share a 62-bit hash.
            let mut id = forked_structural_relationship_id(&identity);
            while used_ids.contains(&id) || structural.allocated.contains(&id) {
                id = FORKED_STRUCTURAL_ID_BASE + (id - FORKED_STRUCTURAL_ID_BASE + 1) % FORKED_STRUCTURAL_ID_SPAN;
            }
            used_ids.insert(id);
            structural.allocated.insert(id);
            structural.ids.insert(identity, id);
            return Ok(id);
        }
        // The counter stays above the forked range, so neither kind of
        // generated id can meet the other or a stored one.
        let exhausted = || StorageError::new("relationship identity space is exhausted");
        while used_ids.contains(&structural.next) || structural.allocated.contains(&structural.next) {
            structural.next = structural.next.checked_sub(1).ok_or_else(exhausted)?;
        }
        if structural.next < FORKED_STRUCTURAL_ID_BASE + FORKED_STRUCTURAL_ID_SPAN {
            return Err(exhausted());
        }
        let id = structural.next;
        used_ids.insert(id);
        structural.allocated.insert(id);
        structural.ids.insert(identity, id);
        structural.next = structural.next.saturating_sub(1);
        Ok(id)
    }

    /// Structural ids this query has handed out so far, on any of its reads.
    fn structural_relationship_ids_in_use(&self) -> std::vec::IntoIter<RelationshipId> {
        let structural = self.structural.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        structural.ids.values().copied().collect::<Vec<_>>().into_iter()
    }

    /// Charges `edges` to the query's scanned-edge total, shared by every
    /// read forked from this one, and returns the new total.
    fn charge_scanned_edges(&self, edges: usize) -> u64 {
        let edges = edges as u64;
        self.scanned_edges.fetch_add(edges, Ordering::AcqRel).saturating_add(edges)
    }

    /// Charges `edges` only if the total stays within `limit`.
    fn try_charge_scanned_edges(&self, edges: usize, limit: u64) -> bool {
        let edges = edges as u64;
        self.scanned_edges
            .try_update(Ordering::AcqRel, Ordering::Acquire, |scanned| {
                scanned.checked_add(edges).filter(|charged| *charged <= limit)
            })
            .is_ok()
    }
}

fn engine_result_to_hydradb(
    result: QueryResult,
    read_epoch: StorageSequence,
    storage_sequence: StorageSequence,
    budget: &QueryBudget,
) -> Result<QueryResultSet> {
    let columns = result.columns.into_iter().map(|column| QueryColumn::new(column.name)).collect();
    let mut rows = Vec::with_capacity(result.rows.len());
    for row in result.rows {
        budget.check("experimental_cypher_result")?;
        let values = row.values.into_iter().map(engine_value_to_hydradb).collect::<Result<Vec<_>>>()?;
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
        EngineQueryValue::RelationshipId(id) => Ok(QueryValue::Property(VertexPropertyValue::Integer(id))),
        EngineQueryValue::Count(value) => Ok(QueryValue::Count(value)),
        EngineQueryValue::Scalar(value) => scalar_to_query_value(&value),
        EngineQueryValue::List(values) => values
            .into_iter()
            .map(engine_value_to_hydradb)
            .collect::<Result<Vec<_>>>()
            .map(QueryValue::List),
        EngineQueryValue::Path(path) => engine_path_to_hydradb(*path).map(|path| QueryValue::Path(Box::new(path))),
        EngineQueryValue::Record(fields) => fields
            .into_iter()
            .map(|(name, value)| Ok((name.into_string(), engine_value_to_hydradb(value)?)))
            .collect::<Result<Vec<_>>>()
            .map(QueryValue::Record),
        EngineQueryValue::Temporal(value) => engine_temporal_to_hydradb(value).map(QueryValue::Temporal),
    }
}

/// A GQL temporal value (§4.16.6) as the fields Bolt sends and the literal
/// string HTTP does.
fn engine_temporal_to_hydradb(value: hydradb_cypher_engine::TemporalValue) -> Result<crate::query::algebra::QueryTemporal> {
    use crate::query::algebra::{QueryTemporal, QueryTemporalKind};
    use hydradb_cypher_engine::{epoch_seconds, nanos_of_day, DurationValue, TemporalValue};
    const NANOS_PER_DAY: i128 = 86_400_000_000_000;
    let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).expect("the Unix epoch is a date");
    let kind = match value {
        TemporalValue::Date(date) => QueryTemporalKind::Date {
            days: date.signed_duration_since(epoch).num_days(),
        },
        TemporalValue::LocalTime(time) => QueryTemporalKind::LocalTime {
            nanoseconds: nanos_of_day(time),
        },
        TemporalValue::ZonedTime { time, offset } => QueryTemporalKind::ZonedTime {
            nanoseconds: nanos_of_day(time),
            offset_seconds: offset,
        },
        TemporalValue::LocalDatetime(datetime) => {
            let (seconds, nanoseconds) = epoch_seconds(datetime, 0);
            QueryTemporalKind::LocalDatetime { seconds, nanoseconds }
        }
        TemporalValue::ZonedDatetime { datetime, offset } => {
            let (seconds, nanoseconds) = epoch_seconds(datetime, offset);
            QueryTemporalKind::ZonedDatetime {
                seconds,
                nanoseconds,
                offset_seconds: offset,
            }
        }
        TemporalValue::Duration(DurationValue::YearMonth { months }) => QueryTemporalKind::Duration {
            months,
            days: 0,
            seconds: 0,
            nanoseconds: 0,
        },
        TemporalValue::Duration(DurationValue::DayTime { nanoseconds }) => {
            let rest = nanoseconds % NANOS_PER_DAY;
            // A result carries its day count as a 64-bit integer; a longer
            // duration is an error, never a different duration.
            let days = i64::try_from(nanoseconds / NANOS_PER_DAY).map_err(|_| {
                experimental_query_error(
                    QueryFailureReason::Evaluation,
                    "a duration of more than 2^63 days cannot be returned",
                )
            })?;
            QueryTemporalKind::Duration {
                months: 0,
                days,
                seconds: (rest / 1_000_000_000) as i64,
                nanoseconds: (rest % 1_000_000_000) as i32,
            }
        }
    };
    Ok(QueryTemporal {
        kind,
        text: value.to_literal_string(),
    })
}

/// A GQL path value (§4.15.2) as the native path HydraDB's protocols already
/// send: every node and edge with its labels or type and its properties.
fn engine_path_to_hydradb(path: hydradb_cypher_engine::PathValue) -> Result<crate::query::algebra::QueryPath> {
    use crate::query::algebra::{QueryPath, QueryPathNode, QueryPathRelationship};
    let properties = |properties: std::collections::BTreeMap<String, ScalarValue>| {
        properties
            .into_iter()
            .filter(|(_, value)| !matches!(value, ScalarValue::Null))
            .map(|(name, value)| {
                scalar_to_property(&value)
                    .map(|value| (name, value))
                    .map_err(|error| experimental_query_error(QueryFailureReason::Evaluation, error))
            })
            .collect::<Result<std::collections::BTreeMap<_, _>>>()
    };
    Ok(QueryPath {
        nodes: path
            .nodes
            .into_iter()
            .map(|node| {
                Ok(QueryPathNode {
                    id: node.id,
                    labels: node.labels.into_iter().collect(),
                    properties: properties(node.properties)?,
                })
            })
            .collect::<Result<Vec<_>>>()?,
        relationships: path
            .relationships
            .into_iter()
            .map(|relationship| {
                Ok(QueryPathRelationship {
                    id: Some(relationship.id),
                    edge_type: relationship.relationship_type,
                    src: relationship.source,
                    dst: relationship.target,
                    properties: properties(relationship.properties)?,
                })
            })
            .collect::<Result<Vec<_>>>()?,
    })
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
            Ok(QueryValue::Property(VertexPropertyValue::Float(QueryFloat(value))))
        }
        ScalarValue::Decimal(value) => Ok(QueryValue::Decimal(value.to_string())),
        ScalarValue::Bytes(value) => Ok(QueryValue::Bytes(value.to_vec())),
        ScalarValue::Date(value) => Ok(QueryValue::Property(VertexPropertyValue::Date(*value))),
        ScalarValue::List(values) => values
            .iter()
            .map(scalar_to_query_value)
            .collect::<Result<Vec<_>>>()
            .map(QueryValue::List),
        ScalarValue::String(value) => Ok(QueryValue::Property(VertexPropertyValue::String(value.to_string()))),
    }
}

fn scalar_to_property(value: &ScalarValue) -> StorageResult<VertexPropertyValue> {
    match value {
        ScalarValue::Null => Err(StorageError::new("NULL cannot be a property seek key")),
        ScalarValue::Boolean(value) => Ok(VertexPropertyValue::Bool(*value)),
        ScalarValue::Integer(value) => scalar_integer_to_property(*value).map_err(storage_error),
        ScalarValue::Float(value) => {
            let value = value.parse::<f64>().map_err(|error| StorageError::new(error.to_string()))?;
            if !value.is_finite() {
                return Err(StorageError::new("non-finite floating-point values are not supported"));
            }
            Ok(VertexPropertyValue::Float(QueryFloat(value)))
        }
        ScalarValue::String(value) => Ok(VertexPropertyValue::String(value.to_string())),
        ScalarValue::Date(value) => Ok(VertexPropertyValue::Date(*value)),
        ScalarValue::List(values) => values
            .iter()
            .map(scalar_to_property)
            .collect::<StorageResult<Vec<_>>>()
            .and_then(|values| crate::VertexPropertyList::from_property_values(values).map_err(StorageError::new))
            .map(VertexPropertyValue::List),
        ScalarValue::Decimal(_) | ScalarValue::Bytes(_) => Err(StorageError::new("no property holds a decimal or a byte string")),
    }
}

fn scalar_integer_to_property(value: i128) -> Result<VertexPropertyValue> {
    match u64::try_from(value) {
        Ok(value) => Ok(VertexPropertyValue::Integer(value)),
        Err(_) => i64::try_from(value)
            .map(VertexPropertyValue::SignedInteger)
            .map_err(|_| experimental_query_error(QueryFailureReason::Evaluation, "integer is outside HydraDB's i64/u64 range")),
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
fn may_satisfy_lower_bound(value: Option<&VertexPropertyValue>, lower: i128, inclusive: bool) -> bool {
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
