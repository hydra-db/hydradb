use super::topology_tail::{expand_range_with_overlay, GraphTopologyOverlay, GraphTopologyTail};
use super::*;
use crate::QueryFailureReason;
#[cfg(feature = "opencypher")]
use futures::{stream, StreamExt as _, TryStreamExt as _};
#[cfg(feature = "opencypher")]
use std::sync::atomic::AtomicU64;
#[cfg(feature = "opencypher")]
use std::time::{Duration, Instant};
use tracing::Instrument as _;

#[cfg(feature = "opencypher")]
use crate::core::metrics::CypherRoute;

#[cfg(feature = "opencypher")]
const QUERY_METADATA_HYDRATION_CONCURRENCY: usize = 16;

#[cfg(feature = "opencypher")]
const QUERY_STATS_ANCHOR_READ_CONCURRENCY: usize = 8;

#[cfg(feature = "opencypher")]
pub(crate) const QUERY_NODE_EQUALITY_PROBE_ALTERNATIVES: usize = 512;

/// How many equality candidates one runtime access-path probe ranks, and so
/// how many index walks it runs at once.
///
/// The walks are concurrent and `QueryBudget::read_only_io` bounds time and
/// cancellation, not parallel I/O, so without this a wide inline property map
/// would open one object-store scan per property. The cap is also what keeps
/// the probe useful: every candidate divides one fixed candidate allowance, so
/// past a handful each slice is too small to prove anything and every walk
/// declines after paying for itself.
///
/// Candidates past the cap are dropped in predicate order. That is the
/// ordering this probe exists to stop trusting, but a vertex pattern pinned by
/// more than this many equalities is not a shape worth widening the fan-out
/// for.
#[cfg(feature = "opencypher")]
pub(crate) const QUERY_PROPERTY_PROBE_CANDIDATES: usize = 8;

#[cfg(feature = "opencypher")]
const ORDERED_PROPERTY_INDEX_TYPE_PREFIXES: [&str; 5] = ["b", "i", "j", "n", "s"];

async fn run_graph_compute<T, F>(
    metrics: Arc<GraphOperationalMetrics>,
    operation: &'static str,
    compute: F,
) -> Result<T>
where
    T: Send + 'static,
    F: FnOnce() -> Result<T> + Send + 'static,
{
    // `kernel.expand` covers the queue wait as well as the compute, because
    // `spawn_blocking` can sit behind a saturated blocking pool for longer than
    // the traversal itself takes and the aggregate counters cannot tell the two
    // apart on a single query. The span cannot follow the closure onto the
    // blocking thread, which is exactly why the wrapper is the right place for
    // it. `operation` is the rung of the sparse-kernel ladder that ran.
    let span = tracing::info_span!("kernel.expand", hydradb.kernel = operation);
    let queued_at = std::time::Instant::now();
    tokio::task::spawn_blocking(move || {
        let queue_us = queued_at
            .elapsed()
            .as_micros()
            .try_into()
            .unwrap_or(u64::MAX);
        metrics
            .graph_compute_tasks
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        metrics
            .graph_compute_queue_us
            .fetch_add(queue_us, std::sync::atomic::Ordering::Relaxed);
        let compute_started = std::time::Instant::now();
        let result = compute();
        metrics.graph_compute_duration_us.fetch_add(
            compute_started
                .elapsed()
                .as_micros()
                .try_into()
                .unwrap_or(u64::MAX),
            std::sync::atomic::Ordering::Relaxed,
        );
        result
    })
    .instrument(span)
    .await
    .map_err(|err| GraphError::CorruptValue {
        key: format!("query/compute/{operation}"),
        reason: format!("graph compute task failed: {err}"),
    })?
}

#[cfg_attr(not(feature = "opencypher"), allow(dead_code))]
fn run_graph_compute_inline<T, F>(metrics: Arc<GraphOperationalMetrics>, compute: F) -> Result<T>
where
    F: FnOnce() -> Result<T>,
{
    metrics
        .graph_compute_tasks
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let compute_started = std::time::Instant::now();
    let result = compute();
    metrics.graph_compute_duration_us.fetch_add(
        compute_started
            .elapsed()
            .as_micros()
            .try_into()
            .unwrap_or(u64::MAX),
        std::sync::atomic::Ordering::Relaxed,
    );
    result
}

impl GraphShard {
    pub(crate) async fn edge_exists_in_storage_snapshot(
        &self,
        snapshot: &GraphStorageSnapshot,
        cell_id: &str,
        edge_type: &str,
        src: VertexId,
        dst: VertexId,
        topology_sequence: StorageSequence,
    ) -> Result<bool> {
        validate_component("cell_id", cell_id)?;
        validate_component("edge_type", edge_type)?;
        let key = keys::out_edge(cell_id, edge_type, src, dst);
        if snapshot
            .get_with_options(key.as_bytes(), &remote_read_options())
            .await?
            .is_some()
        {
            return Ok(true);
        }

        let tombstone_key = keys::out_segment_tombstone(cell_id, edge_type, src, dst);
        let tombstone_epoch = match snapshot
            .get_with_options(tombstone_key.as_bytes(), &remote_read_options())
            .await?
        {
            Some(value) => {
                let epoch = decode_u64(&tombstone_key, &value)?;
                (epoch <= topology_sequence).then_some(epoch)
            }
            None => None,
        };
        let prefix = keys::out_segment_src_prefix(cell_id, edge_type, src);
        let mut iter = snapshot
            .scan_prefix_with_options(prefix.as_bytes(), .., &remote_scan_options())
            .await?;
        while let Some(kv) = iter.next().await? {
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            let segment = decode_out_edge_segment(&key, &kv.value)?;
            if segment.storage_sequence > topology_sequence {
                break;
            }
            if segment.destinations.iter().copied().any(|candidate| {
                candidate == dst && segment_edge_visible(segment.storage_sequence, tombstone_epoch)
            }) {
                return Ok(true);
            }
        }
        Ok(false)
    }

    pub(crate) async fn out_neighbors_in_storage_snapshot(
        &self,
        snapshot: &GraphStorageSnapshot,
        cell_id: &str,
        edge_type: &str,
        src: VertexId,
        topology_sequence: StorageSequence,
    ) -> Result<Vec<VertexId>> {
        self.out_neighbors_in_storage_snapshot_inner(
            snapshot,
            cell_id,
            edge_type,
            src,
            topology_sequence,
            None,
        )
        .await
    }

    #[cfg(feature = "opencypher")]
    #[allow(dead_code)] // Used only when `experimental-cypher-engine` is also enabled.
    pub(crate) async fn out_neighbors_in_storage_snapshot_for_query(
        &self,
        snapshot: &GraphStorageSnapshot,
        cell_id: &str,
        edge_type: &str,
        src: VertexId,
        topology_sequence: StorageSequence,
        budget: &QueryBudget,
    ) -> Result<Vec<VertexId>> {
        self.out_neighbors_in_storage_snapshot_inner(
            snapshot,
            cell_id,
            edge_type,
            src,
            topology_sequence,
            Some(budget),
        )
        .await
    }

    async fn out_neighbors_in_storage_snapshot_inner(
        &self,
        snapshot: &GraphStorageSnapshot,
        cell_id: &str,
        edge_type: &str,
        src: VertexId,
        topology_sequence: StorageSequence,
        budget: Option<&QueryBudget>,
    ) -> Result<Vec<VertexId>> {
        validate_component("cell_id", cell_id)?;
        validate_component("edge_type", edge_type)?;
        check_optional_query_budget(budget, "query_out_neighbors_storage_scan")?;
        let mut neighbors = BTreeSet::new();
        let scan_options = remote_scan_options_for_expected_items(
            self.degree_for_cache_admission(snapshot, &keys::degree_out(cell_id, edge_type, src))
                .await?,
        );
        check_optional_query_budget(budget, "query_out_neighbors_storage_scan")?;
        let mut scanned_records = 0_u64;

        let prefix = keys::out_prefix(cell_id, edge_type, src);
        let mut iter = snapshot
            .scan_prefix_with_options(prefix.as_bytes(), .., &scan_options)
            .await?;
        loop {
            check_optional_query_budget(budget, "query_out_neighbors_edge_scan")?;
            let Some(kv) = iter.next().await? else {
                break;
            };
            check_optional_query_budget(budget, "query_out_neighbors_edge_scan")?;
            scanned_records = scanned_records.saturating_add(1);
            self.ensure_optional_query_scan_limit(
                budget,
                "query_out_neighbors_storage_records",
                scanned_records,
            )?;
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            neighbors.insert(decode_edge_record(&key, &kv.value)?.dst);
            self.ensure_optional_query_intermediate_limit(
                budget,
                "query_out_neighbors_vertices",
                neighbors.len(),
            )?;
        }

        let tombstone_prefix = keys::out_segment_tombstone_src_prefix(cell_id, edge_type, src);
        let mut tombstone_iter = snapshot
            .scan_prefix_with_options(tombstone_prefix.as_bytes(), .., &remote_scan_options())
            .await?;
        let mut tombstones = BTreeMap::new();
        loop {
            check_optional_query_budget(budget, "query_out_neighbors_tombstone_scan")?;
            let Some(kv) = tombstone_iter.next().await? else {
                break;
            };
            check_optional_query_budget(budget, "query_out_neighbors_tombstone_scan")?;
            scanned_records = scanned_records.saturating_add(1);
            self.ensure_optional_query_scan_limit(
                budget,
                "query_out_neighbors_storage_records",
                scanned_records,
            )?;
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            let (key_cell_id, key_edge_type, key_src, dst) =
                parse_out_edge_segment_tombstone_key(&key)?;
            if key_cell_id != cell_id || key_edge_type != edge_type || key_src != src {
                return Err(GraphError::CorruptValue {
                    key,
                    reason: "segment tombstone identity does not match snapshot prefix".to_string(),
                });
            }
            let epoch = decode_u64(&key, &kv.value)?;
            if epoch <= topology_sequence {
                tombstones.insert(dst, epoch);
                self.ensure_optional_query_intermediate_limit(
                    budget,
                    "query_out_neighbors_tombstones",
                    tombstones.len(),
                )?;
            }
        }

        let segment_prefix = keys::out_segment_src_prefix(cell_id, edge_type, src);
        let mut segment_iter = snapshot
            .scan_prefix_with_options(segment_prefix.as_bytes(), .., &remote_scan_options())
            .await?;
        loop {
            check_optional_query_budget(budget, "query_out_neighbors_segment_scan")?;
            let Some(kv) = segment_iter.next().await? else {
                break;
            };
            check_optional_query_budget(budget, "query_out_neighbors_segment_scan")?;
            scanned_records = scanned_records.saturating_add(1);
            self.ensure_optional_query_scan_limit(
                budget,
                "query_out_neighbors_storage_records",
                scanned_records,
            )?;
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            let segment = decode_out_edge_segment(&key, &kv.value)?;
            if segment.storage_sequence > topology_sequence {
                break;
            }
            for dst in segment.destinations.iter().copied() {
                check_optional_query_budget(budget, "query_out_neighbors_segment_edges")?;
                scanned_records = scanned_records.saturating_add(1);
                self.ensure_optional_query_scan_limit(
                    budget,
                    "query_out_neighbors_storage_records",
                    scanned_records,
                )?;
                if segment_edge_visible(segment.storage_sequence, tombstones.get(&dst).copied()) {
                    neighbors.insert(dst);
                    self.ensure_optional_query_intermediate_limit(
                        budget,
                        "query_out_neighbors_vertices",
                        neighbors.len(),
                    )?;
                }
            }
        }
        Ok(neighbors.into_iter().collect())
    }

    fn ensure_optional_query_scan_limit(
        &self,
        budget: Option<&QueryBudget>,
        operation: &'static str,
        records: u64,
    ) -> Result<()> {
        if budget.is_some() {
            ensure_limit(operation, records, self.limits.max_query_scan_edges)?;
        }
        Ok(())
    }

    fn ensure_optional_query_intermediate_limit(
        &self,
        budget: Option<&QueryBudget>,
        operation: &'static str,
        rows: usize,
    ) -> Result<()> {
        if budget.is_some() {
            ensure_limit(
                operation,
                rows as u64,
                self.limits.max_query_intermediate_rows as u64,
            )?;
        }
        Ok(())
    }

    pub(crate) async fn out_degree_in_storage_snapshot(
        &self,
        snapshot: &GraphStorageSnapshot,
        cell_id: &str,
        edge_type: &str,
        src: VertexId,
    ) -> Result<u64> {
        validate_component("cell_id", cell_id)?;
        validate_component("edge_type", edge_type)?;
        let key = keys::degree_out(cell_id, edge_type, src);
        match snapshot
            .get_with_options(key.as_bytes(), &remote_read_options())
            .await?
        {
            Some(value) => decode_u64(&key, &value),
            None => Ok(0),
        }
    }

    #[cfg(feature = "opencypher")]
    pub(crate) async fn in_degree_in_storage_snapshot(
        &self,
        snapshot: &GraphStorageSnapshot,
        cell_id: &str,
        edge_type: &str,
        dst: VertexId,
    ) -> Result<u64> {
        validate_component("cell_id", cell_id)?;
        validate_component("edge_type", edge_type)?;
        let key = keys::degree_in(cell_id, edge_type, dst);
        match snapshot
            .get_with_options(key.as_bytes(), &remote_read_options())
            .await?
        {
            Some(value) => decode_u64(&key, &value),
            None => Ok(0),
        }
    }

    pub(crate) async fn in_neighbors_in_storage_snapshot(
        &self,
        snapshot: &GraphStorageSnapshot,
        cell_id: &str,
        edge_type: &str,
        dst: VertexId,
    ) -> Result<Vec<VertexId>> {
        validate_component("cell_id", cell_id)?;
        validate_component("edge_type", edge_type)?;
        let scan_options = remote_scan_options_for_expected_items(
            self.degree_for_cache_admission(snapshot, &keys::degree_in(cell_id, edge_type, dst))
                .await?,
        );
        let prefix = keys::in_prefix(cell_id, edge_type, dst);
        let mut iter = snapshot
            .scan_prefix_with_options(prefix.as_bytes(), .., &scan_options)
            .await?;
        let mut neighbors = BTreeSet::new();
        while let Some(kv) = iter.next().await? {
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            neighbors.insert(decode_edge_record(&key, &kv.value)?.src);
        }
        Ok(neighbors.into_iter().collect())
    }

    async fn degree_for_cache_admission(
        &self,
        snapshot: &GraphStorageSnapshot,
        key: &str,
    ) -> Result<u64> {
        match snapshot
            .get_with_options(key.as_bytes(), &remote_read_options())
            .await?
        {
            Some(value) => decode_u64(key, &value),
            // Missing counters must fail closed for cache admission. The scan
            // remains correct; it simply cannot displace known hot blocks.
            None => Ok(u64::MAX),
        }
    }

    pub async fn execute_cypher(&self, context: QueryContext, query: &str) -> Result<QueryOutput> {
        if context.cypher_engine == CypherEngineMode::Experimental {
            #[cfg(feature = "opencypher")]
            {
                if let Some(procedure) = crate::query::path_procedure::parse_native_path_procedure(
                    query,
                    &context.parameters,
                    self.limits.max_traversal_hops,
                )? {
                    self.operation_metrics
                        .record_cypher_route(CypherRoute::NativePathFallback);
                    return Box::pin(self.execute_native_path_rows(context, procedure))
                        .await
                        .map(QueryOutput::Rows);
                }
                if let Some(parsed) = parse_opencypher_mutation_query_with_list_parameters(
                    query,
                    &context.parameters,
                    &context.list_parameters,
                )? {
                    self.operation_metrics
                        .record_cypher_route(CypherRoute::MutationFallback);
                    let result =
                        Box::pin(self.execute_parsed_opencypher_mutation(context, parsed)).await?;
                    return Ok(QueryOutput::Mutation(result));
                }
            }
            return self
                .execute_cypher_rows(context, query)
                .await
                .map(QueryOutput::Rows);
        }
        #[cfg(feature = "opencypher")]
        self.operation_metrics
            .record_cypher_route(CypherRoute::Legacy);
        Box::pin(self.execute_opencypher(context, query)).await
    }

    pub async fn execute_cypher_rows(
        &self,
        context: QueryContext,
        query: &str,
    ) -> Result<QueryResultSet> {
        if context.cypher_engine == CypherEngineMode::Experimental {
            #[cfg(feature = "opencypher")]
            {
                if let Some(procedure) = crate::query::path_procedure::parse_native_path_procedure(
                    query,
                    &context.parameters,
                    self.limits.max_traversal_hops,
                )? {
                    self.operation_metrics
                        .record_cypher_route(CypherRoute::NativePathFallback);
                    return Box::pin(self.execute_native_path_rows(context, procedure)).await;
                }
                if let Some(parsed) = parse_opencypher_mutation_query_with_list_parameters(
                    query,
                    &context.parameters,
                    &context.list_parameters,
                )? {
                    self.operation_metrics
                        .record_cypher_route(CypherRoute::MutationFallback);
                    let mutation =
                        Box::pin(self.execute_parsed_opencypher_mutation(context, parsed)).await?;
                    return Ok(mutation.into_result_set());
                }
            }
            #[cfg(feature = "experimental-cypher-engine")]
            {
                self.operation_metrics
                    .record_cypher_route(CypherRoute::Experimental);
                return super::experimental_cypher::execute_experimental_cypher_rows(
                    self, context, query,
                )
                .await;
            }
            #[cfg(not(feature = "experimental-cypher-engine"))]
            {
                return Err(GraphError::UnsupportedQuery {
                    reason: QueryFailureReason::Other,
                    dialect: "Cypher25",
                    feature: "the experimental-cypher-engine Cargo feature is not enabled"
                        .to_string(),
                });
            }
        }
        #[cfg(feature = "opencypher")]
        self.operation_metrics
            .record_cypher_route(CypherRoute::Legacy);
        Box::pin(self.execute_opencypher_rows(context, query)).await
    }

    pub async fn execute_cypher_rows_page(
        &self,
        context: QueryContext,
        query: &str,
        cursor: Option<QueryCursorToken>,
        page_size: usize,
    ) -> Result<QueryResultPage> {
        if context.cypher_engine == CypherEngineMode::Experimental {
            #[cfg(feature = "opencypher")]
            {
                if let Some(procedure) = crate::query::path_procedure::parse_native_path_procedure(
                    query,
                    &context.parameters,
                    self.limits.max_traversal_hops,
                )? {
                    self.operation_metrics
                        .record_cypher_route(CypherRoute::NativePathFallback);
                    return Box::pin(self.execute_native_path_rows_page(
                        context, query, procedure, cursor, page_size,
                    ))
                    .await;
                }
            }
            #[cfg(feature = "opencypher")]
            if cursor.is_none() {
                if let Some(parsed) = parse_opencypher_mutation_query_with_list_parameters(
                    query,
                    &context.parameters,
                    &context.list_parameters,
                )? {
                    self.operation_metrics
                        .record_cypher_route(CypherRoute::MutationFallback);
                    let mutation =
                        Box::pin(self.execute_parsed_opencypher_mutation(context, parsed)).await?;
                    let rows = mutation.into_result_set();
                    return Ok(QueryResultPage::new(rows.columns, rows.rows, None));
                }
            }
            return Err(GraphError::UnsupportedQuery {
                reason: QueryFailureReason::InvalidRequest,
                dialect: "Cypher25",
                feature: if cursor.is_some() {
                    "experimental mutations cannot continue from a cursor"
                } else {
                    "direct experimental read pages are not implemented; use the client server cursor"
                }
                .to_string(),
            });
        }
        #[cfg(feature = "opencypher")]
        self.operation_metrics
            .record_cypher_route(CypherRoute::Legacy);
        Box::pin(self.execute_opencypher_rows_page(context, query, cursor, page_size)).await
    }

    pub async fn execute_opencypher(
        &self,
        context: QueryContext,
        query: &str,
    ) -> Result<QueryOutput> {
        #[cfg(feature = "opencypher")]
        {
            if let Some(procedure) = crate::query::path_procedure::parse_native_path_procedure(
                query,
                &context.parameters,
                self.limits.max_traversal_hops,
            )? {
                return Box::pin(self.execute_native_path_rows(context, procedure))
                    .await
                    .map(QueryOutput::Rows);
            }
            if let Some(parsed) = parse_opencypher_mutation_query_with_list_parameters(
                query,
                &context.parameters,
                &context.list_parameters,
            )? {
                let result =
                    Box::pin(self.execute_parsed_opencypher_mutation(context, parsed)).await?;
                return Ok(QueryOutput::Mutation(result));
            }
            let parsed = self
                .parsed_opencypher_row_query(
                    &context.cell_id,
                    query,
                    &context.parameters,
                    &context.list_parameters,
                )
                .await?;
            let context = merge_opencypher_window(context, opencypher_outer_window(&parsed))?;
            let result_set =
                Box::pin(self.execute_parsed_opencypher_rows(context, parsed.clone())).await?;
            Ok(query_result_set_to_output(&parsed, result_set))
        }
        #[cfg(not(feature = "opencypher"))]
        {
            let _ = (context, query);
            Err(GraphError::UnsupportedQuery {
                reason: QueryFailureReason::Other,
                dialect: "OpenCypher",
                feature: "enable the opencypher Cargo feature to parse Cypher".to_string(),
            })
        }
    }

    pub async fn execute_opencypher_rows(
        &self,
        context: QueryContext,
        query: &str,
    ) -> Result<QueryResultSet> {
        #[cfg(feature = "opencypher")]
        {
            if let Some(procedure) = crate::query::path_procedure::parse_native_path_procedure(
                query,
                &context.parameters,
                self.limits.max_traversal_hops,
            )? {
                return Box::pin(self.execute_native_path_rows(context, procedure)).await;
            }
            let parsed = self
                .parsed_opencypher_row_query(
                    &context.cell_id,
                    query,
                    &context.parameters,
                    &context.list_parameters,
                )
                .await?;
            let context = merge_opencypher_window(context, opencypher_outer_window(&parsed))?;
            Box::pin(self.execute_parsed_opencypher_rows(context, parsed)).await
        }
        #[cfg(not(feature = "opencypher"))]
        {
            let _ = (context, query);
            Err(GraphError::UnsupportedQuery {
                reason: QueryFailureReason::Other,
                dialect: "OpenCypher",
                feature: "enable the opencypher Cargo feature to parse Cypher".to_string(),
            })
        }
    }

    pub async fn execute_opencypher_rows_page(
        &self,
        context: QueryContext,
        query: &str,
        cursor: Option<QueryCursorToken>,
        page_size: usize,
    ) -> Result<QueryResultPage> {
        #[cfg(feature = "opencypher")]
        {
            if let Some(procedure) = crate::query::path_procedure::parse_native_path_procedure(
                query,
                &context.parameters,
                self.limits.max_traversal_hops,
            )? {
                return Box::pin(
                    self.execute_native_path_rows_page(
                        context, query, procedure, cursor, page_size,
                    ),
                )
                .await;
            }
            let parsed = self
                .parsed_opencypher_row_query(
                    &context.cell_id,
                    query,
                    &context.parameters,
                    &context.list_parameters,
                )
                .await?;
            let context = merge_opencypher_window(context, opencypher_outer_window(&parsed))?;
            if context.read_epoch.is_some() && context.validated_read_epoch().is_none() {
                return Err(GraphError::UnsupportedQuery {
                    reason: QueryFailureReason::InvalidRequest,
                    dialect: "OpenCypher",
                    feature: "historical graph epochs are not storage snapshots; execute against a current SlateDB snapshot"
                        .to_string(),
                });
            }
            if context.read_epoch.is_none() {
                let snapshot = if context.uses_refreshed_reader() {
                    self.db.reader_snapshot().await?
                } else {
                    self.db.snapshot().await?
                };
                let read_epoch = snapshot.seq();
                let context = context.with_validated_storage_read_epoch(read_epoch, read_epoch);
                return GraphStore::scope_snapshot(
                    snapshot,
                    Box::pin(
                        self.execute_parsed_opencypher_rows_page(
                            context, parsed, cursor, page_size,
                        ),
                    ),
                )
                .await;
            }
            Box::pin(self.execute_parsed_opencypher_rows_page(context, parsed, cursor, page_size))
                .await
        }
        #[cfg(not(feature = "opencypher"))]
        {
            let _ = (context, query, cursor, page_size);
            Err(GraphError::UnsupportedQuery {
                reason: QueryFailureReason::Other,
                dialect: "OpenCypher",
                feature: "enable the opencypher Cargo feature to parse Cypher".to_string(),
            })
        }
    }

    #[cfg(feature = "opencypher")]
    async fn execute_parsed_opencypher_rows_page(
        &self,
        context: QueryContext,
        mut parsed: ParsedRowQuery,
        cursor: Option<QueryCursorToken>,
        page_size: usize,
    ) -> Result<QueryResultPage> {
        let cursor_offset = cursor.map_or(0, |cursor| cursor.offset);
        let started = std::time::Instant::now();
        let result_budget = QueryBudget::new(
            context.max_runtime_ms.or(self.limits.max_query_runtime_ms),
            context.cancellation_token.clone(),
        )
        .with_max_result_bytes(context.max_result_bytes);
        match self
            .try_execute_source_relationship_id_rows_page(
                &context,
                &parsed,
                cursor_offset,
                page_size,
            )
            .await
        {
            Ok(Some(page)) => {
                if let Err(error) = result_budget.account_result_rows(&page.rows) {
                    self.record_streaming_query_rows_failure(started, &error);
                    return Err(error);
                }
                self.record_streaming_query_rows_success(page.rows.len(), started);
                return Ok(page);
            }
            Ok(None) => {}
            Err(err) => {
                self.record_streaming_query_rows_failure(started, &err);
                return Err(err);
            }
        }
        match Box::pin(self.try_execute_graph_kernel_opencypher_rows_page(
            &context,
            &parsed,
            cursor_offset,
            page_size,
        ))
        .await
        {
            Ok(Some(page)) => {
                if let Err(error) = result_budget.account_result_rows(&page.rows) {
                    self.record_streaming_query_rows_failure(started, &error);
                    return Err(error);
                }
                self.record_streaming_query_rows_success(page.rows.len(), started);
                return Ok(page);
            }
            Ok(None) => {}
            Err(err) => {
                self.record_streaming_query_rows_failure(started, &err);
                return Err(err);
            }
        }

        match self
            .try_execute_streaming_opencypher_rows_page(&context, &parsed, cursor_offset, page_size)
            .await
        {
            Ok(Some(page)) => {
                if let Err(error) = result_budget.account_result_rows(&page.rows) {
                    self.record_streaming_query_rows_failure(started, &error);
                    return Err(error);
                }
                self.record_streaming_query_rows_success(page.rows.len(), started);
                return Ok(page);
            }
            Ok(None) => {}
            Err(err) => {
                self.record_streaming_query_rows_failure(started, &err);
                return Err(err);
            }
        }

        let mut context = self.query_page_context(context, cursor_offset, page_size)?;
        // The execution window includes one probe row to determine whether a
        // cursor follows. Account only the page after that probe is removed.
        context.max_result_bytes = None;
        parsed.window = QueryWindow::default();
        let mut result_set = Box::pin(self.execute_parsed_opencypher_rows(context, parsed)).await?;
        let next_cursor = if result_set.rows.len() > page_size {
            result_set.rows.truncate(page_size);
            Some(QueryCursorToken::new(
                cursor_offset.checked_add(page_size as u64).ok_or_else(|| {
                    GraphError::AdmissionRejected {
                        operation: "query_cursor_offset",
                        actual: u64::MAX,
                        limit: u64::MAX - 1,
                    }
                })?,
            ))
        } else {
            None
        };
        let page = QueryResultPage::new(result_set.columns, result_set.rows, next_cursor);
        result_budget.account_result_rows(&page.rows)?;
        Ok(page)
    }

    pub async fn execute_query_statement(
        &self,
        context: QueryContext,
        statement: QueryStatement,
    ) -> Result<QueryOutput> {
        let plan = self.plan_query_statement(context, statement)?;
        self.execute_query_plan(plan).await
    }

    pub fn plan_query_statement(
        &self,
        context: QueryContext,
        statement: QueryStatement,
    ) -> Result<QueryPlan> {
        QueryPlanner::plan(&context, &statement)
    }

    #[cfg(feature = "opencypher")]
    pub async fn explain_opencypher_rows(
        &self,
        context: QueryContext,
        query: &str,
    ) -> Result<RowQueryPlan> {
        validate_component("cell_id", &context.cell_id)?;
        if context.read_epoch.is_some() && context.validated_read_epoch().is_none() {
            return Err(GraphError::UnsupportedQuery {
                reason: QueryFailureReason::InvalidRequest,
                dialect: "OpenCypher",
                feature: "historical graph epochs are not storage snapshots; execute against a current SlateDB snapshot".to_string(),
            });
        }
        let parsed = self
            .parsed_opencypher_row_query(
                &context.cell_id,
                query,
                &context.parameters,
                &context.list_parameters,
            )
            .await?;
        let context = merge_opencypher_window(context, opencypher_outer_window(&parsed))?;
        let read_epoch = self.query_read_epoch(&context).await?;
        self.explain_row_query_plan_with_stats(&context.cell_id, read_epoch, &parsed)
            .await
    }

    #[cfg(feature = "opencypher")]
    async fn parsed_opencypher_row_query(
        &self,
        cell_id: &str,
        query: &str,
        parameters: &BTreeMap<String, VertexPropertyValue>,
        lists: &BTreeMap<String, Vec<VertexPropertyValue>>,
    ) -> Result<ParsedRowQuery> {
        // Parameter values are baked into the lowered tree, so anything
        // parameterised must bypass the cache — lists included.
        if !parameters.is_empty() || !lists.is_empty() {
            return parse_opencypher_row_query_with_list_parameters(query, parameters, lists);
        }
        let key = ParsedRowQueryCacheKey::new(query);
        if let Some(parsed) = self.parsed_row_query_cache.lock().await.get(&key) {
            self.cache_metrics
                .record_hit(GraphCacheKind::ParsedRowQuery);
            return Ok(parsed);
        }

        self.cache_metrics
            .record_miss(GraphCacheKind::ParsedRowQuery);
        let parsed = parse_opencypher_row_query_with_list_parameters(query, parameters, lists)?;
        self.parsed_row_query_cache.lock().await.insert(
            key,
            parsed.clone(),
            cell_id.to_string(),
            false,
            &self.cache_metrics,
        );
        Ok(parsed)
    }

    #[cfg(feature = "opencypher")]
    // `skip_all` is not tidiness: `query` holds the lowered predicate tree with
    // every parameter value already substituted in, and `context` holds the
    // parameter map itself. Neither may ever reach a span.
    #[tracing::instrument(
        name = "query.execute",
        level = "info",
        skip_all,
        fields(
            hydradb.cell_id = %context.cell_id,
            hydradb.read_epoch = tracing::field::Empty,
            hydradb.query.rows_returned = tracing::field::Empty,
            hydradb.query.runtime_property_index = tracing::field::Empty,
            error.class = tracing::field::Empty,
            error.operation = tracing::field::Empty,
            hydradb.sampling.tail_keep = tracing::field::Empty,
        )
    )]
    async fn execute_parsed_opencypher_rows(
        &self,
        context: QueryContext,
        query: ParsedRowQuery,
    ) -> Result<QueryResultSet> {
        if context.read_epoch.is_some() && context.validated_read_epoch().is_none() {
            return Err(GraphError::UnsupportedQuery {
                reason: QueryFailureReason::InvalidRequest,
                dialect: "OpenCypher",
                feature: "historical graph epochs are not storage snapshots; execute against a current SlateDB snapshot"
                    .to_string(),
            });
        }
        self.operation_metrics
            .query_rows_started
            .fetch_add(1, Ordering::Relaxed);
        let started = std::time::Instant::now();
        let result = if context.read_epoch.is_none() {
            // The epoch every freshness bug is an argument about is decided
            // here, by whichever of the two readers the context selected — so
            // the choice is a field, not an inference from which code path ran.
            let snapshot = async {
                if context.uses_refreshed_reader() {
                    self.db.reader_snapshot().await
                } else {
                    self.db.snapshot().await
                }
            }
            .instrument(tracing::info_span!(
                "storage.snapshot",
                hydradb.cell_id = %context.cell_id,
                refreshed_reader = context.uses_refreshed_reader(),
            ))
            .await;
            match snapshot {
                Ok(snapshot) => {
                    let read_epoch = snapshot.seq();
                    tracing::Span::current().record("hydradb.read_epoch", read_epoch);
                    let context = context.with_validated_storage_read_epoch(read_epoch, read_epoch);
                    GraphStore::scope_snapshot(
                        snapshot,
                        Box::pin(self.execute_parsed_opencypher_rows_inner(context, query)),
                    )
                    .await
                }
                Err(err) => Err(err),
            }
        } else {
            Box::pin(self.execute_parsed_opencypher_rows_inner(context, query)).await
        };
        self.operation_metrics
            .query_rows_latency
            .record(started.elapsed());
        let span = tracing::Span::current();
        match &result {
            Ok(result_set) => {
                self.operation_metrics
                    .query_rows_completed
                    .fetch_add(1, Ordering::Relaxed);
                self.operation_metrics
                    .query_rows_returned
                    .fetch_add(result_set.rows.len() as u64, Ordering::Relaxed);
                span.record("hydradb.query.rows_returned", result_set.rows.len() as u64);
                if let Some(read_epoch) = result_set.read_epoch {
                    span.record("hydradb.read_epoch", read_epoch);
                }
            }
            Err(err) => {
                self.operation_metrics.record_query_rows_failure(err);
                span.record("error.class", err.class());
                if let Some(operation) = err.limit_operation() {
                    span.record("error.operation", operation);
                }
                span.record("hydradb.sampling.tail_keep", "error");
            }
        }
        result
    }

    #[cfg(feature = "opencypher")]
    async fn execute_parsed_opencypher_rows_inner(
        &self,
        context: QueryContext,
        query: ParsedRowQuery,
    ) -> Result<QueryResultSet> {
        validate_component("cell_id", &context.cell_id)?;
        let budget = QueryBudget::new(
            context.max_runtime_ms.or(self.limits.max_query_runtime_ms),
            context.cancellation_token.clone(),
        )
        .with_max_result_bytes(context.max_result_bytes);
        budget.check("cypher_rows")?;
        let storage_sequence = context.validated_storage_sequence();
        let read_epoch = self.query_read_epoch(&context).await?;

        let result = if !query.union_arms.is_empty() {
            self.execute_union_opencypher_rows(
                &context.cell_id,
                read_epoch,
                query,
                context.result_window,
                &budget,
            )
            .await
        } else {
            self.execute_single_opencypher_rows(
                &context.cell_id,
                read_epoch,
                query,
                context.result_window,
                &budget,
            )
            .await
        }?;
        budget.account_result_rows(&result.rows)?;
        let result = result.with_read_epoch(read_epoch);
        Ok(match storage_sequence {
            Some(sequence) => result.with_storage_sequence(sequence),
            None => result,
        })
    }

    #[cfg(feature = "opencypher")]
    async fn execute_union_opencypher_rows(
        &self,
        cell_id: &str,
        read_epoch: StorageSequence,
        mut query: ParsedRowQuery,
        window: QueryWindow,
        budget: &QueryBudget,
    ) -> Result<QueryResultSet> {
        budget.check("cypher_union")?;
        let union_all = query.union_all;
        let mut arms = std::mem::take(&mut query.union_arms);
        query.union_all = false;
        let columns = query.columns.clone();

        let first_window = query.window;
        let mut rows = self
            .execute_single_opencypher_rows(cell_id, read_epoch, query, first_window, budget)
            .await?
            .rows;
        for arm in arms.drain(..) {
            budget.check("cypher_union_arm")?;
            if arm.columns != columns {
                return Err(GraphError::UnsupportedQuery {
                    reason: QueryFailureReason::Union,
                    dialect: "OpenCypher",
                    feature: "UNION arms must project the same column names".to_string(),
                });
            }
            let arm_window = arm.window;
            rows.extend(
                self.execute_single_opencypher_rows(cell_id, read_epoch, arm, arm_window, budget)
                    .await?
                    .rows,
            );
        }

        if !union_all {
            let mut seen = BTreeSet::new();
            rows.retain(|row| seen.insert(row.values.clone()));
        }

        let projected = rows
            .into_iter()
            .map(|row| ProjectedQueryRow {
                row,
                sort_keys: Vec::new(),
            })
            .collect();
        self.finish_projected_rows(columns, projected, &[], false, window, budget)
    }

    #[cfg(feature = "opencypher")]
    async fn execute_single_opencypher_rows(
        &self,
        cell_id: &str,
        read_epoch: StorageSequence,
        query: ParsedRowQuery,
        window: QueryWindow,
        budget: &QueryBudget,
    ) -> Result<QueryResultSet> {
        if let Some(result) = self
            .try_execute_graph_kernel_row_query(cell_id, read_epoch, &query, window, budget)
            .await?
        {
            return Ok(result);
        }
        if let Some(result) = self
            .try_execute_source_relationship_id_rows_query(
                cell_id, read_epoch, &query, window, budget,
            )
            .await?
        {
            return Ok(result);
        }
        if let Some(result) = self
            .try_execute_relationship_count_query(cell_id, read_epoch, &query, window, budget)
            .await?
        {
            return Ok(result);
        }
        if let Some(result) = self
            .try_execute_ordered_relationship_property_rows_query(
                cell_id, read_epoch, &query, window, budget,
            )
            .await?
        {
            return Ok(result);
        }
        if let Some(result) = self
            .try_execute_relationship_rows_query(cell_id, read_epoch, &query, window, budget)
            .await?
        {
            return Ok(result);
        }
        if let Some(result) = Box::pin(self.try_execute_ordered_string_vertex_rows_query(
            cell_id, read_epoch, &query, window, budget,
        ))
        .await?
        {
            return Ok(result);
        }

        let bindings = if query.pattern_groups.is_empty() {
            let group = RowMatchGroup {
                patterns: query.patterns.clone(),
                predicate: query.predicate.clone(),
                optional: false,
            };
            let seeds = self
                .node_equality_predicate_seed_rows(
                    cell_id,
                    &group,
                    read_epoch,
                    budget,
                    &BindingRow::default(),
                )
                .await?
                .unwrap_or_else(|| vec![BindingRow::default()]);
            let mut bindings = self
                .match_row_patterns_from_rows(cell_id, &query.patterns, read_epoch, budget, seeds)
                .await?;
            if let Some(predicate) = &query.predicate {
                let mut filtered = Vec::with_capacity(bindings.len());
                for row in bindings {
                    budget.check("cypher_where")?;
                    if row_predicate_matches(&row, predicate)? {
                        filtered.push(row);
                    }
                }
                bindings = filtered;
            }
            bindings
        } else {
            self.match_row_pattern_groups(cell_id, &query.pattern_groups, read_epoch, budget)
                .await?
        };

        if row_projections_have_aggregates(&query.projections) {
            let projected = aggregate_projected_rows(
                bindings,
                &query.projections,
                &query.columns,
                &query.order_by,
                budget,
            )?;
            return self.finish_projected_rows(
                query.columns,
                projected,
                &query.order_by,
                query.distinct,
                window,
                budget,
            );
        }

        let mut projected = Vec::with_capacity(bindings.len());
        for binding in &bindings {
            budget.check("cypher_project")?;
            let row = project_binding_row(binding, &query.projections)?;
            let sort_keys = sort_keys_for_row(binding, &row, &query.columns, &query.order_by)?;
            push_projected_query_row(&mut projected, row, sort_keys);
        }
        self.finish_projected_rows(
            query.columns,
            projected,
            &query.order_by,
            query.distinct,
            window,
            budget,
        )
    }

    #[cfg(feature = "opencypher")]
    async fn try_execute_ordered_string_vertex_rows_query(
        &self,
        cell_id: &str,
        read_epoch: StorageSequence,
        query: &ParsedRowQuery,
        window: QueryWindow,
        budget: &QueryBudget,
    ) -> Result<Option<QueryResultSet>> {
        let Some(spec) = ordered_string_vertex_index_spec(query, window) else {
            return Ok(None);
        };
        let OrderedStringVertexIndexSpec {
            node,
            binding,
            property,
            predicate,
            ascending,
            limit,
        } = spec;
        if limit == 0 {
            return Ok(Some(QueryResultSet::new(query.columns.clone(), Vec::new())));
        }

        let skip = usize::try_from(window.skip).map_err(|_| GraphError::AdmissionRejected {
            operation: "query_result_skip",
            actual: window.skip,
            limit: usize::MAX as u64,
        })?;
        let required = skip
            .checked_add(limit)
            .ok_or(GraphError::AdmissionRejected {
                operation: "query_result_window",
                actual: u64::MAX,
                limit: usize::MAX as u64,
            })?;
        // A complete equality union can filter the ordered index before any
        // metadata I/O. Retained IDs obey the existing index-candidate cap;
        // metadata rows still obey the independent intermediate-row cap.
        let group = RowMatchGroup {
            patterns: query.patterns.clone(),
            predicate: query.predicate.clone(),
            optional: false,
        };
        let hydrate_seed = |vertices: Vec<VertexId>| async move {
            let mut projected = OrderedRowWindow::new(required, &query.order_by);
            for batch in vertices.chunks(QUERY_METADATA_HYDRATION_CONCURRENCY) {
                for (vertex, metadata) in self
                    .vertex_metadata_batch_at(cell_id, batch, read_epoch, budget)
                    .await?
                {
                    budget.check("cypher_ordered_vertex_equality_project")?;
                    let Some(mut row) = BindingRow::from_node(node, vertex) else {
                        continue;
                    };
                    row.metadata.insert(binding.to_string(), metadata);
                    if !row_matches_node(&row, node)? || !row_predicate_matches(&row, predicate)? {
                        continue;
                    }
                    let result = project_binding_row(&row, &query.projections)?;
                    let sort_keys =
                        sort_keys_for_row(&row, &result, &query.columns, &query.order_by)?;
                    projected.push(result, sort_keys);
                    self.ensure_query_intermediate_rows(
                        "cypher_ordered_vertex_window",
                        projected.len(),
                    )?;
                }
            }
            self.finish_projected_rows(
                query.columns.clone(),
                projected.into_rows(),
                &query.order_by,
                false,
                window,
                budget,
            )
        };
        // The equality probe is speculative: a broad cold prefix must not
        // delay an ordered page that can already produce the exact result.
        let equality = async {
            let Some((_, vertices)) = self
                .node_equality_predicate_seed_vertices(
                    cell_id,
                    &group,
                    read_epoch,
                    budget,
                    &BindingRow::default(),
                    NodeEqualityProbe::Ordered(256),
                )
                .await?
            else {
                return Ok::<_, GraphError>(None);
            };
            Ok(Some(hydrate_seed(vertices).await?))
        };
        let ordered = async {
            let mut seed: Option<Vec<VertexId>> = None;
            let mut seed_probed = false;
            let mut examined = 0_usize;
            let mut rejected = 0_usize;

            let prefix = format!(
                "{}s",
                keys::vertex_property_index_property_prefix(cell_id, property)
            );
            let order = if ascending {
                slatedb::IterationOrder::Ascending
            } else {
                slatedb::IterationOrder::Descending
            };
            let ordered_scan_options = remote_scan_options().with_order(order);
            // Keyset pagination must seek to its lower bound instead of replaying
            // the complete property index on every page. The prefix already fixes
            // the string type marker (`s`), so the SlateDB subrange starts at the
            // encoded string payload that follows it.
            let start_suffix = ascending
                .then(|| string_property_lower_bound(predicate, binding, property))
                .flatten()
                .map(|bound| {
                    encode_vertex_property_value_key(&VertexPropertyValue::String(bound))
                        .strip_prefix('s')
                        .unwrap_or_default()
                        .as_bytes()
                        .to_vec()
                });
            // Admit only a bounded leading page into the shared block cache. The
            // actual scan stays non-admitting: predicates and primary-key ties can
            // make a small LIMIT walk a large index. A short warm-up avoids paying
            // the same first-block S3 reads on every ordered page without admitting
            // the complete long scan or changing its result/scan-limit semantics.
            if self.db.has_block_cache() {
                let warm_options = remote_scan_options()
                    .with_order(order)
                    .with_cache_blocks(true);
                let mut warm = budget
                    .read_only_io(
                        "cypher_ordered_vertex_index_warm",
                        self.db.scan_prefix_with_options(
                            prefix.as_bytes(),
                            start_suffix.clone(),
                            &warm_options,
                        ),
                    )
                    .await?;
                for _ in 0..required.saturating_add(1).min(256) {
                    if budget
                        .read_only_io("cypher_ordered_vertex_index_warm_next", async {
                            warm.next().await.map_err(GraphError::from)
                        })
                        .await?
                        .is_none()
                    {
                        break;
                    }
                }
                drop(warm);
            }
            let mut iter = budget
                .read_only_io(
                    "cypher_ordered_vertex_index_seek",
                    self.db.scan_prefix_with_options(
                        prefix.as_bytes(),
                        start_suffix.clone(),
                        &ordered_scan_options,
                    ),
                )
                .await?;
            let mut projected = OrderedRowWindow::new(required, &query.order_by);
            let mut scanned = 0_usize;
            let mut boundary = None::<String>;
            let mut exhausted = false;
            let mut seed_fallback = false;
            while !exhausted {
                let batch_capacity = (if boundary.is_some() {
                    QUERY_METADATA_HYDRATION_CONCURRENCY
                } else {
                    required
                        .saturating_sub(projected.len())
                        .clamp(1, QUERY_METADATA_HYDRATION_CONCURRENCY)
                })
                .min(
                    self.limits
                        .max_query_index_candidates
                        .saturating_sub(scanned)
                        .max(1),
                );
                let mut candidates = Vec::with_capacity(batch_capacity);
                while candidates.len() < batch_capacity {
                    let Some(kv) = budget
                        .read_only_io("cypher_ordered_vertex_index_next", async {
                            iter.next().await.map_err(GraphError::from)
                        })
                        .await?
                    else {
                        exhausted = true;
                        break;
                    };
                    budget.check("cypher_ordered_vertex_property_index")?;
                    let key = String::from_utf8_lossy(&kv.key).into_owned();
                    let (_cell, indexed_property, encoded, vertex_id) =
                        parse_vertex_property_index_key(&key)?;
                    if indexed_property != *property {
                        continue;
                    }
                    if projected.len() >= required && boundary.as_deref() != Some(encoded.as_str())
                    {
                        exhausted = true;
                        break;
                    }
                    scanned = scanned.saturating_add(1);
                    if scanned > self.limits.max_query_index_candidates && seed.is_some() {
                        seed_fallback = true;
                        exhausted = true;
                        break;
                    }
                    self.ensure_query_index_candidates(
                        "cypher_ordered_vertex_property_candidates",
                        scanned,
                    )?;
                    if seed
                        .as_ref()
                        .is_some_and(|vertices| vertices.binary_search(&vertex_id).is_err())
                    {
                        continue;
                    }
                    candidates.push((encoded, vertex_id));
                }
                if seed_fallback {
                    break;
                }
                let vertex_ids = candidates
                    .iter()
                    .map(|(_, vertex_id)| *vertex_id)
                    .collect::<Vec<_>>();
                let metadata = self
                    .vertex_metadata_batch_at(cell_id, &vertex_ids, read_epoch, budget)
                    .await?;
                for ((encoded, vertex_id), (_, metadata)) in candidates.into_iter().zip(metadata) {
                    if projected.len() >= required && boundary.as_deref() != Some(encoded.as_str())
                    {
                        exhausted = true;
                        break;
                    }
                    examined += 1;
                    if metadata
                        .properties
                        .get(property)
                        .is_none_or(|value| encode_vertex_property_value_key(value) != encoded)
                    {
                        rejected += 1;
                        continue;
                    }
                    let Some(mut row) = BindingRow::from_node(node, vertex_id) else {
                        rejected += 1;
                        continue;
                    };
                    row.metadata.insert(binding.to_string(), metadata);
                    if !row_matches_node(&row, node)? || !row_predicate_matches(&row, predicate)? {
                        rejected += 1;
                        continue;
                    }
                    let result_row = project_binding_row(&row, &query.projections)?;
                    let sort_keys =
                        sort_keys_for_row(&row, &result_row, &query.columns, &query.order_by)?;
                    projected.push(result_row, sort_keys);
                    // Consume the complete primary-key tie for secondary ordering,
                    // but retain only the best window, not the whole tie population.
                    self.ensure_query_intermediate_rows(
                        "cypher_ordered_vertex_property_rows",
                        projected.len(),
                    )?;
                    if projected.len() == required {
                        boundary = Some(encoded);
                    }
                }
                // Dense pages should finish directly, without first enumerating a
                // huge type union. Switch once when a bounded sample shows poor
                // selectivity, or when the ordered access reaches its scan bound.
                if !seed_probed
                    && !exhausted
                    && ((rejected >= QUERY_METADATA_HYDRATION_CONCURRENCY
                        && rejected.saturating_mul(2) > examined)
                        || scanned >= self.limits.max_query_index_candidates)
                {
                    seed_probed = true;
                    seed = self
                        .node_equality_predicate_seed_vertices(
                            cell_id,
                            &group,
                            read_epoch,
                            budget,
                            &BindingRow::default(),
                            NodeEqualityProbe::Ordered(self.limits.max_query_index_candidates),
                        )
                        .await?
                        .map(|(_, vertices)| vertices);
                    if seed.as_ref().is_some_and(Vec::is_empty) {
                        return Ok(Some(QueryResultSet::new(query.columns.clone(), Vec::new())));
                    }
                    if seed.is_some() {
                        // Restart against the same pinned snapshot. Discard the
                        // sample's projections so it cannot duplicate output rows.
                        projected = OrderedRowWindow::new(required, &query.order_by);
                        boundary = None;
                        scanned = 0;
                        iter = budget
                            .read_only_io(
                                "cypher_ordered_vertex_index_seek",
                                self.db.scan_prefix_with_options(
                                    prefix.as_bytes(),
                                    start_suffix.clone(),
                                    &ordered_scan_options,
                                ),
                            )
                            .await?;
                    }
                }
            }

            // The ordered scan may encounter too many unrelated index entries.
            // A complete seed remains an exact bounded fallback, never a partial
            // result from the interrupted ordered pass. Release its iterator first.
            drop(iter);
            if seed_fallback {
                if let Some(vertices) = seed {
                    return Ok(Some(hydrate_seed(vertices).await?));
                }
            }

            Ok(Some(self.finish_projected_rows(
                query.columns.clone(),
                projected.into_rows(),
                &query.order_by,
                false,
                window,
                budget,
            )?))
        };
        tokio::pin!(equality, ordered);
        tokio::select! {
            seed = &mut equality => match seed? {
                Some(result) => Ok(Some(result)),
                None => ordered.await,
            },
            result = &mut ordered => match result {
                Ok(result) => Ok(result),
                Err(error) => match equality.await? {
                    Some(result) => Ok(Some(result)),
                    None => Err(error),
                },
            },
        }
    }

    #[cfg(feature = "opencypher")]
    async fn try_execute_graph_kernel_row_query(
        &self,
        cell_id: &str,
        read_epoch: StorageSequence,
        query: &ParsedRowQuery,
        window: QueryWindow,
        budget: &QueryBudget,
    ) -> Result<Option<QueryResultSet>> {
        let Some(request) = graph_kernel_row_query_request(query) else {
            return Ok(None);
        };
        if request.projection == GraphKernelProjection::CountAll && request.edge.dst.id.is_none() {
            let (count, edge_visits) = self
                .reachable_count_in_hop_range_at(
                    cell_id,
                    &request.edge.edge_type,
                    request.src,
                    request.hop_range,
                    read_epoch,
                    budget,
                )
                .await?;
            self.ensure_query_scan_edges("cypher_graph_kernel_count_edge_visits", edge_visits)?;
            let row = QueryRow::new(vec![QueryValue::Count(count)]);
            let rows = vec![row];
            return Ok(Some(QueryResultSet::new(
                query.columns.clone(),
                self.apply_query_row_window(rows, window)?,
            )));
        }
        if request.projection == GraphKernelProjection::NodeId
            && request.edge.dst.id.is_none()
            && !window.is_default()
            && !query.distinct
        {
            let (vertices, edge_visits) = self
                .reachable_vertices_window_in_hop_range_at(ReachableWindowRequest {
                    cell_id,
                    edge_type: &request.edge.edge_type,
                    src: request.src,
                    hop_range: request.hop_range,
                    read_epoch,
                    window,
                    ascending: request.ascending,
                    budget,
                })
                .await?;
            self.ensure_query_scan_edges("cypher_graph_kernel_window_edge_visits", edge_visits)?;
            self.ensure_query_intermediate_rows("cypher_graph_kernel_window_rows", vertices.len())?;
            let rows = graph_kernel_node_id_rows(vertices, budget)?;
            return Ok(Some(QueryResultSet::new(query.columns.clone(), rows)));
        }

        let (mut vertices, edge_visits) = self
            .reachable_vertices_in_hop_range_at(
                cell_id,
                &request.edge.edge_type,
                request.src,
                request.hop_range,
                read_epoch,
                budget,
            )
            .await?;
        self.ensure_query_scan_edges("cypher_graph_kernel_edge_visits", edge_visits)?;
        if let Some(dst) = request.edge.dst.id {
            vertices.retain(|vertex| *vertex == dst);
        }
        graph_kernel_order_vertices(&mut vertices, request.ascending);
        self.ensure_query_intermediate_rows("cypher_graph_kernel_rows", vertices.len())?;

        let projected = match request.projection {
            GraphKernelProjection::NodeId => graph_kernel_node_id_rows(vertices, budget)?
                .into_iter()
                .map(|row| ProjectedQueryRow {
                    row,
                    sort_keys: Vec::new(),
                })
                .collect(),
            GraphKernelProjection::CountAll => {
                let row = QueryRow::new(vec![QueryValue::Count(vertices.len() as u64)]);
                vec![ProjectedQueryRow {
                    row,
                    sort_keys: Vec::new(),
                }]
            }
        };
        Ok(Some(self.finish_projected_rows(
            query.columns.clone(),
            projected,
            &[],
            query.distinct,
            window,
            budget,
        )?))
    }

    #[cfg(feature = "opencypher")]
    async fn try_execute_relationship_rows_query(
        &self,
        cell_id: &str,
        read_epoch: StorageSequence,
        query: &ParsedRowQuery,
        window: QueryWindow,
        budget: &QueryBudget,
    ) -> Result<Option<QueryResultSet>> {
        let Some(edge) = relationship_rows_query_edge(query) else {
            return Ok(None);
        };
        if !relationship_rows_projection_supported(edge, &query.projections)
            || !relationship_rows_order_supported(
                edge,
                &query.projections,
                &query.columns,
                &query.order_by,
            )
        {
            return Ok(None);
        }
        let (Some(src), Some(dst)) = (edge.src.id, edge.dst.id) else {
            return Ok(None);
        };
        let relationships = if let Some((property, value)) = edge.properties.iter().next() {
            self.relationships_for_edge_property_at(RelationshipPropertyLookup {
                cell_id,
                edge_type: &edge.edge_type,
                src,
                dst,
                property,
                value,
                read_epoch,
                budget,
            })
            .await?
        } else {
            self.relationships_for_edge_at(cell_id, &edge.edge_type, src, dst, read_epoch, budget)
                .await?
        };
        let mut projected = Vec::with_capacity(relationships.len());
        for (relationship, metadata) in relationships {
            budget.check("cypher_relationship_rows_fast_path")?;
            if !relationship_metadata_matches(&metadata, edge) {
                continue;
            }
            let row = project_relationship_row(edge, &relationship, &metadata, &query.projections)?;
            let sort_keys =
                sort_keys_for_relationship_row(edge, &relationship, &metadata, &row, query)?;
            push_projected_query_row(&mut projected, row, sort_keys);
            self.ensure_query_intermediate_rows(
                "cypher_relationship_rows_fast_path",
                projected.len(),
            )?;
        }
        Ok(Some(self.finish_projected_rows(
            query.columns.clone(),
            projected,
            &query.order_by,
            query.distinct,
            window,
            budget,
        )?))
    }

    #[cfg(feature = "opencypher")]
    async fn try_execute_ordered_relationship_property_rows_query(
        &self,
        cell_id: &str,
        read_epoch: StorageSequence,
        query: &ParsedRowQuery,
        window: QueryWindow,
        budget: &QueryBudget,
    ) -> Result<Option<QueryResultSet>> {
        let Some(spec) = ordered_relationship_property_index_spec(query, window) else {
            return Ok(None);
        };
        if read_epoch != self.current_epoch(cell_id).await? {
            return Ok(None);
        }
        let OrderedRelationshipPropertyIndexSpec {
            edge,
            property,
            predicate,
            ascending,
            limit,
        } = spec;
        if limit == 0 {
            return Ok(Some(QueryResultSet::new(query.columns.clone(), Vec::new())));
        }
        let skip = usize::try_from(window.skip).map_err(|_| GraphError::AdmissionRejected {
            operation: "query_result_skip",
            actual: window.skip,
            limit: usize::MAX as u64,
        })?;
        let required = skip
            .checked_add(limit)
            .ok_or(GraphError::AdmissionRejected {
                operation: "query_result_window",
                actual: u64::MAX,
                limit: usize::MAX as u64,
            })?;
        let order = if ascending {
            slatedb::IterationOrder::Ascending
        } else {
            slatedb::IterationOrder::Descending
        };
        let edge_prefix =
            keys::edge_property_index_property_prefix(cell_id, &edge.edge_type, property);
        let relationship_prefix =
            keys::relationship_property_index_property_prefix(cell_id, &edge.edge_type, property);
        let mut edge_iters = Vec::with_capacity(ORDERED_PROPERTY_INDEX_TYPE_PREFIXES.len());
        let mut relationship_iters = Vec::with_capacity(ORDERED_PROPERTY_INDEX_TYPE_PREFIXES.len());
        for value_type in ORDERED_PROPERTY_INDEX_TYPE_PREFIXES {
            edge_iters.push(
                self.db
                    .scan_prefix_with_options(
                        format!("{edge_prefix}{value_type}").as_bytes(),
                        None,
                        &remote_scan_options().with_order(order),
                    )
                    .await?,
            );
            relationship_iters.push(
                self.db
                    .scan_prefix_with_options(
                        format!("{relationship_prefix}{value_type}").as_bytes(),
                        None,
                        &remote_scan_options().with_order(order),
                    )
                    .await?,
            );
        }
        let mut next_edges = Vec::with_capacity(edge_iters.len());
        for iter in &mut edge_iters {
            next_edges.push(match iter.next().await? {
                Some(kv) => Some(ordered_edge_property_candidate(&kv.key)?),
                None => None,
            });
        }
        let mut next_relationships = Vec::with_capacity(relationship_iters.len());
        for iter in &mut relationship_iters {
            next_relationships.push(match iter.next().await? {
                Some(kv) => Some(ordered_relationship_property_candidate(&kv.key)?),
                None => None,
            });
        }

        let mut metadata_cache = BTreeMap::new();
        let mut relationship_metadata_cache = BTreeMap::new();
        let mut state = EdgeRowMatchState {
            cell_id,
            read_epoch,
            edge,
            rows: Vec::with_capacity(required),
            pending: Vec::with_capacity(QUERY_METADATA_HYDRATION_CONCURRENCY),
            metadata_cache: &mut metadata_cache,
            edge_metadata_cache: &mut relationship_metadata_cache,
            budget,
        };
        let mut scanned = 0_usize;
        while let Some(source) = ordered_relationship_property_candidate_source(
            &next_edges,
            &next_relationships,
            ascending,
        )? {
            budget.check("cypher_ordered_relationship_property_index")?;
            let candidate = match source {
                OrderedRelationshipPropertyCandidateSource::Edge(index) => {
                    let candidate = next_edges[index]
                        .take()
                        .expect("edge candidate source is populated");
                    next_edges[index] = match edge_iters[index].next().await? {
                        Some(kv) => Some(ordered_edge_property_candidate(&kv.key)?),
                        None => None,
                    };
                    candidate
                }
                OrderedRelationshipPropertyCandidateSource::Relationship(index) => {
                    let candidate = next_relationships[index]
                        .take()
                        .expect("relationship candidate source is populated");
                    next_relationships[index] = match relationship_iters[index].next().await? {
                        Some(kv) => Some(ordered_relationship_property_candidate(&kv.key)?),
                        None => None,
                    };
                    candidate
                }
            };
            scanned = scanned.saturating_add(1);
            self.ensure_query_index_candidates(
                "cypher_ordered_relationship_property_candidates",
                scanned,
            )?;

            let relationship = BoundRelationship {
                edge_type: edge.edge_type.clone(),
                src: candidate.src,
                dst: candidate.dst,
                relationship_id: candidate.relationship_id,
            };
            let metadata = if let Some(relationship_id) = candidate.relationship_id {
                let key = keys::relationship(
                    cell_id,
                    &edge.edge_type,
                    candidate.src,
                    candidate.dst,
                    relationship_id,
                );
                let Some(value) = self.read_remote(&key).await? else {
                    continue;
                };
                decode_relationship_record(&key, &value)?.metadata
            } else {
                self.edge_metadata_at(
                    cell_id,
                    &edge.edge_type,
                    candidate.src,
                    candidate.dst,
                    read_epoch,
                    budget,
                )
                .await?
            };
            if metadata
                .properties
                .get(property)
                .is_none_or(|value| encode_vertex_property_value_key(value) != candidate.encoded)
            {
                continue;
            }
            if let Some(row) = BindingRow::from_relationship(edge, relationship, metadata) {
                state.pending.push(row);
            }
            if state.pending.len() >= QUERY_METADATA_HYDRATION_CONCURRENCY
                || state.rows.len().saturating_add(state.pending.len()) >= required
            {
                self.flush_pending_edge_rows(&mut state).await?;
                retain_binding_rows_matching_predicate(&mut state.rows, predicate, budget)?;
                if state.rows.len() >= required {
                    break;
                }
            }
        }
        let mut bindings = self.finish_edge_rows(state).await?;
        retain_binding_rows_matching_predicate(&mut bindings, predicate, budget)?;
        let mut projected = Vec::with_capacity(bindings.len());
        for binding in bindings {
            budget.check("cypher_ordered_relationship_property_project")?;
            let row = project_binding_row(&binding, &query.projections)?;
            let sort_keys = sort_keys_for_row(&binding, &row, &query.columns, &query.order_by)?;
            push_projected_query_row(&mut projected, row, sort_keys);
        }
        Ok(Some(self.finish_projected_rows(
            query.columns.clone(),
            projected,
            &query.order_by,
            false,
            window,
            budget,
        )?))
    }

    #[cfg(feature = "opencypher")]
    async fn try_execute_source_relationship_id_rows_query(
        &self,
        cell_id: &str,
        read_epoch: StorageSequence,
        query: &ParsedRowQuery,
        window: QueryWindow,
        budget: &QueryBudget,
    ) -> Result<Option<QueryResultSet>> {
        let Some(edge) = source_relationship_id_rows_query_edge(query) else {
            return Ok(None);
        };
        let Some(src) = edge.src.id else {
            return Ok(None);
        };

        let mut bindings = self
            .source_relationship_id_bindings_at(cell_id, edge, src, read_epoch, budget)
            .await?;
        self.ensure_query_intermediate_rows("cypher_source_relationship_id_rows", bindings.len())?;
        let mut projected = Vec::with_capacity(bindings.len());
        for binding in bindings.drain(..) {
            budget.check("cypher_source_relationship_id_project")?;
            let row = project_binding_row(&binding, &query.projections)?;
            let sort_keys = sort_keys_for_row(&binding, &row, &query.columns, &query.order_by)?;
            push_projected_query_row(&mut projected, row, sort_keys);
        }
        Ok(Some(self.finish_projected_rows(
            query.columns.clone(),
            projected,
            &query.order_by,
            query.distinct,
            window,
            budget,
        )?))
    }

    #[cfg(feature = "opencypher")]
    async fn try_execute_relationship_count_query(
        &self,
        cell_id: &str,
        read_epoch: StorageSequence,
        query: &ParsedRowQuery,
        window: QueryWindow,
        budget: &QueryBudget,
    ) -> Result<Option<QueryResultSet>> {
        let Some(edge) = relationship_count_query_edge(query) else {
            return Ok(None);
        };
        let (Some(src), Some(dst)) = (edge.src.id, edge.dst.id) else {
            return Ok(None);
        };
        let count = self
            .relationship_count_for_edge_at(cell_id, &edge.edge_type, src, dst, read_epoch, budget)
            .await?;
        let row = QueryRow::new(vec![QueryValue::Count(count)]);
        let rows = vec![row];
        Ok(Some(QueryResultSet::new(
            query.columns.clone(),
            self.apply_query_row_window(rows, window)?,
        )))
    }

    #[cfg(feature = "opencypher")]
    async fn try_execute_source_relationship_id_rows_page(
        &self,
        context: &QueryContext,
        query: &ParsedRowQuery,
        cursor_offset: u64,
        page_size: usize,
    ) -> Result<Option<QueryResultPage>> {
        let Some(edge) = source_relationship_id_rows_query_edge(query) else {
            return Ok(None);
        };
        let Some(ascending) = source_relationship_id_page_order(edge, query) else {
            return Ok(None);
        };
        if query.distinct {
            return Ok(None);
        }
        let Some(src) = edge.src.id else {
            return Ok(None);
        };
        let page_context = self.query_page_context(context.clone(), cursor_offset, page_size)?;
        let window = page_context.result_window;
        if window.limit == Some(0) {
            return Ok(Some(QueryResultPage::new(
                query.columns.clone(),
                Vec::new(),
                None,
            )));
        }
        let read_epoch = self.query_read_epoch(context).await?;
        let budget = QueryBudget::new(
            context.max_runtime_ms.or(self.limits.max_query_runtime_ms),
            context.cancellation_token.clone(),
        );
        budget.check("cypher_source_relationship_page")?;

        let cached = self
            .source_relationship_dsts_at(&context.cell_id, edge, src, read_epoch, &budget)
            .await?;
        let mut dsts = cached.as_ref().clone();
        dsts.sort_unstable();
        if !ascending {
            dsts.reverse();
        }
        let skip = usize::try_from(window.skip).map_err(|_| GraphError::AdmissionRejected {
            operation: "query_result_skip",
            actual: window.skip,
            limit: usize::MAX as u64,
        })?;
        let probe_limit = window.limit.unwrap_or(page_size.saturating_add(1));
        let page_dsts = dsts
            .into_iter()
            .skip(skip)
            .take(probe_limit)
            .collect::<Vec<_>>();
        let has_next = page_dsts.len() > page_size;
        let page_dsts = &page_dsts[..page_dsts.len().min(page_size)];
        let bindings =
            source_relationship_id_bindings_from_dsts(edge, src, page_dsts, self, &budget)?;
        let mut rows = Vec::with_capacity(bindings.len());
        for binding in bindings {
            budget.check("cypher_source_relationship_page_project")?;
            let row = project_binding_row(&binding, &query.projections)?;
            rows.push(row);
        }
        Ok(Some(QueryResultPage::new(
            query.columns.clone(),
            rows,
            query_next_cursor(cursor_offset, page_size, has_next)?,
        )))
    }

    #[cfg(feature = "opencypher")]
    async fn execute_parsed_opencypher_mutation(
        &self,
        context: QueryContext,
        query: ParsedMutationQuery,
    ) -> Result<QueryMutationResult> {
        validate_component("cell_id", &context.cell_id)?;
        if context.read_epoch.is_some() {
            return Err(GraphError::UnsupportedQuery {
                reason: QueryFailureReason::InvalidRequest,
                dialect: "OpenCypher",
                feature: "mutation queries cannot run at a historical read epoch".to_string(),
            });
        }
        let budget = QueryBudget::new(
            context.max_runtime_ms.or(self.limits.max_query_runtime_ms),
            context.cancellation_token.clone(),
        );
        budget.check("cypher_mutation")?;

        if query.patterns.is_empty() {
            return self
                .execute_patternless_mutation(&context, &query.actions, &budget)
                .await;
        }

        let read_epoch = self.current_epoch(&context.cell_id).await?;
        let mut bindings = self
            .match_row_patterns(&context.cell_id, &query.patterns, read_epoch, &budget)
            .await?;
        if let Some(predicate) = &query.predicate {
            let mut filtered = Vec::with_capacity(bindings.len());
            for row in bindings {
                budget.check("cypher_mutation_where")?;
                if row_predicate_matches(&row, predicate)? {
                    filtered.push(row);
                }
            }
            bindings = filtered;
        }

        // `WITH ... LIMIT n` caps the rows that reach the write. It applies
        // after the predicate, which is where Cypher puts it: the barrier sees
        // the filtered rows, not the raw pattern matches.
        //
        // This is a ceiling on rows mutated, not on rows examined. The shapes
        // that use it anchor on a property seek, so matching already costs the
        // anchor's degree rather than the graph; an unanchored pattern would
        // still materialize every match before the truncation. Pushing the
        // bound into `match_row_patterns` is the follow-up.
        if let Some(limit) = query.row_limit {
            budget.check("cypher_mutation_limit")?;
            bindings.truncate(limit);
        }

        let returned_rows = query
            .returning
            .as_ref()
            .map(|returning| mutation_return_rows(returning, &bindings));

        let mut result = QueryMutationResult {
            matched_rows: bindings.len() as u64,
            returned_rows,
            ..QueryMutationResult::default()
        };
        let mut pending_metadata = BTreeMap::<VertexId, VertexMetadata>::new();
        let mut original_metadata = BTreeMap::<VertexId, VertexMetadata>::new();
        let mut pending_edge_metadata = BTreeMap::<BoundRelationship, EdgeMetadata>::new();
        let mut original_edge_metadata = BTreeMap::<BoundRelationship, EdgeMetadata>::new();

        for action in &query.actions {
            budget.check("cypher_mutation_action")?;
            match action {
                RowMutationAction::DeleteBinding { binding, detach } => {
                    let mut relationships = BTreeSet::new();
                    let mut vertices = BTreeSet::new();
                    for row in &bindings {
                        if let Some(relationship) = row.relationships.get(binding) {
                            relationships.insert(relationship.clone());
                        } else {
                            vertices.insert(row.get(binding)?);
                        }
                    }
                    let deleted = self
                        .delete_bound_relationships_batch(
                            &context,
                            relationships,
                            Vec::new(),
                            &budget,
                        )
                        .await?;
                    result.deleted_edges = result.deleted_edges.saturating_add(deleted.deleted);
                    result.noops = result.noops.saturating_add(deleted.already_deleted);
                    result.topology_sequence =
                        result.topology_sequence.max(deleted.topology_sequence);
                    for vertex_id in vertices {
                        budget.check("cypher_delete_vertex")?;
                        let delete = if *detach {
                            self.detach_delete_vertex(
                                &context.cell_id,
                                vertex_id,
                                &format!(
                                    "{}.detach-delete-node.{vertex_id}",
                                    context.idempotency_key
                                ),
                            )
                            .await?
                        } else {
                            self.delete_vertex(
                                &context.cell_id,
                                vertex_id,
                                &format!("{}.delete-node.{vertex_id}", context.idempotency_key),
                            )
                            .await?
                        };
                        result.deleted_edges = result
                            .deleted_edges
                            .saturating_add(delete.incident_edges_deleted);
                        if delete.incident_edges_deleted > 0 {
                            result.topology_sequence =
                                result.topology_sequence.max(Some(delete.epoch));
                        }
                        if delete.vertex_deleted {
                            result.updated_vertices = result.updated_vertices.saturating_add(1);
                        } else if delete.incident_edges_deleted == 0
                            && delete.relationships_deleted == 0
                        {
                            result.noops = result.noops.saturating_add(1);
                        }
                    }
                }
                RowMutationAction::DeleteRelationship { binding, detach } => {
                    let _ = detach;
                    let mut relationships = BTreeSet::new();
                    for row in &bindings {
                        let Some(relationship) = row.relationships.get(binding) else {
                            return Err(GraphError::UnsupportedQuery {
                                reason: QueryFailureReason::Mutation,
                                dialect: "OpenCypher",
                                feature: format!(
                                    "DELETE references unbound relationship {binding}"
                                ),
                            });
                        };
                        relationships.insert(relationship.clone());
                    }
                    let deleted = self
                        .delete_bound_relationships_batch(
                            &context,
                            relationships,
                            Vec::new(),
                            &budget,
                        )
                        .await?;
                    result.deleted_edges = result.deleted_edges.saturating_add(deleted.deleted);
                    result.noops = result.noops.saturating_add(deleted.already_deleted);
                    result.topology_sequence =
                        result.topology_sequence.max(deleted.topology_sequence);
                }
                RowMutationAction::SetProperty { .. }
                | RowMutationAction::SetLabels { .. }
                | RowMutationAction::RemoveProperty { .. }
                | RowMutationAction::RemoveLabels { .. } => {
                    let mut state = VertexMutationApplyState {
                        cell_id: &context.cell_id,
                        read_epoch,
                        pending_metadata: &mut pending_metadata,
                        original_metadata: &mut original_metadata,
                        pending_edge_metadata: &mut pending_edge_metadata,
                        original_edge_metadata: &mut original_edge_metadata,
                        budget: &budget,
                    };
                    for row in &bindings {
                        self.apply_vertex_mutation_action(row, action, &mut state)
                            .await?;
                    }
                }
                RowMutationAction::CreateEdge { .. } | RowMutationAction::MergeEdge { .. } => {
                    return Err(GraphError::UnsupportedQuery {
                        reason: QueryFailureReason::Mutation,
                        dialect: "OpenCypher",
                        feature: "CREATE and MERGE are executable only as standalone clauses"
                            .to_string(),
                    });
                }
            }
        }

        for (vertex_id, metadata) in pending_metadata {
            budget.check("cypher_set_vertex_metadata")?;
            if original_metadata.get(&vertex_id) == Some(&metadata) {
                result.noops = result.noops.saturating_add(1);
                continue;
            }
            self.set_vertex_metadata(&context.cell_id, vertex_id, metadata)
                .await?;
            result.updated_vertices = result.updated_vertices.saturating_add(1);
        }
        for (relationship, metadata) in pending_edge_metadata {
            budget.check("cypher_set_relationship_metadata")?;
            if original_edge_metadata.get(&relationship) == Some(&metadata) {
                result.noops = result.noops.saturating_add(1);
                continue;
            }
            let changed = if let Some(relationship_id) = relationship.relationship_id {
                self.set_relationship_metadata(
                    &context.cell_id,
                    &relationship.edge_type,
                    relationship.src,
                    relationship.dst,
                    relationship_id,
                    metadata,
                )
                .await?
            } else {
                self.set_edge_metadata(
                    &context.cell_id,
                    &relationship.edge_type,
                    relationship.src,
                    relationship.dst,
                    metadata,
                )
                .await?
            };
            if changed {
                result.updated_relationships = result.updated_relationships.saturating_add(1);
            } else {
                result.noops = result.noops.saturating_add(1);
            }
        }

        Ok(result)
    }

    #[cfg(feature = "opencypher")]
    async fn delete_bound_relationships_batch(
        &self,
        context: &QueryContext,
        relationships: BTreeSet<BoundRelationship>,
        structural_delete_guards: Vec<EdgeMutation>,
        budget: &QueryBudget,
    ) -> Result<RelationshipDeleteBatchResult> {
        let mut structural = Vec::new();
        let mut identified = Vec::new();
        for relationship in relationships {
            budget.check("cypher_delete_relationship_collect")?;
            let mutation = bound_relationship_delete_mutation(context, &relationship);
            match relationship.relationship_id {
                Some(relationship_id) => identified.push((mutation, relationship_id)),
                None => structural.push(mutation),
            }
        }

        let mut deleted = 0_u64;
        let mut noops = 0_u64;
        let mut topology_sequence = None;
        if !structural.is_empty() {
            budget.check("cypher_delete_edge_batch")?;
            let batch = self
                .delete_edge_mutations_batch(&context.cell_id, structural)
                .await?;
            deleted = deleted.saturating_add(batch.deleted);
            noops = noops.saturating_add(batch.already_deleted);
            if batch.deleted > 0 {
                topology_sequence = Some(batch.end_epoch);
            }
        }
        if !identified.is_empty() {
            budget.check("cypher_delete_relationship_batch")?;
            let batch = self
                .delete_relationship_mutations_batch_with_guards(
                    &context.cell_id,
                    identified,
                    structural_delete_guards,
                )
                .await?;
            deleted = deleted.saturating_add(batch.deleted);
            noops = noops.saturating_add(batch.already_deleted);
            topology_sequence = topology_sequence.max(batch.topology_sequence);
        }
        Ok(RelationshipDeleteBatchResult {
            deleted,
            already_deleted: noops,
            topology_sequence,
        })
    }

    #[cfg(feature = "opencypher")]
    pub(crate) async fn delete_relationships_by_property_values_batch(
        &self,
        context: &QueryContext,
        edge_type: &str,
        property: &str,
        values: Vec<VertexPropertyValue>,
    ) -> Result<RelationshipDeleteBatchResult> {
        validate_component("cell_id", &context.cell_id)?;
        validate_component("edge_type", edge_type)?;
        validate_component("property", property)?;
        let budget = QueryBudget::new(
            context.max_runtime_ms.or(self.limits.max_query_runtime_ms),
            context.cancellation_token.clone(),
        );
        let read_epoch = self.current_epoch(&context.cell_id).await?;
        let scans = values
            .into_iter()
            .flat_map(|value| {
                equivalent_property_index_keys(&value)
                    .into_iter()
                    .map(move |encoded| (value.clone(), encoded))
            })
            .collect::<Vec<_>>();
        let mut scan_results = stream::iter(scans)
            .map(|(value, encoded)| {
                let budget = budget.clone();
                async move {
                    budget.check("cypher_delete_relationship_property_batch")?;
                    let (concrete, structural) = tokio::try_join!(
                        self.scan_relationship_property_index_current(
                            &context.cell_id,
                            edge_type,
                            property,
                            &encoded,
                            &budget,
                        ),
                        self.scan_edge_property_index_current(
                            &context.cell_id,
                            edge_type,
                            property,
                            &encoded,
                            &budget,
                        ),
                    )?;
                    Ok::<_, GraphError>((value, concrete, structural))
                }
            })
            .buffer_unordered(QUERY_METADATA_HYDRATION_CONCURRENCY);

        let mut concrete_candidates =
            BTreeMap::<BoundRelationship, Vec<VertexPropertyValue>>::new();
        let mut structural_candidates =
            BTreeMap::<(VertexId, VertexId), Vec<VertexPropertyValue>>::new();
        while let Some((value, concrete, structural)) = scan_results.try_next().await? {
            for (src, dst, relationship_id) in concrete {
                concrete_candidates
                    .entry(BoundRelationship {
                        edge_type: edge_type.to_string(),
                        src,
                        dst,
                        relationship_id: Some(relationship_id),
                    })
                    .or_default()
                    .push(value.clone());
            }
            for edge in structural {
                structural_candidates
                    .entry(edge)
                    .or_default()
                    .push(value.clone());
            }
            self.ensure_query_index_candidates(
                "cypher_delete_relationship_property_candidates",
                concrete_candidates
                    .len()
                    .saturating_add(structural_candidates.len()),
            )?;
        }

        let concrete = stream::iter(concrete_candidates)
            .map(|(relationship, values)| {
                let budget = budget.clone();
                async move {
                    let metadata = self
                        .relationship_metadata_at(
                            &context.cell_id,
                            &relationship,
                            read_epoch,
                            &budget,
                        )
                        .await?;
                    Ok::<_, GraphError>(
                        metadata
                            .properties
                            .get(property)
                            .is_some_and(|existing| {
                                values
                                    .iter()
                                    .any(|value| vertex_property_values_equal(existing, value))
                            })
                            .then_some(relationship),
                    )
                }
            })
            .buffered(QUERY_METADATA_HYDRATION_CONCURRENCY)
            .try_collect::<Vec<_>>()
            .await?;
        let structural = stream::iter(structural_candidates)
            .map(|((src, dst), values)| {
                let budget = budget.clone();
                async move {
                    if self
                        .relationship_count_for_edge_at(
                            &context.cell_id,
                            edge_type,
                            src,
                            dst,
                            read_epoch,
                            &budget,
                        )
                        .await?
                        > 0
                    {
                        return Ok::<_, GraphError>(None);
                    }
                    let metadata = self
                        .edge_metadata_at(
                            &context.cell_id,
                            edge_type,
                            src,
                            dst,
                            read_epoch,
                            &budget,
                        )
                        .await?;
                    Ok(metadata
                        .properties
                        .get(property)
                        .is_some_and(|existing| {
                            values
                                .iter()
                                .any(|value| vertex_property_values_equal(existing, value))
                        })
                        .then_some(BoundRelationship {
                            edge_type: edge_type.to_string(),
                            src,
                            dst,
                            relationship_id: None,
                        }))
                }
            })
            .buffered(QUERY_METADATA_HYDRATION_CONCURRENCY)
            .try_collect::<Vec<_>>()
            .await?;
        let relationships = concrete
            .into_iter()
            .chain(structural)
            .flatten()
            .collect::<BTreeSet<_>>();
        let structural_delete_guards = relationships
            .iter()
            .filter(|relationship| relationship.relationship_id.is_some())
            .map(|relationship| {
                let mut structural = relationship.clone();
                structural.relationship_id = None;
                (
                    (structural.edge_type.clone(), structural.src, structural.dst),
                    bound_relationship_delete_mutation(context, &structural),
                )
            })
            .collect::<BTreeMap<_, _>>();
        self.delete_bound_relationships_batch(
            context,
            relationships,
            structural_delete_guards.into_values().collect(),
            &budget,
        )
        .await
    }

    #[cfg(feature = "opencypher")]
    async fn execute_patternless_mutation(
        &self,
        context: &QueryContext,
        actions: &[RowMutationAction],
        budget: &QueryBudget,
    ) -> Result<QueryMutationResult> {
        if actions
            .iter()
            .any(|action| matches!(action, RowMutationAction::CreateEdge { .. }))
        {
            if !actions
                .iter()
                .all(|action| matches!(action, RowMutationAction::CreateEdge { .. }))
            {
                return Err(GraphError::UnsupportedQuery {
                    reason: QueryFailureReason::Mutation,
                    dialect: "OpenCypher",
                    feature: "CREATE batches cannot mix mutation action types".to_string(),
                });
            }
            if actions.len() > 1
                && actions.iter().any(|action| match action {
                    RowMutationAction::CreateEdge {
                        src_metadata,
                        dst_metadata,
                        edge_metadata,
                        ..
                    } => {
                        !src_metadata.labels.is_empty()
                            || !src_metadata.properties.is_empty()
                            || !dst_metadata.labels.is_empty()
                            || !dst_metadata.properties.is_empty()
                            || !edge_metadata.properties.is_empty()
                    }
                    _ => false,
                })
            {
                return Err(GraphError::UnsupportedQuery {
                    reason: QueryFailureReason::Mutation,
                    dialect: "OpenCypher",
                    feature: "multi-pattern CREATE with labels or properties requires the metadata batch writer"
                        .to_string(),
                });
            }

            if actions.len() == 1 {
                let RowMutationAction::CreateEdge {
                    edge_type,
                    src,
                    dst,
                    src_metadata,
                    dst_metadata,
                    edge_metadata,
                } = &actions[0]
                else {
                    unreachable!("CREATE batch was validated above")
                };
                let mutation = EdgeMutation {
                    cell_id: context.cell_id.clone(),
                    edge_type: edge_type.clone(),
                    src: *src,
                    dst: *dst,
                    idempotency_key: format!(
                        "{}.create.{}.{}.{}.00000000000000000000",
                        context.idempotency_key, edge_type, src, dst
                    ),
                };
                let created = if src_metadata.labels.is_empty()
                    && src_metadata.properties.is_empty()
                    && dst_metadata.labels.is_empty()
                    && dst_metadata.properties.is_empty()
                    && edge_metadata.properties.is_empty()
                {
                    self.create_relationship(mutation, EdgeMetadata::default())
                        .await?
                } else if edge_metadata.properties.is_empty() {
                    self.create_relationship_with_vertex_metadata(
                        mutation,
                        src_metadata.clone(),
                        dst_metadata.clone(),
                    )
                    .await?
                } else {
                    self.create_relationship_with_full_metadata(
                        mutation,
                        src_metadata.clone(),
                        dst_metadata.clone(),
                        edge_metadata.clone(),
                    )
                    .await?
                };
                return Ok(QueryMutationResult {
                    created_edges: u64::from(created.structural_edge_inserted),
                    created_relationships: u64::from(!created.already_created),
                    noops: u64::from(created.already_created),
                    topology_sequence: created.structural_edge_inserted.then_some(created.epoch),
                    ..QueryMutationResult::default()
                });
            }

            let mutations = actions
                .iter()
                .enumerate()
                .map(|(index, action)| {
                    let RowMutationAction::CreateEdge {
                        edge_type,
                        src,
                        dst,
                        ..
                    } = action
                    else {
                        unreachable!("CREATE batch was validated above")
                    };
                    EdgeMutation {
                        cell_id: context.cell_id.clone(),
                        edge_type: edge_type.clone(),
                        src: *src,
                        dst: *dst,
                        idempotency_key: format!(
                            "{}.create.{}.{}.{}.{index:020}",
                            context.idempotency_key, edge_type, src, dst
                        ),
                    }
                })
                .collect::<Vec<_>>();
            let batch = self
                .write_edge_mutations_batch(&context.cell_id, mutations)
                .await?;
            return Ok(QueryMutationResult {
                created_edges: batch.inserted,
                noops: batch.already_existed,
                topology_sequence: (batch.inserted > 0).then_some(batch.end_epoch),
                ..QueryMutationResult::default()
            });
        }

        let mut result = QueryMutationResult::default();
        for action in actions {
            budget.check("cypher_patternless_mutation")?;
            let RowMutationAction::MergeEdge {
                edge_type,
                src,
                dst,
                src_metadata,
                dst_metadata,
                edge_metadata,
            } = action
            else {
                return Err(GraphError::UnsupportedQuery {
                    reason: QueryFailureReason::Mutation,
                    dialect: "OpenCypher",
                    feature: "patternless mutation supports only standalone MERGE".to_string(),
                });
            };
            let mutation = EdgeMutation {
                cell_id: context.cell_id.clone(),
                edge_type: edge_type.clone(),
                src: *src,
                dst: *dst,
                idempotency_key: format!(
                    "{}.merge.{}.{}.{}",
                    context.idempotency_key, edge_type, src, dst
                ),
            };
            let commit = if src_metadata.labels.is_empty()
                && src_metadata.properties.is_empty()
                && dst_metadata.labels.is_empty()
                && dst_metadata.properties.is_empty()
                && edge_metadata.properties.is_empty()
            {
                self.write_edge(mutation).await?
            } else if edge_metadata.properties.is_empty() {
                self.write_edge_with_vertex_metadata(
                    mutation,
                    src_metadata.clone(),
                    dst_metadata.clone(),
                )
                .await?
            } else {
                self.write_edge_with_full_metadata(
                    mutation,
                    src_metadata.clone(),
                    dst_metadata.clone(),
                    edge_metadata.clone(),
                )
                .await?
            };
            if commit.already_existed {
                result.noops = result.noops.saturating_add(1);
            } else {
                result.created_edges = result.created_edges.saturating_add(1);
                result.topology_sequence = result.topology_sequence.max(Some(commit.epoch));
            }
        }
        Ok(result)
    }

    #[cfg(feature = "opencypher")]
    async fn apply_vertex_mutation_action(
        &self,
        row: &BindingRow,
        action: &RowMutationAction,
        state: &mut VertexMutationApplyState<'_>,
    ) -> Result<()> {
        let binding = match action {
            RowMutationAction::SetProperty { binding, .. }
            | RowMutationAction::SetLabels { binding, .. }
            | RowMutationAction::RemoveProperty { binding, .. }
            | RowMutationAction::RemoveLabels { binding, .. } => binding,
            _ => {
                return Err(GraphError::UnsupportedQuery {
                    reason: QueryFailureReason::Other,
                    dialect: "OpenCypher",
                    feature: "non-vertex mutation action cannot update metadata".to_string(),
                });
            }
        };
        if let Some(relationship) = row.relationships.get(binding) {
            match action {
                RowMutationAction::SetProperty {
                    property, value, ..
                } => {
                    if state.pending_edge_metadata.get(relationship).is_none() {
                        state.budget.check("cypher_load_relationship_metadata")?;
                        let metadata = match row.relationship_metadata.get(relationship) {
                            Some(metadata) => metadata.clone(),
                            None => {
                                self.edge_metadata_at(
                                    state.cell_id,
                                    &relationship.edge_type,
                                    relationship.src,
                                    relationship.dst,
                                    state.read_epoch,
                                    state.budget,
                                )
                                .await?
                            }
                        };
                        state
                            .original_edge_metadata
                            .insert(relationship.clone(), metadata.clone());
                        state
                            .pending_edge_metadata
                            .insert(relationship.clone(), metadata);
                    }
                    let metadata = state
                        .pending_edge_metadata
                        .get_mut(relationship)
                        .ok_or_else(|| GraphError::UnsupportedQuery {
                            reason: QueryFailureReason::Other,
                            dialect: "OpenCypher",
                            feature: format!("metadata for relationship {binding} was not loaded"),
                        })?;
                    metadata.properties.insert(property.clone(), value.clone());
                    return Ok(());
                }
                RowMutationAction::RemoveProperty { property, .. } => {
                    if state.pending_edge_metadata.get(relationship).is_none() {
                        state.budget.check("cypher_load_relationship_metadata")?;
                        let metadata = match row.relationship_metadata.get(relationship) {
                            Some(metadata) => metadata.clone(),
                            None => {
                                self.edge_metadata_at(
                                    state.cell_id,
                                    &relationship.edge_type,
                                    relationship.src,
                                    relationship.dst,
                                    state.read_epoch,
                                    state.budget,
                                )
                                .await?
                            }
                        };
                        state
                            .original_edge_metadata
                            .insert(relationship.clone(), metadata.clone());
                        state
                            .pending_edge_metadata
                            .insert(relationship.clone(), metadata);
                    }
                    let metadata = state
                        .pending_edge_metadata
                        .get_mut(relationship)
                        .ok_or_else(|| GraphError::UnsupportedQuery {
                            reason: QueryFailureReason::Other,
                            dialect: "OpenCypher",
                            feature: format!("metadata for relationship {binding} was not loaded"),
                        })?;
                    metadata.properties.remove(property);
                    return Ok(());
                }
                RowMutationAction::SetLabels { .. } | RowMutationAction::RemoveLabels { .. } => {
                    return Err(GraphError::UnsupportedQuery {
                        reason: QueryFailureReason::Mutation,
                        dialect: "OpenCypher",
                        feature: "relationship labels are not executable in Query engine"
                            .to_string(),
                    });
                }
                _ => {}
            }
        }
        let vertex_id = row.get(binding)?;
        if state.pending_metadata.get(&vertex_id).is_none() {
            state.budget.check("cypher_load_mutation_metadata")?;
            let metadata = match row.metadata.get(binding) {
                Some(metadata) => metadata.clone(),
                None => {
                    self.vertex_metadata_at(
                        state.cell_id,
                        vertex_id,
                        state.read_epoch,
                        state.budget,
                    )
                    .await?
                }
            };
            state.original_metadata.insert(vertex_id, metadata.clone());
            state.pending_metadata.insert(vertex_id, metadata);
        }
        let metadata = state.pending_metadata.get_mut(&vertex_id).ok_or_else(|| {
            GraphError::UnsupportedQuery {
                reason: QueryFailureReason::Other,
                dialect: "OpenCypher",
                feature: format!("metadata for {binding} was not loaded"),
            }
        })?;
        match action {
            RowMutationAction::SetProperty {
                property, value, ..
            } => {
                metadata.properties.insert(property.clone(), value.clone());
            }
            RowMutationAction::SetLabels { labels, .. } => {
                metadata.labels.extend(labels.iter().cloned());
            }
            RowMutationAction::RemoveProperty { property, .. } => {
                metadata.properties.remove(property);
            }
            RowMutationAction::RemoveLabels { labels, .. } => {
                for label in labels {
                    metadata.labels.remove(label);
                }
            }
            _ => {}
        }
        Ok(())
    }

    pub async fn execute_query_plan(&self, plan: QueryPlan) -> Result<QueryOutput> {
        self.validate_executable_query_plan(&plan).await?;
        let budget = QueryBudget::new(
            plan.max_runtime_ms.or(self.limits.max_query_runtime_ms),
            None,
        );
        budget.check("query_plan")?;
        match plan.physical {
            PhysicalQueryPlan::WriteEdge {
                edge_type,
                src,
                dst,
            } => {
                let result = self
                    .create_relationship(
                        EdgeMutation {
                            cell_id: plan.cell_id,
                            edge_type,
                            src,
                            dst,
                            idempotency_key: plan.idempotency_key,
                        },
                        EdgeMetadata::default(),
                    )
                    .await?;
                Ok(QueryOutput::Write(CommitResult {
                    epoch: result.epoch,
                    already_existed: result.already_created,
                }))
            }
            PhysicalQueryPlan::WriteEdgeWithMetadata {
                edge_type,
                src,
                dst,
                src_metadata,
                dst_metadata,
            } => {
                let result = self
                    .create_relationship_with_vertex_metadata(
                        EdgeMutation {
                            cell_id: plan.cell_id,
                            edge_type,
                            src,
                            dst,
                            idempotency_key: plan.idempotency_key,
                        },
                        src_metadata,
                        dst_metadata,
                    )
                    .await?;
                Ok(QueryOutput::Write(CommitResult {
                    epoch: result.epoch,
                    already_existed: result.already_created,
                }))
            }
            PhysicalQueryPlan::WriteEdgeWithFullMetadata {
                edge_type,
                src,
                dst,
                src_metadata,
                dst_metadata,
                edge_metadata,
            } => {
                let result = self
                    .create_relationship_with_full_metadata(
                        EdgeMutation {
                            cell_id: plan.cell_id,
                            edge_type,
                            src,
                            dst,
                            idempotency_key: plan.idempotency_key,
                        },
                        src_metadata,
                        dst_metadata,
                        edge_metadata,
                    )
                    .await?;
                Ok(QueryOutput::Write(CommitResult {
                    epoch: result.epoch,
                    already_existed: result.already_created,
                }))
            }
            PhysicalQueryPlan::OutDegreeCounter { edge_type, src } => {
                let count = if let Some(read_epoch) = plan.read_epoch {
                    self.out_degree_at(&plan.cell_id, &edge_type, src, read_epoch)
                        .await?
                } else {
                    self.out_degree(&plan.cell_id, &edge_type, src).await?
                };
                Ok(QueryOutput::Count(count))
            }
            PhysicalQueryPlan::OutNeighbors { edge_type, src } => {
                let read_epoch = match plan.read_epoch {
                    Some(read_epoch) => read_epoch,
                    None => self.current_epoch(&plan.cell_id).await?,
                };
                let vertices = self
                    .out_neighbors_window_at(
                        &plan.cell_id,
                        &edge_type,
                        src,
                        read_epoch,
                        plan.result_window,
                        Some(&budget),
                    )
                    .await?;
                budget.check("query_out_neighbors")?;
                Ok(QueryOutput::Vertices(vertices))
            }
            PhysicalQueryPlan::EdgeExistsToCount {
                edge_type,
                src,
                dst,
            } => {
                let exists = self
                    .query_edge_exists(&plan.cell_id, &edge_type, src, dst, plan.read_epoch)
                    .await?;
                Ok(QueryOutput::Count(u64::from(exists)))
            }
            PhysicalQueryPlan::EdgeExistsToVertices {
                edge_type,
                src,
                dst,
            } => {
                let exists = self
                    .query_edge_exists(&plan.cell_id, &edge_type, src, dst, plan.read_epoch)
                    .await?;
                if exists {
                    Ok(QueryOutput::Vertices(
                        self.apply_query_window(vec![dst], plan.result_window)?,
                    ))
                } else {
                    Ok(QueryOutput::Vertices(Vec::new()))
                }
            }
            PhysicalQueryPlan::EdgeExistsToBool {
                edge_type,
                src,
                dst,
            } => {
                let exists = self
                    .query_edge_exists(&plan.cell_id, &edge_type, src, dst, plan.read_epoch)
                    .await?;
                Ok(QueryOutput::Bool(exists))
            }
            PhysicalQueryPlan::ReachableVertices {
                edge_type,
                src,
                min_hops,
                max_hops,
                return_count,
            } => {
                let read_epoch = match plan.read_epoch {
                    Some(read_epoch) => read_epoch,
                    None => self.current_epoch(&plan.cell_id).await?,
                };
                let vertices = self
                    .reachable_vertices_in_hop_range_at(
                        &plan.cell_id,
                        &edge_type,
                        src,
                        (min_hops, max_hops),
                        read_epoch,
                        &budget,
                    )
                    .await?
                    .0;
                budget.check("query_reachable")?;
                if return_count {
                    Ok(QueryOutput::Count(vertices.len() as u64))
                } else {
                    Ok(QueryOutput::Vertices(
                        self.apply_query_window(vertices, plan.result_window)?,
                    ))
                }
            }
        }
    }

    async fn validate_executable_query_plan(&self, plan: &QueryPlan) -> Result<()> {
        plan.validate_for_execution()?;
        if !plan.is_write() {
            if let Some(read_epoch) = plan.read_epoch {
                let current_epoch = self.current_epoch(&plan.cell_id).await?;
                if read_epoch > current_epoch {
                    return Err(GraphError::SnapshotAhead {
                        cell_id: plan.cell_id.clone(),
                        read_epoch,
                        current_epoch,
                    });
                }
                if read_epoch != current_epoch {
                    return Err(GraphError::UnsupportedQuery {
                        reason: QueryFailureReason::Other,
                        dialect: "GraphQueryPlan",
                        feature: "stale query plans are not pinned SlateDB snapshots".to_string(),
                    });
                }
            }
        }
        Ok(())
    }

    #[cfg(feature = "opencypher")]
    async fn query_read_epoch(&self, context: &QueryContext) -> Result<StorageSequence> {
        if let Some(read_epoch) = context.validated_read_epoch() {
            return Ok(read_epoch);
        }
        self.current_epoch(&context.cell_id).await
    }

    #[cfg(feature = "opencypher")]
    pub fn start_query_stats_refresh_job(
        self: Arc<Self>,
        specs: Vec<QueryStatsRefreshSpec>,
        interval: Duration,
    ) -> Result<QueryStatsRefreshHandle> {
        if specs.is_empty() {
            return Err(GraphError::UnsupportedQuery {
                reason: QueryFailureReason::Other,
                dialect: "QueryStats",
                feature: "stats refresh job requires at least one spec".to_string(),
            });
        }
        if interval.is_zero() {
            return Err(GraphError::AdmissionRejected {
                operation: "query_stats_refresh_interval_ms",
                actual: 0,
                limit: u64::MAX,
            });
        }
        for spec in &specs {
            validate_component("cell_id", &spec.cell_id)?;
            validate_query_stats_refresh_kind(&spec.kind)?;
        }
        let (stop_tx, mut stop_rx) = tokio::sync::watch::channel(false);
        let handle = tokio::spawn(async move {
            loop {
                for spec in &specs {
                    if *stop_rx.borrow() {
                        return Ok(());
                    }
                    if let Err(err) = self.refresh_query_stats_spec(spec).await {
                        tracing::warn!(
                            target: "hydradb",
                            cell_id = %spec.cell_id,
                            error = %err,
                            "query stats background refresh failed"
                        );
                    }
                }
                tokio::select! {
                    changed = stop_rx.changed() => {
                        if changed.is_err() || *stop_rx.borrow() {
                            return Ok(());
                        }
                    }
                    _ = tokio::time::sleep(interval) => {}
                }
            }
        });
        Ok(QueryStatsRefreshHandle {
            stop_tx: Some(stop_tx),
            handle: Some(handle),
        })
    }

    #[cfg(feature = "opencypher")]
    pub async fn refresh_query_stats_spec(
        &self,
        spec: &QueryStatsRefreshSpec,
    ) -> Result<QueryStatsRefreshResult> {
        validate_component("cell_id", &spec.cell_id)?;
        validate_query_stats_refresh_kind(&spec.kind)?;
        match &spec.kind {
            QueryStatsRefreshKind::Cardinality(QueryCardinalityStatsKind::EdgeExpansion {
                edge_type,
                direction,
                source_labels,
            }) => Ok(QueryStatsRefreshResult::Cardinality(
                self.refresh_edge_expansion_query_stats(
                    &spec.cell_id,
                    edge_type,
                    *direction,
                    source_labels,
                )
                .await?,
            )),
            QueryStatsRefreshKind::Cardinality(QueryCardinalityStatsKind::EdgeType {
                edge_type,
            }) => Ok(QueryStatsRefreshResult::Cardinality(
                self.refresh_edge_type_query_stats(&spec.cell_id, edge_type)
                    .await?,
            )),
            QueryStatsRefreshKind::Cardinality(QueryCardinalityStatsKind::VertexLabel {
                label,
            }) => Ok(QueryStatsRefreshResult::Cardinality(
                self.refresh_vertex_label_query_stats(&spec.cell_id, label)
                    .await?,
            )),
            QueryStatsRefreshKind::Cardinality(
                QueryCardinalityStatsKind::VertexLabelIntersection { labels },
            ) => Ok(QueryStatsRefreshResult::Cardinality(
                self.refresh_vertex_label_intersection_query_stats(&spec.cell_id, labels)
                    .await?,
            )),
            QueryStatsRefreshKind::Cardinality(QueryCardinalityStatsKind::VertexProperty {
                property,
                value,
            }) => Ok(QueryStatsRefreshResult::Cardinality(
                self.refresh_vertex_property_query_stats(&spec.cell_id, property, value)
                    .await?,
            )),
            QueryStatsRefreshKind::Cardinality(QueryCardinalityStatsKind::EdgeProperty {
                edge_type,
                property,
                value,
            }) => Ok(QueryStatsRefreshResult::Cardinality(
                self.refresh_edge_property_query_stats(&spec.cell_id, edge_type, property, value)
                    .await?,
            )),
            QueryStatsRefreshKind::VertexPropertyHistogram { property } => {
                Ok(QueryStatsRefreshResult::Histogram(
                    self.refresh_vertex_property_histogram_query_stats(&spec.cell_id, property)
                        .await?,
                ))
            }
            QueryStatsRefreshKind::EdgePropertyHistogram {
                edge_type,
                property,
            } => Ok(QueryStatsRefreshResult::Histogram(
                self.refresh_edge_property_histogram_query_stats(
                    &spec.cell_id,
                    edge_type,
                    property,
                )
                .await?,
            )),
        }
    }

    #[cfg(feature = "opencypher")]
    pub async fn refresh_edge_type_query_stats(
        &self,
        cell_id: &str,
        edge_type: &str,
    ) -> Result<QueryCardinalityStatsRefresh> {
        validate_component("cell_id", cell_id)?;
        validate_component("edge_type", edge_type)?;
        self.ensure_write_authority(cell_id, "refresh_edge_type_query_stats")?;
        let read_epoch = self.snapshot(cell_id).await?.read_epoch();
        let budget = QueryBudget::new(self.limits.max_query_runtime_ms, None);
        let count = self
            .edge_type_cardinality_from_degree_counters(cell_id, edge_type, &budget)
            .await?;
        let stats = QueryStatsRecord::point_count(count, read_epoch, graph_now_millis());
        self.publish_query_stats_record_after_snapshot(
            cell_id,
            "refresh_edge_type_query_stats",
            read_epoch,
            keys::query_stats_edge_type(cell_id, edge_type),
            &stats,
        )
        .await?;
        Ok(QueryCardinalityStatsRefresh {
            cell_id: cell_id.to_string(),
            read_epoch,
            kind: QueryCardinalityStatsKind::EdgeType {
                edge_type: edge_type.to_string(),
            },
            count,
            stats,
        })
    }

    #[cfg(feature = "opencypher")]
    pub async fn refresh_edge_expansion_query_stats(
        &self,
        cell_id: &str,
        edge_type: &str,
        direction: QueryStatsDirection,
        source_labels: &[String],
    ) -> Result<QueryCardinalityStatsRefresh> {
        validate_component("cell_id", cell_id)?;
        validate_component("edge_type", edge_type)?;
        let source_labels = canonical_query_stats_source_labels(source_labels)?;
        self.ensure_write_authority(cell_id, "refresh_edge_expansion_query_stats")?;
        let snapshot = self.snapshot(cell_id).await?;
        let read_epoch = snapshot.read_epoch();
        let budget = QueryBudget::new(self.limits.max_query_runtime_ms, None);
        let anchor = self
            .select_vertex_label_intersection_anchor(cell_id, &source_labels, read_epoch, &budget)
            .await?;
        let count = self
            .count_edge_expansion_at(EdgeExpansionStatsRequest {
                cell_id,
                edge_type,
                direction,
                anchor: &anchor,
                source_labels: &source_labels,
                snapshot: snapshot.storage_snapshot.as_ref(),
                read_epoch,
                budget: &budget,
            })
            .await?;
        let stats = QueryStatsRecord::point_count(count, read_epoch, graph_now_millis());
        let label_refs = source_labels.iter().map(String::as_str).collect::<Vec<_>>();
        self.publish_query_stats_record_after_snapshot(
            cell_id,
            "refresh_edge_expansion_query_stats",
            read_epoch,
            keys::query_stats_edge_expansion(
                cell_id,
                edge_type,
                direction.as_key_component(),
                &label_refs,
            ),
            &stats,
        )
        .await?;
        Ok(QueryCardinalityStatsRefresh {
            cell_id: cell_id.to_string(),
            read_epoch,
            kind: QueryCardinalityStatsKind::EdgeExpansion {
                edge_type: edge_type.to_string(),
                direction,
                source_labels,
            },
            count,
            stats,
        })
    }

    #[cfg(feature = "opencypher")]
    pub async fn refresh_vertex_label_query_stats(
        &self,
        cell_id: &str,
        label: &str,
    ) -> Result<QueryCardinalityStatsRefresh> {
        validate_component("cell_id", cell_id)?;
        validate_component("label", label)?;
        self.ensure_write_authority(cell_id, "refresh_vertex_label_query_stats")?;
        let read_epoch = self.snapshot(cell_id).await?.read_epoch();
        let budget = QueryBudget::new(self.limits.max_query_runtime_ms, None);
        let count = self
            .count_vertex_label_index_at(cell_id, label, read_epoch, &budget)
            .await?;
        let stats = QueryStatsRecord::point_count(count, read_epoch, graph_now_millis());
        self.publish_query_stats_record_after_snapshot(
            cell_id,
            "refresh_vertex_label_query_stats",
            read_epoch,
            keys::query_stats_vertex_label(cell_id, label),
            &stats,
        )
        .await?;
        Ok(QueryCardinalityStatsRefresh {
            cell_id: cell_id.to_string(),
            read_epoch,
            kind: QueryCardinalityStatsKind::VertexLabel {
                label: label.to_string(),
            },
            count,
            stats,
        })
    }

    #[cfg(feature = "opencypher")]
    pub async fn refresh_vertex_label_intersection_query_stats(
        &self,
        cell_id: &str,
        labels: &[String],
    ) -> Result<QueryCardinalityStatsRefresh> {
        validate_component("cell_id", cell_id)?;
        let labels = canonical_query_stats_labels(labels)?;
        self.ensure_write_authority(cell_id, "refresh_vertex_label_intersection_query_stats")?;
        let read_epoch = self.snapshot(cell_id).await?.read_epoch();
        let budget = QueryBudget::new(self.limits.max_query_runtime_ms, None);
        let anchor = self
            .select_vertex_label_intersection_anchor(cell_id, &labels, read_epoch, &budget)
            .await?;
        let count = self
            .count_vertex_label_intersection_at(cell_id, &anchor, &labels, read_epoch, &budget)
            .await?;
        let stats = QueryStatsRecord::point_count(count, read_epoch, graph_now_millis());
        let label_refs = labels.iter().map(String::as_str).collect::<Vec<_>>();
        self.publish_query_stats_record_after_snapshot(
            cell_id,
            "refresh_vertex_label_intersection_query_stats",
            read_epoch,
            keys::query_stats_vertex_label_intersection(cell_id, &label_refs),
            &stats,
        )
        .await?;
        Ok(QueryCardinalityStatsRefresh {
            cell_id: cell_id.to_string(),
            read_epoch,
            kind: QueryCardinalityStatsKind::VertexLabelIntersection { labels },
            count,
            stats,
        })
    }

    #[cfg(feature = "opencypher")]
    pub async fn refresh_vertex_property_query_stats(
        &self,
        cell_id: &str,
        property: &str,
        value: &VertexPropertyValue,
    ) -> Result<QueryCardinalityStatsRefresh> {
        validate_component("cell_id", cell_id)?;
        validate_component("property", property)?;
        self.ensure_write_authority(cell_id, "refresh_vertex_property_query_stats")?;
        let read_epoch = self.snapshot(cell_id).await?.read_epoch();
        let budget = QueryBudget::new(self.limits.max_query_runtime_ms, None);
        let count = self
            .count_vertex_property_index_at(cell_id, property, value, read_epoch, &budget)
            .await?;
        let encoded = encode_vertex_property_value_key(value);
        let histogram = self
            .vertex_property_histogram_counts(cell_id, property, read_epoch, &budget)
            .await?;
        let stats = stats_record_from_bucket_count(count, read_epoch, &histogram);
        self.publish_query_stats_record_after_snapshot(
            cell_id,
            "refresh_vertex_property_query_stats",
            read_epoch,
            keys::query_stats_vertex_property(cell_id, property, &encoded),
            &stats,
        )
        .await?;
        Ok(QueryCardinalityStatsRefresh {
            cell_id: cell_id.to_string(),
            read_epoch,
            kind: QueryCardinalityStatsKind::VertexProperty {
                property: property.to_string(),
                value: value.clone(),
            },
            count,
            stats,
        })
    }

    #[cfg(feature = "opencypher")]
    pub async fn refresh_edge_property_query_stats(
        &self,
        cell_id: &str,
        edge_type: &str,
        property: &str,
        value: &VertexPropertyValue,
    ) -> Result<QueryCardinalityStatsRefresh> {
        validate_component("cell_id", cell_id)?;
        validate_component("edge_type", edge_type)?;
        validate_component("property", property)?;
        self.ensure_write_authority(cell_id, "refresh_edge_property_query_stats")?;
        let read_epoch = self.snapshot(cell_id).await?.read_epoch();
        let budget = QueryBudget::new(self.limits.max_query_runtime_ms, None);
        let count = self
            .count_edge_property_index_at(cell_id, edge_type, property, value, read_epoch, &budget)
            .await?;
        let encoded = encode_vertex_property_value_key(value);
        let histogram = self
            .edge_property_histogram_counts(cell_id, edge_type, property, read_epoch, &budget)
            .await?;
        let stats = stats_record_from_bucket_count(count, read_epoch, &histogram);
        self.publish_query_stats_record_after_snapshot(
            cell_id,
            "refresh_edge_property_query_stats",
            read_epoch,
            keys::query_stats_edge_property(cell_id, edge_type, property, &encoded),
            &stats,
        )
        .await?;
        Ok(QueryCardinalityStatsRefresh {
            cell_id: cell_id.to_string(),
            read_epoch,
            kind: QueryCardinalityStatsKind::EdgeProperty {
                edge_type: edge_type.to_string(),
                property: property.to_string(),
                value: value.clone(),
            },
            count,
            stats,
        })
    }

    #[cfg(feature = "opencypher")]
    pub async fn refresh_vertex_property_histogram_query_stats(
        &self,
        cell_id: &str,
        property: &str,
    ) -> Result<QueryStatsHistogramRefresh> {
        validate_component("cell_id", cell_id)?;
        validate_component("property", property)?;
        self.ensure_write_authority(cell_id, "refresh_vertex_property_histogram_query_stats")?;
        let read_epoch = self.snapshot(cell_id).await?.read_epoch();
        let budget = QueryBudget::new(self.limits.max_query_runtime_ms, None);
        let buckets = self
            .vertex_property_histogram_counts(cell_id, property, read_epoch, &budget)
            .await?;
        let stats = stats_record_from_histogram(read_epoch, &buckets);
        self.publish_query_stats_histogram_after_snapshot(
            QueryStatsHistogramPublish {
                cell_id,
                operation: "refresh_vertex_property_histogram_query_stats",
                read_epoch,
                histogram_key: keys::query_stats_vertex_property_histogram(cell_id, property),
                bucket_prefix: keys::query_stats_vertex_property_prefix(cell_id, property),
                stats: &stats,
                buckets: &buckets,
            },
            |encoded| keys::query_stats_vertex_property(cell_id, property, encoded),
        )
        .await?;
        Ok(QueryStatsHistogramRefresh {
            cell_id: cell_id.to_string(),
            read_epoch,
            property: property.to_string(),
            edge_type: None,
            stats,
            buckets,
        })
    }

    #[cfg(feature = "opencypher")]
    pub async fn refresh_edge_property_histogram_query_stats(
        &self,
        cell_id: &str,
        edge_type: &str,
        property: &str,
    ) -> Result<QueryStatsHistogramRefresh> {
        validate_component("cell_id", cell_id)?;
        validate_component("edge_type", edge_type)?;
        validate_component("property", property)?;
        self.ensure_write_authority(cell_id, "refresh_edge_property_histogram_query_stats")?;
        let read_epoch = self.snapshot(cell_id).await?.read_epoch();
        let budget = QueryBudget::new(self.limits.max_query_runtime_ms, None);
        let buckets = self
            .edge_property_histogram_counts(cell_id, edge_type, property, read_epoch, &budget)
            .await?;
        let stats = stats_record_from_histogram(read_epoch, &buckets);
        self.publish_query_stats_histogram_after_snapshot(
            QueryStatsHistogramPublish {
                cell_id,
                operation: "refresh_edge_property_histogram_query_stats",
                read_epoch,
                histogram_key: keys::query_stats_edge_property_histogram(
                    cell_id, edge_type, property,
                ),
                bucket_prefix: keys::query_stats_edge_property_prefix(cell_id, edge_type, property),
                stats: &stats,
                buckets: &buckets,
            },
            |encoded| keys::query_stats_edge_property(cell_id, edge_type, property, encoded),
        )
        .await?;
        Ok(QueryStatsHistogramRefresh {
            cell_id: cell_id.to_string(),
            read_epoch,
            property: property.to_string(),
            edge_type: Some(edge_type.to_string()),
            stats,
            buckets,
        })
    }

    #[cfg(feature = "opencypher")]
    async fn publish_query_stats_record_after_snapshot(
        &self,
        cell_id: &str,
        operation: &'static str,
        read_epoch: StorageSequence,
        key: String,
        stats: &QueryStatsRecord,
    ) -> Result<()> {
        let _permit = self.acquire_graph_write_permit(operation).await?;
        let lock = self.acquire_local_write_guard(cell_id, operation).await?;
        let result = async {
            let current_epoch = self.current_epoch(cell_id).await?;
            if current_epoch != read_epoch {
                return Err(GraphError::QueryStatsSnapshotChanged {
                    operation,
                    cell_id: cell_id.to_string(),
                    read_epoch,
                    current_epoch,
                });
            }
            let mut batch = GraphWriteBatch::new();
            batch.put(key.as_bytes(), encode_u64(stats.count));
            batch.put(
                keys::query_stats_record_key(&key).as_bytes(),
                encode_query_stats_record(stats),
            );
            self.write_graph_batch_strict(cell_id, operation, batch)
                .await
        }
        .await;
        finish_local_write(lock, result).await
    }

    #[cfg(feature = "opencypher")]
    async fn publish_query_stats_histogram_after_snapshot(
        &self,
        publish: QueryStatsHistogramPublish<'_>,
        bucket_key: impl Fn(&str) -> String,
    ) -> Result<()> {
        let stale_bucket_keys = self.stale_query_stats_bucket_keys(&publish).await?;
        let _permit = self.acquire_graph_write_permit(publish.operation).await?;
        let lock = self
            .acquire_local_write_guard(publish.cell_id, publish.operation)
            .await?;
        let result = async {
            let current_epoch = self.current_epoch(publish.cell_id).await?;
            if current_epoch != publish.read_epoch {
                return Err(GraphError::QueryStatsSnapshotChanged {
                    operation: publish.operation,
                    cell_id: publish.cell_id.to_string(),
                    read_epoch: publish.read_epoch,
                    current_epoch,
                });
            }
            let mut batch = GraphWriteBatch::new();
            batch.put(
                publish.histogram_key.as_bytes(),
                encode_u64(publish.stats.count),
            );
            batch.put(
                keys::query_stats_record_key(&publish.histogram_key).as_bytes(),
                encode_query_stats_record(publish.stats),
            );
            for key in &stale_bucket_keys {
                batch.delete(key.as_bytes());
                batch.delete(keys::query_stats_record_key(key).as_bytes());
            }
            for (encoded, count) in publish.buckets {
                let key = bucket_key(encoded);
                let bucket_stats = QueryStatsRecord {
                    count: *count,
                    read_epoch: publish.stats.read_epoch,
                    refreshed_at_ms: publish.stats.refreshed_at_ms,
                    distinct_values: publish.stats.distinct_values,
                    total_values: publish.stats.total_values,
                    most_common_count: publish.stats.most_common_count,
                    bloom: None,
                };
                batch.put(key.as_bytes(), encode_u64(*count));
                batch.put(
                    keys::query_stats_record_key(&key).as_bytes(),
                    encode_query_stats_record(&bucket_stats),
                );
            }
            self.write_graph_batch_strict(publish.cell_id, publish.operation, batch)
                .await
        }
        .await;
        finish_local_write(lock, result).await
    }

    #[cfg(feature = "opencypher")]
    async fn stale_query_stats_bucket_keys(
        &self,
        publish: &QueryStatsHistogramPublish<'_>,
    ) -> Result<Vec<String>> {
        let mut iter = self.scan_remote_prefix(&publish.bucket_prefix).await?;
        let mut keys_to_delete = Vec::new();
        while let Some(kv) = iter.next().await? {
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            let Some(encoded) = key.strip_prefix(&publish.bucket_prefix) else {
                return Err(GraphError::CorruptValue {
                    key,
                    reason: "query stats bucket key does not match scan prefix".to_string(),
                });
            };
            if encoded.contains('/') {
                continue;
            }
            if !publish.buckets.contains_key(encoded) {
                keys_to_delete.push(key);
            }
        }
        Ok(keys_to_delete)
    }

    #[cfg(feature = "opencypher")]
    async fn edge_type_cardinality_from_degree_counters(
        &self,
        cell_id: &str,
        edge_type: &str,
        budget: &QueryBudget,
    ) -> Result<u64> {
        budget.check("query_stats_edge_type_degree_scan")?;
        let mut iter = self
            .scan_remote_prefix(&keys::degree_out_prefix(cell_id, edge_type))
            .await?;
        let mut count = 0_u64;
        while let Some(kv) = iter.next().await? {
            budget.check("query_stats_edge_type_degree_scan")?;
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            let degree = decode_u64(&key, &kv.value)?;
            count = count
                .checked_add(degree)
                .ok_or_else(|| GraphError::CorruptValue {
                    key,
                    reason: "edge-type cardinality overflow while summing degree counters"
                        .to_string(),
                })?;
        }
        Ok(count)
    }

    #[cfg(feature = "opencypher")]
    async fn vertex_property_histogram_counts(
        &self,
        cell_id: &str,
        property: &str,
        read_epoch: StorageSequence,
        budget: &QueryBudget,
    ) -> Result<BTreeMap<String, u64>> {
        budget.check("query_stats_vertex_property_histogram")?;
        let mut iter = self
            .scan_remote_prefix(&keys::vertex_property_index_property_prefix(
                cell_id, property,
            ))
            .await?;
        let mut buckets = BTreeMap::<String, u64>::new();
        let mut total = 0_u64;
        while let Some(kv) = iter.next().await? {
            budget.check("query_stats_vertex_property_histogram")?;
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            let (_cell_id, _property, encoded, vertex_id) = parse_vertex_property_index_key(&key)?;
            let metadata = self
                .vertex_metadata_at(cell_id, vertex_id, read_epoch, budget)
                .await?;
            let Some(value) = metadata.properties.get(property) else {
                continue;
            };
            if encode_vertex_property_value_key(value) != encoded {
                continue;
            }
            *buckets.entry(encoded).or_default() += 1;
            total = total
                .checked_add(1)
                .ok_or_else(|| GraphError::CorruptValue {
                    key,
                    reason: "vertex-property histogram count overflow".to_string(),
                })?;
            self.ensure_query_index_candidates(
                "query_stats_vertex_property_histogram_candidates",
                total as usize,
            )?;
        }
        Ok(buckets)
    }

    #[cfg(feature = "opencypher")]
    async fn edge_property_histogram_counts(
        &self,
        cell_id: &str,
        edge_type: &str,
        property: &str,
        read_epoch: StorageSequence,
        budget: &QueryBudget,
    ) -> Result<BTreeMap<String, u64>> {
        budget.check("query_stats_edge_property_histogram")?;
        let mut iter = self
            .scan_remote_prefix(&keys::edge_property_index_property_prefix(
                cell_id, edge_type, property,
            ))
            .await?;
        let mut buckets = BTreeMap::<String, u64>::new();
        let mut total = 0_u64;
        let latest_snapshot = read_epoch == self.current_epoch(cell_id).await?;
        while let Some(kv) = iter.next().await? {
            budget.check("query_stats_edge_property_histogram")?;
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            let (_cell_id, _edge_type, _property, encoded, src, dst) =
                parse_edge_property_index_key(&key)?;
            let edge_exists = if latest_snapshot {
                self.edge_exists(cell_id, edge_type, src, dst).await?
            } else {
                self.edge_exists_at(cell_id, edge_type, src, dst, read_epoch)
                    .await?
            };
            if !edge_exists {
                continue;
            }
            let metadata = self
                .edge_metadata_at(cell_id, edge_type, src, dst, read_epoch, budget)
                .await?;
            let Some(value) = metadata.properties.get(property) else {
                continue;
            };
            if encode_vertex_property_value_key(value) != encoded {
                continue;
            }
            *buckets.entry(encoded).or_default() += 1;
            total = total
                .checked_add(1)
                .ok_or_else(|| GraphError::CorruptValue {
                    key,
                    reason: "edge-property histogram count overflow".to_string(),
                })?;
            self.ensure_query_index_candidates(
                "query_stats_edge_property_histogram_candidates",
                total as usize,
            )?;
        }
        Ok(buckets)
    }

    #[cfg(feature = "opencypher")]
    async fn count_vertex_label_index_at(
        &self,
        cell_id: &str,
        label: &str,
        _read_epoch: StorageSequence,
        budget: &QueryBudget,
    ) -> Result<u64> {
        let mut iter = self
            .scan_remote_prefix(&keys::vertex_label_prefix(cell_id, label))
            .await?;
        let mut count = 0_u64;
        while let Some(kv) = iter.next().await? {
            budget.check("query_stats_vertex_label_count_current")?;
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            let _vertex_id = decode_u64(&key, &kv.value)?;
            count = count
                .checked_add(1)
                .ok_or_else(|| GraphError::CorruptValue {
                    key,
                    reason: "vertex-label stats count overflow".to_string(),
                })?;
            self.ensure_query_index_candidates(
                "query_stats_vertex_label_count_candidates",
                count as usize,
            )?;
        }
        Ok(count)
    }

    #[cfg(feature = "opencypher")]
    pub(super) async fn count_vertex_label_intersection_at(
        &self,
        cell_id: &str,
        anchor: &str,
        labels: &[String],
        read_epoch: StorageSequence,
        budget: &QueryBudget,
    ) -> Result<u64> {
        let mut iter = self
            .scan_remote_prefix(&keys::vertex_label_prefix(cell_id, anchor))
            .await?;
        let mut count = 0_u64;
        let mut candidates = 0_usize;
        while let Some(kv) = iter.next().await? {
            budget.check("query_stats_vertex_label_intersection")?;
            candidates = candidates
                .checked_add(1)
                .ok_or(GraphError::AdmissionRejected {
                    operation: "query_stats_vertex_label_intersection_candidates",
                    actual: u64::MAX,
                    limit: self.limits.max_query_index_candidates as u64,
                })?;
            // Charge every anchor row before metadata hydration. A sparse
            // intersection must not evade admission merely because most of a
            // broad anchor fails the remaining labels.
            self.ensure_query_index_candidates(
                "query_stats_vertex_label_intersection_candidates",
                candidates,
            )?;
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            let vertex_id = decode_u64(&key, &kv.value)?;
            let metadata = self
                .vertex_metadata_at(cell_id, vertex_id, read_epoch, budget)
                .await?;
            if labels.iter().all(|label| metadata.labels.contains(label)) {
                count = count
                    .checked_add(1)
                    .ok_or_else(|| GraphError::CorruptValue {
                        key,
                        reason: "vertex-label intersection count overflow".to_string(),
                    })?;
            }
        }
        Ok(count)
    }

    #[cfg(feature = "opencypher")]
    async fn count_edge_expansion_at(&self, request: EdgeExpansionStatsRequest<'_>) -> Result<u64> {
        let prefix = keys::vertex_label_prefix(request.cell_id, request.anchor);
        let mut iter = request
            .snapshot
            .scan_prefix_with_options(prefix.as_bytes(), .., &remote_scan_options())
            .await?;
        let mut candidates = 0_usize;
        let mut relationships = 0_u64;
        while let Some(kv) = iter.next().await? {
            request.budget.check("query_stats_edge_expansion")?;
            candidates = candidates
                .checked_add(1)
                .ok_or(GraphError::AdmissionRejected {
                    operation: "query_stats_edge_expansion_candidates",
                    actual: u64::MAX,
                    limit: self.limits.max_query_index_candidates as u64,
                })?;
            self.ensure_query_index_candidates(
                "query_stats_edge_expansion_candidates",
                candidates,
            )?;
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            let vertex_id = decode_u64(&key, &kv.value)?;
            let metadata = self
                .vertex_metadata_at(
                    request.cell_id,
                    vertex_id,
                    request.read_epoch,
                    request.budget,
                )
                .await?;
            if !request
                .source_labels
                .iter()
                .all(|label| metadata.labels.contains(label))
            {
                continue;
            }
            let degree = match request.direction {
                QueryStatsDirection::Outgoing => {
                    self.out_degree_in_storage_snapshot(
                        request.snapshot,
                        request.cell_id,
                        request.edge_type,
                        vertex_id,
                    )
                    .await?
                }
                QueryStatsDirection::Incoming => {
                    self.in_degree_in_storage_snapshot(
                        request.snapshot,
                        request.cell_id,
                        request.edge_type,
                        vertex_id,
                    )
                    .await?
                }
                QueryStatsDirection::Undirected => {
                    let outgoing = self
                        .out_degree_in_storage_snapshot(
                            request.snapshot,
                            request.cell_id,
                            request.edge_type,
                            vertex_id,
                        )
                        .await?;
                    let incoming = self
                        .in_degree_in_storage_snapshot(
                            request.snapshot,
                            request.cell_id,
                            request.edge_type,
                            vertex_id,
                        )
                        .await?;
                    outgoing
                        .checked_add(incoming)
                        .ok_or_else(|| GraphError::CorruptValue {
                            key: key.clone(),
                            reason: "undirected edge-expansion degree overflow".to_string(),
                        })?
                }
            };
            relationships =
                relationships
                    .checked_add(degree)
                    .ok_or_else(|| GraphError::CorruptValue {
                        key,
                        reason: "edge-expansion count overflow".to_string(),
                    })?;
            self.ensure_query_scan_edges("query_stats_edge_expansion_edges", relationships)?;
        }
        Ok(relationships)
    }

    #[cfg(feature = "opencypher")]
    async fn select_vertex_label_intersection_anchor(
        &self,
        cell_id: &str,
        labels: &[String],
        read_epoch: StorageSequence,
        budget: &QueryBudget,
    ) -> Result<String> {
        let now_ms = graph_now_millis();
        let candidates: Vec<_> = stream::iter(labels.iter().cloned())
            .map(|label| async move {
                let key = keys::query_stats_vertex_label(cell_id, &label);
                let count = budget
                    .read_only_io(
                        "query_stats_vertex_label_intersection_anchor",
                        self.query_stats_record(&key),
                    )
                    .await?
                    .filter(|record| !record.is_unusable_at(read_epoch, now_ms))
                    .map(|record| record.count);
                Ok::<_, GraphError>((label, count))
            })
            .buffer_unordered(QUERY_STATS_ANCHOR_READ_CONCURRENCY)
            .try_collect()
            .await?;
        candidates
            .into_iter()
            .min_by(|(left_label, left_count), (right_label, right_count)| {
                left_count
                    .unwrap_or(u64::MAX)
                    .cmp(&right_count.unwrap_or(u64::MAX))
                    .then_with(|| left_label.cmp(right_label))
            })
            .map(|(label, _)| label)
            .ok_or_else(|| GraphError::UnsupportedQuery {
                reason: QueryFailureReason::Other,
                dialect: "QueryStats",
                feature: "a label intersection requires at least one label".to_string(),
            })
    }

    #[cfg(feature = "opencypher")]
    async fn count_vertex_property_index_at(
        &self,
        cell_id: &str,
        property: &str,
        value: &VertexPropertyValue,
        read_epoch: StorageSequence,
        budget: &QueryBudget,
    ) -> Result<u64> {
        let encoded_keys = equivalent_property_index_keys(value);
        if encoded_keys.len() == 1 {
            return self
                .count_vertex_property_encoded_index_at(
                    cell_id,
                    property,
                    &encoded_keys[0],
                    read_epoch,
                    budget,
                )
                .await;
        }

        let mut vertices = BTreeSet::<VertexId>::new();
        for encoded in encoded_keys {
            self.collect_vertex_property_encoded_index_at(
                cell_id,
                property,
                &encoded,
                read_epoch,
                budget,
                &mut vertices,
            )
            .await?;
        }
        Ok(vertices.len() as u64)
    }

    #[cfg(feature = "opencypher")]
    async fn collect_vertex_property_encoded_index_at(
        &self,
        cell_id: &str,
        property: &str,
        encoded: &str,
        _read_epoch: StorageSequence,
        budget: &QueryBudget,
        vertices: &mut BTreeSet<VertexId>,
    ) -> Result<()> {
        let mut iter = self
            .scan_remote_prefix(&keys::vertex_property_index_prefix(
                cell_id, property, encoded,
            ))
            .await?;
        while let Some(kv) = iter.next().await? {
            budget.check("query_stats_vertex_property_collect_current")?;
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            let (_cell_id, _property, _encoded, vertex_id) = parse_vertex_property_index_key(&key)?;
            vertices.insert(vertex_id);
            self.ensure_query_index_candidates(
                "query_stats_vertex_property_collect_candidates",
                vertices.len(),
            )?;
        }
        Ok(())
    }

    #[cfg(feature = "opencypher")]
    async fn count_vertex_property_encoded_index_at(
        &self,
        cell_id: &str,
        property: &str,
        encoded: &str,
        _read_epoch: StorageSequence,
        budget: &QueryBudget,
    ) -> Result<u64> {
        let mut iter = self
            .scan_remote_prefix(&keys::vertex_property_index_prefix(
                cell_id, property, encoded,
            ))
            .await?;
        let mut count = 0_u64;
        while let Some(kv) = iter.next().await? {
            budget.check("query_stats_vertex_property_count_current")?;
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            let (_cell_id, _property, _encoded, _vertex_id) =
                parse_vertex_property_index_key(&key)?;
            count = count
                .checked_add(1)
                .ok_or_else(|| GraphError::CorruptValue {
                    key,
                    reason: "vertex-property stats count overflow".to_string(),
                })?;
            self.ensure_query_index_candidates(
                "query_stats_vertex_property_count_candidates",
                count as usize,
            )?;
        }
        Ok(count)
    }

    #[cfg(feature = "opencypher")]
    async fn count_edge_property_index_at(
        &self,
        cell_id: &str,
        edge_type: &str,
        property: &str,
        value: &VertexPropertyValue,
        _read_epoch: StorageSequence,
        budget: &QueryBudget,
    ) -> Result<u64> {
        let mut edges = BTreeSet::<(VertexId, VertexId)>::new();
        for encoded in equivalent_property_index_keys(value) {
            let scan = EncodedPropertyIndexScan {
                cell_id,
                edge_type,
                property,
                encoded: &encoded,
                budget,
            };
            self.collect_edge_property_encoded_index_at(scan, &mut edges)
                .await?;
        }
        Ok(edges.len() as u64)
    }

    #[cfg(feature = "opencypher")]
    async fn collect_edge_property_encoded_index_at(
        &self,
        scan: EncodedPropertyIndexScan<'_>,
        edges: &mut BTreeSet<(VertexId, VertexId)>,
    ) -> Result<()> {
        let mut iter = self
            .scan_remote_prefix(&keys::edge_property_index_prefix(
                scan.cell_id,
                scan.edge_type,
                scan.property,
                scan.encoded,
            ))
            .await?;
        while let Some(kv) = iter.next().await? {
            scan.budget
                .check("query_stats_edge_property_count_current")?;
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            let (_cell_id, _edge_type, _property, _encoded, src, dst) =
                parse_edge_property_index_key(&key)?;
            edges.insert((src, dst));
            self.ensure_query_index_candidates(
                "query_stats_edge_property_count_candidates",
                edges.len(),
            )?;
        }

        self.collect_relationship_property_encoded_index_at(scan, edges)
            .await?;
        Ok(())
    }

    #[cfg(feature = "opencypher")]
    async fn collect_relationship_property_encoded_index_at(
        &self,
        scan: EncodedPropertyIndexScan<'_>,
        edges: &mut BTreeSet<(VertexId, VertexId)>,
    ) -> Result<()> {
        let mut iter = self
            .scan_remote_prefix(&keys::relationship_property_index_prefix(
                scan.cell_id,
                scan.edge_type,
                scan.property,
                scan.encoded,
            ))
            .await?;
        while let Some(kv) = iter.next().await? {
            scan.budget
                .check("query_stats_relationship_property_count_current")?;
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            let (_cell_id, _edge_type, _property, _encoded, src, dst, _relationship_id) =
                parse_relationship_property_index_key(&key)?;
            edges.insert((src, dst));
            self.ensure_query_index_candidates(
                "query_stats_relationship_property_count_candidates",
                edges.len(),
            )?;
        }
        Ok(())
    }

    #[cfg(feature = "opencypher")]
    async fn match_row_patterns(
        &self,
        cell_id: &str,
        patterns: &[RowPattern],
        read_epoch: StorageSequence,
        budget: &QueryBudget,
    ) -> Result<Vec<BindingRow>> {
        if patterns.is_empty() {
            return Err(GraphError::UnsupportedQuery {
                reason: QueryFailureReason::Pattern,
                dialect: "OpenCypher",
                feature: "MATCH requires at least one executable pattern".to_string(),
            });
        }

        self.match_row_patterns_from_rows(
            cell_id,
            patterns,
            read_epoch,
            budget,
            vec![BindingRow::default()],
        )
        .await
    }

    #[cfg(feature = "opencypher")]
    async fn match_row_pattern_groups(
        &self,
        cell_id: &str,
        groups: &[RowMatchGroup],
        read_epoch: StorageSequence,
        budget: &QueryBudget,
    ) -> Result<Vec<BindingRow>> {
        if groups.is_empty() {
            return Err(GraphError::UnsupportedQuery {
                reason: QueryFailureReason::Pattern,
                dialect: "OpenCypher",
                feature: "MATCH requires at least one executable pattern".to_string(),
            });
        }

        let groups = budget
            .read_only_io(
                "cypher_plan_groups",
                self.optimize_row_match_groups_with_stats(cell_id, groups, read_epoch),
            )
            .await?;
        let mut rows = vec![BindingRow::default()];
        for group in &groups {
            budget.check("cypher_match_group")?;
            if group.patterns.is_empty() {
                return Err(GraphError::UnsupportedQuery {
                    reason: QueryFailureReason::Pattern,
                    dialect: "OpenCypher",
                    feature: "MATCH requires at least one executable pattern".to_string(),
                });
            }

            let mut group_rows = Vec::new();
            for row in rows {
                let initial_rows = if let Some(seed_rows) =
                    self.node_id_predicate_seed_rows(group, budget, &row)?
                {
                    seed_rows
                } else if let Some(seed_rows) = self
                    .relationship_predicate_seed_rows(cell_id, group, read_epoch, budget, &row)
                    .await?
                {
                    seed_rows
                } else if let Some(seed_rows) = self
                    .node_equality_predicate_seed_rows(cell_id, group, read_epoch, budget, &row)
                    .await?
                {
                    seed_rows
                } else if let Some(seed_rows) = self
                    .prefix_predicate_seed_rows(cell_id, group, read_epoch, budget, &row)
                    .await?
                {
                    seed_rows
                } else {
                    vec![row.clone()]
                };
                let mut matches = self
                    .match_row_patterns_from_rows(
                        cell_id,
                        &group.patterns,
                        read_epoch,
                        budget,
                        initial_rows,
                    )
                    .await?;
                if let Some(predicate) = &group.predicate {
                    let mut filtered = Vec::with_capacity(matches.len());
                    for matched in matches {
                        budget.check("cypher_group_where")?;
                        if row_predicate_matches(&matched, predicate)? {
                            filtered.push(matched);
                        }
                    }
                    matches = filtered;
                }

                if matches.is_empty() && group.optional {
                    let mut optional_row = row;
                    optional_row.mark_optional_group_nulls(group);
                    self.push_binding_row(
                        &mut group_rows,
                        optional_row,
                        "cypher_optional_match_rows",
                    )?;
                } else {
                    for matched in matches {
                        self.push_binding_row(&mut group_rows, matched, "cypher_match_group_rows")?;
                    }
                }
            }

            rows = group_rows;
            self.ensure_query_intermediate_rows("cypher_match_group_pipeline_rows", rows.len())?;
            if rows.is_empty() {
                break;
            }
        }
        Ok(rows)
    }

    #[cfg(feature = "opencypher")]
    pub(crate) async fn native_path_relationships_at(
        &self,
        cell_id: &str,
        edge_type: &str,
        src: VertexId,
        dst: VertexId,
        read_epoch: StorageSequence,
        budget: &QueryBudget,
    ) -> Result<Vec<(Option<RelationshipId>, EdgeMetadata)>> {
        let mut relationships = self
            .relationships_for_edge_at(cell_id, edge_type, src, dst, read_epoch, budget)
            .await?;
        let structural_metadata = relationships.iter().find_map(|(relationship, metadata)| {
            relationship
                .relationship_id
                .is_none()
                .then(|| metadata.clone())
        });
        if let Some(structural_metadata) = structural_metadata {
            if relationships
                .iter()
                .any(|(relationship, _)| relationship.relationship_id.is_some())
            {
                relationships = relationships
                    .into_iter()
                    .filter_map(|(relationship, metadata)| {
                        relationship.relationship_id.map(|_| {
                            let mut merged = structural_metadata.clone();
                            merged.properties.extend(metadata.properties);
                            (relationship, merged)
                        })
                    })
                    .collect();
            }
        }
        relationships.sort_by_key(|(relationship, _)| relationship.relationship_id);
        Ok(relationships
            .into_iter()
            .map(|(relationship, metadata)| (relationship.relationship_id, metadata))
            .collect())
    }

    #[cfg(feature = "opencypher")]
    async fn relationship_predicate_seed_rows(
        &self,
        cell_id: &str,
        group: &RowMatchGroup,
        read_epoch: StorageSequence,
        budget: &QueryBudget,
        input: &BindingRow,
    ) -> Result<Option<Vec<BindingRow>>> {
        if read_epoch != self.current_epoch(cell_id).await? {
            return Ok(None);
        }

        // An indexed relationship equality may be written either in WHERE
        // (`WHERE r.chunk_id = $id`) or directly in the pattern
        // (`[r:RELATES {chunk_id: $id}]`). Both are the same logical anchor.
        // Treating only the WHERE form as an index seed made the latter begin
        // from every matching endpoint node, which is catastrophic for the
        // production chunk-retrieval UNION on large tenant graphs.
        let predicate_constraints = group
            .predicate
            .as_ref()
            .map(row_predicate_property_equality_constraints)
            .unwrap_or_default();
        let mut seeds =
            Vec::<(&RowEdgePattern, RowPredicatePropertyEqualityConstraint, u64)>::new();
        for pattern in &group.patterns {
            let RowPattern::Edge(edge) = pattern else {
                continue;
            };
            let Some(binding) = edge.binding.as_ref() else {
                continue;
            };
            // This seed binds the stored source/destination directly. An
            // undirected pattern must go through the two-orientation matcher.
            if edge.direction == EdgeDirection::Bidirectional
                || edge.hop_range.is_some()
                || input.relationships.contains_key(binding)
            {
                continue;
            }

            let mut constraints = predicate_constraints
                .iter()
                .filter(|constraint| constraint.binding == *binding)
                .cloned()
                .collect::<Vec<_>>();
            constraints.extend(edge.properties.iter().map(|(property, value)| {
                RowPredicatePropertyEqualityConstraint {
                    binding: binding.clone(),
                    property: property.clone(),
                    values: vec![value.clone()],
                }
            }));

            // Inline properties live in a BTreeMap, but lexical property order
            // is unrelated to selectivity. Rank every valid equality using the
            // same persisted point/histogram statistics as the optimizer.
            for constraint in constraints {
                let estimate = self
                    .edge_property_equality_estimate(
                        cell_id,
                        &edge.edge_type,
                        &constraint.property,
                        &constraint.values,
                        16,
                    )
                    .await?;
                seeds.push((edge, constraint, estimate));
            }
        }
        // `sort_by_key` is stable, so absent/equal statistics retain parser
        // order, but execution is not committed to that guess: an over-budget
        // probe falls through to the next property instead of failing.
        seeds.sort_by_key(|(_, _, estimate)| *estimate);
        if seeds.is_empty() {
            return Ok(None);
        }

        // Race bounded probes instead of scanning them serially. With stale or
        // absent statistics, a selective index can finish without waiting for
        // every earlier broad index to consume a full candidate allowance and
        // the shared wall-clock budget. Dropping the race cancels unfinished
        // probes as soon as one usable seed completes.
        let mut probes = stream::FuturesUnordered::new();
        for (edge, constraint, _estimate) in seeds {
            probes.push(async move {
                let pairs = self
                    .try_scan_edge_property_values_at(
                        cell_id,
                        &edge.edge_type,
                        &constraint.property,
                        &constraint.values,
                        budget,
                        self.limits.max_query_index_candidates,
                    )
                    .await?;
                Ok::<_, GraphError>((edge, pairs))
            });
        }

        let mut selected = None;
        while let Some(probe) = probes.next().await {
            let (edge, pairs) = probe?;
            if let Some(pairs) = pairs {
                selected = Some((edge, pairs));
                break;
            }
        }
        let Some((edge, pairs)) = selected else {
            self.ensure_query_index_candidates(
                "cypher_edge_predicate_index_candidates",
                self.limits.max_query_index_candidates.saturating_add(1),
            )?;
            return Ok(None);
        };

        let mut metadata_cache = BTreeMap::new();
        let mut edge_metadata_cache = BTreeMap::new();
        let candidates = {
            let mut state = EdgeRowMatchState {
                cell_id,
                read_epoch,
                edge,
                rows: Vec::new(),
                pending: Vec::new(),
                metadata_cache: &mut metadata_cache,
                edge_metadata_cache: &mut edge_metadata_cache,
                budget,
            };
            for (src, dst) in pairs {
                budget.check("cypher_edge_predicate_index_rows")?;
                self.push_matching_edge_row(edge, src, dst, &mut state)
                    .await?;
            }
            self.finish_edge_rows(state).await?
        };

        let mut rows = Vec::with_capacity(candidates.len());
        for candidate in candidates {
            budget.check("cypher_edge_predicate_index_join")?;
            if let Some(joined) = input.join(&candidate) {
                self.push_binding_row(&mut rows, joined, "cypher_edge_predicate_index_seed_rows")?;
            }
        }
        Ok(Some(rows))
    }

    /// `MATCH (n) WHERE n.id IN $ids`: bind the listed vertices directly.
    ///
    /// Ahead of the property and prefix seeds because an id is the tightest
    /// anchor there is — it names the row instead of narrowing to it — and it
    /// costs no index read to produce. Each seeded row then reaches the node
    /// matcher as a plain id seek, the same plan an inline `(n {id: ...})`
    /// already gets, and the group predicate filters what comes back.
    ///
    /// An id in the list that names no stored vertex binds the same way an
    /// inline `(n {id: <unstored>})` does today. That is deliberate: a vertex
    /// here has no existence separate from what constrains it — an id can be an
    /// edge endpoint with no metadata record of its own — so a bare `(n)` with
    /// no labels and no properties has nothing to reject the binding with, by
    /// either spelling. Changing that is a change to node-pattern semantics,
    /// not to this seed.
    #[cfg(feature = "opencypher")]
    fn node_id_predicate_seed_rows(
        &self,
        group: &RowMatchGroup,
        budget: &QueryBudget,
        input: &BindingRow,
    ) -> Result<Option<Vec<BindingRow>>> {
        let Some(predicate) = group.predicate.as_ref() else {
            return Ok(None);
        };
        let Some((binding, ids)) = row_predicate_node_id_values(predicate) else {
            return Ok(None);
        };
        if input.values.contains_key(&binding) || input.null_values.contains(&binding) {
            return Ok(None);
        }
        // The binding has to name a node this group matches, and one whose id
        // the pattern does not already pin: an inline `{id: ...}` is the
        // stricter anchor and `constrain_row_pattern` applies it anyway.
        let seedable_node = |node: &RowNodePattern| {
            node.binding.as_deref() == Some(binding.as_str()) && node.id.is_none()
        };
        let seedable = group.patterns.iter().any(|pattern| match pattern {
            RowPattern::Node(node) => seedable_node(node),
            RowPattern::Edge(edge) => seedable_node(&edge.src) || seedable_node(&edge.dst),
        });
        if !seedable {
            return Ok(None);
        }
        self.ensure_query_index_candidates("cypher_node_id_seed_candidates", ids.len())?;
        let mut rows = Vec::with_capacity(ids.len());
        for id in ids {
            budget.check("cypher_node_id_seed_bind")?;
            let mut row = input.clone();
            if row.bind(Some(&binding), id) {
                self.push_binding_row(&mut rows, row, "cypher_node_id_seed_rows")?;
            }
        }
        Ok(Some(rows))
    }

    #[cfg(feature = "opencypher")]
    async fn node_equality_predicate_seed_rows(
        &self,
        cell_id: &str,
        group: &RowMatchGroup,
        read_epoch: StorageSequence,
        budget: &QueryBudget,
        input: &BindingRow,
    ) -> Result<Option<Vec<BindingRow>>> {
        let Some((binding, vertices)) = self
            .node_equality_predicate_seed_vertices(
                cell_id,
                group,
                read_epoch,
                budget,
                input,
                NodeEqualityProbe::PredicateEqualities(
                    256.min(self.limits.max_query_intermediate_rows),
                ),
            )
            .await?
        else {
            return Ok(None);
        };
        let mut rows = Vec::with_capacity(vertices.len());
        for vertex in vertices {
            budget.check("cypher_node_equality_seed_bind")?;
            let mut row = input.clone();
            if row.bind(Some(&binding), vertex) {
                self.push_binding_row(&mut rows, row, "cypher_node_equality_seed_rows")?;
            }
        }
        Ok(Some(rows))
    }

    #[cfg(feature = "opencypher")]
    async fn node_equality_predicate_seed_vertices(
        &self,
        cell_id: &str,
        group: &RowMatchGroup,
        read_epoch: StorageSequence,
        budget: &QueryBudget,
        input: &BindingRow,
        probe: NodeEqualityProbe,
    ) -> Result<Option<(String, Vec<VertexId>)>> {
        let (max_probe_entries, ordered) = match probe {
            NodeEqualityProbe::Ordered(max) => (max, true),
            NodeEqualityProbe::PredicateEqualities(max) => (max, false),
        };
        let mut constraints = group
            .predicate
            .as_ref()
            .map(row_predicate_property_equality_constraints)
            .unwrap_or_default();
        let eligible = |constraint: &RowPredicatePropertyEqualityConstraint| {
            !constraint.values.is_empty()
                && !input.values.contains_key(&constraint.binding)
                && !input.null_values.contains(&constraint.binding)
                && group.patterns.iter().any(|pattern| {
                    let eligible_node = |node: &RowNodePattern| {
                        node.binding.as_ref() == Some(&constraint.binding) && node.id.is_none()
                    };
                    match pattern {
                        RowPattern::Node(node) => eligible_node(node),
                        RowPattern::Edge(edge) => {
                            eligible_node(&edge.src) || eligible_node(&edge.dst)
                        }
                    }
                })
        };
        constraints.retain(&eligible);
        // Keep the existing predicate probe allowance intact. Broad inline
        // tenant constraints must not dilute a previously usable WHERE seed.
        let use_inline = ordered && constraints.is_empty();
        for pattern in &group.patterns {
            if !use_inline {
                break;
            }
            let mut add_node = |node: &RowNodePattern| {
                let Some(binding) = node.binding.as_ref().filter(|_| node.id.is_none()) else {
                    return;
                };
                for (property, value) in &node.properties {
                    let constraint = RowPredicatePropertyEqualityConstraint {
                        binding: binding.clone(),
                        property: property.clone(),
                        values: vec![value.clone()],
                    };
                    if !constraints.contains(&constraint) {
                        constraints.push(constraint);
                    }
                }
            };
            match pattern {
                RowPattern::Node(node) => add_node(node),
                RowPattern::Edge(edge) => {
                    add_node(&edge.src);
                    add_node(&edge.dst);
                }
            }
        }
        let constraints = constraints
            .iter()
            .filter(|constraint| eligible(constraint))
            .take(QUERY_PROPERTY_PROBE_CANDIDATES)
            .collect::<Vec<_>>();
        if constraints.is_empty() || read_epoch != self.current_epoch(cell_id).await? {
            return Ok(None);
        }

        // Runtime probes must also consider single equalities. Divide the
        // candidate allowance across constraints, while bounding equality
        // alternatives separately. Prefix opens and the terminal poll do not
        // produce candidates: charging them to the candidate allowance made a
        // mostly-absent batched OR exhaust its probe before reaching a later
        // selective value. An incomplete probe must still fall back, never seed
        // a partial result set. Ordered scans additionally admit inline values.
        let candidates = constraints
            .iter()
            .map(|constraint| {
                (
                    constraint.property.clone(),
                    constraint
                        .values
                        .iter()
                        .flat_map(equivalent_property_index_keys)
                        // The probe rejects a candidate that reaches its own
                        // alternatives cap, so truncating here cannot turn an
                        // incomplete probe into a complete one.
                        .take(QUERY_NODE_EQUALITY_PROBE_ALTERNATIVES)
                        .collect::<Vec<_>>(),
                )
            })
            .collect::<Vec<_>>();
        let Some((index, vertices)) = self
            .probe_property_index_candidates(cell_id, &candidates, max_probe_entries, budget)
            .await?
        else {
            return Ok(None);
        };
        Ok(Some((constraints[index].binding.clone(), vertices)))
    }

    /// Rank equality candidates by bounded index walks and return the vertices
    /// of the first one that proves selective.
    ///
    /// Each candidate gets a slice of one fixed candidate allowance. A
    /// candidate that exhausts its slice is too broad to anchor on and is
    /// discarded; one that finishes inside it has both proven itself selective
    /// and produced its exact vertex set. The walks race, so an answer costs
    /// about what the narrowest index takes to read rather than the sum.
    ///
    /// At most [`QUERY_PROPERTY_PROBE_CANDIDATES`] are ranked, which is also
    /// the bound on concurrent walks; anything past that is ignored.
    ///
    /// A candidate's keys must cover every value it stands for. One with no
    /// keys is declined rather than answered with an empty vertex set, because
    /// the caller cannot tell those apart and would drop every matching row.
    ///
    /// This is what lets a query plan well with no statistics at all: nothing
    /// here consults a stored cardinality, it measures the index instead.
    ///
    /// `candidates` pairs a property with the already-encoded index keys that
    /// satisfy it, so this never interprets a value. Callers holding a
    /// `VertexPropertyValue` expand it with `equivalent_property_index_keys`.
    /// The returned index selects the winning candidate, because the caller,
    /// not this walk, knows what a property was standing in for.
    #[cfg(feature = "opencypher")]
    pub(crate) async fn probe_property_index_candidates(
        &self,
        cell_id: &str,
        candidates: &[(String, Vec<String>)],
        max_probe_entries: usize,
        budget: &QueryBudget,
    ) -> Result<Option<(usize, Vec<VertexId>)>> {
        // Enforced here rather than trusted from the caller: this is where the
        // concurrent walks are started, and the experimental engine offers
        // every equality conjunct it found rather than a pre-trimmed list.
        let candidates = &candidates[..candidates.len().min(QUERY_PROPERTY_PROBE_CANDIDATES)];
        if candidates.is_empty() {
            return Ok(None);
        }
        let allowance = self
            .limits
            .max_query_index_candidates
            .min(max_probe_entries)
            / candidates.len();
        let alternative_allowance = QUERY_NODE_EQUALITY_PROBE_ALTERNATIVES / candidates.len();
        let concurrency = candidates.len();
        let mut probes = stream::iter(candidates.iter().enumerate())
            .map(|(index, (property, encoded_keys))| async move {
                // Nothing to walk is not the same as nothing to find. A
                // candidate with no index keys proves nothing about the
                // vertices it matches, and a completed empty walk would look
                // exactly like a property that is selective to zero rows.
                if encoded_keys.is_empty() {
                    return Ok::<_, GraphError>(None);
                }
                let mut remaining = allowance;
                let mut vertices = BTreeSet::new();
                let mut encodings = BTreeSet::new();
                let mut complete = true;
                'keys: for encoded in encoded_keys {
                    if encodings.contains(encoded) {
                        continue;
                    }
                    if encodings.len() >= alternative_allowance {
                        complete = false;
                        break 'keys;
                    }
                    encodings.insert(encoded.clone());
                    budget.check("cypher_node_equality_seed_seek")?;
                    let prefix = keys::vertex_property_index_prefix(cell_id, property, encoded);
                    let mut iter = budget
                        .read_only_io(
                            "cypher_node_equality_seed_seek",
                            self.db.scan_prefix_with_options(
                                prefix.as_bytes(),
                                None,
                                &remote_scan_options_for_expected_items(allowance as u64),
                            ),
                        )
                        .await?;
                    loop {
                        budget.check("cypher_node_equality_seed_probe")?;
                        let Some(kv) = budget
                            .read_only_io("cypher_node_equality_seed_probe", async {
                                iter.next().await.map_err(GraphError::from)
                            })
                            .await?
                        else {
                            break;
                        };
                        let Some(next) = remaining.checked_sub(1) else {
                            complete = false;
                            break 'keys;
                        };
                        remaining = next;
                        let key = String::from_utf8_lossy(&kv.key);
                        vertices.insert(decode_u64(&key, &kv.value)?);
                    }
                }
                if !complete {
                    return Ok::<_, GraphError>(None);
                }
                Ok(Some((index, vertices.into_iter().collect::<Vec<_>>())))
            })
            .buffer_unordered(concurrency)
            .boxed();
        while let Some(probe) = probes.try_next().await? {
            if let Some((index, vertices)) = probe {
                tracing::Span::current().record(
                    "hydradb.query.runtime_property_index",
                    candidates[index].0.as_str(),
                );
                return Ok(Some((index, vertices)));
            }
        }
        Ok(None)
    }

    #[cfg(feature = "opencypher")]
    async fn prefix_predicate_seed_rows(
        &self,
        cell_id: &str,
        group: &RowMatchGroup,
        read_epoch: StorageSequence,
        budget: &QueryBudget,
        input: &BindingRow,
    ) -> Result<Option<Vec<BindingRow>>> {
        let Some(RowPredicate::StartsWith {
            expression: RowExpression::Property { binding, property },
            prefix,
        }) = group.predicate.as_ref()
        else {
            return Ok(None);
        };
        if input.values.contains_key(binding) || read_epoch != self.current_epoch(cell_id).await? {
            return Ok(None);
        }
        let binding_is_present = group.patterns.iter().any(|pattern| match pattern {
            RowPattern::Node(node) => node.binding.as_deref() == Some(binding.as_str()),
            RowPattern::Edge(edge) => {
                edge.src.binding.as_deref() == Some(binding.as_str())
                    || edge.dst.binding.as_deref() == Some(binding.as_str())
            }
        });
        if !binding_is_present {
            return Ok(None);
        }

        let encoded_prefix =
            encode_vertex_property_value_key(&VertexPropertyValue::String(prefix.clone()));
        let index_prefix = format!(
            "{}{}",
            keys::vertex_property_index_property_prefix(cell_id, property),
            encoded_prefix
        );
        let mut iter = self.scan_remote_prefix(&index_prefix).await?;
        let mut vertices = BTreeSet::new();
        while let Some(kv) = iter.next().await? {
            budget.check("cypher_vertex_property_prefix_index")?;
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            let (_cell_id, indexed_property, _encoded, vertex_id) =
                parse_vertex_property_index_key(&key)?;
            if indexed_property != *property {
                continue;
            }
            vertices.insert(vertex_id);
            self.ensure_query_index_candidates(
                "cypher_vertex_property_prefix_candidates",
                vertices.len(),
            )?;
        }

        let mut rows = Vec::with_capacity(vertices.len());
        for vertex_id in vertices {
            budget.check("cypher_vertex_property_prefix_bind")?;
            let mut row = input.clone();
            if row.bind(Some(binding), vertex_id) {
                rows.push(row);
            }
        }
        Ok(Some(rows))
    }

    #[cfg(feature = "opencypher")]
    async fn match_row_patterns_from_rows(
        &self,
        cell_id: &str,
        patterns: &[RowPattern],
        read_epoch: StorageSequence,
        budget: &QueryBudget,
        mut rows: Vec<BindingRow>,
    ) -> Result<Vec<BindingRow>> {
        let initial_bindings = common_binding_row_bound_names(&rows);
        let patterns = budget
            .read_only_io(
                "cypher_plan_patterns",
                self.optimize_row_patterns_with_stats(
                    cell_id,
                    patterns,
                    read_epoch,
                    &initial_bindings,
                ),
            )
            .await?;
        for pattern in &patterns {
            budget.check("cypher_match_pipeline")?;
            if let Some(next_rows) = self
                .match_expand_into_pattern_from_rows(cell_id, pattern, read_epoch, budget, &rows)
                .await?
            {
                rows = next_rows;
                self.ensure_query_intermediate_rows("cypher_expand_into_rows", rows.len())?;
                if rows.is_empty() {
                    break;
                }
                continue;
            }
            if let Some(next_rows) = self
                .match_hash_join_pattern_from_rows(cell_id, pattern, read_epoch, budget, &rows)
                .await?
            {
                rows = next_rows;
                self.ensure_query_intermediate_rows("cypher_hash_join_rows", rows.len())?;
                if rows.is_empty() {
                    break;
                }
                continue;
            }
            let mut next_rows = Vec::new();
            for row in rows {
                let Some(bound_pattern) = constrain_row_pattern(pattern, &row)? else {
                    continue;
                };
                let matches = self
                    .match_row_pattern(cell_id, &bound_pattern, read_epoch, budget)
                    .await?;
                for matched in matches {
                    budget.check("cypher_match_join")?;
                    if let Some(joined) = row.join(&matched) {
                        self.push_binding_row(&mut next_rows, joined, "cypher_match_join_rows")?;
                    }
                }
            }
            rows = next_rows;
            self.ensure_query_intermediate_rows("cypher_match_pipeline_rows", rows.len())?;
            if rows.is_empty() {
                break;
            }
        }
        Ok(rows)
    }

    #[cfg(feature = "opencypher")]
    async fn match_expand_into_pattern_from_rows(
        &self,
        cell_id: &str,
        pattern: &RowPattern,
        read_epoch: StorageSequence,
        budget: &QueryBudget,
        rows: &[BindingRow],
    ) -> Result<Option<Vec<BindingRow>>> {
        let RowPattern::Edge(edge) = pattern else {
            return Ok(None);
        };
        if edge.direction == EdgeDirection::Bidirectional
            || edge.hop_range.is_some()
            || rows.is_empty()
        {
            return Ok(None);
        }

        let current_epoch = self.current_epoch(cell_id).await?;
        let latest_snapshot = read_epoch == current_epoch;
        let mut next_rows = Vec::new();
        let mut metadata_cache = BTreeMap::new();
        let mut edge_metadata_cache = BTreeMap::new();

        let mut groups: Vec<(usize, RowEdgePattern, Vec<BindingRow>)> = Vec::new();
        for (input_index, row) in rows.iter().enumerate() {
            budget.check("cypher_expand_into")?;
            let Some(RowPattern::Edge(bound_edge)) = constrain_row_pattern(pattern, row)? else {
                continue;
            };
            let (Some(src), Some(dst)) = (bound_edge.src.id, bound_edge.dst.id) else {
                return Ok(None);
            };
            let exists = if latest_snapshot {
                self.edge_exists(cell_id, &bound_edge.edge_type, src, dst)
                    .await?
            } else {
                self.edge_exists_at(cell_id, &bound_edge.edge_type, src, dst, read_epoch)
                    .await?
            };
            if !exists {
                continue;
            }
            let relationships = if let Some((property, value)) = bound_edge.properties.iter().next()
            {
                self.relationships_for_edge_property_at(RelationshipPropertyLookup {
                    cell_id,
                    edge_type: &bound_edge.edge_type,
                    src,
                    dst,
                    property,
                    value,
                    read_epoch,
                    budget,
                })
                .await?
            } else {
                self.relationships_for_edge_at(
                    cell_id,
                    &bound_edge.edge_type,
                    src,
                    dst,
                    read_epoch,
                    budget,
                )
                .await?
            };
            let mut matches = Vec::new();
            for (relationship, relationship_metadata) in relationships {
                let Some(matched) =
                    BindingRow::from_relationship(&bound_edge, relationship, relationship_metadata)
                else {
                    continue;
                };
                matches.push(matched);
            }
            if !matches.is_empty() {
                groups.push((input_index, bound_edge, matches));
            }
        }

        let wanted = groups
            .iter()
            .flat_map(|(_, _, matches)| matches.iter())
            .flat_map(|matched| matched.values.values().copied())
            .collect::<Vec<_>>();
        self.hydrate_vertex_metadata_cache_at(
            cell_id,
            &wanted,
            read_epoch,
            &mut metadata_cache,
            budget,
        )
        .await?;

        for (input_index, bound_edge, matches) in groups {
            let row = &rows[input_index];
            for mut matched in matches {
                self.hydrate_binding_metadata(
                    cell_id,
                    read_epoch,
                    &mut matched,
                    &mut metadata_cache,
                    budget,
                )
                .await?;
                self.hydrate_row_relationship_metadata(
                    cell_id,
                    read_epoch,
                    &mut matched,
                    &bound_edge,
                    &mut edge_metadata_cache,
                    budget,
                )
                .await?;
                if row_matches_edge_pattern(&matched, &bound_edge)? {
                    if let Some(joined) = row.join(&matched) {
                        self.push_binding_row(&mut next_rows, joined, "cypher_expand_into_rows")?;
                    }
                }
            }
        }
        Ok(Some(next_rows))
    }

    #[cfg(feature = "opencypher")]
    async fn match_hash_join_pattern_from_rows(
        &self,
        cell_id: &str,
        pattern: &RowPattern,
        read_epoch: StorageSequence,
        budget: &QueryBudget,
        rows: &[BindingRow],
    ) -> Result<Option<Vec<BindingRow>>> {
        if rows.len() <= 1 {
            return Ok(None);
        }
        let row_bindings = binding_rows_bound_names_union(rows);
        let pattern_bindings = row_pattern_bound_names(pattern);
        let join_bindings: Vec<_> = row_bindings
            .intersection(&pattern_bindings)
            .cloned()
            .collect();

        if join_bindings.is_empty() {
            let matches = self
                .match_row_pattern(cell_id, pattern, read_epoch, budget)
                .await?;
            let mut next_rows = Vec::new();
            for row in rows {
                for matched in &matches {
                    budget.check("cypher_precomputed_cross_join")?;
                    if let Some(joined) = row.join(matched) {
                        self.push_binding_row(
                            &mut next_rows,
                            joined,
                            "cypher_precomputed_cross_join_rows",
                        )?;
                    }
                }
            }
            return Ok(Some(next_rows));
        }

        if !hash_joinable_pattern(pattern) {
            return Ok(None);
        }
        let matches = if let RowPattern::Node(node) = pattern {
            // The incoming rows already identify the join vertices. Hydrating
            // the entire node pattern first discards selective upstream work.
            let binding = node.binding.as_ref().expect("node has a join binding");
            let vertices: Vec<_> = rows
                .iter()
                .filter_map(|row| row.values.get(binding).copied())
                .filter(|vertex| node.id.is_none_or(|id| id == *vertex))
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect();
            if vertices.len() <= self.limits.max_query_index_candidates.min(256) {
                self.match_node_row_candidates(cell_id, node, read_epoch, budget, vertices)
                    .await?
            } else {
                self.match_row_pattern(cell_id, pattern, read_epoch, budget)
                    .await?
            }
        } else {
            self.match_row_pattern(cell_id, pattern, read_epoch, budget)
                .await?
        };
        let mut matches_by_key = BTreeMap::<Vec<VertexId>, Vec<BindingRow>>::new();
        for matched in matches {
            if let Some(key) = binding_row_join_key(&matched, &join_bindings) {
                matches_by_key.entry(key).or_default().push(matched);
            }
        }

        let mut next_rows = Vec::new();
        for row in rows {
            budget.check("cypher_hash_join")?;
            let Some(key) = binding_row_join_key(row, &join_bindings) else {
                continue;
            };
            let Some(matches) = matches_by_key.get(&key) else {
                continue;
            };
            for matched in matches {
                if let Some(joined) = row.join(matched) {
                    self.push_binding_row(&mut next_rows, joined, "cypher_hash_join_rows")?;
                }
            }
        }
        Ok(Some(next_rows))
    }

    #[cfg(feature = "opencypher")]
    async fn match_row_pattern(
        &self,
        cell_id: &str,
        pattern: &RowPattern,
        read_epoch: StorageSequence,
        budget: &QueryBudget,
    ) -> Result<Vec<BindingRow>> {
        validate_component("cell_id", cell_id)?;
        budget.check("cypher_match")?;
        match pattern {
            RowPattern::Node(node) => {
                self.match_node_row_pattern(cell_id, node, read_epoch, budget)
                    .await
            }
            RowPattern::Edge(edge) => {
                self.match_edge_row_pattern(cell_id, edge, read_epoch, budget)
                    .await
            }
        }
    }

    #[cfg(feature = "opencypher")]
    async fn match_node_row_pattern(
        &self,
        cell_id: &str,
        node: &RowNodePattern,
        read_epoch: StorageSequence,
        budget: &QueryBudget,
    ) -> Result<Vec<BindingRow>> {
        let Some(vertices) = self
            .candidate_vertex_ids(cell_id, node, read_epoch, budget)
            .await?
        else {
            return Err(GraphError::UnsupportedQuery {
                reason: QueryFailureReason::Pattern,
                dialect: "OpenCypher",
                feature: "node-only MATCH requires an id, label, or property predicate".to_string(),
            });
        };
        self.match_node_row_candidates(cell_id, node, read_epoch, budget, vertices)
            .await
    }

    #[cfg(feature = "opencypher")]
    async fn match_node_row_candidates(
        &self,
        cell_id: &str,
        node: &RowNodePattern,
        read_epoch: StorageSequence,
        budget: &QueryBudget,
        vertices: Vec<VertexId>,
    ) -> Result<Vec<BindingRow>> {
        self.ensure_query_index_candidates("cypher_node_candidates", vertices.len())?;
        let mut rows = Vec::with_capacity(vertices.len());
        let mut metadata_cache = BTreeMap::new();

        let mut pending = Vec::with_capacity(vertices.len());
        for vertex_id in vertices {
            budget.check("cypher_node_rows")?;
            if let Some(row) = BindingRow::from_node(node, vertex_id) {
                pending.push(row);
            }
        }

        let wanted = pending
            .iter()
            .flat_map(|row| row.values.values().copied())
            .collect::<Vec<_>>();
        self.hydrate_vertex_metadata_cache_at(
            cell_id,
            &wanted,
            read_epoch,
            &mut metadata_cache,
            budget,
        )
        .await?;

        for mut row in pending {
            self.hydrate_binding_metadata(
                cell_id,
                read_epoch,
                &mut row,
                &mut metadata_cache,
                budget,
            )
            .await?;
            if row_matches_node(&row, node)? {
                self.push_binding_row(&mut rows, row, "cypher_node_rows")?;
            }
        }
        Ok(rows)
    }

    /// `(a)-[:R]-(b)`: match the hop in both orientations and merge.
    ///
    /// The fan-out lives here, inside pattern matching, and NOT at the query
    /// level as union arms. `execute_union_opencypher_rows` finishes each arm
    /// independently and never reapplies the query's `ORDER BY` across them, so
    /// expanding an undirected hop into arms would silently unsort the result
    /// and let each arm keep its own `LIMIT`. Merging here means the combined
    /// rows go through one projection pipeline, so `ORDER BY`, `DISTINCT` and
    /// `LIMIT` each apply once.
    ///
    /// `reversed()` returns an `Outbound` pattern, so the oriented matcher
    /// below cannot fan out a second time.
    #[cfg(feature = "opencypher")]
    async fn match_edge_row_pattern(
        &self,
        cell_id: &str,
        edge: &RowEdgePattern,
        read_epoch: StorageSequence,
        budget: &QueryBudget,
    ) -> Result<Vec<BindingRow>> {
        if edge.direction != EdgeDirection::Bidirectional {
            return self
                .match_edge_row_pattern_oriented(cell_id, edge, read_epoch, budget)
                .await;
        }
        let mut rows = self
            .match_edge_row_pattern_oriented(cell_id, edge, read_epoch, budget)
            .await?;
        let reversed = edge.reversed();
        let reverse_rows = self
            .match_edge_row_pattern_oriented(cell_id, &reversed, read_epoch, budget)
            .await?;
        // A single-hop row carries one physical relationship. Only self-loops
        // duplicate the forward orientation. A non-loop has two matches even
        // when neither endpoint has a binding, so row-value dedup is unsound.
        for row in reverse_rows {
            budget.check("cypher_undirected_edge_merge")?;
            if row
                .relationship_metadata
                .keys()
                .any(|relationship| relationship.src != relationship.dst)
            {
                self.push_binding_row(&mut rows, row, "cypher_undirected_edge_rows")?;
            }
        }
        self.ensure_query_intermediate_rows("cypher_undirected_edge_rows", rows.len())?;
        Ok(rows)
    }

    #[cfg(feature = "opencypher")]
    async fn match_edge_row_pattern_oriented(
        &self,
        cell_id: &str,
        edge: &RowEdgePattern,
        read_epoch: StorageSequence,
        budget: &QueryBudget,
    ) -> Result<Vec<BindingRow>> {
        validate_component("edge_type", &edge.edge_type)?;
        if let Some((min_hops, max_hops)) = edge.hop_range {
            return self
                .match_reachable_row_pattern(cell_id, edge, min_hops, max_hops, read_epoch, budget)
                .await;
        }

        let access = budget
            .read_only_io(
                "cypher_plan_edge_access",
                self.best_row_edge_access_with_stats(cell_id, edge, read_epoch, &BTreeSet::new()),
            )
            .await?;
        match access {
            RowQueryAccess::ExpandInto { .. } => {
                self.match_edge_row_pattern_expand_into(cell_id, edge, read_epoch, budget)
                    .await
            }
            RowQueryAccess::BoundOutExpand { .. } => {
                let Some(sources) = self
                    .candidate_vertex_ids(cell_id, &edge.src, read_epoch, budget)
                    .await?
                else {
                    return self
                        .match_edge_row_pattern_full_scan(cell_id, edge, read_epoch, budget)
                        .await;
                };
                self.match_edge_row_pattern_from_sources(cell_id, edge, sources, read_epoch, budget)
                    .await
            }
            RowQueryAccess::BoundInExpand { .. } => {
                let Some(destinations) = self
                    .candidate_vertex_ids(cell_id, &edge.dst, read_epoch, budget)
                    .await?
                else {
                    return self
                        .match_edge_row_pattern_full_scan(cell_id, edge, read_epoch, budget)
                        .await;
                };
                self.match_edge_row_pattern_from_destinations(
                    cell_id,
                    edge,
                    destinations,
                    read_epoch,
                    budget,
                )
                .await
            }
            RowQueryAccess::EdgePropertyIndex { .. } => {
                self.match_edge_row_pattern_from_property_index(cell_id, edge, read_epoch, budget)
                    .await
            }
            RowQueryAccess::FullEdgeScan { .. } => {
                self.match_edge_row_pattern_full_scan(cell_id, edge, read_epoch, budget)
                    .await
            }
            RowQueryAccess::VariableLengthExpand { .. } => Err(GraphError::CorruptValue {
                key: format!("cell/{cell_id}/query/edge-access/{}", edge.edge_type),
                reason: "optimizer sent variable-length edge pattern to single-edge matcher"
                    .to_string(),
            }),
            RowQueryAccess::VertexIdSeek
            | RowQueryAccess::VertexPropertyIndex { .. }
            | RowQueryAccess::VertexLabelScan { .. }
            | RowQueryAccess::AllVertexScan => Err(GraphError::CorruptValue {
                key: format!("cell/{cell_id}/query/edge-access/{}", edge.edge_type),
                reason: "optimizer selected node access for edge pattern".to_string(),
            }),
        }
    }

    #[cfg(feature = "opencypher")]
    async fn match_edge_row_pattern_expand_into(
        &self,
        cell_id: &str,
        edge: &RowEdgePattern,
        read_epoch: StorageSequence,
        budget: &QueryBudget,
    ) -> Result<Vec<BindingRow>> {
        let Some(sources) = self
            .candidate_vertex_ids(cell_id, &edge.src, read_epoch, budget)
            .await?
        else {
            return self
                .match_edge_row_pattern_full_scan(cell_id, edge, read_epoch, budget)
                .await;
        };
        let Some(destinations) = self
            .candidate_vertex_ids(cell_id, &edge.dst, read_epoch, budget)
            .await?
        else {
            return self
                .match_edge_row_pattern_full_scan(cell_id, edge, read_epoch, budget)
                .await;
        };
        self.ensure_query_index_candidates("cypher_edge_expand_into_sources", sources.len())?;
        self.ensure_query_index_candidates(
            "cypher_edge_expand_into_destinations",
            destinations.len(),
        )?;
        let candidate_pairs = sources
            .len()
            .checked_mul(destinations.len())
            .ok_or_else(|| GraphError::CorruptValue {
                key: format!("cell/{cell_id}/query/expand-into/{}", edge.edge_type),
                reason: "expand-into candidate pair count overflow".to_string(),
            })?;
        self.ensure_query_index_candidates("cypher_edge_expand_into_pairs", candidate_pairs)?;

        let current_epoch = self.current_epoch(cell_id).await?;
        let latest_snapshot = read_epoch == current_epoch;
        let mut metadata_cache = BTreeMap::new();
        let mut edge_metadata_cache = BTreeMap::new();
        let mut state = EdgeRowMatchState {
            cell_id,
            read_epoch,
            edge,
            rows: Vec::new(),
            pending: Vec::new(),
            metadata_cache: &mut metadata_cache,
            edge_metadata_cache: &mut edge_metadata_cache,
            budget,
        };
        for src in sources {
            for dst in &destinations {
                budget.check("cypher_edge_expand_into")?;
                let exists = if latest_snapshot {
                    self.edge_exists(cell_id, &edge.edge_type, src, *dst)
                        .await?
                } else {
                    self.edge_exists_at(cell_id, &edge.edge_type, src, *dst, read_epoch)
                        .await?
                };
                if exists {
                    self.push_matching_edge_row(edge, src, *dst, &mut state)
                        .await?;
                }
            }
        }
        self.finish_edge_rows(state).await
    }

    #[cfg(feature = "opencypher")]
    async fn match_edge_row_pattern_from_property_index(
        &self,
        cell_id: &str,
        edge: &RowEdgePattern,
        read_epoch: StorageSequence,
        budget: &QueryBudget,
    ) -> Result<Vec<BindingRow>> {
        let Some((property, value)) = edge.properties.iter().next() else {
            return Ok(Vec::new());
        };
        let pairs = self
            .scan_edge_property_index_at(
                cell_id,
                &edge.edge_type,
                property,
                value,
                read_epoch,
                budget,
            )
            .await?;
        self.ensure_query_index_candidates("cypher_edge_property_candidates", pairs.len())?;
        let mut metadata_cache = BTreeMap::new();
        let mut edge_metadata_cache = BTreeMap::new();
        let mut state = EdgeRowMatchState {
            cell_id,
            read_epoch,
            edge,
            rows: Vec::new(),
            pending: Vec::new(),
            metadata_cache: &mut metadata_cache,
            edge_metadata_cache: &mut edge_metadata_cache,
            budget,
        };
        for (src, dst) in pairs {
            budget.check("cypher_edge_property_rows")?;
            self.push_matching_edge_row(edge, src, dst, &mut state)
                .await?;
        }
        self.finish_edge_rows(state).await
    }

    #[cfg(feature = "opencypher")]
    async fn match_edge_row_pattern_from_sources(
        &self,
        cell_id: &str,
        edge: &RowEdgePattern,
        sources: Vec<VertexId>,
        read_epoch: StorageSequence,
        budget: &QueryBudget,
    ) -> Result<Vec<BindingRow>> {
        self.ensure_query_index_candidates("cypher_edge_source_candidates", sources.len())?;
        let mut metadata_cache = BTreeMap::new();
        let mut edge_metadata_cache = BTreeMap::new();
        let mut state = EdgeRowMatchState {
            cell_id,
            read_epoch,
            edge,
            rows: Vec::new(),
            pending: Vec::new(),
            metadata_cache: &mut metadata_cache,
            edge_metadata_cache: &mut edge_metadata_cache,
            budget,
        };
        for src in sources {
            budget.check("cypher_edge_sources")?;
            let neighbors = self
                .out_neighbors_at_for_query(cell_id, &edge.edge_type, src, read_epoch, budget)
                .await?;
            let scanned_edges = budget.add_scanned_edges(neighbors.len() as u64);
            self.ensure_query_scan_edges("cypher_edge_neighbor_scan", scanned_edges)?;
            for dst in neighbors {
                self.push_matching_edge_row(edge, src, dst, &mut state)
                    .await?;
            }
        }
        self.finish_edge_rows(state).await
    }

    #[cfg(feature = "opencypher")]
    async fn match_edge_row_pattern_from_destinations(
        &self,
        cell_id: &str,
        edge: &RowEdgePattern,
        destinations: Vec<VertexId>,
        read_epoch: StorageSequence,
        budget: &QueryBudget,
    ) -> Result<Vec<BindingRow>> {
        self.ensure_query_index_candidates(
            "cypher_edge_destination_candidates",
            destinations.len(),
        )?;
        let mut metadata_cache = BTreeMap::new();
        let mut edge_metadata_cache = BTreeMap::new();
        let mut state = EdgeRowMatchState {
            cell_id,
            read_epoch,
            edge,
            rows: Vec::new(),
            pending: Vec::new(),
            metadata_cache: &mut metadata_cache,
            edge_metadata_cache: &mut edge_metadata_cache,
            budget,
        };
        for dst in destinations {
            budget.check("cypher_edge_destinations")?;
            let neighbors = self
                .in_neighbors_at_for_query(cell_id, &edge.edge_type, dst, read_epoch, budget)
                .await?;
            let scanned_edges = budget.add_scanned_edges(neighbors.len() as u64);
            self.ensure_query_scan_edges("cypher_edge_reverse_neighbor_scan", scanned_edges)?;
            for src in neighbors {
                self.push_matching_edge_row(edge, src, dst, &mut state)
                    .await?;
            }
        }
        self.finish_edge_rows(state).await
    }

    #[cfg(feature = "opencypher")]
    async fn match_edge_row_pattern_full_scan(
        &self,
        cell_id: &str,
        edge: &RowEdgePattern,
        read_epoch: StorageSequence,
        budget: &QueryBudget,
    ) -> Result<Vec<BindingRow>> {
        let mut metadata_cache = BTreeMap::new();
        let mut edge_metadata_cache = BTreeMap::new();
        let records = self
            .edges_at_with_budget(cell_id, &edge.edge_type, read_epoch, Some(budget))
            .await?;
        let scanned_edges = budget.add_scanned_edges(records.len() as u64);
        self.ensure_query_scan_edges("cypher_edge_full_scan", scanned_edges)?;
        let mut state = EdgeRowMatchState {
            cell_id,
            read_epoch,
            edge,
            rows: Vec::new(),
            pending: Vec::new(),
            metadata_cache: &mut metadata_cache,
            edge_metadata_cache: &mut edge_metadata_cache,
            budget,
        };
        for record in records {
            budget.check("cypher_edge_full_scan")?;
            self.push_matching_edge_row(edge, record.src, record.dst, &mut state)
                .await?;
        }
        self.finish_edge_rows(state).await
    }

    #[cfg(feature = "opencypher")]
    async fn push_matching_edge_row(
        &self,
        edge: &RowEdgePattern,
        src: VertexId,
        dst: VertexId,
        state: &mut EdgeRowMatchState<'_>,
    ) -> Result<()> {
        state.budget.check("cypher_edge_rows")?;
        if matches!(edge.src.id, Some(fixed_src) if fixed_src != src)
            || matches!(edge.dst.id, Some(fixed_dst) if fixed_dst != dst)
        {
            return Ok(());
        }
        let relationships = if let Some((property, value)) = edge.properties.iter().next() {
            self.relationships_for_edge_property_at(RelationshipPropertyLookup {
                cell_id: state.cell_id,
                edge_type: &edge.edge_type,
                src,
                dst,
                property,
                value,
                read_epoch: state.read_epoch,
                budget: state.budget,
            })
            .await?
        } else {
            self.relationships_for_edge_at(
                state.cell_id,
                &edge.edge_type,
                src,
                dst,
                state.read_epoch,
                state.budget,
            )
            .await?
        };
        for (relationship, metadata) in relationships {
            let Some(row) = BindingRow::from_relationship(edge, relationship, metadata) else {
                continue;
            };
            state.pending.push(row);
            // Checked per row, not per pair. One `(src, dst)` usually carries a
            // single relationship, but a multigraph pair can carry thousands,
            // and checking after the loop would buffer every one of them before
            // asking -- which is not the bound `EDGE_ROW_HYDRATION_BATCH`
            // claims. Every caller still flushes after its traversal; this only
            // decides how often that happens, never whether it does.
            if state.pending.len() >= EDGE_ROW_HYDRATION_BATCH {
                self.flush_pending_edge_rows(state).await?;
            }
        }
        Ok(())
    }

    #[cfg(feature = "opencypher")]
    async fn flush_pending_edge_rows(&self, state: &mut EdgeRowMatchState<'_>) -> Result<()> {
        if state.pending.is_empty() {
            return Ok(());
        }
        let pending = std::mem::take(&mut state.pending);

        let wanted = pending
            .iter()
            .flat_map(|row| row.values.values().copied())
            .collect::<Vec<_>>();
        self.hydrate_vertex_metadata_cache_at(
            state.cell_id,
            &wanted,
            state.read_epoch,
            state.metadata_cache,
            state.budget,
        )
        .await?;

        for mut row in pending {
            self.hydrate_binding_metadata(
                state.cell_id,
                state.read_epoch,
                &mut row,
                state.metadata_cache,
                state.budget,
            )
            .await?;
            self.hydrate_row_relationship_metadata(
                state.cell_id,
                state.read_epoch,
                &mut row,
                state.edge,
                state.edge_metadata_cache,
                state.budget,
            )
            .await?;
            if row_matches_edge_pattern(&row, state.edge)? {
                self.push_binding_row(&mut state.rows, row, "cypher_edge_rows")?;
            }
        }
        Ok(())
    }

    /// Drain the last batch and hand back the traversal's rows.
    ///
    /// Every traversal ends here. Rows accumulate un-hydrated while the pattern
    /// is walked, so a caller that returned its rows directly would drop the
    /// last partial batch and report fewer matches than it found, with no error
    /// anywhere -- a failure mode the suite did not catch when I introduced it
    /// deliberately.
    ///
    /// Taking `state` by value drops the buffer with it, which is what makes an
    /// error raised mid-traversal safe: the query failed, the buffered rows are
    /// discarded, and nothing has to distinguish that from a forgotten flush.
    /// An earlier attempt asserted on a non-empty buffer in `Drop` and turned
    /// every mid-scan admission failure into a panic.
    #[cfg(feature = "opencypher")]
    async fn finish_edge_rows(&self, mut state: EdgeRowMatchState<'_>) -> Result<Vec<BindingRow>> {
        self.flush_pending_edge_rows(&mut state).await?;
        Ok(state.rows)
    }

    #[cfg(feature = "opencypher")]
    async fn match_reachable_row_pattern(
        &self,
        cell_id: &str,
        edge: &RowEdgePattern,
        min_hops: u8,
        max_hops: u8,
        read_epoch: StorageSequence,
        budget: &QueryBudget,
    ) -> Result<Vec<BindingRow>> {
        let Some(src) = edge.src.id else {
            return Err(GraphError::UnsupportedQuery {
                reason: QueryFailureReason::Pattern,
                dialect: "OpenCypher",
                feature: "variable-length MATCH requires a fixed source id".to_string(),
            });
        };
        let (vertices, edge_visits) = self
            .reachable_vertices_in_hop_range_at(
                cell_id,
                &edge.edge_type,
                src,
                (min_hops, max_hops),
                read_epoch,
                budget,
            )
            .await?;
        budget.check("cypher_reachable")?;
        self.ensure_query_scan_edges("cypher_reachable_edge_visits", edge_visits)?;
        self.ensure_query_intermediate_rows("cypher_reachable_rows", vertices.len())?;
        let mut rows = Vec::with_capacity(vertices.len());
        let mut metadata_cache = BTreeMap::new();
        let mut edge_metadata_cache = BTreeMap::new();

        let mut pending = Vec::with_capacity(vertices.len());
        for dst in vertices {
            budget.check("cypher_reachable_rows")?;
            if matches!(edge.dst.id, Some(fixed_dst) if fixed_dst != dst) {
                continue;
            }
            if let Some(row) = BindingRow::from_edge(edge, src, dst) {
                pending.push(row);
            }
        }

        let wanted = pending
            .iter()
            .flat_map(|row| row.values.values().copied())
            .collect::<Vec<_>>();
        self.hydrate_vertex_metadata_cache_at(
            cell_id,
            &wanted,
            read_epoch,
            &mut metadata_cache,
            budget,
        )
        .await?;

        for mut row in pending {
            self.hydrate_binding_metadata(
                cell_id,
                read_epoch,
                &mut row,
                &mut metadata_cache,
                budget,
            )
            .await?;
            self.hydrate_row_relationship_metadata(
                cell_id,
                read_epoch,
                &mut row,
                edge,
                &mut edge_metadata_cache,
                budget,
            )
            .await?;
            if row_matches_edge_pattern(&row, edge)? {
                self.push_binding_row(&mut rows, row, "cypher_reachable_rows")?;
            }
        }
        Ok(rows)
    }

    #[cfg(feature = "opencypher")]
    async fn candidate_vertex_ids(
        &self,
        cell_id: &str,
        pattern: &RowNodePattern,
        read_epoch: StorageSequence,
        budget: &QueryBudget,
    ) -> Result<Option<Vec<VertexId>>> {
        budget.check("cypher_candidate_vertices")?;
        let access = budget
            .read_only_io(
                "cypher_plan_node_access",
                self.best_row_node_access_with_stats(cell_id, pattern, &BTreeSet::new()),
            )
            .await?;
        match access {
            RowQueryAccess::VertexIdSeek => Ok(pattern.id.map(|id| vec![id])),
            RowQueryAccess::VertexPropertyIndex { property } => {
                let Some(value) = pattern.properties.get(&property) else {
                    return Err(GraphError::CorruptValue {
                        key: format!("cell/{cell_id}/query/node-access/{property}"),
                        reason: "optimizer selected missing vertex property".to_string(),
                    });
                };
                if pattern.properties.len() > 1 {
                    // Statistics can lag a hot scope. Probe every inline
                    // equality within one shared allowance and
                    // use the first complete index, so a stale estimate for a
                    // tenant-wide property cannot hide a selective identity
                    // property and turn a valid lookup into an admission
                    // failure. If every probe is broad, preserve the selected
                    // plan's full allowance rather than multiplying a 250k
                    // scan by the number of properties.
                    let mut properties = Vec::with_capacity(pattern.properties.len());
                    properties.push((property.as_str(), value));
                    properties.extend(
                        pattern
                            .properties
                            .iter()
                            .filter(|(candidate, _)| candidate.as_str() != property)
                            .map(|(candidate, value)| (candidate.as_str(), value)),
                    );
                    // Give each property a fixed slice of one aggregate probe
                    // budget. A cached broad prefix can otherwise consume a
                    // shared atomic allowance before a selective property gets
                    // polled, making the result depend on cache timing.
                    let probe_budget = self.limits.max_query_index_candidates.min(256);
                    let allowance = probe_budget / properties.len();
                    let alternative_allowance =
                        QUERY_NODE_EQUALITY_PROBE_ALTERNATIVES / properties.len();
                    if allowance > 0 && alternative_allowance > 0 {
                        let mut probes = stream::FuturesUnordered::new();
                        for (property, value) in properties {
                            probes.push(async move {
                                let vertices = self
                                    .try_scan_vertex_property_value_at(
                                        cell_id,
                                        property,
                                        value,
                                        budget,
                                        allowance,
                                        alternative_allowance,
                                    )
                                    .await?;
                                Ok::<_, GraphError>((property, vertices))
                            });
                        }
                        while let Some(probe) = probes.next().await {
                            let (winning_property, vertices) = probe?;
                            if let Some(vertices) = vertices {
                                tracing::Span::current().record(
                                    "hydradb.query.runtime_property_index",
                                    winning_property,
                                );
                                return Ok(Some(vertices));
                            }
                        }
                    }
                }
                Ok(Some(
                    self.scan_vertex_property_index_at(
                        cell_id, &property, value, read_epoch, budget,
                    )
                    .await?,
                ))
            }
            RowQueryAccess::VertexLabelScan { label } => Ok(Some(
                self.scan_vertex_label_index_at(cell_id, &label, read_epoch, budget)
                    .await?,
            )),
            RowQueryAccess::AllVertexScan => Ok(None),
            RowQueryAccess::BoundOutExpand { .. }
            | RowQueryAccess::BoundInExpand { .. }
            | RowQueryAccess::ExpandInto { .. }
            | RowQueryAccess::EdgePropertyIndex { .. }
            | RowQueryAccess::FullEdgeScan { .. }
            | RowQueryAccess::VariableLengthExpand { .. } => Err(GraphError::CorruptValue {
                key: format!("cell/{cell_id}/query/node-access"),
                reason: "optimizer selected edge access for node pattern".to_string(),
            }),
        }
    }

    #[cfg(feature = "opencypher")]
    pub(crate) async fn vertex_metadata_at(
        &self,
        cell_id: &str,
        vertex_id: VertexId,
        _read_epoch: StorageSequence,
        budget: &QueryBudget,
    ) -> Result<VertexMetadata> {
        validate_component("cell_id", cell_id)?;
        let key = keys::vertex(cell_id, vertex_id);
        match budget
            .read_only_io("cypher_vertex_metadata", self.read_remote(&key))
            .await?
        {
            Some(value) => decode_vertex_metadata(&key, &value),
            None => Ok(VertexMetadata::default()),
        }
    }

    #[cfg(feature = "opencypher")]
    pub(crate) async fn vertex_metadata_batch_at(
        &self,
        cell_id: &str,
        vertex_ids: &[VertexId],
        read_epoch: StorageSequence,
        budget: &QueryBudget,
    ) -> Result<Vec<(VertexId, VertexMetadata)>> {
        stream::iter(vertex_ids.iter().copied().map(|vertex_id| async move {
            budget.check("cypher_metadata_hydration")?;
            let started = Instant::now();
            let metadata = self
                .vertex_metadata_at(cell_id, vertex_id, read_epoch, budget)
                .instrument(tracing::info_span!(
                    "query.property_fetch",
                    hydradb.cell_id = %cell_id,
                    hydradb.property_owner = "vertex",
                ))
                .await?;
            self.operation_metrics
                .record_property_fetch(started.elapsed().as_micros() as u64);
            Ok::<_, GraphError>((vertex_id, metadata))
        }))
        .buffered(QUERY_METADATA_HYDRATION_CONCURRENCY)
        .boxed()
        .try_collect()
        .await
    }

    #[cfg(feature = "opencypher")]
    pub(crate) async fn hydrate_vertex_metadata_cache_at(
        &self,
        cell_id: &str,
        vertex_ids: &[VertexId],
        read_epoch: StorageSequence,
        cache: &mut BTreeMap<VertexId, VertexMetadata>,
        budget: &QueryBudget,
    ) -> Result<()> {
        let mut seen = BTreeSet::new();
        let pending = vertex_ids
            .iter()
            .copied()
            .filter(|vertex_id| !cache.contains_key(vertex_id) && seen.insert(*vertex_id))
            .collect::<Vec<_>>();
        for (vertex_id, metadata) in self
            .vertex_metadata_batch_at(cell_id, &pending, read_epoch, budget)
            .await?
        {
            cache.insert(vertex_id, metadata);
        }
        Ok(())
    }

    #[cfg(feature = "opencypher")]
    async fn edge_metadata_at(
        &self,
        cell_id: &str,
        edge_type: &str,
        src: VertexId,
        dst: VertexId,
        _read_epoch: StorageSequence,
        budget: &QueryBudget,
    ) -> Result<EdgeMetadata> {
        validate_component("cell_id", cell_id)?;
        validate_component("edge_type", edge_type)?;
        let key = keys::edge_metadata(cell_id, edge_type, src, dst);
        match budget
            .read_only_io("cypher_edge_metadata", self.read_remote(&key))
            .await?
        {
            Some(value) => decode_edge_metadata(&key, &value),
            None => Ok(EdgeMetadata::default()),
        }
    }

    #[cfg(feature = "opencypher")]
    async fn relationship_metadata_at(
        &self,
        cell_id: &str,
        relationship: &BoundRelationship,
        read_epoch: StorageSequence,
        budget: &QueryBudget,
    ) -> Result<EdgeMetadata> {
        let Some(relationship_id) = relationship.relationship_id else {
            return self
                .edge_metadata_at(
                    cell_id,
                    &relationship.edge_type,
                    relationship.src,
                    relationship.dst,
                    read_epoch,
                    budget,
                )
                .await;
        };
        budget.check("cypher_relationship_record")?;
        let key = keys::relationship(
            cell_id,
            &relationship.edge_type,
            relationship.src,
            relationship.dst,
            relationship_id,
        );
        let Some(value) = budget
            .read_only_io("cypher_relationship_record", self.read_remote(&key))
            .await?
        else {
            return Ok(EdgeMetadata::default());
        };
        let record = decode_relationship_record(&key, &value)?;
        Ok(record.metadata)
    }

    #[cfg(feature = "opencypher")]
    async fn relationships_for_edge_at(
        &self,
        cell_id: &str,
        edge_type: &str,
        src: VertexId,
        dst: VertexId,
        read_epoch: StorageSequence,
        budget: &QueryBudget,
    ) -> Result<Vec<(BoundRelationship, EdgeMetadata)>> {
        let cache_key = RelationshipRowsCacheKey::new(cell_id, edge_type, src, dst, read_epoch);
        if let Some(cached) = self.relationship_rows_cache.lock().await.get(&cache_key) {
            self.cache_metrics
                .record_hit(GraphCacheKind::RelationshipRows);
            return Ok(relationship_rows_from_cache_value(
                edge_type, src, dst, cached,
            ));
        }
        self.cache_metrics
            .record_miss(GraphCacheKind::RelationshipRows);

        let current_epoch = self.current_epoch(cell_id).await?;
        let latest_snapshot = read_epoch == current_epoch;
        let structural_exists = if latest_snapshot {
            self.edge_exists(cell_id, edge_type, src, dst).await?
        } else {
            self.edge_exists_at(cell_id, edge_type, src, dst, read_epoch)
                .await?
        };
        if !structural_exists {
            let cache_value = relationship_rows_cache_value(&[]);
            let resident_bytes = cache_key
                .estimated_resident_bytes()
                .saturating_add(cache_value.estimated_resident_bytes());
            self.relationship_rows_cache.lock().await.insert_sized(
                cache_key,
                cache_value,
                cell_id.to_string(),
                false,
                resident_bytes,
                &self.cache_metrics,
            );
            return Ok(Vec::new());
        }

        let mut rows = Vec::new();
        let mut saw_live_relationship_record = false;
        let prefix = keys::relationship_edge_prefix(cell_id, edge_type, src, dst);
        let mut iter = self.scan_remote_prefix(&prefix).await?;
        while let Some(kv) = iter.next().await? {
            budget.check("cypher_relationship_edge_records")?;
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            let record = decode_relationship_record(&key, &kv.value)?;
            saw_live_relationship_record = true;
            let relationship = BoundRelationship {
                edge_type: record.edge_type,
                src: record.src,
                dst: record.dst,
                relationship_id: Some(record.relationship_id),
            };
            let metadata = if latest_snapshot {
                record.metadata
            } else {
                self.relationship_metadata_at(cell_id, &relationship, read_epoch, budget)
                    .await?
            };
            rows.push((relationship, metadata));
            self.ensure_query_index_candidates("cypher_relationship_edge_records", rows.len())?;
        }
        if saw_live_relationship_record {
            let structural_metadata = self
                .edge_metadata_at(cell_id, edge_type, src, dst, read_epoch, budget)
                .await?;
            if !structural_metadata.properties.is_empty() {
                rows.push((
                    BoundRelationship {
                        edge_type: edge_type.to_string(),
                        src,
                        dst,
                        relationship_id: None,
                    },
                    structural_metadata,
                ));
            }
        }
        if rows.is_empty() && !saw_live_relationship_record {
            let metadata = self
                .edge_metadata_at(cell_id, edge_type, src, dst, read_epoch, budget)
                .await?;
            rows.push((
                BoundRelationship {
                    edge_type: edge_type.to_string(),
                    src,
                    dst,
                    relationship_id: None,
                },
                metadata,
            ));
        }
        let cache_value = relationship_rows_cache_value(&rows);
        let resident_bytes = cache_key
            .estimated_resident_bytes()
            .saturating_add(cache_value.estimated_resident_bytes());
        self.relationship_rows_cache.lock().await.insert_sized(
            cache_key,
            cache_value,
            cell_id.to_string(),
            false,
            resident_bytes,
            &self.cache_metrics,
        );
        Ok(rows)
    }

    #[cfg(feature = "opencypher")]
    async fn source_relationship_id_bindings_at(
        &self,
        cell_id: &str,
        edge: &RowEdgePattern,
        src: VertexId,
        read_epoch: StorageSequence,
        budget: &QueryBudget,
    ) -> Result<Vec<BindingRow>> {
        let cached = self
            .source_relationship_dsts_at(cell_id, edge, src, read_epoch, budget)
            .await?;
        source_relationship_id_bindings_from_dsts(edge, src, cached.as_slice(), self, budget)
    }

    #[cfg(feature = "opencypher")]
    async fn source_relationship_dsts_at(
        &self,
        cell_id: &str,
        edge: &RowEdgePattern,
        src: VertexId,
        read_epoch: StorageSequence,
        budget: &QueryBudget,
    ) -> Result<Arc<Vec<VertexId>>> {
        let cache_key =
            SourceRelationshipRowsCacheKey::new(cell_id, &edge.edge_type, src, read_epoch);
        if let Some(cached) = self
            .source_relationship_rows_cache
            .lock()
            .await
            .get(&cache_key)
        {
            self.cache_metrics
                .record_hit(GraphCacheKind::RelationshipRows);
            return Ok(cached);
        }
        self.cache_metrics
            .record_miss(GraphCacheKind::RelationshipRows);

        let prefix = keys::relationship_source_prefix(cell_id, &edge.edge_type, src);
        let mut iter = self.scan_remote_prefix(&prefix).await?;
        let mut dsts = Vec::new();
        let mut relationship_dsts = BTreeSet::new();
        while let Some(kv) = iter.next().await? {
            budget.check("cypher_source_relationship_records")?;
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            let record = decode_relationship_record(&key, &kv.value)?;
            relationship_dsts.insert(record.dst);
            dsts.push(record.dst);
            self.ensure_query_intermediate_rows("cypher_source_relationship_records", dsts.len())?;
        }

        let structural_neighbors = self
            .out_neighbors_at_for_query(cell_id, &edge.edge_type, src, read_epoch, budget)
            .await?;
        for dst in structural_neighbors {
            budget.check("cypher_source_relationship_structural_fallback")?;
            if relationship_dsts.contains(&dst) {
                continue;
            }
            dsts.push(dst);
            self.ensure_query_intermediate_rows(
                "cypher_source_relationship_structural_fallback",
                dsts.len(),
            )?;
        }
        let cached = Arc::new(dsts);
        let resident_bytes = source_relationship_rows_resident_bytes(&cache_key, &cached);
        self.source_relationship_rows_cache
            .lock()
            .await
            .insert_sized(
                cache_key,
                Arc::clone(&cached),
                cell_id.to_string(),
                false,
                resident_bytes,
                &self.cache_metrics,
            );
        Ok(cached)
    }

    #[cfg(feature = "opencypher")]
    async fn relationships_for_edge_property_at(
        &self,
        lookup: RelationshipPropertyLookup<'_>,
    ) -> Result<Vec<(BoundRelationship, EdgeMetadata)>> {
        let RelationshipPropertyLookup {
            cell_id,
            edge_type,
            src,
            dst,
            property,
            value,
            read_epoch,
            budget,
        } = lookup;
        validate_component("cell_id", cell_id)?;
        validate_component("edge_type", edge_type)?;
        validate_component("property", property)?;
        let mut rows = Vec::new();
        let mut seen_relationship_ids = BTreeSet::new();
        let current_epoch = self.current_epoch(cell_id).await?;
        let latest_snapshot = read_epoch == current_epoch;
        let structural_exists = if latest_snapshot {
            self.edge_exists(cell_id, edge_type, src, dst).await?
        } else {
            self.edge_exists_at(cell_id, edge_type, src, dst, read_epoch)
                .await?
        };
        if !structural_exists {
            return Ok(Vec::new());
        }
        for encoded in equivalent_property_index_keys(value) {
            let encoded_rows = self
                .relationships_for_edge_encoded_property_at(EncodedRelationshipPropertyLookup {
                    cell_id,
                    edge_type,
                    src,
                    dst,
                    property,
                    encoded: &encoded,
                    read_epoch,
                    latest_snapshot,
                    budget,
                })
                .await?;
            for (relationship, metadata) in encoded_rows {
                let Some(relationship_id) = relationship.relationship_id else {
                    continue;
                };
                if !seen_relationship_ids.insert(relationship_id) {
                    continue;
                }
                rows.push((relationship, metadata));
                self.ensure_query_index_candidates(
                    "cypher_relationship_property_edge_candidates",
                    rows.len(),
                )?;
            }
        }

        let structural_metadata = self
            .edge_metadata_at(cell_id, edge_type, src, dst, read_epoch, budget)
            .await?;
        if structural_metadata
            .properties
            .get(property)
            .is_some_and(|existing| vertex_property_values_equal(existing, value))
        {
            rows.push((
                BoundRelationship {
                    edge_type: edge_type.to_string(),
                    src,
                    dst,
                    relationship_id: None,
                },
                structural_metadata,
            ));
        }
        Ok(rows)
    }

    #[cfg(feature = "opencypher")]
    async fn relationships_for_edge_encoded_property_at(
        &self,
        lookup: EncodedRelationshipPropertyLookup<'_>,
    ) -> Result<Vec<(BoundRelationship, EdgeMetadata)>> {
        let EncodedRelationshipPropertyLookup {
            cell_id,
            edge_type,
            src,
            dst,
            property,
            encoded,
            read_epoch,
            latest_snapshot,
            budget,
        } = lookup;
        let cache_key = RelationshipPropertyRowsCacheKey::new(
            cell_id, edge_type, src, dst, property, encoded, read_epoch,
        );
        if let Some(cached) = self
            .relationship_property_rows_cache
            .lock()
            .await
            .get(&cache_key)
        {
            self.cache_metrics
                .record_hit(GraphCacheKind::RelationshipPropertyRows);
            return Ok(relationship_rows_from_cache_value(
                edge_type, src, dst, cached,
            ));
        }
        self.cache_metrics
            .record_miss(GraphCacheKind::RelationshipPropertyRows);

        let relationship_ids = self
            .scan_relationship_property_index_current_for_edge(
                RelationshipPropertyEdgeIndexLookup {
                    cell_id,
                    edge_type,
                    property,
                    encoded,
                    src,
                    dst,
                    budget,
                },
            )
            .await?;

        let mut rows = Vec::with_capacity(relationship_ids.len());
        for relationship_id in relationship_ids {
            budget.check("cypher_relationship_property_edge_candidates")?;
            let relationship = BoundRelationship {
                edge_type: edge_type.to_string(),
                src,
                dst,
                relationship_id: Some(relationship_id),
            };
            let key = keys::relationship(cell_id, edge_type, src, dst, relationship_id);
            let Some(value) = self.read_remote(&key).await? else {
                continue;
            };
            let record = decode_relationship_record(&key, &value)?;
            let metadata = if latest_snapshot {
                record.metadata
            } else {
                self.relationship_metadata_at(cell_id, &relationship, read_epoch, budget)
                    .await?
            };
            rows.push((relationship, metadata));
            self.ensure_query_index_candidates(
                "cypher_relationship_property_edge_candidates",
                rows.len(),
            )?;
        }
        let cache_value = relationship_rows_cache_value(&rows);
        let resident_bytes = cache_key
            .estimated_resident_bytes()
            .saturating_add(cache_value.estimated_resident_bytes());
        self.relationship_property_rows_cache
            .lock()
            .await
            .insert_sized(
                cache_key,
                cache_value,
                cell_id.to_string(),
                false,
                resident_bytes,
                &self.cache_metrics,
            );
        Ok(rows)
    }

    #[cfg(feature = "opencypher")]
    async fn relationship_count_for_edge_at(
        &self,
        cell_id: &str,
        edge_type: &str,
        src: VertexId,
        dst: VertexId,
        read_epoch: StorageSequence,
        budget: &QueryBudget,
    ) -> Result<u64> {
        validate_component("cell_id", cell_id)?;
        validate_component("edge_type", edge_type)?;
        let current_epoch = self.current_epoch(cell_id).await?;
        if read_epoch == current_epoch {
            let count = self
                .read_counter(&keys::relationship_count(cell_id, edge_type, src, dst))
                .await?;
            if count > 0 {
                return Ok(count);
            }
        }

        let relationship_records = self
            .relationship_record_count_for_edge_at(cell_id, edge_type, src, dst, read_epoch, budget)
            .await?;
        if relationship_records > 0 {
            return Ok(relationship_records);
        }

        let structural_exists = if read_epoch == current_epoch {
            self.edge_exists(cell_id, edge_type, src, dst).await?
        } else {
            self.edge_exists_at(cell_id, edge_type, src, dst, read_epoch)
                .await?
        };
        Ok(u64::from(structural_exists))
    }

    #[cfg(feature = "opencypher")]
    async fn relationship_record_count_for_edge_at(
        &self,
        cell_id: &str,
        edge_type: &str,
        src: VertexId,
        dst: VertexId,
        _read_epoch: StorageSequence,
        budget: &QueryBudget,
    ) -> Result<u64> {
        let prefix = keys::relationship_edge_prefix(cell_id, edge_type, src, dst);
        let mut iter = self.scan_remote_prefix(&prefix).await?;
        let mut count = 0_u64;
        while let Some(kv) = iter.next().await? {
            budget.check("cypher_relationship_count_records")?;
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            decode_relationship_record(&key, &kv.value)?;
            count = count.saturating_add(1);
        }
        Ok(count)
    }

    #[cfg(feature = "opencypher")]
    pub(super) async fn scan_edge_property_index_at(
        &self,
        cell_id: &str,
        edge_type: &str,
        property: &str,
        value: &VertexPropertyValue,
        _read_epoch: StorageSequence,
        budget: &QueryBudget,
    ) -> Result<Vec<(VertexId, VertexId)>> {
        validate_component("cell_id", cell_id)?;
        validate_component("edge_type", edge_type)?;
        validate_component("property", property)?;
        let mut edges = BTreeSet::<(VertexId, VertexId)>::new();
        for encoded in equivalent_property_index_keys(value) {
            for edge in self
                .scan_edge_property_index_current(cell_id, edge_type, property, &encoded, budget)
                .await?
            {
                edges.insert(edge);
                self.ensure_query_index_candidates(
                    "cypher_edge_property_index_candidates",
                    edges.len(),
                )?;
            }
            for (src, dst, _relationship_id) in self
                .scan_relationship_property_index_current(
                    cell_id, edge_type, property, &encoded, budget,
                )
                .await?
            {
                edges.insert((src, dst));
                self.ensure_query_index_candidates(
                    "cypher_edge_property_index_candidates",
                    edges.len(),
                )?;
            }
        }
        Ok(edges.into_iter().collect())
    }

    #[cfg(feature = "opencypher")]
    async fn try_scan_edge_property_values_at(
        &self,
        cell_id: &str,
        edge_type: &str,
        property: &str,
        values: &[VertexPropertyValue],
        budget: &QueryBudget,
        candidate_limit: usize,
    ) -> Result<Option<Vec<(VertexId, VertexId)>>> {
        validate_component("cell_id", cell_id)?;
        validate_component("edge_type", edge_type)?;
        validate_component("property", property)?;

        let mut edges = BTreeSet::<(VertexId, VertexId)>::new();
        let mut scanned_entries = 0_usize;
        for value in values {
            for encoded in equivalent_property_index_keys(value) {
                let mut edge_iter = self
                    .scan_remote_prefix(&keys::edge_property_index_prefix(
                        cell_id, edge_type, property, &encoded,
                    ))
                    .await?;
                while let Some(kv) = edge_iter.next().await? {
                    budget.check("cypher_edge_property_seed_probe")?;
                    scanned_entries = scanned_entries.saturating_add(1);
                    if scanned_entries > candidate_limit {
                        return Ok(None);
                    }
                    let key = String::from_utf8_lossy(&kv.key).into_owned();
                    let (_cell_id, _edge_type, _property, _encoded, src, dst) =
                        parse_edge_property_index_key(&key)?;
                    edges.insert((src, dst));
                    if edges.len() > candidate_limit {
                        return Ok(None);
                    }
                }

                let mut relationship_iter = self
                    .scan_remote_prefix(&keys::relationship_property_index_prefix(
                        cell_id, edge_type, property, &encoded,
                    ))
                    .await?;
                while let Some(kv) = relationship_iter.next().await? {
                    budget.check("cypher_relationship_property_seed_probe")?;
                    scanned_entries = scanned_entries.saturating_add(1);
                    if scanned_entries > candidate_limit {
                        return Ok(None);
                    }
                    let key = String::from_utf8_lossy(&kv.key).into_owned();
                    let (_cell_id, _edge_type, _property, _encoded, src, dst, _relationship_id) =
                        parse_relationship_property_index_key(&key)?;
                    edges.insert((src, dst));
                    if edges.len() > candidate_limit {
                        return Ok(None);
                    }
                }
            }
        }
        Ok(Some(edges.into_iter().collect()))
    }

    #[cfg(feature = "opencypher")]
    async fn scan_edge_property_index_current(
        &self,
        cell_id: &str,
        edge_type: &str,
        property: &str,
        encoded: &str,
        budget: &QueryBudget,
    ) -> Result<Vec<(VertexId, VertexId)>> {
        let mut iter = budget
            .read_only_io(
                "cypher_edge_property_index_open",
                self.scan_remote_prefix(&keys::edge_property_index_prefix(
                    cell_id, edge_type, property, encoded,
                )),
            )
            .await?;
        let mut edges = Vec::new();
        while let Some(kv) = budget
            .read_only_io("cypher_edge_property_index_read", async {
                Ok(iter.next().await?)
            })
            .await?
        {
            budget.check("cypher_edge_property_index")?;
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            let (_cell_id, _edge_type, _property, _encoded, src, dst) =
                parse_edge_property_index_key(&key)?;
            edges.push((src, dst));
            self.ensure_query_index_candidates(
                "cypher_edge_property_index_candidates",
                edges.len(),
            )?;
        }
        Ok(edges)
    }

    #[cfg(feature = "opencypher")]
    async fn scan_relationship_property_index_current(
        &self,
        cell_id: &str,
        edge_type: &str,
        property: &str,
        encoded: &str,
        budget: &QueryBudget,
    ) -> Result<Vec<(VertexId, VertexId, RelationshipId)>> {
        let mut iter = budget
            .read_only_io(
                "cypher_relationship_property_index_open",
                self.scan_remote_prefix(&keys::relationship_property_index_prefix(
                    cell_id, edge_type, property, encoded,
                )),
            )
            .await?;
        let mut relationships = Vec::new();
        while let Some(kv) = budget
            .read_only_io("cypher_relationship_property_index_read", async {
                Ok(iter.next().await?)
            })
            .await?
        {
            budget.check("cypher_relationship_property_index")?;
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            let (_cell_id, _edge_type, _property, _encoded, src, dst, relationship_id) =
                parse_relationship_property_index_key(&key)?;
            relationships.push((src, dst, relationship_id));
            self.ensure_query_index_candidates(
                "cypher_relationship_property_index_candidates",
                relationships.len(),
            )?;
        }
        Ok(relationships)
    }

    #[cfg(feature = "opencypher")]
    async fn scan_relationship_property_index_current_for_edge(
        &self,
        lookup: RelationshipPropertyEdgeIndexLookup<'_>,
    ) -> Result<Vec<RelationshipId>> {
        let RelationshipPropertyEdgeIndexLookup {
            cell_id,
            edge_type,
            property,
            encoded,
            src,
            dst,
            budget,
        } = lookup;
        let mut iter = budget
            .read_only_io(
                "cypher_relationship_property_index_edge_open",
                self.scan_remote_prefix(&keys::relationship_property_index_edge_prefix(
                    cell_id, edge_type, property, encoded, src, dst,
                )),
            )
            .await?;
        let mut relationships = Vec::new();
        while let Some(kv) = budget
            .read_only_io("cypher_relationship_property_index_edge_read", async {
                Ok(iter.next().await?)
            })
            .await?
        {
            budget.check("cypher_relationship_property_index_edge")?;
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            let (
                _cell_id,
                _edge_type,
                _property,
                _encoded,
                parsed_src,
                parsed_dst,
                relationship_id,
            ) = parse_relationship_property_index_key(&key)?;
            if parsed_src != src || parsed_dst != dst {
                return Err(GraphError::CorruptValue {
                    key,
                    reason: "relationship property index edge prefix returned a different endpoint"
                        .to_string(),
                });
            }
            relationships.push(relationship_id);
            self.ensure_query_index_candidates(
                "cypher_relationship_property_index_edge_candidates",
                relationships.len(),
            )?;
        }
        Ok(relationships)
    }

    #[cfg(feature = "opencypher")]
    pub(super) async fn scan_vertex_label_index_at(
        &self,
        cell_id: &str,
        label: &str,
        _read_epoch: StorageSequence,
        budget: &QueryBudget,
    ) -> Result<Vec<VertexId>> {
        self.scan_vertex_label_index_at_limited(cell_id, label, _read_epoch, budget, None)
            .await
    }

    #[cfg(feature = "experimental-cypher-engine")]
    pub(super) async fn scan_vertex_ids_at(
        &self,
        snapshot: &GraphStorageSnapshot,
        cell_id: &str,
        _read_epoch: StorageSequence,
        budget: &QueryBudget,
    ) -> Result<Vec<VertexId>> {
        validate_component("cell_id", cell_id)?;
        let prefix = keys::vertex_prefix(cell_id);
        let mut iter = snapshot
            .scan_prefix_with_options(prefix.as_bytes(), .., &remote_scan_options())
            .await?;
        let mut vertices = Vec::new();
        while let Some(kv) = iter.next().await? {
            budget.check("cypher_vertex_scan")?;
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            let vertex_id = key
                .strip_prefix(&prefix)
                .and_then(|value| value.parse::<VertexId>().ok())
                .ok_or_else(|| GraphError::CorruptValue {
                    key: key.clone(),
                    reason: "vertex key has an invalid id suffix".to_string(),
                })?;
            vertices.push(vertex_id);
            self.ensure_query_index_candidates("cypher_vertex_scan_candidates", vertices.len())?;
        }
        Ok(vertices)
    }

    #[cfg(feature = "opencypher")]
    pub(super) async fn scan_vertex_label_index_at_limited(
        &self,
        cell_id: &str,
        label: &str,
        _read_epoch: StorageSequence,
        budget: &QueryBudget,
        max_results: Option<usize>,
    ) -> Result<Vec<VertexId>> {
        validate_component("cell_id", cell_id)?;
        validate_component("label", label)?;
        self.scan_vertex_label_index_current(cell_id, label, budget, max_results)
            .await
    }

    #[cfg(feature = "opencypher")]
    async fn scan_vertex_label_index_current(
        &self,
        cell_id: &str,
        label: &str,
        budget: &QueryBudget,
        max_results: Option<usize>,
    ) -> Result<Vec<VertexId>> {
        let mut iter = budget
            .read_only_io(
                "cypher_vertex_label_index_open",
                self.scan_remote_prefix(&keys::vertex_label_prefix(cell_id, label)),
            )
            .await?;
        let mut vertices = Vec::new();
        while let Some(kv) = budget
            .read_only_io("cypher_vertex_label_index_read", async {
                Ok(iter.next().await?)
            })
            .await?
        {
            budget.check("cypher_vertex_label_index")?;
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            vertices.push(decode_u64(&key, &kv.value)?);
            self.ensure_query_index_candidates(
                "cypher_vertex_label_index_candidates",
                vertices.len(),
            )?;
            if max_results.is_some_and(|limit| vertices.len() >= limit) {
                break;
            }
        }
        Ok(vertices)
    }

    #[cfg(feature = "opencypher")]
    pub(super) async fn scan_vertex_property_index_at(
        &self,
        cell_id: &str,
        property: &str,
        value: &VertexPropertyValue,
        _read_epoch: StorageSequence,
        budget: &QueryBudget,
    ) -> Result<Vec<VertexId>> {
        validate_component("cell_id", cell_id)?;
        validate_component("property", property)?;
        let mut vertices = BTreeSet::<VertexId>::new();
        for encoded in equivalent_property_index_keys(value) {
            for vertex_id in self
                .scan_vertex_property_index_current(cell_id, property, &encoded, budget)
                .await?
            {
                vertices.insert(vertex_id);
                self.ensure_query_index_candidates(
                    "cypher_vertex_property_index_candidates",
                    vertices.len(),
                )?;
            }
        }
        Ok(vertices.into_iter().collect())
    }

    #[cfg(feature = "opencypher")]
    pub(crate) async fn try_scan_vertex_property_value_at(
        &self,
        cell_id: &str,
        property: &str,
        value: &VertexPropertyValue,
        budget: &QueryBudget,
        mut probe_allowance: usize,
        alternative_allowance: usize,
    ) -> Result<Option<Vec<VertexId>>> {
        validate_component("cell_id", cell_id)?;
        validate_component("property", property)?;

        let mut vertices = BTreeSet::new();
        let mut encodings = BTreeSet::new();
        for encoded in equivalent_property_index_keys(value) {
            if !encodings.insert(encoded.clone()) {
                continue;
            }
            if encodings.len() > alternative_allowance {
                return Ok(None);
            }
            let mut iter = budget
                .read_only_io(
                    "cypher_vertex_property_seed_probe_open",
                    self.scan_remote_prefix(&keys::vertex_property_index_prefix(
                        cell_id, property, &encoded,
                    )),
                )
                .await?;
            loop {
                let Some(kv) = budget
                    .read_only_io("cypher_vertex_property_seed_probe_read", async {
                        Ok(iter.next().await?)
                    })
                    .await?
                else {
                    break;
                };
                let Some(remaining) = probe_allowance.checked_sub(1) else {
                    return Ok(None);
                };
                probe_allowance = remaining;
                budget.check("cypher_vertex_property_seed_probe")?;
                let key = String::from_utf8_lossy(&kv.key).into_owned();
                vertices.insert(decode_u64(&key, &kv.value)?);
            }
        }
        Ok(Some(vertices.into_iter().collect()))
    }

    #[cfg(feature = "experimental-cypher-engine")]
    pub(super) async fn scan_vertex_property_values_at(
        &self,
        cell_id: &str,
        property: &str,
        _read_epoch: StorageSequence,
        budget: &QueryBudget,
    ) -> Result<Vec<VertexId>> {
        validate_component("cell_id", cell_id)?;
        validate_component("property", property)?;
        let mut iter = self
            .scan_remote_prefix(&keys::vertex_property_index_property_prefix(
                cell_id, property,
            ))
            .await?;
        let mut vertices = BTreeSet::new();
        while let Some(kv) = iter.next().await? {
            budget.check("cypher_vertex_property_values")?;
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            let (_cell_id, indexed_property, _encoded, vertex_id) =
                parse_vertex_property_index_key(&key)?;
            if indexed_property != property {
                continue;
            }
            vertices.insert(vertex_id);
            self.ensure_query_index_candidates(
                "cypher_vertex_property_value_candidates",
                vertices.len(),
            )?;
        }
        Ok(vertices.into_iter().collect())
    }

    #[cfg(feature = "opencypher")]
    async fn scan_vertex_property_index_current(
        &self,
        cell_id: &str,
        property: &str,
        encoded: &str,
        budget: &QueryBudget,
    ) -> Result<Vec<VertexId>> {
        let mut iter = budget
            .read_only_io(
                "cypher_vertex_property_index_open",
                self.scan_remote_prefix(&keys::vertex_property_index_prefix(
                    cell_id, property, encoded,
                )),
            )
            .await?;
        let mut vertices = Vec::new();
        while let Some(kv) = budget
            .read_only_io("cypher_vertex_property_index_read", async {
                Ok(iter.next().await?)
            })
            .await?
        {
            budget.check("cypher_vertex_property_index")?;
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            vertices.push(decode_u64(&key, &kv.value)?);
            self.ensure_query_index_candidates(
                "cypher_vertex_property_index_candidates",
                vertices.len(),
            )?;
        }
        Ok(vertices)
    }

    #[cfg(feature = "opencypher")]
    async fn hydrate_binding_metadata(
        &self,
        cell_id: &str,
        read_epoch: StorageSequence,
        row: &mut BindingRow,
        cache: &mut BTreeMap<VertexId, VertexMetadata>,
        budget: &QueryBudget,
    ) -> Result<()> {
        let bindings: Vec<_> = row
            .values
            .iter()
            .map(|(name, id)| (name.clone(), *id))
            .collect();
        for (binding, vertex_id) in bindings {
            budget.check("cypher_metadata_hydration")?;
            let metadata = match cache.get(&vertex_id) {
                Some(metadata) => metadata.clone(),
                None => {
                    let started = Instant::now();
                    // Spanned on the miss for the same reason it is timed there:
                    // the span count is then the number of storage reads, not the
                    // number of rows. A thousand rows over twenty distinct
                    // vertices emits twenty spans, and the trace shows the fetches
                    // rather than the iteration.
                    let metadata = self
                        .vertex_metadata_at(cell_id, vertex_id, read_epoch, budget)
                        .instrument(tracing::info_span!(
                            "query.property_fetch",
                            hydradb.cell_id = %cell_id,
                            hydradb.property_owner = "vertex",
                        ))
                        .await?;
                    self.operation_metrics
                        .record_property_fetch(started.elapsed().as_micros() as u64);
                    cache.insert(vertex_id, metadata.clone());
                    metadata
                }
            };
            row.metadata.insert(binding, metadata);
        }
        Ok(())
    }

    #[cfg(feature = "opencypher")]
    async fn hydrate_row_relationship_metadata(
        &self,
        cell_id: &str,
        read_epoch: StorageSequence,
        row: &mut BindingRow,
        pattern: &RowEdgePattern,
        cache: &mut BTreeMap<BoundRelationship, EdgeMetadata>,
        budget: &QueryBudget,
    ) -> Result<()> {
        if pattern.binding.is_none() && pattern.properties.is_empty() {
            return Ok(());
        }
        let relationship = relationship_identity_for_pattern(row, pattern)?;
        budget.check("cypher_relationship_metadata_hydration")?;
        let metadata = match cache.get(&relationship) {
            Some(metadata) => metadata.clone(),
            None => {
                let metadata = match row.relationship_metadata.get(&relationship) {
                    Some(metadata) => metadata.clone(),
                    None => {
                        let started = Instant::now();
                        let metadata = self
                            .relationship_metadata_at(cell_id, &relationship, read_epoch, budget)
                            .instrument(tracing::info_span!(
                                "query.property_fetch",
                                hydradb.cell_id = %cell_id,
                                hydradb.property_owner = "relationship",
                            ))
                            .await?;
                        self.operation_metrics
                            .record_property_fetch(started.elapsed().as_micros() as u64);
                        metadata
                    }
                };
                cache.insert(relationship.clone(), metadata.clone());
                metadata
            }
        };
        row.relationship_metadata.insert(relationship, metadata);
        Ok(())
    }

    #[cfg(feature = "opencypher")]
    fn finish_projected_rows(
        &self,
        columns: Vec<QueryColumn>,
        mut projected: Vec<ProjectedQueryRow>,
        order_by: &[RowSort],
        distinct: bool,
        window: QueryWindow,
        budget: &QueryBudget,
    ) -> Result<QueryResultSet> {
        budget.check("cypher_finish_rows")?;
        if distinct {
            let mut deduped = Vec::with_capacity(projected.len());
            let mut seen = BTreeSet::new();
            for row in projected {
                budget.check("cypher_distinct_rows")?;
                if seen.insert(row.row.values.clone()) {
                    deduped.push(row);
                }
            }
            projected = deduped;
            self.ensure_query_intermediate_rows("cypher_distinct_rows", projected.len())?;
        }
        if !order_by.is_empty() {
            projected.sort_by(|left, right| compare_projected_rows(left, right, order_by));
        }
        budget.check("cypher_sort_rows")?;

        let skip = usize::try_from(window.skip).map_err(|_| GraphError::AdmissionRejected {
            operation: "query_result_skip",
            actual: window.skip,
            limit: usize::MAX as u64,
        })?;
        let max = self.limits.max_query_intermediate_rows;
        let mut rows: Vec<_> = projected
            .into_iter()
            .skip(skip)
            .map(|projected| projected.row)
            .collect();
        if let Some(limit) = window.limit {
            ensure_limit("query_result_limit", limit as u64, max as u64)?;
            rows.truncate(limit);
        } else {
            ensure_limit("query_result_rows", rows.len() as u64, max as u64)?;
        }
        Ok(QueryResultSet::new(columns, rows))
    }

    async fn out_neighbors_window_at(
        &self,
        cell_id: &str,
        edge_type: &str,
        src: VertexId,
        read_epoch: StorageSequence,
        window: QueryWindow,
        budget: Option<&QueryBudget>,
    ) -> Result<Vec<VertexId>> {
        check_optional_query_budget(budget, "query_out_neighbors_window")?;
        let vertices = match budget {
            Some(budget) => {
                self.out_neighbors_at_for_query(cell_id, edge_type, src, read_epoch, budget)
                    .await?
            }
            None => {
                self.out_neighbors_at(cell_id, edge_type, src, read_epoch)
                    .await?
            }
        };
        self.apply_query_window(vertices, window)
    }

    fn apply_query_window(
        &self,
        vertices: Vec<VertexId>,
        window: QueryWindow,
    ) -> Result<Vec<VertexId>> {
        let skip = usize::try_from(window.skip).map_err(|_| GraphError::AdmissionRejected {
            operation: "query_result_skip",
            actual: window.skip,
            limit: usize::MAX as u64,
        })?;
        let windowed: Vec<_> = vertices.into_iter().skip(skip).collect();
        self.apply_query_window_fetch_result(windowed, window)
    }

    fn apply_query_window_fetch_result(
        &self,
        mut vertices: Vec<VertexId>,
        window: QueryWindow,
    ) -> Result<Vec<VertexId>> {
        let max = self.limits.max_query_result_vertices;
        if let Some(limit) = window.limit {
            ensure_limit("query_result_limit", limit as u64, max as u64)?;
            vertices.truncate(limit);
        } else {
            ensure_limit("query_result_vertices", vertices.len() as u64, max as u64)?;
        }
        Ok(vertices)
    }

    #[cfg(feature = "opencypher")]
    fn apply_query_row_window(
        &self,
        rows: Vec<QueryRow>,
        window: QueryWindow,
    ) -> Result<Vec<QueryRow>> {
        let skip = usize::try_from(window.skip).map_err(|_| GraphError::AdmissionRejected {
            operation: "query_result_skip",
            actual: window.skip,
            limit: usize::MAX as u64,
        })?;
        let mut rows: Vec<_> = rows.into_iter().skip(skip).collect();
        let max = self.limits.max_query_intermediate_rows;
        if let Some(limit) = window.limit {
            ensure_limit("query_result_limit", limit as u64, max as u64)?;
            rows.truncate(limit);
        } else {
            ensure_limit("query_result_rows", rows.len() as u64, max as u64)?;
        }
        Ok(rows)
    }

    #[cfg(feature = "opencypher")]
    async fn try_execute_graph_kernel_opencypher_rows_page(
        &self,
        context: &QueryContext,
        query: &ParsedRowQuery,
        cursor_offset: u64,
        page_size: usize,
    ) -> Result<Option<QueryResultPage>> {
        let Some(request) = graph_kernel_row_query_request(query) else {
            return Ok(None);
        };
        let page_context = self.query_page_context(context.clone(), cursor_offset, page_size)?;
        let page_window = page_context.result_window;
        if page_window.limit == Some(0) {
            return Ok(Some(QueryResultPage::new(
                query.columns.clone(),
                Vec::new(),
                None,
            )));
        }

        let read_epoch = self.query_read_epoch(context).await?;
        let budget = QueryBudget::new(
            context.max_runtime_ms.or(self.limits.max_query_runtime_ms),
            context.cancellation_token.clone(),
        );
        budget.check("cypher_graph_kernel_page")?;
        if request.edge.dst.id.is_none() {
            match request.projection {
                GraphKernelProjection::NodeId => {
                    let (mut vertices, edge_visits) = Box::pin(
                        self.reachable_vertices_window_in_hop_range_at(ReachableWindowRequest {
                            cell_id: &context.cell_id,
                            edge_type: &request.edge.edge_type,
                            src: request.src,
                            hop_range: request.hop_range,
                            read_epoch,
                            window: page_window,
                            ascending: request.ascending,
                            budget: &budget,
                        }),
                    )
                    .await?;
                    self.ensure_query_scan_edges(
                        "cypher_graph_kernel_page_window_edge_visits",
                        edge_visits,
                    )?;
                    let has_next = vertices.len() > page_size;
                    vertices.truncate(page_size);
                    let rows = graph_kernel_node_id_rows(vertices, &budget)?;
                    return Ok(Some(QueryResultPage::new(
                        query.columns.clone(),
                        rows,
                        query_next_cursor(cursor_offset, page_size, has_next)?,
                    )));
                }
                GraphKernelProjection::CountAll => {
                    let (count, edge_visits) = self
                        .reachable_count_in_hop_range_at(
                            &context.cell_id,
                            &request.edge.edge_type,
                            request.src,
                            request.hop_range,
                            read_epoch,
                            &budget,
                        )
                        .await?;
                    self.ensure_query_scan_edges(
                        "cypher_graph_kernel_page_count_edge_visits",
                        edge_visits,
                    )?;
                    let rows = vec![QueryRow::new(vec![QueryValue::Count(count)])];
                    let mut rows = self.apply_query_row_window(rows, page_window)?;
                    let has_next = rows.len() > page_size;
                    rows.truncate(page_size);
                    return Ok(Some(QueryResultPage::new(
                        query.columns.clone(),
                        rows,
                        query_next_cursor(cursor_offset, page_size, has_next)?,
                    )));
                }
            }
        }

        let (mut vertices, edge_visits) = self
            .reachable_vertices_in_hop_range_at(
                &context.cell_id,
                &request.edge.edge_type,
                request.src,
                request.hop_range,
                read_epoch,
                &budget,
            )
            .await?;
        self.ensure_query_scan_edges("cypher_graph_kernel_page_edge_visits", edge_visits)?;
        if let Some(dst) = request.edge.dst.id {
            vertices.retain(|vertex| *vertex == dst);
        }
        graph_kernel_order_vertices(&mut vertices, request.ascending);

        let rows = match request.projection {
            GraphKernelProjection::NodeId => {
                let mut vertices = self.apply_query_window(vertices, page_window)?;
                let has_next = vertices.len() > page_size;
                vertices.truncate(page_size);
                let mut rows = Vec::with_capacity(vertices.len());
                for vertex in vertices {
                    budget.check("cypher_graph_kernel_page_project")?;
                    rows.push(QueryRow::new(vec![QueryValue::VertexId(vertex)]));
                }
                return Ok(Some(QueryResultPage::new(
                    query.columns.clone(),
                    rows,
                    query_next_cursor(cursor_offset, page_size, has_next)?,
                )));
            }
            GraphKernelProjection::CountAll => {
                let rows = vec![QueryRow::new(vec![
                    QueryValue::Count(vertices.len() as u64),
                ])];
                self.apply_query_row_window(rows, page_window)?
            }
        };
        let has_next = rows.len() > page_size;
        let mut rows = rows;
        rows.truncate(page_size);
        Ok(Some(QueryResultPage::new(
            query.columns.clone(),
            rows,
            query_next_cursor(cursor_offset, page_size, has_next)?,
        )))
    }

    #[cfg(feature = "opencypher")]
    async fn try_execute_streaming_opencypher_rows_page(
        &self,
        context: &QueryContext,
        query: &ParsedRowQuery,
        cursor_offset: u64,
        page_size: usize,
    ) -> Result<Option<QueryResultPage>> {
        let Some(edge) = streaming_neighbor_page_edge(query) else {
            return Ok(None);
        };
        if !streaming_neighbor_order_supported(
            edge,
            &query.projections,
            &query.columns,
            &query.order_by,
        ) {
            return Ok(None);
        }

        let page_context = self.query_page_context(context.clone(), cursor_offset, page_size)?;
        let page_window = page_context.result_window;
        if page_window.limit == Some(0) {
            return Ok(Some(QueryResultPage::new(
                query.columns.clone(),
                Vec::new(),
                None,
            )));
        }

        let read_epoch = self.query_read_epoch(context).await?;
        let budget = QueryBudget::new(
            context.max_runtime_ms.or(self.limits.max_query_runtime_ms),
            context.cancellation_token.clone(),
        );
        budget.check("cypher_rows_page_stream")?;
        let Some(src) = edge.src.id else {
            return Ok(None);
        };
        let mut vertices = self
            .out_neighbors_window_at(
                &context.cell_id,
                &edge.edge_type,
                src,
                read_epoch,
                page_window,
                Some(&budget),
            )
            .await?;
        let has_next = vertices.len() > page_size;
        vertices.truncate(page_size);

        let mut rows = Vec::with_capacity(vertices.len());
        for dst in vertices {
            budget.check("cypher_rows_page_stream_project")?;
            rows.push(QueryRow::new(streaming_neighbor_projection_values(
                edge,
                src,
                dst,
                &query.projections,
            )?));
        }
        let next_cursor = if has_next {
            query_next_cursor(cursor_offset, page_size, true)?
        } else {
            None
        };
        Ok(Some(QueryResultPage::new(
            query.columns.clone(),
            rows,
            next_cursor,
        )))
    }

    #[cfg(feature = "opencypher")]
    fn record_streaming_query_rows_success(&self, row_count: usize, started: std::time::Instant) {
        self.operation_metrics
            .query_rows_started
            .fetch_add(1, Ordering::Relaxed);
        self.operation_metrics
            .query_rows_completed
            .fetch_add(1, Ordering::Relaxed);
        self.operation_metrics
            .query_rows_returned
            .fetch_add(row_count as u64, Ordering::Relaxed);
        self.operation_metrics
            .query_rows_latency
            .record(started.elapsed());
    }

    #[cfg(feature = "opencypher")]
    fn record_streaming_query_rows_failure(&self, started: std::time::Instant, error: &GraphError) {
        self.operation_metrics
            .query_rows_started
            .fetch_add(1, Ordering::Relaxed);
        self.operation_metrics.record_query_rows_failure(error);
        self.operation_metrics
            .query_rows_latency
            .record(started.elapsed());
    }

    #[cfg(feature = "opencypher")]
    fn query_page_context(
        &self,
        context: QueryContext,
        cursor_offset: u64,
        page_size: usize,
    ) -> Result<QueryContext> {
        let max = self.limits.max_query_intermediate_rows;
        if page_size == 0 {
            return Err(GraphError::AdmissionRejected {
                operation: "query_page_size",
                actual: 0,
                limit: max as u64,
            });
        }
        let max_page_size = max.saturating_sub(1);
        ensure_limit("query_page_size", page_size as u64, max_page_size as u64)?;

        let base_window = context.result_window;
        let skip =
            base_window
                .skip
                .checked_add(cursor_offset)
                .ok_or(GraphError::AdmissionRejected {
                    operation: "query_cursor_offset",
                    actual: u64::MAX,
                    limit: u64::MAX - 1,
                })?;
        let probe_limit = match base_window.limit {
            Some(limit) => {
                let limit_u64 = u64::try_from(limit).unwrap_or(u64::MAX);
                if cursor_offset >= limit_u64 {
                    0
                } else {
                    let remaining = limit_u64 - cursor_offset;
                    usize::try_from(remaining)
                        .unwrap_or(usize::MAX)
                        .min(page_size.saturating_add(1))
                }
            }
            None => page_size.saturating_add(1),
        };
        Ok(context.with_result_window(skip, Some(probe_limit)))
    }

    #[cfg(feature = "opencypher")]
    fn ensure_query_intermediate_rows(&self, operation: &'static str, rows: usize) -> Result<()> {
        ensure_limit(
            operation,
            rows as u64,
            self.limits.max_query_intermediate_rows as u64,
        )
    }

    #[cfg(feature = "opencypher")]
    pub(super) fn ensure_query_index_candidates(
        &self,
        operation: &'static str,
        candidates: usize,
    ) -> Result<()> {
        ensure_limit(
            operation,
            candidates as u64,
            self.limits.max_query_index_candidates as u64,
        )
    }

    /// Charge index entries to the query's shared tally and fail once the whole
    /// query has read more of an index than `max_query_index_candidates`
    /// allows.
    ///
    /// The count above is per call, which is the same thing wherever one scan
    /// answers one query. A paged walk asks once per page, so its accounting
    /// has to live on the budget its pages share or it bounds nothing.
    #[cfg(feature = "experimental-cypher-engine")]
    fn charge_query_index_candidates(
        &self,
        operation: &'static str,
        budget: &QueryBudget,
        entries: u64,
    ) -> Result<()> {
        ensure_limit(
            operation,
            budget.add_scanned_index_candidates(entries),
            self.limits.max_query_index_candidates as u64,
        )
    }

    #[cfg(feature = "opencypher")]
    fn ensure_query_scan_edges(&self, operation: &'static str, edges: u64) -> Result<()> {
        ensure_limit(operation, edges, self.limits.max_query_scan_edges)
    }

    #[cfg(feature = "opencypher")]
    fn push_binding_row(
        &self,
        rows: &mut Vec<BindingRow>,
        row: BindingRow,
        operation: &'static str,
    ) -> Result<()> {
        let next_len = rows
            .len()
            .checked_add(1)
            .ok_or_else(|| GraphError::AdmissionRejected {
                operation,
                actual: u64::MAX,
                limit: self.limits.max_query_intermediate_rows as u64,
            })?;
        self.ensure_query_intermediate_rows(operation, next_len)?;
        rows.push(row);
        Ok(())
    }

    async fn query_edge_exists(
        &self,
        cell_id: &str,
        edge_type: &str,
        src: VertexId,
        dst: VertexId,
        read_epoch: Option<StorageSequence>,
    ) -> Result<bool> {
        if let Some(read_epoch) = read_epoch {
            self.edge_exists_at(cell_id, edge_type, src, dst, read_epoch)
                .await
        } else {
            self.edge_exists(cell_id, edge_type, src, dst).await
        }
    }

    fn validate_reachable_hop_request(
        &self,
        operation: &'static str,
        cell_id: &str,
        edge_type: &str,
        hop_range: (u8, u8),
    ) -> Result<(u8, u8)> {
        validate_component("cell_id", cell_id)?;
        validate_component("edge_type", edge_type)?;
        let (min_hops, max_hops) = hop_range;
        if min_hops > max_hops {
            return Err(GraphError::UnsupportedQuery {
                reason: QueryFailureReason::Pattern,
                dialect: "OpenCypher",
                feature: "invalid variable-length hop range".to_string(),
            });
        }
        ensure_limit(
            operation,
            u64::from(max_hops),
            u64::from(self.limits.max_traversal_hops),
        )?;
        Ok((min_hops, max_hops))
    }

    async fn reachable_vertices_in_hop_range_at(
        &self,
        cell_id: &str,
        edge_type: &str,
        src: VertexId,
        hop_range: (u8, u8),
        read_epoch: StorageSequence,
        budget: &QueryBudget,
    ) -> Result<(Vec<VertexId>, u64)> {
        budget.check("cypher_match_reachable")?;
        let (min_hops, max_hops) = self.validate_reachable_hop_request(
            "cypher_match_reachable",
            cell_id,
            edge_type,
            hop_range,
        )?;
        if let Some(result) = self
            .reachable_vertices_with_compiled_graph_kernel(
                cell_id, edge_type, src, hop_range, read_epoch, budget,
            )
            .await?
        {
            return Ok(result);
        }

        let traversal = self
            .reachable_from_storage_frontier(
                cell_id,
                edge_type,
                src,
                (min_hops, max_hops),
                read_epoch,
                budget,
            )
            .await?;
        self.operation_metrics
            .query_rust_sparse_fallbacks
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Ok((traversal.vertices, traversal.edge_visits))
    }

    pub(crate) async fn reachable_from_storage_frontier(
        &self,
        cell_id: &str,
        edge_type: &str,
        src: VertexId,
        hop_range: (u8, u8),
        read_epoch: StorageSequence,
        budget: &QueryBudget,
    ) -> Result<crate::sparse_kernel::SparseTraversal> {
        let (min_hops, max_hops) = hop_range;
        let snapshot = self.db.snapshot().await?;
        let mut frontier = BTreeSet::from([src]);
        let mut reachable = BTreeSet::new();
        let mut edge_visits = 0_u64;
        for hop in 1..=max_hops {
            if frontier.is_empty() {
                break;
            }
            let mut next = BTreeSet::new();
            for source in frontier {
                budget.check("graph_storage_frontier_source")?;
                let neighbors = self
                    .out_neighbors_in_storage_snapshot(
                        &snapshot, cell_id, edge_type, source, read_epoch,
                    )
                    .await?;
                edge_visits = edge_visits.saturating_add(neighbors.len() as u64);
                ensure_limit(
                    "graph_storage_frontier_edges",
                    edge_visits,
                    self.limits.max_query_scan_edges,
                )?;
                next.extend(neighbors);
            }
            ensure_limit(
                "graph_storage_frontier_vertices",
                next.len() as u64,
                self.limits.max_query_intermediate_rows as u64,
            )?;
            if hop >= min_hops {
                reachable.extend(next.iter().copied());
                ensure_limit(
                    "graph_storage_reachable_vertices",
                    reachable.len() as u64,
                    self.limits.max_query_result_vertices as u64,
                )?;
            }
            frontier = next;
        }
        Ok(crate::sparse_kernel::SparseTraversal {
            vertices: reachable.into_iter().collect(),
            edge_visits,
            backend: SparseKernelBackend::Adjacency,
        })
    }

    #[cfg(feature = "opencypher")]
    async fn reachable_count_in_hop_range_at(
        &self,
        cell_id: &str,
        edge_type: &str,
        src: VertexId,
        hop_range: (u8, u8),
        read_epoch: StorageSequence,
        budget: &QueryBudget,
    ) -> Result<(u64, u64)> {
        budget.check("cypher_match_reachable_count")?;
        let hop_range = self.validate_reachable_hop_request(
            "cypher_match_reachable_count",
            cell_id,
            edge_type,
            hop_range,
        )?;
        if let Some(result) = self
            .reachable_count_with_compiled_graph_kernel(
                cell_id, edge_type, src, hop_range, read_epoch, budget,
            )
            .await?
        {
            return Ok(result);
        }
        let (vertices, edge_visits) = self
            .reachable_vertices_in_hop_range_at(
                cell_id, edge_type, src, hop_range, read_epoch, budget,
            )
            .await?;
        Ok((vertices.len() as u64, edge_visits))
    }

    #[cfg(feature = "opencypher")]
    async fn reachable_vertices_window_in_hop_range_at(
        &self,
        request: ReachableWindowRequest<'_>,
    ) -> Result<(Vec<VertexId>, u64)> {
        validate_query_result_window(request.window, self.limits.max_query_result_vertices)?;
        self.validate_reachable_hop_request(
            "cypher_match_reachable_window",
            request.cell_id,
            request.edge_type,
            request.hop_range,
        )?;
        let cache_key = SourceRelationshipRowsCacheKey::reachable(
            request.cell_id,
            request.edge_type,
            request.src,
            request.hop_range,
            request.read_epoch,
        );
        if let Some(cached) = self
            .source_relationship_rows_cache
            .lock()
            .await
            .get(&cache_key)
        {
            self.cache_metrics
                .record_hit(GraphCacheKind::RelationshipRows);
            return Ok((
                graph_kernel_window_sorted_vertices(
                    cached.as_slice(),
                    request.window,
                    request.ascending,
                )?,
                0,
            ));
        }
        self.cache_metrics
            .record_miss(GraphCacheKind::RelationshipRows);

        let (mut vertices, edge_visits) = Box::pin(self.reachable_vertices_in_hop_range_at(
            request.cell_id,
            request.edge_type,
            request.src,
            request.hop_range,
            request.read_epoch,
            request.budget,
        ))
        .await?;
        self.ensure_query_scan_edges("cypher_graph_kernel_page_window_edge_visits", edge_visits)?;
        graph_kernel_order_vertices(&mut vertices, true);
        let cached = Arc::new(vertices);
        let resident_bytes = source_relationship_rows_resident_bytes(&cache_key, &cached);
        self.source_relationship_rows_cache
            .lock()
            .await
            .insert_sized(
                cache_key,
                Arc::clone(&cached),
                request.cell_id,
                false,
                resident_bytes,
                &self.cache_metrics,
            );
        Ok((
            graph_kernel_window_sorted_vertices(
                cached.as_slice(),
                request.window,
                request.ascending,
            )?,
            edge_visits,
        ))
    }

    async fn reachable_vertices_with_compiled_graph_kernel(
        &self,
        cell_id: &str,
        edge_type: &str,
        src: VertexId,
        hop_range: (u8, u8),
        read_epoch: StorageSequence,
        budget: &QueryBudget,
    ) -> Result<Option<(Vec<VertexId>, u64)>> {
        let _ = (cell_id, edge_type, src, hop_range, read_epoch, budget);
        if crate::sparse_kernel::default_matrix_kernel(&self.cache_policy)
            == SparseKernelBackend::Adjacency
        {
            return Ok(None);
        }
        {
            let (min_hops, max_hops) = hop_range;
            budget.check("cypher_graphblas_artifact_lookup")?;
            let Some(artifact) = self
                .traced_latest_matrix_artifact(cell_id, edge_type, read_epoch)
                .await?
            else {
                return Ok(None);
            };
            budget.check("cypher_graphblas_compiled_matrix")?;
            let Some((compiled, overlay, rebuilt)) = self
                .compiled_graphblas_query_snapshot(
                    cell_id,
                    edge_type,
                    artifact.base_epoch,
                    read_epoch,
                    budget,
                )
                .await?
            else {
                return Ok(None);
            };
            self.record_graphblas_snapshot(rebuilt);
            let traversal = run_graph_compute(
                Arc::clone(&self.operation_metrics),
                "graphblas_expand_range",
                move || {
                    if let Some(overlay) = overlay {
                        expand_range_with_overlay(&compiled, &overlay, &[src], min_hops, max_hops)
                    } else {
                        let empty_adjacency = BTreeMap::new();
                        crate::sparse_kernel::expand_range_compiled_graphblas(
                            &compiled,
                            &empty_adjacency,
                            &[src],
                            min_hops,
                            max_hops,
                        )
                    }
                },
            )
            .await?;
            Ok(Some((traversal.vertices, traversal.edge_visits)))
        }
    }

    #[cfg(feature = "opencypher")]
    async fn reachable_count_with_compiled_graph_kernel(
        &self,
        cell_id: &str,
        edge_type: &str,
        src: VertexId,
        hop_range: (u8, u8),
        read_epoch: StorageSequence,
        budget: &QueryBudget,
    ) -> Result<Option<(u64, u64)>> {
        let _ = (cell_id, edge_type, src, hop_range, read_epoch, budget);
        if crate::sparse_kernel::default_matrix_kernel(&self.cache_policy)
            == SparseKernelBackend::Adjacency
        {
            return Ok(None);
        }
        {
            let (min_hops, max_hops) = hop_range;
            budget.check("cypher_graphblas_count_artifact_lookup")?;
            let artifact_started = std::time::Instant::now();
            let Some(artifact) = self
                .traced_latest_matrix_artifact(cell_id, edge_type, read_epoch)
                .await?
            else {
                return Ok(None);
            };
            self.operation_metrics.query_artifact_lookup_us.fetch_add(
                artifact_started
                    .elapsed()
                    .as_micros()
                    .try_into()
                    .unwrap_or(u64::MAX),
                std::sync::atomic::Ordering::Relaxed,
            );
            budget.check("cypher_graphblas_count_compiled_matrix")?;
            let cache_started = std::time::Instant::now();
            let Some((compiled, overlay, rebuilt)) = self
                .compiled_graphblas_query_snapshot(
                    cell_id,
                    edge_type,
                    artifact.base_epoch,
                    read_epoch,
                    budget,
                )
                .await?
            else {
                return Ok(None);
            };
            self.record_graphblas_snapshot(rebuilt);
            self.operation_metrics.query_graphblas_cache_us.fetch_add(
                cache_started
                    .elapsed()
                    .as_micros()
                    .try_into()
                    .unwrap_or(u64::MAX),
                std::sync::atomic::Ordering::Relaxed,
            );
            let run_inline = overlay.is_some() || compiled.prefer_inline_count(max_hops);
            let compute = move || {
                if let Some(overlay) = overlay {
                    let traversal =
                        expand_range_with_overlay(&compiled, &overlay, &[src], min_hops, max_hops)?;
                    Ok(crate::sparse_kernel::SparseTraversalCount {
                        vertices: traversal.vertices.len() as u64,
                        edge_visits: traversal.edge_visits,
                        backend: traversal.backend,
                    })
                } else {
                    let empty_adjacency = BTreeMap::new();
                    crate::sparse_kernel::expand_range_count_compiled_graphblas(
                        &compiled,
                        &empty_adjacency,
                        &[src],
                        min_hops,
                        max_hops,
                    )
                }
            };
            let traversal = if run_inline {
                run_graph_compute_inline(Arc::clone(&self.operation_metrics), compute)?
            } else {
                run_graph_compute(
                    Arc::clone(&self.operation_metrics),
                    "graphblas_expand_range_count",
                    compute,
                )
                .await?
            };
            Ok(Some((traversal.vertices, traversal.edge_visits)))
        }
    }

    /// `latest_matrix_artifact` with the `artifact.lookup` span around it.
    ///
    /// The generation this resolves to is the join key that BFG-006, BFG-013
    /// and BFG-014 are all really about: the question in every one of them is
    /// whether the write, the index cycle and the read agreed on a generation.
    /// The indexer records the same `cell_id` and `base_sequence` on the span
    /// that produced the artifact, so the join is a backend query rather than
    /// two log files and a clock.
    pub(crate) async fn traced_latest_matrix_artifact(
        &self,
        cell_id: &str,
        edge_type: &str,
        read_epoch: StorageSequence,
    ) -> Result<Option<MatrixArtifact>> {
        let span = tracing::info_span!(
            "artifact.lookup",
            hydradb.cell_id = %cell_id,
            hydradb.edge_type = %edge_type,
            hydradb.read_epoch = read_epoch,
            hydradb.base_sequence = tracing::field::Empty,
            hydradb.outcome = tracing::field::Empty,
            error.class = tracing::field::Empty,
            hydradb.sampling.tail_keep = tracing::field::Empty,
        );
        let artifact = self
            .latest_matrix_artifact(cell_id, edge_type, read_epoch)
            .instrument(span.clone())
            .await;
        match &artifact {
            Ok(Some(artifact)) => {
                span.record("hydradb.outcome", "hit");
                span.record("hydradb.base_sequence", artifact.base_epoch);
            }
            // Absence is the interesting case: it means the read fell off the
            // compiled path and back onto a scan.
            Ok(None) => {
                span.record("hydradb.outcome", "miss");
            }
            Err(err) => {
                span.record("error.class", err.class());
                span.record("hydradb.sampling.tail_keep", "error");
            }
        }
        artifact
    }

    pub(crate) async fn compiled_graphblas_query_snapshot(
        &self,
        cell_id: &str,
        edge_type: &str,
        base_epoch: StorageSequence,
        read_epoch: StorageSequence,
        budget: &QueryBudget,
    ) -> Result<
        Option<(
            Arc<crate::sparse_kernel::CompiledGraphBlasMatrix>,
            Option<GraphTopologyOverlay>,
            bool,
        )>,
    > {
        if let Some(generation) = self
            .graph_index_generation_at(cell_id, edge_type, base_epoch)
            .await?
        {
            let storage_snapshot = self.db.snapshot().await?;
            if storage_snapshot.seq() != read_epoch {
                return Ok(None);
            }
            let mut generation = generation;
            for refresh_attempt in 0..=1 {
                let Some(compiled) = self
                    .cached_graphblas_matrix(cell_id, edge_type, generation.base_sequence)
                    .await?
                else {
                    if refresh_attempt == 0 {
                        if let Some(latest) = self.discover_graph_index(cell_id, edge_type).await? {
                            if latest.base_sequence > generation.base_sequence
                                && latest.base_sequence <= read_epoch
                            {
                                generation = latest;
                                continue;
                            }
                        }
                    }
                    return Ok(None);
                };
                if generation.base_sequence >= read_epoch {
                    return Ok(Some((compiled, None, false)));
                }
                // The gap between the compiled generation and the read epoch,
                // replayed from the WAL. BFG-011 was a hole in exactly this
                // span's window, so both ends of it are recorded: what the
                // artifact was built from and what the read needs to see.
                let tail_span = tracing::info_span!(
                    "storage.wal_tail",
                    hydradb.cell_id = %cell_id,
                    hydradb.edge_type = %edge_type,
                    hydradb.read_epoch = read_epoch,
                    hydradb.base_sequence = generation.base_sequence,
                    hydradb.outcome = tracing::field::Empty,
                    error.class = tracing::field::Empty,
                    hydradb.sampling.tail_keep = tracing::field::Empty,
                );
                let tail = self
                    .topology_tail_since(&generation, storage_snapshot.as_ref(), read_epoch, budget)
                    .instrument(tail_span.clone())
                    .await;
                match &tail {
                    Ok(GraphTopologyTail::Complete(_)) => {
                        tail_span.record("hydradb.outcome", "complete");
                    }
                    Ok(GraphTopologyTail::Unavailable) => {
                        tail_span.record("hydradb.outcome", "unavailable");
                    }
                    Err(err) => {
                        tail_span.record("error.class", err.class());
                        tail_span.record("hydradb.sampling.tail_keep", "error");
                    }
                }
                match tail {
                    Ok(GraphTopologyTail::Complete(overlay)) => {
                        return Ok(Some((compiled, Some(overlay), false)));
                    }
                    Ok(GraphTopologyTail::Unavailable) => return Ok(None),
                    Err(err)
                        if refresh_attempt == 0
                            && matches!(
                                &err,
                                GraphError::AdmissionRejected {
                                    operation: "graph_index_wal_affected_edges",
                                    ..
                                }
                            ) =>
                    {
                        let Some(latest) = self.discover_graph_index(cell_id, edge_type).await?
                        else {
                            return Err(err);
                        };
                        if latest.base_sequence <= generation.base_sequence
                            || latest.base_sequence > read_epoch
                        {
                            return Err(err);
                        }
                        generation = latest;
                    }
                    Err(err) => return Err(err),
                }
            }
            unreachable!("graph index tail refresh loop returns on its second attempt");
        }
        if base_epoch < read_epoch {
            budget.check("cypher_graphblas_adjacency_generation")?;
            let adjacency_generation = self
                .read_counter(&keys::adjacency_generation(cell_id, edge_type))
                .await?;
            if adjacency_generation == 0 || adjacency_generation > base_epoch {
                return Ok(None);
            }
        }
        Ok(self
            .cached_graphblas_matrix(cell_id, edge_type, base_epoch)
            .await?
            .map(|compiled| (compiled, None, false)))
    }

    pub(crate) fn record_graphblas_snapshot(&self, rebuilt: bool) {
        let counter = if rebuilt {
            &self.operation_metrics.query_graphblas_rebuilt_snapshots
        } else {
            &self.operation_metrics.query_graphblas_artifact_snapshots
        };
        counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    pub async fn edge_exists(
        &self,
        cell_id: &str,
        edge_type: &str,
        src: VertexId,
        dst: VertexId,
    ) -> Result<bool> {
        validate_component("cell_id", cell_id)?;
        validate_component("edge_type", edge_type)?;
        self.ensure_cell_readable(cell_id, "edge_exists").await?;
        self.snapshot(cell_id)
            .await?
            .edge_exists(edge_type, src, dst)
            .await
    }

    pub async fn out_neighbors(
        &self,
        cell_id: &str,
        edge_type: &str,
        src: VertexId,
    ) -> Result<Vec<VertexId>> {
        validate_component("cell_id", cell_id)?;
        validate_component("edge_type", edge_type)?;
        self.ensure_cell_readable(cell_id, "out_neighbors").await?;
        self.snapshot(cell_id)
            .await?
            .out_neighbors(edge_type, src)
            .await
    }

    pub async fn out_neighbors_batch(
        &self,
        cell_id: &str,
        edge_type: &str,
        sources: impl IntoIterator<Item = VertexId>,
    ) -> Result<Vec<NeighborBatchEntry>> {
        let read_epoch = self.current_epoch(cell_id).await?;
        self.out_neighbors_batch_at(cell_id, edge_type, sources, read_epoch)
            .await
    }

    pub async fn out_neighbors_batch_at(
        &self,
        cell_id: &str,
        edge_type: &str,
        sources: impl IntoIterator<Item = VertexId>,
        read_epoch: StorageSequence,
    ) -> Result<Vec<NeighborBatchEntry>> {
        self.out_neighbors_batch_at_with_cancellation(cell_id, edge_type, sources, read_epoch, None)
            .await
    }

    pub(crate) async fn out_neighbors_batch_at_with_cancellation(
        &self,
        cell_id: &str,
        edge_type: &str,
        sources: impl IntoIterator<Item = VertexId>,
        read_epoch: StorageSequence,
        cancellation_token: Option<QueryCancellationToken>,
    ) -> Result<Vec<NeighborBatchEntry>> {
        validate_component("cell_id", cell_id)?;
        validate_component("edge_type", edge_type)?;
        self.ensure_cell_readable(cell_id, "out_neighbors_batch")
            .await?;
        let sources: Vec<_> = sources.into_iter().collect();
        ensure_limit(
            "out_neighbors_batch_sources",
            sources.len() as u64,
            self.limits.max_query_intermediate_rows as u64,
        )?;
        if sources.is_empty() {
            return Ok(Vec::new());
        }

        let budget = QueryBudget::new(self.limits.max_query_runtime_ms, cancellation_token);
        let snapshot = self.snapshot_at(cell_id, read_epoch).await?;
        let requested: BTreeSet<_> = sources.iter().copied().collect();
        let mut by_source = requested
            .iter()
            .copied()
            .map(|source| (source, BTreeSet::new()))
            .collect::<BTreeMap<_, _>>();
        let mut scanned_edges = 0_u64;

        for source in requested.iter().copied() {
            budget.check("out_neighbors_batch_snapshot_source")?;
            let neighbors = snapshot.out_neighbors(edge_type, source).await?;
            scanned_edges = scanned_edges.saturating_add(neighbors.len() as u64);
            ensure_limit(
                "out_neighbors_batch_scanned_edges",
                scanned_edges,
                self.limits.max_query_scan_edges,
            )?;
            by_source.entry(source).or_default().extend(neighbors);
        }
        Ok(sources
            .into_iter()
            .map(|vertex| NeighborBatchEntry {
                vertex,
                neighbors: by_source
                    .get(&vertex)
                    .map(|neighbors| neighbors.iter().copied().collect())
                    .unwrap_or_default(),
            })
            .collect())
    }

    pub async fn in_neighbors_batch(
        &self,
        cell_id: &str,
        edge_type: &str,
        destinations: impl IntoIterator<Item = VertexId>,
    ) -> Result<Vec<NeighborBatchEntry>> {
        let read_epoch = self.current_epoch(cell_id).await?;
        self.in_neighbors_batch_at(cell_id, edge_type, destinations, read_epoch)
            .await
    }

    pub async fn in_neighbors_batch_at(
        &self,
        cell_id: &str,
        edge_type: &str,
        destinations: impl IntoIterator<Item = VertexId>,
        read_epoch: StorageSequence,
    ) -> Result<Vec<NeighborBatchEntry>> {
        self.in_neighbors_batch_at_with_cancellation(
            cell_id,
            edge_type,
            destinations,
            read_epoch,
            None,
        )
        .await
    }

    pub(crate) async fn in_neighbors_batch_at_with_cancellation(
        &self,
        cell_id: &str,
        edge_type: &str,
        destinations: impl IntoIterator<Item = VertexId>,
        read_epoch: StorageSequence,
        cancellation_token: Option<QueryCancellationToken>,
    ) -> Result<Vec<NeighborBatchEntry>> {
        validate_component("cell_id", cell_id)?;
        validate_component("edge_type", edge_type)?;
        self.ensure_cell_readable(cell_id, "in_neighbors_batch")
            .await?;
        let destinations: Vec<_> = destinations.into_iter().collect();
        ensure_limit(
            "in_neighbors_batch_destinations",
            destinations.len() as u64,
            self.limits.max_query_intermediate_rows as u64,
        )?;
        if destinations.is_empty() {
            return Ok(Vec::new());
        }

        let budget = QueryBudget::new(self.limits.max_query_runtime_ms, cancellation_token);
        let snapshot = self.snapshot_at(cell_id, read_epoch).await?;
        let requested: BTreeSet<_> = destinations.iter().copied().collect();
        let mut by_destination = requested
            .iter()
            .copied()
            .map(|destination| (destination, BTreeSet::new()))
            .collect::<BTreeMap<_, _>>();
        let mut scanned_edges = 0_u64;

        if self.writes_reverse_index() {
            for destination in requested.iter().copied() {
                budget.check("in_neighbors_batch_snapshot_destination")?;
                let neighbors = snapshot.in_neighbors(edge_type, destination).await?;
                scanned_edges = scanned_edges.saturating_add(neighbors.len() as u64);
                ensure_limit(
                    "in_neighbors_batch_scanned_edges",
                    scanned_edges,
                    self.limits.max_query_scan_edges,
                )?;
                by_destination
                    .entry(destination)
                    .or_default()
                    .extend(neighbors);
            }
        } else {
            let adjacency = self
                .canonical_adjacency_at(cell_id, edge_type, read_epoch)
                .await?;
            for (src, destinations) in adjacency {
                budget.check("in_neighbors_batch_snapshot_adjacency")?;
                for destination in destinations {
                    if let Some(neighbors) = by_destination.get_mut(&destination) {
                        scanned_edges = scanned_edges.saturating_add(1);
                        ensure_limit(
                            "in_neighbors_batch_scanned_edges",
                            scanned_edges,
                            self.limits.max_query_scan_edges,
                        )?;
                        neighbors.insert(src);
                    }
                }
            }
        }
        Ok(destinations
            .into_iter()
            .map(|vertex| NeighborBatchEntry {
                vertex,
                neighbors: by_destination
                    .get(&vertex)
                    .map(|neighbors| neighbors.iter().copied().collect())
                    .unwrap_or_default(),
            })
            .collect())
    }

    pub async fn edge_exists_batch(
        &self,
        cell_id: &str,
        edge_type: &str,
        edges: impl IntoIterator<Item = (VertexId, VertexId)>,
    ) -> Result<Vec<EdgeExistenceBatchEntry>> {
        let read_epoch = self.current_epoch(cell_id).await?;
        self.edge_exists_batch_at(cell_id, edge_type, edges, read_epoch)
            .await
    }

    pub async fn edge_exists_batch_at(
        &self,
        cell_id: &str,
        edge_type: &str,
        edges: impl IntoIterator<Item = (VertexId, VertexId)>,
        read_epoch: StorageSequence,
    ) -> Result<Vec<EdgeExistenceBatchEntry>> {
        validate_component("cell_id", cell_id)?;
        validate_component("edge_type", edge_type)?;
        self.ensure_cell_readable(cell_id, "edge_exists_batch")
            .await?;
        let edges: Vec<_> = edges.into_iter().collect();
        ensure_limit(
            "edge_exists_batch_edges",
            edges.len() as u64,
            self.limits.max_query_intermediate_rows as u64,
        )?;
        if edges.is_empty() {
            return Ok(Vec::new());
        }
        let budget = QueryBudget::new(self.limits.max_query_runtime_ms, None);
        let snapshot = self.snapshot_at(cell_id, read_epoch).await?;
        let mut live = BTreeSet::new();
        for (src, dst) in edges.iter().copied().collect::<BTreeSet<_>>() {
            budget.check("edge_exists_batch_snapshot_point")?;
            if snapshot.edge_exists(edge_type, src, dst).await? {
                live.insert((src, dst));
            }
        }
        Ok(edges
            .into_iter()
            .map(|(src, dst)| EdgeExistenceBatchEntry {
                src,
                dst,
                exists: live.contains(&(src, dst)),
            })
            .collect())
    }

    async fn out_neighbors_at_for_query(
        &self,
        cell_id: &str,
        edge_type: &str,
        src: VertexId,
        read_epoch: StorageSequence,
        budget: &QueryBudget,
    ) -> Result<Vec<VertexId>> {
        validate_component("cell_id", cell_id)?;
        validate_component("edge_type", edge_type)?;
        let snapshot = self.snapshot_at(cell_id, read_epoch).await?;
        let mut neighbors = snapshot.out_neighbors(edge_type, src).await?;
        for _ in &neighbors {
            budget.check("query_out_neighbors_scan")?;
        }
        neighbors.sort_unstable();
        Ok(neighbors)
    }

    pub(crate) async fn out_segment_edge_record_at(
        &self,
        cell_id: &str,
        edge_type: &str,
        src: VertexId,
        dst: VertexId,
        read_epoch: StorageSequence,
    ) -> Result<Option<(StorageSequence, EdgeRecord)>> {
        let tombstone_epoch = self
            .out_segment_tombstone_epoch_at(cell_id, edge_type, src, dst, read_epoch)
            .await?;
        let mut latest = None;
        for (sequence, edge) in self
            .scan_out_segments_for_src_at(cell_id, edge_type, src, read_epoch, None)
            .await?
        {
            if edge.dst == dst && segment_edge_visible(sequence, tombstone_epoch) {
                latest = Some((sequence, edge));
            }
        }
        Ok(latest)
    }

    async fn scan_out_segments_for_src_at(
        &self,
        cell_id: &str,
        edge_type: &str,
        src: VertexId,
        read_epoch: StorageSequence,
        budget: Option<&QueryBudget>,
    ) -> Result<Vec<(StorageSequence, EdgeRecord)>> {
        let prefix = keys::out_segment_src_prefix(cell_id, edge_type, src);
        let mut iter = self.scan_remote_prefix(&prefix).await?;
        let mut edges = Vec::new();
        while let Some(kv) = iter.next().await? {
            check_optional_query_budget(budget, "query_out_segment_scan")?;
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            let segment = decode_out_edge_segment(&key, &kv.value)?;
            if segment.storage_sequence > read_epoch {
                break;
            }
            for dst in segment.destinations.iter().copied() {
                check_optional_query_budget(budget, "query_out_segment_edge_scan")?;
                edges.push((
                    segment.storage_sequence,
                    EdgeRecord {
                        cell_id: segment.cell_id.clone(),
                        edge_type: segment.edge_type.clone(),
                        src: segment.src,
                        dst,
                    },
                ));
                if budget.is_some() {
                    ensure_limit(
                        "query_out_segment_records",
                        edges.len() as u64,
                        self.limits.max_query_scan_edges,
                    )?;
                }
            }
        }
        Ok(edges)
    }

    async fn out_segment_tombstone_epoch_at(
        &self,
        cell_id: &str,
        edge_type: &str,
        src: VertexId,
        dst: VertexId,
        read_epoch: StorageSequence,
    ) -> Result<Option<StorageSequence>> {
        let key = keys::out_segment_tombstone(cell_id, edge_type, src, dst);
        let Some(value) = self.read_remote(&key).await? else {
            return Ok(None);
        };
        let epoch = decode_u64(&key, &value)?;
        Ok((epoch <= read_epoch).then_some(epoch))
    }

    async fn out_segment_tombstones_at(
        &self,
        cell_id: &str,
        edge_type: &str,
        read_epoch: StorageSequence,
        budget: Option<&QueryBudget>,
    ) -> Result<BTreeMap<(VertexId, VertexId), StorageSequence>> {
        let prefix = keys::out_segment_tombstone_edge_type_prefix(cell_id, edge_type);
        let mut iter = self.scan_remote_prefix(&prefix).await?;
        let mut tombstones = BTreeMap::new();
        while let Some(kv) = iter.next().await? {
            check_optional_query_budget(budget, "query_out_segment_tombstone_scan")?;
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            let (key_cell_id, key_edge_type, src, dst) =
                parse_out_edge_segment_tombstone_key(&key)?;
            if key_cell_id != cell_id || key_edge_type != edge_type {
                return Err(GraphError::CorruptValue {
                    key,
                    reason: "segment tombstone identity does not match scan prefix".to_string(),
                });
            }
            let epoch = decode_u64(&key, &kv.value)?;
            if epoch <= read_epoch {
                tombstones.insert((src, dst), epoch);
            }
        }
        Ok(tombstones)
    }

    pub(crate) async fn out_segment_edge_pairs_at(
        &self,
        cell_id: &str,
        edge_type: &str,
        read_epoch: StorageSequence,
    ) -> Result<BTreeSet<(VertexId, VertexId)>> {
        validate_component("cell_id", cell_id)?;
        validate_component("edge_type", edge_type)?;
        let prefix = keys::out_segment_edge_type_prefix(cell_id, edge_type);
        let mut iter = self.scan_remote_prefix(&prefix).await?;
        let tombstones = self
            .out_segment_tombstones_at(cell_id, edge_type, read_epoch, None)
            .await?;
        let mut pairs = BTreeSet::new();
        while let Some(kv) = iter.next().await? {
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            let segment = decode_out_edge_segment(&key, &kv.value)?;
            if segment.storage_sequence > read_epoch {
                continue;
            }
            for dst in segment.destinations.iter().copied() {
                let tombstone_epoch = tombstones.get(&(segment.src, dst)).copied();
                if segment_edge_visible(segment.storage_sequence, tombstone_epoch) {
                    pairs.insert((segment.src, dst));
                }
            }
        }
        Ok(pairs)
    }

    pub async fn in_neighbors(
        &self,
        cell_id: &str,
        edge_type: &str,
        dst: VertexId,
    ) -> Result<Vec<VertexId>> {
        let snapshot = self.snapshot(cell_id).await?;
        if self.writes_reverse_index() {
            return snapshot.in_neighbors(edge_type, dst).await;
        }
        let read_epoch = snapshot.read_epoch();
        GraphStore::scope_snapshot(Arc::clone(&snapshot.storage_snapshot), async move {
            let mut neighbors: Vec<_> = self
                .edges_at(cell_id, edge_type, read_epoch)
                .await?
                .into_iter()
                .filter_map(|edge| (edge.dst == dst).then_some(edge.src))
                .collect();
            neighbors.sort_unstable();
            neighbors.dedup();
            Ok(neighbors)
        })
        .await
    }

    pub async fn in_neighbors_at(
        &self,
        cell_id: &str,
        edge_type: &str,
        dst: VertexId,
        read_epoch: StorageSequence,
    ) -> Result<Vec<VertexId>> {
        validate_component("cell_id", cell_id)?;
        validate_component("edge_type", edge_type)?;
        let snapshot = self.snapshot_at(cell_id, read_epoch).await?;
        if self.writes_reverse_index() {
            return snapshot.in_neighbors(edge_type, dst).await;
        }
        GraphStore::scope_snapshot(Arc::clone(&snapshot.storage_snapshot), async move {
            let mut neighbors: Vec<_> = self
                .edges_at(cell_id, edge_type, read_epoch)
                .await?
                .into_iter()
                .filter_map(|edge| (edge.dst == dst).then_some(edge.src))
                .collect();
            neighbors.sort_unstable();
            neighbors.dedup();
            Ok(neighbors)
        })
        .await
    }

    #[cfg(feature = "opencypher")]
    pub(super) async fn in_neighbors_at_for_query(
        &self,
        cell_id: &str,
        edge_type: &str,
        dst: VertexId,
        read_epoch: StorageSequence,
        budget: &QueryBudget,
    ) -> Result<Vec<VertexId>> {
        validate_component("cell_id", cell_id)?;
        validate_component("edge_type", edge_type)?;
        if !self.writes_reverse_index() {
            let mut neighbors = Vec::new();
            for edge in self
                .edges_at_with_budget(cell_id, edge_type, read_epoch, Some(budget))
                .await?
            {
                budget.check("query_in_neighbors_full_scan")?;
                if edge.dst == dst {
                    neighbors.push(edge.src);
                }
            }
            neighbors.sort_unstable();
            neighbors.dedup();
            return Ok(neighbors);
        }

        let snapshot = self.snapshot_at(cell_id, read_epoch).await?;
        let scan_options = remote_scan_options_for_expected_items(
            self.degree_for_cache_admission(
                &snapshot.storage_snapshot,
                &keys::degree_in(cell_id, edge_type, dst),
            )
            .await?,
        );
        let prefix = keys::in_prefix(cell_id, edge_type, dst);
        let mut iter = snapshot
            .storage_snapshot
            .scan_prefix_with_options(prefix.as_bytes(), .., &scan_options)
            .await?;
        let mut neighbors = Vec::new();
        while let Some(kv) = iter.next().await? {
            budget.check("query_in_neighbors_reverse_scan")?;
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            let record = decode_edge_record(&key, &kv.value)?;
            neighbors.push(record.src);
        }
        neighbors.sort_unstable();
        neighbors.dedup();
        Ok(neighbors)
    }

    pub async fn out_degree(&self, cell_id: &str, edge_type: &str, src: VertexId) -> Result<u64> {
        validate_component("cell_id", cell_id)?;
        validate_component("edge_type", edge_type)?;
        self.ensure_cell_readable(cell_id, "out_degree").await?;
        self.snapshot(cell_id)
            .await?
            .out_degree(edge_type, src)
            .await
    }

    pub async fn current_epoch(&self, cell_id: &str) -> Result<StorageSequence> {
        validate_component("cell_id", cell_id)?;
        self.ensure_cell_readable(cell_id, "current_epoch").await?;
        Ok(self.db.snapshot().await?.seq())
    }

    pub(crate) async fn ensure_cell_readable(
        &self,
        cell_id: &str,
        operation: &'static str,
    ) -> Result<()> {
        validate_component("cell_id", cell_id)?;
        // Both marker reads use the pinned query snapshot. Rechecking that
        // same immutable view adds I/O but cannot discover a later drop.
        if GraphStore::snapshot_cell_is_readable(cell_id) {
            return Ok(());
        }
        if self
            .read_remote(&keys::cell_drop_marker(cell_id))
            .await?
            .is_some()
            || self
                .read_remote(&keys::cell_drop_pending_marker(cell_id))
                .await?
                .is_some()
        {
            return Err(GraphError::CellDropped {
                operation,
                cell_id: cell_id.to_string(),
            });
        }
        GraphStore::remember_snapshot_readable_cell(cell_id);
        Ok(())
    }

    pub async fn edges_at(
        &self,
        cell_id: &str,
        edge_type: &str,
        read_epoch: StorageSequence,
    ) -> Result<Vec<EdgeRecord>> {
        self.edges_at_with_budget(cell_id, edge_type, read_epoch, None)
            .await
    }

    async fn edges_at_with_budget(
        &self,
        cell_id: &str,
        edge_type: &str,
        read_epoch: StorageSequence,
        budget: Option<&QueryBudget>,
    ) -> Result<Vec<EdgeRecord>> {
        validate_component("cell_id", cell_id)?;
        validate_component("edge_type", edge_type)?;
        self.ensure_cell_readable(cell_id, "edges_at").await?;
        check_optional_query_budget(budget, "query_edges_at")?;
        let snapshot = self.snapshot_at(cell_id, read_epoch).await?;
        self.edges_in_snapshot_at_topology(&snapshot, edge_type, read_epoch, budget)
            .await
    }

    pub(crate) async fn edges_in_current_snapshot_at_topology(
        &self,
        snapshot: &GraphSnapshot<'_>,
        edge_type: &str,
        topology_sequence: StorageSequence,
    ) -> Result<Vec<EdgeRecord>> {
        validate_component("edge_type", edge_type)?;
        self.edges_in_snapshot_at_topology(snapshot, edge_type, topology_sequence, None)
            .await
    }

    async fn edges_in_snapshot_at_topology(
        &self,
        snapshot: &GraphSnapshot<'_>,
        edge_type: &str,
        topology_sequence: StorageSequence,
        budget: Option<&QueryBudget>,
    ) -> Result<Vec<EdgeRecord>> {
        let cell_id = snapshot.cell_id();
        GraphStore::scope_snapshot(Arc::clone(&snapshot.storage_snapshot), async move {
            let adjacency = self
                .canonical_adjacency_at(cell_id, edge_type, topology_sequence)
                .await?;
            let mut edges = Vec::new();
            for (src, destinations) in adjacency {
                check_optional_query_budget(budget, "query_edges_at_adjacency")?;
                for dst in destinations {
                    check_optional_query_budget(budget, "query_edges_at_adjacency_edge")?;
                    edges.push(EdgeRecord {
                        cell_id: cell_id.to_string(),
                        edge_type: edge_type.to_string(),
                        src,
                        dst,
                    });
                    if budget.is_some() {
                        ensure_limit(
                            "query_edges_at_canonical",
                            edges.len() as u64,
                            self.limits.max_query_scan_edges,
                        )?;
                    }
                }
            }
            Ok(edges)
        })
        .await
    }

    pub async fn validate_cell_edge_type(
        &self,
        cell_id: &str,
        edge_type: &str,
    ) -> Result<GraphRepairReport> {
        validate_component("cell_id", cell_id)?;
        validate_component("edge_type", edge_type)?;
        let read_epoch = self.current_epoch(cell_id).await?;
        let edges = self.edges_at(cell_id, edge_type, read_epoch).await?;
        let mut out_counts = BTreeMap::<VertexId, u64>::new();
        let mut in_counts = BTreeMap::<VertexId, u64>::new();
        for edge in &edges {
            *out_counts.entry(edge.src).or_default() += 1;
            *in_counts.entry(edge.dst).or_default() += 1;
        }
        let mut degree_mismatches = Vec::new();
        for (src, expected) in out_counts {
            let actual = self.out_degree(cell_id, edge_type, src).await?;
            if actual != expected {
                degree_mismatches.push(format!("out:{src}:expected={expected}:actual={actual}"));
            }
        }
        if self.writes_reverse_index() {
            for (dst, expected) in in_counts {
                let actual = self
                    .read_counter(&keys::degree_in(cell_id, edge_type, dst))
                    .await?;
                if actual != expected {
                    degree_mismatches.push(format!("in:{dst}:expected={expected}:actual={actual}"));
                }
            }
        } else {
            let mut iter = self
                .scan_remote_prefix(&keys::degree_in_prefix(cell_id, edge_type))
                .await?;
            while let Some(kv) = iter.next().await? {
                let key = String::from_utf8_lossy(&kv.key);
                degree_mismatches.push(format!("in:{key}:unexpected-under-outbound-only"));
            }
        }
        Ok(GraphRepairReport {
            cell_id: cell_id.to_string(),
            edge_type: edge_type.to_string(),
            read_epoch,
            live_edges: edges.len() as u64,
            degree_mismatches,
        })
    }
}

#[derive(Clone, Debug)]
pub(crate) struct QueryBudget {
    started_at: std::time::Instant,
    max_runtime_ms: Option<u64>,
    cancellation_token: Option<QueryCancellationToken>,
    #[cfg(feature = "opencypher")]
    max_result_bytes: Option<u64>,
    #[cfg(feature = "opencypher")]
    result_bytes: Arc<AtomicU64>,
    /// Edges visited by every edge match in this query, shared across clones.
    /// An undirected hop runs `match_edge_row_pattern_oriented` twice, so a
    /// tally local to one oriented call would let a query spend
    /// `max_query_scan_edges` once per orientation.
    #[cfg(feature = "opencypher")]
    scanned_edges: Arc<AtomicU64>,
    /// Index entries read by every ordered walk in this query, shared across
    /// clones, for the same reason as `scanned_edges`. An ordered walk is paged
    /// and each page is a separate call, so a tally local to one page would
    /// start at zero on every page: `max_query_index_candidates` would bound one
    /// page and nothing at all about a walk, which is to say the query could
    /// read the whole index and never reach the limit.
    #[cfg(feature = "experimental-cypher-engine")]
    scanned_index_candidates: Arc<AtomicU64>,
}

impl QueryBudget {
    /// Add to the query's edge-visit tally and return the new total. Shared
    /// across clones, so both orientations of an undirected hop draw on one
    /// allowance.
    #[cfg(feature = "opencypher")]
    pub(crate) fn add_scanned_edges(&self, edges: u64) -> u64 {
        self.scanned_edges
            .fetch_add(edges, Ordering::Relaxed)
            .saturating_add(edges)
    }

    /// Add to the query's index-entry tally and return the new total. Shared
    /// across clones, so every page of an ordered walk draws on one allowance.
    #[cfg(feature = "experimental-cypher-engine")]
    pub(crate) fn add_scanned_index_candidates(&self, entries: u64) -> u64 {
        self.scanned_index_candidates
            .fetch_add(entries, Ordering::Relaxed)
            .saturating_add(entries)
    }

    /// Only for cancellation-safe read I/O, never commits or native compute
    /// whose resource ownership must outlive cancellation of the caller.
    #[cfg(feature = "opencypher")]
    pub(crate) async fn read_only_io<T>(
        &self,
        operation: &'static str,
        read: impl std::future::Future<Output = Result<T>>,
    ) -> Result<T> {
        self.check(operation)?;
        // A cache hit can be immediately ready for every iteration of a large
        // scan. `await` alone then never yields the worker to other requests.
        tokio::task::consume_budget().await;
        self.check(operation)?;
        let cancelled = async {
            match &self.cancellation_token {
                Some(token) => token.cancelled().await,
                None => std::future::pending::<()>().await,
            }
        };
        let deadline = async {
            match self.max_runtime_ms {
                Some(limit) => {
                    tokio::time::sleep(
                        std::time::Duration::from_millis(limit)
                            .saturating_sub(self.started_at.elapsed()),
                    )
                    .await
                }
                None => std::future::pending::<()>().await,
            }
        };
        tokio::select! {
            biased;
            _ = cancelled => Err(GraphError::QueryTimeout {
                operation: "query_cancelled",
                elapsed_ms: self.started_at.elapsed().as_millis().min(u128::from(u64::MAX)) as u64,
                limit_ms: 0,
            }),
            _ = deadline => Err(GraphError::QueryTimeout {
                operation,
                elapsed_ms: self.started_at.elapsed().as_millis().min(u128::from(u64::MAX)) as u64,
                limit_ms: self.max_runtime_ms.unwrap_or(0),
            }),
            result = read => result,
        }
    }

    pub(crate) fn new(
        max_runtime_ms: Option<u64>,
        cancellation_token: Option<QueryCancellationToken>,
    ) -> Self {
        Self {
            started_at: std::time::Instant::now(),
            max_runtime_ms,
            cancellation_token,
            #[cfg(feature = "opencypher")]
            max_result_bytes: None,
            #[cfg(feature = "opencypher")]
            result_bytes: Arc::new(AtomicU64::new(0)),
            #[cfg(feature = "opencypher")]
            scanned_edges: Arc::new(AtomicU64::new(0)),
            #[cfg(feature = "experimental-cypher-engine")]
            scanned_index_candidates: Arc::new(AtomicU64::new(0)),
        }
    }

    #[cfg(feature = "opencypher")]
    pub(crate) fn with_max_result_bytes(mut self, max_result_bytes: Option<u64>) -> Self {
        self.max_result_bytes = max_result_bytes;
        self
    }

    #[cfg(feature = "opencypher")]
    pub(crate) fn account_result_row(&self, row: &QueryRow) -> Result<()> {
        let Some(limit) = self.max_result_bytes else {
            return Ok(());
        };
        let bytes = row.estimated_resident_bytes();
        let previous = self
            .result_bytes
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                Some(current.saturating_add(bytes))
            })
            .unwrap_or_else(|current| current);
        let actual = previous.saturating_add(bytes);
        if actual > limit {
            return Err(GraphError::AdmissionRejected {
                operation: "client_cursor_buffer_bytes",
                actual,
                limit,
            });
        }
        Ok(())
    }

    #[cfg(feature = "opencypher")]
    pub(crate) fn account_result_rows(&self, rows: &[QueryRow]) -> Result<()> {
        for row in rows {
            self.account_result_row(row)?;
        }
        Ok(())
    }

    pub(crate) fn check(&self, operation: &'static str) -> Result<()> {
        if let Some(token) = &self.cancellation_token {
            if !token.is_cancelled() {
                return self.check_runtime_limit(operation);
            }
            return Err(GraphError::QueryTimeout {
                operation: "query_cancelled",
                elapsed_ms: self
                    .started_at
                    .elapsed()
                    .as_millis()
                    .min(u128::from(u64::MAX)) as u64,
                limit_ms: 0,
            });
        }
        self.check_runtime_limit(operation)
    }

    fn check_runtime_limit(&self, operation: &'static str) -> Result<()> {
        let Some(limit_ms) = self.max_runtime_ms else {
            return Ok(());
        };
        let elapsed_ms = self.started_at.elapsed().as_millis();
        if elapsed_ms >= u128::from(limit_ms) {
            return Err(GraphError::QueryTimeout {
                operation,
                elapsed_ms: elapsed_ms.min(u128::from(u64::MAX)) as u64,
                limit_ms,
            });
        }
        Ok(())
    }
}

fn check_optional_query_budget(
    budget: Option<&QueryBudget>,
    operation: &'static str,
) -> Result<()> {
    if let Some(budget) = budget {
        budget.check(operation)?;
    }
    Ok(())
}

#[cfg(feature = "opencypher")]
struct QueryStatsHistogramPublish<'a> {
    cell_id: &'a str,
    operation: &'static str,
    read_epoch: StorageSequence,
    histogram_key: String,
    bucket_prefix: String,
    stats: &'a QueryStatsRecord,
    buckets: &'a BTreeMap<String, u64>,
}

#[cfg(feature = "opencypher")]
struct EdgeExpansionStatsRequest<'a> {
    cell_id: &'a str,
    edge_type: &'a str,
    direction: QueryStatsDirection,
    anchor: &'a str,
    source_labels: &'a [String],
    snapshot: &'a GraphStorageSnapshot,
    read_epoch: StorageSequence,
    budget: &'a QueryBudget,
}

#[cfg(feature = "opencypher")]
fn stats_record_from_bucket_count(
    count: u64,
    read_epoch: StorageSequence,
    buckets: &BTreeMap<String, u64>,
) -> QueryStatsRecord {
    let mut stats = stats_record_from_histogram(read_epoch, buckets);
    stats.count = count;
    stats
}

#[cfg(feature = "opencypher")]
fn stats_record_from_histogram(
    read_epoch: StorageSequence,
    buckets: &BTreeMap<String, u64>,
) -> QueryStatsRecord {
    let total = buckets.values().copied().sum::<u64>();
    let most_common = buckets.values().copied().max().unwrap_or(0);
    let mut stats = QueryStatsRecord::histogram(
        total,
        read_epoch,
        graph_now_millis(),
        buckets.len() as u64,
        most_common,
    );
    stats.bloom = Some(QueryStatsBloom::from_encoded_values(
        buckets.keys().map(String::as_str),
    ));
    stats
}

#[cfg(feature = "opencypher")]
fn validate_query_stats_refresh_kind(kind: &QueryStatsRefreshKind) -> Result<()> {
    match kind {
        QueryStatsRefreshKind::Cardinality(QueryCardinalityStatsKind::EdgeExpansion {
            edge_type,
            source_labels,
            ..
        }) => {
            validate_component("edge_type", edge_type)?;
            canonical_query_stats_source_labels(source_labels).map(|_| ())
        }
        QueryStatsRefreshKind::Cardinality(QueryCardinalityStatsKind::EdgeType { edge_type }) => {
            validate_component("edge_type", edge_type)
        }
        QueryStatsRefreshKind::Cardinality(QueryCardinalityStatsKind::VertexLabel { label }) => {
            validate_component("label", label)
        }
        QueryStatsRefreshKind::Cardinality(
            QueryCardinalityStatsKind::VertexLabelIntersection { labels },
        ) => canonical_query_stats_labels(labels).map(|_| ()),
        QueryStatsRefreshKind::Cardinality(QueryCardinalityStatsKind::VertexProperty {
            property,
            ..
        }) => validate_component("property", property),
        QueryStatsRefreshKind::Cardinality(QueryCardinalityStatsKind::EdgeProperty {
            edge_type,
            property,
            ..
        }) => {
            validate_component("edge_type", edge_type)?;
            validate_component("property", property)
        }
        QueryStatsRefreshKind::VertexPropertyHistogram { property } => {
            validate_component("property", property)
        }
        QueryStatsRefreshKind::EdgePropertyHistogram {
            edge_type,
            property,
        } => {
            validate_component("edge_type", edge_type)?;
            validate_component("property", property)
        }
    }
}

#[cfg(feature = "opencypher")]
fn canonical_query_stats_labels(labels: &[String]) -> Result<Vec<String>> {
    if labels.len() < 2 {
        return Err(GraphError::UnsupportedQuery {
            reason: QueryFailureReason::Other,
            dialect: "QueryStats",
            feature: "a label intersection requires at least two labels".to_string(),
        });
    }
    let labels = canonical_query_stats_source_labels(labels)?;
    if labels.len() < 2 {
        return Err(GraphError::UnsupportedQuery {
            reason: QueryFailureReason::Other,
            dialect: "QueryStats",
            feature: "a label intersection requires at least two distinct labels".to_string(),
        });
    }
    Ok(labels)
}

#[cfg(feature = "opencypher")]
fn canonical_query_stats_source_labels(labels: &[String]) -> Result<Vec<String>> {
    if labels.is_empty() {
        return Err(GraphError::UnsupportedQuery {
            reason: QueryFailureReason::Other,
            dialect: "QueryStats",
            feature: "edge-expansion statistics require at least one source label".to_string(),
        });
    }
    for label in labels {
        validate_component("label", label)?;
    }
    Ok(labels
        .iter()
        .cloned()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect())
}

#[cfg(feature = "opencypher")]
struct EdgeRowMatchState<'a> {
    cell_id: &'a str,
    read_epoch: StorageSequence,
    /// The pattern every row in this traversal is matched against. Held here
    /// rather than passed to the flush separately so the pattern a row was
    /// built from and the one it is filtered by cannot diverge.
    edge: &'a RowEdgePattern,
    /// Matched rows, hydrated and filtered.
    ///
    /// Owned rather than borrowed so a traversal's results arrive through
    /// [`GraphShard::finish_edge_rows`], which drains [`Self::pending`] first.
    /// With a borrowed `&mut Vec` the natural way to end a traversal was to
    /// return the caller's own vector, which returned any un-hydrated tail as
    /// though it had never matched -- and removing a flush passed the whole
    /// suite when I tried it.
    ///
    /// This is a convention, not an invariant the compiler holds: field privacy
    /// is module-scoped and this module is one file, so `Ok(state.rows)` still
    /// compiles. What it buys is that the correct spelling is also the obvious
    /// one, and that a bypass has to be written deliberately.
    rows: Vec<BindingRow>,
    /// Rows built but not yet hydrated, drained by
    /// [`GraphShard::flush_pending_edge_rows`].
    pending: Vec<BindingRow>,
    metadata_cache: &'a mut BTreeMap<VertexId, VertexMetadata>,
    edge_metadata_cache: &'a mut BTreeMap<BoundRelationship, EdgeMetadata>,
    budget: &'a QueryBudget,
}

/// How many rows accumulate before a traversal hydrates them.
///
/// Batching trades a bounded amount of held memory for round trips saved, and
/// this is the bound: a traversal never holds more than this many un-hydrated
/// rows however large its candidate set. Comfortably above
/// [`QUERY_METADATA_HYDRATION_CONCURRENCY`] so a flush has enough work to keep
/// the fan-out saturated, and far below the intermediate-row admission limit
/// that `push_binding_row` enforces on the rows actually returned.
#[cfg(feature = "opencypher")]
const EDGE_ROW_HYDRATION_BATCH: usize = 1_024;

#[cfg(feature = "opencypher")]
struct VertexMutationApplyState<'a> {
    cell_id: &'a str,
    read_epoch: StorageSequence,
    pending_metadata: &'a mut BTreeMap<VertexId, VertexMetadata>,
    original_metadata: &'a mut BTreeMap<VertexId, VertexMetadata>,
    pending_edge_metadata: &'a mut BTreeMap<BoundRelationship, EdgeMetadata>,
    original_edge_metadata: &'a mut BTreeMap<BoundRelationship, EdgeMetadata>,
    budget: &'a QueryBudget,
}

#[cfg(feature = "opencypher")]
fn streaming_neighbor_page_edge(query: &ParsedRowQuery) -> Option<&RowEdgePattern> {
    if !query.union_arms.is_empty()
        || query.predicate.is_some()
        || row_projections_have_aggregates(&query.projections)
        || query.pattern_groups.len() != 1
    {
        return None;
    }
    let group = query.pattern_groups.first()?;
    if group.optional || group.predicate.is_some() || group.patterns.len() != 1 {
        return None;
    }
    let RowPattern::Edge(edge) = group.patterns.first()? else {
        return None;
    };
    if edge.direction == EdgeDirection::Bidirectional
        || edge.hop_range.is_some()
        || !edge.properties.is_empty()
        || edge.src.id.is_none()
        || edge.dst.id.is_some()
        || !edge.src.labels.is_empty()
        || !row_node_has_only_id_property(&edge.src)
        || !edge.dst.labels.is_empty()
        || !edge.dst.properties.is_empty()
    {
        return None;
    }
    if !query
        .projections
        .iter()
        .all(|projection| streaming_neighbor_projection_supported(edge, projection))
    {
        return None;
    }
    Some(edge)
}

#[cfg(feature = "opencypher")]
fn row_node_has_only_id_property(node: &RowNodePattern) -> bool {
    node.properties.keys().all(|property| property == "id")
}

#[cfg(feature = "opencypher")]
struct GraphKernelRowQueryRequest<'a> {
    edge: &'a RowEdgePattern,
    src: VertexId,
    hop_range: (u8, u8),
    projection: GraphKernelProjection,
    ascending: bool,
}

#[cfg(feature = "opencypher")]
struct ReachableWindowRequest<'a> {
    cell_id: &'a str,
    edge_type: &'a str,
    src: VertexId,
    hop_range: (u8, u8),
    read_epoch: StorageSequence,
    window: QueryWindow,
    ascending: bool,
    budget: &'a QueryBudget,
}

#[cfg(feature = "opencypher")]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum GraphKernelProjection {
    NodeId,
    CountAll,
}

#[cfg(feature = "opencypher")]
fn graph_kernel_node_id_rows(
    vertices: Vec<VertexId>,
    budget: &QueryBudget,
) -> Result<Vec<QueryRow>> {
    let mut rows = Vec::with_capacity(vertices.len());
    for vertex in vertices {
        budget.check("cypher_graph_kernel_project")?;
        let row = QueryRow::new(vec![QueryValue::VertexId(vertex)]);
        rows.push(row);
    }
    Ok(rows)
}

#[cfg(feature = "opencypher")]
fn graph_kernel_order_vertices(vertices: &mut Vec<VertexId>, ascending: bool) {
    vertices.sort_unstable();
    vertices.dedup();
    if !ascending {
        vertices.reverse();
    }
}

#[cfg(feature = "opencypher")]
fn graph_kernel_window_sorted_vertices(
    vertices: &[VertexId],
    window: QueryWindow,
    ascending: bool,
) -> Result<Vec<VertexId>> {
    let skip = usize::try_from(window.skip).map_err(|_| GraphError::AdmissionRejected {
        operation: "query_result_skip",
        actual: window.skip,
        limit: usize::MAX as u64,
    })?;
    let limit = window.limit.unwrap_or(usize::MAX);
    if ascending {
        Ok(vertices.iter().copied().skip(skip).take(limit).collect())
    } else {
        Ok(vertices
            .iter()
            .rev()
            .copied()
            .skip(skip)
            .take(limit)
            .collect())
    }
}

#[cfg(feature = "opencypher")]
fn validate_query_result_window(window: QueryWindow, max_rows: usize) -> Result<()> {
    let _ = usize::try_from(window.skip).map_err(|_| GraphError::AdmissionRejected {
        operation: "query_result_skip",
        actual: window.skip,
        limit: usize::MAX as u64,
    })?;
    if let Some(limit) = window.limit {
        ensure_limit("query_result_limit", limit as u64, max_rows as u64)?;
    }
    Ok(())
}

#[cfg(feature = "opencypher")]
fn graph_kernel_row_query_request(
    query: &ParsedRowQuery,
) -> Option<GraphKernelRowQueryRequest<'_>> {
    if !query.union_arms.is_empty() || query.predicate.is_some() {
        return None;
    }
    let patterns = graph_kernel_query_patterns(query)?;
    let [pattern] = patterns else {
        return None;
    };
    let RowPattern::Edge(edge) = pattern else {
        return None;
    };
    let src = edge.src.id?;
    let hop_range = edge.hop_range?;
    if edge.direction == EdgeDirection::Bidirectional
        || edge.binding.is_some()
        || !edge.properties.is_empty()
        || !graph_kernel_node_pattern(&edge.src)
        || !graph_kernel_node_pattern(&edge.dst)
    {
        return None;
    }
    let projection = graph_kernel_projection(edge, &query.projections)?;
    let ascending = graph_kernel_order_supported(
        edge,
        projection,
        &query.projections,
        &query.columns,
        &query.order_by,
    )?;
    Some(GraphKernelRowQueryRequest {
        edge,
        src,
        hop_range,
        projection,
        ascending,
    })
}

#[cfg(feature = "opencypher")]
fn relationship_count_query_edge(query: &ParsedRowQuery) -> Option<&RowEdgePattern> {
    if !query.union_arms.is_empty() || query.predicate.is_some() || !query.order_by.is_empty() {
        return None;
    }
    if query.projections.as_slice() != [RowProjection::CountAll] {
        return None;
    }
    let patterns = graph_kernel_query_patterns(query)?;
    let [pattern] = patterns else {
        return None;
    };
    let RowPattern::Edge(edge) = pattern else {
        return None;
    };
    if edge.direction == EdgeDirection::Bidirectional
        || edge.hop_range.is_some()
        || edge.src.id.is_none()
        || edge.dst.id.is_none()
        || !edge.properties.is_empty()
        || !graph_kernel_node_pattern(&edge.src)
        || !graph_kernel_node_pattern(&edge.dst)
    {
        return None;
    }
    Some(edge)
}

#[cfg(feature = "opencypher")]
fn source_relationship_id_rows_query_edge(query: &ParsedRowQuery) -> Option<&RowEdgePattern> {
    if !query.union_arms.is_empty()
        || query.predicate.is_some()
        || row_projections_have_aggregates(&query.projections)
    {
        return None;
    }
    let patterns = graph_kernel_query_patterns(query)?;
    let [pattern] = patterns else {
        return None;
    };
    let RowPattern::Edge(edge) = pattern else {
        return None;
    };
    if edge.direction == EdgeDirection::Bidirectional
        || edge.hop_range.is_some()
        || edge.src.id.is_none()
        || edge.dst.id.is_some()
        || edge.binding.is_some()
        || !edge.properties.is_empty()
        || !graph_kernel_node_pattern(&edge.src)
        || !graph_kernel_node_pattern(&edge.dst)
        || !source_relationship_id_projection_supported(edge, &query.projections)
        || !source_relationship_id_order_supported(
            edge,
            &query.projections,
            &query.columns,
            &query.order_by,
        )
    {
        return None;
    }
    Some(edge)
}

#[cfg(feature = "opencypher")]
fn source_relationship_id_projection_supported(
    edge: &RowEdgePattern,
    projections: &[RowProjection],
) -> bool {
    !projections.is_empty()
        && projections.iter().all(|projection| match projection {
            RowProjection::NodeId { binding } => {
                edge.src.binding.as_deref() == Some(binding.as_str())
                    || edge.dst.binding.as_deref() == Some(binding.as_str())
            }
            RowProjection::Property { .. }
            | RowProjection::CountAll
            | RowProjection::Literal(_)
            | RowProjection::Aggregate { .. } => false,
        })
}

#[cfg(feature = "opencypher")]
fn source_relationship_id_order_supported(
    edge: &RowEdgePattern,
    projections: &[RowProjection],
    columns: &[QueryColumn],
    order_by: &[RowSort],
) -> bool {
    order_by.iter().all(|sort| match &sort.expression {
        RowSortExpression::NodeId { binding } => {
            edge.src.binding.as_deref() == Some(binding.as_str())
                || edge.dst.binding.as_deref() == Some(binding.as_str())
        }
        RowSortExpression::Column { name } => columns
            .iter()
            .position(|column| column.name == *name)
            .and_then(|idx| projections.get(idx))
            .is_some_and(|projection| match projection {
                RowProjection::NodeId { binding } => {
                    edge.src.binding.as_deref() == Some(binding.as_str())
                        || edge.dst.binding.as_deref() == Some(binding.as_str())
                }
                RowProjection::Property { .. }
                | RowProjection::CountAll
                | RowProjection::Literal(_)
                | RowProjection::Aggregate { .. } => false,
            }),
        RowSortExpression::Property { .. } | RowSortExpression::CountAll => false,
    })
}

#[cfg(feature = "opencypher")]
fn source_relationship_id_page_order(
    edge: &RowEdgePattern,
    query: &ParsedRowQuery,
) -> Option<bool> {
    let [sort] = query.order_by.as_slice() else {
        return None;
    };
    let orders_destination = match &sort.expression {
        RowSortExpression::NodeId { binding } => {
            edge.dst.binding.as_deref() == Some(binding.as_str())
        }
        RowSortExpression::Column { name } => query
            .columns
            .iter()
            .position(|column| column.name == *name)
            .and_then(|index| query.projections.get(index))
            .is_some_and(|projection| {
                matches!(
                    projection,
                    RowProjection::NodeId { binding }
                        if edge.dst.binding.as_deref() == Some(binding.as_str())
                )
            }),
        RowSortExpression::Property { .. } | RowSortExpression::CountAll => false,
    };
    orders_destination.then_some(sort.ascending)
}

#[cfg(feature = "opencypher")]
fn source_relationship_id_bindings_from_dsts(
    edge: &RowEdgePattern,
    src: VertexId,
    dsts: &[VertexId],
    shard: &GraphShard,
    budget: &QueryBudget,
) -> Result<Vec<BindingRow>> {
    let mut rows = Vec::with_capacity(dsts.len());
    for dst in dsts {
        budget.check("cypher_source_relationship_id_cached_rows")?;
        if let Some(row) = BindingRow::from_edge(edge, src, *dst) {
            rows.push(row);
            shard.ensure_query_intermediate_rows(
                "cypher_source_relationship_id_cached_rows",
                rows.len(),
            )?;
        }
    }
    Ok(rows)
}

#[cfg(feature = "opencypher")]
fn relationship_rows_query_edge(query: &ParsedRowQuery) -> Option<&RowEdgePattern> {
    if !query.union_arms.is_empty()
        || query.predicate.is_some()
        || row_projections_have_aggregates(&query.projections)
    {
        return None;
    }
    let patterns = graph_kernel_query_patterns(query)?;
    let [pattern] = patterns else {
        return None;
    };
    let RowPattern::Edge(edge) = pattern else {
        return None;
    };
    if edge.direction == EdgeDirection::Bidirectional
        || edge.hop_range.is_some()
        || edge.src.id.is_none()
        || edge.dst.id.is_none()
        || edge.binding.is_none()
        || !graph_kernel_node_pattern(&edge.src)
        || !graph_kernel_node_pattern(&edge.dst)
    {
        return None;
    }
    Some(edge)
}

#[cfg(feature = "opencypher")]
fn ordered_relationship_property_index_spec(
    query: &ParsedRowQuery,
    window: QueryWindow,
) -> Option<OrderedRelationshipPropertyIndexSpec<'_>> {
    if !query.union_arms.is_empty()
        || query.distinct
        || row_projections_have_aggregates(&query.projections)
    {
        return None;
    }
    let (patterns, predicate) = if query.pattern_groups.is_empty() {
        (query.patterns.as_slice(), query.predicate.as_ref()?)
    } else {
        let [group] = query.pattern_groups.as_slice() else {
            return None;
        };
        if group.optional {
            return None;
        }
        let predicate = match (query.predicate.as_ref(), group.predicate.as_ref()) {
            (Some(query_predicate), Some(group_predicate))
                if query_predicate == group_predicate =>
            {
                query_predicate
            }
            (Some(predicate), None) | (None, Some(predicate)) => predicate,
            (Some(_), Some(_)) | (None, None) => return None,
        };
        (group.patterns.as_slice(), predicate)
    };
    let [pattern] = patterns else {
        return None;
    };
    let RowPattern::Edge(edge) = pattern else {
        return None;
    };
    if edge.direction == EdgeDirection::Bidirectional
        || edge.hop_range.is_some()
        || !edge.properties.is_empty()
        || edge.binding.is_none()
    {
        return None;
    }
    let [sort] = query.order_by.as_slice() else {
        return None;
    };
    let property = match &sort.expression {
        RowSortExpression::Property { binding, property }
            if edge.binding.as_deref() == Some(binding.as_str()) && property != "id" =>
        {
            property.as_str()
        }
        RowSortExpression::Column { name } => {
            let index = query
                .columns
                .iter()
                .position(|column| column.name == *name)?;
            match query.projections.get(index)? {
                RowProjection::Property { binding, property }
                    if edge.binding.as_deref() == Some(binding.as_str()) && property != "id" =>
                {
                    property.as_str()
                }
                RowProjection::NodeId { .. }
                | RowProjection::Property { .. }
                | RowProjection::CountAll
                | RowProjection::Literal(_)
                | RowProjection::Aggregate { .. } => return None,
            }
        }
        RowSortExpression::NodeId { .. }
        | RowSortExpression::Property { .. }
        | RowSortExpression::CountAll => return None,
    };
    if !row_predicate_requires_property(predicate, edge.binding.as_deref()?, property) {
        return None;
    }
    Some(OrderedRelationshipPropertyIndexSpec {
        edge,
        property,
        predicate,
        ascending: sort.ascending,
        limit: window.limit?,
    })
}

#[cfg(feature = "opencypher")]
fn relationship_rows_projection_supported(
    edge: &RowEdgePattern,
    projections: &[RowProjection],
) -> bool {
    projections.iter().all(|projection| match projection {
        RowProjection::NodeId { binding } => {
            edge.src.binding.as_deref() == Some(binding.as_str())
                || edge.dst.binding.as_deref() == Some(binding.as_str())
        }
        RowProjection::Property { binding, property } => {
            edge.binding.as_deref() == Some(binding.as_str()) && property != "id"
        }
        RowProjection::CountAll | RowProjection::Literal(_) | RowProjection::Aggregate { .. } => {
            false
        }
    })
}

#[cfg(feature = "opencypher")]
fn relationship_rows_order_supported(
    edge: &RowEdgePattern,
    projections: &[RowProjection],
    columns: &[QueryColumn],
    order_by: &[RowSort],
) -> bool {
    order_by.iter().all(|sort| match &sort.expression {
        RowSortExpression::NodeId { binding } => {
            edge.src.binding.as_deref() == Some(binding.as_str())
                || edge.dst.binding.as_deref() == Some(binding.as_str())
        }
        RowSortExpression::Property { binding, property } => {
            edge.binding.as_deref() == Some(binding.as_str()) && property != "id"
        }
        RowSortExpression::Column { name } => columns
            .iter()
            .position(|column| column.name == *name)
            .and_then(|idx| projections.get(idx))
            .is_some_and(|projection| match projection {
                RowProjection::NodeId { binding } => {
                    edge.src.binding.as_deref() == Some(binding.as_str())
                        || edge.dst.binding.as_deref() == Some(binding.as_str())
                }
                RowProjection::Property { binding, property } => {
                    edge.binding.as_deref() == Some(binding.as_str()) && property != "id"
                }
                RowProjection::CountAll
                | RowProjection::Literal(_)
                | RowProjection::Aggregate { .. } => false,
            }),
        RowSortExpression::CountAll => false,
    })
}

#[cfg(feature = "opencypher")]
fn graph_kernel_query_patterns(query: &ParsedRowQuery) -> Option<&[RowPattern]> {
    if query.pattern_groups.is_empty() {
        return Some(&query.patterns);
    }
    if query.pattern_groups.len() != 1 {
        return None;
    }
    let group = query.pattern_groups.first()?;
    if group.optional || group.predicate.is_some() {
        return None;
    }
    Some(&group.patterns)
}

#[cfg(feature = "opencypher")]
fn graph_kernel_node_pattern(node: &RowNodePattern) -> bool {
    node.labels.is_empty() && row_node_has_only_id_property(node)
}

#[cfg(feature = "opencypher")]
fn graph_kernel_projection(
    edge: &RowEdgePattern,
    projections: &[RowProjection],
) -> Option<GraphKernelProjection> {
    let [projection] = projections else {
        return None;
    };
    match projection {
        RowProjection::NodeId { binding } => {
            if edge.dst.binding.as_deref() == Some(binding.as_str()) {
                Some(GraphKernelProjection::NodeId)
            } else {
                None
            }
        }
        RowProjection::CountAll => Some(GraphKernelProjection::CountAll),
        RowProjection::Property { .. }
        | RowProjection::Literal(_)
        | RowProjection::Aggregate { .. } => None,
    }
}

#[cfg(feature = "opencypher")]
fn graph_kernel_order_supported(
    edge: &RowEdgePattern,
    projection: GraphKernelProjection,
    projections: &[RowProjection],
    columns: &[QueryColumn],
    order_by: &[RowSort],
) -> Option<bool> {
    if order_by.is_empty() {
        return Some(true);
    }
    let [sort] = order_by else {
        return None;
    };
    match projection {
        GraphKernelProjection::NodeId => match &sort.expression {
            RowSortExpression::NodeId { binding } => {
                if edge.dst.binding.as_deref() == Some(binding.as_str()) {
                    Some(sort.ascending)
                } else {
                    None
                }
            }
            RowSortExpression::Column { name } => {
                let projection_idx = columns.iter().position(|column| column.name == *name)?;
                match projections.get(projection_idx) {
                    Some(RowProjection::NodeId { binding })
                        if edge.dst.binding.as_deref() == Some(binding.as_str()) =>
                    {
                        Some(sort.ascending)
                    }
                    _ => None,
                }
            }
            RowSortExpression::Property { .. } | RowSortExpression::CountAll => None,
        },
        GraphKernelProjection::CountAll => match &sort.expression {
            RowSortExpression::CountAll => Some(true),
            RowSortExpression::Column { name } => {
                if matches!(columns.first(), Some(column) if column.name == *name) {
                    Some(true)
                } else {
                    None
                }
            }
            RowSortExpression::NodeId { .. } | RowSortExpression::Property { .. } => None,
        },
    }
}

#[cfg(feature = "opencypher")]
fn streaming_neighbor_projection_supported(
    edge: &RowEdgePattern,
    projection: &RowProjection,
) -> bool {
    let RowProjection::NodeId { binding } = projection else {
        return false;
    };
    edge.src.binding.as_deref() == Some(binding.as_str())
        || edge.dst.binding.as_deref() == Some(binding.as_str())
}

#[cfg(feature = "opencypher")]
fn streaming_neighbor_order_supported(
    edge: &RowEdgePattern,
    projections: &[RowProjection],
    columns: &[QueryColumn],
    order_by: &[RowSort],
) -> bool {
    if order_by.is_empty() {
        return true;
    }
    let [sort] = order_by else {
        return false;
    };
    if !sort.ascending {
        return false;
    }
    match &sort.expression {
        RowSortExpression::NodeId { binding } => {
            edge.dst.binding.as_deref() == Some(binding.as_str())
        }
        RowSortExpression::Column { name } => match columns
            .iter()
            .position(|column| column.name == *name)
            .and_then(|idx| projections.get(idx))
        {
            Some(projection) => {
                matches!(
                    projection,
                    RowProjection::NodeId { binding }
                        if edge.dst.binding.as_deref() == Some(binding.as_str())
                )
            }
            None => false,
        },
        RowSortExpression::Property { .. } | RowSortExpression::CountAll => false,
    }
}

#[cfg(feature = "opencypher")]
fn streaming_neighbor_projection_values(
    edge: &RowEdgePattern,
    src: VertexId,
    dst: VertexId,
    projections: &[RowProjection],
) -> Result<Vec<QueryValue>> {
    let mut values = Vec::with_capacity(projections.len());
    for projection in projections {
        let RowProjection::NodeId { binding } = projection else {
            return Err(GraphError::UnsupportedQuery {
                reason: QueryFailureReason::Return,
                dialect: "OpenCypher",
                feature: "streaming neighbor page supports only node-id projections".to_string(),
            });
        };
        if edge.src.binding.as_deref() == Some(binding.as_str()) {
            values.push(QueryValue::VertexId(src));
        } else if edge.dst.binding.as_deref() == Some(binding.as_str()) {
            values.push(QueryValue::VertexId(dst));
        } else {
            return Err(GraphError::UnsupportedQuery {
                reason: QueryFailureReason::Other,
                dialect: "OpenCypher",
                feature: format!("streaming neighbor page cannot project unbound {binding}"),
            });
        }
    }
    Ok(values)
}

#[cfg(feature = "opencypher")]
fn query_result_set_to_output(parsed: &ParsedRowQuery, result_set: QueryResultSet) -> QueryOutput {
    match parsed.projections.as_slice() {
        [RowProjection::NodeId { .. }] => {
            let vertices: Option<Vec<VertexId>> = result_set
                .rows
                .iter()
                .map(|row| match row.values.as_slice() {
                    [QueryValue::VertexId(vertex)] => Some(*vertex),
                    _ => None,
                })
                .collect();
            vertices.map_or(QueryOutput::Rows(result_set), QueryOutput::Vertices)
        }
        [RowProjection::CountAll]
        | [RowProjection::Aggregate {
            function: RowAggregateFunction::Count,
            ..
        }] => match result_set.rows.as_slice() {
            [row] => match row.values.as_slice() {
                [QueryValue::Count(count)] => QueryOutput::Count(*count),
                _ => QueryOutput::Rows(result_set),
            },
            _ => QueryOutput::Rows(result_set),
        },
        _ => QueryOutput::Rows(result_set),
    }
}

#[cfg(feature = "opencypher")]
fn merge_opencypher_window(context: QueryContext, window: QueryWindow) -> Result<QueryContext> {
    if window.is_default() {
        return Ok(context);
    }
    if !context.result_window.is_default() && context.result_window != window {
        return Err(GraphError::UnsupportedQuery {
            reason: QueryFailureReason::InvalidRequest,
            dialect: "OpenCypher",
            feature: "query SKIP/LIMIT conflicts with QueryContext result window".to_string(),
        });
    }
    Ok(context.with_result_window(window.skip, window.limit))
}

#[cfg(feature = "opencypher")]
fn opencypher_outer_window(query: &ParsedRowQuery) -> QueryWindow {
    if query.union_arms.is_empty() {
        query.window
    } else {
        QueryWindow::default()
    }
}

#[cfg(feature = "opencypher")]
fn query_next_cursor(
    cursor_offset: u64,
    page_size: usize,
    has_next: bool,
) -> Result<Option<QueryCursorToken>> {
    if !has_next {
        return Ok(None);
    }
    Ok(Some(QueryCursorToken::new(
        cursor_offset
            .checked_add(u64::try_from(page_size).unwrap_or(u64::MAX))
            .ok_or(GraphError::AdmissionRejected {
                operation: "query_cursor_offset",
                actual: u64::MAX,
                limit: u64::MAX - 1,
            })?,
    )))
}

#[cfg(feature = "opencypher")]
fn parse_vertex_property_index_key(key: &str) -> Result<(String, String, String, VertexId)> {
    let parts: Vec<_> = key.split('/').collect();
    match parts.as_slice() {
        ["cell", cell_id, "vprop_idx", property, encoded, vertex_id] => Ok((
            (*cell_id).to_string(),
            (*property).to_string(),
            (*encoded).to_string(),
            parse_u64(key, vertex_id, "vertex_id")?,
        )),
        _ => Err(GraphError::CorruptValue {
            key: key.to_string(),
            reason: "expected vertex property index key".to_string(),
        }),
    }
}

#[cfg(feature = "opencypher")]
fn parse_edge_property_index_key(
    key: &str,
) -> Result<(String, String, String, String, VertexId, VertexId)> {
    let parts: Vec<_> = key.split('/').collect();
    match parts.as_slice() {
        ["cell", cell_id, "eprop_idx", edge_type, property, encoded, src, dst] => Ok((
            (*cell_id).to_string(),
            (*edge_type).to_string(),
            (*property).to_string(),
            (*encoded).to_string(),
            parse_u64(key, src, "src")?,
            parse_u64(key, dst, "dst")?,
        )),
        _ => Err(GraphError::CorruptValue {
            key: key.to_string(),
            reason: "expected edge property index key".to_string(),
        }),
    }
}

#[cfg(feature = "opencypher")]
#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct BindingRow {
    values: BTreeMap<String, VertexId>,
    null_values: BTreeSet<String>,
    relationships: BTreeMap<String, BoundRelationship>,
    null_relationships: BTreeSet<String>,
    relationship_metadata: BTreeMap<BoundRelationship, EdgeMetadata>,
    metadata: BTreeMap<String, VertexMetadata>,
}

#[cfg(feature = "opencypher")]
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct BoundRelationship {
    edge_type: String,
    src: VertexId,
    dst: VertexId,
    relationship_id: Option<RelationshipId>,
}

#[cfg(feature = "opencypher")]
fn mutation_return_rows(returning: &MutationReturn, bindings: &[BindingRow]) -> QueryResultSet {
    // Cypher's `count(x)` counts rows in which `x` is not null. It is not a
    // distinct count and not a tally of successful storage deletes: two rows
    // binding the same relationship count twice, and a row whose relationship
    // a concurrent writer already removed still counts. Deriving it from the
    // row set rather than from the delete result keeps that true, because the
    // delete result dedupes by relationship identity.
    let counted = bindings
        .iter()
        .filter(|row| row.is_bound(&returning.binding))
        .count() as u64;
    QueryResultSet::new(
        vec![returning.column.clone()],
        vec![QueryRow::new(vec![QueryValue::Count(counted)])],
    )
}

#[cfg(feature = "opencypher")]
fn bound_relationship_delete_mutation(
    context: &QueryContext,
    relationship: &BoundRelationship,
) -> EdgeMutation {
    EdgeMutation {
        cell_id: context.cell_id.clone(),
        edge_type: relationship.edge_type.clone(),
        src: relationship.src,
        dst: relationship.dst,
        idempotency_key: format!(
            "{}.delete.{}.{}.{}.{}",
            context.idempotency_key,
            relationship.edge_type,
            relationship.src,
            relationship.dst,
            relationship
                .relationship_id
                .map_or_else(|| "edge".to_string(), |id| id.to_string())
        ),
    }
}

#[cfg(feature = "opencypher")]
struct RelationshipPropertyLookup<'a> {
    cell_id: &'a str,
    edge_type: &'a str,
    src: VertexId,
    dst: VertexId,
    property: &'a str,
    value: &'a VertexPropertyValue,
    read_epoch: StorageSequence,
    budget: &'a QueryBudget,
}

#[cfg(feature = "opencypher")]
struct EncodedRelationshipPropertyLookup<'a> {
    cell_id: &'a str,
    edge_type: &'a str,
    src: VertexId,
    dst: VertexId,
    property: &'a str,
    encoded: &'a str,
    read_epoch: StorageSequence,
    latest_snapshot: bool,
    budget: &'a QueryBudget,
}

#[cfg(feature = "opencypher")]
struct RelationshipPropertyEdgeIndexLookup<'a> {
    cell_id: &'a str,
    edge_type: &'a str,
    property: &'a str,
    encoded: &'a str,
    src: VertexId,
    dst: VertexId,
    budget: &'a QueryBudget,
}

#[cfg(feature = "opencypher")]
struct OrderedRelationshipPropertyCandidate {
    encoded: String,
    src: VertexId,
    dst: VertexId,
    relationship_id: Option<RelationshipId>,
}

#[cfg(feature = "opencypher")]
#[derive(Clone, Copy)]
enum OrderedRelationshipPropertyCandidateSource {
    Edge(usize),
    Relationship(usize),
}

#[cfg(feature = "opencypher")]
fn ordered_edge_property_candidate(key: &[u8]) -> Result<OrderedRelationshipPropertyCandidate> {
    let key = String::from_utf8_lossy(key).into_owned();
    let (_, _, _, encoded, src, dst) = parse_edge_property_index_key(&key)?;
    Ok(OrderedRelationshipPropertyCandidate {
        encoded,
        src,
        dst,
        relationship_id: None,
    })
}

#[cfg(feature = "opencypher")]
fn ordered_relationship_property_candidate(
    key: &[u8],
) -> Result<OrderedRelationshipPropertyCandidate> {
    let key = String::from_utf8_lossy(key).into_owned();
    let (_, _, _, encoded, src, dst, relationship_id) =
        parse_relationship_property_index_key(&key)?;
    Ok(OrderedRelationshipPropertyCandidate {
        encoded,
        src,
        dst,
        relationship_id: Some(relationship_id),
    })
}

#[cfg(feature = "opencypher")]
fn ordered_relationship_property_source_candidate<'a>(
    source: OrderedRelationshipPropertyCandidateSource,
    edges: &'a [Option<OrderedRelationshipPropertyCandidate>],
    relationships: &'a [Option<OrderedRelationshipPropertyCandidate>],
) -> &'a OrderedRelationshipPropertyCandidate {
    match source {
        OrderedRelationshipPropertyCandidateSource::Edge(index) => edges[index]
            .as_ref()
            .expect("edge candidate source is populated"),
        OrderedRelationshipPropertyCandidateSource::Relationship(index) => relationships[index]
            .as_ref()
            .expect("relationship candidate source is populated"),
    }
}

#[cfg(feature = "opencypher")]
fn ordered_relationship_property_candidate_source(
    edges: &[Option<OrderedRelationshipPropertyCandidate>],
    relationships: &[Option<OrderedRelationshipPropertyCandidate>],
    ascending: bool,
) -> Result<Option<OrderedRelationshipPropertyCandidateSource>> {
    let mut selected = None;
    for source in (0..edges.len())
        .map(OrderedRelationshipPropertyCandidateSource::Edge)
        .chain(
            (0..relationships.len()).map(OrderedRelationshipPropertyCandidateSource::Relationship),
        )
    {
        let candidate = match source {
            OrderedRelationshipPropertyCandidateSource::Edge(index) => edges[index].as_ref(),
            OrderedRelationshipPropertyCandidateSource::Relationship(index) => {
                relationships[index].as_ref()
            }
        };
        let Some(candidate) = candidate else {
            continue;
        };
        let Some(current_source) = selected else {
            selected = Some(source);
            continue;
        };
        let current =
            ordered_relationship_property_source_candidate(current_source, edges, relationships);
        let ordering = compare_encoded_property_order(&candidate.encoded, &current.encoded)?;
        if (ascending && ordering == std::cmp::Ordering::Less)
            || (!ascending && ordering == std::cmp::Ordering::Greater)
        {
            selected = Some(source);
        }
    }
    Ok(selected)
}

#[cfg(feature = "opencypher")]
fn compare_encoded_property_order(left: &str, right: &str) -> Result<std::cmp::Ordering> {
    let left_type = left.as_bytes().first().copied();
    let right_type = right.as_bytes().first().copied();
    if left_type == right_type {
        return Ok(left.cmp(right));
    }
    let left_rank = encoded_property_value_rank(left)?;
    let right_rank = encoded_property_value_rank(right)?;
    if left_rank != right_rank {
        return Ok(left_rank.cmp(&right_rank));
    }
    let left = decode_numeric_property_index_key(left)?;
    let right = decode_numeric_property_index_key(right)?;
    numeric_property_order(&left, &right).ok_or_else(|| GraphError::CorruptValue {
        key: "relationship property index".to_string(),
        reason: "numeric property key decoded to a non-numeric value".to_string(),
    })
}

#[cfg(feature = "opencypher")]
fn encoded_property_value_rank(encoded: &str) -> Result<u8> {
    match encoded.as_bytes().first().copied() {
        Some(b'b') => Ok(0),
        Some(b'i' | b'j' | b'n') => Ok(1),
        Some(b's') => Ok(2),
        _ => Err(GraphError::CorruptValue {
            key: encoded.to_string(),
            reason: "unknown property index value type".to_string(),
        }),
    }
}

#[cfg(feature = "opencypher")]
fn decode_numeric_property_index_key(encoded: &str) -> Result<VertexPropertyValue> {
    let Some((kind, payload)) = encoded.split_at_checked(1) else {
        return Err(GraphError::CorruptValue {
            key: encoded.to_string(),
            reason: "empty numeric property index value".to_string(),
        });
    };
    let corrupt = |reason: String| GraphError::CorruptValue {
        key: encoded.to_string(),
        reason,
    };
    match kind {
        "i" => payload
            .parse::<u64>()
            .map(VertexPropertyValue::Integer)
            .map_err(|error| corrupt(format!("invalid unsigned property index value: {error}"))),
        "j" => u64::from_str_radix(payload, 16)
            .map(|value| VertexPropertyValue::SignedInteger((value ^ (1_u64 << 63)) as i64))
            .map_err(|error| corrupt(format!("invalid signed property index value: {error}"))),
        "n" => u64::from_str_radix(payload, 16)
            .map(|sortable| {
                let bits = if sortable & (1_u64 << 63) == 0 {
                    !sortable
                } else {
                    sortable ^ (1_u64 << 63)
                };
                VertexPropertyValue::Float(QueryFloat(f64::from_bits(bits)))
            })
            .map_err(|error| corrupt(format!("invalid float property index value: {error}"))),
        _ => Err(corrupt("property index value is not numeric".to_string())),
    }
}

#[cfg(feature = "opencypher")]
struct OrderedRelationshipPropertyIndexSpec<'a> {
    edge: &'a RowEdgePattern,
    property: &'a str,
    predicate: &'a RowPredicate,
    ascending: bool,
    limit: usize,
}

#[cfg(feature = "opencypher")]
impl BindingRow {
    /// Whether `binding` names a non-null vertex or relationship in this row.
    ///
    /// An OPTIONAL MATCH that found nothing records the name in
    /// `null_values`/`null_relationships` rather than omitting it, so a
    /// presence check has to consult the bound maps, not the null sets.
    fn is_bound(&self, binding: &str) -> bool {
        self.values.contains_key(binding) || self.relationships.contains_key(binding)
    }

    fn from_node(pattern: &RowNodePattern, vertex_id: VertexId) -> Option<Self> {
        let mut row = Self::default();
        if !row.bind(pattern.binding.as_deref(), vertex_id) {
            return None;
        }
        Some(row)
    }

    fn from_edge(pattern: &RowEdgePattern, src: VertexId, dst: VertexId) -> Option<Self> {
        let relationship = BoundRelationship {
            edge_type: pattern.edge_type.clone(),
            src,
            dst,
            relationship_id: None,
        };
        Self::from_relationship(pattern, relationship, EdgeMetadata::default())
    }

    fn from_relationship(
        pattern: &RowEdgePattern,
        relationship: BoundRelationship,
        metadata: EdgeMetadata,
    ) -> Option<Self> {
        let mut row = Self::default();
        if !row.bind(pattern.src.binding.as_deref(), relationship.src) {
            return None;
        }
        if !row.bind(pattern.dst.binding.as_deref(), relationship.dst) {
            return None;
        }
        if !row.bind_relationship(pattern.binding.as_deref(), relationship.clone()) {
            return None;
        }
        row.relationship_metadata.insert(relationship, metadata);
        Some(row)
    }

    fn bind(&mut self, binding: Option<&str>, value: VertexId) -> bool {
        let Some(binding) = binding else {
            return true;
        };
        if self.null_values.contains(binding) {
            return false;
        }
        match self.values.get(binding) {
            Some(existing) => *existing == value,
            None => {
                self.values.insert(binding.to_string(), value);
                true
            }
        }
    }

    fn bind_relationship(&mut self, binding: Option<&str>, value: BoundRelationship) -> bool {
        let Some(binding) = binding else {
            return true;
        };
        if self.null_relationships.contains(binding) {
            return false;
        }
        match self.relationships.get(binding) {
            Some(existing) => *existing == value,
            None => {
                self.relationships.insert(binding.to_string(), value);
                true
            }
        }
    }

    fn get(&self, binding: &str) -> Result<VertexId> {
        if self.null_values.contains(binding) {
            return Err(GraphError::UnsupportedQuery {
                reason: QueryFailureReason::Other,
                dialect: "OpenCypher",
                feature: format!("variable {binding} is null"),
            });
        }
        self.values
            .get(binding)
            .copied()
            .ok_or_else(|| GraphError::UnsupportedQuery {
                reason: QueryFailureReason::Other,
                dialect: "OpenCypher",
                feature: format!("unbound variable {binding}"),
            })
    }

    fn get_optional(&self, binding: &str) -> Result<Option<VertexId>> {
        if self.null_values.contains(binding) {
            return Ok(None);
        }
        self.values
            .get(binding)
            .copied()
            .map(Some)
            .ok_or_else(|| GraphError::UnsupportedQuery {
                reason: QueryFailureReason::Other,
                dialect: "OpenCypher",
                feature: format!("unbound variable {binding}"),
            })
    }

    fn mark_optional_group_nulls(&mut self, group: &RowMatchGroup) {
        for pattern in &group.patterns {
            self.mark_optional_pattern_nulls(pattern);
        }
    }

    fn mark_optional_pattern_nulls(&mut self, pattern: &RowPattern) {
        match pattern {
            RowPattern::Node(node) => self.mark_optional_node_nulls(node),
            RowPattern::Edge(edge) => {
                if let Some(binding) = &edge.binding {
                    if !self.relationships.contains_key(binding) {
                        self.null_relationships.insert(binding.clone());
                    }
                }
                self.mark_optional_node_nulls(&edge.src);
                self.mark_optional_node_nulls(&edge.dst);
            }
        }
    }

    fn mark_optional_node_nulls(&mut self, node: &RowNodePattern) {
        if let Some(binding) = &node.binding {
            if !self.values.contains_key(binding) {
                self.null_values.insert(binding.clone());
            }
        }
    }

    fn join(&self, other: &Self) -> Option<Self> {
        let mut joined = self.clone();
        for (binding, value) in &other.values {
            if !joined.bind(Some(binding), *value) {
                return None;
            }
        }
        for binding in &other.null_values {
            if joined.values.contains_key(binding) {
                return None;
            }
            joined.null_values.insert(binding.clone());
        }
        for (binding, metadata) in &other.metadata {
            match joined.metadata.get(binding) {
                Some(existing) if existing != metadata => return None,
                Some(_) => {}
                None => {
                    joined.metadata.insert(binding.clone(), metadata.clone());
                }
            }
        }
        for (binding, relationship) in &other.relationships {
            match joined.relationships.get(binding) {
                Some(existing) if existing != relationship => return None,
                Some(_) => {}
                None => {
                    joined
                        .relationships
                        .insert(binding.clone(), relationship.clone());
                }
            }
        }
        for binding in &other.null_relationships {
            if joined.relationships.contains_key(binding) {
                return None;
            }
            joined.null_relationships.insert(binding.clone());
        }
        for (relationship, metadata) in &other.relationship_metadata {
            match joined.relationship_metadata.get(relationship) {
                Some(existing) if existing.properties.is_empty() => {
                    joined
                        .relationship_metadata
                        .insert(relationship.clone(), metadata.clone());
                }
                Some(existing) if metadata.properties.is_empty() || existing == metadata => {}
                Some(_) => return None,
                None => {
                    joined
                        .relationship_metadata
                        .insert(relationship.clone(), metadata.clone());
                }
            }
        }
        Some(joined)
    }
}

#[cfg(feature = "opencypher")]
fn common_binding_row_bound_names(rows: &[BindingRow]) -> BTreeSet<String> {
    let Some(first) = rows.first() else {
        return BTreeSet::new();
    };
    let mut common = binding_row_bound_names(first);
    for row in &rows[1..] {
        let bound = binding_row_bound_names(row);
        common.retain(|binding| bound.contains(binding));
    }
    common
}

#[cfg(feature = "opencypher")]
fn binding_rows_bound_names_union(rows: &[BindingRow]) -> BTreeSet<String> {
    rows.iter().flat_map(binding_row_bound_names).collect()
}

#[cfg(feature = "opencypher")]
fn binding_row_bound_names(row: &BindingRow) -> BTreeSet<String> {
    row.values
        .keys()
        .chain(row.relationships.keys())
        .cloned()
        .collect()
}

#[cfg(feature = "opencypher")]
fn row_pattern_bound_names(pattern: &RowPattern) -> BTreeSet<String> {
    match pattern {
        RowPattern::Node(node) => node.binding.iter().cloned().collect(),
        RowPattern::Edge(edge) => {
            let mut names = BTreeSet::new();
            if let Some(binding) = &edge.src.binding {
                names.insert(binding.clone());
            }
            if let Some(binding) = &edge.dst.binding {
                names.insert(binding.clone());
            }
            names
        }
    }
}

#[cfg(feature = "opencypher")]
fn binding_row_join_key(row: &BindingRow, bindings: &[String]) -> Option<Vec<VertexId>> {
    let mut key = Vec::with_capacity(bindings.len());
    for binding in bindings {
        key.push(*row.values.get(binding)?);
    }
    Some(key)
}

#[cfg(feature = "opencypher")]
fn hash_joinable_pattern(pattern: &RowPattern) -> bool {
    match pattern {
        RowPattern::Node(node) => {
            node.id.is_none() && (!node.labels.is_empty() || node_has_metadata_constraints(node))
        }
        RowPattern::Edge(edge) => edge.hop_range.is_none() && !edge.properties.is_empty(),
    }
}

#[cfg(feature = "opencypher")]
#[derive(Clone, Debug, Eq, PartialEq)]
struct ProjectedQueryRow {
    row: QueryRow,
    sort_keys: Vec<QueryValue>,
}

#[cfg(feature = "opencypher")]
struct OrderedWindowRow<'a> {
    projected: ProjectedQueryRow,
    ordinal: usize,
    order_by: &'a [RowSort],
}

#[cfg(feature = "opencypher")]
impl Ord for OrderedWindowRow<'_> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        compare_projected_rows(&self.projected, &other.projected, self.order_by)
            .then_with(|| self.ordinal.cmp(&other.ordinal))
    }
}

#[cfg(feature = "opencypher")]
impl PartialOrd for OrderedWindowRow<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

#[cfg(feature = "opencypher")]
impl PartialEq for OrderedWindowRow<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other).is_eq()
    }
}

#[cfg(feature = "opencypher")]
impl Eq for OrderedWindowRow<'_> {}

// A max-heap retains exactly the best window. Re-sorting K rows after each
// 16-row hydration batch makes large candidate lookups unnecessarily quadratic.
// The input ordinal preserves the stable-sort behavior for equal sort keys.
#[cfg(feature = "opencypher")]
struct OrderedRowWindow<'a> {
    rows: std::collections::BinaryHeap<OrderedWindowRow<'a>>,
    limit: usize,
    next_ordinal: usize,
    order_by: &'a [RowSort],
}

#[cfg(feature = "opencypher")]
impl<'a> OrderedRowWindow<'a> {
    fn new(limit: usize, order_by: &'a [RowSort]) -> Self {
        Self {
            rows: std::collections::BinaryHeap::new(),
            limit,
            next_ordinal: 0,
            order_by,
        }
    }

    fn len(&self) -> usize {
        self.rows.len()
    }

    fn push(&mut self, row: QueryRow, sort_keys: Vec<QueryValue>) {
        let entry = OrderedWindowRow {
            projected: ProjectedQueryRow { row, sort_keys },
            ordinal: self.next_ordinal,
            order_by: self.order_by,
        };
        self.next_ordinal += 1;
        if self.rows.len() < self.limit {
            self.rows.push(entry);
        } else if let Some(mut worst) = self.rows.peek_mut() {
            if entry < *worst {
                *worst = entry;
            }
        }
    }

    fn into_rows(self) -> Vec<ProjectedQueryRow> {
        self.rows
            .into_sorted_vec()
            .into_iter()
            .map(|entry| entry.projected)
            .collect()
    }
}

#[cfg(all(test, feature = "opencypher"))]
#[test]
fn ordered_row_window_matches_stable_sort_with_ties() {
    for ascending in [false, true] {
        let order = [RowSort {
            expression: RowSortExpression::CountAll,
            ascending,
        }];
        for limit in [0, 1, 17, 5000] {
            let mut window = OrderedRowWindow::new(limit, &order);
            let mut expected = Vec::new();
            for id in 0..10_000_u64 {
                let row = QueryRow::new(vec![QueryValue::VertexId(id)]);
                let sort_keys = vec![QueryValue::Count((id * 97) % 137)];
                window.push(row.clone(), sort_keys.clone());
                expected.push(ProjectedQueryRow { row, sort_keys });
                assert!(window.len() <= limit);
            }
            expected.sort_by(|left, right| compare_projected_rows(left, right, &order));
            expected.truncate(limit);
            assert_eq!(window.into_rows(), expected);
        }
    }
}

#[cfg(feature = "opencypher")]
fn push_projected_query_row(
    projected: &mut Vec<ProjectedQueryRow>,
    row: QueryRow,
    sort_keys: Vec<QueryValue>,
) {
    projected.push(ProjectedQueryRow { row, sort_keys });
}

#[cfg(feature = "opencypher")]
#[derive(Clone, Debug, Eq, PartialEq)]
enum RowScalarValue {
    Value(VertexPropertyValue),
    Missing,
}

#[cfg(feature = "opencypher")]
#[derive(Clone, Debug, Eq, PartialEq)]
enum AggregateAccumulator {
    CountAll(u64),
    CountExpression(u64),
    Sum(u128),
    Avg { sum: u128, count: u64 },
    Collect(Vec<QueryValue>),
}

#[cfg(feature = "opencypher")]
fn constrain_row_pattern(pattern: &RowPattern, row: &BindingRow) -> Result<Option<RowPattern>> {
    Ok(Some(match pattern {
        RowPattern::Node(node) => {
            let Some(node) = constrain_row_node_pattern(node, row)? else {
                return Ok(None);
            };
            RowPattern::Node(node)
        }
        RowPattern::Edge(edge) => {
            if let Some(binding) = &edge.binding {
                if row.null_relationships.contains(binding) {
                    return Ok(None);
                }
            }
            let Some(src) = constrain_row_node_pattern(&edge.src, row)? else {
                return Ok(None);
            };
            let Some(dst) = constrain_row_node_pattern(&edge.dst, row)? else {
                return Ok(None);
            };
            RowPattern::Edge(RowEdgePattern {
                binding: edge.binding.clone(),
                edge_type: edge.edge_type.clone(),
                src,
                dst,
                properties: edge.properties.clone(),
                hop_range: edge.hop_range,
                direction: edge.direction,
            })
        }
    }))
}

#[cfg(feature = "opencypher")]
fn constrain_row_node_pattern(
    node: &RowNodePattern,
    row: &BindingRow,
) -> Result<Option<RowNodePattern>> {
    let Some(binding) = &node.binding else {
        return Ok(Some(node.clone()));
    };
    if row.null_values.contains(binding) {
        return Ok(None);
    }
    let Some(bound_id) = row.values.get(binding).copied() else {
        return Ok(Some(node.clone()));
    };
    if matches!(node.id, Some(pattern_id) if pattern_id != bound_id) {
        return Ok(None);
    }
    let mut constrained = node.clone();
    constrained.id = Some(bound_id);
    if let Some(metadata) = row.metadata.get(binding) {
        if !vertex_metadata_matches(metadata, &constrained) {
            return Ok(None);
        }
    }
    Ok(Some(constrained))
}

#[cfg(feature = "opencypher")]
fn row_matches_edge_pattern(row: &BindingRow, pattern: &RowEdgePattern) -> Result<bool> {
    if !(row_matches_node(row, &pattern.src)? && row_matches_node(row, &pattern.dst)?) {
        return Ok(false);
    }
    if pattern.properties.is_empty() {
        return Ok(true);
    }
    let relationship = relationship_identity_for_pattern(row, pattern)?;
    let Some(metadata) = row.relationship_metadata.get(&relationship) else {
        return Err(GraphError::UnsupportedQuery {
            reason: QueryFailureReason::Other,
            dialect: "OpenCypher",
            feature: "relationship metadata was not hydrated".to_string(),
        });
    };
    Ok(pattern.properties.iter().all(|(property, value)| {
        metadata
            .properties
            .get(property)
            .is_some_and(|existing| vertex_property_values_equal(existing, value))
    }))
}

#[cfg(feature = "opencypher")]
fn relationship_metadata_matches(metadata: &EdgeMetadata, pattern: &RowEdgePattern) -> bool {
    pattern.properties.iter().all(|(property, value)| {
        metadata
            .properties
            .get(property)
            .is_some_and(|existing| vertex_property_values_equal(existing, value))
    })
}

#[cfg(feature = "opencypher")]
fn relationship_identity_for_pattern(
    row: &BindingRow,
    pattern: &RowEdgePattern,
) -> Result<BoundRelationship> {
    if let Some(binding) = &pattern.binding {
        return row.relationships.get(binding).cloned().ok_or_else(|| {
            GraphError::UnsupportedQuery {
                reason: QueryFailureReason::Other,
                dialect: "OpenCypher",
                feature: format!("unbound relationship {binding}"),
            }
        });
    }
    if let (Some(src_binding), Some(dst_binding)) = (
        pattern.src.binding.as_deref(),
        pattern.dst.binding.as_deref(),
    ) {
        return Ok(BoundRelationship {
            edge_type: pattern.edge_type.clone(),
            src: row.get(src_binding)?,
            dst: row.get(dst_binding)?,
            relationship_id: None,
        });
    }
    let mut matches = row
        .relationship_metadata
        .keys()
        .filter(|relationship| relationship.edge_type == pattern.edge_type);
    let Some(relationship) = matches.next().cloned() else {
        return Err(GraphError::UnsupportedQuery {
            reason: QueryFailureReason::Other,
            dialect: "OpenCypher",
            feature: "relationship pattern has no bound identity".to_string(),
        });
    };
    if matches.next().is_some() {
        return Err(GraphError::UnsupportedQuery {
            reason: QueryFailureReason::Pattern,
            dialect: "OpenCypher",
            feature: "anonymous relationship property pattern is ambiguous".to_string(),
        });
    }
    Ok(relationship)
}

#[cfg(feature = "opencypher")]
fn row_matches_node(row: &BindingRow, node: &RowNodePattern) -> Result<bool> {
    let Some(binding) = &node.binding else {
        return Ok(true);
    };
    if row.null_values.contains(binding) {
        return Ok(false);
    }
    if matches!(node.id, Some(id) if row.get(binding)? != id) {
        return Ok(false);
    }
    if !node_has_metadata_constraints(node) {
        return Ok(true);
    }
    let Some(metadata) = row.metadata.get(binding) else {
        return Err(GraphError::UnsupportedQuery {
            reason: QueryFailureReason::Other,
            dialect: "OpenCypher",
            feature: format!("metadata for bound variable {binding} was not hydrated"),
        });
    };
    Ok(vertex_metadata_matches(metadata, node))
}

#[cfg(feature = "opencypher")]
fn node_has_metadata_constraints(node: &RowNodePattern) -> bool {
    !node.labels.is_empty() || node.properties.keys().any(|property| property != "id")
}

#[cfg(feature = "opencypher")]
fn vertex_metadata_matches(metadata: &VertexMetadata, node: &RowNodePattern) -> bool {
    node.labels
        .iter()
        .all(|label| metadata.labels.contains(label))
        && node
            .properties
            .iter()
            .filter(|(property, _)| property.as_str() != "id")
            .all(|(property, value)| {
                metadata
                    .properties
                    .get(property)
                    .is_some_and(|existing| vertex_property_values_equal(existing, value))
            })
}

#[cfg(feature = "opencypher")]
fn row_predicate_matches(row: &BindingRow, predicate: &RowPredicate) -> Result<bool> {
    Ok(eval_row_predicate(row, predicate)? == Some(true))
}

/// None is Cypher's unknown truth value. Collapse it only at WHERE, never
/// inside a boolean expression: NOT unknown must remain unknown.
#[cfg(feature = "opencypher")]
fn eval_row_predicate(row: &BindingRow, predicate: &RowPredicate) -> Result<Option<bool>> {
    Ok(match predicate {
        RowPredicate::Compare { left, op, right } => compare_row_values(
            eval_row_expression(row, left)?,
            *op,
            eval_row_expression(row, right)?,
        )?,
        RowPredicate::StartsWith { expression, prefix } => {
            match eval_row_expression(row, expression)? {
                RowScalarValue::Value(VertexPropertyValue::String(value)) => {
                    Some(value.starts_with(prefix))
                }
                RowScalarValue::Value(_) => Some(false),
                RowScalarValue::Missing => None,
            }
        }
        // Even null IN [] is false: there are no elements that could match.
        RowPredicate::In { values, .. } if values.is_empty() => Some(false),
        RowPredicate::In { expression, values } => match eval_row_expression(row, expression)? {
            RowScalarValue::Value(value) => Some(
                values
                    .iter()
                    .any(|candidate| vertex_property_values_equal(candidate, &value)),
            ),
            RowScalarValue::Missing => None,
        },
        RowPredicate::And(left, right) => match eval_row_predicate(row, left)? {
            Some(false) => Some(false),
            Some(true) => eval_row_predicate(row, right)?,
            None => match eval_row_predicate(row, right)? {
                Some(false) => Some(false),
                _ => None,
            },
        },
        RowPredicate::Or(left, right) => match eval_row_predicate(row, left)? {
            Some(true) => Some(true),
            Some(false) => eval_row_predicate(row, right)?,
            None => match eval_row_predicate(row, right)? {
                Some(true) => Some(true),
                _ => None,
            },
        },
        RowPredicate::Not(inner) => eval_row_predicate(row, inner)?.map(|value| !value),
        // The test for unknown is itself never unknown: a missing property is
        // the only null a row holds.
        RowPredicate::IsNull {
            expression,
            negated,
        } => Some(
            matches!(
                eval_row_expression(row, expression)?,
                RowScalarValue::Missing
            ) != *negated,
        ),
        // The binding itself: null exactly when nothing is bound to it, which
        // an OPTIONAL MATCH that did not match records explicitly. A name the
        // row never bound is null too -- there is nothing there either.
        RowPredicate::BindingIsNull { binding, negated } => {
            let bound = !row.null_values.contains(binding)
                && !row.null_relationships.contains(binding)
                && (row.values.contains_key(binding) || row.relationships.contains_key(binding));
            Some(!bound != *negated)
        }
    })
}

#[cfg(feature = "opencypher")]
fn predicate_guarantees_string_property(
    predicate: &RowPredicate,
    binding: &str,
    property: &str,
) -> bool {
    match predicate {
        RowPredicate::StartsWith {
            expression:
                RowExpression::Property {
                    binding: candidate_binding,
                    property: candidate_property,
                },
            ..
        } => candidate_binding == binding && candidate_property == property,
        RowPredicate::StartsWith { .. } => false,
        RowPredicate::And(left, right) => {
            predicate_guarantees_string_property(left, binding, property)
                || predicate_guarantees_string_property(right, binding, property)
        }
        RowPredicate::Or(left, right) => {
            predicate_guarantees_string_property(left, binding, property)
                && predicate_guarantees_string_property(right, binding, property)
        }
        RowPredicate::Compare { .. }
        | RowPredicate::In { .. }
        | RowPredicate::Not(_)
        | RowPredicate::IsNull { .. }
        | RowPredicate::BindingIsNull { .. } => false,
    }
}

#[cfg(feature = "opencypher")]
fn string_property_lower_bound(
    predicate: &RowPredicate,
    binding: &str,
    property: &str,
) -> Option<String> {
    fn direct_bound(
        left: &RowExpression,
        op: RowComparisonOp,
        right: &RowExpression,
        binding: &str,
        property: &str,
    ) -> Option<String> {
        let is_property = |expression: &RowExpression| {
            matches!(
                expression,
                RowExpression::Property {
                    binding: candidate_binding,
                    property: candidate_property,
                } if candidate_binding == binding && candidate_property == property
            )
        };
        let string_literal = |expression: &RowExpression| match expression {
            RowExpression::Literal(VertexPropertyValue::String(value)) => Some(value.clone()),
            _ => None,
        };

        if is_property(left)
            && matches!(
                op,
                RowComparisonOp::Eq | RowComparisonOp::Gt | RowComparisonOp::Gte
            )
        {
            return string_literal(right);
        }
        if is_property(right)
            && matches!(
                op,
                RowComparisonOp::Eq | RowComparisonOp::Lt | RowComparisonOp::Lte
            )
        {
            return string_literal(left);
        }
        None
    }

    match predicate {
        RowPredicate::Compare { left, op, right } => {
            direct_bound(left, *op, right, binding, property)
        }
        RowPredicate::StartsWith { expression, prefix } => matches!(
            expression,
            RowExpression::Property {
                binding: candidate_binding,
                property: candidate_property,
            } if candidate_binding == binding && candidate_property == property
        )
        .then(|| prefix.clone()),
        // AND may use either proven lower bound, and the stronger bound is
        // safe. OR needs a bound from both branches; the weaker one is the
        // earliest value that either branch can admit.
        RowPredicate::And(left, right) => {
            match (
                string_property_lower_bound(left, binding, property),
                string_property_lower_bound(right, binding, property),
            ) {
                (Some(left), Some(right)) => Some(left.max(right)),
                (left @ Some(_), None) | (None, left @ Some(_)) => left,
                (None, None) => None,
            }
        }
        RowPredicate::Or(left, right) => {
            let left = string_property_lower_bound(left, binding, property)?;
            let right = string_property_lower_bound(right, binding, property)?;
            Some(left.min(right))
        }
        // `IN` deliberately yields no lower bound. The smallest value in the
        // list would be a sound one, but an ordered scan from it is strictly
        // worse than what `row_predicate_property_equality_constraint` already
        // gives this predicate: a multi-value seek that visits only the listed
        // values. Offering a bound here would let the planner pick the weaker
        // access path.
        RowPredicate::In { .. } => None,
        RowPredicate::Not(_) | RowPredicate::IsNull { .. } | RowPredicate::BindingIsNull { .. } => {
            None
        }
    }
}

#[cfg(feature = "opencypher")]
pub(super) struct OrderedStringVertexIndexSpec<'a> {
    pub(super) node: &'a RowNodePattern,
    pub(super) binding: &'a str,
    pub(super) property: &'a str,
    pub(super) predicate: &'a RowPredicate,
    pub(super) ascending: bool,
    pub(super) limit: usize,
}

#[cfg(feature = "opencypher")]
pub(super) fn ordered_string_vertex_index_spec<'a>(
    query: &'a ParsedRowQuery,
    window: QueryWindow,
) -> Option<OrderedStringVertexIndexSpec<'a>> {
    let [RowPattern::Node(node)] = query.patterns.as_slice() else {
        return None;
    };
    let binding = node.binding.as_deref()?;
    let first_sort = query.order_by.first()?;
    let RowSortExpression::Property {
        binding: sort_binding,
        property,
    } = &first_sort.expression
    else {
        return None;
    };
    let limit = window.limit?;
    let predicate = query.predicate.as_ref()?;
    let simple_match_group = query.pattern_groups.is_empty()
        || matches!(
            query.pattern_groups.as_slice(),
            [group]
                if !group.optional
                    && group.patterns == query.patterns
                    && group.predicate.as_ref() == query.predicate.as_ref()
        );
    if query.distinct
        || !simple_match_group
        || !query.union_arms.is_empty()
        || row_projections_have_aggregates(&query.projections)
        || sort_binding != binding
        || !predicate_guarantees_string_property(predicate, binding, property)
    {
        return None;
    }
    Some(OrderedStringVertexIndexSpec {
        node,
        binding,
        property,
        predicate,
        ascending: first_sort.ascending,
        limit,
    })
}

/// One binding's property pinned to a set of literal values by a WHERE clause.
///
/// The binding may name a relationship or a node — the extractor below reads
/// the predicate, not the pattern, so nothing here is edge-specific. Two
/// callers use it against different pattern kinds: the relationship index plan
/// in `query_optimizer.rs` and the vertex equality pushdown beside it.
#[cfg(feature = "opencypher")]
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct RowPredicatePropertyEqualityConstraint {
    pub(super) binding: String,
    pub(super) property: String,
    pub(super) values: Vec<VertexPropertyValue>,
}

#[cfg(feature = "opencypher")]
enum NodeEqualityProbe {
    PredicateEqualities(usize),
    Ordered(usize),
}

/// Every equality constraint a WHERE predicate proves, not just the first.
///
/// [`row_predicate_property_equality_constraint`] stops at the first thing it
/// can prove, which is the right answer for a caller that can use exactly one
/// — the edge property index is keyed on a single property. It is the wrong
/// answer for a caller that wants the *best* one, because `AND` is
/// commutative and the extractor is not: `WHERE s.tier = 'common' AND
/// s.app_external_id = 'ext-3'` yielded the tier, and writing the same two
/// terms the other way round yielded the id. Worse, a leading term the
/// extractor can prove but a caller cannot *use* — an `OR` folded to two
/// values — masked an indexable term to its right entirely, and the query
/// went back to a label scan.
///
/// Descending only `And` is what keeps this sound. Both sides of an `And`
/// hold, so every constraint either side proves is a constraint on the row.
/// An `Or` proves only what the singular extractor already folds into one
/// multi-value constraint, and splitting it would claim each branch
/// separately, which is exactly the row-dropping mistake the singular
/// extractor refuses to make.
#[cfg(feature = "opencypher")]
pub(super) fn row_predicate_property_equality_constraints(
    predicate: &RowPredicate,
) -> Vec<RowPredicatePropertyEqualityConstraint> {
    match predicate {
        RowPredicate::And(left, right) => {
            let mut constraints = row_predicate_property_equality_constraints(left);
            constraints.extend(row_predicate_property_equality_constraints(right));
            constraints
        }
        other => row_predicate_property_equality_constraint(other)
            .into_iter()
            .collect(),
    }
}

/// Pull an equality constraint out of a WHERE predicate, if one is there.
///
/// Accepts `binding.property = literal` in either operand order, descends AND
/// chains taking the first constraint it can prove, and folds an OR of the
/// same binding and property into one multi-value constraint. Everything else
/// — NOT, ordered comparisons, STARTS WITH, an OR that spans two properties —
/// returns `None`, because the index seek it would authorise could drop rows
/// the predicate keeps.
#[cfg(feature = "opencypher")]
pub(super) fn row_predicate_property_equality_constraint(
    predicate: &RowPredicate,
) -> Option<RowPredicatePropertyEqualityConstraint> {
    match predicate {
        RowPredicate::Compare {
            left,
            op: RowComparisonOp::Eq,
            right,
        } => row_property_literal_equality(left, right)
            .or_else(|| row_property_literal_equality(right, left)),
        RowPredicate::And(left, right) => row_predicate_property_equality_constraint(left)
            .or_else(|| row_predicate_property_equality_constraint(right)),
        RowPredicate::Or(left, right) => {
            let mut left = row_predicate_property_equality_constraint(left)?;
            let right = row_predicate_property_equality_constraint(right)?;
            if left.binding != right.binding || left.property != right.property {
                return None;
            }
            for value in right.values {
                if !left.values.contains(&value) {
                    left.values.push(value);
                }
            }
            Some(left)
        }
        // The same multi-value seek the OR-fold above produces. An empty list
        // yields no constraint on purpose: a zero-value seek is not a narrower
        // plan, and letting it through would authorise an index lookup that
        // returns nothing while the predicate still has to reject every row.
        RowPredicate::In {
            expression: RowExpression::Property { binding, property },
            values,
        } if !values.is_empty() => Some(RowPredicatePropertyEqualityConstraint {
            binding: binding.clone(),
            property: property.clone(),
            values: values.clone(),
        }),
        RowPredicate::Compare { .. }
        | RowPredicate::StartsWith { .. }
        | RowPredicate::In { .. }
        | RowPredicate::Not(_)
        | RowPredicate::IsNull { .. }
        | RowPredicate::BindingIsNull { .. } => None,
    }
}

#[cfg(feature = "opencypher")]
fn row_property_literal_equality(
    property_expression: &RowExpression,
    literal_expression: &RowExpression,
) -> Option<RowPredicatePropertyEqualityConstraint> {
    let RowExpression::Property { binding, property } = property_expression else {
        return None;
    };
    let RowExpression::Literal(value) = literal_expression else {
        return None;
    };
    Some(RowPredicatePropertyEqualityConstraint {
        binding: binding.clone(),
        property: property.clone(),
        values: vec![value.clone()],
    })
}

/// Vertex ids a WHERE predicate pins for one binding: `n.id = 7`,
/// `n.id IN [1, 2]`, `n.id IN $ids`.
///
/// `n.id` is not a property. `node_id_expression_binding` routes every `x.id`
/// to `RowExpression::NodeId`, so the property-equality extractor above never
/// sees it, and an unlabelled `MATCH (n) WHERE n.id IN [1, 2]` reached the node
/// matcher with no candidate source at all and failed as an unsupported query.
/// These ids are a candidate set, never a decision: the group predicate still
/// runs over the matched rows afterwards.
#[cfg(feature = "opencypher")]
fn row_predicate_node_id_values(predicate: &RowPredicate) -> Option<(String, Vec<VertexId>)> {
    match predicate {
        RowPredicate::Compare {
            left,
            op: RowComparisonOp::Eq,
            right,
        } => {
            node_id_literal_equality(left, right).or_else(|| node_id_literal_equality(right, left))
        }
        RowPredicate::In {
            expression: RowExpression::NodeId { binding },
            values,
        } => {
            let ids = vertex_id_literals(values)?;
            Some((binding.clone(), ids))
        }
        // Both sides of an `And` hold, so either side's ids already contain
        // every row the whole predicate keeps. Seeking one side and filtering
        // with the rest is the same bargain the property extractor makes.
        RowPredicate::And(left, right) => {
            row_predicate_node_id_values(left).or_else(|| row_predicate_node_id_values(right))
        }
        // Only one side of an `Or` need hold, so one side's ids are not a
        // superset and seeding from them would drop the other side's rows. A
        // union over the same binding is a superset; anything else falls back
        // to the ordinary plan.
        RowPredicate::Or(left, right) => {
            let (binding, mut ids) = row_predicate_node_id_values(left)?;
            let (right_binding, right_ids) = row_predicate_node_id_values(right)?;
            if binding != right_binding {
                return None;
            }
            for id in right_ids {
                if !ids.contains(&id) {
                    ids.push(id);
                }
            }
            Some((binding, ids))
        }
        RowPredicate::Compare { .. }
        | RowPredicate::StartsWith { .. }
        | RowPredicate::In { .. }
        | RowPredicate::Not(_)
        | RowPredicate::IsNull { .. }
        | RowPredicate::BindingIsNull { .. } => None,
    }
}

#[cfg(feature = "opencypher")]
fn node_id_literal_equality(
    id_expression: &RowExpression,
    literal_expression: &RowExpression,
) -> Option<(String, Vec<VertexId>)> {
    let RowExpression::NodeId { binding } = id_expression else {
        return None;
    };
    let RowExpression::Literal(value) = literal_expression else {
        return None;
    };
    Some((binding.clone(), vec![vertex_id_literal(value)?]))
}

/// Drops the entries that are not vertex ids rather than refusing the whole
/// list: `n.id IN [1, 'x']` still seeks vertex 1, and `'x'` names no vertex, so
/// the shorter list is still a superset of the rows the predicate keeps.
/// `None` when nothing usable is left, including for an empty list, so the
/// caller plans the query the way it did before instead of authorising a seek
/// that reads nothing.
#[cfg(feature = "opencypher")]
fn vertex_id_literals(values: &[VertexPropertyValue]) -> Option<Vec<VertexId>> {
    let mut ids = Vec::with_capacity(values.len());
    for value in values {
        if let Some(id) = vertex_id_literal(value) {
            if !ids.contains(&id) {
                ids.push(id);
            }
        }
    }
    (!ids.is_empty()).then_some(ids)
}

#[cfg(feature = "opencypher")]
fn vertex_id_literal(value: &VertexPropertyValue) -> Option<VertexId> {
    match value {
        VertexPropertyValue::Integer(value) => Some(*value),
        VertexPropertyValue::SignedInteger(value) => VertexId::try_from(*value).ok(),
        VertexPropertyValue::Float(_)
        | VertexPropertyValue::Bool(_)
        | VertexPropertyValue::String(_) => None,
    }
}

#[cfg(feature = "opencypher")]
fn row_predicate_requires_property(
    predicate: &RowPredicate,
    binding: &str,
    property: &str,
) -> bool {
    let expression_matches = |expression: &RowExpression| {
        matches!(
            expression,
            RowExpression::Property {
                binding: candidate_binding,
                property: candidate_property,
            } if candidate_binding == binding && candidate_property == property
        )
    };
    match predicate {
        RowPredicate::Compare { left, right, .. } => {
            expression_matches(left) || expression_matches(right)
        }
        RowPredicate::StartsWith { expression, .. } => expression_matches(expression),
        // `x IN [...]` constrains `x` exactly as an equality does, so it must
        // answer the same way a `Compare` on that property would.
        RowPredicate::In { expression, .. } => expression_matches(expression),
        RowPredicate::And(left, right) => {
            row_predicate_requires_property(left, binding, property)
                || row_predicate_requires_property(right, binding, property)
        }
        RowPredicate::Or(left, right) => {
            row_predicate_requires_property(left, binding, property)
                && row_predicate_requires_property(right, binding, property)
        }
        RowPredicate::Not(_) => false,
        // Only the negated form rejects a row without the property.
        RowPredicate::IsNull {
            expression,
            negated,
        } => *negated && expression_matches(expression),
        // A predicate on the binding says nothing about any property of it.
        RowPredicate::BindingIsNull { .. } => false,
    }
}

#[cfg(feature = "opencypher")]
fn retain_binding_rows_matching_predicate(
    rows: &mut Vec<BindingRow>,
    predicate: &RowPredicate,
    budget: &QueryBudget,
) -> Result<()> {
    let mut matching = Vec::with_capacity(rows.len());
    for row in rows.drain(..) {
        budget.check("cypher_ordered_relationship_property_predicate")?;
        if row_predicate_matches(&row, predicate)? {
            matching.push(row);
        }
    }
    *rows = matching;
    Ok(())
}

#[cfg(feature = "opencypher")]
fn eval_row_expression(row: &BindingRow, expression: &RowExpression) -> Result<RowScalarValue> {
    match expression {
        RowExpression::NodeId { binding } => match row.get_optional(binding)? {
            Some(value) => Ok(RowScalarValue::Value(VertexPropertyValue::Integer(value))),
            None => Ok(RowScalarValue::Missing),
        },
        RowExpression::Property { binding, property } => {
            Ok(match binding_property(row, binding, property)? {
                Some(value) => RowScalarValue::Value(value),
                None => RowScalarValue::Missing,
            })
        }
        RowExpression::Literal(value) => Ok(RowScalarValue::Value(value.clone())),
        RowExpression::Null => Ok(RowScalarValue::Missing),
    }
}

#[cfg(feature = "opencypher")]
fn compare_row_values(
    left: RowScalarValue,
    op: RowComparisonOp,
    right: RowScalarValue,
) -> Result<Option<bool>> {
    let (RowScalarValue::Value(left), RowScalarValue::Value(right)) = (left, right) else {
        return Ok(None);
    };
    compare_vertex_property_values(&left, op, &right).map(Some)
}

#[cfg(feature = "opencypher")]
fn compare_vertex_property_values(
    left: &VertexPropertyValue,
    op: RowComparisonOp,
    right: &VertexPropertyValue,
) -> Result<bool> {
    Ok(match op {
        RowComparisonOp::Eq => numeric_property_order(left, right)
            .map(|ordering| compare_ordering(ordering, op))
            .unwrap_or(left == right),
        RowComparisonOp::Ne => numeric_property_order(left, right)
            .map(|ordering| compare_ordering(ordering, op))
            .unwrap_or(left != right),
        RowComparisonOp::Lt | RowComparisonOp::Gt | RowComparisonOp::Lte | RowComparisonOp::Gte => {
            match numeric_property_order(left, right) {
                Some(ordering) => compare_ordering(ordering, op),
                None => match (left, right) {
                    (VertexPropertyValue::String(left), VertexPropertyValue::String(right)) => {
                        compare_ordering(left.cmp(right), op)
                    }
                    _ => {
                        return Err(GraphError::UnsupportedQuery {
                            reason: QueryFailureReason::Evaluation,
                            dialect: "OpenCypher",
                            feature:
                                "ordered comparisons require numeric or matching string values"
                                    .to_string(),
                        });
                    }
                },
            }
        }
    })
}

#[cfg(feature = "opencypher")]
pub(super) fn vertex_property_values_equal(
    left: &VertexPropertyValue,
    right: &VertexPropertyValue,
) -> bool {
    compare_vertex_property_values(left, RowComparisonOp::Eq, right).unwrap_or(false)
}

#[cfg(feature = "opencypher")]
fn compare_ordering(ordering: std::cmp::Ordering, op: RowComparisonOp) -> bool {
    match op {
        RowComparisonOp::Eq => ordering == std::cmp::Ordering::Equal,
        RowComparisonOp::Ne => ordering != std::cmp::Ordering::Equal,
        RowComparisonOp::Lt => ordering == std::cmp::Ordering::Less,
        RowComparisonOp::Gt => ordering == std::cmp::Ordering::Greater,
        RowComparisonOp::Lte => ordering != std::cmp::Ordering::Greater,
        RowComparisonOp::Gte => ordering != std::cmp::Ordering::Less,
    }
}

#[cfg(feature = "opencypher")]
fn binding_property(
    row: &BindingRow,
    binding: &str,
    property: &str,
) -> Result<Option<VertexPropertyValue>> {
    if row.null_values.contains(binding) || row.null_relationships.contains(binding) {
        return Ok(None);
    }
    if row.values.contains_key(binding) && property == "id" {
        return Ok(Some(VertexPropertyValue::Integer(row.get(binding)?)));
    }
    if let Some(relationship) = row.relationships.get(binding) {
        if property == "id" {
            return Err(GraphError::UnsupportedQuery {
                reason: QueryFailureReason::Other,
                dialect: "OpenCypher",
                feature: "relationship id properties are not executable in Query engine"
                    .to_string(),
            });
        }
        let Some(metadata) = row.relationship_metadata.get(relationship) else {
            return Err(GraphError::UnsupportedQuery {
                reason: QueryFailureReason::Other,
                dialect: "OpenCypher",
                feature: format!("metadata for relationship {binding} was not hydrated"),
            });
        };
        return Ok(metadata.properties.get(property).cloned());
    }
    let Some(metadata) = row.metadata.get(binding) else {
        return Err(GraphError::UnsupportedQuery {
            reason: QueryFailureReason::Other,
            dialect: "OpenCypher",
            feature: format!("metadata for bound variable {binding} was not hydrated"),
        });
    };
    Ok(metadata.properties.get(property).cloned())
}

#[cfg(feature = "opencypher")]
fn project_binding_row(row: &BindingRow, projections: &[RowProjection]) -> Result<QueryRow> {
    let mut values = Vec::with_capacity(projections.len());
    for projection in projections {
        match projection {
            RowProjection::NodeId { binding } => {
                values.push(match row.get_optional(binding)? {
                    Some(value) => QueryValue::VertexId(value),
                    None => QueryValue::Null,
                });
            }
            RowProjection::Property { binding, property } => {
                values.push(match binding_property(row, binding, property)? {
                    Some(value) => QueryValue::Property(value),
                    None => QueryValue::Null,
                });
            }
            RowProjection::Literal(literal) => {
                values.push(
                    literal
                        .clone()
                        .map_or(QueryValue::Null, QueryValue::Property),
                );
            }
            RowProjection::CountAll | RowProjection::Aggregate { .. } => {
                return Err(GraphError::UnsupportedQuery {
                    reason: QueryFailureReason::Other,
                    dialect: "OpenCypher",
                    feature: "aggregate projection must be planned as an aggregate".to_string(),
                });
            }
        }
    }
    Ok(QueryRow::new(values))
}

#[cfg(feature = "opencypher")]
fn project_relationship_row(
    edge: &RowEdgePattern,
    relationship: &BoundRelationship,
    metadata: &EdgeMetadata,
    projections: &[RowProjection],
) -> Result<QueryRow> {
    let mut values = Vec::with_capacity(projections.len());
    for projection in projections {
        values.push(match projection {
            RowProjection::NodeId { binding } => {
                QueryValue::VertexId(relationship_endpoint_id(edge, relationship, binding)?)
            }
            RowProjection::Property { binding, property } => {
                if edge.binding.as_deref() != Some(binding.as_str()) {
                    return Err(GraphError::UnsupportedQuery {
                        reason: QueryFailureReason::Return,
                        dialect: "OpenCypher",
                        feature: format!(
                            "relationship fast path cannot project property {binding}.{property}"
                        ),
                    });
                }
                if property == "id" {
                    return Err(GraphError::UnsupportedQuery {
                        reason: QueryFailureReason::Return,
                        dialect: "OpenCypher",
                        feature: "relationship id properties are not executable in Query engine"
                            .to_string(),
                    });
                }
                metadata
                    .properties
                    .get(property)
                    .cloned()
                    .map(QueryValue::Property)
                    .unwrap_or(QueryValue::Null)
            }
            // Unreachable while the guards above reject literals on this
            // path, but the fast path can project a constant perfectly well.
            RowProjection::Literal(literal) => literal
                .clone()
                .map_or(QueryValue::Null, QueryValue::Property),
            RowProjection::CountAll | RowProjection::Aggregate { .. } => {
                return Err(GraphError::UnsupportedQuery {
                    reason: QueryFailureReason::Other,
                    dialect: "OpenCypher",
                    feature: "aggregate projection must be planned as an aggregate".to_string(),
                });
            }
        });
    }
    Ok(QueryRow::new(values))
}

#[cfg(feature = "opencypher")]
fn relationship_rows_cache_value(
    rows: &[(BoundRelationship, EdgeMetadata)],
) -> RelationshipRowsCacheValue {
    RelationshipRowsCacheValue::new(
        rows.iter()
            .map(|(relationship, metadata)| RelationshipRowsCacheEntry {
                relationship_id: relationship.relationship_id,
                metadata: metadata.clone(),
            })
            .collect(),
    )
}

#[cfg(feature = "opencypher")]
fn relationship_rows_from_cache_value(
    edge_type: &str,
    src: VertexId,
    dst: VertexId,
    cached: RelationshipRowsCacheValue,
) -> Vec<(BoundRelationship, EdgeMetadata)> {
    cached
        .rows
        .iter()
        .map(|entry| {
            (
                BoundRelationship {
                    edge_type: edge_type.to_string(),
                    src,
                    dst,
                    relationship_id: entry.relationship_id,
                },
                entry.metadata.clone(),
            )
        })
        .collect()
}

#[cfg(feature = "opencypher")]
fn sort_keys_for_relationship_row(
    edge: &RowEdgePattern,
    relationship: &BoundRelationship,
    metadata: &EdgeMetadata,
    row: &QueryRow,
    query: &ParsedRowQuery,
) -> Result<Vec<QueryValue>> {
    let mut keys = Vec::with_capacity(query.order_by.len());
    for sort in &query.order_by {
        keys.push(match &sort.expression {
            RowSortExpression::NodeId { binding } => {
                QueryValue::VertexId(relationship_endpoint_id(edge, relationship, binding)?)
            }
            RowSortExpression::Property { binding, property } => {
                if edge.binding.as_deref() != Some(binding.as_str()) {
                    return Err(GraphError::UnsupportedQuery {
                        reason: QueryFailureReason::OrderWindow,
                        dialect: "OpenCypher",
                        feature: format!(
                            "relationship fast path cannot sort by property {binding}.{property}"
                        ),
                    });
                }
                metadata
                    .properties
                    .get(property)
                    .cloned()
                    .map(QueryValue::Property)
                    .unwrap_or(QueryValue::Null)
            }
            RowSortExpression::Column { name } => {
                projected_column_value(row, &query.columns, name)?
            }
            RowSortExpression::CountAll => {
                return Err(GraphError::UnsupportedQuery {
                    reason: QueryFailureReason::OrderWindow,
                    dialect: "OpenCypher",
                    feature: "count(*) ORDER BY is only valid for aggregate rows".to_string(),
                });
            }
        });
    }
    Ok(keys)
}

#[cfg(feature = "opencypher")]
fn relationship_endpoint_id(
    edge: &RowEdgePattern,
    relationship: &BoundRelationship,
    binding: &str,
) -> Result<VertexId> {
    if edge.src.binding.as_deref() == Some(binding) {
        return Ok(relationship.src);
    }
    if edge.dst.binding.as_deref() == Some(binding) {
        return Ok(relationship.dst);
    }
    Err(GraphError::UnsupportedQuery {
        reason: QueryFailureReason::Other,
        dialect: "OpenCypher",
        feature: format!("relationship fast path cannot project unbound node {binding}"),
    })
}

#[cfg(feature = "opencypher")]
fn row_projections_have_aggregates(projections: &[RowProjection]) -> bool {
    projections.iter().any(is_aggregate_projection)
}

#[cfg(feature = "opencypher")]
fn is_aggregate_projection(projection: &RowProjection) -> bool {
    matches!(
        projection,
        RowProjection::CountAll | RowProjection::Aggregate { .. }
    )
}

#[cfg(feature = "opencypher")]
fn aggregate_projected_rows(
    bindings: Vec<BindingRow>,
    projections: &[RowProjection],
    columns: &[QueryColumn],
    order_by: &[RowSort],
    budget: &QueryBudget,
) -> Result<Vec<ProjectedQueryRow>> {
    let group_projection_indexes: Vec<_> = projections
        .iter()
        .enumerate()
        .filter_map(|(idx, projection)| (!is_aggregate_projection(projection)).then_some(idx))
        .collect();
    let aggregate_projection_indexes: Vec<_> = projections
        .iter()
        .enumerate()
        .filter_map(|(idx, projection)| is_aggregate_projection(projection).then_some(idx))
        .collect();

    let mut groups = BTreeMap::<Vec<QueryValue>, Vec<AggregateAccumulator>>::new();
    if bindings.is_empty() && group_projection_indexes.is_empty() {
        groups.insert(
            Vec::new(),
            aggregate_projection_indexes
                .iter()
                .map(|idx| new_aggregate_accumulator(&projections[*idx]))
                .collect::<Result<_>>()?,
        );
    }

    for binding in bindings {
        budget.check("cypher_aggregate_group")?;
        let mut group_key = Vec::with_capacity(group_projection_indexes.len());
        for idx in &group_projection_indexes {
            group_key.push(project_single_binding_value(&binding, &projections[*idx])?);
        }
        let states = match groups.entry(group_key) {
            std::collections::btree_map::Entry::Occupied(entry) => entry.into_mut(),
            std::collections::btree_map::Entry::Vacant(entry) => entry.insert(
                aggregate_projection_indexes
                    .iter()
                    .map(|idx| new_aggregate_accumulator(&projections[*idx]))
                    .collect::<Result<_>>()?,
            ),
        };
        for (state_idx, projection_idx) in aggregate_projection_indexes.iter().enumerate() {
            apply_aggregate_projection(
                &mut states[state_idx],
                &projections[*projection_idx],
                &binding,
            )?;
        }
    }

    let mut projected = Vec::with_capacity(groups.len());
    for (group_key, states) in groups {
        budget.check("cypher_aggregate_project")?;
        let mut group_idx = 0;
        let mut aggregate_idx = 0;
        let mut values = Vec::with_capacity(projections.len());
        for projection in projections {
            if is_aggregate_projection(projection) {
                values.push(finalize_aggregate(&states[aggregate_idx])?);
                aggregate_idx += 1;
            } else {
                values.push(group_key[group_idx].clone());
                group_idx += 1;
            }
        }
        let row = QueryRow::new(values);
        let sort_keys = sort_keys_for_projected_only(&row, columns, order_by)?;
        push_projected_query_row(&mut projected, row, sort_keys);
    }
    Ok(projected)
}

#[cfg(feature = "opencypher")]
fn new_aggregate_accumulator(projection: &RowProjection) -> Result<AggregateAccumulator> {
    Ok(match projection {
        RowProjection::CountAll => AggregateAccumulator::CountAll(0),
        RowProjection::Aggregate { function, .. } => match function {
            RowAggregateFunction::Count => AggregateAccumulator::CountExpression(0),
            RowAggregateFunction::Sum => AggregateAccumulator::Sum(0),
            RowAggregateFunction::Avg => AggregateAccumulator::Avg { sum: 0, count: 0 },
            RowAggregateFunction::Collect => AggregateAccumulator::Collect(Vec::new()),
        },
        _ => {
            return Err(GraphError::UnsupportedQuery {
                reason: QueryFailureReason::Other,
                dialect: "OpenCypher",
                feature: "non-aggregate projection cannot create aggregate state".to_string(),
            });
        }
    })
}

#[cfg(feature = "opencypher")]
fn apply_aggregate_projection(
    state: &mut AggregateAccumulator,
    projection: &RowProjection,
    row: &BindingRow,
) -> Result<()> {
    match (state, projection) {
        (AggregateAccumulator::CountAll(count), RowProjection::CountAll) => {
            *count = count.saturating_add(1);
        }
        (
            AggregateAccumulator::CountExpression(count),
            RowProjection::Aggregate {
                function: RowAggregateFunction::Count,
                expression,
            },
        ) => {
            if expression_query_value(row, expression)?.is_some() {
                *count = count.saturating_add(1);
            }
        }
        (
            AggregateAccumulator::Sum(sum),
            RowProjection::Aggregate {
                function: RowAggregateFunction::Sum,
                expression,
            },
        ) => {
            if let Some(value) = aggregate_integer_value(row, expression, "sum")? {
                *sum = sum.checked_add(u128::from(value)).ok_or_else(|| {
                    GraphError::UnsupportedQuery {
                        reason: QueryFailureReason::Evaluation,
                        dialect: "OpenCypher",
                        feature: "sum aggregate overflowed".to_string(),
                    }
                })?;
            }
        }
        (
            AggregateAccumulator::Avg { sum, count },
            RowProjection::Aggregate {
                function: RowAggregateFunction::Avg,
                expression,
            },
        ) => {
            if let Some(value) = aggregate_integer_value(row, expression, "avg")? {
                *sum = sum.checked_add(u128::from(value)).ok_or_else(|| {
                    GraphError::UnsupportedQuery {
                        reason: QueryFailureReason::Evaluation,
                        dialect: "OpenCypher",
                        feature: "avg aggregate overflowed".to_string(),
                    }
                })?;
                *count = count.saturating_add(1);
            }
        }
        (
            AggregateAccumulator::Collect(values),
            RowProjection::Aggregate {
                function: RowAggregateFunction::Collect,
                expression,
            },
        ) => {
            if let Some(value) = expression_query_value(row, expression)? {
                values.push(value);
            }
        }
        _ => {
            return Err(GraphError::UnsupportedQuery {
                reason: QueryFailureReason::Other,
                dialect: "OpenCypher",
                feature: "aggregate projection state mismatch".to_string(),
            });
        }
    }
    Ok(())
}

#[cfg(feature = "opencypher")]
fn aggregate_integer_value(
    row: &BindingRow,
    expression: &RowExpression,
    function: &str,
) -> Result<Option<u64>> {
    match eval_row_expression(row, expression)? {
        RowScalarValue::Missing => Ok(None),
        RowScalarValue::Value(VertexPropertyValue::Integer(value)) => Ok(Some(value)),
        RowScalarValue::Value(_) => Err(GraphError::UnsupportedQuery {
            reason: QueryFailureReason::Evaluation,
            dialect: "OpenCypher",
            feature: format!("{function} aggregate requires integer values"),
        }),
    }
}

#[cfg(feature = "opencypher")]
fn finalize_aggregate(state: &AggregateAccumulator) -> Result<QueryValue> {
    Ok(match state {
        AggregateAccumulator::CountAll(count) | AggregateAccumulator::CountExpression(count) => {
            QueryValue::Count(*count)
        }
        AggregateAccumulator::Sum(sum) => {
            QueryValue::Property(VertexPropertyValue::Integer((*sum).try_into().map_err(
                |_| GraphError::UnsupportedQuery {
                    reason: QueryFailureReason::Evaluation,
                    dialect: "OpenCypher",
                    feature: "sum aggregate exceeds u64 result range".to_string(),
                },
            )?))
        }
        AggregateAccumulator::Avg { sum: _, count: 0 } => QueryValue::Null,
        AggregateAccumulator::Avg { sum, count } => {
            QueryValue::Float(QueryFloat(*sum as f64 / *count as f64))
        }
        AggregateAccumulator::Collect(values) => QueryValue::List(values.clone()),
    })
}

#[cfg(feature = "opencypher")]
fn project_single_binding_value(
    row: &BindingRow,
    projection: &RowProjection,
) -> Result<QueryValue> {
    match projection {
        RowProjection::NodeId { binding } => Ok(match row.get_optional(binding)? {
            Some(value) => QueryValue::VertexId(value),
            None => QueryValue::Null,
        }),
        RowProjection::Property { binding, property } => {
            Ok(match binding_property(row, binding, property)? {
                Some(value) => QueryValue::Property(value),
                None => QueryValue::Null,
            })
        }
        // A constant is a legitimate group key: every row carries the same
        // value, so it partitions nothing and drops out of the grouping.
        RowProjection::Literal(literal) => Ok(literal
            .clone()
            .map_or(QueryValue::Null, QueryValue::Property)),
        RowProjection::CountAll | RowProjection::Aggregate { .. } => {
            Err(GraphError::UnsupportedQuery {
                reason: QueryFailureReason::Other,
                dialect: "OpenCypher",
                feature: "aggregate projection cannot be used as a group key".to_string(),
            })
        }
    }
}

#[cfg(feature = "opencypher")]
fn expression_query_value(
    row: &BindingRow,
    expression: &RowExpression,
) -> Result<Option<QueryValue>> {
    Ok(match expression {
        RowExpression::NodeId { binding } => row.get_optional(binding)?.map(QueryValue::VertexId),
        RowExpression::Property { binding, property } => {
            binding_property(row, binding, property)?.map(QueryValue::Property)
        }
        RowExpression::Literal(value) => Some(QueryValue::Property(value.clone())),
        RowExpression::Null => None,
    })
}

#[cfg(feature = "opencypher")]
fn sort_keys_for_row(
    binding_row: &BindingRow,
    row: &QueryRow,
    columns: &[QueryColumn],
    order_by: &[RowSort],
) -> Result<Vec<QueryValue>> {
    let mut keys = Vec::with_capacity(order_by.len());
    for sort in order_by {
        keys.push(match &sort.expression {
            RowSortExpression::NodeId { binding } => match binding_row.get_optional(binding)? {
                Some(value) => QueryValue::VertexId(value),
                None => QueryValue::Null,
            },
            RowSortExpression::Property { binding, property } => {
                match binding_property(binding_row, binding, property)? {
                    Some(value) => QueryValue::Property(value),
                    None => QueryValue::Null,
                }
            }
            RowSortExpression::Column { name } => projected_column_value(row, columns, name)?,
            RowSortExpression::CountAll => {
                return Err(GraphError::UnsupportedQuery {
                    reason: QueryFailureReason::OrderWindow,
                    dialect: "OpenCypher",
                    feature: "count(*) ORDER BY is only valid for aggregate rows".to_string(),
                });
            }
        });
    }
    Ok(keys)
}

#[cfg(feature = "opencypher")]
fn sort_keys_for_projected_only(
    row: &QueryRow,
    columns: &[QueryColumn],
    order_by: &[RowSort],
) -> Result<Vec<QueryValue>> {
    let mut keys = Vec::with_capacity(order_by.len());
    for sort in order_by {
        keys.push(match &sort.expression {
            RowSortExpression::Column { name } => projected_column_value(row, columns, name)?,
            RowSortExpression::CountAll => {
                let Some(value) = row
                    .values
                    .iter()
                    .find(|value| matches!(value, QueryValue::Count(_)))
                else {
                    return Err(GraphError::UnsupportedQuery {
                        reason: QueryFailureReason::OrderWindow,
                        dialect: "OpenCypher",
                        feature: "count(*) ORDER BY requires an aggregate row".to_string(),
                    });
                };
                value.clone()
            }
            RowSortExpression::NodeId { .. } => {
                return Err(GraphError::UnsupportedQuery {
                    reason: QueryFailureReason::OrderWindow,
                    dialect: "OpenCypher",
                    feature: "aggregate ORDER BY cannot reference row variables".to_string(),
                });
            }
            RowSortExpression::Property { .. } => {
                return Err(GraphError::UnsupportedQuery {
                    reason: QueryFailureReason::OrderWindow,
                    dialect: "OpenCypher",
                    feature: "aggregate ORDER BY cannot reference row properties".to_string(),
                });
            }
        });
    }
    Ok(keys)
}

#[cfg(feature = "opencypher")]
fn projected_column_value(
    row: &QueryRow,
    columns: &[QueryColumn],
    name: &str,
) -> Result<QueryValue> {
    let Some(index) = columns.iter().position(|column| column.name == name) else {
        return Err(GraphError::UnsupportedQuery {
            reason: QueryFailureReason::OrderWindow,
            dialect: "OpenCypher",
            feature: format!("ORDER BY references unknown projection {name}"),
        });
    };
    row.values
        .get(index)
        .cloned()
        .ok_or_else(|| GraphError::UnsupportedQuery {
            reason: QueryFailureReason::OrderWindow,
            dialect: "OpenCypher",
            feature: format!("ORDER BY projection {name} has no row value"),
        })
}

#[cfg(feature = "opencypher")]
fn compare_projected_rows(
    left: &ProjectedQueryRow,
    right: &ProjectedQueryRow,
    order_by: &[RowSort],
) -> std::cmp::Ordering {
    for (idx, sort) in order_by.iter().enumerate() {
        let ordering = compare_query_values(&left.sort_keys[idx], &right.sort_keys[idx]);
        let ordering = if sort.ascending {
            ordering
        } else {
            ordering.reverse()
        };
        if ordering != std::cmp::Ordering::Equal {
            return ordering;
        }
    }
    std::cmp::Ordering::Equal
}

#[cfg(feature = "opencypher")]
#[derive(Clone, Copy)]
struct EncodedPropertyIndexScan<'a> {
    cell_id: &'a str,
    edge_type: &'a str,
    property: &'a str,
    encoded: &'a str,
    budget: &'a QueryBudget,
}

#[cfg(feature = "opencypher")]
fn compare_query_values(left: &QueryValue, right: &QueryValue) -> std::cmp::Ordering {
    match (left, right) {
        (QueryValue::Null, QueryValue::Null) => std::cmp::Ordering::Equal,
        (QueryValue::VertexId(left), QueryValue::VertexId(right))
        | (QueryValue::VertexId(left), QueryValue::Count(right))
        | (QueryValue::Count(left), QueryValue::VertexId(right))
        | (QueryValue::Count(left), QueryValue::Count(right)) => left.cmp(right),
        (QueryValue::Float(left), QueryValue::Float(right)) => left.cmp(right),
        (QueryValue::Property(left), QueryValue::Property(right)) => {
            compare_vertex_property_order(left, right)
        }
        (QueryValue::List(left), QueryValue::List(right)) => {
            for (left, right) in left.iter().zip(right.iter()) {
                let ordering = compare_query_values(left, right);
                if ordering != std::cmp::Ordering::Equal {
                    return ordering;
                }
            }
            left.len().cmp(&right.len())
        }
        (QueryValue::Path(left), QueryValue::Path(right)) => left.cmp(right),
        (QueryValue::Bool(left), QueryValue::Bool(right)) => left.cmp(right),
        _ => query_value_rank(left).cmp(&query_value_rank(right)),
    }
}

#[cfg(feature = "opencypher")]
fn compare_vertex_property_order(
    left: &VertexPropertyValue,
    right: &VertexPropertyValue,
) -> std::cmp::Ordering {
    if let Some(ordering) = numeric_property_order(left, right) {
        return ordering;
    }
    match (left, right) {
        (VertexPropertyValue::Bool(left), VertexPropertyValue::Bool(right)) => left.cmp(right),
        (VertexPropertyValue::String(left), VertexPropertyValue::String(right)) => left.cmp(right),
        _ => vertex_property_rank(left).cmp(&vertex_property_rank(right)),
    }
}

#[cfg(feature = "opencypher")]
fn query_value_rank(value: &QueryValue) -> u8 {
    match value {
        QueryValue::Bool(_) => 1,
        QueryValue::VertexId(_) | QueryValue::Count(_) => 2,
        QueryValue::Float(_) => 3,
        QueryValue::Property(value) => 4 + vertex_property_rank(value),
        QueryValue::List(_) => 8,
        QueryValue::Path(_) => 9,
        QueryValue::Null => u8::MAX,
    }
}

#[cfg(feature = "opencypher")]
fn vertex_property_rank(value: &VertexPropertyValue) -> u8 {
    match value {
        VertexPropertyValue::Bool(_) => 0,
        VertexPropertyValue::Integer(_)
        | VertexPropertyValue::SignedInteger(_)
        | VertexPropertyValue::Float(_) => 1,
        VertexPropertyValue::String(_) => 2,
    }
}

#[cfg(feature = "opencypher")]
pub(crate) fn equivalent_property_index_keys(value: &VertexPropertyValue) -> Vec<String> {
    let mut keys = BTreeSet::new();
    keys.insert(encode_vertex_property_value_key(value));
    match value {
        VertexPropertyValue::Integer(value) => {
            if let Ok(signed) = i64::try_from(*value) {
                keys.insert(encode_vertex_property_value_key(
                    &VertexPropertyValue::SignedInteger(signed),
                ));
            }
            if let Some(float) = VertexPropertyValue::exact_f64_from_u64(*value) {
                insert_float_property_index_key(&mut keys, float);
                if *value == 0 {
                    insert_float_property_index_key(&mut keys, -0.0);
                }
            }
        }
        VertexPropertyValue::SignedInteger(value) => {
            if let Ok(unsigned) = u64::try_from(*value) {
                keys.insert(encode_vertex_property_value_key(
                    &VertexPropertyValue::Integer(unsigned),
                ));
            }
            if let Some(float) = VertexPropertyValue::exact_f64_from_i64(*value) {
                insert_float_property_index_key(&mut keys, float);
                if *value == 0 {
                    insert_float_property_index_key(&mut keys, -0.0);
                }
            }
        }
        VertexPropertyValue::Float(value) => {
            if value.0 == 0.0 {
                insert_float_property_index_key(&mut keys, 0.0);
                insert_float_property_index_key(&mut keys, -0.0);
            }
            if let Some(integer) = VertexPropertyValue::exact_u64_from_f64(value.0) {
                keys.insert(encode_vertex_property_value_key(
                    &VertexPropertyValue::Integer(integer),
                ));
            }
            if let Some(integer) = VertexPropertyValue::exact_i64_from_f64(value.0) {
                keys.insert(encode_vertex_property_value_key(
                    &VertexPropertyValue::SignedInteger(integer),
                ));
            }
        }
        VertexPropertyValue::Bool(_) | VertexPropertyValue::String(_) => {}
    }
    keys.into_iter().collect()
}

#[cfg(feature = "opencypher")]
fn insert_float_property_index_key(keys: &mut BTreeSet<String>, value: f64) {
    keys.insert(encode_vertex_property_value_key(
        &VertexPropertyValue::Float(QueryFloat(value)),
    ));
}

#[cfg(feature = "opencypher")]
fn numeric_property_order(
    left: &VertexPropertyValue,
    right: &VertexPropertyValue,
) -> Option<std::cmp::Ordering> {
    match (left, right) {
        (VertexPropertyValue::Integer(left), VertexPropertyValue::Integer(right)) => {
            Some(left.cmp(right))
        }
        (VertexPropertyValue::SignedInteger(left), VertexPropertyValue::SignedInteger(right)) => {
            Some(left.cmp(right))
        }
        (VertexPropertyValue::SignedInteger(left), VertexPropertyValue::Integer(right)) => {
            Some(compare_i64_u64(*left, *right))
        }
        (VertexPropertyValue::Integer(left), VertexPropertyValue::SignedInteger(right)) => {
            Some(compare_i64_u64(*right, *left).reverse())
        }
        (VertexPropertyValue::Float(left), VertexPropertyValue::Float(right)) => {
            Some(compare_f64_numeric(left.0, right.0))
        }
        (VertexPropertyValue::Integer(left), VertexPropertyValue::Float(right)) => {
            Some(compare_u64_f64(*left, right.0))
        }
        (VertexPropertyValue::Float(left), VertexPropertyValue::Integer(right)) => {
            Some(compare_u64_f64(*right, left.0).reverse())
        }
        (VertexPropertyValue::SignedInteger(left), VertexPropertyValue::Float(right)) => {
            Some(compare_i64_f64(*left, right.0))
        }
        (VertexPropertyValue::Float(left), VertexPropertyValue::SignedInteger(right)) => {
            Some(compare_i64_f64(*right, left.0).reverse())
        }
        _ => None,
    }
}

#[cfg(feature = "opencypher")]
fn compare_i64_u64(left: i64, right: u64) -> std::cmp::Ordering {
    match u64::try_from(left) {
        Ok(left) => left.cmp(&right),
        Err(_) => std::cmp::Ordering::Less,
    }
}

#[cfg(feature = "opencypher")]
fn compare_i64_f64(left: i64, right: f64) -> std::cmp::Ordering {
    const I64_INCLUSIVE_LOWER: f64 = -9223372036854775808.0;
    const I64_EXCLUSIVE_UPPER: f64 = 9223372036854775808.0;
    if right.is_nan() {
        return (left as f64).total_cmp(&right);
    }
    if right < I64_INCLUSIVE_LOWER {
        return std::cmp::Ordering::Greater;
    }
    if right >= I64_EXCLUSIVE_UPPER {
        return std::cmp::Ordering::Less;
    }
    let floor = right.floor();
    let floor_integer = floor as i64;
    match left.cmp(&floor_integer) {
        std::cmp::Ordering::Equal if floor == right => std::cmp::Ordering::Equal,
        std::cmp::Ordering::Equal => std::cmp::Ordering::Less,
        ordering => ordering,
    }
}

#[cfg(feature = "opencypher")]
fn compare_f64_numeric(left: f64, right: f64) -> std::cmp::Ordering {
    if left == right {
        std::cmp::Ordering::Equal
    } else {
        left.total_cmp(&right)
    }
}

#[cfg(feature = "opencypher")]
fn compare_u64_f64(left: u64, right: f64) -> std::cmp::Ordering {
    const U64_EXCLUSIVE_UPPER: f64 = 18446744073709551616.0;
    if right.is_nan() {
        return (left as f64).total_cmp(&right);
    }
    if right < 0.0 {
        return std::cmp::Ordering::Greater;
    }
    if right >= U64_EXCLUSIVE_UPPER {
        return std::cmp::Ordering::Less;
    }
    let floor = right.floor();
    let floor_integer = floor as u64;
    match left.cmp(&floor_integer) {
        std::cmp::Ordering::Equal if floor == right => std::cmp::Ordering::Equal,
        std::cmp::Ordering::Equal => std::cmp::Ordering::Less,
        ordering => ordering,
    }
}

#[cfg(feature = "experimental-cypher-engine")]
#[derive(Eq, PartialEq, Ord, PartialOrd)]
enum OrderedCandidateKey<T: Ord> {
    Ascending(T),
    Descending(std::cmp::Reverse<T>),
}

#[cfg(feature = "experimental-cypher-engine")]
impl<T: Ord> OrderedCandidateKey<T> {
    fn new(value: T, ascending: bool) -> Self {
        if ascending {
            Self::Ascending(value)
        } else {
            Self::Descending(std::cmp::Reverse(value))
        }
    }
}

#[cfg(feature = "experimental-cypher-engine")]
struct OrderedVertexCandidate {
    key: (OrderedCandidateKey<String>, OrderedCandidateKey<VertexId>),
    vertex_id: VertexId,
    metadata: VertexMetadata,
}

#[cfg(feature = "experimental-cypher-engine")]
impl PartialEq for OrderedVertexCandidate {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key
    }
}
#[cfg(feature = "experimental-cypher-engine")]
impl Eq for OrderedVertexCandidate {}
#[cfg(feature = "experimental-cypher-engine")]
impl PartialOrd for OrderedVertexCandidate {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
#[cfg(feature = "experimental-cypher-engine")]
impl Ord for OrderedVertexCandidate {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.key.cmp(&other.key)
    }
}

/// A bounded ordered walk over one string vertex-property index, requested by
/// the experimental engine's `OrderedVertexPropertyScan` operator. Bounds and
/// the prefix are plain strings; this module encodes them the way the index
/// keys are encoded.
#[cfg(feature = "experimental-cypher-engine")]
#[derive(Clone, Debug)]
pub(super) struct OrderedStringVertexWindow {
    pub(super) property: String,
    pub(super) labels: Vec<String>,
    pub(super) ascending: bool,
    pub(super) id_ascending: bool,
    pub(super) prefix: Option<String>,
    /// `(value, inclusive)`.
    pub(super) lower: Option<(String, bool)>,
    /// `(value, inclusive)`.
    pub(super) upper: Option<(String, bool)>,
    /// Entries to return in this page, not rows the caller will keep.
    pub(super) required: usize,
    /// Resume strictly after this `(encoded value, vertex id)`. The index
    /// orders by both, so a page may end part-way through a run of entries
    /// sharing a value and must not repeat or skip any of them.
    pub(super) after: Option<(String, VertexId)>,
    /// Return entries in index order and stop at exactly `required`, dropping
    /// none, instead of finishing the last value's run and keeping the best
    /// `required` of it. A caller that orders ties itself, or applies a
    /// predicate this walk cannot see, must not have an entry dropped on its
    /// behalf -- the cursor would move past it for good -- and must not be
    /// handed a whole run either, which is unbounded. `after` resumes inside a
    /// run, so the caller reads a run in pages and keeps what it needs.
    pub(super) index_order: bool,
}

#[cfg(feature = "experimental-cypher-engine")]
impl OrderedStringVertexWindow {
    /// Index-key encoding of a string value without the leading `s` type tag.
    pub(super) fn encode(value: &str) -> String {
        encode_vertex_property_value_key(&VertexPropertyValue::String(value.to_string()))
            .strip_prefix('s')
            .unwrap_or_default()
            .to_string()
    }

    /// Whether an index entry (already stripped of its `s` tag) satisfies the
    /// prefix and both bounds. Hex encoding preserves byte order, so the
    /// comparison happens on encoded text without decoding each key.
    fn accepts_encoded(&self, encoded: &str) -> bool {
        if let Some(prefix) = &self.prefix {
            if !encoded.starts_with(Self::encode(prefix).as_str()) {
                return false;
            }
        }
        if let Some((lower, inclusive)) = &self.lower {
            let lower = Self::encode(lower);
            let ok = if *inclusive {
                encoded >= lower.as_str()
            } else {
                encoded > lower.as_str()
            };
            if !ok {
                return false;
            }
        }
        if let Some((upper, inclusive)) = &self.upper {
            let upper = Self::encode(upper);
            let ok = if *inclusive {
                encoded <= upper.as_str()
            } else {
                encoded < upper.as_str()
            };
            if !ok {
                return false;
            }
        }
        true
    }

    /// Whether the walk has moved past every entry that could still match, so
    /// the iterator can stop instead of draining the rest of the index.
    fn past_window(&self, encoded: &str) -> bool {
        let prefix = self.prefix.as_deref().map(Self::encode);
        if self.ascending {
            if let Some((upper, inclusive)) = &self.upper {
                let upper = Self::encode(upper);
                if (*inclusive && encoded > upper.as_str())
                    || (!inclusive && encoded >= upper.as_str())
                {
                    return true;
                }
            }
            if let Some(prefix) = &prefix {
                if !encoded.starts_with(prefix.as_str()) && encoded > prefix.as_str() {
                    return true;
                }
            }
        } else {
            if let Some((lower, inclusive)) = &self.lower {
                let lower = Self::encode(lower);
                if (*inclusive && encoded < lower.as_str())
                    || (!inclusive && encoded <= lower.as_str())
                {
                    return true;
                }
            }
            if let Some(prefix) = &prefix {
                if !encoded.starts_with(prefix.as_str()) && encoded < prefix.as_str() {
                    return true;
                }
            }
        }
        false
    }

    /// Whether an entry was already returned by an earlier page.
    ///
    /// The seek positions below land past the entry the last page stopped on,
    /// so on an ordinary page this drops nothing. It stays because those
    /// positions are bounds rather than the contract: each is the tightest of
    /// several candidates and the prefix or a range bound can be the one that
    /// wins, `ascending_start_suffix` saturates on the last possible vertex id,
    /// and a page begins wherever its scan was able to begin. Within one value
    /// the vertex id decides -- in the direction the index is being walked,
    /// which is not always the direction the caller ranks by.
    fn already_returned(&self, encoded: &str, vertex_id: VertexId) -> bool {
        let Some((value, id)) = &self.after else {
            return false;
        };
        match encoded.cmp(value.as_str()) {
            // Within one value the index is keyed by a zero-padded vertex ID,
            // so the walk reads the run in `ascending` order and in no other.
            // `id_ascending` ranks what the walk retains and never decides what
            // it reads, so resuming on it re-reads most of the run whenever the
            // two directions disagree.
            std::cmp::Ordering::Equal => {
                if self.ascending {
                    vertex_id <= *id
                } else {
                    vertex_id >= *id
                }
            }
            std::cmp::Ordering::Less => self.ascending,
            std::cmp::Ordering::Greater => !self.ascending,
        }
    }

    /// The encoded suffix an ascending walk can seek to.
    fn ascending_start_suffix(&self) -> Option<Vec<u8>> {
        if !self.ascending {
            return None;
        }
        let mut start: Option<String> = None;
        if let Some(prefix) = &self.prefix {
            start = Some(Self::encode(prefix));
        }
        if let Some((lower, _)) = &self.lower {
            // An exclusive bound still starts at the bound; the acceptance
            // check drops the equal entry.
            let lower = Self::encode(lower);
            if start.as_ref().is_none_or(|start| lower > *start) {
                start = Some(lower);
            }
        }
        if let Some((value, id)) = &self.after {
            // Past the entry the last page stopped on, not back to the top of
            // that entry's value. Resuming on the value alone re-read the run
            // ahead of the cursor and let `already_returned` discard it: one
            // entry between two values, but the whole run ahead of every page
            // while draining one, so draining an `n`-row tie group cost on the
            // order of `n^2 / 2` index reads. Saturating leaves the last
            // possible id to be read once and dropped, which is what the
            // unsaturated arithmetic would do for every other id anyway.
            let resume = keys::vertex_property_index_entry_suffix(value, id.saturating_add(1));
            if start.as_ref().is_none_or(|start| resume > *start) {
                start = Some(resume);
            }
        }
        start.map(String::into_bytes)
    }

    /// The exclusive encoded suffix a descending walk ends at: SlateDB starts
    /// a descending scan at the top of its range, so this is where a
    /// descending page begins. Without it every later page re-read the index
    /// from its highest key and discarded what earlier pages had returned,
    /// which made a many-page descending walk quadratic in index reads.
    ///
    /// Suffixes are `<hex value>/<vertex id>`. `/` sorts below every hex digit
    /// and `g` above all of them, so `value + "0"` ends just past the run of
    /// entries holding `value` -- and before any longer value that merely
    /// starts with it -- while `prefix + "g"` ends past every value that starts
    /// with `prefix`.
    fn descending_end_suffix(&self) -> Option<Vec<u8>> {
        if self.ascending {
            return None;
        }
        let run_end = |value: &str| format!("{value}0");
        let mut end: Option<String> = None;
        let mut lower_to = |candidate: String| {
            if end.as_ref().is_none_or(|end| candidate < *end) {
                end = Some(candidate);
            }
        };
        if let Some(prefix) = &self.prefix {
            lower_to(format!("{}g", Self::encode(prefix)));
        }
        if let Some((upper, _)) = &self.upper {
            // An exclusive bound still ends past the bound's run; the
            // acceptance check drops the equal entries.
            lower_to(run_end(&Self::encode(upper)));
        }
        if let Some((value, id)) = &self.after {
            // Just below the entry the last page stopped on. The end is
            // exclusive and a descending walk reads a run from its highest id
            // down, so everything from `id` up is already judged; ending at the
            // run's end instead re-read that part on every page, the way an
            // ascending resume on the value alone did.
            lower_to(keys::vertex_property_index_entry_suffix(value, *id));
        }
        end.map(String::into_bytes)
    }
}

#[cfg(feature = "experimental-cypher-engine")]
impl GraphShard {
    /// Walk the ordered string index for `window.property` and hydrate only
    /// the leading rows the window needs. Mirrors the legacy ordered-string
    /// fast path (`try_execute_ordered_string_vertex_rows_query`): seek to the
    /// lower bound, read a bounded batch, hydrate, verify labels and the index
    /// entry against stored metadata, and stop once `required` rows are held
    /// and the current entry no longer ties with the last accepted value. The
    /// caller applies its own deterministic tie-break and truncation.
    pub(super) async fn scan_ordered_string_vertex_window_at(
        &self,
        cell_id: &str,
        window: &OrderedStringVertexWindow,
        read_epoch: StorageSequence,
        budget: &QueryBudget,
    ) -> Result<OrderedStringVertexPage> {
        if window.required == 0 {
            return Ok(OrderedStringVertexPage::default());
        }
        // An `index_order` page resumes exactly where it stops, so a shorter
        // one is always safe; cap it at what this shard may hold rather than
        // failing a caller whose page grew with its residual's miss rate. A
        // ranked page cannot shrink: its `required` is the caller's window.
        let required = if window.index_order {
            window
                .required
                .min(self.limits.max_query_intermediate_rows.max(1))
        } else {
            window.required
        };
        let property = window.property.as_str();
        let prefix = format!(
            "{}s",
            keys::vertex_property_index_property_prefix(cell_id, property)
        );
        let order = if window.ascending {
            slatedb::IterationOrder::Ascending
        } else {
            slatedb::IterationOrder::Descending
        };
        let scan_options = remote_scan_options().with_order(order);
        let mut iter = budget
            .read_only_io(
                "experimental_ordered_vertex_index_seek",
                self.db.scan_prefix_range_with_options(
                    prefix.as_bytes(),
                    window.ascending_start_suffix(),
                    window.descending_end_suffix(),
                    &scan_options,
                ),
            )
            .await?;

        // Never reserve from the user-controlled window. Grow only when an
        // actual qualifying row is retained, after enforcing the row budget.
        let mut accepted = std::collections::BinaryHeap::<OrderedVertexCandidate>::new();
        let mut boundary: Option<String> = None;
        let mut last_seen: Option<(String, VertexId)> = None;
        let mut index_entries = 0_u64;
        let mut hydrate_us = 0_u64;
        let mut exhausted = false;
        // Two different reasons to stop, and only one of them is the caller's
        // business. `exhausted` reports that the index holds nothing further,
        // which is what lets a caller stop paging. Filling `required` ends this
        // page and nothing more, so it moves `stop` alone: a caller whose own
        // filter rejected rows out of this page has to be free to ask again.
        let mut stop = false;
        while !stop {
            let batch_capacity = required
                .saturating_sub(accepted.len())
                .clamp(1, QUERY_METADATA_HYDRATION_CONCURRENCY);
            let mut candidates: Vec<(String, VertexId)> = Vec::with_capacity(batch_capacity);
            while candidates.len() < batch_capacity {
                let Some(kv) = budget
                    .read_only_io("experimental_ordered_vertex_index_next", async {
                        iter.next().await.map_err(GraphError::from)
                    })
                    .await?
                else {
                    exhausted = true;
                    stop = true;
                    break;
                };
                budget.check("experimental_ordered_vertex_property_index")?;
                index_entries += 1;
                // Charged where the entry is read, because reading it is the
                // work: it counts whether it becomes a row, resumes past one an
                // earlier page returned, falls outside the bounds, or turns out
                // to be stale. Counting survivors instead would let a walk a
                // residual starves read without limit -- which is the walk this
                // limit exists for.
                self.charge_query_index_candidates(
                    "experimental_ordered_vertex_property_candidates",
                    budget,
                    1,
                )?;
                let key = String::from_utf8_lossy(&kv.key).into_owned();
                let (_cell, indexed_property, encoded, vertex_id) =
                    parse_vertex_property_index_key(&key)?;
                if indexed_property != property {
                    continue;
                }
                let Some(encoded) = encoded.strip_prefix('s').map(str::to_string) else {
                    // Only string entries share the `s` prefix; anything else
                    // means the scan prefix stopped matching.
                    exhausted = true;
                    stop = true;
                    break;
                };
                if window.past_window(&encoded) {
                    exhausted = true;
                    stop = true;
                    break;
                }
                if window.already_returned(&encoded, vertex_id) {
                    continue;
                }
                if accepted.len() >= required
                    && (window.index_order || boundary.as_deref() != Some(encoded.as_str()))
                {
                    stop = true;
                    break;
                }
                if !window.accepts_encoded(&encoded) {
                    continue;
                }
                candidates.push((encoded, vertex_id));
            }
            if candidates.is_empty() {
                continue;
            }
            let vertex_ids = candidates
                .iter()
                .map(|(_, vertex_id)| *vertex_id)
                .collect::<Vec<_>>();
            let hydrate_started = Instant::now();
            let metadata = self
                .vertex_metadata_batch_at(cell_id, &vertex_ids, read_epoch, budget)
                .await?;
            hydrate_us = hydrate_us.saturating_add(
                u64::try_from(hydrate_started.elapsed().as_micros()).unwrap_or(u64::MAX),
            );
            for ((encoded, vertex_id), (_, metadata)) in candidates.into_iter().zip(metadata) {
                if accepted.len() >= required
                    && (window.index_order || boundary.as_deref() != Some(encoded.as_str()))
                {
                    stop = true;
                    break;
                }
                // Past this point the candidate has been judged, however it is
                // judged, so a later page may resume after it. Advancing before
                // the break above would step over candidates this loop dropped
                // and `already_returned` would then skip them for good.
                last_seen = Some((encoded.clone(), vertex_id));
                // A stale index entry must not surface a value the vertex no
                // longer holds.
                let current = match metadata.properties.get(property) {
                    Some(VertexPropertyValue::String(value)) => {
                        OrderedStringVertexWindow::encode(value)
                    }
                    _ => continue,
                };
                if current != encoded {
                    continue;
                }
                if !window
                    .labels
                    .iter()
                    .all(|label| metadata.labels.contains(label.as_str()))
                {
                    continue;
                }
                let candidate = OrderedVertexCandidate {
                    key: (
                        OrderedCandidateKey::new(encoded.clone(), window.ascending),
                        OrderedCandidateKey::new(vertex_id, window.id_ascending),
                    ),
                    vertex_id,
                    metadata,
                };
                // Past `required` the page is finishing the run of entries
                // tying with its last one, ranked by this walk's own order, so
                // the worst held entry can give way to a better one. An
                // `index_order` page never gets here: it stops at `required`.
                let retain = accepted.len() < required;
                if retain {
                    self.ensure_query_intermediate_rows(
                        "experimental_ordered_vertex_property_rows",
                        accepted.len().saturating_add(1),
                    )?;
                    // Geometric growth is based on observed rows, capped by the
                    // configured retained-row budget and by the page's K.
                    if accepted.len() == accepted.capacity() {
                        let ceiling = required.min(self.limits.max_query_intermediate_rows);
                        let additional = accepted
                            .len()
                            .max(1)
                            .min(ceiling.saturating_sub(accepted.len()))
                            .max(1);
                        accepted.try_reserve_exact(additional).map_err(|error| {
                            GraphError::QueryAllocation {
                                operation: "experimental_ordered_vertex_property_rows",
                                reason: error.to_string(),
                            }
                        })?;
                    }
                    accepted.push(candidate);
                    if accepted.len() == required {
                        boundary = Some(encoded);
                    }
                } else if let Some(mut worst) = accepted.peek_mut() {
                    if candidate < *worst {
                        *worst = candidate;
                    }
                }
            }
        }
        // The caller's operator span declares these; outside one they are
        // silently dropped, which is the right behavior for a bare call.
        let span = tracing::Span::current();
        span.record("hydradb.query.scan.index_entries", index_entries);
        span.record("hydradb.query.scan.hydrate_us", hydrate_us);
        let vertices = accepted
            .into_sorted_vec()
            .into_iter()
            .map(|row| (row.vertex_id, row.metadata))
            .collect::<Vec<_>>();
        Ok(OrderedStringVertexPage {
            vertices,
            // Where the walk stopped in index order, not in the caller's sort
            // order, because that is what a later page resumes after.
            last: last_seen,
            exhausted,
            index_entries,
        })
    }
}

/// One page of `scan_ordered_string_vertex_window_at`.
#[cfg(feature = "experimental-cypher-engine")]
#[derive(Debug, Default)]
pub(super) struct OrderedStringVertexPage {
    pub(super) vertices: Vec<(VertexId, VertexMetadata)>,
    pub(super) last: Option<(String, VertexId)>,
    /// Nothing further lies within the window. Reported rather than inferred
    /// from a short page: a caller applying its own predicate cannot otherwise
    /// tell a page that ended from an index that did.
    pub(super) exhausted: bool,
    /// Index entries this page read, which is what its I/O costs. A page that
    /// resumes where the last one stopped reads about what it returns; one
    /// that restarts reads everything before it again.
    ///
    /// Only the tests that pin that cost read it: the walk records the same
    /// number on its own span from a local. Without this the library build
    /// under `server-runtime,experimental-cypher-engine` fails `-D dead-code`,
    /// which is what `just test-experimental-cypher` builds.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(super) index_entries: u64,
}
