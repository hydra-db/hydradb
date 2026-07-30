use super::topology_tail::{expand_range_with_overlay, GraphTopologyOverlay, GraphTopologyTail};
use super::*;
#[cfg(feature = "opencypher")]
use std::sync::atomic::AtomicU64;
#[cfg(feature = "opencypher")]
use std::time::Duration;
use tracing::Instrument as _;

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
    let span = tracing::info_span!("kernel.expand", turbolay.kernel = operation);
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
        validate_component("cell_id", cell_id)?;
        validate_component("edge_type", edge_type)?;
        let mut neighbors = BTreeSet::new();

        let prefix = keys::out_prefix(cell_id, edge_type, src);
        let mut iter = snapshot
            .scan_prefix_with_options(prefix.as_bytes(), .., &remote_scan_options())
            .await?;
        while let Some(kv) = iter.next().await? {
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            neighbors.insert(decode_edge_record(&key, &kv.value)?.dst);
        }

        let tombstone_prefix = keys::out_segment_tombstone_src_prefix(cell_id, edge_type, src);
        let mut tombstone_iter = snapshot
            .scan_prefix_with_options(tombstone_prefix.as_bytes(), .., &remote_scan_options())
            .await?;
        let mut tombstones = BTreeMap::new();
        while let Some(kv) = tombstone_iter.next().await? {
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
            }
        }

        let segment_prefix = keys::out_segment_src_prefix(cell_id, edge_type, src);
        let mut segment_iter = snapshot
            .scan_prefix_with_options(segment_prefix.as_bytes(), .., &remote_scan_options())
            .await?;
        while let Some(kv) = segment_iter.next().await? {
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            let segment = decode_out_edge_segment(&key, &kv.value)?;
            if segment.storage_sequence > topology_sequence {
                break;
            }
            for dst in segment.destinations.iter().copied() {
                if segment_edge_visible(segment.storage_sequence, tombstones.get(&dst).copied()) {
                    neighbors.insert(dst);
                }
            }
        }
        Ok(neighbors.into_iter().collect())
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

    pub(crate) async fn in_neighbors_in_storage_snapshot(
        &self,
        snapshot: &GraphStorageSnapshot,
        cell_id: &str,
        edge_type: &str,
        dst: VertexId,
    ) -> Result<Vec<VertexId>> {
        validate_component("cell_id", cell_id)?;
        validate_component("edge_type", edge_type)?;
        let prefix = keys::in_prefix(cell_id, edge_type, dst);
        let mut iter = snapshot
            .scan_prefix_with_options(prefix.as_bytes(), .., &remote_scan_options())
            .await?;
        let mut neighbors = BTreeSet::new();
        while let Some(kv) = iter.next().await? {
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            neighbors.insert(decode_edge_record(&key, &kv.value)?.src);
        }
        Ok(neighbors.into_iter().collect())
    }

    pub async fn execute_cypher(&self, context: QueryContext, query: &str) -> Result<QueryOutput> {
        self.execute_opencypher(context, query).await
    }

    pub async fn execute_cypher_rows(
        &self,
        context: QueryContext,
        query: &str,
    ) -> Result<QueryResultSet> {
        self.execute_opencypher_rows(context, query).await
    }

    pub async fn execute_cypher_rows_page(
        &self,
        context: QueryContext,
        query: &str,
        cursor: Option<QueryCursorToken>,
        page_size: usize,
    ) -> Result<QueryResultPage> {
        self.execute_opencypher_rows_page(context, query, cursor, page_size)
            .await
    }

    pub async fn execute_opencypher(
        &self,
        context: QueryContext,
        query: &str,
    ) -> Result<QueryOutput> {
        #[cfg(feature = "opencypher")]
        {
            if let Some(parsed) =
                parse_opencypher_mutation_query_with_parameters(query, &context.parameters)?
            {
                let result = self
                    .execute_parsed_opencypher_mutation(context, parsed)
                    .await?;
                return Ok(QueryOutput::Mutation(result));
            }
            let parsed = self
                .parsed_opencypher_row_query(&context.cell_id, query, &context.parameters)
                .await?;
            let context = merge_opencypher_window(context, opencypher_outer_window(&parsed))?;
            let result_set = self
                .execute_parsed_opencypher_rows(context, parsed.clone())
                .await?;
            Ok(query_result_set_to_output(&parsed, result_set))
        }
        #[cfg(not(feature = "opencypher"))]
        {
            let _ = (context, query);
            Err(GraphError::UnsupportedQuery {
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
            let parsed = self
                .parsed_opencypher_row_query(&context.cell_id, query, &context.parameters)
                .await?;
            let context = merge_opencypher_window(context, opencypher_outer_window(&parsed))?;
            self.execute_parsed_opencypher_rows(context, parsed).await
        }
        #[cfg(not(feature = "opencypher"))]
        {
            let _ = (context, query);
            Err(GraphError::UnsupportedQuery {
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
            let parsed = self
                .parsed_opencypher_row_query(&context.cell_id, query, &context.parameters)
                .await?;
            let context = merge_opencypher_window(context, opencypher_outer_window(&parsed))?;
            if context.read_epoch.is_some() && context.validated_read_epoch().is_none() {
                return Err(GraphError::UnsupportedQuery {
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
                    self.execute_parsed_opencypher_rows_page(context, parsed, cursor, page_size),
                )
                .await;
            }
            self.execute_parsed_opencypher_rows_page(context, parsed, cursor, page_size)
                .await
        }
        #[cfg(not(feature = "opencypher"))]
        {
            let _ = (context, query, cursor, page_size);
            Err(GraphError::UnsupportedQuery {
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
        match self
            .try_execute_graph_kernel_opencypher_rows_page(
                &context,
                &parsed,
                cursor_offset,
                page_size,
            )
            .await
        {
            Ok(Some(page)) => {
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
                self.record_streaming_query_rows_success(page.rows.len(), started);
                return Ok(page);
            }
            Ok(None) => {}
            Err(err) => {
                self.record_streaming_query_rows_failure(started, &err);
                return Err(err);
            }
        }

        let context = self.query_page_context(context, cursor_offset, page_size)?;
        parsed.window = QueryWindow::default();
        let mut result_set = self.execute_parsed_opencypher_rows(context, parsed).await?;
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
        Ok(QueryResultPage::new(
            result_set.columns,
            result_set.rows,
            next_cursor,
        ))
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
                dialect: "OpenCypher",
                feature: "historical graph epochs are not storage snapshots; execute against a current SlateDB snapshot".to_string(),
            });
        }
        let parsed = self
            .parsed_opencypher_row_query(&context.cell_id, query, &context.parameters)
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
    ) -> Result<ParsedRowQuery> {
        if !parameters.is_empty() {
            return parse_opencypher_row_query_with_parameters(query, parameters);
        }
        let key = ParsedRowQueryCacheKey::new(query);
        if let Some(parsed) = self.parsed_row_query_cache.lock().await.get(&key) {
            self.cache_metrics
                .record_hit(GraphCacheKind::ParsedRowQuery);
            return Ok(parsed);
        }

        self.cache_metrics
            .record_miss(GraphCacheKind::ParsedRowQuery);
        let parsed = parse_opencypher_row_query_with_parameters(query, parameters)?;
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
            turbolay.cell_id = %context.cell_id,
            turbolay.read_epoch = tracing::field::Empty,
            turbolay.query.rows_returned = tracing::field::Empty,
            error.class = tracing::field::Empty,
            turbolay.sampling.tail_keep = tracing::field::Empty,
        )
    )]
    async fn execute_parsed_opencypher_rows(
        &self,
        context: QueryContext,
        query: ParsedRowQuery,
    ) -> Result<QueryResultSet> {
        if context.read_epoch.is_some() && context.validated_read_epoch().is_none() {
            return Err(GraphError::UnsupportedQuery {
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
                turbolay.cell_id = %context.cell_id,
                refreshed_reader = context.uses_refreshed_reader(),
            ))
            .await;
            match snapshot {
                Ok(snapshot) => {
                    let read_epoch = snapshot.seq();
                    tracing::Span::current().record("turbolay.read_epoch", read_epoch);
                    let context = context.with_validated_storage_read_epoch(read_epoch, read_epoch);
                    GraphStore::scope_snapshot(
                        snapshot,
                        self.execute_parsed_opencypher_rows_inner(context, query),
                    )
                    .await
                }
                Err(err) => Err(err),
            }
        } else {
            self.execute_parsed_opencypher_rows_inner(context, query)
                .await
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
                span.record("turbolay.query.rows_returned", result_set.rows.len() as u64);
                if let Some(read_epoch) = result_set.read_epoch {
                    span.record("turbolay.read_epoch", read_epoch);
                }
            }
            Err(err) => {
                self.operation_metrics.record_query_rows_failure(err);
                span.record("error.class", err.class());
                span.record("turbolay.sampling.tail_keep", "error");
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
            .try_execute_relationship_rows_query(cell_id, read_epoch, &query, window, budget)
            .await?
        {
            return Ok(result);
        }

        let bindings = if query.pattern_groups.is_empty() {
            let mut bindings = self
                .match_row_patterns(cell_id, &query.patterns, read_epoch, budget)
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
            push_projected_query_row(&mut projected, row, sort_keys, budget)?;
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
            budget.account_result_row(&row)?;
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
                budget.account_result_row(&row)?;
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
            push_projected_query_row(&mut projected, row, sort_keys, budget)?;
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
            push_projected_query_row(&mut projected, row, sort_keys, budget)?;
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
        budget.account_result_row(&row)?;
        let rows = vec![row];
        Ok(Some(QueryResultSet::new(
            query.columns.clone(),
            self.apply_query_row_window(rows, window)?,
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

        let mut result = QueryMutationResult {
            matched_rows: bindings.len() as u64,
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
                    let (deleted, noops) = self
                        .delete_bound_relationships_batch(&context, relationships, &budget)
                        .await?;
                    result.deleted_edges = result.deleted_edges.saturating_add(deleted);
                    result.noops = result.noops.saturating_add(noops);
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
                                dialect: "OpenCypher",
                                feature: format!(
                                    "DELETE references unbound relationship {binding}"
                                ),
                            });
                        };
                        relationships.insert(relationship.clone());
                    }
                    let (deleted, noops) = self
                        .delete_bound_relationships_batch(&context, relationships, &budget)
                        .await?;
                    result.deleted_edges = result.deleted_edges.saturating_add(deleted);
                    result.noops = result.noops.saturating_add(noops);
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
        budget: &QueryBudget,
    ) -> Result<(u64, u64)> {
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
        if !structural.is_empty() {
            budget.check("cypher_delete_edge_batch")?;
            let batch = self
                .delete_edge_mutations_batch(&context.cell_id, structural)
                .await?;
            deleted = deleted.saturating_add(batch.deleted);
            noops = noops.saturating_add(batch.already_deleted);
        }
        for (mutation, relationship_id) in identified {
            budget.check("cypher_delete_relationship")?;
            if self
                .delete_relationship(mutation, relationship_id)
                .await?
                .deleted
            {
                deleted = deleted.saturating_add(1);
            } else {
                noops = noops.saturating_add(1);
            }
        }
        Ok((deleted, noops))
    }

    #[cfg(feature = "opencypher")]
    pub(crate) async fn delete_relationships_by_property_values_batch(
        &self,
        context: &QueryContext,
        edge_type: &str,
        property: &str,
        values: Vec<VertexPropertyValue>,
    ) -> Result<(u64, u64)> {
        validate_component("cell_id", &context.cell_id)?;
        validate_component("edge_type", edge_type)?;
        validate_component("property", property)?;
        let budget = QueryBudget::new(
            context.max_runtime_ms.or(self.limits.max_query_runtime_ms),
            context.cancellation_token.clone(),
        );
        let read_epoch = self.current_epoch(&context.cell_id).await?;
        let mut relationships = BTreeSet::new();
        for value in values {
            budget.check("cypher_delete_relationship_property_batch")?;
            for encoded in equivalent_property_index_keys(&value) {
                for (src, dst, relationship_id) in self
                    .scan_relationship_property_index_current(
                        &context.cell_id,
                        edge_type,
                        property,
                        &encoded,
                        &budget,
                    )
                    .await?
                {
                    let relationship = BoundRelationship {
                        edge_type: edge_type.to_string(),
                        src,
                        dst,
                        relationship_id: Some(relationship_id),
                    };
                    let metadata = self
                        .relationship_metadata_at(
                            &context.cell_id,
                            &relationship,
                            read_epoch,
                            &budget,
                        )
                        .await?;
                    if metadata
                        .properties
                        .get(property)
                        .is_some_and(|existing| vertex_property_values_equal(existing, &value))
                    {
                        relationships.insert(relationship);
                    }
                }
                for (src, dst) in self
                    .scan_edge_property_index_current(
                        &context.cell_id,
                        edge_type,
                        property,
                        &encoded,
                        &budget,
                    )
                    .await?
                {
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
                        continue;
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
                    if metadata
                        .properties
                        .get(property)
                        .is_some_and(|existing| vertex_property_values_equal(existing, &value))
                    {
                        relationships.insert(BoundRelationship {
                            edge_type: edge_type.to_string(),
                            src,
                            dst,
                            relationship_id: None,
                        });
                    }
                }
            }
        }
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
        if !structural_delete_guards.is_empty() {
            budget.check("cypher_delete_relationship_reserve_scope")?;
            self.reserve_edge_delete_noops_batch(
                &context.cell_id,
                structural_delete_guards.into_values(),
            )
            .await?;
        }
        self.delete_bound_relationships_batch(context, relationships, &budget)
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
                            dialect: "OpenCypher",
                            feature: format!("metadata for relationship {binding} was not loaded"),
                        })?;
                    metadata.properties.remove(property);
                    return Ok(());
                }
                RowMutationAction::SetLabels { .. } | RowMutationAction::RemoveLabels { .. } => {
                    return Err(GraphError::UnsupportedQuery {
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
                            target: "slatedb_graph_kernel",
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
                dialect: "OpenCypher",
                feature: "MATCH requires at least one executable pattern".to_string(),
            });
        }

        let groups = self
            .optimize_row_match_groups_with_stats(cell_id, groups, read_epoch)
            .await?;
        let mut rows = vec![BindingRow::default()];
        for group in &groups {
            budget.check("cypher_match_group")?;
            if group.patterns.is_empty() {
                return Err(GraphError::UnsupportedQuery {
                    dialect: "OpenCypher",
                    feature: "MATCH requires at least one executable pattern".to_string(),
                });
            }

            let mut group_rows = Vec::new();
            for row in rows {
                let initial_rows = if let Some(seed_rows) = self
                    .relationship_predicate_seed_rows(cell_id, group, read_epoch, budget, &row)
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
    async fn relationship_predicate_seed_rows(
        &self,
        cell_id: &str,
        group: &RowMatchGroup,
        read_epoch: StorageSequence,
        budget: &QueryBudget,
        input: &BindingRow,
    ) -> Result<Option<Vec<BindingRow>>> {
        let Some(constraint) = group
            .predicate
            .as_ref()
            .and_then(row_predicate_relationship_property_constraint)
        else {
            return Ok(None);
        };
        if input.relationships.contains_key(&constraint.binding)
            || read_epoch != self.current_epoch(cell_id).await?
        {
            return Ok(None);
        }
        let Some(edge) = group.patterns.iter().find_map(|pattern| match pattern {
            RowPattern::Edge(edge)
                if edge.binding.as_deref() == Some(constraint.binding.as_str())
                    && edge.hop_range.is_none()
                    && edge.properties.is_empty() =>
            {
                Some(edge)
            }
            RowPattern::Node(_) | RowPattern::Edge(_) => None,
        }) else {
            return Ok(None);
        };

        let mut pairs = BTreeSet::new();
        for value in &constraint.values {
            for pair in self
                .scan_edge_property_index_at(
                    cell_id,
                    &edge.edge_type,
                    &constraint.property,
                    value,
                    read_epoch,
                    budget,
                )
                .await?
            {
                pairs.insert(pair);
                self.ensure_query_index_candidates(
                    "cypher_edge_predicate_index_candidates",
                    pairs.len(),
                )?;
            }
        }

        let mut candidates = Vec::new();
        let mut metadata_cache = BTreeMap::new();
        let mut edge_metadata_cache = BTreeMap::new();
        {
            let mut state = EdgeRowMatchState {
                cell_id,
                read_epoch,
                rows: &mut candidates,
                metadata_cache: &mut metadata_cache,
                edge_metadata_cache: &mut edge_metadata_cache,
                budget,
            };
            for (src, dst) in pairs {
                budget.check("cypher_edge_predicate_index_rows")?;
                self.push_matching_edge_row(edge, src, dst, &mut state)
                    .await?;
            }
        }

        let mut rows = Vec::with_capacity(candidates.len());
        for candidate in candidates {
            budget.check("cypher_edge_predicate_index_join")?;
            if let Some(joined) = input.join(&candidate) {
                self.push_binding_row(&mut rows, joined, "cypher_edge_predicate_index_seed_rows")?;
            }
        }
        Ok(Some(rows))
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
        let patterns = self
            .optimize_row_patterns_with_stats(cell_id, patterns, read_epoch, &initial_bindings)
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
        if edge.hop_range.is_some() || rows.is_empty() {
            return Ok(None);
        }

        let current_epoch = self.current_epoch(cell_id).await?;
        let latest_snapshot = read_epoch == current_epoch;
        let mut next_rows = Vec::new();
        let mut metadata_cache = BTreeMap::new();
        let mut edge_metadata_cache = BTreeMap::new();
        for row in rows {
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
            for (relationship, relationship_metadata) in relationships {
                let Some(mut matched) =
                    BindingRow::from_relationship(&bound_edge, relationship, relationship_metadata)
                else {
                    continue;
                };
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
        let matches = self
            .match_row_pattern(cell_id, pattern, read_epoch, budget)
            .await?;
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
                dialect: "OpenCypher",
                feature: "node-only MATCH requires an id, label, or property predicate".to_string(),
            });
        };
        self.ensure_query_index_candidates("cypher_node_candidates", vertices.len())?;
        let mut rows = Vec::with_capacity(vertices.len());
        let mut metadata_cache = BTreeMap::new();
        for vertex_id in vertices {
            budget.check("cypher_node_rows")?;
            if let Some(mut row) = BindingRow::from_node(node, vertex_id) {
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
        }
        Ok(rows)
    }

    #[cfg(feature = "opencypher")]
    async fn match_edge_row_pattern(
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

        let access = self
            .best_row_edge_access_with_stats(cell_id, edge, read_epoch, &BTreeSet::new())
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
        let mut rows = Vec::new();
        let mut metadata_cache = BTreeMap::new();
        let mut edge_metadata_cache = BTreeMap::new();
        let mut state = EdgeRowMatchState {
            cell_id,
            read_epoch,
            rows: &mut rows,
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
        Ok(rows)
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
        let mut rows = Vec::new();
        let mut metadata_cache = BTreeMap::new();
        let mut edge_metadata_cache = BTreeMap::new();
        let mut state = EdgeRowMatchState {
            cell_id,
            read_epoch,
            rows: &mut rows,
            metadata_cache: &mut metadata_cache,
            edge_metadata_cache: &mut edge_metadata_cache,
            budget,
        };
        for (src, dst) in pairs {
            budget.check("cypher_edge_property_rows")?;
            self.push_matching_edge_row(edge, src, dst, &mut state)
                .await?;
        }
        Ok(rows)
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
        let mut rows = Vec::new();
        let mut metadata_cache = BTreeMap::new();
        let mut edge_metadata_cache = BTreeMap::new();
        let mut scanned_edges = 0_u64;
        {
            let mut state = EdgeRowMatchState {
                cell_id,
                read_epoch,
                rows: &mut rows,
                metadata_cache: &mut metadata_cache,
                edge_metadata_cache: &mut edge_metadata_cache,
                budget,
            };
            for src in sources {
                budget.check("cypher_edge_sources")?;
                let neighbors = self
                    .out_neighbors_at_for_query(cell_id, &edge.edge_type, src, read_epoch, budget)
                    .await?;
                scanned_edges = scanned_edges.saturating_add(neighbors.len() as u64);
                self.ensure_query_scan_edges("cypher_edge_neighbor_scan", scanned_edges)?;
                for dst in neighbors {
                    self.push_matching_edge_row(edge, src, dst, &mut state)
                        .await?;
                }
            }
        }
        Ok(rows)
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
        let mut rows = Vec::new();
        let mut metadata_cache = BTreeMap::new();
        let mut edge_metadata_cache = BTreeMap::new();
        let mut scanned_edges = 0_u64;
        {
            let mut state = EdgeRowMatchState {
                cell_id,
                read_epoch,
                rows: &mut rows,
                metadata_cache: &mut metadata_cache,
                edge_metadata_cache: &mut edge_metadata_cache,
                budget,
            };
            for dst in destinations {
                budget.check("cypher_edge_destinations")?;
                let neighbors = self
                    .in_neighbors_at_for_query(cell_id, &edge.edge_type, dst, read_epoch, budget)
                    .await?;
                scanned_edges = scanned_edges.saturating_add(neighbors.len() as u64);
                self.ensure_query_scan_edges("cypher_edge_reverse_neighbor_scan", scanned_edges)?;
                for src in neighbors {
                    self.push_matching_edge_row(edge, src, dst, &mut state)
                        .await?;
                }
            }
        }
        Ok(rows)
    }

    #[cfg(feature = "opencypher")]
    async fn match_edge_row_pattern_full_scan(
        &self,
        cell_id: &str,
        edge: &RowEdgePattern,
        read_epoch: StorageSequence,
        budget: &QueryBudget,
    ) -> Result<Vec<BindingRow>> {
        let mut rows = Vec::new();
        let mut metadata_cache = BTreeMap::new();
        let mut edge_metadata_cache = BTreeMap::new();
        let records = self
            .edges_at_with_budget(cell_id, &edge.edge_type, read_epoch, Some(budget))
            .await?;
        self.ensure_query_scan_edges("cypher_edge_full_scan", records.len() as u64)?;
        {
            let mut state = EdgeRowMatchState {
                cell_id,
                read_epoch,
                rows: &mut rows,
                metadata_cache: &mut metadata_cache,
                edge_metadata_cache: &mut edge_metadata_cache,
                budget,
            };
            for record in records {
                budget.check("cypher_edge_full_scan")?;
                self.push_matching_edge_row(edge, record.src, record.dst, &mut state)
                    .await?;
            }
        }
        Ok(rows)
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
            let Some(mut row) = BindingRow::from_relationship(edge, relationship, metadata) else {
                continue;
            };
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
                edge,
                state.edge_metadata_cache,
                state.budget,
            )
            .await?;
            if row_matches_edge_pattern(&row, edge)? {
                self.push_binding_row(state.rows, row, "cypher_edge_rows")?;
            }
        }
        Ok(())
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
        for dst in vertices {
            budget.check("cypher_reachable_rows")?;
            if matches!(edge.dst.id, Some(fixed_dst) if fixed_dst != dst) {
                continue;
            }
            if let Some(mut row) = BindingRow::from_edge(edge, src, dst) {
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
        let access = self
            .best_row_node_access_with_stats(cell_id, pattern, &BTreeSet::new())
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
    async fn vertex_metadata_at(
        &self,
        cell_id: &str,
        vertex_id: VertexId,
        _read_epoch: StorageSequence,
        _budget: &QueryBudget,
    ) -> Result<VertexMetadata> {
        validate_component("cell_id", cell_id)?;
        let key = keys::vertex(cell_id, vertex_id);
        match self.read_remote(&key).await? {
            Some(value) => decode_vertex_metadata(&key, &value),
            None => Ok(VertexMetadata::default()),
        }
    }

    #[cfg(feature = "opencypher")]
    async fn edge_metadata_at(
        &self,
        cell_id: &str,
        edge_type: &str,
        src: VertexId,
        dst: VertexId,
        _read_epoch: StorageSequence,
        _budget: &QueryBudget,
    ) -> Result<EdgeMetadata> {
        validate_component("cell_id", cell_id)?;
        validate_component("edge_type", edge_type)?;
        let key = keys::edge_metadata(cell_id, edge_type, src, dst);
        match self.read_remote(&key).await? {
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
        let Some(value) = self.read_remote(&key).await? else {
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
            return source_relationship_id_bindings_from_dsts(
                edge,
                src,
                cached.as_slice(),
                self,
                budget,
            );
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
        source_relationship_id_bindings_from_dsts(edge, src, cached.as_slice(), self, budget)
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
    async fn scan_edge_property_index_at(
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
    #[cfg(feature = "opencypher")]
    async fn scan_edge_property_index_current(
        &self,
        cell_id: &str,
        edge_type: &str,
        property: &str,
        encoded: &str,
        budget: &QueryBudget,
    ) -> Result<Vec<(VertexId, VertexId)>> {
        let mut iter = self
            .scan_remote_prefix(&keys::edge_property_index_prefix(
                cell_id, edge_type, property, encoded,
            ))
            .await?;
        let mut edges = Vec::new();
        while let Some(kv) = iter.next().await? {
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
        let mut iter = self
            .scan_remote_prefix(&keys::relationship_property_index_prefix(
                cell_id, edge_type, property, encoded,
            ))
            .await?;
        let mut relationships = Vec::new();
        while let Some(kv) = iter.next().await? {
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
        let mut iter = self
            .scan_remote_prefix(&keys::relationship_property_index_edge_prefix(
                cell_id, edge_type, property, encoded, src, dst,
            ))
            .await?;
        let mut relationships = Vec::new();
        while let Some(kv) = iter.next().await? {
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
    async fn scan_vertex_label_index_at(
        &self,
        cell_id: &str,
        label: &str,
        _read_epoch: StorageSequence,
        budget: &QueryBudget,
    ) -> Result<Vec<VertexId>> {
        validate_component("cell_id", cell_id)?;
        validate_component("label", label)?;
        self.scan_vertex_label_index_current(cell_id, label, budget)
            .await
    }

    #[cfg(feature = "opencypher")]
    async fn scan_vertex_label_index_current(
        &self,
        cell_id: &str,
        label: &str,
        budget: &QueryBudget,
    ) -> Result<Vec<VertexId>> {
        let mut iter = self
            .scan_remote_prefix(&keys::vertex_label_prefix(cell_id, label))
            .await?;
        let mut vertices = Vec::new();
        while let Some(kv) = iter.next().await? {
            budget.check("cypher_vertex_label_index")?;
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            vertices.push(decode_u64(&key, &kv.value)?);
            self.ensure_query_index_candidates(
                "cypher_vertex_label_index_candidates",
                vertices.len(),
            )?;
        }
        Ok(vertices)
    }

    #[cfg(feature = "opencypher")]
    async fn scan_vertex_property_index_at(
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
    async fn scan_vertex_property_index_current(
        &self,
        cell_id: &str,
        property: &str,
        encoded: &str,
        budget: &QueryBudget,
    ) -> Result<Vec<VertexId>> {
        let mut iter = self
            .scan_remote_prefix(&keys::vertex_property_index_prefix(
                cell_id, property, encoded,
            ))
            .await?;
        let mut vertices = Vec::new();
        while let Some(kv) = iter.next().await? {
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
                    let metadata = self
                        .vertex_metadata_at(cell_id, vertex_id, read_epoch, budget)
                        .await?;
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
                        self.relationship_metadata_at(cell_id, &relationship, read_epoch, budget)
                            .await?
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
        let max = self.limits.max_query_result_vertices;
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
        let max = self.limits.max_query_result_vertices;
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
                    let (mut vertices, edge_visits) = self
                        .reachable_vertices_window_in_hop_range_at(ReachableWindowRequest {
                            cell_id: &context.cell_id,
                            edge_type: &request.edge.edge_type,
                            src: request.src,
                            hop_range: request.hop_range,
                            read_epoch,
                            window: page_window,
                            ascending: request.ascending,
                            budget: &budget,
                        })
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
        let max = self.limits.max_query_result_vertices;
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
    fn ensure_query_index_candidates(
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
        if let Some(result) = self
            .reachable_vertices_window_with_compiled_graph_kernel(&request)
            .await?
        {
            return Ok(result);
        }
        let (mut vertices, edge_visits) = self
            .reachable_vertices_in_hop_range_at(
                request.cell_id,
                request.edge_type,
                request.src,
                request.hop_range,
                request.read_epoch,
                request.budget,
            )
            .await?;
        graph_kernel_order_vertices(&mut vertices, request.ascending);
        Ok((
            self.apply_query_window(vertices, request.window)?,
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

    #[cfg(feature = "opencypher")]
    async fn reachable_vertices_window_with_compiled_graph_kernel(
        &self,
        request: &ReachableWindowRequest<'_>,
    ) -> Result<Option<(Vec<VertexId>, u64)>> {
        let _ = request;
        if crate::sparse_kernel::default_matrix_kernel(&self.cache_policy)
            == SparseKernelBackend::Adjacency
        {
            return Ok(None);
        }
        {
            let (min_hops, max_hops) = request.hop_range;
            request
                .budget
                .check("cypher_graphblas_window_artifact_lookup")?;
            let Some(artifact) = self
                .traced_latest_matrix_artifact(
                    request.cell_id,
                    request.edge_type,
                    request.read_epoch,
                )
                .await?
            else {
                return Ok(None);
            };
            request
                .budget
                .check("cypher_graphblas_window_compiled_matrix")?;
            let Some((compiled, overlay, rebuilt)) = self
                .compiled_graphblas_query_snapshot(
                    request.cell_id,
                    request.edge_type,
                    artifact.base_epoch,
                    request.read_epoch,
                    request.budget,
                )
                .await?
            else {
                return Ok(None);
            };
            self.record_graphblas_snapshot(rebuilt);
            let src = request.src;
            let window = crate::sparse_kernel::SparseTraversalWindow {
                skip: request.window.skip,
                limit: request.window.limit,
                ascending: request.ascending,
            };
            let traversal = run_graph_compute(
                Arc::clone(&self.operation_metrics),
                "graphblas_expand_range_window",
                move || {
                    if let Some(overlay) = overlay {
                        let traversal = expand_range_with_overlay(
                            &compiled,
                            &overlay,
                            &[src],
                            min_hops,
                            max_hops,
                        )?;
                        let mut vertices = traversal.vertices;
                        graph_kernel_order_vertices(&mut vertices, window.ascending);
                        let start = usize::try_from(window.skip)
                            .unwrap_or(usize::MAX)
                            .min(vertices.len());
                        let end = window.limit.map_or(vertices.len(), |limit| {
                            start.saturating_add(limit).min(vertices.len())
                        });
                        Ok(crate::sparse_kernel::SparseTraversal {
                            vertices: vertices[start..end].to_vec(),
                            edge_visits: traversal.edge_visits,
                            backend: traversal.backend,
                        })
                    } else {
                        let empty_adjacency = BTreeMap::new();
                        crate::sparse_kernel::expand_range_window_compiled_graphblas(
                            &compiled,
                            &empty_adjacency,
                            &[src],
                            min_hops,
                            max_hops,
                            window,
                        )
                    }
                },
            )
            .await?;
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
    async fn traced_latest_matrix_artifact(
        &self,
        cell_id: &str,
        edge_type: &str,
        read_epoch: StorageSequence,
    ) -> Result<Option<MatrixArtifact>> {
        let span = tracing::info_span!(
            "artifact.lookup",
            turbolay.cell_id = %cell_id,
            turbolay.edge_type = %edge_type,
            turbolay.read_epoch = read_epoch,
            turbolay.base_sequence = tracing::field::Empty,
            turbolay.outcome = tracing::field::Empty,
            error.class = tracing::field::Empty,
            turbolay.sampling.tail_keep = tracing::field::Empty,
        );
        let artifact = self
            .latest_matrix_artifact(cell_id, edge_type, read_epoch)
            .instrument(span.clone())
            .await;
        match &artifact {
            Ok(Some(artifact)) => {
                span.record("turbolay.outcome", "hit");
                span.record("turbolay.base_sequence", artifact.base_epoch);
            }
            // Absence is the interesting case: it means the read fell off the
            // compiled path and back onto a scan.
            Ok(None) => {
                span.record("turbolay.outcome", "miss");
            }
            Err(err) => {
                span.record("error.class", err.class());
                span.record("turbolay.sampling.tail_keep", "error");
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
                    turbolay.cell_id = %cell_id,
                    turbolay.edge_type = %edge_type,
                    turbolay.read_epoch = read_epoch,
                    turbolay.base_sequence = generation.base_sequence,
                    turbolay.outcome = tracing::field::Empty,
                    error.class = tracing::field::Empty,
                    turbolay.sampling.tail_keep = tracing::field::Empty,
                );
                let tail = self
                    .topology_tail_since(&generation, storage_snapshot.as_ref(), read_epoch, budget)
                    .instrument(tail_span.clone())
                    .await;
                match &tail {
                    Ok(GraphTopologyTail::Complete(_)) => {
                        tail_span.record("turbolay.outcome", "complete");
                    }
                    Ok(GraphTopologyTail::Unavailable) => {
                        tail_span.record("turbolay.outcome", "unavailable");
                    }
                    Err(err) => {
                        tail_span.record("error.class", err.class());
                        tail_span.record("turbolay.sampling.tail_keep", "error");
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

    fn record_graphblas_snapshot(&self, rebuilt: bool) {
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
        let mut neighbors = Vec::new();
        for edge in self
            .edges_at_with_budget(cell_id, edge_type, read_epoch, Some(budget))
            .await?
        {
            budget.check("query_out_neighbors_scan")?;
            if edge.src == src {
                neighbors.push(edge.dst);
            }
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
    async fn in_neighbors_at_for_query(
        &self,
        cell_id: &str,
        edge_type: &str,
        dst: VertexId,
        read_epoch: StorageSequence,
        budget: &QueryBudget,
    ) -> Result<Vec<VertexId>> {
        validate_component("cell_id", cell_id)?;
        validate_component("edge_type", edge_type)?;
        let current_epoch = self.current_epoch(cell_id).await?;
        if !self.writes_reverse_index() || read_epoch != current_epoch {
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
        let prefix = keys::in_prefix(cell_id, edge_type, dst);
        let mut iter = self.scan_remote_prefix(&prefix).await?;
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
}

impl QueryBudget {
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
        }
    }

    #[cfg(feature = "opencypher")]
    fn with_max_result_bytes(mut self, max_result_bytes: Option<u64>) -> Self {
        self.max_result_bytes = max_result_bytes;
        self
    }

    #[cfg(feature = "opencypher")]
    fn account_result_row(&self, row: &QueryRow) -> Result<()> {
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
    QueryStatsRecord::histogram(
        total,
        read_epoch,
        graph_now_millis(),
        buckets.len() as u64,
        most_common,
    )
}

#[cfg(feature = "opencypher")]
fn validate_query_stats_refresh_kind(kind: &QueryStatsRefreshKind) -> Result<()> {
    match kind {
        QueryStatsRefreshKind::Cardinality(QueryCardinalityStatsKind::EdgeType { edge_type }) => {
            validate_component("edge_type", edge_type)
        }
        QueryStatsRefreshKind::Cardinality(QueryCardinalityStatsKind::VertexLabel { label }) => {
            validate_component("label", label)
        }
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
struct EdgeRowMatchState<'a> {
    cell_id: &'a str,
    read_epoch: StorageSequence,
    rows: &'a mut Vec<BindingRow>,
    metadata_cache: &'a mut BTreeMap<VertexId, VertexMetadata>,
    edge_metadata_cache: &'a mut BTreeMap<BoundRelationship, EdgeMetadata>,
    budget: &'a QueryBudget,
}

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
    if edge.hop_range.is_some()
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
        budget.account_result_row(&row)?;
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
    if edge.binding.is_some()
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
    if edge.hop_range.is_some()
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
    if edge.hop_range.is_some()
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
                | RowProjection::Aggregate { .. } => false,
            }),
        RowSortExpression::Property { .. } | RowSortExpression::CountAll => false,
    })
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
    if edge.hop_range.is_some()
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
        RowProjection::CountAll | RowProjection::Aggregate { .. } => false,
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
                RowProjection::CountAll | RowProjection::Aggregate { .. } => false,
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
        RowProjection::Property { .. } | RowProjection::Aggregate { .. } => None,
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
impl BindingRow {
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
                dialect: "OpenCypher",
                feature: format!("variable {binding} is null"),
            });
        }
        self.values
            .get(binding)
            .copied()
            .ok_or_else(|| GraphError::UnsupportedQuery {
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
fn push_projected_query_row(
    projected: &mut Vec<ProjectedQueryRow>,
    row: QueryRow,
    sort_keys: Vec<QueryValue>,
    budget: &QueryBudget,
) -> Result<()> {
    budget.account_result_row(&row)?;
    projected.push(ProjectedQueryRow { row, sort_keys });
    Ok(())
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
            dialect: "OpenCypher",
            feature: "relationship pattern has no bound identity".to_string(),
        });
    };
    if matches.next().is_some() {
        return Err(GraphError::UnsupportedQuery {
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
    Ok(match predicate {
        RowPredicate::Compare { left, op, right } => compare_row_values(
            eval_row_expression(row, left)?,
            *op,
            eval_row_expression(row, right)?,
        )?,
        RowPredicate::StartsWith { expression, prefix } => {
            match eval_row_expression(row, expression)? {
                RowScalarValue::Value(VertexPropertyValue::String(value)) => {
                    value.starts_with(prefix)
                }
                RowScalarValue::Value(_) | RowScalarValue::Missing => false,
            }
        }
        RowPredicate::And(left, right) => {
            row_predicate_matches(row, left)? && row_predicate_matches(row, right)?
        }
        RowPredicate::Or(left, right) => {
            row_predicate_matches(row, left)? || row_predicate_matches(row, right)?
        }
        RowPredicate::Not(inner) => !row_predicate_matches(row, inner)?,
    })
}

#[cfg(feature = "opencypher")]
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct RowPredicateRelationshipPropertyConstraint {
    pub(super) binding: String,
    pub(super) property: String,
    pub(super) values: Vec<VertexPropertyValue>,
}

#[cfg(feature = "opencypher")]
pub(super) fn row_predicate_relationship_property_constraint(
    predicate: &RowPredicate,
) -> Option<RowPredicateRelationshipPropertyConstraint> {
    match predicate {
        RowPredicate::Compare {
            left,
            op: RowComparisonOp::Eq,
            right,
        } => row_property_literal_equality(left, right)
            .or_else(|| row_property_literal_equality(right, left)),
        RowPredicate::And(left, right) => row_predicate_relationship_property_constraint(left)
            .or_else(|| row_predicate_relationship_property_constraint(right)),
        RowPredicate::Or(left, right) => {
            let mut left = row_predicate_relationship_property_constraint(left)?;
            let right = row_predicate_relationship_property_constraint(right)?;
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
        RowPredicate::Compare { .. } | RowPredicate::StartsWith { .. } | RowPredicate::Not(_) => {
            None
        }
    }
}

#[cfg(feature = "opencypher")]
fn row_property_literal_equality(
    property_expression: &RowExpression,
    literal_expression: &RowExpression,
) -> Option<RowPredicateRelationshipPropertyConstraint> {
    let RowExpression::Property { binding, property } = property_expression else {
        return None;
    };
    let RowExpression::Literal(value) = literal_expression else {
        return None;
    };
    Some(RowPredicateRelationshipPropertyConstraint {
        binding: binding.clone(),
        property: property.clone(),
        values: vec![value.clone()],
    })
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
    }
}

#[cfg(feature = "opencypher")]
fn compare_row_values(
    left: RowScalarValue,
    op: RowComparisonOp,
    right: RowScalarValue,
) -> Result<bool> {
    let (RowScalarValue::Value(left), RowScalarValue::Value(right)) = (left, right) else {
        return Ok(false);
    };
    compare_vertex_property_values(&left, op, &right)
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
fn vertex_property_values_equal(left: &VertexPropertyValue, right: &VertexPropertyValue) -> bool {
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
                dialect: "OpenCypher",
                feature: "relationship id properties are not executable in Query engine"
                    .to_string(),
            });
        }
        let Some(metadata) = row.relationship_metadata.get(relationship) else {
            return Err(GraphError::UnsupportedQuery {
                dialect: "OpenCypher",
                feature: format!("metadata for relationship {binding} was not hydrated"),
            });
        };
        return Ok(metadata.properties.get(property).cloned());
    }
    let Some(metadata) = row.metadata.get(binding) else {
        return Err(GraphError::UnsupportedQuery {
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
            RowProjection::CountAll | RowProjection::Aggregate { .. } => {
                return Err(GraphError::UnsupportedQuery {
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
                        dialect: "OpenCypher",
                        feature: format!(
                            "relationship fast path cannot project property {binding}.{property}"
                        ),
                    });
                }
                if property == "id" {
                    return Err(GraphError::UnsupportedQuery {
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
            RowProjection::CountAll | RowProjection::Aggregate { .. } => {
                return Err(GraphError::UnsupportedQuery {
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
        push_projected_query_row(&mut projected, row, sort_keys, budget)?;
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
        RowProjection::CountAll | RowProjection::Aggregate { .. } => {
            Err(GraphError::UnsupportedQuery {
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
                        dialect: "OpenCypher",
                        feature: "count(*) ORDER BY requires an aggregate row".to_string(),
                    });
                };
                value.clone()
            }
            RowSortExpression::NodeId { .. } => {
                return Err(GraphError::UnsupportedQuery {
                    dialect: "OpenCypher",
                    feature: "aggregate ORDER BY cannot reference row variables".to_string(),
                });
            }
            RowSortExpression::Property { .. } => {
                return Err(GraphError::UnsupportedQuery {
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
            dialect: "OpenCypher",
            feature: format!("ORDER BY references unknown projection {name}"),
        });
    };
    row.values
        .get(index)
        .cloned()
        .ok_or_else(|| GraphError::UnsupportedQuery {
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
fn equivalent_property_index_keys(value: &VertexPropertyValue) -> Vec<String> {
    let mut keys = BTreeSet::new();
    keys.insert(encode_vertex_property_value_key(value));
    match value {
        VertexPropertyValue::Integer(value) => {
            if let Some(float) = VertexPropertyValue::exact_f64_from_u64(*value) {
                insert_float_property_index_key(&mut keys, float);
                if *value == 0 {
                    insert_float_property_index_key(&mut keys, -0.0);
                }
            }
        }
        VertexPropertyValue::SignedInteger(value) => {
            if let Some(float) = VertexPropertyValue::exact_f64_from_i64(*value) {
                insert_float_property_index_key(&mut keys, float);
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
            } else if let Some(integer) = VertexPropertyValue::exact_i64_from_f64(value.0) {
                keys.insert(encode_vertex_property_value_key(
                    &VertexPropertyValue::from_i64(integer),
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
