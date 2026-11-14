use futures::{stream, StreamExt as _, TryStreamExt as _};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tracing::Instrument as _;

use super::topology_tail::GraphTopologyOverlay;
use super::*;
use crate::engine::remote_io::RemoteIoWindow;
use crate::query::path_procedure::{
    NativePathDirection, NativePathNodeSelector, NativePathProcedure, NativePathProcedureKind, NativePathProjection, NativeProcedure,
};
use crate::QueryFailureReason;
use crate::{QueryCursorToken, QueryPath, QueryPathNode, QueryPathRelationship, QueryResultPage, QueryResultSet, QueryRow, QueryValue};

const MAX_NATIVE_PATH_PAGE_CURSORS: usize = 1_024;
const MAX_NATIVE_PATH_PAGE_CURSOR_BYTES: u64 = 64 * 1024 * 1024;
const NATIVE_PATH_PAGE_CURSOR_TTL: std::time::Duration = std::time::Duration::from_secs(60);
// Point reads in flight while a path call hydrates relationships and
// vertices. A seed's neighbours are scattered across the keyspace, so on a
// cold scope nearly every read is its own object-store round trip and the
// hydration takes about `reads / concurrency` of them: at 16, a 960-edge hub
// cost seconds of S3 latency per call.
const NATIVE_PATH_HYDRATION_CONCURRENCY: usize = 64;
const NATIVE_PATH_SELECTOR_HYDRATION_BATCH_SIZE: usize = 256;

pub(super) enum PathTopologySource {
    Compiled {
        matrix: Arc<crate::sparse_kernel::CompiledGraphBlasMatrix>,
        overlay: Option<GraphTopologyOverlay>,
    },
    Storage,
}

pub(super) struct TypedPathTopology {
    pub(super) edge_type: String,
    pub(super) source: PathTopologySource,
}

// Planning and execution share the same directional access decision. A cold
// compiled reverse index must never be built as a side effect of a path query:
// full-index shards can use the pinned incoming storage index; outbound-only
// shards need a bounded scan of their forward topology instead.
enum NativePathNeighborAccess<'a> {
    CompiledReverseScan {
        matrix: &'a crate::sparse_kernel::CompiledGraphBlasMatrix,
        overlay: Option<&'a GraphTopologyOverlay>,
        scan_work: u64,
    },
    StorageReverseScan,
    Compiled {
        matrix: &'a crate::sparse_kernel::CompiledGraphBlasMatrix,
        overlay: Option<&'a GraphTopologyOverlay>,
        scan_work: u64,
    },
    Storage,
}

impl TypedPathTopology {
    fn neighbor_access(&self, vertex: VertexId, incoming: bool, reverse_storage: bool) -> NativePathNeighborAccess<'_> {
        match &self.source {
            PathTopologySource::Compiled { matrix, overlay } => {
                let degree = if incoming {
                    crate::sparse_kernel::compiled_graphblas_cached_in_degree(matrix, vertex)
                } else {
                    Some(crate::sparse_kernel::compiled_graphblas_out_degree(matrix, vertex))
                };
                match degree {
                    Some(degree) => NativePathNeighborAccess::Compiled {
                        matrix,
                        overlay: overlay.as_ref(),
                        scan_work: degree.saturating_add(overlay.as_ref().map_or(0, |overlay| {
                            if incoming {
                                overlay.in_neighbor_scan_work()
                            } else {
                                overlay.out_neighbor_scan_work(vertex)
                            }
                        })),
                    },
                    None if reverse_storage => NativePathNeighborAccess::Storage,
                    None => NativePathNeighborAccess::CompiledReverseScan {
                        matrix,
                        overlay: overlay.as_ref(),
                        scan_work: matrix
                            .in_neighbor_scan_work()
                            .saturating_add(overlay.as_ref().map_or(0, GraphTopologyOverlay::in_neighbor_scan_work)),
                    },
                }
            }
            PathTopologySource::Storage if incoming && !reverse_storage => NativePathNeighborAccess::StorageReverseScan,
            PathTopologySource::Storage => NativePathNeighborAccess::Storage,
        }
    }
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct PathEdge {
    edge_type: String,
    src: VertexId,
    dst: VertexId,
    relationship_id: Option<RelationshipId>,
}

#[derive(Clone, Debug)]
struct CandidatePath {
    nodes: Vec<VertexId>,
    edges: Vec<PathEdge>,
    weight: f64,
    cost: f64,
}

#[derive(Clone, Copy)]
struct NativePathRead<'a> {
    snapshot: &'a GraphStorageSnapshot,
    cell_id: &'a str,
    read_epoch: StorageSequence,
    budget: &'a QueryBudget,
}

struct NativePathFrontier {
    vertices: BTreeSet<VertexId>,
    seen: BTreeSet<VertexId>,
    direction: NativePathDirection,
    depth: u8,
    scan_cost: Option<u64>,
}

impl NativePathFrontier {
    fn new(seeds: &[VertexId], direction: NativePathDirection) -> Self {
        let seen = seeds.iter().copied().collect::<BTreeSet<_>>();
        Self {
            vertices: seen.clone(),
            seen,
            direction,
            depth: 0,
            scan_cost: None,
        }
    }
}

impl GraphShard {
    pub(crate) async fn execute_native_path_rows_page(
        &self,
        context: QueryContext,
        query: &str,
        procedure: NativeProcedure,
        cursor: Option<QueryCursorToken>,
        page_size: usize,
    ) -> Result<QueryResultPage> {
        if page_size == 0 {
            return Err(GraphError::UnsupportedQuery {
                reason: QueryFailureReason::InvalidRequest,
                dialect: "OpenCypher",
                feature: "native path page size must be greater than zero".to_string(),
            });
        }
        if let Some(cursor) = cursor {
            return self.continue_native_path_page(&context, query, cursor, page_size).await;
        }

        let scope = context.scope.clone();
        let cell_id = context.cell_id.clone();
        let parameters = context.parameters.clone();
        let result = Box::pin(self.execute_native_procedure_rows(context, procedure)).await?;
        let QueryResultSet {
            columns,
            rows,
            read_epoch: _,
            storage_sequence: _,
            statuses: _,
        } = result;
        let mut rows = VecDeque::from(rows);
        let page_rows = take_native_path_page_rows(&mut rows, page_size);
        if rows.is_empty() {
            return Ok(QueryResultPage::new(columns, page_rows, None));
        }

        let resident_bytes = native_path_rows_resident_bytes(&rows);
        if resident_bytes > MAX_NATIVE_PATH_PAGE_CURSOR_BYTES {
            return Err(GraphError::AdmissionRejected {
                operation: "native_path_cursor_bytes",
                actual: resident_bytes,
                limit: MAX_NATIVE_PATH_PAGE_CURSOR_BYTES,
            });
        }
        let mut store = self.native_path_page_cursors.lock().await;
        purge_native_path_page_cursors(&mut store);
        if store.cursors.len() >= MAX_NATIVE_PATH_PAGE_CURSORS {
            return Err(GraphError::AdmissionRejected {
                operation: "native_path_cursors",
                actual: store.cursors.len().saturating_add(1) as u64,
                limit: MAX_NATIVE_PATH_PAGE_CURSORS as u64,
            });
        }
        let next_bytes = store.resident_bytes.saturating_add(resident_bytes);
        if next_bytes > MAX_NATIVE_PATH_PAGE_CURSOR_BYTES {
            return Err(GraphError::AdmissionRejected {
                operation: "native_path_cursor_bytes",
                actual: next_bytes,
                limit: MAX_NATIVE_PATH_PAGE_CURSOR_BYTES,
            });
        }
        let cursor_id = next_native_path_page_cursor_id(&mut store)?;
        store.cursors.insert(
            cursor_id,
            crate::core::state::NativePathPageCursor {
                scope,
                cell_id,
                query: query.to_string(),
                parameters,
                columns: columns.clone(),
                rows,
                expires_at: std::time::Instant::now() + NATIVE_PATH_PAGE_CURSOR_TTL,
                resident_bytes,
            },
        );
        store.resident_bytes = next_bytes;
        Ok(QueryResultPage::new(columns, page_rows, Some(QueryCursorToken::new(cursor_id))))
    }

    async fn continue_native_path_page(
        &self,
        context: &QueryContext,
        query: &str,
        cursor: QueryCursorToken,
        page_size: usize,
    ) -> Result<QueryResultPage> {
        let mut store = self.native_path_page_cursors.lock().await;
        purge_native_path_page_cursors(&mut store);
        let Some(stored) = store.cursors.get(&cursor.offset) else {
            return Err(GraphError::UnsupportedQuery {
                reason: QueryFailureReason::InvalidRequest,
                dialect: "OpenCypher",
                feature: "native path cursor is unknown or expired".to_string(),
            });
        };
        if stored.scope != context.scope
            || stored.cell_id != context.cell_id
            || stored.query != query
            || stored.parameters != context.parameters
        {
            return Err(GraphError::UnsupportedQuery {
                reason: QueryFailureReason::InvalidRequest,
                dialect: "OpenCypher",
                feature: "native path cursor does not belong to this query".to_string(),
            });
        }

        let mut stored = store
            .cursors
            .remove(&cursor.offset)
            .expect("native path cursor was checked while holding its lock");
        store.resident_bytes = store.resident_bytes.saturating_sub(stored.resident_bytes);
        let rows = take_native_path_page_rows(&mut stored.rows, page_size);
        let columns = stored.columns.clone();
        let next_cursor = if stored.rows.is_empty() {
            None
        } else {
            stored.resident_bytes = native_path_rows_resident_bytes(&stored.rows);
            stored.expires_at = std::time::Instant::now() + NATIVE_PATH_PAGE_CURSOR_TTL;
            store.resident_bytes = store.resident_bytes.saturating_add(stored.resident_bytes);
            store.cursors.insert(cursor.offset, stored);
            Some(cursor)
        };
        Ok(QueryResultPage::new(columns, rows, next_cursor))
    }

    pub(crate) async fn execute_native_procedure_rows(&self, context: QueryContext, procedure: NativeProcedure) -> Result<QueryResultSet> {
        self.operation_metrics.query_rows_started.fetch_add(1, Ordering::Relaxed);
        let started = std::time::Instant::now();
        let span = native_procedure_span(&context.cell_id, procedure.procedure_name());
        let remote_io = RemoteIoWindow::open();
        let result = Box::pin(self.execute_native_procedure_rows_snapshot(context, procedure))
            .instrument(span.clone())
            .await;
        remote_io.record(&span);
        self.operation_metrics.query_rows_latency.record(started.elapsed());
        match &result {
            Ok(result_set) => {
                self.operation_metrics.query_rows_completed.fetch_add(1, Ordering::Relaxed);
                self.operation_metrics
                    .query_rows_returned
                    .fetch_add(result_set.rows.len() as u64, Ordering::Relaxed);
                span.record("hydradb.query.rows_returned", result_set.rows.len() as u64);
                if let Some(read_epoch) = result_set.read_epoch {
                    span.record("hydradb.read_epoch", read_epoch);
                }
            }
            Err(error) => {
                self.operation_metrics.record_query_rows_failure(error);
                span.record("error.class", error.class());
                if let Some(operation) = error.limit_operation() {
                    span.record("error.operation", operation);
                }
                span.record("hydradb.sampling.tail_keep", "error");
            }
        }
        result
    }

    async fn execute_native_procedure_rows_snapshot(&self, context: QueryContext, procedure: NativeProcedure) -> Result<QueryResultSet> {
        if context.read_epoch.is_some() && context.validated_read_epoch().is_none() {
            return Err(GraphError::UnsupportedQuery {
                reason: QueryFailureReason::InvalidRequest,
                dialect: "OpenCypher",
                feature: "historical graph epochs are not storage snapshots; execute against a current SlateDB snapshot".to_string(),
            });
        }
        if context.read_epoch.is_none() {
            let snapshot = if context.uses_refreshed_reader() {
                self.storage.strong_snapshot().await?
            } else {
                self.storage.snapshot().await?
            };
            let read_epoch = snapshot.seq();
            let context = context.with_validated_storage_read_epoch(read_epoch, read_epoch);
            return SlateGraphStore::scope_snapshot(snapshot, self.execute_native_procedure_rows_inner(context, procedure)).await;
        }
        self.execute_native_procedure_rows_inner(context, procedure).await
    }

    async fn execute_native_procedure_rows_inner(&self, context: QueryContext, procedure: NativeProcedure) -> Result<QueryResultSet> {
        match procedure {
            NativeProcedure::Path(procedure) => Box::pin(self.execute_native_path_rows_inner(context, *procedure)).await,
            NativeProcedure::Degree(procedure) => Box::pin(self.execute_native_degree_rows_inner(context, procedure)).await,
            NativeProcedure::Gds(procedure) => Box::pin(self.execute_gds_rows_inner(context, procedure)).await,
        }
    }

    async fn execute_native_path_rows_inner(&self, context: QueryContext, procedure: NativePathProcedure) -> Result<QueryResultSet> {
        validate_component("cell_id", &context.cell_id)?;
        let budget = QueryBudget::new(
            context.max_runtime_ms.or(self.limits.max_query_runtime_ms),
            context.cancellation_token.clone(),
        )
        .with_max_result_bytes(context.max_result_bytes);
        budget.check("native_path_start")?;
        if procedure.path_count > 0 && procedure.path_count > self.limits.max_query_result_vertices as u64 {
            return Err(GraphError::AdmissionRejected {
                operation: "native_path_count",
                actual: procedure.path_count,
                limit: self.limits.max_query_result_vertices as u64,
            });
        }
        let result_limit = procedure.result_limit.unwrap_or(self.limits.max_query_result_vertices as u64);
        if result_limit > self.limits.max_query_result_vertices as u64 {
            return Err(GraphError::AdmissionRejected {
                operation: "native_path_result_limit",
                actual: result_limit,
                limit: self.limits.max_query_result_vertices as u64,
            });
        }
        let read_epoch = context
            .validated_read_epoch()
            .unwrap_or(self.current_epoch(&context.cell_id).await?);
        let snapshot = self.storage.snapshot().await?;
        if snapshot.seq() != read_epoch {
            return Err(GraphError::SnapshotAhead {
                cell_id: context.cell_id,
                read_epoch,
                current_epoch: snapshot.seq(),
            });
        }
        let result_cache_key = NativePathResultCacheKey {
            scope: context.scope.clone(),
            cell_id: context.cell_id.clone(),
            procedure: format!("{procedure:?}"),
            read_epoch,
            max_result_bytes: context.max_result_bytes,
        };
        if let Some(cached) = self.native_path_result_cache.lock().await.get(&result_cache_key) {
            tracing::Span::current().record("hydradb.query.result_cache", "hit");
            let mut result = (*cached).clone();
            result.storage_sequence = context.validated_storage_sequence();
            return Ok(result);
        }
        tracing::Span::current().record("hydradb.query.result_cache", "miss");

        let topologies = self
            .native_path_topologies(&context.cell_id, &procedure.rel_types, read_epoch, &budget)
            .await?;
        let read = NativePathRead {
            snapshot: snapshot.as_ref(),
            cell_id: &context.cell_id,
            read_epoch,
            budget: &budget,
        };
        let mut vertex_cache = BTreeMap::new();
        let sources = self
            .resolve_native_path_selector(read, procedure.source_selector.as_ref(), procedure.source, &mut vertex_cache)
            .await?;
        let targets = match procedure.target_selector.as_ref() {
            Some(selector) if Some(selector) == procedure.source_selector.as_ref() => Some(sources.clone()),
            Some(selector) => Some(
                self.resolve_native_path_selector(read, Some(selector), 0, &mut vertex_cache)
                    .await?,
            ),
            None => procedure.target.map(|target| vec![target]),
        };
        let mut relationship_cache = BTreeMap::new();
        let mut relationship_choices_cache = BTreeMap::new();
        let mut rows = Vec::new();
        let mut relationship_variant_structures = Vec::new();
        let result_limit_usize = usize::try_from(result_limit).unwrap_or(usize::MAX);
        let adjacency = self
            .native_path_adjacency(read, &procedure, &topologies, &sources, targets.as_deref())
            .await?;
        'sources: for source in sources.iter().copied() {
            let mut scalar = procedure.clone();
            scalar.source = source;
            scalar.source_selector = None;
            scalar.target_selector = None;
            scalar.result_limit = None;
            let candidate_groups = if let Some(targets) = targets
                .as_ref()
                .filter(|_| scalar.weight_property.is_none() && scalar.cost_property.is_none() && scalar.max_cost.is_none())
            {
                let eligible_targets = targets
                    .iter()
                    .copied()
                    .filter(|target| *target != source && (!procedure.pairwise || source < *target))
                    .collect::<Vec<_>>();
                let mut grouped = enumerate_unweighted_candidate_paths_to_targets(
                    &scalar,
                    &adjacency,
                    &eligible_targets,
                    self.limits.max_query_intermediate_rows,
                    &budget,
                )?;
                eligible_targets
                    .into_iter()
                    .map(|target| (Some(target), grouped.remove(&target).unwrap_or_default()))
                    .collect::<Vec<_>>()
            } else {
                targets
                    .as_ref()
                    .map(|targets| targets.iter().copied().map(Some).collect::<Vec<_>>())
                    .unwrap_or_else(|| vec![None])
                    .into_iter()
                    .filter(|target| *target != Some(source) && (!procedure.pairwise || target.is_none_or(|target| source < target)))
                    .map(|target| {
                        scalar.target = target;
                        scalar.kind = if target.is_some() {
                            NativePathProcedureKind::SinglePair
                        } else {
                            NativePathProcedureKind::SingleSource
                        };
                        enumerate_candidate_paths(&scalar, &adjacency, self.limits.max_query_intermediate_rows, &budget)
                            .map(|candidates| (target, candidates))
                    })
                    .collect::<Result<Vec<_>>>()?
            };
            for (target, candidates) in candidate_groups {
                scalar.target = target;
                scalar.kind = if target.is_some() {
                    NativePathProcedureKind::SinglePair
                } else {
                    NativePathProcedureKind::SingleSource
                };
                if scalar.fair_relationship_variants {
                    for structural_path in candidates {
                        relationship_variant_structures.push(structural_path);
                        if relationship_variant_structures.len() >= result_limit_usize {
                            break 'sources;
                        }
                    }
                    continue;
                }
                let prefetch = native_path_vertex_prefetch(&scalar, &candidates, result_limit_usize.saturating_sub(rows.len()));
                let (mut candidates, ()) = futures::try_join!(
                    self.expand_native_path_relationships(
                        read,
                        candidates,
                        &mut relationship_choices_cache,
                        &mut relationship_cache,
                        self.limits.max_query_intermediate_rows,
                        false,
                    ),
                    self.hydrate_vertex_metadata_cache_at_concurrently(
                        read.cell_id,
                        &prefetch,
                        read.read_epoch,
                        &mut vertex_cache,
                        read.budget,
                        NATIVE_PATH_HYDRATION_CONCURRENCY,
                    ),
                )?;
                self.score_native_paths(&scalar, &mut candidates, &relationship_cache, &budget)?;
                select_native_paths(&scalar, &mut candidates);
                candidates.truncate(result_limit_usize.saturating_sub(rows.len()));
                let vertex_ids = candidates
                    .iter()
                    .flat_map(|candidate| candidate.nodes.iter().copied())
                    .collect::<Vec<_>>();
                self.hydrate_vertex_metadata_cache_at_concurrently(
                    read.cell_id,
                    &vertex_ids,
                    read.read_epoch,
                    &mut vertex_cache,
                    read.budget,
                    NATIVE_PATH_HYDRATION_CONCURRENCY,
                )
                .await?;
                for candidate in candidates {
                    budget.check("native_path_hydrate")?;
                    let path = self
                        .hydrate_native_path(
                            read,
                            candidate.nodes.as_slice(),
                            candidate.edges.as_slice(),
                            &mut vertex_cache,
                            &relationship_cache,
                        )
                        .await?;
                    let values = procedure
                        .projections
                        .iter()
                        .map(|projection| match projection {
                            NativePathProjection::Path => QueryValue::Path(Box::new(path.clone())),
                            NativePathProjection::Weight => numeric_query_value(candidate.weight),
                            NativePathProjection::Cost => numeric_query_value(candidate.cost),
                        })
                        .collect();
                    let row = QueryRow::new(values);
                    budget.account_result_row(&row)?;
                    rows.push(row);
                    if rows.len() as u64 >= result_limit {
                        break 'sources;
                    }
                }
            }
        }
        if procedure.fair_relationship_variants {
            let structure_count = relationship_variant_structures.len();
            let base_quota = result_limit_usize.checked_div(structure_count).unwrap_or(0);
            let remainder = result_limit_usize.checked_rem(structure_count).unwrap_or(0);
            self.hydrate_native_path_relationship_choices(
                read,
                &relationship_variant_structures,
                &mut relationship_choices_cache,
                &mut relationship_cache,
            )
            .await?;
            let mut relationship_variant_groups = Vec::with_capacity(structure_count);
            for (index, structural_path) in relationship_variant_structures.into_iter().enumerate() {
                let quota = base_quota.saturating_add(usize::from(index < remainder));
                let mut variants = self
                    .expand_native_path_relationships(
                        read,
                        vec![structural_path],
                        &mut relationship_choices_cache,
                        &mut relationship_cache,
                        quota,
                        true,
                    )
                    .await?;
                self.score_native_paths(&procedure, &mut variants, &relationship_cache, &budget)?;
                select_native_paths(&procedure, &mut variants);
                if !variants.is_empty() {
                    relationship_variant_groups.push(variants);
                }
            }
            let candidates = fair_relationship_variant_candidates(relationship_variant_groups, result_limit_usize);
            let vertex_ids = candidates
                .iter()
                .flat_map(|candidate| candidate.nodes.iter().copied())
                .collect::<Vec<_>>();
            self.hydrate_vertex_metadata_cache_at_concurrently(
                read.cell_id,
                &vertex_ids,
                read.read_epoch,
                &mut vertex_cache,
                read.budget,
                NATIVE_PATH_HYDRATION_CONCURRENCY,
            )
            .await?;
            for candidate in candidates {
                budget.check("native_path_hydrate")?;
                let path = self
                    .hydrate_native_path(
                        read,
                        candidate.nodes.as_slice(),
                        candidate.edges.as_slice(),
                        &mut vertex_cache,
                        &relationship_cache,
                    )
                    .await?;
                let values = procedure
                    .projections
                    .iter()
                    .map(|projection| match projection {
                        NativePathProjection::Path => QueryValue::Path(Box::new(path.clone())),
                        NativePathProjection::Weight => numeric_query_value(candidate.weight),
                        NativePathProjection::Cost => numeric_query_value(candidate.cost),
                    })
                    .collect();
                let row = QueryRow::new(values);
                budget.account_result_row(&row)?;
                rows.push(row);
            }
        }
        let result = QueryResultSet::new(procedure.projections.iter().map(|projection| projection.column()).collect(), rows)
            .with_read_epoch(read_epoch);
        let result = match context.validated_storage_sequence() {
            Some(sequence) => result.with_storage_sequence(sequence),
            None => result,
        };
        let resident_bytes = result_cache_key
            .estimated_resident_bytes()
            .saturating_add(native_path_result_resident_bytes(&result));
        self.native_path_result_cache.lock().await.insert_sized(
            result_cache_key,
            Arc::new(result.clone()),
            context.cell_id,
            false,
            resident_bytes,
            &self.cache_metrics,
        );
        Ok(result)
    }

    async fn resolve_native_path_selector(
        &self,
        read: NativePathRead<'_>,
        selector: Option<&NativePathNodeSelector>,
        fallback: VertexId,
        vertex_cache: &mut BTreeMap<VertexId, VertexMetadata>,
    ) -> Result<Vec<VertexId>> {
        let Some(selector) = selector else {
            return Ok(vec![fallback]);
        };
        let mut vertices = BTreeSet::new();
        self.refuse_pending_write_back(read.cell_id, &selector.property, read.budget)
            .await?;
        // The catalog is read at the selector's own snapshot, so the index
        // generation and its entries come from the same point in time.
        let catalog_key = keys::vertex_write_back_generation(read.cell_id, &selector.property);
        let catalog = read
            .budget
            .read_only_io("native_path_selector_generation", async {
                Ok::<_, GraphError>(
                    read.snapshot
                        .get_with_options(catalog_key.as_bytes(), &remote_read_options())
                        .await?,
                )
            })
            .await?;
        let property_prefix = match catalog {
            Some(value) => {
                keys::vertex_write_back_index_generation_prefix(read.cell_id, &selector.property, decode_u64(&catalog_key, &value)?)
            }
            None => keys::vertex_property_index_property_prefix(read.cell_id, &selector.property),
        };
        let property_prefix = property_prefix.as_str();
        let raw_candidate_count = Arc::new(AtomicUsize::new(0));
        let indexed_candidates = stream::iter(selector.values.iter().cloned().map(|value| {
            let raw_candidate_count = Arc::clone(&raw_candidate_count);
            async move {
                read.budget.check("native_path_selector")?;
                let property_value = VertexPropertyValue::String(value);
                let encoded = encode_vertex_property_value_key(&property_value);
                let prefix = keys::vertex_property_index_value_prefix(property_prefix, &encoded);
                // Selector cardinality is unknown until the prefix is consumed. Keep
                // broad values out of SlateDB's shared block cache; validated results
                // are bounded below and the snapshot still caches SST metadata.
                let mut candidates = read
                    .budget
                    .read_only_io("native_path_selector", async {
                        Ok::<_, GraphError>(
                            read.snapshot
                                .scan_prefix_with_options(prefix.as_bytes(), .., &remote_scan_options())
                                .await?,
                        )
                    })
                    .await?;
                let mut indexed_candidates = Vec::new();
                while let Some(kv) = read
                    .budget
                    .read_only_io("native_path_selector", async { Ok::<_, GraphError>(candidates.next().await?) })
                    .await?
                {
                    let count = raw_candidate_count.fetch_add(1, Ordering::Relaxed) + 1;
                    self.ensure_query_index_candidates("native_path_selector_candidates", count)?;
                    let key = String::from_utf8_lossy(&kv.key).into_owned();
                    let vertex = decode_u64(&key, &kv.value)?;
                    indexed_candidates.push((vertex, property_value.clone()));
                }
                Ok::<_, GraphError>(indexed_candidates)
            }
        }))
        .buffered(NATIVE_PATH_HYDRATION_CONCURRENCY)
        .boxed()
        .try_collect::<Vec<_>>()
        .await?
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
        for batch in indexed_candidates.chunks(NATIVE_PATH_SELECTOR_HYDRATION_BATCH_SIZE) {
            let vertex_ids = batch.iter().map(|(vertex, _)| *vertex).collect::<Vec<_>>();
            self.hydrate_vertex_metadata_cache_at_concurrently(
                read.cell_id,
                &vertex_ids,
                read.read_epoch,
                vertex_cache,
                read.budget,
                NATIVE_PATH_HYDRATION_CONCURRENCY,
            )
            .await?;
            for (vertex, property_value) in batch {
                let metadata = vertex_cache.get(vertex).expect("selector metadata was hydrated");
                if metadata.labels.contains(&selector.label) && metadata.properties.get(&selector.property) == Some(property_value) {
                    vertices.insert(*vertex);
                }
            }
        }
        Ok(vertices.into_iter().collect())
    }

    async fn hydrate_native_path_relationship_choices(
        &self,
        read: NativePathRead<'_>,
        candidates: &[CandidatePath],
        choices_cache: &mut BTreeMap<PathEdge, Vec<PathEdge>>,
        relationship_cache: &mut BTreeMap<PathEdge, EdgeMetadata>,
    ) -> Result<()> {
        let missing_edges = candidates
            .iter()
            .flat_map(|candidate| candidate.edges.iter().cloned())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .filter(|edge| !choices_cache.contains_key(edge))
            .collect::<Vec<_>>();
        let hydrated = stream::iter(missing_edges.into_iter().map(|structural_edge| async move {
            read.budget.check("native_path_relationship_hydration")?;
            let relationships = self
                .native_path_relationships_at(
                    read.cell_id,
                    &structural_edge.edge_type,
                    structural_edge.src,
                    structural_edge.dst,
                    read.read_epoch,
                    read.budget,
                )
                .await?;
            Ok::<_, GraphError>((structural_edge, relationships))
        }))
        .buffered(NATIVE_PATH_HYDRATION_CONCURRENCY)
        .boxed()
        .try_collect::<Vec<_>>()
        .await?;
        for (structural_edge, relationships) in hydrated {
            let mut choices = Vec::with_capacity(relationships.len());
            for (relationship_id, metadata) in relationships {
                let edge = PathEdge {
                    relationship_id,
                    ..structural_edge.clone()
                };
                relationship_cache.insert(edge.clone(), metadata);
                choices.push(edge);
            }
            choices_cache.insert(structural_edge, choices);
        }
        Ok(())
    }

    pub(super) async fn native_path_topologies(
        &self,
        cell_id: &str,
        rel_types: &[String],
        read_epoch: StorageSequence,
        budget: &QueryBudget,
    ) -> Result<Vec<TypedPathTopology>> {
        let mut topologies = Vec::with_capacity(rel_types.len());
        for edge_type in rel_types {
            budget.check("native_path_artifact_lookup")?;
            let source = if crate::sparse_kernel::default_matrix_kernel(&self.cache_policy) == SparseKernelBackend::Adjacency {
                PathTopologySource::Storage
            } else if let Some(artifact) = budget
                .read_only_io(
                    "native_path_artifact_lookup",
                    self.traced_latest_matrix_artifact(cell_id, edge_type, read_epoch),
                )
                .await?
            {
                match self
                    .compiled_graphblas_query_snapshot(cell_id, edge_type, artifact.base_epoch, read_epoch, budget)
                    .await?
                {
                    Some((matrix, overlay, rebuilt)) => {
                        self.record_graphblas_snapshot(rebuilt);
                        PathTopologySource::Compiled { matrix, overlay }
                    }
                    None => PathTopologySource::Storage,
                }
            } else {
                PathTopologySource::Storage
            };
            topologies.push(TypedPathTopology {
                edge_type: edge_type.clone(),
                source,
            });
        }
        Ok(topologies)
    }

    async fn native_path_adjacency(
        &self,
        read: NativePathRead<'_>,
        procedure: &NativePathProcedure,
        topologies: &[TypedPathTopology],
        sources: &[VertexId],
        targets: Option<&[VertexId]>,
    ) -> Result<BTreeMap<VertexId, Vec<PathEdge>>> {
        if sources.is_empty() || targets.is_some_and(|targets| targets.is_empty()) {
            return Ok(BTreeMap::new());
        }
        // Every path of length <= L is covered by a source prefix and a
        // reverse target suffix whose depths sum to L. Choose the cheaper next
        // layer rather than forcing a fixed split: an expensive dead source
        // branch must not prevent a small reverse search from answering.
        let reverse_direction = match procedure.direction {
            NativePathDirection::Outgoing => NativePathDirection::Incoming,
            NativePathDirection::Incoming => NativePathDirection::Outgoing,
            NativePathDirection::Both => NativePathDirection::Both,
        };
        let mut source = NativePathFrontier::new(sources, procedure.direction);
        let mut target = NativePathFrontier::new(targets.unwrap_or_default(), reverse_direction);
        let mut adjacency: BTreeMap<VertexId, BTreeSet<PathEdge>> = BTreeMap::new();
        let mut edge_visits = 0_u64;
        let mut vertices = source.seen.union(&target.seen).copied().collect::<BTreeSet<_>>();
        if vertices.len() > self.limits.max_query_intermediate_rows {
            return Err(GraphError::AdmissionRejected {
                operation: "native_path_vertices",
                actual: vertices.len() as u64,
                limit: self.limits.max_query_intermediate_rows as u64,
            });
        }
        for layer in 0..procedure.max_len {
            // An exhausted side has already provided every edge reachable from
            // that side, so every possible source-target path is represented.
            if source.vertices.is_empty() || (targets.is_some() && target.vertices.is_empty()) {
                break;
            }
            let reverse = if targets.is_some() {
                let source_cost = self.native_path_frontier_scan_cost(read, &mut source, topologies).await?;
                let target_cost = self.native_path_frontier_scan_cost(read, &mut target, topologies).await?;
                target_cost < source_cost || (target_cost == source_cost && target.depth <= source.depth)
            } else {
                false
            };
            let (active, other) = if reverse { (&mut target, &source) } else { (&mut source, &target) };
            // Only the last layer must meet the other ball. Filtering earlier
            // would lose paths through vertices not reached by the other side.
            let allowed = (targets.is_some() && layer + 1 == procedure.max_len).then_some(&other.seen);
            let mut next = BTreeSet::new();
            for vertex in std::mem::take(&mut active.vertices) {
                read.budget.check("native_path_frontier")?;
                let edges = self
                    .native_path_successors(read, vertex, active.direction, topologies, allowed, &mut edge_visits)
                    .await?;
                for edge in edges {
                    let neighbor = far_endpoint(vertex, &edge).expect("successor touches frontier");
                    if allowed.is_some_and(|vertices| !vertices.contains(&neighbor)) {
                        continue;
                    }
                    // Final-hop endpoints are retained only as adjacency edges;
                    // they never enter another frontier. Edge scans and candidate
                    // enumeration have their own limits, so do not charge these
                    // endpoints against the frontier-vertex allowance.
                    if layer + 1 < procedure.max_len {
                        vertices.insert(neighbor);
                        if vertices.len() > self.limits.max_query_intermediate_rows {
                            return Err(GraphError::AdmissionRejected {
                                operation: "native_path_vertices",
                                actual: vertices.len() as u64,
                                limit: self.limits.max_query_intermediate_rows as u64,
                            });
                        }
                        if active.seen.insert(neighbor) {
                            next.insert(neighbor);
                        }
                    }
                    // Target expansion walks backwards; enumeration still
                    // needs adjacency in the query's forward direction.
                    let from = if reverse { neighbor } else { vertex };
                    adjacency.entry(from).or_default().insert(edge);
                }
            }
            active.vertices = next;
            active.depth += 1;
            active.scan_cost = None;
        }
        Ok(adjacency
            .into_iter()
            .map(|(vertex, edges)| (vertex, edges.into_iter().collect()))
            .collect())
    }

    async fn native_path_frontier_scan_cost(
        &self,
        read: NativePathRead<'_>,
        frontier: &mut NativePathFrontier,
        topologies: &[TypedPathTopology],
    ) -> Result<u64> {
        if let Some(cost) = frontier.scan_cost {
            return Ok(cost);
        }
        let mut cost = 0_u64;
        for vertex in &frontier.vertices {
            for topology in topologies {
                read.budget.check("native_path_frontier_cost")?;
                for incoming in [false, true] {
                    if (incoming && frontier.direction == NativePathDirection::Outgoing)
                        || (!incoming && frontier.direction == NativePathDirection::Incoming)
                    {
                        continue;
                    }
                    let degree = match topology.neighbor_access(*vertex, incoming, self.writes_reverse_index()) {
                        NativePathNeighborAccess::Compiled { scan_work, .. }
                        | NativePathNeighborAccess::CompiledReverseScan { scan_work, .. } => scan_work,
                        // There is no cheap reverse estimate without an artifact
                        // or reverse index. Prefer a known-cost forward frontier.
                        NativePathNeighborAccess::StorageReverseScan => u64::MAX,
                        NativePathNeighborAccess::Storage if incoming => {
                            read.budget
                                .read_only_io(
                                    "native_path_frontier_cost",
                                    self.in_degree_in_storage_snapshot(read.snapshot, read.cell_id, &topology.edge_type, *vertex),
                                )
                                .await?
                        }
                        NativePathNeighborAccess::Storage => {
                            read.budget
                                .read_only_io(
                                    "native_path_frontier_cost",
                                    self.out_degree_in_storage_snapshot(read.snapshot, read.cell_id, &topology.edge_type, *vertex),
                                )
                                .await?
                        }
                    };
                    cost = cost.saturating_add(degree);
                }
            }
        }
        // Degree counters only choose a side; actual expansion is still charged
        // against one shared budget. A missing/stale estimate never proves that
        // a vertex has no neighbors, nor admits work past the limit.
        frontier.scan_cost = Some(cost);
        Ok(cost)
    }

    async fn native_path_successors(
        &self,
        read: NativePathRead<'_>,
        vertex: VertexId,
        direction: NativePathDirection,
        topologies: &[TypedPathTopology],
        allowed: Option<&BTreeSet<VertexId>>,
        edge_visits: &mut u64,
    ) -> Result<Vec<PathEdge>> {
        let mut edges = BTreeSet::new();
        for topology in topologies {
            for incoming in [false, true] {
                if (incoming && direction == NativePathDirection::Outgoing) || (!incoming && direction == NativePathDirection::Incoming) {
                    continue;
                }
                read.budget.check("native_path_neighbors")?;
                let neighbors = match topology.neighbor_access(vertex, incoming, self.writes_reverse_index()) {
                    NativePathNeighborAccess::Compiled {
                        matrix,
                        overlay,
                        scan_work,
                    } => {
                        self.charge_native_path_edges(edge_visits, scan_work)?;
                        let mut neighbors = match (incoming, allowed) {
                            (false, Some(allowed)) => {
                                crate::sparse_kernel::compiled_graphblas_out_neighbors_intersecting(matrix, vertex, allowed)
                            }
                            (false, None) => crate::sparse_kernel::compiled_graphblas_out_neighbors(matrix, vertex),
                            (true, Some(allowed)) => {
                                crate::sparse_kernel::compiled_graphblas_in_neighbors_intersecting(matrix, vertex, allowed)
                            }
                            (true, None) => crate::sparse_kernel::compiled_graphblas_in_neighbors(matrix, vertex),
                        }
                        .into_iter()
                        .collect::<BTreeSet<_>>();
                        if let Some(overlay) = overlay {
                            if incoming {
                                overlay.apply_in_neighbors(vertex, &mut neighbors);
                            } else {
                                overlay.apply_out_neighbors(vertex, &mut neighbors);
                            }
                        }
                        if let Some(allowed) = allowed {
                            neighbors.retain(|neighbor| allowed.contains(neighbor));
                        }
                        neighbors.into_iter().collect::<Vec<_>>()
                    }
                    NativePathNeighborAccess::CompiledReverseScan {
                        matrix,
                        overlay,
                        scan_work,
                    } => {
                        // Check the full work before scanning; no transpose or
                        // graph-sized copy is allocated. Cancellation is checked
                        // for every visited vertex and edge.
                        self.charge_native_path_edges(edge_visits, scan_work)?;
                        let mut neighbors = matrix
                            .in_neighbors_without_reverse(vertex, || read.budget.check("native_path_reverse_scan"))?
                            .into_iter()
                            .collect::<BTreeSet<_>>();
                        if let Some(overlay) = overlay {
                            overlay.apply_in_neighbors(vertex, &mut neighbors);
                        }
                        if let Some(allowed) = allowed {
                            neighbors.retain(|v| allowed.contains(v));
                        }
                        neighbors.into_iter().collect()
                    }
                    NativePathNeighborAccess::StorageReverseScan => {
                        self.native_path_reverse_from_storage(read, &topology.edge_type, vertex, edge_visits)
                            .await?
                    }
                    NativePathNeighborAccess::Storage => {
                        // This snapshot includes WAL additions/deletions. Applying
                        // the compiled artifact's overlay here would count it twice.
                        let neighbors = if incoming {
                            read.budget
                                .read_only_io(
                                    "native_path_storage_neighbors",
                                    self.in_neighbors_in_storage_snapshot(read.snapshot, read.cell_id, &topology.edge_type, vertex),
                                )
                                .await?
                        } else {
                            read.budget
                                .read_only_io(
                                    "native_path_storage_neighbors",
                                    self.out_neighbors_in_storage_snapshot(
                                        read.snapshot,
                                        read.cell_id,
                                        &topology.edge_type,
                                        vertex,
                                        read.read_epoch,
                                    ),
                                )
                                .await?
                        };
                        self.charge_native_path_edges(edge_visits, neighbors.len() as u64)?;
                        neighbors
                    }
                };
                edges.extend(neighbors.into_iter().map(|neighbor| PathEdge {
                    edge_type: topology.edge_type.clone(),
                    src: if incoming { neighbor } else { vertex },
                    dst: if incoming { vertex } else { neighbor },
                    relationship_id: None,
                }));
            }
        }
        Ok(edges.into_iter().collect())
    }

    fn charge_native_path_edges(&self, visits: &mut u64, work: u64) -> Result<()> {
        *visits = visits.saturating_add(work);
        ensure_limit("native_path_edges", *visits, self.limits.max_query_scan_edges)
    }

    // OutboundOnly still has a source/degree index. Enumerate it from the
    // pinned snapshot and use the ordinary outgoing reader, which understands
    // segment-backed adjacency and tombstones as well as individual edge rows.
    async fn native_path_reverse_from_storage(
        &self,
        read: NativePathRead<'_>,
        edge_type: &str,
        dst: VertexId,
        visits: &mut u64,
    ) -> Result<Vec<VertexId>> {
        validate_component("cell_id", read.cell_id)?;
        validate_component("edge_type", edge_type)?;
        let prefix = keys::degree_out_prefix(read.cell_id, edge_type);
        let mut iter = read
            .budget
            .read_only_io("native_path_reverse_storage", async {
                Ok::<_, GraphError>(
                    read.snapshot
                        .scan_prefix_with_options(prefix.as_bytes(), .., &remote_scan_options())
                        .await?,
                )
            })
            .await?;
        let mut neighbors = Vec::new();
        loop {
            let Some(kv) = read
                .budget
                .read_only_io("native_path_reverse_storage", async { Ok::<_, GraphError>(iter.next().await?) })
                .await?
            else {
                break;
            };
            self.charge_native_path_edges(visits, 1)?;
            let key = String::from_utf8_lossy(&kv.key);
            let src = parse_u64(&key, key.strip_prefix(&prefix).unwrap_or_default(), "source vertex")?;
            let outgoing = read
                .budget
                .read_only_io(
                    "native_path_reverse_storage",
                    self.out_neighbors_in_storage_snapshot_inner(
                        read.snapshot,
                        read.cell_id,
                        edge_type,
                        src,
                        read.read_epoch,
                        Some(super::query::StorageNeighborScanBudget {
                            query: read.budget,
                            scanned_records: visits,
                            operation: "native_path_edges",
                        }),
                    ),
                )
                .await?;
            if outgoing.binary_search(&dst).is_ok() {
                neighbors.push(src);
            }
        }
        Ok(neighbors)
    }

    async fn expand_native_path_relationships(
        &self,
        read: NativePathRead<'_>,
        candidates: Vec<CandidatePath>,
        choices_cache: &mut BTreeMap<PathEdge, Vec<PathEdge>>,
        relationship_cache: &mut BTreeMap<PathEdge, EdgeMetadata>,
        max_candidates: usize,
        truncate_at_limit: bool,
    ) -> Result<Vec<CandidatePath>> {
        self.hydrate_native_path_relationship_choices(read, &candidates, choices_cache, relationship_cache)
            .await?;

        let mut expanded = Vec::new();
        for candidate in candidates {
            let mut edge_variants = vec![Vec::with_capacity(candidate.edges.len())];
            for structural_edge in &candidate.edges {
                let choices = choices_cache.get(structural_edge).expect("relationship choices were inserted");
                let product_size = edge_variants.len().saturating_mul(choices.len());
                if !truncate_at_limit && expanded.len().saturating_add(product_size) > max_candidates {
                    return Err(GraphError::AdmissionRejected {
                        operation: "native_path_parallel_candidates",
                        actual: expanded.len().saturating_add(product_size) as u64,
                        limit: max_candidates as u64,
                    });
                }
                let mut next_variants = Vec::with_capacity(product_size.min(max_candidates));
                'variants: for variant in edge_variants {
                    for choice in choices {
                        if next_variants.len() >= max_candidates {
                            break 'variants;
                        }
                        let mut next = variant.clone();
                        next.push(choice.clone());
                        next_variants.push(next);
                    }
                }
                edge_variants = next_variants;
            }
            for edges in edge_variants {
                if expanded.len() >= max_candidates {
                    if truncate_at_limit {
                        break;
                    }
                    return Err(GraphError::AdmissionRejected {
                        operation: "native_path_parallel_candidates",
                        actual: expanded.len().saturating_add(1) as u64,
                        limit: max_candidates as u64,
                    });
                }
                expanded.push(CandidatePath {
                    nodes: candidate.nodes.clone(),
                    edges,
                    weight: 0.0,
                    cost: 0.0,
                });
            }
        }
        Ok(expanded)
    }

    fn score_native_paths(
        &self,
        procedure: &NativePathProcedure,
        candidates: &mut [CandidatePath],
        relationship_cache: &BTreeMap<PathEdge, EdgeMetadata>,
        budget: &QueryBudget,
    ) -> Result<()> {
        for candidate in candidates {
            budget.check("native_path_score")?;
            let mut weight = 0.0;
            let mut cost = 0.0;
            for edge in &candidate.edges {
                let relationship = if procedure.weight_property.is_some() || procedure.cost_property.is_some() {
                    Some(native_path_relationship_cached(edge, relationship_cache)?)
                } else {
                    None
                };
                weight += relationship.map_or(1.0, |metadata| {
                    procedure
                        .weight_property
                        .as_deref()
                        .map_or(1.0, |property| numeric_property(metadata, property, 1.0))
                });
                cost += relationship.map_or(0.0, |metadata| {
                    procedure
                        .cost_property
                        .as_deref()
                        .map_or(0.0, |property| numeric_property(metadata, property, 0.0))
                });
            }
            candidate.weight = weight;
            candidate.cost = cost;
        }
        Ok(())
    }

    async fn hydrate_native_path(
        &self,
        read: NativePathRead<'_>,
        nodes: &[VertexId],
        edges: &[PathEdge],
        vertex_cache: &mut BTreeMap<VertexId, VertexMetadata>,
        relationship_cache: &BTreeMap<PathEdge, EdgeMetadata>,
    ) -> Result<QueryPath> {
        self.hydrate_vertex_metadata_cache_at_concurrently(
            read.cell_id,
            nodes,
            read.read_epoch,
            vertex_cache,
            read.budget,
            NATIVE_PATH_HYDRATION_CONCURRENCY,
        )
        .await?;
        let mut path_nodes = Vec::with_capacity(nodes.len());
        for vertex in nodes {
            let metadata = vertex_cache.get(vertex).expect("vertex metadata was hydrated");
            path_nodes.push(QueryPathNode {
                id: *vertex,
                labels: metadata.labels.iter().cloned().collect(),
                properties: metadata.properties.clone(),
            });
        }
        let mut path_relationships = Vec::with_capacity(edges.len());
        for edge in edges {
            let metadata = native_path_relationship_cached(edge, relationship_cache)?;
            path_relationships.push(QueryPathRelationship {
                id: edge.relationship_id,
                edge_type: edge.edge_type.clone(),
                src: edge.src,
                dst: edge.dst,
                properties: metadata.properties.clone(),
            });
        }
        Ok(QueryPath {
            nodes: path_nodes,
            relationships: path_relationships,
        })
    }
}

/// The span a native procedure call runs under. It declares every field the
/// call records, since `tracing` drops a record for an undeclared field
/// without a word.
fn native_procedure_span(cell_id: &str, procedure_name: &'static str) -> tracing::Span {
    tracing::info_span!(
        "query.execute",
        hydradb.cell_id = %cell_id,
        hydradb.query.access_path = "NativePathProcedure",
        hydradb.query.procedure = procedure_name,
        hydradb.query.result_cache = tracing::field::Empty,
        hydradb.read_epoch = tracing::field::Empty,
        hydradb.gds.index_base_epoch = tracing::field::Empty,
        hydradb.gds.graph_cache = tracing::field::Empty,
        hydradb.gds.deleted_pairs = tracing::field::Empty,
        hydradb.gds.sampling_size = tracing::field::Empty,
        hydradb.gds.sampling_seed = tracing::field::Empty,
        hydradb.gds.properties_written = tracing::field::Empty,
        hydradb.gds.skipped_missing_vertices = tracing::field::Empty,
        hydradb.gds.write_millis = tracing::field::Empty,
        hydradb.gds.carried_forward = tracing::field::Empty,
        hydradb.gds.carry_forward_entries = tracing::field::Empty,
        hydradb.gds.carry_forward_millis = tracing::field::Empty,
        hydradb.gds.pre_processing_millis = tracing::field::Empty,
        hydradb.gds.compute_millis = tracing::field::Empty,
        hydradb.gds.iterations = tracing::field::Empty,
        hydradb.gds.node_count = tracing::field::Empty,
        hydradb.gds.relationship_count = tracing::field::Empty,
        hydradb.query.rows_returned = tracing::field::Empty,
        hydradb.storage.remote_gets = tracing::field::Empty,
        hydradb.storage.remote_get_bytes = tracing::field::Empty,
        hydradb.storage.ranged_gets = tracing::field::Empty,
        hydradb.storage.puts = tracing::field::Empty,
        hydradb.storage.put_bytes = tracing::field::Empty,
        error.class = tracing::field::Empty,
        error.operation = tracing::field::Empty,
        hydradb.sampling.tail_keep = tracing::field::Empty,
    )
}

fn take_native_path_page_rows(rows: &mut VecDeque<QueryRow>, page_size: usize) -> Vec<QueryRow> {
    let take = page_size.min(rows.len());
    rows.drain(..take).collect()
}

fn native_path_rows_resident_bytes(rows: &VecDeque<QueryRow>) -> u64 {
    rows.iter()
        .fold(0_u64, |total, row| total.saturating_add(row.estimated_resident_bytes()))
}

pub(super) fn native_path_result_resident_bytes(result: &QueryResultSet) -> usize {
    let columns = result.columns.iter().fold(0_usize, |bytes, column| {
        bytes
            .saturating_add(std::mem::size_of::<QueryColumn>())
            .saturating_add(column.name.capacity())
    });
    result.rows.iter().fold(columns, |bytes, row| {
        bytes.saturating_add(usize::try_from(row.estimated_resident_bytes()).unwrap_or(usize::MAX))
    })
}

fn purge_native_path_page_cursors(store: &mut crate::core::state::NativePathPageCursorStore) {
    let now = std::time::Instant::now();
    let mut released = 0_u64;
    store.cursors.retain(|_, cursor| {
        let retain = cursor.expires_at > now;
        if !retain {
            released = released.saturating_add(cursor.resident_bytes);
        }
        retain
    });
    store.resident_bytes = store.resident_bytes.saturating_sub(released);
}

fn next_native_path_page_cursor_id(store: &mut crate::core::state::NativePathPageCursorStore) -> Result<u64> {
    for _ in 0..=MAX_NATIVE_PATH_PAGE_CURSORS {
        let candidate = store.next_id.max(1);
        store.next_id = candidate.wrapping_add(1).max(1);
        if !store.cursors.contains_key(&candidate) {
            return Ok(candidate);
        }
    }
    Err(GraphError::AdmissionRejected {
        operation: "native_path_cursor_ids",
        actual: store.cursors.len().saturating_add(1) as u64,
        limit: MAX_NATIVE_PATH_PAGE_CURSORS as u64,
    })
}

fn native_path_relationship_cached<'a>(edge: &PathEdge, cache: &'a BTreeMap<PathEdge, EdgeMetadata>) -> Result<&'a EdgeMetadata> {
    cache.get(edge).ok_or_else(|| GraphError::CorruptValue {
        key: format!(
            "native-path/{}/{}/{}/{:?}",
            edge.edge_type, edge.src, edge.dst, edge.relationship_id
        ),
        reason: "enumerated relationship metadata is missing from the pinned snapshot cache".to_string(),
    })
}

fn enumerate_candidate_paths(
    procedure: &NativePathProcedure,
    adjacency: &BTreeMap<VertexId, Vec<PathEdge>>,
    max_candidates: usize,
    budget: &QueryBudget,
) -> Result<Vec<CandidatePath>> {
    if procedure.max_len == 0 {
        return Ok(Vec::new());
    }
    if procedure.weight_property.is_none() && procedure.cost_property.is_none() && procedure.max_cost.is_none() {
        return enumerate_unweighted_candidate_paths(procedure, adjacency, max_candidates, budget);
    }
    let mut candidates = Vec::new();
    let mut nodes = vec![procedure.source];
    let mut edges = Vec::new();
    let mut on_path = BTreeSet::from([procedure.source]);
    enumerate_candidate_paths_from(
        procedure,
        adjacency,
        procedure.source,
        &mut nodes,
        &mut edges,
        &mut on_path,
        &mut candidates,
        max_candidates,
        budget,
    )?;
    Ok(candidates)
}

fn enumerate_unweighted_candidate_paths(
    procedure: &NativePathProcedure,
    adjacency: &BTreeMap<VertexId, Vec<PathEdge>>,
    max_candidates: usize,
    budget: &QueryBudget,
) -> Result<Vec<CandidatePath>> {
    let mut queue = VecDeque::from([(vec![procedure.source], Vec::<PathEdge>::new())]);
    let mut candidates = Vec::new();
    let mut minimum_depth = None;
    while let Some((nodes, edges)) = queue.pop_front() {
        budget.check("native_path_breadth_first")?;
        if minimum_depth.is_some_and(|depth| edges.len() >= depth) || edges.len() >= usize::from(procedure.max_len) {
            continue;
        }
        let current = *nodes.last().expect("path always contains its source");
        for edge in adjacency.get(&current).into_iter().flatten() {
            let Some(next) = far_endpoint(current, edge) else {
                continue;
            };
            if nodes.contains(&next) {
                continue;
            }
            let mut next_nodes = nodes.clone();
            next_nodes.push(next);
            let mut next_edges = edges.clone();
            next_edges.push(edge.clone());
            let at_target = procedure.target.is_some_and(|target| target == next);
            if procedure.kind == NativePathProcedureKind::SingleSource || at_target {
                if candidates.len() >= max_candidates {
                    return Err(GraphError::AdmissionRejected {
                        operation: "native_path_candidates",
                        actual: candidates.len().saturating_add(1) as u64,
                        limit: max_candidates as u64,
                    });
                }
                candidates.push(CandidatePath {
                    nodes: next_nodes.clone(),
                    edges: next_edges.clone(),
                    weight: next_edges.len() as f64,
                    cost: 0.0,
                });
                if procedure.path_count > 0 && candidates.len() >= usize::try_from(procedure.path_count).unwrap_or(usize::MAX) {
                    return Ok(candidates);
                }
                if procedure.path_count == 0 {
                    minimum_depth.get_or_insert(next_edges.len());
                }
            }
            if !(procedure.kind == NativePathProcedureKind::SinglePair && at_target) {
                queue.push_back((next_nodes, next_edges));
                if queue.len().saturating_add(candidates.len()) > max_candidates {
                    return Err(GraphError::AdmissionRejected {
                        operation: "native_path_frontier_paths",
                        actual: queue.len().saturating_add(candidates.len()) as u64,
                        limit: max_candidates as u64,
                    });
                }
            }
        }
    }
    Ok(candidates)
}

fn enumerate_unweighted_candidate_paths_to_targets(
    procedure: &NativePathProcedure,
    adjacency: &BTreeMap<VertexId, Vec<PathEdge>>,
    targets: &[VertexId],
    max_candidates: usize,
    budget: &QueryBudget,
) -> Result<BTreeMap<VertexId, Vec<CandidatePath>>> {
    if procedure.max_len == 0 || targets.is_empty() {
        return Ok(BTreeMap::new());
    }
    let target_set = targets.iter().copied().collect::<BTreeSet<_>>();
    let mut grouped = targets
        .iter()
        .copied()
        .map(|target| (target, Vec::new()))
        .collect::<BTreeMap<_, _>>();
    let mut minimum_depths = BTreeMap::<VertexId, usize>::new();
    let mut queue = VecDeque::from([(vec![procedure.source], Vec::<PathEdge>::new())]);
    let requested_per_target = usize::try_from(procedure.path_count).unwrap_or(usize::MAX);

    while let Some((nodes, edges)) = queue.pop_front() {
        budget.check("native_path_multi_target_breadth_first")?;
        if procedure.path_count == 0
            && minimum_depths.len() == target_set.len()
            && minimum_depths.values().copied().max().is_some_and(|depth| edges.len() >= depth)
        {
            break;
        }
        if edges.len() >= usize::from(procedure.max_len) {
            continue;
        }
        let current = *nodes.last().expect("path always contains its source");
        for edge in adjacency.get(&current).into_iter().flatten() {
            let Some(next) = far_endpoint(current, edge) else {
                continue;
            };
            if nodes.contains(&next) {
                continue;
            }
            let mut next_nodes = nodes.clone();
            next_nodes.push(next);
            let mut next_edges = edges.clone();
            next_edges.push(edge.clone());

            if target_set.contains(&next) {
                let depth = next_edges.len();
                let candidate_count = grouped.values().map(Vec::len).sum::<usize>();
                let candidates_len = grouped.get(&next).expect("target group exists").len();
                let should_record = if procedure.path_count == 0 {
                    let minimum_depth = minimum_depths.entry(next).or_insert(depth);
                    depth == *minimum_depth
                } else {
                    candidates_len < requested_per_target
                };
                if should_record {
                    if candidate_count >= max_candidates {
                        return Err(GraphError::AdmissionRejected {
                            operation: "native_path_candidates",
                            actual: candidate_count.saturating_add(1) as u64,
                            limit: max_candidates as u64,
                        });
                    }
                    grouped.get_mut(&next).expect("target group exists").push(CandidatePath {
                        nodes: next_nodes.clone(),
                        edges: next_edges.clone(),
                        weight: next_edges.len() as f64,
                        cost: 0.0,
                    });
                }
                if procedure.path_count > 0 && grouped.values().all(|candidates| candidates.len() >= requested_per_target) {
                    return Ok(grouped);
                }
            }

            queue.push_back((next_nodes, next_edges));
            let candidate_count = grouped.values().map(Vec::len).sum::<usize>();
            if queue.len().saturating_add(candidate_count) > max_candidates {
                return Err(GraphError::AdmissionRejected {
                    operation: "native_path_frontier_paths",
                    actual: queue.len().saturating_add(candidate_count) as u64,
                    limit: max_candidates as u64,
                });
            }
        }
    }
    Ok(grouped)
}

#[allow(clippy::too_many_arguments)]
fn enumerate_candidate_paths_from(
    procedure: &NativePathProcedure,
    adjacency: &BTreeMap<VertexId, Vec<PathEdge>>,
    current: VertexId,
    nodes: &mut Vec<VertexId>,
    edges: &mut Vec<PathEdge>,
    on_path: &mut BTreeSet<VertexId>,
    candidates: &mut Vec<CandidatePath>,
    max_candidates: usize,
    budget: &QueryBudget,
) -> Result<()> {
    budget.check("native_path_enumerate")?;
    if edges.len() >= usize::from(procedure.max_len) {
        return Ok(());
    }
    for edge in adjacency.get(&current).into_iter().flatten() {
        let Some(next) = far_endpoint(current, edge) else {
            continue;
        };
        if !on_path.insert(next) {
            continue;
        }
        nodes.push(next);
        edges.push(edge.clone());
        let at_target = procedure.target.is_some_and(|target| target == next);
        if procedure.kind == NativePathProcedureKind::SingleSource || at_target {
            if candidates.len() >= max_candidates {
                return Err(GraphError::AdmissionRejected {
                    operation: "native_path_candidates",
                    actual: candidates.len().saturating_add(1) as u64,
                    limit: max_candidates as u64,
                });
            }
            candidates.push(CandidatePath {
                nodes: nodes.clone(),
                edges: edges.clone(),
                weight: 0.0,
                cost: 0.0,
            });
        }
        if !(procedure.kind == NativePathProcedureKind::SinglePair && at_target) {
            enumerate_candidate_paths_from(
                procedure,
                adjacency,
                next,
                nodes,
                edges,
                on_path,
                candidates,
                max_candidates,
                budget,
            )?;
        }
        edges.pop();
        nodes.pop();
        on_path.remove(&next);
    }
    Ok(())
}

/// The vertices an unweighted call will return from `candidates`, known
/// before their relationships are read, so both hydrations can share the
/// object-store latency instead of queueing behind each other.
///
/// Unweighted paths rank by length, then node sequence, then edges, and every
/// relationship variant of a structural path keeps its rank among the other
/// structural paths. So the first `keep` structural paths in that order hold
/// every returned vertex unless a structural edge turns out to have no
/// relationship, in which case the later hydration reads the rest. This only
/// warms the metadata cache; it never decides a result.
fn native_path_vertex_prefetch(procedure: &NativePathProcedure, candidates: &[CandidatePath], remaining_rows: usize) -> Vec<VertexId> {
    if procedure.weight_property.is_some() || procedure.cost_property.is_some() || procedure.max_cost.is_some() {
        return Vec::new();
    }
    let keep = if procedure.path_count == 0 {
        remaining_rows
    } else {
        usize::try_from(procedure.path_count).unwrap_or(usize::MAX).min(remaining_rows)
    };
    let mut ranked = candidates
        .iter()
        .map(|candidate| (candidate.edges.len(), &candidate.nodes))
        .collect::<Vec<_>>();
    if ranked.len() > keep {
        ranked.select_nth_unstable(keep);
        ranked.truncate(keep);
    }
    ranked.into_iter().flat_map(|(_, nodes)| nodes.iter().copied()).collect()
}

fn select_native_paths(procedure: &NativePathProcedure, candidates: &mut Vec<CandidatePath>) {
    candidates.retain(|candidate| {
        candidate.weight.is_finite() && candidate.cost.is_finite() && procedure.max_cost.is_none_or(|max_cost| candidate.cost <= max_cost.0)
    });
    candidates.sort_by(|left, right| {
        left.weight
            .total_cmp(&right.weight)
            .then_with(|| left.cost.total_cmp(&right.cost))
            .then_with(|| left.edges.len().cmp(&right.edges.len()))
            .then_with(|| left.nodes.cmp(&right.nodes))
            .then_with(|| left.edges.cmp(&right.edges))
    });
    if procedure.fair_relationship_variants {
        return;
    }
    if procedure.path_count == 0 {
        if let Some(weight) = candidates.first().map(|candidate| candidate.weight) {
            candidates.retain(|candidate| candidate.weight.total_cmp(&weight).is_eq());
        }
    } else {
        candidates.truncate(usize::try_from(procedure.path_count).unwrap_or(usize::MAX));
    }
}

fn fair_relationship_variant_candidates(groups: Vec<Vec<CandidatePath>>, limit: usize) -> Vec<CandidatePath> {
    let mut groups = groups.into_iter().map(VecDeque::from).collect::<Vec<_>>();
    let mut selected = Vec::with_capacity(limit);
    while selected.len() < limit {
        let mut advanced = false;
        for group in &mut groups {
            let Some(candidate) = group.pop_front() else {
                continue;
            };
            selected.push(candidate);
            advanced = true;
            if selected.len() >= limit {
                break;
            }
        }
        if !advanced {
            break;
        }
    }
    selected
}

fn far_endpoint(current: VertexId, edge: &PathEdge) -> Option<VertexId> {
    if edge.src == current {
        Some(edge.dst)
    } else if edge.dst == current {
        Some(edge.src)
    } else {
        None
    }
}

fn numeric_property(metadata: &EdgeMetadata, property: &str, default: f64) -> f64 {
    match metadata.properties.get(property) {
        Some(VertexPropertyValue::Integer(value)) => *value as f64,
        Some(VertexPropertyValue::SignedInteger(value)) => *value as f64,
        Some(VertexPropertyValue::Float(value)) => value.0,
        _ => default,
    }
}

fn numeric_query_value(value: f64) -> QueryValue {
    if value >= 0.0 && value.fract() == 0.0 && value <= u64::MAX as f64 {
        QueryValue::Count(value as u64)
    } else {
        QueryValue::Float(QueryFloat(value))
    }
}
