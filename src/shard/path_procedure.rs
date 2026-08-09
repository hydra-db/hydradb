use futures::{stream, StreamExt as _, TryStreamExt as _};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tracing::Instrument as _;

use super::topology_tail::GraphTopologyOverlay;
use super::*;
use crate::query::path_procedure::{
    NativePathDirection, NativePathNodeSelector, NativePathProcedure, NativePathProcedureKind,
    NativePathProjection,
};
use crate::{
    QueryCursorToken, QueryPath, QueryPathNode, QueryPathRelationship, QueryResultPage,
    QueryResultSet, QueryRow, QueryValue,
};

const MAX_NATIVE_PATH_PAGE_CURSORS: usize = 1_024;
const MAX_NATIVE_PATH_PAGE_CURSOR_BYTES: u64 = 64 * 1024 * 1024;
const NATIVE_PATH_PAGE_CURSOR_TTL: std::time::Duration = std::time::Duration::from_secs(60);
const NATIVE_PATH_HYDRATION_CONCURRENCY: usize = 16;
const NATIVE_PATH_SELECTOR_HYDRATION_BATCH_SIZE: usize = 256;

enum PathTopologySource {
    Compiled {
        matrix: Arc<crate::sparse_kernel::CompiledGraphBlasMatrix>,
        overlay: Option<GraphTopologyOverlay>,
    },
    Storage,
}

struct TypedPathTopology {
    edge_type: String,
    source: PathTopologySource,
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

impl GraphShard {
    pub(crate) async fn execute_native_path_rows_page(
        &self,
        context: QueryContext,
        query: &str,
        procedure: NativePathProcedure,
        cursor: Option<QueryCursorToken>,
        page_size: usize,
    ) -> Result<QueryResultPage> {
        if page_size == 0 {
            return Err(GraphError::UnsupportedQuery {
                dialect: "OpenCypher",
                feature: "native path page size must be greater than zero".to_string(),
            });
        }
        if let Some(cursor) = cursor {
            return self
                .continue_native_path_page(&context, query, cursor, page_size)
                .await;
        }

        let scope = context.scope.clone();
        let cell_id = context.cell_id.clone();
        let parameters = context.parameters.clone();
        let result = self.execute_native_path_rows(context, procedure).await?;
        let QueryResultSet {
            columns,
            rows,
            read_epoch: _,
            storage_sequence: _,
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
        Ok(QueryResultPage::new(
            columns,
            page_rows,
            Some(QueryCursorToken::new(cursor_id)),
        ))
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

    pub(crate) async fn execute_native_path_rows(
        &self,
        context: QueryContext,
        procedure: NativePathProcedure,
    ) -> Result<QueryResultSet> {
        self.operation_metrics
            .query_rows_started
            .fetch_add(1, Ordering::Relaxed);
        let started = std::time::Instant::now();
        let span = tracing::info_span!(
            "query.execute",
            turbolay.cell_id = %context.cell_id,
            turbolay.query.access_path = "NativePathProcedure",
            turbolay.read_epoch = tracing::field::Empty,
            turbolay.query.rows_returned = tracing::field::Empty,
            error.class = tracing::field::Empty,
            turbolay.sampling.tail_keep = tracing::field::Empty,
        );
        let result = self
            .execute_native_path_rows_snapshot(context, procedure)
            .instrument(span.clone())
            .await;
        self.operation_metrics
            .query_rows_latency
            .record(started.elapsed());
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
            Err(error) => {
                self.operation_metrics.record_query_rows_failure(error);
                span.record("error.class", error.class());
                span.record("turbolay.sampling.tail_keep", "error");
            }
        }
        result
    }

    async fn execute_native_path_rows_snapshot(
        &self,
        context: QueryContext,
        procedure: NativePathProcedure,
    ) -> Result<QueryResultSet> {
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
                self.execute_native_path_rows_inner(context, procedure),
            )
            .await;
        }
        self.execute_native_path_rows_inner(context, procedure)
            .await
    }

    async fn execute_native_path_rows_inner(
        &self,
        context: QueryContext,
        procedure: NativePathProcedure,
    ) -> Result<QueryResultSet> {
        validate_component("cell_id", &context.cell_id)?;
        let budget = QueryBudget::new(
            context.max_runtime_ms.or(self.limits.max_query_runtime_ms),
            context.cancellation_token.clone(),
        )
        .with_max_result_bytes(context.max_result_bytes);
        budget.check("native_path_start")?;
        if procedure.path_count > 0
            && procedure.path_count > self.limits.max_query_result_vertices as u64
        {
            return Err(GraphError::AdmissionRejected {
                operation: "native_path_count",
                actual: procedure.path_count,
                limit: self.limits.max_query_result_vertices as u64,
            });
        }
        let result_limit = procedure
            .result_limit
            .unwrap_or(self.limits.max_query_result_vertices as u64);
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
        let snapshot = self.db.snapshot().await?;
        if snapshot.seq() != read_epoch {
            return Err(GraphError::SnapshotAhead {
                cell_id: context.cell_id,
                read_epoch,
                current_epoch: snapshot.seq(),
            });
        }

        let topologies = self
            .native_path_topologies(&context.cell_id, &procedure, read_epoch, &budget)
            .await?;
        let read = NativePathRead {
            snapshot: snapshot.as_ref(),
            cell_id: &context.cell_id,
            read_epoch,
            budget: &budget,
        };
        let sources = self
            .resolve_native_path_selector(
                read,
                procedure.source_selector.as_ref(),
                procedure.source,
            )
            .await?;
        let targets = match procedure.target_selector.as_ref() {
            Some(selector) => Some(
                self.resolve_native_path_selector(read, Some(selector), 0)
                    .await?,
            ),
            None => procedure.target.map(|target| vec![target]),
        };
        let mut relationship_cache = BTreeMap::new();
        let mut relationship_choices_cache = BTreeMap::new();
        let mut vertex_cache = BTreeMap::new();
        let mut rows = Vec::new();
        let mut relationship_variant_structures = Vec::new();
        let result_limit_usize = usize::try_from(result_limit).unwrap_or(usize::MAX);
        'sources: for source in sources {
            let mut scalar = procedure.clone();
            scalar.source = source;
            scalar.source_selector = None;
            scalar.target_selector = None;
            scalar.result_limit = None;
            let adjacency = self
                .native_path_adjacency(read, &scalar, &topologies)
                .await?;
            let endpoints = targets
                .as_ref()
                .map(|targets| targets.iter().copied().map(Some).collect::<Vec<_>>())
                .unwrap_or_else(|| vec![None]);
            for target in endpoints {
                if target == Some(source)
                    || procedure.pairwise && target.is_some_and(|target| source >= target)
                {
                    continue;
                }
                scalar.target = target;
                scalar.kind = if target.is_some() {
                    NativePathProcedureKind::SinglePair
                } else {
                    NativePathProcedureKind::SingleSource
                };
                let candidates = enumerate_candidate_paths(
                    &scalar,
                    &adjacency,
                    self.limits.max_query_intermediate_rows,
                    &budget,
                )?;
                if scalar.fair_relationship_variants {
                    for structural_path in candidates {
                        relationship_variant_structures.push(structural_path);
                        if relationship_variant_structures.len() >= result_limit_usize {
                            break 'sources;
                        }
                    }
                    continue;
                }
                let mut candidates = self
                    .expand_native_path_relationships(
                        read,
                        candidates,
                        &mut relationship_choices_cache,
                        &mut relationship_cache,
                        self.limits.max_query_intermediate_rows,
                        false,
                    )
                    .await?;
                self.score_native_paths(&scalar, &mut candidates, &relationship_cache, &budget)?;
                select_native_paths(&scalar, &mut candidates);
                candidates.truncate(result_limit_usize.saturating_sub(rows.len()));
                let vertex_ids = candidates
                    .iter()
                    .flat_map(|candidate| candidate.nodes.iter().copied())
                    .collect::<Vec<_>>();
                self.hydrate_vertex_metadata_cache_at(
                    read.cell_id,
                    &vertex_ids,
                    read.read_epoch,
                    &mut vertex_cache,
                    read.budget,
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
            let mut relationship_variant_groups = Vec::with_capacity(structure_count);
            for (index, structural_path) in relationship_variant_structures.into_iter().enumerate()
            {
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
            let candidates = fair_relationship_variant_candidates(
                relationship_variant_groups,
                result_limit_usize,
            );
            let vertex_ids = candidates
                .iter()
                .flat_map(|candidate| candidate.nodes.iter().copied())
                .collect::<Vec<_>>();
            self.hydrate_vertex_metadata_cache_at(
                read.cell_id,
                &vertex_ids,
                read.read_epoch,
                &mut vertex_cache,
                read.budget,
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
        let result = QueryResultSet::new(
            procedure
                .projections
                .iter()
                .map(|projection| projection.column())
                .collect(),
            rows,
        )
        .with_read_epoch(read_epoch);
        Ok(match context.validated_storage_sequence() {
            Some(sequence) => result.with_storage_sequence(sequence),
            None => result,
        })
    }

    async fn resolve_native_path_selector(
        &self,
        read: NativePathRead<'_>,
        selector: Option<&NativePathNodeSelector>,
        fallback: VertexId,
    ) -> Result<Vec<VertexId>> {
        let Some(selector) = selector else {
            return Ok(vec![fallback]);
        };
        let mut vertices = BTreeSet::new();
        let raw_candidate_count = Arc::new(AtomicUsize::new(0));
        let indexed_candidates = stream::iter(selector.values.iter().cloned().map(|value| {
            let raw_candidate_count = Arc::clone(&raw_candidate_count);
            async move {
                read.budget.check("native_path_selector")?;
                let property_value = VertexPropertyValue::String(value);
                let encoded = encode_vertex_property_value_key(&property_value);
                let prefix =
                    keys::vertex_property_index_prefix(read.cell_id, &selector.property, &encoded);
                let mut candidates = read
                    .snapshot
                    .scan_prefix_with_options(prefix.as_bytes(), .., &cached_remote_scan_options())
                    .await?;
                let mut indexed_candidates = Vec::new();
                while let Some(kv) = candidates.next().await? {
                    read.budget.check("native_path_selector")?;
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
            let metadata = self
                .vertex_metadata_batch_at(read.cell_id, &vertex_ids, read.read_epoch, read.budget)
                .await?;
            for ((vertex, property_value), (_, metadata)) in batch.iter().zip(metadata) {
                if metadata.labels.contains(&selector.label)
                    && metadata.properties.get(&selector.property) == Some(property_value)
                {
                    vertices.insert(*vertex);
                }
            }
        }
        Ok(vertices.into_iter().collect())
    }

    async fn native_path_topologies(
        &self,
        cell_id: &str,
        procedure: &NativePathProcedure,
        read_epoch: StorageSequence,
        budget: &QueryBudget,
    ) -> Result<Vec<TypedPathTopology>> {
        let mut topologies = Vec::with_capacity(procedure.rel_types.len());
        for edge_type in &procedure.rel_types {
            budget.check("native_path_artifact_lookup")?;
            let source = if crate::sparse_kernel::default_matrix_kernel(&self.cache_policy)
                == SparseKernelBackend::Adjacency
            {
                PathTopologySource::Storage
            } else if let Some(artifact) = self
                .traced_latest_matrix_artifact(cell_id, edge_type, read_epoch)
                .await?
            {
                match self
                    .compiled_graphblas_query_snapshot(
                        cell_id,
                        edge_type,
                        artifact.base_epoch,
                        read_epoch,
                        budget,
                    )
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
    ) -> Result<BTreeMap<VertexId, Vec<PathEdge>>> {
        let mut adjacency = BTreeMap::new();
        let mut seen = BTreeSet::from([procedure.source]);
        let mut frontier = BTreeSet::from([procedure.source]);
        let mut edge_visits = 0_u64;
        for depth in 0..procedure.max_len {
            if frontier.is_empty() {
                break;
            }
            let mut next = BTreeSet::new();
            for vertex in frontier {
                read.budget.check("native_path_frontier")?;
                let edges = self
                    .native_path_successors(read, vertex, procedure.direction, topologies)
                    .await?;
                edge_visits = edge_visits.saturating_add(edges.len() as u64);
                if edge_visits > self.limits.max_query_scan_edges {
                    return Err(GraphError::AdmissionRejected {
                        operation: "native_path_edges",
                        actual: edge_visits,
                        limit: self.limits.max_query_scan_edges,
                    });
                }
                if depth + 1 < procedure.max_len {
                    for edge in &edges {
                        let neighbor =
                            far_endpoint(vertex, edge).expect("successor touches source");
                        if seen.insert(neighbor) {
                            next.insert(neighbor);
                        }
                    }
                }
                adjacency.insert(vertex, edges);
            }
            if seen.len() > self.limits.max_query_intermediate_rows {
                return Err(GraphError::AdmissionRejected {
                    operation: "native_path_vertices",
                    actual: seen.len() as u64,
                    limit: self.limits.max_query_intermediate_rows as u64,
                });
            }
            frontier = next;
        }
        Ok(adjacency)
    }

    async fn native_path_successors(
        &self,
        read: NativePathRead<'_>,
        vertex: VertexId,
        direction: NativePathDirection,
        topologies: &[TypedPathTopology],
    ) -> Result<Vec<PathEdge>> {
        let mut edges = BTreeSet::new();
        for topology in topologies {
            read.budget.check("native_path_neighbors")?;
            if direction != NativePathDirection::Incoming {
                let neighbors = match &topology.source {
                    PathTopologySource::Compiled { matrix, overlay } => {
                        let mut neighbors =
                            crate::sparse_kernel::compiled_graphblas_out_neighbors(matrix, vertex)
                                .into_iter()
                                .collect::<BTreeSet<_>>();
                        if let Some(overlay) = overlay {
                            overlay.apply_out_neighbors(vertex, &mut neighbors);
                        }
                        neighbors.into_iter().collect()
                    }
                    PathTopologySource::Storage => {
                        self.out_neighbors_in_storage_snapshot(
                            read.snapshot,
                            read.cell_id,
                            &topology.edge_type,
                            vertex,
                            read.read_epoch,
                        )
                        .await?
                    }
                };
                edges.extend(neighbors.into_iter().map(|dst| PathEdge {
                    edge_type: topology.edge_type.clone(),
                    src: vertex,
                    dst,
                    relationship_id: None,
                }));
            }
            if direction != NativePathDirection::Outgoing {
                let neighbors = match &topology.source {
                    PathTopologySource::Compiled { matrix, overlay } => {
                        let mut neighbors =
                            crate::sparse_kernel::compiled_graphblas_in_neighbors(matrix, vertex)
                                .into_iter()
                                .collect::<BTreeSet<_>>();
                        if let Some(overlay) = overlay {
                            overlay.apply_in_neighbors(vertex, &mut neighbors);
                        }
                        neighbors.into_iter().collect()
                    }
                    PathTopologySource::Storage => {
                        self.in_neighbors_in_storage_snapshot(
                            read.snapshot,
                            read.cell_id,
                            &topology.edge_type,
                            vertex,
                        )
                        .await?
                    }
                };
                edges.extend(neighbors.into_iter().map(|src| PathEdge {
                    edge_type: topology.edge_type.clone(),
                    src,
                    dst: vertex,
                    relationship_id: None,
                }));
            }
        }
        Ok(edges.into_iter().collect())
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

        let mut expanded = Vec::new();
        for candidate in candidates {
            let mut edge_variants = vec![Vec::with_capacity(candidate.edges.len())];
            for structural_edge in &candidate.edges {
                let choices = choices_cache
                    .get(structural_edge)
                    .expect("relationship choices were inserted");
                let product_size = edge_variants.len().saturating_mul(choices.len());
                if !truncate_at_limit
                    && expanded.len().saturating_add(product_size) > max_candidates
                {
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
                let relationship =
                    if procedure.weight_property.is_some() || procedure.cost_property.is_some() {
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
        self.hydrate_vertex_metadata_cache_at(
            read.cell_id,
            nodes,
            read.read_epoch,
            vertex_cache,
            read.budget,
        )
        .await?;
        let mut path_nodes = Vec::with_capacity(nodes.len());
        for vertex in nodes {
            let metadata = vertex_cache
                .get(vertex)
                .expect("vertex metadata was hydrated");
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

fn take_native_path_page_rows(rows: &mut VecDeque<QueryRow>, page_size: usize) -> Vec<QueryRow> {
    let take = page_size.min(rows.len());
    rows.drain(..take).collect()
}

fn native_path_rows_resident_bytes(rows: &VecDeque<QueryRow>) -> u64 {
    rows.iter().fold(0_u64, |total, row| {
        total.saturating_add(row.estimated_resident_bytes())
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

fn next_native_path_page_cursor_id(
    store: &mut crate::core::state::NativePathPageCursorStore,
) -> Result<u64> {
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

fn native_path_relationship_cached<'a>(
    edge: &PathEdge,
    cache: &'a BTreeMap<PathEdge, EdgeMetadata>,
) -> Result<&'a EdgeMetadata> {
    cache.get(edge).ok_or_else(|| GraphError::CorruptValue {
        key: format!(
            "native-path/{}/{}/{}/{:?}",
            edge.edge_type, edge.src, edge.dst, edge.relationship_id
        ),
        reason: "enumerated relationship metadata is missing from the pinned snapshot cache"
            .to_string(),
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
    if procedure.weight_property.is_none()
        && procedure.cost_property.is_none()
        && procedure.max_cost.is_none()
    {
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
        if minimum_depth.is_some_and(|depth| edges.len() >= depth)
            || edges.len() >= usize::from(procedure.max_len)
        {
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
                if procedure.path_count > 0
                    && candidates.len()
                        >= usize::try_from(procedure.path_count).unwrap_or(usize::MAX)
                {
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

fn select_native_paths(procedure: &NativePathProcedure, candidates: &mut Vec<CandidatePath>) {
    candidates.retain(|candidate| {
        candidate.weight.is_finite()
            && candidate.cost.is_finite()
            && procedure
                .max_cost
                .is_none_or(|max_cost| candidate.cost <= max_cost.0)
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

fn fair_relationship_variant_candidates(
    groups: Vec<Vec<CandidatePath>>,
    limit: usize,
) -> Vec<CandidatePath> {
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

#[cfg(test)]
mod tests {
    use super::*;

    fn procedure(kind: NativePathProcedureKind, target: Option<VertexId>) -> NativePathProcedure {
        NativePathProcedure {
            kind,
            source: 1,
            target,
            source_selector: None,
            target_selector: None,
            pairwise: false,
            fair_relationship_variants: false,
            rel_types: vec!["RELATES".to_string()],
            direction: NativePathDirection::Outgoing,
            max_len: 3,
            weight_property: None,
            cost_property: None,
            max_cost: None,
            path_count: 1,
            result_limit: None,
            projections: vec![NativePathProjection::Path],
        }
    }

    fn edge(src: VertexId, dst: VertexId) -> PathEdge {
        PathEdge {
            edge_type: "RELATES".to_string(),
            src,
            dst,
            relationship_id: None,
        }
    }

    #[test]
    fn single_pair_returns_shortest_deterministic_path() {
        let adjacency = BTreeMap::from([
            (1, vec![edge(1, 2), edge(1, 3)]),
            (2, vec![edge(2, 4)]),
            (3, vec![edge(3, 5)]),
            (5, vec![edge(5, 4)]),
        ]);
        let budget = QueryBudget::new(None, None);
        let mut candidates = enumerate_candidate_paths(
            &procedure(NativePathProcedureKind::SinglePair, Some(4)),
            &adjacency,
            100,
            &budget,
        )
        .unwrap();
        for candidate in &mut candidates {
            candidate.weight = candidate.edges.len() as f64;
        }
        select_native_paths(
            &procedure(NativePathProcedureKind::SinglePair, Some(4)),
            &mut candidates,
        );
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].nodes, vec![1, 2, 4]);
    }

    #[test]
    fn enumeration_never_emits_cyclic_paths() {
        let adjacency = BTreeMap::from([(1, vec![edge(1, 2)]), (2, vec![edge(2, 1), edge(2, 3)])]);
        let budget = QueryBudget::new(None, None);
        let mut request = procedure(NativePathProcedureKind::SingleSource, None);
        request.path_count = 10;
        let candidates = enumerate_candidate_paths(&request, &adjacency, 100, &budget).unwrap();
        assert_eq!(
            candidates
                .iter()
                .map(|candidate| candidate.nodes.clone())
                .collect::<Vec<_>>(),
            vec![vec![1, 2], vec![1, 2, 3]]
        );
    }
}
