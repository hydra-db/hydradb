use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::Arc;
use tracing::Instrument as _;

use super::topology_tail::GraphTopologyOverlay;
use super::*;
use crate::query::path_procedure::{
    NativePathDirection, NativePathProcedure, NativePathProcedureKind, NativePathProjection,
};
use crate::{
    QueryPath, QueryPathNode, QueryPathRelationship, QueryResultSet, QueryRow, QueryValue,
};

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
        let adjacency = self
            .native_path_adjacency(read, &procedure, &topologies)
            .await?;
        let mut candidates = enumerate_candidate_paths(
            &procedure,
            &adjacency,
            self.limits.max_query_intermediate_rows,
            &budget,
        )?;
        let mut relationship_cache = BTreeMap::new();
        self.score_native_paths(
            &context.cell_id,
            read_epoch,
            &procedure,
            &mut candidates,
            &mut relationship_cache,
            &budget,
        )
        .await?;
        select_native_paths(&procedure, &mut candidates);
        if candidates.len() > self.limits.max_query_result_vertices {
            return Err(GraphError::AdmissionRejected {
                operation: "native_path_results",
                actual: candidates.len() as u64,
                limit: self.limits.max_query_result_vertices as u64,
            });
        }

        let mut vertex_cache = BTreeMap::new();
        let mut rows = Vec::with_capacity(candidates.len());
        for candidate in candidates {
            budget.check("native_path_hydrate")?;
            let path = self
                .hydrate_native_path(
                    read,
                    candidate.nodes.as_slice(),
                    candidate.edges.as_slice(),
                    &mut vertex_cache,
                    &mut relationship_cache,
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
                }));
            }
        }
        Ok(edges.into_iter().collect())
    }

    async fn score_native_paths(
        &self,
        cell_id: &str,
        read_epoch: StorageSequence,
        procedure: &NativePathProcedure,
        candidates: &mut [CandidatePath],
        relationship_cache: &mut BTreeMap<PathEdge, (Option<RelationshipId>, EdgeMetadata)>,
        budget: &QueryBudget,
    ) -> Result<()> {
        for candidate in candidates {
            budget.check("native_path_score")?;
            let mut weight = 0.0;
            let mut cost = 0.0;
            for edge in &candidate.edges {
                let relationship =
                    if procedure.weight_property.is_some() || procedure.cost_property.is_some() {
                        Some(
                            self.native_path_relationship_cached(
                                cell_id,
                                read_epoch,
                                edge,
                                relationship_cache,
                                budget,
                            )
                            .await?,
                        )
                    } else {
                        None
                    };
                weight += relationship.map_or(1.0, |(_, metadata)| {
                    procedure
                        .weight_property
                        .as_deref()
                        .map_or(1.0, |property| numeric_property(metadata, property, 1.0))
                });
                cost += relationship.map_or(0.0, |(_, metadata)| {
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

    async fn native_path_relationship_cached<'a>(
        &self,
        cell_id: &str,
        read_epoch: StorageSequence,
        edge: &PathEdge,
        cache: &'a mut BTreeMap<PathEdge, (Option<RelationshipId>, EdgeMetadata)>,
        budget: &QueryBudget,
    ) -> Result<&'a (Option<RelationshipId>, EdgeMetadata)> {
        if !cache.contains_key(edge) {
            let relationship = self
                .native_path_relationship_at(
                    cell_id,
                    &edge.edge_type,
                    edge.src,
                    edge.dst,
                    read_epoch,
                    budget,
                )
                .await?;
            cache.insert(edge.clone(), relationship);
        }
        Ok(cache.get(edge).expect("path relationship was inserted"))
    }

    async fn hydrate_native_path(
        &self,
        read: NativePathRead<'_>,
        nodes: &[VertexId],
        edges: &[PathEdge],
        vertex_cache: &mut BTreeMap<VertexId, VertexMetadata>,
        relationship_cache: &mut BTreeMap<PathEdge, (Option<RelationshipId>, EdgeMetadata)>,
    ) -> Result<QueryPath> {
        let mut path_nodes = Vec::with_capacity(nodes.len());
        for vertex in nodes {
            if !vertex_cache.contains_key(vertex) {
                let metadata = self
                    .vertex_metadata_at(read.cell_id, *vertex, read.read_epoch, read.budget)
                    .await?;
                vertex_cache.insert(*vertex, metadata);
            }
            let metadata = vertex_cache
                .get(vertex)
                .expect("vertex metadata was inserted");
            path_nodes.push(QueryPathNode {
                id: *vertex,
                labels: metadata.labels.iter().cloned().collect(),
                properties: metadata.properties.clone(),
            });
        }
        let mut path_relationships = Vec::with_capacity(edges.len());
        for edge in edges {
            let (relationship_id, metadata) = self
                .native_path_relationship_cached(
                    read.cell_id,
                    read.read_epoch,
                    edge,
                    relationship_cache,
                    read.budget,
                )
                .await?;
            path_relationships.push(QueryPathRelationship {
                id: *relationship_id,
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
    if procedure.path_count == 0 {
        if let Some(weight) = candidates.first().map(|candidate| candidate.weight) {
            candidates.retain(|candidate| candidate.weight.total_cmp(&weight).is_eq());
        }
    } else {
        candidates.truncate(usize::try_from(procedure.path_count).unwrap_or(usize::MAX));
    }
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
            rel_types: vec!["RELATES".to_string()],
            direction: NativePathDirection::Outgoing,
            max_len: 3,
            weight_property: None,
            cost_property: None,
            max_cost: None,
            path_count: 1,
            projections: vec![NativePathProjection::Path],
        }
    }

    fn edge(src: VertexId, dst: VertexId) -> PathEdge {
        PathEdge {
            edge_type: "RELATES".to_string(),
            src,
            dst,
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
