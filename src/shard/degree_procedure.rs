//! `CALL algo.degree(...)`: relationship degree of every vertex with a label.
//!
//! The Cypher form of this question -- `MATCH (e:Entity)-[:RELATES]->() RETURN
//! e, count(*)` plus its incoming mirror, or the undirected `-[:RELATES]-`,
//! which counts a self-loop once -- expands and materialises every
//! relationship record. Here the structural degree comes from the same
//! topology the path procedures read (the compiled CSC generation plus its
//! WAL-tail overlay, or a pinned storage scan when no generation applies), so
//! it is exact at the read epoch exactly when `algo.MSpaths` is. Parallel
//! relationships between one pair are then added from the per-pair
//! relationship counters, which a single prefix scan returns. With
//! `neighborLabel`, each relationship is checked against the set of vertices
//! carrying that label, read once from the label index. Only vertices
//! that clear `minDegree` are hydrated, highest degree first, until `limit`
//! rows carry the label.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use super::path_procedure::{native_path_result_resident_bytes, PathTopologySource, TypedPathTopology};
use super::*;
use crate::query::path_procedure::{NativeDegreeProcedure, NativeDegreeValue};
use crate::{QueryResultSet, QueryRow, QueryValue};

/// Vertices whose metadata is read concurrently while ranked candidates are
/// checked for the label.
const NATIVE_DEGREE_HYDRATION_BATCH_SIZE: usize = 256;
/// Metadata point reads in flight; ranked vertices are scattered, so each
/// cold read is its own object-store round trip.
const NATIVE_DEGREE_HYDRATION_CONCURRENCY: usize = 64;

#[derive(Clone, Copy)]
pub(super) struct DegreeRead<'a> {
    pub(super) snapshot: &'a GraphStorageSnapshot,
    pub(super) cell_id: &'a str,
    pub(super) read_epoch: StorageSequence,
    pub(super) budget: &'a QueryBudget,
}

/// Structural edges of one type, as the degree pass needs to consult them
/// after counting: whether a counted pair is still a live edge.
enum StructuralEdges<'a> {
    Compiled {
        matrix: &'a crate::sparse_kernel::CompiledGraphBlasMatrix,
        overlay: Option<&'a super::topology_tail::GraphTopologyOverlay>,
    },
    Storage(BTreeMap<VertexId, Vec<VertexId>>),
}

impl StructuralEdges<'_> {
    fn contains(&self, src: VertexId, dst: VertexId) -> bool {
        match self {
            Self::Compiled { matrix, overlay } => overlay
                .and_then(|overlay| overlay.state(src, dst))
                .unwrap_or_else(|| matrix.contains_edge(src, dst)),
            Self::Storage(adjacency) => adjacency
                .get(&src)
                .is_some_and(|destinations| destinations.binary_search(&dst).is_ok()),
        }
    }
}

/// Which endpoint of a relationship it counts for: its source when outgoing
/// degree is asked for, its destination when incoming is, and only when the
/// other endpoint is in `neighbors` if a neighbour label was given. With
/// `self_loops_once` (the undirected direction) a self-loop counts once for
/// its vertex rather than once per direction.
struct DegreeRule {
    outgoing: bool,
    incoming: bool,
    self_loops_once: bool,
    neighbors: Option<HashSet<VertexId>>,
}

impl DegreeRule {
    fn counts_toward(&self, other: VertexId) -> bool {
        self.neighbors.as_ref().is_none_or(|neighbors| neighbors.contains(&other))
    }

    /// The vertices one relationship `src -> dst` counts for, at most two.
    fn endpoints(&self, src: VertexId, dst: VertexId) -> impl Iterator<Item = VertexId> {
        let outgoing = (self.outgoing && self.counts_toward(dst)).then_some(src);
        let incoming =
            (self.incoming && self.counts_toward(src) && !(self.self_loops_once && src == dst && outgoing.is_some())).then_some(dst);
        outgoing.into_iter().chain(incoming)
    }

    fn credit(&self, degrees: &mut HashMap<VertexId, u64>, src: VertexId, dst: VertexId, relationships: u64) {
        for vertex in self.endpoints(src, dst) {
            *degrees.entry(vertex).or_default() += relationships;
        }
    }

    fn debit(&self, degrees: &mut HashMap<VertexId, u64>, src: VertexId, dst: VertexId) {
        for vertex in self.endpoints(src, dst) {
            let degree = degrees.entry(vertex).or_default();
            *degree = degree.saturating_sub(1);
        }
    }
}

impl GraphShard {
    pub(super) async fn execute_native_degree_rows_inner(
        &self,
        context: QueryContext,
        procedure: NativeDegreeProcedure,
    ) -> Result<QueryResultSet> {
        validate_component("cell_id", &context.cell_id)?;
        let budget = QueryBudget::new(
            context.max_runtime_ms.or(self.limits.max_query_runtime_ms),
            context.cancellation_token.clone(),
        )
        .with_max_result_bytes(context.max_result_bytes);
        budget.check("native_degree_start")?;
        let max_rows = self.limits.max_query_result_vertices as u64;
        let limit = procedure.limit.unwrap_or(max_rows);
        if limit > max_rows {
            return Err(GraphError::AdmissionRejected {
                operation: "native_degree_limit",
                actual: limit,
                limit: max_rows,
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

        let read = DegreeRead {
            snapshot: snapshot.as_ref(),
            cell_id: &context.cell_id,
            read_epoch,
            budget: &budget,
        };
        let degrees = self.native_degree_counts(read, &procedure).await?;
        let rows = self.native_degree_rows(read, &procedure, degrees, limit).await?;

        let result = QueryResultSet::new(procedure.columns(), rows).with_read_epoch(read_epoch);
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

    /// Relationship degree of every vertex with at least one relationship of
    /// the requested types and direction, before any label filter.
    async fn native_degree_counts(&self, read: DegreeRead<'_>, procedure: &NativeDegreeProcedure) -> Result<HashMap<VertexId, u64>> {
        let mut scanned_records = 0_u64;
        let neighbors = match &procedure.neighbor_label {
            Some(label) => Some(self.native_degree_label_members(read, label, &mut scanned_records).await?),
            None => None,
        };
        let rule = DegreeRule {
            outgoing: procedure.direction.outgoing(),
            incoming: procedure.direction.incoming(),
            self_loops_once: procedure.direction.self_loops_once(),
            neighbors,
        };
        let topologies = self
            .native_path_topologies(read.cell_id, &procedure.rel_types, read.read_epoch, read.budget)
            .await?;
        let mut degrees = HashMap::<VertexId, u64>::new();
        for TypedPathTopology { edge_type, source } in &topologies {
            read.budget.check("native_degree_topology")?;
            let edges = match source {
                PathTopologySource::Compiled { matrix, overlay } => {
                    let mut checked = 0_u32;
                    let mut check = || {
                        checked = checked.wrapping_add(1);
                        if checked.is_multiple_of(4_096) {
                            read.budget.check("native_degree_compiled")?;
                        }
                        Ok(())
                    };
                    if rule.neighbors.is_none() {
                        // Unfiltered degrees are pointer differences of the CSC.
                        matrix.accumulate_degrees(
                            rule.outgoing,
                            rule.incoming,
                            rule.self_loops_once,
                            |vertex, degree| *degrees.entry(vertex).or_default() += degree,
                            &mut check,
                        )?;
                    } else {
                        matrix.for_each_edge(|src, dst| rule.credit(&mut degrees, src, dst, 1), &mut check)?;
                    }
                    // The overlay records the live state at the read epoch of
                    // every pair written since the generation; only a pair whose
                    // state differs from the generation changes a degree.
                    if let Some(overlay) = overlay {
                        for (src, dst, exists) in overlay.entries() {
                            check()?;
                            if exists == matrix.contains_edge(src, dst) {
                                continue;
                            }
                            if exists {
                                rule.credit(&mut degrees, src, dst, 1);
                            } else {
                                rule.debit(&mut degrees, src, dst);
                            }
                        }
                    }
                    StructuralEdges::Compiled {
                        matrix: matrix.as_ref(),
                        overlay: overlay.as_ref(),
                    }
                }
                PathTopologySource::Storage => {
                    let adjacency = self.native_degree_storage_adjacency(read, edge_type, &mut scanned_records).await?;
                    let mut checked = 0_u32;
                    for (src, destinations) in &adjacency {
                        for dst in destinations {
                            checked = checked.wrapping_add(1);
                            if checked.is_multiple_of(4_096) {
                                read.budget.check("native_degree_storage")?;
                            }
                            rule.credit(&mut degrees, *src, *dst, 1);
                        }
                    }
                    StructuralEdges::Storage(adjacency)
                }
            };
            self.ensure_native_degree_vertices(degrees.len())?;
            self.add_parallel_relationship_degrees(read, edge_type, &edges, &rule, &mut degrees, &mut scanned_records)
                .await?;
        }
        degrees.retain(|_, degree| *degree > 0);
        Ok(degrees)
    }

    /// Every vertex carrying `label` at the read epoch, from the label index:
    /// one sequential scan of id-only keys, never a metadata read. The index
    /// is written in the same transaction as the vertex's labels.
    pub(super) async fn native_degree_label_members(
        &self,
        read: DegreeRead<'_>,
        label: &str,
        scanned_records: &mut u64,
    ) -> Result<HashSet<VertexId>> {
        let mut members = HashSet::new();
        self.for_each_native_degree_label_member(read, label, scanned_records, |vertex| {
            members.insert(vertex);
        })
        .await?;
        Ok(members)
    }

    /// Every member of a label, in ascending id order: the label index keys
    /// are zero-padded ids under one prefix, and the scan is ordered.
    pub(super) async fn for_each_native_degree_label_member(
        &self,
        read: DegreeRead<'_>,
        label: &str,
        scanned_records: &mut u64,
        mut visit: impl FnMut(VertexId),
    ) -> Result<()> {
        let prefix = keys::vertex_label_prefix(read.cell_id, label);
        let mut iter = read
            .budget
            .read_only_io("native_degree_neighbor_labels", async {
                Ok::<_, GraphError>(
                    read.snapshot
                        .scan_prefix_with_options(prefix.as_bytes(), .., &remote_bulk_scan_options())
                        .await?,
                )
            })
            .await?;
        loop {
            let Some(kv) = read
                .budget
                .read_only_io("native_degree_neighbor_labels", async { Ok::<_, GraphError>(iter.next().await?) })
                .await?
            else {
                break;
            };
            self.charge_native_degree_records(scanned_records, 1)?;
            let key = String::from_utf8_lossy(&kv.key);
            visit(parse_u64(&key, key.strip_prefix(&prefix).unwrap_or_default(), "vertex id")?);
        }
        Ok(())
    }

    /// A structural edge stands for one relationship, or for each of its
    /// relationship records when it has several: what the experimental
    /// engine's expansion matches. Its own properties are inherited by its
    /// records, not a relationship of their own (`ordered_relationship_walk`);
    /// the legacy engine's expansion also returns the edge itself then. Every pair with relationships has a counter; only the pairs
    /// with more than one change a degree.
    async fn add_parallel_relationship_degrees(
        &self,
        read: DegreeRead<'_>,
        edge_type: &str,
        edges: &StructuralEdges<'_>,
        rule: &DegreeRule,
        degrees: &mut HashMap<VertexId, u64>,
        scanned_records: &mut u64,
    ) -> Result<()> {
        let prefix = keys::relationship_count_edge_type_prefix(read.cell_id, edge_type);
        let mut iter = read
            .budget
            .read_only_io("native_degree_relationship_counts", async {
                Ok::<_, GraphError>(
                    read.snapshot
                        .scan_prefix_with_options(prefix.as_bytes(), .., &remote_bulk_scan_options())
                        .await?,
                )
            })
            .await?;
        loop {
            let Some(kv) = read
                .budget
                .read_only_io("native_degree_relationship_counts", async {
                    Ok::<_, GraphError>(iter.next().await?)
                })
                .await?
            else {
                break;
            };
            self.charge_native_degree_records(scanned_records, 1)?;
            let key = String::from_utf8_lossy(&kv.key);
            let count = decode_u64(&key, &kv.value)?;
            if count <= 1 {
                continue;
            }
            let (src, dst) = parse_relationship_count_suffix(&key, &prefix)?;
            if !edges.contains(src, dst) {
                continue;
            }
            rule.credit(degrees, src, dst, count - 1);
        }
        self.ensure_native_degree_vertices(degrees.len())
    }

    /// The structural adjacency of one edge type at the read epoch, read from
    /// the pinned snapshot: individual edge rows plus visible segment
    /// destinations, as the index builder assembles a generation.
    async fn native_degree_storage_adjacency(
        &self,
        read: DegreeRead<'_>,
        edge_type: &str,
        scanned_records: &mut u64,
    ) -> Result<BTreeMap<VertexId, Vec<VertexId>>> {
        let mut adjacency = BTreeMap::<VertexId, Vec<VertexId>>::new();
        let prefix = keys::out_edge_type_prefix(read.cell_id, edge_type);
        let mut iter = read
            .budget
            .read_only_io("native_degree_edge_scan", async {
                Ok::<_, GraphError>(
                    read.snapshot
                        .scan_prefix_with_options(prefix.as_bytes(), .., &remote_bulk_scan_options())
                        .await?,
                )
            })
            .await?;
        loop {
            let Some(kv) = read
                .budget
                .read_only_io("native_degree_edge_scan", async { Ok::<_, GraphError>(iter.next().await?) })
                .await?
            else {
                break;
            };
            self.charge_native_degree_records(scanned_records, 1)?;
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            let record = decode_edge_record(&key, &kv.value)?;
            adjacency.entry(record.src).or_default().push(record.dst);
        }

        let prefix = keys::out_segment_tombstone_edge_type_prefix(read.cell_id, edge_type);
        let mut iter = read
            .budget
            .read_only_io("native_degree_tombstone_scan", async {
                Ok::<_, GraphError>(
                    read.snapshot
                        .scan_prefix_with_options(prefix.as_bytes(), .., &remote_bulk_scan_options())
                        .await?,
                )
            })
            .await?;
        let mut tombstones = BTreeMap::new();
        loop {
            let Some(kv) = read
                .budget
                .read_only_io("native_degree_tombstone_scan", async { Ok::<_, GraphError>(iter.next().await?) })
                .await?
            else {
                break;
            };
            self.charge_native_degree_records(scanned_records, 1)?;
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            let (key_cell_id, key_edge_type, src, dst) = parse_out_edge_segment_tombstone_key(&key)?;
            if key_cell_id != read.cell_id || key_edge_type != edge_type {
                return Err(GraphError::CorruptValue {
                    key,
                    reason: "segment tombstone identity does not match scan prefix".to_string(),
                });
            }
            let epoch = decode_u64(&key, &kv.value)?;
            if epoch <= read.read_epoch {
                tombstones.insert((src, dst), epoch);
            }
        }

        let prefix = keys::out_segment_edge_type_prefix(read.cell_id, edge_type);
        let mut iter = read
            .budget
            .read_only_io("native_degree_segment_scan", async {
                Ok::<_, GraphError>(
                    read.snapshot
                        .scan_prefix_with_options(prefix.as_bytes(), .., &remote_bulk_scan_options())
                        .await?,
                )
            })
            .await?;
        loop {
            let Some(kv) = read
                .budget
                .read_only_io("native_degree_segment_scan", async { Ok::<_, GraphError>(iter.next().await?) })
                .await?
            else {
                break;
            };
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            let segment = decode_out_edge_segment(&key, &kv.value)?;
            self.charge_native_degree_records(scanned_records, segment.destinations.len().max(1) as u64)?;
            if segment.storage_sequence > read.read_epoch {
                continue;
            }
            for dst in segment.destinations.iter().copied() {
                if segment_edge_visible(segment.storage_sequence, tombstones.get(&(segment.src, dst)).copied()) {
                    adjacency.entry(segment.src).or_default().push(dst);
                }
            }
        }
        for destinations in adjacency.values_mut() {
            destinations.sort_unstable();
            destinations.dedup();
        }
        Ok(adjacency)
    }

    /// Rank by degree, then keep vertices with the label until `limit` rows.
    async fn native_degree_rows(
        &self,
        read: DegreeRead<'_>,
        procedure: &NativeDegreeProcedure,
        degrees: HashMap<VertexId, u64>,
        limit: u64,
    ) -> Result<Vec<QueryRow>> {
        let limit = usize::try_from(limit).unwrap_or(usize::MAX);
        let threshold = procedure.min_degree.max(1);
        let mut ranked = degrees
            .iter()
            .filter(|(_, degree)| **degree >= threshold)
            .map(|(vertex, degree)| (*vertex, *degree))
            .collect::<Vec<_>>();
        ranked.sort_unstable_by(|left, right| right.1.cmp(&left.1).then(left.0.cmp(&right.0)));

        let mut rows = Vec::new();
        let mut vertex_cache = BTreeMap::new();
        for batch in ranked.chunks(NATIVE_DEGREE_HYDRATION_BATCH_SIZE) {
            if rows.len() >= limit {
                break;
            }
            read.budget.check("native_degree_hydrate")?;
            let vertex_ids = batch.iter().map(|(vertex, _)| *vertex).collect::<Vec<_>>();
            self.hydrate_vertex_metadata_cache_at_concurrently(
                read.cell_id,
                &vertex_ids,
                read.read_epoch,
                &mut vertex_cache,
                read.budget,
                NATIVE_DEGREE_HYDRATION_CONCURRENCY,
            )
            .await?;
            for (vertex, degree) in batch {
                let metadata = vertex_cache.remove(vertex).expect("ranked vertex metadata was hydrated");
                if !metadata.labels.contains(&procedure.label) {
                    continue;
                }
                let row = native_degree_row(procedure, *vertex, *degree, &metadata);
                read.budget.account_result_row(&row)?;
                rows.push(row);
                if rows.len() >= limit {
                    break;
                }
            }
        }

        // `minDegree: 0` also asks for labelled vertices with no relationship
        // at all. They rank last, in vertex id order -- the label index order.
        if procedure.min_degree == 0 && rows.len() < limit {
            let prefix = keys::vertex_label_prefix(read.cell_id, &procedure.label);
            let mut iter = read
                .budget
                .read_only_io("native_degree_label_scan", async {
                    Ok::<_, GraphError>(
                        read.snapshot
                            .scan_prefix_with_options(prefix.as_bytes(), .., &remote_bulk_scan_options())
                            .await?,
                    )
                })
                .await?;
            let mut pending = Vec::new();
            loop {
                let next = read
                    .budget
                    .read_only_io("native_degree_label_scan", async { Ok::<_, GraphError>(iter.next().await?) })
                    .await?;
                if let Some(kv) = &next {
                    let key = String::from_utf8_lossy(&kv.key);
                    let vertex = parse_u64(&key, key.strip_prefix(&prefix).unwrap_or_default(), "vertex id")?;
                    if !degrees.contains_key(&vertex) {
                        pending.push(vertex);
                    }
                }
                if pending.len() < NATIVE_DEGREE_HYDRATION_BATCH_SIZE && next.is_some() {
                    continue;
                }
                self.hydrate_vertex_metadata_cache_at_concurrently(
                    read.cell_id,
                    &pending,
                    read.read_epoch,
                    &mut vertex_cache,
                    read.budget,
                    NATIVE_DEGREE_HYDRATION_CONCURRENCY,
                )
                .await?;
                for vertex in pending.drain(..) {
                    let metadata = vertex_cache.remove(&vertex).expect("label member metadata was hydrated");
                    if !metadata.labels.contains(&procedure.label) {
                        continue;
                    }
                    let row = native_degree_row(procedure, vertex, 0, &metadata);
                    read.budget.account_result_row(&row)?;
                    rows.push(row);
                    if rows.len() >= limit {
                        return Ok(rows);
                    }
                }
                if next.is_none() {
                    break;
                }
            }
        }
        Ok(rows)
    }

    fn charge_native_degree_records(&self, scanned: &mut u64, records: u64) -> Result<()> {
        *scanned = scanned.saturating_add(records);
        ensure_limit("native_degree_scan_edges", *scanned, self.limits.max_query_scan_edges)
    }

    fn ensure_native_degree_vertices(&self, vertices: usize) -> Result<()> {
        ensure_limit(
            "native_degree_vertices",
            vertices as u64,
            self.limits.max_query_intermediate_rows as u64,
        )
    }
}

fn native_degree_row(procedure: &NativeDegreeProcedure, vertex: VertexId, degree: u64, metadata: &VertexMetadata) -> QueryRow {
    QueryRow::new(
        procedure
            .projections
            .iter()
            .map(|projection| match &projection.value {
                NativeDegreeValue::Node => QueryValue::VertexId(vertex),
                // A Cypher projection of `n.id` is the vertex id.
                NativeDegreeValue::NodeProperty(property) if property == "id" => QueryValue::Property(VertexPropertyValue::Integer(vertex)),
                NativeDegreeValue::NodeProperty(property) => metadata
                    .properties
                    .get(property)
                    .cloned()
                    .map_or(QueryValue::Null, QueryValue::Property),
                NativeDegreeValue::Degree => QueryValue::Count(degree),
            })
            .collect(),
    )
}

/// `{src:020}/{dst:020}` after a relationship-count type prefix.
fn parse_relationship_count_suffix(key: &str, prefix: &str) -> Result<(VertexId, VertexId)> {
    let suffix = key.strip_prefix(prefix).unwrap_or_default();
    let Some((src, dst)) = suffix.split_once('/') else {
        return Err(GraphError::CorruptValue {
            key: key.to_string(),
            reason: "relationship count key has no source/destination pair".to_string(),
        });
    };
    Ok((parse_u64(key, src, "source vertex")?, parse_u64(key, dst, "destination vertex")?))
}
