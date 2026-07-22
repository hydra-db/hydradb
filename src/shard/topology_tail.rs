use super::*;
use slatedb::ValueDeletable;

#[derive(Clone, Debug, Default)]
pub(crate) struct GraphTopologyOverlay {
    states: BTreeMap<VertexId, BTreeMap<VertexId, bool>>,
}

pub(crate) enum GraphTopologyTail {
    Complete(GraphTopologyOverlay),
    Unavailable,
}

impl GraphTopologyOverlay {
    fn set(&mut self, src: VertexId, dst: VertexId, exists: bool) {
        self.states.entry(src).or_default().insert(dst, exists);
    }

    fn state(&self, src: VertexId, dst: VertexId) -> Option<bool> {
        self.states
            .get(&src)
            .and_then(|destinations| destinations.get(&dst))
            .copied()
    }

    /// Test-only introspection. The WAL-tail overlay is otherwise only
    /// observable through a compiled GraphBLAS traversal, which needs the
    /// SuiteSparse C kernel; these let a repro assert on the overlay directly.
    #[cfg(test)]
    pub(crate) fn test_state(&self, src: VertexId, dst: VertexId) -> Option<bool> {
        self.state(src, dst)
    }

    #[cfg(test)]
    pub(crate) fn test_len(&self) -> usize {
        self.states.values().map(BTreeMap::len).sum()
    }
}

impl GraphShard {
    pub(crate) async fn topology_tail_since(
        &self,
        generation: &crate::GraphIndexGeneration,
        snapshot: &GraphStorageSnapshot,
        read_sequence: StorageSequence,
        budget: &QueryBudget,
    ) -> Result<GraphTopologyTail> {
        if snapshot.seq() != read_sequence {
            return Ok(GraphTopologyTail::Unavailable);
        }
        if generation.base_sequence >= read_sequence {
            return Ok(GraphTopologyTail::Complete(GraphTopologyOverlay::default()));
        }
        let last_wal_id = self.db.last_durable_wal_id().await?;
        if generation.last_wal_id >= last_wal_id {
            return Ok(GraphTopologyTail::Complete(GraphTopologyOverlay::default()));
        }

        let mut affected = BTreeSet::new();
        let wal_reader = self.db.wal_reader();
        for wal_id in generation.last_wal_id.saturating_add(1)..=last_wal_id {
            budget.check("graph_index_wal_file")?;
            let mut entries = match wal_reader.get(wal_id).iterator().await {
                Ok(entries) => entries,
                Err(error) => {
                    tracing::debug!(
                        wal_id,
                        error = %error,
                        "graph index WAL tail is unavailable; using snapshot adjacency"
                    );
                    return Ok(GraphTopologyTail::Unavailable);
                }
            };
            while let Some(entry) = entries.next().await? {
                budget.check("graph_index_wal_entry")?;
                if entry.seq <= generation.base_sequence || entry.seq > read_sequence {
                    continue;
                }
                collect_topology_entry(
                    &entry.key,
                    &entry.value,
                    &generation.cell_id,
                    &generation.edge_type,
                    &mut affected,
                )?;
                ensure_limit(
                    "graph_index_wal_affected_edges",
                    affected.len() as u64,
                    self.limits.max_query_scan_edges,
                )?;
            }
        }

        let mut overlay = GraphTopologyOverlay::default();
        for (src, dst) in affected {
            budget.check("graph_index_wal_resolve_edge")?;
            let exists = self
                .edge_exists_in_storage_snapshot(
                    snapshot,
                    &generation.cell_id,
                    &generation.edge_type,
                    src,
                    dst,
                    read_sequence,
                )
                .await?;
            overlay.set(src, dst, exists);
        }
        Ok(GraphTopologyTail::Complete(overlay))
    }
}

fn collect_topology_entry(
    key: &[u8],
    value: &ValueDeletable,
    cell_id: &str,
    edge_type: &str,
    affected: &mut BTreeSet<(VertexId, VertexId)>,
) -> Result<()> {
    let Ok(key) = std::str::from_utf8(key) else {
        return Ok(());
    };
    let fields = key.split('/').collect::<Vec<_>>();
    match fields.as_slice() {
        ["cell", key_cell, "e", "out", key_type, src, dst]
            if *key_cell == cell_id && *key_type == edge_type =>
        {
            affected.insert((parse_u64(key, src, "src")?, parse_u64(key, dst, "dst")?));
        }
        ["cell", key_cell, "seg", "tomb", "out", key_type, src, dst]
            if *key_cell == cell_id && *key_type == edge_type =>
        {
            affected.insert((parse_u64(key, src, "src")?, parse_u64(key, dst, "dst")?));
        }
        ["cell", key_cell, "seg", "out", key_type, _, _, _]
            if *key_cell == cell_id && *key_type == edge_type =>
        {
            if let Some(value) = value.as_bytes() {
                let segment = decode_out_edge_segment(key, &value)?;
                affected.extend(
                    segment
                        .destinations
                        .into_iter()
                        .map(|dst| (segment.src, dst)),
                );
            }
        }
        _ => {}
    }
    Ok(())
}

pub(crate) fn expand_range_with_overlay(
    compiled: &crate::sparse_kernel::CompiledGraphBlasMatrix,
    overlay: &GraphTopologyOverlay,
    starts: &[VertexId],
    min_hops: u8,
    max_hops: u8,
) -> Result<crate::sparse_kernel::SparseTraversal> {
    let mut frontier = starts.iter().copied().collect::<BTreeSet<_>>();
    let mut reachable = BTreeSet::new();
    let mut edge_visits = 0_u64;
    let empty = BTreeMap::new();

    for hop in 1..=max_hops {
        if frontier.is_empty() {
            break;
        }
        let frontier_values = frontier.iter().copied().collect::<Vec<_>>();
        let base =
            crate::sparse_kernel::expand_compiled_graphblas(compiled, &empty, &frontier_values, 1)?;
        edge_visits = edge_visits.saturating_add(base.edge_visits);
        let mut next = base.vertices.into_iter().collect::<BTreeSet<_>>();

        for src in &frontier {
            let Some(changes) = overlay.states.get(src) else {
                continue;
            };
            edge_visits = edge_visits.saturating_add(changes.len() as u64);
            for (dst, exists) in changes {
                if *exists {
                    next.insert(*dst);
                    continue;
                }
                if next.contains(dst)
                    && !frontier.iter().any(|candidate| {
                        overlay.state(*candidate, *dst).unwrap_or_else(|| {
                            crate::sparse_kernel::compiled_graphblas_contains_edge(
                                compiled, *candidate, *dst,
                            )
                        })
                    })
                {
                    next.remove(dst);
                }
            }
        }

        if hop >= min_hops {
            reachable.extend(next.iter().copied());
        }
        frontier = next;
    }

    Ok(crate::sparse_kernel::SparseTraversal {
        vertices: reachable.into_iter().collect(),
        edge_visits,
        backend: SparseKernelBackend::SuiteSparseGraphBlas,
    })
}
