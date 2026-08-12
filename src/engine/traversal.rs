use super::*;
use crate::shard::QueryBudget;
use crate::sparse_kernel::FrontierWalk;

/// The one place a [`SparseTraversal`] becomes a [`MatrixTraversalResult`], and
/// therefore the one place `matrix_reachable`'s specification is stated.
///
/// That specification is [`FrontierWalk::FixedHops`]: vertices at BFS distance
/// `1..=hops`, start vertices excluded. Most branches below reach it through
/// `expand`, which strips its own start set. The overlay branch reaches it
/// through a `1..=hops` range walk, which does not — so a start vertex sitting
/// on a cycle came back from that branch and from no other, and one call
/// answered three different ways depending on how many start ids it was handed
/// and whether a matrix happened to be cached.
///
/// Normalising here rather than at each branch is the point: five call sites
/// each remembering to strip is five chances to forget, and the last four years
/// of this function are the evidence. For the branches that already strip, the
/// `retain` is a no-op costing one set lookup per result vertex.
fn matrix_traversal_result(
    backend: TraversalBackend,
    traversal: crate::sparse_kernel::SparseTraversal,
    starts: &[VertexId],
    hops: u8,
    base_epoch: StorageSequence,
) -> MatrixTraversalResult {
    let start_set: BTreeSet<VertexId> = starts.iter().copied().collect();
    let mut vertices = traversal.vertices;
    vertices.retain(|vertex| !start_set.contains(vertex));
    MatrixTraversalResult {
        backend,
        vertices,
        hops,
        base_epoch,
        edge_visits: traversal.edge_visits,
        sparse_kernel: traversal.backend,
    }
}

impl GraphShard {
    pub async fn matrix_reachable(
        &self,
        cell_id: &str,
        edge_type: &str,
        starts: &[VertexId],
        hops: u8,
        read_epoch: StorageSequence,
    ) -> Result<MatrixTraversalResult> {
        self.matrix_reachable_with_kernel(
            cell_id,
            edge_type,
            starts,
            hops,
            read_epoch,
            default_matrix_kernel(&self.cache_policy),
        )
        .await
    }

    pub async fn matrix_reachable_with_kernel(
        &self,
        cell_id: &str,
        edge_type: &str,
        starts: &[VertexId],
        hops: u8,
        read_epoch: StorageSequence,
        sparse_kernel: SparseKernelBackend,
    ) -> Result<MatrixTraversalResult> {
        validate_component("cell_id", cell_id)?;
        validate_component("edge_type", edge_type)?;
        ensure_limit(
            "matrix_reachable",
            u64::from(hops),
            u64::from(self.limits.max_traversal_hops),
        )?;
        // An `Adjacency` policy is a hard ceiling, not a default: it means this
        // shard does no matrix compilation at all. Without this clamp a caller
        // asking for a compiled rung would fall through to `expand_sparse` with
        // its own argument and compile a matrix anyway, reporting a rung the
        // policy had disabled.
        let sparse_kernel = match default_matrix_kernel(&self.cache_policy) {
            SparseKernelBackend::Adjacency => SparseKernelBackend::Adjacency,
            _ => sparse_kernel,
        };
        let profile = matrix_profile_enabled();
        let total_started = Instant::now();

        let started = Instant::now();
        let artifact = self
            .latest_matrix_artifact(cell_id, edge_type, read_epoch)
            .await?;
        record_matrix_profile(
            profile,
            "latest_matrix_artifact",
            started.elapsed(),
            artifact.as_ref().map_or(0, |artifact| artifact.base_epoch),
        );

        let base_epoch = artifact.as_ref().map_or(0, |artifact| artifact.base_epoch);

        // Both compiled rungs go through the compiled-matrix path; only kernel 1
        // skips it, because it has no compiled form.
        if sparse_kernel != SparseKernelBackend::Adjacency && artifact.is_some() {
            let started = Instant::now();
            let Some((compiled, overlay, _)) = self
                .compiled_graphblas_query_snapshot(
                    cell_id,
                    edge_type,
                    base_epoch,
                    read_epoch,
                    &QueryBudget::new(self.limits.max_query_runtime_ms, None),
                )
                .await?
            else {
                // One start walks outward from storage, touching only the
                // reachable subgraph; anything else materialises the whole
                // adjacency first. That is a cost decision and stays. What must
                // not vary with it is the *answer*, so the walk is asked for
                // fixed hops explicitly rather than being handed a `(1, hops)`
                // pair that reads as a range.
                let traversal = if let [start] = starts {
                    self.reachable_from_storage_frontier(
                        cell_id,
                        edge_type,
                        *start,
                        FrontierWalk::FixedHops(hops),
                        read_epoch,
                        &QueryBudget::new(self.limits.max_query_runtime_ms, None),
                    )
                    .await?
                } else {
                    let adjacency = self
                        .canonical_adjacency_at(cell_id, edge_type, read_epoch)
                        .await?;
                    expand_sparse(&adjacency, starts, hops, sparse_kernel)?
                };
                return Ok(matrix_traversal_result(
                    TraversalBackend::DirectSnapshot,
                    traversal,
                    starts,
                    hops,
                    base_epoch,
                ));
            };
            record_matrix_profile(profile, "cached_graphblas_matrix", started.elapsed(), 0);

            let started = Instant::now();
            let empty_adjacency = BTreeMap::new();
            let traversal = if let Some(overlay) = overlay {
                crate::shard::topology_tail::expand_range_with_overlay(
                    &compiled, &overlay, starts, 1, hops,
                )?
            } else {
                expand_compiled_graphblas(&compiled, &empty_adjacency, starts, hops)?
            };
            record_matrix_profile(
                profile,
                "expand_compiled_graphblas",
                started.elapsed(),
                traversal.vertices.len() as u64,
            );
            record_matrix_profile(
                profile,
                "matrix_reachable_total",
                total_started.elapsed(),
                0,
            );
            return Ok(matrix_traversal_result(
                TraversalBackend::MatrixSnapshot,
                traversal,
                starts,
                hops,
                base_epoch,
            ));
        }

        let started = Instant::now();
        let adjacency = self
            .canonical_adjacency_at(cell_id, edge_type, read_epoch)
            .await?;
        record_matrix_profile(
            profile,
            "canonical_adjacency",
            started.elapsed(),
            adjacency.len() as u64,
        );

        let traversal = expand_sparse(&adjacency, starts, hops, sparse_kernel)?;
        record_matrix_profile(
            profile,
            "matrix_reachable_total",
            total_started.elapsed(),
            0,
        );
        Ok(matrix_traversal_result(
            TraversalBackend::MatrixSnapshot,
            traversal,
            starts,
            hops,
            base_epoch,
        ))
    }

    pub async fn direct_snapshot_reachable(
        &self,
        cell_id: &str,
        edge_type: &str,
        starts: &[VertexId],
        hops: u8,
        read_epoch: StorageSequence,
    ) -> Result<MatrixTraversalResult> {
        validate_component("cell_id", cell_id)?;
        validate_component("edge_type", edge_type)?;
        ensure_limit(
            "direct_snapshot_reachable",
            u64::from(hops),
            u64::from(self.limits.max_traversal_hops),
        )?;
        let adjacency = self
            .canonical_adjacency_at(cell_id, edge_type, read_epoch)
            .await?;
        let traversal = expand_sparse(&adjacency, starts, hops, SparseKernelBackend::Adjacency)?;
        // `expand_sparse` already strips the start set, so this is a no-op —
        // routed through the same helper anyway so `benchmark_hot_hops` is
        // comparing two results built to one specification.
        Ok(matrix_traversal_result(
            TraversalBackend::DirectSnapshot,
            traversal,
            starts,
            hops,
            0,
        ))
    }

    pub async fn benchmark_hot_hops(
        &self,
        cell_id: &str,
        edge_type: &str,
        starts: &[VertexId],
        hops: u8,
        read_epoch: StorageSequence,
    ) -> Result<BenchmarkResult> {
        let direct = self
            .direct_snapshot_reachable(cell_id, edge_type, starts, hops, read_epoch)
            .await?;
        let matrix = self
            .matrix_reachable(cell_id, edge_type, starts, hops, read_epoch)
            .await?;
        Ok(BenchmarkResult {
            matrix_wins: matrix.vertices == direct.vertices
                && matrix.edge_visits < direct.edge_visits,
            direct,
            matrix,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sparse_kernel::{SparseKernelBackend, SparseTraversal};

    fn traversal(vertices: Vec<VertexId>) -> SparseTraversal {
        SparseTraversal {
            vertices,
            edge_visits: 7,
            backend: SparseKernelBackend::Adjacency,
        }
    }

    /// The specification, stated as a test: a start vertex never appears in its
    /// own reachability answer, however it was reached.
    ///
    /// The range-backed branches return start vertices that sit on a cycle; the
    /// `expand`-backed ones never do. This is the single point that reconciles
    /// them, so it is the single point worth pinning.
    #[test]
    fn a_start_vertex_reached_around_a_cycle_is_stripped_from_the_result() {
        let result = matrix_traversal_result(
            TraversalBackend::MatrixSnapshot,
            traversal(vec![1, 2, 3]),
            &[1],
            2,
            0,
        );
        assert_eq!(result.vertices, vec![2, 3]);
    }

    /// Every start is stripped, not just the first, and a start absent from the
    /// graph strips nothing. Both matter because the arity of `starts` used to
    /// select the branch, and therefore the answer.
    #[test]
    fn stripping_covers_every_start_and_tolerates_unknown_ones() {
        let result = matrix_traversal_result(
            TraversalBackend::DirectSnapshot,
            traversal(vec![1, 2, 3, 4]),
            &[1, 3, 999],
            3,
            0,
        );
        assert_eq!(result.vertices, vec![2, 4]);
    }

    /// Idempotent, so routing the branches that already strip through it is
    /// free of behaviour change — which is what makes it safe to apply to all
    /// five rather than only the two that were wrong.
    #[test]
    fn normalising_an_already_stripped_result_changes_nothing() {
        let already = traversal(vec![2, 3]);
        let result = matrix_traversal_result(
            TraversalBackend::MatrixSnapshot,
            already.clone(),
            &[1],
            2,
            9,
        );
        assert_eq!(result.vertices, already.vertices);
        assert_eq!(result.edge_visits, already.edge_visits);
        assert_eq!(result.sparse_kernel, already.backend);
        assert_eq!(result.base_epoch, 9);
        assert_eq!(result.hops, 2);
    }
}
