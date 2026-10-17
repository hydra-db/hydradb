use super::*;
use crate::shard::QueryBudget;

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
                // Every branch of this function must answer the same question:
                // vertices reachable within `1..=hops` steps, starts excluded
                // (what `expand_sparse`/`expand_compiled_graphblas` below
                // compute). A single-start carve-out used to route here through
                // `reachable_from_storage_frontier` instead, which keeps a
                // start that's reachable via a cycle — a different answer that
                // depended only on `starts.len()`. Always go through the
                // adjacency path so the answer is invariant in start-set arity.
                let adjacency = self
                    .canonical_adjacency_at(cell_id, edge_type, read_epoch)
                    .await?;
                let traversal = expand_sparse(&adjacency, starts, hops, sparse_kernel)?;
                return Ok(MatrixTraversalResult {
                    backend: TraversalBackend::DirectSnapshot,
                    vertices: traversal.vertices,
                    hops,
                    base_epoch,
                    edge_visits: traversal.edge_visits,
                    sparse_kernel: traversal.backend,
                });
            };
            record_matrix_profile(profile, "cached_graphblas_matrix", started.elapsed(), 0);

            let started = Instant::now();
            let empty_adjacency = BTreeMap::new();
            let traversal = if let Some(overlay) = overlay {
                // `expand_compiled_graphblas` below answers "reachable within
                // `1..=hops` steps, starts excluded" — it seeds its `seen` set
                // with `starts` and filters them back out at the end, even if
                // a start is revisited through a cycle. `expand_range_with_overlay`
                // has no such start filter, so a start reachable through a
                // cycle would stay in its result. Keep the same `1..=hops`
                // range (so every level still contributes) and filter starts
                // out here, so the two branches agree regardless of whether a
                // compiled matrix happens to have a WAL overlay.
                let mut ranged = crate::shard::topology_tail::expand_range_with_overlay(
                    &compiled, &overlay, starts, 1, hops,
                )?;
                ranged.vertices.retain(|vertex| !starts.contains(vertex));
                ranged
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
            return Ok(MatrixTraversalResult {
                backend: TraversalBackend::MatrixSnapshot,
                vertices: traversal.vertices,
                hops,
                base_epoch,
                edge_visits: traversal.edge_visits,
                sparse_kernel: traversal.backend,
            });
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
        Ok(MatrixTraversalResult {
            backend: TraversalBackend::MatrixSnapshot,
            vertices: traversal.vertices,
            hops,
            base_epoch,
            edge_visits: traversal.edge_visits,
            sparse_kernel: traversal.backend,
        })
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
        Ok(MatrixTraversalResult {
            backend: TraversalBackend::DirectSnapshot,
            vertices: traversal.vertices,
            hops,
            base_epoch: 0,
            edge_visits: traversal.edge_visits,
            sparse_kernel: traversal.backend,
        })
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
