use super::*;

impl GraphShard {
    pub async fn matrix_reachable(
        &self,
        cell_id: &str,
        edge_type: &str,
        starts: &[VertexId],
        hops: u8,
        read_epoch: GraphEpoch,
    ) -> Result<MatrixTraversalResult> {
        self.matrix_reachable_with_kernel(
            cell_id,
            edge_type,
            starts,
            hops,
            read_epoch,
            default_matrix_kernel(),
        )
        .await
    }

    pub async fn matrix_reachable_with_kernel(
        &self,
        cell_id: &str,
        edge_type: &str,
        starts: &[VertexId],
        hops: u8,
        read_epoch: GraphEpoch,
        sparse_kernel: SparseKernelBackend,
    ) -> Result<MatrixTraversalResult> {
        validate_component("cell_id", cell_id)?;
        validate_component("edge_type", edge_type)?;
        ensure_limit(
            "matrix_reachable",
            u64::from(hops),
            u64::from(self.limits.max_traversal_hops),
        )?;
        if hops == 1 {
            if let Some(result) = self
                .supernode_one_hop_reachable(cell_id, edge_type, starts, read_epoch, sparse_kernel)
                .await?
            {
                return Ok(result);
            }
        }
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
        let started = Instant::now();
        let deltas = if read_epoch <= base_epoch {
            Vec::new()
        } else {
            self.deltas_between(cell_id, edge_type, base_epoch, read_epoch)
                .await?
        };
        record_matrix_profile(
            profile,
            "deltas_since",
            started.elapsed(),
            deltas.len() as u64,
        );

        if sparse_kernel == SparseKernelBackend::SuiteSparseGraphBlas
            && deltas.is_empty()
            && artifact.is_some()
        {
            let started = Instant::now();
            let compiled = self
                .cached_graphblas_matrix(cell_id, edge_type, base_epoch)
                .await?;
            record_matrix_profile(profile, "cached_graphblas_matrix", started.elapsed(), 0);

            let started = Instant::now();
            let empty_adjacency = BTreeMap::new();
            let traversal = expand_compiled_graphblas(&compiled, &empty_adjacency, starts, hops)?;
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
                backend: TraversalBackend::MatrixOverlay,
                vertices: traversal.vertices,
                hops,
                base_epoch,
                edge_visits: traversal.edge_visits,
                delta_records_applied: 0,
                sparse_kernel: traversal.backend,
            });
        }

        let started = Instant::now();
        let base_adjacency = if let Some(artifact) = artifact.as_ref() {
            self.cached_matrix_adjacency(cell_id, edge_type, artifact.base_epoch)
                .await?
        } else {
            Arc::new(BTreeMap::new())
        };
        record_matrix_profile(
            profile,
            "cached_matrix_adjacency",
            started.elapsed(),
            base_adjacency.len() as u64,
        );

        let mut adjacency = base_adjacency.as_ref().clone();
        let applied = apply_delta_overlay(&mut adjacency, deltas, base_epoch, read_epoch);
        let traversal = expand_sparse(&adjacency, starts, hops, sparse_kernel)?;
        record_matrix_profile(
            profile,
            "matrix_reachable_total",
            total_started.elapsed(),
            0,
        );
        Ok(MatrixTraversalResult {
            backend: TraversalBackend::MatrixOverlay,
            vertices: traversal.vertices,
            hops,
            base_epoch,
            edge_visits: traversal.edge_visits,
            delta_records_applied: applied,
            sparse_kernel: traversal.backend,
        })
    }

    pub async fn posting_reachable(
        &self,
        cell_id: &str,
        edge_type: &str,
        starts: &[VertexId],
        hops: u8,
        read_epoch: GraphEpoch,
    ) -> Result<MatrixTraversalResult> {
        validate_component("cell_id", cell_id)?;
        validate_component("edge_type", edge_type)?;
        ensure_limit(
            "posting_reachable",
            u64::from(hops),
            u64::from(self.limits.max_traversal_hops),
        )?;
        let watermark = self.delta_gc_watermark(cell_id, edge_type).await?;
        let (adjacency, applied) = if watermark == 0 {
            let deltas = self
                .deltas_between(cell_id, edge_type, 0, read_epoch)
                .await?;
            let mut adjacency = BTreeMap::new();
            let applied = apply_delta_overlay(&mut adjacency, deltas, 0, read_epoch);
            (adjacency, applied)
        } else {
            let edges = self.edges_at(cell_id, edge_type, read_epoch).await?;
            (adjacency_from_edges(&edges), 0)
        };
        let traversal = expand_sparse(&adjacency, starts, hops, SparseKernelBackend::RustSparse)?;
        Ok(MatrixTraversalResult {
            backend: TraversalBackend::PostingExpansion,
            vertices: traversal.vertices,
            hops,
            base_epoch: 0,
            edge_visits: traversal.edge_visits,
            delta_records_applied: applied,
            sparse_kernel: traversal.backend,
        })
    }

    pub async fn benchmark_hot_hops(
        &self,
        cell_id: &str,
        edge_type: &str,
        starts: &[VertexId],
        hops: u8,
        read_epoch: GraphEpoch,
    ) -> Result<BenchmarkResult> {
        let posting = self
            .posting_reachable(cell_id, edge_type, starts, hops, read_epoch)
            .await?;
        let matrix = self
            .matrix_reachable(cell_id, edge_type, starts, hops, read_epoch)
            .await?;
        Ok(BenchmarkResult {
            matrix_wins: matrix.vertices == posting.vertices
                && (matrix.edge_visits < posting.edge_visits
                    || matrix.delta_records_applied < posting.delta_records_applied),
            posting,
            matrix,
        })
    }
}
