use super::*;

impl GraphShard {
    pub(crate) async fn cached_matrix_adjacency(
        &self,
        cell_id: &str,
        edge_type: &str,
        base_epoch: StorageSequence,
    ) -> Result<Arc<MatrixAdjacency>> {
        let cache_key = MatrixCacheKey::new(cell_id, edge_type, base_epoch);
        if let Some(cached) = self.matrix_cache.lock().await.get(&cache_key) {
            self.cache_metrics
                .record_hit(GraphCacheKind::MatrixAdjacency);
            return Ok(cached);
        }

        self.cache_metrics
            .record_miss(GraphCacheKind::MatrixAdjacency);
        let _permit = self
            .acquire_hydration_permit("cached_matrix_adjacency")
            .await?;
        let artifact = self
            .latest_matrix_artifact(cell_id, edge_type, base_epoch)
            .await?
            .filter(|artifact| artifact.base_epoch == base_epoch)
            .ok_or_else(|| GraphError::CorruptValue {
                key: matrix_manifest_key(cell_id, edge_type, base_epoch),
                reason: "missing matrix artifact manifest".to_string(),
            })?;
        let adjacency = Arc::new(self.load_matrix_adjacency(&artifact).await?);
        self.record_hydration_complete();
        trim_process_memory_after_hydration();
        let mut cache = self.matrix_cache.lock().await;
        Ok(cache
            .insert_sized(
                cache_key,
                Arc::clone(&adjacency),
                cell_id.to_string(),
                adjacency_edge_count(adjacency.as_ref()) >= self.cache_policy.pin_matrix_min_edges,
                adjacency_resident_bytes(adjacency.as_ref()),
                &self.cache_metrics,
            )
            .unwrap_or(adjacency))
    }

    #[cfg(feature = "graphblas")]
    pub(crate) async fn cached_graphblas_matrix(
        &self,
        cell_id: &str,
        edge_type: &str,
        base_epoch: StorageSequence,
    ) -> Result<Arc<CompiledGraphBlasMatrix>> {
        let cache_key = MatrixCacheKey::new(cell_id, edge_type, base_epoch);
        if let Some(cached) = self.graphblas_cache.lock().await.get(&cache_key) {
            self.cache_metrics.record_hit(GraphCacheKind::GraphBlas);
            record_matrix_profile(
                matrix_profile_enabled(),
                "graphblas_cache_hit",
                Duration::ZERO,
                base_epoch,
            );
            return Ok(cached);
        }

        self.cache_metrics.record_miss(GraphCacheKind::GraphBlas);
        let _compile_permit = self
            .matrix_compilation_gate
            .clone()
            .acquire_owned()
            .await
            .map_err(|err| GraphError::CorruptValue {
                key: "cache/matrix_compilation".to_string(),
                reason: format!("matrix compilation gate closed: {err}"),
            })?;
        if let Some(cached) = self.graphblas_cache.lock().await.get(&cache_key) {
            self.cache_metrics.record_hit(GraphCacheKind::GraphBlas);
            return Ok(cached);
        }
        let started = Instant::now();
        let external = self
            .graph_index_generation_at(cell_id, edge_type, base_epoch)
            .await?;
        let (compiled, compile_units) = if let Some(generation) = external {
            let csc = self.graph_index_csc(&generation).await?;
            let edge_count = csc.indices.len() as u64;
            (Arc::new(compile_graphblas_csc_owned(csc)?), edge_count)
        } else if compact_csc_kernel_enabled() {
            if let Some((compiled, edge_count)) = self
                .compact_graphblas_csc_matrix(cell_id, edge_type, base_epoch)
                .await?
            {
                (Arc::new(compiled), edge_count)
            } else if let Some(csc) = self.graphblas_csc(cell_id, edge_type, base_epoch).await? {
                let edge_count = csc.indices.len() as u64;
                (Arc::new(compile_graphblas_csc_owned(csc)?), edge_count)
            } else {
                let adjacency = self
                    .cached_matrix_adjacency(cell_id, edge_type, base_epoch)
                    .await?;
                let edge_count = adjacency.values().map(|dsts| dsts.len() as u64).sum();
                (
                    Arc::new(compile_graphblas_matrix(adjacency.as_ref())?),
                    edge_count,
                )
            }
        } else if let Some(csc) = self.graphblas_csc(cell_id, edge_type, base_epoch).await? {
            let edge_count = csc.indices.len() as u64;
            (Arc::new(compile_graphblas_csc_owned(csc)?), edge_count)
        } else {
            let adjacency = self
                .cached_matrix_adjacency(cell_id, edge_type, base_epoch)
                .await?;
            let edge_count = adjacency.values().map(|dsts| dsts.len() as u64).sum();
            (
                Arc::new(compile_graphblas_matrix(adjacency.as_ref())?),
                edge_count,
            )
        };
        trim_process_memory_after_hydration();
        record_matrix_profile(
            matrix_profile_enabled(),
            "compile_graphblas_matrix",
            started.elapsed(),
            compile_units,
        );
        let mut cache = self.graphblas_cache.lock().await;
        Ok(cache
            .insert_sized(
                cache_key,
                Arc::clone(&compiled),
                cell_id.to_string(),
                compile_units >= self.cache_policy.pin_matrix_min_edges,
                compiled.estimated_resident_bytes(),
                &self.cache_metrics,
            )
            .unwrap_or(compiled))
    }

    #[cfg(feature = "graphblas")]
    async fn compact_graphblas_csc_matrix(
        &self,
        cell_id: &str,
        edge_type: &str,
        base_epoch: StorageSequence,
    ) -> Result<Option<(CompiledGraphBlasMatrix, u64)>> {
        let key = graphblas_csc_key(cell_id, edge_type, base_epoch);
        let _permit = self
            .acquire_hydration_permit("compact_graphblas_csc_matrix")
            .await?;
        let Some(value) = self.read_remote(&key).await? else {
            self.record_hydration_complete();
            return Ok(None);
        };
        if value.starts_with(GRAPHBLAS_CSC_MAGIC) {
            self.record_hydration_complete();
            return Ok(None);
        }

        let manifest = decode_graphblas_csc_manifest(&key, &value)?;
        if manifest.cell_id != cell_id
            || manifest.edge_type != edge_type
            || manifest.base_epoch != base_epoch
        {
            return corrupt(&key, "GraphBLAS CSC manifest identity does not match key");
        }

        let vertices = self
            .load_graphblas_csc_chunks(
                cell_id,
                edge_type,
                base_epoch,
                "vertices",
                manifest.vertex_chunks,
                manifest.vertices_len,
            )
            .await?;
        let pointers = self
            .load_graphblas_csc_chunks_u32(
                cell_id,
                edge_type,
                base_epoch,
                "pointers",
                manifest.pointer_chunks,
                manifest.pointers_len,
            )
            .await?;
        let indices = self
            .load_graphblas_csc_chunks_u32(
                cell_id,
                edge_type,
                base_epoch,
                "indices",
                manifest.index_chunks,
                manifest.indices_len,
            )
            .await?;
        if graphblas_csc_checksum_compact(&vertices, &pointers, &indices) != manifest.checksum {
            return corrupt(&key, "GraphBLAS CSC checksum mismatch");
        }
        let edge_count = indices.len() as u64;
        let compiled = compile_graphblas_compact_csc_u32(vertices, pointers, indices)?;
        self.record_hydration_complete();
        Ok(Some((compiled, edge_count)))
    }

    pub(crate) async fn graphblas_csc(
        &self,
        cell_id: &str,
        edge_type: &str,
        base_epoch: StorageSequence,
    ) -> Result<Option<GraphBlasCsc>> {
        let key = graphblas_csc_key(cell_id, edge_type, base_epoch);
        let _permit = self.acquire_hydration_permit("graphblas_csc").await?;
        let Some(value) = self.read_remote(&key).await? else {
            self.record_hydration_complete();
            return Ok(None);
        };
        if value.starts_with(GRAPHBLAS_CSC_MAGIC) {
            let csc = decode_graphblas_csc(&key, &value, cell_id, edge_type, base_epoch)?;
            self.record_hydration_complete();
            return Ok(Some(csc));
        }

        let manifest = decode_graphblas_csc_manifest(&key, &value)?;
        if manifest.cell_id != cell_id
            || manifest.edge_type != edge_type
            || manifest.base_epoch != base_epoch
        {
            return corrupt(&key, "GraphBLAS CSC manifest identity does not match key");
        }

        let vertices = self
            .load_graphblas_csc_chunks(
                cell_id,
                edge_type,
                base_epoch,
                "vertices",
                manifest.vertex_chunks,
                manifest.vertices_len,
            )
            .await?;
        let pointers = self
            .load_graphblas_csc_chunks(
                cell_id,
                edge_type,
                base_epoch,
                "pointers",
                manifest.pointer_chunks,
                manifest.pointers_len,
            )
            .await?;
        let indices = self
            .load_graphblas_csc_chunks(
                cell_id,
                edge_type,
                base_epoch,
                "indices",
                manifest.index_chunks,
                manifest.indices_len,
            )
            .await?;
        let csc = GraphBlasCsc {
            vertices,
            pointers,
            indices,
        };
        if graphblas_csc_checksum(&csc) != manifest.checksum {
            return corrupt(&key, "GraphBLAS CSC checksum mismatch");
        }
        validate_graphblas_csc_artifact(&key, &csc)?;
        self.record_hydration_complete();
        Ok(Some(csc))
    }

    async fn load_graphblas_csc_chunks(
        &self,
        cell_id: &str,
        edge_type: &str,
        base_epoch: StorageSequence,
        field: &'static str,
        chunk_count: u64,
        expected_len: u64,
    ) -> Result<Vec<u64>> {
        let mut values = Vec::new();
        for chunk_id in 0..chunk_count {
            let key = graphblas_csc_chunk_key(cell_id, edge_type, base_epoch, field, chunk_id);
            let Some(value) = self.read_remote(&key).await? else {
                return corrupt(&key, "missing GraphBLAS CSC chunk");
            };
            values.extend(decode_graphblas_csc_chunk(&key, &value, field, chunk_id)?);
        }
        if values.len() as u64 != expected_len {
            return corrupt(
                &graphblas_csc_key(cell_id, edge_type, base_epoch),
                format!(
                    "GraphBLAS CSC {field} length {} does not match manifest {expected_len}",
                    values.len()
                ),
            );
        }
        Ok(values)
    }

    #[cfg(feature = "graphblas")]
    async fn load_graphblas_csc_chunks_u32(
        &self,
        cell_id: &str,
        edge_type: &str,
        base_epoch: StorageSequence,
        field: &'static str,
        chunk_count: u64,
        expected_len: u64,
    ) -> Result<Vec<u32>> {
        let mut values = Vec::new();
        for chunk_id in 0..chunk_count {
            let key = graphblas_csc_chunk_key(cell_id, edge_type, base_epoch, field, chunk_id);
            let Some(value) = self.read_remote(&key).await? else {
                return corrupt(&key, "missing GraphBLAS CSC chunk");
            };
            values.extend(decode_graphblas_csc_chunk_u32(
                &key, &value, field, chunk_id,
            )?);
        }
        if values.len() as u64 != expected_len {
            return corrupt(
                &graphblas_csc_key(cell_id, edge_type, base_epoch),
                format!(
                    "GraphBLAS CSC {field} length {} does not match manifest {expected_len}",
                    values.len()
                ),
            );
        }
        Ok(values)
    }

    async fn load_matrix_adjacency(&self, artifact: &MatrixArtifact) -> Result<MatrixAdjacency> {
        if artifact.out_tiles == 0 {
            if let Some(canonical) = self
                .graphblas_csc(&artifact.cell_id, &artifact.edge_type, artifact.base_epoch)
                .await?
            {
                let adjacency = canonical.to_adjacency()?;
                let edge_count = adjacency_edge_count(&adjacency);
                if edge_count != artifact.edge_count {
                    return corrupt(
                        &matrix_manifest_key(
                            &artifact.cell_id,
                            &artifact.edge_type,
                            artifact.base_epoch,
                        ),
                        format!(
                            "canonical adjacency edge count {edge_count} does not match manifest {}",
                            artifact.edge_count
                        ),
                    );
                }
                return Ok(adjacency);
            }
        }
        let mut adjacency: MatrixAdjacency = BTreeMap::new();
        let prefix = matrix_tile_prefix(
            &artifact.cell_id,
            &artifact.edge_type,
            artifact.base_epoch,
            MatrixDirection::Out,
        );
        let mut iter = self.scan_remote_prefix(&prefix).await?;
        let mut tile_count = 0_u64;
        while let Some(kv) = iter.next().await? {
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            let tile = decode_matrix_tile(&key, &kv.value)?;
            if tile.cell_id != artifact.cell_id
                || tile.edge_type != artifact.edge_type
                || tile.base_epoch != artifact.base_epoch
                || tile.tile_size != artifact.tile_size
                || tile.direction != MatrixDirection::Out
            {
                return corrupt(&key, "matrix tile identity does not match manifest");
            }
            tile_count = tile_count.saturating_add(1);
            for (src, dsts) in tile.rows {
                adjacency.entry(src).or_default().extend(dsts);
            }
        }
        if tile_count != artifact.out_tiles {
            return corrupt(
                &matrix_manifest_key(&artifact.cell_id, &artifact.edge_type, artifact.base_epoch),
                format!(
                    "matrix tile count {tile_count} does not match manifest {}",
                    artifact.out_tiles
                ),
            );
        }
        let edge_count = adjacency_edge_count(&adjacency);
        if edge_count != artifact.edge_count {
            return corrupt(
                &matrix_manifest_key(&artifact.cell_id, &artifact.edge_type, artifact.base_epoch),
                format!(
                    "matrix edge count {edge_count} does not match manifest {}",
                    artifact.edge_count
                ),
            );
        }
        Ok(adjacency)
    }
}
