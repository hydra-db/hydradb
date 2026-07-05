use super::*;

impl GraphShard {
    pub async fn build_posting_chunks(
        &self,
        cell_id: &str,
        edge_type: &str,
        base_epoch: GraphEpoch,
        chunk_size: usize,
    ) -> Result<Vec<PostingChunk>> {
        validate_component("cell_id", cell_id)?;
        validate_component("edge_type", edge_type)?;
        self.ensure_write_authority(cell_id, "build_posting_chunks")?;
        if chunk_size == 0 {
            return Err(GraphError::CorruptValue {
                key: "posting_chunk_size".to_string(),
                reason: "chunk size must be greater than zero".to_string(),
            });
        }
        ensure_limit(
            "build_posting_chunks",
            base_epoch,
            self.limits.max_artifact_source_epochs,
        )?;

        let _permit = self
            .acquire_artifact_build_permit("build_posting_chunks")
            .await?;
        let started = Instant::now();
        let edges = self.edges_at(cell_id, edge_type, base_epoch).await?;
        ensure_limit(
            "build_posting_chunks_edges",
            edges.len() as u64,
            self.limits.max_artifact_build_edges,
        )?;
        let mut by_out: BTreeMap<VertexId, Vec<VertexId>> = BTreeMap::new();
        let mut by_in: BTreeMap<VertexId, Vec<VertexId>> = BTreeMap::new();
        for edge in edges {
            by_out.entry(edge.src).or_default().push(edge.dst);
            by_in.entry(edge.dst).or_default().push(edge.src);
        }

        let mut chunks = Vec::new();
        append_posting_chunks(
            cell_id,
            edge_type,
            base_epoch,
            ArtifactDirection::Out,
            chunk_size,
            by_out,
            &mut chunks,
        );
        append_posting_chunks(
            cell_id,
            edge_type,
            base_epoch,
            ArtifactDirection::In,
            chunk_size,
            by_in,
            &mut chunks,
        );

        let mut batch = GraphWriteBatch::new();
        let mut pending_writes = 0_usize;
        for chunk in &chunks {
            put_artifact_record(
                self,
                cell_id,
                "build_posting_chunks",
                &mut batch,
                &mut pending_writes,
                posting_key(chunk),
                encode_posting_chunk(chunk),
            )
            .await?;
        }
        flush_artifact_put_batch(
            self,
            cell_id,
            "build_posting_chunks",
            &mut batch,
            &mut pending_writes,
        )
        .await?;
        if !chunks.is_empty() {
            let mut cache = self.posting_chunk_cache.lock().await;
            for chunk in &chunks {
                cache.insert(
                    PostingChunkCacheKey::from_chunk(chunk),
                    chunk.clone(),
                    chunk.cell_id.clone(),
                    false,
                    &self.cache_metrics,
                );
            }
        }
        self.record_artifact_build_completed(started.elapsed());
        Ok(chunks)
    }

    pub async fn posting_chunks(
        &self,
        cell_id: &str,
        edge_type: &str,
        direction: ArtifactDirection,
        owner: VertexId,
        base_epoch: GraphEpoch,
    ) -> Result<Vec<PostingChunk>> {
        validate_component("cell_id", cell_id)?;
        validate_component("edge_type", edge_type)?;
        let prefix = posting_prefix(cell_id, edge_type, direction, owner, base_epoch);
        let mut iter = self.scan_remote_prefix(&prefix).await?;
        let mut chunks = Vec::new();
        while let Some(kv) = iter.next().await? {
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            chunks.push(decode_posting_chunk(&key, &kv.value)?);
        }
        chunks.sort_by_key(|chunk| chunk.chunk_id);
        if let Some(group) = self
            .supernode_group(cell_id, edge_type, direction, owner, base_epoch)
            .await?
            .filter(|group| group.base_epoch == base_epoch)
        {
            chunks.retain(|chunk| chunk.chunk_id < group.chunk_count);
        }
        Ok(chunks)
    }

    pub async fn build_matrix_tiles(
        &self,
        cell_id: &str,
        edge_type: &str,
        base_epoch: GraphEpoch,
        tile_size: u64,
    ) -> Result<MatrixArtifact> {
        validate_component("cell_id", cell_id)?;
        validate_component("edge_type", edge_type)?;
        self.ensure_write_authority(cell_id, "build_matrix_tiles")?;
        if tile_size == 0 {
            return Err(GraphError::CorruptValue {
                key: "matrix_tile_size".to_string(),
                reason: "tile size must be greater than zero".to_string(),
            });
        }
        ensure_limit(
            "build_matrix_tiles",
            base_epoch,
            self.limits.max_artifact_source_epochs,
        )?;

        let _permit = self
            .acquire_artifact_build_permit("build_matrix_tiles")
            .await?;
        let started = Instant::now();
        let edges = self.edges_at(cell_id, edge_type, base_epoch).await?;
        ensure_limit(
            "build_matrix_tiles_edges",
            edges.len() as u64,
            self.limits.max_artifact_build_edges,
        )?;
        let out_tiles = matrix_tiles_from_edges(
            cell_id,
            edge_type,
            base_epoch,
            tile_size,
            ArtifactDirection::Out,
            &edges,
        );
        let transpose_tiles = matrix_tiles_from_edges(
            cell_id,
            edge_type,
            base_epoch,
            tile_size,
            ArtifactDirection::In,
            &edges,
        );
        let adjacency = Arc::new(adjacency_from_edges(&edges));
        let graphblas_csc = graphblas_csc_from_adjacency(adjacency.as_ref())?;
        let artifact = MatrixArtifact {
            cell_id: cell_id.to_string(),
            edge_type: edge_type.to_string(),
            base_epoch,
            tile_size,
            out_tiles: out_tiles.len() as u64,
            transpose_tiles: transpose_tiles.len() as u64,
            edge_count: edges.len() as u64,
        };

        let mut data_batch = GraphWriteBatch::new();
        let mut pending_writes = 0_usize;
        for tile in out_tiles.iter().chain(transpose_tiles.iter()) {
            put_artifact_record(
                self,
                cell_id,
                "build_matrix_tiles",
                &mut data_batch,
                &mut pending_writes,
                matrix_tile_key(tile),
                encode_matrix_tile(tile),
            )
            .await?;
        }
        let graphblas_manifest = append_graphblas_csc_chunks(
            self,
            &mut data_batch,
            &mut pending_writes,
            cell_id,
            edge_type,
            base_epoch,
            &graphblas_csc,
        )
        .await?;
        flush_artifact_put_batch(
            self,
            cell_id,
            "build_matrix_tiles",
            &mut data_batch,
            &mut pending_writes,
        )
        .await?;

        let mut manifest_batch = GraphWriteBatch::new();
        manifest_batch.put(
            matrix_manifest_key(cell_id, edge_type, base_epoch),
            encode_matrix_artifact(&artifact),
        );
        manifest_batch.put(
            graphblas_csc_key(cell_id, edge_type, base_epoch),
            encode_graphblas_csc_manifest(&graphblas_manifest),
        );
        self.write_graph_batch_strict(cell_id, "build_matrix_tiles", manifest_batch)
            .await?;
        let cache_key = MatrixCacheKey::new(cell_id, edge_type, base_epoch);
        let pinned = self.cache_policy.pin_matrix_artifact(&artifact);
        let tenant = cell_id.to_string();
        self.matrix_artifact_cache.lock().await.insert(
            cache_key.clone(),
            artifact.clone(),
            tenant.clone(),
            pinned,
            &self.cache_metrics,
        );
        self.matrix_cache.lock().await.insert(
            cache_key.clone(),
            Arc::clone(&adjacency),
            tenant.clone(),
            pinned,
            &self.cache_metrics,
        );

        #[cfg(feature = "graphblas")]
        {
            let started = Instant::now();
            let compiled = Arc::new(compile_graphblas_csc(&graphblas_csc)?);
            phase0_matrix_profile(
                phase0_matrix_profile_enabled(),
                "build_matrix_tiles_precompile_graphblas",
                started.elapsed(),
                edges.len() as u64,
            );
            if let Some(prewarm_start) = adjacency
                .iter()
                .filter(|(_, dsts)| !dsts.is_empty())
                .min_by_key(|(_, dsts)| dsts.len())
                .map(|(src, _)| *src)
            {
                let started = Instant::now();
                let prewarm =
                    expand_compiled_graphblas(&compiled, adjacency.as_ref(), &[prewarm_start], 1)?;
                phase0_matrix_profile(
                    phase0_matrix_profile_enabled(),
                    "build_matrix_tiles_prewarm_graphblas",
                    started.elapsed(),
                    prewarm.vertices.len() as u64,
                );
            }
            self.graphblas_cache.lock().await.insert(
                cache_key,
                compiled,
                tenant,
                pinned,
                &self.cache_metrics,
            );
        }
        self.record_artifact_build_completed(started.elapsed());
        Ok(artifact)
    }

    pub async fn latest_matrix_artifact(
        &self,
        cell_id: &str,
        edge_type: &str,
        read_epoch: GraphEpoch,
    ) -> Result<Option<MatrixArtifact>> {
        validate_component("cell_id", cell_id)?;
        validate_component("edge_type", edge_type)?;
        if let Some(cached) = self.matrix_artifact_cache.lock().await.get_latest_by(
            |key, _| {
                key.cell_id == cell_id && key.edge_type == edge_type && key.base_epoch <= read_epoch
            },
            |key, _| key.base_epoch,
        ) {
            self.cache_metrics
                .record_hit(GraphCacheKind::MatrixArtifact);
            return Ok(Some(cached));
        }

        self.cache_metrics
            .record_miss(GraphCacheKind::MatrixArtifact);
        let _permit = self
            .acquire_hydration_permit("latest_matrix_artifact")
            .await?;
        let prefix = matrix_manifest_prefix(cell_id, edge_type);
        let mut iter = self.scan_remote_prefix(&prefix).await?;
        let mut latest = None;
        let mut decoded = Vec::new();
        while let Some(kv) = iter.next().await? {
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            let artifact = decode_matrix_artifact(&key, &kv.value)?;
            decoded.push(artifact.clone());
            if artifact.base_epoch <= read_epoch
                && latest
                    .as_ref()
                    .is_none_or(|current: &MatrixArtifact| artifact.base_epoch > current.base_epoch)
            {
                latest = Some(artifact);
            }
        }
        self.record_hydration_complete();
        if !decoded.is_empty() {
            let mut cache = self.matrix_artifact_cache.lock().await;
            for artifact in decoded {
                let pinned = self.cache_policy.pin_matrix_artifact(&artifact);
                cache.insert(
                    MatrixCacheKey::new(
                        &artifact.cell_id,
                        &artifact.edge_type,
                        artifact.base_epoch,
                    ),
                    artifact,
                    cell_id.to_string(),
                    pinned,
                    &self.cache_metrics,
                );
            }
        }
        Ok(latest)
    }
}
