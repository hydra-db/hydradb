use super::*;
use crate::keys;

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
        self.validate_write_fence(cell_id, "build_posting_chunks")
            .await?;
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

        let manifests = posting_manifests_from_chunks(&chunks)?;
        let artifact_manifest = posting_artifact_manifest_from_owner_manifests(
            cell_id, edge_type, base_epoch, &manifests,
        )?;

        let artifact_lock = self
            .acquire_posting_artifact_write_lock(
                cell_id,
                edge_type,
                base_epoch,
                "build_posting_chunks",
            )
            .await?;
        let publish_result = async {
            prepare_artifact_build(
                self,
                cell_id,
                "prepare_posting_artifact_build",
                &[posting_cleanup_marker_key(cell_id, edge_type, base_epoch)],
            )
            .await?;
            ensure_posting_artifact_publish_compatible(
                self,
                cell_id,
                edge_type,
                base_epoch,
                artifact_manifest.as_ref(),
                &manifests,
            )
            .await?;
            let mut batch = GraphWriteBatch::new();
            let mut pending_writes = 0_usize;
            for chunk in &chunks {
                put_artifact_record(
                    self,
                    &[&artifact_lock],
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
                &[&artifact_lock],
                cell_id,
                "build_posting_chunks",
                &mut batch,
                &mut pending_writes,
            )
            .await?;
            artifact_lock.renew().await?;
            for manifest in &manifests {
                put_artifact_record(
                    self,
                    &[&artifact_lock],
                    cell_id,
                    "build_posting_chunks_manifest",
                    &mut batch,
                    &mut pending_writes,
                    posting_manifest_key(
                        &manifest.cell_id,
                        &manifest.edge_type,
                        manifest.direction,
                        manifest.owner,
                        manifest.base_epoch,
                    ),
                    encode_posting_manifest(manifest),
                )
                .await?;
            }
            flush_artifact_put_batch(
                self,
                &[&artifact_lock],
                cell_id,
                "build_posting_chunks_manifest",
                &mut batch,
                &mut pending_writes,
            )
            .await?;
            artifact_lock.renew().await?;
            if let Some(artifact_manifest) = artifact_manifest.as_ref() {
                batch.put(
                    posting_artifact_manifest_key(cell_id, edge_type, base_epoch),
                    encode_posting_artifact_manifest(artifact_manifest),
                );
                publish_artifact_records_guarded(
                    self,
                    cell_id,
                    "build_posting_chunks_epoch_manifest",
                    &[posting_cleanup_marker_key(cell_id, edge_type, base_epoch)],
                    batch,
                )
                .await?;
            }
            artifact_lock.renew().await
        }
        .await;
        if let Err(err) = crate::release_cell_write_lock(artifact_lock, publish_result).await {
            cleanup_unpublished_posting_artifact_epoch(
                self,
                cell_id,
                edge_type,
                base_epoch,
                "build_posting_chunks_abort",
            )
            .await;
            return Err(err);
        }
        if !chunks.is_empty() {
            let mut cache = self.posting_chunk_cache.lock().await;
            for chunk in &chunks {
                cache.insert_sized(
                    PostingChunkCacheKey::from_chunk(chunk),
                    chunk.clone(),
                    chunk.cell_id.clone(),
                    false,
                    posting_chunk_resident_bytes(chunk),
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
        let mut chunks_by_id = BTreeMap::new();
        while let Some(kv) = iter.next().await? {
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            let chunk = decode_posting_chunk(&key, &kv.value)?;
            chunks_by_id.insert(chunk.chunk_id, chunk);
        }

        let expected = if let Some(group) = self
            .supernode_group(cell_id, edge_type, direction, owner, base_epoch)
            .await?
            .filter(|group| group.base_epoch == base_epoch)
        {
            Some(PostingChunkManifest {
                cell_id: group.cell_id,
                edge_type: group.edge_type,
                direction: group.direction,
                owner: group.vertex_id,
                base_epoch: group.base_epoch,
                chunk_count: group.chunk_count,
                vertex_count: group.degree,
                chunk_checksums: Vec::new(),
                chunk_bounds: group.chunk_bounds,
            })
        } else {
            let artifact_manifest_key =
                posting_artifact_manifest_key(cell_id, edge_type, base_epoch);
            if let Some(value) = self.read_remote(&artifact_manifest_key).await? {
                decode_posting_artifact_manifest(&artifact_manifest_key, &value)?;
            } else {
                return Ok(Vec::new());
            }
            let manifest_key =
                posting_manifest_key(cell_id, edge_type, direction, owner, base_epoch);
            match self.read_remote(&manifest_key).await? {
                Some(value) => Some(decode_posting_manifest(&manifest_key, &value)?),
                None => None,
            }
        };

        let Some(expected) = expected else {
            return Ok(Vec::new());
        };
        let mut chunks = Vec::with_capacity(
            usize::try_from(expected.chunk_count).unwrap_or(GRAPH_CHUNK_PREALLOC_LIMIT),
        );
        for chunk_id in 0..expected.chunk_count {
            let Some(chunk) = chunks_by_id.remove(&chunk_id) else {
                return Err(GraphError::CorruptValue {
                    key: posting_chunk_key(
                        cell_id, edge_type, direction, owner, base_epoch, chunk_id,
                    ),
                    reason: "posting manifest references missing chunk".to_string(),
                });
            };
            if let Some(checksum) = expected.chunk_checksums.get(chunk_id as usize) {
                let actual = posting_chunk_checksum(&chunk);
                if actual != *checksum {
                    return Err(GraphError::CorruptValue {
                        key: posting_chunk_key(
                            cell_id, edge_type, direction, owner, base_epoch, chunk_id,
                        ),
                        reason: "posting chunk checksum does not match manifest".to_string(),
                    });
                }
            }
            chunks.push(chunk);
        }
        Ok(chunks)
    }

    pub(crate) async fn latest_posting_artifact_epoch(
        &self,
        cell_id: &str,
        edge_type: &str,
        read_epoch: GraphEpoch,
    ) -> Result<Option<GraphEpoch>> {
        validate_component("cell_id", cell_id)?;
        validate_component("edge_type", edge_type)?;
        let mut iter = self
            .scan_remote_prefix(&posting_artifact_manifest_prefix(cell_id, edge_type))
            .await?;
        let mut latest = None;
        while let Some(kv) = iter.next().await? {
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            let manifest = decode_posting_artifact_manifest(&key, &kv.value)?;
            if manifest.cell_id != cell_id || manifest.edge_type != edge_type {
                return corrupt(
                    &key,
                    "posting artifact manifest identity does not match prefix",
                );
            }
            if manifest.base_epoch <= read_epoch
                && latest.is_none_or(|epoch| manifest.base_epoch > epoch)
            {
                latest = Some(manifest.base_epoch);
            }
        }
        Ok(latest)
    }

    pub(crate) async fn posting_edge_exists_at_base(
        &self,
        cell_id: &str,
        edge_type: &str,
        src: VertexId,
        dst: VertexId,
        base_epoch: GraphEpoch,
    ) -> Result<bool> {
        let artifact_key = posting_artifact_manifest_key(cell_id, edge_type, base_epoch);
        let Some(value) = self.read_remote(&artifact_key).await? else {
            return corrupt(&artifact_key, "missing posting artifact epoch manifest");
        };
        decode_posting_artifact_manifest(&artifact_key, &value)?;

        let manifest_key =
            posting_manifest_key(cell_id, edge_type, ArtifactDirection::Out, src, base_epoch);
        let Some(value) = self.read_remote(&manifest_key).await? else {
            return Ok(false);
        };
        let manifest = decode_posting_manifest(&manifest_key, &value)?;
        if manifest.chunk_count == 0 {
            return Ok(false);
        }
        if manifest.chunk_bounds.is_empty() {
            let mut low = 0_u64;
            let mut high = manifest.chunk_count;
            while low < high {
                let chunk_id = low + (high - low) / 2;
                let chunk = self
                    .posting_point_lookup_chunk(
                        cell_id, edge_type, src, base_epoch, &manifest, chunk_id,
                    )
                    .await?;
                let Some(first) = chunk.vertices.first().copied() else {
                    return corrupt(&manifest_key, "posting manifest references empty chunk");
                };
                let last = chunk.vertices.last().copied().unwrap_or(first);
                if dst < first {
                    high = chunk_id;
                } else if dst > last {
                    low = chunk_id.saturating_add(1);
                } else {
                    return Ok(chunk.vertices.binary_search(&dst).is_ok());
                }
            }
            return Ok(false);
        }
        let Some(bound) = manifest
            .chunk_bounds
            .iter()
            .find(|bound| bound.first <= dst && dst <= bound.last)
        else {
            return Ok(false);
        };
        let chunk = self
            .posting_point_lookup_chunk(
                cell_id,
                edge_type,
                src,
                base_epoch,
                &manifest,
                bound.chunk_id,
            )
            .await?;
        if chunk.vertices.first().copied() != Some(bound.first)
            || chunk.vertices.last().copied() != Some(bound.last)
        {
            return corrupt(&manifest_key, "posting point-lookup chunk bounds mismatch");
        }
        Ok(chunk.vertices.binary_search(&dst).is_ok())
    }

    async fn posting_point_lookup_chunk(
        &self,
        cell_id: &str,
        edge_type: &str,
        src: VertexId,
        base_epoch: GraphEpoch,
        manifest: &PostingChunkManifest,
        chunk_id: u64,
    ) -> Result<PostingChunk> {
        let key = posting_chunk_key(
            cell_id,
            edge_type,
            ArtifactDirection::Out,
            src,
            base_epoch,
            chunk_id,
        );
        let Some(value) = self.read_remote(&key).await? else {
            return corrupt(
                &key,
                "posting manifest references missing point-lookup chunk",
            );
        };
        let chunk = decode_posting_chunk(&key, &value)?;
        let checksum_index = usize::try_from(chunk_id).map_err(|err| GraphError::CorruptValue {
            key: key.clone(),
            reason: format!("posting chunk id does not fit usize: {err}"),
        })?;
        let expected = manifest
            .chunk_checksums
            .get(checksum_index)
            .ok_or_else(|| GraphError::CorruptValue {
                key: key.clone(),
                reason: "posting manifest is missing point-lookup chunk checksum".to_string(),
            })?;
        if posting_chunk_checksum(&chunk) != *expected {
            return corrupt(&key, "posting point-lookup chunk checksum mismatch");
        }
        Ok(chunk)
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
        self.validate_write_fence(cell_id, "build_matrix_tiles")
            .await?;
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
        if base_epoch == self.current_epoch(cell_id).await? {
            return self
                .build_current_matrix_tiles_streaming(
                    cell_id, edge_type, base_epoch, tile_size, started,
                )
                .await;
        }
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

        match async {
            let artifact_lock = self
                .acquire_matrix_artifact_write_lock(
                    cell_id,
                    edge_type,
                    base_epoch,
                    "build_matrix_tiles",
                )
                .await?;
            let result = async {
                prepare_matrix_artifact_build(self, cell_id, edge_type, base_epoch).await?;
                let mut data_batch = GraphWriteBatch::new();
                let mut pending_writes = 0_usize;
                for tile in out_tiles.iter().chain(transpose_tiles.iter()) {
                    put_artifact_record(
                        self,
                        &[&artifact_lock],
                        cell_id,
                        "build_matrix_tiles",
                        &mut data_batch,
                        &mut pending_writes,
                        matrix_tile_key(tile),
                        encode_matrix_tile(tile),
                    )
                    .await?;
                }
                artifact_lock.renew().await?;
                let graphblas_manifest = append_graphblas_csc_chunks(
                    self,
                    &artifact_lock,
                    &mut data_batch,
                    &mut pending_writes,
                    cell_id,
                    edge_type,
                    base_epoch,
                    &graphblas_csc,
                )
                .await?;
                artifact_lock.renew().await?;
                flush_artifact_put_batch(
                    self,
                    &[&artifact_lock],
                    cell_id,
                    "build_matrix_tiles",
                    &mut data_batch,
                    &mut pending_writes,
                )
                .await?;
                artifact_lock.renew().await?;

                let mut manifest_batch = GraphWriteBatch::new();
                manifest_batch.put(
                    matrix_manifest_key(cell_id, edge_type, base_epoch),
                    encode_matrix_artifact(&artifact),
                );
                manifest_batch.put(
                    graphblas_csc_key(cell_id, edge_type, base_epoch),
                    encode_graphblas_csc_manifest(&graphblas_manifest),
                );
                publish_matrix_artifact_manifests_with_cell_lock(
                    self,
                    cell_id,
                    "build_matrix_tiles",
                    edge_type,
                    base_epoch,
                    manifest_batch,
                )
                .await
            }
            .await;
            crate::release_cell_write_lock(artifact_lock, result).await
        }
        .await
        {
            Ok(()) => {}
            Err(err) => {
                self.cleanup_unpublished_matrix_artifacts_after_abort(
                    cell_id, edge_type, base_epoch, &err,
                )
                .await;
                return Err(err);
            }
        }
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
        if self.cache_policy.max_matrix_adjacencies > 0 {
            self.matrix_cache.lock().await.insert_sized(
                cache_key.clone(),
                Arc::clone(&adjacency),
                tenant.clone(),
                pinned,
                adjacency_resident_bytes(adjacency.as_ref()),
                &self.cache_metrics,
            );
        }

        #[cfg(feature = "graphblas")]
        if self.cache_policy.max_graphblas_matrices > 0 {
            let started = Instant::now();
            let compiled = Arc::new(compile_graphblas_csc(&graphblas_csc)?);
            record_matrix_profile(
                matrix_profile_enabled(),
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
                record_matrix_profile(
                    matrix_profile_enabled(),
                    "build_matrix_tiles_prewarm_graphblas",
                    started.elapsed(),
                    prewarm.vertices.len() as u64,
                );
            }
            let resident_bytes = compiled.estimated_resident_bytes();
            self.graphblas_cache.lock().await.insert_sized(
                cache_key,
                compiled,
                tenant,
                pinned,
                resident_bytes,
                &self.cache_metrics,
            );
        }
        self.record_artifact_build_completed(started.elapsed());
        Ok(artifact)
    }

    async fn build_current_matrix_tiles_streaming(
        &self,
        cell_id: &str,
        edge_type: &str,
        base_epoch: GraphEpoch,
        tile_size: u64,
        started: Instant,
    ) -> Result<MatrixArtifact> {
        let mut rows = self
            .current_matrix_rows(cell_id, edge_type, base_epoch)
            .await?;
        rows.normalize();
        ensure_limit(
            "build_matrix_tiles_edges",
            rows.live_edges,
            self.limits.max_artifact_build_edges,
        )?;

        let artifact = match async {
            let artifact_lock = self
                .acquire_matrix_artifact_write_lock(
                    cell_id,
                    edge_type,
                    base_epoch,
                    "build_matrix_tiles",
                )
                .await?;
            let result = async {
                prepare_matrix_artifact_build(self, cell_id, edge_type, base_epoch).await?;
                let mut data_batch = GraphWriteBatch::new();
                let mut pending_writes = 0_usize;
                let out_tiles = append_matrix_tiles_from_rows(
                    self,
                    &artifact_lock,
                    &mut data_batch,
                    &mut pending_writes,
                    cell_id,
                    edge_type,
                    base_epoch,
                    tile_size,
                    ArtifactDirection::Out,
                    &rows.rows,
                )
                .await?;
                artifact_lock.renew().await?;
                let reversed = rows.reversed();
                let transpose_tiles = append_matrix_tiles_from_rows(
                    self,
                    &artifact_lock,
                    &mut data_batch,
                    &mut pending_writes,
                    cell_id,
                    edge_type,
                    base_epoch,
                    tile_size,
                    ArtifactDirection::In,
                    &reversed.rows,
                )
                .await?;
                drop(reversed);

                artifact_lock.renew().await?;
                let graphblas_manifest = append_graphblas_csc_chunks_from_rows(
                    self,
                    &artifact_lock,
                    &mut data_batch,
                    &mut pending_writes,
                    cell_id,
                    edge_type,
                    base_epoch,
                    &rows,
                )
                .await?;
                artifact_lock.renew().await?;
                flush_artifact_put_batch(
                    self,
                    &[&artifact_lock],
                    cell_id,
                    "build_matrix_tiles",
                    &mut data_batch,
                    &mut pending_writes,
                )
                .await?;
                artifact_lock.renew().await?;

                let artifact = MatrixArtifact {
                    cell_id: cell_id.to_string(),
                    edge_type: edge_type.to_string(),
                    base_epoch,
                    tile_size,
                    out_tiles,
                    transpose_tiles,
                    edge_count: rows.live_edges,
                };

                let lock = self
                    .acquire_cell_write_lock(cell_id, "build_matrix_tiles")
                    .await?;
                let publish_result = async {
                    let ending_epoch = self.current_epoch(cell_id).await?;
                    if ending_epoch != base_epoch {
                        return Err(GraphError::SnapshotChanged {
                            operation: "build_matrix_tiles",
                            cell_id: cell_id.to_string(),
                            edge_type: edge_type.to_string(),
                            read_epoch: base_epoch,
                            current_epoch: ending_epoch,
                        });
                    }

                    let mut manifest_batch = GraphWriteBatch::new();
                    manifest_batch.put(
                        matrix_manifest_key(cell_id, edge_type, base_epoch),
                        encode_matrix_artifact(&artifact),
                    );
                    manifest_batch.put(
                        graphblas_csc_key(cell_id, edge_type, base_epoch),
                        encode_graphblas_csc_manifest(&graphblas_manifest),
                    );
                    publish_matrix_artifact_manifests(
                        self,
                        cell_id,
                        "build_matrix_tiles",
                        edge_type,
                        base_epoch,
                        manifest_batch,
                    )
                    .await
                }
                .await;
                crate::release_cell_write_lock(lock, publish_result).await?;

                Ok(artifact)
            }
            .await;
            crate::release_cell_write_lock(artifact_lock, result).await
        }
        .await
        {
            Ok(artifact) => artifact,
            Err(err) => {
                self.cleanup_unpublished_matrix_artifacts_after_abort(
                    cell_id, edge_type, base_epoch, &err,
                )
                .await;
                return Err(err);
            }
        };

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

        let adjacency = if self.cache_policy.max_matrix_adjacencies > 0 {
            Some(Arc::new(rows.to_adjacency()))
        } else {
            None
        };
        if let Some(adjacency) = &adjacency {
            self.matrix_cache.lock().await.insert_sized(
                cache_key.clone(),
                Arc::clone(adjacency),
                tenant.clone(),
                pinned,
                adjacency_resident_bytes(adjacency.as_ref()),
                &self.cache_metrics,
            );
        }

        #[cfg(feature = "graphblas")]
        if self.cache_policy.max_graphblas_matrices > 0 {
            let started = Instant::now();
            let graphblas_csc = matrix_rows_to_graphblas_csc(&rows)?;
            let compiled = Arc::new(compile_graphblas_csc_owned(graphblas_csc)?);
            record_matrix_profile(
                matrix_profile_enabled(),
                "build_matrix_tiles_precompile_graphblas",
                started.elapsed(),
                rows.live_edges,
            );
            if let Some(prewarm_start) = rows
                .rows
                .iter()
                .filter(|(_, dsts)| !dsts.is_empty())
                .min_by_key(|(_, dsts)| dsts.len())
                .map(|(src, _)| *src)
            {
                let empty_adjacency = MatrixAdjacency::new();
                let adjacency_ref = adjacency.as_deref().unwrap_or(&empty_adjacency);
                let started = Instant::now();
                let prewarm =
                    expand_compiled_graphblas(&compiled, adjacency_ref, &[prewarm_start], 1)?;
                record_matrix_profile(
                    matrix_profile_enabled(),
                    "build_matrix_tiles_prewarm_graphblas",
                    started.elapsed(),
                    prewarm.vertices.len() as u64,
                );
            }
            let resident_bytes = compiled.estimated_resident_bytes();
            self.graphblas_cache.lock().await.insert_sized(
                cache_key,
                compiled,
                tenant,
                pinned,
                resident_bytes,
                &self.cache_metrics,
            );
        }

        drop(rows);
        trim_process_memory_after_hydration();

        self.record_artifact_build_completed(started.elapsed());
        Ok(artifact)
    }

    async fn cleanup_unpublished_matrix_artifacts_after_abort(
        &self,
        cell_id: &str,
        edge_type: &str,
        base_epoch: GraphEpoch,
        build_error: &GraphError,
    ) {
        let cleanup = cleanup_unpublished_matrix_artifact_epoch(
            self,
            cell_id,
            edge_type,
            base_epoch,
            "build_matrix_tiles_abort",
        )
        .await;
        if cleanup.cleanup_errors > 0 {
            tracing::warn!(
                target: "slatedb_graph_kernel",
                cell_id,
                edge_type,
                base_epoch,
                deleted_keys = cleanup.deleted_keys,
                cleanup_errors = cleanup.cleanup_errors,
                build_error = %build_error,
                "matrix artifact abort cleanup was incomplete"
            );
        }
    }

    async fn current_matrix_rows(
        &self,
        cell_id: &str,
        edge_type: &str,
        base_epoch: GraphEpoch,
    ) -> Result<MatrixRows> {
        let mut rows = MatrixRows::default();
        let prefix = keys::out_edge_type_prefix(cell_id, edge_type);
        let mut iter = self.scan_remote_prefix(&prefix).await?;
        while let Some(kv) = iter.next().await? {
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            let record = decode_edge_record(&key, &kv.value)?;
            if record.epoch <= base_epoch {
                rows.push(record.src, record.dst);
                ensure_limit(
                    "build_matrix_tiles_edges",
                    rows.raw_edges,
                    self.limits.max_artifact_build_edges,
                )?;
            }
        }

        let tombstones = self
            .current_out_segment_tombstones(cell_id, edge_type, base_epoch)
            .await?;
        let prefix = keys::out_segment_edge_type_prefix(cell_id, edge_type);
        let mut iter = self.scan_remote_prefix(&prefix).await?;
        while let Some(kv) = iter.next().await? {
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            let segment = decode_out_edge_segment(&key, &kv.value)?;
            if segment.start_epoch > base_epoch {
                continue;
            }
            for (epoch, dst) in segment.edges.iter().copied() {
                if epoch > base_epoch {
                    break;
                }
                let tombstone_epoch = tombstones.get(&(segment.src, dst)).copied();
                if segment_edge_visible(epoch, tombstone_epoch) {
                    rows.push(segment.src, dst);
                    ensure_limit(
                        "build_matrix_tiles_edges",
                        rows.raw_edges,
                        self.limits.max_artifact_build_edges,
                    )?;
                }
            }
        }
        Ok(rows)
    }

    async fn current_out_segment_tombstones(
        &self,
        cell_id: &str,
        edge_type: &str,
        base_epoch: GraphEpoch,
    ) -> Result<BTreeMap<(VertexId, VertexId), GraphEpoch>> {
        let prefix = keys::out_segment_tombstone_edge_type_prefix(cell_id, edge_type);
        let mut iter = self.scan_remote_prefix(&prefix).await?;
        let mut tombstones = BTreeMap::new();
        while let Some(kv) = iter.next().await? {
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            let (key_cell_id, key_edge_type, src, dst) =
                parse_out_edge_segment_tombstone_key(&key)?;
            if key_cell_id != cell_id || key_edge_type != edge_type {
                return Err(GraphError::CorruptValue {
                    key,
                    reason: "segment tombstone identity does not match scan prefix".to_string(),
                });
            }
            let epoch = decode_u64(&key, &kv.value)?;
            if epoch <= base_epoch {
                tombstones.insert((src, dst), epoch);
            }
        }
        Ok(tombstones)
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
        let mut latest: Option<MatrixArtifact> = None;
        let mut decoded = Vec::new();
        while let Some(kv) = iter.next().await? {
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            let artifact = decode_matrix_artifact(&key, &kv.value)?;
            decoded.push(artifact.clone());
            if artifact.base_epoch <= read_epoch
                && match latest.as_ref() {
                    Some(current) => artifact.base_epoch > current.base_epoch,
                    None => true,
                }
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

async fn prepare_matrix_artifact_build(
    shard: &GraphShard,
    cell_id: &str,
    edge_type: &str,
    base_epoch: GraphEpoch,
) -> Result<()> {
    prepare_artifact_build(
        shard,
        cell_id,
        "prepare_matrix_artifact_build",
        &[matrix_cleanup_marker_key(cell_id, edge_type, base_epoch)],
    )
    .await
}

async fn publish_matrix_artifact_manifests(
    shard: &GraphShard,
    cell_id: &str,
    operation: &'static str,
    edge_type: &str,
    base_epoch: GraphEpoch,
    batch: GraphWriteBatch,
) -> Result<()> {
    publish_artifact_records_guarded(
        shard,
        cell_id,
        operation,
        &[matrix_cleanup_marker_key(cell_id, edge_type, base_epoch)],
        batch,
    )
    .await
}

async fn publish_matrix_artifact_manifests_with_cell_lock(
    shard: &GraphShard,
    cell_id: &str,
    operation: &'static str,
    edge_type: &str,
    base_epoch: GraphEpoch,
    batch: GraphWriteBatch,
) -> Result<()> {
    publish_artifact_records_guarded_with_cell_lock(
        shard,
        cell_id,
        operation,
        &[matrix_cleanup_marker_key(cell_id, edge_type, base_epoch)],
        batch,
    )
    .await
}

async fn ensure_posting_artifact_publish_compatible(
    shard: &GraphShard,
    cell_id: &str,
    edge_type: &str,
    base_epoch: GraphEpoch,
    artifact_manifest: Option<&PostingArtifactManifest>,
    manifests: &[PostingChunkManifest],
) -> Result<()> {
    let artifact_key = posting_artifact_manifest_key(cell_id, edge_type, base_epoch);
    let Some(value) = shard.read_remote(&artifact_key).await? else {
        return Ok(());
    };
    let existing_artifact = decode_posting_artifact_manifest(&artifact_key, &value)?;
    if Some(&existing_artifact) != artifact_manifest {
        return Err(GraphError::CorruptValue {
            key: artifact_key,
            reason: "posting artifact epoch is already published with a different layout"
                .to_string(),
        });
    }
    for manifest in manifests {
        let key = posting_manifest_key(
            &manifest.cell_id,
            &manifest.edge_type,
            manifest.direction,
            manifest.owner,
            manifest.base_epoch,
        );
        let Some(value) = shard.read_remote(&key).await? else {
            continue;
        };
        let existing = decode_posting_manifest(&key, &value)?;
        if existing != *manifest {
            return Err(GraphError::CorruptValue {
                key,
                reason: "posting artifact epoch is already published with a different chunk layout"
                    .to_string(),
            });
        }
    }
    Ok(())
}
