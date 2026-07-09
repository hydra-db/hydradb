use super::*;

impl GraphShard {
    pub async fn build_supernode_groups(
        &self,
        cell_id: &str,
        edge_type: &str,
        base_epoch: GraphEpoch,
        degree_threshold: u64,
        chunk_size: usize,
    ) -> Result<Vec<SupernodeGroup>> {
        self.build_supernode_groups_for_directions(
            cell_id,
            edge_type,
            base_epoch,
            degree_threshold,
            chunk_size,
            &[ArtifactDirection::Out, ArtifactDirection::In],
        )
        .await
    }

    pub async fn build_supernode_groups_for_directions(
        &self,
        cell_id: &str,
        edge_type: &str,
        base_epoch: GraphEpoch,
        degree_threshold: u64,
        chunk_size: usize,
        directions: &[ArtifactDirection],
    ) -> Result<Vec<SupernodeGroup>> {
        validate_component("cell_id", cell_id)?;
        validate_component("edge_type", edge_type)?;
        self.ensure_write_authority(cell_id, "build_supernode_groups_for_directions")?;
        if chunk_size == 0 {
            return Err(GraphError::CorruptValue {
                key: "supernode_chunk_size".to_string(),
                reason: "chunk size must be greater than zero".to_string(),
            });
        }

        ensure_limit(
            "build_supernode_groups",
            base_epoch,
            self.limits.max_artifact_source_epochs,
        )?;

        let _permit = self
            .acquire_artifact_build_permit("build_supernode_groups")
            .await?;
        let started = Instant::now();
        let groups = if base_epoch == self.current_epoch(cell_id).await? {
            self.build_current_supernode_groups(
                cell_id,
                edge_type,
                base_epoch,
                degree_threshold,
                chunk_size,
                directions,
            )
            .await?
        } else {
            let edges = self.edges_at(cell_id, edge_type, base_epoch).await?;
            ensure_limit(
                "build_supernode_groups_edges",
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
            let mut groups = Vec::new();
            if directions.contains(&ArtifactDirection::Out) {
                append_supernode_chunks(
                    cell_id,
                    edge_type,
                    base_epoch,
                    ArtifactDirection::Out,
                    degree_threshold,
                    chunk_size,
                    by_out,
                    &mut chunks,
                    &mut groups,
                );
            }
            if directions.contains(&ArtifactDirection::In) {
                append_supernode_chunks(
                    cell_id,
                    edge_type,
                    base_epoch,
                    ArtifactDirection::In,
                    degree_threshold,
                    chunk_size,
                    by_in,
                    &mut chunks,
                    &mut groups,
                );
            }
            self.persist_supernode_artifacts(cell_id, &chunks, &groups)
                .await?;
            groups
        };
        self.record_artifact_build_completed(started.elapsed());
        Ok(groups)
    }

    async fn build_current_supernode_groups(
        &self,
        cell_id: &str,
        edge_type: &str,
        base_epoch: GraphEpoch,
        degree_threshold: u64,
        chunk_size: usize,
        directions: &[ArtifactDirection],
    ) -> Result<Vec<SupernodeGroup>> {
        let starting_epoch = self.current_epoch(cell_id).await?;
        if starting_epoch != base_epoch {
            return Err(GraphError::SnapshotChanged {
                operation: "build_supernode_groups",
                cell_id: cell_id.to_string(),
                edge_type: edge_type.to_string(),
                read_epoch: base_epoch,
                current_epoch: starting_epoch,
            });
        }
        let mut chunks = Vec::new();
        let mut groups = Vec::new();
        for direction in directions {
            self.append_current_supernode_groups(
                cell_id,
                edge_type,
                base_epoch,
                *direction,
                degree_threshold,
                chunk_size,
                &mut chunks,
                &mut groups,
            )
            .await?;
        }
        let ending_epoch = self.current_epoch(cell_id).await?;
        if ending_epoch != base_epoch {
            return Err(GraphError::SnapshotChanged {
                operation: "build_supernode_groups",
                cell_id: cell_id.to_string(),
                edge_type: edge_type.to_string(),
                read_epoch: base_epoch,
                current_epoch: ending_epoch,
            });
        }
        self.persist_supernode_artifacts(cell_id, &chunks, &groups)
            .await?;
        Ok(groups)
    }

    #[allow(clippy::too_many_arguments)]
    async fn append_current_supernode_groups(
        &self,
        cell_id: &str,
        edge_type: &str,
        base_epoch: GraphEpoch,
        direction: ArtifactDirection,
        degree_threshold: u64,
        chunk_size: usize,
        chunks: &mut Vec<PostingChunk>,
        groups: &mut Vec<SupernodeGroup>,
    ) -> Result<()> {
        for owner in self
            .current_supernode_owners(cell_id, edge_type, direction, degree_threshold)
            .await?
        {
            let first_chunk_id = chunks.len() as u64;
            let mut chunk_bounds = Vec::new();
            let mut vertices = Vec::with_capacity(chunk_size);
            let prefix = match direction {
                ArtifactDirection::Out => crate::keys::out_prefix(cell_id, edge_type, owner),
                ArtifactDirection::In => crate::keys::in_prefix(cell_id, edge_type, owner),
            };
            let mut iter = self.scan_remote_prefix(&prefix).await?;
            let mut last_vertex = None;
            while let Some(kv) = iter.next().await? {
                let key = String::from_utf8_lossy(&kv.key).into_owned();
                let edge = decode_edge_record(&key, &kv.value)?;
                if edge.epoch > base_epoch {
                    continue;
                }
                let vertex = match direction {
                    ArtifactDirection::Out => edge.dst,
                    ArtifactDirection::In => edge.src,
                };
                if last_vertex == Some(vertex) {
                    continue;
                }
                last_vertex = Some(vertex);
                vertices.push(vertex);
                if vertices.len() == chunk_size {
                    append_current_supernode_chunk(
                        cell_id,
                        edge_type,
                        base_epoch,
                        direction,
                        owner,
                        chunks,
                        &mut chunk_bounds,
                        &mut vertices,
                    );
                }
            }
            if !vertices.is_empty() {
                append_current_supernode_chunk(
                    cell_id,
                    edge_type,
                    base_epoch,
                    direction,
                    owner,
                    chunks,
                    &mut chunk_bounds,
                    &mut vertices,
                );
            }
            let chunk_count = (chunks.len() as u64).saturating_sub(first_chunk_id);
            if chunk_count == 0 {
                continue;
            }
            let degree = chunks
                .iter()
                .skip(first_chunk_id as usize)
                .map(|chunk| chunk.vertices.len() as u64)
                .sum::<u64>();
            groups.push(SupernodeGroup {
                cell_id: cell_id.to_string(),
                edge_type: edge_type.to_string(),
                direction,
                vertex_id: owner,
                base_epoch,
                degree,
                chunk_count,
                page_size: chunk_size as u64,
                chunk_bounds,
            });
        }
        Ok(())
    }

    async fn current_supernode_owners(
        &self,
        cell_id: &str,
        edge_type: &str,
        direction: ArtifactDirection,
        degree_threshold: u64,
    ) -> Result<Vec<VertexId>> {
        let prefix = match direction {
            ArtifactDirection::Out => crate::keys::degree_out_prefix(cell_id, edge_type),
            ArtifactDirection::In => crate::keys::degree_in_prefix(cell_id, edge_type),
        };
        let mut iter = self.scan_remote_prefix(&prefix).await?;
        let mut owners = Vec::new();
        while let Some(kv) = iter.next().await? {
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            let degree = decode_u64(&key, &kv.value)?;
            if degree >= degree_threshold {
                owners.push(parse_last_key_component(&key, "supernode_owner")?);
            }
        }
        Ok(owners)
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn rollup_artifacts(
        &self,
        cell_id: &str,
        edge_type: &str,
        base_epoch: GraphEpoch,
        posting_chunk_size: usize,
        matrix_tile_size: u64,
        supernode_degree_threshold: u64,
        supernode_chunk_size: usize,
    ) -> Result<GraphRollup> {
        validate_component("cell_id", cell_id)?;
        validate_component("edge_type", edge_type)?;
        self.ensure_write_authority(cell_id, "rollup_artifacts")?;
        let posting_chunks = self
            .build_posting_chunks(cell_id, edge_type, base_epoch, posting_chunk_size)
            .await?;
        let matrix = self
            .build_matrix_tiles(cell_id, edge_type, base_epoch, matrix_tile_size)
            .await?;
        let supernode_groups = self
            .persist_supernode_groups_from_chunks(
                cell_id,
                edge_type,
                base_epoch,
                supernode_degree_threshold,
                supernode_chunk_size,
                &posting_chunks,
            )
            .await?;

        let rollup = GraphRollup {
            cell_id: cell_id.to_string(),
            edge_type: edge_type.to_string(),
            base_epoch,
            posting_chunks: posting_chunks.len() as u64,
            matrix_edge_count: matrix.edge_count,
            supernode_groups: supernode_groups.len() as u64,
        };
        let mut batch = GraphWriteBatch::new();
        batch.put(
            rollup_key(cell_id, edge_type, base_epoch),
            encode_graph_rollup(&rollup),
        );
        self.write_graph_batch_strict(cell_id, "rollup_artifacts", batch)
            .await?;
        tracing::info!(
            target: "slatedb_graph_kernel",
            cell_id,
            edge_type,
            base_epoch,
            posting_chunks = rollup.posting_chunks,
            matrix_edge_count = rollup.matrix_edge_count,
            supernode_groups = rollup.supernode_groups,
            "published graph rollup"
        );
        Ok(rollup)
    }

    pub async fn latest_rollup(
        &self,
        cell_id: &str,
        edge_type: &str,
        read_epoch: GraphEpoch,
    ) -> Result<Option<GraphRollup>> {
        validate_component("cell_id", cell_id)?;
        validate_component("edge_type", edge_type)?;
        let prefix = rollup_prefix(cell_id, edge_type);
        let mut iter = self.scan_remote_prefix(&prefix).await?;
        let mut latest: Option<GraphRollup> = None;
        while let Some(kv) = iter.next().await? {
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            let rollup = decode_graph_rollup(&key, &kv.value)?;
            if rollup.base_epoch <= read_epoch
                && match latest.as_ref() {
                    Some(current) => rollup.base_epoch > current.base_epoch,
                    None => true,
                }
            {
                latest = Some(rollup);
            }
        }
        Ok(latest)
    }

    pub async fn delete_graph_artifacts_before(
        &self,
        cell_id: &str,
        edge_type: &str,
        keep_epoch: GraphEpoch,
    ) -> Result<ArtifactGcResult> {
        validate_component("cell_id", cell_id)?;
        validate_component("edge_type", edge_type)?;
        self.ensure_write_authority(cell_id, "delete_graph_artifacts_before")?;
        let _permit = self
            .acquire_gc_permit("delete_graph_artifacts_before")
            .await?;
        let started = Instant::now();
        let safe_keep_epoch = self.artifact_gc_safe_keep_epoch(cell_id, edge_type).await?;
        if keep_epoch > safe_keep_epoch {
            return Err(self.record_retention_reject(
                "delete_graph_artifacts_before",
                cell_id,
                keep_epoch,
                safe_keep_epoch,
            ));
        }
        let mut result = ArtifactGcResult::default();
        let mut batch = GraphWriteBatch::new();
        let mut pending_deletes = 0_usize;

        for prefix in graph_artifact_gc_prefixes(cell_id, edge_type) {
            let mut iter = self.scan_remote_prefix(&prefix).await?;
            while let Some(kv) = iter.next().await? {
                let key = String::from_utf8_lossy(&kv.key).into_owned();
                let Some(base_epoch) = graph_artifact_epoch_from_key(&key)? else {
                    result.retained_keys += 1;
                    continue;
                };
                if base_epoch < keep_epoch {
                    batch.delete(key.as_bytes());
                    result.deleted_keys += 1;
                    pending_deletes += 1;
                    if pending_deletes >= GRAPH_ARTIFACT_GC_BATCH_KEYS {
                        flush_artifact_gc_batch(
                            self,
                            cell_id,
                            "delete_graph_artifacts_before",
                            &mut batch,
                            &mut pending_deletes,
                        )
                        .await?;
                    }
                } else {
                    result.retained_keys += 1;
                }
            }
        }

        flush_artifact_gc_batch(
            self,
            cell_id,
            "delete_graph_artifacts_before",
            &mut batch,
            &mut pending_deletes,
        )
        .await?;
        self.matrix_artifact_cache.lock().await.retain(|key, _| {
            key.cell_id != cell_id || key.edge_type != edge_type || key.base_epoch >= keep_epoch
        });
        self.matrix_cache.lock().await.retain(|key, _| {
            key.cell_id != cell_id || key.edge_type != edge_type || key.base_epoch >= keep_epoch
        });
        self.graphblas_cache.lock().await.retain(|key, _| {
            key.cell_id != cell_id || key.edge_type != edge_type || key.base_epoch >= keep_epoch
        });
        self.supernode_group_cache.lock().await.retain(|key, _| {
            key.cell_id != cell_id || key.edge_type != edge_type || key.base_epoch >= keep_epoch
        });
        self.posting_chunk_cache.lock().await.retain(|key, _| {
            key.cell_id != cell_id || key.edge_type != edge_type || key.base_epoch >= keep_epoch
        });
        self.materialized_supernode_cache
            .lock()
            .await
            .retain(|key, _| {
                key.cell_id != cell_id || key.edge_type != edge_type || key.base_epoch >= keep_epoch
            });
        tracing::info!(
            target: "slatedb_graph_kernel",
            cell_id,
            edge_type,
            keep_epoch,
            deleted_keys = result.deleted_keys,
            retained_keys = result.retained_keys,
            "deleted old graph artifacts"
        );
        self.record_gc_completed(result.deleted_keys, started.elapsed());
        Ok(result)
    }

    async fn persist_supernode_groups_from_chunks(
        &self,
        cell_id: &str,
        edge_type: &str,
        base_epoch: GraphEpoch,
        degree_threshold: u64,
        chunk_size: usize,
        chunks: &[PostingChunk],
    ) -> Result<Vec<SupernodeGroup>> {
        let mut groups = Vec::new();
        let mut batch = GraphWriteBatch::new();
        let mut pending_writes = 0_usize;
        for direction in [ArtifactDirection::Out, ArtifactDirection::In] {
            let mut owners: BTreeMap<VertexId, Vec<&PostingChunk>> = BTreeMap::new();
            for chunk in chunks.iter().filter(|chunk| chunk.direction == direction) {
                owners.entry(chunk.owner).or_default().push(chunk);
            }
            for (owner, owner_chunks) in owners {
                let degree: u64 = owner_chunks
                    .iter()
                    .map(|chunk| chunk.vertices.len() as u64)
                    .sum();
                if degree < degree_threshold {
                    continue;
                }
                let group = SupernodeGroup {
                    cell_id: cell_id.to_string(),
                    edge_type: edge_type.to_string(),
                    direction,
                    vertex_id: owner,
                    base_epoch,
                    degree,
                    chunk_count: owner_chunks.len() as u64,
                    page_size: chunk_size as u64,
                    chunk_bounds: supernode_chunk_bounds(&owner_chunks),
                };
                put_artifact_record(
                    self,
                    cell_id,
                    "persist_supernode_groups_from_chunks",
                    &mut batch,
                    &mut pending_writes,
                    supernode_group_key(&group),
                    encode_supernode_group(&group),
                )
                .await?;
                groups.push(group);
            }
        }
        flush_artifact_put_batch(
            self,
            cell_id,
            "persist_supernode_groups_from_chunks",
            &mut batch,
            &mut pending_writes,
        )
        .await?;
        if !groups.is_empty() {
            let mut cache = self.supernode_group_cache.lock().await;
            for group in &groups {
                cache.insert(
                    SupernodeCacheKey::new(
                        &group.cell_id,
                        &group.edge_type,
                        group.direction,
                        group.vertex_id,
                        group.base_epoch,
                    ),
                    group.clone(),
                    group.cell_id.clone(),
                    self.cache_policy.pin_supernode_group(group),
                    &self.cache_metrics,
                );
            }
        }
        Ok(groups)
    }

    async fn persist_supernode_artifacts(
        &self,
        cell_id: &str,
        chunks: &[PostingChunk],
        groups: &[SupernodeGroup],
    ) -> Result<()> {
        let mut batch = GraphWriteBatch::new();
        let mut pending_writes = 0_usize;
        for chunk in chunks {
            put_artifact_record(
                self,
                cell_id,
                "persist_supernode_artifacts",
                &mut batch,
                &mut pending_writes,
                posting_key(chunk),
                encode_posting_chunk(chunk),
            )
            .await?;
        }
        for group in groups {
            put_artifact_record(
                self,
                cell_id,
                "persist_supernode_artifacts",
                &mut batch,
                &mut pending_writes,
                supernode_group_key(group),
                encode_supernode_group(group),
            )
            .await?;
        }
        flush_artifact_put_batch(
            self,
            cell_id,
            "persist_supernode_artifacts",
            &mut batch,
            &mut pending_writes,
        )
        .await?;
        if !chunks.is_empty() {
            let mut cache = self.posting_chunk_cache.lock().await;
            for chunk in chunks {
                let pinned = groups.iter().any(|group| {
                    group.cell_id == chunk.cell_id
                        && group.edge_type == chunk.edge_type
                        && group.direction == chunk.direction
                        && group.vertex_id == chunk.owner
                        && group.base_epoch == chunk.base_epoch
                        && self.cache_policy.pin_supernode_group(group)
                });
                cache.insert(
                    PostingChunkCacheKey::from_chunk(chunk),
                    chunk.clone(),
                    chunk.cell_id.clone(),
                    pinned,
                    &self.cache_metrics,
                );
            }
        }
        if !groups.is_empty() {
            let mut cache = self.supernode_group_cache.lock().await;
            for group in groups {
                cache.insert(
                    SupernodeCacheKey::new(
                        &group.cell_id,
                        &group.edge_type,
                        group.direction,
                        group.vertex_id,
                        group.base_epoch,
                    ),
                    group.clone(),
                    group.cell_id.clone(),
                    self.cache_policy.pin_supernode_group(group),
                    &self.cache_metrics,
                );
            }
        }
        Ok(())
    }

    pub async fn supernode_group(
        &self,
        cell_id: &str,
        edge_type: &str,
        direction: ArtifactDirection,
        vertex_id: VertexId,
        read_epoch: GraphEpoch,
    ) -> Result<Option<SupernodeGroup>> {
        validate_component("cell_id", cell_id)?;
        validate_component("edge_type", edge_type)?;
        if let Some(cached) = self.supernode_group_cache.lock().await.get_latest_by(
            |key, _| {
                key.cell_id == cell_id
                    && key.edge_type == edge_type
                    && key.direction == direction
                    && key.vertex_id == vertex_id
                    && key.base_epoch <= read_epoch
            },
            |key, _| key.base_epoch,
        ) {
            self.cache_metrics
                .record_hit(GraphCacheKind::SupernodeGroup);
            self.prefetch_supernode_chunks(&cached).await;
            return Ok(Some(cached));
        }

        self.cache_metrics
            .record_miss(GraphCacheKind::SupernodeGroup);
        let _permit = self.acquire_hydration_permit("supernode_group").await?;
        let prefix = supernode_group_prefix(cell_id, edge_type, direction, vertex_id);
        let mut iter = self.scan_remote_prefix(&prefix).await?;
        let mut latest: Option<SupernodeGroup> = None;
        let mut decoded = Vec::new();
        while let Some(kv) = iter.next().await? {
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            let group = decode_supernode_group(&key, &kv.value)?;
            decoded.push(group.clone());
            if group.base_epoch <= read_epoch
                && match latest.as_ref() {
                    Some(current) => group.base_epoch > current.base_epoch,
                    None => true,
                }
            {
                latest = Some(group);
            }
        }
        self.record_hydration_complete();
        if !decoded.is_empty() {
            let mut cache = self.supernode_group_cache.lock().await;
            for group in decoded {
                cache.insert(
                    SupernodeCacheKey::new(
                        &group.cell_id,
                        &group.edge_type,
                        group.direction,
                        group.vertex_id,
                        group.base_epoch,
                    ),
                    group.clone(),
                    group.cell_id.clone(),
                    self.cache_policy.pin_supernode_group(&group),
                    &self.cache_metrics,
                );
            }
        }
        if let Some(group) = latest.as_ref() {
            self.prefetch_supernode_chunks(group).await;
        }
        Ok(latest)
    }

    pub(crate) async fn supernode_one_hop_reachable(
        &self,
        cell_id: &str,
        edge_type: &str,
        starts: &[VertexId],
        read_epoch: GraphEpoch,
        sparse_kernel: SparseKernelBackend,
    ) -> Result<Option<MatrixTraversalResult>> {
        let profile = matrix_profile_enabled();
        let total_started = Instant::now();
        if starts.is_empty() {
            return Ok(Some(MatrixTraversalResult {
                backend: TraversalBackend::MatrixOverlay,
                vertices: Vec::new(),
                hops: 1,
                base_epoch: read_epoch,
                edge_visits: 0,
                delta_records_applied: 0,
                sparse_kernel,
            }));
        }

        if let [start] = starts {
            let started = Instant::now();
            let Some(group) = self
                .supernode_group(
                    cell_id,
                    edge_type,
                    ArtifactDirection::Out,
                    *start,
                    read_epoch,
                )
                .await?
            else {
                record_matrix_profile(
                    profile,
                    "supernode_one_hop_group_miss",
                    started.elapsed(),
                    *start,
                );
                return Ok(None);
            };
            record_matrix_profile(
                profile,
                "supernode_one_hop_group_hit",
                started.elapsed(),
                group.chunk_count,
            );

            let started = Instant::now();
            let overlay = match self.supernode_delta_overlay(&group, read_epoch).await {
                Ok(overlay) => overlay,
                Err(GraphError::SnapshotExpired { .. }) => return Ok(None),
                Err(err) => return Err(err),
            };
            let delta_records_applied =
                (overlay.plus.len() as u64).saturating_add(overlay.minus.len() as u64);
            record_matrix_profile(
                profile,
                "supernode_one_hop_overlay",
                started.elapsed(),
                delta_records_applied,
            );

            let started = Instant::now();
            let (vertices, edge_visits) = if overlay.is_empty() {
                let vertices = self.materialized_supernode_vertices(&group).await?;
                (vertices.as_ref().clone(), group.degree)
            } else {
                let degree = self.supernode_degree_with_overlay(&group, &overlay).await?;
                let limit = usize::try_from(degree).unwrap_or(usize::MAX);
                (
                    self.merged_supernode_vertices(&group, &overlay, 0, limit)
                        .await?,
                    degree,
                )
            };
            record_matrix_profile(
                profile,
                "supernode_one_hop_materialize",
                started.elapsed(),
                vertices.len() as u64,
            );
            record_matrix_profile(
                profile,
                "supernode_one_hop_total",
                total_started.elapsed(),
                vertices.len() as u64,
            );
            return Ok(Some(MatrixTraversalResult {
                backend: TraversalBackend::MatrixOverlay,
                vertices,
                hops: 1,
                base_epoch: group.base_epoch,
                edge_visits,
                delta_records_applied,
                sparse_kernel,
            }));
        }

        let mut result = BTreeSet::new();
        let mut base_epoch = read_epoch;
        let mut edge_visits = 0_u64;
        let mut delta_records_applied = 0_u64;
        for start in starts {
            let started = Instant::now();
            let Some(group) = self
                .supernode_group(
                    cell_id,
                    edge_type,
                    ArtifactDirection::Out,
                    *start,
                    read_epoch,
                )
                .await?
            else {
                record_matrix_profile(
                    profile,
                    "supernode_one_hop_group_miss",
                    started.elapsed(),
                    *start,
                );
                return Ok(None);
            };
            record_matrix_profile(
                profile,
                "supernode_one_hop_group_hit",
                started.elapsed(),
                group.chunk_count,
            );
            base_epoch = base_epoch.min(group.base_epoch);
            let started = Instant::now();
            let overlay = match self.supernode_delta_overlay(&group, read_epoch).await {
                Ok(overlay) => overlay,
                Err(GraphError::SnapshotExpired { .. }) => return Ok(None),
                Err(err) => return Err(err),
            };
            record_matrix_profile(
                profile,
                "supernode_one_hop_overlay",
                started.elapsed(),
                overlay.plus.len() as u64 + overlay.minus.len() as u64,
            );
            delta_records_applied = delta_records_applied
                .saturating_add(overlay.plus.len() as u64)
                .saturating_add(overlay.minus.len() as u64);
            let started = Instant::now();
            let degree = self.supernode_degree_with_overlay(&group, &overlay).await?;
            record_matrix_profile(
                profile,
                "supernode_one_hop_degree",
                started.elapsed(),
                degree,
            );
            edge_visits = edge_visits.saturating_add(degree);
            let limit = usize::try_from(degree).unwrap_or(usize::MAX);
            let started = Instant::now();
            for vertex in self
                .merged_supernode_vertices(&group, &overlay, 0, limit)
                .await?
            {
                result.insert(vertex);
            }
            record_matrix_profile(
                profile,
                "supernode_one_hop_materialize",
                started.elapsed(),
                result.len() as u64,
            );
        }

        record_matrix_profile(
            profile,
            "supernode_one_hop_total",
            total_started.elapsed(),
            result.len() as u64,
        );
        Ok(Some(MatrixTraversalResult {
            backend: TraversalBackend::MatrixOverlay,
            vertices: result.into_iter().collect(),
            hops: 1,
            base_epoch,
            edge_visits,
            delta_records_applied,
            sparse_kernel,
        }))
    }

    pub async fn supernode_degree(
        &self,
        cell_id: &str,
        edge_type: &str,
        vertex_id: VertexId,
        read_epoch: GraphEpoch,
    ) -> Result<u64> {
        if let Some(group) = self
            .supernode_group(
                cell_id,
                edge_type,
                ArtifactDirection::Out,
                vertex_id,
                read_epoch,
            )
            .await?
        {
            let overlay = self.supernode_delta_overlay(&group, read_epoch).await?;
            return self.supernode_degree_with_overlay(&group, &overlay).await;
        }
        self.out_degree_at(cell_id, edge_type, vertex_id, read_epoch)
            .await
    }

    pub async fn supernode_page(
        &self,
        cell_id: &str,
        edge_type: &str,
        direction: ArtifactDirection,
        vertex_id: VertexId,
        read_epoch: GraphEpoch,
        page_id: u64,
    ) -> Result<Option<SupernodePage>> {
        let Some(group) = self
            .supernode_group(cell_id, edge_type, direction, vertex_id, read_epoch)
            .await?
        else {
            return Ok(None);
        };
        if group.page_size == 0 {
            return Err(GraphError::CorruptValue {
                key: supernode_group_key(&group),
                reason: "supernode page size must be greater than zero".to_string(),
            });
        }
        let overlay = self.supernode_delta_overlay(&group, read_epoch).await?;
        let page_size =
            usize::try_from(group.page_size).map_err(|err| GraphError::CorruptValue {
                key: supernode_group_key(&group),
                reason: format!("invalid supernode page size: {err}"),
            })?;
        let skip =
            page_id
                .checked_mul(group.page_size)
                .ok_or_else(|| GraphError::CorruptValue {
                    key: supernode_group_key(&group),
                    reason: "supernode page offset overflow".to_string(),
                })?;
        let mut vertices = self
            .merged_supernode_vertices(&group, &overlay, skip, page_size.saturating_add(1))
            .await?;
        if vertices.is_empty() {
            return Ok(None);
        }
        let has_next = vertices.len() > page_size;
        vertices.truncate(page_size);
        Ok(Some(SupernodePage {
            vertex_id,
            edge_type: edge_type.to_string(),
            direction,
            page_id,
            has_next,
            vertices,
        }))
    }

    pub async fn out_supernode_window(
        &self,
        cell_id: &str,
        edge_type: &str,
        vertex_id: VertexId,
        read_epoch: GraphEpoch,
        window: crate::QueryWindow,
        fetch_limit: usize,
    ) -> Result<Option<Vec<VertexId>>> {
        let Some(group) = self
            .supernode_group(
                cell_id,
                edge_type,
                ArtifactDirection::Out,
                vertex_id,
                read_epoch,
            )
            .await?
        else {
            return Ok(None);
        };
        let overlay = self.supernode_delta_overlay(&group, read_epoch).await?;
        self.merged_supernode_vertices(&group, &overlay, window.skip, fetch_limit)
            .await
            .map(Some)
    }

    pub async fn supernode_edge_exists(
        &self,
        cell_id: &str,
        edge_type: &str,
        src: VertexId,
        dst: VertexId,
        read_epoch: GraphEpoch,
    ) -> Result<bool> {
        if let Some(group) = self
            .supernode_group(cell_id, edge_type, ArtifactDirection::Out, src, read_epoch)
            .await?
        {
            let overlay = self.supernode_delta_overlay(&group, read_epoch).await?;
            if overlay.plus.contains(&dst) {
                return Ok(true);
            }
            if overlay.minus.contains(&dst) {
                return Ok(false);
            }
            return self.supernode_base_contains(&group, dst).await;
        }
        self.edge_exists_at(cell_id, edge_type, src, dst, read_epoch)
            .await
    }

    pub async fn supernode_intersection(
        &self,
        cell_id: &str,
        edge_type: &str,
        vertex_id: VertexId,
        candidates: &[VertexId],
        read_epoch: GraphEpoch,
    ) -> Result<Vec<VertexId>> {
        let mut result = BTreeSet::new();
        if let Some(group) = self
            .supernode_group(
                cell_id,
                edge_type,
                ArtifactDirection::Out,
                vertex_id,
                read_epoch,
            )
            .await?
        {
            let overlay = self.supernode_delta_overlay(&group, read_epoch).await?;
            for candidate in candidates.iter().copied().collect::<BTreeSet<_>>() {
                if overlay.plus.contains(&candidate)
                    || (!overlay.minus.contains(&candidate)
                        && self.supernode_base_contains(&group, candidate).await?)
                {
                    result.insert(candidate);
                }
            }
            return Ok(result.into_iter().collect());
        }
        let candidate_set: HashSet<VertexId> = candidates.iter().copied().collect();
        for dst in self
            .out_neighbors_at(cell_id, edge_type, vertex_id, read_epoch)
            .await?
        {
            if candidate_set.contains(&dst) {
                result.insert(dst);
            }
        }
        Ok(result.into_iter().collect())
    }

    async fn supernode_delta_overlay(
        &self,
        group: &SupernodeGroup,
        read_epoch: GraphEpoch,
    ) -> Result<SupernodeDeltaOverlay> {
        if read_epoch <= group.base_epoch {
            return Ok(SupernodeDeltaOverlay::default());
        }
        let watermark = self
            .delta_gc_watermark(&group.cell_id, &group.edge_type)
            .await?;
        if group.base_epoch < watermark {
            return Err(GraphError::SnapshotExpired {
                cell_id: group.cell_id.clone(),
                edge_type: group.edge_type.clone(),
                read_epoch: group.base_epoch,
                min_epoch: watermark,
            });
        }
        let mut records = self
            .owner_delta_records_between(group, DeltaKind::Plus, read_epoch)
            .await?;
        records.extend(
            self.owner_delta_records_between(group, DeltaKind::Minus, read_epoch)
                .await?,
        );
        if records.is_empty() {
            records = self
                .deltas_between(
                    &group.cell_id,
                    &group.edge_type,
                    group.base_epoch,
                    read_epoch,
                )
                .await?;
        }
        sort_deltas(&mut records);

        let mut overlay = SupernodeDeltaOverlay::default();
        for delta in records {
            let (owner, neighbor) = match group.direction {
                ArtifactDirection::Out => (delta.edge.src, delta.edge.dst),
                ArtifactDirection::In => (delta.edge.dst, delta.edge.src),
            };
            if owner != group.vertex_id {
                continue;
            }
            match delta.kind {
                DeltaKind::Plus => {
                    overlay.minus.remove(&neighbor);
                    overlay.plus.insert(neighbor);
                }
                DeltaKind::Minus => {
                    overlay.plus.remove(&neighbor);
                    overlay.minus.insert(neighbor);
                }
            }
        }
        Ok(overlay)
    }

    async fn owner_delta_records_between(
        &self,
        group: &SupernodeGroup,
        kind: DeltaKind,
        read_epoch: GraphEpoch,
    ) -> Result<Vec<DeltaRecord>> {
        if group.base_epoch == GraphEpoch::MAX {
            return Ok(Vec::new());
        }
        let prefix = crate::keys::owner_delta_prefix(
            &group.cell_id,
            kind,
            &group.edge_type,
            direction_str(group.direction),
            group.vertex_id,
        );
        let start_suffix = format!("{:020}", group.base_epoch + 1);
        let mut iter = self.scan_remote_prefix_from(&prefix, &start_suffix).await?;
        let mut records = Vec::new();
        while let Some(kv) = iter.next().await? {
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            let record = decode_delta_record(&key, &kv.value)?;
            if record.edge.epoch > read_epoch {
                break;
            }
            if record.edge.epoch > group.base_epoch {
                records.push(record);
            }
        }
        sort_deltas(&mut records);
        Ok(records)
    }

    async fn supernode_degree_with_overlay(
        &self,
        group: &SupernodeGroup,
        overlay: &SupernodeDeltaOverlay,
    ) -> Result<u64> {
        let mut degree = i128::from(group.degree);
        for vertex in &overlay.minus {
            if self.supernode_base_contains(group, *vertex).await? {
                degree -= 1;
            }
        }
        for vertex in &overlay.plus {
            if !self.supernode_base_contains(group, *vertex).await? {
                degree += 1;
            }
        }
        Ok(degree.max(0) as u64)
    }

    async fn supernode_base_contains(
        &self,
        group: &SupernodeGroup,
        vertex: VertexId,
    ) -> Result<bool> {
        if let Some(bound) = supernode_bound_for_vertex(group, vertex) {
            let Some(chunk) = self.posting_chunk(group, bound.chunk_id).await? else {
                return Err(GraphError::CorruptValue {
                    key: posting_chunk_key(
                        &group.cell_id,
                        &group.edge_type,
                        group.direction,
                        group.vertex_id,
                        group.base_epoch,
                        bound.chunk_id,
                    ),
                    reason: "missing supernode posting chunk".to_string(),
                });
            };
            return Ok(chunk.vertices.binary_search(&vertex).is_ok());
        }

        if !group.chunk_bounds.is_empty() {
            return Ok(false);
        }

        for chunk_id in 0..group.chunk_count {
            let Some(chunk) = self.posting_chunk(group, chunk_id).await? else {
                return Err(GraphError::CorruptValue {
                    key: posting_chunk_key(
                        &group.cell_id,
                        &group.edge_type,
                        group.direction,
                        group.vertex_id,
                        group.base_epoch,
                        chunk_id,
                    ),
                    reason: "missing supernode posting chunk".to_string(),
                });
            };
            let Some(first) = chunk.vertices.first() else {
                continue;
            };
            if vertex < *first {
                return Ok(false);
            }
            let Some(last) = chunk.vertices.last() else {
                continue;
            };
            if vertex <= *last {
                return Ok(chunk.vertices.binary_search(&vertex).is_ok());
            }
        }
        Ok(false)
    }

    async fn merged_supernode_vertices(
        &self,
        group: &SupernodeGroup,
        overlay: &SupernodeDeltaOverlay,
        skip: u64,
        limit: usize,
    ) -> Result<Vec<VertexId>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        if overlay.is_empty()
            && skip == 0
            && usize::try_from(group.degree)
                .map(|degree| limit >= degree)
                .unwrap_or(true)
        {
            return Ok(self
                .materialized_supernode_vertices(group)
                .await?
                .as_ref()
                .clone());
        }

        let plus: Vec<_> = overlay.plus.iter().copied().collect();
        let mut plus_idx = 0_usize;
        let mut chunk_id = 0_u64;
        let mut chunk_vertices = Vec::new();
        let mut chunk_idx = 0_usize;
        let mut pending_base = None;
        let mut skipped = 0_u64;
        let mut output = Vec::with_capacity(limit.min(GRAPH_PREALLOC_LIMIT));

        loop {
            while pending_base.is_none()
                && (chunk_idx < chunk_vertices.len() || chunk_id < group.chunk_count)
            {
                if chunk_idx >= chunk_vertices.len() {
                    let Some(chunk) = self.posting_chunk(group, chunk_id).await? else {
                        return Err(GraphError::CorruptValue {
                            key: posting_chunk_key(
                                &group.cell_id,
                                &group.edge_type,
                                group.direction,
                                group.vertex_id,
                                group.base_epoch,
                                chunk_id,
                            ),
                            reason: "missing supernode posting chunk".to_string(),
                        });
                    };
                    chunk_vertices = chunk.vertices;
                    chunk_idx = 0;
                    chunk_id += 1;
                }
                pending_base = next_live_base_vertex(&chunk_vertices, &mut chunk_idx, overlay);
            }

            let next_plus = plus.get(plus_idx).copied();
            let Some(next) = merge_next_vertex(pending_base, next_plus) else {
                break;
            };
            if next_plus == Some(next) {
                plus_idx += 1;
            }
            if pending_base == Some(next) {
                pending_base = None;
            }

            if skipped < skip {
                skipped += 1;
                continue;
            }
            output.push(next);
            if output.len() == limit {
                break;
            }
        }

        Ok(output)
    }

    async fn materialized_supernode_vertices(
        &self,
        group: &SupernodeGroup,
    ) -> Result<Arc<Vec<VertexId>>> {
        let cache_key = SupernodeCacheKey::new(
            &group.cell_id,
            &group.edge_type,
            group.direction,
            group.vertex_id,
            group.base_epoch,
        );
        if let Some(cached) = self
            .materialized_supernode_cache
            .lock()
            .await
            .get(&cache_key)
        {
            self.cache_metrics
                .record_hit(GraphCacheKind::MaterializedSupernode);
            return Ok(cached);
        }

        self.cache_metrics
            .record_miss(GraphCacheKind::MaterializedSupernode);
        let started = Instant::now();
        let chunks = self
            .supernode_chunks_range(group, 0, group.chunk_count)
            .await?;
        let capacity: usize = chunks.iter().map(|chunk| chunk.vertices.len()).sum();
        let mut vertices = Vec::with_capacity(capacity.min(GRAPH_PREALLOC_LIMIT));
        for chunk in chunks {
            vertices.extend(chunk.vertices);
        }
        let vertices = Arc::new(vertices);
        record_matrix_profile(
            matrix_profile_enabled(),
            "supernode_materialized_load",
            started.elapsed(),
            vertices.len() as u64,
        );
        let mut cache = self.materialized_supernode_cache.lock().await;
        Ok(cache
            .insert(
                cache_key,
                Arc::clone(&vertices),
                group.cell_id.clone(),
                self.cache_policy.pin_supernode_group(group),
                &self.cache_metrics,
            )
            .unwrap_or(vertices))
    }

    async fn supernode_chunks_range(
        &self,
        group: &SupernodeGroup,
        start_chunk_id: u64,
        chunk_count: u64,
    ) -> Result<Vec<PostingChunk>> {
        let end_chunk_id =
            start_chunk_id
                .checked_add(chunk_count)
                .ok_or_else(|| GraphError::CorruptValue {
                    key: supernode_group_key(group),
                    reason: "supernode chunk range overflow".to_string(),
                })?;
        let reads = (start_chunk_id..end_chunk_id).map(|chunk_id| async move {
            self.posting_chunk(group, chunk_id)
                .await?
                .ok_or_else(|| GraphError::CorruptValue {
                    key: posting_chunk_key(
                        &group.cell_id,
                        &group.edge_type,
                        group.direction,
                        group.vertex_id,
                        group.base_epoch,
                        chunk_id,
                    ),
                    reason: "missing supernode posting chunk".to_string(),
                })
        });
        let mut chunks = Vec::with_capacity(
            usize::try_from(chunk_count)
                .unwrap_or(0)
                .min(GRAPH_CHUNK_PREALLOC_LIMIT),
        );
        for chunk in join_all(reads).await {
            chunks.push(chunk?);
        }
        Ok(chunks)
    }

    pub(crate) async fn posting_chunk(
        &self,
        group: &SupernodeGroup,
        chunk_id: u64,
    ) -> Result<Option<PostingChunk>> {
        let cache_key = PostingChunkCacheKey::new(group, chunk_id);
        if let Some(cached) = self.posting_chunk_cache.lock().await.get(&cache_key) {
            self.cache_metrics.record_hit(GraphCacheKind::PostingChunk);
            return Ok(Some(cached));
        }

        self.cache_metrics.record_miss(GraphCacheKind::PostingChunk);
        let _permit = self.acquire_hydration_permit("posting_chunk").await?;
        let key = posting_chunk_key(
            &group.cell_id,
            &group.edge_type,
            group.direction,
            group.vertex_id,
            group.base_epoch,
            chunk_id,
        );
        let decoded = self
            .read_remote(&key)
            .await?
            .map(|value| decode_posting_chunk(&key, &value))
            .transpose()?;
        self.record_hydration_complete();
        if let Some(chunk) = decoded.clone() {
            self.posting_chunk_cache.lock().await.insert(
                cache_key,
                chunk,
                group.cell_id.clone(),
                self.cache_policy.pin_supernode_group(group),
                &self.cache_metrics,
            );
        }
        Ok(decoded)
    }

    async fn prefetch_supernode_chunks(&self, group: &SupernodeGroup) {
        let chunk_count = self
            .cache_policy
            .prefetch_supernode_chunks
            .min(group.chunk_count);
        if chunk_count == 0 {
            self.cache_metrics
                .prefetch_skipped
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            return;
        }
        self.cache_metrics
            .prefetch_requests
            .fetch_add(chunk_count, std::sync::atomic::Ordering::Relaxed);
        if self
            .supernode_chunks_range(group, 0, chunk_count)
            .await
            .is_err()
        {
            self.cache_metrics
                .prefetch_skipped
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
    }

    pub(crate) async fn cached_matrix_adjacency(
        &self,
        cell_id: &str,
        edge_type: &str,
        base_epoch: GraphEpoch,
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
            .insert(
                cache_key,
                Arc::clone(&adjacency),
                cell_id.to_string(),
                adjacency_edge_count(adjacency.as_ref()) >= self.cache_policy.pin_matrix_min_edges,
                &self.cache_metrics,
            )
            .unwrap_or(adjacency))
    }

    pub(crate) async fn cached_graphblas_matrix(
        &self,
        cell_id: &str,
        edge_type: &str,
        base_epoch: GraphEpoch,
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
        let started = Instant::now();
        let (compiled, compile_units) = if compact_csc_kernel_enabled() {
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
            .insert(
                cache_key,
                Arc::clone(&compiled),
                cell_id.to_string(),
                compile_units >= self.cache_policy.pin_matrix_min_edges,
                &self.cache_metrics,
            )
            .unwrap_or(compiled))
    }

    async fn compact_graphblas_csc_matrix(
        &self,
        cell_id: &str,
        edge_type: &str,
        base_epoch: GraphEpoch,
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
        base_epoch: GraphEpoch,
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
        base_epoch: GraphEpoch,
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

    async fn load_graphblas_csc_chunks_u32(
        &self,
        cell_id: &str,
        edge_type: &str,
        base_epoch: GraphEpoch,
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
        let mut adjacency: MatrixAdjacency = BTreeMap::new();
        let prefix = matrix_tile_prefix(
            &artifact.cell_id,
            &artifact.edge_type,
            artifact.base_epoch,
            ArtifactDirection::Out,
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
                || tile.direction != ArtifactDirection::Out
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
