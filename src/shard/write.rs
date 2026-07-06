use super::*;

impl GraphShard {
    pub async fn set_vertex_metadata(
        &self,
        cell_id: &str,
        vertex_id: VertexId,
        metadata: VertexMetadata,
    ) -> Result<()> {
        validate_component("cell_id", cell_id)?;
        validate_vertex_metadata(&metadata)?;
        self.ensure_write_authority(cell_id, "set_vertex_metadata")?;
        let _permit = self
            .acquire_graph_write_permit("set_vertex_metadata")
            .await?;
        let _writer = self.writer_lane(cell_id).lock().await;
        for attempt in 0..GRAPH_TXN_MAX_RETRIES {
            match self
                .set_vertex_metadata_txn(cell_id, vertex_id, metadata.clone())
                .await
            {
                Err(err)
                    if is_retryable_write_conflict(&err) && attempt + 1 < GRAPH_TXN_MAX_RETRIES =>
                {
                    self.operation_metrics
                        .write_retries
                        .fetch_add(1, Ordering::Relaxed);
                    tokio::task::yield_now().await;
                }
                Err(err @ GraphError::StaleShardLease { .. }) => {
                    self.operation_metrics
                        .stale_write_rejects
                        .fetch_add(1, Ordering::Relaxed);
                    return Err(err);
                }
                Ok(()) => {
                    self.operation_metrics
                        .write_commits
                        .fetch_add(1, Ordering::Relaxed);
                    return Ok(());
                }
                result => return result,
            }
        }
        unreachable!("transaction retry loop always returns on final attempt")
    }

    async fn set_vertex_metadata_txn(
        &self,
        cell_id: &str,
        vertex_id: VertexId,
        metadata: VertexMetadata,
    ) -> Result<()> {
        let lock = self
            .acquire_cell_write_lock(cell_id, "set_vertex_metadata")
            .await?;
        let result = self
            .set_vertex_metadata_txn_locked(cell_id, vertex_id, metadata)
            .await;
        release_cell_write_lock(lock, result).await
    }

    async fn set_vertex_metadata_txn_locked(
        &self,
        cell_id: &str,
        vertex_id: VertexId,
        metadata: VertexMetadata,
    ) -> Result<()> {
        let txn = self.db.begin(IsolationLevel::SerializableSnapshot).await?;
        self.validate_write_fence_txn(&txn, cell_id, "set_vertex_metadata")
            .await?;
        let vertex_key = keys::vertex(cell_id, vertex_id);
        let previous = match read_txn_remote(&txn, &vertex_key).await? {
            Some(value) => decode_vertex_metadata(&vertex_key, &value)?,
            None => VertexMetadata::default(),
        };
        if previous == metadata {
            return Ok(());
        }
        let epoch = next_epoch_txn(&txn, cell_id).await?;
        txn.put(keys::last_epoch(cell_id).as_bytes(), encode_u64(epoch))?;
        apply_vertex_metadata_update_txn(&txn, cell_id, vertex_id, &previous, &metadata, epoch)?;
        commit_txn_strict(txn, self.await_durable_writes).await
    }

    pub async fn set_edge_metadata(
        &self,
        cell_id: &str,
        edge_type: &str,
        src: VertexId,
        dst: VertexId,
        metadata: EdgeMetadata,
    ) -> Result<bool> {
        validate_component("cell_id", cell_id)?;
        validate_component("edge_type", edge_type)?;
        validate_edge_metadata(&metadata)?;
        self.ensure_write_authority(cell_id, "set_edge_metadata")?;
        let _permit = self.acquire_graph_write_permit("set_edge_metadata").await?;
        let _writer = self.writer_lane(cell_id).lock().await;
        for attempt in 0..GRAPH_TXN_MAX_RETRIES {
            match self
                .set_edge_metadata_txn(cell_id, edge_type, src, dst, metadata.clone())
                .await
            {
                Err(err)
                    if is_retryable_write_conflict(&err) && attempt + 1 < GRAPH_TXN_MAX_RETRIES =>
                {
                    self.operation_metrics
                        .write_retries
                        .fetch_add(1, Ordering::Relaxed);
                    tokio::task::yield_now().await;
                }
                Err(err @ GraphError::StaleShardLease { .. }) => {
                    self.operation_metrics
                        .stale_write_rejects
                        .fetch_add(1, Ordering::Relaxed);
                    return Err(err);
                }
                Ok(result) => {
                    self.operation_metrics
                        .write_commits
                        .fetch_add(1, Ordering::Relaxed);
                    return Ok(result);
                }
                result => return result,
            }
        }
        unreachable!("transaction retry loop always returns on final attempt")
    }

    async fn set_edge_metadata_txn(
        &self,
        cell_id: &str,
        edge_type: &str,
        src: VertexId,
        dst: VertexId,
        metadata: EdgeMetadata,
    ) -> Result<bool> {
        let lock = self
            .acquire_cell_write_lock(cell_id, "set_edge_metadata")
            .await?;
        let result = self
            .set_edge_metadata_txn_locked(cell_id, edge_type, src, dst, metadata)
            .await;
        release_cell_write_lock(lock, result).await
    }

    async fn set_edge_metadata_txn_locked(
        &self,
        cell_id: &str,
        edge_type: &str,
        src: VertexId,
        dst: VertexId,
        metadata: EdgeMetadata,
    ) -> Result<bool> {
        let txn = self.db.begin(IsolationLevel::SerializableSnapshot).await?;
        self.validate_write_fence_txn(&txn, cell_id, "set_edge_metadata")
            .await?;
        let current_epoch = read_counter_txn(&txn, &keys::last_epoch(cell_id)).await?;
        if edge_epoch_at_txn(&txn, cell_id, edge_type, src, dst, current_epoch)
            .await?
            .is_none()
        {
            return Err(GraphError::UnsupportedQuery {
                dialect: "GraphQuery",
                feature: "cannot set metadata for a missing edge".to_string(),
            });
        }
        let edge_metadata_key = keys::edge_metadata(cell_id, edge_type, src, dst);
        let previous = match read_txn_remote(&txn, &edge_metadata_key).await? {
            Some(value) => decode_edge_metadata(&edge_metadata_key, &value)?,
            None => EdgeMetadata::default(),
        };
        if previous == metadata {
            return Ok(false);
        }
        let epoch = next_epoch_txn(&txn, cell_id).await?;
        txn.put(keys::last_epoch(cell_id).as_bytes(), encode_u64(epoch))?;
        apply_edge_metadata_update_txn(
            &txn,
            EdgeMetadataTarget {
                cell_id,
                edge_type,
                src,
                dst,
            },
            &previous,
            &metadata,
            epoch,
        )?;
        commit_txn_strict(txn, self.await_durable_writes).await?;
        Ok(true)
    }

    pub async fn write_edge(&self, mutation: EdgeMutation) -> Result<CommitResult> {
        validate_component("cell_id", &mutation.cell_id)?;
        validate_component("edge_type", &mutation.edge_type)?;
        validate_component("idempotency_key", &mutation.idempotency_key)?;
        self.ensure_write_authority(&mutation.cell_id, "write_edge")?;

        let _permit = self.acquire_graph_write_permit("write_edge").await?;
        let _writer = self.writer_lane(&mutation.cell_id).lock().await;
        for attempt in 0..GRAPH_TXN_MAX_RETRIES {
            match self.write_edge_txn(&mutation).await {
                Err(err)
                    if is_retryable_write_conflict(&err) && attempt + 1 < GRAPH_TXN_MAX_RETRIES =>
                {
                    self.operation_metrics
                        .write_retries
                        .fetch_add(1, Ordering::Relaxed);
                    tokio::task::yield_now().await;
                }
                Err(err @ GraphError::StaleShardLease { .. }) => {
                    self.operation_metrics
                        .stale_write_rejects
                        .fetch_add(1, Ordering::Relaxed);
                    return Err(err);
                }
                Ok(result) => {
                    self.operation_metrics
                        .write_commits
                        .fetch_add(1, Ordering::Relaxed);
                    return Ok(result);
                }
                result => return result,
            }
        }
        unreachable!("transaction retry loop always returns on final attempt")
    }

    pub(crate) async fn write_edge_txn(&self, mutation: &EdgeMutation) -> Result<CommitResult> {
        let lock = self
            .acquire_cell_write_lock(&mutation.cell_id, "write_edge")
            .await?;
        let result = self.write_edge_txn_locked(mutation).await;
        release_cell_write_lock(lock, result).await
    }

    async fn write_edge_txn_locked(&self, mutation: &EdgeMutation) -> Result<CommitResult> {
        self.write_edge_txn_locked_with_metadata(
            mutation,
            &[],
            &EdgeMetadata::default(),
            "write_edge",
        )
        .await
    }

    pub async fn write_edge_with_vertex_metadata(
        &self,
        mutation: EdgeMutation,
        src_metadata: VertexMetadata,
        dst_metadata: VertexMetadata,
    ) -> Result<CommitResult> {
        validate_component("cell_id", &mutation.cell_id)?;
        validate_component("edge_type", &mutation.edge_type)?;
        validate_component("idempotency_key", &mutation.idempotency_key)?;
        validate_vertex_metadata(&src_metadata)?;
        validate_vertex_metadata(&dst_metadata)?;
        self.ensure_write_authority(&mutation.cell_id, "write_edge_with_vertex_metadata")?;

        let metadata_updates = coalesce_vertex_metadata_updates([
            (mutation.src, src_metadata),
            (mutation.dst, dst_metadata),
        ])?;
        let _permit = self
            .acquire_graph_write_permit("write_edge_with_vertex_metadata")
            .await?;
        let _writer = self.writer_lane(&mutation.cell_id).lock().await;
        for attempt in 0..GRAPH_TXN_MAX_RETRIES {
            match self
                .write_edge_with_vertex_metadata_txn(&mutation, &metadata_updates)
                .await
            {
                Err(err)
                    if is_retryable_write_conflict(&err) && attempt + 1 < GRAPH_TXN_MAX_RETRIES =>
                {
                    self.operation_metrics
                        .write_retries
                        .fetch_add(1, Ordering::Relaxed);
                    tokio::task::yield_now().await;
                }
                Err(err @ GraphError::StaleShardLease { .. }) => {
                    self.operation_metrics
                        .stale_write_rejects
                        .fetch_add(1, Ordering::Relaxed);
                    return Err(err);
                }
                Ok(result) => {
                    self.operation_metrics
                        .write_commits
                        .fetch_add(1, Ordering::Relaxed);
                    return Ok(result);
                }
                result => return result,
            }
        }
        unreachable!("transaction retry loop always returns on final attempt")
    }

    async fn write_edge_with_vertex_metadata_txn(
        &self,
        mutation: &EdgeMutation,
        metadata_updates: &[(VertexId, VertexMetadata)],
    ) -> Result<CommitResult> {
        let lock = self
            .acquire_cell_write_lock(&mutation.cell_id, "write_edge_with_vertex_metadata")
            .await?;
        let result = self
            .write_edge_txn_locked_with_metadata(
                mutation,
                metadata_updates,
                &EdgeMetadata::default(),
                "write_edge_with_vertex_metadata",
            )
            .await;
        release_cell_write_lock(lock, result).await
    }

    pub async fn write_edge_with_full_metadata(
        &self,
        mutation: EdgeMutation,
        src_metadata: VertexMetadata,
        dst_metadata: VertexMetadata,
        edge_metadata: EdgeMetadata,
    ) -> Result<CommitResult> {
        validate_component("cell_id", &mutation.cell_id)?;
        validate_component("edge_type", &mutation.edge_type)?;
        validate_component("idempotency_key", &mutation.idempotency_key)?;
        validate_vertex_metadata(&src_metadata)?;
        validate_vertex_metadata(&dst_metadata)?;
        validate_edge_metadata(&edge_metadata)?;
        self.ensure_write_authority(&mutation.cell_id, "write_edge_with_full_metadata")?;

        let metadata_updates = coalesce_vertex_metadata_updates([
            (mutation.src, src_metadata),
            (mutation.dst, dst_metadata),
        ])?;
        let _permit = self
            .acquire_graph_write_permit("write_edge_with_full_metadata")
            .await?;
        let _writer = self.writer_lane(&mutation.cell_id).lock().await;
        for attempt in 0..GRAPH_TXN_MAX_RETRIES {
            match self
                .write_edge_with_full_metadata_txn(&mutation, &metadata_updates, &edge_metadata)
                .await
            {
                Err(err)
                    if is_retryable_write_conflict(&err) && attempt + 1 < GRAPH_TXN_MAX_RETRIES =>
                {
                    self.operation_metrics
                        .write_retries
                        .fetch_add(1, Ordering::Relaxed);
                    tokio::task::yield_now().await;
                }
                Err(err @ GraphError::StaleShardLease { .. }) => {
                    self.operation_metrics
                        .stale_write_rejects
                        .fetch_add(1, Ordering::Relaxed);
                    return Err(err);
                }
                Ok(result) => {
                    self.operation_metrics
                        .write_commits
                        .fetch_add(1, Ordering::Relaxed);
                    return Ok(result);
                }
                result => return result,
            }
        }
        unreachable!("transaction retry loop always returns on final attempt")
    }

    async fn write_edge_with_full_metadata_txn(
        &self,
        mutation: &EdgeMutation,
        metadata_updates: &[(VertexId, VertexMetadata)],
        edge_metadata: &EdgeMetadata,
    ) -> Result<CommitResult> {
        let lock = self
            .acquire_cell_write_lock(&mutation.cell_id, "write_edge_with_full_metadata")
            .await?;
        let result = self
            .write_edge_txn_locked_with_metadata(
                mutation,
                metadata_updates,
                edge_metadata,
                "write_edge_with_full_metadata",
            )
            .await;
        release_cell_write_lock(lock, result).await
    }

    async fn write_edge_txn_locked_with_metadata(
        &self,
        mutation: &EdgeMutation,
        metadata_updates: &[(VertexId, VertexMetadata)],
        edge_metadata: &EdgeMetadata,
        operation: &'static str,
    ) -> Result<CommitResult> {
        let txn = self.db.begin(IsolationLevel::SerializableSnapshot).await?;
        self.validate_write_fence_txn(&txn, &mutation.cell_id, operation)
            .await?;
        let idem_key = keys::idempotency(&mutation.cell_id, "create", &mutation.idempotency_key);

        if let Some(value) = read_txn_remote(&txn, &idem_key).await? {
            return decode_commit_idempotency(&idem_key, mutation, &value);
        }

        let current_epoch = read_counter_txn(&txn, &keys::last_epoch(&mutation.cell_id)).await?;
        let existing_edge_epoch = edge_epoch_at_txn(
            &txn,
            &mutation.cell_id,
            &mutation.edge_type,
            mutation.src,
            mutation.dst,
            current_epoch,
        )
        .await?;

        let mut changed_metadata = Vec::new();
        for (vertex_id, requested) in metadata_updates {
            let vertex_key = keys::vertex(&mutation.cell_id, *vertex_id);
            let previous = match read_txn_remote(&txn, &vertex_key).await? {
                Some(value) => decode_vertex_metadata(&vertex_key, &value)?,
                None => VertexMetadata::default(),
            };
            let next = merge_vertex_metadata(&previous, requested);
            if previous != next {
                changed_metadata.push((*vertex_id, previous, next));
            }
        }
        let edge_metadata_key = keys::edge_metadata(
            &mutation.cell_id,
            &mutation.edge_type,
            mutation.src,
            mutation.dst,
        );
        let previous_edge_metadata = match read_txn_remote(&txn, &edge_metadata_key).await? {
            Some(value) => decode_edge_metadata(&edge_metadata_key, &value)?,
            None => EdgeMetadata::default(),
        };
        let next_edge_metadata = merge_edge_metadata(&previous_edge_metadata, edge_metadata);
        let edge_metadata_changed = previous_edge_metadata != next_edge_metadata;

        if let Some(existing_epoch) = existing_edge_epoch {
            if changed_metadata.is_empty() && !edge_metadata_changed {
                let result = CommitResult {
                    epoch: existing_epoch,
                    already_existed: true,
                };
                txn.put(
                    idem_key.as_bytes(),
                    encode_commit_idempotency(mutation, &result),
                )?;
                commit_txn_strict(txn, self.await_durable_writes).await?;
                return Ok(result);
            }
        }

        let epoch = current_epoch
            .checked_add(1)
            .ok_or_else(|| GraphError::CorruptValue {
                key: keys::last_epoch(&mutation.cell_id),
                reason: "epoch overflow".to_string(),
            })?;
        let result = CommitResult {
            epoch,
            already_existed: existing_edge_epoch.is_some(),
        };
        txn.put(
            keys::last_epoch(&mutation.cell_id).as_bytes(),
            encode_u64(epoch),
        )?;
        for (vertex_id, previous, next) in &changed_metadata {
            apply_vertex_metadata_update_txn(
                &txn,
                &mutation.cell_id,
                *vertex_id,
                previous,
                next,
                epoch,
            )?;
        }
        if edge_metadata_changed {
            apply_edge_metadata_update_txn(
                &txn,
                EdgeMetadataTarget {
                    cell_id: &mutation.cell_id,
                    edge_type: &mutation.edge_type,
                    src: mutation.src,
                    dst: mutation.dst,
                },
                &previous_edge_metadata,
                &next_edge_metadata,
                epoch,
            )?;
        }
        if existing_edge_epoch.is_some() {
            txn.put(
                idem_key.as_bytes(),
                encode_commit_idempotency(mutation, &result),
            )?;
            commit_txn_strict(txn, self.await_durable_writes).await?;
            return Ok(result);
        }

        let record = EdgeRecord {
            cell_id: mutation.cell_id.clone(),
            edge_type: mutation.edge_type.clone(),
            src: mutation.src,
            dst: mutation.dst,
            epoch,
        };
        let edge_value = encode_edge_record(&record);
        let delta_value = encode_delta_record(&DeltaRecord {
            kind: DeltaKind::Plus,
            edge: record.clone(),
        });
        let out_degree_key = keys::degree_out(&mutation.cell_id, &mutation.edge_type, mutation.src);
        let out_degree = read_counter_txn(&txn, &out_degree_key).await? + 1;
        let in_degree = if self.writes_reverse_index() {
            let in_degree_key =
                keys::degree_in(&mutation.cell_id, &mutation.edge_type, mutation.dst);
            let in_degree = read_counter_txn(&txn, &in_degree_key).await? + 1;
            Some((in_degree_key, in_degree))
        } else {
            None
        };

        txn.put(
            keys::out_edge(
                &mutation.cell_id,
                &mutation.edge_type,
                mutation.src,
                mutation.dst,
            )
            .as_bytes(),
            &edge_value,
        )?;
        if self.writes_reverse_index() {
            txn.put(
                keys::in_edge(
                    &mutation.cell_id,
                    &mutation.edge_type,
                    mutation.dst,
                    mutation.src,
                )
                .as_bytes(),
                &edge_value,
            )?;
        }
        txn.put(out_degree_key.as_bytes(), encode_u64(out_degree))?;
        if let Some((in_degree_key, in_degree)) = in_degree {
            txn.put(in_degree_key.as_bytes(), encode_u64(in_degree))?;
        }
        txn.put(
            keys::outbox(
                &mutation.cell_id,
                epoch,
                DeltaKind::Plus,
                &mutation.edge_type,
                mutation.src,
                mutation.dst,
            )
            .as_bytes(),
            &delta_value,
        )?;
        txn.put(
            idem_key.as_bytes(),
            encode_commit_idempotency(mutation, &result),
        )?;

        commit_txn_strict(txn, self.await_durable_writes).await?;
        Ok(result)
    }

    pub async fn delete_edge(&self, mutation: EdgeMutation) -> Result<DeleteResult> {
        validate_component("cell_id", &mutation.cell_id)?;
        validate_component("edge_type", &mutation.edge_type)?;
        validate_component("idempotency_key", &mutation.idempotency_key)?;
        self.ensure_write_authority(&mutation.cell_id, "delete_edge")?;

        let _permit = self.acquire_graph_write_permit("delete_edge").await?;
        let _writer = self.writer_lane(&mutation.cell_id).lock().await;
        for attempt in 0..GRAPH_TXN_MAX_RETRIES {
            match self.delete_edge_txn(&mutation).await {
                Err(err)
                    if is_retryable_write_conflict(&err) && attempt + 1 < GRAPH_TXN_MAX_RETRIES =>
                {
                    self.operation_metrics
                        .write_retries
                        .fetch_add(1, Ordering::Relaxed);
                    tokio::task::yield_now().await;
                }
                Err(err @ GraphError::StaleShardLease { .. }) => {
                    self.operation_metrics
                        .stale_write_rejects
                        .fetch_add(1, Ordering::Relaxed);
                    return Err(err);
                }
                Ok(result) => {
                    self.operation_metrics
                        .write_commits
                        .fetch_add(1, Ordering::Relaxed);
                    return Ok(result);
                }
                result => return result,
            }
        }
        unreachable!("transaction retry loop always returns on final attempt")
    }

    pub(crate) async fn delete_edge_txn(&self, mutation: &EdgeMutation) -> Result<DeleteResult> {
        let lock = self
            .acquire_cell_write_lock(&mutation.cell_id, "delete_edge")
            .await?;
        let result = self.delete_edge_txn_locked(mutation).await;
        release_cell_write_lock(lock, result).await
    }

    async fn delete_edge_txn_locked(&self, mutation: &EdgeMutation) -> Result<DeleteResult> {
        let txn = self.db.begin(IsolationLevel::SerializableSnapshot).await?;
        self.validate_write_fence_txn(&txn, &mutation.cell_id, "delete_edge")
            .await?;
        let idem_key = keys::idempotency(&mutation.cell_id, "delete", &mutation.idempotency_key);

        if let Some(value) = read_txn_remote(&txn, &idem_key).await? {
            return decode_delete_idempotency(&idem_key, mutation, &value);
        }

        let canonical_key = keys::edge(
            &mutation.cell_id,
            &mutation.edge_type,
            mutation.src,
            mutation.dst,
        );
        let edge_key = keys::out_edge(
            &mutation.cell_id,
            &mutation.edge_type,
            mutation.src,
            mutation.dst,
        );

        let Some(existing) = read_txn_remote(&txn, &edge_key).await? else {
            let current_epoch =
                read_counter_txn(&txn, &keys::last_epoch(&mutation.cell_id)).await?;
            let segment_edge = if self.writes_reverse_index() {
                None
            } else {
                self.out_segment_edge_record_at(
                    &mutation.cell_id,
                    &mutation.edge_type,
                    mutation.src,
                    mutation.dst,
                    current_epoch,
                )
                .await?
            };
            let Some(segment_edge) = segment_edge else {
                let result = DeleteResult {
                    epoch: current_epoch,
                    deleted: false,
                };
                txn.put(
                    idem_key.as_bytes(),
                    encode_delete_idempotency(mutation, &result),
                )?;
                commit_txn_strict(txn, self.await_durable_writes).await?;
                return Ok(result);
            };
            let tombstone_key = keys::out_segment_tombstone(
                &mutation.cell_id,
                &mutation.edge_type,
                mutation.src,
                mutation.dst,
            );
            if let Some(value) = read_txn_remote(&txn, &tombstone_key).await? {
                let tombstone_epoch = decode_u64(&tombstone_key, &value)?;
                if !segment_edge_visible(segment_edge.epoch, Some(tombstone_epoch)) {
                    let result = DeleteResult {
                        epoch: current_epoch,
                        deleted: false,
                    };
                    txn.put(
                        idem_key.as_bytes(),
                        encode_delete_idempotency(mutation, &result),
                    )?;
                    commit_txn_strict(txn, self.await_durable_writes).await?;
                    return Ok(result);
                }
            }
            let epoch = current_epoch
                .checked_add(1)
                .ok_or_else(|| GraphError::CorruptValue {
                    key: keys::last_epoch(&mutation.cell_id),
                    reason: "epoch overflow".to_string(),
                })?;
            let result = DeleteResult {
                epoch,
                deleted: true,
            };
            let record = EdgeRecord {
                cell_id: mutation.cell_id.clone(),
                edge_type: mutation.edge_type.clone(),
                src: mutation.src,
                dst: mutation.dst,
                epoch,
            };
            let delta_value = encode_delta_record(&DeltaRecord {
                kind: DeltaKind::Minus,
                edge: record,
            });
            let out_degree_key =
                keys::degree_out(&mutation.cell_id, &mutation.edge_type, mutation.src);
            let out_degree = read_counter_txn(&txn, &out_degree_key)
                .await?
                .saturating_sub(1);
            let edge_metadata_key = keys::edge_metadata(
                &mutation.cell_id,
                &mutation.edge_type,
                mutation.src,
                mutation.dst,
            );
            let previous_edge_metadata = match read_txn_remote(&txn, &edge_metadata_key).await? {
                Some(value) => decode_edge_metadata(&edge_metadata_key, &value)?,
                None => EdgeMetadata::default(),
            };

            txn.put(
                keys::last_epoch(&mutation.cell_id).as_bytes(),
                encode_u64(epoch),
            )?;
            if !previous_edge_metadata.properties.is_empty() {
                apply_edge_metadata_update_txn(
                    &txn,
                    EdgeMetadataTarget {
                        cell_id: &mutation.cell_id,
                        edge_type: &mutation.edge_type,
                        src: mutation.src,
                        dst: mutation.dst,
                    },
                    &previous_edge_metadata,
                    &EdgeMetadata::default(),
                    epoch,
                )?;
            }
            txn.put(tombstone_key.as_bytes(), encode_u64(epoch))?;
            txn.put(out_degree_key.as_bytes(), encode_u64(out_degree))?;
            txn.put(
                keys::outbox(
                    &mutation.cell_id,
                    epoch,
                    DeltaKind::Minus,
                    &mutation.edge_type,
                    mutation.src,
                    mutation.dst,
                )
                .as_bytes(),
                &delta_value,
            )?;
            txn.put(
                idem_key.as_bytes(),
                encode_delete_idempotency(mutation, &result),
            )?;
            commit_txn_strict(txn, self.await_durable_writes).await?;
            return Ok(result);
        };

        decode_edge_record(&edge_key, &existing)?;
        let epoch = next_epoch_txn(&txn, &mutation.cell_id).await?;
        let record = EdgeRecord {
            cell_id: mutation.cell_id.clone(),
            edge_type: mutation.edge_type.clone(),
            src: mutation.src,
            dst: mutation.dst,
            epoch,
        };
        let result = DeleteResult {
            epoch,
            deleted: true,
        };
        let delta_value = encode_delta_record(&DeltaRecord {
            kind: DeltaKind::Minus,
            edge: record.clone(),
        });

        let out_degree_key = keys::degree_out(&mutation.cell_id, &mutation.edge_type, mutation.src);
        let out_degree = read_counter_txn(&txn, &out_degree_key)
            .await?
            .saturating_sub(1);
        let in_degree = if self.writes_reverse_index() {
            let in_degree_key =
                keys::degree_in(&mutation.cell_id, &mutation.edge_type, mutation.dst);
            let in_degree = read_counter_txn(&txn, &in_degree_key)
                .await?
                .saturating_sub(1);
            Some((in_degree_key, in_degree))
        } else {
            None
        };
        let edge_metadata_key = keys::edge_metadata(
            &mutation.cell_id,
            &mutation.edge_type,
            mutation.src,
            mutation.dst,
        );
        let previous_edge_metadata = match read_txn_remote(&txn, &edge_metadata_key).await? {
            Some(value) => decode_edge_metadata(&edge_metadata_key, &value)?,
            None => EdgeMetadata::default(),
        };

        txn.put(
            keys::last_epoch(&mutation.cell_id).as_bytes(),
            encode_u64(epoch),
        )?;
        if !previous_edge_metadata.properties.is_empty() {
            apply_edge_metadata_update_txn(
                &txn,
                EdgeMetadataTarget {
                    cell_id: &mutation.cell_id,
                    edge_type: &mutation.edge_type,
                    src: mutation.src,
                    dst: mutation.dst,
                },
                &previous_edge_metadata,
                &EdgeMetadata::default(),
                epoch,
            )?;
        }
        txn.delete(canonical_key.as_bytes())?;
        txn.delete(
            keys::out_edge(
                &mutation.cell_id,
                &mutation.edge_type,
                mutation.src,
                mutation.dst,
            )
            .as_bytes(),
        )?;
        txn.delete(
            keys::in_edge(
                &mutation.cell_id,
                &mutation.edge_type,
                mutation.dst,
                mutation.src,
            )
            .as_bytes(),
        )?;
        txn.put(out_degree_key.as_bytes(), encode_u64(out_degree))?;
        if let Some((in_degree_key, in_degree)) = in_degree {
            txn.put(in_degree_key.as_bytes(), encode_u64(in_degree))?;
        }
        txn.put(
            keys::outbox(
                &mutation.cell_id,
                epoch,
                DeltaKind::Minus,
                &mutation.edge_type,
                mutation.src,
                mutation.dst,
            )
            .as_bytes(),
            &delta_value,
        )?;
        txn.put(
            idem_key.as_bytes(),
            encode_delete_idempotency(mutation, &result),
        )?;

        commit_txn_strict(txn, self.await_durable_writes).await?;
        Ok(result)
    }

    pub async fn bulk_import_edges(
        &self,
        cell_id: &str,
        edge_type: &str,
        edges: impl IntoIterator<Item = (VertexId, VertexId)>,
        idempotency_key: &str,
    ) -> Result<BulkImportResult> {
        self.bulk_import_edges_with_options(
            cell_id,
            edge_type,
            edges,
            idempotency_key,
            BulkImportOptions::default(),
        )
        .await
    }

    pub async fn bulk_append_edges_trusted(
        &self,
        cell_id: &str,
        edge_type: &str,
        edges: impl IntoIterator<Item = (VertexId, VertexId)>,
        idempotency_key: &str,
    ) -> Result<BulkImportResult> {
        self.bulk_append_edges_trusted_bounded(
            cell_id,
            edge_type,
            edges,
            idempotency_key,
            DEFAULT_TRUSTED_APPEND_CHUNK_EDGES,
        )
        .await
    }

    pub async fn bulk_append_edges_trusted_bounded(
        &self,
        cell_id: &str,
        edge_type: &str,
        edges: impl IntoIterator<Item = (VertexId, VertexId)>,
        idempotency_key: &str,
        max_edges_per_commit: usize,
    ) -> Result<BulkImportResult> {
        validate_component("cell_id", cell_id)?;
        validate_component("edge_type", edge_type)?;
        validate_component("idempotency_key", idempotency_key)?;
        if max_edges_per_commit == 0 {
            return Err(GraphError::CorruptValue {
                key: "trusted_append_chunk_size".to_string(),
                reason: "chunk size must be greater than zero".to_string(),
            });
        }
        let edges: Vec<_> = edges.into_iter().collect();
        if edges.len() > max_edges_per_commit {
            return self
                .bulk_import_edges_chunked_with_options(
                    cell_id,
                    edge_type,
                    edges,
                    idempotency_key,
                    max_edges_per_commit,
                    BulkImportOptions::checked_batch_append(),
                )
                .await;
        }
        self.bulk_import_edges_with_options(
            cell_id,
            edge_type,
            edges,
            idempotency_key,
            BulkImportOptions::checked_batch_append(),
        )
        .await
    }

    pub async fn bulk_append_supernode_segment_trusted(
        &self,
        cell_id: &str,
        edge_type: &str,
        src: VertexId,
        dsts: impl IntoIterator<Item = VertexId>,
        idempotency_key: &str,
    ) -> Result<BulkImportResult> {
        validate_component("cell_id", cell_id)?;
        validate_component("edge_type", edge_type)?;
        validate_component("idempotency_key", idempotency_key)?;
        if self.writes_reverse_index() {
            return Err(GraphError::UnsupportedQuery {
                dialect: "GraphWrite",
                feature: "segment trusted append requires outbound-only index policy".to_string(),
            });
        }
        self.ensure_write_authority(cell_id, "bulk_append_supernode_segment_trusted")?;

        let mut dsts: Vec<_> = dsts.into_iter().collect();
        ensure_limit(
            "bulk_append_supernode_segment_trusted",
            dsts.len() as u64,
            self.limits.max_bulk_import_edges as u64,
        )?;
        dsts.sort_unstable();
        dsts.dedup();
        let edges: Vec<_> = dsts.iter().copied().map(|dst| (src, dst)).collect();
        let fingerprint = bulk_import_fingerprint(cell_id, edge_type, &edges);

        let _permit = self
            .acquire_graph_write_permit("bulk_append_supernode_segment_trusted")
            .await?;
        let _writer = self.writer_lane(cell_id).lock().await;
        for attempt in 0..GRAPH_TXN_MAX_RETRIES {
            match self
                .bulk_append_supernode_segment_trusted_txn(
                    cell_id,
                    edge_type,
                    src,
                    &dsts,
                    idempotency_key,
                    fingerprint,
                )
                .await
            {
                Err(err)
                    if is_retryable_write_conflict(&err) && attempt + 1 < GRAPH_TXN_MAX_RETRIES =>
                {
                    self.operation_metrics
                        .write_retries
                        .fetch_add(1, Ordering::Relaxed);
                    tokio::task::yield_now().await;
                }
                Err(err @ GraphError::StaleShardLease { .. }) => {
                    self.operation_metrics
                        .stale_write_rejects
                        .fetch_add(1, Ordering::Relaxed);
                    return Err(err);
                }
                Ok(result) => {
                    self.operation_metrics
                        .write_commits
                        .fetch_add(1, Ordering::Relaxed);
                    return Ok(result);
                }
                result => return result,
            }
        }
        unreachable!("transaction retry loop always returns on final attempt")
    }

    pub async fn bulk_import_edges_with_options(
        &self,
        cell_id: &str,
        edge_type: &str,
        edges: impl IntoIterator<Item = (VertexId, VertexId)>,
        idempotency_key: &str,
        options: BulkImportOptions,
    ) -> Result<BulkImportResult> {
        validate_component("cell_id", cell_id)?;
        validate_component("edge_type", edge_type)?;
        validate_component("idempotency_key", idempotency_key)?;
        self.ensure_write_authority(cell_id, "bulk_import_edges")?;

        let mut edges: Vec<_> = edges.into_iter().collect();
        ensure_limit(
            "bulk_import_edges",
            edges.len() as u64,
            self.limits.max_bulk_import_edges as u64,
        )?;
        edges.sort_unstable();
        edges.dedup();
        let fingerprint = bulk_import_fingerprint(cell_id, edge_type, &edges);

        let _permit = self.acquire_graph_write_permit("bulk_import_edges").await?;
        let _writer = self.writer_lane(cell_id).lock().await;
        for attempt in 0..GRAPH_TXN_MAX_RETRIES {
            match self
                .bulk_import_edges_txn(
                    cell_id,
                    edge_type,
                    &edges,
                    idempotency_key,
                    fingerprint,
                    options,
                )
                .await
            {
                Err(err)
                    if is_retryable_write_conflict(&err) && attempt + 1 < GRAPH_TXN_MAX_RETRIES =>
                {
                    self.operation_metrics
                        .write_retries
                        .fetch_add(1, Ordering::Relaxed);
                    tokio::task::yield_now().await;
                }
                Err(err @ GraphError::StaleShardLease { .. }) => {
                    self.operation_metrics
                        .stale_write_rejects
                        .fetch_add(1, Ordering::Relaxed);
                    return Err(err);
                }
                Ok(result) => {
                    self.operation_metrics
                        .write_commits
                        .fetch_add(1, Ordering::Relaxed);
                    return Ok(result);
                }
                result => return result,
            }
        }
        unreachable!("transaction retry loop always returns on final attempt")
    }

    pub async fn write_edge_mutations_batch(
        &self,
        cell_id: &str,
        mutations: impl IntoIterator<Item = EdgeMutation>,
    ) -> Result<EdgeMutationBatchResult> {
        validate_component("cell_id", cell_id)?;
        self.ensure_write_authority(cell_id, "write_edge_mutations_batch")?;

        let mutations: Vec<_> = mutations.into_iter().collect();
        if mutations.is_empty() {
            let epoch = self.current_epoch(cell_id).await?;
            return Ok(EdgeMutationBatchResult {
                start_epoch: epoch,
                end_epoch: epoch,
                inserted: 0,
                already_existed: 0,
                results: Vec::new(),
            });
        }
        ensure_limit(
            "write_edge_mutations_batch",
            mutations.len() as u64,
            self.limits.max_bulk_import_edges as u64,
        )?;
        for mutation in &mutations {
            validate_component("cell_id", &mutation.cell_id)?;
            validate_component("edge_type", &mutation.edge_type)?;
            validate_component("idempotency_key", &mutation.idempotency_key)?;
            if mutation.cell_id != cell_id {
                return Err(GraphError::CorruptValue {
                    key: format!("cell/{cell_id}/write_edge_mutations_batch"),
                    reason: format!(
                        "batch contains mutation for different cell {}",
                        mutation.cell_id
                    ),
                });
            }
        }

        let _permit = self
            .acquire_graph_write_permit("write_edge_mutations_batch")
            .await?;
        let _writer = self.writer_lane(cell_id).lock().await;
        for attempt in 0..GRAPH_TXN_MAX_RETRIES {
            match self
                .write_edge_mutations_batch_txn(
                    cell_id,
                    &mutations,
                    "write_edge_mutations_batch",
                    None,
                )
                .await
            {
                Err(err)
                    if is_retryable_write_conflict(&err) && attempt + 1 < GRAPH_TXN_MAX_RETRIES =>
                {
                    self.operation_metrics
                        .write_retries
                        .fetch_add(1, Ordering::Relaxed);
                    tokio::task::yield_now().await;
                }
                Err(err @ GraphError::StaleShardLease { .. }) => {
                    self.operation_metrics
                        .stale_write_rejects
                        .fetch_add(1, Ordering::Relaxed);
                    return Err(err);
                }
                Ok(result) => {
                    self.operation_metrics
                        .write_commits
                        .fetch_add(1, Ordering::Relaxed);
                    return Ok(result);
                }
                result => return result,
            }
        }
        unreachable!("transaction retry loop always returns on final attempt")
    }

    pub async fn ingest_edge_mutations(
        &self,
        cell_id: &str,
        mutations: impl IntoIterator<Item = EdgeMutation>,
        options: EdgeIngestOptions,
    ) -> Result<EdgeIngestResult> {
        validate_component("cell_id", cell_id)?;
        if options.batch_size == 0 {
            return Err(GraphError::CorruptValue {
                key: "edge_ingest_batch_size".to_string(),
                reason: "batch size must be greater than zero".to_string(),
            });
        }
        if self.limits.max_bulk_import_edges == 0 {
            return Err(GraphError::AdmissionRejected {
                operation: "ingest_edge_mutations",
                actual: 1,
                limit: 0,
            });
        }

        let batch_size = options.batch_size.min(self.limits.max_bulk_import_edges);
        let mut chunk = Vec::with_capacity(batch_size);
        let mut start_epoch = None;
        let mut end_epoch = self.current_epoch(cell_id).await?;
        let mut inserted = 0_u64;
        let mut already_existed = 0_u64;
        let mut batches = 0_u64;
        let mut mutations_seen = 0_u64;

        for mutation in mutations {
            mutations_seen = mutations_seen.saturating_add(1);
            chunk.push(mutation);
            if chunk.len() == batch_size {
                let result = self
                    .write_edge_mutations_batch(cell_id, std::mem::take(&mut chunk))
                    .await?;
                merge_ingest_batch(
                    &result,
                    &mut start_epoch,
                    &mut end_epoch,
                    &mut inserted,
                    &mut already_existed,
                    &mut batches,
                );
                chunk = Vec::with_capacity(batch_size);
            }
        }
        if !chunk.is_empty() {
            let result = self.write_edge_mutations_batch(cell_id, chunk).await?;
            merge_ingest_batch(
                &result,
                &mut start_epoch,
                &mut end_epoch,
                &mut inserted,
                &mut already_existed,
                &mut batches,
            );
        }

        Ok(EdgeIngestResult {
            start_epoch: start_epoch.unwrap_or(end_epoch),
            end_epoch,
            inserted,
            already_existed,
            batches,
            mutations: mutations_seen,
        })
    }

    pub async fn append_edge_mutation_log(
        &self,
        cell_id: &str,
        batch_id: &str,
        mutations: impl IntoIterator<Item = EdgeMutation>,
    ) -> Result<EdgeMutationLogAppendResult> {
        validate_component("cell_id", cell_id)?;
        validate_component("batch_id", batch_id)?;
        self.ensure_write_authority(cell_id, "append_edge_mutation_log")?;

        let mutations: Vec<_> = mutations.into_iter().collect();
        if mutations.is_empty() {
            return Ok(EdgeMutationLogAppendResult {
                log_epoch: self
                    .read_counter(&keys::mutation_log_epoch(cell_id))
                    .await?,
                mutations: 0,
                already_appended: false,
            });
        }
        ensure_limit(
            "append_edge_mutation_log",
            mutations.len() as u64,
            self.limits.max_bulk_import_edges as u64,
        )?;
        validate_edge_mutations_for_cell(cell_id, &mutations, "append_edge_mutation_log")?;
        let fingerprint = edge_mutation_log_fingerprint(cell_id, batch_id, &mutations);

        let _permit = self
            .acquire_graph_write_permit("append_edge_mutation_log")
            .await?;
        let _writer = self.writer_lane(cell_id).lock().await;
        for attempt in 0..GRAPH_TXN_MAX_RETRIES {
            match self
                .append_edge_mutation_log_txn(cell_id, batch_id, &mutations, fingerprint)
                .await
            {
                Err(err)
                    if is_retryable_write_conflict(&err) && attempt + 1 < GRAPH_TXN_MAX_RETRIES =>
                {
                    self.operation_metrics
                        .write_retries
                        .fetch_add(1, Ordering::Relaxed);
                    tokio::task::yield_now().await;
                }
                Err(err @ GraphError::StaleShardLease { .. }) => {
                    self.operation_metrics
                        .stale_write_rejects
                        .fetch_add(1, Ordering::Relaxed);
                    return Err(err);
                }
                Ok(result) => {
                    self.operation_metrics
                        .write_commits
                        .fetch_add(1, Ordering::Relaxed);
                    return Ok(result);
                }
                result => return result,
            }
        }
        unreachable!("transaction retry loop always returns on final attempt")
    }

    pub(crate) async fn append_edge_mutation_log_txn(
        &self,
        cell_id: &str,
        batch_id: &str,
        mutations: &[EdgeMutation],
        fingerprint: u64,
    ) -> Result<EdgeMutationLogAppendResult> {
        let lock = self
            .acquire_cell_write_lock(cell_id, "append_edge_mutation_log")
            .await?;
        let result = self
            .append_edge_mutation_log_txn_locked(cell_id, batch_id, mutations, fingerprint)
            .await;
        release_cell_write_lock(lock, result).await
    }

    async fn append_edge_mutation_log_txn_locked(
        &self,
        cell_id: &str,
        batch_id: &str,
        mutations: &[EdgeMutation],
        fingerprint: u64,
    ) -> Result<EdgeMutationLogAppendResult> {
        let txn = self.db.begin(IsolationLevel::SerializableSnapshot).await?;
        self.validate_write_fence_txn(&txn, cell_id, "append_edge_mutation_log")
            .await?;
        let idem_key = keys::idempotency(cell_id, "mutation-log", batch_id);
        if let Some(value) = read_txn_remote(&txn, &idem_key).await? {
            return decode_mutation_log_append_idempotency(
                &idem_key,
                batch_id,
                fingerprint,
                &value,
            );
        }

        let current_log_epoch = read_counter_txn(&txn, &keys::mutation_log_epoch(cell_id)).await?;
        let log_epoch =
            current_log_epoch
                .checked_add(1)
                .ok_or_else(|| GraphError::CorruptValue {
                    key: keys::mutation_log_epoch(cell_id),
                    reason: "mutation log epoch overflow".to_string(),
                })?;
        let result = EdgeMutationLogAppendResult {
            log_epoch,
            mutations: mutations.len() as u64,
            already_appended: false,
        };
        let batch = EdgeMutationLogBatch {
            cell_id: cell_id.to_string(),
            batch_id: batch_id.to_string(),
            fingerprint,
            mutations: mutations.to_vec(),
        };
        txn.put(
            keys::mutation_log_entry(cell_id, log_epoch, batch_id).as_bytes(),
            encode_edge_mutation_log_batch(&batch),
        )?;
        txn.put(
            keys::mutation_log_epoch(cell_id).as_bytes(),
            encode_u64(log_epoch),
        )?;
        txn.put(
            idem_key.as_bytes(),
            encode_mutation_log_append_idempotency(batch_id, fingerprint, &result),
        )?;
        commit_txn_strict(txn, self.await_durable_writes).await?;
        Ok(result)
    }

    pub async fn materialize_edge_mutation_log(
        &self,
        cell_id: &str,
        max_batches: usize,
    ) -> Result<EdgeMutationLogMaterializeResult> {
        validate_component("cell_id", cell_id)?;
        self.ensure_write_authority(cell_id, "materialize_edge_mutation_log")?;

        let mut result = EdgeMutationLogMaterializeResult {
            materialized_log_epoch: self
                .read_counter(&keys::mutation_log_materialized_epoch(cell_id))
                .await?,
            current_epoch: self.current_epoch(cell_id).await?,
            ..Default::default()
        };
        if max_batches == 0 {
            result.last_log_epoch = self
                .read_counter(&keys::mutation_log_epoch(cell_id))
                .await?;
            return Ok(result);
        }

        while result.materialized_batches < max_batches as u64 {
            let start_suffix = result
                .materialized_log_epoch
                .checked_add(1)
                .map(|epoch| format!("{epoch:020}/"))
                .unwrap_or_else(|| format!("{:020}/", GraphEpoch::MAX));
            let mut iter = self
                .scan_remote_prefix_from(&keys::mutation_log_prefix(cell_id), &start_suffix)
                .await?;
            let mut pending = Vec::new();
            let mut pending_mutations = 0_usize;
            while result.materialized_batches < max_batches as u64 {
                let Some(kv) = iter.next().await? else {
                    break;
                };
                let key = String::from_utf8_lossy(&kv.key).into_owned();
                let log_epoch = parse_mutation_log_epoch(&key)?;
                if log_epoch <= result.materialized_log_epoch {
                    continue;
                }
                let batch = decode_edge_mutation_log_batch(&key, &kv.value)?;
                if batch.cell_id != cell_id {
                    return Err(GraphError::CorruptValue {
                        key,
                        reason: format!(
                            "mutation log batch belongs to cell {}, expected {cell_id}",
                            batch.cell_id
                        ),
                    });
                }
                validate_edge_mutations_for_cell(
                    cell_id,
                    &batch.mutations,
                    "materialize_edge_mutation_log",
                )?;
                let materialize_edge_limit = self
                    .limits
                    .max_bulk_import_edges
                    .min(GRAPH_MUTATION_LOG_MATERIALIZE_TXN_EDGES);
                if !pending.is_empty()
                    && pending_mutations.saturating_add(batch.mutations.len())
                        > materialize_edge_limit
                {
                    break;
                }
                pending_mutations = pending_mutations.saturating_add(batch.mutations.len());
                result.scanned_batches = result.scanned_batches.saturating_add(1);
                result.materialized_batches = result.materialized_batches.saturating_add(1);
                result.mutations = result
                    .mutations
                    .saturating_add(batch.mutations.len() as u64);
                result.materialized_log_epoch = log_epoch;
                pending.push((log_epoch, batch.mutations));
            }
            if pending.is_empty() {
                break;
            }
            let last_log_epoch = pending
                .last()
                .map(|(log_epoch, _)| *log_epoch)
                .unwrap_or(result.materialized_log_epoch);
            let batch_result = self
                .materialize_edge_mutation_log_batches(cell_id, last_log_epoch, pending)
                .await?;
            result.inserted = result.inserted.saturating_add(batch_result.inserted);
            result.already_existed = result
                .already_existed
                .saturating_add(batch_result.already_existed);
            result.current_epoch = batch_result.end_epoch;
        }
        result.last_log_epoch = self
            .read_counter(&keys::mutation_log_epoch(cell_id))
            .await?;
        result.current_epoch = self.current_epoch(cell_id).await?;
        Ok(result)
    }

    async fn materialize_edge_mutation_log_batches(
        &self,
        cell_id: &str,
        last_log_epoch: GraphEpoch,
        batches: Vec<(GraphEpoch, Vec<EdgeMutation>)>,
    ) -> Result<EdgeMutationBatchResult> {
        let mutation_count = batches
            .iter()
            .map(|(_, mutations)| mutations.len())
            .sum::<usize>();
        ensure_limit(
            "materialize_edge_mutation_log",
            mutation_count as u64,
            self.limits.max_bulk_import_edges as u64,
        )?;
        let mut mutations = Vec::with_capacity(mutation_count);
        for (_, batch_mutations) in batches {
            mutations.extend(batch_mutations);
        }
        let _permit = self
            .acquire_graph_write_permit("materialize_edge_mutation_log")
            .await?;
        let _writer = self.writer_lane(cell_id).lock().await;
        for attempt in 0..GRAPH_TXN_MAX_RETRIES {
            match self
                .write_edge_mutations_batch_txn(
                    cell_id,
                    &mutations,
                    "materialize_edge_mutation_log",
                    Some(last_log_epoch),
                )
                .await
            {
                Err(err)
                    if is_retryable_write_conflict(&err) && attempt + 1 < GRAPH_TXN_MAX_RETRIES =>
                {
                    self.operation_metrics
                        .write_retries
                        .fetch_add(1, Ordering::Relaxed);
                    tokio::task::yield_now().await;
                }
                Err(err @ GraphError::StaleShardLease { .. }) => {
                    self.operation_metrics
                        .stale_write_rejects
                        .fetch_add(1, Ordering::Relaxed);
                    return Err(err);
                }
                Ok(result) => {
                    self.operation_metrics
                        .write_commits
                        .fetch_add(1, Ordering::Relaxed);
                    return Ok(result);
                }
                result => return result,
            }
        }
        unreachable!("transaction retry loop always returns on final attempt")
    }

    pub(crate) async fn write_edge_mutations_batch_txn(
        &self,
        cell_id: &str,
        mutations: &[EdgeMutation],
        operation: &'static str,
        materialized_log_epoch: Option<GraphEpoch>,
    ) -> Result<EdgeMutationBatchResult> {
        let lock = self.acquire_cell_write_lock(cell_id, operation).await?;
        let result = self
            .write_edge_mutations_batch_txn_locked(
                cell_id,
                mutations,
                operation,
                materialized_log_epoch,
            )
            .await;
        release_cell_write_lock(lock, result).await
    }

    async fn write_edge_mutations_batch_txn_locked(
        &self,
        cell_id: &str,
        mutations: &[EdgeMutation],
        operation: &'static str,
        materialized_log_epoch: Option<GraphEpoch>,
    ) -> Result<EdgeMutationBatchResult> {
        let txn = self.db.begin(IsolationLevel::SerializableSnapshot).await?;
        self.validate_write_fence_txn(&txn, cell_id, operation)
            .await?;

        let mut idempotency_keys = BTreeSet::new();
        for mutation in mutations {
            if !idempotency_keys.insert(mutation.idempotency_key.clone()) {
                return Err(GraphError::IdempotencyConflict {
                    operation: "create",
                    idempotency_key: mutation.idempotency_key.clone(),
                });
            }
        }

        let current_epoch = read_counter_txn(&txn, &keys::last_epoch(cell_id)).await?;
        let mut next_epoch = current_epoch;
        let mut results = Vec::with_capacity(mutations.len());
        let mut known_edges = BTreeMap::<(String, VertexId, VertexId), GraphEpoch>::new();
        let mut segment_edges_by_type_src =
            BTreeMap::<(String, VertexId), BTreeMap<VertexId, GraphEpoch>>::new();
        let mut out_increments = BTreeMap::<(String, VertexId), u64>::new();
        let mut in_increments = BTreeMap::<(String, VertexId), u64>::new();
        let write_reverse_index = self.writes_reverse_index();
        let mut inserted = 0_u64;
        let mut already_existed = 0_u64;

        for mutation in mutations {
            let idem_key = keys::idempotency(cell_id, "create", &mutation.idempotency_key);
            if let Some(value) = read_txn_remote(&txn, &idem_key).await? {
                let result = decode_commit_idempotency(&idem_key, mutation, &value)?;
                if result.already_existed {
                    already_existed = already_existed.saturating_add(1);
                } else {
                    inserted = inserted.saturating_add(1);
                }
                results.push(result);
                continue;
            }

            let identity = (mutation.edge_type.clone(), mutation.src, mutation.dst);
            if let Some(epoch) = known_edges.get(&identity).copied() {
                let result = CommitResult {
                    epoch,
                    already_existed: true,
                };
                txn.put(
                    idem_key.as_bytes(),
                    encode_commit_idempotency(mutation, &result),
                )?;
                already_existed = already_existed.saturating_add(1);
                results.push(result);
                continue;
            }

            let edge_key = keys::out_edge(cell_id, &mutation.edge_type, mutation.src, mutation.dst);
            if let Some(value) = read_txn_remote(&txn, &edge_key).await? {
                let record = decode_edge_record(&edge_key, &value)?;
                let result = CommitResult {
                    epoch: record.epoch,
                    already_existed: true,
                };
                known_edges.insert(identity, record.epoch);
                txn.put(
                    idem_key.as_bytes(),
                    encode_commit_idempotency(mutation, &result),
                )?;
                already_existed = already_existed.saturating_add(1);
                results.push(result);
                continue;
            }

            let segment_cache_key = (mutation.edge_type.clone(), mutation.src);
            let segment_epoch = match segment_edges_by_type_src.get(&segment_cache_key) {
                Some(edges) => edges.get(&mutation.dst).copied(),
                None => {
                    let edges = out_segment_edges_for_src_txn(
                        &txn,
                        cell_id,
                        &mutation.edge_type,
                        mutation.src,
                        current_epoch,
                    )
                    .await?;
                    let epoch = edges.get(&mutation.dst).copied();
                    segment_edges_by_type_src.insert(segment_cache_key, edges);
                    epoch
                }
            };
            if let Some(epoch) = segment_epoch {
                let result = CommitResult {
                    epoch,
                    already_existed: true,
                };
                known_edges.insert(identity, epoch);
                txn.put(
                    idem_key.as_bytes(),
                    encode_commit_idempotency(mutation, &result),
                )?;
                already_existed = already_existed.saturating_add(1);
                results.push(result);
                continue;
            }

            next_epoch = next_epoch
                .checked_add(1)
                .ok_or_else(|| GraphError::CorruptValue {
                    key: operation.to_string(),
                    reason: "epoch overflow during edge mutation batch".to_string(),
                })?;
            let record = EdgeRecord {
                cell_id: cell_id.to_string(),
                edge_type: mutation.edge_type.clone(),
                src: mutation.src,
                dst: mutation.dst,
                epoch: next_epoch,
            };
            let result = CommitResult {
                epoch: next_epoch,
                already_existed: false,
            };
            let edge_value = encode_edge_record(&record);
            let delta_value = encode_delta_record(&DeltaRecord {
                kind: DeltaKind::Plus,
                edge: record.clone(),
            });
            txn.put(
                keys::out_edge(cell_id, &mutation.edge_type, mutation.src, mutation.dst).as_bytes(),
                &edge_value,
            )?;
            if write_reverse_index {
                txn.put(
                    keys::in_edge(cell_id, &mutation.edge_type, mutation.dst, mutation.src)
                        .as_bytes(),
                    &edge_value,
                )?;
            }
            txn.put(
                keys::outbox(
                    cell_id,
                    next_epoch,
                    DeltaKind::Plus,
                    &mutation.edge_type,
                    mutation.src,
                    mutation.dst,
                )
                .as_bytes(),
                &delta_value,
            )?;
            txn.put(
                idem_key.as_bytes(),
                encode_commit_idempotency(mutation, &result),
            )?;
            known_edges.insert(identity, next_epoch);
            *out_increments
                .entry((mutation.edge_type.clone(), mutation.src))
                .or_insert(0) += 1;
            if write_reverse_index {
                *in_increments
                    .entry((mutation.edge_type.clone(), mutation.dst))
                    .or_insert(0) += 1;
            }
            inserted = inserted.saturating_add(1);
            results.push(result);
        }

        for ((edge_type, src), increment) in out_increments {
            let key = keys::degree_out(cell_id, &edge_type, src);
            let base = read_counter_txn(&txn, &key).await?;
            txn.put(key.as_bytes(), encode_u64(base + increment))?;
        }
        if write_reverse_index {
            for ((edge_type, dst), increment) in in_increments {
                let key = keys::degree_in(cell_id, &edge_type, dst);
                let base = read_counter_txn(&txn, &key).await?;
                txn.put(key.as_bytes(), encode_u64(base + increment))?;
            }
        }
        if next_epoch > current_epoch {
            txn.put(keys::last_epoch(cell_id).as_bytes(), encode_u64(next_epoch))?;
        }
        if let Some(log_epoch) = materialized_log_epoch {
            txn.put(
                keys::mutation_log_materialized_epoch(cell_id).as_bytes(),
                encode_u64(log_epoch),
            )?;
        }

        let inserted_start_epoch = results
            .iter()
            .filter(|result| !result.already_existed)
            .map(|result| result.epoch)
            .min();
        let inserted_end_epoch = results
            .iter()
            .filter(|result| !result.already_existed)
            .map(|result| result.epoch)
            .max();
        commit_txn_strict(txn, self.await_durable_writes).await?;
        Ok(EdgeMutationBatchResult {
            start_epoch: inserted_start_epoch.unwrap_or(current_epoch),
            end_epoch: inserted_end_epoch.unwrap_or(next_epoch),
            inserted,
            already_existed,
            results,
        })
    }

    pub async fn write_edges_batch(
        &self,
        cell_id: &str,
        edge_type: &str,
        edges: impl IntoIterator<Item = (VertexId, VertexId)>,
        idempotency_key: &str,
    ) -> Result<BulkImportResult> {
        self.bulk_import_edges(cell_id, edge_type, edges, idempotency_key)
            .await
    }

    pub async fn write_edges_batch_chunked(
        &self,
        cell_id: &str,
        edge_type: &str,
        edges: impl IntoIterator<Item = (VertexId, VertexId)>,
        idempotency_key: &str,
        chunk_size: usize,
    ) -> Result<BulkImportResult> {
        self.bulk_import_edges_chunked(cell_id, edge_type, edges, idempotency_key, chunk_size)
            .await
    }

    pub async fn bulk_import_edges_chunked(
        &self,
        cell_id: &str,
        edge_type: &str,
        edges: impl IntoIterator<Item = (VertexId, VertexId)>,
        idempotency_key: &str,
        chunk_size: usize,
    ) -> Result<BulkImportResult> {
        self.bulk_import_edges_chunked_with_options(
            cell_id,
            edge_type,
            edges,
            idempotency_key,
            chunk_size,
            BulkImportOptions::default(),
        )
        .await
    }

    pub async fn bulk_append_edges_trusted_chunked(
        &self,
        cell_id: &str,
        edge_type: &str,
        edges: impl IntoIterator<Item = (VertexId, VertexId)>,
        idempotency_key: &str,
        chunk_size: usize,
    ) -> Result<BulkImportResult> {
        self.bulk_import_edges_chunked_with_options(
            cell_id,
            edge_type,
            edges,
            idempotency_key,
            chunk_size,
            BulkImportOptions::checked_batch_append(),
        )
        .await
    }

    pub async fn bulk_import_edges_chunked_with_options(
        &self,
        cell_id: &str,
        edge_type: &str,
        edges: impl IntoIterator<Item = (VertexId, VertexId)>,
        idempotency_key: &str,
        chunk_size: usize,
        options: BulkImportOptions,
    ) -> Result<BulkImportResult> {
        validate_component("cell_id", cell_id)?;
        validate_component("edge_type", edge_type)?;
        validate_component("idempotency_key", idempotency_key)?;
        if chunk_size == 0 {
            return Err(GraphError::CorruptValue {
                key: "bulk_import_chunk_size".to_string(),
                reason: "chunk size must be greater than zero".to_string(),
            });
        }

        let mut edges: Vec<_> = edges.into_iter().collect();
        edges.sort_unstable_by_key(|(src, dst)| (bulk_import_chunk_order(*src, *dst), *src, *dst));
        edges.dedup();

        let mut start_epoch = None;
        let mut end_epoch = self.current_epoch(cell_id).await?;
        let mut inserted = 0_u64;
        let mut already_existed = 0_u64;
        let mut chunk = Vec::with_capacity(chunk_size);
        let mut chunk_id = 0_u64;
        for edge in edges {
            chunk.push(edge);
            if chunk.len() == chunk_size {
                let result = self
                    .bulk_import_edges_with_options(
                        cell_id,
                        edge_type,
                        std::mem::take(&mut chunk),
                        &format!("{idempotency_key}-chunk-{chunk_id:020}"),
                        options,
                    )
                    .await?;
                start_epoch.get_or_insert(result.start_epoch);
                end_epoch = result.end_epoch;
                inserted = inserted.saturating_add(result.inserted);
                already_existed = already_existed.saturating_add(result.already_existed);
                chunk_id = chunk_id.saturating_add(1);
                chunk = Vec::with_capacity(chunk_size);
            }
        }
        if !chunk.is_empty() {
            let result = self
                .bulk_import_edges_with_options(
                    cell_id,
                    edge_type,
                    chunk,
                    &format!("{idempotency_key}-chunk-{chunk_id:020}"),
                    options,
                )
                .await?;
            start_epoch.get_or_insert(result.start_epoch);
            end_epoch = result.end_epoch;
            inserted = inserted.saturating_add(result.inserted);
            already_existed = already_existed.saturating_add(result.already_existed);
        }

        Ok(BulkImportResult {
            start_epoch: start_epoch.unwrap_or(end_epoch),
            end_epoch,
            inserted,
            already_existed,
        })
    }

    pub(crate) async fn bulk_append_supernode_segment_trusted_txn(
        &self,
        cell_id: &str,
        edge_type: &str,
        src: VertexId,
        dsts: &[VertexId],
        idempotency_key: &str,
        fingerprint: u64,
    ) -> Result<BulkImportResult> {
        let lock = self
            .acquire_cell_write_lock(cell_id, "bulk_append_supernode_segment_trusted")
            .await?;
        let result = self
            .bulk_append_supernode_segment_trusted_txn_locked(
                cell_id,
                edge_type,
                src,
                dsts,
                idempotency_key,
                fingerprint,
            )
            .await;
        release_cell_write_lock(lock, result).await
    }

    async fn bulk_append_supernode_segment_trusted_txn_locked(
        &self,
        cell_id: &str,
        edge_type: &str,
        src: VertexId,
        dsts: &[VertexId],
        idempotency_key: &str,
        fingerprint: u64,
    ) -> Result<BulkImportResult> {
        let txn = self.db.begin(IsolationLevel::SerializableSnapshot).await?;
        self.validate_write_fence_txn(&txn, cell_id, "bulk_append_supernode_segment_trusted")
            .await?;
        let idem_key = keys::idempotency(cell_id, "segment-import", idempotency_key);
        if let Some(value) = read_txn_remote(&txn, &idem_key).await? {
            return decode_bulk_import_idempotency(&idem_key, idempotency_key, fingerprint, &value);
        }
        let fingerprint_key = segment_import_fingerprint_key(cell_id, edge_type, src, fingerprint);
        if let Some(value) = read_txn_remote(&txn, &fingerprint_key).await? {
            return decode_bulk_import_fingerprint_idempotency(
                &fingerprint_key,
                fingerprint,
                &value,
            );
        }

        let current_epoch = read_counter_txn(&txn, &keys::last_epoch(cell_id)).await?;
        let existing =
            out_neighbors_for_src_txn(&txn, cell_id, edge_type, src, current_epoch).await?;
        let inserted_dsts: Vec<_> = dsts
            .iter()
            .copied()
            .filter(|dst| !existing.contains(dst))
            .collect();
        let already_existed = u64::try_from(dsts.len().saturating_sub(inserted_dsts.len()))
            .map_err(|err| GraphError::CorruptValue {
                key: "segment_import".to_string(),
                reason: format!("too many existing edges in one segment import: {err}"),
            })?;
        let inserted =
            u64::try_from(inserted_dsts.len()).map_err(|err| GraphError::CorruptValue {
                key: "segment_import".to_string(),
                reason: format!("too many edges in one segment import: {err}"),
            })?;
        let end_epoch =
            current_epoch
                .checked_add(inserted)
                .ok_or_else(|| GraphError::CorruptValue {
                    key: "segment_import".to_string(),
                    reason: "epoch overflow during segment import".to_string(),
                })?;
        let start_epoch = if inserted == 0 {
            current_epoch
        } else {
            current_epoch + 1
        };
        let result = BulkImportResult {
            start_epoch,
            end_epoch,
            inserted,
            already_existed,
        };

        if inserted > 0 {
            txn.put(
                keys::out_segment(
                    cell_id,
                    edge_type,
                    src,
                    end_epoch,
                    start_epoch,
                    idempotency_key,
                )
                .as_bytes(),
                encode_out_edge_segment(&inserted_dsts),
            )?;
            txn.put(keys::last_epoch(cell_id).as_bytes(), encode_u64(end_epoch))?;
            let degree_key = keys::degree_out(cell_id, edge_type, src);
            let base = if current_epoch == 0 {
                0
            } else {
                read_counter_txn(&txn, &degree_key).await?
            };
            txn.put(degree_key.as_bytes(), encode_u64(base + inserted))?;
            txn.put(
                keys::outbox_batch(
                    cell_id,
                    end_epoch,
                    start_epoch,
                    DeltaKind::Plus,
                    edge_type,
                    idempotency_key,
                )
                .as_bytes(),
                encode_outbox_delta_batch_same_src(
                    cell_id,
                    edge_type,
                    DeltaKind::Plus,
                    start_epoch,
                    end_epoch,
                    src,
                    &inserted_dsts,
                ),
            )?;
        }
        txn.put(
            keys::mutation_batch(cell_id, result.start_epoch, idempotency_key).as_bytes(),
            encode_mutation_batch_log(edge_type, idempotency_key, fingerprint, &result),
        )?;
        txn.put(
            idem_key.as_bytes(),
            encode_bulk_import_idempotency(idempotency_key, fingerprint, &result),
        )?;
        txn.put(
            fingerprint_key.as_bytes(),
            encode_bulk_import_idempotency(idempotency_key, fingerprint, &result),
        )?;

        commit_txn_strict(txn, self.await_durable_writes).await?;
        Ok(result)
    }

    pub(crate) async fn bulk_import_edges_txn(
        &self,
        cell_id: &str,
        edge_type: &str,
        edges: &[(VertexId, VertexId)],
        idempotency_key: &str,
        fingerprint: u64,
        options: BulkImportOptions,
    ) -> Result<BulkImportResult> {
        let lock = self
            .acquire_cell_write_lock(cell_id, "bulk_import_edges")
            .await?;
        let result = self
            .bulk_import_edges_txn_locked(
                cell_id,
                edge_type,
                edges,
                idempotency_key,
                fingerprint,
                options,
            )
            .await;
        release_cell_write_lock(lock, result).await
    }

    async fn bulk_import_edges_txn_locked(
        &self,
        cell_id: &str,
        edge_type: &str,
        edges: &[(VertexId, VertexId)],
        idempotency_key: &str,
        fingerprint: u64,
        options: BulkImportOptions,
    ) -> Result<BulkImportResult> {
        let preflight_started = std::time::Instant::now();
        let txn = self.db.begin(IsolationLevel::SerializableSnapshot).await?;
        self.validate_write_fence_txn(&txn, cell_id, "bulk_import_edges")
            .await?;
        let idem_key = keys::idempotency(cell_id, "bulk-import", idempotency_key);
        if let Some(value) = read_txn_remote(&txn, &idem_key).await? {
            return decode_bulk_import_idempotency(&idem_key, idempotency_key, fingerprint, &value);
        }

        let current_epoch = read_counter_txn(&txn, &keys::last_epoch(cell_id)).await?;
        let fresh_cell = current_epoch == 0;
        let mut already_existed = 0_u64;
        let mut inserted_edges = Vec::new();
        let mut segment_neighbors_by_src = BTreeMap::<VertexId, BTreeSet<VertexId>>::new();
        for (src, dst) in edges.iter().copied() {
            if options.duplicate_policy.check_existing() && !fresh_cell {
                if read_txn_remote(&txn, &keys::out_edge(cell_id, edge_type, src, dst))
                    .await?
                    .is_some()
                {
                    already_existed += 1;
                    continue;
                }
                let segment_exists = match segment_neighbors_by_src.get(&src) {
                    Some(neighbors) => neighbors.contains(&dst),
                    None => {
                        let neighbors = out_segment_neighbors_for_src_txn(
                            &txn,
                            cell_id,
                            edge_type,
                            src,
                            current_epoch,
                        )
                        .await?;
                        let exists = neighbors.contains(&dst);
                        segment_neighbors_by_src.insert(src, neighbors);
                        exists
                    }
                };
                if segment_exists {
                    already_existed += 1;
                    continue;
                }
            }
            inserted_edges.push((src, dst));
        }
        let preflight_elapsed = preflight_started.elapsed();

        let inserted =
            u64::try_from(inserted_edges.len()).map_err(|err| GraphError::CorruptValue {
                key: "bulk_import".to_string(),
                reason: format!("too many edges in one import: {err}"),
            })?;
        let end_epoch =
            current_epoch
                .checked_add(inserted)
                .ok_or_else(|| GraphError::CorruptValue {
                    key: "bulk_import".to_string(),
                    reason: "epoch overflow during bulk import".to_string(),
                })?;
        let start_epoch = if inserted == 0 {
            current_epoch
        } else {
            current_epoch + 1
        };
        let result = BulkImportResult {
            start_epoch,
            end_epoch,
            inserted,
            already_existed,
        };

        let write_reverse_index = self.writes_reverse_index();
        let mut out_increments = std::collections::BTreeMap::<VertexId, u64>::new();
        let mut in_increments = std::collections::BTreeMap::<VertexId, u64>::new();
        let batch_build_started = std::time::Instant::now();
        for (offset, (src, dst)) in inserted_edges.iter().copied().enumerate() {
            let epoch = current_epoch + 1 + offset as u64;
            let record = EdgeRecord {
                cell_id: cell_id.to_string(),
                edge_type: edge_type.to_string(),
                src,
                dst,
                epoch,
            };
            let edge_value = encode_edge_record(&record);
            let delta_value = encode_delta_record(&DeltaRecord {
                kind: DeltaKind::Plus,
                edge: record.clone(),
            });
            txn.put(
                keys::out_edge(cell_id, edge_type, src, dst).as_bytes(),
                &edge_value,
            )?;
            if write_reverse_index {
                txn.put(
                    keys::in_edge(cell_id, edge_type, dst, src).as_bytes(),
                    &edge_value,
                )?;
            }
            if options.delta_log_policy.write_per_edge() {
                txn.put(
                    keys::outbox(cell_id, epoch, DeltaKind::Plus, edge_type, src, dst).as_bytes(),
                    &delta_value,
                )?;
            }
            *out_increments.entry(src).or_insert(0) += 1;
            if write_reverse_index {
                *in_increments.entry(dst).or_insert(0) += 1;
            }
        }
        let batch_build_elapsed = batch_build_started.elapsed();

        let counter_read_started = std::time::Instant::now();
        for (src, increment) in out_increments {
            let key = keys::degree_out(cell_id, edge_type, src);
            let base = if fresh_cell {
                0
            } else {
                read_counter_txn(&txn, &key).await?
            };
            txn.put(key.as_bytes(), encode_u64(base + increment))?;
        }
        if write_reverse_index {
            for (dst, increment) in in_increments {
                let key = keys::degree_in(cell_id, edge_type, dst);
                let base = if fresh_cell {
                    0
                } else {
                    read_counter_txn(&txn, &key).await?
                };
                txn.put(key.as_bytes(), encode_u64(base + increment))?;
            }
        }
        let counter_read_elapsed = counter_read_started.elapsed();
        if inserted > 0 {
            txn.put(keys::last_epoch(cell_id).as_bytes(), encode_u64(end_epoch))?;
            if options.delta_log_policy.write_batch() {
                txn.put(
                    keys::outbox_batch(
                        cell_id,
                        end_epoch,
                        start_epoch,
                        DeltaKind::Plus,
                        edge_type,
                        idempotency_key,
                    )
                    .as_bytes(),
                    encode_outbox_delta_batch(
                        cell_id,
                        edge_type,
                        DeltaKind::Plus,
                        start_epoch,
                        end_epoch,
                        &inserted_edges,
                    ),
                )?;
            }
        }
        txn.put(
            keys::mutation_batch(cell_id, result.start_epoch, idempotency_key).as_bytes(),
            encode_mutation_batch_log(edge_type, idempotency_key, fingerprint, &result),
        )?;
        txn.put(
            idem_key.as_bytes(),
            encode_bulk_import_idempotency(idempotency_key, fingerprint, &result),
        )?;

        let commit_started = std::time::Instant::now();
        commit_txn_strict(txn, self.await_durable_writes).await?;
        let commit_elapsed = commit_started.elapsed();
        self.record_bulk_import_profile(
            preflight_elapsed,
            batch_build_elapsed,
            counter_read_elapsed,
            commit_elapsed,
        );
        Ok(result)
    }
}

fn validate_vertex_metadata(metadata: &VertexMetadata) -> Result<()> {
    for label in &metadata.labels {
        validate_component("label", label)?;
    }
    for property in metadata.properties.keys() {
        validate_component("property", property)?;
    }
    Ok(())
}

fn validate_edge_metadata(metadata: &EdgeMetadata) -> Result<()> {
    for property in metadata.properties.keys() {
        validate_component("property", property)?;
    }
    Ok(())
}

fn coalesce_vertex_metadata_updates(
    updates: impl IntoIterator<Item = (VertexId, VertexMetadata)>,
) -> Result<Vec<(VertexId, VertexMetadata)>> {
    let mut by_vertex = BTreeMap::<VertexId, VertexMetadata>::new();
    for (vertex_id, metadata) in updates {
        validate_vertex_metadata(&metadata)?;
        let entry = by_vertex.entry(vertex_id).or_default();
        entry.labels.extend(metadata.labels);
        for (property, value) in metadata.properties {
            match entry.properties.get(&property) {
                Some(existing) if existing != &value => {
                    return Err(GraphError::UnsupportedQuery {
                        dialect: "GraphQuery",
                        feature: format!(
                            "conflicting metadata values for vertex {vertex_id} property {property}"
                        ),
                    });
                }
                _ => {
                    entry.properties.insert(property, value);
                }
            }
        }
    }
    Ok(by_vertex.into_iter().collect())
}

fn merge_vertex_metadata(previous: &VertexMetadata, requested: &VertexMetadata) -> VertexMetadata {
    let mut next = previous.clone();
    next.labels.extend(requested.labels.iter().cloned());
    next.properties.extend(
        requested
            .properties
            .iter()
            .map(|(property, value)| (property.clone(), value.clone())),
    );
    next
}

fn merge_edge_metadata(previous: &EdgeMetadata, requested: &EdgeMetadata) -> EdgeMetadata {
    let mut next = previous.clone();
    next.properties.extend(
        requested
            .properties
            .iter()
            .map(|(property, value)| (property.clone(), value.clone())),
    );
    next
}

fn apply_vertex_metadata_update_txn(
    txn: &DbTransaction,
    cell_id: &str,
    vertex_id: VertexId,
    previous: &VertexMetadata,
    next: &VertexMetadata,
    epoch: GraphEpoch,
) -> Result<()> {
    validate_vertex_metadata(next)?;
    let vertex_key = keys::vertex(cell_id, vertex_id);
    txn.put(
        keys::vertex_delta(cell_id, vertex_id, epoch).as_bytes(),
        encode_vertex_metadata(next).as_slice(),
    )?;
    delete_vertex_metadata_indexes_txn(txn, cell_id, vertex_id, previous)?;
    if next.labels.is_empty() && next.properties.is_empty() {
        txn.delete(vertex_key.as_bytes())?;
    } else {
        txn.put(
            vertex_key.as_bytes(),
            encode_vertex_metadata(next).as_slice(),
        )?;
        put_vertex_metadata_indexes_txn(txn, cell_id, vertex_id, next)?;
    }
    put_vertex_metadata_index_deltas_txn(txn, cell_id, vertex_id, previous, next, epoch)
}

fn put_vertex_metadata_indexes_txn(
    txn: &DbTransaction,
    cell_id: &str,
    vertex_id: VertexId,
    metadata: &VertexMetadata,
) -> Result<()> {
    for label in &metadata.labels {
        txn.put(
            keys::vertex_label(cell_id, label, vertex_id).as_bytes(),
            encode_u64(vertex_id).as_slice(),
        )?;
    }
    for (property, value) in &metadata.properties {
        txn.put(
            keys::vertex_property_index(
                cell_id,
                property,
                &encode_vertex_property_value_key(value),
                vertex_id,
            )
            .as_bytes(),
            encode_u64(vertex_id).as_slice(),
        )?;
    }
    Ok(())
}

fn delete_vertex_metadata_indexes_txn(
    txn: &DbTransaction,
    cell_id: &str,
    vertex_id: VertexId,
    metadata: &VertexMetadata,
) -> Result<()> {
    for label in &metadata.labels {
        txn.delete(keys::vertex_label(cell_id, label, vertex_id).as_bytes())?;
    }
    for (property, value) in &metadata.properties {
        txn.delete(
            keys::vertex_property_index(
                cell_id,
                property,
                &encode_vertex_property_value_key(value),
                vertex_id,
            )
            .as_bytes(),
        )?;
    }
    Ok(())
}

fn put_vertex_metadata_index_deltas_txn(
    txn: &DbTransaction,
    cell_id: &str,
    vertex_id: VertexId,
    previous: &VertexMetadata,
    next: &VertexMetadata,
    epoch: GraphEpoch,
) -> Result<()> {
    for label in previous.labels.difference(&next.labels) {
        txn.put(
            keys::vertex_label_delta(cell_id, label, epoch, vertex_id).as_bytes(),
            encode_vertex_index_delta(false).as_slice(),
        )?;
    }
    for label in next.labels.difference(&previous.labels) {
        txn.put(
            keys::vertex_label_delta(cell_id, label, epoch, vertex_id).as_bytes(),
            encode_vertex_index_delta(true).as_slice(),
        )?;
    }
    for (property, value) in &previous.properties {
        if next.properties.get(property) != Some(value) {
            txn.put(
                keys::vertex_property_index_delta(
                    cell_id,
                    property,
                    &encode_vertex_property_value_key(value),
                    epoch,
                    vertex_id,
                )
                .as_bytes(),
                encode_vertex_index_delta(false).as_slice(),
            )?;
        }
    }
    for (property, value) in &next.properties {
        if previous.properties.get(property) != Some(value) {
            txn.put(
                keys::vertex_property_index_delta(
                    cell_id,
                    property,
                    &encode_vertex_property_value_key(value),
                    epoch,
                    vertex_id,
                )
                .as_bytes(),
                encode_vertex_index_delta(true).as_slice(),
            )?;
        }
    }
    Ok(())
}

fn apply_edge_metadata_update_txn(
    txn: &DbTransaction,
    target: EdgeMetadataTarget<'_>,
    previous: &EdgeMetadata,
    next: &EdgeMetadata,
    epoch: GraphEpoch,
) -> Result<()> {
    validate_edge_metadata(next)?;
    let edge_metadata_key =
        keys::edge_metadata(target.cell_id, target.edge_type, target.src, target.dst);
    txn.put(
        keys::edge_metadata_delta(
            target.cell_id,
            target.edge_type,
            target.src,
            target.dst,
            epoch,
        )
        .as_bytes(),
        encode_edge_metadata(next).as_slice(),
    )?;
    delete_edge_metadata_indexes_txn(txn, target, previous)?;
    if next.properties.is_empty() {
        txn.delete(edge_metadata_key.as_bytes())?;
    } else {
        txn.put(
            edge_metadata_key.as_bytes(),
            encode_edge_metadata(next).as_slice(),
        )?;
        put_edge_metadata_indexes_txn(txn, target, next)?;
    }
    put_edge_metadata_index_deltas_txn(txn, target, previous, next, epoch)
}

#[derive(Clone, Copy)]
struct EdgeMetadataTarget<'a> {
    cell_id: &'a str,
    edge_type: &'a str,
    src: VertexId,
    dst: VertexId,
}

fn put_edge_metadata_indexes_txn(
    txn: &DbTransaction,
    target: EdgeMetadataTarget<'_>,
    metadata: &EdgeMetadata,
) -> Result<()> {
    for (property, value) in &metadata.properties {
        txn.put(
            keys::edge_property_index(
                target.cell_id,
                target.edge_type,
                property,
                &encode_vertex_property_value_key(value),
                target.src,
                target.dst,
            )
            .as_bytes(),
            encode_u64(target.dst).as_slice(),
        )?;
    }
    Ok(())
}

fn delete_edge_metadata_indexes_txn(
    txn: &DbTransaction,
    target: EdgeMetadataTarget<'_>,
    metadata: &EdgeMetadata,
) -> Result<()> {
    for (property, value) in &metadata.properties {
        txn.delete(
            keys::edge_property_index(
                target.cell_id,
                target.edge_type,
                property,
                &encode_vertex_property_value_key(value),
                target.src,
                target.dst,
            )
            .as_bytes(),
        )?;
    }
    Ok(())
}

fn put_edge_metadata_index_deltas_txn(
    txn: &DbTransaction,
    target: EdgeMetadataTarget<'_>,
    previous: &EdgeMetadata,
    next: &EdgeMetadata,
    epoch: GraphEpoch,
) -> Result<()> {
    for (property, value) in &previous.properties {
        if next.properties.get(property) != Some(value) {
            txn.put(
                keys::edge_property_index_delta(
                    target.cell_id,
                    target.edge_type,
                    property,
                    &encode_vertex_property_value_key(value),
                    epoch,
                    target.src,
                    target.dst,
                )
                .as_bytes(),
                encode_vertex_index_delta(false).as_slice(),
            )?;
        }
    }
    for (property, value) in &next.properties {
        if previous.properties.get(property) != Some(value) {
            txn.put(
                keys::edge_property_index_delta(
                    target.cell_id,
                    target.edge_type,
                    property,
                    &encode_vertex_property_value_key(value),
                    epoch,
                    target.src,
                    target.dst,
                )
                .as_bytes(),
                encode_vertex_index_delta(true).as_slice(),
            )?;
        }
    }
    Ok(())
}
