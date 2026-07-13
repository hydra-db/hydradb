use super::*;

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct IncidentEdge {
    edge_type: String,
    src: VertexId,
    dst: VertexId,
}

#[derive(Clone, Debug)]
struct DeleteOutboxRun {
    edge_type: String,
    start_epoch: GraphEpoch,
    end_epoch: GraphEpoch,
    edges: Vec<(VertexId, VertexId)>,
}

const VERTEX_DELETE_LOCK_RENEW_ITEMS: u64 = 64;

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
        Err(GraphError::RetryExhausted {
            operation: "graph transaction",
            attempts: GRAPH_TXN_MAX_RETRIES,
        })
    }

    pub async fn set_vertex_metadata_batch(
        &self,
        cell_id: &str,
        updates: impl IntoIterator<Item = (VertexId, VertexMetadata)>,
    ) -> Result<usize> {
        validate_component("cell_id", cell_id)?;
        self.ensure_write_authority(cell_id, "set_vertex_metadata_batch")?;
        let updates = coalesce_vertex_metadata_updates(updates)?;
        if updates.is_empty() {
            return Ok(0);
        }
        ensure_limit(
            "set_vertex_metadata_batch",
            updates.len() as u64,
            self.limits.max_bulk_import_edges as u64,
        )?;
        let _permit = self
            .acquire_graph_write_permit("set_vertex_metadata_batch")
            .await?;
        let _writer = self.writer_lane(cell_id).lock().await;
        for attempt in 0..GRAPH_TXN_MAX_RETRIES {
            match self
                .set_vertex_metadata_batch_txn(cell_id, updates.clone())
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
                Ok(changed) => {
                    if changed > 0 {
                        self.operation_metrics
                            .write_commits
                            .fetch_add(1, Ordering::Relaxed);
                    }
                    return Ok(changed);
                }
                result => return result,
            }
        }
        Err(GraphError::RetryExhausted {
            operation: "graph transaction",
            attempts: GRAPH_TXN_MAX_RETRIES,
        })
    }

    pub async fn import_vertex_metadata_batch(
        &self,
        cell_id: &str,
        updates: impl IntoIterator<Item = (VertexId, VertexMetadata)>,
    ) -> Result<usize> {
        validate_component("cell_id", cell_id)?;
        self.ensure_write_authority(cell_id, "import_vertex_metadata_batch")?;
        let updates = coalesce_vertex_metadata_updates(updates)?;
        if updates.is_empty() {
            return Ok(0);
        }
        ensure_limit(
            "import_vertex_metadata_batch",
            updates.len() as u64,
            self.limits.max_bulk_import_edges as u64,
        )?;
        let _permit = self
            .acquire_graph_write_permit("import_vertex_metadata_batch")
            .await?;
        let _writer = self.writer_lane(cell_id).lock().await;
        for attempt in 0..GRAPH_TXN_MAX_RETRIES {
            match self
                .import_vertex_metadata_batch_txn(cell_id, updates.clone())
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
                Ok(changed) => {
                    if changed > 0 {
                        self.operation_metrics
                            .write_commits
                            .fetch_add(1, Ordering::Relaxed);
                    }
                    return Ok(changed);
                }
                result => return result,
            }
        }
        Err(GraphError::RetryExhausted {
            operation: "graph transaction",
            attempts: GRAPH_TXN_MAX_RETRIES,
        })
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
        let txn = self
            .db
            .writer()?
            .begin(IsolationLevel::SerializableSnapshot)
            .await?;
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

    async fn set_vertex_metadata_batch_txn(
        &self,
        cell_id: &str,
        updates: Vec<(VertexId, VertexMetadata)>,
    ) -> Result<usize> {
        let lock = self
            .acquire_cell_write_lock(cell_id, "set_vertex_metadata_batch")
            .await?;
        let result = self
            .set_vertex_metadata_batch_txn_locked(cell_id, updates)
            .await;
        release_cell_write_lock(lock, result).await
    }

    async fn set_vertex_metadata_batch_txn_locked(
        &self,
        cell_id: &str,
        updates: Vec<(VertexId, VertexMetadata)>,
    ) -> Result<usize> {
        let txn = self
            .db
            .writer()?
            .begin(IsolationLevel::SerializableSnapshot)
            .await?;
        self.validate_write_fence_txn(&txn, cell_id, "set_vertex_metadata_batch")
            .await?;
        let mut changed = Vec::new();
        for (vertex_id, metadata) in updates {
            let vertex_key = keys::vertex(cell_id, vertex_id);
            let previous = match read_txn_remote(&txn, &vertex_key).await? {
                Some(value) => decode_vertex_metadata(&vertex_key, &value)?,
                None => VertexMetadata::default(),
            };
            if previous != metadata {
                changed.push((vertex_id, previous, metadata));
            }
        }
        if changed.is_empty() {
            return Ok(0);
        }
        let epoch = next_epoch_txn(&txn, cell_id).await?;
        txn.put(keys::last_epoch(cell_id).as_bytes(), encode_u64(epoch))?;
        for (vertex_id, previous, metadata) in &changed {
            apply_vertex_metadata_update_txn(&txn, cell_id, *vertex_id, previous, metadata, epoch)?;
        }
        let changed_count = changed.len();
        commit_txn_strict(txn, self.await_durable_writes).await?;
        Ok(changed_count)
    }

    async fn import_vertex_metadata_batch_txn(
        &self,
        cell_id: &str,
        updates: Vec<(VertexId, VertexMetadata)>,
    ) -> Result<usize> {
        let lock = self
            .acquire_cell_write_lock(cell_id, "import_vertex_metadata_batch")
            .await?;
        let result = self
            .import_vertex_metadata_batch_txn_locked(cell_id, updates)
            .await;
        release_cell_write_lock(lock, result).await
    }

    async fn import_vertex_metadata_batch_txn_locked(
        &self,
        cell_id: &str,
        updates: Vec<(VertexId, VertexMetadata)>,
    ) -> Result<usize> {
        let txn = self
            .db
            .writer()?
            .begin(IsolationLevel::SerializableSnapshot)
            .await?;
        self.validate_write_fence_txn(&txn, cell_id, "import_vertex_metadata_batch")
            .await?;
        let mut changed = Vec::new();
        for (vertex_id, metadata) in updates {
            let vertex_key = keys::vertex(cell_id, vertex_id);
            let previous = match read_txn_remote(&txn, &vertex_key).await? {
                Some(value) => decode_vertex_metadata(&vertex_key, &value)?,
                None => VertexMetadata::default(),
            };
            if previous == metadata {
                continue;
            }
            if previous != VertexMetadata::default() {
                return Err(GraphError::CorruptValue {
                    key: vertex_key,
                    reason: format!(
                        "vertex {vertex_id} already has different metadata during import"
                    ),
                });
            }
            changed.push((vertex_id, previous, metadata));
        }
        if changed.is_empty() {
            return Ok(0);
        }
        let epoch = next_epoch_txn(&txn, cell_id).await?;
        txn.put(keys::last_epoch(cell_id).as_bytes(), encode_u64(epoch))?;
        for (vertex_id, previous, metadata) in &changed {
            apply_vertex_metadata_update_txn(&txn, cell_id, *vertex_id, previous, metadata, epoch)?;
        }
        let changed_count = changed.len();
        commit_txn_strict(txn, self.await_durable_writes).await?;
        Ok(changed_count)
    }

    pub async fn delete_vertex(
        &self,
        cell_id: &str,
        vertex_id: VertexId,
        idempotency_key: &str,
    ) -> Result<VertexDeleteResult> {
        self.delete_vertex_with_options(cell_id, vertex_id, idempotency_key, false)
            .await
    }

    pub async fn detach_delete_vertex(
        &self,
        cell_id: &str,
        vertex_id: VertexId,
        idempotency_key: &str,
    ) -> Result<VertexDeleteResult> {
        self.delete_vertex_with_options(cell_id, vertex_id, idempotency_key, true)
            .await
    }

    async fn delete_vertex_with_options(
        &self,
        cell_id: &str,
        vertex_id: VertexId,
        idempotency_key: &str,
        detach: bool,
    ) -> Result<VertexDeleteResult> {
        validate_component("cell_id", cell_id)?;
        validate_component("idempotency_key", idempotency_key)?;
        let operation = if detach {
            "detach_delete_vertex"
        } else {
            "delete_vertex"
        };
        self.ensure_write_authority(cell_id, operation)?;

        let _permit = self.acquire_graph_write_permit(operation).await?;
        let _writer = self.writer_lane(cell_id).lock().await;
        for attempt in 0..GRAPH_TXN_MAX_RETRIES {
            let lock = self.acquire_cell_write_lock(cell_id, operation).await?;
            let result = self
                .delete_vertex_txn_locked(
                    cell_id,
                    vertex_id,
                    idempotency_key,
                    detach,
                    operation,
                    &lock,
                )
                .await;
            match release_cell_write_lock(lock, result).await {
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
        Err(GraphError::RetryExhausted {
            operation: "graph transaction",
            attempts: GRAPH_TXN_MAX_RETRIES,
        })
    }

    async fn delete_vertex_txn_locked(
        &self,
        cell_id: &str,
        vertex_id: VertexId,
        idempotency_key: &str,
        detach: bool,
        operation: &'static str,
        lock: &CellWriteLock,
    ) -> Result<VertexDeleteResult> {
        let idem_key = keys::idempotency(cell_id, "vertex-delete", idempotency_key);
        let txn = self
            .db
            .writer()?
            .begin(IsolationLevel::SerializableSnapshot)
            .await?;
        self.validate_write_fence_txn(&txn, cell_id, operation)
            .await?;
        if let Some(value) = read_txn_remote(&txn, &idem_key).await? {
            return decode_vertex_delete_idempotency(
                &idem_key,
                cell_id,
                vertex_id,
                idempotency_key,
                &value,
            );
        }
        let read_epoch = read_counter_txn(&txn, &keys::last_epoch(cell_id)).await?;
        drop(txn);

        let incident_edges = self
            .incident_edges_for_vertex_at(cell_id, vertex_id, read_epoch, lock)
            .await?;
        if !detach && !incident_edges.is_empty() {
            return Err(GraphError::UnsupportedQuery {
                dialect: "Graph",
                feature: format!(
                    "DELETE vertex {vertex_id} requires DETACH because it has {} incident edge(s)",
                    incident_edges.len()
                ),
            });
        }

        let mut incident_edges_deleted = 0_u64;
        let mut relationships_deleted = 0_u64;
        if detach {
            for (idx, edge) in incident_edges.into_iter().enumerate() {
                renew_vertex_delete_lock_after_items(lock, idx as u64).await?;
                let txn = self
                    .db
                    .writer()?
                    .begin(IsolationLevel::SerializableSnapshot)
                    .await?;
                self.validate_write_fence_txn(&txn, cell_id, operation)
                    .await?;
                let current_epoch = read_counter_txn(&txn, &keys::last_epoch(cell_id)).await?;
                let relationships = live_relationships_for_edge_txn(
                    &txn,
                    cell_id,
                    &edge.edge_type,
                    edge.src,
                    edge.dst,
                    current_epoch,
                )
                .await?;
                drop(txn);

                if relationships.is_empty() {
                    let mutation = EdgeMutation {
                        cell_id: cell_id.to_string(),
                        edge_type: edge.edge_type.clone(),
                        src: edge.src,
                        dst: edge.dst,
                        idempotency_key: format!(
                            "{idempotency_key}.detach-edge.{}.{}.{}",
                            edge.edge_type, edge.src, edge.dst
                        ),
                    };
                    let delete = self.delete_edge_txn_locked(&mutation).await?;
                    if delete.deleted {
                        incident_edges_deleted = incident_edges_deleted.saturating_add(1);
                    }
                    lock.renew().await?;
                    continue;
                }

                for relationship in relationships {
                    lock.renew().await?;
                    let mutation = EdgeMutation {
                        cell_id: cell_id.to_string(),
                        edge_type: relationship.edge_type.clone(),
                        src: relationship.src,
                        dst: relationship.dst,
                        idempotency_key: format!(
                            "{idempotency_key}.detach-rel.{}",
                            relationship.relationship_id
                        ),
                    };
                    let delete = self
                        .delete_relationship_txn_locked(&mutation, relationship.relationship_id)
                        .await?;
                    if delete.deleted {
                        relationships_deleted = relationships_deleted.saturating_add(1);
                    }
                }
                incident_edges_deleted = incident_edges_deleted.saturating_add(1);
                lock.renew().await?;
            }
        }

        self.delete_vertex_metadata_txn_locked(
            cell_id,
            vertex_id,
            idempotency_key,
            operation,
            incident_edges_deleted,
            relationships_deleted,
        )
        .await
    }

    async fn delete_vertex_metadata_txn_locked(
        &self,
        cell_id: &str,
        vertex_id: VertexId,
        idempotency_key: &str,
        operation: &'static str,
        incident_edges_deleted: u64,
        relationships_deleted: u64,
    ) -> Result<VertexDeleteResult> {
        let txn = self
            .db
            .writer()?
            .begin(IsolationLevel::SerializableSnapshot)
            .await?;
        self.validate_write_fence_txn(&txn, cell_id, operation)
            .await?;
        let idem_key = keys::idempotency(cell_id, "vertex-delete", idempotency_key);
        if let Some(value) = read_txn_remote(&txn, &idem_key).await? {
            return decode_vertex_delete_idempotency(
                &idem_key,
                cell_id,
                vertex_id,
                idempotency_key,
                &value,
            );
        }

        let current_epoch = read_counter_txn(&txn, &keys::last_epoch(cell_id)).await?;
        let vertex_key = keys::vertex(cell_id, vertex_id);
        let previous = match read_txn_remote(&txn, &vertex_key).await? {
            Some(value) => decode_vertex_metadata(&vertex_key, &value)?,
            None => VertexMetadata::default(),
        };
        let (epoch, vertex_deleted) = if previous.labels.is_empty()
            && previous.properties.is_empty()
        {
            (current_epoch, false)
        } else {
            let epoch = current_epoch
                .checked_add(1)
                .ok_or_else(|| GraphError::CorruptValue {
                    key: keys::last_epoch(cell_id),
                    reason: "epoch overflow during vertex delete".to_string(),
                })?;
            txn.put(keys::last_epoch(cell_id).as_bytes(), encode_u64(epoch))?;
            apply_vertex_metadata_update_txn(
                &txn,
                cell_id,
                vertex_id,
                &previous,
                &VertexMetadata::default(),
                epoch,
            )?;
            (epoch, true)
        };

        let result = VertexDeleteResult {
            epoch,
            vertex_deleted,
            incident_edges_deleted,
            relationships_deleted,
        };
        txn.put(
            idem_key.as_bytes(),
            encode_vertex_delete_idempotency(cell_id, vertex_id, &result),
        )?;
        commit_txn_strict(txn, self.await_durable_writes).await?;
        Ok(result)
    }

    async fn incident_edges_for_vertex_at(
        &self,
        cell_id: &str,
        vertex_id: VertexId,
        read_epoch: GraphEpoch,
        lock: &CellWriteLock,
    ) -> Result<BTreeSet<IncidentEdge>> {
        let mut edges = BTreeSet::new();
        let mut scanned = 0_u64;

        let mut out_iter = self
            .scan_remote_prefix(&keys::out_edge_cell_prefix(cell_id))
            .await?;
        while let Some(kv) = out_iter.next().await? {
            scanned = scanned.saturating_add(1);
            renew_vertex_delete_lock_after_items(lock, scanned).await?;
            ensure_limit(
                "delete_vertex_scan_edges",
                scanned,
                self.limits.max_query_scan_edges,
            )?;
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            let record = parse_edge_record_key(&key)?;
            if record.src == vertex_id || record.dst == vertex_id {
                edges.insert(IncidentEdge {
                    edge_type: record.edge_type,
                    src: record.src,
                    dst: record.dst,
                });
            }
        }

        let mut relationship_iter = self
            .scan_remote_prefix(&keys::relationship_cell_prefix(cell_id))
            .await?;
        while let Some(kv) = relationship_iter.next().await? {
            scanned = scanned.saturating_add(1);
            renew_vertex_delete_lock_after_items(lock, scanned).await?;
            ensure_limit(
                "delete_vertex_scan_relationships",
                scanned,
                self.limits.max_query_scan_edges,
            )?;
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            let record = decode_relationship_record(&key, &kv.value)?;
            if record.epoch > read_epoch {
                continue;
            }
            let tombstone_key = keys::relationship_tombstone(
                cell_id,
                &record.edge_type,
                record.src,
                record.dst,
                record.relationship_id,
            );
            if let Some(value) = self.read_remote(&tombstone_key).await? {
                let tombstone_epoch = decode_u64(&tombstone_key, &value)?;
                if record.epoch <= tombstone_epoch && tombstone_epoch <= read_epoch {
                    continue;
                }
            }
            if record.src == vertex_id || record.dst == vertex_id {
                edges.insert(IncidentEdge {
                    edge_type: record.edge_type,
                    src: record.src,
                    dst: record.dst,
                });
            }
        }

        let mut segment_iter = self
            .scan_remote_prefix(&keys::out_segment_cell_prefix(cell_id))
            .await?;
        while let Some(kv) = segment_iter.next().await? {
            scanned = scanned.saturating_add(1);
            renew_vertex_delete_lock_after_items(lock, scanned).await?;
            ensure_limit(
                "delete_vertex_scan_segments",
                scanned,
                self.limits.max_query_scan_edges,
            )?;
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            let segment = decode_out_edge_segment(&key, &kv.value)?;
            if segment.end_epoch > read_epoch {
                continue;
            }
            for (edge_epoch, dst) in segment.edges {
                if edge_epoch > read_epoch {
                    continue;
                }
                if segment.src != vertex_id && dst != vertex_id {
                    continue;
                }
                let tombstone_key =
                    keys::out_segment_tombstone(cell_id, &segment.edge_type, segment.src, dst);
                let tombstone_epoch = match self.read_remote(&tombstone_key).await? {
                    Some(value) => Some(decode_u64(&tombstone_key, &value)?),
                    None => None,
                };
                if segment_edge_visible(edge_epoch, tombstone_epoch) {
                    edges.insert(IncidentEdge {
                        edge_type: segment.edge_type.clone(),
                        src: segment.src,
                        dst,
                    });
                }
            }
        }

        ensure_limit(
            "delete_vertex_incident_edges",
            edges.len() as u64,
            self.limits.max_query_intermediate_rows as u64,
        )?;
        Ok(edges)
    }

    pub async fn drop_cell(
        &self,
        cell_id: &str,
        idempotency_key: &str,
    ) -> Result<GraphCellDropResult> {
        validate_component("cell_id", cell_id)?;
        validate_component("idempotency_key", idempotency_key)?;
        self.ensure_write_authority(cell_id, "drop_cell")?;

        let _permit = self.acquire_graph_write_permit("drop_cell").await?;
        let _writer = self.writer_lane(cell_id).lock().await;
        for attempt in 0..GRAPH_TXN_MAX_RETRIES {
            let lock = self.acquire_cell_write_lock(cell_id, "drop_cell").await?;
            let result = self.drop_cell_locked(cell_id, idempotency_key, &lock).await;
            match release_cell_write_lock(lock, result).await {
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
        Err(GraphError::RetryExhausted {
            operation: "graph transaction",
            attempts: GRAPH_TXN_MAX_RETRIES,
        })
    }

    async fn drop_cell_locked(
        &self,
        cell_id: &str,
        idempotency_key: &str,
        lock: &CellWriteLock,
    ) -> Result<GraphCellDropResult> {
        let idem_key = keys::cell_drop_idempotency(cell_id, idempotency_key);
        let marker_key = keys::cell_drop_marker(cell_id);
        let pending_marker_key = keys::cell_drop_pending_marker(cell_id);
        let write_fence_key = keys::write_fence(cell_id);
        let txn = self
            .db
            .writer()?
            .begin(IsolationLevel::SerializableSnapshot)
            .await?;
        if let Some(value) = read_txn_remote(&txn, &idem_key).await? {
            return decode_cell_drop_idempotency(&idem_key, cell_id, idempotency_key, &value);
        }
        if let Some(value) = read_txn_remote(&txn, &marker_key).await? {
            let result = GraphCellDropResult {
                marker_epoch: decode_u64(&marker_key, &value)?,
                deleted_keys: 0,
                batches: 0,
                already_dropped: true,
            };
            txn.put(
                idem_key.as_bytes(),
                encode_cell_drop_idempotency(cell_id, idempotency_key, &result),
            )?;
            commit_txn_strict(txn, self.await_durable_writes).await?;
            return Ok(result);
        }
        self.validate_write_fence_txn(&txn, cell_id, "drop_cell")
            .await?;
        let marker_epoch = match read_txn_remote(&txn, &pending_marker_key).await? {
            Some(value) => decode_u64(&pending_marker_key, &value)?,
            None => {
                let epoch = read_counter_txn(&txn, &keys::last_epoch(cell_id))
                    .await?
                    .saturating_add(1);
                txn.put(pending_marker_key.as_bytes(), encode_u64(epoch))?;
                epoch
            }
        };
        commit_txn_strict(txn, self.await_durable_writes).await?;

        self.wait_for_drop_read_leases(cell_id, lock).await?;
        lock.renew().await?;

        let mut deleted_keys = 0_u64;
        let mut batches = 0_u64;
        let mut pending = Vec::new();
        let mut iter = self.scan_remote_prefix(&keys::cell_prefix(cell_id)).await?;
        while let Some(kv) = iter.next().await? {
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            if key == write_fence_key || key == pending_marker_key {
                continue;
            }
            pending.push(key);
            if pending.len() >= GRAPH_DELTA_GC_BATCH_KEYS {
                lock.renew().await?;
                let deleted = self.flush_drop_cell_batch(cell_id, &mut pending).await?;
                lock.renew().await?;
                deleted_keys = deleted_keys.saturating_add(deleted);
                batches = batches.saturating_add(1);
            }
        }
        if !pending.is_empty() {
            lock.renew().await?;
            let deleted = self.flush_drop_cell_batch(cell_id, &mut pending).await?;
            lock.renew().await?;
            deleted_keys = deleted_keys.saturating_add(deleted);
            batches = batches.saturating_add(1);
        }

        let result = GraphCellDropResult {
            marker_epoch,
            deleted_keys,
            batches,
            already_dropped: false,
        };
        let txn = self
            .db
            .writer()?
            .begin(IsolationLevel::SerializableSnapshot)
            .await?;
        self.validate_write_fence_txn(&txn, cell_id, "drop_cell")
            .await?;
        if let Some(value) = read_txn_remote(&txn, &idem_key).await? {
            return decode_cell_drop_idempotency(&idem_key, cell_id, idempotency_key, &value);
        }
        txn.put(marker_key.as_bytes(), encode_u64(marker_epoch))?;
        txn.delete(pending_marker_key.as_bytes())?;
        txn.delete(write_fence_key.as_bytes())?;
        txn.put(
            idem_key.as_bytes(),
            encode_cell_drop_idempotency(cell_id, idempotency_key, &result),
        )?;
        commit_txn_strict(txn, self.await_durable_writes).await?;
        Ok(result)
    }

    async fn flush_drop_cell_batch(
        &self,
        cell_id: &str,
        keys_to_delete: &mut Vec<String>,
    ) -> Result<u64> {
        if keys_to_delete.is_empty() {
            return Ok(0);
        }
        let keys = std::mem::take(keys_to_delete);
        let txn = self
            .db
            .writer()?
            .begin(IsolationLevel::SerializableSnapshot)
            .await?;
        self.validate_write_fence_txn(&txn, cell_id, "drop_cell")
            .await?;
        for key in &keys {
            txn.delete(key.as_bytes())?;
        }
        let deleted = keys.len() as u64;
        commit_txn_strict(txn, self.await_durable_writes).await?;
        Ok(deleted)
    }

    async fn wait_for_drop_read_leases(&self, cell_id: &str, lock: &CellWriteLock) -> Result<()> {
        if self.retention_policy.read_lease_ttl_ms == 0 {
            return Ok(());
        }
        let started_ms = graph_now_millis();
        let timeout_ms = self
            .retention_policy
            .read_lease_ttl_ms
            .saturating_add(1_000);
        loop {
            let Some(read_epoch) = self.min_active_read_epoch(cell_id).await? else {
                return Ok(());
            };
            if graph_now_millis().saturating_sub(started_ms) >= timeout_ms {
                return Err(GraphError::ActiveReadLease {
                    operation: "drop_cell",
                    cell_id: cell_id.to_string(),
                    read_epoch,
                });
            }
            lock.renew().await?;
            let sleep_ms = self.retention_policy.read_lease_ttl_ms.clamp(1, 50);
            tokio::time::sleep(std::time::Duration::from_millis(sleep_ms)).await;
        }
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
        Err(GraphError::RetryExhausted {
            operation: "graph transaction",
            attempts: GRAPH_TXN_MAX_RETRIES,
        })
    }

    pub async fn set_edge_metadata_batch(
        &self,
        cell_id: &str,
        edge_type: &str,
        updates: impl IntoIterator<Item = (VertexId, VertexId, EdgeMetadata)>,
    ) -> Result<usize> {
        validate_component("cell_id", cell_id)?;
        validate_component("edge_type", edge_type)?;
        self.ensure_write_authority(cell_id, "set_edge_metadata_batch")?;
        let updates = coalesce_edge_metadata_updates(updates)?;
        if updates.is_empty() {
            return Ok(0);
        }
        ensure_limit(
            "set_edge_metadata_batch",
            updates.len() as u64,
            self.limits.max_bulk_import_edges as u64,
        )?;
        let _permit = self
            .acquire_graph_write_permit("set_edge_metadata_batch")
            .await?;
        let _writer = self.writer_lane(cell_id).lock().await;
        for attempt in 0..GRAPH_TXN_MAX_RETRIES {
            match self
                .set_edge_metadata_batch_txn(cell_id, edge_type, updates.clone())
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
                Ok(changed) => {
                    if changed > 0 {
                        self.operation_metrics
                            .write_commits
                            .fetch_add(1, Ordering::Relaxed);
                    }
                    return Ok(changed);
                }
                result => return result,
            }
        }
        Err(GraphError::RetryExhausted {
            operation: "graph transaction",
            attempts: GRAPH_TXN_MAX_RETRIES,
        })
    }

    pub async fn import_relationships_batch(
        &self,
        cell_id: &str,
        edge_type: &str,
        relationships: impl IntoIterator<Item = RelationshipMutation>,
        idempotency_key: &str,
    ) -> Result<RelationshipImportResult> {
        validate_component("cell_id", cell_id)?;
        validate_component("edge_type", edge_type)?;
        validate_component("idempotency_key", idempotency_key)?;
        self.ensure_write_authority(cell_id, "import_relationships_batch")?;

        let mut relationships = coalesce_relationship_imports(cell_id, edge_type, relationships)?;
        if relationships.is_empty() {
            let epoch = self.current_epoch(cell_id).await?;
            return Ok(RelationshipImportResult {
                start_epoch: epoch,
                end_epoch: epoch,
                relationships_inserted: 0,
                relationships_already_existed: 0,
                structural_edges_inserted: 0,
                structural_edges_already_existed: 0,
            });
        }
        ensure_limit(
            "import_relationships_batch",
            relationships.len() as u64,
            self.limits.max_bulk_import_edges as u64,
        )?;
        relationships.sort_by_key(|relationship| {
            (
                relationship.relationship_id,
                relationship.src,
                relationship.dst,
                relationship.edge_type.clone(),
            )
        });
        let fingerprint = relationship_import_fingerprint(cell_id, edge_type, &relationships);

        let _permit = self
            .acquire_graph_write_permit("import_relationships_batch")
            .await?;
        let _writer = self.writer_lane(cell_id).lock().await;
        for attempt in 0..GRAPH_TXN_MAX_RETRIES {
            match self
                .import_relationships_batch_txn(
                    cell_id,
                    edge_type,
                    &relationships,
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
        Err(GraphError::RetryExhausted {
            operation: "graph transaction",
            attempts: GRAPH_TXN_MAX_RETRIES,
        })
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
        let txn = self
            .db
            .writer()?
            .begin(IsolationLevel::SerializableSnapshot)
            .await?;
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

    async fn set_edge_metadata_batch_txn(
        &self,
        cell_id: &str,
        edge_type: &str,
        updates: Vec<(VertexId, VertexId, EdgeMetadata)>,
    ) -> Result<usize> {
        let lock = self
            .acquire_cell_write_lock(cell_id, "set_edge_metadata_batch")
            .await?;
        let result = self
            .set_edge_metadata_batch_txn_locked(cell_id, edge_type, updates)
            .await;
        release_cell_write_lock(lock, result).await
    }

    async fn set_edge_metadata_batch_txn_locked(
        &self,
        cell_id: &str,
        edge_type: &str,
        updates: Vec<(VertexId, VertexId, EdgeMetadata)>,
    ) -> Result<usize> {
        let txn = self
            .db
            .writer()?
            .begin(IsolationLevel::SerializableSnapshot)
            .await?;
        self.validate_write_fence_txn(&txn, cell_id, "set_edge_metadata_batch")
            .await?;
        let current_epoch = read_counter_txn(&txn, &keys::last_epoch(cell_id)).await?;
        let mut changed = Vec::new();
        for (src, dst, metadata) in updates {
            if edge_epoch_at_txn(&txn, cell_id, edge_type, src, dst, current_epoch)
                .await?
                .is_none()
            {
                return Err(GraphError::UnsupportedQuery {
                    dialect: "GraphQuery",
                    feature: format!(
                        "cannot set metadata for missing edge {edge_type}({src}->{dst})"
                    ),
                });
            }
            let edge_metadata_key = keys::edge_metadata(cell_id, edge_type, src, dst);
            let previous = match read_txn_remote(&txn, &edge_metadata_key).await? {
                Some(value) => decode_edge_metadata(&edge_metadata_key, &value)?,
                None => EdgeMetadata::default(),
            };
            if previous != metadata {
                changed.push((src, dst, previous, metadata));
            }
        }
        if changed.is_empty() {
            return Ok(0);
        }
        let epoch = next_epoch_txn(&txn, cell_id).await?;
        txn.put(keys::last_epoch(cell_id).as_bytes(), encode_u64(epoch))?;
        for (src, dst, previous, metadata) in &changed {
            apply_edge_metadata_update_txn(
                &txn,
                EdgeMetadataTarget {
                    cell_id,
                    edge_type,
                    src: *src,
                    dst: *dst,
                },
                previous,
                metadata,
                epoch,
            )?;
        }
        let changed_count = changed.len();
        commit_txn_strict(txn, self.await_durable_writes).await?;
        Ok(changed_count)
    }

    async fn import_relationships_batch_txn(
        &self,
        cell_id: &str,
        edge_type: &str,
        relationships: &[RelationshipMutation],
        idempotency_key: &str,
        fingerprint: u64,
    ) -> Result<RelationshipImportResult> {
        let lock = self
            .acquire_cell_write_lock(cell_id, "import_relationships_batch")
            .await?;
        let result = self
            .import_relationships_batch_txn_locked(
                cell_id,
                edge_type,
                relationships,
                idempotency_key,
                fingerprint,
            )
            .await;
        release_cell_write_lock(lock, result).await
    }

    async fn import_relationships_batch_txn_locked(
        &self,
        cell_id: &str,
        edge_type: &str,
        relationships: &[RelationshipMutation],
        idempotency_key: &str,
        fingerprint: u64,
    ) -> Result<RelationshipImportResult> {
        let txn = self
            .db
            .writer()?
            .begin(IsolationLevel::SerializableSnapshot)
            .await?;
        self.validate_write_fence_txn(&txn, cell_id, "import_relationships_batch")
            .await?;
        let idem_key = keys::idempotency(cell_id, "relationship-import", idempotency_key);
        if let Some(value) = read_txn_remote(&txn, &idem_key).await? {
            return decode_relationship_import_idempotency(
                &idem_key,
                idempotency_key,
                fingerprint,
                &value,
            );
        }

        let current_epoch = read_counter_txn(&txn, &keys::last_epoch(cell_id)).await?;
        let current_relationship_id =
            read_counter_txn(&txn, &keys::last_relationship_id(cell_id)).await?;
        let max_requested_relationship_id = relationships
            .iter()
            .map(|relationship| relationship.relationship_id)
            .max()
            .unwrap_or(0);
        let fresh_cell = current_epoch == 0;
        let mut relationships_inserted = Vec::new();
        let mut relationships_already_existed = 0_u64;
        for relationship in relationships {
            let rel_key = keys::relationship(
                cell_id,
                edge_type,
                relationship.src,
                relationship.dst,
                relationship.relationship_id,
            );
            let id_key = keys::relationship_id(cell_id, relationship.relationship_id);
            let existing = match read_txn_remote(&txn, &id_key).await? {
                Some(value) => {
                    let target_key =
                        std::str::from_utf8(&value).map_err(|err| GraphError::CorruptValue {
                            key: id_key.clone(),
                            reason: format!("relationship id pointer is not UTF-8: {err}"),
                        })?;
                    match read_txn_remote(&txn, target_key).await? {
                        Some(value) => {
                            let record = decode_relationship_record(target_key, &value)?;
                            if relationship_deleted_at_txn(&txn, &record, current_epoch).await? {
                                txn.delete(id_key.as_bytes())?;
                                None
                            } else {
                                Some(record)
                            }
                        }
                        None => {
                            return Err(GraphError::CorruptValue {
                                key: id_key,
                                reason: format!(
                                    "relationship id points at missing record {target_key}"
                                ),
                            });
                        }
                    }
                }
                None => match read_txn_remote(&txn, &rel_key).await? {
                    Some(value) => {
                        let record = decode_relationship_record(&rel_key, &value)?;
                        if relationship_deleted_at_txn(&txn, &record, current_epoch).await? {
                            None
                        } else {
                            Some(record)
                        }
                    }
                    None => None,
                },
            };
            if let Some(existing) = existing {
                let requested = RelationshipRecord {
                    cell_id: cell_id.to_string(),
                    edge_type: edge_type.to_string(),
                    src: relationship.src,
                    dst: relationship.dst,
                    relationship_id: relationship.relationship_id,
                    epoch: existing.epoch,
                    metadata: relationship.metadata.clone(),
                };
                if existing != requested {
                    return Err(GraphError::IdempotencyConflict {
                        operation: "relationship-import",
                        idempotency_key: idempotency_key.to_string(),
                    });
                }
                relationships_already_existed = relationships_already_existed.saturating_add(1);
            } else {
                relationships_inserted.push(relationship.clone());
            }
        }

        let mut structural_edges = BTreeSet::<(VertexId, VertexId)>::new();
        let mut structural_edges_already_existed = 0_u64;
        let mut segment_neighbors_by_src = BTreeMap::<VertexId, BTreeSet<VertexId>>::new();
        for relationship in &relationships_inserted {
            if !structural_edges.insert((relationship.src, relationship.dst)) {
                continue;
            }
            if !fresh_cell {
                if read_txn_remote(
                    &txn,
                    &keys::out_edge(cell_id, edge_type, relationship.src, relationship.dst),
                )
                .await?
                .is_some()
                {
                    structural_edges_already_existed =
                        structural_edges_already_existed.saturating_add(1);
                    continue;
                }
                let segment_exists = match segment_neighbors_by_src.get(&relationship.src) {
                    Some(neighbors) => neighbors.contains(&relationship.dst),
                    None => {
                        let neighbors = out_segment_neighbors_for_src_txn(
                            &txn,
                            cell_id,
                            edge_type,
                            relationship.src,
                            current_epoch,
                        )
                        .await?;
                        let exists = neighbors.contains(&relationship.dst);
                        segment_neighbors_by_src.insert(relationship.src, neighbors);
                        exists
                    }
                };
                if segment_exists {
                    structural_edges_already_existed =
                        structural_edges_already_existed.saturating_add(1);
                    continue;
                }
            }
        }
        let structural_edges_inserted =
            u64::try_from(structural_edges.len()).map_err(|err| GraphError::CorruptValue {
                key: "relationship_import".to_string(),
                reason: format!("too many structural edges in one import: {err}"),
            })? - structural_edges_already_existed;
        let relationships_inserted_count =
            u64::try_from(relationships_inserted.len()).map_err(|err| {
                GraphError::CorruptValue {
                    key: "relationship_import".to_string(),
                    reason: format!("too many relationships in one import: {err}"),
                }
            })?;
        let changed = relationships_inserted_count > 0 || structural_edges_inserted > 0;
        let epoch = if changed {
            current_epoch
                .checked_add(1)
                .ok_or_else(|| GraphError::CorruptValue {
                    key: keys::last_epoch(cell_id),
                    reason: "epoch overflow during relationship import".to_string(),
                })?
        } else {
            current_epoch
        };
        let result = RelationshipImportResult {
            start_epoch: epoch,
            end_epoch: epoch,
            relationships_inserted: relationships_inserted_count,
            relationships_already_existed,
            structural_edges_inserted,
            structural_edges_already_existed,
        };

        let write_reverse_index = self.writes_reverse_index();
        let mut out_increments = BTreeMap::<VertexId, u64>::new();
        let mut in_increments = BTreeMap::<VertexId, u64>::new();
        for (src, dst) in structural_edges {
            if !fresh_cell
                && edge_epoch_at_txn(&txn, cell_id, edge_type, src, dst, current_epoch)
                    .await?
                    .is_some()
            {
                continue;
            }
            let record = EdgeRecord {
                cell_id: cell_id.to_string(),
                edge_type: edge_type.to_string(),
                src,
                dst,
                epoch,
            };
            let edge_value = encode_edge_record(&record);
            let delta = DeltaRecord {
                kind: DeltaKind::Plus,
                edge: record,
            };
            let delta_value = encode_delta_record(&delta);
            put_scoped_delta_indexes_txn(&txn, &delta)?;
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
            txn.put(
                keys::outbox(cell_id, epoch, DeltaKind::Plus, edge_type, src, dst).as_bytes(),
                &delta_value,
            )?;
            *out_increments.entry(src).or_insert(0) += 1;
            if write_reverse_index {
                *in_increments.entry(dst).or_insert(0) += 1;
            }
        }
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
        for relationship in &relationships_inserted {
            let record = RelationshipRecord {
                cell_id: cell_id.to_string(),
                edge_type: edge_type.to_string(),
                src: relationship.src,
                dst: relationship.dst,
                relationship_id: relationship.relationship_id,
                epoch,
                metadata: relationship.metadata.clone(),
            };
            let value = encode_relationship_record(&record);
            txn.put(
                keys::relationship(
                    cell_id,
                    edge_type,
                    relationship.src,
                    relationship.dst,
                    relationship.relationship_id,
                )
                .as_bytes(),
                &value,
            )?;
            txn.put(
                keys::relationship_id(cell_id, relationship.relationship_id).as_bytes(),
                keys::relationship(
                    cell_id,
                    edge_type,
                    relationship.src,
                    relationship.dst,
                    relationship.relationship_id,
                )
                .as_bytes(),
            )?;
            put_relationship_metadata_delta_txn(&txn, &record, &record.metadata, epoch)?;
            put_relationship_property_indexes_txn(&txn, &record)?;
            put_relationship_property_index_deltas_txn(
                &txn,
                &record,
                &EdgeMetadata::default(),
                &record.metadata,
                epoch,
            )?;
        }
        let mut relationship_count_increments = BTreeMap::<(VertexId, VertexId), u64>::new();
        for relationship in &relationships_inserted {
            *relationship_count_increments
                .entry((relationship.src, relationship.dst))
                .or_insert(0) += 1;
        }
        for ((src, dst), increment) in relationship_count_increments {
            let key = keys::relationship_count(cell_id, edge_type, src, dst);
            let base = if fresh_cell {
                0
            } else {
                read_counter_txn(&txn, &key).await?
            };
            txn.put(key.as_bytes(), encode_u64(base + increment))?;
        }
        if max_requested_relationship_id > current_relationship_id {
            txn.put(
                keys::last_relationship_id(cell_id).as_bytes(),
                encode_u64(max_requested_relationship_id),
            )?;
        }
        if changed {
            txn.put(keys::last_epoch(cell_id).as_bytes(), encode_u64(epoch))?;
        }
        txn.put(
            idem_key.as_bytes(),
            encode_relationship_import_idempotency(idempotency_key, fingerprint, &result),
        )?;
        commit_txn_strict(txn, self.await_durable_writes).await?;
        Ok(result)
    }

    pub async fn create_relationship(
        &self,
        mutation: EdgeMutation,
        edge_metadata: EdgeMetadata,
    ) -> Result<RelationshipCreateResult> {
        self.create_relationship_with_full_metadata(
            mutation,
            VertexMetadata::default(),
            VertexMetadata::default(),
            edge_metadata,
        )
        .await
    }

    pub async fn create_relationship_with_vertex_metadata(
        &self,
        mutation: EdgeMutation,
        src_metadata: VertexMetadata,
        dst_metadata: VertexMetadata,
    ) -> Result<RelationshipCreateResult> {
        self.create_relationship_with_full_metadata(
            mutation,
            src_metadata,
            dst_metadata,
            EdgeMetadata::default(),
        )
        .await
    }

    pub async fn create_relationship_with_full_metadata(
        &self,
        mutation: EdgeMutation,
        src_metadata: VertexMetadata,
        dst_metadata: VertexMetadata,
        edge_metadata: EdgeMetadata,
    ) -> Result<RelationshipCreateResult> {
        validate_component("cell_id", &mutation.cell_id)?;
        validate_component("edge_type", &mutation.edge_type)?;
        validate_component("idempotency_key", &mutation.idempotency_key)?;
        validate_vertex_metadata(&src_metadata)?;
        validate_vertex_metadata(&dst_metadata)?;
        validate_edge_metadata(&edge_metadata)?;
        self.ensure_write_authority(&mutation.cell_id, "create_relationship")?;

        let metadata_updates = coalesce_vertex_metadata_updates([
            (mutation.src, src_metadata),
            (mutation.dst, dst_metadata),
        ])?;
        let fingerprint =
            relationship_create_fingerprint(&mutation, metadata_updates.as_slice(), &edge_metadata);
        let _permit = self
            .acquire_graph_write_permit("create_relationship")
            .await?;
        let _writer = self.writer_lane(&mutation.cell_id).lock().await;
        for attempt in 0..GRAPH_TXN_MAX_RETRIES {
            match self
                .create_relationship_txn(&mutation, &metadata_updates, &edge_metadata, fingerprint)
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
        Err(GraphError::RetryExhausted {
            operation: "graph transaction",
            attempts: GRAPH_TXN_MAX_RETRIES,
        })
    }

    async fn create_relationship_txn(
        &self,
        mutation: &EdgeMutation,
        metadata_updates: &[(VertexId, VertexMetadata)],
        edge_metadata: &EdgeMetadata,
        fingerprint: u64,
    ) -> Result<RelationshipCreateResult> {
        let lock = self
            .acquire_cell_write_lock(&mutation.cell_id, "create_relationship")
            .await?;
        let result = self
            .create_relationship_txn_locked(mutation, metadata_updates, edge_metadata, fingerprint)
            .await;
        release_cell_write_lock(lock, result).await
    }

    async fn create_relationship_txn_locked(
        &self,
        mutation: &EdgeMutation,
        metadata_updates: &[(VertexId, VertexMetadata)],
        edge_metadata: &EdgeMetadata,
        fingerprint: u64,
    ) -> Result<RelationshipCreateResult> {
        let txn = self
            .db
            .writer()?
            .begin(IsolationLevel::SerializableSnapshot)
            .await?;
        self.validate_write_fence_txn(&txn, &mutation.cell_id, "create_relationship")
            .await?;
        let idem_key = keys::idempotency(
            &mutation.cell_id,
            "relationship-create",
            &mutation.idempotency_key,
        );
        if let Some(value) = read_txn_remote(&txn, &idem_key).await? {
            return decode_relationship_create_idempotency(
                &idem_key,
                mutation,
                fingerprint,
                &value,
            );
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
        let structural_edge_inserted = existing_edge_epoch.is_none();
        let epoch = current_epoch
            .checked_add(1)
            .ok_or_else(|| GraphError::CorruptValue {
                key: keys::last_epoch(&mutation.cell_id),
                reason: "epoch overflow during relationship create".to_string(),
            })?;

        let mut relationship_id =
            read_counter_txn(&txn, &keys::last_relationship_id(&mutation.cell_id))
                .await?
                .checked_add(1)
                .ok_or_else(|| GraphError::CorruptValue {
                    key: keys::last_relationship_id(&mutation.cell_id),
                    reason: "relationship id overflow".to_string(),
                })?;
        loop {
            if read_txn_remote(
                &txn,
                &keys::relationship_id(&mutation.cell_id, relationship_id),
            )
            .await?
            .is_none()
            {
                break;
            }
            relationship_id =
                relationship_id
                    .checked_add(1)
                    .ok_or_else(|| GraphError::CorruptValue {
                        key: keys::last_relationship_id(&mutation.cell_id),
                        reason: "relationship id overflow".to_string(),
                    })?;
        }

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

        txn.put(
            keys::last_epoch(&mutation.cell_id).as_bytes(),
            encode_u64(epoch),
        )?;
        txn.put(
            keys::last_relationship_id(&mutation.cell_id).as_bytes(),
            encode_u64(relationship_id),
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

        if structural_edge_inserted {
            let record = EdgeRecord {
                cell_id: mutation.cell_id.clone(),
                edge_type: mutation.edge_type.clone(),
                src: mutation.src,
                dst: mutation.dst,
                epoch,
            };
            let edge_value = encode_edge_record(&record);
            let delta = DeltaRecord {
                kind: DeltaKind::Plus,
                edge: record,
            };
            let delta_value = encode_delta_record(&delta);
            put_scoped_delta_indexes_txn(&txn, &delta)?;
            let out_degree_key =
                keys::degree_out(&mutation.cell_id, &mutation.edge_type, mutation.src);
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
        }

        let record = RelationshipRecord {
            cell_id: mutation.cell_id.clone(),
            edge_type: mutation.edge_type.clone(),
            src: mutation.src,
            dst: mutation.dst,
            relationship_id,
            epoch,
            metadata: edge_metadata.clone(),
        };
        let relationship_key = keys::relationship(
            &mutation.cell_id,
            &mutation.edge_type,
            mutation.src,
            mutation.dst,
            relationship_id,
        );
        txn.put(
            relationship_key.as_bytes(),
            encode_relationship_record(&record),
        )?;
        txn.put(
            keys::relationship_id(&mutation.cell_id, relationship_id).as_bytes(),
            relationship_key.as_bytes(),
        )?;
        let relationship_count_key = keys::relationship_count(
            &mutation.cell_id,
            &mutation.edge_type,
            mutation.src,
            mutation.dst,
        );
        let relationship_count = read_counter_txn(&txn, &relationship_count_key).await? + 1;
        txn.put(
            relationship_count_key.as_bytes(),
            encode_u64(relationship_count),
        )?;
        put_relationship_metadata_delta_txn(&txn, &record, edge_metadata, epoch)?;
        put_relationship_property_indexes_txn(&txn, &record)?;
        put_relationship_property_index_deltas_txn(
            &txn,
            &record,
            &EdgeMetadata::default(),
            edge_metadata,
            epoch,
        )?;

        let result = RelationshipCreateResult {
            epoch,
            relationship_id,
            structural_edge_inserted,
            already_created: false,
        };
        txn.put(
            idem_key.as_bytes(),
            encode_relationship_create_idempotency(mutation, fingerprint, &result),
        )?;
        commit_txn_strict(txn, self.await_durable_writes).await?;
        Ok(result)
    }

    pub async fn set_relationship_metadata(
        &self,
        cell_id: &str,
        edge_type: &str,
        src: VertexId,
        dst: VertexId,
        relationship_id: RelationshipId,
        metadata: EdgeMetadata,
    ) -> Result<bool> {
        validate_component("cell_id", cell_id)?;
        validate_component("edge_type", edge_type)?;
        validate_edge_metadata(&metadata)?;
        self.ensure_write_authority(cell_id, "set_relationship_metadata")?;
        let _permit = self
            .acquire_graph_write_permit("set_relationship_metadata")
            .await?;
        let _writer = self.writer_lane(cell_id).lock().await;
        for attempt in 0..GRAPH_TXN_MAX_RETRIES {
            match self
                .set_relationship_metadata_txn(
                    cell_id,
                    edge_type,
                    src,
                    dst,
                    relationship_id,
                    metadata.clone(),
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
                Ok(changed) => {
                    if changed {
                        self.operation_metrics
                            .write_commits
                            .fetch_add(1, Ordering::Relaxed);
                    }
                    return Ok(changed);
                }
                result => return result,
            }
        }
        Err(GraphError::RetryExhausted {
            operation: "graph transaction",
            attempts: GRAPH_TXN_MAX_RETRIES,
        })
    }

    async fn set_relationship_metadata_txn(
        &self,
        cell_id: &str,
        edge_type: &str,
        src: VertexId,
        dst: VertexId,
        relationship_id: RelationshipId,
        metadata: EdgeMetadata,
    ) -> Result<bool> {
        let lock = self
            .acquire_cell_write_lock(cell_id, "set_relationship_metadata")
            .await?;
        let result = self
            .set_relationship_metadata_txn_locked(
                cell_id,
                edge_type,
                src,
                dst,
                relationship_id,
                metadata,
            )
            .await;
        release_cell_write_lock(lock, result).await
    }

    async fn set_relationship_metadata_txn_locked(
        &self,
        cell_id: &str,
        edge_type: &str,
        src: VertexId,
        dst: VertexId,
        relationship_id: RelationshipId,
        metadata: EdgeMetadata,
    ) -> Result<bool> {
        let txn = self
            .db
            .writer()?
            .begin(IsolationLevel::SerializableSnapshot)
            .await?;
        self.validate_write_fence_txn(&txn, cell_id, "set_relationship_metadata")
            .await?;
        let current_epoch = read_counter_txn(&txn, &keys::last_epoch(cell_id)).await?;
        let key = keys::relationship(cell_id, edge_type, src, dst, relationship_id);
        let Some(value) = read_txn_remote(&txn, &key).await? else {
            return Err(GraphError::UnsupportedQuery {
                dialect: "GraphQuery",
                feature: "cannot set metadata for a missing relationship".to_string(),
            });
        };
        let mut record = decode_relationship_record(&key, &value)?;
        if record.epoch > current_epoch
            || relationship_deleted_at_txn(&txn, &record, current_epoch).await?
        {
            return Err(GraphError::UnsupportedQuery {
                dialect: "GraphQuery",
                feature: "cannot set metadata for a deleted relationship".to_string(),
            });
        }
        if record.metadata == metadata {
            return Ok(false);
        }
        let previous = record.metadata.clone();
        let epoch = next_epoch_txn(&txn, cell_id).await?;
        record.metadata = metadata.clone();
        txn.put(
            key.as_bytes(),
            encode_relationship_record(&record).as_slice(),
        )?;
        put_relationship_metadata_delta_txn(&txn, &record, &metadata, epoch)?;
        delete_relationship_property_indexes_txn(&txn, &record, &previous)?;
        put_relationship_property_indexes_txn(&txn, &record)?;
        put_relationship_property_index_deltas_txn(&txn, &record, &previous, &metadata, epoch)?;
        txn.put(keys::last_epoch(cell_id).as_bytes(), encode_u64(epoch))?;
        commit_txn_strict(txn, self.await_durable_writes).await?;
        Ok(true)
    }

    pub async fn delete_relationship(
        &self,
        mutation: EdgeMutation,
        relationship_id: RelationshipId,
    ) -> Result<DeleteResult> {
        validate_component("cell_id", &mutation.cell_id)?;
        validate_component("edge_type", &mutation.edge_type)?;
        validate_component("idempotency_key", &mutation.idempotency_key)?;
        self.ensure_write_authority(&mutation.cell_id, "delete_relationship")?;

        let _permit = self
            .acquire_graph_write_permit("delete_relationship")
            .await?;
        let _writer = self.writer_lane(&mutation.cell_id).lock().await;
        for attempt in 0..GRAPH_TXN_MAX_RETRIES {
            match self
                .delete_relationship_txn(&mutation, relationship_id)
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
        Err(GraphError::RetryExhausted {
            operation: "graph transaction",
            attempts: GRAPH_TXN_MAX_RETRIES,
        })
    }

    async fn delete_relationship_txn(
        &self,
        mutation: &EdgeMutation,
        relationship_id: RelationshipId,
    ) -> Result<DeleteResult> {
        let lock = self
            .acquire_cell_write_lock(&mutation.cell_id, "delete_relationship")
            .await?;
        let result = self
            .delete_relationship_txn_locked(mutation, relationship_id)
            .await;
        release_cell_write_lock(lock, result).await
    }

    async fn delete_relationship_txn_locked(
        &self,
        mutation: &EdgeMutation,
        relationship_id: RelationshipId,
    ) -> Result<DeleteResult> {
        let txn = self
            .db
            .writer()?
            .begin(IsolationLevel::SerializableSnapshot)
            .await?;
        self.validate_write_fence_txn(&txn, &mutation.cell_id, "delete_relationship")
            .await?;
        let idem_key = keys::idempotency(
            &mutation.cell_id,
            "relationship-delete",
            &mutation.idempotency_key,
        );
        if let Some(value) = read_txn_remote(&txn, &idem_key).await? {
            return decode_relationship_delete_idempotency(
                &idem_key,
                mutation,
                relationship_id,
                &value,
            );
        }

        let current_epoch = read_counter_txn(&txn, &keys::last_epoch(&mutation.cell_id)).await?;
        let key = keys::relationship(
            &mutation.cell_id,
            &mutation.edge_type,
            mutation.src,
            mutation.dst,
            relationship_id,
        );
        let Some(value) = read_txn_remote(&txn, &key).await? else {
            let result = DeleteResult {
                epoch: current_epoch,
                deleted: false,
            };
            txn.put(
                idem_key.as_bytes(),
                encode_relationship_delete_idempotency(mutation, relationship_id, &result),
            )?;
            commit_txn_strict(txn, self.await_durable_writes).await?;
            return Ok(result);
        };
        let record = decode_relationship_record(&key, &value)?;
        if relationship_deleted_at_txn(&txn, &record, current_epoch).await? {
            let result = DeleteResult {
                epoch: current_epoch,
                deleted: false,
            };
            txn.put(
                idem_key.as_bytes(),
                encode_relationship_delete_idempotency(mutation, relationship_id, &result),
            )?;
            commit_txn_strict(txn, self.await_durable_writes).await?;
            return Ok(result);
        }

        let epoch = current_epoch
            .checked_add(1)
            .ok_or_else(|| GraphError::CorruptValue {
                key: keys::last_epoch(&mutation.cell_id),
                reason: "epoch overflow during relationship delete".to_string(),
            })?;
        let other_live_relationships = live_relationships_for_edge_txn(
            &txn,
            &mutation.cell_id,
            &mutation.edge_type,
            mutation.src,
            mutation.dst,
            current_epoch,
        )
        .await?
        .into_iter()
        .any(|record| record.relationship_id != relationship_id);

        txn.put(
            keys::last_epoch(&mutation.cell_id).as_bytes(),
            encode_u64(epoch),
        )?;
        txn.put(
            keys::relationship_tombstone(
                &mutation.cell_id,
                &mutation.edge_type,
                mutation.src,
                mutation.dst,
                relationship_id,
            )
            .as_bytes(),
            encode_u64(epoch),
        )?;
        txn.delete(keys::relationship_id(&mutation.cell_id, relationship_id).as_bytes())?;
        let relationship_count_key = keys::relationship_count(
            &mutation.cell_id,
            &mutation.edge_type,
            mutation.src,
            mutation.dst,
        );
        let relationship_count = read_counter_txn(&txn, &relationship_count_key)
            .await?
            .saturating_sub(1);
        if relationship_count == 0 {
            txn.delete(relationship_count_key.as_bytes())?;
        } else {
            txn.put(
                relationship_count_key.as_bytes(),
                encode_u64(relationship_count),
            )?;
        }
        delete_relationship_property_indexes_txn(&txn, &record, &record.metadata)?;
        put_relationship_property_index_deltas_txn(
            &txn,
            &record,
            &record.metadata,
            &EdgeMetadata::default(),
            epoch,
        )?;

        if !other_live_relationships {
            delete_structural_edge_txn(self, &txn, mutation, epoch).await?;
        }

        let result = DeleteResult {
            epoch,
            deleted: true,
        };
        txn.put(
            idem_key.as_bytes(),
            encode_relationship_delete_idempotency(mutation, relationship_id, &result),
        )?;
        commit_txn_strict(txn, self.await_durable_writes).await?;
        Ok(result)
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
        Err(GraphError::RetryExhausted {
            operation: "graph transaction",
            attempts: GRAPH_TXN_MAX_RETRIES,
        })
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
        Err(GraphError::RetryExhausted {
            operation: "graph transaction",
            attempts: GRAPH_TXN_MAX_RETRIES,
        })
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
        Err(GraphError::RetryExhausted {
            operation: "graph transaction",
            attempts: GRAPH_TXN_MAX_RETRIES,
        })
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
        let txn = self
            .db
            .writer()?
            .begin(IsolationLevel::SerializableSnapshot)
            .await?;
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
        let delta = DeltaRecord {
            kind: DeltaKind::Plus,
            edge: record.clone(),
        };
        let delta_value = encode_delta_record(&delta);
        put_scoped_delta_indexes_txn(&txn, &delta)?;
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

    pub async fn delete_edges_batch(
        &self,
        cell_id: &str,
        edge_type: &str,
        edges: impl IntoIterator<Item = (VertexId, VertexId)>,
        idempotency_key: &str,
    ) -> Result<EdgeDeleteBatchResult> {
        validate_component("cell_id", cell_id)?;
        validate_component("edge_type", edge_type)?;
        validate_component("idempotency_key", idempotency_key)?;
        let mut edges: Vec<_> = edges.into_iter().collect();
        ensure_limit(
            "delete_edges_batch",
            edges.len() as u64,
            self.limits.max_bulk_import_edges as u64,
        )?;
        edges.sort_unstable();
        edges.dedup();
        let mutations = edges.into_iter().map(|(src, dst)| EdgeMutation {
            cell_id: cell_id.to_string(),
            edge_type: edge_type.to_string(),
            src,
            dst,
            idempotency_key: format!("{idempotency_key}-{src:020}-{dst:020}"),
        });
        self.delete_edge_mutations_batch(cell_id, mutations).await
    }

    pub async fn delete_edges_batch_chunked(
        &self,
        cell_id: &str,
        edge_type: &str,
        edges: impl IntoIterator<Item = (VertexId, VertexId)>,
        idempotency_key: &str,
        chunk_size: usize,
    ) -> Result<EdgeDeleteBatchResult> {
        validate_component("cell_id", cell_id)?;
        validate_component("edge_type", edge_type)?;
        validate_component("idempotency_key", idempotency_key)?;
        if chunk_size == 0 {
            return Err(GraphError::CorruptValue {
                key: "delete_edges_batch_chunk_size".to_string(),
                reason: "chunk size must be greater than zero".to_string(),
            });
        }
        ensure_limit(
            "delete_edges_batch_chunk_size",
            chunk_size as u64,
            self.limits.max_bulk_import_edges as u64,
        )?;

        let mut start_epoch: Option<GraphEpoch> = None;
        let mut end_epoch = self.current_epoch(cell_id).await?;
        let mut deleted = 0_u64;
        let mut already_deleted = 0_u64;
        let mut results = Vec::new();
        let mut chunk_id = 0_usize;
        let mut chunk = Vec::with_capacity(chunk_size);
        for edge in edges {
            chunk.push(edge);
            if chunk.len() < chunk_size {
                continue;
            }
            let result = self
                .delete_edges_batch(
                    cell_id,
                    edge_type,
                    chunk.drain(..),
                    &format!("{idempotency_key}-chunk-{chunk_id:020}"),
                )
                .await?;
            if result.deleted > 0 {
                start_epoch = Some(
                    start_epoch.map_or(result.start_epoch, |epoch| epoch.min(result.start_epoch)),
                );
            }
            end_epoch = end_epoch.max(result.end_epoch);
            deleted = deleted.saturating_add(result.deleted);
            already_deleted = already_deleted.saturating_add(result.already_deleted);
            results.extend(result.results);
            chunk_id += 1;
        }
        if !chunk.is_empty() {
            let result = self
                .delete_edges_batch(
                    cell_id,
                    edge_type,
                    chunk.drain(..),
                    &format!("{idempotency_key}-chunk-{chunk_id:020}"),
                )
                .await?;
            if result.deleted > 0 {
                start_epoch = Some(
                    start_epoch.map_or(result.start_epoch, |epoch| epoch.min(result.start_epoch)),
                );
            }
            end_epoch = end_epoch.max(result.end_epoch);
            deleted = deleted.saturating_add(result.deleted);
            already_deleted = already_deleted.saturating_add(result.already_deleted);
            results.extend(result.results);
        }

        Ok(EdgeDeleteBatchResult {
            start_epoch: start_epoch.unwrap_or(end_epoch),
            end_epoch,
            deleted,
            already_deleted,
            results,
        })
    }

    pub async fn delete_edge_mutations_batch(
        &self,
        cell_id: &str,
        mutations: impl IntoIterator<Item = EdgeMutation>,
    ) -> Result<EdgeDeleteBatchResult> {
        validate_component("cell_id", cell_id)?;
        self.ensure_write_authority(cell_id, "delete_edge_mutations_batch")?;

        let mutations: Vec<_> = mutations.into_iter().collect();
        if mutations.is_empty() {
            let epoch = self.current_epoch(cell_id).await?;
            return Ok(EdgeDeleteBatchResult {
                start_epoch: epoch,
                end_epoch: epoch,
                deleted: 0,
                already_deleted: 0,
                results: Vec::new(),
            });
        }
        ensure_limit(
            "delete_edge_mutations_batch",
            mutations.len() as u64,
            self.limits.max_bulk_import_edges as u64,
        )?;
        validate_edge_mutations_for_cell(cell_id, &mutations, "delete_edge_mutations_batch")?;
        validate_unique_delete_mutation_identities(&mutations)?;

        let _permit = self
            .acquire_graph_write_permit("delete_edge_mutations_batch")
            .await?;
        let _writer = self.writer_lane(cell_id).lock().await;
        for attempt in 0..GRAPH_TXN_MAX_RETRIES {
            match self
                .delete_edge_mutations_batch_txn(cell_id, &mutations)
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
        Err(GraphError::RetryExhausted {
            operation: "graph transaction",
            attempts: GRAPH_TXN_MAX_RETRIES,
        })
    }

    #[cfg(feature = "opencypher")]
    pub(crate) async fn reserve_edge_delete_noops_batch(
        &self,
        cell_id: &str,
        mutations: impl IntoIterator<Item = EdgeMutation>,
    ) -> Result<()> {
        validate_component("cell_id", cell_id)?;
        self.ensure_write_authority(cell_id, "reserve_edge_delete_noops_batch")?;

        let mutations: Vec<_> = mutations.into_iter().collect();
        if mutations.is_empty() {
            return Ok(());
        }
        ensure_limit(
            "reserve_edge_delete_noops_batch",
            mutations.len() as u64,
            self.limits.max_bulk_import_edges as u64,
        )?;
        validate_edge_mutations_for_cell(cell_id, &mutations, "reserve_edge_delete_noops_batch")?;
        validate_unique_delete_mutation_identities(&mutations)?;

        let _permit = self
            .acquire_graph_write_permit("reserve_edge_delete_noops_batch")
            .await?;
        let _writer = self.writer_lane(cell_id).lock().await;
        for attempt in 0..GRAPH_TXN_MAX_RETRIES {
            match self
                .reserve_edge_delete_noops_batch_txn(cell_id, &mutations)
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
        Err(GraphError::RetryExhausted {
            operation: "graph transaction",
            attempts: GRAPH_TXN_MAX_RETRIES,
        })
    }

    #[cfg(feature = "opencypher")]
    async fn reserve_edge_delete_noops_batch_txn(
        &self,
        cell_id: &str,
        mutations: &[EdgeMutation],
    ) -> Result<()> {
        let lock = self
            .acquire_cell_write_lock(cell_id, "reserve_edge_delete_noops_batch")
            .await?;
        let result = self
            .reserve_edge_delete_noops_batch_txn_locked(cell_id, mutations)
            .await;
        release_cell_write_lock(lock, result).await
    }

    #[cfg(feature = "opencypher")]
    async fn reserve_edge_delete_noops_batch_txn_locked(
        &self,
        cell_id: &str,
        mutations: &[EdgeMutation],
    ) -> Result<()> {
        let txn = self
            .db
            .writer()?
            .begin(IsolationLevel::SerializableSnapshot)
            .await?;
        self.validate_write_fence_txn(&txn, cell_id, "reserve_edge_delete_noops_batch")
            .await?;
        let current_epoch = read_counter_txn(&txn, &keys::last_epoch(cell_id)).await?;

        for mutation in mutations {
            let idem_key = keys::idempotency(cell_id, "delete", &mutation.idempotency_key);
            if let Some(value) = read_txn_remote(&txn, &idem_key).await? {
                decode_delete_idempotency(&idem_key, mutation, &value)?;
                continue;
            }
            txn.put(
                idem_key.as_bytes(),
                encode_delete_idempotency(
                    mutation,
                    &DeleteResult {
                        epoch: current_epoch,
                        deleted: false,
                    },
                ),
            )?;
        }

        commit_txn_strict(txn, self.await_durable_writes).await
    }

    async fn delete_edge_mutations_batch_txn(
        &self,
        cell_id: &str,
        mutations: &[EdgeMutation],
    ) -> Result<EdgeDeleteBatchResult> {
        let lock = self
            .acquire_cell_write_lock(cell_id, "delete_edge_mutations_batch")
            .await?;
        let result = self
            .delete_edge_mutations_batch_txn_locked(cell_id, mutations)
            .await;
        release_cell_write_lock(lock, result).await
    }

    async fn delete_edge_mutations_batch_txn_locked(
        &self,
        cell_id: &str,
        mutations: &[EdgeMutation],
    ) -> Result<EdgeDeleteBatchResult> {
        let txn = self
            .db
            .writer()?
            .begin(IsolationLevel::SerializableSnapshot)
            .await?;
        self.validate_write_fence_txn(&txn, cell_id, "delete_edge_mutations_batch")
            .await?;

        let current_epoch = read_counter_txn(&txn, &keys::last_epoch(cell_id)).await?;
        let mut next_epoch = current_epoch;
        let mut results = Vec::with_capacity(mutations.len());
        let mut deleted = 0_u64;
        let mut already_deleted = 0_u64;
        let mut out_decrements = BTreeMap::<(String, VertexId), u64>::new();
        let mut in_decrements = BTreeMap::<(String, VertexId), u64>::new();
        let mut segment_edges_by_type_src =
            BTreeMap::<(String, VertexId), BTreeMap<VertexId, GraphEpoch>>::new();
        let mut outbox_runs = Vec::<DeleteOutboxRun>::new();
        let mut current_run = None::<DeleteOutboxRun>;
        let write_reverse_index = self.writes_reverse_index();

        for mutation in mutations {
            let idem_key = keys::idempotency(cell_id, "delete", &mutation.idempotency_key);
            if let Some(value) = read_txn_remote(&txn, &idem_key).await? {
                let result = decode_delete_idempotency(&idem_key, mutation, &value)?;
                if result.deleted {
                    deleted = deleted.saturating_add(1);
                } else {
                    already_deleted = already_deleted.saturating_add(1);
                }
                results.push(result);
                continue;
            }

            let edge_key = keys::out_edge(cell_id, &mutation.edge_type, mutation.src, mutation.dst);
            let canonical = match read_txn_remote(&txn, &edge_key).await? {
                Some(value) => Some(decode_edge_record(&edge_key, &value)?),
                None => None,
            };
            let segment_epoch = if canonical.is_none() && !write_reverse_index {
                let cache_key = (mutation.edge_type.clone(), mutation.src);
                match segment_edges_by_type_src.get(&cache_key) {
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
                        segment_edges_by_type_src.insert(cache_key, edges);
                        epoch
                    }
                }
            } else {
                None
            };

            if canonical.is_none() && segment_epoch.is_none() {
                let result = DeleteResult {
                    epoch: current_epoch,
                    deleted: false,
                };
                txn.put(
                    idem_key.as_bytes(),
                    encode_delete_idempotency(mutation, &result),
                )?;
                already_deleted = already_deleted.saturating_add(1);
                results.push(result);
                continue;
            }

            next_epoch = next_epoch
                .checked_add(1)
                .ok_or_else(|| GraphError::CorruptValue {
                    key: keys::last_epoch(cell_id),
                    reason: "epoch overflow during edge delete batch".to_string(),
                })?;
            let result = DeleteResult {
                epoch: next_epoch,
                deleted: true,
            };
            let edge_metadata_key =
                keys::edge_metadata(cell_id, &mutation.edge_type, mutation.src, mutation.dst);
            let previous_edge_metadata = match read_txn_remote(&txn, &edge_metadata_key).await? {
                Some(value) => decode_edge_metadata(&edge_metadata_key, &value)?,
                None => EdgeMetadata::default(),
            };
            tombstone_relationships_for_structural_edge_delete_txn(
                &txn,
                mutation,
                next_epoch,
                current_epoch,
            )
            .await?;
            if !previous_edge_metadata.properties.is_empty() {
                apply_edge_metadata_update_txn(
                    &txn,
                    EdgeMetadataTarget {
                        cell_id,
                        edge_type: &mutation.edge_type,
                        src: mutation.src,
                        dst: mutation.dst,
                    },
                    &previous_edge_metadata,
                    &EdgeMetadata::default(),
                    next_epoch,
                )?;
            }

            if canonical.is_some() {
                txn.delete(
                    keys::edge(cell_id, &mutation.edge_type, mutation.src, mutation.dst).as_bytes(),
                )?;
                txn.delete(edge_key.as_bytes())?;
                if write_reverse_index {
                    txn.delete(
                        keys::in_edge(cell_id, &mutation.edge_type, mutation.dst, mutation.src)
                            .as_bytes(),
                    )?;
                }
                if write_reverse_index {
                    *in_decrements
                        .entry((mutation.edge_type.clone(), mutation.dst))
                        .or_insert(0) += 1;
                }
            } else {
                txn.put(
                    keys::out_segment_tombstone(
                        cell_id,
                        &mutation.edge_type,
                        mutation.src,
                        mutation.dst,
                    )
                    .as_bytes(),
                    encode_u64(next_epoch),
                )?;
            }
            *out_decrements
                .entry((mutation.edge_type.clone(), mutation.src))
                .or_insert(0) += 1;
            push_delete_outbox_run(
                &mut outbox_runs,
                &mut current_run,
                &mutation.edge_type,
                next_epoch,
                mutation.src,
                mutation.dst,
            )?;
            txn.put(
                idem_key.as_bytes(),
                encode_delete_idempotency(mutation, &result),
            )?;
            deleted = deleted.saturating_add(1);
            results.push(result);
        }
        if let Some(run) = current_run.take() {
            outbox_runs.push(run);
        }

        for ((edge_type, src), decrement) in out_decrements {
            let key = keys::degree_out(cell_id, &edge_type, src);
            let base = read_counter_txn(&txn, &key).await?;
            txn.put(key.as_bytes(), encode_u64(base.saturating_sub(decrement)))?;
        }
        if write_reverse_index {
            for ((edge_type, dst), decrement) in in_decrements {
                let key = keys::degree_in(cell_id, &edge_type, dst);
                let base = read_counter_txn(&txn, &key).await?;
                txn.put(key.as_bytes(), encode_u64(base.saturating_sub(decrement)))?;
            }
        }
        for (run_id, run) in outbox_runs.iter().enumerate() {
            for (offset, (src, dst)) in run.edges.iter().copied().enumerate() {
                let epoch = run.start_epoch + offset as u64;
                let delta = DeltaRecord {
                    kind: DeltaKind::Minus,
                    edge: EdgeRecord {
                        cell_id: cell_id.to_string(),
                        edge_type: run.edge_type.clone(),
                        src,
                        dst,
                        epoch,
                    },
                };
                txn.put(
                    keys::outbox(cell_id, epoch, DeltaKind::Minus, &run.edge_type, src, dst)
                        .as_bytes(),
                    encode_delta_record(&delta),
                )?;
                put_scoped_delta_indexes_txn(&txn, &delta)?;
            }
            txn.put(
                keys::outbox_batch(
                    cell_id,
                    run.end_epoch,
                    run.start_epoch,
                    DeltaKind::Minus,
                    &run.edge_type,
                    &format!("delete-batch-{run_id:020}"),
                )
                .as_bytes(),
                encode_outbox_delta_batch(
                    cell_id,
                    &run.edge_type,
                    DeltaKind::Minus,
                    run.start_epoch,
                    run.end_epoch,
                    &run.edges,
                ),
            )?;
        }
        if next_epoch > current_epoch {
            txn.put(keys::last_epoch(cell_id).as_bytes(), encode_u64(next_epoch))?;
        }

        let deleted_start_epoch = results
            .iter()
            .filter(|result| result.deleted)
            .map(|result| result.epoch)
            .min();
        let deleted_end_epoch = results
            .iter()
            .filter(|result| result.deleted)
            .map(|result| result.epoch)
            .max();
        commit_txn_strict(txn, self.await_durable_writes).await?;
        Ok(EdgeDeleteBatchResult {
            start_epoch: deleted_start_epoch.unwrap_or(current_epoch),
            end_epoch: deleted_end_epoch.unwrap_or(next_epoch),
            deleted,
            already_deleted,
            results,
        })
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
        Err(GraphError::RetryExhausted {
            operation: "graph transaction",
            attempts: GRAPH_TXN_MAX_RETRIES,
        })
    }

    pub(crate) async fn delete_edge_txn(&self, mutation: &EdgeMutation) -> Result<DeleteResult> {
        let lock = self
            .acquire_cell_write_lock(&mutation.cell_id, "delete_edge")
            .await?;
        let result = self.delete_edge_txn_locked(mutation).await;
        release_cell_write_lock(lock, result).await
    }

    async fn delete_edge_txn_locked(&self, mutation: &EdgeMutation) -> Result<DeleteResult> {
        let txn = self
            .db
            .writer()?
            .begin(IsolationLevel::SerializableSnapshot)
            .await?;
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
            let delta = DeltaRecord {
                kind: DeltaKind::Minus,
                edge: record,
            };
            let delta_value = encode_delta_record(&delta);
            put_scoped_delta_indexes_txn(&txn, &delta)?;
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

            tombstone_relationships_for_structural_edge_delete_txn(
                &txn,
                mutation,
                epoch,
                current_epoch,
            )
            .await?;
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
        let delta = DeltaRecord {
            kind: DeltaKind::Minus,
            edge: record.clone(),
        };
        let delta_value = encode_delta_record(&delta);
        put_scoped_delta_indexes_txn(&txn, &delta)?;

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

        tombstone_relationships_for_structural_edge_delete_txn(
            &txn,
            mutation,
            epoch,
            epoch.saturating_sub(1),
        )
        .await?;
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
        Err(GraphError::RetryExhausted {
            operation: "graph transaction",
            attempts: GRAPH_TXN_MAX_RETRIES,
        })
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
        Err(GraphError::RetryExhausted {
            operation: "graph transaction",
            attempts: GRAPH_TXN_MAX_RETRIES,
        })
    }

    pub async fn write_edge_mutations_batch(
        &self,
        cell_id: &str,
        mutations: impl IntoIterator<Item = EdgeMutation>,
    ) -> Result<EdgeMutationBatchResult> {
        self.write_edge_mutations_batch_with_endpoint_labels(
            cell_id,
            mutations.into_iter().collect(),
            None,
            "write_edge_mutations_batch",
        )
        .await
    }

    #[cfg(feature = "opencypher")]
    pub(crate) async fn write_edge_mutations_batch_between_labeled_vertices(
        &self,
        cell_id: &str,
        mutations: impl IntoIterator<Item = EdgeMutation>,
        source_label: &str,
        destination_label: &str,
    ) -> Result<EdgeMutationBatchResult> {
        validate_component("source_label", source_label)?;
        validate_component("destination_label", destination_label)?;
        self.write_edge_mutations_batch_with_endpoint_labels(
            cell_id,
            mutations.into_iter().collect(),
            Some((source_label, destination_label)),
            "write_edge_mutations_batch_between_labeled_vertices",
        )
        .await
    }

    async fn write_edge_mutations_batch_with_endpoint_labels(
        &self,
        cell_id: &str,
        mutations: Vec<EdgeMutation>,
        endpoint_labels: Option<(&str, &str)>,
        operation: &'static str,
    ) -> Result<EdgeMutationBatchResult> {
        validate_component("cell_id", cell_id)?;
        self.ensure_write_authority(cell_id, operation)?;

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
            operation,
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

        let _permit = self.acquire_graph_write_permit(operation).await?;
        let _writer = self.writer_lane(cell_id).lock().await;
        for attempt in 0..GRAPH_TXN_MAX_RETRIES {
            match self
                .write_edge_mutations_batch_txn(
                    cell_id,
                    &mutations,
                    operation,
                    None,
                    endpoint_labels,
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
        Err(GraphError::RetryExhausted {
            operation: "graph transaction",
            attempts: GRAPH_TXN_MAX_RETRIES,
        })
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
        Err(GraphError::RetryExhausted {
            operation: "graph transaction",
            attempts: GRAPH_TXN_MAX_RETRIES,
        })
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
        let txn = self
            .db
            .writer()?
            .begin(IsolationLevel::SerializableSnapshot)
            .await?;
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
        Err(GraphError::RetryExhausted {
            operation: "graph transaction",
            attempts: GRAPH_TXN_MAX_RETRIES,
        })
    }

    pub(crate) async fn write_edge_mutations_batch_txn(
        &self,
        cell_id: &str,
        mutations: &[EdgeMutation],
        operation: &'static str,
        materialized_log_epoch: Option<GraphEpoch>,
        endpoint_labels: Option<(&str, &str)>,
    ) -> Result<EdgeMutationBatchResult> {
        let lock = self.acquire_cell_write_lock(cell_id, operation).await?;
        let result = self
            .write_edge_mutations_batch_txn_locked(
                cell_id,
                mutations,
                operation,
                materialized_log_epoch,
                endpoint_labels,
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
        endpoint_labels: Option<(&str, &str)>,
    ) -> Result<EdgeMutationBatchResult> {
        let txn = self
            .db
            .writer()?
            .begin(IsolationLevel::SerializableSnapshot)
            .await?;
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
        let mut validated_endpoints = BTreeSet::<(VertexId, String)>::new();
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

            if let Some((source_label, destination_label)) = endpoint_labels {
                for (vertex, label) in [
                    (mutation.src, source_label),
                    (mutation.dst, destination_label),
                ] {
                    if !validated_endpoints.insert((vertex, label.to_string())) {
                        continue;
                    }
                    let key = keys::vertex(cell_id, vertex);
                    let Some(value) = read_txn_remote(&txn, &key).await? else {
                        return Err(GraphError::UnsupportedQuery {
                            dialect: "OpenCypher",
                            feature: format!(
                                "MATCH endpoint vertex {vertex} with label {label} does not exist"
                            ),
                        });
                    };
                    let metadata = decode_vertex_metadata(&key, &value)?;
                    if !metadata.labels.contains(label) {
                        return Err(GraphError::UnsupportedQuery {
                            dialect: "OpenCypher",
                            feature: format!(
                                "MATCH endpoint vertex {vertex} does not have label {label}"
                            ),
                        });
                    }
                }
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
            let delta = DeltaRecord {
                kind: DeltaKind::Plus,
                edge: record.clone(),
            };
            let delta_value = encode_delta_record(&delta);
            put_scoped_delta_indexes_txn(&txn, &delta)?;
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
        crate::engine::trim_process_memory_after_hydration();

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
        let txn = self
            .db
            .writer()?
            .begin(IsolationLevel::SerializableSnapshot)
            .await?;
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
            let segment_edges: Vec<_> = inserted_dsts
                .iter()
                .copied()
                .enumerate()
                .map(|(offset, dst)| (start_epoch + offset as u64, dst))
                .collect();
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
                encode_out_edge_segment_records(&segment_edges),
            )?;
            txn.put(keys::last_epoch(cell_id).as_bytes(), encode_u64(end_epoch))?;
            let degree_key = keys::degree_out(cell_id, edge_type, src);
            let base = if current_epoch == 0 {
                0
            } else {
                read_counter_txn(&txn, &degree_key).await?
            };
            txn.put(degree_key.as_bytes(), encode_u64(base + inserted))?;
            for (offset, dst) in inserted_dsts.iter().copied().enumerate() {
                let delta = DeltaRecord {
                    kind: DeltaKind::Plus,
                    edge: EdgeRecord {
                        cell_id: cell_id.to_string(),
                        edge_type: edge_type.to_string(),
                        src,
                        dst,
                        epoch: start_epoch + offset as u64,
                    },
                };
                put_scoped_delta_indexes_txn(&txn, &delta)?;
            }
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
        let txn = self
            .db
            .writer()?
            .begin(IsolationLevel::SerializableSnapshot)
            .await?;
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
            let delta = DeltaRecord {
                kind: DeltaKind::Plus,
                edge: record.clone(),
            };
            let delta_value = encode_delta_record(&delta);
            put_scoped_delta_indexes_txn(&txn, &delta)?;
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

fn push_delete_outbox_run(
    runs: &mut Vec<DeleteOutboxRun>,
    current: &mut Option<DeleteOutboxRun>,
    edge_type: &str,
    epoch: GraphEpoch,
    src: VertexId,
    dst: VertexId,
) -> Result<()> {
    let can_extend = current
        .as_ref()
        .is_some_and(|run| run.edge_type == edge_type && run.end_epoch.saturating_add(1) == epoch);
    if !can_extend {
        if let Some(run) = current.take() {
            runs.push(run);
        }
        *current = Some(DeleteOutboxRun {
            edge_type: edge_type.to_string(),
            start_epoch: epoch,
            end_epoch: epoch,
            edges: Vec::new(),
        });
    }
    let Some(run) = current.as_mut() else {
        return Err(GraphError::CorruptValue {
            key: format!("delete/outbox/{edge_type}/{epoch}"),
            reason: "delete outbox run was not initialized".to_string(),
        });
    };
    run.end_epoch = epoch;
    run.edges.push((src, dst));
    Ok(())
}

fn validate_unique_delete_mutation_identities(mutations: &[EdgeMutation]) -> Result<()> {
    let mut identities = BTreeMap::<(&str, VertexId, VertexId), &str>::new();
    for mutation in mutations {
        let identity = (mutation.edge_type.as_str(), mutation.src, mutation.dst);
        if let Some(first_key) = identities.insert(identity, mutation.idempotency_key.as_str()) {
            return Err(GraphError::IdempotencyConflict {
                operation: "delete",
                idempotency_key: format!("{first_key},{}", mutation.idempotency_key),
            });
        }
    }
    Ok(())
}

async fn renew_vertex_delete_lock_after_items(lock: &CellWriteLock, items: u64) -> Result<()> {
    if items == 0
        || (items >= VERTEX_DELETE_LOCK_RENEW_ITEMS
            && items / VERTEX_DELETE_LOCK_RENEW_ITEMS * VERTEX_DELETE_LOCK_RENEW_ITEMS == items)
    {
        lock.renew().await?;
    }
    Ok(())
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

fn coalesce_edge_metadata_updates(
    updates: impl IntoIterator<Item = (VertexId, VertexId, EdgeMetadata)>,
) -> Result<Vec<(VertexId, VertexId, EdgeMetadata)>> {
    let mut by_edge = BTreeMap::<(VertexId, VertexId), EdgeMetadata>::new();
    for (src, dst, metadata) in updates {
        validate_edge_metadata(&metadata)?;
        match by_edge.get(&(src, dst)) {
            Some(existing) if existing != &metadata => {
                return Err(GraphError::UnsupportedQuery {
                    dialect: "GraphQuery",
                    feature: format!(
                        "conflicting metadata values for edge {src}->{dst} in one batch"
                    ),
                });
            }
            Some(_) => {}
            None => {
                by_edge.insert((src, dst), metadata);
            }
        }
    }
    Ok(by_edge
        .into_iter()
        .map(|((src, dst), metadata)| (src, dst, metadata))
        .collect())
}

fn coalesce_relationship_imports(
    cell_id: &str,
    edge_type: &str,
    relationships: impl IntoIterator<Item = RelationshipMutation>,
) -> Result<Vec<RelationshipMutation>> {
    let mut by_id = BTreeMap::<RelationshipId, RelationshipMutation>::new();
    for relationship in relationships {
        validate_component("cell_id", &relationship.cell_id)?;
        validate_component("edge_type", &relationship.edge_type)?;
        validate_edge_metadata(&relationship.metadata)?;
        if relationship.cell_id != cell_id {
            return Err(GraphError::CorruptValue {
                key: format!("cell/{cell_id}/relationship_import"),
                reason: format!(
                    "batch contains relationship for different cell {}",
                    relationship.cell_id
                ),
            });
        }
        if relationship.edge_type != edge_type {
            return Err(GraphError::CorruptValue {
                key: format!("cell/{cell_id}/relationship_import/{edge_type}"),
                reason: format!(
                    "batch contains relationship for different edge type {}",
                    relationship.edge_type
                ),
            });
        }
        match by_id.get(&relationship.relationship_id) {
            Some(existing) if existing != &relationship => {
                return Err(GraphError::IdempotencyConflict {
                    operation: "relationship-import",
                    idempotency_key: format!("{:020}", relationship.relationship_id),
                });
            }
            Some(_) => {}
            None => {
                by_id.insert(relationship.relationship_id, relationship);
            }
        }
    }
    Ok(by_id.into_values().collect())
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

fn put_relationship_property_indexes_txn(
    txn: &DbTransaction,
    record: &RelationshipRecord,
) -> Result<()> {
    for (property, value) in &record.metadata.properties {
        txn.put(
            keys::relationship_property_index(
                &record.cell_id,
                &record.edge_type,
                property,
                &encode_vertex_property_value_key(value),
                record.src,
                record.dst,
                record.relationship_id,
            )
            .as_bytes(),
            encode_u64(record.relationship_id).as_slice(),
        )?;
    }
    Ok(())
}

fn put_relationship_metadata_delta_txn(
    txn: &DbTransaction,
    record: &RelationshipRecord,
    metadata: &EdgeMetadata,
    epoch: GraphEpoch,
) -> Result<()> {
    validate_edge_metadata(metadata)?;
    txn.put(
        keys::relationship_metadata_delta(
            &record.cell_id,
            &record.edge_type,
            record.src,
            record.dst,
            record.relationship_id,
            epoch,
        )
        .as_bytes(),
        encode_edge_metadata(metadata).as_slice(),
    )?;
    Ok(())
}

async fn relationship_tombstone_epoch_txn(
    txn: &DbTransaction,
    record: &RelationshipRecord,
) -> Result<Option<GraphEpoch>> {
    let key = keys::relationship_tombstone(
        &record.cell_id,
        &record.edge_type,
        record.src,
        record.dst,
        record.relationship_id,
    );
    match read_txn_remote(txn, &key).await? {
        Some(value) => Ok(Some(decode_u64(&key, &value)?)),
        None => Ok(None),
    }
}

async fn relationship_deleted_at_txn(
    txn: &DbTransaction,
    record: &RelationshipRecord,
    read_epoch: GraphEpoch,
) -> Result<bool> {
    Ok(match relationship_tombstone_epoch_txn(txn, record).await? {
        Some(tombstone_epoch) => record.epoch <= tombstone_epoch && tombstone_epoch <= read_epoch,
        None => false,
    })
}

async fn live_relationships_for_edge_txn(
    txn: &DbTransaction,
    cell_id: &str,
    edge_type: &str,
    src: VertexId,
    dst: VertexId,
    read_epoch: GraphEpoch,
) -> Result<Vec<RelationshipRecord>> {
    let prefix = keys::relationship_edge_prefix(cell_id, edge_type, src, dst);
    let mut iter = txn.scan_prefix(prefix.as_bytes(), ..).await?;
    let mut records = Vec::new();
    while let Some(kv) = iter.next().await? {
        let key = String::from_utf8_lossy(&kv.key).into_owned();
        let record = decode_relationship_record(&key, &kv.value)?;
        if record.epoch > read_epoch {
            continue;
        }
        if relationship_deleted_at_txn(txn, &record, read_epoch).await? {
            continue;
        }
        records.push(record);
    }
    Ok(records)
}

async fn delete_structural_edge_txn(
    shard: &GraphShard,
    txn: &DbTransaction,
    mutation: &EdgeMutation,
    epoch: GraphEpoch,
) -> Result<()> {
    let edge_key = keys::out_edge(
        &mutation.cell_id,
        &mutation.edge_type,
        mutation.src,
        mutation.dst,
    );
    if read_txn_remote(txn, &edge_key).await?.is_none() {
        return Ok(());
    }
    let record = EdgeRecord {
        cell_id: mutation.cell_id.clone(),
        edge_type: mutation.edge_type.clone(),
        src: mutation.src,
        dst: mutation.dst,
        epoch,
    };
    let delta = DeltaRecord {
        kind: DeltaKind::Minus,
        edge: record,
    };
    let delta_value = encode_delta_record(&delta);
    put_scoped_delta_indexes_txn(txn, &delta)?;
    let out_degree_key = keys::degree_out(&mutation.cell_id, &mutation.edge_type, mutation.src);
    let out_degree = read_counter_txn(txn, &out_degree_key)
        .await?
        .saturating_sub(1);
    let in_degree = if shard.writes_reverse_index() {
        let in_degree_key = keys::degree_in(&mutation.cell_id, &mutation.edge_type, mutation.dst);
        let in_degree = read_counter_txn(txn, &in_degree_key)
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
    let previous_edge_metadata = match read_txn_remote(txn, &edge_metadata_key).await? {
        Some(value) => decode_edge_metadata(&edge_metadata_key, &value)?,
        None => EdgeMetadata::default(),
    };
    if !previous_edge_metadata.properties.is_empty() {
        apply_edge_metadata_update_txn(
            txn,
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
    txn.delete(
        keys::edge(
            &mutation.cell_id,
            &mutation.edge_type,
            mutation.src,
            mutation.dst,
        )
        .as_bytes(),
    )?;
    txn.delete(edge_key.as_bytes())?;
    if shard.writes_reverse_index() {
        txn.delete(
            keys::in_edge(
                &mutation.cell_id,
                &mutation.edge_type,
                mutation.dst,
                mutation.src,
            )
            .as_bytes(),
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
            DeltaKind::Minus,
            &mutation.edge_type,
            mutation.src,
            mutation.dst,
        )
        .as_bytes(),
        &delta_value,
    )?;
    Ok(())
}

fn put_scoped_delta_indexes_txn(txn: &DbTransaction, delta: &DeltaRecord) -> Result<()> {
    let value = encode_delta_record(delta);
    let edge = &delta.edge;
    txn.put(
        keys::owner_delta(
            &edge.cell_id,
            delta.kind,
            &edge.edge_type,
            "out",
            edge.src,
            edge.epoch,
            edge.dst,
        )
        .as_bytes(),
        &value,
    )?;
    txn.put(
        keys::owner_delta(
            &edge.cell_id,
            delta.kind,
            &edge.edge_type,
            "in",
            edge.dst,
            edge.epoch,
            edge.src,
        )
        .as_bytes(),
        &value,
    )?;
    txn.put(
        keys::pair_delta(
            &edge.cell_id,
            delta.kind,
            &edge.edge_type,
            edge.src,
            edge.dst,
            edge.epoch,
        )
        .as_bytes(),
        value,
    )?;
    Ok(())
}

async fn tombstone_relationships_for_structural_edge_delete_txn(
    txn: &DbTransaction,
    mutation: &EdgeMutation,
    delete_epoch: GraphEpoch,
    read_epoch: GraphEpoch,
) -> Result<u64> {
    let relationships = live_relationships_for_edge_txn(
        txn,
        &mutation.cell_id,
        &mutation.edge_type,
        mutation.src,
        mutation.dst,
        read_epoch,
    )
    .await?;
    for record in &relationships {
        txn.put(
            keys::relationship_tombstone(
                &record.cell_id,
                &record.edge_type,
                record.src,
                record.dst,
                record.relationship_id,
            )
            .as_bytes(),
            encode_u64(delete_epoch),
        )?;
        txn.delete(keys::relationship_id(&record.cell_id, record.relationship_id).as_bytes())?;
        delete_relationship_property_indexes_txn(txn, record, &record.metadata)?;
        put_relationship_property_index_deltas_txn(
            txn,
            record,
            &record.metadata,
            &EdgeMetadata::default(),
            delete_epoch,
        )?;
    }
    txn.delete(
        keys::relationship_count(
            &mutation.cell_id,
            &mutation.edge_type,
            mutation.src,
            mutation.dst,
        )
        .as_bytes(),
    )?;
    u64::try_from(relationships.len()).map_err(|err| GraphError::CorruptValue {
        key: keys::relationship_edge_prefix(
            &mutation.cell_id,
            &mutation.edge_type,
            mutation.src,
            mutation.dst,
        ),
        reason: format!("too many relationships to tombstone during edge delete: {err}"),
    })
}

fn delete_relationship_property_indexes_txn(
    txn: &DbTransaction,
    record: &RelationshipRecord,
    metadata: &EdgeMetadata,
) -> Result<()> {
    for (property, value) in &metadata.properties {
        txn.delete(
            keys::relationship_property_index(
                &record.cell_id,
                &record.edge_type,
                property,
                &encode_vertex_property_value_key(value),
                record.src,
                record.dst,
                record.relationship_id,
            )
            .as_bytes(),
        )?;
    }
    Ok(())
}

fn put_relationship_property_index_deltas_txn(
    txn: &DbTransaction,
    record: &RelationshipRecord,
    previous: &EdgeMetadata,
    next: &EdgeMetadata,
    epoch: GraphEpoch,
) -> Result<()> {
    for (property, value) in &previous.properties {
        if next.properties.get(property) != Some(value) {
            txn.put(
                keys::relationship_property_index_delta(keys::RelationshipPropertyIndexDeltaKey {
                    cell_id: &record.cell_id,
                    edge_type: &record.edge_type,
                    property,
                    encoded_value: &encode_vertex_property_value_key(value),
                    epoch,
                    src: record.src,
                    dst: record.dst,
                    relationship_id: record.relationship_id,
                })
                .as_bytes(),
                encode_vertex_index_delta(false).as_slice(),
            )?;
        }
    }
    for (property, value) in &next.properties {
        if previous.properties.get(property) != Some(value) {
            txn.put(
                keys::relationship_property_index_delta(keys::RelationshipPropertyIndexDeltaKey {
                    cell_id: &record.cell_id,
                    edge_type: &record.edge_type,
                    property,
                    encoded_value: &encode_vertex_property_value_key(value),
                    epoch,
                    src: record.src,
                    dst: record.dst,
                    relationship_id: record.relationship_id,
                })
                .as_bytes(),
                encode_vertex_index_delta(true).as_slice(),
            )?;
        }
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
