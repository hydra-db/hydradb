use super::*;

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct IncidentEdge {
    edge_type: String,
    src: VertexId,
    dst: VertexId,
}

const VERTEX_DELETE_LOCK_RENEW_ITEMS: u64 = 64;

#[derive(Clone, Copy)]
struct RelationshipImportOptions<'a> {
    endpoint_labels: Option<(&'a str, &'a str)>,
    create_always: bool,
    update_existing_metadata: bool,
    operation: &'static str,
}

#[cfg(feature = "opencypher")]
struct RelationshipPropertyTxnLookup<'a> {
    cell_id: &'a str,
    edge_type: &'a str,
    src: VertexId,
    dst: VertexId,
    property: &'a str,
    value: &'a VertexPropertyValue,
}

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

    pub async fn merge_vertex_metadata_batch(
        &self,
        cell_id: &str,
        updates: impl IntoIterator<Item = (VertexId, VertexMetadata)>,
    ) -> Result<usize> {
        validate_component("cell_id", cell_id)?;
        self.ensure_write_authority(cell_id, "merge_vertex_metadata_batch")?;
        let updates = coalesce_vertex_metadata_updates(updates)?;
        if updates.is_empty() {
            return Ok(0);
        }
        ensure_limit(
            "merge_vertex_metadata_batch",
            updates.len() as u64,
            self.limits.max_bulk_import_edges as u64,
        )?;
        let _permit = self
            .acquire_graph_write_permit("merge_vertex_metadata_batch")
            .await?;
        let _writer = self.writer_lane(cell_id).lock().await;
        for attempt in 0..GRAPH_TXN_MAX_RETRIES {
            match self
                .merge_vertex_metadata_batch_txn(cell_id, updates.clone())
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
            .acquire_local_write_guard(cell_id, "set_vertex_metadata")
            .await?;
        let result = self
            .set_vertex_metadata_txn_locked(cell_id, vertex_id, metadata)
            .await;
        finish_local_write(lock, result).await
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
        apply_vertex_metadata_update_txn(&txn, cell_id, vertex_id, &previous, &metadata, epoch)?;
        commit_txn_strict(txn, self.await_durable_writes).await
    }

    async fn set_vertex_metadata_batch_txn(
        &self,
        cell_id: &str,
        updates: Vec<(VertexId, VertexMetadata)>,
    ) -> Result<usize> {
        let lock = self
            .acquire_local_write_guard(cell_id, "set_vertex_metadata_batch")
            .await?;
        let result = self
            .set_vertex_metadata_batch_txn_locked(cell_id, updates)
            .await;
        finish_local_write(lock, result).await
    }

    async fn merge_vertex_metadata_batch_txn(
        &self,
        cell_id: &str,
        updates: Vec<(VertexId, VertexMetadata)>,
    ) -> Result<usize> {
        let lock = self
            .acquire_local_write_guard(cell_id, "merge_vertex_metadata_batch")
            .await?;
        let result = self
            .merge_vertex_metadata_batch_txn_locked(cell_id, updates)
            .await;
        finish_local_write(lock, result).await
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
        for (vertex_id, previous, metadata) in &changed {
            apply_vertex_metadata_update_txn(&txn, cell_id, *vertex_id, previous, metadata, epoch)?;
        }
        let changed_count = changed.len();
        commit_txn_strict(txn, self.await_durable_writes).await?;
        Ok(changed_count)
    }

    async fn merge_vertex_metadata_batch_txn_locked(
        &self,
        cell_id: &str,
        updates: Vec<(VertexId, VertexMetadata)>,
    ) -> Result<usize> {
        let txn = self
            .db
            .writer()?
            .begin(IsolationLevel::SerializableSnapshot)
            .await?;
        self.validate_write_fence_txn(&txn, cell_id, "merge_vertex_metadata_batch")
            .await?;
        let mut changed = Vec::new();
        for (vertex_id, patch) in updates {
            let vertex_key = keys::vertex(cell_id, vertex_id);
            let previous = match read_txn_remote(&txn, &vertex_key).await? {
                Some(value) => decode_vertex_metadata(&vertex_key, &value)?,
                None => VertexMetadata::default(),
            };
            let mut merged = previous.clone();
            merged.labels.extend(patch.labels);
            merged.properties.extend(patch.properties);
            if previous != merged {
                changed.push((vertex_id, previous, merged));
            }
        }
        if changed.is_empty() {
            return Ok(0);
        }
        let epoch = next_epoch_txn(&txn, cell_id).await?;
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
            .acquire_local_write_guard(cell_id, "import_vertex_metadata_batch")
            .await?;
        let result = self
            .import_vertex_metadata_batch_txn_locked(cell_id, updates)
            .await;
        finish_local_write(lock, result).await
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
            let lock = self.acquire_local_write_guard(cell_id, operation).await?;
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
            match finish_local_write(lock, result).await {
                Err(err)
                    if is_retryable_write_conflict(&err) && attempt + 1 < GRAPH_TXN_MAX_RETRIES =>
                {
                    self.operation_metrics
                        .write_retries
                        .fetch_add(1, Ordering::Relaxed);
                    tokio::task::yield_now().await;
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
        lock: &LocalWriteGuard,
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
        let read_epoch = txn.seqnum();
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
                let current_epoch = txn.seqnum();
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

        let current_epoch = txn.seqnum();
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
                    key: "storage_sequence".to_string(),
                    reason: "epoch overflow during vertex delete".to_string(),
                })?;
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
        read_epoch: StorageSequence,
        lock: &LocalWriteGuard,
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
            if segment.storage_sequence > read_epoch {
                continue;
            }
            for dst in segment.destinations {
                if segment.src != vertex_id && dst != vertex_id {
                    continue;
                }
                let tombstone_key =
                    keys::out_segment_tombstone(cell_id, &segment.edge_type, segment.src, dst);
                let tombstone_epoch = match self.read_remote(&tombstone_key).await? {
                    Some(value) => Some(decode_u64(&tombstone_key, &value)?),
                    None => None,
                };
                if segment_edge_visible(segment.storage_sequence, tombstone_epoch) {
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
            let lock = self.acquire_local_write_guard(cell_id, "drop_cell").await?;
            let result = self.drop_cell_locked(cell_id, idempotency_key, &lock).await;
            match finish_local_write(lock, result).await {
                Err(err)
                    if is_retryable_write_conflict(&err) && attempt + 1 < GRAPH_TXN_MAX_RETRIES =>
                {
                    self.operation_metrics
                        .write_retries
                        .fetch_add(1, Ordering::Relaxed);
                    tokio::task::yield_now().await;
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
        lock: &LocalWriteGuard,
    ) -> Result<GraphCellDropResult> {
        let idem_key = keys::cell_drop_idempotency(cell_id, idempotency_key);
        let marker_key = keys::cell_drop_marker(cell_id);
        let pending_marker_key = keys::cell_drop_pending_marker(cell_id);
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
                let epoch = txn.seqnum().saturating_add(1);
                txn.put(pending_marker_key.as_bytes(), encode_u64(epoch))?;
                epoch
            }
        };
        commit_txn_strict(txn, self.await_durable_writes).await?;

        let mut deleted_keys = 0_u64;
        let mut batches = 0_u64;
        let mut pending = Vec::new();
        let mut iter = self.scan_remote_prefix(&keys::cell_prefix(cell_id)).await?;
        while let Some(kv) = iter.next().await? {
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            if key == pending_marker_key {
                continue;
            }
            pending.push(key);
            if pending.len() >= GRAPH_MAINTENANCE_BATCH_KEYS {
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
        self.import_relationships_batch_with_endpoint_labels(
            cell_id,
            edge_type,
            relationships.into_iter().collect(),
            idempotency_key,
            RelationshipImportOptions {
                endpoint_labels: None,
                create_always: false,
                update_existing_metadata: false,
                operation: "import_relationships_batch",
            },
        )
        .await
    }

    #[cfg(feature = "opencypher")]
    pub(crate) async fn create_relationships_batch_between_labeled_vertices(
        &self,
        cell_id: &str,
        edge_type: &str,
        relationships: impl IntoIterator<Item = RelationshipMutation>,
        idempotency_key: &str,
        source_label: &str,
        destination_label: &str,
    ) -> Result<RelationshipImportResult> {
        validate_component("source_label", source_label)?;
        validate_component("destination_label", destination_label)?;
        self.import_relationships_batch_with_endpoint_labels(
            cell_id,
            edge_type,
            relationships.into_iter().collect(),
            idempotency_key,
            RelationshipImportOptions {
                endpoint_labels: Some((source_label, destination_label)),
                create_always: true,
                update_existing_metadata: false,
                operation: "create_relationships_batch_between_labeled_vertices",
            },
        )
        .await
    }

    #[cfg(feature = "opencypher")]
    pub(crate) async fn merge_relationships_batch_between_labeled_vertices(
        &self,
        cell_id: &str,
        edge_type: &str,
        relationships: impl IntoIterator<Item = RelationshipMutation>,
        idempotency_key: &str,
        source_label: &str,
        destination_label: &str,
    ) -> Result<RelationshipImportResult> {
        validate_component("source_label", source_label)?;
        validate_component("destination_label", destination_label)?;
        self.import_relationships_batch_with_endpoint_labels(
            cell_id,
            edge_type,
            relationships.into_iter().collect(),
            idempotency_key,
            RelationshipImportOptions {
                endpoint_labels: Some((source_label, destination_label)),
                create_always: false,
                update_existing_metadata: true,
                operation: "merge_relationships_batch_between_labeled_vertices",
            },
        )
        .await
    }

    async fn import_relationships_batch_with_endpoint_labels(
        &self,
        cell_id: &str,
        edge_type: &str,
        relationships: Vec<RelationshipMutation>,
        idempotency_key: &str,
        options: RelationshipImportOptions<'_>,
    ) -> Result<RelationshipImportResult> {
        validate_component("cell_id", cell_id)?;
        validate_component("edge_type", edge_type)?;
        validate_component("idempotency_key", idempotency_key)?;
        self.ensure_write_authority(cell_id, options.operation)?;

        let mut relationships = if options.create_always {
            validate_relationship_creates(cell_id, edge_type, relationships)?
        } else {
            coalesce_relationship_imports(cell_id, edge_type, relationships)?
        };
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

        let _permit = self.acquire_graph_write_permit(options.operation).await?;
        let _writer = self.writer_lane(cell_id).lock().await;
        for attempt in 0..GRAPH_TXN_MAX_RETRIES {
            match self
                .import_relationships_batch_txn(
                    cell_id,
                    edge_type,
                    &relationships,
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
            .acquire_local_write_guard(cell_id, "set_edge_metadata")
            .await?;
        let result = self
            .set_edge_metadata_txn_locked(cell_id, edge_type, src, dst, metadata)
            .await;
        finish_local_write(lock, result).await
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
        let current_epoch = txn.seqnum();
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
            .acquire_local_write_guard(cell_id, "set_edge_metadata_batch")
            .await?;
        let result = self
            .set_edge_metadata_batch_txn_locked(cell_id, edge_type, updates)
            .await;
        finish_local_write(lock, result).await
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
        let current_epoch = txn.seqnum();
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
        options: RelationshipImportOptions<'_>,
    ) -> Result<RelationshipImportResult> {
        let lock = self
            .acquire_local_write_guard(cell_id, options.operation)
            .await?;
        let result = self
            .import_relationships_batch_txn_locked(
                cell_id,
                edge_type,
                relationships,
                idempotency_key,
                fingerprint,
                options,
            )
            .await;
        finish_local_write(lock, result).await
    }

    async fn import_relationships_batch_txn_locked(
        &self,
        cell_id: &str,
        edge_type: &str,
        relationships: &[RelationshipMutation],
        idempotency_key: &str,
        fingerprint: u64,
        options: RelationshipImportOptions<'_>,
    ) -> Result<RelationshipImportResult> {
        let txn = self
            .db
            .writer()?
            .begin(IsolationLevel::SerializableSnapshot)
            .await?;
        self.validate_write_fence_txn(&txn, cell_id, options.operation)
            .await?;
        if let Some((source_label, destination_label)) = options.endpoint_labels {
            let mut validated = BTreeSet::new();
            for relationship in relationships {
                for (vertex, label) in [
                    (relationship.src, source_label),
                    (relationship.dst, destination_label),
                ] {
                    if !validated.insert((vertex, label)) {
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
        }
        let idem_key = keys::idempotency(cell_id, "relationship-import", idempotency_key);
        if let Some(value) = read_txn_remote(&txn, &idem_key).await? {
            return decode_relationship_import_idempotency(
                &idem_key,
                idempotency_key,
                fingerprint,
                &value,
            );
        }

        let current_epoch = txn.seqnum();
        let current_relationship_id =
            read_counter_txn(&txn, &keys::last_relationship_id(cell_id)).await?;
        let mut relationships = relationships.to_vec();
        let mut next_relationship_id = current_relationship_id;
        if options.create_always {
            for relationship in &mut relationships {
                relationship.relationship_id = next_available_relationship_id_txn(
                    &txn,
                    cell_id,
                    &mut next_relationship_id,
                    "CREATE",
                )
                .await?;
            }
        } else if options.update_existing_metadata {
            #[cfg(not(feature = "opencypher"))]
            return Err(GraphError::UnsupportedQuery {
                dialect: "OpenCypher",
                feature: "relationship MERGE requires the opencypher feature".to_string(),
            });
            #[cfg(feature = "opencypher")]
            {
                let mut resolved = Vec::new();
                for relationship in relationships {
                    let external_id = relationship.relationship_id;
                    let identity = VertexPropertyValue::Integer(external_id);
                    if relationship.metadata.properties.get("id") != Some(&identity) {
                        return Err(GraphError::CorruptValue {
                            key: format!(
                                "cell/{cell_id}/relationship-merge/{edge_type}/{}/{external_id}",
                                relationship.src
                            ),
                            reason:
                                "relationship MERGE identity metadata does not match the parsed id"
                                    .to_string(),
                        });
                    }
                    let existing_ids = relationship_ids_for_edge_property_txn(
                        &txn,
                        RelationshipPropertyTxnLookup {
                            cell_id,
                            edge_type,
                            src: relationship.src,
                            dst: relationship.dst,
                            property: "id",
                            value: &identity,
                        },
                    )
                    .await?;
                    if existing_ids.is_empty() {
                        let mut inserted = relationship;
                        inserted.relationship_id = next_available_relationship_id_txn(
                            &txn,
                            cell_id,
                            &mut next_relationship_id,
                            "MERGE",
                        )
                        .await?;
                        resolved.push(inserted);
                    } else {
                        for relationship_id in existing_ids {
                            let mut matched = relationship.clone();
                            matched.relationship_id = relationship_id;
                            resolved.push(matched);
                        }
                    }
                }
                relationships = resolved;
            }
        }
        let max_requested_relationship_id = relationships
            .iter()
            .map(|relationship| relationship.relationship_id)
            .max()
            .unwrap_or(0);
        let fresh_cell = current_epoch == 0;
        let mut relationships_inserted = Vec::new();
        let mut relationships_updated = Vec::<(RelationshipRecord, EdgeMetadata)>::new();
        let mut relationships_already_existed = 0_u64;
        for relationship in &relationships {
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
                            Some(record)
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
                        Some(record)
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
                    metadata: relationship.metadata.clone(),
                };
                if existing.cell_id != requested.cell_id
                    || existing.edge_type != requested.edge_type
                    || existing.src != requested.src
                    || existing.dst != requested.dst
                    || existing.relationship_id != requested.relationship_id
                {
                    return Err(GraphError::IdempotencyConflict {
                        operation: "relationship-import",
                        idempotency_key: idempotency_key.to_string(),
                    });
                }
                if existing.metadata != requested.metadata {
                    if !options.update_existing_metadata {
                        return Err(GraphError::IdempotencyConflict {
                            operation: "relationship-import",
                            idempotency_key: idempotency_key.to_string(),
                        });
                    }
                    let next_metadata =
                        merge_edge_metadata(&existing.metadata, &requested.metadata);
                    if next_metadata != existing.metadata {
                        let previous_metadata = existing.metadata.clone();
                        let mut updated = existing;
                        updated.metadata = next_metadata;
                        relationships_updated.push((updated, previous_metadata));
                    }
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
        let changed = relationships_inserted_count > 0
            || structural_edges_inserted > 0
            || !relationships_updated.is_empty();
        let epoch = if changed {
            current_epoch
                .checked_add(1)
                .ok_or_else(|| GraphError::CorruptValue {
                    key: "storage_sequence".to_string(),
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
            };
            let edge_value = encode_edge_record(&record);
            mark_adjacency_dirty_txn(&txn, cell_id, edge_type, epoch)?;
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
            put_relationship_property_indexes_txn(&txn, &record)?;
        }
        for (record, previous_metadata) in &relationships_updated {
            let key = keys::relationship(
                cell_id,
                edge_type,
                record.src,
                record.dst,
                record.relationship_id,
            );
            txn.put(
                key.as_bytes(),
                encode_relationship_record(record).as_slice(),
            )?;
            delete_relationship_property_indexes_txn(&txn, record, previous_metadata)?;
            put_relationship_property_indexes_txn(&txn, record)?;
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
            .acquire_local_write_guard(&mutation.cell_id, "create_relationship")
            .await?;
        let result = self
            .create_relationship_txn_locked(mutation, metadata_updates, edge_metadata, fingerprint)
            .await;
        finish_local_write(lock, result).await
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

        let current_epoch = txn.seqnum();
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
                key: "storage_sequence".to_string(),
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
            };
            let edge_value = encode_edge_record(&record);
            mark_adjacency_dirty_txn(&txn, &mutation.cell_id, &mutation.edge_type, epoch)?;
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
        }

        let record = RelationshipRecord {
            cell_id: mutation.cell_id.clone(),
            edge_type: mutation.edge_type.clone(),
            src: mutation.src,
            dst: mutation.dst,
            relationship_id,
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
        put_relationship_property_indexes_txn(&txn, &record)?;

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
            .acquire_local_write_guard(cell_id, "set_relationship_metadata")
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
        finish_local_write(lock, result).await
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
        let key = keys::relationship(cell_id, edge_type, src, dst, relationship_id);
        let Some(value) = read_txn_remote(&txn, &key).await? else {
            return Err(GraphError::UnsupportedQuery {
                dialect: "GraphQuery",
                feature: "cannot set metadata for a missing relationship".to_string(),
            });
        };
        let mut record = decode_relationship_record(&key, &value)?;
        if record.metadata == metadata {
            return Ok(false);
        }
        let previous = record.metadata.clone();
        record.metadata = metadata.clone();
        txn.put(
            key.as_bytes(),
            encode_relationship_record(&record).as_slice(),
        )?;
        delete_relationship_property_indexes_txn(&txn, &record, &previous)?;
        put_relationship_property_indexes_txn(&txn, &record)?;
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
            .acquire_local_write_guard(&mutation.cell_id, "delete_relationship")
            .await?;
        let result = self
            .delete_relationship_txn_locked(mutation, relationship_id)
            .await;
        finish_local_write(lock, result).await
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

        let current_epoch = txn.seqnum();
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
        let epoch = current_epoch
            .checked_add(1)
            .ok_or_else(|| GraphError::CorruptValue {
                key: "storage_sequence".to_string(),
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
        txn.delete(key.as_bytes())?;
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
            .acquire_local_write_guard(&mutation.cell_id, "write_edge")
            .await?;
        let result = self.write_edge_txn_locked(mutation).await;
        finish_local_write(lock, result).await
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
            .acquire_local_write_guard(&mutation.cell_id, "write_edge_with_vertex_metadata")
            .await?;
        let result = self
            .write_edge_txn_locked_with_metadata(
                mutation,
                metadata_updates,
                &EdgeMetadata::default(),
                "write_edge_with_vertex_metadata",
            )
            .await;
        finish_local_write(lock, result).await
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
            .acquire_local_write_guard(&mutation.cell_id, "write_edge_with_full_metadata")
            .await?;
        let result = self
            .write_edge_txn_locked_with_metadata(
                mutation,
                metadata_updates,
                edge_metadata,
                "write_edge_with_full_metadata",
            )
            .await;
        finish_local_write(lock, result).await
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

        let current_epoch = txn.seqnum();
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
                key: "storage_sequence".to_string(),
                reason: "epoch overflow".to_string(),
            })?;
        let result = CommitResult {
            epoch,
            already_existed: existing_edge_epoch.is_some(),
        };
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
        };
        let edge_value = encode_edge_record(&record);
        mark_adjacency_dirty_txn(&txn, &mutation.cell_id, &mutation.edge_type, epoch)?;
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

        let mut start_epoch: Option<StorageSequence> = None;
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
            .acquire_local_write_guard(cell_id, "reserve_edge_delete_noops_batch")
            .await?;
        let result = self
            .reserve_edge_delete_noops_batch_txn_locked(cell_id, mutations)
            .await;
        finish_local_write(lock, result).await
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
        let current_epoch = txn.seqnum();

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
            .acquire_local_write_guard(cell_id, "delete_edge_mutations_batch")
            .await?;
        let result = self
            .delete_edge_mutations_batch_txn_locked(cell_id, mutations)
            .await;
        finish_local_write(lock, result).await
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

        let current_epoch = txn.seqnum();
        let commit_epoch =
            current_epoch
                .checked_add(1)
                .ok_or_else(|| GraphError::CorruptValue {
                    key: "storage_sequence".to_string(),
                    reason: "SlateDB storage sequence overflow during edge delete batch"
                        .to_string(),
                })?;
        let mut next_epoch = current_epoch;
        let mut results = Vec::with_capacity(mutations.len());
        let mut deleted = 0_u64;
        let mut already_deleted = 0_u64;
        let mut out_decrements = BTreeMap::<(String, VertexId), u64>::new();
        let mut in_decrements = BTreeMap::<(String, VertexId), u64>::new();
        let mut segment_edges_by_type_src =
            BTreeMap::<(String, VertexId), BTreeMap<VertexId, StorageSequence>>::new();
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

            next_epoch = commit_epoch;
            let result = DeleteResult {
                epoch: next_epoch,
                deleted: true,
            };
            mark_adjacency_dirty_txn(&txn, cell_id, &mutation.edge_type, next_epoch)?;
            let edge_metadata_key =
                keys::edge_metadata(cell_id, &mutation.edge_type, mutation.src, mutation.dst);
            let previous_edge_metadata = match read_txn_remote(&txn, &edge_metadata_key).await? {
                Some(value) => decode_edge_metadata(&edge_metadata_key, &value)?,
                None => EdgeMetadata::default(),
            };
            delete_relationships_for_structural_edge_txn(&txn, mutation, current_epoch).await?;
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
            txn.put(
                idem_key.as_bytes(),
                encode_delete_idempotency(mutation, &result),
            )?;
            deleted = deleted.saturating_add(1);
            results.push(result);
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
            .acquire_local_write_guard(&mutation.cell_id, "delete_edge")
            .await?;
        let result = self.delete_edge_txn_locked(mutation).await;
        finish_local_write(lock, result).await
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
            let current_epoch = txn.seqnum();
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
            let Some((segment_sequence, _segment_edge)) = segment_edge else {
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
                if !segment_edge_visible(segment_sequence, Some(tombstone_epoch)) {
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
                    key: "storage_sequence".to_string(),
                    reason: "epoch overflow".to_string(),
                })?;
            let result = DeleteResult {
                epoch,
                deleted: true,
            };
            mark_adjacency_dirty_txn(&txn, &mutation.cell_id, &mutation.edge_type, epoch)?;
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

            delete_relationships_for_structural_edge_txn(&txn, mutation, current_epoch).await?;
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
                idem_key.as_bytes(),
                encode_delete_idempotency(mutation, &result),
            )?;
            commit_txn_strict(txn, self.await_durable_writes).await?;
            return Ok(result);
        };

        decode_edge_record(&edge_key, &existing)?;
        let epoch = next_epoch_txn(&txn, &mutation.cell_id).await?;
        let result = DeleteResult {
            epoch,
            deleted: true,
        };
        mark_adjacency_dirty_txn(&txn, &mutation.cell_id, &mutation.edge_type, epoch)?;

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

        delete_relationships_for_structural_edge_txn(&txn, mutation, epoch.saturating_sub(1))
            .await?;
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

    pub async fn bulk_append_out_adjacency_segment_trusted(
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
        self.ensure_write_authority(cell_id, "bulk_append_out_adjacency_segment_trusted")?;

        let mut dsts: Vec<_> = dsts.into_iter().collect();
        ensure_limit(
            "bulk_append_out_adjacency_segment_trusted",
            dsts.len() as u64,
            self.limits.max_bulk_import_edges as u64,
        )?;
        dsts.sort_unstable();
        dsts.dedup();
        let edges: Vec<_> = dsts.iter().copied().map(|dst| (src, dst)).collect();
        let fingerprint = bulk_import_fingerprint(cell_id, edge_type, &edges);

        let _permit = self
            .acquire_graph_write_permit("bulk_append_out_adjacency_segment_trusted")
            .await?;
        let _writer = self.writer_lane(cell_id).lock().await;
        for attempt in 0..GRAPH_TXN_MAX_RETRIES {
            match self
                .bulk_append_out_adjacency_segment_trusted_txn(
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
                .write_edge_mutations_batch_txn(cell_id, &mutations, operation, endpoint_labels)
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

    pub(crate) async fn write_edge_mutations_batch_txn(
        &self,
        cell_id: &str,
        mutations: &[EdgeMutation],
        operation: &'static str,
        endpoint_labels: Option<(&str, &str)>,
    ) -> Result<EdgeMutationBatchResult> {
        let lock = self.acquire_local_write_guard(cell_id, operation).await?;
        let result = self
            .write_edge_mutations_batch_txn_locked(cell_id, mutations, operation, endpoint_labels)
            .await;
        finish_local_write(lock, result).await
    }

    async fn write_edge_mutations_batch_txn_locked(
        &self,
        cell_id: &str,
        mutations: &[EdgeMutation],
        operation: &'static str,
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

        let current_epoch = txn.seqnum();
        let commit_epoch =
            current_epoch
                .checked_add(1)
                .ok_or_else(|| GraphError::CorruptValue {
                    key: operation.to_string(),
                    reason: "SlateDB storage sequence overflow during edge mutation batch"
                        .to_string(),
                })?;
        let mut next_epoch = current_epoch;
        let mut results = Vec::with_capacity(mutations.len());
        let mut known_edges = BTreeMap::<(String, VertexId, VertexId), StorageSequence>::new();
        let mut validated_endpoints = BTreeSet::<(VertexId, String)>::new();
        let mut segment_edges_by_type_src =
            BTreeMap::<(String, VertexId), BTreeMap<VertexId, StorageSequence>>::new();
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
                decode_edge_record(&edge_key, &value)?;
                let result = CommitResult {
                    epoch: current_epoch,
                    already_existed: true,
                };
                known_edges.insert(identity, current_epoch);
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

            next_epoch = commit_epoch;
            let record = EdgeRecord {
                cell_id: cell_id.to_string(),
                edge_type: mutation.edge_type.clone(),
                src: mutation.src,
                dst: mutation.dst,
            };
            let result = CommitResult {
                epoch: next_epoch,
                already_existed: false,
            };
            let edge_value = encode_edge_record(&record);
            mark_adjacency_dirty_txn(&txn, cell_id, &mutation.edge_type, next_epoch)?;
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

    pub(crate) async fn bulk_append_out_adjacency_segment_trusted_txn(
        &self,
        cell_id: &str,
        edge_type: &str,
        src: VertexId,
        dsts: &[VertexId],
        idempotency_key: &str,
        fingerprint: u64,
    ) -> Result<BulkImportResult> {
        let lock = self
            .acquire_local_write_guard(cell_id, "bulk_append_out_adjacency_segment_trusted")
            .await?;
        let result = self
            .bulk_append_out_adjacency_segment_trusted_txn_locked(
                cell_id,
                edge_type,
                src,
                dsts,
                idempotency_key,
                fingerprint,
            )
            .await;
        finish_local_write(lock, result).await
    }

    async fn bulk_append_out_adjacency_segment_trusted_txn_locked(
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
        self.validate_write_fence_txn(&txn, cell_id, "bulk_append_out_adjacency_segment_trusted")
            .await?;
        let idem_key = keys::idempotency(cell_id, "segment-import", idempotency_key);
        if let Some(value) = read_txn_remote(&txn, &idem_key).await? {
            return decode_bulk_import_idempotency(&idem_key, idempotency_key, fingerprint, &value);
        }
        let fingerprint_key = segment_import_fingerprint_key(cell_id, edge_type, src, fingerprint);
        let fingerprint_result = read_txn_remote(&txn, &fingerprint_key).await?;

        let current_epoch = txn.seqnum();
        let existing =
            out_neighbors_for_src_txn(&txn, cell_id, edge_type, src, current_epoch).await?;
        if let Some(value) = fingerprint_result {
            let all_edges_still_exist = dsts.iter().all(|dst| existing.contains(dst));
            if all_edges_still_exist {
                return decode_bulk_import_fingerprint_idempotency(
                    &fingerprint_key,
                    fingerprint,
                    &value,
                );
            }
        }
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
        let start_epoch = if inserted == 0 {
            current_epoch
        } else {
            current_epoch
                .checked_add(1)
                .ok_or_else(|| GraphError::CorruptValue {
                    key: "segment_import".to_string(),
                    reason: "SlateDB storage sequence overflow during segment import".to_string(),
                })?
        };
        let end_epoch = start_epoch;
        let result = BulkImportResult {
            start_epoch,
            end_epoch,
            inserted,
            already_existed,
        };

        if inserted > 0 {
            mark_adjacency_dirty_txn(&txn, cell_id, edge_type, end_epoch)?;
            for dst in &inserted_dsts {
                txn.delete(keys::out_segment_tombstone(cell_id, edge_type, src, *dst).as_bytes())?;
            }
            txn.put(
                keys::out_segment(cell_id, edge_type, src, end_epoch, idempotency_key).as_bytes(),
                encode_out_edge_segment_records(&inserted_dsts),
            )?;
            let degree_key = keys::degree_out(cell_id, edge_type, src);
            let base = if current_epoch == 0 {
                0
            } else {
                read_counter_txn(&txn, &degree_key).await?
            };
            txn.put(degree_key.as_bytes(), encode_u64(base + inserted))?;
        }
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
            .acquire_local_write_guard(cell_id, "bulk_import_edges")
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
        finish_local_write(lock, result).await
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

        let current_epoch = txn.seqnum();
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
        let start_epoch = if inserted == 0 {
            current_epoch
        } else {
            current_epoch
                .checked_add(1)
                .ok_or_else(|| GraphError::CorruptValue {
                    key: "bulk_import".to_string(),
                    reason: "SlateDB storage sequence overflow during bulk import".to_string(),
                })?
        };
        let end_epoch = start_epoch;
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
        for (src, dst) in inserted_edges.iter().copied() {
            let epoch = end_epoch;
            let record = EdgeRecord {
                cell_id: cell_id.to_string(),
                edge_type: edge_type.to_string(),
                src,
                dst,
            };
            let edge_value = encode_edge_record(&record);
            mark_adjacency_dirty_txn(&txn, cell_id, edge_type, epoch)?;
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
            mark_adjacency_dirty_txn(&txn, cell_id, edge_type, end_epoch)?;
        }
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

async fn renew_vertex_delete_lock_after_items(lock: &LocalWriteGuard, items: u64) -> Result<()> {
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

fn validate_relationship_creates(
    cell_id: &str,
    edge_type: &str,
    relationships: impl IntoIterator<Item = RelationshipMutation>,
) -> Result<Vec<RelationshipMutation>> {
    relationships
        .into_iter()
        .map(|relationship| {
            validate_relationship_batch_entry(cell_id, edge_type, &relationship)?;
            Ok(relationship)
        })
        .collect()
}

fn validate_relationship_batch_entry(
    cell_id: &str,
    edge_type: &str,
    relationship: &RelationshipMutation,
) -> Result<()> {
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
    Ok(())
}

fn coalesce_relationship_imports(
    cell_id: &str,
    edge_type: &str,
    relationships: impl IntoIterator<Item = RelationshipMutation>,
) -> Result<Vec<RelationshipMutation>> {
    let mut by_id = BTreeMap::<RelationshipId, RelationshipMutation>::new();
    for relationship in relationships {
        validate_relationship_batch_entry(cell_id, edge_type, &relationship)?;
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
    _epoch: StorageSequence,
) -> Result<()> {
    validate_vertex_metadata(next)?;
    let vertex_key = keys::vertex(cell_id, vertex_id);
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
    Ok(())
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

fn apply_edge_metadata_update_txn(
    txn: &DbTransaction,
    target: EdgeMetadataTarget<'_>,
    previous: &EdgeMetadata,
    next: &EdgeMetadata,
    _epoch: StorageSequence,
) -> Result<()> {
    validate_edge_metadata(next)?;
    let edge_metadata_key =
        keys::edge_metadata(target.cell_id, target.edge_type, target.src, target.dst);
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
    Ok(())
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

async fn next_available_relationship_id_txn(
    txn: &DbTransaction,
    cell_id: &str,
    cursor: &mut RelationshipId,
    operation: &str,
) -> Result<RelationshipId> {
    loop {
        *cursor = cursor
            .checked_add(1)
            .ok_or_else(|| GraphError::CorruptValue {
                key: keys::last_relationship_id(cell_id),
                reason: format!("relationship id overflow during {operation}"),
            })?;
        if read_txn_remote(txn, &keys::relationship_id(cell_id, *cursor))
            .await?
            .is_none()
        {
            return Ok(*cursor);
        }
    }
}

#[cfg(feature = "opencypher")]
async fn relationship_ids_for_edge_property_txn(
    txn: &DbTransaction,
    lookup: RelationshipPropertyTxnLookup<'_>,
) -> Result<Vec<RelationshipId>> {
    let RelationshipPropertyTxnLookup {
        cell_id,
        edge_type,
        src,
        dst,
        property,
        value,
    } = lookup;
    let encoded = encode_vertex_property_value_key(value);
    let prefix = keys::relationship_property_index_edge_prefix(
        cell_id, edge_type, property, &encoded, src, dst,
    );
    let mut iter = txn.scan_prefix(prefix.as_bytes(), ..).await?;
    let mut relationship_ids = Vec::new();
    while let Some(kv) = iter.next().await? {
        let key = String::from_utf8_lossy(&kv.key).into_owned();
        let (
            parsed_cell_id,
            parsed_edge_type,
            parsed_property,
            parsed_encoded,
            parsed_src,
            parsed_dst,
            relationship_id,
        ) = parse_relationship_property_index_key(&key)?;
        if parsed_cell_id != cell_id
            || parsed_edge_type != edge_type
            || parsed_property != property
            || parsed_encoded != encoded
            || parsed_src != src
            || parsed_dst != dst
        {
            return Err(GraphError::CorruptValue {
                key,
                reason: "relationship property index escaped its requested prefix".to_string(),
            });
        }
        let record_key = keys::relationship(cell_id, edge_type, src, dst, relationship_id);
        let Some(record_value) = read_txn_remote(txn, &record_key).await? else {
            return Err(GraphError::CorruptValue {
                key,
                reason: format!("relationship property index points at missing {record_key}"),
            });
        };
        let record = decode_relationship_record(&record_key, &record_value)?;
        if record.metadata.properties.get(property) == Some(value) {
            relationship_ids.push(relationship_id);
        }
    }
    relationship_ids.sort_unstable();
    relationship_ids.dedup();
    Ok(relationship_ids)
}

async fn live_relationships_for_edge_txn(
    txn: &DbTransaction,
    cell_id: &str,
    edge_type: &str,
    src: VertexId,
    dst: VertexId,
    _read_epoch: StorageSequence,
) -> Result<Vec<RelationshipRecord>> {
    let prefix = keys::relationship_edge_prefix(cell_id, edge_type, src, dst);
    let mut iter = txn.scan_prefix(prefix.as_bytes(), ..).await?;
    let mut records = Vec::new();
    while let Some(kv) = iter.next().await? {
        let key = String::from_utf8_lossy(&kv.key).into_owned();
        records.push(decode_relationship_record(&key, &kv.value)?);
    }
    Ok(records)
}

async fn delete_structural_edge_txn(
    shard: &GraphShard,
    txn: &DbTransaction,
    mutation: &EdgeMutation,
    epoch: StorageSequence,
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
    mark_adjacency_dirty_txn(txn, &mutation.cell_id, &mutation.edge_type, epoch)?;
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
    Ok(())
}

fn mark_adjacency_dirty_txn(
    txn: &DbTransaction,
    cell_id: &str,
    edge_type: &str,
    epoch: StorageSequence,
) -> Result<()> {
    txn.put(
        keys::matrix_dirty(cell_id, edge_type).as_bytes(),
        encode_u64(epoch),
    )?;
    txn.put(
        keys::adjacency_generation(cell_id, edge_type).as_bytes(),
        encode_u64(epoch),
    )?;
    Ok(())
}

async fn delete_relationships_for_structural_edge_txn(
    txn: &DbTransaction,
    mutation: &EdgeMutation,
    read_epoch: StorageSequence,
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
        txn.delete(
            keys::relationship(
                &record.cell_id,
                &record.edge_type,
                record.src,
                record.dst,
                record.relationship_id,
            )
            .as_bytes(),
        )?;
        txn.delete(keys::relationship_id(&record.cell_id, record.relationship_id).as_bytes())?;
        delete_relationship_property_indexes_txn(txn, record, &record.metadata)?;
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
        reason: format!("too many relationships to delete with structural edge: {err}"),
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
