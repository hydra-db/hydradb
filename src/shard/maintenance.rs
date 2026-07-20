use super::*;

struct SegmentCompactionRequest<'a> {
    cell_id: &'a str,
    edge_type: &'a str,
    src: VertexId,
    compacted_through_epoch: StorageSequence,
    idempotency_key: &'a str,
    started: std::time::Instant,
    lock: &'a LocalWriteGuard,
}

impl GraphShard {
    pub async fn edge_exists_at(
        &self,
        cell_id: &str,
        edge_type: &str,
        src: VertexId,
        dst: VertexId,
        read_epoch: StorageSequence,
    ) -> Result<bool> {
        Ok(self
            .edges_at(cell_id, edge_type, read_epoch)
            .await?
            .into_iter()
            .any(|edge| edge.src == src && edge.dst == dst))
    }

    pub async fn out_neighbors_at(
        &self,
        cell_id: &str,
        edge_type: &str,
        src: VertexId,
        read_epoch: StorageSequence,
    ) -> Result<Vec<VertexId>> {
        let mut neighbors: Vec<_> = self
            .edges_at(cell_id, edge_type, read_epoch)
            .await?
            .into_iter()
            .filter_map(|edge| (edge.src == src).then_some(edge.dst))
            .collect();
        neighbors.sort_unstable();
        Ok(neighbors)
    }

    pub async fn out_degree_at(
        &self,
        cell_id: &str,
        edge_type: &str,
        src: VertexId,
        read_epoch: StorageSequence,
    ) -> Result<u64> {
        Ok(self
            .out_neighbors_at(cell_id, edge_type, src, read_epoch)
            .await?
            .len() as u64)
    }

    pub async fn compact_out_adjacency_segments(
        &self,
        cell_id: &str,
        edge_type: &str,
        src: VertexId,
        compacted_through_epoch: StorageSequence,
        idempotency_key: &str,
    ) -> Result<SegmentCompactionResult> {
        validate_component("cell_id", cell_id)?;
        validate_component("edge_type", edge_type)?;
        validate_component("idempotency_key", idempotency_key)?;
        self.ensure_write_authority(cell_id, "compact_out_adjacency_segments")?;

        let started = std::time::Instant::now();
        let _permit = self
            .acquire_gc_permit("compact_out_adjacency_segments")
            .await?;
        let _writer = self.writer_lane(cell_id).lock().await;
        let lock = self
            .acquire_local_write_guard(cell_id, "compact_out_adjacency_segments")
            .await?;
        let result = self
            .compact_out_adjacency_segments_locked(SegmentCompactionRequest {
                cell_id,
                edge_type,
                src,
                compacted_through_epoch,
                idempotency_key,
                started,
                lock: &lock,
            })
            .await;
        finish_local_write(lock, result).await
    }

    async fn compact_out_adjacency_segments_locked(
        &self,
        request: SegmentCompactionRequest<'_>,
    ) -> Result<SegmentCompactionResult> {
        let SegmentCompactionRequest {
            cell_id,
            edge_type,
            src,
            compacted_through_epoch,
            idempotency_key,
            started,
            lock,
        } = request;
        let idempotency_operation = segment_compaction_idempotency_operation(edge_type, src);
        let idem_key = keys::idempotency(cell_id, &idempotency_operation, idempotency_key);
        if let Some(value) = self.read_remote(&idem_key).await? {
            return decode_segment_compaction_idempotency(
                &idem_key,
                idempotency_key,
                compacted_through_epoch,
                &value,
            );
        }

        let current_epoch = self.current_epoch(cell_id).await?;
        if compacted_through_epoch > current_epoch {
            return Err(GraphError::SnapshotAhead {
                cell_id: cell_id.to_string(),
                read_epoch: compacted_through_epoch,
                current_epoch,
            });
        }
        let Some(artifact) = self
            .latest_matrix_artifact(cell_id, edge_type, compacted_through_epoch)
            .await?
        else {
            return Err(GraphError::CorruptValue {
                key: keys::out_segment_src_prefix(cell_id, edge_type, src),
                reason: "cannot compact adjacency segments without a matrix artifact".to_string(),
            });
        };
        if artifact.base_epoch != compacted_through_epoch {
            return Err(GraphError::CorruptValue {
                key: keys::out_segment_src_prefix(cell_id, edge_type, src),
                reason: format!(
                    "latest matrix artifact is at epoch {}, expected {compacted_through_epoch}",
                    artifact.base_epoch
                ),
            });
        }
        let current_degree = self.out_neighbors(cell_id, edge_type, src).await?.len() as u64;

        let mut segment_iter = self
            .scan_remote_prefix(&keys::out_segment_src_prefix(cell_id, edge_type, src))
            .await?;
        let mut source_segments = Vec::new();
        let mut input_edges = 0_u64;
        while let Some(kv) = segment_iter.next().await? {
            if should_renew_cell_lock_after_items(source_segments.len()) {
                lock.renew().await?;
            }
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            let segment = decode_out_edge_segment(&key, &kv.value)?;
            if segment.storage_sequence > compacted_through_epoch {
                break;
            }
            input_edges = input_edges.saturating_add(segment.destinations.len() as u64);
            source_segments.push((key, segment));
        }

        let mut tombstone_iter = self
            .scan_remote_prefix(&keys::out_segment_tombstone_src_prefix(
                cell_id, edge_type, src,
            ))
            .await?;
        let mut tombstones = BTreeMap::<VertexId, (StorageSequence, String)>::new();
        while let Some(kv) = tombstone_iter.next().await? {
            if should_renew_cell_lock_after_items(tombstones.len()) {
                lock.renew().await?;
            }
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            let (key_cell_id, key_edge_type, key_src, dst) =
                parse_out_edge_segment_tombstone_key(&key)?;
            if key_cell_id != cell_id || key_edge_type != edge_type || key_src != src {
                return Err(GraphError::CorruptValue {
                    key,
                    reason: "segment tombstone identity does not match scan prefix".to_string(),
                });
            }
            let epoch = decode_u64(&key, &kv.value)?;
            if epoch <= current_epoch {
                tombstones.insert(dst, (epoch, key));
            }
        }

        let mut live = BTreeMap::<VertexId, StorageSequence>::new();
        for (_, segment) in &source_segments {
            for dst in &segment.destinations {
                let tombstone_epoch = tombstones
                    .get(dst)
                    .map(|(epoch, _)| *epoch)
                    .filter(|epoch| *epoch <= compacted_through_epoch);
                if segment_edge_visible(segment.storage_sequence, tombstone_epoch) {
                    live.entry(*dst)
                        .and_modify(|existing| {
                            *existing = (*existing).max(segment.storage_sequence)
                        })
                        .or_insert(segment.storage_sequence);
                }
            }
        }
        let compacted_destinations: Vec<_> = live.into_keys().collect();

        let mut batch = GraphWriteBatch::new();
        for (key, _) in &source_segments {
            batch.delete(key.as_bytes());
        }
        let mut deleted_tombstone_keys = 0_u64;
        for (_, (epoch, key)) in tombstones {
            if epoch <= compacted_through_epoch {
                batch.delete(key.as_bytes());
                deleted_tombstone_keys = deleted_tombstone_keys.saturating_add(1);
            }
        }
        if !compacted_destinations.is_empty() {
            batch.put(
                keys::out_segment(
                    cell_id,
                    edge_type,
                    src,
                    compacted_through_epoch,
                    &format!("compact-{idempotency_key}"),
                ),
                encode_out_edge_segment_records(&compacted_destinations),
            );
        }
        batch.put(
            keys::degree_out(cell_id, edge_type, src),
            encode_u64(current_degree),
        );
        let result = SegmentCompactionResult {
            compacted_through_epoch,
            source_segments: source_segments.len() as u64,
            deleted_segment_keys: source_segments.len() as u64,
            deleted_tombstone_keys,
            input_edges,
            output_edges: compacted_destinations.len() as u64,
        };
        batch.put(
            idem_key.as_bytes(),
            encode_segment_compaction_idempotency(idempotency_key, &result),
        );
        lock.renew().await?;
        self.write_graph_batch_strict(cell_id, "compact_out_adjacency_segments", batch)
            .await?;
        self.record_gc_completed(
            result
                .deleted_segment_keys
                .saturating_add(result.deleted_tombstone_keys),
            started.elapsed(),
        );
        Ok(result)
    }

    pub(crate) async fn read_counter(&self, key: &str) -> Result<u64> {
        match self.read_remote(key).await? {
            Some(value) => decode_u64(key, &value),
            None => Ok(0),
        }
    }

    pub(crate) async fn read_remote(&self, key: &str) -> Result<Option<Bytes>> {
        let value = self
            .db
            .get_with_options(key.as_bytes(), &remote_read_options())
            .await?;
        Ok(value)
    }

    pub(crate) async fn scan_remote_prefix(&self, prefix: &str) -> Result<slatedb::DbIterator> {
        let iter = self
            .db
            .scan_prefix_with_options(prefix.as_bytes(), None, &remote_scan_options())
            .await?;
        Ok(iter)
    }

    #[cfg(test)]
    pub(crate) async fn write_strict_for_test(&self, batch: WriteBatch) -> Result<()> {
        let options = WriteOptions {
            await_durable: true,
            ..Default::default()
        };
        self.db
            .writer()?
            .write_with_options(batch, &options)
            .await?;
        Ok(())
    }

    pub(crate) async fn write_graph_batch_strict(
        &self,
        cell_id: &str,
        operation: &'static str,
        batch: GraphWriteBatch,
    ) -> Result<()> {
        validate_component("cell_id", cell_id)?;
        if batch.is_empty() {
            return Ok(());
        }
        let record_count = batch.len();
        let started = std::time::Instant::now();
        for attempt in 0..GRAPH_TXN_MAX_RETRIES {
            match self
                .write_graph_batch_txn(cell_id, operation, batch.clone())
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
                    self.record_graph_batch_commit(operation, record_count, started.elapsed());
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

    pub(crate) async fn write_graph_batch_strict_guarded(
        &self,
        cell_id: &str,
        operation: &'static str,
        guards: Vec<GraphWriteGuard>,
        batch: GraphWriteBatch,
    ) -> Result<()> {
        validate_component("cell_id", cell_id)?;
        if batch.is_empty() {
            return Ok(());
        }
        let record_count = batch.len();
        let started = std::time::Instant::now();
        for attempt in 0..GRAPH_TXN_MAX_RETRIES {
            match self
                .write_graph_batch_guarded_txn(cell_id, operation, &guards, batch.clone())
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
                    self.record_graph_batch_commit(operation, record_count, started.elapsed());
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

    pub(crate) async fn write_graph_batch_strict_with_cell_lock(
        &self,
        cell_id: &str,
        operation: &'static str,
        batch: GraphWriteBatch,
    ) -> Result<()> {
        if batch.is_empty() {
            return Ok(());
        }
        let lock = self.acquire_local_write_guard(cell_id, operation).await?;
        let result = self
            .write_graph_batch_strict(cell_id, operation, batch)
            .await;
        finish_local_write(lock, result).await
    }

    async fn write_graph_batch_txn(
        &self,
        cell_id: &str,
        operation: &'static str,
        batch: GraphWriteBatch,
    ) -> Result<()> {
        let txn = self
            .db
            .writer()?
            .begin(IsolationLevel::SerializableSnapshot)
            .await?;
        self.validate_write_fence_txn(&txn, cell_id, operation)
            .await?;
        for op in batch.ops {
            match op {
                GraphWriteOp::Put(key, value) => txn.put(key.as_ref(), value.as_ref())?,
                GraphWriteOp::Delete(key) => txn.delete(key.as_ref())?,
            }
        }
        commit_txn_strict(txn, self.await_durable_writes).await
    }

    async fn write_graph_batch_guarded_txn(
        &self,
        cell_id: &str,
        operation: &'static str,
        guards: &[GraphWriteGuard],
        batch: GraphWriteBatch,
    ) -> Result<()> {
        let txn = self
            .db
            .writer()?
            .begin(IsolationLevel::SerializableSnapshot)
            .await?;
        self.validate_write_fence_txn(&txn, cell_id, operation)
            .await?;
        for guard in guards {
            let (key, matches) = match guard {
                GraphWriteGuard::Absent(key) => {
                    let key_text = String::from_utf8_lossy(key).into_owned();
                    let matches = read_txn_remote(&txn, &key_text).await?.is_none();
                    (key_text, matches)
                }
                GraphWriteGuard::Equals(key, expected) => {
                    let key_text = String::from_utf8_lossy(key).into_owned();
                    let matches = read_txn_remote(&txn, &key_text)
                        .await?
                        .as_ref()
                        .is_some_and(|actual| actual.as_ref() == expected.as_ref());
                    (key_text, matches)
                }
            };
            if !matches {
                return Err(GraphError::ConditionalWriteConflict { operation, key });
            }
        }
        for op in batch.ops {
            match op {
                GraphWriteOp::Put(key, value) => txn.put(key.as_ref(), value.as_ref())?,
                GraphWriteOp::Delete(key) => txn.delete(key.as_ref())?,
            }
        }
        commit_txn_strict(txn, self.await_durable_writes).await
    }
}

fn should_renew_cell_lock_after_items(items: usize) -> bool {
    items == 0
        || (items >= GRAPH_MAINTENANCE_BATCH_KEYS
            && items / GRAPH_MAINTENANCE_BATCH_KEYS * GRAPH_MAINTENANCE_BATCH_KEYS == items)
}
