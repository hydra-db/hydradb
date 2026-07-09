use super::*;

impl GraphShard {
    pub async fn delete_deltas_through_rollup(
        &self,
        cell_id: &str,
        edge_type: &str,
        compact_through_epoch: GraphEpoch,
    ) -> Result<DeltaGcResult> {
        validate_component("cell_id", cell_id)?;
        validate_component("edge_type", edge_type)?;
        self.ensure_write_authority(cell_id, "delete_deltas_through_rollup")?;
        let _permit = self
            .acquire_gc_permit("delete_deltas_through_rollup")
            .await?;
        let started = std::time::Instant::now();
        let safe_epoch = self.delta_gc_safe_epoch(cell_id, edge_type).await?;
        if compact_through_epoch > safe_epoch {
            return Err(self.record_retention_reject(
                "delete_deltas_through_rollup",
                cell_id,
                compact_through_epoch,
                safe_epoch,
            ));
        }
        let Some(artifact) = self
            .latest_matrix_artifact(cell_id, edge_type, compact_through_epoch)
            .await?
        else {
            return Err(GraphError::CorruptValue {
                key: keys::delta_gc_watermark(cell_id, edge_type),
                reason: "cannot compact deltas without a matrix rollup artifact".to_string(),
            });
        };
        if artifact.base_epoch != compact_through_epoch {
            return Err(GraphError::CorruptValue {
                key: keys::delta_gc_watermark(cell_id, edge_type),
                reason: format!(
                    "latest matrix artifact is at epoch {}, expected {compact_through_epoch}",
                    artifact.base_epoch
                ),
            });
        }

        let mut watermark_batch = GraphWriteBatch::new();
        watermark_batch.put(
            keys::delta_gc_watermark(cell_id, edge_type),
            encode_u64(compact_through_epoch),
        );
        self.write_graph_batch_strict(cell_id, "delete_deltas_through_rollup", watermark_batch)
            .await?;

        let mut result = DeltaGcResult {
            compacted_through_epoch: compact_through_epoch,
            ..DeltaGcResult::default()
        };
        self.delete_outbox_deltas_through(cell_id, edge_type, compact_through_epoch, &mut result)
            .await?;
        self.delete_outbox_delta_batches_through(
            cell_id,
            edge_type,
            compact_through_epoch,
            &mut result,
        )
        .await?;
        self.delete_delta_prefix_through(
            cell_id,
            &keys::delta_plus_prefix(cell_id, edge_type),
            compact_through_epoch,
            &mut result,
        )
        .await?;
        self.delete_delta_prefix_through(
            cell_id,
            &keys::delta_minus_prefix(cell_id, edge_type),
            compact_through_epoch,
            &mut result,
        )
        .await?;
        self.delete_owner_delta_prefix_through(
            cell_id,
            &keys::owner_delta_kind_prefix(cell_id, edge_type, DeltaKind::Plus),
            compact_through_epoch,
            &mut result,
        )
        .await?;
        self.delete_owner_delta_prefix_through(
            cell_id,
            &keys::owner_delta_kind_prefix(cell_id, edge_type, DeltaKind::Minus),
            compact_through_epoch,
            &mut result,
        )
        .await?;
        tracing::info!(
            target: "slatedb_graph_kernel",
            cell_id,
            edge_type,
            compact_through_epoch,
            deleted_delta_keys = result.deleted_delta_keys,
            retained_delta_keys = result.retained_delta_keys,
            "deleted graph deltas through rollup"
        );
        self.record_gc_completed(result.deleted_delta_keys, started.elapsed());
        Ok(result)
    }

    pub(crate) async fn delta_gc_watermark(
        &self,
        cell_id: &str,
        edge_type: &str,
    ) -> Result<GraphEpoch> {
        self.read_counter(&keys::delta_gc_watermark(cell_id, edge_type))
            .await
    }

    async fn delete_delta_prefix_through(
        &self,
        cell_id: &str,
        prefix: &str,
        compact_through_epoch: GraphEpoch,
        result: &mut DeltaGcResult,
    ) -> Result<()> {
        let mut iter = self.scan_remote_prefix(prefix).await?;
        let mut batch = GraphWriteBatch::new();
        let mut pending_deletes = 0_usize;
        while let Some(kv) = iter.next().await? {
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            let edge = decode_edge_record(&key, &kv.value)?;
            if edge.epoch <= compact_through_epoch {
                batch.delete(key.as_bytes());
                result.deleted_delta_keys += 1;
                pending_deletes += 1;
                if pending_deletes >= GRAPH_DELTA_GC_BATCH_KEYS {
                    self.flush_delta_gc_batch(cell_id, &mut batch, &mut pending_deletes)
                        .await?;
                }
            } else {
                result.retained_delta_keys += 1;
            }
        }
        self.flush_delta_gc_batch(cell_id, &mut batch, &mut pending_deletes)
            .await
    }

    async fn delete_outbox_deltas_through(
        &self,
        cell_id: &str,
        edge_type: &str,
        compact_through_epoch: GraphEpoch,
        result: &mut DeltaGcResult,
    ) -> Result<()> {
        let mut iter = self
            .scan_remote_prefix(&keys::outbox_prefix(cell_id))
            .await?;
        let mut batch = GraphWriteBatch::new();
        let mut pending_deletes = 0_usize;
        while let Some(kv) = iter.next().await? {
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            let delta = decode_delta_record(&key, &kv.value)?;
            if delta.edge.epoch > compact_through_epoch {
                break;
            }
            if delta.edge.edge_type != edge_type {
                continue;
            }
            batch.delete(key.as_bytes());
            result.deleted_delta_keys += 1;
            pending_deletes += 1;
            if pending_deletes >= GRAPH_DELTA_GC_BATCH_KEYS {
                self.flush_delta_gc_batch(cell_id, &mut batch, &mut pending_deletes)
                    .await?;
            }
        }
        self.flush_delta_gc_batch(cell_id, &mut batch, &mut pending_deletes)
            .await
    }

    async fn delete_outbox_delta_batches_through(
        &self,
        cell_id: &str,
        edge_type: &str,
        compact_through_epoch: GraphEpoch,
        result: &mut DeltaGcResult,
    ) -> Result<()> {
        let mut iter = self
            .scan_remote_prefix(&keys::outbox_batch_prefix(cell_id))
            .await?;
        let mut batch = GraphWriteBatch::new();
        let mut pending_deletes = 0_usize;
        while let Some(kv) = iter.next().await? {
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            let delta_batch = decode_outbox_delta_batch(&key, &kv.value)?;
            if delta_batch.end_epoch > compact_through_epoch {
                break;
            }
            if delta_batch.edge_type != edge_type {
                continue;
            }
            batch.delete(key.as_bytes());
            result.deleted_delta_keys += 1;
            pending_deletes += 1;
            if pending_deletes >= GRAPH_DELTA_GC_BATCH_KEYS {
                self.flush_delta_gc_batch(cell_id, &mut batch, &mut pending_deletes)
                    .await?;
            }
        }
        self.flush_delta_gc_batch(cell_id, &mut batch, &mut pending_deletes)
            .await
    }

    async fn delete_owner_delta_prefix_through(
        &self,
        cell_id: &str,
        prefix: &str,
        compact_through_epoch: GraphEpoch,
        result: &mut DeltaGcResult,
    ) -> Result<()> {
        let mut iter = self.scan_remote_prefix(prefix).await?;
        let mut batch = GraphWriteBatch::new();
        let mut pending_deletes = 0_usize;
        while let Some(kv) = iter.next().await? {
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            let delta = decode_delta_record(&key, &kv.value)?;
            if delta.edge.epoch <= compact_through_epoch {
                batch.delete(key.as_bytes());
                result.deleted_delta_keys += 1;
                pending_deletes += 1;
                if pending_deletes >= GRAPH_DELTA_GC_BATCH_KEYS {
                    self.flush_delta_gc_batch(cell_id, &mut batch, &mut pending_deletes)
                        .await?;
                }
            } else {
                result.retained_delta_keys += 1;
            }
        }
        self.flush_delta_gc_batch(cell_id, &mut batch, &mut pending_deletes)
            .await
    }

    async fn flush_delta_gc_batch(
        &self,
        cell_id: &str,
        batch: &mut GraphWriteBatch,
        pending_deletes: &mut usize,
    ) -> Result<()> {
        if *pending_deletes == 0 {
            return Ok(());
        }
        let batch_to_write = std::mem::replace(batch, GraphWriteBatch::new());
        self.write_graph_batch_strict(cell_id, "delete_deltas_through_rollup", batch_to_write)
            .await?;
        *pending_deletes = 0;
        Ok(())
    }

    pub async fn edge_exists_at(
        &self,
        cell_id: &str,
        edge_type: &str,
        src: VertexId,
        dst: VertexId,
        read_epoch: GraphEpoch,
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
        read_epoch: GraphEpoch,
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
        read_epoch: GraphEpoch,
    ) -> Result<u64> {
        Ok(self
            .out_neighbors_at(cell_id, edge_type, src, read_epoch)
            .await?
            .len() as u64)
    }

    pub async fn compact_supernode_segments(
        &self,
        cell_id: &str,
        edge_type: &str,
        src: VertexId,
        compacted_through_epoch: GraphEpoch,
        idempotency_key: &str,
    ) -> Result<SegmentCompactionResult> {
        validate_component("cell_id", cell_id)?;
        validate_component("edge_type", edge_type)?;
        validate_component("idempotency_key", idempotency_key)?;
        self.ensure_write_authority(cell_id, "compact_supernode_segments")?;

        let started = std::time::Instant::now();
        let _permit = self.acquire_gc_permit("compact_supernode_segments").await?;
        let _writer = self.writer_lane(cell_id).lock().await;
        let lock = self
            .acquire_cell_write_lock(cell_id, "compact_supernode_segments")
            .await?;
        let result = self
            .compact_supernode_segments_locked(
                cell_id,
                edge_type,
                src,
                compacted_through_epoch,
                idempotency_key,
                started,
            )
            .await;
        release_cell_write_lock(lock, result).await
    }

    async fn compact_supernode_segments_locked(
        &self,
        cell_id: &str,
        edge_type: &str,
        src: VertexId,
        compacted_through_epoch: GraphEpoch,
        idempotency_key: &str,
        started: std::time::Instant,
    ) -> Result<SegmentCompactionResult> {
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
                reason: "cannot compact segments without a matrix rollup artifact".to_string(),
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
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            let segment = decode_out_edge_segment(&key, &kv.value)?;
            if segment.start_epoch > compacted_through_epoch {
                break;
            }
            if segment.end_epoch > compacted_through_epoch {
                continue;
            }
            input_edges = input_edges.saturating_add(segment.edges.len() as u64);
            source_segments.push((key, segment));
        }

        let mut tombstone_iter = self
            .scan_remote_prefix(&keys::out_segment_tombstone_src_prefix(
                cell_id, edge_type, src,
            ))
            .await?;
        let mut tombstones = BTreeMap::<VertexId, (GraphEpoch, String)>::new();
        while let Some(kv) = tombstone_iter.next().await? {
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

        let mut live = BTreeMap::<VertexId, GraphEpoch>::new();
        for (_, segment) in &source_segments {
            for (epoch, dst) in &segment.edges {
                if *epoch > compacted_through_epoch {
                    continue;
                }
                let tombstone_epoch = tombstones
                    .get(dst)
                    .map(|(epoch, _)| *epoch)
                    .filter(|epoch| *epoch <= compacted_through_epoch);
                if segment_edge_visible(*epoch, tombstone_epoch) {
                    live.entry(*dst)
                        .and_modify(|existing| *existing = (*existing).max(*epoch))
                        .or_insert(*epoch);
                }
            }
        }
        let mut compacted_edges: Vec<_> =
            live.into_iter().map(|(dst, epoch)| (epoch, dst)).collect();
        compacted_edges.sort_unstable();

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
        if let (Some((start_epoch, _)), Some((end_epoch, _))) =
            (compacted_edges.first(), compacted_edges.last())
        {
            batch.put(
                keys::out_segment(
                    cell_id,
                    edge_type,
                    src,
                    *end_epoch,
                    *start_epoch,
                    &format!("compact-{idempotency_key}"),
                ),
                encode_out_edge_segment_records(&compacted_edges),
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
            output_edges: compacted_edges.len() as u64,
        };
        batch.put(
            idem_key.as_bytes(),
            encode_segment_compaction_idempotency(idempotency_key, &result),
        );
        self.write_graph_batch_strict(cell_id, "compact_supernode_segments", batch)
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
            .scan_prefix_with_options(prefix.as_bytes(), .., &remote_scan_options())
            .await?;
        Ok(iter)
    }

    pub(crate) async fn scan_remote_prefix_from(
        &self,
        prefix: &str,
        start_suffix: &str,
    ) -> Result<slatedb::DbIterator> {
        let iter = self
            .db
            .scan_prefix_with_options(
                prefix.as_bytes(),
                start_suffix.as_bytes().to_vec()..,
                &remote_scan_options(),
            )
            .await?;
        Ok(iter)
    }

    #[cfg(test)]
    pub(crate) async fn write_strict_for_test(&self, batch: WriteBatch) -> Result<()> {
        let options = WriteOptions {
            await_durable: true,
            ..Default::default()
        };
        self.db.write_with_options(batch, &options).await?;
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
                Err(err @ GraphError::StaleShardLease { .. }) => {
                    self.operation_metrics
                        .stale_write_rejects
                        .fetch_add(1, Ordering::Relaxed);
                    return Err(err);
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

    async fn write_graph_batch_txn(
        &self,
        cell_id: &str,
        operation: &'static str,
        batch: GraphWriteBatch,
    ) -> Result<()> {
        let txn = self.db.begin(IsolationLevel::SerializableSnapshot).await?;
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
}
