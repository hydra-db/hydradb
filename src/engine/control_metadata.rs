use super::*;

const CONTROL_CATALOG_PREFIX: &str = "control/catalog/";
const CONTROL_WATERMARK_PREFIX: &str = "control/watermark/";
const CONTROL_EDGE_WATERMARK_PREFIX: &str = "control/watermark_edge/";
const CONTROL_IDEMPOTENCY_PREFIX: &str = "control/idem/";

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GraphShardCatalogEntry {
    pub graph_id: Option<String>,
    pub cell_id: String,
    pub owner_node_id: String,
    pub lease_token: u64,
    pub schema_epoch: Option<GraphEpoch>,
    pub graph_epoch: Option<GraphEpoch>,
    pub generation: u64,
}

impl GraphShardCatalogEntry {
    pub fn has_graph_metadata(&self) -> bool {
        self.graph_id.is_some() && self.schema_epoch.is_some() && self.graph_epoch.is_some()
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct GraphControlWatermark {
    pub cell_id: String,
    pub durable_epoch: GraphEpoch,
    pub safe_read_epoch: GraphEpoch,
    pub outbox_epoch: GraphEpoch,
    pub artifact_epoch: GraphEpoch,
    pub generation: u64,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct GraphControlEdgeWatermark {
    pub cell_id: String,
    pub edge_type: String,
    pub durable_epoch: GraphEpoch,
    pub safe_read_epoch: GraphEpoch,
    pub outbox_epoch: GraphEpoch,
    pub artifact_epoch: GraphEpoch,
    pub generation: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GraphControlIdempotencyRecord {
    pub cell_id: String,
    pub operation: String,
    pub idempotency_key: String,
    pub result: Vec<u8>,
    pub generation: u64,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct GraphControlRepairReport {
    pub cell_id: String,
    pub edge_type: String,
    pub read_epoch: GraphEpoch,
    pub live_edges: u64,
    pub delta_records: u64,
    pub mismatch_count: u64,
    pub watermark: GraphControlEdgeWatermark,
    pub repaired_watermark: bool,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct GraphControlCellDropReport {
    pub cell_id: String,
    pub deleted_control_keys: u64,
}

impl GraphControlPlane {
    pub async fn publish_placement_with_catalog(
        &self,
        placement: &ShardPlacement,
        graph_id: &str,
        schema_epoch: GraphEpoch,
    ) -> Result<Vec<GraphShardCatalogEntry>> {
        validate_component("graph_id", graph_id)?;
        if !self.scope().is_default() && graph_id != self.scope().graph_id.as_str() {
            return Err(GraphError::GraphScopeMismatch {
                expected: self.scope().to_string(),
                actual: GraphScope::new(
                    self.scope().namespace.clone(),
                    GraphId::new(graph_id.to_string())?,
                )
                .to_string(),
            });
        }
        for attempt in 0..GRAPH_CONTROL_TXN_MAX_RETRIES {
            match self
                .publish_placement_with_catalog_txn(placement, graph_id, schema_epoch)
                .await
            {
                Err(GraphError::Slate(err))
                    if err.kind() == ErrorKind::Transaction
                        && attempt + 1 < GRAPH_CONTROL_TXN_MAX_RETRIES =>
                {
                    tokio::task::yield_now().await;
                }
                result => return result,
            }
        }
        Err(GraphError::RetryExhausted {
            operation: "control transaction",
            attempts: GRAPH_CONTROL_TXN_MAX_RETRIES,
        })
    }

    pub async fn publish_scoped_placement_with_catalog(
        &self,
        placement: &ShardPlacement,
        schema_epoch: GraphEpoch,
    ) -> Result<Vec<GraphShardCatalogEntry>> {
        self.publish_placement_with_catalog(placement, self.scope().graph_id.as_str(), schema_epoch)
            .await
    }

    async fn publish_placement_with_catalog_txn(
        &self,
        placement: &ShardPlacement,
        graph_id: &str,
        schema_epoch: GraphEpoch,
    ) -> Result<Vec<GraphShardCatalogEntry>> {
        let txn = self.db.begin(IsolationLevel::SerializableSnapshot).await?;
        let mut entries = Vec::with_capacity(placement.owners.len());
        for (cell_id, node_id) in &placement.owners {
            validate_component("cell_id", cell_id)?;
            validate_component("node_id", node_id)?;
            txn.put(
                control_placement_key(cell_id).as_bytes(),
                encode_control_placement(cell_id, node_id),
            )?;

            let metadata_key = control_catalog_key(cell_id);
            let existing = read_control_txn(&txn, &metadata_key)
                .await?
                .map(|value| decode_control_catalog(&metadata_key, &value))
                .transpose()?;
            let generation = existing
                .as_ref()
                .map(|entry| entry.generation)
                .unwrap_or_default()
                .checked_add(1)
                .ok_or_else(|| GraphError::CorruptValue {
                    key: metadata_key.clone(),
                    reason: "catalog generation overflow".to_string(),
                })?;
            let graph_epoch = existing
                .as_ref()
                .and_then(|entry| entry.graph_epoch)
                .unwrap_or_default();
            let lease_token = existing
                .as_ref()
                .map(|entry| entry.lease_token)
                .unwrap_or_default();
            let entry = GraphShardCatalogEntry {
                graph_id: Some(graph_id.to_string()),
                cell_id: cell_id.clone(),
                owner_node_id: node_id.clone(),
                lease_token,
                schema_epoch: Some(schema_epoch),
                graph_epoch: Some(graph_epoch),
                generation,
            };
            txn.put(metadata_key.as_bytes(), encode_control_catalog(&entry))?;
            entries.push(entry);
        }
        commit_control_txn(txn).await?;
        self.metrics
            .metadata_cas_successes
            .fetch_add(entries.len() as u64, Ordering::Relaxed);
        Ok(entries)
    }

    pub async fn current_shard_metadata(
        &self,
        cell_id: &str,
    ) -> Result<Option<GraphShardCatalogEntry>> {
        validate_component("cell_id", cell_id)?;
        let key = control_catalog_key(cell_id);
        self.db
            .get_with_options(key.as_bytes(), &control_read_options())
            .await?
            .map(|value| decode_control_catalog(&key, &value))
            .transpose()
    }

    pub async fn list_shard_metadata(&self) -> Result<Vec<GraphShardCatalogEntry>> {
        let mut iter = self
            .db
            .scan_prefix_with_options(
                CONTROL_CATALOG_PREFIX.as_bytes(),
                ..,
                &control_scan_options(),
            )
            .await?;
        let mut entries = Vec::new();
        while let Some(kv) = iter.next().await? {
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            entries.push(decode_control_catalog(&key, &kv.value)?);
        }
        entries.sort_by(|left, right| left.cell_id.cmp(&right.cell_id));
        Ok(entries)
    }

    pub async fn drop_cell_control_state(
        &self,
        cell_id: &str,
        expected_lease: Option<&ShardLease>,
    ) -> Result<GraphControlCellDropReport> {
        validate_component("cell_id", cell_id)?;
        if let Some(lease) = expected_lease {
            validate_component("node_id", &lease.owner_node_id)?;
            if lease.cell_id != cell_id {
                return Err(GraphError::StaleShardLease {
                    cell_id: cell_id.to_string(),
                    node_id: lease.owner_node_id.clone(),
                    lease_token: lease.lease_token,
                });
            }
        }
        for attempt in 0..GRAPH_CONTROL_TXN_MAX_RETRIES {
            match self
                .drop_cell_control_state_txn(cell_id, expected_lease)
                .await
            {
                Err(GraphError::Slate(err))
                    if err.kind() == ErrorKind::Transaction
                        && attempt + 1 < GRAPH_CONTROL_TXN_MAX_RETRIES =>
                {
                    tokio::task::yield_now().await;
                }
                result => return result,
            }
        }
        Err(GraphError::RetryExhausted {
            operation: "control transaction",
            attempts: GRAPH_CONTROL_TXN_MAX_RETRIES,
        })
    }

    async fn drop_cell_control_state_txn(
        &self,
        cell_id: &str,
        expected_lease: Option<&ShardLease>,
    ) -> Result<GraphControlCellDropReport> {
        let txn = self.db.begin(IsolationLevel::SerializableSnapshot).await?;
        let lease_key = control_lease_key(cell_id);
        if let Some(expected) = expected_lease {
            if let Some(value) = read_control_txn(&txn, &lease_key).await? {
                let current = decode_shard_lease(&lease_key, &value)?;
                if current.owner_node_id != expected.owner_node_id
                    || current.lease_token != expected.lease_token
                {
                    return Err(GraphError::StaleShardLease {
                        cell_id: cell_id.to_string(),
                        node_id: expected.owner_node_id.clone(),
                        lease_token: expected.lease_token,
                    });
                }
            }
        }

        let mut keys = vec![
            control_placement_key(cell_id),
            lease_key,
            control_lease_token_key(cell_id),
            control_catalog_key(cell_id),
            control_watermark_key(cell_id),
        ];
        keys.extend(
            control_keys_with_prefix_txn(&txn, &control_edge_watermark_cell_prefix(cell_id))
                .await?,
        );
        keys.extend(
            control_keys_with_prefix_txn(&txn, &control_idempotency_cell_prefix(cell_id)).await?,
        );
        keys.sort();
        keys.dedup();
        let deleted_control_keys = keys.len() as u64;
        for key in keys {
            txn.delete(key.as_bytes())?;
        }
        commit_control_txn(txn).await?;
        Ok(GraphControlCellDropReport {
            cell_id: cell_id.to_string(),
            deleted_control_keys,
        })
    }

    pub async fn compare_and_publish_shard_metadata(
        &self,
        mut entry: GraphShardCatalogEntry,
        expected_generation: Option<u64>,
    ) -> Result<GraphShardCatalogEntry> {
        validate_catalog_entry(&entry)?;
        self.metrics
            .metadata_cas_attempts
            .fetch_add(1, Ordering::Relaxed);
        for attempt in 0..GRAPH_CONTROL_TXN_MAX_RETRIES {
            match self
                .compare_and_publish_shard_metadata_txn(&mut entry, expected_generation)
                .await
            {
                Err(GraphError::Slate(err))
                    if err.kind() == ErrorKind::Transaction
                        && attempt + 1 < GRAPH_CONTROL_TXN_MAX_RETRIES =>
                {
                    tokio::task::yield_now().await;
                }
                Ok(published) => {
                    self.metrics
                        .metadata_cas_successes
                        .fetch_add(1, Ordering::Relaxed);
                    return Ok(published);
                }
                Err(err @ GraphError::ControlMetadataConflict { .. }) => {
                    self.metrics
                        .metadata_cas_conflicts
                        .fetch_add(1, Ordering::Relaxed);
                    return Err(err);
                }
                result => return result,
            }
        }
        Err(GraphError::RetryExhausted {
            operation: "control transaction",
            attempts: GRAPH_CONTROL_TXN_MAX_RETRIES,
        })
    }

    async fn compare_and_publish_shard_metadata_txn(
        &self,
        entry: &mut GraphShardCatalogEntry,
        expected_generation: Option<u64>,
    ) -> Result<GraphShardCatalogEntry> {
        let txn = self.db.begin(IsolationLevel::SerializableSnapshot).await?;
        let key = control_catalog_key(&entry.cell_id);
        let current = read_control_txn(&txn, &key)
            .await?
            .map(|value| decode_control_catalog(&key, &value))
            .transpose()?;
        let actual_generation = current.as_ref().map(|entry| entry.generation);
        if expected_generation.is_some() && actual_generation != expected_generation {
            return Err(GraphError::ControlMetadataConflict {
                key,
                expected_generation,
                actual_generation,
            });
        }
        entry.generation = actual_generation
            .unwrap_or_default()
            .checked_add(1)
            .ok_or_else(|| GraphError::CorruptValue {
                key: control_catalog_key(&entry.cell_id),
                reason: "catalog generation overflow".to_string(),
            })?;
        txn.put(
            control_catalog_key(&entry.cell_id).as_bytes(),
            encode_control_catalog(entry),
        )?;
        commit_control_txn(txn).await?;
        Ok(entry.clone())
    }

    pub async fn current_watermark(&self, cell_id: &str) -> Result<Option<GraphControlWatermark>> {
        validate_component("cell_id", cell_id)?;
        let key = control_watermark_key(cell_id);
        self.db
            .get_with_options(key.as_bytes(), &control_read_options())
            .await?
            .map(|value| decode_control_watermark(&key, &value))
            .transpose()
    }

    pub async fn advance_watermark(
        &self,
        mut requested: GraphControlWatermark,
        expected_generation: Option<u64>,
    ) -> Result<GraphControlWatermark> {
        validate_watermark(&requested)?;
        for attempt in 0..GRAPH_CONTROL_TXN_MAX_RETRIES {
            match self
                .advance_watermark_txn(&mut requested, expected_generation)
                .await
            {
                Err(GraphError::Slate(err))
                    if err.kind() == ErrorKind::Transaction
                        && attempt + 1 < GRAPH_CONTROL_TXN_MAX_RETRIES =>
                {
                    tokio::task::yield_now().await;
                }
                Ok((watermark, advanced)) => {
                    if advanced {
                        self.metrics
                            .watermark_advances
                            .fetch_add(1, Ordering::Relaxed);
                    }
                    return Ok(watermark);
                }
                Err(err @ GraphError::ControlMetadataConflict { .. }) => {
                    self.metrics
                        .metadata_cas_conflicts
                        .fetch_add(1, Ordering::Relaxed);
                    return Err(err);
                }
                Err(err @ GraphError::ControlWatermarkRegression { .. }) => {
                    self.metrics
                        .watermark_rejects
                        .fetch_add(1, Ordering::Relaxed);
                    return Err(err);
                }
                result => return result.map(|(watermark, _)| watermark),
            }
        }
        Err(GraphError::RetryExhausted {
            operation: "control transaction",
            attempts: GRAPH_CONTROL_TXN_MAX_RETRIES,
        })
    }

    async fn advance_watermark_txn(
        &self,
        requested: &mut GraphControlWatermark,
        expected_generation: Option<u64>,
    ) -> Result<(GraphControlWatermark, bool)> {
        let txn = self.db.begin(IsolationLevel::SerializableSnapshot).await?;
        let key = control_watermark_key(&requested.cell_id);
        let current = read_control_txn(&txn, &key)
            .await?
            .map(|value| decode_control_watermark(&key, &value))
            .transpose()?;
        let actual_generation = current.as_ref().map(|watermark| watermark.generation);
        if expected_generation.is_some() && actual_generation != expected_generation {
            return Err(GraphError::ControlMetadataConflict {
                key,
                expected_generation,
                actual_generation,
            });
        }

        if let Some(current) = current {
            reject_regression(
                &requested.cell_id,
                "durable_epoch",
                requested.durable_epoch,
                current.durable_epoch,
            )?;
            reject_regression(
                &requested.cell_id,
                "safe_read_epoch",
                requested.safe_read_epoch,
                current.safe_read_epoch,
            )?;
            reject_regression(
                &requested.cell_id,
                "outbox_epoch",
                requested.outbox_epoch,
                current.outbox_epoch,
            )?;
            reject_regression(
                &requested.cell_id,
                "artifact_epoch",
                requested.artifact_epoch,
                current.artifact_epoch,
            )?;
            let advanced = requested.durable_epoch > current.durable_epoch
                || requested.safe_read_epoch > current.safe_read_epoch
                || requested.outbox_epoch > current.outbox_epoch
                || requested.artifact_epoch > current.artifact_epoch;
            if !advanced {
                return Ok((current, false));
            }
            requested.generation =
                current
                    .generation
                    .checked_add(1)
                    .ok_or_else(|| GraphError::CorruptValue {
                        key: control_watermark_key(&requested.cell_id),
                        reason: "watermark generation overflow".to_string(),
                    })?;
        } else {
            requested.generation = 1;
        }

        txn.put(
            control_watermark_key(&requested.cell_id).as_bytes(),
            encode_control_watermark(requested),
        )?;
        commit_control_txn(txn).await?;
        Ok((requested.clone(), true))
    }

    pub async fn current_edge_watermark(
        &self,
        cell_id: &str,
        edge_type: &str,
    ) -> Result<Option<GraphControlEdgeWatermark>> {
        validate_component("cell_id", cell_id)?;
        validate_component("edge_type", edge_type)?;
        let key = control_edge_watermark_key(cell_id, edge_type);
        self.db
            .get_with_options(key.as_bytes(), &control_read_options())
            .await?
            .map(|value| decode_control_edge_watermark(&key, &value))
            .transpose()
    }

    pub async fn advance_edge_watermark(
        &self,
        mut requested: GraphControlEdgeWatermark,
        expected_generation: Option<u64>,
    ) -> Result<GraphControlEdgeWatermark> {
        validate_edge_watermark(&requested)?;
        for attempt in 0..GRAPH_CONTROL_TXN_MAX_RETRIES {
            match self
                .advance_edge_watermark_txn(&mut requested, expected_generation)
                .await
            {
                Err(GraphError::Slate(err))
                    if err.kind() == ErrorKind::Transaction
                        && attempt + 1 < GRAPH_CONTROL_TXN_MAX_RETRIES =>
                {
                    tokio::task::yield_now().await;
                }
                Ok((watermark, advanced)) => {
                    if advanced {
                        self.metrics
                            .watermark_advances
                            .fetch_add(1, Ordering::Relaxed);
                    }
                    return Ok(watermark);
                }
                Err(err @ GraphError::ControlMetadataConflict { .. }) => {
                    self.metrics
                        .metadata_cas_conflicts
                        .fetch_add(1, Ordering::Relaxed);
                    return Err(err);
                }
                Err(err @ GraphError::ControlWatermarkRegression { .. }) => {
                    self.metrics
                        .watermark_rejects
                        .fetch_add(1, Ordering::Relaxed);
                    return Err(err);
                }
                result => return result.map(|(watermark, _)| watermark),
            }
        }
        Err(GraphError::RetryExhausted {
            operation: "control transaction",
            attempts: GRAPH_CONTROL_TXN_MAX_RETRIES,
        })
    }

    async fn advance_edge_watermark_txn(
        &self,
        requested: &mut GraphControlEdgeWatermark,
        expected_generation: Option<u64>,
    ) -> Result<(GraphControlEdgeWatermark, bool)> {
        let txn = self.db.begin(IsolationLevel::SerializableSnapshot).await?;
        let key = control_edge_watermark_key(&requested.cell_id, &requested.edge_type);
        let current = read_control_txn(&txn, &key)
            .await?
            .map(|value| decode_control_edge_watermark(&key, &value))
            .transpose()?;
        let actual_generation = current.as_ref().map(|watermark| watermark.generation);
        if expected_generation.is_some() && actual_generation != expected_generation {
            return Err(GraphError::ControlMetadataConflict {
                key,
                expected_generation,
                actual_generation,
            });
        }

        if let Some(current) = current {
            reject_regression(
                &requested.cell_id,
                "durable_epoch",
                requested.durable_epoch,
                current.durable_epoch,
            )?;
            reject_regression(
                &requested.cell_id,
                "safe_read_epoch",
                requested.safe_read_epoch,
                current.safe_read_epoch,
            )?;
            reject_regression(
                &requested.cell_id,
                "outbox_epoch",
                requested.outbox_epoch,
                current.outbox_epoch,
            )?;
            reject_regression(
                &requested.cell_id,
                "artifact_epoch",
                requested.artifact_epoch,
                current.artifact_epoch,
            )?;
            let advanced = requested.durable_epoch > current.durable_epoch
                || requested.safe_read_epoch > current.safe_read_epoch
                || requested.outbox_epoch > current.outbox_epoch
                || requested.artifact_epoch > current.artifact_epoch;
            if !advanced {
                return Ok((current, false));
            }
            requested.generation =
                current
                    .generation
                    .checked_add(1)
                    .ok_or_else(|| GraphError::CorruptValue {
                        key: control_edge_watermark_key(&requested.cell_id, &requested.edge_type),
                        reason: "edge watermark generation overflow".to_string(),
                    })?;
        } else {
            requested.generation = 1;
        }

        txn.put(
            control_edge_watermark_key(&requested.cell_id, &requested.edge_type).as_bytes(),
            encode_control_edge_watermark(requested),
        )?;
        commit_control_txn(txn).await?;
        Ok((requested.clone(), true))
    }

    pub async fn control_idempotency_result(
        &self,
        cell_id: &str,
        operation: &str,
        idempotency_key: &str,
    ) -> Result<Option<GraphControlIdempotencyRecord>> {
        validate_control_idempotency(cell_id, operation, idempotency_key)?;
        let key = control_idempotency_key(cell_id, operation, idempotency_key);
        self.db
            .get_with_options(key.as_bytes(), &control_read_options())
            .await?
            .map(|value| decode_control_idempotency(&key, &value))
            .transpose()
    }

    pub async fn commit_control_idempotency(
        &self,
        cell_id: &str,
        operation: &str,
        idempotency_key: &str,
        result: &[u8],
    ) -> Result<GraphControlIdempotencyRecord> {
        validate_control_idempotency(cell_id, operation, idempotency_key)?;
        for attempt in 0..GRAPH_CONTROL_TXN_MAX_RETRIES {
            match self
                .commit_control_idempotency_txn(cell_id, operation, idempotency_key, result)
                .await
            {
                Err(GraphError::Slate(err))
                    if err.kind() == ErrorKind::Transaction
                        && attempt + 1 < GRAPH_CONTROL_TXN_MAX_RETRIES =>
                {
                    tokio::task::yield_now().await;
                }
                result => return result,
            }
        }
        Err(GraphError::RetryExhausted {
            operation: "control transaction",
            attempts: GRAPH_CONTROL_TXN_MAX_RETRIES,
        })
    }

    async fn commit_control_idempotency_txn(
        &self,
        cell_id: &str,
        operation: &str,
        idempotency_key: &str,
        result: &[u8],
    ) -> Result<GraphControlIdempotencyRecord> {
        let txn = self.db.begin(IsolationLevel::SerializableSnapshot).await?;
        let key = control_idempotency_key(cell_id, operation, idempotency_key);
        if let Some(value) = read_control_txn(&txn, &key).await? {
            let record = decode_control_idempotency(&key, &value)?;
            if record.result != result {
                return Err(GraphError::IdempotencyConflict {
                    operation: "control",
                    idempotency_key: idempotency_key.to_string(),
                });
            }
            self.metrics
                .control_idempotency_replays
                .fetch_add(1, Ordering::Relaxed);
            return Ok(record);
        }
        let record = GraphControlIdempotencyRecord {
            cell_id: cell_id.to_string(),
            operation: operation.to_string(),
            idempotency_key: idempotency_key.to_string(),
            result: result.to_vec(),
            generation: 1,
        };
        txn.put(key.as_bytes(), encode_control_idempotency(&record))?;
        commit_control_txn(txn).await?;
        self.metrics
            .control_idempotency_commits
            .fetch_add(1, Ordering::Relaxed);
        Ok(record)
    }

    pub async fn repair_cell_control_state(
        &self,
        shard: &GraphShard,
        cell_id: &str,
        edge_type: &str,
    ) -> Result<GraphControlRepairReport> {
        validate_component("cell_id", cell_id)?;
        validate_component("edge_type", edge_type)?;
        self.metrics.repair_runs.fetch_add(1, Ordering::Relaxed);

        let correctness = shard
            .verify_current_graph(cell_id, edge_type, 1, 64)
            .await?;
        if !correctness.is_clean() {
            return Err(GraphError::CorruptValue {
                key: control_edge_watermark_key(cell_id, edge_type),
                reason: format!(
                    "cannot repair control state while graph verifier reports {} mismatches",
                    correctness.mismatch_count
                ),
            });
        }

        let deltas = shard.outbox_since(cell_id, 0).await?;
        let delta_records = deltas
            .iter()
            .filter(|delta| delta.edge.edge_type == edge_type)
            .count() as u64;
        let outbox_epoch = deltas
            .iter()
            .filter(|delta| delta.edge.edge_type == edge_type)
            .map(|delta| delta.edge.epoch)
            .max()
            .unwrap_or_default();
        let matrix_epoch = shard
            .latest_matrix_artifact(cell_id, edge_type, correctness.read_epoch)
            .await?
            .map(|artifact| artifact.base_epoch)
            .unwrap_or_default();
        let rollup_epoch = shard
            .latest_rollup(cell_id, edge_type, correctness.read_epoch)
            .await?
            .map(|rollup| rollup.base_epoch)
            .unwrap_or_default();
        let artifact_epoch = matrix_epoch.max(rollup_epoch);

        let before = self.current_edge_watermark(cell_id, edge_type).await?;
        let outbox_epoch = before
            .as_ref()
            .map(|watermark| watermark.outbox_epoch)
            .unwrap_or_default()
            .max(outbox_epoch);
        let artifact_epoch = before
            .as_ref()
            .map(|watermark| watermark.artifact_epoch)
            .unwrap_or_default()
            .max(artifact_epoch);
        let requested = GraphControlEdgeWatermark {
            cell_id: cell_id.to_string(),
            edge_type: edge_type.to_string(),
            durable_epoch: correctness.read_epoch,
            safe_read_epoch: correctness.read_epoch,
            outbox_epoch,
            artifact_epoch,
            generation: 0,
        };
        let watermark = self.advance_edge_watermark(requested, None).await?;
        let repaired_watermark = match before.as_ref() {
            Some(before) => {
                watermark.durable_epoch > before.durable_epoch
                    || watermark.safe_read_epoch > before.safe_read_epoch
                    || watermark.outbox_epoch > before.outbox_epoch
                    || watermark.artifact_epoch > before.artifact_epoch
            }
            None => true,
        };
        if repaired_watermark {
            self.metrics.repair_actions.fetch_add(1, Ordering::Relaxed);
        }

        Ok(GraphControlRepairReport {
            cell_id: cell_id.to_string(),
            edge_type: edge_type.to_string(),
            read_epoch: correctness.read_epoch,
            live_edges: correctness.digest.live_edges,
            delta_records,
            mismatch_count: correctness.mismatch_count,
            watermark,
            repaired_watermark,
        })
    }
}

fn control_catalog_key(cell_id: &str) -> String {
    format!("{CONTROL_CATALOG_PREFIX}{cell_id}")
}

pub(super) async fn bump_catalog_lease_txn(
    txn: &DbTransaction,
    cell_id: &str,
    owner_node_id: &str,
    lease_token: u64,
) -> Result<()> {
    let key = control_catalog_key(cell_id);
    let Some(value) = read_control_txn(txn, &key).await? else {
        let entry = GraphShardCatalogEntry {
            graph_id: None,
            cell_id: cell_id.to_string(),
            owner_node_id: owner_node_id.to_string(),
            lease_token,
            schema_epoch: None,
            graph_epoch: None,
            generation: 1,
        };
        txn.put(key.as_bytes(), encode_control_catalog(&entry))?;
        return Ok(());
    };
    let mut entry = decode_control_catalog(&key, &value)?;
    entry.owner_node_id = owner_node_id.to_string();
    entry.lease_token = lease_token;
    entry.generation = entry
        .generation
        .checked_add(1)
        .ok_or_else(|| GraphError::CorruptValue {
            key: key.clone(),
            reason: "catalog generation overflow".to_string(),
        })?;
    txn.put(key.as_bytes(), encode_control_catalog(&entry))?;
    Ok(())
}

fn control_watermark_key(cell_id: &str) -> String {
    format!("{CONTROL_WATERMARK_PREFIX}{cell_id}")
}

fn control_edge_watermark_key(cell_id: &str, edge_type: &str) -> String {
    format!("{CONTROL_EDGE_WATERMARK_PREFIX}{cell_id}/{edge_type}")
}

async fn control_keys_with_prefix_txn(txn: &DbTransaction, prefix: &str) -> Result<Vec<String>> {
    let mut iter = txn.scan_prefix(prefix.as_bytes(), ..).await?;
    let mut keys = Vec::new();
    while let Some(kv) = iter.next().await? {
        keys.push(String::from_utf8_lossy(&kv.key).into_owned());
    }
    Ok(keys)
}

fn control_edge_watermark_cell_prefix(cell_id: &str) -> String {
    format!("{CONTROL_EDGE_WATERMARK_PREFIX}{cell_id}/")
}

fn control_idempotency_key(cell_id: &str, operation: &str, idempotency_key: &str) -> String {
    format!("{CONTROL_IDEMPOTENCY_PREFIX}{cell_id}/{operation}/{idempotency_key}")
}

fn control_idempotency_cell_prefix(cell_id: &str) -> String {
    format!("{CONTROL_IDEMPOTENCY_PREFIX}{cell_id}/")
}

fn validate_catalog_entry(entry: &GraphShardCatalogEntry) -> Result<()> {
    if let Some(graph_id) = &entry.graph_id {
        validate_component("graph_id", graph_id)?;
    }
    let graph_fields = [
        entry.graph_id.is_some(),
        entry.schema_epoch.is_some(),
        entry.graph_epoch.is_some(),
    ];
    if graph_fields.iter().any(|present| *present) && !graph_fields.iter().all(|present| *present) {
        return corrupt(
            &control_catalog_key(&entry.cell_id),
            "catalog graph metadata must be all present or all absent",
        );
    }
    validate_component("cell_id", &entry.cell_id)?;
    validate_component("node_id", &entry.owner_node_id)?;
    Ok(())
}

fn validate_watermark(watermark: &GraphControlWatermark) -> Result<()> {
    validate_component("cell_id", &watermark.cell_id)?;
    if watermark.safe_read_epoch > watermark.durable_epoch {
        return corrupt(
            &control_watermark_key(&watermark.cell_id),
            "safe_read_epoch cannot exceed durable_epoch",
        );
    }
    if watermark.outbox_epoch > watermark.durable_epoch {
        return corrupt(
            &control_watermark_key(&watermark.cell_id),
            "outbox_epoch cannot exceed durable_epoch",
        );
    }
    if watermark.artifact_epoch > watermark.durable_epoch {
        return corrupt(
            &control_watermark_key(&watermark.cell_id),
            "artifact_epoch cannot exceed durable_epoch",
        );
    }
    Ok(())
}

fn validate_edge_watermark(watermark: &GraphControlEdgeWatermark) -> Result<()> {
    validate_component("cell_id", &watermark.cell_id)?;
    validate_component("edge_type", &watermark.edge_type)?;
    if watermark.safe_read_epoch > watermark.durable_epoch {
        return corrupt(
            &control_edge_watermark_key(&watermark.cell_id, &watermark.edge_type),
            "safe_read_epoch cannot exceed durable_epoch",
        );
    }
    if watermark.outbox_epoch > watermark.durable_epoch {
        return corrupt(
            &control_edge_watermark_key(&watermark.cell_id, &watermark.edge_type),
            "outbox_epoch cannot exceed durable_epoch",
        );
    }
    if watermark.artifact_epoch > watermark.durable_epoch {
        return corrupt(
            &control_edge_watermark_key(&watermark.cell_id, &watermark.edge_type),
            "artifact_epoch cannot exceed durable_epoch",
        );
    }
    Ok(())
}

fn validate_control_idempotency(
    cell_id: &str,
    operation: &str,
    idempotency_key: &str,
) -> Result<()> {
    validate_component("cell_id", cell_id)?;
    validate_component("operation", operation)?;
    validate_component("idempotency_key", idempotency_key)?;
    Ok(())
}

fn reject_regression(
    cell_id: &str,
    field: &'static str,
    requested_epoch: GraphEpoch,
    current_epoch: GraphEpoch,
) -> Result<()> {
    if requested_epoch < current_epoch {
        return Err(GraphError::ControlWatermarkRegression {
            cell_id: cell_id.to_string(),
            field,
            requested_epoch,
            current_epoch,
        });
    }
    Ok(())
}

fn parse_optional_u64(key: &str, value: &str, field: &str) -> Result<Option<u64>> {
    if value.is_empty() {
        return Ok(None);
    }
    parse_u64(key, value, field).map(Some)
}

fn encode_control_catalog(entry: &GraphShardCatalogEntry) -> Vec<u8> {
    format!(
        "catalog2\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n",
        entry.graph_id.as_deref().unwrap_or(""),
        entry.cell_id,
        entry.owner_node_id,
        entry.lease_token,
        entry
            .schema_epoch
            .map(|epoch| epoch.to_string())
            .unwrap_or_default(),
        entry
            .graph_epoch
            .map(|epoch| epoch.to_string())
            .unwrap_or_default(),
        entry.generation
    )
    .into_bytes()
}

fn decode_control_catalog(key: &str, value: &[u8]) -> Result<GraphShardCatalogEntry> {
    let text = text_value(key, value)?;
    let parts: Vec<&str> = text.trim_end_matches('\n').split('\t').collect();
    if parts.len() != 8 {
        return corrupt(key, "expected catalog record with 8 fields");
    }
    if parts[0] == "catalog1" {
        let entry = GraphShardCatalogEntry {
            graph_id: Some(parts[1].to_string()),
            cell_id: parts[2].to_string(),
            owner_node_id: parts[3].to_string(),
            lease_token: parse_u64(key, parts[4], "lease_token")?,
            schema_epoch: Some(parse_u64(key, parts[5], "schema_epoch")?),
            graph_epoch: Some(parse_u64(key, parts[6], "graph_epoch")?),
            generation: parse_u64(key, parts[7], "generation")?,
        };
        validate_catalog_entry(&entry)?;
        return Ok(entry);
    }
    if parts[0] != "catalog2" {
        return corrupt(key, "expected catalog1 or catalog2 record");
    }
    let entry = GraphShardCatalogEntry {
        graph_id: (!parts[1].is_empty()).then(|| parts[1].to_string()),
        cell_id: parts[2].to_string(),
        owner_node_id: parts[3].to_string(),
        lease_token: parse_u64(key, parts[4], "lease_token")?,
        schema_epoch: parse_optional_u64(key, parts[5], "schema_epoch")?,
        graph_epoch: parse_optional_u64(key, parts[6], "graph_epoch")?,
        generation: parse_u64(key, parts[7], "generation")?,
    };
    validate_catalog_entry(&entry)?;
    Ok(entry)
}

fn encode_control_watermark(watermark: &GraphControlWatermark) -> Vec<u8> {
    format!(
        "watermark1\t{}\t{}\t{}\t{}\t{}\t{}\n",
        watermark.cell_id,
        watermark.durable_epoch,
        watermark.safe_read_epoch,
        watermark.outbox_epoch,
        watermark.artifact_epoch,
        watermark.generation
    )
    .into_bytes()
}

fn decode_control_watermark(key: &str, value: &[u8]) -> Result<GraphControlWatermark> {
    let text = text_value(key, value)?;
    let parts: Vec<&str> = text.trim_end_matches('\n').split('\t').collect();
    if parts.len() != 7 || parts[0] != "watermark1" {
        return corrupt(key, "expected watermark1 record with 7 fields");
    }
    let watermark = GraphControlWatermark {
        cell_id: parts[1].to_string(),
        durable_epoch: parse_u64(key, parts[2], "durable_epoch")?,
        safe_read_epoch: parse_u64(key, parts[3], "safe_read_epoch")?,
        outbox_epoch: parse_u64(key, parts[4], "outbox_epoch")?,
        artifact_epoch: parse_u64(key, parts[5], "artifact_epoch")?,
        generation: parse_u64(key, parts[6], "generation")?,
    };
    validate_watermark(&watermark)?;
    Ok(watermark)
}

fn encode_control_edge_watermark(watermark: &GraphControlEdgeWatermark) -> Vec<u8> {
    format!(
        "edge_watermark1\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n",
        watermark.cell_id,
        watermark.edge_type,
        watermark.durable_epoch,
        watermark.safe_read_epoch,
        watermark.outbox_epoch,
        watermark.artifact_epoch,
        watermark.generation
    )
    .into_bytes()
}

fn decode_control_edge_watermark(key: &str, value: &[u8]) -> Result<GraphControlEdgeWatermark> {
    let text = text_value(key, value)?;
    let parts: Vec<&str> = text.trim_end_matches('\n').split('\t').collect();
    if parts.len() != 8 || parts[0] != "edge_watermark1" {
        return corrupt(key, "expected edge_watermark1 record with 8 fields");
    }
    let watermark = GraphControlEdgeWatermark {
        cell_id: parts[1].to_string(),
        edge_type: parts[2].to_string(),
        durable_epoch: parse_u64(key, parts[3], "durable_epoch")?,
        safe_read_epoch: parse_u64(key, parts[4], "safe_read_epoch")?,
        outbox_epoch: parse_u64(key, parts[5], "outbox_epoch")?,
        artifact_epoch: parse_u64(key, parts[6], "artifact_epoch")?,
        generation: parse_u64(key, parts[7], "generation")?,
    };
    validate_edge_watermark(&watermark)?;
    Ok(watermark)
}

fn encode_control_idempotency(record: &GraphControlIdempotencyRecord) -> Vec<u8> {
    format!(
        "control_idem1\t{}\t{}\t{}\t{}\t{}\n",
        record.cell_id,
        record.operation,
        record.idempotency_key,
        record.generation,
        hex_encode(&record.result)
    )
    .into_bytes()
}

fn decode_control_idempotency(key: &str, value: &[u8]) -> Result<GraphControlIdempotencyRecord> {
    let text = text_value(key, value)?;
    let parts: Vec<&str> = text.trim_end_matches('\n').split('\t').collect();
    if parts.len() != 6 || parts[0] != "control_idem1" {
        return corrupt(key, "expected control_idem1 record with 6 fields");
    }
    validate_control_idempotency(parts[1], parts[2], parts[3])?;
    Ok(GraphControlIdempotencyRecord {
        cell_id: parts[1].to_string(),
        operation: parts[2].to_string(),
        idempotency_key: parts[3].to_string(),
        generation: parse_u64(key, parts[4], "generation")?,
        result: hex_decode(key, parts[5])?,
    })
}

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

fn hex_decode(key: &str, value: &str) -> Result<Vec<u8>> {
    let bytes = value.as_bytes();
    let chunks = bytes.chunks_exact(2);
    if !chunks.remainder().is_empty() {
        return corrupt(key, "hex payload has odd length");
    }
    let mut out = Vec::with_capacity(value.len() / 2);
    for pair in chunks {
        let hi = hex_value(key, pair[0])?;
        let lo = hex_value(key, pair[1])?;
        out.push((hi << 4) | lo);
    }
    Ok(out)
}

fn hex_value(key: &str, byte: u8) -> Result<u8> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        b'A'..=b'F' => Ok(byte - b'A' + 10),
        _ => corrupt(key, format!("invalid hex digit {}", byte as char)),
    }
}
