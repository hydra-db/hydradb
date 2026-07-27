use super::*;

impl GraphShard {
    pub async fn delete_graph_artifacts_before(
        &self,
        cell_id: &str,
        edge_type: &str,
        keep_epoch: StorageSequence,
    ) -> Result<ArtifactGcResult> {
        validate_component("cell_id", cell_id)?;
        validate_component("edge_type", edge_type)?;
        self.ensure_write_authority(cell_id, "delete_graph_artifacts_before")?;
        let _permit = self
            .acquire_gc_permit("delete_graph_artifacts_before")
            .await?;
        let started = Instant::now();
        let mut result = ArtifactGcResult::default();
        let mut batch = GraphWriteBatch::new();
        let mut pending_deletes = 0_usize;
        for prefix in graph_artifact_gc_prefixes(cell_id, edge_type) {
            let mut iter = self.scan_remote_prefix(&prefix).await?;
            while let Some(kv) = iter.next().await? {
                let key = String::from_utf8_lossy(&kv.key).into_owned();
                let Some(base_epoch) = graph_artifact_epoch_from_key(&key)? else {
                    result.retained_keys = result.retained_keys.saturating_add(1);
                    continue;
                };
                if base_epoch < keep_epoch {
                    batch.delete(key.as_bytes());
                    result.deleted_keys = result.deleted_keys.saturating_add(1);
                    pending_deletes = pending_deletes.saturating_add(1);
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
                    result.retained_keys = result.retained_keys.saturating_add(1);
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
        self.record_gc_completed(result.deleted_keys, started.elapsed());
        Ok(result)
    }
}
