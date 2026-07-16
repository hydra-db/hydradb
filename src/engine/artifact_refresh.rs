use super::*;

impl MatrixArtifactRefreshHandle {
    pub async fn stop(self) -> Result<()> {
        let _ = self.stop_tx.send(true);
        match self.task.await {
            Ok(result) => result,
            Err(err) => Err(GraphError::CorruptValue {
                key: "artifact/matrix_refresh".to_string(),
                reason: format!("matrix artifact refresh task failed: {err}"),
            }),
        }
    }
}

impl MatrixArtifactRefreshPolicy {
    fn validate(&self) -> Result<()> {
        if self.interval.is_zero() {
            return invalid_refresh_policy("interval must be greater than zero");
        }
        if self.max_dirty_age.is_zero() {
            return invalid_refresh_policy("max dirty age must be greater than zero");
        }
        if self.min_epoch_lag == 0 {
            return invalid_refresh_policy("minimum epoch lag must be greater than zero");
        }
        if self.tile_size == 0 {
            return invalid_refresh_policy("tile size must be greater than zero");
        }
        if self.max_edge_types_per_cycle == 0 {
            return invalid_refresh_policy("edge types per cycle must be greater than zero");
        }
        Ok(())
    }
}

impl RoutedGraphCluster {
    pub fn start_matrix_artifact_refresh_job(
        self: &Arc<Self>,
        policy: MatrixArtifactRefreshPolicy,
    ) -> Result<MatrixArtifactRefreshHandle> {
        policy.validate()?;
        let cluster = Arc::clone(self);
        let metrics = Arc::clone(&self.maintenance_metrics);
        let (stop_tx, mut stop_rx) = watch::channel(false);
        let task = tokio::spawn(async move {
            let mut first_observed_dirty = BTreeMap::<(String, String), Instant>::new();
            let mut next_candidate_offset = 0_usize;
            loop {
                metrics
                    .matrix_refresh_cycles
                    .fetch_add(1, Ordering::Relaxed);
                let shards = cluster
                    .shards
                    .iter()
                    .map(|(cell_id, shard)| (cell_id.clone(), Arc::clone(shard)))
                    .collect::<Vec<_>>();
                match refresh_matrix_artifacts_once_for_shards(
                    shards,
                    &policy,
                    &mut first_observed_dirty,
                    &mut next_candidate_offset,
                )
                .await
                {
                    Ok(report) => {
                        metrics
                            .matrix_refresh_dirty_edge_types
                            .fetch_add(report.dirty_edge_types, Ordering::Relaxed);
                        metrics
                            .matrix_refresh_artifacts_built
                            .fetch_add(report.artifacts_built, Ordering::Relaxed);
                        metrics
                            .matrix_refresh_artifacts_deferred
                            .fetch_add(report.artifacts_deferred, Ordering::Relaxed);
                        metrics
                            .matrix_refresh_failures
                            .fetch_add(report.artifact_failures, Ordering::Relaxed);
                    }
                    Err(err) => {
                        metrics
                            .matrix_refresh_failures
                            .fetch_add(1, Ordering::Relaxed);
                        tracing::warn!(
                            target: "slatedb_graph_kernel",
                            error = %err,
                            "automatic matrix artifact refresh failed"
                        );
                    }
                }
                tokio::select! {
                    changed = stop_rx.changed() => {
                        if changed.is_err() || *stop_rx.borrow() {
                            break;
                        }
                    }
                    _ = tokio::time::sleep(policy.interval) => {}
                }
            }
            Ok(())
        });
        Ok(MatrixArtifactRefreshHandle { stop_tx, task })
    }
}

async fn refresh_matrix_artifacts_once_for_shards(
    shards: Vec<(String, Arc<GraphShard>)>,
    policy: &MatrixArtifactRefreshPolicy,
    first_observed_dirty: &mut BTreeMap<(String, String), Instant>,
    next_candidate_offset: &mut usize,
) -> Result<MatrixArtifactRefreshReport> {
    let mut report = MatrixArtifactRefreshReport::default();
    let mut candidates = Vec::new();
    let mut dirty_keys = BTreeSet::new();

    for (cell_id, shard) in &shards {
        let dirty = match shard.matrix_dirty_edge_types(cell_id).await {
            Ok(dirty) => dirty,
            Err(err) => {
                report.artifact_failures = report.artifact_failures.saturating_add(1);
                tracing::warn!(
                    target: "slatedb_graph_kernel",
                    cell_id,
                    error = %err,
                    "failed to scan dirty matrix edge types"
                );
                continue;
            }
        };
        report.dirty_edge_types = report.dirty_edge_types.saturating_add(dirty.len() as u64);
        for (edge_type, dirty_epoch) in dirty {
            let key = (cell_id.clone(), edge_type.clone());
            dirty_keys.insert(key);
            candidates.push((cell_id.clone(), Arc::clone(shard), edge_type, dirty_epoch));
        }
    }
    first_observed_dirty.retain(|key, _| dirty_keys.contains(key));
    if candidates.is_empty() {
        *next_candidate_offset = 0;
        return Ok(report);
    }

    candidates.sort_by(|left, right| (&left.0, &left.2).cmp(&(&right.0, &right.2)));
    let start = *next_candidate_offset % candidates.len();
    candidates.rotate_left(start);
    let process_count = policy.max_edge_types_per_cycle.min(candidates.len());
    *next_candidate_offset = (start + process_count) % candidates.len();

    for (cell_id, shard, edge_type, dirty_epoch) in candidates.into_iter().take(process_count) {
        let observation_key = (cell_id.clone(), edge_type.clone());
        let observed_at = *first_observed_dirty
            .entry(observation_key.clone())
            .or_insert_with(Instant::now);
        let latest = match shard
            .latest_matrix_artifact(&cell_id, &edge_type, dirty_epoch)
            .await
        {
            Ok(latest) => latest,
            Err(err) => {
                report.artifact_failures = report.artifact_failures.saturating_add(1);
                tracing::warn!(
                    target: "slatedb_graph_kernel",
                    cell_id,
                    edge_type,
                    error = %err,
                    "failed to inspect matrix artifact freshness"
                );
                continue;
            }
        };

        if latest
            .as_ref()
            .is_some_and(|artifact| artifact.base_epoch >= dirty_epoch)
        {
            match shard
                .clear_matrix_dirty_marker(&cell_id, &edge_type, dirty_epoch)
                .await
            {
                Ok(_) => {
                    report.artifacts_current = report.artifacts_current.saturating_add(1);
                    first_observed_dirty.remove(&observation_key);
                }
                Err(err) => {
                    report.artifact_failures = report.artifact_failures.saturating_add(1);
                    tracing::warn!(
                        target: "slatedb_graph_kernel",
                        cell_id,
                        edge_type,
                        error = %err,
                        "failed to clear current matrix dirty marker"
                    );
                }
            }
            continue;
        }

        let artifact_epoch = latest.as_ref().map_or(0, |artifact| artifact.base_epoch);
        let epoch_lag = dirty_epoch.saturating_sub(artifact_epoch);
        let due = latest.is_none()
            || epoch_lag >= policy.min_epoch_lag
            || observed_at.elapsed() >= policy.max_dirty_age;
        if !due {
            report.artifacts_deferred = report.artifacts_deferred.saturating_add(1);
            continue;
        }

        let base_epoch = match shard.current_epoch(&cell_id).await {
            Ok(epoch) => epoch,
            Err(err) => {
                report.artifact_failures = report.artifact_failures.saturating_add(1);
                tracing::warn!(
                    target: "slatedb_graph_kernel",
                    cell_id,
                    edge_type,
                    error = %err,
                    "failed to read matrix refresh epoch"
                );
                continue;
            }
        };
        match shard
            .build_adjacency_image(&cell_id, &edge_type, base_epoch, policy.tile_size)
            .await
        {
            Ok(artifact) => {
                let _ = shard
                    .clear_matrix_dirty_marker(&cell_id, &edge_type, artifact.base_epoch)
                    .await?;
                first_observed_dirty.remove(&observation_key);
                report.artifacts_built = report.artifacts_built.saturating_add(1);
                tracing::info!(
                    target: "slatedb_graph_kernel",
                    cell_id,
                    edge_type,
                    base_epoch = artifact.base_epoch,
                    edge_count = artifact.edge_count,
                    "automatic matrix artifact refresh completed"
                );
            }
            Err(GraphError::SnapshotChanged { .. }) => {
                report.artifacts_deferred = report.artifacts_deferred.saturating_add(1);
            }
            Err(err) => {
                report.artifact_failures = report.artifact_failures.saturating_add(1);
                tracing::warn!(
                    target: "slatedb_graph_kernel",
                    cell_id,
                    edge_type,
                    base_epoch,
                    error = %err,
                    "automatic matrix artifact build failed"
                );
            }
        }
    }
    Ok(report)
}

impl GraphShard {
    async fn matrix_dirty_edge_types(
        &self,
        cell_id: &str,
    ) -> Result<Vec<(String, TopologySequence)>> {
        validate_component("cell_id", cell_id)?;
        let mut result = Vec::new();
        let prefix = crate::keys::matrix_dirty_prefix(cell_id);
        let mut iter = self.scan_remote_prefix(&prefix).await?;
        while let Some(kv) = iter.next().await? {
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            let edge_type = key
                .strip_prefix(&prefix)
                .filter(|value| !value.is_empty())
                .ok_or_else(|| GraphError::CorruptValue {
                    key: key.clone(),
                    reason: "invalid matrix dirty marker key".to_string(),
                })?
                .to_string();
            validate_component("edge_type", &edge_type)?;
            result.push((edge_type, decode_u64(&key, &kv.value)?));
        }
        result.sort_by(|left, right| left.0.cmp(&right.0));
        Ok(result)
    }

    async fn clear_matrix_dirty_marker(
        &self,
        cell_id: &str,
        edge_type: &str,
        built_epoch: TopologySequence,
    ) -> Result<bool> {
        let key = crate::keys::matrix_dirty(cell_id, edge_type);
        let Some(value) = self.read_remote(&key).await? else {
            return Ok(true);
        };
        if decode_u64(&key, &value)? > built_epoch {
            return Ok(false);
        }
        let mut batch = GraphWriteBatch::new();
        batch.delete(&key);
        match self
            .write_graph_batch_strict_guarded(
                cell_id,
                "clear_matrix_dirty_marker",
                vec![GraphWriteGuard::equals(&key, &value)],
                batch,
            )
            .await
        {
            Ok(()) => Ok(true),
            Err(GraphError::ConditionalWriteConflict { .. }) => Ok(false),
            Err(err) => Err(err),
        }
    }
}

fn invalid_refresh_policy(reason: &str) -> Result<()> {
    Err(GraphError::CorruptValue {
        key: "artifact/matrix_refresh_policy".to_string(),
        reason: reason.to_string(),
    })
}
