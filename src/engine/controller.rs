use super::*;

impl GraphClusterControllerConfig {
    pub fn new(
        cell_ids: impl IntoIterator<Item = impl Into<String>>,
        heartbeat_ttl: Duration,
        lease_ttl: Duration,
    ) -> Result<Self> {
        let mut normalized = BTreeSet::new();
        for cell_id in cell_ids {
            let cell_id = cell_id.into();
            validate_component("cell_id", &cell_id)?;
            normalized.insert(cell_id);
        }
        if normalized.is_empty() {
            return Err(GraphError::CorruptValue {
                key: "control/controller/cells".to_string(),
                reason: "cluster controller needs at least one cell".to_string(),
            });
        }
        validate_controller_duration("heartbeat_ttl", heartbeat_ttl)?;
        lease_ttl_ms(lease_ttl)?;
        Ok(Self {
            cell_ids: normalized.into_iter().collect(),
            heartbeat_ttl,
            lease_ttl,
            rebalance_mode: GraphClusterRebalanceMode::StabilityFirst,
            discover_existing_cells: true,
            max_expired_heartbeats_to_prune: GRAPH_CONTROLLER_EXPIRED_HEARTBEAT_PRUNE_LIMIT,
        })
    }

    pub fn discover_existing(heartbeat_ttl: Duration, lease_ttl: Duration) -> Result<Self> {
        validate_controller_duration("heartbeat_ttl", heartbeat_ttl)?;
        lease_ttl_ms(lease_ttl)?;
        Ok(Self {
            cell_ids: Vec::new(),
            heartbeat_ttl,
            lease_ttl,
            rebalance_mode: GraphClusterRebalanceMode::StabilityFirst,
            discover_existing_cells: true,
            max_expired_heartbeats_to_prune: GRAPH_CONTROLLER_EXPIRED_HEARTBEAT_PRUNE_LIMIT,
        })
    }

    pub fn with_rebalance_mode(mut self, rebalance_mode: GraphClusterRebalanceMode) -> Self {
        self.rebalance_mode = rebalance_mode;
        self
    }

    pub fn with_existing_cell_discovery(mut self, enabled: bool) -> Self {
        self.discover_existing_cells = enabled;
        self
    }

    pub fn with_expired_heartbeat_prune_limit(mut self, max_records: usize) -> Self {
        self.max_expired_heartbeats_to_prune = max_records;
        self
    }

    fn validated_configured_cells(&self) -> Result<Vec<String>> {
        let mut normalized = BTreeSet::new();
        for cell_id in &self.cell_ids {
            validate_component("cell_id", cell_id)?;
            normalized.insert(cell_id.clone());
        }
        if normalized.is_empty() && !self.discover_existing_cells {
            return Err(GraphError::CorruptValue {
                key: "control/controller/cells".to_string(),
                reason: "cluster controller needs at least one cell".to_string(),
            });
        }
        validate_controller_duration("heartbeat_ttl", self.heartbeat_ttl)?;
        lease_ttl_ms(self.lease_ttl)?;
        Ok(normalized.into_iter().collect())
    }
}

impl NodeHeartbeatHandle {
    pub fn set_state(&self, state: GraphNodeHealthState) -> Result<()> {
        self.state_tx
            .send(state)
            .map_err(|_| GraphError::CorruptValue {
                key: "control/node_heartbeat".to_string(),
                reason: "node heartbeat task has stopped".to_string(),
            })
    }

    pub async fn stop(self) -> Result<()> {
        let _ = self.stop_tx.send(true);
        match self.task.await {
            Ok(result) => result,
            Err(err) => Err(GraphError::CorruptValue {
                key: "control/node_heartbeat".to_string(),
                reason: format!("node heartbeat task failed: {err}"),
            }),
        }
    }
}

impl GraphClusterControllerHandle {
    pub async fn stop(self) -> Result<()> {
        let _ = self.stop_tx.send(true);
        match self.task.await {
            Ok(result) => result,
            Err(err) => Err(GraphError::CorruptValue {
                key: "control/cluster_controller".to_string(),
                reason: format!("cluster controller task failed: {err}"),
            }),
        }
    }
}

impl GraphControlPlane {
    pub async fn publish_node_heartbeat(
        &self,
        node_id: &str,
        state: GraphNodeHealthState,
    ) -> Result<GraphNodeHeartbeat> {
        self.publish_node_heartbeat_at(node_id, state, now_millis())
            .await
    }

    pub(crate) async fn publish_node_heartbeat_at(
        &self,
        node_id: &str,
        state: GraphNodeHealthState,
        now_ms: u64,
    ) -> Result<GraphNodeHeartbeat> {
        validate_component("node_id", node_id)?;
        for attempt in 0..GRAPH_CONTROL_TXN_MAX_RETRIES {
            match self
                .publish_node_heartbeat_txn(node_id, state, now_ms)
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

    async fn publish_node_heartbeat_txn(
        &self,
        node_id: &str,
        state: GraphNodeHealthState,
        now_ms: u64,
    ) -> Result<GraphNodeHeartbeat> {
        let txn = self.db.begin(IsolationLevel::SerializableSnapshot).await?;
        let key = control_node_key(node_id);
        let existing = read_control_txn(&txn, &key)
            .await?
            .map(|value| decode_node_heartbeat(&key, &value))
            .transpose()?;
        let generation = existing
            .as_ref()
            .map(|heartbeat| heartbeat.generation)
            .unwrap_or_default()
            .checked_add(1)
            .ok_or_else(|| GraphError::CorruptValue {
                key: key.clone(),
                reason: "node heartbeat generation overflow".to_string(),
            })?;
        let heartbeat = GraphNodeHeartbeat {
            node_id: node_id.to_string(),
            state,
            started_at_ms: existing
                .as_ref()
                .map(|heartbeat| heartbeat.started_at_ms)
                .unwrap_or(now_ms),
            last_seen_ms: now_ms,
            generation,
        };
        txn.put(key.as_bytes(), encode_node_heartbeat(&heartbeat))?;
        commit_control_txn(txn).await?;
        self.metrics
            .node_heartbeat_writes
            .fetch_add(1, Ordering::Relaxed);
        tracing::debug!(
            target: "slatedb_graph_kernel",
            node_id,
            state = encode_node_health_state(state),
            generation,
            last_seen_ms = now_ms,
            "published node heartbeat"
        );
        Ok(heartbeat)
    }

    pub async fn node_heartbeat(&self, node_id: &str) -> Result<Option<GraphNodeHeartbeat>> {
        validate_component("node_id", node_id)?;
        let key = control_node_key(node_id);
        self.db
            .get_with_options(key.as_bytes(), &control_read_options())
            .await?
            .map(|value| decode_node_heartbeat(&key, &value))
            .transpose()
    }

    pub async fn load_node_heartbeats(&self) -> Result<Vec<GraphNodeHeartbeat>> {
        let mut iter = self
            .db
            .scan_prefix_with_options(CONTROL_NODE_PREFIX.as_bytes(), .., &control_scan_options())
            .await?;
        let mut heartbeats = Vec::new();
        while let Some(kv) = iter.next().await? {
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            heartbeats.push(decode_node_heartbeat(&key, &kv.value)?);
        }
        heartbeats.sort_by(|left, right| left.node_id.cmp(&right.node_id));
        Ok(heartbeats)
    }

    pub(crate) async fn prune_expired_node_heartbeats(
        &self,
        expired_heartbeats: &[GraphNodeHeartbeat],
        max_records: usize,
    ) -> Result<Vec<String>> {
        let bounded: Vec<_> = expired_heartbeats
            .iter()
            .take(max_records)
            .cloned()
            .collect();
        if bounded.is_empty() {
            return Ok(Vec::new());
        }
        for attempt in 0..GRAPH_CONTROL_TXN_MAX_RETRIES {
            match self.prune_expired_node_heartbeats_txn(&bounded).await {
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

    async fn prune_expired_node_heartbeats_txn(
        &self,
        expired_heartbeats: &[GraphNodeHeartbeat],
    ) -> Result<Vec<String>> {
        let txn = self.db.begin(IsolationLevel::SerializableSnapshot).await?;
        let mut pruned = Vec::new();
        for expired in expired_heartbeats {
            validate_component("node_id", &expired.node_id)?;
            let key = control_node_key(&expired.node_id);
            let Some(current) = read_control_txn(&txn, &key)
                .await?
                .map(|value| decode_node_heartbeat(&key, &value))
                .transpose()?
            else {
                continue;
            };
            if current == *expired {
                txn.delete(key.as_bytes())?;
                pruned.push(expired.node_id.clone());
            }
        }
        commit_control_txn(txn).await?;
        if !pruned.is_empty() {
            self.metrics
                .node_heartbeat_prunes
                .fetch_add(pruned.len() as u64, Ordering::Relaxed);
            pruned.sort();
        }
        Ok(pruned)
    }

    pub async fn reconcile_cluster(
        &self,
        config: &GraphClusterControllerConfig,
    ) -> Result<GraphClusterControllerReport> {
        self.reconcile_cluster_at(config, now_millis()).await
    }

    pub(crate) async fn reconcile_cluster_at(
        &self,
        config: &GraphClusterControllerConfig,
        now_ms: u64,
    ) -> Result<GraphClusterControllerReport> {
        self.metrics.controller_runs.fetch_add(1, Ordering::Relaxed);
        let result = match self.acquire_controller_reconcile_lock().await {
            Ok(lock) => {
                let reconcile = self.reconcile_cluster_at_inner(config, now_ms, &lock).await;
                release_cell_write_lock(lock, reconcile).await
            }
            Err(err) => Err(err),
        };
        match &result {
            Ok(_) => {
                self.metrics
                    .controller_successes
                    .fetch_add(1, Ordering::Relaxed);
            }
            Err(_) => {
                self.metrics
                    .controller_failures
                    .fetch_add(1, Ordering::Relaxed);
            }
        }
        result
    }

    async fn reconcile_cluster_at_inner(
        &self,
        config: &GraphClusterControllerConfig,
        now_ms: u64,
        controller_lock: &CellWriteLock,
    ) -> Result<GraphClusterControllerReport> {
        config.validated_configured_cells()?;
        controller_lock.renew().await?;
        let heartbeat_ttl_ms = validate_controller_duration("heartbeat_ttl", config.heartbeat_ttl)?;
        let mut report = GraphClusterControllerReport {
            now_ms,
            ..GraphClusterControllerReport::default()
        };

        let heartbeats = self.load_node_heartbeats().await?;
        let mut expired_heartbeats = Vec::new();
        for heartbeat in heartbeats {
            let expired = match heartbeat.last_seen_ms.checked_add(heartbeat_ttl_ms) {
                Some(expires_at) => expires_at <= now_ms,
                None => true,
            };
            if expired {
                report.expired_nodes.push(heartbeat.node_id.clone());
                expired_heartbeats.push(heartbeat);
                continue;
            }
            match heartbeat.state {
                GraphNodeHealthState::Active => report.active_nodes.push(heartbeat.node_id),
                GraphNodeHealthState::Draining => report.draining_nodes.push(heartbeat.node_id),
            }
        }
        report.active_nodes.sort();
        report.active_nodes.dedup();
        report.draining_nodes.sort();
        report.draining_nodes.dedup();
        report.expired_nodes.sort();
        report.expired_nodes.dedup();
        if config.max_expired_heartbeats_to_prune > 0 && !expired_heartbeats.is_empty() {
            report.pruned_expired_nodes = self
                .prune_expired_node_heartbeats(
                    &expired_heartbeats,
                    config.max_expired_heartbeats_to_prune,
                )
                .await?;
        }
        controller_lock.renew().await?;

        let mut placement = self.load_placement_map().await?;
        let cell_ids = self.controller_cell_ids(config, &placement).await?;
        report.controlled_cells = cell_ids.clone();
        let active_nodes: BTreeSet<_> = report.active_nodes.iter().cloned().collect();
        let draining_nodes: BTreeSet<_> = report.draining_nodes.iter().cloned().collect();
        for cell_id in &cell_ids {
            controller_lock.renew().await?;
            let previous = placement.get(cell_id).cloned();
            let previous_is_usable = previous.as_ref().is_some_and(|owner| {
                active_nodes.contains(owner) && !draining_nodes.contains(owner)
            });
            let target_owner = if previous_is_usable
                && config.rebalance_mode == GraphClusterRebalanceMode::StabilityFirst
            {
                previous.clone()
            } else {
                choose_controller_owner(cell_id, &report.active_nodes)
            };
            let Some(target_owner) = target_owner else {
                report.unassigned_cells.push(cell_id.clone());
                continue;
            };
            let current_lease = self.current_lease(cell_id).await?;
            if let Some(lease) = current_lease
                .as_ref()
                .filter(|lease| lease.expires_at_ms > now_ms)
            {
                if lease.owner_node_id != target_owner {
                    report.pending_failovers.push(GraphPendingFailover {
                        cell_id: cell_id.clone(),
                        current_owner_node_id: lease.owner_node_id.clone(),
                        target_owner_node_id: target_owner,
                        lease_expires_at_ms: lease.expires_at_ms,
                    });
                    continue;
                }
                if previous.as_deref() == Some(target_owner.as_str()) {
                    continue;
                }
            }

            let (lease, lease_changed, committed_previous) = match self
                .assign_controller_cell_at(cell_id, &target_owner, config.lease_ttl, now_ms)
                .await
            {
                Ok(assignment) => assignment,
                Err(GraphError::ShardLeaseHeld {
                    owner_node_id,
                    expires_at_ms,
                    ..
                }) => {
                    report.pending_failovers.push(GraphPendingFailover {
                        cell_id: cell_id.clone(),
                        current_owner_node_id: owner_node_id,
                        target_owner_node_id: target_owner,
                        lease_expires_at_ms: expires_at_ms,
                    });
                    continue;
                }
                Err(err) => return Err(err),
            };
            if committed_previous.as_deref() != Some(target_owner.as_str()) {
                report.reassignments.push(GraphShardReassignment {
                    cell_id: cell_id.clone(),
                    previous_owner_node_id: committed_previous,
                    new_owner_node_id: target_owner.clone(),
                });
            }
            placement.insert(cell_id.clone(), target_owner);
            if lease_changed {
                report.failed_over_leases.push(lease);
            }
        }

        if !report.reassignments.is_empty() {
            self.metrics
                .controller_reassignments
                .fetch_add(report.reassignments.len() as u64, Ordering::Relaxed);
        }

        if !report.failed_over_leases.is_empty() {
            self.metrics
                .controller_failovers
                .fetch_add(report.failed_over_leases.len() as u64, Ordering::Relaxed);
        }
        if !report.pending_failovers.is_empty() {
            self.metrics
                .controller_pending_failovers
                .fetch_add(report.pending_failovers.len() as u64, Ordering::Relaxed);
        }
        report.unassigned_cells.sort();
        report.unassigned_cells.dedup();
        controller_lock.renew().await?;
        Ok(report)
    }

    async fn acquire_controller_reconcile_lock(&self) -> Result<CellWriteLock> {
        let db_path = if self.store_path.as_ref().is_empty() {
            "__root__"
        } else {
            self.store_path.as_ref()
        };
        let path = Path::from_iter([
            "__slatedb_graph_kernel",
            "controller_locks",
            db_path,
            "reconcile",
        ]);
        acquire_distributed_write_lock(
            Arc::clone(&self.object_store),
            path,
            "controller",
            "reconcile_cluster",
            GRAPH_CONTROLLER_LOCK_TTL_MS,
        )
        .await
    }

    pub async fn start_node_heartbeat(
        self: Arc<Self>,
        node_id: impl Into<String>,
        initial_state: GraphNodeHealthState,
        interval: Duration,
    ) -> Result<NodeHeartbeatHandle> {
        if interval.is_zero() {
            return Err(GraphError::CorruptValue {
                key: "control/node_heartbeat_interval".to_string(),
                reason: "node heartbeat interval must be greater than zero".to_string(),
            });
        }
        let node_id = node_id.into();
        validate_component("node_id", &node_id)?;
        self.publish_node_heartbeat(&node_id, initial_state).await?;
        let control = Arc::clone(&self);
        let (stop_tx, mut stop_rx) = watch::channel(false);
        let (state_tx, mut state_rx) = watch::channel(initial_state);
        let task = tokio::spawn(async move {
            loop {
                tokio::select! {
                    changed = stop_rx.changed() => {
                        if changed.is_err() || *stop_rx.borrow() {
                            break;
                        }
                    }
                    changed = state_rx.changed() => {
                        if changed.is_err() {
                            break;
                        }
                        let state = *state_rx.borrow();
                        publish_heartbeat_or_log(&control, &node_id, state).await;
                    }
                    _ = tokio::time::sleep(interval) => {
                        let state = *state_rx.borrow();
                        publish_heartbeat_or_log(&control, &node_id, state).await;
                    }
                }
            }
            Ok(())
        });
        Ok(NodeHeartbeatHandle {
            stop_tx,
            state_tx,
            task,
        })
    }

    pub fn start_cluster_controller(
        self: Arc<Self>,
        config: GraphClusterControllerConfig,
        interval: Duration,
    ) -> Result<GraphClusterControllerHandle> {
        if interval.is_zero() {
            return Err(GraphError::CorruptValue {
                key: "control/cluster_controller_interval".to_string(),
                reason: "cluster controller interval must be greater than zero".to_string(),
            });
        }
        config.validated_configured_cells()?;
        let control = Arc::clone(&self);
        let (stop_tx, mut stop_rx) = watch::channel(false);
        let task = tokio::spawn(async move {
            loop {
                match control.reconcile_cluster(&config).await {
                    Ok(report) => {
                        tracing::debug!(
                            target: "slatedb_graph_kernel",
                            active_nodes = report.active_nodes.len(),
                            draining_nodes = report.draining_nodes.len(),
                            expired_nodes = report.expired_nodes.len(),
                            reassignments = report.reassignments.len(),
                            failed_over_leases = report.failed_over_leases.len(),
                            pending_failovers = report.pending_failovers.len(),
                            unassigned_cells = report.unassigned_cells.len(),
                            "cluster controller reconciled"
                        );
                    }
                    Err(err) => {
                        tracing::warn!(
                            target: "slatedb_graph_kernel",
                            error = %err,
                            "cluster controller reconcile failed"
                        );
                        tokio::task::yield_now().await;
                    }
                }
                tokio::select! {
                    changed = stop_rx.changed() => {
                        if changed.is_err() || *stop_rx.borrow() {
                            break;
                        }
                    }
                    _ = tokio::time::sleep(interval) => {}
                }
            }
            Ok(())
        });
        Ok(GraphClusterControllerHandle { stop_tx, task })
    }

    async fn load_placement_map(&self) -> Result<BTreeMap<String, String>> {
        let mut iter = self
            .db
            .scan_prefix_with_options(
                CONTROL_PLACEMENT_PREFIX.as_bytes(),
                ..,
                &control_scan_options(),
            )
            .await?;
        let mut placement = BTreeMap::new();
        while let Some(kv) = iter.next().await? {
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            let (cell_id, node_id) = decode_control_placement(&key, &kv.value)?;
            placement.insert(cell_id, node_id);
        }
        Ok(placement)
    }

    async fn controller_cell_ids(
        &self,
        config: &GraphClusterControllerConfig,
        placement: &BTreeMap<String, String>,
    ) -> Result<Vec<String>> {
        let mut cells = BTreeSet::new();
        for cell_id in config.validated_configured_cells()? {
            cells.insert(cell_id);
        }
        if config.discover_existing_cells {
            cells.extend(placement.keys().cloned());
            for entry in self.list_shard_metadata().await? {
                cells.insert(entry.cell_id);
            }
        }
        Ok(cells.into_iter().collect())
    }
}

fn choose_controller_owner(cell_id: &str, active_nodes: &[String]) -> Option<String> {
    active_nodes
        .iter()
        .max_by_key(|node_id| rendezvous_score(cell_id, node_id))
        .cloned()
}

fn validate_controller_duration(field: &'static str, duration: Duration) -> Result<u64> {
    if duration.is_zero() {
        return Err(GraphError::CorruptValue {
            key: format!("control/controller/{field}"),
            reason: format!("{field} must be greater than zero"),
        });
    }
    u64::try_from(duration.as_millis()).map_err(|err| GraphError::CorruptValue {
        key: format!("control/controller/{field}"),
        reason: format!("{field} is too large: {err}"),
    })
}

async fn publish_heartbeat_or_log(
    control: &GraphControlPlane,
    node_id: &str,
    state: GraphNodeHealthState,
) {
    if let Err(err) = control.publish_node_heartbeat(node_id, state).await {
        tracing::warn!(
            target: "slatedb_graph_kernel",
            node_id,
            state = encode_node_health_state(state),
            error = %err,
            "node heartbeat publish failed"
        );
        tokio::task::yield_now().await;
    }
}
