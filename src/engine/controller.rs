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
        config.validated_configured_cells()?;
        let heartbeat_ttl_ms = validate_controller_duration("heartbeat_ttl", config.heartbeat_ttl)?;
        let mut report = GraphClusterControllerReport {
            now_ms,
            ..GraphClusterControllerReport::default()
        };

        let heartbeats = self.load_node_heartbeats().await?;
        for heartbeat in heartbeats {
            let expired = match heartbeat.last_seen_ms.checked_add(heartbeat_ttl_ms) {
                Some(expires_at) => expires_at <= now_ms,
                None => true,
            };
            if expired {
                report.expired_nodes.push(heartbeat.node_id);
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

        let mut placement = self.load_placement_map().await?;
        let cell_ids = self.controller_cell_ids(config, &placement).await?;
        report.controlled_cells = cell_ids.clone();
        let active_nodes: BTreeSet<_> = report.active_nodes.iter().cloned().collect();
        let draining_nodes: BTreeSet<_> = report.draining_nodes.iter().cloned().collect();
        for cell_id in &cell_ids {
            let previous = placement.get(cell_id).cloned();
            let previous_is_usable = match previous.as_ref() {
                Some(owner) => active_nodes.contains(owner) && !draining_nodes.contains(owner),
                None => false,
            };
            let Some(new_owner) = choose_controller_owner(cell_id, &report.active_nodes) else {
                report.unassigned_cells.push(cell_id.clone());
                continue;
            };
            if previous_is_usable
                && (config.rebalance_mode == GraphClusterRebalanceMode::StabilityFirst
                    || previous.as_deref() == Some(new_owner.as_str()))
            {
                continue;
            }
            if previous.as_deref() == Some(new_owner.as_str()) {
                continue;
            }
            placement.insert(cell_id.clone(), new_owner.clone());
            report.reassignments.push(GraphShardReassignment {
                cell_id: cell_id.clone(),
                previous_owner_node_id: previous,
                new_owner_node_id: new_owner,
            });
        }

        if !report.reassignments.is_empty() {
            self.publish_controller_reassignments(&report.reassignments)
                .await?;
            self.metrics
                .controller_reassignments
                .fetch_add(report.reassignments.len() as u64, Ordering::Relaxed);
        }

        for cell_id in &cell_ids {
            let Some(target_owner) = placement.get(cell_id).cloned() else {
                continue;
            };
            if !active_nodes.contains(&target_owner) {
                continue;
            }
            match self.current_lease(cell_id).await? {
                Some(lease)
                    if lease.owner_node_id == target_owner && lease.expires_at_ms > now_ms => {}
                Some(lease)
                    if lease.owner_node_id != target_owner && lease.expires_at_ms > now_ms =>
                {
                    report.pending_failovers.push(GraphPendingFailover {
                        cell_id: cell_id.clone(),
                        current_owner_node_id: lease.owner_node_id,
                        target_owner_node_id: target_owner,
                        lease_expires_at_ms: lease.expires_at_ms,
                    });
                }
                Some(_) | None => {
                    let lease = self
                        .failover_expired_cell_at(cell_id, &target_owner, config.lease_ttl, now_ms)
                        .await?;
                    report.failed_over_leases.push(lease);
                }
            }
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
        Ok(report)
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

    async fn publish_controller_reassignments(
        &self,
        reassignments: &[GraphShardReassignment],
    ) -> Result<()> {
        for attempt in 0..GRAPH_CONTROL_TXN_MAX_RETRIES {
            match self
                .publish_controller_reassignments_txn(reassignments)
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

    async fn publish_controller_reassignments_txn(
        &self,
        reassignments: &[GraphShardReassignment],
    ) -> Result<()> {
        let txn = self.db.begin(IsolationLevel::SerializableSnapshot).await?;
        for reassignment in reassignments {
            validate_component("cell_id", &reassignment.cell_id)?;
            validate_component("node_id", &reassignment.new_owner_node_id)?;
            let placement_key = control_placement_key(&reassignment.cell_id);
            let current = read_control_txn(&txn, &placement_key)
                .await?
                .map(|value| decode_control_placement(&placement_key, &value))
                .transpose()?
                .map(|(_, owner)| owner);
            if current != reassignment.previous_owner_node_id {
                return Err(GraphError::ControlMetadataConflict {
                    key: placement_key,
                    expected_generation: None,
                    actual_generation: None,
                });
            }
            txn.put(
                control_placement_key(&reassignment.cell_id).as_bytes(),
                encode_control_placement(&reassignment.cell_id, &reassignment.new_owner_node_id),
            )?;
        }
        commit_control_txn(txn).await
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
