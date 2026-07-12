use super::*;

impl GraphControlPlane {
    pub async fn open(
        path: impl Into<slatedb::object_store::path::Path>,
        object_store: Arc<dyn ObjectStore>,
    ) -> Result<Self> {
        Self::open_with_cache(path, object_store, GraphCacheConfig::default()).await
    }

    pub async fn open_with_cache(
        path: impl Into<slatedb::object_store::path::Path>,
        object_store: Arc<dyn ObjectStore>,
        cache: GraphCacheConfig,
    ) -> Result<Self> {
        Self::open_scoped_with_cache(path, object_store, GraphScope::default(), cache).await
    }

    pub async fn open_scoped(
        base_path: impl Into<slatedb::object_store::path::Path>,
        object_store: Arc<dyn ObjectStore>,
        scope: GraphScope,
    ) -> Result<Self> {
        Self::open_scoped_with_cache(base_path, object_store, scope, GraphCacheConfig::default())
            .await
    }

    pub async fn open_scoped_with_cache(
        base_path: impl Into<slatedb::object_store::path::Path>,
        object_store: Arc<dyn ObjectStore>,
        scope: GraphScope,
        cache: GraphCacheConfig,
    ) -> Result<Self> {
        let base_path = base_path.into();
        let store_path =
            slatedb::object_store::path::Path::from(scope.scoped_store_path(base_path.as_ref()));
        Self::open_at_path(store_path, object_store, cache, scope).await
    }

    async fn open_at_path(
        store_path: slatedb::object_store::path::Path,
        object_store: Arc<dyn ObjectStore>,
        cache: GraphCacheConfig,
        scope: GraphScope,
    ) -> Result<Self> {
        Ok(Self {
            db: open_graph_db(
                store_path.clone(),
                Arc::clone(&object_store),
                &cache,
                &GraphDurabilityConfig::default(),
            )
            .await?,
            object_store,
            store_path,
            scope,
            metrics: Arc::new(GraphControlMetrics::default()),
        })
    }

    pub fn scope(&self) -> &GraphScope {
        &self.scope
    }

    pub fn graph_control_metrics(&self) -> GraphControlMetricsSnapshot {
        self.metrics.snapshot()
    }

    pub async fn close(&self) -> Result<()> {
        self.db.close().await?;
        Ok(())
    }

    pub async fn publish_placement(&self, placement: &ShardPlacement) -> Result<()> {
        self.publish_scoped_placement_with_catalog(placement, 0)
            .await
            .map(|_| ())
    }

    pub async fn rebalance_rendezvous(
        &self,
        cell_ids: impl IntoIterator<Item = impl Into<String>>,
        node_ids: impl IntoIterator<Item = impl Into<String>>,
    ) -> Result<ShardPlacement> {
        let placement = ShardPlacement::rendezvous(cell_ids, node_ids)?;
        self.publish_placement(&placement).await?;
        Ok(placement)
    }

    pub async fn load_placement(&self) -> Result<ShardPlacement> {
        let mut iter = self
            .db
            .scan_prefix_with_options(
                CONTROL_PLACEMENT_PREFIX.as_bytes(),
                ..,
                &control_scan_options(),
            )
            .await?;
        let mut assignments = Vec::new();
        while let Some(kv) = iter.next().await? {
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            assignments.push(decode_control_placement(&key, &kv.value)?);
        }
        ShardPlacement::fixed(assignments)
    }

    pub async fn acquire_lease(
        &self,
        cell_id: &str,
        node_id: &str,
        ttl: Duration,
    ) -> Result<ShardLease> {
        self.acquire_lease_at(cell_id, node_id, ttl, now_millis())
            .await
    }

    pub async fn renew_lease(&self, lease: &ShardLease, ttl: Duration) -> Result<ShardLease> {
        self.renew_lease_at(lease, ttl, now_millis()).await
    }

    pub async fn release_lease(&self, lease: &ShardLease) -> Result<bool> {
        validate_component("cell_id", &lease.cell_id)?;
        validate_component("node_id", &lease.owner_node_id)?;
        self.metrics
            .lease_release_attempts
            .fetch_add(1, Ordering::Relaxed);
        for attempt in 0..GRAPH_CONTROL_TXN_MAX_RETRIES {
            match self.release_lease_txn(lease).await {
                Err(GraphError::Slate(err))
                    if err.kind() == ErrorKind::Transaction
                        && attempt + 1 < GRAPH_CONTROL_TXN_MAX_RETRIES =>
                {
                    tokio::task::yield_now().await;
                }
                Ok(released) => {
                    self.metrics
                        .lease_release_successes
                        .fetch_add(if released { 1 } else { 0 }, Ordering::Relaxed);
                    return Ok(released);
                }
                Err(err) => {
                    self.metrics
                        .lease_release_failures
                        .fetch_add(1, Ordering::Relaxed);
                    return Err(err);
                }
            }
        }
        Err(GraphError::RetryExhausted {
            operation: "control transaction",
            attempts: GRAPH_CONTROL_TXN_MAX_RETRIES,
        })
    }

    pub(crate) async fn acquire_lease_at(
        &self,
        cell_id: &str,
        node_id: &str,
        ttl: Duration,
        now_ms: u64,
    ) -> Result<ShardLease> {
        validate_component("cell_id", cell_id)?;
        validate_component("node_id", node_id)?;
        self.metrics
            .lease_acquire_attempts
            .fetch_add(1, Ordering::Relaxed);
        let ttl_ms = match lease_ttl_ms(ttl) {
            Ok(ttl_ms) => ttl_ms,
            Err(err) => {
                self.metrics
                    .lease_acquire_failures
                    .fetch_add(1, Ordering::Relaxed);
                return Err(err);
            }
        };
        for attempt in 0..GRAPH_CONTROL_TXN_MAX_RETRIES {
            match self
                .acquire_lease_txn(cell_id, node_id, ttl_ms, now_ms)
                .await
            {
                Err(GraphError::Slate(err))
                    if err.kind() == ErrorKind::Transaction
                        && attempt + 1 < GRAPH_CONTROL_TXN_MAX_RETRIES =>
                {
                    tokio::task::yield_now().await;
                }
                Ok(lease) => {
                    self.metrics
                        .lease_acquire_successes
                        .fetch_add(1, Ordering::Relaxed);
                    return Ok(lease);
                }
                Err(err) => {
                    self.metrics
                        .lease_acquire_failures
                        .fetch_add(1, Ordering::Relaxed);
                    return Err(err);
                }
            }
        }
        Err(GraphError::RetryExhausted {
            operation: "control transaction",
            attempts: GRAPH_CONTROL_TXN_MAX_RETRIES,
        })
    }

    async fn acquire_lease_txn(
        &self,
        cell_id: &str,
        node_id: &str,
        ttl_ms: u64,
        now_ms: u64,
    ) -> Result<ShardLease> {
        let txn = self.db.begin(IsolationLevel::SerializableSnapshot).await?;
        let placement_key = control_placement_key(cell_id);
        let owner = read_control_txn(&txn, &placement_key)
            .await?
            .map(|value| decode_control_placement(&placement_key, &value))
            .transpose()?
            .map(|(_, owner)| owner)
            .ok_or_else(|| GraphError::UnknownShard {
                cell_id: cell_id.to_string(),
            })?;
        if owner != node_id {
            return Err(GraphError::ShardNotOwned {
                cell_id: cell_id.to_string(),
                owner_node_id: owner,
                local_node_id: node_id.to_string(),
            });
        }

        let lease_key = control_lease_key(cell_id);
        if let Some(value) = read_control_txn(&txn, &lease_key).await? {
            let current = decode_shard_lease(&lease_key, &value)?;
            if current.owner_node_id != node_id && current.expires_at_ms > now_ms {
                return Err(GraphError::ShardLeaseHeld {
                    cell_id: cell_id.to_string(),
                    owner_node_id: current.owner_node_id,
                    expires_at_ms: current.expires_at_ms,
                });
            }
        }

        let token_key = control_lease_token_key(cell_id);
        let token = read_control_counter_txn(&txn, &token_key)
            .await?
            .checked_add(1)
            .ok_or_else(|| GraphError::CorruptValue {
                key: token_key.clone(),
                reason: "lease token overflow".to_string(),
            })?;
        let lease = ShardLease {
            cell_id: cell_id.to_string(),
            owner_node_id: node_id.to_string(),
            lease_token: token,
            expires_at_ms: now_ms
                .checked_add(ttl_ms)
                .ok_or_else(|| GraphError::CorruptValue {
                    key: lease_key.clone(),
                    reason: "lease expiry overflow".to_string(),
                })?,
        };
        txn.put(token_key.as_bytes(), encode_u64_be(token))?;
        txn.put(lease_key.as_bytes(), encode_shard_lease(&lease))?;
        control_metadata::bump_catalog_lease_txn(
            &txn,
            self.scope.graph_id.as_str(),
            cell_id,
            node_id,
            token,
        )
        .await?;
        commit_control_txn(txn).await?;
        tracing::info!(
            target: "slatedb_graph_kernel",
            cell_id,
            node_id,
            lease_token = lease.lease_token,
            expires_at_ms = lease.expires_at_ms,
            "acquired shard lease"
        );
        Ok(lease)
    }

    async fn renew_lease_at(
        &self,
        lease: &ShardLease,
        ttl: Duration,
        now_ms: u64,
    ) -> Result<ShardLease> {
        self.metrics
            .lease_renew_attempts
            .fetch_add(1, Ordering::Relaxed);
        let ttl_ms = match lease_ttl_ms(ttl) {
            Ok(ttl_ms) => ttl_ms,
            Err(err) => {
                self.metrics
                    .lease_renew_failures
                    .fetch_add(1, Ordering::Relaxed);
                return Err(err);
            }
        };
        for attempt in 0..GRAPH_CONTROL_TXN_MAX_RETRIES {
            match self.renew_lease_txn(lease, ttl_ms, now_ms).await {
                Err(GraphError::Slate(err))
                    if err.kind() == ErrorKind::Transaction
                        && attempt + 1 < GRAPH_CONTROL_TXN_MAX_RETRIES =>
                {
                    self.metrics
                        .lease_renew_retries
                        .fetch_add(1, Ordering::Relaxed);
                    tokio::task::yield_now().await;
                }
                Ok(renewed) => {
                    self.metrics
                        .lease_renew_successes
                        .fetch_add(1, Ordering::Relaxed);
                    return Ok(renewed);
                }
                Err(err @ GraphError::StaleShardLease { .. }) => {
                    self.metrics
                        .lease_renew_failures
                        .fetch_add(1, Ordering::Relaxed);
                    self.metrics
                        .lease_renew_lost
                        .fetch_add(1, Ordering::Relaxed);
                    return Err(err);
                }
                Err(err) => {
                    self.metrics
                        .lease_renew_failures
                        .fetch_add(1, Ordering::Relaxed);
                    return Err(err);
                }
            }
        }
        Err(GraphError::RetryExhausted {
            operation: "control transaction",
            attempts: GRAPH_CONTROL_TXN_MAX_RETRIES,
        })
    }

    async fn renew_lease_txn(
        &self,
        lease: &ShardLease,
        ttl_ms: u64,
        now_ms: u64,
    ) -> Result<ShardLease> {
        let txn = self.db.begin(IsolationLevel::SerializableSnapshot).await?;
        let placement_key = control_placement_key(&lease.cell_id);
        let owner = read_control_txn(&txn, &placement_key)
            .await?
            .map(|value| decode_control_placement(&placement_key, &value))
            .transpose()?
            .map(|(_, owner)| owner)
            .ok_or_else(|| GraphError::UnknownShard {
                cell_id: lease.cell_id.clone(),
            })?;
        if owner != lease.owner_node_id {
            return Err(GraphError::StaleShardLease {
                cell_id: lease.cell_id.clone(),
                node_id: lease.owner_node_id.clone(),
                lease_token: lease.lease_token,
            });
        }
        let lease_key = control_lease_key(&lease.cell_id);
        let Some(value) = read_control_txn(&txn, &lease_key).await? else {
            return Err(GraphError::StaleShardLease {
                cell_id: lease.cell_id.clone(),
                node_id: lease.owner_node_id.clone(),
                lease_token: lease.lease_token,
            });
        };
        let current = decode_shard_lease(&lease_key, &value)?;
        if current.owner_node_id != lease.owner_node_id
            || current.lease_token != lease.lease_token
            || current.expires_at_ms <= now_ms
        {
            return Err(GraphError::StaleShardLease {
                cell_id: lease.cell_id.clone(),
                node_id: lease.owner_node_id.clone(),
                lease_token: lease.lease_token,
            });
        }

        let renewed = ShardLease {
            expires_at_ms: now_ms
                .checked_add(ttl_ms)
                .ok_or_else(|| GraphError::CorruptValue {
                    key: lease_key.clone(),
                    reason: "lease expiry overflow".to_string(),
                })?,
            ..lease.clone()
        };
        txn.put(lease_key.as_bytes(), encode_shard_lease(&renewed))?;
        commit_control_txn(txn).await?;
        tracing::debug!(
            target: "slatedb_graph_kernel",
            cell_id = %renewed.cell_id,
            node_id = %renewed.owner_node_id,
            lease_token = renewed.lease_token,
            expires_at_ms = renewed.expires_at_ms,
            "renewed shard lease"
        );
        Ok(renewed)
    }

    async fn release_lease_txn(&self, lease: &ShardLease) -> Result<bool> {
        let txn = self.db.begin(IsolationLevel::SerializableSnapshot).await?;
        let lease_key = control_lease_key(&lease.cell_id);
        let Some(value) = read_control_txn(&txn, &lease_key).await? else {
            return Ok(false);
        };
        let current = decode_shard_lease(&lease_key, &value)?;
        if current.owner_node_id != lease.owner_node_id || current.lease_token != lease.lease_token
        {
            return Err(GraphError::StaleShardLease {
                cell_id: lease.cell_id.clone(),
                node_id: lease.owner_node_id.clone(),
                lease_token: lease.lease_token,
            });
        }
        txn.delete(lease_key.as_bytes())?;
        commit_control_txn(txn).await?;
        tracing::info!(
            target: "slatedb_graph_kernel",
            cell_id = %lease.cell_id,
            node_id = %lease.owner_node_id,
            lease_token = lease.lease_token,
            "released shard lease"
        );
        Ok(true)
    }

    pub async fn current_lease(&self, cell_id: &str) -> Result<Option<ShardLease>> {
        validate_component("cell_id", cell_id)?;
        let key = control_lease_key(cell_id);
        self.db
            .get_with_options(key.as_bytes(), &control_read_options())
            .await?
            .map(|value| decode_shard_lease(&key, &value))
            .transpose()
    }

    pub async fn failover_expired_cell(
        &self,
        cell_id: &str,
        new_node_id: &str,
        ttl: Duration,
    ) -> Result<ShardLease> {
        self.failover_expired_cell_at(cell_id, new_node_id, ttl, now_millis())
            .await
    }

    pub(crate) async fn failover_expired_cell_at(
        &self,
        cell_id: &str,
        new_node_id: &str,
        ttl: Duration,
        now_ms: u64,
    ) -> Result<ShardLease> {
        validate_component("cell_id", cell_id)?;
        validate_component("node_id", new_node_id)?;
        let ttl_ms = lease_ttl_ms(ttl)?;
        for attempt in 0..GRAPH_CONTROL_TXN_MAX_RETRIES {
            match self
                .failover_expired_cell_txn(cell_id, new_node_id, ttl_ms, now_ms)
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

    pub(crate) async fn assign_controller_cell_at(
        &self,
        cell_id: &str,
        new_node_id: &str,
        ttl: Duration,
        now_ms: u64,
    ) -> Result<(ShardLease, bool, Option<String>)> {
        validate_component("cell_id", cell_id)?;
        validate_component("node_id", new_node_id)?;
        let ttl_ms = lease_ttl_ms(ttl)?;
        for attempt in 0..GRAPH_CONTROL_TXN_MAX_RETRIES {
            match self
                .assign_controller_cell_txn(cell_id, new_node_id, ttl_ms, now_ms)
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

    async fn assign_controller_cell_txn(
        &self,
        cell_id: &str,
        new_node_id: &str,
        ttl_ms: u64,
        now_ms: u64,
    ) -> Result<(ShardLease, bool, Option<String>)> {
        let txn = self.db.begin(IsolationLevel::SerializableSnapshot).await?;
        let placement_key = control_placement_key(cell_id);
        let current_placement = read_control_txn(&txn, &placement_key)
            .await?
            .map(|value| decode_control_placement(&placement_key, &value))
            .transpose()?
            .map(|(_, owner)| owner);
        let lease_key = control_lease_key(cell_id);
        let current_lease = read_control_txn(&txn, &lease_key)
            .await?
            .map(|value| decode_shard_lease(&lease_key, &value))
            .transpose()?;

        if let Some(current) = current_lease
            .as_ref()
            .filter(|lease| lease.expires_at_ms > now_ms)
        {
            if current.owner_node_id != new_node_id {
                return Err(GraphError::ShardLeaseHeld {
                    cell_id: cell_id.to_string(),
                    owner_node_id: current.owner_node_id.clone(),
                    expires_at_ms: current.expires_at_ms,
                });
            }
            if current_placement.as_deref() != Some(new_node_id) {
                txn.put(
                    placement_key.as_bytes(),
                    encode_control_placement(cell_id, new_node_id),
                )?;
                control_metadata::bump_catalog_lease_txn(
                    &txn,
                    self.scope.graph_id.as_str(),
                    cell_id,
                    new_node_id,
                    current.lease_token,
                )
                .await?;
                commit_control_txn(txn).await?;
            }
            return Ok((current.clone(), false, current_placement));
        }

        let token_key = control_lease_token_key(cell_id);
        let token = read_control_counter_txn(&txn, &token_key)
            .await?
            .checked_add(1)
            .ok_or_else(|| GraphError::CorruptValue {
                key: token_key.clone(),
                reason: "lease token overflow".to_string(),
            })?;
        let lease = ShardLease {
            cell_id: cell_id.to_string(),
            owner_node_id: new_node_id.to_string(),
            lease_token: token,
            expires_at_ms: now_ms
                .checked_add(ttl_ms)
                .ok_or_else(|| GraphError::CorruptValue {
                    key: lease_key.clone(),
                    reason: "lease expiry overflow".to_string(),
                })?,
        };
        txn.put(
            placement_key.as_bytes(),
            encode_control_placement(cell_id, new_node_id),
        )?;
        txn.put(token_key.as_bytes(), encode_u64_be(token))?;
        txn.put(lease_key.as_bytes(), encode_shard_lease(&lease))?;
        control_metadata::bump_catalog_lease_txn(
            &txn,
            self.scope.graph_id.as_str(),
            cell_id,
            new_node_id,
            token,
        )
        .await?;
        commit_control_txn(txn).await?;
        tracing::warn!(
            target: "slatedb_graph_kernel",
            cell_id,
            new_node_id,
            lease_token = lease.lease_token,
            expires_at_ms = lease.expires_at_ms,
            "atomically assigned shard placement and lease"
        );
        Ok((lease, true, current_placement))
    }

    async fn failover_expired_cell_txn(
        &self,
        cell_id: &str,
        new_node_id: &str,
        ttl_ms: u64,
        now_ms: u64,
    ) -> Result<ShardLease> {
        let txn = self.db.begin(IsolationLevel::SerializableSnapshot).await?;
        let placement_key = control_placement_key(cell_id);
        let Some(existing_placement) = read_control_txn(&txn, &placement_key).await? else {
            return Err(GraphError::UnknownShard {
                cell_id: cell_id.to_string(),
            });
        };
        let (placed_cell_id, _) = decode_control_placement(&placement_key, &existing_placement)?;
        if placed_cell_id != cell_id {
            return Err(GraphError::UnknownShard {
                cell_id: cell_id.to_string(),
            });
        }

        let lease_key = control_lease_key(cell_id);
        if let Some(value) = read_control_txn(&txn, &lease_key).await? {
            let current = decode_shard_lease(&lease_key, &value)?;
            if current.owner_node_id != new_node_id && current.expires_at_ms > now_ms {
                return Err(GraphError::ShardLeaseHeld {
                    cell_id: cell_id.to_string(),
                    owner_node_id: current.owner_node_id,
                    expires_at_ms: current.expires_at_ms,
                });
            }
        }

        let token_key = control_lease_token_key(cell_id);
        let token = read_control_counter_txn(&txn, &token_key)
            .await?
            .checked_add(1)
            .ok_or_else(|| GraphError::CorruptValue {
                key: token_key.clone(),
                reason: "lease token overflow".to_string(),
            })?;
        let lease = ShardLease {
            cell_id: cell_id.to_string(),
            owner_node_id: new_node_id.to_string(),
            lease_token: token,
            expires_at_ms: now_ms
                .checked_add(ttl_ms)
                .ok_or_else(|| GraphError::CorruptValue {
                    key: lease_key.clone(),
                    reason: "lease expiry overflow".to_string(),
                })?,
        };
        txn.put(
            placement_key.as_bytes(),
            encode_control_placement(cell_id, new_node_id),
        )?;
        txn.put(token_key.as_bytes(), encode_u64_be(token))?;
        txn.put(lease_key.as_bytes(), encode_shard_lease(&lease))?;
        control_metadata::bump_catalog_lease_txn(
            &txn,
            self.scope.graph_id.as_str(),
            cell_id,
            new_node_id,
            token,
        )
        .await?;
        commit_control_txn(txn).await?;
        tracing::warn!(
            target: "slatedb_graph_kernel",
            cell_id,
            new_node_id,
            lease_token = lease.lease_token,
            expires_at_ms = lease.expires_at_ms,
            "failed over expired shard lease"
        );
        Ok(lease)
    }
}
