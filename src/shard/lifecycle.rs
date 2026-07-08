use super::*;

impl GraphShard {
    pub async fn open(path: impl Into<Path>, object_store: Arc<dyn ObjectStore>) -> Result<Self> {
        Self::open_with_options(path, object_store, GraphOpenOptions::default()).await
    }

    pub async fn open_with_limits(
        path: impl Into<Path>,
        object_store: Arc<dyn ObjectStore>,
        limits: GraphLimits,
    ) -> Result<Self> {
        Self::open_with_options(
            path,
            object_store,
            GraphOpenOptions {
                limits,
                cache: GraphCacheConfig::default(),
                durability: GraphDurabilityConfig::default(),
                cache_policy: GraphCachePolicy::default(),
                retention_policy: GraphRetentionPolicy::default(),
                backpressure_policy: GraphBackpressurePolicy::default(),
                index_policy: GraphIndexPolicy::default(),
            },
        )
        .await
    }

    pub async fn open_with_options(
        path: impl Into<Path>,
        object_store: Arc<dyn ObjectStore>,
        options: GraphOpenOptions,
    ) -> Result<Self> {
        Self::open_internal(path, object_store, options, GraphWriteAuthority::ReadOnly).await
    }

    pub async fn open_standalone_writer(
        path: impl Into<Path>,
        object_store: Arc<dyn ObjectStore>,
    ) -> Result<Self> {
        Self::open_standalone_writer_with_options(path, object_store, GraphOpenOptions::default())
            .await
    }

    pub async fn open_standalone_writer_with_limits(
        path: impl Into<Path>,
        object_store: Arc<dyn ObjectStore>,
        limits: GraphLimits,
    ) -> Result<Self> {
        Self::open_standalone_writer_with_options(
            path,
            object_store,
            GraphOpenOptions {
                limits,
                cache: GraphCacheConfig::default(),
                durability: GraphDurabilityConfig::default(),
                cache_policy: GraphCachePolicy::default(),
                retention_policy: GraphRetentionPolicy::default(),
                backpressure_policy: GraphBackpressurePolicy::default(),
                index_policy: GraphIndexPolicy::default(),
            },
        )
        .await
    }

    pub async fn open_standalone_writer_with_options(
        path: impl Into<Path>,
        object_store: Arc<dyn ObjectStore>,
        options: GraphOpenOptions,
    ) -> Result<Self> {
        Self::open_internal(path, object_store, options, GraphWriteAuthority::Standalone).await
    }

    pub(crate) async fn open_leased_writer(
        path: impl Into<Path>,
        object_store: Arc<dyn ObjectStore>,
        options: GraphOpenOptions,
        local_node_id: String,
        leases: Arc<RwLock<BTreeMap<String, engine::ShardLease>>>,
    ) -> Result<Self> {
        Self::open_internal(
            path,
            object_store,
            options,
            GraphWriteAuthority::Leased {
                local_node_id,
                leases,
            },
        )
        .await
    }

    #[cfg(feature = "chaos-harness")]
    pub async fn open_chaos_leased_writer_with_options(
        path: impl Into<Path>,
        object_store: Arc<dyn ObjectStore>,
        options: GraphOpenOptions,
        local_node_id: impl Into<String>,
        lease: engine::ShardLease,
    ) -> Result<Self> {
        let local_node_id = local_node_id.into();
        validate_component("node_id", &local_node_id)?;
        if lease.owner_node_id != local_node_id {
            return Err(GraphError::StaleShardLease {
                cell_id: lease.cell_id.clone(),
                node_id: local_node_id,
                lease_token: lease.lease_token,
            });
        }
        let leases = Arc::new(RwLock::new(BTreeMap::from([(
            lease.cell_id.clone(),
            lease,
        )])));
        Self::open_leased_writer(path, object_store, options, local_node_id, leases).await
    }

    #[cfg(feature = "chaos-harness")]
    pub async fn open_chaos_leased_writer(
        path: impl Into<Path>,
        object_store: Arc<dyn ObjectStore>,
        local_node_id: impl Into<String>,
        lease: engine::ShardLease,
    ) -> Result<Self> {
        Self::open_chaos_leased_writer_with_options(
            path,
            object_store,
            GraphOpenOptions::default(),
            local_node_id,
            lease,
        )
        .await
    }

    async fn open_internal(
        path: impl Into<Path>,
        object_store: Arc<dyn ObjectStore>,
        options: GraphOpenOptions,
        write_authority: GraphWriteAuthority,
    ) -> Result<Self> {
        if !options.durability.await_durable_writes
            && !matches!(&write_authority, GraphWriteAuthority::ReadOnly)
        {
            return Err(GraphError::UnsafeDurabilityConfig {
                operation: "open_write_authoritative_shard",
                reason: "graph writers allocate epochs from remote-visible metadata; relaxed durability can release cross-process fences before last_epoch, degree counters, and idempotency keys are visible".to_string(),
            });
        }

        let store_path = path.into();
        let db = open_graph_db(
            store_path.clone(),
            Arc::clone(&object_store),
            &options.cache,
            &options.durability,
        )
        .await?;
        ensure_store_format(&db, &write_authority).await?;
        let cache_policy = options.cache_policy;
        let backpressure_policy = options.backpressure_policy;
        let tenant_quota = cache_policy.max_entries_per_cell;
        let cache_metrics = Arc::new(GraphCacheMetrics::default());
        let operation_metrics = Arc::new(GraphOperationalMetrics::default());
        let hydration_gate = Arc::new(Semaphore::new(cache_policy.hydration_permits()));
        let graph_write_gate = Arc::new(Semaphore::new(
            backpressure_policy.max_concurrent_graph_writes.max(1),
        ));
        let artifact_build_gate = Arc::new(Semaphore::new(
            backpressure_policy.max_concurrent_artifact_builds.max(1),
        ));
        let gc_gate = Arc::new(Semaphore::new(
            backpressure_policy.max_concurrent_gc_jobs.max(1),
        ));
        Ok(Self {
            db,
            object_store,
            store_path,
            limits: options.limits,
            cache_policy: cache_policy.clone(),
            retention_policy: options.retention_policy,
            cache_metrics,
            operation_metrics,
            hydration_gate,
            graph_write_gate,
            artifact_build_gate,
            gc_gate,
            index_policy: options.index_policy,
            await_durable_writes: options.durability.await_durable_writes,
            write_authority,
            writer_lanes: (0..GRAPH_WRITE_LANES).map(|_| Mutex::new(())).collect(),
            matrix_artifact_cache: Mutex::new(BoundedGraphCache::new(
                cache_policy.max_matrix_artifacts,
                tenant_quota,
            )),
            matrix_cache: Mutex::new(BoundedGraphCache::new(
                cache_policy.max_matrix_adjacencies,
                tenant_quota,
            )),
            graphblas_cache: Mutex::new(BoundedGraphCache::new(
                cache_policy.max_graphblas_matrices,
                tenant_quota,
            )),
            #[cfg(feature = "opencypher")]
            parsed_row_query_cache: Mutex::new(BoundedGraphCache::new(
                cache_policy.max_parsed_row_queries,
                tenant_quota,
            )),
            #[cfg(feature = "opencypher")]
            reachability_cache: Mutex::new(BoundedGraphCache::new(
                cache_policy.max_reachability_results,
                tenant_quota,
            )),
            #[cfg(feature = "opencypher")]
            relationship_rows_cache: Mutex::new(BoundedGraphCache::new(
                cache_policy.max_relationship_row_sets,
                tenant_quota,
            )),
            #[cfg(feature = "opencypher")]
            source_relationship_rows_cache: Mutex::new(BoundedGraphCache::new(
                cache_policy.max_relationship_row_sets,
                tenant_quota,
            )),
            #[cfg(feature = "opencypher")]
            relationship_property_rows_cache: Mutex::new(BoundedGraphCache::new(
                cache_policy.max_relationship_property_row_sets,
                tenant_quota,
            )),
            supernode_group_cache: Mutex::new(BoundedGraphCache::new(
                cache_policy.max_supernode_groups,
                tenant_quota,
            )),
            posting_chunk_cache: Mutex::new(BoundedGraphCache::new(
                cache_policy.max_posting_chunks,
                tenant_quota,
            )),
            materialized_supernode_cache: Mutex::new(BoundedGraphCache::new(
                cache_policy.max_materialized_supernodes,
                tenant_quota,
            )),
        })
    }

    pub async fn close(&self) -> Result<()> {
        self.db.close().await?;
        Ok(())
    }

    pub fn graph_cache_metrics(&self) -> GraphCacheMetricsSnapshot {
        self.cache_metrics.snapshot()
    }

    pub fn graph_operational_metrics(&self) -> GraphOperationalMetricsSnapshot {
        self.operation_metrics.snapshot()
    }

    pub fn graph_index_policy(&self) -> GraphIndexPolicy {
        self.index_policy
    }

    pub(crate) fn writes_reverse_index(&self) -> bool {
        self.index_policy.write_reverse_index()
    }

    pub(crate) fn writer_lane(&self, cell_id: &str) -> &Mutex<()> {
        &self.writer_lanes[writer_lane_index(cell_id)]
    }

    pub(crate) fn cell_write_lock_path(&self, cell_id: &str) -> Path {
        let db_path = if self.store_path.as_ref().is_empty() {
            "__root__"
        } else {
            self.store_path.as_ref()
        };
        Path::from_iter(["__slatedb_graph_kernel", "write_locks", db_path, cell_id])
    }

    pub(crate) async fn acquire_cell_write_lock(
        &self,
        cell_id: &str,
        operation: &'static str,
    ) -> Result<CellWriteLock> {
        let path = self.cell_write_lock_path(cell_id);
        let owner_token = new_cell_write_lock_owner_token();

        for attempt in 0..GRAPH_CELL_WRITE_LOCK_MAX_ATTEMPTS {
            let now_ms = graph_now_millis();
            let payload = encode_cell_write_lock_record(
                cell_id,
                operation,
                &owner_token,
                now_ms,
                now_ms.saturating_add(GRAPH_CELL_WRITE_LOCK_TTL_MS),
                CellWriteLockState::Active,
            );
            match self
                .object_store
                .put_opts(&path, payload.clone().into(), PutMode::Create.into())
                .await
            {
                Ok(_) => {
                    return Ok(CellWriteLock {
                        object_store: Arc::clone(&self.object_store),
                        path,
                        owner_token,
                    });
                }
                Err(slatedb::object_store::Error::AlreadyExists { .. }) => {
                    if let Some(lock) = self
                        .try_reclaim_cell_write_lock(&path, cell_id, operation, &owner_token)
                        .await?
                    {
                        return Ok(lock);
                    }
                    if attempt + 1 < GRAPH_CELL_WRITE_LOCK_MAX_ATTEMPTS {
                        tokio::time::sleep(Duration::from_millis(GRAPH_CELL_WRITE_LOCK_BACKOFF_MS))
                            .await;
                        continue;
                    }
                    return Err(GraphError::CellWriteConflict {
                        operation,
                        cell_id: cell_id.to_string(),
                    });
                }
                Err(err) => return Err(err.into()),
            }
        }

        Err(GraphError::CellWriteConflict {
            operation,
            cell_id: cell_id.to_string(),
        })
    }

    async fn try_reclaim_cell_write_lock(
        &self,
        path: &Path,
        cell_id: &str,
        operation: &'static str,
        owner_token: &str,
    ) -> Result<Option<CellWriteLock>> {
        let current = match self.object_store.get(path).await {
            Ok(current) => current,
            Err(slatedb::object_store::Error::NotFound { .. }) => return Ok(None),
            Err(err) => return Err(err.into()),
        };
        let version = UpdateVersion {
            e_tag: current.meta.e_tag.clone(),
            version: current.meta.version.clone(),
        };
        let value = current.bytes().await?;
        let record = decode_cell_write_lock_record(path.as_ref(), &value)?;
        if record.cell_id != cell_id {
            return Err(GraphError::CorruptValue {
                key: path.to_string(),
                reason: format!(
                    "cell write lock belongs to cell {}, expected {cell_id}",
                    record.cell_id
                ),
            });
        }
        let now_ms = graph_now_millis();
        if !record.is_expired(now_ms) {
            return Ok(None);
        }
        let payload = encode_cell_write_lock_record(
            cell_id,
            operation,
            owner_token,
            now_ms,
            now_ms.saturating_add(GRAPH_CELL_WRITE_LOCK_TTL_MS),
            CellWriteLockState::Active,
        );
        match self
            .object_store
            .put_opts(path, payload.into(), PutMode::Update(version).into())
            .await
        {
            Ok(_) => Ok(Some(CellWriteLock {
                object_store: Arc::clone(&self.object_store),
                path: path.clone(),
                owner_token: owner_token.to_string(),
            })),
            Err(slatedb::object_store::Error::Precondition { .. })
            | Err(slatedb::object_store::Error::NotFound { .. })
            | Err(slatedb::object_store::Error::NotImplemented { .. })
            | Err(slatedb::object_store::Error::NotSupported { .. }) => Ok(None),
            Err(err) => Err(err.into()),
        }
    }

    pub async fn graph_cache_entry_counts(&self) -> GraphCacheEntryCounts {
        GraphCacheEntryCounts {
            matrix_artifacts: self.matrix_artifact_cache.lock().await.len(),
            matrix_adjacencies: self.matrix_cache.lock().await.len(),
            graphblas_matrices: self.graphblas_cache.lock().await.len(),
            #[cfg(feature = "opencypher")]
            parsed_row_queries: self.parsed_row_query_cache.lock().await.len(),
            #[cfg(feature = "opencypher")]
            reachability_results: self.reachability_cache.lock().await.len(),
            #[cfg(feature = "opencypher")]
            relationship_row_sets: self.relationship_rows_cache.lock().await.len()
                + self.source_relationship_rows_cache.lock().await.len(),
            #[cfg(feature = "opencypher")]
            relationship_property_row_sets: self
                .relationship_property_rows_cache
                .lock()
                .await
                .len(),
            supernode_groups: self.supernode_group_cache.lock().await.len(),
            posting_chunks: self.posting_chunk_cache.lock().await.len(),
            materialized_supernodes: self.materialized_supernode_cache.lock().await.len(),
        }
    }

    pub(crate) async fn acquire_hydration_permit(
        &self,
        operation: &'static str,
    ) -> Result<OwnedSemaphorePermit> {
        self.cache_metrics
            .hydration_started
            .fetch_add(1, Ordering::Relaxed);
        if self.hydration_gate.available_permits() == 0 {
            self.cache_metrics
                .hydration_waited
                .fetch_add(1, Ordering::Relaxed);
        }
        self.hydration_gate
            .clone()
            .acquire_owned()
            .await
            .map_err(|err| GraphError::CorruptValue {
                key: format!("cache/hydration/{operation}"),
                reason: format!("hydration gate closed: {err}"),
            })
    }

    pub(crate) fn record_hydration_complete(&self) {
        self.cache_metrics
            .hydration_completed
            .fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) async fn acquire_graph_write_permit(
        &self,
        operation: &'static str,
    ) -> Result<OwnedSemaphorePermit> {
        self.operation_metrics
            .write_attempts
            .fetch_add(1, Ordering::Relaxed);
        self.acquire_operation_permit(operation, &self.graph_write_gate)
            .await
    }

    pub(crate) async fn acquire_artifact_build_permit(
        &self,
        operation: &'static str,
    ) -> Result<OwnedSemaphorePermit> {
        self.operation_metrics
            .artifact_builds_started
            .fetch_add(1, Ordering::Relaxed);
        self.acquire_operation_permit(operation, &self.artifact_build_gate)
            .await
    }

    pub(crate) async fn acquire_gc_permit(
        &self,
        operation: &'static str,
    ) -> Result<OwnedSemaphorePermit> {
        self.operation_metrics
            .gc_jobs_started
            .fetch_add(1, Ordering::Relaxed);
        self.acquire_operation_permit(operation, &self.gc_gate)
            .await
    }

    async fn acquire_operation_permit(
        &self,
        operation: &'static str,
        gate: &Arc<Semaphore>,
    ) -> Result<OwnedSemaphorePermit> {
        if gate.available_permits() == 0 {
            self.operation_metrics
                .backpressure_waits
                .fetch_add(1, Ordering::Relaxed);
        }
        gate.clone()
            .acquire_owned()
            .await
            .map_err(|err| GraphError::CorruptValue {
                key: format!("backpressure/{operation}"),
                reason: format!("operation gate closed: {err}"),
            })
    }

    pub(crate) fn ensure_write_authority(
        &self,
        cell_id: &str,
        operation: &'static str,
    ) -> Result<()> {
        self.active_write_lease(cell_id, operation).map(|_| ())
    }

    pub(crate) fn active_write_lease(
        &self,
        cell_id: &str,
        operation: &'static str,
    ) -> Result<Option<engine::ShardLease>> {
        match &self.write_authority {
            GraphWriteAuthority::ReadOnly => Err(GraphError::WriteRequiresLease {
                operation,
                cell_id: cell_id.to_string(),
            }),
            GraphWriteAuthority::Standalone => Ok(None),
            GraphWriteAuthority::Leased {
                local_node_id,
                leases,
            } => {
                let Some(lease) = leases.read().map_err(lock_error)?.get(cell_id).cloned() else {
                    return Err(GraphError::WriteRequiresLease {
                        operation,
                        cell_id: cell_id.to_string(),
                    });
                };
                if lease.owner_node_id == *local_node_id && lease.expires_at_ms > graph_now_millis()
                {
                    Ok(Some(lease))
                } else {
                    Err(GraphError::StaleShardLease {
                        cell_id: cell_id.to_string(),
                        node_id: local_node_id.clone(),
                        lease_token: lease.lease_token,
                    })
                }
            }
        }
    }

    pub(crate) async fn install_write_fence(
        &self,
        cell_id: &str,
        lease: &engine::ShardLease,
    ) -> Result<()> {
        validate_component("cell_id", cell_id)?;
        validate_component("node_id", &lease.owner_node_id)?;
        if lease.cell_id != cell_id {
            return Err(GraphError::StaleShardLease {
                cell_id: cell_id.to_string(),
                node_id: lease.owner_node_id.clone(),
                lease_token: lease.lease_token,
            });
        }
        let Some(active) = self.active_write_lease(cell_id, "install_write_fence")? else {
            return Ok(());
        };
        if active.owner_node_id != lease.owner_node_id || active.lease_token != lease.lease_token {
            return Err(GraphError::StaleShardLease {
                cell_id: cell_id.to_string(),
                node_id: lease.owner_node_id.clone(),
                lease_token: lease.lease_token,
            });
        }

        let _writer = self.writer_lane(cell_id).lock().await;
        for attempt in 0..GRAPH_TXN_MAX_RETRIES {
            match self.install_write_fence_txn(cell_id, lease).await {
                Err(err)
                    if is_retryable_write_conflict(&err) && attempt + 1 < GRAPH_TXN_MAX_RETRIES =>
                {
                    tokio::task::yield_now().await;
                }
                result => return result,
            }
        }
        unreachable!("transaction retry loop always returns on final attempt")
    }

    async fn install_write_fence_txn(
        &self,
        cell_id: &str,
        lease: &engine::ShardLease,
    ) -> Result<()> {
        let txn = self.db.begin(IsolationLevel::SerializableSnapshot).await?;
        let key = keys::write_fence(cell_id);
        if let Some(value) = read_txn_remote(&txn, &key).await? {
            let current = decode_write_fence(&key, &value)?;
            if current.lease_token > lease.lease_token
                || (current.lease_token == lease.lease_token
                    && current.owner_node_id != lease.owner_node_id)
            {
                return Err(GraphError::StaleShardLease {
                    cell_id: cell_id.to_string(),
                    node_id: lease.owner_node_id.clone(),
                    lease_token: lease.lease_token,
                });
            }
        }
        txn.put(
            key.as_bytes(),
            encode_write_fence(&GraphWriteFence::from(lease)),
        )?;
        commit_txn_strict(txn, self.await_durable_writes).await
    }

    pub(crate) async fn validate_write_fence_txn(
        &self,
        txn: &DbTransaction,
        cell_id: &str,
        operation: &'static str,
    ) -> Result<()> {
        let Some(lease) = self.active_write_lease(cell_id, operation)? else {
            return Ok(());
        };
        let key = keys::write_fence(cell_id);
        let Some(value) = read_txn_remote(txn, &key).await? else {
            return Err(GraphError::WriteRequiresLease {
                operation,
                cell_id: cell_id.to_string(),
            });
        };
        let fence = decode_write_fence(&key, &value)?;
        if fence.cell_id == cell_id
            && fence.owner_node_id == lease.owner_node_id
            && fence.lease_token == lease.lease_token
        {
            Ok(())
        } else {
            Err(GraphError::StaleShardLease {
                cell_id: cell_id.to_string(),
                node_id: lease.owner_node_id,
                lease_token: lease.lease_token,
            })
        }
    }

    async fn publish_read_lease(&self, cell_id: &str, read_epoch: GraphEpoch) -> Result<()> {
        if self.retention_policy.read_lease_ttl_ms == 0 {
            return Ok(());
        }
        let now_ms = graph_now_millis();
        let lease_id = format!(
            "{now_ms:020}-{:020}",
            GRAPH_READ_LEASE_COUNTER.fetch_add(1, Ordering::Relaxed)
        );
        let lease = GraphReadLease {
            cell_id: cell_id.to_string(),
            lease_id: lease_id.clone(),
            read_epoch,
            expires_at_ms: now_ms.saturating_add(self.retention_policy.read_lease_ttl_ms),
        };
        let mut batch = WriteBatch::new();
        batch.put(
            keys::read_lease(cell_id, &lease_id).as_bytes(),
            encode_read_lease(&lease),
        );
        let options = WriteOptions {
            await_durable: true,
            ..Default::default()
        };
        self.db.write_with_options(batch, &options).await?;
        self.operation_metrics
            .read_leases_created
            .fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    async fn min_active_read_epoch(&self, cell_id: &str) -> Result<Option<GraphEpoch>> {
        if self.retention_policy.read_lease_ttl_ms == 0 {
            return Ok(None);
        }
        let now_ms = graph_now_millis();
        let prefix = keys::read_lease_prefix(cell_id);
        let mut iter = self.scan_remote_prefix(&prefix).await?;
        let mut scanned = 0_u64;
        let mut min_epoch = None;
        let mut expired_batch = GraphWriteBatch::new();
        let mut pending_deletes = 0_usize;
        while let Some(kv) = iter.next().await? {
            scanned = scanned.saturating_add(1);
            if scanned > self.retention_policy.max_read_leases_to_scan {
                self.operation_metrics
                    .retention_rejects
                    .fetch_add(1, Ordering::Relaxed);
                return Err(GraphError::AdmissionRejected {
                    operation: "read_lease_scan",
                    actual: scanned,
                    limit: self.retention_policy.max_read_leases_to_scan,
                });
            }
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            let lease = decode_read_lease(&key, &kv.value)?;
            if lease.cell_id != cell_id {
                return Err(GraphError::CorruptValue {
                    key,
                    reason: "read lease cell id does not match key prefix".to_string(),
                });
            }
            if lease.expires_at_ms <= now_ms {
                expired_batch.delete(key.as_bytes());
                pending_deletes += 1;
                if pending_deletes >= GRAPH_DELTA_GC_BATCH_KEYS {
                    self.flush_read_lease_gc_batch(
                        cell_id,
                        &mut expired_batch,
                        &mut pending_deletes,
                    )
                    .await?;
                }
            } else {
                min_epoch = Some(min_epoch.map_or(lease.read_epoch, |epoch: GraphEpoch| {
                    epoch.min(lease.read_epoch)
                }));
            }
        }
        self.flush_read_lease_gc_batch(cell_id, &mut expired_batch, &mut pending_deletes)
            .await?;
        Ok(min_epoch)
    }

    async fn flush_read_lease_gc_batch(
        &self,
        cell_id: &str,
        batch: &mut GraphWriteBatch,
        pending_deletes: &mut usize,
    ) -> Result<()> {
        if *pending_deletes == 0 {
            return Ok(());
        }
        let batch_to_write = std::mem::replace(batch, GraphWriteBatch::new());
        self.write_graph_batch_strict(cell_id, "prune_read_leases", batch_to_write)
            .await?;
        *pending_deletes = 0;
        Ok(())
    }

    pub(crate) async fn delta_gc_safe_epoch(
        &self,
        cell_id: &str,
        edge_type: &str,
    ) -> Result<GraphEpoch> {
        let current_epoch = self.current_epoch(cell_id).await?;
        let retained_safe_epoch =
            current_epoch.saturating_sub(self.retention_policy.min_retained_epochs);
        if self.min_active_read_epoch(cell_id).await?.is_some() {
            let watermark = self.delta_gc_watermark(cell_id, edge_type).await?;
            Ok(retained_safe_epoch.min(watermark))
        } else {
            Ok(retained_safe_epoch)
        }
    }

    pub(crate) async fn artifact_gc_safe_keep_epoch(
        &self,
        cell_id: &str,
        edge_type: &str,
    ) -> Result<GraphEpoch> {
        if self.min_active_read_epoch(cell_id).await?.is_some() {
            return Ok(1);
        }
        if self.retention_policy.min_retained_epochs == 0 {
            return Ok(GraphEpoch::MAX);
        }
        let current_epoch = self.current_epoch(cell_id).await?;
        let oldest_retained_epoch =
            current_epoch.saturating_sub(self.retention_policy.min_retained_epochs);
        if oldest_retained_epoch == 0 {
            return Ok(1);
        }
        Ok(self
            .latest_matrix_artifact(cell_id, edge_type, oldest_retained_epoch)
            .await?
            .map_or(1, |artifact| artifact.base_epoch))
    }

    pub(crate) fn record_retention_reject(
        &self,
        operation: &'static str,
        cell_id: &str,
        requested_epoch: GraphEpoch,
        safe_epoch: GraphEpoch,
    ) -> GraphError {
        self.operation_metrics
            .retention_rejects
            .fetch_add(1, Ordering::Relaxed);
        GraphError::RetentionViolation {
            operation,
            cell_id: cell_id.to_string(),
            requested_epoch,
            safe_epoch,
        }
    }

    pub(crate) fn record_artifact_build_completed(&self, duration: std::time::Duration) {
        self.operation_metrics
            .artifact_builds_completed
            .fetch_add(1, Ordering::Relaxed);
        self.operation_metrics
            .artifact_build_duration_us
            .fetch_add(duration_micros_u64(duration), Ordering::Relaxed);
    }

    pub(crate) fn record_gc_completed(&self, deleted_keys: u64, duration: std::time::Duration) {
        self.operation_metrics
            .gc_jobs_completed
            .fetch_add(1, Ordering::Relaxed);
        self.operation_metrics
            .gc_keys_deleted
            .fetch_add(deleted_keys, Ordering::Relaxed);
        self.operation_metrics
            .gc_duration_us
            .fetch_add(duration_micros_u64(duration), Ordering::Relaxed);
    }

    pub(crate) fn record_verifier_completed(
        &self,
        mismatch_count: u64,
        duration: std::time::Duration,
    ) {
        self.operation_metrics
            .verifier_runs
            .fetch_add(1, Ordering::Relaxed);
        self.operation_metrics
            .verifier_duration_us
            .fetch_add(duration_micros_u64(duration), Ordering::Relaxed);
        if mismatch_count > 0 {
            self.operation_metrics
                .verifier_failures
                .fetch_add(1, Ordering::Relaxed);
        }
    }

    pub(crate) fn record_graph_batch_commit(
        &self,
        operation: &'static str,
        record_count: usize,
        duration: std::time::Duration,
    ) {
        if matches!(
            operation,
            "build_posting_chunks"
                | "build_matrix_tiles"
                | "build_supernode_groups"
                | "rollup_artifacts"
        ) {
            self.operation_metrics
                .artifact_publish_batches
                .fetch_add(1, Ordering::Relaxed);
            self.operation_metrics
                .artifact_records_published
                .fetch_add(record_count as u64, Ordering::Relaxed);
            self.operation_metrics
                .artifact_publish_duration_us
                .fetch_add(duration_micros_u64(duration), Ordering::Relaxed);
        }
    }

    pub(crate) fn record_bulk_import_profile(
        &self,
        preflight: std::time::Duration,
        batch_build: std::time::Duration,
        counter_read: std::time::Duration,
        commit: std::time::Duration,
    ) {
        self.operation_metrics
            .bulk_import_batches_profiled
            .fetch_add(1, Ordering::Relaxed);
        self.operation_metrics
            .bulk_import_preflight_us
            .fetch_add(duration_micros_u64(preflight), Ordering::Relaxed);
        self.operation_metrics
            .bulk_import_batch_build_us
            .fetch_add(duration_micros_u64(batch_build), Ordering::Relaxed);
        self.operation_metrics
            .bulk_import_counter_read_us
            .fetch_add(duration_micros_u64(counter_read), Ordering::Relaxed);
        self.operation_metrics
            .bulk_import_commit_us
            .fetch_add(duration_micros_u64(commit), Ordering::Relaxed);
    }

    pub async fn snapshot(&self, cell_id: &str) -> Result<GraphSnapshot<'_>> {
        validate_component("cell_id", cell_id)?;
        let read_epoch = self.current_epoch(cell_id).await?;
        self.publish_read_lease(cell_id, read_epoch).await?;
        Ok(GraphSnapshot {
            shard: self,
            cell_id: cell_id.to_string(),
            read_epoch,
        })
    }

    pub async fn snapshot_at(
        &self,
        cell_id: &str,
        read_epoch: GraphEpoch,
    ) -> Result<GraphSnapshot<'_>> {
        validate_component("cell_id", cell_id)?;
        let current_epoch = self.current_epoch(cell_id).await?;
        if read_epoch > current_epoch {
            return Err(GraphError::SnapshotAhead {
                cell_id: cell_id.to_string(),
                read_epoch,
                current_epoch,
            });
        }
        self.publish_read_lease(cell_id, read_epoch).await?;
        Ok(GraphSnapshot {
            shard: self,
            cell_id: cell_id.to_string(),
            read_epoch,
        })
    }
}
