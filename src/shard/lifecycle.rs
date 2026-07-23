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
                ..GraphOpenOptions::default()
            },
        )
        .await
    }

    pub async fn open_with_options(
        path: impl Into<Path>,
        object_store: Arc<dyn ObjectStore>,
        options: GraphOpenOptions,
    ) -> Result<Self> {
        Self::open_with_memory_options(path, object_store, options, GraphMemoryConfig::default())
            .await
    }

    pub async fn open_with_memory_options(
        path: impl Into<Path>,
        object_store: Arc<dyn ObjectStore>,
        options: GraphOpenOptions,
        memory: GraphMemoryConfig,
    ) -> Result<Self> {
        Self::open_internal(
            path,
            object_store,
            options,
            memory,
            GraphWriteAuthority::ReadOnly,
        )
        .await
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
                ..GraphOpenOptions::default()
            },
        )
        .await
    }

    pub async fn open_standalone_writer_with_options(
        path: impl Into<Path>,
        object_store: Arc<dyn ObjectStore>,
        options: GraphOpenOptions,
    ) -> Result<Self> {
        Self::open_standalone_writer_with_memory_options(
            path,
            object_store,
            options,
            GraphMemoryConfig::default(),
        )
        .await
    }

    pub async fn open_standalone_writer_with_memory_options(
        path: impl Into<Path>,
        object_store: Arc<dyn ObjectStore>,
        options: GraphOpenOptions,
        memory: GraphMemoryConfig,
    ) -> Result<Self> {
        Self::open_internal(
            path,
            object_store,
            options,
            memory,
            GraphWriteAuthority::Writer,
        )
        .await
    }

    pub(crate) async fn open_promotable_with_memory_options(
        path: impl Into<Path>,
        object_store: Arc<dyn ObjectStore>,
        options: GraphOpenOptions,
        memory: GraphMemoryConfig,
    ) -> Result<Self> {
        Self::open_internal(
            path,
            object_store,
            options,
            memory,
            GraphWriteAuthority::Promotable,
        )
        .await
    }

    async fn open_internal(
        path: impl Into<Path>,
        object_store: Arc<dyn ObjectStore>,
        options: GraphOpenOptions,
        memory: GraphMemoryConfig,
        write_authority: GraphWriteAuthority,
    ) -> Result<Self> {
        if !options.durability.await_durable_writes
            && !matches!(&write_authority, GraphWriteAuthority::ReadOnly)
        {
            return Err(GraphError::UnsafeDurabilityConfig {
                operation: "open_write_authoritative_shard",
                reason: "graph writes rely on remotely durable SlateDB sequences, degree counters, and idempotency records".to_string(),
            });
        }

        let store_path = path.into();
        let db = GraphStore::lazy(
            store_path.clone(),
            Arc::clone(&object_store),
            options.cache.clone(),
            memory.storage.clone(),
            options.durability.clone(),
        );
        match &write_authority {
            GraphWriteAuthority::ReadOnly => {
                let _ = db.open_reader().await?;
            }
            GraphWriteAuthority::Promotable => {}
            GraphWriteAuthority::Writer => {
                db.promote_writer().await?;
            }
        }
        let cache_policy = options.cache_policy;
        let backpressure_policy = options.backpressure_policy;
        let tenant_quota = cache_policy.max_entries_per_cell;
        let cache_metrics = Arc::new(GraphCacheMetrics::default());
        let operation_metrics = Arc::new(GraphOperationalMetrics::default());
        let hydration_gate = Arc::new(Semaphore::new(cache_policy.hydration_permits()));
        #[cfg(feature = "graphblas")]
        let matrix_compilation_gate = Arc::new(Semaphore::new(memory.matrix_compilation_permits()));
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
            limits: options.limits,
            cache_policy: cache_policy.clone(),
            cache_metrics,
            operation_metrics,
            hydration_gate,
            #[cfg(feature = "graphblas")]
            matrix_compilation_gate,
            graph_write_gate,
            artifact_build_gate,
            gc_gate,
            index_policy: options.index_policy,
            await_durable_writes: options.durability.await_durable_writes,
            write_authority,
            local_write_guard: Arc::new(Mutex::new(())),
            local_artifact_guard: Arc::new(Mutex::new(())),
            writer_lanes: (0..GRAPH_WRITE_LANES).map(|_| Mutex::new(())).collect(),
            matrix_artifact_cache: Mutex::new(BoundedGraphCache::new(
                cache_policy.max_matrix_artifacts,
                tenant_quota,
            )),
            graph_index_generations: Mutex::new(BTreeMap::new()),
            matrix_cache: Mutex::new(BoundedGraphCache::new_with_byte_limit(
                cache_policy.max_matrix_adjacencies,
                tenant_quota,
                memory.max_matrix_adjacency_bytes,
            )),
            graphblas_cache: Mutex::new(BoundedGraphCache::new_with_byte_limit(
                cache_policy.max_graphblas_matrices,
                tenant_quota,
                memory.max_graphblas_bytes,
            )),
            #[cfg(feature = "opencypher")]
            parsed_row_query_cache: Mutex::new(BoundedGraphCache::new(
                cache_policy.max_parsed_row_queries,
                tenant_quota,
            )),
            #[cfg(feature = "opencypher")]
            relationship_rows_cache: Mutex::new(BoundedGraphCache::new_with_byte_limit(
                cache_policy.max_relationship_row_sets,
                tenant_quota,
                memory.max_relationship_rows_bytes,
            )),
            #[cfg(feature = "opencypher")]
            source_relationship_rows_cache: Mutex::new(BoundedGraphCache::new_with_byte_limit(
                cache_policy.max_relationship_row_sets,
                tenant_quota,
                memory.max_source_relationship_rows_bytes,
            )),
            #[cfg(feature = "opencypher")]
            relationship_property_rows_cache: Mutex::new(BoundedGraphCache::new_with_byte_limit(
                cache_policy.max_relationship_property_row_sets,
                tenant_quota,
                memory.max_relationship_property_rows_bytes,
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

    pub(crate) async fn acquire_local_write_guard(
        &self,
        cell_id: &str,
        operation: &'static str,
    ) -> Result<LocalWriteGuard> {
        validate_component("cell_id", cell_id)?;
        validate_component("operation", operation)?;
        let guard = LocalWriteGuard::new(Arc::clone(&self.local_write_guard).lock_owned().await);
        self.db.refresh_writer_fence().await?;
        Ok(guard)
    }

    pub(crate) async fn acquire_local_artifact_guard(
        &self,
        cell_id: &str,
        edge_type: &str,
        _base_epoch: StorageSequence,
        operation: &'static str,
    ) -> Result<LocalWriteGuard> {
        validate_component("cell_id", cell_id)?;
        validate_component("edge_type", edge_type)?;
        validate_component("operation", operation)?;
        let guard = Arc::clone(&self.local_artifact_guard)
            .try_lock_owned()
            .map_err(|_| GraphError::ConditionalWriteConflict {
                operation,
                key: format!("local-artifact/{cell_id}/{edge_type}"),
            })?;
        Ok(LocalWriteGuard::new(guard))
    }

    pub async fn graph_cache_entry_counts(&self) -> GraphCacheEntryCounts {
        GraphCacheEntryCounts {
            matrix_artifacts: self.matrix_artifact_cache.lock().await.len(),
            matrix_adjacencies: self.matrix_cache.lock().await.len(),
            graphblas_matrices: self.graphblas_cache.lock().await.len(),
            #[cfg(feature = "opencypher")]
            parsed_row_queries: self.parsed_row_query_cache.lock().await.len(),
            #[cfg(feature = "opencypher")]
            relationship_row_sets: self.relationship_rows_cache.lock().await.len()
                + self.source_relationship_rows_cache.lock().await.len(),
            #[cfg(feature = "opencypher")]
            relationship_property_row_sets: self
                .relationship_property_rows_cache
                .lock()
                .await
                .len(),
        }
    }

    pub async fn graph_cache_resident_bytes(&self) -> GraphCacheResidentBytes {
        GraphCacheResidentBytes {
            matrix_adjacencies: self.matrix_cache.lock().await.resident_bytes(),
            graphblas_matrices: self.graphblas_cache.lock().await.resident_bytes(),
            #[cfg(feature = "opencypher")]
            relationship_rows: self.relationship_rows_cache.lock().await.resident_bytes(),
            #[cfg(feature = "opencypher")]
            source_relationship_rows: self
                .source_relationship_rows_cache
                .lock()
                .await
                .resident_bytes(),
            #[cfg(feature = "opencypher")]
            relationship_property_rows: self
                .relationship_property_rows_cache
                .lock()
                .await
                .resident_bytes(),
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
        match &self.write_authority {
            GraphWriteAuthority::ReadOnly => Err(GraphError::WriteRequiresWriter {
                operation,
                cell_id: cell_id.to_string(),
            }),
            GraphWriteAuthority::Promotable | GraphWriteAuthority::Writer => {
                self.db.writer().map(|_| ())
            }
        }
    }

    pub(crate) async fn promote_to_writer(
        &self,
        cell_id: &str,
        operation: &'static str,
    ) -> Result<()> {
        validate_component("cell_id", cell_id)?;
        if matches!(&self.write_authority, GraphWriteAuthority::ReadOnly) {
            return Err(GraphError::WriteRequiresWriter {
                operation,
                cell_id: cell_id.to_string(),
            });
        }
        self.db.promote_writer().await?;
        Ok(())
    }

    pub(crate) async fn validate_write_fence_txn(
        &self,
        txn: &DbTransaction,
        cell_id: &str,
        operation: &'static str,
    ) -> Result<()> {
        if operation != "drop_cell" {
            let drop_marker = keys::cell_drop_marker(cell_id);
            let pending_drop_marker = keys::cell_drop_pending_marker(cell_id);
            if read_txn_remote(txn, &drop_marker).await?.is_some()
                || read_txn_remote(txn, &pending_drop_marker).await?.is_some()
            {
                return Err(GraphError::CellDropped {
                    operation,
                    cell_id: cell_id.to_string(),
                });
            }
        }
        self.ensure_write_authority(cell_id, operation)
    }

    pub(crate) async fn validate_write_fence(
        &self,
        cell_id: &str,
        operation: &'static str,
    ) -> Result<()> {
        self.db.refresh_writer_fence().await?;
        let txn = self
            .db
            .writer()?
            .begin(IsolationLevel::SerializableSnapshot)
            .await?;
        self.validate_write_fence_txn(&txn, cell_id, operation)
            .await
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
        if operation == "build_matrix_tiles" {
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
        self.snapshot_for_query(cell_id, false).await
    }

    pub(crate) async fn snapshot_for_query(
        &self,
        cell_id: &str,
        refreshed_reader: bool,
    ) -> Result<GraphSnapshot<'_>> {
        validate_component("cell_id", cell_id)?;
        let storage_snapshot = if refreshed_reader {
            self.db.reader_snapshot().await?
        } else {
            self.db.snapshot().await?
        };
        let read_epoch = storage_snapshot.seq();
        Ok(GraphSnapshot {
            shard: self,
            cell_id: cell_id.to_string(),
            read_epoch,
            storage_snapshot,
        })
    }

    pub async fn current_storage_sequence(&self, cell_id: &str) -> Result<StorageSequence> {
        validate_component("cell_id", cell_id)?;
        self.db.durable_sequence().await
    }

    pub async fn refresh_storage_sequence(&self, cell_id: &str) -> Result<StorageSequence> {
        validate_component("cell_id", cell_id)?;
        self.ensure_cell_readable(cell_id, "refresh_storage_sequence")
            .await?;
        self.db.refresh_durable_reader().await
    }

    pub async fn wait_for_storage_sequence(
        &self,
        cell_id: &str,
        minimum: StorageSequence,
    ) -> Result<StorageSequence> {
        validate_component("cell_id", cell_id)?;
        let current = self.db.durable_sequence().await?;
        if current >= minimum {
            return Ok(current);
        }
        let deadline = std::time::Instant::now()
            + std::time::Duration::from_millis(
                self.limits.max_query_runtime_ms.unwrap_or(30_000).max(1),
            );
        loop {
            let current = self.db.refresh_durable_reader().await?;
            if current >= minimum {
                return Ok(current);
            }
            if std::time::Instant::now() >= deadline {
                return Err(GraphError::SnapshotAhead {
                    cell_id: cell_id.to_string(),
                    read_epoch: minimum,
                    current_epoch: current,
                });
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    }

    pub async fn snapshot_at(
        &self,
        cell_id: &str,
        read_epoch: StorageSequence,
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
        if read_epoch != current_epoch {
            return Err(GraphError::UnsupportedQuery {
                dialect: "GraphSnapshot",
                feature: "historical graph epochs are not SlateDB snapshots".to_string(),
            });
        }
        Ok(GraphSnapshot {
            shard: self,
            cell_id: cell_id.to_string(),
            read_epoch,
            storage_snapshot: self.db.snapshot().await?,
        })
    }
}
