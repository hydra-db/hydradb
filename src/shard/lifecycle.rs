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
                reason: "graph writers allocate epochs from remote-visible metadata; relaxed durability can release cross-process fences before last_epoch, degree counters, and idempotency keys are visible".to_string(),
            });
        }

        let store_path = path.into();
        let db = match &write_authority {
            GraphWriteAuthority::ReadOnly => GraphStore::Reader(Arc::new(
                open_graph_reader(
                    store_path.clone(),
                    Arc::clone(&object_store),
                    &options.cache,
                )
                .await?,
            )),
            GraphWriteAuthority::Writer => GraphStore::Writer(
                open_graph_db(
                    store_path.clone(),
                    Arc::clone(&object_store),
                    &options.cache,
                    &memory.storage,
                    &options.durability,
                )
                .await?,
            ),
        };
        ensure_store_format(&db, &write_authority).await?;
        let cache_policy = options.cache_policy;
        let backpressure_policy = options.backpressure_policy;
        let tenant_quota = cache_policy.max_entries_per_cell;
        let cache_metrics = Arc::new(GraphCacheMetrics::default());
        let operation_metrics = Arc::new(GraphOperationalMetrics::default());
        let hydration_gate = Arc::new(Semaphore::new(cache_policy.hydration_permits()));
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
            object_store,
            store_path,
            limits: options.limits,
            cache_policy: cache_policy.clone(),
            cache_metrics,
            operation_metrics,
            hydration_gate,
            matrix_compilation_gate,
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

    pub(crate) fn cell_write_lock_path(&self, cell_id: &str) -> Path {
        let db_path = if self.store_path.as_ref().is_empty() {
            "__root__"
        } else {
            self.store_path.as_ref()
        };
        Path::from_iter(["__slatedb_graph_kernel", "write_locks", db_path, cell_id])
    }

    pub(crate) fn graph_artifact_write_lock_path(
        &self,
        artifact_kind: &'static str,
        cell_id: &str,
        edge_type: &str,
        base_epoch: TopologySequence,
    ) -> Path {
        let db_path = if self.store_path.as_ref().is_empty() {
            "__root__"
        } else {
            self.store_path.as_ref()
        };
        let base_epoch = format!("{base_epoch:020}");
        let lock_namespace = match artifact_kind {
            "matrix" => "matrix_artifact_locks",
            _ => "artifact_locks",
        };
        Path::from_iter([
            "__slatedb_graph_kernel",
            lock_namespace,
            db_path,
            cell_id,
            edge_type,
            &base_epoch,
        ])
    }

    pub(crate) fn matrix_artifact_write_lock_path(
        &self,
        cell_id: &str,
        edge_type: &str,
        base_epoch: TopologySequence,
    ) -> Path {
        self.graph_artifact_write_lock_path("matrix", cell_id, edge_type, base_epoch)
    }

    pub(crate) async fn acquire_cell_write_lock(
        &self,
        cell_id: &str,
        operation: &'static str,
    ) -> Result<CellWriteLock> {
        let path = self.cell_write_lock_path(cell_id);
        self.acquire_write_lock_at_path(path, cell_id, operation)
            .await
    }

    pub(crate) async fn acquire_matrix_artifact_write_lock(
        &self,
        cell_id: &str,
        edge_type: &str,
        base_epoch: TopologySequence,
        operation: &'static str,
    ) -> Result<CellWriteLock> {
        validate_component("cell_id", cell_id)?;
        validate_component("edge_type", edge_type)?;
        let path = self.matrix_artifact_write_lock_path(cell_id, edge_type, base_epoch);
        self.acquire_write_lock_at_path(path, cell_id, operation)
            .await
    }

    async fn acquire_write_lock_at_path(
        &self,
        path: Path,
        cell_id: &str,
        operation: &'static str,
    ) -> Result<CellWriteLock> {
        acquire_distributed_write_lock(
            Arc::clone(&self.object_store),
            path,
            cell_id,
            operation,
            GRAPH_CELL_WRITE_LOCK_TTL_MS,
        )
        .await
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
            GraphWriteAuthority::Writer => self.db.writer().map(|_| ()),
        }
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
        validate_component("cell_id", cell_id)?;
        let storage_snapshot = self.db.snapshot().await?;
        let read_epoch = if let Some(snapshot) = storage_snapshot.as_ref() {
            let key = keys::last_epoch(cell_id);
            match snapshot
                .get_with_options(key.as_bytes(), &remote_read_options())
                .await?
            {
                Some(value) => decode_u64(&key, &value)?,
                None => 0,
            }
        } else {
            // DbReader pins a checkpoint/manifest even though this SlateDB
            // revision does not expose a DbSnapshot handle for it.
            self.current_epoch(cell_id).await?
        };
        Ok(GraphSnapshot {
            shard: self,
            cell_id: cell_id.to_string(),
            read_epoch,
            storage_snapshot,
        })
    }

    pub async fn snapshot_at(
        &self,
        cell_id: &str,
        read_epoch: TopologySequence,
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
