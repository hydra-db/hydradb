use super::*;
use crate::core::memory_diagnostics::{
    request_estimated_bytes, MemoryDiagnosticGuard, MemoryStage,
};

use tracing::Instrument as _;

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
            None,
            None,
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
            None,
            None,
        )
        .await
    }

    pub(crate) async fn open_promotable_for_node_with_memory_options(
        path: impl Into<Path>,
        object_store: Arc<dyn ObjectStore>,
        options: GraphOpenOptions,
        memory: GraphMemoryConfig,
        local_node_id: &str,
        writer_registry: Arc<crate::ProcessWriterRegistry>,
    ) -> Result<Self> {
        Self::open_internal(
            path,
            object_store,
            options,
            memory,
            GraphWriteAuthority::Promotable,
            Some(local_node_id),
            Some(writer_registry),
        )
        .await
    }

    async fn open_internal(
        path: impl Into<Path>,
        object_store: Arc<dyn ObjectStore>,
        options: GraphOpenOptions,
        memory: GraphMemoryConfig,
        write_authority: GraphWriteAuthority,
        process_writer_node_id: Option<&str>,
        writer_registry: Option<Arc<crate::ProcessWriterRegistry>>,
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
        let process_writer = process_writer_node_id.zip(writer_registry);
        let db = GraphStore::lazy(
            store_path.clone(),
            Arc::clone(&object_store),
            &options,
            &memory,
            process_writer,
        )?;
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
            matrix_compilation_gate,
            graph_write_gate,
            write_pipeline_gate: Arc::new(Semaphore::new(
                super::write_pipeline::MAX_PENDING_DURABLE_WRITES,
            )),
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
            // Sized like the parsed-query cache: one shape asks for a handful
            // of facts, so the vocabulary of facts is a small multiple of the
            // vocabulary of shapes. The byte cap matters only for records that
            // carry a Bloom filter; plain counts are a few hundred bytes.
            #[cfg(feature = "experimental-cypher-engine")]
            experimental_statistics_memo: Mutex::new(BoundedGraphCache::new_with_byte_limit(
                cache_policy.max_parsed_row_queries,
                tenant_quota,
                crate::shard::experimental_cypher::STATISTICS_MEMO_MAX_BYTES,
            )),
            #[cfg(feature = "experimental-cypher-engine")]
            planner_statistics: options.planner_statistics,
            #[cfg(feature = "experimental-cypher-engine")]
            planner_statistics_cache: Default::default(),
            #[cfg(feature = "experimental-cypher-engine")]
            planner_statistics_hints: Default::default(),
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
            #[cfg(feature = "opencypher")]
            native_path_result_cache: Mutex::new(BoundedGraphCache::new_with_byte_limit(
                cache_policy.max_relationship_row_sets,
                tenant_quota,
                memory.max_native_path_result_bytes,
            )),
            #[cfg(feature = "opencypher")]
            native_path_page_cursors: Mutex::new(Default::default()),
            wal_tail_file_cache: Mutex::new(Default::default()),
            xlog_floor_ensured: std::sync::RwLock::new(std::collections::HashSet::new()),
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

    pub fn graph_storage_metrics(&self) -> GraphStorageMetricsSnapshot {
        self.db.storage_metrics()
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
        let guard = LocalWriteGuard::new(
            Arc::clone(&self.local_write_guard)
                .lock_owned()
                .instrument(tracing::info_span!("shard.local_write_guard", hydradb.cell_id = %cell_id, hydradb.write.operation = operation))
                .await,
        );
        self.await_prior_write_durability().await?;
        self.db.refresh_writer_fence().await?;
        Ok(guard)
    }

    pub(crate) async fn acquire_local_write_guard_no_fence(
        &self,
        cell_id: &str,
        operation: &'static str,
    ) -> Result<LocalWriteGuard> {
        validate_component("cell_id", cell_id)?;
        validate_component("operation", operation)?;
        let guard = LocalWriteGuard::new(
            Arc::clone(&self.local_write_guard)
                .lock_owned()
                .instrument(tracing::info_span!("shard.local_write_guard", hydradb.cell_id = %cell_id, hydradb.write.operation = operation))
                .await,
        );
        self.await_prior_write_durability().await?;
        Ok(guard)
    }

    /// Only for paths that must commit before reporting success. SlateDB fences
    /// a superseded writer at the create-only WAL barrier, so a durable commit
    /// already verifies ownership. A replay/no-op must use the manifest check
    /// instead; an in-memory acknowledgement has no WAL barrier to wait for.
    pub(crate) async fn validate_changing_write(&self) -> Result<()> {
        if self.await_durable_writes && self.db.writer()?.snapshot().await.is_ok() {
            return Ok(());
        }
        // Retain fenced-handle cleanup/backoff and memory-only write behavior.
        self.db.refresh_writer_fence().await
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
        // Each lock is bound to its own `let` so the guard drops at the end of that
        // statement. Building the struct literal directly from `lock().await` would
        // keep every temporary guard alive until the function returns, holding all of
        // these read-path mutexes simultaneously.
        let matrix_artifacts = self.matrix_artifact_cache.lock().await.len();
        let matrix_adjacencies = self.matrix_cache.lock().await.len();
        let graphblas_matrices = self.graphblas_cache.lock().await.len();
        #[cfg(feature = "opencypher")]
        let parsed_row_queries = self.parsed_row_query_cache.lock().await.len();
        #[cfg(feature = "opencypher")]
        let relationship_row_sets = {
            let relationship_rows = self.relationship_rows_cache.lock().await.len();
            let source_relationship_rows = self.source_relationship_rows_cache.lock().await.len();
            relationship_rows + source_relationship_rows
        };
        #[cfg(feature = "opencypher")]
        let relationship_property_row_sets =
            self.relationship_property_rows_cache.lock().await.len();
        #[cfg(feature = "opencypher")]
        let native_path_results = self.native_path_result_cache.lock().await.len();
        GraphCacheEntryCounts {
            matrix_artifacts,
            matrix_adjacencies,
            graphblas_matrices,
            #[cfg(feature = "opencypher")]
            parsed_row_queries,
            #[cfg(feature = "opencypher")]
            relationship_row_sets,
            #[cfg(feature = "opencypher")]
            relationship_property_row_sets,
            #[cfg(feature = "opencypher")]
            native_path_results,
        }
    }

    pub async fn graph_cache_resident_bytes(&self) -> GraphCacheResidentBytes {
        // See `graph_cache_entry_counts`: one `let` per lock keeps the acquisitions
        // disjoint instead of overlapping for the whole function body.
        let matrix_adjacencies = self.matrix_cache.lock().await.resident_bytes();
        let graphblas_matrices = self.graphblas_cache.lock().await.resident_bytes();
        #[cfg(feature = "opencypher")]
        let relationship_rows = self.relationship_rows_cache.lock().await.resident_bytes();
        #[cfg(feature = "opencypher")]
        let source_relationship_rows = self
            .source_relationship_rows_cache
            .lock()
            .await
            .resident_bytes();
        #[cfg(feature = "opencypher")]
        let relationship_property_rows = self
            .relationship_property_rows_cache
            .lock()
            .await
            .resident_bytes();
        #[cfg(feature = "opencypher")]
        let native_path_results = self.native_path_result_cache.lock().await.resident_bytes();
        GraphCacheResidentBytes {
            matrix_adjacencies,
            graphblas_matrices,
            #[cfg(feature = "opencypher")]
            relationship_rows,
            #[cfg(feature = "opencypher")]
            source_relationship_rows,
            #[cfg(feature = "opencypher")]
            relationship_property_rows,
            #[cfg(feature = "opencypher")]
            native_path_results,
        }
    }

    pub(crate) async fn acquire_hydration_permit(
        &self,
        operation: &'static str,
    ) -> Result<OwnedSemaphorePermit> {
        self.cache_metrics.record_hydration_started();
        if self.hydration_gate.available_permits() == 0 {
            self.cache_metrics.record_hydration_waited();
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
        self.cache_metrics.record_hydration_completed();
    }

    pub(crate) async fn acquire_graph_write_permit(
        &self,
        operation: &'static str,
    ) -> Result<(
        OwnedSemaphorePermit,
        MemoryDiagnosticGuard,
        MemoryDiagnosticGuard,
    )> {
        self.operation_metrics
            .write_attempts
            .fetch_add(1, Ordering::Relaxed);
        let bytes = request_estimated_bytes();
        let waiting = MemoryDiagnosticGuard::new(MemoryStage::ShardWriteWait, bytes);
        let acquire = self.acquire_operation_permit(operation, &self.graph_write_gate);
        // A queued request has not started a transaction. Stop it here without
        // dropping a commit already in progress or releasing another writer's permit.
        let permit = match crate::QueryCancellationToken::current() {
            Some(token) => tokio::select! {
                biased;
                _ = token.cancelled() => return Err(GraphError::QueryTimeout {
                    operation: "query_cancelled",
                    elapsed_ms: 0,
                    limit_ms: 0,
                }),
                permit = acquire => permit?,
            },
            None => acquire.await?,
        };
        drop(waiting);
        // The same hold split by commit path. A pipeline write releases the
        // permit after its local commit; an inline write still holds it through
        // its remote durability wait. Zero bytes: `shard_write_active` already
        // carries this request's byte view.
        let hold = if super::write_pipeline::current().is_some() {
            MemoryStage::ShardWriteActivePipeline
        } else {
            MemoryStage::ShardWriteActiveInline
        };
        Ok((
            permit,
            MemoryDiagnosticGuard::new(MemoryStage::ShardWriteActive, bytes),
            MemoryDiagnosticGuard::new(hold, 0),
        ))
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

    /// Whether this shard may write at all, as its own span.
    ///
    /// In-memory and I/O-free, so the span is a timestamp pair. It is worth that
    /// because it separates two failures a caller cannot otherwise tell apart:
    /// a shard opened read-only, and a shard whose writer handle has gone. Both
    /// classify as `fencing`, and only the span name says which.
    pub(crate) fn ensure_write_authority(
        &self,
        cell_id: &str,
        operation: &'static str,
    ) -> Result<()> {
        let span = tracing::info_span!(
            "writer.authority",
            hydradb.cell_id = %cell_id,
            error.class = tracing::field::Empty,
        );
        let _entered = span.enter();
        match &self.write_authority {
            GraphWriteAuthority::ReadOnly => Err(GraphError::WriteRequiresWriter {
                operation,
                cell_id: cell_id.to_string(),
            }),
            GraphWriteAuthority::Promotable | GraphWriteAuthority::Writer => {
                self.db.writer().map(|_| ())
            }
        }
        .inspect_err(|error| {
            tracing::Span::current().record("error.class", error.class());
        })
    }

    /// Take the SlateDB writer for `cell_id`, if this node does not already
    /// hold it.
    ///
    /// # Why the span is conditional
    ///
    /// The don't-promote-a-cell-you-do-not-own rule from the rendezvous
    /// placement note is a correctness invariant, and a span that materialises
    /// only when a promotion *actually happens* makes violations countable
    /// rather than theoretical. A `writer.promote` per write — which is what an
    /// unconditional span would give, since this function is idempotent and is
    /// called on every routed write — would bury the handful of real promotions
    /// under the no-ops and make the count meaningless.
    ///
    /// The pre-check is the same one `await_writer_reopen` already makes and is
    /// racy in the same harmless way: losing the race emits one span for a
    /// promotion that turned out to be a no-op, which is a diagnostic
    /// imprecision and not a behaviour change.
    pub(crate) async fn promote_to_writer(
        &self,
        cell_id: &str,
        operation: &'static str,
    ) -> Result<()> {
        validate_component("cell_id", cell_id)?;
        if matches!(&self.write_authority, GraphWriteAuthority::ReadOnly) {
            let error = GraphError::WriteRequiresWriter {
                operation,
                cell_id: cell_id.to_string(),
            };
            tracing::Span::current().record("error.class", error.class());
            return Err(error);
        }
        if self.db.writer().is_ok() {
            // Already holding it: `promote_writer` short-circuits and there is
            // no promotion to make countable.
            self.db.promote_writer().await?;
            return Ok(());
        }
        let span = tracing::info_span!(
            "writer.promote",
            hydradb.cell_id = %cell_id,
            hydradb.writer.epoch = tracing::field::Empty,
            error.class = tracing::field::Empty,
        );
        async {
            match self.db.promote_writer().await {
                Ok(_) => {
                    if let Some(epoch) = self.db.writer_epoch() {
                        tracing::Span::current().record("hydradb.writer.epoch", epoch);
                    }
                    Ok(())
                }
                Err(error) => {
                    tracing::Span::current().record("error.class", error.class());
                    Err(error)
                }
            }
        }
        .instrument(span)
        .await
    }

    pub(crate) async fn retire_writer(&self) -> Result<bool> {
        self.db.retire_writer().await
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
            let (dropped, pending) = tokio::join!(
                read_txn_remote(txn, &drop_marker),
                read_txn_remote(txn, &pending_drop_marker),
            );
            if dropped?.is_some() || pending?.is_some() {
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

    /// The relationship CREATE/MERGE counterpart of
    /// [`Self::record_bulk_import_profile`]: one call per committed
    /// `import_relationships_batch_txn_locked` transaction, attributing its
    /// storage time to the phase that spent it. Recorded only on the attempt
    /// that commits, so every `_us` sum divides by `batches_profiled`; the
    /// per-attempt story, including the attempts that fail or retry, lives on
    /// the `shard.write_txn` span's `hydradb.relimport.*` attributes instead.
    pub(crate) fn record_relationship_import_profile(
        &self,
        profile: &crate::RelationshipImportProfile,
    ) {
        self.operation_metrics
            .relationship_import_batches_profiled
            .fetch_add(1, Ordering::Relaxed);
        self.operation_metrics
            .relationship_import_endpoint_check_us
            .fetch_add(
                duration_micros_u64(profile.endpoint_check),
                Ordering::Relaxed,
            );
        self.operation_metrics
            .relationship_import_identity_scan_us
            .fetch_add(
                duration_micros_u64(profile.identity_scan),
                Ordering::Relaxed,
            );
        self.operation_metrics
            .relationship_import_identity_pointer_hits
            .fetch_add(profile.identity_pointer_hits, Ordering::Relaxed);
        self.operation_metrics
            .relationship_import_identity_pointer_misses
            .fetch_add(profile.identity_pointer_misses, Ordering::Relaxed);
        self.operation_metrics
            .relationship_import_record_read_us
            .fetch_add(duration_micros_u64(profile.record_read), Ordering::Relaxed);
        self.operation_metrics
            .relationship_import_structural_check_us
            .fetch_add(
                duration_micros_u64(profile.structural_check),
                Ordering::Relaxed,
            );
        self.operation_metrics
            .relationship_import_segment_scans
            .fetch_add(profile.segment_scans, Ordering::Relaxed);
        self.operation_metrics
            .relationship_import_segment_neighbors
            .fetch_add(profile.segment_neighbors, Ordering::Relaxed);
        self.operation_metrics
            .relationship_import_counter_read_us
            .fetch_add(duration_micros_u64(profile.counter_read), Ordering::Relaxed);
        self.operation_metrics
            .relationship_import_commit_us
            .fetch_add(duration_micros_u64(profile.commit), Ordering::Relaxed);
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
        Ok(self
            .wait_for_storage_sequence_observed(cell_id, minimum)
            .await?
            .0)
    }

    /// [`Self::wait_for_storage_sequence`], reporting what the wait did.
    ///
    /// The sequence alone cannot distinguish the two outcomes this whole change
    /// is about — a wait that exited on its first check because this node holds
    /// the cell's writer, and a wait that spun on the object store because it
    /// does not — and both return the same number. [`BookmarkWait`] carries the
    /// difference out to the client, which is the only layer that knows which
    /// request paid for it. Change 4 of
    /// the cell-affine read-routing design.
    ///
    /// Nothing here allocates, locks or reaches the store that did not already:
    /// the writer flag falls out of the branch `durable_sequence` took anyway,
    /// and the refresh count is a local `u32`.
    pub async fn wait_for_storage_sequence_observed(
        &self,
        cell_id: &str,
        minimum: StorageSequence,
    ) -> Result<(StorageSequence, BookmarkWait)> {
        validate_component("cell_id", cell_id)?;
        let (current, served_by_cell_writer) = self.db.durable_sequence_observed().await?;
        let mut wait = BookmarkWait {
            served_by_cell_writer,
            refreshes: 0,
        };
        if current >= minimum {
            return Ok((current, wait));
        }
        // The wait gets its own budget, deliberately smaller than
        // `max_query_runtime_ms`. It used to *be* `max_query_runtime_ms`, so a
        // read whose reader could not catch up spent the entire 30s query
        // budget here and was then killed by the client watchdog at 29,999ms
        // (`src/client/service.rs`) with a phase-agnostic timeout — the
        // `SnapshotAhead` below, which names the cell and both epochs, lost
        // that race and so was almost never observed in production. Bounding
        // the wait separately makes it lose to nothing: it degrades into a
        // clear, Bolt-retriable error with the rest of the budget intact.
        // Change 3 of the cell-affine read-routing design.
        let deadline = std::time::Instant::now()
            + std::time::Duration::from_millis(self.limits.max_bookmark_wait_ms);
        loop {
            let current = self.db.refresh_durable_reader().await?;
            // Counted after the call rather than before it, so the number is
            // "manifest refreshes this wait actually performed" and a saturating
            // add keeps a pathological wait from wrapping to zero and reading as
            // the free path.
            wait.refreshes = wait.refreshes.saturating_add(1);
            if current >= minimum {
                return Ok((current, wait));
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
        let storage_snapshot = self.db.snapshot().await?;
        let current_epoch = storage_snapshot.seq();
        if read_epoch > current_epoch {
            return Err(GraphError::SnapshotAhead {
                cell_id: cell_id.to_string(),
                read_epoch,
                current_epoch,
            });
        }
        if read_epoch != current_epoch {
            return Err(GraphError::SnapshotUnavailable {
                cell_id: cell_id.to_string(),
                read_epoch,
                current_epoch,
            });
        }
        Ok(GraphSnapshot {
            shard: self,
            cell_id: cell_id.to_string(),
            read_epoch,
            storage_snapshot,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use slatedb::object_store::memory::InMemory;

    #[cfg(feature = "opencypher")]
    #[tokio::test]
    async fn independent_cache_byte_budgets_for_relationship_rows_and_native_path_results() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

        // Case 1: Relationship rows budget is generous, native path result budget is 1 byte.
        let memory1 = GraphMemoryConfig {
            max_relationship_rows_bytes: 8 * 1024 * 1024,
            max_native_path_result_bytes: 1,
            ..GraphMemoryConfig::default()
        };
        let shard1 = GraphShard::open_standalone_writer_with_memory_options(
            "graph/independent-budget-rel-generous",
            object_store.clone(),
            GraphOpenOptions::default(),
            memory1,
        )
        .await
        .unwrap();

        shard1
            .execute_cypher(
                QueryContext::new("cell-a", "init-1"),
                "CREATE (a:Entity {id: 1, name: 'alpha'})-[:RELATES]->(b:Entity {id: 2, name: 'beta'})",
            )
            .await
            .unwrap();
        shard1
            .set_vertex_metadata(
                "cell-a",
                3,
                VertexMetadata::default()
                    .with_label("Entity")
                    .with_property("name", VertexPropertyValue::String("gamma".to_string())),
            )
            .await
            .unwrap();
        shard1.build_graph_index("cell-a", "RELATES").await.unwrap();

        let rel_result = shard1
            .execute_cypher_rows(
                QueryContext::new("cell-a", "rel-q1"),
                "MATCH ({id: 1})-[r:RELATES]->({id: 2}) RETURN r._fid AS fid",
            )
            .await
            .unwrap();
        assert_eq!(rel_result.rows.len(), 1);

        let path_query = "CALL algo.MSpaths({sourceLabel: 'Entity', sourceProperty: 'name',                           sourceValues: ['alpha', 'gamma'], targetValues: ['alpha', 'gamma'],                           pairwise: true, relTypes: ['RELATES'], maxLen: 2, relDirection: 'outgoing',                           pathCount: 1, resultLimit: 10}) YIELD path RETURN path";
        let _ = shard1
            .execute_cypher_rows(QueryContext::new("cell-a", "path-q1"), path_query)
            .await
            .unwrap();

        let counts1 = shard1.graph_cache_entry_counts().await;
        let resident1 = shard1.graph_cache_resident_bytes().await;
        assert!(counts1.relationship_row_sets >= 1);
        assert!(resident1.relationship_rows > 0);
        assert_eq!(counts1.native_path_results, 0);
        assert_eq!(resident1.native_path_results, 0);
        assert_eq!(
            resident1.total(),
            resident1.matrix_adjacencies
                + resident1.graphblas_matrices
                + resident1.relationship_rows
                + resident1.source_relationship_rows
                + resident1.relationship_property_rows
        );
        shard1.close().await.unwrap();

        // Case 2: Relationship rows budget is 1 byte, native path result budget is generous.
        let memory2 = GraphMemoryConfig {
            max_relationship_rows_bytes: 1,
            max_source_relationship_rows_bytes: 1,
            max_relationship_property_rows_bytes: 1,
            max_native_path_result_bytes: 8 * 1024 * 1024,
            ..GraphMemoryConfig::default()
        };
        let shard2 = GraphShard::open_standalone_writer_with_memory_options(
            "graph/independent-budget-path-generous",
            object_store,
            GraphOpenOptions::default(),
            memory2,
        )
        .await
        .unwrap();

        shard2
            .execute_cypher(
                QueryContext::new("cell-a", "init-2"),
                "CREATE (a:Entity {id: 1, name: 'alpha'})-[:RELATES]->(b:Entity {id: 2, name: 'beta'})",
            )
            .await
            .unwrap();
        shard2
            .set_vertex_metadata(
                "cell-a",
                3,
                VertexMetadata::default()
                    .with_label("Entity")
                    .with_property("name", VertexPropertyValue::String("gamma".to_string())),
            )
            .await
            .unwrap();
        shard2.build_graph_index("cell-a", "RELATES").await.unwrap();

        let rel_result2 = shard2
            .execute_cypher_rows(
                QueryContext::new("cell-a", "rel-q2"),
                "MATCH ({id: 1})-[r:RELATES]->({id: 2}) RETURN r._fid AS fid",
            )
            .await
            .unwrap();
        assert_eq!(rel_result2.rows.len(), 1);

        let _ = shard2
            .execute_cypher_rows(QueryContext::new("cell-a", "path-q2"), path_query)
            .await
            .unwrap();

        let counts2 = shard2.graph_cache_entry_counts().await;
        let resident2 = shard2.graph_cache_resident_bytes().await;
        assert_eq!(counts2.relationship_row_sets, 0);
        assert_eq!(resident2.relationship_rows, 0);
        assert_eq!(counts2.native_path_results, 1);
        assert!(resident2.native_path_results > 0);
        assert_eq!(
            resident2.total(),
            resident2.matrix_adjacencies
                + resident2.graphblas_matrices
                + resident2.relationship_rows
                + resident2.source_relationship_rows
                + resident2.relationship_property_rows
                + resident2.native_path_results
        );
        shard2.close().await.unwrap();
    }
}
