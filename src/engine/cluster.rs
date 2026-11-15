use super::*;
use crate::keys;
#[cfg(feature = "datafusion-cypher-engine")]
use crate::QueryFailureReason;

use chrono::Utc;
use hydradb_placement::cell_writer::{self, CellWriterRecord};
use tracing::Instrument as _;

/// Stamp a failure onto the span that raised it.
///
/// §6 of the telemetry crate design: a `fencing` error seen
/// at `client.mutate` says a write failed, and the same error on
/// `placement.resolve` says why. So each site records its own class rather than
/// letting an outer span report one it did not produce.
///
/// `contention` and `fencing` are expected classes. Nothing here logs — the
/// attribute exists so a dashboard can chart the *rate*, not so each occurrence
/// pages someone.
fn record_error_class(error: &GraphError) {
    tracing::Span::current().record("error.class", error.class());
}

impl GraphCluster {
    pub async fn open_cells(
        base_path: impl Into<String>,
        cell_ids: impl IntoIterator<Item = impl Into<String>>,
        object_store: Arc<dyn ObjectStore>,
    ) -> Result<Self> {
        Self::open_cells_scoped(base_path, GraphScope::default(), cell_ids, object_store).await
    }

    pub async fn open_cells_scoped(
        base_path: impl Into<String>,
        scope: GraphScope,
        cell_ids: impl IntoIterator<Item = impl Into<String>>,
        object_store: Arc<dyn ObjectStore>,
    ) -> Result<Self> {
        Self::open_cells_scoped_with_options(base_path, scope, cell_ids, object_store, GraphOpenOptions::default()).await
    }

    pub async fn open_cells_scoped_with_options(
        base_path: impl Into<String>,
        scope: GraphScope,
        cell_ids: impl IntoIterator<Item = impl Into<String>>,
        object_store: Arc<dyn ObjectStore>,
        options: GraphOpenOptions,
    ) -> Result<Self> {
        let store_path = scope.scoped_store_path(&base_path.into());
        Self::open_cells_at_path(store_path, scope, cell_ids, object_store, false, options).await
    }

    pub async fn open_cells_standalone_writers(
        base_path: impl Into<String>,
        cell_ids: impl IntoIterator<Item = impl Into<String>>,
        object_store: Arc<dyn ObjectStore>,
    ) -> Result<Self> {
        Self::open_cells_standalone_writers_scoped(base_path, GraphScope::default(), cell_ids, object_store).await
    }

    pub async fn open_cells_standalone_writers_scoped(
        base_path: impl Into<String>,
        scope: GraphScope,
        cell_ids: impl IntoIterator<Item = impl Into<String>>,
        object_store: Arc<dyn ObjectStore>,
    ) -> Result<Self> {
        let store_path = scope.scoped_store_path(&base_path.into());
        Self::open_cells_at_path(store_path, scope, cell_ids, object_store, true, GraphOpenOptions::default()).await
    }

    async fn open_cells_at_path(
        base_path: String,
        scope: GraphScope,
        cell_ids: impl IntoIterator<Item = impl Into<String>>,
        object_store: Arc<dyn ObjectStore>,
        writable: bool,
        options: GraphOpenOptions,
    ) -> Result<Self> {
        let cell_ids = cell_ids.into_iter().map(Into::into).collect::<Vec<String>>();
        for cell_id in &cell_ids {
            validate_component("cell_id", cell_id)?;
        }
        let mut shards = BTreeMap::new();
        for cell_id in cell_ids {
            let path = format!("{base_path}/{cell_id}");
            let opened = if writable {
                GraphShard::open_standalone_writer_with_options(path, Arc::clone(&object_store), options.clone()).await
            } else {
                GraphShard::open_with_options(path, Arc::clone(&object_store), options.clone()).await
            };
            let shard = match opened {
                Ok(shard) => shard,
                Err(err) => {
                    close_shards_best_effort(shards).await;
                    return Err(err);
                }
            };
            let shard = Arc::new(shard);
            #[cfg(feature = "datafusion-cypher-engine")]
            shard.register_datafusion_self(&shard);
            shards.insert(cell_id, shard);
        }
        Ok(Self { scope, shards })
    }

    pub fn scope(&self) -> &GraphScope {
        &self.scope
    }

    pub fn shard(&self, cell_id: &str) -> Option<&GraphShard> {
        self.shards.get(cell_id).map(Arc::as_ref)
    }

    #[cfg(any(feature = "opencypher", feature = "datafusion-cypher-engine"))]
    pub(crate) fn shard_arc(&self, cell_id: &str) -> Option<Arc<GraphShard>> {
        self.shards.get(cell_id).cloned()
    }

    pub fn shard_count(&self) -> usize {
        self.shards.len()
    }

    #[cfg(feature = "opencypher")]
    pub async fn execute_cypher(&self, context: crate::QueryContext, query: &str) -> Result<crate::QueryOutput> {
        if context.scope != self.scope {
            return Err(GraphError::GraphScopeMismatch {
                expected: self.scope.to_string(),
                actual: context.scope.to_string(),
            });
        }
        let shard = self.shard_arc(&context.cell_id).ok_or_else(|| GraphError::UnknownShard {
            cell_id: context.cell_id.clone(),
        })?;
        shard.execute_cypher(context, query).await
    }

    #[cfg(feature = "opencypher")]
    pub async fn execute_cypher_rows(&self, context: crate::QueryContext, query: &str) -> Result<crate::QueryResultSet> {
        if context.scope != self.scope {
            return Err(GraphError::GraphScopeMismatch {
                expected: self.scope.to_string(),
                actual: context.scope.to_string(),
            });
        }
        let shard = self.shard_arc(&context.cell_id).ok_or_else(|| GraphError::UnknownShard {
            cell_id: context.cell_id.clone(),
        })?;
        shard.execute_cypher_rows(context, query).await
    }

    pub async fn close(&self) -> Result<()> {
        for shard in self.shards.values() {
            shard.close().await?;
        }
        Ok(())
    }
}

impl ObjectStoreNodeDirectory {
    pub fn new(
        cell_ids: impl IntoIterator<Item = impl Into<String>>,
        node_ids: impl IntoIterator<Item = impl Into<String>>,
    ) -> Result<Self> {
        let mut cells = BTreeSet::new();
        for cell_id in cell_ids {
            let cell_id = cell_id.into();
            validate_component("cell_id", &cell_id)?;
            if !cells.insert(cell_id.clone()) {
                return Err(GraphError::CorruptValue {
                    key: format!("directory/cell/{cell_id}"),
                    reason: "duplicate cell id".to_string(),
                });
            }
        }
        if cells.is_empty() {
            return Err(GraphError::CorruptValue {
                key: "directory/cells".to_string(),
                reason: "at least one cell id is required".to_string(),
            });
        }
        let mut nodes = Vec::new();
        for node_id in node_ids {
            let node_id = node_id.into();
            validate_component("node_id", &node_id)?;
            nodes.push(node_id);
        }
        nodes.sort();
        nodes.dedup();
        if nodes.is_empty() {
            return Err(GraphError::CorruptValue {
                key: "directory/nodes".to_string(),
                reason: "at least one node id is required".to_string(),
            });
        }
        Ok(Self {
            cells,
            nodes: nodes.into_iter().collect(),
        })
    }

    pub fn cells(&self) -> impl Iterator<Item = &str> {
        self.cells.iter().map(String::as_str)
    }

    pub fn node_ids(&self) -> impl Iterator<Item = &str> {
        self.nodes.iter().map(String::as_str)
    }

    pub fn contains_cell(&self, cell_id: &str) -> Result<bool> {
        validate_component("cell_id", cell_id)?;
        Ok(self.cells.contains(cell_id))
    }

    pub fn contains_node(&self, node_id: &str) -> Result<bool> {
        validate_component("node_id", node_id)?;
        Ok(self.nodes.contains(node_id))
    }
}

struct RoutedClusterOpenConfig {
    base_path: String,
    scope: GraphScope,
    local_node_id: String,
    directory: ObjectStoreNodeDirectory,
    placement: PlacementView,
    object_store: Arc<dyn ObjectStore>,
    writer_registry: Option<Arc<ProcessWriterRegistry>>,
    promotable: bool,
    options: GraphOpenOptions,
    memory: GraphMemoryConfig,
    writer_leases: Option<Arc<ObjectStoreWriterLeaseDirectory>>,
    writer_gate: crate::WriterGate,
    cache_totals: BTreeMap<String, Arc<crate::GraphCacheMetrics>>,
    operational_totals: Option<Arc<crate::CumulativeOperationalCounters>>,
}

impl RoutedGraphCluster {
    pub async fn open_readers(
        base_path: impl Into<String>,
        local_node_id: impl Into<String>,
        directory: ObjectStoreNodeDirectory,
        placement: PlacementView,
        object_store: Arc<dyn ObjectStore>,
    ) -> Result<Self> {
        Self::open_readers_scoped(base_path, GraphScope::default(), local_node_id, directory, placement, object_store).await
    }

    pub async fn open_promotable(
        base_path: impl Into<String>,
        local_node_id: impl Into<String>,
        directory: ObjectStoreNodeDirectory,
        placement: PlacementView,
        object_store: Arc<dyn ObjectStore>,
    ) -> Result<Self> {
        Self::open_promotable_scoped_with_memory_options(
            base_path,
            GraphScope::default(),
            local_node_id,
            directory,
            placement,
            object_store,
            GraphOpenOptions::default(),
            GraphMemoryConfig::default(),
        )
        .await
    }

    pub async fn open_promotable_with_options(
        base_path: impl Into<String>,
        local_node_id: impl Into<String>,
        directory: ObjectStoreNodeDirectory,
        placement: PlacementView,
        object_store: Arc<dyn ObjectStore>,
        options: GraphOpenOptions,
    ) -> Result<Self> {
        Self::open_promotable_scoped_with_memory_options(
            base_path,
            GraphScope::default(),
            local_node_id,
            directory,
            placement,
            object_store,
            options,
            GraphMemoryConfig::default(),
        )
        .await
    }

    pub async fn open_readers_scoped(
        base_path: impl Into<String>,
        scope: GraphScope,
        local_node_id: impl Into<String>,
        directory: ObjectStoreNodeDirectory,
        placement: PlacementView,
        object_store: Arc<dyn ObjectStore>,
    ) -> Result<Self> {
        let base_path = scope.scoped_store_path(&base_path.into());
        Self::open_at_path(RoutedClusterOpenConfig {
            base_path,
            scope,
            local_node_id: local_node_id.into(),
            directory,
            placement,
            object_store,
            writer_registry: None,
            promotable: false,
            options: GraphOpenOptions::default(),
            memory: GraphMemoryConfig::default(),
            writer_leases: None,
            writer_gate: crate::WriterGate::default(),
            cache_totals: BTreeMap::new(),
            operational_totals: None,
        })
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn open_promotable_scoped_with_memory_options(
        base_path: impl Into<String>,
        scope: GraphScope,
        local_node_id: impl Into<String>,
        directory: ObjectStoreNodeDirectory,
        placement: PlacementView,
        object_store: Arc<dyn ObjectStore>,
        options: GraphOpenOptions,
        memory: GraphMemoryConfig,
    ) -> Result<Self> {
        let writer_registry = process_writer_registry(&object_store);
        let base_path = base_path.into();
        let writer_leases =
            super::writer_lease::process_writer_lease_directory(base_path.clone(), Arc::clone(&object_store), Duration::from_secs(30));
        Self::open_promotable_scoped_with_writer_registry(
            base_path,
            scope,
            local_node_id,
            directory,
            placement,
            object_store,
            writer_registry,
            writer_leases,
            crate::WriterGate::default(),
            options,
            memory,
            BTreeMap::new(),
            None,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn open_promotable_scoped_with_writer_registry(
        base_path: impl Into<String>,
        scope: GraphScope,
        local_node_id: impl Into<String>,
        directory: ObjectStoreNodeDirectory,
        placement: PlacementView,
        object_store: Arc<dyn ObjectStore>,
        writer_registry: Arc<ProcessWriterRegistry>,
        writer_leases: Arc<ObjectStoreWriterLeaseDirectory>,
        writer_gate: crate::WriterGate,
        options: GraphOpenOptions,
        memory: GraphMemoryConfig,
        cache_totals: BTreeMap<String, Arc<crate::GraphCacheMetrics>>,
        operational_totals: Option<Arc<crate::CumulativeOperationalCounters>>,
    ) -> Result<Self> {
        let base_path = scope.scoped_store_path(&base_path.into());
        Self::open_at_path(RoutedClusterOpenConfig {
            base_path,
            scope,
            local_node_id: local_node_id.into(),
            directory,
            placement,
            object_store,
            writer_registry: Some(writer_registry),
            promotable: true,
            options,
            memory,
            writer_leases: Some(writer_leases),
            writer_gate,
            cache_totals,
            operational_totals,
        })
        .await
    }

    async fn open_at_path(config: RoutedClusterOpenConfig) -> Result<Self> {
        let RoutedClusterOpenConfig {
            base_path,
            scope,
            local_node_id,
            directory,
            placement,
            object_store,
            writer_registry,
            promotable,
            options,
            memory,
            writer_leases,
            writer_gate,
            cache_totals,
            operational_totals,
        } = config;
        validate_component("node_id", &local_node_id)?;
        if !directory.contains_node(&local_node_id)? {
            return Err(GraphError::CorruptValue {
                key: format!("directory/node/{local_node_id}"),
                reason: "local node is not present in the object-store node directory".to_string(),
            });
        }
        // A placement view answers `Local` or `Remote` relative to *its* node
        // id, so a handle built for another node would have this one refuse
        // every write it owns and promote itself for every cell it does not —
        // and nothing about either symptom points at the mismatch.
        if placement.local_node_id() != local_node_id {
            return Err(GraphError::CorruptValue {
                key: format!("directory/node/{local_node_id}"),
                reason: format!(
                    "placement view belongs to node {}, not to this cluster's node",
                    placement.local_node_id()
                ),
            });
        }
        for cell_id in directory.cells() {
            validate_component("cell_id", cell_id)?;
        }
        if let Some(writer_leases) = &writer_leases {
            writer_leases.register_scope(&scope, &local_node_id);
        }
        let mut shards = BTreeMap::new();
        let mut operational_registrations = Vec::new();
        for cell_id in directory.cells().map(str::to_string) {
            let path = format!("{base_path}/{cell_id}");
            let opened = if promotable {
                GraphShard::open_promotable_for_node_with_memory_options(
                    path,
                    Arc::clone(&object_store),
                    options.clone(),
                    memory.clone(),
                    &local_node_id,
                    Arc::clone(writer_registry.as_ref().expect("promotable cluster must retain a writer registry")),
                )
                .await
            } else {
                GraphShard::open_with_memory_options(path, Arc::clone(&object_store), options.clone(), memory.clone()).await
            };
            let mut shard = match opened {
                Ok(shard) => shard,
                Err(err) => {
                    close_routed_shards_best_effort(shards).await;
                    if let Some(writer_leases) = &writer_leases {
                        let _ = writer_leases.release_scope(&scope, &local_node_id).await;
                    }
                    return Err(err);
                }
            };
            if !promotable && shard.read_remote(&keys::cell_drop_marker(&cell_id)).await?.is_some() {
                shard.close().await?;
                continue;
            }
            if let Some(totals) = cache_totals.get(&cell_id) {
                shard.cache_metrics = Arc::new(crate::GraphCacheMetrics::with_totals(Arc::clone(totals)));
            }
            if let Some(totals) = &operational_totals {
                operational_registrations.push(totals.register(cell_id.clone(), Arc::clone(&shard.operation_metrics)));
            }
            let shard = shard.into_shared();
            #[cfg(feature = "datafusion-cypher-engine")]
            shard.register_datafusion_self(&shard);
            shards.insert(cell_id, shard);
        }
        Ok(Self {
            scope,
            local_node_id,
            directory,
            placement,
            writer_leases,
            writer_lease_registration_active: std::sync::atomic::AtomicBool::new(promotable),
            #[cfg(feature = "query-transport")]
            scope_registration: tokio::sync::OnceCell::new(),
            shards,
            promotable,
            writer_gate,
            operational_registrations,
        })
    }

    pub fn local_node_id(&self) -> &str {
        &self.local_node_id
    }
    /// The shared placement handle, for a caller that must hand the same live
    /// set to another reader (the Bolt routing provider) rather than build a
    /// second one.
    pub fn placement(&self) -> &PlacementView {
        &self.placement
    }
    pub fn scope(&self) -> &GraphScope {
        &self.scope
    }
    pub fn directory(&self) -> &ObjectStoreNodeDirectory {
        &self.directory
    }
    pub fn local_cells(&self) -> Vec<&str> {
        self.shards.keys().map(String::as_str).collect()
    }
    pub async fn local_shard_runtime_metrics(&self) -> Vec<GraphShardRuntimeMetrics> {
        let mut metrics = Vec::with_capacity(self.shards.len());
        for (cell_id, shard) in &self.shards {
            let write_permit_capacity = shard.graph_write_permit_capacity;
            metrics.push(GraphShardRuntimeMetrics {
                cell_id: cell_id.clone(),
                write_permits_in_use: write_permit_capacity.saturating_sub(shard.graph_write_gate.available_permits()),
                write_permit_capacity,
                operational: shard.graph_operational_metrics(),
                cache: shard.graph_cache_metrics(),
                cache_entries: shard.graph_cache_entry_counts().await,
                cache_resident_bytes: shard.graph_cache_resident_bytes().await,
                storage: shard.graph_storage_metrics(),
            });
        }
        metrics
    }

    pub fn shard(&self, cell_id: &str) -> Result<&GraphShard> {
        validate_component("cell_id", cell_id)?;
        self.shards.get(cell_id).map(Arc::as_ref).ok_or_else(|| GraphError::UnknownShard {
            cell_id: cell_id.to_string(),
        })
    }

    /// [`Self::shard`] as an owned handle, for work that outlives the call.
    #[cfg(feature = "opencypher")]
    pub(crate) fn shard_arc(&self, cell_id: &str) -> Result<Arc<GraphShard>> {
        validate_component("cell_id", cell_id)?;
        self.shards.get(cell_id).cloned().ok_or_else(|| GraphError::UnknownShard {
            cell_id: cell_id.to_string(),
        })
    }

    #[cfg(feature = "query-transport")]
    async fn retire_cell_writer(&self, cell_id: &str) -> Result<bool> {
        self.shard(cell_id)?.retire_writer().await
    }

    #[cfg(feature = "query-transport")]
    fn holds_unowned_writer(&self) -> bool {
        self.shards.iter().any(|(cell_id, shard)| {
            shard.storage.writer_epoch().is_some()
                && !self
                    .writer_leases
                    .as_ref()
                    .is_some_and(|leases| leases.holds_valid_local(&self.scope, cell_id, &self.local_node_id))
        })
    }

    /// The gate every routed write passes through, and the one branch that ends
    /// the writer duel.
    ///
    /// > A node must not promote itself for a cell it does not own, unless the
    /// > computed owner is not live.
    ///
    /// Touch point (b) of the rendezvous placement design.
    /// Before this existed, any node opened the SlateDB writer on demand, which
    /// fenced whoever held it; the fenced node's next write took it straight
    /// back, and three nodes traded one cell's epoch forever. Rendezvous decides
    /// which of them has a *reason* to hold it, and the other two decline here.
    ///
    /// # Ownership first, pacing second
    ///
    /// The order is deliberate. A node that does not own the cell must be
    /// refused outright, not merely asked to wait — a wait it would spend and
    /// then promote anyway is the duel with extra latency. Only once ownership
    /// is settled does the fence backoff (touch point (d)) apply.
    ///
    /// [`CellOwnership::Unowned`] and [`CellOwnership::Unknown`] both mean
    /// "rendezvous named nobody" and have opposite answers, which is why they
    /// are matched separately here rather than through an `Option`: an empty
    /// live set licenses promotion, a *shed* view forbids it.
    ///
    /// # What the span is for
    ///
    /// `writer.acquire` is the span the whole of §4 exists to produce. The
    /// `cell-writer-fencing-pingpong` incident — three nodes trading one cell's
    /// epoch — is invisible as scattered `Fenced` errors and obvious as a span
    /// tree: three traces, three `hydradb.node_id`s, one `hydradb.cell_id`,
    /// and `hydradb.writer.epoch` climbing on every acquisition. The epoch is
    /// recorded after the promotion because that is the only moment it is known.
    pub(crate) async fn ensure_local_writer(&self, cell_id: &str) -> Result<()> {
        let span = tracing::info_span!(
            "writer.acquire",
            hydradb.scope = %self.scope,
            hydradb.cell_id = %cell_id,
            hydradb.node_id = %self.local_node_id,
            hydradb.writer.epoch = tracing::field::Empty,
            hydradb.writer.lease_generation = tracing::field::Empty,
            error.class = tracing::field::Empty,
        );
        self.ensure_local_writer_traced(cell_id).instrument(span).await
    }

    /// The body of [`Self::ensure_local_writer`], running inside its span.
    ///
    /// Split out only so the span can wrap an `async fn` without an inline
    /// `async` block swallowing the borrow checker's diagnostics; the control
    /// flow is unchanged.
    async fn ensure_local_writer_traced(&self, cell_id: &str) -> Result<()> {
        validate_component("cell_id", cell_id).inspect_err(record_error_class)?;
        if !self.promotable {
            let error = GraphError::WriteRequiresWriter {
                operation: "routed_write",
                cell_id: cell_id.to_string(),
            };
            record_error_class(&error);
            return Err(error);
        }
        // The object store has not passed the create-only probe: fencing
        // cannot stop a superseded writer there, so take no writer at all.
        // No owner hint, as for a shed view: this node knows nothing useful
        // about who should write, only that it must not.
        if !self.writer_gate.is_open() {
            let error = GraphError::NotCellWriter {
                cell_id: cell_id.to_string(),
                owner: None,
            };
            record_error_class(&error);
            return Err(error);
        }
        let shard = self
            .shards
            .get(cell_id)
            .ok_or_else(|| GraphError::UnknownShard {
                cell_id: cell_id.to_string(),
            })
            .inspect_err(record_error_class)?;

        let writer_leases = self.writer_leases.as_ref().expect("promotable routed clusters have writer leases");
        // Placement is the stable routing preference, while the object-store
        // lease is the authority. An existing lease holder must keep serving
        // through a temporarily divergent heartbeat view; a node without a
        // lease may contend only when rendezvous currently selects it.
        let can_resume_durable_lease = writer_leases.holds_valid_local(&self.scope, cell_id, &self.local_node_id)
            || writer_leases.holds_abandoned_local(&self.scope, cell_id, &self.local_node_id);
        if !can_resume_durable_lease {
            self.resolve_placement(cell_id)?;
        }
        // Serve any local fencing backoff before acquiring durable ownership so
        // the wait cannot consume the lease window before SlateDB opens.
        self.await_writer_reopen(shard, cell_id).await?;
        let lease_generation = writer_leases
            .acquire_or_renew(&self.scope, cell_id, &self.local_node_id)
            .await
            .inspect_err(record_error_class)?;
        tracing::Span::current().record("hydradb.writer.lease_generation", lease_generation);

        // Whether this shard already held a writer, taken *before* the call.
        // `promote_to_writer` is idempotent and says nothing about which of the
        // two it did, and the difference is the whole of "after a successful
        // promotion, and only then": a shard that was already writing claimed no
        // epoch, so there is nothing new to record and no PUT to make. Without
        // this check the record would be rewritten on every single write, which
        // is precisely the object-store request on the write path that decision
        // 3 refuses to add.
        let already_writing = shard.storage.writer_epoch().is_some();
        if let Err(error) = shard.promote_to_writer(cell_id, "routed_write").await {
            if let Err(release_error) = writer_leases.release_cell(&self.scope, cell_id, &self.local_node_id).await {
                tracing::warn!(
                    %release_error,
                    %cell_id,
                    "failed to release writer lease after SlateDB promotion failure"
                );
            }
            return Err(error);
        }
        if !writer_leases.holds_valid_local(&self.scope, cell_id, &self.local_node_id) {
            if let Err(error) = shard.retire_writer().await {
                tracing::warn!(%error, %cell_id, "failed to retire writer after lease expired during promotion");
            }
            let error = GraphError::NotCellWriter {
                cell_id: cell_id.to_string(),
                owner: writer_leases
                    .current_owner(&self.scope, cell_id)
                    .await
                    .ok()
                    .flatten()
                    .map(|owner| owner.node_id),
            };
            record_error_class(&error);
            return Err(error);
        }
        if let Some(epoch) = shard.storage.writer_epoch() {
            // The climbing value in the ping-pong query. Taken from the
            // manifest, which is the authority; the advisory record written
            // below only ever repeats it.
            tracing::Span::current().record("hydradb.writer.epoch", epoch);
        }
        if !already_writing {
            self.record_cell_writer(shard, cell_id).await;
        }
        Ok(())
    }

    /// Ask rendezvous who owns the cell, as its own span.
    ///
    /// Synchronous and I/O-free by decision 10, so the span costs a timestamp
    /// pair. It earns that because the refusal it may return is the single most
    /// common write-path `fencing` error, and a class recorded here says
    /// "rendezvous named somebody else" rather than the far vaguer "a write
    /// failed" that the same error carries at the root.
    fn resolve_placement(&self, cell_id: &str) -> Result<()> {
        let span = tracing::info_span!(
            "placement.resolve",
            hydradb.scope = %self.scope,
            hydradb.cell_id = %cell_id,
            hydradb.node_id = %self.local_node_id,
            hydradb.placement.ownership = tracing::field::Empty,
            error.class = tracing::field::Empty,
        );
        let _entered = span.enter();
        let ownership = self.placement.ownership(&self.scope.to_string(), cell_id);
        // Recorded for all four answers, not just the refusals. "How often does
        // a write reach a node that does not own the cell" is the question the
        // write-routing work exists to answer, and it cannot be answered from
        // failures alone — a rate needs its denominator, and the successful
        // `local` case is the denominator.
        span.record(
            "hydradb.placement.ownership",
            match &ownership {
                CellOwnership::Local => "local",
                CellOwnership::Remote { .. } => "remote",
                CellOwnership::Unowned => "unowned",
                CellOwnership::Unknown => "unknown",
            },
        );
        match ownership {
            // The rendezvous owner, or a known-empty fleet with no owner to
            // defer to. Either way this node may hold the writer.
            CellOwnership::Local | CellOwnership::Unowned => Ok(()),
            // A live peer owns the cell. Name it, so the driver re-routes
            // instead of retrying into the same wrong node.
            CellOwnership::Remote { node_id } => Err(GraphError::NotCellWriter {
                cell_id: cell_id.to_string(),
                owner: Some(node_id),
            })
            .inspect_err(record_error_class),
            // This node has shed its view (decision 7) and knows nothing about
            // the fleet. Refuse with no hint: a stale guess would point at a
            // node that may itself have shed, and promoting here is the
            // unbounded duel — a partitioned node can never learn it lost.
            CellOwnership::Unknown => Err(GraphError::NotCellWriter {
                cell_id: cell_id.to_string(),
                owner: None,
            })
            .inspect_err(record_error_class),
        }
    }

    /// Write down that this node just took `cell_id`'s writer, for whoever reads
    /// the next incident's logs.
    ///
    /// Option 3a of decision 3 in
    /// the rendezvous placement design, and the only place the
    /// record is written. Three properties are load-bearing, and each is
    /// deliberate rather than incidental:
    ///
    /// - **It returns nothing.** A failed PUT cannot fail the promotion, because
    ///   the promotion has already succeeded — the epoch is claimed and the
    ///   write is about to proceed. Turning a diagnostics write into an
    ///   availability dependency on a second object-store request would be
    ///   strictly worse than keeping no record at all, so the error is logged at
    ///   `warn!` and dropped.
    /// - **It runs after the promotion, never before.** Written first it would
    ///   be a claim, and a claim that is read back is option 3b — the
    ///   read-then-act race decision 3 rejects.
    /// - **Nothing reads it here.** The promote path performs no GET. Ownership
    ///   was settled above by rendezvous, synchronously and from the shared live
    ///   set; this record is never an input to that decision. The epoch comes
    ///   from the manifest of the writer just opened, which remains the only
    ///   authority on who holds it.
    async fn record_cell_writer(&self, shard: &GraphShard, cell_id: &str) {
        let Some(epoch) = shard.storage.writer_epoch() else {
            // Racing a fence between the promotion and this line. The record
            // would name an epoch this node no longer holds, so write nothing.
            tracing::debug!(
                node_id = %self.local_node_id,
                cell_id,
                "promoted writer was gone before it could be recorded"
            );
            return;
        };
        let Some((base, _)) = shard.storage.cell_location() else {
            tracing::warn!(
                node_id = %self.local_node_id,
                cell_id,
                path = %shard.storage.store_path(),
                "no base path to write the advisory cell-writer record under"
            );
            return;
        };
        // The one clock read on this path, and it is here rather than inside
        // `CellWriterRecord` by decision 10.
        let record = CellWriterRecord::new(&self.local_node_id, epoch, Utc::now());
        if let Err(error) = cell_writer::put_cell_writer(shard.storage.object_store().as_ref(), &base, cell_id, &record).await {
            // Advisory: the write path is already past this point.
            tracing::warn!(
                node_id = %self.local_node_id,
                cell_id,
                epoch,
                %error,
                "could not record the advisory cell writer; the promotion stands"
            );
        }
    }

    /// Touch point (d)'s backoff, enforced where the promotion happens.
    ///
    /// `refresh_writer_fence` records the wait and returns; nothing re-opens a
    /// writer until it comes back through here, so this is the one place the
    /// pacing can be applied without walking past the ownership check above.
    ///
    /// # Waiting rather than refusing, and the cap on it
    ///
    /// This node is the owner, so there is nowhere better to send the write:
    /// refusing would hand the driver an error it can only answer by retrying
    /// into this same node, turning a bounded local wait into an unbounded
    /// client-side one. So the common case — a fence, which arms exactly one
    /// `heartbeat_interval` sized to let the rival refresh its view and stand
    /// down — is waited out and the caller's write then succeeds on its
    /// original request.
    ///
    /// The wait is capped at that same interval because the gate is also armed
    /// by *plain* re-open failures, whose ladder climbs to a minute. Holding a
    /// Bolt request open for a minute is worse for the client than telling it
    /// to come back, so past the cap this refuses with a retryable
    /// `AdmissionRejected` instead. The cap costs nothing in the case decision
    /// 6 actually sizes: a fence never asks for longer than one interval.
    async fn await_writer_reopen(&self, shard: &GraphShard, cell_id: &str) -> Result<()> {
        // A shard that already holds its writer is not re-opening anything, so
        // the gate does not apply to it — only a promotion that would really
        // open a writer is paced.
        if shard.storage.writer().is_ok() {
            return Ok(());
        }
        let Some(delay) = shard.storage.writer_reopen_delay() else {
            return Ok(());
        };
        let cap = self.placement.config().heartbeat_interval;
        let span = tracing::info_span!(
            "writer.reopen_wait",
            hydradb.node_id = %self.local_node_id,
            hydradb.cell_id = %cell_id,
            hydradb.writer.reopen_delay_ms = delay.as_millis(),
            hydradb.writer.reopen_cap_ms = cap.as_millis(),
            hydradb.outcome = tracing::field::Empty,
            error.class = tracing::field::Empty,
        );
        let result = async {
            if delay > cap {
                let error = GraphError::AdmissionRejected {
                    operation: "writer_reopen",
                    actual: delay.as_millis() as u64,
                    limit: cap.as_millis() as u64,
                };
                record_error_class(&error);
                tracing::Span::current().record("hydradb.outcome", "failed");
                tracing::warn!(
                    operation = "writer_reopen",
                    delay_ms = delay.as_millis(),
                    cap_ms = cap.as_millis(),
                    error.class = error.class(),
                    "writer re-open delay exceeded the request wait cap"
                );
                return Err(error);
            }
            tracing::info!(
                operation = "writer_reopen",
                delay_ms = delay.as_millis(),
                cap_ms = cap.as_millis(),
                "pacing a writer re-open"
            );
            tokio::time::sleep(delay).await;
            tracing::Span::current().record("hydradb.outcome", "success");
            Ok(())
        }
        .instrument(span)
        .await;
        if let Err(error) = &result {
            // The nested span says where the rejection arose; the enclosing
            // `writer.acquire` span must still report that the acquisition
            // itself failed.
            record_error_class(error);
        }
        result
    }

    pub async fn write_edge(&self, mutation: crate::EdgeMutation) -> Result<crate::CommitResult> {
        let shard = self.shard(&mutation.cell_id)?;
        self.ensure_local_writer(&mutation.cell_id).await?;
        shard.write_edge(mutation).await
    }

    pub async fn delete_edge(&self, mutation: crate::EdgeMutation) -> Result<crate::DeleteResult> {
        let shard = self.shard(&mutation.cell_id)?;
        self.ensure_local_writer(&mutation.cell_id).await?;
        shard.delete_edge(mutation).await
    }

    pub async fn delete_edges_batch(
        &self,
        cell_id: &str,
        edge_type: &str,
        edges: impl IntoIterator<Item = (VertexId, VertexId)>,
        idempotency_key: &str,
    ) -> Result<crate::EdgeDeleteBatchResult> {
        let shard = self.shard(cell_id)?;
        self.ensure_local_writer(cell_id).await?;
        shard.delete_edges_batch(cell_id, edge_type, edges, idempotency_key).await
    }

    pub async fn delete_edges_batch_chunked(
        &self,
        cell_id: &str,
        edge_type: &str,
        edges: impl IntoIterator<Item = (VertexId, VertexId)>,
        idempotency_key: &str,
        chunk_size: usize,
    ) -> Result<crate::EdgeDeleteBatchResult> {
        let shard = self.shard(cell_id)?;
        self.ensure_local_writer(cell_id).await?;
        shard
            .delete_edges_batch_chunked(cell_id, edge_type, edges, idempotency_key, chunk_size)
            .await
    }

    pub async fn delete_edge_mutations_batch(
        &self,
        cell_id: &str,
        mutations: impl IntoIterator<Item = crate::EdgeMutation>,
    ) -> Result<crate::EdgeDeleteBatchResult> {
        let shard = self.shard(cell_id)?;
        self.ensure_local_writer(cell_id).await?;
        shard.delete_edge_mutations_batch(cell_id, mutations).await
    }

    pub async fn delete_vertex(
        &self,
        cell_id: &str,
        vertex_id: crate::VertexId,
        idempotency_key: &str,
    ) -> Result<crate::VertexDeleteResult> {
        let shard = self.shard(cell_id)?;
        self.ensure_local_writer(cell_id).await?;
        shard.delete_vertex(cell_id, vertex_id, idempotency_key).await
    }

    pub async fn detach_delete_vertex(
        &self,
        cell_id: &str,
        vertex_id: crate::VertexId,
        idempotency_key: &str,
    ) -> Result<crate::VertexDeleteResult> {
        let shard = self.shard(cell_id)?;
        self.ensure_local_writer(cell_id).await?;
        shard.detach_delete_vertex(cell_id, vertex_id, idempotency_key).await
    }

    pub async fn drop_cell(&self, cell_id: &str, idempotency_key: &str) -> Result<crate::GraphCellDropResult> {
        let shard = self.shard(cell_id)?;
        self.ensure_local_writer(cell_id).await?;
        shard.drop_cell(cell_id, idempotency_key).await
    }

    pub async fn ingest_edge_mutations(
        &self,
        cell_id: &str,
        mutations: impl IntoIterator<Item = crate::EdgeMutation>,
        options: crate::EdgeIngestOptions,
    ) -> Result<crate::EdgeIngestResult> {
        let shard = self.shard(cell_id)?;
        self.ensure_local_writer(cell_id).await?;
        shard.ingest_edge_mutations(cell_id, mutations, options).await
    }

    pub async fn bulk_import_edges(
        &self,
        cell_id: &str,
        edge_type: &str,
        edges: impl IntoIterator<Item = (VertexId, VertexId)>,
        idempotency_key: &str,
    ) -> Result<crate::BulkImportResult> {
        let shard = self.shard(cell_id)?;
        self.ensure_local_writer(cell_id).await?;
        shard.bulk_import_edges(cell_id, edge_type, edges, idempotency_key).await
    }

    pub async fn write_edges_batch(
        &self,
        cell_id: &str,
        edge_type: &str,
        edges: impl IntoIterator<Item = (VertexId, VertexId)>,
        idempotency_key: &str,
    ) -> Result<crate::BulkImportResult> {
        let shard = self.shard(cell_id)?;
        self.ensure_local_writer(cell_id).await?;
        shard.write_edges_batch(cell_id, edge_type, edges, idempotency_key).await
    }

    pub async fn write_edges_batch_chunked(
        &self,
        cell_id: &str,
        edge_type: &str,
        edges: impl IntoIterator<Item = (VertexId, VertexId)>,
        idempotency_key: &str,
        chunk_size: usize,
    ) -> Result<crate::BulkImportResult> {
        let shard = self.shard(cell_id)?;
        self.ensure_local_writer(cell_id).await?;
        shard
            .write_edges_batch_chunked(cell_id, edge_type, edges, idempotency_key, chunk_size)
            .await
    }

    pub async fn set_vertex_metadata(&self, cell_id: &str, vertex_id: crate::VertexId, metadata: crate::VertexMetadata) -> Result<()> {
        let shard = self.shard(cell_id)?;
        self.ensure_local_writer(cell_id).await?;
        shard.set_vertex_metadata(cell_id, vertex_id, metadata).await
    }

    pub async fn set_vertex_metadata_batch(
        &self,
        cell_id: &str,
        updates: impl IntoIterator<Item = (crate::VertexId, crate::VertexMetadata)>,
    ) -> Result<usize> {
        let shard = self.shard(cell_id)?;
        self.ensure_local_writer(cell_id).await?;
        shard.set_vertex_metadata_batch(cell_id, updates).await
    }

    pub async fn import_vertex_metadata_batch(
        &self,
        cell_id: &str,
        updates: impl IntoIterator<Item = (crate::VertexId, crate::VertexMetadata)>,
    ) -> Result<usize> {
        let shard = self.shard(cell_id)?;
        self.ensure_local_writer(cell_id).await?;
        shard.import_vertex_metadata_batch(cell_id, updates).await
    }

    pub async fn set_edge_metadata(
        &self,
        cell_id: &str,
        edge_type: &str,
        src: crate::VertexId,
        dst: crate::VertexId,
        metadata: crate::EdgeMetadata,
    ) -> Result<bool> {
        let shard = self.shard(cell_id)?;
        self.ensure_local_writer(cell_id).await?;
        shard.set_edge_metadata(cell_id, edge_type, src, dst, metadata).await
    }

    pub async fn set_edge_metadata_batch(
        &self,
        cell_id: &str,
        edge_type: &str,
        updates: impl IntoIterator<Item = (crate::VertexId, crate::VertexId, crate::EdgeMetadata)>,
    ) -> Result<usize> {
        let shard = self.shard(cell_id)?;
        self.ensure_local_writer(cell_id).await?;
        shard.set_edge_metadata_batch(cell_id, edge_type, updates).await
    }

    pub async fn import_relationships_batch(
        &self,
        cell_id: &str,
        edge_type: &str,
        relationships: impl IntoIterator<Item = crate::RelationshipMutation>,
        idempotency_key: &str,
    ) -> Result<crate::RelationshipImportResult> {
        let shard = self.shard(cell_id)?;
        self.ensure_local_writer(cell_id).await?;
        shard
            .import_relationships_batch(cell_id, edge_type, relationships, idempotency_key)
            .await
    }

    pub async fn write_edge_mutations_batch(
        &self,
        cell_id: &str,
        mutations: impl IntoIterator<Item = crate::EdgeMutation>,
    ) -> Result<crate::EdgeMutationBatchResult> {
        let shard = self.shard(cell_id)?;
        self.ensure_local_writer(cell_id).await?;
        shard.write_edge_mutations_batch(cell_id, mutations).await
    }

    pub async fn out_neighbors_batch(
        &self,
        cell_id: &str,
        edge_type: &str,
        sources: impl IntoIterator<Item = VertexId>,
    ) -> Result<Vec<crate::NeighborBatchEntry>> {
        self.shard(cell_id)?.out_neighbors_batch(cell_id, edge_type, sources).await
    }

    pub async fn in_neighbors_batch(
        &self,
        cell_id: &str,
        edge_type: &str,
        destinations: impl IntoIterator<Item = VertexId>,
    ) -> Result<Vec<crate::NeighborBatchEntry>> {
        self.shard(cell_id)?.in_neighbors_batch(cell_id, edge_type, destinations).await
    }

    pub async fn edge_exists_batch(
        &self,
        cell_id: &str,
        edge_type: &str,
        edges: impl IntoIterator<Item = (VertexId, VertexId)>,
    ) -> Result<Vec<crate::EdgeExistenceBatchEntry>> {
        self.shard(cell_id)?.edge_exists_batch(cell_id, edge_type, edges).await
    }

    pub async fn execute_query_statement(
        &self,
        context: crate::QueryContext,
        statement: crate::QueryStatement,
    ) -> Result<crate::QueryOutput> {
        self.ensure_query_scope(&context.scope)?;
        let shard = self.shard(&context.cell_id)?;
        let plan = shard.plan_query_statement(context, statement)?;
        if plan.is_write() {
            self.ensure_local_writer(&plan.cell_id).await?;
        }
        shard.execute_query_plan(plan).await
    }

    pub async fn execute_query_plan(&self, plan: crate::QueryPlan) -> Result<crate::QueryOutput> {
        let shard = self.shard(&plan.cell_id)?;
        if plan.is_write() {
            self.ensure_local_writer(&plan.cell_id).await?;
        }
        shard.execute_query_plan(plan).await
    }

    #[cfg(feature = "opencypher")]
    pub async fn execute_cypher(&self, context: crate::QueryContext, query: &str) -> Result<crate::QueryOutput> {
        self.ensure_query_scope(&context.scope)?;
        #[cfg(feature = "datafusion-cypher-engine")]
        if context.cypher_engine == crate::CypherEngineMode::DataFusion {
            let shard = self.shard_arc(&context.cell_id)?;
            let prepared = shard.prepare_datafusion_cypher(&context, query).await?;
            if prepared.effect == crate::PreparedCypherEffect::Write {
                self.ensure_local_writer(&context.cell_id).await?;
            }
            return shard.execute_datafusion_cypher(context, query).await;
        }
        let shard = self.shard(&context.cell_id)?;
        let requires_writer = matches!(
            crate::query::opencypher::classify_opencypher_query_access(query)?,
            crate::query::opencypher::OpenCypherQueryAccess::Write
        );
        if requires_writer {
            self.ensure_local_writer(&context.cell_id).await?;
            let shard = self.shard_arc(&context.cell_id)?;
            let query = query.to_string();
            return run_write_to_completion(async move { shard.execute_cypher(context, &query).await }).await;
        }
        shard.execute_cypher(context, query).await
    }

    #[cfg(feature = "opencypher")]
    pub async fn execute_cypher_rows(&self, context: crate::QueryContext, query: &str) -> Result<crate::QueryResultSet> {
        self.ensure_query_scope(&context.scope)?;
        #[cfg(feature = "datafusion-cypher-engine")]
        if context.cypher_engine == crate::CypherEngineMode::DataFusion {
            let shard = self.shard_arc(&context.cell_id)?;
            let prepared = shard.prepare_datafusion_cypher(&context, query).await?;
            if prepared.effect == crate::PreparedCypherEffect::Write {
                self.ensure_local_writer(&context.cell_id).await?;
            }
            return shard.execute_datafusion_cypher_rows(context, query).await;
        }
        let shard = self.shard(&context.cell_id)?;
        shard.execute_cypher_rows(context, query).await
    }

    #[cfg(feature = "opencypher")]
    pub async fn execute_cypher_rows_page(
        &self,
        context: crate::QueryContext,
        query: &str,
        cursor: Option<crate::QueryCursorToken>,
        page_size: usize,
    ) -> Result<crate::QueryResultPage> {
        self.ensure_query_scope(&context.scope)?;
        #[cfg(feature = "datafusion-cypher-engine")]
        if context.cypher_engine == crate::CypherEngineMode::DataFusion {
            let shard = self.shard_arc(&context.cell_id)?;
            let prepared = shard.prepare_datafusion_cypher(&context, query).await?;
            if prepared.effect == crate::PreparedCypherEffect::Write {
                if cursor.is_some() {
                    return Err(GraphError::UnsupportedQuery {
                        reason: QueryFailureReason::InvalidRequest,
                        dialect: "ClientProtocol",
                        feature: "mutation queries cannot continue from a result cursor".to_string(),
                    });
                }
                self.ensure_local_writer(&context.cell_id).await?;
            }
            return shard.execute_datafusion_cypher_rows_page(context, query, cursor, page_size).await;
        }
        let shard = self.shard(&context.cell_id)?;
        shard.execute_cypher_rows_page(context, query, cursor, page_size).await
    }

    #[cfg(feature = "opencypher")]
    pub async fn execute_cypher_rows_many(
        &self,
        contexts: impl IntoIterator<Item = crate::QueryContext>,
        query: &str,
    ) -> Result<BTreeMap<String, crate::QueryResultSet>> {
        let mut result_sets = BTreeMap::new();
        for context in contexts {
            self.ensure_query_scope(&context.scope)?;
            validate_component("cell_id", &context.cell_id)?;
            if result_sets.contains_key(&context.cell_id) {
                return Err(GraphError::CorruptValue {
                    key: format!("query/cell/{}", context.cell_id),
                    reason: "duplicate cell in routed query request".to_string(),
                });
            }
            let cell_id = context.cell_id.clone();
            let shard = self.shard(&cell_id)?;
            let result_set = shard.execute_cypher_rows(context, query).await?;
            result_sets.insert(cell_id, result_set);
        }
        Ok(result_sets)
    }

    pub(crate) fn ensure_query_scope(&self, scope: &GraphScope) -> Result<()> {
        if scope == &self.scope {
            return Ok(());
        }
        Err(GraphError::GraphScopeMismatch {
            expected: self.scope.to_string(),
            actual: scope.to_string(),
        })
    }

    pub async fn close(&self) -> Result<()> {
        self.close_with_lease_release(true).await
    }

    #[cfg(feature = "query-transport")]
    async fn close_for_eviction(&self) -> Result<()> {
        self.close_with_lease_release(false).await
    }

    async fn close_with_lease_release(&self, release_durable_lease: bool) -> Result<()> {
        let mut failures = Vec::new();
        for (cell_id, shard) in &self.shards {
            if let Err(error) = shard.close().await {
                failures.push(format!("{cell_id}: {error}"));
            }
        }
        if self
            .writer_lease_registration_active
            .swap(false, std::sync::atomic::Ordering::AcqRel)
        {
            if let Some(writer_leases) = &self.writer_leases {
                if release_durable_lease {
                    failures.extend(
                        writer_leases
                            .release_scope(&self.scope, &self.local_node_id)
                            .await
                            .into_iter()
                            .map(|(cell_id, error)| format!("{cell_id} writer lease: {error}")),
                    );
                } else {
                    writer_leases.abandon_scope(&self.scope, &self.local_node_id);
                }
            }
        }
        if failures.is_empty() {
            Ok(())
        } else {
            Err(GraphError::CorruptValue {
                key: format!("routed-cluster/{}/close", self.scope),
                reason: failures.join("; "),
            })
        }
    }
}

/// Evicted scopes that may finish closing in the background at once. Each
/// one still holds its SlateDB handles (a writer's memtable among them) until
/// it closes, so this is how far open-or-closing scopes may exceed
/// `max_open_scopes`; past it, the evicting request closes the scope itself.
#[cfg(feature = "query-transport")]
const BACKGROUND_SCOPE_CLOSES: u32 = 8;

/// Background scope closes that failed, held for `close` to report. Each
/// failure is also logged when it happens, so a long-running node with a
/// failing store keeps only a count past the first few messages.
#[cfg(feature = "query-transport")]
#[derive(Debug, Default)]
pub(super) struct BackgroundCloseFailures {
    count: u64,
    first: Vec<String>,
}

#[cfg(feature = "query-transport")]
impl BackgroundCloseFailures {
    const KEPT: usize = 8;

    fn record(&mut self, failure: String) {
        self.count += 1;
        if self.first.len() < Self::KEPT {
            self.first.push(failure);
        }
    }

    /// The kept messages, then how many more failed: what `close` reports.
    fn take_report(&mut self) -> Vec<String> {
        let taken = std::mem::take(self);
        let mut report = taken.first;
        let omitted = taken.count - report.len() as u64;
        if omitted > 0 {
            report.push(format!("{omitted} more background scope closes failed; each is in the error log"));
        }
        report
    }
}

#[cfg(feature = "query-transport")]
impl ScopedRoutedGraphCluster {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        base_path: impl Into<String>,
        root_namespace: NamespacePath,
        graph_id: GraphId,
        local_node_id: impl Into<String>,
        directory: ObjectStoreNodeDirectory,
        placement: PlacementView,
        object_store: Arc<dyn ObjectStore>,
        options: GraphOpenOptions,
        memory: GraphMemoryConfig,
        max_open_scopes: usize,
    ) -> Result<Self> {
        Self::new_with_writer_lease_duration(
            base_path,
            root_namespace,
            graph_id,
            local_node_id,
            directory,
            placement,
            object_store,
            options,
            memory,
            max_open_scopes,
            Duration::from_secs(30),
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn new_with_writer_lease_duration(
        base_path: impl Into<String>,
        root_namespace: NamespacePath,
        graph_id: GraphId,
        local_node_id: impl Into<String>,
        directory: ObjectStoreNodeDirectory,
        placement: PlacementView,
        object_store: Arc<dyn ObjectStore>,
        options: GraphOpenOptions,
        memory: GraphMemoryConfig,
        max_open_scopes: usize,
        writer_lease_duration: Duration,
    ) -> Result<Self> {
        let base_path = base_path.into();
        let local_node_id = local_node_id.into();
        validate_component("node_id", &local_node_id)?;
        if !directory.contains_node(&local_node_id)? {
            return Err(GraphError::CorruptValue {
                key: format!("directory/node/{local_node_id}"),
                reason: "local node is not present in the object-store node directory".to_string(),
            });
        }
        if placement.local_node_id() != local_node_id {
            return Err(GraphError::CorruptValue {
                key: format!("directory/node/{local_node_id}"),
                reason: format!(
                    "placement view belongs to node {}, not to this runtime's node",
                    placement.local_node_id()
                ),
            });
        }
        if max_open_scopes == 0 {
            return Err(GraphError::AdmissionRejected {
                operation: "max_open_scopes",
                actual: 0,
                limit: 1,
            });
        }
        // Every scope reads through one disk cache the caller built over
        // `object_store` (`SharedDiskCache`). A cache in the options would be
        // one per scope, each with the whole budget or a slice of it, which is
        // the thrash the shared cache replaces.
        if options.cache.object_store_cache_dir.is_some() {
            return Err(GraphError::CorruptValue {
                key: "options/cache/object_store_cache_dir".to_string(),
                reason: "a scoped runtime shares one disk cache; fix: build it with SharedDiskCache::take and pass its object store"
                    .to_string(),
            });
        }
        let writer_registry = process_writer_registry(&object_store);
        let slatedb_cache = crate::process_slate_db_cache_with_config(&object_store, options.cache.slatedb_cache_config())?;
        let writer_leases =
            super::writer_lease::process_writer_lease_directory(base_path.clone(), Arc::clone(&object_store), writer_lease_duration);
        let cache_totals = directory
            .cells()
            .map(|cell| (cell.to_owned(), Arc::new(crate::GraphCacheMetrics::default())))
            .collect();
        let operational_totals = Arc::new(crate::CumulativeOperationalCounters::default());
        Ok(Self {
            base_path: base_path.clone(),
            root_namespace: root_namespace.clone(),
            graph_id: graph_id.clone(),
            local_node_id,
            directory,
            placement,
            scope_directory: ObjectStoreGraphScopeDirectory::new(base_path, root_namespace, graph_id, Arc::clone(&object_store)),
            indexer_change_wake: None,
            shared_disk_cache: None,
            object_store,
            slatedb_cache,
            cache_totals,
            operational_totals,
            writer_leases,
            writer_registry,
            writer_gate: crate::WriterGate::default(),
            options,
            memory,
            max_open_scopes,
            access_clock: AtomicU64::new(0),
            clusters: tokio::sync::Mutex::new(BTreeMap::new()),
            scope_open_gates: tokio::sync::Mutex::new(BTreeMap::new()),
            scope_closures: Arc::new(std::sync::Mutex::new(BTreeMap::new())),
            scope_capacity_gate: tokio::sync::Mutex::new(()),
            scope_capacity_available: Arc::new(tokio::sync::Notify::new()),
            scope_open_reservations: Arc::new(AtomicUsize::new(0)),
            scope_close_budget: Arc::new(tokio::sync::Semaphore::new(BACKGROUND_SCOPE_CLOSES as usize)),
            background_close_failures: Arc::new(std::sync::Mutex::new(BackgroundCloseFailures::default())),
            background_scope_closes: BACKGROUND_SCOPE_CLOSES,
        })
    }

    /// Use explicit writer ownership for equivalent handles to one physical store.
    ///
    /// Call this immediately after construction when multiple runtimes wrap the
    /// same backend with distinct [`ObjectStore`] handles. Independent physical
    /// stores must use distinct registries, regardless of their display strings.
    pub fn with_writer_registry(mut self, writer_registry: Arc<ProcessWriterRegistry>) -> Self {
        self.writer_registry = writer_registry;
        self
    }

    /// Wake an external indexer notifier after its durable change hint lands.
    /// The switch every scope's cluster checks before taking a writer. Open
    /// unless the caller closes it; see [`crate::WriterGate`].
    pub fn writer_gate(&self) -> crate::WriterGate {
        self.writer_gate.clone()
    }

    /// Report `cache`'s counters as the node's disk cache. The runtime's
    /// object store must be `cache.object_store()`, which is what the scopes
    /// read through.
    pub fn with_shared_disk_cache(mut self, cache: SharedDiskCache) -> Self {
        self.shared_disk_cache = Some(cache);
        self
    }

    /// The node's disk cache counters; `None` when it keeps no disk cache.
    pub fn shared_disk_cache_metrics(&self) -> Option<crate::SharedDiskCacheMetrics> {
        self.shared_disk_cache.as_ref().map(SharedDiskCache::metrics)
    }

    pub fn with_indexer_change_wake(mut self, wake: Arc<tokio::sync::Notify>) -> Self {
        self.indexer_change_wake = Some(wake);
        self
    }

    pub fn writer_lease_directory(&self) -> Arc<ObjectStoreWriterLeaseDirectory> {
        Arc::clone(&self.writer_leases)
    }

    /// Cumulative cache operations for this runtime, including closed scopes.
    pub fn graph_cache_metrics(&self) -> BTreeMap<String, GraphCacheMetricsSnapshot> {
        self.cache_totals
            .iter()
            .map(|(cell, metrics)| (cell.clone(), metrics.snapshot()))
            .collect()
    }

    /// Cumulative operational counters for this runtime, per cell, including
    /// closed scopes — the counter totals an export must render so a scope
    /// closing never makes one fall. See [`crate::CumulativeOperationalCounters`].
    pub fn cell_operational_counter_totals(&self) -> BTreeMap<String, crate::CellOperationalCounterTotals> {
        self.operational_totals.totals()
    }

    pub fn slatedb_cache_metrics(&self) -> SlateDbCacheMetricsSnapshot {
        self.slatedb_cache.snapshot()
    }

    /// Renew every writer lease held by this process and withdraw any writer
    /// whose ownership can no longer be proven.
    pub async fn renew_writer_leases(&self) -> Vec<WriterLeaseRenewalFailure> {
        let mut failures = self.writer_leases.renew_local(&self.local_node_id).await;
        for failure in failures.iter_mut().filter(|failure| failure.ownership_lost) {
            let cluster = self
                .clusters
                .lock()
                .await
                .get(&failure.scope)
                .map(|entry| Arc::clone(&entry.cluster));
            let Some(cluster) = cluster else {
                continue;
            };
            if let Err(error) = cluster.retire_cell_writer(&failure.cell_id).await {
                tracing::error!(
                    scope = %failure.scope,
                    cell_id = %failure.cell_id,
                    %error,
                    "failed to retire writer after durable lease loss"
                );
            }
        }
        failures
    }

    pub fn root_scope(&self) -> GraphScope {
        GraphScope::new(self.root_namespace.clone(), self.graph_id.clone())
    }

    fn validate_scope(&self, scope: &GraphScope) -> Result<()> {
        if scope.graph_id == self.graph_id && scope.namespace.is_descendant_of(&self.root_namespace) {
            return Ok(());
        }
        Err(GraphError::GraphScopeMismatch {
            expected: format!("{}/graphs/{} and descendants", self.root_namespace, self.graph_id),
            actual: scope.to_string(),
        })
    }

    /// The options a scope opens with: the runtime's own. The disk cache is
    /// not among them; every scope reads through the shared one in the
    /// runtime's object store.
    fn options_for_scope(&self, _scope: &GraphScope) -> GraphOpenOptions {
        self.options.clone()
    }

    #[tracing::instrument(name = "graph.scope_acquire", skip_all)]
    pub(crate) async fn cluster_for_scope(&self, scope: &GraphScope) -> Result<Arc<RoutedGraphCluster>> {
        self.cluster_for_scope_once(scope).await
    }

    async fn cluster_for_scope_once(&self, scope: &GraphScope) -> Result<Arc<RoutedGraphCluster>> {
        self.validate_scope(scope)?;
        loop {
            let access = self.access_clock.fetch_add(1, Ordering::Relaxed) + 1;
            if let Some(cluster) = self.cached_scope(scope, access).await {
                return Ok(cluster);
            }

            let scope_gate = {
                let mut gates = self.scope_open_gates.lock().await;
                gates.retain(|_, gate| gate.strong_count() > 0);
                if let Some(gate) = gates.get(scope).and_then(std::sync::Weak::upgrade) {
                    gate
                } else {
                    let gate = Arc::new(tokio::sync::Mutex::new(()));
                    gates.insert(scope.clone(), Arc::downgrade(&gate));
                    gate
                }
            };
            let scope_open_guard = scope_gate.lock().await;
            if let Some(cluster) = self.cached_scope(scope, access).await {
                return Ok(cluster);
            }

            // Reserve capacity without holding either async lock across
            // close/open I/O. A scope removed for eviction is first published
            // in `scope_closures`, so a concurrent reopen waits for its old
            // SlateDB writer to finish closing rather than fencing it.
            let capacity_guard = self.scope_capacity_gate.lock().await;
            if let Some(cluster) = self.cached_scope(scope, access).await {
                return Ok(cluster);
            }
            if let Some(mut closing) = self.scope_closure(scope) {
                drop(capacity_guard);
                drop(scope_open_guard);
                let _ = closing.wait_for(|closed| *closed).await;
                continue;
            }
            let (evicted, reservation) = {
                let mut clusters = self.clusters.lock().await;
                let occupied = clusters.len().saturating_add(self.scope_open_reservations.load(Ordering::Acquire));
                let evicted = if occupied >= self.max_open_scopes {
                    let candidate = clusters
                        .iter()
                        .filter(|(_, entry)| Arc::strong_count(&entry.cluster) == 1)
                        .min_by_key(|(_, entry)| entry.last_used)
                        .map(|(scope, _)| scope.clone())
                        .ok_or(GraphError::AdmissionRejected {
                            operation: "open_graph_scopes",
                            actual: occupied.saturating_add(1) as u64,
                            limit: self.max_open_scopes as u64,
                        })?;
                    let entry = clusters.remove(&candidate).expect("selected scoped cluster must still exist");
                    let close = ScopeCloseReservation::new(Arc::clone(&self.scope_closures), candidate.clone());
                    Some((candidate, entry, close))
                } else {
                    None
                };
                self.scope_open_reservations.fetch_add(1, Ordering::AcqRel);
                (evicted, ScopedOpenReservation::new(Arc::clone(&self.scope_open_reservations)))
            };
            drop(capacity_guard);
            let reservation = if let Some((candidate, entry, close)) = evicted {
                let last_used = entry.last_used;
                let cluster = match Arc::try_unwrap(entry.cluster) {
                    Ok(cluster) => cluster,
                    Err(cluster) => {
                        // A background reader (metrics collection, index
                        // discovery) upgraded its `Weak` to this scope between
                        // the idle check above and here: the scope is in use
                        // after all, and nothing is wrong with it. Put it back
                        // before ending its close reservation, so a reopen
                        // that waited on the close finds it cached, then give
                        // up the slot and choose again. The reader's hold is
                        // brief; failing the request instead reported an
                        // ordinary race as corruption.
                        // Under the `clusters` lock, so no other opener can
                        // evict it again and publish a new close marker in
                        // between; and the reservation removes only its own.
                        let mut clusters = self.clusters.lock().await;
                        clusters.insert(candidate.clone(), ScopedRoutedClusterEntry { cluster, last_used });
                        drop(close);
                        drop(clusters);
                        tracing::debug!(scope = %candidate, "eviction candidate was in use; choosing again");
                        drop(reservation);
                        drop(scope_open_guard);
                        tokio::task::yield_now().await;
                        continue;
                    }
                };
                // Draining the evicted scope's writer is a flush to object
                // storage: tens of milliseconds the request that needs the slot
                // has no reason to wait for. With a close permit free, the old
                // scope closes on its own task and this request opens its scope
                // now. Reopening the old scope still waits for that close (its
                // `scope_closures` entry), so two writers never overlap.
                if let Ok(permit) = Arc::clone(&self.scope_close_budget).try_acquire_owned() {
                    let failures = Arc::clone(&self.background_close_failures);
                    tokio::spawn(
                        async move {
                            if let Err(error) = cluster.close_for_eviction().await {
                                tracing::error!(
                                    scope = %candidate,
                                    %error,
                                    "an evicted graph scope failed to close in the background"
                                );
                                failures
                                    .lock()
                                    .expect("background close failures lock poisoned")
                                    .record(format!("{candidate}: {error}"));
                            }
                            drop(cluster);
                            close.release();
                            drop(permit);
                        }
                        .instrument(tracing::info_span!("graph.scope_evict", hydradb.scope_close = "background")),
                    );
                    reservation
                } else {
                    // Closing outlives the request that triggered eviction. If the
                    // request is cancelled, this task still drains the old writer
                    // and only then releases same-scope reopeners. It also owns
                    // capacity until cleanup finishes, even if its caller leaves.
                    // Every close permit is held: close it here, as the request
                    // always did, so closing scopes stay bounded.
                    tokio::spawn(
                        async move {
                            let result = cluster.close_for_eviction().await;
                            drop(cluster);
                            close.release();
                            result?;
                            Ok::<_, GraphError>(reservation)
                        }
                        .instrument(tracing::info_span!("graph.scope_evict", hydradb.scope_close = "inline")),
                    )
                    .await
                    .map_err(|error| GraphError::CorruptValue {
                        key: format!("scoped-cluster/{candidate}"),
                        reason: format!("scope close task failed: {error}"),
                    })??
                }
            } else {
                reservation
            };

            let cluster = Arc::new(
                RoutedGraphCluster::open_promotable_scoped_with_writer_registry(
                    self.base_path.clone(),
                    scope.clone(),
                    self.local_node_id.clone(),
                    self.directory.clone(),
                    // Cloned, never rebuilt: every scope's cluster and the routing
                    // provider must answer from one live set.
                    self.placement.clone(),
                    Arc::clone(&self.object_store),
                    Arc::clone(&self.writer_registry),
                    Arc::clone(&self.writer_leases),
                    self.writer_gate.clone(),
                    self.options_for_scope(scope),
                    self.memory.clone(),
                    self.cache_totals.clone(),
                    Some(Arc::clone(&self.operational_totals)),
                )
                .instrument(tracing::info_span!("graph.scope_open"))
                .await?,
            );
            #[cfg(feature = "experimental-cypher-engine")]
            self.install_planner_statistics_hints(scope, &cluster);
            let _capacity_guard = self.scope_capacity_gate.lock().await;
            reservation.release();
            self.clusters.lock().await.insert(
                scope.clone(),
                ScopedRoutedClusterEntry {
                    cluster: Arc::clone(&cluster),
                    last_used: access,
                },
            );
            return Ok(cluster);
        }
    }

    /// Wait for an in-flight scoped query to release an eviction candidate.
    ///
    /// `max_open_scopes` is a memory bound, not a request-rate bound. A burst
    /// across more scopes than the node can retain must therefore wait behind
    /// active scopes instead of surfacing the cache's instantaneous state as a
    /// client admission failure. The client query runtime owns the deadline and
    /// cancellation token; callers outside that runtime keep the immediate
    /// `cluster_for_scope` behavior so maintenance code cannot wait forever.
    #[tracing::instrument(name = "graph.scope_capacity_wait", skip_all)]
    pub(crate) async fn cluster_for_scope_wait(&self, scope: &GraphScope) -> Result<Arc<RoutedGraphCluster>> {
        let Some(cancellation) = crate::QueryCancellationToken::current() else {
            return self.cluster_for_scope(scope).await;
        };
        loop {
            // Register before checking capacity so a query release between the
            // failed check and the await cannot become a lost wake-up.
            let capacity_available = self.scope_capacity_available.notified();
            match self.cluster_for_scope_once(scope).await {
                Ok(cluster) => return Ok(cluster),
                Err(GraphError::AdmissionRejected {
                    operation: "open_graph_scopes",
                    ..
                }) => {
                    tokio::select! {
                        _ = capacity_available => {}
                        // Weak handles used by background maintenance do not
                        // carry a client release guard. Recheck occasionally
                        // so their short upgrade window cannot strand a wait.
                        _ = tokio::time::sleep(Duration::from_millis(100)) => {}
                        _ = cancellation.cancelled() => {
                            return Err(GraphError::QueryTimeout {
                                operation: "client_query_cancelled",
                                elapsed_ms: 0,
                                limit_ms: 0,
                            });
                        }
                    }
                }
                Err(error) => return Err(error),
            }
        }
    }

    pub(crate) fn scope_capacity_release_guard(&self) -> ScopeCapacityRelease {
        ScopeCapacityRelease {
            available: Arc::clone(&self.scope_capacity_available),
        }
    }

    fn scope_closure(&self, scope: &GraphScope) -> Option<tokio::sync::watch::Receiver<bool>> {
        self.scope_closures
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(scope)
            .cloned()
    }

    async fn cached_scope(&self, scope: &GraphScope, access: u64) -> Option<Arc<RoutedGraphCluster>> {
        let mut clusters = self.clusters.lock().await;
        let entry = clusters.get_mut(scope)?;
        entry.last_used = access;
        Some(Arc::clone(&entry.cluster))
    }

    pub(crate) async fn cluster_for_scope_write_wait(&self, scope: &GraphScope, cell_id: &str) -> Result<Arc<RoutedGraphCluster>> {
        let cluster = self.cluster_for_scope_wait(scope).await?;
        cluster.ensure_local_writer(cell_id).await?;
        cluster
            .scope_registration
            .get_or_try_init(|| self.scope_directory.register(scope))
            .await?;
        Ok(cluster)
    }

    /// Give every shard of `cluster` a way to ask the indexer for planner
    /// statistics: a marker beside the change hints, then the same wake the
    /// write path uses. Outside the query's latency path, and best effort
    /// like a change hint.
    #[cfg(feature = "experimental-cypher-engine")]
    fn install_planner_statistics_hints(&self, scope: &GraphScope, cluster: &RoutedGraphCluster) {
        for cell_id in self.directory.cells() {
            let Ok(shard) = cluster.shard(cell_id) else {
                continue;
            };
            let directory = self.scope_directory.clone();
            let wake = self.indexer_change_wake.clone();
            let scope = scope.clone();
            shard.planner_statistics_hints.install(Arc::new(
                move |cell_id: &str, on_failure: crate::shard::PlannerStatisticsHintFailure| {
                    let directory = directory.clone();
                    let wake = wake.clone();
                    let scope = scope.clone();
                    let cell_id = cell_id.to_string();
                    tokio::spawn(async move {
                        match directory.request_planner_statistics(&scope, &cell_id).await {
                            Ok(()) => {
                                if let Some(wake) = wake {
                                    wake.notify_one();
                                }
                            }
                            Err(error) => {
                                tracing::warn!(
                                    hydradb.scope = %scope,
                                    hydradb.cell_id = %cell_id,
                                    error = %error,
                                    "failed to persist planner statistics request; the next query asks again"
                                );
                                on_failure();
                            }
                        }
                    });
                },
            ));
        }
    }

    /// Notify the background indexer after a committed scoped mutation.
    ///
    /// The notification is deliberately outside the client latency path. The
    /// durable graph write remains authoritative and the indexer's fair sweep
    /// repairs a notification lost during process shutdown.
    pub(crate) fn notify_graph_index_change(&self, scope: GraphScope, cell_id: String, sequence: crate::StorageSequence) {
        let directory = self.scope_directory.clone();
        let wake = self.indexer_change_wake.clone();
        tokio::spawn(async move {
            match directory.notify_changed(&scope, &cell_id, sequence).await {
                Ok(()) => {
                    if let Some(wake) = wake {
                        wake.notify_one();
                    }
                }
                Err(error) => {
                    tracing::warn!(
                        hydradb.scope = %scope,
                        hydradb.cell_id = %cell_id,
                        hydradb.base_sequence = sequence,
                        error = %error,
                        "failed to persist graph index change hint"
                    );
                }
            }
        });
    }

    /// Close idle writers that the current placement view assigns to another node.
    ///
    /// Placement can change without another request ever reaching the previous
    /// owner. Keeping that node's SlateDB writer open lets its background tasks
    /// race the replacement writer indefinitely. Removing the idle scoped
    /// cluster completes the handoff; a later read can reopen the scope, while a
    /// write still has to pass `ensure_local_writer` against the latest view.
    pub async fn retire_unowned_writers(&self) -> Result<usize> {
        let _capacity_guard = self.scope_capacity_gate.lock().await;
        let candidates = {
            let clusters = self.clusters.lock().await;
            clusters
                .iter()
                .filter(|(_, entry)| Arc::strong_count(&entry.cluster) == 1 && entry.cluster.holds_unowned_writer())
                .map(|(scope, _)| scope.clone())
                .collect::<Vec<_>>()
        };

        let mut retiring = Vec::with_capacity(candidates.len());
        {
            let mut clusters = self.clusters.lock().await;
            for scope in candidates {
                let Some(entry) = clusters.get(&scope) else {
                    continue;
                };
                if Arc::strong_count(&entry.cluster) != 1 || !entry.cluster.holds_unowned_writer() {
                    continue;
                }
                let Some(entry) = clusters.remove(&scope) else {
                    continue;
                };
                let close = ScopeCloseReservation::new(Arc::clone(&self.scope_closures), scope.clone());
                match Arc::try_unwrap(entry.cluster) {
                    Ok(cluster) => retiring.push((scope, cluster, close)),
                    Err(cluster) => {
                        close.release();
                        clusters.insert(
                            scope,
                            ScopedRoutedClusterEntry {
                                cluster,
                                last_used: entry.last_used,
                            },
                        );
                    }
                }
            }
        }
        drop(_capacity_guard);

        let mut retired = 0;
        let mut failures = Vec::new();
        for (scope, cluster, close) in retiring {
            let result = cluster.close().await;
            drop(cluster);
            close.release();
            match result {
                Ok(()) => retired += 1,
                Err(error) => failures.push(format!("{scope}: {error}")),
            }
        }
        if failures.is_empty() {
            Ok(retired)
        } else {
            Err(GraphError::CorruptValue {
                key: "scoped-clusters/placement-retire".to_string(),
                reason: failures.join("; "),
            })
        }
    }

    pub async fn loaded_scopes(&self) -> Vec<GraphScope> {
        self.clusters.lock().await.keys().cloned().collect()
    }

    /// Every writer this node holds, across all scopes, for the buddy WAL
    /// feed. Reads the process writer registry, so it sees exactly the writers
    /// this node's scopes have installed and never opens a scope to answer.
    pub fn writer_directory(&self) -> crate::NodeWriters {
        self.writer_registry.node_writers(self.local_node_id.clone())
    }

    /// Non-owning handles to every open scope's cluster.
    ///
    /// `Weak`, and the return type is the whole point. Eviction's candidate
    /// filter is `Arc::strong_count(&entry.cluster) == 1` (see
    /// `cluster_for_scope`), so a strong clone a caller keeps makes that scope
    /// un-evictable for as long as it keeps it — and at `max_open_scopes` a
    /// query for a not-yet-open scope then fails with `AdmissionRejected`
    /// rather than waiting. The index-discovery loop iterated a `Vec` of strong
    /// clones across per-cell object-store I/O, which turned a background
    /// interval task into a client-visible error rate.
    ///
    /// Callers must upgrade **one at a time** and drop each `Arc` before
    /// upgrading the next. An upgrade returning `None` means the scope was
    /// evicted since the snapshot, which is a skip, not an error.
    ///
    /// This shrinks the un-evictable window rather than eliminating it: while a
    /// caller holds one upgraded `Arc`, that one scope is still un-evictable.
    /// Closing the window entirely needs an in-use count separate from the
    /// `Arc` count, which is more machinery than the residual risk justifies.
    pub async fn loaded_clusters(&self) -> Vec<std::sync::Weak<RoutedGraphCluster>> {
        self.clusters
            .lock()
            .await
            .values()
            .map(|entry| Arc::downgrade(&entry.cluster))
            .collect()
    }

    /// Per-scope shard runtime metrics, collected one scope at a time.
    ///
    /// The `clusters` mutex is taken only to snapshot the map, and what is
    /// snapshotted is `Weak` rather than `Arc` deliberately. Eviction's
    /// candidate filter is `Arc::strong_count(&entry.cluster) == 1`, so
    /// retaining a strong clone of every open scope for the length of the
    /// collection — which is what this did — made *every* scope un-evictable
    /// for that whole span. At `max_open_scopes` a query for a not-yet-open
    /// scope then found no eviction candidate and got
    /// `AdmissionRejected { operation: "open_graph_scopes", .. }` straight back:
    /// a hard client error with no retry and no wait, produced by a scrape.
    ///
    /// Upgrading one at a time and dropping each `Arc` before the next
    /// **shrinks** that window from "every open scope, for the whole
    /// collection" to "one scope, for one cluster's worth of cache-lock reads".
    /// It does not eliminate it — the scope currently being read is still
    /// un-evictable. Eliminating it needs an explicit in-use count separate
    /// from the `Arc` count, which is more machinery than the residual risk
    /// justifies at `max_open_scopes = 8` and a 60s export interval.
    ///
    /// A `Weak` that fails to upgrade means the scope was evicted between the
    /// snapshot and its turn. That is a skip, not an error: the metrics are
    /// advisory and a closed scope has nothing left to report.
    pub async fn local_shard_runtime_metrics(&self) -> Vec<ScopedGraphShardRuntimeMetrics> {
        let clusters = self
            .clusters
            .lock()
            .await
            .iter()
            .map(|(scope, entry)| (scope.clone(), Arc::downgrade(&entry.cluster)))
            .collect::<Vec<_>>();
        let mut metrics = Vec::new();
        for (scope, cluster) in clusters {
            let Some(cluster) = cluster.upgrade() else {
                continue;
            };
            metrics.extend(
                cluster
                    .local_shard_runtime_metrics()
                    .await
                    .into_iter()
                    .map(|shard| ScopedGraphShardRuntimeMetrics {
                        scope: scope.clone(),
                        shard,
                    }),
            );
            // `cluster` drops here, before the next upgrade, so at most one
            // scope is pinned at a time.
        }
        metrics
    }

    pub async fn close(&self) -> Result<()> {
        // Background closes of evicted scopes finish before the runtime does:
        // shutdown must not cut off a writer drain.
        let _drained = self
            .scope_close_budget
            .acquire_many(self.background_scope_closes)
            .await
            .map_err(|error| GraphError::CorruptValue {
                key: "runtime/scoped_clusters/close".to_string(),
                reason: format!("background scope closes: {error}"),
            })?;
        let entries = std::mem::take(&mut *self.clusters.lock().await);
        let mut failures = self
            .background_close_failures
            .lock()
            .expect("background close failures lock poisoned")
            .take_report();
        for (scope, entry) in entries {
            match Arc::try_unwrap(entry.cluster) {
                Ok(cluster) => {
                    if let Err(error) = cluster.close().await {
                        failures.push(format!("{scope}: {error}"));
                    }
                }
                Err(_) => failures.push(format!("{scope}: scoped cluster is still in use")),
            }
        }
        if failures.is_empty() {
            Ok(())
        } else {
            Err(GraphError::CorruptValue {
                key: "runtime/scoped_clusters/close".to_string(),
                reason: failures.join("; "),
            })
        }
    }
}

#[cfg(feature = "query-transport")]
struct ScopedOpenReservation {
    counter: Arc<AtomicUsize>,
    active: bool,
}

#[cfg(feature = "query-transport")]
pub(crate) struct ScopeCapacityRelease {
    available: Arc<tokio::sync::Notify>,
}

#[cfg(feature = "query-transport")]
impl Drop for ScopeCapacityRelease {
    fn drop(&mut self) {
        self.available.notify_one();
    }
}

#[cfg(feature = "query-transport")]
impl ScopedOpenReservation {
    fn new(counter: Arc<AtomicUsize>) -> Self {
        Self { counter, active: true }
    }

    fn release(mut self) {
        self.counter.fetch_sub(1, Ordering::AcqRel);
        self.active = false;
    }
}

#[cfg(feature = "query-transport")]
impl Drop for ScopedOpenReservation {
    fn drop(&mut self) {
        if self.active {
            self.counter.fetch_sub(1, Ordering::AcqRel);
        }
    }
}

#[cfg(feature = "query-transport")]
struct ScopeCloseReservation {
    closures: Arc<std::sync::Mutex<BTreeMap<GraphScope, tokio::sync::watch::Receiver<bool>>>>,
    scope: GraphScope,
    completed: tokio::sync::watch::Sender<bool>,
    /// This reservation's own marker, so finishing removes it and never a
    /// later close's marker for the same scope.
    marker: tokio::sync::watch::Receiver<bool>,
    active: bool,
}

#[cfg(feature = "query-transport")]
impl ScopeCloseReservation {
    fn new(closures: Arc<std::sync::Mutex<BTreeMap<GraphScope, tokio::sync::watch::Receiver<bool>>>>, scope: GraphScope) -> Self {
        let (completed, receiver) = tokio::sync::watch::channel(false);
        closures
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(scope.clone(), receiver.clone());
        Self {
            closures,
            scope,
            completed,
            marker: receiver,
            active: true,
        }
    }

    fn release(mut self) {
        self.finish();
    }

    fn finish(&mut self) {
        if !self.active {
            return;
        }
        let _ = self.completed.send(true);
        let mut closures = self.closures.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        if closures.get(&self.scope).is_some_and(|marker| marker.same_channel(&self.marker)) {
            closures.remove(&self.scope);
        }
        self.active = false;
    }
}

#[cfg(feature = "query-transport")]
impl Drop for ScopeCloseReservation {
    fn drop(&mut self) {
        self.finish();
    }
}

/// Run a write statement on its own task, so it finishes even when the
/// future awaiting it is dropped. A Bolt RESET, GOODBYE or closed connection
/// drops the client's future wherever it is; a statement whose effects span
/// more than one batch transaction must not stop between them because its
/// caller went away. Cancellation stays cooperative: the request's token is
/// re-entered on the task, so a cancelled statement still stops during its
/// read-only matching, before anything is written.
#[cfg(any(feature = "opencypher", feature = "query-transport"))]
pub(crate) async fn run_write_to_completion<T, F>(write: F) -> Result<T>
where
    T: Send + 'static,
    F: std::future::Future<Output = Result<T>> + Send + 'static,
{
    let token = crate::QueryCancellationToken::current();
    let estimated_bytes = crate::core::memory_diagnostics::request_estimated_bytes();
    let task = tokio::spawn(
        crate::core::memory_diagnostics::REQUEST_ESTIMATED_BYTES
            .scope(estimated_bytes, crate::QueryCancellationToken::reenter(token, write))
            .instrument(tracing::Span::current()),
    );
    match task.await {
        Ok(result) => result,
        Err(error) if error.is_panic() => std::panic::resume_unwind(error.into_panic()),
        Err(error) => Err(GraphError::CorruptValue {
            key: "cluster/write_task".to_string(),
            reason: format!("write task did not complete: {error}"),
        }),
    }
}

async fn close_shards_best_effort(shards: BTreeMap<String, Arc<GraphShard>>) {
    for shard in shards.into_values() {
        let _ = shard.close().await;
    }
}

async fn close_routed_shards_best_effort(shards: BTreeMap<String, Arc<GraphShard>>) {
    for shard in shards.into_values() {
        let _ = shard.close().await;
    }
}
