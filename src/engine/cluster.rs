use super::*;
use crate::keys;

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
        let store_path = scope.scoped_store_path(&base_path.into());
        Self::open_cells_at_path(store_path, scope, cell_ids, object_store, false).await
    }

    pub async fn open_cells_standalone_writers(
        base_path: impl Into<String>,
        cell_ids: impl IntoIterator<Item = impl Into<String>>,
        object_store: Arc<dyn ObjectStore>,
    ) -> Result<Self> {
        Self::open_cells_standalone_writers_scoped(
            base_path,
            GraphScope::default(),
            cell_ids,
            object_store,
        )
        .await
    }

    pub async fn open_cells_standalone_writers_scoped(
        base_path: impl Into<String>,
        scope: GraphScope,
        cell_ids: impl IntoIterator<Item = impl Into<String>>,
        object_store: Arc<dyn ObjectStore>,
    ) -> Result<Self> {
        let store_path = scope.scoped_store_path(&base_path.into());
        Self::open_cells_at_path(store_path, scope, cell_ids, object_store, true).await
    }

    async fn open_cells_at_path(
        base_path: String,
        scope: GraphScope,
        cell_ids: impl IntoIterator<Item = impl Into<String>>,
        object_store: Arc<dyn ObjectStore>,
        writable: bool,
    ) -> Result<Self> {
        let cell_ids = cell_ids
            .into_iter()
            .map(Into::into)
            .collect::<Vec<String>>();
        for cell_id in &cell_ids {
            validate_component("cell_id", cell_id)?;
        }
        let mut shards = BTreeMap::new();
        for cell_id in cell_ids {
            let path = format!("{base_path}/{cell_id}");
            let opened = if writable {
                GraphShard::open_standalone_writer(path, Arc::clone(&object_store)).await
            } else {
                GraphShard::open(path, Arc::clone(&object_store)).await
            };
            let shard = match opened {
                Ok(shard) => shard,
                Err(err) => {
                    close_shards_best_effort(shards).await;
                    return Err(err);
                }
            };
            shards.insert(cell_id, shard);
        }
        Ok(Self { scope, shards })
    }

    pub fn scope(&self) -> &GraphScope {
        &self.scope
    }

    pub fn shard(&self, cell_id: &str) -> Option<&GraphShard> {
        self.shards.get(cell_id)
    }

    pub fn shard_count(&self) -> usize {
        self.shards.len()
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
    promotable: bool,
    options: GraphOpenOptions,
    memory: GraphMemoryConfig,
}

impl RoutedGraphCluster {
    pub async fn open_readers(
        base_path: impl Into<String>,
        local_node_id: impl Into<String>,
        directory: ObjectStoreNodeDirectory,
        placement: PlacementView,
        object_store: Arc<dyn ObjectStore>,
    ) -> Result<Self> {
        Self::open_readers_scoped(
            base_path,
            GraphScope::default(),
            local_node_id,
            directory,
            placement,
            object_store,
        )
        .await
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
            promotable: false,
            options: GraphOpenOptions::default(),
            memory: GraphMemoryConfig::default(),
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
        let base_path = scope.scoped_store_path(&base_path.into());
        Self::open_at_path(RoutedClusterOpenConfig {
            base_path,
            scope,
            local_node_id: local_node_id.into(),
            directory,
            placement,
            object_store,
            promotable: true,
            options,
            memory,
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
            promotable,
            options,
            memory,
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
        let mut shards = BTreeMap::new();
        for cell_id in directory.cells().map(str::to_string) {
            let path = format!("{base_path}/{cell_id}");
            let opened = if promotable {
                GraphShard::open_promotable_with_memory_options(
                    path,
                    Arc::clone(&object_store),
                    options.clone(),
                    memory.clone(),
                )
                .await
            } else {
                GraphShard::open_with_memory_options(
                    path,
                    Arc::clone(&object_store),
                    options.clone(),
                    memory.clone(),
                )
                .await
            };
            let shard = match opened {
                Ok(shard) => shard,
                Err(err) => {
                    close_routed_shards_best_effort(shards).await;
                    return Err(err);
                }
            };
            if !promotable
                && shard
                    .read_remote(&keys::cell_drop_marker(&cell_id))
                    .await?
                    .is_some()
            {
                shard.close().await?;
                continue;
            }
            shards.insert(cell_id, Arc::new(shard));
        }
        Ok(Self {
            scope,
            local_node_id,
            directory,
            placement,
            shards,
            promotable,
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
            metrics.push(GraphShardRuntimeMetrics {
                cell_id: cell_id.clone(),
                operational: shard.graph_operational_metrics(),
                cache: shard.graph_cache_metrics(),
                cache_entries: shard.graph_cache_entry_counts().await,
                cache_resident_bytes: shard.graph_cache_resident_bytes().await,
            });
        }
        metrics
    }

    pub fn shard(&self, cell_id: &str) -> Result<&GraphShard> {
        validate_component("cell_id", cell_id)?;
        self.shards
            .get(cell_id)
            .map(Arc::as_ref)
            .ok_or_else(|| GraphError::UnknownShard {
                cell_id: cell_id.to_string(),
            })
    }

    /// The gate every routed write passes through, and the one branch that ends
    /// the writer duel.
    ///
    /// > A node must not promote itself for a cell it does not own, unless the
    /// > computed owner is not live.
    ///
    /// Touch point (b) of `docs/plans/2026-07-25-rendezvous-placement.md`.
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
    pub(crate) async fn ensure_local_writer(&self, cell_id: &str) -> Result<()> {
        validate_component("cell_id", cell_id)?;
        if !self.promotable {
            return Err(GraphError::WriteRequiresWriter {
                operation: "routed_write",
                cell_id: cell_id.to_string(),
            });
        }
        let shard = self
            .shards
            .get(cell_id)
            .ok_or_else(|| GraphError::UnknownShard {
                cell_id: cell_id.to_string(),
            })?;

        match self.placement.ownership(&self.scope.to_string(), cell_id) {
            // The rendezvous owner, or a known-empty fleet with no owner to
            // defer to. Either way this node may hold the writer.
            CellOwnership::Local | CellOwnership::Unowned => {}
            // A live peer owns the cell. Name it, so the driver re-routes
            // instead of retrying into the same wrong node.
            CellOwnership::Remote { node_id } => {
                return Err(GraphError::NotCellWriter {
                    cell_id: cell_id.to_string(),
                    owner: Some(node_id),
                })
            }
            // This node has shed its view (decision 7) and knows nothing about
            // the fleet. Refuse with no hint: a stale guess would point at a
            // node that may itself have shed, and promoting here is the
            // unbounded duel — a partitioned node can never learn it lost.
            CellOwnership::Unknown => {
                return Err(GraphError::NotCellWriter {
                    cell_id: cell_id.to_string(),
                    owner: None,
                })
            }
        }

        self.await_writer_reopen(shard, cell_id).await?;
        shard.promote_to_writer(cell_id, "routed_write").await
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
        if shard.db.writer().is_ok() {
            return Ok(());
        }
        let Some(delay) = shard.db.writer_reopen_delay() else {
            return Ok(());
        };
        let cap = self.placement.config().heartbeat_interval;
        if delay > cap {
            return Err(GraphError::AdmissionRejected {
                operation: "writer_reopen",
                actual: delay.as_millis() as u64,
                limit: cap.as_millis() as u64,
            });
        }
        tracing::debug!(
            node_id = %self.local_node_id,
            cell_id,
            delay_ms = delay.as_millis(),
            "pacing a writer re-open after a fence"
        );
        tokio::time::sleep(delay).await;
        Ok(())
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
        shard
            .delete_edges_batch(cell_id, edge_type, edges, idempotency_key)
            .await
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
        shard
            .delete_vertex(cell_id, vertex_id, idempotency_key)
            .await
    }

    pub async fn detach_delete_vertex(
        &self,
        cell_id: &str,
        vertex_id: crate::VertexId,
        idempotency_key: &str,
    ) -> Result<crate::VertexDeleteResult> {
        let shard = self.shard(cell_id)?;
        self.ensure_local_writer(cell_id).await?;
        shard
            .detach_delete_vertex(cell_id, vertex_id, idempotency_key)
            .await
    }

    pub async fn drop_cell(
        &self,
        cell_id: &str,
        idempotency_key: &str,
    ) -> Result<crate::GraphCellDropResult> {
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
        shard
            .ingest_edge_mutations(cell_id, mutations, options)
            .await
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
        shard
            .bulk_import_edges(cell_id, edge_type, edges, idempotency_key)
            .await
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
        shard
            .write_edges_batch(cell_id, edge_type, edges, idempotency_key)
            .await
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

    pub async fn set_vertex_metadata(
        &self,
        cell_id: &str,
        vertex_id: crate::VertexId,
        metadata: crate::VertexMetadata,
    ) -> Result<()> {
        let shard = self.shard(cell_id)?;
        self.ensure_local_writer(cell_id).await?;
        shard
            .set_vertex_metadata(cell_id, vertex_id, metadata)
            .await
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
        shard
            .set_edge_metadata(cell_id, edge_type, src, dst, metadata)
            .await
    }

    pub async fn set_edge_metadata_batch(
        &self,
        cell_id: &str,
        edge_type: &str,
        updates: impl IntoIterator<Item = (crate::VertexId, crate::VertexId, crate::EdgeMetadata)>,
    ) -> Result<usize> {
        let shard = self.shard(cell_id)?;
        self.ensure_local_writer(cell_id).await?;
        shard
            .set_edge_metadata_batch(cell_id, edge_type, updates)
            .await
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
        self.shard(cell_id)?
            .out_neighbors_batch(cell_id, edge_type, sources)
            .await
    }

    pub async fn in_neighbors_batch(
        &self,
        cell_id: &str,
        edge_type: &str,
        destinations: impl IntoIterator<Item = VertexId>,
    ) -> Result<Vec<crate::NeighborBatchEntry>> {
        self.shard(cell_id)?
            .in_neighbors_batch(cell_id, edge_type, destinations)
            .await
    }

    pub async fn edge_exists_batch(
        &self,
        cell_id: &str,
        edge_type: &str,
        edges: impl IntoIterator<Item = (VertexId, VertexId)>,
    ) -> Result<Vec<crate::EdgeExistenceBatchEntry>> {
        self.shard(cell_id)?
            .edge_exists_batch(cell_id, edge_type, edges)
            .await
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
    pub async fn execute_cypher(
        &self,
        context: crate::QueryContext,
        query: &str,
    ) -> Result<crate::QueryOutput> {
        self.ensure_query_scope(&context.scope)?;
        let shard = self.shard(&context.cell_id)?;
        let requires_writer = matches!(
            crate::query::opencypher::classify_opencypher_query_access(query)?,
            crate::query::opencypher::OpenCypherQueryAccess::Write
        );
        if requires_writer {
            self.ensure_local_writer(&context.cell_id).await?;
        }
        shard.execute_cypher(context, query).await
    }

    #[cfg(feature = "opencypher")]
    pub async fn execute_cypher_rows(
        &self,
        context: crate::QueryContext,
        query: &str,
    ) -> Result<crate::QueryResultSet> {
        self.ensure_query_scope(&context.scope)?;
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
        let shard = self.shard(&context.cell_id)?;
        shard
            .execute_cypher_rows_page(context, query, cursor, page_size)
            .await
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

    fn ensure_query_scope(&self, scope: &GraphScope) -> Result<()> {
        if scope == &self.scope {
            return Ok(());
        }
        Err(GraphError::GraphScopeMismatch {
            expected: self.scope.to_string(),
            actual: scope.to_string(),
        })
    }

    pub async fn close(&self) -> Result<()> {
        for shard in self.shards.values() {
            shard.close().await?;
        }
        Ok(())
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
        Ok(Self {
            base_path: base_path.clone(),
            root_namespace: root_namespace.clone(),
            graph_id: graph_id.clone(),
            local_node_id,
            directory,
            placement,
            scope_directory: ObjectStoreGraphScopeDirectory::new(
                base_path,
                root_namespace,
                graph_id,
                Arc::clone(&object_store),
            ),
            object_store,
            options,
            memory,
            max_open_scopes,
            access_clock: AtomicU64::new(0),
            clusters: tokio::sync::Mutex::new(BTreeMap::new()),
        })
    }

    pub fn root_scope(&self) -> GraphScope {
        GraphScope::new(self.root_namespace.clone(), self.graph_id.clone())
    }

    fn validate_scope(&self, scope: &GraphScope) -> Result<()> {
        if scope.graph_id == self.graph_id && scope.namespace.is_descendant_of(&self.root_namespace)
        {
            return Ok(());
        }
        Err(GraphError::GraphScopeMismatch {
            expected: format!(
                "{}/graphs/{} and descendants",
                self.root_namespace, self.graph_id
            ),
            actual: scope.to_string(),
        })
    }

    fn options_for_scope(&self, scope: &GraphScope) -> GraphOpenOptions {
        let mut options = self.options.clone();
        if let Some(root) = &options.cache.object_store_cache_dir {
            let mut scoped_root = root.join("scopes");
            for segment in scope.namespace.segments() {
                scoped_root.push(segment.as_str());
            }
            scoped_root.push("graphs");
            scoped_root.push(scope.graph_id.as_str());
            options.cache.object_store_cache_dir = Some(scoped_root);
        }
        if let Some(bytes) = options.cache.object_store_cache_bytes {
            options.cache.object_store_cache_bytes = Some((bytes / self.max_open_scopes).max(1));
        }
        options
    }

    pub(crate) async fn cluster_for_scope(
        &self,
        scope: &GraphScope,
    ) -> Result<Arc<RoutedGraphCluster>> {
        self.validate_scope(scope)?;
        let access = self.access_clock.fetch_add(1, Ordering::Relaxed) + 1;
        let mut clusters = self.clusters.lock().await;
        if let Some(entry) = clusters.get_mut(scope) {
            entry.last_used = access;
            return Ok(Arc::clone(&entry.cluster));
        }

        if clusters.len() >= self.max_open_scopes {
            let candidate = clusters
                .iter()
                .filter(|(_, entry)| Arc::strong_count(&entry.cluster) == 1)
                .min_by_key(|(_, entry)| entry.last_used)
                .map(|(scope, _)| scope.clone())
                .ok_or(GraphError::AdmissionRejected {
                    operation: "open_graph_scopes",
                    actual: clusters.len().saturating_add(1) as u64,
                    limit: self.max_open_scopes as u64,
                })?;
            let entry = clusters
                .remove(&candidate)
                .expect("selected scoped cluster must still exist");
            let cluster = Arc::try_unwrap(entry.cluster).map_err(|_| GraphError::CorruptValue {
                key: format!("scoped-cluster/{candidate}"),
                reason: "idle scoped cluster acquired a concurrent owner during eviction"
                    .to_string(),
            })?;
            cluster.close().await?;
        }

        let cluster = Arc::new(
            RoutedGraphCluster::open_promotable_scoped_with_memory_options(
                self.base_path.clone(),
                scope.clone(),
                self.local_node_id.clone(),
                self.directory.clone(),
                // Cloned, never rebuilt: every scope's cluster and the routing
                // provider must answer from one live set.
                self.placement.clone(),
                Arc::clone(&self.object_store),
                self.options_for_scope(scope),
                self.memory.clone(),
            )
            .await?,
        );
        clusters.insert(
            scope.clone(),
            ScopedRoutedClusterEntry {
                cluster: Arc::clone(&cluster),
                last_used: access,
            },
        );
        Ok(cluster)
    }

    pub(crate) async fn cluster_for_scope_write(
        &self,
        scope: &GraphScope,
        cell_id: &str,
    ) -> Result<Arc<RoutedGraphCluster>> {
        let cluster = self.cluster_for_scope(scope).await?;
        cluster.ensure_local_writer(cell_id).await?;
        self.scope_directory.register(scope).await?;
        Ok(cluster)
    }

    pub async fn loaded_scopes(&self) -> Vec<GraphScope> {
        self.clusters.lock().await.keys().cloned().collect()
    }

    pub async fn loaded_clusters(&self) -> Vec<Arc<RoutedGraphCluster>> {
        self.clusters
            .lock()
            .await
            .values()
            .map(|entry| Arc::clone(&entry.cluster))
            .collect()
    }

    pub async fn local_shard_runtime_metrics(&self) -> Vec<ScopedGraphShardRuntimeMetrics> {
        let clusters = self
            .clusters
            .lock()
            .await
            .iter()
            .map(|(scope, entry)| (scope.clone(), Arc::clone(&entry.cluster)))
            .collect::<Vec<_>>();
        let mut metrics = Vec::new();
        for (scope, cluster) in clusters {
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
        }
        metrics
    }

    pub async fn close(&self) -> Result<()> {
        let entries = std::mem::take(&mut *self.clusters.lock().await);
        let mut failures = Vec::new();
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

async fn close_shards_best_effort(shards: BTreeMap<String, GraphShard>) {
    for shard in shards.into_values() {
        let _ = shard.close().await;
    }
}

async fn close_routed_shards_best_effort(shards: BTreeMap<String, Arc<GraphShard>>) {
    for shard in shards.into_values() {
        let _ = shard.close().await;
    }
}

#[cfg(all(test, feature = "query-transport"))]
mod scoped_cluster_tests {
    use slatedb::object_store::memory::InMemory;

    use super::*;
    use crate::NamespaceId;

    #[test]
    fn scoped_clusters_partition_the_local_slate_cache_budget() {
        let root = NamespacePath::root(NamespaceId::new("production").unwrap());
        let graph_id = GraphId::new("hydradb").unwrap();
        let runtime = ScopedRoutedGraphCluster::new(
            "graph/data",
            root.clone(),
            graph_id.clone(),
            "node-a",
            ObjectStoreNodeDirectory::new(["cell-0"], ["node-a"]).unwrap(),
            PlacementView::new("node-a", ["node-a"], PlacementConfig::default()).unwrap(),
            Arc::new(InMemory::new()),
            GraphOpenOptions {
                cache: crate::GraphCacheConfig::disk_cache("/cache", 800),
                ..GraphOpenOptions::default()
            },
            GraphMemoryConfig::default(),
            8,
        )
        .unwrap();
        let first = GraphScope::new(
            root.child(NamespaceId::new("tenant-a").unwrap()).unwrap(),
            graph_id.clone(),
        );
        let second = GraphScope::new(
            runtime
                .root_scope()
                .namespace
                .child(NamespaceId::new("tenant-b").unwrap())
                .unwrap(),
            graph_id,
        );

        let first_options = runtime.options_for_scope(&first);
        let second_options = runtime.options_for_scope(&second);
        assert_eq!(first_options.cache.object_store_cache_bytes, Some(100));
        assert_eq!(second_options.cache.object_store_cache_bytes, Some(100));
        assert_ne!(
            first_options.cache.object_store_cache_dir,
            second_options.cache.object_store_cache_dir
        );
        assert!(first_options
            .cache
            .object_store_cache_dir
            .unwrap()
            .ends_with("production/tenant-a/graphs/hydradb"));
    }
}

/// Touch point (b): the promotion gate, in all the states this layer can reach.
#[cfg(test)]
mod placement_gate_tests {
    use super::*;

    use std::fmt;
    use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};

    use async_trait::async_trait;
    use futures::stream::BoxStream;
    use futures::StreamExt;
    use slatedb::object_store::memory::InMemory;
    use slatedb::object_store::path::Path;
    use slatedb::object_store::{
        CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
        PutMultipartOptions, PutOptions, PutPayload, PutResult,
    };

    const CELL: &str = "cell-a";
    const FLEET: &[&str] = &["node-a", "node-b", "node-c"];

    fn scope_key() -> String {
        GraphScope::default().to_string()
    }

    /// The node rendezvous names for `CELL`, and one that it does not.
    fn owner_and_peer() -> (&'static str, &'static str) {
        let owner = turbolay_placement::hash::owner(&scope_key(), CELL, FLEET)
            .expect("a non-empty fleet has an owner");
        let peer = FLEET
            .iter()
            .copied()
            .find(|node_id| *node_id != owner)
            .expect("three nodes cannot all be the owner");
        (owner, peer)
    }

    fn fleet_view(local_node_id: &str, config: PlacementConfig) -> PlacementView {
        PlacementView::new(local_node_id, FLEET.iter().copied(), config).expect("a valid fleet")
    }

    async fn open_cluster(
        base_path: &str,
        local_node_id: &str,
        placement: PlacementView,
        object_store: Arc<dyn ObjectStore>,
    ) -> RoutedGraphCluster {
        RoutedGraphCluster::open_promotable(
            base_path,
            local_node_id,
            ObjectStoreNodeDirectory::new([CELL], FLEET.iter().copied())
                .expect("a valid directory"),
            placement,
            object_store,
        )
        .await
        .expect("the cluster opens")
    }

    fn edge(key: &str) -> crate::EdgeMutation {
        crate::EdgeMutation {
            cell_id: CELL.to_string(),
            edge_type: "FOLLOWS".to_string(),
            src: 1,
            dst: 2,
            idempotency_key: key.to_string(),
        }
    }

    /// Whether the shard is actually holding the SlateDB writer, which is what
    /// "refused rather than promoted" has to mean to be worth anything.
    fn holds_the_writer(cluster: &RoutedGraphCluster) -> bool {
        cluster
            .shard(CELL)
            .expect("the shard is open")
            .db
            .writer()
            .is_ok()
    }

    /// The owner promotes and the write lands.
    #[tokio::test]
    async fn the_rendezvous_owner_opens_the_writer() {
        let (owner, _) = owner_and_peer();
        let cluster = open_cluster(
            "gate/owner",
            owner,
            fleet_view(owner, PlacementConfig::default()),
            Arc::new(InMemory::new()),
        )
        .await;

        cluster.write_edge(edge("owner-write")).await.unwrap();
        assert!(holds_the_writer(&cluster));
        cluster.close().await.unwrap();
    }

    /// The branch that ends the duel: a node that is not the owner refuses, and
    /// — the part that matters — leaves the writer alone rather than taking the
    /// epoch from whoever holds it. The peer is named so the driver can
    /// re-route instead of retrying into the same wrong node.
    #[tokio::test]
    async fn a_non_owner_refuses_the_write_and_does_not_promote() {
        let (owner, peer) = owner_and_peer();
        let cluster = open_cluster(
            "gate/non-owner",
            peer,
            fleet_view(peer, PlacementConfig::default()),
            Arc::new(InMemory::new()),
        )
        .await;

        let error = cluster
            .write_edge(edge("non-owner-write"))
            .await
            .unwrap_err();
        assert!(
            matches!(
                &error,
                GraphError::NotCellWriter { cell_id, owner: Some(hint) }
                    if cell_id == CELL && hint == owner
            ),
            "expected a hinted refusal, got {error:?}"
        );
        assert!(
            !holds_the_writer(&cluster),
            "a refusal that still opened the writer is the duel"
        );
        cluster.close().await.unwrap();
    }

    /// Decision 7 at the gate: a node past its LIST grace knows nothing about
    /// the fleet, so it refuses **with no hint** — a stale guess would point at
    /// a node that may itself have shed — and it must not promote, because a
    /// partitioned node that promotes can never learn it lost.
    #[tokio::test]
    async fn a_shed_view_refuses_with_no_hint_and_does_not_promote() {
        let store = ListFailingStore::new();
        // Milliseconds rather than the 5s/15s defaults, so grace expires within
        // the test instead of being slept through. Decision 10 gives the crate's
        // own rules this property for free; this is the cheapest way to extend
        // it to a layer that measures real elapsed time.
        let config = PlacementConfig {
            heartbeat_interval: Duration::from_millis(1),
            heartbeat_timeout: Duration::from_millis(5),
        };
        let (_, peer) = owner_and_peer();
        let placement = fleet_view(peer, config);
        let cluster = open_cluster(
            "gate/shed",
            peer,
            placement.clone(),
            Arc::clone(&store) as Arc<dyn ObjectStore>,
        )
        .await;

        store.break_list();
        tokio::time::sleep(config.heartbeat_timeout * 2).await;
        let view = placement
            .refresh(store.as_ref(), &Path::from("gate/shed"))
            .await;
        assert_eq!(view.candidates(), None, "the view must have shed");

        let error = cluster.write_edge(edge("shed-write")).await.unwrap_err();
        assert!(
            matches!(&error, GraphError::NotCellWriter { cell_id, owner: None } if cell_id == CELL),
            "expected an unhinted refusal, got {error:?}"
        );
        assert!(!holds_the_writer(&cluster));
        cluster.close().await.unwrap();
    }

    /// The fourth state, and the one this layer cannot reach on its own:
    /// `LiveNodeSet` always counts the local node live, so a *successful* view
    /// never has an empty candidate list. The seam's own tests build one by
    /// hand; what is pinned here is the rule the gate's first arm implements —
    /// `Unowned` and `Unknown` are both "nobody was named" and have opposite
    /// answers, and folding them together is how the permanent-refusal trap
    /// gets built.
    #[test]
    fn a_known_empty_fleet_licenses_promotion_where_a_shed_view_does_not() {
        assert!(CellOwnership::Unowned.may_promote());
        assert_eq!(CellOwnership::Unowned.owner_hint(), None);
        assert!(!CellOwnership::Unknown.may_promote());
        assert_eq!(CellOwnership::Unknown.owner_hint(), None);
    }

    /// Ownership is checked *before* the fence pacing, so a non-owner is
    /// refused outright instead of being made to wait first: a wait it would
    /// spend and then be refused anyway is latency bought for nothing.
    #[tokio::test]
    async fn ownership_is_refused_without_first_serving_the_reopen_delay() {
        let (_, peer) = owner_and_peer();
        let cluster = open_cluster(
            "gate/order",
            peer,
            fleet_view(peer, PlacementConfig::default()),
            Arc::new(InMemory::new()),
        )
        .await;

        let started = Instant::now();
        let error = cluster.write_edge(edge("ordered")).await.unwrap_err();
        assert!(matches!(error, GraphError::NotCellWriter { .. }));
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "the refusal must not wait out anything"
        );
        cluster.close().await.unwrap();
    }

    /// An `ObjectStore` whose LIST can be switched off. The kernel has no
    /// shared test double for this — `crates/placement`'s is `#[cfg(test)]`
    /// inside that crate and cannot be imported — and decision 7's state is
    /// only reachable through a failing LIST.
    struct ListFailingStore {
        inner: Arc<dyn ObjectStore>,
        fail_list: AtomicBool,
    }

    impl ListFailingStore {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                inner: Arc::new(InMemory::new()),
                fail_list: AtomicBool::new(false),
            })
        }

        fn break_list(&self) {
            self.fail_list.store(true, AtomicOrdering::SeqCst);
        }

        fn injected() -> slatedb::object_store::Error {
            slatedb::object_store::Error::Generic {
                store: "ListFailingStore",
                source: "injected LIST fault".into(),
            }
        }
    }

    impl fmt::Display for ListFailingStore {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "ListFailingStore({})", self.inner)
        }
    }

    impl fmt::Debug for ListFailingStore {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "ListFailingStore({:?})", self.inner)
        }
    }

    #[async_trait]
    impl ObjectStore for ListFailingStore {
        async fn put_opts(
            &self,
            location: &Path,
            payload: PutPayload,
            opts: PutOptions,
        ) -> slatedb::object_store::Result<PutResult> {
            self.inner.put_opts(location, payload, opts).await
        }

        async fn put_multipart_opts(
            &self,
            location: &Path,
            opts: PutMultipartOptions,
        ) -> slatedb::object_store::Result<Box<dyn MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }

        async fn get_opts(
            &self,
            location: &Path,
            options: GetOptions,
        ) -> slatedb::object_store::Result<GetResult> {
            self.inner.get_opts(location, options).await
        }

        fn delete_stream(
            &self,
            locations: BoxStream<'static, slatedb::object_store::Result<Path>>,
        ) -> BoxStream<'static, slatedb::object_store::Result<Path>> {
            self.inner.delete_stream(locations)
        }

        fn list(
            &self,
            prefix: Option<&Path>,
        ) -> BoxStream<'static, slatedb::object_store::Result<ObjectMeta>> {
            if self.fail_list.load(AtomicOrdering::SeqCst) {
                return futures::stream::once(async { Err(Self::injected()) }).boxed();
            }
            self.inner.list(prefix)
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&Path>,
        ) -> slatedb::object_store::Result<ListResult> {
            if self.fail_list.load(AtomicOrdering::SeqCst) {
                return Err(Self::injected());
            }
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy_opts(
            &self,
            from: &Path,
            to: &Path,
            options: CopyOptions,
        ) -> slatedb::object_store::Result<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }
}
