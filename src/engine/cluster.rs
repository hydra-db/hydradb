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

impl ShardPlacement {
    pub fn fixed(
        assignments: impl IntoIterator<Item = (impl Into<String>, impl Into<String>)>,
    ) -> Result<Self> {
        let mut owners = BTreeMap::new();
        for (cell_id, node_id) in assignments {
            let cell_id = cell_id.into();
            let node_id = node_id.into();
            validate_component("cell_id", &cell_id)?;
            validate_component("node_id", &node_id)?;
            if owners.insert(cell_id.clone(), node_id).is_some() {
                return Err(GraphError::CorruptValue {
                    key: format!("placement/{cell_id}"),
                    reason: "duplicate cell placement".to_string(),
                });
            }
        }
        if owners.is_empty() {
            return Err(GraphError::CorruptValue {
                key: "placement".to_string(),
                reason: "at least one cell placement is required".to_string(),
            });
        }
        Ok(Self { owners })
    }

    pub fn rendezvous(
        cell_ids: impl IntoIterator<Item = impl Into<String>>,
        node_ids: impl IntoIterator<Item = impl Into<String>>,
    ) -> Result<Self> {
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
                key: "placement".to_string(),
                reason: "at least one owner node is required".to_string(),
            });
        }

        let mut owners = BTreeMap::new();
        for cell_id in cell_ids {
            let cell_id = cell_id.into();
            validate_component("cell_id", &cell_id)?;
            let Some(owner) = nodes
                .iter()
                .max_by_key(|node_id| rendezvous_score(&cell_id, node_id))
                .cloned()
            else {
                return Err(GraphError::CorruptValue {
                    key: "placement".to_string(),
                    reason: "at least one owner node is required".to_string(),
                });
            };
            if owners.insert(cell_id.clone(), owner).is_some() {
                return Err(GraphError::CorruptValue {
                    key: format!("placement/{cell_id}"),
                    reason: "duplicate cell placement".to_string(),
                });
            }
        }
        if owners.is_empty() {
            return Err(GraphError::CorruptValue {
                key: "placement".to_string(),
                reason: "at least one cell placement is required".to_string(),
            });
        }
        Ok(Self { owners })
    }

    pub fn owner(&self, cell_id: &str) -> Result<&str> {
        validate_component("cell_id", cell_id)?;
        self.owners
            .get(cell_id)
            .map(String::as_str)
            .ok_or_else(|| GraphError::UnknownShard {
                cell_id: cell_id.to_string(),
            })
    }

    pub fn ensure_local_owner(&self, local_node_id: &str, cell_id: &str) -> Result<()> {
        validate_component("node_id", local_node_id)?;
        let owner = self.owner(cell_id)?;
        if owner == local_node_id {
            Ok(())
        } else {
            Err(GraphError::ShardNotOwned {
                cell_id: cell_id.to_string(),
                owner_node_id: owner.to_string(),
                local_node_id: local_node_id.to_string(),
            })
        }
    }

    pub fn cells_for_node(&self, node_id: &str) -> Result<Vec<String>> {
        validate_component("node_id", node_id)?;
        Ok(self
            .owners
            .iter()
            .filter_map(|(cell_id, owner)| (owner == node_id).then_some(cell_id.clone()))
            .collect())
    }

    pub fn cells(&self) -> impl Iterator<Item = &str> {
        self.owners.keys().map(String::as_str)
    }

    pub fn node_ids(&self) -> impl Iterator<Item = &str> {
        let mut nodes = self.owners.values().map(String::as_str).collect::<Vec<_>>();
        nodes.sort_unstable();
        nodes.dedup();
        nodes.into_iter()
    }
}

struct RoutedClusterOpenConfig {
    base_path: String,
    scope: GraphScope,
    local_node_id: String,
    placement: ShardPlacement,
    object_store: Arc<dyn ObjectStore>,
    writable: bool,
    options: GraphOpenOptions,
    memory: GraphMemoryConfig,
}

impl RoutedGraphCluster {
    pub async fn open_owned(
        base_path: impl Into<String>,
        local_node_id: impl Into<String>,
        placement: ShardPlacement,
        object_store: Arc<dyn ObjectStore>,
    ) -> Result<Self> {
        Self::open_owned_scoped(
            base_path,
            GraphScope::default(),
            local_node_id,
            placement,
            object_store,
        )
        .await
    }

    pub async fn open_fenced_owned(
        base_path: impl Into<String>,
        local_node_id: impl Into<String>,
        placement: ShardPlacement,
        object_store: Arc<dyn ObjectStore>,
    ) -> Result<Self> {
        Self::open_fenced_owned_scoped_with_memory_options(
            base_path,
            GraphScope::default(),
            local_node_id,
            placement,
            object_store,
            GraphOpenOptions::default(),
            GraphMemoryConfig::default(),
        )
        .await
    }

    pub async fn open_fenced_owned_with_options(
        base_path: impl Into<String>,
        local_node_id: impl Into<String>,
        placement: ShardPlacement,
        object_store: Arc<dyn ObjectStore>,
        options: GraphOpenOptions,
    ) -> Result<Self> {
        Self::open_fenced_owned_scoped_with_memory_options(
            base_path,
            GraphScope::default(),
            local_node_id,
            placement,
            object_store,
            options,
            GraphMemoryConfig::default(),
        )
        .await
    }

    pub async fn open_owned_scoped(
        base_path: impl Into<String>,
        scope: GraphScope,
        local_node_id: impl Into<String>,
        placement: ShardPlacement,
        object_store: Arc<dyn ObjectStore>,
    ) -> Result<Self> {
        let base_path = scope.scoped_store_path(&base_path.into());
        Self::open_at_path(RoutedClusterOpenConfig {
            base_path,
            scope,
            local_node_id: local_node_id.into(),
            placement,
            object_store,
            writable: false,
            options: GraphOpenOptions::default(),
            memory: GraphMemoryConfig::default(),
        })
        .await
    }

    pub async fn open_fenced_owned_scoped_with_memory_options(
        base_path: impl Into<String>,
        scope: GraphScope,
        local_node_id: impl Into<String>,
        placement: ShardPlacement,
        object_store: Arc<dyn ObjectStore>,
        options: GraphOpenOptions,
        memory: GraphMemoryConfig,
    ) -> Result<Self> {
        let base_path = scope.scoped_store_path(&base_path.into());
        Self::open_at_path(RoutedClusterOpenConfig {
            base_path,
            scope,
            local_node_id: local_node_id.into(),
            placement,
            object_store,
            writable: true,
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
            placement,
            object_store,
            writable,
            options,
            memory,
        } = config;
        validate_component("node_id", &local_node_id)?;
        for cell_id in placement.cells() {
            validate_component("cell_id", cell_id)?;
        }
        let mut shards = BTreeMap::new();
        for cell_id in placement.cells_for_node(&local_node_id)? {
            let path = format!("{base_path}/{cell_id}");
            let opened = if writable {
                GraphShard::open_standalone_writer_with_memory_options(
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
            if shard
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
            placement,
            shards,
            writable,
            maintenance_metrics: Arc::new(GraphNodeMaintenanceMetrics::default()),
        })
    }

    pub fn local_node_id(&self) -> &str {
        &self.local_node_id
    }
    pub fn scope(&self) -> &GraphScope {
        &self.scope
    }
    pub fn placement(&self) -> &ShardPlacement {
        &self.placement
    }
    pub fn local_cells(&self) -> Vec<&str> {
        self.shards.keys().map(String::as_str).collect()
    }
    pub fn maintenance_metrics(&self) -> GraphNodeMaintenanceMetricsSnapshot {
        self.maintenance_metrics.snapshot()
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
        self.placement
            .ensure_local_owner(&self.local_node_id, cell_id)?;
        self.shards
            .get(cell_id)
            .map(Arc::as_ref)
            .ok_or_else(|| GraphError::UnknownShard {
                cell_id: cell_id.to_string(),
            })
    }

    pub(crate) fn ensure_local_writer(&self, cell_id: &str) -> Result<()> {
        self.placement
            .ensure_local_owner(&self.local_node_id, cell_id)?;
        if !self.writable {
            return Err(GraphError::WriteRequiresWriter {
                operation: "routed_write",
                cell_id: cell_id.to_string(),
            });
        }
        self.shards
            .get(cell_id)
            .ok_or_else(|| GraphError::UnknownShard {
                cell_id: cell_id.to_string(),
            })?
            .db
            .writer()?;
        Ok(())
    }

    pub async fn write_edge(&self, mutation: crate::EdgeMutation) -> Result<crate::CommitResult> {
        let shard = self.shard(&mutation.cell_id)?;
        self.ensure_local_writer(&mutation.cell_id)?;
        shard.write_edge(mutation).await
    }

    pub async fn delete_edge(&self, mutation: crate::EdgeMutation) -> Result<crate::DeleteResult> {
        let shard = self.shard(&mutation.cell_id)?;
        self.ensure_local_writer(&mutation.cell_id)?;
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
        self.ensure_local_writer(cell_id)?;
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
        self.ensure_local_writer(cell_id)?;
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
        self.ensure_local_writer(cell_id)?;
        shard.delete_edge_mutations_batch(cell_id, mutations).await
    }

    pub async fn delete_vertex(
        &self,
        cell_id: &str,
        vertex_id: crate::VertexId,
        idempotency_key: &str,
    ) -> Result<crate::VertexDeleteResult> {
        let shard = self.shard(cell_id)?;
        self.ensure_local_writer(cell_id)?;
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
        self.ensure_local_writer(cell_id)?;
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
        self.ensure_local_writer(cell_id)?;
        shard.drop_cell(cell_id, idempotency_key).await
    }

    pub async fn ingest_edge_mutations(
        &self,
        cell_id: &str,
        mutations: impl IntoIterator<Item = crate::EdgeMutation>,
        options: crate::EdgeIngestOptions,
    ) -> Result<crate::EdgeIngestResult> {
        let shard = self.shard(cell_id)?;
        self.ensure_local_writer(cell_id)?;
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
        self.ensure_local_writer(cell_id)?;
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
        self.ensure_local_writer(cell_id)?;
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
        self.ensure_local_writer(cell_id)?;
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
        self.ensure_local_writer(cell_id)?;
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
        self.ensure_local_writer(cell_id)?;
        shard.set_vertex_metadata_batch(cell_id, updates).await
    }

    pub async fn import_vertex_metadata_batch(
        &self,
        cell_id: &str,
        updates: impl IntoIterator<Item = (crate::VertexId, crate::VertexMetadata)>,
    ) -> Result<usize> {
        let shard = self.shard(cell_id)?;
        self.ensure_local_writer(cell_id)?;
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
        self.ensure_local_writer(cell_id)?;
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
        self.ensure_local_writer(cell_id)?;
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
        self.ensure_local_writer(cell_id)?;
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
        self.ensure_local_writer(cell_id)?;
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
            self.ensure_local_writer(&plan.cell_id)?;
        }
        shard.execute_query_plan(plan).await
    }

    pub async fn execute_query_plan(&self, plan: crate::QueryPlan) -> Result<crate::QueryOutput> {
        let shard = self.shard(&plan.cell_id)?;
        if plan.is_write() {
            self.ensure_local_writer(&plan.cell_id)?;
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
            self.ensure_local_writer(&context.cell_id)?;
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
