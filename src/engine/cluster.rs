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
        object_store: Arc<dyn ObjectStore>,
    ) -> Result<Self> {
        Self::open_readers_scoped(
            base_path,
            GraphScope::default(),
            local_node_id,
            directory,
            object_store,
        )
        .await
    }

    pub async fn open_promotable(
        base_path: impl Into<String>,
        local_node_id: impl Into<String>,
        directory: ObjectStoreNodeDirectory,
        object_store: Arc<dyn ObjectStore>,
    ) -> Result<Self> {
        Self::open_promotable_scoped_with_memory_options(
            base_path,
            GraphScope::default(),
            local_node_id,
            directory,
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
        object_store: Arc<dyn ObjectStore>,
        options: GraphOpenOptions,
    ) -> Result<Self> {
        Self::open_promotable_scoped_with_memory_options(
            base_path,
            GraphScope::default(),
            local_node_id,
            directory,
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
        object_store: Arc<dyn ObjectStore>,
    ) -> Result<Self> {
        let base_path = scope.scoped_store_path(&base_path.into());
        Self::open_at_path(RoutedClusterOpenConfig {
            base_path,
            scope,
            local_node_id: local_node_id.into(),
            directory,
            object_store,
            promotable: false,
            options: GraphOpenOptions::default(),
            memory: GraphMemoryConfig::default(),
        })
        .await
    }

    pub async fn open_promotable_scoped_with_memory_options(
        base_path: impl Into<String>,
        scope: GraphScope,
        local_node_id: impl Into<String>,
        directory: ObjectStoreNodeDirectory,
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
            shards,
            promotable,
        })
    }

    pub fn local_node_id(&self) -> &str {
        &self.local_node_id
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
        shard.promote_to_writer(cell_id, "routed_write").await
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
