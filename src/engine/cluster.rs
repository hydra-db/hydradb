use super::*;

impl LeaseRenewalHandle {
    pub async fn stop(self) -> Result<()> {
        let _ = self.stop_tx.send(true);
        match self.task.await {
            Ok(result) => result,
            Err(err) => Err(GraphError::CorruptValue {
                key: "control/lease_renewer".to_string(),
                reason: format!("lease renewer task failed: {err}"),
            }),
        }
    }
}

impl GraphNode {
    pub async fn open(
        base_path: impl Into<String>,
        local_node_id: impl Into<String>,
        control: Arc<GraphControlPlane>,
        object_store: Arc<dyn ObjectStore>,
        lease_ttl: Duration,
        lease_renew_interval: Duration,
    ) -> Result<Self> {
        Self::open_with_options(
            base_path,
            local_node_id,
            control,
            object_store,
            lease_ttl,
            lease_renew_interval,
            GraphOpenOptions::default(),
        )
        .await
    }

    pub async fn open_with_options(
        base_path: impl Into<String>,
        local_node_id: impl Into<String>,
        control: Arc<GraphControlPlane>,
        object_store: Arc<dyn ObjectStore>,
        lease_ttl: Duration,
        lease_renew_interval: Duration,
        options: GraphOpenOptions,
    ) -> Result<Self> {
        let cluster = RoutedGraphCluster::open_owned_with_control_and_options(
            base_path,
            local_node_id,
            control.as_ref(),
            object_store,
            lease_ttl,
            options,
        )
        .await?;
        let lease_renewer =
            cluster.start_lease_renewer(control, lease_ttl, lease_renew_interval)?;
        Ok(Self {
            cluster,
            lease_renewer,
        })
    }

    pub fn cluster(&self) -> &RoutedGraphCluster {
        &self.cluster
    }

    pub async fn close(self) -> Result<()> {
        self.lease_renewer.stop().await?;
        self.cluster.close().await
    }
}

impl GraphCluster {
    pub async fn open_cells(
        base_path: impl Into<String>,
        cell_ids: impl IntoIterator<Item = impl Into<String>>,
        object_store: Arc<dyn ObjectStore>,
    ) -> Result<Self> {
        let base_path = base_path.into();
        let mut shards = BTreeMap::new();
        for cell_id in cell_ids {
            let cell_id = cell_id.into();
            validate_component("cell_id", &cell_id)?;
            let path = format!("{base_path}/{cell_id}");
            let shard = GraphShard::open(path, Arc::clone(&object_store)).await?;
            shards.insert(cell_id, shard);
        }
        Ok(Self { shards })
    }

    pub async fn open_cells_standalone_writers(
        base_path: impl Into<String>,
        cell_ids: impl IntoIterator<Item = impl Into<String>>,
        object_store: Arc<dyn ObjectStore>,
    ) -> Result<Self> {
        let base_path = base_path.into();
        let mut shards = BTreeMap::new();
        for cell_id in cell_ids {
            let cell_id = cell_id.into();
            validate_component("cell_id", &cell_id)?;
            let path = format!("{base_path}/{cell_id}");
            let shard = GraphShard::open_standalone_writer(path, Arc::clone(&object_store)).await?;
            shards.insert(cell_id, shard);
        }
        Ok(Self { shards })
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
            let owner = nodes
                .iter()
                .max_by_key(|node_id| rendezvous_score(&cell_id, node_id))
                .expect("nodes is not empty")
                .clone();
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

impl RoutedGraphCluster {
    pub async fn open_owned(
        base_path: impl Into<String>,
        local_node_id: impl Into<String>,
        placement: ShardPlacement,
        object_store: Arc<dyn ObjectStore>,
    ) -> Result<Self> {
        let base_path = base_path.into();
        let local_node_id = local_node_id.into();
        validate_component("node_id", &local_node_id)?;

        let mut shards = BTreeMap::new();
        for cell_id in placement.cells_for_node(&local_node_id)? {
            let path = format!("{base_path}/{cell_id}");
            let shard = GraphShard::open(path, Arc::clone(&object_store)).await?;
            shards.insert(cell_id, shard);
        }

        Ok(Self {
            local_node_id,
            placement,
            shards,
            leases: Arc::new(RwLock::new(BTreeMap::new())),
        })
    }

    pub async fn open_owned_with_control(
        base_path: impl Into<String>,
        local_node_id: impl Into<String>,
        control: &GraphControlPlane,
        object_store: Arc<dyn ObjectStore>,
        lease_ttl: Duration,
    ) -> Result<Self> {
        Self::open_owned_with_control_and_options(
            base_path,
            local_node_id,
            control,
            object_store,
            lease_ttl,
            GraphOpenOptions::default(),
        )
        .await
    }

    pub async fn open_owned_with_control_and_options(
        base_path: impl Into<String>,
        local_node_id: impl Into<String>,
        control: &GraphControlPlane,
        object_store: Arc<dyn ObjectStore>,
        lease_ttl: Duration,
        options: GraphOpenOptions,
    ) -> Result<Self> {
        let base_path = base_path.into();
        let local_node_id = local_node_id.into();
        let placement = control.load_placement().await?;
        validate_component("node_id", &local_node_id)?;
        let local_cells = placement.cells_for_node(&local_node_id)?;
        let leases = Arc::new(RwLock::new(BTreeMap::new()));
        for cell_id in &local_cells {
            let lease = control
                .acquire_lease(cell_id, &local_node_id, lease_ttl)
                .await?;
            leases
                .write()
                .map_err(lock_error)?
                .insert(cell_id.clone(), lease);
        }

        let mut shards = BTreeMap::new();
        for cell_id in local_cells {
            let path = format!("{base_path}/{cell_id}");
            let shard = GraphShard::open_leased_writer(
                path,
                Arc::clone(&object_store),
                options.clone(),
                local_node_id.clone(),
                Arc::clone(&leases),
            )
            .await?;
            let lease = leases
                .read()
                .map_err(lock_error)?
                .get(&cell_id)
                .cloned()
                .ok_or_else(|| GraphError::WriteRequiresLease {
                    operation: "install_write_fence",
                    cell_id: cell_id.clone(),
                })?;
            shard.install_write_fence(&cell_id, &lease).await?;
            shards.insert(cell_id, shard);
        }

        Ok(Self {
            local_node_id,
            placement,
            shards,
            leases,
        })
    }

    pub fn local_node_id(&self) -> &str {
        &self.local_node_id
    }

    pub fn placement(&self) -> &ShardPlacement {
        &self.placement
    }

    pub fn local_cells(&self) -> Vec<&str> {
        self.shards.keys().map(String::as_str).collect()
    }

    pub fn lease(&self, cell_id: &str) -> Option<ShardLease> {
        self.leases
            .read()
            .ok()
            .and_then(|leases| leases.get(cell_id).cloned())
    }

    pub async fn renew_leases(
        &mut self,
        control: &GraphControlPlane,
        lease_ttl: Duration,
    ) -> Result<()> {
        let leases: Vec<_> = self
            .leases
            .read()
            .map_err(lock_error)?
            .values()
            .cloned()
            .collect();
        for lease in leases {
            let renewed = control.renew_lease(&lease, lease_ttl).await?;
            self.leases
                .write()
                .map_err(lock_error)?
                .insert(renewed.cell_id.clone(), renewed);
        }
        Ok(())
    }

    pub fn start_lease_renewer(
        &self,
        control: Arc<GraphControlPlane>,
        lease_ttl: Duration,
        interval: Duration,
    ) -> Result<LeaseRenewalHandle> {
        if interval.is_zero() {
            return Err(GraphError::CorruptValue {
                key: "control/lease_renew_interval".to_string(),
                reason: "lease renewal interval must be greater than zero".to_string(),
            });
        }
        let leases = Arc::clone(&self.leases);
        let (stop_tx, mut stop_rx) = watch::channel(false);
        let task = tokio::spawn(async move {
            loop {
                tokio::select! {
                    changed = stop_rx.changed() => {
                        if changed.is_err() || *stop_rx.borrow() {
                            break;
                        }
                    }
                    _ = tokio::time::sleep(interval) => {
                        let current: Vec<_> = leases
                            .read()
                            .map_err(lock_error)?
                            .values()
                            .cloned()
                            .collect();
                        for lease in current {
                            match control.renew_lease(&lease, lease_ttl).await {
                                Ok(renewed) => {
                                    leases
                                        .write()
                                        .map_err(lock_error)?
                                        .insert(renewed.cell_id.clone(), renewed);
                                }
                                Err(GraphError::StaleShardLease { cell_id, .. }) => {
                                    tracing::warn!(
                                        target: "slatedb_graph_kernel",
                                        cell_id,
                                        "lease renewal lost shard lease"
                                    );
                                    leases.write().map_err(lock_error)?.remove(&cell_id);
                                }
                                Err(GraphError::Slate(err))
                                    if err.kind() == ErrorKind::Transaction =>
                                {
                                    tracing::warn!(
                                        target: "slatedb_graph_kernel",
                                        cell_id = %lease.cell_id,
                                        error = %err,
                                        "lease renewal transaction conflict"
                                    );
                                    tokio::task::yield_now().await;
                                }
                                Err(err) => {
                                    tracing::warn!(
                                        target: "slatedb_graph_kernel",
                                        cell_id = %lease.cell_id,
                                        error = %err,
                                        "lease renewal failed"
                                    );
                                    tokio::task::yield_now().await;
                                }
                            }
                        }
                    }
                }
            }
            Ok(())
        });
        Ok(LeaseRenewalHandle { stop_tx, task })
    }

    pub fn shard(&self, cell_id: &str) -> Result<&GraphShard> {
        self.placement
            .ensure_local_owner(&self.local_node_id, cell_id)?;
        self.shards
            .get(cell_id)
            .ok_or_else(|| GraphError::UnknownShard {
                cell_id: cell_id.to_string(),
            })
    }

    fn ensure_active_write_lease(&self, cell_id: &str) -> Result<()> {
        if let Some(lease) = self
            .leases
            .read()
            .map_err(lock_error)?
            .get(cell_id)
            .cloned()
        {
            if lease.owner_node_id == self.local_node_id && lease.expires_at_ms > now_millis() {
                return Ok(());
            }
            return Err(GraphError::StaleShardLease {
                cell_id: cell_id.to_string(),
                node_id: self.local_node_id.clone(),
                lease_token: lease.lease_token,
            });
        }
        Err(GraphError::WriteRequiresLease {
            operation: "routed_write",
            cell_id: cell_id.to_string(),
        })
    }

    pub async fn write_edge(&self, mutation: crate::EdgeMutation) -> Result<crate::CommitResult> {
        let shard = self.shard(&mutation.cell_id)?;
        self.ensure_active_write_lease(&mutation.cell_id)?;
        shard.write_edge(mutation).await
    }

    pub async fn delete_edge(&self, mutation: crate::EdgeMutation) -> Result<crate::DeleteResult> {
        let shard = self.shard(&mutation.cell_id)?;
        self.ensure_active_write_lease(&mutation.cell_id)?;
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
        self.ensure_active_write_lease(cell_id)?;
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
        self.ensure_active_write_lease(cell_id)?;
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
        self.ensure_active_write_lease(cell_id)?;
        shard.delete_edge_mutations_batch(cell_id, mutations).await
    }

    pub async fn delete_vertex(
        &self,
        cell_id: &str,
        vertex_id: crate::VertexId,
        idempotency_key: &str,
    ) -> Result<crate::VertexDeleteResult> {
        let shard = self.shard(cell_id)?;
        self.ensure_active_write_lease(cell_id)?;
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
        self.ensure_active_write_lease(cell_id)?;
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
        self.ensure_active_write_lease(cell_id)?;
        shard.drop_cell(cell_id, idempotency_key).await
    }

    pub async fn ingest_edge_mutations(
        &self,
        cell_id: &str,
        mutations: impl IntoIterator<Item = crate::EdgeMutation>,
        options: crate::EdgeIngestOptions,
    ) -> Result<crate::EdgeIngestResult> {
        let shard = self.shard(cell_id)?;
        self.ensure_active_write_lease(cell_id)?;
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
        self.ensure_active_write_lease(cell_id)?;
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
        self.ensure_active_write_lease(cell_id)?;
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
        self.ensure_active_write_lease(cell_id)?;
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
        self.ensure_active_write_lease(cell_id)?;
        shard
            .set_vertex_metadata(cell_id, vertex_id, metadata)
            .await
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
        self.ensure_active_write_lease(cell_id)?;
        shard
            .set_edge_metadata(cell_id, edge_type, src, dst, metadata)
            .await
    }

    pub async fn execute_query_statement(
        &self,
        context: crate::QueryContext,
        statement: crate::QueryStatement,
    ) -> Result<crate::QueryOutput> {
        let shard = self.shard(&context.cell_id)?;
        let plan = shard.plan_query_statement(context, statement)?;
        if plan.is_write() {
            self.ensure_active_write_lease(&plan.cell_id)?;
        }
        shard.execute_query_plan(plan).await
    }

    pub async fn execute_query_plan(&self, plan: crate::QueryPlan) -> Result<crate::QueryOutput> {
        let shard = self.shard(&plan.cell_id)?;
        if plan.is_write() {
            self.ensure_active_write_lease(&plan.cell_id)?;
        }
        shard.execute_query_plan(plan).await
    }

    #[cfg(feature = "opencypher")]
    pub async fn execute_cypher(
        &self,
        context: crate::QueryContext,
        query: &str,
    ) -> Result<crate::QueryOutput> {
        let shard = self.shard(&context.cell_id)?;
        let requires_write_lease =
            if crate::parse_opencypher_mutation_query_with_parameters(query, &context.parameters)?
                .is_some()
            {
                true
            } else {
                crate::parse_opencypher_with_parameters(query, &context.parameters)
                    .map(|parsed| parsed.statement.is_write())
                    .unwrap_or(false)
            };
        if requires_write_lease {
            self.ensure_active_write_lease(&context.cell_id)?;
        }
        shard.execute_cypher(context, query).await
    }

    #[cfg(feature = "opencypher")]
    pub async fn execute_cypher_rows(
        &self,
        context: crate::QueryContext,
        query: &str,
    ) -> Result<crate::QueryResultSet> {
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

    #[cfg(feature = "opencypher")]
    pub fn explain_cypher(
        &self,
        context: crate::QueryContext,
        query: &str,
    ) -> Result<crate::QueryPlan> {
        let shard = self.shard(&context.cell_id)?;
        shard.explain_cypher(context, query)
    }

    pub async fn close(&self) -> Result<()> {
        for shard in self.shards.values() {
            shard.close().await?;
        }
        Ok(())
    }
}
