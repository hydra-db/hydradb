use super::*;

impl GraphShard {
    pub async fn execute_cypher(&self, context: QueryContext, query: &str) -> Result<QueryOutput> {
        self.execute_opencypher(context, query).await
    }

    pub async fn execute_opencypher(
        &self,
        context: QueryContext,
        query: &str,
    ) -> Result<QueryOutput> {
        #[cfg(feature = "opencypher")]
        {
            let statement = parse_opencypher(query)?;
            self.execute_query_statement(context, statement).await
        }
        #[cfg(not(feature = "opencypher"))]
        {
            let _ = (context, query);
            Err(GraphError::UnsupportedQuery {
                dialect: "OpenCypher",
                feature: "enable the opencypher Cargo feature to parse Cypher".to_string(),
            })
        }
    }

    pub async fn execute_query_statement(
        &self,
        context: QueryContext,
        statement: QueryStatement,
    ) -> Result<QueryOutput> {
        let plan = self.plan_query_statement(context, statement)?;
        self.execute_query_plan(plan).await
    }

    pub fn plan_query_statement(
        &self,
        context: QueryContext,
        statement: QueryStatement,
    ) -> Result<QueryPlan> {
        QueryPlanner::plan(&context, &statement)
    }

    #[cfg(feature = "opencypher")]
    pub fn plan_opencypher(&self, context: QueryContext, query: &str) -> Result<QueryPlan> {
        let statement = parse_opencypher(query)?;
        self.plan_query_statement(context, statement)
    }

    #[cfg(feature = "opencypher")]
    pub fn explain_cypher(&self, context: QueryContext, query: &str) -> Result<QueryPlan> {
        self.plan_opencypher(context, query)
    }

    pub async fn execute_query_plan(&self, plan: QueryPlan) -> Result<QueryOutput> {
        self.validate_executable_query_plan(&plan).await?;
        match plan.physical {
            PhysicalQueryPlan::WriteEdge {
                edge_type,
                src,
                dst,
            } => {
                let result = self
                    .write_edge(EdgeMutation {
                        cell_id: plan.cell_id,
                        edge_type,
                        src,
                        dst,
                        idempotency_key: plan.idempotency_key,
                    })
                    .await?;
                Ok(QueryOutput::Write(result))
            }
            PhysicalQueryPlan::OutDegreeCounter { edge_type, src } => {
                let count = if let Some(read_epoch) = plan.read_epoch {
                    self.out_degree_at(&plan.cell_id, &edge_type, src, read_epoch)
                        .await?
                } else {
                    self.out_degree(&plan.cell_id, &edge_type, src).await?
                };
                Ok(QueryOutput::Count(count))
            }
            PhysicalQueryPlan::OutNeighbors { edge_type, src } => {
                let vertices = if let Some(read_epoch) = plan.read_epoch {
                    self.out_neighbors_at(&plan.cell_id, &edge_type, src, read_epoch)
                        .await?
                } else {
                    self.out_neighbors(&plan.cell_id, &edge_type, src).await?
                };
                Ok(QueryOutput::Vertices(vertices))
            }
            PhysicalQueryPlan::EdgeExistsToCount {
                edge_type,
                src,
                dst,
            } => {
                let exists = self
                    .query_edge_exists(&plan.cell_id, &edge_type, src, dst, plan.read_epoch)
                    .await?;
                Ok(QueryOutput::Count(u64::from(exists)))
            }
            PhysicalQueryPlan::EdgeExistsToVertices {
                edge_type,
                src,
                dst,
            } => {
                let exists = self
                    .query_edge_exists(&plan.cell_id, &edge_type, src, dst, plan.read_epoch)
                    .await?;
                if exists {
                    Ok(QueryOutput::Vertices(vec![dst]))
                } else {
                    Ok(QueryOutput::Vertices(Vec::new()))
                }
            }
            PhysicalQueryPlan::EdgeExistsToBool {
                edge_type,
                src,
                dst,
            } => {
                let exists = self
                    .query_edge_exists(&plan.cell_id, &edge_type, src, dst, plan.read_epoch)
                    .await?;
                Ok(QueryOutput::Bool(exists))
            }
            PhysicalQueryPlan::ReachableVertices {
                edge_type,
                src,
                min_hops,
                max_hops,
                return_count,
            } => {
                let read_epoch = match plan.read_epoch {
                    Some(read_epoch) => read_epoch,
                    None => self.current_epoch(&plan.cell_id).await?,
                };
                let vertices = self
                    .reachable_vertices_in_hop_range_at(
                        &plan.cell_id,
                        &edge_type,
                        src,
                        min_hops,
                        max_hops,
                        read_epoch,
                    )
                    .await?
                    .0;
                if return_count {
                    Ok(QueryOutput::Count(vertices.len() as u64))
                } else {
                    Ok(QueryOutput::Vertices(vertices))
                }
            }
        }
    }

    async fn validate_executable_query_plan(&self, plan: &QueryPlan) -> Result<()> {
        plan.validate_for_execution()?;
        if !plan.is_write() {
            if let Some(read_epoch) = plan.read_epoch {
                let current_epoch = self.current_epoch(&plan.cell_id).await?;
                if read_epoch > current_epoch {
                    return Err(GraphError::SnapshotAhead {
                        cell_id: plan.cell_id.clone(),
                        read_epoch,
                        current_epoch,
                    });
                }
            }
        }
        Ok(())
    }

    async fn query_edge_exists(
        &self,
        cell_id: &str,
        edge_type: &str,
        src: VertexId,
        dst: VertexId,
        read_epoch: Option<GraphEpoch>,
    ) -> Result<bool> {
        if let Some(read_epoch) = read_epoch {
            self.edge_exists_at(cell_id, edge_type, src, dst, read_epoch)
                .await
        } else {
            self.edge_exists(cell_id, edge_type, src, dst).await
        }
    }

    async fn reachable_vertices_in_hop_range_at(
        &self,
        cell_id: &str,
        edge_type: &str,
        src: VertexId,
        min_hops: u8,
        max_hops: u8,
        read_epoch: GraphEpoch,
    ) -> Result<(Vec<VertexId>, u64)> {
        validate_component("cell_id", cell_id)?;
        validate_component("edge_type", edge_type)?;
        if min_hops > max_hops {
            return Err(GraphError::UnsupportedQuery {
                dialect: "OpenCypher",
                feature: "invalid variable-length hop range".to_string(),
            });
        }
        ensure_limit(
            "cypher_match_reachable",
            u64::from(max_hops),
            u64::from(self.limits.max_traversal_hops),
        )?;

        let mut adjacency = BTreeMap::<VertexId, BTreeSet<VertexId>>::new();
        for edge in self.edges_at(cell_id, edge_type, read_epoch).await? {
            adjacency.entry(edge.src).or_default().insert(edge.dst);
        }

        let mut result = BTreeSet::new();
        if min_hops == 0 {
            result.insert(src);
        }
        if max_hops == 0 {
            return Ok((result.into_iter().collect(), 0));
        }

        let mut frontier = BTreeSet::from([src]);
        let mut edge_visits = 0_u64;
        for depth in 1..=max_hops {
            let mut next = BTreeSet::new();
            for vertex in &frontier {
                if let Some(neighbors) = adjacency.get(vertex) {
                    edge_visits = edge_visits.saturating_add(neighbors.len() as u64);
                    next.extend(neighbors.iter().copied());
                }
            }
            if depth >= min_hops {
                result.extend(next.iter().copied());
            }
            if next.is_empty() {
                break;
            }
            frontier = next;
        }
        Ok((result.into_iter().collect(), edge_visits))
    }

    pub async fn edge_exists(
        &self,
        cell_id: &str,
        edge_type: &str,
        src: VertexId,
        dst: VertexId,
    ) -> Result<bool> {
        validate_component("cell_id", cell_id)?;
        validate_component("edge_type", edge_type)?;
        let key = keys::out_edge(cell_id, edge_type, src, dst);
        if self.read_remote(&key).await?.is_some() {
            return Ok(true);
        }
        let read_epoch = self.current_epoch(cell_id).await?;
        Ok(self
            .out_segment_edge_record_at(cell_id, edge_type, src, dst, read_epoch)
            .await?
            .is_some())
    }

    pub async fn out_neighbors(
        &self,
        cell_id: &str,
        edge_type: &str,
        src: VertexId,
    ) -> Result<Vec<VertexId>> {
        validate_component("cell_id", cell_id)?;
        validate_component("edge_type", edge_type)?;
        let prefix = keys::out_prefix(cell_id, edge_type, src);
        let mut iter = self.scan_remote_prefix(&prefix).await?;
        let mut neighbors = Vec::new();
        while let Some(kv) = iter.next().await? {
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            let record = decode_edge_record(&key, &kv.value)?;
            neighbors.push(record.dst);
        }
        let read_epoch = self.current_epoch(cell_id).await?;
        let tombstones = self
            .scan_out_segment_tombstones_for_src_at(cell_id, edge_type, src, read_epoch)
            .await?;
        neighbors.extend(
            self.scan_out_segments_for_src_at(cell_id, edge_type, src, read_epoch)
                .await?
                .into_iter()
                .filter(|edge| segment_edge_visible(edge.epoch, tombstones.get(&edge.dst).copied()))
                .map(|edge| edge.dst),
        );
        neighbors.sort_unstable();
        neighbors.dedup();
        Ok(neighbors)
    }

    pub(crate) async fn out_segment_edge_record_at(
        &self,
        cell_id: &str,
        edge_type: &str,
        src: VertexId,
        dst: VertexId,
        read_epoch: GraphEpoch,
    ) -> Result<Option<EdgeRecord>> {
        let tombstone_epoch = self
            .out_segment_tombstone_epoch_at(cell_id, edge_type, src, dst, read_epoch)
            .await?;
        let mut latest = None;
        for edge in self
            .scan_out_segments_for_src_at(cell_id, edge_type, src, read_epoch)
            .await?
        {
            if edge.dst == dst && segment_edge_visible(edge.epoch, tombstone_epoch) {
                latest = Some(edge);
            }
        }
        Ok(latest)
    }

    async fn scan_out_segments_for_src_at(
        &self,
        cell_id: &str,
        edge_type: &str,
        src: VertexId,
        read_epoch: GraphEpoch,
    ) -> Result<Vec<EdgeRecord>> {
        let prefix = keys::out_segment_src_prefix(cell_id, edge_type, src);
        let mut iter = self.scan_remote_prefix(&prefix).await?;
        let mut edges = Vec::new();
        while let Some(kv) = iter.next().await? {
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            let segment = decode_out_edge_segment(&key, &kv.value)?;
            if segment.start_epoch > read_epoch {
                break;
            }
            for (epoch, dst) in segment.edges.iter().copied() {
                if epoch > read_epoch {
                    break;
                }
                edges.push(EdgeRecord {
                    cell_id: segment.cell_id.clone(),
                    edge_type: segment.edge_type.clone(),
                    src: segment.src,
                    dst,
                    epoch,
                });
            }
        }
        Ok(edges)
    }

    async fn out_segment_tombstone_epoch_at(
        &self,
        cell_id: &str,
        edge_type: &str,
        src: VertexId,
        dst: VertexId,
        read_epoch: GraphEpoch,
    ) -> Result<Option<GraphEpoch>> {
        let key = keys::out_segment_tombstone(cell_id, edge_type, src, dst);
        let Some(value) = self.read_remote(&key).await? else {
            return Ok(None);
        };
        let epoch = decode_u64(&key, &value)?;
        Ok((epoch <= read_epoch).then_some(epoch))
    }

    async fn scan_out_segment_tombstones_for_src_at(
        &self,
        cell_id: &str,
        edge_type: &str,
        src: VertexId,
        read_epoch: GraphEpoch,
    ) -> Result<BTreeMap<VertexId, GraphEpoch>> {
        let prefix = keys::out_segment_tombstone_src_prefix(cell_id, edge_type, src);
        let mut iter = self.scan_remote_prefix(&prefix).await?;
        let mut tombstones = BTreeMap::new();
        while let Some(kv) = iter.next().await? {
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            let (key_cell_id, key_edge_type, key_src, dst) =
                parse_out_edge_segment_tombstone_key(&key)?;
            if key_cell_id != cell_id || key_edge_type != edge_type || key_src != src {
                return Err(GraphError::CorruptValue {
                    key,
                    reason: "segment tombstone identity does not match scan prefix".to_string(),
                });
            }
            let epoch = decode_u64(&key, &kv.value)?;
            if epoch <= read_epoch {
                tombstones.insert(dst, epoch);
            }
        }
        Ok(tombstones)
    }

    async fn out_segment_tombstones_at(
        &self,
        cell_id: &str,
        edge_type: &str,
        read_epoch: GraphEpoch,
    ) -> Result<BTreeMap<(VertexId, VertexId), GraphEpoch>> {
        let prefix = keys::out_segment_tombstone_edge_type_prefix(cell_id, edge_type);
        let mut iter = self.scan_remote_prefix(&prefix).await?;
        let mut tombstones = BTreeMap::new();
        while let Some(kv) = iter.next().await? {
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            let (key_cell_id, key_edge_type, src, dst) =
                parse_out_edge_segment_tombstone_key(&key)?;
            if key_cell_id != cell_id || key_edge_type != edge_type {
                return Err(GraphError::CorruptValue {
                    key,
                    reason: "segment tombstone identity does not match scan prefix".to_string(),
                });
            }
            let epoch = decode_u64(&key, &kv.value)?;
            if epoch <= read_epoch {
                tombstones.insert((src, dst), epoch);
            }
        }
        Ok(tombstones)
    }

    pub(crate) async fn out_segment_edge_pairs_at(
        &self,
        cell_id: &str,
        edge_type: &str,
        read_epoch: GraphEpoch,
    ) -> Result<BTreeSet<(VertexId, VertexId)>> {
        validate_component("cell_id", cell_id)?;
        validate_component("edge_type", edge_type)?;
        let prefix = keys::out_segment_edge_type_prefix(cell_id, edge_type);
        let mut iter = self.scan_remote_prefix(&prefix).await?;
        let tombstones = self
            .out_segment_tombstones_at(cell_id, edge_type, read_epoch)
            .await?;
        let mut pairs = BTreeSet::new();
        while let Some(kv) = iter.next().await? {
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            let segment = decode_out_edge_segment(&key, &kv.value)?;
            if segment.start_epoch > read_epoch {
                continue;
            }
            for (epoch, dst) in segment.edges.iter().copied() {
                if epoch > read_epoch {
                    break;
                }
                let tombstone_epoch = tombstones.get(&(segment.src, dst)).copied();
                if segment_edge_visible(epoch, tombstone_epoch) {
                    pairs.insert((segment.src, dst));
                }
            }
        }
        Ok(pairs)
    }

    pub async fn in_neighbors(
        &self,
        cell_id: &str,
        edge_type: &str,
        dst: VertexId,
    ) -> Result<Vec<VertexId>> {
        validate_component("cell_id", cell_id)?;
        validate_component("edge_type", edge_type)?;
        if !self.writes_reverse_index() {
            let read_epoch = self.current_epoch(cell_id).await?;
            let mut neighbors: Vec<_> = self
                .edges_at(cell_id, edge_type, read_epoch)
                .await?
                .into_iter()
                .filter_map(|edge| (edge.dst == dst).then_some(edge.src))
                .collect();
            neighbors.sort_unstable();
            return Ok(neighbors);
        }
        let prefix = keys::in_prefix(cell_id, edge_type, dst);
        let mut iter = self.scan_remote_prefix(&prefix).await?;
        let mut neighbors = Vec::new();
        while let Some(kv) = iter.next().await? {
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            let record = decode_edge_record(&key, &kv.value)?;
            neighbors.push(record.src);
        }
        neighbors.sort_unstable();
        Ok(neighbors)
    }

    pub async fn out_degree(&self, cell_id: &str, edge_type: &str, src: VertexId) -> Result<u64> {
        validate_component("cell_id", cell_id)?;
        validate_component("edge_type", edge_type)?;
        self.read_counter(&keys::degree_out(cell_id, edge_type, src))
            .await
    }

    pub async fn outbox_since(
        &self,
        cell_id: &str,
        after_epoch: GraphEpoch,
    ) -> Result<Vec<DeltaRecord>> {
        validate_component("cell_id", cell_id)?;
        let prefix = keys::outbox_prefix(cell_id);
        let mut iter = self.scan_remote_prefix(&prefix).await?;
        let mut records = Vec::new();
        while let Some(kv) = iter.next().await? {
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            let record = decode_delta_record(&key, &kv.value)?;
            if record.edge.epoch > after_epoch {
                records.push(record);
            }
        }
        records.extend(
            self.scan_outbox_delta_batches_between(cell_id, None, after_epoch, GraphEpoch::MAX)
                .await?,
        );
        sort_deltas(&mut records);
        Ok(records)
    }

    pub async fn deltas_since(
        &self,
        cell_id: &str,
        edge_type: &str,
        after_epoch: GraphEpoch,
    ) -> Result<Vec<DeltaRecord>> {
        self.deltas_between(cell_id, edge_type, after_epoch, GraphEpoch::MAX)
            .await
    }

    pub async fn deltas_between(
        &self,
        cell_id: &str,
        edge_type: &str,
        after_epoch: GraphEpoch,
        read_epoch: GraphEpoch,
    ) -> Result<Vec<DeltaRecord>> {
        validate_component("cell_id", cell_id)?;
        validate_component("edge_type", edge_type)?;
        if after_epoch >= read_epoch {
            return Ok(Vec::new());
        }
        let watermark = self.delta_gc_watermark(cell_id, edge_type).await?;
        if after_epoch < watermark {
            return Err(GraphError::SnapshotExpired {
                cell_id: cell_id.to_string(),
                edge_type: edge_type.to_string(),
                read_epoch: after_epoch,
                min_epoch: watermark,
            });
        }

        let mut records = self
            .scan_outbox_deltas_between(cell_id, edge_type, after_epoch, read_epoch)
            .await?;
        records.extend(
            self.scan_outbox_delta_batches_between(
                cell_id,
                Some(edge_type),
                after_epoch,
                read_epoch,
            )
            .await?,
        );

        let final_watermark = self.delta_gc_watermark(cell_id, edge_type).await?;
        if after_epoch < final_watermark {
            return Err(GraphError::SnapshotExpired {
                cell_id: cell_id.to_string(),
                edge_type: edge_type.to_string(),
                read_epoch: after_epoch,
                min_epoch: final_watermark,
            });
        }

        sort_deltas(&mut records);
        Ok(records)
    }

    async fn scan_outbox_delta_batches_between(
        &self,
        cell_id: &str,
        edge_type: Option<&str>,
        after_epoch: GraphEpoch,
        read_epoch: GraphEpoch,
    ) -> Result<Vec<DeltaRecord>> {
        let start_suffix = after_epoch
            .checked_add(1)
            .map(|epoch| format!("{epoch:020}/"))
            .unwrap_or_else(|| format!("{:020}/", GraphEpoch::MAX));
        let mut iter = self
            .scan_remote_prefix_from(&keys::outbox_batch_prefix(cell_id), &start_suffix)
            .await?;
        let mut records = Vec::new();
        while let Some(kv) = iter.next().await? {
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            let batch = decode_outbox_delta_batch(&key, &kv.value)?;
            if batch.start_epoch > read_epoch {
                break;
            }
            if let Some(edge_type) = edge_type {
                if batch.edge_type != edge_type {
                    continue;
                }
            }
            for (offset, (src, dst)) in batch.edges.iter().copied().enumerate() {
                let epoch = batch.start_epoch + offset as u64;
                if epoch <= after_epoch {
                    continue;
                }
                if epoch > read_epoch {
                    break;
                }
                records.push(DeltaRecord {
                    kind: batch.kind,
                    edge: EdgeRecord {
                        cell_id: batch.cell_id.clone(),
                        edge_type: batch.edge_type.clone(),
                        src,
                        dst,
                        epoch,
                    },
                });
            }
        }
        sort_deltas(&mut records);
        Ok(records)
    }

    async fn scan_outbox_deltas_between(
        &self,
        cell_id: &str,
        edge_type: &str,
        after_epoch: GraphEpoch,
        read_epoch: GraphEpoch,
    ) -> Result<Vec<DeltaRecord>> {
        let start_suffix = after_epoch
            .checked_add(1)
            .map(|epoch| format!("{epoch:020}/"))
            .unwrap_or_else(|| format!("{:020}/", GraphEpoch::MAX));
        let mut iter = self
            .scan_remote_prefix_from(&keys::outbox_prefix(cell_id), &start_suffix)
            .await?;
        let mut records = Vec::new();
        while let Some(kv) = iter.next().await? {
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            let record = decode_delta_record(&key, &kv.value)?;
            if record.edge.epoch > read_epoch {
                break;
            }
            if record.edge.edge_type == edge_type && record.edge.epoch > after_epoch {
                records.push(record);
            }
        }
        sort_deltas(&mut records);
        Ok(records)
    }

    pub async fn current_epoch(&self, cell_id: &str) -> Result<GraphEpoch> {
        validate_component("cell_id", cell_id)?;
        self.read_counter(&keys::last_epoch(cell_id)).await
    }

    pub async fn edges_at(
        &self,
        cell_id: &str,
        edge_type: &str,
        read_epoch: GraphEpoch,
    ) -> Result<Vec<EdgeRecord>> {
        validate_component("cell_id", cell_id)?;
        validate_component("edge_type", edge_type)?;
        let mut edges = std::collections::BTreeMap::new();
        let base_epoch = if let Some(artifact) = self
            .latest_matrix_artifact(cell_id, edge_type, read_epoch)
            .await?
        {
            let adjacency = self
                .cached_matrix_adjacency(cell_id, edge_type, artifact.base_epoch)
                .await?;
            for (src, dsts) in adjacency.iter() {
                for dst in dsts {
                    edges.insert(
                        (*src, *dst),
                        EdgeRecord {
                            cell_id: cell_id.to_string(),
                            edge_type: edge_type.to_string(),
                            src: *src,
                            dst: *dst,
                            epoch: artifact.base_epoch,
                        },
                    );
                }
            }
            artifact.base_epoch
        } else {
            0
        };
        for delta in self
            .deltas_between(cell_id, edge_type, base_epoch, read_epoch)
            .await?
        {
            let key = (delta.edge.src, delta.edge.dst);
            match delta.kind {
                DeltaKind::Plus => {
                    edges.insert(key, delta.edge);
                }
                DeltaKind::Minus => {
                    edges.remove(&key);
                }
            }
        }
        Ok(edges.into_values().collect())
    }

    pub async fn validate_cell_edge_type(
        &self,
        cell_id: &str,
        edge_type: &str,
    ) -> Result<GraphRepairReport> {
        validate_component("cell_id", cell_id)?;
        validate_component("edge_type", edge_type)?;
        let read_epoch = self.current_epoch(cell_id).await?;
        let edges = self.edges_at(cell_id, edge_type, read_epoch).await?;
        let deltas = self
            .deltas_between(cell_id, edge_type, 0, read_epoch)
            .await?;
        let mut out_counts = BTreeMap::<VertexId, u64>::new();
        let mut in_counts = BTreeMap::<VertexId, u64>::new();
        for edge in &edges {
            *out_counts.entry(edge.src).or_default() += 1;
            *in_counts.entry(edge.dst).or_default() += 1;
        }
        let mut degree_mismatches = Vec::new();
        for (src, expected) in out_counts {
            let actual = self.out_degree(cell_id, edge_type, src).await?;
            if actual != expected {
                degree_mismatches.push(format!("out:{src}:expected={expected}:actual={actual}"));
            }
        }
        if self.writes_reverse_index() {
            for (dst, expected) in in_counts {
                let actual = self
                    .read_counter(&keys::degree_in(cell_id, edge_type, dst))
                    .await?;
                if actual != expected {
                    degree_mismatches.push(format!("in:{dst}:expected={expected}:actual={actual}"));
                }
            }
        } else {
            let mut iter = self
                .scan_remote_prefix(&keys::degree_in_prefix(cell_id, edge_type))
                .await?;
            while let Some(kv) = iter.next().await? {
                let key = String::from_utf8_lossy(&kv.key);
                degree_mismatches.push(format!("in:{key}:unexpected-under-outbound-only"));
            }
        }
        Ok(GraphRepairReport {
            cell_id: cell_id.to_string(),
            edge_type: edge_type.to_string(),
            read_epoch,
            live_edges: edges.len() as u64,
            delta_records: deltas.len() as u64,
            degree_mismatches,
        })
    }
}
