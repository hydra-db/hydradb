use super::*;

impl GraphShard {
    pub async fn execute_cypher(&self, context: QueryContext, query: &str) -> Result<QueryOutput> {
        self.execute_opencypher(context, query).await
    }

    pub async fn execute_cypher_rows(
        &self,
        context: QueryContext,
        query: &str,
    ) -> Result<QueryResultSet> {
        self.execute_opencypher_rows(context, query).await
    }

    pub async fn execute_opencypher(
        &self,
        context: QueryContext,
        query: &str,
    ) -> Result<QueryOutput> {
        #[cfg(feature = "opencypher")]
        {
            let plan = self.plan_opencypher(context, query)?;
            self.execute_query_plan(plan).await
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

    pub async fn execute_opencypher_rows(
        &self,
        context: QueryContext,
        query: &str,
    ) -> Result<QueryResultSet> {
        #[cfg(feature = "opencypher")]
        {
            let parsed = parse_opencypher_row_query(query)?;
            let context = merge_opencypher_window(context, parsed.window)?;
            self.execute_parsed_opencypher_rows(context, parsed).await
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
        let parsed = parse_opencypher_with_window(query)?;
        self.plan_parsed_opencypher(context, parsed)
    }

    #[cfg(feature = "opencypher")]
    fn plan_parsed_opencypher(
        &self,
        context: QueryContext,
        parsed: ParsedQuery,
    ) -> Result<QueryPlan> {
        let context = merge_opencypher_window(context, parsed.window)?;
        self.plan_query_statement(context, parsed.statement)
    }

    #[cfg(feature = "opencypher")]
    pub fn explain_cypher(&self, context: QueryContext, query: &str) -> Result<QueryPlan> {
        self.plan_opencypher(context, query)
    }

    #[cfg(feature = "opencypher")]
    async fn execute_parsed_opencypher_rows(
        &self,
        context: QueryContext,
        query: ParsedRowQuery,
    ) -> Result<QueryResultSet> {
        validate_component("cell_id", &context.cell_id)?;
        let read_epoch = match context.read_epoch {
            Some(read_epoch) => {
                let current_epoch = self.current_epoch(&context.cell_id).await?;
                if read_epoch > current_epoch {
                    return Err(GraphError::SnapshotAhead {
                        cell_id: context.cell_id.clone(),
                        read_epoch,
                        current_epoch,
                    });
                }
                read_epoch
            }
            None => self.current_epoch(&context.cell_id).await?,
        };

        let mut bindings = self
            .match_row_pattern(&context.cell_id, &query.pattern, read_epoch)
            .await?;
        if let Some(predicate) = &query.predicate {
            let mut filtered = Vec::with_capacity(bindings.len());
            for row in bindings {
                if row_predicate_matches(&row, predicate)? {
                    filtered.push(row);
                }
            }
            bindings = filtered;
        }

        if query.projections == [RowProjection::CountAll] {
            let row = QueryRow::new(vec![QueryValue::Count(bindings.len() as u64)]);
            let sort_keys = sort_keys_for_projected_only(&row, &query.columns, &query.order_by)?;
            return self.finish_projected_rows(
                query.columns,
                vec![ProjectedQueryRow { row, sort_keys }],
                &query.order_by,
                context.result_window,
            );
        }

        let mut projected = Vec::with_capacity(bindings.len());
        for binding in &bindings {
            let row = project_binding_row(binding, &query.projections)?;
            let sort_keys = sort_keys_for_row(binding, &row, &query.columns, &query.order_by)?;
            projected.push(ProjectedQueryRow { row, sort_keys });
        }
        self.finish_projected_rows(
            query.columns,
            projected,
            &query.order_by,
            context.result_window,
        )
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
            PhysicalQueryPlan::WriteEdgeWithMetadata {
                edge_type,
                src,
                dst,
                src_metadata,
                dst_metadata,
            } => {
                let result = self
                    .write_edge_with_vertex_metadata(
                        EdgeMutation {
                            cell_id: plan.cell_id,
                            edge_type,
                            src,
                            dst,
                            idempotency_key: plan.idempotency_key,
                        },
                        src_metadata,
                        dst_metadata,
                    )
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
                let read_epoch = match plan.read_epoch {
                    Some(read_epoch) => read_epoch,
                    None => self.current_epoch(&plan.cell_id).await?,
                };
                let vertices = self
                    .out_neighbors_window_at(
                        &plan.cell_id,
                        &edge_type,
                        src,
                        read_epoch,
                        plan.result_window,
                    )
                    .await?;
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
                    Ok(QueryOutput::Vertices(
                        self.apply_query_window(vec![dst], plan.result_window)?,
                    ))
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
                    Ok(QueryOutput::Vertices(
                        self.apply_query_window(vertices, plan.result_window)?,
                    ))
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

    #[cfg(feature = "opencypher")]
    async fn match_row_pattern(
        &self,
        cell_id: &str,
        pattern: &RowPattern,
        read_epoch: GraphEpoch,
    ) -> Result<Vec<BindingRow>> {
        validate_component("cell_id", cell_id)?;
        match pattern {
            RowPattern::Node(node) => self.match_node_row_pattern(cell_id, node, read_epoch).await,
            RowPattern::Edge(edge) => self.match_edge_row_pattern(cell_id, edge, read_epoch).await,
        }
    }

    #[cfg(feature = "opencypher")]
    async fn match_node_row_pattern(
        &self,
        cell_id: &str,
        node: &RowNodePattern,
        read_epoch: GraphEpoch,
    ) -> Result<Vec<BindingRow>> {
        let Some(vertices) = self.candidate_vertex_ids(cell_id, node, read_epoch).await? else {
            return Err(GraphError::UnsupportedQuery {
                dialect: "OpenCypher",
                feature: "node-only MATCH requires an id, label, or property predicate".to_string(),
            });
        };
        let mut rows = Vec::with_capacity(vertices.len());
        let mut metadata_cache = BTreeMap::new();
        for vertex_id in vertices {
            if let Some(mut row) = BindingRow::from_node(node, vertex_id) {
                self.hydrate_binding_metadata(cell_id, read_epoch, &mut row, &mut metadata_cache)
                    .await?;
                if row_matches_node(&row, node)? {
                    rows.push(row);
                }
            }
        }
        Ok(rows)
    }

    #[cfg(feature = "opencypher")]
    async fn match_edge_row_pattern(
        &self,
        cell_id: &str,
        edge: &RowEdgePattern,
        read_epoch: GraphEpoch,
    ) -> Result<Vec<BindingRow>> {
        validate_component("edge_type", &edge.edge_type)?;
        if let Some((min_hops, max_hops)) = edge.hop_range {
            return self
                .match_reachable_row_pattern(cell_id, edge, min_hops, max_hops, read_epoch)
                .await;
        }

        let mut rows = Vec::new();
        let mut metadata_cache = BTreeMap::new();
        let candidate_sources = self
            .candidate_vertex_ids(cell_id, &edge.src, read_epoch)
            .await?;
        if let Some(sources) = candidate_sources {
            for src in sources {
                for dst in self
                    .out_neighbors_at(cell_id, &edge.edge_type, src, read_epoch)
                    .await?
                {
                    if matches!(edge.dst.id, Some(fixed_dst) if fixed_dst != dst) {
                        continue;
                    }
                    if let Some(mut row) = BindingRow::from_edge(edge, src, dst) {
                        self.hydrate_binding_metadata(
                            cell_id,
                            read_epoch,
                            &mut row,
                            &mut metadata_cache,
                        )
                        .await?;
                        if row_matches_edge_pattern(&row, edge)? {
                            rows.push(row);
                        }
                    }
                }
            }
            return Ok(rows);
        }

        if let Some(src) = edge.src.id {
            for dst in self
                .out_neighbors_at(cell_id, &edge.edge_type, src, read_epoch)
                .await?
            {
                if matches!(edge.dst.id, Some(fixed_dst) if fixed_dst != dst) {
                    continue;
                }
                if let Some(mut row) = BindingRow::from_edge(edge, src, dst) {
                    self.hydrate_binding_metadata(
                        cell_id,
                        read_epoch,
                        &mut row,
                        &mut metadata_cache,
                    )
                    .await?;
                    if row_matches_edge_pattern(&row, edge)? {
                        rows.push(row);
                    }
                }
            }
            return Ok(rows);
        }

        for record in self.edges_at(cell_id, &edge.edge_type, read_epoch).await? {
            if matches!(edge.dst.id, Some(fixed_dst) if fixed_dst != record.dst) {
                continue;
            }
            if let Some(mut row) = BindingRow::from_edge(edge, record.src, record.dst) {
                self.hydrate_binding_metadata(cell_id, read_epoch, &mut row, &mut metadata_cache)
                    .await?;
                if row_matches_edge_pattern(&row, edge)? {
                    rows.push(row);
                }
            }
        }
        Ok(rows)
    }

    #[cfg(feature = "opencypher")]
    async fn match_reachable_row_pattern(
        &self,
        cell_id: &str,
        edge: &RowEdgePattern,
        min_hops: u8,
        max_hops: u8,
        read_epoch: GraphEpoch,
    ) -> Result<Vec<BindingRow>> {
        let Some(src) = edge.src.id else {
            return Err(GraphError::UnsupportedQuery {
                dialect: "OpenCypher",
                feature: "variable-length MATCH requires a fixed source id".to_string(),
            });
        };
        let vertices = self
            .reachable_vertices_in_hop_range_at(
                cell_id,
                &edge.edge_type,
                src,
                min_hops,
                max_hops,
                read_epoch,
            )
            .await?
            .0;
        let mut rows = Vec::with_capacity(vertices.len());
        let mut metadata_cache = BTreeMap::new();
        for dst in vertices {
            if matches!(edge.dst.id, Some(fixed_dst) if fixed_dst != dst) {
                continue;
            }
            if let Some(mut row) = BindingRow::from_edge(edge, src, dst) {
                self.hydrate_binding_metadata(cell_id, read_epoch, &mut row, &mut metadata_cache)
                    .await?;
                if row_matches_edge_pattern(&row, edge)? {
                    rows.push(row);
                }
            }
        }
        Ok(rows)
    }

    #[cfg(feature = "opencypher")]
    async fn candidate_vertex_ids(
        &self,
        cell_id: &str,
        pattern: &RowNodePattern,
        read_epoch: GraphEpoch,
    ) -> Result<Option<Vec<VertexId>>> {
        if let Some(id) = pattern.id {
            return Ok(Some(vec![id]));
        }
        if let Some((property, value)) = pattern
            .properties
            .iter()
            .find(|(property, _)| property.as_str() != "id")
        {
            return Ok(Some(
                self.scan_vertex_property_index_at(cell_id, property, value, read_epoch)
                    .await?,
            ));
        }
        if let Some(label) = pattern.labels.iter().next() {
            return Ok(Some(
                self.scan_vertex_label_index_at(cell_id, label, read_epoch)
                    .await?,
            ));
        }
        Ok(None)
    }

    #[cfg(feature = "opencypher")]
    async fn vertex_metadata_at(
        &self,
        cell_id: &str,
        vertex_id: VertexId,
        read_epoch: GraphEpoch,
    ) -> Result<VertexMetadata> {
        validate_component("cell_id", cell_id)?;
        let prefix = keys::vertex_delta_prefix(cell_id, vertex_id);
        let mut iter = self.scan_remote_prefix(&prefix).await?;
        let mut latest = None;
        let mut saw_delta = false;
        while let Some(kv) = iter.next().await? {
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            let epoch = parse_vertex_delta_key(&key)?;
            saw_delta = true;
            if epoch > read_epoch {
                break;
            }
            latest = Some(decode_vertex_metadata(&key, &kv.value)?);
        }
        if let Some(metadata) = latest {
            return Ok(metadata);
        }
        if saw_delta {
            return Ok(VertexMetadata::default());
        }
        let key = keys::vertex(cell_id, vertex_id);
        match self.read_remote(&key).await? {
            Some(value) => decode_vertex_metadata(&key, &value),
            None => Ok(VertexMetadata::default()),
        }
    }

    #[cfg(feature = "opencypher")]
    async fn scan_vertex_label_index_at(
        &self,
        cell_id: &str,
        label: &str,
        read_epoch: GraphEpoch,
    ) -> Result<Vec<VertexId>> {
        validate_component("cell_id", cell_id)?;
        validate_component("label", label)?;
        let mut iter = self
            .scan_remote_prefix(&keys::vertex_label_delta_prefix(cell_id, label))
            .await?;
        let mut latest = BTreeMap::<VertexId, bool>::new();
        let mut saw_delta = false;
        while let Some(kv) = iter.next().await? {
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            let (epoch, vertex_id) = parse_vertex_label_delta_key(&key)?;
            saw_delta = true;
            if epoch > read_epoch {
                break;
            }
            latest.insert(vertex_id, decode_vertex_index_delta(&key, &kv.value)?);
        }
        if saw_delta {
            return Ok(latest
                .into_iter()
                .filter_map(|(vertex_id, present)| present.then_some(vertex_id))
                .collect());
        }
        self.scan_vertex_label_index_current(cell_id, label).await
    }

    #[cfg(feature = "opencypher")]
    async fn scan_vertex_label_index_current(
        &self,
        cell_id: &str,
        label: &str,
    ) -> Result<Vec<VertexId>> {
        let mut iter = self
            .scan_remote_prefix(&keys::vertex_label_prefix(cell_id, label))
            .await?;
        let mut vertices = Vec::new();
        while let Some(kv) = iter.next().await? {
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            vertices.push(decode_u64(&key, &kv.value)?);
        }
        Ok(vertices)
    }

    #[cfg(feature = "opencypher")]
    async fn scan_vertex_property_index_at(
        &self,
        cell_id: &str,
        property: &str,
        value: &VertexPropertyValue,
        read_epoch: GraphEpoch,
    ) -> Result<Vec<VertexId>> {
        validate_component("cell_id", cell_id)?;
        validate_component("property", property)?;
        let encoded = encode_vertex_property_value_key(value);
        let mut iter = self
            .scan_remote_prefix(&keys::vertex_property_index_delta_prefix(
                cell_id, property, &encoded,
            ))
            .await?;
        let mut latest = BTreeMap::<VertexId, bool>::new();
        let mut saw_delta = false;
        while let Some(kv) = iter.next().await? {
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            let (epoch, vertex_id) = parse_vertex_property_index_delta_key(&key)?;
            saw_delta = true;
            if epoch > read_epoch {
                break;
            }
            latest.insert(vertex_id, decode_vertex_index_delta(&key, &kv.value)?);
        }
        if saw_delta {
            return Ok(latest
                .into_iter()
                .filter_map(|(vertex_id, present)| present.then_some(vertex_id))
                .collect());
        }
        self.scan_vertex_property_index_current(cell_id, property, &encoded)
            .await
    }

    #[cfg(feature = "opencypher")]
    async fn scan_vertex_property_index_current(
        &self,
        cell_id: &str,
        property: &str,
        encoded: &str,
    ) -> Result<Vec<VertexId>> {
        let mut iter = self
            .scan_remote_prefix(&keys::vertex_property_index_prefix(
                cell_id, property, encoded,
            ))
            .await?;
        let mut vertices = Vec::new();
        while let Some(kv) = iter.next().await? {
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            vertices.push(decode_u64(&key, &kv.value)?);
        }
        Ok(vertices)
    }

    #[cfg(feature = "opencypher")]
    async fn hydrate_binding_metadata(
        &self,
        cell_id: &str,
        read_epoch: GraphEpoch,
        row: &mut BindingRow,
        cache: &mut BTreeMap<VertexId, VertexMetadata>,
    ) -> Result<()> {
        let bindings: Vec<_> = row
            .values
            .iter()
            .map(|(name, id)| (name.clone(), *id))
            .collect();
        for (binding, vertex_id) in bindings {
            let metadata = match cache.get(&vertex_id) {
                Some(metadata) => metadata.clone(),
                None => {
                    let metadata = self
                        .vertex_metadata_at(cell_id, vertex_id, read_epoch)
                        .await?;
                    cache.insert(vertex_id, metadata.clone());
                    metadata
                }
            };
            row.metadata.insert(binding, metadata);
        }
        Ok(())
    }

    #[cfg(feature = "opencypher")]
    fn finish_projected_rows(
        &self,
        columns: Vec<QueryColumn>,
        mut projected: Vec<ProjectedQueryRow>,
        order_by: &[RowSort],
        window: QueryWindow,
    ) -> Result<QueryResultSet> {
        if !order_by.is_empty() {
            projected.sort_by(|left, right| compare_projected_rows(left, right, order_by));
        }

        let skip = usize::try_from(window.skip).map_err(|_| GraphError::AdmissionRejected {
            operation: "query_result_skip",
            actual: window.skip,
            limit: usize::MAX as u64,
        })?;
        let max = self.limits.max_query_result_vertices;
        let mut rows: Vec<_> = projected
            .into_iter()
            .skip(skip)
            .map(|projected| projected.row)
            .collect();
        if let Some(limit) = window.limit {
            ensure_limit("query_result_limit", limit as u64, max as u64)?;
            rows.truncate(limit);
        } else {
            ensure_limit("query_result_rows", rows.len() as u64, max as u64)?;
        }
        Ok(QueryResultSet::new(columns, rows))
    }

    async fn out_neighbors_window_at(
        &self,
        cell_id: &str,
        edge_type: &str,
        src: VertexId,
        read_epoch: GraphEpoch,
        window: QueryWindow,
    ) -> Result<Vec<VertexId>> {
        let fetch_limit = self.query_window_fetch_limit(window)?;
        if let Some(vertices) = self
            .out_supernode_window(cell_id, edge_type, src, read_epoch, window, fetch_limit)
            .await?
        {
            return self.apply_query_window_fetch_result(vertices, window);
        }

        let vertices = self
            .out_neighbors_at(cell_id, edge_type, src, read_epoch)
            .await?;
        self.apply_query_window(vertices, window)
    }

    fn query_window_fetch_limit(&self, window: QueryWindow) -> Result<usize> {
        let max = self.limits.max_query_result_vertices;
        if let Some(limit) = window.limit {
            ensure_limit("query_result_limit", limit as u64, max as u64)?;
            Ok(limit)
        } else {
            Ok(max.saturating_add(1))
        }
    }

    fn apply_query_window(
        &self,
        vertices: Vec<VertexId>,
        window: QueryWindow,
    ) -> Result<Vec<VertexId>> {
        let skip = usize::try_from(window.skip).map_err(|_| GraphError::AdmissionRejected {
            operation: "query_result_skip",
            actual: window.skip,
            limit: usize::MAX as u64,
        })?;
        let windowed: Vec<_> = vertices.into_iter().skip(skip).collect();
        self.apply_query_window_fetch_result(windowed, window)
    }

    fn apply_query_window_fetch_result(
        &self,
        mut vertices: Vec<VertexId>,
        window: QueryWindow,
    ) -> Result<Vec<VertexId>> {
        let max = self.limits.max_query_result_vertices;
        if let Some(limit) = window.limit {
            ensure_limit("query_result_limit", limit as u64, max as u64)?;
            vertices.truncate(limit);
        } else {
            ensure_limit("query_result_vertices", vertices.len() as u64, max as u64)?;
        }
        Ok(vertices)
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

#[cfg(feature = "opencypher")]
fn merge_opencypher_window(context: QueryContext, window: QueryWindow) -> Result<QueryContext> {
    if window.is_default() {
        return Ok(context);
    }
    if !context.result_window.is_default() && context.result_window != window {
        return Err(GraphError::UnsupportedQuery {
            dialect: "OpenCypher",
            feature: "query SKIP/LIMIT conflicts with QueryContext result window".to_string(),
        });
    }
    Ok(context.with_result_window(window.skip, window.limit))
}

#[cfg(feature = "opencypher")]
fn parse_vertex_delta_key(key: &str) -> Result<GraphEpoch> {
    let parts: Vec<_> = key.split('/').collect();
    match parts.as_slice() {
        ["cell", _cell_id, "vertex_delta", _vertex_id, epoch] => {
            parse_u64(key, epoch, "vertex_metadata_epoch")
        }
        _ => Err(GraphError::CorruptValue {
            key: key.to_string(),
            reason: "expected vertex metadata delta key".to_string(),
        }),
    }
}

#[cfg(feature = "opencypher")]
fn parse_vertex_label_delta_key(key: &str) -> Result<(GraphEpoch, VertexId)> {
    let parts: Vec<_> = key.split('/').collect();
    match parts.as_slice() {
        ["cell", _cell_id, "vlabel_delta", _label, epoch, vertex_id] => Ok((
            parse_u64(key, epoch, "vertex_label_epoch")?,
            parse_u64(key, vertex_id, "vertex_id")?,
        )),
        _ => Err(GraphError::CorruptValue {
            key: key.to_string(),
            reason: "expected vertex label delta key".to_string(),
        }),
    }
}

#[cfg(feature = "opencypher")]
fn parse_vertex_property_index_delta_key(key: &str) -> Result<(GraphEpoch, VertexId)> {
    let parts: Vec<_> = key.split('/').collect();
    match parts.as_slice() {
        ["cell", _cell_id, "vprop_delta", _property, _value, epoch, vertex_id] => Ok((
            parse_u64(key, epoch, "vertex_property_epoch")?,
            parse_u64(key, vertex_id, "vertex_id")?,
        )),
        _ => Err(GraphError::CorruptValue {
            key: key.to_string(),
            reason: "expected vertex property delta key".to_string(),
        }),
    }
}

#[cfg(feature = "opencypher")]
#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct BindingRow {
    values: BTreeMap<String, VertexId>,
    metadata: BTreeMap<String, VertexMetadata>,
}

#[cfg(feature = "opencypher")]
impl BindingRow {
    fn from_node(pattern: &RowNodePattern, vertex_id: VertexId) -> Option<Self> {
        let mut row = Self::default();
        if !row.bind(pattern.binding.as_deref(), vertex_id) {
            return None;
        }
        Some(row)
    }

    fn from_edge(pattern: &RowEdgePattern, src: VertexId, dst: VertexId) -> Option<Self> {
        let mut row = Self::default();
        if !row.bind(pattern.src.binding.as_deref(), src) {
            return None;
        }
        if !row.bind(pattern.dst.binding.as_deref(), dst) {
            return None;
        }
        Some(row)
    }

    fn bind(&mut self, binding: Option<&str>, value: VertexId) -> bool {
        let Some(binding) = binding else {
            return true;
        };
        match self.values.get(binding) {
            Some(existing) => *existing == value,
            None => {
                self.values.insert(binding.to_string(), value);
                true
            }
        }
    }

    fn get(&self, binding: &str) -> Result<VertexId> {
        self.values
            .get(binding)
            .copied()
            .ok_or_else(|| GraphError::UnsupportedQuery {
                dialect: "OpenCypher",
                feature: format!("unbound variable {binding}"),
            })
    }
}

#[cfg(feature = "opencypher")]
#[derive(Clone, Debug, Eq, PartialEq)]
struct ProjectedQueryRow {
    row: QueryRow,
    sort_keys: Vec<QueryValue>,
}

#[cfg(feature = "opencypher")]
#[derive(Clone, Debug, Eq, PartialEq)]
enum RowScalarValue {
    Value(VertexPropertyValue),
    Missing,
}

#[cfg(feature = "opencypher")]
fn row_matches_edge_pattern(row: &BindingRow, pattern: &RowEdgePattern) -> Result<bool> {
    Ok(row_matches_node(row, &pattern.src)? && row_matches_node(row, &pattern.dst)?)
}

#[cfg(feature = "opencypher")]
fn row_matches_node(row: &BindingRow, node: &RowNodePattern) -> Result<bool> {
    let Some(binding) = &node.binding else {
        return Ok(true);
    };
    if matches!(node.id, Some(id) if row.get(binding)? != id) {
        return Ok(false);
    }
    if !node_has_metadata_constraints(node) {
        return Ok(true);
    }
    let Some(metadata) = row.metadata.get(binding) else {
        return Err(GraphError::UnsupportedQuery {
            dialect: "OpenCypher",
            feature: format!("metadata for bound variable {binding} was not hydrated"),
        });
    };
    Ok(vertex_metadata_matches(metadata, node))
}

#[cfg(feature = "opencypher")]
fn node_has_metadata_constraints(node: &RowNodePattern) -> bool {
    !node.labels.is_empty() || node.properties.keys().any(|property| property != "id")
}

#[cfg(feature = "opencypher")]
fn vertex_metadata_matches(metadata: &VertexMetadata, node: &RowNodePattern) -> bool {
    node.labels
        .iter()
        .all(|label| metadata.labels.contains(label))
        && node
            .properties
            .iter()
            .filter(|(property, _)| property.as_str() != "id")
            .all(|(property, value)| metadata.properties.get(property) == Some(value))
}

#[cfg(feature = "opencypher")]
fn row_predicate_matches(row: &BindingRow, predicate: &RowPredicate) -> Result<bool> {
    Ok(match predicate {
        RowPredicate::Compare { left, op, right } => compare_row_values(
            eval_row_expression(row, left)?,
            *op,
            eval_row_expression(row, right)?,
        )?,
        RowPredicate::And(left, right) => {
            row_predicate_matches(row, left)? && row_predicate_matches(row, right)?
        }
        RowPredicate::Or(left, right) => {
            row_predicate_matches(row, left)? || row_predicate_matches(row, right)?
        }
        RowPredicate::Not(inner) => !row_predicate_matches(row, inner)?,
    })
}

#[cfg(feature = "opencypher")]
fn eval_row_expression(row: &BindingRow, expression: &RowExpression) -> Result<RowScalarValue> {
    match expression {
        RowExpression::NodeId { binding } => Ok(RowScalarValue::Value(
            VertexPropertyValue::Integer(row.get(binding)?),
        )),
        RowExpression::Property { binding, property } => {
            Ok(match binding_property(row, binding, property)? {
                Some(value) => RowScalarValue::Value(value),
                None => RowScalarValue::Missing,
            })
        }
        RowExpression::Literal(value) => Ok(RowScalarValue::Value(value.clone())),
    }
}

#[cfg(feature = "opencypher")]
fn compare_row_values(
    left: RowScalarValue,
    op: RowComparisonOp,
    right: RowScalarValue,
) -> Result<bool> {
    let (RowScalarValue::Value(left), RowScalarValue::Value(right)) = (left, right) else {
        return Ok(false);
    };
    compare_vertex_property_values(&left, op, &right)
}

#[cfg(feature = "opencypher")]
fn compare_vertex_property_values(
    left: &VertexPropertyValue,
    op: RowComparisonOp,
    right: &VertexPropertyValue,
) -> Result<bool> {
    Ok(match op {
        RowComparisonOp::Eq => left == right,
        RowComparisonOp::Ne => left != right,
        RowComparisonOp::Lt | RowComparisonOp::Gt | RowComparisonOp::Lte | RowComparisonOp::Gte => {
            match (left, right) {
                (VertexPropertyValue::Integer(left), VertexPropertyValue::Integer(right)) => {
                    compare_ordering(left.cmp(right), op)
                }
                (VertexPropertyValue::String(left), VertexPropertyValue::String(right)) => {
                    compare_ordering(left.cmp(right), op)
                }
                _ => {
                    return Err(GraphError::UnsupportedQuery {
                        dialect: "OpenCypher",
                        feature: "ordered comparisons require matching integer or string values"
                            .to_string(),
                    });
                }
            }
        }
    })
}

#[cfg(feature = "opencypher")]
fn compare_ordering(ordering: std::cmp::Ordering, op: RowComparisonOp) -> bool {
    match op {
        RowComparisonOp::Eq => ordering == std::cmp::Ordering::Equal,
        RowComparisonOp::Ne => ordering != std::cmp::Ordering::Equal,
        RowComparisonOp::Lt => ordering == std::cmp::Ordering::Less,
        RowComparisonOp::Gt => ordering == std::cmp::Ordering::Greater,
        RowComparisonOp::Lte => ordering != std::cmp::Ordering::Greater,
        RowComparisonOp::Gte => ordering != std::cmp::Ordering::Less,
    }
}

#[cfg(feature = "opencypher")]
fn binding_property(
    row: &BindingRow,
    binding: &str,
    property: &str,
) -> Result<Option<VertexPropertyValue>> {
    if property == "id" {
        return Ok(Some(VertexPropertyValue::Integer(row.get(binding)?)));
    }
    let Some(metadata) = row.metadata.get(binding) else {
        return Err(GraphError::UnsupportedQuery {
            dialect: "OpenCypher",
            feature: format!("metadata for bound variable {binding} was not hydrated"),
        });
    };
    Ok(metadata.properties.get(property).cloned())
}

#[cfg(feature = "opencypher")]
fn project_binding_row(row: &BindingRow, projections: &[RowProjection]) -> Result<QueryRow> {
    let mut values = Vec::with_capacity(projections.len());
    for projection in projections {
        match projection {
            RowProjection::NodeId { binding } => {
                values.push(QueryValue::VertexId(row.get(binding)?));
            }
            RowProjection::Property { binding, property } => {
                let value = binding_property(row, binding, property)?.ok_or_else(|| {
                    GraphError::UnsupportedQuery {
                        dialect: "OpenCypher",
                        feature: format!("property {binding}.{property} is missing"),
                    }
                })?;
                values.push(QueryValue::Property(value));
            }
            RowProjection::CountAll => {
                return Err(GraphError::UnsupportedQuery {
                    dialect: "OpenCypher",
                    feature: "count(*) projection must be planned as an aggregate".to_string(),
                });
            }
        }
    }
    Ok(QueryRow::new(values))
}

#[cfg(feature = "opencypher")]
fn sort_keys_for_row(
    binding_row: &BindingRow,
    row: &QueryRow,
    columns: &[QueryColumn],
    order_by: &[RowSort],
) -> Result<Vec<QueryValue>> {
    let mut keys = Vec::with_capacity(order_by.len());
    for sort in order_by {
        keys.push(match &sort.expression {
            RowSortExpression::NodeId { binding } => {
                QueryValue::VertexId(binding_row.get(binding)?)
            }
            RowSortExpression::Property { binding, property } => {
                let value = binding_property(binding_row, binding, property)?.ok_or_else(|| {
                    GraphError::UnsupportedQuery {
                        dialect: "OpenCypher",
                        feature: format!("ORDER BY property {binding}.{property} is missing"),
                    }
                })?;
                QueryValue::Property(value)
            }
            RowSortExpression::Column { name } => projected_column_value(row, columns, name)?,
            RowSortExpression::CountAll => {
                return Err(GraphError::UnsupportedQuery {
                    dialect: "OpenCypher",
                    feature: "count(*) ORDER BY is only valid for aggregate rows".to_string(),
                });
            }
        });
    }
    Ok(keys)
}

#[cfg(feature = "opencypher")]
fn sort_keys_for_projected_only(
    row: &QueryRow,
    columns: &[QueryColumn],
    order_by: &[RowSort],
) -> Result<Vec<QueryValue>> {
    let mut keys = Vec::with_capacity(order_by.len());
    for sort in order_by {
        keys.push(match &sort.expression {
            RowSortExpression::Column { name } => projected_column_value(row, columns, name)?,
            RowSortExpression::CountAll => {
                let Some(value) = row.values.first() else {
                    return Err(GraphError::UnsupportedQuery {
                        dialect: "OpenCypher",
                        feature: "count(*) ORDER BY requires an aggregate row".to_string(),
                    });
                };
                value.clone()
            }
            RowSortExpression::NodeId { .. } => {
                return Err(GraphError::UnsupportedQuery {
                    dialect: "OpenCypher",
                    feature: "aggregate ORDER BY cannot reference row variables".to_string(),
                });
            }
            RowSortExpression::Property { .. } => {
                return Err(GraphError::UnsupportedQuery {
                    dialect: "OpenCypher",
                    feature: "aggregate ORDER BY cannot reference row properties".to_string(),
                });
            }
        });
    }
    Ok(keys)
}

#[cfg(feature = "opencypher")]
fn projected_column_value(
    row: &QueryRow,
    columns: &[QueryColumn],
    name: &str,
) -> Result<QueryValue> {
    let Some(index) = columns.iter().position(|column| column.name == name) else {
        return Err(GraphError::UnsupportedQuery {
            dialect: "OpenCypher",
            feature: format!("ORDER BY references unknown projection {name}"),
        });
    };
    row.values
        .get(index)
        .cloned()
        .ok_or_else(|| GraphError::UnsupportedQuery {
            dialect: "OpenCypher",
            feature: format!("ORDER BY projection {name} has no row value"),
        })
}

#[cfg(feature = "opencypher")]
fn compare_projected_rows(
    left: &ProjectedQueryRow,
    right: &ProjectedQueryRow,
    order_by: &[RowSort],
) -> std::cmp::Ordering {
    for (idx, sort) in order_by.iter().enumerate() {
        let ordering = compare_query_values(&left.sort_keys[idx], &right.sort_keys[idx]);
        let ordering = if sort.ascending {
            ordering
        } else {
            ordering.reverse()
        };
        if ordering != std::cmp::Ordering::Equal {
            return ordering;
        }
    }
    std::cmp::Ordering::Equal
}

#[cfg(feature = "opencypher")]
fn compare_query_values(left: &QueryValue, right: &QueryValue) -> std::cmp::Ordering {
    match (left, right) {
        (QueryValue::VertexId(left), QueryValue::VertexId(right))
        | (QueryValue::VertexId(left), QueryValue::Count(right))
        | (QueryValue::Count(left), QueryValue::VertexId(right))
        | (QueryValue::Count(left), QueryValue::Count(right)) => left.cmp(right),
        (QueryValue::Property(left), QueryValue::Property(right)) => {
            compare_vertex_property_order(left, right)
        }
        (QueryValue::Bool(left), QueryValue::Bool(right)) => left.cmp(right),
        _ => query_value_rank(left).cmp(&query_value_rank(right)),
    }
}

#[cfg(feature = "opencypher")]
fn compare_vertex_property_order(
    left: &VertexPropertyValue,
    right: &VertexPropertyValue,
) -> std::cmp::Ordering {
    match (left, right) {
        (VertexPropertyValue::Integer(left), VertexPropertyValue::Integer(right)) => {
            left.cmp(right)
        }
        (VertexPropertyValue::Bool(left), VertexPropertyValue::Bool(right)) => left.cmp(right),
        (VertexPropertyValue::String(left), VertexPropertyValue::String(right)) => left.cmp(right),
        _ => vertex_property_rank(left).cmp(&vertex_property_rank(right)),
    }
}

#[cfg(feature = "opencypher")]
fn query_value_rank(value: &QueryValue) -> u8 {
    match value {
        QueryValue::Bool(_) => 0,
        QueryValue::VertexId(_) | QueryValue::Count(_) => 1,
        QueryValue::Property(value) => 2 + vertex_property_rank(value),
    }
}

#[cfg(feature = "opencypher")]
fn vertex_property_rank(value: &VertexPropertyValue) -> u8 {
    match value {
        VertexPropertyValue::Bool(_) => 0,
        VertexPropertyValue::Integer(_) => 1,
        VertexPropertyValue::String(_) => 2,
    }
}
