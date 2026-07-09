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

    pub async fn execute_cypher_rows_page(
        &self,
        context: QueryContext,
        query: &str,
        cursor: Option<QueryCursorToken>,
        page_size: usize,
    ) -> Result<QueryResultPage> {
        self.execute_opencypher_rows_page(context, query, cursor, page_size)
            .await
    }

    pub async fn execute_opencypher(
        &self,
        context: QueryContext,
        query: &str,
    ) -> Result<QueryOutput> {
        #[cfg(feature = "opencypher")]
        {
            if let Some(parsed) =
                parse_opencypher_mutation_query_with_parameters(query, &context.parameters)?
            {
                let result = self
                    .execute_parsed_opencypher_mutation(context, parsed)
                    .await?;
                return Ok(QueryOutput::Mutation(result));
            }
            let parsed = match self
                .parsed_opencypher_row_query(&context.cell_id, query, &context.parameters)
                .await
            {
                Ok(parsed) => parsed,
                Err(err) if is_non_row_opencypher_query(&err) => {
                    let plan = self.plan_opencypher(context, query)?;
                    return self.execute_query_plan(plan).await;
                }
                Err(err) => return Err(err),
            };
            let context = merge_opencypher_window(context, parsed.window)?;
            let result_set = self
                .execute_parsed_opencypher_rows(context, parsed.clone())
                .await?;
            Ok(query_result_set_to_output(&parsed, result_set))
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
            let parsed = self
                .parsed_opencypher_row_query(&context.cell_id, query, &context.parameters)
                .await?;
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

    pub async fn execute_opencypher_rows_page(
        &self,
        context: QueryContext,
        query: &str,
        cursor: Option<QueryCursorToken>,
        page_size: usize,
    ) -> Result<QueryResultPage> {
        #[cfg(feature = "opencypher")]
        {
            let parsed = self
                .parsed_opencypher_row_query(&context.cell_id, query, &context.parameters)
                .await?;
            let context = merge_opencypher_window(context, parsed.window)?;
            let cursor_offset = cursor.map_or(0, |cursor| cursor.offset);

            let started = std::time::Instant::now();
            match self
                .try_execute_graph_kernel_opencypher_rows_page(
                    &context,
                    &parsed,
                    cursor_offset,
                    page_size,
                )
                .await
            {
                Ok(Some(page)) => {
                    self.record_streaming_query_rows_success(page.rows.len(), started);
                    return Ok(page);
                }
                Ok(None) => {}
                Err(err) => {
                    self.record_streaming_query_rows_failure(started);
                    return Err(err);
                }
            }

            match self
                .try_execute_streaming_opencypher_rows_page(
                    &context,
                    &parsed,
                    cursor_offset,
                    page_size,
                )
                .await
            {
                Ok(Some(page)) => {
                    self.record_streaming_query_rows_success(page.rows.len(), started);
                    return Ok(page);
                }
                Ok(None) => {}
                Err(err) => {
                    self.record_streaming_query_rows_failure(started);
                    return Err(err);
                }
            }

            let mut parsed = parsed;
            let context = self.query_page_context(context, cursor_offset, page_size)?;
            parsed.window = QueryWindow::default();
            let mut result_set = self.execute_parsed_opencypher_rows(context, parsed).await?;
            let next_cursor = if result_set.rows.len() > page_size {
                result_set.rows.truncate(page_size);
                Some(QueryCursorToken::new(
                    cursor_offset.checked_add(page_size as u64).ok_or_else(|| {
                        GraphError::AdmissionRejected {
                            operation: "query_cursor_offset",
                            actual: u64::MAX,
                            limit: u64::MAX - 1,
                        }
                    })?,
                ))
            } else {
                None
            };
            Ok(QueryResultPage::new(
                result_set.columns,
                result_set.rows,
                next_cursor,
            ))
        }
        #[cfg(not(feature = "opencypher"))]
        {
            let _ = (context, query, cursor, page_size);
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
        let parsed = parse_opencypher_with_parameters(query, &context.parameters)?;
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
    pub async fn explain_opencypher_rows(
        &self,
        context: QueryContext,
        query: &str,
    ) -> Result<RowQueryPlan> {
        validate_component("cell_id", &context.cell_id)?;
        let parsed = self
            .parsed_opencypher_row_query(&context.cell_id, query, &context.parameters)
            .await?;
        let context = merge_opencypher_window(context, parsed.window)?;
        let read_epoch = self.query_read_epoch(&context).await?;
        self.explain_row_query_plan_with_stats(&context.cell_id, read_epoch, &parsed)
            .await
    }

    #[cfg(feature = "opencypher")]
    async fn parsed_opencypher_row_query(
        &self,
        cell_id: &str,
        query: &str,
        parameters: &BTreeMap<String, VertexPropertyValue>,
    ) -> Result<ParsedRowQuery> {
        if !parameters.is_empty() {
            return parse_opencypher_row_query_with_parameters(query, parameters);
        }
        let key = ParsedRowQueryCacheKey::new(query);
        if let Some(parsed) = self.parsed_row_query_cache.lock().await.get(&key) {
            self.cache_metrics
                .record_hit(GraphCacheKind::ParsedRowQuery);
            return Ok(parsed);
        }

        self.cache_metrics
            .record_miss(GraphCacheKind::ParsedRowQuery);
        let parsed = parse_opencypher_row_query_with_parameters(query, parameters)?;
        self.parsed_row_query_cache.lock().await.insert(
            key,
            parsed.clone(),
            cell_id.to_string(),
            false,
            &self.cache_metrics,
        );
        Ok(parsed)
    }

    #[cfg(feature = "opencypher")]
    async fn execute_parsed_opencypher_rows(
        &self,
        context: QueryContext,
        query: ParsedRowQuery,
    ) -> Result<QueryResultSet> {
        self.operation_metrics
            .query_rows_started
            .fetch_add(1, Ordering::Relaxed);
        let started = std::time::Instant::now();
        let result = self
            .execute_parsed_opencypher_rows_inner(context, query)
            .await;
        let elapsed_us = started.elapsed().as_micros().try_into().unwrap_or(u64::MAX);
        self.operation_metrics
            .query_rows_duration_us
            .fetch_add(elapsed_us, Ordering::Relaxed);
        match &result {
            Ok(result_set) => {
                self.operation_metrics
                    .query_rows_completed
                    .fetch_add(1, Ordering::Relaxed);
                self.operation_metrics
                    .query_rows_returned
                    .fetch_add(result_set.rows.len() as u64, Ordering::Relaxed);
            }
            Err(_) => {
                self.operation_metrics
                    .query_rows_failed
                    .fetch_add(1, Ordering::Relaxed);
            }
        }
        result
    }

    #[cfg(feature = "opencypher")]
    async fn execute_parsed_opencypher_rows_inner(
        &self,
        context: QueryContext,
        query: ParsedRowQuery,
    ) -> Result<QueryResultSet> {
        validate_component("cell_id", &context.cell_id)?;
        let budget = QueryBudget::new(
            context.max_runtime_ms.or(self.limits.max_query_runtime_ms),
            context.cancellation_token.clone(),
        );
        budget.check("cypher_rows")?;
        let read_epoch = self.query_read_epoch(&context).await?;

        if !query.union_arms.is_empty() {
            return self
                .execute_union_opencypher_rows(
                    &context.cell_id,
                    read_epoch,
                    query,
                    context.result_window,
                    &budget,
                )
                .await;
        }
        self.execute_single_opencypher_rows(
            &context.cell_id,
            read_epoch,
            query,
            context.result_window,
            &budget,
        )
        .await
    }

    #[cfg(feature = "opencypher")]
    async fn execute_union_opencypher_rows(
        &self,
        cell_id: &str,
        read_epoch: GraphEpoch,
        mut query: ParsedRowQuery,
        window: QueryWindow,
        budget: &QueryBudget,
    ) -> Result<QueryResultSet> {
        budget.check("cypher_union")?;
        let union_all = query.union_all;
        let mut arms = std::mem::take(&mut query.union_arms);
        query.union_all = false;
        let columns = query.columns.clone();

        let mut rows = self
            .execute_single_opencypher_rows(
                cell_id,
                read_epoch,
                query,
                QueryWindow::default(),
                budget,
            )
            .await?
            .rows;
        for arm in arms.drain(..) {
            budget.check("cypher_union_arm")?;
            if arm.columns != columns {
                return Err(GraphError::UnsupportedQuery {
                    dialect: "OpenCypher",
                    feature: "UNION arms must project the same column names".to_string(),
                });
            }
            rows.extend(
                self.execute_single_opencypher_rows(
                    cell_id,
                    read_epoch,
                    arm,
                    QueryWindow::default(),
                    budget,
                )
                .await?
                .rows,
            );
        }

        if !union_all {
            let mut seen = BTreeSet::new();
            rows.retain(|row| seen.insert(row.values.clone()));
        }

        let projected = rows
            .into_iter()
            .map(|row| ProjectedQueryRow {
                row,
                sort_keys: Vec::new(),
            })
            .collect();
        self.finish_projected_rows(columns, projected, &[], window, budget)
    }

    #[cfg(feature = "opencypher")]
    async fn execute_single_opencypher_rows(
        &self,
        cell_id: &str,
        read_epoch: GraphEpoch,
        query: ParsedRowQuery,
        window: QueryWindow,
        budget: &QueryBudget,
    ) -> Result<QueryResultSet> {
        if let Some(result) = self
            .try_execute_graph_kernel_row_query(cell_id, read_epoch, &query, window, budget)
            .await?
        {
            return Ok(result);
        }
        if let Some(result) = self
            .try_execute_source_relationship_id_rows_query(
                cell_id, read_epoch, &query, window, budget,
            )
            .await?
        {
            return Ok(result);
        }
        if let Some(result) = self
            .try_execute_relationship_count_query(cell_id, read_epoch, &query, window, budget)
            .await?
        {
            return Ok(result);
        }
        if let Some(result) = self
            .try_execute_relationship_rows_query(cell_id, read_epoch, &query, window, budget)
            .await?
        {
            return Ok(result);
        }

        let bindings = if query.pattern_groups.is_empty() {
            let mut bindings = self
                .match_row_patterns(cell_id, &query.patterns, read_epoch, budget)
                .await?;
            if let Some(predicate) = &query.predicate {
                let mut filtered = Vec::with_capacity(bindings.len());
                for row in bindings {
                    budget.check("cypher_where")?;
                    if row_predicate_matches(&row, predicate)? {
                        filtered.push(row);
                    }
                }
                bindings = filtered;
            }
            bindings
        } else {
            self.match_row_pattern_groups(cell_id, &query.pattern_groups, read_epoch, budget)
                .await?
        };

        if row_projections_have_aggregates(&query.projections) {
            let projected = aggregate_projected_rows(
                bindings,
                &query.projections,
                &query.columns,
                &query.order_by,
                budget,
            )?;
            return self.finish_projected_rows(
                query.columns,
                projected,
                &query.order_by,
                window,
                budget,
            );
        }

        let mut projected = Vec::with_capacity(bindings.len());
        for binding in &bindings {
            budget.check("cypher_project")?;
            let row = project_binding_row(binding, &query.projections)?;
            let sort_keys = sort_keys_for_row(binding, &row, &query.columns, &query.order_by)?;
            projected.push(ProjectedQueryRow { row, sort_keys });
        }
        self.finish_projected_rows(query.columns, projected, &query.order_by, window, budget)
    }

    #[cfg(feature = "opencypher")]
    async fn try_execute_graph_kernel_row_query(
        &self,
        cell_id: &str,
        read_epoch: GraphEpoch,
        query: &ParsedRowQuery,
        window: QueryWindow,
        budget: &QueryBudget,
    ) -> Result<Option<QueryResultSet>> {
        let Some(request) = graph_kernel_row_query_request(query) else {
            return Ok(None);
        };
        if request.projection == GraphKernelProjection::CountAll && request.edge.dst.id.is_none() {
            let (count, edge_visits) = self
                .reachable_count_in_hop_range_at(
                    cell_id,
                    &request.edge.edge_type,
                    request.src,
                    request.hop_range,
                    read_epoch,
                    budget,
                )
                .await?;
            self.ensure_query_scan_edges("cypher_graph_kernel_count_edge_visits", edge_visits)?;
            let rows = vec![QueryRow::new(vec![QueryValue::Count(count)])];
            return Ok(Some(QueryResultSet::new(
                query.columns.clone(),
                self.apply_query_row_window(rows, window)?,
            )));
        }
        if request.projection == GraphKernelProjection::NodeId
            && request.edge.dst.id.is_none()
            && !window.is_default()
        {
            let (vertices, edge_visits) = self
                .reachable_vertices_window_in_hop_range_at(ReachableWindowRequest {
                    cell_id,
                    edge_type: &request.edge.edge_type,
                    src: request.src,
                    hop_range: request.hop_range,
                    read_epoch,
                    window,
                    ascending: request.ascending,
                    budget,
                })
                .await?;
            self.ensure_query_scan_edges("cypher_graph_kernel_window_edge_visits", edge_visits)?;
            self.ensure_query_intermediate_rows("cypher_graph_kernel_window_rows", vertices.len())?;
            let rows = graph_kernel_node_id_rows(vertices, budget)?;
            return Ok(Some(QueryResultSet::new(query.columns.clone(), rows)));
        }

        let (mut vertices, edge_visits) = self
            .reachable_vertices_in_hop_range_at(
                cell_id,
                &request.edge.edge_type,
                request.src,
                request.hop_range,
                read_epoch,
                budget,
            )
            .await?;
        self.ensure_query_scan_edges("cypher_graph_kernel_edge_visits", edge_visits)?;
        if let Some(dst) = request.edge.dst.id {
            vertices.retain(|vertex| *vertex == dst);
        }
        graph_kernel_order_vertices(&mut vertices, request.ascending);
        self.ensure_query_intermediate_rows("cypher_graph_kernel_rows", vertices.len())?;

        let rows = match request.projection {
            GraphKernelProjection::NodeId => graph_kernel_node_id_rows(vertices, budget)?,
            GraphKernelProjection::CountAll => {
                vec![QueryRow::new(vec![
                    QueryValue::Count(vertices.len() as u64),
                ])]
            }
        };
        Ok(Some(QueryResultSet::new(
            query.columns.clone(),
            self.apply_query_row_window(rows, window)?,
        )))
    }

    #[cfg(feature = "opencypher")]
    async fn try_execute_relationship_rows_query(
        &self,
        cell_id: &str,
        read_epoch: GraphEpoch,
        query: &ParsedRowQuery,
        window: QueryWindow,
        budget: &QueryBudget,
    ) -> Result<Option<QueryResultSet>> {
        let Some(edge) = relationship_rows_query_edge(query) else {
            return Ok(None);
        };
        if !relationship_rows_projection_supported(edge, &query.projections)
            || !relationship_rows_order_supported(
                edge,
                &query.projections,
                &query.columns,
                &query.order_by,
            )
        {
            return Ok(None);
        }
        let (Some(src), Some(dst)) = (edge.src.id, edge.dst.id) else {
            return Ok(None);
        };
        let relationships = if let Some((property, value)) = edge.properties.iter().next() {
            self.relationships_for_edge_property_at(RelationshipPropertyLookup {
                cell_id,
                edge_type: &edge.edge_type,
                src,
                dst,
                property,
                value,
                read_epoch,
                budget,
            })
            .await?
        } else {
            self.relationships_for_edge_at(cell_id, &edge.edge_type, src, dst, read_epoch, budget)
                .await?
        };
        let mut projected = Vec::with_capacity(relationships.len());
        for (relationship, metadata) in relationships {
            budget.check("cypher_relationship_rows_fast_path")?;
            if !relationship_metadata_matches(&metadata, edge) {
                continue;
            }
            let row = project_relationship_row(edge, &relationship, &metadata, &query.projections)?;
            let sort_keys =
                sort_keys_for_relationship_row(edge, &relationship, &metadata, &row, query)?;
            projected.push(ProjectedQueryRow { row, sort_keys });
            self.ensure_query_intermediate_rows(
                "cypher_relationship_rows_fast_path",
                projected.len(),
            )?;
        }
        Ok(Some(self.finish_projected_rows(
            query.columns.clone(),
            projected,
            &query.order_by,
            window,
            budget,
        )?))
    }

    #[cfg(feature = "opencypher")]
    async fn try_execute_source_relationship_id_rows_query(
        &self,
        cell_id: &str,
        read_epoch: GraphEpoch,
        query: &ParsedRowQuery,
        window: QueryWindow,
        budget: &QueryBudget,
    ) -> Result<Option<QueryResultSet>> {
        let Some(edge) = source_relationship_id_rows_query_edge(query) else {
            return Ok(None);
        };
        let Some(src) = edge.src.id else {
            return Ok(None);
        };

        let mut bindings = self
            .source_relationship_id_bindings_at(cell_id, edge, src, read_epoch, budget)
            .await?;
        self.ensure_query_intermediate_rows("cypher_source_relationship_id_rows", bindings.len())?;
        let mut projected = Vec::with_capacity(bindings.len());
        for binding in bindings.drain(..) {
            budget.check("cypher_source_relationship_id_project")?;
            let row = project_binding_row(&binding, &query.projections)?;
            let sort_keys = sort_keys_for_row(&binding, &row, &query.columns, &query.order_by)?;
            projected.push(ProjectedQueryRow { row, sort_keys });
        }
        Ok(Some(self.finish_projected_rows(
            query.columns.clone(),
            projected,
            &query.order_by,
            window,
            budget,
        )?))
    }

    #[cfg(feature = "opencypher")]
    async fn try_execute_relationship_count_query(
        &self,
        cell_id: &str,
        read_epoch: GraphEpoch,
        query: &ParsedRowQuery,
        window: QueryWindow,
        budget: &QueryBudget,
    ) -> Result<Option<QueryResultSet>> {
        let Some(edge) = relationship_count_query_edge(query) else {
            return Ok(None);
        };
        let (Some(src), Some(dst)) = (edge.src.id, edge.dst.id) else {
            return Ok(None);
        };
        let count = self
            .relationship_count_for_edge_at(cell_id, &edge.edge_type, src, dst, read_epoch, budget)
            .await?;
        let rows = vec![QueryRow::new(vec![QueryValue::Count(count)])];
        Ok(Some(QueryResultSet::new(
            query.columns.clone(),
            self.apply_query_row_window(rows, window)?,
        )))
    }

    #[cfg(feature = "opencypher")]
    async fn execute_parsed_opencypher_mutation(
        &self,
        context: QueryContext,
        query: ParsedMutationQuery,
    ) -> Result<QueryMutationResult> {
        validate_component("cell_id", &context.cell_id)?;
        if context.read_epoch.is_some() {
            return Err(GraphError::UnsupportedQuery {
                dialect: "OpenCypher",
                feature: "mutation queries cannot run at a historical read epoch".to_string(),
            });
        }
        let budget = QueryBudget::new(
            context.max_runtime_ms.or(self.limits.max_query_runtime_ms),
            context.cancellation_token.clone(),
        );
        budget.check("cypher_mutation")?;

        if query.patterns.is_empty() {
            return self
                .execute_patternless_mutation(&context, &query.actions, &budget)
                .await;
        }

        let read_epoch = self.current_epoch(&context.cell_id).await?;
        let mut bindings = self
            .match_row_patterns(&context.cell_id, &query.patterns, read_epoch, &budget)
            .await?;
        if let Some(predicate) = &query.predicate {
            let mut filtered = Vec::with_capacity(bindings.len());
            for row in bindings {
                budget.check("cypher_mutation_where")?;
                if row_predicate_matches(&row, predicate)? {
                    filtered.push(row);
                }
            }
            bindings = filtered;
        }

        let mut result = QueryMutationResult {
            matched_rows: bindings.len() as u64,
            ..QueryMutationResult::default()
        };
        let mut pending_metadata = BTreeMap::<VertexId, VertexMetadata>::new();
        let mut original_metadata = BTreeMap::<VertexId, VertexMetadata>::new();
        let mut pending_edge_metadata = BTreeMap::<BoundRelationship, EdgeMetadata>::new();
        let mut original_edge_metadata = BTreeMap::<BoundRelationship, EdgeMetadata>::new();

        for action in &query.actions {
            budget.check("cypher_mutation_action")?;
            match action {
                RowMutationAction::DeleteBinding { binding, detach } => {
                    let mut relationships = BTreeSet::new();
                    let mut vertices = BTreeSet::new();
                    for row in &bindings {
                        if let Some(relationship) = row.relationships.get(binding) {
                            relationships.insert(relationship.clone());
                        } else {
                            vertices.insert(row.get(binding)?);
                        }
                    }
                    for relationship in relationships {
                        budget.check("cypher_delete_relationship")?;
                        let edge_type = relationship.edge_type.clone();
                        let mutation = EdgeMutation {
                            cell_id: context.cell_id.clone(),
                            edge_type: edge_type.clone(),
                            src: relationship.src,
                            dst: relationship.dst,
                            idempotency_key: format!(
                                "{}.delete.{}.{}.{}.{}",
                                context.idempotency_key,
                                edge_type,
                                relationship.src,
                                relationship.dst,
                                relationship
                                    .relationship_id
                                    .map_or_else(|| "edge".to_string(), |id| id.to_string())
                            ),
                        };
                        let delete = if let Some(relationship_id) = relationship.relationship_id {
                            self.delete_relationship(mutation, relationship_id).await?
                        } else {
                            self.delete_edge(mutation).await?
                        };
                        if delete.deleted {
                            result.deleted_edges = result.deleted_edges.saturating_add(1);
                        } else {
                            result.noops = result.noops.saturating_add(1);
                        }
                    }
                    for vertex_id in vertices {
                        budget.check("cypher_delete_vertex")?;
                        let delete = if *detach {
                            self.detach_delete_vertex(
                                &context.cell_id,
                                vertex_id,
                                &format!(
                                    "{}.detach-delete-node.{vertex_id}",
                                    context.idempotency_key
                                ),
                            )
                            .await?
                        } else {
                            self.delete_vertex(
                                &context.cell_id,
                                vertex_id,
                                &format!("{}.delete-node.{vertex_id}", context.idempotency_key),
                            )
                            .await?
                        };
                        result.deleted_edges = result
                            .deleted_edges
                            .saturating_add(delete.incident_edges_deleted);
                        if delete.vertex_deleted {
                            result.updated_vertices = result.updated_vertices.saturating_add(1);
                        } else if delete.incident_edges_deleted == 0
                            && delete.relationships_deleted == 0
                        {
                            result.noops = result.noops.saturating_add(1);
                        }
                    }
                }
                RowMutationAction::DeleteRelationship { binding, detach } => {
                    let _ = detach;
                    let mut relationships = BTreeSet::new();
                    for row in &bindings {
                        let Some(relationship) = row.relationships.get(binding) else {
                            return Err(GraphError::UnsupportedQuery {
                                dialect: "OpenCypher",
                                feature: format!(
                                    "DELETE references unbound relationship {binding}"
                                ),
                            });
                        };
                        relationships.insert(relationship.clone());
                    }
                    for relationship in relationships {
                        budget.check("cypher_delete_relationship")?;
                        let edge_type = relationship.edge_type.clone();
                        let mutation = EdgeMutation {
                            cell_id: context.cell_id.clone(),
                            edge_type: edge_type.clone(),
                            src: relationship.src,
                            dst: relationship.dst,
                            idempotency_key: format!(
                                "{}.delete.{}.{}.{}.{}",
                                context.idempotency_key,
                                edge_type,
                                relationship.src,
                                relationship.dst,
                                relationship
                                    .relationship_id
                                    .map_or_else(|| "edge".to_string(), |id| id.to_string())
                            ),
                        };
                        let delete = if let Some(relationship_id) = relationship.relationship_id {
                            self.delete_relationship(mutation, relationship_id).await?
                        } else {
                            self.delete_edge(mutation).await?
                        };
                        if delete.deleted {
                            result.deleted_edges = result.deleted_edges.saturating_add(1);
                        } else {
                            result.noops = result.noops.saturating_add(1);
                        }
                    }
                }
                RowMutationAction::SetProperty { .. }
                | RowMutationAction::SetLabels { .. }
                | RowMutationAction::RemoveProperty { .. }
                | RowMutationAction::RemoveLabels { .. } => {
                    let mut state = VertexMutationApplyState {
                        cell_id: &context.cell_id,
                        read_epoch,
                        pending_metadata: &mut pending_metadata,
                        original_metadata: &mut original_metadata,
                        pending_edge_metadata: &mut pending_edge_metadata,
                        original_edge_metadata: &mut original_edge_metadata,
                        budget: &budget,
                    };
                    for row in &bindings {
                        self.apply_vertex_mutation_action(row, action, &mut state)
                            .await?;
                    }
                }
                RowMutationAction::MergeEdge { .. } => {
                    return Err(GraphError::UnsupportedQuery {
                        dialect: "OpenCypher",
                        feature: "MERGE is executable only as a standalone clause".to_string(),
                    });
                }
            }
        }

        for (vertex_id, metadata) in pending_metadata {
            budget.check("cypher_set_vertex_metadata")?;
            if original_metadata.get(&vertex_id) == Some(&metadata) {
                result.noops = result.noops.saturating_add(1);
                continue;
            }
            self.set_vertex_metadata(&context.cell_id, vertex_id, metadata)
                .await?;
            result.updated_vertices = result.updated_vertices.saturating_add(1);
        }
        for (relationship, metadata) in pending_edge_metadata {
            budget.check("cypher_set_relationship_metadata")?;
            if original_edge_metadata.get(&relationship) == Some(&metadata) {
                result.noops = result.noops.saturating_add(1);
                continue;
            }
            let changed = if let Some(relationship_id) = relationship.relationship_id {
                self.set_relationship_metadata(
                    &context.cell_id,
                    &relationship.edge_type,
                    relationship.src,
                    relationship.dst,
                    relationship_id,
                    metadata,
                )
                .await?
            } else {
                self.set_edge_metadata(
                    &context.cell_id,
                    &relationship.edge_type,
                    relationship.src,
                    relationship.dst,
                    metadata,
                )
                .await?
            };
            if changed {
                result.updated_relationships = result.updated_relationships.saturating_add(1);
            } else {
                result.noops = result.noops.saturating_add(1);
            }
        }

        Ok(result)
    }

    #[cfg(feature = "opencypher")]
    async fn execute_patternless_mutation(
        &self,
        context: &QueryContext,
        actions: &[RowMutationAction],
        budget: &QueryBudget,
    ) -> Result<QueryMutationResult> {
        let mut result = QueryMutationResult::default();
        for action in actions {
            budget.check("cypher_patternless_mutation")?;
            let RowMutationAction::MergeEdge {
                edge_type,
                src,
                dst,
                src_metadata,
                dst_metadata,
                edge_metadata,
            } = action
            else {
                return Err(GraphError::UnsupportedQuery {
                    dialect: "OpenCypher",
                    feature: "patternless mutation supports only standalone MERGE".to_string(),
                });
            };
            let mutation = EdgeMutation {
                cell_id: context.cell_id.clone(),
                edge_type: edge_type.clone(),
                src: *src,
                dst: *dst,
                idempotency_key: format!(
                    "{}.merge.{}.{}.{}",
                    context.idempotency_key, edge_type, src, dst
                ),
            };
            let commit = if src_metadata.labels.is_empty()
                && src_metadata.properties.is_empty()
                && dst_metadata.labels.is_empty()
                && dst_metadata.properties.is_empty()
                && edge_metadata.properties.is_empty()
            {
                self.write_edge(mutation).await?
            } else if edge_metadata.properties.is_empty() {
                self.write_edge_with_vertex_metadata(
                    mutation,
                    src_metadata.clone(),
                    dst_metadata.clone(),
                )
                .await?
            } else {
                self.write_edge_with_full_metadata(
                    mutation,
                    src_metadata.clone(),
                    dst_metadata.clone(),
                    edge_metadata.clone(),
                )
                .await?
            };
            if commit.already_existed {
                result.noops = result.noops.saturating_add(1);
            } else {
                result.created_edges = result.created_edges.saturating_add(1);
            }
        }
        Ok(result)
    }

    #[cfg(feature = "opencypher")]
    async fn apply_vertex_mutation_action(
        &self,
        row: &BindingRow,
        action: &RowMutationAction,
        state: &mut VertexMutationApplyState<'_>,
    ) -> Result<()> {
        let binding = match action {
            RowMutationAction::SetProperty { binding, .. }
            | RowMutationAction::SetLabels { binding, .. }
            | RowMutationAction::RemoveProperty { binding, .. }
            | RowMutationAction::RemoveLabels { binding, .. } => binding,
            _ => {
                return Err(GraphError::UnsupportedQuery {
                    dialect: "OpenCypher",
                    feature: "non-vertex mutation action cannot update metadata".to_string(),
                });
            }
        };
        if let Some(relationship) = row.relationships.get(binding) {
            match action {
                RowMutationAction::SetProperty {
                    property, value, ..
                } => {
                    if state.pending_edge_metadata.get(relationship).is_none() {
                        state.budget.check("cypher_load_relationship_metadata")?;
                        let metadata = match row.relationship_metadata.get(relationship) {
                            Some(metadata) => metadata.clone(),
                            None => {
                                self.edge_metadata_at(
                                    state.cell_id,
                                    &relationship.edge_type,
                                    relationship.src,
                                    relationship.dst,
                                    state.read_epoch,
                                    state.budget,
                                )
                                .await?
                            }
                        };
                        state
                            .original_edge_metadata
                            .insert(relationship.clone(), metadata.clone());
                        state
                            .pending_edge_metadata
                            .insert(relationship.clone(), metadata);
                    }
                    let metadata = state
                        .pending_edge_metadata
                        .get_mut(relationship)
                        .ok_or_else(|| GraphError::UnsupportedQuery {
                            dialect: "OpenCypher",
                            feature: format!("metadata for relationship {binding} was not loaded"),
                        })?;
                    metadata.properties.insert(property.clone(), value.clone());
                    return Ok(());
                }
                RowMutationAction::RemoveProperty { property, .. } => {
                    if state.pending_edge_metadata.get(relationship).is_none() {
                        state.budget.check("cypher_load_relationship_metadata")?;
                        let metadata = match row.relationship_metadata.get(relationship) {
                            Some(metadata) => metadata.clone(),
                            None => {
                                self.edge_metadata_at(
                                    state.cell_id,
                                    &relationship.edge_type,
                                    relationship.src,
                                    relationship.dst,
                                    state.read_epoch,
                                    state.budget,
                                )
                                .await?
                            }
                        };
                        state
                            .original_edge_metadata
                            .insert(relationship.clone(), metadata.clone());
                        state
                            .pending_edge_metadata
                            .insert(relationship.clone(), metadata);
                    }
                    let metadata = state
                        .pending_edge_metadata
                        .get_mut(relationship)
                        .ok_or_else(|| GraphError::UnsupportedQuery {
                            dialect: "OpenCypher",
                            feature: format!("metadata for relationship {binding} was not loaded"),
                        })?;
                    metadata.properties.remove(property);
                    return Ok(());
                }
                RowMutationAction::SetLabels { .. } | RowMutationAction::RemoveLabels { .. } => {
                    return Err(GraphError::UnsupportedQuery {
                        dialect: "OpenCypher",
                        feature: "relationship labels are not executable in Query engine"
                            .to_string(),
                    });
                }
                _ => {}
            }
        }
        let vertex_id = row.get(binding)?;
        if state.pending_metadata.get(&vertex_id).is_none() {
            state.budget.check("cypher_load_mutation_metadata")?;
            let metadata = match row.metadata.get(binding) {
                Some(metadata) => metadata.clone(),
                None => {
                    self.vertex_metadata_at(
                        state.cell_id,
                        vertex_id,
                        state.read_epoch,
                        state.budget,
                    )
                    .await?
                }
            };
            state.original_metadata.insert(vertex_id, metadata.clone());
            state.pending_metadata.insert(vertex_id, metadata);
        }
        let metadata = state.pending_metadata.get_mut(&vertex_id).ok_or_else(|| {
            GraphError::UnsupportedQuery {
                dialect: "OpenCypher",
                feature: format!("metadata for {binding} was not loaded"),
            }
        })?;
        match action {
            RowMutationAction::SetProperty {
                property, value, ..
            } => {
                metadata.properties.insert(property.clone(), value.clone());
            }
            RowMutationAction::SetLabels { labels, .. } => {
                metadata.labels.extend(labels.iter().cloned());
            }
            RowMutationAction::RemoveProperty { property, .. } => {
                metadata.properties.remove(property);
            }
            RowMutationAction::RemoveLabels { labels, .. } => {
                for label in labels {
                    metadata.labels.remove(label);
                }
            }
            _ => {}
        }
        Ok(())
    }

    pub async fn execute_query_plan(&self, plan: QueryPlan) -> Result<QueryOutput> {
        self.validate_executable_query_plan(&plan).await?;
        let budget = QueryBudget::new(
            plan.max_runtime_ms.or(self.limits.max_query_runtime_ms),
            None,
        );
        budget.check("query_plan")?;
        match plan.physical {
            PhysicalQueryPlan::WriteEdge {
                edge_type,
                src,
                dst,
            } => {
                let result = self
                    .create_relationship(
                        EdgeMutation {
                            cell_id: plan.cell_id,
                            edge_type,
                            src,
                            dst,
                            idempotency_key: plan.idempotency_key,
                        },
                        EdgeMetadata::default(),
                    )
                    .await?;
                Ok(QueryOutput::Write(CommitResult {
                    epoch: result.epoch,
                    already_existed: result.already_created,
                }))
            }
            PhysicalQueryPlan::WriteEdgeWithMetadata {
                edge_type,
                src,
                dst,
                src_metadata,
                dst_metadata,
            } => {
                let result = self
                    .create_relationship_with_vertex_metadata(
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
                Ok(QueryOutput::Write(CommitResult {
                    epoch: result.epoch,
                    already_existed: result.already_created,
                }))
            }
            PhysicalQueryPlan::WriteEdgeWithFullMetadata {
                edge_type,
                src,
                dst,
                src_metadata,
                dst_metadata,
                edge_metadata,
            } => {
                let result = self
                    .create_relationship_with_full_metadata(
                        EdgeMutation {
                            cell_id: plan.cell_id,
                            edge_type,
                            src,
                            dst,
                            idempotency_key: plan.idempotency_key,
                        },
                        src_metadata,
                        dst_metadata,
                        edge_metadata,
                    )
                    .await?;
                Ok(QueryOutput::Write(CommitResult {
                    epoch: result.epoch,
                    already_existed: result.already_created,
                }))
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
                        Some(&budget),
                    )
                    .await?;
                budget.check("query_out_neighbors")?;
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
                        (min_hops, max_hops),
                        read_epoch,
                        &budget,
                    )
                    .await?
                    .0;
                budget.check("query_reachable")?;
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
    async fn query_read_epoch(&self, context: &QueryContext) -> Result<GraphEpoch> {
        match context.read_epoch {
            Some(read_epoch) => {
                let current_epoch = self.current_epoch(&context.cell_id).await?;
                if read_epoch > current_epoch {
                    return Err(GraphError::SnapshotAhead {
                        cell_id: context.cell_id.clone(),
                        read_epoch,
                        current_epoch,
                    });
                }
                self.pin_read_epoch(&context.cell_id, "query_read_epoch", read_epoch)
                    .await?;
                Ok(read_epoch)
            }
            None => {
                self.pin_current_read_epoch(&context.cell_id, "query_read_epoch")
                    .await
            }
        }
    }

    #[cfg(feature = "opencypher")]
    pub fn start_query_stats_refresh_job(
        self: Arc<Self>,
        specs: Vec<QueryStatsRefreshSpec>,
        interval: Duration,
    ) -> Result<QueryStatsRefreshHandle> {
        if specs.is_empty() {
            return Err(GraphError::UnsupportedQuery {
                dialect: "QueryStats",
                feature: "stats refresh job requires at least one spec".to_string(),
            });
        }
        if interval.is_zero() {
            return Err(GraphError::AdmissionRejected {
                operation: "query_stats_refresh_interval_ms",
                actual: 0,
                limit: u64::MAX,
            });
        }
        for spec in &specs {
            validate_component("cell_id", &spec.cell_id)?;
            validate_query_stats_refresh_kind(&spec.kind)?;
        }
        let (stop_tx, mut stop_rx) = tokio::sync::watch::channel(false);
        let handle = tokio::spawn(async move {
            loop {
                for spec in &specs {
                    if *stop_rx.borrow() {
                        return Ok(());
                    }
                    if let Err(err) = self.refresh_query_stats_spec(spec).await {
                        tracing::warn!(
                            target: "slatedb_graph_kernel",
                            cell_id = %spec.cell_id,
                            error = %err,
                            "query stats background refresh failed"
                        );
                    }
                }
                tokio::select! {
                    changed = stop_rx.changed() => {
                        if changed.is_err() || *stop_rx.borrow() {
                            return Ok(());
                        }
                    }
                    _ = tokio::time::sleep(interval) => {}
                }
            }
        });
        Ok(QueryStatsRefreshHandle {
            stop_tx: Some(stop_tx),
            handle: Some(handle),
        })
    }

    #[cfg(feature = "opencypher")]
    pub async fn refresh_query_stats_spec(
        &self,
        spec: &QueryStatsRefreshSpec,
    ) -> Result<QueryStatsRefreshResult> {
        validate_component("cell_id", &spec.cell_id)?;
        validate_query_stats_refresh_kind(&spec.kind)?;
        match &spec.kind {
            QueryStatsRefreshKind::Cardinality(QueryCardinalityStatsKind::EdgeType {
                edge_type,
            }) => Ok(QueryStatsRefreshResult::Cardinality(
                self.refresh_edge_type_query_stats(&spec.cell_id, edge_type)
                    .await?,
            )),
            QueryStatsRefreshKind::Cardinality(QueryCardinalityStatsKind::VertexLabel {
                label,
            }) => Ok(QueryStatsRefreshResult::Cardinality(
                self.refresh_vertex_label_query_stats(&spec.cell_id, label)
                    .await?,
            )),
            QueryStatsRefreshKind::Cardinality(QueryCardinalityStatsKind::VertexProperty {
                property,
                value,
            }) => Ok(QueryStatsRefreshResult::Cardinality(
                self.refresh_vertex_property_query_stats(&spec.cell_id, property, value)
                    .await?,
            )),
            QueryStatsRefreshKind::Cardinality(QueryCardinalityStatsKind::EdgeProperty {
                edge_type,
                property,
                value,
            }) => Ok(QueryStatsRefreshResult::Cardinality(
                self.refresh_edge_property_query_stats(&spec.cell_id, edge_type, property, value)
                    .await?,
            )),
            QueryStatsRefreshKind::VertexPropertyHistogram { property } => {
                Ok(QueryStatsRefreshResult::Histogram(
                    self.refresh_vertex_property_histogram_query_stats(&spec.cell_id, property)
                        .await?,
                ))
            }
            QueryStatsRefreshKind::EdgePropertyHistogram {
                edge_type,
                property,
            } => Ok(QueryStatsRefreshResult::Histogram(
                self.refresh_edge_property_histogram_query_stats(
                    &spec.cell_id,
                    edge_type,
                    property,
                )
                .await?,
            )),
        }
    }

    #[cfg(feature = "opencypher")]
    pub async fn refresh_edge_type_query_stats(
        &self,
        cell_id: &str,
        edge_type: &str,
    ) -> Result<QueryCardinalityStatsRefresh> {
        validate_component("cell_id", cell_id)?;
        validate_component("edge_type", edge_type)?;
        self.ensure_write_authority(cell_id, "refresh_edge_type_query_stats")?;
        let read_epoch = self.snapshot(cell_id).await?.read_epoch();
        let budget = QueryBudget::new(self.limits.max_query_runtime_ms, None);
        let count = self
            .edge_type_cardinality_from_degree_counters(cell_id, edge_type, &budget)
            .await?;
        let stats = QueryStatsRecord::point_count(count, read_epoch, graph_now_millis());
        self.publish_query_stats_record_after_snapshot(
            cell_id,
            "refresh_edge_type_query_stats",
            read_epoch,
            keys::query_stats_edge_type(cell_id, edge_type),
            &stats,
        )
        .await?;
        Ok(QueryCardinalityStatsRefresh {
            cell_id: cell_id.to_string(),
            read_epoch,
            kind: QueryCardinalityStatsKind::EdgeType {
                edge_type: edge_type.to_string(),
            },
            count,
            stats,
        })
    }

    #[cfg(feature = "opencypher")]
    pub async fn refresh_vertex_label_query_stats(
        &self,
        cell_id: &str,
        label: &str,
    ) -> Result<QueryCardinalityStatsRefresh> {
        validate_component("cell_id", cell_id)?;
        validate_component("label", label)?;
        self.ensure_write_authority(cell_id, "refresh_vertex_label_query_stats")?;
        let read_epoch = self.snapshot(cell_id).await?.read_epoch();
        let budget = QueryBudget::new(self.limits.max_query_runtime_ms, None);
        let count = self
            .count_vertex_label_index_at(cell_id, label, read_epoch, &budget)
            .await?;
        let stats = QueryStatsRecord::point_count(count, read_epoch, graph_now_millis());
        self.publish_query_stats_record_after_snapshot(
            cell_id,
            "refresh_vertex_label_query_stats",
            read_epoch,
            keys::query_stats_vertex_label(cell_id, label),
            &stats,
        )
        .await?;
        Ok(QueryCardinalityStatsRefresh {
            cell_id: cell_id.to_string(),
            read_epoch,
            kind: QueryCardinalityStatsKind::VertexLabel {
                label: label.to_string(),
            },
            count,
            stats,
        })
    }

    #[cfg(feature = "opencypher")]
    pub async fn refresh_vertex_property_query_stats(
        &self,
        cell_id: &str,
        property: &str,
        value: &VertexPropertyValue,
    ) -> Result<QueryCardinalityStatsRefresh> {
        validate_component("cell_id", cell_id)?;
        validate_component("property", property)?;
        self.ensure_write_authority(cell_id, "refresh_vertex_property_query_stats")?;
        let read_epoch = self.snapshot(cell_id).await?.read_epoch();
        let budget = QueryBudget::new(self.limits.max_query_runtime_ms, None);
        let count = self
            .count_vertex_property_index_at(cell_id, property, value, read_epoch, &budget)
            .await?;
        let encoded = encode_vertex_property_value_key(value);
        let histogram = self
            .vertex_property_histogram_counts(cell_id, property, read_epoch, &budget)
            .await?;
        let stats = stats_record_from_bucket_count(count, read_epoch, &histogram);
        self.publish_query_stats_record_after_snapshot(
            cell_id,
            "refresh_vertex_property_query_stats",
            read_epoch,
            keys::query_stats_vertex_property(cell_id, property, &encoded),
            &stats,
        )
        .await?;
        Ok(QueryCardinalityStatsRefresh {
            cell_id: cell_id.to_string(),
            read_epoch,
            kind: QueryCardinalityStatsKind::VertexProperty {
                property: property.to_string(),
                value: value.clone(),
            },
            count,
            stats,
        })
    }

    #[cfg(feature = "opencypher")]
    pub async fn refresh_edge_property_query_stats(
        &self,
        cell_id: &str,
        edge_type: &str,
        property: &str,
        value: &VertexPropertyValue,
    ) -> Result<QueryCardinalityStatsRefresh> {
        validate_component("cell_id", cell_id)?;
        validate_component("edge_type", edge_type)?;
        validate_component("property", property)?;
        self.ensure_write_authority(cell_id, "refresh_edge_property_query_stats")?;
        let read_epoch = self.snapshot(cell_id).await?.read_epoch();
        let budget = QueryBudget::new(self.limits.max_query_runtime_ms, None);
        let count = self
            .count_edge_property_index_at(cell_id, edge_type, property, value, read_epoch, &budget)
            .await?;
        let encoded = encode_vertex_property_value_key(value);
        let histogram = self
            .edge_property_histogram_counts(cell_id, edge_type, property, read_epoch, &budget)
            .await?;
        let stats = stats_record_from_bucket_count(count, read_epoch, &histogram);
        self.publish_query_stats_record_after_snapshot(
            cell_id,
            "refresh_edge_property_query_stats",
            read_epoch,
            keys::query_stats_edge_property(cell_id, edge_type, property, &encoded),
            &stats,
        )
        .await?;
        Ok(QueryCardinalityStatsRefresh {
            cell_id: cell_id.to_string(),
            read_epoch,
            kind: QueryCardinalityStatsKind::EdgeProperty {
                edge_type: edge_type.to_string(),
                property: property.to_string(),
                value: value.clone(),
            },
            count,
            stats,
        })
    }

    #[cfg(feature = "opencypher")]
    pub async fn refresh_vertex_property_histogram_query_stats(
        &self,
        cell_id: &str,
        property: &str,
    ) -> Result<QueryStatsHistogramRefresh> {
        validate_component("cell_id", cell_id)?;
        validate_component("property", property)?;
        self.ensure_write_authority(cell_id, "refresh_vertex_property_histogram_query_stats")?;
        let read_epoch = self.snapshot(cell_id).await?.read_epoch();
        let budget = QueryBudget::new(self.limits.max_query_runtime_ms, None);
        let buckets = self
            .vertex_property_histogram_counts(cell_id, property, read_epoch, &budget)
            .await?;
        let stats = stats_record_from_histogram(read_epoch, &buckets);
        self.publish_query_stats_histogram_after_snapshot(
            QueryStatsHistogramPublish {
                cell_id,
                operation: "refresh_vertex_property_histogram_query_stats",
                read_epoch,
                histogram_key: keys::query_stats_vertex_property_histogram(cell_id, property),
                bucket_prefix: keys::query_stats_vertex_property_prefix(cell_id, property),
                stats: &stats,
                buckets: &buckets,
            },
            |encoded| keys::query_stats_vertex_property(cell_id, property, encoded),
        )
        .await?;
        Ok(QueryStatsHistogramRefresh {
            cell_id: cell_id.to_string(),
            read_epoch,
            property: property.to_string(),
            edge_type: None,
            stats,
            buckets,
        })
    }

    #[cfg(feature = "opencypher")]
    pub async fn refresh_edge_property_histogram_query_stats(
        &self,
        cell_id: &str,
        edge_type: &str,
        property: &str,
    ) -> Result<QueryStatsHistogramRefresh> {
        validate_component("cell_id", cell_id)?;
        validate_component("edge_type", edge_type)?;
        validate_component("property", property)?;
        self.ensure_write_authority(cell_id, "refresh_edge_property_histogram_query_stats")?;
        let read_epoch = self.snapshot(cell_id).await?.read_epoch();
        let budget = QueryBudget::new(self.limits.max_query_runtime_ms, None);
        let buckets = self
            .edge_property_histogram_counts(cell_id, edge_type, property, read_epoch, &budget)
            .await?;
        let stats = stats_record_from_histogram(read_epoch, &buckets);
        self.publish_query_stats_histogram_after_snapshot(
            QueryStatsHistogramPublish {
                cell_id,
                operation: "refresh_edge_property_histogram_query_stats",
                read_epoch,
                histogram_key: keys::query_stats_edge_property_histogram(
                    cell_id, edge_type, property,
                ),
                bucket_prefix: keys::query_stats_edge_property_prefix(cell_id, edge_type, property),
                stats: &stats,
                buckets: &buckets,
            },
            |encoded| keys::query_stats_edge_property(cell_id, edge_type, property, encoded),
        )
        .await?;
        Ok(QueryStatsHistogramRefresh {
            cell_id: cell_id.to_string(),
            read_epoch,
            property: property.to_string(),
            edge_type: Some(edge_type.to_string()),
            stats,
            buckets,
        })
    }

    #[cfg(feature = "opencypher")]
    async fn publish_query_stats_record_after_snapshot(
        &self,
        cell_id: &str,
        operation: &'static str,
        read_epoch: GraphEpoch,
        key: String,
        stats: &QueryStatsRecord,
    ) -> Result<()> {
        let _permit = self.acquire_graph_write_permit(operation).await?;
        let lock = self.acquire_cell_write_lock(cell_id, operation).await?;
        let result = async {
            let current_epoch = self.current_epoch(cell_id).await?;
            if current_epoch != read_epoch {
                return Err(GraphError::QueryStatsSnapshotChanged {
                    operation,
                    cell_id: cell_id.to_string(),
                    read_epoch,
                    current_epoch,
                });
            }
            let mut batch = GraphWriteBatch::new();
            batch.put(key.as_bytes(), encode_u64(stats.count));
            batch.put(
                keys::query_stats_record_key(&key).as_bytes(),
                encode_query_stats_record(stats),
            );
            self.write_graph_batch_strict(cell_id, operation, batch)
                .await
        }
        .await;
        release_cell_write_lock(lock, result).await
    }

    #[cfg(feature = "opencypher")]
    async fn publish_query_stats_histogram_after_snapshot(
        &self,
        publish: QueryStatsHistogramPublish<'_>,
        bucket_key: impl Fn(&str) -> String,
    ) -> Result<()> {
        let stale_bucket_keys = self.stale_query_stats_bucket_keys(&publish).await?;
        let _permit = self.acquire_graph_write_permit(publish.operation).await?;
        let lock = self
            .acquire_cell_write_lock(publish.cell_id, publish.operation)
            .await?;
        let result = async {
            let current_epoch = self.current_epoch(publish.cell_id).await?;
            if current_epoch != publish.read_epoch {
                return Err(GraphError::QueryStatsSnapshotChanged {
                    operation: publish.operation,
                    cell_id: publish.cell_id.to_string(),
                    read_epoch: publish.read_epoch,
                    current_epoch,
                });
            }
            let mut batch = GraphWriteBatch::new();
            batch.put(
                publish.histogram_key.as_bytes(),
                encode_u64(publish.stats.count),
            );
            batch.put(
                keys::query_stats_record_key(&publish.histogram_key).as_bytes(),
                encode_query_stats_record(publish.stats),
            );
            for key in &stale_bucket_keys {
                batch.delete(key.as_bytes());
                batch.delete(keys::query_stats_record_key(key).as_bytes());
            }
            for (encoded, count) in publish.buckets {
                let key = bucket_key(encoded);
                let bucket_stats = QueryStatsRecord {
                    count: *count,
                    read_epoch: publish.stats.read_epoch,
                    refreshed_at_ms: publish.stats.refreshed_at_ms,
                    distinct_values: publish.stats.distinct_values,
                    total_values: publish.stats.total_values,
                    most_common_count: publish.stats.most_common_count,
                };
                batch.put(key.as_bytes(), encode_u64(*count));
                batch.put(
                    keys::query_stats_record_key(&key).as_bytes(),
                    encode_query_stats_record(&bucket_stats),
                );
            }
            self.write_graph_batch_strict(publish.cell_id, publish.operation, batch)
                .await
        }
        .await;
        release_cell_write_lock(lock, result).await
    }

    #[cfg(feature = "opencypher")]
    async fn stale_query_stats_bucket_keys(
        &self,
        publish: &QueryStatsHistogramPublish<'_>,
    ) -> Result<Vec<String>> {
        let mut iter = self.scan_remote_prefix(&publish.bucket_prefix).await?;
        let mut keys_to_delete = Vec::new();
        while let Some(kv) = iter.next().await? {
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            let Some(encoded) = key.strip_prefix(&publish.bucket_prefix) else {
                return Err(GraphError::CorruptValue {
                    key,
                    reason: "query stats bucket key does not match scan prefix".to_string(),
                });
            };
            if encoded.contains('/') {
                continue;
            }
            if !publish.buckets.contains_key(encoded) {
                keys_to_delete.push(key);
            }
        }
        Ok(keys_to_delete)
    }

    #[cfg(feature = "opencypher")]
    async fn edge_type_cardinality_from_degree_counters(
        &self,
        cell_id: &str,
        edge_type: &str,
        budget: &QueryBudget,
    ) -> Result<u64> {
        budget.check("query_stats_edge_type_degree_scan")?;
        let mut iter = self
            .scan_remote_prefix(&keys::degree_out_prefix(cell_id, edge_type))
            .await?;
        let mut count = 0_u64;
        while let Some(kv) = iter.next().await? {
            budget.check("query_stats_edge_type_degree_scan")?;
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            let degree = decode_u64(&key, &kv.value)?;
            count = count
                .checked_add(degree)
                .ok_or_else(|| GraphError::CorruptValue {
                    key,
                    reason: "edge-type cardinality overflow while summing degree counters"
                        .to_string(),
                })?;
        }
        Ok(count)
    }

    #[cfg(feature = "opencypher")]
    async fn vertex_property_histogram_counts(
        &self,
        cell_id: &str,
        property: &str,
        read_epoch: GraphEpoch,
        budget: &QueryBudget,
    ) -> Result<BTreeMap<String, u64>> {
        budget.check("query_stats_vertex_property_histogram")?;
        let mut iter = self
            .scan_remote_prefix(&keys::vertex_property_index_property_prefix(
                cell_id, property,
            ))
            .await?;
        let mut buckets = BTreeMap::<String, u64>::new();
        let mut total = 0_u64;
        while let Some(kv) = iter.next().await? {
            budget.check("query_stats_vertex_property_histogram")?;
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            let (_cell_id, _property, encoded, vertex_id) = parse_vertex_property_index_key(&key)?;
            let metadata = self
                .vertex_metadata_at(cell_id, vertex_id, read_epoch, budget)
                .await?;
            let Some(value) = metadata.properties.get(property) else {
                continue;
            };
            if encode_vertex_property_value_key(value) != encoded {
                continue;
            }
            *buckets.entry(encoded).or_default() += 1;
            total = total
                .checked_add(1)
                .ok_or_else(|| GraphError::CorruptValue {
                    key,
                    reason: "vertex-property histogram count overflow".to_string(),
                })?;
            self.ensure_query_index_candidates(
                "query_stats_vertex_property_histogram_candidates",
                total as usize,
            )?;
        }
        Ok(buckets)
    }

    #[cfg(feature = "opencypher")]
    async fn edge_property_histogram_counts(
        &self,
        cell_id: &str,
        edge_type: &str,
        property: &str,
        read_epoch: GraphEpoch,
        budget: &QueryBudget,
    ) -> Result<BTreeMap<String, u64>> {
        budget.check("query_stats_edge_property_histogram")?;
        let mut iter = self
            .scan_remote_prefix(&keys::edge_property_index_property_prefix(
                cell_id, edge_type, property,
            ))
            .await?;
        let mut buckets = BTreeMap::<String, u64>::new();
        let mut total = 0_u64;
        let latest_snapshot = read_epoch == self.current_epoch(cell_id).await?;
        while let Some(kv) = iter.next().await? {
            budget.check("query_stats_edge_property_histogram")?;
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            let (_cell_id, _edge_type, _property, encoded, src, dst) =
                parse_edge_property_index_key(&key)?;
            let edge_exists = if latest_snapshot {
                self.edge_exists(cell_id, edge_type, src, dst).await?
            } else {
                self.edge_exists_at(cell_id, edge_type, src, dst, read_epoch)
                    .await?
            };
            if !edge_exists {
                continue;
            }
            let metadata = self
                .edge_metadata_at(cell_id, edge_type, src, dst, read_epoch, budget)
                .await?;
            let Some(value) = metadata.properties.get(property) else {
                continue;
            };
            if encode_vertex_property_value_key(value) != encoded {
                continue;
            }
            *buckets.entry(encoded).or_default() += 1;
            total = total
                .checked_add(1)
                .ok_or_else(|| GraphError::CorruptValue {
                    key,
                    reason: "edge-property histogram count overflow".to_string(),
                })?;
            self.ensure_query_index_candidates(
                "query_stats_edge_property_histogram_candidates",
                total as usize,
            )?;
        }
        Ok(buckets)
    }

    #[cfg(feature = "opencypher")]
    async fn count_vertex_label_index_at(
        &self,
        cell_id: &str,
        label: &str,
        read_epoch: GraphEpoch,
        budget: &QueryBudget,
    ) -> Result<u64> {
        let mut iter = self
            .scan_remote_prefix(&keys::vertex_label_delta_prefix(cell_id, label))
            .await?;
        let mut latest = BTreeMap::<VertexId, bool>::new();
        let mut saw_delta = false;
        while let Some(kv) = iter.next().await? {
            budget.check("query_stats_vertex_label_count_delta")?;
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            let (epoch, vertex_id) = parse_vertex_label_delta_key(&key)?;
            saw_delta = true;
            if epoch > read_epoch {
                break;
            }
            latest.insert(vertex_id, decode_vertex_index_delta(&key, &kv.value)?);
            self.ensure_query_index_candidates(
                "query_stats_vertex_label_count_candidates",
                latest.len(),
            )?;
        }
        if saw_delta {
            return Ok(latest.values().filter(|present| **present).count() as u64);
        }

        let mut iter = self
            .scan_remote_prefix(&keys::vertex_label_prefix(cell_id, label))
            .await?;
        let mut count = 0_u64;
        while let Some(kv) = iter.next().await? {
            budget.check("query_stats_vertex_label_count_current")?;
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            let _vertex_id = decode_u64(&key, &kv.value)?;
            count = count
                .checked_add(1)
                .ok_or_else(|| GraphError::CorruptValue {
                    key,
                    reason: "vertex-label stats count overflow".to_string(),
                })?;
            self.ensure_query_index_candidates(
                "query_stats_vertex_label_count_candidates",
                count as usize,
            )?;
        }
        Ok(count)
    }

    #[cfg(feature = "opencypher")]
    async fn count_vertex_property_index_at(
        &self,
        cell_id: &str,
        property: &str,
        value: &VertexPropertyValue,
        read_epoch: GraphEpoch,
        budget: &QueryBudget,
    ) -> Result<u64> {
        let encoded_keys = equivalent_property_index_keys(value);
        if encoded_keys.len() == 1 {
            return self
                .count_vertex_property_encoded_index_at(
                    cell_id,
                    property,
                    &encoded_keys[0],
                    read_epoch,
                    budget,
                )
                .await;
        }

        let mut vertices = BTreeSet::<VertexId>::new();
        for encoded in encoded_keys {
            self.collect_vertex_property_encoded_index_at(
                cell_id,
                property,
                &encoded,
                read_epoch,
                budget,
                &mut vertices,
            )
            .await?;
        }
        Ok(vertices.len() as u64)
    }

    #[cfg(feature = "opencypher")]
    async fn collect_vertex_property_encoded_index_at(
        &self,
        cell_id: &str,
        property: &str,
        encoded: &str,
        read_epoch: GraphEpoch,
        budget: &QueryBudget,
        vertices: &mut BTreeSet<VertexId>,
    ) -> Result<()> {
        let mut iter = self
            .scan_remote_prefix(&keys::vertex_property_index_delta_prefix(
                cell_id, property, encoded,
            ))
            .await?;
        let mut latest = BTreeMap::<VertexId, bool>::new();
        let mut saw_delta = false;
        while let Some(kv) = iter.next().await? {
            budget.check("query_stats_vertex_property_collect_delta")?;
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            let (epoch, vertex_id) = parse_vertex_property_index_delta_key(&key)?;
            saw_delta = true;
            if epoch > read_epoch {
                break;
            }
            latest.insert(vertex_id, decode_vertex_index_delta(&key, &kv.value)?);
            self.ensure_query_index_candidates(
                "query_stats_vertex_property_collect_candidates",
                latest.len(),
            )?;
        }
        if saw_delta {
            for (vertex_id, present) in latest {
                if present {
                    vertices.insert(vertex_id);
                    self.ensure_query_index_candidates(
                        "query_stats_vertex_property_collect_candidates",
                        vertices.len(),
                    )?;
                }
            }
            return Ok(());
        }

        let mut iter = self
            .scan_remote_prefix(&keys::vertex_property_index_prefix(
                cell_id, property, encoded,
            ))
            .await?;
        while let Some(kv) = iter.next().await? {
            budget.check("query_stats_vertex_property_collect_current")?;
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            let (_cell_id, _property, _encoded, vertex_id) = parse_vertex_property_index_key(&key)?;
            vertices.insert(vertex_id);
            self.ensure_query_index_candidates(
                "query_stats_vertex_property_collect_candidates",
                vertices.len(),
            )?;
        }
        Ok(())
    }

    #[cfg(feature = "opencypher")]
    async fn count_vertex_property_encoded_index_at(
        &self,
        cell_id: &str,
        property: &str,
        encoded: &str,
        read_epoch: GraphEpoch,
        budget: &QueryBudget,
    ) -> Result<u64> {
        let mut iter = self
            .scan_remote_prefix(&keys::vertex_property_index_delta_prefix(
                cell_id, property, encoded,
            ))
            .await?;
        let mut latest = BTreeMap::<VertexId, bool>::new();
        let mut saw_delta = false;
        while let Some(kv) = iter.next().await? {
            budget.check("query_stats_vertex_property_count_delta")?;
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            let (epoch, vertex_id) = parse_vertex_property_index_delta_key(&key)?;
            saw_delta = true;
            if epoch > read_epoch {
                break;
            }
            latest.insert(vertex_id, decode_vertex_index_delta(&key, &kv.value)?);
            self.ensure_query_index_candidates(
                "query_stats_vertex_property_count_candidates",
                latest.len(),
            )?;
        }
        if saw_delta {
            return Ok(latest.values().filter(|present| **present).count() as u64);
        }

        let mut iter = self
            .scan_remote_prefix(&keys::vertex_property_index_prefix(
                cell_id, property, encoded,
            ))
            .await?;
        let mut count = 0_u64;
        while let Some(kv) = iter.next().await? {
            budget.check("query_stats_vertex_property_count_current")?;
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            let (_cell_id, _property, _encoded, _vertex_id) =
                parse_vertex_property_index_key(&key)?;
            count = count
                .checked_add(1)
                .ok_or_else(|| GraphError::CorruptValue {
                    key,
                    reason: "vertex-property stats count overflow".to_string(),
                })?;
            self.ensure_query_index_candidates(
                "query_stats_vertex_property_count_candidates",
                count as usize,
            )?;
        }
        Ok(count)
    }

    #[cfg(feature = "opencypher")]
    async fn count_edge_property_index_at(
        &self,
        cell_id: &str,
        edge_type: &str,
        property: &str,
        value: &VertexPropertyValue,
        read_epoch: GraphEpoch,
        budget: &QueryBudget,
    ) -> Result<u64> {
        let mut edges = BTreeSet::<(VertexId, VertexId)>::new();
        for encoded in equivalent_property_index_keys(value) {
            let scan = EncodedPropertyIndexScan {
                cell_id,
                edge_type,
                property,
                encoded: &encoded,
                read_epoch,
                budget,
            };
            self.collect_edge_property_encoded_index_at(scan, &mut edges)
                .await?;
        }
        Ok(edges.len() as u64)
    }

    #[cfg(feature = "opencypher")]
    async fn collect_edge_property_encoded_index_at(
        &self,
        scan: EncodedPropertyIndexScan<'_>,
        edges: &mut BTreeSet<(VertexId, VertexId)>,
    ) -> Result<()> {
        let mut iter = self
            .scan_remote_prefix(&keys::edge_property_index_delta_prefix(
                scan.cell_id,
                scan.edge_type,
                scan.property,
                scan.encoded,
            ))
            .await?;
        let mut latest = BTreeMap::<(VertexId, VertexId), bool>::new();
        let mut saw_delta = false;
        while let Some(kv) = iter.next().await? {
            scan.budget.check("query_stats_edge_property_count_delta")?;
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            let (epoch, src, dst) = parse_edge_property_index_delta_key(&key)?;
            saw_delta = true;
            if epoch > scan.read_epoch {
                break;
            }
            latest.insert((src, dst), decode_vertex_index_delta(&key, &kv.value)?);
            self.ensure_query_index_candidates(
                "query_stats_edge_property_count_candidates",
                latest.len(),
            )?;
        }
        if saw_delta {
            for (edge, present) in latest {
                if present {
                    edges.insert(edge);
                    self.ensure_query_index_candidates(
                        "query_stats_edge_property_count_candidates",
                        edges.len(),
                    )?;
                }
            }
        } else {
            let mut iter = self
                .scan_remote_prefix(&keys::edge_property_index_prefix(
                    scan.cell_id,
                    scan.edge_type,
                    scan.property,
                    scan.encoded,
                ))
                .await?;
            while let Some(kv) = iter.next().await? {
                scan.budget
                    .check("query_stats_edge_property_count_current")?;
                let key = String::from_utf8_lossy(&kv.key).into_owned();
                let (_cell_id, _edge_type, _property, _encoded, src, dst) =
                    parse_edge_property_index_key(&key)?;
                edges.insert((src, dst));
                self.ensure_query_index_candidates(
                    "query_stats_edge_property_count_candidates",
                    edges.len(),
                )?;
            }
        }

        self.collect_relationship_property_encoded_index_at(scan, edges)
            .await?;
        Ok(())
    }

    #[cfg(feature = "opencypher")]
    async fn collect_relationship_property_encoded_index_at(
        &self,
        scan: EncodedPropertyIndexScan<'_>,
        edges: &mut BTreeSet<(VertexId, VertexId)>,
    ) -> Result<()> {
        let mut iter = self
            .scan_remote_prefix(&keys::relationship_property_index_delta_prefix(
                scan.cell_id,
                scan.edge_type,
                scan.property,
                scan.encoded,
            ))
            .await?;
        let mut latest = BTreeMap::<(VertexId, VertexId, RelationshipId), bool>::new();
        let mut saw_delta = false;
        while let Some(kv) = iter.next().await? {
            scan.budget
                .check("query_stats_relationship_property_count_delta")?;
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            let (epoch, src, dst, relationship_id) =
                parse_relationship_property_index_delta_key(&key)?;
            saw_delta = true;
            if epoch > scan.read_epoch {
                break;
            }
            latest.insert(
                (src, dst, relationship_id),
                decode_vertex_index_delta(&key, &kv.value)?,
            );
            self.ensure_query_index_candidates(
                "query_stats_relationship_property_count_candidates",
                latest.len(),
            )?;
        }
        if saw_delta {
            for ((src, dst, _relationship_id), present) in latest {
                if present {
                    edges.insert((src, dst));
                    self.ensure_query_index_candidates(
                        "query_stats_relationship_property_count_candidates",
                        edges.len(),
                    )?;
                }
            }
            return Ok(());
        }

        let mut iter = self
            .scan_remote_prefix(&keys::relationship_property_index_prefix(
                scan.cell_id,
                scan.edge_type,
                scan.property,
                scan.encoded,
            ))
            .await?;
        while let Some(kv) = iter.next().await? {
            scan.budget
                .check("query_stats_relationship_property_count_current")?;
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            let (_cell_id, _edge_type, _property, _encoded, src, dst, _relationship_id) =
                parse_relationship_property_index_key(&key)?;
            edges.insert((src, dst));
            self.ensure_query_index_candidates(
                "query_stats_relationship_property_count_candidates",
                edges.len(),
            )?;
        }
        Ok(())
    }

    #[cfg(feature = "opencypher")]
    async fn match_row_patterns(
        &self,
        cell_id: &str,
        patterns: &[RowPattern],
        read_epoch: GraphEpoch,
        budget: &QueryBudget,
    ) -> Result<Vec<BindingRow>> {
        if patterns.is_empty() {
            return Err(GraphError::UnsupportedQuery {
                dialect: "OpenCypher",
                feature: "MATCH requires at least one executable pattern".to_string(),
            });
        }

        self.match_row_patterns_from_rows(
            cell_id,
            patterns,
            read_epoch,
            budget,
            vec![BindingRow::default()],
        )
        .await
    }

    #[cfg(feature = "opencypher")]
    async fn match_row_pattern_groups(
        &self,
        cell_id: &str,
        groups: &[RowMatchGroup],
        read_epoch: GraphEpoch,
        budget: &QueryBudget,
    ) -> Result<Vec<BindingRow>> {
        if groups.is_empty() {
            return Err(GraphError::UnsupportedQuery {
                dialect: "OpenCypher",
                feature: "MATCH requires at least one executable pattern".to_string(),
            });
        }

        let groups = self
            .optimize_row_match_groups_with_stats(cell_id, groups, read_epoch)
            .await?;
        let mut rows = vec![BindingRow::default()];
        for group in &groups {
            budget.check("cypher_match_group")?;
            if group.patterns.is_empty() {
                return Err(GraphError::UnsupportedQuery {
                    dialect: "OpenCypher",
                    feature: "MATCH requires at least one executable pattern".to_string(),
                });
            }

            let mut group_rows = Vec::new();
            for row in rows {
                let mut matches = self
                    .match_row_patterns_from_rows(
                        cell_id,
                        &group.patterns,
                        read_epoch,
                        budget,
                        vec![row.clone()],
                    )
                    .await?;
                if let Some(predicate) = &group.predicate {
                    let mut filtered = Vec::with_capacity(matches.len());
                    for matched in matches {
                        budget.check("cypher_group_where")?;
                        if row_predicate_matches(&matched, predicate)? {
                            filtered.push(matched);
                        }
                    }
                    matches = filtered;
                }

                if matches.is_empty() && group.optional {
                    let mut optional_row = row;
                    optional_row.mark_optional_group_nulls(group);
                    self.push_binding_row(
                        &mut group_rows,
                        optional_row,
                        "cypher_optional_match_rows",
                    )?;
                } else {
                    for matched in matches {
                        self.push_binding_row(&mut group_rows, matched, "cypher_match_group_rows")?;
                    }
                }
            }

            rows = group_rows;
            self.ensure_query_intermediate_rows("cypher_match_group_pipeline_rows", rows.len())?;
            if rows.is_empty() {
                break;
            }
        }
        Ok(rows)
    }

    #[cfg(feature = "opencypher")]
    async fn match_row_patterns_from_rows(
        &self,
        cell_id: &str,
        patterns: &[RowPattern],
        read_epoch: GraphEpoch,
        budget: &QueryBudget,
        mut rows: Vec<BindingRow>,
    ) -> Result<Vec<BindingRow>> {
        let initial_bindings = common_binding_row_bound_names(&rows);
        let patterns = self
            .optimize_row_patterns_with_stats(cell_id, patterns, read_epoch, &initial_bindings)
            .await?;
        for pattern in &patterns {
            budget.check("cypher_match_pipeline")?;
            if let Some(next_rows) = self
                .match_expand_into_pattern_from_rows(cell_id, pattern, read_epoch, budget, &rows)
                .await?
            {
                rows = next_rows;
                self.ensure_query_intermediate_rows("cypher_expand_into_rows", rows.len())?;
                if rows.is_empty() {
                    break;
                }
                continue;
            }
            if let Some(next_rows) = self
                .match_hash_join_pattern_from_rows(cell_id, pattern, read_epoch, budget, &rows)
                .await?
            {
                rows = next_rows;
                self.ensure_query_intermediate_rows("cypher_hash_join_rows", rows.len())?;
                if rows.is_empty() {
                    break;
                }
                continue;
            }
            let mut next_rows = Vec::new();
            for row in rows {
                let Some(bound_pattern) = constrain_row_pattern(pattern, &row)? else {
                    continue;
                };
                let matches = self
                    .match_row_pattern(cell_id, &bound_pattern, read_epoch, budget)
                    .await?;
                for matched in matches {
                    budget.check("cypher_match_join")?;
                    if let Some(joined) = row.join(&matched) {
                        self.push_binding_row(&mut next_rows, joined, "cypher_match_join_rows")?;
                    }
                }
            }
            rows = next_rows;
            self.ensure_query_intermediate_rows("cypher_match_pipeline_rows", rows.len())?;
            if rows.is_empty() {
                break;
            }
        }
        Ok(rows)
    }

    #[cfg(feature = "opencypher")]
    async fn match_expand_into_pattern_from_rows(
        &self,
        cell_id: &str,
        pattern: &RowPattern,
        read_epoch: GraphEpoch,
        budget: &QueryBudget,
        rows: &[BindingRow],
    ) -> Result<Option<Vec<BindingRow>>> {
        let RowPattern::Edge(edge) = pattern else {
            return Ok(None);
        };
        if edge.hop_range.is_some() || rows.is_empty() {
            return Ok(None);
        }

        let current_epoch = self.current_epoch(cell_id).await?;
        let latest_snapshot = read_epoch == current_epoch;
        let mut next_rows = Vec::new();
        let mut metadata_cache = BTreeMap::new();
        let mut edge_metadata_cache = BTreeMap::new();
        for row in rows {
            budget.check("cypher_expand_into")?;
            let Some(RowPattern::Edge(bound_edge)) = constrain_row_pattern(pattern, row)? else {
                continue;
            };
            let (Some(src), Some(dst)) = (bound_edge.src.id, bound_edge.dst.id) else {
                return Ok(None);
            };
            let exists = if latest_snapshot {
                self.edge_exists(cell_id, &bound_edge.edge_type, src, dst)
                    .await?
            } else {
                self.edge_exists_at(cell_id, &bound_edge.edge_type, src, dst, read_epoch)
                    .await?
            };
            if !exists {
                continue;
            }
            let relationships = if let Some((property, value)) = bound_edge.properties.iter().next()
            {
                self.relationships_for_edge_property_at(RelationshipPropertyLookup {
                    cell_id,
                    edge_type: &bound_edge.edge_type,
                    src,
                    dst,
                    property,
                    value,
                    read_epoch,
                    budget,
                })
                .await?
            } else {
                self.relationships_for_edge_at(
                    cell_id,
                    &bound_edge.edge_type,
                    src,
                    dst,
                    read_epoch,
                    budget,
                )
                .await?
            };
            for (relationship, relationship_metadata) in relationships {
                let Some(mut matched) =
                    BindingRow::from_relationship(&bound_edge, relationship, relationship_metadata)
                else {
                    continue;
                };
                self.hydrate_binding_metadata(
                    cell_id,
                    read_epoch,
                    &mut matched,
                    &mut metadata_cache,
                    budget,
                )
                .await?;
                self.hydrate_row_relationship_metadata(
                    cell_id,
                    read_epoch,
                    &mut matched,
                    &bound_edge,
                    &mut edge_metadata_cache,
                    budget,
                )
                .await?;
                if row_matches_edge_pattern(&matched, &bound_edge)? {
                    if let Some(joined) = row.join(&matched) {
                        self.push_binding_row(&mut next_rows, joined, "cypher_expand_into_rows")?;
                    }
                }
            }
        }
        Ok(Some(next_rows))
    }

    #[cfg(feature = "opencypher")]
    async fn match_hash_join_pattern_from_rows(
        &self,
        cell_id: &str,
        pattern: &RowPattern,
        read_epoch: GraphEpoch,
        budget: &QueryBudget,
        rows: &[BindingRow],
    ) -> Result<Option<Vec<BindingRow>>> {
        if rows.len() <= 1 {
            return Ok(None);
        }
        let row_bindings = binding_rows_bound_names_union(rows);
        let pattern_bindings = row_pattern_bound_names(pattern);
        let join_bindings: Vec<_> = row_bindings
            .intersection(&pattern_bindings)
            .cloned()
            .collect();

        if join_bindings.is_empty() {
            let matches = self
                .match_row_pattern(cell_id, pattern, read_epoch, budget)
                .await?;
            let mut next_rows = Vec::new();
            for row in rows {
                for matched in &matches {
                    budget.check("cypher_precomputed_cross_join")?;
                    if let Some(joined) = row.join(matched) {
                        self.push_binding_row(
                            &mut next_rows,
                            joined,
                            "cypher_precomputed_cross_join_rows",
                        )?;
                    }
                }
            }
            return Ok(Some(next_rows));
        }

        if !hash_joinable_pattern(pattern) {
            return Ok(None);
        }
        let matches = self
            .match_row_pattern(cell_id, pattern, read_epoch, budget)
            .await?;
        let mut matches_by_key = BTreeMap::<Vec<VertexId>, Vec<BindingRow>>::new();
        for matched in matches {
            if let Some(key) = binding_row_join_key(&matched, &join_bindings) {
                matches_by_key.entry(key).or_default().push(matched);
            }
        }

        let mut next_rows = Vec::new();
        for row in rows {
            budget.check("cypher_hash_join")?;
            let Some(key) = binding_row_join_key(row, &join_bindings) else {
                continue;
            };
            let Some(matches) = matches_by_key.get(&key) else {
                continue;
            };
            for matched in matches {
                if let Some(joined) = row.join(matched) {
                    self.push_binding_row(&mut next_rows, joined, "cypher_hash_join_rows")?;
                }
            }
        }
        Ok(Some(next_rows))
    }

    #[cfg(feature = "opencypher")]
    async fn match_row_pattern(
        &self,
        cell_id: &str,
        pattern: &RowPattern,
        read_epoch: GraphEpoch,
        budget: &QueryBudget,
    ) -> Result<Vec<BindingRow>> {
        validate_component("cell_id", cell_id)?;
        budget.check("cypher_match")?;
        match pattern {
            RowPattern::Node(node) => {
                self.match_node_row_pattern(cell_id, node, read_epoch, budget)
                    .await
            }
            RowPattern::Edge(edge) => {
                self.match_edge_row_pattern(cell_id, edge, read_epoch, budget)
                    .await
            }
        }
    }

    #[cfg(feature = "opencypher")]
    async fn match_node_row_pattern(
        &self,
        cell_id: &str,
        node: &RowNodePattern,
        read_epoch: GraphEpoch,
        budget: &QueryBudget,
    ) -> Result<Vec<BindingRow>> {
        let Some(vertices) = self
            .candidate_vertex_ids(cell_id, node, read_epoch, budget)
            .await?
        else {
            return Err(GraphError::UnsupportedQuery {
                dialect: "OpenCypher",
                feature: "node-only MATCH requires an id, label, or property predicate".to_string(),
            });
        };
        self.ensure_query_index_candidates("cypher_node_candidates", vertices.len())?;
        let mut rows = Vec::with_capacity(vertices.len());
        let mut metadata_cache = BTreeMap::new();
        for vertex_id in vertices {
            budget.check("cypher_node_rows")?;
            if let Some(mut row) = BindingRow::from_node(node, vertex_id) {
                self.hydrate_binding_metadata(
                    cell_id,
                    read_epoch,
                    &mut row,
                    &mut metadata_cache,
                    budget,
                )
                .await?;
                if row_matches_node(&row, node)? {
                    self.push_binding_row(&mut rows, row, "cypher_node_rows")?;
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
        budget: &QueryBudget,
    ) -> Result<Vec<BindingRow>> {
        validate_component("edge_type", &edge.edge_type)?;
        if let Some((min_hops, max_hops)) = edge.hop_range {
            return self
                .match_reachable_row_pattern(cell_id, edge, min_hops, max_hops, read_epoch, budget)
                .await;
        }

        let access = self
            .best_row_edge_access_with_stats(cell_id, edge, read_epoch, &BTreeSet::new())
            .await?;
        match access {
            RowQueryAccess::ExpandInto { .. } => {
                self.match_edge_row_pattern_expand_into(cell_id, edge, read_epoch, budget)
                    .await
            }
            RowQueryAccess::BoundOutExpand { .. } => {
                let Some(sources) = self
                    .candidate_vertex_ids(cell_id, &edge.src, read_epoch, budget)
                    .await?
                else {
                    return self
                        .match_edge_row_pattern_full_scan(cell_id, edge, read_epoch, budget)
                        .await;
                };
                self.match_edge_row_pattern_from_sources(cell_id, edge, sources, read_epoch, budget)
                    .await
            }
            RowQueryAccess::BoundInExpand { .. } => {
                let Some(destinations) = self
                    .candidate_vertex_ids(cell_id, &edge.dst, read_epoch, budget)
                    .await?
                else {
                    return self
                        .match_edge_row_pattern_full_scan(cell_id, edge, read_epoch, budget)
                        .await;
                };
                self.match_edge_row_pattern_from_destinations(
                    cell_id,
                    edge,
                    destinations,
                    read_epoch,
                    budget,
                )
                .await
            }
            RowQueryAccess::EdgePropertyIndex { .. } => {
                self.match_edge_row_pattern_from_property_index(cell_id, edge, read_epoch, budget)
                    .await
            }
            RowQueryAccess::FullEdgeScan { .. } => {
                self.match_edge_row_pattern_full_scan(cell_id, edge, read_epoch, budget)
                    .await
            }
            RowQueryAccess::VariableLengthExpand { .. } => Err(GraphError::CorruptValue {
                key: format!("cell/{cell_id}/query/edge-access/{}", edge.edge_type),
                reason: "optimizer sent variable-length edge pattern to single-edge matcher"
                    .to_string(),
            }),
            RowQueryAccess::VertexIdSeek
            | RowQueryAccess::VertexPropertyIndex { .. }
            | RowQueryAccess::VertexLabelScan { .. }
            | RowQueryAccess::AllVertexScan => Err(GraphError::CorruptValue {
                key: format!("cell/{cell_id}/query/edge-access/{}", edge.edge_type),
                reason: "optimizer selected node access for edge pattern".to_string(),
            }),
        }
    }

    #[cfg(feature = "opencypher")]
    async fn match_edge_row_pattern_expand_into(
        &self,
        cell_id: &str,
        edge: &RowEdgePattern,
        read_epoch: GraphEpoch,
        budget: &QueryBudget,
    ) -> Result<Vec<BindingRow>> {
        let Some(sources) = self
            .candidate_vertex_ids(cell_id, &edge.src, read_epoch, budget)
            .await?
        else {
            return self
                .match_edge_row_pattern_full_scan(cell_id, edge, read_epoch, budget)
                .await;
        };
        let Some(destinations) = self
            .candidate_vertex_ids(cell_id, &edge.dst, read_epoch, budget)
            .await?
        else {
            return self
                .match_edge_row_pattern_full_scan(cell_id, edge, read_epoch, budget)
                .await;
        };
        self.ensure_query_index_candidates("cypher_edge_expand_into_sources", sources.len())?;
        self.ensure_query_index_candidates(
            "cypher_edge_expand_into_destinations",
            destinations.len(),
        )?;
        let candidate_pairs = sources
            .len()
            .checked_mul(destinations.len())
            .ok_or_else(|| GraphError::CorruptValue {
                key: format!("cell/{cell_id}/query/expand-into/{}", edge.edge_type),
                reason: "expand-into candidate pair count overflow".to_string(),
            })?;
        self.ensure_query_index_candidates("cypher_edge_expand_into_pairs", candidate_pairs)?;

        let current_epoch = self.current_epoch(cell_id).await?;
        let latest_snapshot = read_epoch == current_epoch;
        let mut rows = Vec::new();
        let mut metadata_cache = BTreeMap::new();
        let mut edge_metadata_cache = BTreeMap::new();
        let mut state = EdgeRowMatchState {
            cell_id,
            read_epoch,
            rows: &mut rows,
            metadata_cache: &mut metadata_cache,
            edge_metadata_cache: &mut edge_metadata_cache,
            budget,
        };
        for src in sources {
            for dst in &destinations {
                budget.check("cypher_edge_expand_into")?;
                let exists = if latest_snapshot {
                    self.edge_exists(cell_id, &edge.edge_type, src, *dst)
                        .await?
                } else {
                    self.edge_exists_at(cell_id, &edge.edge_type, src, *dst, read_epoch)
                        .await?
                };
                if exists {
                    self.push_matching_edge_row(edge, src, *dst, &mut state)
                        .await?;
                }
            }
        }
        Ok(rows)
    }

    #[cfg(feature = "opencypher")]
    async fn match_edge_row_pattern_from_property_index(
        &self,
        cell_id: &str,
        edge: &RowEdgePattern,
        read_epoch: GraphEpoch,
        budget: &QueryBudget,
    ) -> Result<Vec<BindingRow>> {
        let Some((property, value)) = edge.properties.iter().next() else {
            return Ok(Vec::new());
        };
        let pairs = self
            .scan_edge_property_index_at(
                cell_id,
                &edge.edge_type,
                property,
                value,
                read_epoch,
                budget,
            )
            .await?;
        self.ensure_query_index_candidates("cypher_edge_property_candidates", pairs.len())?;
        let mut rows = Vec::new();
        let mut metadata_cache = BTreeMap::new();
        let mut edge_metadata_cache = BTreeMap::new();
        let mut state = EdgeRowMatchState {
            cell_id,
            read_epoch,
            rows: &mut rows,
            metadata_cache: &mut metadata_cache,
            edge_metadata_cache: &mut edge_metadata_cache,
            budget,
        };
        for (src, dst) in pairs {
            budget.check("cypher_edge_property_rows")?;
            self.push_matching_edge_row(edge, src, dst, &mut state)
                .await?;
        }
        Ok(rows)
    }

    #[cfg(feature = "opencypher")]
    async fn match_edge_row_pattern_from_sources(
        &self,
        cell_id: &str,
        edge: &RowEdgePattern,
        sources: Vec<VertexId>,
        read_epoch: GraphEpoch,
        budget: &QueryBudget,
    ) -> Result<Vec<BindingRow>> {
        self.ensure_query_index_candidates("cypher_edge_source_candidates", sources.len())?;
        let mut rows = Vec::new();
        let mut metadata_cache = BTreeMap::new();
        let mut edge_metadata_cache = BTreeMap::new();
        let mut scanned_edges = 0_u64;
        {
            let mut state = EdgeRowMatchState {
                cell_id,
                read_epoch,
                rows: &mut rows,
                metadata_cache: &mut metadata_cache,
                edge_metadata_cache: &mut edge_metadata_cache,
                budget,
            };
            for src in sources {
                budget.check("cypher_edge_sources")?;
                let neighbors = self
                    .out_neighbors_at_for_query(cell_id, &edge.edge_type, src, read_epoch, budget)
                    .await?;
                scanned_edges = scanned_edges.saturating_add(neighbors.len() as u64);
                self.ensure_query_scan_edges("cypher_edge_neighbor_scan", scanned_edges)?;
                for dst in neighbors {
                    self.push_matching_edge_row(edge, src, dst, &mut state)
                        .await?;
                }
            }
        }
        Ok(rows)
    }

    #[cfg(feature = "opencypher")]
    async fn match_edge_row_pattern_from_destinations(
        &self,
        cell_id: &str,
        edge: &RowEdgePattern,
        destinations: Vec<VertexId>,
        read_epoch: GraphEpoch,
        budget: &QueryBudget,
    ) -> Result<Vec<BindingRow>> {
        self.ensure_query_index_candidates(
            "cypher_edge_destination_candidates",
            destinations.len(),
        )?;
        let mut rows = Vec::new();
        let mut metadata_cache = BTreeMap::new();
        let mut edge_metadata_cache = BTreeMap::new();
        let mut scanned_edges = 0_u64;
        {
            let mut state = EdgeRowMatchState {
                cell_id,
                read_epoch,
                rows: &mut rows,
                metadata_cache: &mut metadata_cache,
                edge_metadata_cache: &mut edge_metadata_cache,
                budget,
            };
            for dst in destinations {
                budget.check("cypher_edge_destinations")?;
                let neighbors = self
                    .in_neighbors_at_for_query(cell_id, &edge.edge_type, dst, read_epoch, budget)
                    .await?;
                scanned_edges = scanned_edges.saturating_add(neighbors.len() as u64);
                self.ensure_query_scan_edges("cypher_edge_reverse_neighbor_scan", scanned_edges)?;
                for src in neighbors {
                    self.push_matching_edge_row(edge, src, dst, &mut state)
                        .await?;
                }
            }
        }
        Ok(rows)
    }

    #[cfg(feature = "opencypher")]
    async fn match_edge_row_pattern_full_scan(
        &self,
        cell_id: &str,
        edge: &RowEdgePattern,
        read_epoch: GraphEpoch,
        budget: &QueryBudget,
    ) -> Result<Vec<BindingRow>> {
        let mut rows = Vec::new();
        let mut metadata_cache = BTreeMap::new();
        let mut edge_metadata_cache = BTreeMap::new();
        let records = self
            .edges_at_with_budget(cell_id, &edge.edge_type, read_epoch, Some(budget))
            .await?;
        self.ensure_query_scan_edges("cypher_edge_full_scan", records.len() as u64)?;
        {
            let mut state = EdgeRowMatchState {
                cell_id,
                read_epoch,
                rows: &mut rows,
                metadata_cache: &mut metadata_cache,
                edge_metadata_cache: &mut edge_metadata_cache,
                budget,
            };
            for record in records {
                budget.check("cypher_edge_full_scan")?;
                self.push_matching_edge_row(edge, record.src, record.dst, &mut state)
                    .await?;
            }
        }
        Ok(rows)
    }

    #[cfg(feature = "opencypher")]
    async fn push_matching_edge_row(
        &self,
        edge: &RowEdgePattern,
        src: VertexId,
        dst: VertexId,
        state: &mut EdgeRowMatchState<'_>,
    ) -> Result<()> {
        state.budget.check("cypher_edge_rows")?;
        if matches!(edge.src.id, Some(fixed_src) if fixed_src != src)
            || matches!(edge.dst.id, Some(fixed_dst) if fixed_dst != dst)
        {
            return Ok(());
        }
        let relationships = if let Some((property, value)) = edge.properties.iter().next() {
            self.relationships_for_edge_property_at(RelationshipPropertyLookup {
                cell_id: state.cell_id,
                edge_type: &edge.edge_type,
                src,
                dst,
                property,
                value,
                read_epoch: state.read_epoch,
                budget: state.budget,
            })
            .await?
        } else {
            self.relationships_for_edge_at(
                state.cell_id,
                &edge.edge_type,
                src,
                dst,
                state.read_epoch,
                state.budget,
            )
            .await?
        };
        for (relationship, metadata) in relationships {
            let Some(mut row) = BindingRow::from_relationship(edge, relationship, metadata) else {
                continue;
            };
            self.hydrate_binding_metadata(
                state.cell_id,
                state.read_epoch,
                &mut row,
                state.metadata_cache,
                state.budget,
            )
            .await?;
            self.hydrate_row_relationship_metadata(
                state.cell_id,
                state.read_epoch,
                &mut row,
                edge,
                state.edge_metadata_cache,
                state.budget,
            )
            .await?;
            if row_matches_edge_pattern(&row, edge)? {
                self.push_binding_row(state.rows, row, "cypher_edge_rows")?;
            }
        }
        Ok(())
    }

    #[cfg(feature = "opencypher")]
    async fn match_reachable_row_pattern(
        &self,
        cell_id: &str,
        edge: &RowEdgePattern,
        min_hops: u8,
        max_hops: u8,
        read_epoch: GraphEpoch,
        budget: &QueryBudget,
    ) -> Result<Vec<BindingRow>> {
        let Some(src) = edge.src.id else {
            return Err(GraphError::UnsupportedQuery {
                dialect: "OpenCypher",
                feature: "variable-length MATCH requires a fixed source id".to_string(),
            });
        };
        let (vertices, edge_visits) = self
            .reachable_vertices_in_hop_range_at(
                cell_id,
                &edge.edge_type,
                src,
                (min_hops, max_hops),
                read_epoch,
                budget,
            )
            .await?;
        budget.check("cypher_reachable")?;
        self.ensure_query_scan_edges("cypher_reachable_edge_visits", edge_visits)?;
        self.ensure_query_intermediate_rows("cypher_reachable_rows", vertices.len())?;
        let mut rows = Vec::with_capacity(vertices.len());
        let mut metadata_cache = BTreeMap::new();
        let mut edge_metadata_cache = BTreeMap::new();
        for dst in vertices {
            budget.check("cypher_reachable_rows")?;
            if matches!(edge.dst.id, Some(fixed_dst) if fixed_dst != dst) {
                continue;
            }
            if let Some(mut row) = BindingRow::from_edge(edge, src, dst) {
                self.hydrate_binding_metadata(
                    cell_id,
                    read_epoch,
                    &mut row,
                    &mut metadata_cache,
                    budget,
                )
                .await?;
                self.hydrate_row_relationship_metadata(
                    cell_id,
                    read_epoch,
                    &mut row,
                    edge,
                    &mut edge_metadata_cache,
                    budget,
                )
                .await?;
                if row_matches_edge_pattern(&row, edge)? {
                    self.push_binding_row(&mut rows, row, "cypher_reachable_rows")?;
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
        budget: &QueryBudget,
    ) -> Result<Option<Vec<VertexId>>> {
        budget.check("cypher_candidate_vertices")?;
        let access = self
            .best_row_node_access_with_stats(cell_id, pattern, &BTreeSet::new())
            .await?;
        match access {
            RowQueryAccess::VertexIdSeek => Ok(pattern.id.map(|id| vec![id])),
            RowQueryAccess::VertexPropertyIndex { property } => {
                let Some(value) = pattern.properties.get(&property) else {
                    return Err(GraphError::CorruptValue {
                        key: format!("cell/{cell_id}/query/node-access/{property}"),
                        reason: "optimizer selected missing vertex property".to_string(),
                    });
                };
                Ok(Some(
                    self.scan_vertex_property_index_at(
                        cell_id, &property, value, read_epoch, budget,
                    )
                    .await?,
                ))
            }
            RowQueryAccess::VertexLabelScan { label } => Ok(Some(
                self.scan_vertex_label_index_at(cell_id, &label, read_epoch, budget)
                    .await?,
            )),
            RowQueryAccess::AllVertexScan => Ok(None),
            RowQueryAccess::BoundOutExpand { .. }
            | RowQueryAccess::BoundInExpand { .. }
            | RowQueryAccess::ExpandInto { .. }
            | RowQueryAccess::EdgePropertyIndex { .. }
            | RowQueryAccess::FullEdgeScan { .. }
            | RowQueryAccess::VariableLengthExpand { .. } => Err(GraphError::CorruptValue {
                key: format!("cell/{cell_id}/query/node-access"),
                reason: "optimizer selected edge access for node pattern".to_string(),
            }),
        }
    }

    #[cfg(feature = "opencypher")]
    async fn vertex_metadata_at(
        &self,
        cell_id: &str,
        vertex_id: VertexId,
        read_epoch: GraphEpoch,
        budget: &QueryBudget,
    ) -> Result<VertexMetadata> {
        validate_component("cell_id", cell_id)?;
        let prefix = keys::vertex_delta_prefix(cell_id, vertex_id);
        let mut iter = self.scan_remote_prefix(&prefix).await?;
        let mut latest = None;
        let mut saw_delta = false;
        while let Some(kv) = iter.next().await? {
            budget.check("cypher_vertex_metadata_delta")?;
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
    async fn edge_metadata_at(
        &self,
        cell_id: &str,
        edge_type: &str,
        src: VertexId,
        dst: VertexId,
        read_epoch: GraphEpoch,
        budget: &QueryBudget,
    ) -> Result<EdgeMetadata> {
        validate_component("cell_id", cell_id)?;
        validate_component("edge_type", edge_type)?;
        let prefix = keys::edge_metadata_delta_prefix(cell_id, edge_type, src, dst);
        let mut iter = self.scan_remote_prefix(&prefix).await?;
        let mut latest = None;
        let mut saw_delta = false;
        while let Some(kv) = iter.next().await? {
            budget.check("cypher_edge_metadata_delta")?;
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            let epoch = parse_edge_metadata_delta_key(&key)?;
            saw_delta = true;
            if epoch > read_epoch {
                break;
            }
            latest = Some(decode_edge_metadata(&key, &kv.value)?);
        }
        if let Some(metadata) = latest {
            return Ok(metadata);
        }
        if saw_delta {
            return Ok(EdgeMetadata::default());
        }
        let key = keys::edge_metadata(cell_id, edge_type, src, dst);
        match self.read_remote(&key).await? {
            Some(value) => decode_edge_metadata(&key, &value),
            None => Ok(EdgeMetadata::default()),
        }
    }

    #[cfg(feature = "opencypher")]
    async fn relationship_metadata_at(
        &self,
        cell_id: &str,
        relationship: &BoundRelationship,
        read_epoch: GraphEpoch,
        budget: &QueryBudget,
    ) -> Result<EdgeMetadata> {
        let Some(relationship_id) = relationship.relationship_id else {
            return self
                .edge_metadata_at(
                    cell_id,
                    &relationship.edge_type,
                    relationship.src,
                    relationship.dst,
                    read_epoch,
                    budget,
                )
                .await;
        };
        budget.check("cypher_relationship_record")?;
        let key = keys::relationship(
            cell_id,
            &relationship.edge_type,
            relationship.src,
            relationship.dst,
            relationship_id,
        );
        let Some(value) = self.read_remote(&key).await? else {
            return Ok(EdgeMetadata::default());
        };
        let record = decode_relationship_record(&key, &value)?;
        if record.epoch > read_epoch {
            return Ok(EdgeMetadata::default());
        }
        let tombstone_key = keys::relationship_tombstone(
            cell_id,
            &relationship.edge_type,
            relationship.src,
            relationship.dst,
            relationship_id,
        );
        if let Some(value) = self.read_remote(&tombstone_key).await? {
            let tombstone_epoch = decode_u64(&tombstone_key, &value)?;
            if relationship_tombstone_hides_record(&record, tombstone_epoch, read_epoch) {
                return Ok(EdgeMetadata::default());
            }
        }
        let delta_prefix = keys::relationship_metadata_delta_prefix(
            cell_id,
            &relationship.edge_type,
            relationship.src,
            relationship.dst,
            relationship_id,
        );
        let mut iter = self.scan_remote_prefix(&delta_prefix).await?;
        let mut latest = None;
        while let Some(kv) = iter.next().await? {
            budget.check("cypher_relationship_metadata_delta")?;
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            let epoch = parse_relationship_metadata_delta_key(&key)?;
            if epoch > read_epoch {
                break;
            }
            latest = Some(decode_edge_metadata(&key, &kv.value)?);
        }
        if let Some(metadata) = latest {
            return Ok(metadata);
        }
        Ok(record.metadata)
    }

    #[cfg(feature = "opencypher")]
    async fn relationships_for_edge_at(
        &self,
        cell_id: &str,
        edge_type: &str,
        src: VertexId,
        dst: VertexId,
        read_epoch: GraphEpoch,
        budget: &QueryBudget,
    ) -> Result<Vec<(BoundRelationship, EdgeMetadata)>> {
        let cache_key = RelationshipRowsCacheKey::new(cell_id, edge_type, src, dst, read_epoch);
        if let Some(cached) = self.relationship_rows_cache.lock().await.get(&cache_key) {
            self.cache_metrics
                .record_hit(GraphCacheKind::RelationshipRows);
            return Ok(relationship_rows_from_cache_value(
                edge_type, src, dst, cached,
            ));
        }
        self.cache_metrics
            .record_miss(GraphCacheKind::RelationshipRows);

        let current_epoch = self.current_epoch(cell_id).await?;
        let latest_snapshot = read_epoch == current_epoch;
        let structural_exists = if latest_snapshot {
            self.edge_exists(cell_id, edge_type, src, dst).await?
        } else {
            self.edge_exists_at(cell_id, edge_type, src, dst, read_epoch)
                .await?
        };
        if !structural_exists {
            self.relationship_rows_cache.lock().await.insert(
                cache_key,
                relationship_rows_cache_value(&[]),
                cell_id.to_string(),
                false,
                &self.cache_metrics,
            );
            return Ok(Vec::new());
        }

        let mut rows = Vec::new();
        let mut saw_live_relationship_record = false;
        let tombstones = self
            .relationship_tombstone_epochs_for_edge_at(
                cell_id, edge_type, src, dst, read_epoch, budget,
            )
            .await?;
        let prefix = keys::relationship_edge_prefix(cell_id, edge_type, src, dst);
        let mut iter = self.scan_remote_prefix(&prefix).await?;
        while let Some(kv) = iter.next().await? {
            budget.check("cypher_relationship_edge_records")?;
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            let record = decode_relationship_record(&key, &kv.value)?;
            if record.epoch > read_epoch {
                continue;
            }
            if tombstones
                .get(&record.relationship_id)
                .is_some_and(|tombstone_epoch| {
                    relationship_tombstone_hides_record(&record, *tombstone_epoch, read_epoch)
                })
            {
                continue;
            }
            saw_live_relationship_record = true;
            let relationship = BoundRelationship {
                edge_type: record.edge_type,
                src: record.src,
                dst: record.dst,
                relationship_id: Some(record.relationship_id),
            };
            let metadata = if latest_snapshot {
                record.metadata
            } else {
                self.relationship_metadata_at(cell_id, &relationship, read_epoch, budget)
                    .await?
            };
            rows.push((relationship, metadata));
            self.ensure_query_index_candidates("cypher_relationship_edge_records", rows.len())?;
        }
        if saw_live_relationship_record {
            let structural_metadata = self
                .edge_metadata_at(cell_id, edge_type, src, dst, read_epoch, budget)
                .await?;
            if !structural_metadata.properties.is_empty() {
                rows.push((
                    BoundRelationship {
                        edge_type: edge_type.to_string(),
                        src,
                        dst,
                        relationship_id: None,
                    },
                    structural_metadata,
                ));
            }
        }
        if rows.is_empty() && !saw_live_relationship_record {
            let metadata = self
                .edge_metadata_at(cell_id, edge_type, src, dst, read_epoch, budget)
                .await?;
            rows.push((
                BoundRelationship {
                    edge_type: edge_type.to_string(),
                    src,
                    dst,
                    relationship_id: None,
                },
                metadata,
            ));
        }
        let cache_value = relationship_rows_cache_value(&rows);
        self.relationship_rows_cache.lock().await.insert(
            cache_key,
            cache_value,
            cell_id.to_string(),
            false,
            &self.cache_metrics,
        );
        Ok(rows)
    }

    #[cfg(feature = "opencypher")]
    async fn source_relationship_id_bindings_at(
        &self,
        cell_id: &str,
        edge: &RowEdgePattern,
        src: VertexId,
        read_epoch: GraphEpoch,
        budget: &QueryBudget,
    ) -> Result<Vec<BindingRow>> {
        let cache_key =
            SourceRelationshipRowsCacheKey::new(cell_id, &edge.edge_type, src, read_epoch);
        if let Some(cached) = self
            .source_relationship_rows_cache
            .lock()
            .await
            .get(&cache_key)
        {
            self.cache_metrics
                .record_hit(GraphCacheKind::RelationshipRows);
            return source_relationship_id_bindings_from_dsts(
                edge,
                src,
                cached.as_slice(),
                self,
                budget,
            );
        }
        self.cache_metrics
            .record_miss(GraphCacheKind::RelationshipRows);

        let tombstones = self
            .relationship_tombstone_epochs_for_source_at(
                cell_id,
                &edge.edge_type,
                src,
                read_epoch,
                budget,
            )
            .await?;
        let prefix = keys::relationship_source_prefix(cell_id, &edge.edge_type, src);
        let mut iter = self.scan_remote_prefix(&prefix).await?;
        let mut dsts = Vec::new();
        let mut relationship_dsts = BTreeSet::new();
        while let Some(kv) = iter.next().await? {
            budget.check("cypher_source_relationship_records")?;
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            let record = decode_relationship_record(&key, &kv.value)?;
            if record.epoch > read_epoch {
                continue;
            }
            if tombstones
                .get(&(record.dst, record.relationship_id))
                .is_some_and(|tombstone_epoch| {
                    relationship_tombstone_hides_record(&record, *tombstone_epoch, read_epoch)
                })
            {
                continue;
            }
            relationship_dsts.insert(record.dst);
            dsts.push(record.dst);
            self.ensure_query_intermediate_rows("cypher_source_relationship_records", dsts.len())?;
        }

        let structural_neighbors = self
            .out_neighbors_at_for_query(cell_id, &edge.edge_type, src, read_epoch, budget)
            .await?;
        for dst in structural_neighbors {
            budget.check("cypher_source_relationship_structural_fallback")?;
            if relationship_dsts.contains(&dst) {
                continue;
            }
            dsts.push(dst);
            self.ensure_query_intermediate_rows(
                "cypher_source_relationship_structural_fallback",
                dsts.len(),
            )?;
        }
        let cached = Arc::new(dsts);
        self.source_relationship_rows_cache.lock().await.insert(
            cache_key,
            Arc::clone(&cached),
            cell_id.to_string(),
            false,
            &self.cache_metrics,
        );
        source_relationship_id_bindings_from_dsts(edge, src, cached.as_slice(), self, budget)
    }

    #[cfg(feature = "opencypher")]
    async fn relationship_tombstone_epochs_for_edge_at(
        &self,
        cell_id: &str,
        edge_type: &str,
        src: VertexId,
        dst: VertexId,
        read_epoch: GraphEpoch,
        budget: &QueryBudget,
    ) -> Result<BTreeMap<RelationshipId, GraphEpoch>> {
        let mut tombstones = BTreeMap::new();
        let prefix = keys::relationship_tombstone_edge_prefix(cell_id, edge_type, src, dst);
        let mut iter = self.scan_remote_prefix(&prefix).await?;
        while let Some(kv) = iter.next().await? {
            budget.check("cypher_relationship_tombstones")?;
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            let relationship_id = parse_relationship_tombstone_edge_key(&key)?;
            let tombstone_epoch = decode_u64(&key, &kv.value)?;
            if tombstone_epoch <= read_epoch {
                tombstones.insert(relationship_id, tombstone_epoch);
            }
            self.ensure_query_index_candidates("cypher_relationship_tombstones", tombstones.len())?;
        }
        Ok(tombstones)
    }

    #[cfg(feature = "opencypher")]
    async fn relationship_tombstone_epochs_for_source_at(
        &self,
        cell_id: &str,
        edge_type: &str,
        src: VertexId,
        read_epoch: GraphEpoch,
        budget: &QueryBudget,
    ) -> Result<BTreeMap<(VertexId, RelationshipId), GraphEpoch>> {
        let mut tombstones = BTreeMap::new();
        let prefix = keys::relationship_tombstone_source_prefix(cell_id, edge_type, src);
        let mut iter = self.scan_remote_prefix(&prefix).await?;
        while let Some(kv) = iter.next().await? {
            budget.check("cypher_source_relationship_tombstones")?;
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            let (dst, relationship_id) = parse_relationship_tombstone_source_key(&key)?;
            let epoch = decode_u64(&key, &kv.value)?;
            if epoch <= read_epoch {
                tombstones.insert((dst, relationship_id), epoch);
            }
            self.ensure_query_index_candidates(
                "cypher_source_relationship_tombstones",
                tombstones.len(),
            )?;
        }
        Ok(tombstones)
    }

    #[cfg(feature = "opencypher")]
    async fn relationships_for_edge_property_at(
        &self,
        lookup: RelationshipPropertyLookup<'_>,
    ) -> Result<Vec<(BoundRelationship, EdgeMetadata)>> {
        let RelationshipPropertyLookup {
            cell_id,
            edge_type,
            src,
            dst,
            property,
            value,
            read_epoch,
            budget,
        } = lookup;
        validate_component("cell_id", cell_id)?;
        validate_component("edge_type", edge_type)?;
        validate_component("property", property)?;
        let mut rows = Vec::new();
        let mut seen_relationship_ids = BTreeSet::new();
        let current_epoch = self.current_epoch(cell_id).await?;
        let latest_snapshot = read_epoch == current_epoch;
        let structural_exists = if latest_snapshot {
            self.edge_exists(cell_id, edge_type, src, dst).await?
        } else {
            self.edge_exists_at(cell_id, edge_type, src, dst, read_epoch)
                .await?
        };
        if !structural_exists {
            return Ok(Vec::new());
        }
        let tombstones = self
            .relationship_tombstone_epochs_for_edge_at(
                cell_id, edge_type, src, dst, read_epoch, budget,
            )
            .await?;
        for encoded in equivalent_property_index_keys(value) {
            let encoded_rows = self
                .relationships_for_edge_encoded_property_at(EncodedRelationshipPropertyLookup {
                    cell_id,
                    edge_type,
                    src,
                    dst,
                    property,
                    encoded: &encoded,
                    read_epoch,
                    latest_snapshot,
                    tombstones: &tombstones,
                    budget,
                })
                .await?;
            for (relationship, metadata) in encoded_rows {
                let Some(relationship_id) = relationship.relationship_id else {
                    continue;
                };
                if !seen_relationship_ids.insert(relationship_id) {
                    continue;
                }
                rows.push((relationship, metadata));
                self.ensure_query_index_candidates(
                    "cypher_relationship_property_edge_candidates",
                    rows.len(),
                )?;
            }
        }

        let structural_metadata = self
            .edge_metadata_at(cell_id, edge_type, src, dst, read_epoch, budget)
            .await?;
        if structural_metadata
            .properties
            .get(property)
            .is_some_and(|existing| vertex_property_values_equal(existing, value))
        {
            rows.push((
                BoundRelationship {
                    edge_type: edge_type.to_string(),
                    src,
                    dst,
                    relationship_id: None,
                },
                structural_metadata,
            ));
        }
        Ok(rows)
    }

    #[cfg(feature = "opencypher")]
    async fn relationships_for_edge_encoded_property_at(
        &self,
        lookup: EncodedRelationshipPropertyLookup<'_>,
    ) -> Result<Vec<(BoundRelationship, EdgeMetadata)>> {
        let EncodedRelationshipPropertyLookup {
            cell_id,
            edge_type,
            src,
            dst,
            property,
            encoded,
            read_epoch,
            latest_snapshot,
            tombstones,
            budget,
        } = lookup;
        let cache_key = RelationshipPropertyRowsCacheKey::new(
            cell_id, edge_type, src, dst, property, encoded, read_epoch,
        );
        if let Some(cached) = self
            .relationship_property_rows_cache
            .lock()
            .await
            .get(&cache_key)
        {
            self.cache_metrics
                .record_hit(GraphCacheKind::RelationshipPropertyRows);
            return Ok(relationship_rows_from_cache_value(
                edge_type, src, dst, cached,
            ));
        }
        self.cache_metrics
            .record_miss(GraphCacheKind::RelationshipPropertyRows);

        let relationship_ids = if latest_snapshot {
            self.scan_relationship_property_index_current_for_edge(
                RelationshipPropertyEdgeIndexLookup {
                    cell_id,
                    edge_type,
                    property,
                    encoded,
                    src,
                    dst,
                    budget,
                },
            )
            .await?
        } else {
            self.scan_relationship_property_index_at(
                cell_id, edge_type, property, encoded, read_epoch, budget,
            )
            .await?
            .into_iter()
            .filter_map(|(candidate_src, candidate_dst, relationship_id)| {
                (candidate_src == src && candidate_dst == dst).then_some(relationship_id)
            })
            .collect()
        };

        let mut rows = Vec::with_capacity(relationship_ids.len());
        for relationship_id in relationship_ids {
            budget.check("cypher_relationship_property_edge_candidates")?;
            let relationship = BoundRelationship {
                edge_type: edge_type.to_string(),
                src,
                dst,
                relationship_id: Some(relationship_id),
            };
            let key = keys::relationship(cell_id, edge_type, src, dst, relationship_id);
            let Some(value) = self.read_remote(&key).await? else {
                continue;
            };
            let record = decode_relationship_record(&key, &value)?;
            if record.epoch > read_epoch {
                continue;
            }
            if tombstones
                .get(&relationship_id)
                .is_some_and(|tombstone_epoch| {
                    relationship_tombstone_hides_record(&record, *tombstone_epoch, read_epoch)
                })
            {
                continue;
            }
            let metadata = if latest_snapshot {
                record.metadata
            } else {
                self.relationship_metadata_at(cell_id, &relationship, read_epoch, budget)
                    .await?
            };
            rows.push((relationship, metadata));
            self.ensure_query_index_candidates(
                "cypher_relationship_property_edge_candidates",
                rows.len(),
            )?;
        }
        self.relationship_property_rows_cache.lock().await.insert(
            cache_key,
            relationship_rows_cache_value(&rows),
            cell_id.to_string(),
            false,
            &self.cache_metrics,
        );
        Ok(rows)
    }

    #[cfg(feature = "opencypher")]
    async fn relationship_count_for_edge_at(
        &self,
        cell_id: &str,
        edge_type: &str,
        src: VertexId,
        dst: VertexId,
        read_epoch: GraphEpoch,
        budget: &QueryBudget,
    ) -> Result<u64> {
        validate_component("cell_id", cell_id)?;
        validate_component("edge_type", edge_type)?;
        let current_epoch = self.current_epoch(cell_id).await?;
        if read_epoch == current_epoch {
            let count = self
                .read_counter(&keys::relationship_count(cell_id, edge_type, src, dst))
                .await?;
            if count > 0 {
                return Ok(count);
            }
        }

        let relationship_records = self
            .relationship_record_count_for_edge_at(cell_id, edge_type, src, dst, read_epoch, budget)
            .await?;
        if relationship_records > 0 {
            return Ok(relationship_records);
        }

        let structural_exists = if read_epoch == current_epoch {
            self.edge_exists(cell_id, edge_type, src, dst).await?
        } else {
            self.edge_exists_at(cell_id, edge_type, src, dst, read_epoch)
                .await?
        };
        Ok(u64::from(structural_exists))
    }

    #[cfg(feature = "opencypher")]
    async fn relationship_record_count_for_edge_at(
        &self,
        cell_id: &str,
        edge_type: &str,
        src: VertexId,
        dst: VertexId,
        read_epoch: GraphEpoch,
        budget: &QueryBudget,
    ) -> Result<u64> {
        let prefix = keys::relationship_edge_prefix(cell_id, edge_type, src, dst);
        let mut iter = self.scan_remote_prefix(&prefix).await?;
        let mut count = 0_u64;
        while let Some(kv) = iter.next().await? {
            budget.check("cypher_relationship_count_records")?;
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            let record = decode_relationship_record(&key, &kv.value)?;
            if record.epoch > read_epoch {
                continue;
            }
            let tombstone_key = keys::relationship_tombstone(
                cell_id,
                &record.edge_type,
                record.src,
                record.dst,
                record.relationship_id,
            );
            if let Some(value) = self.read_remote(&tombstone_key).await? {
                let tombstone_epoch = decode_u64(&tombstone_key, &value)?;
                if relationship_tombstone_hides_record(&record, tombstone_epoch, read_epoch) {
                    continue;
                }
            }
            count = count.saturating_add(1);
        }
        Ok(count)
    }

    #[cfg(feature = "opencypher")]
    async fn scan_edge_property_index_at(
        &self,
        cell_id: &str,
        edge_type: &str,
        property: &str,
        value: &VertexPropertyValue,
        read_epoch: GraphEpoch,
        budget: &QueryBudget,
    ) -> Result<Vec<(VertexId, VertexId)>> {
        validate_component("cell_id", cell_id)?;
        validate_component("edge_type", edge_type)?;
        validate_component("property", property)?;
        let mut edges = BTreeSet::<(VertexId, VertexId)>::new();
        for encoded in equivalent_property_index_keys(value) {
            let mut iter = self
                .scan_remote_prefix(&keys::edge_property_index_delta_prefix(
                    cell_id, edge_type, property, &encoded,
                ))
                .await?;
            let mut latest = BTreeMap::<(VertexId, VertexId), bool>::new();
            let mut saw_delta = false;
            while let Some(kv) = iter.next().await? {
                budget.check("cypher_edge_property_index")?;
                let key = String::from_utf8_lossy(&kv.key).into_owned();
                let (epoch, src, dst) = parse_edge_property_index_delta_key(&key)?;
                saw_delta = true;
                if epoch > read_epoch {
                    break;
                }
                latest.insert((src, dst), decode_vertex_index_delta(&key, &kv.value)?);
                self.ensure_query_index_candidates(
                    "cypher_edge_property_index_candidates",
                    latest.len(),
                )?;
            }
            let encoded_edges = if saw_delta {
                latest
                    .into_iter()
                    .filter_map(|(edge, present)| present.then_some(edge))
                    .collect()
            } else {
                self.scan_edge_property_index_current(
                    cell_id, edge_type, property, &encoded, budget,
                )
                .await?
            };
            for edge in encoded_edges {
                edges.insert(edge);
                self.ensure_query_index_candidates(
                    "cypher_edge_property_index_candidates",
                    edges.len(),
                )?;
            }
            let relationship_edges = self
                .scan_relationship_property_index_at(
                    cell_id, edge_type, property, &encoded, read_epoch, budget,
                )
                .await?;
            for (src, dst, _relationship_id) in relationship_edges {
                edges.insert((src, dst));
                self.ensure_query_index_candidates(
                    "cypher_edge_property_index_candidates",
                    edges.len(),
                )?;
            }
        }
        Ok(edges.into_iter().collect())
    }

    #[cfg(feature = "opencypher")]
    async fn scan_relationship_property_index_at(
        &self,
        cell_id: &str,
        edge_type: &str,
        property: &str,
        encoded: &str,
        read_epoch: GraphEpoch,
        budget: &QueryBudget,
    ) -> Result<Vec<(VertexId, VertexId, RelationshipId)>> {
        let mut iter = self
            .scan_remote_prefix(&keys::relationship_property_index_delta_prefix(
                cell_id, edge_type, property, encoded,
            ))
            .await?;
        let mut latest = BTreeMap::<(VertexId, VertexId, RelationshipId), bool>::new();
        let mut saw_delta = false;
        while let Some(kv) = iter.next().await? {
            budget.check("cypher_relationship_property_index_delta")?;
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            let (epoch, src, dst, relationship_id) =
                parse_relationship_property_index_delta_key(&key)?;
            saw_delta = true;
            if epoch > read_epoch {
                break;
            }
            latest.insert(
                (src, dst, relationship_id),
                decode_vertex_index_delta(&key, &kv.value)?,
            );
            self.ensure_query_index_candidates(
                "cypher_relationship_property_index_candidates",
                latest.len(),
            )?;
        }
        let relationships = if saw_delta {
            latest
                .into_iter()
                .filter_map(|(relationship, present)| present.then_some(relationship))
                .collect()
        } else {
            self.scan_relationship_property_index_current(
                cell_id, edge_type, property, encoded, budget,
            )
            .await?
        };
        Ok(relationships)
    }

    #[cfg(feature = "opencypher")]
    async fn scan_edge_property_index_current(
        &self,
        cell_id: &str,
        edge_type: &str,
        property: &str,
        encoded: &str,
        budget: &QueryBudget,
    ) -> Result<Vec<(VertexId, VertexId)>> {
        let mut iter = self
            .scan_remote_prefix(&keys::edge_property_index_prefix(
                cell_id, edge_type, property, encoded,
            ))
            .await?;
        let mut edges = Vec::new();
        while let Some(kv) = iter.next().await? {
            budget.check("cypher_edge_property_index")?;
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            let (_cell_id, _edge_type, _property, _encoded, src, dst) =
                parse_edge_property_index_key(&key)?;
            edges.push((src, dst));
            self.ensure_query_index_candidates(
                "cypher_edge_property_index_candidates",
                edges.len(),
            )?;
        }
        Ok(edges)
    }

    #[cfg(feature = "opencypher")]
    async fn scan_relationship_property_index_current(
        &self,
        cell_id: &str,
        edge_type: &str,
        property: &str,
        encoded: &str,
        budget: &QueryBudget,
    ) -> Result<Vec<(VertexId, VertexId, RelationshipId)>> {
        let mut iter = self
            .scan_remote_prefix(&keys::relationship_property_index_prefix(
                cell_id, edge_type, property, encoded,
            ))
            .await?;
        let mut relationships = Vec::new();
        while let Some(kv) = iter.next().await? {
            budget.check("cypher_relationship_property_index")?;
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            let (_cell_id, _edge_type, _property, _encoded, src, dst, relationship_id) =
                parse_relationship_property_index_key(&key)?;
            relationships.push((src, dst, relationship_id));
            self.ensure_query_index_candidates(
                "cypher_relationship_property_index_candidates",
                relationships.len(),
            )?;
        }
        Ok(relationships)
    }

    #[cfg(feature = "opencypher")]
    async fn scan_relationship_property_index_current_for_edge(
        &self,
        lookup: RelationshipPropertyEdgeIndexLookup<'_>,
    ) -> Result<Vec<RelationshipId>> {
        let RelationshipPropertyEdgeIndexLookup {
            cell_id,
            edge_type,
            property,
            encoded,
            src,
            dst,
            budget,
        } = lookup;
        let mut iter = self
            .scan_remote_prefix(&keys::relationship_property_index_edge_prefix(
                cell_id, edge_type, property, encoded, src, dst,
            ))
            .await?;
        let mut relationships = Vec::new();
        while let Some(kv) = iter.next().await? {
            budget.check("cypher_relationship_property_index_edge")?;
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            let (
                _cell_id,
                _edge_type,
                _property,
                _encoded,
                parsed_src,
                parsed_dst,
                relationship_id,
            ) = parse_relationship_property_index_key(&key)?;
            if parsed_src != src || parsed_dst != dst {
                return Err(GraphError::CorruptValue {
                    key,
                    reason: "relationship property index edge prefix returned a different endpoint"
                        .to_string(),
                });
            }
            relationships.push(relationship_id);
            self.ensure_query_index_candidates(
                "cypher_relationship_property_index_edge_candidates",
                relationships.len(),
            )?;
        }
        Ok(relationships)
    }

    #[cfg(feature = "opencypher")]
    async fn scan_vertex_label_index_at(
        &self,
        cell_id: &str,
        label: &str,
        read_epoch: GraphEpoch,
        budget: &QueryBudget,
    ) -> Result<Vec<VertexId>> {
        validate_component("cell_id", cell_id)?;
        validate_component("label", label)?;
        let mut iter = self
            .scan_remote_prefix(&keys::vertex_label_delta_prefix(cell_id, label))
            .await?;
        let mut latest = BTreeMap::<VertexId, bool>::new();
        let mut saw_delta = false;
        while let Some(kv) = iter.next().await? {
            budget.check("cypher_vertex_label_index")?;
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            let (epoch, vertex_id) = parse_vertex_label_delta_key(&key)?;
            saw_delta = true;
            if epoch > read_epoch {
                break;
            }
            latest.insert(vertex_id, decode_vertex_index_delta(&key, &kv.value)?);
            self.ensure_query_index_candidates(
                "cypher_vertex_label_index_candidates",
                latest.len(),
            )?;
        }
        if saw_delta {
            return Ok(latest
                .into_iter()
                .filter_map(|(vertex_id, present)| present.then_some(vertex_id))
                .collect());
        }
        self.scan_vertex_label_index_current(cell_id, label, budget)
            .await
    }

    #[cfg(feature = "opencypher")]
    async fn scan_vertex_label_index_current(
        &self,
        cell_id: &str,
        label: &str,
        budget: &QueryBudget,
    ) -> Result<Vec<VertexId>> {
        let mut iter = self
            .scan_remote_prefix(&keys::vertex_label_prefix(cell_id, label))
            .await?;
        let mut vertices = Vec::new();
        while let Some(kv) = iter.next().await? {
            budget.check("cypher_vertex_label_index")?;
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            vertices.push(decode_u64(&key, &kv.value)?);
            self.ensure_query_index_candidates(
                "cypher_vertex_label_index_candidates",
                vertices.len(),
            )?;
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
        budget: &QueryBudget,
    ) -> Result<Vec<VertexId>> {
        validate_component("cell_id", cell_id)?;
        validate_component("property", property)?;
        let mut vertices = BTreeSet::<VertexId>::new();
        for encoded in equivalent_property_index_keys(value) {
            let mut iter = self
                .scan_remote_prefix(&keys::vertex_property_index_delta_prefix(
                    cell_id, property, &encoded,
                ))
                .await?;
            let mut latest = BTreeMap::<VertexId, bool>::new();
            let mut saw_delta = false;
            while let Some(kv) = iter.next().await? {
                budget.check("cypher_vertex_property_index")?;
                let key = String::from_utf8_lossy(&kv.key).into_owned();
                let (epoch, vertex_id) = parse_vertex_property_index_delta_key(&key)?;
                saw_delta = true;
                if epoch > read_epoch {
                    break;
                }
                latest.insert(vertex_id, decode_vertex_index_delta(&key, &kv.value)?);
                self.ensure_query_index_candidates(
                    "cypher_vertex_property_index_candidates",
                    latest.len(),
                )?;
            }
            let encoded_vertices = if saw_delta {
                latest
                    .into_iter()
                    .filter_map(|(vertex_id, present)| present.then_some(vertex_id))
                    .collect()
            } else {
                self.scan_vertex_property_index_current(cell_id, property, &encoded, budget)
                    .await?
            };
            for vertex_id in encoded_vertices {
                vertices.insert(vertex_id);
                self.ensure_query_index_candidates(
                    "cypher_vertex_property_index_candidates",
                    vertices.len(),
                )?;
            }
        }
        Ok(vertices.into_iter().collect())
    }

    #[cfg(feature = "opencypher")]
    async fn scan_vertex_property_index_current(
        &self,
        cell_id: &str,
        property: &str,
        encoded: &str,
        budget: &QueryBudget,
    ) -> Result<Vec<VertexId>> {
        let mut iter = self
            .scan_remote_prefix(&keys::vertex_property_index_prefix(
                cell_id, property, encoded,
            ))
            .await?;
        let mut vertices = Vec::new();
        while let Some(kv) = iter.next().await? {
            budget.check("cypher_vertex_property_index")?;
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            vertices.push(decode_u64(&key, &kv.value)?);
            self.ensure_query_index_candidates(
                "cypher_vertex_property_index_candidates",
                vertices.len(),
            )?;
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
        budget: &QueryBudget,
    ) -> Result<()> {
        let bindings: Vec<_> = row
            .values
            .iter()
            .map(|(name, id)| (name.clone(), *id))
            .collect();
        for (binding, vertex_id) in bindings {
            budget.check("cypher_metadata_hydration")?;
            let metadata = match cache.get(&vertex_id) {
                Some(metadata) => metadata.clone(),
                None => {
                    let metadata = self
                        .vertex_metadata_at(cell_id, vertex_id, read_epoch, budget)
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
    async fn hydrate_row_relationship_metadata(
        &self,
        cell_id: &str,
        read_epoch: GraphEpoch,
        row: &mut BindingRow,
        pattern: &RowEdgePattern,
        cache: &mut BTreeMap<BoundRelationship, EdgeMetadata>,
        budget: &QueryBudget,
    ) -> Result<()> {
        if pattern.binding.is_none() && pattern.properties.is_empty() {
            return Ok(());
        }
        let relationship = relationship_identity_for_pattern(row, pattern)?;
        budget.check("cypher_relationship_metadata_hydration")?;
        let metadata = match cache.get(&relationship) {
            Some(metadata) => metadata.clone(),
            None => {
                let metadata = match row.relationship_metadata.get(&relationship) {
                    Some(metadata) => metadata.clone(),
                    None => {
                        self.relationship_metadata_at(cell_id, &relationship, read_epoch, budget)
                            .await?
                    }
                };
                cache.insert(relationship.clone(), metadata.clone());
                metadata
            }
        };
        row.relationship_metadata.insert(relationship, metadata);
        Ok(())
    }

    #[cfg(feature = "opencypher")]
    fn finish_projected_rows(
        &self,
        columns: Vec<QueryColumn>,
        mut projected: Vec<ProjectedQueryRow>,
        order_by: &[RowSort],
        window: QueryWindow,
        budget: &QueryBudget,
    ) -> Result<QueryResultSet> {
        budget.check("cypher_finish_rows")?;
        if !order_by.is_empty() {
            projected.sort_by(|left, right| compare_projected_rows(left, right, order_by));
        }
        budget.check("cypher_sort_rows")?;

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
        budget: Option<&QueryBudget>,
    ) -> Result<Vec<VertexId>> {
        check_optional_query_budget(budget, "query_out_neighbors_window")?;
        let fetch_limit = self.query_window_fetch_limit(window)?;
        if let Some(vertices) = self
            .out_supernode_window(cell_id, edge_type, src, read_epoch, window, fetch_limit)
            .await?
        {
            check_optional_query_budget(budget, "query_out_supernode_window")?;
            return self.apply_query_window_fetch_result(vertices, window);
        }

        let vertices = match budget {
            Some(budget) => {
                self.out_neighbors_at_for_query(cell_id, edge_type, src, read_epoch, budget)
                    .await?
            }
            None => {
                self.out_neighbors_at(cell_id, edge_type, src, read_epoch)
                    .await?
            }
        };
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

    #[cfg(feature = "opencypher")]
    fn apply_query_row_window(
        &self,
        rows: Vec<QueryRow>,
        window: QueryWindow,
    ) -> Result<Vec<QueryRow>> {
        let skip = usize::try_from(window.skip).map_err(|_| GraphError::AdmissionRejected {
            operation: "query_result_skip",
            actual: window.skip,
            limit: usize::MAX as u64,
        })?;
        let mut rows: Vec<_> = rows.into_iter().skip(skip).collect();
        let max = self.limits.max_query_result_vertices;
        if let Some(limit) = window.limit {
            ensure_limit("query_result_limit", limit as u64, max as u64)?;
            rows.truncate(limit);
        } else {
            ensure_limit("query_result_rows", rows.len() as u64, max as u64)?;
        }
        Ok(rows)
    }

    #[cfg(feature = "opencypher")]
    async fn try_execute_graph_kernel_opencypher_rows_page(
        &self,
        context: &QueryContext,
        query: &ParsedRowQuery,
        cursor_offset: u64,
        page_size: usize,
    ) -> Result<Option<QueryResultPage>> {
        let Some(request) = graph_kernel_row_query_request(query) else {
            return Ok(None);
        };
        let page_context = self.query_page_context(context.clone(), cursor_offset, page_size)?;
        let page_window = page_context.result_window;
        if page_window.limit == Some(0) {
            return Ok(Some(QueryResultPage::new(
                query.columns.clone(),
                Vec::new(),
                None,
            )));
        }

        let read_epoch = self.query_read_epoch(context).await?;
        let budget = QueryBudget::new(
            context.max_runtime_ms.or(self.limits.max_query_runtime_ms),
            context.cancellation_token.clone(),
        );
        budget.check("cypher_graph_kernel_page")?;
        if request.edge.dst.id.is_none() {
            match request.projection {
                GraphKernelProjection::NodeId => {
                    let (mut vertices, edge_visits) = self
                        .reachable_vertices_window_in_hop_range_at(ReachableWindowRequest {
                            cell_id: &context.cell_id,
                            edge_type: &request.edge.edge_type,
                            src: request.src,
                            hop_range: request.hop_range,
                            read_epoch,
                            window: page_window,
                            ascending: request.ascending,
                            budget: &budget,
                        })
                        .await?;
                    self.ensure_query_scan_edges(
                        "cypher_graph_kernel_page_window_edge_visits",
                        edge_visits,
                    )?;
                    let has_next = vertices.len() > page_size;
                    vertices.truncate(page_size);
                    let rows = graph_kernel_node_id_rows(vertices, &budget)?;
                    return Ok(Some(QueryResultPage::new(
                        query.columns.clone(),
                        rows,
                        query_next_cursor(cursor_offset, page_size, has_next)?,
                    )));
                }
                GraphKernelProjection::CountAll => {
                    let (count, edge_visits) = self
                        .reachable_count_in_hop_range_at(
                            &context.cell_id,
                            &request.edge.edge_type,
                            request.src,
                            request.hop_range,
                            read_epoch,
                            &budget,
                        )
                        .await?;
                    self.ensure_query_scan_edges(
                        "cypher_graph_kernel_page_count_edge_visits",
                        edge_visits,
                    )?;
                    let rows = vec![QueryRow::new(vec![QueryValue::Count(count)])];
                    let mut rows = self.apply_query_row_window(rows, page_window)?;
                    let has_next = rows.len() > page_size;
                    rows.truncate(page_size);
                    return Ok(Some(QueryResultPage::new(
                        query.columns.clone(),
                        rows,
                        query_next_cursor(cursor_offset, page_size, has_next)?,
                    )));
                }
            }
        }

        let (mut vertices, edge_visits) = self
            .reachable_vertices_in_hop_range_at(
                &context.cell_id,
                &request.edge.edge_type,
                request.src,
                request.hop_range,
                read_epoch,
                &budget,
            )
            .await?;
        self.ensure_query_scan_edges("cypher_graph_kernel_page_edge_visits", edge_visits)?;
        if let Some(dst) = request.edge.dst.id {
            vertices.retain(|vertex| *vertex == dst);
        }
        graph_kernel_order_vertices(&mut vertices, request.ascending);

        let rows = match request.projection {
            GraphKernelProjection::NodeId => {
                let mut vertices = self.apply_query_window(vertices, page_window)?;
                let has_next = vertices.len() > page_size;
                vertices.truncate(page_size);
                let mut rows = Vec::with_capacity(vertices.len());
                for vertex in vertices {
                    budget.check("cypher_graph_kernel_page_project")?;
                    rows.push(QueryRow::new(vec![QueryValue::VertexId(vertex)]));
                }
                return Ok(Some(QueryResultPage::new(
                    query.columns.clone(),
                    rows,
                    query_next_cursor(cursor_offset, page_size, has_next)?,
                )));
            }
            GraphKernelProjection::CountAll => {
                let rows = vec![QueryRow::new(vec![
                    QueryValue::Count(vertices.len() as u64),
                ])];
                self.apply_query_row_window(rows, page_window)?
            }
        };
        let has_next = rows.len() > page_size;
        let mut rows = rows;
        rows.truncate(page_size);
        Ok(Some(QueryResultPage::new(
            query.columns.clone(),
            rows,
            query_next_cursor(cursor_offset, page_size, has_next)?,
        )))
    }

    #[cfg(feature = "opencypher")]
    async fn try_execute_streaming_opencypher_rows_page(
        &self,
        context: &QueryContext,
        query: &ParsedRowQuery,
        cursor_offset: u64,
        page_size: usize,
    ) -> Result<Option<QueryResultPage>> {
        let Some(edge) = streaming_neighbor_page_edge(query) else {
            return Ok(None);
        };
        if !streaming_neighbor_order_supported(
            edge,
            &query.projections,
            &query.columns,
            &query.order_by,
        ) {
            return Ok(None);
        }

        let page_context = self.query_page_context(context.clone(), cursor_offset, page_size)?;
        let page_window = page_context.result_window;
        if page_window.limit == Some(0) {
            return Ok(Some(QueryResultPage::new(
                query.columns.clone(),
                Vec::new(),
                None,
            )));
        }

        let read_epoch = self.query_read_epoch(context).await?;
        let budget = QueryBudget::new(
            context.max_runtime_ms.or(self.limits.max_query_runtime_ms),
            context.cancellation_token.clone(),
        );
        budget.check("cypher_rows_page_stream")?;
        let Some(src) = edge.src.id else {
            return Ok(None);
        };
        let mut vertices = self
            .out_neighbors_window_at(
                &context.cell_id,
                &edge.edge_type,
                src,
                read_epoch,
                page_window,
                Some(&budget),
            )
            .await?;
        let has_next = vertices.len() > page_size;
        vertices.truncate(page_size);

        let mut rows = Vec::with_capacity(vertices.len());
        for dst in vertices {
            budget.check("cypher_rows_page_stream_project")?;
            rows.push(QueryRow::new(streaming_neighbor_projection_values(
                edge,
                src,
                dst,
                &query.projections,
            )?));
        }
        let next_cursor = if has_next {
            query_next_cursor(cursor_offset, page_size, true)?
        } else {
            None
        };
        Ok(Some(QueryResultPage::new(
            query.columns.clone(),
            rows,
            next_cursor,
        )))
    }

    #[cfg(feature = "opencypher")]
    fn record_streaming_query_rows_success(&self, row_count: usize, started: std::time::Instant) {
        self.operation_metrics
            .query_rows_started
            .fetch_add(1, Ordering::Relaxed);
        self.operation_metrics
            .query_rows_completed
            .fetch_add(1, Ordering::Relaxed);
        self.operation_metrics
            .query_rows_returned
            .fetch_add(row_count as u64, Ordering::Relaxed);
        let elapsed_us = started.elapsed().as_micros().try_into().unwrap_or(u64::MAX);
        self.operation_metrics
            .query_rows_duration_us
            .fetch_add(elapsed_us, Ordering::Relaxed);
    }

    #[cfg(feature = "opencypher")]
    fn record_streaming_query_rows_failure(&self, started: std::time::Instant) {
        self.operation_metrics
            .query_rows_started
            .fetch_add(1, Ordering::Relaxed);
        self.operation_metrics
            .query_rows_failed
            .fetch_add(1, Ordering::Relaxed);
        let elapsed_us = started.elapsed().as_micros().try_into().unwrap_or(u64::MAX);
        self.operation_metrics
            .query_rows_duration_us
            .fetch_add(elapsed_us, Ordering::Relaxed);
    }

    #[cfg(feature = "opencypher")]
    fn query_page_context(
        &self,
        context: QueryContext,
        cursor_offset: u64,
        page_size: usize,
    ) -> Result<QueryContext> {
        let max = self.limits.max_query_result_vertices;
        if page_size == 0 {
            return Err(GraphError::AdmissionRejected {
                operation: "query_page_size",
                actual: 0,
                limit: max as u64,
            });
        }
        let max_page_size = max.saturating_sub(1);
        ensure_limit("query_page_size", page_size as u64, max_page_size as u64)?;

        let base_window = context.result_window;
        let skip =
            base_window
                .skip
                .checked_add(cursor_offset)
                .ok_or(GraphError::AdmissionRejected {
                    operation: "query_cursor_offset",
                    actual: u64::MAX,
                    limit: u64::MAX - 1,
                })?;
        let probe_limit = match base_window.limit {
            Some(limit) => {
                let limit_u64 = u64::try_from(limit).unwrap_or(u64::MAX);
                if cursor_offset >= limit_u64 {
                    0
                } else {
                    let remaining = limit_u64 - cursor_offset;
                    usize::try_from(remaining)
                        .unwrap_or(usize::MAX)
                        .min(page_size.saturating_add(1))
                }
            }
            None => page_size.saturating_add(1),
        };
        Ok(context.with_result_window(skip, Some(probe_limit)))
    }

    #[cfg(feature = "opencypher")]
    fn ensure_query_intermediate_rows(&self, operation: &'static str, rows: usize) -> Result<()> {
        ensure_limit(
            operation,
            rows as u64,
            self.limits.max_query_intermediate_rows as u64,
        )
    }

    #[cfg(feature = "opencypher")]
    fn ensure_query_index_candidates(
        &self,
        operation: &'static str,
        candidates: usize,
    ) -> Result<()> {
        ensure_limit(
            operation,
            candidates as u64,
            self.limits.max_query_index_candidates as u64,
        )
    }

    #[cfg(feature = "opencypher")]
    fn ensure_query_scan_edges(&self, operation: &'static str, edges: u64) -> Result<()> {
        ensure_limit(operation, edges, self.limits.max_query_scan_edges)
    }

    #[cfg(feature = "opencypher")]
    fn push_binding_row(
        &self,
        rows: &mut Vec<BindingRow>,
        row: BindingRow,
        operation: &'static str,
    ) -> Result<()> {
        let next_len = rows
            .len()
            .checked_add(1)
            .ok_or_else(|| GraphError::AdmissionRejected {
                operation,
                actual: u64::MAX,
                limit: self.limits.max_query_intermediate_rows as u64,
            })?;
        self.ensure_query_intermediate_rows(operation, next_len)?;
        rows.push(row);
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

    fn validate_reachable_hop_request(
        &self,
        operation: &'static str,
        cell_id: &str,
        edge_type: &str,
        hop_range: (u8, u8),
    ) -> Result<(u8, u8)> {
        validate_component("cell_id", cell_id)?;
        validate_component("edge_type", edge_type)?;
        let (min_hops, max_hops) = hop_range;
        if min_hops > max_hops {
            return Err(GraphError::UnsupportedQuery {
                dialect: "OpenCypher",
                feature: "invalid variable-length hop range".to_string(),
            });
        }
        ensure_limit(
            operation,
            u64::from(max_hops),
            u64::from(self.limits.max_traversal_hops),
        )?;
        Ok((min_hops, max_hops))
    }

    #[cfg(feature = "opencypher")]
    async fn cached_reachability_count(&self, key: &ReachabilityCacheKey) -> Option<(u64, u64)> {
        let value = self.reachability_cache.lock().await.get(key);
        if let Some(value) = value {
            self.cache_metrics
                .record_hit(GraphCacheKind::ReachabilityResult);
            return Some((value.count(), value.edge_visits()));
        }
        self.cache_metrics
            .record_miss(GraphCacheKind::ReachabilityResult);
        None
    }

    #[cfg(feature = "opencypher")]
    async fn cached_reachability_vertices(
        &self,
        key: &ReachabilityCacheKey,
    ) -> Option<(Vec<VertexId>, u64)> {
        let value = self.reachability_cache.lock().await.get(key);
        if let Some(value) = value.and_then(|value| {
            value
                .vertices()
                .map(|vertices| (vertices, value.edge_visits()))
        }) {
            self.cache_metrics
                .record_hit(GraphCacheKind::ReachabilityResult);
            return Some((value.0.as_ref().clone(), value.1));
        }
        self.cache_metrics
            .record_miss(GraphCacheKind::ReachabilityResult);
        None
    }

    #[cfg(feature = "opencypher")]
    async fn cache_reachability_result(
        &self,
        key: ReachabilityCacheKey,
        value: ReachabilityCacheValue,
    ) {
        let tenant = key.cell_id().to_string();
        self.reachability_cache
            .lock()
            .await
            .insert(key, value, tenant, false, &self.cache_metrics);
    }

    async fn reachable_vertices_in_hop_range_at(
        &self,
        cell_id: &str,
        edge_type: &str,
        src: VertexId,
        hop_range: (u8, u8),
        read_epoch: GraphEpoch,
        budget: &QueryBudget,
    ) -> Result<(Vec<VertexId>, u64)> {
        budget.check("cypher_match_reachable")?;
        let (min_hops, max_hops) = self.validate_reachable_hop_request(
            "cypher_match_reachable",
            cell_id,
            edge_type,
            hop_range,
        )?;
        #[cfg(feature = "opencypher")]
        let cache_key =
            ReachabilityCacheKey::new(cell_id, edge_type, src, (min_hops, max_hops), read_epoch);
        #[cfg(feature = "opencypher")]
        if let Some(result) = self.cached_reachability_vertices(&cache_key).await {
            return Ok(result);
        }

        if let Some(result) = self
            .reachable_vertices_with_compiled_graph_kernel(
                cell_id, edge_type, src, hop_range, read_epoch, budget,
            )
            .await?
        {
            #[cfg(feature = "opencypher")]
            self.cache_reachability_result(
                cache_key,
                ReachabilityCacheValue::from_vertices(result.0.clone(), result.1),
            )
            .await;
            return Ok(result);
        }

        let (adjacency, delta_records_applied) = self
            .reachable_query_adjacency_at(cell_id, edge_type, read_epoch, budget)
            .await?;
        let traversal = crate::sparse_kernel::expand_range(
            &adjacency,
            &[src],
            min_hops,
            max_hops,
            SparseKernelBackend::RustSparse,
        )?;
        let result = (
            traversal.vertices,
            traversal.edge_visits.saturating_add(delta_records_applied),
        );
        #[cfg(feature = "opencypher")]
        self.cache_reachability_result(
            cache_key,
            ReachabilityCacheValue::from_vertices(result.0.clone(), result.1),
        )
        .await;
        Ok(result)
    }

    #[cfg(feature = "opencypher")]
    async fn reachable_count_in_hop_range_at(
        &self,
        cell_id: &str,
        edge_type: &str,
        src: VertexId,
        hop_range: (u8, u8),
        read_epoch: GraphEpoch,
        budget: &QueryBudget,
    ) -> Result<(u64, u64)> {
        budget.check("cypher_match_reachable_count")?;
        let hop_range = self.validate_reachable_hop_request(
            "cypher_match_reachable_count",
            cell_id,
            edge_type,
            hop_range,
        )?;
        let cache_key = ReachabilityCacheKey::new(cell_id, edge_type, src, hop_range, read_epoch);
        if let Some(result) = self.cached_reachability_count(&cache_key).await {
            return Ok(result);
        }
        if let Some(result) = self
            .reachable_count_with_compiled_graph_kernel(
                cell_id, edge_type, src, hop_range, read_epoch, budget,
            )
            .await?
        {
            self.cache_reachability_result(
                cache_key,
                ReachabilityCacheValue::count_only(result.0, result.1),
            )
            .await;
            return Ok(result);
        }
        let (vertices, edge_visits) = self
            .reachable_vertices_in_hop_range_at(
                cell_id, edge_type, src, hop_range, read_epoch, budget,
            )
            .await?;
        Ok((vertices.len() as u64, edge_visits))
    }

    #[cfg(feature = "opencypher")]
    async fn reachable_vertices_window_in_hop_range_at(
        &self,
        request: ReachableWindowRequest<'_>,
    ) -> Result<(Vec<VertexId>, u64)> {
        validate_query_result_window(request.window, self.limits.max_query_result_vertices)?;
        self.validate_reachable_hop_request(
            "cypher_match_reachable_window",
            request.cell_id,
            request.edge_type,
            request.hop_range,
        )?;
        let cache_key = ReachabilityCacheKey::new(
            request.cell_id,
            request.edge_type,
            request.src,
            request.hop_range,
            request.read_epoch,
        );
        if let Some((mut vertices, edge_visits)) =
            self.cached_reachability_vertices(&cache_key).await
        {
            graph_kernel_order_vertices(&mut vertices, request.ascending);
            return Ok((
                self.apply_query_window(vertices, request.window)?,
                edge_visits,
            ));
        }
        let window_cache_key = ReachabilityCacheKey::new_window(
            request.cell_id,
            request.edge_type,
            request.src,
            request.hop_range,
            request.read_epoch,
            request.window,
            request.ascending,
        );
        if let Some(result) = self.cached_reachability_vertices(&window_cache_key).await {
            return Ok(result);
        }
        if let Some(result) = self
            .reachable_vertices_window_with_compiled_graph_kernel(&request)
            .await?
        {
            self.cache_reachability_result(
                window_cache_key,
                ReachabilityCacheValue::from_vertices(result.0.clone(), result.1),
            )
            .await;
            return Ok(result);
        }
        let (mut vertices, edge_visits) = self
            .reachable_vertices_in_hop_range_at(
                request.cell_id,
                request.edge_type,
                request.src,
                request.hop_range,
                request.read_epoch,
                request.budget,
            )
            .await?;
        graph_kernel_order_vertices(&mut vertices, request.ascending);
        Ok((
            self.apply_query_window(vertices, request.window)?,
            edge_visits,
        ))
    }

    async fn reachable_vertices_with_compiled_graph_kernel(
        &self,
        cell_id: &str,
        edge_type: &str,
        src: VertexId,
        hop_range: (u8, u8),
        read_epoch: GraphEpoch,
        budget: &QueryBudget,
    ) -> Result<Option<(Vec<VertexId>, u64)>> {
        let _ = (cell_id, edge_type, src, hop_range, read_epoch, budget);
        #[cfg(feature = "graphblas")]
        {
            let (min_hops, max_hops) = hop_range;
            budget.check("cypher_graphblas_artifact_lookup")?;
            let Some(artifact) = self
                .latest_matrix_artifact(cell_id, edge_type, read_epoch)
                .await?
            else {
                return Ok(None);
            };
            if artifact.base_epoch != read_epoch {
                return Ok(None);
            }
            budget.check("cypher_graphblas_compiled_matrix")?;
            let compiled = self
                .cached_graphblas_matrix(cell_id, edge_type, artifact.base_epoch)
                .await?;
            let empty_adjacency = BTreeMap::new();
            let traversal = crate::sparse_kernel::expand_range_compiled_graphblas(
                &compiled,
                &empty_adjacency,
                &[src],
                min_hops,
                max_hops,
            )?;
            Ok(Some((traversal.vertices, traversal.edge_visits)))
        }
        #[cfg(not(feature = "graphblas"))]
        {
            Ok(None)
        }
    }

    #[cfg(feature = "opencypher")]
    async fn reachable_count_with_compiled_graph_kernel(
        &self,
        cell_id: &str,
        edge_type: &str,
        src: VertexId,
        hop_range: (u8, u8),
        read_epoch: GraphEpoch,
        budget: &QueryBudget,
    ) -> Result<Option<(u64, u64)>> {
        let _ = (cell_id, edge_type, src, hop_range, read_epoch, budget);
        #[cfg(feature = "graphblas")]
        {
            let (min_hops, max_hops) = hop_range;
            budget.check("cypher_graphblas_count_artifact_lookup")?;
            let Some(artifact) = self
                .latest_matrix_artifact(cell_id, edge_type, read_epoch)
                .await?
            else {
                return Ok(None);
            };
            if artifact.base_epoch != read_epoch {
                return Ok(None);
            }
            budget.check("cypher_graphblas_count_compiled_matrix")?;
            let compiled = self
                .cached_graphblas_matrix(cell_id, edge_type, artifact.base_epoch)
                .await?;
            let empty_adjacency = BTreeMap::new();
            let traversal = crate::sparse_kernel::expand_range_count_compiled_graphblas(
                &compiled,
                &empty_adjacency,
                &[src],
                min_hops,
                max_hops,
            )?;
            Ok(Some((traversal.vertices, traversal.edge_visits)))
        }
        #[cfg(not(feature = "graphblas"))]
        {
            Ok(None)
        }
    }

    #[cfg(feature = "opencypher")]
    async fn reachable_vertices_window_with_compiled_graph_kernel(
        &self,
        request: &ReachableWindowRequest<'_>,
    ) -> Result<Option<(Vec<VertexId>, u64)>> {
        let _ = request;
        #[cfg(feature = "graphblas")]
        {
            let (min_hops, max_hops) = request.hop_range;
            request
                .budget
                .check("cypher_graphblas_window_artifact_lookup")?;
            let Some(artifact) = self
                .latest_matrix_artifact(request.cell_id, request.edge_type, request.read_epoch)
                .await?
            else {
                return Ok(None);
            };
            if artifact.base_epoch != request.read_epoch {
                return Ok(None);
            }
            request
                .budget
                .check("cypher_graphblas_window_compiled_matrix")?;
            let compiled = self
                .cached_graphblas_matrix(request.cell_id, request.edge_type, artifact.base_epoch)
                .await?;
            let empty_adjacency = BTreeMap::new();
            let traversal = crate::sparse_kernel::expand_range_window_compiled_graphblas(
                &compiled,
                &empty_adjacency,
                &[request.src],
                min_hops,
                max_hops,
                crate::sparse_kernel::SparseTraversalWindow {
                    skip: request.window.skip,
                    limit: request.window.limit,
                    ascending: request.ascending,
                },
            )?;
            Ok(Some((traversal.vertices, traversal.edge_visits)))
        }
        #[cfg(not(feature = "graphblas"))]
        {
            Ok(None)
        }
    }

    async fn reachable_query_adjacency_at(
        &self,
        cell_id: &str,
        edge_type: &str,
        read_epoch: GraphEpoch,
        budget: &QueryBudget,
    ) -> Result<(BTreeMap<VertexId, BTreeSet<VertexId>>, u64)> {
        budget.check("cypher_reachable_artifact_lookup")?;
        let artifact = self
            .latest_matrix_artifact(cell_id, edge_type, read_epoch)
            .await?;
        let base_epoch = artifact.as_ref().map_or(0, |artifact| artifact.base_epoch);

        budget.check("cypher_reachable_adjacency_hydrate")?;
        let mut adjacency = if artifact.is_some() {
            self.cached_matrix_adjacency(cell_id, edge_type, base_epoch)
                .await?
                .as_ref()
                .clone()
        } else {
            BTreeMap::new()
        };
        budget.check("cypher_reachable_delta_overlay")?;

        let deltas = if read_epoch <= base_epoch {
            Vec::new()
        } else {
            self.deltas_between(cell_id, edge_type, base_epoch, read_epoch)
                .await?
        };
        let applied =
            crate::engine::apply_delta_overlay(&mut adjacency, deltas, base_epoch, read_epoch);
        Ok((adjacency, applied))
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
        self.ensure_cell_readable(cell_id, "edge_exists").await?;
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
        self.ensure_cell_readable(cell_id, "out_neighbors").await?;
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
            .scan_out_segment_tombstones_for_src_at(cell_id, edge_type, src, read_epoch, None)
            .await?;
        neighbors.extend(
            self.scan_out_segments_for_src_at(cell_id, edge_type, src, read_epoch, None)
                .await?
                .into_iter()
                .filter(|edge| segment_edge_visible(edge.epoch, tombstones.get(&edge.dst).copied()))
                .map(|edge| edge.dst),
        );
        neighbors.sort_unstable();
        neighbors.dedup();
        Ok(neighbors)
    }

    async fn out_neighbors_at_for_query(
        &self,
        cell_id: &str,
        edge_type: &str,
        src: VertexId,
        read_epoch: GraphEpoch,
        budget: &QueryBudget,
    ) -> Result<Vec<VertexId>> {
        validate_component("cell_id", cell_id)?;
        validate_component("edge_type", edge_type)?;
        let mut neighbors = Vec::new();
        for edge in self
            .edges_at_with_budget(cell_id, edge_type, read_epoch, Some(budget))
            .await?
        {
            budget.check("query_out_neighbors_scan")?;
            if edge.src == src {
                neighbors.push(edge.dst);
            }
        }
        neighbors.sort_unstable();
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
            .scan_out_segments_for_src_at(cell_id, edge_type, src, read_epoch, None)
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
        budget: Option<&QueryBudget>,
    ) -> Result<Vec<EdgeRecord>> {
        let prefix = keys::out_segment_src_prefix(cell_id, edge_type, src);
        let mut iter = self.scan_remote_prefix(&prefix).await?;
        let mut edges = Vec::new();
        while let Some(kv) = iter.next().await? {
            check_optional_query_budget(budget, "query_out_segment_scan")?;
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            let segment = decode_out_edge_segment(&key, &kv.value)?;
            if segment.start_epoch > read_epoch {
                break;
            }
            for (epoch, dst) in segment.edges.iter().copied() {
                check_optional_query_budget(budget, "query_out_segment_edge_scan")?;
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
        budget: Option<&QueryBudget>,
    ) -> Result<BTreeMap<VertexId, GraphEpoch>> {
        let prefix = keys::out_segment_tombstone_src_prefix(cell_id, edge_type, src);
        let mut iter = self.scan_remote_prefix(&prefix).await?;
        let mut tombstones = BTreeMap::new();
        while let Some(kv) = iter.next().await? {
            check_optional_query_budget(budget, "query_out_segment_tombstone_scan")?;
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
        budget: Option<&QueryBudget>,
    ) -> Result<BTreeMap<(VertexId, VertexId), GraphEpoch>> {
        let prefix = keys::out_segment_tombstone_edge_type_prefix(cell_id, edge_type);
        let mut iter = self.scan_remote_prefix(&prefix).await?;
        let mut tombstones = BTreeMap::new();
        while let Some(kv) = iter.next().await? {
            check_optional_query_budget(budget, "query_out_segment_tombstone_scan")?;
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
            .out_segment_tombstones_at(cell_id, edge_type, read_epoch, None)
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
        let read_epoch = self.current_epoch(cell_id).await?;
        self.in_neighbors_at(cell_id, edge_type, dst, read_epoch)
            .await
    }

    pub async fn in_neighbors_at(
        &self,
        cell_id: &str,
        edge_type: &str,
        dst: VertexId,
        read_epoch: GraphEpoch,
    ) -> Result<Vec<VertexId>> {
        validate_component("cell_id", cell_id)?;
        validate_component("edge_type", edge_type)?;
        let current_epoch = self.current_epoch(cell_id).await?;
        if !self.writes_reverse_index() || read_epoch != current_epoch {
            let mut neighbors: Vec<_> = self
                .edges_at(cell_id, edge_type, read_epoch)
                .await?
                .into_iter()
                .filter_map(|edge| (edge.dst == dst).then_some(edge.src))
                .collect();
            neighbors.sort_unstable();
            neighbors.dedup();
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
        neighbors.dedup();
        Ok(neighbors)
    }

    #[cfg(feature = "opencypher")]
    async fn in_neighbors_at_for_query(
        &self,
        cell_id: &str,
        edge_type: &str,
        dst: VertexId,
        read_epoch: GraphEpoch,
        budget: &QueryBudget,
    ) -> Result<Vec<VertexId>> {
        validate_component("cell_id", cell_id)?;
        validate_component("edge_type", edge_type)?;
        let current_epoch = self.current_epoch(cell_id).await?;
        if !self.writes_reverse_index() || read_epoch != current_epoch {
            let mut neighbors = Vec::new();
            for edge in self
                .edges_at_with_budget(cell_id, edge_type, read_epoch, Some(budget))
                .await?
            {
                budget.check("query_in_neighbors_full_scan")?;
                if edge.dst == dst {
                    neighbors.push(edge.src);
                }
            }
            neighbors.sort_unstable();
            neighbors.dedup();
            return Ok(neighbors);
        }
        let prefix = keys::in_prefix(cell_id, edge_type, dst);
        let mut iter = self.scan_remote_prefix(&prefix).await?;
        let mut neighbors = Vec::new();
        while let Some(kv) = iter.next().await? {
            budget.check("query_in_neighbors_reverse_scan")?;
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            let record = decode_edge_record(&key, &kv.value)?;
            neighbors.push(record.src);
        }
        neighbors.sort_unstable();
        neighbors.dedup();
        Ok(neighbors)
    }

    pub async fn out_degree(&self, cell_id: &str, edge_type: &str, src: VertexId) -> Result<u64> {
        validate_component("cell_id", cell_id)?;
        validate_component("edge_type", edge_type)?;
        self.ensure_cell_readable(cell_id, "out_degree").await?;
        self.read_counter(&keys::degree_out(cell_id, edge_type, src))
            .await
    }

    pub async fn outbox_since(
        &self,
        cell_id: &str,
        after_epoch: GraphEpoch,
    ) -> Result<Vec<DeltaRecord>> {
        validate_component("cell_id", cell_id)?;
        self.ensure_cell_readable(cell_id, "outbox_since").await?;
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
            self.scan_outbox_delta_batches_between(
                cell_id,
                None,
                after_epoch,
                GraphEpoch::MAX,
                None,
            )
            .await?,
        );
        sort_and_dedup_deltas(&mut records);
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
        self.deltas_between_with_budget(cell_id, edge_type, after_epoch, read_epoch, None)
            .await
    }

    async fn deltas_between_with_budget(
        &self,
        cell_id: &str,
        edge_type: &str,
        after_epoch: GraphEpoch,
        read_epoch: GraphEpoch,
        budget: Option<&QueryBudget>,
    ) -> Result<Vec<DeltaRecord>> {
        validate_component("cell_id", cell_id)?;
        validate_component("edge_type", edge_type)?;
        self.ensure_cell_readable(cell_id, "deltas_between").await?;
        check_optional_query_budget(budget, "query_deltas_between")?;
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
            .scan_outbox_deltas_between(cell_id, edge_type, after_epoch, read_epoch, budget)
            .await?;
        records.extend(
            self.scan_outbox_delta_batches_between(
                cell_id,
                Some(edge_type),
                after_epoch,
                read_epoch,
                budget,
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

        sort_and_dedup_deltas(&mut records);
        Ok(records)
    }

    async fn scan_outbox_delta_batches_between(
        &self,
        cell_id: &str,
        edge_type: Option<&str>,
        after_epoch: GraphEpoch,
        read_epoch: GraphEpoch,
        budget: Option<&QueryBudget>,
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
            check_optional_query_budget(budget, "query_outbox_batch_scan")?;
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
                check_optional_query_budget(budget, "query_outbox_batch_edge_scan")?;
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
                if budget.is_some() {
                    ensure_limit(
                        "query_outbox_batch_records",
                        records.len() as u64,
                        self.limits.max_query_scan_edges,
                    )?;
                }
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
        budget: Option<&QueryBudget>,
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
            check_optional_query_budget(budget, "query_outbox_delta_scan")?;
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            let record = decode_delta_record(&key, &kv.value)?;
            if record.edge.epoch > read_epoch {
                break;
            }
            if record.edge.edge_type == edge_type && record.edge.epoch > after_epoch {
                records.push(record);
                if budget.is_some() {
                    ensure_limit(
                        "query_outbox_delta_records",
                        records.len() as u64,
                        self.limits.max_query_scan_edges,
                    )?;
                }
            }
        }
        sort_deltas(&mut records);
        Ok(records)
    }

    pub async fn current_epoch(&self, cell_id: &str) -> Result<GraphEpoch> {
        validate_component("cell_id", cell_id)?;
        self.ensure_cell_readable(cell_id, "current_epoch").await?;
        self.read_counter(&keys::last_epoch(cell_id)).await
    }

    pub(crate) async fn pin_current_read_epoch(
        &self,
        cell_id: &str,
        operation: &'static str,
    ) -> Result<GraphEpoch> {
        validate_component("cell_id", cell_id)?;
        self.ensure_cell_readable(cell_id, operation).await?;
        let read_epoch = self.read_counter(&keys::last_epoch(cell_id)).await?;
        self.publish_read_lease(cell_id, read_epoch).await?;
        self.ensure_cell_readable(cell_id, operation).await?;
        Ok(read_epoch)
    }

    pub(crate) async fn pin_read_epoch(
        &self,
        cell_id: &str,
        operation: &'static str,
        read_epoch: GraphEpoch,
    ) -> Result<()> {
        validate_component("cell_id", cell_id)?;
        self.ensure_cell_readable(cell_id, operation).await?;
        self.publish_read_lease(cell_id, read_epoch).await?;
        self.ensure_cell_readable(cell_id, operation).await
    }

    pub(crate) async fn ensure_cell_readable(
        &self,
        cell_id: &str,
        operation: &'static str,
    ) -> Result<()> {
        validate_component("cell_id", cell_id)?;
        if self
            .read_remote(&keys::cell_drop_marker(cell_id))
            .await?
            .is_some()
            || self
                .read_remote(&keys::cell_drop_pending_marker(cell_id))
                .await?
                .is_some()
        {
            return Err(GraphError::CellDropped {
                operation,
                cell_id: cell_id.to_string(),
            });
        }
        Ok(())
    }

    pub async fn edges_at(
        &self,
        cell_id: &str,
        edge_type: &str,
        read_epoch: GraphEpoch,
    ) -> Result<Vec<EdgeRecord>> {
        self.edges_at_with_budget(cell_id, edge_type, read_epoch, None)
            .await
    }

    async fn edges_at_with_budget(
        &self,
        cell_id: &str,
        edge_type: &str,
        read_epoch: GraphEpoch,
        budget: Option<&QueryBudget>,
    ) -> Result<Vec<EdgeRecord>> {
        validate_component("cell_id", cell_id)?;
        validate_component("edge_type", edge_type)?;
        self.ensure_cell_readable(cell_id, "edges_at").await?;
        check_optional_query_budget(budget, "query_edges_at")?;
        let mut edges = std::collections::BTreeMap::new();
        let base_epoch = if let Some(artifact) = self
            .latest_matrix_artifact(cell_id, edge_type, read_epoch)
            .await?
        {
            let adjacency = self
                .cached_matrix_adjacency(cell_id, edge_type, artifact.base_epoch)
                .await?;
            for (src, dsts) in adjacency.iter() {
                check_optional_query_budget(budget, "query_edges_at_adjacency")?;
                for dst in dsts {
                    check_optional_query_budget(budget, "query_edges_at_adjacency_edge")?;
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
                    if budget.is_some() {
                        ensure_limit(
                            "query_edges_at_base",
                            edges.len() as u64,
                            self.limits.max_query_scan_edges,
                        )?;
                    }
                }
            }
            artifact.base_epoch
        } else {
            0
        };
        for delta in self
            .deltas_between_with_budget(cell_id, edge_type, base_epoch, read_epoch, budget)
            .await?
        {
            check_optional_query_budget(budget, "query_edges_at_delta_apply")?;
            let key = (delta.edge.src, delta.edge.dst);
            match delta.kind {
                DeltaKind::Plus => {
                    edges.insert(key, delta.edge);
                    if budget.is_some() {
                        ensure_limit(
                            "query_edges_at_overlay",
                            edges.len() as u64,
                            self.limits.max_query_scan_edges,
                        )?;
                    }
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

#[derive(Clone, Debug)]
struct QueryBudget {
    started_at: std::time::Instant,
    max_runtime_ms: Option<u64>,
    cancellation_token: Option<QueryCancellationToken>,
}

impl QueryBudget {
    fn new(
        max_runtime_ms: Option<u64>,
        cancellation_token: Option<QueryCancellationToken>,
    ) -> Self {
        Self {
            started_at: std::time::Instant::now(),
            max_runtime_ms,
            cancellation_token,
        }
    }

    fn check(&self, operation: &'static str) -> Result<()> {
        if let Some(token) = &self.cancellation_token {
            if !token.is_cancelled() {
                return self.check_runtime_limit(operation);
            }
            return Err(GraphError::QueryTimeout {
                operation: "query_cancelled",
                elapsed_ms: self
                    .started_at
                    .elapsed()
                    .as_millis()
                    .min(u128::from(u64::MAX)) as u64,
                limit_ms: 0,
            });
        }
        self.check_runtime_limit(operation)
    }

    fn check_runtime_limit(&self, operation: &'static str) -> Result<()> {
        let Some(limit_ms) = self.max_runtime_ms else {
            return Ok(());
        };
        let elapsed_ms = self.started_at.elapsed().as_millis();
        if elapsed_ms >= u128::from(limit_ms) {
            return Err(GraphError::QueryTimeout {
                operation,
                elapsed_ms: elapsed_ms.min(u128::from(u64::MAX)) as u64,
                limit_ms,
            });
        }
        Ok(())
    }
}

fn check_optional_query_budget(
    budget: Option<&QueryBudget>,
    operation: &'static str,
) -> Result<()> {
    if let Some(budget) = budget {
        budget.check(operation)?;
    }
    Ok(())
}

#[cfg(feature = "opencypher")]
struct QueryStatsHistogramPublish<'a> {
    cell_id: &'a str,
    operation: &'static str,
    read_epoch: GraphEpoch,
    histogram_key: String,
    bucket_prefix: String,
    stats: &'a QueryStatsRecord,
    buckets: &'a BTreeMap<String, u64>,
}

#[cfg(feature = "opencypher")]
fn stats_record_from_bucket_count(
    count: u64,
    read_epoch: GraphEpoch,
    buckets: &BTreeMap<String, u64>,
) -> QueryStatsRecord {
    let mut stats = stats_record_from_histogram(read_epoch, buckets);
    stats.count = count;
    stats
}

#[cfg(feature = "opencypher")]
fn stats_record_from_histogram(
    read_epoch: GraphEpoch,
    buckets: &BTreeMap<String, u64>,
) -> QueryStatsRecord {
    let total = buckets.values().copied().sum::<u64>();
    let most_common = buckets.values().copied().max().unwrap_or(0);
    QueryStatsRecord::histogram(
        total,
        read_epoch,
        graph_now_millis(),
        buckets.len() as u64,
        most_common,
    )
}

#[cfg(feature = "opencypher")]
fn validate_query_stats_refresh_kind(kind: &QueryStatsRefreshKind) -> Result<()> {
    match kind {
        QueryStatsRefreshKind::Cardinality(QueryCardinalityStatsKind::EdgeType { edge_type }) => {
            validate_component("edge_type", edge_type)
        }
        QueryStatsRefreshKind::Cardinality(QueryCardinalityStatsKind::VertexLabel { label }) => {
            validate_component("label", label)
        }
        QueryStatsRefreshKind::Cardinality(QueryCardinalityStatsKind::VertexProperty {
            property,
            ..
        }) => validate_component("property", property),
        QueryStatsRefreshKind::Cardinality(QueryCardinalityStatsKind::EdgeProperty {
            edge_type,
            property,
            ..
        }) => {
            validate_component("edge_type", edge_type)?;
            validate_component("property", property)
        }
        QueryStatsRefreshKind::VertexPropertyHistogram { property } => {
            validate_component("property", property)
        }
        QueryStatsRefreshKind::EdgePropertyHistogram {
            edge_type,
            property,
        } => {
            validate_component("edge_type", edge_type)?;
            validate_component("property", property)
        }
    }
}

#[cfg(feature = "opencypher")]
struct EdgeRowMatchState<'a> {
    cell_id: &'a str,
    read_epoch: GraphEpoch,
    rows: &'a mut Vec<BindingRow>,
    metadata_cache: &'a mut BTreeMap<VertexId, VertexMetadata>,
    edge_metadata_cache: &'a mut BTreeMap<BoundRelationship, EdgeMetadata>,
    budget: &'a QueryBudget,
}

#[cfg(feature = "opencypher")]
struct VertexMutationApplyState<'a> {
    cell_id: &'a str,
    read_epoch: GraphEpoch,
    pending_metadata: &'a mut BTreeMap<VertexId, VertexMetadata>,
    original_metadata: &'a mut BTreeMap<VertexId, VertexMetadata>,
    pending_edge_metadata: &'a mut BTreeMap<BoundRelationship, EdgeMetadata>,
    original_edge_metadata: &'a mut BTreeMap<BoundRelationship, EdgeMetadata>,
    budget: &'a QueryBudget,
}

#[cfg(feature = "opencypher")]
fn streaming_neighbor_page_edge(query: &ParsedRowQuery) -> Option<&RowEdgePattern> {
    if !query.union_arms.is_empty()
        || query.predicate.is_some()
        || row_projections_have_aggregates(&query.projections)
        || query.pattern_groups.len() != 1
    {
        return None;
    }
    let group = query.pattern_groups.first()?;
    if group.optional || group.predicate.is_some() || group.patterns.len() != 1 {
        return None;
    }
    let RowPattern::Edge(edge) = group.patterns.first()? else {
        return None;
    };
    if edge.hop_range.is_some()
        || !edge.properties.is_empty()
        || edge.src.id.is_none()
        || edge.dst.id.is_some()
        || !edge.src.labels.is_empty()
        || !row_node_has_only_id_property(&edge.src)
        || !edge.dst.labels.is_empty()
        || !edge.dst.properties.is_empty()
    {
        return None;
    }
    if !query
        .projections
        .iter()
        .all(|projection| streaming_neighbor_projection_supported(edge, projection))
    {
        return None;
    }
    Some(edge)
}

#[cfg(feature = "opencypher")]
fn row_node_has_only_id_property(node: &RowNodePattern) -> bool {
    node.properties.keys().all(|property| property == "id")
}

#[cfg(feature = "opencypher")]
struct GraphKernelRowQueryRequest<'a> {
    edge: &'a RowEdgePattern,
    src: VertexId,
    hop_range: (u8, u8),
    projection: GraphKernelProjection,
    ascending: bool,
}

#[cfg(feature = "opencypher")]
struct ReachableWindowRequest<'a> {
    cell_id: &'a str,
    edge_type: &'a str,
    src: VertexId,
    hop_range: (u8, u8),
    read_epoch: GraphEpoch,
    window: QueryWindow,
    ascending: bool,
    budget: &'a QueryBudget,
}

#[cfg(feature = "opencypher")]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum GraphKernelProjection {
    NodeId,
    CountAll,
}

#[cfg(feature = "opencypher")]
fn graph_kernel_node_id_rows(
    vertices: Vec<VertexId>,
    budget: &QueryBudget,
) -> Result<Vec<QueryRow>> {
    let mut rows = Vec::with_capacity(vertices.len());
    for vertex in vertices {
        budget.check("cypher_graph_kernel_project")?;
        rows.push(QueryRow::new(vec![QueryValue::VertexId(vertex)]));
    }
    Ok(rows)
}

#[cfg(feature = "opencypher")]
fn graph_kernel_order_vertices(vertices: &mut Vec<VertexId>, ascending: bool) {
    vertices.sort_unstable();
    vertices.dedup();
    if !ascending {
        vertices.reverse();
    }
}

#[cfg(feature = "opencypher")]
fn validate_query_result_window(window: QueryWindow, max_rows: usize) -> Result<()> {
    let _ = usize::try_from(window.skip).map_err(|_| GraphError::AdmissionRejected {
        operation: "query_result_skip",
        actual: window.skip,
        limit: usize::MAX as u64,
    })?;
    if let Some(limit) = window.limit {
        ensure_limit("query_result_limit", limit as u64, max_rows as u64)?;
    }
    Ok(())
}

#[cfg(feature = "opencypher")]
fn graph_kernel_row_query_request(
    query: &ParsedRowQuery,
) -> Option<GraphKernelRowQueryRequest<'_>> {
    if !query.union_arms.is_empty() || query.predicate.is_some() {
        return None;
    }
    let patterns = graph_kernel_query_patterns(query)?;
    let [pattern] = patterns else {
        return None;
    };
    let RowPattern::Edge(edge) = pattern else {
        return None;
    };
    let src = edge.src.id?;
    let hop_range = edge.hop_range?;
    if edge.binding.is_some()
        || !edge.properties.is_empty()
        || !graph_kernel_node_pattern(&edge.src)
        || !graph_kernel_node_pattern(&edge.dst)
    {
        return None;
    }
    let projection = graph_kernel_projection(edge, &query.projections)?;
    let ascending = graph_kernel_order_supported(
        edge,
        projection,
        &query.projections,
        &query.columns,
        &query.order_by,
    )?;
    Some(GraphKernelRowQueryRequest {
        edge,
        src,
        hop_range,
        projection,
        ascending,
    })
}

#[cfg(feature = "opencypher")]
fn relationship_count_query_edge(query: &ParsedRowQuery) -> Option<&RowEdgePattern> {
    if !query.union_arms.is_empty() || query.predicate.is_some() || !query.order_by.is_empty() {
        return None;
    }
    if query.projections.as_slice() != [RowProjection::CountAll] {
        return None;
    }
    let patterns = graph_kernel_query_patterns(query)?;
    let [pattern] = patterns else {
        return None;
    };
    let RowPattern::Edge(edge) = pattern else {
        return None;
    };
    if edge.hop_range.is_some()
        || edge.src.id.is_none()
        || edge.dst.id.is_none()
        || !edge.properties.is_empty()
        || !graph_kernel_node_pattern(&edge.src)
        || !graph_kernel_node_pattern(&edge.dst)
    {
        return None;
    }
    Some(edge)
}

#[cfg(feature = "opencypher")]
fn source_relationship_id_rows_query_edge(query: &ParsedRowQuery) -> Option<&RowEdgePattern> {
    if !query.union_arms.is_empty()
        || query.predicate.is_some()
        || row_projections_have_aggregates(&query.projections)
    {
        return None;
    }
    let patterns = graph_kernel_query_patterns(query)?;
    let [pattern] = patterns else {
        return None;
    };
    let RowPattern::Edge(edge) = pattern else {
        return None;
    };
    if edge.hop_range.is_some()
        || edge.src.id.is_none()
        || edge.dst.id.is_some()
        || edge.binding.is_some()
        || !edge.properties.is_empty()
        || !graph_kernel_node_pattern(&edge.src)
        || !graph_kernel_node_pattern(&edge.dst)
        || !source_relationship_id_projection_supported(edge, &query.projections)
        || !source_relationship_id_order_supported(
            edge,
            &query.projections,
            &query.columns,
            &query.order_by,
        )
    {
        return None;
    }
    Some(edge)
}

#[cfg(feature = "opencypher")]
fn source_relationship_id_projection_supported(
    edge: &RowEdgePattern,
    projections: &[RowProjection],
) -> bool {
    !projections.is_empty()
        && projections.iter().all(|projection| match projection {
            RowProjection::NodeId { binding } => {
                edge.src.binding.as_deref() == Some(binding.as_str())
                    || edge.dst.binding.as_deref() == Some(binding.as_str())
            }
            RowProjection::Property { .. }
            | RowProjection::CountAll
            | RowProjection::Aggregate { .. } => false,
        })
}

#[cfg(feature = "opencypher")]
fn source_relationship_id_order_supported(
    edge: &RowEdgePattern,
    projections: &[RowProjection],
    columns: &[QueryColumn],
    order_by: &[RowSort],
) -> bool {
    order_by.iter().all(|sort| match &sort.expression {
        RowSortExpression::NodeId { binding } => {
            edge.src.binding.as_deref() == Some(binding.as_str())
                || edge.dst.binding.as_deref() == Some(binding.as_str())
        }
        RowSortExpression::Column { name } => columns
            .iter()
            .position(|column| column.name == *name)
            .and_then(|idx| projections.get(idx))
            .is_some_and(|projection| match projection {
                RowProjection::NodeId { binding } => {
                    edge.src.binding.as_deref() == Some(binding.as_str())
                        || edge.dst.binding.as_deref() == Some(binding.as_str())
                }
                RowProjection::Property { .. }
                | RowProjection::CountAll
                | RowProjection::Aggregate { .. } => false,
            }),
        RowSortExpression::Property { .. } | RowSortExpression::CountAll => false,
    })
}

#[cfg(feature = "opencypher")]
fn source_relationship_id_bindings_from_dsts(
    edge: &RowEdgePattern,
    src: VertexId,
    dsts: &[VertexId],
    shard: &GraphShard,
    budget: &QueryBudget,
) -> Result<Vec<BindingRow>> {
    let mut rows = Vec::with_capacity(dsts.len());
    for dst in dsts {
        budget.check("cypher_source_relationship_id_cached_rows")?;
        if let Some(row) = BindingRow::from_edge(edge, src, *dst) {
            rows.push(row);
            shard.ensure_query_intermediate_rows(
                "cypher_source_relationship_id_cached_rows",
                rows.len(),
            )?;
        }
    }
    Ok(rows)
}

#[cfg(feature = "opencypher")]
fn relationship_rows_query_edge(query: &ParsedRowQuery) -> Option<&RowEdgePattern> {
    if !query.union_arms.is_empty()
        || query.predicate.is_some()
        || row_projections_have_aggregates(&query.projections)
    {
        return None;
    }
    let patterns = graph_kernel_query_patterns(query)?;
    let [pattern] = patterns else {
        return None;
    };
    let RowPattern::Edge(edge) = pattern else {
        return None;
    };
    if edge.hop_range.is_some()
        || edge.src.id.is_none()
        || edge.dst.id.is_none()
        || edge.binding.is_none()
        || !graph_kernel_node_pattern(&edge.src)
        || !graph_kernel_node_pattern(&edge.dst)
    {
        return None;
    }
    Some(edge)
}

#[cfg(feature = "opencypher")]
fn relationship_rows_projection_supported(
    edge: &RowEdgePattern,
    projections: &[RowProjection],
) -> bool {
    projections.iter().all(|projection| match projection {
        RowProjection::NodeId { binding } => {
            edge.src.binding.as_deref() == Some(binding.as_str())
                || edge.dst.binding.as_deref() == Some(binding.as_str())
        }
        RowProjection::Property { binding, property } => {
            edge.binding.as_deref() == Some(binding.as_str()) && property != "id"
        }
        RowProjection::CountAll | RowProjection::Aggregate { .. } => false,
    })
}

#[cfg(feature = "opencypher")]
fn relationship_rows_order_supported(
    edge: &RowEdgePattern,
    projections: &[RowProjection],
    columns: &[QueryColumn],
    order_by: &[RowSort],
) -> bool {
    order_by.iter().all(|sort| match &sort.expression {
        RowSortExpression::NodeId { binding } => {
            edge.src.binding.as_deref() == Some(binding.as_str())
                || edge.dst.binding.as_deref() == Some(binding.as_str())
        }
        RowSortExpression::Property { binding, property } => {
            edge.binding.as_deref() == Some(binding.as_str()) && property != "id"
        }
        RowSortExpression::Column { name } => columns
            .iter()
            .position(|column| column.name == *name)
            .and_then(|idx| projections.get(idx))
            .is_some_and(|projection| match projection {
                RowProjection::NodeId { binding } => {
                    edge.src.binding.as_deref() == Some(binding.as_str())
                        || edge.dst.binding.as_deref() == Some(binding.as_str())
                }
                RowProjection::Property { binding, property } => {
                    edge.binding.as_deref() == Some(binding.as_str()) && property != "id"
                }
                RowProjection::CountAll | RowProjection::Aggregate { .. } => false,
            }),
        RowSortExpression::CountAll => false,
    })
}

#[cfg(feature = "opencypher")]
fn graph_kernel_query_patterns(query: &ParsedRowQuery) -> Option<&[RowPattern]> {
    if query.pattern_groups.is_empty() {
        return Some(&query.patterns);
    }
    if query.pattern_groups.len() != 1 {
        return None;
    }
    let group = query.pattern_groups.first()?;
    if group.optional || group.predicate.is_some() {
        return None;
    }
    Some(&group.patterns)
}

#[cfg(feature = "opencypher")]
fn graph_kernel_node_pattern(node: &RowNodePattern) -> bool {
    node.labels.is_empty() && row_node_has_only_id_property(node)
}

#[cfg(feature = "opencypher")]
fn graph_kernel_projection(
    edge: &RowEdgePattern,
    projections: &[RowProjection],
) -> Option<GraphKernelProjection> {
    let [projection] = projections else {
        return None;
    };
    match projection {
        RowProjection::NodeId { binding } => {
            if edge.dst.binding.as_deref() == Some(binding.as_str()) {
                Some(GraphKernelProjection::NodeId)
            } else {
                None
            }
        }
        RowProjection::CountAll => Some(GraphKernelProjection::CountAll),
        RowProjection::Property { .. } | RowProjection::Aggregate { .. } => None,
    }
}

#[cfg(feature = "opencypher")]
fn graph_kernel_order_supported(
    edge: &RowEdgePattern,
    projection: GraphKernelProjection,
    projections: &[RowProjection],
    columns: &[QueryColumn],
    order_by: &[RowSort],
) -> Option<bool> {
    if order_by.is_empty() {
        return Some(true);
    }
    let [sort] = order_by else {
        return None;
    };
    match projection {
        GraphKernelProjection::NodeId => match &sort.expression {
            RowSortExpression::NodeId { binding } => {
                if edge.dst.binding.as_deref() == Some(binding.as_str()) {
                    Some(sort.ascending)
                } else {
                    None
                }
            }
            RowSortExpression::Column { name } => {
                let projection_idx = columns.iter().position(|column| column.name == *name)?;
                match projections.get(projection_idx) {
                    Some(RowProjection::NodeId { binding })
                        if edge.dst.binding.as_deref() == Some(binding.as_str()) =>
                    {
                        Some(sort.ascending)
                    }
                    _ => None,
                }
            }
            RowSortExpression::Property { .. } | RowSortExpression::CountAll => None,
        },
        GraphKernelProjection::CountAll => match &sort.expression {
            RowSortExpression::CountAll => Some(true),
            RowSortExpression::Column { name } => {
                if matches!(columns.first(), Some(column) if column.name == *name) {
                    Some(true)
                } else {
                    None
                }
            }
            RowSortExpression::NodeId { .. } | RowSortExpression::Property { .. } => None,
        },
    }
}

#[cfg(feature = "opencypher")]
fn streaming_neighbor_projection_supported(
    edge: &RowEdgePattern,
    projection: &RowProjection,
) -> bool {
    let RowProjection::NodeId { binding } = projection else {
        return false;
    };
    edge.src.binding.as_deref() == Some(binding.as_str())
        || edge.dst.binding.as_deref() == Some(binding.as_str())
}

#[cfg(feature = "opencypher")]
fn streaming_neighbor_order_supported(
    edge: &RowEdgePattern,
    projections: &[RowProjection],
    columns: &[QueryColumn],
    order_by: &[RowSort],
) -> bool {
    if order_by.is_empty() {
        return true;
    }
    let [sort] = order_by else {
        return false;
    };
    if !sort.ascending {
        return false;
    }
    match &sort.expression {
        RowSortExpression::NodeId { binding } => {
            edge.dst.binding.as_deref() == Some(binding.as_str())
        }
        RowSortExpression::Column { name } => match columns
            .iter()
            .position(|column| column.name == *name)
            .and_then(|idx| projections.get(idx))
        {
            Some(projection) => {
                matches!(
                    projection,
                    RowProjection::NodeId { binding }
                        if edge.dst.binding.as_deref() == Some(binding.as_str())
                )
            }
            None => false,
        },
        RowSortExpression::Property { .. } | RowSortExpression::CountAll => false,
    }
}

#[cfg(feature = "opencypher")]
fn streaming_neighbor_projection_values(
    edge: &RowEdgePattern,
    src: VertexId,
    dst: VertexId,
    projections: &[RowProjection],
) -> Result<Vec<QueryValue>> {
    let mut values = Vec::with_capacity(projections.len());
    for projection in projections {
        let RowProjection::NodeId { binding } = projection else {
            return Err(GraphError::UnsupportedQuery {
                dialect: "OpenCypher",
                feature: "streaming neighbor page supports only node-id projections".to_string(),
            });
        };
        if edge.src.binding.as_deref() == Some(binding.as_str()) {
            values.push(QueryValue::VertexId(src));
        } else if edge.dst.binding.as_deref() == Some(binding.as_str()) {
            values.push(QueryValue::VertexId(dst));
        } else {
            return Err(GraphError::UnsupportedQuery {
                dialect: "OpenCypher",
                feature: format!("streaming neighbor page cannot project unbound {binding}"),
            });
        }
    }
    Ok(values)
}

#[cfg(feature = "opencypher")]
fn is_non_row_opencypher_query(err: &GraphError) -> bool {
    matches!(
        err,
        GraphError::UnsupportedQuery { dialect, feature }
            if *dialect == "OpenCypher"
                && feature.starts_with("row execution supports MATCH ... RETURN queries")
    )
}

#[cfg(feature = "opencypher")]
fn query_result_set_to_output(parsed: &ParsedRowQuery, result_set: QueryResultSet) -> QueryOutput {
    match parsed.projections.as_slice() {
        [RowProjection::NodeId { .. }] => {
            let vertices: Option<Vec<VertexId>> = result_set
                .rows
                .iter()
                .map(|row| match row.values.as_slice() {
                    [QueryValue::VertexId(vertex)] => Some(*vertex),
                    _ => None,
                })
                .collect();
            vertices.map_or(QueryOutput::Rows(result_set), QueryOutput::Vertices)
        }
        [RowProjection::CountAll]
        | [RowProjection::Aggregate {
            function: RowAggregateFunction::Count,
            ..
        }] => match result_set.rows.as_slice() {
            [row] => match row.values.as_slice() {
                [QueryValue::Count(count)] => QueryOutput::Count(*count),
                _ => QueryOutput::Rows(result_set),
            },
            _ => QueryOutput::Rows(result_set),
        },
        _ => QueryOutput::Rows(result_set),
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
fn query_next_cursor(
    cursor_offset: u64,
    page_size: usize,
    has_next: bool,
) -> Result<Option<QueryCursorToken>> {
    if !has_next {
        return Ok(None);
    }
    Ok(Some(QueryCursorToken::new(
        cursor_offset
            .checked_add(u64::try_from(page_size).unwrap_or(u64::MAX))
            .ok_or(GraphError::AdmissionRejected {
                operation: "query_cursor_offset",
                actual: u64::MAX,
                limit: u64::MAX - 1,
            })?,
    )))
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
fn parse_edge_metadata_delta_key(key: &str) -> Result<GraphEpoch> {
    let parts: Vec<_> = key.split('/').collect();
    match parts.as_slice() {
        ["cell", _cell_id, "emeta_delta", _edge_type, _src, _dst, epoch] => {
            parse_u64(key, epoch, "edge_metadata_epoch")
        }
        _ => Err(GraphError::CorruptValue {
            key: key.to_string(),
            reason: "expected edge metadata delta key".to_string(),
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
fn parse_vertex_property_index_key(key: &str) -> Result<(String, String, String, VertexId)> {
    let parts: Vec<_> = key.split('/').collect();
    match parts.as_slice() {
        ["cell", cell_id, "vprop_idx", property, encoded, vertex_id] => Ok((
            (*cell_id).to_string(),
            (*property).to_string(),
            (*encoded).to_string(),
            parse_u64(key, vertex_id, "vertex_id")?,
        )),
        _ => Err(GraphError::CorruptValue {
            key: key.to_string(),
            reason: "expected vertex property index key".to_string(),
        }),
    }
}

#[cfg(feature = "opencypher")]
fn parse_edge_property_index_key(
    key: &str,
) -> Result<(String, String, String, String, VertexId, VertexId)> {
    let parts: Vec<_> = key.split('/').collect();
    match parts.as_slice() {
        ["cell", cell_id, "eprop_idx", edge_type, property, encoded, src, dst] => Ok((
            (*cell_id).to_string(),
            (*edge_type).to_string(),
            (*property).to_string(),
            (*encoded).to_string(),
            parse_u64(key, src, "src")?,
            parse_u64(key, dst, "dst")?,
        )),
        _ => Err(GraphError::CorruptValue {
            key: key.to_string(),
            reason: "expected edge property index key".to_string(),
        }),
    }
}

#[cfg(feature = "opencypher")]
fn parse_edge_property_index_delta_key(key: &str) -> Result<(GraphEpoch, VertexId, VertexId)> {
    let parts: Vec<_> = key.split('/').collect();
    match parts.as_slice() {
        ["cell", _cell_id, "eprop_delta", _edge_type, _property, _encoded, epoch, src, dst] => {
            Ok((
                parse_u64(key, epoch, "edge_property_epoch")?,
                parse_u64(key, src, "src")?,
                parse_u64(key, dst, "dst")?,
            ))
        }
        _ => Err(GraphError::CorruptValue {
            key: key.to_string(),
            reason: "expected edge property index delta key".to_string(),
        }),
    }
}

#[cfg(feature = "opencypher")]
fn parse_relationship_metadata_delta_key(key: &str) -> Result<GraphEpoch> {
    let parts: Vec<_> = key.split('/').collect();
    match parts.as_slice() {
        ["cell", _cell_id, "rel_delta", _edge_type, _src, _dst, _relationship_id, epoch] => {
            parse_u64(key, epoch, "relationship_metadata_epoch")
        }
        _ => Err(GraphError::CorruptValue {
            key: key.to_string(),
            reason: "expected relationship metadata delta key".to_string(),
        }),
    }
}

#[cfg(feature = "opencypher")]
fn parse_relationship_tombstone_edge_key(key: &str) -> Result<RelationshipId> {
    let parts: Vec<_> = key.split('/').collect();
    match parts.as_slice() {
        ["cell", _cell_id, "rel_tomb", _edge_type, _src, _dst, relationship_id] => {
            parse_u64(key, relationship_id, "relationship_id")
        }
        _ => Err(GraphError::CorruptValue {
            key: key.to_string(),
            reason: "expected relationship tombstone key".to_string(),
        }),
    }
}

#[cfg(feature = "opencypher")]
fn parse_relationship_tombstone_source_key(key: &str) -> Result<(VertexId, RelationshipId)> {
    let parts: Vec<_> = key.split('/').collect();
    match parts.as_slice() {
        ["cell", _cell_id, "rel_tomb", _edge_type, _src, dst, relationship_id] => Ok((
            parse_u64(key, dst, "dst")?,
            parse_u64(key, relationship_id, "relationship_id")?,
        )),
        _ => Err(GraphError::CorruptValue {
            key: key.to_string(),
            reason: "expected relationship tombstone key".to_string(),
        }),
    }
}

#[cfg(feature = "opencypher")]
fn relationship_tombstone_hides_record(
    record: &RelationshipRecord,
    tombstone_epoch: GraphEpoch,
    read_epoch: GraphEpoch,
) -> bool {
    record.epoch <= tombstone_epoch && tombstone_epoch <= read_epoch
}

#[cfg(feature = "opencypher")]
fn parse_relationship_property_index_delta_key(
    key: &str,
) -> Result<(GraphEpoch, VertexId, VertexId, RelationshipId)> {
    let parts: Vec<_> = key.split('/').collect();
    match parts.as_slice() {
        ["cell", _cell_id, "rprop_delta", _edge_type, _property, _encoded, epoch, src, dst, relationship_id] => {
            Ok((
                parse_u64(key, epoch, "relationship_property_epoch")?,
                parse_u64(key, src, "src")?,
                parse_u64(key, dst, "dst")?,
                parse_u64(key, relationship_id, "relationship_id")?,
            ))
        }
        _ => Err(GraphError::CorruptValue {
            key: key.to_string(),
            reason: "expected relationship property index delta key".to_string(),
        }),
    }
}

#[cfg(feature = "opencypher")]
#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct BindingRow {
    values: BTreeMap<String, VertexId>,
    null_values: BTreeSet<String>,
    relationships: BTreeMap<String, BoundRelationship>,
    null_relationships: BTreeSet<String>,
    relationship_metadata: BTreeMap<BoundRelationship, EdgeMetadata>,
    metadata: BTreeMap<String, VertexMetadata>,
}

#[cfg(feature = "opencypher")]
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct BoundRelationship {
    edge_type: String,
    src: VertexId,
    dst: VertexId,
    relationship_id: Option<RelationshipId>,
}

#[cfg(feature = "opencypher")]
struct RelationshipPropertyLookup<'a> {
    cell_id: &'a str,
    edge_type: &'a str,
    src: VertexId,
    dst: VertexId,
    property: &'a str,
    value: &'a VertexPropertyValue,
    read_epoch: GraphEpoch,
    budget: &'a QueryBudget,
}

#[cfg(feature = "opencypher")]
struct EncodedRelationshipPropertyLookup<'a> {
    cell_id: &'a str,
    edge_type: &'a str,
    src: VertexId,
    dst: VertexId,
    property: &'a str,
    encoded: &'a str,
    read_epoch: GraphEpoch,
    latest_snapshot: bool,
    tombstones: &'a BTreeMap<RelationshipId, GraphEpoch>,
    budget: &'a QueryBudget,
}

#[cfg(feature = "opencypher")]
struct RelationshipPropertyEdgeIndexLookup<'a> {
    cell_id: &'a str,
    edge_type: &'a str,
    property: &'a str,
    encoded: &'a str,
    src: VertexId,
    dst: VertexId,
    budget: &'a QueryBudget,
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
        let relationship = BoundRelationship {
            edge_type: pattern.edge_type.clone(),
            src,
            dst,
            relationship_id: None,
        };
        Self::from_relationship(pattern, relationship, EdgeMetadata::default())
    }

    fn from_relationship(
        pattern: &RowEdgePattern,
        relationship: BoundRelationship,
        metadata: EdgeMetadata,
    ) -> Option<Self> {
        let mut row = Self::default();
        if !row.bind(pattern.src.binding.as_deref(), relationship.src) {
            return None;
        }
        if !row.bind(pattern.dst.binding.as_deref(), relationship.dst) {
            return None;
        }
        if !row.bind_relationship(pattern.binding.as_deref(), relationship.clone()) {
            return None;
        }
        row.relationship_metadata.insert(relationship, metadata);
        Some(row)
    }

    fn bind(&mut self, binding: Option<&str>, value: VertexId) -> bool {
        let Some(binding) = binding else {
            return true;
        };
        if self.null_values.contains(binding) {
            return false;
        }
        match self.values.get(binding) {
            Some(existing) => *existing == value,
            None => {
                self.values.insert(binding.to_string(), value);
                true
            }
        }
    }

    fn bind_relationship(&mut self, binding: Option<&str>, value: BoundRelationship) -> bool {
        let Some(binding) = binding else {
            return true;
        };
        if self.null_relationships.contains(binding) {
            return false;
        }
        match self.relationships.get(binding) {
            Some(existing) => *existing == value,
            None => {
                self.relationships.insert(binding.to_string(), value);
                true
            }
        }
    }

    fn get(&self, binding: &str) -> Result<VertexId> {
        if self.null_values.contains(binding) {
            return Err(GraphError::UnsupportedQuery {
                dialect: "OpenCypher",
                feature: format!("variable {binding} is null"),
            });
        }
        self.values
            .get(binding)
            .copied()
            .ok_or_else(|| GraphError::UnsupportedQuery {
                dialect: "OpenCypher",
                feature: format!("unbound variable {binding}"),
            })
    }

    fn get_optional(&self, binding: &str) -> Result<Option<VertexId>> {
        if self.null_values.contains(binding) {
            return Ok(None);
        }
        self.values
            .get(binding)
            .copied()
            .map(Some)
            .ok_or_else(|| GraphError::UnsupportedQuery {
                dialect: "OpenCypher",
                feature: format!("unbound variable {binding}"),
            })
    }

    fn mark_optional_group_nulls(&mut self, group: &RowMatchGroup) {
        for pattern in &group.patterns {
            self.mark_optional_pattern_nulls(pattern);
        }
    }

    fn mark_optional_pattern_nulls(&mut self, pattern: &RowPattern) {
        match pattern {
            RowPattern::Node(node) => self.mark_optional_node_nulls(node),
            RowPattern::Edge(edge) => {
                if let Some(binding) = &edge.binding {
                    if !self.relationships.contains_key(binding) {
                        self.null_relationships.insert(binding.clone());
                    }
                }
                self.mark_optional_node_nulls(&edge.src);
                self.mark_optional_node_nulls(&edge.dst);
            }
        }
    }

    fn mark_optional_node_nulls(&mut self, node: &RowNodePattern) {
        if let Some(binding) = &node.binding {
            if !self.values.contains_key(binding) {
                self.null_values.insert(binding.clone());
            }
        }
    }

    fn join(&self, other: &Self) -> Option<Self> {
        let mut joined = self.clone();
        for (binding, value) in &other.values {
            if !joined.bind(Some(binding), *value) {
                return None;
            }
        }
        for binding in &other.null_values {
            if joined.values.contains_key(binding) {
                return None;
            }
            joined.null_values.insert(binding.clone());
        }
        for (binding, metadata) in &other.metadata {
            match joined.metadata.get(binding) {
                Some(existing) if existing != metadata => return None,
                Some(_) => {}
                None => {
                    joined.metadata.insert(binding.clone(), metadata.clone());
                }
            }
        }
        for (binding, relationship) in &other.relationships {
            match joined.relationships.get(binding) {
                Some(existing) if existing != relationship => return None,
                Some(_) => {}
                None => {
                    joined
                        .relationships
                        .insert(binding.clone(), relationship.clone());
                }
            }
        }
        for binding in &other.null_relationships {
            if joined.relationships.contains_key(binding) {
                return None;
            }
            joined.null_relationships.insert(binding.clone());
        }
        for (relationship, metadata) in &other.relationship_metadata {
            match joined.relationship_metadata.get(relationship) {
                Some(existing) if existing.properties.is_empty() => {
                    joined
                        .relationship_metadata
                        .insert(relationship.clone(), metadata.clone());
                }
                Some(existing) if metadata.properties.is_empty() || existing == metadata => {}
                Some(_) => return None,
                None => {
                    joined
                        .relationship_metadata
                        .insert(relationship.clone(), metadata.clone());
                }
            }
        }
        Some(joined)
    }
}

#[cfg(feature = "opencypher")]
fn common_binding_row_bound_names(rows: &[BindingRow]) -> BTreeSet<String> {
    let Some(first) = rows.first() else {
        return BTreeSet::new();
    };
    let mut common = binding_row_bound_names(first);
    for row in &rows[1..] {
        let bound = binding_row_bound_names(row);
        common.retain(|binding| bound.contains(binding));
    }
    common
}

#[cfg(feature = "opencypher")]
fn binding_rows_bound_names_union(rows: &[BindingRow]) -> BTreeSet<String> {
    rows.iter().flat_map(binding_row_bound_names).collect()
}

#[cfg(feature = "opencypher")]
fn binding_row_bound_names(row: &BindingRow) -> BTreeSet<String> {
    row.values
        .keys()
        .chain(row.relationships.keys())
        .cloned()
        .collect()
}

#[cfg(feature = "opencypher")]
fn row_pattern_bound_names(pattern: &RowPattern) -> BTreeSet<String> {
    match pattern {
        RowPattern::Node(node) => node.binding.iter().cloned().collect(),
        RowPattern::Edge(edge) => {
            let mut names = BTreeSet::new();
            if let Some(binding) = &edge.src.binding {
                names.insert(binding.clone());
            }
            if let Some(binding) = &edge.dst.binding {
                names.insert(binding.clone());
            }
            names
        }
    }
}

#[cfg(feature = "opencypher")]
fn binding_row_join_key(row: &BindingRow, bindings: &[String]) -> Option<Vec<VertexId>> {
    let mut key = Vec::with_capacity(bindings.len());
    for binding in bindings {
        key.push(*row.values.get(binding)?);
    }
    Some(key)
}

#[cfg(feature = "opencypher")]
fn hash_joinable_pattern(pattern: &RowPattern) -> bool {
    match pattern {
        RowPattern::Node(node) => {
            node.id.is_none() && (!node.labels.is_empty() || node_has_metadata_constraints(node))
        }
        RowPattern::Edge(edge) => edge.hop_range.is_none() && !edge.properties.is_empty(),
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
#[derive(Clone, Debug, Eq, PartialEq)]
enum AggregateAccumulator {
    CountAll(u64),
    CountExpression(u64),
    Sum(u128),
    Avg { sum: u128, count: u64 },
    Collect(Vec<QueryValue>),
}

#[cfg(feature = "opencypher")]
fn constrain_row_pattern(pattern: &RowPattern, row: &BindingRow) -> Result<Option<RowPattern>> {
    Ok(Some(match pattern {
        RowPattern::Node(node) => {
            let Some(node) = constrain_row_node_pattern(node, row)? else {
                return Ok(None);
            };
            RowPattern::Node(node)
        }
        RowPattern::Edge(edge) => {
            if let Some(binding) = &edge.binding {
                if row.null_relationships.contains(binding) {
                    return Ok(None);
                }
            }
            let Some(src) = constrain_row_node_pattern(&edge.src, row)? else {
                return Ok(None);
            };
            let Some(dst) = constrain_row_node_pattern(&edge.dst, row)? else {
                return Ok(None);
            };
            RowPattern::Edge(RowEdgePattern {
                binding: edge.binding.clone(),
                edge_type: edge.edge_type.clone(),
                src,
                dst,
                properties: edge.properties.clone(),
                hop_range: edge.hop_range,
            })
        }
    }))
}

#[cfg(feature = "opencypher")]
fn constrain_row_node_pattern(
    node: &RowNodePattern,
    row: &BindingRow,
) -> Result<Option<RowNodePattern>> {
    let Some(binding) = &node.binding else {
        return Ok(Some(node.clone()));
    };
    if row.null_values.contains(binding) {
        return Ok(None);
    }
    let Some(bound_id) = row.values.get(binding).copied() else {
        return Ok(Some(node.clone()));
    };
    if matches!(node.id, Some(pattern_id) if pattern_id != bound_id) {
        return Ok(None);
    }
    let mut constrained = node.clone();
    constrained.id = Some(bound_id);
    if let Some(metadata) = row.metadata.get(binding) {
        if !vertex_metadata_matches(metadata, &constrained) {
            return Ok(None);
        }
    }
    Ok(Some(constrained))
}

#[cfg(feature = "opencypher")]
fn row_matches_edge_pattern(row: &BindingRow, pattern: &RowEdgePattern) -> Result<bool> {
    if !(row_matches_node(row, &pattern.src)? && row_matches_node(row, &pattern.dst)?) {
        return Ok(false);
    }
    if pattern.properties.is_empty() {
        return Ok(true);
    }
    let relationship = relationship_identity_for_pattern(row, pattern)?;
    let Some(metadata) = row.relationship_metadata.get(&relationship) else {
        return Err(GraphError::UnsupportedQuery {
            dialect: "OpenCypher",
            feature: "relationship metadata was not hydrated".to_string(),
        });
    };
    Ok(pattern.properties.iter().all(|(property, value)| {
        metadata
            .properties
            .get(property)
            .is_some_and(|existing| vertex_property_values_equal(existing, value))
    }))
}

#[cfg(feature = "opencypher")]
fn relationship_metadata_matches(metadata: &EdgeMetadata, pattern: &RowEdgePattern) -> bool {
    pattern.properties.iter().all(|(property, value)| {
        metadata
            .properties
            .get(property)
            .is_some_and(|existing| vertex_property_values_equal(existing, value))
    })
}

#[cfg(feature = "opencypher")]
fn relationship_identity_for_pattern(
    row: &BindingRow,
    pattern: &RowEdgePattern,
) -> Result<BoundRelationship> {
    if let Some(binding) = &pattern.binding {
        return row.relationships.get(binding).cloned().ok_or_else(|| {
            GraphError::UnsupportedQuery {
                dialect: "OpenCypher",
                feature: format!("unbound relationship {binding}"),
            }
        });
    }
    if let (Some(src_binding), Some(dst_binding)) = (
        pattern.src.binding.as_deref(),
        pattern.dst.binding.as_deref(),
    ) {
        return Ok(BoundRelationship {
            edge_type: pattern.edge_type.clone(),
            src: row.get(src_binding)?,
            dst: row.get(dst_binding)?,
            relationship_id: None,
        });
    }
    let mut matches = row
        .relationship_metadata
        .keys()
        .filter(|relationship| relationship.edge_type == pattern.edge_type);
    let Some(relationship) = matches.next().cloned() else {
        return Err(GraphError::UnsupportedQuery {
            dialect: "OpenCypher",
            feature: "relationship pattern has no bound identity".to_string(),
        });
    };
    if matches.next().is_some() {
        return Err(GraphError::UnsupportedQuery {
            dialect: "OpenCypher",
            feature: "anonymous relationship property pattern is ambiguous".to_string(),
        });
    }
    Ok(relationship)
}

#[cfg(feature = "opencypher")]
fn row_matches_node(row: &BindingRow, node: &RowNodePattern) -> Result<bool> {
    let Some(binding) = &node.binding else {
        return Ok(true);
    };
    if row.null_values.contains(binding) {
        return Ok(false);
    }
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
            .all(|(property, value)| {
                metadata
                    .properties
                    .get(property)
                    .is_some_and(|existing| vertex_property_values_equal(existing, value))
            })
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
        RowExpression::NodeId { binding } => match row.get_optional(binding)? {
            Some(value) => Ok(RowScalarValue::Value(VertexPropertyValue::Integer(value))),
            None => Ok(RowScalarValue::Missing),
        },
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
        RowComparisonOp::Eq => numeric_property_order(left, right)
            .map(|ordering| compare_ordering(ordering, op))
            .unwrap_or(left == right),
        RowComparisonOp::Ne => numeric_property_order(left, right)
            .map(|ordering| compare_ordering(ordering, op))
            .unwrap_or(left != right),
        RowComparisonOp::Lt | RowComparisonOp::Gt | RowComparisonOp::Lte | RowComparisonOp::Gte => {
            match numeric_property_order(left, right) {
                Some(ordering) => compare_ordering(ordering, op),
                None => match (left, right) {
                    (VertexPropertyValue::String(left), VertexPropertyValue::String(right)) => {
                        compare_ordering(left.cmp(right), op)
                    }
                    _ => {
                        return Err(GraphError::UnsupportedQuery {
                            dialect: "OpenCypher",
                            feature:
                                "ordered comparisons require numeric or matching string values"
                                    .to_string(),
                        });
                    }
                },
            }
        }
    })
}

#[cfg(feature = "opencypher")]
fn vertex_property_values_equal(left: &VertexPropertyValue, right: &VertexPropertyValue) -> bool {
    compare_vertex_property_values(left, RowComparisonOp::Eq, right).unwrap_or(false)
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
    if row.null_values.contains(binding) || row.null_relationships.contains(binding) {
        return Ok(None);
    }
    if row.values.contains_key(binding) && property == "id" {
        return Ok(Some(VertexPropertyValue::Integer(row.get(binding)?)));
    }
    if let Some(relationship) = row.relationships.get(binding) {
        if property == "id" {
            return Err(GraphError::UnsupportedQuery {
                dialect: "OpenCypher",
                feature: "relationship id properties are not executable in Query engine"
                    .to_string(),
            });
        }
        let Some(metadata) = row.relationship_metadata.get(relationship) else {
            return Err(GraphError::UnsupportedQuery {
                dialect: "OpenCypher",
                feature: format!("metadata for relationship {binding} was not hydrated"),
            });
        };
        return Ok(metadata.properties.get(property).cloned());
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
                values.push(match row.get_optional(binding)? {
                    Some(value) => QueryValue::VertexId(value),
                    None => QueryValue::Null,
                });
            }
            RowProjection::Property { binding, property } => {
                values.push(match binding_property(row, binding, property)? {
                    Some(value) => QueryValue::Property(value),
                    None => QueryValue::Null,
                });
            }
            RowProjection::CountAll | RowProjection::Aggregate { .. } => {
                return Err(GraphError::UnsupportedQuery {
                    dialect: "OpenCypher",
                    feature: "aggregate projection must be planned as an aggregate".to_string(),
                });
            }
        }
    }
    Ok(QueryRow::new(values))
}

#[cfg(feature = "opencypher")]
fn project_relationship_row(
    edge: &RowEdgePattern,
    relationship: &BoundRelationship,
    metadata: &EdgeMetadata,
    projections: &[RowProjection],
) -> Result<QueryRow> {
    let mut values = Vec::with_capacity(projections.len());
    for projection in projections {
        values.push(match projection {
            RowProjection::NodeId { binding } => {
                QueryValue::VertexId(relationship_endpoint_id(edge, relationship, binding)?)
            }
            RowProjection::Property { binding, property } => {
                if edge.binding.as_deref() != Some(binding.as_str()) {
                    return Err(GraphError::UnsupportedQuery {
                        dialect: "OpenCypher",
                        feature: format!(
                            "relationship fast path cannot project property {binding}.{property}"
                        ),
                    });
                }
                if property == "id" {
                    return Err(GraphError::UnsupportedQuery {
                        dialect: "OpenCypher",
                        feature: "relationship id properties are not executable in Query engine"
                            .to_string(),
                    });
                }
                metadata
                    .properties
                    .get(property)
                    .cloned()
                    .map(QueryValue::Property)
                    .unwrap_or(QueryValue::Null)
            }
            RowProjection::CountAll | RowProjection::Aggregate { .. } => {
                return Err(GraphError::UnsupportedQuery {
                    dialect: "OpenCypher",
                    feature: "aggregate projection must be planned as an aggregate".to_string(),
                });
            }
        });
    }
    Ok(QueryRow::new(values))
}

#[cfg(feature = "opencypher")]
fn relationship_rows_cache_value(
    rows: &[(BoundRelationship, EdgeMetadata)],
) -> RelationshipRowsCacheValue {
    RelationshipRowsCacheValue::new(
        rows.iter()
            .map(|(relationship, metadata)| RelationshipRowsCacheEntry {
                relationship_id: relationship.relationship_id,
                metadata: metadata.clone(),
            })
            .collect(),
    )
}

#[cfg(feature = "opencypher")]
fn relationship_rows_from_cache_value(
    edge_type: &str,
    src: VertexId,
    dst: VertexId,
    cached: RelationshipRowsCacheValue,
) -> Vec<(BoundRelationship, EdgeMetadata)> {
    cached
        .rows
        .iter()
        .map(|entry| {
            (
                BoundRelationship {
                    edge_type: edge_type.to_string(),
                    src,
                    dst,
                    relationship_id: entry.relationship_id,
                },
                entry.metadata.clone(),
            )
        })
        .collect()
}

#[cfg(feature = "opencypher")]
fn sort_keys_for_relationship_row(
    edge: &RowEdgePattern,
    relationship: &BoundRelationship,
    metadata: &EdgeMetadata,
    row: &QueryRow,
    query: &ParsedRowQuery,
) -> Result<Vec<QueryValue>> {
    let mut keys = Vec::with_capacity(query.order_by.len());
    for sort in &query.order_by {
        keys.push(match &sort.expression {
            RowSortExpression::NodeId { binding } => {
                QueryValue::VertexId(relationship_endpoint_id(edge, relationship, binding)?)
            }
            RowSortExpression::Property { binding, property } => {
                if edge.binding.as_deref() != Some(binding.as_str()) {
                    return Err(GraphError::UnsupportedQuery {
                        dialect: "OpenCypher",
                        feature: format!(
                            "relationship fast path cannot sort by property {binding}.{property}"
                        ),
                    });
                }
                metadata
                    .properties
                    .get(property)
                    .cloned()
                    .map(QueryValue::Property)
                    .unwrap_or(QueryValue::Null)
            }
            RowSortExpression::Column { name } => {
                projected_column_value(row, &query.columns, name)?
            }
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
fn relationship_endpoint_id(
    edge: &RowEdgePattern,
    relationship: &BoundRelationship,
    binding: &str,
) -> Result<VertexId> {
    if edge.src.binding.as_deref() == Some(binding) {
        return Ok(relationship.src);
    }
    if edge.dst.binding.as_deref() == Some(binding) {
        return Ok(relationship.dst);
    }
    Err(GraphError::UnsupportedQuery {
        dialect: "OpenCypher",
        feature: format!("relationship fast path cannot project unbound node {binding}"),
    })
}

#[cfg(feature = "opencypher")]
fn row_projections_have_aggregates(projections: &[RowProjection]) -> bool {
    projections.iter().any(is_aggregate_projection)
}

#[cfg(feature = "opencypher")]
fn is_aggregate_projection(projection: &RowProjection) -> bool {
    matches!(
        projection,
        RowProjection::CountAll | RowProjection::Aggregate { .. }
    )
}

#[cfg(feature = "opencypher")]
fn aggregate_projected_rows(
    bindings: Vec<BindingRow>,
    projections: &[RowProjection],
    columns: &[QueryColumn],
    order_by: &[RowSort],
    budget: &QueryBudget,
) -> Result<Vec<ProjectedQueryRow>> {
    let group_projection_indexes: Vec<_> = projections
        .iter()
        .enumerate()
        .filter_map(|(idx, projection)| (!is_aggregate_projection(projection)).then_some(idx))
        .collect();
    let aggregate_projection_indexes: Vec<_> = projections
        .iter()
        .enumerate()
        .filter_map(|(idx, projection)| is_aggregate_projection(projection).then_some(idx))
        .collect();

    let mut groups = BTreeMap::<Vec<QueryValue>, Vec<AggregateAccumulator>>::new();
    if bindings.is_empty() && group_projection_indexes.is_empty() {
        groups.insert(
            Vec::new(),
            aggregate_projection_indexes
                .iter()
                .map(|idx| new_aggregate_accumulator(&projections[*idx]))
                .collect::<Result<_>>()?,
        );
    }

    for binding in bindings {
        budget.check("cypher_aggregate_group")?;
        let mut group_key = Vec::with_capacity(group_projection_indexes.len());
        for idx in &group_projection_indexes {
            group_key.push(project_single_binding_value(&binding, &projections[*idx])?);
        }
        let states = match groups.entry(group_key) {
            std::collections::btree_map::Entry::Occupied(entry) => entry.into_mut(),
            std::collections::btree_map::Entry::Vacant(entry) => entry.insert(
                aggregate_projection_indexes
                    .iter()
                    .map(|idx| new_aggregate_accumulator(&projections[*idx]))
                    .collect::<Result<_>>()?,
            ),
        };
        for (state_idx, projection_idx) in aggregate_projection_indexes.iter().enumerate() {
            apply_aggregate_projection(
                &mut states[state_idx],
                &projections[*projection_idx],
                &binding,
            )?;
        }
    }

    let mut projected = Vec::with_capacity(groups.len());
    for (group_key, states) in groups {
        budget.check("cypher_aggregate_project")?;
        let mut group_idx = 0;
        let mut aggregate_idx = 0;
        let mut values = Vec::with_capacity(projections.len());
        for projection in projections {
            if is_aggregate_projection(projection) {
                values.push(finalize_aggregate(&states[aggregate_idx])?);
                aggregate_idx += 1;
            } else {
                values.push(group_key[group_idx].clone());
                group_idx += 1;
            }
        }
        let row = QueryRow::new(values);
        let sort_keys = sort_keys_for_projected_only(&row, columns, order_by)?;
        projected.push(ProjectedQueryRow { row, sort_keys });
    }
    Ok(projected)
}

#[cfg(feature = "opencypher")]
fn new_aggregate_accumulator(projection: &RowProjection) -> Result<AggregateAccumulator> {
    Ok(match projection {
        RowProjection::CountAll => AggregateAccumulator::CountAll(0),
        RowProjection::Aggregate { function, .. } => match function {
            RowAggregateFunction::Count => AggregateAccumulator::CountExpression(0),
            RowAggregateFunction::Sum => AggregateAccumulator::Sum(0),
            RowAggregateFunction::Avg => AggregateAccumulator::Avg { sum: 0, count: 0 },
            RowAggregateFunction::Collect => AggregateAccumulator::Collect(Vec::new()),
        },
        _ => {
            return Err(GraphError::UnsupportedQuery {
                dialect: "OpenCypher",
                feature: "non-aggregate projection cannot create aggregate state".to_string(),
            });
        }
    })
}

#[cfg(feature = "opencypher")]
fn apply_aggregate_projection(
    state: &mut AggregateAccumulator,
    projection: &RowProjection,
    row: &BindingRow,
) -> Result<()> {
    match (state, projection) {
        (AggregateAccumulator::CountAll(count), RowProjection::CountAll) => {
            *count = count.saturating_add(1);
        }
        (
            AggregateAccumulator::CountExpression(count),
            RowProjection::Aggregate {
                function: RowAggregateFunction::Count,
                expression,
            },
        ) => {
            if expression_query_value(row, expression)?.is_some() {
                *count = count.saturating_add(1);
            }
        }
        (
            AggregateAccumulator::Sum(sum),
            RowProjection::Aggregate {
                function: RowAggregateFunction::Sum,
                expression,
            },
        ) => {
            if let Some(value) = aggregate_integer_value(row, expression, "sum")? {
                *sum = sum.checked_add(u128::from(value)).ok_or_else(|| {
                    GraphError::UnsupportedQuery {
                        dialect: "OpenCypher",
                        feature: "sum aggregate overflowed".to_string(),
                    }
                })?;
            }
        }
        (
            AggregateAccumulator::Avg { sum, count },
            RowProjection::Aggregate {
                function: RowAggregateFunction::Avg,
                expression,
            },
        ) => {
            if let Some(value) = aggregate_integer_value(row, expression, "avg")? {
                *sum = sum.checked_add(u128::from(value)).ok_or_else(|| {
                    GraphError::UnsupportedQuery {
                        dialect: "OpenCypher",
                        feature: "avg aggregate overflowed".to_string(),
                    }
                })?;
                *count = count.saturating_add(1);
            }
        }
        (
            AggregateAccumulator::Collect(values),
            RowProjection::Aggregate {
                function: RowAggregateFunction::Collect,
                expression,
            },
        ) => {
            if let Some(value) = expression_query_value(row, expression)? {
                values.push(value);
            }
        }
        _ => {
            return Err(GraphError::UnsupportedQuery {
                dialect: "OpenCypher",
                feature: "aggregate projection state mismatch".to_string(),
            });
        }
    }
    Ok(())
}

#[cfg(feature = "opencypher")]
fn aggregate_integer_value(
    row: &BindingRow,
    expression: &RowExpression,
    function: &str,
) -> Result<Option<u64>> {
    match eval_row_expression(row, expression)? {
        RowScalarValue::Missing => Ok(None),
        RowScalarValue::Value(VertexPropertyValue::Integer(value)) => Ok(Some(value)),
        RowScalarValue::Value(_) => Err(GraphError::UnsupportedQuery {
            dialect: "OpenCypher",
            feature: format!("{function} aggregate requires integer values"),
        }),
    }
}

#[cfg(feature = "opencypher")]
fn finalize_aggregate(state: &AggregateAccumulator) -> Result<QueryValue> {
    Ok(match state {
        AggregateAccumulator::CountAll(count) | AggregateAccumulator::CountExpression(count) => {
            QueryValue::Count(*count)
        }
        AggregateAccumulator::Sum(sum) => {
            QueryValue::Property(VertexPropertyValue::Integer((*sum).try_into().map_err(
                |_| GraphError::UnsupportedQuery {
                    dialect: "OpenCypher",
                    feature: "sum aggregate exceeds u64 result range".to_string(),
                },
            )?))
        }
        AggregateAccumulator::Avg { sum: _, count: 0 } => QueryValue::Null,
        AggregateAccumulator::Avg { sum, count } => {
            QueryValue::Float(QueryFloat(*sum as f64 / *count as f64))
        }
        AggregateAccumulator::Collect(values) => QueryValue::List(values.clone()),
    })
}

#[cfg(feature = "opencypher")]
fn project_single_binding_value(
    row: &BindingRow,
    projection: &RowProjection,
) -> Result<QueryValue> {
    match projection {
        RowProjection::NodeId { binding } => Ok(match row.get_optional(binding)? {
            Some(value) => QueryValue::VertexId(value),
            None => QueryValue::Null,
        }),
        RowProjection::Property { binding, property } => {
            Ok(match binding_property(row, binding, property)? {
                Some(value) => QueryValue::Property(value),
                None => QueryValue::Null,
            })
        }
        RowProjection::CountAll | RowProjection::Aggregate { .. } => {
            Err(GraphError::UnsupportedQuery {
                dialect: "OpenCypher",
                feature: "aggregate projection cannot be used as a group key".to_string(),
            })
        }
    }
}

#[cfg(feature = "opencypher")]
fn expression_query_value(
    row: &BindingRow,
    expression: &RowExpression,
) -> Result<Option<QueryValue>> {
    Ok(match expression {
        RowExpression::NodeId { binding } => row.get_optional(binding)?.map(QueryValue::VertexId),
        RowExpression::Property { binding, property } => {
            binding_property(row, binding, property)?.map(QueryValue::Property)
        }
        RowExpression::Literal(value) => Some(QueryValue::Property(value.clone())),
    })
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
            RowSortExpression::NodeId { binding } => match binding_row.get_optional(binding)? {
                Some(value) => QueryValue::VertexId(value),
                None => QueryValue::Null,
            },
            RowSortExpression::Property { binding, property } => {
                match binding_property(binding_row, binding, property)? {
                    Some(value) => QueryValue::Property(value),
                    None => QueryValue::Null,
                }
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
                let Some(value) = row
                    .values
                    .iter()
                    .find(|value| matches!(value, QueryValue::Count(_)))
                else {
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
#[derive(Clone, Copy)]
struct EncodedPropertyIndexScan<'a> {
    cell_id: &'a str,
    edge_type: &'a str,
    property: &'a str,
    encoded: &'a str,
    read_epoch: GraphEpoch,
    budget: &'a QueryBudget,
}

#[cfg(feature = "opencypher")]
fn compare_query_values(left: &QueryValue, right: &QueryValue) -> std::cmp::Ordering {
    match (left, right) {
        (QueryValue::Null, QueryValue::Null) => std::cmp::Ordering::Equal,
        (QueryValue::VertexId(left), QueryValue::VertexId(right))
        | (QueryValue::VertexId(left), QueryValue::Count(right))
        | (QueryValue::Count(left), QueryValue::VertexId(right))
        | (QueryValue::Count(left), QueryValue::Count(right)) => left.cmp(right),
        (QueryValue::Float(left), QueryValue::Float(right)) => left.cmp(right),
        (QueryValue::Property(left), QueryValue::Property(right)) => {
            compare_vertex_property_order(left, right)
        }
        (QueryValue::List(left), QueryValue::List(right)) => {
            for (left, right) in left.iter().zip(right.iter()) {
                let ordering = compare_query_values(left, right);
                if ordering != std::cmp::Ordering::Equal {
                    return ordering;
                }
            }
            left.len().cmp(&right.len())
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
    if let Some(ordering) = numeric_property_order(left, right) {
        return ordering;
    }
    match (left, right) {
        (VertexPropertyValue::Bool(left), VertexPropertyValue::Bool(right)) => left.cmp(right),
        (VertexPropertyValue::String(left), VertexPropertyValue::String(right)) => left.cmp(right),
        _ => vertex_property_rank(left).cmp(&vertex_property_rank(right)),
    }
}

#[cfg(feature = "opencypher")]
fn query_value_rank(value: &QueryValue) -> u8 {
    match value {
        QueryValue::Bool(_) => 1,
        QueryValue::VertexId(_) | QueryValue::Count(_) => 2,
        QueryValue::Float(_) => 3,
        QueryValue::Property(value) => 4 + vertex_property_rank(value),
        QueryValue::List(_) => 8,
        QueryValue::Null => u8::MAX,
    }
}

#[cfg(feature = "opencypher")]
fn vertex_property_rank(value: &VertexPropertyValue) -> u8 {
    match value {
        VertexPropertyValue::Bool(_) => 0,
        VertexPropertyValue::Integer(_) | VertexPropertyValue::Float(_) => 1,
        VertexPropertyValue::String(_) => 2,
    }
}

#[cfg(feature = "opencypher")]
fn equivalent_property_index_keys(value: &VertexPropertyValue) -> Vec<String> {
    let mut keys = BTreeSet::new();
    keys.insert(encode_vertex_property_value_key(value));
    match value {
        VertexPropertyValue::Integer(value) => {
            if let Some(float) = VertexPropertyValue::exact_f64_from_u64(*value) {
                insert_float_property_index_key(&mut keys, float);
                if *value == 0 {
                    insert_float_property_index_key(&mut keys, -0.0);
                }
            }
        }
        VertexPropertyValue::Float(value) => {
            if value.0 == 0.0 {
                insert_float_property_index_key(&mut keys, 0.0);
                insert_float_property_index_key(&mut keys, -0.0);
            }
            if let Some(integer) = VertexPropertyValue::exact_u64_from_f64(value.0) {
                keys.insert(encode_vertex_property_value_key(
                    &VertexPropertyValue::Integer(integer),
                ));
            }
        }
        VertexPropertyValue::Bool(_) | VertexPropertyValue::String(_) => {}
    }
    keys.into_iter().collect()
}

#[cfg(feature = "opencypher")]
fn insert_float_property_index_key(keys: &mut BTreeSet<String>, value: f64) {
    keys.insert(encode_vertex_property_value_key(
        &VertexPropertyValue::Float(QueryFloat(value)),
    ));
}

#[cfg(feature = "opencypher")]
fn numeric_property_order(
    left: &VertexPropertyValue,
    right: &VertexPropertyValue,
) -> Option<std::cmp::Ordering> {
    match (left, right) {
        (VertexPropertyValue::Integer(left), VertexPropertyValue::Integer(right)) => {
            Some(left.cmp(right))
        }
        (VertexPropertyValue::Float(left), VertexPropertyValue::Float(right)) => {
            Some(compare_f64_numeric(left.0, right.0))
        }
        (VertexPropertyValue::Integer(left), VertexPropertyValue::Float(right)) => {
            Some(compare_u64_f64(*left, right.0))
        }
        (VertexPropertyValue::Float(left), VertexPropertyValue::Integer(right)) => {
            Some(compare_u64_f64(*right, left.0).reverse())
        }
        _ => None,
    }
}

#[cfg(feature = "opencypher")]
fn compare_f64_numeric(left: f64, right: f64) -> std::cmp::Ordering {
    if left == right {
        std::cmp::Ordering::Equal
    } else {
        left.total_cmp(&right)
    }
}

#[cfg(feature = "opencypher")]
fn compare_u64_f64(left: u64, right: f64) -> std::cmp::Ordering {
    const U64_EXCLUSIVE_UPPER: f64 = 18446744073709551616.0;
    if right.is_nan() {
        return (left as f64).total_cmp(&right);
    }
    if right < 0.0 {
        return std::cmp::Ordering::Greater;
    }
    if right >= U64_EXCLUSIVE_UPPER {
        return std::cmp::Ordering::Less;
    }
    let floor = right.floor();
    let floor_integer = floor as u64;
    match left.cmp(&floor_integer) {
        std::cmp::Ordering::Equal if floor == right => std::cmp::Ordering::Equal,
        std::cmp::Ordering::Equal => std::cmp::Ordering::Less,
        ordering => ordering,
    }
}
