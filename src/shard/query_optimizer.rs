use super::*;

#[cfg(feature = "opencypher")]
#[derive(Clone, Debug)]
pub(crate) struct OptimizedRowMatchGroup {
    pub(crate) group: RowMatchGroup,
    pub(crate) plan: RowQueryPlanGroup,
}

#[cfg(feature = "opencypher")]
#[derive(Clone, Debug)]
struct OptimizedRowPattern {
    pattern: RowPattern,
    plan: RowQueryPlanPattern,
}

#[cfg(feature = "opencypher")]
impl GraphShard {
    pub(crate) async fn explain_row_query_plan_with_stats(
        &self,
        cell_id: &str,
        read_epoch: GraphEpoch,
        query: &ParsedRowQuery,
    ) -> Result<RowQueryPlan> {
        let groups = self
            .optimized_row_query_groups(cell_id, &query.patterns, &query.pattern_groups, read_epoch)
            .await?;
        let mut union_arms = Vec::with_capacity(query.union_arms.len());
        for arm in &query.union_arms {
            union_arms.push(
                Box::pin(self.explain_row_query_plan_with_stats(cell_id, read_epoch, arm)).await?,
            );
        }
        Ok(RowQueryPlan {
            cell_id: cell_id.to_string(),
            read_epoch,
            columns: query.columns.clone(),
            groups: groups.into_iter().map(|group| group.plan).collect(),
            union_all: query.union_all,
            union_arms,
        })
    }

    pub(crate) async fn optimize_row_match_groups_with_stats(
        &self,
        cell_id: &str,
        groups: &[RowMatchGroup],
        read_epoch: GraphEpoch,
    ) -> Result<Vec<RowMatchGroup>> {
        Ok(self
            .optimize_row_match_group_plans_with_stats(cell_id, groups, read_epoch)
            .await?
            .into_iter()
            .map(|group| group.group)
            .collect())
    }

    pub(crate) async fn optimize_row_patterns_with_stats(
        &self,
        cell_id: &str,
        patterns: &[RowPattern],
        read_epoch: GraphEpoch,
        initial_bindings: &BTreeSet<String>,
    ) -> Result<Vec<RowPattern>> {
        Ok(self
            .optimize_row_pattern_plans_with_stats(cell_id, patterns, read_epoch, initial_bindings)
            .await?
            .into_iter()
            .map(|pattern| pattern.pattern)
            .collect())
    }

    async fn optimized_row_query_groups(
        &self,
        cell_id: &str,
        patterns: &[RowPattern],
        groups: &[RowMatchGroup],
        read_epoch: GraphEpoch,
    ) -> Result<Vec<OptimizedRowMatchGroup>> {
        if groups.is_empty() {
            let group = RowMatchGroup {
                patterns: patterns.to_vec(),
                predicate: None,
                optional: false,
            };
            return self
                .optimize_row_match_group_plans_with_stats(cell_id, &[group], read_epoch)
                .await;
        }
        self.optimize_row_match_group_plans_with_stats(cell_id, groups, read_epoch)
            .await
    }

    async fn optimize_row_match_group_plans_with_stats(
        &self,
        cell_id: &str,
        groups: &[RowMatchGroup],
        read_epoch: GraphEpoch,
    ) -> Result<Vec<OptimizedRowMatchGroup>> {
        let mut output = Vec::with_capacity(groups.len());
        let mut required_segment = Vec::<(usize, RowMatchGroup)>::new();
        let mut available_bindings = BTreeSet::new();

        for (idx, group) in groups.iter().cloned().enumerate() {
            if !group.optional && group.predicate.is_none() {
                required_segment.push((idx, group));
                continue;
            }
            self.flush_required_row_groups(
                cell_id,
                read_epoch,
                &mut required_segment,
                &mut available_bindings,
                &mut output,
            )
            .await?;

            let optimized_patterns = self
                .optimize_row_pattern_plans_with_stats(
                    cell_id,
                    &group.patterns,
                    read_epoch,
                    &available_bindings,
                )
                .await?;
            let mut plan = row_query_plan_group(&group, &optimized_patterns);
            plan.optimizer_passes
                .push(RowQueryOptimizerPass::PreserveOptionalBoundary);
            available_bindings.extend(row_match_group_bindings(&group));
            output.push(OptimizedRowMatchGroup {
                group: RowMatchGroup {
                    patterns: optimized_patterns
                        .into_iter()
                        .map(|pattern| pattern.pattern)
                        .collect(),
                    predicate: group.predicate,
                    optional: group.optional,
                },
                plan,
            });
        }

        self.flush_required_row_groups(
            cell_id,
            read_epoch,
            &mut required_segment,
            &mut available_bindings,
            &mut output,
        )
        .await?;
        Ok(output)
    }

    async fn flush_required_row_groups(
        &self,
        cell_id: &str,
        read_epoch: GraphEpoch,
        required_segment: &mut Vec<(usize, RowMatchGroup)>,
        available_bindings: &mut BTreeSet<String>,
        output: &mut Vec<OptimizedRowMatchGroup>,
    ) -> Result<()> {
        while !required_segment.is_empty() {
            let mut best = None::<(RowGroupChoice, OptimizedRowMatchGroup)>;
            for (position, (original_idx, group)) in required_segment.iter().enumerate() {
                let optimized_patterns = self
                    .optimize_row_pattern_plans_with_stats(
                        cell_id,
                        &group.patterns,
                        read_epoch,
                        available_bindings,
                    )
                    .await?;
                let mut plan = row_query_plan_group(group, &optimized_patterns);
                plan.optimizer_passes.push(RowQueryOptimizerPass::JoinOrder);
                let group_bindings = row_match_group_bindings(group);
                let disconnected = !available_bindings.is_empty()
                    && group_bindings.is_disjoint(available_bindings);
                let choice = RowGroupChoice {
                    disconnected,
                    estimated_cardinality: plan.estimated_cardinality,
                    original_idx: *original_idx,
                    position,
                };
                let candidate = OptimizedRowMatchGroup {
                    group: RowMatchGroup {
                        patterns: optimized_patterns
                            .into_iter()
                            .map(|pattern| pattern.pattern)
                            .collect(),
                        predicate: group.predicate.clone(),
                        optional: group.optional,
                    },
                    plan,
                };
                if match best.as_ref() {
                    Some((best_choice, _)) => choice < *best_choice,
                    None => true,
                } {
                    best = Some((choice, candidate));
                }
            }

            let Some((choice, group)) = best else {
                break;
            };
            let (_, original_group) = required_segment.remove(choice.position);
            available_bindings.extend(row_match_group_bindings(&original_group));
            output.push(group);
        }
        Ok(())
    }

    async fn optimize_row_pattern_plans_with_stats(
        &self,
        cell_id: &str,
        patterns: &[RowPattern],
        read_epoch: GraphEpoch,
        initial_bindings: &BTreeSet<String>,
    ) -> Result<Vec<OptimizedRowPattern>> {
        let current_epoch = self.current_epoch(cell_id).await?;
        let latest_snapshot = read_epoch == current_epoch;
        let reverse_index_available = self.writes_reverse_index() && latest_snapshot;
        let mut remaining: Vec<_> = patterns
            .iter()
            .cloned()
            .enumerate()
            .map(|(original_index, pattern)| RemainingRowPattern {
                original_index,
                pattern,
            })
            .collect();
        let mut bound = initial_bindings.clone();
        let mut output = Vec::with_capacity(patterns.len());

        while !remaining.is_empty() {
            let mut best = None::<(RowPatternChoice, OptimizedRowPattern)>;
            for (position, remaining_pattern) in remaining.iter().enumerate() {
                let mut optimized = self
                    .plan_row_pattern_with_stats(
                        cell_id,
                        reverse_index_available,
                        latest_snapshot,
                        &remaining_pattern.pattern,
                        remaining_pattern.original_index,
                        &bound,
                    )
                    .await?;
                let pattern_bindings = row_pattern_bindings(&remaining_pattern.pattern);
                let connected = !bound.is_empty() && !pattern_bindings.is_disjoint(&bound);
                if connected || !output.is_empty() {
                    optimized
                        .plan
                        .optimizer_passes
                        .push(RowQueryOptimizerPass::ConnectivityOrder);
                }
                let choice = RowPatternChoice {
                    disconnected: !output.is_empty() && !connected,
                    access_priority: access_priority(&optimized.plan.access),
                    estimated_cardinality: optimized.plan.estimated_cardinality,
                    original_index: remaining_pattern.original_index,
                    position,
                };
                if match best.as_ref() {
                    Some((best_choice, _)) => choice < *best_choice,
                    None => true,
                } {
                    best = Some((choice, optimized));
                }
            }

            let Some((choice, optimized)) = best else {
                break;
            };
            let removed = remaining.remove(choice.position);
            bound.extend(row_pattern_bindings(&removed.pattern));
            output.push(optimized);
        }
        Ok(output)
    }

    async fn plan_row_pattern_with_stats(
        &self,
        cell_id: &str,
        reverse_index_available: bool,
        latest_snapshot: bool,
        pattern: &RowPattern,
        original_index: usize,
        bound: &BTreeSet<String>,
    ) -> Result<OptimizedRowPattern> {
        let node_or_edge = match pattern {
            RowPattern::Node(node) => {
                self.plan_row_node_with_stats(cell_id, node, original_index, bound)
                    .await?
            }
            RowPattern::Edge(edge) => {
                self.plan_row_edge_with_stats(
                    cell_id,
                    reverse_index_available,
                    latest_snapshot,
                    edge,
                    original_index,
                    bound,
                )
                .await?
            }
        };
        Ok(OptimizedRowPattern {
            pattern: pattern.clone(),
            plan: node_or_edge,
        })
    }

    async fn plan_row_node_with_stats(
        &self,
        cell_id: &str,
        node: &RowNodePattern,
        original_index: usize,
        bound: &BTreeSet<String>,
    ) -> Result<RowQueryPlanPattern> {
        let access = self.best_row_node_access(cell_id, node, bound).await?;
        Ok(RowQueryPlanPattern {
            original_index,
            estimated_cardinality: access.estimated_cardinality,
            bindings: row_node_bindings(node).into_iter().collect(),
            optimizer_passes: access.passes,
            access: access.access,
        })
    }

    async fn plan_row_edge_with_stats(
        &self,
        cell_id: &str,
        reverse_index_available: bool,
        latest_snapshot: bool,
        edge: &RowEdgePattern,
        original_index: usize,
        bound: &BTreeSet<String>,
    ) -> Result<RowQueryPlanPattern> {
        let access = self
            .best_row_edge_access(
                cell_id,
                edge,
                reverse_index_available,
                latest_snapshot,
                bound,
            )
            .await?;
        Ok(RowQueryPlanPattern {
            original_index,
            estimated_cardinality: access.estimated_cardinality,
            bindings: row_edge_bindings(edge).into_iter().collect(),
            optimizer_passes: access.passes,
            access: access.access,
        })
    }

    pub(crate) async fn best_row_node_access_with_stats(
        &self,
        cell_id: &str,
        node: &RowNodePattern,
        bound: &BTreeSet<String>,
    ) -> Result<RowQueryAccess> {
        Ok(self
            .best_row_node_access(cell_id, node, bound)
            .await?
            .access)
    }

    pub(crate) async fn best_row_edge_access_with_stats(
        &self,
        cell_id: &str,
        edge: &RowEdgePattern,
        read_epoch: GraphEpoch,
        bound: &BTreeSet<String>,
    ) -> Result<RowQueryAccess> {
        let current_epoch = self.current_epoch(cell_id).await?;
        let latest_snapshot = read_epoch == current_epoch;
        let reverse_index_available = self.writes_reverse_index() && latest_snapshot;
        Ok(self
            .best_row_edge_access(
                cell_id,
                edge,
                reverse_index_available,
                latest_snapshot,
                bound,
            )
            .await?
            .access)
    }

    async fn best_row_node_access(
        &self,
        cell_id: &str,
        node: &RowNodePattern,
        bound: &BTreeSet<String>,
    ) -> Result<AccessEstimate> {
        if matches!(node.binding.as_ref(), Some(binding) if bound.contains(binding))
            || node.id.is_some()
        {
            return Ok(AccessEstimate::new(RowQueryAccess::VertexIdSeek, 1));
        }

        let mut best = None::<AccessEstimate>;
        for (property, value) in node
            .properties
            .iter()
            .filter(|(property, _)| property.as_str() != "id")
        {
            let encoded = encode_vertex_property_value_key(value);
            let estimate = self
                .query_stats_estimate(
                    cell_id,
                    &keys::query_stats_vertex_property(cell_id, property, &encoded),
                    Some(&keys::query_stats_vertex_property_histogram(
                        cell_id, property,
                    )),
                    8,
                )
                .await?
                .unwrap_or(8);
            choose_best_access(
                &mut best,
                AccessEstimate::new(
                    RowQueryAccess::VertexPropertyIndex {
                        property: property.clone(),
                    },
                    estimate,
                )
                .with_pass(RowQueryOptimizerPass::UtilizeVertexIndex),
            );
        }
        for label in &node.labels {
            let estimate = self
                .query_stats_estimate(
                    cell_id,
                    &keys::query_stats_vertex_label(cell_id, label),
                    None,
                    64,
                )
                .await?
                .unwrap_or(64);
            choose_best_access(
                &mut best,
                AccessEstimate::new(
                    RowQueryAccess::VertexLabelScan {
                        label: label.clone(),
                    },
                    estimate,
                )
                .with_pass(RowQueryOptimizerPass::CostBasedLabelScan),
            );
        }
        Ok(best.unwrap_or_else(|| {
            AccessEstimate::new(RowQueryAccess::AllVertexScan, 1_000_000)
                .with_pass(RowQueryOptimizerPass::FullScanFallback)
        }))
    }

    async fn best_row_edge_access(
        &self,
        cell_id: &str,
        edge: &RowEdgePattern,
        reverse_index_available: bool,
        latest_snapshot: bool,
        bound: &BTreeSet<String>,
    ) -> Result<AccessEstimate> {
        if let Some((min_hops, max_hops)) = edge.hop_range {
            let anchored = row_node_is_bound_or_seekable(&edge.src, bound);
            return Ok(AccessEstimate::new(
                RowQueryAccess::VariableLengthExpand {
                    edge_type: edge.edge_type.clone(),
                    min_hops,
                    max_hops,
                },
                if anchored { 10_000 } else { 2_000_000 },
            ));
        }

        let source = self.best_row_node_access(cell_id, &edge.src, bound).await?;
        let destination = self.best_row_node_access(cell_id, &edge.dst, bound).await?;
        let source_seekable = row_node_is_bound_or_seekable(&edge.src, bound)
            || endpoint_access_is_indexed(&source.access);
        let destination_seekable = row_node_is_bound_or_seekable(&edge.dst, bound)
            || endpoint_access_is_indexed(&destination.access);

        if row_node_is_bound_or_seekable(&edge.src, bound)
            && row_node_is_bound_or_seekable(&edge.dst, bound)
        {
            return Ok(AccessEstimate::new(
                RowQueryAccess::ExpandInto {
                    edge_type: edge.edge_type.clone(),
                },
                1,
            )
            .with_pass(RowQueryOptimizerPass::ExpandInto));
        }

        let mut best = None::<AccessEstimate>;
        if source_seekable {
            choose_best_access(
                &mut best,
                AccessEstimate::new(
                    RowQueryAccess::BoundOutExpand {
                        edge_type: edge.edge_type.clone(),
                    },
                    source.estimated_cardinality.saturating_add(4),
                ),
            );
        }
        if reverse_index_available && destination_seekable {
            choose_best_access(
                &mut best,
                AccessEstimate::new(
                    RowQueryAccess::BoundInExpand {
                        edge_type: edge.edge_type.clone(),
                    },
                    destination.estimated_cardinality.saturating_add(4),
                )
                .with_pass(RowQueryOptimizerPass::ReverseExpand),
            );
        }
        if latest_snapshot {
            for (property, value) in &edge.properties {
                let encoded = encode_vertex_property_value_key(value);
                let estimate = self
                    .query_stats_estimate(
                        cell_id,
                        &keys::query_stats_edge_property(
                            cell_id,
                            &edge.edge_type,
                            property,
                            &encoded,
                        ),
                        Some(&keys::query_stats_edge_property_histogram(
                            cell_id,
                            &edge.edge_type,
                            property,
                        )),
                        16,
                    )
                    .await?
                    .unwrap_or(16);
                choose_best_access(
                    &mut best,
                    AccessEstimate::new(
                        RowQueryAccess::EdgePropertyIndex {
                            edge_type: edge.edge_type.clone(),
                            property: property.clone(),
                        },
                        estimate,
                    )
                    .with_pass(RowQueryOptimizerPass::UtilizeEdgeIndex),
                );
            }
        }
        if let Some(best) = best {
            return Ok(best);
        }

        let estimate = self
            .query_stats_estimate(
                cell_id,
                &keys::query_stats_edge_type(cell_id, &edge.edge_type),
                None,
                1_000_000,
            )
            .await?
            .unwrap_or(1_000_000);
        Ok(AccessEstimate::new(
            RowQueryAccess::FullEdgeScan {
                edge_type: edge.edge_type.clone(),
            },
            estimate,
        )
        .with_pass(RowQueryOptimizerPass::FullScanFallback))
    }

    pub(crate) async fn query_stats_estimate(
        &self,
        cell_id: &str,
        key: &str,
        histogram_key: Option<&str>,
        fallback: u64,
    ) -> Result<Option<u64>> {
        let current_epoch = self.current_epoch(cell_id).await?;
        if let Some(record) = self.query_stats_record(key).await? {
            return Ok(Some(stats_record_cost_estimate(
                &record,
                current_epoch,
                fallback,
            )));
        }
        if let Some(histogram_key) = histogram_key {
            if let Some(record) = self.query_stats_record(histogram_key).await? {
                let estimate = record.equality_estimate();
                return Ok(Some(stats_record_cost_estimate(
                    &QueryStatsRecord {
                        count: estimate,
                        ..record
                    },
                    current_epoch,
                    fallback,
                )));
            }
        }
        Ok(None)
    }

    pub(crate) async fn query_stats_record(&self, key: &str) -> Result<Option<QueryStatsRecord>> {
        let record_key = keys::query_stats_record_key(key);
        if let Some(value) = self.read_remote(&record_key).await? {
            return decode_query_stats_record(&record_key, &value).map(Some);
        }
        match self.read_remote(key).await? {
            Some(value) => decode_query_stats_record(key, &value).map(Some),
            None => Ok(None),
        }
    }
}

#[cfg(feature = "opencypher")]
fn stats_record_cost_estimate(
    record: &QueryStatsRecord,
    current_epoch: GraphEpoch,
    fallback: u64,
) -> u64 {
    let base = record.count.max(1);
    if record.is_stale_at(current_epoch, graph_now_millis()) {
        return base.saturating_mul(4).max(fallback);
    }
    base
}

#[cfg(feature = "opencypher")]
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct RowGroupChoice {
    disconnected: bool,
    estimated_cardinality: u64,
    original_idx: usize,
    position: usize,
}

#[cfg(feature = "opencypher")]
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct RowPatternChoice {
    disconnected: bool,
    access_priority: u8,
    estimated_cardinality: u64,
    original_index: usize,
    position: usize,
}

#[cfg(feature = "opencypher")]
#[derive(Clone, Debug)]
struct RemainingRowPattern {
    original_index: usize,
    pattern: RowPattern,
}

#[cfg(feature = "opencypher")]
#[derive(Clone, Debug)]
struct AccessEstimate {
    access: RowQueryAccess,
    estimated_cardinality: u64,
    passes: Vec<RowQueryOptimizerPass>,
}

#[cfg(feature = "opencypher")]
impl AccessEstimate {
    fn new(access: RowQueryAccess, estimated_cardinality: u64) -> Self {
        Self {
            access,
            estimated_cardinality,
            passes: Vec::new(),
        }
    }

    fn with_pass(mut self, pass: RowQueryOptimizerPass) -> Self {
        self.passes.push(pass);
        self
    }
}

#[cfg(feature = "opencypher")]
fn row_query_plan_group(
    group: &RowMatchGroup,
    patterns: &[OptimizedRowPattern],
) -> RowQueryPlanGroup {
    RowQueryPlanGroup {
        optional: group.optional,
        has_predicate: group.predicate.is_some(),
        estimated_cardinality: patterns
            .first()
            .map_or(0, |pattern| pattern.plan.estimated_cardinality),
        patterns: patterns
            .iter()
            .map(|pattern| pattern.plan.clone())
            .collect(),
        optimizer_passes: Vec::new(),
    }
}

#[cfg(feature = "opencypher")]
fn choose_best_access(best: &mut Option<AccessEstimate>, candidate: AccessEstimate) {
    if match best.as_ref() {
        Some(best) => {
            (
                candidate.estimated_cardinality,
                access_priority(&candidate.access),
            ) < (best.estimated_cardinality, access_priority(&best.access))
        }
        None => true,
    } {
        *best = Some(candidate);
    }
}

#[cfg(feature = "opencypher")]
fn access_priority(access: &RowQueryAccess) -> u8 {
    match access {
        RowQueryAccess::ExpandInto { .. } => 0,
        RowQueryAccess::VertexIdSeek => 1,
        RowQueryAccess::BoundOutExpand { .. } | RowQueryAccess::BoundInExpand { .. } => 2,
        RowQueryAccess::VertexPropertyIndex { .. } | RowQueryAccess::EdgePropertyIndex { .. } => 3,
        RowQueryAccess::VertexLabelScan { .. } => 4,
        RowQueryAccess::VariableLengthExpand { .. } => 5,
        RowQueryAccess::FullEdgeScan { .. } | RowQueryAccess::AllVertexScan => 9,
    }
}

#[cfg(feature = "opencypher")]
fn endpoint_access_is_indexed(access: &RowQueryAccess) -> bool {
    matches!(
        access,
        RowQueryAccess::VertexIdSeek
            | RowQueryAccess::VertexPropertyIndex { .. }
            | RowQueryAccess::VertexLabelScan { .. }
    )
}

#[cfg(feature = "opencypher")]
fn row_node_is_bound_or_seekable(node: &RowNodePattern, bound: &BTreeSet<String>) -> bool {
    node.id.is_some() || matches!(node.binding.as_ref(), Some(binding) if bound.contains(binding))
}

#[cfg(feature = "opencypher")]
fn row_match_group_bindings(group: &RowMatchGroup) -> BTreeSet<String> {
    group
        .patterns
        .iter()
        .flat_map(row_pattern_bindings)
        .collect()
}

#[cfg(feature = "opencypher")]
fn row_pattern_bindings(pattern: &RowPattern) -> BTreeSet<String> {
    match pattern {
        RowPattern::Node(node) => row_node_bindings(node),
        RowPattern::Edge(edge) => row_edge_bindings(edge),
    }
}

#[cfg(feature = "opencypher")]
fn row_edge_bindings(edge: &RowEdgePattern) -> BTreeSet<String> {
    let mut bindings = row_node_bindings(&edge.src);
    bindings.extend(row_node_bindings(&edge.dst));
    if let Some(binding) = &edge.binding {
        bindings.insert(binding.clone());
    }
    bindings
}

#[cfg(feature = "opencypher")]
fn row_node_bindings(node: &RowNodePattern) -> BTreeSet<String> {
    node.binding.iter().cloned().collect()
}

#[cfg(feature = "opencypher")]
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn access_priority_prefers_expand_into_before_scans() {
        assert!(
            access_priority(&RowQueryAccess::ExpandInto {
                edge_type: "FOLLOWS".to_string()
            }) < access_priority(&RowQueryAccess::FullEdgeScan {
                edge_type: "FOLLOWS".to_string()
            })
        );
    }
}
