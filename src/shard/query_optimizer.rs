use super::*;

#[cfg(feature = "opencypher")]
use super::query::row_predicate_relationship_property_constraint;

#[cfg(feature = "opencypher")]
use tracing::Instrument as _;

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
        read_epoch: StorageSequence,
        query: &ParsedRowQuery,
    ) -> Result<RowQueryPlan> {
        let span = query_plan_span(cell_id, read_epoch);
        let started = std::time::Instant::now();
        let plan = async {
            let groups = self
                .optimized_row_query_groups(
                    cell_id,
                    &query.patterns,
                    &query.pattern_groups,
                    read_epoch,
                )
                .await?;
            let mut union_arms = Vec::with_capacity(query.union_arms.len());
            for arm in &query.union_arms {
                union_arms.push(
                    Box::pin(self.explain_row_query_plan_with_stats(cell_id, read_epoch, arm))
                        .await?,
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
        .instrument(span.clone())
        .await;
        match &plan {
            Ok(plan) => {
                RowQueryPlanSummary::from_plan(plan).record(&span, cell_id, started.elapsed())
            }
            Err(err) => record_plan_error(&span, err),
        }
        plan
    }

    pub(crate) async fn optimize_row_match_groups_with_stats(
        &self,
        cell_id: &str,
        groups: &[RowMatchGroup],
        read_epoch: StorageSequence,
    ) -> Result<Vec<RowMatchGroup>> {
        let span = query_plan_span(cell_id, read_epoch);
        let started = std::time::Instant::now();
        let planned = self
            .optimize_row_match_group_plans_with_stats(cell_id, groups, read_epoch)
            .instrument(span.clone())
            .await;
        let planned = match planned {
            Ok(planned) => planned,
            Err(err) => {
                record_plan_error(&span, &err);
                return Err(err);
            }
        };
        RowQueryPlanSummary::from_groups(planned.iter().map(|group| &group.plan)).record(
            &span,
            cell_id,
            started.elapsed(),
        );
        Ok(planned.into_iter().map(|group| group.group).collect())
    }

    pub(crate) async fn optimize_row_patterns_with_stats(
        &self,
        cell_id: &str,
        patterns: &[RowPattern],
        read_epoch: StorageSequence,
        initial_bindings: &BTreeSet<String>,
    ) -> Result<Vec<RowPattern>> {
        let span = query_plan_span(cell_id, read_epoch);
        let started = std::time::Instant::now();
        let planned = self
            .optimize_row_pattern_plans_with_stats(cell_id, patterns, read_epoch, initial_bindings)
            .instrument(span.clone())
            .await;
        let planned = match planned {
            Ok(planned) => planned,
            Err(err) => {
                record_plan_error(&span, &err);
                return Err(err);
            }
        };
        RowQueryPlanSummary::from_patterns(planned.iter().map(|pattern| &pattern.plan)).record(
            &span,
            cell_id,
            started.elapsed(),
        );
        Ok(planned.into_iter().map(|pattern| pattern.pattern).collect())
    }

    async fn optimized_row_query_groups(
        &self,
        cell_id: &str,
        patterns: &[RowPattern],
        groups: &[RowMatchGroup],
        read_epoch: StorageSequence,
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
        read_epoch: StorageSequence,
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

            let mut optimized_patterns = self
                .optimize_row_pattern_plans_with_stats(
                    cell_id,
                    &group.patterns,
                    read_epoch,
                    &available_bindings,
                )
                .await?;
            self.apply_relationship_predicate_index_plan(
                cell_id,
                read_epoch,
                group.predicate.as_ref(),
                &mut optimized_patterns,
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
        read_epoch: StorageSequence,
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

    async fn apply_relationship_predicate_index_plan(
        &self,
        cell_id: &str,
        read_epoch: StorageSequence,
        predicate: Option<&RowPredicate>,
        patterns: &mut [OptimizedRowPattern],
    ) -> Result<()> {
        let Some(constraint) = predicate.and_then(row_predicate_relationship_property_constraint)
        else {
            return Ok(());
        };
        if read_epoch != self.current_epoch(cell_id).await? {
            return Ok(());
        }
        let Some(pattern) = patterns.iter_mut().find(|pattern| {
            matches!(
                (&pattern.pattern, &pattern.plan.access),
                (
                    RowPattern::Edge(edge),
                    RowQueryAccess::FullEdgeScan { .. }
                ) if edge.binding.as_deref() == Some(constraint.binding.as_str())
                    && edge.hop_range.is_none()
                    && edge.properties.is_empty()
            )
        }) else {
            return Ok(());
        };
        let RowPattern::Edge(edge) = &pattern.pattern else {
            return Ok(());
        };

        let mut estimate = 0_u64;
        for value in &constraint.values {
            let encoded = encode_vertex_property_value_key(value);
            estimate = estimate.saturating_add(
                self.query_stats_estimate(
                    cell_id,
                    &keys::query_stats_edge_property(
                        cell_id,
                        &edge.edge_type,
                        &constraint.property,
                        &encoded,
                    ),
                    Some(&keys::query_stats_edge_property_histogram(
                        cell_id,
                        &edge.edge_type,
                        &constraint.property,
                    )),
                    16,
                )
                .await?
                .unwrap_or(16),
            );
        }
        pattern.plan.access = RowQueryAccess::EdgePropertyIndex {
            edge_type: edge.edge_type.clone(),
            property: constraint.property,
        };
        pattern.plan.estimated_cardinality = estimate.max(1);
        pattern
            .plan
            .optimizer_passes
            .retain(|pass| !matches!(pass, RowQueryOptimizerPass::FullScanFallback));
        pattern
            .plan
            .optimizer_passes
            .push(RowQueryOptimizerPass::UtilizeEdgeIndex);
        Ok(())
    }

    async fn optimize_row_pattern_plans_with_stats(
        &self,
        cell_id: &str,
        patterns: &[RowPattern],
        read_epoch: StorageSequence,
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
        read_epoch: StorageSequence,
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
            )
            .with_pass(RowQueryOptimizerPass::GraphKernel));
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
    current_epoch: StorageSequence,
    fallback: u64,
) -> u64 {
    let base = record.count.max(1);
    if record.is_unusable_at(current_epoch, graph_now_millis()) {
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

// ---------------------------------------------------------------------------
// query.plan telemetry
//
// The planner already computes everything an operator needs to explain a slow
// query — access path, optimizer passes, cardinality estimate — and then throws
// it away: `optimize_row_match_groups_with_stats` and
// `optimize_row_patterns_with_stats` both build a full plan and return only the
// rewritten patterns, and `RowQueryPlan` never leaves the shard. Summarising it
// onto the span before it is dropped is what turns "a query timed out" into
// "a query timed out on FullEdgeScan over RELATES".
// ---------------------------------------------------------------------------

/// Slow-query threshold in milliseconds, from `GRAPH_SLOW_QUERY_MS`.
///
/// Read once. The value is a logging threshold, not a limit, so a process that
/// starts before the variable is set simply logs at the default.
#[cfg(feature = "opencypher")]
fn slow_query_threshold_ms() -> u64 {
    static THRESHOLD_MS: std::sync::OnceLock<u64> = std::sync::OnceLock::new();
    *THRESHOLD_MS.get_or_init(|| {
        std::env::var("GRAPH_SLOW_QUERY_MS")
            .ok()
            .and_then(|value| value.trim().parse::<u64>().ok())
            .unwrap_or(1_000)
    })
}

/// A `RowQueryPlan` reduced to the handful of bounded-cardinality strings that
/// belong on a span.
///
/// Every field here is schema-derived — access-path variants, edge types, label
/// and property *names*, pass names. No parameter or property *value* is ever
/// admitted, per §2 of the telemetry plan.
#[cfg(feature = "opencypher")]
#[derive(Default)]
pub(crate) struct RowQueryPlanSummary {
    access_paths: Vec<String>,
    optimizer_passes: BTreeSet<&'static str>,
    /// The largest cardinality any single planned unit is expected to produce.
    /// A sum would double-count joined groups and a product would be dominated
    /// by an arbitrary estimate; the maximum is the number that predicts which
    /// plan blows up.
    rows_estimated: u64,
    full_scan: bool,
}

#[cfg(feature = "opencypher")]
impl RowQueryPlanSummary {
    fn absorb_pattern(&mut self, pattern: &RowQueryPlanPattern) {
        self.access_paths.push(access_path_label(&pattern.access));
        self.full_scan |= access_is_full_scan(&pattern.access);
        self.rows_estimated = self.rows_estimated.max(pattern.estimated_cardinality);
        for pass in &pattern.optimizer_passes {
            self.absorb_pass(pass);
        }
    }

    fn absorb_group(&mut self, group: &RowQueryPlanGroup) {
        self.rows_estimated = self.rows_estimated.max(group.estimated_cardinality);
        for pass in &group.optimizer_passes {
            self.absorb_pass(pass);
        }
        for pattern in &group.patterns {
            self.absorb_pattern(pattern);
        }
    }

    fn absorb_plan(&mut self, plan: &RowQueryPlan) {
        for group in &plan.groups {
            self.absorb_group(group);
        }
        for arm in &plan.union_arms {
            self.absorb_plan(arm);
        }
    }

    fn absorb_pass(&mut self, pass: &RowQueryOptimizerPass) {
        if matches!(pass, RowQueryOptimizerPass::FullScanFallback) {
            self.full_scan = true;
        }
        self.optimizer_passes.insert(optimizer_pass_label(pass));
    }

    fn from_groups<'a>(groups: impl IntoIterator<Item = &'a RowQueryPlanGroup>) -> Self {
        let mut summary = Self::default();
        for group in groups {
            summary.absorb_group(group);
        }
        summary
    }

    fn from_patterns<'a>(patterns: impl IntoIterator<Item = &'a RowQueryPlanPattern>) -> Self {
        let mut summary = Self::default();
        for pattern in patterns {
            summary.absorb_pattern(pattern);
        }
        summary
    }

    fn from_plan(plan: &RowQueryPlan) -> Self {
        let mut summary = Self::default();
        summary.absorb_plan(plan);
        summary
    }

    fn access_paths(&self) -> String {
        self.access_paths.join(",")
    }

    fn optimizer_passes(&self) -> String {
        self.optimizer_passes
            .iter()
            .copied()
            .collect::<Vec<_>>()
            .join(",")
    }

    /// Stamp the summary onto `span` and, if the plan or the elapsed time asks
    /// for it, emit the structured slow-query / bad-plan WARN.
    ///
    /// Two independent triggers, per §3 of the telemetry plan. The elapsed-time
    /// one is the obvious one; the full-scan one is the one that pays, because
    /// it catches a bad plan on a small graph *before* it becomes a timeout on
    /// a large one. A full-scanning query that returns in 3ms today is a
    /// 30-second timeout after the tenant grows, and only that rule sees it
    /// coming.
    fn record(&self, span: &tracing::Span, cell_id: &str, elapsed: std::time::Duration) {
        let access_paths = self.access_paths();
        let optimizer_passes = self.optimizer_passes();
        span.record("turbolay.query.access_path", access_paths.as_str());
        span.record("turbolay.query.optimizer_passes", optimizer_passes.as_str());
        span.record("turbolay.query.rows_estimated", self.rows_estimated);
        span.record("turbolay.query.full_scan", self.full_scan);
        if self.full_scan {
            // Not a head-sampling decision and cannot be made into one: the
            // verdict does not exist until the planner has run, and `query.plan`
            // is a child of `client.query`, whose fate was settled when the
            // request arrived. This marks the trace for the collector's tail
            // sampler — see `turbolay_telemetry::sampling`.
            span.record("turbolay.sampling.tail_keep", "full_scan");
        }

        let elapsed_ms = elapsed.as_millis() as u64;
        let slow = elapsed_ms >= slow_query_threshold_ms();
        if !slow && !self.full_scan {
            return;
        }
        tracing::warn!(
            turbolay.cell_id = %cell_id,
            turbolay.query.access_path = %access_paths,
            turbolay.query.optimizer_passes = %optimizer_passes,
            turbolay.query.rows_estimated = self.rows_estimated,
            turbolay.query.full_scan = self.full_scan,
            planning_elapsed_ms = elapsed_ms,
            reason = if self.full_scan { "full_scan" } else { "slow" },
            "query plan warrants attention"
        );
    }
}

/// A bounded, schema-derived label for one access path.
///
/// The qualifier matters: `FullEdgeScan` alone says a query scanned;
/// `FullEdgeScan:RELATES` says which relationship to go index. Edge types,
/// labels and property names are all schema, so they are bounded and safe as
/// span attributes — property *values* are not, and never appear here.
#[cfg(feature = "opencypher")]
fn access_path_label(access: &RowQueryAccess) -> String {
    match access {
        RowQueryAccess::VertexIdSeek => "VertexIdSeek".to_string(),
        RowQueryAccess::VertexPropertyIndex { property } => {
            format!("VertexPropertyIndex:{property}")
        }
        RowQueryAccess::VertexLabelScan { label } => format!("VertexLabelScan:{label}"),
        RowQueryAccess::AllVertexScan => "AllVertexScan".to_string(),
        RowQueryAccess::BoundOutExpand { edge_type } => format!("BoundOutExpand:{edge_type}"),
        RowQueryAccess::BoundInExpand { edge_type } => format!("BoundInExpand:{edge_type}"),
        RowQueryAccess::ExpandInto { edge_type } => format!("ExpandInto:{edge_type}"),
        RowQueryAccess::EdgePropertyIndex {
            edge_type,
            property,
        } => format!("EdgePropertyIndex:{edge_type}.{property}"),
        RowQueryAccess::FullEdgeScan { edge_type } => format!("FullEdgeScan:{edge_type}"),
        RowQueryAccess::VariableLengthExpand {
            edge_type,
            min_hops,
            max_hops,
        } => format!("VariableLengthExpand:{edge_type}:{min_hops}..{max_hops}"),
    }
}

#[cfg(feature = "opencypher")]
fn access_is_full_scan(access: &RowQueryAccess) -> bool {
    matches!(
        access,
        RowQueryAccess::AllVertexScan | RowQueryAccess::FullEdgeScan { .. }
    )
}

#[cfg(feature = "opencypher")]
fn optimizer_pass_label(pass: &RowQueryOptimizerPass) -> &'static str {
    match pass {
        RowQueryOptimizerPass::UtilizeVertexIndex => "UtilizeVertexIndex",
        RowQueryOptimizerPass::UtilizeEdgeIndex => "UtilizeEdgeIndex",
        RowQueryOptimizerPass::CostBasedLabelScan => "CostBasedLabelScan",
        RowQueryOptimizerPass::ConnectivityOrder => "ConnectivityOrder",
        RowQueryOptimizerPass::JoinOrder => "JoinOrder",
        RowQueryOptimizerPass::ExpandInto => "ExpandInto",
        RowQueryOptimizerPass::GraphKernel => "GraphKernel",
        RowQueryOptimizerPass::ReverseExpand => "ReverseExpand",
        RowQueryOptimizerPass::FullScanFallback => "FullScanFallback",
        RowQueryOptimizerPass::PreserveOptionalBoundary => "PreserveOptionalBoundary",
    }
}

/// Record a planning failure on the `query.plan` span itself.
///
/// The innermost span that produced the error is the one that carries it: the
/// same failure surfacing at `client.query` says a query failed, while here it
/// says planning failed, which is a different bug.
#[cfg(feature = "opencypher")]
fn record_plan_error(span: &tracing::Span, err: &GraphError) {
    span.record("error.class", err.class());
    span.record("turbolay.sampling.tail_keep", "error");
}

/// The `query.plan` span, with every recorded field declared empty up front —
/// `Span::record` only accepts fields the callsite already knows about.
#[cfg(feature = "opencypher")]
fn query_plan_span(cell_id: &str, read_epoch: StorageSequence) -> tracing::Span {
    tracing::info_span!(
        "query.plan",
        turbolay.cell_id = %cell_id,
        turbolay.read_epoch = read_epoch,
        turbolay.query.access_path = tracing::field::Empty,
        turbolay.query.optimizer_passes = tracing::field::Empty,
        turbolay.query.rows_estimated = tracing::field::Empty,
        turbolay.query.full_scan = tracing::field::Empty,
        turbolay.sampling.tail_keep = tracing::field::Empty,
        error.class = tracing::field::Empty,
    )
}

#[cfg(feature = "opencypher")]
#[cfg(test)]
mod tests {
    use super::*;

    fn plan_pattern(
        access: RowQueryAccess,
        passes: Vec<RowQueryOptimizerPass>,
    ) -> RowQueryPlanPattern {
        RowQueryPlanPattern {
            original_index: 0,
            access,
            estimated_cardinality: 7,
            bindings: Vec::new(),
            optimizer_passes: passes,
        }
    }

    /// Rule 2 of the slow-query/bad-plan warn is the one that pays, so it is
    /// the one worth pinning: the alarm bit is a property of the plan, not of
    /// how long the plan took to build.
    #[test]
    fn full_scan_alarm_bit_is_set_by_plan_shape_alone() {
        let full_edge_scan = RowQueryPlanSummary::from_patterns(&[plan_pattern(
            RowQueryAccess::FullEdgeScan {
                edge_type: "RELATES".to_string(),
            },
            Vec::new(),
        )]);
        assert!(full_edge_scan.full_scan);
        assert_eq!(full_edge_scan.access_paths(), "FullEdgeScan:RELATES");

        let all_vertex_scan = RowQueryPlanSummary::from_patterns(&[plan_pattern(
            RowQueryAccess::AllVertexScan,
            Vec::new(),
        )]);
        assert!(all_vertex_scan.full_scan);

        let fallback_pass = RowQueryPlanSummary::from_patterns(&[plan_pattern(
            RowQueryAccess::VertexIdSeek,
            vec![RowQueryOptimizerPass::FullScanFallback],
        )]);
        assert!(fallback_pass.full_scan);

        let indexed = RowQueryPlanSummary::from_patterns(&[plan_pattern(
            RowQueryAccess::ExpandInto {
                edge_type: "RELATES".to_string(),
            },
            vec![RowQueryOptimizerPass::UtilizeEdgeIndex],
        )]);
        assert!(!indexed.full_scan);
        assert_eq!(indexed.optimizer_passes(), "UtilizeEdgeIndex");
    }

    /// A `tracing` subscriber that remembers which span fields were filled in
    /// after the span was created, and with what.
    ///
    /// Hand-rolled against the `tracing` facade rather than built on
    /// `tracing-subscriber`, which is deliberately not a dependency of this
    /// library — see the note on it in the root manifest. Only the four methods
    /// the trait requires and `record` do anything.
    struct RecordedFields(std::sync::Arc<std::sync::Mutex<Vec<(String, String)>>>);

    impl tracing::field::Visit for RecordedFields {
        fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
            self.0
                .lock()
                .expect("field capture mutex is not poisoned")
                .push((field.name().to_string(), format!("{value:?}")));
        }

        fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
            self.0
                .lock()
                .expect("field capture mutex is not poisoned")
                .push((field.name().to_string(), value.to_string()));
        }
    }

    impl tracing::Subscriber for RecordedFields {
        fn enabled(&self, _metadata: &tracing::Metadata<'_>) -> bool {
            true
        }

        fn new_span(&self, _span: &tracing::span::Attributes<'_>) -> tracing::span::Id {
            tracing::span::Id::from_u64(1)
        }

        fn record(&self, _span: &tracing::span::Id, values: &tracing::span::Record<'_>) {
            values.record(&mut RecordedFields(self.0.clone()));
        }

        fn record_follows_from(&self, _span: &tracing::span::Id, _follows: &tracing::span::Id) {}

        fn event(&self, _event: &tracing::Event<'_>) {}

        fn enter(&self, _span: &tracing::span::Id) {}

        fn exit(&self, _span: &tracing::span::Id) {}
    }

    /// The full-scan alarm must reach the span as the *tail* sampling marker.
    ///
    /// This drives the real function, on the real `query.plan` span, in the
    /// order production uses: the span is created with the field empty, entered,
    /// and only then filled in. That ordering is precisely why the head sampler
    /// cannot act on it — it decided when the span started — so recording
    /// `turbolay.sampling.force` here would be a silent no-op dressed up as a
    /// guarantee. The marker the collector's tail sampler reads is the one that
    /// can still change the outcome, and it is the one that has to be here.
    #[test]
    fn a_full_scan_plan_marks_the_trace_for_the_tail_sampler() {
        let captured = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let subscriber = RecordedFields(captured.clone());

        tracing::subscriber::with_default(subscriber, || {
            let span = query_plan_span("cell-7", 42);
            let entered = span.enter();
            RowQueryPlanSummary::from_patterns(&[plan_pattern(
                RowQueryAccess::AllVertexScan,
                Vec::new(),
            )])
            .record(&span, "cell-7", std::time::Duration::from_millis(1));
            drop(entered);
        });

        let recorded = captured
            .lock()
            .expect("field capture mutex is not poisoned");
        assert!(
            recorded
                .iter()
                .any(|(name, value)| name == "turbolay.sampling.tail_keep" && value == "full_scan"),
            "the full-scan alarm did not mark the trace for the tail sampler: {recorded:?}"
        );
        assert!(
            !recorded
                .iter()
                .any(|(name, _)| name == "turbolay.sampling.force"),
            "the head-sampling key was recorded after the span started, where \
             it can never be read: {recorded:?}"
        );
    }

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
