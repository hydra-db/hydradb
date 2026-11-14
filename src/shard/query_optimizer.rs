use super::*;

#[cfg(feature = "opencypher")]
use super::query::{ordered_string_vertex_index_spec, row_predicate_property_equality_constraints, RowPredicatePropertyEqualityConstraint};

#[cfg(feature = "opencypher")]
use crate::core::metrics::QueryPlanShapes;

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
            let mut groups = self
                .optimized_row_query_groups(cell_id, &query.patterns, &query.pattern_groups, read_epoch)
                .await?;
            if let Some(spec) = ordered_string_vertex_index_spec(query, query.window) {
                let estimated = query
                    .window
                    .skip
                    .saturating_add(u64::try_from(spec.limit).unwrap_or(u64::MAX))
                    .max(1);
                if let [group] = groups.as_mut_slice() {
                    if let [pattern] = group.plan.patterns.as_mut_slice() {
                        pattern.access = RowQueryAccess::VertexPropertyIndex {
                            property: spec.property.to_string(),
                        };
                        pattern.estimated_cardinality = estimated;
                        pattern
                            .optimizer_passes
                            .retain(|pass| !matches!(pass, RowQueryOptimizerPass::FullScanFallback));
                        if !pattern.optimizer_passes.contains(&RowQueryOptimizerPass::UtilizeVertexIndex) {
                            pattern.optimizer_passes.push(RowQueryOptimizerPass::UtilizeVertexIndex);
                        }
                        pattern.optimizer_passes.push(RowQueryOptimizerPass::OrderedLimitPushdown);
                        group.plan.estimated_cardinality = estimated;
                    }
                }
            }
            let mut union_arms = Vec::with_capacity(query.union_arms.len());
            for arm in &query.union_arms {
                union_arms.push(Box::pin(self.explain_row_query_plan_with_stats(cell_id, read_epoch, arm)).await?);
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
            Ok(plan) => RowQueryPlanSummary::from_plan(plan).record(&span, cell_id, started.elapsed()),
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
        let summary = RowQueryPlanSummary::from_groups(planned.iter().map(|group| &group.plan));
        summary.record(&span, cell_id, started.elapsed());
        // The one place a plan is counted. `optimize_row_patterns_with_stats`
        // re-plans each group once per input row inside the match loop, and
        // `explain_row_query_plan_with_stats` plans without executing; counting
        // either would make the denominator scale with result size or with
        // EXPLAIN traffic instead of with queries.
        self.operation_metrics.record_query_plan(summary.plan_shapes());
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
        RowQueryPlanSummary::from_patterns(planned.iter().map(|pattern| &pattern.plan)).record(&span, cell_id, started.elapsed());
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
            return self.optimize_row_match_group_plans_with_stats(cell_id, &[group], read_epoch).await;
        }
        self.optimize_row_match_group_plans_with_stats(cell_id, groups, read_epoch).await
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

        for (idx, mut group) in groups.iter().cloned().enumerate() {
            if !group.optional && group.predicate.is_none() {
                required_segment.push((idx, group));
                continue;
            }
            self.flush_required_row_groups(cell_id, read_epoch, &mut required_segment, &mut available_bindings, &mut output)
                .await?;

            // Fold the WHERE equality into the node pattern *before* planning
            // it. `best_row_node_access` reads only labels and properties, and
            // `candidate_vertex_ids` re-derives the access path from the
            // pattern again at execution time, so rewriting the plan alone
            // would change EXPLAIN and nothing else. Doing it first also means
            // the cardinality estimate below is the index's, not the scan's.
            let pushed_equality = push_down_vertex_equality_predicate(&mut group, &available_bindings);

            let mut optimized_patterns = self
                .optimize_row_pattern_plans_with_stats(cell_id, &group.patterns, read_epoch, &available_bindings)
                .await?;
            self.apply_relationship_predicate_index_plan(cell_id, read_epoch, group.predicate.as_ref(), &mut optimized_patterns)
                .await?;
            let mut plan = row_query_plan_group(&group, &optimized_patterns);
            plan.optimizer_passes.push(RowQueryOptimizerPass::PreserveOptionalBoundary);
            if pushed_equality {
                plan.optimizer_passes.push(RowQueryOptimizerPass::EqualityPredicatePushdown);
            }
            available_bindings.extend(row_match_group_bindings(&group));
            output.push(OptimizedRowMatchGroup {
                group: RowMatchGroup {
                    patterns: optimized_patterns.into_iter().map(|pattern| pattern.pattern).collect(),
                    predicate: group.predicate,
                    optional: group.optional,
                },
                plan,
            });
        }

        self.flush_required_row_groups(cell_id, read_epoch, &mut required_segment, &mut available_bindings, &mut output)
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
                    .optimize_row_pattern_plans_with_stats(cell_id, &group.patterns, read_epoch, available_bindings)
                    .await?;
                let mut plan = row_query_plan_group(group, &optimized_patterns);
                plan.optimizer_passes.push(RowQueryOptimizerPass::JoinOrder);
                let group_bindings = row_match_group_bindings(group);
                let disconnected = !available_bindings.is_empty() && group_bindings.is_disjoint(available_bindings);
                let choice = RowGroupChoice {
                    disconnected,
                    estimated_cardinality: plan.estimated_cardinality,
                    original_idx: *original_idx,
                    position,
                };
                let candidate = OptimizedRowMatchGroup {
                    group: RowMatchGroup {
                        patterns: optimized_patterns.into_iter().map(|pattern| pattern.pattern).collect(),
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
        let Some(predicate) = predicate else {
            return Ok(());
        };
        if read_epoch != self.current_epoch(cell_id).await? {
            return Ok(());
        }
        // Rank every relationship equality rather than depending on predicate
        // order. Execution uses the same estimator, keeping EXPLAIN and the
        // physical seed scan aligned when several indexed properties exist.
        let mut best = None::<(RowPredicatePropertyEqualityConstraint, usize, u64)>;
        for constraint in row_predicate_property_equality_constraints(predicate) {
            let Some(index) = patterns.iter().position(|pattern| {
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
                continue;
            };
            if !matches!(&patterns[index].pattern, RowPattern::Edge(_)) {
                continue;
            }
            let estimate = edge_property_equality_estimate(&constraint.values);
            let replace = best.as_ref().is_none_or(|(_, _, current_estimate)| estimate < *current_estimate);
            if replace {
                best = Some((constraint, index, estimate));
            }
        }
        let Some((constraint, index, estimate)) = best else {
            return Ok(());
        };
        let pattern = &mut patterns[index];
        let RowPattern::Edge(edge) = &pattern.pattern else {
            return Ok(());
        };

        pattern.plan.access = RowQueryAccess::EdgePropertyIndex {
            edge_type: edge.edge_type.clone(),
            property: constraint.property,
        };
        pattern.plan.estimated_cardinality = estimate.max(1);
        pattern
            .plan
            .optimizer_passes
            .retain(|pass| !matches!(pass, RowQueryOptimizerPass::FullScanFallback));
        pattern.plan.optimizer_passes.push(RowQueryOptimizerPass::UtilizeEdgeIndex);
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
        // Reverse adjacency is stored in SlateDB and can be read from the same
        // pinned snapshot as the rest of the query. It is not limited to the
        // latest epoch.
        let reverse_index_available = self.writes_reverse_index();
        let mut remaining: Vec<_> = patterns
            .iter()
            .cloned()
            .enumerate()
            .map(|(original_index, pattern)| RemainingRowPattern { original_index, pattern })
            .collect();
        let mut bound = initial_bindings.clone();
        let mut output = Vec::with_capacity(patterns.len());

        while !remaining.is_empty() {
            let mut best = None::<(RowPatternChoice, OptimizedRowPattern)>;
            for (position, remaining_pattern) in remaining.iter().enumerate() {
                let mut optimized = self
                    .plan_row_pattern_with_stats(
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
                    optimized.plan.optimizer_passes.push(RowQueryOptimizerPass::ConnectivityOrder);
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
        reverse_index_available: bool,
        latest_snapshot: bool,
        pattern: &RowPattern,
        original_index: usize,
        bound: &BTreeSet<String>,
    ) -> Result<OptimizedRowPattern> {
        let node_or_edge = match pattern {
            RowPattern::Node(node) => self.plan_row_node_with_stats(node, original_index, bound).await?,
            RowPattern::Edge(edge) => {
                self.plan_row_edge_with_stats(reverse_index_available, latest_snapshot, edge, original_index, bound)
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
        node: &RowNodePattern,
        original_index: usize,
        bound: &BTreeSet<String>,
    ) -> Result<RowQueryPlanPattern> {
        let access = self.best_row_node_access(node, bound).await?;
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
        reverse_index_available: bool,
        latest_snapshot: bool,
        edge: &RowEdgePattern,
        original_index: usize,
        bound: &BTreeSet<String>,
    ) -> Result<RowQueryPlanPattern> {
        let access = self
            .best_row_edge_access(edge, reverse_index_available, latest_snapshot, bound)
            .await?;
        Ok(RowQueryPlanPattern {
            original_index,
            estimated_cardinality: access.estimated_cardinality,
            bindings: row_edge_bindings(edge).into_iter().collect(),
            optimizer_passes: access.passes,
            access: access.access,
        })
    }

    pub(crate) async fn best_row_node_access_with_stats(&self, node: &RowNodePattern, bound: &BTreeSet<String>) -> Result<RowQueryAccess> {
        Ok(self.best_row_node_access(node, bound).await?.access)
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
        let reverse_index_available = self.writes_reverse_index();
        Ok(self
            .best_row_edge_access(edge, reverse_index_available, latest_snapshot, bound)
            .await?
            .access)
    }

    async fn best_row_node_access(&self, node: &RowNodePattern, bound: &BTreeSet<String>) -> Result<AccessEstimate> {
        if matches!(node.binding.as_ref(), Some(binding) if bound.contains(binding)) || node.id.is_some() {
            return Ok(AccessEstimate::new(RowQueryAccess::VertexIdSeek, 1));
        }

        // Fixed estimates: an indexed equality beats a label scan, and the
        // first candidate wins a tie.
        let properties = node
            .properties
            .iter()
            .filter(|(property, value)| property.as_str() != "id" && value.is_scalar_indexable())
            .map(|(property, _)| {
                AccessEstimate::new(
                    RowQueryAccess::VertexPropertyIndex {
                        property: property.clone(),
                    },
                    8,
                )
                .with_pass(RowQueryOptimizerPass::UtilizeVertexIndex)
            });
        let labels = node.labels.iter().map(|label| {
            AccessEstimate::new(RowQueryAccess::VertexLabelScan { label: label.clone() }, 64)
                .with_pass(RowQueryOptimizerPass::CostBasedLabelScan)
        });
        let mut best = None::<AccessEstimate>;
        for access in properties.chain(labels) {
            choose_best_access(&mut best, access);
        }
        Ok(best.unwrap_or_else(|| {
            AccessEstimate::new(RowQueryAccess::AllVertexScan, 1_000_000).with_pass(RowQueryOptimizerPass::FullScanFallback)
        }))
    }

    async fn best_row_edge_access(
        &self,
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

        let source = self.best_row_node_access(&edge.src, bound).await?;
        let destination = self.best_row_node_access(&edge.dst, bound).await?;
        let source_seekable = row_node_is_bound_or_seekable(&edge.src, bound) || endpoint_access_is_indexed(&source.access);
        let destination_seekable = row_node_is_bound_or_seekable(&edge.dst, bound) || endpoint_access_is_indexed(&destination.access);

        if row_node_is_bound_or_seekable(&edge.src, bound) && row_node_is_bound_or_seekable(&edge.dst, bound) {
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
            for (property, _) in edge.properties.iter().filter(|(_, value)| value.is_scalar_indexable()) {
                choose_best_access(
                    &mut best,
                    AccessEstimate::new(
                        RowQueryAccess::EdgePropertyIndex {
                            edge_type: edge.edge_type.clone(),
                            property: property.clone(),
                        },
                        16,
                    )
                    .with_pass(RowQueryOptimizerPass::UtilizeEdgeIndex),
                );
            }
        }
        if let Some(best) = best {
            return Ok(best);
        }

        Ok(AccessEstimate::new(
            RowQueryAccess::FullEdgeScan {
                edge_type: edge.edge_type.clone(),
            },
            1_000_000,
        )
        .with_pass(RowQueryOptimizerPass::FullScanFallback))
    }
}

/// An edge property equality's cost without statistics: one index seek per
/// accepted value.
#[cfg(feature = "opencypher")]
pub(crate) fn edge_property_equality_estimate(values: &[VertexPropertyValue]) -> u64 {
    16_u64.saturating_mul(values.len() as u64).max(1)
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
fn row_query_plan_group(group: &RowMatchGroup, patterns: &[OptimizedRowPattern]) -> RowQueryPlanGroup {
    RowQueryPlanGroup {
        optional: group.optional,
        has_predicate: group.predicate.is_some(),
        estimated_cardinality: patterns.first().map_or(0, |pattern| pattern.plan.estimated_cardinality),
        patterns: patterns.iter().map(|pattern| pattern.plan.clone()).collect(),
        optimizer_passes: Vec::new(),
    }
}

#[cfg(feature = "opencypher")]
fn choose_best_access(best: &mut Option<AccessEstimate>, candidate: AccessEstimate) {
    if match best.as_ref() {
        Some(best) => {
            (candidate.estimated_cardinality, access_priority(&candidate.access))
                < (best.estimated_cardinality, access_priority(&best.access))
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
        RowQueryAccess::VertexIdSeek | RowQueryAccess::VertexPropertyIndex { .. } | RowQueryAccess::VertexLabelScan { .. }
    )
}

#[cfg(feature = "opencypher")]
fn row_node_is_bound_or_seekable(node: &RowNodePattern, bound: &BTreeSet<String>) -> bool {
    node.id.is_some() || matches!(node.binding.as_ref(), Some(binding) if bound.contains(binding))
}

#[cfg(feature = "opencypher")]
fn row_match_group_bindings(group: &RowMatchGroup) -> BTreeSet<String> {
    group.patterns.iter().flat_map(row_pattern_bindings).collect()
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
pub(super) fn slow_query_threshold_ms() -> u64 {
    static THRESHOLD_MS: std::sync::OnceLock<u64> = std::sync::OnceLock::new();
    *THRESHOLD_MS.get_or_init(|| {
        std::env::var("GRAPH_SLOW_QUERY_MS")
            .ok()
            .and_then(|value| value.trim().parse::<u64>().ok())
            .unwrap_or(1_000)
    })
}

/// The estimated-row count at which a plan is worth a WARN on its own.
///
/// Dimensioned in rows rather than in access-path variants because the variant
/// test already exists and already missed the case that reached production: a
/// `VertexLabelScan` is not a full scan, so a plan that hydrated 13,008
/// vertices to return one row raised nothing at all. The estimate is the number
/// that saw it — 13,008 against 1 for the same query written with the property
/// in the pattern — and a rule written on the estimate catches the next shape
/// without having to name it first.
///
/// 10,000 is chosen to sit above the row counts a healthy plan reaches and
/// below the engine's own admission limits (`max_query_intermediate_rows` is
/// 250,000), so it fires while the query still succeeds. That is the whole
/// point: a plan estimating 13,008 rows returns in 40 ms on a small tenant and
/// times out after that tenant grows, and this is the rule that sees it coming.
#[cfg(feature = "opencypher")]
pub(super) fn wide_plan_threshold_rows() -> u64 {
    static THRESHOLD_ROWS: std::sync::OnceLock<u64> = std::sync::OnceLock::new();
    *THRESHOLD_ROWS.get_or_init(|| {
        std::env::var("GRAPH_WIDE_PLAN_ROWS")
            .ok()
            .and_then(|value| value.trim().parse::<u64>().ok())
            .unwrap_or(10_000)
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

    /// The four watched shapes, for [`GraphOperationalMetrics::record_query_plan`].
    ///
    /// Derived from the same fields the span reports, so a counter and a trace
    /// cannot disagree about what a plan contained. `access_paths` is scanned
    /// by prefix because its entries are qualified — `VertexLabelScan:Source`
    /// carries the label the operator needs on the span, and the counter wants
    /// only the variant.
    fn plan_shapes(&self) -> QueryPlanShapes {
        QueryPlanShapes {
            label_scan: self.has_access_kind("VertexLabelScan"),
            property_index: self.has_access_kind("VertexPropertyIndex"),
            full_scan: self.full_scan,
            equality_pushdown: self
                .optimizer_passes
                .contains(&optimizer_pass_label(&RowQueryOptimizerPass::EqualityPredicatePushdown)),
        }
    }

    fn has_access_kind(&self, kind: &str) -> bool {
        self.access_paths.iter().any(|path| path.split(':').next() == Some(kind))
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
        self.optimizer_passes.iter().copied().collect::<Vec<_>>().join(",")
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
        span.record("hydradb.query.access_path", access_paths.as_str());
        span.record("hydradb.query.optimizer_passes", optimizer_passes.as_str());
        span.record("hydradb.query.rows_estimated", self.rows_estimated);
        span.record("hydradb.query.full_scan", self.full_scan);

        // The two plan-shape rules, most specific first. `full_scan` names the
        // access-path variants that read everything; `wide_plan` catches the
        // ones that merely read far too much, which is the same failure without
        // a variant to match on.
        let plan_alarm = if self.full_scan {
            Some("full_scan")
        } else if self.rows_estimated >= wide_plan_threshold_rows() {
            Some("wide_plan")
        } else {
            None
        };
        if let Some(alarm) = plan_alarm {
            // Not a head-sampling decision and cannot be made into one: the
            // verdict does not exist until the planner has run, and `query.plan`
            // is a child of `client.query`, whose fate was settled when the
            // request arrived. This marks the trace for the collector's tail
            // sampler — see `hydradb_telemetry::sampling`.
            span.record("hydradb.sampling.tail_keep", alarm);
        }

        let elapsed_ms = elapsed.as_millis() as u64;
        let slow = elapsed_ms >= slow_query_threshold_ms();
        if !slow && plan_alarm.is_none() {
            return;
        }
        tracing::warn!(
            hydradb.cell_id = %cell_id,
            hydradb.query.access_path = %access_paths,
            hydradb.query.optimizer_passes = %optimizer_passes,
            hydradb.query.rows_estimated = self.rows_estimated,
            hydradb.query.full_scan = self.full_scan,
            planning_elapsed_ms = elapsed_ms,
            // The plan verdict outranks the clock: a slow *and* wide plan is
            // reported as wide, because that is the half an operator can fix.
            reason = plan_alarm.unwrap_or("slow"),
            "query plan warrants attention"
        );
    }
}

/// Fold a WHERE equality into the node pattern it constrains.
///
/// `MATCH (s:Source) WHERE s.app_external_id = 'x'` and
/// `MATCH (s:Source {app_external_id: 'x'})` mean the same thing, but only the
/// second is anchorable. `best_row_node_access` derives an access path from a
/// pattern's labels and properties; a predicate lives in
/// `RowMatchGroup.predicate` and never reaches it, so the WHERE form plans
/// `VertexLabelScan`, hydrates every vertex carrying the label and filters
/// afterwards. On 13,008 `:Source` vertices that is ~800x the indexed form with
/// no object store in the path — `examples/where_vs_inline_bench.rs` measures
/// it, and the vertex-equality predicate pushdown design
/// records the numbers.
///
/// The rewrite is exact rather than an approximation. Both forms end in
/// `compare_vertex_property_values(.., Eq, ..)` — the pattern through
/// `vertex_metadata_matches`, the predicate through `row_predicate_matches` —
/// and `scan_vertex_property_index_at` probes every encoding in
/// `equivalent_property_index_keys`, so a seek finds the same int/float
/// spellings the comparison accepts. The predicate stays in place and still
/// filters the matched rows, so a pattern this declines to touch costs
/// correctness nothing.
///
/// Returns whether any pattern was rewritten.
#[cfg(feature = "opencypher")]
fn push_down_vertex_equality_predicate(group: &mut RowMatchGroup, bound: &BTreeSet<String>) -> bool {
    let Some(predicate) = group.predicate.as_ref() else {
        return false;
    };

    let mut pushed = false;
    // Every constraint the predicate proves, not the first one it proves.
    // Taking the first made the plan depend on the order the terms were
    // written, and let a term this pass cannot use hide one it can. Pushing
    // all of them hands `best_row_node_access` the whole set, and it already
    // compares estimates across a pattern's properties and picks the most
    // selective.
    for constraint in row_predicate_property_equality_constraints(predicate) {
        // A node pattern carries one value per property, so `p = a OR p = b`
        // has nowhere to go. Anchoring on either half alone would drop the
        // other half's rows, and the surviving predicate cannot put back what
        // the seek never read. `continue`, not `return`: the terms beside it
        // may still be usable.
        let [value] = constraint.values.as_slice() else {
            continue;
        };
        // `id` is not a property here. `RowNodePattern.id` is its own field,
        // the index search filters the name out, and
        // `node_has_metadata_constraints` does not count it — so writing it
        // into the map would assert a metadata constraint that vertex records
        // do not carry. Unreachable from Cypher as it stands, because
        // `node_id_expression_binding` routes every `x.id` to
        // `RowExpression::NodeId` before a property constraint can be built;
        // it stays because the invariant is about the pattern, not about
        // today's lowering.
        if constraint.property == "id" || bound.contains(&constraint.binding) {
            continue;
        }

        // A node pattern that declares the binding anchors it, and an edge
        // endpoint naming the same binding then joins through that node. A
        // constraint copied onto the endpoint makes it a second, weaker
        // anchor: the endpoint carries only the pushed property, so it seeks
        // that index by itself. A mutation's MATCH clauses share one group, so
        // `MATCH (s:Source {source_id: $id}) WHERE s.tenant_id = $t
        // MATCH (s)-[e]->()` sought every vertex of the tenant through its
        // `(s)` endpoint and failed admission on a large scope.
        let declared_by_node = group
            .patterns
            .iter()
            .any(|pattern| matches!(pattern, RowPattern::Node(node) if node.binding.as_deref() == Some(constraint.binding.as_str())));

        for pattern in &mut group.patterns {
            match pattern {
                RowPattern::Node(node) => {
                    pushed |= constrain_row_node_property(node, &constraint, value);
                }
                // A binding can be an edge endpoint rather than a standalone
                // node, and that is the shape worth anchoring: it is the
                // difference between seeking one source and expanding from
                // every one of them. Endpoint metadata is hydrated for every
                // bound value regardless of the pattern
                // (`hydrate_binding_metadata`), so the added constraint has
                // something to match against.
                RowPattern::Edge(_) if declared_by_node => {}
                RowPattern::Edge(edge) => {
                    pushed |= constrain_row_node_property(&mut edge.src, &constraint, value);
                    pushed |= constrain_row_node_property(&mut edge.dst, &constraint, value);
                }
            }
        }
    }
    pushed
}

/// Add one `property = value` constraint to a node pattern, if it is the
/// pattern the constraint names and does not already say something about that
/// property.
#[cfg(feature = "opencypher")]
fn constrain_row_node_property(
    node: &mut RowNodePattern,
    constraint: &RowPredicatePropertyEqualityConstraint,
    value: &VertexPropertyValue,
) -> bool {
    if node.binding.as_deref() != Some(constraint.binding.as_str()) {
        return false;
    }
    // An id already seeks a single vertex; nothing beats that.
    if node.id.is_some() || node.properties.contains_key(&constraint.property) {
        return false;
    }
    node.properties.insert(constraint.property.clone(), value.clone());
    true
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
        RowQueryAccess::EdgePropertyIndex { edge_type, property } => format!("EdgePropertyIndex:{edge_type}.{property}"),
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
    matches!(access, RowQueryAccess::AllVertexScan | RowQueryAccess::FullEdgeScan { .. })
}

#[cfg(feature = "opencypher")]
fn optimizer_pass_label(pass: &RowQueryOptimizerPass) -> &'static str {
    match pass {
        RowQueryOptimizerPass::UtilizeVertexIndex => "UtilizeVertexIndex",
        RowQueryOptimizerPass::OrderedLimitPushdown => "OrderedLimitPushdown",
        RowQueryOptimizerPass::EqualityPredicatePushdown => "EqualityPredicatePushdown",
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
    span.record("hydradb.sampling.tail_keep", "error");
}

/// The `query.plan` span, with every recorded field declared empty up front —
/// `Span::record` only accepts fields the callsite already knows about.
#[cfg(feature = "opencypher")]
fn query_plan_span(cell_id: &str, read_epoch: StorageSequence) -> tracing::Span {
    tracing::info_span!(
        "query.plan",
        hydradb.cell_id = %cell_id,
        hydradb.read_epoch = read_epoch,
        hydradb.query.access_path = tracing::field::Empty,
        hydradb.query.optimizer_passes = tracing::field::Empty,
        hydradb.query.rows_estimated = tracing::field::Empty,
        hydradb.query.full_scan = tracing::field::Empty,
        hydradb.sampling.tail_keep = tracing::field::Empty,
        error.class = tracing::field::Empty,
    )
}
