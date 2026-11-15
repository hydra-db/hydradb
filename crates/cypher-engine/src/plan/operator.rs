//! Structural facts about a physical plan that observability needs and
//! execution does not: a stable operator name and the child edges.
//!
//! Kept apart from the executor on purpose. Every consumer that walks a plan
//! for telemetry goes through [`GraphPhysicalPlan::children`], so a new
//! operator variant is one match arm here rather than one in every walker.

use std::collections::BTreeSet;

use hydradb_cypher_ast::QueryConjunction;

use crate::{GraphPhysicalPlan, Symbol};

impl GraphPhysicalPlan {
    /// The operator's name as `EXPLAIN` spells it, without arguments. A closed
    /// vocabulary, so it is safe as a metric label.
    pub fn operator_name(&self) -> &'static str {
        match self {
            Self::SingleRow => "SingleRowExec",
            Self::Unwind { .. } => "UnwindExec",
            // Named as EXPLAIN names them, so a profile tells the set
            // operators apart.
            Self::Union { conjunction, .. } => match conjunction {
                QueryConjunction::Union => "UnionExec",
                QueryConjunction::Except => "ExceptExec",
                QueryConjunction::Intersect => "IntersectExec",
                QueryConjunction::Otherwise => "OtherwiseExec",
            },
            Self::PathUnion { .. } => "PathUnionExec",
            Self::VertexScan { .. } => "VertexScan",
            Self::OrderedRelationshipPropertyScan { .. } => "OrderedRelationshipPropertyScan",
            Self::RelationshipPropertySeek { .. } => "RelationshipPropertySeek",
            Self::RelationshipPropertyMultiSeek { .. } => "RelationshipPropertyMultiSeek",
            Self::RelationshipPropertyRangeProbe { .. } => "RelationshipPropertyRangeProbe",
            Self::RelationshipTypeProbe { .. } => "RelationshipTypeProbe",
            Self::Expand { .. } => "ExpandExec",
            Self::VariableExpand { .. } => "VariableExpandExec",
            // The two join behaviours are separate operators to telemetry for
            // the same reason `EXPLAIN` names them apart: a regression in
            // `OPTIONAL MATCH` is not a regression in the inner join.
            Self::NaturalJoin { optional: true, .. } => "LeftOuterNaturalJoinExec",
            Self::NaturalJoin { optional: false, .. } => "NaturalJoinExec",
            Self::Exists { .. } => "ExistsMarkExec",
            Self::ScalarSubquery { .. } => "ScalarSubqueryExec",
            Self::CallProcedure { .. } => "CallProcedureExec",
            Self::Finish { .. } => "FinishExec",
            Self::SelectPaths { .. } => "SelectPathsExec",
            Self::BindPath { .. } => "BindPathExec",
            Self::Next { .. } => "NextExec",
            Self::Filter { .. } => "FilterExec",
            Self::Sort { .. } => "SortExec",
            Self::Skip { .. } => "SkipExec",
            Self::Limit { .. } => "LimitExec",
            Self::Project { .. } => "ProjectExec",
        }
    }

    /// Direct inputs, left to right, in the order `EXPLAIN` renders them.
    pub fn children(&self) -> Vec<&GraphPhysicalPlan> {
        match self {
            Self::Union { arms, .. } => arms.iter().collect(),
            Self::PathUnion { inputs, .. } => inputs.iter().collect(),
            Self::SingleRow
            | Self::VertexScan { .. }
            | Self::OrderedRelationshipPropertyScan { .. }
            | Self::RelationshipPropertySeek { .. }
            | Self::RelationshipPropertyMultiSeek { .. } => Vec::new(),
            Self::RelationshipPropertyRangeProbe { fallback, .. } | Self::RelationshipTypeProbe { fallback, .. } => vec![fallback],
            Self::Unwind { input, .. }
            | Self::Expand { input, .. }
            | Self::Filter { input, .. }
            | Self::Sort { input, .. }
            | Self::Skip { input, .. }
            | Self::Limit { input, .. }
            | Self::Project { input, .. }
            | Self::Finish { input }
            | Self::SelectPaths { input, .. }
            | Self::BindPath { input, .. } => vec![input],
            Self::VariableExpand { input, iteration, .. } => {
                let mut children = vec![input.as_ref()];
                children.extend(iteration.as_ref().map(|iteration| iteration.plan.as_ref()));
                children
            }
            Self::Next { input, next } => vec![input, next],
            Self::NaturalJoin { left, right, .. } => vec![left, right],
            Self::Exists { input, subquery, .. }
            | Self::ScalarSubquery { input, subquery, .. }
            | Self::CallProcedure { input, subquery, .. } => vec![input, subquery],
        }
    }

    /// Every binding this plan can put on a row, nodes and relationships
    /// alike.
    ///
    /// An OPTIONAL join needs this to say what its right side *would* have
    /// bound when it matches nothing. Cypher binds those names to null rather
    /// than leaving them unbound, and only the plan knows which names they
    /// are.
    pub fn bindings(&self) -> BTreeSet<Symbol> {
        let mut bindings = BTreeSet::new();
        self.collect_bindings(&mut bindings);
        bindings
    }

    fn collect_bindings(&self, into: &mut BTreeSet<Symbol>) {
        match self {
            Self::VertexScan { scan } => {
                into.insert(scan.binding().clone());
            }
            // Both bind the whole hop: the source, the relationship and the
            // target are all produced by the operator itself.
            Self::RelationshipPropertySeek {
                from, relationship, to, ..
            }
            | Self::RelationshipPropertyMultiSeek {
                from, relationship, to, ..
            }
            | Self::OrderedRelationshipPropertyScan {
                from, relationship, to, ..
            }
            | Self::RelationshipPropertyRangeProbe {
                from, relationship, to, ..
            }
            | Self::RelationshipTypeProbe {
                from, relationship, to, ..
            } => {
                into.insert(from.clone());
                into.insert(relationship.clone());
                into.insert(to.clone());
            }
            Self::Expand {
                from, relationship, to, ..
            } => {
                into.insert(from.clone());
                if let Some(relationship) = relationship {
                    into.insert(relationship.clone());
                }
                into.insert(to.clone());
            }
            Self::VariableExpand {
                input,
                from,
                relationship,
                to,
                ..
            } => {
                into.insert(from.clone());
                into.extend(relationship.clone());
                into.insert(to.clone());
                input.collect_bindings(into);
                return;
            }
            // Bind nothing of their own; their children do.
            // An unwound element is a value, never a node or relationship, so
            // an OPTIONAL miss has nothing of it to bind to null.
            Self::Next { next, .. } => {
                next.collect_bindings(into);
                return;
            }
            Self::SingleRow
            | Self::Unwind { .. }
            | Self::Union { .. }
            | Self::Filter { .. }
            | Self::Sort { .. }
            | Self::Skip { .. }
            | Self::Limit { .. }
            | Self::Project { .. }
            | Self::Finish { .. }
            | Self::SelectPaths { .. }
            | Self::BindPath { .. }
            | Self::PathUnion { .. }
            | Self::NaturalJoin { .. } => {}
            // The subquery's bindings never reach the rows; only the input's
            // do, and the mark is a value.
            Self::Exists { input, .. } => {
                input.collect_bindings(into);
                return;
            }
            // The value may be a node or a relationship, which an OPTIONAL
            // miss binds to null like any other.
            Self::ScalarSubquery { input, binding, .. } => {
                input.collect_bindings(into);
                into.insert(binding.clone());
                return;
            }
            Self::CallProcedure { input, columns, .. } => {
                input.collect_bindings(into);
                into.extend(columns.iter().cloned());
                return;
            }
        }
        for child in self.children() {
            child.collect_bindings(into);
        }
    }

    /// Whether a prepared access may read the vertex inventory, including
    /// fallbacks. ExecutionProfile::ran_full_scan reports what actually ran.
    pub fn may_scan_inventory(&self) -> bool {
        matches!(self, Self::VertexScan { scan } if scan.properties().may_scan_inventory)
            || self.children().into_iter().any(Self::may_scan_inventory)
    }
}
