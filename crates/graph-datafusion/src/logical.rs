//! DataFusion logical extension nodes for graph-specific semantics.
//!
//! The node payloads are deliberately backend-neutral. Provider handles,
//! snapshots, parameters, and mutation sessions belong to query execution
//! context and are never embedded in a reusable logical plan.

use std::cmp::Ordering;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion::common::{internal_err, DFSchemaRef, Result};
use datafusion::logical_expr::{Expr, Extension, LogicalPlan, UserDefinedLogicalNodeCore};
use hydradb_graph_plan::MutationProgram as BoundMutationProgram;

/// Traversal direction for a relationship pattern.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum Direction {
    Outgoing,
    Incoming,
    Undirected,
}

/// Policy chosen by the runtime before a mutation session is opened.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum MutationAtomicity {
    /// Stage the whole statement and commit exactly once.
    Statement,
    /// Preserve explicit, deterministic legacy checkpoints.
    LegacyChunked,
}

/// The type of entity whose existence is checked by [`GraphOperator::Exists`].
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum EntityKind {
    Node,
    Relationship,
}

/// Pattern-local path uniqueness semantics.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum RelationshipUniqueness {
    /// Each relationship may occur at most once in one matched path.
    Path,
    /// Relationships may repeat; the maximum hop bound still applies.
    None,
}

/// A single graph-specific logical operator with its ordered logical inputs,
/// expressions, and output schema.
#[derive(Clone, Debug)]
pub struct GraphLogicalNode {
    pub operator: GraphOperator,
    pub inputs: Vec<LogicalPlan>,
    pub expressions: Vec<Expr>,
    /// Full bound mutation data. It is retained in the logical plan rather
    /// than flattened to clause names so CREATE/MERGE/SET/REMOVE/DELETE and
    /// FOREACH branch semantics survive lowering.
    pub mutation_program: Option<BoundMutationProgram>,
    pub schema: DFSchemaRef,
}

/// Descriptors for graph operators supported by the physical extension
/// planner. The names are stable and used in DataFusion EXPLAIN output.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum GraphOperator {
    GraphScan {
        binding: String,
        labels: Vec<String>,
        batch_size: usize,
    },
    GraphSeek {
        binding: String,
        property: String,
        value_expression: u32,
        internal_id: bool,
        batch_size: usize,
    },
    /// Seek using a query-scoped parameter slot rather than capturing its
    /// request value in a reusable logical plan.
    GraphSeekParameter {
        binding: String,
        property: String,
        parameter: Arc<str>,
        internal_id: bool,
        batch_size: usize,
    },
    Expand {
        source: String,
        target: String,
        relationship: String,
        relationship_type: String,
        direction: Direction,
        uniqueness: RelationshipUniqueness,
        excluded_relationships: Vec<String>,
        batch_size: usize,
    },
    ExpandInto {
        source: String,
        target: String,
        relationship: String,
        relationship_type: String,
        direction: Direction,
        uniqueness: RelationshipUniqueness,
        excluded_relationships: Vec<String>,
        batch_size: usize,
    },
    VariableExpand {
        source: String,
        target: String,
        relationship: String,
        relationship_type: String,
        direction: Direction,
        min_hops: u32,
        max_hops: u32,
        uniqueness: RelationshipUniqueness,
        excluded_relationships: Vec<String>,
        batch_size: usize,
    },
    GraphHydrate {
        binding: String,
        properties: Vec<String>,
        batch_size: usize,
    },
    Exists {
        binding: String,
        entity_kind: EntityKind,
    },
    OptionalGraphApply,
    CorrelatedApply,
    MutationProgram {
        atomicity: MutationAtomicity,
    },
    /// Materializes mutation effects exactly once and forms a hard optimizer
    /// barrier around the input program.
    MutationSink {
        atomicity: MutationAtomicity,
    },
}

impl GraphOperator {
    pub fn name(&self) -> &'static str {
        match self {
            Self::GraphScan { .. } => "GraphScan",
            Self::GraphSeek { .. } | Self::GraphSeekParameter { .. } => "GraphSeek",
            Self::Expand { .. } => "Expand",
            Self::ExpandInto { .. } => "ExpandInto",
            Self::VariableExpand { .. } => "VariableExpand",
            Self::GraphHydrate { .. } => "GraphHydrate",
            Self::Exists { .. } => "Exists",
            Self::OptionalGraphApply => "OptionalGraphApply",
            Self::CorrelatedApply => "CorrelatedApply",
            Self::MutationProgram { .. } => "MutationProgram",
            Self::MutationSink { .. } => "MutationSink",
        }
    }

    fn expected_inputs(&self) -> std::ops::RangeInclusive<usize> {
        match self {
            Self::GraphScan { .. } => 0..=0,
            Self::GraphSeek { .. } | Self::GraphSeekParameter { .. } => 0..=0,
            Self::OptionalGraphApply | Self::CorrelatedApply => 2..=2,
            _ => 1..=1,
        }
    }

    fn is_effectful(&self) -> bool {
        matches!(self, Self::MutationProgram { .. } | Self::MutationSink { .. })
    }
}

impl GraphLogicalNode {
    /// Construct a logical graph extension and reject malformed input arity or
    /// expression references before DataFusion starts physical planning.
    pub fn try_new(operator: GraphOperator, inputs: Vec<LogicalPlan>, expressions: Vec<Expr>, schema: DFSchemaRef) -> Result<Self> {
        Self::try_new_with_mutation_program(operator, inputs, expressions, schema, None)
    }

    /// Create a mutation extension while retaining the complete graph-plan IR
    /// payload for physical execution.
    pub fn mutation_program(
        input: LogicalPlan,
        program: BoundMutationProgram,
        atomicity: MutationAtomicity,
        expressions: Vec<Expr>,
        schema: DFSchemaRef,
    ) -> Result<Self> {
        Self::try_new_with_mutation_program(
            GraphOperator::MutationProgram { atomicity },
            vec![input],
            expressions,
            schema,
            Some(program),
        )
    }

    fn try_new_with_mutation_program(
        operator: GraphOperator,
        inputs: Vec<LogicalPlan>,
        expressions: Vec<Expr>,
        schema: DFSchemaRef,
        mutation_program: Option<BoundMutationProgram>,
    ) -> Result<Self> {
        let expected = operator.expected_inputs();
        if !expected.contains(&inputs.len()) {
            return internal_err!(
                "{} expects {} logical inputs, received {}",
                operator.name(),
                if expected.start() == expected.end() {
                    expected.start().to_string()
                } else {
                    format!("{}..={}", expected.start(), expected.end())
                },
                inputs.len()
            );
        }
        if let GraphOperator::GraphSeek { value_expression, .. } = &operator {
            if *value_expression as usize >= expressions.len() {
                return internal_err!("GraphSeek value expression index {value_expression} is out of range");
            }
        }
        if let GraphOperator::GraphSeekParameter { parameter, .. } = &operator {
            if parameter.is_empty() {
                return internal_err!("GraphSeek parameter slot must not be empty");
            }
        }
        if let GraphOperator::VariableExpand { min_hops, max_hops, .. } = &operator {
            if max_hops < min_hops {
                return internal_err!("VariableExpand maximum hop count is less than minimum");
            }
        }
        if matches!(&operator, GraphOperator::MutationProgram { .. }) != mutation_program.is_some() {
            return internal_err!("MutationProgram extension must carry its complete bound graph program");
        }
        if let Some(program) = &mutation_program {
            if !program.preserves_clause_order() {
                return internal_err!("MutationProgram clauses must preserve source order");
            }
            let policy_matches = matches!(
                (program.policy, &operator),
                (
                    hydradb_graph_plan::MutationPolicy::Statement,
                    GraphOperator::MutationProgram {
                        atomicity: MutationAtomicity::Statement
                    }
                ) | (
                    hydradb_graph_plan::MutationPolicy::LegacyChunked,
                    GraphOperator::MutationProgram {
                        atomicity: MutationAtomicity::LegacyChunked
                    }
                )
            );
            if !policy_matches {
                return internal_err!("MutationProgram policy differs from its bound graph IR policy");
            }
        }
        Ok(Self {
            operator,
            inputs,
            expressions,
            mutation_program,
            schema,
        })
    }

    /// Wrap this descriptor as a DataFusion extension logical plan.
    pub fn into_plan(self) -> LogicalPlan {
        LogicalPlan::Extension(Extension { node: Arc::new(self) })
    }

    /// True for mutation nodes whose operators must remain single-owner.
    pub fn is_effectful(&self) -> bool {
        self.operator.is_effectful()
    }
}

impl PartialOrd for GraphLogicalNode {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        let mutation_key = |node: &Self| node.mutation_program.as_ref().map(|program| format!("{program:?}"));
        (
            &self.operator,
            &self.inputs,
            &self.expressions,
            mutation_key(self),
            format!("{:?}", self.schema),
        )
            .partial_cmp(&(
                &other.operator,
                &other.inputs,
                &other.expressions,
                mutation_key(other),
                format!("{:?}", other.schema),
            ))
    }
}

impl PartialEq for GraphLogicalNode {
    fn eq(&self, other: &Self) -> bool {
        self.operator == other.operator
            && self.inputs == other.inputs
            && self.expressions == other.expressions
            && self.mutation_program == other.mutation_program
            && self.schema == other.schema
    }
}

impl Eq for GraphLogicalNode {}

impl Hash for GraphLogicalNode {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.operator.hash(state);
        self.inputs.hash(state);
        self.expressions.hash(state);
        // graph-plan's mutation IR is Eq but intentionally not Hash. Its
        // deterministic derived Debug representation preserves the Eq/Hash
        // contract without inventing a lossy operation identity.
        self.mutation_program.as_ref().map(|program| format!("{program:?}")).hash(state);
        format!("{:?}", self.schema).hash(state);
    }
}

impl UserDefinedLogicalNodeCore for GraphLogicalNode {
    fn name(&self) -> &str {
        self.operator.name()
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        self.inputs.iter().collect()
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        self.expressions.clone()
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}: {:?}", self.operator.name(), self.operator)
    }

    fn with_exprs_and_inputs(&self, expressions: Vec<Expr>, inputs: Vec<LogicalPlan>) -> Result<Self> {
        Self::try_new_with_mutation_program(
            self.operator.clone(),
            inputs,
            expressions,
            Arc::clone(&self.schema),
            self.mutation_program.clone(),
        )
    }

    fn supports_limit_pushdown(&self) -> bool {
        // A limit may not suppress graph expansion completeness or mutation
        // effects. Callers can place a limit after these nodes explicitly.
        false
    }
}

/// Construct a graph extension node as a DataFusion logical plan.
pub fn graph_extension(
    operator: GraphOperator,
    inputs: Vec<LogicalPlan>,
    expressions: Vec<Expr>,
    schema: DFSchemaRef,
) -> Result<LogicalPlan> {
    Ok(GraphLogicalNode::try_new(operator, inputs, expressions, schema)?.into_plan())
}

/// Construct a `MutationProgram` extension without dropping bound operation
/// payloads from graph-plan IR.
pub fn graph_mutation_program(
    input: LogicalPlan,
    program: BoundMutationProgram,
    atomicity: MutationAtomicity,
    expressions: Vec<Expr>,
    schema: DFSchemaRef,
) -> Result<LogicalPlan> {
    let output_schema = Arc::clone(&schema);
    let mutation = GraphLogicalNode::mutation_program(input, program, atomicity, expressions, schema)?.into_plan();
    GraphLogicalNode::try_new(GraphOperator::MutationSink { atomicity }, vec![mutation], Vec::new(), output_schema)
        .map(GraphLogicalNode::into_plan)
}

/// Return the graph extension payload when the logical plan is a graph node.
pub fn graph_node(plan: &LogicalPlan) -> Option<&GraphLogicalNode> {
    match plan {
        LogicalPlan::Extension(Extension { node }) => node.as_any().downcast_ref(),
        _ => None,
    }
}

/// Ensure the extension planner receives the exact expression count expected
/// by a logical descriptor.
pub(crate) fn validate_expressions(node: &GraphLogicalNode) -> Result<()> {
    if let GraphOperator::GraphSeek { value_expression, .. } = &node.operator {
        if *value_expression as usize >= node.expressions.len() {
            return internal_err!("GraphSeek value expression index {value_expression} is out of range");
        }
    }
    if let GraphOperator::GraphSeekParameter { parameter, .. } = &node.operator {
        if parameter.is_empty() {
            return internal_err!("GraphSeek parameter slot must not be empty");
        }
    }
    Ok(())
}

/// Utility used in plan tests to assert an operator's attached output schema.
pub fn extension_schema(node: &GraphLogicalNode) -> &DFSchemaRef {
    &node.schema
}
