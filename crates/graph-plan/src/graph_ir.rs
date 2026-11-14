//! Backend-neutral, bound graph logical IR.

use crate::{ScalarValue, Symbol};

/// Identity of a lexical binding scope in a bound query.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ScopeId(pub u64);

/// Semantic kind of a bound value, independent of storage representation.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum EntityKind {
    Node,
    Relationship,
    Path,
    Scalar,
    List,
    Map,
    Any,
}

/// A resolved variable reference. Scope and nullability are retained by the
/// binder so lowerers do not need to infer them from query text.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct BoundSymbol {
    pub symbol: Symbol,
    pub kind: EntityKind,
    pub scope: ScopeId,
    pub nullable: bool,
}

impl BoundSymbol {
    pub fn new(symbol: impl Into<Symbol>, kind: EntityKind, scope: ScopeId) -> Self {
        Self {
            symbol: symbol.into(),
            kind,
            scope,
            nullable: false,
        }
    }

    pub fn nullable(mut self, nullable: bool) -> Self {
        self.nullable = nullable;
        self
    }
}

/// Lexical scope and its explicit outer correlations.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Scope {
    pub id: ScopeId,
    pub parent: Option<ScopeId>,
    pub bindings: Vec<BoundSymbol>,
    pub correlations: Vec<BoundSymbol>,
}

impl Scope {
    pub fn new(id: ScopeId) -> Self {
        Self {
            id,
            parent: None,
            bindings: Vec::new(),
            correlations: Vec::new(),
        }
    }

    pub fn nested(id: ScopeId, parent: ScopeId) -> Self {
        Self {
            id,
            parent: Some(parent),
            bindings: Vec::new(),
            correlations: Vec::new(),
        }
    }

    pub fn bind(&mut self, symbol: impl Into<Symbol>, kind: EntityKind) -> BoundSymbol {
        let binding = BoundSymbol::new(symbol, kind, self.id);
        self.bindings.push(binding.clone());
        binding
    }

    pub fn correlate(&mut self, binding: BoundSymbol) {
        if !self.correlations.contains(&binding) {
            self.correlations.push(binding);
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum GraphBinaryOperator {
    Equal,
    NotEqual,
    LessThan,
    LessThanOrEqual,
    GreaterThan,
    GreaterThanOrEqual,
    And,
    Or,
    Add,
    Subtract,
    Multiply,
    Divide,
    In,
    StartsWith,
    EndsWith,
    Contains,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum UnaryOperator {
    Not,
    Negate,
    IsNull,
    IsNotNull,
}

/// Typed expressions contain resolved symbols rather than backend column
/// names. Literal values remain in the Cypher value domain.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum GraphExpression {
    Binding(BoundSymbol),
    Property {
        entity: BoundSymbol,
        property: Symbol,
    },
    Parameter(Symbol),
    Literal(ScalarValue),
    Binary {
        left: Box<GraphExpression>,
        operator: GraphBinaryOperator,
        right: Box<GraphExpression>,
    },
    Unary {
        operator: UnaryOperator,
        expression: Box<GraphExpression>,
    },
    Function {
        name: Symbol,
        arguments: Vec<GraphExpression>,
        distinct: bool,
    },
    List(Vec<GraphExpression>),
    Map(Vec<(Symbol, GraphExpression)>),
    Case {
        branches: Vec<(GraphExpression, GraphExpression)>,
        fallback: Option<Box<GraphExpression>>,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Projection {
    pub expression: GraphExpression,
    pub alias: Option<BoundSymbol>,
}

/// Effects are ordered by increasing strength. A write, external effect, or
/// unknown effect is an optimizer barrier; pure and snapshot reads may be
/// reordered only when the surrounding operator semantics also allow it.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum EffectClass {
    Pure,
    GraphRead,
    GraphWrite,
    External,
    Unknown,
}

impl EffectClass {
    pub const fn is_barrier(self) -> bool {
        matches!(self, Self::GraphWrite | Self::External | Self::Unknown)
    }

    pub const fn may_reorder_with(self, other: Self) -> bool {
        !self.is_barrier() && !other.is_barrier()
    }

    pub const fn combine(self, other: Self) -> Self {
        use EffectClass::*;
        match (self, other) {
            (Unknown, _) | (_, Unknown) => Unknown,
            (External, _) | (_, External) => External,
            (GraphWrite, _) | (_, GraphWrite) => GraphWrite,
            (GraphRead, _) | (_, GraphRead) => GraphRead,
            _ => Pure,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum Direction {
    Outgoing,
    Incoming,
    Undirected,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ExpansionMode {
    Expand,
    Into,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum RelationshipUniqueness {
    /// A relationship may occur at most once in an individual path.
    RelationshipPerPath,
    /// A relationship may not be revisited in the current traversal branch.
    RelationshipPerBranch,
    /// No uniqueness constraint is imposed by the pattern.
    None,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct TraversalBounds {
    pub minimum_hops: u32,
    pub maximum_hops: Option<u32>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum GraphSeekPredicate {
    Id(GraphExpression),
    PropertyEquals { property: Symbol, value: GraphExpression },
    PropertyIn { property: Symbol, values: GraphExpression },
    Composite(Vec<(Symbol, GraphExpression)>),
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum AggregateFunction {
    Count,
    Collect,
    Sum,
    Average,
    Minimum,
    Maximum,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AggregateCall {
    pub function: AggregateFunction,
    pub argument: Option<GraphExpression>,
    pub distinct: bool,
    pub output: BoundSymbol,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SortKey {
    pub expression: GraphExpression,
    pub ascending: bool,
    pub nulls_first: Option<bool>,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum WindowFunction {
    RowNumber,
    Rank,
    DenseRank,
    Aggregate(AggregateFunction),
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum WindowFrameBound {
    UnboundedPreceding,
    Preceding(u64),
    CurrentRow,
    Following(u64),
    UnboundedFollowing,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct WindowFrame {
    pub start: WindowFrameBound,
    pub end: WindowFrameBound,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WindowCall {
    pub function: WindowFunction,
    pub arguments: Vec<GraphExpression>,
    pub partition_by: Vec<GraphExpression>,
    pub order_by: Vec<SortKey>,
    pub frame: Option<WindowFrame>,
    pub output: BoundSymbol,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum UnionMode {
    All,
    Distinct,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ProcedureEffect {
    ReadOnly,
    Mutating,
    External,
    Unknown,
}

impl From<ProcedureEffect> for EffectClass {
    fn from(effect: ProcedureEffect) -> Self {
        match effect {
            ProcedureEffect::ReadOnly => Self::GraphRead,
            ProcedureEffect::Mutating => Self::GraphWrite,
            ProcedureEffect::External => Self::External,
            ProcedureEffect::Unknown => Self::Unknown,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GraphProcedure {
    pub name: Symbol,
    pub arguments: Vec<GraphExpression>,
    pub outputs: Vec<BoundSymbol>,
    pub effect: ProcedureEffect,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum MutationPolicy {
    Statement,
    LegacyChunked,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MutationVertex {
    pub binding: Option<BoundSymbol>,
    pub labels: Vec<Symbol>,
    pub properties: Vec<(Symbol, GraphExpression)>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MutationRelationship {
    pub binding: Option<BoundSymbol>,
    pub from: Option<BoundSymbol>,
    pub to: Option<BoundSymbol>,
    pub types: Vec<Symbol>,
    pub direction: Direction,
    pub properties: Vec<(Symbol, GraphExpression)>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct MutationPattern {
    pub vertices: Vec<MutationVertex>,
    pub relationships: Vec<MutationRelationship>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MutationUpdate {
    pub target: GraphExpression,
    pub value: GraphExpression,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MutationOperation {
    Create {
        pattern: MutationPattern,
    },
    Merge {
        pattern: MutationPattern,
        on_match: Vec<MutationUpdate>,
        on_create: Vec<MutationUpdate>,
    },
    Set {
        updates: Vec<MutationUpdate>,
    },
    Remove {
        targets: Vec<GraphExpression>,
    },
    Delete {
        targets: Vec<GraphExpression>,
        detach: bool,
    },
    Foreach {
        variable: BoundSymbol,
        list: GraphExpression,
        steps: Vec<MutationOperation>,
    },
}

impl MutationOperation {
    pub const fn effect_class(&self) -> EffectClass {
        EffectClass::GraphWrite
    }

    pub const fn name(&self) -> &'static str {
        match self {
            Self::Create { .. } => "Create",
            Self::Merge { .. } => "Merge",
            Self::Set { .. } => "Set",
            Self::Remove { .. } => "Remove",
            Self::Delete { .. } => "Delete",
            Self::Foreach { .. } => "Foreach",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MutationStep {
    /// Original clause ordinal. Lowering must not sort or coalesce steps.
    pub ordinal: u64,
    pub operation: MutationOperation,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MutationProgram {
    pub policy: MutationPolicy,
    pub steps: Vec<MutationStep>,
    pub returning: Vec<Projection>,
}

impl MutationProgram {
    /// True only when clause ordinals are strictly increasing as stored.
    pub fn preserves_clause_order(&self) -> bool {
        self.steps.windows(2).all(|pair| pair[0].ordinal < pair[1].ordinal)
    }

    pub fn effect_class(&self) -> EffectClass {
        if self.steps.is_empty() {
            EffectClass::Pure
        } else {
            EffectClass::GraphWrite
        }
    }

    pub fn may_reorder_steps(&self, left: usize, right: usize) -> bool {
        left != right
            && self.steps.get(left).is_some_and(|step| !step.operation.effect_class().is_barrier())
            && self
                .steps
                .get(right)
                .is_some_and(|step| !step.operation.effect_class().is_barrier())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ApplyOutput {
    pub nullable: Vec<BoundSymbol>,
}

/// Every node is annotated with the binding scope visible at that point.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GraphIr {
    pub scope: Scope,
    pub kind: GraphIrKind,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum GraphIrKind {
    SingleRow,
    NodeScan {
        input: Option<Box<GraphIr>>,
        binding: BoundSymbol,
        labels: Vec<Symbol>,
    },
    RelationshipScan {
        input: Option<Box<GraphIr>>,
        binding: BoundSymbol,
        types: Vec<Symbol>,
    },
    GraphSeek {
        input: Option<Box<GraphIr>>,
        binding: BoundSymbol,
        predicate: GraphSeekPredicate,
    },
    Filter {
        input: Box<GraphIr>,
        predicate: GraphExpression,
    },
    Project {
        input: Box<GraphIr>,
        items: Vec<Projection>,
    },
    Expand {
        input: Box<GraphIr>,
        from: BoundSymbol,
        relationship: BoundSymbol,
        to: BoundSymbol,
        types: Vec<Symbol>,
        direction: Direction,
        mode: ExpansionMode,
        uniqueness: RelationshipUniqueness,
        /// Relationship bindings already traversed in this pattern. Excluded
        /// only when `uniqueness` requires relationship-per-path semantics.
        excluded_relationships: Vec<BoundSymbol>,
    },
    VariableExpand {
        input: Box<GraphIr>,
        from: BoundSymbol,
        relationship: Option<BoundSymbol>,
        to: BoundSymbol,
        path: Option<BoundSymbol>,
        types: Vec<Symbol>,
        direction: Direction,
        bounds: TraversalBounds,
        uniqueness: RelationshipUniqueness,
        /// Prior relationship bindings in this MATCH pattern, excluded only
        /// when `uniqueness` requires relationship-per-path semantics.
        excluded_relationships: Vec<BoundSymbol>,
    },
    GraphHydrate {
        input: Box<GraphIr>,
        entity: BoundSymbol,
        properties: Vec<Symbol>,
    },
    Exists {
        input: Box<GraphIr>,
        subquery: Box<GraphIr>,
        correlation: Vec<BoundSymbol>,
        output: BoundSymbol,
        negated: bool,
    },
    OptionalApply {
        left: Box<GraphIr>,
        right: Box<GraphIr>,
        correlation: Vec<BoundSymbol>,
        output: ApplyOutput,
    },
    CorrelatedApply {
        left: Box<GraphIr>,
        right: Box<GraphIr>,
        correlation: Vec<BoundSymbol>,
    },
    Unwind {
        input: Box<GraphIr>,
        expression: GraphExpression,
        binding: BoundSymbol,
    },
    Union {
        arms: Vec<GraphIr>,
        mode: UnionMode,
        outputs: Vec<BoundSymbol>,
    },
    Aggregation {
        input: Box<GraphIr>,
        group_by: Vec<Projection>,
        aggregates: Vec<AggregateCall>,
    },
    Sort {
        input: Box<GraphIr>,
        keys: Vec<SortKey>,
    },
    Window {
        input: Box<GraphIr>,
        calls: Vec<WindowCall>,
    },
    ResultWindow {
        input: Box<GraphIr>,
        skip: Option<GraphExpression>,
        limit: Option<GraphExpression>,
    },
    GraphProcedure {
        input: Option<Box<GraphIr>>,
        procedure: GraphProcedure,
    },
    MutationProgram {
        input: Box<GraphIr>,
        program: MutationProgram,
    },
}

impl GraphIr {
    pub fn new(scope: Scope, kind: GraphIrKind) -> Self {
        Self { scope, kind }
    }

    pub fn effect_class(&self) -> EffectClass {
        use GraphIrKind::*;
        match &self.kind {
            SingleRow => EffectClass::Pure,
            NodeScan { input, .. } | RelationshipScan { input, .. } | GraphSeek { input, .. } => {
                effect_of_optional_input(input.as_deref()).combine(EffectClass::GraphRead)
            }
            Filter { input, .. }
            | Project { input, .. }
            | Unwind { input, .. }
            | Sort { input, .. }
            | Window { input, .. }
            | ResultWindow { input, .. }
            | Aggregation { input, .. }
            | GraphHydrate { input, .. } => input.effect_class().combine(EffectClass::GraphRead),
            Exists { input, subquery, .. } => input.effect_class().combine(subquery.effect_class()),
            Expand { input, .. } | VariableExpand { input, .. } => input.effect_class().combine(EffectClass::GraphRead),
            OptionalApply { left, right, .. } | CorrelatedApply { left, right, .. } => left.effect_class().combine(right.effect_class()),
            Union { arms, .. } => arms
                .iter()
                .fold(EffectClass::Pure, |effect, arm| effect.combine(arm.effect_class())),
            GraphProcedure { input, procedure } => effect_of_optional_input(input.as_deref()).combine(procedure.effect.into()),
            MutationProgram { input, program } => input.effect_class().combine(program.effect_class()),
        }
    }
}

fn effect_of_optional_input(input: Option<&GraphIr>) -> EffectClass {
    input.map_or(EffectClass::Pure, GraphIr::effect_class)
}
