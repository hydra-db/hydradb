use super::value_type::GqlType;
use hydradb_cypher_ast::{NullOrdering, PathSearchKind, QueryConjunction};

use crate::{
    AggregateFunction, BoundValue, ElementTestKind, PathPart, PathWord, PatternDirection, ScalarFunction, ScalarValue, SortDirection,
    Symbol,
};

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum PhysicalBinaryOperator {
    Equal,
    NotEqual,
    LessThan,
    LessThanOrEqual,
    GreaterThan,
    GreaterThanOrEqual,
    And,
    Or,
    Xor,
    Add,
    Subtract,
    Multiply,
    Divide,
    Modulo,
    StartsWith,
    EndsWith,
    Contains,
    Concatenate,
    /// Membership in a list computed per row; see `LogicalBinaryOperator::In`.
    In,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum PhysicalUnaryOperator {
    Not,
    Plus,
    Minus,
    /// Postfix; never unknown, since it is the test for unknown.
    IsNull,
    IsNotNull,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PhysicalExpression {
    Binding(Symbol),
    Identity(Symbol),
    Property {
        binding: Symbol,
        property: Symbol,
    },
    Value(BoundValue),
    Aggregate {
        function: AggregateFunction,
        distinct: bool,
        expression: Option<Box<PhysicalExpression>>,
        /// See [`crate::LogicalExpression::Aggregate`].
        fraction: Option<Box<PhysicalExpression>>,
        group: Option<Symbol>,
    },
    Function {
        function: ScalarFunction,
        arguments: Vec<PhysicalExpression>,
    },
    /// A list value: a literal, or a list parameter bound element by element.
    List(Vec<PhysicalExpression>),
    Case {
        operand: Option<Box<PhysicalExpression>>,
        branches: Vec<(PhysicalExpression, PhysicalExpression)>,
        default: Option<Box<PhysicalExpression>>,
    },
    Unary {
        operator: PhysicalUnaryOperator,
        expression: Box<PhysicalExpression>,
    },
    Binary {
        left: Box<PhysicalExpression>,
        operator: PhysicalBinaryOperator,
        right: Box<PhysicalExpression>,
    },
    /// `<expression> IN <list>` with every candidate already resolved to a
    /// bound value, so execution never revisits parameters.
    InList {
        expression: Box<PhysicalExpression>,
        values: Vec<BoundValue>,
    },
    /// See [`crate::LogicalExpression::ElementTest`].
    ElementTest {
        binding: Symbol,
        test: ElementTestKind,
    },
    /// See [`crate::LogicalExpression::TypeTest`].
    TypeTest {
        expression: Box<PhysicalExpression>,
        value_type: GqlType,
        negated: bool,
    },
    /// See [`crate::LogicalExpression::Cast`].
    Cast {
        expression: Box<PhysicalExpression>,
        value_type: GqlType,
        request: Option<Box<PhysicalExpression>>,
    },
    /// See [`crate::LogicalExpression::Record`].
    Record(Vec<(Symbol, PhysicalExpression)>),
    /// See [`crate::LogicalExpression::Let`].
    Let {
        bindings: Vec<(Symbol, PhysicalExpression)>,
        body: Box<PhysicalExpression>,
    },
    /// See [`crate::LogicalExpression::PathConstructor`].
    PathConstructor(Vec<PhysicalExpression>),
    /// See [`crate::LogicalExpression::Field`].
    Field {
        expression: Box<PhysicalExpression>,
        field: Symbol,
    },
    /// GQL's SIMPLE and ACYCLIC path modes (§22.2 General Rule 16 d) ii)):
    /// whether the path through `parts` repeats no node, the first and last
    /// excepted when `simple`. Each part is a node name, or, flagged, the
    /// name of a variable-length hop's relationship list, whose inner nodes
    /// count too.
    PathNodesDistinct {
        simple: bool,
        parts: Vec<PathPart>,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PhysicalProjection {
    pub expression: PhysicalExpression,
    pub alias: Option<Symbol>,
}

impl PhysicalProjection {
    pub fn column_name(&self) -> String {
        self.alias
            .as_ref()
            .map(ToString::to_string)
            .unwrap_or_else(|| expression_name(&self.expression))
    }
}

/// [`expression_name`], except that a value bound from a parameter keeps the
/// parameter's spelling. A column name must not depend on the values a query
/// is run with, and binding has already replaced `$i` by its value.
fn written_name(expression: &PhysicalExpression) -> String {
    match expression {
        PhysicalExpression::Value(crate::BoundValue {
            origin: crate::ValueOrigin::Parameter(name),
            ..
        }) => {
            let name = name.as_str();
            if name.starts_with('$') {
                name.to_string()
            } else {
                format!("${name}")
            }
        }
        expression => expression_name(expression),
    }
}

fn expression_name(expression: &PhysicalExpression) -> String {
    match expression {
        PhysicalExpression::Binding(binding) => binding.to_string(),
        PhysicalExpression::Identity(binding) => format!("{binding}.id"),
        PhysicalExpression::ElementTest { binding, test } => test.expression_name(binding),
        PhysicalExpression::TypeTest {
            expression,
            value_type,
            negated,
        } => format!(
            "{} IS {}TYPED {value_type}",
            expression_name(expression),
            if *negated { "NOT " } else { "" }
        ),
        PhysicalExpression::Cast {
            expression, value_type, ..
        } => format!("CAST({} AS {value_type})", expression_name(expression)),
        PhysicalExpression::Record(fields) => format!(
            "{{{}}}",
            fields
                .iter()
                .map(|(name, value)| format!("{name}: {}", expression_name(value)))
                .collect::<Vec<_>>()
                .join(", ")
        ),
        PhysicalExpression::Field { expression, field } => format!("{}.{field}", expression_name(expression)),
        PhysicalExpression::Let { bindings, body } => format!(
            "LET {} IN {} END",
            bindings
                .iter()
                .map(|(name, value)| format!("{name} = {}", expression_name(value)))
                .collect::<Vec<_>>()
                .join(", "),
            expression_name(body)
        ),
        PhysicalExpression::PathConstructor(elements) => {
            format!("PATH[{}]", elements.iter().map(expression_name).collect::<Vec<_>>().join(", "))
        }
        PhysicalExpression::PathNodesDistinct { simple, parts } => format!(
            "{}({})",
            if *simple { "simple" } else { "acyclic" },
            parts.iter().map(|part| part.name().as_str()).collect::<Vec<_>>().join(", ")
        ),
        PhysicalExpression::Property { binding, property } => format!("{binding}.{property}"),
        PhysicalExpression::Value(value) => match &value.value {
            ScalarValue::Null => "NULL".to_string(),
            ScalarValue::Boolean(value) => value.to_string(),
            ScalarValue::Integer(value) => value.to_string(),
            ScalarValue::Float(value) | ScalarValue::String(value) => value.to_string(),
            ScalarValue::Decimal(value) => format!("{value}M"),
            ScalarValue::Bytes(value) => format!("X'{}'", hydradb_cypher_ast::bytes::hex(value)),
            ScalarValue::Date(value) => format!("DATE_EPOCH_DAY({value})"),
            ScalarValue::List(values) => format!("{values:?}"),
        },
        PhysicalExpression::InList { expression, values } => {
            format!("{} IN [{} values]", expression_name(expression), values.len())
        }
        PhysicalExpression::Aggregate {
            function,
            distinct,
            expression,
            fraction,
            ..
        } => format!(
            "{}({}{}{})",
            function.name(),
            if *distinct { "DISTINCT " } else { "" },
            expression.as_deref().map(expression_name).unwrap_or_else(|| "*".to_string()),
            fraction
                .as_deref()
                .map(|fraction| format!(", {}", expression_name(fraction)))
                .unwrap_or_default()
        ),
        PhysicalExpression::Case { .. } => "CASE".to_string(),
        PhysicalExpression::List(items) => format!("[{}]", items.iter().map(expression_name).collect::<Vec<_>>().join(", ")),
        // `list[index]` is written as a postfix, so its column keeps that form,
        // with parameters named as written: `n.tags[$i]`, whatever `$i` holds.
        PhysicalExpression::Function {
            function: super::ScalarFunction::IdProperty,
            arguments,
        } => {
            format!("{}.id", expression_name(&arguments[0]))
        }
        PhysicalExpression::Function {
            function: crate::ScalarFunction::ListElement,
            arguments,
        } if arguments.len() == 2 => format!("{}[{}]", written_name(&arguments[0]), written_name(&arguments[1])),
        PhysicalExpression::Function { function, arguments } => format!(
            "{}({})",
            function.name(),
            arguments.iter().map(expression_name).collect::<Vec<_>>().join(", ")
        ),
        PhysicalExpression::Unary { operator, expression } => {
            let operand = expression_name(expression);
            match operator {
                PhysicalUnaryOperator::Not => format!("NOT {operand}"),
                PhysicalUnaryOperator::Plus => format!("+{operand}"),
                PhysicalUnaryOperator::Minus => format!("-{operand}"),
                PhysicalUnaryOperator::IsNull => format!("{operand} IS NULL"),
                PhysicalUnaryOperator::IsNotNull => format!("{operand} IS NOT NULL"),
            }
        }
        PhysicalExpression::Binary { .. } => "expression".to_string(),
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PhysicalSort {
    pub expression: PhysicalExpression,
    pub direction: SortDirection,
    /// See [`crate::LogicalSort::nulls`].
    pub nulls: Option<NullOrdering>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum GraphPhysicalPlan {
    /// See [`crate::GraphLogicalPlan::SingleRow`].
    SingleRow,
    /// See [`crate::GraphLogicalPlan::Unwind`].
    Unwind {
        input: Box<GraphPhysicalPlan>,
        expression: PhysicalExpression,
        binding: Symbol,
        requires_list: bool,
        /// See [`crate::GraphLogicalPlan::Unwind`].
        position: Option<(Symbol, i128)>,
    },
    /// A composite query (GQL §14.2): the arms joined, left to right, by one
    /// conjunction. `all` keeps duplicates; it means nothing to `OTHERWISE`.
    Union {
        arms: Vec<GraphPhysicalPlan>,
        all: bool,
        conjunction: QueryConjunction,
    },
    /// Provider-prepared vertex reads, including their output and ordering guarantees.
    VertexScan {
        scan: crate::PreparedVertexScan,
    },
    /// Bounded ordered walk of one relationship property index: the
    /// relationship counterpart of `OrderedVertexPropertyScan`. Replaces
    /// `Sort(Filter(Expand(NodeScan)))` beneath a constant window when the
    /// window's leading sort key is a property of the expanded relationship,
    /// so only the leading `required` relationships (plus the boundary tie)
    /// are read and their endpoints hydrated.
    ///
    /// `residual` has the same meaning as on the vertex walk. Endpoint labels
    /// are always checked in memory.
    OrderedRelationshipPropertyScan {
        from: Symbol,
        source_labels: Vec<Symbol>,
        relationship: Symbol,
        relationship_type: Symbol,
        /// Pattern direction from `from` to `to`; never undirected.
        direction: PatternDirection,
        to: Symbol,
        target_labels: Vec<Symbol>,
        property: Symbol,
        sort_direction: SortDirection,
        prefix: Option<BoundValue>,
        lower: Option<(BoundValue, bool)>,
        upper: Option<(BoundValue, bool)>,
        required: usize,
        items: Vec<PhysicalSort>,
        residual: Option<PhysicalExpression>,
    },
    RelationshipPropertySeek {
        from: Symbol,
        source_labels: Vec<Symbol>,
        relationship: Symbol,
        relationship_type: Symbol,
        property: Symbol,
        value: BoundValue,
        direction: PatternDirection,
        to: Symbol,
        target_labels: Vec<Symbol>,
    },
    /// [`GraphPhysicalPlan::RelationshipPropertySeek`] for several values of
    /// one property: `r.p = $a OR r.p = $b` or `r.p IN $list`. The union of one
    /// index seek per distinct value, each relationship reported once however
    /// many values matched it. Like the single seek, the `Filter` above it
    /// still applies the whole predicate.
    RelationshipPropertyMultiSeek {
        from: Symbol,
        source_labels: Vec<Symbol>,
        relationship: Symbol,
        relationship_type: Symbol,
        property: Symbol,
        values: Vec<BoundValue>,
        direction: PatternDirection,
        to: Symbol,
        target_labels: Vec<Symbol>,
    },
    /// Try a bounded range-index probe before a label-wide expansion. The
    /// fallback preserves the original plan when the range is too broad.
    RelationshipPropertyRangeProbe {
        from: Symbol,
        source_labels: Vec<Symbol>,
        relationship: Symbol,
        relationship_type: Symbol,
        property: Symbol,
        lower: BoundValue,
        inclusive: bool,
        direction: PatternDirection,
        to: Symbol,
        target_labels: Vec<Symbol>,
        fallback: Box<GraphPhysicalPlan>,
    },
    /// Enter the pattern through the relationship type instead of a vertex
    /// label, for a type rare enough that reading its whole edge population
    /// beats sweeping the anchor label to discover that most of its vertices
    /// have no such edge.
    ///
    /// Selectivity is measured, not estimated: the probe walks the type's
    /// edges under a fixed entry budget and declines when it exhausts it, so
    /// no statistic decides this and a type that grows dense stops being
    /// probed on its own. `fallback` is the label-anchored plan this replaced
    /// and runs verbatim when the probe declines.
    RelationshipTypeProbe {
        from: Symbol,
        source_labels: Vec<Symbol>,
        relationship: Symbol,
        relationship_type: Symbol,
        direction: PatternDirection,
        to: Symbol,
        target_labels: Vec<Symbol>,
        fallback: Box<GraphPhysicalPlan>,
    },
    Expand {
        input: Box<GraphPhysicalPlan>,
        from: Symbol,
        relationship: Option<Symbol>,
        relationship_types: Vec<Symbol>,
        direction: PatternDirection,
        to: Symbol,
        target_labels: Vec<Symbol>,
    },
    VariableExpand {
        input: Box<GraphPhysicalPlan>,
        from: Symbol,
        /// `[rs:R*1..3]`: the path's relationships, in walk order, bound to
        /// `rs` as a list value.
        relationship: Option<Symbol>,
        relationship_types: Vec<Symbol>,
        direction: PatternDirection,
        to: Symbol,
        target_labels: Vec<Symbol>,
        min_hops: u8,
        /// `None` for an unbounded pattern (`*`, `*2..`): trail semantics end
        /// every walk, and the query's row budgets bound how far it gets.
        max_hops: Option<u8>,
        /// See [`crate::GraphLogicalPlan::VariableExpand`].
        trail: bool,
        nodes: crate::WalkNodes,
        iteration: Option<crate::IterationPlan<GraphPhysicalPlan>>,
    },
    NaturalJoin {
        left: Box<GraphPhysicalPlan>,
        right: Box<GraphPhysicalPlan>,
        optional: bool,
    },
    /// `EXISTS { ... }` as a mark join (GQL §19.4): every input row once,
    /// with `binding` holding whether `subquery`, run correlated with that
    /// row, yields at least one row.
    Exists {
        input: Box<GraphPhysicalPlan>,
        subquery: Box<GraphPhysicalPlan>,
        binding: Symbol,
    },
    /// `VALUE { ... }` (GQL §20.6): every input row once, with `binding`
    /// holding `column` of the one row `subquery`, run correlated with that
    /// row, yields; null when it yields none.
    ScalarSubquery {
        input: Box<GraphPhysicalPlan>,
        subquery: Box<GraphPhysicalPlan>,
        column: Symbol,
        binding: Symbol,
    },
    /// See [`crate::GraphLogicalPlan::CallProcedure`].
    CallProcedure {
        input: Box<GraphPhysicalPlan>,
        subquery: Box<GraphPhysicalPlan>,
        imports: Vec<Symbol>,
        columns: Vec<Symbol>,
        outputs_to_record: bool,
        optional: bool,
    },
    /// GQL's `FINISH` (§14.10): `input` runs, and the result is omitted.
    Next {
        input: Box<GraphPhysicalPlan>,
        next: Box<GraphPhysicalPlan>,
    },
    Finish {
        input: Box<GraphPhysicalPlan>,
    },
    /// A path variable and its subpath variables (GQL §16.4, §16.7, G004,
    /// G048): each of `names` bound to the path value `input`'s row binds
    /// through `parts`.
    BindPath {
        input: Box<GraphPhysicalPlan>,
        names: Vec<Symbol>,
        parts: Vec<PathPart>,
        /// See [`crate::GraphLogicalPlan::BindPath`].
        node_groups: Vec<(Symbol, bool)>,
    },
    /// A selective path search (GQL §22.4): `input`'s rows, the matches of
    /// one path pattern, partitioned by the nodes bound to `first` and
    /// `last`, of which each partition keeps what `kind` and `count` select.
    /// A path's length is `fixed_edges` plus the lengths of the relationship
    /// lists named in `lists`.
    SelectPaths {
        input: Box<GraphPhysicalPlan>,
        first: Symbol,
        last: Symbol,
        fixed_edges: usize,
        lists: Vec<Symbol>,
        kind: PathSearchKind,
        count: PhysicalExpression,
    },
    /// A path pattern union or multiset alternation (GQL §16.7, G030 to
    /// G033): the rows of each of `inputs`, an operand's matches, with the
    /// names another operand binds null. For a union, `words` gives each
    /// operand's word, and a reduced match (§16.4 General Rule 10) two
    /// operands both find is kept once.
    PathUnion {
        inputs: Vec<GraphPhysicalPlan>,
        words: Option<Vec<PathWord>>,
    },
    Filter {
        operation: hydradb_cypher_ast::operation::Operation,
        input: Box<GraphPhysicalPlan>,
        predicate: PhysicalExpression,
    },
    Sort {
        input: Box<GraphPhysicalPlan>,
        items: Vec<PhysicalSort>,
        estimated_input_rows: Option<u64>,
    },
    Skip {
        input: Box<GraphPhysicalPlan>,
        count: BoundValue,
    },
    Limit {
        input: Box<GraphPhysicalPlan>,
        count: BoundValue,
    },
    Project {
        context: crate::plan::logical::ProjectionContext,
        operation: hydradb_cypher_ast::operation::Operation,
        input: Box<GraphPhysicalPlan>,
        items: Vec<PhysicalProjection>,
        distinct: bool,
        post_sort: Vec<PhysicalSort>,
        post_skip: Option<BoundValue>,
        post_limit: Option<BoundValue>,
    },
}

impl GraphPhysicalPlan {
    pub(crate) fn result_omitted(&self) -> bool {
        match self {
            Self::Finish { .. } => true,
            Self::Next { next, .. } => next.result_omitted(),
            _ => false,
        }
    }

    pub(crate) fn operation(&self) -> hydradb_cypher_ast::operation::Operation {
        use hydradb_cypher_ast::operation;
        match self {
            Self::Filter { operation, .. } | Self::Project { operation, .. } => *operation,
            Self::Sort { .. } | Self::Skip { .. } | Self::Limit { .. } => operation::ORDER_BY_AND_PAGE,
            Self::SingleRow
            | Self::Union { .. }
            | Self::Exists { .. }
            | Self::ScalarSubquery { .. }
            | Self::Finish { .. }
            | Self::Next { .. } => operation::UNRECOGNIZED,
            Self::Unwind { .. } => operation::FOR,
            Self::CallProcedure { .. } => operation::CALL_PROCEDURE,
            Self::VertexScan { .. }
            | Self::OrderedRelationshipPropertyScan { .. }
            | Self::RelationshipPropertySeek { .. }
            | Self::RelationshipPropertyMultiSeek { .. }
            | Self::RelationshipPropertyRangeProbe { .. }
            | Self::RelationshipTypeProbe { .. }
            | Self::Expand { .. }
            | Self::VariableExpand { .. }
            | Self::SelectPaths { .. }
            | Self::BindPath { .. }
            | Self::PathUnion { .. }
            | Self::NaturalJoin { .. } => operation::MATCH,
        }
    }
}
