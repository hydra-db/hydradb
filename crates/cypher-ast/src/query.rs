use crate::{CypherDialect, Expression, Identifier, SyntaxFragment};

#[derive(Clone, Debug, PartialEq)]
pub struct Document {
    pub dialect: CypherDialect,
    pub statements: Vec<Statement>,
}

impl Document {
    pub fn new(dialect: CypherDialect, statements: Vec<Statement>) -> Self {
        Self { dialect, statements }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum Statement {
    Query(Query),
    Syntax(SyntaxFragment),
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct Query {
    pub clauses: Vec<Clause>,
    pub unions: Vec<UnionArm>,
}

impl Query {
    /// Combine clause and UNION-arm effects conservatively.
    pub fn effect_classification(&self) -> EffectClassification {
        self.clauses
            .iter()
            .map(Clause::effect_classification)
            .chain(self.unions.iter().map(|arm| arm.query.effect_classification()))
            .fold(EffectClassification::PURE, EffectClassification::combine)
    }

    /// Classify whether this query may write before opening provider state.
    pub fn write_classification(&self) -> WriteClassification {
        self.effect_classification().write_classification()
    }
}

/// A conservative summary of the observable effects of evaluating syntax.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
pub struct EffectClassification {
    pub reads: bool,
    pub writes: bool,
    pub unknown: bool,
}

impl EffectClassification {
    pub const PURE: Self = Self {
        reads: false,
        writes: false,
        unknown: false,
    };
    pub const READ: Self = Self {
        reads: true,
        writes: false,
        unknown: false,
    };
    pub const READ_WRITE: Self = Self {
        reads: true,
        writes: true,
        unknown: false,
    };
    pub const UNKNOWN: Self = Self {
        reads: true,
        writes: false,
        unknown: true,
    };

    pub const fn combine(self, other: Self) -> Self {
        Self {
            reads: self.reads || other.reads,
            writes: self.writes || other.writes,
            unknown: self.unknown || other.unknown,
        }
    }

    pub const fn may_write(self) -> bool {
        self.writes || self.unknown
    }

    pub const fn write_classification(self) -> WriteClassification {
        if self.unknown {
            WriteClassification::Unknown
        } else if self.writes {
            WriteClassification::Writes
        } else {
            WriteClassification::ReadOnly
        }
    }
}

/// Whether a query is read-only, known to write, or may write because it has
/// unclassified syntax.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum WriteClassification {
    ReadOnly,
    Writes,
    Unknown,
}

impl WriteClassification {
    pub const fn may_write(self) -> bool {
        !matches!(self, Self::ReadOnly)
    }
}

/// One arm after the first of a composite query (GQL §14.2): how it joins
/// the arms before it, and itself.
#[derive(Clone, Debug, PartialEq)]
pub struct UnionArm {
    pub conjunction: QueryConjunction,
    /// `ALL`; `DISTINCT`, written or implicit, otherwise. Meaningless for
    /// `OTHERWISE`.
    pub all: bool,
    pub query: Box<Query>,
}

/// A <query conjunction> (GQL §14.2).
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum QueryConjunction {
    Union,
    /// GQ04, GQ05.
    Except,
    /// GQ06, GQ07.
    Intersect,
    /// GQ02: the arms before it if they have a row, else this arm.
    Otherwise,
}

#[derive(Clone, Debug, PartialEq)]
pub enum Clause {
    Match(MatchClause),
    With(WithClause),
    Unwind(UnwindClause),
    Let(LetClause),
    Filter(FilterClause),
    Create(CreateClause),
    Merge(MergeClause),
    Set(SetClause),
    Remove(RemoveClause),
    Delete(DeleteClause),
    Foreach(ForeachClause),
    OrderByAndPage(OrderByAndPageClause),
    Return(ReturnClause),
    /// GQL's `FINISH` (§14.10, GQ27): the query ends with an omitted result.
    Finish,
    /// GQL's `NEXT` (§9.2, GQ20) between two statements: what follows runs
    /// over the table the RETURN before it produced.
    Next,
    /// `OPTIONAL { MATCH ... MATCH ... }` (§14.4, GQ21): the match
    /// statements together, optional as one.
    OptionalBlock(Vec<Clause>),
    /// `[OPTIONAL] CALL [(a, b)] { query }` (§15.1, §15.2, GP01 to GP03).
    Call(CallClause),
    Syntax(SyntaxFragment),
}

impl Clause {
    /// Effects that this clause can have when evaluated.
    pub fn effect_classification(&self) -> EffectClassification {
        match self {
            Self::Match(_) => EffectClassification::READ,
            Self::With(_)
            | Self::Unwind(_)
            | Self::Let(_)
            | Self::Filter(_)
            | Self::OrderByAndPage(_)
            | Self::Return(_)
            | Self::Finish
            | Self::Next => EffectClassification::PURE,
            Self::Create(_) | Self::Merge(_) | Self::Set(_) | Self::Remove(_) | Self::Delete(_) | Self::Foreach(_) => {
                EffectClassification::READ_WRITE
            }
            Self::OptionalBlock(clauses) => clauses
                .iter()
                .map(Self::effect_classification)
                .fold(EffectClassification::PURE, EffectClassification::combine),
            Self::Call(call) => call.query.effect_classification(),
            Self::Syntax(_) => EffectClassification::UNKNOWN,
        }
    }

    pub fn write_classification(&self) -> WriteClassification {
        self.effect_classification().write_classification()
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct CreateClause {
    pub patterns: Vec<PathPattern>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct MergeClause {
    pub pattern: PathPattern,
    pub on_create: Vec<SetItem>,
    pub on_match: Vec<SetItem>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct SetClause {
    pub items: Vec<SetItem>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum SetItem {
    Property {
        target: Expression,
        property: Identifier,
        value: Expression,
    },
    Properties {
        target: Expression,
        operator: PropertiesUpdate,
        value: Expression,
    },
    Labels {
        target: Expression,
        labels: Vec<Identifier>,
    },
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum PropertiesUpdate {
    Replace,
    Merge,
}

#[derive(Clone, Debug, PartialEq)]
pub struct RemoveClause {
    pub items: Vec<RemoveItem>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum RemoveItem {
    Property { target: Expression, property: Identifier },
    Labels { target: Expression, labels: Vec<Identifier> },
}

#[derive(Clone, Debug, PartialEq)]
pub struct DeleteClause {
    pub expressions: Vec<Expression>,
    pub detach: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ForeachClause {
    pub variable: Identifier,
    pub expression: Expression,
    pub clauses: Vec<MutationClause>,
}

/// Mutation-only clauses permitted in a `FOREACH` body.
#[derive(Clone, Debug, PartialEq)]
pub enum MutationClause {
    Create(CreateClause),
    Merge(Box<MergeClause>),
    Set(SetClause),
    Remove(RemoveClause),
    Delete(DeleteClause),
    Foreach(ForeachClause),
}

impl MutationClause {
    pub fn effect_classification(&self) -> EffectClassification {
        EffectClassification::READ_WRITE
    }
}

/// An inline procedure call (GQL §15.2).
#[derive(Clone, Debug, PartialEq)]
pub struct CallClause {
    pub optional: bool,
    /// Internal continuation lowering restores definitions to the working
    /// record, rather than adding procedure results to the working table.
    pub outputs_to_record: bool,
    /// The variable scope clause, `(a, b)` or `()`; `None` when none is
    /// written, which imports every incoming name (Syntax Rule 3, GP02).
    pub scope: Option<Vec<Identifier>>,
    pub query: Box<Query>,
}

/// A `WITH` clause: a projection that is also a scope boundary.
///
/// Carries everything [`ReturnClause`] does, because `WITH` projects the same
/// way `RETURN` does, plus the `WHERE` that may follow it - which `RETURN` has
/// no form of. `group_by` is GQL's explicit grouping; without it, grouping
/// follows from the aggregates among `items`, as it does for `RETURN`.
#[derive(Clone, Debug, PartialEq)]
pub struct WithClause {
    /// A RETURN rewritten as a NEXT boundary projects only table columns;
    /// a Cypher WITH also carries readable working-record bindings.
    pub table_only: bool,
    pub quantifier: Option<SetQuantifier>,
    pub items: ProjectionItems,
    pub group_by: Vec<Expression>,
    pub order_by: Vec<SortItem>,
    pub skip: Option<Expression>,
    pub limit: Option<Expression>,
    pub predicate: Option<Expression>,
}

/// GQL's `LET a = <expression>, ...`: new names for the rest of the query,
/// each computed from what was in scope before the clause.
#[derive(Clone, Debug, PartialEq)]
pub struct LetClause {
    pub bindings: Vec<(Identifier, Expression)>,
    /// A block's value variable definition (§10.3, GP05), not a LET
    /// statement: it holds for every statement of the body, NEXT included.
    pub definition: bool,
}

/// GQL's `FILTER [WHERE] <predicate>`: keep the rows the predicate holds for.
#[derive(Clone, Debug, PartialEq)]
pub struct FilterClause {
    pub predicate: Expression,
}

/// Standalone sorting/windowing (§14.9), without a projection boundary.
#[derive(Clone, Debug, PartialEq)]
pub struct OrderByAndPageClause {
    pub order_by: Vec<SortItem>,
    pub skip: Option<Expression>,
    pub limit: Option<Expression>,
}

/// `UNWIND <expression> AS <variable>`: one row per element of a list.
#[derive(Clone, Debug, PartialEq)]
pub struct UnwindClause {
    pub expression: Expression,
    pub variable: Identifier,
    /// GQL's `FOR`, whose source must be a list (§14.8 Syntax Rule 10).
    /// Cypher's `UNWIND` also takes a single value as a list of one.
    pub requires_list: bool,
    /// `FOR ... WITH ORDINALITY i` or `WITH OFFSET i` (GQ11, GQ24).
    pub position: Option<ForPosition>,
}

/// A FOR element's position, bound beside it (GQL §14.8).
#[derive(Clone, Debug, PartialEq)]
pub struct ForPosition {
    pub variable: Identifier,
    /// `WITH OFFSET` counts from 0, `WITH ORDINALITY` from 1.
    pub offset: bool,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum MatchMode {
    RepeatableElements,
    DifferentRelationships,
}

#[derive(Clone, Debug, PartialEq)]
pub struct MatchClause {
    pub optional: bool,
    pub mode: Option<MatchMode>,
    pub patterns: Vec<PathPattern>,
    pub predicate: Option<Expression>,
    /// `KEEP <path pattern prefix>` (GQL §16.4, G006, G007): the prefix
    /// every path pattern of the clause takes.
    pub keep: Option<PathPrefix>,
    /// `YIELD a, b` (GQL §16.3, GQ19): the pattern's variables the clause
    /// keeps, beside those it read from before.
    pub yield_items: Option<Vec<Identifier>>,
}

/// A <path pattern prefix> (GQL §16.6): a path mode, a selective search, or
/// both. `ALL` alone selects nothing.
#[derive(Clone, Debug, PartialEq)]
pub struct PathPrefix {
    pub mode: Option<PathMode>,
    pub search: Option<PathSearch>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct PathPattern {
    /// Standard-normalized structure for semantic validation, including valid
    /// factors outside the executable adapter's subset. `elements` is empty
    /// when that adapter cannot preserve every predicate and binding.
    pub factor: Option<crate::PathFactor>,
    /// The path variable (GQL §16.4, G004): `p = (a)-->(b)`.
    pub binding: Option<Identifier>,
    /// Subpath variables (§16.7, G048) of a parenthesized path that is the
    /// whole pattern, `(q = (a)-->(b))`, which name the same path.
    pub subpaths: Vec<Identifier>,
    /// A selective <path search prefix> (GQL §16.6, G016 to G020), which
    /// keeps some of the path's matches between each pair of endpoints.
    pub search: Option<PathSearch>,
    /// Written search presence, including nonselective ALL (§16.6), retained
    /// only in syntax for KEEP validation (§16.4 SR 6 b) i)).
    pub search_prefix_written: bool,
    pub elements: Vec<PatternElement>,
    /// The path modes written for the whole path (GQL §16.6, §16.7): in a
    /// prefix, `MATCH TRAIL (a)-->(b)`, or inside the parentheses around it,
    /// `MATCH (ACYCLIC (a)-->(b))`. Each holds of the path.
    pub modes: Vec<PathMode>,
    /// The WHERE inside the parentheses around the whole path (GQL §16.7,
    /// G050), `MATCH ((a)-->(b) WHERE a.x = b.x)`. It holds of each of the
    /// path's matches, so of a selective path it is applied before the
    /// search selects among them (§22.4).
    pub predicate: Option<Expression>,
    /// A <path pattern union> or <path multiset alternation> (GQL §16.7,
    /// G030 to G033): the path is any one of the operands, and `elements` is
    /// empty.
    pub alternatives: Option<PathAlternatives>,
}

impl PathPattern {
    /// The element patterns of the path, or of every operand of its union.
    pub fn all_elements(&self) -> Vec<&PatternElement> {
        match &self.alternatives {
            None => self.elements.iter().collect(),
            Some(alternatives) => alternatives
                .operands
                .iter()
                .flat_map(|operand| operand.path.all_elements())
                .collect(),
        }
    }
}

/// The operands of a <path pattern union>, `A | B`, or of a <path multiset
/// alternation>, `A |+| B` (GQL §16.7).
#[derive(Clone, Debug, PartialEq)]
pub struct PathAlternatives {
    /// `|+|`: a match of two operands is two matches (§16.7 Syntax Rule
    /// 10), where `|` keeps one.
    pub multiset: bool,
    pub operands: Vec<PathOperand>,
}

/// An operand of a path pattern union or multiset alternation: a path term,
/// and the `WHERE` of the parentheses around it (§16.7 G050), which holds of
/// this operand's matches only.
#[derive(Clone, Debug, PartialEq)]
pub struct PathOperand {
    pub path: PathPattern,
    pub predicate: Option<Expression>,
}

/// A selective <path search prefix> (GQL §16.6), normalized by its Syntax
/// Rule 2: `ANY` is `ANY 1`, `ANY SHORTEST` is `SHORTEST 1`, and `ALL
/// SHORTEST` is `SHORTEST 1 GROUP`. `count` is a literal or a parameter.
#[derive(Clone, Debug, PartialEq)]
pub struct PathSearch {
    pub kind: PathSearchKind,
    pub count: Expression,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum PathSearchKind {
    /// `ANY N`: any N paths (G016).
    Any,
    /// `SHORTEST N`: the N shortest paths (G018, G019).
    Shortest,
    /// `SHORTEST N GROUPS`: the paths of the N shortest lengths (G017,
    /// G020).
    ShortestGroups,
}

/// A <path mode> (GQL §16.6, G010 to G013).
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum PathMode {
    /// No constraint.
    Walk,
    /// No edge twice.
    Trail,
    /// No node twice, except that the path may end where it began.
    Simple,
    /// No node twice.
    Acyclic,
}

#[derive(Clone, Debug, PartialEq)]
pub enum PatternElement {
    Node(NodePattern),
    Relationship(RelationshipPattern),
}

/// A label expression (GQL §16.8): label names combined with `&`, `|` and
/// `!`, and `%` for any label.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum LabelExpression {
    Label(Identifier),
    /// `%`: the element has at least one label.
    Wildcard,
    Not(Box<LabelExpression>),
    And(Vec<LabelExpression>),
    Or(Vec<LabelExpression>),
}

impl std::fmt::Display for LabelExpression {
    /// GQL's own spelling, parenthesized wherever precedence needs it.
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let operand = |formatter: &mut std::fmt::Formatter<'_>, term: &LabelExpression| match term {
            LabelExpression::And(_) | LabelExpression::Or(_) => write!(formatter, "({term})"),
            _ => write!(formatter, "{term}"),
        };
        match self {
            Self::Label(name) => write!(formatter, "{name}"),
            Self::Wildcard => formatter.write_str("%"),
            Self::Not(term) => {
                formatter.write_str("!")?;
                operand(formatter, term)
            }
            Self::And(terms) | Self::Or(terms) => {
                let separator = if matches!(self, Self::And(_)) { "&" } else { "|" };
                for (index, term) in terms.iter().enumerate() {
                    if index > 0 {
                        formatter.write_str(separator)?;
                    }
                    operand(formatter, term)?;
                }
                Ok(())
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct NodePattern {
    pub variable: Option<Identifier>,
    /// Labels every match carries, in source order: the whole label
    /// expression when it is a plain conjunction such as `:A:B`.
    pub labels: Vec<Identifier>,
    /// A label expression that is more than a conjunction of names, such as
    /// `:A|B` or `:!A`. `labels` then holds the names it requires, if any.
    pub label_expression: Option<LabelExpression>,
    pub properties: Option<Expression>,
    pub predicate: Option<Expression>,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum Direction {
    Outgoing,
    Incoming,
    /// `-[ ]-`: either orientation, GQL's <full edge any direction>.
    Undirected,
    /// `<-[ ]->`: GQL's <full edge left or right>.
    Bidirectional,
    /// GQL's `~[ ]~` (§16.7, G043): an undirected edge.
    UndirectedEdge,
    /// `<~[ ]~`: pointing left, or undirected.
    LeftOrUndirected,
    /// `~[ ]~>`: undirected, or pointing right.
    UndirectedOrRight,
}

#[derive(Clone, Debug, PartialEq)]
pub struct RelationshipPattern {
    pub variable: Option<Identifier>,
    /// Relationship types in source order, any one of which a match has: the
    /// whole label expression when it is a plain disjunction such as `:A|B`.
    pub types: Vec<Identifier>,
    /// A label expression that is more than a disjunction of names, such as
    /// `:!A`; `types` is then empty.
    pub label_expression: Option<LabelExpression>,
    pub direction: Direction,
    pub length: Option<PathLength>,
    pub properties: Option<Expression>,
    pub predicate: Option<Expression>,
    /// Set when the hop is a quantified path of one edge (GQL §16.7, G035),
    /// `((a)-[e]->(b)){1,3}`: the group variables of its iterations' first
    /// and last nodes.
    pub iteration_nodes: Option<IterationNodes>,
}

/// The node group variables of a quantified path (GQL §16.7): `start` binds
/// the list of each iteration's first node, `end` of each one's last.
#[derive(Clone, Debug, PartialEq)]
pub struct IterationNodes {
    /// The singleton primary before repetition, including every predicate.
    /// Nested predicate queries retain their own graph-matching depth.
    pub primary: Box<PathPattern>,
    pub start: Option<Identifier>,
    pub end: Option<Identifier>,
    /// Whether the edge inside was quantified too, which §16.7 Syntax Rule 9
    /// forbids.
    pub nested: bool,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct PathLength {
    pub minimum: Option<u32>,
    pub maximum: Option<u32>,
    /// Written as a GQL <graph pattern quantifier> (`{1,3}`, `+`, `*` after
    /// an edge) rather than Cypher's `*1..3`, whose Syntax Rules (§16.11)
    /// then apply.
    pub quantifier: bool,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum SetQuantifier {
    All,
    Distinct,
}

#[derive(Clone, Debug, PartialEq)]
pub enum ProjectionItems {
    /// `*`, then any further items: `WITH *, n.score * 10 AS scaled`. The
    /// star stands for every name in scope, which only lowering knows.
    All(Vec<Projection>),
    Items(Vec<Projection>),
}

impl ProjectionItems {
    /// Explicit projections, including any extras after a star.
    pub fn items(&self) -> &[Projection] {
        match self {
            Self::All(items) | Self::Items(items) => items,
        }
    }

    pub fn includes_all(&self) -> bool {
        matches!(self, Self::All(_))
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct Projection {
    pub expression: Expression,
    pub alias: Option<Identifier>,
}

impl Projection {
    /// An explicit alias or the implicit name of a projected variable.
    pub fn output_binding(&self) -> Option<&Identifier> {
        self.alias.as_ref().or(match &self.expression {
            Expression::Identifier(name) => Some(name),
            _ => None,
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum OrderDirection {
    Ascending,
    Descending,
}

#[derive(Clone, Debug, PartialEq)]
pub struct SortItem {
    pub expression: Expression,
    pub direction: Option<OrderDirection>,
    /// `NULLS FIRST` or `NULLS LAST` (GQL §16.17, GA03); the implicit order
    /// (IS001) when absent.
    pub nulls: Option<NullOrdering>,
}

/// Where nulls go in a sort, whatever its direction (GQL §16.17).
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum NullOrdering {
    First,
    Last,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ReturnClause {
    pub quantifier: Option<SetQuantifier>,
    pub items: ProjectionItems,
    pub group_by: Vec<Expression>,
    pub order_by: Vec<SortItem>,
    pub skip: Option<Expression>,
    pub limit: Option<Expression>,
}
