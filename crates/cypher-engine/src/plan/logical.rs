use std::collections::BTreeSet;
use std::fmt;

use super::value_type::GqlType;
use hydradb_cypher_ast::{LabelExpression, NullOrdering, PathSearchKind, QueryConjunction};

use crate::ScalarValue;

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Symbol(Box<str>);

impl Symbol {
    pub fn new(value: impl Into<Box<str>>) -> Self {
        Self(value.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<&str> for Symbol {
    fn from(value: &str) -> Self {
        Self::new(value)
    }
}

impl From<String> for Symbol {
    fn from(value: String) -> Self {
        Self::new(value)
    }
}

impl fmt::Display for Symbol {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ExpandDirection {
    Outgoing,
    Incoming,
}

impl fmt::Display for ExpandDirection {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Outgoing => "outgoing",
            Self::Incoming => "incoming",
        })
    }
}

/// The direction written in a pattern, which is not the same thing as the
/// direction a storage expansion runs in.
///
/// `ExpandDirection` stays two-valued because it is the storage contract: an
/// edge is stored pointing one way, and the HydraDB adapter answers one
/// orientation per request. Undirected is a plan-level concept that the
/// executor satisfies by running both orientations and merging, so adding a
/// third `ExpandDirection` variant would push a case into the storage adapter
/// that storage cannot answer.
/// What a variable-length walk holds of the nodes it visits: the SIMPLE and
/// ACYCLIC path modes (GQL §16.6, §22.2 General Rule 16 d) ii)), applied as
/// the walk grows rather than only to its finished paths. A walk's interior
/// nodes are its path's interior nodes, so pruning at a repeat discards only
/// paths the mode rejects; the whole path's mode is still checked after.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum WalkNodes {
    /// Nodes may repeat.
    #[default]
    Any,
    /// No node repeats, except that the walk may end where it began. That
    /// closing step may reuse an edge where edges may repeat: `a-b-a` over
    /// one undirected edge is a simple path.
    Simple,
    /// No node repeats.
    Acyclic,
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum PatternDirection {
    Outgoing,
    Incoming,
    Undirected,
}

impl PatternDirection {
    /// The storage expansions that together satisfy this pattern direction.
    pub fn storage_directions(self) -> &'static [ExpandDirection] {
        match self {
            Self::Outgoing => &[ExpandDirection::Outgoing],
            Self::Incoming => &[ExpandDirection::Incoming],
            Self::Undirected => &[ExpandDirection::Outgoing, ExpandDirection::Incoming],
        }
    }
}

impl fmt::Display for PatternDirection {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Outgoing => "outgoing",
            Self::Incoming => "incoming",
            Self::Undirected => "undirected",
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum SortDirection {
    Ascending,
    Descending,
}

impl fmt::Display for SortDirection {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Ascending => "ASC",
            Self::Descending => "DESC",
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum LogicalBinaryOperator {
    Equal,
    NotEqual,
    LessThan,
    LessThanOrEqual,
    GreaterThan,
    GreaterThanOrEqual,
    And,
    Or,
    /// `a XOR b` (§20.20): true when exactly one is true, null when either
    /// is null.
    Xor,
    Add,
    Subtract,
    Multiply,
    Divide,
    Modulo,
    StartsWith,
    EndsWith,
    Contains,
    /// `a || b` on character strings (§20.23).
    Concatenate,
    /// `x IN list` where the list is computed per row, such as a stored list
    /// property or a `WITH` alias. A literal or parameter list is planned as
    /// [`LogicalExpression::In`] instead, which can drive an index seek.
    In,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum LogicalUnaryOperator {
    Not,
    Plus,
    Minus,
    IsNull,
    IsNotNull,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum AggregateFunction {
    Count,
    Sum,
    Average,
    Collect,
    /// The least non-null input under the engine's value order; null when
    /// there is none.
    Min,
    /// The greatest non-null input under the engine's value order; null when
    /// there is none.
    Max,
    /// `STDDEV_SAMP(x)` (§20.9, GF10): null for fewer than two inputs.
    StddevSamp,
    /// `STDDEV_POP(x)` (GF10).
    StddevPop,
    /// `PERCENTILE_CONT(x, p)` (GF11): interpolated between neighbours.
    PercentileCont,
    /// `PERCENTILE_DISC(x, p)` (GF11): an input value; a fractional position
    /// rounds to the nearest (IA005).
    PercentileDisc,
}

impl AggregateFunction {
    /// The function a Cypher name denotes, matched case-insensitively.
    pub fn from_name(name: &str) -> Option<Self> {
        match name.to_ascii_lowercase().as_str() {
            "count" => Some(Self::Count),
            "sum" => Some(Self::Sum),
            "avg" => Some(Self::Average),
            // GQL's COLLECT_LIST (GF10) is Cypher's collect: nulls are left
            // out, with the warning 01G11.
            "collect" | "collect_list" => Some(Self::Collect),
            "stddev_samp" => Some(Self::StddevSamp),
            "stddev_pop" => Some(Self::StddevPop),
            "percentile_cont" => Some(Self::PercentileCont),
            "percentile_disc" => Some(Self::PercentileDisc),
            "min" => Some(Self::Min),
            "max" => Some(Self::Max),
            _ => None,
        }
    }

    /// The name EXPLAIN and column naming print.
    pub fn name(self) -> &'static str {
        match self {
            Self::Count => "count",
            Self::Sum => "sum",
            Self::Average => "avg",
            Self::Collect => "collect",
            Self::Min => "min",
            Self::Max => "max",
            Self::StddevSamp => "stddev_samp",
            Self::StddevPop => "stddev_pop",
            Self::PercentileCont => "percentile_cont",
            Self::PercentileDisc => "percentile_disc",
        }
    }

    /// Whether the function is a binary set function, which takes a fraction
    /// beside its input (§20.9).
    pub fn takes_fraction(self) -> bool {
        matches!(self, Self::PercentileCont | Self::PercentileDisc)
    }
}

/// A function that reads one row and returns one value, as opposed to an
/// [`AggregateFunction`], which reads a group.
///
/// Kept apart from the aggregates on purpose: a scalar call inside RETURN does
/// not make the projection group, and an aggregate inside a scalar call does.
/// Treating `coalesce` as an aggregate because the lowering's function arm
/// used to recognise only aggregates would group rows that must not be.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ScalarFunction {
    /// Internal MATCH-clause trail constraint; not callable by Cypher name.
    RelationshipsUnique,
    /// `list[index]`, Cypher's list element access; not callable by name.
    /// The index counts from 0, a negative one counts back from the end, and
    /// one past either end is null.
    ListElement,
    /// `coalesce(a, b, ...)`: the first argument that is not null, else null.
    Coalesce,
    /// `toUpper(s)`: the string in upper case, by Unicode case mapping.
    ToUpper,
    /// `toLower(s)`: the string in lower case, by Unicode case mapping.
    ToLower,
    /// `toString(v)`: a value's string spelling.
    ToString,
    /// `toInteger(v)`: a value as an integer, or null when it has none.
    ToInteger,
    /// `size(s)`: the number of characters in a string.
    Size,
    /// `NULLIF(a, b)`: null when `a = b`, otherwise `a` (§20.7).
    NullIf,
    /// `CHAR_LENGTH(s)` / `CHARACTER_LENGTH(s)`: length in characters (§20.22).
    CharLength,
    /// `UPPER(s)` (§20.24 fold): as [`Self::ToUpper`], and normalized again
    /// when its input was NFC.
    Upper,
    /// `LOWER(s)` (§20.24 fold).
    Lower,
    /// `LEFT(s, n)`: the first `n` characters (§20.24).
    Left,
    /// `RIGHT(s, n)`: the last `n` characters (§20.24).
    Right,
    /// `TRIM(s)` and `TRIM([BOTH] c FROM s)`: every leading and trailing
    /// occurrence of the one character `c`, a space by default (§20.24).
    TrimBoth,
    /// `TRIM(LEADING c FROM s)`.
    TrimLeading,
    /// `TRIM(TRAILING c FROM s)`.
    TrimTrailing,
    /// `LTRIM(s [, chars])`: leading characters in the set `chars` (§20.24).
    LTrim,
    /// `RTRIM(s [, chars])`.
    RTrim,
    /// `BTRIM(s [, chars])`: `RTRIM(LTRIM(s, chars), chars)`.
    BTrim,
    /// `NORMALIZE(s [, form])` (§20.24); the form travels as a string
    /// argument, `NFC` when absent.
    Normalize,
    /// `ABS(n)` (§20.22).
    Abs,
    /// `MOD(n, m)`: the remainder with the sign of `n`.
    Mod,
    /// `FLOOR(n)`.
    Floor,
    /// `CEIL(n)` / `CEILING(n)`.
    Ceil,
    /// `SQRT(n)`, which is `POWER(n, 0.5)`.
    Sqrt,
    /// `EXP(n)`.
    Exp,
    /// `LN(n)`; also Cypher's one-argument `log(n)`.
    Ln,
    /// `LOG(base, n)`.
    Log,
    /// `LOG10(n)`, which is `LOG(10, n)`.
    Log10,
    /// `POWER(base, exponent)`.
    Power,
    Sin,
    Cos,
    Tan,
    Cot,
    Asin,
    Acos,
    Atan,
    Sinh,
    Cosh,
    Tanh,
    /// `DEGREES(radians)`.
    Degrees,
    /// `RADIANS(degrees)`.
    Radians,
    /// `x IS TRUE` (§20.20); not callable by name.
    IsTrue,
    /// `x IS FALSE`; not callable by name.
    IsFalse,
    /// `x IS UNKNOWN`; not callable by name.
    IsUnknown,
    /// `s IS [form] NORMALIZED` (§19.7), the form a string argument; not
    /// callable by name.
    IsNormalized,
    /// `ELEMENT_ID(x)` (§20.10, G100): a string naming the element, unique
    /// within the request (ID076).
    ElementId,
    /// Cypher's numeric internal node/relationship identity, never a property.
    Id,
    /// Internal lowering helper for a stored `id` on a computed element/record.
    IdProperty,
    /// `ALL_DIFFERENT(a, b, ...)` (§19.11, G113).
    AllDifferent,
    /// `BYTE_LENGTH(b)` and `OCTET_LENGTH(b)` (§20.22, GV35).
    ByteLength,
    /// `DATE([string | record])` and `CURRENT_DATE` (§20.27, GV39).
    Date,
    /// `ZONED_TIME(...)` and `CURRENT_TIME` (GV40); Cypher's `time`.
    ZonedTime,
    /// `LOCAL_TIME(...)` (GV39); Cypher's `localtime`.
    LocalTime,
    /// `ZONED_DATETIME(...)` and `CURRENT_TIMESTAMP` (GV40); Cypher's
    /// `datetime`.
    ZonedDatetime,
    /// `LOCAL_DATETIME(...)` and `LOCAL_TIMESTAMP` (GV39); Cypher's
    /// `localdatetime`.
    LocalDatetime,
    /// `DURATION(string | record)` (§20.29, GV41).
    Duration,
    /// `DURATION_BETWEEN(a, b) [qualifier]` (§20.28, GV41), the qualifier
    /// a third, string argument.
    DurationBetween,
    /// `SAME(a, b, ...)` (§19.12, G114).
    Same,
    /// `CARDINALITY(list)` (§20.22, GF12): the number of elements.
    Cardinality,
    /// `PATH_LENGTH(p)` (§20.22, GF04): the path's number of edges.
    PathLength,
    /// `ELEMENTS(p)` (§20.16, GF04): the path's nodes and edges, in order.
    Elements,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ScalarFunctionFamily {
    Coalesce,
    RelationshipTrail,
    ListElement,
    CypherConversion,
    Element,
    Temporal,
    Gql,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ScalarArgumentRole {
    Value,
    ElementVariable,
    RelationshipTrail,
}

impl ScalarFunction {
    /// Dispatch policy, kept separate where Cypher conversions differ from GQL.
    pub fn family(self) -> ScalarFunctionFamily {
        match self {
            Self::Date
            | Self::ZonedTime
            | Self::LocalTime
            | Self::ZonedDatetime
            | Self::LocalDatetime
            | Self::Duration
            | Self::DurationBetween => ScalarFunctionFamily::Temporal,
            Self::Coalesce => ScalarFunctionFamily::Coalesce,
            Self::RelationshipsUnique => ScalarFunctionFamily::RelationshipTrail,
            Self::ListElement => ScalarFunctionFamily::ListElement,
            Self::ToUpper | Self::ToLower | Self::ToString | Self::ToInteger | Self::Size => ScalarFunctionFamily::CypherConversion,
            Self::Id
            | Self::IdProperty
            | Self::ElementId
            | Self::AllDifferent
            | Self::Same
            | Self::Cardinality
            | Self::PathLength
            | Self::Elements => ScalarFunctionFamily::Element,
            Self::NullIf
            | Self::CharLength
            | Self::ByteLength
            | Self::Upper
            | Self::Lower
            | Self::Left
            | Self::Right
            | Self::TrimBoth
            | Self::TrimLeading
            | Self::TrimTrailing
            | Self::LTrim
            | Self::RTrim
            | Self::BTrim
            | Self::Normalize
            | Self::Abs
            | Self::Mod
            | Self::Floor
            | Self::Ceil
            | Self::Sqrt
            | Self::Exp
            | Self::Ln
            | Self::Log
            | Self::Log10
            | Self::Power
            | Self::Sin
            | Self::Cos
            | Self::Tan
            | Self::Cot
            | Self::Asin
            | Self::Acos
            | Self::Atan
            | Self::Sinh
            | Self::Cosh
            | Self::Tanh
            | Self::Degrees
            | Self::Radians
            | Self::IsTrue
            | Self::IsFalse
            | Self::IsUnknown
            | Self::IsNormalized => ScalarFunctionFamily::Gql,
        }
    }

    pub fn argument_role(self, _index: usize) -> ScalarArgumentRole {
        match self {
            Self::ElementId | Self::AllDifferent | Self::Same => ScalarArgumentRole::ElementVariable,
            Self::RelationshipsUnique => ScalarArgumentRole::RelationshipTrail,
            _ => ScalarArgumentRole::Value,
        }
    }

    /// The function a Cypher name denotes, matched case-insensitively as
    /// Cypher function names are.
    pub fn from_name(name: &str) -> Option<Self> {
        match name.to_ascii_lowercase().as_str() {
            "byte_length" | "octet_length" => Some(Self::ByteLength),
            "date" | "current_date" => Some(Self::Date),
            "zoned_time" | "current_time" | "time" => Some(Self::ZonedTime),
            "local_time" | "localtime" => Some(Self::LocalTime),
            "zoned_datetime" | "current_timestamp" | "datetime" => Some(Self::ZonedDatetime),
            "local_datetime" | "local_timestamp" | "localdatetime" => Some(Self::LocalDatetime),
            "duration" => Some(Self::Duration),
            "duration_between" => Some(Self::DurationBetween),
            "coalesce" => Some(Self::Coalesce),
            "toupper" => Some(Self::ToUpper),
            "tolower" => Some(Self::ToLower),
            "tostring" => Some(Self::ToString),
            "tointeger" => Some(Self::ToInteger),
            "size" => Some(Self::Size),
            "nullif" => Some(Self::NullIf),
            "char_length" | "character_length" => Some(Self::CharLength),
            "upper" => Some(Self::Upper),
            "lower" => Some(Self::Lower),
            "left" => Some(Self::Left),
            "right" => Some(Self::Right),
            "trim" => Some(Self::TrimBoth),
            "ltrim" => Some(Self::LTrim),
            "rtrim" => Some(Self::RTrim),
            "btrim" => Some(Self::BTrim),
            "normalize" => Some(Self::Normalize),
            "abs" => Some(Self::Abs),
            "mod" => Some(Self::Mod),
            "floor" => Some(Self::Floor),
            "ceil" | "ceiling" => Some(Self::Ceil),
            "sqrt" => Some(Self::Sqrt),
            "exp" => Some(Self::Exp),
            "ln" => Some(Self::Ln),
            "log" => Some(Self::Log),
            "log10" => Some(Self::Log10),
            "power" => Some(Self::Power),
            "sin" => Some(Self::Sin),
            "cos" => Some(Self::Cos),
            "tan" => Some(Self::Tan),
            "cot" => Some(Self::Cot),
            "asin" => Some(Self::Asin),
            "acos" => Some(Self::Acos),
            "atan" => Some(Self::Atan),
            "sinh" => Some(Self::Sinh),
            "cosh" => Some(Self::Cosh),
            "tanh" => Some(Self::Tanh),
            "degrees" => Some(Self::Degrees),
            "radians" => Some(Self::Radians),
            "element_id" => Some(Self::ElementId),
            "id" => Some(Self::Id),
            "all_different" => Some(Self::AllDifferent),
            "same" => Some(Self::Same),
            "cardinality" => Some(Self::Cardinality),
            "path_length" => Some(Self::PathLength),
            "elements" => Some(Self::Elements),
            _ => None,
        }
    }

    /// The name EXPLAIN and column naming print.
    pub fn name(self) -> &'static str {
        match self {
            Self::RelationshipsUnique => "relationships_unique",
            Self::ListElement => "list_element",
            Self::ByteLength => "byte_length",
            Self::Date => "date",
            Self::ZonedTime => "zoned_time",
            Self::LocalTime => "local_time",
            Self::ZonedDatetime => "zoned_datetime",
            Self::LocalDatetime => "local_datetime",
            Self::Duration => "duration",
            Self::DurationBetween => "duration_between",
            Self::Coalesce => "coalesce",
            Self::ToUpper => "toUpper",
            Self::ToLower => "toLower",
            Self::ToString => "toString",
            Self::ToInteger => "toInteger",
            Self::Size => "size",
            Self::NullIf => "nullif",
            Self::CharLength => "char_length",
            Self::Upper => "upper",
            Self::Lower => "lower",
            Self::Left => "left",
            Self::Right => "right",
            Self::TrimBoth => "trim",
            Self::TrimLeading => "trim_leading",
            Self::TrimTrailing => "trim_trailing",
            Self::LTrim => "ltrim",
            Self::RTrim => "rtrim",
            Self::BTrim => "btrim",
            Self::Normalize => "normalize",
            Self::Abs => "abs",
            Self::Mod => "mod",
            Self::Floor => "floor",
            Self::Ceil => "ceil",
            Self::Sqrt => "sqrt",
            Self::Exp => "exp",
            Self::Ln => "ln",
            Self::Log => "log",
            Self::Log10 => "log10",
            Self::Power => "power",
            Self::Sin => "sin",
            Self::Cos => "cos",
            Self::Tan => "tan",
            Self::Cot => "cot",
            Self::Asin => "asin",
            Self::Acos => "acos",
            Self::Atan => "atan",
            Self::Sinh => "sinh",
            Self::Cosh => "cosh",
            Self::Tanh => "tanh",
            Self::Degrees => "degrees",
            Self::Radians => "radians",
            Self::IsTrue => "is_true",
            Self::IsFalse => "is_false",
            Self::IsUnknown => "is_unknown",
            Self::IsNormalized => "is_normalized",
            Self::ElementId => "element_id",
            Self::Id => "id",
            Self::IdProperty => "stored_id_property",
            Self::AllDifferent => "all_different",
            Self::Same => "same",
            Self::Cardinality => "cardinality",
            Self::PathLength => "path_length",
            Self::Elements => "elements",
        }
    }

    /// The temporal functions, which read the request timestamp (§20.27
    /// General Rule 1) as a last argument the planner adds.
    pub fn is_temporal(self) -> bool {
        matches!(
            self,
            Self::Date
                | Self::ZonedTime
                | Self::LocalTime
                | Self::ZonedDatetime
                | Self::LocalDatetime
                | Self::Duration
                | Self::DurationBetween
        )
    }

    /// Whether the function accepts `count` source arguments.
    pub fn accepts_arity(self, count: usize) -> bool {
        match self {
            Self::RelationshipsUnique => true,
            Self::ListElement => count == 2,
            Self::Date | Self::ZonedTime | Self::LocalTime | Self::ZonedDatetime | Self::LocalDatetime => count <= 1,
            Self::Duration => count == 1,
            Self::DurationBetween => count == 2 || count == 3,
            Self::Coalesce => count >= 1,
            Self::ToUpper
            | Self::ToLower
            | Self::ToString
            | Self::ToInteger
            | Self::Size
            | Self::CharLength
            | Self::ByteLength
            | Self::Upper
            | Self::Lower
            | Self::Abs
            | Self::Floor
            | Self::Ceil
            | Self::Sqrt
            | Self::Exp
            | Self::Ln
            | Self::Log10
            | Self::Sin
            | Self::Cos
            | Self::Tan
            | Self::Cot
            | Self::Asin
            | Self::Acos
            | Self::Atan
            | Self::Sinh
            | Self::Cosh
            | Self::Tanh
            | Self::Degrees
            | Self::Radians
            | Self::IsTrue
            | Self::IsFalse
            | Self::IsUnknown
            | Self::Id
            | Self::IdProperty
            | Self::ElementId
            | Self::Cardinality
            | Self::PathLength
            | Self::Elements => count == 1,
            Self::AllDifferent | Self::Same => count >= 2,
            Self::IsNormalized => count == 2,
            Self::NullIf | Self::Left | Self::Right | Self::Mod | Self::Power => count == 2,
            // Cypher's `log(n)` is the natural logarithm; GQL's `LOG(base, n)`.
            Self::Log => count == 1 || count == 2,
            // `TRIM(s)`, or `s` and the trim character from the FROM form.
            Self::TrimBoth | Self::TrimLeading | Self::TrimTrailing => count == 1 || count == 2,
            Self::LTrim | Self::RTrim | Self::BTrim | Self::Normalize => count == 1 || count == 2,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LogicalExpression {
    Binding(Symbol),
    Identity(Symbol),
    Property {
        binding: Symbol,
        property: Symbol,
    },
    Parameter(Symbol),
    Literal(ScalarValue),
    Aggregate {
        function: AggregateFunction,
        /// `count(DISTINCT x)`: the group's distinct non-null inputs only.
        distinct: bool,
        expression: Option<Box<LogicalExpression>>,
        /// The fraction of `PERCENTILE_CONT` and `PERCENTILE_DISC`, the
        /// independent value expression of a binary set function (§20.9):
        /// read once for the group, not for each of its rows.
        fraction: Option<Box<LogicalExpression>>,
        /// A horizontal aggregate (§20.1 Syntax Rule 7, GE09): the group
        /// variable whose elements it aggregates within one record, rather
        /// than over a group of records.
        group: Option<Symbol>,
    },
    /// A scalar function call; see [`ScalarFunction`].
    Function {
        function: ScalarFunction,
        arguments: Vec<LogicalExpression>,
    },
    /// A list literal outside `IN`, such as `[1, n.name]`: a value, evaluated
    /// per row. A list never becomes a stored property, which is why this is
    /// an expression rather than a `ScalarValue` variant.
    List(Vec<LogicalExpression>),
    /// `CASE`; see `hydradb_cypher_ast::Expression::Case`. Evaluated lazily:
    /// only the branch taken is computed.
    Case {
        operand: Option<Box<LogicalExpression>>,
        branches: Vec<(LogicalExpression, LogicalExpression)>,
        default: Option<Box<LogicalExpression>>,
    },
    Unary {
        operator: LogicalUnaryOperator,
        expression: Box<LogicalExpression>,
    },
    Binary {
        left: Box<LogicalExpression>,
        operator: LogicalBinaryOperator,
        right: Box<LogicalExpression>,
    },
    /// `<expression> IN <list>`. The candidate list is kept out of
    /// `LogicalExpression` on purpose: a list is not a scalar, and giving
    /// `ScalarValue` a list variant would force every exhaustive match in the
    /// engine and in the HydraDB storage adapter to grow an arm for a value
    /// that can never be stored as a property.
    In {
        expression: Box<LogicalExpression>,
        list: LogicalInList,
    },
    /// A predicate on the node or edge bound to `binding` that reads its
    /// record (GQL §19.8 to §19.13); see [`ElementTestKind`].
    ElementTest {
        binding: Symbol,
        test: ElementTestKind,
    },
    /// `x IS [NOT] TYPED T` (GQL §19.6): whether the value of `expression`
    /// is of `value_type`.
    TypeTest {
        expression: Box<LogicalExpression>,
        value_type: GqlType,
        negated: bool,
    },
    /// `CAST(x AS T)` (GQL §20.8). A cast to a datetime type also reads the
    /// request timestamp, for the current date a time is given.
    Cast {
        expression: Box<LogicalExpression>,
        value_type: GqlType,
        request: Option<Box<LogicalExpression>>,
    },
    /// A record constructor (GQL §20.18): its fields, ordered by name.
    Record(Vec<(Symbol, LogicalExpression)>),
    /// `LET x = a, ... IN body END` (GQL §20.5): the body read with each
    /// name bound to its value.
    Let {
        bindings: Vec<(Symbol, LogicalExpression)>,
        body: Box<LogicalExpression>,
    },
    /// `PATH[n, e, m, ...]` (GQL §20.14): the path of those elements.
    PathConstructor(Vec<LogicalExpression>),
    /// A field of the record `expression` gives (GQL §20.11): null when the
    /// record has no such field.
    Field {
        expression: Box<LogicalExpression>,
        field: Symbol,
    },
    /// GQL's SIMPLE and ACYCLIC path modes (§22.2 General Rule 16 d) ii)):
    /// whether the path through `parts` repeats no node, the first and last
    /// excepted when `simple`. The inner nodes of a variable-length hop
    /// count too.
    PathNodesDistinct {
        simple: bool,
        parts: Vec<PathPart>,
    },
}

impl LogicalExpression {
    /// Direct subexpressions only; callers own aggregate and lexical boundaries.
    pub(crate) fn children(&self) -> Vec<&Self> {
        match self {
            Self::Unary { expression, .. } | Self::TypeTest { expression, .. } | Self::Field { expression, .. } => vec![expression],
            Self::Cast { expression, request, .. } => std::iter::once(expression.as_ref()).chain(request.as_deref()).collect(),
            Self::Record(fields) => fields.iter().map(|(_, value)| value).collect(),
            Self::Let { bindings, body } => bindings.iter().map(|(_, value)| value).chain([body.as_ref()]).collect(),
            Self::Function { arguments, .. } | Self::List(arguments) | Self::PathConstructor(arguments) => arguments.iter().collect(),
            Self::Case {
                operand,
                branches,
                default,
            } => operand
                .as_deref()
                .into_iter()
                .chain(branches.iter().flat_map(|(when, then)| [when, then]))
                .chain(default.as_deref())
                .collect(),
            Self::Binary { left, right, .. } => vec![left, right],
            Self::In { expression, list } => {
                let mut children = vec![expression.as_ref()];
                if let LogicalInList::Values(values) = list {
                    children.extend(values);
                }
                children
            }
            Self::Aggregate { expression, fraction, .. } => expression.iter().chain(fraction).map(Box::as_ref).collect(),
            Self::Binding(_)
            | Self::Identity(_)
            | Self::ElementTest { .. }
            | Self::PathNodesDistinct { .. }
            | Self::Property { .. }
            | Self::Parameter(_)
            | Self::Literal(_) => Vec::new(),
        }
    }

    /// Mutable counterpart of [`Self::children`], in the same written order.
    pub(crate) fn children_mut(&mut self) -> Vec<&mut Self> {
        match self {
            Self::Unary { expression, .. } | Self::TypeTest { expression, .. } | Self::Field { expression, .. } => vec![expression],
            Self::Cast { expression, request, .. } => std::iter::once(expression.as_mut()).chain(request.as_deref_mut()).collect(),
            Self::Record(fields) => fields.iter_mut().map(|(_, value)| value).collect(),
            Self::Let { bindings, body } => bindings.iter_mut().map(|(_, value)| value).chain([body.as_mut()]).collect(),
            Self::Function { arguments, .. } | Self::List(arguments) | Self::PathConstructor(arguments) => arguments.iter_mut().collect(),
            Self::Case {
                operand,
                branches,
                default,
            } => operand
                .as_deref_mut()
                .into_iter()
                .chain(branches.iter_mut().flat_map(|(when, then)| [when, then]))
                .chain(default.as_deref_mut())
                .collect(),
            Self::Binary { left, right, .. } => vec![left, right],
            Self::In { expression, list } => {
                let mut children = vec![expression.as_mut()];
                if let LogicalInList::Values(values) = list {
                    children.extend(values);
                }
                children
            }
            Self::Aggregate { expression, fraction, .. } => expression.iter_mut().chain(fraction).map(Box::as_mut).collect(),
            Self::Binding(_)
            | Self::Identity(_)
            | Self::ElementTest { .. }
            | Self::PathNodesDistinct { .. }
            | Self::Property { .. }
            | Self::Parameter(_)
            | Self::Literal(_) => Vec::new(),
        }
    }

    /// Incoming names read by the expression, excluding LET's local names.
    pub(crate) fn binding_references(&self) -> BTreeSet<&Symbol> {
        self.binding_references_with_aggregates(true)
    }

    /// Incoming grouping names, excluding vertical aggregate operands.
    pub(crate) fn grouping_references(&self) -> BTreeSet<&Symbol> {
        self.binding_references_with_aggregates(false)
    }

    fn binding_references_with_aggregates(&self, aggregates: bool) -> BTreeSet<&Symbol> {
        match self {
            Self::Aggregate { group: None, .. } if !aggregates => BTreeSet::new(),
            Self::Binding(name) | Self::Identity(name) | Self::Property { binding: name, .. } => BTreeSet::from([name]),
            Self::ElementTest { binding, test } => std::iter::once(binding).chain(test.edge()).collect(),
            Self::PathNodesDistinct { parts, .. } => parts.iter().map(PathPart::name).collect(),
            Self::Let { bindings, body } => bindings
                .iter()
                .flat_map(|(_, value)| value.binding_references_with_aggregates(aggregates))
                .chain(
                    body.binding_references_with_aggregates(aggregates)
                        .into_iter()
                        .filter(|name| bindings.iter().all(|(own, _)| own != *name)),
                )
                .collect(),
            other => other
                .children()
                .into_iter()
                .flat_map(|child| child.binding_references_with_aggregates(aggregates))
                .collect(),
        }
    }
}

/// One element of a path as a row binds it, in the path's order.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PathPart {
    Node(Symbol),
    /// A fixed hop's relationship.
    Edge(Symbol),
    /// A variable-length hop's relationship list, in walk order.
    Edges(Symbol),
}

impl PathPart {
    pub fn name(&self) -> &Symbol {
        match self {
            Self::Node(name) | Self::Edge(name) | Self::Edges(name) => name,
        }
    }
}

/// How the matches of a path pattern union operand read as words (GQL
/// §22.2): its subpath variables, and its elements in written order.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PathWord {
    pub subpaths: Vec<Symbol>,
    pub letters: Vec<WordLetter>,
}

/// One element of a [`PathWord`]: the binding the row holds it under, the
/// variable naming it in the word (`None` for the anonymous symbol), and for
/// a quantified path, the variables naming each iteration's first and last
/// node (§16.7 G035).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WordLetter {
    pub part: PathPart,
    pub name: Option<Symbol>,
    pub iteration: Option<(Option<Symbol>, Option<Symbol>)>,
}

/// What [`LogicalExpression::ElementTest`] asks. Each is unknown when the
/// element is null, and raises `22G03` when it is bound to something that is
/// not the element kind the predicate takes.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ElementTestKind {
    /// §19.9, §22.5: nodes use their label set, edges their one type.
    Labeled(LabelExpression),
    /// `e IS DIRECTED` (§19.8): every HydraDB edge is directed.
    Directed,
    /// `n IS SOURCE OF e` (§19.10), `e` the edge binding.
    SourceOf(Symbol),
    /// `n IS DESTINATION OF e` (§19.10).
    DestinationOf(Symbol),
    /// `PROPERTY_EXISTS(x, p)` (§19.13): whether the element has `p`.
    PropertyExists(Symbol),
    /// Excludes the managed node identity alias.
    StoredPropertyExists(Symbol),
}

impl ElementTestKind {
    /// The edge binding the test reads beside its element, if any.
    pub fn edge(&self) -> Option<&Symbol> {
        match self {
            Self::SourceOf(edge) | Self::DestinationOf(edge) => Some(edge),
            Self::Labeled(_) | Self::Directed | Self::PropertyExists(_) | Self::StoredPropertyExists(_) => None,
        }
    }

    /// The name EXPLAIN and column naming print.
    pub fn name(&self) -> String {
        match self {
            Self::Labeled(labels) => format!("is_labeled({labels})"),
            Self::Directed => "is_directed".to_string(),
            Self::SourceOf(edge) => format!("is_source_of({edge})"),
            Self::DestinationOf(edge) => format!("is_destination_of({edge})"),
            Self::PropertyExists(property) => format!("property_exists({property})"),
            Self::StoredPropertyExists(property) => format!("property_exists({property})"),
        }
    }

    pub(crate) fn expression_name(&self, binding: &Symbol) -> String {
        match self {
            Self::Labeled(labels) => format!("{binding}:{labels}"),
            _ => format!("{binding} {}", self.name()),
        }
    }
}

/// The right-hand side of `IN`, before parameters are resolved.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LogicalInList {
    /// An inline list literal, one entry per element.
    Values(Vec<LogicalExpression>),
    /// A parameter bound to a list. Resolved during physical planning from the
    /// planning context's list parameters.
    Parameter(Symbol),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LogicalProjection {
    pub expression: LogicalExpression,
    pub alias: Option<Symbol>,
}

/// Fixed-record bindings are independent of projected table columns.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ProjectionContext {
    pub fixed: Vec<Symbol>,
    pub table_columns: Vec<Symbol>,
    pub defined: Vec<Symbol>,
    pub explicit_grouping: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LogicalSort {
    pub expression: LogicalExpression,
    pub direction: SortDirection,
    /// An explicit null ordering (GQL §16.17, GA03) that differs from the
    /// implicit one, which puts nulls after every value (IS001): last
    /// ascending, first descending. `None` is the implicit order, which an
    /// ordered index scan already gives.
    pub nulls: Option<NullOrdering>,
}

/// A repeated primary's singleton evaluation scope. Its correlated queries
/// are ordinary child plans, at their own graph-matching depth.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IterationPlan<P> {
    pub plan: Box<P>,
    pub source: Symbol,
    pub relationship: Symbol,
    pub target: Symbol,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum GraphLogicalPlan {
    /// One row that binds nothing: where a query that does not open with
    /// MATCH starts. `RETURN 1`, `WITH 1 AS x` and an opening OPTIONAL MATCH
    /// all read from it. Under a correlated input it is that input's row.
    SingleRow,
    /// `UNWIND expression AS binding`: each input row once per element of the
    /// list the expression evaluates to, with `binding` holding the element.
    /// A null list yields no rows and a non-list value one, unless
    /// `requires_list` (GQL's `FOR`), when a non-list value raises `22G03`.
    Unwind {
        input: Box<GraphLogicalPlan>,
        expression: LogicalExpression,
        binding: Symbol,
        requires_list: bool,
        /// `FOR ... WITH ORDINALITY` or `WITH OFFSET` (GQ11, GQ24): the name
        /// bound to each element's position, and the first position's
        /// number, 1 or 0.
        position: Option<(Symbol, i128)>,
    },
    /// A composite query (GQL §14.2): the arms joined, left to right, by one
    /// conjunction. `all` keeps duplicates; it means nothing to `OTHERWISE`.
    Union {
        arms: Vec<GraphLogicalPlan>,
        all: bool,
        conjunction: QueryConjunction,
    },
    NodeScan {
        binding: Symbol,
        labels: Vec<Symbol>,
    },
    Expand {
        input: Box<GraphLogicalPlan>,
        from: Symbol,
        relationship: Option<Symbol>,
        relationship_types: Vec<Symbol>,
        direction: PatternDirection,
        to: Symbol,
        target_labels: Vec<Symbol>,
    },
    VariableExpand {
        input: Box<GraphLogicalPlan>,
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
        /// Whether the walk may not reuse a relationship: false only under
        /// REPEATABLE ELEMENTS (GQL §16.4, §22.2), for a bounded WALK or a
        /// SIMPLE or ACYCLIC path, whose `nodes` rule bounds it instead.
        trail: bool,
        nodes: WalkNodes,
        iteration: Option<IterationPlan<GraphLogicalPlan>>,
    },
    NaturalJoin {
        left: Box<GraphLogicalPlan>,
        right: Box<GraphLogicalPlan>,
        optional: bool,
    },
    /// `EXISTS { ... }` as a mark join (GQL §19.4): every input row once,
    /// with `binding` holding whether `subquery`, run correlated with that
    /// row, yields at least one row.
    Exists {
        input: Box<GraphLogicalPlan>,
        subquery: Box<GraphLogicalPlan>,
        binding: Symbol,
    },
    /// `VALUE { ... }` (GQL §20.6): every input row once, with `binding`
    /// holding `column` of the one row `subquery`, run correlated with that
    /// row, yields; null when it yields none.
    ScalarSubquery {
        input: Box<GraphLogicalPlan>,
        subquery: Box<GraphLogicalPlan>,
        column: Symbol,
        binding: Symbol,
    },
    /// `[OPTIONAL] CALL { ... }` (GQL §15.1, §15.2): every row `subquery`
    /// yields, run correlated with each input row, beside that row, its
    /// `columns` carried in; an OPTIONAL call keeps a row it yields nothing
    /// for, the columns null.
    CallProcedure {
        input: Box<GraphLogicalPlan>,
        subquery: Box<GraphLogicalPlan>,
        /// The input names the procedure reads (§15.2 Syntax Rule 7): its
        /// rows are seeded with these alone.
        imports: Vec<Symbol>,
        columns: Vec<Symbol>,
        outputs_to_record: bool,
        optional: bool,
    },
    /// GQL's `FINISH` (§14.10): `input` runs, and the result is omitted.
    /// A statement with an omitted result, then its continuation on the
    /// original working record and a new unit table (§9.2 GR3a(ii)).
    Next {
        input: Box<GraphLogicalPlan>,
        next: Box<GraphLogicalPlan>,
    },
    Finish {
        input: Box<GraphLogicalPlan>,
    },
    /// A path variable and its subpath variables (GQL §16.4, §16.7, G004,
    /// G048): each of `names` bound to the path value `input`'s row binds
    /// through `parts`.
    BindPath {
        input: Box<GraphLogicalPlan>,
        names: Vec<Symbol>,
        parts: Vec<PathPart>,
        /// Group variables of a quantified path's iteration nodes (G035):
        /// each name bound to the list of the path's nodes without its last
        /// (`false`, each iteration's first) or its first (`true`).
        node_groups: Vec<(Symbol, bool)>,
    },
    /// A selective path search (GQL §22.4): `input`'s rows, the matches of
    /// one path pattern, partitioned by the nodes bound to `first` and
    /// `last`, of which each partition keeps what `kind` and `count` select.
    /// A path's length is `fixed_edges` plus the lengths of the relationship
    /// lists named in `lists`.
    SelectPaths {
        input: Box<GraphLogicalPlan>,
        first: Symbol,
        last: Symbol,
        fixed_edges: usize,
        lists: Vec<Symbol>,
        kind: PathSearchKind,
        count: LogicalExpression,
    },
    /// A path pattern union or multiset alternation (GQL §16.7, G030 to
    /// G033): the rows of each of `inputs`, an operand's matches, with the
    /// names another operand binds null. For a union, `words` gives each
    /// operand's word, and a reduced match (§16.4 General Rule 10) two
    /// operands both find is kept once.
    PathUnion {
        inputs: Vec<GraphLogicalPlan>,
        words: Option<Vec<PathWord>>,
    },
    Filter {
        operation: hydradb_cypher_ast::operation::Operation,
        input: Box<GraphLogicalPlan>,
        predicate: LogicalExpression,
    },
    Sort {
        input: Box<GraphLogicalPlan>,
        items: Vec<LogicalSort>,
    },
    Skip {
        input: Box<GraphLogicalPlan>,
        count: LogicalExpression,
    },
    Limit {
        input: Box<GraphLogicalPlan>,
        count: LogicalExpression,
    },
    Project {
        context: crate::plan::logical::ProjectionContext,
        operation: hydradb_cypher_ast::operation::Operation,
        input: Box<GraphLogicalPlan>,
        items: Vec<LogicalProjection>,
        distinct: bool,
        post_sort: Vec<LogicalSort>,
        post_skip: Option<LogicalExpression>,
        post_limit: Option<LogicalExpression>,
    },
}

impl GraphLogicalPlan {
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
            Self::NodeScan { .. }
            | Self::Expand { .. }
            | Self::VariableExpand { .. }
            | Self::NaturalJoin { .. }
            | Self::BindPath { .. }
            | Self::PathUnion { .. }
            | Self::SelectPaths { .. } => operation::MATCH,
        }
    }
}
