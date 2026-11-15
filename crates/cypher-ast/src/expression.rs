use std::fmt;

use crate::QueryFailureReason;

/// A normalized Cypher identifier without quoting delimiters.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Identifier(Box<str>);

impl Identifier {
    pub fn new(value: impl Into<Box<str>>) -> Self {
        Self(value.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for Identifier {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl From<&str> for Identifier {
    fn from(value: &str) -> Self {
        Self::new(value)
    }
}

impl From<String> for Identifier {
    fn from(value: String) -> Self {
        Self::new(value)
    }
}

/// Syntax retained until a typed AST variant is implemented.
///
/// This explicit escape hatch lets parser adapters grow incrementally without
/// leaking their generated context types into the shared AST.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct SyntaxFragment {
    text: Box<str>,
    unsupported: Option<QueryFailureReason>,
    rejection: Option<crate::GqlStatus>,
}

impl SyntaxFragment {
    pub fn new(text: impl Into<Box<str>>) -> Self {
        Self {
            text: text.into(),
            unsupported: None,
            rejection: None,
        }
    }

    /// A fragment retained because typed lowering stopped inside a clause it
    /// could name, so a caller rejecting it can say which one.
    pub fn unsupported(text: impl Into<Box<str>>, reason: QueryFailureReason) -> Self {
        Self {
            text: text.into(),
            unsupported: Some(reason),
            rejection: None,
        }
    }

    pub fn text(&self) -> &str {
        &self.text
    }

    /// Where typed lowering stopped, when the adapter could tell.
    pub fn unsupported_reason(&self) -> Option<QueryFailureReason> {
        self.unsupported
    }

    pub fn rejected(text: impl Into<Box<str>>, reason: QueryFailureReason, status: crate::GqlStatus) -> Self {
        Self {
            text: text.into(),
            unsupported: Some(reason),
            rejection: Some(status),
        }
    }

    pub fn rejection_status(&self) -> Option<crate::GqlStatus> {
        self.rejection
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Literal {
    Null,
    Boolean(bool),
    Integer(i128),
    /// The original normalized spelling is retained to avoid losing decimal
    /// precision before semantic type analysis.
    Float(Box<str>),
    String(Box<str>),
    /// An exact numeric literal with the suffix `M` (GQL §21.2, GL05,
    /// GL06): its digits, without the suffix or underscores.
    Decimal(Box<str>),
    /// A byte string literal, `X'00FF'` (GQL §21.2, GV35).
    Bytes(Box<[u8]>),
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum UnaryOperator {
    Not,
    Plus,
    Minus,
    /// Postfix `<expression> IS NULL`.
    IsNull,
    /// Postfix `<expression> IS NOT NULL`.
    IsNotNull,
    /// GQL's boolean test `<expression> IS TRUE` (§20.20): true only for
    /// true, never unknown.
    IsTrue,
    /// `<expression> IS NOT TRUE`.
    IsNotTrue,
    /// `<expression> IS FALSE`.
    IsFalse,
    /// `<expression> IS NOT FALSE`.
    IsNotFalse,
    /// `<expression> IS UNKNOWN`: whether a boolean is null.
    IsUnknown,
    /// `<expression> IS NOT UNKNOWN`.
    IsNotUnknown,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum BinaryOperator {
    Or,
    Xor,
    And,
    Equal,
    NotEqual,
    LessThan,
    LessThanOrEqual,
    GreaterThan,
    GreaterThanOrEqual,
    Add,
    Subtract,
    Multiply,
    Divide,
    Modulo,
    Power,
    Concatenate,
    StartsWith,
    EndsWith,
    Contains,
    /// `<expression> IN <list>`. The right side is a list literal or a
    /// parameter bound to a list; it is never an ordinary scalar expression.
    In,
}

#[derive(Clone, Debug, PartialEq)]
pub enum Expression {
    Identifier(Identifier),
    Parameter(Identifier),
    Literal(Literal),
    Property {
        target: Box<Expression>,
        key: Identifier,
    },
    /// `target[index]`: one element of a list (Cypher's list element
    /// access). An index counts from 0, a negative index counts back from the
    /// end, and an index past either end is null.
    Index {
        target: Box<Expression>,
        index: Box<Expression>,
    },
    Unary {
        operator: UnaryOperator,
        expression: Box<Expression>,
    },
    Binary {
        left: Box<Expression>,
        operator: BinaryOperator,
        right: Box<Expression>,
    },
    Function {
        name: Vec<Identifier>,
        distinct: bool,
        arguments: Vec<Expression>,
    },
    List(Vec<Expression>),
    Map(Vec<(Identifier, Expression)>),
    /// `CASE [operand] WHEN w THEN t ... [ELSE e] END`. With an operand each
    /// `WHEN` is a value compared to it (the simple form); without, each is a
    /// condition (the searched form).
    Case {
        operand: Option<Box<Expression>>,
        branches: Vec<(Expression, Expression)>,
        default: Option<Box<Expression>>,
    },
    /// `EXISTS { ... }` (GQL §19.4): whether the subquery yields a row for
    /// the current one. Its clauses may read the enclosing query's names, and
    /// need not end in RETURN.
    Exists(Box<crate::Query>),
    /// `VALUE { ... }` (GQL §20.6, GQ18): the one value the subquery returns,
    /// or null when it returns no row.
    Value(Box<crate::Query>),
    /// GQL's normalized predicate (§19.7): `<expression> IS [NOT] [form]
    /// NORMALIZED`, the form one of `NFC`, `NFD`, `NFKC`, `NFKD`; `NFC` when
    /// unwritten.
    Normalized {
        expression: Box<Expression>,
        form: Box<str>,
        negated: bool,
    },
    /// GQL's `SESSION_USER` (§20.3): the session's authorization identifier.
    SessionUser,
    /// An unquoted word GQL reserves for a value that Cypher also lets a
    /// variable spell: the variable when one of that name is in scope where
    /// the word stands, otherwise the value. Which it is depends on scope, so
    /// the parser leaves it to the planner, which knows the names visible at
    /// each clause.
    ValueKeyword {
        keyword: ValueKeyword,
        spelling: Identifier,
    },
    /// `LET x = a, ... IN body END` (GQL §20.5, GE03).
    Let {
        bindings: Vec<(Identifier, Expression)>,
        body: Box<Expression>,
    },
    /// `PATH[n, e, m, ...]` (GQL §20.14, GE06).
    PathConstructor(Vec<Expression>),
    /// A temporal literal (GQL §21.2): `DATE '2026-01-01'`, `TIME '...'`,
    /// `DATETIME '...'` or `TIMESTAMP '...'`, and `DURATION '...'`.
    TemporalLiteral {
        kind: TemporalLiteralKind,
        text: Box<str>,
    },
    /// `x IS [NOT] TYPED T` and `x IS [NOT] :: T` (GQL §19.6, GA06).
    TypeTest {
        expression: Box<Expression>,
        value_type: crate::ValueType,
        negated: bool,
    },
    /// `CAST(x AS T)` (GQL §20.8, GA05).
    Cast {
        expression: Box<Expression>,
        value_type: crate::ValueType,
    },
    /// A predicate on the node or edge bound to `element` (GQL §19.8 to
    /// §19.10, §19.13): `NOT` it when `negated`. The element, and the edge
    /// of SOURCE OF and DESTINATION OF, are variable references held as
    /// subexpressions, so every walk over an expression's children reads and
    /// renames them as it does any other reference.
    ElementTest {
        element: Box<Expression>,
        test: ElementTest,
        negated: bool,
    },
    Syntax(SyntaxFragment),
}

/// A value [`Expression::ValueKeyword`] can stand for.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ValueKeyword {
    /// `UNKNOWN`, the null value of the boolean type (§21.2).
    Unknown,
    /// `SESSION_USER` (§20.3).
    SessionUser,
    /// `CURRENT_DATE`, `CURRENT_TIME`, `CURRENT_TIMESTAMP`, `LOCAL_TIMESTAMP`
    /// and `LOCAL_TIME` without parentheses (§20.27): the function of that
    /// name, in lower case.
    CurrentTemporal(&'static str),
}

impl ValueKeyword {
    /// The keyword an unquoted word is, if it is one, in any case.
    pub fn from_word(word: &str) -> Option<Self> {
        const CURRENT_TEMPORALS: [&str; 5] = ["current_date", "current_time", "current_timestamp", "local_timestamp", "local_time"];
        if word.eq_ignore_ascii_case("UNKNOWN") {
            Some(Self::Unknown)
        } else if word.eq_ignore_ascii_case("SESSION_USER") {
            Some(Self::SessionUser)
        } else {
            CURRENT_TEMPORALS
                .into_iter()
                .find(|name| word.eq_ignore_ascii_case(name))
                .map(Self::CurrentTemporal)
        }
    }

    /// The expression the word means when no variable spells it.
    pub fn value(self) -> Expression {
        match self {
            Self::Unknown => Expression::Literal(Literal::Null),
            Self::SessionUser => Expression::SessionUser,
            Self::CurrentTemporal(name) => Expression::Function {
                name: vec![Identifier::new(name)],
                distinct: false,
                arguments: Vec::new(),
            },
        }
    }
}

impl Expression {
    /// Direct structural children, in evaluation order. Nested queries are
    /// scope boundaries; this is not free-variable or aggregate analysis.
    pub fn children(&self) -> Vec<&Self> {
        match self {
            Self::Function { arguments, .. } | Self::List(arguments) | Self::PathConstructor(arguments) => arguments.iter().collect(),
            Self::Let { bindings, body } => bindings.iter().map(|(_, value)| value).chain([body.as_ref()]).collect(),
            Self::Property { target, .. } => vec![target],
            Self::Index { target, index } => vec![target, index],
            Self::Unary { expression, .. }
            | Self::Normalized { expression, .. }
            | Self::TypeTest { expression, .. }
            | Self::Cast { expression, .. } => vec![expression],
            Self::Binary { left, right, .. } => vec![left, right],
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
            Self::Map(entries) => entries.iter().map(|(_, value)| value).collect(),
            Self::ElementTest { element, test, .. } => match test {
                ElementTest::SourceOf(edge) | ElementTest::DestinationOf(edge) => vec![element, edge],
                ElementTest::Labeled(_) | ElementTest::Directed | ElementTest::PropertyExists(_) => vec![element],
            },
            Self::Exists(_)
            | Self::Value(_)
            | Self::Identifier(_)
            | Self::Parameter(_)
            | Self::Literal(_)
            | Self::SessionUser
            | Self::ValueKeyword { .. }
            | Self::TemporalLiteral { .. }
            | Self::Syntax(_) => Vec::new(),
        }
    }

    /// This expression rebuilt with every direct subexpression replaced by
    /// `rewrite` of it. Subqueries are not entered: their expressions belong
    /// to another scope. Every variant is listed, so a new one cannot be
    /// silently skipped by a rewrite built on this.
    pub fn try_map_children<E>(&self, rewrite: &mut dyn FnMut(&Expression) -> Result<Expression, E>) -> Result<Expression, E> {
        Ok(match self {
            Self::Property { target, key } => Self::Property {
                target: Box::new(rewrite(target)?),
                key: key.clone(),
            },
            Self::Index { target, index } => Self::Index {
                target: Box::new(rewrite(target)?),
                index: Box::new(rewrite(index)?),
            },
            Self::Unary { operator, expression } => Self::Unary {
                operator: *operator,
                expression: Box::new(rewrite(expression)?),
            },
            Self::Binary { left, operator, right } => Self::Binary {
                left: Box::new(rewrite(left)?),
                operator: *operator,
                right: Box::new(rewrite(right)?),
            },
            Self::Function { name, distinct, arguments } => Self::Function {
                name: name.clone(),
                distinct: *distinct,
                arguments: arguments.iter().map(&mut *rewrite).collect::<Result<_, E>>()?,
            },
            Self::List(items) => Self::List(items.iter().map(&mut *rewrite).collect::<Result<_, E>>()?),
            Self::Map(entries) => Self::Map(
                entries
                    .iter()
                    .map(|(key, value)| Ok((key.clone(), rewrite(value)?)))
                    .collect::<Result<_, E>>()?,
            ),
            Self::Case {
                operand,
                branches,
                default,
            } => Self::Case {
                operand: match operand.as_deref() {
                    Some(operand) => Some(Box::new(rewrite(operand)?)),
                    None => None,
                },
                branches: branches
                    .iter()
                    .map(|(when, then)| Ok((rewrite(when)?, rewrite(then)?)))
                    .collect::<Result<_, E>>()?,
                default: match default.as_deref() {
                    Some(default) => Some(Box::new(rewrite(default)?)),
                    None => None,
                },
            },
            Self::Normalized { expression, form, negated } => Self::Normalized {
                expression: Box::new(rewrite(expression)?),
                form: form.clone(),
                negated: *negated,
            },
            Self::ElementTest { element, test, negated } => Self::ElementTest {
                element: Box::new(rewrite(element)?),
                test: match test {
                    ElementTest::SourceOf(edge) => ElementTest::SourceOf(Box::new(rewrite(edge)?)),
                    ElementTest::DestinationOf(edge) => ElementTest::DestinationOf(Box::new(rewrite(edge)?)),
                    test @ (ElementTest::Labeled(_) | ElementTest::Directed | ElementTest::PropertyExists(_)) => test.clone(),
                },
                negated: *negated,
            },
            Self::TypeTest {
                expression,
                value_type,
                negated,
            } => Self::TypeTest {
                expression: Box::new(rewrite(expression)?),
                value_type: value_type.clone(),
                negated: *negated,
            },
            Self::Cast { expression, value_type } => Self::Cast {
                expression: Box::new(rewrite(expression)?),
                value_type: value_type.clone(),
            },
            Self::Let { bindings, body } => Self::Let {
                bindings: bindings
                    .iter()
                    .map(|(name, value)| Ok((name.clone(), rewrite(value)?)))
                    .collect::<Result<_, E>>()?,
                body: Box::new(rewrite(body)?),
            },
            Self::PathConstructor(elements) => Self::PathConstructor(elements.iter().map(&mut *rewrite).collect::<Result<_, E>>()?),
            Self::Identifier(_)
            | Self::Parameter(_)
            | Self::Literal(_)
            | Self::Exists(_)
            | Self::Value(_)
            | Self::SessionUser
            | Self::ValueKeyword { .. }
            | Self::TemporalLiteral { .. }
            | Self::Syntax(_) => self.clone(),
        })
    }

    /// [`Expression::try_map_children`] for a rewrite that cannot fail.
    pub fn map_children(&self, rewrite: &mut dyn FnMut(&Expression) -> Expression) -> Expression {
        let Ok(expression) = self.try_map_children::<std::convert::Infallible>(&mut |expression| Ok(rewrite(expression)));
        expression
    }
}

/// Which temporal literal (GQL §21.2).
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum TemporalLiteralKind {
    Date,
    Time,
    /// `DATETIME '...'`.
    Datetime,
    /// `TIMESTAMP '...'`: GQL's synonym of DATETIME, and SQL's keyword, so
    /// the one that may also take SQL's form (GL12).
    Timestamp,
    Duration,
    /// SQL's `INTERVAL [±] '...' <qualifier>` (§21.2 Syntax Rule 70, GL12).
    /// Its text starts with the sign written before the string, `+` when
    /// none is.
    Interval(IntervalQualifier),
}

/// A SQL <interval qualifier> (ISO/IEC 9075-2 §10.1): `DAY(3) TO SECOND(2)`.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct IntervalQualifier {
    pub start: IntervalField,
    pub end: Option<IntervalField>,
    /// The leading field's precision; two when none is written.
    pub leading_precision: Option<u32>,
    /// The seconds' fractional precision; six when none is written.
    pub fractional_precision: Option<u32>,
}

/// A field of a SQL interval, most significant first.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum IntervalField {
    Year,
    Month,
    Day,
    Hour,
    Minute,
    Second,
}

/// What [`Expression::ElementTest`] asks of its element.
#[derive(Clone, Debug, PartialEq)]
pub enum ElementTest {
    /// `x IS [NOT] LABELED L` or `x:L` (§19.9, G111).
    Labeled(crate::LabelExpression),
    /// `e IS [NOT] DIRECTED` (§19.8, G110).
    Directed,
    /// `n IS [NOT] SOURCE OF e` (§19.10, G112).
    SourceOf(Box<Expression>),
    /// `n IS [NOT] DESTINATION OF e` (§19.10, G112).
    DestinationOf(Box<Expression>),
    /// `PROPERTY_EXISTS(x, p)` (§19.13, G115); never negated.
    PropertyExists(Identifier),
}

impl Expression {
    pub fn syntax(text: impl Into<Box<str>>) -> Self {
        Self::Syntax(SyntaxFragment::new(text))
    }
}
