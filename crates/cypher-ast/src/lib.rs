//! Parser-independent Cypher abstract syntax tree.
//!
//! Parser implementations depend on this crate, never the other way around.
//! These types describe Cypher syntax; graph planning and execution semantics
//! belong to the later graph IR.

#![forbid(unsafe_code)]

pub mod bytes;
mod dialect;
mod expression;
mod failure;
pub mod numeric;
mod path_factor;
mod query;
mod status;
mod value_type;

pub use dialect::{CypherDialect, CypherVersion};
pub use expression::{
    BinaryOperator, ElementTest, Expression, Identifier, IntervalField, IntervalQualifier, Literal, SyntaxFragment, TemporalLiteralKind,
    UnaryOperator, ValueKeyword,
};
pub use failure::QueryFailureReason;
pub use path_factor::{PathFactor, SimplifiedPath};
pub use query::{
    CallClause, Clause, CreateClause, DeleteClause, Direction, Document, EffectClassification, FilterClause, ForPosition, ForeachClause,
    IterationNodes, LabelExpression, LetClause, MatchClause, MatchMode, MergeClause, MutationClause, NodePattern, NullOrdering,
    OrderByAndPageClause, OrderDirection, PathAlternatives, PathLength, PathMode, PathOperand, PathPattern, PathPrefix, PathSearch,
    PathSearchKind, PatternElement, Projection, ProjectionItems, PropertiesUpdate, Query, QueryConjunction, RelationshipPattern,
    RemoveClause, RemoveItem, ReturnClause, SetClause, SetItem, SetQuantifier, SortItem, Statement, UnionArm, UnwindClause, WithClause,
    WriteClassification,
};
pub use status::{operation, DiagnosticRecord, GqlStatus, GqlStatusObject, StatusCategory};
pub use value_type::{FloatSize, IntegerSize, ValueType, ValueTypeKind};
