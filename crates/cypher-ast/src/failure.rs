/// Why a query could not run, at the granularity a metric label can carry.
///
/// One vocabulary for both engines, so a dashboard can put the legacy and the
/// experimental failure for the same query shape side by side. It lives here
/// rather than in either engine because the Cypher 25 lowering, the
/// experimental planner and the legacy lowering all have to name the same
/// buckets, and this is the one crate all three already depend on.
///
/// Bounded and closed on purpose: the free-text message beside it stays in
/// logs and traces, and only this becomes a label.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum QueryFailureReason {
    /// The text is not valid Cypher.
    ParseError,
    /// Cypher 25 accepted the text, but typed lowering stopped somewhere it
    /// could not attribute to a clause.
    UntypedStatement,
    /// `MATCH` shapes: relationship direction, variable length, hints,
    /// anonymous or untyped relationships, bindings.
    Pattern,
    /// `WHERE` predicates and the operators inside them.
    Where,
    /// `RETURN` and `WITH` projections, `DISTINCT`, aggregates, functions.
    Return,
    /// `ORDER BY`, `SKIP` and `LIMIT`.
    OrderWindow,
    /// `UNION` arms.
    Union,
    /// `UNWIND` batch shapes.
    Unwind,
    /// `CREATE`, `MERGE`, `SET`, `DELETE` and `REMOVE`.
    Mutation,
    /// Parameters: composite values outside `UNWIND`, missing values.
    Parameter,
    /// `CALL` procedures.
    Procedure,
    /// The request, not the query: limits, cursors, bookmarks, databases.
    InvalidRequest,
    /// A supported query that failed while evaluating a value: overflow,
    /// division by zero, an operand of the wrong type.
    Evaluation,
    /// A supported iterative algorithm that did not reach its tolerance
    /// within its iteration limit, so it has no answer to return.
    NotConverged,
    /// A supported read over an index generation whose changes since cannot
    /// be read back, so its answer would be unchecked.
    IndexBehind,
    /// A supported read or write of a property a write-back has started and
    /// not completed, whose values and index disagree until a run of it
    /// completes.
    WriteBackInProgress,
    /// Anything not yet placed in a bucket. A non-zero rate means a failure
    /// site needs one.
    Other,
}

impl QueryFailureReason {
    /// Every reason, in [`Self::index`] order.
    pub const ALL: [Self; 17] = [
        Self::ParseError,
        Self::UntypedStatement,
        Self::Pattern,
        Self::Where,
        Self::Return,
        Self::OrderWindow,
        Self::Union,
        Self::Unwind,
        Self::Mutation,
        Self::Parameter,
        Self::Procedure,
        Self::InvalidRequest,
        Self::Evaluation,
        Self::NotConverged,
        Self::IndexBehind,
        Self::WriteBackInProgress,
        Self::Other,
    ];

    pub const COUNT: usize = Self::ALL.len();

    /// The label value. Stable wire vocabulary: renaming one breaks every
    /// dashboard and alert that selects it.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ParseError => "parse_error",
            Self::UntypedStatement => "untyped_statement",
            Self::Pattern => "unsupported_pattern",
            Self::Where => "unsupported_where",
            Self::Return => "unsupported_return",
            Self::OrderWindow => "unsupported_order_window",
            Self::Union => "unsupported_union",
            Self::Unwind => "unsupported_unwind",
            Self::Mutation => "unsupported_mutation",
            Self::Parameter => "unsupported_parameter",
            Self::Procedure => "unsupported_procedure",
            Self::InvalidRequest => "invalid_request",
            Self::Evaluation => "evaluation_error",
            Self::NotConverged => "not_converged",
            Self::IndexBehind => "index_behind",
            Self::WriteBackInProgress => "write_back_in_progress",
            Self::Other => "unsupported_other",
        }
    }

    /// Position in [`Self::ALL`], for dimensioning a counter array.
    pub const fn index(self) -> usize {
        self as usize
    }

    /// The bucket a clause keyword opens, for a lowering that stopped at one
    /// it does not handle. `None` for a word that is not a clause keyword.
    pub fn for_clause_keyword(word: &str) -> Option<Self> {
        let word = word.to_ascii_uppercase();
        Some(match word.as_str() {
            "MATCH" | "OPTIONAL" => Self::Pattern,
            "WHERE" => Self::Where,
            "RETURN" | "WITH" => Self::Return,
            "ORDER" | "SKIP" | "LIMIT" => Self::OrderWindow,
            "UNION" => Self::Union,
            "UNWIND" => Self::Unwind,
            "CREATE" | "MERGE" | "SET" | "DELETE" | "DETACH" | "REMOVE" | "FOREACH" => Self::Mutation,
            "CALL" => Self::Procedure,
            _ => return None,
        })
    }
}
