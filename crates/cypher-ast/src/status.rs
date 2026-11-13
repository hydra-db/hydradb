//! GQLSTATUS: the condition codes of ISO/IEC 39075 (GQL), §23.1, Table 8.
//!
//! A [`GqlStatus`] names one condition: a two-character class and a
//! three-character subclass. Its category says whether it reports success,
//! a warning, no data, extra information, or an exception, and its standard
//! description is spelled exactly as §4.9.2 prescribes.
//!
//! This is the one vocabulary every stage reports in -- the parser adapter, the
//! planner and the executor -- so a client sees the same code for the same
//! condition whichever stage found it.

use std::fmt;

/// What kind of outcome a condition reports (Table 8, "Category").
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum StatusCategory {
    /// `S`: successful completion.
    Success,
    /// `W`: successful completion with a warning.
    Warning,
    /// `N`: completion without data.
    NoData,
    /// `I`: additional information, only ever in a nested status object.
    Informational,
    /// `X`: an exception; the request failed.
    Exception,
}

/// One GQLSTATUS condition.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct GqlStatus {
    code: &'static str,
    category: StatusCategory,
    condition: &'static str,
    /// `None` for the `000` subclass, which has no subcondition.
    subcondition: Option<&'static str>,
}

macro_rules! statuses {
    ($( $(#[$doc:meta])* $name:ident = $code:literal, $category:ident, $condition:literal, $subcondition:expr; )*) => {
        impl GqlStatus {
            $(
                $(#[$doc])*
                pub const $name: Self = Self {
                    code: $code,
                    category: StatusCategory::$category,
                    condition: $condition,
                    subcondition: $subcondition,
                };
            )*

            /// Every condition this crate defines, in Table 8 order.
            pub const ALL: &'static [Self] = &[$(Self::$name),*];
        }
    };
}

statuses! {
    SUCCESS = "00000", Success, "successful completion", None;
    OMITTED_RESULT = "00001", Success, "successful completion", Some("omitted result");

    WARNING = "01000", Warning, "warning", None;
    WARNING_STRING_RIGHT_TRUNCATION = "01004", Warning, "warning", Some("string data, right truncation");
    GRAPH_DOES_NOT_EXIST = "01G03", Warning, "warning", Some("graph does not exist");
    GRAPH_TYPE_DOES_NOT_EXIST = "01G04", Warning, "warning", Some("graph type does not exist");
    /// An aggregate skipped a null input (§20.9, General Rule 5).
    NULL_VALUE_ELIMINATED_IN_SET_FUNCTION = "01G11", Warning, "warning", Some("null value eliminated in set function");

    NO_DATA = "02000", NoData, "no data", None;

    INFORMATIONAL = "03000", Informational, "informational", None;

    CONNECTION_EXCEPTION = "08000", Exception, "connection exception", None;
    TRANSACTION_RESOLUTION_UNKNOWN = "08007", Exception, "connection exception", Some("transaction resolution unknown");

    DATA_EXCEPTION = "22000", Exception, "data exception", None;
    STRING_DATA_RIGHT_TRUNCATION = "22001", Exception, "data exception", Some("string data, right truncation");
    NUMERIC_VALUE_OUT_OF_RANGE = "22003", Exception, "data exception", Some("numeric value out of range");
    NULL_VALUE_NOT_ALLOWED = "22004", Exception, "data exception", Some("null value not allowed");
    INVALID_DATETIME_FORMAT = "22007", Exception, "data exception", Some("invalid date, time, or, datetime format");
    DATETIME_FIELD_OVERFLOW = "22008", Exception, "data exception", Some("datetime field overflow");
    SUBSTRING_ERROR = "22011", Exception, "data exception", Some("substring error");
    DIVISION_BY_ZERO = "22012", Exception, "data exception", Some("division by zero");
    INTERVAL_FIELD_OVERFLOW = "22015", Exception, "data exception", Some("interval field overflow");
    INVALID_CHARACTER_VALUE_FOR_CAST = "22018", Exception, "data exception", Some("invalid character value for cast");
    INVALID_ARGUMENT_FOR_NATURAL_LOGARITHM = "2201E", Exception, "data exception", Some("invalid argument for natural logarithm");
    INVALID_ARGUMENT_FOR_POWER_FUNCTION = "2201F", Exception, "data exception", Some("invalid argument for power function");
    TRIM_ERROR = "22027", Exception, "data exception", Some("trim error");
    /// Corrigendum 1 (§20.22 General Rule 8): a general logarithm whose base or
    /// argument is out of its domain. Not in Table 8, which the corrigendum
    /// does not amend.
    INVALID_ARGUMENT_FOR_GENERAL_LOGARITHM_FUNCTION = "2201Z", Exception, "data exception", Some("invalid argument for general logarithm function");
    /// Corrigendum 1 (§20.16): a list element position out of range.
    LIST_ELEMENT_ERROR_CORRIGENDUM = "2202E", Exception, "data exception", Some("list element error");
    /// Corrigendum 1 (§20.22 General Rule 7): `ACOS` or `ASIN` outside [-1, 1].
    INVALID_ARGUMENT_FOR_TRIGONOMETRIC_FUNCTION = "2202M", Exception, "data exception", Some("invalid argument for trigonometric function");
    /// Corrigendum 1 (§20.8 GR4dvi1): a list exceeds its target bound.
    LIST_DATA_RIGHT_TRUNCATION = "2202F", Exception, "data exception", Some("list data, right truncation");
    NEGATIVE_LIMIT_VALUE = "22G02", Exception, "data exception", Some("negative limit value");
    /// A value of the wrong type for the operation: `'a' + 1`, `UPPER(1)`.
    INVALID_VALUE_TYPE = "22G03", Exception, "data exception", Some("invalid value type");
    VALUES_NOT_COMPARABLE = "22G04", Exception, "data exception", Some("values not comparable");
    INVALID_DATETIME_FUNCTION_FIELD_NAME = "22G05", Exception, "data exception", Some("invalid date, time, or datetime function field name");
    INVALID_DATETIME_FUNCTION_VALUE = "22G06", Exception, "data exception", Some("invalid datetime function value");
    INVALID_DURATION_FUNCTION_FIELD_NAME = "22G07", Exception, "data exception", Some("invalid duration function field name");
    LIST_DATA_RIGHT_TRUNCATION_DEPRECATED = "22G0B", Exception, "data exception", Some("list data, right truncation");
    LIST_ELEMENT_ERROR = "22G0C", Exception, "data exception", Some("list element error");
    INVALID_NUMBER_OF_PATHS_OR_GROUPS = "22G0F", Exception, "data exception", Some("invalid number of paths or groups");
    INVALID_DURATION_FORMAT = "22G0H", Exception, "data exception", Some("invalid duration format");
    MULTIPLE_ASSIGNMENTS_TO_A_GRAPH_ELEMENT_PROPERTY = "22G0M", Exception, "data exception", Some("multiple assignments to a graph element property");
    NODE_LABELS_BELOW_SUPPORTED_MINIMUM = "22G0N", Exception, "data exception", Some("number of node labels below supported minimum");
    NODE_LABELS_EXCEED_SUPPORTED_MAXIMUM = "22G0P", Exception, "data exception", Some("number of node labels exceeds supported maximum");
    EDGE_LABELS_BELOW_SUPPORTED_MINIMUM = "22G0Q", Exception, "data exception", Some("number of edge labels below supported minimum");
    EDGE_LABELS_EXCEED_SUPPORTED_MAXIMUM = "22G0R", Exception, "data exception", Some("number of edge labels exceeds supported maximum");
    NODE_PROPERTIES_EXCEED_SUPPORTED_MAXIMUM = "22G0S", Exception, "data exception", Some("number of node properties exceeds supported maximum");
    EDGE_PROPERTIES_EXCEED_SUPPORTED_MAXIMUM = "22G0T", Exception, "data exception", Some("number of edge properties exceeds supported maximum");
    RECORD_FIELDS_DO_NOT_MATCH = "22G0U", Exception, "data exception", Some("record fields do not match");
    REFERENCE_VALUE_INVALID_BASE_TYPE = "22G0V", Exception, "data exception", Some("reference value, invalid base type");
    REFERENCE_VALUE_INVALID_CONSTRAINED_TYPE = "22G0W", Exception, "data exception", Some("reference value, invalid constrained type");
    RECORD_DATA_FIELD_UNASSIGNABLE = "22G0X", Exception, "data exception", Some("record data, field unassignable");
    RECORD_DATA_FIELD_MISSING = "22G0Y", Exception, "data exception", Some("record data, field missing");
    MALFORMED_PATH = "22G0Z", Exception, "data exception", Some("malformed path");
    PATH_DATA_RIGHT_TRUNCATION = "22G10", Exception, "data exception", Some("path data, right truncation");
    REFERENCE_VALUE_REFERENT_DELETED = "22G11", Exception, "data exception", Some("reference value, referent deleted");
    /// Table 8 still lists it, but Corrigendum 1 moves every rule that raised
    /// it to [`Self::INVALID_VALUE_TYPE`]; nothing here raises it.
    INVALID_VALUE_TYPE_DEPRECATED = "22G12", Exception, "data exception", Some("invalid value type");
    INVALID_GROUP_VARIABLE_VALUE = "22G13", Exception, "data exception", Some("invalid group variable value");
    INCOMPATIBLE_TEMPORAL_INSTANT_UNIT_GROUPS = "22G14", Exception, "data exception", Some("incompatible temporal instant unit groups");

    INVALID_TRANSACTION_STATE = "25000", Exception, "invalid transaction state", None;
    ACTIVE_GQL_TRANSACTION = "25G01", Exception, "invalid transaction state", Some("active GQL-transaction");
    CATALOG_AND_DATA_STATEMENT_MIXING_NOT_SUPPORTED = "25G02", Exception, "invalid transaction state", Some("catalog and data statement mixing not supported");
    READ_ONLY_GQL_TRANSACTION = "25G03", Exception, "invalid transaction state", Some("read-only GQL-transaction");
    ACCESSING_MULTIPLE_GRAPHS_NOT_SUPPORTED = "25G04", Exception, "invalid transaction state", Some("accessing multiple graphs not supported");

    INVALID_TRANSACTION_TERMINATION = "2D000", Exception, "invalid transaction termination", None;

    TRANSACTION_ROLLBACK = "40000", Exception, "transaction rollback", None;
    STATEMENT_COMPLETION_UNKNOWN = "40003", Exception, "transaction rollback", Some("statement completion unknown");

    /// Text that is not a GQL-program, or that violates a Syntax Rule with no
    /// more specific condition (§4.7.3).
    SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION = "42000", Exception, "syntax error or access rule violation", None;
    INVALID_SYNTAX = "42001", Exception, "syntax error or access rule violation", Some("invalid syntax");
    /// A reference to a name that is not in scope; its diagnostic record
    /// carries `INVALID_REFERENCE` (§23.2).
    INVALID_REFERENCE = "42002", Exception, "syntax error or access rule violation", Some("invalid reference");
    USE_OF_VISUALLY_CONFUSABLE_IDENTIFIERS = "42004", Exception, "syntax error or access rule violation", Some("use of visually confusable identifiers");
    EDGE_LABELS_BELOW_SUPPORTED_MINIMUM_SYNTAX = "42006", Exception, "syntax error or access rule violation", Some("number of edge labels below supported minimum");
    EDGE_LABELS_EXCEED_SUPPORTED_MAXIMUM_SYNTAX = "42007", Exception, "syntax error or access rule violation", Some("number of edge labels exceeds supported maximum");
    EDGE_PROPERTIES_EXCEED_SUPPORTED_MAXIMUM_SYNTAX = "42008", Exception, "syntax error or access rule violation", Some("number of edge properties exceeds supported maximum");
    NODE_LABELS_BELOW_SUPPORTED_MINIMUM_SYNTAX = "42009", Exception, "syntax error or access rule violation", Some("number of node labels below supported minimum");
    NODE_LABELS_EXCEED_SUPPORTED_MAXIMUM_SYNTAX = "42010", Exception, "syntax error or access rule violation", Some("number of node labels exceeds supported maximum");
    NODE_PROPERTIES_EXCEED_SUPPORTED_MAXIMUM_SYNTAX = "42011", Exception, "syntax error or access rule violation", Some("number of node properties exceeds supported maximum");
    NODE_TYPE_KEY_LABELS_BELOW_SUPPORTED_MINIMUM = "42012", Exception, "syntax error or access rule violation", Some("number of node type key labels below supported minimum");
    NODE_TYPE_KEY_LABELS_EXCEED_SUPPORTED_MAXIMUM = "42013", Exception, "syntax error or access rule violation", Some("number of node type key labels exceeds supported maximum");
    EDGE_TYPE_KEY_LABELS_BELOW_SUPPORTED_MINIMUM = "42014", Exception, "syntax error or access rule violation", Some("number of edge type key labels below supported minimum");
    EDGE_TYPE_KEY_LABELS_EXCEED_SUPPORTED_MAXIMUM = "42015", Exception, "syntax error or access rule violation", Some("number of edge type key labels exceeds supported maximum");

    DEPENDENT_OBJECT_ERROR = "G1000", Exception, "dependent object error", None;
    EDGES_STILL_EXIST = "G1001", Exception, "dependent object error", Some("edges still exist");
    ENDPOINT_NODE_IS_DELETED = "G1002", Exception, "dependent object error", Some("endpoint node is deleted");
    ENDPOINT_NODE_NOT_IN_CURRENT_WORKING_GRAPH = "G1003", Exception, "dependent object error", Some("endpoint node not in current working graph");

    GRAPH_TYPE_VIOLATION = "G2000", Exception, "graph type violation", None;

    /// Valid GQL that this implementation does not support: ISO/IEC 9075's
    /// condition, a standard-defined class GQL leaves to other standards.
    FEATURE_NOT_SUPPORTED = "0A000", Exception, "feature not supported", None;
    /// A failure that is not the query's: storage, limits, cancellation.
    /// Class `50` is implementation-defined (§23.1).
    GENERAL_PROCESSING_EXCEPTION = "50000", Exception, "general processing exception", None;
}

impl GqlStatus {
    /// Historical name for the condition corrected by Cor1 §20.8.
    pub const ARRAY_DATA_RIGHT_TRUNCATION: Self = Self::LIST_DATA_RIGHT_TRUNCATION;

    /// The five-character GQLSTATUS, such as `22012`.
    pub const fn code(self) -> &'static str {
        self.code
    }

    /// The two-character class.
    pub fn class(self) -> &'static str {
        &self.code[..2]
    }

    /// The three-character subclass; `000` means none.
    pub fn subclass(self) -> &'static str {
        &self.code[2..]
    }

    pub const fn category(self) -> StatusCategory {
        self.category
    }

    pub const fn is_exception(self) -> bool {
        matches!(self.category, StatusCategory::Exception)
    }

    /// The condition this one belongs to, for example "data exception".
    pub const fn condition(self) -> &'static str {
        self.condition
    }

    pub const fn subcondition(self) -> Option<&'static str> {
        self.subcondition
    }

    /// The condition for a code, when it is one this crate defines.
    pub fn from_code(code: &str) -> Option<Self> {
        Self::ALL.iter().find(|status| status.code == code).copied()
    }

    /// The standard description of §4.9.2: `error: data exception - division
    /// by zero`, `warn: null value eliminated in set function`, `note:
    /// successful completion`.
    pub fn standard_description(self) -> String {
        let text = match (self.category, self.subcondition) {
            (StatusCategory::Warning | StatusCategory::Informational, Some(subcondition)) => subcondition.to_string(),
            (_, Some(subcondition)) => format!("{} - {subcondition}", self.condition),
            (_, None) => self.condition.to_string(),
        };
        let prefix = match self.category {
            StatusCategory::Exception => "error",
            StatusCategory::Warning => "warn",
            StatusCategory::Success | StatusCategory::NoData => "note",
            StatusCategory::Informational => "info",
        };
        format!("{prefix}: {text}")
    }

    /// How strongly this condition claims to be the outcome's status (§4.9.3):
    /// transaction rollback over every other exception, every exception over
    /// every completion, no data over a warning, a warning over success.
    pub fn precedence(self) -> u8 {
        match self.category {
            StatusCategory::Exception if self.class() == "40" => 5,
            StatusCategory::Exception => 4,
            StatusCategory::NoData => 3,
            StatusCategory::Warning => 2,
            StatusCategory::Success => 1,
            StatusCategory::Informational => 0,
        }
    }
}

impl fmt::Display for GqlStatus {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.code)
    }
}

/// The diagnostic record of an exception (§23.2), which GQL-status objects
/// carry under Feature GA08.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct DiagnosticRecord {
    /// The operation that raised the condition (Table 9), such as
    /// `MATCH STATEMENT`.
    pub operation: &'static str,
    /// Its code: positive for GQL operations, negative for
    /// implementation-defined ones, 0 for an unrecognized statement.
    pub operation_code: i32,
    /// The working schema, when there is one.
    pub current_schema: Option<String>,
    /// For `42002`, the name that could not be resolved.
    pub invalid_reference: Option<String>,
    /// Implementation-defined fields; each name starts with `_` (§4.9.2).
    pub additional: Vec<(String, String)>,
}

/// One GQL-status object (§4.9.2): a condition, its description, and the
/// diagnostic record of an exception.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GqlStatusObject {
    pub status: GqlStatus,
    /// The standard description, then any implementation-defined detail.
    pub description: String,
    pub diagnostic: Option<DiagnosticRecord>,
    /// A further status object, such as the cause of this one.
    pub cause: Option<Box<GqlStatusObject>>,
}

impl GqlStatusObject {
    /// A status object with the standard description alone.
    pub fn new(status: GqlStatus) -> Self {
        Self {
            status,
            description: status.standard_description(),
            diagnostic: None,
            cause: None,
        }
    }

    /// A status object whose description adds `detail` after the standard
    /// description.
    pub fn with_detail(status: GqlStatus, detail: impl AsRef<str>) -> Self {
        let detail = detail.as_ref();
        let mut object = Self::new(status);
        if !detail.is_empty() {
            object.description = format!("{}: {detail}", object.description);
        }
        object
    }

    pub fn with_diagnostic(mut self, diagnostic: DiagnosticRecord) -> Self {
        self.diagnostic = Some(diagnostic);
        self
    }
}

/// The operation codes of Table 9, for diagnostic records.
pub mod operation {
    /// `(identifier, code)` of one operation.
    pub type Operation = (&'static str, i32);

    pub const SESSION_SET_SCHEMA: Operation = ("SESSION SET SCHEMA COMMAND", 1);
    pub const SESSION_SET_GRAPH: Operation = ("SESSION SET PROPERTY GRAPH COMMAND", 2);
    pub const SESSION_SET_TIME_ZONE: Operation = ("SESSION SET TIME ZONE COMMAND", 3);
    pub const SESSION_RESET: Operation = ("SESSION RESET COMMAND", 7);
    pub const SESSION_CLOSE: Operation = ("SESSION CLOSE COMMAND", 8);
    pub const START_TRANSACTION: Operation = ("START TRANSACTION COMMAND", 50);
    pub const ROLLBACK: Operation = ("ROLLBACK COMMAND", 51);
    pub const COMMIT: Operation = ("COMMIT COMMAND", 52);
    pub const INSERT: Operation = ("INSERT STATEMENT", 500);
    pub const SET: Operation = ("SET STATEMENT", 501);
    pub const REMOVE: Operation = ("REMOVE STATEMENT", 502);
    pub const DELETE: Operation = ("DELETE STATEMENT", 503);
    pub const MATCH: Operation = ("MATCH STATEMENT", 600);
    pub const FILTER: Operation = ("FILTER STATEMENT", 601);
    pub const LET: Operation = ("LET STATEMENT", 602);
    pub const FOR: Operation = ("FOR STATEMENT", 603);
    pub const ORDER_BY_AND_PAGE: Operation = ("ORDER BY AND PAGE STATEMENT", 604);
    pub const RETURN: Operation = ("RETURN STATEMENT", 605);
    pub const SELECT: Operation = ("SELECT STATEMENT", 606);
    pub const CALL_PROCEDURE: Operation = ("CALL PROCEDURE STATEMENT", 800);
    /// A statement the engine could not attribute: a zero-length identifier.
    pub const UNRECOGNIZED: Operation = ("", 0);
}
