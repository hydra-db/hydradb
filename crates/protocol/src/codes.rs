//! Neo4j status codes HydraDB sends over Bolt.
//!
//! Drivers act on the code, not the message, so these are a contract: a code
//! moved from `ClientError` to `TransientError` changes whether a driver
//! retries.

/// The request reached a node that does not own the cell. Drivers discard the
/// routing table and retry; the proxy retries at the owner named in the
/// message (see [`crate::owner`]).
pub const NOT_A_LEADER: &str = "Neo.ClientError.Cluster.NotALeader";
/// This node cannot serve right now; ask another router.
pub const DATABASE_UNAVAILABLE: &str = "Neo.TransientError.General.DatabaseUnavailable";
/// Credentials missing or wrong.
pub const UNAUTHORIZED: &str = "Neo.ClientError.Security.Unauthorized";
/// A message that is invalid in the current state, or malformed.
pub const REQUEST_INVALID: &str = "Neo.ClientError.Request.Invalid";
/// BEGIN sent straight to a graph-node, which runs auto-commit units only.
pub const TRANSACTION_START_FAILED: &str = "Neo.ClientError.Transaction.TransactionStartFailed";
/// ROLLBACK of a transaction whose write already committed: the proxy runs a
/// transaction's one statement as an auto-commit unit.
pub const TRANSACTION_ROLLBACK_FAILED: &str = "Neo.DatabaseError.Transaction.TransactionRollbackFailed";
/// A syntax or argument problem with the statement.
pub const STATEMENT_INVALID_SYNTAX: &str = "Neo.ClientError.Statement.InvalidSyntax";
/// The statement is well formed but an argument is not, such as two rows
/// setting one property to different values.
pub const STATEMENT_ARGUMENT_ERROR: &str = "Neo.ClientError.Statement.ArgumentError";
/// The statement could not run against the snapshot it needed.
pub const STATEMENT_EXECUTION_FAILED: &str = "Neo.DatabaseError.Statement.ExecutionFailed";
/// The query ran past its deadline.
pub const TRANSACTION_TERMINATED: &str = "Neo.TransientError.Transaction.Terminated";
/// An idempotency key was reused for a different outcome.
pub const TRANSACTION_INVALID: &str = "Neo.ClientError.Transaction.Invalid";
/// A bookmark the node has not caught up to within the wait budget.
pub const BOOKMARK_TIMEOUT: &str = "Neo.TransientError.Transaction.BookmarkTimeout";
/// Admission refused the request; transient.
pub const RESOURCE_EXHAUSTED: &str = "Neo.TransientError.General.MemoryPoolOutOfMemoryError";
/// Authenticated, but not allowed this scope.
pub const FORBIDDEN: &str = "Neo.ClientError.Security.Forbidden";
/// Anything the node will not describe further.
pub const UNKNOWN_ERROR: &str = "Neo.DatabaseError.General.UnknownError";
