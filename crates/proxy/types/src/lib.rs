//! Plain data the turbolay proxy routes with, and the retry policy.
//!
//! Nothing here does I/O, reads a clock or holds a lock. [`classify`] is a pure
//! function compiled into the binary: every replica answers a retry question
//! the same way, and only the budget is configuration.

#![forbid(unsafe_code)]

mod retry;
mod route;

pub use retry::{classify, Outcome, RetryBudget, RetryDecision};
pub use route::{Access, NodeId, NodeInfo, Operation, Protocol, Roles, RouteKey, RouteSnapshot};
