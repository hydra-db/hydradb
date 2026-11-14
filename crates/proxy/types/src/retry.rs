//! What is retryable, as one pure function.
//!
//! - refused before running (NotALeader or 421, connection refused, the
//!   connection lost before PULL was sent, since RUN only prepares): always, at
//!   whichever node owns it now
//! - outcome unknown (lost after PULL, before any row went out): never. A safe
//!   retry needs an idempotency key at the current lease holder, which the
//!   proxy does not carry
//! - once a row reached the caller: never, so a stream never silently restarts
//! - IdempotencyConflict and every other error the node chose to send: never

use crate::{Access, NodeId, Operation};

/// How a unit's attempt ended, as far as retrying cares.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Outcome {
    /// The unit finished and the caller has the answer.
    Completed,
    /// The node refused because it does not own the cell, before running.
    /// `owner` is its hint, when it had one.
    RefusedNotOwner { owner: Option<NodeId> },
    /// Could not open or check out a connection to the node.
    ConnectFailed,
    /// The backend connection died before PULL was sent. Nothing ran.
    LostBeforePull,
    /// The backend connection died after PULL, before any row reached the
    /// caller. The write may have run.
    LostAfterPull,
    /// At least one row reached the caller.
    RowsSent,
    /// The node answered with an error of its own. The caller gets it.
    NodeError,
}

/// What to do after an attempt.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RetryDecision {
    /// Try again at this node: the owner a refusal named.
    RetryAt(NodeId),
    /// Try the same node again. Writes have exactly one candidate, the owner,
    /// and are never moved off it for health.
    RetrySame,
    /// Try the next candidate. Reads have several.
    RetryNext,
    /// Hand the outcome to the caller.
    GiveUp,
}

/// Attempts per unit, the first included.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RetryBudget {
    pub max_attempts: u32,
}

impl Default for RetryBudget {
    fn default() -> Self {
        Self { max_attempts: 3 }
    }
}

/// Decide whether attempt number `attempt` (1-based) is retried.
pub fn classify(operation: Operation, outcome: &Outcome, attempt: u32, budget: RetryBudget) -> RetryDecision {
    if attempt >= budget.max_attempts {
        return RetryDecision::GiveUp;
    }
    match outcome {
        Outcome::RefusedNotOwner { owner: Some(owner) } => RetryDecision::RetryAt(owner.clone()),
        // No hint means the node had nothing truthful to name; another try at
        // the same computed owner would get the same answer.
        Outcome::RefusedNotOwner { owner: None } => RetryDecision::GiveUp,
        Outcome::ConnectFailed | Outcome::LostBeforePull => match operation.access {
            Access::Write => RetryDecision::RetrySame,
            Access::Read => RetryDecision::RetryNext,
        },
        Outcome::LostAfterPull | Outcome::RowsSent | Outcome::NodeError | Outcome::Completed => RetryDecision::GiveUp,
    }
}
