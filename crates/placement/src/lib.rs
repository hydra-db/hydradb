//! Placement and liveness for Turbolay cell writers.
//!
//! `owner(scope, cell) = argmax over live nodes of H(scope ‖ cell ‖ node)`.
//!
//! This crate answers one question — *which node should own this cell's
//! writer* — and deliberately knows nothing about graphs, shards or queries.
//! It does not depend on `slatedb-graph-kernel`; the dependency runs the other
//! way, which is what keeps it testable in isolation and keeps the kernel's
//! types out of the routing layer.
//!
//! Placement is advisory. The SlateDB writer epoch remains the only authority
//! on who actually holds a cell's writer; this crate decides who should *try*.
//!
//! # Time
//!
//! Nothing in this crate reads a clock. [`heartbeat::list_heartbeats`] is the
//! single impure boundary: it takes `now` and collapses each object's
//! `LastModified` into a [`heartbeat::HeartbeatEntry::age`]. Every function
//! below that point is pure, synchronous and time-free, which is what makes the
//! liveness rules testable by hand-building entries instead of sleeping.
//! Decision 10 of `docs/plans/2026-07-25-rendezvous-placement.md`.
//!
//! # The three modules
//!
//! [`hash`] is the frozen rendezvous score — pure functions over `&str`, and a
//! wire format: changing the key encoding, the finalizer or the tie-break
//! reshuffles every cell in the fleet.
//!
//! [`heartbeat`] is the object layout under `_graph_nodes/v1/` and the LIST
//! boundary. Everything placement needs is in the object *name*; the body is
//! observability and is never fetched to make a decision.
//!
//! [`liveness`] turns entries into a live set — configured membership
//! intersected with heartbeat freshness — and carries the two rules that make
//! that safe under failure: a bounded grace on a failed LIST, and a startup
//! window in which a peer that has never published counts as live rather than
//! dead.
//!
//! What is *not* here: the publisher loop, which needs the node's readiness
//! state and belongs in `graph-node`. This crate is entirely passive.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

pub mod hash;
pub mod heartbeat;
pub mod liveness;

#[cfg(test)]
mod fault_store;

pub use hash::{owner, rank, score};
pub use heartbeat::{HeartbeatEntry, PlacementError};
pub use liveness::{live_nodes, LiveNodeSet, NodeView};
