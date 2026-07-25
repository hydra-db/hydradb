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
//! Still to land (see `docs/plans/2026-07-25-rendezvous-placement.md`):
//! `heartbeat` (write, list, parse, liveness) and `directory` (live set =
//! configured membership ∩ fresh heartbeats).

#![forbid(unsafe_code)]
#![deny(missing_docs)]

pub mod hash;

pub use hash::{owner, rank, score};
