//! The anonymous usage report a Turbolay install sends (ENG-3474).
//!
//! This crate holds what the sender and the receiver must agree on, and
//! nothing that needs a runtime: no I/O, no clock, no environment. GraphNode
//! gathers the inputs and sends; the receiver checks what arrives.
//!
//! - [`install_id`]: the random anonymous ID, formatted from caller-supplied bytes.
//! - [`report`]: the report's shape and [`report::validate`], the receiver's
//!   check.
//! - [`delta`]: turning running totals into "since the last report".

pub mod delta;
pub mod install_id;
pub mod report;

pub use install_id::{install_id, is_install_id};
pub use report::{validate, UsageReport, MAX_REPORT_BYTES, SCHEMA_VERSION};
