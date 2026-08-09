//! The coarse failure vocabulary carried as `error.class`.
//!
//! `src/core/error.rs` already has a well-shaped taxonomy; it just never
//! reaches a log in structured form, because errors surface as `%error`
//! display strings that a backend cannot group. This enum is the grouping key.
//!
//! # Why the vocabulary lives here and the mapping does not
//!
//! Mapping `GraphError` to a class needs to name kernel types, and this crate
//! deliberately does not depend on the kernel — the same rule
//! `turbolay-placement` follows. So the *vocabulary* is defined here, where
//! every sink can see it, and the *mapping* is a `GraphError::class()` method
//! to be added on the kernel side when the paths are instrumented. That is the
//! open decision recorded in §8 of the plan, and it is resolved in the
//! direction that keeps the match arms next to the variants they map, so a
//! newly added variant is an obvious omission rather than a silent `Other`.
//!
//! Until then this enum stands alone and is fully usable: anything that can
//! classify itself calls [`ErrorClass::as_str`] and records it under
//! [`crate::semconv::ERROR_CLASS`].

use std::fmt;

/// Coarse failure class for the `error.class` attribute.
///
/// The variants mirror the `GraphError` groupings in
/// `docs/plans/2026-07-26-otel-telemetry-crate.md` §6.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
#[non_exhaustive]
pub enum ErrorClass {
    /// Write contention: conditional-write conflicts, idempotency conflicts,
    /// exhausted retries. **Expected, not alarming** — see below.
    Contention,
    /// Writer lifecycle: a write requires a writer, a cell was dropped, or
    /// SlateDB closed a writer as fenced. **Expected occasionally, not alarming
    /// per occurrence.**
    Fencing,
    /// A request reached a node that cannot serve the selected cell. Drivers
    /// normally recover by refreshing their routing table.
    Routing,
    /// Epoch and snapshot disagreements — the BFG-007 / BFG-009 / BFG-011
    /// family.
    Freshness,
    /// Admission control refusal or saturation.
    Admission,
    /// A request exceeded its configured execution deadline.
    Timeout,
    /// Cypher the engine will not accept: parse failures, unsupported features,
    /// missing parameters.
    Query,
    /// Scope mismatch or denied access.
    Authz,
    /// Corrupt values and malformed keys. Never expected.
    Corruption,
    /// Refused configuration, such as an unsafe durability setting.
    Config,
    /// The object store or SlateDB itself.
    Storage,
    /// The sparse kernel.
    Kernel,
    /// Deliberate escape hatch so a caller can classify something this list
    /// does not cover, rather than mislabel it.
    Other,
}

impl ErrorClass {
    /// The attribute value.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Contention => "contention",
            Self::Fencing => "fencing",
            Self::Routing => "routing",
            Self::Freshness => "freshness",
            Self::Admission => "admission",
            Self::Timeout => "timeout",
            Self::Query => "query",
            Self::Authz => "authz",
            Self::Corruption => "corruption",
            Self::Config => "config",
            Self::Storage => "storage",
            Self::Kernel => "kernel",
            Self::Other => "other",
        }
    }

    /// Whether this class is part of normal operation.
    ///
    /// Contention retries, routing refreshes, and occasional writer handovers
    /// are normal. The class exists so a dashboard can chart the rate and alert
    /// on a change in it — not so every occurrence pages somebody. Getting this
    /// wrong in the first week generates noise that discredits the whole
    /// effort, so the distinction is in the type rather than in a runbook
    /// nobody reads.
    pub fn is_expected_during_normal_operation(self) -> bool {
        matches!(self, Self::Contention | Self::Fencing | Self::Routing)
    }

    /// Every class, for tests and for building dashboards.
    pub const ALL: &'static [ErrorClass] = &[
        Self::Contention,
        Self::Fencing,
        Self::Routing,
        Self::Freshness,
        Self::Admission,
        Self::Timeout,
        Self::Query,
        Self::Authz,
        Self::Corruption,
        Self::Config,
        Self::Storage,
        Self::Kernel,
        Self::Other,
    ];
}

impl fmt::Display for ErrorClass {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// The outcome of a unit of work, recorded under [`crate::semconv::OUTCOME`].
///
/// [`Outcome::Skipped`] is the variant that earns this type its place. The
/// indexer's common case is "this generation is already current, do nothing"
/// (`src/bin/graph-indexer.rs:224`), which today produces no output at all — so
/// a healthy idle indexer and a stopped one look identical. An explicit skipped
/// outcome distinguishes them, and that distinction is most of what an indexer
/// needs to report.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum Outcome {
    /// Work was performed and succeeded.
    Success,
    /// No work was needed.
    Skipped,
    /// Work was attempted and failed.
    Failed,
}

impl Outcome {
    /// The attribute value.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::Skipped => "skipped",
            Self::Failed => "failed",
        }
    }
}

impl fmt::Display for Outcome {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn class_strings_are_unique() {
        let mut names: Vec<_> = ErrorClass::ALL.iter().map(|class| class.as_str()).collect();
        names.sort_unstable();
        let before = names.len();
        names.dedup();
        assert_eq!(before, names.len(), "two classes share a string");
    }

    #[test]
    fn class_strings_are_lower_snake() {
        for class in ErrorClass::ALL {
            let name = class.as_str();
            assert!(
                name.chars().all(|c| c.is_ascii_lowercase() || c == '_'),
                "{name} is not lower_snake"
            );
        }
    }

    #[test]
    fn only_contention_fencing_and_routing_are_expected() {
        let expected: Vec<_> = ErrorClass::ALL
            .iter()
            .filter(|class| class.is_expected_during_normal_operation())
            .copied()
            .collect();
        assert_eq!(
            expected,
            vec![
                ErrorClass::Contention,
                ErrorClass::Fencing,
                ErrorClass::Routing,
            ]
        );
    }

    #[test]
    fn outcome_strings_are_distinct() {
        assert_eq!(Outcome::Success.as_str(), "success");
        assert_eq!(Outcome::Skipped.as_str(), "skipped");
        assert_eq!(Outcome::Failed.as_str(), "failed");
    }
}
