//! The field denylist, and the machinery that applies it.
//!
//! Turbolay is multi-tenant. Query parameters, property maps and bookmarks all
//! carry customer data, and a telemetry pipeline ships to a third-party backend
//! by design — so "do not log parameter values" cannot be a convention that 50
//! call sites are each trusted to remember. The failure mode of the convention
//! is tenant data in someone else's SaaS, discovered months later.
//!
//! So the rule lives in exactly one function, [`is_redacted`], and is applied
//! mechanically at every sink: the fmt layer via [`RedactingVisitor`], and the
//! OTLP exporters via the processor wrappers in [`crate::otlp`].
//!
//! Redaction replaces the *value* and keeps the *key*. Knowing that a span
//! carried `parameters` is useful; knowing what they were is a liability. A
//! dropped key would also make the omission invisible, and an invisible safety
//! net is one nobody notices has failed.

use tracing::field::{Field, Visit};

/// What a redacted value is replaced with.
pub const REDACTED: &str = "[redacted]";

/// Field names whose values never leave the process.
///
/// Matching is on the whole name and on a `.`-suffix, so `parameters` also
/// covers `query.parameters` — see [`is_redacted`].
///
/// Ordering is irrelevant; grouped by what they leak.
const DENYLIST: &[&str] = &[
    // Query inputs — the tenant's data, verbatim.
    "parameters",
    "params",
    "parameter_values",
    "query_parameters",
    // Graph payloads.
    "properties",
    "property_value",
    "property_values",
    "metadata",
    "vertex_properties",
    "edge_properties",
    // Credentials.
    "token",
    "bearer",
    "authorization",
    "auth",
    "password",
    "secret",
    "credentials",
    "api_key",
    // Opaque client state that encodes positions and, transitively, tenancy.
    "bookmark",
    "bookmarks",
];

/// Whether a field's value must be replaced before export.
///
/// Matches the full name (`parameters`) or any dotted suffix
/// (`query.parameters`, `turbolay.query.parameters`). Suffix matching rather
/// than substring matching is deliberate: substring would swallow
/// `parameter_count`, which is a harmless integer and genuinely useful.
pub fn is_redacted(name: &str) -> bool {
    DENYLIST.iter().any(|denied| {
        name == *denied
            || name
                .strip_suffix(denied)
                .is_some_and(|prefix| prefix.ends_with('.'))
    })
}

/// A [`Visit`] wrapper that substitutes [`REDACTED`] for denylisted values and
/// forwards everything else untouched.
///
/// Wrapping the visitor rather than post-processing formatted output is what
/// makes this total: it sits between the event and *every* formatter, so a new
/// sink cannot forget to apply it.
pub struct RedactingVisitor<'a> {
    inner: &'a mut dyn Visit,
}

impl<'a> RedactingVisitor<'a> {
    /// Wrap a visitor.
    pub fn new(inner: &'a mut dyn Visit) -> Self {
        Self { inner }
    }

    /// `true` when this field must be replaced. Extracted so every `record_*`
    /// arm reads the same way.
    fn deny(&self, field: &Field) -> bool {
        is_redacted(field.name())
    }
}

impl Visit for RedactingVisitor<'_> {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        if self.deny(field) {
            self.inner.record_str(field, REDACTED);
        } else {
            self.inner.record_debug(field, value);
        }
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        if self.deny(field) {
            self.inner.record_str(field, REDACTED);
        } else {
            self.inner.record_str(field, value);
        }
    }

    // Numeric and boolean fields cannot be denylisted in practice — the
    // denylist names string-ish payloads — but a `parameters = 3` would still
    // be caught rather than slipping through a type-shaped hole.
    fn record_i64(&mut self, field: &Field, value: i64) {
        if self.deny(field) {
            self.inner.record_str(field, REDACTED);
        } else {
            self.inner.record_i64(field, value);
        }
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        if self.deny(field) {
            self.inner.record_str(field, REDACTED);
        } else {
            self.inner.record_u64(field, value);
        }
    }

    fn record_i128(&mut self, field: &Field, value: i128) {
        if self.deny(field) {
            self.inner.record_str(field, REDACTED);
        } else {
            self.inner.record_i128(field, value);
        }
    }

    fn record_u128(&mut self, field: &Field, value: u128) {
        if self.deny(field) {
            self.inner.record_str(field, REDACTED);
        } else {
            self.inner.record_u128(field, value);
        }
    }

    fn record_bool(&mut self, field: &Field, value: bool) {
        if self.deny(field) {
            self.inner.record_str(field, REDACTED);
        } else {
            self.inner.record_bool(field, value);
        }
    }

    fn record_f64(&mut self, field: &Field, value: f64) {
        if self.deny(field) {
            self.inner.record_str(field, REDACTED);
        } else {
            self.inner.record_f64(field, value);
        }
    }

    fn record_error(&mut self, field: &Field, value: &(dyn std::error::Error + 'static)) {
        if self.deny(field) {
            self.inner.record_str(field, REDACTED);
        } else {
            self.inner.record_error(field, value);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plain_names_are_denied() {
        assert!(is_redacted("parameters"));
        assert!(is_redacted("token"));
        assert!(is_redacted("password"));
        assert!(is_redacted("bookmark"));
    }

    #[test]
    fn dotted_suffixes_are_denied() {
        assert!(is_redacted("query.parameters"));
        assert!(is_redacted("turbolay.query.parameters"));
        assert!(is_redacted("http.authorization"));
    }

    /// The reason for suffix-rather-than-substring matching. `parameter_count`
    /// is a harmless integer and one of the more useful things to log.
    #[test]
    fn substring_lookalikes_survive() {
        assert!(!is_redacted("parameter_count"));
        assert!(!is_redacted("token_bucket_depth"));
        assert!(!is_redacted("authorization_latency_ms"));
        assert!(!is_redacted("has_bookmark"));
    }

    #[test]
    fn ordinary_fields_survive() {
        assert!(!is_redacted("cell_id"));
        assert!(!is_redacted("turbolay.cell_id"));
        assert!(!is_redacted("elapsed_ms"));
        assert!(!is_redacted("message"));
    }

    /// A prefix that merely *ends* with a denied word but has no dot boundary
    /// is not a match — `mytoken` is somebody's unrelated field.
    #[test]
    fn suffix_without_dot_boundary_survives() {
        assert!(!is_redacted("mytoken"));
        assert!(!is_redacted("subparameters"));
    }
}
