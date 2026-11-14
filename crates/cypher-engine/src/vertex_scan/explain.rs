use std::fmt::Write;

use super::{GuardedAccess, PreparedVertexScan, VertexAccess};
use crate::plan::explain::{
    redacting_literals, render_bound, render_derived, render_expression, render_sort, render_symbols, render_value,
};
use crate::PropertyPointSet;

impl PreparedVertexScan {
    pub(crate) fn render(&self, depth: usize, output: &mut String) {
        let indent = "  ".repeat(depth);
        let _ = write!(
            output,
            "{indent}VertexScan(provider=slatedb, access={}, binding={}, labels={}",
            self.access_kind(),
            self.binding(),
            render_symbols(self.labels())
        );
        match &self.plan {
            VertexAccess::IdPoint { value } => {
                let _ = write!(output, ", value={}", render_value(value));
            }
            VertexAccess::IdPoints { values } => {
                let values = if redacting_literals() {
                    "...".to_string()
                } else {
                    values.iter().map(render_value).collect::<Vec<_>>().join(", ")
                };
                let _ = write!(output, ", values=[{values}]");
            }
            VertexAccess::PropertyPoints { property, points } => {
                let _ = write!(output, ", property={property}");
                match points {
                    PropertyPointSet::Single(value) => {
                        let _ = write!(output, ", value={}", render_value(value));
                    }
                    PropertyPointSet::Multiple(values) => {
                        let values = if redacting_literals() {
                            "...".to_string()
                        } else {
                            values.iter().map(render_value).collect::<Vec<_>>().join(", ")
                        };
                        let _ = write!(output, ", values=[{values}]");
                    }
                }
            }
            VertexAccess::PropertyPresence { property } => {
                let _ = write!(output, ", property={property}");
            }
            VertexAccess::LabelPrefix { label } => {
                let _ = write!(output, ", label={label}");
            }
            VertexAccess::AllVertices => {}
            VertexAccess::OrderedProperty(ordered) => {
                let prefix = ordered.prefix.as_ref().map(render_value).unwrap_or_else(|| "none".to_string());
                let residual = ordered
                    .residual
                    .as_ref()
                    .map(render_expression)
                    .unwrap_or_else(|| "none".to_string());
                let items = ordered.items.iter().map(render_sort).collect::<Vec<_>>().join(", ");
                let _ = write!(
                    output,
                    ", property={}, direction={}, prefix={prefix}, lower={}, upper={}, required={}, residual={residual}, items=[{items}]",
                    ordered.property,
                    ordered.direction,
                    render_bound(ordered.lower.as_ref()),
                    render_bound(ordered.upper.as_ref()),
                    render_derived(ordered.required)
                );
            }
            VertexAccess::Guarded {
                access,
                estimated_rows,
                read_limit,
                ..
            } => {
                match access {
                    GuardedAccess::PropertySeek { property, values } => {
                        let values = if redacting_literals() && values.len() > 1 {
                            "...".to_string()
                        } else {
                            values.iter().map(render_value).collect::<Vec<_>>().join(", ")
                        };
                        let _ = write!(output, ", property={property}, values=[{values}]");
                    }
                    GuardedAccess::LabelScan { label } => {
                        let _ = write!(output, ", label={label}");
                    }
                }
                let _ = write!(
                    output,
                    ", estimated_rows={}, read_limit={}",
                    render_derived(estimated_rows),
                    render_derived(read_limit)
                );
            }
            VertexAccess::Range {
                property,
                lower,
                upper,
                estimated_rows,
                read_limit,
                ..
            } => {
                // `index_only` is a function of the query's shape alone, so it
                // may show in a plan shape; it is rendered only when set, so a
                // range access that hydrates keeps the line it always had.
                let _ = write!(
                    output,
                    ", property={property}, lower={}, upper={}, estimated_rows={}, read_limit={}{}",
                    render_bound(lower.as_ref()),
                    render_bound(upper.as_ref()),
                    render_derived(estimated_rows),
                    render_derived(read_limit),
                    if self.index_only() { ", index_only=true" } else { "" }
                );
            }
        }
        let _ = writeln!(output, ")");
        if let VertexAccess::Guarded { fallback, .. } | VertexAccess::Range { fallback, .. } = &self.plan {
            let _ = writeln!(output, "{indent}  fallback:");
            fallback.render(depth + 2, output);
        }
    }
}
