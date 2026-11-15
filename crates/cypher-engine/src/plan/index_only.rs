//! Which statistics-chosen range seeks may answer from the index alone.
//!
//! A `VertexScan` whose access is a property range hydrates every entry it
//! keeps, and for a query whose only use of the binding is `count(n)`, `n.id`
//! or the seek's own property, the record is the whole cost of the seek: the
//! index entry already carries the id and the value. This pass reads the
//! finished plan, once a query is prepared, and sets such a scan's output to
//! `VertexOutput::IndexEntries`, after which the executor asks storage for
//! entries rather than records and binds a record that holds exactly what
//! the entry proves.
//!
//! The rule is deliberately narrow, because a partial record is only safe
//! where nothing can observe that it is partial:
//!
//!   * the seek sits at the bottom of a linear chain of `Project`, `Filter`,
//!     `Sort`, `Skip` and `Limit` reaching the root, so no join, expansion,
//!     subquery or set operator ever sees the binding;
//!   * every expression in that chain reads the binding only in a shape the
//!     entry provably covers: the seek's property, the argument of `count`,
//!     or its identity (`n.id`) as a whole projection item, never inside
//!     another expression;
//!   * a projection item that is the identity makes its name an alias of the
//!     binding, read under the same rule from then on, by the operators
//!     above it and by the projection's own post-projection sort.
//!
//! `RETURN n` is not covered: the engine would return the id, but a client
//! asked for the node and the record it would expect is the one this pass
//! avoids reading. Any shape outside the rule keeps the hydrating seek, which
//! is always correct.

use std::collections::BTreeSet;

use crate::vertex_scan::PreparedVertexScan;
use crate::{AggregateFunction, GraphPhysicalPlan, PhysicalExpression, PhysicalProjection, Symbol};

/// Mark the range access at the bottom of `plan` when the rule above admits it.
pub(crate) fn mark_index_only_range_seeks(plan: &mut GraphPhysicalPlan) {
    if !index_only_chain(plan) {
        return;
    }
    let mut cursor = plan;
    loop {
        match cursor {
            GraphPhysicalPlan::Project { input, .. }
            | GraphPhysicalPlan::Filter { input, .. }
            | GraphPhysicalPlan::Sort { input, .. }
            | GraphPhysicalPlan::Skip { input, .. }
            | GraphPhysicalPlan::Limit { input, .. } => cursor = input,
            GraphPhysicalPlan::VertexScan { scan } => {
                scan.set_index_only();
                return;
            }
            _ => return,
        }
    }
}

/// A result projection that only counts the rows of an index-only range seek
/// under one Filter: `MATCH (n:L) WHERE n.p > $t RETURN count(n)`.
pub(crate) struct CountOnlyRangeSeek<'a> {
    /// The projection's items, every one a count of the seek's rows.
    pub(crate) items: &'a [PhysicalProjection],
    pub(crate) filter: &'a GraphPhysicalPlan,
    pub(crate) predicate: &'a PhysicalExpression,
    pub(crate) scan: &'a GraphPhysicalPlan,
    pub(crate) prepared: &'a PreparedVertexScan,
}

/// The count-only shape of `plan`, when it has one.
///
/// Such a query needs the number of entries the Filter accepts and nothing
/// else, so the executor may walk the range a page at a time and keep no
/// row, instead of collecting every match before counting them. The seek
/// must already be index-only, which proves nothing reads the binding past
/// the entry; every item must be `count(*)` or `count` of the binding, with
/// no group or fraction; and the projection must read no fixed record and
/// carry no sort or window, so its one output row is that count in each
/// column. `DISTINCT` changes nothing: the index holds one entry per vertex
/// and property, so the entries a walk counts are distinct vertices.
pub(crate) fn count_only_range_seek(plan: &GraphPhysicalPlan) -> Option<CountOnlyRangeSeek<'_>> {
    let GraphPhysicalPlan::Project {
        context,
        input,
        items,
        post_sort,
        post_skip,
        post_limit,
        ..
    } = plan
    else {
        return None;
    };
    if !context.fixed.is_empty() || !post_sort.is_empty() || post_skip.is_some() || post_limit.is_some() || items.is_empty() {
        return None;
    }
    let filter = input.as_ref();
    let GraphPhysicalPlan::Filter { input, predicate, .. } = filter else {
        return None;
    };
    let scan = input.as_ref();
    let GraphPhysicalPlan::VertexScan { scan: prepared } = scan else {
        return None;
    };
    if !prepared.index_only() || prepared.range_property().is_none() {
        return None;
    }
    let counts_rows = |expression: &PhysicalExpression| match expression {
        PhysicalExpression::Aggregate {
            function: AggregateFunction::Count,
            expression,
            fraction: None,
            group: None,
            ..
        } => match expression.as_deref() {
            None => true,
            Some(PhysicalExpression::Binding(binding)) => binding == prepared.binding(),
            Some(_) => false,
        },
        _ => false,
    };
    items
        .iter()
        .all(|item| counts_rows(&item.expression))
        .then_some(CountOnlyRangeSeek {
            items,
            filter,
            predicate,
            scan,
            prepared,
        })
}

/// Whether `plan` is a chain the rule admits, ending in a range seek whose
/// binding every operator above reads within the rule.
fn index_only_chain(plan: &GraphPhysicalPlan) -> bool {
    let mut chain = Vec::new();
    let mut cursor = plan;
    let (binding, property) = loop {
        match cursor {
            GraphPhysicalPlan::Project { input, .. }
            | GraphPhysicalPlan::Filter { input, .. }
            | GraphPhysicalPlan::Sort { input, .. }
            | GraphPhysicalPlan::Skip { input, .. }
            | GraphPhysicalPlan::Limit { input, .. } => {
                chain.push(cursor);
                cursor = input;
            }
            GraphPhysicalPlan::VertexScan { scan } => match scan.range_property() {
                Some(property) => break (scan.binding(), property),
                None => return false,
            },
            _ => return false,
        }
    };
    // Upward from the seek, so a projection's aliases are known before the
    // operators that read them.
    let mut aliases = BTreeSet::from([binding.clone()]);
    for operator in chain.into_iter().rev() {
        match operator {
            GraphPhysicalPlan::Filter { predicate, .. } => {
                if !reads_within_rule(predicate, &aliases, property) {
                    return false;
                }
            }
            GraphPhysicalPlan::Sort { items, .. } => {
                if !items.iter().all(|item| reads_within_rule(&item.expression, &aliases, property)) {
                    return false;
                }
            }
            GraphPhysicalPlan::Skip { .. } | GraphPhysicalPlan::Limit { .. } => {}
            GraphPhysicalPlan::Project { items, post_sort, .. } => {
                if !items.iter().all(|item| projects_within_rule(&item.expression, &aliases, property)) {
                    return false;
                }
                // An item that is the vertex's id carries its record forward
                // under the item's name (the engine resolves `x.p` through the
                // id), so the name is read under the rule from here on. Every
                // other admitted item is a value computed from the entry,
                // which no later read can turn back into the record.
                let carried = items
                    .iter()
                    .filter(|item| is_alias_identity(&item.expression, &aliases))
                    .filter_map(projected_name)
                    .collect::<Vec<_>>();
                aliases.extend(carried);
                // The post-projection sort runs over the projection's output,
                // so it reads the names just carried as well as the input's.
                if !post_sort.iter().all(|item| reads_within_rule(&item.expression, &aliases, property)) {
                    return false;
                }
            }
            _ => return false,
        }
    }
    true
}

/// The name a projection item binds: its alias, or for a bare binding the
/// binding itself. A computed item without an alias names nothing a later
/// operator can read.
fn projected_name(item: &PhysicalProjection) -> Option<Symbol> {
    item.alias.clone().or_else(|| match &item.expression {
        PhysicalExpression::Binding(binding) | PhysicalExpression::Identity(binding) => Some(binding.clone()),
        _ => None,
    })
}

/// Whether `expression` is the id of one of `aliases`, as a whole.
fn is_alias_identity(expression: &PhysicalExpression, aliases: &BTreeSet<Symbol>) -> bool {
    matches!(expression, PhysicalExpression::Identity(binding) if aliases.contains(binding))
}

/// Whether a projection item may be computed from the index entry: the
/// vertex's id as the whole item, or anything [`reads_within_rule`] admits.
///
/// The id evaluates to the vertex itself and the row keeps its record beside
/// it, so it is admitted only where nothing reads that record: as the value
/// a projection returns or names. Nested in any expression -- a field, a
/// `LET`, a path, a function, a comparison -- it reaches a reader that would
/// see the partial record.
fn projects_within_rule(expression: &PhysicalExpression, aliases: &BTreeSet<Symbol>, property: &Symbol) -> bool {
    is_alias_identity(expression, aliases) || reads_within_rule(expression, aliases, property)
}

/// Whether every read `expression` makes of `aliases` is one the index
/// entry covers. The rule is an allow-list of shapes, because a partial
/// record answers any other read with a silent null where the full record
/// has a value: missing an index-only plan costs a hydration, and a wrong
/// index-only plan returns wrong data.
///
/// An expression that names no alias reads nothing from the entry and is
/// admitted whole. One that does is admitted only as:
///
///   * the seek's own property of an alias, which the entry carries;
///   * `count(*)`, or `count` of an alias, which reads only whether the row
///     binds it;
///   * an operator that computes a plain value from admitted operands: an
///     arithmetic, comparison or logical operator, a function call, a list,
///     a record, a `CASE`, a cast or a type test. Their operands are plain
///     values, never the vertex, since the vertex itself is never admitted
///     here.
///
/// Everything else that names an alias -- the alias as a value, its id
/// inside an expression, another property, a label or element test, a
/// field, a `LET`, a path -- needs the record.
fn reads_within_rule(expression: &PhysicalExpression, aliases: &BTreeSet<Symbol>, property: &Symbol) -> bool {
    if !mentions_alias(expression, aliases) {
        return true;
    }
    let within = |expression: &PhysicalExpression| reads_within_rule(expression, aliases, property);
    match expression {
        PhysicalExpression::Property { binding, property: read } => aliases.contains(binding) && read == property,
        PhysicalExpression::Aggregate {
            function,
            expression,
            fraction,
            group,
            ..
        } => {
            // A horizontal aggregate's group is a list; an alias never is.
            if group.as_ref().is_some_and(|group| aliases.contains(group)) || !fraction.as_deref().is_none_or(within) {
                return false;
            }
            match expression.as_deref() {
                Some(PhysicalExpression::Binding(binding)) if aliases.contains(binding) => *function == AggregateFunction::Count,
                Some(expression) => within(expression),
                None => true,
            }
        }
        PhysicalExpression::Function { arguments, .. } | PhysicalExpression::List(arguments) => arguments.iter().all(within),
        PhysicalExpression::Case {
            operand,
            branches,
            default,
        } => {
            operand.as_deref().is_none_or(within)
                && branches.iter().all(|(condition, result)| within(condition) && within(result))
                && default.as_deref().is_none_or(within)
        }
        PhysicalExpression::Unary { expression, .. }
        | PhysicalExpression::TypeTest { expression, .. }
        | PhysicalExpression::InList { expression, .. } => within(expression),
        PhysicalExpression::Cast { expression, request, .. } => within(expression) && request.as_deref().is_none_or(within),
        PhysicalExpression::Binary { left, right, .. } => within(left) && within(right),
        PhysicalExpression::Record(fields) => fields.iter().all(|(_, value)| within(value)),
        PhysicalExpression::Binding(_)
        | PhysicalExpression::Identity(_)
        | PhysicalExpression::Value(_)
        | PhysicalExpression::Field { .. }
        | PhysicalExpression::Let { .. }
        | PhysicalExpression::PathConstructor(_)
        | PhysicalExpression::ElementTest { .. }
        | PhysicalExpression::PathNodesDistinct { .. } => false,
    }
}

/// Whether `expression` names any of `aliases` at all, in any form.
fn mentions_alias(expression: &PhysicalExpression, aliases: &BTreeSet<Symbol>) -> bool {
    let mentions = |expression: &PhysicalExpression| mentions_alias(expression, aliases);
    match expression {
        PhysicalExpression::Binding(binding) | PhysicalExpression::Identity(binding) | PhysicalExpression::Property { binding, .. } => {
            aliases.contains(binding)
        }
        PhysicalExpression::Value(_) => false,
        PhysicalExpression::Aggregate {
            expression,
            fraction,
            group,
            ..
        } => {
            group.as_ref().is_some_and(|group| aliases.contains(group))
                || expression.as_deref().is_some_and(mentions)
                || fraction.as_deref().is_some_and(mentions)
        }
        PhysicalExpression::Function { arguments, .. }
        | PhysicalExpression::List(arguments)
        | PhysicalExpression::PathConstructor(arguments) => arguments.iter().any(mentions),
        PhysicalExpression::Case {
            operand,
            branches,
            default,
        } => {
            operand.as_deref().is_some_and(mentions)
                || branches.iter().any(|(condition, result)| mentions(condition) || mentions(result))
                || default.as_deref().is_some_and(mentions)
        }
        PhysicalExpression::Unary { expression, .. }
        | PhysicalExpression::TypeTest { expression, .. }
        | PhysicalExpression::Field { expression, .. }
        | PhysicalExpression::InList { expression, .. } => mentions(expression),
        PhysicalExpression::Cast { expression, request, .. } => mentions(expression) || request.as_deref().is_some_and(mentions),
        PhysicalExpression::Binary { left, right, .. } => mentions(left) || mentions(right),
        PhysicalExpression::ElementTest { binding, test } => {
            aliases.contains(binding) || test.edge().is_some_and(|edge| aliases.contains(edge))
        }
        PhysicalExpression::Record(fields) => fields.iter().any(|(_, value)| mentions(value)),
        PhysicalExpression::Let { bindings, body } => bindings.iter().any(|(_, value)| mentions(value)) || mentions(body),
        PhysicalExpression::PathNodesDistinct { parts, .. } => parts.iter().any(|part| aliases.contains(part.name())),
    }
}
