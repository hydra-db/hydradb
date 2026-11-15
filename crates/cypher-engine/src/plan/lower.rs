use hydradb_cypher_ast::operation;
use std::borrow::Cow;
use std::cell::Cell;
use std::collections::{BTreeSet, VecDeque};

use hydradb_cypher_ast::{
    BinaryOperator, CallClause, Clause, Direction, Document, ElementTest, Expression, GqlStatus, Identifier, LabelExpression, Literal,
    MatchClause, NodePattern, NullOrdering, OrderDirection, PathMode, PathPattern, PatternElement, Projection, ProjectionItems, Query,
    QueryFailureReason, ReturnClause, SetQuantifier, Statement, UnaryOperator, WithClause,
};

use super::resolve_value_type;
use crate::{
    AggregateFunction, ElementTestKind, GraphLogicalPlan, GraphPlanError, GraphPlanResult, LogicalBinaryOperator, LogicalExpression,
    LogicalInList, LogicalProjection, LogicalSort, LogicalUnaryOperator, PathPart, PathWord, PatternDirection, ScalarArgumentRole,
    ScalarFunction, ScalarValue, SortDirection, Symbol, WalkNodes, WordLetter,
};

/// Upper bound on `IN` candidates. A list this long is a caller that should be
/// paging, and an unbounded list would turn one predicate into an unbounded
/// number of index seeks.
///
/// Matches legacy's ceiling (`src/query/opencypher.rs`). It used to be 1,024,
/// which refused a list of 2,000 that the legacy route answered - the same
/// public failure class on both, so nothing watching error classes could see
/// the difference. What bounds the work is the query's shared index-candidate
/// tally, which the multi-value seek now charges; the list length only bounds
/// how many seeks may be attempted before that tally stops them.
pub(crate) const MAX_IN_LIST_VALUES: usize = 10_000;

pub fn lower_cypher_ast(document: &Document) -> GraphPlanResult<GraphLogicalPlan> {
    lower_cypher_ast_with_id_properties(document, false)
}

/// BYOG stores `id` as an ordinary property; managed graphs retain their legacy identity alias.
pub fn lower_cypher_ast_with_id_properties(document: &Document, id_properties: bool) -> GraphPlanResult<GraphLogicalPlan> {
    let query = match document.statements.as_slice() {
        [Statement::Query(query)] => query,
        // The message stays the one production logs have always carried; the
        // reason says which clause the Cypher 25 lowering stopped in, which
        // the message alone never could.
        // The lowering found text the grammar lets through but GQL's lexical
        // rules forbid, such as `0b12`: a syntax error, not a feature.
        [Statement::Syntax(fragment)]
            if fragment.rejection_status().is_some() || fragment.unsupported_reason() == Some(QueryFailureReason::ParseError) =>
        {
            return Err(GraphPlanError::Rejected {
                status: fragment.rejection_status().unwrap_or(GqlStatus::INVALID_SYNTAX),
                reason: fragment.unsupported_reason().unwrap_or(QueryFailureReason::ParseError),
                message: "invalid syntax or statement boundary".to_string(),
                reference: None,
            });
        }
        [Statement::Syntax(fragment)] => {
            return Err(GraphPlanError::UnsupportedCypher(
                fragment.unsupported_reason().unwrap_or(QueryFailureReason::UntypedStatement),
                "expected exactly one typed query statement".to_string(),
            ));
        }
        _ => {
            return Err(GraphPlanError::UnsupportedCypher(
                QueryFailureReason::UntypedStatement,
                "expected exactly one typed query statement".to_string(),
            ));
        }
    };
    lower_query(query).map(|plan| resolve_relationship_ids(plan, id_properties))
}

/// The name `n.id` and `{id: ...}` read.
const ID_PROPERTY: &str = "id";

/// Resolve syntactic `.id` and `{id: ...}` separately from structural identities.
/// Managed nodes retain the legacy identity alias; relationships and BYOG nodes
/// use stored properties. The pass also covers correlated subqueries and aliases.
fn resolve_relationship_ids(plan: GraphLogicalPlan, id_properties: bool) -> GraphLogicalPlan {
    relationship_ids_in_plan(plan, &RelationshipScope::default(), id_properties).0
}

/// Relationship kinds in the readable scope and its independent fixed record.
#[derive(Clone, Default)]
struct RelationshipScope {
    visible: BTreeSet<Symbol>,
    fixed: BTreeSet<Symbol>,
}

impl RelationshipScope {
    fn imported(visible: BTreeSet<Symbol>) -> Self {
        Self {
            fixed: visible.clone(),
            visible,
        }
    }
}

/// Rewrite `plan` given the relationship names its correlated input binds, and
/// return it with the relationship names its own rows carry.
fn relationship_ids_in_plan(
    plan: GraphLogicalPlan,
    outer: &RelationshipScope,
    id_properties: bool,
) -> (GraphLogicalPlan, RelationshipScope) {
    match plan {
        plan @ (GraphLogicalPlan::SingleRow | GraphLogicalPlan::NodeScan { .. }) => (plan, outer.clone()),
        GraphLogicalPlan::Unwind {
            input,
            expression,
            binding,
            requires_list,
            position,
        } => {
            let (input, relationships) = relationship_ids_in_plan(*input, outer, id_properties);
            let expression = relationship_ids_in_expression(expression, &relationships.visible, id_properties);
            (
                GraphLogicalPlan::Unwind {
                    input: Box::new(input),
                    expression,
                    binding,
                    requires_list,
                    position,
                },
                relationships,
            )
        }
        GraphLogicalPlan::Union { arms, all, conjunction } => {
            // A column is a relationship only when every arm returns one: one
            // that may hold a node keeps `.id` an identity, which each row's
            // value then answers.
            let mut relationships: Option<RelationshipScope> = None;
            let arms = arms
                .into_iter()
                .map(|arm| {
                    let (arm, carried) = relationship_ids_in_plan(arm, outer, id_properties);
                    relationships = Some(match relationships.take() {
                        None => carried,
                        Some(held) => RelationshipScope {
                            visible: held.visible.intersection(&carried.visible).cloned().collect(),
                            fixed: held.fixed.intersection(&carried.fixed).cloned().collect(),
                        },
                    });
                    arm
                })
                .collect();
            let relationships = relationships.unwrap_or_default();
            (GraphLogicalPlan::Union { arms, all, conjunction }, relationships)
        }
        GraphLogicalPlan::Exists { input, subquery, binding } => {
            let (input, relationships) = relationship_ids_in_plan(*input, outer, id_properties);
            let (subquery, _) = relationship_ids_in_plan(
                *subquery,
                &RelationshipScope::imported(relationships.visible.clone()),
                id_properties,
            );
            (
                GraphLogicalPlan::Exists {
                    input: Box::new(input),
                    subquery: Box::new(subquery),
                    binding,
                },
                relationships,
            )
        }
        GraphLogicalPlan::ScalarSubquery {
            input,
            subquery,
            column,
            binding,
        } => {
            let (input, mut relationships) = relationship_ids_in_plan(*input, outer, id_properties);
            let (subquery, carried) = relationship_ids_in_plan(
                *subquery,
                &RelationshipScope::imported(relationships.visible.clone()),
                id_properties,
            );
            // The value is the subquery's column, which may be an edge.
            if carried.visible.contains(&column) {
                relationships.visible.insert(binding.clone());
            }
            (
                GraphLogicalPlan::ScalarSubquery {
                    input: Box::new(input),
                    subquery: Box::new(subquery),
                    column,
                    binding,
                },
                relationships,
            )
        }
        GraphLogicalPlan::CallProcedure {
            input,
            subquery,
            imports,
            columns,
            outputs_to_record,
            optional,
        } => {
            let (input, mut relationships) = relationship_ids_in_plan(*input, outer, id_properties);
            // The call sees only what it imports: an outer relationship it
            // does not is a fresh name inside it.
            let imported = relationships
                .visible
                .iter()
                .filter(|name| imports.contains(*name))
                .cloned()
                .collect();
            let (subquery, carried) = relationship_ids_in_plan(*subquery, &RelationshipScope::imported(imported), id_properties);
            let returned: BTreeSet<_> = columns.iter().filter(|column| carried.visible.contains(*column)).cloned().collect();
            relationships.visible.extend(returned.iter().cloned());
            if outputs_to_record {
                relationships.fixed.extend(returned);
            }
            (
                GraphLogicalPlan::CallProcedure {
                    input: Box::new(input),
                    subquery: Box::new(subquery),
                    imports,
                    columns,
                    outputs_to_record,
                    optional,
                },
                relationships,
            )
        }
        GraphLogicalPlan::Next { input, next } => {
            let (input, _) = relationship_ids_in_plan(*input, outer, id_properties);
            let (next, relationships) = relationship_ids_in_plan(*next, outer, id_properties);
            (
                GraphLogicalPlan::Next {
                    input: Box::new(input),
                    next: Box::new(next),
                },
                relationships,
            )
        }
        GraphLogicalPlan::Finish { input } => {
            let (input, relationships) = relationship_ids_in_plan(*input, outer, id_properties);
            (GraphLogicalPlan::Finish { input: Box::new(input) }, relationships)
        }
        GraphLogicalPlan::BindPath {
            input,
            names,
            parts,
            node_groups,
        } => {
            let (input, relationships) = relationship_ids_in_plan(*input, outer, id_properties);
            (
                GraphLogicalPlan::BindPath {
                    input: Box::new(input),
                    names,
                    parts,
                    node_groups,
                },
                relationships,
            )
        }
        GraphLogicalPlan::SelectPaths {
            input,
            first,
            last,
            fixed_edges,
            lists,
            kind,
            count,
        } => {
            let (input, relationships) = relationship_ids_in_plan(*input, outer, id_properties);
            (
                GraphLogicalPlan::SelectPaths {
                    input: Box::new(input),
                    first,
                    last,
                    fixed_edges,
                    lists,
                    kind,
                    count,
                },
                relationships,
            )
        }
        GraphLogicalPlan::Expand {
            input,
            from,
            relationship,
            relationship_types,
            direction,
            to,
            target_labels,
        } => {
            let (input, mut relationships) = relationship_ids_in_plan(*input, outer, id_properties);
            relationships.visible.extend(relationship.clone());
            (
                GraphLogicalPlan::Expand {
                    input: Box::new(input),
                    from,
                    relationship,
                    relationship_types,
                    direction,
                    to,
                    target_labels,
                },
                relationships,
            )
        }
        // `rs` of `[rs:R*1..3]` is a list of relationships, not one; `rs.id`
        // reads nothing either way.
        GraphLogicalPlan::VariableExpand {
            input,
            from,
            relationship,
            relationship_types,
            direction,
            to,
            target_labels,
            min_hops,
            max_hops,
            trail,
            nodes,
            iteration,
        } => {
            let (input, relationships) = relationship_ids_in_plan(*input, outer, id_properties);
            (
                GraphLogicalPlan::VariableExpand {
                    input: Box::new(input),
                    from,
                    relationship,
                    relationship_types,
                    direction,
                    to,
                    target_labels,
                    min_hops,
                    max_hops,
                    trail,
                    nodes,
                    iteration: iteration.map(|mut iteration| {
                        let mut singleton_relationships = relationships.clone();
                        singleton_relationships.visible.insert(iteration.relationship.clone());
                        iteration.plan = Box::new(relationship_ids_in_plan(*iteration.plan, &singleton_relationships, id_properties).0);
                        iteration
                    }),
                },
                relationships,
            )
        }
        GraphLogicalPlan::PathUnion { inputs, words } => {
            let mut relationships = RelationshipScope::default();
            let inputs = inputs
                .into_iter()
                .map(|input| {
                    let (input, bound) = relationship_ids_in_plan(input, outer, id_properties);
                    relationships.visible.extend(bound.visible);
                    relationships.fixed.extend(bound.fixed);
                    input
                })
                .collect();
            (GraphLogicalPlan::PathUnion { inputs, words }, relationships)
        }
        // The right side runs once per left row and reads its names.
        GraphLogicalPlan::NaturalJoin { left, right, optional } => {
            let (left, mut relationships) = relationship_ids_in_plan(*left, outer, id_properties);
            let (right, carried) = relationship_ids_in_plan(*right, &relationships, id_properties);
            relationships.visible.extend(carried.visible);
            relationships.fixed.extend(carried.fixed);
            (
                GraphLogicalPlan::NaturalJoin {
                    left: Box::new(left),
                    right: Box::new(right),
                    optional,
                },
                relationships,
            )
        }
        GraphLogicalPlan::Filter {
            operation,
            input,
            predicate,
        } => {
            let (input, relationships) = relationship_ids_in_plan(*input, outer, id_properties);
            let predicate = relationship_ids_in_expression(predicate, &relationships.visible, id_properties);
            (
                GraphLogicalPlan::Filter {
                    operation,
                    input: Box::new(input),
                    predicate,
                },
                relationships,
            )
        }
        GraphLogicalPlan::Sort { input, items } => {
            let (input, relationships) = relationship_ids_in_plan(*input, outer, id_properties);
            let items = relationship_ids_in_sorts(items, &relationships.visible, id_properties);
            (
                GraphLogicalPlan::Sort {
                    input: Box::new(input),
                    items,
                },
                relationships,
            )
        }
        GraphLogicalPlan::Skip { input, count } => {
            let (input, relationships) = relationship_ids_in_plan(*input, outer, id_properties);
            let count = relationship_ids_in_expression(count, &relationships.visible, id_properties);
            (
                GraphLogicalPlan::Skip {
                    input: Box::new(input),
                    count,
                },
                relationships,
            )
        }
        GraphLogicalPlan::Limit { input, count } => {
            let (input, relationships) = relationship_ids_in_plan(*input, outer, id_properties);
            let count = relationship_ids_in_expression(count, &relationships.visible, id_properties);
            (
                GraphLogicalPlan::Limit {
                    input: Box::new(input),
                    count,
                },
                relationships,
            )
        }
        // A projection replaces the scope: what it carries on is what a bare
        // relationship binding item names. Its own ORDER BY reads both, the
        // projected names first.
        GraphLogicalPlan::Project {
            context,
            operation,
            input,
            items,
            distinct,
            post_sort,
            post_skip,
            post_limit,
        } => {
            let (input, relationships) = relationship_ids_in_plan(*input, outer, id_properties);
            let projected = items
                .iter()
                .filter_map(|item| match (&item.alias, &item.expression) {
                    (Some(name), _) | (None, LogicalExpression::Binding(name)) => Some(name.clone()),
                    _ => None,
                })
                .collect::<BTreeSet<_>>();
            let carried = items
                .iter()
                .filter_map(|item| match &item.expression {
                    LogicalExpression::Binding(binding) if relationships.visible.contains(binding) => {
                        Some(item.alias.clone().unwrap_or_else(|| binding.clone()))
                    }
                    _ => None,
                })
                .chain(
                    relationships
                        .fixed
                        .iter()
                        .filter(|name| context.fixed.contains(name) && !projected.contains(*name))
                        .cloned(),
                )
                .collect::<BTreeSet<_>>();
            let items = items
                .into_iter()
                .map(|item| LogicalProjection {
                    expression: relationship_ids_in_expression(item.expression, &relationships.visible, id_properties),
                    alias: item.alias,
                })
                .collect();
            // A key reads the projected names over the input's (as
            // `sort_overlay` lays them), and its aggregates the input rows:
            // `RETURN n AS r ... ORDER BY r.id` reads the node, not the edge.
            let sortable = relationships
                .visible
                .difference(&projected)
                .filter(|name| !context.fixed.contains(name))
                .cloned()
                .chain(carried.iter().cloned())
                .collect::<BTreeSet<_>>();
            let post_sort = post_sort
                .into_iter()
                .map(|item| LogicalSort {
                    expression: relationship_ids_in_key(item.expression, &sortable, &relationships.visible, id_properties),
                    direction: item.direction,
                    nulls: item.nulls,
                })
                .collect();
            let mut fixed = relationships.fixed;
            fixed.extend(context.defined.iter().filter(|name| carried.contains(*name)).cloned());
            (
                GraphLogicalPlan::Project {
                    context,
                    operation,
                    input: Box::new(input),
                    items,
                    distinct,
                    post_sort,
                    post_skip: post_skip.map(|count| relationship_ids_in_expression(count, &sortable, id_properties)),
                    post_limit: post_limit.map(|count| relationship_ids_in_expression(count, &sortable, id_properties)),
                },
                RelationshipScope { visible: carried, fixed },
            )
        }
    }
}

fn relationship_ids_in_sorts(items: Vec<LogicalSort>, relationships: &BTreeSet<Symbol>, id_properties: bool) -> Vec<LogicalSort> {
    items
        .into_iter()
        .map(|item| LogicalSort {
            expression: relationship_ids_in_expression(item.expression, relationships, id_properties),
            direction: item.direction,
            nulls: item.nulls,
        })
        .collect()
}

fn relationship_ids_in_expression(
    expression: LogicalExpression,
    relationships: &BTreeSet<Symbol>,
    id_properties: bool,
) -> LogicalExpression {
    relationship_ids_in_key(expression, relationships, relationships, id_properties)
}

/// [`relationship_ids_in_expression`] for an expression whose aggregates
/// read other rows than the rest of it: `relationships` names the edges
/// outside its aggregates, `aggregated` inside them.
fn relationship_ids_in_key(
    expression: LogicalExpression,
    relationships: &BTreeSet<Symbol>,
    aggregated: &BTreeSet<Symbol>,
    id_properties: bool,
) -> LogicalExpression {
    let rewrite = |expression: LogicalExpression| relationship_ids_in_key(expression, relationships, aggregated, id_properties);
    let rewrite_box =
        |expression: Box<LogicalExpression>| Box::new(relationship_ids_in_key(*expression, relationships, aggregated, id_properties));
    match expression {
        LogicalExpression::Function {
            function: ScalarFunction::IdProperty,
            arguments,
        } if matches!(arguments.as_slice(), [LogicalExpression::Binding(_)]) => {
            let LogicalExpression::Binding(binding) = arguments.into_iter().next().unwrap() else {
                unreachable!()
            };
            if id_properties || relationships.contains(&binding) {
                LogicalExpression::Property {
                    binding,
                    property: Symbol::from(ID_PROPERTY),
                }
            } else {
                LogicalExpression::Identity(binding)
            }
        }
        LogicalExpression::Field { expression, field } if id_properties && field.as_str() == ID_PROPERTY => LogicalExpression::Function {
            function: ScalarFunction::IdProperty,
            arguments: vec![rewrite(*expression)],
        },
        LogicalExpression::ElementTest {
            binding,
            test: ElementTestKind::PropertyExists(property),
        } if id_properties && property.as_str() == ID_PROPERTY => LogicalExpression::ElementTest {
            binding,
            test: ElementTestKind::StoredPropertyExists(property),
        },
        expression @ (LogicalExpression::Binding(_)
        | LogicalExpression::Identity(_)
        | LogicalExpression::ElementTest { .. }
        | LogicalExpression::PathNodesDistinct { .. }
        | LogicalExpression::Property { .. }
        | LogicalExpression::Parameter(_)
        | LogicalExpression::Literal(_)) => expression,
        LogicalExpression::Aggregate {
            function,
            distinct,
            expression,
            fraction,
            group,
        } => LogicalExpression::Aggregate {
            function,
            distinct,
            expression: expression.map(|expression| Box::new(relationship_ids_in_key(*expression, aggregated, aggregated, id_properties))),
            fraction,
            group,
        },
        LogicalExpression::Function { function, arguments } => LogicalExpression::Function {
            function,
            arguments: arguments.into_iter().map(rewrite).collect(),
        },
        LogicalExpression::List(items) => LogicalExpression::List(items.into_iter().map(rewrite).collect()),
        LogicalExpression::Case {
            operand,
            branches,
            default,
        } => LogicalExpression::Case {
            operand: operand.map(rewrite_box),
            branches: branches.into_iter().map(|(when, then)| (rewrite(when), rewrite(then))).collect(),
            default: default.map(rewrite_box),
        },
        LogicalExpression::Unary { operator, expression } => LogicalExpression::Unary {
            operator,
            expression: rewrite_box(expression),
        },
        LogicalExpression::TypeTest {
            expression,
            value_type,
            negated,
        } => LogicalExpression::TypeTest {
            expression: rewrite_box(expression),
            value_type,
            negated,
        },
        LogicalExpression::Cast {
            expression,
            value_type,
            request,
        } => LogicalExpression::Cast {
            expression: rewrite_box(expression),
            value_type,
            request,
        },
        LogicalExpression::Record(fields) => {
            LogicalExpression::Record(fields.into_iter().map(|(name, value)| (name, rewrite(value))).collect())
        }
        LogicalExpression::Let { bindings, body } => LogicalExpression::Let {
            bindings: bindings.into_iter().map(|(name, value)| (name, rewrite(value))).collect(),
            body: rewrite_box(body),
        },
        LogicalExpression::PathConstructor(elements) => LogicalExpression::PathConstructor(elements.into_iter().map(rewrite).collect()),
        LogicalExpression::Field { expression, field } => LogicalExpression::Field {
            expression: rewrite_box(expression),
            field,
        },
        LogicalExpression::Binary { left, operator, right } => LogicalExpression::Binary {
            left: rewrite_box(left),
            operator,
            right: rewrite_box(right),
        },
        LogicalExpression::In { expression, list } => LogicalExpression::In {
            expression: rewrite_box(expression),
            list: match list {
                LogicalInList::Values(values) => LogicalInList::Values(values.into_iter().map(rewrite).collect()),
                list @ LogicalInList::Parameter(_) => list,
            },
        },
    }
}

fn lower_query(query: &Query) -> GraphPlanResult<GraphLogicalPlan> {
    lower_query_in(query, &mut OuterScope::default())
}

/// What an enclosing query makes readable to a subquery: its names, which of
/// them patterns have bound and which hold values, and the names the
/// lowering synthesized, so the subquery numbers its own after them. A
/// top-level query has none of them.
#[derive(Default)]
struct OuterScope {
    operation: Cell<operation::Operation>,
    visible: BTreeSet<Symbol>,
    bound: BTreeSet<Symbol>,
    values: BTreeSet<Symbol>,
    /// Readable fixed variables, not columns of the working table (§9.2).
    record: BTreeSet<Symbol>,
    table_columns: BTreeSet<Symbol>,
    synthesized: BTreeSet<Symbol>,
    /// An `EXISTS` subquery's clauses need not end in RETURN; without one it
    /// lowers to its binding plan.
    open: bool,
    /// The lowering's names for the unnamed columns of a RETURN before NEXT
    /// (`return_before_next`): read as the projection's own, never a column
    /// of the query's, so never part of `*`. A user's column of the same
    /// spelling is not one of them.
    next_columns: BTreeSet<Symbol>,
    /// The group variables an OPTIONAL block's FILTER may aggregate: those
    /// of the patterns before it.
    groups: BTreeSet<Symbol>,
}

#[derive(Default)]
struct FinalScope {
    visible: BTreeSet<Symbol>,
    groups: BTreeSet<Symbol>,
    returned_values: BTreeSet<Symbol>,
}

struct LoweredQuery {
    plan: GraphLogicalPlan,
    scope: FinalScope,
}

impl OuterScope {
    /// Correlation copies the caller's bindings into a fixed record over a
    /// unit table. Generated names belong to the child until lowering succeeds.
    fn correlated_query(context: BindingScope<'_>, synthesized: &BTreeSet<Symbol>) -> Self {
        let BindingScope {
            visible,
            bound,
            values,
            groups,
            ..
        } = context;
        Self {
            operation: Cell::new(operation::UNRECOGNIZED),
            visible: visible.clone(),
            bound: bound.clone(),
            values: values.clone(),
            record: visible.clone(),
            table_columns: BTreeSet::new(),
            synthesized: synthesized.clone(),
            open: false,
            next_columns: BTreeSet::new(),
            groups: groups.clone(),
        }
    }

    fn export_generated(self, into: &mut BTreeSet<Symbol>) {
        into.extend(self.synthesized);
    }
}

fn lower_query_in(query: &Query, outer: &mut OuterScope) -> GraphPlanResult<GraphLogicalPlan> {
    lower_query_scoped(query, outer).map(|lowered| lowered.plan)
}

fn lower_query_scoped(query: &Query, outer: &mut OuterScope) -> GraphPlanResult<LoweredQuery> {
    let definitions = with_next_definitions(query, outer);
    let query = definitions.as_ref().unwrap_or(query);
    let reserved = if query.clauses.iter().any(|clause| matches!(clause, Clause::Next)) {
        reserved_query_names(query)
    } else {
        BTreeSet::new()
    };
    lower_query_block(query, outer, &reserved)
}

fn lower_query_block(query: &Query, outer: &mut OuterScope, reserved: &BTreeSet<Symbol>) -> GraphPlanResult<LoweredQuery> {
    outer.operation.set(operation::UNRECOGNIZED);
    check_sort_aggregates(query).map_err(|error| error.in_operation(operation::ORDER_BY_AND_PAGE))?;
    if let Some(index) = finish_before_next(query, outer)? {
        let before = Query {
            clauses: query.clauses[..index].to_vec(),
            unions: Vec::new(),
        };
        let after = Query {
            clauses: query.clauses[index + 1..].to_vec(),
            unions: Vec::new(),
        };
        let input = lower_query_block(&before, outer, reserved)?;
        let next = lower_query_block(&after, outer, reserved)?;
        return Ok(LoweredQuery {
            plan: GraphLogicalPlan::Next {
                input: Box::new(input.plan),
                next: Box::new(next.plan),
            },
            scope: next.scope,
        });
    }
    check_aggregate_placement(query)?;
    lower_validated_query(query, outer, reserved, false).map_err(|error| error.in_operation(outer.operation.get()))
}

fn lower_validated_query(
    query: &Query,
    outer: &mut OuterScope,
    reserved: &BTreeSet<Symbol>,
    mut sort_keys_validated: bool,
) -> GraphPlanResult<LoweredQuery> {
    if !sort_keys_validated {
        if let Some(Clause::Return(returned)) = query.clauses.last() {
            if !returned.group_by.is_empty() {
                check_restricted_sort_keys(returned, returned.items.items())
                    .map_err(|error| error.in_operation(operation::ORDER_BY_AND_PAGE))?;
                sort_keys_validated = true;
            }
        }
    }
    // Hidden keys are normalized into projected keys; explicit grouping
    // remains distinct from implicit grouping at the projection boundary.
    if let Some(rewritten) = normalize_explicit_grouping(query, reserved)? {
        return lower_validated_query(&rewritten, outer, reserved, sort_keys_validated);
    }
    let finished = matches!(query.clauses.last(), Some(Clause::Finish));
    let (returned, matched_clauses) = match query.clauses.split_last() {
        Some((Clause::Return(returned), matched_clauses)) => (Some(returned), matched_clauses),
        Some((Clause::Finish, matched_clauses)) => (None, matched_clauses),
        Some(_) if outer.open && query.unions.is_empty() => (None, query.clauses.as_slice()),
        _ => {
            return Err(GraphPlanError::UnsupportedCypher(
                QueryFailureReason::Return,
                "expected clauses followed by RETURN".to_string(),
            ))
        }
    };
    // `None` until a clause produces rows. A clause that needs input before
    // any has been produced -- WITH, or an OPTIONAL MATCH opening the query --
    // reads `SingleRow`, the one row that binds nothing, as does a query that
    // is only a RETURN.
    let mut plan = None;
    let mut declared: BTreeSet<Symbol> = outer.visible.clone();
    // The names still readable here. Identical to `declared` until a WITH
    // narrows, after which `declared` stays cumulative for the synthesized
    // collision check while this follows the projection boundary.
    let mut visible: BTreeSet<Symbol> = outer.visible.clone();
    // Every expression is checked against the names visible at its boundary.
    // What earlier patterns have produced. Not `declared`, which carries
    // every name the clause mentions before any of it is lowered, so a
    // pattern would find its own bindings already bound and anchor on
    // nothing.
    //
    // An OPTIONAL clause's names belong here too. A miss binds them to null
    // rather than leaving them absent, and a pattern anchored on a null name
    // matches nothing and eliminates the row -- so the anchor is answered on
    // a hit and short-circuits on a miss. Neither case scans.
    let mut bound: BTreeSet<Symbol> = outer.bound.clone();
    // Names `lower_path` synthesized for a relationship carrying inline
    // properties but no variable of its own. Numbered across the query, not
    // per pattern: two patterns each numbering their own hops from zero would
    // both synthesize `__hydradb_inline_relationship_0`, and a row cannot hold
    // one name for two unrelated edges.
    //
    // Kept rather than counted, so the check at the end can compare against
    // the names this query actually produced.
    let mut synthesized: BTreeSet<Symbol> = outer.synthesized.clone();
    // Names the most recent WITH projected as values rather than bindings. A
    // pattern may read one in a predicate, and may not bind one as a node or a
    // relationship: a scalar is not a graph element, and anchoring a pattern on
    // it silently answers a cross product.
    let mut values: BTreeSet<Symbol> = outer.values.clone();
    let mut record = outer.record.clone();
    // Fixed-record kinds outlive table projections that shadow their names.
    let mut record_values: BTreeSet<Symbol> = values.intersection(&record).cloned().collect();
    let mut table_columns = outer.table_columns.clone();
    // The group variables the patterns so far declared, which a WHERE or a
    // FILTER may aggregate horizontally (GE09). A projection ends them.
    let mut groups: BTreeSet<Symbol> = outer.groups.clone();
    let mut clauses: VecDeque<_> = matched_clauses.iter().map(Cow::Borrowed).collect();
    while let Some(clause) = clauses.pop_front() {
        outer.operation.set(clause_operation(&clause));
        // GQL's value keywords are the variables of their names where those
        // are in scope here, and their values elsewhere.
        let resolved = resolve_clause_keywords(&clause, &visible, &record);
        let clause = &resolved;
        // GQL's LET and FILTER are the WITH * they mean, lowered by the same
        // path so scope, value tracking and checks are shared.
        let amended_table = match clause {
            Clause::Let(binding) => {
                let mut columns = table_columns.clone();
                if !binding.definition {
                    columns.extend(binding.bindings.iter().map(|(name, _)| Symbol::from(name.as_str())));
                }
                Some(columns)
            }
            _ => None,
        };
        let definitions = match clause {
            Clause::Let(binding) if binding.definition => binding.bindings.iter().map(|(name, _)| Symbol::from(name.as_str())).collect(),
            _ => BTreeSet::new(),
        };
        let desugared;
        let clause = match clause {
            Clause::Let(binding) => {
                desugared = Clause::With(let_as_with(binding)?);
                &desugared
            }
            Clause::Filter(filter) => {
                validate_filter(filter)?;
                let mut input = plan.take().unwrap_or(GraphLogicalPlan::SingleRow);
                let predicate = lower_predicate_into(
                    &filter.predicate,
                    &mut input,
                    PredicateScope {
                        record: &record,
                        table_columns: &table_columns,
                        visible: &visible,
                        bound: &bound,
                        values: &values,
                        groups: &groups,
                        clause: "FILTER",
                    },
                    &mut synthesized,
                )?;
                plan = Some(GraphLogicalPlan::Filter {
                    operation: operation::FILTER,
                    input: Box::new(input),
                    predicate,
                });
                continue;
            }
            clause => clause,
        };
        let matched = match clause {
            Clause::Return(returned) if matches!(clauses.front().map(|clause| clause.as_ref()), Some(Clause::Next)) => {
                clauses.pop_front();
                // Classify named columns only after resolving this statement's
                // expressions, then lower its projections through the same scope path.
                let projections = Query {
                    clauses: return_before_next(returned.clone(), reserved, &mut outer.next_columns),
                    unions: Vec::new(),
                };
                let projections = normalize_explicit_grouping(&projections, reserved)?.unwrap_or(projections);
                for clause in projections.clauses.into_iter().rev() {
                    clauses.push_front(Cow::Owned(clause));
                }
                continue;
            }
            Clause::Match(matched) => matched,
            Clause::OrderByAndPage(page) => {
                let mut input = plan.take().unwrap_or(GraphLogicalPlan::SingleRow);
                let mut items = Vec::with_capacity(page.order_by.len());
                for item in &page.order_by {
                    let expression = hoist_exists_into(
                        &item.expression,
                        &mut input,
                        BindingScope {
                            visible: &visible,
                            bound: &bound,
                            values: &values,
                            groups: &groups,
                            record: &record,
                            table_columns: &table_columns,
                        },
                        &mut synthesized,
                    )?;
                    let scope = with_marks(&visible, &synthesized);
                    check_scope(&expression, &scope, &values, "ORDER BY")?;
                    let expression = lower_expression(&expression)?;
                    validate_declared_bindings(&expression, &scope)?;
                    let (direction, nulls) = sort_order(item);
                    items.push(LogicalSort {
                        expression,
                        direction,
                        nulls,
                    });
                }
                if !items.is_empty() {
                    input = GraphLogicalPlan::Sort {
                        input: Box::new(input),
                        items,
                    };
                }
                if let Some(skip) = &page.skip {
                    input = GraphLogicalPlan::Skip {
                        input: Box::new(input),
                        count: lower_window_expression(skip, "OFFSET")?,
                    };
                }
                if let Some(limit) = &page.limit {
                    input = GraphLogicalPlan::Limit {
                        input: Box::new(input),
                        count: lower_window_expression(limit, "LIMIT")?,
                    };
                }
                plan = Some(input);
                continue;
            }
            Clause::With(with) => {
                // Keep WITH's projection boundary distinct from standalone sorting.
                let input = plan.take().unwrap_or(GraphLogicalPlan::SingleRow);
                // `*` expands without NEXT's own names, which the rewrite's
                // other clauses still read until its last projection drops
                // them.
                let starred;
                let with = match &with.items {
                    ProjectionItems::All(_) => {
                        starred = WithClause {
                            items: ProjectionItems::Items(star_expanded(
                                &with.items,
                                &projection_columns(&visible, &table_columns, outer, with.table_only),
                                if with.table_only { "RETURN" } else { "WITH" },
                            )?),
                            ..with.clone()
                        };
                        &starred
                    }
                    ProjectionItems::Items(_) => with,
                };
                let (mut projected, projected_scope, projected_values) = lower_with(
                    with,
                    input,
                    BindingScope {
                        record: &record,
                        table_columns: &table_columns,
                        visible: &visible,
                        bound: &bound,
                        values: &values,
                        groups: &groups,
                    },
                    &record_values,
                    &mut synthesized,
                    outer.operation.get(),
                )?;
                if let GraphLogicalPlan::Project { context, .. } = &mut projected {
                    context.defined = definitions.iter().cloned().collect();
                }
                plan = Some(projected);
                groups.clear();
                // Cumulative: a name the projection dropped can still collide
                // with a synthesized one, and that check runs over the query.
                declared.extend(projected_scope.iter().cloned());
                // A later pattern may anchor on what the projection produced,
                // and may read nothing else.
                bound = projected_scope.clone();
                visible = projected_scope;
                record_values.extend(definitions.intersection(&projected_values).cloned());
                values = projected_values;
                table_columns = amended_table.unwrap_or_else(|| {
                    with.items
                        .items()
                        .iter()
                        .filter_map(|item| item.output_binding())
                        .map(|name| Symbol::from(name.as_str()))
                        .collect()
                });
                record.extend(definitions);
                continue;
            }
            Clause::Unwind(unwind) => {
                let binding = Symbol::from(unwind.variable.as_str());
                // Checked against what is readable here, as any expression is.
                check_scope(&unwind.expression, &visible, &values, "UNWIND")?;
                let expression = lower_expression(&unwind.expression)?;
                validate_declared_bindings(&expression, &visible)?;
                if visible.contains(&binding) {
                    return Err(GraphPlanError::syntax(
                        QueryFailureReason::Unwind,
                        format!("UNWIND cannot rebind {binding}, which is already in scope"),
                    ));
                }
                // A FOR source is a list (§14.8 Syntax Rule 10); a literal
                // that is not one breaks it before anything runs.
                if unwind.requires_list && matches!(&unwind.expression, Expression::Literal(literal) if !matches!(literal, Literal::Null)) {
                    return Err(GraphPlanError::syntax(
                        QueryFailureReason::Unwind,
                        "FOR iterates over a list, not a single value".to_string(),
                    ));
                }
                // §14.8 Syntax Rules 10 c) i) 2) and 11: the position's name
                // differs from the element's and is new.
                let position = match &unwind.position {
                    Some(position) => {
                        let name = Symbol::from(position.variable.as_str());
                        if name == binding || visible.contains(&name) {
                            return Err(GraphPlanError::syntax(
                                QueryFailureReason::Unwind,
                                format!("FOR cannot bind its position to {name}, which is already a name"),
                            ));
                        }
                        Some((name, if position.offset { 0 } else { 1 }))
                    }
                    None => None,
                };
                plan = Some(GraphLogicalPlan::Unwind {
                    input: Box::new(plan.take().unwrap_or(GraphLogicalPlan::SingleRow)),
                    expression,
                    binding: binding.clone(),
                    requires_list: unwind.requires_list,
                    position: position.clone(),
                });
                if let Some((name, _)) = position {
                    declared.insert(name.clone());
                    visible.insert(name.clone());
                    table_columns.insert(name.clone());
                    values.insert(name);
                }
                // The element is a value: readable by what follows, and never
                // a node or relationship a later pattern could bind.
                declared.insert(binding.clone());
                visible.insert(binding.clone());
                table_columns.insert(binding.clone());
                values.insert(binding);
                continue;
            }
            // §15.1, §15.2 GP01 to GP03: an inline procedure call runs its
            // query for each row, over the names it imports, and the row
            // continues beside each row the query returns.
            Clause::Call(call) => {
                let imports: Vec<Symbol> = match &call.scope {
                    // Syntax Rule 3 (GP02): every incoming name.
                    None => visible.iter().cloned().collect(),
                    // Syntax Rule 6 a) ii) (GP03).
                    Some(names) => {
                        let mut imports: Vec<Symbol> = Vec::new();
                        for name in names {
                            let name = Symbol::from(name.as_str());
                            if imports.contains(&name) {
                                return Err(GraphPlanError::syntax(
                                    QueryFailureReason::Procedure,
                                    format!("the variable scope clause names {name} twice"),
                                ));
                            }
                            if !visible.contains(&name) {
                                return Err(GraphPlanError::invalid_reference(
                                    QueryFailureReason::Procedure,
                                    name.as_str(),
                                    format!("CALL imports {name}, which is not in scope"),
                                ));
                            }
                            imports.push(name);
                        }
                        imports
                    }
                };
                let columns = call_columns(&call.query)?;
                // Syntax Rule 9: what the call returns is new to the table.
                if let Some(column) = columns.iter().find(|column| {
                    if call.outputs_to_record {
                        table_columns.contains(*column) || (visible.contains(*column) && !record.contains(*column))
                    } else {
                        visible.contains(*column) || record.contains(*column)
                    }
                }) {
                    return Err(GraphPlanError::syntax(
                        QueryFailureReason::Procedure,
                        format!("CALL returns {column}, which is already in scope"),
                    ));
                }
                let imported: BTreeSet<Symbol> = imports.iter().cloned().collect();
                let mut scope = OuterScope {
                    operation: Cell::new(operation::UNRECOGNIZED),
                    visible: imported.clone(),
                    bound: bound.intersection(&imported).cloned().collect(),
                    values: values.intersection(&imported).cloned().collect(),
                    record: imported.clone(),
                    table_columns: BTreeSet::new(),
                    synthesized: synthesized.clone(),
                    open: false,
                    next_columns: BTreeSet::new(),
                    // The groups it imports stay groups inside it.
                    groups: groups.intersection(&imported).cloned().collect(),
                };
                let subquery = lower_query_scoped(&call.query, &mut scope)?;
                synthesized.extend(scope.synthesized);
                plan = Some(GraphLogicalPlan::CallProcedure {
                    input: Box::new(plan.take().unwrap_or(GraphLogicalPlan::SingleRow)),
                    subquery: Box::new(subquery.plan),
                    imports,
                    columns: columns.clone(),
                    outputs_to_record: call.outputs_to_record,
                    optional: call.optional,
                });
                for column in columns {
                    if call.outputs_to_record {
                        record.insert(column.clone());
                        if subquery.scope.returned_values.contains(&column) {
                            record_values.insert(column.clone());
                        }
                    } else {
                        table_columns.insert(column.clone());
                    }
                    declared.insert(column.clone());
                    visible.insert(column.clone());
                    // As the call's own RETURN classified it, from its scope.
                    if subquery.scope.returned_values.contains(&column) {
                        values.insert(column);
                    } else {
                        bound.insert(column);
                    }
                }
                continue;
            }
            // §14.4 Syntax Rule 4 f) ii), GQ21: `OPTIONAL { block }` is the
            // block run correlated with each row, as an OPTIONAL MATCH's
            // pattern is, and optional as one: a row the block misses keeps
            // every name it would bind, null.
            Clause::OptionalBlock(statements) => {
                let names: BTreeSet<Symbol> = flattened_clauses(statements)
                    .into_iter()
                    .filter_map(|clause| match clause {
                        Clause::Match(matched) => Some(clause_bindings(matched)),
                        _ => None,
                    })
                    .flatten()
                    .collect();
                if let Some(collision) = names.intersection(&values).next() {
                    return Err(GraphPlanError::syntax(
                        QueryFailureReason::Pattern,
                        format!("{collision} holds a value projected by WITH and cannot also bind a node or relationship"),
                    ));
                }
                let mut scope = OuterScope {
                    operation: Cell::new(operation::UNRECOGNIZED),
                    visible: visible.clone(),
                    bound: bound.clone(),
                    values: values.clone(),
                    record: record.clone(),
                    table_columns: table_columns.clone(),
                    synthesized: synthesized.clone(),
                    open: true,
                    next_columns: BTreeSet::new(),
                    groups: groups.clone(),
                };
                let block = Query {
                    clauses: statements.clone(),
                    unions: Vec::new(),
                };
                let right = lower_query_scoped(&block, &mut scope)?;
                synthesized.extend(scope.synthesized);
                // It hands on only what is left in its scope at its end: a
                // YIELD may have dropped some of what its MATCHes declare.
                let exported = right.scope.visible;
                let names: BTreeSet<Symbol> = names.into_iter().filter(|name| exported.contains(name)).collect();
                declared.extend(names.iter().cloned());
                table_columns.extend(names.difference(&visible).cloned());
                visible.extend(names.iter().cloned());
                bound.extend(names);
                // Its path and group variables are values, null on a miss.
                let path_names: Vec<Symbol> = flattened_clauses(statements)
                    .into_iter()
                    .filter_map(|clause| match clause {
                        Clause::Match(matched) => Some(clause_path_names(matched)),
                        _ => None,
                    })
                    .flatten()
                    .filter(|name| exported.contains(name))
                    .collect();
                declared.extend(path_names.iter().cloned());
                table_columns.extend(path_names.iter().filter(|name| !visible.contains(*name)).cloned());
                visible.extend(path_names.iter().cloned());
                values.extend(path_names);
                // The groups still groups at its end -- a YIELD or WITH inside
                // ends them as it would outside -- stay groups after it.
                groups.extend(right.scope.groups.into_iter().filter(|name| exported.contains(name)));
                plan = Some(GraphLogicalPlan::NaturalJoin {
                    left: Box::new(plan.take().unwrap_or(GraphLogicalPlan::SingleRow)),
                    right: Box::new(right.plan),
                    optional: true,
                });
                continue;
            }
            _ => {
                return Err(GraphPlanError::UnsupportedCypher(
                    QueryFailureReason::Other,
                    "only MATCH, WITH and UNWIND clauses may precede RETURN".to_string(),
                ));
            }
        };
        // DIFFERENT EDGES is what every MATCH does already (ID086), so
        // spelling it out changes nothing; REPEATABLE ELEMENTS is not lowered.
        // REPEATABLE ELEMENTS lifts DIFFERENT EDGES (ID086, §16.4 General Rule
        // 8): two hops of the clause may bind one edge.
        let kept;
        let matched = if matched.keep.is_some() || matched.patterns.iter().any(|path| path.predicate.is_some()) {
            kept = with_path_predicates(with_keep(matched)?);
            &kept
        } else {
            matched
        };
        let incoming = visible.clone();
        // §16.7 Syntax Rule 16: a path pattern matches at least one node, which
        // a path of nothing but questioned primaries (G037) need not.
        if matched.patterns.iter().any(|path| {
            path.alternatives.as_ref().is_some_and(|alternatives| {
                alternatives
                    .operands
                    .iter()
                    .any(|operand| operand.path.elements.is_empty() && operand.path.alternatives.is_none())
            })
        }) {
            return Err(GraphPlanError::syntax(
                QueryFailureReason::Pattern,
                "a path pattern must match at least one node",
            ));
        }
        let repeatable = matches!(matched.mode, Some(hydradb_cypher_ast::MatchMode::RepeatableElements));
        super::factor::validate_match_factors(matched)?;
        if matched.patterns.iter().any(|path| match &path.alternatives {
            Some(alternatives) => alternatives.operands.iter().any(|operand| operand.path.elements.is_empty()),
            None => path.elements.is_empty(),
        }) {
            return Err(GraphPlanError::UnsupportedCypher(
                QueryFailureReason::Pattern,
                "path factors outside the executable subset retain their syntax but cannot yet execute".to_string(),
            ));
        }
        // §16.4 Syntax Rule 12: under DIFFERENT EDGES a selective path pattern
        // is the only one of its graph pattern.
        if !repeatable && matched.patterns.len() > 1 && matched.patterns.iter().any(|path| path.search.is_some()) {
            return Err(GraphPlanError::syntax(
                QueryFailureReason::Pattern,
                "under DIFFERENT EDGES a selective path pattern stands alone in its MATCH".to_string(),
            ));
        }
        if matched.patterns.is_empty() {
            return Err(GraphPlanError::UnsupportedCypher(
                QueryFailureReason::Pattern,
                "MATCH modes and empty patterns are not lowered yet".to_string(),
            ));
        }
        // A value the last WITH computed cannot be bound by a pattern. Checked
        // before the clause is lowered, because by the time `lower_path`
        // anchors on the name the mistake looks like an ordinary join.
        if let Some(collision) = clause_bindings(matched).intersection(&values).next() {
            return Err(GraphPlanError::syntax(
                QueryFailureReason::Pattern,
                format!("{collision} holds a value projected by WITH and cannot also bind a node or relationship"),
            ));
        }
        // A name is a node variable or an edge variable, never both, across
        // every pattern of the clause; checked once here, not per path, where
        // `(x), ()-[x]->()` passed unseen.
        let (nodes, edges) = clause_element_kinds(matched);
        if let Some(name) = nodes.intersection(&edges).next() {
            return Err(GraphPlanError::syntax(
                QueryFailureReason::Pattern,
                format!("{name} cannot be both a node and a relationship"),
            ));
        }
        // What the clause can see from outside it, for §16.6 Syntax Rule 7.
        let exterior = visible.clone();
        // §16.4 Syntax Rules 4 and 15, §16.7 Syntax Rule 20: path and subpath
        // variables are new names, none of them an element's, and no two
        // paths share one. Each holds a path value.
        let path_names = clause_path_names(matched);
        let elements = clause_bindings(matched);
        let mut seen_paths = BTreeSet::new();
        for name in &path_names {
            if exterior.contains(name) || elements.contains(name) || !seen_paths.insert(name.clone()) {
                return Err(GraphPlanError::syntax(
                    QueryFailureReason::Pattern,
                    format!("the path variable {name} is already a name"),
                ));
            }
        }
        declared.extend(clause_bindings(matched));
        table_columns.extend(clause_bindings(matched).difference(&visible).cloned());
        visible.extend(clause_bindings(matched));
        declared.extend(path_names.iter().cloned());
        table_columns.extend(path_names.iter().filter(|name| !visible.contains(*name)).cloned());
        visible.extend(path_names.iter().cloned());
        values.extend(path_names.iter().cloned());
        // Patterns of one clause join unconditionally, so a later pattern may
        // anchor on an earlier one's bindings. Whether the clause as a whole
        // survives to bind anything for the *next* clause is what `optional`
        // decides, below.
        // A predicate written inside a pattern reads the same scope a clause
        // WHERE does, so it is held to the same declared-binding check.
        for predicate in element_predicates(matched)
            .chain(operand_predicates(matched))
            .chain(&matched.predicate)
        {
            check_field_sources(predicate, &values)?;
        }
        for predicate in element_predicates(matched) {
            validate_declared_bindings(&lower_expression(predicate)?, &visible)?;
        }
        // Cypher matches a clause's patterns relationship-isomorphically: no
        // relationship is used twice within one MATCH. With two or more
        // hops this can happen across fixed and variable-length paths, so
        // every hop is named and their relationship identities held disjoint.
        let clause_hops = matched
            .patterns
            .iter()
            .flat_map(PathPattern::all_elements)
            .filter(|element| matches!(element, PatternElement::Relationship(_)))
            .count();
        let name_every_hop = clause_hops >= 2
            || matched.patterns.iter().any(|path| {
                !path.modes.is_empty()
                    || path.search.is_some()
                    || path.binding.is_some()
                    || !path.subpaths.is_empty()
                    || !iteration_names(path).is_empty()
                    || path.alternatives.is_some()
            });
        let mut hop_names: Vec<Symbol> = Vec::new();
        let mut clause_bound = bound.clone();
        let mut path_values = BTreeSet::new();
        let mut lowered = None;
        let mut clause_deferred = Vec::new();
        for written in &matched.patterns {
            // §16.7 G030 to G033: a union's operands lower one by one, each a
            // path of its own, and their matches are the pattern's.
            let operands = union_operands(written, &exterior, &clause_bound, matched)?;
            let union = written.alternatives.is_some();
            let mut operand_plans = Vec::new();
            let mut words = Vec::new();
            for (pattern, operand_predicate) in &operands {
                let pattern: &PathPattern = pattern;
                use hydradb_cypher_ast::PathMode;
                let restrictive = pattern
                    .modes
                    .iter()
                    .any(|mode| matches!(mode, PathMode::Trail | PathMode::Simple | PathMode::Acyclic));
                // §16.4 Syntax Rule 14: an unbounded quantifier needs DIFFERENT
                // EDGES, a restrictive path mode or a selective path pattern.
                let unbounded = pattern
                .elements
                .iter()
                .any(|element| matches!(element, PatternElement::Relationship(relationship) if relationship.length.is_some_and(|length| length.maximum.is_none())));
                if repeatable && unbounded && !restrictive {
                    if pattern.search.is_some() {
                        // Selective, so permitted, but its candidates are walks
                        // without end, which the expansion cannot enumerate.
                        return Err(GraphPlanError::UnsupportedCypher(
                            QueryFailureReason::Pattern,
                            "a path search over unbounded walks is not supported; add TRAIL, SIMPLE or ACYCLIC".to_string(),
                        ));
                    }
                    return Err(GraphPlanError::syntax(
                        QueryFailureReason::Pattern,
                        "under REPEATABLE ELEMENTS an unbounded quantifier needs TRAIL, SIMPLE or ACYCLIC".to_string(),
                    ));
                }
                if pattern.search.is_some() {
                    check_selective_path(pattern, &exterior, matched)?;
                }
                // §16.7 Syntax Rule 9: quantified paths do not nest.
                if pattern.elements.iter().any(|element| {
                matches!(element, PatternElement::Relationship(relationship) if relationship.iteration_nodes.as_ref().is_some_and(|iteration| iteration.nested))
            }) {
                return Err(GraphPlanError::syntax(
                    QueryFailureReason::Pattern,
                    "a quantified path contains no quantified path or edge".to_string(),
                ));
            }
                let first_hop = hop_names.len();
                let mut parts = Vec::new();
                let mut path_deferred = Vec::new();
                let pattern_plan = lower_path(
                    pattern,
                    &clause_bound,
                    &mut synthesized,
                    PathOptions {
                        record: &record,
                        table_columns: &table_columns,
                        values: &values,
                        visible: &visible,
                        incoming: &exterior,
                        path_values: &path_values,
                        name_every_hop,
                        // Edges repeat only under REPEATABLE ELEMENTS, and then
                        // not on a TRAIL; SIMPLE and ACYCLIC bound the walk by
                        // its nodes instead, which lets a SIMPLE path close
                        // over the edge it left by.
                        trail: !repeatable || pattern.modes.contains(&PathMode::Trail),
                        nodes: if pattern.modes.contains(&PathMode::Acyclic) {
                            WalkNodes::Acyclic
                        } else if pattern.modes.contains(&PathMode::Simple) {
                            WalkNodes::Simple
                        } else {
                            WalkNodes::Any
                        },
                        // An operand's word is read in written order.
                        anchor: !union,
                    },
                    &mut hop_names,
                    &mut path_deferred,
                    &mut parts,
                )?;
                // §22.2 General Rule 16 d) ii): each path mode holds of the path.
                // TRAIL is what DIFFERENT EDGES already holds of the whole clause.
                for mode in &pattern.modes {
                    match mode {
                        PathMode::Walk => {}
                        PathMode::Trail => {
                            // DIFFERENT EDGES holds two names' edges apart, but one
                            // name written for two hops is one edge traversed twice.
                            let hops = &hop_names[first_hop..];
                            let repeated = hops.iter().collect::<BTreeSet<_>>().len() < hops.len();
                            if repeatable || repeated {
                                path_deferred.extend(trail_relationships(hops));
                            }
                        }
                        PathMode::Simple | PathMode::Acyclic => path_deferred.push(LogicalExpression::PathNodesDistinct {
                            simple: matches!(mode, PathMode::Simple),
                            parts: parts.clone(),
                        }),
                    }
                }
                let pattern_plan = match &pattern.search {
                    // An operand's predicates and path modes hold of its own
                    // matches: on another operand's rows its names are null.
                    // Its WHERE may read its subpaths and groups, so they are
                    // bound before it.
                    None if union => {
                        let mut input =
                            path_deferred
                                .into_iter()
                                .fold(bind_path_values(pattern_plan, pattern, &parts), |input, predicate| {
                                    GraphLogicalPlan::Filter {
                                        operation: operation::MATCH,
                                        input: Box::new(input),
                                        predicate,
                                    }
                                });
                        if let Some(predicate) = operand_predicate {
                            // §16.7 Syntax Rule 11: the operand, and so every
                            // subquery in its WHERE, reads its own variables and
                            // those from outside the union only. Another
                            // operand's names are out of its scope, so inside a
                            // subquery one is a new variable or no reference at
                            // all, never a correlation with that operand.
                            let own: BTreeSet<Symbol> = path_bindings(pattern).into_iter().chain(path_variables(pattern)).collect();
                            let others: BTreeSet<Symbol> = path_bindings(written)
                                .into_iter()
                                .chain(path_variables(written))
                                .filter(|name| !own.contains(name))
                                .collect();
                            let outside = |names: &BTreeSet<Symbol>| names.difference(&others).cloned().collect::<BTreeSet<_>>();
                            let mut operand_bound = clause_bound.clone();
                            operand_bound.extend(path_bindings(pattern));
                            // It aggregates its own groups horizontally (GE09), and so may
                            // the subqueries in it.
                            let own_groups: BTreeSet<Symbol> = groups.iter().cloned().chain(group_variables(pattern)).collect();
                            let predicate = lower_predicate_into(
                                predicate,
                                &mut input,
                                PredicateScope {
                                    record: &record,
                                    table_columns: &table_columns,
                                    visible: &outside(&visible),
                                    bound: &outside(&operand_bound),
                                    values: &outside(&values),
                                    groups: &own_groups,
                                    clause: "a union operand",
                                },
                                &mut synthesized,
                            )?;
                            input = GraphLogicalPlan::Filter {
                                operation: operation::MATCH,
                                input: Box::new(input),
                                predicate,
                            };
                        }
                        input
                    }
                    None => {
                        clause_deferred.append(&mut path_deferred);
                        bind_path_values(pattern_plan, pattern, &parts)
                    }
                    // §22.4: a selective path's candidates are its matches, held
                    // to its own predicates, path modes and, under DIFFERENT EDGES,
                    // distinct edges (§16.4 General Rule 5 c) i)); the selection
                    // is made over them, before the graph pattern's WHERE.
                    Some(search) => {
                        if !repeatable {
                            path_deferred.extend(distinct_relationships(&hop_names[first_hop..]));
                        }
                        // A WHERE around the path may read its subpaths and
                        // groups, so then every candidate binds them; without
                        // one only the selected paths do.
                        let early = pattern.predicate.is_some();
                        let candidates = if early {
                            bind_path_values(pattern_plan, pattern, &parts)
                        } else {
                            pattern_plan
                        };
                        let mut input = path_deferred
                            .into_iter()
                            .fold(candidates, |input, predicate| GraphLogicalPlan::Filter {
                                operation: operation::MATCH,
                                input: Box::new(input),
                                predicate,
                            });
                        // The WHERE around the path (G050) is one of its own
                        // predicates, so it too filters the candidates.
                        if let Some(predicate) = &pattern.predicate {
                            let mut path_bound = clause_bound.clone();
                            path_bound.extend(path_bindings(pattern));
                            // It aggregates its own groups horizontally (GE09), and so may
                            // the subqueries in it.
                            let own_groups: BTreeSet<Symbol> = groups.iter().cloned().chain(group_variables(pattern)).collect();
                            let predicate = lower_predicate_into(
                                predicate,
                                &mut input,
                                PredicateScope {
                                    record: &record,
                                    table_columns: &table_columns,
                                    visible: &visible,
                                    bound: &path_bound,
                                    values: &values,
                                    groups: &own_groups,
                                    clause: "a selective path",
                                },
                                &mut synthesized,
                            )?;
                            input = GraphLogicalPlan::Filter {
                                operation: operation::MATCH,
                                input: Box::new(input),
                                predicate,
                            };
                        }
                        let selected = selected_paths(input, pattern, &parts, search)?;
                        if early {
                            selected
                        } else {
                            bind_path_values(selected, pattern, &parts)
                        }
                    }
                };
                if union {
                    words.push(path_word(pattern, &parts, &synthesized));
                }
                operand_plans.push(pattern_plan);
            }
            let pattern_plan = match written.alternatives.as_ref() {
                None => operand_plans.pop().expect("one operand"),
                Some(alternatives) => GraphLogicalPlan::PathUnion {
                    inputs: operand_plans,
                    // §16.7 Syntax Rule 10: each operand of a multiset
                    // alternation has a subpath variable of its own, so no
                    // two operands' words agree.
                    words: (!alternatives.multiset).then_some(words),
                },
            };
            clause_bound.extend(path_bindings(written));
            path_values.extend(path_variables(written));
            lowered = Some(match lowered {
                Some(previous) => GraphLogicalPlan::NaturalJoin {
                    left: Box::new(previous),
                    right: Box::new(pattern_plan),
                    optional: false,
                },
                None => pattern_plan,
            });
        }
        bound = clause_bound;
        let mut clause_plan = lowered.expect("non-empty patterns");
        if !repeatable {
            clause_deferred.extend(distinct_relationships(&hop_names));
        }
        for predicate in clause_deferred {
            clause_plan = GraphLogicalPlan::Filter {
                operation: hydradb_cypher_ast::operation::MATCH,
                input: Box::new(clause_plan),
                predicate,
            };
        }
        // An EXISTS in the WHERE becomes a mark computed over the rows the
        // WHERE filters: the optional pattern's own for an OPTIONAL MATCH,
        // the joined rows otherwise.
        let mut marks = Vec::new();
        // This MATCH's groups are groups to its WHERE's subqueries too.
        groups.extend(matched.patterns.iter().flat_map(group_variables));
        let predicate = matched
            .predicate
            .as_ref()
            .map(|predicate| {
                lower_predicate(
                    predicate,
                    PredicateScope {
                        record: &record,
                        table_columns: &table_columns,
                        visible: &visible,
                        bound: &bound,
                        values: &values,
                        groups: &groups,
                        clause: "WHERE",
                    },
                    &mut synthesized,
                    &mut marks,
                )
            })
            .transpose()?;
        // A predicate on an OPTIONAL MATCH belongs to the optional pattern,
        // not to the rows the clause is optional *for*: a row whose optional
        // side fails it keeps its place with that side null, exactly as a row
        // whose pattern never matched does. Filtering after the join would
        // drop it instead, turning "whose optional side is not X" into "which
        // has no optional side at all". An ordinary MATCH is an inner join, so
        // there the two positions agree and the filter stays above it, where
        // the planner already reads it.
        if matched.optional {
            if let Some(predicate) = predicate.clone() {
                clause_plan = GraphLogicalPlan::Filter {
                    operation: hydradb_cypher_ast::operation::MATCH,
                    input: Box::new(apply_marks(clause_plan, std::mem::take(&mut marks))),
                    predicate,
                };
            }
        }
        clause_plan = match plan {
            Some(previous) => GraphLogicalPlan::NaturalJoin {
                left: Box::new(previous),
                right: Box::new(clause_plan),
                optional: matched.optional,
            },
            // An OPTIONAL MATCH that opens the query is optional for the one
            // row nothing has bound: a miss still yields that row, its names
            // null, which is what makes `OPTIONAL MATCH (n:Missing) RETURN n`
            // answer one row rather than none.
            None if matched.optional => GraphLogicalPlan::NaturalJoin {
                left: Box::new(GraphLogicalPlan::SingleRow),
                right: Box::new(clause_plan),
                optional: true,
            },
            None => clause_plan,
        };
        if !matched.optional {
            if let Some(predicate) = predicate {
                clause_plan = GraphLogicalPlan::Filter {
                    operation: hydradb_cypher_ast::operation::MATCH,
                    input: Box::new(apply_marks(clause_plan, marks)),
                    predicate,
                };
            }
        }
        plan = Some(clause_plan);
        // §16.3 Syntax Rule 12 a): YIELD keeps the names the clause read from
        // before and the ones it lists, and is the projection of exactly those.
        if let Some(yielded) = &matched.yield_items {
            let projection = yield_projection(&incoming, yielded, matched)?;
            let input = plan.take().unwrap_or(GraphLogicalPlan::SingleRow);
            let (projected, projected_scope, projected_values) = lower_with(
                &projection,
                input,
                BindingScope {
                    record: &record,
                    table_columns: &table_columns,
                    visible: &visible,
                    bound: &bound,
                    values: &values,
                    groups: &groups,
                },
                &record_values,
                &mut synthesized,
                operation::MATCH,
            )?;
            plan = Some(projected);
            groups.clear();
            declared.extend(projected_scope.iter().cloned());
            bound = projected_scope.clone();
            visible = projected_scope;
            values = projected_values;
            table_columns.retain(|name| visible.contains(name));
        }
    }
    let mut plan = plan.unwrap_or(GraphLogicalPlan::SingleRow);
    if finished {
        // FINISH reads no result; its clauses are complete here.
        reject_synthesized_collisions(&declared, &synthesized)?;
        outer.synthesized.extend(synthesized.iter().cloned());
        return Ok(LoweredQuery {
            plan: GraphLogicalPlan::Finish { input: Box::new(plan) },
            scope: FinalScope::default(),
        });
    }
    let Some(returned) = returned else {
        // Every clause has been read, so both sets are complete: what the
        // caller named, and what the lowering named for it.
        reject_synthesized_collisions(&declared, &synthesized)?;
        outer.synthesized.extend(synthesized.iter().cloned());
        // An open EXISTS subquery: only whether rows exist is read.
        return Ok(LoweredQuery {
            plan,
            scope: FinalScope {
                visible,
                groups,
                ..FinalScope::default()
            },
        });
    };
    outer.operation.set(operation::RETURN);
    let returned = &resolve_return_keywords(returned, &visible);
    let items = star_expanded(
        &returned.items,
        &projection_columns(&visible, &table_columns, outer, true),
        "RETURN",
    )?;
    if !sort_keys_validated {
        check_restricted_sort_keys(returned, &items)?;
    }
    // An EXISTS in a RETURN item is a mark computed over the rows projected.
    let mut marks = Vec::new();
    let items = items
        .into_iter()
        .map(|item| {
            Ok(Projection {
                expression: hoist_exists(
                    &item.expression,
                    BindingScope {
                        visible: &visible,
                        bound: &bound,
                        values: &values,
                        groups: &groups,
                        record: &record,
                        table_columns: &table_columns,
                    },
                    &mut synthesized,
                    &mut marks,
                )?,
                alias: item.alias,
            })
        })
        .collect::<GraphPlanResult<Vec<_>>>()?;
    let aggregates = items.iter().any(|item| ast_contains_aggregate(&item.expression));
    // Aggregate aliases are materialized by the original result projection.
    // Hoisting them into a preceding WITH would aggregate the input twice.
    let (sort_view, sort_elements) = if aggregates {
        (items.clone(), Vec::new())
    } else {
        bind_sort_element_columns(&items, &returned.order_by, &mut synthesized)
    };
    // Only now are both sets complete: RETURN's own marks are names the
    // lowering made too, and the RETURN reads and names columns of its own.
    let mut named = declared.clone();
    for item in &returned_items_as_written(returned) {
        named.extend(item.alias.iter().map(|alias| Symbol::from(alias.as_str())));
        named.extend(expression_bindings(&item.expression).unwrap_or_default());
    }
    reject_synthesized_collisions(&named, &synthesized)?;
    outer.synthesized.extend(synthesized.iter().cloned());
    // A column is an element only when it names one a pattern bound; a
    // value, a mark or anything computed is a value (as `lower_with` says).
    // Every arm of a union adds its own: a column that is a value in any arm
    // may hold a value, so no later pattern may bind it.
    let mut returned_values: BTreeSet<Symbol> = items
        .iter()
        .filter_map(|item| {
            let name = match (&item.alias, &item.expression) {
                (Some(alias), _) => Symbol::from(alias.as_str()),
                (None, Expression::Identifier(name)) => Symbol::from(name.as_str()),
                _ => return None,
            };
            let element = matches!(&item.expression, Expression::Identifier(source)
                if bound.contains(&Symbol::from(source.as_str())) && !values.contains(&Symbol::from(source.as_str())));
            (!element).then_some(name)
        })
        .collect();
    plan = apply_marks(plan, marks);
    let visible = with_marks(&visible, &synthesized);
    if !sort_elements.is_empty() {
        // The input's names pass through; the lowering's own are added.
        let own: BTreeSet<Symbol> = sort_elements
            .iter()
            .filter_map(|item| item.alias.as_ref().map(|name| Symbol::from(name.as_str())))
            .collect();
        let base: BTreeSet<Symbol> = visible.difference(&own).cloned().collect();
        let with = passing_with(ProjectionItems::All(sort_elements), None);
        plan = lower_with(
            &with,
            plan,
            BindingScope {
                record: &record,
                table_columns: &table_columns,
                visible: &base,
                bound: &bound,
                values: &values,
                groups: &groups,
            },
            &record_values,
            &mut synthesized,
            operation::RETURN,
        )?
        .0;
    }
    let items = &items;
    let written = &sort_view;
    {
        // Validate every result reference, including initial OPTIONAL MATCH.
        for item in items {
            check_scope(&item.expression, &visible, &values, "RETURN")?;
        }
        // The result is a binding table, whose columns have distinct names
        // (§14.11 Syntax Rule 11).
        let mut names = BTreeSet::new();
        for item in items {
            let name = match (&item.alias, &item.expression) {
                (Some(name), _) | (None, Expression::Identifier(name)) => name,
                _ => continue,
            };
            if !names.insert(name.as_str()) {
                return Err(GraphPlanError::syntax(
                    QueryFailureReason::Return,
                    format!("RETURN names the column {name} more than once"),
                ));
            }
        }
        // ORDER BY at the result may also name a column the projection just
        // created, which is not a binding at all.
        let mut sortable = visible.clone();
        sortable.extend(item_aliases(items));
        let mut sortable_values = values.clone();
        sortable_values.extend(item_aliases(items));
        for item in &returned.order_by {
            check_scope(&item.expression, &sortable, &sortable_values, "ORDER BY")?;
        }
    }
    let items = items
        .iter()
        .map(|item| {
            Ok(LogicalProjection {
                expression: lower_expression(&item.expression)?,
                alias: item.alias.as_ref().map(|alias| Symbol::from(alias.as_str())),
            })
        })
        .collect::<GraphPlanResult<Vec<_>>>()?;

    let aggregates = items.iter().any(|item| contains_aggregate(&item.expression))
        || returned.order_by.iter().any(|sort| ast_contains_aggregate(&sort.expression));
    let sort_items = returned
        .order_by
        .iter()
        .map(|item| {
            let (direction, nulls) = sort_order(item);
            Ok(LogicalSort {
                // Before the projection (no aggregate) a key naming a column
                // reads what the column stands for; after it, the column
                // itself, read where the projection laid its names.
                expression: if aggregates {
                    lower_expression(&item.expression)?
                } else {
                    lower_expression(&sort_key_through_aliases(&item.expression, written))?
                },
                direction,
                nulls,
            })
        })
        .collect::<GraphPlanResult<Vec<_>>>()?;
    if !aggregates && !sort_items.is_empty() {
        plan = GraphLogicalPlan::Sort {
            input: Box::new(plan),
            items: sort_items.clone(),
        };
    }
    let distinct = matches!(returned.quantifier, Some(SetQuantifier::Distinct));
    // SKIP and LIMIT count result rows. Aggregation and DISTINCT both change
    // how many rows there are, so their window applies after the projection,
    // never to the binding rows beneath it.
    let window_after_projection = aggregates || distinct;
    if !window_after_projection {
        if let Some(skip) = &returned.skip {
            plan = GraphLogicalPlan::Skip {
                input: Box::new(plan),
                count: lower_window_expression(skip, "SKIP")?,
            };
        }
    }
    if !window_after_projection {
        if let Some(limit) = &returned.limit {
            plan = GraphLogicalPlan::Limit {
                input: Box::new(plan),
                count: lower_window_expression(limit, "LIMIT")?,
            };
        }
    }
    let base = GraphLogicalPlan::Project {
        context: crate::plan::logical::ProjectionContext {
            fixed: record.iter().cloned().collect(),
            table_columns: table_columns.iter().cloned().collect(),
            defined: Vec::new(),
            explicit_grouping: !returned.group_by.is_empty(),
        },
        operation: hydradb_cypher_ast::operation::RETURN,
        input: Box::new(plan),
        items,
        distinct,
        post_sort: if aggregates { sort_items } else { Vec::new() },
        post_skip: if window_after_projection {
            returned
                .skip
                .as_ref()
                .map(|skip| lower_window_expression(skip, "SKIP"))
                .transpose()?
        } else {
            None
        },
        post_limit: if window_after_projection {
            returned
                .limit
                .as_ref()
                .map(|limit| lower_window_expression(limit, "LIMIT"))
                .transpose()?
        } else {
            None
        },
    };
    if query.unions.is_empty() {
        return Ok(LoweredQuery {
            plan: base,
            scope: FinalScope {
                returned_values,
                ..FinalScope::default()
            },
        });
    }
    // §14.2 Syntax Rule 3: one composite query uses one conjunction, ALL
    // or DISTINCT included.
    let (conjunction, all) = (query.unions[0].conjunction, query.unions[0].all);
    if query.unions.iter().any(|arm| arm.conjunction != conjunction || arm.all != all) {
        return Err(GraphPlanError::syntax(
            QueryFailureReason::Union,
            "a composite query cannot mix its conjunctions, nor ALL with DISTINCT".to_string(),
        ));
    }
    // The arms are column name-equal (§14.2 Syntax Rule 10). An arm with an
    // unnamed item or `*` is left to the capability check.
    let first = returned_names(query);
    if first.is_some()
        && query
            .unions
            .iter()
            .any(|arm| returned_names(&arm.query).is_some_and(|names| Some(names) != first))
    {
        return Err(GraphPlanError::syntax(
            QueryFailureReason::Union,
            "every arm of a composite query must return the same column names".to_string(),
        ));
    }
    let mut arms = Vec::with_capacity(query.unions.len() + 1);
    arms.push(base);
    for arm in &query.unions {
        let arm = lower_query_scoped(&arm.query, outer)?;
        returned_values.extend(arm.scope.returned_values);
        arms.push(arm.plan);
    }
    Ok(LoweredQuery {
        plan: GraphLogicalPlan::Union { arms, all, conjunction },
        scope: FinalScope {
            returned_values,
            ..FinalScope::default()
        },
    })
}

/// Check result boundaries (§9.2 SR7, §14.2 SR5) before capability refusal.
/// Returns whether NEXT occurs in this query or one of its composite arms.
fn check_composition_results(query: &Query) -> GraphPlanResult<bool> {
    let has_next = query.clauses.iter().any(|clause| matches!(clause, Clause::Next));
    if has_next
        && query
            .clauses
            .split(|clause| matches!(clause, Clause::Next))
            .any(|part| !matches!(part.last(), Some(Clause::Return(_) | Clause::Finish)))
    {
        return Err(GraphPlanError::syntax(
            QueryFailureReason::Return,
            "every statement joined by NEXT ends in RETURN or FINISH".to_string(),
        ));
    }
    // A NEXT arm is a nested procedure body, not a directly contained FINISH.
    let finishes = |arm: &Query| {
        matches!(arm.clauses.last(), Some(Clause::Finish)) && !arm.clauses.iter().any(|clause| matches!(clause, Clause::Next))
    };
    if !query.unions.is_empty() && (finishes(query) || query.unions.iter().any(|arm| finishes(&arm.query))) {
        return Err(GraphPlanError::syntax(
            QueryFailureReason::Union,
            "FINISH cannot end an arm of a composite query".to_string(),
        ));
    }
    let mut any_next = has_next;
    for arm in &query.unions {
        any_next |= check_composition_results(&arm.query)?;
    }
    Ok(any_next)
}

fn with_next_definitions(query: &Query, outer: &OuterScope) -> Option<Query> {
    if !query.clauses.iter().any(|clause| matches!(clause, Clause::Next))
        || !query
            .clauses
            .iter()
            .any(|clause| matches!(clause, Clause::Let(binding) if binding.definition))
    {
        return None;
    }
    let mut clauses: Vec<Clause> = Vec::with_capacity(query.clauses.len());
    // The block's value variable definitions (§9.2 General Rule 1) hold for
    // every statement: their values cross each NEXT beside the table.
    // Each is evaluated again at the head of every later statement, as it
    // was at the head of the body: its value keywords resolved against what
    // the body's head could see, and run in a call that imports nothing, so
    // a column a statement returns (one named `current_date`, or a name a
    // subquery in the definition matches, say) cannot change what the
    // definition means. Their values come from the request alone, so each
    // evaluation is the same.
    let mut definitions: Vec<Clause> = Vec::new();
    let mut defined: BTreeSet<Symbol> = outer.visible.clone();
    for clause in &query.clauses {
        if !matches!(clause, Clause::Next) {
            if let Clause::Let(binding) = clause {
                if binding.definition {
                    let mut resolved = binding.clone();
                    for (name, value) in &mut resolved.bindings {
                        *value = resolve_value_keywords(value, &defined);
                        defined.insert(Symbol::from(name.as_str()));
                    }
                    definitions.push(Clause::Let(resolved));
                }
            }
            clauses.push(clause.clone());
            continue;
        }
        clauses.push(Clause::Next);
        if !definitions.is_empty() {
            // A column of a definition's name is then declared twice
            // (§9.2 Syntax Rule 12 c)), which the call refuses.
            clauses.push(Clause::Call(CallClause {
                optional: false,
                outputs_to_record: true,
                scope: Some(Vec::new()),
                query: Box::new(Query {
                    clauses: definitions
                        .iter()
                        .cloned()
                        .chain([Clause::Return(ReturnClause {
                            quantifier: None,
                            items: ProjectionItems::Items(
                                defined
                                    .difference(&outer.visible)
                                    .map(|name| Projection {
                                        expression: Expression::Identifier(Identifier::from(name.as_str())),
                                        alias: None,
                                    })
                                    .collect(),
                            ),
                            group_by: Vec::new(),
                            order_by: Vec::new(),
                            skip: None,
                            limit: None,
                        })])
                        .collect(),
                    unions: Vec::new(),
                }),
            }));
        }
    }
    Some(Query {
        clauses,
        unions: query.unions.clone(),
    })
}

/// Validate NEXT boundaries before lowering; FINISH restarts from a unit
/// table (§9.2 GR3), unlike RETURN, whose columns remain in scope.
fn finish_before_next(query: &Query, outer: &OuterScope) -> GraphPlanResult<Option<usize>> {
    let has_next = check_composition_results(query)?;
    if !has_next {
        return Ok(None);
    }
    if !query.unions.is_empty() {
        return Err(GraphPlanError::UnsupportedCypher(
            QueryFailureReason::Return,
            "NEXT in a composite query is not supported".to_string(),
        ));
    }
    if !outer.visible.is_empty() || outer.open {
        return Err(GraphPlanError::UnsupportedCypher(
            QueryFailureReason::Return,
            "NEXT inside a subquery is not supported".to_string(),
        ));
    }
    Ok(query
        .clauses
        .windows(2)
        .position(|pair| matches!(pair, [Clause::Finish, Clause::Next]))
        .map(|index| index + 1))
}

/// The WITH a RETURN before NEXT means. A RETURN may sort by names it does
/// not return (§14.10 Syntax Rule 4, before projection) and a WITH may not,
/// so such a RETURN becomes two: the first projects the items beside the
/// names its sort reads and sorts and pages there, the second keeps only the
/// items. A projection keeps its input's order, so the second inherits it.
fn return_before_next(mut returned: ReturnClause, reserved: &BTreeSet<Symbol>, next_columns: &mut BTreeSet<Symbol>) -> Vec<Clause> {
    let hidden = name_next_columns(&mut returned, reserved, next_columns);
    let restricted = matches!(returned.quantifier, Some(SetQuantifier::Distinct))
        || !returned.group_by.is_empty()
        || returned.items.items().iter().any(|item| ast_contains_aggregate(&item.expression));
    if restricted {
        rewrite_next_sort_keys(&mut returned);
    }
    next_projection_boundaries(returned, &hidden, restricted)
}

fn name_next_columns(returned: &mut ReturnClause, reserved: &BTreeSet<Symbol>, next_columns: &mut BTreeSet<Symbol>) -> BTreeSet<String> {
    // A WITH names every computed item. No later statement can read a column
    // a RETURN leaves unnamed (§14.11 Syntax Rule 8 b) gives it no name), so
    // it takes a name of the lowering's own for this projection alone: the
    // statement after NEXT sees only the named columns.
    let mut hidden: BTreeSet<String> = BTreeSet::new();
    {
        // `RETURN *, 2` names its extras as `RETURN 2` names its items.
        let items = match &mut returned.items {
            ProjectionItems::Items(items) | ProjectionItems::All(items) => items,
        };
        // Names the query uses, which a lowering name must avoid: an item's
        // would shadow the input name a sort key reads, and any other would
        // be taken for the lowering's own.
        let mut used: BTreeSet<String> = reserved.iter().map(|name| name.as_str().to_string()).collect();
        let read = items
            .iter()
            .map(|item| &item.expression)
            .chain(returned.order_by.iter().map(|sort| &sort.expression))
            .chain(returned.group_by.iter());
        for expression in read {
            used.extend(
                expression_bindings(expression)
                    .unwrap_or_default()
                    .iter()
                    .map(|name| name.as_str().to_string()),
            );
        }
        used.extend(
            items
                .iter()
                .flat_map(|item| item.alias.iter())
                .map(|alias| alias.as_str().to_string()),
        );
        let mut next = 0;
        for item in items.iter_mut() {
            if item.alias.is_none() && !matches!(item.expression, Expression::Identifier(_)) {
                let name = loop {
                    let name = format!("{NEXT_COLUMN_PREFIX}{next}");
                    next += 1;
                    if !used.contains(&name) {
                        break name;
                    }
                };
                used.insert(name.clone());
                hidden.insert(name.clone());
                next_columns.insert(Symbol::from(name.as_str()));
                item.alias = Some(Identifier::from(name));
            }
        }
    }
    hidden
}

fn rewrite_next_sort_keys(returned: &mut ReturnClause) {
    // After DISTINCT or an aggregation a key reads only the items, and
    // may repeat one's expression (§14.10 Syntax Rule 4): the WITH reads
    // that item's column instead.
    let items = match &returned.items {
        ProjectionItems::Items(items) | ProjectionItems::All(items) => items,
    };
    let replacements: Vec<(Expression, Identifier)> = items
        .iter()
        .filter_map(|item| match (&item.alias, &item.expression) {
            (Some(name), expression) => Some((expression.clone(), name.clone())),
            (None, Expression::Identifier(name)) => Some((Expression::Identifier(name.clone()), name.clone())),
            (None, _) => None,
        })
        .collect();
    // An output name in a key is that output, not the input another
    // item's expression reads.
    let replacements = unshadowed(replacements, items);
    for sort in &mut returned.order_by {
        sort.expression = replace_group_keys(&sort.expression, &replacements);
    }
}

fn return_as_with(returned: ReturnClause) -> Clause {
    Clause::With(WithClause {
        table_only: true,
        quantifier: returned.quantifier,
        items: returned.items,
        group_by: returned.group_by,
        order_by: returned.order_by,
        skip: returned.skip,
        limit: returned.limit,
        predicate: None,
    })
}

fn next_projection_boundaries(returned: ReturnClause, hidden: &BTreeSet<String>, restricted: bool) -> Vec<Clause> {
    let ProjectionItems::Items(items) = &returned.items else {
        // `*` keeps every input name, so a key needs none beside it; a
        // lowering name leaves with the star the next WITH expands, which
        // never includes one.
        if hidden.is_empty() {
            return vec![return_as_with(returned)];
        }
        return vec![
            return_as_with(returned),
            Clause::With(passing_with(ProjectionItems::All(Vec::new()), None)),
        ];
    };
    let names: Vec<&str> = items
        .iter()
        .filter_map(|item| match (&item.alias, &item.expression) {
            (Some(name), _) | (None, Expression::Identifier(name)) => Some(name.as_str()),
            _ => None,
        })
        .collect();
    let mut extra: Vec<Identifier> = Vec::new();
    for sort in &returned.order_by {
        let mut read = Vec::new();
        references_outside_aggregates(&sort.expression, &mut read);
        for name in read {
            if !names.contains(&name.as_str()) && !extra.contains(name) {
                extra.push(name.clone());
            }
        }
    }
    if restricted {
        extra.clear();
    }
    if extra.is_empty() && hidden.is_empty() {
        return vec![return_as_with(returned)];
    }
    let mut wide = items.clone();
    wide.extend(extra.into_iter().map(|name| Projection {
        expression: Expression::Identifier(name),
        alias: None,
    }));
    let narrow = names
        .iter()
        .filter(|name| !hidden.contains(**name))
        .map(|name| Projection {
            expression: Expression::Identifier(Identifier::from(*name)),
            alias: None,
        })
        .collect();
    vec![
        return_as_with(ReturnClause {
            items: ProjectionItems::Items(wide),
            ..returned
        }),
        Clause::With(passing_with(ProjectionItems::Items(narrow), None)),
    ]
}

/// The column names of a query's RETURN, when every item is named.
fn returned_names(query: &Query) -> Option<Vec<&str>> {
    let Some(Clause::Return(returned)) = query.clauses.last() else {
        return None;
    };
    let ProjectionItems::Items(items) = &returned.items else {
        return None;
    };
    items
        .iter()
        .map(|item| match (&item.alias, &item.expression) {
            (Some(name), _) | (None, Expression::Identifier(name)) => Some(name.as_str()),
            _ => None,
        })
        .collect()
}

/// The prefix of a mark an `EXISTS` subquery binds; see [`hoist_exists`].
const EXISTS_MARK_PREFIX: &str = "__hydradb_exists_";
/// The name an unnamed column of a RETURN before NEXT is passed on under.
const NEXT_COLUMN_PREFIX: &str = "__hydradb_next_column_";

/// `visible` with every EXISTS mark synthesized so far, which an expression
/// whose EXISTS were hoisted reads in their place.
fn with_marks(visible: &BTreeSet<Symbol>, synthesized: &BTreeSet<Symbol>) -> BTreeSet<Symbol> {
    let mut readable = visible.clone();
    readable.extend(
        synthesized
            .iter()
            .filter(|name| name.as_str().starts_with(EXISTS_MARK_PREFIX))
            .cloned(),
    );
    readable
}

/// A subquery hoisted out of an expression, computed into a mark name over
/// the rows the expression reads.
type Mark = (Symbol, Subquery);

enum Subquery {
    /// `EXISTS { ... }`: whether it yields a row.
    Exists(GraphLogicalPlan),
    /// `VALUE { ... }`: its one column, `column`, of the one row it yields.
    Value { plan: GraphLogicalPlan, column: Symbol },
}

/// Run each mark's subquery over `plan`'s rows, innermost first.
fn apply_marks(plan: GraphLogicalPlan, marks: Vec<Mark>) -> GraphLogicalPlan {
    marks.into_iter().fold(plan, |input, (binding, subquery)| match subquery {
        Subquery::Exists(subquery) => GraphLogicalPlan::Exists {
            input: Box::new(input),
            subquery: Box::new(subquery),
            binding,
        },
        Subquery::Value { plan, column } => GraphLogicalPlan::ScalarSubquery {
            input: Box::new(input),
            subquery: Box::new(plan),
            column,
            binding,
        },
    })
}

/// [`hoist_exists`], applying the marks to `plan` at once.
fn hoist_exists_into(
    expression: &Expression,
    plan: &mut GraphLogicalPlan,
    context: BindingScope<'_>,
    synthesized: &mut BTreeSet<Symbol>,
) -> GraphPlanResult<Expression> {
    let mut marks = Vec::new();
    let rewritten = hoist_exists(expression, context, synthesized, &mut marks)?;
    let input = std::mem::replace(plan, GraphLogicalPlan::SingleRow);
    *plan = apply_marks(input, marks);
    Ok(rewritten)
}

struct PredicateScope<'a> {
    record: &'a BTreeSet<Symbol>,
    table_columns: &'a BTreeSet<Symbol>,
    visible: &'a BTreeSet<Symbol>,
    bound: &'a BTreeSet<Symbol>,
    values: &'a BTreeSet<Symbol>,
    groups: &'a BTreeSet<Symbol>,
    clause: &'a str,
}

fn lower_predicate(
    expression: &Expression,
    scope: PredicateScope<'_>,
    synthesized: &mut BTreeSet<Symbol>,
    marks: &mut Vec<Mark>,
) -> GraphPlanResult<LogicalExpression> {
    let expression = hoist_exists(
        expression,
        BindingScope {
            visible: scope.visible,
            bound: scope.bound,
            values: scope.values,
            groups: scope.groups,
            record: scope.record,
            table_columns: scope.table_columns,
        },
        synthesized,
        marks,
    )?;
    let readable = with_marks(scope.visible, synthesized);
    let lowered = horizontal_aggregates(lower_expression(&expression)?, scope.groups)?;
    validate_declared_bindings(&lowered, &readable)?;
    check_scope(&expression, &readable, scope.values, scope.clause)?;
    Ok(lowered)
}

fn lower_predicate_into(
    expression: &Expression,
    plan: &mut GraphLogicalPlan,
    scope: PredicateScope<'_>,
    synthesized: &mut BTreeSet<Symbol>,
) -> GraphPlanResult<LogicalExpression> {
    let mut marks = Vec::new();
    let expression = lower_predicate(expression, scope, synthesized, &mut marks)?;
    let input = std::mem::replace(plan, GraphLogicalPlan::SingleRow);
    *plan = apply_marks(input, marks);
    Ok(expression)
}

/// Replace every `EXISTS { ... }` in `expression` with a fresh mark name and
/// lower its subquery against the enclosing scope (GQL §19.4).
///
/// The subquery reads the enclosing names because it runs correlated: each
/// row it is asked about seeds it, as an OPTIONAL MATCH's right side is
/// seeded, so a pattern naming an enclosing variable is anchored on that
/// row's element. The mark then holds whether the subquery found a row.
fn hoist_exists(
    expression: &Expression,
    context: BindingScope<'_>,
    synthesized: &mut BTreeSet<Symbol>,
    marks: &mut Vec<Mark>,
) -> GraphPlanResult<Expression> {
    let mut hoist = |expression: &Expression| hoist_exists(expression, context, synthesized, marks);
    Ok(match expression {
        Expression::Exists(query) => {
            let mut scope = OuterScope::correlated_query(context, synthesized);
            scope.open = true;
            let subquery = lower_query_in(&named_return_items(query), &mut scope)?;
            scope.export_generated(synthesized);
            let mark = new_mark(synthesized);
            marks.push((mark.clone(), Subquery::Exists(subquery)));
            Expression::Identifier(Identifier::from(mark.as_str()))
        }
        Expression::Value(query) => {
            check_value_query_shape(query)?;
            let mut scope = OuterScope::correlated_query(context, synthesized);
            let plan = lower_query_in(query, &mut scope)?;
            let column = value_query_column(&plan, &scope.synthesized)?;
            check_value_cardinality(query, &plan)?;
            scope.export_generated(synthesized);
            let mark = new_mark(synthesized);
            marks.push((mark.clone(), Subquery::Value { plan, column }));
            Expression::Identifier(Identifier::from(mark.as_str()))
        }
        // A mark is computed before the LET runs, so a subquery in its body
        // could not see the names it binds (deviations L24).
        Expression::Let { body, .. } if contains_subquery(body) => {
            return Err(GraphPlanError::UnsupportedCypher(
                QueryFailureReason::Return,
                "a subquery inside a LET body is not supported".to_string(),
            ));
        }
        // Every other expression: its children, whatever they are -- a
        // record's fields, a property's target, a CASE's branches.
        other => other.try_map_children(&mut hoist)?,
    })
}

/// Whether `expression` holds an `EXISTS` or `VALUE` subquery anywhere.
fn contains_subquery(expression: &Expression) -> bool {
    match expression {
        Expression::Exists(_) | Expression::Value(_) => true,
        other => other.children().into_iter().any(contains_subquery),
    }
}

/// A fresh name for a hoisted subquery's mark, recorded as synthesized.
fn new_mark(synthesized: &mut BTreeSet<Symbol>) -> Symbol {
    let index = synthesized
        .iter()
        .filter(|name| name.as_str().starts_with(EXISTS_MARK_PREFIX))
        .count();
    let mark = Symbol::from(format!("{EXISTS_MARK_PREFIX}{index}").as_str());
    synthesized.insert(mark.clone());
    mark
}

/// Names returned by an inline procedure (§15.1 Syntax Rule 8). Binding
/// kinds come from the lowered child's [`FinalScope::returned_values`].
fn call_columns(query: &Query) -> GraphPlanResult<Vec<Symbol>> {
    let Some(Clause::Return(returned)) = query.clauses.last() else {
        return Err(GraphPlanError::UnsupportedCypher(
            QueryFailureReason::Procedure,
            "an inline procedure that does not end in RETURN is not supported".to_string(),
        ));
    };
    let ProjectionItems::Items(items) = &returned.items else {
        return Err(GraphPlanError::UnsupportedCypher(
            QueryFailureReason::Procedure,
            "an inline procedure's RETURN * is not supported".to_string(),
        ));
    };
    let mut columns = Vec::new();
    for item in items {
        let name = match (&item.alias, &item.expression) {
            (Some(name), _) | (None, Expression::Identifier(name)) => Symbol::from(name.as_str()),
            _ => {
                return Err(GraphPlanError::syntax(
                    QueryFailureReason::Procedure,
                    "an inline procedure's RETURN names each column",
                ))
            }
        };
        columns.push(name);
    }
    Ok(columns)
}

/// §20.6 Syntax Rules 3 to 9: one linear query with one named column.
fn check_value_query_shape(query: &Query) -> GraphPlanResult<()> {
    let syntax = |message: &str| GraphPlanError::syntax(QueryFailureReason::Return, message.to_string());
    if !query.unions.is_empty() {
        return Err(syntax("a VALUE subquery is one linear query, not a composite one"));
    }
    let Some(Clause::Return(returned)) = query.clauses.last() else {
        return Err(syntax("a VALUE subquery ends in RETURN"));
    };
    let ProjectionItems::Items(items) = &returned.items else {
        return Err(syntax("a VALUE subquery returns one named column, not *"));
    };
    let [_] = items.as_slice() else {
        return Err(syntax("a VALUE subquery returns one column"));
    };
    Ok(())
}

/// Read the resolved result name, never a mark synthesized for an unnamed item.
fn value_query_column(plan: &GraphLogicalPlan, synthesized: &BTreeSet<Symbol>) -> GraphPlanResult<Symbol> {
    let mut result = plan;
    while let GraphLogicalPlan::Next { next, .. } = result {
        result = next;
    }
    if let GraphLogicalPlan::Project { items, .. } = result {
        if let [item] = items.as_slice() {
            match (&item.alias, &item.expression) {
                (Some(name), _) => return Ok(name.clone()),
                (None, LogicalExpression::Binding(name)) if !synthesized.contains(name) => return Ok(name.clone()),
                _ => {}
            }
        }
    }
    Err(GraphPlanError::syntax(
        QueryFailureReason::Return,
        "a VALUE subquery's column needs a name",
    ))
}

/// §20.6 SR10: cardinality follows the resolved record/table scope.
fn check_value_cardinality(query: &Query, plan: &GraphLogicalPlan) -> GraphPlanResult<()> {
    let Some(Clause::Return(returned)) = query.clauses.last() else {
        unreachable!("VALUE shape is checked before lowering")
    };
    if matches!(&returned.limit, Some(Expression::Literal(Literal::Integer(1)))) {
        return Ok(());
    }
    let mut result = plan;
    while let GraphLogicalPlan::Next { next, .. } = result {
        result = next;
    }
    let ungrouped = match result {
        GraphLogicalPlan::Project { context, items, .. } if items.iter().any(|item| contains_aggregate(&item.expression)) => {
            items.iter().all(|item| {
                item.expression
                    .grouping_references()
                    .into_iter()
                    .all(|name| context.fixed.contains(name) && !context.table_columns.contains(name))
            })
        }
        _ => false,
    };
    if returned.group_by.is_empty() && ungrouped {
        Ok(())
    } else {
        Err(GraphPlanError::syntax(
            QueryFailureReason::Return,
            "a VALUE subquery returns at most one row: end it in LIMIT 1, or aggregate without GROUP BY",
        ))
    }
}

/// A subquery's RETURN with every item named: below a result plan a
/// projection must name what it computes, and only whether rows exist is
/// read. Under UNION an item is named so only where every arm of the
/// composite, nested braces included, leaves the same expression unnamed in
/// its place: the arms' columns then agree, as they would as results, and
/// arms that differ are not made to.
fn named_return_items(query: &Query) -> Query {
    fn returns<'a>(query: &'a Query, into: &mut Vec<&'a [Projection]>) {
        if let Some(Clause::Return(returned)) = query.clauses.last() {
            into.push(match &returned.items {
                ProjectionItems::All(items) | ProjectionItems::Items(items) => items,
            });
        }
        for arm in &query.unions {
            returns(&arm.query, into);
        }
    }
    fn name(query: &mut Query, named: &[bool]) {
        if let Some(Clause::Return(returned)) = query.clauses.last_mut() {
            let items = match &mut returned.items {
                ProjectionItems::All(items) | ProjectionItems::Items(items) => items,
            };
            for (index, item) in items.iter_mut().enumerate() {
                if named.get(index).copied().unwrap_or(false) {
                    item.alias = Some(Identifier::from(format!("__hydradb_exists_item_{index}")));
                }
            }
        }
        for arm in &mut query.unions {
            name(&mut arm.query, named);
        }
    }
    let unnamed = |item: &Projection| item.alias.is_none() && !matches!(item.expression, Expression::Identifier(_));
    let mut all = Vec::new();
    returns(query, &mut all);
    let named: Vec<bool> = match all.split_first() {
        None => Vec::new(),
        Some((first, rest)) => first
            .iter()
            .enumerate()
            .map(|(index, item)| {
                unnamed(item)
                    && rest.iter().all(|other| {
                        other
                            .get(index)
                            .is_some_and(|other| unnamed(other) && other.expression == item.expression)
                    })
            })
            .collect(),
    };
    let mut query = query.clone();
    name(&mut query, &named);
    query
}

/// `<expression> IN <list>`.
///
/// A list literal or a parameter that will be bound to a list becomes
/// [`LogicalExpression::In`], whose values are known at planning time and can
/// drive an index seek. A scalar parameter is deliberately still accepted here
/// and resolved as a one-element membership test during physical planning, so
/// a caller that pages an id set down to a single value does not have to
/// rewrite the query. A non-null scalar or map literal is rejected here. Any
/// other right side, such as a stored list property (`'x' IN n.tags`), a `WITH`
/// alias or `null`, becomes the binary `In` operator, evaluated row by row.
fn lower_in_expression(left: &Expression, right: &Expression) -> GraphPlanResult<LogicalExpression> {
    let expression = Box::new(lower_expression(left)?);
    let list = match right {
        Expression::List(elements) => {
            if elements.len() > MAX_IN_LIST_VALUES {
                return Err(GraphPlanError::UnsupportedCypher(
                    QueryFailureReason::Where,
                    format!("IN list of {} values exceeds the {MAX_IN_LIST_VALUES}-value limit", elements.len()),
                ));
            }
            LogicalInList::Values(elements.iter().map(lower_expression).collect::<GraphPlanResult<Vec<_>>>()?)
        }
        Expression::Parameter(parameter) => LogicalInList::Parameter(Symbol::from(parameter.as_str())),
        // A scalar or map literal is never a list, so it is rejected while
        // planning, as Cypher requires (`1 IN true` is a compile-time
        // InvalidArgumentType), rather than at every row. `null` is not
        // rejected: `1 IN null` is null, and it takes the per-row operator
        // below, which returns null for a null list in every form.
        Expression::Literal(literal) if !matches!(literal, Literal::Null) => {
            return Err(GraphPlanError::UnsupportedCypher(
                QueryFailureReason::Where,
                "IN requires a list literal or a list parameter".to_string(),
            ))
        }
        Expression::Map(_) => {
            return Err(GraphPlanError::UnsupportedCypher(
                QueryFailureReason::Where,
                "IN requires a list literal or a list parameter".to_string(),
            ))
        }
        _ => {
            return Ok(LogicalExpression::Binary {
                left: expression,
                operator: LogicalBinaryOperator::In,
                right: Box::new(lower_expression(right)?),
            })
        }
    };
    Ok(LogicalExpression::In { expression, list })
}

fn contains_aggregate(expression: &LogicalExpression) -> bool {
    match expression {
        LogicalExpression::Aggregate { group, .. } => group.is_none(),
        other => other.children().into_iter().any(contains_aggregate),
    }
}

/// A non-aggregating RETURN sort key with each result alias it reads replaced by that
/// item's expression, at any depth: a key reads the result's columns over the
/// input's (§14.10 Syntax Rule 4), so `k + 0` for `x AS k` reads `x + 0`,
/// computed before the projection. Aggregating sorts read materialized aliases.
fn sort_key_through_aliases(expression: &Expression, items: &[Projection]) -> Expression {
    if let Expression::Identifier(name) = expression {
        if let Some(item) = items.iter().find(|item| item.alias.as_ref() == Some(name)) {
            return item.expression.clone();
        }
    }
    expression.map_children(&mut |child| sort_key_through_aliases(child, items))
}

fn lower_window_expression(window: &Expression, field: &'static str) -> GraphPlanResult<LogicalExpression> {
    if is_integer_window_expression(window) {
        lower_expression(window)
    } else if let Expression::Literal(literal) = window {
        // §20.3 General Rules: a null is not allowed, and anything else must
        // be an exact number with scale 0.
        let (status, what) = match literal {
            Literal::Null => (GqlStatus::NULL_VALUE_NOT_ALLOWED, "null"),
            _ => (GqlStatus::INVALID_VALUE_TYPE, "not an integer"),
        };
        Err(GraphPlanError::data(
            status,
            QueryFailureReason::OrderWindow,
            format!("{field} is {what}"),
        ))
    } else {
        Err(GraphPlanError::UnsupportedCypher(
            QueryFailureReason::OrderWindow,
            format!("{field} supports integer literals, parameters, and constant arithmetic"),
        ))
    }
}

fn is_integer_window_expression(expression: &Expression) -> bool {
    match expression {
        Expression::Parameter(_) | Expression::Literal(Literal::Integer(_)) => true,
        Expression::Unary {
            operator: UnaryOperator::Plus | UnaryOperator::Minus,
            expression,
        } => is_integer_window_expression(expression),
        Expression::Binary {
            left,
            operator:
                BinaryOperator::Add | BinaryOperator::Subtract | BinaryOperator::Multiply | BinaryOperator::Divide | BinaryOperator::Modulo,
            right,
        } => is_integer_window_expression(left) && is_integer_window_expression(right),
        _ => false,
    }
}

/// Borrowed readable scope and independent table/record provenance.
#[derive(Clone, Copy)]
struct BindingScope<'a> {
    record: &'a BTreeSet<Symbol>,
    table_columns: &'a BTreeSet<Symbol>,
    visible: &'a BTreeSet<Symbol>,
    bound: &'a BTreeSet<Symbol>,
    values: &'a BTreeSet<Symbol>,
    groups: &'a BTreeSet<Symbol>,
}

/// Close a projection segment, returning its plan, readable names and
/// value-kind names. Identity projections preserve graph-element bindings;
/// computed values must not become graph-pattern anchors.
fn lower_with(
    with: &WithClause,
    input: GraphLogicalPlan,
    context: BindingScope<'_>,
    record_values: &BTreeSet<Symbol>,
    synthesized: &mut BTreeSet<Symbol>,
    operation: operation::Operation,
) -> GraphPlanResult<(GraphLogicalPlan, BTreeSet<Symbol>, BTreeSet<Symbol>)> {
    let BindingScope {
        visible: scope,
        bound,
        values: incoming_values,
        ..
    } = context;
    let items = &star_expanded(&with.items, scope, "WITH")?;

    // An `EXISTS` or `VALUE` subquery in an item is a mark computed over the
    // rows the projection reads, as in a RETURN item (GQ09, GQ18).
    let mut marks = Vec::new();
    let items = items
        .iter()
        .map(|item| {
            Ok(Projection {
                expression: hoist_exists(&item.expression, context, synthesized, &mut marks)?,
                alias: item.alias.clone(),
            })
        })
        .collect::<GraphPlanResult<Vec<_>>>()?;
    let input = apply_marks(input, marks);
    let readable = with_marks(scope, synthesized);

    let mut projected_scope = BTreeSet::new();
    let mut projected_values = BTreeSet::new();
    let mut projections = Vec::with_capacity(items.len());
    for item in &items {
        check_scope(&item.expression, &readable, incoming_values, "WITH")?;
        let name = item
            .output_binding()
            .map(|name| Symbol::from(name.as_str()))
            .ok_or_else(|| GraphPlanError::syntax(QueryFailureReason::Return, "a computed WITH item must be given a name with AS"))?;
        if !projected_scope.insert(name.clone()) {
            return Err(GraphPlanError::syntax(
                QueryFailureReason::Return,
                format!("WITH projects {name} more than once"),
            ));
        }
        // A bare identifier carries through whatever the name already held, so
        // renaming a value leaves it a value: `WITH e.score AS n WITH n AS x`
        // makes `x` a scalar just as surely as `n`. Reading only the expression
        // shape would call `x` a binding and let a later pattern bind it.
        // Anything other than an identifier computes a value outright.
        // An identifier carries an element only when it names one a pattern
        // bound: a hoisted subquery's mark, like a value, is a value.
        let carries_value = match &item.expression {
            Expression::Identifier(binding) => {
                let binding = Symbol::from(binding.as_str());
                incoming_values.contains(&binding) || !bound.contains(&binding)
            }
            _ => true,
        };
        if carries_value {
            projected_values.insert(name.clone());
        }
        projections.push(LogicalProjection {
            expression: lower_expression(&item.expression)?,
            alias: Some(name),
        });
    }

    // A sort key reads the projected names over the input's (see
    // `sort_overlay`), as a RETURN's after an aggregation does.
    projected_values.extend(record_values.iter().filter(|name| !projected_scope.contains(*name)).cloned());
    projected_scope.extend(context.record.iter().cloned());
    let mut post_sort = Vec::with_capacity(with.order_by.len());
    for item in &with.order_by {
        check_scope(&item.expression, &projected_scope, &projected_values, "ORDER BY after WITH")?;
        let (direction, nulls) = sort_order(item);
        post_sort.push(LogicalSort {
            expression: lower_expression(&item.expression)?,
            direction,
            nulls,
        });
    }

    let plan = GraphLogicalPlan::Project {
        context: crate::plan::logical::ProjectionContext {
            fixed: context.record.iter().cloned().collect(),
            table_columns: context.table_columns.iter().cloned().collect(),
            defined: Vec::new(),
            explicit_grouping: !with.group_by.is_empty(),
        },
        operation,
        input: Box::new(input),
        items: projections,
        distinct: with.quantifier.is_some(),
        post_sort,
        post_skip: with.skip.as_ref().map(|skip| lower_window_expression(skip, "SKIP")).transpose()?,
        post_limit: with
            .limit
            .as_ref()
            .map(|limit| lower_window_expression(limit, "LIMIT"))
            .transpose()?,
    };

    let plan = match &with.predicate {
        Some(predicate) => {
            check_scope(predicate, &projected_scope, &projected_values, "WHERE after WITH")?;
            GraphLogicalPlan::Filter {
                operation,
                input: Box::new(plan),
                predicate: lower_expression(predicate)?,
            }
        }
        None => plan,
    };
    Ok((plan, projected_scope, projected_values))
}

/// How an explicit GROUP BY maps onto the grouping lowering already does.
enum ExplicitGrouping {
    /// Every key is a returned item and something aggregates.
    Projected,
    /// Every key is a returned item and nothing aggregates: grouping is then
    /// deduplication of the rows.
    Distinct,
    /// Some keys are not returned. They are projected under reserved names by
    /// an inner WITH, which groups by them, and an outer projection drops them.
    Hidden(Vec<Expression>),
}

/// The prefix hidden GROUP BY keys are projected under; see
/// [`ExplicitGrouping::Hidden`].
const GROUP_KEY_PREFIX: &str = "__hydradb_group_";

/// Classify an explicit GROUP BY against the items it groups.
///
/// A key naming an item's alias stands for that item's expression. A
/// non-aggregate item must be a key or be computed from keys that are bare
/// names, as Neo4j requires: `RETURN x AS k, x * 2 AS d ... GROUP BY k` is
/// grouped, `RETURN x AS k ... GROUP BY x + 1` is refused.
fn explicit_grouping(items: &[Projection], keys: &[Expression], clause: &str) -> GraphPlanResult<ExplicitGrouping> {
    let mut resolved: Vec<Expression> = Vec::with_capacity(keys.len());
    for key in keys {
        // This rewrite runs before value keywords are resolved, so a key
        // spelled like one (`GROUP BY unknown`) names an item of that alias
        // as an identifier would; one that names none is resolved later.
        let named = match key {
            Expression::Identifier(name) => Some(name.clone()),
            Expression::ValueKeyword { spelling, .. } => items
                .iter()
                .filter_map(|item| item.alias.as_ref())
                .find(|alias| *alias == spelling)
                .or_else(|| {
                    items
                        .iter()
                        .filter_map(|item| item.alias.as_ref())
                        .find(|alias| alias.as_str().eq_ignore_ascii_case(spelling.as_str()))
                })
                .cloned(),
            _ => None,
        };
        let key = match &named {
            Some(name) => match items.iter().find(|item| item.alias.as_ref() == Some(name)) {
                // Grouping and aggregating items are disjoint (§14.11 Syntax
                // Rule 10 a) ii), as Corrigendum 1 replaces it).
                Some(item) if ast_contains_aggregate(&item.expression) => {
                    return Err(GraphPlanError::syntax(
                        QueryFailureReason::Return,
                        format!("GROUP BY names {name}, which is an aggregating {clause} item"),
                    ));
                }
                Some(item) => item.expression.clone(),
                None => key.clone(),
            },
            _ => key.clone(),
        };
        if !resolved.contains(&key) {
            resolved.push(key);
        }
    }
    let key_names: BTreeSet<Symbol> = resolved
        .iter()
        .filter_map(|key| match key {
            Expression::Identifier(name) => Some(Symbol::from(name.as_str())),
            _ => None,
        })
        .collect();
    let aggregates = items.iter().any(|item| ast_contains_aggregate(&item.expression));
    for item in items.iter().filter(|item| !ast_contains_aggregate(&item.expression)) {
        if resolved.contains(&item.expression) {
            continue;
        }
        let derived = expression_bindings(&item.expression).is_some_and(|reads| reads.is_subset(&key_names));
        if !derived {
            return Err(GraphPlanError::syntax(
                QueryFailureReason::Return,
                format!("{clause} item is neither a GROUP BY key nor computed from one"),
            ));
        }
    }
    let hidden: Vec<Expression> = resolved
        .into_iter()
        .filter(|key| !items.iter().any(|item| &item.expression == key) || expression_bindings(key).is_some_and(|reads| reads.is_empty()))
        .collect();
    Ok(match (hidden.is_empty(), aggregates) {
        (true, true) => ExplicitGrouping::Projected,
        (true, false) => ExplicitGrouping::Distinct,
        (false, _) => ExplicitGrouping::Hidden(hidden),
    })
}

fn clause_operation(clause: &Clause) -> operation::Operation {
    match clause {
        Clause::Match(_) => operation::MATCH,
        Clause::Filter(_) => operation::FILTER,
        Clause::Let(_) => operation::LET,
        Clause::Unwind(_) => operation::FOR,
        Clause::Call(_) => operation::CALL_PROCEDURE,
        Clause::OrderByAndPage(_) => operation::ORDER_BY_AND_PAGE,
        Clause::With(_) | Clause::Return(_) => operation::RETURN,
        Clause::Create(_) | Clause::Merge(_) => operation::INSERT,
        Clause::Set(_) => operation::SET,
        Clause::Remove(_) => operation::REMOVE,
        Clause::Delete(_) => operation::DELETE,
        Clause::Foreach(_) => operation::UNRECOGNIZED,
        Clause::Finish | Clause::Next | Clause::OptionalBlock(_) | Clause::Syntax(_) => operation::UNRECOGNIZED,
    }
}

/// Only projections and sort keys aggregate (§20.9 SR7, §20.1 SR7).
/// An aggregate cannot contain another aggregate or a subquery (§20.9 SR3–4).
fn check_aggregate_placement(query: &Query) -> GraphPlanResult<()> {
    let nowhere = |expression: &Expression, reason: QueryFailureReason, place: &str| -> GraphPlanResult<()> {
        if ast_contains_aggregate(expression) {
            return Err(GraphPlanError::syntax(reason, format!("an aggregate is not allowed in {place}")));
        }
        Ok(())
    };
    let projected = |items: &ProjectionItems, order_by: &[hydradb_cypher_ast::SortItem]| -> GraphPlanResult<()> {
        let items = match items {
            ProjectionItems::All(items) | ProjectionItems::Items(items) => items,
        };
        for expression in items
            .iter()
            .map(|item| &item.expression)
            .chain(order_by.iter().map(|sort| &sort.expression))
        {
            check_aggregate_arguments(expression)?;
        }
        Ok(())
    };
    for clause in flattened_clauses(&query.clauses) {
        (|| -> GraphPlanResult<()> {
            match clause {
                Clause::Match(matched) => {
                    for predicate in matched
                        .predicate
                        .iter()
                        .chain(matched.patterns.iter().filter_map(|pattern| pattern.predicate.as_ref()))
                    {
                        check_horizontal_aggregate_arguments(predicate)?;
                    }
                    for predicate in operand_predicates(matched) {
                        check_horizontal_aggregate_arguments(predicate)?;
                    }
                    for expression in matched.patterns.iter().flat_map(path_element_expression_roots) {
                        nowhere(expression, QueryFailureReason::Pattern, "a path factor")?;
                    }
                }
                Clause::Unwind(unwind) => nowhere(&unwind.expression, QueryFailureReason::Unwind, "UNWIND")?,
                // LET and FILTER report their own (`lower_let`, `validate_filter`).
                Clause::Let(_) | Clause::Filter(_) => {}
                Clause::With(with) => {
                    projected(&with.items, &with.order_by)?;
                    for expression in with.predicate.iter().chain(&with.skip).chain(&with.limit) {
                        nowhere(expression, QueryFailureReason::Where, "a WHERE, OFFSET or LIMIT after WITH")?;
                    }
                }
                Clause::Return(returned) => {
                    projected(&returned.items, &returned.order_by)?;
                    for expression in returned.skip.iter().chain(&returned.limit) {
                        nowhere(expression, QueryFailureReason::OrderWindow, "OFFSET or LIMIT")?;
                    }
                }
                Clause::OrderByAndPage(page) => {
                    for sort in &page.order_by {
                        nowhere(&sort.expression, QueryFailureReason::OrderWindow, "a standalone ORDER BY")?;
                        check_aggregate_arguments(&sort.expression)?;
                    }
                    for expression in page.skip.iter().chain(&page.limit) {
                        nowhere(expression, QueryFailureReason::OrderWindow, "OFFSET or LIMIT")?;
                    }
                }
                Clause::Create(_) | Clause::Merge(_) | Clause::Set(_) | Clause::Remove(_) | Clause::Delete(_) | Clause::Foreach(_) => {}
                Clause::Finish | Clause::Next | Clause::OptionalBlock(_) | Clause::Call(_) | Clause::Syntax(_) => {}
            }
            Ok(())
        })()
        .map_err(|error| error.in_operation(clause_operation(clause)))?;
    }
    Ok(())
}

/// A query's clauses with every `OPTIONAL { ... }` block opened, in order.
fn flattened_clauses(clauses: &[Clause]) -> Vec<&Clause> {
    let mut flat = Vec::new();
    for clause in clauses {
        flat.push(clause);
        if let Clause::OptionalBlock(inner) = clause {
            flat.extend(flattened_clauses(inner));
        }
    }
    flat
}

/// No aggregate's argument holds another aggregate or a subquery.
fn check_aggregate_arguments(expression: &Expression) -> GraphPlanResult<()> {
    let mut problem = None;
    visit_aggregates(expression, &mut |arguments| {
        if arguments.iter().any(ast_contains_aggregate) {
            problem.get_or_insert("an aggregate's argument contains another aggregate");
        }
        if arguments.iter().any(contains_nested_query) {
            problem.get_or_insert("an aggregate's argument contains a subquery");
        }
    });
    match problem {
        Some(message) => Err(GraphPlanError::syntax(QueryFailureReason::Return, message)),
        None => Ok(()),
    }
}

/// [`check_aggregate_arguments`] for an aggregate over a group variable
/// (GE09), which reads its elements one at a time: a subquery hoisted out of
/// its argument would be computed once, before any element is bound, so one
/// there is refused (deviations L23) rather than read with the whole list.
fn check_horizontal_aggregate_arguments(expression: &Expression) -> GraphPlanResult<()> {
    check_aggregate_arguments(expression)?;
    let mut subquery = false;
    visit_aggregates(expression, &mut |arguments| {
        subquery |= arguments.iter().any(contains_subquery);
    });
    if subquery {
        return Err(GraphPlanError::UnsupportedCypher(
            QueryFailureReason::Where,
            "a subquery inside a horizontal aggregate's argument is not supported".to_string(),
        ));
    }
    Ok(())
}

/// Calls `visit` with the arguments of every aggregate in `expression`.
fn visit_aggregates(expression: &Expression, visit: &mut dyn FnMut(&[Expression])) {
    if let Expression::Function { name, arguments, .. } = expression {
        if matches!(name.as_slice(), [name] if AggregateFunction::from_name(name.as_str()).is_some()) {
            visit(arguments);
        }
    }
    for child in ast_children(expression) {
        visit_aggregates(child, visit);
    }
}

/// Whether an expression holds an `EXISTS { query }`: a nested query
/// specification, whose `<procedure body>` ends in RETURN. `EXISTS` over a
/// graph pattern has none.
fn contains_nested_query(expression: &Expression) -> bool {
    match expression {
        Expression::Exists(query) => matches!(query.clauses.last(), Some(Clause::Return(_))),
        Expression::Value(_) => true,
        other => ast_children(other).into_iter().any(contains_nested_query),
    }
}

/// The direct subexpressions of an expression; a subquery's are its own.
fn ast_children(expression: &Expression) -> Vec<&Expression> {
    expression.children()
}

/// LET values read the enclosing scope; only its body sees the new names.
/// Nested queries have no expression children and own their scope traversal.
fn try_for_each_scoped_child<E>(
    expression: &Expression,
    scope: &BTreeSet<Symbol>,
    mut visit: impl FnMut(&Expression, &BTreeSet<Symbol>) -> Result<(), E>,
) -> Result<(), E> {
    if let Expression::Let { bindings, body } = expression {
        for (_, value) in bindings {
            visit(value, scope)?;
        }
        let mut inner = scope.clone();
        inner.extend(bindings.iter().map(|(name, _)| Symbol::from(name.as_str())));
        visit(body, &inner)
    } else {
        ast_children(expression).into_iter().try_for_each(|child| visit(child, scope))
    }
}

/// A RETURN that neither groups nor aggregates has no group for a sort key to
/// aggregate over (§14.10 Syntax Rule 4) c) i) 2) A) V), as Corrigendum 1
/// replaces it).
fn check_sort_aggregates(query: &Query) -> GraphPlanResult<()> {
    for clause in &query.clauses {
        let Clause::Return(returned) = clause else { continue };
        let items = match &returned.items {
            ProjectionItems::All(items) | ProjectionItems::Items(items) => items,
        };
        let aggregates = items.iter().any(|item| ast_contains_aggregate(&item.expression));
        let grouped = !returned.group_by.is_empty() || aggregates;
        if !grouped && returned.order_by.iter().any(|sort| ast_contains_aggregate(&sort.expression)) {
            return Err(GraphPlanError::syntax(
                QueryFailureReason::OrderWindow,
                "ORDER BY aggregates, but RETURN neither groups nor aggregates",
            ));
        }
        // §14.10 Syntax Rule 4 c) i) 2) A) I): no sort key holds a subquery.
        if returned.order_by.iter().any(|sort| contains_nested_query(&sort.expression)) {
            return Err(GraphPlanError::syntax(
                QueryFailureReason::OrderWindow,
                "an ORDER BY key cannot contain a subquery",
            ));
        }
        // The restriction on what sort keys read after DISTINCT or an
        // aggregation needs `*` expanded, so it is checked where RETURN is
        // lowered (`check_restricted_sort_keys`).
    }
    Ok(())
}

/// §14.10 Syntax Rule 4 c) i) 2) A) III) and IV): after DISTINCT or an
/// aggregation, a sort key reads only return items (and GROUP BY keys),
/// outside its own aggregates. `items` are the RETURN's items with `*`
/// expanded. A key that repeats a projected expression is also kept, as Cypher
/// reads it.
fn check_restricted_sort_keys(returned: &ReturnClause, items: &[Projection]) -> GraphPlanResult<()> {
    let aggregates = items.iter().any(|item| ast_contains_aggregate(&item.expression));
    let grouped = !returned.group_by.is_empty() || aggregates;
    let distinct = matches!(returned.quantifier, Some(SetQuantifier::Distinct));
    if !(distinct || grouped) {
        return Ok(());
    }
    let mut readable: BTreeSet<&str> = items
        .iter()
        .filter_map(|item| match (&item.alias, &item.expression) {
            (Some(name), _) | (None, Expression::Identifier(name)) => Some(name.as_str()),
            _ => None,
        })
        .collect();
    for key in &returned.group_by {
        let mut names = Vec::new();
        references_outside_aggregates(key, &mut names);
        readable.extend(names.into_iter().map(Identifier::as_str));
    }
    for sort in &returned.order_by {
        if items.iter().any(|item| item.expression == sort.expression) {
            continue;
        }
        let mut names = Vec::new();
        references_outside_aggregates(&sort.expression, &mut names);
        if let Some(name) = names.into_iter().find(|name| !readable.contains(name.as_str())) {
            return Err(GraphPlanError::syntax(
                QueryFailureReason::OrderWindow,
                format!("after DISTINCT or an aggregation, ORDER BY can read only what RETURN returns, not {name}"),
            ));
        }
    }
    Ok(())
}

/// The variables an expression reads, except inside an aggregate.
fn references_outside_aggregates<'a>(expression: &'a Expression, names: &mut Vec<&'a Identifier>) {
    match expression {
        Expression::Identifier(name) => names.push(name),
        Expression::Function { name, .. } if matches!(name.as_slice(), [name] if AggregateFunction::from_name(name.as_str()).is_some()) => {
        }
        // The body's own names are not the row's.
        Expression::Let { bindings, body } => {
            for (_, value) in bindings {
                references_outside_aggregates(value, names);
            }
            let mut inner = Vec::new();
            references_outside_aggregates(body, &mut inner);
            names.extend(inner.into_iter().filter(|name| bindings.iter().all(|(own, _)| own != *name)));
        }
        other => {
            for child in ast_children(other) {
                references_outside_aggregates(child, names);
            }
        }
    }
}

/// Bind computed output columns used in variable-only element operands.
/// The original sort expressions remain available for scope checking;
/// these bindings never expose additional input names to ORDER BY.
fn bind_sort_element_columns(
    items: &[Projection],
    sort: &[hydradb_cypher_ast::SortItem],
    synthesized: &mut BTreeSet<Symbol>,
) -> (Vec<Projection>, Vec<Projection>) {
    let mut operands = Vec::new();
    for item in sort {
        element_operands(&item.expression, &mut operands);
    }
    let mut view = items.to_vec();
    let mut bindings = Vec::new();
    for item in &mut view {
        let tested = item.alias.as_ref().is_some_and(|alias| operands.contains(&alias));
        if tested && !matches!(item.expression, Expression::Identifier(_)) {
            let name = (0..)
                .map(|index| Symbol::from(format!("__hydradb_sort_element_{index}").as_str()))
                .find(|name| !synthesized.contains(name))
                .expect("a fresh name");
            synthesized.insert(name.clone());
            let name = Identifier::from(name.as_str());
            bindings.push(Projection {
                expression: std::mem::replace(&mut item.expression, Expression::Identifier(name.clone())),
                alias: Some(name),
            });
        }
    }
    (view, bindings)
}

/// The element variables read in this expression's scope, not nested queries.
fn element_operands<'a>(expression: &'a Expression, into: &mut Vec<&'a Identifier>) {
    if let Expression::ElementTest { element, test, .. } = expression {
        let edge = match test {
            ElementTest::SourceOf(edge) | ElementTest::DestinationOf(edge) => Some(edge.as_ref()),
            _ => None,
        };
        for operand in std::iter::once(element.as_ref()).chain(edge) {
            if let Expression::Identifier(name) = operand {
                into.push(name);
            }
        }
    }
    if let Expression::Function { name, arguments, .. } = expression {
        if let [name] = name.as_slice() {
            if let Some(function) = ScalarFunction::from_name(name.as_str()) {
                for (index, argument) in arguments.iter().enumerate() {
                    if function.argument_role(index) == ScalarArgumentRole::ElementVariable {
                        if let Expression::Identifier(name) = argument {
                            into.push(name);
                        }
                    }
                }
            }
        }
    }
    for child in ast_children(expression) {
        element_operands(child, into);
    }
}

/// The variable an element test reads (§19.8 to §19.10: an element
/// variable reference). The parser builds only these; a rewrite that put an
/// expression in its place has no element to test.
fn element_variable(expression: &Expression) -> GraphPlanResult<Symbol> {
    match expression {
        Expression::Identifier(name) => Ok(Symbol::from(name.as_str())),
        _ => Err(GraphPlanError::syntax(
            QueryFailureReason::Where,
            "an element predicate tests an element variable, not an expression".to_string(),
        )),
    }
}

fn ast_contains_aggregate(expression: &Expression) -> bool {
    !matches!(expression, Expression::ElementTest { .. })
        && (matches!(expression, Expression::Function { name, .. } if matches!(name.as_slice(), [name] if AggregateFunction::from_name(name.as_str()).is_some()))
            || expression.children().into_iter().any(ast_contains_aggregate))
}

/// The inner and outer item lists for hidden keys: the inner projects every
/// key and every item under a name, the outer keeps only the items' names.
struct HiddenGroupItems {
    input: Vec<Projection>,
    inner: Vec<Projection>,
    outer: Vec<Projection>,
    replacements: Vec<(Expression, Identifier)>,
}

fn retain_sort_aggregates(expression: &Expression, inner: &mut Vec<Projection>, used: &mut BTreeSet<Symbol>) -> Expression {
    if matches!(expression, Expression::Function { name, .. } if matches!(name.as_slice(), [name] if AggregateFunction::from_name(name.as_str()).is_some()))
    {
        let name = match inner
            .iter()
            .find(|item| item.expression == *expression)
            .and_then(Projection::output_binding)
        {
            Some(name) => name.clone(),
            None => {
                let mut index = 0;
                let name = loop {
                    let candidate = Symbol::from(format!("__hydradb_sort_aggregate_{index}").as_str());
                    if used.insert(candidate.clone()) {
                        break Identifier::from(candidate.as_str());
                    }
                    index += 1;
                };
                inner.push(Projection {
                    expression: expression.clone(),
                    alias: Some(name.clone()),
                });
                name
            }
        };
        return Expression::Identifier(name);
    }
    expression.map_children(&mut |child| retain_sort_aggregates(child, inner, used))
}

fn hidden_key_items(
    items: &[Projection],
    hidden: Vec<Expression>,
    clause: &str,
    used: &mut BTreeSet<Symbol>,
) -> GraphPlanResult<HiddenGroupItems> {
    let mut input = Vec::new();
    let mut inner = Vec::new();
    let mut replacements = Vec::new();
    for key in hidden {
        let mut index = 0;
        let name = loop {
            let candidate = Symbol::from(format!("{GROUP_KEY_PREFIX}{index}").as_str());
            if used.insert(candidate.clone()) {
                break Identifier::from(candidate.as_str());
            }
            index += 1;
        };
        // Bind each key before aggregation. Even a constant is then an actual
        // input-row key, so empty input forms zero groups, not a global group.
        input.push(Projection {
            expression: key.clone(),
            alias: Some(name.clone()),
        });
        inner.push(Projection {
            expression: Expression::Identifier(name.clone()),
            alias: Some(name.clone()),
        });
        replacements.push((key, name));
    }
    let mut outer = Vec::with_capacity(items.len());
    for item in items {
        let name = match (&item.alias, &item.expression) {
            (Some(alias), _) => alias.clone(),
            (None, Expression::Identifier(name)) => name.clone(),
            (None, _) => {
                return Err(GraphPlanError::syntax(
                    QueryFailureReason::Return,
                    format!("a computed {clause} item must be named with AS when grouping by a key it does not return"),
                ));
            }
        };
        inner.push(Projection {
            expression: item.expression.clone(),
            alias: Some(name.clone()),
        });
        outer.push(Projection {
            expression: Expression::Identifier(name.clone()),
            alias: Some(name),
        });
    }
    Ok(HiddenGroupItems {
        input,
        inner,
        outer,
        replacements,
    })
}

// ORDER BY can still read hidden keys after grouping. Rewrite their entire
// expression (including compound sort expressions) to the retained binding.
fn replace_group_keys(expression: &Expression, replacements: &[(Expression, Identifier)]) -> Expression {
    if let Some((_, name)) = replacements.iter().find(|(key, _)| key == expression) {
        return Expression::Identifier(name.clone());
    }
    expression.map_children(&mut |child| replace_group_keys(child, replacements))
}

fn group_input(input: Vec<Projection>) -> Clause {
    Clause::With(WithClause {
        table_only: false,
        quantifier: None,
        items: ProjectionItems::All(input),
        group_by: Vec::new(),
        order_by: Vec::new(),
        skip: None,
        limit: None,
        predicate: None,
    })
}

fn reserved_query_names(query: &Query) -> BTreeSet<Symbol> {
    let mut names = BTreeSet::new();
    for clause in flattened_clauses(&query.clauses) {
        let expressions: Vec<&Expression> = match clause {
            Clause::With(with) => with
                .group_by
                .iter()
                .chain(with.order_by.iter().map(|s| &s.expression))
                .chain(with.predicate.iter())
                .collect(),
            Clause::Return(returned) => returned
                .group_by
                .iter()
                .chain(returned.order_by.iter().map(|s| &s.expression))
                .collect(),
            Clause::Unwind(unwind) => vec![&unwind.expression],
            Clause::OrderByAndPage(page) => page
                .order_by
                .iter()
                .map(|item| &item.expression)
                .chain(&page.skip)
                .chain(&page.limit)
                .collect(),
            Clause::Let(let_clause) => let_clause.bindings.iter().map(|(_, expression)| expression).collect(),
            Clause::Filter(filter) => vec![&filter.predicate],
            Clause::Match(matched) => matched
                .predicate
                .iter()
                .chain(matched.patterns.iter().flat_map(path_expression_roots))
                .collect(),
            Clause::Create(_) | Clause::Merge(_) | Clause::Set(_) | Clause::Remove(_) | Clause::Delete(_) | Clause::Foreach(_) => {
                Vec::new()
            }
            Clause::Finish | Clause::Next | Clause::OptionalBlock(_) | Clause::Call(_) | Clause::Syntax(_) => Vec::new(),
        };
        for expression in expressions {
            names.extend(expression_mentions(expression));
        }
        let items = match clause {
            Clause::Match(matched) => {
                names.extend(clause_bindings(matched));
                names.extend(clause_path_names(matched));
                continue;
            }
            Clause::Unwind(unwind) => {
                names.insert(Symbol::from(unwind.variable.as_str()));
                // A FOR's position is declared beside its element.
                names.extend(unwind.position.iter().map(|position| Symbol::from(position.variable.as_str())));
                continue;
            }
            Clause::Let(let_clause) => {
                names.extend(let_clause.bindings.iter().map(|(name, _)| Symbol::from(name.as_str())));
                continue;
            }
            Clause::Filter(_) | Clause::OrderByAndPage(_) => continue,
            Clause::With(with) => &with.items,
            Clause::Return(returned) => &returned.items,
            Clause::Call(call) => {
                names.extend(reserved_query_names(&call.query));
                continue;
            }
            Clause::Create(_) | Clause::Merge(_) | Clause::Set(_) | Clause::Remove(_) | Clause::Delete(_) | Clause::Foreach(_) => continue,
            Clause::Finish | Clause::Next | Clause::OptionalBlock(_) | Clause::Syntax(_) => continue,
        };
        let (ProjectionItems::Items(items) | ProjectionItems::All(items)) = items;
        for item in items {
            if let Some(alias) = &item.alias {
                names.insert(Symbol::from(alias.as_str()));
            }
            names.extend(expression_mentions(&item.expression));
        }
    }
    for arm in &query.unions {
        names.extend(reserved_query_names(&arm.query));
    }
    names
}

/// Every name `expression` mentions, inside its subqueries too, where
/// `expression_bindings` gives up at a subquery: names to keep clear of, not
/// what it reads (a subquery's own aliases are mentioned, not read; see
/// [`expression_reads`]).
fn expression_mentions(expression: &Expression) -> BTreeSet<Symbol> {
    fn collect(expression: &Expression, into: &mut BTreeSet<Symbol>) {
        match expression {
            Expression::Identifier(identifier) => {
                into.insert(Symbol::from(identifier.as_str()));
            }
            Expression::Exists(query) | Expression::Value(query) => into.extend(reserved_query_names(query)),
            other => {
                for child in ast_children(other) {
                    collect(child, into);
                }
            }
        }
    }
    let mut names = BTreeSet::new();
    collect(expression, &mut names);
    names
}

/// The names of `visible` that `expression` reads, through its subqueries
/// too: a subquery reads an enclosing name it refers to without declaring
/// it first (§4.11.5's non-local references).
fn expression_reads(expression: &Expression, visible: &BTreeSet<Symbol>) -> BTreeSet<Symbol> {
    let mut reads = BTreeSet::new();
    collect_reads(expression, visible, &BTreeSet::new(), &mut reads);
    reads
}

/// [`expression_reads`], with `local` the names the enclosing query
/// declared itself, which shadow `visible`.
fn collect_reads(expression: &Expression, visible: &BTreeSet<Symbol>, local: &BTreeSet<Symbol>, reads: &mut BTreeSet<Symbol>) {
    match expression {
        Expression::Identifier(name) => {
            let name = Symbol::from(name.as_str());
            if !local.contains(&name) && visible.contains(&name) {
                reads.insert(name);
            }
        }
        Expression::Exists(query) | Expression::Value(query) => {
            let outer = visible.difference(local).cloned().collect::<BTreeSet<_>>();
            reads.extend(query_free_references(query, &outer));
        }
        // A keyword is the variable of its name where one is in scope, as
        // `resolve_value_keywords` reads it once this position is lowered.
        Expression::ValueKeyword { spelling, .. } => {
            let scope = visible.union(local).cloned().collect::<BTreeSet<_>>();
            if let Some(name) = keyword_variable(spelling.as_str(), &scope) {
                if !local.contains(name) {
                    reads.insert(name.clone());
                }
            }
        }
        other => {
            let _: Result<(), std::convert::Infallible> = try_for_each_scoped_child(other, local, |child, local| {
                collect_reads(child, visible, local, reads);
                Ok(())
            });
        }
    }
}

/// The names of `outer` that `query` reads: each clause in turn, with the
/// names the query has declared so far its own. A pattern variable the
/// query has not declared is a reference to the enclosing one of that name;
/// a projection closes the scope, leaving the names it projects.
fn query_free_references(query: &Query, outer: &BTreeSet<Symbol>) -> BTreeSet<Symbol> {
    let enclosing = outer;
    let mut reads = BTreeSet::new();
    let mut local = BTreeSet::new();
    let mut outer = enclosing.clone();
    for clause in flattened_clauses(&query.clauses) {
        match clause {
            Clause::Match(matched) => {
                let declared = clause_bindings(matched).into_iter().chain(clause_path_names(matched));
                for name in declared {
                    if local.contains(&name) {
                        continue;
                    }
                    if outer.contains(&name) {
                        reads.insert(name);
                    } else {
                        local.insert(name);
                    }
                }
                // Union operands' elements and WHERE clauses too.
                let expressions = matched
                    .predicate
                    .iter()
                    .chain(matched.patterns.iter().flat_map(path_expression_roots));
                for expression in expressions {
                    collect_reads(expression, &outer, &local, &mut reads);
                }
            }
            Clause::Unwind(unwind) => {
                collect_reads(&unwind.expression, &outer, &local, &mut reads);
                local.insert(Symbol::from(unwind.variable.as_str()));
            }
            Clause::Let(binding) => {
                for (name, value) in &binding.bindings {
                    collect_reads(value, &outer, &local, &mut reads);
                    local.insert(Symbol::from(name.as_str()));
                }
            }
            Clause::Filter(filter) => collect_reads(&filter.predicate, &outer, &local, &mut reads),
            Clause::OrderByAndPage(ordered) => {
                for expression in ordered
                    .order_by
                    .iter()
                    .map(|sort| &sort.expression)
                    .chain(&ordered.skip)
                    .chain(&ordered.limit)
                {
                    collect_reads(expression, &outer, &local, &mut reads);
                }
            }
            // As `lower_with` scopes them: the items and grouping keys read the
            // incoming names, the WHERE and sort keys only what it projects.
            Clause::With(with) => {
                let (ProjectionItems::All(items) | ProjectionItems::Items(items)) = &with.items;
                for expression in items.iter().map(|item| &item.expression).chain(&with.group_by) {
                    collect_reads(expression, &outer, &local, &mut reads);
                }
                project_scope(&with.items, &mut local, &mut outer);
                for expression in with
                    .order_by
                    .iter()
                    .map(|sort| &sort.expression)
                    .chain(&with.predicate)
                    .chain(&with.skip)
                    .chain(&with.limit)
                {
                    collect_reads(expression, &outer, &local, &mut reads);
                }
            }
            // A RETURN's sort keys read its items' names first, then the
            // incoming ones (§14.10 Syntax Rule 4).
            Clause::Return(returned) => {
                let (ProjectionItems::All(items) | ProjectionItems::Items(items)) = &returned.items;
                for expression in items.iter().map(|item| &item.expression).chain(&returned.group_by) {
                    collect_reads(expression, &outer, &local, &mut reads);
                }
                let sorted = local
                    .iter()
                    .cloned()
                    .chain(projected_names(&returned.items))
                    .collect::<BTreeSet<_>>();
                for expression in returned
                    .order_by
                    .iter()
                    .map(|sort| &sort.expression)
                    .chain(&returned.skip)
                    .chain(&returned.limit)
                {
                    collect_reads(expression, &outer, &sorted, &mut reads);
                }
                // A statement after NEXT reads the RETURN's columns.
                project_scope(&returned.items, &mut local, &mut outer);
            }
            // §15.2: the body sees the names its scope clause imports, or every
            // incoming one without it (Syntax Rule 3); what it returns joins
            // the scope.
            Clause::Call(call) => {
                let (inner_local, inner_outer): (BTreeSet<Symbol>, BTreeSet<Symbol>) = match &call.scope {
                    None => (local.clone(), outer.clone()),
                    Some(imported) => {
                        let imported = imported.iter().map(|name| Symbol::from(name.as_str())).collect::<BTreeSet<_>>();
                        for name in &imported {
                            if !local.contains(name) && outer.contains(name) {
                                reads.insert(name.clone());
                            }
                        }
                        (
                            local.intersection(&imported).cloned().collect(),
                            outer.intersection(&imported).cloned().collect(),
                        )
                    }
                };
                let candidates = inner_outer.difference(&inner_local).cloned().collect::<BTreeSet<_>>();
                reads.extend(query_free_references(&call.query, &candidates));
                local.extend(returned_names(&call.query).into_iter().flatten().map(Symbol::from));
            }
            Clause::Create(_) | Clause::Merge(_) | Clause::Set(_) | Clause::Remove(_) | Clause::Delete(_) | Clause::Foreach(_) => {}
            Clause::Finish | Clause::Next | Clause::OptionalBlock(_) | Clause::Syntax(_) => {}
        }
    }
    for arm in &query.unions {
        reads.extend(query_free_references(&arm.query, enclosing));
    }
    reads
}

/// The names a projection's items give: an alias, or a bare variable's own.
fn projected_names(items: &ProjectionItems) -> BTreeSet<Symbol> {
    let (ProjectionItems::All(listed) | ProjectionItems::Items(listed)) = items;
    listed
        .iter()
        .filter_map(|item| match (&item.alias, &item.expression) {
            (Some(name), _) | (None, Expression::Identifier(name)) => Some(Symbol::from(name.as_str())),
            _ => None,
        })
        .collect()
}

/// The scope after a projection, for [`query_free_references`]: the names it
/// projects are the query's own. `*` carries every visible name through, so
/// the enclosing names stay readable, but where an item's alias shadows one;
/// otherwise the projection closes the enclosing scope.
fn project_scope(items: &ProjectionItems, local: &mut BTreeSet<Symbol>, outer: &mut BTreeSet<Symbol>) {
    let projected = projected_names(items);
    if matches!(items, ProjectionItems::All(_)) {
        outer.retain(|name| !projected.contains(name));
        local.extend(projected);
    } else {
        *local = projected;
        outer.clear();
    }
}

/// The inner WITH of a hidden-key rewrite: projection and grouping only.
fn grouping_with(items: Vec<Projection>, aggregates: bool, explicit: bool) -> Clause {
    let group_by = if explicit && aggregates {
        items
            .iter()
            .filter(|item| !ast_contains_aggregate(&item.expression))
            .filter(|item| !expression_bindings(&item.expression).is_some_and(|reads| reads.is_empty()))
            .filter_map(Projection::output_binding)
            .cloned()
            .map(Expression::Identifier)
            .collect()
    } else {
        Vec::new()
    };
    Clause::With(WithClause {
        table_only: false,
        // With nothing aggregating, the groups are the distinct rows.
        quantifier: (!aggregates).then_some(SetQuantifier::Distinct),
        items: ProjectionItems::Items(items),
        group_by,
        order_by: Vec::new(),
        skip: None,
        limit: None,
        predicate: None,
    })
}

/// Normalize explicit keys onto projected items, retaining GROUP BY provenance.
/// Unions keep their arms; each arm is normalized when it is lowered.
fn normalize_explicit_grouping(query: &Query, reserved: &BTreeSet<Symbol>) -> GraphPlanResult<Option<Query>> {
    let grouped = query.clauses.iter().enumerate().any(|(index, clause)| match clause {
        Clause::With(with) => !with.group_by.is_empty(),
        Clause::Return(returned) => !returned.group_by.is_empty() && !matches!(query.clauses.get(index + 1), Some(Clause::Next)),
        _ => false,
    });
    if !grouped {
        return Ok(None);
    }
    // `*` with GROUP BY breaks §14.11 Syntax Rule 6 b).
    let star_refused = |clause: &str| GraphPlanError::syntax(QueryFailureReason::Return, format!("{clause} * cannot have a GROUP BY"));
    let mut used = reserved_query_names(query);
    used.extend(reserved.iter().cloned());
    let mut clauses = Vec::with_capacity(query.clauses.len() + 1);
    for (index, clause) in query.clauses.iter().enumerate() {
        match clause {
            Clause::With(with) if !with.group_by.is_empty() => {
                let ProjectionItems::Items(items) = &with.items else {
                    return Err(star_refused("WITH"));
                };
                match explicit_grouping(items, &with.group_by, "WITH")? {
                    ExplicitGrouping::Projected => clauses.push(Clause::With(WithClause {
                        order_by: keys_read_as_items(&with.order_by, items, &with.group_by),
                        ..with.clone()
                    })),
                    ExplicitGrouping::Distinct => clauses.push(Clause::With(WithClause {
                        quantifier: Some(SetQuantifier::Distinct),
                        group_by: Vec::new(),
                        order_by: keys_read_as_items(&with.order_by, items, &with.group_by),
                        ..with.clone()
                    })),
                    ExplicitGrouping::Hidden(hidden) => {
                        let mut hidden = hidden_key_items(items, hidden, "WITH", &mut used)?;
                        let mut with = with.clone();
                        for sort in &mut with.order_by {
                            sort.expression = retain_sort_aggregates(&sort.expression, &mut hidden.inner, &mut used);
                        }
                        let aggregates = hidden.inner.iter().any(|item| ast_contains_aggregate(&item.expression));
                        clauses.push(group_input(hidden.input));
                        let retained: Vec<Identifier> = hidden
                            .inner
                            .iter()
                            .filter_map(Projection::output_binding)
                            .filter(|name| !hidden.outer.iter().any(|item| item.output_binding() == Some(*name)))
                            .cloned()
                            .collect();
                        clauses.push(grouping_with(hidden.inner, aggregates, true));
                        let mut projected = hidden.outer.clone();
                        for sort in &mut with.order_by {
                            sort.expression = replace_group_keys(&sort.expression, &unshadowed(hidden.replacements.clone(), items));
                        }
                        // A WITH sort sees projected names. Carry hidden keys
                        // through that boundary, then remove them before the next
                        // caller clause. Do not widen DISTINCT's equality key.
                        let reads_hidden = with.order_by.iter().any(|sort| {
                            expression_bindings(&sort.expression)
                                .is_some_and(|reads| retained.iter().any(|name| reads.contains(&Symbol::from(name.as_str()))))
                        });
                        if reads_hidden && with.quantifier.is_some() {
                            return Err(GraphPlanError::UnsupportedCypher(
                                QueryFailureReason::Return,
                                "DISTINCT WITH cannot order by a key it does not project".into(),
                            ));
                        }
                        if reads_hidden {
                            projected.extend(retained.iter().map(|name| Projection {
                                expression: Expression::Identifier(name.clone()),
                                alias: Some(name.clone()),
                            }));
                        }
                        clauses.push(Clause::With(WithClause {
                            items: ProjectionItems::Items(projected),
                            group_by: Vec::new(),
                            ..with
                        }));
                        if reads_hidden {
                            clauses.push(grouping_with(hidden.outer, true, false));
                        }
                    }
                }
            }
            // NEXT must first resolve and name the result columns in their
            // input scope; its resulting WITHs normalize grouping afterward.
            Clause::Return(returned) if !returned.group_by.is_empty() && !matches!(query.clauses.get(index + 1), Some(Clause::Next)) => {
                let ProjectionItems::Items(items) = &returned.items else {
                    return Err(star_refused("RETURN"));
                };
                match explicit_grouping(items, &returned.group_by, "RETURN")? {
                    ExplicitGrouping::Projected => clauses.push(Clause::Return(ReturnClause {
                        order_by: keys_read_as_items(&returned.order_by, items, &returned.group_by),
                        ..returned.clone()
                    })),
                    ExplicitGrouping::Distinct => clauses.push(Clause::Return(ReturnClause {
                        quantifier: Some(SetQuantifier::Distinct),
                        group_by: Vec::new(),
                        order_by: keys_read_as_items(&returned.order_by, items, &returned.group_by),
                        ..returned.clone()
                    })),
                    ExplicitGrouping::Hidden(hidden) => {
                        let mut hidden = hidden_key_items(items, hidden, "RETURN", &mut used)?;
                        let mut returned = returned.clone();
                        for sort in &mut returned.order_by {
                            sort.expression = retain_sort_aggregates(&sort.expression, &mut hidden.inner, &mut used);
                        }
                        let aggregates = hidden.inner.iter().any(|item| ast_contains_aggregate(&item.expression));
                        clauses.push(group_input(hidden.input));
                        clauses.push(grouping_with(hidden.inner, aggregates, true));
                        let outer = hidden.outer;
                        for sort in &mut returned.order_by {
                            sort.expression = replace_group_keys(&sort.expression, &unshadowed(hidden.replacements.clone(), items));
                        }
                        clauses.push(Clause::Return(ReturnClause {
                            items: ProjectionItems::Items(outer),
                            group_by: Vec::new(),
                            ..returned.clone()
                        }));
                    }
                }
            }
            clause => clauses.push(clause.clone()),
        }
    }
    if clauses == query.clauses {
        return Ok(None);
    }
    Ok(Some(Query {
        clauses,
        unions: query.unions.clone(),
    }))
}

/// `order_by` reading each GROUP BY key through the returned item that
/// carries it: once the keys are dropped for implicit grouping, a key such as
/// `x` in `x + 0` is still a grouping column (§14.10 Syntax Rule 4), now named
/// by its item.
fn keys_read_as_items(
    order_by: &[hydradb_cypher_ast::SortItem],
    items: &[Projection],
    group_by: &[Expression],
) -> Vec<hydradb_cypher_ast::SortItem> {
    let replacements: Vec<(Expression, Identifier)> = group_by
        .iter()
        .filter_map(|key| {
            items
                .iter()
                .find(|item| item.expression == *key)
                .and_then(|item| match (&item.alias, &item.expression) {
                    (Some(name), _) | (None, Expression::Identifier(name)) => Some((key.clone(), name.clone())),
                    (None, _) => None,
                })
        })
        .collect();
    let replacements = unshadowed(replacements, items);
    order_by
        .iter()
        .map(|sort| hydradb_cypher_ast::SortItem {
            expression: replace_group_keys(&sort.expression, &replacements),
            ..sort.clone()
        })
        .collect()
}

/// `replacements` without those whose expression reads one of `items`'
/// output names: in a sort key an output name means the output column
/// (§14.10 Syntax Rule 4), so a key such as `x` there is not the input `x`
/// another item or a GROUP BY key reads.
fn unshadowed(replacements: Vec<(Expression, Identifier)>, items: &[Projection]) -> Vec<(Expression, Identifier)> {
    let outputs: BTreeSet<Symbol> = items
        .iter()
        .filter_map(|item| match (&item.alias, &item.expression) {
            (Some(name), _) | (None, Expression::Identifier(name)) => Some(Symbol::from(name.as_str())),
            (None, _) => None,
        })
        .collect();
    replacements
        .into_iter()
        .filter(|(expression, name)| {
            !expression_bindings(expression)
                .unwrap_or_default()
                .iter()
                .any(|read| outputs.contains(read) && read.as_str() != name.as_str())
        })
        .collect()
}

/// `LET a = x, b = y` as `WITH *, x AS a, y AS b`.
///
/// Each binding reads only what was in scope before the clause -- in Neo4j
/// `LET a = 1, b = a + 1` refuses `a` -- which is exactly what the WITH
/// checks. Rebinding a name in scope is refused there too, and an aggregate
/// is refused here, as Neo4j refuses it: LET does not group.
fn let_as_with(binding: &hydradb_cypher_ast::LetClause) -> GraphPlanResult<WithClause> {
    let items = binding
        .bindings
        .iter()
        .map(|(name, expression)| {
            if ast_contains_aggregate(expression) {
                return Err(GraphPlanError::syntax(
                    QueryFailureReason::Return,
                    "LET cannot aggregate".to_string(),
                ));
            }
            Ok(Projection {
                expression: expression.clone(),
                alias: Some(name.clone()),
            })
        })
        .collect::<GraphPlanResult<Vec<_>>>()?;
    Ok(passing_with(ProjectionItems::All(items), None))
}

/// FILTER keeps the same scope, and aggregates only horizontally (GE09).
fn validate_filter(filter: &hydradb_cypher_ast::FilterClause) -> GraphPlanResult<()> {
    check_horizontal_aggregate_arguments(&filter.predicate)
}

/// The group variables a path pattern declares (§16.7 Syntax Rule 22): a
/// quantified edge's, and a quantified path's nodes.
fn group_variables(path: &PathPattern) -> BTreeSet<Symbol> {
    path.all_elements()
        .into_iter()
        .filter_map(|element| match element {
            PatternElement::Relationship(relationship) if relationship.length.is_some() => relationship.variable.as_ref(),
            _ => None,
        })
        .chain(&iteration_names(path))
        .map(|name| Symbol::from(name.as_str()))
        .collect()
}

/// §20.1 Syntax Rule 7: an aggregate outside a projection's aggregating
/// value expression aggregates within the record, over the elements of the
/// one group variable its argument reads (GE09), and may read them through
/// any expression (GQ17).
fn horizontal_aggregates(mut expression: LogicalExpression, groups: &BTreeSet<Symbol>) -> GraphPlanResult<LogicalExpression> {
    visit_aggregates_mut(&mut expression, &mut |aggregate| {
        let LogicalExpression::Aggregate { expression, group, .. } = aggregate else {
            return Ok(());
        };
        let mut read = BTreeSet::new();
        if let Some(expression) = expression {
            logical_bindings(expression, &mut read);
        }
        let mut read = read.into_iter().filter(|name| groups.contains(name));
        match (read.next(), read.next()) {
            (Some(name), None) => {
                *group = Some(name);
                Ok(())
            }
            _ => Err(GraphPlanError::syntax(
                QueryFailureReason::Where,
                "an aggregate outside RETURN or WITH reads exactly one group variable",
            )),
        }
    })?;
    Ok(expression)
}

/// The names a logical expression reads.
fn logical_bindings(expression: &LogicalExpression, into: &mut BTreeSet<Symbol>) {
    into.extend(expression.binding_references().into_iter().cloned());
}

/// Calls `visit` with every aggregate in `expression`, outermost first.
fn visit_aggregates_mut(
    expression: &mut LogicalExpression,
    visit: &mut dyn FnMut(&mut LogicalExpression) -> GraphPlanResult<()>,
) -> GraphPlanResult<()> {
    if matches!(expression, LogicalExpression::Aggregate { .. }) {
        return visit(expression);
    }
    expression
        .children_mut()
        .into_iter()
        .try_for_each(|child| visit_aggregates_mut(child, visit))
}

fn passing_with(items: ProjectionItems, predicate: Option<Expression>) -> WithClause {
    WithClause {
        table_only: false,
        quantifier: None,
        items,
        group_by: Vec::new(),
        order_by: Vec::new(),
        skip: None,
        limit: None,
        predicate,
    }
}

/// `visible` without the lowering's names for NEXT's unnamed columns, which
/// no clause of the query reads and `*` never expands.
fn projection_columns(
    visible: &BTreeSet<Symbol>,
    table_columns: &BTreeSet<Symbol>,
    outer: &OuterScope,
    table_only: bool,
) -> BTreeSet<Symbol> {
    let names = if table_only { table_columns } else { visible };
    names
        .intersection(visible)
        .filter(|name| !outer.next_columns.contains(*name))
        .cloned()
        .collect()
}

/// A projection list with `*` expanded in scope-name order, then its extras.
/// The scope excludes synthesized names. An empty RETURN star is refused;
/// an empty WITH star retains the row binding nothing.
fn star_expanded(items: &ProjectionItems, scope: &BTreeSet<Symbol>, clause: &str) -> GraphPlanResult<Vec<Projection>> {
    if !items.includes_all() {
        return Ok(items.items().to_vec());
    }
    let extra = items.items();
    let mut expanded: Vec<Projection> = scope
        .iter()
        .map(|name| Projection {
            expression: Expression::Identifier(Identifier::from(name.as_str())),
            alias: None,
        })
        .collect();
    if expanded.is_empty() && clause == "RETURN" {
        return Err(GraphPlanError::syntax(
            QueryFailureReason::Return,
            "RETURN * is not allowed when there are no variables in scope".to_string(),
        ));
    }
    expanded.extend(extra.iter().cloned());
    Ok(expanded)
}

/// Whether `expression` is `count(*)`, whose `*` is carried as an unparsed
/// fragment but reads no binding at all.
fn is_count_star(expression: &Expression) -> bool {
    matches!(
        expression,
        Expression::Function { name, arguments, .. }
            if matches!(name.as_slice(), [function] if function.as_str().eq_ignore_ascii_case("count"))
                && matches!(arguments.as_slice(), [Expression::Syntax(fragment)] if fragment.text() == "*")
    )
}

fn check_scope(expression: &Expression, scope: &BTreeSet<Symbol>, values: &BTreeSet<Symbol>, clause: &str) -> GraphPlanResult<()> {
    // `count(*)` is the one fragment whose reads are known: none. It is a row
    // counter, not a reference, and refusing it would take `count(*)` away
    // from every query with a WITH in it.
    if is_count_star(expression) {
        return Ok(());
    }
    check_field_sources(expression, values)?;
    check_let_names(expression, scope)?;
    // Any other `None` is a fragment this crate has not parsed, so it cannot
    // be shown to stay inside the scope. Refusing beats guessing: the
    // alternative is admitting a reference to a binding the projection
    // dropped.
    let Some(bindings) = expression_bindings(expression) else {
        return Err(GraphPlanError::UnsupportedCypher(
            QueryFailureReason::Return,
            format!("{clause} reads syntax this engine cannot scope-check"),
        ));
    };
    for binding in bindings {
        if !scope.contains(&binding) {
            return Err(GraphPlanError::invalid_reference(
                QueryFailureReason::Return,
                binding.as_str(),
                format!("{clause} references out-of-scope binding {binding}"),
            ));
        }
    }
    Ok(())
}

/// §20.1 Syntax Rules 4 and 5, §20.11: a property's source is a node, an edge
/// or a record. A property of a node or an edge is a property value, never
/// any of them, so `n.a.b` is wrong unless `n` holds a value, which may be a
/// record (GV45): only a name in `values` can.
fn check_field_sources(expression: &Expression, values: &BTreeSet<Symbol>) -> GraphPlanResult<()> {
    if let Expression::Property { target, key } = expression {
        if let Expression::Property { target: inner, .. } = target.as_ref() {
            let mut root = inner.as_ref();
            while let Expression::Property { target, .. } = root {
                root = target;
            }
            if let Expression::Identifier(name) = root {
                if !values.contains(&Symbol::from(name.as_str())) {
                    return Err(GraphPlanError::syntax(
                        QueryFailureReason::Return,
                        format!("only a node, an edge or a record has properties, so .{key} cannot follow a property of {name}"),
                    ));
                }
            }
        }
    }
    try_for_each_scoped_child(expression, values, check_field_sources)
}

/// §20.5 Syntax Rule 3 b): a LET name is new to the record it amends.
fn check_let_names(expression: &Expression, scope: &BTreeSet<Symbol>) -> GraphPlanResult<()> {
    if let Expression::Let { bindings, .. } = expression {
        if let Some((name, _)) = bindings.iter().find(|(name, _)| scope.contains(&Symbol::from(name.as_str()))) {
            return Err(GraphPlanError::syntax(
                QueryFailureReason::Return,
                format!("LET cannot rebind {name}, which is already in scope"),
            ));
        }
    }
    try_for_each_scoped_child(expression, scope, check_let_names)
}

/// The node group variables a path's quantified paths declare (§16.7, G035).
/// `plan` binding the values a path declares (§16.4, §16.7): its path and
/// subpath variables, and each quantified path's group variables, read off
/// `parts`, the path's parts in written order.
fn bind_path_values(plan: GraphLogicalPlan, pattern: &PathPattern, parts: &[PathPart]) -> GraphLogicalPlan {
    let names = pattern
        .binding
        .iter()
        .chain(&pattern.subpaths)
        .map(|name| Symbol::from(name.as_str()))
        .collect::<Vec<_>>();
    let mut plan = if names.is_empty() {
        plan
    } else {
        GraphLogicalPlan::BindPath {
            input: Box::new(plan),
            names,
            parts: parts.to_vec(),
            node_groups: Vec::new(),
        }
    };
    // §16.7 G035: each quantified path's iteration nodes, read off the
    // path of its own hop; the row keeps their records. A quantified
    // path that names no group binds nothing here.
    let quantified = pattern.elements.iter().filter_map(|element| match element {
        PatternElement::Relationship(relationship) if relationship.length.is_some() => Some(relationship),
        _ => None,
    });
    let lists = parts.iter().enumerate().filter(|(_, part)| matches!(part, PathPart::Edges(_)));
    for (relationship, (index, _)) in quantified.zip(lists) {
        let Some(iteration) = &relationship.iteration_nodes else {
            continue;
        };
        let groups = |name: &Option<Identifier>, ends: bool| name.as_ref().map(|name| (Symbol::from(name.as_str()), ends));
        let node_groups: Vec<_> = groups(&iteration.start, false)
            .into_iter()
            .chain(groups(&iteration.end, true))
            .collect();
        if node_groups.is_empty() {
            continue;
        }
        plan = GraphLogicalPlan::BindPath {
            input: Box::new(plan),
            names: Vec::new(),
            parts: parts[index - 1..=index + 1].to_vec(),
            node_groups,
        };
    }
    plan
}

/// The path, subpath and group variables a MATCH declares: values, where
/// [`clause_bindings`] are its nodes and relationships.
fn clause_path_names(matched: &MatchClause) -> Vec<Symbol> {
    matched.patterns.iter().flat_map(path_variables).collect()
}

/// Node variables exported as group lists by repeated primaries.
fn iteration_names(path: &PathPattern) -> Vec<Identifier> {
    path.all_elements()
        .into_iter()
        .filter_map(|element| match element {
            PatternElement::Relationship(relationship) => relationship.iteration_nodes.as_ref(),
            PatternElement::Node(_) => None,
        })
        .flat_map(|iteration| iteration.start.iter().chain(&iteration.end).cloned().collect::<BTreeSet<_>>())
        .collect()
}

/// The path, subpath and group node variables a path pattern declares
/// (§16.4 Syntax Rules 4 and 15, §16.7 Syntax Rule 20), each once: more than
/// one operand of a union may declare one.
fn path_variables(path: &PathPattern) -> Vec<Symbol> {
    let mut names = path.binding.iter().chain(&path.subpaths).cloned().collect::<Vec<_>>();
    match &path.alternatives {
        None => names.extend(iteration_names(path)),
        Some(alternatives) => {
            let mut operands = Vec::new();
            for operand in &alternatives.operands {
                for name in operand.path.subpaths.iter().cloned().chain(iteration_names(&operand.path)) {
                    if !operands.contains(&name) {
                        operands.push(name);
                    }
                }
            }
            names.extend(operands);
        }
    }
    names.into_iter().map(|name| Symbol::from(name.as_str())).collect()
}

/// The `WHERE` of each union operand's parentheses.
fn operand_predicates(matched: &MatchClause) -> Vec<&Expression> {
    matched
        .patterns
        .iter()
        .filter_map(|path| path.alternatives.as_ref())
        .flat_map(|alternatives| &alternatives.operands)
        .filter_map(|operand| operand.predicate.as_ref())
        .collect()
}

/// Every variable `expression` reads, but those inside its subqueries.
fn names_read_outside_subqueries(expression: &Expression, into: &mut BTreeSet<Symbol>) {
    match expression {
        Expression::Identifier(name) => {
            into.insert(Symbol::from(name.as_str()));
        }
        Expression::Exists(_) | Expression::Value(_) => {}
        // A LET body's own names are not read from outside it.
        Expression::Let { bindings, body } => {
            for (_, value) in bindings {
                names_read_outside_subqueries(value, into);
            }
            let mut inner = BTreeSet::new();
            names_read_outside_subqueries(body, &mut inner);
            for (name, _) in bindings {
                inner.remove(&Symbol::from(name.as_str()));
            }
            into.extend(inner);
        }
        other => {
            let _ = other.try_map_children::<()>(&mut |child| {
                names_read_outside_subqueries(child, into);
                Ok(child.clone())
            });
        }
    }
}

/// The paths a path pattern lowers as, each with the `WHERE` that holds of it
/// alone: the pattern itself, or each operand of its union (§16.7 G030 to
/// G033) under the pattern's own variable and path modes.
fn union_operands<'a>(
    written: &'a PathPattern,
    exterior: &BTreeSet<Symbol>,
    clause_bound: &BTreeSet<Symbol>,
    matched: &MatchClause,
) -> GraphPlanResult<Vec<(std::borrow::Cow<'a, PathPattern>, Option<&'a Expression>)>> {
    let Some(alternatives) = &written.alternatives else {
        return Ok(vec![(std::borrow::Cow::Borrowed(written), None)]);
    };
    let syntax = |message: String| GraphPlanError::syntax(QueryFailureReason::Pattern, message);
    let unsupported = |message: &str| GraphPlanError::UnsupportedCypher(QueryFailureReason::Pattern, message.to_string());
    if written.search.is_some() {
        return Err(unsupported("a path search over a path pattern union is not supported"));
    }
    if alternatives.operands.iter().any(|operand| operand.path.alternatives.is_some()) {
        return Err(unsupported("a path pattern union inside another is not supported"));
    }
    // §16.7 Syntax Rule 10 c): a multiset alternation's operands name
    // distinct subpaths.
    if alternatives.multiset {
        let mut seen = BTreeSet::new();
        for name in alternatives.operands.iter().flat_map(|operand| &operand.path.subpaths) {
            if !seen.insert(name.as_str()) {
                return Err(syntax(format!("the multiset alternation operands both declare the subpath {name}")));
            }
        }
    }
    // A variable's degree in each operand (§16.7 Syntax Rule 22 a) and e)):
    // a group variable is a quantified edge's or a quantified path's node.
    let groups = |path: &PathPattern| {
        path.elements
            .iter()
            .filter_map(|element| match element {
                PatternElement::Relationship(relationship) if relationship.length.is_some() => relationship.variable.as_ref(),
                _ => None,
            })
            .chain(&iteration_names(path))
            .map(|name| Symbol::from(name.as_str()))
            .collect::<BTreeSet<_>>()
    };
    let declared = alternatives
        .operands
        .iter()
        .map(|operand| {
            path_bindings(&operand.path)
                .into_iter()
                .chain(groups(&operand.path))
                .collect::<BTreeSet<_>>()
        })
        .collect::<Vec<_>>();
    let grouped = alternatives
        .operands
        .iter()
        .map(|operand| groups(&operand.path))
        .collect::<Vec<_>>();
    let everywhere = declared
        .iter()
        .skip(1)
        .fold(declared[0].clone(), |all, names| all.intersection(names).cloned().collect());
    let somewhere = declared.iter().flatten().cloned().collect::<BTreeSet<_>>();
    if let Some(name) = somewhere.iter().find(|name| {
        let mut degrees = declared
            .iter()
            .zip(&grouped)
            .filter(|(names, _)| names.contains(*name))
            .map(|(_, groups)| groups.contains(*name));
        let first = degrees.next();
        degrees.any(|degree| Some(degree) != first)
    }) {
        return Err(unsupported(&format!(
            "{name} is a group variable in one operand of the union and a single element in another; this is not supported"
        )));
    }
    // §16.4 Syntax Rule 3: a variable two path patterns share is an
    // unconditional singleton of both, so declared by every operand.
    let others = matched
        .patterns
        .iter()
        .filter(|other| !std::ptr::eq(*other, written))
        .flat_map(path_bindings)
        .collect::<BTreeSet<_>>();
    for name in somewhere.difference(&everywhere) {
        if others.contains(name) {
            return Err(syntax(format!(
                "{name} is declared by only some operands of a path pattern union and cannot be shared with another path pattern"
            )));
        }
        if exterior.contains(name) {
            return Err(unsupported(&format!(
                "{name} is bound before the MATCH and declared by only some operands of a path pattern union; this is not supported"
            )));
        }
    }
    // The path, subpath and group values of the clause's other patterns: an
    // earlier one's are bound on the operand's rows, as its elements are; a
    // later one's are not yet.
    let other_values: BTreeSet<Symbol> = matched
        .patterns
        .iter()
        .filter(|other| !std::ptr::eq(*other, written))
        .flat_map(path_variables)
        .collect();
    let earlier_values: BTreeSet<Symbol> = matched
        .patterns
        .iter()
        .take_while(|other| !std::ptr::eq(*other, written))
        .flat_map(path_variables)
        .collect();
    let mut operands = Vec::new();
    for (operand, names) in alternatives.operands.iter().zip(&declared) {
        // §16.7 Syntax Rule 11: an operand reads its own variables and those
        // declared outside the union only. One it reads from a later path
        // pattern of the clause would be held to every operand's rows.
        // Every expression it holds: element WHEREs, inline properties, and
        // the WHERE around it.
        let predicates = operand
            .path
            .elements
            .iter()
            .flat_map(|element| match element {
                PatternElement::Node(node) => [node.predicate.as_ref(), node.properties.as_ref()],
                PatternElement::Relationship(relationship) => [relationship.predicate.as_ref(), relationship.properties.as_ref()],
            })
            .flatten()
            .chain(&operand.predicate);
        // Its path and subpath variables too: they are bound before its WHERE.
        let readable: BTreeSet<Symbol> = names
            .iter()
            .cloned()
            .chain(
                written
                    .binding
                    .iter()
                    .chain(&written.subpaths)
                    .chain(&operand.path.subpaths)
                    .map(|name| Symbol::from(name.as_str())),
            )
            .collect();
        // What a subquery in it sees: the operand's own names and those of
        // the rest of the clause and query, never another operand's (§16.7
        // Syntax Rule 11), so there such a name is the subquery's own.
        let visible: BTreeSet<Symbol> = exterior
            .iter()
            .chain(clause_bound)
            .chain(&others)
            .chain(&other_values)
            .chain(&readable)
            .cloned()
            .collect();
        for predicate in predicates {
            // The names it reads outside any subquery, and those its
            // subqueries read: a subquery runs on the operand's rows too.
            let mut reads = BTreeSet::new();
            names_read_outside_subqueries(predicate, &mut reads);
            reads.extend(expression_reads(predicate, &visible));
            for name in reads.difference(&readable) {
                if somewhere.contains(name) && !others.contains(name) {
                    return Err(syntax(format!(
                        "an operand of a path pattern union reads {name}, which only another operand declares"
                    )));
                }
                if !exterior.contains(name) && !clause_bound.contains(name) && !earlier_values.contains(name) {
                    return Err(unsupported(&format!(
                        "an operand of a path pattern union reads {name}, which a later path pattern declares; this is not supported"
                    )));
                }
            }
        }
        let mut path = operand.path.clone();
        path.binding = written.binding.clone();
        path.subpaths = written.subpaths.iter().chain(&operand.path.subpaths).cloned().collect();
        path.modes = written.modes.iter().chain(&operand.path.modes).copied().collect();
        operands.push((std::borrow::Cow::Owned(path), operand.predicate.as_ref()));
    }
    Ok(operands)
}

/// A union operand's word (§22.2): its elements in written order, `parts`
/// being how the lowering bound them, each with the variable the query
/// declared for it.
fn path_word(path: &PathPattern, parts: &[PathPart], synthesized: &BTreeSet<Symbol>) -> PathWord {
    let named = |name: &Option<Identifier>| name.as_ref().map(|name| Symbol::from(name.as_str()));
    let letters = parts
        .iter()
        .zip(&path.elements)
        .map(|(part, element)| match element {
            PatternElement::Node(node) => WordLetter {
                part: part.clone(),
                name: named(&node.variable).filter(|name| !synthesized.contains(name)),
                iteration: None,
            },
            PatternElement::Relationship(relationship) => WordLetter {
                part: part.clone(),
                name: named(&relationship.variable),
                iteration: relationship
                    .iteration_nodes
                    .as_ref()
                    .map(|iteration| (named(&iteration.start), named(&iteration.end))),
            },
        })
        .collect();
    let mut subpaths = path.subpaths.iter().map(|name| Symbol::from(name.as_str())).collect::<Vec<_>>();
    subpaths.sort();
    PathWord { subpaths, letters }
}

/// Expression roots at this path's factor depth. Consumers still choose
/// their own expression/query boundaries; iteration primaries are not lost
/// merely because the executable adapter represents them as one hop.
fn path_expression_roots(path: &PathPattern) -> Vec<&Expression> {
    let mut roots: Vec<_> = path.predicate.iter().collect();
    if let Some(alternatives) = &path.alternatives {
        for operand in &alternatives.operands {
            roots.extend(operand.predicate.iter());
            roots.extend(operand.path.predicate.iter());
        }
    }
    roots.extend(path_element_expression_roots(path));
    roots
}

fn path_element_expression_roots(path: &PathPattern) -> Vec<&Expression> {
    let mut roots = Vec::new();
    if let Some(alternatives) = &path.alternatives {
        for operand in &alternatives.operands {
            roots.extend(path_element_expression_roots(&operand.path));
        }
    }
    for element in &path.elements {
        match element {
            PatternElement::Node(node) => roots.extend(node.properties.iter().chain(&node.predicate)),
            PatternElement::Relationship(edge) => {
                roots.extend(edge.properties.iter().chain(&edge.predicate));
                if let Some(iteration) = &edge.iteration_nodes {
                    roots.extend(path_expression_roots(&iteration.primary));
                }
            }
        }
    }
    roots
}
/// `KEEP P` (GQL §16.4 Syntax Rule 6): every path pattern takes the prefix
/// P, and none may have a search prefix of its own.
fn with_keep(matched: &MatchClause) -> GraphPlanResult<MatchClause> {
    let mut matched = matched.clone();
    let Some(keep) = matched.keep.take() else {
        return Ok(matched);
    };
    for pattern in &mut matched.patterns {
        if pattern.search_prefix_written
            || pattern.search.is_some()
            || pattern.factor.as_ref().is_some_and(|factor| factor.has_written_search())
        {
            return Err(GraphPlanError::syntax(
                QueryFailureReason::Pattern,
                "a path pattern under KEEP has no search prefix of its own".to_string(),
            ));
        }
        if keep.search.is_some() {
            pattern.search = keep.search.clone();
        }
        pattern.modes.extend(keep.mode);
    }
    Ok(matched)
}

/// The WHERE around a path that selects nothing holds of the match as the
/// clause's WHERE does (§16.7, G050), so it joins that WHERE, where its
/// subqueries are hoisted with the rest. A selective path keeps its own: it
/// filters the path's candidates before the search (§22.4).
fn with_path_predicates(mut matched: MatchClause) -> MatchClause {
    let folded: Vec<Expression> = matched
        .patterns
        .iter_mut()
        .filter(|pattern| pattern.search.is_none())
        .filter_map(|pattern| pattern.predicate.take())
        .collect();
    matched.predicate = folded
        .into_iter()
        .chain(matched.predicate.take())
        .reduce(|left, right| Expression::Binary {
            left: Box::new(left),
            operator: hydradb_cypher_ast::BinaryOperator::And,
            right: Box::new(right),
        });
    matched
}

/// §16.6 Syntax Rules 2 b) ii) and 7: a literal count is positive, and a
/// selective path's strict interior variables -- every variable it declares
/// but its first and last node's -- are its own.
fn check_selective_path(pattern: &PathPattern, outer: &BTreeSet<Symbol>, matched: &MatchClause) -> GraphPlanResult<()> {
    let syntax = |message: String| GraphPlanError::syntax(QueryFailureReason::Pattern, message);
    if let Some(search) = &pattern.search {
        if matches!(search.count, Expression::Literal(Literal::Integer(count)) if count <= 0) {
            return Err(syntax("a path search counts a positive integer".to_string()));
        }
    }
    let boundary = |element: Option<&PatternElement>| match element {
        Some(PatternElement::Node(node)) => node.variable.as_ref().map(|variable| Symbol::from(variable.as_str())),
        _ => None,
    };
    let mut interior = path_bindings(pattern);
    for name in [boundary(pattern.elements.first()), boundary(pattern.elements.last())]
        .into_iter()
        .flatten()
    {
        interior.remove(&name);
    }
    // What the other paths declare: their elements, and their path and
    // group values, which are no more bound while its candidates are chosen.
    let others = matched
        .patterns
        .iter()
        .filter(|other| !std::ptr::eq(*other, pattern))
        .flat_map(|other| path_bindings(other).into_iter().chain(path_variables(other)))
        .collect::<BTreeSet<_>>();
    if let Some(shared) = interior.iter().find(|name| outer.contains(*name) || others.contains(*name)) {
        return Err(syntax(format!(
            "{shared} is inside a selective path pattern and cannot be shared with the rest of the query"
        )));
    }
    // Syntax Rule 8: its predicates filter its candidates before the graph
    // pattern's other paths are joined, so they read no variable only those
    // paths declare. What the enclosing query bound is read from its row.
    let own = path_bindings(pattern)
        .into_iter()
        .chain(path_variables(pattern))
        .collect::<BTreeSet<_>>();
    for predicate in path_expression_roots(pattern) {
        // Its subqueries' references count: they are contained in the path.
        let visible = outer.iter().chain(&own).chain(&others).cloned().collect::<BTreeSet<_>>();
        let reads = expression_reads(predicate, &visible);
        if let Some(name) = reads
            .iter()
            .find(|name| others.contains(*name) && !own.contains(*name) && !outer.contains(*name))
        {
            return Err(syntax(format!(
                "a selective path pattern reads {name}, which only another path of its graph pattern declares"
            )));
        }
    }
    Ok(())
}

/// The selection a path search makes over `input`, the matches of `pattern`
/// whose parts `parts` names (§22.4).
fn selected_paths(
    input: GraphLogicalPlan,
    pattern: &PathPattern,
    parts: &[PathPart],
    search: &hydradb_cypher_ast::PathSearch,
) -> GraphPlanResult<GraphLogicalPlan> {
    let (Some(PathPart::Node(first)), Some(PathPart::Node(last))) = (parts.first(), parts.last()) else {
        return Err(GraphPlanError::UnsupportedCypher(
            QueryFailureReason::Pattern,
            "a path search needs the path's end nodes".to_string(),
        ));
    };
    let fixed_edges = pattern
        .elements
        .iter()
        .filter(|element| matches!(element, PatternElement::Relationship(relationship) if relationship.length.is_none()))
        .count();
    Ok(GraphLogicalPlan::SelectPaths {
        input: Box::new(input),
        first: first.clone(),
        last: last.clone(),
        fixed_edges,
        lists: parts
            .iter()
            .filter_map(|part| match part {
                PathPart::Edges(name) => Some(name.clone()),
                _ => None,
            })
            .collect(),
        kind: search.kind,
        count: lower_expression(&search.count)?,
    })
}

/// How [`lower_path`] lowers a path: whether it names every hop, and what its
/// variable-length hops hold of the edges and nodes they visit.
#[derive(Clone, Copy)]
struct PathOptions<'a> {
    record: &'a BTreeSet<Symbol>,
    table_columns: &'a BTreeSet<Symbol>,
    values: &'a BTreeSet<Symbol>,
    visible: &'a BTreeSet<Symbol>,
    /// Names already carried by the clause's incoming working-table row.
    incoming: &'a BTreeSet<Symbol>,
    /// Values bound by earlier patterns, readable but not element anchors.
    path_values: &'a BTreeSet<Symbol>,
    name_every_hop: bool,
    /// Whether variable-length hops keep to a trail.
    trail: bool,
    /// What they hold of the nodes they visit.
    nodes: WalkNodes,
    /// Whether a one-hop path may start from its bound far end.
    anchor: bool,
}

/// Lower one path pattern.
///
/// With `name_every_hop`, an unnamed fixed or variable-length hop gets a
/// synthesized relationship name. The clause holds each relationship or
/// relationship-list binding disjoint from the other named trails.
fn lower_path(
    path: &PathPattern,
    bound: &BTreeSet<Symbol>,
    synthesized: &mut BTreeSet<Symbol>,
    options: PathOptions<'_>,
    hop_names: &mut Vec<Symbol>,
    clause_deferred: &mut Vec<LogicalExpression>,
    parts: &mut Vec<PathPart>,
) -> GraphPlanResult<GraphLogicalPlan> {
    let PathOptions {
        record,
        table_columns,
        values,
        visible,
        incoming,
        path_values,
        name_every_hop,
        trail,
        nodes,
        anchor,
    } = options;
    let anchored = anchor.then(|| anchor_on_bound_binding(path, bound)).flatten();
    let elements = anchored.as_deref().unwrap_or(path.elements.as_slice());
    let first_part = parts.len();
    let Some(PatternElement::Node(first)) = elements.first() else {
        return Err(GraphPlanError::UnsupportedCypher(
            QueryFailureReason::Pattern,
            "a path must begin with a node".to_string(),
        ));
    };
    let first_scan = lower_node_scan(first, synthesized)?;
    let GraphLogicalPlan::NodeScan {
        binding: first_binding, ..
    } = &first_scan
    else {
        unreachable!("node lowering returns a scan")
    };
    let mut from = first_binding.clone();
    parts.push(PathPart::Node(from.clone()));
    let mut plan = apply_pattern_properties(first_scan, &from, first.properties.as_ref(), PatternElementKind::Node)?;
    plan = apply_label_expression(plan, &from, first.label_expression.as_ref());
    // Names this path has bound so far, on top of the incoming row and earlier
    // patterns. An element's WHERE is applied as soon as everything it
    // reads is in here, so a selective one narrows the rows before the next
    // hop rather than after the whole walk; one that reads further along the
    // path waits in `deferred` until the path is complete.
    let mut available = bound.union(incoming).cloned().collect::<BTreeSet<_>>();
    available.extend(path_values.iter().cloned());
    available.insert(from.clone());
    let mut deferred = Vec::new();
    plan = apply_element_predicate(plan, first.predicate.as_ref(), &available, &mut deferred)?;
    let mut remaining = &elements[1..];

    loop {
        // §16.7 G046: a node pattern beside another binds the node it does,
        // a hop of no edge.
        if let [PatternElement::Node(target), tail @ ..] = remaining {
            let GraphLogicalPlan::NodeScan {
                binding: written_to,
                labels: target_labels,
            } = lower_node_scan(target, synthesized)?
            else {
                unreachable!("node lowering returns a scan")
            };
            let to = if written_to == from {
                let index = synthesized
                    .iter()
                    .filter(|name| name.as_str().starts_with(ANONYMOUS_NODE_PREFIX))
                    .count();
                let fresh = Symbol::from(format!("{ANONYMOUS_NODE_PREFIX}{index}").as_str());
                synthesized.insert(fresh.clone());
                fresh
            } else {
                written_to.clone()
            };
            plan = GraphLogicalPlan::VariableExpand {
                input: Box::new(plan),
                from,
                relationship: None,
                relationship_types: Vec::new(),
                direction: PatternDirection::Undirected,
                to: to.clone(),
                target_labels,
                min_hops: 0,
                max_hops: Some(0),
                iteration: None,
                trail: false,
                nodes: WalkNodes::Any,
            };
            if to != written_to {
                plan = GraphLogicalPlan::Filter {
                    operation: operation::MATCH,
                    input: Box::new(plan),
                    predicate: LogicalExpression::Binary {
                        left: Box::new(LogicalExpression::Identity(to.clone())),
                        operator: LogicalBinaryOperator::Equal,
                        right: Box::new(LogicalExpression::Identity(written_to.clone())),
                    },
                };
            }
            plan = apply_pattern_properties(plan, &to, target.properties.as_ref(), PatternElementKind::Node)?;
            plan = apply_label_expression(plan, &to, target.label_expression.as_ref());
            parts.push(PathPart::Node(written_to.clone()));
            available.insert(written_to.clone());
            plan = apply_element_predicate(plan, target.predicate.as_ref(), &available, &mut deferred)?;
            from = written_to;
            remaining = tail;
            continue;
        }
        let [PatternElement::Relationship(relationship), PatternElement::Node(target), tail @ ..] = remaining else {
            break;
        };
        if relationship.predicate.is_some() && relationship.length.is_some() {
            return Err(GraphPlanError::UnsupportedCypher(
                QueryFailureReason::Where,
                "a WHERE inside a variable-length relationship is not supported".to_string(),
            ));
        }
        let GraphLogicalPlan::NodeScan {
            binding: written_to,
            labels: target_labels,
        } = lower_node_scan(target, synthesized)?
        else {
            unreachable!("node lowering returns a scan")
        };
        // `(n)-[:R]->(n)` names one node at both ends of a hop: it matches the
        // R relationships from n back to n. The hop expands into a fresh name,
        // unspellable like any synthesized one, and a filter holds that end to
        // be n itself; the path then continues from n.
        let to = if written_to == from {
            let index = synthesized
                .iter()
                .filter(|name| name.as_str().starts_with(ANONYMOUS_NODE_PREFIX))
                .count();
            let fresh = Symbol::from(format!("{ANONYMOUS_NODE_PREFIX}{index}").as_str());
            synthesized.insert(fresh.clone());
            fresh
        } else {
            written_to.clone()
        };
        let relationship_binding = relationship
            .variable
            .as_ref()
            .map(|binding| Symbol::from(binding.as_str()))
            .or_else(|| {
                let needs_name = relationship.properties.is_some() || relationship.label_expression.is_some() || name_every_hop;
                needs_name.then(|| {
                    // Counts this prefix only: nodes synthesize into the same
                    // set and their names are not part of this sequence.
                    let index = synthesized
                        .iter()
                        .filter(|name| name.as_str().starts_with(INLINE_RELATIONSHIP_PREFIX))
                        .count();
                    let name = Symbol::from(format!("{INLINE_RELATIONSHIP_PREFIX}{index}").as_str());
                    synthesized.insert(name.clone());
                    name
                })
            });
        if name_every_hop {
            hop_names.extend(relationship_binding.clone());
        }
        if relationship_binding
            .as_ref()
            .is_some_and(|binding| binding == &from || binding == &to)
        {
            return Err(GraphPlanError::syntax(
                QueryFailureReason::Pattern,
                "relationship and node bindings must be distinct".to_string(),
            ));
        }
        // HydraDB stores no undirected edge, so one that may be undirected
        // matches the directed edges it also allows (§16.7, G043), and `~[ ]~`
        // matches none.
        let direction = match relationship.direction {
            Direction::Outgoing | Direction::UndirectedOrRight => PatternDirection::Outgoing,
            Direction::Incoming | Direction::LeftOrUndirected => PatternDirection::Incoming,
            // `-[:R]-` and `<-[:R]->` both mean "either orientation satisfies
            // this hop"; the executor runs both and merges.
            Direction::Undirected | Direction::Bidirectional | Direction::UndirectedEdge => PatternDirection::Undirected,
        };
        let no_edge = relationship.direction == Direction::UndirectedEdge;
        // Of `~[ ]~{0,n}` only the iteration of no edge can match, and of any
        // other `~[ ]~` nothing.
        let matches_nothing = no_edge && relationship.length.is_none_or(|length| length.minimum.unwrap_or(1) > 0);
        let relationship_types = relationship
            .types
            .iter()
            .map(|relationship_type| Symbol::from(relationship_type.as_str()))
            .collect();
        // The hop still binds its names, so a miss nulls them as any would,
        // but it reads no edge: nothing reaches it (see `plan_filter`).
        if matches_nothing {
            plan = GraphLogicalPlan::Filter {
                operation: operation::MATCH,
                input: Box::new(plan),
                predicate: LogicalExpression::Literal(ScalarValue::Boolean(false)),
            };
        }
        plan = if let Some(length) = relationship.length {
            if relationship.label_expression.is_some() {
                return Err(GraphPlanError::UnsupportedCypher(
                    QueryFailureReason::Pattern,
                    "a label expression on a variable-length relationship is not supported".to_string(),
                ));
            }
            if relationship.properties.is_some() {
                return Err(GraphPlanError::UnsupportedCypher(
                    QueryFailureReason::Pattern,
                    "variable-length relationship properties are not supported".to_string(),
                ));
            }
            let min_hops = length.minimum.unwrap_or(1);
            // No maximum is Cypher's unbounded walk. It terminates because a
            // path never reuses a relationship; how much work it may do is the
            // query's row budgets' to decide, which fail it loudly rather than
            // truncating the answer at an invented depth.
            let max_hops = length.maximum;
            if max_hops.is_some_and(|max_hops| min_hops > max_hops) {
                return Err(GraphPlanError::syntax(
                    QueryFailureReason::Pattern,
                    "variable-length traversal minimum exceeds maximum".to_string(),
                ));
            }
            // §16.11 Syntax Rules 1 and 3 a) i): a GQL upper bound is positive,
            // and no bound exceeds IL018, 255.
            if length.quantifier && max_hops == Some(0) {
                return Err(GraphPlanError::syntax(
                    QueryFailureReason::Pattern,
                    "a quantifier's upper bound is greater than 0".to_string(),
                ));
            }
            let hop_bound = |bound: u32| {
                u8::try_from(bound).map_err(|_| {
                    GraphPlanError::syntax(
                        QueryFailureReason::Pattern,
                        format!("a quantifier's bounds are at most {}", u8::MAX),
                    )
                })
            };
            let min_hops = hop_bound(min_hops)?;
            // The written bound is checked even where only no edge can match.
            let max_hops = max_hops.map(hop_bound).transpose()?;
            let max_hops = if no_edge && min_hops == 0 { Some(0) } else { max_hops };
            GraphLogicalPlan::VariableExpand {
                input: Box::new(plan),
                from,
                relationship: relationship_binding.clone(),
                relationship_types,
                direction,
                to: to.clone(),
                target_labels,
                min_hops,
                max_hops,
                trail,
                nodes,
                iteration: relationship
                    .iteration_nodes
                    .as_ref()
                    .map(|iteration| lower_iteration(&iteration.primary, &available, visible, values, record, table_columns, synthesized))
                    .transpose()?,
            }
        } else {
            GraphLogicalPlan::Expand {
                input: Box::new(plan),
                from,
                relationship: relationship_binding.clone(),
                relationship_types,
                direction,
                to: to.clone(),
                target_labels,
            }
        };
        if to != written_to {
            plan = GraphLogicalPlan::Filter {
                operation: hydradb_cypher_ast::operation::MATCH,
                input: Box::new(plan),
                predicate: LogicalExpression::Binary {
                    left: Box::new(LogicalExpression::Identity(to.clone())),
                    operator: LogicalBinaryOperator::Equal,
                    right: Box::new(LogicalExpression::Identity(written_to.clone())),
                },
            };
        }
        plan = apply_pattern_properties(plan, &to, target.properties.as_ref(), PatternElementKind::Node)?;
        plan = apply_label_expression(plan, &to, target.label_expression.as_ref());
        if let Some(binding) = &relationship_binding {
            plan = apply_label_expression(plan, binding, relationship.label_expression.as_ref());
        }
        if let Some(properties) = &relationship.properties {
            let binding = relationship_binding.as_ref().expect("inline properties synthesize a binding");
            plan = apply_pattern_properties(plan, binding, Some(properties), PatternElementKind::Relationship)?;
        }
        parts.extend(relationship_binding.clone().map(|binding| {
            if relationship.length.is_some() {
                PathPart::Edges(binding)
            } else {
                PathPart::Edge(binding)
            }
        }));
        parts.push(PathPart::Node(written_to.clone()));
        available.insert(written_to.clone());
        available.extend(relationship_binding);
        plan = apply_element_predicate(plan, relationship.predicate.as_ref(), &available, &mut deferred)?;
        plan = apply_element_predicate(plan, target.predicate.as_ref(), &available, &mut deferred)?;
        from = written_to;
        remaining = tail;
    }
    if !remaining.is_empty() {
        return Err(GraphPlanError::UnsupportedCypher(
            QueryFailureReason::Pattern,
            "a path pattern ends with a node".to_string(),
        ));
    }
    // The path is the one written, whichever end it was read from: its
    // value, subpaths and path modes read the parts in written order.
    if anchored.is_some() {
        parts[first_part..].reverse();
    }
    clause_deferred.extend(deferred);
    Ok(plan)
}

fn lower_iteration(
    primary: &PathPattern,
    outside: &BTreeSet<Symbol>,
    visible: &BTreeSet<Symbol>,
    values: &BTreeSet<Symbol>,
    record: &BTreeSet<Symbol>,
    table_columns: &BTreeSet<Symbol>,
    synthesized: &mut BTreeSet<Symbol>,
) -> GraphPlanResult<super::IterationPlan<GraphLogicalPlan>> {
    let [PatternElement::Node(source), PatternElement::Relationship(edge), PatternElement::Node(target)] = primary.elements.as_slice()
    else {
        return Err(GraphPlanError::UnsupportedCypher(
            QueryFailureReason::Pattern,
            "a repeated primary's composition is not lowered".to_string(),
        ));
    };
    let node_name = |node: &NodePattern, synthesized: &mut BTreeSet<Symbol>| -> GraphPlanResult<Symbol> {
        let GraphLogicalPlan::NodeScan { binding, .. } = lower_node_scan(node, synthesized)? else {
            unreachable!()
        };
        Ok(binding)
    };
    let from = node_name(source, synthesized)?;
    let to = node_name(target, synthesized)?;
    let relationship = match &edge.variable {
        Some(name) => Symbol::from(name.as_str()),
        None => {
            let name = Symbol::from(format!("{INLINE_RELATIONSHIP_PREFIX}{}", synthesized.len()).as_str());
            synthesized.insert(name.clone());
            name
        }
    };
    let mut scope = outside.difference(synthesized).cloned().collect::<BTreeSet<_>>();
    scope.extend(
        source
            .variable
            .iter()
            .chain(&edge.variable)
            .chain(&target.variable)
            .map(|name| Symbol::from(name.as_str())),
    );
    let declared = visible.union(&scope).cloned().collect::<BTreeSet<_>>();
    if path_expression_roots(primary)
        .iter()
        .any(|expression| !expression_reads(expression, &declared).is_subset(&scope))
    {
        return Err(GraphPlanError::UnsupportedCypher(
            QueryFailureReason::Pattern,
            "an iteration predicate depends on a binding not yet available".to_string(),
        ));
    }
    let mut local_values = values.clone();
    for name in [&from, &relationship, &to] {
        local_values.remove(name);
    }
    let mut plan = GraphLogicalPlan::SingleRow;
    for (binding, node) in [(&from, source), (&to, target)] {
        for label in &node.labels {
            plan = apply_label_expression(plan, binding, Some(&LabelExpression::Label(label.clone())));
        }
        plan = apply_label_expression(plan, binding, node.label_expression.as_ref());
        if let Some(properties) = &node.properties {
            let expression = resolve_value_keywords(properties, &scope);
            check_scope(&expression, &scope, &local_values, "a repeated primary")?;
            plan = apply_pattern_properties(plan, binding, Some(&expression), PatternElementKind::Node)?;
        }
    }
    plan = apply_label_expression(plan, &relationship, edge.label_expression.as_ref());
    if let Some(properties) = &edge.properties {
        let expression = resolve_value_keywords(properties, &scope);
        check_scope(&expression, &scope, &local_values, "a repeated primary")?;
        plan = apply_pattern_properties(plan, &relationship, Some(&expression), PatternElementKind::Relationship)?;
    }
    for predicate in source
        .predicate
        .iter()
        .chain(&edge.predicate)
        .chain(&target.predicate)
        .chain(&primary.predicate)
    {
        let predicate = resolve_value_keywords(predicate, &scope);
        let predicate = lower_predicate_into(
            &predicate,
            &mut plan,
            PredicateScope {
                record,
                table_columns,
                visible: &scope,
                bound: &scope,
                values: &local_values,
                groups: &BTreeSet::new(),
                clause: "a repeated primary",
            },
            synthesized,
        )?;
        plan = GraphLogicalPlan::Filter {
            operation: operation::MATCH,
            input: Box::new(plan),
            predicate,
        };
    }
    Ok(super::IterationPlan {
        plan: Box::new(plan),
        source: from,
        relationship,
        target: to,
    })
}

/// Keep only the rows whose element bound to `binding` satisfies a label
/// expression that is more than the label list a scan or expansion already
/// enforces.
fn apply_label_expression(plan: GraphLogicalPlan, binding: &Symbol, expression: Option<&LabelExpression>) -> GraphLogicalPlan {
    let Some(expression) = expression else {
        return plan;
    };
    GraphLogicalPlan::Filter {
        operation: hydradb_cypher_ast::operation::MATCH,
        input: Box::new(plan),
        predicate: LogicalExpression::ElementTest {
            binding: binding.clone(),
            test: ElementTestKind::Labeled(expression.clone()),
        },
    }
}

/// `r1 <> r2 AND ...` over every pair of distinct hop names, or `None` when
/// there is no pair. A name written twice is one relationship on purpose and
/// is not compared with itself.
fn distinct_relationships(names: &[Symbol]) -> Option<LogicalExpression> {
    // Repeating one variable is an identity constraint, not another trail.
    let names = names.iter().collect::<BTreeSet<_>>();
    (names.len() >= 2).then(|| LogicalExpression::Function {
        function: ScalarFunction::RelationshipsUnique,
        arguments: names.into_iter().map(|name| LogicalExpression::Binding(name.clone())).collect(),
    })
}

/// A trail's edges all distinct, every hop counted (§16.4: no edge twice), or
/// `None` for fewer than two hops: a name written for two hops is the same
/// edge both times, so such a path is no trail.
fn trail_relationships(hops: &[Symbol]) -> Option<LogicalExpression> {
    (hops.len() >= 2).then(|| LogicalExpression::Function {
        function: ScalarFunction::RelationshipsUnique,
        arguments: hops.iter().map(|name| LogicalExpression::Binding(name.clone())).collect(),
    })
}

/// Filter `plan` by an element's own `WHERE`, or hold the predicate back.
///
/// `(n WHERE n.score > 2)` means what a clause `WHERE n.score > 2` would for
/// the same pattern, so it lowers to the same Filter. It is applied here only
/// when every name it reads is already bound; a predicate reading an element
/// later in the path, or one this crate cannot read the bindings of, goes to
/// `deferred` and filters the completed path instead.
fn apply_element_predicate(
    plan: GraphLogicalPlan,
    predicate: Option<&Expression>,
    available: &BTreeSet<Symbol>,
    deferred: &mut Vec<LogicalExpression>,
) -> GraphPlanResult<GraphLogicalPlan> {
    let Some(predicate) = predicate else {
        return Ok(plan);
    };
    let lowered = lower_expression(predicate)?;
    let ready = expression_bindings(predicate).is_some_and(|reads| reads.is_subset(available));
    if !ready {
        deferred.push(lowered);
        return Ok(plan);
    }
    Ok(GraphLogicalPlan::Filter {
        operation: hydradb_cypher_ast::operation::MATCH,
        input: Box::new(plan),
        predicate: lowered,
    })
}

/// Every `WHERE` written inside one of a clause's node or relationship
/// patterns.
fn element_predicates(matched: &MatchClause) -> impl Iterator<Item = &Expression> {
    matched
        .patterns
        .iter()
        .flat_map(PathPattern::all_elements)
        .filter_map(|element| match element {
            PatternElement::Node(node) => node.predicate.as_ref(),
            PatternElement::Relationship(relationship) => relationship.predicate.as_ref(),
        })
}

/// Re-read a single-hop pattern from the end an earlier pattern already bound,
/// when only that far end is bound.
///
/// `lower_path` anchors at the node written first and expands away from it,
/// and `NaturalJoin` runs correlated: the left row seeds the right side, and a
/// scan whose binding the seed already holds returns that row instead of
/// scanning. So a pattern written *toward* its bound end anchors on the wrong
/// one: `(c:Chunk)-[:HAS_CHUNK]->(s)` with `s` already bound anchors on `c`,
/// scans every Chunk, hydrates each one, expands it, then discards all but the
/// row the seed already held. Written the other way round --
/// `(s)<-[:HAS_CHUNK]-(c:Chunk)` -- the seed answers the anchor and the
/// expansion walks one vertex's edges. The two spell the same pattern, so
/// which one the caller typed should not decide the cost.
///
/// Reversing the elements and flipping the hop is the whole rewrite; the walk
/// that follows is unchanged. With one hop the synthesized relationship
/// binding keeps index 0, so a pattern this leaves alone keeps its plan hash.
///
/// Only when exactly one end is bound. Neither bound is an ordinary anchored
/// walk. Both bound wants an expand-into access path rather than a reversal,
/// which is a separate change. Variable-length hops are left alone: reversing
/// one is sound, but `VariableExpand` earns its own differential cases first.
///
/// Reversing moves each end's inline map across the expansion, so a map may
/// only be moved when it reads the same bindings on both sides of that move.
/// The hop produces two names -- the far end and the relationship -- and those
/// are the only ones whose availability the move changes.
///
/// The bound end's map moves *earlier*, losing both:
/// `(c:Chunk)-[:HAS_CHUNK]->(s {source_id: c.owner})` would read an absent
/// `c`, compare against null, and drop every row the written order keeps.
///
/// The far end's map moves *later*, gaining the relationship:
/// `(c:Chunk {owner: r.kind})-[r:HAS_CHUNK]->(s)` reads a null `r` where it
/// was written and a bound one after the reversal, admitting rows the written
/// order rejects.
///
/// A name from an earlier clause is unaffected in either direction, optional
/// or not: the move cannot change whether the seed row carries it, so reading
/// one never blocks the rewrite.
///
/// An optional name anchors like any other. A miss binds it to null, and a
/// pattern on a null name matches nothing, so the reversed anchor short-
/// circuits to no rows rather than falling through to a scan.
///
/// This once excluded OPTIONAL names, because a miss left the binding absent
/// rather than null: the seed could not answer the anchor, the scan ran for
/// real, and the bare `(s)` it reverses to carries no labels, so a Chunk label
/// scan became a scan of every vertex in the cell. Recording a miss as null
/// removed that cliff -- the anchor now matches nothing instead of
/// everything -- so the exclusion went with it.
fn anchor_on_bound_binding(path: &PathPattern, bound: &BTreeSet<Symbol>) -> Option<Vec<PatternElement>> {
    let [source, hop, target] = path.elements.as_slice() else {
        return None;
    };
    let PatternElement::Relationship(relationship) = hop else {
        return None;
    };
    let (PatternElement::Node(source), PatternElement::Node(target)) = (source, target) else {
        return None;
    };
    if relationship.length.is_some() {
        return None;
    }
    // An element's WHERE is placed by what it can read at its position in the
    // written order; reversing the hop would move it. Declined rather than
    // re-derived, as an inline map that cannot move is.
    if source.predicate.is_some()
        || relationship.predicate.is_some()
        || target
            .predicate
            .as_ref()
            .is_some_and(|predicate| !expression_bindings(predicate).is_some_and(|reads| reads.is_subset(bound)))
    {
        return None;
    }
    let is_bound = |node: &NodePattern| {
        node.variable
            .as_ref()
            .is_some_and(|variable| bound.contains(&Symbol::from(variable.as_str())))
    };
    if is_bound(source) || !is_bound(target) {
        return None;
    }
    // Moving a map changes *when* it is evaluated as well as what it can
    // read, so a map that can fail is not moved at all. Every leaf yields a
    // value or null; arithmetic divides by zero and overflows.
    if !map_is_total(target.properties.as_ref()) || !map_is_total(source.properties.as_ref()) {
        return None;
    }
    // Only what this hop itself produces can make a map read differently on
    // the far side of the move; see the note above. A name an earlier clause
    // already bound is in the seed row either way, even when this pattern
    // writes it again.
    let hop_binding: BTreeSet<Symbol> = relationship
        .variable
        .as_ref()
        .map(|variable| Symbol::from(variable.as_str()))
        .filter(|binding| !bound.contains(binding))
        .into_iter()
        .collect();
    let mut produced = hop_binding.clone();
    if let Some(variable) = &source.variable {
        produced.insert(Symbol::from(variable.as_str()));
    }
    if reads_any(target.properties.as_ref(), &produced) || reads_any(source.properties.as_ref(), &hop_binding) {
        return None;
    }
    let mut reversed = relationship.clone();
    // Preserve the bound target's WHERE after expansion. Moving it onto the
    // reversed anchor would evaluate it even when no edge exists.
    reversed.predicate = target.predicate.clone();
    let mut anchor = target.clone();
    anchor.predicate = None;
    reversed.direction = match relationship.direction {
        Direction::Outgoing => Direction::Incoming,
        Direction::Incoming => Direction::Outgoing,
        Direction::LeftOrUndirected => Direction::UndirectedOrRight,
        Direction::UndirectedOrRight => Direction::LeftOrUndirected,
        // These already mean the same either way round.
        direction @ (Direction::Undirected | Direction::Bidirectional | Direction::UndirectedEdge) => direction,
    };
    Some(vec![
        PatternElement::Node(anchor),
        PatternElement::Relationship(reversed),
        PatternElement::Node(source.clone()),
    ])
}

/// Whether every value in an inline property map can only evaluate to a value
/// or null, never to an error. The planner's `total_value` draws the same line
/// one stage later: the leaves qualify, and every compound value expression is
/// arithmetic or a call, which divides by zero, overflows, or rejects its
/// argument.
///
/// Reversing moves a map across the expansion, so it is evaluated over a
/// different set of rows. For a total map that only changes cost. For one that
/// can fail it decides whether the query errors at all, which is not a
/// difference a cost rewrite may introduce.
fn map_is_total(properties: Option<&Expression>) -> bool {
    let Some(Expression::Map(entries)) = properties else {
        // A non-map is rejected by `apply_pattern_properties` anyway; leave
        // that error to it rather than reversing around it here.
        return properties.is_none();
    };
    entries.iter().all(|(_, value)| total_map_value(value))
}

/// Whether one inline map value can only evaluate to a value or null.
fn total_map_value(value: &Expression) -> bool {
    match value {
        Expression::Literal(_)
        | Expression::Parameter(_)
        | Expression::SessionUser
        | Expression::ValueKeyword { .. }
        | Expression::TemporalLiteral { .. }
        | Expression::Identifier(_) => true,
        Expression::Property { target, .. } => matches!(**target, Expression::Identifier(_)),
        // A sign on an integer literal is how `-1` is written: the literal is
        // within range by the time it parses, so negating it cannot fail. Not
        // any literal -- the executor takes a sign on an integer and nothing
        // else, so `-true` and `-'text'` raise. And not a sign on a
        // non-literal, which is arithmetic over a value this cannot see.
        Expression::Unary {
            operator: UnaryOperator::Plus | UnaryOperator::Minus,
            expression,
        } => matches!(**expression, Expression::Literal(Literal::Integer(_))),
        _ => false,
    }
}

/// Whether an inline property map reads any of `forbidden`, or cannot be read
/// at all. A map this crate has not parsed counts as reading everything, so
/// the caller declines instead of guessing.
fn reads_any(properties: Option<&Expression>, forbidden: &BTreeSet<Symbol>) -> bool {
    let Some(properties) = properties else {
        return false;
    };
    match expression_bindings(properties) {
        Some(reads) => !reads.is_disjoint(forbidden),
        None => true,
    }
}

/// Every binding an expression reads, or `None` when it holds a syntax
/// fragment this crate has not parsed and so cannot answer for. Callers weigh
/// an unreadable expression as reading something unknown.
fn expression_bindings(expression: &Expression) -> Option<BTreeSet<Symbol>> {
    let mut bindings = BTreeSet::new();
    collect_expression_bindings(expression, &mut bindings).then_some(bindings)
}

/// Walks `expression`, collecting binding names. Returns false as soon as it
/// meets a fragment it cannot read, leaving `into` partly filled: the caller
/// discards it.
fn collect_expression_bindings(expression: &Expression, into: &mut BTreeSet<Symbol>) -> bool {
    collect_scoped_bindings(expression, &BTreeSet::new(), into).is_ok()
}

fn collect_scoped_bindings(expression: &Expression, local: &BTreeSet<Symbol>, into: &mut BTreeSet<Symbol>) -> Result<(), ()> {
    if is_count_star(expression) {
        return Ok(());
    }
    match expression {
        Expression::Identifier(identifier) => {
            let name = Symbol::from(identifier.as_str());
            if !local.contains(&name) {
                into.insert(name);
            }
            Ok(())
        }
        // What a subquery reads is checked when it is lowered.
        Expression::Exists(_) | Expression::Value(_) | Expression::Syntax(_) => Err(()),
        other => try_for_each_scoped_child(other, local, |child, local| collect_scoped_bindings(child, local, into)),
    }
}

/// The scan a node pattern starts from. Its labels only: the inline map and
/// any `WHERE` it carries are applied by `lower_path`, which knows what else
/// the path has bound by then.
fn lower_node_scan(node: &NodePattern, synthesized: &mut BTreeSet<Symbol>) -> GraphPlanResult<GraphLogicalPlan> {
    let binding = node_symbol(node, synthesized)?;
    Ok(GraphLogicalPlan::NodeScan {
        binding,
        labels: node.labels.iter().map(|label| Symbol::from(label.as_str())).collect(),
    })
}

/// The name a node pattern is known by, synthesizing one when it has none.
///
/// `(a)-[:R]->()-[:R]->(c)` names its middle node nowhere, but the two hops
/// still have to meet there: without a shared name they cross instead of
/// joining, and the query answers a cartesian product of the two hops. The
/// name is invented on the same terms as an inline relationship's -- reserved
/// prefix, checked for collisions with the query's own variables -- and since
/// nothing can spell it, nothing downstream can read it.
fn node_symbol(node: &NodePattern, synthesized: &mut BTreeSet<Symbol>) -> GraphPlanResult<Symbol> {
    if let Some(binding) = node.variable.as_ref() {
        return Ok(Symbol::from(binding.as_str()));
    }
    let index = synthesized
        .iter()
        .filter(|name| name.as_str().starts_with(ANONYMOUS_NODE_PREFIX))
        .count();
    let name = Symbol::from(format!("{ANONYMOUS_NODE_PREFIX}{index}").as_str());
    synthesized.insert(name.clone());
    Ok(name)
}

/// Which kind of pattern element an inline property map belongs to. It
/// decides what `{id: ...}` means; see [`resolve_relationship_ids`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PatternElementKind {
    Node,
    Relationship,
}

fn apply_pattern_properties(
    plan: GraphLogicalPlan,
    binding: &Symbol,
    properties: Option<&Expression>,
    kind: PatternElementKind,
) -> GraphPlanResult<GraphLogicalPlan> {
    let Some(properties) = properties else {
        return Ok(plan);
    };
    let Expression::Map(entries) = properties else {
        return Err(GraphPlanError::UnsupportedCypher(
            QueryFailureReason::Pattern,
            "pattern properties must be a map".to_string(),
        ));
    };
    let mut predicate = None;
    for (property, value) in entries {
        // Resolve the managed identity alias only after the source layout is known.
        let left = if property.as_str() == ID_PROPERTY && kind == PatternElementKind::Node {
            LogicalExpression::Function {
                function: ScalarFunction::IdProperty,
                arguments: vec![LogicalExpression::Binding(binding.clone())],
            }
        } else {
            LogicalExpression::Property {
                binding: binding.clone(),
                property: Symbol::from(property.as_str()),
            }
        };
        let equality = LogicalExpression::Binary {
            left: Box::new(left),
            operator: LogicalBinaryOperator::Equal,
            right: Box::new(lower_expression(value)?),
        };
        predicate = Some(match predicate {
            None => equality,
            Some(previous) => LogicalExpression::Binary {
                left: Box::new(previous),
                operator: LogicalBinaryOperator::And,
                right: Box::new(equality),
            },
        });
    }
    Ok(match predicate {
        Some(predicate) => GraphLogicalPlan::Filter {
            operation: hydradb_cypher_ast::operation::MATCH,
            input: Box::new(plan),
            predicate,
        },
        None => plan,
    })
}

/// The prefix `lower_path` synthesizes relationship names under. A
/// relationship with inline properties and no variable of its own needs a name
/// to hang the property filter on, and that name shares the row with the
/// caller's own.
const INLINE_RELATIONSHIP_PREFIX: &str = "__hydradb_inline_relationship_";

/// The prefix `node_symbol` synthesizes anonymous node names under. Same
/// contract as [`INLINE_RELATIONSHIP_PREFIX`]: reserved, collision-checked,
/// and unspellable by a query.
const ANONYMOUS_NODE_PREFIX: &str = "__hydradb_anonymous_node_";

/// A RETURN's items as the query wrote them, `*` aside.
fn returned_items_as_written(returned: &ReturnClause) -> Vec<Projection> {
    let (ProjectionItems::All(items) | ProjectionItems::Items(items)) = &returned.items;
    items.clone()
}

/// Refuse a query whose own variables collide with a name the lowering
/// synthesized for it.
///
/// Compares the names actually produced, not the shape they are spelled in. A
/// query that synthesizes nothing collides with nothing, however its variables
/// are spelled, and one that synthesizes `_0` still leaves `_1` free.
/// Reserving the shape instead refused
/// `MATCH (__hydradb_inline_relationship_0) RETURN ...`, which produces no
/// synthesized name at all and so has nothing to collide with.
///
/// A real collision is refused rather than renamed around: the two names would
/// share one row slot, the node would be seeded with a relationship and join
/// with nothing, and the query would answer empty -- silently, and only for
/// patterns that synthesize that many names.
fn reject_synthesized_collisions(declared: &BTreeSet<Symbol>, synthesized: &BTreeSet<Symbol>) -> GraphPlanResult<()> {
    if let Some(collision) = declared.intersection(synthesized).next() {
        return Err(GraphPlanError::UnsupportedCypher(
            QueryFailureReason::Pattern,
            format!("{collision} is reserved for a relationship this query names itself"),
        ));
    }
    Ok(())
}

/// GQL's `UNKNOWN` and `SESSION_USER` written bare: the variable of that name
/// when `scope` holds one (in any case, as Cypher's variables of those names
/// were matched before), otherwise the value.
fn resolve_value_keywords(expression: &Expression, scope: &BTreeSet<Symbol>) -> Expression {
    match expression {
        Expression::ValueKeyword { keyword, spelling } => match keyword_variable(spelling.as_str(), scope) {
            Some(name) => Expression::Identifier(Identifier::from(name.as_str())),
            None => keyword.value(),
        },
        // A LET body sees the names its bindings declare.
        Expression::Let { bindings, body } => {
            let mut inner = scope.clone();
            inner.extend(bindings.iter().map(|(name, _)| Symbol::from(name.as_str())));
            Expression::Let {
                bindings: bindings
                    .iter()
                    .map(|(name, value)| (name.clone(), resolve_value_keywords(value, scope)))
                    .collect(),
                body: Box::new(resolve_value_keywords(body, &inner)),
            }
        }
        other => other.map_children(&mut |child| resolve_value_keywords(child, scope)),
    }
}

/// The variable a value keyword written as `spelling` names in `scope`: the
/// name as spelled first, then in any case, for Cypher's sake.
fn keyword_variable<'a>(spelling: &str, scope: &'a BTreeSet<Symbol>) -> Option<&'a Symbol> {
    scope
        .get(&Symbol::from(spelling))
        .or_else(|| scope.iter().find(|name| name.as_str().eq_ignore_ascii_case(spelling)))
}

/// A path's value keywords resolved against `scope`: its elements, its WHERE,
/// and every operand of a union it is, with the operand's own WHERE.
fn resolve_path_keywords(path: &mut PathPattern, scope: &BTreeSet<Symbol>) {
    for element in &mut path.elements {
        let (properties, predicate) = match element {
            PatternElement::Node(node) => (&mut node.properties, &mut node.predicate),
            PatternElement::Relationship(relationship) => (&mut relationship.properties, &mut relationship.predicate),
        };
        for expression in properties.iter_mut().chain(predicate.iter_mut()) {
            *expression = resolve_value_keywords(expression, scope);
        }
    }
    if let Some(predicate) = &mut path.predicate {
        *predicate = resolve_value_keywords(predicate, scope);
    }
    if let Some(alternatives) = &mut path.alternatives {
        // §16.7 Syntax Rule 11: an operand sees its own variables and those
        // from outside the union, not another operand's, as it is lowered.
        let declared: BTreeSet<Symbol> = alternatives
            .operands
            .iter()
            .flat_map(|operand| path_bindings(&operand.path).into_iter().chain(path_variables(&operand.path)))
            .collect();
        for operand in &mut alternatives.operands {
            let own: BTreeSet<Symbol> = path_bindings(&operand.path)
                .into_iter()
                .chain(path_variables(&operand.path))
                .collect();
            let operand_scope: BTreeSet<Symbol> = scope
                .iter()
                .filter(|name| own.contains(*name) || !declared.contains(*name))
                .cloned()
                .collect();
            resolve_path_keywords(&mut operand.path, &operand_scope);
            if let Some(predicate) = &mut operand.predicate {
                *predicate = resolve_value_keywords(predicate, &operand_scope);
            }
        }
    }
}

/// Resolve each expression against its incoming, MATCH or projected scope.
fn resolve_clause_keywords(clause: &Clause, visible: &BTreeSet<Symbol>, record: &BTreeSet<Symbol>) -> Clause {
    let resolve = |expression: &mut Expression, scope: &BTreeSet<Symbol>| *expression = resolve_value_keywords(expression, scope);
    let mut clause = clause.clone();
    match &mut clause {
        Clause::Match(matched) => {
            let mut scope = visible.clone();
            scope.extend(clause_bindings(matched));
            scope.extend(
                matched
                    .patterns
                    .iter()
                    .filter_map(|path| path.binding.as_ref())
                    .map(|name| Symbol::from(name.as_str())),
            );
            scope.extend(clause_path_names(matched));
            for path in &mut matched.patterns {
                resolve_path_keywords(path, &scope);
            }
            if let Some(predicate) = &mut matched.predicate {
                resolve(predicate, &scope);
            }
        }
        Clause::With(with) => {
            let mut projected = resolve_projection_keywords(&mut with.items, &mut with.group_by, visible);
            projected.extend(record.iter().cloned());
            for sort in &mut with.order_by {
                resolve(&mut sort.expression, &projected);
            }
            for expression in with.predicate.iter_mut().chain(with.skip.iter_mut()).chain(with.limit.iter_mut()) {
                resolve(expression, &projected);
            }
        }
        Clause::Unwind(unwind) => resolve(&mut unwind.expression, visible),
        Clause::Let(binding) => {
            for (_, value) in &mut binding.bindings {
                resolve(value, visible);
            }
        }
        Clause::Filter(filter) => resolve(&mut filter.predicate, visible),
        Clause::OrderByAndPage(page) => {
            for sort in &mut page.order_by {
                resolve(&mut sort.expression, visible);
            }
            for expression in page.skip.iter_mut().chain(&mut page.limit) {
                resolve(expression, visible);
            }
        }
        Clause::Return(returned) => *returned = resolve_return_keywords(returned, visible),
        // An OPTIONAL block's clauses are lowered as a query of their own,
        // which resolves them against the scope it is given.
        // So is an inline procedure's query, against the names it imports.
        Clause::Create(_) | Clause::Merge(_) | Clause::Set(_) | Clause::Remove(_) | Clause::Delete(_) | Clause::Foreach(_) => {}
        Clause::OptionalBlock(_) | Clause::Call(_) | Clause::Finish | Clause::Next | Clause::Syntax(_) => {}
    }
    clause
}

/// A RETURN with its value keywords resolved: its items against `visible`,
/// its sort keys against those and the items' names.
fn resolve_return_keywords(returned: &ReturnClause, visible: &BTreeSet<Symbol>) -> ReturnClause {
    let (ProjectionItems::All(written) | ProjectionItems::Items(written)) = &returned.items;
    let written = written.clone();
    let mut returned = returned.clone();
    let projected = resolve_projection_keywords(&mut returned.items, &mut returned.group_by, visible);
    let mut unrestricted = visible.clone();
    unrestricted.extend(projected.iter().cloned());
    // After DISTINCT or an aggregation a key reads the items and the grouping
    // keys' names, outside its aggregates (`check_restricted_sort_keys`).
    let restricted = matches!(returned.quantifier, Some(SetQuantifier::Distinct))
        || !returned.group_by.is_empty()
        || written.iter().any(|item| ast_contains_aggregate(&item.expression));
    let mut readable = projected.clone();
    for key in &returned.group_by {
        let mut names = Vec::new();
        references_outside_aggregates(key, &mut names);
        readable.extend(names.into_iter().map(|name| Symbol::from(name.as_str())));
    }
    let (ProjectionItems::All(items) | ProjectionItems::Items(items)) = &returned.items;
    let items = items.clone();
    for sort in &mut returned.order_by {
        // A key that repeats an item reads as the item does.
        let unrestricted_key = resolve_value_keywords(&sort.expression, &unrestricted);
        sort.expression = if !restricted || items.iter().any(|item| item.expression == unrestricted_key) {
            unrestricted_key
        } else {
            resolve_sort_keywords(&sort.expression, &readable, visible)
        };
    }
    for expression in returned.skip.iter_mut().chain(returned.limit.iter_mut()) {
        *expression = resolve_value_keywords(expression, visible);
    }
    returned
}

/// Resolve a projection's items against `visible` and its grouping keys
/// against `visible` and the items' names, and return the names it projects:
/// all a WITH's WHERE and sort keys can read.
fn resolve_projection_keywords(items: &mut ProjectionItems, group_by: &mut [Expression], visible: &BTreeSet<Symbol>) -> BTreeSet<Symbol> {
    let star = matches!(items, ProjectionItems::All(_));
    let (ProjectionItems::All(items) | ProjectionItems::Items(items)) = items;
    // What the projection names: its items, and everything visible for `*`.
    let mut projected = if star { visible.clone() } else { BTreeSet::new() };
    for item in items.iter_mut() {
        item.expression = resolve_value_keywords(&item.expression, visible);
        match (&item.alias, &item.expression) {
            (Some(name), _) | (None, Expression::Identifier(name)) => {
                projected.insert(Symbol::from(name.as_str()));
            }
            _ => {}
        }
    }
    // A grouping key may name an item, so it sees the items' names too.
    let mut keyed = visible.clone();
    keyed.extend(projected.iter().cloned());
    for key in group_by.iter_mut() {
        *key = resolve_value_keywords(key, &keyed);
    }
    projected
}

/// A sort key's value keywords resolved against `outside`, except inside
/// its aggregates, which read the input rows (`inside`).
fn resolve_sort_keywords(expression: &Expression, outside: &BTreeSet<Symbol>, inside: &BTreeSet<Symbol>) -> Expression {
    match expression {
        Expression::Function { name, .. } if matches!(name.as_slice(), [name] if AggregateFunction::from_name(name.as_str()).is_some()) => {
            resolve_value_keywords(expression, inside)
        }
        Expression::ValueKeyword { .. } => resolve_value_keywords(expression, outside),
        other => other.map_children(&mut |child| resolve_sort_keywords(child, outside, inside)),
    }
}

/// The node variables and the edge variables a MATCH clause's patterns
/// declare, apart.
fn clause_element_kinds(matched: &MatchClause) -> (BTreeSet<Symbol>, BTreeSet<Symbol>) {
    let (mut nodes, mut edges) = (BTreeSet::new(), BTreeSet::new());
    // Every operand of a path pattern union too: one operand's node is not
    // another's edge.
    for element in matched.patterns.iter().flat_map(|path| path.all_elements()) {
        match element {
            PatternElement::Node(node) => nodes.extend(node.variable.iter().map(|name| Symbol::from(name.as_str()))),
            PatternElement::Relationship(relationship) => {
                edges.extend(relationship.variable.iter().map(|name| Symbol::from(name.as_str())))
            }
        }
    }
    (nodes, edges)
}

/// The WITH a MATCH's YIELD is: the names `incoming` held, then the yielded
/// ones, each an element or path variable the clause declares (§16.3 Syntax
/// Rule 12 a) i) and ii)).
fn yield_projection(incoming: &BTreeSet<Symbol>, yielded: &[Identifier], matched: &MatchClause) -> GraphPlanResult<WithClause> {
    let mut declared = BTreeSet::new();
    for path in &matched.patterns {
        if let Some(factor) = &path.factor {
            declared.extend(super::factor::element_globals(
                factor,
                path.modes.iter().any(|mode| *mode != PathMode::Walk),
            )?);
        } else {
            declared.extend(path_bindings(path));
            declared.extend(iteration_names(path).iter().map(|name| Symbol::from(name.as_str())));
        }
        declared.extend(path.binding.iter().map(|name| Symbol::from(name.as_str())));
    }
    let mut names: Vec<Symbol> = Vec::new();
    for name in yielded {
        let name = Symbol::from(name.as_str());
        if names.contains(&name) {
            return Err(GraphPlanError::syntax(
                QueryFailureReason::Pattern,
                format!("YIELD names {name} twice"),
            ));
        }
        if !declared.contains(&name) {
            return Err(GraphPlanError::syntax(
                QueryFailureReason::Pattern,
                format!("YIELD can name only a variable its graph pattern declares, not {name}"),
            ));
        }
        names.push(name);
    }
    let items = incoming
        .iter()
        .filter(|name| !names.contains(name))
        .chain(names.iter())
        .map(|name| Projection {
            expression: Expression::Identifier(Identifier::from(name.as_str())),
            alias: None,
        })
        .collect();
    Ok(WithClause {
        table_only: false,
        quantifier: None,
        items: ProjectionItems::Items(items),
        group_by: Vec::new(),
        order_by: Vec::new(),
        skip: None,
        limit: None,
        predicate: None,
    })
}

fn clause_bindings(matched: &MatchClause) -> BTreeSet<Symbol> {
    matched.patterns.iter().flat_map(path_bindings).collect()
}

/// Every binding one path declares, nodes and relationships alike.
fn path_bindings(path: &PathPattern) -> BTreeSet<Symbol> {
    path.all_elements()
        .into_iter()
        .filter_map(|element| match element {
            PatternElement::Node(node) => node.variable.as_ref(),
            PatternElement::Relationship(relationship) => relationship.variable.as_ref(),
        })
        .map(|variable| Symbol::from(variable.as_str()))
        .collect()
}

/// Reject a predicate naming a binding no clause has declared. Cypher scopes
/// run left to right, so a name a later clause introduces is not in scope
/// here either. Without this a bare binding reads as null: `WHERE typo IS
/// NULL` would match every row instead of saying the query is wrong.
fn validate_declared_bindings(expression: &LogicalExpression, declared: &BTreeSet<Symbol>) -> GraphPlanResult<()> {
    let check = |binding: &Symbol| -> GraphPlanResult<()> {
        if declared.contains(binding) {
            return Ok(());
        }
        Err(GraphPlanError::invalid_reference(
            QueryFailureReason::Where,
            binding.as_str(),
            format!("unbound variable {binding}"),
        ))
    };
    match expression {
        LogicalExpression::Binding(binding) | LogicalExpression::Identity(binding) | LogicalExpression::Property { binding, .. } => {
            check(binding)
        }
        LogicalExpression::PathNodesDistinct { parts, .. } => parts.iter().try_for_each(|part| check(part.name())),
        LogicalExpression::ElementTest { binding, test } => {
            check(binding)?;
            test.edge().map_or(Ok(()), check)
        }
        LogicalExpression::Let { bindings, body } => {
            // §20.5 Syntax Rule 3 b).
            if let Some((name, _)) = bindings.iter().find(|(name, _)| declared.contains(name)) {
                return Err(GraphPlanError::syntax(
                    QueryFailureReason::Where,
                    format!("LET cannot rebind {name}, which is already in scope"),
                ));
            }
            bindings
                .iter()
                .try_for_each(|(_, value)| validate_declared_bindings(value, declared))?;
            let mut amended = declared.clone();
            amended.extend(bindings.iter().map(|(name, _)| name.clone()));
            validate_declared_bindings(body, &amended)
        }
        other => other
            .children()
            .into_iter()
            .try_for_each(|child| validate_declared_bindings(child, declared)),
    }
}

/// An exact numeric literal needs an exact numeric type that holds it
/// (§21.2 SR18). The supported integer representation spans INT64 to UINT64.
fn integer_literal(value: i128) -> GraphPlanResult<i128> {
    if !(i128::from(i64::MIN)..=i128::from(u64::MAX)).contains(&value) {
        return Err(GraphPlanError::syntax(
            QueryFailureReason::Evaluation,
            format!("integer literal {value} is outside the supported exact integer range"),
        ));
    }
    Ok(value)
}

/// An approximate numeric literal must be within FLOAT's range (§21.2 Syntax
/// Rule 20).
fn float_literal(spelling: &str) -> GraphPlanResult<ScalarValue> {
    if !spelling.parse::<f64>().is_ok_and(f64::is_finite) {
        return Err(GraphPlanError::syntax(
            QueryFailureReason::Evaluation,
            format!("float literal {spelling} is outside the FLOAT range"),
        ));
    }
    Ok(ScalarValue::Float(spelling.into()))
}

fn lower_expression(expression: &Expression) -> GraphPlanResult<LogicalExpression> {
    Ok(match expression {
        Expression::Identifier(identifier) => LogicalExpression::Binding(Symbol::from(identifier.as_str())),
        // `list[index]` evaluates as the internal `ListElement` function, so
        // planning, binding and EXPLAIN treat it as any other scalar call.
        Expression::Index { target, index } => LogicalExpression::Function {
            function: ScalarFunction::ListElement,
            arguments: vec![lower_expression(target)?, lower_expression(index)?],
        },
        Expression::Property { target, key } => {
            if matches!(target.as_ref(), Expression::Literal(_)) {
                // A literal is never a node, an edge or a record (§20.1
                // Syntax Rules 4 and 5).
                return Err(GraphPlanError::syntax(
                    QueryFailureReason::Return,
                    format!("only a node, an edge or a record has properties, so .{key} cannot follow a literal"),
                ));
            }
            let Expression::Identifier(binding) = target.as_ref() else {
                // §20.11: a field of the record the expression gives.
                return Ok(LogicalExpression::Field {
                    expression: Box::new(lower_expression(target)?),
                    field: Symbol::from(key.as_str()),
                });
            };
            let binding = Symbol::from(binding.as_str());
            if key.as_str() == ID_PROPERTY {
                LogicalExpression::Function {
                    function: ScalarFunction::IdProperty,
                    arguments: vec![LogicalExpression::Binding(binding)],
                }
            } else {
                LogicalExpression::Property {
                    binding,
                    property: Symbol::from(key.as_str()),
                }
            }
        }
        Expression::Parameter(parameter) => LogicalExpression::Parameter(Symbol::from(parameter.as_str())),
        // Bound per request from the session (`LoweredTemplate::bind_as`).
        Expression::SessionUser => LogicalExpression::Parameter(Symbol::from(crate::SESSION_USER_PARAMETER)),
        // A value keyword no clause resolved to a variable is its value.
        Expression::ValueKeyword { keyword, .. } => return lower_expression(&keyword.value()),
        // §21.2 Syntax Rules 59, 62, 64, 68 and 71: the string conforms to
        // its type's ISO 8601 form; a duration of both unit groups raises
        // 22G0H.
        Expression::TemporalLiteral { kind, text } => {
            use crate::temporal::{iso_text, parse_date, parse_datetime, parse_duration, parse_time};
            use crate::temporal::{parse_sql_date, parse_sql_time, parse_sql_timestamp, sql_interval};
            use hydradb_cypher_ast::TemporalLiteralKind;
            let malformed = || GraphPlanError::syntax(QueryFailureReason::Other, format!("'{text}' is not a {kind:?} literal's string"));
            // §21.2 Syntax Rule 69, GL12: a string in SQL's form instead is
            // read as its ISO 8601 equivalent.
            let (function, text): (ScalarFunction, Box<str>) = match kind {
                TemporalLiteralKind::Date => match (parse_date(text), parse_sql_date(text)) {
                    (Some(_), _) => (ScalarFunction::Date, text.clone()),
                    (None, Some(date)) => (ScalarFunction::Date, iso_text(Some(date), None).into()),
                    (None, None) => return Err(malformed()),
                },
                TemporalLiteralKind::Time => {
                    let (function, iso) = match (parse_time(text), parse_sql_time(text)) {
                        (Some(time), _) => (time.1, text.clone()),
                        (None, Some(time)) => (time.1, iso_text(None, Some(time)).into()),
                        (None, None) => return Err(malformed()),
                    };
                    (
                        if function.is_some() {
                            ScalarFunction::ZonedTime
                        } else {
                            ScalarFunction::LocalTime
                        },
                        iso,
                    )
                }
                TemporalLiteralKind::Datetime | TemporalLiteralKind::Timestamp => {
                    let sql = matches!(kind, TemporalLiteralKind::Timestamp)
                        .then(|| parse_sql_timestamp(text))
                        .flatten();
                    let (offset, iso) = match (parse_datetime(text), sql) {
                        (Some((_, offset)), _) => (offset, text.clone()),
                        (None, Some((datetime, offset))) => {
                            (offset, iso_text(Some(datetime.date()), Some((datetime.time(), offset))).into())
                        }
                        (None, None) => return Err(malformed()),
                    };
                    (
                        if offset.is_some() {
                            ScalarFunction::ZonedDatetime
                        } else {
                            ScalarFunction::LocalDatetime
                        },
                        iso,
                    )
                }
                // §21.2 Syntax Rules 70 and 72, GL12: a SQL interval literal
                // is the duration literal of the same fields.
                TemporalLiteralKind::Interval(qualifier) => {
                    let duration = sql_interval(text, *qualifier).ok_or_else(|| {
                        GraphPlanError::syntax(
                            QueryFailureReason::Other,
                            format!("'{}' is not an interval string of its qualifier", text.get(1..).unwrap_or_default()),
                        )
                    })?;
                    (ScalarFunction::Duration, duration.into())
                }
                TemporalLiteralKind::Duration => {
                    if parse_duration(text).is_err() {
                        return Err(GraphPlanError::Rejected {
                            status: GqlStatus::INVALID_DURATION_FORMAT,
                            reason: QueryFailureReason::Other,
                            message: format!("'{text}' is not a duration of one unit group"),
                            reference: None,
                        });
                    }
                    (ScalarFunction::Duration, text.clone())
                }
            };
            LogicalExpression::Function {
                function,
                arguments: vec![
                    LogicalExpression::Literal(ScalarValue::String(text)),
                    LogicalExpression::Parameter(Symbol::from(crate::REQUEST_TIMESTAMP_PARAMETER)),
                ],
            }
        }
        Expression::Literal(literal) => LogicalExpression::Literal(match literal {
            Literal::Null => ScalarValue::Null,
            Literal::Boolean(value) => ScalarValue::Boolean(*value),
            Literal::Integer(value) => ScalarValue::Integer(integer_literal(*value)?),
            Literal::Float(value) => float_literal(value)?,
            Literal::String(value) => ScalarValue::String(value.clone()),
            // §21.2: an exact numeric literal holds no more digits than a
            // decimal (ID034).
            Literal::Decimal(value) => ScalarValue::Decimal(crate::decimal::parse(value).ok_or_else(|| {
                GraphPlanError::syntax(
                    QueryFailureReason::Other,
                    format!("{value}M has more than {} digits", crate::decimal::MAXIMUM_DIGITS),
                )
            })?),
            Literal::Bytes(value) => ScalarValue::Bytes(value.clone()),
        }),
        Expression::Unary { operator, expression } => {
            let truth = |function: ScalarFunction, negated: bool| -> GraphPlanResult<LogicalExpression> {
                let test = LogicalExpression::Function {
                    function,
                    arguments: vec![lower_expression(expression)?],
                };
                Ok(if negated {
                    LogicalExpression::Unary {
                        operator: LogicalUnaryOperator::Not,
                        expression: Box::new(test),
                    }
                } else {
                    test
                })
            };
            match operator {
                UnaryOperator::IsTrue => return truth(ScalarFunction::IsTrue, false),
                UnaryOperator::IsNotTrue => return truth(ScalarFunction::IsTrue, true),
                UnaryOperator::IsFalse => return truth(ScalarFunction::IsFalse, false),
                UnaryOperator::IsNotFalse => return truth(ScalarFunction::IsFalse, true),
                UnaryOperator::IsUnknown => return truth(ScalarFunction::IsUnknown, false),
                UnaryOperator::IsNotUnknown => return truth(ScalarFunction::IsUnknown, true),
                _ => {}
            }
            if let (UnaryOperator::Minus, Expression::Literal(Literal::Integer(value))) = (operator, &**expression) {
                let signed = value.checked_neg().ok_or_else(|| {
                    GraphPlanError::syntax(
                        QueryFailureReason::Evaluation,
                        "the signed literal is outside the supported exact integer range",
                    )
                })?;
                return Ok(LogicalExpression::Literal(ScalarValue::Integer(integer_literal(signed)?)));
            }
            LogicalExpression::Unary {
                operator: match operator {
                    UnaryOperator::Not => LogicalUnaryOperator::Not,
                    UnaryOperator::Plus => LogicalUnaryOperator::Plus,
                    UnaryOperator::Minus => LogicalUnaryOperator::Minus,
                    UnaryOperator::IsNull => LogicalUnaryOperator::IsNull,
                    UnaryOperator::IsNotNull => LogicalUnaryOperator::IsNotNull,
                    _ => unreachable!("truth tests are lowered above"),
                },
                expression: Box::new(lower_expression(expression)?),
            }
        }
        // §20.5 GE03, Syntax Rule 4: the names are distinct.
        Expression::Let { bindings, body } => {
            let mut lowered: Vec<(Symbol, LogicalExpression)> = Vec::new();
            for (name, value) in bindings {
                let name = Symbol::from(name.as_str());
                if lowered.iter().any(|(existing, _)| *existing == name) {
                    return Err(GraphPlanError::syntax(
                        QueryFailureReason::Return,
                        format!("LET binds {name} twice"),
                    ));
                }
                lowered.push((name, lower_expression(value)?));
            }
            let body = lower_expression(body)?;
            if lowered.iter().any(|(_, value)| contains_aggregate(value)) || contains_aggregate(&body) {
                return Err(GraphPlanError::UnsupportedCypher(
                    QueryFailureReason::Return,
                    "an aggregate inside LET ... IN ... END is not supported".to_string(),
                ));
            }
            LogicalExpression::Let {
                bindings: lowered,
                body: Box::new(body),
            }
        }
        // §20.14 GE06.
        Expression::PathConstructor(elements) => {
            LogicalExpression::PathConstructor(elements.iter().map(lower_expression).collect::<GraphPlanResult<_>>()?)
        }
        // §20.18 GV45: a record constructor, its field names distinct
        // (§20.19 Syntax Rule 2).
        Expression::Map(entries) => {
            let mut fields: Vec<(Symbol, LogicalExpression)> = Vec::new();
            for (name, value) in entries {
                let name = Symbol::from(name.as_str());
                if fields.iter().any(|(existing, _)| *existing == name) {
                    return Err(GraphPlanError::syntax(
                        QueryFailureReason::Other,
                        format!("a record names the field {name} twice"),
                    ));
                }
                fields.push((name, lower_expression(value)?));
            }
            fields.sort_by(|left, right| left.0.cmp(&right.0));
            LogicalExpression::Record(fields)
        }
        // §19.6 GA06.
        Expression::TypeTest {
            expression,
            value_type,
            negated,
        } => LogicalExpression::TypeTest {
            expression: Box::new(lower_expression(expression)?),
            value_type: resolve_value_type(value_type)?,
            negated: *negated,
        },
        // §20.8 GA05, Syntax Rules 4 and 5: the target has material values,
        // and includes the null value when the operand is NULL.
        Expression::Cast { expression, value_type } => {
            let value_type = resolve_value_type(value_type)?;
            if value_type.is_immaterial() {
                return Err(GraphPlanError::syntax(
                    QueryFailureReason::Other,
                    "CAST is to a type with material values, not NULL or NOTHING".to_string(),
                ));
            }
            if matches!(**expression, Expression::Literal(Literal::Null)) && !value_type.includes_null() {
                return Err(GraphPlanError::syntax(
                    QueryFailureReason::Other,
                    "CAST of NULL is to a type that includes the null value".to_string(),
                ));
            }
            let request = value_type
                .reads_current_date()
                .then(|| Box::new(LogicalExpression::Parameter(Symbol::from(crate::REQUEST_TIMESTAMP_PARAMETER))));
            LogicalExpression::Cast {
                expression: Box::new(lower_expression(expression)?),
                value_type,
                request,
            }
        }
        Expression::Normalized { expression, form, negated } => {
            let test = LogicalExpression::Function {
                function: ScalarFunction::IsNormalized,
                arguments: vec![
                    lower_expression(expression)?,
                    LogicalExpression::Literal(ScalarValue::String(form.clone())),
                ],
            };
            if *negated {
                LogicalExpression::Unary {
                    operator: LogicalUnaryOperator::Not,
                    expression: Box::new(test),
                }
            } else {
                test
            }
        }
        Expression::ElementTest { element, test, negated } => {
            let binding = element_variable(element)?;
            let tested = match test {
                ElementTest::Labeled(labels) => LogicalExpression::ElementTest {
                    binding,
                    test: ElementTestKind::Labeled(labels.clone()),
                },
                ElementTest::Directed => LogicalExpression::ElementTest {
                    binding,
                    test: ElementTestKind::Directed,
                },
                ElementTest::SourceOf(edge) => LogicalExpression::ElementTest {
                    binding,
                    test: ElementTestKind::SourceOf(element_variable(edge)?),
                },
                ElementTest::DestinationOf(edge) => LogicalExpression::ElementTest {
                    binding,
                    test: ElementTestKind::DestinationOf(element_variable(edge)?),
                },
                ElementTest::PropertyExists(property) => LogicalExpression::ElementTest {
                    binding,
                    test: ElementTestKind::PropertyExists(Symbol::from(property.as_str())),
                },
            };
            if *negated {
                LogicalExpression::Unary {
                    operator: LogicalUnaryOperator::Not,
                    expression: Box::new(tested),
                }
            } else {
                tested
            }
        }
        // `IN` is matched ahead of the generic binary arm: it lowers to its own
        // node rather than to a binary operator, because its right side is a
        // list rather than a scalar expression.
        Expression::Binary {
            left,
            operator: BinaryOperator::In,
            right,
        } => lower_in_expression(left, right)?,
        Expression::Binary { left, operator, right } => LogicalExpression::Binary {
            left: Box::new(lower_expression(left)?),
            operator: match operator {
                BinaryOperator::Equal => LogicalBinaryOperator::Equal,
                BinaryOperator::NotEqual => LogicalBinaryOperator::NotEqual,
                BinaryOperator::LessThan => LogicalBinaryOperator::LessThan,
                BinaryOperator::LessThanOrEqual => LogicalBinaryOperator::LessThanOrEqual,
                BinaryOperator::GreaterThan => LogicalBinaryOperator::GreaterThan,
                BinaryOperator::GreaterThanOrEqual => LogicalBinaryOperator::GreaterThanOrEqual,
                BinaryOperator::And => LogicalBinaryOperator::And,
                BinaryOperator::Or => LogicalBinaryOperator::Or,
                // §20.20 Syntax Rule 4: one operator, not the expansion the rule
                // defines it by, which would copy both operands every time.
                BinaryOperator::Xor => LogicalBinaryOperator::Xor,
                BinaryOperator::Add => LogicalBinaryOperator::Add,
                BinaryOperator::Subtract => LogicalBinaryOperator::Subtract,
                BinaryOperator::Multiply => LogicalBinaryOperator::Multiply,
                BinaryOperator::Divide => LogicalBinaryOperator::Divide,
                BinaryOperator::Modulo => LogicalBinaryOperator::Modulo,
                BinaryOperator::StartsWith => LogicalBinaryOperator::StartsWith,
                BinaryOperator::EndsWith => LogicalBinaryOperator::EndsWith,
                BinaryOperator::Contains => LogicalBinaryOperator::Contains,
                BinaryOperator::Concatenate => LogicalBinaryOperator::Concatenate,
                // `In` is handled by the arm above; reaching it here would
                // mean the match order changed.
                BinaryOperator::In | BinaryOperator::Power => {
                    return Err(GraphPlanError::UnsupportedCypher(
                        QueryFailureReason::Other,
                        format!("binary operator {operator:?} is not lowered yet"),
                    ))
                }
            },
            right: Box::new(lower_expression(right)?),
        },
        Expression::Function { name, distinct, arguments } => lower_function(name, *distinct, arguments)?,
        Expression::List(items) => LogicalExpression::List(items.iter().map(lower_expression).collect::<GraphPlanResult<Vec<_>>>()?),
        Expression::Case {
            operand,
            branches,
            default,
        } => LogicalExpression::Case {
            operand: operand.as_deref().map(lower_expression).transpose()?.map(Box::new),
            branches: branches
                .iter()
                .map(|(when, then)| Ok((lower_expression(when)?, lower_expression(then)?)))
                .collect::<GraphPlanResult<Vec<_>>>()?,
            default: default.as_deref().map(lower_expression).transpose()?.map(Box::new),
        },
        _ => {
            return Err(GraphPlanError::UnsupportedCypher(
                QueryFailureReason::Other,
                "expression is outside the initial typed slice".to_string(),
            ))
        }
    })
}

/// A function call: an aggregate, which reads a group, or a scalar function,
/// which reads one row. Which one decides whether the projection around it
/// groups, so the two never share a representation.
fn lower_function(name: &[Identifier], distinct: bool, arguments: &[Expression]) -> GraphPlanResult<LogicalExpression> {
    let [name] = name else {
        return Err(GraphPlanError::UnsupportedCypher(
            QueryFailureReason::Return,
            "namespaced functions are not supported".to_string(),
        ));
    };
    let function_name = name.as_str().to_ascii_lowercase();
    if let Some(function) = AggregateFunction::from_name(&function_name) {
        let expression = match arguments {
            // The permissive parser accepts this spelling; reject it explicitly.
            [Expression::Syntax(fragment)] if function == AggregateFunction::Count && fragment.text() == "*" && distinct => {
                return Err(GraphPlanError::UnsupportedCypher(
                    QueryFailureReason::Return,
                    "count(DISTINCT *) is not supported; DISTINCT requires an expression".to_string(),
                ));
            }
            [Expression::Syntax(fragment)] if function == AggregateFunction::Count && fragment.text() == "*" && !distinct => None,
            [argument] if !function.takes_fraction() => Some(Box::new(lower_expression(argument)?)),
            [argument, _] if function.takes_fraction() => Some(Box::new(lower_expression(argument)?)),
            _ => {
                return Err(GraphPlanError::UnsupportedCypher(
                    QueryFailureReason::Return,
                    format!(
                        "function {name} requires exactly {} argument{}",
                        if function.takes_fraction() { "two" } else { "one" },
                        if function.takes_fraction() { "s" } else { "" }
                    ),
                ));
            }
        };
        // The fraction is read once for the group (§20.9 General Rule 7 b)),
        // so it may not read the group's rows.
        let fraction = match arguments {
            [_, fraction] if function.takes_fraction() => {
                if expression_bindings(fraction).is_none_or(|bindings| !bindings.is_empty()) {
                    return Err(GraphPlanError::UnsupportedCypher(
                        QueryFailureReason::Return,
                        format!("the fraction of {name} must be a literal or a parameter expression"),
                    ));
                }
                Some(Box::new(lower_expression(fraction)?))
            }
            _ => None,
        };
        return Ok(LogicalExpression::Aggregate {
            function,
            distinct,
            expression,
            fraction,
            group: None,
        });
    }

    // `TRIM(side c FROM s)` arrives as `trim(s, c, side)`; see the parser.
    if function_name == "trim" {
        if let [source, character, Expression::Literal(Literal::String(side))] = arguments {
            let function = match side.as_ref() {
                "LEADING" => ScalarFunction::TrimLeading,
                "TRAILING" => ScalarFunction::TrimTrailing,
                // `TRIM(FROM s)`: §20.24 Syntax Rule 3 needs a side or a
                // character before FROM.
                "" => {
                    return Err(GraphPlanError::syntax(
                        QueryFailureReason::Return,
                        "TRIM ... FROM needs LEADING, TRAILING, BOTH or a trim character".to_string(),
                    ))
                }
                _ => ScalarFunction::TrimBoth,
            };
            return Ok(LogicalExpression::Function {
                function,
                arguments: vec![lower_expression(source)?, lower_expression(character)?],
            });
        }
    }
    let Some(function) = ScalarFunction::from_name(&function_name) else {
        return Err(GraphPlanError::UnsupportedCypher(
            QueryFailureReason::Return,
            format!("function {name} is not supported"),
        ));
    };
    // DISTINCT only means something to a function that reads many values.
    if distinct {
        return Err(GraphPlanError::UnsupportedCypher(
            QueryFailureReason::Return,
            format!("DISTINCT is only valid in an aggregate, not in {name}"),
        ));
    }
    if !function.accepts_arity(arguments.len()) {
        return Err(GraphPlanError::UnsupportedCypher(
            QueryFailureReason::Return,
            format!("function {name} does not take {} arguments", arguments.len()),
        ));
    }
    // §20.10, §19.11 and §19.12: these take element variable references.
    if arguments.iter().enumerate().any(|(index, argument)| {
        function.argument_role(index) == ScalarArgumentRole::ElementVariable && !matches!(argument, Expression::Identifier(_))
    }) {
        return Err(GraphPlanError::syntax(
            QueryFailureReason::Return,
            format!("{name} takes element variables, not expressions"),
        ));
    }
    let mut arguments = arguments.iter().map(lower_expression).collect::<GraphPlanResult<Vec<_>>>()?;
    // §20.27 General Rule 1: a temporal function reads the request's
    // timestamp, set once for it.
    if function.is_temporal() {
        arguments.push(LogicalExpression::Parameter(Symbol::from(crate::REQUEST_TIMESTAMP_PARAMETER)));
    }
    Ok(LogicalExpression::Function { function, arguments })
}

/// A sort item's direction, and its null ordering when that differs from the
/// implicit one (IS001: nulls after every value, so last ascending and first
/// descending). `NULLS LAST` after `ASC` says nothing the default does not.
fn sort_order(item: &hydradb_cypher_ast::SortItem) -> (SortDirection, Option<NullOrdering>) {
    let direction = match item.direction.unwrap_or(OrderDirection::Ascending) {
        OrderDirection::Ascending => SortDirection::Ascending,
        OrderDirection::Descending => SortDirection::Descending,
    };
    let implicit = match direction {
        SortDirection::Ascending => NullOrdering::Last,
        SortDirection::Descending => NullOrdering::First,
    };
    (direction, item.nulls.filter(|nulls| *nulls != implicit))
}

/// The names a RETURN projection creates, which ORDER BY may read although
/// they are columns rather than bindings.
fn item_aliases(items: &[Projection]) -> BTreeSet<Symbol> {
    items
        .iter()
        .filter_map(|item| item.alias.as_ref().map(|alias| Symbol::from(alias.as_str())))
        .collect()
}
