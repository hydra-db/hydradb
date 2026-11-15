//! Two logical filters over one access merge into one, but only when the outer
//! predicate cannot fail.
//!
//! Merging is what lets the runtime equality probe see a `WHERE` clause's
//! narrow equality: `probe_replaceable_access` looks through a filter only to a
//! bare access, so a stack hides the outer predicate from it entirely.
//!
//! Merging is not free of meaning, though. Expression evaluation is eager --
//! `Binary` evaluates both operands before combining them -- so a conjunction
//! does not short-circuit. A stack discards a row the inner predicate rejects
//! before the outer one is evaluated on it; a merged `And` evaluates both. An
//! outer predicate that divides by zero, overflows, or rejects a non-string
//! would therefore fail a query that used to succeed, so those keep the stack.

use hydradb_cypher_engine::{
    plan_physical, GraphLogicalPlan, GraphPhysicalPlan, LogicalBinaryOperator, LogicalExpression, PhysicalPlanningContext, ScalarValue,
    Symbol,
};

fn property(binding: &str, property: &str) -> LogicalExpression {
    LogicalExpression::Property {
        binding: Symbol::from(binding),
        property: Symbol::from(property),
    }
}

fn binary(left: LogicalExpression, operator: LogicalBinaryOperator, right: LogicalExpression) -> LogicalExpression {
    LogicalExpression::Binary {
        left: Box::new(left),
        operator,
        right: Box::new(right),
    }
}

fn equals(binding: &str, name: &str, value: &str) -> LogicalExpression {
    binary(
        property(binding, name),
        LogicalBinaryOperator::Equal,
        LogicalExpression::Literal(ScalarValue::String(value.into())),
    )
}

/// `Filter(outer) <- Filter(tenant_id = 't') <- NodeScan(s:Source)`, the shape
/// a pattern's inline equalities plus a `WHERE` clause over one binding take.
fn stacked(outer: LogicalExpression) -> GraphLogicalPlan {
    GraphLogicalPlan::Filter {
        operation: hydradb_cypher_ast::operation::MATCH,
        input: Box::new(GraphLogicalPlan::Filter {
            operation: hydradb_cypher_ast::operation::MATCH,
            input: Box::new(GraphLogicalPlan::NodeScan {
                binding: Symbol::from("s"),
                labels: vec![Symbol::from("Source")],
            }),
            predicate: equals("s", "tenant_id", "t"),
        }),
        predicate: outer,
    }
}

fn plan(outer: LogicalExpression) -> GraphPhysicalPlan {
    plan_physical(&stacked(outer), &PhysicalPlanningContext::default()).expect("plan the filter stack")
}

/// Filters directly above the access, counted from the top.
fn stacked_filters(plan: &GraphPhysicalPlan) -> usize {
    let mut depth = 0;
    let mut node = plan;
    while let GraphPhysicalPlan::Filter { input, .. } = node {
        depth += 1;
        node = input;
    }
    depth
}

#[test]
fn a_total_outer_predicate_merges_into_one_filter() {
    // Comparisons against a literal cannot fail, so the probe gets to see
    // `app_external_id` -- the whole point of merging.
    let merged = plan(binary(
        equals("s", "app_provider", "p0"),
        LogicalBinaryOperator::And,
        equals("s", "app_external_id", "ext-1"),
    ));
    assert_eq!(stacked_filters(&merged), 1, "a total outer predicate should merge: {merged:?}");
}

#[test]
fn arithmetic_in_the_outer_predicate_keeps_the_stack() {
    // `s.num / s.den` divides by zero on rows the inner predicate rejects. The
    // stack never evaluates it on them; a merged `And` would.
    let fallible = binary(
        binary(property("s", "num"), LogicalBinaryOperator::Divide, property("s", "den")),
        LogicalBinaryOperator::GreaterThan,
        LogicalExpression::Literal(ScalarValue::Integer(0)),
    );
    let planned = plan(fallible);
    assert_eq!(
        stacked_filters(&planned),
        2,
        "arithmetic must not be lifted over the inner predicate: {planned:?}"
    );
}

#[test]
fn starts_with_in_the_outer_predicate_keeps_the_stack() {
    // `STARTS WITH` rejects a non-string operand rather than returning unknown.
    let planned = plan(binary(
        property("s", "name"),
        LogicalBinaryOperator::StartsWith,
        property("s", "prefix"),
    ));
    assert_eq!(
        stacked_filters(&planned),
        2,
        "STARTS WITH must not be lifted over the inner predicate: {planned:?}"
    );
}

#[test]
fn a_fallible_conjunct_keeps_the_stack_even_beside_a_total_one() {
    // One unsafe conjunct is enough: the merged `And` would evaluate it.
    let planned = plan(binary(
        equals("s", "app_provider", "p0"),
        LogicalBinaryOperator::And,
        binary(
            binary(property("s", "num"), LogicalBinaryOperator::Modulo, property("s", "den")),
            LogicalBinaryOperator::Equal,
            LogicalExpression::Literal(ScalarValue::Integer(0)),
        ),
    ));
    assert_eq!(stacked_filters(&planned), 2, "a fallible conjunct must keep the stack: {planned:?}");
}

#[test]
fn a_null_test_in_the_outer_predicate_merges() {
    // `IS NULL` is the test for unknown, so it is total by definition.
    let planned = plan(LogicalExpression::Unary {
        operator: hydradb_cypher_engine::LogicalUnaryOperator::IsNotNull,
        expression: Box::new(property("s", "app_external_id")),
    });
    assert_eq!(stacked_filters(&planned), 1, "a null test should merge: {planned:?}");
}
