//! A sorted-property walk must survive a predicate the index cannot prove.
//!
//! Real queries restrict something other than what they sort by: an inline
//! property map, a tenant equality, a keyset tie-break. Before the residual
//! those all abandoned the ordered walk, so the plan fell back to seeking the
//! broadest equality, hydrating every candidate and sorting the lot.

use hydradb_cypher_engine::{CypherEngine, ParameterMap, ScalarValue};

fn plan_of(query: &str) -> String {
    CypherEngine::new(())
        .prepare(
            query,
            &ParameterMap::from([
                ("t".to_string(), ScalarValue::String("t1".into())),
                ("s".to_string(), ScalarValue::String("s9".into())),
                ("after".to_string(), ScalarValue::String("2026-01-01".into())),
                ("limit".to_string(), ScalarValue::Integer(256)),
            ]),
        )
        .expect("prepare")
        .explain()
}

/// The shape that already worked: every conjunct is a bound on the sort key.
#[test]
fn a_predicate_the_index_proves_needs_no_residual() {
    let plan = plan_of(
        "MATCH (e:Entity) WHERE e.created_at STARTS WITH '2026' \
         RETURN e ORDER BY e.created_at LIMIT 10",
    );
    assert!(plan.contains("access=ordered-property"), "{plan}");
    assert!(plan.contains("residual=none"), "{plan}");
    assert!(!plan.contains("SortExec"), "{plan}");
}

/// An equality on another property used to abandon the walk outright.
#[test]
fn an_equality_on_another_property_becomes_residual() {
    let plan = plan_of(
        "MATCH (e:Entity) WHERE e.tenant_id = $t AND e.created_at STARTS WITH '2026' \
         RETURN e ORDER BY e.created_at LIMIT $limit",
    );
    assert!(plan.contains("access=ordered-property"), "{plan}");
    assert!(plan.contains("e.tenant_id = $t"), "{plan}");
    assert!(!plan.contains("SortExec"), "{plan}");
    assert!(!plan.contains("FilterExec"), "{plan}");
}

/// An inline property map lowers to its own Filter, so the sort sits over a
/// stack of them rather than over one.
#[test]
fn nested_filters_from_an_inline_map_are_peeled() {
    let plan = plan_of(
        "MATCH (e:Entity {tenant_id: $t, sub_tenant_id: $s}) \
         WHERE e.created_at STARTS WITH '' \
         RETURN e ORDER BY e.created_at LIMIT $limit",
    );
    assert!(plan.contains("access=ordered-property"), "{plan}");
    assert!(plan.contains("e.tenant_id = $t"), "{plan}");
    assert!(plan.contains("e.sub_tenant_id = $s"), "{plan}");
    assert!(!plan.contains("SortExec"), "{plan}");
}

/// The production query: inline map, a keyset disjunct, and a prefix. Neither
/// side of the disjunct restricts anything alone, so it travels whole.
#[test]
fn the_keyset_disjunct_travels_whole_into_the_residual() {
    let plan = plan_of(
        "MATCH (e:Entity {tenant_id: $t, sub_tenant_id: $s}) \
         WHERE (e.created_at > $after \
            OR (e.created_at = $after AND e.entity_id > $s)) \
           AND e.created_at STARTS WITH '' \
         RETURN e.entity_id AS entity_id \
         ORDER BY e.created_at LIMIT $limit",
    );
    println!("=== PRODUCTION PLAN ===\n{plan}");
    assert!(plan.contains("access=ordered-property"), "{plan}");
    assert!(!plan.contains("SortExec"), "{plan}");
}

/// An equality on the sorted property is a better access path than a walk:
/// seeking those values returns the same rows already in order.
#[test]
fn an_equality_on_the_sorted_property_keeps_the_seek() {
    let plan = plan_of(
        "MATCH (e:Entity) WHERE e.entity_id IN ['b','a'] \
         RETURN e.entity_id AS id ORDER BY id LIMIT 1",
    );
    assert!(plan.contains("access=property-points"), "{plan}");
    assert!(!plan.contains("access=ordered-property"), "{plan}");
}
