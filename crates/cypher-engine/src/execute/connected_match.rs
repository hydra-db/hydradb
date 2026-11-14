use super::*;

pub(super) fn accepts(left: &GraphPhysicalPlan, right: &GraphPhysicalPlan, rows: &[BindingRow]) -> bool {
    if !simple_input(left) || !list_seed(left) {
        return false;
    }
    let mut hop = right;
    loop {
        hop = match hop {
            GraphPhysicalPlan::Filter { input, .. } => input,
            GraphPhysicalPlan::RelationshipTypeProbe { fallback, .. } => fallback,
            _ => break,
        };
    }
    let GraphPhysicalPlan::Expand {
        input,
        from,
        to,
        relationship,
        relationship_types,
        ..
    } = hop
    else {
        return false;
    };
    if relationship_types.len() != 1 || from == to || relationship.as_ref().is_some_and(|name| name == from || name == to) {
        return false;
    }
    let allowed = |name: &Symbol| name == from || name == to || relationship.as_ref() == Some(name);
    if !source(input, from) || !outer_filters(right, &allowed) {
        return false;
    }
    rows.iter().all(|row| {
        row.vertices.contains_key(from)
            && !row.is_null(from)
            && !row.binds(to)
            && !relationship.as_ref().is_some_and(|name| row.binds(name))
            && row.deferred.is_empty()
            && row.conditions.is_empty()
    })
}

fn simple_input(plan: &GraphPhysicalPlan) -> bool {
    match plan {
        GraphPhysicalPlan::VertexScan { scan } => !scan.properties().window_accepted,
        GraphPhysicalPlan::Filter { input, predicate, .. } => crate::plan::total_condition(predicate) && simple_input(input),
        GraphPhysicalPlan::Expand { input, .. } => simple_input(input),
        GraphPhysicalPlan::RelationshipTypeProbe { fallback, .. } => simple_input(fallback),
        GraphPhysicalPlan::NaturalJoin { left, right, optional } => !optional && simple_input(left) && simple_input(right),
        _ => false,
    }
}

fn safe_filter(predicate: &PhysicalExpression, allowed: &impl Fn(&Symbol) -> bool) -> bool {
    crate::plan::total_condition(predicate) && !row_reads(predicate, &|name| !allowed(name))
}

fn list_seed(plan: &GraphPhysicalPlan) -> bool {
    match plan {
        GraphPhysicalPlan::VertexScan { scan } => scan.request().predicates.iter().any(membership),
        GraphPhysicalPlan::Filter { input, predicate, .. } => membership(predicate) || list_seed(input),
        GraphPhysicalPlan::Expand { input, .. } => list_seed(input),
        GraphPhysicalPlan::RelationshipTypeProbe { fallback, .. } => list_seed(fallback),
        GraphPhysicalPlan::NaturalJoin { left, optional: false, .. } => list_seed(left),
        _ => false,
    }
}

fn membership(predicate: &PhysicalExpression) -> bool {
    match predicate {
        PhysicalExpression::InList { expression, .. } => matches!(
            expression.as_ref(),
            PhysicalExpression::Property { .. } | PhysicalExpression::Identity(_) | PhysicalExpression::Binding(_)
        ),
        PhysicalExpression::Binary {
            left,
            operator: PhysicalBinaryOperator::And,
            right,
        } => membership(left) || membership(right),
        _ => false,
    }
}

fn source(plan: &GraphPhysicalPlan, from: &Symbol) -> bool {
    match plan {
        GraphPhysicalPlan::VertexScan { scan } => scan.binding() == from && !scan.index_only() && !scan.properties().window_accepted,
        GraphPhysicalPlan::Filter { input, predicate, .. } => safe_filter(predicate, &|name| name == from) && source(input, from),
        _ => false,
    }
}

fn outer_filters(plan: &GraphPhysicalPlan, allowed: &impl Fn(&Symbol) -> bool) -> bool {
    match plan {
        GraphPhysicalPlan::Expand { .. } => true,
        GraphPhysicalPlan::Filter { input, predicate, .. } => safe_filter(predicate, allowed) && outer_filters(input, allowed),
        GraphPhysicalPlan::RelationshipTypeProbe { fallback, .. } => outer_filters(fallback, allowed),
        _ => false,
    }
}

pub(super) async fn execute(
    read: &mut ExecutionRead<'_>,
    plan: &GraphPhysicalPlan,
    rows: Vec<BindingRow>,
) -> EngineResult<Vec<BindingRow>> {
    read.checkpoint("cypher_connected_match_batch")?;
    let mut hop = plan;
    loop {
        hop = match hop {
            GraphPhysicalPlan::Filter { input, .. } => input,
            GraphPhysicalPlan::RelationshipTypeProbe { fallback, .. } => fallback,
            _ => break,
        };
    }
    let GraphPhysicalPlan::Expand {
        from, to, relationship, ..
    } = hop
    else {
        unreachable!("connected MATCH shape was checked before execution");
    };
    let mut remaining = BTreeMap::<VertexId, usize>::new();
    for row in &rows {
        read.checkpoint("cypher_connected_match_inputs")?;
        *remaining.entry(row.vertices[from].id).or_default() += 1;
    }
    let names = std::iter::once(to.clone()).chain(relationship.iter().cloned()).collect::<Vec<_>>();
    let capacity = read.max_intermediate_rows().unwrap_or(usize::MAX);
    let mut retained = 0usize;
    let mut matches = BTreeMap::<VertexId, Vec<BindingRow>>::new();
    let mut joined = Vec::new();
    for row in rows {
        read.checkpoint("cypher_connected_match_anchor")?;
        let id = row.vertices[from].id;
        let remaining = remaining.get_mut(&id).expect("source was counted");
        *remaining -= 1;
        if let Some(cached) = matches.get(&id) {
            for matched in cached {
                read.checkpoint("cypher_connected_match_replay")?;
                joined.push(row.overlaid(matched));
                read.check_intermediate_rows("cypher_join_intermediate_rows", joined.len())?;
            }
            if *remaining == 0 {
                let removed = matches.remove(&id).expect("source matches were cached");
                retained -= removed.len().saturating_add(1);
            }
            continue;
        }
        let found = execute_binding_plan_seeded(read, plan, None, Some(&row)).await?;
        read.check_intermediate_rows("cypher_join_intermediate_rows", joined.len().saturating_add(found.len()))?;
        let weight = found.len().saturating_add(1);
        if *remaining > 0 && weight <= capacity.saturating_sub(retained) {
            let mut reusable = Vec::with_capacity(found.len());
            for matched in &found {
                read.checkpoint("cypher_connected_match_retain")?;
                reusable.push(matched.restricted_to(&names));
            }
            matches.insert(id, reusable);
            retained += weight;
        }
        joined.extend(found);
    }
    Ok(joined)
}
