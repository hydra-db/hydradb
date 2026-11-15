use std::collections::{BTreeMap, BTreeSet};

use hydradb_cypher_ast::{MatchClause, PathFactor, PathMode, QueryFailureReason, SimplifiedPath};

use super::{GraphPlanError, GraphPlanResult, Symbol};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Degree {
    Singleton,
    Conditional,
    BoundedGroup,
    UnboundedGroup,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Kind {
    Node,
    Edge,
    Subpath,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct Exposure {
    kind: Kind,
    degree: Degree,
}

#[derive(Debug, Default)]
struct FactorProperties {
    minimum_nodes: u64,
    minimum_edges: u64,
    variables: BTreeMap<Symbol, Exposure>,
    /// Only this graph-matching depth: predicates' queries are not factors
    /// of their enclosing path, and are analyzed on their own invocation.
    quantified: bool,
}

fn syntax(message: impl Into<String>) -> GraphPlanError {
    GraphPlanError::syntax(QueryFailureReason::Pattern, message.into())
}

fn join_variables(into: &mut BTreeMap<Symbol, Exposure>, variables: BTreeMap<Symbol, Exposure>) -> GraphPlanResult<()> {
    for (name, exposure) in variables {
        if let Some(previous) = into.insert(name.clone(), exposure) {
            if previous.kind != exposure.kind {
                return Err(syntax(format!("{name} has incompatible graph-variable kinds")));
            }
            if previous.kind == Kind::Subpath || previous.degree != Degree::Singleton || exposure.degree != Degree::Singleton {
                return Err(syntax(format!(
                    "only unconditional singleton elements may implicitly join on {name}"
                )));
            }
        }
    }
    Ok(())
}

/// §16.7 SR7–9, SR15 and SR20–22 on the normalized algebra, before any
/// execution-subset refusal. Counts saturate: only minima comparisons are
/// needed, so large legal u32 bounds need not overflow a host integer.
fn properties(factor: &PathFactor, restrictive: bool) -> GraphPlanResult<FactorProperties> {
    let mut facts = FactorProperties::default();
    match factor {
        PathFactor::Simplified(expression) => {
            facts = properties(&PathFactor::concatenation(vec![simplified_factor(expression)?]), restrictive)?;
        }
        PathFactor::Node(name) | PathFactor::Edge(name) => {
            let node = matches!(factor, PathFactor::Node(_));
            facts.minimum_nodes = u64::from(node);
            facts.minimum_edges = u64::from(!node);
            if let Some(name) = name {
                facts.variables.insert(
                    Symbol::from(name.as_str()),
                    Exposure {
                        kind: if node { Kind::Node } else { Kind::Edge },
                        degree: Degree::Singleton,
                    },
                );
            }
        }
        PathFactor::Concatenation(parts) => {
            for (index, part) in parts.iter().enumerate() {
                let part_facts = properties(part, restrictive)?;
                facts.minimum_nodes = facts.minimum_nodes.saturating_add(part_facts.minimum_nodes);
                facts.minimum_edges = facts.minimum_edges.saturating_add(part_facts.minimum_edges);
                if index > 0 && matches!((&parts[index - 1], part), (PathFactor::Node(_), PathFactor::Node(_))) {
                    facts.minimum_nodes = facts.minimum_nodes.saturating_sub(1);
                }
                facts.quantified |= part_facts.quantified;
                join_variables(&mut facts.variables, part_facts.variables)?;
            }
        }
        PathFactor::Alternatives(parts) => {
            let arms = parts
                .iter()
                .map(|part| properties(part, restrictive))
                .collect::<GraphPlanResult<Vec<_>>>()?;
            facts.minimum_nodes = arms.iter().map(|arm| arm.minimum_nodes).min().unwrap_or(0);
            facts.minimum_edges = arms.iter().map(|arm| arm.minimum_edges).min().unwrap_or(0);
            facts.quantified = arms.iter().any(|arm| arm.quantified);
            for arm in &arms {
                for (name, exposure) in &arm.variables {
                    if facts
                        .variables
                        .insert(name.clone(), *exposure)
                        .is_some_and(|previous| previous.kind != exposure.kind)
                    {
                        return Err(syntax(format!("{name} has incompatible graph-variable kinds")));
                    }
                }
            }
            for (name, exposure) in &mut facts.variables {
                let degrees: Vec<_> = arms
                    .iter()
                    .filter_map(|arm| arm.variables.get(name).map(|variable| variable.degree))
                    .collect();
                exposure.degree = if degrees.len() == arms.len() && degrees.iter().all(|degree| *degree == Degree::Singleton) {
                    Degree::Singleton
                } else if degrees.contains(&Degree::UnboundedGroup) {
                    Degree::UnboundedGroup
                } else if degrees.contains(&Degree::BoundedGroup) {
                    Degree::BoundedGroup
                } else {
                    Degree::Conditional
                };
            }
        }
        PathFactor::Parenthesized {
            binding,
            restrictive: inner_restrictive,
            search_prefix_written,
            inner,
        } => {
            if *search_prefix_written {
                return Err(syntax("a parenthesized path has a path mode, not a path search prefix"));
            }
            facts = properties(inner, restrictive || *inner_restrictive)?;
            if let Some(name) = binding {
                if facts.minimum_nodes == 0 {
                    return Err(syntax("a named subpath has a positive minimum node count"));
                }
                if facts
                    .variables
                    .insert(
                        Symbol::from(name.as_str()),
                        Exposure {
                            kind: Kind::Subpath,
                            degree: Degree::Singleton,
                        },
                    )
                    .is_some()
                {
                    return Err(syntax(format!("the subpath variable {name} is already declared in its path")));
                }
            }
        }
        PathFactor::Quantified { primary, length } => {
            facts = properties(primary, restrictive)?;
            if facts.minimum_edges == 0 {
                return Err(syntax("a repeated primary has a positive minimum path length"));
            }
            if facts.quantified {
                return Err(syntax("quantified primaries do not nest at the same graph-matching depth"));
            }
            facts.quantified = true;
            let minimum = u64::from(length.minimum.unwrap_or(0));
            facts.minimum_nodes = facts.minimum_nodes.saturating_mul(minimum);
            facts.minimum_edges = facts.minimum_edges.saturating_mul(minimum);
            for exposure in facts.variables.values_mut() {
                exposure.degree = if restrictive || (length.maximum.is_some() && exposure.degree != Degree::UnboundedGroup) {
                    Degree::BoundedGroup
                } else {
                    Degree::UnboundedGroup
                };
            }
        }
        PathFactor::Questioned(primary) => {
            facts = properties(primary, restrictive)?;
            if facts.minimum_edges == 0 {
                return Err(syntax("a questioned primary has a positive minimum path length"));
            }
            facts.minimum_nodes = 0;
            facts.minimum_edges = 0;
            for exposure in facts.variables.values_mut() {
                if exposure.degree == Degree::Singleton {
                    exposure.degree = Degree::Conditional;
                }
            }
        }
    }
    Ok(facts)
}

fn simplified_factor(expression: &SimplifiedPath) -> GraphPlanResult<PathFactor> {
    use SimplifiedPath as S;
    let label_context = matches!(expression, S::Negated(_) | S::Conjunction(_) | S::DirectionOverride { .. });
    if label_context && expression.children().iter().any(simplified_non_label) {
        return Err(syntax(
            "a simplified negation, conjunction or direction override contains a non-label construct",
        ));
    }
    let children = expression
        .children()
        .iter()
        .map(simplified_factor)
        .collect::<GraphPlanResult<Vec<_>>>()?;
    Ok(match expression {
        S::Label(_) | S::Negated(_) | S::Conjunction(_) | S::DirectionOverride { .. } => PathFactor::Edge(None),
        S::Alternatives { .. } => {
            PathFactor::Alternatives(children.into_iter().map(|child| PathFactor::concatenation(vec![child])).collect())
        }
        S::Concatenation(_) => PathFactor::concatenation(children),
        S::Quantified { length, .. } => children.into_iter().next().expect("one primary").quantified(*length),
        S::Questioned(_) => PathFactor::Questioned(Box::new(PathFactor::concatenation(children))),
    })
}

fn simplified_non_label(expression: &SimplifiedPath) -> bool {
    matches!(
        expression,
        SimplifiedPath::Concatenation(_)
            | SimplifiedPath::Quantified { .. }
            | SimplifiedPath::Questioned(_)
            | SimplifiedPath::Alternatives { multiset: true, .. }
            | SimplifiedPath::DirectionOverride { .. }
    ) || expression.children().iter().any(simplified_non_label)
}

pub(super) fn validate_match_factors(matched: &MatchClause) -> GraphPlanResult<()> {
    let mut variables = BTreeMap::new();
    for path in &matched.patterns {
        let Some(factor) = &path.factor else {
            continue;
        };
        let restrictive = path.modes.iter().any(|mode| *mode != PathMode::Walk);
        let facts = properties(factor, restrictive)?;
        if facts.minimum_nodes == 0 {
            return Err(syntax("a path pattern has a positive minimum node count"));
        }
        join_variables(&mut variables, facts.variables)?;
    }
    Ok(())
}

/// §16.3 SR3–5: element globals, never named subpaths.
pub(super) fn element_globals(factor: &PathFactor, restrictive: bool) -> GraphPlanResult<BTreeSet<Symbol>> {
    Ok(properties(factor, restrictive)?
        .variables
        .into_iter()
        .filter(|(_, exposure)| exposure.kind != Kind::Subpath)
        .map(|(name, _)| name)
        .collect())
}
