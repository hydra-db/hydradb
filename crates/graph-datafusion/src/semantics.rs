//! Small, backend-independent semantic guards shared by graph runtimes.

use hydradb_graph_provider::{ExpansionDepth, ExpansionInput, RelationshipUniqueness};
use hydradb_graph_types::{NodeId, RelationshipId};

/// One canonical expansion match, preserving the input row identity assigned
/// by the execution engine.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExpansionMatch {
    pub input_ordinal: u64,
    pub source_node_id: NodeId,
    pub target_node_id: NodeId,
    pub relationship_ids: Vec<RelationshipId>,
    pub path_node_ids: Vec<NodeId>,
}

/// Convert a batch of source/optional-target ids into provider inputs.
///
/// Ordinals are row positions rather than entity IDs, so repeated input rows
/// naming the same node remain distinct matches after expansion.
pub fn expansion_inputs(sources: &[Option<NodeId>], targets: Option<&[Option<NodeId>]>) -> Vec<ExpansionInput> {
    assert!(targets.is_none_or(|targets| targets.len() == sources.len()));
    sources
        .iter()
        .enumerate()
        .filter_map(|(row, source)| {
            let source_node_id = (*source)?;
            Some(ExpansionInput {
                input_ordinal: row as u64,
                source_node_id,
                target_node_id: targets.and_then(|targets| targets[row]),
                excluded_relationships: Vec::new(),
            })
        })
        .collect()
}

/// Validate provider traversal results against the input pattern bounds.
///
/// ExpandInto is represented by an input with `target_node_id` set. This check
/// prevents an implementation from silently turning it into ordinary Expand.
pub fn validate_expansion_match(
    input: &ExpansionInput,
    result: &ExpansionMatch,
    depth: ExpansionDepth,
    uniqueness: RelationshipUniqueness,
) -> bool {
    if result.input_ordinal != input.input_ordinal
        || result.source_node_id != input.source_node_id
        || input.target_node_id.is_some_and(|target| target != result.target_node_id)
    {
        return false;
    }
    let hops = result.relationship_ids.len();
    let (min_hops, max_hops) = match depth {
        ExpansionDepth::Fixed => (1, 1),
        ExpansionDepth::Variable { min_hops, max_hops } => (min_hops as usize, max_hops as usize),
    };
    if hops < min_hops || hops > max_hops || result.path_node_ids.len() != hops + 1 {
        return false;
    }
    if result.path_node_ids.first().copied() != Some(result.source_node_id)
        || result.path_node_ids.last().copied() != Some(result.target_node_id)
    {
        return false;
    }
    match uniqueness {
        RelationshipUniqueness::Walk => true,
        RelationshipUniqueness::Trail => has_no_repeats(&result.relationship_ids),
        RelationshipUniqueness::SimplePath => has_no_repeats(&result.relationship_ids) && has_no_repeats(&result.path_node_ids),
    }
}

fn has_no_repeats<T: Eq + std::hash::Hash>(values: &[T]) -> bool {
    let mut seen = std::collections::HashSet::with_capacity(values.len());
    values.iter().all(|value| seen.insert(value))
}

/// Evaluate a correlated branch once per outer row and preserve every branch
/// result. Optional apply yields exactly one null-extended result if and only
/// if that outer row's branch produces no rows.
pub fn correlated_apply<O, R>(
    outer: impl IntoIterator<Item = O>,
    mut execute_branch: impl FnMut(&O) -> Vec<R>,
    optional: bool,
) -> Vec<(O, Option<R>)>
where
    O: Clone,
{
    let mut output = Vec::new();
    for outer_row in outer {
        let matches = execute_branch(&outer_row);
        if matches.is_empty() {
            if optional {
                output.push((outer_row, None));
            }
        } else {
            output.extend(matches.into_iter().map(|matched| (outer_row.clone(), Some(matched))));
        }
    }
    output
}
