//! Rendezvous placement: which node owns a `(scope, cell)`.
//!
//! Every live node gets a score for the pair, and the highest score wins.
//! There is no stored assignment, no lease and no election service — every
//! node computes the same answer from the same live set. Removing a node moves
//! only the cells it owned; adding one moves only the cells it now wins.
//!
//! The hash and its key encoding are FROZEN, like a wire format:
//! FNV-1a 64 over `scope ++ 0x00 ++ cell_id ++ 0x00 ++ node_id`, ties broken by
//! node id ascending. Changing either splits a mixed-version fleet into two
//! disagreeing views of who owns a cell, which is the exact failure this crate
//! exists to prevent. [`scores_are_frozen`](tests::scores_are_frozen) pins them.
//!
//! `scope` is `GraphScope`'s `Display` — `"{namespace}/graphs/{graph_id}"`.
//! Pinning golden values against it freezes that impl as a wire format too.
//!
//! Ported from `sleet/src/placement.rs`, which is the proven version of exactly
//! this, with sleet's `(database, service)` pair replaced by `(scope, cell)`.

const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
const FNV_PRIME: u64 = 0x100_0000_01b3;

fn fnv1a64(chunks: &[&[u8]]) -> u64 {
    let mut h = FNV_OFFSET;
    for chunk in chunks {
        for &b in *chunk {
            h ^= u64::from(b);
            h = h.wrapping_mul(FNV_PRIME);
        }
    }
    h
}

/// The frozen score of one candidate node for one `(scope, cell)`.
pub fn score(scope: &str, cell_id: &str, node_id: &str) -> u64 {
    fnv1a64(&[
        scope.as_bytes(),
        b"\0",
        cell_id.as_bytes(),
        b"\0",
        node_id.as_bytes(),
    ])
}

/// Candidate node ids ranked best-first for a `(scope, cell)`. Callers pass the
/// live nodes — this function has no opinion on liveness.
pub fn rank<'a>(scope: &str, cell_id: &str, live: &[&'a str]) -> Vec<&'a str> {
    let mut ranked: Vec<(u64, &str)> = live
        .iter()
        .map(|&n| (score(scope, cell_id, n), n))
        .collect();
    // Descending by score; ties broken by node id so every node agrees.
    ranked.sort_by(|a, b| b.0.cmp(&a.0).then_with(|| a.1.cmp(b.1)));
    ranked.into_iter().map(|(_, n)| n).collect()
}

/// The owner of a `(scope, cell)`: the top of the ranking, or `None` when no
/// node is live.
///
/// `None` is not "nobody may write" — it is "placement has no opinion". The
/// caller's rule is: promote if you are the owner, or if there is no live
/// owner; otherwise decline.
pub fn owner<'a>(scope: &str, cell_id: &str, live: &[&'a str]) -> Option<&'a str> {
    rank(scope, cell_id, live).first().copied()
}

#[cfg(test)]
mod tests {
    use super::*;

    const SCOPE: &str = "acme/graphs/social";
    const NODES: &[&str] = &[
        "graph-node-0",
        "graph-node-1",
        "graph-node-2",
        "graph-node-3",
    ];

    /// The hash and key encoding are frozen; if this test breaks, the change
    /// breaks mixed-version fleets.
    #[test]
    fn scores_are_frozen() {
        assert_eq!(
            score(SCOPE, "cell-a", "graph-node-0"),
            0xf0dc_2317_deb2_97c5
        );
        assert_eq!(
            score(SCOPE, "cell-a", "graph-node-1"),
            0xf0dc_2217_deb2_9612
        );
        assert_eq!(
            score(SCOPE, "cell-b", "graph-node-0"),
            0x39ff_e3dd_3cc7_9ffe
        );
        assert_eq!(
            score("acme/graphs/other", "cell-a", "graph-node-0"),
            0xc15e_5780_2a47_9f04
        );
    }

    /// The separator is load-bearing: without it `("ab", "c")` and `("a", "bc")`
    /// would hash alike and two different cells could share an owner by
    /// accident.
    #[test]
    fn the_separator_keeps_segment_boundaries_distinct() {
        assert_ne!(score("a", "bc", "n"), score("ab", "c", "n"));
    }

    #[test]
    fn ranking_is_deterministic_and_cell_dependent() {
        let a = rank(SCOPE, "cell-a", NODES);
        assert_eq!(a, rank(SCOPE, "cell-a", NODES));
        assert_eq!(a.len(), NODES.len());
        // Different cells and scopes rank independently. Not a property FNV
        // guarantees in general, but these fixed inputs demonstrate the keys
        // are distinct.
        assert_ne!(a, rank(SCOPE, "cell-b", NODES));
        assert_ne!(a, rank("acme/graphs/other", "cell-a", NODES));
    }

    /// Minimal disruption: losing a node must not reshuffle the survivors, or a
    /// single failure would move every cell in the fleet.
    #[test]
    fn removing_a_node_preserves_the_order_of_the_rest() {
        // `cell-b` ranks the nodes out of lexical order, so this asserts
        // something the identity permutation would hide.
        let full = rank(SCOPE, "cell-b", NODES);
        let removed = full[1];
        let remaining: Vec<&str> = NODES.iter().copied().filter(|&n| n != removed).collect();
        let expected: Vec<&str> = full.iter().copied().filter(|&n| n != removed).collect();
        assert_eq!(rank(SCOPE, "cell-b", &remaining), expected);
    }

    /// Losing the owner promotes the runner-up, and does not disturb anyone
    /// else's position.
    #[test]
    fn losing_the_owner_promotes_the_runner_up() {
        let full = rank(SCOPE, "cell-b", NODES);
        let survivors: Vec<&str> = full[1..].to_vec();
        assert_eq!(owner(SCOPE, "cell-b", NODES), Some(full[0]));
        assert_eq!(owner(SCOPE, "cell-b", &survivors), Some(full[1]));
    }

    #[test]
    fn every_node_agrees_regardless_of_input_order() {
        let forward: Vec<&str> = NODES.to_vec();
        let mut reversed = forward.clone();
        reversed.reverse();
        assert_eq!(
            rank(SCOPE, "cell-a", &forward),
            rank(SCOPE, "cell-a", &reversed)
        );
    }

    #[test]
    fn no_live_node_means_no_owner() {
        assert_eq!(owner(SCOPE, "cell-a", &[]), None);
        assert!(rank(SCOPE, "cell-a", &[]).is_empty());
    }
}
