//! Rendezvous placement: which node owns a `(scope, cell)`.
//!
//! Every live node gets a score for the pair, and the highest score wins.
//! There is no stored assignment, no lease and no election service — every
//! node computes the same answer from the same live set. Removing a node moves
//! only the cells it owned; adding one moves only the cells it now wins.
//!
//! The hash and its key encoding are FROZEN, like a wire format:
//! FNV-1a 64 over `scope ++ 0x00 ++ cell_id ++ 0x00 ++ node_id`, run through a
//! splitmix64 finalizer, ties broken by node id ascending. Changing any of
//! those splits a mixed-version fleet into two disagreeing views of who owns a
//! cell, which is the exact failure this crate exists to prevent.
//! [`scores_are_frozen`](tests::scores_are_frozen) pins them.
//!
//! The finalizer is not decoration. FNV-1a ends on a single multiply, so keys
//! differing only in their last byte — which is exactly what `graph-node-0`,
//! `graph-node-1`, `graph-node-2` are — come out agreeing in their high bits,
//! and the ranking is decided by a handful of low ones. Measured over 20 000
//! cells, plain FNV-1a splits three such nodes 5000/5000/10000: one node owns
//! half the fleet, at every scale, and it does not converge with cell count.
//! splitmix64's three shift-xor-multiply rounds avalanche that difference
//! across all 64 bits and the same measurement gives 1.006.
//! [`ownership_is_balanced_across_similar_node_ids`] guards it.
//!
//! `scope` is `GraphScope`'s `Display` — `"{namespace}/graphs/{graph_id}"`.
//! Pinning golden values against it freezes that impl as a wire format too.
//!
//! Adapted from a proven rendezvous-placement implementation, with its
//! `(database, service)` pair replaced by `(scope, cell)`.

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

/// The splitmix64 finalizer. Three shift-xor-multiply rounds: the shift-xor
/// pulls high bits down, the multiply pushes low bits back up, so a one-bit
/// input change flips about half the output bits. Standard constants, the same
/// tail SplitMix and xoshiro seeding use.
fn mix64(mut z: u64) -> u64 {
    z = z.wrapping_add(0x9e37_79b9_7f4a_7c15);
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

/// The frozen score of one candidate node for one `(scope, cell)`.
pub fn score(scope: &str, cell_id: &str, node_id: &str) -> u64 {
    mix64(fnv1a64(&[scope.as_bytes(), b"\0", cell_id.as_bytes(), b"\0", node_id.as_bytes()]))
}

/// Candidate node ids ranked best-first for a `(scope, cell)`. Callers pass the
/// live nodes — this function has no opinion on liveness.
pub fn rank<'a>(scope: &str, cell_id: &str, live: &[&'a str]) -> Vec<&'a str> {
    let mut ranked: Vec<(u64, &str)> = live.iter().map(|&n| (score(scope, cell_id, n), n)).collect();
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
