//! RFC 0003 acceptance: **encoded byte order == logical order** for every key
//! component and composite key, plus round-trip and range-totality.
//!
//! This is the M0 north star (handoff §"First failing test"): the composite
//! `Index[pred_id][token]` key must sort in logical value order across the
//! `int` / `float` / `exact` tokenizers and all boundaries, because that is the
//! contract the posting-list set algebra and range predicates rely on.

use proptest::prelude::*;
use turbolay::RecordType;
use turbolay::serde::keys;
use turbolay::serde::token;
use turbolay::serde::{Direction, PredId, Uid};

// ── Acceptance #1: UID ordering (dense u64, big-endian) ────────────────────

proptest! {
    #[test]
    fn should_order_node_keys_by_uid(a: u64, b: u64) {
        // given two uids; then key byte order equals numeric uid order
        prop_assert_eq!(
            keys::node_key(Uid(a)).cmp(&keys::node_key(Uid(b))),
            a.cmp(&b)
        );
    }

    #[test]
    fn should_roundtrip_node_key(uid: u64) {
        prop_assert_eq!(keys::parse_node_key(&keys::node_key(Uid(uid))).unwrap(), Uid(uid));
    }
}

#[test]
fn should_order_uid_boundaries() {
    // given the documented boundary set (0, 1, 2^32-1, 2^32, u64::MAX)
    let bounds = [0u64, 1, (1 << 32) - 1, 1 << 32, u64::MAX];
    for w in bounds.windows(2) {
        assert!(keys::node_key(Uid(w[0])) < keys::node_key(Uid(w[1])));
    }
}

// ── Acceptance #3 + #4: numeric index tokens sort in value order ───────────

proptest! {
    #[test]
    fn should_order_int_index_tokens_within_a_predicate(a: i64, b: i64) {
        // given a fixed predicate; then Index[pred][int_token] order == i64 order
        let pred = PredId(7);
        let ka = keys::index_key(pred, &token::token_int(a));
        let kb = keys::index_key(pred, &token::token_int(b));
        prop_assert_eq!(ka.cmp(&kb), a.cmp(&b));
    }

    #[test]
    fn should_order_float_index_tokens_within_a_predicate(a: f64, b: f64) {
        // NaN has no total order with the reals; the encoding sorts it last but
        // logical `cmp` is undefined, so exclude NaN from the equivalence sweep.
        prop_assume!(!a.is_nan() && !b.is_nan());
        // -0.0 and +0.0 are numerically equal (`partial_cmp` == Equal) but the
        // sortable encoding keeps them distinct and adjacent (-0.0 just below
        // +0.0). That is a desirable property, not a violation, so skip the
        // signed-zero pair — matching upstream `sortable`'s own ordering test.
        prop_assume!(!(a == b && a.to_bits() != b.to_bits()));
        let pred = PredId(7);
        let ka = keys::index_key(pred, &token::token_float(a));
        let kb = keys::index_key(pred, &token::token_float(b));
        prop_assert_eq!(ka.cmp(&kb), a.partial_cmp(&b).unwrap());
    }

    #[test]
    fn should_order_exact_index_tokens_within_a_predicate(a: Vec<u8>, b: Vec<u8>) {
        // given a fixed predicate; then Index[pred][exact_token] order == byte order
        let pred = PredId(7);
        let ka = keys::index_key(pred, &token::token_exact(&a));
        let kb = keys::index_key(pred, &token::token_exact(&b));
        prop_assert_eq!(ka.cmp(&kb), a.cmp(&b));
    }

    #[test]
    fn should_roundtrip_int_and_float_and_exact_tokens(i in any::<i64>(), f in any::<f64>(), s: Vec<u8>) {
        prop_assert_eq!(token::decode_int(&token::token_int(i)).unwrap(), i);
        prop_assert_eq!(token::decode_float(&token::token_float(f)).unwrap().to_bits(), f.to_bits());
        let exact = token::decode_exact(&token::token_exact(&s)).unwrap();
        prop_assert_eq!(exact.as_ref(), s.as_slice());
    }
}

#[test]
fn should_sort_nan_last_among_float_tokens() {
    // given the ordered float boundary set ending in +inf, then NaN
    let ordered = [
        f64::NEG_INFINITY,
        f64::MIN,
        -1.0,
        0.0,
        f64::MIN_POSITIVE,
        1.0,
        f64::MAX,
        f64::INFINITY,
    ];
    let pred = PredId(1);
    for w in ordered.windows(2) {
        let lo = keys::index_key(pred, &token::token_float(w[0]));
        let hi = keys::index_key(pred, &token::token_float(w[1]));
        assert!(lo < hi, "{} should encode below {}", w[0], w[1]);
    }
    let inf = keys::index_key(pred, &token::token_float(f64::INFINITY));
    let nan = keys::index_key(pred, &token::token_float(f64::NAN));
    assert!(nan > inf, "NaN must sort last");
}

// ── Composite clustering: predicate id clusters its tokens ─────────────────

proptest! {
    #[test]
    fn should_cluster_all_tokens_under_their_predicate(p in any::<u32>(), token in any::<Vec<u8>>()) {
        // a token under pred p is inside index_pred_range(p) and, for p != p+1,
        // outside the adjacent predicate's range.
        prop_assume!(p < u32::MAX);
        let key = keys::index_key(PredId(p), &token::token_exact(&token));
        prop_assert!(index_contains(p, &key));
        prop_assert!(!index_contains(p + 1, &key));
    }
}

fn index_contains(pred: u32, key: &[u8]) -> bool {
    let r = keys::index_pred_range(PredId(pred));
    common_range_contains(&r, key)
}

// `BytesRange` implements the containment we need but is defined in `common`;
// re-derive it here against the public bound accessors to avoid depending on a
// `contains` inherent that may not be exported.
fn common_range_contains(r: &common::BytesRange, k: &[u8]) -> bool {
    use std::ops::Bound::{Excluded, Included, Unbounded};
    let lo = match &r.start {
        Included(s) => k >= s.as_ref(),
        Excluded(s) => k > s.as_ref(),
        Unbounded => true,
    };
    let hi = match &r.end {
        Included(e) => k <= e.as_ref(),
        Excluded(e) => k < e.as_ref(),
        Unbounded => true,
    };
    lo && hi
}

// ── Acceptance #5: range totality — tags don't bleed into neighbors ────────

#[test]
fn should_bound_record_type_ranges_without_bleeding() {
    // A Node key is inside the Node range and outside EdgeOut's range.
    let node = keys::node_key(Uid(123));
    assert!(common_range_contains(
        &keys::record_type_range(RecordType::Node),
        &node
    ));
    assert!(!common_range_contains(
        &keys::record_type_range(RecordType::EdgeOut),
        &node
    ));

    // An EdgeOut key sits above every Node key (tag 0x4 > 0x3).
    let edge = keys::edge_key(Direction::Out, Uid(0), PredId(0));
    assert!(edge > node);
    assert!(!common_range_contains(
        &keys::record_type_range(RecordType::Node),
        &edge
    ));
}

#[test]
fn should_bound_changelog_tail_exclusive_lower_inclusive_upper() {
    // (W, latest] : W excluded, latest included, latest+1 excluded.
    let r = keys::log_range(100, 110);
    assert!(!common_range_contains(&r, &keys::log_key(100)));
    assert!(common_range_contains(&r, &keys::log_key(101)));
    assert!(common_range_contains(&r, &keys::log_key(110)));
    assert!(!common_range_contains(&r, &keys::log_key(111)));
}

// ── Acceptance #6: round-trip for the variable-length + composite keys ─────

proptest! {
    #[test]
    fn should_roundtrip_xid_key(xid: Vec<u8>) {
        let parsed = keys::parse_xid_key(&keys::xid_key(&xid)).unwrap();
        prop_assert_eq!(parsed.as_ref(), xid.as_slice());
    }

    #[test]
    fn should_order_xid_keys_by_external_id(a: Vec<u8>, b: Vec<u8>) {
        // terminated_bytes is order-preserving, so xid key order == xid order.
        prop_assert_eq!(keys::xid_key(&a).cmp(&keys::xid_key(&b)), a.cmp(&b));
    }

    #[test]
    fn should_roundtrip_edge_and_count_keys(anchor: u64, pred: u32, degree: u32) {
        for dir in [Direction::Out, Direction::In] {
            let ek = keys::edge_key(dir, Uid(anchor), PredId(pred));
            prop_assert_eq!(keys::parse_edge_key(&ek).unwrap(), (dir, Uid(anchor), PredId(pred)));

            let ck = keys::count_key(PredId(pred), dir, degree);
            prop_assert_eq!(keys::parse_count_key(&ck).unwrap(), (PredId(pred), dir, degree));
        }
    }
}
