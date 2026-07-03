//! RFC 0005 acceptance: `PostingValue` round-trip, set-algebra-vs-oracle, and
//! split-lifecycle reads. This is M1's north star (handoff
//! `2026-07-03-m1-d1-posting-value-handoff.md`), the way `Index[pred][token]`
//! ordering was M0's — I/O-free (no `Storage`), fast, deterministic.

use std::collections::BTreeSet;

use proptest::prelude::*;
use roaring::RoaringTreemap;
use turbolay::posting::{PartRef, PostingValue};

fn treemap_of(vals: &[u64]) -> RoaringTreemap {
    vals.iter().copied().collect()
}

fn btreeset_of(vals: &[u64]) -> BTreeSet<u64> {
    vals.iter().copied().collect()
}

// ── Acceptance #1: round-trip identity ──────────────────────────────────────

/// Boundary-heavy generator: small counts, boundary uids mixed with random
/// ones, duplicates allowed (roaring/BTreeSet dedupe them, which is the
/// point).
fn uid_vec_strategy() -> impl Strategy<Value = Vec<u64>> {
    let boundary = prop_oneof![
        Just(0u64),
        Just(1u64),
        Just(u32::MAX as u64),
        Just(u32::MAX as u64 + 1),
        Just(u64::MAX),
        any::<u64>(),
    ];
    prop::collection::vec(boundary, 0..64)
}

proptest! {
    #[test]
    fn should_roundtrip_single_identity_and_ascending(vals in uid_vec_strategy()) {
        // given a Single built from arbitrary (possibly duplicate) uids
        let set = treemap_of(&vals);
        let value = PostingValue::single(set.clone());

        // when
        let bytes = value.serialize();
        let back = PostingValue::deserialize(&bytes).unwrap();

        // then — identity round-trip
        prop_assert_eq!(&back, &value);

        // and — iteration is strictly ascending (RFC 0003 contract)
        let materialized = back.materialize_single().unwrap();
        let mut prev: Option<u64> = None;
        for uid in materialized.iter() {
            if let Some(p) = prev {
                prop_assert!(uid > p);
            }
            prev = Some(uid);
        }

        // and — matches the dedup'd oracle set
        let oracle = btreeset_of(&vals);
        let got: BTreeSet<u64> = materialized.iter().collect();
        prop_assert_eq!(got, oracle);
    }

    #[test]
    fn should_roundtrip_split_manifest_identity(
        starts in prop::collection::vec(any::<u64>(), 0..16),
    ) {
        // given a hand-built Split manifest (the split operation is D4; D1
        // only needs the value to round-trip)
        let parts: Vec<PartRef> = starts
            .into_iter()
            .enumerate()
            .map(|(i, start_uid)| PartRef {
                start_uid,
                min_uid: start_uid,
                max_uid: start_uid.wrapping_add(i as u64),
                card: i as u32 + 1,
            })
            .collect();
        let value = PostingValue::split(parts.clone());

        // when
        let bytes = value.serialize();
        let back = PostingValue::deserialize(&bytes).unwrap();

        // then
        prop_assert_eq!(&back, &value);
        prop_assert_eq!(back.parts().unwrap(), parts.as_slice());
    }
}

#[test]
fn should_roundtrip_empty_single_and_empty_split() {
    // given / when / then — the canonical "no members" value, and a degenerate
    // (zero-part) split manifest both round-trip.
    let empty_single = PostingValue::empty();
    assert_eq!(
        PostingValue::deserialize(&empty_single.serialize()).unwrap(),
        empty_single
    );

    let empty_split = PostingValue::split(Vec::new());
    assert_eq!(
        PostingValue::deserialize(&empty_split.serialize()).unwrap(),
        empty_split
    );
}

// ── Acceptance #2: set algebra vs BTreeSet<u64> oracle ──────────────────────

proptest! {
    #[test]
    fn should_agree_with_btreeset_oracle_on_set_algebra(
        a in uid_vec_strategy(),
        b in uid_vec_strategy(),
    ) {
        // given two arbitrary uid sets, built both as roaring and as the oracle
        let ra = treemap_of(&a);
        let rb = treemap_of(&b);
        let oa = btreeset_of(&a);
        let ob = btreeset_of(&b);

        // AND
        let and: BTreeSet<u64> = (&ra & &rb).iter().collect();
        prop_assert_eq!(and, &oa & &ob);

        // OR
        let or: BTreeSet<u64> = (&ra | &rb).iter().collect();
        prop_assert_eq!(or, &oa | &ob);

        // NOT / difference, both directions
        let diff_ab: BTreeSet<u64> = (&ra - &rb).iter().collect();
        prop_assert_eq!(&diff_ab, &(&oa - &ob));
        let diff_ba: BTreeSet<u64> = (&rb - &ra).iter().collect();
        prop_assert_eq!(&diff_ba, &(&ob - &oa));

        // cardinality
        prop_assert_eq!(ra.len() as usize, oa.len());
        prop_assert_eq!(rb.len() as usize, ob.len());

        // min / max
        prop_assert_eq!(ra.min(), oa.iter().next().copied());
        prop_assert_eq!(ra.max(), oa.iter().next_back().copied());
        prop_assert_eq!(rb.min(), ob.iter().next().copied());
        prop_assert_eq!(rb.max(), ob.iter().next_back().copied());
    }
}

// ── Acceptance #3: split lifecycle, read side ───────────────────────────────

/// Bin-splits `set` into parts whenever accumulating another uid would push
/// the running part's serialized `Single` posting value over `threshold`
/// bytes. This stands in for D4's real split operation (which also decides
/// *when* and *how* to split against the 512 KiB threshold) — D1 only needs a
/// plausible post-split shape to exercise the read side against.
fn bin_split(set: &RoaringTreemap, threshold: usize) -> (Vec<PartRef>, Vec<RoaringTreemap>) {
    let mut parts = Vec::new();
    let mut part_sets = Vec::new();
    let mut current = RoaringTreemap::new();
    let mut current_start: Option<u64> = None;

    for uid in set.iter() {
        let mut candidate = current.clone();
        candidate.insert(uid);
        let candidate_len = PostingValue::single(candidate.clone()).serialized_len();
        if candidate_len > threshold && !current.is_empty() {
            // flush the current part before starting a new one
            parts.push(PartRef {
                start_uid: current_start.expect("non-empty current has a start"),
                min_uid: current.min().expect("non-empty"),
                max_uid: current.max().expect("non-empty"),
                card: current.len() as u32,
            });
            part_sets.push(std::mem::take(&mut current));
            current_start = None;
        }
        if current_start.is_none() {
            current_start = Some(uid);
        }
        current.insert(uid);
    }
    if !current.is_empty() {
        parts.push(PartRef {
            start_uid: current_start.expect("non-empty current has a start"),
            min_uid: current.min().expect("non-empty"),
            max_uid: current.max().expect("non-empty"),
            card: current.len() as u32,
        });
        part_sets.push(current);
    }
    (parts, part_sets)
}

proptest! {
    #[test]
    fn should_read_split_manifest_matching_part_metadata_and_whole_set(
        vals in prop::collection::vec(0u64..1_000_000, 200..500),
        lo in 0u64..1_000_000,
        span in 1u64..200_000,
    ) {
        // given a set forced to split under a tiny threshold (a few hundred
        // bytes — far below the real 512 KiB, so a few hundred elements
        // reliably span multiple parts)
        let oracle = btreeset_of(&vals);
        let set = treemap_of(&vals);
        let (parts, part_sets) = bin_split(&set, 300);
        prop_assume!(parts.len() >= 2, "need a real multi-part split to exercise this");

        // manifest min/max/card per part match the part's actual roaring
        // min/max/len
        for (part, part_set) in parts.iter().zip(part_sets.iter()) {
            prop_assert_eq!(Some(part.min_uid), part_set.min());
            prop_assert_eq!(Some(part.max_uid), part_set.max());
            prop_assert_eq!(part.card as u64, part_set.len());
            prop_assert_eq!(part.start_uid, part_set.min().unwrap());
        }

        // the Split value itself round-trips
        let manifest = PostingValue::split(parts.clone());
        let back = PostingValue::deserialize(&manifest.serialize()).unwrap();
        prop_assert_eq!(back.parts().unwrap(), parts.as_slice());

        // whole-set union of all parts equals the unsplit oracle
        let whole = PostingValue::union_parts(&part_sets);
        let whole_oracle: BTreeSet<u64> = whole.iter().collect();
        prop_assert_eq!(whole_oracle, oracle.clone());

        // range-filtered read: union only parts whose [min,max] overlaps
        // [lo, hi] — the skip-metadata contract PartRef exists for.
        let hi = lo.saturating_add(span);
        let overlapping: Vec<RoaringTreemap> = parts
            .iter()
            .zip(part_sets.iter())
            .filter(|(p, _)| p.min_uid <= hi && p.max_uid >= lo)
            .map(|(_, s)| s.clone())
            .collect();
        let filtered = PostingValue::union_parts(&overlapping);
        let filtered_oracle: BTreeSet<u64> = oracle.range(lo..=hi).copied().collect();
        let filtered_got: BTreeSet<u64> = filtered
            .iter()
            .filter(|uid| *uid >= lo && *uid <= hi)
            .collect();
        prop_assert_eq!(filtered_got, filtered_oracle);
    }
}

// ── Fail-closed decode: unit cases ──────────────────────────────────────────

#[test]
fn should_reject_unknown_format_on_decode() {
    let bytes = [0x99u8, 0x00];
    assert!(PostingValue::deserialize(&bytes).is_err());
}

#[test]
fn should_reject_bad_kind_byte_on_decode() {
    let bytes = [turbolay::posting::FORMAT_ROARING_V1, 0x42];
    assert!(PostingValue::deserialize(&bytes).is_err());
}

#[test]
fn should_reject_truncated_manifest_on_decode() {
    // claims 3 parts but supplies none
    let mut bytes = vec![turbolay::posting::FORMAT_ROARING_V1, 0x01];
    bytes.extend_from_slice(&3u32.to_le_bytes());
    assert!(PostingValue::deserialize(&bytes).is_err());
}

#[test]
fn should_reject_truncated_single_body_on_decode() {
    let bytes = [turbolay::posting::FORMAT_ROARING_V1, 0x00, 0xff];
    assert!(PostingValue::deserialize(&bytes).is_err());
}

#[test]
fn should_never_panic_on_arbitrary_short_inputs() {
    // given a spread of adversarial byte strings; then decode never panics
    for len in 0..8 {
        let bytes = vec![0xabu8; len];
        let _ = PostingValue::deserialize(&bytes);
    }
}
