//! M1 pre-flight spike (RFC 0005): confirm `RoaringTreemap` portable
//! serialize/deserialize is stable and round-trips, that iteration is
//! ascending-uid (the RFC 0003 contract), and measure serialized size vs a
//! naive 8-bytes-per-uid `UidPack`-style baseline on realistic adjacency.
//!
//! This is a throwaway validation harness, not part of the shipped crate — it
//! exists to de-risk deliverable #1 (`PostingValue`) before it is built. Run
//! with `cargo test --test roaring_spike -- --nocapture` to see the size table.

use roaring::RoaringTreemap;

/// Deterministic LCG so the spike needs no `rand` dep and is reproducible.
struct Lcg(u64);
impl Lcg {
    fn next(&mut self) -> u64 {
        // Numerical Recipes constants.
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        self.0
    }
}

/// Portable serialize round-trips identically and preserves ascending order.
#[test]
fn roaring_treemap_portable_roundtrip_is_identity_and_ascending() {
    let mut src = RoaringTreemap::new();
    // A mix: a dense low run, a sparse high scatter, and a 32-bit boundary
    // crossing (treemap = one bitmap per high 32 bits — exercise two buckets).
    for i in 0..10_000u64 {
        src.insert(i);
    }
    let mut lcg = Lcg(0x1234_5678);
    for _ in 0..10_000 {
        src.insert(lcg.next() % 5_000_000);
    }
    src.insert(u32::MAX as u64); // last uid in bucket 0
    src.insert(u32::MAX as u64 + 1); // first uid in bucket 1
    src.insert(u64::MAX);

    // Serialize via the portable format, read it back.
    let mut buf = Vec::new();
    src.serialize_into(&mut buf).expect("serialize_into");
    let back = RoaringTreemap::deserialize_from(&buf[..]).expect("deserialize_from");

    assert_eq!(src, back, "round-trip must be identity");
    assert_eq!(src.len(), back.len());

    // Iteration is strictly ascending — the ordering contract the whole read
    // path (RFC 0003/0005) relies on.
    let mut prev: Option<u64> = None;
    for uid in back.iter() {
        if let Some(p) = prev {
            assert!(
                uid > p,
                "iteration must be strictly ascending: {p} then {uid}"
            );
        }
        prev = Some(uid);
    }
    assert_eq!(prev, Some(u64::MAX));
}

/// Set algebra matches expectations (sanity before the full oracle proptest).
#[test]
fn roaring_set_algebra_basics() {
    let a: RoaringTreemap = (0..1000u64).collect();
    let b: RoaringTreemap = (500..1500u64).collect();

    assert_eq!((&a & &b).len(), 500, "AND");
    assert_eq!((&a | &b).len(), 1500, "OR");
    assert_eq!((&a - &b).len(), 500, "difference");
    assert_eq!(a.min(), Some(0));
    assert_eq!(a.max(), Some(999));
    assert_eq!(a.len(), 1000);
}

/// Measure serialized size vs a naive fixed-width 8-bytes-per-uid baseline
/// (the "UidPack-style" strawman) across three realistic adjacency shapes.
/// Validates Q2 (roaring is the right default) — prints a table, asserts
/// roaring is not pathologically worse than the naive baseline.
#[test]
fn roaring_serialized_size_vs_naive_baseline() {
    struct Shape {
        name: &'static str,
        build: fn() -> RoaringTreemap,
    }

    let shapes = [
        Shape {
            name: "dense-degree-10k (0..10000)",
            build: || (0..10_000u64).collect(),
        },
        Shape {
            name: "sparse-degree-10k (scatter over 100M)",
            build: || {
                let mut lcg = Lcg(0xDEAD_BEEF);
                let mut s = RoaringTreemap::new();
                while s.len() < 10_000 {
                    s.insert(lcg.next() % 100_000_000);
                }
                s
            },
        },
        Shape {
            name: "clustered-degree-50k (runs)",
            build: || {
                let mut lcg = Lcg(0xF00D);
                let mut s = RoaringTreemap::new();
                let mut base = 0u64;
                while s.len() < 50_000 {
                    base += lcg.next() % 10_000;
                    for j in 0..(lcg.next() % 200) {
                        s.insert(base + j);
                    }
                }
                s
            },
        },
    ];

    println!(
        "\n{:<40} {:>10} {:>14} {:>14} {:>8}",
        "shape", "card", "roaring B", "naive 8B/uid", "ratio"
    );
    for shape in shapes {
        let s = (shape.build)();
        let card = s.len();
        let mut buf = Vec::new();
        s.serialize_into(&mut buf).unwrap();
        let roaring_bytes = buf.len();
        let naive_bytes = (card as usize) * 8;
        let ratio = roaring_bytes as f64 / naive_bytes as f64;
        println!(
            "{:<40} {:>10} {:>14} {:>14} {:>8.3}",
            shape.name, card, roaring_bytes, naive_bytes, ratio
        );

        // Dense/clustered shapes should compress well below the naive baseline;
        // even worst-case sparse random stays within a small constant of it.
        assert!(
            roaring_bytes < naive_bytes * 2,
            "{}: roaring {roaring_bytes}B should not blow past 2x naive {naive_bytes}B",
            shape.name
        );
    }
    println!();
}
