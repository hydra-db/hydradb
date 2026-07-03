//! RFC 0004 acceptance surface for the value layer (M1 D2): `NodeRecord` /
//! `TypedValue` / `ChangeRecord` round-trip identity and the `oversize_node`
//! cap, exercised the way `tests/keyspace_props.rs` and `tests/posting_props.rs`
//! exercise their layers — I/O-free, no `Storage`.

use std::collections::BTreeMap;

use bytes::Bytes;
use proptest::prelude::*;
use turbolay::serde::{LabelId, PredId, PropId, Uid};
use turbolay::value::{
    ChangeOp, ChangeRecord, DEFAULT_NODE_SIZE_CAP, LabelDelta, NodeCodec, NodeRecord, TypedValue,
    V0NodeCodec,
};

fn typed_value_strategy() -> impl Strategy<Value = TypedValue> {
    prop_oneof![
        Just(TypedValue::Null),
        any::<bool>().prop_map(TypedValue::Bool),
        any::<i64>().prop_map(TypedValue::Int),
        any::<f64>()
            .prop_filter(
                "exclude NaN (no total order / equality across encodings)",
                |f| !f.is_nan()
            )
            .prop_map(TypedValue::Float),
        ".*".prop_map(TypedValue::String),
        prop::collection::vec(any::<u8>(), 0..64).prop_map(|v| TypedValue::Bytes(Bytes::from(v))),
        any::<i64>().prop_map(TypedValue::DateTime),
    ]
}

proptest! {
    #[test]
    fn should_roundtrip_typed_value(v in typed_value_strategy()) {
        // TypedValue's codec methods are crate-private; round-trip it via a
        // NodeRecord (its public read/write surface) instead of reaching
        // into the module internals.
        let mut props = BTreeMap::new();
        props.insert(PropId(1), v.clone());
        let record = NodeRecord { labels: Vec::new(), props, xid: String::new() };
        let bytes = V0NodeCodec::encode(&record).unwrap();
        let back = V0NodeCodec::decode(&bytes).unwrap();
        prop_assert_eq!(back.props.get(&PropId(1)).cloned(), Some(v));
    }
}

fn label_ids_strategy() -> impl Strategy<Value = Vec<LabelId>> {
    prop::collection::vec(any::<u32>().prop_map(LabelId), 0..8)
}

fn props_strategy() -> impl Strategy<Value = BTreeMap<PropId, TypedValue>> {
    prop::collection::vec(
        (any::<u32>().prop_map(PropId), typed_value_strategy()),
        0..8,
    )
    .prop_map(|pairs| pairs.into_iter().collect())
}

proptest! {
    #[test]
    fn should_roundtrip_node_record(
        labels in label_ids_strategy(),
        props in props_strategy(),
        xid in ".*",
    ) {
        // given an arbitrary NodeRecord
        let record = NodeRecord { labels, props, xid };

        // when
        let bytes = V0NodeCodec::encode(&record).unwrap();
        let back = V0NodeCodec::decode(&bytes).unwrap();

        // then — identity round-trip
        prop_assert_eq!(back, record);
    }

    #[test]
    fn should_reject_node_record_over_the_configured_cap(
        // a payload comfortably larger than a tiny cap, so this is a clean
        // over-cap case regardless of the small fixed encoding overhead
        blob_len in 200usize..2000,
        cap in 1usize..100,
    ) {
        let mut props = BTreeMap::new();
        props.insert(PropId(1), TypedValue::Bytes(Bytes::from(vec![0u8; blob_len])));
        let record = NodeRecord { labels: Vec::new(), props, xid: String::new() };

        prop_assert!(V0NodeCodec::encode_with_cap(&record, cap).is_err());
    }

    #[test]
    fn should_accept_node_record_at_default_cap_when_small(xid in "[a-z]{0,32}") {
        let record = NodeRecord { labels: Vec::new(), props: BTreeMap::new(), xid };
        prop_assert!(V0NodeCodec::encode_with_cap(&record, DEFAULT_NODE_SIZE_CAP).is_ok());
    }
}

fn change_op_strategy() -> impl Strategy<Value = ChangeOp> {
    prop_oneof![
        Just(ChangeOp::UpsertNode),
        Just(ChangeOp::UpsertEdge),
        Just(ChangeOp::DeleteNode),
        Just(ChangeOp::DeleteEdge),
    ]
}

proptest! {
    #[test]
    fn should_roundtrip_change_record(
        seq in any::<u64>(),
        op in change_op_strategy(),
        subject in any::<u64>(),
        pred in proptest::option::of(any::<u32>()),
        object in proptest::option::of(any::<u64>()),
        value in proptest::option::of(typed_value_strategy()),
        added in label_ids_strategy(),
        removed in label_ids_strategy(),
        has_delta in any::<bool>(),
    ) {
        // given an arbitrary ChangeRecord across the full Option surface
        let record = ChangeRecord {
            seq,
            op,
            subject_uid: Uid(subject),
            pred_id: pred.map(PredId),
            object_uid: object.map(Uid),
            value,
            label_delta: has_delta.then_some(LabelDelta { added, removed }),
        };

        // when
        let bytes = record.encode();
        let back = ChangeRecord::decode(&bytes).unwrap();

        // then
        prop_assert_eq!(back, record);
    }
}

// ── Fail-closed decode: unit cases ──────────────────────────────────────────

#[test]
fn should_reject_truncated_node_record() {
    let record = NodeRecord {
        labels: vec![LabelId(1)],
        props: BTreeMap::new(),
        xid: "x".to_string(),
    };
    let bytes = V0NodeCodec::encode(&record).unwrap();
    for cut in 0..bytes.len() {
        assert!(V0NodeCodec::decode(&bytes[..cut]).is_err());
    }
}

#[test]
fn should_reject_truncated_change_record() {
    let record = ChangeRecord {
        seq: 1,
        op: ChangeOp::UpsertNode,
        subject_uid: Uid(1),
        pred_id: None,
        object_uid: None,
        value: None,
        label_delta: None,
    };
    let bytes = record.encode();
    for cut in 0..bytes.len() {
        assert!(ChangeRecord::decode(&bytes[..cut]).is_err());
    }
}

#[test]
fn should_never_panic_on_arbitrary_bytes() {
    for len in 0..12 {
        let bytes = vec![0x5au8; len];
        let _ = V0NodeCodec::decode(&bytes);
        let _ = ChangeRecord::decode(&bytes);
    }
}
