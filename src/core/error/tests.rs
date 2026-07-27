use super::*;

/// The ten reachable classes, written out rather than derived from
/// [`GraphError::CLASSES`], so that the array and the vocabulary are two
/// statements that can disagree instead of one that cannot be checked.
///
/// Ten and not eleven: `turbolay_telemetry::ErrorClass` also has `Other`, and
/// nothing in this tree constructs it. There is no `other` arm here and there
/// must not be one — the whole value of the taxonomy is that an unclassified
/// variant is a build failure rather than a bucket nobody can act on.
const EXPECTED_CLASSES: [&str; 10] = [
    "contention",
    "fencing",
    "freshness",
    "admission",
    "query",
    "authz",
    "corruption",
    "config",
    "storage",
    "kernel",
];

#[test]
fn the_vocabulary_is_exactly_the_ten_reachable_classes() {
    assert_eq!(GraphError::CLASSES, EXPECTED_CLASSES);
    assert_eq!(GraphError::CLASS_COUNT, 10);
}

#[test]
fn class_strings_are_unique() {
    let mut names = GraphError::CLASSES;
    names.sort_unstable();
    let before = names.len();
    let unique = {
        let mut unique: Vec<&str> = names.to_vec();
        unique.dedup();
        unique.len()
    };
    assert_eq!(before, unique, "two classes share a string");
}

/// The index constants are the only place a name and its slot can disagree:
/// `class_index` returns one of them and `class` looks the slot up, so if a
/// constant points at the wrong entry every consumer is wrong together and
/// silently. This is the check that makes them one thing.
#[test]
fn class_constants_index_their_own_name() {
    assert_eq!(
        GraphError::CLASSES[GraphError::CLASS_CONTENTION],
        "contention"
    );
    assert_eq!(GraphError::CLASSES[GraphError::CLASS_FENCING], "fencing");
    assert_eq!(
        GraphError::CLASSES[GraphError::CLASS_FRESHNESS],
        "freshness"
    );
    assert_eq!(
        GraphError::CLASSES[GraphError::CLASS_ADMISSION],
        "admission"
    );
    assert_eq!(GraphError::CLASSES[GraphError::CLASS_QUERY], "query");
    assert_eq!(GraphError::CLASSES[GraphError::CLASS_AUTHZ], "authz");
    assert_eq!(
        GraphError::CLASSES[GraphError::CLASS_CORRUPTION],
        "corruption"
    );
    assert_eq!(GraphError::CLASSES[GraphError::CLASS_CONFIG], "config");
    assert_eq!(GraphError::CLASSES[GraphError::CLASS_STORAGE], "storage");
    assert_eq!(GraphError::CLASSES[GraphError::CLASS_KERNEL], "kernel");
}

/// `class()` is `CLASSES[class_index()]`, so the two cannot drift — but only as
/// long as every index the match can return is in range. An out-of-range
/// constant is a panic in `class`, not a compile error, and this is where it
/// would surface.
#[test]
fn class_and_class_index_agree_for_every_class() {
    for error in GraphError::one_per_class() {
        let index = error.class_index();
        assert!(
            index < GraphError::CLASS_COUNT,
            "{error} indexes out of range"
        );
        assert_eq!(error.class(), GraphError::CLASSES[index]);
    }
}

/// The sample table other tests index by class position. If it ever stops being
/// in `CLASSES` order, every per-slot assertion built on it becomes vacuous
/// rather than failing, which is the worse of the two outcomes.
#[test]
fn one_error_per_class_is_in_classes_order() {
    for (index, error) in GraphError::one_per_class().into_iter().enumerate() {
        assert_eq!(error.class_index(), index, "{error} is out of order");
        assert_eq!(error.class(), EXPECTED_CLASSES[index]);
    }
}

/// Every class must be reachable from a real error. A class in the vocabulary
/// that no variant maps to is a label an operator will wait for and never see,
/// and it is exactly what an `other` arm would quietly produce.
#[test]
fn every_class_is_reached_by_some_error() {
    let mut reached = [false; GraphError::CLASS_COUNT];
    for error in GraphError::one_per_class() {
        reached[error.class_index()] = true;
    }
    assert!(reached.into_iter().all(|hit| hit));
}

/// `storage` is the one class with two source variants and the sample table
/// carries only one of them, so the other half of that arm would otherwise go
/// unexercised. Both must land in the same slot: a dashboard that separated
/// SlateDB from its object store would be splitting one question in two.
#[test]
fn both_storage_variants_share_the_storage_class() {
    let object_store = GraphError::ObjectStore(slatedb::object_store::Error::Generic {
        store: "test",
        source: "injected".into(),
    });
    let slate = GraphError::Slate(slatedb::Error::internal("injected".to_string()));

    assert_eq!(object_store.class_index(), GraphError::CLASS_STORAGE);
    assert_eq!(slate.class_index(), GraphError::CLASS_STORAGE);
    assert_eq!(slate.class(), "storage");
}
