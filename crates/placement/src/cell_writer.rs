//! The advisory cell-writer record: one object per cell under
//! `<base>/_cell_writers/v1/`, naming whoever last successfully promoted
//! itself to that cell's writer.
//!
//! ```text
//! PUT <base>/_cell_writers/v1/<cell_id>
//!     {"node_id":"graph-node-1","epoch":18,"at":"2026-07-25T14:32:07Z"}
//! ```
//!
//! Option **3a** of decision 3 in `docs/plans/2026-07-25-rendezvous-placement.md`.
//! It exists because the prod incident behind that plan
//! (`cell-writer-fencing-pingpong`) was, in the end, a *diagnostic* failure:
//! three nodes traded one cell's SlateDB writer epoch, and the logs said a write
//! had been fenced without ever saying by whom. SlateDB's epoch carries no
//! identity, so nothing in the manifest can answer that question. This record
//! is the answer, written where the next person will look for it.
//!
//! # Three rules, and none of them is optional
//!
//! **1. It is never consulted to decide whether to promote.** That is
//! rendezvous' job — [`crate::hash::owner`] over the live set — and it is the
//! only thing allowed to have it. Reading this record and acting on it is option
//! **3b**, which decision 3 rejects on three counts: it buys nothing 3a does not
//! already give, it puts a GET on the write path, and it *still* cannot stop a
//! partitioned node, because between the read and the open the world can change
//! and there is no CAS to notice. A read-then-act race that looks like it works
//! is worse than no lease at all, because people trust it. Nothing in this
//! module is reachable from a promotion decision, and [`read_cell_writer`]'s
//! callers are enumerated below precisely so that stays checkable.
//!
//! **2. The SlateDB writer epoch remains the only authority.** This record
//! answers *who last successfully promoted*, not *who holds the writer now*.
//! Those differ the instant a peer fences the recorded node, which is precisely
//! the situation it is read in. **If the record disagrees with the manifest, the
//! manifest is right.** Decision 3 asks for that in writing so it is never
//! re-read as a guarantee by someone who finds an object called `_cell_writers`
//! and assumes it means what its name suggests.
//!
//! **3. It is advisory, so a failed PUT must not fail the promotion.** The
//! caller logs at `warn!` and carries on: the write succeeded, and this is
//! diagnostics. A promotion that failed because its diagnostics could not be
//! written would be strictly worse than having no record at all — it would turn
//! an observability feature into an availability dependency on a second object
//! store request. [`put_cell_writer`] still returns a `Result`, because a
//! library that swallowed the error could not say *why* the record is missing;
//! discarding it is the caller's job, and `ensure_local_writer` is the caller
//! that does it.
//!
//! # Where it is written and read
//!
//! Written on exactly one path: `RoutedGraphCluster::ensure_local_writer`
//! (`src/engine/cluster.rs`), after a promotion that really opened a writer, and
//! only then. A shard that already held its writer re-records nothing, which is
//! what keeps the PUT off the write path — it happens once per epoch claimed,
//! not once per write.
//!
//! Read on paths that are off the write path by construction:
//!
//! 1. **Logging a `CloseReason::Fenced`** (`src/core/state.rs`), so the next
//!    incident is one line — this node lost the epoch, and here is who took it.
//! 2. **Building the `NotCellWriter { owner }` hint** of touch point (c). Today
//!    that hint comes from rendezvous in the `Remote` arm, which needs no I/O,
//!    and is deliberately empty in the `Unknown` arm; see the note on
//!    [`read_cell_writer`].
//!
//! # Layout
//!
//! One object per cell, named by the cell id, under a `v1` directory — the same
//! shape as [`crate::heartbeat`], for the same reasons: an incompatible format
//! lists under a prefix the old fleet never reads, and the name is validated by
//! the one rule both prefixes share.
//!
//! `<base>` is the *scoped* store path — the directory the cell's own SlateDB
//! store sits in — and not the node-level data path that heartbeats use. Cell
//! ids are unique within a scope and repeat across them, so a record keyed by
//! cell id alone under a shared base would have two scopes overwrite each
//! other's attribution.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use slatedb::object_store::{path::Path, ObjectStore, ObjectStoreExt, PutPayload};

use crate::heartbeat::{name_rule_violation, versioned_prefix, PlacementError};

/// Where cell-writer records live, relative to the cell's scoped base path.
///
/// The `v1` is a directory and not a filename suffix, for the reason spelled
/// out on [`crate::heartbeat::HEARTBEAT_PREFIX`]: a future incompatible format
/// lists somewhere the old fleet never looks.
pub const CELL_WRITER_PREFIX: &str = "_cell_writers/v1";

/// Who last successfully promoted itself to a cell's writer.
///
/// **Advisory.** See the module docs: this is not a lease, it prevents nothing,
/// and the manifest wins every disagreement.
///
/// The three fields are exactly the body decision 3 specifies, and the version
/// lives in the prefix rather than in the body — there is no `version` field to
/// forget to bump, because the prefix cannot be forgotten.
///
/// **No `#[serde(deny_unknown_fields)]`, deliberately.** A rolling restart
/// replaces pods one at a time, so an old reader will read bodies written by a
/// new writer. Unknown fields must be ignored, which is what makes adding one a
/// compatible change; removing or retyping one is not, and is what the `v1` in
/// the prefix is for.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CellWriterRecord {
    /// The node that promoted. The whole point of the record: SlateDB's writer
    /// epoch is a monotonic CAS-backed lease that carries no identity, so this
    /// is the only place the identity is written down.
    pub node_id: String,

    /// The SlateDB writer epoch this node claimed, read from
    /// `VersionedManifest::writer_epoch()` after the promotion succeeded.
    ///
    /// It is what makes two records comparable: a fenced node's epoch is lower
    /// than the epoch of whoever fenced it, so a record with a *higher* epoch
    /// than the one in your own handle names the node that took it from you.
    pub epoch: u64,

    /// When the promotion completed, by the promoting node's own clock.
    ///
    /// Its clock, not the store's — like [`crate::heartbeat::Heartbeat`]'s
    /// timestamps, and unlike liveness, which is measured from object-storage
    /// `LastModified` precisely because a local clock cannot be compared across
    /// nodes. This field is for reading in a log line, not for ordering
    /// anything; `epoch` is the order.
    pub at: DateTime<Utc>,
}

impl CellWriterRecord {
    /// A record for a promotion that has already succeeded.
    ///
    /// `at` is a parameter and not a clock read — decision 10. Only the caller
    /// reads the clock, and a caller that has to pass the instant cannot forget
    /// that it is *its* clock it is writing down.
    pub fn new(node_id: impl Into<String>, epoch: u64, at: DateTime<Utc>) -> Self {
        Self {
            node_id: node_id.into(),
            epoch,
            at,
        }
    }
}

/// Check a cell id for use as a cell-writer object name.
///
/// The same rule as [`crate::heartbeat::validate_node_id`], sharing its
/// implementation: nonempty, `[A-Za-z0-9_-.]`, and not a relative path segment.
/// That matches the kernel's `validate_component` (`src/codec.rs:175`), which
/// every cell id has already passed by the time it reaches here — so this is a
/// second line of defence, and the line that keeps a bad id from becoming an
/// object nobody can attribute.
pub fn validate_cell_id(cell_id: &str) -> Result<(), PlacementError> {
    match name_rule_violation(cell_id) {
        Some(reason) => Err(PlacementError::InvalidCellId {
            cell_id: cell_id.to_string(),
            reason,
        }),
        None => Ok(()),
    }
}

/// The cell-writer object name for a cell.
///
/// The encoding is the identity, and it is a function anyway for the same
/// reason [`crate::heartbeat::object_name`] is: it is half of a frozen name
/// format, and validation belongs on the path that builds the name rather than
/// scattered over its callers.
pub fn object_name(cell_id: &str) -> Result<String, PlacementError> {
    validate_cell_id(cell_id)?;
    Ok(cell_id.to_string())
}

/// The cell id a cell-writer object name encodes, or `None` if it is not one.
///
/// The inverse of [`object_name`], rejecting exactly what [`validate_cell_id`]
/// rejects.
pub fn parse_object_name(name: &str) -> Option<&str> {
    validate_cell_id(name).ok().map(|()| name)
}

/// The prefix every cell-writer record lives under.
pub fn cell_writer_prefix(base: &Path) -> Path {
    versioned_prefix(base, CELL_WRITER_PREFIX)
}

/// The full path of one cell's record.
pub fn cell_writer_path(base: &Path, cell_id: &str) -> Result<Path, PlacementError> {
    let name = object_name(cell_id)?;
    Ok(cell_writer_prefix(base).join(name))
}

/// Record that `record.node_id` has promoted itself to `cell_id`'s writer.
///
/// **Call this only after a promotion has succeeded, and discard the error.**
/// Both halves matter. Writing it before or instead of a successful promotion
/// would make it a claim rather than a record, which is the 3b race decision 3
/// rejects; and propagating the error would let a store hiccup fail a write that
/// has already committed its epoch. The caller logs at `warn!` and carries on —
/// see the module docs, rule 3.
pub async fn put_cell_writer(
    store: &dyn ObjectStore,
    base: &Path,
    cell_id: &str,
    record: &CellWriterRecord,
) -> Result<(), PlacementError> {
    let path = cell_writer_path(base, cell_id)?;
    let payload = serde_json::to_vec(record).map_err(|source| PlacementError::EncodeRecord {
        cell_id: cell_id.to_string(),
        source,
    })?;
    store
        .put(&path, PutPayload::from(payload))
        .await
        .map_err(|source| PlacementError::ObjectStore {
            operation: "put",
            path: path.to_string(),
            source,
        })?;
    Ok(())
}

/// Who last successfully promoted itself to `cell_id`'s writer, or `None` if
/// nobody ever has.
///
/// **Never call this to decide whether to promote.** That is rule 1 of the
/// module docs and the whole of decision 3: ownership comes from rendezvous over
/// the live set, synchronously and with no I/O. This function performs a GET,
/// and every caller of it must be on a path where a GET is affordable and where
/// the answer is used for *attribution* — a log line, an error hint — and never
/// for control flow.
///
/// A missing object is `Ok(None)` and not an error: a cell nobody has ever
/// written to has no writer to name, and that is a fact rather than a failure.
/// An object that will not parse *is* an error, because something wrote it and
/// the prefix promised what it would contain.
///
/// # Why the `NotCellWriter` hint does not call this today
///
/// Decision 3 names the owner hint as one of this function's two read paths.
/// Since then, §4(b)'s four-state refinement settled that the hint comes from
/// rendezvous in the `Remote` arm — which already knows the owner and needs no
/// I/O — and is deliberately empty in the `Unknown` arm, where this node has
/// shed its view. That second case is the one where the record would be the only
/// available answer, and it is also the case where asking for it is futile: a
/// shed view means the node's LIST against this very store has been failing for
/// longer than the grace window, so the GET would fail with it, once per refused
/// write. The hint therefore stays rendezvous-only, and this function is called
/// by the fence log.
pub async fn read_cell_writer(
    store: &dyn ObjectStore,
    base: &Path,
    cell_id: &str,
) -> Result<Option<CellWriterRecord>, PlacementError> {
    let path = cell_writer_path(base, cell_id)?;
    let result = match store.get(&path).await {
        Ok(result) => result,
        Err(slatedb::object_store::Error::NotFound { .. }) => return Ok(None),
        Err(source) => {
            return Err(PlacementError::ObjectStore {
                operation: "get",
                path: path.to_string(),
                source,
            })
        }
    };
    let bytes = result
        .bytes()
        .await
        .map_err(|source| PlacementError::ObjectStore {
            operation: "get",
            path: path.to_string(),
            source,
        })?;
    let record = serde_json::from_slice(&bytes).map_err(|source| PlacementError::DecodeRecord {
        path: path.to_string(),
        source,
    })?;
    Ok(Some(record))
}

#[cfg(test)]
mod tests {
    use super::*;

    use chrono::TimeZone;
    use slatedb::object_store::memory::InMemory;

    use crate::fault_store::{FaultStore, Op};

    const CELL: &str = "cell-a";
    const NODE: &str = "graph-node-1";

    fn base() -> Path {
        Path::from("acme/graphs/social")
    }

    fn at() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 7, 25, 14, 32, 7)
            .single()
            .expect("a real instant")
    }

    fn record() -> CellWriterRecord {
        CellWriterRecord::new(NODE, 18, at())
    }

    #[test]
    fn object_names_round_trip_for_every_realistic_cell_id() {
        for id in [
            "cell-a",
            "cell_a",
            "reddit-search",
            "turbolay.cell.0",
            "A9",
            "n",
            "0",
            "-",
        ] {
            let name = object_name(id).unwrap_or_else(|e| panic!("{id:?} should be valid: {e}"));
            assert_eq!(parse_object_name(&name), Some(id), "{id:?}");
        }
    }

    /// The cell id becomes a path segment, so anything that would break the name
    /// or the path has to be caught before a request is issued.
    #[test]
    fn cell_ids_that_would_break_the_name_or_the_path_are_rejected() {
        for bad in [
            "", "/", "a/b", ".", "..", "a b", "a:b", "a%b", "a\nb", "ü", "a\\b", "*",
        ] {
            assert!(validate_cell_id(bad).is_err(), "{bad:?}");
            assert!(object_name(bad).is_err(), "{bad:?}");
            assert_eq!(parse_object_name(bad), None, "{bad:?}");
        }
    }

    /// The rule is shared with the heartbeat's, not merely similar. If these
    /// ever diverge, an id one prefix accepts is one the other percent-encodes.
    #[test]
    fn cell_ids_and_node_ids_are_validated_by_the_same_rule() {
        for id in ["cell-a", "", "/", "..", "a b", "graph-node-0", "ü"] {
            assert_eq!(
                validate_cell_id(id).is_ok(),
                crate::heartbeat::validate_node_id(id).is_ok(),
                "{id:?} is judged differently by the two validators"
            );
        }
    }

    #[test]
    fn records_nest_under_the_versioned_prefix() {
        let path = cell_writer_path(&base(), CELL).expect("valid id");
        assert_eq!(path.as_ref(), "acme/graphs/social/_cell_writers/v1/cell-a");
        // The prefix's `/` must stay a path separator, not become `%2F`.
        assert_eq!(
            cell_writer_prefix(&base()).as_ref(),
            "acme/graphs/social/_cell_writers/v1"
        );
    }

    #[test]
    fn an_invalid_cell_id_fails_before_a_path_is_built() {
        let err = cell_writer_path(&base(), "a/b").expect_err("slashes are rejected");
        assert!(matches!(err, PlacementError::InvalidCellId { .. }), "{err}");
    }

    /// The body is exactly the three fields decision 3 specifies, in the shape
    /// the plan writes them. If this fails, the wire format moved.
    #[test]
    fn the_body_is_the_json_the_plan_specifies() {
        let json = serde_json::to_string(&record()).expect("encodable");
        assert_eq!(
            json,
            r#"{"node_id":"graph-node-1","epoch":18,"at":"2026-07-25T14:32:07Z"}"#
        );
    }

    #[test]
    fn a_record_round_trips_through_json() {
        let json = serde_json::to_string(&record()).expect("encodable");
        let back: CellWriterRecord = serde_json::from_str(&json).expect("decodable");
        assert_eq!(back.node_id, NODE);
        assert_eq!(back.epoch, 18);
        assert_eq!(back.at, at());
    }

    /// A rolling restart replaces pods one at a time, so an old reader will read
    /// a new writer's body. If this fails, `deny_unknown_fields` has been added
    /// and the next upgrade blinds the fence log mid-roll.
    #[test]
    fn unknown_fields_deserialize_so_a_mixed_version_fleet_coexists() {
        let json = r#"{
            "node_id": "graph-node-1",
            "epoch": 18,
            "at": "2026-07-25T14:32:07Z",
            "manifest_id": 91,
            "some_future_object": {"x": 1}
        }"#;
        let record: CellWriterRecord =
            serde_json::from_str(json).expect("unknown fields are ignored");
        assert_eq!(record.node_id, NODE);
        assert_eq!(record.epoch, 18);
    }

    #[tokio::test]
    async fn a_written_record_reads_back() {
        let store = InMemory::new();
        put_cell_writer(&store, &base(), CELL, &record())
            .await
            .expect("an in-memory PUT lands");

        let back = read_cell_writer(&store, &base(), CELL)
            .await
            .expect("readable")
            .expect("just written");
        assert_eq!(back.node_id, NODE);
        assert_eq!(back.epoch, 18);
        assert_eq!(back.at, at());
    }

    /// A cell nobody has ever promoted for has no writer to name. That is a
    /// fact, not a failure — the fence log prints "unknown" and moves on.
    #[tokio::test]
    async fn a_cell_that_was_never_promoted_reads_as_none() {
        let store = InMemory::new();
        assert!(read_cell_writer(&store, &base(), CELL)
            .await
            .expect("a missing record is not an error")
            .is_none());
    }

    /// Each cell gets its own object; a second cell's promotion must not
    /// overwrite the first's attribution.
    #[tokio::test]
    async fn records_for_different_cells_do_not_collide() {
        let store = InMemory::new();
        put_cell_writer(
            &store,
            &base(),
            "cell-a",
            &CellWriterRecord::new("node-a", 1, at()),
        )
        .await
        .expect("landed");
        put_cell_writer(
            &store,
            &base(),
            "cell-b",
            &CellWriterRecord::new("node-b", 2, at()),
        )
        .await
        .expect("landed");

        let a = read_cell_writer(&store, &base(), "cell-a")
            .await
            .expect("readable")
            .expect("written");
        let b = read_cell_writer(&store, &base(), "cell-b")
            .await
            .expect("readable")
            .expect("written");
        assert_eq!((a.node_id.as_str(), a.epoch), ("node-a", 1));
        assert_eq!((b.node_id.as_str(), b.epoch), ("node-b", 2));
    }

    /// The record is last-writer-wins by design: it answers "who promoted most
    /// recently", so the newest epoch is the one that must survive.
    #[tokio::test]
    async fn a_later_promotion_overwrites_the_earlier_record() {
        let store = InMemory::new();
        put_cell_writer(
            &store,
            &base(),
            CELL,
            &CellWriterRecord::new("node-a", 18, at()),
        )
        .await
        .expect("landed");
        put_cell_writer(
            &store,
            &base(),
            CELL,
            &CellWriterRecord::new("node-b", 19, at()),
        )
        .await
        .expect("landed");

        let back = read_cell_writer(&store, &base(), CELL)
            .await
            .expect("readable")
            .expect("written");
        assert_eq!(back.node_id, "node-b");
        assert_eq!(back.epoch, 19);
    }

    /// The whole of rule 3, at this layer: a failing PUT surfaces as an error
    /// the caller can log. It is the *caller's* job to discard it, and
    /// `cluster.rs`'s tests pin that the promotion survives.
    #[tokio::test]
    async fn a_failing_put_is_reported_and_not_swallowed() {
        let store = FaultStore::in_memory();
        store.fail_all(Op::Put);

        let err = put_cell_writer(store.as_ref(), &base(), CELL, &record())
            .await
            .expect_err("the injected fault surfaces");
        assert!(
            matches!(
                err,
                PlacementError::ObjectStore {
                    operation: "put",
                    ..
                }
            ),
            "{err}"
        );
        assert_eq!(store.count(Op::Put), 1);

        // And nothing was written, so a reader sees the absence rather than a
        // half-written body.
        store.heal();
        assert!(read_cell_writer(store.as_ref(), &base(), CELL)
            .await
            .expect("readable")
            .is_none());
    }

    /// A failing GET is an error and not a silent `None`. Reading it as "nobody
    /// has ever promoted" would print a confidently wrong fence log.
    #[tokio::test]
    async fn a_failing_get_is_an_error_and_not_an_absent_record() {
        let store = FaultStore::in_memory();
        put_cell_writer(store.as_ref(), &base(), CELL, &record())
            .await
            .expect("landed");
        store.fail_all(Op::Get);

        let err = read_cell_writer(store.as_ref(), &base(), CELL)
            .await
            .expect_err("the injected fault surfaces");
        assert!(
            matches!(
                err,
                PlacementError::ObjectStore {
                    operation: "get",
                    ..
                }
            ),
            "{err}"
        );
    }

    /// Something wrote an object under a prefix that promised a record. Saying
    /// so beats reporting "nobody has promoted this cell".
    #[tokio::test]
    async fn an_unparseable_record_is_an_error_naming_the_object() {
        let store = InMemory::new();
        let path = cell_writer_path(&base(), CELL).expect("valid id");
        store
            .put(&path, PutPayload::from_static(b"not json"))
            .await
            .expect("in memory");

        let err = read_cell_writer(&store, &base(), CELL)
            .await
            .expect_err("a body that will not parse is an error");
        match err {
            PlacementError::DecodeRecord { path: reported, .. } => {
                assert_eq!(reported, path.to_string());
            }
            other => panic!("{other}"),
        }
    }

    /// Writing with an id the name format cannot carry must not reach the store
    /// at all — an object nobody can attribute is worse than no object.
    #[tokio::test]
    async fn writing_with_an_invalid_cell_id_never_reaches_the_store() {
        let store = FaultStore::in_memory();
        let err = put_cell_writer(store.as_ref(), &base(), "a/b", &record())
            .await
            .expect_err("slashes are rejected");
        assert!(matches!(err, PlacementError::InvalidCellId { .. }), "{err}");
        assert_eq!(store.count(Op::Put), 0, "a request was issued anyway");
    }
}
