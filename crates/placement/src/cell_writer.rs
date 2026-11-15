//! The advisory cell-writer record: one object per cell under
//! `<base>/_cell_writers/v1/`, naming whoever last successfully promoted
//! itself to that cell's writer.
//!
//! ```text
//! PUT <base>/_cell_writers/v1/<cell_id>
//!     {"node_id":"graph-node-1","epoch":18,"at":"2026-07-25T14:32:07Z"}
//! ```
//!
//! Option **3a** of decision 3 in the rendezvous placement design.
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
pub async fn put_cell_writer(store: &dyn ObjectStore, base: &Path, cell_id: &str, record: &CellWriterRecord) -> Result<(), PlacementError> {
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
pub async fn read_cell_writer(store: &dyn ObjectStore, base: &Path, cell_id: &str) -> Result<Option<CellWriterRecord>, PlacementError> {
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
    let bytes = result.bytes().await.map_err(|source| PlacementError::ObjectStore {
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
