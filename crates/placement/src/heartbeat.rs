//! Heartbeats: one object per ready node under `<base>/_graph_nodes/v1/`.
//!
//! A node PUTs `<base>/_graph_nodes/v1/<node-id>` every `heartbeat_interval`
//! while it is ready, and DELETEs it the moment it is not — on unreadiness and
//! in the SIGTERM handler, before draining. Presence of the object *is* the
//! readiness signal, which is why there is no `ready` field in the body and why
//! deleting the wrong node's object would be the same bug as lying about it.
//!
//! Two rules are frozen here, and both exist to keep placement off the network:
//!
//! 1. **Everything placement needs is in the object NAME.** The body is
//!    observability and is never fetched on the placement path. A LIST returns
//!    the name and `LastModified` in one request; a GET per node would turn
//!    every routing refresh into an N-request fan-out, which is precisely the
//!    `/readyz` probe this replaces.
//! 2. **Liveness is the object's `LastModified`, never a local clock.** Every
//!    node reads the same timestamps from the same store, so two nodes asking at
//!    the same instant compute the same live set. Rendezvous only converges if
//!    they do.
//!
//! ## Time enters here and nowhere else
//!
//! [`list_heartbeats`] is the single impure boundary in the crate and the only
//! function that is told what time it is. It collapses each object's
//! `LastModified` into a [`HeartbeatEntry::age`] — a `Duration` — and everything
//! downstream is pure, synchronous and clock-free. Nothing in this file calls
//! `Utc::now()` or `Instant::now()`; not the tests either, which derive their
//! `now` from the object's own `LastModified` and so assert exact ages instead
//! of tolerances. Decision 10 of `docs/plans/2026-07-25-rendezvous-placement.md`.
//!
//! A heartbeat stamped *later* than the reader's `now` — reverse clock skew, or
//! a store whose clock runs ahead — saturates to [`Duration::ZERO`]. Maximally
//! fresh, not an error: the alternative is that a skewed clock silently kills a
//! healthy node.
//!
//! ## What is not an error
//!
//! A missing prefix and an empty LIST are the same valid answer — *I am alone* —
//! and neither is an `Err`. Decision 7 hangs a bounded grace period off LIST
//! *failure*, after which the node sheds ownership and withdraws its own
//! heartbeat; routing an empty fleet down that path would shed on the first
//! cold start. Only a real transport failure is an `Err`.
//!
//! Likewise a stray object under the prefix is skipped, not fatal. One
//! unparseable name must not poison the live set for the whole fleet.
//!
//! Ported from `sleet/src/heartbeat.rs` and `sleet/src/root.rs:129-220`, minus
//! sleet's service-letter suffix, which has no analogue here. The suffix is
//! where per-node cell subsets would go if nodes ever stop serving every cell —
//! in the *name*, so placement still needs no GETs. [`object_name`] and
//! [`parse_object_name`] are the seam that change lands on, which is why they
//! exist as a pair despite the encoding being the identity today.

use std::time::Duration;

use chrono::{DateTime, Utc};
use futures::TryStreamExt;
use serde::{Deserialize, Serialize};
use slatedb::object_store::{path::Path, ObjectStore, ObjectStoreExt, PutPayload};

/// Current heartbeat format version, and the `v1` in the object prefix.
///
/// Bumped only on an *incompatible* change. Adding a body field is compatible
/// by construction — see [`Heartbeat`] — so this is expected to stay at 1.
pub const VERSION: u32 = 1;

/// Where heartbeat objects live, relative to the store's base path.
///
/// The `v1` is a directory rather than a filename suffix so that a future
/// incompatible format lists under a prefix the old fleet never reads. A
/// mixed-version fleet during that migration sees two disjoint live sets, which
/// is bad but visible; sharing a prefix would give it one live set with entries
/// it cannot parse, which is worse.
pub const HEARTBEAT_PREFIX: &str = "_graph_nodes/v1";

/// Everything this crate can fail at.
///
/// It lives in this module because this module is the only one that performs
/// I/O or parses anything; the placement and liveness functions are total.
#[derive(Debug, thiserror::Error)]
pub enum PlacementError {
    /// A node id that cannot be encoded into an object name or a path.
    ///
    /// Caught before any request is issued, so a misconfigured `GRAPH_NODE_ID`
    /// fails at the first publish rather than writing an object nobody can
    /// attribute.
    #[error("invalid node id {node_id:?}: {reason}")]
    InvalidNodeId {
        /// The rejected id, quoted in the message because the interesting cases
        /// are whitespace and empty.
        node_id: String,
        /// Which rule it broke.
        reason: &'static str,
    },

    /// An object-store request failed.
    ///
    /// Carries the operation and path because the caller's next decision
    /// depends on which one broke: decision 7 sheds ownership on a failing LIST
    /// while PUTs may still be landing, and that partial failure is the case
    /// that motivates withdrawing the heartbeat.
    #[error("object store {operation} failed at {path}")]
    ObjectStore {
        /// `list`, `put` or `delete`.
        operation: &'static str,
        /// The path the request targeted.
        path: String,
        /// The underlying store error.
        #[source]
        source: slatedb::object_store::Error,
    },

    /// A cell id that cannot be encoded into an object name or a path.
    ///
    /// The cell-writer record's name *is* the cell id
    /// ([`crate::cell_writer`]), so the same rule that guards a node id guards
    /// this — see [`validate_node_id`], which shares its implementation.
    #[error("invalid cell id {cell_id:?}: {reason}")]
    InvalidCellId {
        /// The rejected id, quoted for the same reason as `InvalidNodeId`.
        cell_id: String,
        /// Which rule it broke.
        reason: &'static str,
    },

    /// A heartbeat body would not serialize. Unreachable in practice — the body
    /// is strings, an integer and timestamps — but the alternative is an
    /// `expect` in a library.
    #[error("could not encode the heartbeat body for {node_id}")]
    EncodeBody {
        /// The node whose body failed to encode.
        node_id: String,
        /// The underlying serde error.
        #[source]
        source: serde_json::Error,
    },

    /// A cell-writer record would not serialize. As unreachable as
    /// [`PlacementError::EncodeBody`], and here for the same reason.
    #[error("could not encode the cell-writer record for {cell_id}")]
    EncodeRecord {
        /// The cell whose record failed to encode.
        cell_id: String,
        /// The underlying serde error.
        #[source]
        source: serde_json::Error,
    },

    /// An object under a versioned prefix did not parse as the record that
    /// prefix promises.
    ///
    /// Reachable, unlike the encode variants: a truncated write, a hand-edited
    /// object, or a future format that should have bumped its prefix. It
    /// carries the path because the first thing anyone will do about it is look
    /// at the object.
    #[error("could not decode the cell-writer record at {path}")]
    DecodeRecord {
        /// Where the unparseable object is.
        path: String,
        /// The underlying serde error.
        #[source]
        source: serde_json::Error,
    },
}

/// The body of a heartbeat object. **Observability only** — never fetched on
/// the placement path, and no decision in this crate reads it.
///
/// Its job is to make the next incident readable in one line: who, running
/// what, started when, and what its own clock thought the time was. That last
/// one is the reason `published_at` is here despite `LastModified` being the
/// authority — the two disagreeing is the signature of clock skew, and it is
/// invisible from either value alone.
///
/// **No `#[serde(deny_unknown_fields)]`, deliberately.** A rolling restart
/// replaces pods one at a time, so an old reader will see bodies written by a
/// new writer. Unknown fields must be ignored, which makes adding a field a
/// compatible change and keeps [`VERSION`] pinned. Removing or retyping one is
/// not, and is what a version bump is for. Decision 8.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Heartbeat {
    /// Format version; see [`VERSION`].
    pub version: u32,

    /// The node that wrote this. Duplicates the object name so a body pulled
    /// out of a bucket on its own is still self-describing — and so a body that
    /// disagrees with its name is a detectable bug rather than a silent one.
    pub node_id: String,

    /// The build the node is running. The first question of any mixed-version
    /// incident.
    pub turbolay_version: String,

    /// When the node's process started, by the node's own clock. A `started_at`
    /// that keeps moving is a crash loop, which otherwise looks identical to a
    /// healthy node from the outside.
    pub started_at: DateTime<Utc>,

    /// When the node wrote this heartbeat, by the node's own clock. Compare
    /// against the object's `LastModified` to measure clock skew; placement
    /// uses `LastModified` and ignores this field entirely.
    pub published_at: DateTime<Utc>,

    /// The cells this node serves. Today every node serves every configured
    /// cell, so this is uniform across the fleet and purely diagnostic. If that
    /// ever stops being true, placement reads the subset from the object *name*
    /// and this field stays observability.
    pub cells: Vec<String>,
}

impl Heartbeat {
    /// A current-version body.
    ///
    /// Both timestamps are parameters rather than clock reads — decision 10
    /// applies to the body as much as to the liveness path, and a caller that
    /// has to pass `published_at` cannot forget that it is *its* clock and not
    /// the store's.
    pub fn new(
        node_id: impl Into<String>,
        turbolay_version: impl Into<String>,
        started_at: DateTime<Utc>,
        published_at: DateTime<Utc>,
        cells: Vec<String>,
    ) -> Self {
        Self {
            version: VERSION,
            node_id: node_id.into(),
            turbolay_version: turbolay_version.into(),
            started_at,
            published_at,
            cells,
        }
    }
}

/// One heartbeat object found by LIST: everything placement knows about a node,
/// all of it from the name and the timestamp.
///
/// The age is a [`Duration`] and not a timestamp on purpose. Collapsing it at
/// the boundary is what makes every liveness rule below a pure function over
/// hand-built entries — no store, no clock, no sleeping test.
#[derive(Clone, Debug)]
pub struct HeartbeatEntry {
    /// The node id, decoded from the object name.
    pub node_id: String,

    /// The reader's clock minus the object's `LastModified`, saturating at
    /// [`Duration::ZERO`] for an object stamped in the future.
    pub age: Duration,

    /// Where the object is, so a caller can DELETE it without re-deriving the
    /// path.
    pub location: Path,
}

/// Check a node id for use as a heartbeat object name.
///
/// Nonempty and `[A-Za-z0-9_-.]`, matching the kernel's
/// `validate_component` (`src/codec.rs:175`) exactly. That is not a
/// coincidence to be tidied away later: node ids cross between the two crates
/// through `ObjectStoreNodeDirectory`, and an id the kernel accepts but
/// placement rejects would be a node that starts, joins the directory, and then
/// silently fails to publish.
///
/// The one rule added on top is `.` and `..`, which the kernel's character
/// class permits and an object path does not. `Path::parse` rejects them and
/// `Path::join` percent-encodes them to `%2E`, so either way the name would
/// stop round-tripping — a node that never appears in its own live set.
pub fn validate_node_id(node_id: &str) -> Result<(), PlacementError> {
    match name_rule_violation(node_id) {
        Some(reason) => Err(PlacementError::InvalidNodeId {
            node_id: node_id.to_string(),
            reason,
        }),
        None => Ok(()),
    }
}

/// The single rule behind every object name this crate writes, or `None` if
/// `value` breaks none of it.
///
/// Shared by [`validate_node_id`] and
/// [`crate::cell_writer::validate_cell_id`] rather than copied, because the two
/// names sit under sibling prefixes in the same bucket and a rule that drifted
/// between them would mean an id one prefix accepts and the other percent-
/// encodes. It returns the reason instead of an error so each caller can name
/// the thing it rejected.
pub(crate) fn name_rule_violation(value: &str) -> Option<&'static str> {
    if value.is_empty() {
        Some("must not be empty")
    } else if value == "." || value == ".." {
        Some("must not be a relative path segment")
    } else if !value
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-' | b'.'))
    {
        Some("must be ASCII alphanumeric, '_', '-' or '.'")
    } else {
        None
    }
}

/// The heartbeat object name for a node.
///
/// The encoding is the identity today. It is a function anyway because it is
/// half of the frozen name format — see the module docs on where a per-node
/// cell subset would land — and because validation belongs on the path that
/// builds the name, not scattered over its callers.
pub fn object_name(node_id: &str) -> Result<String, PlacementError> {
    validate_node_id(node_id)?;
    Ok(node_id.to_string())
}

/// The node id a heartbeat object name encodes, or `None` if it is not a
/// heartbeat.
///
/// The inverse of [`object_name`], and it must reject exactly what
/// [`validate_node_id`] rejects: anything looser would attribute a stray object
/// to a node that does not exist and hand it ownership of a cell.
pub fn parse_object_name(name: &str) -> Option<&str> {
    validate_node_id(name).ok().map(|()| name)
}

/// The prefix every heartbeat object lives under.
///
/// Built by folding [`HEARTBEAT_PREFIX`]'s segments through `Path::join` rather
/// than formatting a string. `join` percent-encodes whatever it is handed, so
/// joining `"_graph_nodes/v1"` whole would produce the single segment
/// `_graph_nodes%2Fv1`; and re-parsing `format!("{base}/{HEARTBEAT_PREFIX}")`
/// would encode any `%` already in `base` a second time.
pub fn heartbeat_prefix(base: &Path) -> Path {
    versioned_prefix(base, HEARTBEAT_PREFIX)
}

/// `base` extended by a `a/b`-style prefix constant, one segment at a time.
///
/// Shared with [`crate::cell_writer`], which lays its objects out the same way
/// and would otherwise copy the two mistakes this avoids: `join`ing the whole
/// constant produces the single segment `_graph_nodes%2Fv1`, and re-parsing
/// `format!("{base}/{prefix}")` encodes any `%` already in `base` a second time.
pub(crate) fn versioned_prefix(base: &Path, prefix: &str) -> Path {
    prefix
        .split('/')
        .fold(base.clone(), |path, segment| path.join(segment))
}

/// The full path of one node's heartbeat object.
pub fn heartbeat_path(base: &Path, node_id: &str) -> Result<Path, PlacementError> {
    let name = object_name(node_id)?;
    Ok(heartbeat_prefix(base).join(name))
}

/// Publish a node's heartbeat.
///
/// The path comes from `body.node_id`, so the name and the body cannot
/// disagree — there is no second place to pass an id and get it wrong.
pub async fn put_heartbeat(
    store: &dyn ObjectStore,
    base: &Path,
    body: &Heartbeat,
) -> Result<(), PlacementError> {
    let path = heartbeat_path(base, &body.node_id)?;
    let payload = serde_json::to_vec(body).map_err(|source| PlacementError::EncodeBody {
        node_id: body.node_id.clone(),
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

/// Withdraw a node's heartbeat: on unreadiness, on SIGTERM before draining, and
/// when a failing LIST puts the node past its grace period (decision 7).
///
/// **Idempotent by design.** The publisher DELETEs once per interval for as long
/// as it is unready, and the shutdown path DELETEs again on top of that, so an
/// already-absent object is the normal case and not a failure. Reporting it
/// would make the caller's retry loop log an error every five seconds while
/// behaving perfectly.
pub async fn delete_heartbeat(
    store: &dyn ObjectStore,
    base: &Path,
    node_id: &str,
) -> Result<(), PlacementError> {
    let path = heartbeat_path(base, node_id)?;
    match store.delete(&path).await {
        Ok(()) | Err(slatedb::object_store::Error::NotFound { .. }) => Ok(()),
        Err(source) => Err(PlacementError::ObjectStore {
            operation: "delete",
            path: path.to_string(),
            source,
        }),
    }
}

/// Every heartbeat object under `base`, with its name parsed and its age
/// resolved against `now`.
///
/// **The single impure boundary of this crate, and the only function in it that
/// is told what time it is.** `now` is a parameter and not a clock read so that
/// every rule downstream is a pure function of `Duration`s — see the module
/// docs and decision 10.
///
/// Three things that are deliberately *not* errors:
///
/// - **A missing prefix lists as an empty fleet.** `Ok(vec![])` means "nobody is
///   publishing", which is the correct answer on a cold start and must not
///   share a path with a LIST that failed.
/// - **An unparseable name is skipped.** A stray object under the prefix costs
///   one skipped entry, not the whole live set.
/// - **A future `LastModified` saturates to [`Duration::ZERO`].** Reverse clock
///   skew makes a heartbeat maximally fresh, never dead.
pub async fn list_heartbeats(
    store: &dyn ObjectStore,
    base: &Path,
    now: DateTime<Utc>,
) -> Result<Vec<HeartbeatEntry>, PlacementError> {
    let prefix = heartbeat_prefix(base);
    let metas = match store.list(Some(&prefix)).try_collect::<Vec<_>>().await {
        Ok(metas) => metas,
        // A prefix that has never been written to. Not a failure: it is the
        // answer "I am alone", and decision 7 must be able to tell the two
        // apart.
        Err(slatedb::object_store::Error::NotFound { .. }) => Vec::new(),
        Err(source) => {
            return Err(PlacementError::ObjectStore {
                operation: "list",
                path: prefix.to_string(),
                source,
            })
        }
    };

    let mut entries = Vec::with_capacity(metas.len());
    for meta in metas {
        let Some(node_id) = node_id_at(&prefix, &meta.location) else {
            continue;
        };
        // The whole of decision 10 is this line: the timestamp dies here and a
        // `Duration` leaves. `to_std` fails on a negative span, which is
        // reverse clock skew, and `unwrap_or_default` is `Duration::ZERO` —
        // maximally fresh.
        let age = (now - meta.last_modified).to_std().unwrap_or_default();
        entries.push(HeartbeatEntry {
            node_id,
            age,
            location: meta.location,
        });
    }
    Ok(entries)
}

/// The node id of an object sitting *directly* under `prefix`, or `None` if it
/// is not a heartbeat.
///
/// The depth check is the part worth keeping. LIST is recursive, so anything
/// nested below the prefix comes back too, and taking the last path segment —
/// which is what sleet does, over a flat prefix — would read
/// `…/v1/graph-node-0/scratch` as a heartbeat for a node called `scratch`.
fn node_id_at(prefix: &Path, location: &Path) -> Option<String> {
    let mut parts = location.prefix_match(prefix)?;
    let name = parts.next()?;
    if parts.next().is_some() {
        return None;
    }
    parse_object_name(name.as_ref()).map(str::to_string)
}

#[cfg(test)]
mod tests {
    use super::*;

    use chrono::TimeZone;
    use slatedb::object_store::memory::InMemory;

    const NODE: &str = "graph-node-0";

    /// Ids the fleet actually produces, plus the edges of the character class
    /// the kernel's `validate_component` accepts.
    const VALID_IDS: &[&str] = &[
        "graph-node-0",
        "graph-node-1",
        "graph-node-42",
        "node-a",
        "node_a",
        "A9",
        "n",
        "turbolay.graph-node.0",
        "0",
        "-",
    ];

    fn base() -> Path {
        Path::from("acme/graphs/social")
    }

    fn body(node_id: &str) -> Heartbeat {
        let at = Utc
            .with_ymd_and_hms(2026, 7, 25, 14, 32, 7)
            .single()
            .expect("a real instant");
        Heartbeat::new(
            node_id,
            "0.1.0",
            at,
            at,
            vec!["cell-a".to_string(), "cell-b".to_string()],
        )
    }

    async fn store_with(node_ids: &[&str]) -> InMemory {
        let store = InMemory::new();
        for node_id in node_ids {
            put_heartbeat(&store, &base(), &body(node_id))
                .await
                .expect("the store is in memory and the ids are valid");
        }
        store
    }

    /// The object's own `LastModified`, so tests can name a `now` relative to it
    /// and assert exact ages. This is why no test in this file reads a clock.
    async fn last_modified(store: &InMemory, node_id: &str) -> DateTime<Utc> {
        let path = heartbeat_path(&base(), node_id).expect("valid id");
        store
            .head(&path)
            .await
            .expect("just published")
            .last_modified
    }

    #[test]
    fn object_names_round_trip_for_every_realistic_node_id() {
        for id in VALID_IDS {
            let name = object_name(id).unwrap_or_else(|e| panic!("{id:?} should be valid: {e}"));
            assert_eq!(
                parse_object_name(&name),
                Some(*id),
                "{id:?} did not survive the round trip"
            );
        }
    }

    /// Everything that would break the name encoding or the path. `/` and the
    /// empty string are the two that matter; the rest are the character class.
    #[test]
    fn node_ids_that_would_break_the_name_or_the_path_are_rejected() {
        for bad in [
            "", "/", "a/b", ".", "..", "a b", "a:b", "a%b", "a\nb", "ü", "a\\b", "*",
        ] {
            assert!(
                validate_node_id(bad).is_err(),
                "{bad:?} should have been rejected"
            );
            assert!(object_name(bad).is_err(), "{bad:?}");
            // The parser must reject exactly what validation rejects, or a
            // stray object gets attributed to a node that does not exist.
            assert_eq!(parse_object_name(bad), None, "{bad:?}");
        }
    }

    #[test]
    fn heartbeat_paths_nest_under_the_versioned_prefix() {
        let path = heartbeat_path(&base(), NODE).expect("valid id");
        assert_eq!(
            path.as_ref(),
            "acme/graphs/social/_graph_nodes/v1/graph-node-0"
        );
        // The prefix's `/` must stay a path separator, not become `%2F`.
        assert_eq!(
            heartbeat_prefix(&base()).as_ref(),
            "acme/graphs/social/_graph_nodes/v1"
        );
    }

    #[test]
    fn an_invalid_node_id_fails_before_a_path_is_built() {
        let err = heartbeat_path(&base(), "a/b").expect_err("slashes are rejected");
        assert!(matches!(err, PlacementError::InvalidNodeId { .. }), "{err}");
    }

    #[test]
    fn a_body_round_trips_through_json() {
        let json = serde_json::to_string(&body(NODE)).expect("encodable");
        let back: Heartbeat = serde_json::from_str(&json).expect("decodable");
        assert_eq!(back.version, VERSION);
        assert_eq!(back.node_id, NODE);
        assert_eq!(back.turbolay_version, "0.1.0");
        assert_eq!(back.cells, vec!["cell-a".to_string(), "cell-b".to_string()]);
    }

    /// A rolling restart replaces pods one at a time, so an old reader will see
    /// a new writer's body. If this test fails, `deny_unknown_fields` has been
    /// added and the next upgrade breaks `sleet status`' equivalent mid-roll.
    #[test]
    fn unknown_body_fields_deserialize_so_a_mixed_version_fleet_coexists() {
        let json = r#"{
            "version": 1,
            "node_id": "graph-node-0",
            "turbolay_version": "9.9.9",
            "started_at": "2026-07-25T14:00:00Z",
            "published_at": "2026-07-25T14:32:07Z",
            "cells": ["cell-a"],
            "writer_epoch": 18,
            "some_future_object": {"x": 1}
        }"#;
        let hb: Heartbeat = serde_json::from_str(json).expect("unknown fields are ignored");
        assert_eq!(hb.node_id, NODE);
        assert_eq!(hb.cells, vec!["cell-a".to_string()]);
    }

    /// A prefix nobody has ever written to. `Ok(vec![])` and not `Err`:
    /// decision 7 sheds ownership on a failed LIST, and a cold start must not
    /// go down that path.
    #[tokio::test]
    async fn a_missing_prefix_lists_as_an_empty_fleet_not_an_error() {
        let store = InMemory::new();
        let now = Utc.timestamp_opt(0, 0).single().expect("the epoch");
        let entries = list_heartbeats(&store, &base(), now)
            .await
            .expect("an empty fleet is a valid answer");
        assert!(entries.is_empty());
    }

    #[tokio::test]
    async fn a_published_heartbeat_comes_back_from_list() {
        let store = store_with(&["graph-node-0", "graph-node-1"]).await;
        let now = last_modified(&store, NODE).await;
        let mut entries = list_heartbeats(&store, &base(), now).await.expect("listed");
        entries.sort_by(|a, b| a.node_id.cmp(&b.node_id));

        let ids: Vec<&str> = entries.iter().map(|e| e.node_id.as_str()).collect();
        assert_eq!(ids, vec!["graph-node-0", "graph-node-1"]);
        assert_eq!(
            entries[0].location.as_ref(),
            "acme/graphs/social/_graph_nodes/v1/graph-node-0"
        );
    }

    /// The age comes from the `now` parameter and from nothing else. Asserting
    /// an *exact* duration is the proof: any clock read inside
    /// `list_heartbeats` would make this off by the test's own runtime.
    ///
    /// Note what this does not do: sleep, or backdate the store. `InMemory`
    /// stamps `LastModified` from the real wall clock and cannot be moved, so
    /// "this heartbeat is an hour old" is expressed by moving `now` forward
    /// instead. That is the whole point of the parameter.
    #[tokio::test]
    async fn age_is_measured_from_the_now_parameter_and_no_clock() {
        let store = store_with(&[NODE]).await;
        let stamped = last_modified(&store, NODE).await;
        let now = stamped + chrono::Duration::seconds(3600);

        let entries = list_heartbeats(&store, &base(), now).await.expect("listed");
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].age, Duration::from_secs(3600));

        // Same `now`, same answer, however long the test has been running.
        let again = list_heartbeats(&store, &base(), now).await.expect("listed");
        assert_eq!(again[0].age, entries[0].age);
    }

    /// Reverse clock skew: the object is stamped after the reader's `now`. The
    /// node is maximally fresh, not an error and not dead — the alternative is
    /// that one skewed clock evicts a healthy node from the fleet.
    #[tokio::test]
    async fn a_heartbeat_from_the_future_saturates_to_zero_age() {
        let store = store_with(&[NODE]).await;
        let stamped = last_modified(&store, NODE).await;
        let now = stamped - chrono::Duration::seconds(3600);

        let entries = list_heartbeats(&store, &base(), now).await.expect("listed");
        assert_eq!(entries[0].age, Duration::ZERO);
    }

    /// One stray object must not poison the live set for the whole fleet.
    #[tokio::test]
    async fn objects_that_are_not_heartbeats_are_skipped_not_fatal() {
        let store = store_with(&[NODE]).await;
        let prefix = heartbeat_prefix(&base());
        for junk in [
            // A name outside the character class, percent-encoded by the path.
            prefix.clone().join("not a node id"),
            // Something nested a level deeper. LIST is recursive, so this comes
            // back too; taking the last segment would read it as a heartbeat
            // for a node called `scratch`.
            prefix.clone().join(NODE).join("scratch"),
            // A relative segment, which `join` encodes to `%2E%2E`.
            prefix.clone().join(".."),
        ] {
            store
                .put(&junk, PutPayload::from_static(b"{}"))
                .await
                .expect("in memory");
        }

        let now = last_modified(&store, NODE).await;
        let entries = list_heartbeats(&store, &base(), now).await.expect("listed");
        let ids: Vec<&str> = entries.iter().map(|e| e.node_id.as_str()).collect();
        assert_eq!(ids, vec![NODE], "junk objects leaked into the live set");
    }

    #[tokio::test]
    async fn deleting_a_heartbeat_withdraws_the_node() {
        let store = store_with(&["graph-node-0", "graph-node-1"]).await;
        let now = last_modified(&store, NODE).await;

        delete_heartbeat(&store, &base(), NODE)
            .await
            .expect("deleted");

        let entries = list_heartbeats(&store, &base(), now).await.expect("listed");
        let ids: Vec<&str> = entries.iter().map(|e| e.node_id.as_str()).collect();
        assert_eq!(ids, vec!["graph-node-1"]);
    }

    /// The publisher DELETEs once per interval for as long as it is unready,
    /// and again on SIGTERM. An already-absent object is the normal case.
    #[tokio::test]
    async fn deleting_a_heartbeat_that_is_already_gone_is_not_an_error() {
        let store = store_with(&[NODE]).await;
        for _ in 0..3 {
            delete_heartbeat(&store, &base(), NODE)
                .await
                .expect("withdrawal is idempotent");
        }
    }

    /// The object name is derived from the body, so there is no second place to
    /// pass an id and no way for the two to disagree.
    #[tokio::test]
    async fn the_object_name_comes_from_the_body_so_they_cannot_disagree() {
        let store = InMemory::new();
        put_heartbeat(&store, &base(), &body("graph-node-2"))
            .await
            .expect("published");

        let now = last_modified(&store, "graph-node-2").await;
        let entries = list_heartbeats(&store, &base(), now).await.expect("listed");
        assert_eq!(entries[0].node_id, "graph-node-2");
        assert_eq!(
            entries[0].location,
            heartbeat_path(&base(), "graph-node-2").expect("valid id")
        );
    }

    #[tokio::test]
    async fn publishing_with_an_invalid_node_id_never_reaches_the_store() {
        let store = InMemory::new();
        let err = put_heartbeat(&store, &base(), &body("a/b"))
            .await
            .expect_err("slashes are rejected");
        assert!(matches!(err, PlacementError::InvalidNodeId { .. }), "{err}");

        let now = Utc.timestamp_opt(0, 0).single().expect("the epoch");
        assert!(list_heartbeats(&store, &base(), now)
            .await
            .expect("listed")
            .is_empty());
    }
}
