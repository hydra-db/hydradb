//! Liveness: which configured nodes are up, and what to do when we cannot tell.
//!
//! The live set is `configured membership ∩ fresh heartbeats`. Membership is
//! static config; freshness comes from the object store's `LastModified`,
//! already collapsed to an age at the LIST boundary. Neither operand alone is
//! enough: a decommissioned node's leftover object must not win placement, and
//! a configured node that has stopped publishing must not keep it.
//!
//! Everything here is pure and synchronous. No async, no object store, no
//! clock, no `Instant` field this module reads for itself — decision 10 of
//! the rendezvous placement design. Time reaches this module
//! only as `Duration` parameters that the caller measured:
//! [`HeartbeatEntry::age`], the elapsed time since process start, and the
//! elapsed time since the last successful LIST. That is what makes every rule
//! below a plain table test that runs in microseconds instead of a test that
//! sleeps through a 15s timeout.
//!
//! The invariants, each with a test:
//!
//! - **Freshness is `age < timeout`, strict.** A heartbeat exactly at the
//!   timeout is dead. Half-open intervals compose; `<=` at both ends of a
//!   two-sided rule does not.
//! - **Dedupe before filtering.** A node may briefly have two heartbeat
//!   objects; the youngest is the one that describes it. Filtering first and
//!   deduplicating after would let a stale leftover speak for a live node.
//! - **Output order is node id ascending, always.** Rendezvous only stops the
//!   writer duel if every node computes the same owner, and `hash::rank`'s
//!   tie-break can only do that if the candidate list is itself deterministic.
//! - **[`live_nodes`] is self-agnostic.** Self-always-live is a rule of the
//!   *decision* layer ([`LiveNodeSet`]), not of the filter. Keeping the filter
//!   honest leaves a truthful view available for status reporting, which is the
//!   only way an operator ever sees "this node's own heartbeat is 40s old".
//! - **A node always counts itself live** (§3). A node reading its own
//!   heartbeat as stale — skewed clock, slow PUT, throttled write — has no
//!   reliable proof it should stand down. Peers that consider it dead take over
//!   in parallel, which is a safe double-run bounded by SlateDB's writer epoch;
//!   excluding itself instead leaves the cell unowned by anybody.
//! - **Never-published is not expired** (decision 8). For the first
//!   `heartbeat_timeout` after process start, a configured peer with no
//!   heartbeat object at all counts as live. Only expiry is evidence of death.
//!   This is the rolling-restart case: a freshly started process must not
//!   conclude the rest of the fleet is dead merely because it has not seen it
//!   yet.
//! - **A failed LIST gets bounded grace, then sheds** (decision 7). See
//!   [`ViewState`] — and note that shedding is inseparable from withdrawing the
//!   heartbeat.
//!
//! An **empty** LIST is a successful answer, not a failure. It is evidence that
//! every peer withdrew (decision 4 deletes the heartbeat on unready and on
//! SIGTERM), and it must never touch the grace state.
//!
//! Adapted from a proven liveness implementation (`youngest_per_node`,
//! `node_view`, and `owned_assignments`, where self-always-live lives), with
//! its per-node service list dropped — every HydraDB node serves
//! every cell today, and §3's extension point puts that in the object *name*
//! when it stops being true.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use crate::heartbeat::HeartbeatEntry;

/// One live node, deduplicated to the youngest heartbeat per node id.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NodeView {
    /// The node id.
    pub node_id: String,
    /// The age of the node's youngest heartbeat.
    ///
    /// `Duration::ZERO` when the entry is *synthesized* rather than observed —
    /// the local node with no heartbeat object yet, or a configured peer held
    /// live by decision 8's startup window. There is no object to age in those
    /// cases, and zero is the one value that cannot be misread as expired.
    /// Whenever an object does exist its real age is reported, including for
    /// the local node, whose own staleness is worth seeing in a status page
    /// even though it never changes the decision.
    pub age: Duration,
}

/// The youngest heartbeat per node id.
///
/// A node can briefly own two objects — a rename, a redeployment writing under
/// a new name before the old one is swept — and the youngest is the one that
/// describes it. This runs *before* the freshness filter so that a stale
/// leftover can never outvote a live heartbeat from the same node.
pub fn youngest_per_node(entries: &[HeartbeatEntry]) -> BTreeMap<&str, &HeartbeatEntry> {
    let mut by_id: BTreeMap<&str, &HeartbeatEntry> = BTreeMap::new();
    for entry in entries {
        match by_id.get(entry.node_id.as_str()) {
            Some(existing) if existing.age <= entry.age => {}
            _ => {
                by_id.insert(&entry.node_id, entry);
            }
        }
    }
    by_id
}

/// The live set: heartbeats younger than `timeout`, the youngest name winning
/// when a node briefly has two.
///
/// The comparison is strict — a heartbeat exactly at `timeout` is dead — and
/// the result is ordered by node id, because `hash::rank` can only make every
/// node agree on an owner if every node feeds it the same candidate list.
///
/// This is a pure filter and knows nothing about who is asking: it does not
/// force the reader into its own result. That rule belongs to [`LiveNodeSet`],
/// which layers it on top, leaving this function usable for honest status
/// reporting.
pub fn live_nodes(entries: &[HeartbeatEntry], timeout: Duration) -> Vec<NodeView> {
    youngest_per_node(entries)
        .into_values()
        .filter(|entry| entry.age < timeout)
        .map(|entry| NodeView {
            node_id: entry.node_id.clone(),
            age: entry.age,
        })
        .collect()
}

/// What this node's publisher must do with its own heartbeat object.
///
/// This exists as a type rather than a `bool` because getting it wrong is the
/// nastiest failure in the design — see [`ViewState::Shed`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HeartbeatAction {
    /// Keep publishing, subject to the usual readiness gate (decision 4).
    Publish,
    /// Stop publishing and DELETE the heartbeat object, now.
    Withdraw,
}

/// How much this node currently knows about the fleet, and therefore what it is
/// allowed to do (decision 7).
///
/// The states are distinguished because "serve the cached set" and "withdraw"
/// are different actions and a bare `is_stale` bool cannot express both.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ViewState {
    /// The last LIST succeeded. The view is current.
    Fresh,
    /// The last LIST failed, less than one `heartbeat_timeout` ago. Serve the
    /// cached live set and keep retrying with backoff. Grace is exactly
    /// `heartbeat_timeout` because peers take over that long after this node's
    /// last successful PUT, and in a partition LIST and PUT fail together: a
    /// shorter grace leaves a window with no owner, a longer one leaves a duel.
    Grace,
    /// Grace expired. The node knows nothing about the fleet and must
    /// **withdraw**: go unready, stop publishing, DELETE its heartbeat, and
    /// refuse promotion with `NotCellWriter` carrying no owner hint.
    ///
    /// The withdrawal is not optional bookkeeping, it is the whole point. In a
    /// partial failure — LIST throttled while PUTs still land — a node that
    /// sheds but keeps publishing stays the computed owner for every peer, so
    /// they route every write to it and it refuses every one, permanently,
    /// with nothing left to time out. Withdrawing is what lets peers take over.
    /// [`LiveView::candidates`] returns `None` in this state precisely so a
    /// caller cannot fall through to the "no live owner, promote myself" arm.
    Shed,
}

impl ViewState {
    /// Stable telemetry vocabulary for this state.
    ///
    /// Lower-case strings are deliberate: they are emitted by the kernel when
    /// a placement view changes state, so changing one is a telemetry schema
    /// change rather than a cosmetic rename.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Fresh => "fresh",
            Self::Grace => "grace",
            Self::Shed => "shed",
        }
    }

    /// What the publisher must do with this node's heartbeat.
    #[must_use]
    pub fn heartbeat_action(self) -> HeartbeatAction {
        match self {
            Self::Fresh | Self::Grace => HeartbeatAction::Publish,
            Self::Shed => HeartbeatAction::Withdraw,
        }
    }

    /// Whether the node may serve, i.e. anything but [`ViewState::Shed`]. A
    /// node that cannot read the live set is not ready, which is the same
    /// signal decision 4 already gates publication on.
    #[must_use]
    pub fn is_ready(self) -> bool {
        !matches!(self, Self::Shed)
    }
}

/// One answer from [`LiveNodeSet`]: the live set plus the state it was derived
/// in.
///
/// Ignoring one of these is always a bug — the failure paths are exactly the
/// ones that carry an obligation — so the type is `#[must_use]`.
#[derive(Clone, Debug, PartialEq, Eq)]
#[must_use = "a Shed view obliges the caller to withdraw its heartbeat"]
pub enum LiveView {
    /// Derived from a successful LIST.
    Fresh(Vec<NodeView>),
    /// The cached set from the last successful LIST, still inside grace.
    Grace(Vec<NodeView>),
    /// Grace expired. Deliberately carries no node list: there is nothing
    /// truthful to route with, and nothing here that could be mistaken for one.
    Shed,
}

impl LiveView {
    /// Which of the three decision-7 states this view is in.
    #[must_use]
    pub fn state(&self) -> ViewState {
        match self {
            Self::Fresh(_) => ViewState::Fresh,
            Self::Grace(_) => ViewState::Grace,
            Self::Shed => ViewState::Shed,
        }
    }

    /// The live nodes, for status reporting. Empty when shed.
    #[must_use]
    pub fn nodes(&self) -> &[NodeView] {
        match self {
            Self::Fresh(nodes) | Self::Grace(nodes) => nodes,
            Self::Shed => &[],
        }
    }

    /// Candidate node ids to hand to `hash::owner`, or `None` when the node has
    /// shed.
    ///
    /// `None` and `Some(&[])` mean opposite things and the distinction is the
    /// reason this returns an `Option`. `Some(&[])` is "the live set is empty",
    /// which by `hash::owner`'s contract means placement has no opinion and the
    /// caller may promote itself. `None` is "this node does not know the live
    /// set", where promoting itself is exactly the unbounded duel decision 7
    /// exists to stop. A caller that reached for a bare `Vec` would get the
    /// first behaviour while meaning the second.
    #[must_use]
    pub fn candidates(&self) -> Option<Vec<&str>> {
        match self {
            Self::Fresh(nodes) | Self::Grace(nodes) => Some(nodes.iter().map(|n| n.node_id.as_str()).collect()),
            Self::Shed => None,
        }
    }

    /// What the publisher must do with this node's heartbeat, given this view.
    /// Call it on every view; [`ViewState::Shed`] documents why skipping it is
    /// the worst bug available here.
    #[must_use]
    pub fn heartbeat_action(&self) -> HeartbeatAction {
        self.state().heartbeat_action()
    }
}

/// Configured membership intersected with heartbeat freshness, carrying
/// decision 7's grace state and decision 8's startup window.
///
/// This is the decision layer over [`live_nodes`]: it knows who the fleet is
/// supposed to be, who this node is, and what the last successful LIST said.
/// It still reads no clock — both elapsed times arrive as parameters, from the
/// caller's own monotonic `Instant`s.
///
/// Nothing here mutates on failure. [`LiveNodeSet::observe_list`] takes
/// `&mut self` and replaces the cache; [`LiveNodeSet::observe_list_failure`]
/// takes `&self`, because a failure is not new information about the fleet.
#[derive(Clone, Debug)]
pub struct LiveNodeSet {
    local_node_id: String,
    members: BTreeSet<String>,
    heartbeat_timeout: Duration,
    cached: Vec<NodeView>,
}

impl LiveNodeSet {
    /// A live set for `local_node_id` over the configured fleet `members`.
    ///
    /// Before the first LIST the cache holds the whole configured fleet, every
    /// entry synthesized. That is decision 8's posture applied to the moment
    /// with the least information of all: a process that has not yet read the
    /// store must degrade to "everyone I was told about is up", not to "I am
    /// alone and therefore own everything". It is bounded regardless — if LIST
    /// keeps failing, grace expires one `heartbeat_timeout` in and the node
    /// sheds.
    pub fn new<I, S>(local_node_id: impl Into<String>, members: I, heartbeat_timeout: Duration) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let local_node_id = local_node_id.into();
        let mut members: BTreeSet<String> = members.into_iter().map(Into::into).collect();
        members.insert(local_node_id.clone());
        let cached = members
            .iter()
            .map(|id| NodeView {
                node_id: id.clone(),
                age: Duration::ZERO,
            })
            .collect();
        Self {
            local_node_id,
            members,
            heartbeat_timeout,
            cached,
        }
    }

    /// The grace window a failed LIST gets: exactly `heartbeat_timeout`, not a
    /// separate knob. Decision 7 makes them equal so peer takeover and local
    /// shedding coincide.
    #[must_use]
    pub fn grace(&self) -> Duration {
        self.heartbeat_timeout
    }

    /// The last live set derived from a successful LIST, whatever the current
    /// view state. For status reporting; routing decisions must go through a
    /// [`LiveView`], which knows whether this is still trustworthy.
    #[must_use]
    pub fn last_known(&self) -> &[NodeView] {
        &self.cached
    }

    /// Record a successful LIST and derive the current view.
    ///
    /// An empty `entries` is a successful answer — every peer withdrew — and
    /// takes this path like any other, so it refreshes the cache and never
    /// touches the grace state.
    ///
    /// `since_start` is how long this process has been running, for decision
    /// 8's startup window; it is a parameter and not an `Instant::now()` read
    /// so that the whole rule stays testable without sleeping.
    pub fn observe_list(&mut self, entries: &[HeartbeatEntry], since_start: Duration) -> LiveView {
        let youngest = youngest_per_node(entries);
        let mut live: BTreeMap<String, NodeView> = BTreeMap::new();

        // Membership ∩ freshness. Heartbeats from ids outside the configured
        // fleet are ignored: a decommissioned node's leftover object must not
        // win a cell that nobody would ever route to it.
        for view in live_nodes(entries, self.heartbeat_timeout) {
            if self.members.contains(&view.node_id) {
                live.insert(view.node_id.clone(), view);
            }
        }

        // Decision 8: never-published is not expired. Inside the startup window
        // a configured peer with no object *at all* counts as live. The test is
        // "has no entry", not "has no fresh entry" — an expired heartbeat is
        // positive evidence of death and the window does not resurrect it.
        if since_start < self.heartbeat_timeout {
            for member in &self.members {
                if !youngest.contains_key(member.as_str()) {
                    live.entry(member.clone()).or_insert_with(|| NodeView {
                        node_id: member.clone(),
                        age: Duration::ZERO,
                    });
                }
            }
        }

        // Self is always live (§3), and unconditionally — membership is config
        // and a node that is not in its own config is still the one running.
        // The real age is kept when an object exists so a skewed clock or a
        // slow PUT is visible in status; it just never changes the decision.
        let self_age = youngest.get(self.local_node_id.as_str()).map_or(Duration::ZERO, |entry| entry.age);
        live.insert(
            self.local_node_id.clone(),
            NodeView {
                node_id: self.local_node_id.clone(),
                age: self_age,
            },
        );

        // `BTreeMap` keyed by node id, so the order is deterministic across
        // nodes without a sort here.
        self.cached = live.into_values().collect();
        LiveView::Fresh(self.cached.clone())
    }

    /// Derive the current view after a *failed* LIST.
    ///
    /// `since_last_success` is the elapsed time since the last successful LIST,
    /// measured by the caller from a monotonic `Instant`. Inside grace the
    /// cached set is served; at or past grace the node sheds — the comparison
    /// is strict on the same side as `age < timeout`, so a heartbeat and a view
    /// built from it expire on the same schedule.
    ///
    /// Takes `&self`: a failure teaches this node nothing about the fleet, so
    /// it must not overwrite what the last success established.
    pub fn observe_list_failure(&self, since_last_success: Duration) -> LiveView {
        if since_last_success < self.heartbeat_timeout {
            LiveView::Grace(self.cached.clone())
        } else {
            LiveView::Shed
        }
    }
}
