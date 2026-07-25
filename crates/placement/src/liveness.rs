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
//! `docs/plans/2026-07-25-rendezvous-placement.md`. Time reaches this module
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
//! Ported from `sleet/src/root.rs` (`youngest_per_node`, `node_view`) and
//! `sleet/src/daemon.rs` (`owned_assignments`, where self-always-live lives),
//! with sleet's per-node service list dropped — every Turbolay node serves
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
            Self::Fresh(nodes) | Self::Grace(nodes) => {
                Some(nodes.iter().map(|n| n.node_id.as_str()).collect())
            }
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
    pub fn new<I, S>(
        local_node_id: impl Into<String>,
        members: I,
        heartbeat_timeout: Duration,
    ) -> Self
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
        let self_age = youngest
            .get(self.local_node_id.as_str())
            .map_or(Duration::ZERO, |entry| entry.age);
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

#[cfg(test)]
mod tests {
    use super::*;
    use slatedb::object_store::path::Path;

    const TIMEOUT: Duration = Duration::from_secs(15);
    const LOCAL: &str = "graph-node-0";
    const FLEET: &[&str] = &["graph-node-0", "graph-node-1", "graph-node-2"];

    /// A heartbeat of `age_secs`, hand-built. No store, no clock, no sleep —
    /// the whole point of collapsing time to an age at the LIST boundary.
    fn entry(node_id: &str, age_secs: u64) -> HeartbeatEntry {
        HeartbeatEntry {
            node_id: node_id.to_string(),
            age: Duration::from_secs(age_secs),
            location: Path::from(format!("_graph_nodes/v1/{node_id}")),
        }
    }

    fn ids(nodes: &[NodeView]) -> Vec<&str> {
        nodes.iter().map(|n| n.node_id.as_str()).collect()
    }

    fn fleet() -> LiveNodeSet {
        LiveNodeSet::new(LOCAL, FLEET.iter().copied(), TIMEOUT)
    }

    /// A set whose cache has been primed by one successful LIST, past the
    /// startup window so decision 8 does not colour the result. The view is
    /// discarded on purpose — `#[must_use]` objecting to that is exactly the
    /// property being relied on everywhere else.
    fn seeded(entries: &[HeartbeatEntry]) -> LiveNodeSet {
        let mut set = fleet();
        let _ = set.observe_list(entries, TIMEOUT);
        set
    }

    /// The comparison is `age < timeout`, strict, and this pins both sides of
    /// the boundary. A `<=` here would make a node live for one extra tick
    /// after every peer has already stopped waiting for it.
    #[test]
    fn a_heartbeat_exactly_at_the_timeout_is_dead_and_one_tick_younger_is_live() {
        let dead = live_nodes(&[entry("graph-node-1", 15)], TIMEOUT);
        assert!(dead.is_empty(), "age == timeout must be dead");

        let live = live_nodes(
            &[HeartbeatEntry {
                age: TIMEOUT - Duration::from_nanos(1),
                ..entry("graph-node-1", 0)
            }],
            TIMEOUT,
        );
        assert_eq!(ids(&live), ["graph-node-1"]);
    }

    #[test]
    fn a_node_whose_only_heartbeat_expired_is_not_live() {
        let live = live_nodes(
            &[entry("graph-node-1", 60), entry("graph-node-2", 2)],
            TIMEOUT,
        );
        assert_eq!(ids(&live), ["graph-node-2"]);
    }

    /// Deduplication runs before the freshness filter, so a node that briefly
    /// owns two objects is judged on the younger one and reported at its age.
    /// Filtering first would let the leftover speak for a node that is up.
    #[test]
    fn a_node_with_a_stale_and_a_fresh_heartbeat_is_live_at_the_younger_age() {
        let entries = [entry("graph-node-1", 90), entry("graph-node-1", 3)];
        let youngest = youngest_per_node(&entries);
        assert_eq!(youngest.len(), 1);
        assert_eq!(youngest["graph-node-1"].age, Duration::from_secs(3));

        let live = live_nodes(&entries, TIMEOUT);
        assert_eq!(live.len(), 1);
        assert_eq!(live[0].age, Duration::from_secs(3));
    }

    /// Rendezvous only ends the duel if every node computes the same owner, and
    /// it can only do that if every node feeds `hash::rank` the same list.
    /// LIST order is not guaranteed, so the ordering has to come from here.
    #[test]
    fn the_live_set_is_ordered_by_node_id_whatever_order_the_list_returned() {
        let forward = [
            entry("graph-node-0", 1),
            entry("graph-node-1", 2),
            entry("graph-node-2", 3),
        ];
        let mut reversed = forward.clone();
        reversed.reverse();
        assert_eq!(
            ids(&live_nodes(&forward, TIMEOUT)),
            ["graph-node-0", "graph-node-1", "graph-node-2"]
        );
        assert_eq!(
            live_nodes(&forward, TIMEOUT),
            live_nodes(&reversed, TIMEOUT)
        );
    }

    /// The layering: the pure filter reports the truth, including that this
    /// node's own heartbeat is ancient, and the decision layer overrides it.
    /// Both views exist on purpose — one is for placement, one is for the
    /// operator trying to work out why the clock is skewed.
    #[test]
    fn the_local_node_stays_live_at_an_absurd_age_but_the_pure_filter_still_says_dead() {
        let entries = [entry(LOCAL, 9_999), entry("graph-node-1", 2)];

        assert_eq!(ids(&live_nodes(&entries, TIMEOUT)), ["graph-node-1"]);

        let view = fleet().observe_list(&entries, Duration::from_secs(3_600));
        assert_eq!(view.state(), ViewState::Fresh);
        assert_eq!(view.candidates(), Some(vec![LOCAL, "graph-node-1"]));
        assert_eq!(view.nodes()[0].age, Duration::from_secs(9_999));
    }

    /// Decision 8, the rolling restart: a process that has just started has not
    /// seen the fleet yet, and "not seen" is not "dead".
    #[test]
    fn a_never_published_peer_is_live_inside_the_startup_window() {
        let view = fleet().observe_list(&[entry(LOCAL, 1)], TIMEOUT - Duration::from_secs(1));
        assert_eq!(
            view.candidates(),
            Some(vec!["graph-node-0", "graph-node-1", "graph-node-2"])
        );
    }

    #[test]
    fn a_never_published_peer_is_dead_once_the_startup_window_closes() {
        let view = fleet().observe_list(&[entry(LOCAL, 1)], TIMEOUT);
        assert_eq!(view.candidates(), Some(vec![LOCAL]));
    }

    /// Only *expiry* is evidence of death, and conversely the window covers
    /// only the never-seen case. A peer whose heartbeat exists and has expired
    /// is dead from the first second of the process's life.
    #[test]
    fn an_expired_peer_is_dead_even_inside_the_startup_window() {
        let entries = [entry(LOCAL, 1), entry("graph-node-1", 60)];
        let view = fleet().observe_list(&entries, Duration::from_secs(1));
        assert_eq!(view.candidates(), Some(vec![LOCAL, "graph-node-2"]));
    }

    /// A leftover object from a node that is no longer configured must not win
    /// a cell, because nothing would ever route to it.
    #[test]
    fn heartbeats_from_nodes_outside_the_configured_fleet_are_ignored() {
        let entries = [entry(LOCAL, 1), entry("graph-node-99", 1)];
        let view = fleet().observe_list(&entries, Duration::from_secs(3_600));
        assert_eq!(view.candidates(), Some(vec![LOCAL]));
    }

    /// Decision 7, inside the bound: one throttled LIST must not drop a healthy
    /// writer, so the cached set is served unchanged.
    #[test]
    fn a_failed_list_inside_grace_serves_the_cached_live_set() {
        let set = seeded(&[entry(LOCAL, 1), entry("graph-node-1", 1)]);
        let cached = set.last_known().to_vec();

        let view = set.observe_list_failure(TIMEOUT - Duration::from_secs(1));
        assert_eq!(view.state(), ViewState::Grace);
        assert_eq!(view.nodes(), cached.as_slice());
        assert_eq!(view.heartbeat_action(), HeartbeatAction::Publish);
        assert!(view.state().is_ready());
    }

    /// Decision 7, past the bound. The assertion that matters is the pairing:
    /// shedding without withdrawing leaves this node the computed owner for
    /// every peer while it refuses every write they send, permanently.
    #[test]
    fn a_failed_list_past_grace_sheds_and_withdraws_the_heartbeat() {
        let set = seeded(&[entry(LOCAL, 1), entry("graph-node-1", 1)]);

        let view = set.observe_list_failure(TIMEOUT);
        assert_eq!(view.state(), ViewState::Shed);
        assert_eq!(view.heartbeat_action(), HeartbeatAction::Withdraw);
        assert!(!view.state().is_ready());
        assert!(view.nodes().is_empty());

        // One tick inside is still grace: the boundary is strict on the same
        // side as `age < timeout`.
        assert_eq!(
            set.observe_list_failure(TIMEOUT - Duration::from_nanos(1))
                .state(),
            ViewState::Grace
        );
    }

    /// `None` is not `Some(&[])`. An empty candidate list means "no live
    /// owner", which licenses self-promotion; a shed view must offer no list at
    /// all, or the caller falls straight through into the unbounded duel.
    #[test]
    fn a_shed_view_offers_no_candidates_while_any_other_view_offers_at_least_itself() {
        let mut set = fleet();
        assert_eq!(
            set.observe_list(&[], Duration::from_secs(3_600))
                .candidates(),
            Some(vec![LOCAL])
        );
        assert_eq!(set.observe_list_failure(TIMEOUT).candidates(), None);
    }

    /// An empty LIST is a successful answer — decision 4 has every peer DELETE
    /// its heartbeat on shutdown — so it refreshes the view and must not be
    /// routed onto the failure path.
    #[test]
    fn an_empty_list_is_a_successful_answer_and_not_a_failure() {
        let mut set = seeded(&[entry(LOCAL, 1), entry("graph-node-1", 1)]);

        let view = set.observe_list(&[], Duration::from_secs(3_600));
        assert_eq!(view.state(), ViewState::Fresh);
        assert_eq!(view.heartbeat_action(), HeartbeatAction::Publish);
        // The peer really is gone, and the cache says so: an empty answer is
        // information, not an absence of it.
        assert_eq!(view.candidates(), Some(vec![LOCAL]));
        assert_eq!(ids(set.last_known()), [LOCAL]);
    }

    /// A failure is not information about the fleet, so it must never overwrite
    /// what the last success established — repeat failures keep serving the
    /// same set until grace runs out.
    #[test]
    fn a_failed_list_never_replaces_the_cached_view() {
        let set = seeded(&[entry(LOCAL, 1), entry("graph-node-1", 1)]);
        let expected = vec![LOCAL, "graph-node-1"];

        for elapsed in [1_u64, 5, 14] {
            let view = set.observe_list_failure(Duration::from_secs(elapsed));
            assert_eq!(view.candidates(), Some(expected.clone()));
        }
        assert_eq!(ids(set.last_known()), expected.as_slice());
    }

    /// Before any LIST at all the fleet is assumed up, which is decision 8's
    /// posture at the moment of least information. Assuming the opposite would
    /// have a starting node claim every cell for the first grace window.
    #[test]
    fn before_the_first_list_the_configured_fleet_is_assumed_live() {
        let set = fleet();
        assert_eq!(ids(set.last_known()), FLEET);
        assert_eq!(
            set.observe_list_failure(Duration::from_secs(1))
                .candidates(),
            Some(FLEET.to_vec())
        );
    }

    #[test]
    fn grace_is_exactly_the_heartbeat_timeout_and_not_a_separate_knob() {
        assert_eq!(fleet().grace(), TIMEOUT);
    }
}
