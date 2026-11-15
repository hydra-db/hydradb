//! The placement seam: one live set, shared by everything in the kernel that
//! needs to know who owns a cell's writer.
//!
//! [`PlacementView`] is a cheaply-clonable handle around
//! [`hydradb_placement::LiveNodeSet`]. A background task refreshes it once per
//! `heartbeat_interval` from a single object-store LIST; every reader — the
//! Bolt routing provider on one side, `RoutedGraphCluster::ensure_local_writer`
//! on the other — takes a snapshot of the *same* view, synchronously, with no
//! I/O.
//!
//! **The sharing is the point, not an optimisation.** Rendezvous placement only
//! ends the writer duel if every reader computes ownership from the same live
//! set. That is why decision 4 of
//! the rendezvous placement design deletes the per-caller
//! `/readyz` fan-out: N probes are computed per caller and two drivers asking at
//! the same instant can get different answers, whereas one LIST behind one
//! handle cannot. A second `PlacementView` over the same store would reintroduce
//! exactly the inconsistency the probe had, so the runtime builds one and clones
//! it.
//!
//! # This module owns the clock, and it is the only one that may
//!
//! `hydradb-placement` reads no clock at all (decision 10): `list_heartbeats`
//! is told what time it is, and every liveness rule below it is a pure function
//! of `Duration`s. Somebody has to actually look at a clock, and this seam is
//! that somebody:
//!
//! - **Wall clock.** [`PlacementView::refresh`] is the only place in the kernel
//!   that reads the wall clock for placement. If a second placement `now` ever
//!   appears, two readers can disagree about a heartbeat's age, and that is the
//!   bug this note exists to prevent.
//! - **Monotonic clock.** The `Instant`s behind decision 8's `since_start` and
//!   decision 7's `since_last_success` live in [`RefreshState`] and nowhere
//!   else. They are monotonic and measure elapsed duration only; cross-node
//!   comparison stays on object-storage `LastModified`, which is the whole
//!   reason liveness is not derived from a local clock.
//!
//! # What this module is not
//!
//! It does not publish heartbeats — that is the readiness-gated publisher in
//! `graph-node`, which reads [`PlacementView::heartbeat_action`] to know whether
//! decision 7 has obliged it to withdraw. It does not build routing tables, and
//! it does not promote writers. It answers one question, four ways:
//! [`CellOwnership`].

use super::*;

use std::sync::{Mutex, PoisonError, RwLock};

use chrono::Utc;
use hydradb_placement::hash as rendezvous;
use hydradb_placement::heartbeat::{self, HeartbeatEntry, PlacementError};
use hydradb_placement::liveness::{HeartbeatAction, LiveNodeSet, LiveView, NodeView, ViewState};
use slatedb::object_store::path::Path;

/// The two durations placement runs on: decision 5's 5s / 15s.
///
/// `heartbeat_timeout` is doing double duty on purpose. It is how old a
/// heartbeat may be and still count as live, *and* it is the grace a failing
/// LIST gets before the view is shed (decision 7). They are equal so that peer
/// takeover and local shedding coincide: a shorter grace leaves a window with no
/// owner, a longer one leaves a duel.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PlacementConfig {
    /// How often the refresh task LISTs. Also the publisher's cadence and the
    /// fence backoff in touch point (d).
    pub heartbeat_interval: Duration,
    /// Liveness cutoff, and decision 7's grace window.
    pub heartbeat_timeout: Duration,
}

impl Default for PlacementConfig {
    fn default() -> Self {
        Self {
            heartbeat_interval: Duration::from_secs(5),
            heartbeat_timeout: Duration::from_secs(15),
        }
    }
}

impl PlacementConfig {
    /// Decision 5's rule: a non-zero interval, strictly shorter than the
    /// timeout.
    ///
    /// The authoritative rejection is at process startup, in `graph-node`'s
    /// config parsing, so that a misconfiguration fails before the node serves
    /// anything. This is the same rule restated where the durations are
    /// actually used, so a caller that builds a [`PlacementConfig`] by hand —
    /// a test, or a future embedder — cannot skip it. An interval at or past the
    /// timeout means every heartbeat expires before its own publisher refreshes
    /// it: the whole fleet reads as dead, no cell has an owner, and nothing
    /// about the symptom points at the config.
    pub fn validate(&self) -> Result<()> {
        if self.heartbeat_interval.is_zero() {
            return Err(GraphError::CorruptValue {
                key: "placement/heartbeat_interval".to_string(),
                reason: "heartbeat interval must be greater than zero".to_string(),
            });
        }
        if self.heartbeat_interval >= self.heartbeat_timeout {
            return Err(GraphError::CorruptValue {
                key: "placement/heartbeat_interval".to_string(),
                reason: format!(
                    "heartbeat interval ({}ms) must be less than the heartbeat timeout ({}ms)",
                    self.heartbeat_interval.as_millis(),
                    self.heartbeat_timeout.as_millis(),
                ),
            });
        }
        Ok(())
    }
}

/// Who should own a cell's writer — the **four** states of touch point (b),
/// kept apart because two of the pairs have opposite answers.
///
/// The pair that matters is [`CellOwnership::Unowned`] and
/// [`CellOwnership::Unknown`]. Both mean "rendezvous named nobody", and a bare
/// `Option<String>` collapses them into one — which is how decision 7's
/// permanent-refusal trap gets built by accident:
///
/// - `Unowned` is *the live set is known and empty*. Nobody can be the owner, so
///   this node may promote itself. Refusing here would leave the cell unwritable
///   with nothing to time out.
/// - `Unknown` is *this node does not know the live set* — its LIST has been
///   failing past the grace window and [`LiveView::candidates`] returned `None`.
///   Promoting here is exactly the unbounded duel decision 7 exists to stop: a
///   partitioned node can never learn it lost, so it re-promotes forever.
///
/// The `Unowned` arm is close to unreachable in production, because
/// [`LiveNodeSet`] always counts the local node live (§3), so a successful view
/// has at least one candidate. It is preserved anyway: it costs one match arm,
/// and the arm it would otherwise be folded into is the one that must never be
/// reached by falling through.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CellOwnership {
    /// The rendezvous owner is this node. Promote.
    Local,
    /// The rendezvous owner is a live peer. Refuse, and hand the driver the
    /// hint so it can re-route instead of retrying into the same wrong node.
    Remote {
        /// The owning node's id.
        node_id: String,
    },
    /// The live set is known and nobody owns the cell. Promote.
    Unowned,
    /// This node has shed its view (decision 7) and knows nothing about the
    /// fleet. Refuse, with **no** owner hint — there is nothing truthful to
    /// point at, and a stale guess would send the write to a node that may
    /// itself have shed.
    Unknown,
}

impl CellOwnership {
    /// Whether this node may open the writer for the cell.
    ///
    /// True for [`CellOwnership::Local`] and [`CellOwnership::Unowned`], false
    /// for the other two. This is touch point (b)'s rule in one call, and the
    /// reason [`CellOwnership::Unowned`] is a separate variant rather than a
    /// `None`.
    #[must_use]
    pub fn may_promote(&self) -> bool {
        matches!(self, Self::Local | Self::Unowned)
    }

    /// The owner hint for `GraphError::NotCellWriter`: `Some` only when the
    /// owner is a *different*, live node.
    ///
    /// `None` for [`CellOwnership::Unknown`] is decision 7's rule, and `None`
    /// for [`CellOwnership::Local`] is a type-level nudge — a caller building a
    /// refusal has no business naming itself as the owner.
    #[must_use]
    pub fn owner_hint(&self) -> Option<&str> {
        match self {
            Self::Remote { node_id } => Some(node_id.as_str()),
            Self::Local | Self::Unowned | Self::Unknown => None,
        }
    }
}

/// A cheaply-clonable handle on the fleet's live set and the ownership answers
/// derived from it.
///
/// Clone it freely; every clone shares one `LiveNodeSet`, one refresh task and
/// one published snapshot. Building a second handle over the same store is a
/// bug — see the module docs.
///
/// Reads ([`PlacementView::view`], [`PlacementView::ownership`],
/// [`PlacementView::owner`]) are synchronous, allocate at most a candidate list,
/// and never touch the network. That is a requirement and not a happy accident:
/// §3 puts placement off the write path entirely, and `ensure_local_writer`
/// calls into this on every single write.
#[derive(Clone, Debug)]
pub struct PlacementView {
    shared: Arc<Shared>,
}

#[derive(Debug)]
struct Shared {
    local_node_id: String,
    config: PlacementConfig,
    /// The `LiveNodeSet` and the monotonic clock behind it. A `std::sync::Mutex`
    /// and not a `tokio::sync::Mutex` deliberately: it is taken only by the
    /// refresh task, for the microseconds it takes to fold a LIST result, and it
    /// is never held across an `.await`.
    refresh: Mutex<RefreshState>,
    /// The snapshot readers see. Separate from `refresh` so a reader on the
    /// write path never queues behind a refresh in progress.
    published: RwLock<Arc<LiveView>>,
}

/// The monotonic half of this seam's clock, plus the set it feeds.
///
/// **One origin, not two.** `started` is the only `Instant`; the last successful
/// LIST is recorded as an elapsed duration *from that same origin*. A second
/// `Instant` would work too, right up until the two are read a scheduling
/// quantum apart and decision 7's grace and decision 8's startup window disagree
/// about what "now" is.
#[derive(Debug)]
struct RefreshState {
    live: LiveNodeSet,
    /// Process start, and this seam's monotonic origin.
    started: Instant,
    /// Elapsed-since-start at the last successful LIST, for decision 7's grace
    /// window. Seeded to [`Duration::ZERO`], so a node whose LIST has *never*
    /// worked sheds one `heartbeat_timeout` after boot rather than trusting its
    /// assumed fleet forever.
    last_success: Duration,
    /// Test-only clock offset, added to elapsed-since-start.
    ///
    /// Always [`Duration::ZERO`] outside tests. It exists so the grace and
    /// startup-window rules can be exercised without sleeping through a 15s
    /// timeout — decision 10 gives the placement crate that property for free by
    /// taking `Duration`s, and this is the smallest thing that extends it to the
    /// layer that measures them. Because everything derives from one origin, a
    /// skew applied *before* a successful LIST does not then make that success
    /// look old. Heartbeat *ages* are untouched by it either way: they come from
    /// the store's `LastModified`, not from here.
    skew: Duration,
}

impl RefreshState {
    /// How long this process has been running, for decision 8's startup window.
    fn since_start(&self) -> Duration {
        self.started.elapsed() + self.skew
    }

    /// How long since the last successful LIST, for decision 7's grace window.
    /// Monotone, so the subtraction cannot underflow.
    fn since_last_success(&self) -> Duration {
        self.since_start().saturating_sub(self.last_success)
    }
}

impl PlacementView {
    /// A view for `local_node_id` over the configured fleet `members`.
    ///
    /// Node ids go through the kernel's own `validate_component`, which is the
    /// same character class `hydradb_placement::heartbeat::validate_node_id`
    /// enforces. Checking here rather than at the first LIST means a
    /// misconfigured `GRAPH_NODE_ID` fails at startup instead of becoming a node
    /// that runs, joins the directory, and silently never appears in its own
    /// live set.
    ///
    /// Before the first refresh the published view is
    /// `LiveView::Grace(configured fleet)`: decision 8's posture at the moment
    /// of least information. A process that has not read the store yet must
    /// degrade to "everyone I was told about is up", not to "I am alone and
    /// therefore own everything" — the latter has a starting node claim every
    /// cell. It is bounded regardless: if the LIST keeps failing, grace expires
    /// one `heartbeat_timeout` after start and the node sheds.
    pub fn new<I, S>(local_node_id: impl Into<String>, members: I, config: PlacementConfig) -> Result<Self>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        config.validate()?;
        let local_node_id = local_node_id.into();
        validate_component("node_id", &local_node_id)?;
        let mut member_ids = Vec::new();
        for member in members {
            let member = member.into();
            validate_component("node_id", &member)?;
            member_ids.push(member);
        }

        let live = LiveNodeSet::new(&local_node_id, member_ids, config.heartbeat_timeout);
        // Grace, not Fresh: nothing has been read yet, so the fleet is assumed
        // up but the view is explicitly not authoritative.
        let initial = Arc::new(live.observe_list_failure(Duration::ZERO));
        Ok(Self {
            shared: Arc::new(Shared {
                local_node_id,
                config,
                refresh: Mutex::new(RefreshState {
                    live,
                    started: Instant::now(),
                    last_success: Duration::ZERO,
                    skew: Duration::ZERO,
                }),
                published: RwLock::new(initial),
            }),
        })
    }

    /// A view over the fleet an [`ObjectStoreNodeDirectory`] configures.
    ///
    /// The directory is the *membership* operand of `membership ∩ liveness`; it
    /// performs no object-store I/O despite the name. This constructor exists so
    /// the runtime cannot accidentally build a live set over a different node
    /// list than the cluster was opened with.
    pub fn from_directory(local_node_id: impl Into<String>, directory: &ObjectStoreNodeDirectory, config: PlacementConfig) -> Result<Self> {
        Self::new(local_node_id, directory.node_ids(), config)
    }

    /// This node's id — the one ownership answers are relative to.
    #[must_use]
    pub fn local_node_id(&self) -> &str {
        &self.shared.local_node_id
    }

    /// The durations this view runs on.
    #[must_use]
    pub fn config(&self) -> PlacementConfig {
        self.shared.config
    }

    /// The current snapshot: synchronous, non-blocking, no I/O.
    ///
    /// Returns an `Arc` so a caller building a routing table for many cells can
    /// hold one view across all of them and be sure every entry was computed
    /// from the same live set. Deriving each entry from its own [`view`] call
    /// would let a refresh land in the middle and produce a table that names two
    /// different fleets.
    ///
    /// [`view`]: PlacementView::view
    #[must_use]
    pub fn view(&self) -> Arc<LiveView> {
        Arc::clone(&read_lock(&self.shared.published))
    }

    /// Which of decision 7's three states the current view is in.
    #[must_use]
    pub fn state(&self) -> ViewState {
        self.view().state()
    }

    /// The live nodes as of the current snapshot, for status reporting. Empty
    /// when shed.
    ///
    /// Routing must not use this to decide ownership — it cannot tell "the fleet
    /// is empty" from "I have shed my view". Go through [`PlacementView::view`]
    /// and `LiveView::candidates`, or through [`PlacementView::ownership`].
    #[must_use]
    pub fn live_nodes(&self) -> Vec<NodeView> {
        self.view().nodes().to_vec()
    }

    /// What the heartbeat publisher must do right now.
    ///
    /// [`HeartbeatAction::Withdraw`] is not advisory bookkeeping: a node that
    /// sheds while it keeps publishing stays the computed owner for every peer,
    /// so they route every write to it and it refuses every one, permanently,
    /// with nothing left to time out. Withdrawing is what lets peers take over.
    #[must_use]
    pub fn heartbeat_action(&self) -> HeartbeatAction {
        self.view().heartbeat_action()
    }

    /// Whether this node may serve — false exactly when the view is shed. The
    /// same signal decision 4 gates heartbeat publication on: a node that cannot
    /// read the live set is not ready.
    #[must_use]
    pub fn is_ready(&self) -> bool {
        self.view().state().is_ready()
    }

    /// Who should own `(scope, cell_id)`, in all four states.
    ///
    /// Takes its own snapshot. Use [`PlacementView::ownership_in`] when several
    /// cells must be answered against one view.
    #[must_use]
    pub fn ownership(&self, scope: &str, cell_id: &str) -> CellOwnership {
        self.ownership_in(&self.view(), scope, cell_id)
    }

    /// [`PlacementView::ownership`] against a caller-held snapshot.
    ///
    /// This is the whole of touch point (b), and the `match` is deliberately
    /// exhaustive over `LiveView::candidates`' `Option`: `None` is a shed view,
    /// `Some(&[])` is a known-empty fleet, and there is no way to fall from the
    /// first into the second.
    #[must_use]
    pub fn ownership_in(&self, view: &LiveView, scope: &str, cell_id: &str) -> CellOwnership {
        match view.candidates() {
            None => CellOwnership::Unknown,
            Some(live) => match rendezvous::owner(scope, cell_id, &live) {
                Some(node_id) if node_id == self.shared.local_node_id => CellOwnership::Local,
                Some(node_id) => CellOwnership::Remote {
                    node_id: node_id.to_string(),
                },
                None => CellOwnership::Unowned,
            },
        }
    }

    /// The rendezvous owner's node id, whoever it is — including this node.
    ///
    /// For routing, which needs the owner's *address* and does not care whether
    /// it is the local node. `None` covers both [`CellOwnership::Unowned`] and
    /// [`CellOwnership::Unknown`]; a routing table has the same answer for both
    /// (no WRITE endpoint to advertise), which is why this collapse is safe here
    /// and is not safe in `ensure_local_writer`.
    #[must_use]
    pub fn owner(&self, scope: &str, cell_id: &str) -> Option<String> {
        self.owner_in(&self.view(), scope, cell_id)
    }

    /// [`PlacementView::owner`] against a caller-held snapshot.
    #[must_use]
    pub fn owner_in(&self, view: &LiveView, scope: &str, cell_id: &str) -> Option<String> {
        let live = view.candidates()?;
        rendezvous::owner(scope, cell_id, &live).map(str::to_string)
    }

    /// One refresh tick: LIST the heartbeats, fold them into the live set, and
    /// publish the resulting view.
    ///
    /// **The only wall-clock read on the kernel's placement path.**
    ///
    /// Returns the view now visible to readers *whether or not the LIST
    /// succeeded* — a failed LIST still produces a view, and that view carries
    /// the obligation. The failure is not returned as an `Err` because the
    /// caller's decision does not depend on the error, it depends on the state
    /// the failure put the node in: `ViewState::Grace` means keep serving and
    /// retry, `ViewState::Shed` means withdraw. The underlying store error is
    /// logged at `warn`, which is where it is useful.
    ///
    /// Call it directly once before opening listeners if the first request must
    /// not race the refresh task's first tick; otherwise let
    /// [`PlacementView::spawn_refresh`] drive it.
    pub async fn refresh(&self, store: &dyn ObjectStore, base: &Path) -> Arc<LiveView> {
        let listed = heartbeat::list_heartbeats(store, base, Utc::now()).await;
        self.install(listed)
    }

    /// Spawn the refresh loop. The returned handle stops it when dropped.
    ///
    /// `MissedTickBehavior::Delay`, so a LIST that runs longer than the interval
    /// delays the next tick instead of queueing a burst of them at a store that
    /// is evidently already struggling. `tokio::time::interval` fires its first
    /// tick immediately, so the view is refreshed once as soon as the task is
    /// scheduled.
    #[must_use = "dropping the handle stops the refresh loop and freezes the live set"]
    pub fn spawn_refresh(&self, store: Arc<dyn ObjectStore>, base: Path) -> PlacementRefreshHandle {
        let view = self.clone();
        let interval = self.shared.config.heartbeat_interval;
        let task = tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                ticker.tick().await;
                let _ = view.refresh(store.as_ref(), &base).await;
            }
        });
        PlacementRefreshHandle { task }
    }

    /// Fold one LIST outcome into the live set and publish the result.
    ///
    /// Split out of [`PlacementView::refresh`] so the store I/O and the
    /// decision are separable, and because both elapsed durations must be read
    /// under the same lock that mutates the set — otherwise a concurrent refresh
    /// could stamp `last_success` between the measurement and its use.
    fn install(&self, listed: std::result::Result<Vec<HeartbeatEntry>, PlacementError>) -> Arc<LiveView> {
        let (view, previous, current, since_last_success) = {
            let mut state = lock(&self.shared.refresh);
            let since_last_success = state.since_last_success();
            let view = match listed {
                Ok(entries) => {
                    let since_start = state.since_start();
                    let view = state.live.observe_list(&entries, since_start);
                    state.last_success = since_start;
                    view
                }
                Err(error) => {
                    // A failure teaches this node nothing about the fleet, so
                    // `observe_list_failure` takes `&self` and leaves the cache
                    // alone. All that changes is how long it has been trusted.
                    let view = state.live.observe_list_failure(since_last_success);
                    tracing::warn!(
                        node_id = %self.shared.local_node_id,
                        hydradb.placement.state = view.state().as_str(),
                        since_last_success_ms = since_last_success.as_millis(),
                        error = %error,
                        "placement heartbeat LIST failed"
                    );
                    view
                }
            };
            let view = Arc::new(view);
            // Publish before releasing `refresh`. The loop normally has one
            // writer, but `refresh` is public; keeping the two locks in this
            // order prevents concurrent direct refreshes from publishing their
            // snapshots out of observation order.
            let mut published = write_lock(&self.shared.published);
            let previous = published.state();
            let current = view.state();
            *published = Arc::clone(&view);
            (view, previous, current, since_last_success)
        };
        if previous != current {
            tracing::info!(
                node_id = %self.shared.local_node_id,
                hydradb.placement.previous_state = previous.as_str(),
                hydradb.placement.state = current.as_str(),
                hydradb.placement.live_nodes = view.nodes().len(),
                since_last_success_ms = since_last_success.as_millis(),
                "placement view state changed"
            );
        }
        view
    }
}

/// Keeps the refresh loop alive. Dropping it aborts the task, after which the
/// published view never changes again — which is why it is `#[must_use]` at the
/// call site and why the runtime holds it for the process's lifetime.
#[derive(Debug)]
pub struct PlacementRefreshHandle {
    task: tokio::task::JoinHandle<()>,
}

impl PlacementRefreshHandle {
    /// Stop the refresh loop now.
    pub fn abort(&self) {
        self.task.abort();
    }
}

impl Drop for PlacementRefreshHandle {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// Lock helpers that recover from poisoning instead of panicking.
///
/// A panic elsewhere in the process must not turn every subsequent write into a
/// panic here. The guarded state is a live set and a snapshot, both of which are
/// wholly replaced on the next refresh, so there is no invariant a poisoned
/// lock could be protecting.
fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

fn read_lock<T>(lock: &RwLock<T>) -> std::sync::RwLockReadGuard<'_, T> {
    lock.read().unwrap_or_else(PoisonError::into_inner)
}

fn write_lock<T>(lock: &RwLock<T>) -> std::sync::RwLockWriteGuard<'_, T> {
    lock.write().unwrap_or_else(PoisonError::into_inner)
}
