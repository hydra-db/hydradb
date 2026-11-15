//! What buddy WAL is doing, as Prometheus series.
//!
//! One [`BuddyWalMetrics`] per node, shared by every part of buddy WAL on it:
//! the owner's feed and confirm routes, the replica's peer followers, and the
//! stores' readers and waits. It holds plain atomics and renders its own text,
//! so the crate needs no metrics library and the host only appends
//! [`BuddyWalMetrics::render`] to its `/metrics` body.
//!
//! The series answer the questions a rollout needs answered:
//! - are the followers connected, and why do their sessions end;
//! - do replicas catch up from the feed, or fall back to object storage;
//! - are bookmark waits and strong reads served by the feed, and if not, why;
//! - how long catching up and waiting take.

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// Defines an outcome enum whose variants index a counter array, with the
/// label each renders as.
macro_rules! outcomes {
    ($(#[$doc:meta])* $name:ident { $($(#[$vdoc:meta])* $variant:ident => $label:literal),+ $(,)? }) => {
        $(#[$doc])*
        #[derive(Clone, Copy, Debug, PartialEq, Eq)]
        pub enum $name { $($(#[$vdoc])* $variant),+ }

        impl $name {
            const ALL: &'static [$name] = &[$($name::$variant),+];

            fn label(self) -> &'static str {
                match self { $($name::$variant => $label),+ }
            }
        }
    };
}

outcomes! {
    /// How the owner answered a peer's request to confirm a writer.
    ConfirmServed {
        /// The writer was proved current.
        Confirmed => "confirmed",
        /// This node holds no current writer for the database.
        NotOwner => "not_owner",
        /// The check failed.
        Unavailable => "unavailable",
    }
}

outcomes! {
    /// How a replica's reader came to a frontier the feed reported.
    CatchUp {
        /// Replayed exactly the reported WAL files.
        Replayed => "replayed",
        /// The replay failed and a full refresh from object storage did it.
        Refreshed => "refreshed",
        /// Both failed; the follower retries.
        Failed => "failed",
    }
}

outcomes! {
    /// How a bookmark wait on a followed reader ended.
    FollowedWait {
        /// The reader reached the bookmark, woken by the feed.
        Reached => "reached",
        /// No peer vouched for the writer, so the wait polled object storage.
        Unvouched => "unvouched",
        /// The feed stopped vouching mid-wait; the wait polled object storage.
        FeedLost => "feed_lost",
        /// The reader closed mid-wait.
        ReaderClosed => "reader_closed",
        /// The wait reached its deadline; the caller polled object storage.
        Deadline => "deadline",
    }
}

outcomes! {
    /// How a strong read on a replica was served.
    StrongRead {
        /// The owner confirmed and the reader reached its frontier.
        Confirmed => "confirmed",
        /// No peer vouched for the writer: refreshed from object storage.
        Unvouched => "unvouched",
        /// The peer did not confirm (not the owner, unreachable, slow).
        Declined => "declined",
        /// The owner confirmed, but the reader did not catch up in time.
        ReaderBehind => "reader_behind",
    }
}

/// Counters indexed by an outcome enum.
struct Counters<const N: usize>([AtomicU64; N]);

impl<const N: usize> Default for Counters<N> {
    fn default() -> Self {
        Self(std::array::from_fn(|_| AtomicU64::new(0)))
    }
}

impl<const N: usize> Counters<N> {
    fn add(&self, index: usize) {
        self.0[index].fetch_add(1, Ordering::Relaxed);
    }

    fn get(&self, index: usize) -> u64 {
        self.0[index].load(Ordering::Relaxed)
    }
}

/// Upper bounds of the duration histograms, in seconds: from one replayed WAL
/// file (milliseconds) to a wait running out its budget (seconds).
const BUCKETS: [f64; 12] = [0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0];

/// A Prometheus histogram of durations.
#[derive(Default)]
struct Histogram {
    buckets: [AtomicU64; BUCKETS.len()],
    count: AtomicU64,
    sum_micros: AtomicU64,
}

impl Histogram {
    fn observe(&self, elapsed: Duration) {
        let seconds = elapsed.as_secs_f64();
        if let Some(index) = BUCKETS.iter().position(|bound| seconds <= *bound) {
            self.buckets[index].fetch_add(1, Ordering::Relaxed);
        }
        self.count.fetch_add(1, Ordering::Relaxed);
        self.sum_micros
            .fetch_add(u64::try_from(elapsed.as_micros()).unwrap_or(u64::MAX), Ordering::Relaxed);
    }

    fn render(&self, out: &mut String, name: &str, help: &str) {
        let _ = writeln!(out, "# HELP {name} {help}\n# TYPE {name} histogram");
        let mut cumulative = 0;
        for (bound, bucket) in BUCKETS.iter().zip(&self.buckets) {
            cumulative += bucket.load(Ordering::Relaxed);
            let _ = writeln!(out, "{name}_bucket{{le=\"{bound}\"}} {cumulative}");
        }
        let count = self.count.load(Ordering::Relaxed);
        let _ = writeln!(out, "{name}_bucket{{le=\"+Inf\"}} {count}");
        let sum = self.sum_micros.load(Ordering::Relaxed) as f64 / 1e6;
        let _ = writeln!(out, "{name}_sum {sum}\n{name}_count {count}");
    }
}

/// One followed peer.
#[derive(Default)]
struct PeerState {
    connected: bool,
    sessions_ended: BTreeMap<&'static str, u64>,
}

/// Buddy WAL's metrics for one node. See the module docs.
#[derive(Default)]
pub struct BuddyWalMetrics {
    feed_subscribers: AtomicI64,
    confirms_served: Counters<3>,
    peers: Mutex<BTreeMap<String, PeerState>>,
    frontier_events: AtomicU64,
    catch_ups: Counters<3>,
    catch_up_seconds: Histogram,
    followed_waits: Counters<5>,
    followed_wait_seconds: Histogram,
    strong_reads: Counters<4>,
}

/// Counts one open feed subscription until dropped.
pub struct FeedSubscription(Arc<BuddyWalMetrics>);

impl Drop for FeedSubscription {
    fn drop(&mut self) {
        self.0.feed_subscribers.fetch_sub(1, Ordering::Relaxed);
    }
}

impl BuddyWalMetrics {
    pub fn new() -> Self {
        Self::default()
    }

    /// A feed subscription opened; it counts until the returned guard drops.
    pub fn feed_subscription(self: &Arc<Self>) -> FeedSubscription {
        self.feed_subscribers.fetch_add(1, Ordering::Relaxed);
        FeedSubscription(Arc::clone(self))
    }

    pub fn confirm_served(&self, outcome: ConfirmServed) {
        self.confirms_served.add(outcome as usize);
    }

    /// Whether this node currently has an accepted subscription to `peer`.
    pub fn peer_connected(&self, peer: &str, connected: bool) {
        self.peers().entry(peer.to_string()).or_default().connected = connected;
    }

    /// A subscription to `peer` ended, for `reason`.
    pub fn peer_session_ended(&self, peer: &str, reason: &'static str) {
        let mut peers = self.peers();
        let state = peers.entry(peer.to_string()).or_default();
        state.connected = false;
        *state.sessions_ended.entry(reason).or_default() += 1;
    }

    /// Feed events applied to this node's board.
    pub fn frontier_events(&self, events: u64) {
        self.frontier_events.fetch_add(events, Ordering::Relaxed);
    }

    /// A reader came to a reported frontier, taking `elapsed`.
    pub fn catch_up(&self, outcome: CatchUp, elapsed: Duration) {
        self.catch_ups.add(outcome as usize);
        self.catch_up_seconds.observe(elapsed);
    }

    /// A bookmark wait on a followed reader ended, after `elapsed`.
    pub fn followed_wait(&self, outcome: FollowedWait, elapsed: Duration) {
        self.followed_waits.add(outcome as usize);
        self.followed_wait_seconds.observe(elapsed);
    }

    pub fn strong_read(&self, outcome: StrongRead) {
        self.strong_reads.add(outcome as usize);
    }

    fn peers(&self) -> std::sync::MutexGuard<'_, BTreeMap<String, PeerState>> {
        self.peers.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Every series, in the Prometheus text format.
    pub fn render(&self) -> String {
        let mut out = String::new();
        let gauge = |out: &mut String, name: &str, help: &str, value: i64| {
            let _ = writeln!(out, "# HELP {name} {help}\n# TYPE {name} gauge\n{name} {value}");
        };
        gauge(
            &mut out,
            "graph_buddy_wal_feed_subscribers",
            "Open subscriptions to this node's feed.",
            self.feed_subscribers.load(Ordering::Relaxed),
        );
        render_outcomes(
            &mut out,
            "graph_buddy_wal_confirms_served_total",
            "Writer confirmations this node answered for peers' strong reads.",
            ConfirmServed::ALL,
            ConfirmServed::label,
            |outcome| self.confirms_served.get(*outcome as usize),
        );

        let peers = self.peers();
        let _ = writeln!(
            out,
            "# HELP graph_buddy_wal_peer_connected Whether this node has an accepted subscription to the peer's feed.\n# TYPE graph_buddy_wal_peer_connected gauge"
        );
        for (peer, state) in peers.iter() {
            let _ = writeln!(
                out,
                "graph_buddy_wal_peer_connected{{peer=\"{peer}\"}} {}",
                u8::from(state.connected)
            );
        }
        let _ = writeln!(
            out,
            "# HELP graph_buddy_wal_peer_sessions_ended_total Subscriptions to a peer's feed that ended, by reason.\n# TYPE graph_buddy_wal_peer_sessions_ended_total counter"
        );
        for (peer, state) in peers.iter() {
            for (reason, count) in &state.sessions_ended {
                let _ = writeln!(
                    out,
                    "graph_buddy_wal_peer_sessions_ended_total{{peer=\"{peer}\",reason=\"{reason}\"}} {count}"
                );
            }
        }
        drop(peers);

        let _ = writeln!(
            out,
            "# HELP graph_buddy_wal_frontier_events_total Feed events applied to this node's board.\n# TYPE graph_buddy_wal_frontier_events_total counter\ngraph_buddy_wal_frontier_events_total {}",
            self.frontier_events.load(Ordering::Relaxed)
        );
        render_outcomes(
            &mut out,
            "graph_buddy_wal_catch_ups_total",
            "Readers brought to a reported frontier, by how.",
            CatchUp::ALL,
            CatchUp::label,
            |outcome| self.catch_ups.get(*outcome as usize),
        );
        self.catch_up_seconds.render(
            &mut out,
            "graph_buddy_wal_catch_up_seconds",
            "Time for a reader to reach a reported frontier.",
        );
        render_outcomes(
            &mut out,
            "graph_buddy_wal_followed_waits_total",
            "Bookmark waits on a followed reader, by how they ended.",
            FollowedWait::ALL,
            FollowedWait::label,
            |outcome| self.followed_waits.get(*outcome as usize),
        );
        self.followed_wait_seconds.render(
            &mut out,
            "graph_buddy_wal_followed_wait_seconds",
            "Time a bookmark wait spent on a followed reader.",
        );
        render_outcomes(
            &mut out,
            "graph_buddy_wal_strong_reads_total",
            "Strong reads on a replica, by how they were served.",
            StrongRead::ALL,
            StrongRead::label,
            |outcome| self.strong_reads.get(*outcome as usize),
        );
        out
    }
}

fn render_outcomes<T>(out: &mut String, name: &str, help: &str, outcomes: &[T], label: fn(T) -> &'static str, value: impl Fn(&T) -> u64)
where
    T: Copy,
{
    let _ = writeln!(out, "# HELP {name} {help}\n# TYPE {name} counter");
    for outcome in outcomes {
        let _ = writeln!(out, "{name}{{outcome=\"{}\"}} {}", label(*outcome), value(outcome));
    }
}
