//! Buddy WAL in the store, both directions.
//!
//! - Owner side: the process writer registry already holds every routed
//!   writer this process owns, keyed by database path and node; [`NodeWriters`]
//!   lists one node's for the feed. It never opens a store or promotes a
//!   writer: it only reports writers that exist.
//!   [`NodeWriters`] also confirms a writer for a peer's strong read, with the
//!   same shared check the owner's own strong reads use.
//! - Replica side: with a [`WalFeed`], each reader follows its writer's
//!   frontier, replaying exactly the WAL files the writer reports flushed;
//!   bookmark waits wait on the reader rather than polling object storage;
//!   and strong reads ask the owner to confirm its writer instead of
//!   refreshing from object storage.

use futures::future::BoxFuture;
use futures::stream::BoxStream;
use futures::{FutureExt as _, StreamExt as _};
use hydradb_buddy_wal::{
    BuddyWalMetrics, CatchUp, Confirmed, FollowedWait, Frontier, FrontierBoard, FrontierConfirmer, FrontierStream, LiveWriter, StrongRead,
    WriterDirectory,
};
use slatedb::DbReader;

use super::*;

/// A store, held weakly: what a writer state keeps of the store that
/// installed its writer.
#[derive(Clone)]
pub(super) struct WeakStore {
    inner: Weak<SlateGraphStoreInner>,
    mutation: Weak<SlateMutationState>,
}

impl WeakStore {
    pub(super) fn of(store: &SlateGraphStore) -> Self {
        Self {
            inner: Arc::downgrade(&store.inner),
            mutation: Arc::downgrade(&store.mutation),
        }
    }

    fn upgrade(&self) -> Option<SlateGraphStore> {
        Some(SlateGraphStore {
            inner: self.inner.upgrade()?,
            mutation: self.mutation.upgrade()?,
        })
    }

    fn is_dropped(&self) -> bool {
        self.inner.strong_count() == 0
    }
}

impl SlateGraphStore {
    /// Register this store with its writer state, beside any other store
    /// sharing it, so the writer can be confirmed through whichever is still
    /// open. Called once, as the store is built.
    pub(super) fn sharing_writer_state(self) -> Self {
        let mut stores = self
            .inner
            .writer_state
            .stores
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        stores.retain(|store| !store.is_dropped());
        stores.push(WeakStore::of(&self));
        drop(stores);
        self
    }
}

impl ProcessWriterState {
    /// A store sharing this state that still sees the live writer: the one to
    /// confirm it through. Not necessarily the store that opened the writer,
    /// which may since have closed while another keeps the writer alive.
    fn confirming_store(&self) -> Option<SlateGraphStore> {
        let stores = self.stores.lock().unwrap_or_else(|poisoned| poisoned.into_inner()).clone();
        stores
            .iter()
            .filter_map(WeakStore::upgrade)
            .find(|store| store.readable_writer().is_some())
    }
}

/// The writers one node holds, as a [`WriterDirectory`].
pub struct NodeWriters {
    registry: Arc<ProcessWriterRegistry>,
    node_id: String,
}

impl ProcessWriterRegistry {
    /// The writers `node_id` holds in this registry.
    pub fn node_writers(self: &Arc<Self>, node_id: impl Into<String>) -> NodeWriters {
        NodeWriters {
            registry: Arc::clone(self),
            node_id: node_id.into(),
        }
    }
}

impl WriterDirectory for NodeWriters {
    fn live_writers(&self) -> Vec<LiveWriter> {
        let states: Vec<_> = {
            let states = self.registry.states.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
            states
                .iter()
                .filter(|(key, _)| key.node_id == self.node_id)
                .filter_map(|(key, state)| Some((key.path.clone(), state.upgrade()?)))
                .collect()
        };
        states.into_iter().filter_map(|(db, state)| state.live_writer(db)).collect()
    }

    fn installs(&self) -> BoxStream<'static, ()> {
        let installs = self.registry.installs.subscribe();
        futures::stream::unfold(installs, |mut installs| async move {
            installs.changed().await.ok()?;
            Some(((), installs))
        })
        .boxed()
    }

    fn confirm(&self, db: &str) -> BoxFuture<'static, std::result::Result<Option<Confirmed>, String>> {
        let key = ProcessWriterKey {
            path: db.to_string(),
            node_id: self.node_id.clone(),
        };
        // One keyed lookup, not a scan: the registry is keyed by path and node.
        let state = self
            .registry
            .states
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(&key)
            .and_then(Weak::upgrade);
        async move {
            let Some(store) = state.and_then(|state| state.confirming_store()) else {
                return Ok(None);
            };
            store.confirm_for_peer().await.map_err(|error| error.to_string())
        }
        .boxed()
    }
}

impl ProcessWriterState {
    /// This state's writer, if it is open and not closing.
    fn live_writer(&self, db: String) -> Option<LiveWriter> {
        if self.is_closing() {
            return None;
        }
        let writer = self.writer.read().unwrap_or_else(|poisoned| poisoned.into_inner()).clone()?;
        let status = writer.status();
        if status.close_reason.is_some() {
            return None;
        }
        Some(LiveWriter {
            db,
            writer_epoch: status.current_manifest.writer_epoch(),
            frontiers: frontiers(writer),
        })
    }
}

/// `writer`'s durable frontier, starting at its current state.
///
/// Follows `Db::subscribe`, which wakes on every status change, and ends when
/// the writer closes or is fenced. Each item is read from SlateDB's own status
/// after the fact, so it only describes state already durable in object
/// storage. The handle is kept for `last_flushed_wal_id`, which SlateDB
/// exposes only on `Db`; every close path sets `close_reason`, which ends the
/// stream and releases it.
fn frontiers(writer: TrackedMemory<Db>) -> FrontierStream {
    let status = writer.subscribe();
    futures::stream::unfold((writer, status, true), |(writer, mut status, first)| async move {
        if !first && status.changed().await.is_err() {
            return None;
        }
        let (writer_epoch, durable_seq, manifest_id, closed) = {
            let current = status.borrow_and_update();
            (
                current.current_manifest.writer_epoch(),
                current.durable_seq,
                current.current_manifest.id(),
                current.close_reason.is_some(),
            )
        };
        if closed {
            return None;
        }
        let frontier = Frontier {
            writer_epoch,
            durable_seq,
            last_wal_id: writer.last_flushed_wal_id(),
            manifest_id,
        };
        Some((frontier, (writer, status, false)))
    })
    .boxed()
}

/// Peers' writer frontiers, as handed to a store through
/// [`GraphOpenOptions::wal_feed`](crate::GraphOpenOptions::wal_feed), and
/// optionally a way to ask those peers to confirm a writer for a strong read.
#[derive(Clone)]
pub struct WalFeed {
    board: Arc<FrontierBoard>,
    confirmer: Option<Arc<dyn FrontierConfirmer>>,
    metrics: Arc<BuddyWalMetrics>,
}

impl WalFeed {
    pub fn new(board: Arc<FrontierBoard>) -> Self {
        Self {
            board,
            confirmer: None,
            metrics: Arc::new(BuddyWalMetrics::new()),
        }
    }

    /// Serve strong reads by asking the vouching peer to confirm its writer.
    pub fn with_confirmer(mut self, confirmer: Arc<dyn FrontierConfirmer>) -> Self {
        self.confirmer = Some(confirmer);
        self
    }

    /// Record catch-ups, followed waits and strong reads in `metrics`, the
    /// node's shared buddy WAL metrics.
    pub fn with_metrics(mut self, metrics: Arc<BuddyWalMetrics>) -> Self {
        self.metrics = metrics;
        self
    }
}

/// How a wait on a followed reader ended.
enum Followed {
    Reached(u64),
    Unvouched,
    FeedLost,
    ReaderClosed,
    Deadline,
}

impl Followed {
    fn outcome(&self) -> FollowedWait {
        match self {
            Self::Reached(_) => FollowedWait::Reached,
            Self::Unvouched => FollowedWait::Unvouched,
            Self::FeedLost => FollowedWait::FeedLost,
            Self::ReaderClosed => FollowedWait::ReaderClosed,
            Self::Deadline => FollowedWait::Deadline,
        }
    }

    fn reached(&self) -> Option<u64> {
        match self {
            Self::Reached(sequence) => Some(*sequence),
            _ => None,
        }
    }
}

/// Two feeds are the same feed only if they share the board and the
/// confirmer.
impl PartialEq for WalFeed {
    fn eq(&self, other: &Self) -> bool {
        let same_confirmer = match (&self.confirmer, &other.confirmer) {
            (Some(left), Some(right)) => Arc::ptr_eq(left, right),
            (None, None) => true,
            _ => false,
        };
        Arc::ptr_eq(&self.board, &other.board) && same_confirmer && Arc::ptr_eq(&self.metrics, &other.metrics)
    }
}

impl Eq for WalFeed {}

impl std::fmt::Debug for WalFeed {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("WalFeed")
    }
}

/// How long a replica's strong read waits for its reader to reach the owner's
/// confirmed frontier, after the confirmation itself, before it refreshes from
/// object storage instead. The follower replays each flush as it lands, so
/// this is normally one flush interval.
const CONFIRMED_WAIT: Duration = Duration::from_secs(2);

/// How soon a follower whose catch-up failed tries again, doubling up to
/// [`MAX_RETRY`] while storage keeps failing. A new frontier retries at once.
const MIN_RETRY: Duration = Duration::from_millis(100);
const MAX_RETRY: Duration = Duration::from_secs(5);

impl SlateGraphStore {
    /// Keep `reader` caught up with its writer's frontier for as long as the
    /// reader is open.
    ///
    /// Each frontier names the last WAL file the writer flushed, so the reader
    /// replays exactly those files: no manifest read and no search for where
    /// the WAL ends. A replay that fails (the files were garbage collected
    /// after a newer manifest covered them) falls back to a full refresh, and
    /// if that fails too the follower retries with backoff rather than waiting
    /// for a frontier a quiet writer may never send. Only WAL is followed
    /// here; moving to a newer manifest stays with the reader's own background
    /// poll. The task holds the reader weakly and ends when the reader closes
    /// or is dropped.
    pub(super) fn follow_wal_feed(&self, reader: &Arc<TrackedMemory<DbReader>>) {
        let Some(WalFeed { board, metrics, .. }) = &self.inner.wal_feed else {
            return;
        };
        let metrics = Arc::clone(metrics);
        let mut frontiers = board.follow(self.inner.path.as_ref());
        let mut status = reader.subscribe();
        let reader = Arc::downgrade(reader);
        let db = self.inner.path.to_string();
        self.inner.runtime_handle.spawn(async move {
            let mut retry = MIN_RETRY;
            loop {
                let through = frontiers.borrow_and_update().map(|frontier| frontier.last_wal_id);
                let behind = match through {
                    Some(through) => {
                        let Some(reader) = reader.upgrade() else {
                            return;
                        };
                        let started = std::time::Instant::now();
                        let outcome = catch_up(&reader, through, &db).await;
                        metrics.catch_up(outcome, started.elapsed());
                        outcome == CatchUp::Failed
                    }
                    None => false,
                };
                let retry_after = async {
                    if behind {
                        tokio::time::sleep(retry).await;
                    } else {
                        std::future::pending::<()>().await;
                    }
                };
                tokio::select! {
                    changed = frontiers.changed() => {
                        if changed.is_err() {
                            return;
                        }
                        retry = MIN_RETRY;
                    }
                    () = retry_after => retry = (retry * 2).min(MAX_RETRY),
                    // The reader's status sender lives with the reader, so this
                    // ends when the reader is dropped, and a close is a change.
                    _ = status.wait_for(|status| status.close_reason.is_some()) => return,
                }
            }
        });
    }

    /// The owner's side of a peer's strong read: confirm this store's writer
    /// with the same shared check this node's own strong reads use
    /// (`confirm_writer_current`), and report what it has acknowledged.
    /// `None` when there is no live writer here or the check finds it
    /// superseded; never opens or promotes one.
    pub(super) async fn confirm_for_peer(&self) -> Result<Option<Confirmed>> {
        if self.readable_writer().is_none() {
            return Ok(None);
        }
        match self.confirm_writer_current().await {
            Ok(confirmed) => Ok(confirmed),
            // A fenced writer was cleared by the check: not ours to confirm.
            Err(_) if self.readable_writer().is_none() => Ok(None),
            Err(error) => Err(error),
        }
    }

    /// The replica's side of a strong read: ask the peer whose feed vouches
    /// for this database to confirm its writer, then wait until this node's
    /// reader has applied what that writer had acknowledged.
    ///
    /// The owner's check begins after the question arrives, which is after
    /// the read arrived, so the confirmed sequence covers every write
    /// acknowledged before the read, on any node: the same argument as the
    /// owner's own strong reads. `None` whenever that path cannot answer (no
    /// feed or confirmer, nobody vouching, a refusal, a timeout, or the reader
    /// not catching up within [`CONFIRMED_WAIT`]); the caller then refreshes
    /// from object storage as before.
    pub(super) async fn strong_frontier_from_owner(&self) -> Result<Option<u64>> {
        let Some(WalFeed {
            board,
            confirmer: Some(confirmer),
            metrics,
        }) = &self.inner.wal_feed
        else {
            return Ok(None);
        };
        let db = self.inner.path.as_ref();
        let Some(peer) = board.vouching(db) else {
            metrics.strong_read(StrongRead::Unvouched);
            return Ok(None);
        };
        let Some(confirmed) = confirmer.confirm(&peer, db).await else {
            metrics.strong_read(StrongRead::Declined);
            return Ok(None);
        };
        let followed = self
            .follow_until(board, confirmed.durable_seq, tokio::time::Instant::now() + CONFIRMED_WAIT)
            .await?;
        metrics.strong_read(match followed {
            Followed::Reached(_) => StrongRead::Confirmed,
            _ => StrongRead::ReaderBehind,
        });
        Ok(followed.reached())
    }

    /// Wait for this store's reader to reach `minimum`, if a peer's feed
    /// vouches for its writer: the reader's follower replays each flush as it
    /// is reported, so this only waits for that, with no polling.
    ///
    /// `None` when no feed vouches for the writer, when the feed stops
    /// vouching during the wait (the peer went away or the writer closed), or
    /// when `deadline` comes first; the caller then refreshes from object
    /// storage as without a feed, with whatever budget is left.
    pub async fn wait_for_followed_sequence(&self, minimum: u64, deadline: tokio::time::Instant) -> Result<Option<u64>> {
        let Some(WalFeed { board, metrics, .. }) = &self.inner.wal_feed else {
            return Ok(None);
        };
        let started = std::time::Instant::now();
        let followed = self.follow_until(board, minimum, deadline).await?;
        metrics.followed_wait(followed.outcome(), started.elapsed());
        Ok(followed.reached())
    }

    /// Wait for the reader to reach `minimum` while `board` vouches for its
    /// writer, and say how the wait ended. Shared by bookmark waits and
    /// replica strong reads, which record it as different metrics.
    async fn follow_until(&self, board: &FrontierBoard, minimum: u64, deadline: tokio::time::Instant) -> Result<Followed> {
        // A plain lookup first: a wait no peer can answer leaves the board
        // untouched, and only a vouched database is followed.
        if board.latest(self.inner.path.as_ref()).is_none() {
            return Ok(Followed::Unvouched);
        }
        let mut vouched = board.follow(self.inner.path.as_ref());
        if vouched.borrow_and_update().is_none() {
            return Ok(Followed::Unvouched);
        }
        let Some(reader) = self.open_reader().await? else {
            return Ok(Followed::Unvouched);
        };
        let mut status = reader.subscribe();
        let followed = async {
            tokio::select! {
                reached = status.wait_for(|status| status.durable_seq >= minimum || status.close_reason.is_some()) => {
                    match reached {
                        Ok(status) if status.close_reason.is_none() => Followed::Reached(status.durable_seq),
                        _ => Followed::ReaderClosed,
                    }
                }
                _ = vouched.wait_for(Option::is_none) => Followed::FeedLost,
            }
        };
        Ok(tokio::time::timeout_at(deadline, followed).await.unwrap_or(Followed::Deadline))
    }
}

/// Bring `reader` through WAL file `through`, and say how: by replaying the
/// reported files, by a full refresh after the replay failed, or not at all
/// (`Failed`: the reader is still behind and the follower retries).
async fn catch_up(reader: &DbReader, through: u64, db: &str) -> CatchUp {
    // Files already replayed are skipped without I/O.
    let Err(error) = reader.replay_wal_through(through).await else {
        return CatchUp::Replayed;
    };
    tracing::debug!(%db, through, %error, "buddy WAL replay failed; refreshing the reader instead");
    match reader.refresh().await {
        Ok(()) => CatchUp::Refreshed,
        Err(error) => {
            tracing::warn!(%db, %error, "reader refresh after a failed buddy WAL replay failed; retrying");
            CatchUp::Failed
        }
    }
}
