//! What a replica knows about other nodes' writers: the latest frontier per
//! database, as a watch a reader can follow.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard, Weak};

use tokio::sync::watch;

use crate::{FeedEvent, Frontier};

/// One database's entry: the latest live frontier, and which peer reported it.
struct Entry {
    frontier: watch::Sender<Option<Frontier>>,
    source: Option<String>,
}

impl Entry {
    fn new() -> Self {
        Self {
            frontier: watch::Sender::new(None),
            source: None,
        }
    }

    /// Nobody vouches for the database and no reader here follows it, so
    /// nothing is lost by dropping the entry.
    fn unused(&self) -> bool {
        self.frontier.borrow().is_none() && self.frontier.receiver_count() == 0
    }
}

/// The latest frontier of every database some peer's feed reports.
///
/// `None` means no peer currently vouches for the database: none has
/// reported it, its writer closed, or the peer that reported it disconnected.
/// A follower must then fall back to reading object storage, as without the
/// feed. Each frontier only moves forward, and a newer writer's replaces an
/// older one's, whichever peer reports it.
///
/// Entries are kept only while some peer vouches for the database or some
/// reader here follows it, so the board holds at most the peers' live
/// writers plus the databases this node reads, however many databases come
/// and go.
///
/// Every operation is one lookup under the board's lock; none scans it. An
/// entry is removed at the moment it becomes unused: when its writer closes
/// or its peer is lost with nobody following it, or when its last
/// [`Followed`] handle is dropped with nobody vouching for it.
#[derive(Default)]
pub struct FrontierBoard {
    entries: Arc<Entries>,
}

type Entries = Mutex<HashMap<String, Entry>>;

fn lock(entries: &Entries) -> MutexGuard<'_, HashMap<String, Entry>> {
    entries.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// A reader's hold on one database's frontier: a watch of it, which removes
/// the database's entry when dropped if nothing else needs it.
///
/// The underlying receiver never leaves the handle: every way to watch the
/// frontier is a method here, and a clone is another `Followed`. Each
/// receiver of an entry therefore belongs to a handle whose drop checks
/// whether it was the last one, so no copy can outlive the cleanup.
pub struct Followed {
    frontier: Option<watch::Receiver<Option<Frontier>>>,
    db: String,
    entries: Weak<Entries>,
}

type FrontierRef<'a> = watch::Ref<'a, Option<Frontier>>;

impl Followed {
    fn receiver(&self) -> &watch::Receiver<Option<Frontier>> {
        self.frontier.as_ref().expect("held until drop")
    }

    fn receiver_mut(&mut self) -> &mut watch::Receiver<Option<Frontier>> {
        self.frontier.as_mut().expect("held until drop")
    }

    /// The current frontier. See [`watch::Receiver::borrow`].
    pub fn borrow(&self) -> FrontierRef<'_> {
        self.receiver().borrow()
    }

    /// The current frontier, marked seen. See
    /// [`watch::Receiver::borrow_and_update`].
    pub fn borrow_and_update(&mut self) -> FrontierRef<'_> {
        self.receiver_mut().borrow_and_update()
    }

    /// Whether the frontier changed since it was last seen. See
    /// [`watch::Receiver::has_changed`].
    pub fn has_changed(&self) -> Result<bool, watch::error::RecvError> {
        self.receiver().has_changed()
    }

    /// Wait for the next change. See [`watch::Receiver::changed`].
    pub async fn changed(&mut self) -> Result<(), watch::error::RecvError> {
        self.receiver_mut().changed().await
    }

    /// Wait until the frontier satisfies `condition`. See
    /// [`watch::Receiver::wait_for`].
    pub async fn wait_for(&mut self, condition: impl FnMut(&Option<Frontier>) -> bool) -> Result<FrontierRef<'_>, watch::error::RecvError> {
        self.receiver_mut().wait_for(condition).await
    }
}

impl Clone for Followed {
    fn clone(&self) -> Self {
        Self {
            frontier: Some(self.receiver().clone()),
            db: self.db.clone(),
            entries: Weak::clone(&self.entries),
        }
    }
}

impl Drop for Followed {
    fn drop(&mut self) {
        // Let go of the watch first, so the entry counts one follower fewer.
        drop(self.frontier.take());
        if let Some(entries) = self.entries.upgrade() {
            let mut entries = lock(&entries);
            if entries.get(&self.db).is_some_and(Entry::unused) {
                entries.remove(&self.db);
            }
        }
    }
}

impl FrontierBoard {
    pub fn new() -> Self {
        Self::default()
    }

    /// Follow `db`: its current frontier, then every change, for as long as
    /// the handle lives.
    pub fn follow(&self, db: &str) -> Followed {
        let frontier = lock(&self.entries)
            .entry(db.to_string())
            .or_insert_with(Entry::new)
            .frontier
            .subscribe();
        Followed {
            frontier: Some(frontier),
            db: db.to_string(),
            entries: Arc::downgrade(&self.entries),
        }
    }

    /// The current frontier of `db`, if a peer vouches for one.
    pub fn latest(&self, db: &str) -> Option<Frontier> {
        lock(&self.entries).get(db).and_then(|entry| *entry.frontier.borrow())
    }

    /// The peer that currently vouches for `db`: the node whose feed reported
    /// its latest frontier, and so the node to ask to confirm it.
    pub fn vouching(&self, db: &str) -> Option<String> {
        lock(&self.entries)
            .get(db)
            .filter(|entry| entry.frontier.borrow().is_some())
            .and_then(|entry| entry.source.clone())
    }

    /// Apply one event from `source`'s feed.
    pub fn apply(&self, source: &str, event: FeedEvent) {
        let mut entries = lock(&self.entries);
        match event {
            FeedEvent::Frontier { db, frontier } => {
                let entry = entries.entry(db).or_insert_with(Entry::new);
                let advanced = entry.frontier.send_if_modified(|current| {
                    let advances = current.is_none_or(|current| frontier.supersedes(&current));
                    if advances {
                        *current = Some(frontier);
                    }
                    advances
                });
                if advanced {
                    entry.source = Some(source.to_string());
                }
            }
            FeedEvent::Closed { db, writer_epoch } => {
                if let Some(entry) = entries.get_mut(&db) {
                    let cleared = entry.frontier.send_if_modified(|current| {
                        let ends = current.is_some_and(|current| current.writer_epoch == writer_epoch);
                        if ends {
                            *current = None;
                        }
                        ends
                    });
                    if cleared {
                        entry.source = None;
                    }
                    if entry.unused() {
                        entries.remove(&db);
                    }
                }
            }
        }
    }

    /// `source`'s feed is gone: nothing it reported can be relied on to keep
    /// moving, so stop vouching for it.
    ///
    /// The one operation that walks the board, and it runs only when a peer
    /// connection ends.
    pub fn forget(&self, source: &str) {
        let mut entries = lock(&self.entries);
        for entry in entries.values_mut().filter(|entry| entry.source.as_deref() == Some(source)) {
            entry.source = None;
            entry.frontier.send_replace(None);
        }
        entries.retain(|_, entry| !entry.unused());
    }
}
