//! One node's writers, multiplexed into a single stream.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use futures::stream::{self, BoxStream, SelectAll};
use futures::{Stream, StreamExt as _};
use serde::{Deserialize, Serialize};

use crate::{Frontier, LiveWriter, WriterDirectory};

/// What a node's feed says about one of its databases.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum FeedEvent {
    /// `db`'s writer has made `frontier` durable.
    Frontier {
        db: String,
        #[serde(flatten)]
        frontier: Frontier,
    },
    /// `db`'s writer at `writer_epoch` closed or was fenced. This node no
    /// longer writes `db` unless a later frontier for it says otherwise; a
    /// replica that follows `db` should find its owner again.
    Closed { db: String, writer_epoch: u64 },
}

/// Every writer `directory` holds, as one stream.
///
/// A subscriber first hears the current frontier of each live writer, then
/// every change: new frontiers, writers installed later, and writers that
/// close. Within one database the frontiers only move forward, and a newer
/// writer's frontier replaces an older one's.
///
/// Nothing buffers. Each writer's frontier is a status watch, so a subscriber
/// that falls behind skips to the latest state rather than queueing every
/// step: what it needs is where the writer is, not where it has been.
pub fn node_feed(directory: Arc<dyn WriterDirectory>) -> BoxStream<'static, FeedEvent> {
    NodeFeed::new(directory).boxed()
}

enum Update {
    Frontier(Frontier),
    Ended,
}

/// One writer's updates, tagged with the database and writer they belong to.
type Tagged = BoxStream<'static, (String, u64, Update)>;

/// The writer currently followed for one database.
struct Followed {
    writer_epoch: u64,
    last: Option<Frontier>,
}

struct NodeFeed {
    directory: Arc<dyn WriterDirectory>,
    installs: Option<BoxStream<'static, ()>>,
    rescan: bool,
    writers: SelectAll<Tagged>,
    followed: HashMap<String, Followed>,
}

impl NodeFeed {
    fn new(directory: Arc<dyn WriterDirectory>) -> Self {
        let installs = Some(directory.installs());
        Self {
            directory,
            installs,
            rescan: true,
            writers: SelectAll::new(),
            followed: HashMap::new(),
        }
    }

    /// Start following every live writer not already followed.
    fn follow_new_writers(&mut self) {
        for LiveWriter {
            db,
            writer_epoch,
            frontiers,
        } in self.directory.live_writers()
        {
            if self.followed.get(&db).is_some_and(|followed| followed.writer_epoch >= writer_epoch) {
                continue;
            }
            self.followed.insert(db.clone(), Followed { writer_epoch, last: None });
            let ended = stream::iter([(db.clone(), writer_epoch, Update::Ended)]);
            let tagged = frontiers
                .map(move |frontier| (db.clone(), writer_epoch, Update::Frontier(frontier)))
                .chain(ended);
            self.writers.push(tagged.boxed());
        }
    }

    /// The event `update` makes, if any. Updates from a writer that a newer
    /// one has replaced, repeats and regressions make none.
    fn event(&mut self, db: String, writer_epoch: u64, update: Update) -> Option<FeedEvent> {
        let followed = self
            .followed
            .get_mut(&db)
            .filter(|followed| followed.writer_epoch == writer_epoch)?;
        match update {
            Update::Frontier(frontier) => {
                if followed.last.is_some_and(|last| !frontier.supersedes(&last)) {
                    return None;
                }
                followed.last = Some(frontier);
                Some(FeedEvent::Frontier { db, frontier })
            }
            Update::Ended => {
                self.followed.remove(&db);
                Some(FeedEvent::Closed { db, writer_epoch })
            }
        }
    }
}

impl Stream for NodeFeed {
    type Item = FeedEvent;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<FeedEvent>> {
        let this = self.get_mut();
        loop {
            // Drain install wake-ups first, so a new writer's first frontier is
            // not held behind a busy writer's stream.
            while let Some(installs) = this.installs.as_mut() {
                match installs.poll_next_unpin(cx) {
                    Poll::Ready(Some(())) => this.rescan = true,
                    Poll::Ready(None) => this.installs = None,
                    Poll::Pending => break,
                }
            }
            if std::mem::take(&mut this.rescan) {
                this.follow_new_writers();
            }
            match this.writers.poll_next_unpin(cx) {
                Poll::Ready(Some((db, writer_epoch, update))) => {
                    if let Some(event) = this.event(db, writer_epoch, update) {
                        return Poll::Ready(Some(event));
                    }
                }
                // No writer left and none can be installed: the node is done.
                Poll::Ready(None) if this.installs.is_none() => return Poll::Ready(None),
                // Either way `installs` has registered this task's waker.
                Poll::Ready(None) | Poll::Pending => return Poll::Pending,
            }
        }
    }
}
