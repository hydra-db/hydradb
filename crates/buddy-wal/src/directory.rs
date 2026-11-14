use futures::future::BoxFuture;
use futures::stream::BoxStream;
use serde::{Deserialize, Serialize};

use crate::Frontier;

/// An owner's answer to "is your writer still current?": its writer, proved
/// current by a check begun after the question arrived, and everything that
/// writer had acknowledged by then.
///
/// A strong read that waits until its reader has applied `durable_seq` sees
/// every write acknowledged before the question was asked, on any node.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Confirmed {
    pub writer_epoch: u64,
    pub durable_seq: u64,
}

/// One writer's frontier, in order, starting from its current one. It ends
/// when the writer closes or is fenced.
pub type FrontierStream = BoxStream<'static, Frontier>;

/// A writer this node holds.
pub struct LiveWriter {
    /// The database's object-store path. A replica's reader opens the same
    /// path, so this is the key a replica already has: the feed needs no
    /// notion of graphs, scopes or cells.
    pub db: String,
    /// Tells this writer apart from earlier and later ones on the same path.
    pub writer_epoch: u64,
    /// The writer's frontier.
    pub frontiers: FrontierStream,
}

/// The writers one node holds: what the graph side implements so the feed can
/// publish them.
pub trait WriterDirectory: Send + Sync + 'static {
    /// Every writer this node holds now.
    ///
    /// Must not open graphs or promote writers: a replica listening is never
    /// a reason to take ownership.
    fn live_writers(&self) -> Vec<LiveWriter>;

    /// Yields after a writer may have been installed on this node.
    ///
    /// The feed takes this before its first [`live_writers`] call, so a
    /// writer installed between the two is still found. Spurious wake-ups are
    /// harmless; a missed install is not. The stream ends only when the node
    /// can install no more writers.
    ///
    /// [`live_writers`]: WriterDirectory::live_writers
    fn installs(&self) -> BoxStream<'static, ()>;

    /// Prove `db`'s writer here is still current, with a check begun after
    /// this call, and return what it has acknowledged.
    ///
    /// `Ok(None)` when this node holds no current writer for `db`: none was
    /// ever installed, it closed, or the check found it superseded. Like
    /// [`live_writers`](WriterDirectory::live_writers), it never opens or
    /// promotes anything.
    fn confirm(&self, db: &str) -> BoxFuture<'static, Result<Option<Confirmed>, String>>;
}

/// A replica's way to put [`WriterDirectory::confirm`] to a peer.
pub trait FrontierConfirmer: Send + Sync + 'static {
    /// Ask `peer` (a node id) to confirm `db`'s writer. `None` on any
    /// failure: the peer is unreachable, slow, or no longer holds a current
    /// writer for `db`. The caller then reads object storage instead.
    fn confirm(&self, peer: &str, db: &str) -> BoxFuture<'static, Option<Confirmed>>;
}
