use serde::{Deserialize, Serialize};

/// What a graph's writer has made durable: everything a replica needs to know
/// to catch up, and nothing a replica could mistake for data.
///
/// Every field is already true in object storage when a frontier is
/// published, so acting on it can never expose a write that is not durable.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Frontier {
    /// The epoch of the writer that produced this state. A newer writer has a
    /// higher epoch, and its frontier supersedes every older writer's.
    pub writer_epoch: u64,
    /// The highest sequence number durable in the WAL.
    pub durable_seq: u64,
    /// The id of the last WAL file durably flushed.
    pub last_wal_id: u64,
    /// The id of the manifest the writer last observed.
    pub manifest_id: u64,
}

impl Frontier {
    /// Whether `self` carries news relative to `previous`.
    ///
    /// A higher writer epoch always does. Within one epoch every field only
    /// moves forward, so a frontier that moves any field back is stale and
    /// one that moves none forward is a repeat.
    pub fn supersedes(&self, previous: &Frontier) -> bool {
        if self.writer_epoch != previous.writer_epoch {
            return self.writer_epoch > previous.writer_epoch;
        }
        let forward = self.durable_seq >= previous.durable_seq
            && self.last_wal_id >= previous.last_wal_id
            && self.manifest_id >= previous.manifest_id;
        forward && self != previous
    }
}
