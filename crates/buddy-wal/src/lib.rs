//! Buddy WAL, phase 1: the owner tells replicas what is durable.
//!
//! A replica graph-node reads a graph through a SlateDB `DbReader`, which only
//! learns of new writes by refreshing from object storage. Owner routing
//! avoided that by sending every read to the writer's node; fleet routing paid
//! for it with a refresh poll. This crate is the third way: the node holding a
//! graph's writer publishes that writer's durable [`Frontier`], and replicas
//! follow it instead of polling.
//!
//! Only durable state is ever published. Object storage stays the source of
//! truth and the only durability path; the feed is an accelerator a replica
//! may lose at any time and recover from storage.
//!
//! One stream per node, not per graph: a node's feed carries every writer it
//! holds, keyed by database path, so a replica needs one connection per peer
//! however many graphs it follows.
//!
//! The crate owns the vocabulary and the transport, nothing else:
//! - [`Frontier`] and [`FeedEvent`], the wire types;
//! - [`WriterDirectory`], what the graph side implements to list its writers;
//! - [`node_feed`], one node's writers multiplexed into one stream;
//! - [`server::feed_router`] (feature `server`), the owner's route;
//! - [`FrontierBoard`], what a replica knows of other nodes' writers;
//! - [`client::follow_peer`] (feature `client`), a replica following one peer.

mod board;
mod directory;
mod feed;
mod frontier;
mod metrics;

#[cfg(feature = "client")]
pub mod client;
#[cfg(feature = "server")]
pub mod server;

pub use board::{Followed, FrontierBoard};
pub use directory::{Confirmed, FrontierConfirmer, FrontierStream, LiveWriter, WriterDirectory};
pub use feed::{node_feed, FeedEvent};
pub use frontier::Frontier;
pub use metrics::{BuddyWalMetrics, CatchUp, ConfirmServed, FeedSubscription, FollowedWait, StrongRead};

/// Where a node serves its feed, on its admin listener.
pub const FEED_ROUTE: &str = "/v1/buddy-wal/feed";

/// The SSE event name. Each event's data is a JSON array of [`FeedEvent`]s.
pub const FEED_EVENT: &str = "feed";

/// Where a node confirms its writer for a database (`?db=<path>`), answering
/// with a [`Confirmed`] or 409 `not_owner`.
pub const CONFIRM_ROUTE: &str = "/v1/buddy-wal/confirm";
