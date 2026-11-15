//! The buddy WAL feed for this node's writers (`GRAPH_BUDDY_WAL`).
//!
//! The glue between `hydradb-buddy-wal`, which owns the feed, its route and
//! its client, and the scoped cluster, which owns the writers and readers.
//! Neither knows the other; this file is the only place they meet.
//!
//! - Owner side: [`feed_routes`] publishes this node's writers on the public
//!   HTTP listener, so the feed rides the node's TLS like client queries do.
//! - Replica side: [`PeerLinks`] follows every peer's feed into one board,
//!   which the node's stores read as their `WalFeed`, and asks peers to
//!   confirm their writers for strong reads.

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;

use hydradb::ScopedRoutedGraphCluster;
use hydradb_buddy_wal::client::{follow_peer, Peer, PeerConfirmations};
use hydradb_buddy_wal::server::{feed_router, Authorizer};
use hydradb_buddy_wal::{BuddyWalMetrics, FrontierBoard};
use tokio::sync::watch;
use tokio::task::JoinHandle;

use crate::listeners::ListenersStopping;

/// The feed route for the public HTTP listener, authorized by the graph auth
/// token as client queries are. Open feeds end when the listeners stop.
pub fn feed_routes(
    node: &ScopedRoutedGraphCluster,
    token: &str,
    metrics: Arc<BuddyWalMetrics>,
) -> impl FnOnce(ListenersStopping) -> axum::Router {
    let directory = Arc::new(node.writer_directory());
    let token: Arc<str> = Arc::from(token);
    let authorize: Authorizer = Arc::new(move |headers| crate::admin::valid_control_bearer(headers, &token));
    move |stopping| feed_router(directory, authorize, metrics, stopping)
}

/// How followers reach their peers' HTTP listeners.
pub enum PeerTransport<'a> {
    /// TLS, verifying each peer's certificate against this CA (PEM).
    Tls { ca_certificate: &'a Path },
    /// Plaintext, for a fleet that serves its clients without TLS.
    Plaintext,
}

impl PeerTransport<'_> {
    fn scheme(&self) -> &'static str {
        match self {
            Self::Tls { .. } => "https",
            Self::Plaintext => "http",
        }
    }

    fn client(&self) -> std::io::Result<reqwest::Client> {
        let builder = reqwest::Client::builder();
        let builder = match self {
            Self::Tls { ca_certificate } => {
                let pem = std::fs::read(ca_certificate).map_err(|error| {
                    std::io::Error::new(
                        error.kind(),
                        format!("cannot read GRAPH_TLS_CA_CERTIFICATE at {}: {error}", ca_certificate.display()),
                    )
                })?;
                let ca = reqwest::Certificate::from_pem(&pem).map_err(|error| {
                    std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!(
                            "GRAPH_TLS_CA_CERTIFICATE at {} is not a PEM certificate: {error}",
                            ca_certificate.display()
                        ),
                    )
                })?;
                builder.tls_certs_only([ca])
            }
            Self::Plaintext => builder,
        };
        builder.build().map_err(std::io::Error::other)
    }
}

/// This node's replica side, started: a `WalFeed` for its stores, and the
/// followers feeding it.
///
/// `None`, logged as an error, when the peers cannot be followed (the CA
/// certificate is missing or unreadable). Buddy WAL only accelerates reads,
/// so a node that cannot follow its peers still starts and serves every read
/// from object storage, exactly as with the flag off; it also still serves
/// its own feed. A misconfigured optional accelerator must not take a node
/// down.
pub fn start_replica(
    peers: &BTreeMap<String, String>,
    token: &str,
    transport: &PeerTransport<'_>,
    metrics: Arc<BuddyWalMetrics>,
) -> Option<(hydradb::WalFeed, PeerFollowers)> {
    match PeerLinks::new(peers, token, transport, Arc::clone(&metrics)) {
        Ok(links) => {
            let board = Arc::new(FrontierBoard::new());
            let followers = links.follow(Arc::clone(&board));
            let feed = hydradb::WalFeed::new(board)
                .with_confirmer(Arc::new(links.confirmations()))
                .with_metrics(metrics);
            Some((feed, followers))
        }
        Err(error) => {
            tracing::error!(
                %error,
                "buddy WAL cannot follow peers; this node serves its own feed but its reads keep polling object storage"
            );
            None
        }
    }
}

/// This node's replica side: one task per peer, all reporting into one board.
pub struct PeerFollowers {
    stop: watch::Sender<bool>,
    tasks: Vec<JoinHandle<()>>,
}

impl PeerFollowers {
    /// Stop following. Readers fall back to polling object storage.
    pub async fn stop(self) {
        self.stop.send_replace(true);
        for task in self.tasks {
            if let Err(error) = task.await {
                tracing::warn!(%error, "buddy WAL peer follower task failed");
            }
        }
    }
}

/// This node's connections to its peers: one client (and so one TLS trust)
/// and one token, used both to follow their feeds and to ask them to confirm
/// a writer for a strong read.
pub struct PeerLinks {
    peers: Vec<Peer>,
    token: Arc<str>,
    client: reqwest::Client,
    metrics: Arc<BuddyWalMetrics>,
}

impl PeerLinks {
    /// Links to every peer in `peers` (node id to the `host:port` of its HTTP
    /// listener), presenting the graph auth token over `transport`.
    pub fn new(
        peers: &BTreeMap<String, String>,
        token: &str,
        transport: &PeerTransport<'_>,
        metrics: Arc<BuddyWalMetrics>,
    ) -> std::io::Result<Self> {
        Ok(Self {
            peers: peers
                .iter()
                .map(|(node_id, address)| Peer {
                    node_id: node_id.clone(),
                    base_url: format!("{}://{address}", transport.scheme()),
                })
                .collect(),
            token: Arc::from(token),
            client: transport.client()?,
            metrics,
        })
    }

    /// Follow every peer's feed into `board`, which the node's stores read as
    /// their `WalFeed`.
    pub fn follow(&self, board: Arc<FrontierBoard>) -> PeerFollowers {
        let (stop, stopped) = watch::channel(false);
        let tasks = self
            .peers
            .iter()
            .map(|peer| {
                let mut stopped = stopped.clone();
                let stopping = async move {
                    let _ = stopped.wait_for(|stop| *stop).await;
                };
                tokio::spawn(follow_peer(
                    peer.clone(),
                    Arc::clone(&self.token),
                    Arc::clone(&board),
                    self.client.clone(),
                    Arc::clone(&self.metrics),
                    stopping,
                ))
            })
            .collect();
        PeerFollowers { stop, tasks }
    }

    /// Ask peers to confirm their writers, for strong reads on this node.
    pub fn confirmations(&self) -> PeerConfirmations {
        PeerConfirmations::new(self.peers.iter().cloned(), Arc::clone(&self.token), self.client.clone())
    }
}
