//! A replica's side of the feed: follow one peer's feed into a
//! [`FrontierBoard`], reconnecting for as long as the replica runs.

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use std::collections::BTreeMap;

use futures::future::BoxFuture;
use futures::FutureExt as _;

use crate::{BuddyWalMetrics, Confirmed, FeedEvent, FrontierBoard, FrontierConfirmer, CONFIRM_ROUTE, FEED_EVENT, FEED_ROUTE};

/// The feed's keep-alive is 10 s; three missed in a row means the connection
/// is dead even if TCP has not noticed.
const IDLE_TIMEOUT: Duration = Duration::from_secs(30);
const MIN_BACKOFF: Duration = Duration::from_millis(100);
const MAX_BACKOFF: Duration = Duration::from_secs(5);

/// The most one event may hold before it is complete. A full batch of 512
/// feed events is about 80 KB, so this is far above any real event, and
/// bounds what a faulty peer can make this node buffer.
const MAX_EVENT_BYTES: usize = 4 * 1024 * 1024;

/// How long a strong read waits for a peer to confirm its writer, request and
/// answer together. A strong read is on the request path, and reading object
/// storage instead is always correct, so a slow peer is not worth waiting on.
const CONFIRM_TIMEOUT: Duration = Duration::from_secs(1);

/// A peer node whose writers this replica follows.
#[derive(Clone, Debug)]
pub struct Peer {
    /// The peer's node id; names what it reported in the board.
    pub node_id: String,
    /// Where the peer serves its feed: scheme, host and port, such as
    /// `https://graph-node-1.graph-node-headless.prod.svc:8443`.
    pub base_url: String,
}

impl Peer {
    fn feed_url(&self) -> String {
        format!("{}{FEED_ROUTE}", self.base_url.trim_end_matches('/'))
    }
}

/// Confirms writers with the peers whose feeds report them, over the same
/// client (and so the same TLS trust) and token the feeds use.
pub struct PeerConfirmations {
    peers: BTreeMap<String, String>,
    token: Arc<str>,
    client: reqwest::Client,
}

impl PeerConfirmations {
    pub fn new(peers: impl IntoIterator<Item = Peer>, token: Arc<str>, client: reqwest::Client) -> Self {
        Self {
            peers: peers.into_iter().map(|peer| (peer.node_id, peer.base_url)).collect(),
            token,
            client,
        }
    }
}

impl FrontierConfirmer for PeerConfirmations {
    fn confirm(&self, peer: &str, db: &str) -> BoxFuture<'static, Option<Confirmed>> {
        let Some(base_url) = self.peers.get(peer) else {
            return futures::future::ready(None).boxed();
        };
        let request = self
            .client
            .get(format!("{}{CONFIRM_ROUTE}", base_url.trim_end_matches('/')))
            .query(&[("db", db)])
            .bearer_auth(&*self.token);
        let (peer, db) = (peer.to_string(), db.to_string());
        async move {
            let answer = async {
                let response = request.send().await.ok()?;
                if !response.status().is_success() {
                    tracing::debug!(%peer, %db, status = %response.status(), "buddy WAL peer declined to confirm its writer");
                    return None;
                }
                response.json::<Confirmed>().await.ok()
            };
            tokio::time::timeout(CONFIRM_TIMEOUT, answer).await.ok().flatten()
        }
        .boxed()
    }
}

/// Why one subscription ended.
#[derive(Debug, thiserror::Error)]
enum SessionEnd {
    #[error("feed request failed: {0}")]
    Request(#[from] reqwest::Error),
    #[error("feed refused with {0}")]
    Refused(reqwest::StatusCode),
    #[error("no bytes for {0:?}")]
    Idle(Duration),
    #[error("feed ended")]
    Ended,
    #[error("malformed feed event: {0}")]
    Malformed(String),
    #[error("a feed event exceeded {MAX_EVENT_BYTES} bytes")]
    Oversized,
}

impl SessionEnd {
    /// The `reason` label of `graph_buddy_wal_peer_sessions_ended_total`.
    fn reason(&self) -> &'static str {
        match self {
            Self::Request(_) => "request_failed",
            Self::Refused(_) => "refused",
            Self::Idle(_) => "idle",
            Self::Ended => "ended",
            Self::Malformed(_) => "malformed",
            Self::Oversized => "oversized",
        }
    }
}

/// Follow `peer`'s feed into `board` until `stopping` resolves.
///
/// Whenever a subscription ends, for any reason, the board stops vouching for
/// what this peer reported, so readers fall back to object storage until the
/// next subscription catches up. Reconnects back off from 100 ms to 5 s, and
/// reset once a subscription is accepted.
pub async fn follow_peer(
    peer: Peer,
    token: Arc<str>,
    board: Arc<FrontierBoard>,
    client: reqwest::Client,
    metrics: Arc<BuddyWalMetrics>,
    stopping: impl Future<Output = ()>,
) {
    let mut stopping = std::pin::pin!(stopping);
    let mut backoff = MIN_BACKOFF;
    metrics.peer_connected(&peer.node_id, false);
    loop {
        let mut accepted = false;
        let end = tokio::select! {
            () = &mut stopping => {
                board.forget(&peer.node_id);
                metrics.peer_connected(&peer.node_id, false);
                return;
            }
            end = subscribe(&peer, &token, &board, &client, &metrics, &mut accepted) => end,
        };
        board.forget(&peer.node_id);
        metrics.peer_session_ended(&peer.node_id, end.reason());
        if accepted {
            backoff = MIN_BACKOFF;
        }
        tracing::debug!(peer = %peer.node_id, error = %end, ?backoff, "buddy WAL feed subscription ended; reconnecting");
        tokio::select! {
            () = &mut stopping => return,
            () = tokio::time::sleep(backoff) => {}
        }
        backoff = (backoff * 2).min(MAX_BACKOFF);
    }
}

/// One subscription, applied to `board` until it ends.
async fn subscribe(
    peer: &Peer,
    token: &str,
    board: &FrontierBoard,
    client: &reqwest::Client,
    metrics: &BuddyWalMetrics,
    accepted: &mut bool,
) -> SessionEnd {
    // Bounded like the stream itself, so a peer that accepts the connection
    // and stalls before answering is retried rather than waited on forever;
    // only the wait for the response is bounded, not the stream's lifetime.
    let request = client.get(peer.feed_url()).bearer_auth(token).send();
    let mut response = match tokio::time::timeout(IDLE_TIMEOUT, request).await {
        Err(_) => return SessionEnd::Idle(IDLE_TIMEOUT),
        Ok(Ok(response)) if response.status().is_success() => response,
        Ok(Ok(response)) => return SessionEnd::Refused(response.status()),
        Ok(Err(error)) => return error.into(),
    };
    *accepted = true;
    metrics.peer_connected(&peer.node_id, true);
    let mut decoder = SseDecoder::default();
    loop {
        let chunk = match tokio::time::timeout(IDLE_TIMEOUT, response.chunk()).await {
            Err(_) => return SessionEnd::Idle(IDLE_TIMEOUT),
            Ok(Err(error)) => return error.into(),
            Ok(Ok(None)) => return SessionEnd::Ended,
            Ok(Ok(Some(chunk))) => chunk,
        };
        let Ok(events) = decoder.push(&chunk) else {
            return SessionEnd::Oversized;
        };
        for event in events {
            if event.name != FEED_EVENT {
                continue;
            }
            let events: Vec<FeedEvent> = match serde_json::from_str(&event.data) {
                Ok(events) => events,
                Err(error) => return SessionEnd::Malformed(error.to_string()),
            };
            metrics.frontier_events(events.len() as u64);
            for event in events {
                board.apply(&peer.node_id, event);
            }
        }
    }
}

/// One server-sent event.
#[derive(Debug, PartialEq, Eq)]
struct SseEvent {
    name: String,
    data: String,
}

/// Splits a server-sent-events byte stream into events, whatever the chunk
/// boundaries. Comments (the keep-alive) and fields other than `event` and
/// `data` are ignored. An unfinished event may hold at most `limit` bytes.
struct SseDecoder {
    pending: Vec<u8>,
    limit: usize,
}

/// An event grew past the decoder's limit without finishing.
#[derive(Debug, PartialEq, Eq)]
struct Oversized;

impl Default for SseDecoder {
    fn default() -> Self {
        Self::with_limit(MAX_EVENT_BYTES)
    }
}

impl SseDecoder {
    fn with_limit(limit: usize) -> Self {
        Self {
            pending: Vec::new(),
            limit,
        }
    }

    fn push(&mut self, chunk: &[u8]) -> Result<Vec<SseEvent>, Oversized> {
        self.pending.extend(chunk.iter().filter(|byte| **byte != b'\r'));
        let mut events = Vec::new();
        while let Some(end) = self.pending.windows(2).position(|window| window == b"\n\n") {
            // A finished event is held to the same limit as an unfinished
            // one, so one sent whole in a chunk cannot slip past it.
            if end > self.limit {
                return Err(Oversized);
            }
            let block: Vec<u8> = self.pending.drain(..end + 2).collect();
            if let Some(event) = Self::event(&String::from_utf8_lossy(&block)) {
                events.push(event);
            }
        }
        if self.pending.len() > self.limit {
            return Err(Oversized);
        }
        Ok(events)
    }

    fn event(block: &str) -> Option<SseEvent> {
        let mut name = String::from("message");
        let mut data: Option<String> = None;
        for line in block.lines() {
            let (field, value) = line.split_once(':').unwrap_or((line, ""));
            let value = value.strip_prefix(' ').unwrap_or(value);
            match field {
                "event" => name = value.to_string(),
                "data" => match &mut data {
                    Some(data) => {
                        data.push('\n');
                        data.push_str(value);
                    }
                    None => data = Some(value.to_string()),
                },
                _ => {}
            }
        }
        Some(SseEvent { name, data: data? })
    }
}
