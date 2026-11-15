use std::path::{Path, PathBuf};
use std::time::Duration;

use tokio::sync::watch;
use turbolay_proxy_types::NodeInfo;

/// Which backend nodes exist. Runs until the proxy stops, sending every change.
#[async_trait::async_trait]
pub trait MembershipSource: Send + 'static {
    async fn run(self: Box<Self>, nodes: watch::Sender<Vec<NodeInfo>>);
}

/// Which proxy replicas are ready, as their advertised Bolt addresses.
#[async_trait::async_trait]
pub trait ProxySetSource: Send + 'static {
    async fn run(self: Box<Self>, proxies: watch::Sender<Vec<String>>);
}

/// Membership from config. Sends once and then holds the channel open.
pub struct StaticMembership {
    nodes: Vec<NodeInfo>,
}

impl StaticMembership {
    pub fn new(nodes: Vec<NodeInfo>) -> Self {
        Self { nodes }
    }
}

#[async_trait::async_trait]
impl MembershipSource for StaticMembership {
    async fn run(self: Box<Self>, nodes: watch::Sender<Vec<NodeInfo>>) {
        nodes.send_replace(self.nodes);
        nodes.closed().await;
    }
}

/// A fixed replica set from config.
pub struct StaticProxySet {
    proxies: Vec<String>,
}

impl StaticProxySet {
    pub fn new(proxies: Vec<String>) -> Self {
        Self { proxies }
    }
}

#[async_trait::async_trait]
impl ProxySetSource for StaticProxySet {
    async fn run(self: Box<Self>, proxies: watch::Sender<Vec<String>>) {
        proxies.send_replace(self.proxies);
        proxies.closed().await;
    }
}

/// The replica set from a file, one `host:port` per line, `#` comments,
/// re-read every `interval`. Local runs and tests rewrite the file to add and
/// remove replicas, which is what EndpointSlice events do in k8s. A missing or
/// unreadable file keeps the last good set rather than emptying it.
pub struct FileProxySet {
    path: PathBuf,
    interval: Duration,
}

impl FileProxySet {
    pub fn new(path: impl AsRef<Path>, interval: Duration) -> Self {
        Self {
            path: path.as_ref().to_path_buf(),
            interval,
        }
    }
}

/// Parse the replica file's contents.
pub fn parse_proxy_file(contents: &str) -> Vec<String> {
    contents
        .lines()
        .map(|line| line.split('#').next().unwrap_or("").trim())
        .filter(|line| !line.is_empty())
        .map(str::to_string)
        .collect()
}

#[async_trait::async_trait]
impl ProxySetSource for FileProxySet {
    async fn run(self: Box<Self>, proxies: watch::Sender<Vec<String>>) {
        let mut ticker = tokio::time::interval(self.interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = ticker.tick() => {}
                _ = proxies.closed() => return,
            }
            match tokio::fs::read_to_string(&self.path).await {
                // A file rewritten in place reads empty between truncate and
                // write. An empty set would hand every replica the whole
                // budget, so it is never published.
                Ok(contents) if parse_proxy_file(&contents).is_empty() => {
                    tracing::warn!(path = %self.path.display(), "proxy set file is empty, keeping the last set")
                }
                Ok(contents) => {
                    let mut next = parse_proxy_file(&contents);
                    next.sort();
                    proxies.send_if_modified(|current| {
                        let changed = *current != next;
                        if changed {
                            *current = next;
                        }
                        changed
                    });
                }
                Err(error) => tracing::warn!(path = %self.path.display(), %error, "proxy set file unreadable, keeping the last set"),
            }
        }
    }
}
