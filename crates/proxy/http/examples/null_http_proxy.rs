//! The null HTTP proxy: pingora with none of turbolay's logic.
//!
//! Requests go to a fixed list of upstreams in turn, over h2c, exactly as
//! `turbolay-proxy` talks to a node, but with no scope parsing, routing,
//! admission, owner cache or retry policy. It is the yardstick for "what HTTP
//! proxying costs on this machine": the gap between it and `turbolay-proxy` is
//! what the turbolay logic costs.
//!
//! `NULL_LISTEN` (default 127.0.0.1:37030), `NULL_TARGETS` (comma list),
//! `NULL_THREADS`, `NULL_MAX_H2_STREAMS`.
//!
//! `cargo run --release -p turbolay-proxy-http --example null_http_proxy`

use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};

use async_trait::async_trait;
use pingora_core::server::configuration::ServerConf;
use pingora_core::server::Server;
use pingora_core::upstreams::peer::HttpPeer;
use pingora_error::Result;
use pingora_proxy::{ProxyHttp, Session};

#[global_allocator]
static ALLOCATOR: mimalloc::MiMalloc = mimalloc::MiMalloc;

struct NullProxy {
    /// Upstreams, taken in turn, so the fan-out matches turbolay-proxy
    /// spreading fleet reads over the same nodes.
    targets: Vec<SocketAddr>,
    next: AtomicUsize,
    max_h2_streams: usize,
}

#[async_trait]
impl ProxyHttp for NullProxy {
    type CTX = ();

    fn new_ctx(&self) {}

    async fn upstream_peer(&self, _session: &mut Session, _ctx: &mut ()) -> Result<Box<HttpPeer>> {
        let target = self.targets[self.next.fetch_add(1, Ordering::Relaxed) % self.targets.len()];
        let mut peer = HttpPeer::new(target, false, String::new());
        // The same peer options turbolay-proxy sets, so the only difference
        // left between the two is the turbolay logic.
        peer.options.connection_timeout = Some(std::time::Duration::from_secs(3));
        peer.options.idle_timeout = Some(std::time::Duration::from_secs(60));
        peer.options.set_http_version(2, 2);
        peer.options.max_h2_streams = self.max_h2_streams;
        Ok(Box::new(peer))
    }
}

fn env(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.to_string())
}

fn main() {
    let conf = ServerConf {
        threads: env("NULL_THREADS", "4").parse().expect("NULL_THREADS"),
        work_stealing: true,
        // A short shutdown, so a benchmark run ends promptly instead of
        // waiting out pingora's default grace.
        grace_period_seconds: Some(1),
        graceful_shutdown_timeout_seconds: Some(1),
        ..ServerConf::default()
    };
    let mut server = Server::new_with_opt_and_conf(None, conf);
    server.bootstrap();
    let targets = env("NULL_TARGETS", "127.0.0.1:37002")
        .split(',')
        .map(|target| target.parse().expect("NULL_TARGETS"))
        .collect();
    let max_h2_streams = env("NULL_MAX_H2_STREAMS", "64").parse().expect("NULL_MAX_H2_STREAMS");
    let proxy = NullProxy {
        targets,
        next: AtomicUsize::new(0),
        max_h2_streams,
    };
    let mut service = pingora_proxy::http_proxy_service(&server.configuration, proxy);
    service.add_tcp(&env("NULL_LISTEN", "127.0.0.1:37030"));
    server.add_service(service);
    server.run_forever();
}
