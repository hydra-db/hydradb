//! The admin listener: `/readyz`, `/metrics`, `/debug/route`.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use http::Response;
use nix::sys::resource::{getrusage, UsageWho};
use nix::sys::time::TimeVal;
use pingora_core::apps::http_app::ServeHttp;
use pingora_core::protocols::http::ServerSession;
use prometheus::core::{Collector, Desc};
use prometheus::proto::MetricFamily;
use prometheus::{CounterVec, Opts, TEXT_FORMAT};
use turbolay_proxy_bolt::BoltProxy;
use turbolay_proxy_core::Lifecycle;

pub struct AdminApp {
    pub proxy: Arc<BoltProxy>,
    pub lifecycle: Lifecycle,
    /// Set once the replica source has reported. Until then this replica
    /// does not know its share of each node's budget.
    pub peers_known: Arc<AtomicBool>,
}

fn respond(status: u16, content_type: &str, body: Vec<u8>) -> Response<Vec<u8>> {
    Response::builder()
        .status(status)
        .header("content-type", content_type)
        .header("content-length", body.len())
        .body(body)
        .expect("valid response")
}

/// Process CPU time, user and system, read at every scrape, for the
/// benchmarks: a proxy that is bounded by the machine shows CPU seconds
/// rising at the core count.
pub struct ProcessCpu {
    opts: Opts,
    /// Never counted; it only describes the family for the registry.
    template: CounterVec,
}

impl Default for ProcessCpu {
    fn default() -> Self {
        let opts = Opts::new("turbolay_proxy_process_cpu_seconds_total", "Process CPU time, by mode.");
        Self {
            template: CounterVec::new(opts.clone(), &["mode"]).expect("valid metric"),
            opts,
        }
    }
}

impl Collector for ProcessCpu {
    fn desc(&self) -> Vec<&Desc> {
        self.template.desc()
    }

    fn collect(&self) -> Vec<MetricFamily> {
        let seconds = |time: TimeVal| time.tv_sec() as f64 + time.tv_usec() as f64 / 1e6;
        let (user, system) =
            getrusage(UsageWho::RUSAGE_SELF).map_or((0.0, 0.0), |usage| (seconds(usage.user_time()), seconds(usage.system_time())));
        let cpu = CounterVec::new(self.opts.clone(), &["mode"]).expect("valid metric");
        cpu.with_label_values(&["user"]).inc_by(user);
        cpu.with_label_values(&["system"]).inc_by(system);
        cpu.collect()
    }
}

fn query_param(query: &str, name: &str) -> Option<String> {
    form_urlencoded::parse(query.as_bytes())
        .find(|(key, _)| key == name)
        .map(|(_, value)| value.into_owned())
}

#[async_trait]
impl ServeHttp for AdminApp {
    async fn response(&self, session: &mut ServerSession) -> Response<Vec<u8>> {
        let uri = session.req_header().uri.clone();
        match uri.path() {
            "/readyz" | "/healthz" => {
                let ready = self.lifecycle.is_serving()
                    && self.peers_known.load(Ordering::Acquire)
                    && !self.proxy.router().current().nodes.is_empty();
                if ready {
                    respond(200, "text/plain", b"ready\n".to_vec())
                } else {
                    respond(503, "text/plain", b"not ready\n".to_vec())
                }
            }
            "/metrics" => respond(200, TEXT_FORMAT, self.proxy.router().render_metrics().into_bytes()),
            "/debug/route" => {
                let query = uri.query().unwrap_or("");
                let database = query_param(query, "database");
                let read = query_param(query, "access").as_deref() == Some("read");
                match self.proxy.debug_route(database.as_deref(), read).await {
                    Ok((scope, candidates)) => respond(
                        200,
                        "application/json",
                        serde_json::to_vec(&serde_json::json!({
                            "scope": scope,
                            "access": if read { "read" } else { "write" },
                            "policy": self.proxy.router().config().read_policy.as_str(),
                            "candidates": candidates,
                        }))
                        .expect("serialisable"),
                    ),
                    Err(error) => respond(
                        400,
                        "application/json",
                        serde_json::to_vec(&serde_json::json!({"error": error})).expect("serialisable"),
                    ),
                }
            }
            _ => respond(404, "text/plain", b"not found\n".to_vec()),
        }
    }
}
