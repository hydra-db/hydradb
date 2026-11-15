//! The turbolay proxy's HTTP adapter.
//!
//! A pingora [`ProxyHttp`]: the request head names the scope (graph id from
//! the path, namespace from `x-graph-namespace`) and the access mode
//! (`x-graph-access`, missing means write). The core routes and admits, and
//! pingora streams the request and the NDJSON response through untouched,
//! reusing upstream connections.
//!
//! A 421 naming an owner in `x-graph-owner` is retried at that owner before
//! any byte goes downstream, from pingora's retry buffer. pingora skips
//! retries for POST by default, and every turbolay query is a POST, so
//! [`ProxyHttp::error_while_proxy`] and [`ProxyHttp::fail_to_connect`] are
//! overridden to ask the core's retry policy instead.

#![forbid(unsafe_code)]

use std::net::SocketAddr;
use std::sync::{Arc, LazyLock};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use hydradb_protocol::http::{parse_access, HttpAccess, HttpErrorBody, HttpErrorEnvelope, GRAPH_ACCESS_HEADER, GRAPH_OWNER_HEADER};
use hydradb_scope::http::{http_graph_scope, HttpScopeError, GRAPH_NAMESPACE_HEADER};
use moka::sync::Cache;
use pingora_core::upstreams::peer::HttpPeer;
use pingora_error::{Error, ErrorType, Result};
use pingora_http::ResponseHeader;
use pingora_proxy::{ProxyHttp, Session};
use turbolay_proxy_core::{Permit, Placement, RendezvousPlacement, RetryClass, RouteOutcome, Router};
use turbolay_proxy_types::{Access, NodeId, Operation, Outcome, Protocol, RetryDecision, RouteKey};

/// HTTP adapter settings.
#[derive(Clone, Debug)]
pub struct HttpConfig {
    /// The cell every request is routed by. The proxy routes on the request
    /// head alone, so it never reads the `cell_id` in the body.
    pub cell_id: String,
    /// Accept a missing `x-graph-namespace` as the default namespace, as
    /// graph-node does when its own scope is the default one.
    pub allow_default_namespace: bool,
    pub connect_timeout: Duration,
    /// How long an idle upstream connection is kept for reuse.
    pub upstream_idle_timeout: Duration,
    /// Speak HTTP/2 to the nodes (h2c on plaintext).
    pub upstream_h2: bool,
    /// Concurrent streams per upstream h2 connection before pingora opens
    /// another.
    pub max_h2_streams: usize,
}

impl Default for HttpConfig {
    fn default() -> Self {
        Self {
            cell_id: "cell-0".into(),
            allow_default_namespace: false,
            connect_timeout: Duration::from_secs(3),
            upstream_idle_timeout: Duration::from_secs(60),
            upstream_h2: true,
            max_h2_streams: 64,
        }
    }
}

/// The adapter.
pub struct HttpProxy<P: Placement = RendezvousPlacement> {
    router: Arc<Router<P>>,
    config: HttpConfig,
    resolved: ResolvedAddresses,
}

impl<P: Placement> HttpProxy<P> {
    pub fn new(router: Arc<Router<P>>, config: HttpConfig) -> Self {
        Self {
            router,
            config,
            resolved: ResolvedAddresses::default(),
        }
    }
}

/// How long a resolved node address is reused before it is looked up again.
const RESOLVED_TTL: Duration = Duration::from_secs(30);

/// Node addresses already resolved to a socket address.
///
/// In k8s the node addresses are DNS names, and resolving per request would
/// put a blocking lookup on every request. Keyed by node, with the spelling that
/// was resolved, so an address change in the snapshot is never answered from
/// a stale entry.
struct ResolvedAddresses {
    table: Cache<NodeId, (String, SocketAddr)>,
}

impl Default for ResolvedAddresses {
    fn default() -> Self {
        Self {
            table: Cache::builder().time_to_live(RESOLVED_TTL).build(),
        }
    }
}

impl ResolvedAddresses {
    fn get(&self, node: &NodeId, address: &str) -> Option<SocketAddr> {
        self.table
            .get(node)
            .filter(|(spelling, _)| spelling == address)
            .map(|(_, socket)| socket)
    }

    fn insert(&self, node: &NodeId, address: &str, socket: SocketAddr) {
        self.table.insert(node.clone(), (address.to_string(), socket));
    }
}

/// Per request state.
#[derive(Default)]
pub struct RequestCtx {
    key: Option<RouteKey>,
    operation: Option<Operation>,
    candidates: Vec<NodeId>,
    candidate: usize,
    target: Option<NodeId>,
    attempt: u32,
    permit: Option<Permit>,
    /// The outcome of the attempt in progress, set by the filters.
    outcome: Option<Outcome>,
    /// Where the next attempt goes, decided when the last one failed.
    next: Option<NodeId>,
    counted: bool,
    /// What the response headers said, recorded once the body is through:
    /// a stream that breaks after the headers is not a success.
    answer: Option<RouteOutcome>,
    cancel: bool,
}

/// What the path names.
enum Target<'a> {
    Query { graph_id: &'a str },
    Cancel { graph_id: &'a str },
    Other,
}

#[derive(Clone, Copy)]
enum Endpoint {
    Query,
    Cancel,
}

/// The routes the proxy routes; anything else goes to any node.
static ENDPOINTS: LazyLock<matchit::Router<Endpoint>> = LazyLock::new(|| {
    let mut endpoints = matchit::Router::new();
    endpoints
        .insert("/v1/graphs/{graph_id}/query", Endpoint::Query)
        .expect("valid route");
    endpoints
        .insert("/v1/graphs/{graph_id}/queries/{query_id}/cancel", Endpoint::Cancel)
        .expect("valid route");
    endpoints
});

fn parse_path(path: &str) -> Target<'_> {
    let Ok(matched) = ENDPOINTS.at(path) else {
        return Target::Other;
    };
    let graph_id = matched.params.get("graph_id").expect("both routes name the graph");
    match matched.value {
        Endpoint::Query => Target::Query { graph_id },
        Endpoint::Cancel => Target::Cancel { graph_id },
    }
}

fn error_body(code: &str, message: String) -> Bytes {
    let envelope = HttpErrorEnvelope {
        error: HttpErrorBody {
            code: code.into(),
            message,
            owner: None,
        },
    };
    Bytes::from(serde_json::to_vec(&envelope).expect("serialisable"))
}

impl<P: Placement> HttpProxy<P> {
    async fn reject(&self, session: &mut Session, status: u16, code: &str, message: String) -> Result<bool> {
        let body = error_body(code, message);
        let mut header = ResponseHeader::build(status, Some(3))?;
        header.insert_header("content-type", "application/json")?;
        header.insert_header("content-length", body.len().to_string())?;
        header.insert_header("cache-control", "no-store")?;
        session.write_response_header(Box::new(header), false).await?;
        session.write_response_body(Some(body), true).await?;
        Ok(true)
    }

    fn record(&self, ctx: &RequestCtx, outcome: RouteOutcome) {
        if let (Some(target), Some(operation)) = (&ctx.target, ctx.operation) {
            self.router.nodes().record(target, operation.protocol, operation.access, outcome);
        }
    }

    /// Ask the retry policy about the attempt that just failed, and pick the
    /// next target. True when it is retried.
    fn decide(&self, ctx: &mut RequestCtx, outcome: Outcome) -> bool {
        let Some(operation) = ctx.operation else { return false };
        let owner = match (&outcome, &ctx.key, &ctx.target) {
            (Outcome::RefusedNotOwner { owner }, Some(key), Some(target)) => {
                self.router.learn_refusal(key, target, owner.as_ref().map(NodeId::as_str))
            }
            _ => None,
        };
        let decision = self.router.classify(operation, &outcome, ctx.attempt);
        let next = match decision {
            // A hint naming a node outside the snapshot cannot be reached;
            // the 421 goes to the client as received.
            RetryDecision::RetryAt(_) => {
                if owner.is_some() {
                    self.router.metrics().retry(RetryClass::NotOwner);
                }
                owner
            }
            RetryDecision::RetrySame => {
                self.router.metrics().retry(RetryClass::ConnectFailed);
                ctx.target.clone()
            }
            RetryDecision::RetryNext if ctx.candidate + 1 < ctx.candidates.len() => {
                self.router.metrics().retry(RetryClass::ConnectFailed);
                ctx.candidate += 1;
                Some(ctx.candidates[ctx.candidate].clone())
            }
            RetryDecision::RetryNext | RetryDecision::GiveUp => None,
        };
        ctx.outcome = Some(outcome);
        ctx.next = next;
        ctx.next.is_some()
    }
}

#[async_trait]
impl<P: Placement> ProxyHttp for HttpProxy<P> {
    type CTX = RequestCtx;

    fn new_ctx(&self) -> RequestCtx {
        RequestCtx::default()
    }

    async fn request_filter(&self, session: &mut Session, ctx: &mut RequestCtx) -> Result<bool> {
        let header = session.req_header();
        let (graph_id, cancel) = match parse_path(header.uri.path()) {
            Target::Query { graph_id } => (graph_id, false),
            Target::Cancel { graph_id } => (graph_id, true),
            Target::Other => return Ok(false),
        };
        // Decoded once, as graph-node's path extractor does, so the proxy
        // accepts exactly the graph names graph-node accepts.
        let Ok(graph_id) = percent_encoding::percent_decode_str(graph_id).decode_utf8() else {
            return self
                .reject(session, 400, "invalid_request", "graph id is not valid UTF-8".into())
                .await;
        };
        let graph_id = graph_id.into_owned();
        let header = session.req_header();
        let namespace = header.headers.get(GRAPH_NAMESPACE_HEADER).and_then(|value| value.to_str().ok());
        let key = http_graph_scope(namespace, &graph_id, self.config.allow_default_namespace)
            .and_then(|scope| Ok(RouteKey::new(&scope, &self.config.cell_id)?));
        let key = match key {
            Ok(key) => key,
            Err(HttpScopeError::MissingNamespace) => {
                return self
                    .reject(
                        session,
                        400,
                        "missing_namespace",
                        format!("{GRAPH_NAMESPACE_HEADER} header is required"),
                    )
                    .await
            }
            Err(HttpScopeError::Invalid(error)) => return self.reject(session, 400, "invalid_request", error.to_string()).await,
        };
        let header = session.req_header();
        // A cancel goes to the scope's owner, where the cursor lives. If
        // ownership moved since the query started, the cancel misses it.
        let access = match (
            cancel,
            parse_access(header.headers.get(GRAPH_ACCESS_HEADER).and_then(|v| v.to_str().ok())),
        ) {
            (true, _) | (false, HttpAccess::Write) => Access::Write,
            (false, HttpAccess::Read) => Access::Read,
        };
        let operation = Operation {
            access,
            protocol: Protocol::Http,
        };
        // Only a write can be refused with a 421 after its body went out, so
        // only a write's body is buffered for replay. A connect failure is
        // retried before any body is sent, which needs no buffer.
        if access == Access::Write {
            session.enable_retry_buffering();
        }
        self.router.metrics().unit(operation.protocol, operation.access);
        match self.router.route(&key, operation).await {
            Ok(route) => ctx.candidates = route.candidates,
            Err(_) => {
                return self
                    .reject(session, 503, "routing_unavailable", "no graph node is configured".into())
                    .await
            }
        }
        ctx.key = Some(key);
        ctx.operation = Some(operation);
        ctx.cancel = cancel;
        Ok(false)
    }

    async fn upstream_peer(&self, _session: &mut Session, ctx: &mut RequestCtx) -> Result<Box<HttpPeer>> {
        // Target and address under a short guard: no reference taken on the
        // snapshot, nothing held across the awaits below.
        let (target, address) = {
            let snapshot = self.router.snapshot();
            let target = match ctx.next.take() {
                Some(next) => next,
                None if ctx.attempt == 0 && !ctx.candidates.is_empty() => ctx.candidates[0].clone(),
                None if ctx.attempt == 0 => match snapshot.nodes.first() {
                    // A path the proxy does not route: any node answers it.
                    Some(node) => node.id.clone(),
                    None => return Err(Error::explain(ErrorType::ConnectNoRoute, "no graph node is configured")),
                },
                None => return Err(Error::explain(ErrorType::ConnectNoRoute, "no retry target")),
            };
            let Some((_, node)) = snapshot.node(target.as_str()) else {
                return Err(Error::explain(ErrorType::ConnectNoRoute, format!("node {target} left the fleet")));
            };
            let Some(address) = node.http_addr.as_deref() else {
                return Err(Error::explain(
                    ErrorType::ConnectNoRoute,
                    format!("node {target} has no HTTP address"),
                ));
            };
            // The resolved address, if we have a fresh one for this exact
            // spelling; otherwise the spelling, to resolve below.
            let resolved = self.resolved.get(&target, address);
            (target, resolved.ok_or_else(|| address.to_string()))
        };
        ctx.attempt += 1;
        ctx.outcome = None;
        // A retry holds at most one permit: the old one goes before the new.
        ctx.permit = None;
        // A cancel takes no permit: the query it stops may hold the last one.
        if ctx.operation.is_some() && !ctx.cancel {
            match self.router.admit(&target).await {
                Ok(permit) => ctx.permit = Some(permit),
                Err(_) => {
                    ctx.target = Some(target);
                    self.record(ctx, RouteOutcome::AdmitTimeout);
                    let mut error = Error::explain(ErrorType::HTTPStatus(503), "graph node is at its concurrency budget");
                    error.set_retry(false);
                    return Err(error);
                }
            }
        }
        let socket = match address {
            Ok(socket) => socket,
            Err(address) => {
                let socket = match address.parse::<std::net::SocketAddr>() {
                    Ok(socket) => socket,
                    Err(_) => tokio::net::lookup_host(address.as_str())
                        .await
                        .ok()
                        .and_then(|mut addresses| addresses.next())
                        .ok_or_else(|| Error::explain(ErrorType::ConnectNoRoute, format!("cannot resolve {address}")))?,
                };
                self.resolved.insert(&target, &address, socket);
                socket
            }
        };
        ctx.target = Some(target);
        let mut peer = HttpPeer::new(socket, false, String::new());
        peer.options.connection_timeout = Some(self.config.connect_timeout);
        peer.options.idle_timeout = Some(self.config.upstream_idle_timeout);
        if self.config.upstream_h2 {
            // h2c with prior knowledge: requests multiplex over a few
            // connections, so there is no per-request pool checkout, and none
            // of the fd and liveness syscalls HTTP/1 reuse makes each time.
            peer.options.set_http_version(2, 2);
            peer.options.max_h2_streams = self.config.max_h2_streams;
        }
        Ok(Box::new(peer))
    }

    async fn upstream_response_filter(&self, session: &mut Session, response: &mut ResponseHeader, ctx: &mut RequestCtx) -> Result<()> {
        if response.status.as_u16() == 421 {
            let owner = response
                .headers
                .get(GRAPH_OWNER_HEADER)
                .and_then(|value| value.to_str().ok())
                .map(NodeId::new);
            self.record(ctx, RouteOutcome::RefusedNotOwner);
            // A body too large for the retry buffer cannot be replayed, so
            // the client gets the 421 as the node sent it.
            let replayable = !session.as_ref().retry_buffer_truncated();
            if self.decide(ctx, Outcome::RefusedNotOwner { owner }) && replayable {
                // Nothing has gone downstream yet: replay at the owner.
                let mut error = Error::explain(ErrorType::HTTPStatus(421), "refused by a non-owner, retrying at the owner");
                error.set_retry(true);
                return Err(error);
            }
            ctx.counted = true;
            return Ok(());
        }
        ctx.counted = true;
        ctx.answer = Some(if response.status.is_server_error() {
            RouteOutcome::NodeError
        } else {
            RouteOutcome::Ok
        });
        Ok(())
    }

    async fn logging(&self, _session: &mut Session, error: Option<&Error>, ctx: &mut RequestCtx) {
        if let Some(answer) = ctx.answer.take() {
            self.record(ctx, if error.is_some() { RouteOutcome::LostAfterPull } else { answer });
        }
    }

    fn fail_to_connect(&self, session: &mut Session, _peer: &HttpPeer, ctx: &mut RequestCtx, mut error: Box<Error>) -> Box<Error> {
        self.record(ctx, RouteOutcome::ConnectFailed);
        let retry = !session.as_ref().retry_buffer_truncated() && self.decide(ctx, Outcome::ConnectFailed);
        error.set_retry(retry);
        error
    }

    fn error_while_proxy(
        &self,
        peer: &HttpPeer,
        session: &mut Session,
        error: Box<Error>,
        ctx: &mut RequestCtx,
        _client_reused: bool,
    ) -> Box<Error> {
        let mut error = error.more_context(format!("Peer: {peer}"));
        // A refusal already decided to retry in the response filter.
        if matches!(ctx.outcome, Some(Outcome::RefusedNotOwner { .. })) && ctx.next.is_some() {
            error.set_retry(!session.as_ref().retry_buffer_truncated());
            return error;
        }
        // The request went out and the answer did not come back: an unknown
        // outcome for a write, which the retry policy gives up on.
        if !ctx.counted {
            self.record(ctx, RouteOutcome::LostAfterPull);
        }
        let retry = self.decide(ctx, Outcome::LostAfterPull);
        error.set_retry(retry);
        error
    }
}
