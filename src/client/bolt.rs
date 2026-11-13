use std::collections::{BTreeMap, VecDeque};
use std::future::Future;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use boltr::error::BoltError;
use boltr::message::{ClientMessage, ServerMessage};
use boltr::types::{BoltDict, BoltValue};
use hydradb_bolt_server::{
    run_session, ConnectionGate, ConnectionPermit, Credentials, Interrupted, Outcome, SessionHandler, SessionIo, SessionLimits,
};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;
use tokio::task::{JoinHandle, JoinSet};
use tokio::time::Instant;
use tokio_rustls::TlsAcceptor;
use ulid::Ulid;

use tracing::Instrument;

use super::service::{
    sanitize_caller_metadata, ClientBookmark, ClientDatabaseResolver, ClientQueryCredentials, ClientQueryPage, ClientQueryRequest,
    ClientQueryService, ClientQuerySession, ClientQueryTarget, ClientReadConsistency, PreparedClientQuery,
};
use crate::query::opencypher::{opencypher_query_fingerprint, opencypher_query_shape_for_log};
use crate::QueryFailureReason;
use crate::{
    GqlStatusReport, GraphError, GraphScope, QueryColumn, QueryCursorToken, QueryRow, QueryTransportAction,
    QueryTransportConnectionIdentity, QueryTransportTlsServerConfigProvider, Result,
};

mod routing;
mod values;
use routing::validate_bolt_routing_table;
pub use routing::{BoltReadRouting, BoltRoutingServer, BoltRoutingTable, BoltRoutingTableProvider, ObjectStoreBoltRoutingTableProvider};
use values::{
    bolt_parameter_to_query_value, explicit_transactions_unsupported, graph_error_to_bolt, highest_matching_bookmark, query_value_to_bolt,
};

const DEFAULT_BOLT_MAX_CONNECTIONS: usize = 1024;
const DEFAULT_BOLT_GRACEFUL_SHUTDOWN: Duration = Duration::from_secs(30);
const DEFAULT_BOLT_ROUTING_TTL_SECS: i64 = 30;
const DEFAULT_BOLT_PREFETCH_ROWS: usize = 256;
const MAX_BOLT_MUTATION_IDEMPOTENCY_KEY_LEN: usize = 128;

static NEXT_BOLT_SESSION_ID: AtomicU64 = AtomicU64::new(1);

trait BoltIo: AsyncRead + AsyncWrite + Send + Unpin {}
impl<T> BoltIo for T where T: AsyncRead + AsyncWrite + Send + Unpin {}

#[derive(Clone)]
pub struct BoltServerConfig {
    pub database_resolver: Arc<dyn ClientDatabaseResolver>,
    pub default_database: String,
    pub routing_ttl_secs: i64,
    pub routing_servers: Vec<BoltRoutingServer>,
    pub routing_table_provider: Option<Arc<dyn BoltRoutingTableProvider>>,
    pub max_message_bytes: usize,
    pub max_connections: usize,
    pub prefetch_rows: usize,
    pub max_pipelined_messages: usize,
    pub handshake_timeout: Duration,
    pub authentication_timeout: Duration,
    pub idle_timeout: Duration,
    pub max_connection_age: Duration,
    pub write_timeout: Duration,
    pub graceful_shutdown_timeout: Duration,
    pub allow_plaintext: bool,
    pub tls_config_provider: Option<Arc<dyn QueryTransportTlsServerConfigProvider>>,
}

impl BoltServerConfig {
    pub fn new(database_resolver: Arc<dyn ClientDatabaseResolver>) -> Self {
        let limits = SessionLimits::default();
        Self {
            database_resolver,
            default_database: "default".to_string(),
            routing_ttl_secs: DEFAULT_BOLT_ROUTING_TTL_SECS,
            routing_servers: Vec::new(),
            routing_table_provider: None,
            max_message_bytes: limits.max_message_bytes,
            max_connections: DEFAULT_BOLT_MAX_CONNECTIONS,
            prefetch_rows: DEFAULT_BOLT_PREFETCH_ROWS,
            max_pipelined_messages: limits.max_pipelined_messages,
            handshake_timeout: limits.handshake_timeout,
            authentication_timeout: limits.authentication_timeout,
            idle_timeout: limits.idle_timeout,
            max_connection_age: limits.max_connection_age,
            write_timeout: limits.write_timeout,
            graceful_shutdown_timeout: DEFAULT_BOLT_GRACEFUL_SHUTDOWN,
            allow_plaintext: false,
            tls_config_provider: None,
        }
    }

    pub fn with_default_database(mut self, database: impl Into<String>) -> Self {
        self.default_database = database.into();
        self
    }

    pub fn with_routing_ttl_secs(mut self, ttl_secs: i64) -> Self {
        self.routing_ttl_secs = ttl_secs;
        self
    }

    pub fn with_routing_server(mut self, server: BoltRoutingServer) -> Self {
        self.routing_servers.push(server);
        self
    }

    pub fn with_routing_table_provider(mut self, provider: Arc<dyn BoltRoutingTableProvider>) -> Self {
        self.routing_table_provider = Some(provider);
        self
    }

    pub fn with_max_message_bytes(mut self, max_message_bytes: usize) -> Self {
        self.max_message_bytes = max_message_bytes;
        self
    }

    pub fn with_max_connections(mut self, max_connections: usize) -> Self {
        self.max_connections = max_connections;
        self
    }

    pub fn with_prefetch_rows(mut self, prefetch_rows: usize) -> Self {
        self.prefetch_rows = prefetch_rows;
        self
    }

    pub fn with_max_pipelined_messages(mut self, max_pipelined_messages: usize) -> Self {
        self.max_pipelined_messages = max_pipelined_messages;
        self
    }

    pub fn with_handshake_timeout(mut self, timeout: Duration) -> Self {
        self.handshake_timeout = timeout;
        self
    }

    pub fn with_authentication_timeout(mut self, timeout: Duration) -> Self {
        self.authentication_timeout = timeout;
        self
    }

    pub fn with_idle_timeout(mut self, timeout: Duration) -> Self {
        self.idle_timeout = timeout;
        self
    }

    pub fn with_max_connection_age(mut self, max_connection_age: Duration) -> Self {
        self.max_connection_age = max_connection_age;
        self
    }

    pub fn with_write_timeout(mut self, timeout: Duration) -> Self {
        self.write_timeout = timeout;
        self
    }

    pub fn with_graceful_shutdown_timeout(mut self, timeout: Duration) -> Self {
        self.graceful_shutdown_timeout = timeout;
        self
    }

    pub fn with_tls_provider(mut self, provider: Arc<dyn QueryTransportTlsServerConfigProvider>) -> Self {
        self.tls_config_provider = Some(provider);
        self
    }

    pub fn insecure_allow_plaintext(mut self) -> Self {
        self.allow_plaintext = true;
        self
    }

    fn validate(&self) -> Result<()> {
        if self.default_database.trim().is_empty() {
            return bolt_config_error("default database cannot be empty");
        }
        if self.routing_ttl_secs <= 0 {
            return bolt_config_error("routing TTL must be greater than zero");
        }
        if !self.routing_servers.is_empty() {
            validate_bolt_routing_table(self.routing_ttl_secs, &self.routing_servers)?;
        }
        if self.max_message_bytes == 0 || self.max_connections == 0 || self.prefetch_rows == 0 || self.max_pipelined_messages == 0 {
            return bolt_config_error("message, connection, prefetch, and pipeline limits must be greater than zero");
        }
        if let Err(error) = self.session_limits().validate() {
            return bolt_config_error(&error.to_string());
        }
        if self.graceful_shutdown_timeout.is_zero() {
            return bolt_config_error("Bolt connection timeouts must be greater than zero");
        }
        if self.tls_config_provider.is_none() && !self.allow_plaintext {
            return bolt_config_error("Bolt requires TLS unless plaintext is explicitly enabled for development");
        }
        self.database_resolver.resolve_database(Some(&self.default_database))?;
        Ok(())
    }

    fn session_limits(&self) -> SessionLimits {
        SessionLimits {
            max_message_bytes: self.max_message_bytes,
            max_pipelined_messages: self.max_pipelined_messages,
            handshake_timeout: self.handshake_timeout,
            authentication_timeout: self.authentication_timeout,
            idle_timeout: self.idle_timeout,
            max_connection_age: self.max_connection_age,
            write_timeout: self.write_timeout,
        }
    }
}

fn bolt_config_error<T>(reason: &str) -> Result<T> {
    Err(GraphError::UnsupportedQuery {
        reason: QueryFailureReason::InvalidRequest,
        dialect: "Bolt",
        feature: reason.to_string(),
    })
}

#[derive(Default)]
struct BoltServerMetrics {
    accepted_connections: AtomicU64,
    active_connections: AtomicU64,
    rejected_connections: AtomicU64,
    handshake_failures: AtomicU64,
}

pub struct BoltServerHandle {
    local_addr: SocketAddr,
    stop_tx: watch::Sender<bool>,
    task: Option<JoinHandle<Result<()>>>,
    metrics: Arc<BoltServerMetrics>,
}

impl BoltServerHandle {
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    pub fn accepted_connections(&self) -> u64 {
        self.metrics.accepted_connections.load(Ordering::Relaxed)
    }

    pub fn active_connections(&self) -> u64 {
        self.metrics.active_connections.load(Ordering::Relaxed)
    }

    pub async fn stop(mut self) -> Result<()> {
        let _ = self.stop_tx.send(true);
        self.task
            .take()
            .expect("Bolt server task exists until stop")
            .await
            .map_err(|err| GraphError::CorruptValue {
                key: "client/bolt/server".to_string(),
                reason: err.to_string(),
            })?
    }
}

impl Drop for BoltServerHandle {
    fn drop(&mut self) {
        let _ = self.stop_tx.send(true);
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

pub struct ClientBoltServer;

impl ClientBoltServer {
    pub async fn bind(addr: SocketAddr, service: ClientQueryService, config: BoltServerConfig) -> Result<BoltServerHandle> {
        config.validate()?;
        let listener = TcpListener::bind(addr).await.map_err(|err| bolt_io_error("bind", err))?;
        let local_addr = listener.local_addr().map_err(|err| bolt_io_error("local_addr", err))?;
        let config = Arc::new(config);
        let metrics = Arc::new(BoltServerMetrics::default());
        let connection_gate = ConnectionGate::new(config.max_connections);
        let (stop_tx, stop_rx) = watch::channel(false);
        let task = tokio::spawn(run_bolt_server(
            listener,
            service,
            Arc::clone(&config),
            Arc::clone(&metrics),
            connection_gate,
            stop_rx,
        ));
        Ok(BoltServerHandle {
            local_addr,
            stop_tx,
            task: Some(task),
            metrics,
        })
    }
}

async fn run_bolt_server(
    listener: TcpListener,
    service: ClientQueryService,
    config: Arc<BoltServerConfig>,
    metrics: Arc<BoltServerMetrics>,
    connection_gate: ConnectionGate,
    mut stop_rx: watch::Receiver<bool>,
) -> Result<()> {
    let mut connections = JoinSet::new();
    loop {
        tokio::select! {
            biased;
            changed = stop_rx.changed() => {
                if changed.is_err() || *stop_rx.borrow() {
                    break;
                }
            }
            accepted = listener.accept() => {
                let (stream, peer_addr) = match accepted {
                    Ok(accepted) => accepted,
                    Err(err) => {
                        tracing::warn!(target: "hydradb", error = %err, "Bolt accept failed");
                        continue;
                    }
                };
                let permit = match connection_gate.try_acquire() {
                    Some(permit) => permit,
                    None => {
                        metrics.rejected_connections.fetch_add(1, Ordering::Relaxed);
                        continue;
                    }
                };
                metrics.accepted_connections.fetch_add(1, Ordering::Relaxed);
                connections.spawn(serve_bolt_connection(
                    stream,
                    peer_addr,
                    service.clone(),
                    Arc::clone(&config),
                    Arc::clone(&metrics),
                    permit,
                    Instant::now() + config.authentication_timeout,
                ));
            }
            completed = connections.join_next(), if !connections.is_empty() => {
                if let Some(Err(err)) = completed {
                    tracing::warn!(target: "hydradb", error = %err, "Bolt connection task failed");
                }
            }
        }
    }
    let graceful = async { while connections.join_next().await.is_some() {} };
    if tokio::time::timeout(config.graceful_shutdown_timeout, graceful).await.is_err() {
        connections.abort_all();
        while connections.join_next().await.is_some() {}
    }
    Ok(())
}

async fn serve_bolt_connection(
    stream: TcpStream,
    peer_addr: SocketAddr,
    service: ClientQueryService,
    config: Arc<BoltServerConfig>,
    metrics: Arc<BoltServerMetrics>,
    _permit: ConnectionPermit,
    authentication_deadline: Instant,
) {
    metrics.active_connections.fetch_add(1, Ordering::Relaxed);
    let result = serve_bolt_connection_inner(stream, peer_addr, service, config, authentication_deadline).await;
    metrics.active_connections.fetch_sub(1, Ordering::Relaxed);
    if let Err(err) = result {
        if matches!(err, BoltError::Protocol(_)) {
            metrics.handshake_failures.fetch_add(1, Ordering::Relaxed);
        }
        tracing::debug!(target: "hydradb", %peer_addr, error = %err, "Bolt connection closed");
    }
}

async fn serve_bolt_connection_inner(
    stream: TcpStream,
    peer_addr: SocketAddr,
    service: ClientQueryService,
    config: Arc<BoltServerConfig>,
    authentication_deadline: Instant,
) -> std::result::Result<(), BoltError> {
    stream.set_nodelay(true)?;
    let (mut io, identity): (Box<dyn BoltIo>, QueryTransportConnectionIdentity) = match &config.tls_config_provider {
        Some(provider) => {
            let server_config = provider
                .current_server_config()
                .map_err(|err| BoltError::Backend(err.to_string()))?;
            let tls_stream = tokio::time::timeout_at(
                std::cmp::min(Instant::now() + config.handshake_timeout, authentication_deadline),
                TlsAcceptor::from(server_config).accept(stream),
            )
            .await
            .map_err(|_| BoltError::Protocol("TLS handshake timed out".to_string()))?
            .map_err(|err| BoltError::Io(std::io::Error::other(err)))?;
            let identity = bolt_tls_identity(&tls_stream);
            (Box::new(tls_stream), identity)
        }
        None => (Box::new(stream), QueryTransportConnectionIdentity::default()),
    };
    hydradb_bolt_server::handshake(&mut *io, config.handshake_timeout, authentication_deadline).await?;

    let context = Arc::new(BoltConnectionContext::new(
        service,
        Arc::clone(&config.database_resolver),
        config.default_database.clone(),
        config.routing_ttl_secs,
        config.routing_servers.clone(),
        config.routing_table_provider.clone(),
        identity,
    ));
    let handler = BoltConnectionHandler {
        context,
        session: BoltProtocolSession::new(),
        prefetch_rows: config.prefetch_rows,
    };
    run_session(io, peer_addr, handler, config.session_limits(), authentication_deadline).await
}

fn bolt_tls_identity<S>(stream: &tokio_rustls::server::TlsStream<S>) -> QueryTransportConnectionIdentity {
    let mut identity = QueryTransportConnectionIdentity::default();
    let (_, connection) = stream.get_ref();
    if let Some(certificates) = connection.peer_certificates() {
        for (index, certificate) in certificates.iter().enumerate() {
            let fingerprint = format!("sha256:{}", lowercase_hex(&Sha256::digest(certificate.as_ref())));
            if index == 0 {
                identity.tls_peer_leaf_fingerprint = Some(fingerprint.clone());
            }
            identity.tls_peer_fingerprints.insert(fingerprint);
        }
    }
    identity
}

fn lowercase_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(HEX[(byte >> 4) as usize] as char);
        output.push(HEX[(byte & 0x0f) as usize] as char);
    }
    output
}

struct BoltConnectionContext {
    service: ClientQueryService,
    database_resolver: Arc<dyn ClientDatabaseResolver>,
    default_database: String,
    routing_ttl_secs: i64,
    routing_servers: Vec<BoltRoutingServer>,
    routing_table_provider: Option<Arc<dyn BoltRoutingTableProvider>>,
    identity: QueryTransportConnectionIdentity,
}

impl BoltConnectionContext {
    fn new(
        service: ClientQueryService,
        database_resolver: Arc<dyn ClientDatabaseResolver>,
        default_database: String,
        routing_ttl_secs: i64,
        routing_servers: Vec<BoltRoutingServer>,
        routing_table_provider: Option<Arc<dyn BoltRoutingTableProvider>>,
        identity: QueryTransportConnectionIdentity,
    ) -> Self {
        Self {
            service,
            database_resolver,
            default_database,
            routing_ttl_secs,
            routing_servers,
            routing_table_provider,
            identity,
        }
    }
}

struct BoltProtocolSession {
    id: String,
    authenticated: Option<ClientQuerySession>,
    database: Option<String>,
    next_query: u64,
    pending: Option<PendingBoltResult>,
}

impl BoltProtocolSession {
    fn new() -> Self {
        Self {
            id: format!("bolt-{}", NEXT_BOLT_SESSION_ID.fetch_add(1, Ordering::Relaxed)),
            authenticated: None,
            database: None,
            next_query: 1,
            pending: None,
        }
    }

    fn next_query_id(&mut self) -> String {
        let query_id = format!("{}-query-{}", self.id, self.next_query);
        self.next_query = self.next_query.saturating_add(1);
        query_id
    }

    fn authenticated(&self) -> std::result::Result<ClientQuerySession, BoltError> {
        self.authenticated
            .clone()
            .ok_or_else(|| BoltError::Authentication("session is not authenticated".to_string()))
    }
}

struct PendingBoltResult {
    database: String,
    prepared: PreparedClientQuery,
    deadline: Instant,
    columns: Vec<QueryColumn>,
    started: bool,
    /// How many pages have been fetched from the service for this result. Only
    /// telemetry reads it — it is what orders the `query.page` spans of one
    /// lazily paging client.
    pages: u64,
    /// The `client.query` / `client.mutate` root, opened at RUN and held for
    /// the whole statement.
    ///
    /// It lives here rather than being opened per PULL because a lazily paging
    /// client would otherwise produce one trace per page, each holding a single
    /// `query.page` and no way to see the statement as a whole. Held as a
    /// handle and used as an explicit `parent:`, never entered across an
    /// await — the pages are fetched from a `select!` that also services
    /// pipelined messages, and an entered guard would leak the span onto
    /// whatever else that loop touches.
    statement_span: tracing::Span,
    rows: VecDeque<QueryRow>,
    next_cursor: Option<QueryCursorToken>,
    bookmark: Option<ClientBookmark>,
    /// The outcome's GQL-status objects, from the first page that carries
    /// them; sent as `statuses` in the final SUCCESS.
    statuses: Vec<GqlStatusReport>,
}

type PageAwaitResult = Interrupted<Result<Box<ClientQueryPage>>>;

#[derive(Clone, Copy)]
enum BoltRecordDisposition {
    Send,
    Discard,
}

/// graph-node's half of one Bolt connection: authentication against the
/// query service, and RUN, PULL, DISCARD and ROUTE.
struct BoltConnectionHandler {
    context: Arc<BoltConnectionContext>,
    session: BoltProtocolSession,
    prefetch_rows: usize,
}

impl SessionHandler for BoltConnectionHandler {
    fn server_agent(&self) -> String {
        hydradb_protocol::bolt::server_agent(env!("CARGO_PKG_VERSION"))
    }

    fn connection_id(&self) -> String {
        self.session.id.clone()
    }

    fn is_authenticated(&self) -> bool {
        self.session.authenticated.is_some()
    }

    fn logon(&mut self, credentials: Credentials) -> bool {
        match self
            .context
            .service
            .authenticate(&bolt_client_credentials(credentials), &self.context.identity)
        {
            Ok(authenticated) => {
                self.session.authenticated = Some(authenticated);
                true
            }
            Err(_) => false,
        }
    }

    fn logoff(&mut self) {
        self.session.authenticated = None;
        self.session.database = None;
    }

    async fn clear_pending_result(&mut self) {
        clear_pending_result(&self.context, &mut self.session).await;
    }

    async fn handle<W>(&mut self, message: ClientMessage, io: &mut SessionIo<W>) -> std::result::Result<Outcome, BoltError>
    where
        W: AsyncWrite + Unpin + Send,
    {
        match message {
            ClientMessage::Run { query, parameters, extra } => self.run(query, parameters, extra, io).await,
            ClientMessage::Pull { extra } => self.consume(&extra, "PULL", BoltRecordDisposition::Send, io).await,
            ClientMessage::Discard { extra } => self.consume(&extra, "DISCARD", BoltRecordDisposition::Discard, io).await,
            ClientMessage::Route { bookmarks, extra, .. } => self.route(bookmarks, extra, io).await,
            // BEGIN, COMMIT and ROLLBACK: the session hands over nothing else.
            // Production clients reach graph-node through the turbolay proxy,
            // which serves a single-statement transaction as an auto-commit
            // RUN, so no transaction arrives here from it.
            _ => {
                io.failure(&explicit_transactions_unsupported()).await?;
                Ok(Outcome::Failed)
            }
        }
    }
}

impl BoltConnectionHandler {
    async fn run<W>(
        &mut self,
        query: String,
        parameters: BoltDict,
        extra: BoltDict,
        io: &mut SessionIo<W>,
    ) -> std::result::Result<Outcome, BoltError>
    where
        W: AsyncWrite + Unpin + Send,
    {
        let session = &mut self.session;
        let context = &self.context;
        // Read before `extra` is consumed below. A malformed
        // `tx_metadata` is reported by `prepare_bolt_run` a line later
        // with the right error; there is nothing useful to say about it
        // twice.
        let inbound_traceparent = bolt_traceparent(&extra).ok().flatten();
        let query_fingerprint = opencypher_query_fingerprint(&query);
        // Kept for the preparation-failure arm below, which names the
        // statement it could not run; `query` itself moves into the
        // request. Taken as the shape rather than as a copy of the
        // text: this is held across preparation, and a RUN may carry a
        // statement up to the 2 MiB message limit on each of the
        // connections the listener allows, whereas the shape is capped
        // at 4 KiB.
        let query_shape = opencypher_query_shape_for_log(&query);
        // Opened before anything can reject the request, and from the
        // `db` name alone, so that a RUN which never becomes a
        // statement is still attributed to the tenant that sent it.
        let run_span = bolt_run_span(session, context, &extra);
        run_span.record("hydradb.query.fingerprint", query_fingerprint.as_str());
        crate::core::trace_context::adopt_remote_parent(&run_span, inbound_traceparent.as_deref());
        let (authenticated, database, request) = match run_span.in_scope(|| prepare_bolt_run(session, context, query, parameters, extra)) {
            Ok(prepared) => prepared,
            Err(error) => {
                run_span.in_scope(|| {
                    tracing::warn!(target: "hydradb", error = %error, "Bolt RUN rejected");
                });
                io.failure(&error).await?;
                return Ok(Outcome::Failed);
            }
        };
        let prepared = match context
            .service
            .prepare_page_request(&authenticated, request, self.prefetch_rows, false)
            .instrument(run_span.clone())
            .await
        {
            Ok(prepared) => prepared,
            Err(error) => {
                run_span.record("error.class", error.class());
                run_span.in_scope(|| {
                    // A query-class failure (unsupported, parse,
                    // missing parameter) is about the statement, so the
                    // line names it: the literal-free shape plus the
                    // same closed reason the failure counter uses.
                    // Everything else -- timeouts, freshness, storage
                    // -- is not, and keeps the short line.
                    match error.failure_reason() {
                        Some(reason) => tracing::warn!(
                            target: "hydradb",
                            error = %error,
                            failure_reason = reason.as_str(),
                            cypher_engine = context.service.cypher_engine().as_str(),
                            query_shape = %query_shape,
                            "Bolt RUN preparation failed"
                        ),
                        None => tracing::warn!(
                            target: "hydradb",
                            error = %error,
                            "Bolt RUN preparation failed"
                        ),
                    }
                });
                send_bolt_graph_failure(io, error).await?;
                return Ok(Outcome::Failed);
            }
        };
        let query_deadline = Instant::now()
            + Duration::from_millis(
                prepared
                    .request
                    .max_runtime_ms
                    .expect("Bolt requests are normalized before execution"),
            );
        let fields = prepared
            .columns
            .iter()
            .map(|column| BoltValue::String(column.name.clone()))
            .collect();
        io.success(BoltDict::from([
            ("fields".to_string(), BoltValue::List(fields)),
            ("t_first".to_string(), BoltValue::Integer(0)),
            ("db".to_string(), BoltValue::String(database.clone())),
        ]))
        .await?;
        // Opened here, at RUN, and not at the execution boundary the
        // PULLs land on: one statement is one trace, however many
        // pages the client chooses to pull.
        let statement_span = crate::client::service::client_root_span(&prepared.request, Some(prepared.action));
        // Join the caller's trace if they sent one, before the span is
        // ever entered. This is the ingestion-to-HydraDB edge; the
        // node-to-node edge is the transport's own `traceparent` field.
        crate::core::trace_context::adopt_remote_parent(&statement_span, inbound_traceparent.as_deref());
        session.pending = Some(PendingBoltResult {
            database,
            columns: prepared.columns.clone(),
            prepared,
            deadline: query_deadline,
            started: false,
            pages: 0,
            statement_span,
            rows: VecDeque::new(),
            next_cursor: None,
            bookmark: None,
            statuses: Vec::new(),
        });
        Ok(Outcome::Streaming)
    }

    /// PULL or DISCARD, by `disposition`.
    async fn consume<W>(
        &mut self,
        extra: &BoltDict,
        operation: &'static str,
        disposition: BoltRecordDisposition,
        io: &mut SessionIo<W>,
    ) -> std::result::Result<Outcome, BoltError>
    where
        W: AsyncWrite + Unpin + Send,
    {
        let session = &mut self.session;
        let context = &self.context;
        let count = match bolt_stream_count(extra, operation) {
            Ok(count) => count,
            Err(error) => {
                io.failure(&error).await?;
                return Ok(Outcome::Failed);
            }
        };
        let authenticated = match session.authenticated() {
            Ok(authenticated) => authenticated,
            Err(error) => {
                io.failure(&error).await?;
                return Ok(Outcome::Failed);
            }
        };
        let Some(pending) = session.pending.as_mut() else {
            let error = BoltError::Protocol("no pending result".to_string());
            io.failure(&error).await?;
            return Ok(Outcome::Failed);
        };
        let result = consume_bolt_records(context, &authenticated, pending, count, self.prefetch_rows, disposition, io).await;
        match result {
            Err(error) => {
                io.failure(&error).await?;
                clear_pending_result(context, session).await;
                Ok(Outcome::Failed)
            }
            Ok(PageAwaitResult::Complete(Ok(_))) => {
                let pending = session.pending.as_ref().expect("PULL and DISCARD keep pending state until summary");
                let has_more = !pending.rows.is_empty() || pending.next_cursor.is_some();
                io.success(bolt_result_summary_metadata(pending, has_more)).await?;
                if has_more {
                    return Ok(Outcome::Streaming);
                }
                clear_pending_result(context, session).await;
                Ok(Outcome::Ready)
            }
            Ok(PageAwaitResult::Complete(Err(error))) => {
                send_bolt_graph_failure(io, error).await?;
                clear_pending_result(context, session).await;
                Ok(Outcome::Failed)
            }
            Ok(PageAwaitResult::Reset) => {
                clear_pending_result(context, session).await;
                Ok(Outcome::Ready)
            }
            Ok(PageAwaitResult::Goodbye) => Ok(Outcome::Close),
        }
    }

    async fn route<W>(&mut self, bookmarks: Vec<String>, extra: BoltDict, io: &mut SessionIo<W>) -> std::result::Result<Outcome, BoltError>
    where
        W: AsyncWrite + Unpin + Send,
    {
        let route_span = bolt_route_span(&self.session, &self.context, &extra);
        match prepare_bolt_route(&self.session, &self.context, bookmarks, &extra)
            .instrument(route_span.clone())
            .await
        {
            Ok(routing_table) => {
                io.success(BoltDict::from([("rt".to_string(), BoltValue::Dict(routing_table))]))
                    .await?;
                Ok(Outcome::Ready)
            }
            Err(error) => {
                route_span.in_scope(|| {
                    tracing::warn!(target: "hydradb", error = %error, "Bolt ROUTE rejected");
                });
                io.failure(&error).await?;
                Ok(Outcome::Failed)
            }
        }
    }
}

async fn clear_pending_result(context: &BoltConnectionContext, session: &mut BoltProtocolSession) {
    let Some(pending) = session.pending.take() else {
        return;
    };
    let (Some(authenticated), Some(cursor)) = (session.authenticated.as_ref(), pending.next_cursor) else {
        return;
    };
    context
        .service
        .release_server_cursor(authenticated, &pending.prepared.request, cursor)
        .await;
}

fn bolt_client_credentials(credentials: Credentials) -> ClientQueryCredentials {
    match credentials {
        Credentials::None => ClientQueryCredentials::None,
        Credentials::Bearer(token) => ClientQueryCredentials::Bearer(token),
        Credentials::Basic { principal, credentials } => ClientQueryCredentials::Basic { principal, credentials },
    }
}

fn selected_bolt_database(
    session: &BoltProtocolSession,
    context: &BoltConnectionContext,
    extra: &BoltDict,
) -> std::result::Result<String, BoltError> {
    match extra.get("db") {
        Some(BoltValue::String(database)) => Ok(database.clone()),
        Some(_) => Err(BoltError::Protocol("db metadata must be a string".to_string())),
        None => Ok(session.database.clone().unwrap_or_else(|| context.default_database.clone())),
    }
}

/// The fields every Bolt ingress span carries before its request is understood.
///
/// A macro rather than a function because `tracing` builds each span's metadata
/// into a `static`, so the name has to be a literal at the call site — the same
/// constraint `client_root_span` works around with `root_span!`.
macro_rules! bolt_ingress_span {
    ($name:literal) => {
        tracing::info_span!(
            target: "hydradb",
            $name,
            hydradb.scope = tracing::field::Empty,
            hydradb.cell_id = tracing::field::Empty,
            hydradb.tenant_id = tracing::field::Empty,
            hydradb.tenant.scope_id = tracing::field::Empty,
            hydradb.sub_tenant_id = tracing::field::Empty,
            hydradb.sub_tenant.scope_id = tracing::field::Empty,
            hydradb.query.fingerprint = tracing::field::Empty,
            error.class = tracing::field::Empty,
        )
    };
}

/// Fill an ingress span from the database the message names.
///
/// The database is resolved here a second time — the preparation that follows
/// resolves the same one — and every failure is swallowed. This span exists to
/// attribute a rejection to a tenant; it must never *be* the rejection, and the
/// real error arrives a line later with its own message. A malformed `db`
/// simply leaves the fields absent, which is the honest answer: there is no
/// tenant to name.
fn record_bolt_ingress_scope(span: &tracing::Span, session: &BoltProtocolSession, context: &BoltConnectionContext, extra: &BoltDict) {
    let Ok(database) = selected_bolt_database(session, context, extra) else {
        return;
    };
    let Ok(target) = context.database_resolver.resolve_database(Some(&database)) else {
        return;
    };
    span.record("hydradb.scope", tracing::field::display(&target.scope));
    span.record("hydradb.cell_id", target.cell_id.as_str());
    crate::client::service::record_scope_tenancy(span, &target.scope);
}

/// The span a RUN is prepared under.
///
/// `client.query` cannot cover this: it needs a parsed request, and the two
/// steps that produce one — [`prepare_bolt_run`] and
/// `ClientQueryService::prepare_page_request` — are also the two that reject a
/// query for an unknown database, a denied scope or a syntax error. Those
/// rejections are exactly the lines an operator wants attributed to a customer,
/// and before this span they carried no tenancy and, for every error class Bolt
/// translates by hand, were not logged at all.
///
/// It is deliberately short: it closes as soon as the statement is prepared,
/// and `client.query` takes over as the root of the statement's own trace.
fn bolt_run_span(session: &BoltProtocolSession, context: &BoltConnectionContext, extra: &BoltDict) -> tracing::Span {
    let span = bolt_ingress_span!("bolt.run");
    record_bolt_ingress_scope(&span, session, context, extra);
    span
}

/// The span a ROUTE is served under.
///
/// ROUTE never runs a query, so it opens no `client.query` and would otherwise
/// have no tenancy anywhere. It is also the message a driver sends *first* and
/// the one that fails when a tenant's grant is wrong, so a forbidden ROUTE with
/// no tenant on it is a support ticket with nothing to grep for.
fn bolt_route_span(session: &BoltProtocolSession, context: &BoltConnectionContext, extra: &BoltDict) -> tracing::Span {
    let span = bolt_ingress_span!("bolt.route");
    record_bolt_ingress_scope(&span, session, context, extra);
    span
}

fn prepare_bolt_run(
    session: &mut BoltProtocolSession,
    context: &BoltConnectionContext,
    query: String,
    parameters: BoltDict,
    extra: BoltDict,
) -> std::result::Result<(ClientQuerySession, String, ClientQueryRequest), BoltError> {
    let authenticated = session.authenticated()?;
    validate_bolt_run_extra(&extra)?;
    let database = selected_bolt_database(session, context, &extra)?;
    let target = context
        .database_resolver
        .resolve_database(Some(&database))
        .map_err(graph_error_to_bolt)?;
    let query_id = session.next_query_id();
    let mut request = bolt_query_request(target.clone(), query_id.clone(), query, parameters, &extra)?;
    let runtime_limit_ms = context
        .service
        .effective_runtime_limit_ms(request.max_runtime_ms)
        .map_err(graph_error_to_bolt)?;
    request.max_runtime_ms = Some(runtime_limit_ms);
    if let Some(bookmark) = highest_matching_bookmark(&target, bolt_bookmarks_from_extra(&extra)?)? {
        request = request.after_bookmark(bookmark);
    }
    Ok((authenticated, database, request))
}

fn bolt_query_request(
    target: ClientQueryTarget,
    query_id: String,
    query: String,
    parameters: BoltDict,
    extra: &BoltDict,
) -> std::result::Result<ClientQueryRequest, BoltError> {
    let parameters = parameters
        .iter()
        .map(|(name, value)| Ok((name.clone(), bolt_parameter_to_query_value(name, value)?)))
        .collect::<std::result::Result<BTreeMap<_, _>, BoltError>>()?;
    let mut request = ClientQueryRequest::new(target, query_id, query).with_query_parameters(parameters);
    #[cfg(feature = "query-telemetry")]
    {
        request.protocol = "bolt";
    }
    request = match bolt_mutation_idempotency_key(extra)? {
        Some(caller_key) => request.with_mutation_idempotency_key(caller_key),
        None => request.with_server_generated_mutation_idempotency_key(format!("bolt-mutation-v1-{}", Ulid::new())),
    };
    match extra.get("tx_timeout") {
        Some(BoltValue::Integer(timeout_ms)) => {
            request = request.with_timeout_ms(
                u64::try_from(*timeout_ms).map_err(|_| BoltError::Protocol("tx_timeout must be a non-negative integer".to_string()))?,
            );
        }
        Some(_) => {
            return Err(BoltError::Protocol("tx_timeout must be an integer".to_string()));
        }
        None => {}
    }
    if let Some(consistency) = bolt_read_consistency(extra)? {
        request = request.with_consistency(consistency);
    }
    let caller = bolt_caller_metadata(extra)?;
    if let Some(correlation_id) = caller.correlation_id {
        request = request.with_correlation_id(correlation_id);
    }
    if let Some(caller_step) = caller.caller_step {
        request = request.with_caller_step(caller_step);
    }
    Ok(request)
}

/// `tx_metadata` key carrying the requested read consistency. This is the one
/// key the channel has carried in production, and its handling below is
/// unchanged by the allowlist that now surrounds it.
const TX_METADATA_CONSISTENCY: &str = "hydradb.consistency";
const LEGACY_TX_METADATA_CONSISTENCY: &str = "turbolay.consistency";

/// `tx_metadata` key carrying the caller's own request identifier — the field
/// that makes a HydraDB log line and an ingestion log line joinable.
const TX_METADATA_CORRELATION_ID: &str = "hydradb.correlation_id";
const LEGACY_TX_METADATA_CORRELATION_ID: &str = "turbolay.correlation_id";

/// `tx_metadata` key carrying the caller's operation label, e.g. which step of
/// a multi-step workflow issued this query.
const TX_METADATA_CALLER_STEP: &str = "hydradb.caller.step";
const LEGACY_TX_METADATA_CALLER_STEP: &str = "turbolay.caller.step";

/// Optional caller-owned mutation identity. A caller that retries the same
/// logical write must preserve this value across attempts. When absent, Bolt
/// generates a globally unique identity for this RUN request.
const TX_METADATA_IDEMPOTENCY_KEY: &str = "hydradb.idempotency_key";
const LEGACY_TX_METADATA_IDEMPOTENCY_KEY: &str = "turbolay.idempotency_key";

/// Every `tx_metadata` key HydraDB reads.
///
/// The dict arrives from any Bolt client, so each recognized value has a
/// dedicated parser and an unrecognized key is dropped rather than forwarded.
/// Correlation fields become telemetry, traceparent sets remote trace context,
/// and the idempotency key controls durable write deduplication.
const TX_METADATA_ALLOWLIST: &[&str] = &[
    TX_METADATA_CONSISTENCY,
    LEGACY_TX_METADATA_CONSISTENCY,
    TX_METADATA_CORRELATION_ID,
    LEGACY_TX_METADATA_CORRELATION_ID,
    TX_METADATA_CALLER_STEP,
    LEGACY_TX_METADATA_CALLER_STEP,
    TX_METADATA_IDEMPOTENCY_KEY,
    LEGACY_TX_METADATA_IDEMPOTENCY_KEY,
    TX_METADATA_TRACEPARENT,
];

/// `tx_metadata` key carrying the caller's W3C trace context.
///
/// Spelled without the `hydradb.` prefix, unlike its neighbours, because it is
/// not ours: `traceparent` is the name W3C Trace Context defines and every
/// other tracing system already sends. Renaming it would mean every client
/// library had to special-case HydraDB.
const TX_METADATA_TRACEPARENT: &str = "traceparent";

/// The caller-supplied correlation fields lifted out of `tx_metadata`, already
/// validated. Absent and invalid are the same state: `None`.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct BoltCallerMetadata {
    correlation_id: Option<String>,
    caller_step: Option<String>,
}

/// The `tx_metadata` dict, type-checked once for every reader of it.
fn bolt_tx_metadata(extra: &BoltDict) -> std::result::Result<Option<&BoltDict>, BoltError> {
    match extra.get("tx_metadata") {
        None => Ok(None),
        Some(BoltValue::Dict(metadata)) => Ok(Some(metadata)),
        Some(_) => Err(BoltError::Protocol("tx_metadata must be a dictionary".to_string())),
    }
}

/// Read the allowlisted caller-correlation keys out of `tx_metadata`.
///
/// Every value is untrusted. An over-long, non-printable, empty or wrongly
/// typed value is **dropped**, not truncated and not repaired, and HydraDB
/// never mints a replacement: a server-invented correlation id matches nothing
/// upstream, so it looks like a join key while joining nothing. That is the
/// deliberate difference from the ingestion side's `sanitize_correlation_id`,
/// which generates a UUID4 when the header is absent.
///
/// A malformed value is not a protocol error either. Telemetry metadata must
/// never be able to fail a query.
fn bolt_caller_metadata(extra: &BoltDict) -> std::result::Result<BoltCallerMetadata, BoltError> {
    let Some(metadata) = bolt_tx_metadata(extra)? else {
        return Ok(BoltCallerMetadata::default());
    };
    let dropped = metadata.keys().filter(|key| !TX_METADATA_ALLOWLIST.contains(&key.as_str())).count();
    if dropped > 0 {
        // The count, never the keys: they are caller-controlled strings and
        // this line is here so "I set the key and nothing appeared" is
        // diagnosable, not so unknown input reaches a log sink.
        tracing::debug!(
            target: "hydradb",
            dropped,
            "Bolt tx_metadata keys outside the allowlist were dropped"
        );
    }
    Ok(BoltCallerMetadata {
        correlation_id: allowlisted_caller_value(metadata, TX_METADATA_CORRELATION_ID)
            .or_else(|| allowlisted_caller_value(metadata, LEGACY_TX_METADATA_CORRELATION_ID)),
        caller_step: allowlisted_caller_value(metadata, TX_METADATA_CALLER_STEP)
            .or_else(|| allowlisted_caller_value(metadata, LEGACY_TX_METADATA_CALLER_STEP)),
    })
}

fn bolt_mutation_idempotency_key(extra: &BoltDict) -> std::result::Result<Option<String>, BoltError> {
    let Some(metadata) = bolt_tx_metadata(extra)? else {
        return Ok(None);
    };
    let Some(value) = aliased_metadata_value(metadata, TX_METADATA_IDEMPOTENCY_KEY, LEGACY_TX_METADATA_IDEMPOTENCY_KEY)? else {
        return Ok(None);
    };
    let BoltValue::String(value) = value else {
        return Err(BoltError::Query {
            code: "Neo.ClientError.Statement.TypeError".to_string(),
            message: format!("{TX_METADATA_IDEMPOTENCY_KEY} must be a string"),
        });
    };
    if value.len() > MAX_BOLT_MUTATION_IDEMPOTENCY_KEY_LEN || crate::validate_component("mutation_idempotency_key", value).is_err() {
        return Err(BoltError::Query {
            code: "Neo.ClientError.Statement.ArgumentError".to_string(),
            message: format!(
                "{TX_METADATA_IDEMPOTENCY_KEY} must be 1-{MAX_BOLT_MUTATION_IDEMPOTENCY_KEY_LEN} ASCII letters, digits, '.', '_' or '-'"
            ),
        });
    }
    Ok(Some(format!("bolt-caller-v1-{value}")))
}

/// The caller's W3C trace context, if it sent a well-formed one.
///
/// Kept separate from [`bolt_caller_metadata`] because it has a different
/// destination and a different lifetime: the correlation fields ride on the
/// request as attributes, whereas this one is consumed once, at RUN, to give
/// the statement span a remote parent. It is validated only for shape here —
/// the layout check that matters happens where it is adopted.
fn bolt_traceparent(extra: &BoltDict) -> std::result::Result<Option<String>, BoltError> {
    let Some(metadata) = bolt_tx_metadata(extra)? else {
        return Ok(None);
    };
    Ok(allowlisted_caller_value(metadata, TX_METADATA_TRACEPARENT))
}

/// One allowlisted key, validated. Non-strings are dropped like any other
/// invalid value.
fn allowlisted_caller_value(metadata: &BoltDict, key: &str) -> Option<String> {
    debug_assert!(TX_METADATA_ALLOWLIST.contains(&key));
    match metadata.get(key) {
        Some(BoltValue::String(value)) => sanitize_caller_metadata(value),
        _ => None,
    }
}

fn aliased_metadata_value<'a>(
    metadata: &'a BoltDict,
    canonical_key: &str,
    legacy_key: &str,
) -> std::result::Result<Option<&'a BoltValue>, BoltError> {
    match (metadata.get(canonical_key), metadata.get(legacy_key)) {
        (Some(canonical), Some(legacy)) if canonical != legacy => Err(BoltError::Protocol(format!(
            "conflicting Bolt metadata values for {canonical_key} and {legacy_key}"
        ))),
        (Some(value), _) | (_, Some(value)) => Ok(Some(value)),
        (None, None) => Ok(None),
    }
}

fn bolt_read_consistency(extra: &BoltDict) -> std::result::Result<Option<ClientReadConsistency>, BoltError> {
    let direct = extra.get("consistency");
    let metadata = match bolt_tx_metadata(extra)? {
        Some(metadata) => aliased_metadata_value(metadata, TX_METADATA_CONSISTENCY, LEGACY_TX_METADATA_CONSISTENCY)?,
        None => None,
    };
    let value = match (direct, metadata) {
        (Some(left), Some(right)) if left != right => {
            return Err(BoltError::Protocol("conflicting Bolt consistency metadata".to_string()));
        }
        (Some(value), _) | (_, Some(value)) => Some(value),
        (None, None) => None,
    };
    match value {
        None => Ok(None),
        Some(BoltValue::String(value)) if value == "causal" => Ok(Some(ClientReadConsistency::Causal)),
        Some(BoltValue::String(value)) if value == "strong" => Ok(Some(ClientReadConsistency::Strong)),
        Some(BoltValue::String(_)) => Err(BoltError::Protocol("consistency metadata must be causal or strong".to_string())),
        Some(_) => Err(BoltError::Protocol("consistency metadata must be a string".to_string())),
    }
}

fn validate_bolt_run_extra(extra: &BoltDict) -> std::result::Result<(), BoltError> {
    if extra.contains_key("imp_user") {
        return Err(BoltError::Forbidden("Bolt user impersonation is not supported".to_string()));
    }
    match extra.get("mode") {
        None => Ok(()),
        Some(BoltValue::String(mode)) if mode == "r" || mode == "w" => Ok(()),
        Some(BoltValue::String(_)) => Err(BoltError::Protocol("mode metadata must be r or w".to_string())),
        Some(_) => Err(BoltError::Protocol("mode metadata must be a string".to_string())),
    }
}

fn bolt_bookmarks_from_extra(extra: &BoltDict) -> std::result::Result<Vec<String>, BoltError> {
    match extra.get("bookmarks") {
        None => Ok(Vec::new()),
        Some(BoltValue::List(bookmarks)) => bookmarks
            .iter()
            .map(|bookmark| {
                bookmark
                    .as_str()
                    .map(str::to_string)
                    .ok_or_else(|| BoltError::Protocol("bookmarks must be strings".to_string()))
            })
            .collect(),
        Some(_) => Err(BoltError::Protocol("bookmarks must be a list of strings".to_string())),
    }
}

async fn await_bolt_page<W, F>(
    task: F,
    service: &ClientQueryService,
    authenticated: &ClientQuerySession,
    scope: &GraphScope,
    query_id: &str,
    io: &mut SessionIo<W>,
) -> std::result::Result<PageAwaitResult, BoltError>
where
    W: AsyncWrite + Unpin,
    F: Future<Output = Result<ClientQueryPage>>,
{
    let cancel = async {
        let _ = service.cancel(authenticated, scope, query_id).await;
    };
    io.interruptible(async { task.await.map(Box::new) }, cancel).await
}

/// Record-encoding time accumulated across one PULL and reported to the
/// service between page fetches, so a fetch's wait is never billed as
/// serialization. Dropping it reports what is left, so a PULL that fails or
/// returns early still counts the records it already wrote.
struct SerializeTally<'a> {
    service: &'a ClientQueryService,
    elapsed: Duration,
    rows: usize,
}

impl<'a> SerializeTally<'a> {
    fn new(service: &'a ClientQueryService) -> Self {
        Self {
            service,
            elapsed: Duration::ZERO,
            rows: 0,
        }
    }

    fn add(&mut self, elapsed: Duration) {
        self.elapsed += elapsed;
        self.rows += 1;
    }

    fn flush(&mut self) {
        if self.rows > 0 {
            self.service.record_serialization(self.elapsed, self.rows);
        }
        self.elapsed = Duration::ZERO;
        self.rows = 0;
    }
}

impl Drop for SerializeTally<'_> {
    fn drop(&mut self) {
        self.flush();
    }
}

async fn consume_bolt_records<W>(
    context: &Arc<BoltConnectionContext>,
    authenticated: &ClientQuerySession,
    pending: &mut PendingBoltResult,
    count: Option<usize>,
    prefetch_rows: usize,
    disposition: BoltRecordDisposition,
    io: &mut SessionIo<W>,
) -> std::result::Result<PageAwaitResult, BoltError>
where
    W: AsyncWrite + Unpin,
{
    let mut remaining = count.unwrap_or(usize::MAX);
    // Encoding and writing records, excluding the page fetches interleaved
    // with them, so serialization is its own term in a request's decomposition.
    let mut serialize = SerializeTally::new(&context.service);
    while remaining > 0 {
        if let Some(row) = pending.rows.pop_front() {
            if matches!(disposition, BoltRecordDisposition::Send) {
                let started = Instant::now();
                let values = row
                    .values
                    .iter()
                    .map(query_value_to_bolt)
                    .collect::<std::result::Result<Vec<_>, BoltError>>()?;
                io.send(&ServerMessage::Record { data: values }).await?;
                serialize.add(started.elapsed());
            }
            remaining -= 1;
            continue;
        }
        serialize.flush();
        let cursor = if pending.started {
            let Some(cursor) = pending.next_cursor.take() else {
                break;
            };
            Some(cursor)
        } else {
            pending.started = true;
            None
        };
        let fetch_size = count.map_or(prefetch_rows, |count| prefetch_rows.min(count.max(1)));
        let query_id = pending.prepared.request.query_id.clone();
        let scope = pending.prepared.request.target.scope.clone();
        // One span per fetch, which is at least one per PULL. The budget below
        // is recomputed here against a fixed deadline, so a stream that reports
        // "29999 ms; limit is 29999 ms" explains itself from the span sequence
        // instead of from a code read — and a client that pages lazily shows
        // where the wall-clock actually went.
        pending.pages += 1;
        let page_span = tracing::info_span!(
            target: "hydradb",
            parent: &pending.statement_span,
            "query.page",
            hydradb.scope = %scope,
            hydradb.tenant_id = tracing::field::Empty,
            hydradb.tenant.scope_id = tracing::field::Empty,
            hydradb.sub_tenant_id = tracing::field::Empty,
            hydradb.sub_tenant.scope_id = tracing::field::Empty,
            hydradb.correlation_id = tracing::field::Empty,
            hydradb.caller.step = tracing::field::Empty,
            hydradb.read_epoch = tracing::field::Empty,
            hydradb.query.rows_returned = tracing::field::Empty,
            error.class = tracing::field::Empty,
            page = pending.pages,
            fetch_size,
            remaining_runtime_ms = tracing::field::Empty,
        );
        // Repeated from the statement span rather than inherited from it: a
        // PULL that arrives long after its RUN is read on its own in a trace
        // backend, and `hydradb.scope` two lines up is repeated for the same
        // reason.
        crate::client::service::record_scope_tenancy(&page_span, &scope);
        if let Some(correlation_id) = pending.prepared.request.correlation_id.as_deref() {
            page_span.record("hydradb.correlation_id", correlation_id);
        }
        if let Some(caller_step) = pending.prepared.request.caller_step.as_deref() {
            page_span.record("hydradb.caller.step", caller_step);
        }
        let remaining_runtime_ms = match remaining_bolt_runtime_ms(pending.deadline) {
            Ok(remaining) => remaining,
            Err(error) => {
                page_span.record("error.class", "admission");
                return Err(error);
            }
        };
        page_span.record("remaining_runtime_ms", remaining_runtime_ms);
        pending.prepared.request.max_runtime_ms = Some(remaining_runtime_ms);
        let task = context
            .service
            .execute_prepared_page(authenticated, pending.prepared.clone(), cursor, fetch_size);
        let awaited = await_bolt_page(task, &context.service, authenticated, &scope, &query_id, io)
            .instrument(page_span.clone())
            .await?;
        match awaited {
            PageAwaitResult::Complete(Ok(page)) => {
                if page.page.columns != pending.columns {
                    let error = GraphError::CorruptValue {
                        key: "client/bolt/result_columns".to_string(),
                        reason: "result columns changed between cursor pages".to_string(),
                    };
                    page_span.record("error.class", error.class());
                    return Ok(PageAwaitResult::Complete(Err(error)));
                }
                page_span.record("hydradb.query.rows_returned", page.page.rows.len() as u64);
                if let Some(read_epoch) = page.read_epoch {
                    page_span.record("hydradb.read_epoch", read_epoch);
                }
                pending.prepared.request.read_epoch = page.read_epoch;
                pending.rows.extend(page.page.rows);
                pending.next_cursor = page.page.next_cursor;
                if pending.statuses.is_empty() {
                    pending.statuses = page.page.statuses;
                }
                pending.bookmark = page.bookmark;
            }
            PageAwaitResult::Complete(Err(error)) => {
                page_span.record("error.class", error.class());
                return Ok(PageAwaitResult::Complete(Err(error)));
            }
            outcome => return Ok(outcome),
        }
    }
    serialize.flush();
    Ok(PageAwaitResult::Complete(Ok(Box::new(ClientQueryPage {
        query_id: pending.prepared.request.query_id.clone(),
        page: crate::QueryResultPage::new(pending.columns.clone(), Vec::new(), pending.next_cursor),
        read_epoch: pending.prepared.request.read_epoch,
        bookmark: pending.bookmark.clone(),
    }))))
}

fn remaining_bolt_runtime_ms(deadline: Instant) -> std::result::Result<u64, BoltError> {
    let remaining = deadline.saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        return Err(BoltError::Query {
            code: "Neo.TransientError.Transaction.Terminated".to_string(),
            message: "query stream deadline elapsed".to_string(),
        });
    }
    Ok(u64::try_from(remaining.as_millis().max(1)).unwrap_or(u64::MAX))
}

fn bolt_result_summary_metadata(pending: &PendingBoltResult, has_more: bool) -> BoltDict {
    let mut metadata = BoltDict::from([("has_more".to_string(), BoltValue::Boolean(has_more))]);
    if !has_more {
        if let Some(bookmark) = &pending.bookmark {
            metadata.insert("bookmark".to_string(), BoltValue::String(bookmark.encode()));
        }
        metadata.insert("db".to_string(), BoltValue::String(pending.database.clone()));
        metadata.insert("t_last".to_string(), BoltValue::Integer(0));
        // The statement's own access, classified from its text, not the
        // client's declared mode: graph-node does not refuse a write sent in
        // read mode. The turbolay proxy allows a ROLLBACK only after "r".
        let query_type = match pending.prepared.action {
            QueryTransportAction::Read => "r",
            QueryTransportAction::Write | QueryTransportAction::Cancel | QueryTransportAction::Admin => "rw",
        };
        metadata.insert("type".to_string(), BoltValue::String(query_type.to_string()));
        // GQL-status objects (ISO/IEC 39075 §4.9), in the shape Bolt 5.6
        // gives `statuses`. A driver of an older protocol ignores the key.
        if !pending.statuses.is_empty() {
            metadata.insert("statuses".to_string(), bolt_statuses(&pending.statuses));
        }
    }
    metadata
}

/// GQL-status objects as Bolt 5.6's `statuses` list.
fn bolt_statuses(statuses: &[GqlStatusReport]) -> BoltValue {
    BoltValue::List(
        statuses
            .iter()
            .map(|status| {
                BoltValue::Dict(BoltDict::from([
                    ("gql_status".to_string(), BoltValue::String(status.gql_status.clone())),
                    ("status_description".to_string(), BoltValue::String(status.description.clone())),
                    ("diagnostic_record".to_string(), bolt_diagnostic_record(status)),
                ]))
            })
            .collect(),
    )
}

/// A GQL diagnostic record (§23.2) as a Bolt map of strings.
fn bolt_diagnostic_record(status: &GqlStatusReport) -> BoltValue {
    BoltValue::Dict(
        status
            .diagnostic
            .iter()
            .map(|(name, value)| (name.clone(), BoltValue::String(value.clone())))
            .collect(),
    )
}

fn bolt_stream_count(extra: &BoltDict, operation: &'static str) -> std::result::Result<Option<usize>, BoltError> {
    if let Some(query_id) = extra.get("qid") {
        if query_id.as_int() != Some(-1) {
            return Err(BoltError::Protocol(format!("{operation} qid must be -1 for an auto-commit result")));
        }
    }
    let n = extra.get("n").and_then(BoltValue::as_int).unwrap_or(-1);
    if n == -1 {
        return Ok(None);
    }
    if n <= 0 {
        return Err(BoltError::Protocol(format!("{operation} n must be -1 or a positive integer")));
    }
    usize::try_from(n)
        .map(Some)
        .map_err(|_| BoltError::Protocol(format!("{operation} n exceeds platform limits")))
}

async fn prepare_bolt_route(
    session: &BoltProtocolSession,
    context: &BoltConnectionContext,
    bookmarks: Vec<String>,
    extra: &BoltDict,
) -> std::result::Result<BoltDict, BoltError> {
    let authenticated = session.authenticated()?;
    if extra.contains_key("imp_user") {
        return Err(BoltError::Forbidden("Bolt user impersonation is not supported".to_string()));
    }
    let database = selected_bolt_database(session, context, extra)?;
    let target = context
        .database_resolver
        .resolve_database(Some(&database))
        .map_err(graph_error_to_bolt)?;
    context
        .service
        .authorize_any_action(
            &authenticated,
            &target.scope,
            &[QueryTransportAction::Read, QueryTransportAction::Write],
            "route to",
        )
        .map_err(graph_error_to_bolt)?;
    // Routing depends on placement and writer leases, not the router's data
    // snapshot. Validate scope/encoding here; RUN enforces the bookmark on the
    // selected execution node before reading graph data.
    highest_matching_bookmark(&target, bookmarks)?;
    let table = match &context.routing_table_provider {
        Some(provider) => provider.routing_table(&database, &target).await.map_err(graph_error_to_bolt)?,
        None if !context.routing_servers.is_empty() => {
            BoltRoutingTable::new(context.routing_ttl_secs, context.routing_servers.clone()).map_err(graph_error_to_bolt)?
        }
        None => {
            return Err(BoltError::ResourceExhausted(
                "no Bolt routing table provider or static routing table is configured".to_string(),
            ));
        }
    };
    Ok(bolt_routing_table(&table, database))
}

fn bolt_routing_table(table: &BoltRoutingTable, database: String) -> BoltDict {
    let servers = table
        .servers
        .iter()
        .map(|server| {
            BoltValue::Dict(BoltDict::from([
                (
                    "addresses".to_string(),
                    BoltValue::List(server.addresses.iter().cloned().map(BoltValue::String).collect()),
                ),
                ("role".to_string(), BoltValue::String(server.role.clone())),
            ]))
        })
        .collect();
    BoltDict::from([
        ("ttl".to_string(), BoltValue::Integer(table.ttl_secs)),
        ("db".to_string(), BoltValue::String(database)),
        ("servers".to_string(), BoltValue::List(servers)),
    ])
}

/// FAILURE for a failed query. Beside `code` and `message`, which every Bolt
/// version reads, an engine error carries its GQL-status object (ISO/IEC
/// 39075 §4.9) in the fields Bolt 5.7 gives FAILURE: `gql_status`,
/// `description`, `neo4j_code` and `diagnostic_record`. A driver of an older
/// protocol ignores them.
async fn send_bolt_graph_failure<W>(io: &mut SessionIo<W>, error: GraphError) -> std::result::Result<(), BoltError>
where
    W: AsyncWrite + Unpin,
{
    let metadata = bolt_graph_failure_metadata(error);
    io.send(&ServerMessage::Failure { metadata }).await
}

/// The metadata of [`send_bolt_graph_failure`].
fn bolt_graph_failure_metadata(error: GraphError) -> BoltDict {
    let gql = match &error {
        GraphError::GqlQuery { status, .. } => Some((**status).clone()),
        _ => None,
    };
    let mut metadata = graph_error_to_bolt(error).to_failure_metadata();
    if let Some(status) = gql {
        if let Some(code) = metadata.get("code").cloned() {
            metadata.insert("neo4j_code".to_string(), code);
        }
        metadata.insert("gql_status".to_string(), BoltValue::String(status.gql_status.clone()));
        metadata.insert("description".to_string(), BoltValue::String(status.description.clone()));
        metadata.insert("diagnostic_record".to_string(), bolt_diagnostic_record(&status));
    }
    metadata
}

fn bolt_io_error(operation: &'static str, error: std::io::Error) -> GraphError {
    GraphError::CorruptValue {
        key: format!("client/bolt/{operation}"),
        reason: error.to_string(),
    }
}
