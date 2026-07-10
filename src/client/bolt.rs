use std::collections::{BTreeMap, VecDeque};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use boltr::chunk::ChunkWriter;
use boltr::error::BoltError;
use boltr::message::encode::encode_server_message;
use boltr::message::{ClientMessage, ServerMessage};
use boltr::types::{BoltDict, BoltValue};
use bytes::BytesMut;
use sha2::{Digest, Sha256};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, watch, OwnedSemaphorePermit, Semaphore};
use tokio::task::{JoinHandle, JoinSet};
use tokio::time::Instant;
use tokio_rustls::TlsAcceptor;

use super::service::{
    ClientBookmark, ClientDatabaseResolver, ClientQueryCredentials, ClientQueryPage,
    ClientQueryRequest, ClientQueryService, ClientQuerySession, ClientQueryTarget,
};
use crate::{
    GraphError, GraphScope, QueryColumn, QueryCursorToken, QueryRow, QueryTransportAction,
    QueryTransportConnectionIdentity, QueryTransportTlsServerConfigProvider, Result,
};

mod routing;
mod values;
mod wire;
use routing::validate_bolt_routing_table;
pub use routing::{
    BoltRoutingServer, BoltRoutingTable, BoltRoutingTableProvider,
    ControllerBoltRoutingTableProvider,
};
use values::{
    bolt_parameter_to_property, explicit_transactions_unsupported, graph_error_to_bolt,
    highest_matching_bookmark, query_value_to_bolt,
};
use wire::{bolt_server_handshake, read_bolt_messages};

const BOLT_MAGIC: [u8; 4] = [0x60, 0x60, 0xb0, 0x17];
const BOLT_MANIFEST_V1: [u8; 4] = [0x00, 0x00, 0x01, 0xff];
const BOLT_NO_VERSION: [u8; 4] = [0, 0, 0, 0];
const BOLT_SUPPORTED_VERSIONS: [(u8, u8); 4] = [(5, 4), (5, 3), (5, 2), (5, 1)];
const DEFAULT_BOLT_MAX_MESSAGE_BYTES: usize = 2 * 1024 * 1024;
const DEFAULT_BOLT_MAX_CONNECTIONS: usize = 1024;
const DEFAULT_BOLT_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);
const DEFAULT_BOLT_GRACEFUL_SHUTDOWN: Duration = Duration::from_secs(30);
const DEFAULT_BOLT_IDLE_TIMEOUT: Duration = Duration::from_secs(60);
const DEFAULT_BOLT_MAX_CONNECTION_AGE: Duration = Duration::from_secs(60 * 60);
const DEFAULT_BOLT_WRITE_TIMEOUT: Duration = Duration::from_secs(30);
const DEFAULT_BOLT_ROUTING_TTL_SECS: i64 = 30;
const DEFAULT_BOLT_PREFETCH_ROWS: usize = 256;
const DEFAULT_BOLT_MAX_PIPELINED_MESSAGES: usize = 8;
const MAX_BOLT_PIPELINE_BUFFER_BYTES: usize = 64 * 1024 * 1024;
const BOLT_MAX_PACKSTREAM_DEPTH: usize = 32;

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
    pub idle_timeout: Duration,
    pub max_connection_age: Duration,
    pub write_timeout: Duration,
    pub graceful_shutdown_timeout: Duration,
    pub allow_plaintext: bool,
    pub tls_config_provider: Option<Arc<dyn QueryTransportTlsServerConfigProvider>>,
}

impl BoltServerConfig {
    pub fn new(database_resolver: Arc<dyn ClientDatabaseResolver>) -> Self {
        Self {
            database_resolver,
            default_database: "default".to_string(),
            routing_ttl_secs: DEFAULT_BOLT_ROUTING_TTL_SECS,
            routing_servers: Vec::new(),
            routing_table_provider: None,
            max_message_bytes: DEFAULT_BOLT_MAX_MESSAGE_BYTES,
            max_connections: DEFAULT_BOLT_MAX_CONNECTIONS,
            prefetch_rows: DEFAULT_BOLT_PREFETCH_ROWS,
            max_pipelined_messages: DEFAULT_BOLT_MAX_PIPELINED_MESSAGES,
            handshake_timeout: DEFAULT_BOLT_HANDSHAKE_TIMEOUT,
            idle_timeout: DEFAULT_BOLT_IDLE_TIMEOUT,
            max_connection_age: DEFAULT_BOLT_MAX_CONNECTION_AGE,
            write_timeout: DEFAULT_BOLT_WRITE_TIMEOUT,
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

    pub fn with_routing_table_provider(
        mut self,
        provider: Arc<dyn BoltRoutingTableProvider>,
    ) -> Self {
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

    pub fn with_tls_provider(
        mut self,
        provider: Arc<dyn QueryTransportTlsServerConfigProvider>,
    ) -> Self {
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
        if self.max_message_bytes == 0
            || self.max_connections == 0
            || self.prefetch_rows == 0
            || self.max_pipelined_messages == 0
        {
            return bolt_config_error(
                "message, connection, prefetch, and pipeline limits must be greater than zero",
            );
        }
        let buffered_bytes = self
            .max_message_bytes
            .checked_mul(self.max_pipelined_messages)
            .and_then(|bytes| bytes.checked_mul(2))
            .ok_or_else(|| GraphError::UnsupportedQuery {
                dialect: "Bolt",
                feature: "Bolt pipeline byte budget overflows usize".to_string(),
            })?;
        if buffered_bytes > MAX_BOLT_PIPELINE_BUFFER_BYTES {
            return bolt_config_error(
                "message size and pipeline depth can buffer more than 64 MiB per connection",
            );
        }
        if self.handshake_timeout.is_zero()
            || self.idle_timeout.is_zero()
            || self.max_connection_age.is_zero()
            || self.write_timeout.is_zero()
            || self.graceful_shutdown_timeout.is_zero()
        {
            return bolt_config_error("Bolt connection timeouts must be greater than zero");
        }
        if self.tls_config_provider.is_none() && !self.allow_plaintext {
            return bolt_config_error(
                "Bolt requires TLS unless plaintext is explicitly enabled for development",
            );
        }
        self.database_resolver
            .resolve_database(Some(&self.default_database))?;
        Ok(())
    }
}

fn bolt_config_error<T>(reason: &str) -> Result<T> {
    Err(GraphError::UnsupportedQuery {
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
    pub async fn bind(
        addr: SocketAddr,
        service: ClientQueryService,
        config: BoltServerConfig,
    ) -> Result<BoltServerHandle> {
        config.validate()?;
        let listener = TcpListener::bind(addr)
            .await
            .map_err(|err| bolt_io_error("bind", err))?;
        let local_addr = listener
            .local_addr()
            .map_err(|err| bolt_io_error("local_addr", err))?;
        let config = Arc::new(config);
        let metrics = Arc::new(BoltServerMetrics::default());
        let connection_gate = Arc::new(Semaphore::new(config.max_connections));
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
    connection_gate: Arc<Semaphore>,
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
                        tracing::warn!(target: "slatedb_graph_kernel", error = %err, "Bolt accept failed");
                        continue;
                    }
                };
                let permit = match Arc::clone(&connection_gate).try_acquire_owned() {
                    Ok(permit) => permit,
                    Err(_) => {
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
                ));
            }
            completed = connections.join_next(), if !connections.is_empty() => {
                if let Some(Err(err)) = completed {
                    tracing::warn!(target: "slatedb_graph_kernel", error = %err, "Bolt connection task failed");
                }
            }
        }
    }
    let graceful = async { while connections.join_next().await.is_some() {} };
    if tokio::time::timeout(config.graceful_shutdown_timeout, graceful)
        .await
        .is_err()
    {
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
    _permit: OwnedSemaphorePermit,
) {
    metrics.active_connections.fetch_add(1, Ordering::Relaxed);
    let result = serve_bolt_connection_inner(stream, peer_addr, service, config).await;
    metrics.active_connections.fetch_sub(1, Ordering::Relaxed);
    if let Err(err) = result {
        if matches!(err, BoltError::Protocol(_)) {
            metrics.handshake_failures.fetch_add(1, Ordering::Relaxed);
        }
        tracing::debug!(target: "slatedb_graph_kernel", %peer_addr, error = %err, "Bolt connection closed");
    }
}

async fn serve_bolt_connection_inner(
    stream: TcpStream,
    peer_addr: SocketAddr,
    service: ClientQueryService,
    config: Arc<BoltServerConfig>,
) -> std::result::Result<(), BoltError> {
    stream.set_nodelay(true)?;
    let (mut io, identity): (Box<dyn BoltIo>, QueryTransportConnectionIdentity) =
        match &config.tls_config_provider {
            Some(provider) => {
                let server_config = provider
                    .current_server_config()
                    .map_err(|err| BoltError::Backend(err.to_string()))?;
                let tls_stream = tokio::time::timeout(
                    config.handshake_timeout,
                    TlsAcceptor::from(server_config).accept(stream),
                )
                .await
                .map_err(|_| BoltError::Protocol("TLS handshake timed out".to_string()))?
                .map_err(|err| BoltError::Io(std::io::Error::other(err)))?;
                let identity = bolt_tls_identity(&tls_stream);
                (Box::new(tls_stream), identity)
            }
            None => (
                Box::new(stream),
                QueryTransportConnectionIdentity::default(),
            ),
        };
    tokio::time::timeout(config.handshake_timeout, bolt_server_handshake(&mut *io))
        .await
        .map_err(|_| BoltError::Protocol("Bolt handshake timed out".to_string()))??;

    let context = Arc::new(BoltConnectionContext::new(
        service,
        Arc::clone(&config.database_resolver),
        config.default_database.clone(),
        config.routing_ttl_secs,
        config.routing_servers.clone(),
        config.routing_table_provider.clone(),
        identity,
    ));
    run_bolt_protocol(io, peer_addr, context, config.as_ref()).await
}

fn bolt_tls_identity<S>(
    stream: &tokio_rustls::server::TlsStream<S>,
) -> QueryTransportConnectionIdentity {
    let mut identity = QueryTransportConnectionIdentity::default();
    let (_, connection) = stream.get_ref();
    if let Some(certificates) = connection.peer_certificates() {
        for (index, certificate) in certificates.iter().enumerate() {
            let fingerprint = format!(
                "sha256:{}",
                lowercase_hex(&Sha256::digest(certificate.as_ref()))
            );
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

#[derive(Clone, Copy, Eq, PartialEq)]
enum BoltState {
    Negotiation,
    Authentication,
    Ready,
    Streaming,
    Failed,
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
            id: format!(
                "bolt-{}",
                NEXT_BOLT_SESSION_ID.fetch_add(1, Ordering::Relaxed)
            ),
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
    request: ClientQueryRequest,
    deadline: Instant,
    columns: Vec<QueryColumn>,
    rows: VecDeque<QueryRow>,
    next_cursor: Option<QueryCursorToken>,
    bookmark: Option<ClientBookmark>,
}

enum PageAwaitResult {
    Complete(Result<ClientQueryPage>),
    Reset,
    Goodbye,
}

#[derive(Clone, Copy)]
enum BoltRecordDisposition {
    Send,
    Discard,
}

struct BoltReaderTask(JoinHandle<()>);

impl BoltReaderTask {
    fn new(task: JoinHandle<()>) -> Self {
        Self(task)
    }

    async fn stop(mut self) {
        self.0.abort();
        let _ = (&mut self.0).await;
    }
}

impl Drop for BoltReaderTask {
    fn drop(&mut self) {
        self.0.abort();
    }
}

struct BoltMessageWriter<W> {
    writer: ChunkWriter<W>,
    timeout: Duration,
}

impl<W> BoltMessageWriter<W>
where
    W: AsyncWrite + Unpin,
{
    fn new(writer: W, timeout: Duration) -> Self {
        Self {
            writer: ChunkWriter::new(writer),
            timeout,
        }
    }

    async fn send(&mut self, message: &ServerMessage) -> std::result::Result<(), BoltError> {
        let mut bytes = BytesMut::new();
        encode_server_message(&mut bytes, message);
        tokio::time::timeout(self.timeout, async {
            self.writer.write_message(&bytes).await?;
            self.writer.flush().await
        })
        .await
        .map_err(|_| {
            BoltError::Io(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "Bolt response write timed out",
            ))
        })?
    }
}

struct BoltConnectionChannels<'a, W> {
    message_rx: &'a mut mpsc::Receiver<std::result::Result<ClientMessage, BoltError>>,
    queued: &'a mut VecDeque<ClientMessage>,
    writer: &'a mut BoltMessageWriter<W>,
    max_queued_messages: usize,
}

async fn run_bolt_protocol(
    io: Box<dyn BoltIo>,
    peer_addr: SocketAddr,
    context: Arc<BoltConnectionContext>,
    config: &BoltServerConfig,
) -> std::result::Result<(), BoltError> {
    let (read_half, write_half) = tokio::io::split(io);
    let (message_tx, mut message_rx) = mpsc::channel(config.max_pipelined_messages);
    let reader_task = BoltReaderTask::new(tokio::spawn(read_bolt_messages(
        read_half,
        message_tx,
        config.max_message_bytes,
    )));
    let mut writer = BoltMessageWriter::new(write_half, config.write_timeout);
    let mut queued = VecDeque::new();
    let mut state = BoltState::Negotiation;
    let mut session = BoltProtocolSession::new();
    let connection_deadline = Instant::now() + config.max_connection_age;

    loop {
        if Instant::now() >= connection_deadline {
            break;
        }
        let incoming = match queued.pop_front() {
            Some(message) => Some(Ok(message)),
            None => {
                tokio::select! {
                    incoming = message_rx.recv() => incoming,
                    _ = tokio::time::sleep(config.idle_timeout) => {
                        return Err(BoltError::Io(std::io::Error::new(
                            std::io::ErrorKind::TimedOut,
                            "Bolt connection idle timeout elapsed",
                        )));
                    }
                    _ = tokio::time::sleep_until(connection_deadline) => break,
                }
            }
        };
        let message = match incoming {
            Some(Ok(message)) => message,
            Some(Err(error)) => {
                send_bolt_failure(&mut writer, &error).await?;
                state = BoltState::Failed;
                continue;
            }
            None => break,
        };

        if state == BoltState::Failed {
            match message {
                ClientMessage::Reset => {
                    session.pending = None;
                    send_bolt_success(&mut writer, BoltDict::new()).await?;
                    state = BoltState::Ready;
                }
                ClientMessage::Goodbye => break,
                _ => send_bolt_message(&mut writer, &ServerMessage::Ignored).await?,
            }
            continue;
        }

        match (state, message) {
            (BoltState::Negotiation, ClientMessage::Hello { .. }) => {
                send_bolt_success(
                    &mut writer,
                    BoltDict::from([
                        (
                            "server".to_string(),
                            BoltValue::String(format!(
                                "SlateDBGraph/{}",
                                env!("CARGO_PKG_VERSION")
                            )),
                        ),
                        (
                            "connection_id".to_string(),
                            BoltValue::String(session.id.clone()),
                        ),
                        ("hints".to_string(), BoltValue::Dict(BoltDict::new())),
                    ]),
                )
                .await?;
                state = BoltState::Authentication;
            }
            (BoltState::Authentication, ClientMessage::Logon { auth }) => {
                let credentials = match bolt_client_credentials(&auth) {
                    Ok(credentials) => credentials,
                    Err(error) => {
                        send_bolt_failure(&mut writer, &error).await?;
                        break;
                    }
                };
                match context
                    .service
                    .authenticate(&credentials, &context.identity)
                {
                    Ok(authenticated) => {
                        session.authenticated = Some(authenticated);
                        send_bolt_success(&mut writer, BoltDict::new()).await?;
                        state = BoltState::Ready;
                    }
                    Err(_) => {
                        send_bolt_failure(
                            &mut writer,
                            &BoltError::Authentication("invalid credentials".to_string()),
                        )
                        .await?;
                        break;
                    }
                }
            }
            (_, ClientMessage::Goodbye) => break,
            (BoltState::Ready, ClientMessage::Reset)
            | (BoltState::Streaming, ClientMessage::Reset) => {
                session.pending = None;
                send_bolt_success(&mut writer, BoltDict::new()).await?;
                state = BoltState::Ready;
            }
            (BoltState::Ready, ClientMessage::Logoff) => {
                session.pending = None;
                session.authenticated = None;
                session.database = None;
                send_bolt_success(&mut writer, BoltDict::new()).await?;
                state = BoltState::Authentication;
            }
            (
                BoltState::Ready,
                ClientMessage::Run {
                    query,
                    parameters,
                    extra,
                },
            ) => {
                let (authenticated, database, target, request, query_id) =
                    match prepare_bolt_run(&mut session, &context, query, parameters, extra) {
                        Ok(prepared) => prepared,
                        Err(error) => {
                            send_bolt_failure(&mut writer, &error).await?;
                            state = BoltState::Failed;
                            continue;
                        }
                    };
                let mut request = request;
                let query_deadline = Instant::now()
                    + Duration::from_millis(
                        request
                            .max_runtime_ms
                            .expect("Bolt requests are normalized before execution"),
                    );
                let task = spawn_bolt_page(
                    context.service.clone(),
                    authenticated.clone(),
                    request.clone(),
                    None,
                    config.prefetch_rows,
                );
                let page_result = {
                    let mut channels = BoltConnectionChannels {
                        message_rx: &mut message_rx,
                        queued: &mut queued,
                        writer: &mut writer,
                        max_queued_messages: config.max_pipelined_messages,
                    };
                    await_bolt_page(
                        task,
                        &context.service,
                        &authenticated,
                        &target.scope,
                        &query_id,
                        &mut channels,
                    )
                    .await
                };
                match page_result {
                    Err(error) => {
                        send_bolt_failure(&mut writer, &error).await?;
                        state = BoltState::Failed;
                    }
                    Ok(PageAwaitResult::Complete(Ok(page))) => {
                        request.read_epoch = page.read_epoch;
                        let fields = page
                            .page
                            .columns
                            .iter()
                            .map(|column| BoltValue::String(column.name.clone()))
                            .collect();
                        send_bolt_success(
                            &mut writer,
                            BoltDict::from([
                                ("fields".to_string(), BoltValue::List(fields)),
                                ("t_first".to_string(), BoltValue::Integer(0)),
                                ("db".to_string(), BoltValue::String(database.clone())),
                            ]),
                        )
                        .await?;
                        session.pending = Some(PendingBoltResult {
                            database,
                            request,
                            deadline: query_deadline,
                            columns: page.page.columns,
                            rows: page.page.rows.into(),
                            next_cursor: page.page.next_cursor,
                            bookmark: page.bookmark,
                        });
                        state = BoltState::Streaming;
                    }
                    Ok(PageAwaitResult::Complete(Err(error))) => {
                        send_bolt_failure(&mut writer, &graph_error_to_bolt(error)).await?;
                        state = BoltState::Failed;
                    }
                    Ok(PageAwaitResult::Reset) => {
                        session.pending = None;
                        state = BoltState::Ready;
                    }
                    Ok(PageAwaitResult::Goodbye) => break,
                }
            }
            (BoltState::Streaming, ClientMessage::Pull { extra }) => {
                let n = match bolt_stream_count(&extra, "PULL") {
                    Ok(n) => n,
                    Err(error) => {
                        send_bolt_failure(&mut writer, &error).await?;
                        state = BoltState::Failed;
                        continue;
                    }
                };
                let authenticated = match session.authenticated() {
                    Ok(authenticated) => authenticated,
                    Err(error) => {
                        send_bolt_failure(&mut writer, &error).await?;
                        state = BoltState::Failed;
                        continue;
                    }
                };
                let Some(pending) = session.pending.as_mut() else {
                    let error = BoltError::Protocol("no pending result".to_string());
                    send_bolt_failure(&mut writer, &error).await?;
                    state = BoltState::Failed;
                    continue;
                };
                let pull_result = {
                    let mut channels = BoltConnectionChannels {
                        message_rx: &mut message_rx,
                        queued: &mut queued,
                        writer: &mut writer,
                        max_queued_messages: config.max_pipelined_messages,
                    };
                    consume_bolt_records(
                        &context,
                        &authenticated,
                        pending,
                        n,
                        config.prefetch_rows,
                        BoltRecordDisposition::Send,
                        &mut channels,
                    )
                    .await
                };
                match pull_result {
                    Err(error) => {
                        send_bolt_failure(&mut writer, &error).await?;
                        session.pending = None;
                        state = BoltState::Failed;
                    }
                    Ok(PageAwaitResult::Complete(Ok(_))) => {
                        let pending = session
                            .pending
                            .as_ref()
                            .expect("PULL keeps pending state until summary");
                        let has_more = !pending.rows.is_empty() || pending.next_cursor.is_some();
                        let metadata = bolt_result_summary_metadata(pending, has_more);
                        send_bolt_success(&mut writer, metadata).await?;
                        if !has_more {
                            session.pending = None;
                            state = BoltState::Ready;
                        }
                    }
                    Ok(PageAwaitResult::Complete(Err(error))) => {
                        send_bolt_failure(&mut writer, &graph_error_to_bolt(error)).await?;
                        session.pending = None;
                        state = BoltState::Failed;
                    }
                    Ok(PageAwaitResult::Reset) => {
                        session.pending = None;
                        state = BoltState::Ready;
                    }
                    Ok(PageAwaitResult::Goodbye) => break,
                }
            }
            (BoltState::Streaming, ClientMessage::Discard { extra }) => {
                let count = match bolt_stream_count(&extra, "DISCARD") {
                    Ok(count) => count,
                    Err(error) => {
                        send_bolt_failure(&mut writer, &error).await?;
                        state = BoltState::Failed;
                        continue;
                    }
                };
                if count.is_none() {
                    let Some(pending) = session.pending.take() else {
                        let error = BoltError::Protocol("no pending result".to_string());
                        send_bolt_failure(&mut writer, &error).await?;
                        state = BoltState::Failed;
                        continue;
                    };
                    send_bolt_success(&mut writer, bolt_result_summary_metadata(&pending, false))
                        .await?;
                    state = BoltState::Ready;
                    continue;
                }
                let authenticated = match session.authenticated() {
                    Ok(authenticated) => authenticated,
                    Err(error) => {
                        send_bolt_failure(&mut writer, &error).await?;
                        state = BoltState::Failed;
                        continue;
                    }
                };
                let Some(pending) = session.pending.as_mut() else {
                    let error = BoltError::Protocol("no pending result".to_string());
                    send_bolt_failure(&mut writer, &error).await?;
                    state = BoltState::Failed;
                    continue;
                };
                let discard_result = {
                    let mut channels = BoltConnectionChannels {
                        message_rx: &mut message_rx,
                        queued: &mut queued,
                        writer: &mut writer,
                        max_queued_messages: config.max_pipelined_messages,
                    };
                    consume_bolt_records(
                        &context,
                        &authenticated,
                        pending,
                        count,
                        config.prefetch_rows,
                        BoltRecordDisposition::Discard,
                        &mut channels,
                    )
                    .await
                };
                match discard_result {
                    Err(error) => {
                        send_bolt_failure(&mut writer, &error).await?;
                        session.pending = None;
                        state = BoltState::Failed;
                    }
                    Ok(PageAwaitResult::Complete(Ok(_))) => {
                        let pending = session
                            .pending
                            .as_ref()
                            .expect("bounded DISCARD keeps pending state until summary");
                        let has_more = !pending.rows.is_empty() || pending.next_cursor.is_some();
                        send_bolt_success(
                            &mut writer,
                            bolt_result_summary_metadata(pending, has_more),
                        )
                        .await?;
                        if !has_more {
                            session.pending = None;
                            state = BoltState::Ready;
                        }
                    }
                    Ok(PageAwaitResult::Complete(Err(error))) => {
                        send_bolt_failure(&mut writer, &graph_error_to_bolt(error)).await?;
                        session.pending = None;
                        state = BoltState::Failed;
                    }
                    Ok(PageAwaitResult::Reset) => {
                        session.pending = None;
                        state = BoltState::Ready;
                    }
                    Ok(PageAwaitResult::Goodbye) => break,
                }
            }
            (
                BoltState::Ready,
                ClientMessage::Route {
                    bookmarks, extra, ..
                },
            ) => match prepare_bolt_route(&session, &context, bookmarks, &extra).await {
                Ok(routing_table) => {
                    send_bolt_success(
                        &mut writer,
                        BoltDict::from([("rt".to_string(), BoltValue::Dict(routing_table))]),
                    )
                    .await?;
                }
                Err(error) => {
                    send_bolt_failure(&mut writer, &error).await?;
                    state = BoltState::Failed;
                }
            },
            (BoltState::Ready, ClientMessage::Telemetry { .. }) => {
                send_bolt_success(&mut writer, BoltDict::new()).await?;
            }
            (
                BoltState::Ready,
                ClientMessage::Begin { .. } | ClientMessage::Commit | ClientMessage::Rollback,
            ) => {
                send_bolt_failure(&mut writer, &explicit_transactions_unsupported()).await?;
                state = BoltState::Failed;
            }
            _ => {
                send_bolt_failure(
                    &mut writer,
                    &BoltError::Protocol(
                        "message is invalid in the current Bolt state".to_string(),
                    ),
                )
                .await?;
                state = BoltState::Failed;
            }
        }
    }

    reader_task.stop().await;
    tracing::debug!(target: "slatedb_graph_kernel", %peer_addr, "Bolt session closed");
    Ok(())
}

fn bolt_client_credentials(
    auth: &BoltDict,
) -> std::result::Result<ClientQueryCredentials, BoltError> {
    let scheme = auth
        .get("scheme")
        .and_then(BoltValue::as_str)
        .unwrap_or("none");
    match scheme {
        "none" => Ok(ClientQueryCredentials::None),
        "bearer" => Ok(ClientQueryCredentials::Bearer(
            required_bolt_string(auth, "credentials")?.to_string(),
        )),
        "basic" => Ok(ClientQueryCredentials::Basic {
            principal: required_bolt_string(auth, "principal")?.to_string(),
            credentials: required_bolt_string(auth, "credentials")?.to_string(),
        }),
        _ => Err(BoltError::Authentication(format!(
            "unsupported authentication scheme {scheme}"
        ))),
    }
}

fn required_bolt_string<'a>(
    values: &'a BoltDict,
    name: &str,
) -> std::result::Result<&'a str, BoltError> {
    values
        .get(name)
        .and_then(BoltValue::as_str)
        .ok_or_else(|| BoltError::Authentication(format!("missing {name}")))
}

fn selected_bolt_database(
    session: &BoltProtocolSession,
    context: &BoltConnectionContext,
    extra: &BoltDict,
) -> std::result::Result<String, BoltError> {
    match extra.get("db") {
        Some(BoltValue::String(database)) => Ok(database.clone()),
        Some(_) => Err(BoltError::Protocol(
            "db metadata must be a string".to_string(),
        )),
        None => Ok(session
            .database
            .clone()
            .unwrap_or_else(|| context.default_database.clone())),
    }
}

fn prepare_bolt_run(
    session: &mut BoltProtocolSession,
    context: &BoltConnectionContext,
    query: String,
    parameters: BoltDict,
    extra: BoltDict,
) -> std::result::Result<
    (
        ClientQuerySession,
        String,
        ClientQueryTarget,
        ClientQueryRequest,
        String,
    ),
    BoltError,
> {
    let authenticated = session.authenticated()?;
    validate_bolt_run_extra(&extra)?;
    let database = selected_bolt_database(session, context, &extra)?;
    let target = context
        .database_resolver
        .resolve_database(Some(&database))
        .map_err(graph_error_to_bolt)?;
    let query_id = session.next_query_id();
    let mut request =
        bolt_query_request(target.clone(), query_id.clone(), query, parameters, &extra)?;
    let runtime_limit_ms = context
        .service
        .effective_runtime_limit_ms(request.max_runtime_ms)
        .map_err(graph_error_to_bolt)?;
    request.max_runtime_ms = Some(runtime_limit_ms);
    if let Some(bookmark) = highest_matching_bookmark(&target, bolt_bookmarks_from_extra(&extra)?)?
    {
        request = request.after_bookmark(bookmark);
    }
    Ok((authenticated, database, target, request, query_id))
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
        .map(|(name, value)| Ok((name.clone(), bolt_parameter_to_property(name, value)?)))
        .collect::<std::result::Result<BTreeMap<_, _>, BoltError>>()?;
    let mut request = ClientQueryRequest::new(target, query_id, query).with_parameters(parameters);
    match extra.get("tx_timeout") {
        Some(BoltValue::Integer(timeout_ms)) => {
            request = request.with_timeout_ms(u64::try_from(*timeout_ms).map_err(|_| {
                BoltError::Protocol("tx_timeout must be a non-negative integer".to_string())
            })?);
        }
        Some(_) => {
            return Err(BoltError::Protocol(
                "tx_timeout must be an integer".to_string(),
            ));
        }
        None => {}
    }
    Ok(request)
}

fn validate_bolt_run_extra(extra: &BoltDict) -> std::result::Result<(), BoltError> {
    if extra.contains_key("imp_user") {
        return Err(BoltError::Forbidden(
            "Bolt user impersonation is not supported".to_string(),
        ));
    }
    match extra.get("mode") {
        None => Ok(()),
        Some(BoltValue::String(mode)) if mode == "r" || mode == "w" => Ok(()),
        Some(BoltValue::String(_)) => Err(BoltError::Protocol(
            "mode metadata must be r or w".to_string(),
        )),
        Some(_) => Err(BoltError::Protocol(
            "mode metadata must be a string".to_string(),
        )),
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
        Some(_) => Err(BoltError::Protocol(
            "bookmarks must be a list of strings".to_string(),
        )),
    }
}

fn spawn_bolt_page(
    service: ClientQueryService,
    authenticated: ClientQuerySession,
    request: ClientQueryRequest,
    cursor: Option<QueryCursorToken>,
    page_size: usize,
) -> JoinHandle<Result<ClientQueryPage>> {
    tokio::spawn(async move {
        service
            .execute_page(&authenticated, request, cursor, page_size)
            .await
    })
}

async fn await_bolt_page<W>(
    mut task: JoinHandle<Result<ClientQueryPage>>,
    service: &ClientQueryService,
    authenticated: &ClientQuerySession,
    scope: &GraphScope,
    query_id: &str,
    channels: &mut BoltConnectionChannels<'_, W>,
) -> std::result::Result<PageAwaitResult, BoltError>
where
    W: AsyncWrite + Unpin,
{
    loop {
        tokio::select! {
            result = &mut task => {
                let result = match result {
                    Ok(result) => result,
                    Err(error) => Err(GraphError::CorruptValue {
                        key: "client/bolt/query_task".to_string(),
                        reason: error.to_string(),
                    }),
                };
                return Ok(PageAwaitResult::Complete(result));
            }
            incoming = channels.message_rx.recv() => {
                match incoming {
                    Some(Ok(ClientMessage::Reset)) => {
                        let _ = service.cancel(authenticated, scope, query_id).await;
                        task.abort();
                        let _ = task.await;
                        send_bolt_message(channels.writer, &ServerMessage::Ignored).await?;
                        while channels.queued.pop_front().is_some() {
                            send_bolt_message(channels.writer, &ServerMessage::Ignored).await?;
                        }
                        send_bolt_success(channels.writer, BoltDict::new()).await?;
                        return Ok(PageAwaitResult::Reset);
                    }
                    Some(Ok(ClientMessage::Goodbye)) | None => {
                        let _ = service.cancel(authenticated, scope, query_id).await;
                        task.abort();
                        let _ = task.await;
                        return Ok(PageAwaitResult::Goodbye);
                    }
                    Some(Ok(message)) => {
                        if channels.queued.len() >= channels.max_queued_messages {
                            task.abort();
                            let _ = task.await;
                            return Err(BoltError::ResourceExhausted(
                                "Bolt pipelined message queue is full".to_string(),
                            ));
                        }
                        channels.queued.push_back(message);
                    }
                    Some(Err(error)) => {
                        task.abort();
                        let _ = task.await;
                        return Err(error);
                    }
                }
            }
        }
    }
}

async fn consume_bolt_records<W>(
    context: &Arc<BoltConnectionContext>,
    authenticated: &ClientQuerySession,
    pending: &mut PendingBoltResult,
    count: Option<usize>,
    prefetch_rows: usize,
    disposition: BoltRecordDisposition,
    channels: &mut BoltConnectionChannels<'_, W>,
) -> std::result::Result<PageAwaitResult, BoltError>
where
    W: AsyncWrite + Unpin,
{
    let mut remaining = count.unwrap_or(usize::MAX);
    while remaining > 0 {
        if let Some(row) = pending.rows.pop_front() {
            if matches!(disposition, BoltRecordDisposition::Send) {
                let values = row
                    .values
                    .iter()
                    .map(query_value_to_bolt)
                    .collect::<std::result::Result<Vec<_>, BoltError>>()?;
                send_bolt_message(channels.writer, &ServerMessage::Record { data: values }).await?;
            }
            remaining -= 1;
            continue;
        }
        let Some(cursor) = pending.next_cursor.take() else {
            break;
        };
        pending.request.max_runtime_ms = Some(remaining_bolt_runtime_ms(pending.deadline)?);
        let fetch_size = count.map_or(prefetch_rows, |count| prefetch_rows.min(count.max(1)));
        let query_id = pending.request.query_id.clone();
        let scope = pending.request.target.scope.clone();
        let task = spawn_bolt_page(
            context.service.clone(),
            authenticated.clone(),
            pending.request.clone(),
            Some(cursor),
            fetch_size,
        );
        match await_bolt_page(
            task,
            &context.service,
            authenticated,
            &scope,
            &query_id,
            channels,
        )
        .await?
        {
            PageAwaitResult::Complete(Ok(page)) => {
                if page.page.columns != pending.columns {
                    return Ok(PageAwaitResult::Complete(Err(GraphError::CorruptValue {
                        key: "client/bolt/result_columns".to_string(),
                        reason: "result columns changed between cursor pages".to_string(),
                    })));
                }
                pending.request.read_epoch = page.read_epoch;
                pending.rows.extend(page.page.rows);
                pending.next_cursor = page.page.next_cursor;
                pending.bookmark = page.bookmark;
            }
            outcome => return Ok(outcome),
        }
    }
    Ok(PageAwaitResult::Complete(Ok(ClientQueryPage {
        query_id: pending.request.query_id.clone(),
        page: crate::QueryResultPage::new(pending.columns.clone(), Vec::new(), pending.next_cursor),
        read_epoch: pending.request.read_epoch,
        bookmark: pending.bookmark.clone(),
    })))
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
        metadata.insert(
            "db".to_string(),
            BoltValue::String(pending.database.clone()),
        );
        metadata.insert("t_last".to_string(), BoltValue::Integer(0));
        metadata.insert("type".to_string(), BoltValue::String("r".to_string()));
    }
    metadata
}

fn bolt_stream_count(
    extra: &BoltDict,
    operation: &'static str,
) -> std::result::Result<Option<usize>, BoltError> {
    if let Some(query_id) = extra.get("qid") {
        if query_id.as_int() != Some(-1) {
            return Err(BoltError::Protocol(format!(
                "{operation} qid must be -1 for an auto-commit result"
            )));
        }
    }
    let n = extra.get("n").and_then(BoltValue::as_int).unwrap_or(-1);
    if n == -1 {
        return Ok(None);
    }
    if n <= 0 {
        return Err(BoltError::Protocol(format!(
            "{operation} n must be -1 or a positive integer"
        )));
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
        return Err(BoltError::Forbidden(
            "Bolt user impersonation is not supported".to_string(),
        ));
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
    if let Some(bookmark) = highest_matching_bookmark(&target, bookmarks)? {
        context
            .service
            .ensure_bookmark(&bookmark)
            .await
            .map_err(graph_error_to_bolt)?;
    }
    let table = match &context.routing_table_provider {
        Some(provider) => provider
            .routing_table(&database, &target)
            .await
            .map_err(graph_error_to_bolt)?,
        None if !context.routing_servers.is_empty() => {
            BoltRoutingTable::new(context.routing_ttl_secs, context.routing_servers.clone())
                .map_err(graph_error_to_bolt)?
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
                    BoltValue::List(
                        server
                            .addresses
                            .iter()
                            .cloned()
                            .map(BoltValue::String)
                            .collect(),
                    ),
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

async fn send_bolt_success<W>(
    writer: &mut BoltMessageWriter<W>,
    metadata: BoltDict,
) -> std::result::Result<(), BoltError>
where
    W: AsyncWrite + Unpin,
{
    send_bolt_message(writer, &ServerMessage::Success { metadata }).await
}

async fn send_bolt_failure<W>(
    writer: &mut BoltMessageWriter<W>,
    error: &BoltError,
) -> std::result::Result<(), BoltError>
where
    W: AsyncWrite + Unpin,
{
    send_bolt_message(
        writer,
        &ServerMessage::Failure {
            metadata: error.to_failure_metadata(),
        },
    )
    .await
}

async fn send_bolt_message<W>(
    writer: &mut BoltMessageWriter<W>,
    message: &ServerMessage,
) -> std::result::Result<(), BoltError>
where
    W: AsyncWrite + Unpin,
{
    writer.send(message).await
}

fn bolt_io_error(operation: &'static str, error: std::io::Error) -> GraphError {
    GraphError::CorruptValue {
        key: format!("client/bolt/{operation}"),
        reason: error.to_string(),
    }
}

#[cfg(test)]
mod tests;
