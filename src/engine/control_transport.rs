use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{watch, Semaphore};
use tokio::task::{JoinHandle, JoinSet};
use tokio_rustls::rustls::pki_types::ServerName;
use tokio_rustls::{TlsAcceptor, TlsConnector};

use super::*;
use crate::{QueryTransportTlsClientConfigProvider, QueryTransportTlsServerConfigProvider};

const CONTROL_RPC_VERSION: u16 = 1;
const DEFAULT_MAX_FRAME_BYTES: usize = 1024 * 1024;
const DEFAULT_MAX_CONCURRENT_REQUESTS: usize = 256;

#[derive(Clone)]
pub struct GraphControlRpcClientConfig {
    pub max_frame_bytes: usize,
    pub request_timeout: Duration,
    pub server_name: String,
    pub tls_config_provider: Option<Arc<dyn QueryTransportTlsClientConfigProvider>>,
    allow_plaintext: bool,
}

impl GraphControlRpcClientConfig {
    pub fn new(
        server_name: impl Into<String>,
        tls_config_provider: Arc<dyn QueryTransportTlsClientConfigProvider>,
    ) -> Self {
        Self {
            max_frame_bytes: DEFAULT_MAX_FRAME_BYTES,
            request_timeout: Duration::from_secs(5),
            server_name: server_name.into(),
            tls_config_provider: Some(tls_config_provider),
            allow_plaintext: false,
        }
    }

    pub fn with_request_timeout(mut self, timeout: Duration) -> Self {
        self.request_timeout = timeout;
        self
    }

    pub fn with_max_frame_bytes(mut self, max_frame_bytes: usize) -> Self {
        self.max_frame_bytes = max_frame_bytes;
        self
    }

    pub fn insecure_allow_plaintext() -> Self {
        Self {
            max_frame_bytes: DEFAULT_MAX_FRAME_BYTES,
            request_timeout: Duration::from_secs(5),
            server_name: "localhost".to_string(),
            tls_config_provider: None,
            allow_plaintext: true,
        }
    }

    fn validate(&self) -> Result<()> {
        validate_control_rpc_limits(
            self.max_frame_bytes,
            self.request_timeout,
            "control_rpc_client",
        )?;
        if self.tls_config_provider.is_none() && !self.allow_plaintext {
            return control_rpc_config_error("control RPC client requires mTLS configuration");
        }
        if self.server_name.trim().is_empty() {
            return control_rpc_config_error("control RPC TLS server name cannot be empty");
        }
        Ok(())
    }
}

#[derive(Clone)]
pub struct GraphControlRpcServerConfig {
    pub max_frame_bytes: usize,
    pub request_timeout: Duration,
    pub max_concurrent_requests: usize,
    pub tls_config_provider: Option<Arc<dyn QueryTransportTlsServerConfigProvider>>,
    allow_plaintext: bool,
}

impl GraphControlRpcServerConfig {
    pub fn new(tls_config_provider: Arc<dyn QueryTransportTlsServerConfigProvider>) -> Self {
        Self {
            max_frame_bytes: DEFAULT_MAX_FRAME_BYTES,
            request_timeout: Duration::from_secs(5),
            max_concurrent_requests: DEFAULT_MAX_CONCURRENT_REQUESTS,
            tls_config_provider: Some(tls_config_provider),
            allow_plaintext: false,
        }
    }

    pub fn with_request_timeout(mut self, timeout: Duration) -> Self {
        self.request_timeout = timeout;
        self
    }

    pub fn with_max_concurrent_requests(mut self, limit: usize) -> Self {
        self.max_concurrent_requests = limit;
        self
    }

    pub fn insecure_allow_plaintext() -> Self {
        Self {
            max_frame_bytes: DEFAULT_MAX_FRAME_BYTES,
            request_timeout: Duration::from_secs(5),
            max_concurrent_requests: DEFAULT_MAX_CONCURRENT_REQUESTS,
            tls_config_provider: None,
            allow_plaintext: true,
        }
    }

    fn validate(&self) -> Result<()> {
        validate_control_rpc_limits(
            self.max_frame_bytes,
            self.request_timeout,
            "control_rpc_server",
        )?;
        if self.max_concurrent_requests == 0 {
            return control_rpc_config_error("control RPC concurrency limit must be nonzero");
        }
        if self.tls_config_provider.is_none() && !self.allow_plaintext {
            return control_rpc_config_error("control RPC server requires mTLS configuration");
        }
        Ok(())
    }
}

#[derive(Clone)]
pub struct GraphControlRpcClient {
    endpoint: String,
    scope: GraphScope,
    config: GraphControlRpcClientConfig,
}

impl GraphControlRpcClient {
    pub fn new(
        endpoint: impl std::fmt::Display,
        scope: GraphScope,
        config: GraphControlRpcClientConfig,
    ) -> Result<Self> {
        config.validate()?;
        let endpoint = endpoint.to_string().trim().to_string();
        validate_control_rpc_endpoint(&endpoint)?;
        Ok(Self {
            endpoint,
            scope,
            config,
        })
    }

    async fn call(&self, operation: ControlOperation) -> Result<ControlPayload> {
        let request = ControlRequest {
            version: CONTROL_RPC_VERSION,
            scope: self.scope.to_string(),
            operation,
        };
        let future =
            async {
                let stream = TcpStream::connect(self.endpoint.as_str())
                    .await
                    .map_err(|error| control_rpc_io_error("connect", error))?;
                stream
                    .set_nodelay(true)
                    .map_err(|error| control_rpc_io_error("set_nodelay", error))?;
                if let Some(provider) = &self.config.tls_config_provider {
                    let server_name = ServerName::try_from(self.config.server_name.clone())
                        .map_err(|error| GraphError::CorruptValue {
                            key: "control/rpc/server_name".to_string(),
                            reason: error.to_string(),
                        })?;
                    let config = provider.current_client_config()?;
                    let mut stream = TlsConnector::from(config)
                        .connect(server_name, stream)
                        .await
                        .map_err(|error| control_rpc_io_error("tls_connect", error))?;
                    write_frame(&mut stream, &request, self.config.max_frame_bytes).await?;
                    read_frame(&mut stream, self.config.max_frame_bytes).await
                } else {
                    let mut stream = stream;
                    write_frame(&mut stream, &request, self.config.max_frame_bytes).await?;
                    read_frame(&mut stream, self.config.max_frame_bytes).await
                }
            };
        let response: ControlResponse = tokio::time::timeout(self.config.request_timeout, future)
            .await
            .map_err(|_| GraphError::QueryTimeout {
                operation: "control_rpc",
                elapsed_ms: duration_millis(self.config.request_timeout),
                limit_ms: duration_millis(self.config.request_timeout),
            })??;
        if response.version != CONTROL_RPC_VERSION {
            return control_rpc_config_error("control RPC response version mismatch");
        }
        match response.result {
            Ok(payload) => Ok(payload),
            Err(error) => Err(error.into_graph_error()),
        }
    }
}

#[async_trait]
impl GraphControlClient for GraphControlRpcClient {
    fn scope(&self) -> &GraphScope {
        &self.scope
    }

    async fn load_placement(&self) -> Result<ShardPlacement> {
        expect_payload(
            self.call(ControlOperation::LoadPlacement).await?,
            |payload| {
                if let ControlPayload::Placement(value) = payload {
                    Some(value)
                } else {
                    None
                }
            },
        )
    }

    async fn acquire_lease(
        &self,
        cell_id: &str,
        node_id: &str,
        ttl: Duration,
    ) -> Result<ShardLease> {
        expect_payload(
            self.call(ControlOperation::AcquireLease {
                cell_id: cell_id.to_string(),
                node_id: node_id.to_string(),
                ttl_ms: duration_millis(ttl),
            })
            .await?,
            |payload| {
                if let ControlPayload::Lease(value) = payload {
                    Some(value)
                } else {
                    None
                }
            },
        )
    }

    async fn renew_lease(&self, lease: &ShardLease, ttl: Duration) -> Result<ShardLease> {
        expect_payload(
            self.call(ControlOperation::RenewLease {
                lease: lease.clone(),
                ttl_ms: duration_millis(ttl),
            })
            .await?,
            |payload| {
                if let ControlPayload::Lease(value) = payload {
                    Some(value)
                } else {
                    None
                }
            },
        )
    }

    async fn release_lease(&self, lease: &ShardLease) -> Result<bool> {
        expect_payload(
            self.call(ControlOperation::ReleaseLease {
                lease: lease.clone(),
            })
            .await?,
            |payload| {
                if let ControlPayload::Released(value) = payload {
                    Some(value)
                } else {
                    None
                }
            },
        )
    }

    async fn drop_cell_control_state(
        &self,
        cell_id: &str,
        expected_lease: Option<&ShardLease>,
    ) -> Result<()> {
        expect_payload(
            self.call(ControlOperation::DropCell {
                cell_id: cell_id.to_string(),
                expected_lease: expected_lease.cloned(),
            })
            .await?,
            |payload| {
                if matches!(payload, ControlPayload::Unit) {
                    Some(())
                } else {
                    None
                }
            },
        )
    }

    async fn publish_node_heartbeat(
        &self,
        node_id: &str,
        state: GraphNodeHealthState,
    ) -> Result<GraphNodeHeartbeat> {
        expect_payload(
            self.call(ControlOperation::Heartbeat {
                node_id: node_id.to_string(),
                state,
            })
            .await?,
            |payload| {
                if let ControlPayload::Heartbeat(value) = payload {
                    Some(value)
                } else {
                    None
                }
            },
        )
    }

    async fn load_node_heartbeats(&self) -> Result<Vec<GraphNodeHeartbeat>> {
        expect_payload(
            self.call(ControlOperation::LoadHeartbeats).await?,
            |payload| {
                if let ControlPayload::Heartbeats(value) = payload {
                    Some(value)
                } else {
                    None
                }
            },
        )
    }

    async fn current_lease(&self, cell_id: &str) -> Result<Option<ShardLease>> {
        expect_payload(
            self.call(ControlOperation::CurrentLease {
                cell_id: cell_id.to_string(),
            })
            .await?,
            |payload| {
                if let ControlPayload::OptionalLease(value) = payload {
                    Some(value)
                } else {
                    None
                }
            },
        )
    }

    async fn metrics(&self) -> Result<GraphControlMetricsSnapshot> {
        expect_payload(self.call(ControlOperation::Metrics).await?, |payload| {
            if let ControlPayload::Metrics(value) = payload {
                Some(value)
            } else {
                None
            }
        })
    }
}

pub struct GraphControlRpcServer {
    local_addr: SocketAddr,
    stop_tx: watch::Sender<bool>,
    task: JoinHandle<Result<()>>,
}

impl GraphControlRpcServer {
    pub async fn bind(
        addr: SocketAddr,
        control: Arc<GraphControlPlane>,
        config: GraphControlRpcServerConfig,
    ) -> Result<Self> {
        config.validate()?;
        let listener = TcpListener::bind(addr)
            .await
            .map_err(|error| control_rpc_io_error("bind", error))?;
        let local_addr = listener
            .local_addr()
            .map_err(|error| control_rpc_io_error("local_addr", error))?;
        let config = Arc::new(config);
        let permits = Arc::new(Semaphore::new(config.max_concurrent_requests));
        let (stop_tx, mut stop_rx) = watch::channel(false);
        let task = tokio::spawn(async move {
            let mut connections = JoinSet::new();
            loop {
                tokio::select! {
                    changed = stop_rx.changed() => {
                        if changed.is_err() || *stop_rx.borrow() {
                            break;
                        }
                    }
                    accepted = listener.accept() => {
                        let (stream, _) = accepted.map_err(|error| control_rpc_io_error("accept", error))?;
                        let Ok(permit) = Arc::clone(&permits).try_acquire_owned() else {
                            tracing::warn!("control RPC connection rejected by admission control");
                            continue;
                        };
                        let control = Arc::clone(&control);
                        let config = Arc::clone(&config);
                        connections.spawn(async move {
                            let _permit = permit;
                            if let Err(error) = serve_connection(stream, control, config).await {
                                tracing::warn!(error = %error, "control RPC connection failed");
                            }
                        });
                    }
                    completed = connections.join_next(), if !connections.is_empty() => {
                        if let Some(Err(error)) = completed {
                            tracing::warn!(error = %error, "control RPC connection task panicked");
                        }
                    }
                }
            }
            connections.abort_all();
            while connections.join_next().await.is_some() {}
            Ok(())
        });
        Ok(Self {
            local_addr,
            stop_tx,
            task,
        })
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    pub async fn stop(self) -> Result<()> {
        let _ = self.stop_tx.send(true);
        self.task.await.map_err(|error| GraphError::CorruptValue {
            key: "control/rpc/server".to_string(),
            reason: error.to_string(),
        })?
    }
}

async fn serve_connection(
    stream: TcpStream,
    control: Arc<GraphControlPlane>,
    config: Arc<GraphControlRpcServerConfig>,
) -> Result<()> {
    stream
        .set_nodelay(true)
        .map_err(|error| control_rpc_io_error("set_nodelay", error))?;
    let future = async {
        if let Some(provider) = &config.tls_config_provider {
            let tls_config = provider.current_server_config()?;
            let mut stream = TlsAcceptor::from(tls_config)
                .accept(stream)
                .await
                .map_err(|error| control_rpc_io_error("tls_accept", error))?;
            process_request(&mut stream, &control, config.max_frame_bytes).await
        } else {
            let mut stream = stream;
            process_request(&mut stream, &control, config.max_frame_bytes).await
        }
    };
    tokio::time::timeout(config.request_timeout, future)
        .await
        .map_err(|_| GraphError::QueryTimeout {
            operation: "control_rpc_server",
            elapsed_ms: duration_millis(config.request_timeout),
            limit_ms: duration_millis(config.request_timeout),
        })?
}

async fn process_request<S>(
    stream: &mut S,
    control: &GraphControlPlane,
    max_frame_bytes: usize,
) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let request: ControlRequest = read_frame(stream, max_frame_bytes).await?;
    let result = if request.version != CONTROL_RPC_VERSION {
        Err(control_rpc_config_error_value(
            "unsupported control RPC version",
        ))
    } else if request.scope != control.scope().to_string() {
        Err(WireError::from_graph_error(
            GraphError::GraphScopeMismatch {
                expected: control.scope().to_string(),
                actual: request.scope,
            },
        ))
    } else {
        execute_operation(control, request.operation)
            .await
            .map_err(WireError::from_graph_error)
    };
    let response = ControlResponse {
        version: CONTROL_RPC_VERSION,
        result,
    };
    write_frame(stream, &response, max_frame_bytes).await
}

async fn execute_operation(
    control: &GraphControlPlane,
    operation: ControlOperation,
) -> Result<ControlPayload> {
    match operation {
        ControlOperation::LoadPlacement => control
            .load_placement()
            .await
            .map(ControlPayload::Placement),
        ControlOperation::AcquireLease {
            cell_id,
            node_id,
            ttl_ms,
        } => control
            .acquire_lease(&cell_id, &node_id, Duration::from_millis(ttl_ms))
            .await
            .map(ControlPayload::Lease),
        ControlOperation::RenewLease { lease, ttl_ms } => control
            .renew_lease(&lease, Duration::from_millis(ttl_ms))
            .await
            .map(ControlPayload::Lease),
        ControlOperation::ReleaseLease { lease } => control
            .release_lease(&lease)
            .await
            .map(ControlPayload::Released),
        ControlOperation::DropCell {
            cell_id,
            expected_lease,
        } => control
            .drop_cell_control_state(&cell_id, expected_lease.as_ref())
            .await
            .map(|_| ControlPayload::Unit),
        ControlOperation::Heartbeat { node_id, state } => control
            .publish_node_heartbeat(&node_id, state)
            .await
            .map(ControlPayload::Heartbeat),
        ControlOperation::LoadHeartbeats => control
            .load_node_heartbeats()
            .await
            .map(ControlPayload::Heartbeats),
        ControlOperation::CurrentLease { cell_id } => control
            .current_lease(&cell_id)
            .await
            .map(ControlPayload::OptionalLease),
        ControlOperation::Metrics => Ok(ControlPayload::Metrics(control.graph_control_metrics())),
    }
}

#[derive(Serialize, Deserialize)]
struct ControlRequest {
    version: u16,
    scope: String,
    operation: ControlOperation,
}

#[derive(Serialize, Deserialize)]
enum ControlOperation {
    LoadPlacement,
    AcquireLease {
        cell_id: String,
        node_id: String,
        ttl_ms: u64,
    },
    RenewLease {
        lease: ShardLease,
        ttl_ms: u64,
    },
    ReleaseLease {
        lease: ShardLease,
    },
    DropCell {
        cell_id: String,
        expected_lease: Option<ShardLease>,
    },
    Heartbeat {
        node_id: String,
        state: GraphNodeHealthState,
    },
    LoadHeartbeats,
    CurrentLease {
        cell_id: String,
    },
    Metrics,
}

#[derive(Serialize, Deserialize)]
struct ControlResponse {
    version: u16,
    result: std::result::Result<ControlPayload, WireError>,
}

#[derive(Serialize, Deserialize)]
enum ControlPayload {
    Placement(ShardPlacement),
    Lease(ShardLease),
    OptionalLease(Option<ShardLease>),
    Released(bool),
    Heartbeat(GraphNodeHeartbeat),
    Heartbeats(Vec<GraphNodeHeartbeat>),
    Metrics(GraphControlMetricsSnapshot),
    Unit,
}

#[derive(Serialize, Deserialize)]
struct WireError {
    kind: String,
    message: String,
    cell_id: Option<String>,
    node_id: Option<String>,
    lease_token: Option<u64>,
    key: Option<String>,
    reason: Option<String>,
}

impl WireError {
    fn from_graph_error(error: GraphError) -> Self {
        if let GraphError::StaleShardLease {
            cell_id,
            node_id,
            lease_token,
        } = error
        {
            return Self {
                kind: "stale_shard_lease".to_string(),
                message: "stale shard lease".to_string(),
                cell_id: Some(cell_id),
                node_id: Some(node_id),
                lease_token: Some(lease_token),
                key: None,
                reason: None,
            };
        }
        if let GraphError::CorruptValue { key, reason } = error {
            return Self {
                kind: "corrupt_value".to_string(),
                message: reason.clone(),
                cell_id: None,
                node_id: None,
                lease_token: None,
                key: Some(key),
                reason: Some(reason),
            };
        }
        Self {
            kind: "remote".to_string(),
            message: error.to_string(),
            cell_id: None,
            node_id: None,
            lease_token: None,
            key: None,
            reason: None,
        }
    }

    fn into_graph_error(self) -> GraphError {
        if self.kind == "stale_shard_lease" {
            return GraphError::StaleShardLease {
                cell_id: self.cell_id.unwrap_or_default(),
                node_id: self.node_id.unwrap_or_default(),
                lease_token: self.lease_token.unwrap_or_default(),
            };
        }
        if self.kind == "corrupt_value" {
            return GraphError::CorruptValue {
                key: self.key.unwrap_or_else(|| "control/rpc/remote".to_string()),
                reason: self.reason.unwrap_or(self.message),
            };
        }
        GraphError::CorruptValue {
            key: "control/rpc/remote".to_string(),
            reason: self.message,
        }
    }
}

async fn write_frame<S, T>(stream: &mut S, value: &T, max_frame_bytes: usize) -> Result<()>
where
    S: AsyncWrite + Unpin,
    T: Serialize,
{
    let bytes = serde_json::to_vec(value).map_err(control_rpc_json_error)?;
    if bytes.is_empty() || bytes.len() > max_frame_bytes || bytes.len() > u32::MAX as usize {
        return Err(GraphError::AdmissionRejected {
            operation: "control_rpc_frame_write",
            actual: bytes.len() as u64,
            limit: max_frame_bytes as u64,
        });
    }
    stream
        .write_u32(bytes.len() as u32)
        .await
        .map_err(|error| control_rpc_io_error("write_length", error))?;
    stream
        .write_all(&bytes)
        .await
        .map_err(|error| control_rpc_io_error("write_body", error))?;
    stream
        .flush()
        .await
        .map_err(|error| control_rpc_io_error("flush", error))
}

async fn read_frame<S, T>(stream: &mut S, max_frame_bytes: usize) -> Result<T>
where
    S: AsyncRead + Unpin,
    T: for<'de> Deserialize<'de>,
{
    let length = stream
        .read_u32()
        .await
        .map_err(|error| control_rpc_io_error("read_length", error))? as usize;
    if length == 0 || length > max_frame_bytes {
        return Err(GraphError::AdmissionRejected {
            operation: "control_rpc_frame_read",
            actual: length as u64,
            limit: max_frame_bytes as u64,
        });
    }
    let mut bytes = vec![0; length];
    stream
        .read_exact(&mut bytes)
        .await
        .map_err(|error| control_rpc_io_error("read_body", error))?;
    serde_json::from_slice(&bytes).map_err(control_rpc_json_error)
}

fn expect_payload<T>(
    payload: ControlPayload,
    extract: impl FnOnce(ControlPayload) -> Option<T>,
) -> Result<T> {
    extract(payload).ok_or_else(|| GraphError::CorruptValue {
        key: "control/rpc/response".to_string(),
        reason: "control RPC response payload did not match request".to_string(),
    })
}

fn validate_control_rpc_limits(max_frame_bytes: usize, timeout: Duration, key: &str) -> Result<()> {
    if max_frame_bytes < 128 || timeout.is_zero() {
        return control_rpc_config_error(&format!(
            "{key} requires max_frame_bytes >= 128 and a nonzero timeout"
        ));
    }
    Ok(())
}

fn duration_millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

fn control_rpc_config_error<T>(reason: &str) -> Result<T> {
    Err(GraphError::CorruptValue {
        key: "control/rpc/config".to_string(),
        reason: reason.to_string(),
    })
}

fn validate_control_rpc_endpoint(endpoint: &str) -> Result<()> {
    let endpoint = endpoint.trim();
    let Some((host, port)) = endpoint.rsplit_once(':') else {
        return control_rpc_config_error("control RPC endpoint must use host:port");
    };
    if host.is_empty()
        || host.chars().any(char::is_whitespace)
        || host.contains('/')
        || (host.contains(':')
            && !(host.starts_with('[')
                && host.ends_with(']')
                && host[1..host.len() - 1]
                    .parse::<std::net::Ipv6Addr>()
                    .is_ok()))
    {
        return control_rpc_config_error("control RPC endpoint has an invalid host");
    }
    let port = port
        .parse::<u16>()
        .map_err(|error| GraphError::CorruptValue {
            key: "control/rpc/endpoint".to_string(),
            reason: format!("invalid port: {error}"),
        })?;
    if port == 0 {
        return control_rpc_config_error("control RPC endpoint port must be nonzero");
    }
    Ok(())
}

fn control_rpc_config_error_value(reason: &str) -> WireError {
    WireError {
        kind: "protocol".to_string(),
        message: reason.to_string(),
        cell_id: None,
        node_id: None,
        lease_token: None,
        key: None,
        reason: None,
    }
}

fn control_rpc_io_error(operation: &str, error: impl std::fmt::Display) -> GraphError {
    GraphError::CorruptValue {
        key: format!("control/rpc/{operation}"),
        reason: error.to_string(),
    }
}

fn control_rpc_json_error(error: serde_json::Error) -> GraphError {
    GraphError::CorruptValue {
        key: "control/rpc/json".to_string(),
        reason: error.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use slatedb::object_store::memory::InMemory;

    #[test]
    fn control_rpc_client_accepts_dns_endpoint_and_rejects_urls() {
        let client = GraphControlRpcClient::new(
            "  graph-controller.namespace.svc.cluster.local:9443  ",
            GraphScope::default(),
            GraphControlRpcClientConfig::insecure_allow_plaintext(),
        )
        .unwrap();
        assert_eq!(
            client.endpoint,
            "graph-controller.namespace.svc.cluster.local:9443"
        );

        for endpoint in [
            "https://graph-controller:9443",
            "graph-controller",
            "graph-controller:not-a-port",
            "graph-controller:0",
        ] {
            assert!(GraphControlRpcClient::new(
                endpoint,
                GraphScope::default(),
                GraphControlRpcClientConfig::insecure_allow_plaintext(),
            )
            .is_err());
        }
    }

    #[tokio::test]
    async fn remote_control_client_preserves_scope_and_stale_lease_errors() {
        let object_store: Arc<dyn slatedb::object_store::ObjectStore> = Arc::new(InMemory::new());
        let control = Arc::new(
            GraphControlPlane::open("control-rpc", object_store)
                .await
                .unwrap(),
        );
        let placement = ShardPlacement::fixed([("cell-a", "node-a")]).unwrap();
        control.publish_placement(&placement).await.unwrap();
        let server = GraphControlRpcServer::bind(
            "127.0.0.1:0".parse().unwrap(),
            Arc::clone(&control),
            GraphControlRpcServerConfig::insecure_allow_plaintext(),
        )
        .await
        .unwrap();
        let client = GraphControlRpcClient::new(
            format!("localhost:{}", server.local_addr().port()),
            GraphScope::default(),
            GraphControlRpcClientConfig::insecure_allow_plaintext(),
        )
        .unwrap();

        assert_eq!(client.load_placement().await.unwrap(), placement);
        let heartbeat = client
            .publish_node_heartbeat("node-a", GraphNodeHealthState::Active)
            .await
            .unwrap();
        assert_eq!(heartbeat.node_id, "node-a");
        let old = client
            .acquire_lease("cell-a", "node-a", Duration::from_millis(5))
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(10)).await;
        let replacement = control
            .failover_expired_cell("cell-a", "node-b", Duration::from_secs(30))
            .await
            .unwrap();
        assert!(replacement.lease_token > old.lease_token);
        assert!(matches!(
            client.renew_lease(&old, Duration::from_secs(30)).await,
            Err(GraphError::StaleShardLease { cell_id, node_id, lease_token })
                if cell_id == "cell-a" && node_id == "node-a" && lease_token == old.lease_token
        ));

        server.stop().await.unwrap();
        control.close().await.unwrap();
    }

    #[tokio::test]
    async fn remote_control_server_rejects_wrong_graph_scope() {
        let object_store: Arc<dyn slatedb::object_store::ObjectStore> = Arc::new(InMemory::new());
        let scope = GraphScope::default();
        let control = Arc::new(
            GraphControlPlane::open_scoped("control-rpc-scope", object_store, scope)
                .await
                .unwrap(),
        );
        let server = GraphControlRpcServer::bind(
            "127.0.0.1:0".parse().unwrap(),
            Arc::clone(&control),
            GraphControlRpcServerConfig::insecure_allow_plaintext(),
        )
        .await
        .unwrap();
        let wrong_scope = GraphScope::new(
            crate::NamespacePath::root(crate::NamespaceId::new("other").unwrap()),
            crate::GraphId::new("graph").unwrap(),
        );
        let client = GraphControlRpcClient::new(
            server.local_addr(),
            wrong_scope,
            GraphControlRpcClientConfig::insecure_allow_plaintext(),
        )
        .unwrap();
        assert!(client
            .load_placement()
            .await
            .unwrap_err()
            .to_string()
            .contains("scope mismatch"));
        server.stop().await.unwrap();
        control.close().await.unwrap();
    }

    #[tokio::test]
    async fn controller_outage_revokes_writer_after_local_lease_expiry() {
        let object_store: Arc<dyn slatedb::object_store::ObjectStore> = Arc::new(InMemory::new());
        let control = Arc::new(
            GraphControlPlane::open("control-rpc-outage", Arc::clone(&object_store))
                .await
                .unwrap(),
        );
        control
            .publish_placement(&ShardPlacement::fixed([("cell-a", "node-a")]).unwrap())
            .await
            .unwrap();
        let server = GraphControlRpcServer::bind(
            "127.0.0.1:0".parse().unwrap(),
            Arc::clone(&control),
            GraphControlRpcServerConfig::insecure_allow_plaintext(),
        )
        .await
        .unwrap();
        let client = Arc::new(
            GraphControlRpcClient::new(
                server.local_addr(),
                GraphScope::default(),
                GraphControlRpcClientConfig::insecure_allow_plaintext()
                    .with_request_timeout(Duration::from_millis(25)),
            )
            .unwrap(),
        );
        let cluster = RoutedGraphCluster::open_owned_with_control(
            "control-rpc-outage-data",
            "node-a",
            client.as_ref(),
            object_store,
            Duration::from_millis(100),
        )
        .await
        .unwrap();
        let renewer = cluster
            .start_lease_renewer(
                Arc::clone(&client),
                Duration::from_millis(100),
                Duration::from_millis(10),
            )
            .unwrap();
        server.stop().await.unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            while cluster.lease("cell-a").is_some() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        assert!(matches!(
            cluster.ensure_active_write_lease("cell-a"),
            Err(GraphError::WriteRequiresLease { .. })
        ));
        renewer.stop().await.unwrap();
        cluster.close().await.unwrap();
        control.close().await.unwrap();
    }

    #[tokio::test]
    async fn remote_failover_preserves_graph_indexes_artifacts_and_cold_reader_truth() {
        let object_store: Arc<dyn slatedb::object_store::ObjectStore> = Arc::new(InMemory::new());
        let control = Arc::new(
            GraphControlPlane::open("control-rpc-correctness", Arc::clone(&object_store))
                .await
                .unwrap(),
        );
        control
            .publish_placement(&ShardPlacement::fixed([("cell-a", "node-a")]).unwrap())
            .await
            .unwrap();
        let server = GraphControlRpcServer::bind(
            "127.0.0.1:0".parse().unwrap(),
            Arc::clone(&control),
            GraphControlRpcServerConfig::insecure_allow_plaintext(),
        )
        .await
        .unwrap();
        let client = GraphControlRpcClient::new(
            server.local_addr(),
            GraphScope::default(),
            GraphControlRpcClientConfig::insecure_allow_plaintext(),
        )
        .unwrap();
        let data_path = "control-rpc-correctness-data";
        let edge_type = "FOLLOWS";
        let node_a = RoutedGraphCluster::open_owned_with_control(
            data_path,
            "node-a",
            &client,
            Arc::clone(&object_store),
            Duration::from_secs(60),
        )
        .await
        .unwrap();
        for (src, dst, key) in [(1, 2, "a-1"), (2, 3, "a-2"), (3, 1, "a-3")] {
            node_a
                .write_edge(crate::EdgeMutation {
                    cell_id: "cell-a".to_string(),
                    edge_type: edge_type.to_string(),
                    src,
                    dst,
                    idempotency_key: key.to_string(),
                })
                .await
                .unwrap();
        }
        let base_epoch = node_a
            .shard("cell-a")
            .unwrap()
            .current_epoch("cell-a")
            .await
            .unwrap();
        node_a
            .shard("cell-a")
            .unwrap()
            .build_matrix_tiles("cell-a", edge_type, base_epoch, 4)
            .await
            .unwrap();
        assert_eq!(
            node_a
                .shard("cell-a")
                .unwrap()
                .matrix_reachable_with_kernel(
                    "cell-a",
                    edge_type,
                    &[1],
                    3,
                    base_epoch,
                    crate::SparseKernelBackend::RustSparse,
                )
                .await
                .unwrap()
                .vertices,
            vec![2, 3]
        );
        let old_lease = node_a.lease("cell-a").unwrap();
        control
            .publish_placement(&ShardPlacement::fixed([("cell-a", "node-b")]).unwrap())
            .await
            .unwrap();
        control
            .failover_expired_cell_at(
                "cell-a",
                "node-b",
                Duration::from_secs(60),
                old_lease.expires_at_ms + 1,
            )
            .await
            .unwrap();
        let node_b = RoutedGraphCluster::open_owned_with_control(
            data_path,
            "node-b",
            &client,
            Arc::clone(&object_store),
            Duration::from_secs(60),
        )
        .await
        .unwrap();
        assert_eq!(
            node_b
                .shard("cell-a")
                .unwrap()
                .out_neighbors("cell-a", edge_type, 1)
                .await
                .unwrap(),
            vec![2]
        );
        node_b
            .write_edge(crate::EdgeMutation {
                cell_id: "cell-a".to_string(),
                edge_type: edge_type.to_string(),
                src: 3,
                dst: 4,
                idempotency_key: "b-add".to_string(),
            })
            .await
            .unwrap();
        node_b
            .delete_edge(crate::EdgeMutation {
                cell_id: "cell-a".to_string(),
                edge_type: edge_type.to_string(),
                src: 2,
                dst: 3,
                idempotency_key: "b-delete".to_string(),
            })
            .await
            .unwrap();
        let stale_error = node_a
            .write_edge(crate::EdgeMutation {
                cell_id: "cell-a".to_string(),
                edge_type: edge_type.to_string(),
                src: 9,
                dst: 10,
                idempotency_key: "stale-a".to_string(),
            })
            .await
            .unwrap_err();
        assert!(
            matches!(&stale_error, GraphError::StaleShardLease { .. })
                || matches!(
                    &stale_error,
                    GraphError::Slate(error) if matches!(error.kind(), ErrorKind::Closed(_))
                ),
            "unexpected stale-writer error: {stale_error:?}"
        );

        let final_epoch = node_b
            .shard("cell-a")
            .unwrap()
            .current_epoch("cell-a")
            .await
            .unwrap();
        let overlay = node_b
            .shard("cell-a")
            .unwrap()
            .matrix_reachable_with_kernel(
                "cell-a",
                edge_type,
                &[1],
                4,
                final_epoch,
                crate::SparseKernelBackend::RustSparse,
            )
            .await
            .unwrap();
        assert_eq!(overlay.vertices, vec![2]);
        assert!(overlay.delta_records_applied >= 2);
        let report = node_b
            .shard("cell-a")
            .unwrap()
            .verify_current_graph("cell-a", edge_type, 4, 16)
            .await
            .unwrap();
        assert_eq!(report.mismatch_count, 0, "{:?}", report.mismatch_samples);
        let expected_digest = report.digest;

        node_a.close().await.unwrap();
        node_b.close().await.unwrap();
        server.stop().await.unwrap();
        control.close().await.unwrap();

        let reader = GraphShard::open_with_options(
            format!(
                "{}/cell-a",
                GraphScope::default().scoped_store_path(data_path)
            ),
            object_store,
            GraphOpenOptions {
                retention_policy: crate::GraphRetentionPolicy {
                    read_lease_ttl_ms: 0,
                    ..crate::GraphRetentionPolicy::default()
                },
                ..GraphOpenOptions::default()
            },
        )
        .await
        .unwrap();
        let read_epoch = reader.current_epoch("cell-a").await.unwrap();
        assert_eq!(
            reader
                .export_live_graph_digest("cell-a", edge_type, read_epoch)
                .await
                .unwrap(),
            expected_digest
        );
        assert_eq!(
            reader
                .matrix_reachable_with_kernel(
                    "cell-a",
                    edge_type,
                    &[1],
                    4,
                    read_epoch,
                    crate::SparseKernelBackend::RustSparse,
                )
                .await
                .unwrap()
                .vertices,
            vec![2]
        );
        reader.close().await.unwrap();
    }
}
