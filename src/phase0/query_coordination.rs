use std::collections::{BTreeMap, BTreeSet};
#[cfg(feature = "query-transport")]
use std::net::SocketAddr;
use std::sync::Arc;
#[cfg(feature = "query-transport")]
use std::time::Duration;

use async_trait::async_trait;
use futures::future::join_all;
#[cfg(feature = "query-transport")]
use tokio::io::{AsyncReadExt, AsyncWriteExt};
#[cfg(feature = "query-transport")]
use tokio::net::{TcpListener, TcpStream};
#[cfg(feature = "query-transport")]
use tokio::sync::watch;
#[cfg(feature = "query-transport")]
use tokio::task::JoinHandle;

use super::{RoutedPhase0Cluster, ShardPlacement};
use crate::{
    validate_component, GraphError, QueryContext, QueryCursorToken, QueryResultPage,
    QueryResultSet, Result,
};

#[cfg(feature = "query-transport")]
const QUERY_TRANSPORT_VERSION: u16 = 1;
#[cfg(feature = "query-transport")]
const DEFAULT_QUERY_TRANSPORT_MAX_FRAME_BYTES: usize = 1 << 20;
#[cfg(feature = "query-transport")]
const DEFAULT_QUERY_TRANSPORT_TIMEOUT_MS: u64 = 30_000;

#[async_trait]
pub trait QueryCellClient: Send + Sync {
    async fn execute_cypher_rows(
        &self,
        context: QueryContext,
        query: &str,
    ) -> Result<QueryResultSet>;

    async fn execute_cypher_rows_page(
        &self,
        context: QueryContext,
        query: &str,
        cursor: Option<QueryCursorToken>,
        page_size: usize,
    ) -> Result<QueryResultPage>;
}

#[cfg(feature = "query-transport")]
pub struct TcpQueryCellClient {
    addr: SocketAddr,
    max_frame_bytes: usize,
    timeout: Duration,
}

#[cfg(feature = "query-transport")]
impl TcpQueryCellClient {
    pub fn new(addr: SocketAddr) -> Self {
        Self {
            addr,
            max_frame_bytes: DEFAULT_QUERY_TRANSPORT_MAX_FRAME_BYTES,
            timeout: Duration::from_millis(DEFAULT_QUERY_TRANSPORT_TIMEOUT_MS),
        }
    }

    pub fn with_max_frame_bytes(mut self, max_frame_bytes: usize) -> Self {
        self.max_frame_bytes = max_frame_bytes;
        self
    }

    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }
}

#[cfg(feature = "query-transport")]
pub struct TcpQueryServer {
    local_addr: SocketAddr,
    stop_tx: watch::Sender<bool>,
    task: JoinHandle<Result<()>>,
}

#[cfg(feature = "query-transport")]
impl TcpQueryServer {
    pub async fn bind(addr: SocketAddr, client: Arc<dyn QueryCellClient>) -> Result<Self> {
        Self::bind_with_max_frame_bytes(addr, client, DEFAULT_QUERY_TRANSPORT_MAX_FRAME_BYTES).await
    }

    pub async fn bind_with_max_frame_bytes(
        addr: SocketAddr,
        client: Arc<dyn QueryCellClient>,
        max_frame_bytes: usize,
    ) -> Result<Self> {
        let listener = TcpListener::bind(addr)
            .await
            .map_err(|err| transport_error("bind", err))?;
        let local_addr = listener
            .local_addr()
            .map_err(|err| transport_error("local_addr", err))?;
        let (stop_tx, mut stop_rx) = watch::channel(false);
        let task = tokio::spawn(async move {
            loop {
                tokio::select! {
                    changed = stop_rx.changed() => {
                        if changed.is_err() || *stop_rx.borrow() {
                            break;
                        }
                    }
                    accepted = listener.accept() => {
                        let (stream, _) = accepted.map_err(|err| transport_error("accept", err))?;
                        let client = Arc::clone(&client);
                        tokio::spawn(async move {
                            if let Err(err) = serve_query_transport_stream(stream, client, max_frame_bytes).await {
                                tracing::warn!(
                                    target: "slatedb_graph_kernel",
                                    error = %err,
                                    "query transport connection failed"
                                );
                            }
                        });
                    }
                }
            }
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
        match self.task.await {
            Ok(result) => result,
            Err(err) => Err(GraphError::CorruptValue {
                key: "query/transport/server".to_string(),
                reason: format!("query transport server task failed: {err}"),
            }),
        }
    }
}

#[cfg(feature = "query-transport")]
#[async_trait]
impl QueryCellClient for TcpQueryCellClient {
    async fn execute_cypher_rows(
        &self,
        context: QueryContext,
        query: &str,
    ) -> Result<QueryResultSet> {
        let request = QueryTransportRequest::Rows {
            version: QUERY_TRANSPORT_VERSION,
            context,
            query: query.to_string(),
        };
        match self.send(request).await? {
            QueryTransportResponse::Rows { result } => Ok(result),
            QueryTransportResponse::Page { .. } => Err(transport_protocol_error(
                "query/transport/rows",
                "server returned page response for rows request",
            )),
            QueryTransportResponse::Error { message } => {
                Err(transport_remote_error("query/transport/rows", message))
            }
        }
    }

    async fn execute_cypher_rows_page(
        &self,
        context: QueryContext,
        query: &str,
        cursor: Option<QueryCursorToken>,
        page_size: usize,
    ) -> Result<QueryResultPage> {
        let request = QueryTransportRequest::Page {
            version: QUERY_TRANSPORT_VERSION,
            context,
            query: query.to_string(),
            cursor,
            page_size,
        };
        match self.send(request).await? {
            QueryTransportResponse::Page { result } => Ok(result),
            QueryTransportResponse::Rows { .. } => Err(transport_protocol_error(
                "query/transport/page",
                "server returned rows response for page request",
            )),
            QueryTransportResponse::Error { message } => {
                Err(transport_remote_error("query/transport/page", message))
            }
        }
    }
}

#[cfg(feature = "query-transport")]
impl TcpQueryCellClient {
    async fn send(&self, request: QueryTransportRequest) -> Result<QueryTransportResponse> {
        let future = async {
            let mut stream = TcpStream::connect(self.addr)
                .await
                .map_err(|err| transport_error("connect", err))?;
            write_query_transport_frame(&mut stream, &request, self.max_frame_bytes).await?;
            let frame = read_query_transport_frame(&mut stream, self.max_frame_bytes).await?;
            serde_json::from_slice(&frame).map_err(|err| transport_json_error("decode", err))
        };
        tokio::time::timeout(self.timeout, future)
            .await
            .map_err(|_| GraphError::QueryTimeout {
                operation: "query_transport",
                elapsed_ms: self.timeout.as_millis().min(u128::from(u64::MAX)) as u64,
                limit_ms: self.timeout.as_millis().min(u128::from(u64::MAX)) as u64,
            })?
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DistributedQueryPageRequest {
    pub context: QueryContext,
    pub cursor: Option<QueryCursorToken>,
}

impl DistributedQueryPageRequest {
    pub fn new(context: QueryContext, cursor: Option<QueryCursorToken>) -> Self {
        Self { context, cursor }
    }
}

pub struct DistributedQueryCoordinator {
    placement: ShardPlacement,
    clients: BTreeMap<String, Arc<dyn QueryCellClient>>,
}

impl DistributedQueryCoordinator {
    pub fn new(placement: ShardPlacement) -> Self {
        Self {
            placement,
            clients: BTreeMap::new(),
        }
    }

    pub fn register_client(
        &mut self,
        node_id: impl Into<String>,
        client: Arc<dyn QueryCellClient>,
    ) -> Result<()> {
        let node_id = node_id.into();
        validate_component("node_id", &node_id)?;
        self.clients.insert(node_id, client);
        Ok(())
    }

    pub fn with_client(
        mut self,
        node_id: impl Into<String>,
        client: Arc<dyn QueryCellClient>,
    ) -> Result<Self> {
        self.register_client(node_id, client)?;
        Ok(self)
    }

    pub async fn execute_cypher_rows_many(
        &self,
        contexts: impl IntoIterator<Item = QueryContext>,
        query: &str,
    ) -> Result<BTreeMap<String, QueryResultSet>> {
        let query = Arc::new(query.to_string());
        let mut seen = BTreeSet::new();
        let mut jobs = Vec::new();
        for context in contexts {
            let cell_id = checked_unique_cell(&mut seen, &context.cell_id)?;
            let client = self.client_for_cell(&cell_id)?;
            let query = Arc::clone(&query);
            jobs.push(async move {
                let result = client.execute_cypher_rows(context, query.as_str()).await;
                (cell_id, result)
            });
        }

        let mut result_sets = BTreeMap::new();
        for (cell_id, result) in join_all(jobs).await {
            result_sets.insert(cell_id, result?);
        }
        Ok(result_sets)
    }

    pub async fn execute_cypher_rows_pages(
        &self,
        requests: impl IntoIterator<Item = DistributedQueryPageRequest>,
        query: &str,
        page_size: usize,
    ) -> Result<BTreeMap<String, QueryResultPage>> {
        let query = Arc::new(query.to_string());
        let mut seen = BTreeSet::new();
        let mut jobs = Vec::new();
        for request in requests {
            let cell_id = checked_unique_cell(&mut seen, &request.context.cell_id)?;
            let client = self.client_for_cell(&cell_id)?;
            let query = Arc::clone(&query);
            jobs.push(async move {
                let result = client
                    .execute_cypher_rows_page(
                        request.context,
                        query.as_str(),
                        request.cursor,
                        page_size,
                    )
                    .await;
                (cell_id, result)
            });
        }

        let mut pages = BTreeMap::new();
        for (cell_id, result) in join_all(jobs).await {
            pages.insert(cell_id, result?);
        }
        Ok(pages)
    }

    fn client_for_cell(&self, cell_id: &str) -> Result<Arc<dyn QueryCellClient>> {
        let owner = self.placement.owner(cell_id)?;
        self.clients
            .get(owner)
            .cloned()
            .ok_or_else(|| GraphError::CorruptValue {
                key: format!("query/node/{owner}"),
                reason: format!("missing query client for owner node {owner}"),
            })
    }
}

#[async_trait]
impl QueryCellClient for RoutedPhase0Cluster {
    async fn execute_cypher_rows(
        &self,
        context: QueryContext,
        query: &str,
    ) -> Result<QueryResultSet> {
        RoutedPhase0Cluster::execute_cypher_rows(self, context, query).await
    }

    async fn execute_cypher_rows_page(
        &self,
        context: QueryContext,
        query: &str,
        cursor: Option<QueryCursorToken>,
        page_size: usize,
    ) -> Result<QueryResultPage> {
        RoutedPhase0Cluster::execute_cypher_rows_page(self, context, query, cursor, page_size).await
    }
}

fn checked_unique_cell(seen: &mut BTreeSet<String>, cell_id: &str) -> Result<String> {
    validate_component("cell_id", cell_id)?;
    if !seen.insert(cell_id.to_string()) {
        return Err(GraphError::CorruptValue {
            key: format!("query/cell/{cell_id}"),
            reason: "duplicate cell in distributed query request".to_string(),
        });
    }
    Ok(cell_id.to_string())
}

#[cfg(feature = "query-transport")]
#[derive(serde::Deserialize, serde::Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum QueryTransportRequest {
    Rows {
        version: u16,
        context: QueryContext,
        query: String,
    },
    Page {
        version: u16,
        context: QueryContext,
        query: String,
        cursor: Option<QueryCursorToken>,
        page_size: usize,
    },
}

#[cfg(feature = "query-transport")]
#[derive(serde::Deserialize, serde::Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum QueryTransportResponse {
    Rows { result: QueryResultSet },
    Page { result: QueryResultPage },
    Error { message: String },
}

#[cfg(feature = "query-transport")]
async fn serve_query_transport_stream(
    mut stream: TcpStream,
    client: Arc<dyn QueryCellClient>,
    max_frame_bytes: usize,
) -> Result<()> {
    let frame = read_query_transport_frame(&mut stream, max_frame_bytes).await?;
    let response = match serde_json::from_slice::<QueryTransportRequest>(&frame) {
        Ok(request) => execute_query_transport_request(client, request).await,
        Err(err) => QueryTransportResponse::Error {
            message: format!("invalid query transport request: {err}"),
        },
    };
    write_query_transport_frame(&mut stream, &response, max_frame_bytes).await
}

#[cfg(feature = "query-transport")]
async fn execute_query_transport_request(
    client: Arc<dyn QueryCellClient>,
    request: QueryTransportRequest,
) -> QueryTransportResponse {
    match request {
        QueryTransportRequest::Rows {
            version,
            context,
            query,
        } => {
            if version != QUERY_TRANSPORT_VERSION {
                return transport_version_error(version);
            }
            match client.execute_cypher_rows(context, &query).await {
                Ok(result) => QueryTransportResponse::Rows { result },
                Err(err) => QueryTransportResponse::Error {
                    message: err.to_string(),
                },
            }
        }
        QueryTransportRequest::Page {
            version,
            context,
            query,
            cursor,
            page_size,
        } => {
            if version != QUERY_TRANSPORT_VERSION {
                return transport_version_error(version);
            }
            match client
                .execute_cypher_rows_page(context, &query, cursor, page_size)
                .await
            {
                Ok(result) => QueryTransportResponse::Page { result },
                Err(err) => QueryTransportResponse::Error {
                    message: err.to_string(),
                },
            }
        }
    }
}

#[cfg(feature = "query-transport")]
fn transport_version_error(version: u16) -> QueryTransportResponse {
    QueryTransportResponse::Error {
        message: format!(
            "unsupported query transport version {version}; expected {QUERY_TRANSPORT_VERSION}"
        ),
    }
}

#[cfg(feature = "query-transport")]
async fn write_query_transport_frame<T: serde::Serialize>(
    stream: &mut TcpStream,
    message: &T,
    max_frame_bytes: usize,
) -> Result<()> {
    let mut bytes =
        serde_json::to_vec(message).map_err(|err| transport_json_error("encode", err))?;
    if bytes.len() >= max_frame_bytes {
        return Err(GraphError::AdmissionRejected {
            operation: "query_transport_frame",
            actual: bytes.len() as u64,
            limit: max_frame_bytes as u64,
        });
    }
    bytes.push(b'\n');
    stream
        .write_all(&bytes)
        .await
        .map_err(|err| transport_error("write", err))
}

#[cfg(feature = "query-transport")]
async fn read_query_transport_frame(
    stream: &mut TcpStream,
    max_frame_bytes: usize,
) -> Result<Vec<u8>> {
    let mut frame = Vec::new();
    let mut buf = [0_u8; 4096];
    loop {
        let read = stream
            .read(&mut buf)
            .await
            .map_err(|err| transport_error("read", err))?;
        if read == 0 {
            break;
        }
        if let Some(newline) = buf[..read].iter().position(|byte| *byte == b'\n') {
            frame.extend_from_slice(&buf[..newline]);
            break;
        }
        frame.extend_from_slice(&buf[..read]);
        if frame.len() > max_frame_bytes {
            return Err(GraphError::AdmissionRejected {
                operation: "query_transport_frame",
                actual: frame.len() as u64,
                limit: max_frame_bytes as u64,
            });
        }
    }
    if frame.is_empty() {
        return Err(transport_protocol_error(
            "query/transport/frame",
            "empty query transport frame",
        ));
    }
    if frame.len() > max_frame_bytes {
        return Err(GraphError::AdmissionRejected {
            operation: "query_transport_frame",
            actual: frame.len() as u64,
            limit: max_frame_bytes as u64,
        });
    }
    Ok(frame)
}

#[cfg(feature = "query-transport")]
fn transport_error(operation: &'static str, err: std::io::Error) -> GraphError {
    GraphError::CorruptValue {
        key: format!("query/transport/{operation}"),
        reason: err.to_string(),
    }
}

#[cfg(feature = "query-transport")]
fn transport_json_error(operation: &'static str, err: serde_json::Error) -> GraphError {
    GraphError::CorruptValue {
        key: format!("query/transport/{operation}"),
        reason: err.to_string(),
    }
}

#[cfg(feature = "query-transport")]
fn transport_protocol_error(key: &str, reason: &str) -> GraphError {
    GraphError::CorruptValue {
        key: key.to_string(),
        reason: reason.to_string(),
    }
}

#[cfg(feature = "query-transport")]
fn transport_remote_error(key: &str, message: String) -> GraphError {
    GraphError::UnsupportedQuery {
        dialect: "QueryTransport",
        feature: format!("{key}: {message}"),
    }
}
