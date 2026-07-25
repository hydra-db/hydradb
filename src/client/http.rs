use std::collections::{BTreeMap, VecDeque};
use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use axum::body::{Body, Bytes};
use axum::extract::{DefaultBodyLimit, Path, State};
use axum::http::header::{ACCEPT, AUTHORIZATION, CACHE_CONTROL, CONTENT_TYPE, WWW_AUTHENTICATE};
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use axum_server::tls_rustls::RustlsConfig;
use futures::stream;
use serde::{Deserialize, Serialize};
use tokio::task::JoinHandle;
use tokio::time::Instant;

use super::service::{
    ClientBookmark, ClientQueryCredentials, ClientQueryPage, ClientQueryRequest,
    ClientQueryService, ClientQuerySession, ClientQueryTarget, ClientReadConsistency,
};
use crate::{
    GraphError, GraphId, GraphScope, NamespaceId, NamespacePath, QueryCursorToken, QueryFloat,
    QueryParameterValue, QueryTransportConnectionIdentity, QueryTransportTlsServerConfigProvider,
    QueryValue, Result, VertexPropertyValue,
};

const GRAPH_NAMESPACE_HEADER: &str = "x-graph-namespace";
const NDJSON_CONTENT_TYPE: &str = "application/x-ndjson";
const DEFAULT_HTTP_MAX_BODY_BYTES: usize = 1024 * 1024;
const DEFAULT_HTTP_PAGE_SIZE: usize = 256;
const DEFAULT_HTTP_GRACEFUL_SHUTDOWN: Duration = Duration::from_secs(30);
const DEFAULT_TLS_RELOAD_INTERVAL: Duration = Duration::from_secs(1);

#[derive(Clone)]
pub struct HttpQueryServerConfig {
    pub max_body_bytes: usize,
    pub default_page_size: usize,
    pub graceful_shutdown_timeout: Duration,
    pub tls_reload_interval: Duration,
    pub allow_default_namespace: bool,
    pub allow_plaintext: bool,
    pub tls_config_provider: Option<Arc<dyn QueryTransportTlsServerConfigProvider>>,
}

impl Default for HttpQueryServerConfig {
    fn default() -> Self {
        Self {
            max_body_bytes: DEFAULT_HTTP_MAX_BODY_BYTES,
            default_page_size: DEFAULT_HTTP_PAGE_SIZE,
            graceful_shutdown_timeout: DEFAULT_HTTP_GRACEFUL_SHUTDOWN,
            tls_reload_interval: DEFAULT_TLS_RELOAD_INTERVAL,
            allow_default_namespace: false,
            allow_plaintext: false,
            tls_config_provider: None,
        }
    }
}

impl HttpQueryServerConfig {
    pub fn with_max_body_bytes(mut self, max_body_bytes: usize) -> Self {
        self.max_body_bytes = max_body_bytes;
        self
    }

    pub fn with_default_page_size(mut self, default_page_size: usize) -> Self {
        self.default_page_size = default_page_size;
        self
    }

    pub fn with_graceful_shutdown_timeout(mut self, timeout: Duration) -> Self {
        self.graceful_shutdown_timeout = timeout;
        self
    }

    pub fn with_tls_reload_interval(mut self, interval: Duration) -> Self {
        self.tls_reload_interval = interval;
        self
    }

    pub fn with_tls_provider(
        mut self,
        provider: Arc<dyn QueryTransportTlsServerConfigProvider>,
    ) -> Self {
        self.tls_config_provider = Some(provider);
        self
    }

    pub fn allow_default_namespace(mut self) -> Self {
        self.allow_default_namespace = true;
        self
    }

    pub fn insecure_allow_plaintext(mut self) -> Self {
        self.allow_plaintext = true;
        self
    }

    fn validate(&self, service: &ClientQueryService) -> Result<()> {
        if self.max_body_bytes == 0
            || self.default_page_size == 0
            || self.default_page_size > service.max_page_size()
        {
            return http_config_error("HTTP body and page limits are invalid");
        }
        if self.tls_reload_interval.is_zero() {
            return http_config_error("TLS reload interval must be greater than zero");
        }
        if self.tls_config_provider.is_none() && !self.allow_plaintext {
            return http_config_error(
                "HTTP query API requires TLS unless plaintext is explicitly enabled for development",
            );
        }
        Ok(())
    }
}

fn http_config_error(reason: &str) -> Result<()> {
    Err(GraphError::UnsupportedQuery {
        dialect: "HttpQueryApi",
        feature: reason.to_string(),
    })
}

pub struct HttpQueryServerHandle {
    local_addr: SocketAddr,
    handle: axum_server::Handle<SocketAddr>,
    task: Option<JoinHandle<Result<()>>>,
    tls_reload_task: Option<JoinHandle<()>>,
    graceful_shutdown_timeout: Duration,
}

impl HttpQueryServerHandle {
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    pub async fn stop(mut self) -> Result<()> {
        self.handle
            .graceful_shutdown(Some(self.graceful_shutdown_timeout));
        if let Some(task) = self.tls_reload_task.take() {
            task.abort();
            let _ = task.await;
        }
        self.task
            .take()
            .expect("HTTP server task exists until stop")
            .await
            .map_err(|err| GraphError::CorruptValue {
                key: "client/http/server".to_string(),
                reason: err.to_string(),
            })?
    }
}

impl Drop for HttpQueryServerHandle {
    fn drop(&mut self) {
        self.handle.graceful_shutdown(Some(Duration::ZERO));
        if let Some(task) = self.tls_reload_task.take() {
            task.abort();
        }
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

pub struct ClientHttpServer;

impl ClientHttpServer {
    pub async fn bind(
        addr: SocketAddr,
        service: ClientQueryService,
        config: HttpQueryServerConfig,
    ) -> Result<HttpQueryServerHandle> {
        config.validate(&service)?;
        let listener =
            std::net::TcpListener::bind(addr).map_err(|err| http_io_error("bind", err))?;
        listener
            .set_nonblocking(true)
            .map_err(|err| http_io_error("nonblocking", err))?;
        let local_addr = listener
            .local_addr()
            .map_err(|err| http_io_error("local_addr", err))?;
        let state = Arc::new(HttpServerState {
            service,
            default_page_size: config.default_page_size,
            allow_default_namespace: config.allow_default_namespace,
            next_query_id: AtomicU64::new(1),
        });
        let router = Router::new()
            .route("/healthz", get(health))
            .route("/v1/graphs/{graph_id}/query", post(execute_query))
            .route(
                "/v1/graphs/{graph_id}/queries/{query_id}/cancel",
                post(cancel_query),
            )
            .layer(DefaultBodyLimit::max(config.max_body_bytes))
            .with_state(state);
        let handle = axum_server::Handle::new();
        let (task, tls_reload_task) = match config.tls_config_provider.clone() {
            Some(provider) => {
                let generation = provider.generation();
                let rustls_config =
                    RustlsConfig::from_config(http_tls_config(provider.current_server_config()?));
                let reload_config = rustls_config.clone();
                let reload_interval = config.tls_reload_interval;
                let tls_reload_task = tokio::spawn(async move {
                    let mut generation = generation;
                    let mut interval = tokio::time::interval(reload_interval);
                    loop {
                        interval.tick().await;
                        let next_generation = provider.generation();
                        if next_generation == generation {
                            continue;
                        }
                        match provider.current_server_config() {
                            Ok(server_config) => {
                                reload_config.reload_from_config(http_tls_config(server_config));
                                generation = next_generation;
                            }
                            Err(err) => tracing::warn!(
                                target: "slatedb_graph_kernel",
                                error = %err,
                                "HTTPS TLS configuration reload failed"
                            ),
                        }
                    }
                });
                let server = axum_server::from_tcp_rustls(listener, rustls_config)
                    .map_err(|err| http_io_error("tls_server", err))?
                    .handle(handle.clone());
                let task = tokio::spawn(async move {
                    server
                        .serve(router.into_make_service())
                        .await
                        .map_err(|err| http_io_error("serve", err))
                });
                (task, Some(tls_reload_task))
            }
            None => {
                let server = axum_server::from_tcp(listener)
                    .map_err(|err| http_io_error("server", err))?
                    .handle(handle.clone());
                let task = tokio::spawn(async move {
                    server
                        .serve(router.into_make_service())
                        .await
                        .map_err(|err| http_io_error("serve", err))
                });
                (task, None)
            }
        };
        Ok(HttpQueryServerHandle {
            local_addr,
            handle,
            task: Some(task),
            tls_reload_task,
            graceful_shutdown_timeout: config.graceful_shutdown_timeout,
        })
    }
}

fn http_tls_config(
    config: Arc<tokio_rustls::rustls::ServerConfig>,
) -> Arc<tokio_rustls::rustls::ServerConfig> {
    let mut config = (*config).clone();
    config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
    Arc::new(config)
}

struct HttpServerState {
    service: ClientQueryService,
    default_page_size: usize,
    allow_default_namespace: bool,
    next_query_id: AtomicU64,
}

#[derive(Deserialize)]
struct HttpQueryRequestBody {
    cell_id: String,
    query: String,
    #[serde(default)]
    query_id: Option<String>,
    #[serde(default)]
    parameters: BTreeMap<String, serde_json::Value>,
    #[serde(default)]
    bookmark: Option<String>,
    #[serde(default)]
    read_epoch: Option<u64>,
    #[serde(default)]
    timeout_ms: Option<u64>,
    #[serde(default)]
    page_size: Option<usize>,
    #[serde(default)]
    cursor: Option<u64>,
    #[serde(default)]
    consistency: Option<ClientReadConsistency>,
}

#[derive(Serialize)]
struct HttpQueryResponseBody {
    query_id: String,
    columns: Vec<String>,
    rows: Vec<Vec<HttpQueryValue>>,
    read_epoch: Option<u64>,
    next_cursor: Option<u64>,
    bookmark: Option<String>,
}

#[derive(Clone, Serialize)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
enum HttpQueryValue {
    Null,
    VertexId(u64),
    Integer(u64),
    SignedInteger(i64),
    Float(f64),
    Boolean(bool),
    String(String),
    List(Vec<HttpQueryValue>),
}

#[derive(Serialize)]
struct HttpErrorEnvelope {
    error: HttpErrorBody,
}

#[derive(Serialize)]
struct HttpErrorBody {
    code: &'static str,
    message: String,
    /// The node that owns the cell, on a 421. Serialized only when there is one
    /// — a node that has shed its view of the fleet refuses with no hint at
    /// all, and an absent field says that where a `null` would not.
    #[serde(skip_serializing_if = "Option::is_none")]
    owner: Option<String>,
}

struct HttpApiError {
    status: StatusCode,
    code: &'static str,
    message: String,
    owner: Option<String>,
    authenticate: bool,
}

impl HttpApiError {
    fn unauthenticated() -> Self {
        Self {
            status: StatusCode::UNAUTHORIZED,
            code: "unauthenticated",
            message: "valid bearer authentication is required".to_string(),
            owner: None,
            authenticate: true,
        }
    }

    fn from_graph(error: GraphError) -> Self {
        Self::from_graph_ref(&error)
    }

    fn from_graph_ref(error: &GraphError) -> Self {
        match error {
            GraphError::GraphScopeAccessDenied { .. } => Self {
                status: StatusCode::FORBIDDEN,
                code: "permission_denied",
                message: error.to_string(),
                owner: None,
                authenticate: false,
            },
            GraphError::AdmissionRejected { .. } => Self {
                status: StatusCode::TOO_MANY_REQUESTS,
                code: "resource_exhausted",
                message: error.to_string(),
                owner: None,
                authenticate: false,
            },
            GraphError::QueryTimeout { .. } => Self {
                status: StatusCode::REQUEST_TIMEOUT,
                code: "query_timeout",
                message: error.to_string(),
                owner: None,
                authenticate: false,
            },
            // Touch point (c) for HTTP. 421 Misdirected Request is the honest
            // status: the request reached a node that cannot serve it, and the
            // fix is to send it elsewhere. Unlike a Bolt driver an HTTP client
            // has no routing table to discard, so the owner is in the body
            // rather than in a code the client is expected to already know.
            // Forwarding the write over `QueryServiceEndpoint` is the better
            // answer here, and is a deliberate follow-up.
            GraphError::NotCellWriter { owner, .. } => Self {
                status: StatusCode::MISDIRECTED_REQUEST,
                code: "not_cell_writer",
                message: error.to_string(),
                owner: owner.clone(),
                authenticate: false,
            },
            GraphError::InvalidKeyComponent { .. }
            | GraphError::GraphScopeMismatch { .. }
            | GraphError::MissingQueryParameter { .. }
            | GraphError::QueryParse { .. }
            | GraphError::SnapshotAhead { .. }
            | GraphError::UnsupportedQuery { .. } => Self {
                status: StatusCode::BAD_REQUEST,
                code: "invalid_request",
                message: error.to_string(),
                owner: None,
                authenticate: false,
            },
            _ => {
                tracing::warn!(target: "slatedb_graph_kernel", error = %error, "HTTP suppressed internal graph error");
                Self {
                    status: StatusCode::INTERNAL_SERVER_ERROR,
                    code: "internal",
                    message: "internal query execution error".to_string(),
                    owner: None,
                    authenticate: false,
                }
            }
        }
    }
}

impl IntoResponse for HttpApiError {
    fn into_response(self) -> Response {
        let mut response = (
            self.status,
            Json(HttpErrorEnvelope {
                error: HttpErrorBody {
                    code: self.code,
                    message: self.message,
                    owner: self.owner,
                },
            }),
        )
            .into_response();
        if self.authenticate {
            response.headers_mut().insert(
                WWW_AUTHENTICATE,
                HeaderValue::from_static("Bearer realm=\"slatedb-graph\""),
            );
        }
        response
            .headers_mut()
            .insert(CACHE_CONTROL, HeaderValue::from_static("no-store"));
        response
    }
}

async fn health() -> impl IntoResponse {
    Json(serde_json::json!({"status": "ok"}))
}

async fn execute_query(
    State(state): State<Arc<HttpServerState>>,
    Path(graph_id): Path<String>,
    headers: HeaderMap,
    Json(body): Json<HttpQueryRequestBody>,
) -> Response {
    match execute_query_inner(state, graph_id, &headers, body).await {
        Ok(response) => response,
        Err(error) => error.into_response(),
    }
}

async fn execute_query_inner(
    state: Arc<HttpServerState>,
    graph_id: String,
    headers: &HeaderMap,
    body: HttpQueryRequestBody,
) -> std::result::Result<Response, HttpApiError> {
    let session = authenticate_http(&state.service, headers)?;
    let scope = http_graph_scope(headers, graph_id, state.allow_default_namespace)?;
    let target = ClientQueryTarget::new(scope, body.cell_id).map_err(HttpApiError::from_graph)?;
    let query_id = body.query_id.unwrap_or_else(|| {
        format!(
            "http-query-{}",
            state.next_query_id.fetch_add(1, Ordering::Relaxed)
        )
    });
    let parameters = body
        .parameters
        .iter()
        .map(|(name, value)| Ok((name.clone(), http_parameter_to_query_value(name, value)?)))
        .collect::<std::result::Result<BTreeMap<_, _>, HttpApiError>>()?;
    let mut request =
        ClientQueryRequest::new(target, query_id, body.query).with_query_parameters(parameters);
    if let Some(consistency) = body.consistency {
        request = request.with_consistency(consistency);
    }
    if body.read_epoch.is_some() {
        return Err(HttpApiError::from_graph(GraphError::UnsupportedQuery {
            dialect: "HTTP",
            feature: "read_epoch is not a storage snapshot selector; use bookmark for causal reads"
                .to_string(),
        }));
    }
    if let Some(timeout_ms) = body.timeout_ms {
        request = request.with_timeout_ms(timeout_ms);
    }
    if let Some(bookmark) = body.bookmark {
        request = request
            .after_bookmark(ClientBookmark::parse(&bookmark).map_err(HttpApiError::from_graph)?);
    }
    let page_size = body.page_size.unwrap_or(state.default_page_size);
    let cursor = body.cursor.map(QueryCursorToken::new);
    let runtime_limit_ms = state
        .service
        .effective_runtime_limit_ms(request.max_runtime_ms)
        .map_err(HttpApiError::from_graph)?;
    request.max_runtime_ms = Some(runtime_limit_ms);
    let query_deadline = Instant::now() + Duration::from_millis(runtime_limit_ms);
    let first_page = state
        .service
        .execute_page(&session, request.clone(), cursor, page_size)
        .await
        .map_err(HttpApiError::from_graph)?;
    if accepts_ndjson(headers) {
        return ndjson_response(
            state.service.clone(),
            session,
            request,
            page_size,
            first_page,
            query_deadline,
            runtime_limit_ms,
        )
        .map_err(HttpApiError::from_graph);
    }
    let response = http_query_response(first_page).map_err(HttpApiError::from_graph)?;
    Ok(with_no_store(
        (StatusCode::OK, Json(response)).into_response(),
    ))
}

async fn cancel_query(
    State(state): State<Arc<HttpServerState>>,
    Path((graph_id, query_id)): Path<(String, String)>,
    headers: HeaderMap,
) -> Response {
    let result = async {
        let session = authenticate_http(&state.service, &headers)?;
        let scope = http_graph_scope(&headers, graph_id, state.allow_default_namespace)?;
        state
            .service
            .cancel(&session, &scope, &query_id)
            .await
            .map_err(HttpApiError::from_graph)?;
        Ok::<_, HttpApiError>(with_no_store(StatusCode::NO_CONTENT.into_response()))
    }
    .await;
    result.unwrap_or_else(IntoResponse::into_response)
}

fn authenticate_http(
    service: &ClientQueryService,
    headers: &HeaderMap,
) -> std::result::Result<ClientQuerySession, HttpApiError> {
    let value = headers
        .get(AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .ok_or_else(HttpApiError::unauthenticated)?;
    let (scheme, token) = value
        .split_once(' ')
        .ok_or_else(HttpApiError::unauthenticated)?;
    if !scheme.eq_ignore_ascii_case("bearer") || token.is_empty() {
        return Err(HttpApiError::unauthenticated());
    }
    service
        .authenticate(
            &ClientQueryCredentials::Bearer(token.to_string()),
            &QueryTransportConnectionIdentity::default(),
        )
        .map_err(|_| HttpApiError::unauthenticated())
}

fn http_graph_scope(
    headers: &HeaderMap,
    graph_id: String,
    allow_default_namespace: bool,
) -> std::result::Result<GraphScope, HttpApiError> {
    let namespace = match headers
        .get(GRAPH_NAMESPACE_HEADER)
        .and_then(|value| value.to_str().ok())
    {
        Some(namespace) => NamespacePath::new(
            namespace
                .split('/')
                .map(|segment| NamespaceId::new(segment.to_string()))
                .collect::<Result<Vec<_>>>()
                .map_err(HttpApiError::from_graph)?,
        )
        .map_err(HttpApiError::from_graph)?,
        None if allow_default_namespace => NamespacePath::default(),
        None => {
            return Err(HttpApiError {
                status: StatusCode::BAD_REQUEST,
                code: "missing_namespace",
                message: format!("{GRAPH_NAMESPACE_HEADER} header is required"),
                owner: None,
                authenticate: false,
            })
        }
    };
    Ok(GraphScope::new(
        namespace,
        GraphId::new(graph_id).map_err(HttpApiError::from_graph)?,
    ))
}

fn accepts_ndjson(headers: &HeaderMap) -> bool {
    headers
        .get_all(ACCEPT)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .any(|value| {
            value
                .split(',')
                .any(|media_type| media_type.trim().starts_with(NDJSON_CONTENT_TYPE))
        })
}

fn http_parameter_to_property(
    name: &str,
    value: &serde_json::Value,
) -> std::result::Result<VertexPropertyValue, HttpApiError> {
    match value {
        serde_json::Value::Bool(value) => Ok(VertexPropertyValue::Bool(*value)),
        serde_json::Value::Number(value) => {
            if let Some(value) = value.as_u64() {
                return Ok(VertexPropertyValue::Integer(value));
            }
            if let Some(value) = value.as_i64() {
                return Ok(VertexPropertyValue::from_i64(value));
            }
            if value.is_f64() {
                if let Some(value) = value.as_f64().filter(|value| value.is_finite()) {
                    return Ok(VertexPropertyValue::Float(QueryFloat(value)));
                }
            }
            Err(invalid_http_parameter(name))
        }
        serde_json::Value::String(value) => Ok(VertexPropertyValue::String(value.clone())),
        _ => Err(invalid_http_parameter(name)),
    }
}

fn http_parameter_to_query_value(
    name: &str,
    value: &serde_json::Value,
) -> std::result::Result<QueryParameterValue, HttpApiError> {
    http_parameter_to_query_value_at_depth(name, value, 0)
}

fn http_parameter_to_query_value_at_depth(
    name: &str,
    value: &serde_json::Value,
    depth: usize,
) -> std::result::Result<QueryParameterValue, HttpApiError> {
    if depth > 16 {
        return Err(invalid_http_parameter(name));
    }
    match value {
        serde_json::Value::Array(values) => values
            .iter()
            .map(|value| http_parameter_to_query_value_at_depth(name, value, depth + 1))
            .collect::<std::result::Result<Vec<_>, _>>()
            .map(QueryParameterValue::List),
        serde_json::Value::Object(values) => values
            .iter()
            .map(|(key, value)| {
                Ok((
                    key.clone(),
                    http_parameter_to_query_value_at_depth(name, value, depth + 1)?,
                ))
            })
            .collect::<std::result::Result<BTreeMap<_, _>, HttpApiError>>()
            .map(QueryParameterValue::Map),
        _ => http_parameter_to_property(name, value).map(QueryParameterValue::Scalar),
    }
}

fn invalid_http_parameter(name: &str) -> HttpApiError {
    HttpApiError {
        status: StatusCode::BAD_REQUEST,
        code: "invalid_parameter",
        message: format!(
            "parameter ${name} must contain booleans, signed or unsigned integers, finite floats, strings, lists, or string-keyed maps"
        ),
        owner: None,
        authenticate: false,
    }
}

fn http_query_response(page: ClientQueryPage) -> Result<HttpQueryResponseBody> {
    Ok(HttpQueryResponseBody {
        query_id: page.query_id,
        columns: page
            .page
            .columns
            .iter()
            .map(|column| column.name.clone())
            .collect(),
        rows: page
            .page
            .rows
            .iter()
            .map(|row| {
                row.values
                    .iter()
                    .map(http_query_value)
                    .collect::<Result<Vec<_>>>()
            })
            .collect::<Result<Vec<_>>>()?,
        read_epoch: page.read_epoch,
        next_cursor: page.page.next_cursor.map(|cursor| cursor.offset),
        bookmark: page.bookmark.map(|bookmark| bookmark.encode()),
    })
}

fn http_query_value(value: &QueryValue) -> Result<HttpQueryValue> {
    match value {
        QueryValue::Null => Ok(HttpQueryValue::Null),
        QueryValue::VertexId(value) => Ok(HttpQueryValue::VertexId(*value)),
        QueryValue::Count(value) => Ok(HttpQueryValue::Integer(*value)),
        QueryValue::Bool(value) => Ok(HttpQueryValue::Boolean(*value)),
        QueryValue::Float(value) if value.0.is_finite() => Ok(HttpQueryValue::Float(value.0)),
        QueryValue::Float(_) => Err(non_finite_json_error()),
        QueryValue::Property(value) => match value {
            VertexPropertyValue::Integer(value) => Ok(HttpQueryValue::Integer(*value)),
            VertexPropertyValue::SignedInteger(value) => Ok(HttpQueryValue::SignedInteger(*value)),
            VertexPropertyValue::Bool(value) => Ok(HttpQueryValue::Boolean(*value)),
            VertexPropertyValue::Float(value) if value.0.is_finite() => {
                Ok(HttpQueryValue::Float(value.0))
            }
            VertexPropertyValue::Float(_) => Err(non_finite_json_error()),
            VertexPropertyValue::String(value) => Ok(HttpQueryValue::String(value.clone())),
        },
        QueryValue::List(values) => values
            .iter()
            .map(http_query_value)
            .collect::<Result<Vec<_>>>()
            .map(HttpQueryValue::List),
    }
}

fn non_finite_json_error() -> GraphError {
    GraphError::UnsupportedQuery {
        dialect: "HttpQueryApi",
        feature: "non-finite floats cannot be represented in JSON".to_string(),
    }
}

struct NdjsonQueryState {
    service: ClientQueryService,
    session: ClientQuerySession,
    request: ClientQueryRequest,
    page_size: usize,
    next_cursor: Option<QueryCursorToken>,
    deadline: Instant,
    runtime_limit_ms: u64,
    lines: VecDeque<Bytes>,
    done: bool,
}

fn ndjson_response(
    service: ClientQueryService,
    session: ClientQuerySession,
    mut request: ClientQueryRequest,
    page_size: usize,
    first_page: ClientQueryPage,
    deadline: Instant,
    runtime_limit_ms: u64,
) -> Result<Response> {
    request.read_epoch = first_page.read_epoch;
    let mut state = NdjsonQueryState {
        service,
        session,
        request,
        page_size,
        next_cursor: first_page.page.next_cursor,
        deadline,
        runtime_limit_ms,
        lines: VecDeque::new(),
        done: false,
    };
    enqueue_ndjson_header(&mut state, &first_page)?;
    enqueue_ndjson_rows(&mut state, &first_page)?;
    if state.next_cursor.is_none() {
        enqueue_ndjson_summary(&mut state, first_page.bookmark.as_ref())?;
        state.done = true;
    }
    let stream = stream::unfold(state, |mut state| async move {
        loop {
            if let Some(line) = state.lines.pop_front() {
                return Some((Ok::<_, Infallible>(line), state));
            }
            if state.done {
                return None;
            }
            let remaining_runtime_ms =
                match remaining_http_runtime_ms(state.deadline, state.runtime_limit_ms) {
                    Ok(remaining_runtime_ms) => remaining_runtime_ms,
                    Err(error) => {
                        enqueue_ndjson_error(&mut state, &error);
                        state.done = true;
                        continue;
                    }
                };
            state.request.max_runtime_ms = Some(remaining_runtime_ms);
            let cursor = state.next_cursor.take();
            match state
                .service
                .execute_page(
                    &state.session,
                    state.request.clone(),
                    cursor,
                    state.page_size,
                )
                .await
            {
                Ok(page) => {
                    state.request.read_epoch = page.read_epoch;
                    state.next_cursor = page.page.next_cursor;
                    if let Err(error) = enqueue_ndjson_rows(&mut state, &page) {
                        enqueue_ndjson_error(&mut state, &error);
                        state.done = true;
                    } else if state.next_cursor.is_none() {
                        if let Err(error) =
                            enqueue_ndjson_summary(&mut state, page.bookmark.as_ref())
                        {
                            enqueue_ndjson_error(&mut state, &error);
                        }
                        state.done = true;
                    }
                }
                Err(error) => {
                    enqueue_ndjson_error(&mut state, &error);
                    state.done = true;
                }
            }
        }
    });
    let mut response = Response::new(Body::from_stream(stream));
    response.headers_mut().insert(
        CONTENT_TYPE,
        HeaderValue::from_static("application/x-ndjson; charset=utf-8"),
    );
    Ok(with_no_store(response))
}

fn remaining_http_runtime_ms(deadline: Instant, runtime_limit_ms: u64) -> Result<u64> {
    let remaining = deadline.saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        return Err(GraphError::QueryTimeout {
            operation: "http_ndjson_query_stream",
            elapsed_ms: runtime_limit_ms,
            limit_ms: runtime_limit_ms,
        });
    }
    Ok(u64::try_from(remaining.as_millis().max(1)).unwrap_or(u64::MAX))
}

fn with_no_store(mut response: Response) -> Response {
    response
        .headers_mut()
        .insert(CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
}

fn enqueue_ndjson_header(state: &mut NdjsonQueryState, page: &ClientQueryPage) -> Result<()> {
    enqueue_ndjson(
        state,
        &serde_json::json!({
            "type": "header",
            "query_id": page.query_id,
            "columns": page.page.columns.iter().map(|column| &column.name).collect::<Vec<_>>(),
            "read_epoch": page.read_epoch,
        }),
    )
}

fn enqueue_ndjson_rows(state: &mut NdjsonQueryState, page: &ClientQueryPage) -> Result<()> {
    for row in &page.page.rows {
        let values = row
            .values
            .iter()
            .map(http_query_value)
            .collect::<Result<Vec<_>>>()?;
        enqueue_ndjson(state, &serde_json::json!({"type": "row", "values": values}))?;
    }
    Ok(())
}

fn enqueue_ndjson_summary(
    state: &mut NdjsonQueryState,
    bookmark: Option<&ClientBookmark>,
) -> Result<()> {
    enqueue_ndjson(
        state,
        &serde_json::json!({
            "type": "summary",
            "bookmark": bookmark.map(ClientBookmark::encode),
            "has_more": false,
        }),
    )
}

fn enqueue_ndjson_error(state: &mut NdjsonQueryState, error: &GraphError) {
    let api_error = HttpApiError::from_graph_ref(error);
    let value = serde_json::json!({
        "type": "error",
        "code": api_error.code,
        "message": api_error.message,
    });
    let _ = enqueue_ndjson(state, &value);
}

fn enqueue_ndjson(state: &mut NdjsonQueryState, value: &serde_json::Value) -> Result<()> {
    let mut line = serde_json::to_vec(value).map_err(|err| GraphError::CorruptValue {
        key: "client/http/ndjson".to_string(),
        reason: err.to_string(),
    })?;
    line.push(b'\n');
    state.lines.push_back(Bytes::from(line));
    Ok(())
}

fn http_io_error(operation: &'static str, error: std::io::Error) -> GraphError {
    GraphError::CorruptValue {
        key: format!("client/http/{operation}"),
        reason: error.to_string(),
    }
}

#[cfg(test)]
mod tests;
