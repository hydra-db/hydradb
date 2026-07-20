use std::collections::{BTreeMap, VecDeque};
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::FutureExt;
use tokio::sync::{Mutex, OwnedSemaphorePermit, Semaphore};

use crate::query::coordination::{
    QueryCellClient, QueryTransportAction, QueryTransportAuthPolicy,
    QueryTransportConnectionIdentity, QueryTransportNamespaceQuotas, QueryTransportPrincipal,
    QueryTransportScopeAuthorizer, QueryTransportSecret, QueryTransportServerConfig,
};
use crate::query::opencypher::{
    classify_opencypher_query_access, parse_opencypher_mutation_query_with_parameters,
    parse_opencypher_row_query_with_parameters, parse_opencypher_unwind_batch,
    OpenCypherQueryAccess, ParsedUnwindBatchKind,
};
use crate::{
    validate_component, EdgeMetadata, GraphError, GraphId, GraphScope, NamespaceId, NamespacePath,
    QueryBatchEdge, QueryBatchOperation, QueryBatchRelationship, QueryBatchRelationshipMerge,
    QueryBatchVertex, QueryCancellationToken, QueryColumn, QueryContext, QueryCursorToken,
    QueryParameterValue, QueryResultPage, QueryResultSet, QueryRow, Result, StorageSequence,
    VertexMetadata, VertexPropertyValue,
};

const DEFAULT_MAX_QUERY_BYTES: usize = 1024 * 1024;
const DEFAULT_MAX_PARAMETERS: usize = 1024;
const DEFAULT_MAX_PAGE_SIZE: usize = 4096;
const DEFAULT_MAX_QUERY_RUNTIME_MS: u64 = 30_000;
const DEFAULT_MAX_SERVER_CURSORS: usize = 1024;
const DEFAULT_MAX_CURSOR_BUFFER_BYTES: u64 = 64 * 1024 * 1024;
const DEFAULT_CURSOR_TTL_MS: u64 = 60_000;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClientQueryTarget {
    pub scope: GraphScope,
    pub cell_id: String,
}

impl ClientQueryTarget {
    pub fn new(scope: GraphScope, cell_id: impl Into<String>) -> Result<Self> {
        let cell_id = cell_id.into();
        validate_component("cell_id", &cell_id)?;
        Ok(Self { scope, cell_id })
    }
}

pub trait ClientDatabaseResolver: Send + Sync {
    fn resolve_database(&self, database: Option<&str>) -> Result<ClientQueryTarget>;
}

#[derive(Clone, Default)]
pub struct StaticClientDatabaseResolver {
    targets: BTreeMap<String, ClientQueryTarget>,
    default_database: Option<String>,
}

impl StaticClientDatabaseResolver {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(&mut self, database: impl Into<String>, target: ClientQueryTarget) -> Result<()> {
        let database = validate_database_name(database.into())?;
        self.targets.insert(database, target);
        Ok(())
    }

    pub fn with_database(
        mut self,
        database: impl Into<String>,
        target: ClientQueryTarget,
    ) -> Result<Self> {
        self.insert(database, target)?;
        Ok(self)
    }

    pub fn with_default_database(mut self, database: impl Into<String>) -> Result<Self> {
        let database = validate_database_name(database.into())?;
        self.default_database = Some(database);
        Ok(self)
    }

    pub fn single(database: impl Into<String>, target: ClientQueryTarget) -> Result<Self> {
        let database = validate_database_name(database.into())?;
        Self::new()
            .with_database(database.clone(), target)?
            .with_default_database(database)
    }
}

impl ClientDatabaseResolver for StaticClientDatabaseResolver {
    fn resolve_database(&self, database: Option<&str>) -> Result<ClientQueryTarget> {
        let database = match database {
            Some(database) => validate_database_name(database.to_string())?,
            None => self
                .default_database
                .clone()
                .ok_or_else(|| GraphError::UnsupportedQuery {
                    dialect: "ClientProtocol",
                    feature: "no default graph database is configured".to_string(),
                })?,
        };
        self.targets
            .get(&database)
            .cloned()
            .ok_or_else(|| GraphError::UnsupportedQuery {
                dialect: "ClientProtocol",
                feature: format!("unknown graph database {database}"),
            })
    }
}

fn validate_database_name(database: String) -> Result<String> {
    let database = database.trim().to_string();
    if database.is_empty() || database.len() > 255 || database.chars().any(char::is_control) {
        return Err(GraphError::InvalidKeyComponent {
            component: "database",
            value: database,
        });
    }
    Ok(database)
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClientBookmark {
    pub target: ClientQueryTarget,
    pub epoch: StorageSequence,
}

impl ClientBookmark {
    pub fn new(target: ClientQueryTarget, epoch: StorageSequence) -> Self {
        Self { target, epoch }
    }

    pub fn encode(&self) -> String {
        format!(
            "sgk:1:{}:{}:{}:{}",
            hex_encode(self.target.scope.namespace.to_string().as_bytes()),
            hex_encode(self.target.scope.graph_id.as_str().as_bytes()),
            hex_encode(self.target.cell_id.as_bytes()),
            self.epoch
        )
    }

    pub fn parse(value: &str) -> Result<Self> {
        value.parse()
    }
}

impl std::fmt::Display for ClientBookmark {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.encode())
    }
}

impl FromStr for ClientBookmark {
    type Err = GraphError;

    fn from_str(value: &str) -> Result<Self> {
        let parts: Vec<_> = value.split(':').collect();
        if parts.len() != 6 || parts[0] != "sgk" || parts[1] != "1" {
            return Err(invalid_bookmark("unsupported bookmark format"));
        }
        let namespace = String::from_utf8(hex_decode(parts[2])?)
            .map_err(|_| invalid_bookmark("namespace is not UTF-8"))?;
        let graph_id = String::from_utf8(hex_decode(parts[3])?)
            .map_err(|_| invalid_bookmark("graph id is not UTF-8"))?;
        let cell_id = String::from_utf8(hex_decode(parts[4])?)
            .map_err(|_| invalid_bookmark("cell id is not UTF-8"))?;
        let epoch = parts[5]
            .parse::<StorageSequence>()
            .map_err(|_| invalid_bookmark("epoch is not an unsigned integer"))?;
        let namespace = NamespacePath::new(
            namespace
                .split('/')
                .map(|segment| NamespaceId::new(segment.to_string()))
                .collect::<Result<Vec<_>>>()?,
        )?;
        let scope = GraphScope::new(namespace, GraphId::new(graph_id)?);
        Ok(Self::new(ClientQueryTarget::new(scope, cell_id)?, epoch))
    }
}

fn invalid_bookmark(reason: &str) -> GraphError {
    GraphError::UnsupportedQuery {
        dialect: "ClientProtocol",
        feature: format!("invalid bookmark: {reason}"),
    }
}

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(HEX[(byte >> 4) as usize] as char);
        output.push(HEX[(byte & 0x0f) as usize] as char);
    }
    output
}

fn hex_decode(value: &str) -> Result<Vec<u8>> {
    if !value.len().is_multiple_of(2) {
        return Err(invalid_bookmark("hex field has odd length"));
    }
    value
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            let high = hex_nibble(pair[0])?;
            let low = hex_nibble(pair[1])?;
            Ok((high << 4) | low)
        })
        .collect()
}

fn hex_nibble(byte: u8) -> Result<u8> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        b'A'..=b'F' => Ok(byte - b'A' + 10),
        _ => Err(invalid_bookmark("field contains non-hex characters")),
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ClientQueryCredentials {
    None,
    Bearer(String),
    Basic {
        principal: String,
        credentials: String,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClientQuerySession {
    principal: QueryTransportPrincipal,
}

impl ClientQuerySession {
    pub fn principal(&self) -> &QueryTransportPrincipal {
        &self.principal
    }
}

#[derive(Clone, Debug)]
pub struct ClientQueryRequest {
    pub target: ClientQueryTarget,
    pub query_id: String,
    pub query: String,
    pub parameters: BTreeMap<String, QueryParameterValue>,
    pub read_epoch: Option<StorageSequence>,
    pub max_runtime_ms: Option<u64>,
    pub bookmark: Option<ClientBookmark>,
    pub consistency: ClientReadConsistency,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[cfg_attr(
    feature = "query-transport",
    derive(serde::Serialize, serde::Deserialize)
)]
#[cfg_attr(feature = "query-transport", serde(rename_all = "snake_case"))]
pub enum ClientReadConsistency {
    #[default]
    Causal,
    Strong,
}

impl ClientQueryRequest {
    pub fn new(
        target: ClientQueryTarget,
        query_id: impl Into<String>,
        query: impl Into<String>,
    ) -> Self {
        Self {
            target,
            query_id: query_id.into(),
            query: query.into(),
            parameters: BTreeMap::new(),
            read_epoch: None,
            max_runtime_ms: None,
            bookmark: None,
            consistency: ClientReadConsistency::Causal,
        }
    }

    pub fn with_parameters(
        mut self,
        parameters: impl IntoIterator<Item = (String, VertexPropertyValue)>,
    ) -> Self {
        self.parameters.extend(
            parameters
                .into_iter()
                .map(|(name, value)| (name, QueryParameterValue::Scalar(value))),
        );
        self
    }

    pub fn with_query_parameters(
        mut self,
        parameters: impl IntoIterator<Item = (String, QueryParameterValue)>,
    ) -> Self {
        self.parameters.extend(parameters);
        self
    }

    pub fn at_epoch(mut self, read_epoch: StorageSequence) -> Self {
        self.read_epoch = Some(read_epoch);
        self
    }

    pub fn with_timeout_ms(mut self, max_runtime_ms: u64) -> Self {
        self.max_runtime_ms = Some(max_runtime_ms);
        self
    }

    pub fn after_bookmark(mut self, bookmark: ClientBookmark) -> Self {
        self.bookmark = Some(bookmark);
        self
    }

    pub fn with_consistency(mut self, consistency: ClientReadConsistency) -> Self {
        self.consistency = consistency;
        self
    }

    pub fn strong(mut self) -> Self {
        self.consistency = ClientReadConsistency::Strong;
        self
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClientQueryResult {
    pub query_id: String,
    pub result: QueryResultSet,
    pub read_epoch: Option<StorageSequence>,
    pub bookmark: Option<ClientBookmark>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClientQueryPage {
    pub query_id: String,
    pub page: QueryResultPage,
    pub read_epoch: Option<StorageSequence>,
    pub bookmark: Option<ClientBookmark>,
}

#[derive(Clone, Debug)]
pub(crate) struct PreparedClientQuery {
    pub(crate) request: ClientQueryRequest,
    pub(crate) action: QueryTransportAction,
    pub(crate) columns: Vec<QueryColumn>,
    pub(crate) scalar_parameters: BTreeMap<String, VertexPropertyValue>,
    pub(crate) batch_operation: Option<QueryBatchOperation>,
}

#[derive(Clone)]
pub struct ClientQueryServiceConfig {
    pub auth_policy: QueryTransportAuthPolicy,
    pub scope_authorizer: Arc<dyn QueryTransportScopeAuthorizer>,
    pub namespace_quotas: QueryTransportNamespaceQuotas,
    pub max_concurrent_queries: usize,
    pub max_query_bytes: usize,
    pub max_parameters: usize,
    pub max_page_size: usize,
    pub max_query_runtime_ms: u64,
    pub max_server_cursors: usize,
    pub max_cursor_buffer_bytes: u64,
    pub cursor_ttl_ms: u64,
}

impl Default for ClientQueryServiceConfig {
    fn default() -> Self {
        Self::from_query_transport(&QueryTransportServerConfig::default())
    }
}

impl ClientQueryServiceConfig {
    pub fn from_query_transport(config: &QueryTransportServerConfig) -> Self {
        Self {
            auth_policy: config.auth_policy.clone(),
            scope_authorizer: Arc::clone(&config.scope_authorizer),
            namespace_quotas: config.namespace_quotas.clone(),
            max_concurrent_queries: config.max_concurrent_requests,
            max_query_bytes: DEFAULT_MAX_QUERY_BYTES,
            max_parameters: DEFAULT_MAX_PARAMETERS,
            max_page_size: DEFAULT_MAX_PAGE_SIZE,
            max_query_runtime_ms: DEFAULT_MAX_QUERY_RUNTIME_MS,
            max_server_cursors: DEFAULT_MAX_SERVER_CURSORS,
            max_cursor_buffer_bytes: DEFAULT_MAX_CURSOR_BUFFER_BYTES,
            cursor_ttl_ms: DEFAULT_CURSOR_TTL_MS,
        }
    }

    pub fn with_required_bearer_token(mut self, token: impl Into<String>) -> Self {
        self.auth_policy = match QueryTransportSecret::try_new(token) {
            Ok(secret) => QueryTransportAuthPolicy::BearerToken(secret),
            Err(_) => QueryTransportAuthPolicy::RejectAll,
        };
        self
    }

    pub fn with_auth_policy(mut self, auth_policy: QueryTransportAuthPolicy) -> Self {
        self.auth_policy = auth_policy;
        self
    }

    pub fn with_scope_authorizer(
        mut self,
        authorizer: Arc<dyn QueryTransportScopeAuthorizer>,
    ) -> Self {
        self.scope_authorizer = authorizer;
        self
    }

    pub fn with_namespace_quotas(mut self, quotas: QueryTransportNamespaceQuotas) -> Self {
        self.namespace_quotas = quotas;
        self
    }

    pub fn with_max_concurrent_queries(mut self, max_concurrent_queries: usize) -> Self {
        self.max_concurrent_queries = max_concurrent_queries;
        self
    }

    pub fn with_max_query_bytes(mut self, max_query_bytes: usize) -> Self {
        self.max_query_bytes = max_query_bytes;
        self
    }

    pub fn with_max_parameters(mut self, max_parameters: usize) -> Self {
        self.max_parameters = max_parameters;
        self
    }

    pub fn with_max_page_size(mut self, max_page_size: usize) -> Self {
        self.max_page_size = max_page_size;
        self
    }

    pub fn with_max_query_runtime_ms(mut self, max_query_runtime_ms: u64) -> Self {
        self.max_query_runtime_ms = max_query_runtime_ms;
        self
    }

    pub fn with_server_cursor_limits(
        mut self,
        max_server_cursors: usize,
        max_cursor_buffer_bytes: u64,
        cursor_ttl_ms: u64,
    ) -> Self {
        self.max_server_cursors = max_server_cursors;
        self.max_cursor_buffer_bytes = max_cursor_buffer_bytes;
        self.cursor_ttl_ms = cursor_ttl_ms;
        self
    }

    fn validate(&self) -> Result<()> {
        self.namespace_quotas.validate()?;
        if self.max_concurrent_queries == 0
            || self.max_query_bytes == 0
            || self.max_parameters == 0
            || self.max_page_size == 0
            || self.max_query_runtime_ms == 0
            || self.max_server_cursors == 0
            || self.max_cursor_buffer_bytes == 0
            || self.cursor_ttl_ms == 0
        {
            return Err(GraphError::UnsupportedQuery {
                dialect: "ClientProtocol",
                feature: "client query limits must be greater than zero".to_string(),
            });
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ClientQueryMetricsSnapshot {
    pub queries_started: u64,
    pub queries_completed: u64,
    pub queries_failed: u64,
    pub rows_returned: u64,
    pub auth_failures: u64,
    pub scope_denials: u64,
    pub cancellations: u64,
    pub backpressure_waits: u64,
    pub prepare_requests: u64,
    pub prepare_duration_us: u64,
    pub execution_duration_us: u64,
}

#[derive(Default)]
struct ClientQueryMetrics {
    queries_started: AtomicU64,
    queries_completed: AtomicU64,
    queries_failed: AtomicU64,
    rows_returned: AtomicU64,
    auth_failures: AtomicU64,
    scope_denials: AtomicU64,
    cancellations: AtomicU64,
    backpressure_waits: AtomicU64,
    prepare_requests: AtomicU64,
    prepare_duration_us: AtomicU64,
    execution_duration_us: AtomicU64,
}

impl ClientQueryMetrics {
    fn snapshot(&self) -> ClientQueryMetricsSnapshot {
        ClientQueryMetricsSnapshot {
            queries_started: self.queries_started.load(Ordering::Relaxed),
            queries_completed: self.queries_completed.load(Ordering::Relaxed),
            queries_failed: self.queries_failed.load(Ordering::Relaxed),
            rows_returned: self.rows_returned.load(Ordering::Relaxed),
            auth_failures: self.auth_failures.load(Ordering::Relaxed),
            scope_denials: self.scope_denials.load(Ordering::Relaxed),
            cancellations: self.cancellations.load(Ordering::Relaxed),
            backpressure_waits: self.backpressure_waits.load(Ordering::Relaxed),
            prepare_requests: self.prepare_requests.load(Ordering::Relaxed),
            prepare_duration_us: self.prepare_duration_us.load(Ordering::Relaxed),
            execution_duration_us: self.execution_duration_us.load(Ordering::Relaxed),
        }
    }
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct ClientQueryKey {
    principal: QueryTransportPrincipal,
    scope: GraphScope,
    query_id: String,
}

struct ActiveClientQuery {
    generation: u64,
    cancellation_token: QueryCancellationToken,
}

struct ClientQueryDropGuard {
    inner: Arc<ClientQueryServiceInner>,
    key: ClientQueryKey,
    generation: u64,
    cancellation_token: QueryCancellationToken,
    armed: bool,
}

impl ClientQueryDropGuard {
    fn new(
        inner: Arc<ClientQueryServiceInner>,
        key: ClientQueryKey,
        generation: u64,
        cancellation_token: QueryCancellationToken,
    ) -> Self {
        Self {
            inner,
            key,
            generation,
            cancellation_token,
            armed: true,
        }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for ClientQueryDropGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        self.cancellation_token.cancel();
        let inner = Arc::clone(&self.inner);
        let key = self.key.clone();
        let generation = self.generation;
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(async move {
                let mut active_queries = inner.active_queries.lock().await;
                if active_queries
                    .get(&key)
                    .is_some_and(|active| active.generation == generation)
                {
                    active_queries.remove(&key);
                }
            });
        }
    }
}

struct ClientQueryServiceInner {
    client: Arc<dyn QueryCellClient>,
    config: ClientQueryServiceConfig,
    metrics: ClientQueryMetrics,
    query_gate: Arc<Semaphore>,
    namespace_gates: BTreeMap<NamespacePath, Arc<Semaphore>>,
    active_queries: Mutex<BTreeMap<ClientQueryKey, ActiveClientQuery>>,
    next_generation: AtomicU64,
    cursors: Mutex<BTreeMap<u64, ServerQueryCursor>>,
    next_cursor_id: AtomicU64,
    cursor_buffer_bytes: AtomicU64,
}

struct ServerQueryCursor {
    owner: ClientQueryKey,
    target: ClientQueryTarget,
    query: String,
    parameters: BTreeMap<String, QueryParameterValue>,
    columns: Vec<QueryColumn>,
    rows: VecDeque<QueryRow>,
    read_epoch: Option<StorageSequence>,
    bookmark: Option<ClientBookmark>,
    expires_at: Instant,
    resident_bytes: u64,
}

#[derive(Clone)]
pub struct ClientQueryService {
    inner: Arc<ClientQueryServiceInner>,
}

impl ClientQueryService {
    pub fn new(client: Arc<dyn QueryCellClient>, config: ClientQueryServiceConfig) -> Result<Self> {
        config.validate()?;
        let query_gate = Arc::new(Semaphore::new(config.max_concurrent_queries));
        let namespace_gates = config.namespace_quotas.gates();
        Ok(Self {
            inner: Arc::new(ClientQueryServiceInner {
                client,
                config,
                metrics: ClientQueryMetrics::default(),
                query_gate,
                namespace_gates,
                active_queries: Mutex::new(BTreeMap::new()),
                next_generation: AtomicU64::new(1),
                cursors: Mutex::new(BTreeMap::new()),
                next_cursor_id: AtomicU64::new(1),
                cursor_buffer_bytes: AtomicU64::new(0),
            }),
        })
    }

    pub fn metrics(&self) -> ClientQueryMetricsSnapshot {
        self.inner.metrics.snapshot()
    }

    pub fn max_page_size(&self) -> usize {
        self.inner.config.max_page_size
    }

    pub(crate) fn effective_runtime_limit_ms(&self, requested: Option<u64>) -> Result<u64> {
        let limit = self.inner.config.max_query_runtime_ms;
        let requested = requested.unwrap_or(limit);
        if requested == 0 {
            return Err(GraphError::UnsupportedQuery {
                dialect: "ClientProtocol",
                feature: "query timeout must be greater than zero".to_string(),
            });
        }
        if requested > limit {
            return Err(GraphError::AdmissionRejected {
                operation: "client_query_runtime_ms",
                actual: requested,
                limit,
            });
        }
        Ok(requested)
    }

    pub async fn active_query_count(&self) -> usize {
        self.inner.active_queries.lock().await.len()
    }

    pub fn authorize_action(
        &self,
        session: &ClientQuerySession,
        scope: &GraphScope,
        action: QueryTransportAction,
    ) -> Result<()> {
        self.authorize_scope(session, scope, action)
    }

    pub(crate) fn authorize_any_action(
        &self,
        session: &ClientQuerySession,
        scope: &GraphScope,
        actions: &[QueryTransportAction],
        action_label: &'static str,
    ) -> Result<()> {
        if actions.iter().copied().any(|action| {
            self.inner
                .config
                .scope_authorizer
                .authorize(&session.principal, scope, action)
        }) || self.inner.config.scope_authorizer.authorize(
            &session.principal,
            scope,
            QueryTransportAction::Admin,
        ) {
            return Ok(());
        }
        self.inner
            .metrics
            .scope_denials
            .fetch_add(1, Ordering::Relaxed);
        Err(GraphError::GraphScopeAccessDenied {
            principal: session.principal.error_label().to_string(),
            action: action_label,
            scope: scope.to_string(),
        })
    }

    pub async fn ensure_bookmark(&self, bookmark: &ClientBookmark) -> Result<()> {
        let current_sequence = self
            .inner
            .client
            .wait_for_storage_sequence(
                &bookmark.target.scope,
                &bookmark.target.cell_id,
                bookmark.epoch,
            )
            .await?
            .ok_or_else(|| GraphError::UnsupportedQuery {
                dialect: "ClientProtocol",
                feature: "backend cannot prove bookmark durability".to_string(),
            })?;
        if current_sequence < bookmark.epoch {
            return Err(GraphError::SnapshotAhead {
                cell_id: bookmark.target.cell_id.clone(),
                read_epoch: bookmark.epoch,
                current_epoch: current_sequence,
            });
        }
        Ok(())
    }

    pub fn authenticate(
        &self,
        credentials: &ClientQueryCredentials,
        identity: &QueryTransportConnectionIdentity,
    ) -> Result<ClientQuerySession> {
        let bearer_token = match credentials {
            ClientQueryCredentials::None => None,
            ClientQueryCredentials::Bearer(token) => Some(token.as_str()),
            ClientQueryCredentials::Basic {
                principal,
                credentials,
            } => {
                if principal.trim().is_empty() {
                    return Err(authentication_error());
                }
                Some(credentials.as_str())
            }
        };
        let Some(principal) = self
            .inner
            .config
            .auth_policy
            .authenticate_client(bearer_token, identity)
        else {
            self.inner
                .metrics
                .auth_failures
                .fetch_add(1, Ordering::Relaxed);
            return Err(authentication_error());
        };
        Ok(ClientQuerySession { principal })
    }

    pub async fn execute_rows(
        &self,
        session: &ClientQuerySession,
        mut request: ClientQueryRequest,
    ) -> Result<ClientQueryResult> {
        if request.read_epoch.is_some() {
            return Err(GraphError::UnsupportedQuery {
                dialect: "ClientProtocol",
                feature: "historical graph epochs are not client query snapshots; use a bookmark for causal reads"
                    .to_string(),
            });
        }
        self.validate_request(&request, None)?;
        let runtime_limit_ms = self.normalize_runtime_limit(&mut request)?;
        let action = self.authorize_query(session, &request)?;
        let parsed_unwind = parse_opencypher_unwind_batch(&request.query)?;
        let (batch_operation, scalar_parameters) = match parsed_unwind {
            Some(parsed) => (
                Some(resolve_unwind_batch(parsed, &request.parameters)?),
                BTreeMap::new(),
            ),
            None => (None, scalar_query_parameters(&request.parameters)?),
        };
        if batch_operation.as_ref().is_some_and(|operation| {
            operation.is_write() != (action == QueryTransportAction::Write)
        }) {
            return Err(GraphError::CorruptValue {
                key: "client/query/unwind_access".to_string(),
                reason: "UNWIND batch access classification does not match its operation"
                    .to_string(),
            });
        }
        if let Some(operation) = &batch_operation {
            enforce_limit(
                "client_query_batch_items",
                operation.len(),
                self.inner.config.max_parameters,
            )?;
        }
        let key = client_query_key(session, &request);
        let (generation, cancellation_token) = self.begin_query(key.clone()).await?;
        let result = self
            .run_query_with_timeout(
                &key,
                generation,
                &cancellation_token,
                runtime_limit_ms,
                async {
                    self.validate_bookmark(&request).await?;
                    self.refresh_strong_read(&request, action).await?;
                    let mut context = query_context(
                        &request,
                        scalar_parameters.clone(),
                        cancellation_token.clone(),
                    );
                    if action == QueryTransportAction::Read {
                        context.read_epoch = None;
                        context.max_result_bytes = Some(self.inner.config.max_cursor_buffer_bytes);
                    }
                    let result = match batch_operation {
                        Some(operation) => {
                            self.inner.client.execute_batch(context, operation).await?
                        }
                        None => {
                            self.inner
                                .client
                                .execute_cypher_rows(context, &request.query)
                                .await?
                        }
                    };
                    let read_epoch = result_read_epoch(&result, action)?;
                    let storage_sequence = result_storage_sequence(&result, action)?;
                    let bookmark = self
                        .bookmark_after(&request, action, storage_sequence)
                        .await?;
                    Ok(ClientQueryResult {
                        query_id: request.query_id.clone(),
                        result,
                        read_epoch,
                        bookmark,
                    })
                },
            )
            .await;
        self.record_result_metrics(
            action,
            result
                .as_ref()
                .map(|response| response.result.rows.len())
                .unwrap_or(0),
            result.is_ok(),
        );
        result
    }

    pub async fn execute_page(
        &self,
        session: &ClientQuerySession,
        request: ClientQueryRequest,
        cursor: Option<QueryCursorToken>,
        page_size: usize,
    ) -> Result<ClientQueryPage> {
        if cursor.is_none() && request.read_epoch.is_some() {
            return Err(GraphError::UnsupportedQuery {
                dialect: "ClientProtocol",
                feature: "historical graph epochs are not client query snapshots; use a bookmark for causal reads"
                    .to_string(),
            });
        }
        let prepared = self
            .prepare_page_request(session, request, page_size)
            .await?;
        self.execute_prepared_page(session, prepared, cursor, page_size)
            .await
    }

    pub(crate) async fn execute_prepared_page(
        &self,
        session: &ClientQuerySession,
        prepared: PreparedClientQuery,
        cursor: Option<QueryCursorToken>,
        page_size: usize,
    ) -> Result<ClientQueryPage> {
        let execution_started = std::time::Instant::now();
        let PreparedClientQuery {
            request,
            action,
            columns: _,
            scalar_parameters,
            batch_operation,
        } = prepared;
        // Grants can change while a Bolt cursor is open. The parsed query and
        // access classification are reusable, but authorization is not.
        self.authorize_scope(session, &request.target.scope, action)?;
        if action == QueryTransportAction::Write && cursor.is_some() {
            return Err(GraphError::UnsupportedQuery {
                dialect: "ClientProtocol",
                feature: "mutation queries cannot continue from a result cursor".to_string(),
            });
        }
        let runtime_limit_ms = request
            .max_runtime_ms
            .expect("prepared client queries have a normalized runtime limit");
        let key = client_query_key(session, &request);
        let (generation, cancellation_token) = self.begin_query(key.clone()).await?;
        let result = self
            .run_query_with_timeout(
                &key,
                generation,
                &cancellation_token,
                runtime_limit_ms,
                async {
                    if let Some(cursor) = cursor {
                        return self
                            .continue_server_cursor(session, &request, cursor, page_size)
                            .await;
                    }
                    let mut context = query_context(
                        &request,
                        scalar_parameters.clone(),
                        cancellation_token.clone(),
                    );
                    context.max_runtime_ms = request.max_runtime_ms;
                    context.cancellation_token = Some(cancellation_token.clone());
                    if action == QueryTransportAction::Read {
                        // The client-visible topology watermark is not a
                        // storage snapshot selector. Shard execution creates
                        // one SlateDB DbSnapshot for this complete result.
                        context.read_epoch = None;
                        context.max_result_bytes = Some(self.inner.config.max_cursor_buffer_bytes);
                        self.refresh_strong_read(&request, action).await?;
                        let result = match batch_operation {
                            Some(operation) => {
                                self.inner.client.execute_batch(context, operation).await?
                            }
                            None => {
                                self.inner
                                    .client
                                    .execute_cypher_rows(context, &request.query)
                                    .await?
                            }
                        };
                        let read_epoch = result_read_epoch(&result, action)?;
                        let storage_sequence = result_storage_sequence(&result, action)?;
                        let bookmark = self
                            .bookmark_after(&request, action, storage_sequence)
                            .await?;
                        return self
                            .start_server_cursor(
                                session, &request, result, read_epoch, bookmark, page_size,
                            )
                            .await;
                    }
                    let page = match batch_operation {
                        Some(operation) => {
                            self.inner
                                .client
                                .execute_batch_page(context, operation, None, page_size)
                                .await?
                        }
                        None => {
                            self.inner
                                .client
                                .execute_cypher_rows_page(context, &request.query, None, page_size)
                                .await?
                        }
                    };
                    let bookmark = self.bookmark_after(&request, action, None).await?;
                    Ok(ClientQueryPage {
                        query_id: request.query_id.clone(),
                        page,
                        read_epoch: request.read_epoch,
                        bookmark,
                    })
                },
            )
            .await;
        self.record_result_metrics(
            action,
            result
                .as_ref()
                .map(|response| response.page.rows.len())
                .unwrap_or(0),
            result.is_ok(),
        );
        self.inner.metrics.execution_duration_us.fetch_add(
            execution_started
                .elapsed()
                .as_micros()
                .try_into()
                .unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
        result
    }

    pub(crate) async fn prepare_page_request(
        &self,
        session: &ClientQuerySession,
        mut request: ClientQueryRequest,
        page_size: usize,
    ) -> Result<PreparedClientQuery> {
        let prepare_started = std::time::Instant::now();
        self.inner
            .metrics
            .prepare_requests
            .fetch_add(1, Ordering::Relaxed);
        self.validate_request(&request, Some(page_size))?;
        self.normalize_runtime_limit(&mut request)?;
        let action = self.authorize_query(session, &request)?;
        self.validate_bookmark(&request).await?;

        let parsed_unwind = parse_opencypher_unwind_batch(&request.query)?;
        let (batch_operation, scalar_parameters) = match parsed_unwind {
            Some(parsed) => (
                Some(resolve_unwind_batch(parsed, &request.parameters)?),
                BTreeMap::new(),
            ),
            None => (None, scalar_query_parameters(&request.parameters)?),
        };

        let columns = if let Some(operation) = &batch_operation {
            batch_operation_columns(operation)
        } else {
            match action {
                QueryTransportAction::Read => {
                    parse_opencypher_row_query_with_parameters(&request.query, &scalar_parameters)?
                        .columns
                }
                QueryTransportAction::Write => {
                    let mutation = parse_opencypher_mutation_query_with_parameters(
                        &request.query,
                        &scalar_parameters,
                    )?
                    .is_some();
                    if !mutation {
                        return Err(GraphError::UnsupportedQuery {
                            dialect: "ClientProtocol",
                            feature: "write query is not executable by the mutation engine"
                                .to_string(),
                        });
                    }
                    Vec::new()
                }
                QueryTransportAction::Cancel | QueryTransportAction::Admin => {
                    unreachable!("query access classification only returns read or write")
                }
            }
        };
        if batch_operation.as_ref().is_some_and(|operation| {
            operation.is_write() != (action == QueryTransportAction::Write)
        }) {
            return Err(GraphError::CorruptValue {
                key: "client/query/unwind_access".to_string(),
                reason: "UNWIND batch access classification does not match its operation"
                    .to_string(),
            });
        }
        if let Some(operation) = &batch_operation {
            enforce_limit(
                "client_query_batch_items",
                operation.len(),
                self.inner.config.max_parameters,
            )?;
        }
        let prepared = PreparedClientQuery {
            request,
            action,
            columns,
            scalar_parameters,
            batch_operation,
        };
        self.inner.metrics.prepare_duration_us.fetch_add(
            prepare_started
                .elapsed()
                .as_micros()
                .try_into()
                .unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
        Ok(prepared)
    }

    async fn start_server_cursor(
        &self,
        session: &ClientQuerySession,
        request: &ClientQueryRequest,
        result: QueryResultSet,
        read_epoch: Option<StorageSequence>,
        bookmark: Option<ClientBookmark>,
        page_size: usize,
    ) -> Result<ClientQueryPage> {
        let materialized_bytes = result.estimated_resident_bytes();
        let mut cursors = self.inner.cursors.lock().await;
        self.purge_expired_cursors(&mut cursors);
        let current_bytes = self.inner.cursor_buffer_bytes.load(Ordering::Relaxed);
        let materialized_total = current_bytes.saturating_add(materialized_bytes);
        if materialized_total > self.inner.config.max_cursor_buffer_bytes {
            return Err(GraphError::AdmissionRejected {
                operation: "client_cursor_buffer_bytes",
                actual: materialized_total,
                limit: self.inner.config.max_cursor_buffer_bytes,
            });
        }
        let QueryResultSet {
            columns,
            rows,
            read_epoch: _,
            storage_sequence: _,
        } = result;
        let mut remaining = VecDeque::from(rows);
        let page_rows = take_cursor_rows(&mut remaining, page_size);
        if remaining.is_empty() {
            return Ok(ClientQueryPage {
                query_id: request.query_id.clone(),
                page: QueryResultPage::new(columns, page_rows, None),
                read_epoch,
                bookmark,
            });
        }

        let resident_bytes = query_rows_resident_bytes(&remaining);
        if cursors.len() >= self.inner.config.max_server_cursors {
            return Err(GraphError::AdmissionRejected {
                operation: "client_server_cursors",
                actual: cursors.len().saturating_add(1) as u64,
                limit: self.inner.config.max_server_cursors as u64,
            });
        }
        let next_bytes = current_bytes.saturating_add(resident_bytes);
        let cursor_id = next_cursor_id(&self.inner.next_cursor_id, &cursors)?;
        let cursor = ServerQueryCursor {
            owner: client_query_key(session, request),
            target: request.target.clone(),
            query: request.query.clone(),
            parameters: request.parameters.clone(),
            columns: columns.clone(),
            rows: remaining,
            read_epoch,
            bookmark: bookmark.clone(),
            expires_at: Instant::now() + Duration::from_millis(self.inner.config.cursor_ttl_ms),
            resident_bytes,
        };
        cursors.insert(cursor_id, cursor);
        self.inner
            .cursor_buffer_bytes
            .store(next_bytes, Ordering::Relaxed);
        Ok(ClientQueryPage {
            query_id: request.query_id.clone(),
            page: QueryResultPage::new(columns, page_rows, Some(QueryCursorToken::new(cursor_id))),
            read_epoch,
            bookmark,
        })
    }

    async fn continue_server_cursor(
        &self,
        session: &ClientQuerySession,
        request: &ClientQueryRequest,
        token: QueryCursorToken,
        page_size: usize,
    ) -> Result<ClientQueryPage> {
        let mut cursors = self.inner.cursors.lock().await;
        self.purge_expired_cursors(&mut cursors);
        let expected_owner = client_query_key(session, request);
        let Some(cursor) = cursors.get(&token.offset) else {
            return Err(GraphError::UnsupportedQuery {
                dialect: "ClientProtocol",
                feature: "result cursor is unknown or expired".to_string(),
            });
        };
        if cursor.owner != expected_owner
            || cursor.target != request.target
            || cursor.query != request.query
            || cursor.parameters != request.parameters
        {
            return Err(GraphError::UnsupportedQuery {
                dialect: "ClientProtocol",
                feature: "result cursor does not belong to this query request".to_string(),
            });
        }

        let mut cursor = cursors
            .remove(&token.offset)
            .expect("cursor was checked while holding the cursor lock");
        let previous_bytes = cursor.resident_bytes;
        let page_rows = take_cursor_rows(&mut cursor.rows, page_size);
        cursor.resident_bytes = query_rows_resident_bytes(&cursor.rows);
        let released_bytes = previous_bytes.saturating_sub(cursor.resident_bytes);
        self.inner
            .cursor_buffer_bytes
            .fetch_sub(released_bytes, Ordering::Relaxed);
        let columns = cursor.columns.clone();
        let read_epoch = cursor.read_epoch;
        let bookmark = cursor.bookmark.clone();
        let next_cursor = if cursor.rows.is_empty() {
            None
        } else {
            cursor.expires_at =
                Instant::now() + Duration::from_millis(self.inner.config.cursor_ttl_ms);
            cursors.insert(token.offset, cursor);
            Some(token)
        };
        Ok(ClientQueryPage {
            query_id: request.query_id.clone(),
            page: QueryResultPage::new(columns, page_rows, next_cursor),
            read_epoch,
            bookmark,
        })
    }

    fn purge_expired_cursors(&self, cursors: &mut BTreeMap<u64, ServerQueryCursor>) {
        let now = Instant::now();
        let mut released_bytes = 0_u64;
        cursors.retain(|_, cursor| {
            let retain = cursor.expires_at > now;
            if !retain {
                released_bytes = released_bytes.saturating_add(cursor.resident_bytes);
            }
            retain
        });
        if released_bytes > 0 {
            self.inner
                .cursor_buffer_bytes
                .fetch_sub(released_bytes, Ordering::Relaxed);
        }
    }

    pub(crate) async fn release_server_cursor(
        &self,
        session: &ClientQuerySession,
        request: &ClientQueryRequest,
        token: QueryCursorToken,
    ) -> bool {
        let mut cursors = self.inner.cursors.lock().await;
        self.purge_expired_cursors(&mut cursors);
        let expected_owner = client_query_key(session, request);
        let Some(cursor) = cursors.get(&token.offset) else {
            return false;
        };
        if cursor.owner != expected_owner
            || cursor.target != request.target
            || cursor.query != request.query
            || cursor.parameters != request.parameters
        {
            return false;
        }
        let cursor = cursors
            .remove(&token.offset)
            .expect("cursor ownership was checked while holding the cursor lock");
        self.inner
            .cursor_buffer_bytes
            .fetch_sub(cursor.resident_bytes, Ordering::Relaxed);
        true
    }

    pub async fn cancel(
        &self,
        session: &ClientQuerySession,
        scope: &GraphScope,
        query_id: &str,
    ) -> Result<()> {
        validate_component("query_id", query_id)?;
        self.authorize_scope(session, scope, QueryTransportAction::Cancel)?;
        let key = ClientQueryKey {
            principal: session.principal.clone(),
            scope: scope.clone(),
            query_id: query_id.to_string(),
        };
        let active_queries = self.inner.active_queries.lock().await;
        let Some(active) = active_queries.get(&key) else {
            return Err(GraphError::UnsupportedQuery {
                dialect: "ClientProtocol",
                feature: format!("no active query with id {query_id} was cancelled"),
            });
        };
        active.cancellation_token.cancel();
        self.inner
            .metrics
            .cancellations
            .fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    fn validate_request(
        &self,
        request: &ClientQueryRequest,
        page_size: Option<usize>,
    ) -> Result<()> {
        validate_component("cell_id", &request.target.cell_id)?;
        validate_component("query_id", &request.query_id)?;
        if request.query.is_empty() {
            return Err(GraphError::QueryParse {
                dialect: "OpenCypher",
                reason: "query cannot be empty".to_string(),
            });
        }
        enforce_limit(
            "client_query_bytes",
            request.query.len(),
            self.inner.config.max_query_bytes,
        )?;
        enforce_limit(
            "client_query_parameters",
            request.parameters.len(),
            self.inner.config.max_parameters,
        )?;
        if let Some(page_size) = page_size {
            if page_size == 0 {
                return Err(GraphError::UnsupportedQuery {
                    dialect: "ClientProtocol",
                    feature: "page size must be greater than zero".to_string(),
                });
            }
            enforce_limit(
                "client_query_page_size",
                page_size,
                self.inner.config.max_page_size,
            )?;
        }
        Ok(())
    }

    fn authorize_query(
        &self,
        session: &ClientQuerySession,
        request: &ClientQueryRequest,
    ) -> Result<QueryTransportAction> {
        let action = match classify_opencypher_query_access(&request.query)? {
            OpenCypherQueryAccess::Read => QueryTransportAction::Read,
            OpenCypherQueryAccess::Write => QueryTransportAction::Write,
        };
        self.authorize_scope(session, &request.target.scope, action)?;
        Ok(action)
    }

    fn normalize_runtime_limit(&self, request: &mut ClientQueryRequest) -> Result<u64> {
        let requested = self.effective_runtime_limit_ms(request.max_runtime_ms)?;
        request.max_runtime_ms = Some(requested);
        Ok(requested)
    }

    fn authorize_scope(
        &self,
        session: &ClientQuerySession,
        scope: &GraphScope,
        action: QueryTransportAction,
    ) -> Result<()> {
        self.authorize_any_action(session, scope, &[action], action.as_str())
    }

    async fn validate_bookmark(&self, request: &ClientQueryRequest) -> Result<()> {
        let Some(bookmark) = &request.bookmark else {
            return Ok(());
        };
        if bookmark.target != request.target {
            return Err(GraphError::GraphScopeMismatch {
                expected: format!("{} cell {}", request.target.scope, request.target.cell_id),
                actual: format!("{} cell {}", bookmark.target.scope, bookmark.target.cell_id),
            });
        }
        self.ensure_bookmark(bookmark).await
    }

    async fn refresh_strong_read(
        &self,
        request: &ClientQueryRequest,
        action: QueryTransportAction,
    ) -> Result<()> {
        if request.consistency != ClientReadConsistency::Strong {
            return Ok(());
        }
        if action != QueryTransportAction::Read {
            return Err(GraphError::UnsupportedQuery {
                dialect: "ClientProtocol",
                feature: "strong consistency applies only to read queries".to_string(),
            });
        }
        self.inner
            .client
            .refresh_storage_sequence(&request.target.scope, &request.target.cell_id)
            .await?
            .ok_or_else(|| GraphError::UnsupportedQuery {
                dialect: "ClientProtocol",
                feature: "backend cannot refresh the latest durable SlateDB frontier".to_string(),
            })?;
        Ok(())
    }

    async fn bookmark_after(
        &self,
        request: &ClientQueryRequest,
        action: QueryTransportAction,
        read_storage_sequence: Option<StorageSequence>,
    ) -> Result<Option<ClientBookmark>> {
        let sequence = if action == QueryTransportAction::Read {
            read_storage_sequence
        } else {
            self.inner
                .client
                .current_storage_sequence(&request.target.scope, &request.target.cell_id)
                .await?
        };
        Ok(sequence.map(|sequence| ClientBookmark::new(request.target.clone(), sequence)))
    }

    async fn begin_query(&self, key: ClientQueryKey) -> Result<(u64, QueryCancellationToken)> {
        let mut active_queries = self.inner.active_queries.lock().await;
        if active_queries.contains_key(&key) {
            return Err(GraphError::UnsupportedQuery {
                dialect: "ClientProtocol",
                feature: format!("query id {} is already active", key.query_id),
            });
        }
        let generation = self.inner.next_generation.fetch_add(1, Ordering::Relaxed);
        let cancellation_token = QueryCancellationToken::new();
        active_queries.insert(
            key,
            ActiveClientQuery {
                generation,
                cancellation_token: cancellation_token.clone(),
            },
        );
        Ok((generation, cancellation_token))
    }

    async fn run_query<T, F>(
        &self,
        key: &ClientQueryKey,
        generation: u64,
        cancellation_token: &QueryCancellationToken,
        execute: F,
    ) -> Result<T>
    where
        F: Future<Output = Result<T>>,
    {
        let mut drop_guard = ClientQueryDropGuard::new(
            Arc::clone(&self.inner),
            key.clone(),
            generation,
            cancellation_token.clone(),
        );
        self.inner
            .metrics
            .queries_started
            .fetch_add(1, Ordering::Relaxed);
        let result = AssertUnwindSafe(async {
            let _namespace_permits = self
                .acquire_namespace_permits(&key.scope.namespace, cancellation_token)
                .await?;
            let _query_permit = self.acquire_query_permit(cancellation_token).await?;
            execute.await
        })
        .catch_unwind()
        .await
        .unwrap_or_else(|_| {
            Err(GraphError::CorruptValue {
                key: "client/query/executor".to_string(),
                reason: "query executor panicked".to_string(),
            })
        });
        let cancelled = {
            let mut active_queries = self.inner.active_queries.lock().await;
            let cancelled = active_queries.get(key).is_some_and(|active| {
                active.generation == generation && active.cancellation_token.is_cancelled()
            });
            if active_queries
                .get(key)
                .is_some_and(|active| active.generation == generation)
            {
                active_queries.remove(key);
            }
            cancelled
        };
        drop_guard.disarm();
        if cancelled {
            return Err(client_query_cancelled());
        }
        result
    }

    async fn run_query_with_timeout<T, F>(
        &self,
        key: &ClientQueryKey,
        generation: u64,
        cancellation_token: &QueryCancellationToken,
        runtime_limit_ms: u64,
        execute: F,
    ) -> Result<T>
    where
        F: Future<Output = Result<T>>,
    {
        let query = self.run_query(key, generation, cancellation_token, execute);
        tokio::pin!(query);
        tokio::select! {
            result = &mut query => result,
            _ = tokio::time::sleep(Duration::from_millis(runtime_limit_ms)) => {
                // spawn_blocking work cannot be aborted once native GraphBLAS has
                // started. Signal cooperative cancellation, then keep the query
                // and namespace permits until the executor has actually stopped.
                cancellation_token.cancel();
                let _ = query.await;
                Err(client_query_runtime_exceeded(runtime_limit_ms))
            }
        }
    }

    async fn acquire_query_permit(
        &self,
        cancellation_token: &QueryCancellationToken,
    ) -> Result<OwnedSemaphorePermit> {
        match Arc::clone(&self.inner.query_gate).try_acquire_owned() {
            Ok(permit) => Ok(permit),
            Err(_) => {
                self.inner
                    .metrics
                    .backpressure_waits
                    .fetch_add(1, Ordering::Relaxed);
                tokio::select! {
                    permit = Arc::clone(&self.inner.query_gate).acquire_owned() => {
                        permit.map_err(|err| GraphError::CorruptValue {
                            key: "client/query/backpressure".to_string(),
                            reason: err.to_string(),
                        })
                    }
                    _ = cancellation_token.cancelled() => Err(client_query_cancelled()),
                }
            }
        }
    }

    async fn acquire_namespace_permits(
        &self,
        namespace: &NamespacePath,
        cancellation_token: &QueryCancellationToken,
    ) -> Result<Vec<OwnedSemaphorePermit>> {
        let mut permits = Vec::new();
        for ancestor in namespace.ancestors_inclusive().rev() {
            let Some(gate) = self.inner.namespace_gates.get(&ancestor) else {
                continue;
            };
            let permit = match Arc::clone(gate).try_acquire_owned() {
                Ok(permit) => permit,
                Err(_) => {
                    self.inner
                        .metrics
                        .backpressure_waits
                        .fetch_add(1, Ordering::Relaxed);
                    tokio::select! {
                        permit = Arc::clone(gate).acquire_owned() => {
                            permit.map_err(|err| GraphError::CorruptValue {
                                key: format!("client/query/namespace_quota/{ancestor}"),
                                reason: err.to_string(),
                            })?
                        }
                        _ = cancellation_token.cancelled() => return Err(client_query_cancelled()),
                    }
                }
            };
            permits.push(permit);
        }
        Ok(permits)
    }

    fn record_result_metrics(&self, _action: QueryTransportAction, rows: usize, succeeded: bool) {
        self.inner
            .metrics
            .rows_returned
            .fetch_add(rows as u64, Ordering::Relaxed);
        let counter = if succeeded {
            &self.inner.metrics.queries_completed
        } else {
            &self.inner.metrics.queries_failed
        };
        counter.fetch_add(1, Ordering::Relaxed);
    }
}

fn client_query_key(session: &ClientQuerySession, request: &ClientQueryRequest) -> ClientQueryKey {
    ClientQueryKey {
        principal: session.principal.clone(),
        scope: request.target.scope.clone(),
        query_id: request.query_id.clone(),
    }
}

fn next_cursor_id(next: &AtomicU64, cursors: &BTreeMap<u64, ServerQueryCursor>) -> Result<u64> {
    for _ in 0..=cursors.len() {
        let candidate = next.fetch_add(1, Ordering::Relaxed);
        if candidate != 0 && !cursors.contains_key(&candidate) {
            return Ok(candidate);
        }
    }
    Err(GraphError::AdmissionRejected {
        operation: "client_cursor_id_space",
        actual: cursors.len().saturating_add(1) as u64,
        limit: u64::MAX,
    })
}

fn take_cursor_rows(rows: &mut VecDeque<QueryRow>, page_size: usize) -> Vec<QueryRow> {
    let count = page_size.min(rows.len());
    rows.drain(..count).collect()
}

fn query_rows_resident_bytes(rows: &VecDeque<QueryRow>) -> u64 {
    rows.iter().fold(0_u64, |total, row| {
        total.saturating_add(row.estimated_resident_bytes())
    })
}

fn result_read_epoch(
    result: &QueryResultSet,
    action: QueryTransportAction,
) -> Result<Option<StorageSequence>> {
    if action != QueryTransportAction::Read {
        return Ok(None);
    }
    result
        .read_epoch
        .map(Some)
        .ok_or_else(|| GraphError::CorruptValue {
            key: "client/query/read_epoch".to_string(),
            reason: "read query result did not report its storage snapshot epoch".to_string(),
        })
}

fn result_storage_sequence(
    result: &QueryResultSet,
    action: QueryTransportAction,
) -> Result<Option<StorageSequence>> {
    if action != QueryTransportAction::Read {
        return Ok(None);
    }
    result
        .storage_sequence
        .map(Some)
        .ok_or_else(|| GraphError::CorruptValue {
            key: "client/query/storage_sequence".to_string(),
            reason: "read query result did not report its SlateDB snapshot sequence".to_string(),
        })
}

fn query_context(
    request: &ClientQueryRequest,
    parameters: BTreeMap<String, VertexPropertyValue>,
    cancellation_token: QueryCancellationToken,
) -> QueryContext {
    let mut context = QueryContext::new(&request.target.cell_id, &request.query_id)
        .in_scope(request.target.scope.clone())
        .with_parameters(parameters)
        .with_cancellation_token(cancellation_token);
    if let Some(read_epoch) = request.read_epoch {
        context = context.at_epoch(read_epoch);
    }
    if let Some(max_runtime_ms) = request.max_runtime_ms {
        context = context.with_timeout_ms(max_runtime_ms);
    }
    if request.consistency == ClientReadConsistency::Strong {
        context = context.with_refreshed_reader();
    }
    context
}

fn scalar_query_parameters(
    parameters: &BTreeMap<String, QueryParameterValue>,
) -> Result<BTreeMap<String, VertexPropertyValue>> {
    parameters
        .iter()
        .map(|(name, value)| match value {
            QueryParameterValue::Scalar(value) => Ok((name.clone(), value.clone())),
            QueryParameterValue::List(_) | QueryParameterValue::Map(_) => {
                Err(GraphError::UnsupportedQuery {
                    dialect: "ClientProtocol",
                    feature: format!(
                        "composite parameter ${name} is only supported as an UNWIND input"
                    ),
                })
            }
        })
        .collect()
}

fn resolve_unwind_batch(
    parsed: crate::query::opencypher::ParsedUnwindBatch,
    parameters: &BTreeMap<String, QueryParameterValue>,
) -> Result<QueryBatchOperation> {
    let value = parameters
        .get(&parsed.parameter)
        .or_else(|| parameters.get(&format!("${}", parsed.parameter)))
        .ok_or_else(|| GraphError::MissingQueryParameter {
            dialect: "OpenCypher",
            name: parsed.parameter.clone(),
        })?;
    let QueryParameterValue::List(rows) = value else {
        return Err(GraphError::UnsupportedQuery {
            dialect: "OpenCypher",
            feature: format!("UNWIND parameter ${} must be a list", parsed.parameter),
        });
    };
    match parsed.kind {
        ParsedUnwindBatchKind::OutNeighbors {
            edge_type,
            source_field,
            source_column,
            destination_column,
        } => Ok(QueryBatchOperation::OutNeighbors {
            edge_type,
            sources: rows
                .iter()
                .enumerate()
                .map(|(index, row)| unwind_row_vertex_id(row, index, &source_field))
                .collect::<Result<Vec<_>>>()?,
            source_column,
            destination_column,
        }),
        ParsedUnwindBatchKind::CreateEdges {
            edge_type,
            source_field,
            destination_field,
        } => Ok(QueryBatchOperation::CreateEdges {
            edge_type,
            edges: unwind_batch_edges(rows, &source_field, &destination_field)?,
        }),
        ParsedUnwindBatchKind::CreateEdgesBetweenLabeledVertices {
            edge_type,
            source_field,
            destination_field,
            source_label,
            destination_label,
        } => Ok(QueryBatchOperation::CreateEdgesBetweenLabeledVertices {
            edge_type,
            edges: unwind_batch_edges(rows, &source_field, &destination_field)?,
            source_label,
            destination_label,
        }),
        ParsedUnwindBatchKind::DeleteEdges {
            edge_type,
            source_field,
            destination_field,
        } => {
            let mut edges = unwind_batch_edges(rows, &source_field, &destination_field)?;
            let mut seen = std::collections::BTreeSet::new();
            edges.retain(|edge| seen.insert(*edge));
            Ok(QueryBatchOperation::DeleteEdges { edge_type, edges })
        }
        ParsedUnwindBatchKind::DeleteVertices {
            vertex_field,
            detach,
        } => {
            let mut vertices = rows
                .iter()
                .enumerate()
                .map(|(index, row)| unwind_row_vertex_id(row, index, &vertex_field))
                .collect::<Result<Vec<_>>>()?;
            vertices.sort_unstable();
            vertices.dedup();
            Ok(QueryBatchOperation::DeleteVertices { vertices, detach })
        }
        ParsedUnwindBatchKind::DeleteRelationshipsByProperty {
            edge_type,
            property,
            value_field,
        } => {
            let mut values = rows
                .iter()
                .enumerate()
                .map(|(index, row)| unwind_row_scalar(row, index, &value_field))
                .collect::<Result<Vec<_>>>()?;
            values.sort();
            values.dedup();
            Ok(QueryBatchOperation::DeleteRelationshipsByProperty {
                edge_type,
                property,
                values,
            })
        }
        ParsedUnwindBatchKind::UpsertVertices {
            label,
            vertex_field,
            property_fields,
        } => Ok(QueryBatchOperation::UpsertVertices {
            vertices: rows
                .iter()
                .enumerate()
                .map(|(index, row)| {
                    let mut metadata = VertexMetadata::default().with_label(label.clone());
                    for (property, field) in &property_fields {
                        metadata
                            .properties
                            .insert(property.clone(), unwind_row_scalar(row, index, field)?);
                    }
                    Ok(QueryBatchVertex {
                        vertex: unwind_row_vertex_id(row, index, &vertex_field)?,
                        metadata,
                    })
                })
                .collect::<Result<Vec<_>>>()?,
        }),
        ParsedUnwindBatchKind::CreateRelationshipsBetweenLabeledVertices {
            edge_type,
            source_field,
            destination_field,
            relationship_id_field,
            property_fields,
            source_label,
            destination_label,
        } => Ok(
            QueryBatchOperation::CreateRelationshipsBetweenLabeledVertices {
                edge_type,
                relationships: rows
                    .iter()
                    .enumerate()
                    .map(|(index, row)| {
                        let mut metadata = EdgeMetadata::default();
                        for (property, field) in &property_fields {
                            metadata
                                .properties
                                .insert(property.clone(), unwind_row_scalar(row, index, field)?);
                        }
                        let relationship_id =
                            unwind_row_vertex_id(row, index, &relationship_id_field)?;
                        metadata.properties.insert(
                            "id".to_string(),
                            VertexPropertyValue::Integer(relationship_id),
                        );
                        Ok(QueryBatchRelationship {
                            src: unwind_row_vertex_id(row, index, &source_field)?,
                            dst: unwind_row_vertex_id(row, index, &destination_field)?,
                            metadata,
                        })
                    })
                    .collect::<Result<Vec<_>>>()?,
                source_label,
                destination_label,
            },
        ),
        ParsedUnwindBatchKind::MergeRelationshipsBetweenLabeledVertices {
            edge_type,
            source_field,
            destination_field,
            relationship_id_field,
            property_fields,
            source_label,
            destination_label,
        } => Ok(
            QueryBatchOperation::MergeRelationshipsBetweenLabeledVertices {
                edge_type,
                relationships: rows
                    .iter()
                    .enumerate()
                    .map(|(index, row)| {
                        let mut metadata = EdgeMetadata::default();
                        for (property, field) in &property_fields {
                            metadata
                                .properties
                                .insert(property.clone(), unwind_row_scalar(row, index, field)?);
                        }
                        Ok(QueryBatchRelationshipMerge {
                            src: unwind_row_vertex_id(row, index, &source_field)?,
                            dst: unwind_row_vertex_id(row, index, &destination_field)?,
                            relationship_id: unwind_row_vertex_id(
                                row,
                                index,
                                &relationship_id_field,
                            )?,
                            metadata,
                        })
                    })
                    .collect::<Result<Vec<_>>>()?,
                source_label,
                destination_label,
            },
        ),
    }
}

fn unwind_batch_edges(
    rows: &[QueryParameterValue],
    source_field: &str,
    destination_field: &str,
) -> Result<Vec<QueryBatchEdge>> {
    rows.iter()
        .enumerate()
        .map(|(index, row)| {
            Ok(QueryBatchEdge {
                src: unwind_row_vertex_id(row, index, source_field)?,
                dst: unwind_row_vertex_id(row, index, destination_field)?,
            })
        })
        .collect()
}

fn unwind_row_vertex_id(
    row: &QueryParameterValue,
    index: usize,
    field: &str,
) -> Result<crate::VertexId> {
    let QueryParameterValue::Map(row) = row else {
        return Err(GraphError::UnsupportedQuery {
            dialect: "OpenCypher",
            feature: format!("UNWIND row {index} must be a map"),
        });
    };
    let value = row.get(field).ok_or_else(|| GraphError::UnsupportedQuery {
        dialect: "OpenCypher",
        feature: format!("UNWIND row {index} is missing field {field}"),
    })?;
    match value {
        QueryParameterValue::Scalar(VertexPropertyValue::Integer(value)) => Ok(*value),
        QueryParameterValue::Scalar(VertexPropertyValue::SignedInteger(value)) if *value >= 0 => {
            Ok(*value as u64)
        }
        QueryParameterValue::Scalar(_)
        | QueryParameterValue::List(_)
        | QueryParameterValue::Map(_) => Err(GraphError::UnsupportedQuery {
            dialect: "OpenCypher",
            feature: format!("UNWIND row {index} field {field} must be a non-negative integer"),
        }),
    }
}

fn unwind_row_scalar(
    row: &QueryParameterValue,
    index: usize,
    field: &str,
) -> Result<VertexPropertyValue> {
    let QueryParameterValue::Map(row) = row else {
        return Err(GraphError::UnsupportedQuery {
            dialect: "OpenCypher",
            feature: format!("UNWIND row {index} must be a map"),
        });
    };
    match row.get(field) {
        Some(QueryParameterValue::Scalar(value)) => Ok(value.clone()),
        Some(QueryParameterValue::List(_) | QueryParameterValue::Map(_)) => {
            Err(GraphError::UnsupportedQuery {
                dialect: "OpenCypher",
                feature: format!("UNWIND row {index} field {field} must be scalar"),
            })
        }
        None => Err(GraphError::UnsupportedQuery {
            dialect: "OpenCypher",
            feature: format!("UNWIND row {index} is missing field {field}"),
        }),
    }
}

fn batch_operation_columns(operation: &QueryBatchOperation) -> Vec<QueryColumn> {
    match operation {
        QueryBatchOperation::OutNeighbors {
            source_column,
            destination_column,
            ..
        } => vec![source_column.clone(), destination_column.clone()],
        QueryBatchOperation::CreateEdges { .. }
        | QueryBatchOperation::CreateEdgesBetweenLabeledVertices { .. }
        | QueryBatchOperation::DeleteEdges { .. }
        | QueryBatchOperation::DeleteVertices { .. }
        | QueryBatchOperation::DeleteRelationshipsByProperty { .. }
        | QueryBatchOperation::UpsertVertices { .. }
        | QueryBatchOperation::CreateRelationshipsBetweenLabeledVertices { .. }
        | QueryBatchOperation::MergeRelationshipsBetweenLabeledVertices { .. } => Vec::new(),
    }
}

fn enforce_limit(operation: &'static str, actual: usize, limit: usize) -> Result<()> {
    if actual > limit {
        return Err(GraphError::AdmissionRejected {
            operation,
            actual: actual as u64,
            limit: limit as u64,
        });
    }
    Ok(())
}

fn authentication_error() -> GraphError {
    GraphError::UnsupportedQuery {
        dialect: "ClientProtocol",
        feature: "unauthorized client request".to_string(),
    }
}

fn client_query_cancelled() -> GraphError {
    GraphError::QueryTimeout {
        operation: "client_query_cancelled",
        elapsed_ms: 0,
        limit_ms: 0,
    }
}

fn client_query_runtime_exceeded(limit_ms: u64) -> GraphError {
    GraphError::QueryTimeout {
        operation: "client_query_runtime",
        elapsed_ms: limit_ms,
        limit_ms,
    }
}

#[cfg(test)]
mod tests;
