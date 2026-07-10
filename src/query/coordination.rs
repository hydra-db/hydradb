use std::collections::{BTreeMap, BTreeSet};
#[cfg(feature = "query-transport")]
use std::future::Future;
#[cfg(feature = "query-transport")]
use std::net::SocketAddr;
#[cfg(feature = "query-transport")]
use std::panic::AssertUnwindSafe;
#[cfg(feature = "query-transport")]
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
#[cfg(feature = "query-transport-tls")]
use std::sync::RwLock;
#[cfg(feature = "query-transport")]
use std::time::{Duration, Instant};

use async_trait::async_trait;
#[cfg(feature = "query-service-discovery")]
use base64::Engine;
use futures::future::join_all;
#[cfg(feature = "query-transport")]
use futures::FutureExt;
#[cfg(feature = "query-transport")]
use sha2::{Digest, Sha256};
#[cfg(feature = "query-transport")]
use subtle::ConstantTimeEq;
#[cfg(feature = "query-transport")]
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
#[cfg(feature = "query-transport")]
use tokio::net::{TcpListener, TcpStream};
#[cfg(feature = "query-transport")]
use tokio::sync::{watch, Mutex, OwnedSemaphorePermit, Semaphore};
#[cfg(feature = "query-transport")]
use tokio::task::{JoinHandle, JoinSet};
#[cfg(feature = "query-transport-tls")]
use tokio_rustls::rustls::{
    pki_types::ServerName, ClientConfig as RustlsClientConfig, ServerConfig as RustlsServerConfig,
};
#[cfg(feature = "query-transport-tls")]
use tokio_rustls::{TlsAcceptor, TlsConnector};

#[cfg(feature = "query-transport")]
use crate::QueryCancellationToken;
use crate::{
    validate_component, GraphError, GraphId, GraphScope, NamespacePath, QueryContext,
    QueryCursorToken, QueryResultPage, QueryResultSet, QueryRow, QueryValue, Result,
    RoutedGraphCluster, ShardPlacement,
};

#[cfg(feature = "query-transport")]
const QUERY_TRANSPORT_VERSION: u16 = 2;
#[cfg(feature = "query-transport")]
const QUERY_TRANSPORT_LEGACY_VERSION: u16 = 1;
#[cfg(feature = "query-transport")]
const DEFAULT_QUERY_TRANSPORT_MAX_FRAME_BYTES: usize = 1 << 20;
#[cfg(feature = "query-transport")]
const DEFAULT_QUERY_TRANSPORT_TIMEOUT_MS: u64 = 30_000;
#[cfg(feature = "query-transport")]
const DEFAULT_QUERY_TRANSPORT_MAX_CONNECTIONS: usize = 256;
#[cfg(feature = "query-transport")]
const DEFAULT_QUERY_TRANSPORT_MAX_REQUESTS_PER_CONNECTION: usize = 1_024;
#[cfg(feature = "query-transport")]
const DEFAULT_QUERY_TRANSPORT_HANDSHAKE_TIMEOUT_MS: u64 = 10_000;
#[cfg(feature = "query-transport")]
const DEFAULT_QUERY_TRANSPORT_IDLE_TIMEOUT_MS: u64 = 30_000;
#[cfg(feature = "query-transport")]
const DEFAULT_QUERY_TRANSPORT_GRACEFUL_SHUTDOWN_MS: u64 = 30_000;
#[cfg(feature = "query-transport")]
const DEFAULT_QUERY_TRANSPORT_MAX_IDLE_CONNECTIONS: usize = 16;
#[cfg(feature = "query-transport")]
const DEFAULT_QUERY_TRANSPORT_CLIENT_MAX_CONNECTIONS: usize = 128;
#[cfg(feature = "query-transport")]
const DEFAULT_QUERY_TRANSPORT_CLIENT_MAX_CONTROL_CONNECTIONS: usize = 8;
#[cfg(feature = "query-transport")]
const DEFAULT_QUERY_TRANSPORT_RESERVED_CONTROL_CONNECTIONS: usize = 8;
#[cfg(feature = "query-transport")]
const DEFAULT_QUERY_TRANSPORT_MAX_CONNECTION_AGE_MS: u64 = 300_000;

#[cfg(feature = "query-transport")]
#[derive(Clone)]
pub struct QueryTransportClientConfig {
    pub max_frame_bytes: usize,
    pub timeout: Duration,
    pub bearer_token: Option<QueryTransportSecret>,
    pub max_retries: usize,
    pub max_idle_connections: usize,
    pub max_connections: usize,
    pub max_control_connections: usize,
    pub max_connection_age: Duration,
    pub allow_plaintext: bool,
    #[cfg(feature = "query-transport-tls")]
    pub tls_server_name: Option<String>,
    #[cfg(feature = "query-transport-tls")]
    pub tls_config: Option<Arc<RustlsClientConfig>>,
    #[cfg(feature = "query-transport-tls")]
    pub tls_config_provider: Option<Arc<dyn QueryTransportTlsClientConfigProvider>>,
}

#[cfg(feature = "query-transport")]
impl Default for QueryTransportClientConfig {
    fn default() -> Self {
        Self {
            max_frame_bytes: DEFAULT_QUERY_TRANSPORT_MAX_FRAME_BYTES,
            timeout: Duration::from_millis(DEFAULT_QUERY_TRANSPORT_TIMEOUT_MS),
            bearer_token: None,
            max_retries: 0,
            max_idle_connections: DEFAULT_QUERY_TRANSPORT_MAX_IDLE_CONNECTIONS,
            max_connections: DEFAULT_QUERY_TRANSPORT_CLIENT_MAX_CONNECTIONS,
            max_control_connections: DEFAULT_QUERY_TRANSPORT_CLIENT_MAX_CONTROL_CONNECTIONS,
            max_connection_age: Duration::from_millis(
                DEFAULT_QUERY_TRANSPORT_MAX_CONNECTION_AGE_MS,
            ),
            allow_plaintext: false,
            #[cfg(feature = "query-transport-tls")]
            tls_server_name: None,
            #[cfg(feature = "query-transport-tls")]
            tls_config: None,
            #[cfg(feature = "query-transport-tls")]
            tls_config_provider: None,
        }
    }
}

#[cfg(feature = "query-transport")]
#[derive(Clone, Eq, PartialEq)]
pub struct QueryTransportSecret {
    value: Arc<str>,
    valid: bool,
}

#[cfg(feature = "query-transport")]
impl QueryTransportSecret {
    pub fn try_new(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        if value.trim().is_empty() {
            return Err(GraphError::UnsupportedQuery {
                dialect: "QueryTransport",
                feature: "bearer token cannot be empty".to_string(),
            });
        }
        Ok(Self {
            value: Arc::from(value),
            valid: true,
        })
    }

    pub fn new(value: impl Into<String>) -> Self {
        let value = value.into();
        Self::try_new(value).unwrap_or_else(|_| Self {
            value: Arc::from(""),
            valid: false,
        })
    }

    pub fn expose_secret(&self) -> &str {
        &self.value
    }

    fn is_valid(&self) -> bool {
        self.valid
    }
}

#[cfg(feature = "query-transport")]
impl std::fmt::Debug for QueryTransportSecret {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("QueryTransportSecret(REDACTED)")
    }
}

#[cfg(feature = "query-transport")]
impl From<String> for QueryTransportSecret {
    fn from(value: String) -> Self {
        Self::new(value)
    }
}

#[cfg(feature = "query-transport")]
impl From<&str> for QueryTransportSecret {
    fn from(value: &str) -> Self {
        Self::new(value)
    }
}

#[cfg(feature = "query-transport")]
impl QueryTransportClientConfig {
    pub fn with_bearer_token(mut self, token: impl Into<String>) -> Self {
        self.bearer_token = QueryTransportSecret::try_new(token).ok();
        self
    }

    pub fn with_max_frame_bytes(mut self, max_frame_bytes: usize) -> Self {
        self.max_frame_bytes = max_frame_bytes;
        self
    }

    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    pub fn with_max_retries(mut self, max_retries: usize) -> Self {
        self.max_retries = max_retries;
        self
    }

    pub fn with_max_idle_connections(mut self, max_idle_connections: usize) -> Self {
        self.max_idle_connections = max_idle_connections;
        self
    }

    pub fn with_max_connections(mut self, max_connections: usize) -> Self {
        self.max_connections = max_connections.max(1);
        self
    }

    pub fn with_max_control_connections(mut self, max_control_connections: usize) -> Self {
        self.max_control_connections = max_control_connections.max(1);
        self
    }

    pub fn with_max_connection_age(mut self, max_connection_age: Duration) -> Self {
        self.max_connection_age = max_connection_age;
        self
    }

    pub fn insecure_allow_plaintext(mut self) -> Self {
        self.allow_plaintext = true;
        self
    }

    #[cfg(feature = "query-transport-tls")]
    pub fn with_tls(
        mut self,
        server_name: impl Into<String>,
        config: Arc<RustlsClientConfig>,
    ) -> Self {
        self.tls_server_name = Some(server_name.into());
        self.tls_config = Some(config);
        self.tls_config_provider = None;
        self
    }

    #[cfg(feature = "query-transport-tls")]
    pub fn with_tls_provider(
        mut self,
        server_name: impl Into<String>,
        provider: Arc<dyn QueryTransportTlsClientConfigProvider>,
    ) -> Self {
        self.tls_server_name = Some(server_name.into());
        self.tls_config = None;
        self.tls_config_provider = Some(provider);
        self
    }

    fn validate(&self) -> Result<()> {
        if self.max_frame_bytes < 2 {
            return transport_config_error("max_frame_bytes must be at least 2");
        }
        if self.timeout.is_zero() {
            return transport_config_error("client timeout must be greater than zero");
        }
        if self.max_connection_age.is_zero() {
            return transport_config_error("max_connection_age must be greater than zero");
        }
        if self.max_connections == 0 || self.max_control_connections == 0 {
            return transport_config_error(
                "client connection and control-connection limits must be greater than zero",
            );
        }
        #[cfg(feature = "query-transport-tls")]
        {
            if self.tls_config.is_some() && self.tls_config_provider.is_some() {
                return transport_config_error(
                    "set either a static TLS config or a TLS provider, not both",
                );
            }
            let has_tls = self.tls_config.is_some() || self.tls_config_provider.is_some();
            let missing_server_name = match self.tls_server_name.as_deref() {
                Some(server_name) => server_name.is_empty(),
                None => true,
            };
            if has_tls && missing_server_name {
                return transport_config_error("TLS requires a non-empty server name");
            }
            if !has_tls && self.tls_server_name.is_some() {
                return transport_config_error("TLS server name was set without a TLS config");
            }
            if self.bearer_token.is_some() && !has_tls && !self.allow_plaintext {
                return transport_config_error(
                    "bearer authentication requires TLS unless plaintext is explicitly allowed",
                );
            }
        }
        #[cfg(not(feature = "query-transport-tls"))]
        if self.bearer_token.is_some() && !self.allow_plaintext {
            return transport_config_error(
                "bearer authentication requires TLS unless plaintext is explicitly allowed",
            );
        }
        Ok(())
    }
}

#[cfg(feature = "query-transport")]
#[derive(Clone)]
pub enum QueryTransportAuthPolicy {
    RejectAll,
    BearerToken(QueryTransportSecret),
    BearerTokens(Vec<QueryTransportSecret>),
    #[cfg(feature = "query-transport-tls")]
    MtlsPeerFingerprint {
        allowed_fingerprints: BTreeSet<String>,
        bearer_token: Option<QueryTransportSecret>,
    },
    InsecureAllowAll,
}

#[cfg(feature = "query-transport")]
impl QueryTransportAuthPolicy {
    fn authenticate(
        &self,
        auth: &QueryTransportAuth,
        _identity: &QueryTransportConnectionIdentity,
    ) -> Option<QueryTransportPrincipal> {
        match self {
            Self::RejectAll => None,
            Self::BearerToken(required) => {
                let supplied = auth.bearer_token.as_deref()?;
                (required.is_valid() && constant_time_secret_eq(supplied, required.expose_secret()))
                    .then(|| QueryTransportPrincipal::bearer(required.expose_secret()))
            }
            Self::BearerTokens(required_tokens) => {
                let supplied = auth.bearer_token.as_deref()?;
                let mut matched = None;
                for required in required_tokens {
                    if required.is_valid()
                        && constant_time_secret_eq(supplied, required.expose_secret())
                    {
                        matched = Some(QueryTransportPrincipal::bearer(required.expose_secret()));
                    }
                }
                matched
            }
            #[cfg(feature = "query-transport-tls")]
            Self::MtlsPeerFingerprint {
                allowed_fingerprints,
                bearer_token,
            } => {
                let bearer_ok = match bearer_token {
                    Some(required) => match auth.bearer_token.as_deref() {
                        Some(supplied) => {
                            required.is_valid()
                                && constant_time_secret_eq(supplied, required.expose_secret())
                        }
                        None => false,
                    },
                    None => true,
                };
                if !bearer_ok {
                    return None;
                }
                let authorized = _identity
                    .tls_peer_fingerprints
                    .iter()
                    .any(|fingerprint| allowed_fingerprints.contains(fingerprint));
                authorized
                    .then_some(_identity.tls_peer_leaf_fingerprint.as_deref())
                    .flatten()
                    .map(QueryTransportPrincipal::mtls)
            }
            Self::InsecureAllowAll => Some(QueryTransportPrincipal::insecure()),
        }
    }
}

#[cfg(feature = "query-transport")]
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct QueryTransportPrincipal(String);

#[cfg(feature = "query-transport")]
impl QueryTransportPrincipal {
    fn bearer(token: &str) -> Self {
        Self(format!(
            "bearer:sha256:{}",
            lowercase_hex(&Sha256::digest(token.as_bytes()))
        ))
    }

    #[cfg(feature = "query-transport-tls")]
    fn mtls(fingerprint: &str) -> Self {
        Self(format!("mtls:{fingerprint}"))
    }

    fn insecure() -> Self {
        Self("insecure".to_string())
    }

    pub fn from_bearer_token(token: impl Into<String>) -> Result<Self> {
        let secret = QueryTransportSecret::try_new(token)?;
        Ok(Self::bearer(secret.expose_secret()))
    }

    #[cfg(feature = "query-transport-tls")]
    pub fn from_mtls_peer_fingerprint(fingerprint: impl Into<String>) -> Result<Self> {
        let fingerprints =
            normalized_sha256_fingerprints([fingerprint.into()]).ok_or_else(|| {
                GraphError::UnsupportedQuery {
                    dialect: "QueryTransport",
                    feature: "mTLS principal requires a canonical sha256 fingerprint".to_string(),
                }
            })?;
        let fingerprint =
            fingerprints
                .into_iter()
                .next()
                .ok_or_else(|| GraphError::UnsupportedQuery {
                    dialect: "QueryTransport",
                    feature: "mTLS principal requires a fingerprint".to_string(),
                })?;
        Ok(Self::mtls(&fingerprint))
    }

    pub fn insecure_principal() -> Self {
        Self::insecure()
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    fn error_label(&self) -> &'static str {
        if self.0.starts_with("bearer:") {
            "bearer principal"
        } else if self.0.starts_with("mtls:") {
            "mTLS principal"
        } else {
            "unauthenticated principal"
        }
    }
}

#[cfg(feature = "query-transport")]
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum QueryTransportAction {
    Read,
    Write,
    Admin,
}

#[cfg(feature = "query-transport")]
impl QueryTransportAction {
    fn as_str(self) -> &'static str {
        match self {
            Self::Read => "read",
            Self::Write => "write",
            Self::Admin => "administer",
        }
    }
}

#[cfg(feature = "query-transport")]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryTransportScopeGrant {
    namespace: NamespacePath,
    graph_id: Option<GraphId>,
    include_descendants: bool,
    actions: BTreeSet<QueryTransportAction>,
}

#[cfg(feature = "query-transport")]
impl QueryTransportScopeGrant {
    pub fn graph(
        scope: GraphScope,
        actions: impl IntoIterator<Item = QueryTransportAction>,
    ) -> Self {
        Self {
            namespace: scope.namespace,
            graph_id: Some(scope.graph_id),
            include_descendants: false,
            actions: actions.into_iter().collect(),
        }
    }

    pub fn namespace(
        namespace: NamespacePath,
        include_descendants: bool,
        actions: impl IntoIterator<Item = QueryTransportAction>,
    ) -> Self {
        Self {
            namespace,
            graph_id: None,
            include_descendants,
            actions: actions.into_iter().collect(),
        }
    }

    pub fn read_graph(scope: GraphScope) -> Self {
        Self::graph(scope, [QueryTransportAction::Read])
    }

    pub fn read_namespace(namespace: NamespacePath, include_descendants: bool) -> Self {
        Self::namespace(namespace, include_descendants, [QueryTransportAction::Read])
    }

    fn allows(&self, scope: &GraphScope, action: QueryTransportAction) -> bool {
        let namespace_allowed = if self.include_descendants {
            scope.namespace.is_descendant_of(&self.namespace)
        } else {
            scope.namespace == self.namespace
        };
        let graph_allowed = match &self.graph_id {
            Some(graph_id) => graph_id == &scope.graph_id,
            None => true,
        };
        namespace_allowed && graph_allowed && self.actions.contains(&action)
    }
}

#[cfg(feature = "query-transport")]
pub trait QueryTransportScopeAuthorizer: Send + Sync {
    fn authorize(
        &self,
        principal: &QueryTransportPrincipal,
        scope: &GraphScope,
        action: QueryTransportAction,
    ) -> bool;
}

#[cfg(feature = "query-transport")]
#[derive(Clone, Default)]
pub struct StaticQueryTransportScopeAuthorizer {
    grants: BTreeMap<QueryTransportPrincipal, Vec<QueryTransportScopeGrant>>,
}

#[cfg(feature = "query-transport")]
impl StaticQueryTransportScopeAuthorizer {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn grant(&mut self, principal: QueryTransportPrincipal, grant: QueryTransportScopeGrant) {
        self.grants.entry(principal).or_default().push(grant);
    }

    pub fn with_grant(
        mut self,
        principal: QueryTransportPrincipal,
        grant: QueryTransportScopeGrant,
    ) -> Self {
        self.grant(principal, grant);
        self
    }

    pub fn with_bearer_grant(
        self,
        token: impl Into<String>,
        grant: QueryTransportScopeGrant,
    ) -> Result<Self> {
        Ok(self.with_grant(QueryTransportPrincipal::from_bearer_token(token)?, grant))
    }

    #[cfg(feature = "query-transport-tls")]
    pub fn with_mtls_grant(
        self,
        fingerprint: impl Into<String>,
        grant: QueryTransportScopeGrant,
    ) -> Result<Self> {
        Ok(self.with_grant(
            QueryTransportPrincipal::from_mtls_peer_fingerprint(fingerprint)?,
            grant,
        ))
    }
}

#[cfg(feature = "query-transport")]
impl QueryTransportScopeAuthorizer for StaticQueryTransportScopeAuthorizer {
    fn authorize(
        &self,
        principal: &QueryTransportPrincipal,
        scope: &GraphScope,
        action: QueryTransportAction,
    ) -> bool {
        self.grants
            .get(principal)
            .is_some_and(|grants| grants.iter().any(|grant| grant.allows(scope, action)))
    }
}

#[cfg(feature = "query-transport")]
struct DefaultQueryTransportScopeAuthorizer;

#[cfg(feature = "query-transport")]
impl QueryTransportScopeAuthorizer for DefaultQueryTransportScopeAuthorizer {
    fn authorize(
        &self,
        _principal: &QueryTransportPrincipal,
        scope: &GraphScope,
        _action: QueryTransportAction,
    ) -> bool {
        scope.is_default()
    }
}

#[cfg(feature = "query-transport")]
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct QueryTransportNamespaceQuotas {
    max_concurrent_queries: BTreeMap<NamespacePath, usize>,
}

#[cfg(feature = "query-transport")]
impl QueryTransportNamespaceQuotas {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_query_limit(mut self, namespace: NamespacePath, max_concurrent: usize) -> Self {
        self.max_concurrent_queries
            .insert(namespace, max_concurrent);
        self
    }

    pub fn query_limit(&self, namespace: &NamespacePath) -> Option<usize> {
        self.max_concurrent_queries.get(namespace).copied()
    }

    fn validate(&self) -> Result<()> {
        if self
            .max_concurrent_queries
            .values()
            .any(|limit| *limit == 0)
        {
            return transport_config_error(
                "namespace query concurrency limits must be greater than zero",
            );
        }
        Ok(())
    }

    fn gates(&self) -> BTreeMap<NamespacePath, Arc<Semaphore>> {
        self.max_concurrent_queries
            .iter()
            .map(|(namespace, limit)| (namespace.clone(), Arc::new(Semaphore::new(*limit))))
            .collect()
    }
}

#[cfg(feature = "query-transport")]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryTransportCancellationPrincipal(QueryTransportPrincipal);

#[cfg(feature = "query-transport")]
impl QueryTransportCancellationPrincipal {
    pub fn bearer_token(token: impl Into<String>) -> Result<Self> {
        let secret = QueryTransportSecret::try_new(token)?;
        Ok(Self(QueryTransportPrincipal::bearer(
            secret.expose_secret(),
        )))
    }

    #[cfg(feature = "query-transport-tls")]
    pub fn mtls_peer_fingerprint(fingerprint: impl Into<String>) -> Result<Self> {
        let fingerprints =
            normalized_sha256_fingerprints([fingerprint.into()]).ok_or_else(|| {
                GraphError::UnsupportedQuery {
                    dialect: "QueryTransport",
                    feature: "mTLS cancellation principal requires a canonical sha256 fingerprint"
                        .to_string(),
                }
            })?;
        let fingerprint =
            fingerprints
                .into_iter()
                .next()
                .ok_or_else(|| GraphError::UnsupportedQuery {
                    dialect: "QueryTransport",
                    feature: "mTLS cancellation principal requires a fingerprint".to_string(),
                })?;
        Ok(Self(QueryTransportPrincipal::mtls(&fingerprint)))
    }

    pub fn insecure() -> Self {
        Self(QueryTransportPrincipal::insecure())
    }
}

#[cfg(feature = "query-transport")]
fn constant_time_secret_eq(left: &str, right: &str) -> bool {
    let left = Sha256::digest(left.as_bytes());
    let right = Sha256::digest(right.as_bytes());
    left.as_slice().ct_eq(right.as_slice()).into()
}

#[cfg(feature = "query-transport")]
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct QueryTransportConnectionIdentity {
    #[cfg(feature = "query-transport-tls")]
    pub tls_peer_fingerprints: BTreeSet<String>,
    #[cfg(feature = "query-transport-tls")]
    pub tls_peer_leaf_fingerprint: Option<String>,
}

#[cfg(feature = "query-transport-tls")]
pub trait QueryTransportTlsServerConfigProvider: Send + Sync {
    fn current_server_config(&self) -> Result<Arc<RustlsServerConfig>>;

    fn generation(&self) -> u64 {
        0
    }
}

#[cfg(feature = "query-transport-tls")]
pub trait QueryTransportTlsClientConfigProvider: Send + Sync {
    fn current_client_config(&self) -> Result<Arc<RustlsClientConfig>>;

    fn generation(&self) -> u64 {
        0
    }
}

#[cfg(feature = "query-transport-tls")]
pub struct StaticQueryTransportTlsServerConfigProvider {
    config: Arc<RustlsServerConfig>,
}

#[cfg(feature = "query-transport-tls")]
impl StaticQueryTransportTlsServerConfigProvider {
    pub fn new(config: Arc<RustlsServerConfig>) -> Self {
        Self { config }
    }
}

#[cfg(feature = "query-transport-tls")]
impl QueryTransportTlsServerConfigProvider for StaticQueryTransportTlsServerConfigProvider {
    fn current_server_config(&self) -> Result<Arc<RustlsServerConfig>> {
        Ok(Arc::clone(&self.config))
    }
}

#[cfg(feature = "query-transport-tls")]
pub struct StaticQueryTransportTlsClientConfigProvider {
    config: Arc<RustlsClientConfig>,
}

#[cfg(feature = "query-transport-tls")]
impl StaticQueryTransportTlsClientConfigProvider {
    pub fn new(config: Arc<RustlsClientConfig>) -> Self {
        Self { config }
    }
}

#[cfg(feature = "query-transport-tls")]
impl QueryTransportTlsClientConfigProvider for StaticQueryTransportTlsClientConfigProvider {
    fn current_client_config(&self) -> Result<Arc<RustlsClientConfig>> {
        Ok(Arc::clone(&self.config))
    }
}

#[cfg(feature = "query-transport-tls")]
pub struct ReloadableQueryTransportTlsServerConfigProvider {
    config: RwLock<Arc<RustlsServerConfig>>,
    generation: AtomicU64,
}

#[cfg(feature = "query-transport-tls")]
impl ReloadableQueryTransportTlsServerConfigProvider {
    pub fn new(config: Arc<RustlsServerConfig>) -> Self {
        Self {
            config: RwLock::new(config),
            generation: AtomicU64::new(0),
        }
    }

    pub fn rotate(&self, config: Arc<RustlsServerConfig>) -> Result<()> {
        let mut guard = self.config.write().map_err(|_| GraphError::CorruptValue {
            key: "query/transport/tls/server_config".to_string(),
            reason: "TLS server config lock is poisoned".to_string(),
        })?;
        *guard = config;
        self.generation.fetch_add(1, Ordering::AcqRel);
        Ok(())
    }
}

#[cfg(feature = "query-transport-tls")]
impl QueryTransportTlsServerConfigProvider for ReloadableQueryTransportTlsServerConfigProvider {
    fn current_server_config(&self) -> Result<Arc<RustlsServerConfig>> {
        let guard = self.config.read().map_err(|_| GraphError::CorruptValue {
            key: "query/transport/tls/server_config".to_string(),
            reason: "TLS server config lock is poisoned".to_string(),
        })?;
        Ok(Arc::clone(&guard))
    }

    fn generation(&self) -> u64 {
        self.generation.load(Ordering::Acquire)
    }
}

#[cfg(feature = "query-transport-tls")]
pub struct ReloadableQueryTransportTlsClientConfigProvider {
    config: RwLock<Arc<RustlsClientConfig>>,
    generation: AtomicU64,
}

#[cfg(feature = "query-transport-tls")]
impl ReloadableQueryTransportTlsClientConfigProvider {
    pub fn new(config: Arc<RustlsClientConfig>) -> Self {
        Self {
            config: RwLock::new(config),
            generation: AtomicU64::new(0),
        }
    }

    pub fn rotate(&self, config: Arc<RustlsClientConfig>) -> Result<()> {
        let mut guard = self.config.write().map_err(|_| GraphError::CorruptValue {
            key: "query/transport/tls/client_config".to_string(),
            reason: "TLS client config lock is poisoned".to_string(),
        })?;
        *guard = config;
        self.generation.fetch_add(1, Ordering::AcqRel);
        Ok(())
    }
}

#[cfg(feature = "query-transport-tls")]
impl QueryTransportTlsClientConfigProvider for ReloadableQueryTransportTlsClientConfigProvider {
    fn current_client_config(&self) -> Result<Arc<RustlsClientConfig>> {
        let guard = self.config.read().map_err(|_| GraphError::CorruptValue {
            key: "query/transport/tls/client_config".to_string(),
            reason: "TLS client config lock is poisoned".to_string(),
        })?;
        Ok(Arc::clone(&guard))
    }

    fn generation(&self) -> u64 {
        self.generation.load(Ordering::Acquire)
    }
}

#[cfg(feature = "query-transport")]
#[derive(Clone)]
pub struct QueryTransportServerConfig {
    pub max_frame_bytes: usize,
    pub auth_policy: QueryTransportAuthPolicy,
    pub scope_authorizer: Arc<dyn QueryTransportScopeAuthorizer>,
    pub namespace_quotas: QueryTransportNamespaceQuotas,
    pub max_concurrent_requests: usize,
    pub max_connections: usize,
    pub reserved_control_connections: usize,
    pub max_requests_per_connection: usize,
    pub max_connection_age: Duration,
    pub handshake_timeout: Duration,
    pub idle_timeout: Duration,
    pub write_timeout: Duration,
    pub graceful_shutdown_timeout: Duration,
    pub allow_plaintext: bool,
    pub slow_query_log_threshold: Option<Duration>,
    #[cfg(feature = "query-transport-tls")]
    pub tls_config: Option<Arc<RustlsServerConfig>>,
    #[cfg(feature = "query-transport-tls")]
    pub tls_config_provider: Option<Arc<dyn QueryTransportTlsServerConfigProvider>>,
}

#[cfg(feature = "query-transport")]
impl Default for QueryTransportServerConfig {
    fn default() -> Self {
        Self {
            max_frame_bytes: DEFAULT_QUERY_TRANSPORT_MAX_FRAME_BYTES,
            auth_policy: QueryTransportAuthPolicy::RejectAll,
            scope_authorizer: Arc::new(DefaultQueryTransportScopeAuthorizer),
            namespace_quotas: QueryTransportNamespaceQuotas::default(),
            max_concurrent_requests: 128,
            max_connections: DEFAULT_QUERY_TRANSPORT_MAX_CONNECTIONS,
            reserved_control_connections: DEFAULT_QUERY_TRANSPORT_RESERVED_CONTROL_CONNECTIONS,
            max_requests_per_connection: DEFAULT_QUERY_TRANSPORT_MAX_REQUESTS_PER_CONNECTION,
            max_connection_age: Duration::from_millis(
                DEFAULT_QUERY_TRANSPORT_MAX_CONNECTION_AGE_MS,
            ),
            handshake_timeout: Duration::from_millis(DEFAULT_QUERY_TRANSPORT_HANDSHAKE_TIMEOUT_MS),
            idle_timeout: Duration::from_millis(DEFAULT_QUERY_TRANSPORT_IDLE_TIMEOUT_MS),
            write_timeout: Duration::from_millis(DEFAULT_QUERY_TRANSPORT_TIMEOUT_MS),
            graceful_shutdown_timeout: Duration::from_millis(
                DEFAULT_QUERY_TRANSPORT_GRACEFUL_SHUTDOWN_MS,
            ),
            allow_plaintext: false,
            slow_query_log_threshold: Some(Duration::from_millis(500)),
            #[cfg(feature = "query-transport-tls")]
            tls_config: None,
            #[cfg(feature = "query-transport-tls")]
            tls_config_provider: None,
        }
    }
}

#[cfg(feature = "query-transport")]
impl QueryTransportServerConfig {
    pub fn with_required_bearer_token(mut self, token: impl Into<String>) -> Self {
        self.auth_policy = match QueryTransportSecret::try_new(token) {
            Ok(secret) => QueryTransportAuthPolicy::BearerToken(secret),
            Err(_) => QueryTransportAuthPolicy::RejectAll,
        };
        self
    }

    pub fn with_required_bearer_tokens(mut self, tokens: impl IntoIterator<Item = String>) -> Self {
        let tokens: Result<Vec<_>> = tokens
            .into_iter()
            .map(QueryTransportSecret::try_new)
            .collect();
        self.auth_policy = match tokens {
            Ok(tokens) if !tokens.is_empty() => QueryTransportAuthPolicy::BearerTokens(tokens),
            _ => QueryTransportAuthPolicy::RejectAll,
        };
        self
    }

    pub fn insecure_allow_unauthenticated(mut self) -> Self {
        self.auth_policy = QueryTransportAuthPolicy::InsecureAllowAll;
        self.allow_plaintext = true;
        self
    }

    pub fn insecure_allow_plaintext(mut self) -> Self {
        self.allow_plaintext = true;
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

    pub fn with_max_frame_bytes(mut self, max_frame_bytes: usize) -> Self {
        self.max_frame_bytes = max_frame_bytes;
        self
    }

    pub fn with_max_concurrent_requests(mut self, max_concurrent_requests: usize) -> Self {
        self.max_concurrent_requests = max_concurrent_requests.max(1);
        self
    }

    pub fn with_max_connections(mut self, max_connections: usize) -> Self {
        self.max_connections = max_connections.max(1);
        self
    }

    pub fn with_reserved_control_connections(
        mut self,
        reserved_control_connections: usize,
    ) -> Self {
        self.reserved_control_connections = reserved_control_connections.max(1);
        self
    }

    pub fn with_max_requests_per_connection(mut self, max_requests_per_connection: usize) -> Self {
        self.max_requests_per_connection = max_requests_per_connection.max(1);
        self
    }

    pub fn with_max_connection_age(mut self, max_connection_age: Duration) -> Self {
        self.max_connection_age = max_connection_age;
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

    pub fn with_write_timeout(mut self, timeout: Duration) -> Self {
        self.write_timeout = timeout;
        self
    }

    pub fn with_graceful_shutdown_timeout(mut self, timeout: Duration) -> Self {
        self.graceful_shutdown_timeout = timeout;
        self
    }

    pub fn with_slow_query_log_threshold(mut self, threshold: Option<Duration>) -> Self {
        self.slow_query_log_threshold = threshold;
        self
    }

    #[cfg(feature = "query-transport-tls")]
    pub fn with_tls(mut self, config: Arc<RustlsServerConfig>) -> Self {
        self.tls_config = Some(config);
        self.tls_config_provider = None;
        self
    }

    #[cfg(feature = "query-transport-tls")]
    pub fn with_tls_provider(
        mut self,
        provider: Arc<dyn QueryTransportTlsServerConfigProvider>,
    ) -> Self {
        self.tls_config = None;
        self.tls_config_provider = Some(provider);
        self
    }

    #[cfg(feature = "query-transport-tls")]
    pub fn with_required_mtls_fingerprints(
        mut self,
        allowed_fingerprints: impl IntoIterator<Item = String>,
    ) -> Self {
        self.auth_policy = normalized_sha256_fingerprints(allowed_fingerprints).map_or(
            QueryTransportAuthPolicy::RejectAll,
            |allowed_fingerprints| QueryTransportAuthPolicy::MtlsPeerFingerprint {
                allowed_fingerprints,
                bearer_token: None,
            },
        );
        self
    }

    #[cfg(feature = "query-transport-tls")]
    pub fn with_required_mtls_fingerprints_and_bearer(
        mut self,
        allowed_fingerprints: impl IntoIterator<Item = String>,
        token: impl Into<String>,
    ) -> Self {
        self.auth_policy = match QueryTransportSecret::try_new(token) {
            Ok(secret) => normalized_sha256_fingerprints(allowed_fingerprints).map_or(
                QueryTransportAuthPolicy::RejectAll,
                |allowed_fingerprints| QueryTransportAuthPolicy::MtlsPeerFingerprint {
                    allowed_fingerprints,
                    bearer_token: Some(secret),
                },
            ),
            Err(_) => QueryTransportAuthPolicy::RejectAll,
        };
        self
    }

    fn validate(&self) -> Result<()> {
        self.namespace_quotas.validate()?;
        if self.max_frame_bytes < 2 {
            return transport_config_error("max_frame_bytes must be at least 2");
        }
        if self.max_connections == 0
            || self.reserved_control_connections == 0
            || self.max_concurrent_requests == 0
        {
            return transport_config_error(
                "connection, reserved control-connection, and request limits must be greater than zero",
            );
        }
        if self.max_requests_per_connection == 0 {
            return transport_config_error("max_requests_per_connection must be greater than zero");
        }
        if self.handshake_timeout.is_zero()
            || self.max_connection_age.is_zero()
            || self.idle_timeout.is_zero()
            || self.write_timeout.is_zero()
            || self.graceful_shutdown_timeout.is_zero()
        {
            return transport_config_error("transport timeouts must be greater than zero");
        }
        #[cfg(feature = "query-transport-tls")]
        let tls_configured = self.tls_config.is_some() || self.tls_config_provider.is_some();
        #[cfg(not(feature = "query-transport-tls"))]
        let tls_configured = false;

        #[cfg(feature = "query-transport-tls")]
        if self.tls_config.is_some() && self.tls_config_provider.is_some() {
            return transport_config_error(
                "set either a static TLS config or a TLS provider, not both",
            );
        }

        match &self.auth_policy {
            QueryTransportAuthPolicy::BearerToken(secret) if !secret.is_valid() => {
                return transport_config_error("bearer token cannot be empty");
            }
            QueryTransportAuthPolicy::BearerTokens(tokens)
                if tokens.is_empty() || tokens.iter().any(|secret| !secret.is_valid()) =>
            {
                return transport_config_error(
                    "bearer token list must be non-empty and cannot contain empty tokens",
                );
            }
            #[cfg(feature = "query-transport-tls")]
            QueryTransportAuthPolicy::MtlsPeerFingerprint {
                allowed_fingerprints,
                bearer_token,
            } => {
                let normalized =
                    normalized_sha256_fingerprints(allowed_fingerprints.iter().cloned());
                if normalized.as_ref() != Some(allowed_fingerprints) {
                    return transport_config_error(
                        "mTLS fingerprints must be canonical sha256 values",
                    );
                }
                if bearer_token
                    .as_ref()
                    .is_some_and(|secret| !secret.is_valid())
                {
                    return transport_config_error("bearer token cannot be empty");
                }
            }
            _ => {}
        }

        if !tls_configured
            && !self.allow_plaintext
            && !matches!(self.auth_policy, QueryTransportAuthPolicy::RejectAll)
        {
            return transport_config_error(
                "authenticated query transport requires TLS unless plaintext is explicitly allowed",
            );
        }
        #[cfg(feature = "query-transport-tls")]
        if matches!(
            self.auth_policy,
            QueryTransportAuthPolicy::MtlsPeerFingerprint { .. }
        ) && !tls_configured
        {
            return transport_config_error("mTLS authentication requires a TLS server config");
        }
        Ok(())
    }
}

#[cfg(feature = "query-transport-tls")]
fn normalized_sha256_fingerprints(
    fingerprints: impl IntoIterator<Item = String>,
) -> Option<BTreeSet<String>> {
    let mut normalized = BTreeSet::new();
    for fingerprint in fingerprints {
        let fingerprint = fingerprint.trim().to_ascii_lowercase();
        let digest = fingerprint.strip_prefix("sha256:")?;
        if digest.len() != 64 || !digest.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            return None;
        }
        normalized.insert(format!("sha256:{digest}"));
    }
    (!normalized.is_empty()).then_some(normalized)
}

#[cfg(feature = "query-transport")]
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct QueryTransportMetricsSnapshot {
    pub requests_started: u64,
    pub requests_completed: u64,
    pub requests_failed: u64,
    pub auth_failures: u64,
    pub namespace_access_denials: u64,
    pub namespace_quota_waits: u64,
    pub cancellations: u64,
    pub cancelled_rejections: u64,
    pub slow_queries: u64,
    pub backpressure_waits: u64,
    pub client_retries: u64,
    pub bytes_sent: u64,
    pub bytes_received: u64,
    pub remote_latency_us: u64,
    pub connections_accepted: u64,
    pub connections_active: u64,
    pub connections_rejected: u64,
    pub connections_created: u64,
    pub connections_reused: u64,
    pub client_connection_waits: u64,
    pub handshake_failures: u64,
    pub idle_timeouts: u64,
    pub forced_shutdowns: u64,
}

#[cfg(feature = "query-transport")]
#[derive(Default)]
struct QueryTransportMetrics {
    requests_started: AtomicU64,
    requests_completed: AtomicU64,
    requests_failed: AtomicU64,
    auth_failures: AtomicU64,
    namespace_access_denials: AtomicU64,
    namespace_quota_waits: AtomicU64,
    cancellations: AtomicU64,
    cancelled_rejections: AtomicU64,
    slow_queries: AtomicU64,
    backpressure_waits: AtomicU64,
    client_retries: AtomicU64,
    bytes_sent: AtomicU64,
    bytes_received: AtomicU64,
    remote_latency_us: AtomicU64,
    connections_accepted: AtomicU64,
    connections_active: AtomicU64,
    connections_rejected: AtomicU64,
    connections_created: AtomicU64,
    connections_reused: AtomicU64,
    client_connection_waits: AtomicU64,
    handshake_failures: AtomicU64,
    idle_timeouts: AtomicU64,
    forced_shutdowns: AtomicU64,
}

#[cfg(feature = "query-transport")]
impl QueryTransportMetrics {
    fn snapshot(&self) -> QueryTransportMetricsSnapshot {
        QueryTransportMetricsSnapshot {
            requests_started: self.requests_started.load(Ordering::Relaxed),
            requests_completed: self.requests_completed.load(Ordering::Relaxed),
            requests_failed: self.requests_failed.load(Ordering::Relaxed),
            auth_failures: self.auth_failures.load(Ordering::Relaxed),
            namespace_access_denials: self.namespace_access_denials.load(Ordering::Relaxed),
            namespace_quota_waits: self.namespace_quota_waits.load(Ordering::Relaxed),
            cancellations: self.cancellations.load(Ordering::Relaxed),
            cancelled_rejections: self.cancelled_rejections.load(Ordering::Relaxed),
            slow_queries: self.slow_queries.load(Ordering::Relaxed),
            backpressure_waits: self.backpressure_waits.load(Ordering::Relaxed),
            client_retries: self.client_retries.load(Ordering::Relaxed),
            bytes_sent: self.bytes_sent.load(Ordering::Relaxed),
            bytes_received: self.bytes_received.load(Ordering::Relaxed),
            remote_latency_us: self.remote_latency_us.load(Ordering::Relaxed),
            connections_accepted: self.connections_accepted.load(Ordering::Relaxed),
            connections_active: self.connections_active.load(Ordering::Relaxed),
            connections_rejected: self.connections_rejected.load(Ordering::Relaxed),
            connections_created: self.connections_created.load(Ordering::Relaxed),
            connections_reused: self.connections_reused.load(Ordering::Relaxed),
            client_connection_waits: self.client_connection_waits.load(Ordering::Relaxed),
            handshake_failures: self.handshake_failures.load(Ordering::Relaxed),
            idle_timeouts: self.idle_timeouts.load(Ordering::Relaxed),
            forced_shutdowns: self.forced_shutdowns.load(Ordering::Relaxed),
        }
    }
}

#[cfg(feature = "query-transport")]
#[derive(Clone)]
pub struct QueryServiceEndpoint {
    pub node_id: String,
    pub addr: SocketAddr,
    pub client_config: QueryTransportClientConfig,
}

#[cfg(feature = "query-transport")]
impl QueryServiceEndpoint {
    pub fn new(node_id: impl Into<String>, addr: SocketAddr) -> Self {
        Self {
            node_id: node_id.into(),
            addr,
            client_config: QueryTransportClientConfig::default(),
        }
    }

    pub fn with_client_config(mut self, client_config: QueryTransportClientConfig) -> Self {
        self.client_config = client_config;
        self
    }
}

#[cfg(feature = "query-transport")]
#[derive(Clone, Default)]
pub struct QueryServiceDirectory {
    endpoints: BTreeMap<String, QueryServiceEndpoint>,
}

#[cfg(feature = "query-transport")]
impl QueryServiceDirectory {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(&mut self, endpoint: QueryServiceEndpoint) -> Result<()> {
        validate_component("node_id", &endpoint.node_id)?;
        self.endpoints.insert(endpoint.node_id.clone(), endpoint);
        Ok(())
    }

    pub fn with_endpoint(mut self, endpoint: QueryServiceEndpoint) -> Result<Self> {
        self.insert(endpoint)?;
        Ok(self)
    }

    pub fn endpoint(&self, node_id: &str) -> Option<&QueryServiceEndpoint> {
        self.endpoints.get(node_id)
    }

    pub fn endpoints(&self) -> impl Iterator<Item = &QueryServiceEndpoint> {
        self.endpoints.values()
    }
}

#[cfg(feature = "query-transport")]
#[async_trait]
pub trait QueryServiceDiscovery: Send + Sync {
    async fn resolve_query_services(&self) -> Result<QueryServiceDirectory>;
}

#[cfg(feature = "query-transport")]
#[derive(Clone)]
pub struct StaticQueryServiceDiscovery {
    directory: QueryServiceDirectory,
}

#[cfg(feature = "query-transport")]
impl StaticQueryServiceDiscovery {
    pub fn new(directory: QueryServiceDirectory) -> Self {
        Self { directory }
    }
}

#[cfg(feature = "query-transport")]
#[async_trait]
impl QueryServiceDiscovery for StaticQueryServiceDiscovery {
    async fn resolve_query_services(&self) -> Result<QueryServiceDirectory> {
        Ok(self.directory.clone())
    }
}

#[cfg(feature = "query-service-discovery")]
#[derive(Clone)]
pub struct KubernetesQueryServiceDiscovery {
    api_server: String,
    namespace: String,
    label_selector: String,
    port_name: Option<String>,
    bearer_token: Option<QueryTransportSecret>,
    client_config: QueryTransportClientConfig,
    http: reqwest::Client,
}

#[cfg(feature = "query-service-discovery")]
impl KubernetesQueryServiceDiscovery {
    pub fn new(
        api_server: impl Into<String>,
        namespace: impl Into<String>,
        label_selector: impl Into<String>,
    ) -> Self {
        Self {
            api_server: api_server.into().trim_end_matches('/').to_string(),
            namespace: namespace.into(),
            label_selector: label_selector.into(),
            port_name: None,
            bearer_token: None,
            client_config: QueryTransportClientConfig::default(),
            http: reqwest::Client::new(),
        }
    }

    pub fn with_port_name(mut self, port_name: impl Into<String>) -> Self {
        self.port_name = Some(port_name.into());
        self
    }

    pub fn with_bearer_token(mut self, token: impl Into<String>) -> Self {
        self.bearer_token = QueryTransportSecret::try_new(token).ok();
        self
    }

    pub fn with_client_config(mut self, client_config: QueryTransportClientConfig) -> Self {
        self.client_config = client_config;
        self
    }

    pub fn directory_from_endpointslices_json(
        value: &serde_json::Value,
        port_name: Option<&str>,
        client_config: QueryTransportClientConfig,
    ) -> Result<QueryServiceDirectory> {
        parse_kubernetes_endpointslices(value, port_name, client_config)
    }
}

#[cfg(feature = "query-service-discovery")]
#[async_trait]
impl QueryServiceDiscovery for KubernetesQueryServiceDiscovery {
    async fn resolve_query_services(&self) -> Result<QueryServiceDirectory> {
        let url = format!(
            "{}/apis/discovery.k8s.io/v1/namespaces/{}/endpointslices",
            self.api_server, self.namespace
        );
        let mut request = self
            .http
            .get(url)
            .query(&[("labelSelector", self.label_selector.as_str())]);
        if let Some(token) = &self.bearer_token {
            request = request.bearer_auth(token.expose_secret());
        }
        let value = request
            .send()
            .await
            .map_err(|err| service_discovery_error("kubernetes/send", err))?
            .error_for_status()
            .map_err(|err| service_discovery_error("kubernetes/status", err))?
            .json::<serde_json::Value>()
            .await
            .map_err(|err| service_discovery_error("kubernetes/decode", err))?;
        parse_kubernetes_endpointslices(
            &value,
            self.port_name.as_deref(),
            self.client_config.clone(),
        )
    }
}

#[cfg(feature = "query-service-discovery")]
#[derive(Clone)]
pub struct ConsulQueryServiceDiscovery {
    base_url: String,
    service_name: String,
    token: Option<QueryTransportSecret>,
    client_config: QueryTransportClientConfig,
    http: reqwest::Client,
}

#[cfg(feature = "query-service-discovery")]
impl ConsulQueryServiceDiscovery {
    pub fn new(base_url: impl Into<String>, service_name: impl Into<String>) -> Self {
        Self {
            base_url: base_url.into().trim_end_matches('/').to_string(),
            service_name: service_name.into(),
            token: None,
            client_config: QueryTransportClientConfig::default(),
            http: reqwest::Client::new(),
        }
    }

    pub fn with_token(mut self, token: impl Into<String>) -> Self {
        self.token = QueryTransportSecret::try_new(token).ok();
        self
    }

    pub fn with_client_config(mut self, client_config: QueryTransportClientConfig) -> Self {
        self.client_config = client_config;
        self
    }

    pub fn directory_from_health_service_json(
        value: &serde_json::Value,
        client_config: QueryTransportClientConfig,
    ) -> Result<QueryServiceDirectory> {
        parse_consul_health_service(value, client_config)
    }
}

#[cfg(feature = "query-service-discovery")]
#[async_trait]
impl QueryServiceDiscovery for ConsulQueryServiceDiscovery {
    async fn resolve_query_services(&self) -> Result<QueryServiceDirectory> {
        let url = format!("{}/v1/health/service/{}", self.base_url, self.service_name);
        let mut request = self.http.get(url).query(&[("passing", "1")]);
        if let Some(token) = &self.token {
            request = request.header("X-Consul-Token", token.expose_secret());
        }
        let value = request
            .send()
            .await
            .map_err(|err| service_discovery_error("consul/send", err))?
            .error_for_status()
            .map_err(|err| service_discovery_error("consul/status", err))?
            .json::<serde_json::Value>()
            .await
            .map_err(|err| service_discovery_error("consul/decode", err))?;
        parse_consul_health_service(&value, self.client_config.clone())
    }
}

#[cfg(feature = "query-service-discovery")]
#[derive(Clone)]
pub struct EtcdQueryServiceDiscovery {
    endpoint: String,
    prefix: String,
    token: Option<QueryTransportSecret>,
    client_config: QueryTransportClientConfig,
    http: reqwest::Client,
}

#[cfg(feature = "query-service-discovery")]
impl EtcdQueryServiceDiscovery {
    pub fn new(endpoint: impl Into<String>, prefix: impl Into<String>) -> Self {
        Self {
            endpoint: endpoint.into().trim_end_matches('/').to_string(),
            prefix: prefix.into(),
            token: None,
            client_config: QueryTransportClientConfig::default(),
            http: reqwest::Client::new(),
        }
    }

    pub fn with_token(mut self, token: impl Into<String>) -> Self {
        self.token = QueryTransportSecret::try_new(token).ok();
        self
    }

    pub fn with_client_config(mut self, client_config: QueryTransportClientConfig) -> Self {
        self.client_config = client_config;
        self
    }

    pub fn directory_from_range_json(
        value: &serde_json::Value,
        client_config: QueryTransportClientConfig,
    ) -> Result<QueryServiceDirectory> {
        parse_etcd_query_services(value, client_config)
    }
}

#[cfg(feature = "query-service-discovery")]
#[async_trait]
impl QueryServiceDiscovery for EtcdQueryServiceDiscovery {
    async fn resolve_query_services(&self) -> Result<QueryServiceDirectory> {
        let key = base64::engine::general_purpose::STANDARD.encode(self.prefix.as_bytes());
        let range_end =
            base64::engine::general_purpose::STANDARD.encode(etcd_prefix_end(&self.prefix));
        let body = serde_json::json!({ "key": key, "range_end": range_end });
        let mut request = self
            .http
            .post(format!("{}/v3/kv/range", self.endpoint))
            .json(&body);
        if let Some(token) = &self.token {
            request = request.bearer_auth(token.expose_secret());
        }
        let value = request
            .send()
            .await
            .map_err(|err| service_discovery_error("etcd/send", err))?
            .error_for_status()
            .map_err(|err| service_discovery_error("etcd/status", err))?
            .json::<serde_json::Value>()
            .await
            .map_err(|err| service_discovery_error("etcd/decode", err))?;
        parse_etcd_query_services(&value, self.client_config.clone())
    }
}

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
trait QueryTransportIo: AsyncRead + AsyncWrite + Send + Unpin {}

#[cfg(feature = "query-transport")]
impl<T> QueryTransportIo for T where T: AsyncRead + AsyncWrite + Send + Unpin {}

#[cfg(feature = "query-transport")]
struct PooledQueryTransportConnection {
    io: Box<dyn QueryTransportIo>,
    created_at: Instant,
    tls_generation: u64,
}

#[cfg(feature = "query-transport")]
#[derive(Clone)]
pub struct TcpQueryCellClient {
    addr: SocketAddr,
    config: QueryTransportClientConfig,
    metrics: Arc<QueryTransportMetrics>,
    pool: Arc<Mutex<Vec<PooledQueryTransportConnection>>>,
    connection_gate: Arc<Semaphore>,
    control_connection_gate: Arc<Semaphore>,
}

#[cfg(feature = "query-transport")]
impl TcpQueryCellClient {
    pub fn new(addr: SocketAddr) -> Self {
        Self::with_config(addr, QueryTransportClientConfig::default())
    }

    pub fn with_config(addr: SocketAddr, config: QueryTransportClientConfig) -> Self {
        let max_connections = config.max_connections.max(1);
        let max_control_connections = config.max_control_connections.max(1);
        Self {
            addr,
            config,
            metrics: Arc::new(QueryTransportMetrics::default()),
            pool: Arc::new(Mutex::new(Vec::new())),
            connection_gate: Arc::new(Semaphore::new(max_connections)),
            control_connection_gate: Arc::new(Semaphore::new(max_control_connections)),
        }
    }

    pub fn with_max_frame_bytes(mut self, max_frame_bytes: usize) -> Self {
        self.config.max_frame_bytes = max_frame_bytes;
        self
    }

    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.config.timeout = timeout;
        self
    }

    pub fn with_bearer_token(mut self, token: impl Into<String>) -> Self {
        self.config.bearer_token = QueryTransportSecret::try_new(token).ok();
        self
    }

    pub fn with_max_retries(mut self, max_retries: usize) -> Self {
        self.config.max_retries = max_retries;
        self
    }

    pub fn with_max_idle_connections(mut self, max_idle_connections: usize) -> Self {
        self.config.max_idle_connections = max_idle_connections;
        self
    }

    pub fn with_max_connections(mut self, max_connections: usize) -> Self {
        self.config.max_connections = max_connections.max(1);
        self.connection_gate = Arc::new(Semaphore::new(self.config.max_connections));
        self
    }

    pub fn with_max_control_connections(mut self, max_control_connections: usize) -> Self {
        self.config.max_control_connections = max_control_connections.max(1);
        self.control_connection_gate =
            Arc::new(Semaphore::new(self.config.max_control_connections));
        self
    }

    pub fn with_max_connection_age(mut self, max_connection_age: Duration) -> Self {
        self.config.max_connection_age = max_connection_age;
        self
    }

    pub fn insecure_allow_plaintext(mut self) -> Self {
        self.config.allow_plaintext = true;
        self
    }

    #[cfg(feature = "query-transport-tls")]
    pub fn with_tls(
        mut self,
        server_name: impl Into<String>,
        config: Arc<RustlsClientConfig>,
    ) -> Self {
        self.config.tls_server_name = Some(server_name.into());
        self.config.tls_config = Some(config);
        self.config.tls_config_provider = None;
        self.pool = Arc::new(Mutex::new(Vec::new()));
        self
    }

    #[cfg(feature = "query-transport-tls")]
    pub fn with_tls_provider(
        mut self,
        server_name: impl Into<String>,
        provider: Arc<dyn QueryTransportTlsClientConfigProvider>,
    ) -> Self {
        self.config.tls_server_name = Some(server_name.into());
        self.config.tls_config = None;
        self.config.tls_config_provider = Some(provider);
        self.pool = Arc::new(Mutex::new(Vec::new()));
        self
    }

    pub fn metrics(&self) -> QueryTransportMetricsSnapshot {
        self.metrics.snapshot()
    }

    pub async fn clear_idle_connections(&self) {
        self.pool.lock().await.clear();
    }

    pub async fn idle_connection_count(&self) -> usize {
        self.pool.lock().await.len()
    }
}

#[cfg(feature = "query-transport")]
pub struct TcpQueryServer {
    local_addr: SocketAddr,
    stop_tx: watch::Sender<bool>,
    task: JoinHandle<Result<()>>,
    metrics: Arc<QueryTransportMetrics>,
    lifecycle: Arc<Mutex<QueryTransportLifecycle>>,
}

#[cfg(feature = "query-transport")]
struct QueryTransportServerRuntime {
    config: QueryTransportServerConfig,
    metrics: Arc<QueryTransportMetrics>,
    lifecycle: Arc<Mutex<QueryTransportLifecycle>>,
    request_gate: Arc<Semaphore>,
    namespace_query_gates: BTreeMap<NamespacePath, Arc<Semaphore>>,
    connection_gate: Arc<Semaphore>,
    control_connection_gate: Arc<Semaphore>,
}

#[cfg(feature = "query-transport")]
struct QueryTransportConnectionAdmission {
    _permit: OwnedSemaphorePermit,
    control_only: bool,
}

#[cfg(feature = "query-transport")]
#[derive(Default)]
struct QueryTransportLifecycle {
    next_token: QueryLifecycleToken,
    queries: BTreeMap<QueryLifecycleKey, QueryLifecycleEntry>,
}

#[cfg(feature = "query-transport")]
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct QueryLifecycleKey {
    principal: QueryTransportPrincipal,
    scope: GraphScope,
    query_id: String,
}

#[cfg(feature = "query-transport")]
impl QueryLifecycleKey {
    fn new(
        principal: QueryTransportPrincipal,
        scope: GraphScope,
        query_id: impl Into<String>,
    ) -> Self {
        Self {
            principal,
            scope,
            query_id: query_id.into(),
        }
    }
}

#[cfg(feature = "query-transport")]
type QueryLifecycleToken = u64;

#[cfg(feature = "query-transport")]
struct QueryLifecycleEntry {
    token: QueryLifecycleToken,
    state: QueryLifecycleState,
    cancellation_token: QueryCancellationToken,
}

#[cfg(feature = "query-transport")]
struct ActiveQueryTransportConnection {
    metrics: Arc<QueryTransportMetrics>,
}

#[cfg(feature = "query-transport")]
impl ActiveQueryTransportConnection {
    fn new(metrics: Arc<QueryTransportMetrics>) -> Self {
        metrics.connections_active.fetch_add(1, Ordering::Relaxed);
        Self { metrics }
    }
}

#[cfg(feature = "query-transport")]
impl Drop for ActiveQueryTransportConnection {
    fn drop(&mut self) {
        self.metrics
            .connections_active
            .fetch_sub(1, Ordering::Relaxed);
    }
}

#[cfg(feature = "query-transport")]
#[derive(Clone, Copy, Eq, PartialEq)]
enum QueryLifecycleState {
    Queued,
    Running,
    Cancelled,
}

#[cfg(feature = "query-transport")]
impl TcpQueryServer {
    pub async fn bind(addr: SocketAddr, client: Arc<dyn QueryCellClient>) -> Result<Self> {
        Self::bind_with_config(addr, client, QueryTransportServerConfig::default()).await
    }

    pub async fn bind_with_max_frame_bytes(
        addr: SocketAddr,
        client: Arc<dyn QueryCellClient>,
        max_frame_bytes: usize,
    ) -> Result<Self> {
        Self::bind_with_config(
            addr,
            client,
            QueryTransportServerConfig::default().with_max_frame_bytes(max_frame_bytes),
        )
        .await
    }

    pub async fn bind_with_config(
        addr: SocketAddr,
        client: Arc<dyn QueryCellClient>,
        config: QueryTransportServerConfig,
    ) -> Result<Self> {
        config.validate()?;
        let listener = TcpListener::bind(addr)
            .await
            .map_err(|err| transport_error("bind", err))?;
        let local_addr = listener
            .local_addr()
            .map_err(|err| transport_error("local_addr", err))?;
        let (stop_tx, mut stop_rx) = watch::channel(false);
        let metrics = Arc::new(QueryTransportMetrics::default());
        let lifecycle = Arc::new(Mutex::new(QueryTransportLifecycle::default()));
        let max_concurrent_requests = config.max_concurrent_requests.max(1);
        let max_connections = config.max_connections.max(1);
        let reserved_control_connections = config.reserved_control_connections.max(1);
        let namespace_query_gates = config.namespace_quotas.gates();
        let graceful_shutdown_timeout = config.graceful_shutdown_timeout;
        let runtime = Arc::new(QueryTransportServerRuntime {
            config,
            metrics: Arc::clone(&metrics),
            lifecycle: Arc::clone(&lifecycle),
            request_gate: Arc::new(Semaphore::new(max_concurrent_requests)),
            namespace_query_gates,
            connection_gate: Arc::new(Semaphore::new(max_connections)),
            control_connection_gate: Arc::new(Semaphore::new(reserved_control_connections)),
        });
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
                        let (stream, _) = accepted.map_err(|err| transport_error("accept", err))?;
                        if let Err(err) = stream.set_nodelay(true) {
                            tracing::warn!(
                                target: "slatedb_graph_kernel",
                                error = %err,
                                "query transport failed to configure accepted socket"
                            );
                            drop(stream);
                            continue;
                        }
                        let admission = match Arc::clone(&runtime.connection_gate).try_acquire_owned() {
                            Ok(permit) => QueryTransportConnectionAdmission {
                                _permit: permit,
                                control_only: false,
                            },
                            Err(_) => match Arc::clone(&runtime.control_connection_gate).try_acquire_owned() {
                                Ok(permit) => QueryTransportConnectionAdmission {
                                    _permit: permit,
                                    control_only: true,
                                },
                                Err(_) => {
                                    runtime.metrics.connections_rejected.fetch_add(1, Ordering::Relaxed);
                                    drop(stream);
                                    continue;
                                }
                            },
                        };
                        runtime.metrics.connections_accepted.fetch_add(1, Ordering::Relaxed);
                        let client = Arc::clone(&client);
                        let runtime = Arc::clone(&runtime);
                        let connection_stop_rx = stop_rx.clone();
                        connections.spawn(async move {
                            let control_only = admission.control_only;
                            let _admission = admission;
                            let _active_connection =
                                ActiveQueryTransportConnection::new(Arc::clone(&runtime.metrics));
                            if let Err(err) = serve_query_transport_stream(
                                stream,
                                client,
                                runtime,
                                connection_stop_rx,
                                control_only,
                            )
                            .await
                            {
                                tracing::warn!(
                                    target: "slatedb_graph_kernel",
                                    error = %err,
                                    "query transport connection failed"
                                );
                            }
                        });
                    }
                    joined = connections.join_next(), if !connections.is_empty() => {
                        if let Some(Err(err)) = joined {
                            tracing::warn!(
                                target: "slatedb_graph_kernel",
                                error = %err,
                                "query transport connection task failed"
                            );
                        }
                    }
                }
            }
            let graceful_drain = async {
                while let Some(joined) = connections.join_next().await {
                    if let Err(err) = joined {
                        if !err.is_cancelled() {
                            tracing::warn!(
                                target: "slatedb_graph_kernel",
                                error = %err,
                                "query transport connection task failed during shutdown"
                            );
                        }
                    }
                }
            };
            if tokio::time::timeout(graceful_shutdown_timeout, graceful_drain)
                .await
                .is_err()
            {
                runtime
                    .metrics
                    .forced_shutdowns
                    .fetch_add(1, Ordering::Relaxed);
                connections.abort_all();
                while let Some(joined) = connections.join_next().await {
                    if let Err(err) = joined {
                        if !err.is_cancelled() {
                            tracing::warn!(
                                target: "slatedb_graph_kernel",
                                error = %err,
                                "query transport connection task failed during forced shutdown"
                            );
                        }
                    }
                }
            }
            Ok(())
        });
        Ok(Self {
            local_addr,
            stop_tx,
            task,
            metrics,
            lifecycle,
        })
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    pub fn metrics(&self) -> QueryTransportMetricsSnapshot {
        self.metrics.snapshot()
    }

    pub async fn cancel_query(&self, query_id: impl Into<String>) -> Result<()> {
        self.cancel_query_in_scope(GraphScope::default(), query_id)
            .await
    }

    pub async fn cancel_query_in_scope(
        &self,
        scope: GraphScope,
        query_id: impl Into<String>,
    ) -> Result<()> {
        self.cancel_query_for_principal_in_scope(
            QueryTransportCancellationPrincipal::insecure(),
            scope,
            query_id,
        )
        .await
    }

    pub async fn cancel_query_for_principal(
        &self,
        principal: QueryTransportCancellationPrincipal,
        query_id: impl Into<String>,
    ) -> Result<()> {
        self.cancel_query_for_principal_in_scope(principal, GraphScope::default(), query_id)
            .await
    }

    pub async fn cancel_query_for_principal_in_scope(
        &self,
        principal: QueryTransportCancellationPrincipal,
        scope: GraphScope,
        query_id: impl Into<String>,
    ) -> Result<()> {
        let query_id = query_id.into();
        validate_component("query_id", &query_id)?;
        let lifecycle_key = QueryLifecycleKey::new(principal.0, scope, &query_id);
        let mut lifecycle = self.lifecycle.lock().await;
        cancel_query_lifecycle_entry(&mut lifecycle, &self.metrics, &lifecycle_key)
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
            auth: self.auth(),
            context,
            query: query.to_string(),
        };
        match self.send(request).await? {
            QueryTransportResponse::Rows { result } => Ok(result),
            QueryTransportResponse::Page { .. } => Err(transport_protocol_error(
                "query/transport/rows",
                "server returned page response for rows request",
            )),
            QueryTransportResponse::Cancelled => Err(transport_protocol_error(
                "query/transport/rows",
                "server returned cancel response for rows request",
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
            auth: self.auth(),
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
            QueryTransportResponse::Cancelled => Err(transport_protocol_error(
                "query/transport/page",
                "server returned cancel response for page request",
            )),
            QueryTransportResponse::Error { message } => {
                Err(transport_remote_error("query/transport/page", message))
            }
        }
    }
}

#[cfg(feature = "query-transport")]
impl TcpQueryCellClient {
    pub fn stream_cypher_rows(
        &self,
        context: QueryContext,
        query: impl Into<String>,
        page_size: usize,
    ) -> TcpQueryRowStream {
        TcpQueryRowStream {
            client: self.clone(),
            context,
            query: query.into(),
            page_size,
            cursor: None,
            done: false,
            buffered_rows: Vec::new(),
            columns: None,
        }
    }

    pub async fn cancel_query(&self, query_id: impl Into<String>) -> Result<()> {
        self.cancel_query_in_scope(GraphScope::default(), query_id)
            .await
    }

    pub async fn cancel_query_in_scope(
        &self,
        scope: GraphScope,
        query_id: impl Into<String>,
    ) -> Result<()> {
        let query_id = query_id.into();
        validate_component("query_id", &query_id)?;
        let request = QueryTransportRequest::Cancel {
            version: QUERY_TRANSPORT_VERSION,
            auth: self.auth(),
            scope,
            query_id,
        };
        match self.send_control(request).await? {
            QueryTransportResponse::Cancelled => Ok(()),
            QueryTransportResponse::Rows { .. } | QueryTransportResponse::Page { .. } => {
                Err(transport_protocol_error(
                    "query/transport/cancel",
                    "server returned query data for cancel request",
                ))
            }
            QueryTransportResponse::Error { message } => {
                Err(transport_remote_error("query/transport/cancel", message))
            }
        }
    }

    fn auth(&self) -> QueryTransportAuth {
        QueryTransportAuth {
            bearer_token: self
                .config
                .bearer_token
                .as_ref()
                .filter(|secret| secret.is_valid())
                .map(|secret| secret.expose_secret().to_string()),
        }
    }

    async fn send(&self, request: QueryTransportRequest) -> Result<QueryTransportResponse> {
        let started = std::time::Instant::now();
        let result = match tokio::time::timeout(self.config.timeout, self.send_once(&request)).await
        {
            Ok(result) => result,
            Err(_) => Err(query_transport_client_timeout(self.config.timeout)),
        };
        let elapsed_us = started.elapsed().as_micros().min(u128::from(u64::MAX)) as u64;
        self.metrics
            .remote_latency_us
            .fetch_add(elapsed_us, Ordering::Relaxed);
        result
    }

    async fn send_control(&self, request: QueryTransportRequest) -> Result<QueryTransportResponse> {
        let started = std::time::Instant::now();
        let result =
            match tokio::time::timeout(self.config.timeout, self.send_control_once(&request)).await
            {
                Ok(result) => result,
                Err(_) => Err(query_transport_client_timeout(self.config.timeout)),
            };
        let elapsed_us = started.elapsed().as_micros().min(u128::from(u64::MAX)) as u64;
        self.metrics
            .remote_latency_us
            .fetch_add(elapsed_us, Ordering::Relaxed);
        result
    }

    async fn send_once(&self, request: &QueryTransportRequest) -> Result<QueryTransportResponse> {
        self.config.validate()?;
        let _connection_permit = match Arc::clone(&self.connection_gate).try_acquire_owned() {
            Ok(permit) => permit,
            Err(_) => {
                self.metrics
                    .client_connection_waits
                    .fetch_add(1, Ordering::Relaxed);
                Arc::clone(&self.connection_gate)
                    .acquire_owned()
                    .await
                    .map_err(|err| GraphError::CorruptValue {
                        key: "query/transport/client_connections".to_string(),
                        reason: err.to_string(),
                    })?
            }
        };
        let mut connection = match self.take_idle_connection().await {
            Some(connection) => {
                self.metrics
                    .connections_reused
                    .fetch_add(1, Ordering::Relaxed);
                connection
            }
            None => self.open_connection_with_retries().await?,
        };
        let decoded = self.send_on_io(&mut *connection.io, request).await?;
        if requires_legacy_version_fallback(request, &decoded) {
            self.metrics.client_retries.fetch_add(1, Ordering::Relaxed);
            let mut legacy_request = request.clone();
            legacy_request.set_version(QUERY_TRANSPORT_LEGACY_VERSION);
            let mut legacy_connection = self.open_connection_with_retries().await?;
            let legacy = self
                .send_on_io(&mut *legacy_connection.io, &legacy_request)
                .await?;
            if !legacy.close_connection {
                self.recycle_connection(legacy_connection).await;
            }
            return Ok(legacy.response);
        }
        if !decoded.close_connection {
            self.recycle_connection(connection).await;
        }
        Ok(decoded.response)
    }

    async fn send_control_once(
        &self,
        request: &QueryTransportRequest,
    ) -> Result<QueryTransportResponse> {
        self.config.validate()?;
        let _control_permit = Arc::clone(&self.control_connection_gate)
            .acquire_owned()
            .await
            .map_err(|err| GraphError::CorruptValue {
                key: "query/transport/client_control_connections".to_string(),
                reason: err.to_string(),
            })?;
        let mut connection = self.open_connection_with_retries().await?;
        let decoded = self.send_on_io(&mut *connection.io, request).await?;
        if requires_legacy_version_fallback(request, &decoded) {
            self.metrics.client_retries.fetch_add(1, Ordering::Relaxed);
            let mut legacy_request = request.clone();
            legacy_request.set_version(QUERY_TRANSPORT_LEGACY_VERSION);
            let mut legacy_connection = self.open_connection_with_retries().await?;
            return Ok(self
                .send_on_io(&mut *legacy_connection.io, &legacy_request)
                .await?
                .response);
        }
        Ok(decoded.response)
    }

    async fn open_connection_with_retries(&self) -> Result<PooledQueryTransportConnection> {
        let attempts = self.config.max_retries.saturating_add(1);
        let mut last_err = None;
        for attempt in 0..attempts {
            if attempt > 0 {
                self.metrics.client_retries.fetch_add(1, Ordering::Relaxed);
                tokio::task::yield_now().await;
            }
            match self.open_connection().await {
                Ok(connection) => return Ok(connection),
                Err(err) => last_err = Some(err),
            }
        }
        Err(last_err.unwrap_or_else(|| {
            transport_protocol_error("query/transport/connect", "no connection attempts")
        }))
    }

    async fn open_connection(&self) -> Result<PooledQueryTransportConnection> {
        let stream = TcpStream::connect(self.addr)
            .await
            .map_err(|err| transport_error("connect", err))?;
        stream
            .set_nodelay(true)
            .map_err(|err| transport_error("set_nodelay", err))?;
        self.metrics
            .connections_created
            .fetch_add(1, Ordering::Relaxed);
        #[cfg(feature = "query-transport-tls")]
        {
            let tls_config = if let Some(provider) = &self.config.tls_config_provider {
                Some(provider.current_client_config()?)
            } else {
                self.config.tls_config.clone()
            };
            if let Some(tls_config) = tls_config {
                let server_name = self.config.tls_server_name.clone().ok_or_else(|| {
                    transport_protocol_error("query/transport/tls", "missing TLS server name")
                })?;
                let server_name =
                    ServerName::try_from(server_name).map_err(|err| GraphError::CorruptValue {
                        key: "query/transport/tls/server_name".to_string(),
                        reason: err.to_string(),
                    })?;
                let stream = TlsConnector::from(tls_config)
                    .connect(server_name, stream)
                    .await
                    .map_err(|err| transport_error("tls_connect", err))?;
                return Ok(PooledQueryTransportConnection {
                    io: Box::new(stream),
                    created_at: Instant::now(),
                    tls_generation: self.current_tls_generation(),
                });
            }
        }

        Ok(PooledQueryTransportConnection {
            io: Box::new(stream),
            created_at: Instant::now(),
            tls_generation: self.current_tls_generation(),
        })
    }

    async fn take_idle_connection(&self) -> Option<PooledQueryTransportConnection> {
        let mut pool = self.pool.lock().await;
        while let Some(connection) = pool.pop() {
            if connection.created_at.elapsed() < self.config.max_connection_age
                && connection.tls_generation == self.current_tls_generation()
            {
                return Some(connection);
            }
        }
        None
    }

    async fn recycle_connection(&self, connection: PooledQueryTransportConnection) {
        if self.config.max_idle_connections == 0
            || connection.created_at.elapsed() >= self.config.max_connection_age
            || connection.tls_generation != self.current_tls_generation()
        {
            return;
        }
        let mut pool = self.pool.lock().await;
        if pool.len() < self.config.max_idle_connections {
            pool.push(connection);
        }
    }

    fn current_tls_generation(&self) -> u64 {
        #[cfg(feature = "query-transport-tls")]
        if let Some(provider) = &self.config.tls_config_provider {
            return provider.generation();
        }
        0
    }

    async fn send_on_io<S>(
        &self,
        stream: &mut S,
        request: &QueryTransportRequest,
    ) -> Result<DecodedQueryTransportResponse>
    where
        S: AsyncRead + AsyncWrite + Unpin + ?Sized,
    {
        write_query_transport_frame(stream, request, self.config.max_frame_bytes, &self.metrics)
            .await?;
        let frame =
            read_query_transport_frame(stream, self.config.max_frame_bytes, &self.metrics).await?;
        let frame: QueryTransportResponseWire =
            serde_json::from_slice(&frame).map_err(|err| transport_json_error("decode", err))?;
        Ok(frame.decode())
    }
}

#[cfg(feature = "query-transport")]
fn requires_legacy_version_fallback(
    request: &QueryTransportRequest,
    decoded: &DecodedQueryTransportResponse,
) -> bool {
    if !decoded.legacy || request.version() != QUERY_TRANSPORT_VERSION {
        return false;
    }
    matches!(
        &decoded.response,
        QueryTransportResponse::Error { message }
            if message
                == &format!(
                    "unsupported query transport version {QUERY_TRANSPORT_VERSION}; expected {QUERY_TRANSPORT_LEGACY_VERSION}"
                )
    )
}

#[cfg(feature = "query-transport")]
fn query_transport_client_timeout(timeout: Duration) -> GraphError {
    let timeout_ms = timeout.as_millis().min(u128::from(u64::MAX)) as u64;
    GraphError::QueryTimeout {
        operation: "query_transport",
        elapsed_ms: timeout_ms,
        limit_ms: timeout_ms,
    }
}

#[cfg(feature = "query-transport")]
#[derive(Clone)]
pub struct TcpQueryRowStream {
    client: TcpQueryCellClient,
    context: QueryContext,
    query: String,
    page_size: usize,
    cursor: Option<QueryCursorToken>,
    done: bool,
    buffered_rows: Vec<QueryRow>,
    columns: Option<Vec<crate::QueryColumn>>,
}

#[cfg(feature = "query-transport")]
impl TcpQueryRowStream {
    pub async fn next_page(&mut self) -> Result<Option<QueryResultPage>> {
        if self.done {
            return Ok(None);
        }
        let page = self
            .client
            .execute_cypher_rows_page(
                self.context.clone(),
                &self.query,
                self.cursor,
                self.page_size,
            )
            .await?;
        self.cursor = page.next_cursor;
        self.done = self.cursor.is_none();
        Ok(Some(page))
    }

    pub async fn next_row(&mut self) -> Result<Option<QueryRow>> {
        loop {
            if let Some(row) = self.buffered_rows.pop() {
                return Ok(Some(row));
            }
            let Some(page) = self.next_page().await? else {
                return Ok(None);
            };
            if self.columns.is_none() {
                self.columns = Some(page.columns.clone());
            }
            self.buffered_rows = page.rows;
            self.buffered_rows.reverse();
        }
    }

    pub fn columns(&self) -> Option<&[crate::QueryColumn]> {
        self.columns.as_deref()
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

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DistributedQueryLeg {
    pub name: String,
    pub context: QueryContext,
    pub query: String,
    pub estimated_rows: Option<u64>,
}

impl DistributedQueryLeg {
    pub fn new(
        name: impl Into<String>,
        context: QueryContext,
        query: impl Into<String>,
    ) -> Result<Self> {
        let name = name.into();
        validate_component("query_leg", &name)?;
        Ok(Self {
            name,
            context,
            query: query.into(),
            estimated_rows: None,
        })
    }

    pub fn with_estimated_rows(mut self, estimated_rows: u64) -> Self {
        self.estimated_rows = Some(estimated_rows);
        self
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DistributedQueryJoin {
    pub left_leg: String,
    pub right_leg: String,
    pub left_column: String,
    pub right_column: String,
}

impl DistributedQueryJoin {
    pub fn inner(
        left_leg: impl Into<String>,
        left_column: impl Into<String>,
        right_leg: impl Into<String>,
        right_column: impl Into<String>,
    ) -> Self {
        Self {
            left_leg: left_leg.into(),
            right_leg: right_leg.into(),
            left_column: left_column.into(),
            right_column: right_column.into(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DistributedQueryMerge {
    UnionAll,
    InnerJoin(DistributedQueryJoin),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DistributedQueryPlan {
    pub legs: Vec<DistributedQueryLeg>,
    pub merge: DistributedQueryMerge,
}

impl DistributedQueryPlan {
    pub fn union_all(legs: Vec<DistributedQueryLeg>) -> Self {
        Self {
            legs,
            merge: DistributedQueryMerge::UnionAll,
        }
    }

    pub fn inner_join(legs: Vec<DistributedQueryLeg>, join: DistributedQueryJoin) -> Self {
        Self {
            legs,
            merge: DistributedQueryMerge::InnerJoin(join),
        }
    }

    pub fn optimized_for_costs(mut self) -> Self {
        if !matches!(self.merge, DistributedQueryMerge::InnerJoin(_)) {
            return self;
        }
        self.legs.sort_by_key(|leg| {
            (
                leg.estimated_rows.unwrap_or(u64::MAX),
                leg.context.cell_id.clone(),
                leg.name.clone(),
            )
        });
        self
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DistributedQueryPlanResult {
    pub leg_results: BTreeMap<String, QueryResultSet>,
    pub merged: QueryResultSet,
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

    #[cfg(feature = "query-transport")]
    pub fn from_service_directory(
        placement: ShardPlacement,
        directory: &QueryServiceDirectory,
    ) -> Result<Self> {
        let mut coordinator = Self::new(placement.clone());
        for node_id in placement.node_ids() {
            let endpoint = directory
                .endpoint(node_id)
                .ok_or_else(|| GraphError::CorruptValue {
                    key: format!("query/service/{node_id}"),
                    reason: format!("missing service-discovery endpoint for node {node_id}"),
                })?;
            coordinator.register_client(
                node_id.to_string(),
                Arc::new(TcpQueryCellClient::with_config(
                    endpoint.addr,
                    endpoint.client_config.clone(),
                )),
            )?;
        }
        Ok(coordinator)
    }

    #[cfg(feature = "query-transport")]
    pub async fn from_service_discovery(
        placement: ShardPlacement,
        discovery: &dyn QueryServiceDiscovery,
    ) -> Result<Self> {
        let directory = discovery.resolve_query_services().await?;
        Self::from_service_directory(placement, &directory)
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

    pub async fn execute_distributed_query_plan(
        &self,
        plan: DistributedQueryPlan,
    ) -> Result<DistributedQueryPlanResult> {
        let plan = if matches!(&plan.merge, DistributedQueryMerge::InnerJoin(_)) {
            plan.optimized_for_costs()
        } else {
            plan
        };
        if plan.legs.is_empty() {
            return Err(GraphError::UnsupportedQuery {
                dialect: "DistributedQuery",
                feature: "distributed query plan requires at least one leg".to_string(),
            });
        }
        let leg_order = plan
            .legs
            .iter()
            .map(|leg| leg.name.clone())
            .collect::<Vec<_>>();
        let mut seen_leg_names = BTreeSet::new();
        let mut jobs = Vec::new();
        for leg in plan.legs {
            if !seen_leg_names.insert(leg.name.clone()) {
                return Err(GraphError::CorruptValue {
                    key: format!("query/leg/{}", leg.name),
                    reason: "duplicate distributed query leg".to_string(),
                });
            }
            let cell_id = leg.context.cell_id.clone();
            let client = self.client_for_cell(&cell_id)?;
            jobs.push(async move {
                let result = client
                    .execute_cypher_rows(leg.context, leg.query.as_str())
                    .await;
                (leg.name, result)
            });
        }

        let mut leg_results = BTreeMap::new();
        for (leg_name, result) in join_all(jobs).await {
            leg_results.insert(leg_name, result?);
        }
        let merged = match plan.merge {
            DistributedQueryMerge::UnionAll => {
                merge_distributed_union_all(&leg_results, &leg_order)?
            }
            DistributedQueryMerge::InnerJoin(join) => {
                merge_distributed_inner_join(&leg_results, &join)?
            }
        };
        Ok(DistributedQueryPlanResult {
            leg_results,
            merged,
        })
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

fn merge_distributed_union_all(
    leg_results: &BTreeMap<String, QueryResultSet>,
    leg_order: &[String],
) -> Result<QueryResultSet> {
    let Some(first_leg) = leg_order.first() else {
        return Err(GraphError::UnsupportedQuery {
            dialect: "DistributedQuery",
            feature: "cannot merge an empty distributed result".to_string(),
        });
    };
    let first = leg_results
        .get(first_leg)
        .ok_or_else(|| missing_distributed_leg(first_leg))?;
    let columns = first.columns.clone();
    let mut rows = first.rows.clone();
    for leg_name in leg_order.iter().skip(1) {
        let result = leg_results
            .get(leg_name)
            .ok_or_else(|| missing_distributed_leg(leg_name))?;
        if result.columns != columns {
            return Err(GraphError::UnsupportedQuery {
                dialect: "DistributedQuery",
                feature: format!("UNION ALL leg {leg_name} returned different columns"),
            });
        }
        rows.extend(result.rows.clone());
    }
    Ok(QueryResultSet::new(columns, rows))
}

fn merge_distributed_inner_join(
    leg_results: &BTreeMap<String, QueryResultSet>,
    join: &DistributedQueryJoin,
) -> Result<QueryResultSet> {
    let left = leg_results
        .get(&join.left_leg)
        .ok_or_else(|| missing_distributed_leg(&join.left_leg))?;
    let right = leg_results
        .get(&join.right_leg)
        .ok_or_else(|| missing_distributed_leg(&join.right_leg))?;
    let left_idx = column_index(left, &join.left_column)?;
    let right_idx = column_index(right, &join.right_column)?;

    let mut columns = left
        .columns
        .iter()
        .map(|column| crate::QueryColumn::new(format!("{}.{}", join.left_leg, column.name)))
        .collect::<Vec<_>>();
    columns.extend(
        right
            .columns
            .iter()
            .map(|column| crate::QueryColumn::new(format!("{}.{}", join.right_leg, column.name))),
    );

    let mut rows = Vec::new();
    if right.rows.len() <= left.rows.len() {
        let mut right_by_key = BTreeMap::<QueryValue, Vec<&QueryRow>>::new();
        for row in &right.rows {
            let Some(key) = row.values.get(right_idx).cloned() else {
                return Err(GraphError::CorruptValue {
                    key: format!("query/leg/{}", join.right_leg),
                    reason: "right row is missing join column".to_string(),
                });
            };
            right_by_key.entry(key).or_default().push(row);
        }

        for left_row in &left.rows {
            let Some(key) = left_row.values.get(left_idx) else {
                return Err(GraphError::CorruptValue {
                    key: format!("query/leg/{}", join.left_leg),
                    reason: "left row is missing join column".to_string(),
                });
            };
            if let Some(right_rows) = right_by_key.get(key) {
                for right_row in right_rows {
                    let mut values = left_row.values.clone();
                    values.extend(right_row.values.clone());
                    rows.push(QueryRow::new(values));
                }
            }
        }
    } else {
        let mut left_by_key = BTreeMap::<QueryValue, Vec<&QueryRow>>::new();
        for row in &left.rows {
            let Some(key) = row.values.get(left_idx).cloned() else {
                return Err(GraphError::CorruptValue {
                    key: format!("query/leg/{}", join.left_leg),
                    reason: "left row is missing join column".to_string(),
                });
            };
            left_by_key.entry(key).or_default().push(row);
        }

        for right_row in &right.rows {
            let Some(key) = right_row.values.get(right_idx) else {
                return Err(GraphError::CorruptValue {
                    key: format!("query/leg/{}", join.right_leg),
                    reason: "right row is missing join column".to_string(),
                });
            };
            if let Some(left_rows) = left_by_key.get(key) {
                for left_row in left_rows {
                    let mut values = left_row.values.clone();
                    values.extend(right_row.values.clone());
                    rows.push(QueryRow::new(values));
                }
            }
        }
    }
    Ok(QueryResultSet::new(columns, rows))
}

fn missing_distributed_leg(leg_name: &str) -> GraphError {
    GraphError::CorruptValue {
        key: format!("query/leg/{leg_name}"),
        reason: "distributed query join references a missing leg".to_string(),
    }
}

fn column_index(result: &QueryResultSet, column: &str) -> Result<usize> {
    result
        .columns
        .iter()
        .position(|candidate| candidate.name == column)
        .ok_or_else(|| GraphError::UnsupportedQuery {
            dialect: "DistributedQuery",
            feature: format!("join column {column} is not present in result set"),
        })
}

#[async_trait]
impl QueryCellClient for RoutedGraphCluster {
    async fn execute_cypher_rows(
        &self,
        context: QueryContext,
        query: &str,
    ) -> Result<QueryResultSet> {
        RoutedGraphCluster::execute_cypher_rows(self, context, query).await
    }

    async fn execute_cypher_rows_page(
        &self,
        context: QueryContext,
        query: &str,
        cursor: Option<QueryCursorToken>,
        page_size: usize,
    ) -> Result<QueryResultPage> {
        RoutedGraphCluster::execute_cypher_rows_page(self, context, query, cursor, page_size).await
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

#[cfg(feature = "query-service-discovery")]
fn parse_kubernetes_endpointslices(
    value: &serde_json::Value,
    port_name: Option<&str>,
    client_config: QueryTransportClientConfig,
) -> Result<QueryServiceDirectory> {
    let mut directory = QueryServiceDirectory::new();
    let items = value
        .get("items")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| service_discovery_format_error("kubernetes", "items array is missing"))?;
    for item in items {
        let port = select_kubernetes_port(item, port_name)?;
        let Some(endpoints) = item.get("endpoints").and_then(serde_json::Value::as_array) else {
            continue;
        };
        for endpoint in endpoints {
            if endpoint
                .pointer("/conditions/ready")
                .and_then(serde_json::Value::as_bool)
                == Some(false)
            {
                continue;
            }
            let Some(address) = endpoint
                .get("addresses")
                .and_then(serde_json::Value::as_array)
                .and_then(|addresses| addresses.first())
                .and_then(serde_json::Value::as_str)
            else {
                continue;
            };
            let node_id = endpoint
                .get("hostname")
                .and_then(serde_json::Value::as_str)
                .or_else(|| {
                    endpoint
                        .pointer("/targetRef/name")
                        .and_then(serde_json::Value::as_str)
                })
                .unwrap_or(address);
            directory.insert(
                QueryServiceEndpoint::new(
                    discovery_node_id(node_id),
                    socket_addr_from_host_port("kubernetes", address, port)?,
                )
                .with_client_config(client_config.clone()),
            )?;
        }
    }
    Ok(directory)
}

#[cfg(feature = "query-service-discovery")]
fn select_kubernetes_port(item: &serde_json::Value, port_name: Option<&str>) -> Result<u16> {
    let ports = item
        .get("ports")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| service_discovery_format_error("kubernetes", "ports array is missing"))?;
    let selected = match port_name {
        Some(port_name) => ports
            .iter()
            .find(|port| port.get("name").and_then(serde_json::Value::as_str) == Some(port_name)),
        None => ports.first(),
    }
    .ok_or_else(|| service_discovery_format_error("kubernetes", "matching port is missing"))?;
    u16_from_json_field("kubernetes", selected, "port")
}

#[cfg(feature = "query-service-discovery")]
fn parse_consul_health_service(
    value: &serde_json::Value,
    client_config: QueryTransportClientConfig,
) -> Result<QueryServiceDirectory> {
    let mut directory = QueryServiceDirectory::new();
    let services = value.as_array().ok_or_else(|| {
        service_discovery_format_error("consul", "health service array is missing")
    })?;
    for item in services {
        let service = item
            .get("Service")
            .ok_or_else(|| service_discovery_format_error("consul", "Service object is missing"))?;
        let node = item.get("Node");
        let node_id = service
            .get("ID")
            .and_then(serde_json::Value::as_str)
            .or_else(|| service.get("Service").and_then(serde_json::Value::as_str))
            .ok_or_else(|| service_discovery_format_error("consul", "Service.ID is missing"))?;
        let address = service
            .get("Address")
            .and_then(serde_json::Value::as_str)
            .filter(|address| !address.is_empty())
            .or_else(|| {
                node.and_then(|node| node.get("Address"))
                    .and_then(serde_json::Value::as_str)
            })
            .ok_or_else(|| {
                service_discovery_format_error("consul", "service address is missing")
            })?;
        let port = u16_from_json_field("consul", service, "Port")?;
        directory.insert(
            QueryServiceEndpoint::new(
                discovery_node_id(node_id),
                socket_addr_from_host_port("consul", address, port)?,
            )
            .with_client_config(client_config.clone()),
        )?;
    }
    Ok(directory)
}

#[cfg(feature = "query-service-discovery")]
#[derive(serde::Deserialize)]
struct EtcdQueryServiceRecord {
    node_id: String,
    address: String,
    port: u16,
}

#[cfg(feature = "query-service-discovery")]
fn parse_etcd_query_services(
    value: &serde_json::Value,
    client_config: QueryTransportClientConfig,
) -> Result<QueryServiceDirectory> {
    let mut directory = QueryServiceDirectory::new();
    let Some(kvs) = value.get("kvs").and_then(serde_json::Value::as_array) else {
        return Ok(directory);
    };
    for kv in kvs {
        let encoded = kv
            .get("value")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| service_discovery_format_error("etcd", "kv value is missing"))?;
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(encoded)
            .map_err(|err| service_discovery_error("etcd/base64", err))?;
        let record: EtcdQueryServiceRecord = serde_json::from_slice(&bytes)
            .map_err(|err| service_discovery_error("etcd/record", err))?;
        directory.insert(
            QueryServiceEndpoint::new(
                discovery_node_id(&record.node_id),
                socket_addr_from_host_port("etcd", &record.address, record.port)?,
            )
            .with_client_config(client_config.clone()),
        )?;
    }
    Ok(directory)
}

#[cfg(feature = "query-service-discovery")]
fn etcd_prefix_end(prefix: &str) -> Vec<u8> {
    let mut bytes = prefix.as_bytes().to_vec();
    for idx in (0..bytes.len()).rev() {
        if bytes[idx] != 0xff {
            bytes[idx] = bytes[idx].saturating_add(1);
            bytes.truncate(idx + 1);
            return bytes;
        }
    }
    vec![0]
}

#[cfg(feature = "query-service-discovery")]
fn socket_addr_from_host_port(provider: &'static str, host: &str, port: u16) -> Result<SocketAddr> {
    let raw = if host.contains(':') && !host.starts_with('[') {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
    };
    raw.parse().map_err(|err| GraphError::CorruptValue {
        key: format!("query/service_discovery/{provider}/address"),
        reason: format!("invalid endpoint address {raw}: {err}"),
    })
}

#[cfg(feature = "query-service-discovery")]
fn u16_from_json_field(
    provider: &'static str,
    value: &serde_json::Value,
    field: &str,
) -> Result<u16> {
    let raw = value
        .get(field)
        .and_then(serde_json::Value::as_u64)
        .ok_or_else(|| service_discovery_format_error(provider, &format!("{field} is missing")))?;
    u16::try_from(raw)
        .map_err(|_| service_discovery_format_error(provider, &format!("{field} is out of range")))
}

#[cfg(feature = "query-service-discovery")]
fn discovery_node_id(raw: &str) -> String {
    raw.bytes()
        .map(|byte| {
            if byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.') {
                byte as char
            } else {
                '_'
            }
        })
        .collect()
}

#[cfg(feature = "query-service-discovery")]
fn service_discovery_format_error(provider: &'static str, reason: &str) -> GraphError {
    GraphError::CorruptValue {
        key: format!("query/service_discovery/{provider}"),
        reason: reason.to_string(),
    }
}

#[cfg(feature = "query-service-discovery")]
fn service_discovery_error(provider: &'static str, err: impl std::fmt::Display) -> GraphError {
    GraphError::CorruptValue {
        key: format!("query/service_discovery/{provider}"),
        reason: err.to_string(),
    }
}

#[cfg(feature = "query-transport")]
#[derive(Clone, serde::Deserialize, serde::Serialize)]
struct QueryTransportAuth {
    bearer_token: Option<String>,
}

#[cfg(feature = "query-transport")]
#[derive(Clone, serde::Deserialize, serde::Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum QueryTransportRequest {
    Rows {
        version: u16,
        auth: QueryTransportAuth,
        context: QueryContext,
        query: String,
    },
    Page {
        version: u16,
        auth: QueryTransportAuth,
        context: QueryContext,
        query: String,
        cursor: Option<QueryCursorToken>,
        page_size: usize,
    },
    Cancel {
        version: u16,
        auth: QueryTransportAuth,
        #[serde(default)]
        scope: GraphScope,
        query_id: String,
    },
}

#[cfg(feature = "query-transport")]
impl QueryTransportRequest {
    fn version(&self) -> u16 {
        match self {
            Self::Rows { version, .. }
            | Self::Page { version, .. }
            | Self::Cancel { version, .. } => *version,
        }
    }

    fn set_version(&mut self, new_version: u16) {
        match self {
            Self::Rows { version, .. }
            | Self::Page { version, .. }
            | Self::Cancel { version, .. } => *version = new_version,
        }
    }

    fn is_cancel(&self) -> bool {
        matches!(self, Self::Cancel { .. })
    }
}

#[cfg(feature = "query-transport")]
#[derive(serde::Deserialize, serde::Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum QueryTransportResponse {
    Rows { result: QueryResultSet },
    Page { result: QueryResultPage },
    Cancelled,
    Error { message: String },
}

#[cfg(feature = "query-transport")]
#[derive(serde::Deserialize, serde::Serialize)]
struct QueryTransportResponseFrame {
    response: QueryTransportResponse,
    close_connection: bool,
}

#[cfg(feature = "query-transport")]
#[derive(serde::Deserialize, serde::Serialize)]
#[serde(untagged)]
enum QueryTransportResponseWire {
    Current(QueryTransportResponseFrame),
    Legacy(QueryTransportResponse),
}

#[cfg(feature = "query-transport")]
struct DecodedQueryTransportResponse {
    response: QueryTransportResponse,
    close_connection: bool,
    legacy: bool,
}

#[cfg(feature = "query-transport")]
impl QueryTransportResponseWire {
    fn decode(self) -> DecodedQueryTransportResponse {
        match self {
            Self::Current(frame) => DecodedQueryTransportResponse {
                response: frame.response,
                close_connection: frame.close_connection,
                legacy: false,
            },
            Self::Legacy(response) => DecodedQueryTransportResponse {
                response,
                close_connection: true,
                legacy: true,
            },
        }
    }
}

#[cfg(feature = "query-transport")]
async fn serve_query_transport_stream(
    stream: TcpStream,
    client: Arc<dyn QueryCellClient>,
    runtime: Arc<QueryTransportServerRuntime>,
    shutdown: watch::Receiver<bool>,
    control_only: bool,
) -> Result<()> {
    #[cfg(feature = "query-transport-tls")]
    {
        let tls_config = if let Some(provider) = &runtime.config.tls_config_provider {
            Some(provider.current_server_config()?)
        } else {
            runtime.config.tls_config.clone()
        };
        if let Some(tls_config) = tls_config {
            let acceptor = TlsAcceptor::from(tls_config);
            let handshake =
                tokio::time::timeout(runtime.config.handshake_timeout, acceptor.accept(stream));
            let mut handshake_shutdown = shutdown.clone();
            let handshake = tokio::select! {
                changed = handshake_shutdown.changed() => {
                    let _ = changed;
                    return Ok(());
                }
                handshake = handshake => handshake,
            };
            let mut stream = match handshake {
                Ok(Ok(stream)) => stream,
                Ok(Err(err)) => {
                    runtime
                        .metrics
                        .handshake_failures
                        .fetch_add(1, Ordering::Relaxed);
                    return Err(transport_error("tls_accept", err));
                }
                Err(_) => {
                    runtime
                        .metrics
                        .handshake_failures
                        .fetch_add(1, Ordering::Relaxed);
                    return Err(transport_timeout_error(
                        "tls_handshake",
                        runtime.config.handshake_timeout,
                    ));
                }
            };
            let identity = query_transport_tls_identity(&stream);
            return serve_query_transport_io(
                &mut stream,
                client,
                runtime,
                identity,
                shutdown,
                control_only,
            )
            .await;
        }
    }

    let mut stream = stream;
    serve_query_transport_io(
        &mut stream,
        client,
        runtime,
        QueryTransportConnectionIdentity::default(),
        shutdown,
        control_only,
    )
    .await
}

#[cfg(feature = "query-transport-tls")]
fn query_transport_tls_identity<S>(
    stream: &tokio_rustls::server::TlsStream<S>,
) -> QueryTransportConnectionIdentity {
    let mut identity = QueryTransportConnectionIdentity::default();
    let (_, connection) = stream.get_ref();
    if let Some(certs) = connection.peer_certificates() {
        for (index, cert) in certs.iter().enumerate() {
            let digest = Sha256::digest(cert.as_ref());
            let fingerprint = format!("sha256:{}", lowercase_hex(&digest));
            if index == 0 {
                identity.tls_peer_leaf_fingerprint = Some(fingerprint.clone());
            }
            identity.tls_peer_fingerprints.insert(fingerprint);
        }
    }
    identity
}

#[cfg(feature = "query-transport")]
fn lowercase_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

#[cfg(feature = "query-transport")]
async fn serve_query_transport_io<S>(
    stream: &mut S,
    client: Arc<dyn QueryCellClient>,
    runtime: Arc<QueryTransportServerRuntime>,
    identity: QueryTransportConnectionIdentity,
    mut shutdown: watch::Receiver<bool>,
    control_only: bool,
) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mut read_buffer = Vec::new();
    let connection_started = Instant::now();
    for request_index in 0..runtime.config.max_requests_per_connection {
        if *shutdown.borrow() {
            return Ok(());
        }
        let read = read_optional_query_transport_frame(
            stream,
            runtime.config.max_frame_bytes,
            &runtime.metrics,
            &mut read_buffer,
        );
        let frame = tokio::select! {
            changed = shutdown.changed() => {
                let _ = changed;
                return Ok(());
            }
            frame = tokio::time::timeout(runtime.config.idle_timeout, read) => {
                match frame {
                    Ok(result) => result?,
                    Err(_) => {
                        runtime.metrics.idle_timeouts.fetch_add(1, Ordering::Relaxed);
                        return Ok(());
                    }
                }
            }
        };
        let Some(frame) = frame else {
            return Ok(());
        };
        let (response, request_version) =
            match serde_json::from_slice::<QueryTransportRequest>(&frame) {
                Ok(request) => {
                    let request_version = request.version();
                    let response = if control_only && !request.is_cancel() {
                        QueryTransportResponse::Error {
                            message: "connection is reserved for query cancellation".to_string(),
                        }
                    } else {
                        execute_query_transport_request(
                            Arc::clone(&client),
                            request,
                            Arc::clone(&runtime),
                            identity.clone(),
                        )
                        .await
                    };
                    (response, request_version)
                }
                Err(err) => (
                    QueryTransportResponse::Error {
                        message: format!("invalid query transport request: {err}"),
                    },
                    QUERY_TRANSPORT_VERSION,
                ),
            };
        let legacy_response = request_version == QUERY_TRANSPORT_LEGACY_VERSION;
        let close_connection = legacy_response
            || control_only
            || request_index + 1 >= runtime.config.max_requests_per_connection
            || connection_started.elapsed() >= runtime.config.max_connection_age;
        let wire_response = if legacy_response {
            QueryTransportResponseWire::Legacy(response)
        } else {
            QueryTransportResponseWire::Current(QueryTransportResponseFrame {
                response,
                close_connection,
            })
        };
        match tokio::time::timeout(
            runtime.config.write_timeout,
            write_query_transport_frame(
                stream,
                &wire_response,
                runtime.config.max_frame_bytes,
                &runtime.metrics,
            ),
        )
        .await
        {
            Ok(result) => result?,
            Err(_) => {
                return Err(transport_timeout_error(
                    "frame_write",
                    runtime.config.write_timeout,
                ));
            }
        }
        if close_connection {
            return Ok(());
        }
    }
    Ok(())
}

#[cfg(feature = "query-transport")]
async fn execute_query_transport_request(
    client: Arc<dyn QueryCellClient>,
    request: QueryTransportRequest,
    runtime: Arc<QueryTransportServerRuntime>,
    identity: QueryTransportConnectionIdentity,
) -> QueryTransportResponse {
    match request {
        QueryTransportRequest::Rows {
            version,
            auth,
            context,
            query,
        } => {
            if !query_transport_version_supported(version) {
                return transport_version_error(version);
            }
            let principal = match authenticate_query_transport(&runtime, &auth, &identity) {
                Ok(principal) => principal,
                Err(err) => return transport_error_response(&runtime, err),
            };
            if let Err(err) = authorize_query_transport_scope(
                &runtime,
                &principal,
                &context.scope,
                QueryTransportAction::Read,
            ) {
                return transport_error_response(&runtime, err);
            }
            let query_id = context.idempotency_key.clone();
            let lifecycle_key = QueryLifecycleKey::new(principal, context.scope.clone(), &query_id);
            match execute_metered_query(&runtime, &lifecycle_key, |cancellation_token| {
                client.execute_cypher_rows(
                    context.with_cancellation_token(cancellation_token),
                    &query,
                )
            })
            .await
            {
                Ok(result) => QueryTransportResponse::Rows { result },
                Err(err) => transport_error_response(&runtime, err),
            }
        }
        QueryTransportRequest::Page {
            version,
            auth,
            context,
            query,
            cursor,
            page_size,
        } => {
            if !query_transport_version_supported(version) {
                return transport_version_error(version);
            }
            let principal = match authenticate_query_transport(&runtime, &auth, &identity) {
                Ok(principal) => principal,
                Err(err) => return transport_error_response(&runtime, err),
            };
            if let Err(err) = authorize_query_transport_scope(
                &runtime,
                &principal,
                &context.scope,
                QueryTransportAction::Read,
            ) {
                return transport_error_response(&runtime, err);
            }
            let query_id = context.idempotency_key.clone();
            let lifecycle_key = QueryLifecycleKey::new(principal, context.scope.clone(), &query_id);
            match execute_metered_query(&runtime, &lifecycle_key, |cancellation_token| {
                client.execute_cypher_rows_page(
                    context.with_cancellation_token(cancellation_token),
                    &query,
                    cursor,
                    page_size,
                )
            })
            .await
            {
                Ok(result) => QueryTransportResponse::Page { result },
                Err(err) => transport_error_response(&runtime, err),
            }
        }
        QueryTransportRequest::Cancel {
            version,
            auth,
            scope,
            query_id,
        } => {
            if !query_transport_version_supported(version) {
                return transport_version_error(version);
            }
            let principal = match authenticate_query_transport(&runtime, &auth, &identity) {
                Ok(principal) => principal,
                Err(err) => return transport_error_response(&runtime, err),
            };
            let lifecycle_key = QueryLifecycleKey::new(principal, scope, &query_id);
            match cancel_active_query(&runtime, &lifecycle_key).await {
                Ok(()) => QueryTransportResponse::Cancelled,
                Err(err) => transport_error_response(&runtime, err),
            }
        }
    }
}

#[cfg(feature = "query-transport")]
fn transport_version_error(version: u16) -> QueryTransportResponse {
    QueryTransportResponse::Error {
        message: format!(
            "unsupported query transport version {version}; supported versions are {QUERY_TRANSPORT_LEGACY_VERSION} and {QUERY_TRANSPORT_VERSION}"
        ),
    }
}

#[cfg(feature = "query-transport")]
fn query_transport_version_supported(version: u16) -> bool {
    matches!(
        version,
        QUERY_TRANSPORT_LEGACY_VERSION | QUERY_TRANSPORT_VERSION
    )
}

#[cfg(feature = "query-transport")]
async fn execute_metered_query<F, Fut, T>(
    runtime: &Arc<QueryTransportServerRuntime>,
    lifecycle_key: &QueryLifecycleKey,
    execute: F,
) -> Result<T>
where
    F: FnOnce(QueryCancellationToken) -> Fut,
    Fut: Future<Output = Result<T>>,
{
    let (lifecycle_token, cancellation_token) =
        begin_query_lifecycle(runtime, lifecycle_key).await?;
    let result = async {
        let _namespace_permits = acquire_namespace_query_permits(
            runtime,
            &lifecycle_key.scope.namespace,
            &cancellation_token,
        )
        .await?;
        let _permit = acquire_query_transport_permit(runtime, &cancellation_token).await?;
        activate_query_lifecycle(runtime, lifecycle_key, lifecycle_token).await?;
        ensure_query_not_cancelled(runtime, lifecycle_key, lifecycle_token).await?;
        runtime
            .metrics
            .requests_started
            .fetch_add(1, Ordering::Relaxed);
        let started = std::time::Instant::now();
        let result = match AssertUnwindSafe(execute(cancellation_token))
            .catch_unwind()
            .await
        {
            Ok(result) => result,
            Err(_) => Err(GraphError::CorruptValue {
                key: "query/transport/executor".to_string(),
                reason: "query executor panicked".to_string(),
            }),
        };
        let elapsed = started.elapsed();
        runtime.metrics.remote_latency_us.fetch_add(
            elapsed.as_micros().min(u128::from(u64::MAX)) as u64,
            Ordering::Relaxed,
        );
        let is_slow_query = match runtime.config.slow_query_log_threshold {
            Some(threshold) => elapsed >= threshold,
            None => false,
        };
        if is_slow_query {
            runtime.metrics.slow_queries.fetch_add(1, Ordering::Relaxed);
            tracing::warn!(
                target: "slatedb_graph_kernel",
                query_id = lifecycle_key.query_id,
                elapsed_ms = elapsed.as_millis().min(u128::from(u64::MAX)) as u64,
                "slow query transport request"
            );
        }
        result
    }
    .await;
    let result = match finish_query_lifecycle(runtime, lifecycle_key, lifecycle_token).await {
        QueryLifecycleFinish::Cancelled => Err(cancelled_query_error()),
        QueryLifecycleFinish::NotCancelled | QueryLifecycleFinish::NotOwned => result,
    };
    match &result {
        Ok(_) => runtime
            .metrics
            .requests_completed
            .fetch_add(1, Ordering::Relaxed),
        Err(_) => runtime
            .metrics
            .requests_failed
            .fetch_add(1, Ordering::Relaxed),
    };
    result
}

#[cfg(feature = "query-transport")]
async fn begin_query_lifecycle(
    runtime: &QueryTransportServerRuntime,
    lifecycle_key: &QueryLifecycleKey,
) -> Result<(QueryLifecycleToken, QueryCancellationToken)> {
    let mut lifecycle = runtime.lifecycle.lock().await;
    if lifecycle.queries.contains_key(lifecycle_key) {
        return Err(GraphError::UnsupportedQuery {
            dialect: "QueryTransport",
            feature: format!("query id {} is already active", lifecycle_key.query_id),
        });
    }
    let token = lifecycle.next_token;
    lifecycle.next_token = lifecycle.next_token.wrapping_add(1);
    let cancellation_token = QueryCancellationToken::new();
    lifecycle.queries.insert(
        lifecycle_key.clone(),
        QueryLifecycleEntry {
            token,
            state: QueryLifecycleState::Queued,
            cancellation_token: cancellation_token.clone(),
        },
    );
    Ok((token, cancellation_token))
}

#[cfg(feature = "query-transport")]
async fn activate_query_lifecycle(
    runtime: &QueryTransportServerRuntime,
    lifecycle_key: &QueryLifecycleKey,
    token: QueryLifecycleToken,
) -> Result<()> {
    let mut lifecycle = runtime.lifecycle.lock().await;
    let Some(entry) = lifecycle.queries.get_mut(lifecycle_key) else {
        return Err(cancelled_query_error());
    };
    if entry.token != token {
        return Err(cancelled_query_error());
    }
    if entry.state == QueryLifecycleState::Cancelled {
        return Err(cancelled_query_error());
    }
    entry.state = QueryLifecycleState::Running;
    Ok(())
}

#[cfg(feature = "query-transport")]
async fn finish_query_lifecycle(
    runtime: &QueryTransportServerRuntime,
    lifecycle_key: &QueryLifecycleKey,
    token: QueryLifecycleToken,
) -> QueryLifecycleFinish {
    let mut lifecycle = runtime.lifecycle.lock().await;
    let Some(entry) = lifecycle.queries.get(lifecycle_key) else {
        return QueryLifecycleFinish::NotOwned;
    };
    if entry.token != token {
        return QueryLifecycleFinish::NotOwned;
    }
    let state = entry.state;
    lifecycle.queries.remove(lifecycle_key);
    match state {
        QueryLifecycleState::Cancelled => {
            runtime
                .metrics
                .cancelled_rejections
                .fetch_add(1, Ordering::Relaxed);
            QueryLifecycleFinish::Cancelled
        }
        QueryLifecycleState::Queued | QueryLifecycleState::Running => {
            QueryLifecycleFinish::NotCancelled
        }
    }
}

#[cfg(feature = "query-transport")]
enum QueryLifecycleFinish {
    Cancelled,
    NotCancelled,
    NotOwned,
}

#[cfg(feature = "query-transport")]
async fn cancel_active_query(
    runtime: &QueryTransportServerRuntime,
    lifecycle_key: &QueryLifecycleKey,
) -> Result<()> {
    validate_component("query_id", &lifecycle_key.query_id)?;
    let mut lifecycle = runtime.lifecycle.lock().await;
    cancel_query_lifecycle_entry(&mut lifecycle, &runtime.metrics, lifecycle_key)
}

#[cfg(feature = "query-transport")]
fn cancel_query_lifecycle_entry(
    lifecycle: &mut QueryTransportLifecycle,
    metrics: &QueryTransportMetrics,
    lifecycle_key: &QueryLifecycleKey,
) -> Result<()> {
    let Some(entry) = lifecycle.queries.get(lifecycle_key) else {
        return Err(inactive_query_cancel_error(&lifecycle_key.query_id));
    };
    let queued = entry.state == QueryLifecycleState::Queued;
    if queued {
        if let Some(entry) = lifecycle.queries.remove(lifecycle_key) {
            entry.cancellation_token.cancel();
        }
        metrics.cancelled_rejections.fetch_add(1, Ordering::Relaxed);
    } else if let Some(entry) = lifecycle.queries.get_mut(lifecycle_key) {
        entry.cancellation_token.cancel();
        entry.state = QueryLifecycleState::Cancelled;
    }
    metrics.cancellations.fetch_add(1, Ordering::Relaxed);
    Ok(())
}

#[cfg(feature = "query-transport")]
async fn acquire_query_transport_permit(
    runtime: &Arc<QueryTransportServerRuntime>,
    cancellation_token: &QueryCancellationToken,
) -> Result<OwnedSemaphorePermit> {
    match Arc::clone(&runtime.request_gate).try_acquire_owned() {
        Ok(permit) => Ok(permit),
        Err(_) => {
            runtime
                .metrics
                .backpressure_waits
                .fetch_add(1, Ordering::Relaxed);
            tokio::select! {
                permit = Arc::clone(&runtime.request_gate).acquire_owned() => {
                    permit.map_err(|err| GraphError::CorruptValue {
                        key: "query/transport/backpressure".to_string(),
                        reason: err.to_string(),
                    })
                }
                _ = cancellation_token.cancelled() => Err(cancelled_query_error()),
            }
        }
    }
}

#[cfg(feature = "query-transport")]
async fn acquire_namespace_query_permits(
    runtime: &Arc<QueryTransportServerRuntime>,
    namespace: &NamespacePath,
    cancellation_token: &QueryCancellationToken,
) -> Result<Vec<OwnedSemaphorePermit>> {
    let mut permits = Vec::new();
    for ancestor in namespace.ancestors_inclusive().rev() {
        let Some(gate) = runtime.namespace_query_gates.get(&ancestor) else {
            continue;
        };
        let permit = match Arc::clone(gate).try_acquire_owned() {
            Ok(permit) => permit,
            Err(_) => {
                runtime
                    .metrics
                    .namespace_quota_waits
                    .fetch_add(1, Ordering::Relaxed);
                tokio::select! {
                    permit = Arc::clone(gate).acquire_owned() => {
                        permit.map_err(|err| GraphError::CorruptValue {
                            key: format!("query/transport/namespace_quota/{ancestor}"),
                            reason: err.to_string(),
                        })?
                    }
                    _ = cancellation_token.cancelled() => return Err(cancelled_query_error()),
                }
            }
        };
        permits.push(permit);
    }
    Ok(permits)
}

#[cfg(feature = "query-transport")]
async fn ensure_query_not_cancelled(
    runtime: &QueryTransportServerRuntime,
    lifecycle_key: &QueryLifecycleKey,
    token: QueryLifecycleToken,
) -> Result<()> {
    let lifecycle = runtime.lifecycle.lock().await;
    let Some(entry) = lifecycle.queries.get(lifecycle_key) else {
        return Err(cancelled_query_error());
    };
    if entry.token != token || entry.state == QueryLifecycleState::Cancelled {
        return Err(cancelled_query_error());
    }
    Ok(())
}

#[cfg(feature = "query-transport")]
fn cancelled_query_error() -> GraphError {
    GraphError::QueryTimeout {
        operation: "query_transport_cancelled",
        elapsed_ms: 0,
        limit_ms: 0,
    }
}

#[cfg(feature = "query-transport")]
fn inactive_query_cancel_error(query_id: &str) -> GraphError {
    GraphError::UnsupportedQuery {
        dialect: "QueryTransport",
        feature: format!("no active query with id {query_id} was cancelled"),
    }
}

#[cfg(feature = "query-transport")]
fn authenticate_query_transport(
    runtime: &QueryTransportServerRuntime,
    auth: &QueryTransportAuth,
    identity: &QueryTransportConnectionIdentity,
) -> Result<QueryTransportPrincipal> {
    if let Some(principal) = runtime.config.auth_policy.authenticate(auth, identity) {
        return Ok(principal);
    }
    runtime
        .metrics
        .auth_failures
        .fetch_add(1, Ordering::Relaxed);
    Err(GraphError::UnsupportedQuery {
        dialect: "QueryTransport",
        feature: "unauthorized query transport request".to_string(),
    })
}

#[cfg(feature = "query-transport")]
fn authorize_query_transport_scope(
    runtime: &QueryTransportServerRuntime,
    principal: &QueryTransportPrincipal,
    scope: &GraphScope,
    action: QueryTransportAction,
) -> Result<()> {
    if runtime
        .config
        .scope_authorizer
        .authorize(principal, scope, action)
    {
        return Ok(());
    }
    runtime
        .metrics
        .namespace_access_denials
        .fetch_add(1, Ordering::Relaxed);
    Err(GraphError::GraphScopeAccessDenied {
        principal: principal.error_label().to_string(),
        action: action.as_str(),
        scope: scope.to_string(),
    })
}

#[cfg(feature = "query-transport")]
fn transport_error_response(
    runtime: &QueryTransportServerRuntime,
    err: GraphError,
) -> QueryTransportResponse {
    runtime
        .metrics
        .requests_failed
        .fetch_add(1, Ordering::Relaxed);
    let message = match &err {
        GraphError::AdmissionRejected { .. }
        | GraphError::GraphScopeAccessDenied { .. }
        | GraphError::MissingQueryParameter { .. }
        | GraphError::QueryParse { .. }
        | GraphError::QueryTimeout { .. }
        | GraphError::UnsupportedQuery { .. } => err.to_string(),
        _ => {
            tracing::warn!(
                target: "slatedb_graph_kernel",
                error = %err,
                "query transport suppressed internal error details"
            );
            "internal query execution error".to_string()
        }
    };
    QueryTransportResponse::Error { message }
}

#[cfg(feature = "query-transport")]
async fn write_query_transport_frame<S, T>(
    stream: &mut S,
    message: &T,
    max_frame_bytes: usize,
    metrics: &QueryTransportMetrics,
) -> Result<()>
where
    S: AsyncWrite + Unpin + ?Sized,
    T: serde::Serialize,
{
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
    metrics
        .bytes_sent
        .fetch_add(bytes.len() as u64, Ordering::Relaxed);
    stream
        .write_all(&bytes)
        .await
        .map_err(|err| transport_error("write", err))
}

#[cfg(feature = "query-transport")]
async fn read_query_transport_frame<S>(
    stream: &mut S,
    max_frame_bytes: usize,
    metrics: &QueryTransportMetrics,
) -> Result<Vec<u8>>
where
    S: AsyncRead + Unpin + ?Sized,
{
    let mut read_buffer = Vec::new();
    read_optional_query_transport_frame(stream, max_frame_bytes, metrics, &mut read_buffer)
        .await?
        .ok_or_else(|| {
            transport_protocol_error(
                "query/transport/frame",
                "connection closed before a response frame",
            )
        })
}

#[cfg(feature = "query-transport")]
async fn read_optional_query_transport_frame<S>(
    stream: &mut S,
    max_frame_bytes: usize,
    metrics: &QueryTransportMetrics,
    read_buffer: &mut Vec<u8>,
) -> Result<Option<Vec<u8>>>
where
    S: AsyncRead + Unpin + ?Sized,
{
    let mut buf = [0_u8; 4096];
    loop {
        if let Some(newline) = read_buffer.iter().position(|byte| *byte == b'\n') {
            if newline >= max_frame_bytes {
                return Err(GraphError::AdmissionRejected {
                    operation: "query_transport_frame",
                    actual: newline as u64,
                    limit: max_frame_bytes as u64,
                });
            }
            let mut frame: Vec<u8> = read_buffer.drain(..=newline).collect();
            frame.pop();
            if frame.is_empty() {
                return Err(transport_protocol_error(
                    "query/transport/frame",
                    "empty query transport frame",
                ));
            }
            metrics
                .bytes_received
                .fetch_add((frame.len() + 1) as u64, Ordering::Relaxed);
            return Ok(Some(frame));
        }
        if read_buffer.len() >= max_frame_bytes {
            return Err(GraphError::AdmissionRejected {
                operation: "query_transport_frame",
                actual: read_buffer.len() as u64,
                limit: max_frame_bytes as u64,
            });
        }
        let read = stream
            .read(&mut buf)
            .await
            .map_err(|err| transport_error("read", err))?;
        if read == 0 {
            if read_buffer.is_empty() {
                return Ok(None);
            }
            return Err(transport_protocol_error(
                "query/transport/frame",
                "connection closed before the frame delimiter",
            ));
        }
        read_buffer.extend_from_slice(&buf[..read]);
    }
}

#[cfg(feature = "query-transport")]
fn transport_error(operation: &'static str, err: std::io::Error) -> GraphError {
    GraphError::CorruptValue {
        key: format!("query/transport/{operation}"),
        reason: err.to_string(),
    }
}

#[cfg(feature = "query-transport")]
fn transport_config_error(reason: &str) -> Result<()> {
    Err(GraphError::UnsafeDurabilityConfig {
        operation: "query_transport_config",
        reason: reason.to_string(),
    })
}

#[cfg(feature = "query-transport")]
fn transport_timeout_error(operation: &'static str, timeout: Duration) -> GraphError {
    let limit_ms = timeout.as_millis().min(u128::from(u64::MAX)) as u64;
    GraphError::QueryTimeout {
        operation,
        elapsed_ms: limit_ms,
        limit_ms,
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
