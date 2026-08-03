use std::collections::{BTreeMap, VecDeque};
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine as _;
use futures::FutureExt;
use sha2::{Digest, Sha256};
use tokio::sync::{Mutex, OwnedSemaphorePermit, Semaphore};

use crate::query::coordination::{
    QueryCellClient, QueryTransportAction, QueryTransportAuthPolicy,
    QueryTransportConnectionIdentity, QueryTransportNamespaceQuotas, QueryTransportPrincipal,
    QueryTransportScopeAuthorizer, QueryTransportSecret, QueryTransportServerConfig,
};
use crate::query::opencypher::{
    classify_opencypher_query_access, opencypher_query_fingerprint,
    parse_opencypher_mutation_query_with_parameters, parse_opencypher_row_query_with_parameters,
    parse_opencypher_unwind_batch, OpenCypherQueryAccess, ParsedUnwindBatchKind,
};
use crate::{
    validate_component, AtomicDurationHistogram, DurationHistogramSnapshot, EdgeMetadata,
    GraphError, GraphId, GraphScope, NamespaceId, NamespacePath, QueryBatchEdge,
    QueryBatchMergePolicy, QueryBatchOperation, QueryBatchRelationship,
    QueryBatchRelationshipMerge, QueryBatchVertex, QueryCancellationToken, QueryColumn,
    QueryContext, QueryCursorToken, QueryParameterValue, QueryResultPage, QueryResultSet, QueryRow,
    Result, StorageSequence, VertexMetadata, VertexPropertyValue,
};
use tracing::Instrument as _;

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

const SCOPED_DATABASE_VERSION: &str = "scope1";

#[derive(Clone)]
pub struct HierarchicalClientDatabaseResolver {
    base_database: String,
    root_target: ClientQueryTarget,
}

impl HierarchicalClientDatabaseResolver {
    pub fn new(base_database: impl Into<String>, root_target: ClientQueryTarget) -> Result<Self> {
        let base_database = validate_database_name(base_database.into())?;
        Ok(Self {
            base_database,
            root_target,
        })
    }

    pub fn scoped_database_name(
        &self,
        tenant_id: &str,
        sub_tenant_id: Option<&str>,
    ) -> Result<String> {
        let tenant = encode_database_scope_id("tenant_id", tenant_id)?;
        let sub_tenant = sub_tenant_id
            .filter(|value| !value.is_empty())
            .map(|value| encode_database_scope_id("sub_tenant_id", value))
            .transpose()?
            .unwrap_or_else(|| "_".to_string());
        validate_database_name(format!(
            "{}.{}.{tenant}.{sub_tenant}",
            self.base_database, SCOPED_DATABASE_VERSION
        ))
    }

    fn scoped_target(&self, database: &str) -> Result<ClientQueryTarget> {
        let prefix = format!("{}.{}.", self.base_database, SCOPED_DATABASE_VERSION);
        let encoded = database
            .strip_prefix(&prefix)
            .ok_or_else(|| unknown_graph_database(database))?;
        let (tenant, sub_tenant) = encoded
            .split_once('.')
            .filter(|(_, sub_tenant)| !sub_tenant.contains('.'))
            .ok_or_else(|| unknown_graph_database(database))?;
        let tenant = canonical_database_component(database, tenant)?;
        let mut namespace = self
            .root_target
            .scope
            .namespace
            .child(NamespaceId::new(tenant)?)?;
        if sub_tenant != "_" {
            namespace = namespace.child(NamespaceId::new(canonical_database_component(
                database, sub_tenant,
            )?)?)?;
        }
        ClientQueryTarget::new(
            GraphScope::new(namespace, self.root_target.scope.graph_id.clone()),
            self.root_target.cell_id.clone(),
        )
    }
}

impl ClientDatabaseResolver for HierarchicalClientDatabaseResolver {
    fn resolve_database(&self, database: Option<&str>) -> Result<ClientQueryTarget> {
        let database = database.unwrap_or(&self.base_database);
        let database = validate_database_name(database.to_string())?;
        if database == self.base_database {
            return Ok(self.root_target.clone());
        }
        self.scoped_target(&database)
    }
}

fn encode_database_scope_id(component: &'static str, value: &str) -> Result<String> {
    if value.is_empty() || value.chars().any(char::is_control) {
        return Err(GraphError::InvalidKeyComponent {
            component,
            value: value.to_string(),
        });
    }
    let encoded = URL_SAFE_NO_PAD.encode(value.as_bytes());
    NamespaceId::new(encoded.clone())?;
    Ok(encoded)
}

fn canonical_database_component(database: &str, encoded: &str) -> Result<String> {
    if encoded.is_empty() {
        return Err(unknown_graph_database(database));
    }
    let bytes = URL_SAFE_NO_PAD
        .decode(encoded)
        .map_err(|_| unknown_graph_database(database))?;
    let value = String::from_utf8(bytes).map_err(|_| unknown_graph_database(database))?;
    if value.is_empty()
        || value.chars().any(char::is_control)
        || URL_SAFE_NO_PAD.encode(value.as_bytes()) != encoded
    {
        return Err(unknown_graph_database(database));
    }
    Ok(encoded.to_string())
}

fn unknown_graph_database(database: &str) -> GraphError {
    GraphError::UnsupportedQuery {
        dialect: "ClientProtocol",
        feature: format!("unknown graph database {database}"),
    }
}

/// One tenancy segment of a [`GraphScope`], in both the form the scope stores
/// and the form everything outside Turbolay uses.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ScopeTenant {
    /// The segment verbatim — URL-safe unpadded base64, as
    /// [`encode_database_scope_id`] wrote it. This is the spelling that also
    /// appears in the `scope` metric label and in the object-store prefix, so
    /// it is what joins a log line to a metric series or to an S3 path.
    pub scope_id: String,
    /// The decoded identity: the tenant id the rest of the platform knows, or
    /// the sub-tenant's own name. Falls back to [`Self::scope_id`] unchanged
    /// when the segment is not base64 this crate produced — a hand-built scope
    /// or a [`StaticClientDatabaseResolver`] target carries a literal name, and
    /// the literal name *is* the identity there.
    pub id: String,
}

/// The tenant and sub-tenant a [`GraphScope`] names.
///
/// [`HierarchicalClientDatabaseResolver`] writes exactly one layout — the
/// process's root namespace, then the tenant, then an optional sub-tenant — so
/// both identities are positional and can be read back from a bare scope
/// without the resolver that produced it. That is what makes this usable from
/// [`client_root_span`], which holds a [`ClientQueryRequest`] and nothing else,
/// and from the Bolt page spans, which hold even less.
///
/// A root namespace deeper than one segment would shift both positions. None is
/// configured: `GRAPH_NAMESPACE` is a single segment in `charts/turbolay`, in
/// `scripts/deploy_single_node_k3s.sh` and in `scripts/runtime_smoke.sh`, and
/// the HTTP `x-graph-namespace` header carries the same three-segment path Bolt
/// encodes into its database name. A deeper root does not misreport a tenant as
/// some *other* tenant — it reports a namespace segment that is not one — but
/// it is the assumption to revisit first if these fields ever look wrong.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct ScopeTenancy {
    pub tenant: Option<ScopeTenant>,
    pub sub_tenant: Option<ScopeTenant>,
}

impl ScopeTenancy {
    pub(crate) fn from_scope(scope: &GraphScope) -> Self {
        let segments = scope.namespace.segments();
        Self {
            tenant: segments.get(1).map(ScopeTenant::from_segment),
            sub_tenant: segments.get(2).map(ScopeTenant::from_segment),
        }
    }
}

impl ScopeTenant {
    fn from_segment(segment: &NamespaceId) -> Self {
        let scope_id = segment.as_str().to_string();
        let id = decode_scope_id(&scope_id).unwrap_or_else(|| scope_id.clone());
        Self { scope_id, id }
    }
}

/// Decode a scope segment written by [`encode_database_scope_id`].
///
/// `None` — never a panic, never an error — for anything else.
///
/// The round-trip check is the canonicality test
/// [`canonical_database_component`] applies on the resolution path, and it is
/// necessary but *not* sufficient here. That function is deciding whether a
/// segment came from the encoder, having already been told by the database
/// prefix that it should have; this one is deciding the same question with no
/// such promise, and short literals answer it wrongly. `acme` is canonical
/// base64 for two well-formed UTF-8 code points, so a round-trip check alone
/// reports the namespace `acme` as a tenant named `iʮ` — a nonsense value in
/// the warehouse's `tenant_id` column, which is strictly worse than leaving the
/// segment as it was found.
///
/// So the decoded value must additionally be printable ASCII: the same rule
/// [`sanitize_caller_metadata`] applies to every other caller-supplied string
/// that becomes a log field. It rejects `acme` and accepts every id and
/// sub-tenant name the platform issues. The cost is a genuinely non-ASCII
/// sub-tenant name — a mailbox label in Japanese — reported by its base64
/// spelling instead; nothing is lost, since that spelling is what
/// `turbolay.sub_tenant.scope_id` carries in every case anyway.
fn decode_scope_id(encoded: &str) -> Option<String> {
    let bytes = URL_SAFE_NO_PAD.decode(encoded).ok()?;
    let value = String::from_utf8(bytes).ok()?;
    if value.is_empty()
        || !value.chars().all(|ch| ch.is_ascii_graphic() || ch == ' ')
        || URL_SAFE_NO_PAD.encode(value.as_bytes()) != encoded
    {
        return None;
    }
    Some(value)
}

/// Record the tenancy of `scope` on `span`.
///
/// The four fields are declared `tracing::field::Empty` at each span's creation
/// and filled here, because a scope with no tenancy must leave them *absent*
/// rather than blank: `tenant_id = ""` is a value the log warehouse will
/// happily store in a column nobody can then distinguish from a tenant that
/// genuinely reported nothing.
///
/// Both spellings go on the span. The decoded id is the one that joins to every
/// other system's `tenant_id`; the base64 one is the one that joins to the
/// `scope` metric label and to the object-store prefix, and it is not derivable
/// from the decoded value in the fallback case above.
pub(crate) fn record_scope_tenancy(span: &tracing::Span, scope: &GraphScope) {
    let tenancy = ScopeTenancy::from_scope(scope);
    if let Some(tenant) = &tenancy.tenant {
        span.record("turbolay.tenant_id", tenant.id.as_str());
        span.record("turbolay.tenant.scope_id", tenant.scope_id.as_str());
    }
    if let Some(sub_tenant) = &tenancy.sub_tenant {
        span.record("turbolay.sub_tenant_id", sub_tenant.id.as_str());
        span.record("turbolay.sub_tenant.scope_id", sub_tenant.scope_id.as_str());
    }
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
            .ok_or_else(|| unknown_graph_database(&database))
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
enum ClientMutationIdempotencyKey {
    /// Minted by this server and therefore globally unique without a caller
    /// namespace.
    ServerGenerated(String),
    /// Chosen by an authenticated caller and scoped to that principal before
    /// it enters the cell's durable idempotency namespace.
    CallerSupplied(String),
}

impl ClientMutationIdempotencyKey {
    fn value(&self) -> &str {
        match self {
            Self::ServerGenerated(value) | Self::CallerSupplied(value) => value,
        }
    }
}

#[derive(Clone, Debug)]
pub struct ClientQueryRequest {
    pub target: ClientQueryTarget,
    /// Ephemeral request handle used for cancellation, cursor ownership, and
    /// response correlation. It is not a durable mutation identity.
    pub query_id: String,
    pub query: String,
    pub parameters: BTreeMap<String, QueryParameterValue>,
    pub read_epoch: Option<StorageSequence>,
    pub max_runtime_ms: Option<u64>,
    pub bookmark: Option<ClientBookmark>,
    pub consistency: ClientReadConsistency,
    /// Stable identity for durable mutation deduplication, including whether
    /// the server or an authenticated caller chose it. Caller-owned values are
    /// principal-scoped by `query_context`; generated values already carry
    /// global uniqueness. Other transports retain the historical query-id
    /// fallback when this field is absent.
    mutation_idempotency_key: Option<ClientMutationIdempotencyKey>,
    /// Caller-supplied request identifier, carried through from Bolt
    /// `tx_metadata` (`turbolay.correlation_id`). It exists so a Turbolay span
    /// and the caller's own log line share a field; Turbolay never mints one,
    /// because a server-invented value looks like a join key and joins nothing.
    pub correlation_id: Option<String>,
    /// Caller-supplied operation label (`turbolay.caller.step`) — which step of
    /// a multi-step caller workflow issued this query.
    pub caller_step: Option<String>,
}

/// Maximum accepted length of a caller-supplied correlation id or step label.
const MAX_CALLER_METADATA_LEN: usize = 128;

/// Validate a caller-supplied metadata value: printable ASCII, bounded length,
/// non-empty. Returns `None` for anything else.
///
/// `tx_metadata` arrives from any Bolt client and becomes both a span attribute
/// and a log field, so it is validated here rather than trusted from the
/// caller's own sanitiser. Unlike the ingestion side, an invalid value is
/// **dropped** rather than replaced: a fabricated identifier is worse than an
/// absent one.
pub fn sanitize_caller_metadata(value: &str) -> Option<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() || trimmed.len() > MAX_CALLER_METADATA_LEN {
        return None;
    }
    if !trimmed.chars().all(|ch| ch.is_ascii_graphic() || ch == ' ') {
        return None;
    }
    Some(trimmed.to_string())
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
            mutation_idempotency_key: None,
            correlation_id: None,
            caller_step: None,
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

    /// Attach a caller-owned retry identity. The service binds this value to
    /// the authenticated principal before using it for durable deduplication.
    pub fn with_mutation_idempotency_key(
        mut self,
        mutation_idempotency_key: impl Into<String>,
    ) -> Self {
        self.mutation_idempotency_key = Some(ClientMutationIdempotencyKey::CallerSupplied(
            mutation_idempotency_key.into(),
        ));
        self
    }

    pub(crate) fn with_server_generated_mutation_idempotency_key(
        mut self,
        mutation_idempotency_key: impl Into<String>,
    ) -> Self {
        self.mutation_idempotency_key = Some(ClientMutationIdempotencyKey::ServerGenerated(
            mutation_idempotency_key.into(),
        ));
        self
    }

    pub(crate) fn mutation_idempotency_key(&self) -> Option<&str> {
        self.mutation_idempotency_key
            .as_ref()
            .map(ClientMutationIdempotencyKey::value)
    }

    /// Attach the caller's correlation id. Invalid values are dropped rather
    /// than stored; see [`sanitize_caller_metadata`].
    pub fn with_correlation_id(mut self, correlation_id: impl AsRef<str>) -> Self {
        self.correlation_id = sanitize_caller_metadata(correlation_id.as_ref());
        self
    }

    /// Attach the caller's operation label. Invalid values are dropped rather
    /// than stored; see [`sanitize_caller_metadata`].
    pub fn with_caller_step(mut self, caller_step: impl AsRef<str>) -> Self {
        self.caller_step = sanitize_caller_metadata(caller_step.as_ref());
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
    /// The same failures as [`Self::queries_failed`], split by
    /// [`GraphError::class`] and indexed by [`GraphError::class_index`].
    ///
    /// Total by construction: one call increments both, so the array sums to the
    /// scalar. Enumerate it with [`Self::class_counter_fields`].
    pub queries_failed_by_class: [u64; GraphError::CLASS_COUNT],
    pub rows_returned: u64,
    pub auth_failures: u64,
    pub scope_denials: u64,
    pub cancellations: u64,
    pub backpressure_waits: u64,
    pub prepare_requests: u64,
    pub prepare_duration_us: u64,
    /// Total microseconds across both latency histograms below.
    ///
    /// Retained so nothing that read the old sum has to change. It is derived
    /// rather than stored: every execution is recorded into exactly one of the
    /// two histograms, so their sums add up to the previous value exactly.
    pub execution_duration_us: u64,
    /// End-to-end execution of a read, from the authorization check to the
    /// assembled first page — including the server-cursor start.
    pub read_latency: DurationHistogramSnapshot,
    /// End-to-end execution of a mutation, which is a different distribution
    /// entirely: it carries a commit, and it can never be served from a cursor.
    pub write_latency: DurationHistogramSnapshot,
}

crate::core::metrics::snapshot_fields!(ClientQueryMetricsSnapshot {
    counters {
        queries_started,
        queries_completed,
        queries_failed,
        rows_returned,
        auth_failures,
        scope_denials,
        cancellations,
        backpressure_waits,
        prepare_requests,
        prepare_duration_us,
        execution_duration_us,
    }
    histograms {
        read_latency,
        write_latency,
    }
    class_counters {
        queries_failed_by_class,
    }
});

#[derive(Default)]
struct ClientQueryMetrics {
    queries_started: AtomicU64,
    queries_completed: AtomicU64,
    queries_failed: AtomicU64,
    queries_failed_by_class: crate::core::metrics::ErrorClassCounters,
    rows_returned: AtomicU64,
    auth_failures: AtomicU64,
    scope_denials: AtomicU64,
    cancellations: AtomicU64,
    backpressure_waits: AtomicU64,
    prepare_requests: AtomicU64,
    prepare_duration_us: AtomicU64,
    // One sum fed two structurally different populations before this. Every
    // execution funnels through `execute_prepared_page_inner`, which already
    // knows the `QueryTransportAction` and threw it away, so a mutation's
    // commit path and a page served out of a warm server cursor landed in one
    // distribution. Splitting on the action costs nothing and is the whole
    // reason a percentile out of these is worth reading.
    read_latency: AtomicDurationHistogram,
    write_latency: AtomicDurationHistogram,
}

impl ClientQueryMetrics {
    fn snapshot(&self) -> ClientQueryMetricsSnapshot {
        let read_latency = self.read_latency.snapshot();
        let write_latency = self.write_latency.snapshot();
        ClientQueryMetricsSnapshot {
            queries_started: self.queries_started.load(Ordering::Relaxed),
            queries_completed: self.queries_completed.load(Ordering::Relaxed),
            queries_failed: self.queries_failed.load(Ordering::Relaxed),
            queries_failed_by_class: crate::core::metrics::load_class_counters(
                &self.queries_failed_by_class,
            ),
            rows_returned: self.rows_returned.load(Ordering::Relaxed),
            auth_failures: self.auth_failures.load(Ordering::Relaxed),
            scope_denials: self.scope_denials.load(Ordering::Relaxed),
            cancellations: self.cancellations.load(Ordering::Relaxed),
            backpressure_waits: self.backpressure_waits.load(Ordering::Relaxed),
            prepare_requests: self.prepare_requests.load(Ordering::Relaxed),
            prepare_duration_us: self.prepare_duration_us.load(Ordering::Relaxed),
            execution_duration_us: read_latency.sum_us.saturating_add(write_latency.sum_us),
            read_latency,
            write_latency,
        }
    }

    /// Count one finished request: its rows, and either its completion or its
    /// failure under the failing subsystem's class.
    ///
    /// The scalar and the per-class array are incremented by this one call, so
    /// the array sums to the scalar by construction rather than by every future
    /// call site remembering to bump both.
    fn record_result(&self, rows: usize, error: Option<&GraphError>) {
        self.rows_returned.fetch_add(rows as u64, Ordering::Relaxed);
        match error {
            None => {
                self.queries_completed.fetch_add(1, Ordering::Relaxed);
            }
            Some(error) => {
                self.queries_failed.fetch_add(1, Ordering::Relaxed);
                self.queries_failed_by_class[error.class_index()].fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    /// Route one execution's elapsed time to the histogram for its action.
    ///
    /// Only `Read` and `Write` can reach an execution: the action comes from
    /// `authorize_query`, which maps a two-variant access classification.
    /// `Cancel` and `Admin` authorize control frames, which never run a query.
    /// They are spelled out rather than matched with `_` so that adding a
    /// variant is a compile error here instead of a silent misfiling, and they
    /// fold into reads so that the two sums stay total over every observation.
    fn record_execution(&self, action: QueryTransportAction, elapsed: Duration) {
        match action {
            QueryTransportAction::Write => self.write_latency.record(elapsed),
            QueryTransportAction::Read
            | QueryTransportAction::Cancel
            | QueryTransportAction::Admin => self.read_latency.record(elapsed),
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

    /// Block until this cell has durably reached the bookmarked epoch.
    ///
    /// The span exists only when a bookmark does, and its duration *is*
    /// read-your-writes latency, which no counter measures today. It is the
    /// first thing to look at for the BFG-007 class of complaint — "the write
    /// succeeded but the read did not see it" — because it separates "we waited
    /// for the epoch" from "we never waited and read stale".
    ///
    /// The bookmark value itself is never recorded: it is an opaque
    /// caller-held token, and §2 puts it on the never-recorded list. Its
    /// *epoch* is recorded, which is the part that explains anything.
    pub async fn ensure_bookmark(&self, bookmark: &ClientBookmark) -> Result<()> {
        let span = tracing::info_span!(
            "query.bookmark_wait",
            turbolay.scope = %bookmark.target.scope,
            turbolay.cell_id = %bookmark.target.cell_id,
            turbolay.read_epoch = bookmark.epoch,
            observed_epoch = tracing::field::Empty,
            error.class = tracing::field::Empty,
            turbolay.sampling.tail_keep = tracing::field::Empty,
        );
        let outcome = async {
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
            tracing::Span::current().record("observed_epoch", current_sequence);
            if current_sequence < bookmark.epoch {
                return Err(GraphError::SnapshotAhead {
                    cell_id: bookmark.target.cell_id.clone(),
                    read_epoch: bookmark.epoch,
                    current_epoch: current_sequence,
                });
            }
            Ok(())
        }
        .instrument(span.clone())
        .await;
        if let Err(err) = &outcome {
            record_span_error(&span, err);
        }
        outcome
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
        request: ClientQueryRequest,
    ) -> Result<ClientQueryResult> {
        let span = client_root_span(&request, None);
        let result = self
            .execute_rows_inner(session, request)
            .instrument(span.clone())
            .await;
        match &result {
            Ok(response) => {
                span.record(
                    "turbolay.query.rows_returned",
                    response.result.rows.len() as u64,
                );
                if let Some(read_epoch) = response.read_epoch {
                    span.record("turbolay.read_epoch", read_epoch);
                }
            }
            Err(err) => record_span_error(&span, err),
        }
        result
    }

    async fn execute_rows_inner(
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
        // The limit belongs on the root, not on admission: the staging report
        // of "29999 ms; limit is 29999 ms" is only legible next to the budget
        // it was measured against.
        tracing::Span::current().record("runtime_limit_ms", runtime_limit_ms);
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
                        session,
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
            result.as_ref().err(),
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
        let span = client_root_span(&request, None);
        let result = self
            .execute_page_inner(session, request, cursor, page_size)
            .instrument(span.clone())
            .await;
        match &result {
            Ok(response) => {
                span.record(
                    "turbolay.query.rows_returned",
                    response.page.rows.len() as u64,
                );
                if let Some(read_epoch) = response.read_epoch {
                    span.record("turbolay.read_epoch", read_epoch);
                }
            }
            Err(err) => record_span_error(&span, err),
        }
        result
    }

    async fn execute_page_inner(
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
        // Already inside this request's root span, so go straight to the body.
        self.execute_prepared_page_inner(session, prepared, cursor, page_size)
            .await
    }

    /// The Bolt execution boundary: RUN prepares, each PULL lands here.
    ///
    /// This deliberately opens **no root span**. Bolt never passes through
    /// [`Self::execute_page`], so it would be easy to conclude — as a first
    /// pass here did — that the root belongs at this boundary. It does not: a
    /// client that pages lazily calls this once per PULL, so a root here yields
    /// one trace *per page* rather than one per statement, and the very
    /// question `query.page` exists to answer ("where did this slow query
    /// actually spend its time across its pages?") becomes unanswerable
    /// precisely because each page is a separate trace.
    ///
    /// Instead `client.query` is opened once at RUN in the Bolt loop, held on
    /// `PendingBoltResult`, and every `query.page` is parented to it. This
    /// function inherits that context through the ambient subscriber, so the
    /// spans below it still carry the fingerprint and the correlation id.
    ///
    /// Errors are recorded on the enclosing `query.page`, which already carries
    /// this page's `rows_returned` and `read_epoch`.
    #[cfg_attr(not(feature = "bolt-server"), allow(dead_code))]
    pub(crate) async fn execute_prepared_page(
        &self,
        session: &ClientQuerySession,
        prepared: PreparedClientQuery,
        cursor: Option<QueryCursorToken>,
        page_size: usize,
    ) -> Result<ClientQueryPage> {
        self.execute_prepared_page_inner(session, prepared, cursor, page_size)
            .await
    }

    async fn execute_prepared_page_inner(
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
                        session,
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
            result.as_ref().err(),
        );
        self.inner
            .metrics
            .record_execution(action, execution_started.elapsed());
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
        let runtime_limit_ms = self.normalize_runtime_limit(&mut request)?;
        tracing::Span::current().record("runtime_limit_ms", runtime_limit_ms);
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
        if let Some(mutation_idempotency_key) = request.mutation_idempotency_key() {
            validate_component("mutation_idempotency_key", mutation_idempotency_key)?;
        }
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
            // The read already knows its sequence; nothing is fetched, so
            // there is nothing to time.
            read_storage_sequence
        } else {
            // `write.bookmark` from §4 of the telemetry plan, and the last span
            // on the write path. It is not bookkeeping: this arm reaches the
            // object store on *every* mutation to read back the sequence the
            // commit landed at, and that latency is charged to the caller's
            // write even though no writing is left to do.
            //
            // It is also the number the whole freshness class of bugs is about
            // — the bookmark handed back here is what a later read pins its
            // epoch to, so `turbolay.commit_epoch` recorded here is the value
            // `query.bookmark_wait` is waiting to catch up with, on a different
            // trace and possibly a different node.
            let span = tracing::info_span!(
                "write.bookmark",
                turbolay.scope = %request.target.scope,
                turbolay.cell_id = %request.target.cell_id,
                turbolay.commit_epoch = tracing::field::Empty,
                error.class = tracing::field::Empty,
                turbolay.sampling.tail_keep = tracing::field::Empty,
            );
            let sequence = self
                .inner
                .client
                .current_storage_sequence(&request.target.scope, &request.target.cell_id)
                .instrument(span.clone())
                .await
                .inspect_err(|err| record_span_error(&span, err))?;
            if let Some(sequence) = sequence {
                span.record("turbolay.commit_epoch", sequence);
            }
            sequence
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
            // Admission is the only phase that can consume the whole runtime
            // budget without the query having started, so it gets its own span:
            // a trace where `query.admission` is most of the wall clock is a
            // backpressure report, not a slow query.
            let admission = tracing::info_span!(
                "query.admission",
                turbolay.scope = %key.scope,
                error.class = tracing::field::Empty,
            );
            let permits = async {
                let namespace_permits = self
                    .acquire_namespace_permits(&key.scope.namespace, cancellation_token)
                    .await?;
                let query_permit = self.acquire_query_permit(cancellation_token).await?;
                Ok((namespace_permits, query_permit))
            }
            .instrument(admission.clone())
            .await;
            let (_namespace_permits, _query_permit) = match permits {
                Ok(permits) => permits,
                Err(err) => {
                    record_span_error(&admission, &err);
                    return Err(err);
                }
            };
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

    /// Count one finished request.
    ///
    /// Takes the error rather than a `succeeded: bool` because both callers hold
    /// the `Result` and `as_ref().err()` is free there, and because the class is
    /// the whole reason to distinguish one failure from another: a rate of
    /// `queries_failed` says a client is unhappy, and the same rate split by
    /// class says which subsystem to look at first.
    fn record_result_metrics(
        &self,
        _action: QueryTransportAction,
        rows: usize,
        error: Option<&GraphError>,
    ) {
        self.inner.metrics.record_result(rows, error);
    }
}

/// Record a failure on the span that is closest to producing it.
///
/// The same error carried up to the root says only that a request failed; the
/// class recorded here says which subsystem refused, which is a different
/// question and the one an operator asks first.
fn record_span_error(span: &tracing::Span, err: &GraphError) {
    span.record("error.class", err.class());
    // The head sampler decided this trace's fate when the root span started,
    // which was before this request could possibly be known to fail, so nothing
    // recorded here can change it. The marker is for the collector's tail
    // sampler, which buffers the whole trace and *can* — see
    // `turbolay_telemetry::sampling` for the policy that deployment owes us.
    span.record("turbolay.sampling.tail_keep", "error");
}

/// The trace root for one client request.
///
/// Opened at the service boundary — the point both Bolt and HTTP funnel
/// through — so that every span below it inherits `scope`, `cell_id`, the query
/// fingerprint and the caller's correlation fields without any of them being
/// threaded through a call signature.
///
/// Reads and mutations get different *names* rather than an attribute, because
/// the write path below this point is a different span tree entirely and a
/// backend that has to filter on an attribute to separate them will get it
/// wrong once.
///
/// Bolt opens this once at RUN and keeps it for the whole statement rather than
/// per PULL — see [`Self::execute_prepared_page`]. HTTP opens it per call,
/// which for a non-paging transport is the same thing.
pub(crate) fn client_root_span(
    request: &ClientQueryRequest,
    action: Option<QueryTransportAction>,
) -> tracing::Span {
    let mutation = match action {
        Some(action) => action == QueryTransportAction::Write,
        // Cheap: classification runs through the same thread-local parse cache
        // the request is about to use anyway, so this is a cache lookup and the
        // authorization path below re-reads the same entry. A parse failure is
        // not decided here — it surfaces with its real error a few lines later.
        None => matches!(
            classify_opencypher_query_access(&request.query),
            Ok(OpenCypherQueryAccess::Write)
        ),
    };
    let fingerprint = opencypher_query_fingerprint(&request.query);
    macro_rules! root_span {
        ($name:literal) => {
            tracing::info_span!(
                $name,
                db.system.name = "neo4j",
                turbolay.scope = %request.target.scope,
                turbolay.cell_id = %request.target.cell_id,
                turbolay.tenant_id = tracing::field::Empty,
                turbolay.tenant.scope_id = tracing::field::Empty,
                turbolay.sub_tenant_id = tracing::field::Empty,
                turbolay.sub_tenant.scope_id = tracing::field::Empty,
                turbolay.query.fingerprint = %fingerprint,
                turbolay.consistency = ?request.consistency,
                turbolay.correlation_id = tracing::field::Empty,
                turbolay.caller.step = tracing::field::Empty,
                turbolay.read_epoch = tracing::field::Empty,
                turbolay.query.rows_returned = tracing::field::Empty,
                runtime_limit_ms = tracing::field::Empty,
                error.class = tracing::field::Empty,
                turbolay.sampling.tail_keep = tracing::field::Empty,
            )
        };
    }
    let span = if mutation {
        root_span!("client.mutate")
    } else {
        root_span!("client.query")
    };
    // The tenancy goes on the root and nowhere else, so that every line logged
    // under this request — planner warnings, admission refusals, the error the
    // query dies of — is attributable to a customer without any call site
    // knowing there is a customer. `turbolay.scope` above already contains both
    // segments, but only as one opaque path a warehouse cannot split.
    record_scope_tenancy(&span, &request.target.scope);
    // Absent rather than empty: a blank correlation id is indistinguishable
    // from one that failed validation, and both would join to nothing.
    if let Some(correlation_id) = &request.correlation_id {
        span.record("turbolay.correlation_id", correlation_id.as_str());
    }
    if let Some(caller_step) = &request.caller_step {
        span.record("turbolay.caller.step", caller_step.as_str());
    }
    if let Some(limit) = request.max_runtime_ms {
        span.record("runtime_limit_ms", limit);
    }
    span
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
    session: &ClientQuerySession,
    request: &ClientQueryRequest,
    parameters: BTreeMap<String, VertexPropertyValue>,
    cancellation_token: QueryCancellationToken,
) -> QueryContext {
    let mutation_idempotency_key = match request.mutation_idempotency_key.as_ref() {
        Some(ClientMutationIdempotencyKey::ServerGenerated(value)) => value.clone(),
        Some(ClientMutationIdempotencyKey::CallerSupplied(value)) => {
            principal_scoped_mutation_idempotency_key(session.principal(), value)
        }
        None => request.query_id.clone(),
    };
    let mut context = QueryContext::new(&request.target.cell_id, mutation_idempotency_key)
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

fn principal_scoped_mutation_idempotency_key(
    principal: &QueryTransportPrincipal,
    caller_key: &str,
) -> String {
    // QueryTransportPrincipal already contains only a one-way bearer-token hash
    // or an mTLS certificate fingerprint. Hash it once more so the durable key
    // reveals neither principal kind nor identifier while remaining stable on
    // every node and after process restarts.
    let principal_digest = Sha256::digest(principal.as_str().as_bytes());
    format!(
        "principal-v1-{}-{caller_key}",
        URL_SAFE_NO_PAD.encode(principal_digest)
    )
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
            update_if_newer_by,
            create_only_properties,
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
            merge_policy: update_if_newer_by.map(|update_if_newer_by| QueryBatchMergePolicy {
                update_if_newer_by,
                create_only_properties,
            }),
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
            update_if_newer_by,
            create_only_properties,
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
                merge_policy: update_if_newer_by.map(|update_if_newer_by| QueryBatchMergePolicy {
                    update_if_newer_by,
                    create_only_properties,
                }),
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
