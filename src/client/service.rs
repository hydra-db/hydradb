mod memory_estimates;
use crate::core::memory_diagnostics::{
    request_estimated_bytes, MemoryDiagnosticGuard, MemoryStage, REQUEST_ESTIMATED_BYTES,
};
use crate::{QueryFailureReason, QueryFailureStage};
use std::collections::{BTreeMap, VecDeque};
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};
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
#[cfg(feature = "experimental-cypher-engine")]
use crate::query::experimental_cypher::{experimental_query_columns, prepare_experimental_cypher};
use crate::query::opencypher::{
    classify_opencypher_query_access, opencypher_query_fingerprint,
    parse_opencypher_mutation_query_with_list_parameters,
    parse_opencypher_row_query_with_list_parameters, parse_opencypher_unwind_batch,
    OpenCypherQueryAccess, ParsedUnwindBatchKind, ParsedUnwindConstraintValue,
    ParsedUnwindVertexConstraints,
};
use crate::query::path_procedure::parse_native_path_procedure_columns;
use crate::{
    validate_component, AtomicDurationHistogram, CypherEngineMode, DurationHistogramSnapshot,
    EdgeMetadata, GraphError, GraphId, GraphScope, NamespaceId, NamespacePath, QueryBatchEdge,
    QueryBatchIsolatedVertex, QueryBatchMergePolicy, QueryBatchOperation, QueryBatchRelationship,
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
        reason: QueryFailureReason::InvalidRequest,
        dialect: "ClientProtocol",
        feature: format!("unknown graph database {database}"),
    }
}

/// One tenancy segment of a [`GraphScope`], in both the form the scope stores
/// and the form everything outside HydraDB uses.
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
/// configured: `GRAPH_NAMESPACE` is a single segment in `charts/hydradb`, in
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
/// `hydradb.sub_tenant.scope_id` carries in every case anyway.
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
        span.record("hydradb.tenant_id", tenant.id.as_str());
        span.record("hydradb.tenant.scope_id", tenant.scope_id.as_str());
    }
    if let Some(sub_tenant) = &tenancy.sub_tenant {
        span.record("hydradb.sub_tenant_id", sub_tenant.id.as_str());
        span.record("hydradb.sub_tenant.scope_id", sub_tenant.scope_id.as_str());
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
                    reason: QueryFailureReason::InvalidRequest,
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
        reason: QueryFailureReason::InvalidRequest,
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
        .as_chunks::<2>()
        .0
        .iter()
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
    /// `tx_metadata` (`hydradb.correlation_id`). It exists so a HydraDB span
    /// and the caller's own log line share a field; HydraDB never mints one,
    /// because a server-invented value looks like a join key and joins nothing.
    pub correlation_id: Option<String>,
    /// Caller-supplied operation label (`hydradb.caller.step`) — which step of
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

/// The scalar map every engine has always taken, paired with the list sidecar
/// that `IN` reads. Bound parameters travel as this pair from the transport
/// door to the query context.
type BoundQueryParameters = (
    BTreeMap<String, VertexPropertyValue>,
    BTreeMap<String, Vec<VertexPropertyValue>>,
);

#[derive(Debug)]
pub(crate) struct PreparedClientQuery {
    memory_diagnostic: MemoryDiagnosticGuard,
    pub(crate) request: ClientQueryRequest,
    pub(crate) action: QueryTransportAction,
    pub(crate) columns: Vec<QueryColumn>,
    pub(crate) scalar_parameters: BTreeMap<String, VertexPropertyValue>,
    /// Carried across pages for the same reason the scalars are: a Bolt cursor
    /// is resumed from this struct, and a resumed page has to bind exactly what
    /// the first page bound.
    pub(crate) list_parameters: BTreeMap<String, Vec<VertexPropertyValue>>,
    pub(crate) batch_operation: Option<QueryBatchOperation>,
    /// The engine resolved when this statement was prepared. A Bolt RUN
    /// prepares and each PULL executes, so resolving again at PULL would let a
    /// runtime override flip the engine between a statement's parse and its
    /// execution. Every later stage reads this field, never the service.
    pub(crate) cypher_engine: CypherEngineMode,
}

impl Clone for PreparedClientQuery {
    fn clone(&self) -> Self {
        let request = self.request.clone();
        let scalar_parameters = self.scalar_parameters.clone();
        let list_parameters = self.list_parameters.clone();
        let batch_operation = self.batch_operation.clone();
        let estimated_bytes = memory_estimates::request(&request)
            .saturating_add(memory_estimates::bound(
                &scalar_parameters,
                &list_parameters,
            ))
            .saturating_add(memory_estimates::batch(&batch_operation));
        Self {
            memory_diagnostic: MemoryDiagnosticGuard::new(
                MemoryStage::ClientPrepared,
                estimated_bytes,
            ),
            request,
            action: self.action,
            columns: self.columns.clone(),
            scalar_parameters,
            list_parameters,
            batch_operation,
            cypher_engine: self.cypher_engine,
        }
    }
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
    pub cypher_engine: CypherEngineMode,
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
            cypher_engine: CypherEngineMode::Legacy,
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

    pub fn with_cypher_engine(mut self, cypher_engine: CypherEngineMode) -> Self {
        self.cypher_engine = cypher_engine;
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
                reason: QueryFailureReason::InvalidRequest,
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
    /// Query failures -- the `query` error class -- by stage and
    /// [`crate::QueryFailureReason`]. Enumerate it with
    /// [`Self::failure_counter_fields`].
    ///
    /// Only failures with a reason: timeouts, admission, routing, storage and
    /// the rest are [`Self::queries_failed_by_class`]'s, and so is a remote
    /// failure its owner did not classify. Recorded at every exit of a request
    /// the service accepts, including the prepare step that parses and plans
    /// -- where an unsupported query fails, and which `queries_failed` never
    /// saw -- plus HTTP's `read_epoch` rejection, which happens before the
    /// service sees the request.
    ///
    /// One field per engine, for the reason
    /// [`Self::read_latency_legacy`] gives: the comparison this family exists
    /// for is legacy against experimental, and a single population labelled at
    /// export time would hand its whole history to whichever engine a scrape
    /// happens to find effective. A statement that was admitted carries its
    /// own engine here; one rejected before admission — HTTP's `read_epoch`
    /// check, a malformed request — is counted against the engine effective
    /// at that moment, which is the only true answer for a statement no
    /// engine ever ran.
    pub queries_failed_by_reason_legacy: crate::QueryFailureCountsSnapshot,
    /// [`Self::queries_failed_by_reason_legacy`] for the experimental engine.
    pub queries_failed_by_reason_experimental: crate::QueryFailureCountsSnapshot,
    pub rows_returned: u64,
    pub auth_failures: u64,
    pub scope_denials: u64,
    pub cancellations: u64,
    pub backpressure_waits: u64,
    pub prepare_requests: u64,
    pub prepare_duration_us: u64,
    /// Causal-consistency bookmark waits served, of any outcome.
    ///
    /// The **denominator**. Every counter below is a fraction of it, and none
    /// of them is alertable without it: "forty waits polled" is a fleet at rest
    /// or a fleet on fire depending on whether forty-one waits happened or four
    /// hundred thousand did. Change 4 of
    /// `docs/plans/2026-08-21-cell-affine-read-routing.md` asks for the pair
    /// for exactly this reason.
    ///
    /// It restates [`Self::bookmark_wait_latency`]'s `count()`, and does so
    /// deliberately rather than through
    /// `PrometheusCounterExport::Derived`: a histogram's `_count` is a
    /// *suffix* of a histogram family, so an alert dividing a counter series by
    /// one is a shape a reviewer has to check by hand. One call increments both,
    /// so they cannot drift.
    pub bookmark_waits: u64,
    /// Waits that fell through to the 10ms poll loop — i.e. `durable_sequence()`
    /// did not satisfy the bookmark on its first check.
    ///
    /// **This is the alert.** After cell-affine read routing
    /// (`GRAPH_READ_ROUTING=owner`) a read lands on the node that holds the
    /// cell's writer, whose `durable_seq` is at or past any bookmark minted
    /// from a commit there, so the loop is unreachable and this reads ~0 in
    /// steady state. It goes non-zero around a writer handoff, which is the one
    /// window where it should, and it goes and stays non-zero if reads drift
    /// back onto non-owners — which is the regression this exists to make
    /// visible without re-deriving
    /// `docs/2026-08-21-read-path-30s-timeout-findings.md`.
    pub bookmark_waits_polled: u64,
    /// Waits that ran out of `max_bookmark_wait_ms` and returned
    /// `SnapshotAhead`.
    ///
    /// Not a duplicate of `queries_failed_by_class{error_class="freshness"}`,
    /// which is the obvious objection. On the Bolt path the wait runs inside
    /// `prepare_page_request`, and a prepare that fails never reaches
    /// `record_result_metrics` -- it is traced and logged and counted nowhere
    /// (`src/client/bolt.rs`, the `Bolt RUN preparation failed` arm;
    /// `queries_failed_by_reason` counts only query failures). So before this
    /// counter the error that step 3 of the plan made *reachable* was still
    /// not *countable* on the path that produces it. It is also the only
    /// series that attributes a freshness failure to the bookmark wait rather
    /// than to any of the other `SnapshotAhead` sites in the kernel — artifact
    /// builds, maintenance, path procedures and row execution all raise the
    /// same variant.
    pub bookmark_waits_declined: u64,
    /// Waits answered from the cell writer's own commit status — the free path.
    pub bookmark_waits_on_cell_writer: u64,
    /// Waits answered from a `DbReader` — the node does not hold this cell's
    /// writer.
    ///
    /// The single most diagnostic fact on the read path, and nothing recorded
    /// it before this. `rate(off_cell_writer) / rate(bookmark_waits)` is
    /// "reads are landing on non-owners" as a query rather than an inference;
    /// with `GRAPH_READ_ROUTING=owner` it should be ~0 outside handoffs.
    ///
    /// The two do **not** have to sum to [`Self::bookmark_waits`]: a client
    /// that cannot observe the branch — anything reaching the shard over the
    /// query transport rather than in-process — increments neither, so the gap
    /// is the count of waits nobody could attribute. Guessing would have been
    /// the alternative, and a guess in a metric is worse than a gap.
    pub bookmark_waits_off_cell_writer: u64,
    /// Time statements spent acquiring namespace and node query permits, microseconds.
    pub admission_wait_us: u64,
    /// Time converting result rows to the wire protocol and handing them to the socket, microseconds.
    pub serialize_duration_us: u64,
    /// Rows serialized onto a client protocol.
    pub serialized_rows: u64,
    /// Total microseconds across the four latency histograms below.
    ///
    /// Retained so nothing that read the old sum has to change. It is derived
    /// rather than stored: every execution is recorded into exactly one of the
    /// four histograms, so their sums add up to the previous value exactly.
    pub execution_duration_us: u64,
    /// End-to-end execution of a read, from the authorization check to the
    /// assembled first page — including the server-cursor start.
    ///
    /// One histogram per engine rather than one labelled at export time. The
    /// kill switch means a process can serve both engines in its lifetime, and
    /// a single population relabelled on a flip would hand the whole history
    /// to whichever engine happens to be effective at the next scrape — with
    /// the engine comparison these families exist for as the first casualty.
    /// Each statement lands in the histogram for the engine it was admitted
    /// with, so an export reads the label off the data instead of off the node.
    pub read_latency_legacy: DurationHistogramSnapshot,
    /// [`Self::read_latency_legacy`] for statements the experimental engine ran.
    pub read_latency_experimental: DurationHistogramSnapshot,
    /// End-to-end execution of a mutation, which is a different distribution
    /// entirely: it carries a commit, and it can never be served from a cursor.
    pub write_latency_legacy: DurationHistogramSnapshot,
    /// [`Self::write_latency_legacy`] for statements the experimental engine ran.
    pub write_latency_experimental: DurationHistogramSnapshot,
    /// How long the causal-consistency bookmark wait took, per wait.
    ///
    /// Read-your-writes latency, and the plan's headline ask. It hid inside
    /// `graph_client_prepare_duration` — an average over every prepare — for as
    /// long as it did precisely because that number is a mean over a
    /// bimodal population: a free exit on the owner and a 17-38s manifest
    /// spin everywhere else average to a "prepare" figure that names neither.
    /// A distribution separates them, and the mass above the 2s bucket is
    /// then the same event `bookmark_waits_polled` counts.
    ///
    /// Recorded around the whole wait, including the first `durable_sequence()`
    /// check, so its `count()` and `bookmark_waits` measure the same
    /// population.
    pub bookmark_wait_latency: DurationHistogramSnapshot,
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
        bookmark_waits,
        bookmark_waits_polled,
        bookmark_waits_declined,
        bookmark_waits_on_cell_writer,
        bookmark_waits_off_cell_writer,
        admission_wait_us,
        serialize_duration_us,
        serialized_rows,
        execution_duration_us,
    }
    histograms {
        read_latency_legacy,
        read_latency_experimental,
        write_latency_legacy,
        write_latency_experimental,
        bookmark_wait_latency,
    }
    class_counters {
        queries_failed_by_class,
    }
    failure_counters {
        queries_failed_by_reason_legacy,
        queries_failed_by_reason_experimental,
    }
});

#[derive(Default)]
struct ClientQueryMetrics {
    queries_started: AtomicU64,
    queries_completed: AtomicU64,
    queries_failed: AtomicU64,
    queries_failed_by_class: crate::core::metrics::ErrorClassCounters,
    queries_failed_by_reason_legacy: crate::core::metrics::QueryFailureCounters,
    queries_failed_by_reason_experimental: crate::core::metrics::QueryFailureCounters,
    rows_returned: AtomicU64,
    auth_failures: AtomicU64,
    scope_denials: AtomicU64,
    cancellations: AtomicU64,
    backpressure_waits: AtomicU64,
    prepare_requests: AtomicU64,
    prepare_duration_us: AtomicU64,
    // The bookmark-wait family. Five relaxed counters and one histogram beside
    // the neighbours they are read with, recorded once per `ensure_bookmark`
    // and never on a path that has no bookmark — a request without one does no
    // wait, and counting a zero there would put the fleet's read volume in the
    // denominator of a ratio about read-your-writes.
    bookmark_waits: AtomicU64,
    bookmark_waits_polled: AtomicU64,
    bookmark_waits_declined: AtomicU64,
    bookmark_waits_on_cell_writer: AtomicU64,
    bookmark_waits_off_cell_writer: AtomicU64,
    admission_wait_us: AtomicU64,
    serialize_duration_us: AtomicU64,
    serialized_rows: AtomicU64,
    bookmark_wait_latency: AtomicDurationHistogram,
    // One sum fed two structurally different populations before this. Every
    // execution funnels through `execute_prepared_page_inner`, which already
    // knows the `QueryTransportAction` and threw it away, so a mutation's
    // commit path and a page served out of a warm server cursor landed in one
    // distribution. Splitting on the action costs nothing and is the whole
    // reason a percentile out of these is worth reading.
    // ...and split again by engine, because the kill switch made "the node's
    // engine" and "the engine this statement ran on" two different facts. See
    // `ClientQueryMetricsSnapshot::read_latency_legacy`.
    read_latency_legacy: AtomicDurationHistogram,
    read_latency_experimental: AtomicDurationHistogram,
    write_latency_legacy: AtomicDurationHistogram,
    write_latency_experimental: AtomicDurationHistogram,
}

impl ClientQueryMetrics {
    fn snapshot(&self) -> ClientQueryMetricsSnapshot {
        let read_latency_legacy = self.read_latency_legacy.snapshot();
        let read_latency_experimental = self.read_latency_experimental.snapshot();
        let write_latency_legacy = self.write_latency_legacy.snapshot();
        let write_latency_experimental = self.write_latency_experimental.snapshot();
        ClientQueryMetricsSnapshot {
            queries_started: self.queries_started.load(Ordering::Relaxed),
            queries_completed: self.queries_completed.load(Ordering::Relaxed),
            queries_failed: self.queries_failed.load(Ordering::Relaxed),
            queries_failed_by_class: crate::core::metrics::load_class_counters(
                &self.queries_failed_by_class,
            ),
            queries_failed_by_reason_legacy: self.queries_failed_by_reason_legacy.snapshot(),
            queries_failed_by_reason_experimental: self
                .queries_failed_by_reason_experimental
                .snapshot(),
            rows_returned: self.rows_returned.load(Ordering::Relaxed),
            auth_failures: self.auth_failures.load(Ordering::Relaxed),
            scope_denials: self.scope_denials.load(Ordering::Relaxed),
            cancellations: self.cancellations.load(Ordering::Relaxed),
            backpressure_waits: self.backpressure_waits.load(Ordering::Relaxed),
            prepare_requests: self.prepare_requests.load(Ordering::Relaxed),
            prepare_duration_us: self.prepare_duration_us.load(Ordering::Relaxed),
            bookmark_waits: self.bookmark_waits.load(Ordering::Relaxed),
            bookmark_waits_polled: self.bookmark_waits_polled.load(Ordering::Relaxed),
            bookmark_waits_declined: self.bookmark_waits_declined.load(Ordering::Relaxed),
            bookmark_waits_on_cell_writer: self
                .bookmark_waits_on_cell_writer
                .load(Ordering::Relaxed),
            bookmark_waits_off_cell_writer: self
                .bookmark_waits_off_cell_writer
                .load(Ordering::Relaxed),
            admission_wait_us: self.admission_wait_us.load(Ordering::Relaxed),
            serialize_duration_us: self.serialize_duration_us.load(Ordering::Relaxed),
            serialized_rows: self.serialized_rows.load(Ordering::Relaxed),
            execution_duration_us: [
                &read_latency_legacy,
                &read_latency_experimental,
                &write_latency_legacy,
                &write_latency_experimental,
            ]
            .into_iter()
            .fold(0, |total, histogram| total.saturating_add(histogram.sum_us)),
            read_latency_legacy,
            read_latency_experimental,
            write_latency_legacy,
            write_latency_experimental,
            bookmark_wait_latency: self.bookmark_wait_latency.snapshot(),
        }
    }

    /// Count one finished bookmark wait.
    ///
    /// One call site, in [`ClientQueryService::ensure_bookmark`], so the
    /// denominator and every numerator move together by construction rather
    /// than by two `fetch_add`s staying in step across a future edit — the same
    /// reason [`Self::record_result`] takes the error instead of a bool.
    ///
    /// `wait` is `None` when the client that served the request cannot see
    /// which branch the shard took; that case increments the denominator and
    /// the latency and nothing else, which is the honest shape.
    fn record_bookmark_wait(
        &self,
        elapsed: Duration,
        wait: Option<crate::BookmarkWait>,
        declined: bool,
    ) {
        self.bookmark_waits.fetch_add(1, Ordering::Relaxed);
        self.bookmark_wait_latency.record(elapsed);
        if declined {
            self.bookmark_waits_declined.fetch_add(1, Ordering::Relaxed);
        }
        let Some(wait) = wait else {
            // A wait that *declined* carries no observation — the shard returns
            // `Err(SnapshotAhead)` and the observation does not survive the `?`
            // — but it is still a poll, and by construction rather than by
            // assumption: `wait_for_storage_sequence`'s fast exit returns `Ok`,
            // so the only way to reach `SnapshotAhead` is from inside the loop,
            // after at least one `refresh_durable_reader`. Leaving it out would
            // subtract the worst waits from the numerator of the very ratio
            // they are the point of.
            //
            // The owner/non-owner split is *not* inferred the same way, because
            // there is no theorem to lean on: a caller can present an epoch
            // ahead of every node, and then even the cell's writer polls and
            // declines. So a declined wait counts in neither, and the gap
            // between `on + off` and the total is exactly the waits nobody
            // could attribute.
            if declined {
                self.bookmark_waits_polled.fetch_add(1, Ordering::Relaxed);
            }
            return;
        };
        if wait.entered_poll_loop() {
            self.bookmark_waits_polled.fetch_add(1, Ordering::Relaxed);
        }
        let counter = if wait.served_by_cell_writer {
            &self.bookmark_waits_on_cell_writer
        } else {
            &self.bookmark_waits_off_cell_writer
        };
        counter.fetch_add(1, Ordering::Relaxed);
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
    /// `engine` is the one the statement was **admitted** with, carried on
    /// [`PreparedClientQuery::cypher_engine`], not the node's current engine:
    /// a statement that outlives a kill-switch flip is an observation of the
    /// engine that ran it, and filing it under the new one would corrupt the
    /// comparison in the direction that matters.
    fn record_execution(
        &self,
        action: QueryTransportAction,
        elapsed: Duration,
        engine: CypherEngineMode,
    ) {
        match (action, engine) {
            (QueryTransportAction::Write, CypherEngineMode::Legacy) => {
                self.write_latency_legacy.record(elapsed)
            }
            (QueryTransportAction::Write, CypherEngineMode::Experimental) => {
                self.write_latency_experimental.record(elapsed)
            }
            (
                QueryTransportAction::Read
                | QueryTransportAction::Cancel
                | QueryTransportAction::Admin,
                CypherEngineMode::Legacy,
            ) => self.read_latency_legacy.record(elapsed),
            (
                QueryTransportAction::Read
                | QueryTransportAction::Cancel
                | QueryTransportAction::Admin,
                CypherEngineMode::Experimental,
            ) => self.read_latency_experimental.record(elapsed),
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
    /// Runtime override of `config.cypher_engine`, encoded by
    /// [`encode_engine_override`]. Read once per statement.
    cypher_engine_override: AtomicU8,
}

/// Which Cypher engine a node runs and why, as one consistent reading.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CypherEngineSelection {
    /// `GRAPH_CYPHER_ENGINE` at startup.
    pub configured: CypherEngineMode,
    /// The kill-switch override, if one is set.
    pub override_mode: Option<CypherEngineMode>,
}

impl CypherEngineSelection {
    /// The engine new statements are admitted to.
    pub fn effective(&self) -> CypherEngineMode {
        self.override_mode.unwrap_or(self.configured)
    }
}

const ENGINE_OVERRIDE_NONE: u8 = 0;
const ENGINE_OVERRIDE_LEGACY: u8 = 1;
const ENGINE_OVERRIDE_EXPERIMENTAL: u8 = 2;

fn encode_engine_override(mode: Option<CypherEngineMode>) -> u8 {
    match mode {
        None => ENGINE_OVERRIDE_NONE,
        Some(CypherEngineMode::Legacy) => ENGINE_OVERRIDE_LEGACY,
        Some(CypherEngineMode::Experimental) => ENGINE_OVERRIDE_EXPERIMENTAL,
    }
}

fn decode_engine_override(value: u8) -> Option<CypherEngineMode> {
    match value {
        ENGINE_OVERRIDE_LEGACY => Some(CypherEngineMode::Legacy),
        ENGINE_OVERRIDE_EXPERIMENTAL => Some(CypherEngineMode::Experimental),
        _ => None,
    }
}

struct ServerQueryCursor {
    memory_diagnostic: MemoryDiagnosticGuard,
    diagnostic_request_bytes: u64,
    owner: ClientQueryKey,
    target: ClientQueryTarget,
    query: String,
    parameters: BTreeMap<String, QueryParameterValue>,
    columns: Vec<QueryColumn>,
    rows: VecDeque<BufferedCursorRow>,
    read_epoch: Option<StorageSequence>,
    bookmark: Option<ClientBookmark>,
    expires_at: Instant,
    resident_bytes: u64,
}

// Immutable buffered rows carry the size charged at admission. Pagination must
// not walk their values again while holding the shared cursor registry lock.
struct BufferedCursorRow {
    row: QueryRow,
    resident_bytes: u64,
}

impl BufferedCursorRow {
    fn new(row: QueryRow) -> Self {
        let resident_bytes = estimate_cursor_row(&row)
            .saturating_add((std::mem::size_of::<Self>() - std::mem::size_of::<QueryRow>()) as u64);
        Self {
            row,
            resident_bytes,
        }
    }
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
                cypher_engine_override: AtomicU8::new(ENGINE_OVERRIDE_NONE),
            }),
        })
    }

    pub fn metrics(&self) -> ClientQueryMetricsSnapshot {
        self.inner.metrics.snapshot()
    }

    /// The Cypher engine a statement admitted now would run through: the
    /// runtime override when one is set, otherwise the configured engine.
    ///
    /// Process-wide, so the metric exporters stamp it on the client latency
    /// histograms as a label rather than recording two populations: a legacy
    /// node and an experimental node then sit on one panel without a join on
    /// the instance name. After an override the label moves with it; the
    /// cumulative histogram carries over, so `rate()` over the new label value
    /// counts only statements finished after the flip.
    pub fn cypher_engine(&self) -> CypherEngineMode {
        decode_engine_override(self.inner.cypher_engine_override.load(Ordering::SeqCst))
            .unwrap_or(self.inner.config.cypher_engine)
    }

    /// The engine `GRAPH_CYPHER_ENGINE` configured at startup.
    pub fn configured_cypher_engine(&self) -> CypherEngineMode {
        self.inner.config.cypher_engine
    }

    /// The configured engine and the runtime override, from one load of the
    /// override. Reporting them through separate loads could pair one flip's
    /// override with another's effective engine.
    pub fn cypher_engine_selection(&self) -> CypherEngineSelection {
        CypherEngineSelection {
            configured: self.inner.config.cypher_engine,
            override_mode: decode_engine_override(
                self.inner.cypher_engine_override.load(Ordering::SeqCst),
            ),
        }
    }

    /// Kill switch: route statements admitted from now on through `mode`, or
    /// back to the configured engine when `mode` is `None`. Needs no restart
    /// and does not persist across one.
    ///
    /// Statements already prepared or executing keep the engine they resolved
    /// at admission; see [`PreparedClientQuery::cypher_engine`]. A binary built
    /// without `experimental-cypher-engine` refuses `Experimental` here rather
    /// than accepting it and failing every query afterwards.
    ///
    /// Returns the override this call replaced and the selection it installed.
    /// The selection is built from `mode`, not re-read, so it describes this
    /// call's update even when another flip lands straight after it.
    pub fn set_cypher_engine_override(
        &self,
        mode: Option<CypherEngineMode>,
    ) -> Result<(Option<CypherEngineMode>, CypherEngineSelection)> {
        if mode == Some(CypherEngineMode::Experimental)
            && !cfg!(feature = "experimental-cypher-engine")
        {
            return Err(experimental_engine_not_compiled());
        }
        let previous = decode_engine_override(
            self.inner
                .cypher_engine_override
                .swap(encode_engine_override(mode), Ordering::SeqCst),
        );
        Ok((
            previous,
            CypherEngineSelection {
                configured: self.inner.config.cypher_engine,
                override_mode: mode,
            },
        ))
    }

    /// Account result rows converted to a wire protocol and handed to its
    /// writer. Called by the Bolt and HTTP layers, which own the encoding; the
    /// `query.serialize` span beside each call carries the same numbers for one
    /// request.
    #[cfg_attr(
        not(any(feature = "bolt-server", feature = "http-api")),
        allow(dead_code)
    )]
    pub(crate) fn record_serialization(&self, elapsed: std::time::Duration, rows: usize) {
        let elapsed_us = u64::try_from(elapsed.as_micros()).unwrap_or(u64::MAX);
        self.inner
            .metrics
            .serialize_duration_us
            .fetch_add(elapsed_us, Ordering::Relaxed);
        self.inner
            .metrics
            .serialized_rows
            .fetch_add(rows as u64, Ordering::Relaxed);
        tracing::info_span!(
            "query.serialize",
            hydradb.query.rows_serialized = rows as u64,
            elapsed_us,
        )
        .in_scope(|| {});
    }

    pub fn max_page_size(&self) -> usize {
        self.inner.config.max_page_size
    }

    pub(crate) fn effective_runtime_limit_ms(&self, requested: Option<u64>) -> Result<u64> {
        let limit = self.inner.config.max_query_runtime_ms;
        let requested = requested.unwrap_or(limit);
        if requested == 0 {
            return Err(GraphError::UnsupportedQuery {
                reason: QueryFailureReason::InvalidRequest,
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
    /// read-your-writes latency. It is the first thing to look at for the
    /// BFG-007 class of complaint — "the write succeeded but the read did not
    /// see it" — because it separates "we waited for the epoch" from "we never
    /// waited and read stale".
    ///
    /// # What is measured here, and why it is measured here
    ///
    /// Until change 4 of `docs/plans/2026-08-21-cell-affine-read-routing.md`
    /// this span was the *only* record of the wait, and a span is a sample: the
    /// 17-38s waits in `docs/2026-08-21-read-path-30s-timeout-findings.md` were
    /// found by inferring them out of `graph_client_prepare_duration`, an
    /// average over every prepare. The three instruments below turn that
    /// inference into a measurement — a duration histogram, a count of the
    /// waits that fell through to the poll loop, and a count of the waits that
    /// were served by the cell's own writer — and the point of them is that the
    /// *next* such investigation is a dashboard query rather than a week.
    ///
    /// This is the right layer for all three. The shard knows the two facts and
    /// has no idea which request they belong to; the client knows the request
    /// and could not see the facts. [`crate::BookmarkWait`] is what crosses the
    /// gap.
    ///
    /// The bookmark value itself is never recorded: it is an opaque
    /// caller-held token, and §2 puts it on the never-recorded list. Its
    /// *epoch* is recorded, which is the part that explains anything — and that
    /// rule extends to the log line below, which carries epochs and a cell and
    /// nothing else.
    pub async fn ensure_bookmark(&self, bookmark: &ClientBookmark) -> Result<()> {
        let span = tracing::info_span!(
            "query.bookmark_wait",
            hydradb.scope = %bookmark.target.scope,
            hydradb.cell_id = %bookmark.target.cell_id,
            hydradb.read_epoch = bookmark.epoch,
            observed_epoch = tracing::field::Empty,
            // Unprefixed, like `observed_epoch` beside them, and deliberately:
            // `crates/telemetry`'s `semconv` registry is the vocabulary for keys
            // that *correlate across paths*, and these three describe one wait
            // on one span. Promote them if a second producer ever records them.
            polled = tracing::field::Empty,
            refreshes = tracing::field::Empty,
            on_cell_writer = tracing::field::Empty,
            error.class = tracing::field::Empty,
            hydradb.sampling.tail_keep = tracing::field::Empty,
        );
        let started = std::time::Instant::now();
        // The async block yields the observation alongside the result rather
        // than writing it to a captured slot, so the borrow checker does not
        // have to be argued with about a `&mut` held across an await.
        let (outcome, wait) = async {
            let (current_sequence, wait) = match self
                .inner
                .client
                .wait_for_storage_sequence_observed(
                    &bookmark.target.scope,
                    &bookmark.target.cell_id,
                    bookmark.epoch,
                )
                .await
            {
                Ok(observed) => observed,
                Err(error) => return (Err(error), None),
            };
            if let Some(wait) = wait {
                let span = tracing::Span::current();
                span.record("polled", wait.entered_poll_loop());
                span.record("refreshes", wait.refreshes);
                span.record("on_cell_writer", wait.served_by_cell_writer);
            }
            let Some(current_sequence) = current_sequence else {
                return (
                    Err(GraphError::UnsupportedQuery {
                        reason: QueryFailureReason::InvalidRequest,
                        dialect: "ClientProtocol",
                        feature: "backend cannot prove bookmark durability".to_string(),
                    }),
                    wait,
                );
            };
            tracing::Span::current().record("observed_epoch", current_sequence);
            if current_sequence < bookmark.epoch {
                return (
                    Err(GraphError::SnapshotAhead {
                        cell_id: bookmark.target.cell_id.clone(),
                        read_epoch: bookmark.epoch,
                        current_epoch: current_sequence,
                    }),
                    wait,
                );
            }
            (Ok(()), wait)
        }
        .instrument(span.clone())
        .await;
        let elapsed = started.elapsed();
        // `SnapshotAhead` and nothing else: a store failure mid-wait is a
        // storage-class error, and folding it in here would make the one series
        // that says "this cell could not catch up in time" also say "the object
        // store was unavailable".
        let declined = matches!(&outcome, Err(GraphError::SnapshotAhead { .. }));
        if declined && wait.is_none() {
            // Same theorem as in `record_bookmark_wait`: a decline is a poll.
            // Recorded here rather than inside the block because the shard's
            // `SnapshotAhead` leaves nothing behind to record it from.
            span.record("polled", true);
        }
        self.inner
            .metrics
            .record_bookmark_wait(elapsed, wait, declined);
        if let Some(wait) = wait.filter(|wait| wait.entered_poll_loop()) {
            // INFO, and the level is a judgement rather than a default.
            //
            // Not WARN: with `GRAPH_READ_ROUTING=fleet` — today's default — a
            // read that follows a write is load-balanced across the fleet and
            // two times in three lands off the owner, so WARN would fire on the
            // majority of bookmark-carrying reads and train every operator to
            // filter the exact channel that carries the signal *after* the
            // switch flips. Not DEBUG either: no production deployment collects
            // it, and a handoff window that cannot be explained from the logs a
            // node actually ships is how the last investigation took a week.
            //
            // INFO is affordable because the population is bounded twice over —
            // only reads that carry a bookmark, and of those only the ones whose
            // local reader was genuinely behind — and because change 1 of the
            // plan is expected to drive it to ~0, at which point every line here
            // is a writer handoff worth reading. The always-on signal is
            // `bookmark_waits_polled`; this line is the per-event detail beside
            // it.
            //
            // The string is built inside this branch and nowhere else: the free
            // path — every read once the switch flips — allocates nothing.
            //
            // No `target:` and no `%` sigils, both for the same mechanical
            // reason rather than by preference: `tracing`'s event macros cannot
            // parse a dotted field name after an explicit target, nor a dotted
            // name with a sigil. The default target is the module path, which
            // an `EnvFilter` directive of `hydradb=…` still matches, so nothing
            // is lost.
            let scope = bookmark.target.scope.to_string();
            tracing::info!(
                hydradb.scope = scope.as_str(),
                hydradb.cell_id = bookmark.target.cell_id.as_str(),
                hydradb.read_epoch = bookmark.epoch,
                refreshes = wait.refreshes,
                on_cell_writer = wait.served_by_cell_writer,
                elapsed_ms = elapsed.as_millis() as u64,
                declined,
                "read paid the causal-consistency bookmark wait"
            );
        }
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
                    "hydradb.query.rows_returned",
                    response.result.rows.len() as u64,
                );
                if let Some(read_epoch) = response.read_epoch {
                    span.record("hydradb.read_epoch", read_epoch);
                }
            }
            Err(err) => record_span_error(&span, err),
        }
        result
    }

    /// This entry point has no separate prepare step -- it parses inside
    /// execution -- so every failure it returns counts as `execute`.
    async fn execute_rows_inner(
        &self,
        session: &ClientQuerySession,
        request: ClientQueryRequest,
    ) -> Result<ClientQueryResult> {
        // Resolved here rather than inside the body, so the failure and the
        // execution it belongs to agree on one engine even across a flip.
        let cypher_engine = self.cypher_engine();
        let result = self
            .execute_rows_body(session, request, cypher_engine)
            .await;
        self.record_failure(QueryFailureStage::Execute, &result, cypher_engine);
        result
    }

    async fn execute_rows_body(
        &self,
        session: &ClientQuerySession,
        mut request: ClientQueryRequest,
        cypher_engine: CypherEngineMode,
    ) -> Result<ClientQueryResult> {
        if request.read_epoch.is_some() {
            return Err(GraphError::UnsupportedQuery {
                reason: QueryFailureReason::InvalidRequest,
                dialect: "ClientProtocol",
                feature: "historical graph epochs are not client query snapshots; use a bookmark for causal reads"
                    .to_string(),
            });
        }
        let preparing = MemoryDiagnosticGuard::new(
            MemoryStage::ClientPrepare,
            memory_estimates::request(&request),
        );
        self.validate_request(&request, None)?;
        // `cypher_engine` was resolved once by the caller, before anything
        // read it: a kill-switch flip while this statement is in flight must
        // not change the engine under it, nor under the failure it may leave.
        let runtime_limit_ms = self.normalize_runtime_limit(&mut request)?;
        // The limit belongs on the root, not on admission: the staging report
        // of "29999 ms; limit is 29999 ms" is only legible next to the budget
        // it was measured against.
        tracing::Span::current().record("runtime_limit_ms", runtime_limit_ms);
        let action = self.authorize_query(session, &request, cypher_engine)?;
        let (batch_operation, (scalar_parameters, list_parameters)) =
            self.prepare_transport_parameters(&request)?;
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
        let estimated_bytes = memory_estimates::request(&request)
            .saturating_add(memory_estimates::bound(
                &scalar_parameters,
                &list_parameters,
            ))
            .saturating_add(memory_estimates::batch(&batch_operation));
        let key = client_query_key(session, &request);
        let (generation, cancellation_token) = self.begin_query(key.clone()).await?;
        drop(preparing);
        let result = self
            .run_query_with_timeout(
                &key,
                generation,
                &cancellation_token,
                runtime_limit_ms,
                estimated_bytes,
                async {
                    self.validate_bookmark(&request).await?;
                    self.refresh_strong_read(&request, action, &cancellation_token)
                        .await?;
                    let _context_memory = MemoryDiagnosticGuard::new(
                        MemoryStage::ClientContextParameters,
                        memory_estimates::bound(&scalar_parameters, &list_parameters),
                    );
                    let mut context = query_context(
                        session,
                        &request,
                        scalar_parameters.clone(),
                        list_parameters.clone(),
                        cancellation_token.clone(),
                    )
                    .with_cypher_engine(cypher_engine);
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
                    let _result_memory = MemoryDiagnosticGuard::new(
                        MemoryStage::ClientResultBuffer,
                        result.estimated_resident_bytes(),
                    );
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
                    "hydradb.query.rows_returned",
                    response.page.rows.len() as u64,
                );
                if let Some(read_epoch) = response.read_epoch {
                    span.record("hydradb.read_epoch", read_epoch);
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
            let error = GraphError::UnsupportedQuery {
                reason: QueryFailureReason::InvalidRequest,
                dialect: "ClientProtocol",
                feature: "historical graph epochs are not client query snapshots; use a bookmark for causal reads"
                    .to_string(),
            };
            // Rejected ahead of `prepare_page_request`, so neither of its
            // wrappers sees it; count it where it is raised, against the
            // engine that would have taken it.
            self.record_request_failure(QueryFailureStage::Prepare, &error, self.cypher_engine());
            return Err(error);
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
        // The statement's own engine, from its admission, not the node's
        // current one: a flip between RUN and PULL must not move its failure.
        let cypher_engine = prepared.cypher_engine;
        let result = self
            .execute_prepared_page_body(session, prepared, cursor, page_size)
            .await;
        self.record_failure(QueryFailureStage::Execute, &result, cypher_engine);
        result
    }

    async fn execute_prepared_page_body(
        &self,
        session: &ClientQuerySession,
        prepared: PreparedClientQuery,
        cursor: Option<QueryCursorToken>,
        page_size: usize,
    ) -> Result<ClientQueryPage> {
        let execution_started = std::time::Instant::now();
        let PreparedClientQuery {
            memory_diagnostic,
            request,
            action,
            columns: _,
            scalar_parameters,
            list_parameters,
            batch_operation,
            cypher_engine,
        } = prepared;
        // Grants can change while a Bolt cursor is open. The parsed query and
        // access classification are reusable, but authorization is not.
        self.authorize_scope(session, &request.target.scope, action)?;
        if action == QueryTransportAction::Write && cursor.is_some() {
            return Err(GraphError::UnsupportedQuery {
                reason: QueryFailureReason::InvalidRequest,
                dialect: "ClientProtocol",
                feature: "mutation queries cannot continue from a result cursor".to_string(),
            });
        }
        let runtime_limit_ms = request
            .max_runtime_ms
            .expect("prepared client queries have a normalized runtime limit");
        let estimated_bytes = memory_estimates::request(&request)
            .saturating_add(memory_estimates::bound(
                &scalar_parameters,
                &list_parameters,
            ))
            .saturating_add(memory_estimates::batch(&batch_operation));
        let key = client_query_key(session, &request);
        let (generation, cancellation_token) = self.begin_query(key.clone()).await?;
        drop(memory_diagnostic);
        let result = self
            .run_query_with_timeout(
                &key,
                generation,
                &cancellation_token,
                runtime_limit_ms,
                estimated_bytes,
                async {
                    if let Some(cursor) = cursor {
                        return self
                            .continue_server_cursor(session, &request, cursor, page_size)
                            .await;
                    }
                    let _context_memory = MemoryDiagnosticGuard::new(
                        MemoryStage::ClientContextParameters,
                        memory_estimates::bound(&scalar_parameters, &list_parameters),
                    );
                    let mut context = query_context(
                        session,
                        &request,
                        scalar_parameters.clone(),
                        list_parameters.clone(),
                        cancellation_token.clone(),
                    )
                    .with_cypher_engine(cypher_engine);
                    context.max_runtime_ms = request.max_runtime_ms;
                    context.cancellation_token = Some(cancellation_token.clone());
                    if action == QueryTransportAction::Read {
                        // The client-visible topology watermark is not a
                        // storage snapshot selector. Shard execution creates
                        // one SlateDB DbSnapshot for this complete result.
                        context.read_epoch = None;
                        context.max_result_bytes = Some(self.inner.config.max_cursor_buffer_bytes);
                        self.refresh_strong_read(&request, action, &cancellation_token)
                            .await?;
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
                        let _result_memory = MemoryDiagnosticGuard::new(
                            MemoryStage::ClientResultBuffer,
                            result.estimated_resident_bytes(),
                        );
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
            .record_execution(action, execution_started.elapsed(), cypher_engine);
        result
    }

    /// Both client protocols prepare here, so this is the one place a query
    /// that cannot be parsed, lowered or planned is counted -- on Bolt the
    /// failure goes straight back to the client and never reaches
    /// [`Self::record_result_metrics`].
    pub(crate) async fn prepare_page_request(
        &self,
        session: &ClientQuerySession,
        request: ClientQueryRequest,
        page_size: usize,
    ) -> Result<PreparedClientQuery> {
        // One load, for the statement and for the failure it may leave: the
        // body admits on this engine and a failure inside it is counted
        // against the same one, so a flip between the two cannot file a
        // prepare failure against an engine that never saw the statement.
        let cypher_engine = self.cypher_engine();
        let result = self
            .prepare_page_request_body(session, request, page_size, cypher_engine)
            .await;
        self.record_failure(QueryFailureStage::Prepare, &result, cypher_engine);
        result
    }

    async fn prepare_page_request_body(
        &self,
        session: &ClientQuerySession,
        mut request: ClientQueryRequest,
        page_size: usize,
        cypher_engine: CypherEngineMode,
    ) -> Result<PreparedClientQuery> {
        let _preparing = MemoryDiagnosticGuard::new(
            MemoryStage::ClientPrepare,
            memory_estimates::request(&request),
        );
        let prepare_started = std::time::Instant::now();
        self.inner
            .metrics
            .prepare_requests
            .fetch_add(1, Ordering::Relaxed);
        self.validate_request(&request, Some(page_size))?;
        // `cypher_engine` was resolved once by the caller at RUN and is
        // carried on the prepared statement from here, so every PULL of this
        // statement uses it whatever the kill switch says later.
        let runtime_limit_ms = self.normalize_runtime_limit(&mut request)?;
        tracing::Span::current().record("runtime_limit_ms", runtime_limit_ms);
        let action = self.authorize_query(session, &request, cypher_engine)?;
        self.validate_bookmark(&request).await?;

        let (batch_operation, (scalar_parameters, list_parameters)) =
            self.prepare_transport_parameters(&request)?;

        let columns = if let Some(operation) = &batch_operation {
            batch_operation_columns(operation)
        } else if cypher_engine == CypherEngineMode::Experimental {
            #[cfg(feature = "experimental-cypher-engine")]
            {
                match action {
                    QueryTransportAction::Read => {
                        match parse_native_path_procedure_columns(&request.query)? {
                            Some(columns) => columns,
                            None => experimental_query_columns(
                                &request.query,
                                &scalar_parameters,
                                &list_parameters,
                            )?,
                        }
                    }
                    QueryTransportAction::Write => {
                        let Some(mutation) = parse_opencypher_mutation_query_with_list_parameters(
                            &request.query,
                            &scalar_parameters,
                            &list_parameters,
                        )?
                        else {
                            return Err(GraphError::UnsupportedQuery {
                                reason: QueryFailureReason::Mutation,
                                dialect: "ClientProtocol",
                                feature: "write query is not executable by the mutation engine"
                                    .to_string(),
                            });
                        };
                        // A mutation with a trailing RETURN has to declare its column
                        // before execution: Bolt sends the field names in the RUN
                        // response, ahead of any record.
                        mutation
                            .returning
                            .map(|returning| vec![returning.column])
                            .unwrap_or_default()
                    }
                    QueryTransportAction::Cancel | QueryTransportAction::Admin => {
                        unreachable!("query access classification only returns read or write")
                    }
                }
            }
            #[cfg(not(feature = "experimental-cypher-engine"))]
            {
                return Err(experimental_engine_not_compiled());
            }
        } else {
            match action {
                QueryTransportAction::Read => {
                    match parse_native_path_procedure_columns(&request.query)? {
                        Some(columns) => columns,
                        None => {
                            parse_opencypher_row_query_with_list_parameters(
                                &request.query,
                                &scalar_parameters,
                                &list_parameters,
                            )?
                            .columns
                        }
                    }
                }
                QueryTransportAction::Write => {
                    let Some(mutation) = parse_opencypher_mutation_query_with_list_parameters(
                        &request.query,
                        &scalar_parameters,
                        &list_parameters,
                    )?
                    else {
                        return Err(GraphError::UnsupportedQuery {
                            reason: QueryFailureReason::Mutation,
                            dialect: "ClientProtocol",
                            feature: "write query is not executable by the mutation engine"
                                .to_string(),
                        });
                    };
                    // A mutation with a trailing RETURN has to declare its column
                    // before execution: Bolt sends the field names in the RUN
                    // response, ahead of any record.
                    mutation
                        .returning
                        .map(|returning| vec![returning.column])
                        .unwrap_or_default()
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
            memory_diagnostic: MemoryDiagnosticGuard::new(
                MemoryStage::ClientPrepared,
                memory_estimates::request(&request)
                    .saturating_add(memory_estimates::bound(
                        &scalar_parameters,
                        &list_parameters,
                    ))
                    .saturating_add(memory_estimates::batch(&batch_operation)),
            ),
            request,
            action,
            columns,
            scalar_parameters,
            list_parameters,
            batch_operation,
            cypher_engine,
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
        let QueryResultSet {
            columns,
            rows,
            read_epoch: _,
            storage_sequence: _,
        } = result;
        // Prepare and size the result before taking the registry lock. Rows
        // returned immediately need no retained size-cache allocation.
        let mut rows = rows.into_iter();
        let page_rows: Vec<_> = rows.by_ref().take(page_size).collect();
        let page_bytes = page_rows.iter().fold(0_u64, |total, row| {
            total.saturating_add(estimate_cursor_row(row))
        });
        let remaining: VecDeque<_> = rows.map(BufferedCursorRow::new).collect();
        let resident_bytes = query_rows_resident_bytes(&remaining);
        let materialized_bytes = page_bytes.saturating_add(resident_bytes);
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
        if remaining.is_empty() {
            return Ok(ClientQueryPage {
                query_id: request.query_id.clone(),
                page: QueryResultPage::new(columns, page_rows, None),
                read_epoch,
                bookmark,
            });
        }

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
            memory_diagnostic: MemoryDiagnosticGuard::new(
                MemoryStage::CursorBuffer,
                resident_bytes.saturating_add(memory_estimates::request(request)),
            ),
            diagnostic_request_bytes: memory_estimates::request(request),
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
                reason: QueryFailureReason::InvalidRequest,
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
                reason: QueryFailureReason::InvalidRequest,
                dialect: "ClientProtocol",
                feature: "result cursor does not belong to this query request".to_string(),
            });
        }

        let mut cursor = cursors
            .remove(&token.offset)
            .expect("cursor was checked while holding the cursor lock");
        let (page_rows, released_bytes) = take_cursor_rows(&mut cursor.rows, page_size);
        cursor.resident_bytes = cursor.resident_bytes.saturating_sub(released_bytes);
        cursor.memory_diagnostic.set_bytes(
            cursor
                .resident_bytes
                .saturating_add(cursor.diagnostic_request_bytes),
        );
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
                reason: QueryFailureReason::InvalidRequest,
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
                    reason: QueryFailureReason::InvalidRequest,
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
        cypher_engine: CypherEngineMode,
    ) -> Result<QueryTransportAction> {
        let action = if cypher_engine == CypherEngineMode::Experimental {
            #[cfg(feature = "experimental-cypher-engine")]
            {
                match classify_opencypher_query_access(&request.query)? {
                    OpenCypherQueryAccess::Read => {
                        // A read UNWIND batch is not this engine's to plan:
                        // `prepare_transport_parameters` hands it to the batch
                        // path a step later, as it does on the legacy engine.
                        // It is refused here in two ways -- rows the sidecar
                        // split will not take, and an empty list that splits
                        // cleanly but leaves the planner an UNWIND it cannot
                        // parse -- so both refusals ask the same question.
                        // Asking only on a refusal keeps the batch parse off
                        // every other read, which pays for it in prepare.
                        let refusal_stands = |error: GraphError| -> Result<()> {
                            match parse_opencypher_unwind_batch(&request.query)? {
                                Some(_) => Ok(()),
                                None => Err(error),
                            }
                        };
                        match split_query_parameters(&request.parameters) {
                            Ok((parameters, lists)) => {
                                if parse_native_path_procedure_columns(&request.query)?.is_none() {
                                    if let Err(error) = prepare_experimental_cypher(
                                        &request.query,
                                        &parameters,
                                        &lists,
                                    ) {
                                        refusal_stands(error)?;
                                    }
                                }
                            }
                            Err(error) => refusal_stands(error)?,
                        }
                        QueryTransportAction::Read
                    }
                    OpenCypherQueryAccess::Write => QueryTransportAction::Write,
                }
            }
            #[cfg(not(feature = "experimental-cypher-engine"))]
            {
                return Err(experimental_engine_not_compiled());
            }
        } else {
            match classify_opencypher_query_access(&request.query)? {
                OpenCypherQueryAccess::Read => QueryTransportAction::Read,
                OpenCypherQueryAccess::Write => QueryTransportAction::Write,
            }
        };
        self.authorize_scope(session, &request.target.scope, action)?;
        Ok(action)
    }

    fn prepare_transport_parameters(
        &self,
        request: &ClientQueryRequest,
    ) -> Result<(Option<QueryBatchOperation>, BoundQueryParameters)> {
        match parse_opencypher_unwind_batch(&request.query)? {
            // An UNWIND batch consumes its list itself, so nothing reaches the
            // sidecar on that path.
            Some(parsed) => Ok((
                Some(resolve_unwind_batch(parsed, &request.parameters)?),
                (BTreeMap::new(), BTreeMap::new()),
            )),
            None => {
                let (scalars, lists) = split_query_parameters(&request.parameters)?;
                Ok((None, (scalars, lists)))
            }
        }
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
        cancellation_token: &QueryCancellationToken,
    ) -> Result<()> {
        if request.consistency != ClientReadConsistency::Strong {
            return Ok(());
        }
        if action != QueryTransportAction::Read {
            return Err(GraphError::UnsupportedQuery {
                reason: QueryFailureReason::InvalidRequest,
                dialect: "ClientProtocol",
                feature: "strong consistency applies only to read queries".to_string(),
            });
        }
        let span = tracing::info_span!(
            "query.strong_refresh",
            hydradb.scope = %request.target.scope,
            hydradb.cell_id = %request.target.cell_id,
            error.class = tracing::field::Empty,
        );
        let refresh = self
            .inner
            .client
            .refresh_storage_sequence(&request.target.scope, &request.target.cell_id)
            .instrument(span.clone());
        tokio::pin!(refresh);
        let sequence = tokio::select! {
            result = &mut refresh => result.inspect_err(|error| record_span_error(&span, error))?,
            _ = cancellation_token.cancelled() => return Err(client_query_cancelled()),
        };
        sequence.ok_or_else(|| GraphError::UnsupportedQuery {
            reason: QueryFailureReason::InvalidRequest,
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
            // epoch to, so `hydradb.commit_epoch` recorded here is the value
            // `query.bookmark_wait` is waiting to catch up with, on a different
            // trace and possibly a different node.
            let span = tracing::info_span!(
                "write.bookmark",
                hydradb.scope = %request.target.scope,
                hydradb.cell_id = %request.target.cell_id,
                hydradb.commit_epoch = tracing::field::Empty,
                error.class = tracing::field::Empty,
                hydradb.sampling.tail_keep = tracing::field::Empty,
            );
            let sequence = self
                .inner
                .client
                .current_storage_sequence(&request.target.scope, &request.target.cell_id)
                .instrument(span.clone())
                .await
                .inspect_err(|err| record_span_error(&span, err))?;
            if let Some(sequence) = sequence {
                span.record("hydradb.commit_epoch", sequence);
            }
            sequence
        };
        Ok(sequence.map(|sequence| ClientBookmark::new(request.target.clone(), sequence)))
    }

    async fn begin_query(&self, key: ClientQueryKey) -> Result<(u64, QueryCancellationToken)> {
        let mut active_queries = self.inner.active_queries.lock().await;
        if active_queries.contains_key(&key) {
            return Err(GraphError::UnsupportedQuery {
                reason: QueryFailureReason::InvalidRequest,
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
                hydradb.scope = %key.scope,
                error.class = tracing::field::Empty,
                error.operation = tracing::field::Empty,
            );
            let admission_started = std::time::Instant::now();
            let permits = async {
                let namespace_permits = self
                    .acquire_namespace_permits(&key.scope.namespace, cancellation_token)
                    .await?;
                let query_permit = self.acquire_query_permit(cancellation_token).await?;
                Ok((namespace_permits, query_permit))
            }
            .instrument(admission.clone())
            .await;
            self.inner.metrics.admission_wait_us.fetch_add(
                u64::try_from(admission_started.elapsed().as_micros()).unwrap_or(u64::MAX),
                Ordering::Relaxed,
            );
            let (_namespace_permits, _query_permit) = match permits {
                Ok(permits) => permits,
                Err(err) => {
                    record_span_error(&admission, &err);
                    return Err(err);
                }
            };
            let _execution =
                MemoryDiagnosticGuard::new(MemoryStage::ClientExecute, request_estimated_bytes());
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
        estimated_bytes: u64,
        execute: F,
    ) -> Result<T>
    where
        F: Future<Output = Result<T>>,
    {
        let query = cancellation_token.scope(REQUEST_ESTIMATED_BYTES.scope(
            estimated_bytes,
            self.run_query(key, generation, cancellation_token, execute),
        ));
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
                let _waiting = MemoryDiagnosticGuard::new(
                    MemoryStage::ClientQueryWait,
                    request_estimated_bytes(),
                );
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
                    let _waiting = MemoryDiagnosticGuard::new(
                        MemoryStage::ClientNamespaceWait,
                        request_estimated_bytes(),
                    );
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
    fn record_failure<T>(
        &self,
        stage: QueryFailureStage,
        result: &Result<T>,
        engine: CypherEngineMode,
    ) {
        if let Err(error) = result {
            self.record_request_failure(stage, error, engine);
        }
    }

    /// Count a request a protocol adapter rejected before handing it to this
    /// service, for the checks that are a property of the query request
    /// rather than of the wire format. Framing, decoding and authentication
    /// failures are not query failures and stay out of this family;
    /// authentication has `auth_failures`.
    /// `engine` is the statement's admitted engine where there is one, and
    /// otherwise the engine effective at the moment of the rejection. Either
    /// way it is fixed here, at the point of recording, so a later kill-switch
    /// flip cannot move a failure between engines.
    pub(crate) fn record_request_failure(
        &self,
        stage: QueryFailureStage,
        error: &GraphError,
        engine: CypherEngineMode,
    ) {
        match engine {
            CypherEngineMode::Legacy => &self.inner.metrics.queries_failed_by_reason_legacy,
            CypherEngineMode::Experimental => {
                &self.inner.metrics.queries_failed_by_reason_experimental
            }
        }
        .record(stage, error);
    }

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
    if let Some(operation) = err.limit_operation() {
        span.record("error.operation", operation);
    }
    // The head sampler decided this trace's fate when the root span started,
    // which was before this request could possibly be known to fail, so nothing
    // recorded here can change it. The marker is for the collector's tail
    // sampler, which buffers the whole trace and *can* — see
    // `hydradb_telemetry::sampling` for the policy that deployment owes us.
    span.record("hydradb.sampling.tail_keep", "error");
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
                hydradb.scope = %request.target.scope,
                hydradb.cell_id = %request.target.cell_id,
                hydradb.tenant_id = tracing::field::Empty,
                hydradb.tenant.scope_id = tracing::field::Empty,
                hydradb.sub_tenant_id = tracing::field::Empty,
                hydradb.sub_tenant.scope_id = tracing::field::Empty,
                hydradb.query.fingerprint = %fingerprint,
                hydradb.consistency = ?request.consistency,
                hydradb.correlation_id = tracing::field::Empty,
                hydradb.caller.step = tracing::field::Empty,
                hydradb.read_epoch = tracing::field::Empty,
                hydradb.query.rows_returned = tracing::field::Empty,
                runtime_limit_ms = tracing::field::Empty,
                error.class = tracing::field::Empty,
                error.operation = tracing::field::Empty,
                hydradb.sampling.tail_keep = tracing::field::Empty,
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
    // knowing there is a customer. `hydradb.scope` above already contains both
    // segments, but only as one opaque path a warehouse cannot split.
    record_scope_tenancy(&span, &request.target.scope);
    // Absent rather than empty: a blank correlation id is indistinguishable
    // from one that failed validation, and both would join to nothing.
    if let Some(correlation_id) = &request.correlation_id {
        span.record("hydradb.correlation_id", correlation_id.as_str());
    }
    if let Some(caller_step) = &request.caller_step {
        span.record("hydradb.caller.step", caller_step.as_str());
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

fn take_cursor_rows(
    rows: &mut VecDeque<BufferedCursorRow>,
    page_size: usize,
) -> (Vec<QueryRow>, u64) {
    let count = page_size.min(rows.len());
    let mut released_bytes = 0_u64;
    let page = rows
        .drain(..count)
        .map(|buffered| {
            released_bytes = released_bytes.saturating_add(buffered.resident_bytes);
            buffered.row
        })
        .collect();
    (page, released_bytes)
}

fn query_rows_resident_bytes(rows: &VecDeque<BufferedCursorRow>) -> u64 {
    rows.iter()
        .fold(0_u64, |total, row| total.saturating_add(row.resident_bytes))
}

#[cfg(test)]
thread_local! {
    static CURSOR_ROW_ESTIMATES: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

fn estimate_cursor_row(row: &QueryRow) -> u64 {
    #[cfg(test)]
    CURSOR_ROW_ESTIMATES.with(|count| count.set(count.get() + 1));
    row.estimated_resident_bytes()
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

fn experimental_engine_not_compiled() -> GraphError {
    GraphError::UnsupportedQuery {
        reason: QueryFailureReason::Other,
        dialect: "Cypher25",
        feature: "the experimental-cypher-engine Cargo feature is not enabled".to_string(),
    }
}

fn query_context(
    session: &ClientQuerySession,
    request: &ClientQueryRequest,
    parameters: BTreeMap<String, VertexPropertyValue>,
    list_parameters: BTreeMap<String, Vec<VertexPropertyValue>>,
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
        .with_list_parameters(list_parameters)
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

/// Split bound parameters into the scalar map every engine has always taken and
/// the list sidecar that `IN` reads.
///
/// A list of scalars is no longer refused. Refusing it was what made
/// `WHERE x IN $ids` unusable from a client, whichever engine was selected: the
/// rejection happened here, before the query text was looked at. A list holding
/// anything but scalars, and a map, are still refused with the message they
/// always had, because those remain UNWIND-only inputs.
fn split_query_parameters(
    parameters: &BTreeMap<String, QueryParameterValue>,
) -> Result<BoundQueryParameters> {
    let composite = |name: &str| GraphError::UnsupportedQuery {
        reason: QueryFailureReason::Parameter,
        dialect: "ClientProtocol",
        feature: format!("composite parameter ${name} is only supported as an UNWIND input"),
    };
    let mut scalars = BTreeMap::new();
    let mut lists = BTreeMap::new();
    for (name, value) in parameters {
        match value {
            QueryParameterValue::Scalar(value) => {
                scalars.insert(name.clone(), value.clone());
            }
            QueryParameterValue::List(values) => {
                let mut scalar_values = Vec::with_capacity(values.len());
                for value in values {
                    let QueryParameterValue::Scalar(value) = value else {
                        // A list of rows is an UNWIND input, not an `IN` list.
                        return Err(composite(name));
                    };
                    scalar_values.push(value.clone());
                }
                lists.insert(name.clone(), scalar_values);
            }
            QueryParameterValue::Map(_) => return Err(composite(name)),
        }
    }
    Ok((scalars, lists))
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
            reason: QueryFailureReason::Unwind,
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
        ParsedUnwindBatchKind::DeleteIsolatedVertices {
            vertex_field,
            path_node_constraints,
            deleted_column,
        } => {
            let candidates = unwind_isolated_candidates(
                rows,
                &vertex_field,
                &path_node_constraints,
                parameters,
            )?;
            Ok(QueryBatchOperation::DeleteIsolatedVertices {
                candidates,
                deleted_column,
            })
        }
        ParsedUnwindBatchKind::DeleteVerticesAndIsolatedCandidates {
            detach_vertex_field,
            isolated_parameter,
            isolated_vertex_field,
            isolated_path_node_constraints,
            deleted_column,
        } => {
            let mut detach_vertices = rows
                .iter()
                .enumerate()
                .map(|(index, row)| unwind_row_vertex_id(row, index, &detach_vertex_field))
                .collect::<Result<Vec<_>>>()?;
            let isolated_value = parameters
                .get(&isolated_parameter)
                .or_else(|| parameters.get(&format!("${isolated_parameter}")))
                .ok_or_else(|| GraphError::MissingQueryParameter {
                    dialect: "OpenCypher",
                    name: isolated_parameter.clone(),
                })?;
            let QueryParameterValue::List(isolated_rows) = isolated_value else {
                return Err(GraphError::UnsupportedQuery {
                    reason: QueryFailureReason::Unwind,
                    dialect: "OpenCypher",
                    feature: format!("UNWIND parameter ${isolated_parameter} must be a list"),
                });
            };
            let mut isolated_candidates = unwind_isolated_candidates(
                isolated_rows,
                &isolated_vertex_field,
                &isolated_path_node_constraints,
                parameters,
            )?;
            detach_vertices.sort_unstable();
            detach_vertices.dedup();
            let detach_set = detach_vertices
                .iter()
                .copied()
                .collect::<std::collections::BTreeSet<_>>();
            isolated_candidates.retain(|candidate| !detach_set.contains(&candidate.vertex));
            Ok(QueryBatchOperation::DeleteVerticesAndIsolatedCandidates {
                detach_vertices,
                isolated_candidates,
                deleted_column,
            })
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
        } => {
            let vertices = rows
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
                .collect::<Result<Vec<_>>>()?;
            Ok(match update_if_newer_by {
                Some(update_if_newer_by) => QueryBatchOperation::GuardedUpsertVertices {
                    vertices,
                    merge_policy: QueryBatchMergePolicy {
                        update_if_newer_by,
                        create_only_properties,
                    },
                },
                None => QueryBatchOperation::UpsertVertices { vertices },
            })
        }
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
        } => {
            let relationships = rows
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
                        relationship_id: unwind_row_vertex_id(row, index, &relationship_id_field)?,
                        metadata,
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(match update_if_newer_by {
                Some(update_if_newer_by) => {
                    QueryBatchOperation::GuardedMergeRelationshipsBetweenLabeledVertices {
                        edge_type,
                        relationships,
                        source_label,
                        destination_label,
                        merge_policy: QueryBatchMergePolicy {
                            update_if_newer_by,
                            create_only_properties,
                        },
                    }
                }
                None => QueryBatchOperation::MergeRelationshipsBetweenLabeledVertices {
                    edge_type,
                    relationships,
                    source_label,
                    destination_label,
                },
            })
        }
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
            reason: QueryFailureReason::Unwind,
            dialect: "OpenCypher",
            feature: format!("UNWIND row {index} must be a map"),
        });
    };
    let value = row.get(field).ok_or_else(|| GraphError::UnsupportedQuery {
        reason: QueryFailureReason::Unwind,
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
            reason: QueryFailureReason::Unwind,
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
            reason: QueryFailureReason::Unwind,
            dialect: "OpenCypher",
            feature: format!("UNWIND row {index} must be a map"),
        });
    };
    match row.get(field) {
        Some(QueryParameterValue::Scalar(value)) => Ok(value.clone()),
        Some(QueryParameterValue::List(_) | QueryParameterValue::Map(_)) => {
            Err(GraphError::UnsupportedQuery {
                reason: QueryFailureReason::Unwind,
                dialect: "OpenCypher",
                feature: format!("UNWIND row {index} field {field} must be scalar"),
            })
        }
        None => Err(GraphError::UnsupportedQuery {
            reason: QueryFailureReason::Unwind,
            dialect: "OpenCypher",
            feature: format!("UNWIND row {index} is missing field {field}"),
        }),
    }
}

fn unwind_isolated_candidates(
    rows: &[QueryParameterValue],
    vertex_field: &str,
    constraints: &ParsedUnwindVertexConstraints,
    parameters: &BTreeMap<String, QueryParameterValue>,
) -> Result<Vec<QueryBatchIsolatedVertex>> {
    let mut candidates = BTreeMap::<crate::VertexId, VertexMetadata>::new();
    for (index, row) in rows.iter().enumerate() {
        let vertex = unwind_row_vertex_id(row, index, vertex_field)?;
        let mut metadata = VertexMetadata {
            labels: constraints.labels.clone(),
            properties: BTreeMap::new(),
        };
        for (property, value) in &constraints.properties {
            let value = match value {
                ParsedUnwindConstraintValue::Literal(value) => value.clone(),
                ParsedUnwindConstraintValue::Parameter(name) => {
                    match parameters
                        .get(name)
                        .or_else(|| parameters.get(&format!("${name}")))
                    {
                        Some(QueryParameterValue::Scalar(value)) => value.clone(),
                        Some(QueryParameterValue::List(_) | QueryParameterValue::Map(_)) => {
                            return Err(GraphError::UnsupportedQuery {
                                reason: QueryFailureReason::Unwind,
                                dialect: "OpenCypher",
                                feature: format!(
                                    "isolated vertex constraint parameter ${name} must be scalar"
                                ),
                            });
                        }
                        None => {
                            return Err(GraphError::MissingQueryParameter {
                                dialect: "OpenCypher",
                                name: name.clone(),
                            });
                        }
                    }
                }
                ParsedUnwindConstraintValue::RowField(field) => {
                    unwind_row_scalar(row, index, field)?
                }
            };
            metadata.properties.insert(property.clone(), value);
        }
        if let Some(previous) = candidates.insert(vertex, metadata.clone()) {
            if previous != metadata {
                return Err(GraphError::UnsupportedQuery {
                    reason: QueryFailureReason::Unwind,
                    dialect: "OpenCypher",
                    feature: format!(
                        "UNWIND rows for vertex {vertex} use conflicting isolation constraints"
                    ),
                });
            }
        }
    }
    Ok(candidates
        .into_iter()
        .map(|(vertex, path_node_constraints)| QueryBatchIsolatedVertex {
            vertex,
            path_node_constraints,
        })
        .collect())
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
        | QueryBatchOperation::GuardedUpsertVertices { .. }
        | QueryBatchOperation::CreateRelationshipsBetweenLabeledVertices { .. }
        | QueryBatchOperation::MergeRelationshipsBetweenLabeledVertices { .. }
        | QueryBatchOperation::GuardedMergeRelationshipsBetweenLabeledVertices { .. } => Vec::new(),
        QueryBatchOperation::DeleteIsolatedVertices { deleted_column, .. } => {
            vec![deleted_column.clone()]
        }
        QueryBatchOperation::DeleteVerticesAndIsolatedCandidates { deleted_column, .. } => {
            vec![deleted_column.clone()]
        }
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
        reason: QueryFailureReason::InvalidRequest,
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
