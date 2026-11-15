use std::collections::BTreeMap;

use crate::QueryFailureReason;
use crate::{
    validate_component, CommitResult, EdgeMetadata, GraphError, GraphScope, QueryFloat, RelationshipId, Result, StorageSequence, VertexId,
    VertexMetadata, VertexPropertyValue,
};

#[cfg_attr(feature = "query-transport", derive(serde::Deserialize, serde::Serialize))]
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct QueryPathNode {
    pub id: VertexId,
    pub labels: Vec<String>,
    pub properties: BTreeMap<String, VertexPropertyValue>,
}

#[cfg_attr(feature = "query-transport", derive(serde::Deserialize, serde::Serialize))]
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct QueryPathRelationship {
    pub id: Option<RelationshipId>,
    pub edge_type: String,
    pub src: VertexId,
    pub dst: VertexId,
    pub properties: BTreeMap<String, VertexPropertyValue>,
}

#[cfg_attr(feature = "query-transport", derive(serde::Deserialize, serde::Serialize))]
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct QueryPath {
    pub nodes: Vec<QueryPathNode>,
    pub relationships: Vec<QueryPathRelationship>,
}

#[cfg_attr(feature = "query-transport", derive(serde::Deserialize, serde::Serialize))]
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum QueryParameterValue {
    Scalar(VertexPropertyValue),
    List(Vec<QueryParameterValue>),
    Map(BTreeMap<String, QueryParameterValue>),
}

impl From<VertexPropertyValue> for QueryParameterValue {
    fn from(value: VertexPropertyValue) -> Self {
        Self::Scalar(value)
    }
}

#[cfg_attr(feature = "query-transport", derive(serde::Deserialize, serde::Serialize))]
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct QueryBatchEdge {
    pub src: VertexId,
    pub dst: VertexId,
}

#[cfg_attr(feature = "query-transport", derive(serde::Deserialize, serde::Serialize))]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryBatchVertex {
    pub vertex: VertexId,
    pub metadata: VertexMetadata,
}

#[cfg_attr(feature = "query-transport", derive(serde::Deserialize, serde::Serialize))]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryBatchIsolatedVertex {
    pub vertex: VertexId,
    pub path_node_constraints: VertexMetadata,
}

#[cfg_attr(feature = "query-transport", derive(serde::Deserialize, serde::Serialize))]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryBatchRelationship {
    pub src: VertexId,
    pub dst: VertexId,
    pub metadata: EdgeMetadata,
}

#[cfg_attr(feature = "query-transport", derive(serde::Deserialize, serde::Serialize))]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryBatchRelationshipMerge {
    pub src: VertexId,
    pub dst: VertexId,
    pub relationship_id: RelationshipId,
    pub metadata: EdgeMetadata,
}

/// How an upsert row lands on a vertex or relationship that already exists:
/// the `MERGE … ON CREATE SET … ON MATCH SET …` of the writer.
///
/// With `update_if_newer_by` a row lands only when its guard value is newer
/// than the stored one; with `update_if_not_older_by`, also when it is equal
/// (FalkorDB's `CASE WHEN stored <= incoming`). At most one is set. Without
/// either every row lands. Either way a row that lands on an existing record
/// leaves its create-only properties as they are: they are written when the
/// record is created.
///
/// The not-older guard is a field of its own, not a flag beside
/// `update_if_newer_by`: a peer from before it requires `update_if_newer_by`,
/// so it refuses such a policy rather than applying it as a strict guard.
#[cfg_attr(feature = "query-transport", derive(serde::Deserialize, serde::Serialize))]
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct QueryBatchMergePolicy {
    #[cfg_attr(feature = "query-transport", serde(default, skip_serializing_if = "Option::is_none"))]
    pub update_if_newer_by: Option<String>,
    #[cfg_attr(feature = "query-transport", serde(default, skip_serializing_if = "Option::is_none"))]
    pub update_if_not_older_by: Option<String>,
    pub create_only_properties: std::collections::BTreeSet<String>,
}

impl QueryBatchMergePolicy {
    /// The guard property, whichever rule it has.
    pub fn guard(&self) -> Option<&str> {
        self.update_if_newer_by.as_deref().or(self.update_if_not_older_by.as_deref())
    }

    /// Whether a row whose guard value equals the stored one lands.
    pub fn lands_on_equal(&self) -> bool {
        self.update_if_not_older_by.is_some()
    }

    pub fn validate(&self) -> Result<()> {
        let unsupported = |feature: String| GraphError::UnsupportedQuery {
            reason: QueryFailureReason::Unwind,
            dialect: "QueryBatch",
            feature,
        };
        for property in &self.create_only_properties {
            validate_component("property", property)?;
        }
        if self.update_if_newer_by.is_some() && self.update_if_not_older_by.is_some() {
            return Err(unsupported("a merge policy takes one update guard".into()));
        }
        match self.guard() {
            Some(guard) => {
                validate_component("property", guard)?;
                if self.create_only_properties.contains(guard) {
                    return Err(unsupported(format!("update guard {guard} cannot also be create-only")));
                }
            }
            None if self.create_only_properties.is_empty() => {
                return Err(unsupported("a merge policy needs an update guard or a create-only property".into()))
            }
            None => {}
        }
        Ok(())
    }
}

#[cfg_attr(feature = "query-transport", derive(serde::Deserialize, serde::Serialize))]
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum QueryBatchOperation {
    /// `UNWIND $rows AS <alias> MATCH ... [WHERE ...] (SET | REMOVE)+`, run on
    /// the writer as one statement: `query` is lowered once per row with
    /// `<alias>.<field>` bound to that row, and every row's update commits
    /// together. `parameters` and `list_parameters` are the request's other
    /// parameters; both forms are retained so `IN` keeps collection semantics
    /// while value expressions can bind a homogeneous list as one property.
    MatchMutation {
        query: String,
        alias: String,
        rows: Vec<BTreeMap<String, VertexPropertyValue>>,
        parameters: BTreeMap<String, VertexPropertyValue>,
        #[cfg_attr(feature = "query-transport", serde(default))]
        list_parameters: BTreeMap<String, Vec<VertexPropertyValue>>,
    },
    OutNeighbors {
        edge_type: String,
        sources: Vec<VertexId>,
        source_column: QueryColumn,
        destination_column: QueryColumn,
    },
    CreateEdges {
        edge_type: String,
        edges: Vec<QueryBatchEdge>,
    },
    DeleteEdges {
        edge_type: String,
        edges: Vec<QueryBatchEdge>,
    },
    DeleteVertices {
        vertices: Vec<VertexId>,
        detach: bool,
    },
    DeleteIsolatedVertices {
        candidates: Vec<QueryBatchIsolatedVertex>,
        deleted_column: QueryColumn,
    },
    DeleteVerticesAndIsolatedCandidates {
        detach_vertices: Vec<VertexId>,
        isolated_candidates: Vec<QueryBatchIsolatedVertex>,
        deleted_column: QueryColumn,
    },
    DeleteRelationshipsByProperty {
        edge_type: String,
        property: String,
        values: Vec<VertexPropertyValue>,
    },
    CreateEdgesBetweenLabeledVertices {
        edge_type: String,
        edges: Vec<QueryBatchEdge>,
        source_label: String,
        destination_label: String,
    },
    UpsertVertices {
        vertices: Vec<QueryBatchVertex>,
    },
    GuardedUpsertVertices {
        vertices: Vec<QueryBatchVertex>,
        merge_policy: QueryBatchMergePolicy,
    },
    CreateRelationshipsBetweenLabeledVertices {
        edge_type: String,
        relationships: Vec<QueryBatchRelationship>,
        source_label: String,
        destination_label: String,
    },
    MergeRelationshipsBetweenLabeledVertices {
        edge_type: String,
        relationships: Vec<QueryBatchRelationshipMerge>,
        source_label: String,
        destination_label: String,
    },
    GuardedMergeRelationshipsBetweenLabeledVertices {
        edge_type: String,
        relationships: Vec<QueryBatchRelationshipMerge>,
        source_label: String,
        destination_label: String,
        merge_policy: QueryBatchMergePolicy,
    },
}

impl QueryBatchOperation {
    pub fn is_write(&self) -> bool {
        matches!(
            self,
            Self::MatchMutation { .. }
                | Self::CreateEdges { .. }
                | Self::CreateEdgesBetweenLabeledVertices { .. }
                | Self::DeleteEdges { .. }
                | Self::DeleteVertices { .. }
                | Self::DeleteIsolatedVertices { .. }
                | Self::DeleteVerticesAndIsolatedCandidates { .. }
                | Self::DeleteRelationshipsByProperty { .. }
                | Self::UpsertVertices { .. }
                | Self::GuardedUpsertVertices { .. }
                | Self::CreateRelationshipsBetweenLabeledVertices { .. }
                | Self::MergeRelationshipsBetweenLabeledVertices { .. }
                | Self::GuardedMergeRelationshipsBetweenLabeledVertices { .. }
        )
    }

    pub fn len(&self) -> usize {
        match self {
            Self::MatchMutation { rows, .. } => rows.len(),
            Self::OutNeighbors { sources, .. } => sources.len(),
            Self::CreateEdges { edges, .. } | Self::CreateEdgesBetweenLabeledVertices { edges, .. } | Self::DeleteEdges { edges, .. } => {
                edges.len()
            }
            Self::DeleteVertices { vertices, .. } => vertices.len(),
            Self::DeleteIsolatedVertices { candidates, .. } => candidates.len(),
            Self::DeleteVerticesAndIsolatedCandidates {
                detach_vertices,
                isolated_candidates,
                ..
            } => detach_vertices.len().saturating_add(isolated_candidates.len()),
            Self::DeleteRelationshipsByProperty { values, .. } => values.len(),
            Self::UpsertVertices { vertices } | Self::GuardedUpsertVertices { vertices, .. } => vertices.len(),
            Self::CreateRelationshipsBetweenLabeledVertices { relationships, .. } => relationships.len(),
            Self::MergeRelationshipsBetweenLabeledVertices { relationships, .. }
            | Self::GuardedMergeRelationshipsBetweenLabeledVertices { relationships, .. } => relationships.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[cfg_attr(feature = "query-transport", derive(serde::Deserialize, serde::Serialize))]
#[cfg_attr(feature = "query-transport", serde(rename_all = "snake_case"))]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum CypherEngineMode {
    #[default]
    Legacy,
    Experimental,
    #[cfg(feature = "datafusion-cypher-engine")]
    #[cfg_attr(feature = "query-transport", serde(rename = "datafusion"))]
    DataFusion,
}

impl CypherEngineMode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Legacy => "legacy",
            Self::Experimental => "experimental",
            #[cfg(feature = "datafusion-cypher-engine")]
            Self::DataFusion => "datafusion",
        }
    }

    #[cfg(feature = "query-transport")]
    fn is_legacy(&self) -> bool {
        *self == Self::Legacy
    }
}

/// The mutation commit semantics selected before a statement executes.
///
/// `LegacyChunked` is the compatibility default for existing deployments.
#[cfg_attr(feature = "query-transport", derive(serde::Deserialize, serde::Serialize))]
#[cfg_attr(feature = "query-transport", serde(rename_all = "snake_case"))]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum MutationAtomicityPolicy {
    /// Keep the historical action/chunk commit boundaries during migration.
    #[default]
    LegacyChunked,
    /// Commit all effects of one Cypher statement together or abort them all.
    Statement,
}

impl MutationAtomicityPolicy {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Statement => "statement",
            Self::LegacyChunked => "legacy_chunked",
        }
    }

    #[cfg(feature = "query-transport")]
    fn is_legacy_chunked(&self) -> bool {
        *self == Self::LegacyChunked
    }
}

#[cfg_attr(feature = "query-transport", derive(serde::Deserialize, serde::Serialize))]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryContext {
    pub scope: GraphScope,
    pub cell_id: String,
    pub idempotency_key: String,
    /// Authenticated principal used to scope provider-side mutation receipts.
    /// Older serialized contexts default to the anonymous compatibility
    /// identity; client services set this from their authenticated session.
    #[cfg_attr(feature = "query-transport", serde(default, skip_serializing_if = "String::is_empty"))]
    pub idempotency_principal: String,
    pub read_epoch: Option<StorageSequence>,
    pub result_window: QueryWindow,
    pub parameters: BTreeMap<String, VertexPropertyValue>,
    /// List-valued parameters, kept beside `parameters` rather than in it:
    /// `VertexPropertyValue` is the stored-property type and must not grow a
    /// list variant. Only `IN` reads these. `serde(default)` is load-bearing
    /// during a rolling deploy, where a context serialized by an older node
    /// still has to decode here.
    #[cfg_attr(feature = "query-transport", serde(default))]
    pub list_parameters: BTreeMap<String, Vec<VertexPropertyValue>>,
    pub max_runtime_ms: Option<u64>,
    pub max_result_bytes: Option<u64>,
    #[cfg_attr(feature = "query-transport", serde(default, skip_serializing_if = "CypherEngineMode::is_legacy"))]
    pub cypher_engine: CypherEngineMode,
    /// Resolved at service admission and pinned across preparation and every
    /// page of the statement. Missing fields from older serialized contexts
    /// retain the compatibility policy.
    #[cfg_attr(
        feature = "query-transport",
        serde(default, skip_serializing_if = "MutationAtomicityPolicy::is_legacy_chunked")
    )]
    pub mutation_atomicity_policy: MutationAtomicityPolicy,
    #[cfg_attr(feature = "query-transport", serde(default))]
    refreshed_reader: bool,
    #[cfg_attr(feature = "query-transport", serde(skip, default))]
    pub cancellation_token: Option<QueryCancellationToken>,
    /// The caller's authorization identifier, which GQL's `SESSION_USER`
    /// reads in the experimental engine: the transport principal (a hash of
    /// a bearer token, an mTLS fingerprint, or `insecure`), never a secret.
    /// `serde(default)` keeps a context from an older node decodable.
    #[cfg_attr(feature = "query-transport", serde(default, skip_serializing_if = "Option::is_none"))]
    pub session_user: Option<String>,
    #[cfg(feature = "opencypher")]
    #[cfg_attr(feature = "query-transport", serde(skip, default))]
    validated_read: Option<ValidatedQueryRead>,
}

impl QueryContext {
    pub fn new(cell_id: impl Into<String>, idempotency_key: impl Into<String>) -> Self {
        Self {
            scope: GraphScope::default(),
            cell_id: cell_id.into(),
            idempotency_key: idempotency_key.into(),
            idempotency_principal: String::new(),
            read_epoch: None,
            result_window: QueryWindow::default(),
            parameters: BTreeMap::new(),
            list_parameters: BTreeMap::new(),
            max_runtime_ms: None,
            max_result_bytes: None,
            cypher_engine: CypherEngineMode::Legacy,
            mutation_atomicity_policy: MutationAtomicityPolicy::LegacyChunked,
            refreshed_reader: false,
            cancellation_token: None,
            session_user: None,
            #[cfg(feature = "opencypher")]
            validated_read: None,
        }
    }

    /// The authorization identifier `SESSION_USER` reads.
    pub fn as_session_user(mut self, session_user: impl Into<String>) -> Self {
        self.session_user = Some(session_user.into());
        self
    }

    pub fn in_scope(mut self, scope: GraphScope) -> Self {
        self.scope = scope;
        self
    }

    pub fn with_idempotency_principal(mut self, principal: impl Into<String>) -> Self {
        self.idempotency_principal = principal.into();
        self
    }

    pub fn at_epoch(mut self, read_epoch: StorageSequence) -> Self {
        self.read_epoch = Some(read_epoch);
        self
    }

    pub fn with_result_window(mut self, skip: u64, limit: Option<usize>) -> Self {
        self.result_window = QueryWindow { skip, limit };
        self
    }

    pub fn with_parameter(mut self, name: impl Into<String>, value: VertexPropertyValue) -> Self {
        self.parameters.insert(name.into(), value);
        self
    }

    pub fn with_parameters(mut self, parameters: impl IntoIterator<Item = (String, VertexPropertyValue)>) -> Self {
        self.parameters.extend(parameters);
        self
    }

    pub fn with_list_parameters(mut self, parameters: impl IntoIterator<Item = (String, Vec<VertexPropertyValue>)>) -> Self {
        self.list_parameters.extend(parameters);
        self
    }

    pub fn with_timeout_ms(mut self, max_runtime_ms: u64) -> Self {
        self.max_runtime_ms = Some(max_runtime_ms);
        self
    }

    pub fn with_max_result_bytes(mut self, max_result_bytes: u64) -> Self {
        self.max_result_bytes = Some(max_result_bytes);
        self
    }

    pub fn with_cypher_engine(mut self, cypher_engine: CypherEngineMode) -> Self {
        self.cypher_engine = cypher_engine;
        self
    }

    pub fn with_mutation_atomicity_policy(mut self, policy: MutationAtomicityPolicy) -> Self {
        self.mutation_atomicity_policy = policy;
        self
    }

    #[cfg(feature = "client-api")]
    #[cfg(feature = "opencypher")]
    pub(crate) fn with_refreshed_reader(mut self) -> Self {
        self.refreshed_reader = true;
        self
    }

    #[cfg(feature = "opencypher")]
    pub(crate) fn uses_refreshed_reader(&self) -> bool {
        self.refreshed_reader
    }

    pub fn with_cancellation_token(mut self, token: QueryCancellationToken) -> Self {
        self.cancellation_token = Some(token);
        self
    }

    #[cfg(feature = "opencypher")]
    pub(crate) fn with_validated_storage_read_epoch(
        mut self,
        read_epoch: StorageSequence,
        storage_sequence: crate::StorageSequence,
    ) -> Self {
        self.read_epoch = Some(read_epoch);
        self.validated_read = Some(ValidatedQueryRead {
            cell_id: self.cell_id.clone(),
            read_epoch,
            retention: ValidatedQueryReadRetention::StorageSnapshot(storage_sequence),
        });
        self
    }

    #[cfg(feature = "opencypher")]
    pub(crate) fn validated_read_epoch(&self) -> Option<StorageSequence> {
        self.validated_read.as_ref().and_then(|validated| {
            let _retention = &validated.retention;
            (validated.cell_id == self.cell_id && self.read_epoch == Some(validated.read_epoch)).then_some(validated.read_epoch)
        })
    }

    #[cfg(feature = "opencypher")]
    pub(crate) fn validated_storage_sequence(&self) -> Option<crate::StorageSequence> {
        self.validated_read.as_ref().and_then(|validated| {
            if validated.cell_id != self.cell_id || self.read_epoch != Some(validated.read_epoch) {
                return None;
            }
            match &validated.retention {
                ValidatedQueryReadRetention::StorageSnapshot(sequence) => Some(*sequence),
            }
        })
    }
}

#[cfg(feature = "opencypher")]
#[derive(Clone, Debug, Eq, PartialEq)]
struct ValidatedQueryRead {
    cell_id: String,
    read_epoch: StorageSequence,
    retention: ValidatedQueryReadRetention,
}

#[cfg(feature = "opencypher")]
#[derive(Clone, Debug, Eq, PartialEq)]
enum ValidatedQueryReadRetention {
    StorageSnapshot(crate::StorageSequence),
}

pub use hydradb_graph_slate::QueryCancellationToken;

#[cfg_attr(feature = "query-transport", derive(serde::Deserialize, serde::Serialize))]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct QueryWindow {
    pub skip: u64,
    pub limit: Option<usize>,
}

impl QueryWindow {
    pub fn is_default(self) -> bool {
        self.skip == 0 && self.limit.is_none()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum QueryOutput {
    Write(CommitResult),
    Mutation(QueryMutationResult),
    Rows(QueryResultSet),
    Vertices(Vec<VertexId>),
    Count(u64),
    Bool(bool),
}

#[cfg_attr(feature = "query-transport", derive(serde::Deserialize, serde::Serialize))]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryColumn {
    pub name: String,
}

impl QueryColumn {
    pub fn new(name: impl Into<String>) -> Self {
        Self { name: name.into() }
    }
}

#[cfg_attr(feature = "query-transport", derive(serde::Deserialize, serde::Serialize))]
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum QueryValue {
    Null,
    VertexId(VertexId),
    Count(u64),
    Bool(bool),
    Float(QueryFloat),
    Property(VertexPropertyValue),
    List(Vec<QueryValue>),
    Path(Box<QueryPath>),
    /// A record (GQL §4.15.4): its fields, ordered by name.
    Record(Vec<(String, QueryValue)>),
    /// A temporal instant or duration (GQL §4.16.6).
    Temporal(QueryTemporal),
    /// An exact decimal (GQL §4.16.5, GV17) as its literal, `1.50`.
    Decimal(String),
    /// A byte string (GQL §4.16.4, GV35).
    Bytes(Vec<u8>),
    /// Canonically encoded `hydradb_graph_types::CypherValue`, preserving the
    /// full DataFusion value model, including entities and extended values.
    GraphValue(Vec<u8>),
}

/// A temporal value as the protocols send it: Bolt's fields, and the GQL
/// literal string, without its type word, that HTTP sends.
#[cfg_attr(feature = "query-transport", derive(serde::Deserialize, serde::Serialize))]
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct QueryTemporal {
    pub kind: QueryTemporalKind,
    pub text: String,
}

#[cfg_attr(feature = "query-transport", derive(serde::Deserialize, serde::Serialize))]
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum QueryTemporalKind {
    /// Days since the Unix epoch.
    Date { days: i64 },
    /// Nanoseconds since midnight.
    LocalTime { nanoseconds: i64 },
    /// Local nanoseconds since midnight at a displacement of
    /// `offset_seconds` east of UTC.
    ZonedTime { nanoseconds: i64, offset_seconds: i32 },
    /// The local date and time as seconds since the epoch.
    LocalDatetime { seconds: i64, nanoseconds: u32 },
    /// The instant as UTC seconds since the epoch, at a displacement.
    ZonedDatetime {
        seconds: i64,
        nanoseconds: u32,
        offset_seconds: i32,
    },
    Duration {
        months: i64,
        days: i64,
        seconds: i64,
        nanoseconds: i32,
    },
}

#[cfg_attr(feature = "query-transport", derive(serde::Deserialize, serde::Serialize))]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryRow {
    pub values: Vec<QueryValue>,
}

impl QueryRow {
    pub fn new(values: Vec<QueryValue>) -> Self {
        Self { values }
    }

    #[cfg(feature = "opencypher")]
    pub(crate) fn estimated_resident_bytes(&self) -> u64 {
        self.values.iter().fold(std::mem::size_of::<Self>() as u64, |total, value| {
            total.saturating_add(value.estimated_resident_bytes())
        })
    }
}

impl QueryValue {
    #[cfg(feature = "opencypher")]
    pub(crate) fn estimated_resident_bytes(&self) -> u64 {
        let inline = std::mem::size_of::<Self>() as u64;
        match self {
            Self::Property(value) => inline.saturating_add(value.estimated_heap_bytes() as u64),
            Self::List(values) => values
                .iter()
                .fold(inline, |total, value| total.saturating_add(value.estimated_resident_bytes())),
            Self::Path(path) => path.estimated_resident_bytes(),
            Self::Temporal(temporal) => inline.saturating_add(temporal.text.len() as u64),
            Self::Decimal(text) => inline.saturating_add(text.len() as u64),
            Self::Bytes(bytes) => inline.saturating_add(bytes.len() as u64),
            Self::Record(fields) => fields.iter().fold(inline, |total, (name, value)| {
                total
                    .saturating_add(name.len() as u64)
                    .saturating_add(value.estimated_resident_bytes())
            }),
            Self::GraphValue(value) => inline.saturating_add(value.len() as u64),
            Self::Null | Self::VertexId(_) | Self::Count(_) | Self::Bool(_) | Self::Float(_) => inline,
        }
    }
}

#[cfg(feature = "opencypher")]
impl QueryPath {
    fn estimated_resident_bytes(&self) -> u64 {
        let node_bytes = self.nodes.iter().fold(0_u64, |total, node| {
            total
                .saturating_add(std::mem::size_of::<QueryPathNode>() as u64)
                .saturating_add(node.labels.iter().map(|label| label.len() as u64).sum())
                .saturating_add(property_map_resident_bytes(&node.properties))
        });
        self.relationships.iter().fold(node_bytes, |total, relationship| {
            total
                .saturating_add(std::mem::size_of::<QueryPathRelationship>() as u64)
                .saturating_add(relationship.edge_type.len() as u64)
                .saturating_add(property_map_resident_bytes(&relationship.properties))
        })
    }
}

#[cfg(feature = "opencypher")]
fn property_map_resident_bytes(properties: &BTreeMap<String, VertexPropertyValue>) -> u64 {
    properties.iter().fold(0_u64, |total, (key, value)| {
        total
            .saturating_add(key.len() as u64)
            .saturating_add(std::mem::size_of::<VertexPropertyValue>() as u64)
            .saturating_add(value.estimated_heap_bytes() as u64)
    })
}

#[cfg_attr(feature = "query-transport", derive(serde::Deserialize, serde::Serialize))]
#[derive(Clone, Debug)]
pub struct QueryResultSet {
    pub columns: Vec<QueryColumn>,
    pub rows: Vec<QueryRow>,
    pub read_epoch: Option<StorageSequence>,
    pub storage_sequence: Option<crate::StorageSequence>,
    /// The GQL-status objects of the outcome (ISO/IEC 39075 §4.9): the
    /// primary one first, then the rest. Empty when the engine that ran the
    /// query reports none. Optional on the wire both ways.
    #[cfg_attr(feature = "query-transport", serde(default, skip_serializing_if = "Vec::is_empty"))]
    pub statuses: Vec<crate::GqlStatusReport>,
}

impl PartialEq for QueryResultSet {
    fn eq(&self, other: &Self) -> bool {
        // A snapshot watermark describes execution, not the logical row set.
        self.columns == other.columns && self.rows == other.rows
    }
}

impl Eq for QueryResultSet {}

impl QueryResultSet {
    pub fn new(columns: Vec<QueryColumn>, rows: Vec<QueryRow>) -> Self {
        Self {
            columns,
            rows,
            read_epoch: None,
            storage_sequence: None,
            statuses: Vec::new(),
        }
    }

    /// The outcome's GQL-status objects, primary first.
    pub fn with_statuses(mut self, statuses: Vec<crate::GqlStatusReport>) -> Self {
        self.statuses = statuses;
        self
    }

    pub fn with_read_epoch(mut self, read_epoch: StorageSequence) -> Self {
        self.read_epoch = Some(read_epoch);
        self
    }

    pub fn with_storage_sequence(mut self, storage_sequence: crate::StorageSequence) -> Self {
        self.storage_sequence = Some(storage_sequence);
        self
    }

    // `client-api`, not the two wire protocols above it. The only caller is
    // `ClientQueryService::start_server_cursor`, which charges a materialised
    // result against `max_cursor_buffer_bytes` before it hands out a cursor —
    // server-side cursors are a `client-api` concept, and Bolt and HTTP inherit
    // them rather than owning them. Gating on `bolt-server`/`http-api` compiled
    // only because nothing built `client-api` on its own; `--features client-api`
    // alone failed E0599 here.
    #[cfg(feature = "client-api")]
    pub(crate) fn estimated_resident_bytes(&self) -> u64 {
        self.rows
            .iter()
            .fold(0_u64, |total, row| total.saturating_add(row.estimated_resident_bytes()))
    }
}

#[cfg_attr(feature = "query-transport", derive(serde::Deserialize, serde::Serialize))]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct QueryCursorToken {
    pub offset: u64,
}

impl QueryCursorToken {
    pub fn new(offset: u64) -> Self {
        Self { offset }
    }
}

#[cfg_attr(feature = "query-transport", derive(serde::Deserialize, serde::Serialize))]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryResultPage {
    pub columns: Vec<QueryColumn>,
    pub rows: Vec<QueryRow>,
    pub next_cursor: Option<QueryCursorToken>,
    /// The GQL-status objects of the outcome this page belongs to, as
    /// [`QueryResultSet::statuses`]. Optional on the wire both ways.
    #[cfg_attr(feature = "query-transport", serde(default, skip_serializing_if = "Vec::is_empty"))]
    pub statuses: Vec<crate::GqlStatusReport>,
}

impl QueryResultPage {
    pub fn new(columns: Vec<QueryColumn>, rows: Vec<QueryRow>, next_cursor: Option<QueryCursorToken>) -> Self {
        Self {
            columns,
            rows,
            next_cursor,
            statuses: Vec::new(),
        }
    }

    /// The outcome's GQL-status objects, primary first.
    pub fn with_statuses(mut self, statuses: Vec<crate::GqlStatusReport>) -> Self {
        self.statuses = statuses;
        self
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct QueryMutationResult {
    pub matched_rows: u64,
    /// Standalone vertices a `CREATE (n {...})` wrote. Relationship endpoints
    /// are not counted here.
    pub created_vertices: u64,
    pub created_edges: u64,
    pub created_relationships: u64,
    pub deleted_edges: u64,
    pub updated_vertices: u64,
    pub updated_relationships: u64,
    pub noops: u64,
    pub topology_sequence: Option<crate::StorageSequence>,
    /// Rows produced by a trailing `RETURN count(<binding>)`, when the
    /// mutation had one. `None` is the historical shape: a mutation that
    /// reports only counters and yields an empty result set.
    pub returned_rows: Option<QueryResultSet>,
}

impl QueryMutationResult {
    /// The rows a row-shaped caller sees for this mutation.
    ///
    /// A mutation with a trailing `RETURN count(...)` yields that one row; one
    /// without keeps the historical empty result. Either way the topology
    /// sequence rides along, so a caller can still wait on its own write.
    pub fn into_result_set(self) -> QueryResultSet {
        let rows = self.returned_rows.unwrap_or_else(|| QueryResultSet::new(Vec::new(), Vec::new()));
        match self.topology_sequence {
            Some(sequence) => rows.with_storage_sequence(sequence),
            None => rows,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum QueryStatement {
    CreateEdge {
        edge_type: String,
        src: VertexId,
        dst: VertexId,
    },
    CreateEdgeWithMetadata {
        edge_type: String,
        src: VertexId,
        dst: VertexId,
        src_metadata: VertexMetadata,
        dst_metadata: VertexMetadata,
    },
    CreateEdgeWithFullMetadata {
        edge_type: String,
        src: VertexId,
        dst: VertexId,
        src_metadata: VertexMetadata,
        dst_metadata: VertexMetadata,
        edge_metadata: EdgeMetadata,
    },
    MatchOut {
        edge_type: String,
        src: VertexId,
        return_count: bool,
    },
    MatchOutFiltered {
        edge_type: String,
        src: VertexId,
        dst: VertexId,
        return_count: bool,
    },
    MatchEdge {
        edge_type: String,
        src: VertexId,
        dst: VertexId,
        return_count: bool,
    },
    MatchReachable {
        edge_type: String,
        src: VertexId,
        min_hops: u8,
        max_hops: u8,
        return_count: bool,
    },
}

impl QueryStatement {
    pub fn is_write(&self) -> bool {
        matches!(
            self,
            QueryStatement::CreateEdge { .. }
                | QueryStatement::CreateEdgeWithMetadata { .. }
                | QueryStatement::CreateEdgeWithFullMetadata { .. }
        )
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryPlan {
    pub cell_id: String,
    pub idempotency_key: String,
    pub read_epoch: Option<StorageSequence>,
    pub result_window: QueryWindow,
    pub max_runtime_ms: Option<u64>,
    pub logical: LogicalQueryPlan,
    pub physical: PhysicalQueryPlan,
}

#[cfg_attr(feature = "query-transport", derive(serde::Deserialize, serde::Serialize))]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RowQueryPlan {
    pub cell_id: String,
    pub read_epoch: StorageSequence,
    pub columns: Vec<QueryColumn>,
    pub groups: Vec<RowQueryPlanGroup>,
    pub union_all: bool,
    pub union_arms: Vec<RowQueryPlan>,
}

#[cfg_attr(feature = "query-transport", derive(serde::Deserialize, serde::Serialize))]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RowQueryPlanGroup {
    pub optional: bool,
    pub has_predicate: bool,
    pub estimated_cardinality: u64,
    pub patterns: Vec<RowQueryPlanPattern>,
    pub optimizer_passes: Vec<RowQueryOptimizerPass>,
}

#[cfg_attr(feature = "query-transport", derive(serde::Deserialize, serde::Serialize))]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RowQueryPlanPattern {
    pub original_index: usize,
    pub access: RowQueryAccess,
    pub estimated_cardinality: u64,
    pub bindings: Vec<String>,
    pub optimizer_passes: Vec<RowQueryOptimizerPass>,
}

#[cfg_attr(feature = "query-transport", derive(serde::Deserialize, serde::Serialize))]
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RowQueryAccess {
    VertexIdSeek,
    VertexPropertyIndex { property: String },
    VertexLabelScan { label: String },
    AllVertexScan,
    BoundOutExpand { edge_type: String },
    BoundInExpand { edge_type: String },
    ExpandInto { edge_type: String },
    EdgePropertyIndex { edge_type: String, property: String },
    FullEdgeScan { edge_type: String },
    VariableLengthExpand { edge_type: String, min_hops: u8, max_hops: u8 },
}

#[cfg_attr(feature = "query-transport", derive(serde::Deserialize, serde::Serialize))]
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RowQueryOptimizerPass {
    UtilizeVertexIndex,
    OrderedLimitPushdown,
    /// A WHERE equality was folded into a node pattern so the pattern could be
    /// anchored on the vertex property index instead of scanned by label.
    EqualityPredicatePushdown,
    UtilizeEdgeIndex,
    CostBasedLabelScan,
    ConnectivityOrder,
    JoinOrder,
    ExpandInto,
    GraphKernel,
    ReverseExpand,
    FullScanFallback,
    PreserveOptionalBoundary,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LogicalQueryPlan {
    CreateEdge {
        edge_type: String,
        src: VertexId,
        dst: VertexId,
    },
    CreateEdgeWithMetadata {
        edge_type: String,
        src: VertexId,
        dst: VertexId,
        src_metadata: VertexMetadata,
        dst_metadata: VertexMetadata,
    },
    CreateEdgeWithFullMetadata {
        edge_type: String,
        src: VertexId,
        dst: VertexId,
        src_metadata: VertexMetadata,
        dst_metadata: VertexMetadata,
        edge_metadata: EdgeMetadata,
    },
    MatchOut {
        edge_type: String,
        src: VertexId,
        return_count: bool,
    },
    MatchOutFiltered {
        edge_type: String,
        src: VertexId,
        dst: VertexId,
        return_count: bool,
    },
    MatchEdge {
        edge_type: String,
        src: VertexId,
        dst: VertexId,
        return_count: bool,
    },
    MatchReachable {
        edge_type: String,
        src: VertexId,
        min_hops: u8,
        max_hops: u8,
        return_count: bool,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PhysicalQueryPlan {
    WriteEdge {
        edge_type: String,
        src: VertexId,
        dst: VertexId,
    },
    WriteEdgeWithMetadata {
        edge_type: String,
        src: VertexId,
        dst: VertexId,
        src_metadata: VertexMetadata,
        dst_metadata: VertexMetadata,
    },
    WriteEdgeWithFullMetadata {
        edge_type: String,
        src: VertexId,
        dst: VertexId,
        src_metadata: VertexMetadata,
        dst_metadata: VertexMetadata,
        edge_metadata: EdgeMetadata,
    },
    OutDegreeCounter {
        edge_type: String,
        src: VertexId,
    },
    OutNeighbors {
        edge_type: String,
        src: VertexId,
    },
    EdgeExistsToCount {
        edge_type: String,
        src: VertexId,
        dst: VertexId,
    },
    EdgeExistsToVertices {
        edge_type: String,
        src: VertexId,
        dst: VertexId,
    },
    EdgeExistsToBool {
        edge_type: String,
        src: VertexId,
        dst: VertexId,
    },
    ReachableVertices {
        edge_type: String,
        src: VertexId,
        min_hops: u8,
        max_hops: u8,
        return_count: bool,
    },
}

impl QueryPlan {
    pub fn is_write(&self) -> bool {
        matches!(
            self.physical,
            PhysicalQueryPlan::WriteEdge { .. }
                | PhysicalQueryPlan::WriteEdgeWithMetadata { .. }
                | PhysicalQueryPlan::WriteEdgeWithFullMetadata { .. }
        )
    }

    pub fn returns_vertices(&self) -> bool {
        matches!(
            self.physical,
            PhysicalQueryPlan::OutNeighbors { .. }
                | PhysicalQueryPlan::EdgeExistsToVertices { .. }
                | PhysicalQueryPlan::ReachableVertices { return_count: false, .. }
        )
    }

    pub fn validate_for_execution(&self) -> Result<()> {
        validate_component("cell_id", &self.cell_id)?;
        validate_logical_plan(&self.logical)?;

        let expected_physical = physical_plan(&self.logical);
        if self.physical != expected_physical {
            return Err(GraphError::UnsupportedQuery {
                reason: QueryFailureReason::Other,
                dialect: "GraphQuery",
                feature: "physical query plan does not match logical query plan".to_string(),
            });
        }

        if self.is_write() {
            validate_component("idempotency_key", &self.idempotency_key)?;
            if self.read_epoch.is_some() {
                return Err(GraphError::UnsupportedQuery {
                    reason: QueryFailureReason::InvalidRequest,
                    dialect: "GraphQuery",
                    feature: "snapshot epochs are only valid for read queries".to_string(),
                });
            }
        }
        if !self.result_window.is_default() && !self.returns_vertices() {
            return Err(GraphError::UnsupportedQuery {
                reason: QueryFailureReason::InvalidRequest,
                dialect: "GraphQuery",
                feature: "result windows are only valid for vertex-returning read queries".to_string(),
            });
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct QueryPlanner;

impl QueryPlanner {
    pub fn plan(context: &QueryContext, statement: &QueryStatement) -> Result<QueryPlan> {
        validate_component("cell_id", &context.cell_id)?;

        let logical = logical_plan(statement)?;
        validate_logical_plan(&logical)?;
        let physical = physical_plan(&logical);
        let plan = QueryPlan {
            cell_id: context.cell_id.clone(),
            idempotency_key: context.idempotency_key.clone(),
            read_epoch: context.read_epoch,
            result_window: context.result_window,
            max_runtime_ms: context.max_runtime_ms,
            logical,
            physical,
        };
        plan.validate_for_execution()?;
        Ok(plan)
    }
}

fn logical_plan(statement: &QueryStatement) -> Result<LogicalQueryPlan> {
    Ok(match statement {
        QueryStatement::CreateEdge { edge_type, src, dst } => LogicalQueryPlan::CreateEdge {
            edge_type: edge_type.clone(),
            src: *src,
            dst: *dst,
        },
        QueryStatement::CreateEdgeWithMetadata {
            edge_type,
            src,
            dst,
            src_metadata,
            dst_metadata,
        } => LogicalQueryPlan::CreateEdgeWithMetadata {
            edge_type: edge_type.clone(),
            src: *src,
            dst: *dst,
            src_metadata: src_metadata.clone(),
            dst_metadata: dst_metadata.clone(),
        },
        QueryStatement::CreateEdgeWithFullMetadata {
            edge_type,
            src,
            dst,
            src_metadata,
            dst_metadata,
            edge_metadata,
        } => LogicalQueryPlan::CreateEdgeWithFullMetadata {
            edge_type: edge_type.clone(),
            src: *src,
            dst: *dst,
            src_metadata: src_metadata.clone(),
            dst_metadata: dst_metadata.clone(),
            edge_metadata: edge_metadata.clone(),
        },
        QueryStatement::MatchOut {
            edge_type,
            src,
            return_count,
        } => LogicalQueryPlan::MatchOut {
            edge_type: edge_type.clone(),
            src: *src,
            return_count: *return_count,
        },
        QueryStatement::MatchOutFiltered {
            edge_type,
            src,
            dst,
            return_count,
        } => LogicalQueryPlan::MatchOutFiltered {
            edge_type: edge_type.clone(),
            src: *src,
            dst: *dst,
            return_count: *return_count,
        },
        QueryStatement::MatchEdge {
            edge_type,
            src,
            dst,
            return_count,
        } => LogicalQueryPlan::MatchEdge {
            edge_type: edge_type.clone(),
            src: *src,
            dst: *dst,
            return_count: *return_count,
        },
        QueryStatement::MatchReachable {
            edge_type,
            src,
            min_hops,
            max_hops,
            return_count,
        } => LogicalQueryPlan::MatchReachable {
            edge_type: edge_type.clone(),
            src: *src,
            min_hops: *min_hops,
            max_hops: *max_hops,
            return_count: *return_count,
        },
    })
}

fn validate_logical_plan(plan: &LogicalQueryPlan) -> Result<()> {
    match plan {
        LogicalQueryPlan::CreateEdge { edge_type, .. }
        | LogicalQueryPlan::CreateEdgeWithMetadata { edge_type, .. }
        | LogicalQueryPlan::CreateEdgeWithFullMetadata { edge_type, .. }
        | LogicalQueryPlan::MatchOut { edge_type, .. }
        | LogicalQueryPlan::MatchOutFiltered { edge_type, .. }
        | LogicalQueryPlan::MatchEdge { edge_type, .. }
        | LogicalQueryPlan::MatchReachable { edge_type, .. } => {
            validate_component("edge_type", edge_type)?;
        }
    }
    if let LogicalQueryPlan::CreateEdgeWithMetadata {
        src_metadata,
        dst_metadata,
        ..
    }
    | LogicalQueryPlan::CreateEdgeWithFullMetadata {
        src_metadata,
        dst_metadata,
        ..
    } = plan
    {
        validate_query_vertex_metadata(src_metadata)?;
        validate_query_vertex_metadata(dst_metadata)?;
    }
    if let LogicalQueryPlan::CreateEdgeWithFullMetadata { edge_metadata, .. } = plan {
        validate_query_edge_metadata(edge_metadata)?;
    }
    Ok(())
}

fn physical_plan(logical: &LogicalQueryPlan) -> PhysicalQueryPlan {
    match logical {
        LogicalQueryPlan::CreateEdge { edge_type, src, dst } => PhysicalQueryPlan::WriteEdge {
            edge_type: edge_type.clone(),
            src: *src,
            dst: *dst,
        },
        LogicalQueryPlan::CreateEdgeWithMetadata {
            edge_type,
            src,
            dst,
            src_metadata,
            dst_metadata,
        } => PhysicalQueryPlan::WriteEdgeWithMetadata {
            edge_type: edge_type.clone(),
            src: *src,
            dst: *dst,
            src_metadata: src_metadata.clone(),
            dst_metadata: dst_metadata.clone(),
        },
        LogicalQueryPlan::CreateEdgeWithFullMetadata {
            edge_type,
            src,
            dst,
            src_metadata,
            dst_metadata,
            edge_metadata,
        } => PhysicalQueryPlan::WriteEdgeWithFullMetadata {
            edge_type: edge_type.clone(),
            src: *src,
            dst: *dst,
            src_metadata: src_metadata.clone(),
            dst_metadata: dst_metadata.clone(),
            edge_metadata: edge_metadata.clone(),
        },
        LogicalQueryPlan::MatchOut {
            edge_type,
            src,
            return_count: true,
        } => PhysicalQueryPlan::OutDegreeCounter {
            edge_type: edge_type.clone(),
            src: *src,
        },
        LogicalQueryPlan::MatchOut {
            edge_type,
            src,
            return_count: false,
        } => PhysicalQueryPlan::OutNeighbors {
            edge_type: edge_type.clone(),
            src: *src,
        },
        LogicalQueryPlan::MatchOutFiltered {
            edge_type,
            src,
            dst,
            return_count: true,
        }
        | LogicalQueryPlan::MatchEdge {
            edge_type,
            src,
            dst,
            return_count: true,
        } => PhysicalQueryPlan::EdgeExistsToCount {
            edge_type: edge_type.clone(),
            src: *src,
            dst: *dst,
        },
        LogicalQueryPlan::MatchOutFiltered {
            edge_type,
            src,
            dst,
            return_count: false,
        } => PhysicalQueryPlan::EdgeExistsToVertices {
            edge_type: edge_type.clone(),
            src: *src,
            dst: *dst,
        },
        LogicalQueryPlan::MatchEdge {
            edge_type,
            src,
            dst,
            return_count: false,
        } => PhysicalQueryPlan::EdgeExistsToBool {
            edge_type: edge_type.clone(),
            src: *src,
            dst: *dst,
        },
        LogicalQueryPlan::MatchReachable {
            edge_type,
            src,
            min_hops,
            max_hops,
            return_count,
        } => PhysicalQueryPlan::ReachableVertices {
            edge_type: edge_type.clone(),
            src: *src,
            min_hops: *min_hops,
            max_hops: *max_hops,
            return_count: *return_count,
        },
    }
}

fn validate_query_vertex_metadata(metadata: &VertexMetadata) -> Result<()> {
    for label in &metadata.labels {
        validate_component("label", label)?;
    }
    for property in metadata.properties.keys() {
        validate_component("property", property)?;
    }
    Ok(())
}

fn validate_query_edge_metadata(metadata: &EdgeMetadata) -> Result<()> {
    for property in metadata.properties.keys() {
        validate_component("property", property)?;
    }
    Ok(())
}
