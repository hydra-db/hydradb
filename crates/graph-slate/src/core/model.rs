use std::collections::{BTreeMap, BTreeSet};

use crate::{StorageSequence, VertexId};

pub type RelationshipId = u64;

/// The first relationship id the query engine keeps for itself. A relationship
/// stored without an id (a structural edge) is given one at or above it for the
/// length of one query, so a stored relationship never takes one: below it
/// every id is a stored relationship's, and above it none is.
pub const GENERATED_RELATIONSHIP_ID_FLOOR: RelationshipId = 1 << 63;

/// `id`, if a stored relationship may carry it; see
/// [`GENERATED_RELATIONSHIP_ID_FLOOR`]. `key` names what was being written.
pub fn stored_relationship_id(id: RelationshipId, key: impl FnOnce() -> String) -> crate::Result<RelationshipId> {
    if id < GENERATED_RELATIONSHIP_ID_FLOOR {
        return Ok(id);
    }
    Err(crate::GraphError::CorruptValue {
        key: key(),
        reason: format!("relationship id {id} is in the range the query engine reserves for generated ids (2^63 and above)"),
    })
}
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct GraphRepairReport {
    pub cell_id: String,
    pub edge_type: String,
    pub read_epoch: StorageSequence,
    pub live_edges: u64,
    pub degree_mismatches: Vec<String>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct GraphExportDigest {
    pub cell_id: String,
    pub edge_type: String,
    pub read_epoch: StorageSequence,
    pub live_edges: u64,
    pub edge_checksum: u64,
    pub out_degree_checksum: u64,
    pub in_degree_checksum: u64,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct GraphCorrectnessReport {
    pub cell_id: String,
    pub edge_type: String,
    pub read_epoch: StorageSequence,
    pub digest: GraphExportDigest,
    pub canonical_edges: u64,
    pub out_index_edges: u64,
    pub in_index_edges: u64,
    pub degree_counters: u64,
    pub relationship_records: u64,
    pub relationship_count_counters: u64,
    pub relationship_property_indexes: u64,
    /// `rmerge_idx` identity pointers present for this edge type. Verified
    /// against the invariant that a pointer names the single live
    /// relationship carrying its identity; a missing pointer is never a
    /// mismatch because heal-on-read earns them lazily.
    pub relationship_merge_indexes: u64,
    pub matrix_edges_checked: u64,
    pub traversal_roots_checked: u64,
    pub mismatch_count: u64,
    pub mismatch_samples: Vec<String>,
}

impl GraphCorrectnessReport {
    pub fn is_clean(&self) -> bool {
        self.mismatch_count == 0
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EdgeMutation {
    pub cell_id: String,
    pub edge_type: String,
    pub src: VertexId,
    pub dst: VertexId,
    pub idempotency_key: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EdgeRecord {
    pub cell_id: String,
    pub edge_type: String,
    pub src: VertexId,
    pub dst: VertexId,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NeighborBatchEntry {
    pub vertex: VertexId,
    pub neighbors: Vec<VertexId>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EdgeExistenceBatchEntry {
    pub src: VertexId,
    pub dst: VertexId,
    pub exists: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RelationshipMutation {
    pub cell_id: String,
    pub edge_type: String,
    pub src: VertexId,
    pub dst: VertexId,
    pub relationship_id: RelationshipId,
    pub metadata: EdgeMetadata,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RelationshipRecord {
    pub cell_id: String,
    pub edge_type: String,
    pub src: VertexId,
    pub dst: VertexId,
    pub relationship_id: RelationshipId,
    pub metadata: EdgeMetadata,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RelationshipCreateResult {
    pub epoch: StorageSequence,
    pub relationship_id: RelationshipId,
    pub structural_edge_inserted: bool,
    pub already_created: bool,
}

/// The outcome of creating one standalone vertex.
///
/// `already_created` is true when this call replayed an earlier create with
/// the same idempotency key and payload, so nothing new was written.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VertexCreateResult {
    pub epoch: StorageSequence,
    pub already_created: bool,
}

#[cfg_attr(feature = "query-transport", derive(serde::Deserialize, serde::Serialize))]
#[derive(Clone, Copy, Debug, Default)]
pub struct QueryFloat(pub f64);

impl PartialEq for QueryFloat {
    fn eq(&self, other: &Self) -> bool {
        self.0.to_bits() == other.0.to_bits()
    }
}

impl Eq for QueryFloat {}

impl PartialOrd for QueryFloat {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for QueryFloat {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.total_cmp(&other.0)
    }
}

#[cfg_attr(feature = "query-transport", derive(serde::Deserialize, serde::Serialize))]
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum VertexPropertyInteger {
    Unsigned(u64),
    Signed(i64),
}

#[cfg_attr(feature = "query-transport", derive(serde::Deserialize, serde::Serialize))]
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum VertexPropertyList {
    Empty,
    Integer(Vec<VertexPropertyInteger>),
    Bool(Vec<bool>),
    Float(Vec<QueryFloat>),
    String(Vec<String>),
    Date(Vec<i32>),
}

/// The most elements a stored list property may hold.
///
/// Every write path enforces it: Cypher writes through `storage_property`, and
/// every write through `validate_vertex_metadata` / `validate_edge_metadata`.
/// It sits well above common embedding widths (384-4096 floats).
///
/// Keep it even though request size bounds lists today. Every stored list
/// currently arrives as a literal or a parameter, so the 1 MiB HTTP and query
/// limits bound it. A list built inside a query from graph data —
/// `collect(...)`, `||` or `+` concatenation, `range(...)` — has no request to
/// bound it: `SET p.names = collect(f.name)` is a short query that can store
/// millions of elements. Because this check sits on the write path, those
/// features are covered the day they are added, without their author having
/// to remember a size limit.
///
/// This limit is what keeps the codec's 32-bit element-count header from
/// overflowing; string elements are separately held below 4 GiB for their own
/// byte-length header. Decoding does not enforce it, so a list stored before
/// the limit existed stays readable; its next write must bring it under the
/// limit.
pub const MAX_PROPERTY_LIST_ELEMENTS: usize = 10_000;

impl VertexPropertyList {
    pub fn len(&self) -> usize {
        match self {
            Self::Empty => 0,
            Self::Integer(values) => values.len(),
            Self::Bool(values) => values.len(),
            Self::Float(values) => values.len(),
            Self::String(values) => values.len(),
            Self::Date(values) => values.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        match self {
            Self::Empty => true,
            Self::Integer(values) => values.is_empty(),
            Self::Bool(values) => values.is_empty(),
            Self::Float(values) => values.is_empty(),
            Self::String(values) => values.is_empty(),
            Self::Date(values) => values.is_empty(),
        }
    }

    pub fn from_property_values(values: Vec<VertexPropertyValue>) -> std::result::Result<Self, String> {
        let Some(first) = values.first() else {
            return Ok(Self::Empty);
        };
        match first {
            VertexPropertyValue::Integer(_) | VertexPropertyValue::SignedInteger(_) => values
                .into_iter()
                .map(|value| match value {
                    VertexPropertyValue::Integer(value) => Ok(VertexPropertyInteger::Unsigned(value)),
                    VertexPropertyValue::SignedInteger(value) => Ok(VertexPropertyInteger::Signed(value)),
                    _ => Err("stored lists must be homogeneous; integer and float lists are distinct".into()),
                })
                .collect::<std::result::Result<Vec<_>, _>>()
                .map(Self::Integer),
            VertexPropertyValue::Bool(_) => values
                .into_iter()
                .map(|value| match value {
                    VertexPropertyValue::Bool(value) => Ok(value),
                    _ => Err("stored lists must be homogeneous".into()),
                })
                .collect::<std::result::Result<Vec<_>, _>>()
                .map(Self::Bool),
            VertexPropertyValue::Float(_) => values
                .into_iter()
                .map(|value| match value {
                    VertexPropertyValue::Float(value) if value.0.is_finite() => Ok(value),
                    VertexPropertyValue::Float(_) => Err("stored float lists require finite values".into()),
                    _ => Err("stored lists must be homogeneous; integer and float lists are distinct".into()),
                })
                .collect::<std::result::Result<Vec<_>, _>>()
                .map(Self::Float),
            VertexPropertyValue::String(_) => values
                .into_iter()
                .map(|value| match value {
                    VertexPropertyValue::String(value) => Ok(value),
                    _ => Err("stored lists must be homogeneous".into()),
                })
                .collect::<std::result::Result<Vec<_>, _>>()
                .map(Self::String),
            VertexPropertyValue::Date(_) => values
                .into_iter()
                .map(|value| match value {
                    VertexPropertyValue::Date(value) if date_iso_string(value).is_some() => Ok(value),
                    VertexPropertyValue::Date(_) => Err("stored dates exceed the supported calendar range".into()),
                    _ => Err("stored lists must be homogeneous".into()),
                })
                .collect::<std::result::Result<Vec<_>, _>>()
                .map(Self::Date),
            VertexPropertyValue::List(_) => Err("stored lists cannot contain nested lists".into()),
        }
    }

    pub fn into_property_values(self) -> Vec<VertexPropertyValue> {
        match self {
            Self::Empty => Vec::new(),
            Self::Integer(values) => values
                .into_iter()
                .map(|value| match value {
                    VertexPropertyInteger::Unsigned(value) => VertexPropertyValue::Integer(value),
                    VertexPropertyInteger::Signed(value) => VertexPropertyValue::SignedInteger(value),
                })
                .collect(),
            Self::Bool(values) => values.into_iter().map(VertexPropertyValue::Bool).collect(),
            Self::Float(values) => values.into_iter().map(VertexPropertyValue::Float).collect(),
            Self::String(values) => values.into_iter().map(VertexPropertyValue::String).collect(),
            Self::Date(values) => values.into_iter().map(VertexPropertyValue::Date).collect(),
        }
    }
}

#[cfg_attr(feature = "query-transport", derive(serde::Deserialize, serde::Serialize))]
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum VertexPropertyValue {
    Integer(u64),
    SignedInteger(i64),
    Bool(bool),
    Float(QueryFloat),
    String(String),
    Date(i32),
    List(VertexPropertyList),
}

impl VertexPropertyValue {
    pub fn from_i64(value: i64) -> Self {
        match u64::try_from(value) {
            Ok(value) => Self::Integer(value),
            Err(_) => Self::SignedInteger(value),
        }
    }

    pub fn is_scalar_indexable(&self) -> bool {
        matches!(
            self,
            Self::Integer(_) | Self::SignedInteger(_) | Self::Bool(_) | Self::Float(_) | Self::String(_)
        )
    }

    pub fn estimated_heap_bytes(&self) -> usize {
        match self {
            Self::String(value) => value.capacity(),
            Self::List(VertexPropertyList::Empty) => 0,
            Self::List(VertexPropertyList::Integer(values)) => {
                values.capacity().saturating_mul(std::mem::size_of::<VertexPropertyInteger>())
            }
            Self::List(VertexPropertyList::Bool(values)) => values.capacity().saturating_mul(std::mem::size_of::<bool>()),
            Self::List(VertexPropertyList::Float(values)) => values.capacity().saturating_mul(std::mem::size_of::<QueryFloat>()),
            Self::List(VertexPropertyList::String(values)) => values
                .capacity()
                .saturating_mul(std::mem::size_of::<String>())
                .saturating_add(values.iter().map(String::capacity).sum::<usize>()),
            Self::List(VertexPropertyList::Date(values)) => values.capacity().saturating_mul(std::mem::size_of::<i32>()),
            Self::Integer(_) | Self::SignedInteger(_) | Self::Bool(_) | Self::Float(_) | Self::Date(_) => 0,
        }
    }

    pub(crate) fn validate(&self) -> std::result::Result<(), String> {
        match self {
            Self::Date(value) if date_iso_string(*value).is_none() => Err("stored date exceeds the supported calendar range".into()),
            Self::List(VertexPropertyList::Float(values)) if values.iter().any(|value| !value.0.is_finite()) => {
                Err("stored float lists require finite values".into())
            }
            Self::List(VertexPropertyList::Date(values)) if values.iter().any(|value| date_iso_string(*value).is_none()) => {
                Err("stored date list exceeds the supported calendar range".into())
            }
            _ => Ok(()),
        }
    }

    #[cfg_attr(not(any(feature = "json-properties", feature = "opencypher")), allow(dead_code))]
    pub fn exact_u64_from_f64(value: f64) -> Option<u64> {
        const U64_EXCLUSIVE_UPPER: f64 = 18446744073709551616.0;
        if !value.is_finite() || !(0.0..U64_EXCLUSIVE_UPPER).contains(&value) || value.fract() != 0.0 {
            return None;
        }
        let integer = value as u64;
        ((integer as f64) == value).then_some(integer)
    }

    #[cfg_attr(not(any(feature = "json-properties", feature = "opencypher")), allow(dead_code))]
    pub fn exact_i64_from_f64(value: f64) -> Option<i64> {
        const I64_INCLUSIVE_LOWER: f64 = -9223372036854775808.0;
        const I64_EXCLUSIVE_UPPER: f64 = 9223372036854775808.0;
        if !value.is_finite() || !(I64_INCLUSIVE_LOWER..I64_EXCLUSIVE_UPPER).contains(&value) || value.fract() != 0.0 {
            return None;
        }
        let integer = value as i64;
        ((integer as f64) == value).then_some(integer)
    }

    #[cfg(feature = "opencypher")]
    pub fn exact_f64_from_u64(value: u64) -> Option<f64> {
        let float = value as f64;
        (Self::exact_u64_from_f64(float) == Some(value)).then_some(float)
    }

    #[cfg(feature = "opencypher")]
    pub fn exact_f64_from_i64(value: i64) -> Option<f64> {
        let float = value as f64;
        (Self::exact_i64_from_f64(float) == Some(value)).then_some(float)
    }

    #[cfg(feature = "json-properties")]
    pub fn from_json_value(value: &serde_json::Value) -> Self {
        match value {
            serde_json::Value::Bool(value) => Self::Bool(*value),
            serde_json::Value::Number(value) => {
                if let Some(value) = value.as_u64() {
                    Self::Integer(value)
                } else if let Some(value) = value.as_i64() {
                    Self::from_i64(value)
                } else if let Some(value) = value.as_f64() {
                    Self::exact_u64_from_f64(value)
                        .map(Self::Integer)
                        .or_else(|| Self::exact_i64_from_f64(value).map(Self::from_i64))
                        .unwrap_or(Self::Float(QueryFloat(value)))
                } else {
                    Self::String(value.to_string())
                }
            }
            serde_json::Value::String(value) => Self::String(value.clone()),
            serde_json::Value::Null | serde_json::Value::Array(_) | serde_json::Value::Object(_) => Self::String(value.to_string()),
        }
    }

    #[cfg(feature = "json-properties")]
    pub fn from_typed_json_value(value: &serde_json::Value) -> std::result::Result<Self, String> {
        let object = value.as_object().ok_or_else(|| "typed property must be an object".to_owned())?;
        if object.len() != 2 || !object.contains_key("type") || !object.contains_key("value") {
            return Err("typed property must contain exactly type and value".into());
        }
        let kind = object["type"]
            .as_str()
            .ok_or_else(|| "typed property type must be a string".to_owned())?;
        let value = &object["value"];
        match kind {
            "boolean" => value
                .as_bool()
                .map(Self::Bool)
                .ok_or_else(|| "boolean value must be a boolean".into()),
            "integer" => value
                .as_u64()
                .map(Self::Integer)
                .or_else(|| value.as_i64().map(Self::from_i64))
                .ok_or_else(|| "integer value must fit u64 or i64".into()),
            "unsigned" => value
                .as_u64()
                .map(Self::Integer)
                .ok_or_else(|| "unsigned value must fit u64".into()),
            "float" => value
                .as_f64()
                .filter(|value| value.is_finite())
                .map(|value| Self::Float(QueryFloat(value)))
                .ok_or_else(|| "float value must be finite".into()),
            "string" => value
                .as_str()
                .map(|value| Self::String(value.to_owned()))
                .ok_or_else(|| "string value must be a string".into()),
            "date" => {
                let value = value.as_str().ok_or_else(|| "date value must be an ISO date string".to_owned())?;
                parse_date_epoch_days(value)
                    .map(Self::Date)
                    .ok_or_else(|| invalid_date_message(value))
            }
            "list" => {
                let values = value.as_array().ok_or_else(|| "list value must be an array".to_owned())?;
                let values = values
                    .iter()
                    .map(Self::from_typed_json_value)
                    .collect::<std::result::Result<Vec<_>, _>>()?;
                VertexPropertyList::from_property_values(values).map(Self::List)
            }
            _ => Err(format!("unsupported typed property type `{kind}`")),
        }
    }
}

/// Parses a Cypher date string into stored epoch days, with the parser every
/// engine shares (`hydradb_cypher_date::parse_date`). `None` when the text is
/// not a date or the day falls outside the stored range.
pub fn parse_date_epoch_days(text: &str) -> Option<i32> {
    let date = hydradb_cypher_date::parse_date(text)?;
    let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1)?;
    i32::try_from(date.signed_duration_since(epoch).num_days()).ok()
}

/// The error every Cypher route reports for a string that is not a date.
pub fn invalid_date_message(text: &str) -> String {
    format!("invalid date `{text}`; expected an ISO 8601 date such as YYYY-MM-DD")
}

pub fn date_iso_string(days_since_epoch: i32) -> Option<String> {
    use chrono::{Days, NaiveDate};

    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1)?;
    let date = if days_since_epoch >= 0 {
        epoch.checked_add_days(Days::new(days_since_epoch as u64))?
    } else {
        epoch.checked_sub_days(Days::new(days_since_epoch.unsigned_abs() as u64))?
    };
    Some(date.format("%Y-%m-%d").to_string())
}

#[cfg_attr(feature = "query-transport", derive(serde::Deserialize, serde::Serialize))]
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct VertexMetadata {
    pub labels: BTreeSet<String>,
    pub properties: BTreeMap<String, VertexPropertyValue>,
}

#[cfg_attr(feature = "query-transport", derive(serde::Deserialize, serde::Serialize))]
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct EdgeMetadata {
    pub properties: BTreeMap<String, VertexPropertyValue>,
}

impl EdgeMetadata {
    pub fn with_property(mut self, name: impl Into<String>, value: VertexPropertyValue) -> Self {
        self.properties.insert(name.into(), value);
        self
    }
}

impl VertexMetadata {
    pub fn with_label(mut self, label: impl Into<String>) -> Self {
        self.labels.insert(label.into());
        self
    }

    pub fn with_property(mut self, name: impl Into<String>, value: VertexPropertyValue) -> Self {
        self.properties.insert(name.into(), value);
        self
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OutEdgeSegment {
    pub cell_id: String,
    pub edge_type: String,
    pub src: VertexId,
    pub storage_sequence: StorageSequence,
    pub destinations: Vec<VertexId>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CommitResult {
    pub epoch: StorageSequence,
    pub already_existed: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeleteResult {
    pub epoch: StorageSequence,
    pub deleted: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EdgeDeleteBatchResult {
    pub start_epoch: StorageSequence,
    pub end_epoch: StorageSequence,
    pub deleted: u64,
    pub already_deleted: u64,
    pub results: Vec<DeleteResult>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct VertexDeleteResult {
    pub epoch: StorageSequence,
    pub vertex_deleted: bool,
    pub incident_edges_deleted: u64,
    pub relationships_deleted: u64,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct GraphCellDropResult {
    pub marker_epoch: StorageSequence,
    pub deleted_keys: u64,
    pub batches: u64,
    pub already_dropped: bool,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SegmentCompactionResult {
    pub compacted_through_epoch: StorageSequence,
    pub source_segments: u64,
    pub deleted_segment_keys: u64,
    pub deleted_tombstone_keys: u64,
    pub input_edges: u64,
    pub output_edges: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BulkImportResult {
    pub start_epoch: StorageSequence,
    pub end_epoch: StorageSequence,
    pub inserted: u64,
    pub already_existed: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RelationshipImportResult {
    pub start_epoch: StorageSequence,
    pub end_epoch: StorageSequence,
    pub relationships_inserted: u64,
    pub relationships_already_existed: u64,
    pub structural_edges_inserted: u64,
    pub structural_edges_already_existed: u64,
}

#[cfg(feature = "opencypher")]
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RelationshipDeleteBatchResult {
    pub deleted: u64,
    pub already_deleted: u64,
    pub topology_sequence: Option<StorageSequence>,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct BulkImportOptions {
    pub duplicate_policy: BulkImportDuplicatePolicy,
}

impl BulkImportOptions {
    pub fn trusted_append() -> Self {
        Self {
            duplicate_policy: BulkImportDuplicatePolicy::TrustNoExisting,
        }
    }

    pub fn checked_batch_append() -> Self {
        Self {
            duplicate_policy: BulkImportDuplicatePolicy::CheckExisting,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum BulkImportDuplicatePolicy {
    #[default]
    CheckExisting,
    TrustNoExisting,
}

impl BulkImportDuplicatePolicy {
    pub fn check_existing(self) -> bool {
        matches!(self, Self::CheckExisting)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EdgeMutationBatchResult {
    pub start_epoch: StorageSequence,
    pub end_epoch: StorageSequence,
    pub inserted: u64,
    pub already_existed: u64,
    pub results: Vec<CommitResult>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EdgeIngestOptions {
    pub batch_size: usize,
}

impl Default for EdgeIngestOptions {
    fn default() -> Self {
        Self { batch_size: 1_024 }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EdgeIngestResult {
    pub start_epoch: StorageSequence,
    pub end_epoch: StorageSequence,
    pub inserted: u64,
    pub already_existed: u64,
    pub batches: u64,
    pub mutations: u64,
}
