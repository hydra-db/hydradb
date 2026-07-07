use std::collections::{BTreeMap, BTreeSet};

use crate::{GraphEpoch, VertexId};
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct GraphRepairReport {
    pub cell_id: String,
    pub edge_type: String,
    pub read_epoch: GraphEpoch,
    pub live_edges: u64,
    pub delta_records: u64,
    pub degree_mismatches: Vec<String>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct GraphExportDigest {
    pub cell_id: String,
    pub edge_type: String,
    pub read_epoch: GraphEpoch,
    pub live_edges: u64,
    pub edge_checksum: u64,
    pub out_degree_checksum: u64,
    pub in_degree_checksum: u64,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct GraphCorrectnessReport {
    pub cell_id: String,
    pub edge_type: String,
    pub read_epoch: GraphEpoch,
    pub delta_gc_watermark: GraphEpoch,
    pub digest: GraphExportDigest,
    pub canonical_edges: u64,
    pub out_index_edges: u64,
    pub in_index_edges: u64,
    pub degree_counters: u64,
    pub posting_chunks_checked: u64,
    pub matrix_edges_checked: u64,
    pub supernode_groups_checked: u64,
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
    pub epoch: GraphEpoch,
}

#[cfg_attr(
    feature = "query-transport",
    derive(serde::Deserialize, serde::Serialize)
)]
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

#[cfg_attr(
    feature = "query-transport",
    derive(serde::Deserialize, serde::Serialize)
)]
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum VertexPropertyValue {
    Integer(u64),
    Bool(bool),
    Float(QueryFloat),
    String(String),
}

impl VertexPropertyValue {
    #[cfg_attr(
        not(any(feature = "json-properties", feature = "opencypher")),
        allow(dead_code)
    )]
    pub(crate) fn exact_u64_from_f64(value: f64) -> Option<u64> {
        const U64_EXCLUSIVE_UPPER: f64 = 18446744073709551616.0;
        if !value.is_finite()
            || !(0.0..U64_EXCLUSIVE_UPPER).contains(&value)
            || value.fract() != 0.0
        {
            return None;
        }
        let integer = value as u64;
        ((integer as f64) == value).then_some(integer)
    }

    #[cfg(feature = "opencypher")]
    pub(crate) fn exact_f64_from_u64(value: u64) -> Option<f64> {
        let float = value as f64;
        (Self::exact_u64_from_f64(float) == Some(value)).then_some(float)
    }

    #[cfg(feature = "json-properties")]
    pub fn from_json_value(value: &serde_json::Value) -> Self {
        match value {
            serde_json::Value::Bool(value) => Self::Bool(*value),
            serde_json::Value::Number(value) => {
                if let Some(value) = value.as_u64() {
                    Self::Integer(value)
                } else if let Some(value) = value.as_f64() {
                    Self::exact_u64_from_f64(value)
                        .map(Self::Integer)
                        .unwrap_or(Self::Float(QueryFloat(value)))
                } else {
                    Self::String(value.to_string())
                }
            }
            serde_json::Value::String(value) => Self::String(value.clone()),
            serde_json::Value::Null
            | serde_json::Value::Array(_)
            | serde_json::Value::Object(_) => Self::String(value.to_string()),
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct VertexMetadata {
    pub labels: BTreeSet<String>,
    pub properties: BTreeMap<String, VertexPropertyValue>,
}

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
pub(crate) struct OutEdgeSegment {
    pub(crate) cell_id: String,
    pub(crate) edge_type: String,
    pub(crate) src: VertexId,
    pub(crate) start_epoch: GraphEpoch,
    pub(crate) end_epoch: GraphEpoch,
    pub(crate) edges: Vec<(GraphEpoch, VertexId)>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CommitResult {
    pub epoch: GraphEpoch,
    pub already_existed: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeleteResult {
    pub epoch: GraphEpoch,
    pub deleted: bool,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SegmentCompactionResult {
    pub compacted_through_epoch: GraphEpoch,
    pub source_segments: u64,
    pub deleted_segment_keys: u64,
    pub deleted_tombstone_keys: u64,
    pub input_edges: u64,
    pub output_edges: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BulkImportResult {
    pub start_epoch: GraphEpoch,
    pub end_epoch: GraphEpoch,
    pub inserted: u64,
    pub already_existed: u64,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct BulkImportOptions {
    pub duplicate_policy: BulkImportDuplicatePolicy,
    pub delta_log_policy: BulkImportDeltaLogPolicy,
}

impl BulkImportOptions {
    pub fn trusted_append() -> Self {
        Self {
            duplicate_policy: BulkImportDuplicatePolicy::TrustNoExisting,
            delta_log_policy: BulkImportDeltaLogPolicy::Batch,
        }
    }

    pub fn checked_batch_append() -> Self {
        Self {
            duplicate_policy: BulkImportDuplicatePolicy::CheckExisting,
            delta_log_policy: BulkImportDeltaLogPolicy::Batch,
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
    pub(crate) fn check_existing(self) -> bool {
        matches!(self, Self::CheckExisting)
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum BulkImportDeltaLogPolicy {
    #[default]
    PerEdge,
    Batch,
}

impl BulkImportDeltaLogPolicy {
    pub(crate) fn write_per_edge(self) -> bool {
        matches!(self, Self::PerEdge)
    }

    pub(crate) fn write_batch(self) -> bool {
        matches!(self, Self::Batch)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EdgeMutationBatchResult {
    pub start_epoch: GraphEpoch,
    pub end_epoch: GraphEpoch,
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
    pub start_epoch: GraphEpoch,
    pub end_epoch: GraphEpoch,
    pub inserted: u64,
    pub already_existed: u64,
    pub batches: u64,
    pub mutations: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EdgeMutationLogAppendResult {
    pub log_epoch: GraphEpoch,
    pub mutations: u64,
    pub already_appended: bool,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct EdgeMutationLogMaterializeResult {
    pub scanned_batches: u64,
    pub materialized_batches: u64,
    pub mutations: u64,
    pub inserted: u64,
    pub already_existed: u64,
    pub last_log_epoch: GraphEpoch,
    pub materialized_log_epoch: GraphEpoch,
    pub current_epoch: GraphEpoch,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct EdgeMutationLogBatch {
    pub(crate) cell_id: String,
    pub(crate) batch_id: String,
    pub(crate) fingerprint: u64,
    pub(crate) mutations: Vec<EdgeMutation>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DeltaKind {
    Plus,
    Minus,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeltaRecord {
    pub kind: DeltaKind,
    pub edge: EdgeRecord,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct OutboxDeltaBatch {
    pub(crate) cell_id: String,
    pub(crate) edge_type: String,
    pub(crate) kind: DeltaKind,
    pub(crate) start_epoch: GraphEpoch,
    pub(crate) end_epoch: GraphEpoch,
    pub(crate) edges: Vec<(VertexId, VertexId)>,
}
