#![cfg(test)]

use std::{
    collections::{BTreeMap, HashMap},
    sync::{Arc, Mutex},
    time::Instant,
};

use datafusion::arrow::{
    array::{ArrayRef, BooleanArray, ListArray, MapArray, StringArray, StructArray, UInt64Array},
    buffer::{OffsetBuffer, ScalarBuffer},
    datatypes::{DataType, Field, SchemaRef},
    record_batch::RecordBatch,
};
use futures::stream;
use hydradb_graph_provider::{
    BatchBounds, BudgetLimits, BudgetTracker, CancellationToken, ExpansionRequest, GraphBatchStream, GraphProviderError, GraphReadView,
    GraphScope, NodeFetchRequest, NodeScanSpec, ProviderId, ReadRequest, RelationshipFetchRequest, RelationshipScanSpec, SeekSpec,
    SnapshotId, SnapshotIdentity,
};
use hydradb_graph_types::{
    canonical_expansion_schema, canonical_node_binding_schema, canonical_relationship_binding_schema, value_array, CypherValue, NodeId,
};

pub const TEST_BUDGET: BudgetLimits = BudgetLimits {
    max_batch_rows: 16,
    max_batch_bytes: 64 * 1024,
    max_total_rows: 256,
    max_total_bytes: 1024 * 1024,
    max_batches: 64,
    max_work_units: 1024,
};

pub struct FakeReadView {
    identity: SnapshotIdentity,
    budget: BudgetTracker,
    cancellation: CancellationToken,
    deadline: Option<Instant>,
    expansion_requests: Arc<Mutex<Vec<ExpansionRequest>>>,
    seek_requests: Arc<Mutex<Vec<SeekSpec>>>,
    node_fetch_requests: Arc<Mutex<Vec<NodeFetchRequest>>>,
    node_properties: Arc<Mutex<HashMap<u64, BTreeMap<String, CypherValue>>>>,
}

impl FakeReadView {
    pub fn for_read(request: &ReadRequest) -> Self {
        Self::new(
            request.scope.clone(),
            request.budget,
            request.cancellation.clone(),
            request.deadline,
        )
    }

    pub fn new(scope: GraphScope, budget: BudgetLimits, cancellation: CancellationToken, deadline: Option<Instant>) -> Self {
        Self {
            identity: SnapshotIdentity::new(
                ProviderId::new("test-provider").unwrap(),
                scope,
                SnapshotId::new(Arc::<[u8]>::from(&b"test-snapshot"[..])).unwrap(),
                Some(1),
            ),
            budget: budget.tracker(),
            cancellation,
            deadline,
            expansion_requests: Arc::new(Mutex::new(Vec::new())),
            seek_requests: Arc::new(Mutex::new(Vec::new())),
            node_fetch_requests: Arc::new(Mutex::new(Vec::new())),
            node_properties: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn expansion_requests(&self) -> Vec<ExpansionRequest> {
        self.expansion_requests.lock().unwrap().clone()
    }

    pub fn seek_requests(&self) -> Vec<SeekSpec> {
        self.seek_requests.lock().unwrap().clone()
    }

    pub fn node_fetch_requests(&self) -> Vec<NodeFetchRequest> {
        self.node_fetch_requests.lock().unwrap().clone()
    }

    pub fn set_node_property(&self, node_id: u64, key: impl Into<String>, value: CypherValue) {
        self.node_properties
            .lock()
            .unwrap()
            .entry(node_id)
            .or_insert_with(|| default_node_properties(node_id))
            .insert(key.into(), value);
    }

    fn stream(&self, schema: SchemaRef, batches: Vec<RecordBatch>) -> GraphBatchStream {
        let _ = schema;
        GraphBatchStream::new(
            stream::iter(batches.into_iter().map(Ok::<_, GraphProviderError>)),
            BatchBounds::from_budget(self.budget.limits()),
            self.budget.clone(),
            self.cancellation.clone(),
            self.deadline,
        )
    }

    fn empty(&self, schema: SchemaRef) -> GraphBatchStream {
        self.stream(schema, Vec::new())
    }
}

impl GraphReadView for FakeReadView {
    fn snapshot_identity(&self) -> &SnapshotIdentity {
        &self.identity
    }

    fn scan_nodes(&self, spec: NodeScanSpec) -> hydradb_graph_provider::Result<GraphBatchStream> {
        let ids = [1_u64, 2].into_iter().take(spec.page.limit).collect::<Vec<_>>();
        let batch = canonical_node_batch(&ids.iter().map(|id| Some(*id)).collect::<Vec<_>>(), &vec![true; ids.len()])?;
        Ok(self.stream(
            canonical_node_binding_schema(),
            if ids.is_empty() { Vec::new() } else { vec![batch] },
        ))
    }

    fn scan_relationships(&self, _spec: RelationshipScanSpec) -> hydradb_graph_provider::Result<GraphBatchStream> {
        Ok(self.empty(canonical_relationship_binding_schema()))
    }

    fn seek(&self, spec: SeekSpec) -> hydradb_graph_provider::Result<GraphBatchStream> {
        self.seek_requests.lock().unwrap().push(spec.clone());
        let matches = spec
            .keys
            .iter()
            .any(|key| matches!(key.values.first(), Some(CypherValue::Integer(20))));
        let ids = if matches { vec![1_u64] } else { Vec::new() };
        let mut batches = Vec::new();
        if !ids.is_empty() {
            let node = canonical_node_batch(&[Some(ids[0])], &[true])?;
            let ordinal = UInt64Array::from(vec![spec.keys.first().map_or(0, |key| key.input_ordinal)]);
            let schema = with_ordinal(&node.schema(), "input_ordinal");
            let mut columns = vec![Arc::new(ordinal) as ArrayRef];
            columns.extend(node.columns().iter().cloned());
            batches.push(RecordBatch::try_new(schema, columns).map_err(arrow_error)?);
        }
        Ok(self.stream(canonical_node_binding_schema(), batches))
    }

    fn expand_batch(&self, request: ExpansionRequest) -> hydradb_graph_provider::Result<GraphBatchStream> {
        self.expansion_requests.lock().unwrap().push(request.clone());
        let rows = request
            .inputs
            .iter()
            .filter(|input| input.source_node_id == NodeId(1) && input.target_node_id.is_none_or(|target| target == NodeId(2)))
            .map(|input| (input.input_ordinal, input.source_node_id.0, 2_u64))
            .collect::<Vec<_>>();
        if rows.is_empty() {
            return Ok(self.empty(canonical_expansion_schema()));
        }
        let schema = canonical_expansion_schema();
        let ordinals = Arc::new(UInt64Array::from(rows.iter().map(|row| row.0).collect::<Vec<_>>())) as ArrayRef;
        let sources = Arc::new(UInt64Array::from(rows.iter().map(|row| row.1).collect::<Vec<_>>())) as ArrayRef;
        let destinations = Arc::new(UInt64Array::from(rows.iter().map(|row| row.2).collect::<Vec<_>>())) as ArrayRef;
        let relationships = datafusion::arrow::array::new_null_array(schema.field(3).data_type(), rows.len());
        let paths = datafusion::arrow::array::new_null_array(schema.field(4).data_type(), rows.len());
        let batch =
            RecordBatch::try_new(schema.clone(), vec![ordinals, sources, destinations, relationships, paths]).map_err(arrow_error)?;
        Ok(self.stream(schema, vec![batch]))
    }

    fn fetch_nodes(&self, request: NodeFetchRequest) -> hydradb_graph_provider::Result<GraphBatchStream> {
        self.node_fetch_requests.lock().unwrap().push(request.clone());
        let rows = request
            .entities
            .iter()
            .map(|key| {
                let exists = matches!(key.node_id.0, 1 | 2);
                (key.input_ordinal, exists.then_some(key.node_id.0), exists)
            })
            .collect::<Vec<_>>();
        let ids = rows.iter().map(|row| row.1).collect::<Vec<_>>();
        let properties = {
            let overrides = self.node_properties.lock().unwrap();
            ids.iter()
                .map(|id| {
                    id.map(|id| overrides.get(&id).cloned().unwrap_or_else(|| default_node_properties(id)))
                        .unwrap_or_default()
                })
                .collect::<Vec<_>>()
        };
        let base = canonical_node_batch_with_properties(&ids, &rows.iter().map(|row| row.2).collect::<Vec<_>>(), &properties)?;
        if rows.is_empty() {
            return Ok(self.empty(with_ordinal(&base.schema(), "input_ordinal")));
        }
        let schema = with_ordinal(&base.schema(), "input_ordinal");
        let mut columns = vec![Arc::new(UInt64Array::from(rows.iter().map(|row| row.0).collect::<Vec<_>>())) as ArrayRef];
        columns.extend(base.columns().iter().cloned());
        Ok(self.stream(schema.clone(), vec![RecordBatch::try_new(schema, columns).map_err(arrow_error)?]))
    }

    fn fetch_relationships(&self, request: RelationshipFetchRequest) -> hydradb_graph_provider::Result<GraphBatchStream> {
        let base_schema = canonical_relationship_binding_schema();
        let schema = with_ordinal(&base_schema, "input_ordinal");
        if request.entities.is_empty() {
            return Ok(self.empty(schema));
        }
        let ordinals = Arc::new(UInt64Array::from(
            request.entities.iter().map(|key| key.input_ordinal).collect::<Vec<_>>(),
        )) as ArrayRef;
        let relationships = datafusion::arrow::array::new_null_array(base_schema.field(0).data_type(), request.entities.len());
        let exists = Arc::new(BooleanArray::from(vec![false; request.entities.len()])) as ArrayRef;
        let batch = RecordBatch::try_new(schema.clone(), vec![ordinals, relationships, exists]).map_err(arrow_error)?;
        Ok(self.stream(schema, vec![batch]))
    }
}

fn with_ordinal(schema: &SchemaRef, name: &str) -> SchemaRef {
    let mut fields = vec![Arc::new(Field::new(name, DataType::UInt64, false))];
    fields.extend(schema.fields().iter().cloned());
    Arc::new(datafusion::arrow::datatypes::Schema::new(fields))
}

fn canonical_node_batch(ids: &[Option<u64>], exists: &[bool]) -> hydradb_graph_provider::Result<RecordBatch> {
    let properties = ids
        .iter()
        .map(|id| id.map(default_node_properties).unwrap_or_default())
        .collect::<Vec<_>>();
    canonical_node_batch_with_properties(ids, exists, &properties)
}

fn canonical_node_batch_with_properties(
    ids: &[Option<u64>],
    exists: &[bool],
    properties: &[BTreeMap<String, CypherValue>],
) -> hydradb_graph_provider::Result<RecordBatch> {
    let schema = canonical_node_binding_schema();
    let id_array = Arc::new(UInt64Array::from(ids.to_vec())) as ArrayRef;
    let exists_array = Arc::new(BooleanArray::from(exists.to_vec())) as ArrayRef;
    let label_field = match schema.field(2).data_type() {
        DataType::List(field) => Arc::clone(field),
        _ => unreachable!(),
    };
    let labels = Arc::new(
        ListArray::try_new(
            label_field,
            OffsetBuffer::new(ScalarBuffer::from(vec![0_i32; ids.len() + 1])),
            Arc::new(StringArray::from(Vec::<String>::new())),
            None,
        )
        .map_err(arrow_error)?,
    ) as ArrayRef;
    let property_map = property_map(properties);
    RecordBatch::try_new(schema, vec![id_array, exists_array, labels, property_map]).map_err(arrow_error)
}

fn default_node_properties(id: u64) -> BTreeMap<String, CypherValue> {
    BTreeMap::from([("age".into(), CypherValue::Integer(if id == 1 { 20 } else { 30 }))])
}

fn property_map(rows: &[BTreeMap<String, CypherValue>]) -> ArrayRef {
    let schema = canonical_node_binding_schema();
    let DataType::Map(entry_field, ordered) = schema.field(3).data_type() else {
        unreachable!();
    };
    let DataType::Struct(fields) = entry_field.data_type() else {
        unreachable!();
    };
    let mut offsets = Vec::with_capacity(rows.len() + 1);
    let mut keys = Vec::new();
    let mut values = Vec::new();
    offsets.push(0_i32);
    for row in rows {
        for (key, value) in row {
            keys.push(key.as_str());
            values.push(value.clone());
        }
        offsets.push(keys.len() as i32);
    }
    let keys = Arc::new(StringArray::from(keys)) as ArrayRef;
    let values = Arc::new(value_array(&values).unwrap()) as ArrayRef;
    let entries = StructArray::try_new(fields.clone(), vec![keys, values], None).unwrap();
    let offsets = OffsetBuffer::new(ScalarBuffer::from(offsets));
    Arc::new(MapArray::try_new(Arc::clone(entry_field), offsets, entries, None, *ordered).unwrap())
}

fn arrow_error(error: datafusion::arrow::error::ArrowError) -> GraphProviderError {
    GraphProviderError::InvalidRequest(error.to_string())
}
