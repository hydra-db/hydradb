use std::{
    collections::{BTreeMap, HashSet},
    mem::size_of,
    sync::Arc,
    time::Instant,
};

use arrow_array::{
    builder::{ListBuilder, StringBuilder},
    new_null_array, ArrayRef, BooleanArray, RecordBatch, StringArray, StructArray, UInt64Array,
};
use arrow_buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use futures::stream;
use hydradb_graph_provider::{
    BatchBounds, EntityKind, ExpansionDepth, ExpansionRequest, GraphBatchStream, GraphProviderError, GraphReadView as ProviderReadView,
    MissingEntityPolicy, NodeFetchRequest, NodeScanSpec, PageRequest, Predicate, PredicateAcceptance, PreparedGraphScan,
    PropertyProjection, PropertySelection, RelationshipFetchRequest, RelationshipScanSpec, ScanAcceptance, ScanAccessPath, ScanRequest,
    ScanSource, SeekSpec, SnapshotIdentity,
};
use hydradb_graph_types::{
    canonical_expansion_schema, canonical_node_binding_schema, canonical_relationship_binding_schema, encode_value, semantic_eq,
    value_array, CypherValue, NodeId, RelationshipId, RelationshipIdentity,
};
use sha2::{Digest, Sha256};
use slatedb::{bytes::Bytes, DbIterator};

use crate::{
    codec::{decode_edge_metadata, decode_edge_record, decode_relationship_record, decode_u64, decode_vertex_metadata},
    keys, remote_read_options, remote_scan_options, DbTransaction, GraphError, GraphStorageSnapshot, RelationshipRecord, VertexId,
    VertexMetadata, VertexPropertyValue,
};

#[derive(Clone)]
pub struct SlateGraphReadView {
    cell_id: Arc<str>,
    snapshot: Arc<GraphStorageSnapshot>,
    transaction: Option<Arc<tokio::sync::Mutex<Option<DbTransaction>>>>,
    identity: SnapshotIdentity,
    index_policy: crate::GraphIndexPolicy,
    cancellation: hydradb_graph_provider::CancellationToken,
    deadline: Option<Instant>,
    budget: hydradb_graph_provider::BudgetTracker,
    max_batch_rows: u64,
    max_batch_bytes: u64,
    /// The cell's write-back catalog, read on the first node read that
    /// needs it and shared by every clone of this view, so a query scans the
    /// catalog once per view rather than once per row.
    write_back_catalog: Arc<tokio::sync::OnceCell<Arc<std::collections::BTreeSet<String>>>>,
}

struct SlatePreparedScan {
    view: SlateGraphReadView,
    request: ScanRequest,
    acceptance: ScanAcceptance,
}

impl PreparedGraphScan for SlatePreparedScan {
    fn acceptance(&self) -> &ScanAcceptance {
        &self.acceptance
    }

    fn execute(&self, page: PageRequest) -> hydradb_graph_provider::Result<GraphBatchStream> {
        match &self.request.source {
            ScanSource::Nodes { labels } => self.view.scan_nodes(NodeScanSpec {
                labels: labels.clone(),
                projection: self.request.projection.clone(),
                predicates: self.request.predicates.clone(),
                page,
            }),
            ScanSource::Relationships {
                relationship_types,
                direction,
                source_node_id,
                destination_node_id,
            } => self.view.scan_relationships(RelationshipScanSpec {
                relationship_types: relationship_types.clone(),
                direction: *direction,
                source_node_id: *source_node_id,
                destination_node_id: *destination_node_id,
                projection: self.request.projection.clone(),
                predicates: self.request.predicates.clone(),
                page,
            }),
            ScanSource::Seek { index, keys, internal_id } => self.view.seek(SeekSpec {
                index: index.clone(),
                keys: keys.clone(),
                internal_id: *internal_id,
                page,
            }),
        }
    }
}

impl SlateGraphReadView {
    pub fn new(
        snapshot: Arc<GraphStorageSnapshot>,
        identity: SnapshotIdentity,
        request: hydradb_graph_provider::ReadRequest,
        transaction: Option<Arc<tokio::sync::Mutex<Option<DbTransaction>>>>,
        index_policy: crate::GraphIndexPolicy,
    ) -> Self {
        let max_batch_rows = request.budget.max_batch_rows;
        let max_batch_bytes = request.budget.max_batch_bytes;
        let budget = request.budget_tracker();
        Self {
            cell_id: Arc::from(request.scope.graph()),
            snapshot,
            transaction,
            identity,
            index_policy,
            cancellation: request.cancellation,
            deadline: request.deadline,
            budget,
            max_batch_rows,
            max_batch_bytes,
            write_back_catalog: Arc::new(tokio::sync::OnceCell::new()),
        }
    }

    async fn write_back_catalog(&self) -> hydradb_graph_provider::Result<Arc<std::collections::BTreeSet<String>>> {
        self.write_back_catalog
            .get_or_try_init(|| async {
                let prefix = keys::vertex_write_back_generation_prefix(self.cell_id.as_ref());
                let entries = self.scan_prefix(prefix.as_bytes()).await?;
                crate::write::write_back_catalog_properties(entries, self.cell_id.as_ref())
                    .await
                    .map(Arc::new)
                    .map_err(provider_backend_error)
            })
            .await
            .cloned()
    }

    /// Refuse a node read this engine would answer wrongly: one that names a
    /// write-back property in its projection or predicates, or that asks for
    /// every property of a vertex in a cell holding any write-back property.
    /// The record and the classic index hold neither the values nor the
    /// index entries of such a property, so the answer would silently miss
    /// them rather than fail.
    async fn refuse_write_back_node_read(
        &self,
        projection: &PropertyProjection,
        predicates: &[Predicate],
    ) -> hydradb_graph_provider::Result<()> {
        let catalog = self.write_back_catalog().await?;
        if catalog.is_empty() {
            return Ok(());
        }
        let named = match &projection.properties {
            PropertySelection::None => None,
            PropertySelection::Keys(keys) => keys.iter().find(|key| catalog.contains(key.as_ref())).cloned(),
            PropertySelection::All => {
                return Err(GraphProviderError::InvalidRequest(
                    crate::write::datafusion_write_back_whole_vertex_refusal(self.cell_id.as_ref(), &catalog),
                ));
            }
        };
        let named = named.or_else(|| {
            predicates
                .iter()
                .find_map(|predicate| predicate_write_back_property(predicate, &catalog))
        });
        match named {
            Some(property) => Err(GraphProviderError::InvalidRequest(
                crate::write::datafusion_write_back_property_refusal(&property),
            )),
            None => Ok(()),
        }
    }

    /// Refuse a seek on a write-back property's index: the seek reads the
    /// classic index, which holds no entry for it.
    async fn refuse_write_back_node_seek(&self, property: &str) -> hydradb_graph_provider::Result<()> {
        if self.write_back_catalog().await?.contains(property) {
            return Err(GraphProviderError::InvalidRequest(
                crate::write::datafusion_write_back_property_refusal(property),
            ));
        }
        Ok(())
    }

    fn bounds(&self, page_limit: Option<usize>) -> BatchBounds {
        BatchBounds {
            max_rows: page_limit.map_or(self.max_batch_rows, |limit| self.max_batch_rows.min(limit as u64)),
            max_bytes: self.max_batch_bytes,
        }
    }

    fn check(&self) -> hydradb_graph_provider::Result<()> {
        if self.cancellation.is_cancelled() {
            return Err(GraphProviderError::Cancelled);
        }
        if self.deadline.is_some_and(|deadline| Instant::now() >= deadline) {
            return Err(GraphProviderError::DeadlineExceeded);
        }
        Ok(())
    }

    fn check_work(&self) -> hydradb_graph_provider::Result<()> {
        self.check()?;
        self.budget.consume_work_units(1)
    }

    async fn node_metadata(&self, node: VertexId) -> hydradb_graph_provider::Result<Option<VertexMetadata>> {
        self.check()?;
        let key = keys::vertex(self.cell_id.as_ref(), node);
        let value = self.get(key.as_bytes()).await?;
        value
            .as_ref()
            .map(|bytes| decode_vertex_metadata(&key, bytes).map_err(provider_backend_error))
            .transpose()
    }

    async fn node_metadata_for_seek(&self, node: VertexId) -> hydradb_graph_provider::Result<Option<VertexMetadata>> {
        self.check()?;
        let key = keys::vertex(self.cell_id.as_ref(), node);
        let value = self.get(key.as_bytes()).await?;
        value
            .as_ref()
            .map(|bytes| {
                self.budget.consume_retained_bytes(bytes.len() as u64)?;
                decode_vertex_metadata(&key, bytes).map_err(provider_backend_error)
            })
            .transpose()
    }

    async fn relationship_by_id(&self, id: RelationshipId) -> hydradb_graph_provider::Result<Option<RelationshipRecord>> {
        let key = keys::relationship_id(self.cell_id.as_ref(), id.0);
        let Some(record_key) = self.get(key.as_bytes()).await? else {
            return Ok(None);
        };
        let record_key = std::str::from_utf8(&record_key).map_err(|error| backend_error("invalid_relationship_key", error))?;
        let Some(value) = self.get(record_key.as_bytes()).await? else {
            return Ok(None);
        };
        decode_relationship_record(record_key, &value)
            .map(Some)
            .map_err(provider_backend_error)
    }

    async fn relationship_by_id_for_seek(&self, id: RelationshipId) -> hydradb_graph_provider::Result<Option<RelationshipRecord>> {
        let key = keys::relationship_id(self.cell_id.as_ref(), id.0);
        let Some(record_key) = self.get(key.as_bytes()).await? else {
            return Ok(None);
        };
        self.budget.consume_retained_bytes(record_key.len() as u64)?;
        let record_key = std::str::from_utf8(&record_key).map_err(|error| backend_error("invalid_relationship_key", error))?;
        let Some(value) = self.get(record_key.as_bytes()).await? else {
            return Ok(None);
        };
        self.budget.consume_retained_bytes(value.len() as u64)?;
        decode_relationship_record(record_key, &value)
            .map(Some)
            .map_err(provider_backend_error)
    }

    async fn get(&self, key: &[u8]) -> hydradb_graph_provider::Result<Option<Bytes>> {
        if let Some(transaction) = &self.transaction {
            let transaction = transaction.lock().await;
            if let Some(transaction) = transaction.as_ref() {
                let key = std::str::from_utf8(key).map_err(|error| backend_error("invalid_key", error))?;
                return crate::write::datafusion_read_value(transaction, key)
                    .await
                    .map_err(provider_backend_error);
            }
            return Err(GraphProviderError::SnapshotUnavailable);
        }
        self.snapshot
            .get_with_options(key, &remote_read_options())
            .await
            .map_err(provider_backend_error)
    }

    async fn scan_prefix(&self, prefix: &[u8]) -> hydradb_graph_provider::Result<DbIterator> {
        if let Some(transaction) = &self.transaction {
            let transaction = transaction.lock().await;
            if let Some(transaction) = transaction.as_ref() {
                return transaction.scan_prefix(prefix, ..).await.map_err(provider_backend_error);
            }
            return Err(GraphProviderError::SnapshotUnavailable);
        }
        self.snapshot
            .scan_prefix_with_options(prefix, .., &remote_scan_options())
            .await
            .map_err(provider_backend_error)
    }
}

#[async_trait::async_trait]
impl ProviderReadView for SlateGraphReadView {
    fn snapshot_identity(&self) -> &SnapshotIdentity {
        &self.identity
    }

    async fn prepare_scan(&self, request: ScanRequest) -> hydradb_graph_provider::Result<Box<dyn PreparedGraphScan + '_>> {
        self.check()?;
        if matches!(request.source, ScanSource::Seek { .. }) {
            return Err(GraphProviderError::UnsupportedCapability {
                capability: "prepared seek",
            });
        }
        let access_path = match &request.source {
            ScanSource::Nodes { .. } => ScanAccessPath::NodeScan,
            ScanSource::Relationships { .. } => ScanAccessPath::RelationshipScan,
            ScanSource::Seek { .. } => unreachable!(),
        };
        let acceptance = ScanAcceptance::new(
            &request,
            access_path,
            vec![PredicateAcceptance::Exact; request.predicates.len()],
            true,
            false,
            None,
        )?;
        Ok(Box::new(SlatePreparedScan {
            view: self.clone(),
            request,
            acceptance,
        }))
    }

    fn scan_nodes(&self, spec: NodeScanSpec) -> hydradb_graph_provider::Result<GraphBatchStream> {
        validate_page(&spec.page)?;
        let mut fingerprint_spec = spec.clone();
        fingerprint_spec.page.continuation = None;
        let last_key = continuation_position(&spec.page, &self.identity, "scan_nodes", &fingerprint_spec)?;
        let prefix = format!("cell/{}/vertex/", self.cell_id);
        let state = ScanState {
            iter: None,
            prefix,
            last_key,
            pending: None,
            emitted: 0,
            page_limit: spec.page.limit,
        };
        let labels = spec.labels;
        let predicates = spec.predicates;
        let projection = spec.projection;
        let stream = scan_stateful_stream(self.clone(), state, labels.clone(), predicates.clone(), projection.clone());
        Ok(GraphBatchStream::new(
            stream,
            self.bounds(Some(spec.page.limit)),
            self.budget.clone(),
            self.cancellation.clone(),
            self.deadline,
        ))
    }

    fn scan_relationships(&self, spec: RelationshipScanSpec) -> hydradb_graph_provider::Result<GraphBatchStream> {
        validate_page(&spec.page)?;
        let mut fingerprint_spec = spec.clone();
        fingerprint_spec.page.continuation = None;
        let last_key = continuation_position(&spec.page, &self.identity, "scan_relationships", &fingerprint_spec)?;
        let view = self.clone();
        let prefix = keys::relationship_cell_prefix(self.cell_id.as_ref());
        let state = RelationshipScanState {
            iter: None,
            prefix,
            last_key,
            pending: None,
            emitted: 0,
            page_limit: spec.page.limit,
        };
        let page_limit = state.page_limit;
        let stream = relationship_scan_stream(view, state, spec);
        Ok(GraphBatchStream::new(
            stream,
            self.bounds(Some(page_limit)),
            self.budget.clone(),
            self.cancellation.clone(),
            self.deadline,
        ))
    }

    fn seek(&self, spec: SeekSpec) -> hydradb_graph_provider::Result<GraphBatchStream> {
        // Property-index reads are exact on the persisted index; all results
        // are hydrated from this view and checked against the index value.
        validate_page(&spec.page)?;
        if spec.page.continuation.is_some() {
            return Err(GraphProviderError::UnsupportedCapability {
                capability: "seek continuation tokens",
            });
        }
        if spec.keys.iter().any(|key| key.values.len() != 1)
            || (!spec.internal_id && spec.index.properties.len() != 1)
            || (spec.internal_id && spec.index.entity != EntityKind::Node)
        {
            return Err(GraphProviderError::UnsupportedCapability {
                capability: "composite SlateDB seek",
            });
        }
        reject_native_property_seek(&spec)?;
        let view = self.clone();
        let stream = seek_stream(view.clone(), spec);
        Ok(GraphBatchStream::new(
            stream,
            self.bounds(None),
            self.budget.clone(),
            self.cancellation.clone(),
            self.deadline,
        ))
    }

    fn expand_batch(&self, request: ExpansionRequest) -> hydradb_graph_provider::Result<GraphBatchStream> {
        request.validate()?;
        if request.depth != ExpansionDepth::Fixed {
            return Err(GraphProviderError::UnsupportedCapability {
                capability: "variable SlateDB traversal",
            });
        }
        if request.path_mode != hydradb_graph_provider::PathMode::Endpoints {
            return Err(GraphProviderError::UnsupportedCapability {
                capability: "SlateDB path materialization",
            });
        }
        let view = self.clone();
        let stream = expansion_stream(view.clone(), request);
        Ok(GraphBatchStream::new(
            stream,
            self.bounds(None),
            self.budget.clone(),
            self.cancellation.clone(),
            self.deadline,
        ))
    }

    fn fetch_nodes(&self, request: NodeFetchRequest) -> hydradb_graph_provider::Result<GraphBatchStream> {
        let view = self.clone();
        let stream = stream::try_unfold(
            (view, request, 0_usize, None),
            |(view, request, mut offset, mut pending)| async move {
                if offset >= request.entities.len() && pending.is_none() {
                    return Ok(None);
                }
                if offset == 0 && pending.is_none() {
                    view.refuse_write_back_node_read(&request.projection, &[]).await?;
                }
                let mut rows = Vec::new();
                let mut bytes = BatchByteSizer::new(view.max_batch_bytes);
                while rows.len() < view.max_batch_rows as usize && (pending.is_some() || offset < request.entities.len()) {
                    let row = if let Some(row) = pending.take() {
                        row
                    } else {
                        let key = &request.entities[offset];
                        offset += 1;
                        view.check_work()?;
                        match view.node_metadata(key.node_id.0).await? {
                            Some(metadata) => NodeFetchRow {
                                ordinal: key.input_ordinal,
                                row: NodeRow::present(key.node_id.0, project_node(&metadata, &request.projection)),
                            },
                            None if request.missing == MissingEntityPolicy::Preserve => NodeFetchRow {
                                ordinal: key.input_ordinal,
                                row: NodeRow::missing(),
                            },
                            None => {
                                return Err(GraphProviderError::MissingEntity {
                                    input_ordinal: key.input_ordinal,
                                });
                            }
                        }
                    };
                    let row_bytes = estimated_node_row_bytes(&row.row)?;
                    if !bytes.try_add(row_bytes, rows.is_empty()) {
                        pending = Some(row);
                        break;
                    }
                    rows.push(row);
                }
                let batch = node_fetch_batch(&rows).map_err(|error| backend_error("arrow", error))?;
                Ok(Some((batch, (view, request, offset, pending))))
            },
        );
        Ok(GraphBatchStream::new(
            stream,
            self.bounds(None),
            self.budget.clone(),
            self.cancellation.clone(),
            self.deadline,
        ))
    }

    fn fetch_relationships(&self, request: RelationshipFetchRequest) -> hydradb_graph_provider::Result<GraphBatchStream> {
        let view = self.clone();
        let stream = stream::try_unfold(
            (view, request, 0_usize, None),
            |(view, request, mut offset, mut pending)| async move {
                if offset >= request.entities.len() && pending.is_none() {
                    return Ok(None);
                }
                let mut rows = Vec::new();
                let mut bytes = BatchByteSizer::new(view.max_batch_bytes);
                while rows.len() < view.max_batch_rows as usize && (pending.is_some() || offset < request.entities.len()) {
                    let row = if let Some(row) = pending.take() {
                        row
                    } else {
                        let key = &request.entities[offset];
                        offset += 1;
                        view.check_work()?;
                        match view.relationship_by_id(key.relationship_id).await? {
                            Some(record) => RelationshipFetchRow {
                                ordinal: key.input_ordinal,
                                row: Some(project_relationship(&record, &request.projection)),
                            },
                            None if request.missing == MissingEntityPolicy::Preserve => RelationshipFetchRow {
                                ordinal: key.input_ordinal,
                                row: None,
                            },
                            None => {
                                return Err(GraphProviderError::MissingEntity {
                                    input_ordinal: key.input_ordinal,
                                });
                            }
                        }
                    };
                    let row_bytes = match &row.row {
                        Some(row) => estimated_relationship_row_bytes(row)?,
                        None => 64,
                    };
                    if !bytes.try_add(row_bytes, rows.is_empty()) {
                        pending = Some(row);
                        break;
                    }
                    rows.push(row);
                }
                let batch = relationship_fetch_batch(&rows).map_err(|error| backend_error("arrow", error))?;
                Ok(Some((batch, (view, request, offset, pending))))
            },
        );
        Ok(GraphBatchStream::new(
            stream,
            self.bounds(None),
            self.budget.clone(),
            self.cancellation.clone(),
            self.deadline,
        ))
    }
}

/// The first write-back property a node predicate names, if any.
fn predicate_write_back_property(predicate: &Predicate, catalog: &std::collections::BTreeSet<String>) -> Option<Arc<str>> {
    match predicate {
        Predicate::And(predicates) | Predicate::Or(predicates) => predicates
            .iter()
            .find_map(|predicate| predicate_write_back_property(predicate, catalog)),
        Predicate::Not(predicate) => predicate_write_back_property(predicate, catalog),
        Predicate::PropertyExists(key) | Predicate::PropertyEquals { key, .. } => catalog.contains(key.as_ref()).then(|| Arc::clone(key)),
        Predicate::NodeIdEquals(_)
        | Predicate::RelationshipIdEquals(_)
        | Predicate::LabelEquals(_)
        | Predicate::RelationshipTypeEquals(_) => None,
    }
}

fn reject_native_property_seek(spec: &SeekSpec) -> hydradb_graph_provider::Result<()> {
    if !spec.internal_id
        && spec
            .keys
            .iter()
            .flat_map(|key| &key.values)
            .any(|value| matches!(value, CypherValue::Date(_) | CypherValue::List(_)))
    {
        return Err(GraphProviderError::UnsupportedCapability {
            capability: "native property seek",
        });
    }
    Ok(())
}

struct ScanState {
    iter: Option<DbIterator>,
    prefix: String,
    last_key: Option<Vec<u8>>,
    pending: Option<(Vec<u8>, NodeRow)>,
    emitted: usize,
    page_limit: usize,
}

fn scan_stateful_stream(
    view: SlateGraphReadView,
    state: ScanState,
    labels: Vec<Arc<str>>,
    predicates: Vec<Predicate>,
    projection: PropertyProjection,
) -> impl futures::Stream<Item = hydradb_graph_provider::Result<RecordBatch>> + Send + 'static {
    stream::try_unfold(Some(state), move |state| {
        let view = view.clone();
        let labels = labels.clone();
        let predicates = predicates.clone();
        let projection = projection.clone();
        async move {
            let Some(mut cursor) = state else { return Ok(None) };
            let batch_rows = view.max_batch_rows.min(cursor.page_limit.saturating_sub(cursor.emitted) as u64) as usize;
            if batch_rows == 0 {
                return Ok(None);
            }
            if cursor.iter.is_none() {
                view.refuse_write_back_node_read(&projection, &predicates).await?;
                cursor.iter = Some(view.scan_prefix(cursor.prefix.as_bytes()).await?);
            }
            let mut output = Vec::with_capacity(batch_rows);
            let mut bytes = BatchByteSizer::new(view.max_batch_bytes);
            if let Some((key, row)) = cursor.pending.take() {
                bytes.try_add(estimated_node_row_bytes(&row)?, true);
                cursor.last_key = Some(key);
                output.push(row);
            }
            while output.len() < batch_rows {
                view.check()?;
                let next = cursor
                    .iter
                    .as_mut()
                    .expect("opened above")
                    .next()
                    .await
                    .map_err(provider_backend_error)?;
                let Some(kv) = next else { break };
                view.check_work()?;
                if cursor.last_key.as_ref().is_some_and(|last| kv.key.as_ref() <= last.as_slice()) {
                    continue;
                }
                let key = String::from_utf8_lossy(&kv.key).into_owned();
                let suffix = key
                    .strip_prefix(&cursor.prefix)
                    .ok_or_else(|| backend_error("invalid_vertex_key", "vertex key is outside its scan prefix"))?;
                let node_id = suffix.parse::<u64>().map_err(|error| backend_error("invalid_vertex_id", error))?;
                let metadata = decode_vertex_metadata(&key, &kv.value).map_err(provider_backend_error)?;
                if !labels.iter().all(|label| metadata.labels.contains(label.as_ref()))
                    || !predicates
                        .iter()
                        .all(|predicate| node_predicate(predicate, NodeId(node_id), &metadata))
                {
                    continue;
                }
                let row = NodeRow::present(node_id, project_node(&metadata, &projection));
                if !bytes.try_add(estimated_node_row_bytes(&row)?, output.is_empty()) {
                    cursor.pending = Some((kv.key.to_vec(), row));
                    break;
                }
                cursor.last_key = Some(kv.key.to_vec());
                output.push(row);
            }
            if output.is_empty() {
                return Ok(None);
            }
            cursor.emitted += output.len();
            let batch = node_batch(&output).map_err(|error| backend_error("arrow", error))?;
            Ok(Some((batch, Some(cursor))))
        }
    })
}

struct RelationshipScanState {
    iter: Option<DbIterator>,
    prefix: String,
    last_key: Option<Vec<u8>>,
    pending: Option<(Vec<u8>, RelationshipRow)>,
    emitted: usize,
    page_limit: usize,
}

fn relationship_scan_stream(
    view: SlateGraphReadView,
    state: RelationshipScanState,
    spec: RelationshipScanSpec,
) -> impl futures::Stream<Item = hydradb_graph_provider::Result<RecordBatch>> + Send + 'static {
    stream::try_unfold(Some(state), move |state| {
        let view = view.clone();
        let spec = spec.clone();
        async move {
            let Some(mut cursor) = state else { return Ok(None) };
            let max_rows = view.max_batch_rows.min(cursor.page_limit.saturating_sub(cursor.emitted) as u64) as usize;
            if max_rows == 0 {
                return Ok(None);
            }
            if cursor.iter.is_none() {
                cursor.iter = Some(view.scan_prefix(cursor.prefix.as_bytes()).await?);
            }
            let mut rows = Vec::with_capacity(max_rows);
            let mut bytes = BatchByteSizer::new(view.max_batch_bytes);
            if let Some((key, row)) = cursor.pending.take() {
                bytes.try_add(estimated_relationship_row_bytes(&row)?, true);
                cursor.last_key = Some(key);
                rows.push(row);
            }
            while rows.len() < max_rows {
                view.check()?;
                let Some(kv) = cursor
                    .iter
                    .as_mut()
                    .expect("opened above")
                    .next()
                    .await
                    .map_err(provider_backend_error)?
                else {
                    break;
                };
                view.check_work()?;
                if cursor.last_key.as_ref().is_some_and(|last| kv.key.as_ref() <= last.as_slice()) {
                    continue;
                }
                let key = String::from_utf8_lossy(&kv.key).into_owned();
                let record = decode_relationship_record(&key, &kv.value).map_err(provider_backend_error)?;
                if !spec.relationship_types.is_empty() && !spec.relationship_types.iter().any(|ty| ty.as_ref() == record.edge_type) {
                    continue;
                }
                if spec.source_node_id.is_some_and(|id| id.0 != record.src) || spec.destination_node_id.is_some_and(|id| id.0 != record.dst)
                {
                    continue;
                }
                if !spec.predicates.iter().all(|predicate| relationship_predicate(predicate, &record)) {
                    continue;
                }
                let row = project_relationship(&record, &spec.projection);
                if !bytes.try_add(estimated_relationship_row_bytes(&row)?, rows.is_empty()) {
                    cursor.pending = Some((kv.key.to_vec(), row));
                    break;
                }
                cursor.last_key = Some(kv.key.to_vec());
                rows.push(row);
            }
            if rows.is_empty() {
                return Ok(None);
            }
            cursor.emitted += rows.len();
            let batch = relationship_batch(&rows).map_err(|error| backend_error("arrow", error))?;
            Ok(Some((batch, Some(cursor))))
        }
    })
}

#[derive(Clone)]
struct NodeRow {
    id: Option<u64>,
    exists: bool,
    labels: Vec<String>,
    properties: BTreeMap<String, CypherValue>,
}

impl NodeRow {
    fn present(id: u64, metadata: VertexMetadata) -> Self {
        Self {
            id: Some(id),
            exists: true,
            labels: metadata.labels.into_iter().collect(),
            properties: metadata
                .properties
                .into_iter()
                .map(|(key, value)| (key, crate::property_to_cypher(&value)))
                .collect(),
        }
    }

    fn missing() -> Self {
        Self {
            id: None,
            exists: false,
            labels: Vec::new(),
            properties: BTreeMap::new(),
        }
    }
}

struct NodeFetchRow {
    ordinal: u64,
    row: NodeRow,
}

#[derive(Clone)]
struct RelationshipRow {
    source: u64,
    destination: u64,
    relationship_type: String,
    relationship_id: Option<u64>,
    properties: BTreeMap<String, CypherValue>,
}

struct RelationshipFetchRow {
    ordinal: u64,
    row: Option<RelationshipRow>,
}

struct BatchByteSizer {
    estimated: u64,
    maximum: u64,
}

impl BatchByteSizer {
    fn new(maximum: u64) -> Self {
        Self {
            estimated: (16 * 1024).min(maximum),
            maximum,
        }
    }

    fn try_add(&mut self, bytes: u64, empty: bool) -> bool {
        let next = self.estimated.saturating_add(bytes);
        if !empty && next > self.maximum {
            return false;
        }
        self.estimated = next;
        true
    }
}

fn estimated_node_row_bytes(row: &NodeRow) -> hydradb_graph_provider::Result<u64> {
    let labels = row
        .labels
        .iter()
        .fold(0_u64, |total, label| total.saturating_add(label.len() as u64));
    Ok(256_u64
        .saturating_add(labels)
        .saturating_add(estimated_property_bytes(&row.properties)?))
}

fn estimated_relationship_row_bytes(row: &RelationshipRow) -> hydradb_graph_provider::Result<u64> {
    Ok(256_u64
        .saturating_add(row.relationship_type.len() as u64)
        .saturating_add(estimated_property_bytes(&row.properties)?))
}

fn estimated_property_bytes(properties: &BTreeMap<String, CypherValue>) -> hydradb_graph_provider::Result<u64> {
    properties.iter().try_fold(0_u64, |total, (key, value)| {
        let value_bytes = match value {
            CypherValue::String(value) => value.len() as u64,
            value => encode_value(value)
                .map(|encoded| encoded.len() as u64)
                .map_err(|error| backend_error("value_encode", error))?,
        };
        Ok(total
            .saturating_add(64)
            .saturating_add(key.len() as u64)
            .saturating_add(value_bytes))
    })
}

fn project_node(metadata: &VertexMetadata, projection: &PropertyProjection) -> VertexMetadata {
    let labels = if projection.include_labels {
        metadata.labels.clone()
    } else {
        Default::default()
    };
    let properties = project_properties(&metadata.properties, &projection.properties);
    VertexMetadata { labels, properties }
}

fn project_relationship(record: &RelationshipRecord, projection: &PropertyProjection) -> RelationshipRow {
    let properties = project_properties(&record.metadata.properties, &projection.properties);
    RelationshipRow {
        source: record.src,
        destination: record.dst,
        relationship_type: record.edge_type.clone(),
        relationship_id: Some(record.relationship_id),
        properties: properties
            .into_iter()
            .map(|(key, value)| (key, crate::property_to_cypher(&value)))
            .collect(),
    }
}

fn project_properties(
    properties: &BTreeMap<String, VertexPropertyValue>,
    selection: &PropertySelection,
) -> BTreeMap<String, VertexPropertyValue> {
    match selection {
        PropertySelection::None => BTreeMap::new(),
        PropertySelection::All => properties.clone(),
        PropertySelection::Keys(keys) => keys
            .iter()
            .filter_map(|key| properties.get(key.as_ref()).map(|value| (key.to_string(), value.clone())))
            .collect(),
    }
}

fn node_predicate(predicate: &Predicate, id: NodeId, metadata: &VertexMetadata) -> bool {
    match predicate {
        Predicate::And(predicates) => predicates.iter().all(|predicate| node_predicate(predicate, id, metadata)),
        Predicate::Or(predicates) => predicates.iter().any(|predicate| node_predicate(predicate, id, metadata)),
        Predicate::Not(predicate) => !node_predicate(predicate, id, metadata),
        Predicate::NodeIdEquals(expected) => *expected == id,
        Predicate::LabelEquals(label) => metadata.labels.contains(label.as_ref()),
        Predicate::PropertyExists(key) => metadata.properties.contains_key(key.as_ref()),
        Predicate::PropertyEquals { key, value } => metadata
            .properties
            .get(key.as_ref())
            .is_some_and(|actual| semantic_eq(&crate::property_to_cypher(actual), value)),
        Predicate::RelationshipIdEquals(_) | Predicate::RelationshipTypeEquals(_) => false,
    }
}

fn relationship_predicate(predicate: &Predicate, record: &RelationshipRecord) -> bool {
    match predicate {
        Predicate::And(predicates) => predicates.iter().all(|predicate| relationship_predicate(predicate, record)),
        Predicate::Or(predicates) => predicates.iter().any(|predicate| relationship_predicate(predicate, record)),
        Predicate::Not(predicate) => !relationship_predicate(predicate, record),
        Predicate::RelationshipIdEquals(id) => id.0 == record.relationship_id,
        Predicate::RelationshipTypeEquals(ty) => ty.as_ref() == record.edge_type,
        Predicate::PropertyExists(key) => record.metadata.properties.contains_key(key.as_ref()),
        Predicate::PropertyEquals { key, value } => record
            .metadata
            .properties
            .get(key.as_ref())
            .is_some_and(|actual| semantic_eq(&crate::property_to_cypher(actual), value)),
        Predicate::NodeIdEquals(_) | Predicate::LabelEquals(_) => false,
    }
}

fn node_batch(rows: &[NodeRow]) -> Result<RecordBatch, String> {
    let schema = canonical_node_binding_schema();
    let ids = UInt64Array::from(rows.iter().map(|row| row.id).collect::<Vec<_>>());
    let exists = BooleanArray::from(rows.iter().map(|row| row.exists).collect::<Vec<_>>());
    let label_count = rows.iter().map(|row| row.labels.len()).sum();
    let label_bytes = rows.iter().flat_map(|row| &row.labels).map(String::len).sum();
    let mut labels = ListBuilder::with_capacity(StringBuilder::with_capacity(label_count, label_bytes), rows.len())
        .with_field(Arc::new(Field::new("item", DataType::Utf8, false)));
    for row in rows {
        for label in &row.labels {
            labels.values().append_value(label);
        }
        labels.append(true);
    }
    let properties = property_map_array(&rows.iter().map(|row| row.properties.clone()).collect::<Vec<_>>(), schema.field(3))?;
    RecordBatch::try_new(
        schema,
        vec![Arc::new(ids), Arc::new(exists), Arc::new(labels.finish()), Arc::new(properties)],
    )
    .map_err(|error| error.to_string())
}

fn node_fetch_batch(rows: &[NodeFetchRow]) -> Result<RecordBatch, String> {
    let base = node_batch(&rows.iter().map(|row| row.row.clone()).collect::<Vec<_>>())?;
    let schema = with_ordinal(&base.schema(), "input_ordinal");
    let mut columns: Vec<ArrayRef> = vec![Arc::new(UInt64Array::from(rows.iter().map(|row| row.ordinal).collect::<Vec<_>>()))];
    columns.extend(base.columns().iter().cloned());
    RecordBatch::try_new(schema, columns).map_err(|error| error.to_string())
}

fn relationship_batch(rows: &[RelationshipRow]) -> Result<RecordBatch, String> {
    let schema = canonical_relationship_binding_schema();
    let rel = relationship_struct(rows.iter().map(Some).collect::<Vec<_>>(), schema.field(0).data_type())?;
    let exists = BooleanArray::from(vec![true; rows.len()]);
    RecordBatch::try_new(schema, vec![Arc::new(rel), Arc::new(exists)]).map_err(|error| error.to_string())
}

fn relationship_fetch_batch(rows: &[RelationshipFetchRow]) -> Result<RecordBatch, String> {
    let schema = canonical_relationship_binding_schema();
    let rel = relationship_struct(rows.iter().map(|row| row.row.as_ref()).collect(), schema.field(0).data_type())?;
    let exists = BooleanArray::from(rows.iter().map(|row| row.row.is_some()).collect::<Vec<_>>());
    let base = RecordBatch::try_new(schema, vec![Arc::new(rel), Arc::new(exists)]).map_err(|error| error.to_string())?;
    let schema = with_ordinal(&base.schema(), "input_ordinal");
    let mut columns: Vec<ArrayRef> = vec![Arc::new(UInt64Array::from(rows.iter().map(|row| row.ordinal).collect::<Vec<_>>()))];
    columns.extend(base.columns().iter().cloned());
    RecordBatch::try_new(schema, columns).map_err(|error| error.to_string())
}

fn relationship_struct(rows: Vec<Option<&RelationshipRow>>, dtype: &DataType) -> Result<StructArray, String> {
    let DataType::Struct(fields) = dtype else {
        return Err("relationship field was not a struct".into());
    };
    let properties = property_map_array(
        &rows
            .iter()
            .map(|row| row.map_or_else(BTreeMap::new, |row| row.properties.clone()))
            .collect::<Vec<_>>(),
        &fields[5],
    )?;
    let record_ids = UInt64Array::from(rows.iter().map(|row| row.and_then(|row| row.relationship_id)).collect::<Vec<_>>());
    let source = UInt64Array::from(rows.iter().map(|row| row.map(|row| row.source)).collect::<Vec<_>>());
    let destination = UInt64Array::from(rows.iter().map(|row| row.map(|row| row.destination)).collect::<Vec<_>>());
    let relationship_type = StringArray::from(
        rows.iter()
            .map(|row| row.map(|row| row.relationship_type.as_str()))
            .collect::<Vec<_>>(),
    );
    let kind = StringArray::from(
        rows.iter()
            .map(|row| {
                row.map(|row| {
                    if row.relationship_id.is_some() {
                        "identified"
                    } else {
                        "structural"
                    }
                })
            })
            .collect::<Vec<_>>(),
    );
    let validity = NullBuffer::from(rows.iter().map(Option::is_some).collect::<Vec<_>>());
    StructArray::try_new(
        fields.clone(),
        vec![
            Arc::new(source),
            Arc::new(destination),
            Arc::new(relationship_type),
            Arc::new(kind),
            Arc::new(record_ids),
            Arc::new(properties),
        ],
        Some(validity),
    )
    .map_err(|error| error.to_string())
}

fn property_map_array(rows: &[BTreeMap<String, CypherValue>], field: &Field) -> Result<arrow_array::MapArray, String> {
    let DataType::Map(entries_field, ordered) = field.data_type() else {
        return Err("property field was not a map".into());
    };
    let DataType::Struct(entry_fields) = entries_field.data_type() else {
        return Err("map entry field was not a struct".into());
    };
    let mut keys = Vec::new();
    let mut values = Vec::new();
    let mut offsets = Vec::with_capacity(rows.len() + 1);
    offsets.push(0_i32);
    for row in rows {
        for (key, value) in row {
            keys.push(key.as_str());
            values.push(value.clone());
        }
        offsets.push(i32::try_from(keys.len()).map_err(|error| error.to_string())?);
    }
    let canonical = value_array(&values).map_err(|error| error.to_string())?;
    let entries = StructArray::try_new(
        entry_fields.clone(),
        vec![Arc::new(StringArray::from(keys)), Arc::new(canonical)],
        None,
    )
    .map_err(|error| error.to_string())?;
    arrow_array::MapArray::try_new(
        entries_field.clone(),
        OffsetBuffer::new(ScalarBuffer::from(offsets)),
        entries,
        None,
        *ordered,
    )
    .map_err(|error| error.to_string())
}

fn with_ordinal(schema: &SchemaRef, name: &str) -> SchemaRef {
    let mut fields = vec![Field::new(name, DataType::UInt64, false)];
    fields.extend(schema.fields().iter().map(|field| field.as_ref().clone()));
    Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone()))
}

fn validate_page(page: &PageRequest) -> hydradb_graph_provider::Result<()> {
    if page.limit == 0 {
        return Err(GraphProviderError::InvalidRequest("page size must be non-zero".into()));
    }
    Ok(())
}

fn continuation_position<T: std::fmt::Debug>(
    page: &PageRequest,
    snapshot: &SnapshotIdentity,
    operator: &str,
    spec: &T,
) -> hydradb_graph_provider::Result<Option<Vec<u8>>> {
    let Some(token) = &page.continuation else { return Ok(None) };
    let mut hash = Sha256::new();
    hash.update(operator.as_bytes());
    hash.update(format!("{spec:?}").as_bytes());
    let fingerprint: [u8; 32] = hash.finalize().into();
    token.validate_for(snapshot, operator, &fingerprint)?;
    Ok(Some(token.position().to_vec()))
}

pub(super) fn backend_error(code: &str, error: impl std::fmt::Display) -> GraphProviderError {
    GraphProviderError::Backend {
        code: code.to_owned(),
        message: error.to_string(),
        retryable: false,
    }
}

pub(super) fn provider_backend_error(error: impl Into<GraphError>) -> GraphProviderError {
    match error.into() {
        GraphError::DataFusionProvider(error) => error,
        GraphError::ConditionalWriteConflict { .. } => GraphProviderError::Conflict { retryable: true },
        GraphError::Slate(error) if error.kind() == slatedb::ErrorKind::Transaction => GraphProviderError::Conflict { retryable: true },
        GraphError::DurabilityUnknown { sequence, message } => GraphProviderError::DurabilityUnknown {
            sequence: Some(sequence),
            message,
        },
        error => backend_error("slatedb", error),
    }
}

fn seek_stream(
    view: SlateGraphReadView,
    spec: SeekSpec,
) -> impl futures::Stream<Item = hydradb_graph_provider::Result<RecordBatch>> + Send + 'static {
    let state = SeekState {
        view,
        spec,
        key_index: 0,
        encodings: Vec::new(),
        encoding_index: 0,
        iter: None,
        emitted: 0,
    };
    stream::try_unfold(state, |mut state| async move {
        let batch_rows = usize::try_from(state.view.max_batch_rows)
            .unwrap_or(usize::MAX)
            .min(state.spec.page.limit.saturating_sub(state.emitted));
        if batch_rows == 0 {
            return Ok(None);
        }
        let batch = match state.spec.index.entity {
            EntityKind::Node => {
                let mut rows = Vec::new();
                while rows.len() < batch_rows && state.advance_node_seek(&mut rows).await? {}
                if rows.is_empty() {
                    return Ok(None);
                }
                seek_node_batch(&rows).map_err(|error| backend_error("arrow", error))?
            }
            EntityKind::Relationship => {
                let mut rows = Vec::new();
                while rows.len() < batch_rows && state.advance_relationship_seek(&mut rows).await? {}
                if rows.is_empty() {
                    return Ok(None);
                }
                seek_relationship_batch(&rows).map_err(|error| backend_error("arrow", error))?
            }
        };
        state.emitted = state.emitted.saturating_add(batch.num_rows());
        Ok(Some((batch, state)))
    })
}

struct SeekState {
    view: SlateGraphReadView,
    spec: SeekSpec,
    key_index: usize,
    encodings: Vec<String>,
    encoding_index: usize,
    iter: Option<DbIterator>,
    emitted: usize,
}

impl SeekState {
    async fn advance_node_seek(&mut self, rows: &mut Vec<(u64, NodeRow)>) -> hydradb_graph_provider::Result<bool> {
        self.view.check()?;
        if self.key_index >= self.spec.keys.len() {
            return Ok(false);
        }
        if self.spec.internal_id {
            self.view.check_work()?;
            let (input_ordinal, id) = {
                let seek_key = &self.spec.keys[self.key_index];
                let CypherValue::Integer(id) = seek_key.values[0] else {
                    return Err(GraphProviderError::InvalidRequest(
                        "internal node identity seek requires an integer".into(),
                    ));
                };
                (seek_key.input_ordinal, id)
            };
            self.key_index += 1;
            let id =
                u64::try_from(id).map_err(|_| GraphProviderError::InvalidRequest("internal node identity must be non-negative".into()))?;
            if self.view.node_metadata_for_seek(id).await?.is_some() {
                reserve_seek_row(&self.view, rows)?;
                rows.push((input_ordinal, NodeRow::present(id, VertexMetadata::default())));
            }
            return Ok(true);
        }
        self.prepare_encoding()?;
        let seek_key = &self.spec.keys[self.key_index];
        if self.iter.is_none() {
            if self.encoding_index >= self.encodings.len() {
                self.finish_seek_key();
                return Ok(true);
            }
            self.view
                .refuse_write_back_node_seek(self.spec.index.properties[0].as_ref())
                .await?;
            let prefix = keys::vertex_property_index_prefix(
                self.view.cell_id.as_ref(),
                self.spec.index.properties[0].as_ref(),
                &self.encodings[self.encoding_index],
            );
            self.iter = Some(self.view.scan_prefix(prefix.as_bytes()).await?);
        }
        let next = self
            .iter
            .as_mut()
            .expect("opened above")
            .next()
            .await
            .map_err(provider_backend_error)?;
        let Some(kv) = next else {
            self.iter = None;
            self.encoding_index += 1;
            return Ok(true);
        };
        self.view.check_work()?;
        let key = String::from_utf8_lossy(&kv.key).into_owned();
        let id = decode_u64(&key, &kv.value).map_err(provider_backend_error)?;
        let Some(metadata) = self.view.node_metadata_for_seek(id).await? else {
            return Ok(true);
        };
        let value = &seek_key.values[0];
        if !metadata
            .properties
            .get(self.spec.index.properties[0].as_ref())
            .is_some_and(|actual| semantic_eq(&crate::property_to_cypher(actual), value))
            || (!self.spec.index.token.is_empty() && !metadata.labels.contains(self.spec.index.token.as_ref()))
        {
            return Ok(true);
        }
        reserve_seek_row(&self.view, rows)?;
        rows.push((seek_key.input_ordinal, NodeRow::present(id, VertexMetadata::default())));
        Ok(true)
    }

    async fn advance_relationship_seek(&mut self, rows: &mut Vec<(u64, RelationshipRow)>) -> hydradb_graph_provider::Result<bool> {
        self.view.check()?;
        if self.key_index >= self.spec.keys.len() {
            return Ok(false);
        }
        if self.spec.internal_id {
            return Err(GraphProviderError::UnsupportedCapability {
                capability: "internal relationship identity seek",
            });
        }
        self.prepare_encoding()?;
        let seek_key = &self.spec.keys[self.key_index];
        if self.iter.is_none() {
            if self.encoding_index >= self.encodings.len() {
                self.finish_seek_key();
                return Ok(true);
            }
            let prefix = format!(
                "cell/{}/rprop_idx/{}/{}/{}/",
                self.view.cell_id, self.spec.index.token, self.spec.index.properties[0], self.encodings[self.encoding_index]
            );
            self.iter = Some(self.view.scan_prefix(prefix.as_bytes()).await?);
        }
        let next = self
            .iter
            .as_mut()
            .expect("opened above")
            .next()
            .await
            .map_err(provider_backend_error)?;
        let Some(kv) = next else {
            self.iter = None;
            self.encoding_index += 1;
            return Ok(true);
        };
        self.view.check_work()?;
        let key = String::from_utf8_lossy(&kv.key).into_owned();
        let rel_id = decode_u64(&key, &kv.value).map_err(provider_backend_error)?;
        let Some(record) = self.view.relationship_by_id_for_seek(RelationshipId(rel_id)).await? else {
            return Ok(true);
        };
        let value = &seek_key.values[0];
        if !record
            .metadata
            .properties
            .get(self.spec.index.properties[0].as_ref())
            .is_some_and(|actual| semantic_eq(&crate::property_to_cypher(actual), value))
        {
            return Ok(true);
        }
        self.view
            .budget
            .consume_retained_bytes(u64::try_from(record.edge_type.len()).unwrap_or(u64::MAX))?;
        reserve_seek_row(&self.view, rows)?;
        rows.push((
            seek_key.input_ordinal,
            RelationshipRow {
                source: record.src,
                destination: record.dst,
                relationship_type: record.edge_type,
                relationship_id: Some(record.relationship_id),
                properties: BTreeMap::new(),
            },
        ));
        Ok(true)
    }

    fn prepare_encoding(&mut self) -> hydradb_graph_provider::Result<()> {
        if self.encodings.is_empty() {
            self.view.check_work()?;
            self.encodings = semantic_seek_encodings(&self.spec.keys[self.key_index].values[0])?;
            self.encoding_index = 0;
        }
        Ok(())
    }

    fn finish_seek_key(&mut self) {
        self.key_index += 1;
        self.encodings.clear();
        self.encoding_index = 0;
        self.iter = None;
    }
}

fn reserve_seek_row<T>(view: &SlateGraphReadView, rows: &mut Vec<T>) -> hydradb_graph_provider::Result<()> {
    if rows.len() == rows.capacity() {
        let capacity_limit = usize::try_from(view.max_batch_rows).unwrap_or(usize::MAX);
        let next_capacity = rows.capacity().max(1).saturating_mul(2).min(capacity_limit);
        let additional = next_capacity.saturating_sub(rows.capacity()).max(1);
        let additional_bytes = additional
            .checked_mul(size_of::<T>())
            .and_then(|bytes| u64::try_from(bytes).ok())
            .unwrap_or(u64::MAX);
        view.budget.consume_retained_bytes(additional_bytes)?;
        rows.try_reserve_exact(additional)
            .map_err(|error| backend_error("allocation", error))?;
    }
    Ok(())
}

// Indexes preserve storage types; Cypher equality does not. Probe every exact
// scalar representation, including legacy signed-positive integers and ±0.0.
fn semantic_seek_encodings(value: &CypherValue) -> hydradb_graph_provider::Result<Vec<String>> {
    let mut candidates = vec![storage_property(value)?];
    let integer = match value {
        CypherValue::Integer(integer) => Some(*integer),
        CypherValue::Float(float) if float.is_finite() => {
            let integer = *float as i128;
            semantic_eq(value, &CypherValue::Integer(integer)).then_some(integer)
        }
        _ => None,
    };
    if let Some(integer) = integer {
        if let Ok(unsigned) = u64::try_from(integer) {
            candidates.push(VertexPropertyValue::Integer(unsigned));
        }
        if let Ok(signed) = i64::try_from(integer) {
            candidates.push(VertexPropertyValue::SignedInteger(signed));
        }
        let float = integer as f64;
        if semantic_eq(value, &CypherValue::Float(float)) {
            candidates.push(VertexPropertyValue::Float(crate::QueryFloat(float)));
            if integer == 0 {
                candidates.push(VertexPropertyValue::Float(crate::QueryFloat(-0.0)));
            }
        }
    }
    let mut encodings = candidates.iter().map(crate::encode_vertex_property_value_key).collect::<Vec<_>>();
    encodings.sort();
    encodings.dedup();
    Ok(encodings)
}

fn seek_node_batch(rows: &[(u64, NodeRow)]) -> Result<RecordBatch, String> {
    let base = node_batch(&rows.iter().map(|(_, row)| row.clone()).collect::<Vec<_>>())?;
    let schema = with_ordinal(&base.schema(), "input_ordinal");
    let mut columns: Vec<ArrayRef> = vec![Arc::new(UInt64Array::from(
        rows.iter().map(|(ordinal, _)| *ordinal).collect::<Vec<_>>(),
    ))];
    columns.extend(base.columns().iter().cloned());
    RecordBatch::try_new(schema, columns).map_err(|error| error.to_string())
}

fn seek_relationship_batch(rows: &[(u64, RelationshipRow)]) -> Result<RecordBatch, String> {
    let base = relationship_batch(&rows.iter().map(|(_, row)| row.clone()).collect::<Vec<_>>())?;
    let schema = with_ordinal(&base.schema(), "input_ordinal");
    let mut columns: Vec<ArrayRef> = vec![Arc::new(UInt64Array::from(
        rows.iter().map(|(ordinal, _)| *ordinal).collect::<Vec<_>>(),
    ))];
    columns.extend(base.columns().iter().cloned());
    RecordBatch::try_new(schema, columns).map_err(|error| error.to_string())
}

fn expansion_stream(
    view: SlateGraphReadView,
    request: ExpansionRequest,
) -> impl futures::Stream<Item = hydradb_graph_provider::Result<RecordBatch>> + Send + 'static {
    stream::try_unfold(
        (view, request, ExpansionState::default()),
        |(view, request, mut state)| async move {
            let batch = next_expansion_batch(&view, &request, &mut state).await?;
            Ok(batch.map(|batch| (batch, (view, request, state))))
        },
    )
}

#[derive(Default)]
struct ExpansionState {
    input_index: usize,
    branch_index: usize,
    type_index: usize,
    edge_iter: Option<DbIterator>,
    relationship_iter: Option<DbIterator>,
    seen_relationships: HashSet<(Option<u64>, u64, u64, String)>,
    pending_edge: Option<crate::EdgeRecord>,
    relationship_found: bool,
    emitted_for_input: usize,
    total_rows: u64,
    rows: Vec<ExpansionRow>,
    pending_row: Option<ExpansionRow>,
    estimated_batch_bytes: u64,
}

async fn next_expansion_batch(
    view: &SlateGraphReadView,
    request: &ExpansionRequest,
    state: &mut ExpansionState,
) -> hydradb_graph_provider::Result<Option<RecordBatch>> {
    let max_batch_rows = usize::try_from(view.max_batch_rows.max(1)).unwrap_or(usize::MAX);
    if let Some(row) = state.pending_row.take() {
        if !push_expansion_row(view, state, row)? {
            return Err(backend_error("batching", "pending expansion row did not fit an empty batch"));
        }
    }
    while state.rows.len() < max_batch_rows && state.input_index < request.inputs.len() {
        view.check()?;
        let input = &request.inputs[state.input_index];
        if request.max_results_per_input.is_some_and(|limit| state.emitted_for_input >= limit) {
            advance_expansion_input(state);
            continue;
        }

        let branch_count = match request.constraints.direction {
            hydradb_graph_provider::RelationshipDirection::Both => 2,
            _ => 1,
        };
        let type_count = request.constraints.types.len().max(1);
        if state.branch_index >= branch_count {
            advance_expansion_input(state);
            continue;
        }

        if state.edge_iter.is_none() {
            let edge_type = request.constraints.types.get(state.type_index).map(|edge_type| edge_type.as_ref());
            let prefix = expansion_edge_prefix(view, request, input, edge_type, state.branch_index);
            state.edge_iter = Some(view.scan_prefix(prefix.as_bytes()).await?);
        }

        if let Some(relationships) = state.relationship_iter.as_mut() {
            match relationships.next().await.map_err(provider_backend_error)? {
                Some(kv) => {
                    view.check_work()?;
                    let retained = kv.key.len().saturating_add(kv.value.len());
                    view.budget.consume_retained_bytes(u64::try_from(retained).unwrap_or(u64::MAX))?;
                    let key = String::from_utf8_lossy(&kv.key).into_owned();
                    let record = decode_relationship_record(&key, &kv.value).map_err(provider_backend_error)?;
                    state.relationship_found = true;
                    let relationship_id = record.relationship_id;
                    let destination = if record.src == input.source_node_id.0 {
                        record.dst
                    } else {
                        record.src
                    };
                    if input.target_node_id.is_some_and(|target| target.0 != destination)
                        || input
                            .excluded_relationships
                            .contains(&RelationshipIdentity::Identified(RelationshipId(relationship_id)))
                    {
                        continue;
                    }
                    if !push_expansion_row(
                        view,
                        state,
                        ExpansionRow {
                            ordinal: input.input_ordinal,
                            source: input.source_node_id.0,
                            destination,
                            relationship_id: Some(relationship_id),
                            properties: BTreeMap::new(),
                            relationship_type: record.edge_type,
                            relationship_source: record.src,
                            relationship_destination: record.dst,
                        },
                    )? {
                        break;
                    }
                    continue;
                }
                None => {
                    state.relationship_iter = None;
                    if let Some(edge) = state.pending_edge.take().filter(|_| !state.relationship_found) {
                        let destination = if edge.src == input.source_node_id.0 { edge.dst } else { edge.src };
                        if input.target_node_id.is_none_or(|target| target.0 == destination)
                            && !input.excluded_relationships.contains(&RelationshipIdentity::Structural {
                                source: NodeId(edge.src),
                                destination: NodeId(edge.dst),
                                relationship_type: edge.edge_type.clone(),
                            })
                        {
                            let key = keys::edge_metadata(view.cell_id.as_ref(), &edge.edge_type, edge.src, edge.dst);
                            let properties = match view.get(key.as_bytes()).await? {
                                Some(bytes) => {
                                    view.check_work()?;
                                    view.budget.consume_retained_bytes(u64::try_from(bytes.len()).unwrap_or(u64::MAX))?;
                                    decode_edge_metadata(&key, &bytes)
                                        .map_err(provider_backend_error)?
                                        .properties
                                        .into_iter()
                                        .map(|(key, value)| (key, crate::property_to_cypher(&value)))
                                        .collect()
                                }
                                None => BTreeMap::new(),
                            };
                            if !push_expansion_row(
                                view,
                                state,
                                ExpansionRow {
                                    ordinal: input.input_ordinal,
                                    source: input.source_node_id.0,
                                    destination,
                                    relationship_id: None,
                                    properties,
                                    relationship_type: edge.edge_type,
                                    relationship_source: edge.src,
                                    relationship_destination: edge.dst,
                                },
                            )? {
                                break;
                            }
                            continue;
                        }
                    }
                }
            }
        }

        let Some(edges) = state.edge_iter.as_mut() else {
            continue;
        };
        match edges.next().await.map_err(provider_backend_error)? {
            Some(kv) => {
                view.check_work()?;
                let retained = kv.key.len().saturating_add(kv.value.len());
                view.budget.consume_retained_bytes(u64::try_from(retained).unwrap_or(u64::MAX))?;
                let key = String::from_utf8_lossy(&kv.key).into_owned();
                let edge = decode_edge_record(&key, &kv.value).map_err(provider_backend_error)?;
                let is_outgoing = expansion_is_outgoing(request, state.branch_index);
                let connects = if is_outgoing {
                    edge.src == input.source_node_id.0
                } else {
                    edge.dst == input.source_node_id.0
                };
                if !connects
                    || (!request.constraints.types.is_empty() && !request.constraints.types.iter().any(|ty| ty.as_ref() == edge.edge_type))
                {
                    continue;
                }
                let prefix = keys::relationship_edge_prefix(view.cell_id.as_ref(), &edge.edge_type, edge.src, edge.dst);
                state.relationship_iter = Some(view.scan_prefix(prefix.as_bytes()).await?);
                state.pending_edge = Some(edge);
                state.relationship_found = false;
            }
            None => {
                state.edge_iter = None;
                state.type_index = state.type_index.saturating_add(1);
                if state.type_index >= type_count {
                    state.type_index = 0;
                    state.branch_index = state.branch_index.saturating_add(1);
                }
            }
        }
    }

    if state.rows.is_empty() {
        return Ok(None);
    }
    let rows = std::mem::take(&mut state.rows);
    state.estimated_batch_bytes = 0;
    // Account the Arrow arrays that are about to be allocated in addition to
    // the retained row metadata charged while traversing adjacency.
    let estimated_output_bytes = rows.iter().fold(rows.len().saturating_mul(64), |total, row| {
        total.saturating_add(row.relationship_type.len())
    });
    view.budget
        .consume_retained_bytes(u64::try_from(estimated_output_bytes).unwrap_or(u64::MAX))?;
    expansion_batch(&rows).map(Some).map_err(|error| backend_error("arrow", error))
}

fn push_expansion_row(view: &SlateGraphReadView, state: &mut ExpansionState, row: ExpansionRow) -> hydradb_graph_provider::Result<bool> {
    let identity = (
        row.relationship_id,
        row.relationship_source,
        row.relationship_destination,
        row.relationship_type.clone(),
    );
    if state.seen_relationships.contains(&identity) {
        return Ok(true);
    }
    if state.total_rows >= view.budget.limits().max_total_rows {
        return Err(GraphProviderError::BudgetExceeded {
            resource: hydradb_graph_provider::BudgetResource::Rows,
            limit: view.budget.limits().max_total_rows,
        });
    }
    let row_bytes = estimated_expansion_row_bytes(&row)?;
    let mut bytes = BatchByteSizer {
        estimated: if state.rows.is_empty() {
            (16 * 1024).min(view.max_batch_bytes)
        } else {
            state.estimated_batch_bytes
        },
        maximum: view.max_batch_bytes,
    };
    if !bytes.try_add(row_bytes, state.rows.is_empty()) {
        state.pending_row = Some(row);
        return Ok(false);
    }
    let retained = std::mem::size_of::<ExpansionRow>() + std::mem::size_of_val(&identity) + 2 * row.relationship_type.len();
    view.budget.consume_retained_bytes(u64::try_from(retained).unwrap_or(u64::MAX))?;
    state
        .seen_relationships
        .try_reserve(1)
        .map_err(|error| backend_error("allocation", error))?;
    state
        .rows
        .try_reserve_exact(1)
        .map_err(|error| backend_error("allocation", error))?;
    state.seen_relationships.insert(identity);
    state.rows.push(row);
    state.estimated_batch_bytes = bytes.estimated;
    state.total_rows = state.total_rows.saturating_add(1);
    state.emitted_for_input = state.emitted_for_input.saturating_add(1);
    Ok(true)
}

fn expansion_edge_prefix(
    view: &SlateGraphReadView,
    request: &ExpansionRequest,
    input: &hydradb_graph_provider::ExpansionInput,
    edge_type: Option<&str>,
    branch_index: usize,
) -> String {
    // The immutable shard policy is carried into snapshot and transaction
    // views. Full indexing keeps incoming reads on the reverse index; only
    // OutboundOnly needs a canonical outbound scan. That fallback retains its
    // iterator across batches and charges every visited record to the usual
    // work/byte budgets instead of materializing the cell.
    match (expansion_is_outgoing(request, branch_index), edge_type) {
        (true, Some(edge_type)) => keys::out_prefix(view.cell_id.as_ref(), edge_type, input.source_node_id.0),
        (false, Some(edge_type)) if view.index_policy.write_reverse_index() => {
            keys::in_prefix(view.cell_id.as_ref(), edge_type, input.source_node_id.0)
        }
        (false, None) if view.index_policy.write_reverse_index() => format!("cell/{}/e/in/", view.cell_id),
        (false, Some(edge_type)) => {
            format!("cell/{}/e/out/{edge_type}/", view.cell_id)
        }
        (_, None) => keys::out_edge_cell_prefix(view.cell_id.as_ref()),
    }
}

fn expansion_is_outgoing(request: &ExpansionRequest, branch_index: usize) -> bool {
    use hydradb_graph_provider::RelationshipDirection;
    match request.constraints.direction {
        RelationshipDirection::Outgoing => true,
        RelationshipDirection::Incoming => false,
        RelationshipDirection::Both => branch_index == 0,
    }
}

fn advance_expansion_input(state: &mut ExpansionState) {
    state.input_index = state.input_index.saturating_add(1);
    state.branch_index = 0;
    state.type_index = 0;
    state.edge_iter = None;
    state.relationship_iter = None;
    state.pending_edge = None;
    state.seen_relationships.clear();
    state.emitted_for_input = 0;
}

struct ExpansionRow {
    ordinal: u64,
    source: u64,
    destination: u64,
    relationship_id: Option<u64>,
    properties: BTreeMap<String, CypherValue>,
    relationship_type: String,
    relationship_source: u64,
    relationship_destination: u64,
}

fn estimated_expansion_row_bytes(row: &ExpansionRow) -> hydradb_graph_provider::Result<u64> {
    Ok(384_u64
        .saturating_add(row.relationship_type.len() as u64)
        .saturating_add(estimated_property_bytes(&row.properties)?))
}

fn expansion_batch(rows: &[ExpansionRow]) -> Result<RecordBatch, String> {
    let schema = canonical_expansion_schema();
    let relation_rows = rows
        .iter()
        .map(|row| RelationshipRow {
            source: row.relationship_source,
            destination: row.relationship_destination,
            relationship_type: row.relationship_type.clone(),
            relationship_id: row.relationship_id,
            properties: row.properties.clone(),
        })
        .collect::<Vec<_>>();
    let relationships = relationship_struct(relation_rows.iter().map(Some).collect(), schema.field(3).data_type())?;
    let paths = new_null_array(schema.field(4).data_type(), rows.len());
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(UInt64Array::from(rows.iter().map(|row| row.ordinal).collect::<Vec<_>>())),
            Arc::new(UInt64Array::from(rows.iter().map(|row| row.source).collect::<Vec<_>>())),
            Arc::new(UInt64Array::from(rows.iter().map(|row| row.destination).collect::<Vec<_>>())),
            Arc::new(relationships),
            paths,
        ],
    )
    .map_err(|error| error.to_string())
}

fn storage_property(value: &CypherValue) -> hydradb_graph_provider::Result<VertexPropertyValue> {
    crate::storage_property(value).map_err(provider_backend_error)
}
