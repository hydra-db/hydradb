use super::*;
use crate::query::owned_relationships::{invalid, MAX_OWNED_MUTATION_BYTES, MAX_OWNED_RELATIONSHIPS, SET_ID_PROPERTY};
use sha2::{Digest, Sha256};

const OPERATION: &str = "replace_owned_relationship_set";
const MAX_SOURCE_DEGREE: u64 = 16_384;

fn head_property(request: &OwnedRelationshipSetRequest) -> String {
    let mut digest = Sha256::new();
    for component in std::iter::once(request.edge_type.as_str())
        .chain([request.owner_property.as_str(), request.owner.as_str()])
        .map(str::to_owned)
        .chain(
            request
                .properties
                .properties
                .iter()
                .flat_map(|(key, value)| [key.clone(), encode_vertex_property_value_record(value)]),
        )
    {
        digest.update((component.len() as u64).to_be_bytes());
        digest.update(component.as_bytes());
    }
    format!(
        "{}{digest:x}",
        hydradb_graph_slate::write::OWNED_RELATIONSHIP_HEAD_PREFIX,
        digest = digest.finalize()
    )
}

fn read_head(metadata: &VertexMetadata, property: &str) -> Result<Option<(i64, String, String)>> {
    let Some(value) = metadata.properties.get(property) else {
        return Ok(None);
    };
    let VertexPropertyValue::String(value) = value else {
        return Err(invalid("owned relationship head is corrupt"));
    };
    let head: (i64, String, String) = serde_json::from_str(value).map_err(|_| invalid("owned relationship head is corrupt"))?;
    if head.0 <= 0 || head.1.is_empty() || head.1.len() > 128 || head.2.is_empty() || head.2.len() > 128 {
        return Err(invalid("owned relationship head is corrupt"));
    }
    Ok(Some(head))
}

impl GraphShard {
    pub(crate) async fn replace_owned_relationship_set(
        &self,
        context: &QueryContext,
        request: &OwnedRelationshipSetRequest,
    ) -> Result<OwnedRelationshipSetResult> {
        request.validate()?;
        validate_component("cell_id", &context.cell_id)?;
        self.ensure_write_authority(&context.cell_id, OPERATION)?;
        let budget = super::super::query::QueryBudget::new(context.max_runtime_ms, context.cancellation_token.clone());
        budget.check(OPERATION)?;
        self.run_write_pipeline_fencing_noops(async {
            let _permit = self.acquire_graph_write_permit(OPERATION).await?;
            let _writer = self.writer_lane(&context.cell_id).lock().await;
            for attempt in 0..GRAPH_TXN_MAX_RETRIES {
                budget.check(OPERATION)?;
                let lock = self.acquire_local_write_guard_no_fence(&context.cell_id, OPERATION).await?;
                let result = async {
                    let (txn, result) = self.prepare_owned_relationship_set_txn(&context.cell_id, request, &budget).await?;
                    budget.check(OPERATION)?;
                    if result.applied {
                        commit_txn_traced(
                            txn,
                            self.await_durable_writes,
                            &context.cell_id,
                            &request.edge_type,
                            result.storage_sequence,
                        )
                        .await?;
                    }
                    Ok(result)
                }
                .await;
                match finish_local_write(lock, result).await {
                    Err(error) if is_retryable_write_conflict(&error) && attempt + 1 < GRAPH_TXN_MAX_RETRIES => {
                        self.operation_metrics.write_retries.fetch_add(1, Ordering::Relaxed);
                        tokio::task::yield_now().await;
                    }
                    Ok(result) => {
                        if result.applied {
                            self.operation_metrics.write_commits.fetch_add(1, Ordering::Relaxed);
                        }
                        return Ok(result);
                    }
                    result => return result.inspect_err(record_error_class),
                }
            }
            Err(GraphError::RetryExhausted {
                operation: OPERATION,
                attempts: GRAPH_TXN_MAX_RETRIES,
            })
        })
        .await
    }

    async fn prepare_owned_relationship_set_txn(
        &self,
        cell_id: &str,
        request: &OwnedRelationshipSetRequest,
        budget: &super::super::query::QueryBudget,
    ) -> Result<(DbTransaction, OwnedRelationshipSetResult)> {
        self.validate_changing_write().await?;
        let txn = self.storage.writer()?.begin(IsolationLevel::SerializableSnapshot)?;
        self.validate_write_fence_txn(&txn, cell_id, OPERATION).await?;
        let source_key = keys::vertex(cell_id, request.source);
        let source_record = decode_vertex_record(&source_key, read_txn_remote(&txn, &source_key).await?.as_ref())?
            .ok_or_else(|| invalid("source vertex does not exist"))?;
        let (source_view, _) = overlaid_vertex_metadata_txn(&txn, cell_id, request.source, Some(&source_record)).await?;
        if !vertex_matches_exact_node_constraints(request.source, source_view.as_ref().unwrap(), &request.source_constraints) {
            return Err(invalid("source vertex does not match its scope constraints"));
        }
        let property = head_property(request);
        let previous = read_head(&source_record, &property)?;
        if let Some((version, operation, hash)) = previous {
            if request.version == version && (request.operation_id != operation || request.content_hash != hash) {
                return Err(GraphError::IdempotencyConflict {
                    operation: OPERATION,
                    idempotency_key: request.operation_id.clone(),
                    reason: "the owned set version already has different immutable content",
                });
            }
            if request.version <= version {
                let sequence = txn.seqnum();
                return Ok((
                    txn,
                    OwnedRelationshipSetResult {
                        applied: false,
                        version,
                        created: 0,
                        deleted: 0,
                        storage_sequence: sequence,
                    },
                ));
            }
            if request.operation_id == operation {
                return Err(invalid("an operation_id cannot be reused for another version"));
            }
        }
        for target in &request.targets {
            budget.check(OPERATION)?;
            let key = keys::vertex(cell_id, target.vertex);
            let record = decode_vertex_record(&key, read_txn_remote(&txn, &key).await?.as_ref())?
                .ok_or_else(|| invalid("target vertex does not exist"))?;
            let (view, _) = overlaid_vertex_metadata_txn(&txn, cell_id, target.vertex, Some(&record)).await?;
            if !vertex_matches_exact_node_constraints(target.vertex, view.as_ref().unwrap(), &target.constraints) {
                return Err(invalid("target vertex does not match its scope constraints"));
            }
        }
        ensure_limit(
            OPERATION,
            read_counter_txn(&txn, &keys::degree_out(cell_id, &request.edge_type, request.source)).await?,
            MAX_SOURCE_DEGREE,
        )?;
        let owner_value = VertexPropertyValue::String(request.owner.clone());
        let set_value = VertexPropertyValue::String(property.clone());
        let prefix = format!(
            "{}{:020}/",
            keys::relationship_property_index_prefix(
                cell_id,
                &request.edge_type,
                &request.owner_property,
                &encode_vertex_property_value_key(&owner_value)
            ),
            request.source
        );
        let mut scan = txn.scan_prefix(prefix.as_bytes(), ..).await?;
        let mut records = Vec::new();
        let mut scanned = 0;
        let mut scanned_bytes = 0usize;
        while let Some(kv) = scan.next().await? {
            budget.check(OPERATION)?;
            scanned += 1;
            ensure_limit(OPERATION, scanned, MAX_OWNED_RELATIONSHIPS as u64)?;
            let id = decode_u64(&String::from_utf8_lossy(&kv.key), &kv.value)?;
            let pointer_key = keys::relationship_id(cell_id, id);
            let Some(pointer) = read_txn_remote(&txn, &pointer_key).await? else {
                return Err(invalid("owned relationship index points to a missing record"));
            };
            let key = std::str::from_utf8(&pointer).map_err(|_| invalid("owned relationship pointer is corrupt"))?;
            let Some(value) = read_txn_remote(&txn, key).await? else {
                return Err(invalid("owned relationship index points to a missing record"));
            };
            scanned_bytes = scanned_bytes
                .saturating_add(kv.key.len())
                .saturating_add(kv.value.len())
                .saturating_add(pointer_key.len())
                .saturating_add(pointer.len())
                .saturating_add(value.len());
            ensure_limit(OPERATION, scanned_bytes as u64, MAX_OWNED_MUTATION_BYTES as u64)?;
            let record = decode_relationship_record(key, &value)?;
            if record.src == request.source
                && record.edge_type == request.edge_type
                && record.metadata.properties.get(&request.owner_property) == Some(&owner_value)
                && record.metadata.properties.get(SET_ID_PROPERTY) == Some(&set_value)
                && request
                    .properties
                    .properties
                    .iter()
                    .all(|(key, value)| record.metadata.properties.get(key) == Some(value))
            {
                records.push(record);
            }
        }
        drop(scan);
        let wanted = request.targets.iter().map(|target| target.vertex).collect::<BTreeSet<_>>();
        let mut retained = BTreeSet::new();
        let mut deleting = BTreeMap::<VertexId, BTreeSet<RelationshipId>>::new();
        let mut deletions = Vec::new();
        for record in records {
            if !wanted.contains(&record.dst) || !retained.insert(record.dst) {
                deleting.entry(record.dst).or_default().insert(record.relationship_id);
                deletions.push(record);
            }
        }
        let epoch = txn.seqnum().checked_add(1).ok_or_else(|| invalid("storage sequence overflow"))?;
        for record in &deletions {
            budget.check(OPERATION)?;
            txn.delete(keys::relationship(cell_id, &request.edge_type, request.source, record.dst, record.relationship_id).as_bytes())?;
            txn.delete(keys::relationship_id(cell_id, record.relationship_id).as_bytes())?;
            delete_relationship_property_indexes_txn(&txn, record, &record.metadata)?;
        }
        for (destination, ids) in &deleting {
            let count_key = keys::relationship_count(cell_id, &request.edge_type, request.source, *destination);
            let count = read_counter_txn(&txn, &count_key).await?.saturating_sub(ids.len() as u64);
            if count == 0 {
                txn.delete(count_key.as_bytes())?;
            } else {
                txn.put(count_key.as_bytes(), encode_u64(count))?;
            }
            let legacy_key = keys::edge_metadata(cell_id, &request.edge_type, request.source, *destination);
            if read_txn_remote(&txn, &legacy_key).await?.is_none()
                && !has_surviving_relationship_txn(&txn, cell_id, &request.edge_type, request.source, *destination, ids).await?
            {
                delete_structural_edge_txn(
                    self.writes_reverse_index(),
                    &self.xlog_floor_ensured,
                    &txn,
                    &EdgeMutation {
                        cell_id: cell_id.to_owned(),
                        edge_type: request.edge_type.clone(),
                        src: request.source,
                        dst: *destination,
                        idempotency_key: String::new(),
                    },
                    epoch,
                )
                .await?;
            }
        }
        let additions = wanted.difference(&retained).copied().collect::<Vec<_>>();
        let mut last_id = read_counter_txn(&txn, &keys::last_relationship_id(cell_id)).await?;
        let ids = next_available_relationship_ids_txn(&txn, cell_id, &mut last_id, additions.len(), OPERATION).await?;
        for (destination, relationship_id) in additions.iter().zip(ids) {
            budget.check(OPERATION)?;
            if edge_epoch_at_txn(&txn, cell_id, &request.edge_type, request.source, *destination, txn.seqnum())
                .await?
                .is_none()
            {
                let edge = EdgeRecord {
                    cell_id: cell_id.to_owned(),
                    edge_type: request.edge_type.clone(),
                    src: request.source,
                    dst: *destination,
                };
                let encoded = encode_edge_record(&edge);
                txn.put(
                    keys::out_edge(cell_id, &request.edge_type, request.source, *destination).as_bytes(),
                    &encoded,
                )?;
                let out_key = keys::degree_out(cell_id, &request.edge_type, request.source);
                let degree = read_counter_txn(&txn, &out_key).await? + 1;
                ensure_limit(OPERATION, degree, MAX_SOURCE_DEGREE)?;
                txn.put(out_key.as_bytes(), encode_u64(degree))?;
                if self.writes_reverse_index() {
                    txn.put(
                        keys::in_edge(cell_id, &request.edge_type, *destination, request.source).as_bytes(),
                        &encoded,
                    )?;
                    let in_key = keys::degree_in(cell_id, &request.edge_type, *destination);
                    txn.put(in_key.as_bytes(), encode_u64(read_counter_txn(&txn, &in_key).await? + 1))?;
                }
                self.mark_topology_change_txn(&txn, cell_id, &request.edge_type, epoch, &[(request.source, *destination, true)])
                    .await?;
            } else if read_txn_remote(
                &txn,
                &keys::edge_metadata(cell_id, &request.edge_type, request.source, *destination),
            )
            .await?
            .is_none()
                && !has_surviving_relationship_txn(&txn, cell_id, &request.edge_type, request.source, *destination, &BTreeSet::new())
                    .await?
            {
                return Err(invalid("unidentified bare legacy topology cannot be adopted into an owned set"));
            }
            let mut metadata = request.properties.clone();
            metadata.properties.insert(request.owner_property.clone(), owner_value.clone());
            metadata.properties.insert(SET_ID_PROPERTY.into(), set_value.clone());
            let record = RelationshipRecord {
                cell_id: cell_id.to_owned(),
                edge_type: request.edge_type.clone(),
                src: request.source,
                dst: *destination,
                relationship_id,
                metadata,
            };
            let key = keys::relationship(cell_id, &request.edge_type, request.source, *destination, relationship_id);
            txn.put(key.as_bytes(), encode_relationship_record(&record))?;
            txn.put(keys::relationship_id(cell_id, relationship_id).as_bytes(), key.as_bytes())?;
            put_relationship_property_indexes_txn(&txn, &record)?;
            let count_key = keys::relationship_count(cell_id, &request.edge_type, request.source, *destination);
            txn.put(count_key.as_bytes(), encode_u64(read_counter_txn(&txn, &count_key).await? + 1))?;
        }
        if !additions.is_empty() {
            txn.put(keys::last_relationship_id(cell_id).as_bytes(), encode_u64(last_id))?;
        }
        self.mark_topology_change_txn(&txn, cell_id, &request.edge_type, epoch, &[]).await?;
        if read_txn_remote(&txn, &keys::vertex_write_back_generation(cell_id, &property))
            .await?
            .is_some()
        {
            return Err(invalid("owned relationship heads cannot be write-back properties"));
        }
        let value = VertexPropertyValue::String(
            serde_json::to_string(&(request.version, &request.operation_id, &request.content_hash))
                .map_err(|_| invalid("head encoding failed"))?,
        );
        let mut source = source_record.clone();
        if let Some(previous) = source.properties.insert(property.clone(), value.clone()) {
            txn.delete(
                keys::vertex_property_index(cell_id, &property, &encode_vertex_property_value_key(&previous), request.source).as_bytes(),
            )?;
        }
        txn.put(
            keys::vertex_property_index(cell_id, &property, &encode_vertex_property_value_key(&value), request.source).as_bytes(),
            encode_u64(request.source),
        )?;
        txn.put(source_key.as_bytes(), encode_vertex_metadata(&source))?;
        Ok((
            txn,
            OwnedRelationshipSetResult {
                applied: true,
                version: request.version,
                created: additions.len() as u64,
                deleted: deletions.len() as u64,
                storage_sequence: epoch,
            },
        ))
    }
}
