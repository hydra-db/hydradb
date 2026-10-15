use super::*;

const COMPLETE: &[u8] = b"vertex-membership-v1\ncomplete\n";
const MIGRATION: &str = "unstable_backfill_vertex_membership";

#[derive(Default)]
struct BackfillProgress {
    phase: u8,
    cursor: String,
    offset: usize,
    sequence: StorageSequence,
    examined: u64,
    created: u64,
}

fn corrupt_checkpoint(key: &str, reason: &str) -> GraphError {
    GraphError::CorruptValue {
        key: key.to_string(),
        reason: reason.to_string(),
    }
}

impl BackfillProgress {
    fn encode(&self) -> Vec<u8> {
        format!(
            "vertex-membership-v1\nbackfilling\n{}\n{}\n{}\n{}\n{}\n{}\n",
            self.phase, self.cursor, self.offset, self.sequence, self.examined, self.created
        )
        .into_bytes()
    }

    fn decode(key: &str, value: &[u8]) -> Result<Self> {
        let text = std::str::from_utf8(value)
            .map_err(|_| corrupt_checkpoint(key, "invalid membership checkpoint UTF-8"))?;
        let fields = text.lines().collect::<Vec<_>>();
        if fields.len() != 8 || fields[0] != "vertex-membership-v1" || fields[1] != "backfilling" {
            return Err(corrupt_checkpoint(
                key,
                "unsupported or malformed vertex-membership checkpoint",
            ));
        }
        let malformed = || corrupt_checkpoint(key, "invalid membership checkpoint field");
        let progress = Self {
            phase: fields[2].parse().map_err(|_| malformed())?,
            cursor: fields[3].to_string(),
            offset: fields[4].parse().map_err(|_| malformed())?,
            sequence: fields[5].parse().map_err(|_| malformed())?,
            examined: fields[6].parse().map_err(|_| malformed())?,
            created: fields[7].parse().map_err(|_| malformed())?,
        };
        // Phase 8 is never a backfilling checkpoint: its transaction publishes
        // COMPLETE. Accepting it here would bypass the verification scan.
        if progress.phase >= 8
            || (progress.offset > 0 && (progress.phase % 4 != 2 || progress.cursor.is_empty()))
        {
            return Err(malformed());
        }
        Ok(progress)
    }
}

fn decode_membership_state(key: &str, value: &[u8]) -> Result<bool> {
    if value == COMPLETE {
        Ok(true)
    } else {
        BackfillProgress::decode(key, value)?;
        Ok(false)
    }
}

pub(super) fn membership_maintenance_error(cell_id: &str) -> GraphError {
    GraphError::UnsupportedQuery {
        reason: QueryFailureReason::Other,
        dialect: "GraphMaintenance",
        feature: format!("vertex membership backfill is in progress for cell {cell_id}; resume maintenance before writing"),
    }
}

fn disabled_membership_error(cell_id: &str) -> GraphError {
    GraphError::UnsupportedQuery {
        reason: QueryFailureReason::Other,
        dialect: "GraphConfiguration",
        feature: format!("cell {cell_id} requires unstable_canonical_vertex_membership=true"),
    }
}

pub(super) fn validate_membership_writer(
    cell_id: &str,
    key: &str,
    value: &[u8],
    enabled: bool,
) -> Result<bool> {
    let complete = decode_membership_state(key, value)?;
    if !enabled {
        return Err(disabled_membership_error(cell_id));
    }
    Ok(complete)
}

impl GraphShard {
    /// Returns canonical presence without collapsing an empty vertex into
    /// absence. Reads inherit the caller's scoped storage snapshot.
    pub async fn vertex_metadata_if_exists(
        &self,
        cell_id: &str,
        vertex_id: VertexId,
    ) -> Result<Option<VertexMetadata>> {
        validate_component("cell_id", cell_id)?;
        self.ensure_cell_readable(cell_id, "vertex_metadata_if_exists")
            .await?;
        let key = keys::vertex(cell_id, vertex_id);
        self.read_remote(&key)
            .await?
            .map(|value| decode_vertex_metadata(&key, &value))
            .transpose()
    }

    /// Read readiness at the same task-local snapshot as the graph records.
    pub async fn unstable_vertex_membership_complete(&self, cell_id: &str) -> Result<bool> {
        validate_component("cell_id", cell_id)?;
        let key = keys::vertex_membership(cell_id);
        let Some(value) = self.read_remote(&key).await? else {
            return Ok(false);
        };
        let complete = decode_membership_state(&key, &value)?;
        if complete && !self.unstable_canonical_vertex_membership {
            return Err(disabled_membership_error(cell_id));
        }
        Ok(complete)
    }

    pub(crate) async fn validate_vertex_membership_write_txn(
        &self,
        txn: &DbTransaction,
        cell_id: &str,
        operation: &'static str,
    ) -> Result<()> {
        let key = keys::vertex_membership(cell_id);
        if let Some(value) = read_txn_remote(txn, &key).await? {
            let complete = validate_membership_writer(
                cell_id,
                &key,
                &value,
                self.unstable_canonical_vertex_membership,
            )?;
            if !complete && operation != MIGRATION {
                return Err(membership_maintenance_error(cell_id));
            }
        }
        Ok(())
    }

    /// The topology hook calls this before publishing a live edge. Staged
    /// endpoint reads/writes participate in the same serializable transaction.
    pub(crate) async fn ensure_vertex_endpoints_txn(
        &self,
        txn: &DbTransaction,
        cell_id: &str,
        vertices: &[VertexId],
    ) -> Result<usize> {
        if !self.unstable_canonical_vertex_membership {
            return Ok(0);
        }
        let mut created = 0;
        for &vertex_id in vertices {
            let key = keys::vertex(cell_id, vertex_id);
            match read_txn_remote(txn, &key).await? {
                Some(value) => {
                    decode_vertex_metadata(&key, &value)?;
                }
                None => {
                    txn.put(
                        key.as_bytes(),
                        encode_vertex_metadata(&VertexMetadata::default()),
                    )?;
                    created += 1;
                }
            }
        }
        Ok(created)
    }

    /// Backfill and verify membership under exclusive maintenance ownership.
    /// Stop application writers (including old binaries) for the entire graph
    /// store. Each call commits bounded chunks and can resume after restart.
    /// `complete` is published only after a separate full verification pass.
    pub async fn unstable_backfill_vertex_membership(
        &self,
        cell_id: &str,
        options: VertexMembershipBackfillOptions,
    ) -> Result<VertexMembershipBackfillReport> {
        validate_component("cell_id", cell_id)?;
        if !self.unstable_canonical_vertex_membership {
            return Err(disabled_membership_error(cell_id));
        }
        if !self.await_durable_writes || super::write_pipeline::current().is_some() {
            return Err(GraphError::UnsafeDurabilityConfig { operation: MIGRATION, reason: "membership maintenance requires direct durable commits outside the write pipeline".to_string() });
        }
        if options.max_records_per_commit == 0
            || options.max_commits == 0
            || options.max_bytes_per_commit == 0
        {
            return Err(GraphError::AdmissionRejected {
                operation: MIGRATION,
                actual: 0,
                limit: 1,
            });
        }
        ensure_limit(
            MIGRATION,
            options.max_records_per_commit as u64,
            self.limits.max_bulk_import_edges as u64,
        )?;
        self.ensure_write_authority(cell_id, MIGRATION)?;
        let _permit = self.acquire_graph_write_permit(MIGRATION).await?;
        let lock = self.acquire_local_write_guard(cell_id, MIGRATION).await?;
        let result = self
            .backfill_vertex_membership_locked(cell_id, options)
            .await;
        finish_local_write(lock, result).await
    }

    async fn backfill_vertex_membership_locked(
        &self,
        cell_id: &str,
        options: VertexMembershipBackfillOptions,
    ) -> Result<VertexMembershipBackfillReport> {
        let key = keys::vertex_membership(cell_id);
        let mut report = VertexMembershipBackfillReport {
            legacy_isolated_vertices_may_be_missing: true,
            ..Default::default()
        };
        for commit in 0..options.max_commits {
            QueryBudget::new(None, crate::QueryCancellationToken::current()).check(MIGRATION)?;
            let txn = self
                .db
                .writer()?
                .begin(IsolationLevel::SerializableSnapshot)
                .await?;
            self.validate_write_fence_txn(&txn, cell_id, MIGRATION)
                .await?;
            let current = txn.seqnum();
            let stored = read_txn_remote(&txn, &key).await?;
            if stored
                .as_ref()
                .is_some_and(|value| value.as_ref() == COMPLETE)
            {
                report.complete = true;
                report.storage_sequence = current;
                self.db.refresh_writer_fence().await?;
                return Ok(report);
            }
            let mut progress = match stored {
                Some(value) if !(options.restart && commit == 0) => {
                    BackfillProgress::decode(&key, &value)?
                }
                _ => BackfillProgress {
                    sequence: current,
                    ..Default::default()
                },
            };
            if progress.sequence != current {
                return Err(GraphError::ConditionalWriteConflict {
                    operation: "resume_vertex_membership_backfill",
                    key: key.clone(),
                });
            }
            if progress.phase < 8 {
                self.backfill_vertex_membership_chunk(
                    &txn,
                    cell_id,
                    &mut progress,
                    options.max_records_per_commit,
                    options.max_bytes_per_commit,
                )
                .await?;
            }
            let complete = progress.phase == 8;
            progress.sequence = next_epoch_txn(&txn, cell_id).await?;
            let value = if complete {
                COMPLETE.to_vec()
            } else {
                progress.encode()
            };
            txn.put(key.as_bytes(), value)?;
            QueryBudget::new(None, crate::QueryCancellationToken::current()).check(MIGRATION)?;
            let sequence = commit_txn_strict_with_sequence(txn, true).await?;
            if sequence != Some(progress.sequence) {
                return Err(GraphError::ConditionalWriteConflict {
                    operation: "commit_vertex_membership_backfill",
                    key: key.clone(),
                });
            }
            report = VertexMembershipBackfillReport {
                complete,
                records_examined: progress.examined,
                vertices_created: progress.created,
                storage_sequence: progress.sequence,
                legacy_isolated_vertices_may_be_missing: true,
            };
            tracing::info!(hydradb.cell_id = %cell_id, phase = progress.phase, records_examined = progress.examined, vertices_created = progress.created, storage_sequence = progress.sequence, complete, "vertex membership maintenance progress");
            if complete {
                return Ok(report);
            }
        }
        Ok(report)
    }

    async fn backfill_vertex_membership_chunk(
        &self,
        txn: &DbTransaction,
        cell_id: &str,
        progress: &mut BackfillProgress,
        limit: usize,
        byte_limit: usize,
    ) -> Result<()> {
        let kind = progress.phase % 4;
        let verify = progress.phase >= 4;
        let prefix = match kind {
            0 => format!("cell/{cell_id}/vertex/"),
            1 => keys::out_edge_cell_prefix(cell_id),
            2 => keys::out_segment_cell_prefix(cell_id),
            _ => keys::relationship_cell_prefix(cell_id),
        };
        let mut suffix = if progress.cursor.is_empty() {
            Vec::new()
        } else {
            progress
                .cursor
                .strip_prefix(&prefix)
                .ok_or_else(|| {
                    corrupt_checkpoint(
                        &keys::vertex_membership(cell_id),
                        "checkpoint cursor does not match phase",
                    )
                })?
                .as_bytes()
                .to_vec()
        };
        if !progress.cursor.is_empty() && progress.offset == 0 {
            suffix.push(0);
        }
        let mut iter = txn.scan_prefix(prefix.as_bytes(), suffix..).await?;
        let mut examined = 0;
        let mut bytes = 0_usize;
        let _memory = crate::core::memory_diagnostics::MemoryDiagnosticGuard::new(
            crate::core::memory_diagnostics::MemoryStage::ShardWriteActive,
            byte_limit as u64,
        );
        while examined < limit {
            QueryBudget::new(None, crate::QueryCancellationToken::current()).check(MIGRATION)?;
            let Some(kv) = iter.next().await? else {
                progress.phase += 1;
                progress.cursor.clear();
                progress.offset = 0;
                break;
            };
            // A segment is decoded as one canonical record, even when its
            // destinations are processed over several checkpointed chunks.
            // Charge its full encoded size plus bounded endpoint staging.
            let staged = if kind == 2 {
                (limit - examined).saturating_mul(256)
            } else {
                512
            };
            let record_bytes = kv
                .key
                .len()
                .saturating_add(kv.value.len())
                .saturating_add(staged);
            ensure_limit(
                "vertex_membership_record_bytes",
                record_bytes as u64,
                byte_limit as u64,
            )?;
            if bytes.saturating_add(record_bytes) > byte_limit {
                break;
            }
            bytes += record_bytes;
            let key = std::str::from_utf8(&kv.key)
                .map_err(|_| corrupt_checkpoint(&prefix, "invalid canonical key UTF-8"))?
                .to_string();
            let mut endpoints = Vec::new();
            let mut offset = 0;
            let consumed = match kind {
                0 => {
                    let id = key
                        .strip_prefix(&prefix)
                        .and_then(|suffix| suffix.parse::<VertexId>().ok())
                        .ok_or_else(|| corrupt_checkpoint(&key, "invalid canonical vertex key"))?;
                    if key != keys::vertex(cell_id, id) {
                        return Err(corrupt_checkpoint(&key, "noncanonical vertex key"));
                    }
                    decode_vertex_metadata(&key, &kv.value)?;
                    1
                }
                1 => {
                    let edge = decode_edge_record(&key, &kv.value)?;
                    if edge.cell_id != cell_id
                        || key != keys::out_edge(cell_id, &edge.edge_type, edge.src, edge.dst)
                    {
                        return Err(corrupt_checkpoint(&key, "canonical edge identity mismatch"));
                    }
                    endpoints.extend([edge.src, edge.dst]);
                    1
                }
                2 => {
                    let segment = decode_out_edge_segment(&key, &kv.value)?;
                    ensure_limit(
                        MIGRATION,
                        segment.destinations.len() as u64,
                        self.limits.max_bulk_import_edges as u64,
                    )?;
                    let segment_id = key
                        .rsplit('/')
                        .next()
                        .expect("decoded segment has a final key component");
                    if segment.cell_id != cell_id
                        || segment.storage_sequence > txn.seqnum()
                        || key
                            != keys::out_segment(
                                cell_id,
                                &segment.edge_type,
                                segment.src,
                                segment.storage_sequence,
                                segment_id,
                            )
                    {
                        return Err(corrupt_checkpoint(
                            &key,
                            "invalid segment identity or sequence",
                        ));
                    }
                    let start = if progress.cursor == key {
                        progress.offset
                    } else {
                        0
                    };
                    if start > segment.destinations.len() {
                        return Err(corrupt_checkpoint(
                            &key,
                            "checkpoint segment offset exceeds record",
                        ));
                    }
                    let end = start
                        .saturating_add(limit - examined)
                        .min(segment.destinations.len());
                    for &dst in &segment.destinations[start..end] {
                        let tombstone_key = keys::out_segment_tombstone(
                            cell_id,
                            &segment.edge_type,
                            segment.src,
                            dst,
                        );
                        let tombstone = read_txn_remote(txn, &tombstone_key)
                            .await?
                            .map(|value| decode_u64(&tombstone_key, &value))
                            .transpose()?;
                        if segment_edge_visible(segment.storage_sequence, tombstone) {
                            endpoints.extend([segment.src, dst]);
                        }
                    }
                    if end < segment.destinations.len() {
                        offset = end;
                    }
                    (end - start).max(1)
                }
                _ => {
                    let relationship = decode_relationship_record(&key, &kv.value)?;
                    if relationship.cell_id != cell_id
                        || key
                            != keys::relationship(
                                cell_id,
                                &relationship.edge_type,
                                relationship.src,
                                relationship.dst,
                                relationship.relationship_id,
                            )
                    {
                        return Err(corrupt_checkpoint(
                            &key,
                            "canonical relationship identity mismatch",
                        ));
                    }
                    endpoints.extend([relationship.src, relationship.dst]);
                    1
                }
            };
            endpoints.sort_unstable();
            endpoints.dedup();
            for vertex in endpoints {
                QueryBudget::new(None, crate::QueryCancellationToken::current())
                    .check(MIGRATION)?;
                let vertex_key = keys::vertex(cell_id, vertex);
                match read_txn_remote(txn, &vertex_key).await? {
                    Some(value) => {
                        bytes = bytes
                            .saturating_add(vertex_key.len())
                            .saturating_add(value.len());
                        ensure_limit(
                            "vertex_membership_chunk_bytes",
                            bytes as u64,
                            byte_limit as u64,
                        )?;
                        decode_vertex_metadata(&vertex_key, &value)?;
                    }
                    None if verify => {
                        return Err(corrupt_checkpoint(
                            &vertex_key,
                            "live canonical endpoint is absent during membership verification",
                        ))
                    }
                    None => {
                        txn.put(
                            vertex_key.as_bytes(),
                            encode_vertex_metadata(&VertexMetadata::default()),
                        )?;
                        progress.created = progress.created.checked_add(1).ok_or_else(|| {
                            corrupt_checkpoint(&key, "membership creation counter overflow")
                        })?;
                    }
                }
            }
            progress.examined = progress
                .examined
                .checked_add(consumed as u64)
                .ok_or_else(|| corrupt_checkpoint(&key, "membership scan counter overflow"))?;
            examined += consumed;
            progress.cursor = key;
            progress.offset = offset;
            if offset > 0 {
                break;
            }
        }
        Ok(())
    }
}
