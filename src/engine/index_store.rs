use super::*;
use futures::StreamExt;
use sha2::{Digest, Sha256};
use slatedb::bytes::Bytes;
use slatedb::object_store::{ObjectStoreExt, PutMode, UpdateVersion};

const INDEX_MANIFEST_MAGIC: &str = "turbolay-index-current-v1";
const INDEX_CSC_MAGIC: &[u8] = b"turbolay-index-csc-v1\0";
const INDEX_PUBLISH_ATTEMPTS: usize = 8;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GraphIndexGeneration {
    pub cell_id: String,
    pub edge_type: String,
    pub base_sequence: StorageSequence,
    pub last_wal_id: u64,
    pub edge_count: u64,
    pub checksum: u64,
    pub generation: String,
}

/// Which path produced a generation and how much work it did. The indexer
/// exports these as Prometheus counters, so "the graph previously scanned N
/// edges per cycle, now it applies M delta edges" is a dashboard query, not
/// an anecdote.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GraphIndexBuildPath {
    /// Full canonical scan and re-encode: `edges` is the total edge count of
    /// the published generation — every one of them was scanned from storage.
    Full { edges: u64 },
    /// WAL-tail patch of the previous generation: `delta_edges` is the number
    /// of changed edges actually applied — the only work performed.
    Incremental { delta_edges: u64 },
    /// The published generation already covers the current sequence; no work.
    Current,
}

impl GraphShard {
    pub async fn dirty_graph_index_edge_types(
        &self,
        cell_id: &str,
    ) -> Result<Vec<(String, StorageSequence)>> {
        validate_component("cell_id", cell_id)?;
        let mut result = Vec::new();
        let prefix = crate::keys::matrix_dirty_prefix(cell_id);
        let mut iter = self.scan_remote_prefix(&prefix).await?;
        while let Some(kv) = iter.next().await? {
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            let edge_type = key
                .strip_prefix(&prefix)
                .filter(|value| !value.is_empty())
                .ok_or_else(|| GraphError::CorruptValue {
                    key: key.clone(),
                    reason: "invalid graph index dirty marker key".to_string(),
                })?
                .to_string();
            validate_component("edge_type", &edge_type)?;
            result.push((edge_type, decode_u64(&key, &kv.value)?));
        }
        result.sort_by(|left, right| left.0.cmp(&right.0));
        Ok(result)
    }

    pub async fn current_graph_index(
        &self,
        cell_id: &str,
        edge_type: &str,
    ) -> Result<Option<GraphIndexGeneration>> {
        validate_component("cell_id", cell_id)?;
        validate_component("edge_type", edge_type)?;
        let path = graph_index_manifest_path(self.db.store_path(), cell_id, edge_type);
        let result = match self.db.object_store().get(&path).await {
            Ok(result) => result,
            Err(slatedb::object_store::Error::NotFound { .. }) => return Ok(None),
            Err(err) => return Err(err.into()),
        };
        let value = result.bytes().await?;
        let manifest = decode_graph_index_manifest(path.as_ref(), &value)?;
        if manifest.cell_id != cell_id || manifest.edge_type != edge_type {
            return corrupt(path.as_ref(), "index manifest identity does not match path");
        }
        Ok(Some(manifest))
    }

    pub async fn discover_graph_index(
        &self,
        cell_id: &str,
        edge_type: &str,
    ) -> Result<Option<GraphIndexGeneration>> {
        let Some(generation) = self.current_graph_index(cell_id, edge_type).await? else {
            return Ok(None);
        };
        let key = MatrixCacheKey::new(cell_id, edge_type, generation.base_sequence);
        self.graph_index_generations
            .lock()
            .await
            .insert(key, generation.clone());
        let artifact = MatrixArtifact {
            cell_id: generation.cell_id.clone(),
            edge_type: generation.edge_type.clone(),
            base_epoch: generation.base_sequence,
            tile_size: 0,
            out_tiles: 0,
            transpose_tiles: 0,
            edge_count: generation.edge_count,
        };
        self.matrix_artifact_cache.lock().await.insert(
            MatrixCacheKey::new(cell_id, edge_type, generation.base_sequence),
            artifact.clone(),
            cell_id.to_string(),
            self.cache_policy.pin_matrix_artifact(&artifact),
            &self.cache_metrics,
        );
        Ok(Some(generation))
    }

    pub async fn build_graph_index(
        &self,
        cell_id: &str,
        edge_type: &str,
    ) -> Result<GraphIndexGeneration> {
        validate_component("cell_id", cell_id)?;
        validate_component("edge_type", edge_type)?;
        let _permit = self
            .acquire_artifact_build_permit("build_graph_index")
            .await?;
        self.db.refresh_durable_reader().await?;
        let snapshot = self.db.snapshot().await?;
        let base_sequence = snapshot.seq();
        let last_wal_id = snapshot.last_wal_id().unwrap_or(0);
        let adjacency = GraphStore::scope_snapshot(snapshot, async {
            self.canonical_adjacency_at(cell_id, edge_type, base_sequence)
                .await
        })
        .await?;
        let csc = graphblas_csc_from_adjacency(&adjacency)?;
        let edge_count = csc.indices.len() as u64;
        ensure_limit(
            "build_graph_index_edges",
            edge_count,
            self.limits.max_artifact_build_edges,
        )?;
        let checksum = graphblas_csc_checksum(&csc);
        let payload = encode_graph_index_csc(base_sequence, last_wal_id, checksum, &csc);
        let generation = sha256_hex(&payload);
        let manifest = GraphIndexGeneration {
            cell_id: cell_id.to_string(),
            edge_type: edge_type.to_string(),
            base_sequence,
            last_wal_id,
            edge_count,
            checksum,
            generation,
        };
        let published = self.publish_graph_index(&manifest, payload).await?;
        self.graph_index_generations.lock().await.insert(
            MatrixCacheKey::new(cell_id, edge_type, published.base_sequence),
            published.clone(),
        );
        Ok(published)
    }

    /// Incremental variant of [`GraphShard::build_graph_index`]: instead of
    /// re-scanning every canonical record for the edge type (the full build's
    /// `canonical_adjacency_at` above), load the previously published CSC
    /// generation and patch it with the WAL tail — the exact delta the read
    /// path already computes in `topology_tail_since`
    /// (`src/shard/topology_tail.rs:41`) and then throws away.
    ///
    /// On success returns the generation together with a
    /// [`GraphIndexBuildPath`] carrying the work counters (delta edges
    /// applied vs total edges scanned) that the indexer exports to
    /// Prometheus — the before/after impact number IS this counter.
    ///
    /// Returns `Ok(None)` whenever an incremental build is not possible (no
    /// previous generation, generation payload missing, WAL tail unavailable
    /// because SlateDB already collected those WAL files); the caller falls
    /// back to the full rebuild.
    ///
    /// Correctness oracle: generations are content-addressed
    /// (`generation = sha256(payload)` above), so a finished implementation
    /// must produce a byte-identical payload — and therefore an identical
    /// generation id — to a full rebuild taken at the same snapshot. The
    /// ignored test `incremental_graph_index_matches_full_rebuild` in
    /// `src/tests.rs` asserts exactly that; un-ignore it once the steps below
    /// are filled in.
    // TODO(soham): remove this `allow` once every `todo!()` below is replaced.
    #[allow(unused_variables, unreachable_code, clippy::diverging_sub_expression)]
    pub async fn build_graph_index_incremental(
        &self,
        cell_id: &str,
        edge_type: &str,
    ) -> Result<Option<(GraphIndexGeneration, GraphIndexBuildPath)>> {
        validate_component("cell_id", cell_id)?;
        validate_component("edge_type", edge_type)?;

        let _permit = self
            .acquire_artifact_build_permit("build_graph_index_incremental")
            .await?;
        self.db.refresh_durable_reader().await?;
        let snapshot = self.db.snapshot().await?;
        let base_sequence = snapshot.seq();
        let last_wal_id = snapshot.last_wal_id().unwrap_or(0);

        let Some(previous) = self.current_graph_index(cell_id, edge_type).await? else {
            return Ok(None);
        };
        if previous.base_sequence >= base_sequence {
            return Ok(Some((previous, GraphIndexBuildPath::Current)));
        }

        // The WAL tail is the exact delta the read path computes per query in
        // `topology_tail_since` (`src/shard/query.rs:5525`) and then discards;
        // here it is computed once and folded into the published index. No
        // runtime budget — the WAL entry/edge limits inside the call still
        // apply. `Unavailable` means a WAL file spanning the gap was already
        // collected, so the delta is unrecoverable and the caller must run a
        // full rebuild instead.
        let budget = crate::shard::QueryBudget::new(None, None);
        let overlay = match self
            .topology_tail_since(&previous, snapshot.as_ref(), base_sequence, &budget)
            .await?
        {
            crate::shard::topology_tail::GraphTopologyTail::Complete(overlay) => overlay,
            crate::shard::topology_tail::GraphTopologyTail::Unavailable => {
                return Ok(None);
            }
        };

        // ── STEP 3 (yours): decode the previous generation back into an
        // adjacency map.
        //
        //     self.graph_index_csc(&previous).await?   → Option<GraphBlasCsc>
        //         (None → payload already GC'd → return Ok(None))
        //     csc.to_adjacency()?                      → Adjacency
        //         (src/sparse_kernel/mod.rs:93; Adjacency is
        //          BTreeMap<VertexId, BTreeSet<VertexId>>)
        //
        // Rust book: ch 8 (collections), ch 6 (Option patterns).
        let mut adjacency: crate::sparse_kernel::Adjacency =
            todo!("STEP 3: previous CSC → adjacency");

        // ── STEP 4 (yours): patch the adjacency with the overlay.
        //
        // `overlay.entries()` yields `(src, dst, exists)`:
        //     exists == true  → insert `dst` into `adjacency[src]`
        //                       (hint: the `entry(..).or_default()` API)
        //     exists == false → remove `dst` from `adjacency[src]`
        //
        // ⚠ Normalization gotcha: the full build only materializes sources
        // that still have at least one destination (they come from a scan of
        // records that exist). If a removal empties a source's set, remove
        // the source key too — otherwise the CSC vertex dictionary differs
        // from a full rebuild's and STEP 6's checksum equality fails. The
        // test catches this; try leaving it out once and watch it fail.
        //
        // Rust book: ch 8 (entry API), ch 13 (iterators).
        todo!("STEP 4: apply overlay deltas to adjacency");

        // ── STEP 5 (yours): re-encode, mirroring the full build above
        // line-for-line — same functions, same order:
        //
        //     let csc = graphblas_csc_from_adjacency(&adjacency)?;
        //     let edge_count = csc.indices.len() as u64;
        //     ensure_limit("build_graph_index_edges", edge_count,
        //                  self.limits.max_artifact_build_edges)?;
        //     let checksum = graphblas_csc_checksum(&csc);
        //     let payload = encode_graph_index_csc(base_sequence, last_wal_id,
        //                                          checksum, &csc);
        //     let generation = sha256_hex(&payload);
        //     ... assemble GraphIndexGeneration { .. } exactly as above.
        let (manifest, payload): (GraphIndexGeneration, Vec<u8>) =
            todo!("STEP 5: encode the new generation");

        // ── STEP 6 (done for you): publish through the same CAS path as the
        // full build — monotonicity guard and generation cache come for free.
        // `delta_edges` is the impact counter: the work this build actually
        // did, vs the `edge_count` a full rebuild would have scanned.
        let delta_edges = overlay.entries().count() as u64;
        let published = self.publish_graph_index(&manifest, payload).await?;
        self.graph_index_generations.lock().await.insert(
            MatrixCacheKey::new(cell_id, edge_type, published.base_sequence),
            published.clone(),
        );
        Ok(Some((
            published,
            GraphIndexBuildPath::Incremental { delta_edges },
        )))
    }

    /// Incremental-first entry point for the indexer loop
    /// (`src/bin/graph-indexer.rs:904` calls `build_graph_index` today):
    /// try the delta path, fall back to the full rebuild whenever it
    /// declines. Wire this into the indexer as the final step — after the
    /// equivalence test passes — so the exported counters record which path
    /// ran and how much work it did.
    pub async fn build_graph_index_auto(
        &self,
        cell_id: &str,
        edge_type: &str,
    ) -> Result<(GraphIndexGeneration, GraphIndexBuildPath)> {
        if let Some(result) = self
            .build_graph_index_incremental(cell_id, edge_type)
            .await?
        {
            return Ok(result);
        }
        let generation = self.build_graph_index(cell_id, edge_type).await?;
        let edges = generation.edge_count;
        Ok((generation, GraphIndexBuildPath::Full { edges }))
    }

    pub(crate) async fn graph_index_csc(
        &self,
        generation: &GraphIndexGeneration,
    ) -> Result<Option<GraphBlasCsc>> {
        let path = graph_index_generation_path(
            self.db.store_path(),
            &generation.cell_id,
            &generation.edge_type,
            generation.base_sequence,
            &generation.generation,
        );
        let result = match self.db.object_store().get(&path).await {
            Ok(result) => result,
            Err(slatedb::object_store::Error::NotFound { .. }) => return Ok(None),
            Err(err) => return Err(err.into()),
        };
        let value = match result.bytes().await {
            Ok(value) => value,
            Err(slatedb::object_store::Error::NotFound { .. }) => return Ok(None),
            Err(err) => return Err(err.into()),
        };
        let csc = decode_graph_index_csc(
            path.as_ref(),
            &value,
            generation.base_sequence,
            generation.last_wal_id,
            generation.checksum,
        )?;
        if csc.indices.len() as u64 != generation.edge_count {
            return corrupt(path.as_ref(), "index edge count does not match manifest");
        }
        Ok(Some(csc))
    }

    pub(crate) async fn forget_graph_index_generation(&self, generation: &GraphIndexGeneration) {
        let key = MatrixCacheKey::new(
            &generation.cell_id,
            &generation.edge_type,
            generation.base_sequence,
        );
        self.graph_index_generations.lock().await.remove(&key);
        self.graphblas_cache.lock().await.remove(&key);
    }

    pub(crate) async fn graph_index_generation_at(
        &self,
        cell_id: &str,
        edge_type: &str,
        base_sequence: StorageSequence,
    ) -> Result<Option<GraphIndexGeneration>> {
        let key = MatrixCacheKey::new(cell_id, edge_type, base_sequence);
        if let Some(generation) = self.graph_index_generations.lock().await.get(&key).cloned() {
            return Ok(Some(generation));
        }
        Ok(self
            .discover_graph_index(cell_id, edge_type)
            .await?
            .filter(|generation| generation.base_sequence == base_sequence))
    }

    pub async fn gc_graph_index_generations(
        &self,
        cell_id: &str,
        edge_type: &str,
        retain_previous: usize,
    ) -> Result<u64> {
        validate_component("cell_id", cell_id)?;
        validate_component("edge_type", edge_type)?;
        let Some(current) = self.current_graph_index(cell_id, edge_type).await? else {
            return Ok(0);
        };
        let prefix = graph_index_generations_prefix(self.db.store_path(), cell_id, edge_type);
        let mut stream = self.db.object_store().list(Some(&prefix));
        let mut older = Vec::new();
        while let Some(meta) = stream.next().await.transpose()? {
            let base_sequence = graph_index_sequence_from_path(meta.location.as_ref())?;
            if base_sequence < current.base_sequence {
                older.push((base_sequence, meta.location));
            }
        }
        older.sort_by(|left, right| right.0.cmp(&left.0).then_with(|| left.1.cmp(&right.1)));
        let mut deleted = 0_u64;
        for (_, location) in older.into_iter().skip(retain_previous) {
            self.db.object_store().delete(&location).await?;
            deleted = deleted.saturating_add(1);
        }
        Ok(deleted)
    }

    async fn publish_graph_index(
        &self,
        proposed: &GraphIndexGeneration,
        payload: Vec<u8>,
    ) -> Result<GraphIndexGeneration> {
        let generation_path = graph_index_generation_path(
            self.db.store_path(),
            &proposed.cell_id,
            &proposed.edge_type,
            proposed.base_sequence,
            &proposed.generation,
        );
        match self
            .db
            .object_store()
            .put_opts(
                &generation_path,
                Bytes::from(payload).into(),
                PutMode::Create.into(),
            )
            .await
        {
            Ok(_) | Err(slatedb::object_store::Error::AlreadyExists { .. }) => {}
            Err(err) => return Err(err.into()),
        }

        let manifest_path =
            graph_index_manifest_path(self.db.store_path(), &proposed.cell_id, &proposed.edge_type);
        let manifest_payload = Bytes::from(encode_graph_index_manifest(proposed));
        for _ in 0..INDEX_PUBLISH_ATTEMPTS {
            let current = match self.db.object_store().get(&manifest_path).await {
                Ok(result) => Some(result),
                Err(slatedb::object_store::Error::NotFound { .. }) => None,
                Err(err) => return Err(err.into()),
            };
            let mode = if let Some(current) = current {
                let version = UpdateVersion {
                    e_tag: current.meta.e_tag.clone(),
                    version: current.meta.version.clone(),
                };
                let value = current.bytes().await?;
                let published = decode_graph_index_manifest(manifest_path.as_ref(), &value)?;
                if published.base_sequence > proposed.base_sequence
                    || (published.base_sequence == proposed.base_sequence
                        && published.last_wal_id >= proposed.last_wal_id)
                {
                    return Ok(published);
                }
                PutMode::Update(version)
            } else {
                PutMode::Create
            };
            match self
                .db
                .object_store()
                .put_opts(&manifest_path, manifest_payload.clone().into(), mode.into())
                .await
            {
                Ok(_) => return Ok(proposed.clone()),
                Err(slatedb::object_store::Error::AlreadyExists { .. })
                | Err(slatedb::object_store::Error::Precondition { .. })
                | Err(slatedb::object_store::Error::NotFound { .. }) => continue,
                Err(err) => return Err(err.into()),
            }
        }
        Err(GraphError::ConditionalWriteConflict {
            operation: "publish_graph_index",
            key: manifest_path.to_string(),
        })
    }
}

fn graph_index_root(store_path: &slatedb::object_store::path::Path) -> String {
    format!("{store_path}/_graph_index")
}

fn graph_index_manifest_path(
    store_path: &slatedb::object_store::path::Path,
    cell_id: &str,
    edge_type: &str,
) -> slatedb::object_store::path::Path {
    format!(
        "{}/{cell_id}/{edge_type}/current",
        graph_index_root(store_path)
    )
    .into()
}

fn graph_index_generation_path(
    store_path: &slatedb::object_store::path::Path,
    cell_id: &str,
    edge_type: &str,
    base_sequence: StorageSequence,
    generation: &str,
) -> slatedb::object_store::path::Path {
    format!(
        "{}/{cell_id}/{edge_type}/generations/{base_sequence:020}-{generation}.csc",
        graph_index_root(store_path)
    )
    .into()
}

fn graph_index_generations_prefix(
    store_path: &slatedb::object_store::path::Path,
    cell_id: &str,
    edge_type: &str,
) -> slatedb::object_store::path::Path {
    format!(
        "{}/{cell_id}/{edge_type}/generations",
        graph_index_root(store_path)
    )
    .into()
}

fn encode_graph_index_manifest(manifest: &GraphIndexGeneration) -> Vec<u8> {
    format!(
        "{INDEX_MANIFEST_MAGIC}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n",
        manifest.cell_id,
        manifest.edge_type,
        manifest.base_sequence,
        manifest.last_wal_id,
        manifest.edge_count,
        manifest.checksum,
        manifest.generation,
    )
    .into_bytes()
}

fn decode_graph_index_manifest(key: &str, value: &[u8]) -> Result<GraphIndexGeneration> {
    let text = text_value(key, value)?;
    let fields = text.trim_end_matches('\n').split('\t').collect::<Vec<_>>();
    if fields.len() != 8 || fields[0] != INDEX_MANIFEST_MAGIC {
        return corrupt(key, "expected turbolay index current v1 manifest");
    }
    validate_component("cell_id", fields[1])?;
    validate_component("edge_type", fields[2])?;
    if fields[7].len() != 64 || !fields[7].bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return corrupt(key, "index generation must be a SHA-256 digest");
    }
    Ok(GraphIndexGeneration {
        cell_id: fields[1].to_string(),
        edge_type: fields[2].to_string(),
        base_sequence: parse_u64(key, fields[3], "base_sequence")?,
        last_wal_id: parse_u64(key, fields[4], "last_wal_id")?,
        edge_count: parse_u64(key, fields[5], "edge_count")?,
        checksum: parse_u64(key, fields[6], "checksum")?,
        generation: fields[7].to_string(),
    })
}

fn encode_graph_index_csc(
    base_sequence: u64,
    last_wal_id: u64,
    checksum: u64,
    csc: &GraphBlasCsc,
) -> Vec<u8> {
    let mut value = Vec::with_capacity(
        INDEX_CSC_MAGIC.len()
            + 6 * std::mem::size_of::<u64>()
            + (csc.vertices.len() + csc.pointers.len() + csc.indices.len())
                * std::mem::size_of::<u64>(),
    );
    value.extend_from_slice(INDEX_CSC_MAGIC);
    value.extend_from_slice(&base_sequence.to_le_bytes());
    value.extend_from_slice(&last_wal_id.to_le_bytes());
    value.extend_from_slice(&checksum.to_le_bytes());
    encode_index_u64s(&mut value, &csc.vertices);
    encode_index_u64s(&mut value, &csc.pointers);
    encode_index_u64s(&mut value, &csc.indices);
    value
}

fn decode_graph_index_csc(
    key: &str,
    value: &[u8],
    expected_sequence: u64,
    expected_wal_id: u64,
    expected_checksum: u64,
) -> Result<GraphBlasCsc> {
    if !value.starts_with(INDEX_CSC_MAGIC) {
        return corrupt(key, "expected turbolay index CSC v1 payload");
    }
    let mut cursor = INDEX_CSC_MAGIC.len();
    let base_sequence = decode_index_u64(key, value, &mut cursor, "base_sequence")?;
    let last_wal_id = decode_index_u64(key, value, &mut cursor, "last_wal_id")?;
    let checksum = decode_index_u64(key, value, &mut cursor, "checksum")?;
    if (base_sequence, last_wal_id, checksum)
        != (expected_sequence, expected_wal_id, expected_checksum)
    {
        return corrupt(key, "index CSC identity does not match manifest");
    }
    let csc = GraphBlasCsc {
        vertices: decode_index_u64s(key, value, &mut cursor, "vertices")?,
        pointers: decode_index_u64s(key, value, &mut cursor, "pointers")?,
        indices: decode_index_u64s(key, value, &mut cursor, "indices")?,
    };
    if cursor != value.len() {
        return corrupt(key, "trailing bytes in index CSC payload");
    }
    validate_graphblas_csc_artifact(key, &csc)?;
    if graphblas_csc_checksum(&csc) != checksum {
        return corrupt(key, "index CSC checksum mismatch");
    }
    Ok(csc)
}

fn graph_index_sequence_from_path(key: &str) -> Result<StorageSequence> {
    let file_name = key.rsplit('/').next().unwrap_or(key);
    let sequence = file_name
        .split_once('-')
        .map(|(sequence, _)| sequence)
        .ok_or_else(|| GraphError::CorruptValue {
            key: key.to_string(),
            reason: "index generation key is missing its sequence prefix".to_string(),
        })?;
    if sequence.len() != 20 || !sequence.bytes().all(|byte| byte.is_ascii_digit()) {
        return corrupt(
            key,
            "index generation sequence must be a 20-digit decimal prefix",
        );
    }
    parse_u64(key, sequence, "base_sequence")
}

fn encode_index_u64s(out: &mut Vec<u8>, values: &[u64]) {
    out.extend_from_slice(&(values.len() as u64).to_le_bytes());
    for value in values {
        out.extend_from_slice(&value.to_le_bytes());
    }
}

fn decode_index_u64(key: &str, value: &[u8], cursor: &mut usize, field: &str) -> Result<u64> {
    let end = cursor.saturating_add(std::mem::size_of::<u64>());
    let bytes = value
        .get(*cursor..end)
        .ok_or_else(|| GraphError::CorruptValue {
            key: key.to_string(),
            reason: format!("truncated {field}"),
        })?;
    *cursor = end;
    Ok(u64::from_le_bytes(
        bytes.try_into().expect("u64 byte width"),
    ))
}

fn decode_index_u64s(key: &str, value: &[u8], cursor: &mut usize, field: &str) -> Result<Vec<u64>> {
    let len = usize::try_from(decode_index_u64(key, value, cursor, field)?).map_err(|err| {
        GraphError::CorruptValue {
            key: key.to_string(),
            reason: format!("{field} length does not fit memory: {err}"),
        }
    })?;
    let byte_len =
        len.checked_mul(std::mem::size_of::<u64>())
            .ok_or_else(|| GraphError::CorruptValue {
                key: key.to_string(),
                reason: format!("{field} byte length overflow"),
            })?;
    let end = cursor
        .checked_add(byte_len)
        .ok_or_else(|| GraphError::CorruptValue {
            key: key.to_string(),
            reason: format!("{field} cursor overflow"),
        })?;
    let bytes = value
        .get(*cursor..end)
        .ok_or_else(|| GraphError::CorruptValue {
            key: key.to_string(),
            reason: format!("truncated {field}"),
        })?;
    *cursor = end;
    Ok(bytes
        .chunks_exact(std::mem::size_of::<u64>())
        .map(|chunk| u64::from_le_bytes(chunk.try_into().expect("u64 byte width")))
        .collect())
}

fn sha256_hex(value: &[u8]) -> String {
    Sha256::digest(value)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}
