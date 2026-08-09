use super::*;

pub(crate) async fn read_txn_remote(txn: &DbTransaction, key: &str) -> Result<Option<Bytes>> {
    txn.mark_read([key.as_bytes()])?;
    Ok(txn
        .get_with_options(key.as_bytes(), &remote_read_options())
        .await?)
}

pub(crate) async fn out_neighbors_for_src_txn(
    txn: &DbTransaction,
    cell_id: &str,
    edge_type: &str,
    src: VertexId,
    read_epoch: StorageSequence,
) -> Result<BTreeSet<VertexId>> {
    let mut neighbors = BTreeSet::new();

    {
        let prefix = keys::out_prefix(cell_id, edge_type, src);
        let mut iter = txn.scan_prefix(prefix.as_bytes(), ..).await?;
        while let Some(kv) = iter.next().await? {
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            let record = decode_edge_record(&key, &kv.value)?;
            neighbors.insert(record.dst);
        }
    }

    neighbors
        .extend(out_segment_neighbors_for_src_txn(txn, cell_id, edge_type, src, read_epoch).await?);
    Ok(neighbors)
}

pub(crate) async fn edge_epoch_at_txn(
    txn: &DbTransaction,
    cell_id: &str,
    edge_type: &str,
    src: VertexId,
    dst: VertexId,
    read_epoch: StorageSequence,
) -> Result<Option<StorageSequence>> {
    let edge_key = keys::out_edge(cell_id, edge_type, src, dst);
    if let Some(value) = read_txn_remote(txn, &edge_key).await? {
        decode_edge_record(&edge_key, &value)?;
        return Ok(Some(read_epoch));
    }
    Ok(
        out_segment_edges_for_src_txn(txn, cell_id, edge_type, src, read_epoch)
            .await?
            .get(&dst)
            .copied(),
    )
}

pub(crate) async fn out_segment_neighbors_for_src_txn(
    txn: &DbTransaction,
    cell_id: &str,
    edge_type: &str,
    src: VertexId,
    read_epoch: StorageSequence,
) -> Result<BTreeSet<VertexId>> {
    Ok(
        out_segment_edges_for_src_txn(txn, cell_id, edge_type, src, read_epoch)
            .await?
            .into_keys()
            .collect(),
    )
}

pub(crate) async fn out_segment_edges_for_src_txn(
    txn: &DbTransaction,
    cell_id: &str,
    edge_type: &str,
    src: VertexId,
    read_epoch: StorageSequence,
) -> Result<BTreeMap<VertexId, StorageSequence>> {
    let mut edges = BTreeMap::<VertexId, StorageSequence>::new();
    let mut tombstones = BTreeMap::<VertexId, StorageSequence>::new();
    {
        let prefix = keys::out_segment_tombstone_src_prefix(cell_id, edge_type, src);
        let mut iter = txn.scan_prefix(prefix.as_bytes(), ..).await?;
        while let Some(kv) = iter.next().await? {
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            let (key_cell_id, key_edge_type, key_src, dst) =
                parse_out_edge_segment_tombstone_key(&key)?;
            if key_cell_id != cell_id || key_edge_type != edge_type || key_src != src {
                return Err(GraphError::CorruptValue {
                    key,
                    reason: "segment tombstone identity does not match scan prefix".to_string(),
                });
            }
            let epoch = decode_u64(&key, &kv.value)?;
            if epoch <= read_epoch {
                tombstones.insert(dst, epoch);
            }
        }
    }

    {
        let prefix = keys::out_segment_src_prefix(cell_id, edge_type, src);
        let mut iter = txn.scan_prefix(prefix.as_bytes(), ..).await?;
        while let Some(kv) = iter.next().await? {
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            let segment = decode_out_edge_segment(&key, &kv.value)?;
            if segment.storage_sequence > read_epoch {
                break;
            }
            for dst in segment.destinations.iter().copied() {
                if segment_edge_visible(segment.storage_sequence, tombstones.get(&dst).copied()) {
                    edges
                        .entry(dst)
                        .and_modify(|current| *current = (*current).max(segment.storage_sequence))
                        .or_insert(segment.storage_sequence);
                }
            }
        }
    }
    Ok(edges)
}

pub(crate) async fn read_counter_txn(txn: &DbTransaction, key: &str) -> Result<u64> {
    match read_txn_remote(txn, key).await? {
        Some(value) => decode_u64(key, &value),
        None => Ok(0),
    }
}

pub(crate) async fn next_epoch_txn(txn: &DbTransaction, cell_id: &str) -> Result<StorageSequence> {
    txn.seqnum()
        .checked_add(1)
        .ok_or_else(|| GraphError::CorruptValue {
            key: format!("cell/{cell_id}/storage_sequence"),
            reason: "SlateDB storage sequence overflow".to_string(),
        })
}

pub(crate) async fn commit_txn_strict(txn: DbTransaction, await_durable: bool) -> Result<()> {
    commit_txn_strict_with_sequence(txn, await_durable)
        .await
        .map(|_| ())
}

pub(crate) async fn commit_txn_strict_with_sequence(
    txn: DbTransaction,
    await_durable: bool,
) -> Result<Option<crate::StorageSequence>> {
    let sequence = txn
        .seqnum()
        .checked_add(1)
        .ok_or_else(|| GraphError::CorruptValue {
            key: "storage_sequence".to_string(),
            reason: "SlateDB storage sequence overflow".to_string(),
        })?;
    let options = WriteOptions {
        await_durable,
        seqnum: sequence,
    };
    let handle = txn.commit_with_options(&options).await?;
    Ok(handle.map(|handle| handle.seqnum()))
}

pub(crate) fn remote_read_options() -> ReadOptions {
    ReadOptions {
        durability_filter: DurabilityLevel::Remote,
        ..Default::default()
    }
}

pub(crate) fn remote_scan_options() -> ScanOptions {
    ScanOptions::default()
        .with_durability_filter(DurabilityLevel::Remote)
        .with_cache_blocks(false)
}

const MAX_CACHED_REMOTE_SCAN_ITEMS: u64 = 1_024;

pub(crate) fn remote_scan_options_for_expected_items(expected_items: u64) -> ScanOptions {
    ScanOptions::default()
        .with_durability_filter(DurabilityLevel::Remote)
        .with_cache_blocks(expected_items <= MAX_CACHED_REMOTE_SCAN_ITEMS)
}

pub(crate) fn validate_component(component: &'static str, value: &str) -> Result<()> {
    if value.is_empty()
        || !value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-' | b'.'))
    {
        return Err(GraphError::InvalidKeyComponent {
            component,
            value: value.to_string(),
        });
    }
    Ok(())
}

pub(crate) fn validate_edge_mutations_for_cell(
    cell_id: &str,
    mutations: &[EdgeMutation],
    operation: &'static str,
) -> Result<()> {
    let mut idempotency_keys = BTreeSet::new();
    for mutation in mutations {
        validate_component("cell_id", &mutation.cell_id)?;
        validate_component("edge_type", &mutation.edge_type)?;
        validate_component("idempotency_key", &mutation.idempotency_key)?;
        if mutation.cell_id != cell_id {
            return Err(GraphError::CorruptValue {
                key: format!("cell/{cell_id}/{operation}"),
                reason: format!(
                    "batch contains mutation for different cell {}",
                    mutation.cell_id
                ),
            });
        }
        if !idempotency_keys.insert(mutation.idempotency_key.clone()) {
            return Err(GraphError::IdempotencyConflict {
                operation: "create",
                idempotency_key: mutation.idempotency_key.clone(),
                reason: "the same key appears twice in one batch",
            });
        }
    }
    Ok(())
}

pub(crate) fn encode_u64(value: u64) -> Vec<u8> {
    value.to_be_bytes().to_vec()
}

pub(crate) fn decode_u64(key: &str, value: &[u8]) -> Result<u64> {
    let bytes: [u8; 8] = value.try_into().map_err(|_| GraphError::CorruptValue {
        key: key.to_string(),
        reason: format!("expected 8 bytes, got {}", value.len()),
    })?;
    Ok(u64::from_be_bytes(bytes))
}

#[cfg(feature = "opencypher")]
pub(crate) fn encode_query_stats_record(record: &QueryStatsRecord) -> Vec<u8> {
    format!(
        "query-stats-v1\ncount\t{}\nread_epoch\t{}\nrefreshed_at_ms\t{}\ndistinct_values\t{}\ntotal_values\t{}\nmost_common_count\t{}\n",
        record.count,
        record.read_epoch,
        record.refreshed_at_ms,
        record.distinct_values,
        record.total_values,
        record.most_common_count,
    )
    .into_bytes()
}

#[cfg(feature = "opencypher")]
pub(crate) fn decode_query_stats_record(key: &str, value: &[u8]) -> Result<QueryStatsRecord> {
    if value.len() == 8 {
        let count = decode_u64(key, value)?;
        return Ok(QueryStatsRecord::point_count(count, 0, 0));
    }
    let text = std::str::from_utf8(value).map_err(|err| GraphError::CorruptValue {
        key: key.to_string(),
        reason: err.to_string(),
    })?;
    let mut lines = text.lines();
    match lines.next() {
        Some("query-stats-v1") => {}
        Some(other) => {
            return Err(GraphError::CorruptValue {
                key: key.to_string(),
                reason: format!("unsupported query stats version {other}"),
            });
        }
        None => {
            return Err(GraphError::CorruptValue {
                key: key.to_string(),
                reason: "empty query stats record".to_string(),
            });
        }
    }

    let mut count = None;
    let mut read_epoch = None;
    let mut refreshed_at_ms = None;
    let mut distinct_values = None;
    let mut total_values = None;
    let mut most_common_count = None;
    for line in lines {
        let parts: Vec<_> = line.split('\t').collect();
        let [field, value] = parts.as_slice() else {
            return Err(GraphError::CorruptValue {
                key: key.to_string(),
                reason: format!("invalid query stats line {line}"),
            });
        };
        let parsed = value
            .parse::<u64>()
            .map_err(|err| GraphError::CorruptValue {
                key: key.to_string(),
                reason: format!("invalid query stats {field} value {value}: {err}"),
            })?;
        match *field {
            "count" => count = Some(parsed),
            "read_epoch" => read_epoch = Some(parsed),
            "refreshed_at_ms" => refreshed_at_ms = Some(parsed),
            "distinct_values" => distinct_values = Some(parsed),
            "total_values" => total_values = Some(parsed),
            "most_common_count" => most_common_count = Some(parsed),
            other => {
                return Err(GraphError::CorruptValue {
                    key: key.to_string(),
                    reason: format!("unknown query stats field {other}"),
                });
            }
        }
    }
    Ok(QueryStatsRecord {
        count: required_query_stats_field(key, "count", count)?,
        read_epoch: required_query_stats_field(key, "read_epoch", read_epoch)?,
        refreshed_at_ms: required_query_stats_field(key, "refreshed_at_ms", refreshed_at_ms)?,
        distinct_values: required_query_stats_field(key, "distinct_values", distinct_values)?,
        total_values: required_query_stats_field(key, "total_values", total_values)?,
        most_common_count: required_query_stats_field(key, "most_common_count", most_common_count)?,
    })
}

#[cfg(feature = "opencypher")]
fn required_query_stats_field(key: &str, field: &'static str, value: Option<u64>) -> Result<u64> {
    value.ok_or_else(|| GraphError::CorruptValue {
        key: key.to_string(),
        reason: format!("missing query stats field {field}"),
    })
}

pub(crate) fn encode_vertex_metadata(metadata: &VertexMetadata) -> Vec<u8> {
    let mut value = String::from("vertex-metadata-v1\n");
    for label in &metadata.labels {
        value.push_str("label\t");
        value.push_str(label);
        value.push('\n');
    }
    for (name, property) in &metadata.properties {
        value.push_str("property\t");
        value.push_str(name);
        value.push('\t');
        value.push_str(&encode_vertex_property_value_record(property));
        value.push('\n');
    }
    value.into_bytes()
}

pub(crate) fn decode_vertex_metadata(key: &str, value: &[u8]) -> Result<VertexMetadata> {
    let text = std::str::from_utf8(value).map_err(|err| GraphError::CorruptValue {
        key: key.to_string(),
        reason: err.to_string(),
    })?;
    let mut lines = text.lines();
    match lines.next() {
        Some("vertex-metadata-v1") => {}
        Some(other) => {
            return Err(GraphError::CorruptValue {
                key: key.to_string(),
                reason: format!("unsupported vertex metadata version {other}"),
            });
        }
        None => {
            return Err(GraphError::CorruptValue {
                key: key.to_string(),
                reason: "empty vertex metadata".to_string(),
            });
        }
    }

    let mut metadata = VertexMetadata::default();
    for line in lines {
        let parts: Vec<_> = line.split('\t').collect();
        match parts.as_slice() {
            ["label", label] => {
                validate_component("label", label)?;
                metadata.labels.insert((*label).to_string());
            }
            ["property", name, encoded] => {
                validate_component("property", name)?;
                metadata.properties.insert(
                    (*name).to_string(),
                    decode_vertex_property_value_record(key, encoded)?,
                );
            }
            _ => {
                return Err(GraphError::CorruptValue {
                    key: key.to_string(),
                    reason: format!("invalid vertex metadata line {line}"),
                });
            }
        }
    }
    Ok(metadata)
}

pub(crate) fn encode_edge_metadata(metadata: &EdgeMetadata) -> Vec<u8> {
    let mut value = String::from("edge-metadata-v1\n");
    for (name, property) in &metadata.properties {
        value.push_str("property\t");
        value.push_str(name);
        value.push('\t');
        value.push_str(&encode_vertex_property_value_record(property));
        value.push('\n');
    }
    value.into_bytes()
}

pub(crate) fn encode_relationship_record(record: &RelationshipRecord) -> Vec<u8> {
    let mut value = String::from("relationship\n");
    for (name, property) in &record.metadata.properties {
        value.push_str("property\t");
        value.push_str(name);
        value.push('\t');
        value.push_str(&encode_vertex_property_value_record(property));
        value.push('\n');
    }
    value.into_bytes()
}

pub(crate) fn decode_relationship_record(key: &str, value: &[u8]) -> Result<RelationshipRecord> {
    let text = std::str::from_utf8(value).map_err(|err| GraphError::CorruptValue {
        key: key.to_string(),
        reason: err.to_string(),
    })?;
    let mut lines = text.lines();
    let header = lines.next().ok_or_else(|| GraphError::CorruptValue {
        key: key.to_string(),
        reason: "empty relationship record".to_string(),
    })?;
    if header != "relationship" {
        return Err(GraphError::CorruptValue {
            key: key.to_string(),
            reason: "expected relationship header".to_string(),
        });
    }
    let mut record = parse_relationship_record_key(key)?;
    for line in lines {
        let parts: Vec<_> = line.split('\t').collect();
        match parts.as_slice() {
            ["property", name, encoded] => {
                validate_component("property", name)?;
                record.metadata.properties.insert(
                    (*name).to_string(),
                    decode_vertex_property_value_record(key, encoded)?,
                );
            }
            _ => {
                return Err(GraphError::CorruptValue {
                    key: key.to_string(),
                    reason: format!("invalid relationship record line {line}"),
                });
            }
        }
    }
    Ok(record)
}

pub(crate) fn decode_edge_metadata(key: &str, value: &[u8]) -> Result<EdgeMetadata> {
    let text = std::str::from_utf8(value).map_err(|err| GraphError::CorruptValue {
        key: key.to_string(),
        reason: err.to_string(),
    })?;
    let mut lines = text.lines();
    match lines.next() {
        Some("edge-metadata-v1") => {}
        Some(other) => {
            return Err(GraphError::CorruptValue {
                key: key.to_string(),
                reason: format!("unsupported edge metadata version {other}"),
            });
        }
        None => {
            return Err(GraphError::CorruptValue {
                key: key.to_string(),
                reason: "empty edge metadata".to_string(),
            });
        }
    }

    let mut metadata = EdgeMetadata::default();
    for line in lines {
        let parts: Vec<_> = line.split('\t').collect();
        match parts.as_slice() {
            ["property", name, encoded] => {
                validate_component("property", name)?;
                metadata.properties.insert(
                    (*name).to_string(),
                    decode_vertex_property_value_record(key, encoded)?,
                );
            }
            _ => {
                return Err(GraphError::CorruptValue {
                    key: key.to_string(),
                    reason: format!("invalid edge metadata line {line}"),
                });
            }
        }
    }
    Ok(metadata)
}

pub(crate) fn encode_vertex_property_value_key(value: &VertexPropertyValue) -> String {
    match value {
        VertexPropertyValue::Integer(value) => format!("i{value:020}"),
        VertexPropertyValue::SignedInteger(value) => {
            format!("j{:016x}", (*value as u64) ^ (1_u64 << 63))
        }
        VertexPropertyValue::Bool(false) => "b0".to_string(),
        VertexPropertyValue::Bool(true) => "b1".to_string(),
        VertexPropertyValue::Float(value) => format!("n{:016x}", sortable_float_bits(value.0)),
        VertexPropertyValue::String(value) => format!("s{}", hex_encode(value.as_bytes())),
    }
}

fn encode_vertex_property_value_record(value: &VertexPropertyValue) -> String {
    match value {
        VertexPropertyValue::Integer(value) => format!("i:{value}"),
        VertexPropertyValue::SignedInteger(value) => format!("j:{value}"),
        VertexPropertyValue::Bool(false) => "b:false".to_string(),
        VertexPropertyValue::Bool(true) => "b:true".to_string(),
        VertexPropertyValue::Float(value) => format!("f:{:016x}", value.0.to_bits()),
        VertexPropertyValue::String(value) => format!("s:{}", hex_encode(value.as_bytes())),
    }
}

fn decode_vertex_property_value_record(key: &str, value: &str) -> Result<VertexPropertyValue> {
    let Some((kind, payload)) = value.split_once(':') else {
        return Err(GraphError::CorruptValue {
            key: key.to_string(),
            reason: format!("invalid vertex property value {value}"),
        });
    };
    match kind {
        "i" => payload
            .parse::<u64>()
            .map(VertexPropertyValue::Integer)
            .map_err(|err| GraphError::CorruptValue {
                key: key.to_string(),
                reason: format!("invalid integer vertex property {payload}: {err}"),
            }),
        "j" => payload
            .parse::<i64>()
            .map(VertexPropertyValue::from_i64)
            .map_err(|err| GraphError::CorruptValue {
                key: key.to_string(),
                reason: format!("invalid signed integer vertex property {payload}: {err}"),
            }),
        "b" => match payload {
            "true" => Ok(VertexPropertyValue::Bool(true)),
            "false" => Ok(VertexPropertyValue::Bool(false)),
            other => Err(GraphError::CorruptValue {
                key: key.to_string(),
                reason: format!("invalid boolean vertex property {other}"),
            }),
        },
        "f" => u64::from_str_radix(payload, 16)
            .map(|bits| VertexPropertyValue::Float(QueryFloat(f64::from_bits(bits))))
            .map_err(|err| GraphError::CorruptValue {
                key: key.to_string(),
                reason: format!("invalid float vertex property {payload}: {err}"),
            }),
        "s" => {
            let bytes = hex_decode(key, payload)?;
            String::from_utf8(bytes)
                .map(VertexPropertyValue::String)
                .map_err(|err| GraphError::CorruptValue {
                    key: key.to_string(),
                    reason: err.to_string(),
                })
        }
        other => Err(GraphError::CorruptValue {
            key: key.to_string(),
            reason: format!("unsupported vertex property kind {other}"),
        }),
    }
}

fn sortable_float_bits(value: f64) -> u64 {
    let bits = value.to_bits();
    if bits & (1 << 63) == 0 {
        bits ^ (1 << 63)
    } else {
        !bits
    }
}

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

fn hex_decode(key: &str, text: &str) -> Result<Vec<u8>> {
    if (text.len() & 1) != 0 {
        return Err(GraphError::CorruptValue {
            key: key.to_string(),
            reason: "odd-length hex string".to_string(),
        });
    }
    let mut bytes = Vec::with_capacity(text.len() / 2);
    for pair in text.as_bytes().chunks_exact(2) {
        let high = hex_value(pair[0]).ok_or_else(|| GraphError::CorruptValue {
            key: key.to_string(),
            reason: format!("invalid hex digit {}", pair[0] as char),
        })?;
        let low = hex_value(pair[1]).ok_or_else(|| GraphError::CorruptValue {
            key: key.to_string(),
            reason: format!("invalid hex digit {}", pair[1] as char),
        })?;
        bytes.push((high << 4) | low);
    }
    Ok(bytes)
}

fn hex_value(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

pub(crate) fn encode_edge_record(_record: &EdgeRecord) -> Vec<u8> {
    b"graph-edge\n".to_vec()
}

pub(crate) fn decode_edge_record(key: &str, value: &[u8]) -> Result<EdgeRecord> {
    if value != b"graph-edge\n" {
        return Err(GraphError::CorruptValue {
            key: key.to_string(),
            reason: "expected graph-edge record".to_string(),
        });
    }
    parse_edge_record_key(key)
}

pub(crate) fn encode_out_edge_segment_records(destinations: &[VertexId]) -> Vec<u8> {
    let mut value = Vec::with_capacity(b"graph-out-segment\n".len() + 8 + destinations.len() * 8);
    value.extend_from_slice(b"graph-out-segment\n");
    value.extend_from_slice(&(destinations.len() as u64).to_be_bytes());
    for dst in destinations {
        value.extend_from_slice(&dst.to_be_bytes());
    }
    value
}

pub(crate) fn encode_segment_compaction_idempotency(
    idempotency_key: &str,
    result: &SegmentCompactionResult,
) -> Vec<u8> {
    format!(
        "segment_compact1\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n",
        result.compacted_through_epoch,
        result.source_segments,
        result.deleted_segment_keys,
        result.deleted_tombstone_keys,
        result.input_edges,
        result.output_edges,
        idempotency_key
    )
    .into_bytes()
}

pub(crate) fn decode_segment_compaction_idempotency(
    key: &str,
    idempotency_key: &str,
    compacted_through_epoch: StorageSequence,
    value: &[u8],
) -> Result<SegmentCompactionResult> {
    let text = std::str::from_utf8(value).map_err(|err| GraphError::CorruptValue {
        key: key.to_string(),
        reason: err.to_string(),
    })?;
    let parts: Vec<&str> = text.trim_end_matches('\n').split('\t').collect();
    if parts.len() != 8 || parts[0] != "segment_compact1" {
        return Err(GraphError::CorruptValue {
            key: key.to_string(),
            reason: "expected segment_compact1 record with 8 fields".to_string(),
        });
    }
    if parts[7] != idempotency_key {
        return Err(GraphError::IdempotencyConflict {
            operation: "segment-compact",
            idempotency_key: idempotency_key.to_string(),
            reason: "the stored record names a different key",
        });
    }
    let recorded_epoch = parse_u64(key, parts[1], "compacted_through_epoch")?;
    if recorded_epoch != compacted_through_epoch {
        return Err(GraphError::IdempotencyConflict {
            operation: "segment-compact",
            idempotency_key: idempotency_key.to_string(),
            reason: "a stored result for this key compacted through a different epoch",
        });
    }
    Ok(SegmentCompactionResult {
        compacted_through_epoch: recorded_epoch,
        source_segments: parse_u64(key, parts[2], "source_segments")?,
        deleted_segment_keys: parse_u64(key, parts[3], "deleted_segment_keys")?,
        deleted_tombstone_keys: parse_u64(key, parts[4], "deleted_tombstone_keys")?,
        input_edges: parse_u64(key, parts[5], "input_edges")?,
        output_edges: parse_u64(key, parts[6], "output_edges")?,
    })
}

pub(crate) fn decode_out_edge_segment(key: &str, value: &[u8]) -> Result<OutEdgeSegment> {
    let (cell_id, edge_type, src, storage_sequence) = parse_out_edge_segment_key(key)?;
    let Some(body) = value.strip_prefix(b"graph-out-segment\n") else {
        return Err(GraphError::CorruptValue {
            key: key.to_string(),
            reason: "expected graph-out-segment record".to_string(),
        });
    };
    if body.len() < 8 {
        return Err(GraphError::CorruptValue {
            key: key.to_string(),
            reason: format!("expected graph-out-segment count, got {} bytes", body.len()),
        });
    }
    let expected =
        u64::from_be_bytes(body[..8].try_into().map_err(|_| GraphError::CorruptValue {
            key: key.to_string(),
            reason: "invalid graph-out-segment count bytes".to_string(),
        })?);
    let expected_count = usize::try_from(expected).map_err(|_| GraphError::CorruptValue {
        key: key.to_string(),
        reason: format!("graph-out-segment count {expected} is too large"),
    })?;
    let expected_bytes = expected_count
        .checked_mul(8)
        .ok_or_else(|| GraphError::CorruptValue {
            key: key.to_string(),
            reason: format!("graph-out-segment count {expected} is too large"),
        })?;
    let edge_bytes = &body[8..];
    if edge_bytes.len() != expected_bytes {
        return Err(GraphError::CorruptValue {
            key: key.to_string(),
            reason: format!(
                "expected {expected_bytes} graph-out-segment destination bytes, got {}",
                edge_bytes.len()
            ),
        });
    }
    let mut destinations = Vec::with_capacity(expected_count);
    for chunk in edge_bytes.chunks_exact(8) {
        destinations.push(u64::from_be_bytes(chunk.try_into().map_err(|_| {
            GraphError::CorruptValue {
                key: key.to_string(),
                reason: "invalid graph-out-segment destination bytes".to_string(),
            }
        })?));
    }
    Ok(OutEdgeSegment {
        cell_id,
        edge_type,
        src,
        storage_sequence,
        destinations,
    })
}

pub(crate) fn encode_commit_idempotency(mutation: &EdgeMutation, result: &CommitResult) -> Vec<u8> {
    format!(
        "graph-commit-idempotency-v1\t{}\t{}\t{}\t{}\t{}\t{}\n",
        result.epoch,
        u8::from(result.already_existed),
        mutation.cell_id,
        mutation.edge_type,
        mutation.src,
        mutation.dst
    )
    .into_bytes()
}

pub(crate) fn encode_delete_idempotency(mutation: &EdgeMutation, result: &DeleteResult) -> Vec<u8> {
    format!(
        "graph-delete-idempotency-v1\t{}\t{}\t{}\t{}\t{}\t{}\n",
        result.epoch,
        u8::from(result.deleted),
        mutation.cell_id,
        mutation.edge_type,
        mutation.src,
        mutation.dst
    )
    .into_bytes()
}

pub(crate) fn encode_relationship_delete_idempotency(
    mutation: &EdgeMutation,
    relationship_id: RelationshipId,
    result: &DeleteResult,
) -> Vec<u8> {
    format!(
        "relationship_delete1\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n",
        result.epoch,
        u8::from(result.deleted),
        mutation.cell_id,
        mutation.edge_type,
        mutation.src,
        mutation.dst,
        relationship_id
    )
    .into_bytes()
}

pub(crate) fn encode_vertex_delete_idempotency(
    cell_id: &str,
    vertex_id: VertexId,
    result: &VertexDeleteResult,
) -> Vec<u8> {
    format!(
        "vertex_delete1\t{}\t{}\t{}\t{}\t{}\t{}\n",
        result.epoch,
        u8::from(result.vertex_deleted),
        result.incident_edges_deleted,
        result.relationships_deleted,
        cell_id,
        vertex_id
    )
    .into_bytes()
}

pub(crate) fn encode_cell_drop_idempotency(
    cell_id: &str,
    idempotency_key: &str,
    result: &GraphCellDropResult,
) -> Vec<u8> {
    format!(
        "cell_drop1\t{}\t{}\t{}\t{}\t{}\t{}\n",
        result.marker_epoch,
        result.deleted_keys,
        result.batches,
        u8::from(result.already_dropped),
        cell_id,
        idempotency_key
    )
    .into_bytes()
}

pub(crate) fn encode_bulk_import_idempotency(
    idempotency_key: &str,
    fingerprint: u64,
    result: &BulkImportResult,
) -> Vec<u8> {
    format!(
        "bulk_import1\t{}\t{}\t{}\t{}\t{}\t{}\n",
        result.start_epoch,
        result.end_epoch,
        result.inserted,
        result.already_existed,
        fingerprint,
        idempotency_key
    )
    .into_bytes()
}

pub(crate) fn encode_relationship_import_idempotency(
    idempotency_key: &str,
    fingerprint: u64,
    result: &RelationshipImportResult,
) -> Vec<u8> {
    format!(
        "relationship_import1\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n",
        result.start_epoch,
        result.end_epoch,
        result.relationships_inserted,
        result.relationships_already_existed,
        result.structural_edges_inserted,
        result.structural_edges_already_existed,
        fingerprint,
        idempotency_key
    )
    .into_bytes()
}

pub(crate) fn encode_relationship_create_idempotency(
    mutation: &EdgeMutation,
    fingerprint: u64,
    result: &RelationshipCreateResult,
) -> Vec<u8> {
    format!(
        "relationship_create1\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n",
        result.epoch,
        result.relationship_id,
        u8::from(result.structural_edge_inserted),
        u8::from(result.already_created),
        fingerprint,
        mutation.cell_id,
        mutation.edge_type,
        mutation.src,
        mutation.dst
    )
    .into_bytes()
}

pub(crate) fn decode_bulk_import_idempotency(
    key: &str,
    idempotency_key: &str,
    fingerprint: u64,
    value: &[u8],
) -> Result<BulkImportResult> {
    let text = std::str::from_utf8(value).map_err(|err| GraphError::CorruptValue {
        key: key.to_string(),
        reason: err.to_string(),
    })?;
    let parts: Vec<&str> = text.trim_end_matches('\n').split('\t').collect();
    if parts.len() != 7 || parts[0] != "bulk_import1" {
        return Err(GraphError::CorruptValue {
            key: key.to_string(),
            reason: "expected bulk_import1 record with 7 fields".to_string(),
        });
    }
    if parts[6] != idempotency_key {
        return Err(GraphError::IdempotencyConflict {
            operation: "bulk-import",
            idempotency_key: idempotency_key.to_string(),
            reason: "the stored record names a different key",
        });
    }
    if parse_u64(key, parts[5], "fingerprint")? != fingerprint {
        return Err(GraphError::IdempotencyConflict {
            operation: "bulk-import",
            idempotency_key: idempotency_key.to_string(),
            reason: "this key already stored a result for a different payload",
        });
    }
    Ok(BulkImportResult {
        start_epoch: parse_u64(key, parts[1], "start_epoch")?,
        end_epoch: parse_u64(key, parts[2], "end_epoch")?,
        inserted: parse_u64(key, parts[3], "inserted")?,
        already_existed: parse_u64(key, parts[4], "already_existed")?,
    })
}

pub(crate) fn decode_relationship_import_idempotency(
    key: &str,
    idempotency_key: &str,
    fingerprint: u64,
    value: &[u8],
) -> Result<RelationshipImportResult> {
    let text = std::str::from_utf8(value).map_err(|err| GraphError::CorruptValue {
        key: key.to_string(),
        reason: err.to_string(),
    })?;
    let parts: Vec<&str> = text.trim_end_matches('\n').split('\t').collect();
    if parts.len() != 9 || parts[0] != "relationship_import1" {
        return Err(GraphError::CorruptValue {
            key: key.to_string(),
            reason: "expected relationship_import1 record with 9 fields".to_string(),
        });
    }
    // Split from one `||` into two arms so the reason can name which half
    // failed. The second is the signature of a caller whose keys are not
    // derived from what it is writing: the key replayed, a stored result came
    // back, and it was produced by a different set of relationships. The first
    // cannot happen through `keys::idempotency` — that path *is* the key — so
    // it means the slot holds another request's record.
    if parts[8] != idempotency_key {
        return Err(GraphError::IdempotencyConflict {
            operation: "relationship-import",
            idempotency_key: idempotency_key.to_string(),
            reason: "the stored record names a different key",
        });
    }
    if parse_u64(key, parts[7], "fingerprint")? != fingerprint {
        return Err(GraphError::IdempotencyConflict {
            operation: "relationship-import",
            idempotency_key: idempotency_key.to_string(),
            reason: "this key already stored a result for a different payload",
        });
    }
    Ok(RelationshipImportResult {
        start_epoch: parse_u64(key, parts[1], "start_epoch")?,
        end_epoch: parse_u64(key, parts[2], "end_epoch")?,
        relationships_inserted: parse_u64(key, parts[3], "relationships_inserted")?,
        relationships_already_existed: parse_u64(key, parts[4], "relationships_already_existed")?,
        structural_edges_inserted: parse_u64(key, parts[5], "structural_edges_inserted")?,
        structural_edges_already_existed: parse_u64(
            key,
            parts[6],
            "structural_edges_already_existed",
        )?,
    })
}

pub(crate) fn decode_relationship_create_idempotency(
    key: &str,
    mutation: &EdgeMutation,
    fingerprint: u64,
    value: &[u8],
) -> Result<RelationshipCreateResult> {
    let text = std::str::from_utf8(value).map_err(|err| GraphError::CorruptValue {
        key: key.to_string(),
        reason: err.to_string(),
    })?;
    let parts: Vec<&str> = text.trim_end_matches('\n').split('\t').collect();
    if parts.len() != 10 || parts[0] != "relationship_create1" {
        return Err(GraphError::CorruptValue {
            key: key.to_string(),
            reason: "expected relationship_create1 record with 10 fields".to_string(),
        });
    }
    if parse_u64(key, parts[5], "fingerprint")? != fingerprint
        || parts[6] != mutation.cell_id
        || parts[7] != mutation.edge_type
        || parse_u64(key, parts[8], "src")? != mutation.src
        || parse_u64(key, parts[9], "dst")? != mutation.dst
    {
        return Err(GraphError::IdempotencyConflict {
            operation: "relationship-create",
            idempotency_key: mutation.idempotency_key.clone(),
            reason: "this key already stored a result for a different edge or payload",
        });
    }
    Ok(RelationshipCreateResult {
        epoch: parse_u64(key, parts[1], "epoch")?,
        relationship_id: parse_u64(key, parts[2], "relationship_id")?,
        structural_edge_inserted: parse_bool_u8(key, parts[3], "structural_edge_inserted")?,
        already_created: parse_bool_u8(key, parts[4], "already_created")?,
    })
}

pub(crate) fn decode_delete_idempotency(
    key: &str,
    mutation: &EdgeMutation,
    value: &[u8],
) -> Result<DeleteResult> {
    let text = std::str::from_utf8(value).map_err(|err| GraphError::CorruptValue {
        key: key.to_string(),
        reason: err.to_string(),
    })?;
    let parts: Vec<&str> = text.trim_end_matches('\n').split('\t').collect();
    if parts.len() != 7 || parts[0] != "graph-delete-idempotency-v1" {
        return Err(GraphError::CorruptValue {
            key: key.to_string(),
            reason: "expected graph-delete-idempotency-v1 record with 7 fields".to_string(),
        });
    }
    ensure_idempotent_edge(key, "delete", mutation, &parts[3..7])?;
    let deleted = decode_bool_flag(key, parts[2], "deleted")?;
    Ok(DeleteResult {
        epoch: parse_u64(key, parts[1], "epoch")?,
        deleted,
    })
}

pub(crate) fn decode_relationship_delete_idempotency(
    key: &str,
    mutation: &EdgeMutation,
    relationship_id: RelationshipId,
    value: &[u8],
) -> Result<DeleteResult> {
    let text = std::str::from_utf8(value).map_err(|err| GraphError::CorruptValue {
        key: key.to_string(),
        reason: err.to_string(),
    })?;
    let parts: Vec<&str> = text.trim_end_matches('\n').split('\t').collect();
    if parts.len() != 8 || parts[0] != "relationship_delete1" {
        return Err(GraphError::CorruptValue {
            key: key.to_string(),
            reason: "expected relationship_delete1 record with 8 fields".to_string(),
        });
    }
    if parts[3] != mutation.cell_id
        || parts[4] != mutation.edge_type
        || parse_u64(key, parts[5], "src")? != mutation.src
        || parse_u64(key, parts[6], "dst")? != mutation.dst
        || parse_u64(key, parts[7], "relationship_id")? != relationship_id
    {
        return Err(GraphError::IdempotencyConflict {
            operation: "relationship-delete",
            idempotency_key: mutation.idempotency_key.clone(),
            reason: "this key already stored a result for a different relationship",
        });
    }
    Ok(DeleteResult {
        epoch: parse_u64(key, parts[1], "epoch")?,
        deleted: parse_bool_u8(key, parts[2], "deleted")?,
    })
}

pub(crate) fn decode_vertex_delete_idempotency(
    key: &str,
    cell_id: &str,
    vertex_id: VertexId,
    idempotency_key: &str,
    value: &[u8],
) -> Result<VertexDeleteResult> {
    let text = std::str::from_utf8(value).map_err(|err| GraphError::CorruptValue {
        key: key.to_string(),
        reason: err.to_string(),
    })?;
    let parts: Vec<&str> = text.trim_end_matches('\n').split('\t').collect();
    if parts.len() != 7 || parts[0] != "vertex_delete1" {
        return Err(GraphError::CorruptValue {
            key: key.to_string(),
            reason: "expected vertex_delete1 record with 7 fields".to_string(),
        });
    }
    if parts[5] != cell_id || parse_u64(key, parts[6], "vertex_id")? != vertex_id {
        return Err(GraphError::IdempotencyConflict {
            operation: "vertex-delete",
            idempotency_key: idempotency_key.to_string(),
            reason: "this key already stored a result for a different vertex",
        });
    }
    Ok(VertexDeleteResult {
        epoch: parse_u64(key, parts[1], "epoch")?,
        vertex_deleted: parse_bool_u8(key, parts[2], "vertex_deleted")?,
        incident_edges_deleted: parse_u64(key, parts[3], "incident_edges_deleted")?,
        relationships_deleted: parse_u64(key, parts[4], "relationships_deleted")?,
    })
}

pub(crate) fn decode_cell_drop_idempotency(
    key: &str,
    cell_id: &str,
    idempotency_key: &str,
    value: &[u8],
) -> Result<GraphCellDropResult> {
    let text = std::str::from_utf8(value).map_err(|err| GraphError::CorruptValue {
        key: key.to_string(),
        reason: err.to_string(),
    })?;
    let parts: Vec<&str> = text.trim_end_matches('\n').split('\t').collect();
    if parts.len() != 7 || parts[0] != "cell_drop1" {
        return Err(GraphError::CorruptValue {
            key: key.to_string(),
            reason: "expected cell_drop1 record with 7 fields".to_string(),
        });
    }
    if parts[5] != cell_id || parts[6] != idempotency_key {
        return Err(GraphError::IdempotencyConflict {
            operation: "cell-drop",
            idempotency_key: idempotency_key.to_string(),
            reason: "this key already stored a result for a different cell",
        });
    }
    Ok(GraphCellDropResult {
        marker_epoch: parse_u64(key, parts[1], "marker_epoch")?,
        deleted_keys: parse_u64(key, parts[2], "deleted_keys")?,
        batches: parse_u64(key, parts[3], "batches")?,
        already_dropped: parse_bool_u8(key, parts[4], "already_dropped")?,
    })
}

pub(crate) fn decode_commit_idempotency(
    key: &str,
    mutation: &EdgeMutation,
    value: &[u8],
) -> Result<CommitResult> {
    let text = std::str::from_utf8(value).map_err(|err| GraphError::CorruptValue {
        key: key.to_string(),
        reason: err.to_string(),
    })?;
    let parts: Vec<&str> = text.trim_end_matches('\n').split('\t').collect();
    if parts.len() != 7 || parts[0] != "graph-commit-idempotency-v1" {
        return Err(GraphError::CorruptValue {
            key: key.to_string(),
            reason: "expected graph-commit-idempotency-v1 record with 7 fields".to_string(),
        });
    }
    ensure_idempotent_edge(key, "create", mutation, &parts[3..7])?;
    let already_existed = decode_bool_flag(key, parts[2], "already_existed")?;
    Ok(CommitResult {
        epoch: parse_u64(key, parts[1], "epoch")?,
        already_existed,
    })
}

pub(crate) fn ensure_idempotent_edge(
    key: &str,
    operation: &'static str,
    mutation: &EdgeMutation,
    fields: &[&str],
) -> Result<()> {
    let [cell_id, edge_type, src, dst] = fields else {
        return Err(GraphError::CorruptValue {
            key: key.to_string(),
            reason: "expected idempotency edge identity".to_string(),
        });
    };
    if *cell_id != mutation.cell_id
        || *edge_type != mutation.edge_type
        || parse_u64(key, src, "src")? != mutation.src
        || parse_u64(key, dst, "dst")? != mutation.dst
    {
        return Err(GraphError::IdempotencyConflict {
            operation,
            idempotency_key: mutation.idempotency_key.clone(),
            reason: "this key already stored a result for a different edge",
        });
    }
    Ok(())
}

pub(crate) fn decode_bool_flag(key: &str, value: &str, field: &str) -> Result<bool> {
    match value {
        "0" => Ok(false),
        "1" => Ok(true),
        other => Err(GraphError::CorruptValue {
            key: key.to_string(),
            reason: format!("invalid {field} flag {other}"),
        }),
    }
}

pub(crate) fn bulk_import_fingerprint(
    cell_id: &str,
    edge_type: &str,
    edges: &[(VertexId, VertexId)],
) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    fn update(hash: &mut u64, bytes: &[u8]) {
        for byte in bytes {
            *hash ^= u64::from(*byte);
            *hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
        }
    }
    update(&mut hash, cell_id.as_bytes());
    update(&mut hash, b"\0");
    update(&mut hash, edge_type.as_bytes());
    update(&mut hash, b"\0");
    for (src, dst) in edges {
        update(&mut hash, &src.to_be_bytes());
        update(&mut hash, &dst.to_be_bytes());
    }
    hash
}

pub(crate) fn relationship_import_fingerprint(
    cell_id: &str,
    edge_type: &str,
    relationships: &[RelationshipMutation],
) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    fn update(hash: &mut u64, bytes: &[u8]) {
        for byte in bytes {
            *hash ^= u64::from(*byte);
            *hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
        }
    }
    update(&mut hash, cell_id.as_bytes());
    update(&mut hash, b"\0");
    update(&mut hash, edge_type.as_bytes());
    update(&mut hash, b"\0");
    for relationship in relationships {
        update(&mut hash, &relationship.src.to_be_bytes());
        update(&mut hash, &relationship.dst.to_be_bytes());
        update(&mut hash, &relationship.relationship_id.to_be_bytes());
        for (property, value) in &relationship.metadata.properties {
            update(&mut hash, property.as_bytes());
            update(&mut hash, b"=");
            update(
                &mut hash,
                encode_vertex_property_value_record(value).as_bytes(),
            );
            update(&mut hash, b"\0");
        }
    }
    hash
}

pub(crate) fn relationship_create_fingerprint(
    mutation: &EdgeMutation,
    metadata_updates: &[(VertexId, VertexMetadata)],
    edge_metadata: &EdgeMetadata,
) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    fn update(hash: &mut u64, bytes: &[u8]) {
        for byte in bytes {
            *hash ^= u64::from(*byte);
            *hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
        }
    }
    update(&mut hash, mutation.cell_id.as_bytes());
    update(&mut hash, b"\0");
    update(&mut hash, mutation.edge_type.as_bytes());
    update(&mut hash, b"\0");
    update(&mut hash, &mutation.src.to_be_bytes());
    update(&mut hash, &mutation.dst.to_be_bytes());
    update(&mut hash, b"\0");
    for (vertex_id, metadata) in metadata_updates {
        update(&mut hash, &vertex_id.to_be_bytes());
        for label in &metadata.labels {
            update(&mut hash, b"label:");
            update(&mut hash, label.as_bytes());
            update(&mut hash, b"\0");
        }
        for (property, value) in &metadata.properties {
            update(&mut hash, b"vprop:");
            update(&mut hash, property.as_bytes());
            update(&mut hash, b"=");
            update(
                &mut hash,
                encode_vertex_property_value_record(value).as_bytes(),
            );
            update(&mut hash, b"\0");
        }
    }
    for (property, value) in &edge_metadata.properties {
        update(&mut hash, b"rprop:");
        update(&mut hash, property.as_bytes());
        update(&mut hash, b"=");
        update(
            &mut hash,
            encode_vertex_property_value_record(value).as_bytes(),
        );
        update(&mut hash, b"\0");
    }
    hash
}

pub(crate) fn bulk_import_chunk_order(src: VertexId, dst: VertexId) -> u64 {
    let mut value = src ^ dst.rotate_left(32) ^ 0x9e37_79b9_7f4a_7c15;
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

pub(crate) fn segment_compaction_idempotency_operation(edge_type: &str, src: VertexId) -> String {
    format!("segment-compact-{edge_type}-{src:020}")
}

pub(crate) fn writer_lane_index(cell_id: &str) -> usize {
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    for byte in cell_id.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    (hash as usize) % GRAPH_WRITE_LANES
}

pub(crate) fn graph_now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis().try_into().unwrap_or(u64::MAX))
        .unwrap_or(0)
}

pub(crate) fn duration_micros_u64(duration: std::time::Duration) -> u64 {
    duration.as_micros().try_into().unwrap_or(u64::MAX)
}

pub(crate) fn merge_ingest_batch(
    batch: &EdgeMutationBatchResult,
    start_epoch: &mut Option<StorageSequence>,
    end_epoch: &mut StorageSequence,
    inserted: &mut u64,
    already_existed: &mut u64,
    batches: &mut u64,
) {
    if batch.inserted > 0 {
        *start_epoch =
            Some(start_epoch.map_or(batch.start_epoch, |epoch| epoch.min(batch.start_epoch)));
    }
    *end_epoch = (*end_epoch).max(batch.end_epoch);
    *inserted = inserted.saturating_add(batch.inserted);
    *already_existed = already_existed.saturating_add(batch.already_existed);
    *batches = batches.saturating_add(1);
}

pub(crate) fn parse_edge_record_key(key: &str) -> Result<EdgeRecord> {
    let parts: Vec<&str> = key.split('/').collect();
    match parts.as_slice() {
        ["cell", cell_id, "edge", edge_type, src, dst]
        | ["cell", cell_id, "e", "out", edge_type, src, dst] => {
            validate_component("cell_id", cell_id)?;
            validate_component("edge_type", edge_type)?;
            Ok(EdgeRecord {
                cell_id: (*cell_id).to_string(),
                edge_type: (*edge_type).to_string(),
                src: parse_u64(key, src, "src")?,
                dst: parse_u64(key, dst, "dst")?,
            })
        }
        ["cell", cell_id, "e", "in", edge_type, dst, src] => {
            validate_component("cell_id", cell_id)?;
            validate_component("edge_type", edge_type)?;
            Ok(EdgeRecord {
                cell_id: (*cell_id).to_string(),
                edge_type: (*edge_type).to_string(),
                src: parse_u64(key, src, "src")?,
                dst: parse_u64(key, dst, "dst")?,
            })
        }
        _ => Err(GraphError::CorruptValue {
            key: key.to_string(),
            reason: "cannot infer edge record identity from key".to_string(),
        }),
    }
}

pub(crate) fn parse_relationship_record_key(key: &str) -> Result<RelationshipRecord> {
    let parts: Vec<&str> = key.split('/').collect();
    match parts.as_slice() {
        ["cell", cell_id, "rel", edge_type, src, dst, relationship_id] => {
            validate_component("cell_id", cell_id)?;
            validate_component("edge_type", edge_type)?;
            Ok(RelationshipRecord {
                cell_id: (*cell_id).to_string(),
                edge_type: (*edge_type).to_string(),
                src: parse_u64(key, src, "src")?,
                dst: parse_u64(key, dst, "dst")?,
                relationship_id: parse_u64(key, relationship_id, "relationship_id")?,
                metadata: EdgeMetadata::default(),
            })
        }
        _ => Err(GraphError::CorruptValue {
            key: key.to_string(),
            reason: "cannot infer relationship record identity from key".to_string(),
        }),
    }
}

#[cfg(feature = "opencypher")]
pub(crate) fn parse_relationship_property_index_key(
    key: &str,
) -> Result<(
    String,
    String,
    String,
    String,
    VertexId,
    VertexId,
    RelationshipId,
)> {
    let parts: Vec<_> = key.split('/').collect();
    match parts.as_slice() {
        ["cell", cell_id, "rprop_idx", edge_type, property, encoded, src, dst, relationship_id] => {
            Ok((
                (*cell_id).to_string(),
                (*edge_type).to_string(),
                (*property).to_string(),
                (*encoded).to_string(),
                parse_u64(key, src, "src")?,
                parse_u64(key, dst, "dst")?,
                parse_u64(key, relationship_id, "relationship_id")?,
            ))
        }
        _ => Err(GraphError::CorruptValue {
            key: key.to_string(),
            reason: "expected relationship property index key".to_string(),
        }),
    }
}

pub(crate) fn parse_out_edge_segment_key(
    key: &str,
) -> Result<(String, String, VertexId, StorageSequence)> {
    let parts: Vec<&str> = key.split('/').collect();
    match parts.as_slice() {
        ["cell", cell_id, "seg", "out", edge_type, src, storage_sequence, segment_id] => {
            validate_component("cell_id", cell_id)?;
            validate_component("edge_type", edge_type)?;
            validate_component("segment_id", segment_id)?;
            Ok((
                (*cell_id).to_string(),
                (*edge_type).to_string(),
                parse_u64(key, src, "src")?,
                parse_u64(key, storage_sequence, "storage_sequence")?,
            ))
        }
        _ => Err(GraphError::CorruptValue {
            key: key.to_string(),
            reason: "expected canonical outbound segment key".to_string(),
        }),
    }
}

pub(crate) fn parse_out_edge_segment_tombstone_key(
    key: &str,
) -> Result<(String, String, VertexId, VertexId)> {
    let parts: Vec<&str> = key.split('/').collect();
    match parts.as_slice() {
        ["cell", cell_id, "seg", "tomb", "out", edge_type, src, dst] => {
            validate_component("cell_id", cell_id)?;
            validate_component("edge_type", edge_type)?;
            Ok((
                (*cell_id).to_string(),
                (*edge_type).to_string(),
                parse_u64(key, src, "src")?,
                parse_u64(key, dst, "dst")?,
            ))
        }
        _ => Err(GraphError::CorruptValue {
            key: key.to_string(),
            reason: "expected canonical outbound segment tombstone key".to_string(),
        }),
    }
}

pub(crate) fn segment_edge_visible(
    edge_epoch: StorageSequence,
    tombstone_epoch: Option<StorageSequence>,
) -> bool {
    tombstone_epoch.is_none_or(|epoch| edge_epoch > epoch)
}

pub(crate) fn parse_u64(key: &str, value: &str, field: &str) -> Result<u64> {
    value
        .parse::<u64>()
        .map_err(|err| GraphError::CorruptValue {
            key: key.to_string(),
            reason: format!("invalid {field}: {err}"),
        })
}

fn parse_bool_u8(key: &str, value: &str, field: &str) -> Result<bool> {
    match value {
        "0" => Ok(false),
        "1" => Ok(true),
        _ => Err(GraphError::CorruptValue {
            key: key.to_string(),
            reason: format!("invalid {field}: expected 0 or 1"),
        }),
    }
}

pub(crate) fn ensure_limit(operation: &'static str, actual: u64, limit: u64) -> Result<()> {
    if actual > limit {
        Err(GraphError::AdmissionRejected {
            operation,
            actual,
            limit,
        })
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod scan_option_tests {
    use super::*;

    #[test]
    fn remote_prefix_scans_do_not_pollute_the_block_cache() {
        let options = remote_scan_options();

        assert_eq!(options.durability_filter, DurabilityLevel::Remote);
        assert!(!options.cache_blocks);
    }

    #[test]
    fn bounded_remote_scans_admit_only_small_working_sets() {
        assert!(remote_scan_options_for_expected_items(1_024).cache_blocks);
        assert!(!remote_scan_options_for_expected_items(1_025).cache_blocks);
    }
}
