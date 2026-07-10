use super::*;

pub(crate) async fn ensure_store_format(
    db: &Db,
    write_authority: &GraphWriteAuthority,
) -> Result<()> {
    let current = db
        .get_with_options(GRAPH_STORE_FORMAT_KEY.as_bytes(), &remote_read_options())
        .await?;
    let Some(value) = current else {
        if !matches!(write_authority, GraphWriteAuthority::ReadOnly) {
            tracing::info!(
                target: "slatedb_graph_kernel",
                version = GRAPH_STORE_FORMAT_VERSION,
                "initializing graph store format version"
            );
            let mut batch = WriteBatch::new();
            batch.put(
                GRAPH_STORE_FORMAT_KEY.as_bytes(),
                encode_u64(GRAPH_STORE_FORMAT_VERSION),
            );
            let options = WriteOptions {
                await_durable: true,
                ..Default::default()
            };
            db.write_with_options(batch, &options).await?;
        }
        return Ok(());
    };

    let version = decode_u64(GRAPH_STORE_FORMAT_KEY, &value)?;
    if version != GRAPH_STORE_FORMAT_VERSION {
        tracing::error!(
            target: "slatedb_graph_kernel",
            version,
            expected = GRAPH_STORE_FORMAT_VERSION,
            "unsupported graph store format version"
        );
        return Err(GraphError::CorruptValue {
            key: GRAPH_STORE_FORMAT_KEY.to_string(),
            reason: format!(
                "unsupported graph store format {version}; expected {GRAPH_STORE_FORMAT_VERSION}"
            ),
        });
    }
    Ok(())
}

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
    read_epoch: GraphEpoch,
) -> Result<BTreeSet<VertexId>> {
    let mut neighbors = BTreeSet::new();

    {
        let prefix = keys::out_prefix(cell_id, edge_type, src);
        let mut iter = txn.scan_prefix(prefix.as_bytes(), ..).await?;
        while let Some(kv) = iter.next().await? {
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            let record = decode_edge_record(&key, &kv.value)?;
            if record.epoch <= read_epoch {
                neighbors.insert(record.dst);
            }
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
    read_epoch: GraphEpoch,
) -> Result<Option<GraphEpoch>> {
    let edge_key = keys::out_edge(cell_id, edge_type, src, dst);
    if let Some(value) = read_txn_remote(txn, &edge_key).await? {
        let record = decode_edge_record(&edge_key, &value)?;
        if record.epoch <= read_epoch {
            return Ok(Some(record.epoch));
        }
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
    read_epoch: GraphEpoch,
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
    read_epoch: GraphEpoch,
) -> Result<BTreeMap<VertexId, GraphEpoch>> {
    let mut edges = BTreeMap::<VertexId, GraphEpoch>::new();
    let mut tombstones = BTreeMap::<VertexId, GraphEpoch>::new();
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
            if segment.start_epoch > read_epoch {
                break;
            }
            for (epoch, dst) in segment.edges.iter().copied() {
                if epoch > read_epoch {
                    break;
                }
                if segment_edge_visible(epoch, tombstones.get(&dst).copied()) {
                    edges
                        .entry(dst)
                        .and_modify(|current| *current = (*current).max(epoch))
                        .or_insert(epoch);
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

pub(crate) async fn next_epoch_txn(txn: &DbTransaction, cell_id: &str) -> Result<GraphEpoch> {
    let current = read_counter_txn(txn, &keys::last_epoch(cell_id)).await?;
    current
        .checked_add(1)
        .ok_or_else(|| GraphError::CorruptValue {
            key: keys::last_epoch(cell_id),
            reason: "epoch overflow".to_string(),
        })
}

pub(crate) async fn commit_txn_strict(txn: DbTransaction, await_durable: bool) -> Result<()> {
    let options = WriteOptions {
        await_durable,
        ..Default::default()
    };
    txn.commit_with_options(&options).await?;
    Ok(())
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
    let mut value = format!("relationship1\t{}\n", record.epoch);
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
    let parts: Vec<_> = header.split('\t').collect();
    if parts.len() != 2 || parts[0] != "relationship1" {
        return Err(GraphError::CorruptValue {
            key: key.to_string(),
            reason: "expected relationship1 header".to_string(),
        });
    }
    let mut record = parse_relationship_record_key(key)?;
    record.epoch = parse_u64(key, parts[1], "relationship_epoch")?;
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

pub(crate) fn encode_vertex_index_delta(present: bool) -> Vec<u8> {
    vec![u8::from(present)]
}

#[cfg(feature = "opencypher")]
pub(crate) fn decode_vertex_index_delta(key: &str, value: &[u8]) -> Result<bool> {
    match value {
        [0] => Ok(false),
        [1] => Ok(true),
        _ => Err(GraphError::CorruptValue {
            key: key.to_string(),
            reason: "expected one-byte vertex index delta".to_string(),
        }),
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

pub(crate) fn encode_edge_record(record: &EdgeRecord) -> Vec<u8> {
    encode_edge_epoch(record.epoch)
}

pub(crate) fn encode_edge_epoch(epoch: GraphEpoch) -> Vec<u8> {
    let mut value = Vec::with_capacity(b"edge3".len() + 8);
    value.extend_from_slice(b"edge3");
    value.extend_from_slice(&epoch.to_be_bytes());
    value
}

pub(crate) fn decode_edge_record(key: &str, value: &[u8]) -> Result<EdgeRecord> {
    if let Some(epoch) = value.strip_prefix(b"edge3") {
        let mut record = parse_edge_record_key(key)?;
        record.epoch = decode_u64(key, epoch)?;
        return Ok(record);
    }
    let text = std::str::from_utf8(value).map_err(|err| GraphError::CorruptValue {
        key: key.to_string(),
        reason: err.to_string(),
    })?;
    let parts: Vec<&str> = text.trim_end_matches('\n').split('\t').collect();
    if parts.len() == 2 && parts[0] == "edge2" {
        let mut record = parse_edge_record_key(key)?;
        record.epoch = parse_u64(key, parts[1], "epoch")?;
        return Ok(record);
    }
    if parts.len() == 6 && parts[0] == "edge1" {
        return Ok(EdgeRecord {
            epoch: parse_u64(key, parts[1], "epoch")?,
            cell_id: parts[2].to_string(),
            edge_type: parts[3].to_string(),
            src: parse_u64(key, parts[4], "src")?,
            dst: parse_u64(key, parts[5], "dst")?,
        });
    }
    Err(GraphError::CorruptValue {
        key: key.to_string(),
        reason: "expected edge3, edge2, or edge1 record".to_string(),
    })
}

pub(crate) fn encode_out_edge_segment(dsts: &[VertexId]) -> Vec<u8> {
    let mut value = Vec::with_capacity(b"out_segment1\n".len() + 8 + dsts.len() * 8);
    value.extend_from_slice(b"out_segment1\n");
    value.extend_from_slice(&(dsts.len() as u64).to_be_bytes());
    for dst in dsts {
        value.extend_from_slice(&dst.to_be_bytes());
    }
    value
}

pub(crate) fn encode_out_edge_segment_records(edges: &[(GraphEpoch, VertexId)]) -> Vec<u8> {
    let mut value = Vec::with_capacity(b"out_segment2\n".len() + 8 + edges.len() * 16);
    value.extend_from_slice(b"out_segment2\n");
    value.extend_from_slice(&(edges.len() as u64).to_be_bytes());
    for (epoch, dst) in edges {
        value.extend_from_slice(&epoch.to_be_bytes());
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
    compacted_through_epoch: GraphEpoch,
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
        });
    }
    let recorded_epoch = parse_u64(key, parts[1], "compacted_through_epoch")?;
    if recorded_epoch != compacted_through_epoch {
        return Err(GraphError::IdempotencyConflict {
            operation: "segment-compact",
            idempotency_key: idempotency_key.to_string(),
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
    let (cell_id, edge_type, src, end_epoch, start_epoch) = parse_out_edge_segment_key(key)?;
    if start_epoch > end_epoch {
        return Err(GraphError::CorruptValue {
            key: key.to_string(),
            reason: "out edge segment start epoch is greater than end epoch".to_string(),
        });
    }
    if let Some(body) = value.strip_prefix(b"out_segment2\n") {
        return decode_out_edge_segment_v2(
            key,
            body,
            cell_id,
            edge_type,
            src,
            start_epoch,
            end_epoch,
        );
    }
    let Some(body) = value.strip_prefix(b"out_segment1\n") else {
        return Err(GraphError::CorruptValue {
            key: key.to_string(),
            reason: "expected out_segment2 or out_segment1 record".to_string(),
        });
    };
    if body.len() < 8 {
        return Err(GraphError::CorruptValue {
            key: key.to_string(),
            reason: format!("expected out segment count, got {} bytes", body.len()),
        });
    }
    let expected =
        u64::from_be_bytes(body[..8].try_into().map_err(|_| GraphError::CorruptValue {
            key: key.to_string(),
            reason: "invalid out segment count bytes".to_string(),
        })?);
    let expected_from_epoch = end_epoch.saturating_sub(start_epoch).saturating_add(1);
    if expected != expected_from_epoch {
        return Err(GraphError::CorruptValue {
            key: key.to_string(),
            reason: format!(
                "out segment epoch range implies {expected_from_epoch} edges, header says {expected}"
            ),
        });
    }
    let expected_count = usize::try_from(expected).map_err(|_| GraphError::CorruptValue {
        key: key.to_string(),
        reason: format!("out segment count {expected} is too large"),
    })?;
    let expected_bytes = expected_count
        .checked_mul(8)
        .ok_or_else(|| GraphError::CorruptValue {
            key: key.to_string(),
            reason: format!("out segment count {expected} is too large"),
        })?;
    let dst_bytes = &body[8..];
    if dst_bytes.len() != expected_bytes {
        return Err(GraphError::CorruptValue {
            key: key.to_string(),
            reason: format!(
                "expected {expected_bytes} out segment dst bytes, got {}",
                dst_bytes.len()
            ),
        });
    }
    let mut edges = Vec::with_capacity(expected_count);
    for (offset, chunk) in dst_bytes.chunks_exact(8).enumerate() {
        let dst = u64::from_be_bytes(chunk.try_into().map_err(|_| GraphError::CorruptValue {
            key: key.to_string(),
            reason: "invalid out segment dst bytes".to_string(),
        })?);
        edges.push((start_epoch + offset as u64, dst));
    }
    Ok(OutEdgeSegment {
        cell_id,
        edge_type,
        src,
        start_epoch,
        end_epoch,
        edges,
    })
}

pub(crate) fn decode_out_edge_segment_v2(
    key: &str,
    body: &[u8],
    cell_id: String,
    edge_type: String,
    src: VertexId,
    start_epoch: GraphEpoch,
    end_epoch: GraphEpoch,
) -> Result<OutEdgeSegment> {
    if body.len() < 8 {
        return Err(GraphError::CorruptValue {
            key: key.to_string(),
            reason: format!("expected out_segment2 count, got {} bytes", body.len()),
        });
    }
    let expected =
        u64::from_be_bytes(body[..8].try_into().map_err(|_| GraphError::CorruptValue {
            key: key.to_string(),
            reason: "invalid out_segment2 count bytes".to_string(),
        })?);
    let expected_count = usize::try_from(expected).map_err(|_| GraphError::CorruptValue {
        key: key.to_string(),
        reason: format!("out_segment2 count {expected} is too large"),
    })?;
    let expected_bytes =
        expected_count
            .checked_mul(16)
            .ok_or_else(|| GraphError::CorruptValue {
                key: key.to_string(),
                reason: format!("out_segment2 count {expected} is too large"),
            })?;
    let edge_bytes = &body[8..];
    if edge_bytes.len() != expected_bytes {
        return Err(GraphError::CorruptValue {
            key: key.to_string(),
            reason: format!(
                "expected {expected_bytes} out_segment2 edge bytes, got {}",
                edge_bytes.len()
            ),
        });
    }
    let mut edges = Vec::with_capacity(expected_count);
    let mut previous_epoch = None;
    for chunk in edge_bytes.chunks_exact(16) {
        let epoch =
            u64::from_be_bytes(
                chunk[..8]
                    .try_into()
                    .map_err(|_| GraphError::CorruptValue {
                        key: key.to_string(),
                        reason: "invalid out_segment2 epoch bytes".to_string(),
                    })?,
            );
        let dst =
            u64::from_be_bytes(
                chunk[8..16]
                    .try_into()
                    .map_err(|_| GraphError::CorruptValue {
                        key: key.to_string(),
                        reason: "invalid out_segment2 dst bytes".to_string(),
                    })?,
            );
        if epoch < start_epoch || epoch > end_epoch {
            return Err(GraphError::CorruptValue {
                key: key.to_string(),
                reason: format!(
                    "out_segment2 edge epoch {epoch} outside key range {start_epoch}..={end_epoch}"
                ),
            });
        }
        if previous_epoch.is_some_and(|previous| epoch <= previous) {
            return Err(GraphError::CorruptValue {
                key: key.to_string(),
                reason: "out_segment2 epochs must be strictly increasing".to_string(),
            });
        }
        previous_epoch = Some(epoch);
        edges.push((epoch, dst));
    }
    Ok(OutEdgeSegment {
        cell_id,
        edge_type,
        src,
        start_epoch,
        end_epoch,
        edges,
    })
}

pub(crate) fn encode_commit_idempotency(mutation: &EdgeMutation, result: &CommitResult) -> Vec<u8> {
    format!(
        "commit2\t{}\t{}\t{}\t{}\t{}\t{}\n",
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
        "delete2\t{}\t{}\t{}\t{}\t{}\t{}\n",
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

pub(crate) fn encode_mutation_batch_log(
    edge_type: &str,
    idempotency_key: &str,
    fingerprint: u64,
    result: &BulkImportResult,
) -> Vec<u8> {
    format!(
        "mutation_batch1\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n",
        edge_type,
        result.start_epoch,
        result.end_epoch,
        result.inserted,
        result.already_existed,
        fingerprint,
        idempotency_key
    )
    .into_bytes()
}

pub(crate) fn encode_edge_mutation_log_batch(batch: &EdgeMutationLogBatch) -> Vec<u8> {
    let mut value = format!(
        "edge_mutation_log1\t{}\t{}\t{}\t{}\n",
        batch.cell_id,
        batch.batch_id,
        batch.fingerprint,
        batch.mutations.len()
    );
    for mutation in &batch.mutations {
        value.push_str(&format!(
            "{}\t{}\t{}\t{}\n",
            mutation.edge_type, mutation.src, mutation.dst, mutation.idempotency_key
        ));
    }
    value.into_bytes()
}

pub(crate) fn decode_edge_mutation_log_batch(
    key: &str,
    value: &[u8],
) -> Result<EdgeMutationLogBatch> {
    let text = std::str::from_utf8(value).map_err(|err| GraphError::CorruptValue {
        key: key.to_string(),
        reason: err.to_string(),
    })?;
    let mut lines = text.trim_end_matches('\n').lines();
    let Some(header) = lines.next() else {
        return Err(GraphError::CorruptValue {
            key: key.to_string(),
            reason: "empty edge mutation log batch".to_string(),
        });
    };
    let parts: Vec<&str> = header.split('\t').collect();
    if parts.len() != 5 || parts[0] != "edge_mutation_log1" {
        return Err(GraphError::CorruptValue {
            key: key.to_string(),
            reason: "expected edge_mutation_log1 header with 5 fields".to_string(),
        });
    }
    let cell_id = parts[1].to_string();
    let batch_id = parts[2].to_string();
    let fingerprint = parse_u64(key, parts[3], "fingerprint")?;
    let expected = parse_u64(key, parts[4], "mutation_count")?;
    validate_component("cell_id", &cell_id)?;
    validate_component("batch_id", &batch_id)?;

    let mut mutations = Vec::new();
    for line in lines {
        let parts: Vec<&str> = line.split('\t').collect();
        if parts.len() != 4 {
            return Err(GraphError::CorruptValue {
                key: key.to_string(),
                reason: "expected mutation row with 4 fields".to_string(),
            });
        }
        let edge_type = parts[0].to_string();
        let idempotency_key = parts[3].to_string();
        validate_component("edge_type", &edge_type)?;
        validate_component("idempotency_key", &idempotency_key)?;
        mutations.push(EdgeMutation {
            cell_id: cell_id.clone(),
            edge_type,
            src: parse_u64(key, parts[1], "src")?,
            dst: parse_u64(key, parts[2], "dst")?,
            idempotency_key,
        });
    }
    if mutations.len() as u64 != expected {
        return Err(GraphError::CorruptValue {
            key: key.to_string(),
            reason: format!(
                "expected {expected} mutation rows, decoded {}",
                mutations.len()
            ),
        });
    }
    let batch = EdgeMutationLogBatch {
        cell_id,
        batch_id,
        fingerprint,
        mutations,
    };
    let actual = edge_mutation_log_fingerprint(&batch.cell_id, &batch.batch_id, &batch.mutations);
    if actual != fingerprint {
        return Err(GraphError::CorruptValue {
            key: key.to_string(),
            reason: format!(
                "mutation log fingerprint mismatch expected {fingerprint} got {actual}"
            ),
        });
    }
    Ok(batch)
}

pub(crate) fn encode_mutation_log_append_idempotency(
    batch_id: &str,
    fingerprint: u64,
    result: &EdgeMutationLogAppendResult,
) -> Vec<u8> {
    format!(
        "mutation_log_append1\t{}\t{}\t{}\t{}\n",
        result.log_epoch, result.mutations, fingerprint, batch_id
    )
    .into_bytes()
}

pub(crate) fn decode_mutation_log_append_idempotency(
    key: &str,
    batch_id: &str,
    fingerprint: u64,
    value: &[u8],
) -> Result<EdgeMutationLogAppendResult> {
    let text = std::str::from_utf8(value).map_err(|err| GraphError::CorruptValue {
        key: key.to_string(),
        reason: err.to_string(),
    })?;
    let parts: Vec<&str> = text.trim_end_matches('\n').split('\t').collect();
    if parts.len() != 5 || parts[0] != "mutation_log_append1" {
        return Err(GraphError::CorruptValue {
            key: key.to_string(),
            reason: "expected mutation_log_append1 record with 5 fields".to_string(),
        });
    }
    if parts[4] != batch_id || parse_u64(key, parts[3], "fingerprint")? != fingerprint {
        return Err(GraphError::IdempotencyConflict {
            operation: "mutation-log",
            idempotency_key: batch_id.to_string(),
        });
    }
    Ok(EdgeMutationLogAppendResult {
        log_epoch: parse_u64(key, parts[1], "log_epoch")?,
        mutations: parse_u64(key, parts[2], "mutations")?,
        already_appended: true,
    })
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
    if parts[6] != idempotency_key || parse_u64(key, parts[5], "fingerprint")? != fingerprint {
        return Err(GraphError::IdempotencyConflict {
            operation: "bulk-import",
            idempotency_key: idempotency_key.to_string(),
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
    if parts[8] != idempotency_key || parse_u64(key, parts[7], "fingerprint")? != fingerprint {
        return Err(GraphError::IdempotencyConflict {
            operation: "relationship-import",
            idempotency_key: idempotency_key.to_string(),
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
        });
    }
    Ok(RelationshipCreateResult {
        epoch: parse_u64(key, parts[1], "epoch")?,
        relationship_id: parse_u64(key, parts[2], "relationship_id")?,
        structural_edge_inserted: parse_bool_u8(key, parts[3], "structural_edge_inserted")?,
        already_created: parse_bool_u8(key, parts[4], "already_created")?,
    })
}

pub(crate) fn decode_bulk_import_fingerprint_idempotency(
    key: &str,
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
    if parse_u64(key, parts[5], "fingerprint")? != fingerprint {
        return Err(GraphError::IdempotencyConflict {
            operation: "bulk-import-fingerprint",
            idempotency_key: format!("{fingerprint:020}"),
        });
    }
    Ok(BulkImportResult {
        start_epoch: parse_u64(key, parts[1], "start_epoch")?,
        end_epoch: parse_u64(key, parts[2], "end_epoch")?,
        inserted: parse_u64(key, parts[3], "inserted")?,
        already_existed: parse_u64(key, parts[4], "already_existed")?,
    })
}

pub(crate) fn decode_delete_result(key: &str, value: &[u8]) -> Result<DeleteResult> {
    let text = std::str::from_utf8(value).map_err(|err| GraphError::CorruptValue {
        key: key.to_string(),
        reason: err.to_string(),
    })?;
    let parts: Vec<&str> = text.trim_end_matches('\n').split('\t').collect();
    if parts.len() != 3 || parts[0] != "delete1" {
        return Err(GraphError::CorruptValue {
            key: key.to_string(),
            reason: "expected delete1 record with 3 fields".to_string(),
        });
    }
    let deleted = match parts[2] {
        "0" => false,
        "1" => true,
        other => {
            return Err(GraphError::CorruptValue {
                key: key.to_string(),
                reason: format!("invalid deleted flag {other}"),
            });
        }
    };
    Ok(DeleteResult {
        epoch: parse_u64(key, parts[1], "epoch")?,
        deleted,
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
    if parts.first() == Some(&"delete1") {
        return decode_delete_result(key, value);
    }
    if parts.len() != 7 || parts[0] != "delete2" {
        return Err(GraphError::CorruptValue {
            key: key.to_string(),
            reason: "expected delete2 record with 7 fields".to_string(),
        });
    }
    ensure_idempotent_edge(key, "delete", mutation, &parts[3..7])?;
    let deleted = decode_bool_flag(key, parts[2], "deleted")?;
    Ok(DeleteResult {
        epoch: parse_u64(key, parts[1], "epoch")?,
        deleted,
    })
}

pub(crate) fn decode_commit_result(key: &str, value: &[u8]) -> Result<CommitResult> {
    let text = std::str::from_utf8(value).map_err(|err| GraphError::CorruptValue {
        key: key.to_string(),
        reason: err.to_string(),
    })?;
    let parts: Vec<&str> = text.trim_end_matches('\n').split('\t').collect();
    if parts.len() != 3 || parts[0] != "commit1" {
        return Err(GraphError::CorruptValue {
            key: key.to_string(),
            reason: "expected commit1 record with 3 fields".to_string(),
        });
    }
    let existed = match parts[2] {
        "0" => false,
        "1" => true,
        other => {
            return Err(GraphError::CorruptValue {
                key: key.to_string(),
                reason: format!("invalid already_existed flag {other}"),
            });
        }
    };
    Ok(CommitResult {
        epoch: parse_u64(key, parts[1], "epoch")?,
        already_existed: existed,
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
    if parts.first() == Some(&"commit1") {
        return decode_commit_result(key, value);
    }
    if parts.len() != 7 || parts[0] != "commit2" {
        return Err(GraphError::CorruptValue {
            key: key.to_string(),
            reason: "expected commit2 record with 7 fields".to_string(),
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

pub(crate) fn edge_mutation_log_fingerprint(
    cell_id: &str,
    batch_id: &str,
    mutations: &[EdgeMutation],
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
    update(&mut hash, batch_id.as_bytes());
    update(&mut hash, b"\0");
    for mutation in mutations {
        update(&mut hash, mutation.edge_type.as_bytes());
        update(&mut hash, b"\0");
        update(&mut hash, &mutation.src.to_be_bytes());
        update(&mut hash, &mutation.dst.to_be_bytes());
        update(&mut hash, b"\0");
        update(&mut hash, mutation.idempotency_key.as_bytes());
        update(&mut hash, b"\0");
    }
    hash
}

pub(crate) fn parse_mutation_log_epoch(key: &str) -> Result<GraphEpoch> {
    let parts: Vec<&str> = key.split('/').collect();
    if parts.len() < 5 || parts[0] != "cell" || parts[2] != "mutation_log" {
        return Err(GraphError::CorruptValue {
            key: key.to_string(),
            reason: "expected cell/{cell_id}/mutation_log/{epoch}/{batch_id}".to_string(),
        });
    }
    parse_u64(key, parts[3], "mutation_log_epoch")
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

pub(crate) fn segment_import_fingerprint_key(
    cell_id: &str,
    edge_type: &str,
    src: VertexId,
    fingerprint: u64,
) -> String {
    keys::idempotency(
        cell_id,
        &format!("segment-import-fp-{edge_type}-{src:020}"),
        &format!("{fingerprint:020}"),
    )
}

pub(crate) fn writer_lane_index(cell_id: &str) -> usize {
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    for byte in cell_id.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    (hash as usize) % GRAPH_WRITE_LANES
}

pub(crate) fn lock_error<T>(_: std::sync::PoisonError<T>) -> GraphError {
    GraphError::CorruptValue {
        key: "graph/write_authority_lock".to_string(),
        reason: "write authority lock poisoned".to_string(),
    }
}

pub(crate) fn graph_now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis().try_into().unwrap_or(u64::MAX))
        .unwrap_or(0)
}

pub(crate) fn new_cell_write_lock_owner_token() -> String {
    let counter = GRAPH_CELL_WRITE_LOCK_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{}-{}-{counter}", graph_now_millis(), std::process::id())
}

pub(crate) fn duration_micros_u64(duration: std::time::Duration) -> u64 {
    duration.as_micros().try_into().unwrap_or(u64::MAX)
}

pub(crate) fn merge_ingest_batch(
    batch: &EdgeMutationBatchResult,
    start_epoch: &mut Option<GraphEpoch>,
    end_epoch: &mut GraphEpoch,
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

pub(crate) fn encode_delta_record(record: &DeltaRecord) -> Vec<u8> {
    let _ = record;
    b"delta2\n".to_vec()
}

pub(crate) fn encode_outbox_delta_batch(
    cell_id: &str,
    edge_type: &str,
    kind: DeltaKind,
    start_epoch: GraphEpoch,
    end_epoch: GraphEpoch,
    edges: &[(VertexId, VertexId)],
) -> Vec<u8> {
    if let Some((src, _)) = edges.first() {
        if edges.iter().all(|(candidate, _)| candidate == src) {
            let dsts: Vec<_> = edges.iter().map(|(_, dst)| *dst).collect();
            return encode_outbox_delta_batch_same_src(
                cell_id,
                edge_type,
                kind,
                start_epoch,
                end_epoch,
                *src,
                &dsts,
            );
        }
    }
    let mut value = Vec::with_capacity(b"outbox_batch2\n".len() + 8 + edges.len() * 16);
    value.extend_from_slice(b"outbox_batch2\n");
    value.extend_from_slice(&(edges.len() as u64).to_be_bytes());
    for (src, dst) in edges {
        value.extend_from_slice(&src.to_be_bytes());
        value.extend_from_slice(&dst.to_be_bytes());
    }
    value
}

pub(crate) fn encode_outbox_delta_batch_same_src(
    cell_id: &str,
    edge_type: &str,
    kind: DeltaKind,
    start_epoch: GraphEpoch,
    end_epoch: GraphEpoch,
    src: VertexId,
    dsts: &[VertexId],
) -> Vec<u8> {
    let _ = (cell_id, edge_type, kind, start_epoch, end_epoch);
    let mut value = Vec::with_capacity(b"outbox_batch3\n".len() + 16 + dsts.len() * 8);
    value.extend_from_slice(b"outbox_batch3\n");
    value.extend_from_slice(&(dsts.len() as u64).to_be_bytes());
    value.extend_from_slice(&src.to_be_bytes());
    for dst in dsts {
        value.extend_from_slice(&dst.to_be_bytes());
    }
    value
}

pub(crate) fn decode_outbox_delta_batch(key: &str, value: &[u8]) -> Result<OutboxDeltaBatch> {
    let (key_cell_id, key_end_epoch, key_start_epoch, key_kind, key_edge_type) =
        parse_outbox_batch_key(key)?;
    if let Some(body) = value.strip_prefix(b"outbox_batch3\n") {
        return decode_outbox_delta_batch_v3(
            key,
            body,
            key_cell_id,
            key_edge_type,
            key_kind,
            key_start_epoch,
            key_end_epoch,
        );
    }
    if let Some(body) = value.strip_prefix(b"outbox_batch2\n") {
        return decode_outbox_delta_batch_v2(
            key,
            body,
            key_cell_id,
            key_edge_type,
            key_kind,
            key_start_epoch,
            key_end_epoch,
        );
    }
    let text = std::str::from_utf8(value).map_err(|err| GraphError::CorruptValue {
        key: key.to_string(),
        reason: err.to_string(),
    })?;
    let mut lines = text.trim_end_matches('\n').lines();
    let Some(header) = lines.next() else {
        return Err(GraphError::CorruptValue {
            key: key.to_string(),
            reason: "empty outbox delta batch".to_string(),
        });
    };
    let parts: Vec<&str> = header.split('\t').collect();
    if parts.len() != 7 || parts[0] != "outbox_batch1" {
        return Err(GraphError::CorruptValue {
            key: key.to_string(),
            reason: "expected outbox_batch1 header with 7 fields".to_string(),
        });
    }
    validate_component("cell_id", parts[1])?;
    validate_component("edge_type", parts[2])?;
    let cell_id = parts[1].to_string();
    let edge_type = parts[2].to_string();
    let start_epoch = parse_u64(key, parts[3], "start_epoch")?;
    let end_epoch = parse_u64(key, parts[4], "end_epoch")?;
    let kind = parse_delta_kind(key, parts[5])?;
    let expected = parse_u64(key, parts[6], "edge_count")?;
    if cell_id != key_cell_id
        || edge_type != key_edge_type
        || start_epoch != key_start_epoch
        || end_epoch != key_end_epoch
        || kind != key_kind
    {
        return Err(GraphError::CorruptValue {
            key: key.to_string(),
            reason: "outbox batch header does not match key identity".to_string(),
        });
    }
    if start_epoch > end_epoch {
        return Err(GraphError::CorruptValue {
            key: key.to_string(),
            reason: "outbox batch start epoch is greater than end epoch".to_string(),
        });
    }
    let expected_from_epoch = end_epoch.saturating_sub(start_epoch).saturating_add(1);
    if expected != expected_from_epoch {
        return Err(GraphError::CorruptValue {
            key: key.to_string(),
            reason: format!(
                "outbox batch epoch range implies {expected_from_epoch} edges, header says {expected}"
            ),
        });
    }
    let mut edges = Vec::new();
    for line in lines {
        let parts: Vec<&str> = line.split('\t').collect();
        if parts.len() != 2 {
            return Err(GraphError::CorruptValue {
                key: key.to_string(),
                reason: "expected outbox batch row with 2 fields".to_string(),
            });
        }
        edges.push((
            parse_u64(key, parts[0], "src")?,
            parse_u64(key, parts[1], "dst")?,
        ));
    }
    if edges.len() as u64 != expected {
        return Err(GraphError::CorruptValue {
            key: key.to_string(),
            reason: format!(
                "expected {expected} outbox batch rows, decoded {}",
                edges.len()
            ),
        });
    }
    Ok(OutboxDeltaBatch {
        cell_id,
        edge_type,
        kind,
        start_epoch,
        end_epoch,
        edges,
    })
}

pub(crate) fn decode_outbox_delta_batch_v2(
    key: &str,
    body: &[u8],
    cell_id: String,
    edge_type: String,
    kind: DeltaKind,
    start_epoch: GraphEpoch,
    end_epoch: GraphEpoch,
) -> Result<OutboxDeltaBatch> {
    if start_epoch > end_epoch {
        return Err(GraphError::CorruptValue {
            key: key.to_string(),
            reason: "outbox batch start epoch is greater than end epoch".to_string(),
        });
    }
    if body.len() < 8 {
        return Err(GraphError::CorruptValue {
            key: key.to_string(),
            reason: format!("expected outbox_batch2 count, got {} bytes", body.len()),
        });
    }
    let expected =
        u64::from_be_bytes(body[..8].try_into().map_err(|_| GraphError::CorruptValue {
            key: key.to_string(),
            reason: "invalid outbox_batch2 count bytes".to_string(),
        })?);
    let expected_from_epoch = end_epoch.saturating_sub(start_epoch).saturating_add(1);
    if expected != expected_from_epoch {
        return Err(GraphError::CorruptValue {
            key: key.to_string(),
            reason: format!(
                "outbox batch epoch range implies {expected_from_epoch} edges, header says {expected}"
            ),
        });
    }
    let edge_bytes = &body[8..];
    let expected_count = usize::try_from(expected).map_err(|_| GraphError::CorruptValue {
        key: key.to_string(),
        reason: format!("outbox_batch2 count {expected} is too large"),
    })?;
    let expected_bytes =
        expected_count
            .checked_mul(16)
            .ok_or_else(|| GraphError::CorruptValue {
                key: key.to_string(),
                reason: format!("outbox_batch2 count {expected} is too large"),
            })?;
    if edge_bytes.len() != expected_bytes {
        return Err(GraphError::CorruptValue {
            key: key.to_string(),
            reason: format!(
                "expected {expected_bytes} outbox_batch2 edge bytes, got {}",
                edge_bytes.len()
            ),
        });
    }
    let mut edges = Vec::with_capacity(expected_count);
    for chunk in edge_bytes.chunks_exact(16) {
        let src =
            u64::from_be_bytes(
                chunk[..8]
                    .try_into()
                    .map_err(|_| GraphError::CorruptValue {
                        key: key.to_string(),
                        reason: "invalid outbox_batch2 src bytes".to_string(),
                    })?,
            );
        let dst =
            u64::from_be_bytes(
                chunk[8..16]
                    .try_into()
                    .map_err(|_| GraphError::CorruptValue {
                        key: key.to_string(),
                        reason: "invalid outbox_batch2 dst bytes".to_string(),
                    })?,
            );
        edges.push((src, dst));
    }
    Ok(OutboxDeltaBatch {
        cell_id,
        edge_type,
        kind,
        start_epoch,
        end_epoch,
        edges,
    })
}

pub(crate) fn decode_outbox_delta_batch_v3(
    key: &str,
    body: &[u8],
    cell_id: String,
    edge_type: String,
    kind: DeltaKind,
    start_epoch: GraphEpoch,
    end_epoch: GraphEpoch,
) -> Result<OutboxDeltaBatch> {
    if start_epoch > end_epoch {
        return Err(GraphError::CorruptValue {
            key: key.to_string(),
            reason: "outbox batch start epoch is greater than end epoch".to_string(),
        });
    }
    if body.len() < 16 {
        return Err(GraphError::CorruptValue {
            key: key.to_string(),
            reason: format!(
                "expected outbox_batch3 count and src, got {} bytes",
                body.len()
            ),
        });
    }
    let expected =
        u64::from_be_bytes(body[..8].try_into().map_err(|_| GraphError::CorruptValue {
            key: key.to_string(),
            reason: "invalid outbox_batch3 count bytes".to_string(),
        })?);
    let src = u64::from_be_bytes(
        body[8..16]
            .try_into()
            .map_err(|_| GraphError::CorruptValue {
                key: key.to_string(),
                reason: "invalid outbox_batch3 src bytes".to_string(),
            })?,
    );
    let expected_from_epoch = end_epoch.saturating_sub(start_epoch).saturating_add(1);
    if expected != expected_from_epoch {
        return Err(GraphError::CorruptValue {
            key: key.to_string(),
            reason: format!(
                "outbox batch epoch range implies {expected_from_epoch} edges, header says {expected}"
            ),
        });
    }
    let expected_count = usize::try_from(expected).map_err(|_| GraphError::CorruptValue {
        key: key.to_string(),
        reason: format!("outbox_batch3 count {expected} is too large"),
    })?;
    let expected_dst_bytes =
        expected_count
            .checked_mul(8)
            .ok_or_else(|| GraphError::CorruptValue {
                key: key.to_string(),
                reason: format!("outbox_batch3 count {expected} is too large"),
            })?;
    let dst_bytes = &body[16..];
    if dst_bytes.len() != expected_dst_bytes {
        return Err(GraphError::CorruptValue {
            key: key.to_string(),
            reason: format!(
                "expected {expected_dst_bytes} outbox_batch3 dst bytes, got {}",
                dst_bytes.len()
            ),
        });
    }
    let mut edges = Vec::with_capacity(expected_count);
    for chunk in dst_bytes.chunks_exact(8) {
        let dst = u64::from_be_bytes(chunk.try_into().map_err(|_| GraphError::CorruptValue {
            key: key.to_string(),
            reason: "invalid outbox_batch3 dst bytes".to_string(),
        })?);
        edges.push((src, dst));
    }
    Ok(OutboxDeltaBatch {
        cell_id,
        edge_type,
        kind,
        start_epoch,
        end_epoch,
        edges,
    })
}

pub(crate) fn parse_outbox_batch_key(
    key: &str,
) -> Result<(String, GraphEpoch, GraphEpoch, DeltaKind, String)> {
    let parts: Vec<&str> = key.split('/').collect();
    match parts.as_slice() {
        ["cell", cell_id, "outbox_batch", end_epoch, start_epoch, kind, edge_type, batch_id] => {
            validate_component("cell_id", cell_id)?;
            validate_component("edge_type", edge_type)?;
            validate_component("batch_id", batch_id)?;
            Ok((
                (*cell_id).to_string(),
                parse_u64(key, end_epoch, "end_epoch")?,
                parse_u64(key, start_epoch, "start_epoch")?,
                parse_delta_kind(key, kind)?,
                (*edge_type).to_string(),
            ))
        }
        _ => Err(GraphError::CorruptValue {
            key: key.to_string(),
            reason:
                "expected cell/{cell_id}/outbox_batch/{end_epoch}/{start_epoch}/{kind}/{edge_type}/{batch_id}"
                    .to_string(),
        }),
    }
}

pub(crate) fn decode_delta_record(key: &str, value: &[u8]) -> Result<DeltaRecord> {
    let text = std::str::from_utf8(value).map_err(|err| GraphError::CorruptValue {
        key: key.to_string(),
        reason: err.to_string(),
    })?;
    let parts: Vec<&str> = text.trim_end_matches('\n').split('\t').collect();
    if parts.len() == 1 && parts[0] == "delta2" {
        return parse_delta_record_key(key);
    }
    if parts.len() == 7 && parts[0] == "delta1" {
        let kind = parse_delta_kind(key, parts[1])?;
        return Ok(DeltaRecord {
            kind,
            edge: EdgeRecord {
                epoch: parse_u64(key, parts[2], "epoch")?,
                cell_id: parts[3].to_string(),
                edge_type: parts[4].to_string(),
                src: parse_u64(key, parts[5], "src")?,
                dst: parse_u64(key, parts[6], "dst")?,
            },
        });
    }
    Err(GraphError::CorruptValue {
        key: key.to_string(),
        reason: "expected delta2 or delta1 record".to_string(),
    })
}

pub(crate) fn parse_edge_record_key(key: &str) -> Result<EdgeRecord> {
    let parts: Vec<&str> = key.split('/').collect();
    match parts.as_slice() {
        ["cell", cell_id, "edge", edge_type, src, dst] => {
            validate_component("cell_id", cell_id)?;
            validate_component("edge_type", edge_type)?;
            Ok(EdgeRecord {
                cell_id: (*cell_id).to_string(),
                edge_type: (*edge_type).to_string(),
                src: parse_u64(key, src, "src")?,
                dst: parse_u64(key, dst, "dst")?,
                epoch: 0,
            })
        }
        ["cell", cell_id, "e", "out", edge_type, src, dst] => {
            validate_component("cell_id", cell_id)?;
            validate_component("edge_type", edge_type)?;
            Ok(EdgeRecord {
                cell_id: (*cell_id).to_string(),
                edge_type: (*edge_type).to_string(),
                src: parse_u64(key, src, "src")?,
                dst: parse_u64(key, dst, "dst")?,
                epoch: 0,
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
                epoch: 0,
            })
        }
        ["cell", cell_id, "delta", kind, edge_type, epoch, src, dst]
            if matches!(*kind, "plus" | "minus") =>
        {
            validate_component("cell_id", cell_id)?;
            validate_component("edge_type", edge_type)?;
            Ok(EdgeRecord {
                cell_id: (*cell_id).to_string(),
                edge_type: (*edge_type).to_string(),
                src: parse_u64(key, src, "src")?,
                dst: parse_u64(key, dst, "dst")?,
                epoch: parse_u64(key, epoch, "epoch")?,
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
                epoch: 0,
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
) -> Result<(String, String, VertexId, GraphEpoch, GraphEpoch)> {
    let parts: Vec<&str> = key.split('/').collect();
    match parts.as_slice() {
        [
            "cell",
            cell_id,
            "seg",
            "out",
            edge_type,
            src,
            end_epoch,
            start_epoch,
            segment_id,
        ] => {
            validate_component("cell_id", cell_id)?;
            validate_component("edge_type", edge_type)?;
            validate_component("segment_id", segment_id)?;
            Ok((
                (*cell_id).to_string(),
                (*edge_type).to_string(),
                parse_u64(key, src, "src")?,
                parse_u64(key, end_epoch, "end_epoch")?,
                parse_u64(key, start_epoch, "start_epoch")?,
            ))
        }
        _ => Err(GraphError::CorruptValue {
            key: key.to_string(),
            reason:
                "expected cell/{cell_id}/seg/out/{edge_type}/{src}/{end_epoch}/{start_epoch}/{segment_id}"
                    .to_string(),
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
            reason: "expected cell/{cell_id}/seg/tomb/out/{edge_type}/{src}/{dst}".to_string(),
        }),
    }
}

pub(crate) fn segment_edge_visible(
    edge_epoch: GraphEpoch,
    tombstone_epoch: Option<GraphEpoch>,
) -> bool {
    match tombstone_epoch {
        Some(epoch) => edge_epoch > epoch,
        None => true,
    }
}

pub(crate) fn parse_delta_record_key(key: &str) -> Result<DeltaRecord> {
    let parts: Vec<&str> = key.split('/').collect();
    match parts.as_slice() {
        ["cell", cell_id, "outbox", epoch, kind, edge_type, src, dst] => {
            validate_component("cell_id", cell_id)?;
            validate_component("edge_type", edge_type)?;
            Ok(DeltaRecord {
                kind: parse_delta_kind(key, kind)?,
                edge: EdgeRecord {
                    cell_id: (*cell_id).to_string(),
                    edge_type: (*edge_type).to_string(),
                    src: parse_u64(key, src, "src")?,
                    dst: parse_u64(key, dst, "dst")?,
                    epoch: parse_u64(key, epoch, "epoch")?,
                },
            })
        }
        ["cell", cell_id, "delta_owner", kind, edge_type, direction, owner, epoch, neighbor] => {
            validate_component("cell_id", cell_id)?;
            validate_component("edge_type", edge_type)?;
            let owner = parse_u64(key, owner, "owner")?;
            let neighbor = parse_u64(key, neighbor, "neighbor")?;
            let (src, dst) = match *direction {
                "out" => (owner, neighbor),
                "in" => (neighbor, owner),
                other => {
                    return Err(GraphError::CorruptValue {
                        key: key.to_string(),
                        reason: format!("invalid delta owner direction {other}"),
                    });
                }
            };
            Ok(DeltaRecord {
                kind: parse_delta_kind(key, kind)?,
                edge: EdgeRecord {
                    cell_id: (*cell_id).to_string(),
                    edge_type: (*edge_type).to_string(),
                    src,
                    dst,
                    epoch: parse_u64(key, epoch, "epoch")?,
                },
            })
        }
        _ => Err(GraphError::CorruptValue {
            key: key.to_string(),
            reason: "cannot infer delta record identity from key".to_string(),
        }),
    }
}

pub(crate) fn parse_delta_kind(key: &str, value: &str) -> Result<DeltaKind> {
    match value {
        "plus" | "+" => Ok(DeltaKind::Plus),
        "minus" | "-" => Ok(DeltaKind::Minus),
        other => Err(GraphError::CorruptValue {
            key: key.to_string(),
            reason: format!("invalid delta kind {other}"),
        }),
    }
}

pub(crate) fn sort_deltas(records: &mut [DeltaRecord]) {
    records.sort_by_key(|delta| {
        (
            delta.edge.epoch,
            match delta.kind {
                DeltaKind::Plus => 0_u8,
                DeltaKind::Minus => 1_u8,
            },
            delta.edge.src,
            delta.edge.dst,
        )
    });
}

pub(crate) fn sort_and_dedup_deltas(records: &mut Vec<DeltaRecord>) {
    sort_deltas(records);
    records.dedup_by(|left, right| {
        left.kind == right.kind
            && left.edge.cell_id == right.edge.cell_id
            && left.edge.edge_type == right.edge.edge_type
            && left.edge.src == right.edge.src
            && left.edge.dst == right.edge.dst
            && left.edge.epoch == right.edge.epoch
    });
}

pub(crate) fn encode_write_fence(fence: &GraphWriteFence) -> Vec<u8> {
    format!(
        "write_fence1\t{}\t{}\t{}\t{}\n",
        fence.cell_id, fence.owner_node_id, fence.lease_token, fence.expires_at_ms
    )
    .into_bytes()
}

pub(crate) fn decode_write_fence(key: &str, value: &[u8]) -> Result<GraphWriteFence> {
    let text = std::str::from_utf8(value).map_err(|err| GraphError::CorruptValue {
        key: key.to_string(),
        reason: err.to_string(),
    })?;
    let parts: Vec<&str> = text.trim_end_matches('\n').split('\t').collect();
    if parts.len() != 5 || parts[0] != "write_fence1" {
        return Err(GraphError::CorruptValue {
            key: key.to_string(),
            reason: "expected write_fence1 record with 5 fields".to_string(),
        });
    }
    validate_component("cell_id", parts[1])?;
    validate_component("node_id", parts[2])?;
    Ok(GraphWriteFence {
        cell_id: parts[1].to_string(),
        owner_node_id: parts[2].to_string(),
        lease_token: parse_u64(key, parts[3], "lease_token")?,
        expires_at_ms: parse_u64(key, parts[4], "expires_at_ms")?,
    })
}

pub(crate) fn encode_read_lease(lease: &GraphReadLease) -> Vec<u8> {
    format!(
        "read_lease1\t{}\t{}\t{}\t{}\n",
        lease.cell_id, lease.lease_id, lease.read_epoch, lease.expires_at_ms
    )
    .into_bytes()
}

pub(crate) fn decode_read_lease(key: &str, value: &[u8]) -> Result<GraphReadLease> {
    let text = std::str::from_utf8(value).map_err(|err| GraphError::CorruptValue {
        key: key.to_string(),
        reason: err.to_string(),
    })?;
    let parts: Vec<&str> = text.trim_end_matches('\n').split('\t').collect();
    if parts.len() != 5 || parts[0] != "read_lease1" {
        return Err(GraphError::CorruptValue {
            key: key.to_string(),
            reason: "expected read_lease1 record with 5 fields".to_string(),
        });
    }
    validate_component("cell_id", parts[1])?;
    validate_component("read_lease_id", parts[2])?;
    Ok(GraphReadLease {
        cell_id: parts[1].to_string(),
        lease_id: parts[2].to_string(),
        read_epoch: parse_u64(key, parts[3], "read_epoch")?,
        expires_at_ms: parse_u64(key, parts[4], "expires_at_ms")?,
    })
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
