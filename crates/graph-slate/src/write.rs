use crate::*;
use futures::{stream, StreamExt as _, TryStreamExt as _};
use slatedb::bytes::Bytes;
use slatedb::config::ReadOptions;
use slatedb::DbTransaction;
#[cfg(feature = "datafusion-cypher-engine")]
use slatedb::IsolationLevel;
use std::collections::{BTreeMap, BTreeSet};
#[cfg(feature = "datafusion-cypher-engine")]
use std::sync::Arc;

pub const RELATIONSHIP_IMPORT_READ_CONCURRENCY: usize = 32;

/// The error every write path returns for a list longer than
/// [`MAX_PROPERTY_LIST_ELEMENTS`], so both engines and every client route
/// report the same thing.
pub fn property_list_too_long_error(len: usize) -> GraphError {
    GraphError::UnsupportedQuery {
        reason: QueryFailureReason::Mutation,
        dialect: "Cypher25",
        feature: format!("property lists hold at most {MAX_PROPERTY_LIST_ELEMENTS} elements; this list has {len}"),
    }
}

/// Keeps every stored list inside what the native property codec can write:
/// its element count and each string element's byte length are 32-bit
/// headers. A write that passes here can always be read back.
fn validate_property_list_length(value: &VertexPropertyValue) -> Result<()> {
    match value {
        VertexPropertyValue::List(values) if values.len() > MAX_PROPERTY_LIST_ELEMENTS => Err(property_list_too_long_error(values.len())),
        VertexPropertyValue::List(VertexPropertyList::String(values)) if values.iter().any(|value| u32::try_from(value.len()).is_err()) => {
            Err(GraphError::UnsupportedQuery {
                reason: QueryFailureReason::Mutation,
                dialect: "Cypher25",
                feature: "property list strings must be smaller than 4 GiB".to_string(),
            })
        }
        _ => Ok(()),
    }
}

pub fn validate_vertex_metadata(metadata: &VertexMetadata) -> Result<()> {
    for label in &metadata.labels {
        validate_component("label", label)?;
    }
    for (property, value) in &metadata.properties {
        validate_component("property", property)?;
        validate_property_list_length(value)?;
        value.validate().map_err(|reason| GraphError::CorruptValue {
            key: format!("vertex/property/{property}"),
            reason,
        })?;
    }
    Ok(())
}

pub fn validate_edge_metadata(metadata: &EdgeMetadata) -> Result<()> {
    for (property, value) in &metadata.properties {
        validate_component("property", property)?;
        validate_property_list_length(value)?;
        value.validate().map_err(|reason| GraphError::CorruptValue {
            key: format!("edge/property/{property}"),
            reason,
        })?;
    }
    Ok(())
}

pub fn merge_edge_metadata(previous: &EdgeMetadata, requested: &EdgeMetadata) -> EdgeMetadata {
    let mut next = previous.clone();
    next.properties.extend(
        requested
            .properties
            .iter()
            .map(|(property, value)| (property.clone(), value.clone())),
    );
    next
}

/// The write-back catalog as one transaction sees it. A property's current
/// generation is read on first use and held for the rest of the
/// transaction, so a batch that writes the same property name on thousands
/// of vertices reads its catalog key once, and the usual case, a property
/// the write-back never wrote, costs one bloom-filtered point miss per name
/// per transaction. Held per transaction rather than on the shard so a
/// catalog switch committed between two transactions is seen by the second
/// without an invalidation protocol.
///
/// Pending markers ([`keys::vertex_write_back_pending`]) are held the same
/// way, behind one seek of the cell's marker prefix: a cell with no
/// write-back in flight pays that seek per transaction and nothing per
/// property.
#[derive(Default)]
pub struct WriteBackGenerations {
    resolved: BTreeMap<String, Option<StorageSequence>>,
    any: Option<bool>,
    pending: BTreeMap<String, Option<StorageSequence>>,
    any_pending: Option<bool>,
}

impl WriteBackGenerations {
    /// Whether the cell's catalog names any property: one seek under the
    /// catalog prefix, held for the rest of the transaction. The paths that
    /// would otherwise scan under every vertex they touch ask this first, so
    /// a cell that never ran a write-back pays one seek per transaction for
    /// them and nothing per vertex.
    pub async fn any(&mut self, txn: &DbTransaction, cell_id: &str) -> Result<bool> {
        if let Some(any) = self.any {
            return Ok(any);
        }
        let prefix = keys::vertex_write_back_generation_prefix(cell_id);
        let any = txn.scan_prefix(prefix.as_bytes(), ..).await?.next().await?.is_some();
        self.any = Some(any);
        Ok(any)
    }

    pub async fn generation(&mut self, txn: &DbTransaction, cell_id: &str, property: &str) -> Result<Option<StorageSequence>> {
        if let Some(generation) = self.resolved.get(property) {
            return Ok(*generation);
        }
        let key = keys::vertex_write_back_generation(cell_id, property);
        let generation = read_txn_remote(txn, &key)
            .await?
            .map(|value| decode_u64(&key, &value))
            .transpose()?;
        self.resolved.insert(property.to_string(), generation);
        Ok(generation)
    }

    /// Refuse a write of `property` while a write-back of it is pending: a
    /// hand write moves the entry of the catalog's generation, not the
    /// pending one's, so it would leave the pending generation's entry for
    /// the vertex behind, and the run's next chunk may overwrite the value
    /// it wrote.
    pub async fn refuse_pending(&mut self, txn: &DbTransaction, cell_id: &str, property: &str) -> Result<()> {
        let any_pending = match self.any_pending {
            Some(any_pending) => any_pending,
            None => {
                let prefix = keys::vertex_write_back_pending_prefix(cell_id);
                let any_pending = txn.scan_prefix(prefix.as_bytes(), ..).await?.next().await?.is_some();
                self.any_pending = Some(any_pending);
                any_pending
            }
        };
        if !any_pending {
            return Ok(());
        }
        let pending = match self.pending.get(property) {
            Some(pending) => *pending,
            None => {
                let key = keys::vertex_write_back_pending(cell_id, property);
                let pending = read_txn_remote(txn, &key)
                    .await?
                    .map(|value| decode_u64(&key, &value))
                    .transpose()?;
                self.pending.insert(property.to_string(), pending);
                pending
            }
        };
        match pending {
            Some(generation) => Err(write_back_in_progress(property, generation)),
            None => Ok(()),
        }
    }
}

/// The refusal of a read or write of `property` while the write-back run at
/// `generation` has started and not switched the catalog
/// ([`keys::vertex_write_back_pending`]). Transient: it clears when a run of
/// the property completes.
pub fn write_back_in_progress(property: &str, generation: StorageSequence) -> GraphError {
    GraphError::UnsupportedQuery {
        dialect: "OpenCypher",
        feature: format!(
            "write_back_in_progress: property {property} has a write-back at generation {generation} that has not completed; \
             retry once a write-back of the property completes"
        ),
        reason: QueryFailureReason::WriteBackInProgress,
    }
}

/// Rewrites a vertex's canonical record and brings its label and property
/// indexes from `previous` to `next`, touching only the index keys that
/// differ. Re-putting an unchanged index key would collapse against its own
/// delete inside the batch, but the put still costs WAL, memtable, L0 and
/// every later compaction bytes, so a write that changes one property of a
/// vertex carrying many labels and properties would pay for all of them on
/// every pass. Returns `(puts, deletes)` counting index entries only; the
/// record put is unconditional and is not included.
///
/// A property the write-back catalog names takes the write-back layout
/// instead of the record: its value goes to the vertex's own value key and
/// its index entry to the property's current generation, and the record is
/// written without it. Reads overlay the value key on the record, so a copy
/// in the record would be shadowed by whatever the last write-back put
/// there, and a user's SET would be lost. The entry for the value the key
/// held before is deleted, which needs that value: this is the per-vertex
/// path, so one point read per redirected property is paid here, where the
/// write-back itself reads nothing. A value that is not scalar-indexable
/// keeps its value key and has no index entry, as in the record layout.
///
/// `previous` is the stored record, `None` when the vertex does not exist.
/// Every write-back property `next` names is written, so a caller that
/// builds `next` by patching the stored record starts from
/// [`write_back_patch_base_txn`]: the record may hold a copy of a
/// write-back property older than the value the overlay reads. A
/// write-back value `next` omits is left in place; a caller replacing the
/// whole vertex removes those itself.
///
/// A `next` naming a property whose write-back is pending is refused
/// ([`WriteBackGenerations::refuse_pending`]), whether or not its value
/// changes: the value reads would compare it with is the torn one.
pub async fn apply_vertex_metadata_update_txn(
    txn: &DbTransaction,
    cell_id: &str,
    vertex_id: VertexId,
    previous: Option<&VertexMetadata>,
    next: &VertexMetadata,
    _epoch: StorageSequence,
    generations: &mut WriteBackGenerations,
) -> Result<(u64, u64)> {
    validate_vertex_metadata(next)?;
    if previous.is_none() {
        clear_write_back_values_of_new_vertex_txn(txn, cell_id, vertex_id, generations).await?;
    }
    let absent = VertexMetadata::default();
    let stored = previous.unwrap_or(&absent);
    let mut redirected = Vec::new();
    for (property, value) in &next.properties {
        generations.refuse_pending(txn, cell_id, property).await?;
        if let Some(generation) = generations.generation(txn, cell_id, property).await? {
            redirected.push((property.as_str(), value, generation));
        }
    }
    if redirected.is_empty() {
        return put_vertex_record_and_indexes_txn(txn, cell_id, vertex_id, stored, next);
    }
    let mut record = next.clone();
    for (property, _, _) in &redirected {
        record.properties.remove(*property);
    }
    // A record the redirect leaves as it was is not rewritten: the update
    // changed only write-back properties. A vertex with no record yet gets
    // one even when the redirect empties it, since the record is what makes
    // the vertex exist.
    let (mut puts, mut deletes) = if previous == Some(&record) {
        (0, 0)
    } else {
        put_vertex_record_and_indexes_txn(txn, cell_id, vertex_id, stored, &record)?
    };
    let vertex_ref = encode_u64(vertex_id);
    for (property, value, generation) in redirected {
        let value_key = keys::vertex_write_back_property(cell_id, vertex_id, property);
        let encoded = value.is_scalar_indexable().then(|| encode_vertex_property_value_key(value));
        if let Some(held) = read_txn_remote(txn, &value_key).await? {
            let held = std::str::from_utf8(&held).map_err(|err| GraphError::CorruptValue {
                key: value_key.clone(),
                reason: err.to_string(),
            })?;
            let held = decode_vertex_property_value_record(&value_key, held)?;
            let held = held.is_scalar_indexable().then(|| encode_vertex_property_value_key(&held));
            // Same index entry, same value: nothing to move.
            if held.is_some() && held == encoded {
                continue;
            }
            if let Some(held) = held {
                txn.delete(keys::vertex_write_back_index(cell_id, property, generation, &held, vertex_id).as_bytes())?;
                deletes += 1;
            }
        }
        txn.put(value_key.as_bytes(), encode_vertex_property_value_record(value).as_bytes())?;
        puts += 1;
        if let Some(encoded) = encoded {
            txn.put(
                keys::vertex_write_back_index(cell_id, property, generation, &encoded, vertex_id).as_bytes(),
                vertex_ref.as_slice(),
            )?;
            puts += 1;
        }
    }
    Ok((puts, deletes))
}

/// The vertex a patch is merged into, as reads see it: `record` with the
/// vertex's write-back values laid over it. A write-back writes its values
/// blind and never rewrites the record, so a copy the record held before
/// the run is older than the value reads see; merged back in, it would be
/// written over that value as if the patch had set it. A merge policy's
/// guard compares the patch with the stored value, so the base carries the
/// current one: a guard on a written-back property sees the score, not the
/// stale copy. A caller compares its merged result with this base, not with
/// the record, to tell whether the patch changed anything.
///
/// Only a property with a value under this vertex is replaced. The catalog
/// names a property, not the vertices a run scored, so a vertex outside the
/// run keeps its record's copy, which is the value reads see for it.
///
/// Borrowed when the cell's catalog is empty, the usual case, at one
/// catalog seek per transaction; otherwise one scan under the vertex.
pub async fn write_back_patch_base_txn<'a>(
    txn: &DbTransaction,
    cell_id: &str,
    vertex_id: VertexId,
    record: &'a VertexMetadata,
    generations: &mut WriteBackGenerations,
) -> Result<std::borrow::Cow<'a, VertexMetadata>> {
    if !generations.any(txn, cell_id).await? {
        return Ok(std::borrow::Cow::Borrowed(record));
    }
    let mut base = record.clone();
    base.properties.extend(write_back_values_txn(txn, cell_id, vertex_id).await?);
    Ok(std::borrow::Cow::Owned(base))
}

/// A record is about to be created for `vertex_id`, so any write-back value
/// still under that id belongs to a vertex deleted after a run's projection
/// and before its write-back, which wrote the value blind. Left in place it
/// would be laid over the new vertex as if it were its own. Removed here,
/// at one scan under the vertex per created record when the cell's catalog
/// is not empty, and nothing beyond the per-transaction catalog seek when it
/// is.
pub async fn clear_write_back_values_of_new_vertex_txn(
    txn: &DbTransaction,
    cell_id: &str,
    vertex_id: VertexId,
    generations: &mut WriteBackGenerations,
) -> Result<()> {
    if !generations.any(txn, cell_id).await? {
        return Ok(());
    }
    let values = write_back_values_txn(txn, cell_id, vertex_id).await?;
    if !values.is_empty() {
        remove_write_back_properties_txn(txn, cell_id, vertex_id, &values, generations).await?;
    }
    Ok(())
}

/// Whether `metadata` names a property the write-back catalog holds. A
/// patch result that does is a change even when it equals the stored
/// record: the record's copy is not what reads see.
pub async fn names_write_back_property_txn(
    txn: &DbTransaction,
    cell_id: &str,
    metadata: &VertexMetadata,
    generations: &mut WriteBackGenerations,
) -> Result<bool> {
    for property in metadata.properties.keys() {
        if generations.generation(txn, cell_id, property).await?.is_some() {
            return Ok(true);
        }
    }
    Ok(false)
}

/// The record put and the label and property index diff of
/// [`apply_vertex_metadata_update_txn`], for properties the record holds.
fn put_vertex_record_and_indexes_txn(
    txn: &DbTransaction,
    cell_id: &str,
    vertex_id: VertexId,
    previous: &VertexMetadata,
    next: &VertexMetadata,
) -> Result<(u64, u64)> {
    let vertex_key = keys::vertex(cell_id, vertex_id);
    // The canonical record is the vertex's membership, so clearing its last
    // label or property writes an empty record rather than deleting it. Only
    // `delete_vertex_record_txn` removes a vertex.
    txn.put(vertex_key.as_bytes(), encode_vertex_metadata(next).as_slice())?;
    let (mut puts, mut deletes) = (0_u64, 0_u64);
    let vertex_ref = encode_u64(vertex_id);
    if previous.labels != next.labels {
        bump_vertex_label_version_txn(txn, cell_id)?;
    }
    for label in previous.labels.difference(&next.labels) {
        txn.delete(keys::vertex_label(cell_id, label, vertex_id).as_bytes())?;
        deletes += 1;
    }
    for label in next.labels.difference(&previous.labels) {
        txn.put(keys::vertex_label(cell_id, label, vertex_id).as_bytes(), vertex_ref.as_slice())?;
        puts += 1;
    }
    // The index stores the encoded value segment, so two values that encode
    // the same are the same index entry regardless of how they compare. A
    // value that is not scalar-indexable has no entry on either side.
    let encoded_index_value = |metadata: &VertexMetadata, property: &str| {
        metadata
            .properties
            .get(property)
            .filter(|value| value.is_scalar_indexable())
            .map(encode_vertex_property_value_key)
    };
    for (property, value) in previous.properties.iter().filter(|(_, value)| value.is_scalar_indexable()) {
        let encoded = encode_vertex_property_value_key(value);
        if encoded_index_value(next, property).as_deref() != Some(encoded.as_str()) {
            txn.delete(keys::vertex_property_index(cell_id, property, &encoded, vertex_id).as_bytes())?;
            deletes += 1;
        }
    }
    for (property, value) in next.properties.iter().filter(|(_, value)| value.is_scalar_indexable()) {
        let encoded = encode_vertex_property_value_key(value);
        if encoded_index_value(previous, property).as_deref() != Some(encoded.as_str()) {
            txn.put(
                keys::vertex_property_index(cell_id, property, &encoded, vertex_id).as_bytes(),
                vertex_ref.as_slice(),
            )?;
            puts += 1;
        }
    }
    Ok((puts, deletes))
}

/// Removes a vertex: its canonical record, the indexes over its metadata,
/// and its write-back values with their index entries. The values are found
/// by one prefix scan under the vertex rather than from `previous`, which
/// is the decoded record and does not carry them; the delete batch already
/// reads inside its transaction, so this adds a scan to it, not a phase. An
/// entry of a generation the catalog no longer names is left to the sweep.
pub async fn delete_vertex_record_txn(
    txn: &DbTransaction,
    cell_id: &str,
    vertex_id: VertexId,
    previous: &VertexMetadata,
    generations: &mut WriteBackGenerations,
) -> Result<()> {
    delete_vertex_metadata_indexes_txn(txn, cell_id, vertex_id, previous)?;
    let values = write_back_values_txn(txn, cell_id, vertex_id).await?;
    remove_write_back_properties_txn(txn, cell_id, vertex_id, &values, generations).await?;
    txn.delete(keys::vertex(cell_id, vertex_id).as_bytes())?;
    Ok(())
}

/// The write-back values of one vertex, by property, read inside the
/// transaction: one prefix scan under the vertex.
pub async fn write_back_values_txn(txn: &DbTransaction, cell_id: &str, vertex_id: VertexId) -> Result<Vec<(String, VertexPropertyValue)>> {
    let prefix = keys::vertex_write_back_property_prefix(cell_id, vertex_id);
    let mut iter = txn.scan_prefix_with_options(prefix.as_bytes(), .., &remote_scan_options()).await?;
    let mut values = Vec::new();
    while let Some(kv) = iter.next().await? {
        let key = String::from_utf8_lossy(&kv.key).into_owned();
        let Some(property) = key.strip_prefix(&prefix) else {
            return Err(GraphError::CorruptValue {
                key,
                reason: "expected a write-back value key".to_string(),
            });
        };
        let value = std::str::from_utf8(&kv.value).map_err(|err| GraphError::CorruptValue {
            key: key.clone(),
            reason: err.to_string(),
        })?;
        let value = decode_vertex_property_value_record(&key, value)?;
        values.push((property.to_string(), value));
    }
    Ok(values)
}

/// A vertex as reads see it, from its stored `record`: the record with its
/// write-back values laid over it, and those values. `None` and no values
/// when the vertex does not exist; an absent vertex is not scanned. A
/// replacement of the whole vertex compares with this view, since the
/// record alone can match while reads see a different write-back value.
pub async fn overlaid_vertex_metadata_txn(
    txn: &DbTransaction,
    cell_id: &str,
    vertex_id: VertexId,
    record: Option<&VertexMetadata>,
) -> Result<(Option<VertexMetadata>, Vec<(String, VertexPropertyValue)>)> {
    let Some(record) = record else {
        return Ok((None, Vec::new()));
    };
    let values = write_back_values_txn(txn, cell_id, vertex_id).await?;
    let mut seen = record.clone();
    seen.properties.extend(values.iter().cloned());
    Ok((Some(seen), values))
}

/// Remove write-back values from a vertex: each value key, and its entry in
/// the generation the catalog names for the property. An entry in a
/// generation the catalog no longer names is left to the sweep. A value of
/// a property whose write-back is pending is refused: its entry in the
/// pending generation is not the catalog's, so it would be left behind.
pub async fn remove_write_back_properties_txn(
    txn: &DbTransaction,
    cell_id: &str,
    vertex_id: VertexId,
    values: &[(String, VertexPropertyValue)],
    generations: &mut WriteBackGenerations,
) -> Result<()> {
    for (property, value) in values {
        generations.refuse_pending(txn, cell_id, property).await?;
        if value.is_scalar_indexable() {
            if let Some(generation) = generations.generation(txn, cell_id, property).await? {
                txn.delete(
                    keys::vertex_write_back_index(cell_id, property, generation, &encode_vertex_property_value_key(value), vertex_id)
                        .as_bytes(),
                )?;
            }
        }
        txn.delete(keys::vertex_write_back_property(cell_id, vertex_id, property).as_bytes())?;
    }
    Ok(())
}

/// Gives every vertex in `endpoints` that has no canonical record an empty
/// one, inside the transaction that inserts their edges, so a committed edge
/// never names an endpoint that is absent. Existing records are left as they
/// are, and `skip` names vertices this transaction already writes a record for.
/// Callers pass the endpoints of edges they insert only: rewriting an edge
/// that already exists does not repair an endpoint stored without a record
/// before this rule, which is migration work, not a cost every write pays.
/// A fresh cell (`fresh`) has no records yet, so its endpoints are written
/// without reading them first. Returns the number of records created.
pub async fn ensure_vertex_records_txn(
    txn: &DbTransaction,
    cell_id: &str,
    endpoints: impl IntoIterator<Item = VertexId>,
    skip: &BTreeSet<VertexId>,
    fresh: bool,
) -> Result<usize> {
    let pending = endpoints
        .into_iter()
        .filter(|vertex| !skip.contains(vertex))
        .map(|vertex| (keys::vertex(cell_id, vertex), vertex))
        .collect::<BTreeMap<_, _>>();
    if pending.is_empty() {
        return Ok(0);
    }
    let existing = if fresh {
        BTreeMap::new()
    } else {
        read_txn_remote_many(txn, pending.keys().cloned()).await?
    };
    let empty = encode_vertex_metadata(&VertexMetadata::default());
    // A fresh cell has no catalog, so it skips even the catalog seek.
    let mut generations = WriteBackGenerations::default();
    let mut created = 0;
    for (key, vertex) in &pending {
        if existing.get(key).and_then(Option::as_ref).is_none() {
            if !fresh {
                clear_write_back_values_of_new_vertex_txn(txn, cell_id, *vertex, &mut generations).await?;
            }
            txn.put(key.as_bytes(), empty.as_slice())?;
            created += 1;
        }
    }
    Ok(created)
}

/// Decodes a canonical vertex read: `None` when the vertex has no record.
pub fn decode_vertex_record(key: &str, value: Option<&Bytes>) -> Result<Option<VertexMetadata>> {
    value.map(|value| decode_vertex_metadata(key, value)).transpose()
}

/// Move the cell's label membership version ([`keys::vertex_label_version`])
/// to this transaction's start sequence. Every helper that puts or deletes
/// a label index entry calls it, so no writer can change membership without
/// it.
///
/// The start sequence needs no read and still strictly increases across
/// committed label changes of a cell: two transactions that both put this
/// key conflict unless the second began after the first committed, and so
/// past its commit sequence. The label writers of a cell already hold its
/// writer lane, so that conflict adds no aborts. Repeated puts in one
/// transaction coalesce in its write batch, so a batch commits one version
/// key however many vertices it relabels; each call still costs a batch
/// insert, the same as one label entry.
fn bump_vertex_label_version_txn(txn: &DbTransaction, cell_id: &str) -> Result<()> {
    txn.put(keys::vertex_label_version(cell_id).as_bytes(), encode_u64(txn.seqnum()).as_slice())?;
    Ok(())
}

pub fn put_vertex_metadata_indexes_txn(txn: &DbTransaction, cell_id: &str, vertex_id: VertexId, metadata: &VertexMetadata) -> Result<()> {
    if !metadata.labels.is_empty() {
        bump_vertex_label_version_txn(txn, cell_id)?;
    }
    for label in &metadata.labels {
        txn.put(
            keys::vertex_label(cell_id, label, vertex_id).as_bytes(),
            encode_u64(vertex_id).as_slice(),
        )?;
    }
    for (property, value) in &metadata.properties {
        if !value.is_scalar_indexable() {
            continue;
        }
        txn.put(
            keys::vertex_property_index(cell_id, property, &encode_vertex_property_value_key(value), vertex_id).as_bytes(),
            encode_u64(vertex_id).as_slice(),
        )?;
    }
    Ok(())
}

pub fn delete_vertex_metadata_indexes_txn(
    txn: &DbTransaction,
    cell_id: &str,
    vertex_id: VertexId,
    metadata: &VertexMetadata,
) -> Result<()> {
    if !metadata.labels.is_empty() {
        bump_vertex_label_version_txn(txn, cell_id)?;
    }
    for label in &metadata.labels {
        txn.delete(keys::vertex_label(cell_id, label, vertex_id).as_bytes())?;
    }
    for (property, value) in &metadata.properties {
        if !value.is_scalar_indexable() {
            continue;
        }
        txn.delete(keys::vertex_property_index(cell_id, property, &encode_vertex_property_value_key(value), vertex_id).as_bytes())?;
    }
    Ok(())
}

pub fn apply_edge_metadata_update_txn(
    txn: &DbTransaction,
    target: EdgeMetadataTarget<'_>,
    previous: &EdgeMetadata,
    next: &EdgeMetadata,
    _epoch: StorageSequence,
) -> Result<()> {
    validate_edge_metadata(next)?;
    let edge_metadata_key = keys::edge_metadata(target.cell_id, target.edge_type, target.src, target.dst);
    delete_edge_metadata_indexes_txn(txn, target, previous)?;
    if next.properties.is_empty() {
        txn.delete(edge_metadata_key.as_bytes())?;
    } else {
        txn.put(edge_metadata_key.as_bytes(), encode_edge_metadata(next).as_slice())?;
        put_edge_metadata_indexes_txn(txn, target, next)?;
    }
    Ok(())
}

#[derive(Clone, Copy)]
pub struct EdgeMetadataTarget<'a> {
    pub cell_id: &'a str,
    pub edge_type: &'a str,
    pub src: VertexId,
    pub dst: VertexId,
}

pub fn put_edge_metadata_indexes_txn(txn: &DbTransaction, target: EdgeMetadataTarget<'_>, metadata: &EdgeMetadata) -> Result<()> {
    for (property, value) in &metadata.properties {
        if !value.is_scalar_indexable() {
            continue;
        }
        txn.put(
            keys::edge_property_index(
                target.cell_id,
                target.edge_type,
                property,
                &encode_vertex_property_value_key(value),
                target.src,
                target.dst,
            )
            .as_bytes(),
            encode_u64(target.dst).as_slice(),
        )?;
    }
    Ok(())
}

/// The property MERGE resolves relationship identity by. Not guaranteed
/// unique: parallel relationships between the same endpoints may share a
/// value, and MERGE matches all of them. The `rmerge_idx` pointer therefore
/// only exists while exactly one live row carries an identity — the MERGE
/// path writes it under that proof, and every other write that could mint a
/// duplicate invalidates it (see the index hooks below).
pub const RELATIONSHIP_IDENTITY_PROPERTY: &str = "id";

pub fn put_relationship_property_indexes_txn(txn: &DbTransaction, record: &RelationshipRecord) -> Result<()> {
    for (property, value) in &record.metadata.properties {
        if !value.is_scalar_indexable() {
            continue;
        }
        txn.put(
            keys::relationship_property_index(
                &record.cell_id,
                &record.edge_type,
                property,
                &encode_vertex_property_value_key(value),
                record.src,
                record.dst,
                record.relationship_id,
            )
            .as_bytes(),
            encode_u64(record.relationship_id).as_slice(),
        )?;
    }
    // Invalidate, never write: this hook runs on every path that puts a
    // relationship record — CREATE and plain imports included — any of which
    // may be minting a second row with the same identity value. A deleted
    // pointer costs the next MERGE one fallback scan, which re-earns it if
    // the identity is still unique; the MERGE path's deferred pointer writes
    // run after these hooks and overrule this delete inside its own
    // transaction.
    if let Some(value) = record
        .metadata
        .properties
        .get(RELATIONSHIP_IDENTITY_PROPERTY)
        .filter(|value| value.is_scalar_indexable())
    {
        txn.delete(
            keys::relationship_merge_index(
                &record.cell_id,
                &record.edge_type,
                RELATIONSHIP_IDENTITY_PROPERTY,
                &encode_vertex_property_value_key(value),
                record.src,
                record.dst,
            )
            .as_bytes(),
        )?;
    }
    Ok(())
}

pub async fn read_txn_remote_many(txn: &DbTransaction, keys: impl IntoIterator<Item = String>) -> Result<BTreeMap<String, Option<Bytes>>> {
    let keys = keys.into_iter().collect::<BTreeSet<_>>();
    let options = relationship_import_read_options(keys.len());
    read_txn_remote_many_with_options(txn, keys, &options).await
}

pub async fn read_txn_remote_many_with_options(
    txn: &DbTransaction,
    keys: BTreeSet<String>,
    options: &ReadOptions,
) -> Result<BTreeMap<String, Option<Bytes>>> {
    // Small point-read sets repeat across adjacent relationship batches and
    // are safe to admit to the process-wide bounded cache. Large sets remain
    // uncached so migration traffic cannot replace the complete hot working
    // set with one-use blocks. mark_read still preserves serializable conflict
    // detection in either case.
    stream::iter(keys.into_iter().map(|key| async move {
        let value = read_txn_remote_with_options(txn, &key, options).await?;
        Ok::<_, GraphError>((key, value))
    }))
    .buffer_unordered(RELATIONSHIP_IMPORT_READ_CONCURRENCY)
    .try_collect()
    .await
}

pub async fn read_txn_remote_uncached(txn: &DbTransaction, key: &str) -> Result<Option<Bytes>> {
    read_txn_remote_with_options(txn, key, &relationship_import_uncached_read_options()).await
}

pub async fn read_txn_remote_with_options(txn: &DbTransaction, key: &str, options: &ReadOptions) -> Result<Option<Bytes>> {
    txn.mark_read([key.as_bytes()])?;
    Ok(txn.get_with_options(key.as_bytes(), options).await?)
}

pub fn relationship_import_read_options(expected_items: usize) -> ReadOptions {
    remote_read_options_for_expected_items(expected_items as u64)
}

pub fn relationship_import_uncached_read_options() -> ReadOptions {
    remote_read_options_for_expected_items(u64::MAX)
}

pub fn counter_value(values: &BTreeMap<String, Option<Bytes>>, key: &str, fresh_cell: bool) -> Result<u64> {
    if fresh_cell {
        return Ok(0);
    }
    match values.get(key) {
        Some(Some(value)) => decode_u64(key, value),
        Some(None) => Ok(0),
        None => Err(GraphError::CorruptValue {
            key: key.to_string(),
            reason: "batched counter read did not include the requested key".to_string(),
        }),
    }
}

/// The counter's id after `current`. Every relationship id the store assigns
/// comes from here, so none reaches the range the query engine generates ids
/// in; see [`crate::GENERATED_RELATIONSHIP_ID_FLOOR`].
pub fn next_relationship_id(current: RelationshipId, cell_id: &str) -> Result<RelationshipId> {
    let next = current.checked_add(1).ok_or_else(|| GraphError::CorruptValue {
        key: keys::last_relationship_id(cell_id),
        reason: "relationship id overflow".to_string(),
    })?;
    crate::stored_relationship_id(next, || keys::last_relationship_id(cell_id))
}

pub async fn next_available_relationship_ids_txn(
    txn: &DbTransaction,
    cell_id: &str,
    cursor: &mut RelationshipId,
    count: usize,
    operation: &str,
) -> Result<Vec<RelationshipId>> {
    let mut available = Vec::with_capacity(count);
    while available.len() < count {
        let remaining = count - available.len();
        let mut candidates = Vec::with_capacity(remaining);
        for _ in 0..remaining {
            *cursor = next_relationship_id(*cursor, cell_id).map_err(|error| match error {
                GraphError::CorruptValue { key, reason } => GraphError::CorruptValue {
                    key,
                    reason: format!("{reason} during {operation}"),
                },
                other => other,
            })?;
            candidates.push(*cursor);
        }
        let existing = read_txn_remote_many(
            txn,
            candidates
                .iter()
                .map(|relationship_id| keys::relationship_id(cell_id, *relationship_id)),
        )
        .await?;
        for relationship_id in candidates {
            let key = keys::relationship_id(cell_id, relationship_id);
            if existing.get(&key).and_then(Option::as_ref).is_none() {
                available.push(relationship_id);
            }
        }
    }
    Ok(available)
}

pub async fn live_relationships_for_edge_txn(
    txn: &DbTransaction,
    cell_id: &str,
    edge_type: &str,
    src: VertexId,
    dst: VertexId,
    _read_epoch: StorageSequence,
) -> Result<Vec<RelationshipRecord>> {
    let prefix = keys::relationship_edge_prefix(cell_id, edge_type, src, dst);
    let mut iter = txn.scan_prefix(prefix.as_bytes(), ..).await?;
    let mut records = Vec::new();
    while let Some(kv) = iter.next().await? {
        let key = String::from_utf8_lossy(&kv.key).into_owned();
        records.push(decode_relationship_record(&key, &kv.value)?);
    }
    Ok(records)
}

pub async fn delete_structural_edge_txn(
    writes_reverse_index: bool,
    xlog_floor_ensured: &std::sync::RwLock<std::collections::HashSet<String>>,
    txn: &DbTransaction,
    mutation: &EdgeMutation,
    epoch: StorageSequence,
) -> Result<bool> {
    let edge_key = keys::out_edge(&mutation.cell_id, &mutation.edge_type, mutation.src, mutation.dst);
    if read_txn_remote(txn, &edge_key).await?.is_none() {
        return Ok(false);
    }
    mark_topology_change_txn_inner(
        xlog_floor_ensured,
        txn,
        &mutation.cell_id,
        &mutation.edge_type,
        epoch,
        &[(mutation.src, mutation.dst, false)],
    )
    .await?;
    let out_degree_key = keys::degree_out(&mutation.cell_id, &mutation.edge_type, mutation.src);
    let out_degree = read_counter_txn(txn, &out_degree_key).await?.saturating_sub(1);
    let in_degree = if writes_reverse_index {
        let in_degree_key = keys::degree_in(&mutation.cell_id, &mutation.edge_type, mutation.dst);
        let in_degree = read_counter_txn(txn, &in_degree_key).await?.saturating_sub(1);
        Some((in_degree_key, in_degree))
    } else {
        None
    };
    let edge_metadata_key = keys::edge_metadata(&mutation.cell_id, &mutation.edge_type, mutation.src, mutation.dst);
    let previous_edge_metadata = match read_txn_remote(txn, &edge_metadata_key).await? {
        Some(value) => decode_edge_metadata(&edge_metadata_key, &value)?,
        None => EdgeMetadata::default(),
    };
    if !previous_edge_metadata.properties.is_empty() {
        apply_edge_metadata_update_txn(
            txn,
            EdgeMetadataTarget {
                cell_id: &mutation.cell_id,
                edge_type: &mutation.edge_type,
                src: mutation.src,
                dst: mutation.dst,
            },
            &previous_edge_metadata,
            &EdgeMetadata::default(),
            epoch,
        )?;
    }
    txn.delete(keys::edge(&mutation.cell_id, &mutation.edge_type, mutation.src, mutation.dst).as_bytes())?;
    txn.delete(edge_key.as_bytes())?;
    if writes_reverse_index {
        txn.delete(keys::in_edge(&mutation.cell_id, &mutation.edge_type, mutation.dst, mutation.src).as_bytes())?;
    }
    txn.put(out_degree_key.as_bytes(), encode_u64(out_degree))?;
    if let Some((in_degree_key, in_degree)) = in_degree {
        txn.put(in_degree_key.as_bytes(), encode_u64(in_degree))?;
    }
    Ok(true)
}

pub async fn mark_topology_change_txn_inner(
    xlog_floor_ensured: &std::sync::RwLock<std::collections::HashSet<String>>,
    txn: &DbTransaction,
    cell_id: &str,
    edge_type: &str,
    epoch: StorageSequence,
    changes: &[(VertexId, VertexId, bool)],
) -> Result<()> {
    let span = tracing::info_span!(
        "write.index_update",
        hydradb.cell_id = %cell_id,
        hydradb.edge_type = %edge_type,
        hydradb.commit_epoch = epoch,
        hydradb.xlog_changes = changes.len() as u64,
    );
    span.in_scope(|| {
        txn.put(keys::matrix_dirty(cell_id, edge_type).as_bytes(), encode_u64(epoch))?;
        txn.put(keys::adjacency_generation(cell_id, edge_type).as_bytes(), encode_u64(epoch))
    })?;
    if changes.is_empty() {
        return Ok(());
    }
    // Coverage floor: the first-ever logged change for the pair sets the
    // low-water mark to its own epoch, so the builder knows coverage
    // begins here. The in-memory set only caches *confirmed presence* —
    // a pending put is never cached, so a rolled-back transaction cannot
    // strand the floor.
    let floor_token = format!("{cell_id}/{edge_type}");
    let ensured = xlog_floor_ensured.read().expect("xlog floor cache poisoned").contains(&floor_token);
    if !ensured {
        let low_key = keys::xlog_low_water(cell_id, edge_type);
        match read_txn_remote(txn, &low_key).await? {
            Some(_) if crate::write_pipeline::current().is_none() => {
                xlog_floor_ensured.write().expect("xlog floor cache poisoned").insert(floor_token);
            }
            Some(_) => {}
            None => {
                txn.put(low_key.as_bytes(), encode_u64(epoch))?;
            }
        }
    }
    span.in_scope(|| {
        for (src, dst, exists) in changes {
            txn.put(
                keys::xlog_entry(cell_id, edge_type, epoch, *src, *dst).as_bytes(),
                if *exists { &[1u8][..] } else { &[0u8][..] },
            )?;
        }
        Ok(())
    })
}

pub async fn delete_relationships_for_structural_edge_txn(
    txn: &DbTransaction,
    mutation: &EdgeMutation,
    read_epoch: StorageSequence,
) -> Result<u64> {
    let relationships =
        live_relationships_for_edge_txn(txn, &mutation.cell_id, &mutation.edge_type, mutation.src, mutation.dst, read_epoch).await?;
    delete_prepared_relationships_for_structural_edge_txn(txn, mutation, &relationships)
}

pub fn delete_prepared_relationships_for_structural_edge_txn(
    txn: &DbTransaction,
    mutation: &EdgeMutation,
    relationships: &[RelationshipRecord],
) -> Result<u64> {
    for record in relationships {
        txn.delete(keys::relationship(&record.cell_id, &record.edge_type, record.src, record.dst, record.relationship_id).as_bytes())?;
        txn.delete(keys::relationship_id(&record.cell_id, record.relationship_id).as_bytes())?;
        delete_relationship_property_indexes_txn(txn, record, &record.metadata)?;
    }
    txn.delete(keys::relationship_count(&mutation.cell_id, &mutation.edge_type, mutation.src, mutation.dst).as_bytes())?;
    u64::try_from(relationships.len()).map_err(|err| GraphError::CorruptValue {
        key: keys::relationship_edge_prefix(&mutation.cell_id, &mutation.edge_type, mutation.src, mutation.dst),
        reason: format!("too many relationships to delete with structural edge: {err}"),
    })
}

pub fn delete_relationship_property_indexes_txn(txn: &DbTransaction, record: &RelationshipRecord, metadata: &EdgeMetadata) -> Result<()> {
    for (property, value) in &metadata.properties {
        if !value.is_scalar_indexable() {
            continue;
        }
        txn.delete(
            keys::relationship_property_index(
                &record.cell_id,
                &record.edge_type,
                property,
                &encode_vertex_property_value_key(value),
                record.src,
                record.dst,
                record.relationship_id,
            )
            .as_bytes(),
        )?;
    }
    // A blind delete: the pointer may name a different relationship if two
    // live rows ever shared an identity, but a missing pointer only costs the
    // next MERGE one fallback scan, which rewrites it (heal-on-read). A wrong
    // surviving pointer is the outcome this avoids.
    if let Some(value) = metadata
        .properties
        .get(RELATIONSHIP_IDENTITY_PROPERTY)
        .filter(|value| value.is_scalar_indexable())
    {
        txn.delete(
            keys::relationship_merge_index(
                &record.cell_id,
                &record.edge_type,
                RELATIONSHIP_IDENTITY_PROPERTY,
                &encode_vertex_property_value_key(value),
                record.src,
                record.dst,
            )
            .as_bytes(),
        )?;
    }
    Ok(())
}

pub fn delete_edge_metadata_indexes_txn(txn: &DbTransaction, target: EdgeMetadataTarget<'_>, metadata: &EdgeMetadata) -> Result<()> {
    for (property, value) in &metadata.properties {
        if !value.is_scalar_indexable() {
            continue;
        }
        txn.delete(
            keys::edge_property_index(
                target.cell_id,
                target.edge_type,
                property,
                &encode_vertex_property_value_key(value),
                target.src,
                target.dst,
            )
            .as_bytes(),
        )?;
    }
    Ok(())
}

#[cfg(feature = "datafusion-cypher-engine")]
pub struct DataFusionWriteGuard {
    _local: LocalWriteGuard,
    _graph_permit: crate::lifecycle::GraphWritePermit,
    _write_active_diagnostic: crate::core::memory_diagnostics::MemoryDiagnosticGuard,
    _write_hold_diagnostic: crate::core::memory_diagnostics::MemoryDiagnosticGuard,
    pub pipeline: Option<Arc<crate::write_pipeline::WritePipeline>>,
}

#[cfg(feature = "datafusion-cypher-engine")]
pub async fn datafusion_read_receipt(txn: &DbTransaction, key: &str) -> Result<Option<Vec<u8>>> {
    Ok(read_txn_remote(txn, key).await?.map(|bytes| bytes.to_vec()))
}

#[cfg(feature = "datafusion-cypher-engine")]
pub async fn datafusion_read_value(txn: &DbTransaction, key: &str) -> Result<Option<Bytes>> {
    read_txn_remote(txn, key).await
}

/// The properties a cell's write-back catalog names, from the keys under
/// [`keys::vertex_write_back_generation_prefix`]. The DataFusion engine reads
/// vertex properties from the record and the classic property index, and
/// neither holds a write-back property: its value lives at its own key and
/// its index under the catalog's generation. Until that engine reads and
/// writes the write-back layout, it refuses any query that touches one of
/// these properties instead of answering from the record alone.
#[cfg(feature = "datafusion-cypher-engine")]
pub async fn write_back_catalog_properties(mut entries: slatedb::DbIterator, cell_id: &str) -> Result<BTreeSet<String>> {
    let prefix = keys::vertex_write_back_generation_prefix(cell_id);
    let mut properties = BTreeSet::new();
    while let Some(kv) = entries.next().await? {
        let key = String::from_utf8_lossy(&kv.key).into_owned();
        let Some(property) = key.strip_prefix(&prefix) else {
            return Err(GraphError::CorruptValue {
                key,
                reason: "expected a write-back catalog key".to_string(),
            });
        };
        properties.insert(property.to_string());
    }
    Ok(properties)
}

/// Why the DataFusion engine refuses a query that reads or writes a property
/// the cell's write-back catalog names.
#[cfg(feature = "datafusion-cypher-engine")]
pub fn datafusion_write_back_property_refusal(property: &str) -> String {
    format!(
        "property `{property}` is written by a gds `.write` call and the DataFusion Cypher engine does not read or write \
         gds write-back properties; run this query on the legacy or experimental Cypher engine"
    )
}

/// Why the DataFusion engine refuses a query that reads every property of a
/// vertex in a cell whose write-back catalog is not empty. Whether one
/// vertex carries write-back values is known only by scanning under that
/// vertex, so the refusal is per cell rather than per vertex.
#[cfg(feature = "datafusion-cypher-engine")]
pub fn datafusion_write_back_whole_vertex_refusal(cell_id: &str, properties: &BTreeSet<String>) -> String {
    let named = properties
        .iter()
        .map(|property| format!("`{property}`"))
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "this query reads every property of a vertex, and cell `{cell_id}` holds gds write-back properties ({named}) that the \
         DataFusion Cypher engine does not read; run this query on the legacy or experimental Cypher engine"
    )
}

/// The write-back catalog as one DataFusion mutation session sees it: read
/// on the session's first node write and held until the session opens a new
/// transaction, so a statement that writes thousands of vertices scans the
/// catalog once.
#[cfg(feature = "datafusion-cypher-engine")]
#[derive(Default)]
pub struct DataFusionWriteBackCatalog {
    properties: Option<BTreeSet<String>>,
}

#[cfg(feature = "datafusion-cypher-engine")]
impl DataFusionWriteBackCatalog {
    /// Forget the catalog: the next transaction reads it again, since a
    /// write-back may have switched one in between.
    pub fn reset(&mut self) {
        self.properties = None;
    }

    async fn refuse<'a>(&mut self, txn: &DbTransaction, cell_id: &str, keys: impl IntoIterator<Item = &'a String>) -> Result<()> {
        let properties = match &mut self.properties {
            Some(properties) => properties,
            slot => {
                let prefix = keys::vertex_write_back_generation_prefix(cell_id);
                let entries = txn.scan_prefix(prefix.as_bytes(), ..).await?;
                slot.insert(write_back_catalog_properties(entries, cell_id).await?)
            }
        };
        if let Some(property) = keys.into_iter().find(|key| properties.contains(key.as_str())) {
            return Err(GraphError::UnsupportedQuery {
                reason: QueryFailureReason::Mutation,
                dialect: "DataFusionCypher",
                feature: datafusion_write_back_property_refusal(property),
            });
        }
        Ok(())
    }
}

#[cfg(feature = "datafusion-cypher-engine")]
impl SlateGraphStore {
    fn writes_reverse_index(&self) -> bool {
        self.mutation_state().index_policy.write_reverse_index()
    }

    async fn mark_topology_change_txn(
        &self,
        txn: &DbTransaction,
        cell_id: &str,
        edge_type: &str,
        epoch: StorageSequence,
        changes: &[(VertexId, VertexId, bool)],
    ) -> Result<()> {
        mark_topology_change_txn_inner(&self.mutation_state().xlog_floor_ensured, txn, cell_id, edge_type, epoch, changes).await
    }

    /// Start one serializable SlateDB transaction for a provider mutation
    /// session. The permits and local writer guard outlive the call and are
    /// released only when the session commits or aborts.
    pub async fn begin_datafusion_transaction(&self, cell_id: &str) -> Result<(DbTransaction, DataFusionWriteGuard)> {
        validate_component("cell_id", cell_id)?;
        self.ensure_write_authority(cell_id, "datafusion_cypher_mutation")?;
        let pipeline = crate::write_pipeline::begin_datafusion_write_pipeline(self).await?;
        let (graph_permit, write_active_diagnostic, write_hold_diagnostic) =
            self.acquire_graph_write_permit("datafusion_cypher_mutation").await?;
        let local = self
            .acquire_local_write_guard_no_fence(cell_id, "datafusion_cypher_mutation")
            .await?;
        self.validate_changing_write().await?;
        let txn = self.writer()?.begin(IsolationLevel::SerializableSnapshot)?;
        self.validate_write_fence_txn(&txn, cell_id, "datafusion_cypher_mutation").await?;
        Ok((
            txn,
            DataFusionWriteGuard {
                _local: local,
                _graph_permit: graph_permit,
                _write_active_diagnostic: write_active_diagnostic,
                _write_hold_diagnostic: write_hold_diagnostic,
                pipeline,
            },
        ))
    }

    /// Commit a provider-owned transaction at its next SlateDB sequence.
    /// Outside the legacy write-pipeline task scope this waits directly on the
    /// configured SlateDB durability barrier.
    pub async fn commit_datafusion_transaction(
        &self,
        txn: DbTransaction,
        pipeline: Option<Arc<crate::write_pipeline::WritePipeline>>,
    ) -> Result<Option<u64>> {
        if let Some(pipeline) = pipeline {
            let sequence = txn.seqnum().checked_add(1).ok_or_else(|| GraphError::CorruptValue {
                key: "storage_sequence".into(),
                reason: "SlateDB sequence overflow".into(),
            })?;
            crate::write_pipeline::commit_datafusion_write_pipeline(self, pipeline, txn, sequence).await
        } else {
            commit_txn_strict_with_sequence(txn, self.mutation_state().await_durable_writes).await
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn apply_datafusion_mutation(
        &self,
        txn: &DbTransaction,
        cell_id: &str,
        operation: &hydradb_graph_provider::MutationOperation,
        next_node_id: &mut Option<VertexId>,
        request: &hydradb_graph_provider::MutationRequest,
        budget: &hydradb_graph_provider::BudgetTracker,
        write_back: &mut DataFusionWriteBackCatalog,
    ) -> Result<(
        std::sync::Arc<Vec<hydradb_graph_provider::MutationTarget>>,
        hydradb_graph_provider::MutationCounters,
    )> {
        use hydradb_graph_provider::{MutationCounters, MutationOperation as Op, MutationTarget};

        let mut counters = MutationCounters::default();
        let epoch = txn.seqnum().checked_add(1).ok_or_else(|| GraphError::CorruptValue {
            key: "storage_sequence".into(),
            reason: "SlateDB sequence overflow".into(),
        })?;
        match operation {
            Op::CreateNode {
                identity,
                labels,
                properties,
            } => {
                reject_provider_node_identity_properties(properties)?;
                write_back.refuse(txn, cell_id, properties.keys()).await?;
                let id = provider_reserve_node_id_txn(txn, cell_id, identity.map(|id| id.0), next_node_id, request, budget).await?;
                let metadata = provider_vertex_metadata(labels, properties)?;
                apply_vertex_metadata_update_txn(txn, cell_id, id, None, &metadata, epoch, &mut WriteBackGenerations::default()).await?;
                counters.nodes_created = 1;
                Ok((
                    provider_single_target(budget, MutationTarget::Node(hydradb_graph_types::NodeId(id)))?,
                    counters,
                ))
            }
            Op::CreateRelationship {
                source,
                destination,
                relationship_type,
                properties,
            } => {
                let record = provider_create_relationship_txn(
                    self,
                    txn,
                    cell_id,
                    ProviderRelationshipCreate {
                        source: source.0,
                        destination: destination.0,
                        relationship_type,
                        properties,
                        epoch,
                    },
                )
                .await?;
                counters.relationships_created = 1;
                Ok((
                    provider_single_target(
                        budget,
                        MutationTarget::Relationship(hydradb_graph_types::RelationshipId(record.relationship_id)),
                    )?,
                    counters,
                ))
            }
            Op::SetProperty { target, key, value } => {
                reject_provider_node_identity_property(*target, key)?;
                if matches!(target, MutationTarget::Node(_)) {
                    write_back.refuse(txn, cell_id, [key]).await?;
                }
                let changed = provider_set_property_txn(self, txn, cell_id, *target, key, value, epoch).await?;
                if changed {
                    counters.properties_set = 1;
                }
                Ok((provider_single_target(budget, *target)?, counters))
            }
            Op::RemoveProperty { target, key } => {
                reject_provider_node_identity_property(*target, key)?;
                if matches!(target, MutationTarget::Node(_)) {
                    write_back.refuse(txn, cell_id, [key]).await?;
                }
                let changed =
                    provider_set_property_txn(self, txn, cell_id, *target, key, &hydradb_graph_types::CypherValue::Null, epoch).await?;
                if changed {
                    counters.properties_removed = 1;
                }
                Ok((provider_single_target(budget, *target)?, counters))
            }
            Op::DeleteRelationship { relationship } => {
                if provider_delete_relationship_txn(self, txn, cell_id, relationship.0, epoch, request, budget).await? {
                    counters.relationships_deleted = 1;
                }
                Ok((provider_empty_targets(budget)?, counters))
            }
            Op::DeleteNode { node, detach } => {
                if *detach {
                    return Err(GraphError::UnsupportedQuery {
                        reason: QueryFailureReason::Mutation,
                        dialect: "Cypher25",
                        feature: "DETACH DELETE is not supported by the initial SlateDB provider adapter".into(),
                    });
                }
                if provider_delete_isolated_node_txn(txn, cell_id, node.0, request, budget).await? {
                    counters.nodes_deleted = 1;
                }
                Ok((provider_empty_targets(budget)?, counters))
            }
            Op::MergeNode {
                identity,
                labels,
                match_properties,
                on_create,
                on_match,
            } => {
                reject_provider_node_identity_properties(match_properties)?;
                reject_provider_node_identity_properties(on_create)?;
                reject_provider_node_identity_properties(on_match)?;
                // The match compares record properties, and both branches
                // write theirs: a write-back property in any of the three
                // would be matched or written against the record alone.
                write_back
                    .refuse(txn, cell_id, match_properties.keys().chain(on_create.keys()).chain(on_match.keys()))
                    .await?;
                let existing = match identity {
                    Some(identity) => provider_find_node_identity_txn(txn, cell_id, *identity, labels, match_properties, budget).await?,
                    None => provider_find_node_txn(txn, cell_id, labels, match_properties, request, budget).await?,
                };
                if let Some(targets) = existing {
                    for target in targets.iter() {
                        for (key, value) in on_match {
                            provider_set_property_txn(self, txn, cell_id, *target, key, value, epoch).await?;
                        }
                    }
                    counters.matched = u64::try_from(targets.len()).unwrap_or(u64::MAX);
                    counters.merged = counters.matched;
                    Ok((targets, counters))
                } else {
                    let id = provider_reserve_node_id_txn(txn, cell_id, identity.map(|id| id.0), next_node_id, request, budget).await?;
                    let properties = merge_properties(match_properties, on_create);
                    let metadata = provider_vertex_metadata(labels, &properties)?;
                    let targets = provider_single_target(budget, MutationTarget::Node(hydradb_graph_types::NodeId(id)))?;
                    apply_vertex_metadata_update_txn(txn, cell_id, id, None, &metadata, epoch, &mut WriteBackGenerations::default())
                        .await?;
                    counters.nodes_created = 1;
                    counters.merged = 1;
                    Ok((targets, counters))
                }
            }
            Op::MergeRelationship {
                source,
                destination,
                relationship_type,
                match_properties,
                on_create,
                on_match,
            } => {
                if let Some(record) = provider_find_relationship_txn(
                    txn,
                    cell_id,
                    ProviderRelationshipMatch {
                        source: source.0,
                        destination: destination.0,
                        relationship_type,
                        properties: match_properties,
                    },
                    request,
                    budget,
                )
                .await?
                {
                    let target = MutationTarget::Relationship(hydradb_graph_types::RelationshipId(record.relationship_id));
                    let targets = provider_single_target(budget, target)?;
                    for (key, value) in on_match {
                        provider_set_property_txn(self, txn, cell_id, target, key, value, epoch).await?;
                    }
                    counters.matched = 1;
                    counters.merged = 1;
                    Ok((targets, counters))
                } else {
                    let properties = merge_properties(match_properties, on_create);
                    // Relationship identity is determined by the matched endpoints/type.
                    let record = provider_create_relationship_txn(
                        self,
                        txn,
                        cell_id,
                        ProviderRelationshipCreate {
                            source: source.0,
                            destination: destination.0,
                            relationship_type,
                            properties: &properties,
                            epoch,
                        },
                    )
                    .await?;
                    let targets = provider_single_target(
                        budget,
                        MutationTarget::Relationship(hydradb_graph_types::RelationshipId(record.relationship_id)),
                    )?;
                    counters.relationships_created = 1;
                    counters.merged = 1;
                    Ok((targets, counters))
                }
            }
        }
    }
}

#[cfg(feature = "datafusion-cypher-engine")]
fn reject_provider_node_identity_property(target: hydradb_graph_provider::MutationTarget, key: &str) -> Result<()> {
    if matches!(target, hydradb_graph_provider::MutationTarget::Node(_)) && key == "id" {
        return Err(GraphError::UnsupportedQuery {
            reason: QueryFailureReason::Mutation,
            dialect: "Cypher25",
            feature: "graph identity `id` cannot be changed or removed".into(),
        });
    }
    Ok(())
}

#[cfg(feature = "datafusion-cypher-engine")]
fn reject_provider_node_identity_properties(properties: &hydradb_graph_provider::MutationProperties) -> Result<()> {
    if properties.contains_key("id") {
        return Err(GraphError::UnsupportedQuery {
            reason: QueryFailureReason::Mutation,
            dialect: "Cypher25",
            feature: "node properties cannot contain the reserved graph identity `id`".into(),
        });
    }
    Ok(())
}

#[cfg(feature = "datafusion-cypher-engine")]
async fn provider_reserve_node_id_txn(
    txn: &DbTransaction,
    cell_id: &str,
    identity: Option<VertexId>,
    next_node_id: &mut Option<VertexId>,
    request: &hydradb_graph_provider::MutationRequest,
    budget: &hydradb_graph_provider::BudgetTracker,
) -> Result<VertexId> {
    let next = match *next_node_id {
        Some(id) => id,
        None => provider_next_node_id_txn(txn, cell_id, request, budget).await?,
    };
    let id = identity.unwrap_or(next);
    if identity.is_some() {
        let key = keys::vertex(cell_id, id);
        if read_txn_remote(txn, &key).await?.is_some() {
            return Err(GraphError::UnsupportedQuery {
                reason: QueryFailureReason::Mutation,
                dialect: "Cypher25",
                feature: format!("node identity {id} already exists"),
            });
        }
    }
    let after_id = id.checked_add(1).ok_or_else(|| GraphError::CorruptValue {
        key: keys::vertex(cell_id, id),
        reason: "node identifier space exhausted".into(),
    })?;
    *next_node_id = Some(next.max(after_id));
    Ok(id)
}

#[cfg(feature = "datafusion-cypher-engine")]
fn provider_vertex_metadata(labels: &[String], properties: &hydradb_graph_provider::MutationProperties) -> Result<VertexMetadata> {
    let mut metadata = VertexMetadata {
        labels: labels.iter().cloned().collect(),
        properties: BTreeMap::new(),
    };
    for (key, value) in properties {
        if matches!(value, hydradb_graph_types::CypherValue::Null) {
            continue;
        }
        metadata.properties.insert(key.clone(), provider_property(value)?);
    }
    validate_vertex_metadata(&metadata)?;
    Ok(metadata)
}

#[cfg(feature = "datafusion-cypher-engine")]
pub fn provider_property(value: &hydradb_graph_types::CypherValue) -> Result<VertexPropertyValue> {
    crate::storage_property(value)
}

#[cfg(feature = "datafusion-cypher-engine")]
fn merge_properties(
    first: &hydradb_graph_provider::MutationProperties,
    second: &hydradb_graph_provider::MutationProperties,
) -> hydradb_graph_provider::MutationProperties {
    let mut result = first.clone();
    result.extend(second.iter().map(|(key, value)| (key.clone(), value.clone())));
    result
}

#[cfg(feature = "datafusion-cypher-engine")]
struct ProviderRelationshipCreate<'a> {
    source: VertexId,
    destination: VertexId,
    relationship_type: &'a str,
    properties: &'a hydradb_graph_provider::MutationProperties,
    epoch: StorageSequence,
}

#[cfg(feature = "datafusion-cypher-engine")]
async fn provider_create_relationship_txn(
    shard: &SlateGraphStore,
    txn: &DbTransaction,
    cell_id: &str,
    create: ProviderRelationshipCreate<'_>,
) -> Result<RelationshipRecord> {
    let ProviderRelationshipCreate {
        source,
        destination,
        relationship_type,
        properties,
        epoch,
    } = create;
    validate_component("edge_type", relationship_type)?;
    let mut metadata = EdgeMetadata::default();
    for (key, value) in properties {
        if !matches!(value, hydradb_graph_types::CypherValue::Null) {
            metadata.properties.insert(key.clone(), provider_property(value)?);
        }
    }
    validate_edge_metadata(&metadata)?;
    for id in [source, destination] {
        let key = keys::vertex(cell_id, id);
        if read_txn_remote(txn, &key).await?.is_none() {
            return Err(GraphError::UnsupportedQuery {
                reason: QueryFailureReason::Mutation,
                dialect: "Cypher25",
                feature: "CREATE relationship endpoints must already exist".into(),
            });
        }
    }
    let current = txn.seqnum();
    let relationship_id = next_relationship_id(read_counter_txn(txn, &keys::last_relationship_id(cell_id)).await?, cell_id)?;
    let structural_insert = edge_epoch_at_txn(txn, cell_id, relationship_type, source, destination, current)
        .await?
        .is_none();
    if structural_insert {
        let edge = EdgeRecord {
            cell_id: cell_id.to_owned(),
            edge_type: relationship_type.to_owned(),
            src: source,
            dst: destination,
        };
        let encoded = encode_edge_record(&edge);
        shard
            .mark_topology_change_txn(txn, cell_id, relationship_type, epoch, &[(source, destination, true)])
            .await?;
        let out_key = keys::degree_out(cell_id, relationship_type, source);
        txn.put(out_key.as_bytes(), encode_u64(read_counter_txn(txn, &out_key).await? + 1))?;
        if shard.writes_reverse_index() {
            let in_key = keys::degree_in(cell_id, relationship_type, destination);
            txn.put(in_key.as_bytes(), encode_u64(read_counter_txn(txn, &in_key).await? + 1))?;
            txn.put(keys::in_edge(cell_id, relationship_type, destination, source).as_bytes(), &encoded)?;
        }
        txn.put(keys::out_edge(cell_id, relationship_type, source, destination).as_bytes(), &encoded)?;
        txn.put(keys::edge(cell_id, relationship_type, source, destination).as_bytes(), &encoded)?;
    } else {
        // A property-only relationship addition still advances the topology
        // generation, but it does not change the adjacency xlog.
        shard.mark_topology_change_txn(txn, cell_id, relationship_type, epoch, &[]).await?;
    }
    txn.put(keys::last_relationship_id(cell_id).as_bytes(), encode_u64(relationship_id))?;
    let record = RelationshipRecord {
        cell_id: cell_id.to_owned(),
        edge_type: relationship_type.to_owned(),
        src: source,
        dst: destination,
        relationship_id,
        metadata,
    };
    let relationship_key = keys::relationship(cell_id, relationship_type, source, destination, relationship_id);
    txn.put(relationship_key.as_bytes(), encode_relationship_record(&record))?;
    txn.put(
        keys::relationship_id(cell_id, relationship_id).as_bytes(),
        relationship_key.as_bytes(),
    )?;
    let count_key = keys::relationship_count(cell_id, relationship_type, source, destination);
    txn.put(count_key.as_bytes(), encode_u64(read_counter_txn(txn, &count_key).await? + 1))?;
    put_relationship_property_indexes_txn(txn, &record)?;
    Ok(record)
}

#[cfg(feature = "datafusion-cypher-engine")]
async fn provider_next_node_id_txn(
    txn: &DbTransaction,
    cell_id: &str,
    request: &hydradb_graph_provider::MutationRequest,
    budget: &hydradb_graph_provider::BudgetTracker,
) -> Result<VertexId> {
    let prefix = format!("cell/{cell_id}/vertex/");
    let mut iter = txn.scan_prefix(prefix.as_bytes(), ..).await?;
    let mut maximum = 0_u64;
    while let Some(kv) = iter.next().await? {
        check_datafusion_scan_work(request, budget)?;
        let key = String::from_utf8_lossy(&kv.key);
        let id = key
            .strip_prefix(&prefix)
            .and_then(|suffix| suffix.parse::<u64>().ok())
            .ok_or_else(|| GraphError::CorruptValue {
                key: key.into_owned(),
                reason: "invalid node identifier in vertex key".into(),
            })?;
        maximum = maximum.max(id);
    }
    maximum.checked_add(1).ok_or_else(|| GraphError::CorruptValue {
        key: prefix,
        reason: "node identifier space exhausted".into(),
    })
}

#[cfg(feature = "datafusion-cypher-engine")]
async fn provider_set_property_txn(
    shard: &SlateGraphStore,
    txn: &DbTransaction,
    cell_id: &str,
    target: hydradb_graph_provider::MutationTarget,
    key: &str,
    value: &hydradb_graph_types::CypherValue,
    epoch: StorageSequence,
) -> Result<bool> {
    validate_component("property", key)?;
    match target {
        hydradb_graph_provider::MutationTarget::Node(node) => {
            let record_key = keys::vertex(cell_id, node.0);
            let Some(previous) = decode_vertex_record(&record_key, read_txn_remote(txn, &record_key).await?.as_ref())? else {
                return Ok(false);
            };
            // The caller refuses `key` when it is a write-back property, so
            // the record's copy of `key` is current and decides the no-op.
            let requested = if matches!(value, hydradb_graph_types::CypherValue::Null) {
                None
            } else {
                Some(provider_property(value)?)
            };
            if previous.properties.get(key) == requested.as_ref() {
                return Ok(false);
            }
            let mut generations = WriteBackGenerations::default();
            let mut next = write_back_patch_base_txn(txn, cell_id, node.0, &previous, &mut generations)
                .await?
                .into_owned();
            match requested {
                Some(value) => next.properties.insert(key.to_owned(), value),
                None => next.properties.remove(key),
            };
            apply_vertex_metadata_update_txn(txn, cell_id, node.0, Some(&previous), &next, epoch, &mut generations).await?;
            Ok(true)
        }
        hydradb_graph_provider::MutationTarget::Relationship(relationship_id) => {
            let id_key = keys::relationship_id(cell_id, relationship_id.0);
            let Some(record_key_bytes) = read_txn_remote(txn, &id_key).await? else {
                return Ok(false);
            };
            let record_key = std::str::from_utf8(&record_key_bytes)
                .map_err(|error| GraphError::CorruptValue {
                    key: id_key,
                    reason: error.to_string(),
                })?
                .to_owned();
            let Some(bytes) = read_txn_remote(txn, &record_key).await? else {
                return Ok(false);
            };
            let mut record = decode_relationship_record(&record_key, &bytes)?;
            let previous = record.metadata.clone();
            if matches!(value, hydradb_graph_types::CypherValue::Null) {
                record.metadata.properties.remove(key);
            } else {
                record.metadata.properties.insert(key.to_owned(), provider_property(value)?);
            }
            if record.metadata == previous {
                return Ok(false);
            }
            // Check the whole relationship, not just the property being set,
            // as the node branch does through `apply_vertex_metadata_update_txn`.
            // Otherwise a list stored before a lower `MAX_PROPERTY_LIST_ELEMENTS`
            // would be saved again by an update to an unrelated property.
            validate_edge_metadata(&record.metadata)?;
            delete_relationship_property_indexes_txn(txn, &record, &previous)?;
            put_relationship_property_indexes_txn(txn, &record)?;
            txn.put(record_key.as_bytes(), encode_relationship_record(&record))?;
            shard.mark_topology_change_txn(txn, cell_id, &record.edge_type, epoch, &[]).await?;
            Ok(true)
        }
    }
}

#[cfg(feature = "datafusion-cypher-engine")]
async fn provider_delete_relationship_txn(
    shard: &SlateGraphStore,
    txn: &DbTransaction,
    cell_id: &str,
    id: RelationshipId,
    epoch: StorageSequence,
    request: &hydradb_graph_provider::MutationRequest,
    budget: &hydradb_graph_provider::BudgetTracker,
) -> Result<bool> {
    let id_key = keys::relationship_id(cell_id, id);
    let Some(record_key_bytes) = read_txn_remote(txn, &id_key).await? else {
        return Ok(false);
    };
    let record_key = std::str::from_utf8(&record_key_bytes)
        .map_err(|error| GraphError::CorruptValue {
            key: id_key.clone(),
            reason: error.to_string(),
        })?
        .to_owned();
    let Some(bytes) = read_txn_remote(txn, &record_key).await? else {
        return Ok(false);
    };
    let record = decode_relationship_record(&record_key, &bytes)?;
    let prefix = keys::relationship_edge_prefix(cell_id, &record.edge_type, record.src, record.dst);
    let mut iter = txn.scan_prefix(prefix.as_bytes(), ..).await?;
    let mut other_live = false;
    while let Some(kv) = iter.next().await? {
        check_datafusion_scan_work(request, budget)?;
        let key = String::from_utf8_lossy(&kv.key).into_owned();
        let current = decode_relationship_record(&key, &kv.value)?;
        if current.relationship_id != id {
            other_live = true;
            break;
        }
    }
    txn.delete(record_key.as_bytes())?;
    txn.delete(id_key.as_bytes())?;
    delete_relationship_property_indexes_txn(txn, &record, &record.metadata)?;
    let count_key = keys::relationship_count(cell_id, &record.edge_type, record.src, record.dst);
    let count = read_counter_txn(txn, &count_key).await?.saturating_sub(1);
    if count == 0 {
        txn.delete(count_key.as_bytes())?;
    } else {
        txn.put(count_key.as_bytes(), encode_u64(count))?;
    }
    if !other_live {
        let mutation = EdgeMutation {
            cell_id: cell_id.to_owned(),
            edge_type: record.edge_type.clone(),
            src: record.src,
            dst: record.dst,
            idempotency_key: String::new(),
        };
        delete_structural_edge_txn(
            shard.writes_reverse_index(),
            &shard.mutation_state().xlog_floor_ensured,
            txn,
            &mutation,
            epoch,
        )
        .await?;
    }
    Ok(true)
}

#[cfg(feature = "datafusion-cypher-engine")]
async fn provider_delete_isolated_node_txn(
    txn: &DbTransaction,
    cell_id: &str,
    id: VertexId,
    request: &hydradb_graph_provider::MutationRequest,
    budget: &hydradb_graph_provider::BudgetTracker,
) -> Result<bool> {
    let node_key = keys::vertex(cell_id, id);
    let Some(bytes) = read_txn_remote(txn, &node_key).await? else {
        return Ok(false);
    };
    let metadata = decode_vertex_metadata(&node_key, &bytes)?;
    for (prefix, segments) in [
        (keys::out_edge_cell_prefix(cell_id), false),
        (keys::out_segment_cell_prefix(cell_id), true),
    ] {
        let mut iter = txn.scan_prefix(prefix.as_bytes(), ..).await?;
        while let Some(kv) = iter.next().await? {
            check_datafusion_scan_work(request, budget)?;
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            if segments {
                let segment = decode_out_edge_segment(&key, &kv.value)?;
                if segment.src == id || segment.destinations.contains(&id) {
                    return Err(GraphError::UnsupportedQuery {
                        reason: QueryFailureReason::Mutation,
                        dialect: "Cypher25",
                        feature: "cannot delete a node with incident relationships".into(),
                    });
                }
            } else {
                let edge = decode_edge_record(&key, &kv.value)?;
                if edge.src == id || edge.dst == id {
                    return Err(GraphError::UnsupportedQuery {
                        reason: QueryFailureReason::Mutation,
                        dialect: "Cypher25",
                        feature: "cannot delete a node with incident relationships".into(),
                    });
                }
            }
        }
    }
    let mut rels = txn.scan_prefix(keys::relationship_cell_prefix(cell_id).as_bytes(), ..).await?;
    while let Some(kv) = rels.next().await? {
        check_datafusion_scan_work(request, budget)?;
        let key = String::from_utf8_lossy(&kv.key).into_owned();
        let record = decode_relationship_record(&key, &kv.value)?;
        if record.src == id || record.dst == id {
            return Err(GraphError::UnsupportedQuery {
                reason: QueryFailureReason::Mutation,
                dialect: "Cypher25",
                feature: "cannot delete a node with incident relationships".into(),
            });
        }
    }
    delete_vertex_record_txn(txn, cell_id, id, &metadata, &mut WriteBackGenerations::default()).await?;
    Ok(true)
}

#[cfg(feature = "datafusion-cypher-engine")]
async fn provider_find_node_identity_txn(
    txn: &DbTransaction,
    cell_id: &str,
    identity: hydradb_graph_types::NodeId,
    labels: &[String],
    properties: &hydradb_graph_provider::MutationProperties,
    budget: &hydradb_graph_provider::BudgetTracker,
) -> Result<Option<std::sync::Arc<Vec<hydradb_graph_provider::MutationTarget>>>> {
    use hydradb_graph_provider::MutationTarget;

    let key = keys::vertex(cell_id, identity.0);
    let Some(metadata) = decode_vertex_record(&key, read_txn_remote(txn, &key).await?.as_ref())? else {
        return Ok(None);
    };
    if !labels.iter().all(|label| metadata.labels.contains(label)) || !provider_properties_match(&metadata.properties, properties) {
        return Err(GraphError::UnsupportedQuery {
            reason: QueryFailureReason::Mutation,
            dialect: "Cypher25",
            feature: format!("node identity {} exists but does not satisfy the MERGE pattern", identity.0),
        });
    }
    Ok(Some(provider_single_target(budget, MutationTarget::Node(identity))?))
}

#[cfg(feature = "datafusion-cypher-engine")]
async fn provider_find_node_txn(
    txn: &DbTransaction,
    cell_id: &str,
    labels: &[String],
    properties: &hydradb_graph_provider::MutationProperties,
    request: &hydradb_graph_provider::MutationRequest,
    budget: &hydradb_graph_provider::BudgetTracker,
) -> Result<Option<std::sync::Arc<Vec<hydradb_graph_provider::MutationTarget>>>> {
    use hydradb_graph_provider::MutationTarget;

    let prefix = format!("cell/{cell_id}/vertex/");
    let mut iter = txn.scan_prefix(prefix.as_bytes(), ..).await?;
    let mut targets = Vec::new();
    while let Some(kv) = iter.next().await? {
        check_datafusion_scan_work(request, budget)?;
        let key = String::from_utf8_lossy(&kv.key).into_owned();
        let id = key
            .strip_prefix(&prefix)
            .and_then(|suffix| suffix.parse::<u64>().ok())
            .ok_or_else(|| GraphError::CorruptValue {
                key: key.clone(),
                reason: "invalid node identifier in vertex key".into(),
            })?;
        let metadata = decode_vertex_metadata(&key, &kv.value)?;
        if labels.iter().all(|label| metadata.labels.contains(label)) && provider_properties_match(&metadata.properties, properties) {
            let required_capacity = targets.len().checked_add(1).ok_or_else(|| GraphError::CorruptValue {
                key: prefix.clone(),
                reason: "MERGE target capacity overflow".into(),
            })?;
            let current_capacity = targets.capacity();
            let target_capacity = if required_capacity > current_capacity {
                current_capacity.checked_mul(2).unwrap_or(required_capacity).max(required_capacity)
            } else {
                current_capacity
            };
            let capacity_bytes = if target_capacity > current_capacity {
                if current_capacity == 0 {
                    provider_target_storage_bytes(target_capacity)
                } else {
                    provider_target_storage_bytes(target_capacity).saturating_sub(provider_target_storage_bytes(current_capacity))
                }
            } else {
                0
            };
            budget
                .consume_retained(u64::from(!targets.is_empty()), capacity_bytes)
                .map_err(GraphError::DataFusionProvider)?;
            if target_capacity > current_capacity {
                targets
                    .try_reserve_exact(target_capacity - targets.len())
                    .map_err(|error| GraphError::CorruptValue {
                        key: prefix.clone(),
                        reason: format!("unable to reserve MERGE targets: {error}"),
                    })?;
            }
            targets.push(MutationTarget::Node(hydradb_graph_types::NodeId(id)));
        }
    }
    if targets.is_empty() {
        return Ok(None);
    }
    targets.sort_unstable_by_key(|target| match target {
        MutationTarget::Node(id) => id.0,
        MutationTarget::Relationship(id) => id.0,
    });
    Ok(Some(std::sync::Arc::new(targets)))
}

#[cfg(feature = "datafusion-cypher-engine")]
fn provider_target_storage_bytes(target_count: usize) -> u64 {
    u64::try_from(target_count)
        .unwrap_or(u64::MAX)
        .saturating_mul(std::mem::size_of::<hydradb_graph_provider::MutationTarget>() as u64)
        .saturating_add(std::mem::size_of::<Vec<hydradb_graph_provider::MutationTarget>>() as u64)
        .saturating_add((2 * std::mem::size_of::<usize>()) as u64)
}

#[cfg(feature = "datafusion-cypher-engine")]
fn provider_single_target(
    budget: &hydradb_graph_provider::BudgetTracker,
    target: hydradb_graph_provider::MutationTarget,
) -> Result<std::sync::Arc<Vec<hydradb_graph_provider::MutationTarget>>> {
    budget
        .consume_retained_bytes(provider_target_storage_bytes(1))
        .map_err(GraphError::DataFusionProvider)?;
    let mut targets = Vec::new();
    targets.try_reserve_exact(1).map_err(|error| GraphError::CorruptValue {
        key: "mutation_target".into(),
        reason: format!("unable to reserve mutation target: {error}"),
    })?;
    targets.push(target);
    Ok(std::sync::Arc::new(targets))
}

#[cfg(feature = "datafusion-cypher-engine")]
fn provider_empty_targets(
    budget: &hydradb_graph_provider::BudgetTracker,
) -> Result<std::sync::Arc<Vec<hydradb_graph_provider::MutationTarget>>> {
    budget
        .consume_retained_bytes(provider_target_storage_bytes(0))
        .map_err(GraphError::DataFusionProvider)?;
    Ok(std::sync::Arc::new(Vec::new()))
}

#[cfg(feature = "datafusion-cypher-engine")]
struct ProviderRelationshipMatch<'a> {
    source: VertexId,
    destination: VertexId,
    relationship_type: &'a str,
    properties: &'a hydradb_graph_provider::MutationProperties,
}

#[cfg(feature = "datafusion-cypher-engine")]
async fn provider_find_relationship_txn(
    txn: &DbTransaction,
    cell_id: &str,
    relationship: ProviderRelationshipMatch<'_>,
    request: &hydradb_graph_provider::MutationRequest,
    budget: &hydradb_graph_provider::BudgetTracker,
) -> Result<Option<RelationshipRecord>> {
    let ProviderRelationshipMatch {
        source,
        destination,
        relationship_type,
        properties,
    } = relationship;
    let mut iter = txn.scan_prefix(keys::relationship_cell_prefix(cell_id).as_bytes(), ..).await?;
    while let Some(kv) = iter.next().await? {
        check_datafusion_scan_work(request, budget)?;
        let key = String::from_utf8_lossy(&kv.key).into_owned();
        let record = decode_relationship_record(&key, &kv.value)?;
        if record.src == source
            && record.dst == destination
            && record.edge_type == relationship_type
            && provider_properties_match(&record.metadata.properties, properties)
        {
            return Ok(Some(record));
        }
    }
    Ok(None)
}

#[cfg(feature = "datafusion-cypher-engine")]
fn check_datafusion_scan_work(
    request: &hydradb_graph_provider::MutationRequest,
    budget: &hydradb_graph_provider::BudgetTracker,
) -> Result<()> {
    request.validate().map_err(GraphError::DataFusionProvider)?;
    budget.consume_work_units(1).map_err(GraphError::DataFusionProvider)
}

#[cfg(feature = "datafusion-cypher-engine")]
fn provider_properties_match(
    current: &BTreeMap<String, VertexPropertyValue>,
    expected: &hydradb_graph_provider::MutationProperties,
) -> bool {
    use hydradb_graph_types::semantic_eq;

    expected.iter().all(|(key, expected)| {
        let Some(current) = current.get(key) else {
            return false;
        };
        let current = crate::property_to_cypher(current);
        semantic_eq(&current, expected)
    })
}
