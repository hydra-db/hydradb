use std::collections::BTreeMap;
#[cfg(feature = "opencypher")]
use std::sync::Arc;

#[cfg(feature = "opencypher")]
use crate::{EdgeMetadata, GraphScope, QueryResultSet, RelationshipId, VertexId, VertexPropertyValue};
use crate::{GraphCacheMetrics, StorageSequence};

#[cfg(feature = "opencypher")]
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) struct NativePathResultCacheKey {
    pub(crate) scope: GraphScope,
    pub(crate) cell_id: String,
    pub(crate) procedure: String,
    pub(crate) read_epoch: StorageSequence,
    pub(crate) max_result_bytes: Option<u64>,
}

#[cfg(feature = "opencypher")]
impl NativePathResultCacheKey {
    pub(crate) fn estimated_resident_bytes(&self) -> usize {
        std::mem::size_of::<Self>()
            .saturating_add(self.cell_id.capacity())
            .saturating_add(self.procedure.capacity())
    }
}

#[cfg(feature = "opencypher")]
pub(crate) type NativePathResultCacheValue = Arc<QueryResultSet>;

/// What makes an assembled gds graph reusable: the same named projection
/// over the same published generations minus the same deleted pairs, with
/// the same label members. Everything in it is read from storage at the
/// call's snapshot, so a node that has the graph cached and one that
/// assembles it compute the same key and serve the same graph.
///
/// The read epoch is deliberately not part of it. A write-back advances the
/// epoch, and the algorithm calls that follow one project must still find
/// the graph; what the epoch changes about the graph is the tail's
/// deletions and the label members, which are keyed directly, the members
/// by the cell's label membership version. A write-back writes neither.
#[cfg(feature = "opencypher")]
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) struct GdsProjectedGraphCacheKey {
    pub(crate) scope: GraphScope,
    pub(crate) cell_id: String,
    /// The projection's name: two projections of one shape are two
    /// catalog entries, and one's project must not replace the other's
    /// graph.
    pub(crate) graph_name: String,
    /// The node projection and the resolved relationship projections.
    pub(crate) projection: String,
    /// The cell's label membership version
    /// ([`crate::keys::vertex_label_version`]) for a label projection;
    /// `None` for `'*'`, whose vertices come from the edges alone, and for
    /// a cell whose labels never changed on this layout.
    pub(crate) label_version: Option<StorageSequence>,
    /// Each projected type's generation.
    pub(crate) generations: Vec<(String, StorageSequence)>,
    /// Per generation, the pairs the WAL tail deleted from it, sorted.
    pub(crate) deleted: Vec<Vec<(VertexId, VertexId)>>,
}

#[cfg(feature = "opencypher")]
impl GdsProjectedGraphCacheKey {
    pub(crate) fn estimated_resident_bytes(&self) -> usize {
        let generations = self.generations.iter().fold(
            self.generations
                .capacity()
                .saturating_mul(std::mem::size_of::<(String, StorageSequence)>()),
            |bytes, (edge_type, _)| bytes.saturating_add(edge_type.capacity()),
        );
        let deleted = self.deleted.iter().fold(
            self.deleted
                .capacity()
                .saturating_mul(std::mem::size_of::<Vec<(VertexId, VertexId)>>()),
            |bytes, pairs| bytes.saturating_add(pairs.capacity().saturating_mul(std::mem::size_of::<(VertexId, VertexId)>())),
        );
        std::mem::size_of::<Self>()
            .saturating_add(self.cell_id.capacity())
            .saturating_add(self.graph_name.capacity())
            .saturating_add(self.projection.capacity())
            .saturating_add(generations)
            .saturating_add(deleted)
    }
}

/// A gds projection assembled for the kernels: the projected vertices in
/// ordinal order and the edges over those ordinals. Shared by `Arc` so the
/// compute closures borrow it on the blocking thread without a copy.
///
/// Resident per vertex: 8 bytes, the id. Per edge: 16 bytes, the two
/// ordinals of the COO. A score vector adds 8 bytes per vertex for as long
/// as one call holds it.
#[cfg(feature = "opencypher")]
#[derive(Debug)]
pub(crate) struct GdsProjectedGraph {
    pub(crate) vertices: Vec<VertexId>,
    pub(crate) edges: crate::sparse_kernel::OrdinalEdges,
    /// Generation edges left out because the WAL tail deleted them.
    pub(crate) deleted_pairs_dropped: u64,
    /// [`Self::relationship_count`], counted on first use. Counting sorts a copy
    /// of every pair, so it is done once per assembled graph and every call
    /// that finds the graph cached reads the same number.
    pub(crate) distinct_pairs: std::sync::OnceLock<u64>,
}

#[cfg(feature = "opencypher")]
impl GdsProjectedGraph {
    /// The edges the kernels compute on: pairs that two projected types, or
    /// both orientations of an undirected projection, share count once.
    pub(crate) fn relationship_count(&self) -> u64 {
        *self.distinct_pairs.get_or_init(|| self.edges.distinct_pairs())
    }

    /// The count once something has counted it, without counting: the first
    /// count sorts a copy of every pair, which belongs on the blocking pool.
    pub(crate) fn counted_relationships(&self) -> Option<u64> {
        self.distinct_pairs.get().copied()
    }

    pub(crate) fn estimated_resident_bytes(&self) -> usize {
        std::mem::size_of::<Self>()
            .saturating_add(self.vertices.capacity().saturating_mul(std::mem::size_of::<VertexId>()))
            .saturating_add(self.edges.sources.capacity().saturating_mul(std::mem::size_of::<u64>()))
            .saturating_add(self.edges.targets.capacity().saturating_mul(std::mem::size_of::<u64>()))
    }
}

/// Entries in the projected graph cache; the byte limit is the bound that
/// matters, since one entry is a whole graph.
#[cfg(feature = "opencypher")]
pub(crate) const MAX_GDS_PROJECTED_GRAPHS: usize = 16;
#[cfg(feature = "opencypher")]
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) struct SourceRelationshipRowsCacheKey {
    pub(crate) cell_id: String,
    pub(crate) edge_type: String,
    pub(crate) src: VertexId,
    pub(crate) hop_range: Option<(u8, u8)>,
    pub(crate) read_epoch: StorageSequence,
}

#[cfg(feature = "opencypher")]
impl SourceRelationshipRowsCacheKey {
    pub(crate) fn new(cell_id: &str, edge_type: &str, src: VertexId, read_epoch: StorageSequence) -> Self {
        Self {
            cell_id: cell_id.to_string(),
            edge_type: edge_type.to_string(),
            src,
            hop_range: None,
            read_epoch,
        }
    }

    pub(crate) fn reachable(cell_id: &str, edge_type: &str, src: VertexId, hop_range: (u8, u8), read_epoch: StorageSequence) -> Self {
        Self {
            cell_id: cell_id.to_string(),
            edge_type: edge_type.to_string(),
            src,
            hop_range: Some(hop_range),
            read_epoch,
        }
    }

    pub(crate) fn estimated_resident_bytes(&self) -> usize {
        std::mem::size_of::<Self>()
            .saturating_add(self.cell_id.capacity())
            .saturating_add(self.edge_type.capacity())
    }
}

#[cfg(feature = "opencypher")]
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) struct RelationshipPropertyRowsCacheKey {
    pub(crate) cell_id: String,
    pub(crate) edge_type: String,
    pub(crate) src: VertexId,
    pub(crate) dst: VertexId,
    pub(crate) property: String,
    pub(crate) encoded_value: String,
    pub(crate) read_epoch: StorageSequence,
}

#[cfg(feature = "opencypher")]
impl RelationshipPropertyRowsCacheKey {
    pub(crate) fn new(
        cell_id: &str,
        edge_type: &str,
        src: VertexId,
        dst: VertexId,
        property: &str,
        encoded_value: &str,
        read_epoch: StorageSequence,
    ) -> Self {
        Self {
            cell_id: cell_id.to_string(),
            edge_type: edge_type.to_string(),
            src,
            dst,
            property: property.to_string(),
            encoded_value: encoded_value.to_string(),
            read_epoch,
        }
    }

    pub(crate) fn estimated_resident_bytes(&self) -> usize {
        std::mem::size_of::<Self>()
            .saturating_add(self.cell_id.capacity())
            .saturating_add(self.edge_type.capacity())
            .saturating_add(self.property.capacity())
            .saturating_add(self.encoded_value.capacity())
    }
}

#[cfg(feature = "opencypher")]
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RelationshipRowsCacheEntry {
    pub(crate) relationship_id: Option<RelationshipId>,
    pub(crate) metadata: EdgeMetadata,
}

#[cfg(feature = "opencypher")]
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RelationshipRowsCacheValue {
    pub(crate) rows: Arc<Vec<RelationshipRowsCacheEntry>>,
}

#[cfg(feature = "opencypher")]
impl RelationshipRowsCacheValue {
    pub(crate) fn new(rows: Vec<RelationshipRowsCacheEntry>) -> Self {
        Self { rows: Arc::new(rows) }
    }

    pub(crate) fn estimated_resident_bytes(&self) -> usize {
        let allocation = std::mem::size_of::<Vec<RelationshipRowsCacheEntry>>()
            .saturating_add(
                self.rows
                    .capacity()
                    .saturating_mul(std::mem::size_of::<RelationshipRowsCacheEntry>()),
            )
            .saturating_add(2 * std::mem::size_of::<usize>());
        self.rows
            .iter()
            .fold(std::mem::size_of::<Self>().saturating_add(allocation), |bytes, row| {
                bytes.saturating_add(edge_metadata_heap_bytes(&row.metadata))
            })
    }
}

#[cfg(feature = "opencypher")]
pub(crate) fn source_relationship_rows_resident_bytes(key: &SourceRelationshipRowsCacheKey, rows: &Arc<Vec<VertexId>>) -> usize {
    key.estimated_resident_bytes()
        .saturating_add(std::mem::size_of::<Arc<Vec<VertexId>>>())
        .saturating_add(2 * std::mem::size_of::<usize>())
        .saturating_add(std::mem::size_of::<Vec<VertexId>>())
        .saturating_add(rows.capacity().saturating_mul(std::mem::size_of::<VertexId>()))
}

#[cfg(feature = "opencypher")]
fn edge_metadata_heap_bytes(metadata: &EdgeMetadata) -> usize {
    const BTREE_ENTRY_OVERHEAD_WORDS: usize = 4;
    metadata.properties.iter().fold(0, |bytes, (name, value)| {
        let value_heap_bytes = value.estimated_heap_bytes();
        bytes
            .saturating_add(std::mem::size_of::<String>())
            .saturating_add(std::mem::size_of::<VertexPropertyValue>())
            .saturating_add(BTREE_ENTRY_OVERHEAD_WORDS * std::mem::size_of::<usize>())
            .saturating_add(name.capacity())
            .saturating_add(value_heap_bytes)
    })
}

struct CacheEntry<V> {
    value: V,
    tenant: String,
    pinned: bool,
    last_access: u64,
    resident_bytes: usize,
}

pub(crate) struct BoundedGraphCache<K, V> {
    max_entries: usize,
    max_resident_bytes: usize,
    resident_bytes: usize,
    max_entries_per_tenant: Option<usize>,
    clock: u64,
    entries: BTreeMap<K, CacheEntry<V>>,
    tenant_entries: BTreeMap<String, usize>,
}

impl<K, V> BoundedGraphCache<K, V>
where
    K: Clone + Ord,
    V: Clone,
{
    pub(crate) fn new(max_entries: usize, max_entries_per_tenant: Option<usize>) -> Self {
        Self::new_with_byte_limit(max_entries, max_entries_per_tenant, usize::MAX)
    }

    pub(crate) fn new_with_byte_limit(max_entries: usize, max_entries_per_tenant: Option<usize>, max_resident_bytes: usize) -> Self {
        Self {
            max_entries,
            max_resident_bytes,
            resident_bytes: 0,
            max_entries_per_tenant,
            clock: 0,
            entries: BTreeMap::new(),
            tenant_entries: BTreeMap::new(),
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.entries.len()
    }

    pub(crate) fn resident_bytes(&self) -> usize {
        self.resident_bytes
    }

    pub(crate) fn get(&mut self, key: &K) -> Option<V> {
        self.clock = self.clock.saturating_add(1);
        let entry = self.entries.get_mut(key)?;
        entry.last_access = self.clock;
        Some(entry.value.clone())
    }

    pub(crate) fn get_latest_by(
        &mut self,
        mut predicate: impl FnMut(&K, &V) -> bool,
        mut score: impl FnMut(&K, &V) -> StorageSequence,
    ) -> Option<V> {
        let key = self
            .entries
            .iter()
            .filter(|(key, entry)| predicate(key, &entry.value))
            .max_by_key(|(key, entry)| score(key, &entry.value))
            .map(|(key, _)| key.clone())?;
        self.get(&key)
    }

    pub(crate) fn insert(&mut self, key: K, value: V, tenant: impl Into<String>, pinned: bool, metrics: &GraphCacheMetrics) -> Option<V> {
        self.insert_sized(key, value, tenant, pinned, 0, metrics)
    }

    pub(crate) fn insert_sized(
        &mut self,
        key: K,
        value: V,
        tenant: impl Into<String>,
        pinned: bool,
        resident_bytes: usize,
        metrics: &GraphCacheMetrics,
    ) -> Option<V> {
        if self.max_entries == 0 {
            metrics.record_quota_rejection();
            return None;
        }

        self.clock = self.clock.saturating_add(1);
        let tenant = tenant.into();
        let previous = self.entries.insert(
            key.clone(),
            CacheEntry {
                value,
                tenant: tenant.clone(),
                pinned,
                last_access: self.clock,
                resident_bytes,
            },
        );
        if let Some(previous) = previous {
            self.resident_bytes = self.resident_bytes.saturating_sub(previous.resident_bytes);
            self.decrement_tenant(&previous.tenant);
        }
        self.resident_bytes = self.resident_bytes.saturating_add(resident_bytes);
        *self.tenant_entries.entry(tenant.clone()).or_default() += 1;
        metrics.record_insertion(pinned);

        self.enforce_tenant_quota(&tenant, metrics);
        self.enforce_total_limit(metrics);
        self.get(&key)
    }

    pub(crate) fn retain(&mut self, mut keep: impl FnMut(&K, &V) -> bool) {
        let removed: Vec<_> = self
            .entries
            .iter()
            .filter_map(|(key, entry)| (!keep(key, &entry.value)).then_some(key.clone()))
            .collect();
        for key in removed {
            self.remove(&key);
        }
    }

    fn enforce_tenant_quota(&mut self, tenant: &str, metrics: &GraphCacheMetrics) {
        let Some(limit) = self.max_entries_per_tenant else {
            return;
        };
        while self.tenant_entries.get(tenant).copied().unwrap_or(0) > limit {
            if self.evict_one(Some(tenant), false, metrics).is_none() && self.evict_one(Some(tenant), true, metrics).is_none() {
                metrics.record_quota_rejection();
                break;
            }
        }
    }

    fn enforce_total_limit(&mut self, metrics: &GraphCacheMetrics) {
        while self.entries.len() > self.max_entries || self.resident_bytes > self.max_resident_bytes {
            if self.evict_one(None, false, metrics).is_none() && self.evict_one(None, true, metrics).is_none() {
                break;
            }
        }
    }

    fn evict_one(&mut self, tenant: Option<&str>, allow_pinned: bool, metrics: &GraphCacheMetrics) -> Option<()> {
        let key = self
            .entries
            .iter()
            .filter(|(_, entry)| {
                (match tenant {
                    Some(tenant) => tenant == entry.tenant,
                    None => true,
                }) && (allow_pinned || !entry.pinned)
            })
            .min_by_key(|(_, entry)| entry.last_access)
            .map(|(key, _)| key.clone())?;
        self.remove(&key);
        metrics.record_eviction();
        Some(())
    }

    pub(crate) fn remove(&mut self, key: &K) -> Option<V> {
        let entry = self.entries.remove(key)?;
        self.resident_bytes = self.resident_bytes.saturating_sub(entry.resident_bytes);
        self.decrement_tenant(&entry.tenant);
        Some(entry.value)
    }

    fn decrement_tenant(&mut self, tenant: &str) {
        if let Some(count) = self.tenant_entries.get_mut(tenant) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                self.tenant_entries.remove(tenant);
            }
        }
    }
}
