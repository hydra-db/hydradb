use std::collections::BTreeMap;
#[cfg(feature = "opencypher")]
use std::sync::Arc;

#[cfg(feature = "opencypher")]
use crate::{
    EdgeMetadata, GraphScope, QueryResultSet, RelationshipId, VertexId, VertexPropertyValue,
};
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
#[cfg(feature = "opencypher")]
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) struct RelationshipRowsCacheKey {
    pub(crate) cell_id: String,
    pub(crate) edge_type: String,
    pub(crate) src: VertexId,
    pub(crate) dst: VertexId,
    pub(crate) read_epoch: StorageSequence,
}

#[cfg(feature = "opencypher")]
impl RelationshipRowsCacheKey {
    pub(crate) fn new(
        cell_id: &str,
        edge_type: &str,
        src: VertexId,
        dst: VertexId,
        read_epoch: StorageSequence,
    ) -> Self {
        Self {
            cell_id: cell_id.to_string(),
            edge_type: edge_type.to_string(),
            src,
            dst,
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
pub(crate) struct SourceRelationshipRowsCacheKey {
    pub(crate) cell_id: String,
    pub(crate) edge_type: String,
    pub(crate) src: VertexId,
    pub(crate) hop_range: Option<(u8, u8)>,
    pub(crate) read_epoch: StorageSequence,
}

#[cfg(feature = "opencypher")]
impl SourceRelationshipRowsCacheKey {
    pub(crate) fn new(
        cell_id: &str,
        edge_type: &str,
        src: VertexId,
        read_epoch: StorageSequence,
    ) -> Self {
        Self {
            cell_id: cell_id.to_string(),
            edge_type: edge_type.to_string(),
            src,
            hop_range: None,
            read_epoch,
        }
    }

    pub(crate) fn reachable(
        cell_id: &str,
        edge_type: &str,
        src: VertexId,
        hop_range: (u8, u8),
        read_epoch: StorageSequence,
    ) -> Self {
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
        Self {
            rows: Arc::new(rows),
        }
    }

    pub(crate) fn estimated_resident_bytes(&self) -> usize {
        let allocation = std::mem::size_of::<Vec<RelationshipRowsCacheEntry>>()
            .saturating_add(
                self.rows
                    .capacity()
                    .saturating_mul(std::mem::size_of::<RelationshipRowsCacheEntry>()),
            )
            .saturating_add(2 * std::mem::size_of::<usize>());
        self.rows.iter().fold(
            std::mem::size_of::<Self>().saturating_add(allocation),
            |bytes, row| bytes.saturating_add(edge_metadata_heap_bytes(&row.metadata)),
        )
    }
}

#[cfg(feature = "opencypher")]
pub(crate) fn source_relationship_rows_resident_bytes(
    key: &SourceRelationshipRowsCacheKey,
    rows: &Arc<Vec<VertexId>>,
) -> usize {
    key.estimated_resident_bytes()
        .saturating_add(std::mem::size_of::<Arc<Vec<VertexId>>>())
        .saturating_add(2 * std::mem::size_of::<usize>())
        .saturating_add(std::mem::size_of::<Vec<VertexId>>())
        .saturating_add(
            rows.capacity()
                .saturating_mul(std::mem::size_of::<VertexId>()),
        )
}

#[cfg(feature = "opencypher")]
fn edge_metadata_heap_bytes(metadata: &EdgeMetadata) -> usize {
    const BTREE_ENTRY_OVERHEAD_WORDS: usize = 4;
    metadata.properties.iter().fold(0, |bytes, (name, value)| {
        let value_heap_bytes = match value {
            VertexPropertyValue::String(value) => value.capacity(),
            VertexPropertyValue::Integer(_)
            | VertexPropertyValue::SignedInteger(_)
            | VertexPropertyValue::Bool(_)
            | VertexPropertyValue::Float(_) => 0,
        };
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

    /// `max_entries` and `max_resident_bytes` are both caps, and both spell
    /// "off" as zero: zero is the off value for every cap in the kernel
    /// (`max_entries` here, and the `GraphCachePolicy` and `GraphMemoryConfig`
    /// entries that reach this constructor).
    pub(crate) fn new_with_byte_limit(
        max_entries: usize,
        max_entries_per_tenant: Option<usize>,
        max_resident_bytes: usize,
    ) -> Self {
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

    pub(crate) fn insert(
        &mut self,
        key: K,
        value: V,
        tenant: impl Into<String>,
        pinned: bool,
        metrics: &GraphCacheMetrics,
    ) -> Option<V> {
        self.insert_sized(key, value, tenant, pinned, 0, metrics)
    }

    /// Zero on either limit means the cache is off: the insert is refused and
    /// the caller is left to use the value it just built.
    ///
    /// That refusal has to happen here, before the entry goes in, and not
    /// through [`Self::enforce_total_limit`]. The entry is inserted and counted
    /// *first* and the limits are enforced afterwards, so a byte budget read as
    /// a literal zero could only ever be satisfied by an empty cache: it would
    /// evict every entry held -- pinned ones included, then the entry it was
    /// asked to store -- on every insert, and still report that insert as a
    /// miss. A zero budget asks for no bytes to be cached, not for the cache to
    /// be emptied one insert at a time.
    pub(crate) fn insert_sized(
        &mut self,
        key: K,
        value: V,
        tenant: impl Into<String>,
        pinned: bool,
        resident_bytes: usize,
        metrics: &GraphCacheMetrics,
    ) -> Option<V> {
        if self.max_entries == 0 || self.max_resident_bytes == 0 {
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
            if self.evict_one(Some(tenant), false, metrics).is_none()
                && self.evict_one(Some(tenant), true, metrics).is_none()
            {
                metrics.record_quota_rejection();
                break;
            }
        }
    }

    fn enforce_total_limit(&mut self, metrics: &GraphCacheMetrics) {
        while self.entries.len() > self.max_entries || self.resident_bytes > self.max_resident_bytes
        {
            if self.evict_one(None, false, metrics).is_none()
                && self.evict_one(None, true, metrics).is_none()
            {
                break;
            }
        }
    }

    fn evict_one(
        &mut self,
        tenant: Option<&str>,
        allow_pinned: bool,
        metrics: &GraphCacheMetrics,
    ) -> Option<()> {
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

#[cfg(test)]
mod tests {
    use super::*;

    /// A zero byte budget has to read as "this cache is off", the way a zero
    /// `max_entries` already does, rather than as a real budget of zero bytes
    /// that every insert is then trimmed back down to.
    #[test]
    fn zero_byte_cap_disables_the_cache() {
        let metrics = GraphCacheMetrics::default();
        let mut cache = BoundedGraphCache::<u64, String>::new_with_byte_limit(4, None, 0);

        for id in 1..=3 {
            assert_eq!(
                cache.insert_sized(id, format!("v{id}"), "cell", false, 60, &metrics),
                None,
                "a disabled cache refuses the insert"
            );
            assert_eq!(cache.get(&id), None, "and nothing answers a lookup");
        }

        assert_eq!(cache.len(), 0);
        assert_eq!(cache.resident_bytes(), 0);

        // The insert is refused *before* anything is stored, so no eviction
        // work runs on the way. Reporting the refusal is what makes a cache
        // that caches nothing distinguishable from one that is simply cold.
        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.insertions, 0, "nothing was ever stored");
        assert_eq!(snapshot.evictions, 0, "and so nothing had to be evicted");
        assert_eq!(snapshot.tenant_quota_rejections, 3);
    }

    /// `insert` reports a zero size, but a zero *cap* is still off: an entry
    /// that claims to be free does not buy its way into a disabled cache.
    #[test]
    fn zero_byte_cap_refuses_a_zero_sized_insert() {
        let metrics = GraphCacheMetrics::default();
        let mut cache = BoundedGraphCache::<u64, String>::new_with_byte_limit(4, None, 0);

        assert_eq!(cache.insert(1, "v1".to_string(), "cell", false, &metrics), None);
        assert_eq!(cache.len(), 0);
        assert_eq!(cache.get(&1), None);
        assert_eq!(metrics.snapshot().tenant_quota_rejections, 1);
    }

    /// A zero entry cap has always meant "off". It still does, so both limits
    /// answer the same way.
    #[test]
    fn zero_entry_cap_still_disables_the_cache() {
        let metrics = GraphCacheMetrics::default();
        let mut cache = BoundedGraphCache::<u64, String>::new_with_byte_limit(0, None, usize::MAX);

        assert_eq!(cache.insert_sized(1, "v1".to_string(), "cell", false, 60, &metrics), None);
        assert_eq!(cache.len(), 0);
        assert_eq!(cache.resident_bytes(), 0);
        assert_eq!(metrics.snapshot().tenant_quota_rejections, 1);
    }

    /// A real byte budget behaves exactly as it did before the zero cap was
    /// given its own meaning: it holds what fits and evicts the
    /// least-recently-used unpinned entry to make room.
    #[test]
    fn non_zero_byte_cap_still_caches_and_evicts_by_lru() {
        let metrics = GraphCacheMetrics::default();
        let mut cache = BoundedGraphCache::<u64, String>::new_with_byte_limit(4, None, 100);

        assert_eq!(
            cache.insert_sized(1, "a".to_string(), "cell", false, 60, &metrics),
            Some("a".to_string()),
            "an entry inside the budget is stored and handed back"
        );
        assert_eq!(
            cache.insert_sized(2, "b".to_string(), "cell", false, 60, &metrics),
            Some("b".to_string())
        );

        assert_eq!(cache.len(), 1, "60 + 60 does not fit in 100");
        assert_eq!(cache.resident_bytes(), 60);
        assert_eq!(cache.get(&1), None, "the older entry made room");
        assert_eq!(cache.get(&2), Some("b".to_string()));

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.insertions, 2);
        assert_eq!(snapshot.evictions, 1);
        assert_eq!(snapshot.tenant_quota_rejections, 0);
    }

    /// An entry larger than the whole budget is not cached even though it fits
    /// `max_entries`, and making room for it empties the cache. That is what
    /// the cache did before the zero cap was special-cased and is deliberately
    /// unchanged here: only a budget of exactly zero is "off", not a budget
    /// that some entry happens to exceed.
    #[test]
    fn an_entry_larger_than_the_whole_budget_is_not_cached() {
        let metrics = GraphCacheMetrics::default();
        let mut cache = BoundedGraphCache::<u64, String>::new_with_byte_limit(4, None, 100);

        assert_eq!(
            cache.insert_sized(1, "a".to_string(), "cell", false, 60, &metrics),
            Some("a".to_string())
        );
        assert_eq!(
            cache.insert_sized(2, "big".to_string(), "cell", true, 101, &metrics),
            None,
            "the oversized insert reports a miss, so the caller keeps its own copy"
        );

        assert_eq!(cache.len(), 0);
        assert_eq!(cache.resident_bytes(), 0);
    }
}
