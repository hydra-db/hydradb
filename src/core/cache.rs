use std::collections::BTreeMap;
use std::sync::atomic::Ordering;
#[cfg(feature = "opencypher")]
use std::sync::Arc;

use crate::{engine, GraphCacheMetrics, GraphEpoch, VertexId};
#[cfg(feature = "opencypher")]
use crate::{EdgeMetadata, RelationshipId};
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) struct SupernodeCacheKey {
    pub(crate) cell_id: String,
    pub(crate) edge_type: String,
    pub(crate) direction: engine::ArtifactDirection,
    pub(crate) vertex_id: VertexId,
    pub(crate) base_epoch: GraphEpoch,
}

impl SupernodeCacheKey {
    pub(crate) fn new(
        cell_id: &str,
        edge_type: &str,
        direction: engine::ArtifactDirection,
        vertex_id: VertexId,
        base_epoch: GraphEpoch,
    ) -> Self {
        Self {
            cell_id: cell_id.to_string(),
            edge_type: edge_type.to_string(),
            direction,
            vertex_id,
            base_epoch,
        }
    }
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) struct PostingChunkCacheKey {
    pub(crate) cell_id: String,
    pub(crate) edge_type: String,
    pub(crate) direction: engine::ArtifactDirection,
    pub(crate) vertex_id: VertexId,
    pub(crate) base_epoch: GraphEpoch,
    pub(crate) chunk_id: u64,
}

impl PostingChunkCacheKey {
    pub(crate) fn new(group: &engine::SupernodeGroup, chunk_id: u64) -> Self {
        Self {
            cell_id: group.cell_id.clone(),
            edge_type: group.edge_type.clone(),
            direction: group.direction,
            vertex_id: group.vertex_id,
            base_epoch: group.base_epoch,
            chunk_id,
        }
    }

    pub(crate) fn from_chunk(chunk: &engine::PostingChunk) -> Self {
        Self {
            cell_id: chunk.cell_id.clone(),
            edge_type: chunk.edge_type.clone(),
            direction: chunk.direction,
            vertex_id: chunk.owner,
            base_epoch: chunk.base_epoch,
            chunk_id: chunk.chunk_id,
        }
    }
}

#[cfg(feature = "opencypher")]
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) struct RelationshipRowsCacheKey {
    pub(crate) cell_id: String,
    pub(crate) edge_type: String,
    pub(crate) src: VertexId,
    pub(crate) dst: VertexId,
    pub(crate) read_epoch: GraphEpoch,
}

#[cfg(feature = "opencypher")]
impl RelationshipRowsCacheKey {
    pub(crate) fn new(
        cell_id: &str,
        edge_type: &str,
        src: VertexId,
        dst: VertexId,
        read_epoch: GraphEpoch,
    ) -> Self {
        Self {
            cell_id: cell_id.to_string(),
            edge_type: edge_type.to_string(),
            src,
            dst,
            read_epoch,
        }
    }
}

#[cfg(feature = "opencypher")]
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) struct SourceRelationshipRowsCacheKey {
    pub(crate) cell_id: String,
    pub(crate) edge_type: String,
    pub(crate) src: VertexId,
    pub(crate) read_epoch: GraphEpoch,
}

#[cfg(feature = "opencypher")]
impl SourceRelationshipRowsCacheKey {
    pub(crate) fn new(
        cell_id: &str,
        edge_type: &str,
        src: VertexId,
        read_epoch: GraphEpoch,
    ) -> Self {
        Self {
            cell_id: cell_id.to_string(),
            edge_type: edge_type.to_string(),
            src,
            read_epoch,
        }
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
    pub(crate) read_epoch: GraphEpoch,
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
        read_epoch: GraphEpoch,
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
}

struct CacheEntry<V> {
    value: V,
    tenant: String,
    pinned: bool,
    last_access: u64,
}

pub(crate) struct BoundedGraphCache<K, V> {
    max_entries: usize,
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
        Self {
            max_entries,
            max_entries_per_tenant,
            clock: 0,
            entries: BTreeMap::new(),
            tenant_entries: BTreeMap::new(),
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.entries.len()
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
        mut score: impl FnMut(&K, &V) -> GraphEpoch,
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
        if self.max_entries == 0 {
            metrics
                .tenant_quota_rejections
                .fetch_add(1, Ordering::Relaxed);
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
            },
        );
        if let Some(previous) = previous {
            self.decrement_tenant(&previous.tenant);
        }
        *self.tenant_entries.entry(tenant.clone()).or_default() += 1;
        metrics.insertions.fetch_add(1, Ordering::Relaxed);
        if pinned {
            metrics.pinned_insertions.fetch_add(1, Ordering::Relaxed);
        }

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
                metrics
                    .tenant_quota_rejections
                    .fetch_add(1, Ordering::Relaxed);
                break;
            }
        }
    }

    fn enforce_total_limit(&mut self, metrics: &GraphCacheMetrics) {
        while self.entries.len() > self.max_entries {
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
                tenant.is_none_or(|tenant| tenant == entry.tenant)
                    && (allow_pinned || !entry.pinned)
            })
            .min_by_key(|(_, entry)| entry.last_access)
            .map(|(key, _)| key.clone())?;
        self.remove(&key);
        metrics.evictions.fetch_add(1, Ordering::Relaxed);
        Some(())
    }

    pub(crate) fn remove(&mut self, key: &K) -> Option<V> {
        let entry = self.entries.remove(key)?;
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
