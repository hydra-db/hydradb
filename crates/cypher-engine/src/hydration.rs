//! One hydration identity map per read session.
//!
//! Operators hydrate vertices independently: every invocation of a join's
//! correlated input, every expand hop and every depth of a variable-length
//! expand asks storage for its own targets. On join and multi-hop shapes the
//! same vertex is therefore fetched once per row that reaches it. The executor
//! runs every plan through [`HydrationIdentityMap`], which wraps the session's
//! [`GraphRead`] and answers `hydrate_vertices` from vertices this request has
//! already materialized, so each vertex is fetched from storage at most once
//! per request.
//!
//! The map is sound because a read session is one snapshot: a vertex record,
//! or its absence, cannot change inside it.
//!
//! # Batching
//!
//! Each `hydrate_vertices` call is deduplicated, split into hits and misses,
//! and the misses go to the backend as **one** batch. Rows from ordered
//! property scan pages are whole records too, so they seed the map.
//!
//! # Memory bound and eviction
//!
//! The map holds at most [`GraphRead::max_intermediate_rows`] entries, the
//! same limit that bounds every intermediate row collection of the request.
//! A known-absent vertex is an entry too. Nothing is evicted: once the map is
//! full it stops admitting entries, keeps serving the ones it has, and
//! hydrates every further miss from storage without remembering it. That
//! degrades to the per-operator hydration the engine did before the map, never
//! to an error, so a request that fit the limit before still fits it. A
//! backend that reports no limit (the default) gets an unbounded map, as it
//! gets unbounded intermediate collections.
//!
//! # Repeated seed lookups
//!
//! The same wrapper memoizes exact property seeks and selective equality probes
//! across correlated calls and UNION arms. These answers also belong to the
//! snapshot. The memo has separate entry, ID and estimated-byte limits; errors
//! are never cached, and every hit still checks cancellation. It is dropped
//! with the read session and cannot hide writes from a later query.
//!
//! # Streaming
//!
//! The map changes no storage request. It sits behind the existing batched
//! `hydrate_vertices(&[VertexId])` call, so a cursor-based contract that
//! hydrates page by page can consult it per page unchanged.

use std::collections::{BTreeSet, HashMap};

use async_trait::async_trait;

use crate::{
    BoundedVertexIds, ExpandDirection, ExpandNeighbor, ExpandRequest, GraphPhysicalPlan, GraphRead, OrderedPropertyEntryPage,
    OrderedPropertyScanPage, OrderedPropertyScanRequest, OrderedRelationshipPropertyScanPage, OrderedRelationshipPropertyScanRequest,
    PropertyEqualityCandidate, PropertyPointSet, RelationshipRecord, ScalarValue, StatisticsAccessReport, StorageResult, VertexId,
    VertexRecord, VertexStorageScanPage, VertexStorageScanRequest,
};

/// The read a map sits in front of: the execution's own, or a fork it owns.
enum InnerRead<'a> {
    Borrowed(&'a mut dyn GraphRead),
    Owned(Box<dyn GraphRead + 'a>),
}

impl<'a> std::ops::Deref for InnerRead<'a> {
    type Target = dyn GraphRead + 'a;

    fn deref(&self) -> &Self::Target {
        match self {
            Self::Borrowed(read) => &**read,
            Self::Owned(read) => &**read,
        }
    }
}

impl std::ops::DerefMut for InnerRead<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        match self {
            Self::Borrowed(read) => &mut **read,
            Self::Owned(read) => &mut **read,
        }
    }
}

pub(crate) struct HydrationIdentityMap<'a> {
    inner: InnerRead<'a>,
    /// `None` records a vertex the snapshot does not hold.
    vertices: HashMap<VertexId, Option<VertexRecord>>,
    capacity: Option<usize>,
    lookups: Vec<(EqualityLookup, Option<Vec<VertexId>>)>,
    lookup_bytes: usize,
    lookup_ids: usize,
}

// Incoming/outgoing UNION arms often repeat exactly the same seed lookup.
// Cache only within this snapshot, with separate limits for empty results,
// candidate IDs and strings. A full cache falls back to ordinary reads.
const LOOKUP_MAX_ENTRIES: usize = 64;
const LOOKUP_MAX_IDS: usize = 4096;
const LOOKUP_MAX_BYTES: usize = 256 * 1024;

#[derive(Clone, Eq, PartialEq)]
enum EqualityLookup {
    Seek(String, ScalarValue),
    Probe(Vec<PropertyEqualityCandidate>),
}

/// Borrow the caller's key for comparison and admission. In particular, large
/// IN lists and misses after the cache fills must not allocate owned copies.
#[derive(Clone, Copy)]
enum EqualityLookupRef<'a> {
    Seek(&'a str, &'a ScalarValue),
    Probe(&'a [PropertyEqualityCandidate]),
}

impl EqualityLookupRef<'_> {
    fn matches(self, owned: &EqualityLookup) -> bool {
        match (self, owned) {
            (Self::Seek(property, value), EqualityLookup::Seek(other_property, other_value)) => {
                property == other_property && value == other_value
            }
            (Self::Probe(candidates), EqualityLookup::Probe(other)) => candidates == other,
            _ => false,
        }
    }

    fn into_owned(self) -> EqualityLookup {
        match self {
            Self::Seek(property, value) => EqualityLookup::Seek(property.to_owned(), value.clone()),
            Self::Probe(candidates) => EqualityLookup::Probe(candidates.to_vec()),
        }
    }

    fn retained_bytes(&self) -> usize {
        fn value_bytes(value: &ScalarValue) -> usize {
            size_of::<ScalarValue>()
                + match value {
                    ScalarValue::String(value) | ScalarValue::Float(value) => value.len(),
                    _ => 0,
                }
        }
        size_of::<EqualityLookup>()
            + match self {
                Self::Seek(property, value) => property.len() + value_bytes(value),
                Self::Probe(candidates) => candidates
                    .iter()
                    .map(|candidate| {
                        size_of::<PropertyEqualityCandidate>()
                            + candidate.property.len()
                            + candidate.values.iter().map(value_bytes).sum::<usize>()
                    })
                    .sum::<usize>(),
            }
    }
}

impl<'a> HydrationIdentityMap<'a> {
    pub(crate) fn new(inner: &'a mut dyn GraphRead) -> Self {
        Self::over(InnerRead::Borrowed(inner))
    }

    fn over(inner: InnerRead<'a>) -> Self {
        let capacity = inner.max_intermediate_rows();
        Self {
            inner,
            vertices: HashMap::new(),
            capacity,
            lookups: Vec::new(),
            lookup_bytes: 0,
            lookup_ids: 0,
        }
    }

    fn cached_lookup(&self, key: EqualityLookupRef<'_>) -> Option<Option<Vec<VertexId>>> {
        self.lookups
            .iter()
            .find(|(existing, _)| key.matches(existing))
            .map(|(_, ids)| ids.clone())
    }

    fn admit_lookup(&mut self, key: EqualityLookupRef<'_>, ids: Option<&[VertexId]>) {
        let count = ids.map_or(0, <[VertexId]>::len);
        if self.lookups.len() >= LOOKUP_MAX_ENTRIES
            || self.lookup_ids.saturating_add(count) > self.capacity.unwrap_or(LOOKUP_MAX_IDS).min(LOOKUP_MAX_IDS)
        {
            return;
        }
        let bytes = key.retained_bytes().saturating_add(count.saturating_mul(size_of::<VertexId>()));
        if self.lookup_bytes.saturating_add(bytes) <= LOOKUP_MAX_BYTES {
            self.lookup_ids += count;
            self.lookup_bytes += bytes;
            self.lookups.push((key.into_owned(), ids.map(<[VertexId]>::to_vec)));
        }
    }

    fn admit(&mut self, id: VertexId, vertex: Option<VertexRecord>) {
        if self.vertices.contains_key(&id) || self.capacity.is_none_or(|capacity| self.vertices.len() < capacity) {
            self.vertices.insert(id, vertex);
        }
    }

    async fn scan_vertex_points(&mut self, ids: &[VertexId]) -> StorageResult<VertexStorageScanPage> {
        let mut records = Vec::new();
        let mut misses = Vec::new();
        for id in ids.iter().copied().collect::<BTreeSet<_>>() {
            match self.vertices.get(&id) {
                Some(Some(record)) => records.push(record.clone()),
                Some(None) => {}
                None => misses.push(id),
            }
        }
        if misses.is_empty() {
            return Ok(VertexStorageScanPage::Records(records));
        }

        match self
            .inner
            .scan_vertex_storage(&VertexStorageScanRequest::IdPoints { ids: misses.clone() })
            .await?
        {
            VertexStorageScanPage::Records(fetched) => {
                let present = fetched.iter().map(|record| record.id).collect::<BTreeSet<_>>();
                for record in &fetched {
                    self.admit(record.id, Some(record.clone()));
                }
                for id in misses {
                    if !present.contains(&id) {
                        self.admit(id, None);
                    }
                }
                records.extend(fetched);
                Ok(VertexStorageScanPage::Records(records))
            }
            VertexStorageScanPage::Ids(fetched) => {
                let present = fetched.iter().copied().collect::<BTreeSet<_>>();
                for id in misses {
                    if !present.contains(&id) {
                        self.admit(id, None);
                    }
                }
                let mut ids = records.into_iter().map(|record| record.id).collect::<Vec<_>>();
                ids.extend(fetched);
                Ok(VertexStorageScanPage::Ids(ids))
            }
            VertexStorageScanPage::Ordered(page) => Ok(VertexStorageScanPage::Ordered(page)),
        }
    }
}

#[async_trait]
impl GraphRead for HydrationIdentityMap<'_> {
    fn checkpoint(&self, operation: &'static str) -> StorageResult<()> {
        self.inner.checkpoint(operation)
    }

    /// A fork gets a map of its own over the backend's fork. The arms it
    /// serves run at the same time, so a lookup they repeat is in flight in
    /// each before either could answer it from a shared map: sharing would add
    /// a lock and save nothing.
    fn fork_read(&self) -> Option<Box<dyn GraphRead + '_>> {
        let inner = self.inner.fork_read()?;
        Some(Box::new(HydrationIdentityMap::over(InnerRead::Owned(inner))))
    }

    fn check_intermediate_rows(&self, operation: &'static str, rows: usize) -> StorageResult<()> {
        self.inner.check_intermediate_rows(operation, rows)
    }

    fn max_intermediate_rows(&self) -> Option<usize> {
        self.inner.max_intermediate_rows()
    }

    async fn scan_vertex_storage(&mut self, request: &VertexStorageScanRequest) -> StorageResult<VertexStorageScanPage> {
        if let VertexStorageScanRequest::IdPoints { ids } = request {
            return self.scan_vertex_points(ids).await;
        }
        let lookup = match request {
            VertexStorageScanRequest::PropertyPoints {
                property,
                points: PropertyPointSet::Single(value),
            } => Some(EqualityLookupRef::Seek(property, value)),
            _ => None,
        };
        if let Some(key) = lookup {
            self.inner.checkpoint("cypher_cached_property_seek")?;
            if let Some(Some(ids)) = self.cached_lookup(key) {
                return Ok(VertexStorageScanPage::Ids(ids));
            }
        }

        let page = self.inner.scan_vertex_storage(request).await?;
        match &page {
            VertexStorageScanPage::Ids(ids) => {
                if let Some(key) = lookup {
                    self.admit_lookup(key, Some(ids));
                }
            }
            VertexStorageScanPage::Records(records) => {
                for record in records {
                    self.admit(record.id, Some(record.clone()));
                }
            }
            VertexStorageScanPage::Ordered(page) => {
                for vertex in &page.vertices {
                    self.admit(vertex.id, Some(vertex.clone()));
                }
            }
        }
        Ok(page)
    }

    async fn seek_vertices_by_property(&mut self, property: &str, value: &ScalarValue) -> StorageResult<Vec<VertexId>> {
        self.inner.checkpoint("cypher_cached_property_seek")?;
        let key = EqualityLookupRef::Seek(property, value);
        if let Some(Some(ids)) = self.cached_lookup(key) {
            return Ok(ids);
        }
        let ids = self.inner.seek_vertices_by_property(property, value).await?;
        self.admit_lookup(key, Some(&ids));
        Ok(ids)
    }

    async fn seek_vertices_by_property_values(&mut self, property: &str, values: &[ScalarValue]) -> StorageResult<Vec<VertexId>> {
        self.inner.seek_vertices_by_property_values(property, values).await
    }

    async fn scan_vertices_by_property(&mut self, property: &str) -> StorageResult<Vec<VertexId>> {
        self.inner.scan_vertices_by_property(property).await
    }

    async fn scan_vertices_by_property_ordered(&mut self, request: &OrderedPropertyScanRequest) -> StorageResult<OrderedPropertyScanPage> {
        let page = self.inner.scan_vertices_by_property_ordered(request).await?;
        for vertex in &page.vertices {
            self.admit(vertex.id, Some(vertex.clone()));
        }
        Ok(page)
    }

    /// Forwarded: an entry page carries no record to seed the map with, and
    /// the trait default would walk the records through this map instead.
    async fn scan_vertex_entries_by_property_ordered(
        &mut self,
        request: &OrderedPropertyScanRequest,
    ) -> StorageResult<OrderedPropertyEntryPage> {
        self.inner.scan_vertex_entries_by_property_ordered(request).await
    }

    /// Forwarded, for the same reason: the default would report no such walk.
    async fn scan_vertex_entries_by_property_for_count(
        &mut self,
        request: &OrderedPropertyScanRequest,
    ) -> StorageResult<Option<OrderedPropertyEntryPage>> {
        self.inner.scan_vertex_entries_by_property_for_count(request).await
    }

    async fn scan_relationships_by_property_ordered(
        &mut self,
        request: &OrderedRelationshipPropertyScanRequest,
    ) -> StorageResult<OrderedRelationshipPropertyScanPage> {
        self.inner.scan_relationships_by_property_ordered(request).await
    }

    async fn scan_vertices_by_label(&mut self, label: &str, max_results: Option<usize>) -> StorageResult<Vec<VertexId>> {
        self.inner.scan_vertices_by_label(label, max_results).await
    }

    async fn seek_vertices_by_property_values_bounded(
        &mut self,
        property: &str,
        values: &[ScalarValue],
        max_index_entries: usize,
    ) -> StorageResult<BoundedVertexIds> {
        self.inner
            .seek_vertices_by_property_values_bounded(property, values, max_index_entries)
            .await
    }

    async fn scan_vertices_by_label_bounded(&mut self, label: &str, max_index_entries: usize) -> StorageResult<BoundedVertexIds> {
        self.inner.scan_vertices_by_label_bounded(label, max_index_entries).await
    }

    async fn scan_all_vertices(&mut self) -> StorageResult<Vec<VertexId>> {
        self.inner.scan_all_vertices().await
    }

    async fn seek_relationships_by_property(
        &mut self,
        relationship_type: &str,
        property: &str,
        value: &ScalarValue,
    ) -> StorageResult<Vec<RelationshipRecord>> {
        self.inner.seek_relationships_by_property(relationship_type, property, value).await
    }

    async fn seek_relationships_by_property_values(
        &mut self,
        relationship_type: &str,
        property: &str,
        values: &[ScalarValue],
    ) -> StorageResult<Vec<RelationshipRecord>> {
        self.inner
            .seek_relationships_by_property_values(relationship_type, property, values)
            .await
    }

    /// Forwarded rather than left to the trait default, which declines: a
    /// plan that also expands is wrapped in this map, and a probe that always
    /// declined there would silently cost such a plan its probed access path.
    async fn probe_relationships_by_property_lower_bound(
        &mut self,
        relationship_type: &str,
        property: &str,
        lower: &ScalarValue,
        inclusive: bool,
        max_index_entries: usize,
    ) -> StorageResult<Option<Vec<RelationshipRecord>>> {
        self.inner
            .probe_relationships_by_property_lower_bound(relationship_type, property, lower, inclusive, max_index_entries)
            .await
    }

    /// Forwarded for the same reason as the range probe above.
    async fn probe_relationships_by_type(
        &mut self,
        relationship_type: &str,
        max_index_entries: usize,
    ) -> StorageResult<Option<Vec<RelationshipRecord>>> {
        self.inner.probe_relationships_by_type(relationship_type, max_index_entries).await
    }

    async fn expand_relationships(&mut self, request: &ExpandRequest) -> StorageResult<Vec<RelationshipRecord>> {
        self.inner.expand_relationships(request).await
    }

    async fn expand_neighbors(&mut self, request: &ExpandRequest) -> StorageResult<Option<Vec<ExpandNeighbor>>> {
        self.inner.expand_neighbors(request).await
    }

    async fn outgoing_neighbor_page(
        &mut self,
        input: VertexId,
        edge_type: &str,
        after: Option<VertexId>,
        size: usize,
    ) -> StorageResult<Option<Vec<VertexId>>> {
        self.inner.outgoing_neighbor_page(input, edge_type, after, size).await
    }

    async fn expand_relationships_between(
        &mut self,
        direction: ExpandDirection,
        neighbors: &[ExpandNeighbor],
    ) -> StorageResult<Vec<RelationshipRecord>> {
        self.inner.expand_relationships_between(direction, neighbors).await
    }

    /// Hits come from the map; every miss is fetched in one batch. Like the
    /// backends it wraps, the result is unordered and holds each existing
    /// vertex once.
    async fn hydrate_vertices(&mut self, vertex_ids: &[VertexId]) -> StorageResult<Vec<VertexRecord>> {
        let requested = vertex_ids.iter().copied().collect::<BTreeSet<_>>();
        let mut hydrated = Vec::with_capacity(requested.len());
        let mut misses = Vec::new();
        for id in requested {
            match self.vertices.get(&id) {
                Some(vertex) => hydrated.extend(vertex.iter().cloned()),
                None => misses.push(id),
            }
        }
        if misses.is_empty() {
            return Ok(hydrated);
        }
        let mut fetched = self
            .inner
            .hydrate_vertices(&misses)
            .await?
            .into_iter()
            .map(|vertex| (vertex.id, vertex))
            .collect::<HashMap<_, _>>();
        for id in misses {
            let vertex = fetched.remove(&id);
            if let Some(vertex) = &vertex {
                hydrated.push(vertex.clone());
            }
            self.admit(id, vertex);
        }
        Ok(hydrated)
    }

    /// Cache the backend's exact answer, including the distinction between
    /// an unavailable probe (`None`) and a proven empty match (`Some([])`).
    /// Candidate order is part of the key: it can affect the probe budget.
    async fn probe_selective_equality(&mut self, candidates: &[PropertyEqualityCandidate]) -> StorageResult<Option<Vec<VertexId>>> {
        self.inner.checkpoint("cypher_cached_equality_probe")?;
        let key = EqualityLookupRef::Probe(candidates);
        if let Some(ids) = self.cached_lookup(key) {
            return Ok(ids);
        }
        let ids = self.inner.probe_selective_equality(candidates).await?;
        self.admit_lookup(key, ids.as_deref());
        Ok(ids)
    }

    fn operator_started(&mut self, plan: &GraphPhysicalPlan) {
        self.inner.operator_started(plan);
    }

    fn operator_finished(&mut self, plan: &GraphPhysicalPlan, rows_out: usize) {
        self.inner.operator_finished(plan, rows_out);
    }

    fn record_full_scan(&self) {
        self.inner.record_full_scan();
    }

    fn raise_completion_condition(&mut self, status: hydradb_cypher_ast::GqlStatus) {
        self.inner.raise_completion_condition(status);
    }

    fn take_completion_conditions(&mut self) -> Vec<hydradb_cypher_ast::GqlStatus> {
        self.inner.take_completion_conditions()
    }

    fn record_retained_rows(&self, rows: usize) {
        self.inner.record_retained_rows(rows);
    }

    fn record_hydrated_vertices(&self, vertices: usize) {
        self.inner.record_hydrated_vertices(vertices);
    }

    fn record_statistics_access(&self, report: &StatisticsAccessReport) {
        self.inner.record_statistics_access(report);
    }

    fn max_query_index_candidates(&self) -> Option<usize> {
        self.inner.max_query_index_candidates()
    }
}

/// Whether `plan` can hydrate the same vertex twice.
///
/// The identity map earns its keep only where a vertex is reached more than
/// once: an expansion whose targets overlap, or a join that rebuilds a side.
/// A point seek or a single scan hydrates each vertex exactly once, and
/// wrapping it buys nothing while costing a set build and two clones per
/// vertex -- which a seek-shaped query pays in full and never recovers.
pub(crate) fn plan_reuses_hydration(plan: &GraphPhysicalPlan) -> bool {
    match plan {
        // Operators that read the same vertices more than once: an expansion
        // meets shared neighbours, a join and a correlated subquery run their
        // side once per row.
        GraphPhysicalPlan::Expand { .. }
        | GraphPhysicalPlan::VariableExpand { .. }
        | GraphPhysicalPlan::NaturalJoin { .. }
        | GraphPhysicalPlan::PathUnion { .. }
        | GraphPhysicalPlan::Exists { .. }
        | GraphPhysicalPlan::ScalarSubquery { .. }
        // A call runs its query correlated with each row, as a subquery does.
        | GraphPhysicalPlan::CallProcedure { .. } => true,
        // Anything else reuses when something below it does; walking every
        // child keeps a new operator from hiding its inputs.
        other => other.children().into_iter().any(plan_reuses_hydration),
    }
}
