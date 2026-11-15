//! Shared in-memory graph storage for the engine's integration tests.
//!
//! Lives here rather than in one test binary so more than one integration test
//! can seed a graph without a second copy of the same harness.

// Each integration-test binary compiles this module separately, so helpers
// used by only one of them look dead to the other.
#![allow(dead_code)]

pub mod compat;
pub mod profiling;

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use hydradb_cypher_engine::{
    compare_property_values, scalar_values_equal, BoundedVertexIds, ExpandDirection, ExpandNeighbor, ExpandRequest, GraphRead,
    GraphStorage, OrderedPropertyScanPage, OrderedPropertyScanRequest, OrderedRelationshipPropertyScanPage,
    OrderedRelationshipPropertyScanRequest, OrderedScanCursor, OrderedScanPosition, PropertyEqualityCandidate, ReadRequest,
    RelationshipRecord, ScalarValue, StatisticsAccessReport, StorageResult, VertexId, VertexRecord,
};

pub type Checkpoint = Arc<dyn Fn(&'static str) -> StorageResult<()> + Send + Sync>;

#[derive(Clone, Default)]
pub struct MemoryStorage {
    pub checkpoint: Option<Checkpoint>,
    pub vertices: BTreeMap<VertexId, VertexRecord>,
    pub relationships: Vec<RelationshipRecord>,
    pub expand_requests: Arc<Mutex<Vec<ExpandRequest>>>,
    /// Relationship records every expansion read, eager or windowed: what a
    /// `LIMIT` pushed into the hop saves.
    pub expanded_relationship_records: Arc<Mutex<usize>>,
    /// Answer `expand_neighbors` with `None`, as a backend that cannot list
    /// neighbours apart from their relationships does.
    pub without_neighbor_listing: bool,
    /// Answer the relationship type and range probes the way a backend with
    /// those indexes does, instead of declining as the trait default does.
    pub answers_relationship_probes: bool,
    /// Every relationship probe that reached this backend, as
    /// `type:<T>` or `range:<T>.<p>`.
    pub relationship_probe_requests: Arc<Mutex<Vec<String>>>,
    pub hydration_requests: Arc<Mutex<Vec<Vec<VertexId>>>>,
    pub label_scan_limits: Arc<Mutex<Vec<Option<usize>>>>,
    pub seek_values: Arc<Mutex<Vec<ScalarValue>>>,
    /// Every value a relationship property seek asked this backend for.
    pub relationship_seek_values: Arc<Mutex<Vec<ScalarValue>>>,
    pub ordered_scan_requests: Arc<Mutex<Vec<OrderedPropertyScanRequest>>>,
    /// Index entries judged by entry-bounded ordered pages, labels ignored.
    pub ordered_scan_entries: Arc<Mutex<usize>>,
    /// What every statistics-chosen access reported.
    pub statistics_access: Arc<Mutex<Vec<StatisticsAccessReport>>>,
    /// The limit of every guarded equality seek that reached this backend.
    pub bounded_seek_limits: Arc<Mutex<Vec<usize>>>,
    /// Cap on rows per ordered page, standing in for a backend that pages
    /// more finely than the engine asked. `None` returns the whole window in
    /// one page, so a test can run the same shape at every page size and
    /// prove the engine's loop, not the backend's generosity.
    pub ordered_page_size: Option<usize>,
    pub read_sessions: Arc<Mutex<usize>>,
    pub max_intermediate_rows: Option<usize>,
    /// The query's index-candidate cap, as a backend that enforces one
    /// reports it. `None` has no cap.
    pub max_query_index_candidates: Option<usize>,
    /// Properties this backend's probe is willing to call selective. Empty
    /// means the backend has no probe at all, which is the default trait
    /// behaviour and must leave the planned access path untouched.
    pub selective_properties: Vec<String>,
    /// Answer a winning probe with no vertices, standing in for a property
    /// that is selective all the way to zero rows.
    pub probe_answers_empty: bool,
    pub probe_requests: Arc<Mutex<Vec<Vec<PropertyEqualityCandidate>>>>,
    pub ordered_relationship_scan_requests: Arc<Mutex<Vec<OrderedRelationshipPropertyScanRequest>>>,
    /// Relationships the ordered relationship scan returned, across pages.
    pub ordered_relationship_scan_rows: Arc<Mutex<usize>>,
}

impl MemoryStorage {
    pub fn with_vertices(vertices: impl IntoIterator<Item = VertexRecord>) -> Self {
        Self::with_graph(vertices, [])
    }

    pub fn with_graph(
        vertices: impl IntoIterator<Item = VertexRecord>,
        relationships: impl IntoIterator<Item = RelationshipRecord>,
    ) -> Self {
        Self {
            vertices: vertices.into_iter().map(|vertex| (vertex.id, vertex)).collect(),
            relationships: relationships.into_iter().collect(),
            ..Self::default()
        }
    }
}

pub struct MemoryRead<'a> {
    pub checkpoint: Option<Checkpoint>,
    pub vertices: &'a BTreeMap<VertexId, VertexRecord>,
    pub relationships: &'a [RelationshipRecord],
    pub expand_requests: Arc<Mutex<Vec<ExpandRequest>>>,
    pub expanded_relationship_records: Arc<Mutex<usize>>,
    pub without_neighbor_listing: bool,
    pub answers_relationship_probes: bool,
    pub relationship_probe_requests: Arc<Mutex<Vec<String>>>,
    pub hydration_requests: Arc<Mutex<Vec<Vec<VertexId>>>>,
    pub label_scan_limits: Arc<Mutex<Vec<Option<usize>>>>,
    pub seek_values: Arc<Mutex<Vec<ScalarValue>>>,
    pub relationship_seek_values: Arc<Mutex<Vec<ScalarValue>>>,
    pub ordered_scan_requests: Arc<Mutex<Vec<OrderedPropertyScanRequest>>>,
    pub ordered_scan_entries: Arc<Mutex<usize>>,
    pub statistics_access: Arc<Mutex<Vec<StatisticsAccessReport>>>,
    pub bounded_seek_limits: Arc<Mutex<Vec<usize>>>,
    pub ordered_page_size: Option<usize>,
    pub max_intermediate_rows: Option<usize>,
    pub max_query_index_candidates: Option<usize>,
    pub selective_properties: Vec<String>,
    pub probe_answers_empty: bool,
    pub probe_requests: Arc<Mutex<Vec<Vec<PropertyEqualityCandidate>>>>,
    pub ordered_relationship_scan_requests: Arc<Mutex<Vec<OrderedRelationshipPropertyScanRequest>>>,
    pub ordered_relationship_scan_rows: Arc<Mutex<usize>>,
    pub completion_conditions: Vec<hydradb_cypher_ast::GqlStatus>,
}

impl MemoryRead<'_> {
    /// An ordered page bounded by index entries, as a real index walk reads
    /// them: every entry of the property inside the restrictions counts,
    /// whatever its vertex's labels, so a property shared by many labels costs
    /// what it would in storage. The page ends at `required` rows or
    /// `max_index_entries` judged entries, and resumes after the last entry
    /// it judged.
    fn entry_bounded_ordered_page(&self, request: &OrderedPropertyScanRequest, max_index_entries: usize) -> OrderedPropertyScanPage {
        let mut index = self
            .vertices
            .values()
            .filter_map(|vertex| match vertex.properties.get(&request.property) {
                Some(value) if request.accepts(value) => Some((value.clone(), vertex)),
                _ => None,
            })
            .collect::<Vec<_>>();
        index.sort_by(|(left, left_vertex), (right, right_vertex)| {
            let by_value = compare_property_values(left, right);
            let by_value = if request.ascending { by_value } else { by_value.reverse() };
            by_value.then_with(|| {
                if request.id_ascending {
                    left_vertex.id.cmp(&right_vertex.id)
                } else {
                    right_vertex.id.cmp(&left_vertex.id)
                }
            })
        });
        if let Some(after) = &request.after {
            let seen = |value: &ScalarValue, id: VertexId| match compare_property_values(value, &after.value) {
                std::cmp::Ordering::Equal => {
                    if request.id_ascending {
                        id <= after.vertex_id
                    } else {
                        id >= after.vertex_id
                    }
                }
                std::cmp::Ordering::Less => request.ascending,
                std::cmp::Ordering::Greater => !request.ascending,
            };
            index.retain(|(value, vertex)| !seen(value, vertex.id));
        }
        let page_size = self.ordered_page_size.map_or(request.required, |size| size.min(request.required));
        let mut entries = index.into_iter().peekable();
        let mut judged = 0_usize;
        let mut vertices = Vec::new();
        let mut last = None;
        while entries.peek().is_some() && vertices.len() < page_size && judged < max_index_entries {
            let (value, vertex) = entries.next().expect("peeked");
            judged += 1;
            last = Some(OrderedScanPosition {
                value,
                vertex_id: vertex.id,
            });
            if request.labels.iter().all(|label| vertex.labels.contains(label)) {
                vertices.push(vertex.clone());
            }
        }
        *self.ordered_scan_entries.lock().unwrap() += judged;
        OrderedPropertyScanPage {
            vertices,
            last,
            exhausted: entries.peek().is_none(),
            index_entries: Some(judged as u64),
        }
    }
}

#[async_trait]
impl GraphRead for MemoryRead<'_> {
    fn checkpoint(&self, operation: &'static str) -> StorageResult<()> {
        self.checkpoint.as_ref().map_or(Ok(()), |check| check(operation))
    }

    fn raise_completion_condition(&mut self, status: hydradb_cypher_ast::GqlStatus) {
        self.completion_conditions.push(status);
    }

    fn take_completion_conditions(&mut self) -> Vec<hydradb_cypher_ast::GqlStatus> {
        std::mem::take(&mut self.completion_conditions)
    }

    fn check_intermediate_rows(&self, operation: &'static str, rows: usize) -> StorageResult<()> {
        if self.max_intermediate_rows.is_some_and(|limit| rows > limit) {
            return Err(hydradb_cypher_engine::StorageError::new(format!(
                "{operation} exceeded intermediate row limit"
            )));
        }
        Ok(())
    }

    fn max_intermediate_rows(&self) -> Option<usize> {
        self.max_intermediate_rows
    }

    fn record_statistics_access(&self, report: &StatisticsAccessReport) {
        self.statistics_access.lock().unwrap().push(report.clone());
    }

    fn max_query_index_candidates(&self) -> Option<usize> {
        self.max_query_index_candidates
    }

    async fn seek_vertices_by_property(&mut self, property: &str, value: &ScalarValue) -> StorageResult<Vec<VertexId>> {
        self.seek_values.lock().unwrap().push(value.clone());
        let mut ids = self
            .vertices
            .values()
            .filter(|vertex| {
                vertex
                    .properties
                    .get(property)
                    .is_some_and(|stored| scalar_values_equal(stored, value))
            })
            .map(|vertex| vertex.id)
            .collect::<Vec<_>>();
        ids.reverse();
        if let Some(first) = ids.first().copied() {
            ids.push(first);
        }
        Ok(ids)
    }

    /// Records the limit, then judges a whole seek.
    async fn seek_vertices_by_property_values_bounded(
        &mut self,
        property: &str,
        values: &[ScalarValue],
        max_index_entries: usize,
    ) -> StorageResult<BoundedVertexIds> {
        self.bounded_seek_limits.lock().unwrap().push(max_index_entries);
        let mut ids = Vec::new();
        for value in values {
            ids.extend(self.seek_vertices_by_property(property, value).await?);
        }
        ids.sort_unstable();
        ids.dedup();
        Ok(BoundedVertexIds::within(ids, max_index_entries))
    }

    async fn scan_vertices_by_property(&mut self, property: &str) -> StorageResult<Vec<VertexId>> {
        Ok(self
            .vertices
            .values()
            .filter(|vertex| vertex.properties.contains_key(property))
            .map(|vertex| vertex.id)
            .collect())
    }

    async fn scan_vertices_by_property_ordered(&mut self, request: &OrderedPropertyScanRequest) -> StorageResult<OrderedPropertyScanPage> {
        self.ordered_scan_requests.lock().unwrap().push(request.clone());
        if let Some(max_index_entries) = request.max_index_entries {
            return Ok(self.entry_bounded_ordered_page(request, max_index_entries));
        }
        // This reference backend walks every non-null entry in index order --
        // value under `compare_property_values`, then vertex ID, both in the
        // walk direction -- so booleans, numbers and strings interleave here
        // exactly as the real index walks them.
        let mut matching = self
            .vertices
            .values()
            .filter(|vertex| request.labels.iter().all(|label| vertex.labels.contains(label)))
            .filter_map(|vertex| match vertex.properties.get(&request.property) {
                Some(value) if request.accepts(value) => Some((value.clone(), vertex)),
                _ => None,
            })
            .collect::<Vec<_>>();
        matching.sort_by(|(left, left_vertex), (right, right_vertex)| {
            let by_value = compare_property_values(left, right);
            let by_value = if request.ascending { by_value } else { by_value.reverse() };
            by_value.then_with(|| {
                if request.id_ascending {
                    left_vertex.id.cmp(&right_vertex.id)
                } else {
                    right_vertex.id.cmp(&left_vertex.id)
                }
            })
        });
        // Resume strictly after the caller's position, in walk order.
        if let Some(after) = &request.after {
            let seen = |value: &ScalarValue, id: VertexId| match compare_property_values(value, &after.value) {
                std::cmp::Ordering::Equal => {
                    if request.id_ascending {
                        id <= after.vertex_id
                    } else {
                        id >= after.vertex_id
                    }
                }
                std::cmp::Ordering::Less => request.ascending,
                std::cmp::Ordering::Greater => !request.ascending,
            };
            matching.retain(|(value, vertex)| !seen(value, vertex.id));
        }
        // A page ends exactly where the caller asked. This backend's walk order
        // is its ranking, so an `index_order` page and a ranked one coincide,
        // and nothing is dropped: the position reported below is the last
        // entry returned, and the next page resumes right after it.
        let taken = self
            .ordered_page_size
            .map_or(request.required, |size| size.min(request.required))
            .min(matching.len());
        let exhausted = matching.len() <= taken;
        let page = matching.into_iter().take(taken).collect::<Vec<_>>();
        Ok(OrderedPropertyScanPage {
            last: page.last().map(|(value, vertex)| OrderedScanPosition {
                value: value.clone(),
                vertex_id: vertex.id,
            }),
            vertices: page.into_iter().map(|(_, vertex)| vertex.clone()).collect(),
            exhausted,
            index_entries: None,
        })
    }

    async fn scan_vertices_by_label(&mut self, label: &str, max_results: Option<usize>) -> StorageResult<Vec<VertexId>> {
        self.label_scan_limits.lock().unwrap().push(max_results);
        Ok(self
            .vertices
            .values()
            .filter(|vertex| vertex.labels.contains(label))
            .map(|vertex| vertex.id)
            .take(max_results.unwrap_or(usize::MAX))
            .collect())
    }

    async fn scan_all_vertices(&mut self) -> StorageResult<Vec<VertexId>> {
        Ok(self.vertices.keys().rev().copied().collect())
    }

    async fn seek_relationships_by_property(
        &mut self,
        relationship_type: &str,
        property: &str,
        value: &ScalarValue,
    ) -> StorageResult<Vec<RelationshipRecord>> {
        self.relationship_seek_values.lock().unwrap().push(value.clone());
        Ok(self
            .relationships
            .iter()
            .filter(|relationship| {
                relationship.relationship_type == relationship_type
                    && relationship
                        .properties
                        .get(property)
                        .is_some_and(|stored| scalar_values_equal(stored, value))
            })
            .cloned()
            .collect())
    }

    async fn expand_relationships(&mut self, request: &ExpandRequest) -> StorageResult<Vec<RelationshipRecord>> {
        self.expand_requests.lock().unwrap().push(request.clone());
        let inputs = request.input_vertex_ids.iter().copied().collect::<std::collections::BTreeSet<_>>();
        let mut relationships = self
            .relationships
            .iter()
            .filter(|relationship| inputs.contains(&relationship.source) || inputs.contains(&relationship.target))
            .cloned()
            .collect::<Vec<_>>();
        relationships.reverse();
        if let Some(first) = relationships.first().cloned() {
            relationships.push(first);
        }
        *self.expanded_relationship_records.lock().unwrap() += relationships.len();
        Ok(relationships)
    }

    /// The whole type when it fits `max_index_entries`, else a decline.
    async fn probe_relationships_by_type(
        &mut self,
        relationship_type: &str,
        max_index_entries: usize,
    ) -> StorageResult<Option<Vec<RelationshipRecord>>> {
        if !self.answers_relationship_probes {
            return Ok(None);
        }
        self.relationship_probe_requests
            .lock()
            .unwrap()
            .push(format!("type:{relationship_type}"));
        let relationships = self
            .relationships
            .iter()
            .filter(|relationship| relationship.relationship_type == relationship_type)
            .cloned()
            .collect::<Vec<_>>();
        Ok((relationships.len() <= max_index_entries).then_some(relationships))
    }

    /// The relationships at or above `lower` when they fit
    /// `max_index_entries`, else a decline.
    async fn probe_relationships_by_property_lower_bound(
        &mut self,
        relationship_type: &str,
        property: &str,
        lower: &ScalarValue,
        inclusive: bool,
        max_index_entries: usize,
    ) -> StorageResult<Option<Vec<RelationshipRecord>>> {
        if !self.answers_relationship_probes {
            return Ok(None);
        }
        self.relationship_probe_requests
            .lock()
            .unwrap()
            .push(format!("range:{relationship_type}.{property}"));
        let relationships = self
            .relationships
            .iter()
            .filter(|relationship| {
                relationship.relationship_type == relationship_type
                    && relationship.properties.get(property).is_some_and(|stored| {
                        let ordering = compare_property_values(stored, lower);
                        ordering.is_gt() || (inclusive && ordering.is_eq())
                    })
            })
            .cloned()
            .collect::<Vec<_>>();
        Ok((relationships.len() <= max_index_entries).then_some(relationships))
    }

    /// Neighbours in reverse storage order with the first repeated, so the
    /// engine cannot lean on the order or uniqueness of a listing either.
    async fn expand_neighbors(&mut self, request: &ExpandRequest) -> StorageResult<Option<Vec<ExpandNeighbor>>> {
        if self.without_neighbor_listing {
            return Ok(None);
        }
        self.expand_requests.lock().unwrap().push(request.clone());
        let inputs = request.input_vertex_ids.iter().copied().collect::<std::collections::BTreeSet<_>>();
        let mut neighbors = self
            .relationships
            .iter()
            .filter(|relationship| {
                request.relationship_types.is_empty() || request.relationship_types.contains(&relationship.relationship_type)
            })
            .filter_map(|relationship| {
                let (input, neighbor) = match request.direction {
                    ExpandDirection::Outgoing => (relationship.source, relationship.target),
                    ExpandDirection::Incoming => (relationship.target, relationship.source),
                };
                inputs.contains(&input).then(|| ExpandNeighbor {
                    input,
                    neighbor,
                    relationship_type: relationship.relationship_type.clone(),
                })
            })
            .collect::<Vec<_>>();
        neighbors.reverse();
        if let Some(first) = neighbors.first().cloned() {
            neighbors.push(first);
        }
        Ok(Some(neighbors))
    }

    async fn expand_relationships_between(
        &mut self,
        direction: ExpandDirection,
        neighbors: &[ExpandNeighbor],
    ) -> StorageResult<Vec<RelationshipRecord>> {
        let wanted = neighbors
            .iter()
            .map(|neighbor| (neighbor.input, neighbor.neighbor, neighbor.relationship_type.as_str()))
            .collect::<std::collections::BTreeSet<_>>();
        let mut relationships = self
            .relationships
            .iter()
            .filter(|relationship| {
                let (input, neighbor) = match direction {
                    ExpandDirection::Outgoing => (relationship.source, relationship.target),
                    ExpandDirection::Incoming => (relationship.target, relationship.source),
                };
                wanted.contains(&(input, neighbor, relationship.relationship_type.as_str()))
            })
            .cloned()
            .collect::<Vec<_>>();
        relationships.reverse();
        *self.expanded_relationship_records.lock().unwrap() += relationships.len();
        Ok(relationships)
    }

    async fn scan_relationships_by_property_ordered(
        &mut self,
        request: &OrderedRelationshipPropertyScanRequest,
    ) -> StorageResult<OrderedRelationshipPropertyScanPage> {
        self.ordered_relationship_scan_requests.lock().unwrap().push(request.clone());
        // Pages honestly, like the vertex walk above: every relationship of
        // the type with a non-null value, in `(value, source, target, id)`
        // order in the walk direction, strictly after the cursor, at most
        // `required` (or the configured page size) per page.
        let position = |relationship: &RelationshipRecord| {
            (
                relationship.properties[&request.property].clone(),
                relationship.source,
                relationship.target,
                relationship.id,
            )
        };
        let order = |left: &(ScalarValue, VertexId, VertexId, u64), right: &(ScalarValue, VertexId, VertexId, u64)| {
            let ordering = compare_property_values(&left.0, &right.0).then((left.1, left.2, left.3).cmp(&(right.1, right.2, right.3)));
            if request.ascending {
                ordering
            } else {
                ordering.reverse()
            }
        };
        let mut index = self
            .relationships
            .iter()
            .filter(|relationship| {
                relationship.relationship_type == request.relationship_type
                    && relationship
                        .properties
                        .get(&request.property)
                        .is_some_and(|value| !matches!(value, ScalarValue::Null))
            })
            .collect::<Vec<_>>();
        index.sort_by(|left, right| order(&position(left), &position(right)));
        // The cursor is this backend's own: the relationship ID it stopped on.
        let resume = request.after.as_ref().and_then(|cursor| {
            let id = u64::from_be_bytes(cursor.0.as_slice().try_into().ok()?);
            index
                .iter()
                .find(|relationship| relationship.id == id)
                .map(|relationship| position(relationship))
        });
        let page_size = self.ordered_page_size.map_or(request.required, |size| size.min(request.required));
        let mut relationships = Vec::new();
        for relationship in index {
            let entry = position(relationship);
            if resume.as_ref().is_some_and(|resume| order(&entry, resume).is_le()) {
                continue;
            }
            if relationships.len() >= page_size {
                let last: &RelationshipRecord = relationships.last().expect("a full page has rows");
                let last = OrderedScanCursor(last.id.to_be_bytes().to_vec());
                *self.ordered_relationship_scan_rows.lock().unwrap() += relationships.len();
                return Ok(OrderedRelationshipPropertyScanPage {
                    relationships,
                    last: Some(last),
                    exhausted: false,
                });
            }
            if request.accepts(&entry.0) {
                relationships.push(relationship.clone());
            }
        }
        *self.ordered_relationship_scan_rows.lock().unwrap() += relationships.len();
        Ok(OrderedRelationshipPropertyScanPage {
            relationships,
            last: None,
            exhausted: true,
        })
    }

    async fn hydrate_vertices(&mut self, vertex_ids: &[VertexId]) -> StorageResult<Vec<VertexRecord>> {
        self.hydration_requests.lock().unwrap().push(vertex_ids.to_vec());
        Ok(vertex_ids.iter().rev().filter_map(|id| self.vertices.get(id).cloned()).collect())
    }

    /// Stands in for a backend that can walk a property index under a budget:
    /// a property named selective answers with exactly its matching vertices,
    /// anything else is treated as too broad and declines.
    async fn probe_selective_equality(&mut self, candidates: &[PropertyEqualityCandidate]) -> StorageResult<Option<Vec<VertexId>>> {
        self.probe_requests.lock().unwrap().push(candidates.to_vec());
        let Some(candidate) = candidates
            .iter()
            .find(|candidate| self.selective_properties.contains(&candidate.property))
        else {
            return Ok(None);
        };
        if self.probe_answers_empty {
            return Ok(Some(Vec::new()));
        }
        Ok(Some(
            self.vertices
                .values()
                .filter(|vertex| {
                    vertex
                        .properties
                        .get(candidate.property.as_str())
                        .is_some_and(|stored| candidate.values.iter().any(|value| scalar_values_equal(stored, value)))
                })
                .map(|vertex| vertex.id)
                .collect(),
        ))
    }
}

#[async_trait]
impl GraphStorage for MemoryStorage {
    async fn begin_read(&self, _request: ReadRequest) -> StorageResult<Box<dyn GraphRead + '_>> {
        *self.read_sessions.lock().unwrap() += 1;
        Ok(Box::new(MemoryRead {
            checkpoint: self.checkpoint.clone(),
            vertices: &self.vertices,
            relationships: &self.relationships,
            expand_requests: Arc::clone(&self.expand_requests),
            expanded_relationship_records: Arc::clone(&self.expanded_relationship_records),
            without_neighbor_listing: self.without_neighbor_listing,
            answers_relationship_probes: self.answers_relationship_probes,
            relationship_probe_requests: Arc::clone(&self.relationship_probe_requests),
            hydration_requests: Arc::clone(&self.hydration_requests),
            label_scan_limits: Arc::clone(&self.label_scan_limits),
            seek_values: Arc::clone(&self.seek_values),
            relationship_seek_values: Arc::clone(&self.relationship_seek_values),
            ordered_scan_requests: Arc::clone(&self.ordered_scan_requests),
            ordered_scan_entries: Arc::clone(&self.ordered_scan_entries),
            statistics_access: Arc::clone(&self.statistics_access),
            bounded_seek_limits: Arc::clone(&self.bounded_seek_limits),
            ordered_page_size: self.ordered_page_size,
            max_intermediate_rows: self.max_intermediate_rows,
            max_query_index_candidates: self.max_query_index_candidates,
            selective_properties: self.selective_properties.clone(),
            probe_answers_empty: self.probe_answers_empty,
            probe_requests: Arc::clone(&self.probe_requests),
            ordered_relationship_scan_requests: Arc::clone(&self.ordered_relationship_scan_requests),
            ordered_relationship_scan_rows: Arc::clone(&self.ordered_relationship_scan_rows),
            completion_conditions: Vec::new(),
        }))
    }
}

pub fn entity(id: VertexId, label: &str, entity_id: &str) -> VertexRecord {
    VertexRecord::new(id)
        .with_label(label)
        .with_property("entity_id", ScalarValue::String(entity_id.into()))
}
