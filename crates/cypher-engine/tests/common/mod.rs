use async_trait::async_trait;
use hydradb_cypher_engine::{
    scalar_values_equal, CypherEngine, ExpandDirection, ExpandRequest, GraphRead, GraphStorage,
    OrderedPropertyScanPage, OrderedPropertyScanRequest, ParameterMap, QueryValue, ReadRequest,
    RelationshipRecord, ScalarValue, StorageResult, VertexId, VertexRecord,
};

pub struct MemoryGraph {
    vertices: Vec<VertexRecord>,
    relationships: Vec<RelationshipRecord>,
}

impl Default for MemoryGraph {
    fn default() -> Self {
        Self {
            vertices: [(1, "Alice"), (2, "Bob")]
                .into_iter()
                .map(|(id, name)| {
                    VertexRecord::new(id)
                        .with_label("Person")
                        .with_property("name", ScalarValue::String(name.into()))
                })
                .collect(),
            relationships: vec![RelationshipRecord::new(1, 1, 2, "KNOWS")
                .with_property("name", ScalarValue::String("friend".into()))],
        }
    }
}

#[allow(clippy::double_must_use)]
#[async_trait]
impl GraphStorage for MemoryGraph {
    async fn begin_read(&self, _: ReadRequest) -> StorageResult<Box<dyn GraphRead + '_>> {
        Ok(Box::new(MemoryRead(self)))
    }
}

struct MemoryRead<'a>(&'a MemoryGraph);

#[allow(clippy::double_must_use)]
#[async_trait]
impl GraphRead for MemoryRead<'_> {
    async fn seek_vertices_by_property(
        &mut self,
        property: &str,
        value: &ScalarValue,
    ) -> StorageResult<Vec<VertexId>> {
        Ok(self
            .0
            .vertices
            .iter()
            .filter(|v| {
                v.properties
                    .get(property)
                    .is_some_and(|p| scalar_values_equal(p, value))
            })
            .map(|v| v.id)
            .collect())
    }

    async fn scan_vertices_by_property(&mut self, property: &str) -> StorageResult<Vec<VertexId>> {
        Ok(self
            .0
            .vertices
            .iter()
            .filter(|v| v.properties.contains_key(property))
            .map(|v| v.id)
            .collect())
    }

    async fn scan_vertices_by_property_ordered(
        &mut self,
        _: &OrderedPropertyScanRequest,
    ) -> StorageResult<OrderedPropertyScanPage> {
        panic!("these queries should sort after projection")
    }

    async fn scan_vertices_by_label(
        &mut self,
        label: &str,
        max_results: Option<usize>,
    ) -> StorageResult<Vec<VertexId>> {
        Ok(self
            .0
            .vertices
            .iter()
            .filter(|v| v.labels.contains(label))
            .take(max_results.unwrap_or(usize::MAX))
            .map(|v| v.id)
            .collect())
    }

    async fn scan_all_vertices(&mut self) -> StorageResult<Vec<VertexId>> {
        Ok(self.0.vertices.iter().map(|v| v.id).collect())
    }

    async fn seek_relationships_by_property(
        &mut self,
        relationship_type: &str,
        property: &str,
        value: &ScalarValue,
    ) -> StorageResult<Vec<RelationshipRecord>> {
        Ok(self
            .0
            .relationships
            .iter()
            .filter(|r| {
                r.relationship_type == relationship_type
                    && r.properties
                        .get(property)
                        .is_some_and(|p| scalar_values_equal(p, value))
            })
            .cloned()
            .collect())
    }

    async fn expand_relationships(
        &mut self,
        request: &ExpandRequest,
    ) -> StorageResult<Vec<RelationshipRecord>> {
        Ok(self
            .0
            .relationships
            .iter()
            .filter(|r| {
                let input = match request.direction {
                    ExpandDirection::Outgoing => r.source,
                    ExpandDirection::Incoming => r.target,
                };
                request.input_vertex_ids.contains(&input)
                    && (request.relationship_types.is_empty()
                        || request.relationship_types.contains(&r.relationship_type))
            })
            .cloned()
            .collect())
    }

    async fn hydrate_vertices(&mut self, ids: &[VertexId]) -> StorageResult<Vec<VertexRecord>> {
        Ok(self
            .0
            .vertices
            .iter()
            .filter(|v| ids.contains(&v.id))
            .cloned()
            .collect())
    }
}

pub fn query(source: &str) -> Vec<Vec<QueryValue>> {
    let engine = CypherEngine::new(MemoryGraph::default());
    futures::executor::block_on(engine.execute(source, &ParameterMap::new()))
        .unwrap_or_else(|error| panic!("{source}: {error}"))
        .rows
        .into_iter()
        .map(|row| row.values)
        .collect()
}

pub fn text(value: &str) -> QueryValue {
    QueryValue::Scalar(ScalarValue::String(value.into()))
}
