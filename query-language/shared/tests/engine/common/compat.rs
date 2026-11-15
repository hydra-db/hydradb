//! The parity matrix's `compat` cell as engine test storage.
//!
//! `query-language/cypher/parity/legacy-read-parity.json` seeds the same graph into a real
//! shard, and `just neo4j-oracle` holds its rows to Neo4j. Testing the engine
//! against the same vertices and edges keeps the two levels describing one
//! graph: an answer pinned here is the answer the matrix row records.

use hydradb_cypher_engine::{
    CypherEngine, ListParameterMap, ParameterMap, QueryResult, QueryValue, RelationshipRecord, ScalarValue, VertexRecord,
};

use super::MemoryStorage;

fn item(id: u64, name: Option<&str>, score: Option<i128>, kind: &str) -> VertexRecord {
    let mut vertex = VertexRecord::new(id)
        .with_label("Item")
        .with_property("kind", ScalarValue::String(kind.into()));
    if let Some(name) = name {
        vertex = vertex.with_property("name", ScalarValue::String(name.into()));
    }
    if let Some(score) = score {
        vertex = vertex.with_property("score", ScalarValue::Integer(score));
    }
    vertex
}

fn named(id: u64, label: &str, name: &str) -> VertexRecord {
    VertexRecord::new(id)
        .with_label(label)
        .with_property("name", ScalarValue::String(name.into()))
}

/// Items with missing and duplicate values, a hub with weighted edges to three
/// of them, and `R` edges forming a diamond a->{b,c}->d, a cycle d->a, and a
/// self-loop on `loop`.
pub fn graph() -> MemoryStorage {
    let vertices = [
        item(1000, Some("apple"), Some(3), "fruit"),
        item(1001, Some("banana"), Some(1), "fruit"),
        item(1002, Some("carrot"), Some(2), "veg"),
        item(1003, None, Some(5), "veg"),
        item(1004, Some("apple"), Some(0), "fruit"),
        item(1005, Some("Durian"), None, "fruit"),
        named(1010, "Hub", "hub"),
        named(1020, "Node", "a"),
        named(1021, "Node", "b"),
        named(1022, "Node", "c"),
        named(1023, "Node", "d"),
        named(1024, "Node", "loop"),
    ];
    let weighted =
        |id, target, weight| RelationshipRecord::new(id, 1010, target, "HAS").with_property("weight", ScalarValue::Integer(weight));
    let relationships = [
        weighted(1, 1000, 2),
        weighted(2, 1001, 1),
        weighted(3, 1002, 3),
        RelationshipRecord::new(4, 1020, 1021, "R"),
        RelationshipRecord::new(5, 1020, 1022, "R"),
        RelationshipRecord::new(6, 1021, 1023, "R"),
        RelationshipRecord::new(7, 1022, 1023, "R"),
        RelationshipRecord::new(8, 1023, 1020, "R"),
        RelationshipRecord::new(9, 1024, 1024, "R"),
    ];
    MemoryStorage::with_graph(vertices, relationships)
}

pub async fn run(query: &str) -> QueryResult {
    run_with(query, &ParameterMap::new(), &ListParameterMap::new()).await
}

pub async fn run_with(query: &str, parameters: &ParameterMap, lists: &ListParameterMap) -> QueryResult {
    let engine = CypherEngine::new(graph());
    let prepared = engine
        .prepare_with_lists(query, parameters, lists)
        .unwrap_or_else(|error| panic!("{query:?} failed to prepare: {error}"));
    engine
        .execute_prepared(&prepared)
        .await
        .unwrap_or_else(|error| panic!("{query:?} failed: {error}"))
}

/// The error a query is refused with, from preparation or execution.
pub async fn refusal(query: &str) -> String {
    let engine = CypherEngine::new(graph());
    let error = match engine.prepare_with_lists(query, &ParameterMap::new(), &ListParameterMap::new()) {
        Err(error) => error,
        Ok(prepared) => match engine.execute_prepared(&prepared).await {
            Err(error) => error,
            Ok(result) => panic!("{query:?} was expected to be refused, and returned {:?}", result.rows),
        },
    };
    error.to_string()
}

pub fn columns(result: &QueryResult) -> Vec<&str> {
    result.columns.iter().map(|column| column.name.as_str()).collect()
}

/// Every row as display strings: `"apple"` for a string, `3` for an
/// integer or count, `null`, `#1000` for a vertex, `[..]` for a list.
pub fn table(result: &QueryResult) -> Vec<Vec<String>> {
    result.rows.iter().map(|row| row.values.iter().map(show).collect()).collect()
}

pub fn show(value: &QueryValue) -> String {
    match value {
        QueryValue::Null => "null".to_string(),
        QueryValue::VertexId(id) => format!("#{id}"),
        QueryValue::RelationshipId(id) => format!("rel#{id}"),
        QueryValue::Count(count) => count.to_string(),
        QueryValue::Scalar(ScalarValue::String(value)) => format!("{value:?}"),
        QueryValue::Scalar(ScalarValue::Integer(value)) => value.to_string(),
        QueryValue::Scalar(ScalarValue::Float(value)) => format!("{value}f"),
        QueryValue::Scalar(ScalarValue::Boolean(value)) => value.to_string(),
        QueryValue::Scalar(ScalarValue::Null) => "null".to_string(),
        QueryValue::Scalar(ScalarValue::Date(value)) => format!("date({value})"),
        QueryValue::Scalar(ScalarValue::List(values)) => format!(
            "[{}]",
            values
                .iter()
                .map(|value| show(&QueryValue::Scalar(value.clone())))
                .collect::<Vec<_>>()
                .join(", ")
        ),
        QueryValue::Scalar(ScalarValue::Decimal(value)) => format!("{value}M"),
        QueryValue::Scalar(ScalarValue::Bytes(value)) => {
            format!("X'{}'", hydradb_cypher_ast::bytes::hex(value))
        }
        QueryValue::List(values) => format!("[{}]", values.iter().map(show).collect::<Vec<_>>().join(", ")),
        // `<#a, rel#e, #b>`: nodes and edges in the path's order.
        QueryValue::Path(path) => {
            let mut elements = Vec::new();
            for (index, node) in path.nodes.iter().enumerate() {
                elements.push(format!("#{}", node.id));
                if let Some(relationship) = path.relationships.get(index) {
                    elements.push(format!("rel#{}", relationship.id));
                }
            }
            format!("<{}>", elements.join(", "))
        }
        QueryValue::Temporal(value) => format!("{} '{}'", value.literal_keyword(), value.to_literal_string()),
        QueryValue::Record(fields) => format!(
            "{{{}}}",
            fields
                .iter()
                .map(|(name, value)| format!("{name}: {}", show(value)))
                .collect::<Vec<_>>()
                .join(", ")
        ),
    }
}

/// Build an expected table from string literals.
#[macro_export]
macro_rules! rows {
    ($([$($cell:expr),* $(,)?]),* $(,)?) => {
        vec![$(vec![$($cell.to_string()),*]),*] as Vec<Vec<String>>
    };
}
