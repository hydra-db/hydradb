//! Vertex and edge identity across declared tables.
//!
//! The kernels identify vertices and edges by `i64`. The loader gives every
//! `(table label, key)` a dense internal id: tables in label order, rows in key
//! order. This module turns seeds into internal ids and results back.

use std::fmt;
use std::rc::Rc;

use hydradb_pg_graph_core::GraphError;
use pgrx::pg_sys;
use serde_json::Value;

/// The type of a key column, which decides how its keys are read and stored.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum KeyKind {
    Integer,
    /// Every other type, read as the text PostgreSQL prints for it.
    Text,
}

impl KeyKind {
    pub fn of(type_oid: pg_sys::Oid) -> Self {
        if [pg_sys::INT2OID, pg_sys::INT4OID, pg_sys::INT8OID].contains(&type_oid) {
            Self::Integer
        } else {
            Self::Text
        }
    }
}

/// A key as stored: integer key columns as numbers, every other type as the
/// text PostgreSQL prints for it. Integers order by value and text bytewise,
/// so the order does not depend on collation.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Key {
    Int(i64),
    Text(Rc<str>),
}

impl Key {
    /// A key from the text PostgreSQL prints for it.
    pub fn parse(kind: KeyKind, text: String) -> Option<Self> {
        match kind {
            KeyKind::Integer => text.parse().ok().map(Self::Int),
            KeyKind::Text => Some(Self::Text(text.into())),
        }
    }

    /// The text PostgreSQL prints for the key, as the adjacency tables store it.
    pub fn text(&self) -> String {
        match self {
            Self::Int(n) => n.to_string(),
            Self::Text(s) => s.to_string(),
        }
    }

    pub fn to_json(&self) -> Value {
        match self {
            Self::Int(n) => (*n).into(),
            Self::Text(s) => (**s).into(),
        }
    }
}

impl fmt::Display for Key {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Int(n) => write!(f, "{n}"),
            Self::Text(s) => write!(f, "{s:?}"),
        }
    }
}

#[derive(Clone)]
pub struct Label {
    pub name: String,
    pub kind: KeyKind,
}

/// The `(label, key)` pairs seeds name, as `{"label": key}` or
/// `{"label": [key, ...]}`; the JSON type must match the key column: a JSON
/// integer for integer keys, a JSON string, spelled as PostgreSQL prints it,
/// for every other type. Labels are indexes into `labels`.
pub fn parse_seeds(labels: &[Label], seeds: &Value) -> Result<Vec<(u16, Key)>, GraphError> {
    let seeds = seeds
        .as_object()
        .ok_or_else(|| GraphError::Source("seeds must be a JSON object mapping vertex labels to keys".into()))?;
    let mut parsed = Vec::new();
    for (name, keys) in seeds {
        let label = (0..labels.len() as u16)
            .find(|&label| labels[label as usize].name == *name)
            .ok_or_else(|| GraphError::Source(format!("unknown vertex label {name:?}")))?;
        let integer = labels[label as usize].kind == KeyKind::Integer;
        for value in keys.as_array().map_or(std::slice::from_ref(keys), Vec::as_slice) {
            let key = match (integer, value) {
                (true, Value::Number(n)) => n.as_i64().map(Key::Int),
                (false, Value::String(s)) => Some(Key::Text(s.as_str().into())),
                _ => None,
            }
            .ok_or_else(|| {
                let expected = if integer { "a JSON integer" } else { "a JSON string" };
                GraphError::Source(format!("{value} is not a key of vertex table {name}; pass {expected}"))
            })?;
            parsed.push((label, key));
        }
    }
    Ok(parsed)
}

/// A seed as the caller named it, and its vertex if the graph has one.
pub struct Seed {
    pub label: String,
    pub key: Key,
    pub id: Option<i64>,
}
