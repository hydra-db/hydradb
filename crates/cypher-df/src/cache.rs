use std::collections::HashMap;
use std::sync::Arc;

use hydradb_graph_plan::MutationPolicy;

/// All compile-time inputs that can change a bound template's structure.
/// Request values, snapshots, authorization grants, mutation sessions and
/// execution results deliberately have no representation in this key.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct TemplateKey {
    pub dialect: String,
    pub query_fingerprint: [u8; 32],
    pub parameter_types: Vec<(String, ParameterType)>,
    pub schema_revision: u64,
    pub semantic_codec_version: u32,
    pub capability_fingerprint: [u8; 32],
    pub mutation_policy: MutationPolicy,
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum ParameterType {
    Null,
    Missing,
    Boolean,
    Integer,
    Float,
    Decimal,
    String,
    Bytes,
    List,
    Map,
    Record,
    Node,
    Relationship,
    Path,
    Temporal,
    Spatial,
    Extension,
}

impl ParameterType {
    pub fn of_value(value: &hydradb_graph_types::CypherValue) -> Self {
        use hydradb_graph_types::CypherValue;
        match value {
            CypherValue::Null => Self::Null,
            CypherValue::Missing => Self::Missing,
            CypherValue::Boolean(_) => Self::Boolean,
            CypherValue::Integer(_) => Self::Integer,
            CypherValue::Float(_) => Self::Float,
            CypherValue::Decimal(_) => Self::Decimal,
            CypherValue::String(_) => Self::String,
            CypherValue::Bytes(_) => Self::Bytes,
            CypherValue::List(_) => Self::List,
            CypherValue::Map(_) => Self::Map,
            CypherValue::Record(_) => Self::Record,
            CypherValue::Node(_) | CypherValue::NodeEntity(_) => Self::Node,
            CypherValue::Relationship(_) | CypherValue::RelationshipEntity(_) => Self::Relationship,
            CypherValue::Path(_) => Self::Path,
            CypherValue::Date(_)
            | CypherValue::LocalTime(_)
            | CypherValue::Time(_)
            | CypherValue::LocalDateTime(_)
            | CypherValue::DateTime(_)
            | CypherValue::Duration(_)
            | CypherValue::Temporal(_) => Self::Temporal,
            CypherValue::Point(_) => Self::Spatial,
            CypherValue::Extension(_) => Self::Extension,
        }
    }
}

impl TemplateKey {
    pub fn for_parameters(
        dialect: impl Into<String>,
        query: &str,
        parameters: &std::collections::BTreeMap<String, hydradb_graph_types::CypherValue>,
        schema_revision: u64,
        semantic_codec_version: u32,
        capability_fingerprint: [u8; 32],
        mutation_policy: MutationPolicy,
    ) -> Self {
        Self::new(
            dialect,
            query,
            parameters
                .iter()
                .map(|(name, value)| (name.clone(), ParameterType::of_value(value))),
            schema_revision,
            semantic_codec_version,
            capability_fingerprint,
            mutation_policy,
        )
    }

    pub fn new(
        dialect: impl Into<String>,
        query: &str,
        parameter_types: impl IntoIterator<Item = (String, ParameterType)>,
        schema_revision: u64,
        semantic_codec_version: u32,
        capability_fingerprint: [u8; 32],
        mutation_policy: MutationPolicy,
    ) -> Self {
        use sha2::{Digest, Sha256};

        let query_fingerprint: [u8; 32] = Sha256::digest(query.as_bytes()).into();
        let mut parameter_types: Vec<_> = parameter_types.into_iter().collect();
        parameter_types.sort_by(|left, right| left.0.cmp(&right.0));
        Self {
            dialect: dialect.into(),
            query_fingerprint,
            parameter_types,
            schema_revision,
            semantic_codec_version,
            capability_fingerprint,
            mutation_policy,
        }
    }
}

/// Bounded least-recently-used cache for immutable prepared templates.
#[derive(Debug)]
pub struct TemplateCache<V> {
    entries: HashMap<TemplateKey, (Arc<V>, u64)>,
    clock: u64,
    capacity: usize,
}

impl<V> TemplateCache<V> {
    pub fn new(capacity: usize) -> Self {
        Self {
            entries: HashMap::with_capacity(capacity),
            clock: 0,
            capacity,
        }
    }

    pub fn get(&mut self, key: &TemplateKey) -> Option<Arc<V>> {
        let used = self.tick();
        self.entries.get_mut(key).map(|(value, last_used)| {
            *last_used = used;
            Arc::clone(value)
        })
    }

    pub fn insert(&mut self, key: TemplateKey, value: Arc<V>) {
        if self.capacity == 0 {
            return;
        }
        if self.entries.len() >= self.capacity && !self.entries.contains_key(&key) {
            if let Some(oldest) = self
                .entries
                .iter()
                .min_by_key(|(_, (_, last_used))| *last_used)
                .map(|(key, _)| key.clone())
            {
                self.entries.remove(&oldest);
            }
        }
        let used = self.tick();
        self.entries.insert(key, (value, used));
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    fn tick(&mut self) -> u64 {
        self.clock = self.clock.wrapping_add(1);
        self.clock
    }
}
