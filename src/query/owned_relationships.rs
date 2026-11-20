use crate::{validate_component, GraphError, OwnedRelationshipSetRequest, QueryFailureReason, Result};

pub(crate) const MAX_OWNED_RELATIONSHIPS: usize = 1000;
pub(crate) const SET_ID_PROPERTY: &str = "__hydradb_owned_rel_set_key";
pub(crate) const MAX_OWNED_MUTATION_BYTES: usize = 8 * 1024 * 1024;
const MAX_REQUEST_BYTES: usize = 1024 * 1024;

pub(crate) fn invalid(feature: impl Into<String>) -> GraphError {
    GraphError::UnsupportedQuery {
        reason: QueryFailureReason::Mutation,
        dialect: "NativeRelationshipMutation",
        feature: feature.into(),
    }
}

pub(crate) fn is_owned_relationship_set_call(query: &str) -> Result<bool> {
    use super::path_procedure::{lex, Token};
    const PREFIX: &str = "call hydra.replace_owned_relationship_set";
    if !query
        .trim_start()
        .get(..PREFIX.len())
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case(PREFIX))
    {
        return Ok(false);
    }
    let mut tokens = lex(query)?;
    if tokens.last() == Some(&Token::Semicolon) {
        tokens.pop();
    }
    match tokens.as_slice() {
        [Token::Identifier(call), Token::Identifier(namespace), Token::Dot, Token::Identifier(name), Token::LeftParen, Token::Parameter(parameter), Token::RightParen]
            if call.eq_ignore_ascii_case("call")
                && namespace.eq_ignore_ascii_case("hydra")
                && name.eq_ignore_ascii_case("replace_owned_relationship_set")
                && parameter == "request" =>
        {
            Ok(true)
        }
        _ => Err(invalid(
            "expected CALL hydra.replace_owned_relationship_set($request) without other clauses",
        )),
    }
}

impl OwnedRelationshipSetRequest {
    pub(crate) fn mutation_bytes(&self) -> usize {
        let fields = &self.properties.properties;
        let dynamic = fields.iter().fold(0usize, |bytes, (key, value)| {
            bytes.saturating_add(key.len()).saturating_add(value.estimated_heap_bytes())
        });
        let per_edge = 1024usize.saturating_add(
            8usize.saturating_mul(
                dynamic
                    .saturating_add(self.owner.len())
                    .saturating_add(self.owner_property.len())
                    .saturating_add(self.edge_type.len().saturating_mul(fields.len().saturating_add(4)))
                    .saturating_add(SET_ID_PROPERTY.len())
                    .saturating_add(128),
            ),
        );
        per_edge.saturating_mul(self.targets.len())
    }

    pub(crate) fn validate(&self) -> Result<()> {
        validate_component("edge_type", &self.edge_type)?;
        validate_component("owner_property", &self.owner_property)?;
        if !self.complete || self.version <= 0 {
            return Err(invalid("a complete set and positive signed 64-bit version are required"));
        }
        for (name, value, limit) in [
            ("owner", self.owner.as_str(), 256),
            ("operation_id", self.operation_id.as_str(), 128),
            ("content_hash", self.content_hash.as_str(), 128),
        ] {
            if value.is_empty() || value.len() > limit {
                return Err(invalid(format!("{name} must contain 1..={limit} bytes")));
            }
        }
        if self.targets.len() > MAX_OWNED_RELATIONSHIPS {
            return Err(invalid("owned relationship sets contain at most 1000 targets"));
        }
        if self.owner_property == "id"
            || self.owner_property == SET_ID_PROPERTY
            || self.properties.properties.contains_key("id")
            || self.properties.properties.contains_key(SET_ID_PROPERTY)
            || self.properties.properties.contains_key(&self.owner_property)
            || self.properties.properties.len() > 16
        {
            return Err(invalid(
                "set properties cannot contain reserved identity or owner properties and contain at most 16 fields",
            ));
        }
        hydradb_graph_slate::write::validate_edge_metadata(&self.properties)?;
        let mut vertices = std::collections::BTreeSet::new();
        let mut bytes =
            self.edge_type.len() + self.owner_property.len() + self.owner.len() + self.operation_id.len() + self.content_hash.len();
        for (key, value) in &self.properties.properties {
            bytes = bytes.saturating_add(key.len()).saturating_add(value.estimated_heap_bytes());
        }
        for constraints in std::iter::once(&self.source_constraints).chain(self.targets.iter().map(|target| &target.constraints)) {
            if constraints.labels.is_empty()
                || constraints.labels.len() > 4
                || constraints.properties.is_empty()
                || constraints.properties.len() > 16
            {
                return Err(invalid("each endpoint requires 1..=4 labels and 1..=16 exact property constraints"));
            }
            hydradb_graph_slate::write::validate_vertex_metadata(constraints)?;
            for label in &constraints.labels {
                bytes = bytes.saturating_add(label.len());
            }
            for (key, value) in &constraints.properties {
                bytes = bytes.saturating_add(key.len()).saturating_add(value.estimated_heap_bytes());
            }
        }
        if bytes > MAX_REQUEST_BYTES {
            return Err(invalid("owned relationship set request exceeds 1 MiB"));
        }
        if self.mutation_bytes() > MAX_OWNED_MUTATION_BYTES {
            return Err(invalid("owned relationship set projected mutation exceeds 8 MiB"));
        }
        for target in &self.targets {
            if !vertices.insert(target.vertex) {
                return Err(invalid("target vertices must be distinct"));
            }
        }
        Ok(())
    }
}

#[cfg(feature = "client-api")]
pub(crate) fn resolve(parameters: &std::collections::BTreeMap<String, crate::QueryParameterValue>) -> Result<OwnedRelationshipSetRequest> {
    use crate::{EdgeMetadata, OwnedRelationshipTarget, QueryParameterValue, VertexMetadata, VertexPropertyValue};
    use serde::Deserialize;
    use std::collections::{BTreeMap, BTreeSet};

    #[derive(Deserialize)]
    #[serde(untagged)]
    enum Scalar {
        String(String),
        Bool(bool),
        Signed(i64),
        Unsigned(u64),
    }
    impl From<Scalar> for VertexPropertyValue {
        fn from(value: Scalar) -> Self {
            match value {
                Scalar::String(value) => Self::String(value),
                Scalar::Bool(value) => Self::Bool(value),
                Scalar::Signed(value) if value >= 0 => Self::Integer(value as u64),
                Scalar::Signed(value) => Self::SignedInteger(value),
                Scalar::Unsigned(value) => Self::Integer(value),
            }
        }
    }
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Constraints {
        labels: BTreeSet<String>,
        properties: BTreeMap<String, Scalar>,
    }
    impl From<Constraints> for VertexMetadata {
        fn from(value: Constraints) -> Self {
            Self {
                labels: value.labels,
                properties: value.properties.into_iter().map(|(key, value)| (key, value.into())).collect(),
            }
        }
    }
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Target {
        vertex: u64,
        constraints: Constraints,
    }
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Request {
        source: u64,
        source_constraints: Constraints,
        edge_type: String,
        owner_property: String,
        owner: String,
        properties: BTreeMap<String, Scalar>,
        version: i64,
        operation_id: String,
        content_hash: String,
        complete: bool,
        targets: Vec<Target>,
    }
    fn json(value: &QueryParameterValue, depth: usize, bytes: &mut usize) -> Result<serde_json::Value> {
        if depth > 16 {
            return Err(invalid("request nesting exceeds 16 levels"));
        }
        *bytes = bytes.saturating_add(match value {
            QueryParameterValue::Scalar(VertexPropertyValue::String(value)) => value.len(),
            QueryParameterValue::Map(values) => values
                .keys()
                .map(String::len)
                .sum::<usize>()
                .saturating_add(values.len().saturating_mul(16)),
            QueryParameterValue::List(values) => values.len().saturating_mul(8),
            _ => 8,
        });
        if *bytes > MAX_REQUEST_BYTES {
            return Err(invalid("owned relationship set request exceeds 1 MiB"));
        }
        Ok(match value {
            QueryParameterValue::Scalar(VertexPropertyValue::String(value)) => value.clone().into(),
            QueryParameterValue::Scalar(VertexPropertyValue::Bool(value)) => (*value).into(),
            QueryParameterValue::Scalar(VertexPropertyValue::Integer(value)) => (*value).into(),
            QueryParameterValue::Scalar(VertexPropertyValue::SignedInteger(value)) => (*value).into(),
            QueryParameterValue::Map(values) => serde_json::Value::Object(
                values
                    .iter()
                    .map(|(key, value)| Ok((key.clone(), json(value, depth + 1, bytes)?)))
                    .collect::<Result<_>>()?,
            ),
            QueryParameterValue::List(values) => {
                serde_json::Value::Array(values.iter().map(|value| json(value, depth + 1, bytes)).collect::<Result<_>>()?)
            }
            _ => return Err(invalid("request values must be strings, booleans, integers, lists or maps")),
        })
    }
    let value = parameters.get("request").ok_or_else(|| invalid("request parameter is required"))?;
    let QueryParameterValue::Map(fields) = value else {
        return Err(invalid("request must be a map"));
    };
    if parameters.len() != 1 || fields.len() != 11 {
        return Err(invalid("owned relationship set requires exactly its declared request fields"));
    }
    if !matches!(fields.get("targets"), Some(QueryParameterValue::List(values)) if values.len() <= MAX_OWNED_RELATIONSHIPS) {
        return Err(invalid("owned relationship sets contain at most 1000 targets"));
    }
    let wire: Request =
        serde_json::from_value(json(value, 0, &mut 0)?).map_err(|_| invalid("request does not match the owned relationship set schema"))?;
    let request = OwnedRelationshipSetRequest {
        source: wire.source,
        source_constraints: wire.source_constraints.into(),
        edge_type: wire.edge_type,
        owner_property: wire.owner_property,
        owner: wire.owner,
        properties: EdgeMetadata {
            properties: wire.properties.into_iter().map(|(key, value)| (key, value.into())).collect(),
        },
        version: wire.version,
        operation_id: wire.operation_id,
        content_hash: wire.content_hash,
        complete: wire.complete,
        targets: wire
            .targets
            .into_iter()
            .map(|target| OwnedRelationshipTarget {
                vertex: target.vertex,
                constraints: target.constraints.into(),
            })
            .collect(),
    };
    request.validate()?;
    Ok(request)
}
