use std::fmt::{Display, Formatter};

use crate::{validate_component, GraphError, Result};

pub const DEFAULT_NAMESPACE_ID: &str = "default";
pub const DEFAULT_GRAPH_ID: &str = "default";
pub const MAX_NAMESPACE_DEPTH: usize = 8;

#[cfg_attr(
    feature = "query-transport",
    derive(serde::Deserialize, serde::Serialize)
)]
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[cfg_attr(
    feature = "query-transport",
    serde(try_from = "String", into = "String")
)]
pub struct NamespaceId(String);

impl NamespaceId {
    pub fn new(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        validate_component("namespace_id", &value)?;
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Default for NamespaceId {
    fn default() -> Self {
        Self(DEFAULT_NAMESPACE_ID.to_string())
    }
}

impl Display for NamespaceId {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl TryFrom<String> for NamespaceId {
    type Error = GraphError;

    fn try_from(value: String) -> Result<Self> {
        Self::new(value)
    }
}

impl From<NamespaceId> for String {
    fn from(value: NamespaceId) -> Self {
        value.0
    }
}

#[cfg_attr(
    feature = "query-transport",
    derive(serde::Deserialize, serde::Serialize)
)]
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[cfg_attr(
    feature = "query-transport",
    serde(try_from = "String", into = "String")
)]
pub struct GraphId(String);

impl GraphId {
    pub fn new(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        validate_component("graph_id", &value)?;
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Default for GraphId {
    fn default() -> Self {
        Self(DEFAULT_GRAPH_ID.to_string())
    }
}

impl Display for GraphId {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl TryFrom<String> for GraphId {
    type Error = GraphError;

    fn try_from(value: String) -> Result<Self> {
        Self::new(value)
    }
}

impl From<GraphId> for String {
    fn from(value: GraphId) -> Self {
        value.0
    }
}

#[cfg_attr(
    feature = "query-transport",
    derive(serde::Deserialize, serde::Serialize)
)]
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[cfg_attr(
    feature = "query-transport",
    serde(try_from = "Vec<NamespaceId>", into = "Vec<NamespaceId>")
)]
pub struct NamespacePath {
    segments: Vec<NamespaceId>,
}

impl NamespacePath {
    pub fn root(namespace_id: NamespaceId) -> Self {
        Self {
            segments: vec![namespace_id],
        }
    }

    pub fn new(segments: impl IntoIterator<Item = NamespaceId>) -> Result<Self> {
        let segments: Vec<_> = segments.into_iter().collect();
        if segments.is_empty() {
            return Err(GraphError::CorruptValue {
                key: "namespace_path".to_string(),
                reason: "namespace path must contain a tenant namespace".to_string(),
            });
        }
        if segments.len() > MAX_NAMESPACE_DEPTH {
            return Err(GraphError::AdmissionRejected {
                operation: "namespace_depth",
                actual: segments.len() as u64,
                limit: MAX_NAMESPACE_DEPTH as u64,
            });
        }
        Ok(Self { segments })
    }

    pub fn child(&self, namespace_id: NamespaceId) -> Result<Self> {
        let mut segments = self.segments.clone();
        segments.push(namespace_id);
        Self::new(segments)
    }

    pub fn tenant_id(&self) -> &NamespaceId {
        &self.segments[0]
    }

    pub fn leaf(&self) -> &NamespaceId {
        self.segments
            .last()
            .expect("namespace paths always contain a root")
    }

    pub fn depth(&self) -> usize {
        self.segments.len()
    }

    pub fn segments(&self) -> &[NamespaceId] {
        &self.segments
    }

    pub fn ancestors_inclusive(&self) -> impl DoubleEndedIterator<Item = NamespacePath> + '_ {
        (1..=self.segments.len()).map(|len| NamespacePath {
            segments: self.segments[..len].to_vec(),
        })
    }

    pub fn is_descendant_of(&self, ancestor: &NamespacePath) -> bool {
        self.segments.starts_with(&ancestor.segments)
    }

    fn storage_suffix(&self) -> String {
        let mut suffix = format!("namespaces/{}", self.segments[0]);
        for segment in &self.segments[1..] {
            suffix.push_str("/subnamespaces/");
            suffix.push_str(segment.as_str());
        }
        suffix
    }
}

impl Default for NamespacePath {
    fn default() -> Self {
        Self::root(NamespaceId::default())
    }
}

impl Display for NamespacePath {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        for (index, segment) in self.segments.iter().enumerate() {
            if index > 0 {
                formatter.write_str("/")?;
            }
            Display::fmt(segment, formatter)?;
        }
        Ok(())
    }
}

impl TryFrom<Vec<NamespaceId>> for NamespacePath {
    type Error = GraphError;

    fn try_from(segments: Vec<NamespaceId>) -> Result<Self> {
        Self::new(segments)
    }
}

impl From<NamespacePath> for Vec<NamespaceId> {
    fn from(value: NamespacePath) -> Self {
        value.segments
    }
}

#[cfg_attr(
    feature = "query-transport",
    derive(serde::Deserialize, serde::Serialize)
)]
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct GraphScope {
    pub namespace: NamespacePath,
    pub graph_id: GraphId,
}

impl GraphScope {
    pub fn new(namespace: NamespacePath, graph_id: GraphId) -> Self {
        Self {
            namespace,
            graph_id,
        }
    }

    pub fn tenant(namespace_id: NamespaceId, graph_id: GraphId) -> Self {
        Self::new(NamespacePath::root(namespace_id), graph_id)
    }

    pub fn scoped_store_path(&self, base_path: &str) -> String {
        let base_path = base_path.trim_end_matches('/');
        let scoped_path = format!(
            "{}/graphs/{}",
            self.namespace.storage_suffix(),
            self.graph_id
        );
        if base_path.is_empty() {
            scoped_path
        } else {
            format!("{base_path}/{scoped_path}")
        }
    }

    pub fn is_default(&self) -> bool {
        self == &Self::default()
    }
}

impl Default for GraphScope {
    fn default() -> Self {
        Self::new(NamespacePath::default(), GraphId::default())
    }
}

impl Display for GraphScope {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}/graphs/{}", self.namespace, self.graph_id)
    }
}
