use std::fmt::{Display, Formatter};

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine as _;

use crate::{ScopeError, ScopeResult};

pub const DEFAULT_NAMESPACE_ID: &str = "default";
pub const DEFAULT_GRAPH_ID: &str = "default";
pub const MAX_NAMESPACE_DEPTH: usize = 8;

/// The one rule for a key component: nonempty, `[A-Za-z0-9_.-]` only.
///
/// Every id in a scope, and every cell, node and edge-type id the kernel builds
/// keys from, passes through this. Two crates that cannot depend on this one
/// repeat it for node ids, placement's `heartbeat::validate_node_id` and
/// `hydradb_protocol::owner::parse_owner_hint`, so keep all three in step.
pub fn validate_component(component: &'static str, value: &str) -> ScopeResult<()> {
    if value.is_empty() || !value.bytes().all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-' | b'.')) {
        return Err(ScopeError::InvalidKeyComponent {
            component,
            value: value.to_string(),
        });
    }
    Ok(())
}

#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[cfg_attr(feature = "serde", serde(try_from = "String", into = "String"))]
pub struct NamespaceId(String);

impl NamespaceId {
    pub fn new(value: impl Into<String>) -> ScopeResult<Self> {
        let value = value.into();
        validate_component("namespace_id", &value)?;
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Encode one caller-facing database or collection identifier into the
/// canonical namespace segment used by client protocol routing.
///
/// Keeping this transformation beside [`NamespaceId`] lets online routing and
/// offline image writers share one storage-layout contract.
pub fn encode_scope_namespace_id(component: &'static str, value: &str) -> ScopeResult<NamespaceId> {
    if value.is_empty() || value.chars().any(char::is_control) {
        return Err(ScopeError::InvalidKeyComponent {
            component,
            value: value.to_string(),
        });
    }
    NamespaceId::new(URL_SAFE_NO_PAD.encode(value.as_bytes()))
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
    type Error = ScopeError;

    fn try_from(value: String) -> ScopeResult<Self> {
        Self::new(value)
    }
}

impl From<NamespaceId> for String {
    fn from(value: NamespaceId) -> Self {
        value.0
    }
}

#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[cfg_attr(feature = "serde", serde(try_from = "String", into = "String"))]
pub struct GraphId(String);

impl GraphId {
    pub fn new(value: impl Into<String>) -> ScopeResult<Self> {
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
    type Error = ScopeError;

    fn try_from(value: String) -> ScopeResult<Self> {
        Self::new(value)
    }
}

impl From<GraphId> for String {
    fn from(value: GraphId) -> Self {
        value.0
    }
}

#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[cfg_attr(feature = "serde", serde(try_from = "Vec<NamespaceId>", into = "Vec<NamespaceId>"))]
pub struct NamespacePath {
    segments: Vec<NamespaceId>,
}

impl NamespacePath {
    pub fn root(namespace_id: NamespaceId) -> Self {
        Self {
            segments: vec![namespace_id],
        }
    }

    pub fn new(segments: impl IntoIterator<Item = NamespaceId>) -> ScopeResult<Self> {
        let segments: Vec<_> = segments.into_iter().collect();
        if segments.is_empty() {
            return Err(ScopeError::CorruptValue {
                key: "namespace_path".to_string(),
                reason: "namespace path must contain a tenant namespace".to_string(),
            });
        }
        if segments.len() > MAX_NAMESPACE_DEPTH {
            return Err(ScopeError::DepthExceeded {
                actual: segments.len() as u64,
                limit: MAX_NAMESPACE_DEPTH as u64,
            });
        }
        Ok(Self { segments })
    }

    /// Parse the `/`-joined spelling [`Display`] writes, `tenant/sub/leaf`.
    ///
    /// The HTTP `x-graph-namespace` header and the bookmark both carry this
    /// form. Every segment is validated as a [`NamespaceId`] and the depth
    /// limit applies, so an empty segment (`a//b`) is refused.
    pub fn parse(value: &str) -> ScopeResult<Self> {
        Self::new(
            value
                .split('/')
                .map(|segment| NamespaceId::new(segment.to_string()))
                .collect::<ScopeResult<Vec<_>>>()?,
        )
    }

    pub fn child(&self, namespace_id: NamespaceId) -> ScopeResult<Self> {
        let mut segments = self.segments.clone();
        segments.push(namespace_id);
        Self::new(segments)
    }

    pub fn tenant_id(&self) -> &NamespaceId {
        &self.segments[0]
    }

    pub fn leaf(&self) -> &NamespaceId {
        self.segments.last().expect("namespace paths always contain a root")
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
    type Error = ScopeError;

    fn try_from(segments: Vec<NamespaceId>) -> ScopeResult<Self> {
        Self::new(segments)
    }
}

impl From<NamespacePath> for Vec<NamespaceId> {
    fn from(value: NamespacePath) -> Self {
        value.segments
    }
}

#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct GraphScope {
    pub namespace: NamespacePath,
    pub graph_id: GraphId,
}

impl GraphScope {
    pub fn new(namespace: NamespacePath, graph_id: GraphId) -> Self {
        Self { namespace, graph_id }
    }

    pub fn tenant(namespace_id: NamespaceId, graph_id: GraphId) -> Self {
        Self::new(NamespacePath::root(namespace_id), graph_id)
    }

    pub fn scoped_store_path(&self, base_path: &str) -> String {
        let base_path = base_path.trim_end_matches('/');
        let scoped_path = format!("{}/graphs/{}", self.namespace.storage_suffix(), self.graph_id);
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
