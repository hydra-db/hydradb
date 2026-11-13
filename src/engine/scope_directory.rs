use std::sync::Arc;

use chrono::{DateTime, Utc};
use futures::StreamExt;
use slatedb::bytes::Bytes;
use slatedb::object_store::path::Path;
use slatedb::object_store::{ObjectStore, ObjectStoreExt, PutMode, UpdateVersion};
use ulid::Ulid;

use crate::{GraphError, GraphId, GraphScope, NamespaceId, NamespacePath, Result};

const SCOPE_MARKER_VERSION: &str = "graph-scope-directory1";
const SCOPE_MARKER_NAME: &str = "__scope__";
const SCOPE_CHANGE_SEQUENCE_VERSION: &str = "graph-scope-change2";
const SCOPE_CHANGE_SEQUENCE_PREFIX: &str = "v2";
const SCOPE_CHANGE_ACK_ATTEMPTS: usize = 8;
const MAX_SCOPE_CHANGE_ACK_BYTES: usize = 32;
/// Payload of a planner statistics request marker. The marker's path is the
/// whole message; the payload only names the format.
const PLANNER_STATISTICS_REQUEST_VERSION: &str = "planner-statistics-wanted1";

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GraphScopeChange {
    pub scope: GraphScope,
    pub location: Path,
    pub created_at: DateTime<Utc>,
    pub cell_id: Option<String>,
    pub sequence: Option<u64>,
}

/// One (scope, cell) a node asked the indexer to build planner statistics
/// for, as listed from the request markers.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PlannerStatisticsRequest {
    pub scope: GraphScope,
    pub cell_id: String,
    pub location: Path,
    pub created_at: DateTime<Utc>,
}

#[derive(Clone)]
pub struct ObjectStoreGraphScopeDirectory {
    base_path: String,
    root_namespace: NamespacePath,
    graph_id: GraphId,
    object_store: Arc<dyn ObjectStore>,
}

impl ObjectStoreGraphScopeDirectory {
    pub fn new(base_path: impl Into<String>, root_namespace: NamespacePath, graph_id: GraphId, object_store: Arc<dyn ObjectStore>) -> Self {
        Self {
            base_path: base_path.into().trim_matches('/').to_string(),
            root_namespace,
            graph_id,
            object_store,
        }
    }

    pub fn root_scope(&self) -> GraphScope {
        GraphScope::new(self.root_namespace.clone(), self.graph_id.clone())
    }

    pub async fn register(&self, scope: &GraphScope) -> Result<()> {
        self.validate_scope(scope)?;
        let location = self.marker_path(scope);
        let payload = Bytes::from_static(SCOPE_MARKER_VERSION.as_bytes());
        match self.object_store.put_opts(&location, payload.into(), PutMode::Create.into()).await {
            Ok(_) | Err(slatedb::object_store::Error::AlreadyExists { .. }) => Ok(()),
            Err(error) => Err(error.into()),
        }
    }

    pub async fn list(&self) -> Result<Vec<GraphScope>> {
        let prefix = self.registry_prefix();
        let mut objects = self.object_store.list(Some(&prefix));
        let mut scopes = Vec::new();
        while let Some(metadata) = objects.next().await.transpose()? {
            let scope = self.scope_from_marker_path(metadata.location.as_ref())?;
            self.validate_scope(&scope)?;
            scopes.push(scope);
        }
        scopes.sort();
        scopes.dedup();
        Ok(scopes)
    }

    /// Persist a best-effort fast-lane hint after a scoped graph mutation.
    ///
    /// Every mutation gets a unique, sequence-bearing object. The indexer can
    /// acknowledge all hints covered by one refreshed storage snapshot without
    /// deleting a later mutation. The ordinary scope directory remains the
    /// recovery path if a process exits before this asynchronous hint lands.
    pub async fn notify_changed(&self, scope: &GraphScope, cell_id: &str, sequence: u64) -> Result<()> {
        self.validate_scope(scope)?;
        crate::validate_component("cell_id", cell_id)?;
        let location = self.sequenced_change_path(scope, cell_id, sequence, Ulid::new());
        let payload = Bytes::from_static(SCOPE_CHANGE_SEQUENCE_VERSION.as_bytes());
        self.object_store
            .put_opts(&location, payload.into(), PutMode::Create.into())
            .await?;
        Ok(())
    }

    pub async fn list_changes(&self) -> Result<Vec<GraphScopeChange>> {
        let prefix = self.change_prefix();
        let mut objects = self.object_store.list(Some(&prefix));
        let mut changes = Vec::new();
        while let Some(metadata) = objects.next().await.transpose()? {
            let (scope, cell_id, sequence) = self.change_from_path(metadata.location.as_ref())?;
            self.validate_scope(&scope)?;
            changes.push(GraphScopeChange {
                scope,
                location: metadata.location,
                created_at: metadata.last_modified,
                cell_id,
                sequence,
            });
        }
        changes.sort_by(|left, right| left.scope.cmp(&right.scope).then_with(|| left.location.cmp(&right.location)));
        Ok(changes)
    }

    pub async fn clear_changes(&self, changes: &[GraphScopeChange]) -> Result<()> {
        for change in changes {
            self.validate_scope(&change.scope)?;
        }
        let locations = changes.iter().map(|change| change.location.clone()).collect::<Vec<_>>();
        let deletes = futures::stream::iter(locations)
            .map(|location| {
                let object_store = Arc::clone(&self.object_store);
                async move { object_store.delete(&location).await }
            })
            .buffer_unordered(32);
        let results = deletes.collect::<Vec<_>>().await;
        for result in results {
            result?;
        }
        Ok(())
    }

    /// Ask the indexer to build planner statistics for one (scope, cell).
    ///
    /// A node writes this when queries keep planning without statistics; the
    /// indexer builds hinted scopes ahead of everything else. One marker per
    /// (scope, cell), overwritten rather than accumulated, so a hot scope that
    /// keeps asking costs one object. Best effort, like a change hint: a lost
    /// marker only delays the build until the node asks again.
    pub async fn request_planner_statistics(&self, scope: &GraphScope, cell_id: &str) -> Result<()> {
        self.validate_scope(scope)?;
        crate::validate_component("cell_id", cell_id)?;
        let location = self.planner_statistics_request_path(scope, cell_id);
        let payload = Bytes::from_static(PLANNER_STATISTICS_REQUEST_VERSION.as_bytes());
        self.object_store.put(&location, payload.into()).await?;
        Ok(())
    }

    /// Every outstanding planner statistics request, scope order.
    pub async fn list_planner_statistics_requests(&self) -> Result<Vec<PlannerStatisticsRequest>> {
        let prefix = self.planner_statistics_request_prefix();
        let mut objects = self.object_store.list(Some(&prefix));
        let mut requests = Vec::new();
        while let Some(metadata) = objects.next().await.transpose()? {
            let (scope, cell_id) = self.planner_statistics_request_from_path(metadata.location.as_ref())?;
            self.validate_scope(&scope)?;
            requests.push(PlannerStatisticsRequest {
                scope,
                cell_id,
                location: metadata.location,
                created_at: metadata.last_modified,
            });
        }
        requests.sort_by(|left, right| left.scope.cmp(&right.scope).then_with(|| left.cell_id.cmp(&right.cell_id)));
        Ok(requests)
    }

    /// Remove served (or given-up) requests. A request re-made after the
    /// listing is removed too; the node asks again if it still needs one.
    pub async fn clear_planner_statistics_requests(&self, requests: &[PlannerStatisticsRequest]) -> Result<()> {
        for request in requests {
            self.validate_scope(&request.scope)?;
        }
        let locations = requests.iter().map(|request| request.location.clone()).collect::<Vec<_>>();
        let deletes = futures::stream::iter(locations)
            .map(|location| {
                let object_store = Arc::clone(&self.object_store);
                async move {
                    match object_store.delete(&location).await {
                        Ok(()) | Err(slatedb::object_store::Error::NotFound { .. }) => Ok(()),
                        Err(error) => Err(error),
                    }
                }
            })
            .buffer_unordered(32);
        let results = deletes.collect::<Vec<_>>().await;
        for result in results {
            result?;
        }
        Ok(())
    }

    pub async fn covered_sequence(&self, scope: &GraphScope, cell_id: &str) -> Result<Option<u64>> {
        self.validate_scope(scope)?;
        crate::validate_component("cell_id", cell_id)?;
        let location = self.change_ack_path(scope, cell_id);
        match self.object_store.get(&location).await {
            Ok(result) => {
                let value = result.bytes().await?;
                decode_covered_sequence(location.as_ref(), &value).map(Some)
            }
            Err(slatedb::object_store::Error::NotFound { .. }) => Ok(None),
            Err(error) => Err(error.into()),
        }
    }

    /// Advance the durable per-cell watermark without allowing a concurrent,
    /// older indexer pass to move it backwards.
    pub async fn acknowledge_sequence(&self, scope: &GraphScope, cell_id: &str, sequence: u64) -> Result<()> {
        self.validate_scope(scope)?;
        crate::validate_component("cell_id", cell_id)?;
        let location = self.change_ack_path(scope, cell_id);
        let payload = Bytes::from(sequence.to_string());
        for _ in 0..SCOPE_CHANGE_ACK_ATTEMPTS {
            let current = match self.object_store.get(&location).await {
                Ok(result) => {
                    let version = UpdateVersion {
                        e_tag: result.meta.e_tag.clone(),
                        version: result.meta.version.clone(),
                    };
                    let value = result.bytes().await?;
                    let current = decode_covered_sequence(location.as_ref(), &value)?;
                    if current >= sequence {
                        return Ok(());
                    }
                    Some(version)
                }
                Err(slatedb::object_store::Error::NotFound { .. }) => None,
                Err(error) => return Err(error.into()),
            };
            let mode = current.map_or(PutMode::Create, PutMode::Update);
            match self.object_store.put_opts(&location, payload.clone().into(), mode.into()).await {
                Ok(_) => return Ok(()),
                Err(slatedb::object_store::Error::AlreadyExists { .. })
                | Err(slatedb::object_store::Error::Precondition { .. })
                | Err(slatedb::object_store::Error::NotFound { .. }) => continue,
                Err(error) => return Err(error.into()),
            }
        }
        Err(GraphError::ConditionalWriteConflict {
            operation: "acknowledge_graph_scope_change",
            key: location.to_string(),
        })
    }

    fn validate_scope(&self, scope: &GraphScope) -> Result<()> {
        if scope.graph_id == self.graph_id && scope.namespace.is_descendant_of(&self.root_namespace) {
            return Ok(());
        }
        Err(GraphError::GraphScopeMismatch {
            expected: format!("{}/graphs/{} and descendants", self.root_namespace, self.graph_id),
            actual: scope.to_string(),
        })
    }

    fn registry_prefix(&self) -> Path {
        path_from_base(
            &self.base_path,
            &format!("_graph_scopes/v1/{}/{}", self.graph_id, namespace_key(&self.root_namespace)),
        )
    }

    fn change_prefix(&self) -> Path {
        path_from_base(
            &self.base_path,
            &format!("_graph_scope_changes/v1/{}/{}", self.graph_id, namespace_key(&self.root_namespace)),
        )
    }

    fn change_ack_prefix(&self) -> Path {
        path_from_base(
            &self.base_path,
            &format!(
                "_graph_scope_change_acks/v1/{}/{}",
                self.graph_id,
                namespace_key(&self.root_namespace)
            ),
        )
    }

    fn planner_statistics_request_prefix(&self) -> Path {
        path_from_base(
            &self.base_path,
            &format!(
                "_planner_statistics_wanted/v1/{}/{}",
                self.graph_id,
                namespace_key(&self.root_namespace)
            ),
        )
    }

    fn planner_statistics_request_path(&self, scope: &GraphScope, cell_id: &str) -> Path {
        let relative_namespace = &scope.namespace.segments()[self.root_namespace.depth()..];
        let mut suffix = relative_namespace.iter().map(NamespaceId::as_str).collect::<Vec<_>>().join("/");
        if !suffix.is_empty() {
            suffix.push('/');
        }
        suffix.push_str(&hex_encode(cell_id.as_bytes()));
        Path::from(format!("{}/{suffix}", self.planner_statistics_request_prefix()))
    }

    fn planner_statistics_request_from_path(&self, location: &str) -> Result<(GraphScope, String)> {
        let prefix = self.planner_statistics_request_prefix().to_string();
        let relative = location
            .strip_prefix(&prefix)
            .and_then(|value| value.strip_prefix('/'))
            .ok_or_else(|| GraphError::CorruptValue {
                key: location.to_string(),
                reason: "planner statistics request is outside the configured prefix".to_string(),
            })?;
        let segments = relative.split('/').collect::<Vec<_>>();
        let Some((encoded_cell, namespace_segments)) = segments.split_last() else {
            return Err(GraphError::CorruptValue {
                key: location.to_string(),
                reason: "planner statistics request is missing its cell".to_string(),
            });
        };
        let cell_id = String::from_utf8(hex_decode(encoded_cell, location)?).map_err(|error| GraphError::CorruptValue {
            key: location.to_string(),
            reason: format!("planner statistics request cell id is not UTF-8: {error}"),
        })?;
        crate::validate_component("cell_id", &cell_id)?;
        let mut namespace = self.root_namespace.clone();
        for segment in namespace_segments {
            namespace = namespace.child(NamespaceId::new((*segment).to_string())?)?;
        }
        Ok((GraphScope::new(namespace, self.graph_id.clone()), cell_id))
    }

    fn marker_path(&self, scope: &GraphScope) -> Path {
        let relative_namespace = &scope.namespace.segments()[self.root_namespace.depth()..];
        let mut suffix = relative_namespace.iter().map(NamespaceId::as_str).collect::<Vec<_>>().join("/");
        if !suffix.is_empty() {
            suffix.push('/');
        }
        suffix.push_str(SCOPE_MARKER_NAME);
        Path::from(format!("{}/{suffix}", self.registry_prefix()))
    }

    fn sequenced_change_path(&self, scope: &GraphScope, cell_id: &str, sequence: u64, id: Ulid) -> Path {
        let relative_namespace = &scope.namespace.segments()[self.root_namespace.depth()..];
        let mut suffix = relative_namespace.iter().map(NamespaceId::as_str).collect::<Vec<_>>().join("/");
        if !suffix.is_empty() {
            suffix.push('/');
        }
        suffix.push_str(&format!(
            "{SCOPE_CHANGE_SEQUENCE_PREFIX}.{}.{sequence:020}.{id}",
            hex_encode(cell_id.as_bytes())
        ));
        Path::from(format!("{}/{suffix}", self.change_prefix()))
    }

    fn change_ack_path(&self, scope: &GraphScope, cell_id: &str) -> Path {
        let relative_namespace = &scope.namespace.segments()[self.root_namespace.depth()..];
        let mut suffix = relative_namespace.iter().map(NamespaceId::as_str).collect::<Vec<_>>().join("/");
        if !suffix.is_empty() {
            suffix.push('/');
        }
        suffix.push_str(&hex_encode(cell_id.as_bytes()));
        Path::from(format!("{}/{suffix}", self.change_ack_prefix()))
    }

    fn scope_from_marker_path(&self, location: &str) -> Result<GraphScope> {
        let prefix = self.registry_prefix().to_string();
        let relative = location
            .strip_prefix(&prefix)
            .and_then(|value| value.strip_prefix('/'))
            .ok_or_else(|| GraphError::CorruptValue {
                key: location.to_string(),
                reason: "scope marker is outside the configured registry prefix".to_string(),
            })?;
        let segments = relative.split('/').collect::<Vec<_>>();
        if segments.last().copied() != Some(SCOPE_MARKER_NAME) {
            return Err(GraphError::CorruptValue {
                key: location.to_string(),
                reason: "scope registry object is missing its terminal marker".to_string(),
            });
        }
        let mut namespace = self.root_namespace.clone();
        for segment in &segments[..segments.len() - 1] {
            namespace = namespace.child(NamespaceId::new((*segment).to_string())?)?;
        }
        Ok(GraphScope::new(namespace, self.graph_id.clone()))
    }

    fn change_from_path(&self, location: &str) -> Result<(GraphScope, Option<String>, Option<u64>)> {
        let prefix = self.change_prefix().to_string();
        let relative = location
            .strip_prefix(&prefix)
            .and_then(|value| value.strip_prefix('/'))
            .ok_or_else(|| GraphError::CorruptValue {
                key: location.to_string(),
                reason: "scope change is outside the configured change prefix".to_string(),
            })?;
        let segments = relative.split('/').collect::<Vec<_>>();
        let Some(change_id) = segments.last() else {
            return Err(GraphError::CorruptValue {
                key: location.to_string(),
                reason: "scope change is missing its identifier".to_string(),
            });
        };
        let (cell_id, sequence) = if let Some(encoded) = change_id.strip_prefix(&format!("{SCOPE_CHANGE_SEQUENCE_PREFIX}.")) {
            let parts = encoded.split('.').collect::<Vec<_>>();
            if parts.len() != 3 {
                return Err(GraphError::CorruptValue {
                    key: location.to_string(),
                    reason: "sequenced scope change has an invalid identifier".to_string(),
                });
            }
            let cell_id = String::from_utf8(hex_decode(parts[0], location)?).map_err(|error| GraphError::CorruptValue {
                key: location.to_string(),
                reason: format!("scope change cell id is not UTF-8: {error}"),
            })?;
            crate::validate_component("cell_id", &cell_id)?;
            let sequence = parts[1].parse::<u64>().map_err(|error| GraphError::CorruptValue {
                key: location.to_string(),
                reason: format!("scope change has an invalid sequence: {error}"),
            })?;
            parts[2].parse::<Ulid>().map_err(|error| GraphError::CorruptValue {
                key: location.to_string(),
                reason: format!("scope change has an invalid identifier: {error}"),
            })?;
            (Some(cell_id), Some(sequence))
        } else {
            change_id.parse::<Ulid>().map_err(|error| GraphError::CorruptValue {
                key: location.to_string(),
                reason: format!("scope change has an invalid identifier: {error}"),
            })?;
            (None, None)
        };
        let mut namespace = self.root_namespace.clone();
        for segment in &segments[..segments.len() - 1] {
            namespace = namespace.child(NamespaceId::new((*segment).to_string())?)?;
        }
        Ok((GraphScope::new(namespace, self.graph_id.clone()), cell_id, sequence))
    }
}

fn decode_covered_sequence(location: &str, value: &[u8]) -> Result<u64> {
    if value.len() > MAX_SCOPE_CHANGE_ACK_BYTES {
        return Err(GraphError::CorruptValue {
            key: location.to_string(),
            reason: format!(
                "scope change acknowledgement is {} bytes; maximum is {MAX_SCOPE_CHANGE_ACK_BYTES}",
                value.len()
            ),
        });
    }
    let value = std::str::from_utf8(value).map_err(|error| GraphError::CorruptValue {
        key: location.to_string(),
        reason: format!("scope change acknowledgement is not UTF-8: {error}"),
    })?;
    value.parse::<u64>().map_err(|error| GraphError::CorruptValue {
        key: location.to_string(),
        reason: format!("scope change acknowledgement is not a storage sequence: {error}"),
    })
}

fn hex_encode(value: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(value.len() * 2);
    for byte in value {
        encoded.push(HEX[(byte >> 4) as usize] as char);
        encoded.push(HEX[(byte & 0x0f) as usize] as char);
    }
    encoded
}

fn hex_decode(value: &str, location: &str) -> Result<Vec<u8>> {
    if !value.len().is_multiple_of(2) {
        return Err(GraphError::CorruptValue {
            key: location.to_string(),
            reason: "scope change cell id has invalid hex length".to_string(),
        });
    }
    value
        .as_bytes()
        .as_chunks::<2>()
        .0
        .iter()
        .map(|pair| {
            let high = hex_nibble(pair[0]);
            let low = hex_nibble(pair[1]);
            match (high, low) {
                (Some(high), Some(low)) => Ok((high << 4) | low),
                _ => Err(GraphError::CorruptValue {
                    key: location.to_string(),
                    reason: "scope change cell id contains invalid hex".to_string(),
                }),
            }
        })
        .collect()
}

fn hex_nibble(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        _ => None,
    }
}

fn namespace_key(namespace: &NamespacePath) -> String {
    namespace.segments().iter().map(NamespaceId::as_str).collect::<Vec<_>>().join("/")
}

fn path_from_base(base_path: &str, suffix: &str) -> Path {
    if base_path.is_empty() {
        Path::from(suffix)
    } else {
        Path::from(format!("{base_path}/{suffix}"))
    }
}
