use std::sync::Arc;

use futures::StreamExt;
use slatedb::bytes::Bytes;
use slatedb::object_store::path::Path;
use slatedb::object_store::{ObjectStore, PutMode};

use crate::{GraphError, GraphId, GraphScope, NamespaceId, NamespacePath, Result};

const SCOPE_MARKER_VERSION: &str = "graph-scope-directory1";
const SCOPE_MARKER_NAME: &str = "__scope__";

#[derive(Clone)]
pub struct ObjectStoreGraphScopeDirectory {
    base_path: String,
    root_namespace: NamespacePath,
    graph_id: GraphId,
    object_store: Arc<dyn ObjectStore>,
}

impl ObjectStoreGraphScopeDirectory {
    pub fn new(
        base_path: impl Into<String>,
        root_namespace: NamespacePath,
        graph_id: GraphId,
        object_store: Arc<dyn ObjectStore>,
    ) -> Self {
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
        match self
            .object_store
            .put_opts(&location, payload.into(), PutMode::Create.into())
            .await
        {
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

    fn validate_scope(&self, scope: &GraphScope) -> Result<()> {
        if scope.graph_id == self.graph_id && scope.namespace.is_descendant_of(&self.root_namespace)
        {
            return Ok(());
        }
        Err(GraphError::GraphScopeMismatch {
            expected: format!(
                "{}/graphs/{} and descendants",
                self.root_namespace, self.graph_id
            ),
            actual: scope.to_string(),
        })
    }

    fn registry_prefix(&self) -> Path {
        path_from_base(
            &self.base_path,
            &format!(
                "_graph_scopes/v1/{}/{}",
                self.graph_id,
                namespace_key(&self.root_namespace)
            ),
        )
    }

    fn marker_path(&self, scope: &GraphScope) -> Path {
        let relative_namespace = &scope.namespace.segments()[self.root_namespace.depth()..];
        let mut suffix = relative_namespace
            .iter()
            .map(NamespaceId::as_str)
            .collect::<Vec<_>>()
            .join("/");
        if !suffix.is_empty() {
            suffix.push('/');
        }
        suffix.push_str(SCOPE_MARKER_NAME);
        Path::from(format!("{}/{suffix}", self.registry_prefix()))
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
}

fn namespace_key(namespace: &NamespacePath) -> String {
    namespace
        .segments()
        .iter()
        .map(NamespaceId::as_str)
        .collect::<Vec<_>>()
        .join("/")
}

fn path_from_base(base_path: &str, suffix: &str) -> Path {
    if base_path.is_empty() {
        Path::from(suffix)
    } else {
        Path::from(format!("{base_path}/{suffix}"))
    }
}

#[cfg(test)]
mod tests {
    use slatedb::object_store::memory::InMemory;

    use super::*;

    fn namespace(segments: &[&str]) -> NamespacePath {
        NamespacePath::new(
            segments
                .iter()
                .map(|segment| NamespaceId::new(*segment).unwrap()),
        )
        .unwrap()
    }

    #[tokio::test]
    async fn registers_and_lists_native_graph_scopes_idempotently() {
        let object_store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
        let directory = ObjectStoreGraphScopeDirectory::new(
            "graph/data",
            namespace(&["production"]),
            GraphId::new("hydradb").unwrap(),
            object_store,
        );
        let first = GraphScope::new(
            namespace(&["production", "tenant-a", "collection-a"]),
            GraphId::new("hydradb").unwrap(),
        );
        let second = GraphScope::new(
            namespace(&["production", "tenant-a", "collection-b"]),
            GraphId::new("hydradb").unwrap(),
        );
        let reserved_looking = GraphScope::new(
            namespace(&["production", "_root"]),
            GraphId::new("hydradb").unwrap(),
        );

        directory.register(&second).await.unwrap();
        directory.register(&first).await.unwrap();
        directory.register(&first).await.unwrap();
        directory.register(&reserved_looking).await.unwrap();

        assert_eq!(
            directory.list().await.unwrap(),
            vec![reserved_looking, first, second]
        );
    }

    #[tokio::test]
    async fn rejects_scopes_outside_the_configured_graph_hierarchy() {
        let object_store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
        let directory = ObjectStoreGraphScopeDirectory::new(
            "graph/data",
            namespace(&["production"]),
            GraphId::new("hydradb").unwrap(),
            object_store,
        );
        let wrong_graph = GraphScope::new(
            namespace(&["production", "tenant-a"]),
            GraphId::new("other").unwrap(),
        );

        assert!(matches!(
            directory.register(&wrong_graph).await,
            Err(GraphError::GraphScopeMismatch { .. })
        ));
    }
}
