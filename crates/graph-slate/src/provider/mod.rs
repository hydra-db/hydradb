pub mod mutation;
mod read;

use crate::SlateGraphStore;
use async_trait::async_trait;
use hydradb_graph_provider::{
    validate_mutation_begin, validate_read_open_for, GraphCapabilities, GraphMutationSession as ProviderMutationSession, GraphProvider,
    GraphProviderError, GraphReadView as ProviderReadView, GraphScope, MutationRequest, PredicatePushdown, ReadRequest, Result, SnapshotId,
    SnapshotIdentity,
};
use mutation::SlateGraphMutationSession;
pub use read::SlateGraphReadView;

use std::sync::Arc;

#[async_trait]
impl GraphProvider for SlateGraphStore {
    fn provider_id(&self) -> &hydradb_graph_provider::ProviderId {
        static ID: std::sync::OnceLock<hydradb_graph_provider::ProviderId> = std::sync::OnceLock::new();
        ID.get_or_init(|| hydradb_graph_provider::ProviderId::new("slatedb").expect("valid provider id"))
    }

    fn capabilities(&self, _scope: &GraphScope) -> GraphCapabilities {
        GraphCapabilities {
            node_scan: true,
            relationship_scan: true,
            seek: true,
            ordered_paging: true,
            predicate_pushdown: PredicatePushdown::Inexact,
            batch_adjacency: true,
            graph_traversal: false,
            node_hydration: true,
            relationship_hydration: true,
            retained_snapshots: true,
            read_own_writes: true,
            conflict_validation: true,
            commit_tokens: true,
            statement_atomicity: true,
            legacy_chunked_recovery: self.mutation_state().await_durable_writes,
            idempotent_receipts: true,
            mutations: true,
            max_batch_rows: 4096,
            max_batch_bytes: 8 * 1024 * 1024,
        }
    }

    async fn open_read(&self, request: ReadRequest) -> Result<Arc<dyn ProviderReadView>> {
        let provider = self.provider_id();
        let capabilities = self.capabilities(&request.scope);
        validate_read_open_for(provider, &capabilities, &request)?;
        request.is_cancelled_or_expired()?;

        let snapshot = self.snapshot().await.map_err(read::provider_backend_error)?;
        let version = snapshot.seq();
        if let Some(token) = &request.causal_lower_bound {
            let minimum = u64::from_be_bytes(
                token
                    .as_bytes()
                    .try_into()
                    .map_err(|_| GraphProviderError::InvalidRequest("invalid SlateDB causal token".into()))?,
            );
            if version < minimum {
                return Err(GraphProviderError::SnapshotUnavailable);
            }
        }
        let mut snapshot_id = version.to_be_bytes().to_vec();
        snapshot_id.extend_from_slice(&snapshot.last_wal_id().unwrap_or_default().to_be_bytes());
        let id = SnapshotId::new(snapshot_id)?;
        let identity = SnapshotIdentity::new(provider.clone(), request.scope.clone(), id, Some(version));
        Ok(Arc::new(SlateGraphReadView::new(
            snapshot,
            identity,
            request,
            None,
            self.mutation_state().index_policy,
        )))
    }

    async fn begin_mutation(&self, request: MutationRequest) -> Result<Box<dyn ProviderMutationSession>> {
        let capabilities = self.capabilities(&request.scope);
        validate_mutation_begin(&capabilities, &request)?;
        request.validate()?;
        Ok(Box::new(
            SlateGraphMutationSession::begin(Arc::new(self.clone()), self.provider_id().clone(), request).await?,
        ))
    }

    async fn resolve_statement_receipt(
        &self,
        lookup: hydradb_graph_provider::StatementReceiptLookup,
    ) -> Result<hydradb_graph_provider::StatementReceiptResolution> {
        mutation::resolve_statement_receipt(self, self.provider_id(), lookup).await
    }
}
