use crate::{
    GraphCapabilities, GraphMutationSession, GraphProviderError, GraphReadView, GraphScope, MutationRequest, ReadRequest, Result,
    StatementReceiptLookup, StatementReceiptResolution,
};
use async_trait::async_trait;
use std::sync::Arc;

/// Catalog resolves an already-authorized graph scope to its backend-neutral
/// provider. Authorization and tenant isolation remain the caller's job.
#[async_trait]
pub trait GraphCatalog: Send + Sync {
    async fn provider_for(&self, scope: &GraphScope) -> Result<Arc<dyn GraphProvider>>;
}

/// Backend-neutral graph access contract implemented by a storage provider.
#[async_trait]
pub trait GraphProvider: Send + Sync {
    fn provider_id(&self) -> &crate::ProviderId;

    /// Return the capabilities for exactly this authorized scope. Capability
    /// decisions are required to be stable for the duration of an execution.
    fn capabilities(&self, scope: &GraphScope) -> GraphCapabilities;

    /// Open and pin a retained read view. Implementations must validate request
    /// requirements before starting storage work and must honor its causal lower
    /// bound, deadline, cancellation, budgets, and bounded I/O concurrency. A
    /// causal token from another provider or graph scope is rejected rather than
    /// silently treated as satisfied.
    async fn open_read(&self, request: ReadRequest) -> Result<Arc<dyn GraphReadView>>;

    /// Begin a mutation only after validating the selected atomicity policy and
    /// every required capability. An unsupported policy must fail before any
    /// mutation or transaction side effect begins.
    async fn begin_mutation(&self, request: MutationRequest) -> Result<Box<dyn GraphMutationSession>>;

    /// Resolve a statement receipt without acquiring writer admission or
    /// opening a mutation session. Routers use this before choosing an engine
    /// so retries cannot duplicate work across engine boundaries.
    async fn resolve_statement_receipt(&self, _lookup: StatementReceiptLookup) -> Result<StatementReceiptResolution> {
        Err(GraphProviderError::UnsupportedCapability {
            capability: "admission-free statement receipt resolution",
        })
    }
}

/// Common guard for provider implementations before they allocate a snapshot
/// or transaction. It also makes validation ordering easy to exercise in tests.
pub fn validate_read_open(capabilities: &GraphCapabilities, request: &ReadRequest) -> Result<()> {
    if request.scope.catalog().is_empty() || request.scope.graph().is_empty() {
        return Err(GraphProviderError::InvalidRequest("read request requires a graph scope".into()));
    }
    if request
        .causal_lower_bound
        .as_ref()
        .is_some_and(|token| token.scope() != &request.scope)
    {
        return Err(GraphProviderError::InvalidRequest(
            "causal commit token belongs to a different graph scope".into(),
        ));
    }
    capabilities.validate_read(request)
}

/// Provider-aware open guard that also verifies causal-token provenance.
pub fn validate_read_open_for(provider_id: &crate::ProviderId, capabilities: &GraphCapabilities, request: &ReadRequest) -> Result<()> {
    if request
        .causal_lower_bound
        .as_ref()
        .is_some_and(|token| token.provider() != provider_id)
    {
        return Err(GraphProviderError::InvalidRequest(
            "causal commit token belongs to a different provider".into(),
        ));
    }
    validate_read_open(capabilities, request)
}

/// Validate semantics before storage transaction creation.
pub fn validate_mutation_begin(capabilities: &GraphCapabilities, request: &MutationRequest) -> Result<()> {
    if request.scope.catalog().is_empty() || request.scope.graph().is_empty() {
        return Err(GraphProviderError::InvalidRequest("mutation request requires a graph scope".into()));
    }
    capabilities.validate_mutation(request)
}
