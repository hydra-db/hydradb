use crate::{BudgetResource, GraphProviderError, Result};
use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Arc,
};
use std::time::Instant;

/// Tenant/catalog and graph name, after the service boundary has authorized it.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct GraphScope {
    catalog: Arc<str>,
    graph: Arc<str>,
}

impl GraphScope {
    pub fn new(catalog: impl Into<Arc<str>>, graph: impl Into<Arc<str>>) -> Result<Self> {
        let catalog = catalog.into();
        let graph = graph.into();
        if catalog.is_empty() || graph.is_empty() {
            return Err(GraphProviderError::InvalidRequest(
                "catalog and graph names must be non-empty".into(),
            ));
        }
        Ok(Self { catalog, graph })
    }

    pub fn catalog(&self) -> &str {
        &self.catalog
    }

    pub fn graph(&self) -> &str {
        &self.graph
    }
}

/// Stable logical identity for a provider implementation.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct ProviderId(Arc<str>);

impl ProviderId {
    pub fn new(value: impl Into<Arc<str>>) -> Result<Self> {
        let value = value.into();
        if value.is_empty() {
            return Err(GraphProviderError::InvalidRequest("provider id must be non-empty".into()));
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Opaque identifier for a retained provider snapshot.
#[derive(Clone, Eq, Hash, PartialEq)]
pub struct SnapshotId(Arc<[u8]>);

impl SnapshotId {
    pub fn new(bytes: impl Into<Arc<[u8]>>) -> Result<Self> {
        let bytes = bytes.into();
        if bytes.is_empty() {
            return Err(GraphProviderError::InvalidRequest("snapshot id must be non-empty".into()));
        }
        Ok(Self(bytes))
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

impl std::fmt::Debug for SnapshotId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("SnapshotId")
            .field(&format_args!("{} opaque bytes", self.0.len()))
            .finish()
    }
}

/// Identity of a retained view, not merely a sequence number.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SnapshotIdentity {
    pub provider: ProviderId,
    pub scope: GraphScope,
    pub id: SnapshotId,
    /// Optional backend-neutral, monotonically ordered version if the provider
    /// has one. The snapshot id remains the actual retained-view identity.
    pub version: Option<u64>,
}

impl SnapshotIdentity {
    pub fn new(provider: ProviderId, scope: GraphScope, id: SnapshotId, version: Option<u64>) -> Self {
        Self {
            provider,
            scope,
            id,
            version,
        }
    }
}

/// Cancellation signal shared by the service, provider, and stream consumers.
#[derive(Clone, Debug, Default)]
pub struct CancellationToken(Arc<AtomicBool>);

impl CancellationToken {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn cancel(&self) {
        self.0.store(true, Ordering::Release);
    }

    pub fn is_cancelled(&self) -> bool {
        self.0.load(Ordering::Acquire)
    }
}

/// Per-request resource ceilings. Batch limits apply to each emitted Arrow
/// batch; the other limits apply across every stream sharing the tracker.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BudgetLimits {
    pub max_batch_rows: u64,
    pub max_batch_bytes: u64,
    pub max_total_rows: u64,
    pub max_total_bytes: u64,
    pub max_batches: u64,
    pub max_work_units: u64,
}

impl BudgetLimits {
    pub fn new(
        max_batch_rows: u64,
        max_batch_bytes: u64,
        max_total_rows: u64,
        max_total_bytes: u64,
        max_batches: u64,
        max_work_units: u64,
    ) -> Result<Self> {
        let limits = Self {
            max_batch_rows,
            max_batch_bytes,
            max_total_rows,
            max_total_bytes,
            max_batches,
            max_work_units,
        };
        limits.validate()?;
        Ok(limits)
    }

    pub fn validate(&self) -> Result<()> {
        if self.max_batch_rows == 0
            || self.max_batch_bytes == 0
            || self.max_total_rows == 0
            || self.max_total_bytes == 0
            || self.max_batches == 0
            || self.max_work_units == 0
        {
            return Err(GraphProviderError::InvalidRequest(
                "all provider budget limits must be non-zero".into(),
            ));
        }
        if self.max_batch_rows > self.max_total_rows || self.max_batch_bytes > self.max_total_bytes {
            return Err(GraphProviderError::InvalidRequest(
                "per-batch limits cannot exceed request-wide limits".into(),
            ));
        }
        Ok(())
    }

    pub fn tracker(self) -> BudgetTracker {
        BudgetTracker::new(self)
    }
}

/// Monotonic, shareable accounting for all work performed for one request.
#[derive(Clone, Debug)]
pub struct BudgetTracker {
    limits: BudgetLimits,
    rows: Arc<AtomicU64>,
    bytes: Arc<AtomicU64>,
    batches: Arc<AtomicU64>,
    work_units: Arc<AtomicU64>,
}

impl BudgetTracker {
    pub fn new(limits: BudgetLimits) -> Self {
        Self {
            limits,
            rows: Arc::new(AtomicU64::new(0)),
            bytes: Arc::new(AtomicU64::new(0)),
            batches: Arc::new(AtomicU64::new(0)),
            work_units: Arc::new(AtomicU64::new(0)),
        }
    }

    pub fn limits(&self) -> BudgetLimits {
        self.limits
    }

    pub fn snapshot(&self) -> BudgetSnapshot {
        BudgetSnapshot {
            rows: self.rows.load(Ordering::Relaxed),
            bytes: self.bytes.load(Ordering::Relaxed),
            batches: self.batches.load(Ordering::Relaxed),
            work_units: self.work_units.load(Ordering::Relaxed),
        }
    }

    pub fn consume_work_units(&self, amount: u64) -> Result<()> {
        add_bounded(&self.work_units, amount, self.limits.max_work_units, BudgetResource::WorkUnits)
    }

    /// Charge provider-side metadata or working buffers before allocating
    /// them. Emitted batches are charged separately by `GraphBatchStream`.
    pub fn consume_retained_bytes(&self, amount: u64) -> Result<()> {
        add_bounded(&self.bytes, amount, self.limits.max_total_bytes, BudgetResource::Bytes)
    }

    /// Charge retained result rows and their deterministic byte estimate before
    /// materializing copies that are not part of an emitted Arrow batch.
    pub fn consume_retained(&self, rows: u64, bytes: u64) -> Result<()> {
        // Reservations are intentionally monotonic. If bytes exceed their
        // limit after rows were reserved, the request stops with conservative
        // accounting just like `consume_batch`.
        add_bounded(&self.rows, rows, self.limits.max_total_rows, BudgetResource::Rows)?;
        self.consume_retained_bytes(bytes)
    }

    /// Charge a bounded batch emitted or applied by this request.
    pub fn consume_batch(&self, rows: u64, bytes: u64) -> Result<()> {
        // Reservations are intentionally monotonic. If one of the later
        // dimensions fails, accounting remains conservative for concurrent
        // streams and the request is expected to stop on the resulting error.
        add_bounded(&self.batches, 1, self.limits.max_batches, BudgetResource::Batches)?;
        add_bounded(&self.rows, rows, self.limits.max_total_rows, BudgetResource::Rows)?;
        add_bounded(&self.bytes, bytes, self.limits.max_total_bytes, BudgetResource::Bytes)?;
        Ok(())
    }
}

fn add_bounded(counter: &AtomicU64, amount: u64, limit: u64, resource: BudgetResource) -> Result<()> {
    counter
        .try_update(Ordering::AcqRel, Ordering::Acquire, |current| {
            current.checked_add(amount).filter(|next| *next <= limit)
        })
        .map(|_| ())
        .map_err(|_| GraphProviderError::BudgetExceeded { resource, limit })
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct BudgetSnapshot {
    pub rows: u64,
    pub bytes: u64,
    pub batches: u64,
    pub work_units: u64,
}

/// Service-supplied principal-scoped idempotency identity. Its key is omitted
/// from Debug output to avoid accidental log disclosure.
#[derive(Clone, Eq, PartialEq)]
pub struct IdempotencyIdentity {
    principal: Arc<str>,
    key: Arc<str>,
}

impl IdempotencyIdentity {
    pub fn new(principal: impl Into<Arc<str>>, key: impl Into<Arc<str>>) -> Result<Self> {
        let principal = principal.into();
        let key = key.into();
        if principal.is_empty() || key.is_empty() {
            return Err(GraphProviderError::InvalidRequest(
                "principal and idempotency key must be non-empty".into(),
            ));
        }
        Ok(Self { principal, key })
    }

    pub fn principal(&self) -> &str {
        &self.principal
    }

    pub fn key(&self) -> &str {
        &self.key
    }
}

impl std::fmt::Debug for IdempotencyIdentity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IdempotencyIdentity")
            .field("principal", &self.principal)
            .field("key", &"[REDACTED]")
            .finish()
    }
}

/// Digest of the normalized query, parameters, and relevant semantic inputs.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct MutationFingerprint(pub [u8; 32]);

/// Version of the Cypher semantics used to interpret this mutation.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct StatementSemanticsVersion(pub u32);

/// Proof that the service admitted this request as the active writer.
#[derive(Clone, Eq, PartialEq)]
pub struct WriterAdmissionToken {
    token: Arc<[u8]>,
    epoch: u64,
}

impl WriterAdmissionToken {
    pub fn new(token: impl Into<Arc<[u8]>>, epoch: u64) -> Result<Self> {
        let token = token.into();
        if token.is_empty() {
            return Err(GraphProviderError::InvalidRequest(
                "writer admission token must be non-empty".into(),
            ));
        }
        Ok(Self { token, epoch })
    }

    pub fn token(&self) -> &[u8] {
        &self.token
    }

    pub fn epoch(&self) -> u64 {
        self.epoch
    }
}

impl std::fmt::Debug for WriterAdmissionToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WriterAdmissionToken")
            .field("token", &"[REDACTED]")
            .field("epoch", &self.epoch)
            .finish()
    }
}

/// Immutable read-open parameters. The provider must pin all subsequent
/// operations to the returned view and honor these bounds and signals.
#[derive(Clone, Debug)]
pub struct ReadRequest {
    pub scope: GraphScope,
    pub causal_lower_bound: Option<crate::CommitToken>,
    pub deadline: Option<Instant>,
    pub cancellation: CancellationToken,
    pub budget: BudgetLimits,
    pub max_io_concurrency: usize,
    pub requirements: crate::ReadRequirements,
}

impl ReadRequest {
    pub fn validate(&self) -> Result<()> {
        self.budget.validate()?;
        if self.max_io_concurrency == 0 {
            return Err(GraphProviderError::InvalidRequest("I/O concurrency must be non-zero".into()));
        }
        Ok(())
    }

    pub fn budget_tracker(&self) -> BudgetTracker {
        self.budget.tracker()
    }

    pub fn is_cancelled_or_expired(&self) -> Result<()> {
        if self.cancellation.is_cancelled() {
            return Err(GraphProviderError::Cancelled);
        }
        if self.deadline.is_some_and(|deadline| Instant::now() >= deadline) {
            return Err(GraphProviderError::DeadlineExceeded);
        }
        Ok(())
    }
}
