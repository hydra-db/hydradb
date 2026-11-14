//! Backend-neutral contracts for graph catalogs, bounded graph reads, and
//! transaction-scoped mutations.
//!
//! This crate deliberately knows nothing about a storage engine or query
//! executor. Providers expose graph operations and Arrow batches; callers own
//! authorization, policy selection, and the lifetime of execution state.

mod capabilities;
mod context;
mod error;
mod mutation;
mod provider;
mod read;

pub use capabilities::{GraphCapabilities, MutationRequirements, PredicatePushdown, ReadOperation, ReadRequirements};
pub use context::{
    BudgetLimits, BudgetSnapshot, BudgetTracker, CancellationToken, GraphScope, IdempotencyIdentity, MutationFingerprint, ProviderId,
    ReadRequest, SnapshotId, SnapshotIdentity, StatementSemanticsVersion, WriterAdmissionToken,
};
pub use error::{BudgetResource, GraphProviderError, Result};
pub use mutation::{
    ActionRange, AppliedBindings, ChunkBoundary, ChunkReceipt, CommitDurability, CommitReceipt, CommitToken, CompletedActionRange,
    GraphMutationSession, MutationAction, MutationActionOutcome, MutationAtomicityPolicy, MutationBatch, MutationCounters,
    MutationOperation, MutationProperties, MutationRequest, MutationStatus, MutationTarget, ReturnManifestRow, StatementReceiptLookup,
    StatementReceiptOrigin, StatementReceiptResolution, StatementReplay, MUTATION_RECEIPT_VERSION,
};
pub use provider::{validate_mutation_begin, validate_read_open, validate_read_open_for, GraphCatalog, GraphProvider};
pub use read::{
    BatchBounds, ContinuationToken, EntityKind, ExpansionDepth, ExpansionInput, ExpansionRequest, GraphBatchStream, GraphReadView,
    IndexDescriptor, IndexSeekKey, MissingEntityPolicy, NodeFetchKey, NodeFetchRequest, NodeScanSpec, PageOrder, PageRequest, PathMode,
    Predicate, PredicateAcceptance, PreparedGraphScan, PropertyProjection, PropertySelection, RelationshipConstraints,
    RelationshipDirection, RelationshipFetchKey, RelationshipFetchRequest, RelationshipScanSpec, RelationshipUniqueness, ScanAcceptance,
    ScanAccessPath, ScanFetch, ScanOrder, ScanOrderKey, ScanRequest, ScanSource, SeekSpec,
};
