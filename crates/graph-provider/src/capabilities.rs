use crate::{BudgetLimits, GraphProviderError, MutationAtomicityPolicy, MutationRequest, ReadRequest, Result};
use std::collections::BTreeSet;

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum ReadOperation {
    NodeScan,
    RelationshipScan,
    Seek,
    Expand,
    VariableExpand,
    FetchNodes,
    FetchRelationships,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PredicatePushdown {
    None,
    Inexact,
    Exact,
}

/// Semantics that a particular read plan requires from its provider.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReadRequirements {
    pub operations: BTreeSet<ReadOperation>,
    pub ordered_paging: bool,
    pub exact_predicate_pushdown: bool,
    pub retained_snapshot: bool,
    pub read_own_writes: bool,
}

impl Default for ReadRequirements {
    fn default() -> Self {
        Self {
            operations: BTreeSet::new(),
            ordered_paging: false,
            exact_predicate_pushdown: false,
            retained_snapshot: true,
            read_own_writes: false,
        }
    }
}

impl ReadRequirements {
    pub fn require(mut self, operation: ReadOperation) -> Self {
        self.operations.insert(operation);
        self
    }
}

/// Semantics a mutation session must provide to be safe for its caller.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MutationRequirements {
    pub read_own_writes: bool,
    pub conflict_validation: bool,
    pub commit_tokens: bool,
    pub idempotent_receipts: bool,
}

impl Default for MutationRequirements {
    fn default() -> Self {
        Self {
            read_own_writes: true,
            conflict_validation: true,
            commit_tokens: true,
            idempotent_receipts: true,
        }
    }
}

/// Provider features and hard limits, scoped to the authorized graph.
///
/// A capability may be omitted only when the engine has a semantically correct
/// bounded fallback. Mutation policy and snapshot guarantees are never inferred
/// from a backend name.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GraphCapabilities {
    pub node_scan: bool,
    pub relationship_scan: bool,
    pub seek: bool,
    pub ordered_paging: bool,
    pub predicate_pushdown: PredicatePushdown,
    pub batch_adjacency: bool,
    pub graph_traversal: bool,
    pub node_hydration: bool,
    pub relationship_hydration: bool,
    pub retained_snapshots: bool,
    pub read_own_writes: bool,
    pub conflict_validation: bool,
    pub commit_tokens: bool,
    pub statement_atomicity: bool,
    pub legacy_chunked_recovery: bool,
    pub idempotent_receipts: bool,
    pub mutations: bool,
    pub max_batch_rows: u64,
    pub max_batch_bytes: u64,
}

impl GraphCapabilities {
    pub fn validate_read(&self, request: &ReadRequest) -> Result<()> {
        request.validate()?;
        for operation in &request.requirements.operations {
            let (supported, name) = match operation {
                ReadOperation::NodeScan => (self.node_scan, "node scan"),
                ReadOperation::RelationshipScan => (self.relationship_scan, "relationship scan"),
                ReadOperation::Seek => (self.seek, "index seek"),
                ReadOperation::Expand => (self.batch_adjacency, "batch adjacency"),
                ReadOperation::VariableExpand => (self.graph_traversal, "graph traversal"),
                ReadOperation::FetchNodes => (self.node_hydration, "node hydration"),
                ReadOperation::FetchRelationships => (self.relationship_hydration, "relationship hydration"),
            };
            if !supported {
                return Err(GraphProviderError::UnsupportedCapability { capability: name });
            }
        }
        if request.requirements.ordered_paging && !self.ordered_paging {
            return Err(GraphProviderError::UnsupportedCapability {
                capability: "stable ordered paging",
            });
        }
        if request.requirements.exact_predicate_pushdown && self.predicate_pushdown != PredicatePushdown::Exact {
            return Err(GraphProviderError::UnsupportedCapability {
                capability: "exact predicate pushdown",
            });
        }
        if request.requirements.retained_snapshot && !self.retained_snapshots {
            return Err(GraphProviderError::UnsupportedCapability {
                capability: "retained snapshots",
            });
        }
        if request.requirements.read_own_writes && !self.read_own_writes {
            return Err(GraphProviderError::UnsupportedCapability {
                capability: "read-own-writes",
            });
        }
        validate_batch_limits(request.budget, self)
    }

    pub fn validate_mutation(&self, request: &MutationRequest) -> Result<()> {
        request.validate()?;
        if !self.mutations {
            return Err(GraphProviderError::UnsupportedCapability { capability: "mutations" });
        }
        match request.atomicity {
            MutationAtomicityPolicy::Statement if !self.statement_atomicity => {
                return Err(GraphProviderError::UnsupportedCapability {
                    capability: "statement atomicity",
                });
            }
            MutationAtomicityPolicy::LegacyChunked if !self.legacy_chunked_recovery => {
                return Err(GraphProviderError::UnsupportedCapability {
                    capability: "legacy_chunked recovery",
                });
            }
            _ => {}
        }
        let requirements = &request.requirements;
        if requirements.read_own_writes && !self.read_own_writes {
            return Err(GraphProviderError::UnsupportedCapability {
                capability: "read-own-writes",
            });
        }
        if requirements.conflict_validation && !self.conflict_validation {
            return Err(GraphProviderError::UnsupportedCapability {
                capability: "conflict validation",
            });
        }
        if requirements.commit_tokens && !self.commit_tokens {
            return Err(GraphProviderError::UnsupportedCapability {
                capability: "commit tokens",
            });
        }
        if requirements.idempotent_receipts && !self.idempotent_receipts {
            return Err(GraphProviderError::UnsupportedCapability {
                capability: "idempotent mutation receipts",
            });
        }
        validate_batch_limits(request.budget, self)
    }
}

fn validate_batch_limits(request: BudgetLimits, capabilities: &GraphCapabilities) -> Result<()> {
    if request.max_batch_rows > capabilities.max_batch_rows {
        return Err(GraphProviderError::UnsupportedCapability {
            capability: "requested maximum batch rows",
        });
    }
    if request.max_batch_bytes > capabilities.max_batch_bytes {
        return Err(GraphProviderError::UnsupportedCapability {
            capability: "requested maximum batch bytes",
        });
    }
    Ok(())
}
