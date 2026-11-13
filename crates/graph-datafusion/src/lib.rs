//! DataFusion execution primitives for graph plans.
//!
//! Logical operators carry only query-independent graph semantics. Runtime
//! handles are supplied through query-scoped context when physical execution
//! begins, preventing a cached plan from retaining a stale graph snapshot or
//! transaction.

pub mod context;
pub mod integer_division;
pub mod logical;
pub mod mutation_runtime;
pub mod physical;
pub mod runtime;
pub mod semantic_comparison;
pub mod semantics;
pub mod to_integer;
pub mod value_construction;

pub use context::{
    bounded_batches, provider_batches, validate_plan_capabilities, CorrelationBindings, CorrelationGuard, CorrelationScopes,
    GraphExecutionContext, GraphOperatorRuntime, MutationExecutionFinalizer, MutationExecutionLease, MutationExecutionPermit,
    MutationProgramOutput, MutationProgramRuntime,
};
pub use logical::{
    graph_extension, graph_mutation_program, graph_node, Direction, EntityKind, GraphLogicalNode, GraphOperator, MutationAtomicity,
    RelationshipUniqueness,
};
pub use mutation_runtime::ProviderMutationProgramRuntime;
pub use physical::{graph_extension_planners, GraphExtensionPlanner, GraphOperatorExec};
pub use runtime::ProviderGraphOperatorRuntime;
