//! Backend-neutral graph IR and the legacy KV compatibility planner.
//!
//! [`GraphIr`] records bound graph semantics independently of a storage or
//! execution backend. The original [`GraphPhysicalPlan`] continues to record
//! HydraDB KV operations for existing consumers; new backend lowerers should
//! use [`compatibility`] only when they intentionally need that legacy plan.

#![forbid(unsafe_code)]

pub mod compatibility;
mod error;
mod explain;
mod graph_ir;
mod graph_ir_explain;
mod logical;
mod lower;
mod physical;
mod planner;
mod value;

pub use error::{GraphPlanError, GraphPlanResult};
pub use explain::explain_physical_plan;
pub use graph_ir::{
    AggregateCall, AggregateFunction, ApplyOutput, BoundSymbol, Direction, EffectClass, EntityKind, ExpansionMode, GraphBinaryOperator,
    GraphExpression, GraphIr, GraphIrKind, GraphProcedure, GraphSeekPredicate, MutationOperation, MutationPattern, MutationPolicy,
    MutationProgram, MutationRelationship, MutationStep, MutationUpdate, MutationVertex, ProcedureEffect, Projection,
    RelationshipUniqueness, Scope, ScopeId, SortKey, TraversalBounds, UnaryOperator, UnionMode, WindowCall, WindowFrame, WindowFrameBound,
    WindowFunction,
};
pub use graph_ir_explain::explain_graph_ir;
pub use logical::{GraphLogicalPlan, LogicalBinaryOperator, LogicalExpression, LogicalProjection, Symbol};
pub use lower::lower_cypher_ast;
pub use physical::{GraphPhysicalPlan, PhysicalBinaryOperator, PhysicalExpression, PhysicalProjection};
pub use planner::{plan_physical, PhysicalPlanningContext};
pub use value::{BoundValue, ScalarValue, ValueOrigin};
