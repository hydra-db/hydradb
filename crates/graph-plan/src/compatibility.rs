//! Compatibility exports for the original HydraDB KV planner.
//!
//! New planners should consume [`crate::GraphIr`] and lower it in their own
//! backend. These aliases make it explicit that the existing physical plan is
//! a temporary KV-specific adapter while retaining the crate-root exports
//! used by current consumers.

pub use crate::{explain_physical_plan as explain_kv_plan, lower_cypher_ast as lower_kv_cypher_ast};
pub use crate::{plan_physical as plan_kv_compat, PhysicalPlanningContext as KvPhysicalPlanningContext};
pub use crate::{
    BoundValue as KvBoundValue, GraphLogicalPlan as KvLogicalPlan, GraphPhysicalPlan as KvPhysicalPlan,
    LogicalBinaryOperator as KvLogicalBinaryOperator, LogicalExpression as KvLogicalExpression, LogicalProjection as KvLogicalProjection,
    PhysicalBinaryOperator as KvPhysicalBinaryOperator, PhysicalExpression as KvPhysicalExpression,
    PhysicalProjection as KvPhysicalProjection, ScalarValue as KvScalarValue, Symbol as KvSymbol,
};
