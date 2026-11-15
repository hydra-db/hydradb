//! Isolated Cypher preparation and physical-plan execution.
//!
//! This crate deliberately has no dependency on HydraDB's root package. It
//! develops the new parser/AST/planner/executor path beside the production
//! libcypher `GraphShard` path until a storage adapter is ready. Its plan types
//! are private to this experimental path so new operators cannot force changes
//! into the production executor.

#![forbid(unsafe_code)]

mod capability;
mod decimal;
mod error;
mod execute;
mod hydration;
mod model;
mod observe;
mod plan;
mod prepare;
mod storage;
mod temporal;
mod vertex_scan;
pub use vertex_scan::{PreparedVertexScan, SlateDbDataSource, VertexOutput, VertexScanProperties, VertexScanRequest, VertexWindow};

pub use capability::{
    logical_capability, physical_capability, EngineCapability, UnsupportedShape, EXPAND_FROM_UNBOUND, NESTED_PROJECT_AGGREGATE,
    NESTED_PROJECT_UNNAMED, RESULT_PLAN_ROOT, UNDIRECTED_VARIABLE_LENGTH, UNION_BINDING_MISMATCH, UNION_COLUMN_MISMATCH, UNION_EMPTY,
};
pub use decimal::Decimal;
pub use error::{EngineError, EngineResult, StorageError, StorageResult};
pub use execute::CypherEngine;
pub use execute::ExecutionOutcome;
pub use hydradb_cypher_ast::{DiagnosticRecord, GqlStatus, GqlStatusObject, StatusCategory};
pub use model::{
    compare_property_values, scalar_values_equal, AccessPathKind, BoundedVertexIds, ExpandNeighbor, ExpandRequest, ListParameterMap,
    OrderedPropertyEntryPage, OrderedPropertyScanPage, OrderedPropertyScanRequest, OrderedRelationshipPropertyScanPage,
    OrderedRelationshipPropertyScanRequest, OrderedScanCursor, OrderedScanPosition, ParameterMap, PathValue, PropertyBound,
    PropertyEqualityCandidate, PropertyPointSet, QueryColumn, QueryResult, QueryRow, QueryValue, ReadRequest, RelationshipId,
    RelationshipRecord, StatisticsAccessReport, VertexId, VertexRecord, VertexStorageScanPage, VertexStorageScanRequest,
};
pub use observe::{ExecutionProfile, OperatorProfile, OperatorProfiler};
pub use plan::{
    estimate_operator_rows, explain_physical_plan, explain_physical_plan_shape, lower_cypher_ast, lower_cypher_ast_with_id_properties,
    plan_physical, plan_physical_with_provider, plan_physical_with_statistics, AggregateFunction, BloomFilterStatistics, BoundValue,
    ElementTestKind, ExpandDirection, FloatFormat, GqlType, GqlTypeKind, GraphLogicalPlan, GraphPhysicalPlan, GraphPlanError,
    GraphPlanResult, IterationPlan, LogicalBinaryOperator, LogicalExpression, LogicalInList, LogicalProjection, LogicalSort,
    LogicalUnaryOperator, PathPart, PathWord, PatternDirection, PhysicalBinaryOperator, PhysicalExpression, PhysicalPlanningContext,
    PhysicalProjection, PhysicalSort, PhysicalUnaryOperator, ProjectionContext, PropertyNumericRange, PropertyStatistics,
    ScalarArgumentRole, ScalarFunction, ScalarFunctionFamily, ScalarValue, SortDirection, StatisticsProvider, Symbol, UnknownStatistics,
    ValueOrigin, WalkNodes, WordLetter,
};
pub use prepare::{
    plan_shape_hash, LogicalQuery, LowerTimings, LoweredTemplate, PreparedQuery, DEFAULT_SESSION_USER, REQUEST_TIMESTAMP_PARAMETER,
    SESSION_USER_PARAMETER,
};
pub use storage::{GraphRead, GraphStorage};
pub use temporal::{epoch_seconds, nanos_of_day, DurationValue, TemporalKind, TemporalValue};
