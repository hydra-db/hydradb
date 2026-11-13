use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::{
    explain_physical_plan, explain_physical_plan_shape, lower_cypher_ast_with_id_properties, plan_physical_with_provider, GraphLogicalPlan,
    GraphPhysicalPlan, PhysicalPlanningContext, PhysicalProjection, StatisticsProvider, Symbol,
};
use hydradb_cypher_ast::{Document, QueryFailureReason};
use hydradb_cypher_parser_antlr::parse_cypher25;

use crate::capability::{logical_capability, physical_capability, EngineCapability};
use crate::{EngineError, EngineResult, ListParameterMap, ParameterMap, ScalarValue};

#[derive(Clone, Debug, PartialEq)]
pub struct PreparedQuery {
    pub ast: Arc<Document>,
    pub logical: Arc<GraphLogicalPlan>,
    pub physical: GraphPhysicalPlan,
}

impl PreparedQuery {
    /// Whether the engine can execute this plan, decided from the plan alone.
    ///
    /// Asked after planning and before a storage adapter is constructed, so a
    /// refusal costs no read session. See [`crate::capability`].
    pub fn capability(&self) -> EngineCapability {
        physical_capability(&self.physical)
    }

    pub fn explain(&self) -> String {
        explain_physical_plan(&self.physical)
    }

    pub fn column_names(&self) -> EngineResult<Vec<String>> {
        column_names(&self.physical)
    }

    /// `EXPLAIN` with literals and value lists elided: no tenant value, so it
    /// may go on a sampled trace.
    pub fn plan_shape(&self) -> String {
        explain_physical_plan_shape(&self.physical)
    }

    /// A stable identity for [`Self::plan_shape`]: FNV-1a, 64 bits, rendered
    /// by callers as 16 hex digits. Two requests share it exactly when they
    /// chose the same operators in the same order over the same schema names,
    /// so a plan flip between two requests of one query is visible as a hash
    /// change. It is a trace attribute, never a metric label: its cardinality
    /// is the number of query shapes, which clients control.
    pub fn plan_hash(&self) -> u64 {
        plan_shape_hash(&self.plan_shape())
    }
}

/// The hash [`PreparedQuery::plan_hash`] applies to a rendered shape, for a
/// caller that already holds the shape and should not render it twice.
pub fn plan_shape_hash(shape: &str) -> u64 {
    const OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const PRIME: u64 = 0x0000_0100_0000_01b3;
    shape
        .bytes()
        .fold(OFFSET, |hash, byte| (hash ^ u64::from(byte)).wrapping_mul(PRIME))
}

pub(crate) fn column_names(plan: &GraphPhysicalPlan) -> EngineResult<Vec<String>> {
    match plan {
        GraphPhysicalPlan::Project { items, .. } => Ok(items.iter().map(PhysicalProjection::column_name).collect()),
        // FINISH has an omitted result: no columns (§14.10).
        GraphPhysicalPlan::Finish { .. } => Ok(Vec::new()),
        GraphPhysicalPlan::Next { next, .. } => column_names(next),
        GraphPhysicalPlan::Union { arms, .. } => arms
            .first()
            .ok_or_else(|| EngineError::UnsupportedPlan(QueryFailureReason::Union, "UNION has no arms".to_string()))
            .and_then(column_names),
        _ => Err(EngineError::UnsupportedPlan(
            QueryFailureReason::Other,
            "a result-producing plan must have Project or Union at its root".to_string(),
        )),
    }
}

/// Parsed and lowered query awaiting a coherent statistics view. Lowering is
/// storage-free; callers can load statistics asynchronously before planning.
#[derive(Clone, Debug)]
pub struct LogicalQuery {
    pub ast: Arc<Document>,
    pub logical: Arc<GraphLogicalPlan>,
    planning: PhysicalPlanningContext,
}

impl LogicalQuery {
    /// Bound scalar and list values used by physical planning. Storage-backed
    /// adapters use this read-only view to load only point statistics that the
    /// prepared predicates can consume.
    pub fn planning_context(&self) -> &PhysicalPlanningContext {
        &self.planning
    }

    /// Whether the engine can execute this query, decided from the lowered
    /// plan alone.
    ///
    /// This is the earlier of the two checks and the one that matters for
    /// storage: it runs before any snapshot, so a shape refused here has not
    /// read a byte - not even the point lookups the statistics loader makes.
    pub fn capability(&self) -> EngineCapability {
        logical_capability(&self.logical)
    }

    pub fn plan_with_statistics(self, statistics: &dyn StatisticsProvider) -> EngineResult<PreparedQuery> {
        self.plan_with_provider(&crate::SlateDbDataSource::new(statistics))
    }

    pub fn plan_with_provider(self, provider: &crate::SlateDbDataSource<'_>) -> EngineResult<PreparedQuery> {
        let mut physical = plan_physical_with_provider(&self.logical, &self.planning, provider)?;
        // Settled here and not in the planner, which plans fragment by
        // fragment: a seek only knows what reads its binding once the whole
        // plan stands.
        crate::plan::mark_index_only_range_seeks(&mut physical);
        Ok(PreparedQuery {
            ast: self.ast,
            logical: self.logical,
            physical,
        })
    }
}

/// Wall time of the two halves of [`LoweredTemplate::lower_timed`].
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct LowerTimings {
    pub parse: Duration,
    pub lower: Duration,
}

/// The parameter-independent half of preparation: one query text, parsed
/// and lowered once. Parsing and lowering never read parameter *values* —
/// `$name` stays a `LogicalExpression::Parameter` until physical planning —
/// so a template keyed by query text alone can be shared across requests
/// and bound to each request's parameters with [`LoweredTemplate::bind`].
/// Both halves are `Arc`s so binding is two pointer copies, not a tree clone.
#[derive(Clone, Debug)]
pub struct LoweredTemplate {
    ast: Arc<Document>,
    logical: Arc<GraphLogicalPlan>,
}

impl LoweredTemplate {
    /// Parse and lower `query` with no parameters in sight.
    pub fn lower(query: &str) -> EngineResult<Self> {
        Self::lower_timed(query).map(|(template, _)| template)
    }

    /// [`Self::lower`], also reporting how long each half took, so a slow
    /// preparation can be attributed to the parser or to lowering.
    pub fn lower_timed(query: &str) -> EngineResult<(Self, LowerTimings)> {
        Self::lower_timed_with_id_properties(query, false)
    }

    /// Prepare BYOG property semantics without changing managed collections.
    pub fn lower_timed_with_id_properties(query: &str, id_properties: bool) -> EngineResult<(Self, LowerTimings)> {
        let started = Instant::now();
        let ast = parse_cypher25(query).map_err(|error| EngineError::Parse(format!("{error:?}")))?;
        let parse = started.elapsed();
        let started = Instant::now();
        let logical = lower_cypher_ast_with_id_properties(&ast, id_properties)?;
        let lower = started.elapsed();
        Ok((
            Self {
                ast: Arc::new(ast),
                logical: Arc::new(logical),
            },
            LowerTimings { parse, lower },
        ))
    }

    /// A conservative estimate of the heap this template retains, for cache
    /// accounting. The `Debug` renderings of the tree and plan are a cheap
    /// stand-in for walking every node: they spell out each string and enum
    /// once, so they track allocated size within a small constant factor.
    /// Computed once per lowering, never per request.
    pub fn estimated_bytes(&self) -> usize {
        std::mem::size_of::<Self>() + format!("{:?}", self.ast).len() + format!("{:?}", self.logical).len()
    }

    /// Attach one request's parameter values, producing the same
    /// [`LogicalQuery`] that lowering from scratch would. `SESSION_USER` reads
    /// [`DEFAULT_SESSION_USER`]; see [`Self::bind_as`].
    pub fn bind(&self, parameters: &ParameterMap, lists: &ListParameterMap) -> LogicalQuery {
        self.bind_as(parameters, lists, None)
    }

    /// [`Self::bind`] for a session whose authorization identifier is
    /// `session_user`, which `SESSION_USER` reads (GQL §20.3). A caller's
    /// parameter named [`SESSION_USER_PARAMETER`] is dropped, so a request
    /// cannot choose its own session user.
    pub fn bind_as(&self, parameters: &ParameterMap, lists: &ListParameterMap, session_user: Option<&str>) -> LogicalQuery {
        let mut bound: BTreeMap<Symbol, ScalarValue> = parameters
            .iter()
            .filter(|(name, _)| name.as_str() != SESSION_USER_PARAMETER)
            .map(|(name, value)| (Symbol::from(name.as_str()), value.clone()))
            .collect();
        bound.insert(
            Symbol::from(SESSION_USER_PARAMETER),
            ScalarValue::String(session_user.unwrap_or(DEFAULT_SESSION_USER).into()),
        );
        // §20.27 General Rule 1: the request timestamp, set once for the
        // request, as nanoseconds since the Unix epoch.
        bound.retain(|name, _| name.as_str() != REQUEST_TIMESTAMP_PARAMETER);
        bound.insert(
            Symbol::from(REQUEST_TIMESTAMP_PARAMETER),
            ScalarValue::Integer(chrono::Utc::now().timestamp_nanos_opt().map_or(0, i128::from)),
        );
        LogicalQuery {
            ast: Arc::clone(&self.ast),
            logical: Arc::clone(&self.logical),
            planning: PhysicalPlanningContext {
                parameters: bound,
                list_parameters: lists
                    .iter()
                    .map(|(name, values)| (Symbol::from(name.as_str()), values.clone()))
                    .collect(),
            },
        }
    }
}

pub(crate) fn lower_query_as(
    query: &str,
    parameters: &ParameterMap,
    lists: &ListParameterMap,
    session_user: Option<&str>,
) -> EngineResult<LogicalQuery> {
    Ok(LoweredTemplate::lower(query)?.bind_as(parameters, lists, session_user))
}

/// The parameter `SESSION_USER` lowers to (GQL §20.3). No `$parameter` in
/// query text can name it (`$hydradb.session_user` reads a property of
/// `$hydradb`), and binding sets it from the session alone.
pub const SESSION_USER_PARAMETER: &str = "hydradb.session_user";

/// The parameter the temporal functions read the request timestamp from
/// (GQL §20.27 General Rule 1); like [`SESSION_USER_PARAMETER`], no query
/// can name it.
pub const REQUEST_TIMESTAMP_PARAMETER: &str = "hydradb.request_timestamp";

/// `SESSION_USER` when the engine is given no session user (ID061, IV015).
pub const DEFAULT_SESSION_USER: &str = "anonymous";

pub(crate) fn prepare_query_as(
    query: &str,
    parameters: &ParameterMap,
    lists: &ListParameterMap,
    statistics: &dyn StatisticsProvider,
    session_user: Option<&str>,
) -> EngineResult<PreparedQuery> {
    lower_query_as(query, parameters, lists, session_user)?.plan_with_statistics(statistics)
}
