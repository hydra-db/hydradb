use hydradb_cypher_ast::{GqlStatus, QueryFailureReason};
use std::cell::Cell;
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet, BinaryHeap};

use indexmap::{IndexMap, IndexSet};
use std::future::Future;
use std::pin::Pin;

use crate::{
    AggregateFunction, BoundValue, ElementTestKind, GraphPhysicalPlan, PathPart, PathValue, PathWord, PatternDirection,
    PhysicalBinaryOperator, PhysicalExpression, PhysicalSort, PhysicalUnaryOperator, ScalarFunction, ScalarValue, Symbol, TemporalValue,
    WalkNodes,
};

mod conditions;
mod connected_match;
mod functions;
mod time;
mod types;
mod vertex_scan;

use conditions::{DeferredConditions, ExecutionRead};
use hydradb_cypher_ast::{GqlStatusObject, NullOrdering, PathSearchKind, QueryConjunction};

use crate::capability::{
    nested_projection_name, shape_error, NESTED_PROJECT_UNNAMED, RESULT_PLAN_ROOT, UNION_COLUMN_MISMATCH, UNION_EMPTY,
};
use crate::hydration::{plan_reuses_hydration, HydrationIdentityMap};
use crate::model::{compare_property_values, scalar_value_rank};

use crate::{
    scalar_values_equal, EngineError, EngineResult, ExpandDirection, ExpandNeighbor, ExpandRequest, GraphRead, GraphStorage,
    OrderedRelationshipPropertyScanRequest, ParameterMap, PreparedQuery, PropertyBound, QueryColumn, QueryResult, QueryRow, QueryValue,
    ReadRequest, RelationshipId, RelationshipRecord, SortDirection, VertexId, VertexRecord,
};

#[derive(Clone, Debug, Default, Eq, Hash, PartialEq)]
struct BindingRow {
    vertices: BTreeMap<Symbol, VertexRecord>,
    relationships: BTreeMap<Symbol, RelationshipRecord>,
    /// Names an OPTIONAL match tried to bind and could not.
    ///
    /// Cypher binds those to null rather than leaving them unbound, and the
    /// difference decides what a later clause does with them. A null name
    /// matches no pattern, so a required clause reading one produces nothing
    /// and the row is eliminated. An *absent* name is merely one this plan has
    /// not produced yet, and must be scanned for. Collapsing the two lets a
    /// later clause scan for a name Cypher says is null and bind something
    /// arbitrary to it.
    nulls: BTreeSet<Symbol>,
    /// Scalars a projection bound to a name, which no pattern can produce.
    ///
    /// `WITH e.entity_id AS id` binds a value, not an entity, and the rest of
    /// the query must be able to read `id` the same way it reads `e`. An entity
    /// projected under a new name stays in `vertices` or `relationships`
    /// instead, because flattening it to an id would lose the property map that
    /// `id.name` needs.
    values: BTreeMap<Symbol, QueryValue>,
    /// Full records for a named relationship list, shared across aliases.
    relationship_lists: BTreeMap<Symbol, std::sync::Arc<Vec<RelationshipRecord>>>,
    /// Marks whose subquery raised a data exception for this row. The
    /// exception is the mark's value: raised where the mark is read, and not at
    /// all where it is not, so a subquery in a CASE branch that is not taken
    /// cannot fail the query (§20.7 General Rules evaluate only the chosen
    /// result).
    deferred: BTreeMap<Symbol, (GqlStatus, String, hydradb_cypher_ast::operation::Operation)>,
    /// The records of nodes this row holds only by identity, in a computed
    /// value: `WITH coalesce(n, m) AS x`, a collected list, a group variable.
    /// A projection carries them from its input, so a node keeps its labels
    /// and properties however it reached the name.
    vertex_records: BTreeMap<VertexId, VertexRecord>,
    conditions: BTreeMap<Symbol, DeferredConditions>,
}

/// One invocation's fixed record, independent of table cardinality.
/// No lock is held while evaluating expressions or awaiting storage.
struct FixedRecord(std::sync::Mutex<BindingRow>);

impl FixedRecord {
    fn new(seed: Option<&BindingRow>) -> Self {
        Self(std::sync::Mutex::new(seed.cloned().unwrap_or_default()))
    }

    fn readable(&self, names: &[Symbol]) -> BindingRow {
        self.0.lock().expect("fixed record lock poisoned").restricted_to(names)
    }

    fn contains(&self, names: &[Symbol]) -> bool {
        let record = self.0.lock().expect("fixed record lock poisoned");
        names.iter().all(|name| record.binds(name))
    }

    fn define(&self, row: &BindingRow, names: &[Symbol]) {
        if !names.is_empty() {
            let additions = row.restricted_to(names);
            let mut record = self.0.lock().expect("fixed record lock poisoned");
            *record = record.overlaid(&additions);
        }
    }
}

impl BindingRow {
    /// Whether the row binds `name`, to anything (null included).
    fn binds(&self, name: &Symbol) -> bool {
        self.vertices.contains_key(name)
            || self.relationships.contains_key(name)
            || self.values.contains_key(name)
            || self.relationship_lists.contains_key(name)
            || self.nulls.contains(name)
            || self.deferred.contains_key(name)
    }

    fn read_mark(&self, binding: &Symbol) -> EngineResult<()> {
        if let Some((status, message, operation)) = self.deferred.get(binding) {
            return Err(EngineError::data(*status, message.clone()).in_operation(*operation));
        }
        if let Some(conditions) = self.conditions.get(binding) {
            conditions.publish();
        }
        Ok(())
    }

    /// Whether `binding` is bound to null by an OPTIONAL that matched nothing.
    fn is_null(&self, binding: &Symbol) -> bool {
        self.nulls.contains(binding)
    }

    /// Record every name in `bindings` this row does not carry as null. Used
    /// on the miss side of an optional join, where the right plan's bindings
    /// are exactly the names that failed to bind.
    fn mark_null(&mut self, bindings: &BTreeSet<Symbol>) {
        for binding in bindings {
            if !self.vertices.contains_key(binding) && !self.relationships.contains_key(binding) && !self.values.contains_key(binding) {
                self.nulls.insert(binding.clone());
            }
        }
    }

    /// CALL imports only the named bindings (§15.2); the identity cache is
    /// retained for elements nested inside imported values.
    fn restricted_to(&self, names: &[Symbol]) -> Self {
        if names.is_empty() {
            return Self::default();
        }
        let keep = |name: &Symbol| names.contains(name);
        BindingRow {
            vertices: self
                .vertices
                .iter()
                .filter(|(name, _)| keep(name))
                .map(|(name, value)| (name.clone(), value.clone()))
                .collect(),
            relationships: self
                .relationships
                .iter()
                .filter(|(name, _)| keep(name))
                .map(|(name, value)| (name.clone(), value.clone()))
                .collect(),
            nulls: self.nulls.iter().filter(|name| keep(name)).cloned().collect(),
            values: self
                .values
                .iter()
                .filter(|(name, _)| keep(name))
                .map(|(name, value)| (name.clone(), value.clone()))
                .collect(),
            relationship_lists: self
                .relationship_lists
                .iter()
                .filter(|(name, _)| keep(name))
                .map(|(name, value)| (name.clone(), value.clone()))
                .collect(),
            deferred: self
                .deferred
                .iter()
                .filter(|(name, _)| keep(name))
                .map(|(name, value)| (name.clone(), value.clone()))
                .collect(),
            conditions: self
                .conditions
                .iter()
                .filter(|(name, _)| keep(name))
                .map(|(name, value)| (name.clone(), value.clone()))
                .collect(),
            // Keyed by node, not by name: a kept value may name any of them.
            vertex_records: self.vertex_records.clone(),
        }
    }

    fn clear_binding(&mut self, name: &Symbol) {
        self.vertices.remove(name);
        self.relationships.remove(name);
        self.values.remove(name);
        self.relationship_lists.remove(name);
        self.nulls.remove(name);
        self.deferred.remove(name);
        self.conditions.remove(name);
    }

    /// Overlay names replace every representation, including deferred errors.
    /// All other bindings and graph-element records remain readable.
    fn overlaid(&self, projected: &Self) -> Self {
        let mut overlay = self.clone();
        for name in projected
            .vertices
            .keys()
            .chain(projected.relationships.keys())
            .chain(projected.values.keys())
            .chain(projected.relationship_lists.keys())
            .chain(projected.nulls.iter())
            .chain(projected.deferred.keys())
            .chain(projected.conditions.keys())
        {
            overlay.clear_binding(name);
        }
        overlay.vertices.extend(projected.vertices.clone());
        overlay.relationships.extend(projected.relationships.clone());
        overlay.values.extend(projected.values.clone());
        overlay.relationship_lists.extend(projected.relationship_lists.clone());
        overlay.nulls.extend(projected.nulls.clone());
        overlay.deferred.extend(projected.deferred.clone());
        overlay.conditions.extend(projected.conditions.clone());
        overlay.vertex_records.extend(projected.vertex_records.clone());
        overlay
    }

    /// Projected names shadow fixed bindings, which shadow permitted input names.
    fn for_sort<'a>(&'a self, input: Option<&Self>, needs_input: bool, fixed: Option<&Self>) -> std::borrow::Cow<'a, Self> {
        let columns = match fixed {
            Some(fixed) => std::borrow::Cow::Owned(fixed.overlaid(self)),
            None => std::borrow::Cow::Borrowed(self),
        };
        match input.filter(|_| needs_input) {
            Some(input) => std::borrow::Cow::Owned(input.overlaid(&columns)),
            None => columns,
        }
    }

    /// CALL's result names are disjoint from the input's. Reading the
    /// returned columns forces their deferred errors, not unrelated marks.
    fn joined_call_result(&self, found: &Self, columns: &[Symbol]) -> EngineResult<Self> {
        let mut carried = Self::default();
        for column in columns {
            carry_binding(found, column, column.clone(), &mut carried)?;
        }
        let mut joined = self.clone();
        joined.vertices.extend(carried.vertices);
        joined.relationships.extend(carried.relationships);
        joined.values.extend(carried.values);
        joined.relationship_lists.extend(carried.relationship_lists);
        joined.nulls.extend(carried.nulls);
        joined.vertex_records.extend(carried.vertex_records);
        Ok(joined)
    }

    /// Missing CALL results are null-bound, never absent graph bindings.
    fn null_extended_call(&self, columns: &[Symbol]) -> Self {
        let mut missed = self.clone();
        for column in columns {
            missed.clear_binding(column);
            missed.nulls.insert(column.clone());
        }
        missed
    }
}

struct ExpandSpec<'a> {
    from: &'a Symbol,
    relationship: Option<&'a Symbol>,
    relationship_types: &'a [Symbol],
    direction: PatternDirection,
    to: &'a Symbol,
    target_labels: &'a [Symbol],
}

struct ResidualPredicate<'a> {
    expression: &'a PhysicalExpression,
    operation: hydradb_cypher_ast::operation::Operation,
}

#[derive(Clone, Default)]
struct TraversalHistory {
    used: std::sync::Arc<BTreeSet<RelationshipId>>,
    /// Unnamed walks retain only the trail membership set.
    path: Option<std::sync::Arc<Vec<RelationshipRecord>>>,
    /// The nodes visited, start included, when a path mode prunes on them.
    nodes: Option<std::sync::Arc<BTreeSet<VertexId>>>,
}

impl TraversalHistory {
    fn records(&self) -> &[RelationshipRecord] {
        self.path.as_deref().map(Vec::as_slice).unwrap_or_default()
    }

    fn extend(mut self, relationship: &RelationshipRecord, target: VertexId) -> Self {
        std::sync::Arc::make_mut(&mut self.used).insert(relationship.id);
        if let Some(nodes) = &mut self.nodes {
            std::sync::Arc::make_mut(nodes).insert(target);
        }
        if let Some(path) = &mut self.path {
            std::sync::Arc::make_mut(path).push(relationship.clone());
        }
        self
    }
}

#[derive(Clone)]
struct TraversalState {
    row: BindingRow,
    /// Where the walk began, which a SIMPLE walk may end on.
    start: VertexId,
    current: VertexId,
    /// The iteration's source record, not a captured value in the row.
    iteration_source: Option<VertexRecord>,
    history: TraversalHistory,
}

#[derive(Clone, Debug)]
enum EvaluatedValue {
    Null,
    Vertex(VertexId),
    Relationship(RelationshipId),
    Scalar(ScalarValue),
    Truth(Option<bool>),
    /// A list value: a literal, a list parameter, a collected aggregate read
    /// back from a projection, or an UNWIND source.
    List(Vec<EvaluatedValue>),
    /// A path value (GQL §4.15.2).
    Path(Box<PathValue>),
    /// A record (GQL §4.15.4): fields ordered by name.
    Record(Vec<(Box<str>, EvaluatedValue)>),
    /// A temporal instant or duration (GQL §4.16.6).
    Temporal(TemporalValue),
}

pub struct CypherEngine<S> {
    storage: S,
    /// What `SESSION_USER` reads (GQL §20.3); [`crate::DEFAULT_SESSION_USER`]
    /// when unset.
    session_user: Option<String>,
}

impl<S> CypherEngine<S> {
    pub fn new(storage: S) -> Self {
        Self {
            storage,
            session_user: None,
        }
    }

    /// The session's authorization identifier, which `SESSION_USER` reads.
    pub fn with_session_user(mut self, session_user: impl Into<String>) -> Self {
        self.session_user = Some(session_user.into());
        self
    }

    pub fn prepare(&self, query: &str, parameters: &ParameterMap) -> EngineResult<PreparedQuery> {
        crate::prepare::prepare_query_as(
            query,
            parameters,
            &crate::ListParameterMap::new(),
            &crate::UnknownStatistics,
            self.session_user.as_deref(),
        )
    }

    /// Preparation with list-valued parameters, which `IN` needs. `prepare`
    /// stays list-free so existing callers are unaffected.
    pub fn prepare_with_lists(
        &self,
        query: &str,
        parameters: &ParameterMap,
        lists: &crate::ListParameterMap,
    ) -> EngineResult<PreparedQuery> {
        crate::prepare::prepare_query_as(query, parameters, lists, &crate::UnknownStatistics, self.session_user.as_deref())
    }

    /// Lower without selecting a physical plan. This preserves both parameter
    /// maps while a storage adapter loads statistics for the pinned snapshot.
    pub fn lower_with_lists(
        &self,
        query: &str,
        parameters: &ParameterMap,
        lists: &crate::ListParameterMap,
    ) -> EngineResult<crate::LogicalQuery> {
        crate::prepare::lower_query_as(query, parameters, lists, self.session_user.as_deref())
    }

    pub fn prepare_with_statistics(
        &self,
        query: &str,
        parameters: &ParameterMap,
        statistics: &dyn crate::StatisticsProvider,
    ) -> EngineResult<PreparedQuery> {
        crate::prepare::prepare_query_as(
            query,
            parameters,
            &crate::ListParameterMap::new(),
            statistics,
            self.session_user.as_deref(),
        )
    }
}

impl<S: GraphStorage> CypherEngine<S> {
    pub async fn execute(&self, query: &str, parameters: &ParameterMap) -> EngineResult<QueryResult> {
        let prepared = self.prepare(query, parameters)?;
        self.execute_prepared(&prepared).await
    }

    /// Executes the exact physical artifact held by `prepared` without
    /// re-parsing, re-lowering, or re-planning.
    ///
    /// Capability is settled before the read session opens. It used to be
    /// settled by whichever operator first refused the plan, which meant an
    /// unsupported shape had already cost a `begin_read` - and, through a
    /// storage adapter, a snapshot - before anyone could say it was
    /// unsupported.
    pub async fn execute_prepared(&self, prepared: &PreparedQuery) -> EngineResult<QueryResult> {
        self.execute_prepared_with_status(prepared).await.map(|outcome| outcome.result)
    }

    /// [`Self::execute`], reporting the outcome's GQL-status objects too.
    pub async fn execute_with_status(&self, query: &str, parameters: &ParameterMap) -> EngineResult<ExecutionOutcome> {
        let prepared = self.prepare(query, parameters)?;
        self.execute_prepared_with_status(&prepared).await
    }

    /// [`Self::execute_prepared`], reporting the outcome's GQL-status objects
    /// too (GQL §4.9). A failure is the [`EngineError`], whose
    /// [`EngineError::status_object`] is the failed outcome's status.
    pub async fn execute_prepared_with_status(&self, prepared: &PreparedQuery) -> EngineResult<ExecutionOutcome> {
        prepared.capability().into_result()?;
        let mut read = self.storage.begin_read(ReadRequest::default()).await?;
        // Only a plan that can reach the same vertex twice is wrapped: the map
        // is a cost on every other shape and a saving on none of them.
        if plan_reuses_hydration(&prepared.physical) {
            let mut read = HydrationIdentityMap::new(read.as_mut());
            execute_with_read(&mut read, &prepared.physical).await
        } else {
            execute_with_read(read.as_mut(), &prepared.physical).await
        }
    }
}

async fn execute_with_read(read: &mut dyn GraphRead, plan: &GraphPhysicalPlan) -> EngineResult<ExecutionOutcome> {
    let mut read = ExecutionRead::new(read);
    let result = execute_result_plan(&mut read, plan).await?;
    Ok(ExecutionOutcome::new(result, read.take_conditions(), plan.result_omitted()))
}

/// A successful execution outcome (GQL §4.8.4): the result and the
/// GQL-status objects describing it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExecutionOutcome {
    pub result: QueryResult,
    /// The outcome's status, chosen by §4.9.3 precedence: no data (`02000`)
    /// for an empty result over any warning, a warning such as `01G11` over
    /// success, and success (`00000`) otherwise; for `FINISH`, success with
    /// an omitted result (`00001`), which no data never displaces.
    pub status: GqlStatusObject,
    /// Every other condition raised, as additional status objects.
    pub additional: Vec<GqlStatusObject>,
    /// Whether the result is omitted (FINISH), whatever the status says:
    /// under a warning the warning is primary and `00001` an additional one.
    omitted: bool,
    /// Raw conditions, before choosing precedence or deriving row emptiness.
    raised: Vec<GqlStatus>,
}

impl ExecutionOutcome {
    fn new(result: QueryResult, raised: Vec<GqlStatus>, omitted: bool) -> Self {
        let mut conditions: Vec<GqlStatus> = Vec::new();
        for condition in raised.iter().copied() {
            if !conditions.contains(&condition) {
                conditions.push(condition);
            }
        }
        if result.rows.is_empty() && !omitted && !raised.contains(&GqlStatus::OMITTED_RESULT) {
            conditions.push(GqlStatus::NO_DATA);
        }
        let success = if omitted { GqlStatus::OMITTED_RESULT } else { GqlStatus::SUCCESS };
        let primary = conditions
            .iter()
            .copied()
            .enumerate()
            .max_by_key(|(index, condition)| (condition.precedence(), std::cmp::Reverse(*index)))
            .map(|(index, _)| conditions.remove(index));
        // A warning outranks the omitted result, which is still reported.
        if omitted && primary.is_some() {
            conditions.push(GqlStatus::OMITTED_RESULT);
        }
        Self {
            result,
            status: GqlStatusObject::new(primary.unwrap_or(success)),
            additional: conditions.into_iter().map(GqlStatusObject::new).collect(),
            omitted,
            raised,
        }
    }

    /// Whether the statement's result is omitted (§14.10, FINISH).
    pub fn result_omitted(&self) -> bool {
        self.omitted
    }

    /// This outcome with its result cut to a client's window: `skip` rows
    /// dropped and at most `limit` kept. The status is chosen again for the
    /// rows that remain, so a window that leaves none is no data (`02000`),
    /// as it would be had the query itself returned nothing.
    pub fn windowed(self, skip: usize, limit: Option<usize>) -> Self {
        let mut result = self.result;
        result.rows = result.rows.into_iter().skip(skip).take(limit.unwrap_or(usize::MAX)).collect();
        Self::new(result, self.raised, self.omitted)
    }
}

// Operator dispatch. Every physical operator invocation enters through one of
// the two `execute_*_plan` wrappers below, which bracket the real dispatch with
// `GraphRead::operator_started`/`operator_finished`. That pair is the whole of
// the executor's profiling surface: operator bodies stay unaware of it, and a
// new operator is observed the moment it is dispatched.

fn execute_result_plan<'a>(
    read: &'a mut ExecutionRead<'_>,
    plan: &'a GraphPhysicalPlan,
) -> Pin<Box<dyn Future<Output = EngineResult<QueryResult>> + Send + 'a>> {
    Box::pin(async move {
        read.operator_started(plan);
        let result = dispatch_result_plan(&mut *read, plan).await;
        read.operator_finished(plan, result.as_ref().map_or(0, |result| result.rows.len()));
        result.map_err(|error| error.in_operation(plan.operation()))
    })
}

fn dispatch_result_plan<'a>(
    read: &'a mut ExecutionRead<'_>,
    plan: &'a GraphPhysicalPlan,
) -> Pin<Box<dyn Future<Output = EngineResult<QueryResult>> + Send + 'a>> {
    Box::pin(async move {
        // FINISH runs its statement, for its conditions and its errors, and
        // has no result to give (§14.10 General Rule 2).
        if let GraphPhysicalPlan::Next { input, next } = plan {
            execute_result_plan(read, input).await?;
            return execute_result_plan(read, next).await;
        }
        if let GraphPhysicalPlan::Finish { input } = plan {
            execute_binding_plan(read, input, None).await?;
            return Ok(QueryResult {
                columns: Vec::new(),
                rows: Vec::new(),
            });
        }
        let GraphPhysicalPlan::Union { arms, all, conjunction } = plan else {
            return execute_project(read, plan).await;
        };
        if *conjunction != QueryConjunction::Union {
            let mut columns = None;
            let mut combined = ConjunctionFold::new(*conjunction, *all);
            for arm in arms {
                // §14.2 General Rule 2: OTHERWISE is its first arm's result
                // when that has rows, and the later arms are never run.
                if combined.satisfied() {
                    break;
                }
                let result = execute_result_plan(read, arm).await?;
                if columns.as_ref().is_some_and(|columns| columns != &result.columns) {
                    return Err(shape_error(
                        UNION_COLUMN_MISMATCH,
                        "all arms of a composite query must return the same column names",
                    ));
                }
                columns.get_or_insert(result.columns);
                read.checkpoint("cypher_composite_rows")?;
                combined.push(result.rows, SemanticRow::new, &mut |operation| Ok(read.checkpoint(operation)?))?;
            }
            let columns = columns.ok_or_else(|| shape_error(UNION_EMPTY, "a composite query requires at least one arm"))?;
            return Ok(QueryResult {
                columns,
                rows: combined.into_rows(|row| row.0, &mut |operation| Ok(read.checkpoint(operation)?))?,
            });
        }
        let mut combined: Option<QueryResult> = None;
        // `UNION` keeps the first occurrence of every row across all arms,
        // including duplicates inside the first arm, as legacy does. An
        // `IndexSet` is that rule: insertion-ordered, and an insert of a row
        // already held keeps the one already held.
        let mut deduped: IndexSet<SemanticRow> = IndexSet::new();
        let mut row_ordinal = 0;
        // Arms run side by side when the backend can fork a read for each,
        // and are folded below in arm order either way, so the rows, the first
        // error and the completion conditions are those of running them in turn.
        let mut concurrent = execute_union_arms_concurrently(read, arms).await;
        for arm in arms {
            let result = match concurrent.as_mut() {
                Some(results) => results.next().expect("one result per UNION arm")?,
                None => execute_result_plan(read, arm).await?,
            };
            let combined = match &mut combined {
                Some(combined) => {
                    if combined.columns != result.columns {
                        return Err(shape_error(
                            UNION_COLUMN_MISMATCH,
                            "all UNION arms must return the same column names",
                        ));
                    }
                    combined
                }
                None => combined.insert(QueryResult {
                    columns: result.columns,
                    rows: Vec::new(),
                }),
            };
            if *all {
                combined.rows.extend(result.rows);
                continue;
            }
            // Accumulated in the set rather than pushed through it, so the
            // rows are held once. `combined.rows` takes them below.
            for row in result.rows {
                read.checkpoint("cypher_union_rows")?;
                deduped.insert(SemanticRow::new(row, row_ordinal));
                row_ordinal += 1;
            }
        }
        if !*all {
            if let Some(combined) = &mut combined {
                combined.rows = deduped.into_iter().map(|row| row.0).collect();
            }
        }
        combined.ok_or_else(|| shape_error(UNION_EMPTY, "UNION requires at least one arm"))
    })
}

/// Most `UNION` arms one execution runs at the same time.
const UNION_ARM_CONCURRENCY: usize = 8;

/// Runs every arm of a `UNION` on its own forked read, at most
/// [`UNION_ARM_CONCURRENCY`] at a time, and returns their results in arm
/// order. Each arm's completion conditions are raised on `read` in arm order
/// before its result is handed back. `None` when there is nothing to overlap
/// or the backend cannot fork a read; the caller then runs the arms in turn.
///
/// An arm is independent of its siblings: it reads the same snapshot, binds
/// nothing they use, and the backend's fork shares the per-query limits, so
/// overlapping arms changes only how long the union takes. Arms after a
/// failing one still run, which costs reads but changes no result: the
/// caller returns the first failure in arm order, as running them in turn did.
async fn execute_union_arms_concurrently(
    read: &mut ExecutionRead<'_>,
    arms: &[GraphPhysicalPlan],
) -> Option<std::vec::IntoIter<EngineResult<QueryResult>>> {
    use futures::StreamExt as _;
    if arms.len() < 2 {
        return None;
    }
    // Taken before the forks borrow `read`. Each arm publishes into a sink of
    // its own while it runs, and into this one once it is done: a mark an arm
    // returns may be read after that.
    let sink = read.sink();
    let mut forks = Vec::with_capacity(arms.len());
    for _ in arms {
        forks.push(read.fork()?);
    }
    type ArmOutcome = (EngineResult<QueryResult>, Vec<GqlStatus>);
    let mut branches: Vec<Pin<Box<dyn Future<Output = ArmOutcome> + Send + '_>>> = Vec::with_capacity(arms.len());
    for (arm, mut fork) in arms.iter().zip(forks) {
        let sink = sink.clone();
        branches.push(Box::pin(crate::observe::in_new_lane(async move {
            let mut branch = ExecutionRead::branch(&mut *fork, sink);
            let result = execute_result_plan(&mut branch, arm).await;
            let conditions = branch.finish();
            (result, conditions)
        })));
    }
    let outcomes: Vec<ArmOutcome> = futures::stream::iter(branches).buffered(UNION_ARM_CONCURRENCY).collect().await;
    let mut results = Vec::with_capacity(outcomes.len());
    for (result, conditions) in outcomes {
        read.raise_conditions(conditions);
        results.push(result);
    }
    Some(results.into_iter())
}

/// [`execute_union_arms_concurrently`] for a `UNION` the planner placed under
/// a projection, whose arms produce binding rows rather than results. Each arm
/// starts from the same `seed` and `fixed` record, which it only reads.
async fn execute_union_binding_arms_concurrently<'s>(
    read: &mut ExecutionRead<'_>,
    arms: &'s [GraphPhysicalPlan],
    seed: Option<&'s BindingRow>,
    fixed: &'s FixedRecord,
) -> Option<std::vec::IntoIter<EngineResult<Vec<BindingRow>>>> {
    use futures::StreamExt as _;
    if arms.len() < 2 {
        return None;
    }
    // Taken before the forks borrow `read`. Each arm publishes into a sink of
    // its own while it runs, and into this one once it is done: a mark an arm
    // returns may be read after that.
    let sink = read.sink();
    let mut forks = Vec::with_capacity(arms.len());
    for _ in arms {
        forks.push(read.fork()?);
    }
    type ArmOutcome = (EngineResult<Vec<BindingRow>>, Vec<GqlStatus>);
    let mut branches: Vec<Pin<Box<dyn Future<Output = ArmOutcome> + Send + '_>>> = Vec::with_capacity(arms.len());
    for (arm, mut fork) in arms.iter().zip(forks) {
        let sink = sink.clone();
        branches.push(Box::pin(crate::observe::in_new_lane(async move {
            let mut branch = ExecutionRead::branch(&mut *fork, sink);
            let result = execute_binding_plan_in_context(&mut branch, arm, None, seed, fixed).await;
            let conditions = branch.finish();
            (result, conditions)
        })));
    }
    // The caller checks the combined rows against the intermediate-row limit
    // arm by arm, as running in turn did. Checked here too as each arm lands,
    // so a union that crosses the limit stops before the remaining arms have
    // produced and retained their rows; at most UNION_ARM_CONCURRENCY arms
    // are in flight past that point, each under its own per-read limit.
    let mut arms_in_order = futures::stream::iter(branches).buffered(UNION_ARM_CONCURRENCY);
    let mut outcomes: Vec<ArmOutcome> = Vec::with_capacity(arms.len());
    let mut combined_rows = 0_usize;
    while let Some((result, conditions)) = arms_in_order.next().await {
        // The arm that crosses the limit carries the limit's error as its
        // outcome, so the caller returns it there and never asks for an arm
        // past a short collection.
        let result = match result {
            Ok(rows) => {
                combined_rows = combined_rows.saturating_add(rows.len());
                match read.check_intermediate_rows("cypher_union_intermediate_rows", combined_rows) {
                    Ok(()) => Ok(rows),
                    Err(error) => Err(error.into()),
                }
            }
            Err(error) => Err(error),
        };
        let stop = result.is_err();
        outcomes.push((result, conditions));
        if stop {
            break;
        }
    }
    drop(arms_in_order);
    let mut results = Vec::with_capacity(outcomes.len());
    for (result, conditions) in outcomes {
        read.raise_conditions(conditions);
        results.push(result);
    }
    Some(results.into_iter())
}

async fn execute_project(read: &mut ExecutionRead<'_>, plan: &GraphPhysicalPlan) -> EngineResult<QueryResult> {
    let GraphPhysicalPlan::Project {
        context,
        operation: _,
        input,
        items,
        distinct,
        post_sort,
        post_skip,
        post_limit,
    } = plan
    else {
        return Err(shape_error(
            RESULT_PLAN_ROOT,
            "a result-producing plan must have Project at its root",
        ));
    };
    // A projection that only counts an index-only range seek's rows needs
    // the count, not the rows: walked without keeping any, it is bounded by
    // what it reads rather than by how many rows match.
    if let Some(count) = crate::plan::count_only_range_seek(plan) {
        if let Some(counted) = vertex_scan::count_range_seek(&mut **read, &count).await? {
            return Ok(QueryResult {
                columns: count.items.iter().map(|item| QueryColumn { name: item.column_name() }).collect(),
                rows: vec![QueryRow {
                    values: vec![QueryValue::Count(counted); count.items.len()],
                }],
            });
        }
    }
    let fixed = FixedRecord::new(None);
    let binding_rows = execute_binding_plan_in_context(read, input, None, None, &fixed).await?;
    let record = fixed.readable(&context.fixed);
    let columns = items
        .iter()
        .map(|item| QueryColumn { name: item.column_name() })
        .collect::<Vec<_>>();
    let aggregate = items.iter().any(|item| expression_contains_aggregate(&item.expression))
        || post_sort.iter().any(|item| expression_contains_aggregate(&item.expression));
    let mut projected = if aggregate {
        aggregate_project_rows(&mut **read, &binding_rows, items, post_sort, &record, context)?
    } else {
        binding_rows
            .iter()
            .map(|binding_row| {
                let values = items
                    .iter()
                    .map(|item| evaluate_expression(&item.expression, binding_row).map(to_query_value))
                    .collect::<EngineResult<Vec<_>>>()?;
                Ok((QueryRow { values }, Vec::new()))
            })
            .collect::<EngineResult<Vec<_>>>()?
    };
    if *distinct {
        // Keyed by the row, carrying its sort keys. `or_insert` keeps the
        // first occurrence's keys, and the map keeps first-seen order.
        let mut unique: IndexMap<SemanticRow, Vec<QueryValue>> = IndexMap::with_capacity(projected.len());
        for (ordinal, (row, keys)) in projected.into_iter().enumerate() {
            read.checkpoint("cypher_distinct_rows")?;
            unique.entry(SemanticRow::new(row, ordinal)).or_insert(keys);
        }
        projected = unique.into_iter().map(|(row, keys)| (row.0, keys)).collect();
    }
    if aggregate && !post_sort.is_empty() {
        projected.sort_by(|(_, left), (_, right)| compare_projected_sort_keys(left, right, post_sort));
    }
    let skip = post_skip
        .as_ref()
        .map(|count| window_count(count, "SKIP"))
        .transpose()?
        .unwrap_or(0);
    let limit = post_limit.as_ref().map(|count| window_count(count, "LIMIT")).transpose()?;
    let mut rows = projected.into_iter().skip(skip).map(|(row, _)| row).collect::<Vec<_>>();
    if let Some(limit) = limit {
        rows.truncate(limit);
    }
    Ok(QueryResult { columns, rows })
}

/// Project binding rows into a new scope.
///
/// The output rows carry only what the projection names. An item that is a
/// bare binding carries its record across, so a vertex projected under a new
/// name keeps its properties; anything else is evaluated to a value and bound
/// by its alias. Ordering and the window read the projected scope, which is
/// where Cypher puts them; after grouping or DISTINCT they apply to the
/// projected rows, and otherwise they choose the rows first
/// ([`project_window_first`]).
fn project_binding_rows(
    read: &mut dyn GraphRead,
    input_rows: Vec<BindingRow>,
    plan: &GraphPhysicalPlan,
    fixed: &BindingRow,
) -> EngineResult<Vec<BindingRow>> {
    let GraphPhysicalPlan::Project {
        context,
        items,
        distinct,
        post_sort,
        post_skip,
        post_limit,
        ..
    } = plan
    else {
        return Err(shape_error(RESULT_PLAN_ROOT, "expected a table projection"));
    };
    let distinct = *distinct;
    let aggregate = items.iter().any(|item| expression_contains_aggregate(&item.expression));
    let sort_fixed = (!context.fixed.is_empty()).then_some(fixed);
    if !aggregate && !distinct {
        return project_window_first(input_rows, items, post_sort, post_skip, post_limit, sort_fixed);
    }
    let mut projected = if aggregate {
        Vec::new()
    } else {
        Vec::with_capacity(input_rows.len())
    };
    // A sort key after grouping may read an aggregate (`ORDER BY c` for
    // `count(*) AS c` lowers back to the aggregate), so it is computed over
    // each group, as the result projection computes it.
    let mut group_keys: Vec<Vec<Option<QueryValue>>> = Vec::new();
    let needs_input = sort_needs_input(post_sort, items);
    if aggregate {
        // Grouping at the boundary: one output row per group of the items
        // that do not aggregate, exactly as RETURN groups. A grouping key
        // that is a bare binding carries its entity across, so
        // `WITH n, count(*) AS c MATCH (n)-->()` still knows `n`.
        let eliminated = Cell::new(false);
        for group in group_binding_rows(read, &input_rows, items, fixed, context)?.into_values() {
            let mut output = BindingRow::default();
            let scope = group.first().map_or_else(|| fixed.clone(), |first| fixed.overlaid(first));
            for item in items {
                let name = projected_symbol(item)?;
                if group.is_empty() && matches!(&item.expression, PhysicalExpression::Binding(source) if fixed.binds(source)) {
                    project_row_item(fixed, item, &mut output)?;
                    continue;
                }
                project_group_column(
                    &group,
                    item,
                    name,
                    || evaluate_group_expression_in_scope(&item.expression, &group, &eliminated, Some(&scope)),
                    &scope,
                    &mut output,
                )?;
            }
            // A key reads the projected names over the input's (ORDER BY
            // after a projection sees what it made), its aggregates the group.
            if !post_sort.is_empty() {
                let over = output.for_sort(group.first().copied(), needs_input, sort_fixed);
                group_keys.push(
                    post_sort
                        .iter()
                        .map(|item| evaluate_group_expression_in_scope(&item.expression, &group, &eliminated, Some(&over)).map(Some))
                        .collect::<EngineResult<Vec<_>>>()?,
                );
            } else {
                group_keys.push(Vec::new());
            }
            projected.push(output);
        }
        if eliminated.get() {
            read.raise_completion_condition(GqlStatus::NULL_VALUE_ELIMINATED_IN_SET_FUNCTION);
        }
    }
    for row in input_rows.iter().filter(|_| !aggregate) {
        let mut output = BindingRow::default();
        for item in items {
            project_row_item(row, item, &mut output)?;
        }
        if !post_sort.is_empty() {
            let over = output.for_sort(Some(row), needs_input, sort_fixed);
            group_keys.push(
                post_sort
                    .iter()
                    .map(|item| evaluate_expression(&item.expression, &over).map(|value| Some(to_query_value(value))))
                    .collect::<EngineResult<Vec<_>>>()?,
            );
        } else {
            group_keys.push(Vec::new());
        }
        projected.push(output);
    }

    // Each row's sort keys were read as it was projected.
    let mut keyed: Vec<(BindingRow, Option<Vec<Option<QueryValue>>>)> =
        projected.into_iter().zip(group_keys.into_iter().map(Some)).collect();
    if distinct {
        // Same change as the binding-level UNION above, for the same reason.
        // The first occurrence keeps its sort keys.
        let mut unique: IndexMap<SemanticBindingRow, Option<Vec<Option<QueryValue>>>> = IndexMap::with_capacity(keyed.len());
        for (ordinal, (row, keys)) in keyed.into_iter().enumerate() {
            read.checkpoint("cypher_project_distinct")?;
            unique.entry(SemanticBindingRow::new(row, ordinal)).or_insert(keys);
        }
        keyed = unique.into_iter().map(|(row, keys)| (row.0, keys)).collect();
    }
    let projected: Vec<BindingRow> = if post_sort.is_empty() {
        keyed.into_iter().map(|(row, _)| row).collect()
    } else {
        let mut keyed = keyed
            .into_iter()
            .map(|(row, keys)| {
                let keys = post_sort
                    .iter()
                    .enumerate()
                    .map(|(index, item)| match keys.as_ref().and_then(|keys| keys[index].clone()) {
                        Some(key) => Ok(key),
                        None => evaluate_expression(&item.expression, &row).map(to_query_value),
                    })
                    .collect::<EngineResult<Vec<_>>>()?;
                Ok((row, keys))
            })
            .collect::<EngineResult<Vec<_>>>()?;
        keyed.sort_by(|(_, left), (_, right)| compare_projected_sort_keys(left, right, post_sort));
        keyed.into_iter().map(|(row, _)| row).collect()
    };

    let skip = post_skip
        .as_ref()
        .map(|count| window_count(count, "SKIP"))
        .transpose()?
        .unwrap_or(0);
    let limit = post_limit.as_ref().map(|count| window_count(count, "LIMIT")).transpose()?;
    let mut rows = projected.into_iter().skip(skip).collect::<Vec<_>>();
    if let Some(limit) = limit {
        rows.truncate(limit);
    }
    Ok(rows)
}

/// A projection that neither groups nor drops duplicates maps rows one to
/// one, so its ORDER BY, SKIP and LIMIT choose the rows before the rest of it
/// is computed, as a result statement's do: a sort key is read off the items
/// it names (over the input row when it reads a name the projection drops,
/// [`sort_needs_input`]), and the other items are evaluated only for the rows the window
/// keeps. A row the window drops never raises an exception from an item no
/// key reads, and each item is evaluated once per row it is evaluated for.
fn project_window_first(
    input_rows: Vec<BindingRow>,
    items: &[crate::PhysicalProjection],
    post_sort: &[PhysicalSort],
    post_skip: &Option<BoundValue>,
    post_limit: &Option<BoundValue>,
    fixed: Option<&BindingRow>,
) -> EngineResult<Vec<BindingRow>> {
    // Without a window every row is kept, so every item is computed in the
    // one pass over the input, in its order.
    let windowed = post_skip.is_some() || post_limit.is_some();
    let keyed = items
        .iter()
        .map(|item| match projected_symbol(item) {
            Ok(name) => !windowed || post_sort.iter().any(|sort| row_reads(&sort.expression, &|read| *read == name)),
            // Refused for every row, so for the first.
            Err(_) => true,
        })
        .collect::<Vec<_>>();
    let needs_input = sort_needs_input(post_sort, items);
    let mut chosen = Vec::with_capacity(input_rows.len());
    for (index, row) in input_rows.iter().enumerate() {
        let mut output = BindingRow::default();
        for (item, _) in items.iter().zip(&keyed).filter(|(_, keyed)| **keyed) {
            project_row_item(row, item, &mut output)?;
        }
        let keys = if post_sort.is_empty() {
            Vec::new()
        } else {
            let over = output.for_sort(Some(row), needs_input, fixed);
            post_sort
                .iter()
                .map(|sort| evaluate_expression(&sort.expression, &over).map(to_query_value))
                .collect::<EngineResult<Vec<_>>>()?
        };
        chosen.push((index, output, keys));
    }
    if !post_sort.is_empty() {
        chosen.sort_by(|(_, _, left), (_, _, right)| compare_projected_sort_keys(left, right, post_sort));
    }
    let skip = post_skip
        .as_ref()
        .map(|count| window_count(count, "SKIP"))
        .transpose()?
        .unwrap_or(0);
    let limit = post_limit.as_ref().map(|count| window_count(count, "LIMIT")).transpose()?;
    chosen
        .into_iter()
        .skip(skip)
        .take(limit.unwrap_or(usize::MAX))
        .map(|(index, mut output, _)| {
            for (item, _) in items.iter().zip(&keyed).filter(|(_, keyed)| !**keyed) {
                project_row_item(&input_rows[index], item, &mut output)?;
            }
            Ok(output)
        })
        .collect()
}

/// Project one item of `row` into `output`: a bound element is carried with
/// its record, anything else is evaluated to a value bound by its alias.
fn project_row_item(row: &BindingRow, item: &crate::PhysicalProjection, output: &mut BindingRow) -> EngineResult<()> {
    let name = projected_symbol(item)?;
    match &item.expression {
        PhysicalExpression::Binding(source) => carry_binding(row, source, name, output),
        PhysicalExpression::Identity(source) if identity_is_binding(row, source) => carry_binding(row, source, name, output),
        expression => {
            let value = to_query_value(evaluate_expression(expression, row)?);
            if let Some(records) = retain_relationship_records(std::iter::once(row), &value) {
                output.relationship_lists.insert(name.clone(), records);
            }
            retain_vertex_records(std::iter::once(row), &value, &mut output.vertex_records);
            output.values.insert(name, value);
            Ok(())
        }
    }
}

/// Carry `source` from `row` into `output` under `name`.
///
/// The entity, not its id: `WITH n AS m` must leave `m.name` readable, and an
/// id cannot answer that.
fn carry_binding(row: &BindingRow, source: &Symbol, name: Symbol, output: &mut BindingRow) -> EngineResult<()> {
    // Projecting a subquery's mark reads it, so the exception computing it
    // raised is raised here (a CASE branch not taken reads nothing).
    row.read_mark(source)?;
    if let Some(vertex) = row.vertices.get(source) {
        output.vertices.insert(name, vertex.clone());
    } else if let Some(relationship) = row.relationships.get(source) {
        output.relationships.insert(name, relationship.clone());
    } else if row.is_null(source) {
        // A name an OPTIONAL left null stays *null-bound*, not merely valued
        // null. `nulls` documents why the two cannot be collapsed: a later
        // clause scanning for a name Cypher says is null would bind something
        // arbitrary to it. Projecting it into `values` alone did exactly that
        // -- `OPTIONAL MATCH (a)-[:R]->(p) WITH p MATCH (p:Person)` scanned
        // Person and answered a cross product where the row should have gone.
        output.nulls.insert(name);
    } else if let Some(value) = row.values.get(source) {
        if let Some(records) = row
            .relationship_lists
            .get(source)
            .cloned()
            .or_else(|| retain_relationship_records(std::iter::once(row), value))
        {
            output.relationship_lists.insert(name.clone(), records);
        }
        retain_vertex_records(std::iter::once(row), value, &mut output.vertex_records);
        output.values.insert(name, value.clone());
    } else {
        // Not in scope at all rather than null-bound. Lowering rejects that,
        // so this is a belt-and-braces null and is marked as one for the same
        // reason as above.
        output.nulls.insert(name);
    }
    Ok(())
}

/// Carry only records referenced by a computed result, including nested lists
/// and collected relationships. This metadata is not part of logical row keys.
fn retain_relationship_records<'a>(
    rows: impl IntoIterator<Item = &'a BindingRow>,
    value: &QueryValue,
) -> Option<std::sync::Arc<Vec<RelationshipRecord>>> {
    fn identities(value: &QueryValue, into: &mut BTreeSet<RelationshipId>) {
        match value {
            QueryValue::RelationshipId(id) => {
                into.insert(*id);
            }
            QueryValue::List(values) => {
                for value in values {
                    identities(value, into);
                }
            }
            QueryValue::Record(fields) => {
                for (_, value) in fields {
                    identities(value, into);
                }
            }
            _ => {}
        }
    }
    let mut wanted = BTreeSet::new();
    identities(value, &mut wanted);
    if wanted.is_empty() {
        return None;
    }
    let mut found = BTreeMap::new();
    for row in rows {
        for records in row.relationship_lists.values() {
            if records.len() == wanted.len() && records.iter().all(|record| wanted.contains(&record.id)) {
                return Some(records.clone());
            }
        }
        for record in row_relationship_records(row) {
            if wanted.contains(&record.id) {
                found.entry(record.id).or_insert_with(|| record.clone());
            }
        }
    }
    (!found.is_empty()).then(|| std::sync::Arc::new(found.into_values().collect()))
}

/// Copy into `into` the records `rows` hold for every node `value` names by
/// identity, at any depth of lists and records.
fn retain_vertex_records<'a>(
    rows: impl IntoIterator<Item = &'a BindingRow>,
    value: &QueryValue,
    into: &mut BTreeMap<VertexId, VertexRecord>,
) {
    fn identities(value: &QueryValue, into: &mut BTreeSet<VertexId>) {
        match value {
            QueryValue::VertexId(id) => {
                into.insert(*id);
            }
            QueryValue::List(values) => values.iter().for_each(|value| identities(value, into)),
            QueryValue::Record(fields) => fields.iter().for_each(|(_, value)| identities(value, into)),
            _ => {}
        }
    }
    let mut wanted = BTreeSet::new();
    identities(value, &mut wanted);
    if wanted.is_empty() {
        return;
    }
    for row in rows {
        for record in row_vertex_records(row) {
            if wanted.contains(&record.id) {
                into.entry(record.id).or_insert_with(|| record.clone());
            }
        }
    }
}

/// The node `binding` names in `row`: bound by a pattern, or held by
/// identity in a value whose record the row carries.
fn row_vertex<'a>(row: &'a BindingRow, binding: &Symbol) -> Option<&'a VertexRecord> {
    row.vertices.get(binding).or_else(|| match row.values.get(binding) {
        Some(QueryValue::VertexId(id)) => row_vertex_records(row).find(|record| record.id == *id),
        _ => None,
    })
}

/// Group `rows` by the values of the items that do not aggregate.
///
/// Groups keep first-seen order, and the key is structural value equality
/// (see `QueryValue`), so every NULL key forms one group. With no grouping
/// keys, no input still forms one empty group: `count(*)` over nothing is a
/// row saying 0, not no row.
fn group_binding_rows<'a>(
    read: &dyn GraphRead,
    rows: &'a [BindingRow],
    items: &[crate::PhysicalProjection],
    fixed: &BindingRow,
    context: &crate::ProjectionContext,
) -> EngineResult<IndexMap<SemanticValues, Vec<&'a BindingRow>>> {
    let mut grouping = Vec::new();
    for item in items {
        collect_grouping_expressions(&item.expression, &mut grouping);
    }
    if !context.explicit_grouping {
        grouping.retain(|expression| row_reads(expression, &|name| context.table_columns.contains(name) || !fixed.binds(name)));
    }
    let mut groups: IndexMap<SemanticValues, Vec<&BindingRow>> = IndexMap::new();
    for (ordinal, row) in rows.iter().enumerate() {
        read.checkpoint("cypher_group_rows")?;
        let key = grouping
            .iter()
            .map(|expression| evaluate_expression(expression, row).map(to_query_value))
            .collect::<EngineResult<Vec<_>>>()?;
        groups.entry(SemanticValues::new(key, ordinal)).or_default().push(row);
    }
    if groups.is_empty() && grouping.is_empty() {
        groups.insert(SemanticValues::new(Vec::new(), 0), Vec::new());
    }
    Ok(groups)
}

/// The name a projected item binds in the new scope.
///
/// An alias names itself. A bare binding keeps its own name, which is what
/// makes `WITH a, b` a pass-through. Anything else has no name to be read by
/// later, and is refused rather than invented - the same answer the capability
/// check gives before execution starts.
fn projected_symbol(item: &crate::PhysicalProjection) -> EngineResult<Symbol> {
    nested_projection_name(item).ok_or_else(|| {
        shape_error(
            NESTED_PROJECT_UNNAMED,
            "a projection below a result plan must alias every computed item",
        )
    })
}

pub(crate) fn expression_contains_aggregate(expression: &PhysicalExpression) -> bool {
    match expression {
        PhysicalExpression::Aggregate { group, .. } => group.is_none(),
        PhysicalExpression::InList { expression, .. } => expression_contains_aggregate(expression),
        PhysicalExpression::Unary { expression, .. }
        | PhysicalExpression::TypeTest { expression, .. }
        | PhysicalExpression::Cast { expression, .. }
        | PhysicalExpression::Field { expression, .. } => expression_contains_aggregate(expression),
        PhysicalExpression::Record(fields) => fields.iter().any(|(_, value)| expression_contains_aggregate(value)),
        PhysicalExpression::Let { bindings, body } => {
            bindings.iter().any(|(_, value)| expression_contains_aggregate(value)) || expression_contains_aggregate(body)
        }
        PhysicalExpression::PathConstructor(elements) => elements.iter().any(expression_contains_aggregate),
        PhysicalExpression::Function { arguments, .. } | PhysicalExpression::List(arguments) => {
            arguments.iter().any(expression_contains_aggregate)
        }
        PhysicalExpression::Case { .. } => case_parts(expression).into_iter().any(expression_contains_aggregate),
        PhysicalExpression::Binary { left, right, .. } => expression_contains_aggregate(left) || expression_contains_aggregate(right),
        PhysicalExpression::Binding(_)
        | PhysicalExpression::Identity(_)
        | PhysicalExpression::ElementTest { .. }
        | PhysicalExpression::PathNodesDistinct { .. }
        | PhysicalExpression::Property { .. }
        | PhysicalExpression::Value(_) => false,
    }
}

fn aggregate_project_rows(
    read: &mut dyn GraphRead,
    rows: &[BindingRow],
    items: &[crate::PhysicalProjection],
    sort: &[PhysicalSort],
    fixed: &BindingRow,
    context: &crate::ProjectionContext,
) -> EngineResult<Vec<(QueryRow, Vec<QueryValue>)>> {
    let eliminated = Cell::new(false);
    let needs_input = sort_needs_input(sort, items);
    let sort_fixed = (!context.fixed.is_empty()).then_some(fixed);
    let projected = group_binding_rows(read, rows, items, fixed, context)?
        .into_iter()
        .map(|(_, group)| {
            let scope = group.first().map_or_else(|| fixed.clone(), |first| fixed.overlaid(first));
            let values = items
                .iter()
                .map(|item| evaluate_group_expression_in_scope(&item.expression, &group, &eliminated, Some(&scope)))
                .collect::<EngineResult<Vec<_>>>()?;
            if sort.is_empty() {
                return Ok((QueryRow { values }, Vec::new()));
            }
            // Result aliases shadow input bindings in sort keys.
            let mut columns = BindingRow::default();
            for (item, value) in items.iter().zip(&values) {
                if let Ok(name) = projected_symbol(item) {
                    project_group_column(&group, item, name, || Ok(value.clone()), &scope, &mut columns)?;
                }
            }
            let scope = columns.for_sort(group.first().copied(), needs_input, sort_fixed);
            let sort_values = sort
                .iter()
                .map(|item| evaluate_group_expression_in_scope(&item.expression, &group, &eliminated, Some(&scope)))
                .collect::<EngineResult<Vec<_>>>()?;
            Ok((QueryRow { values }, sort_values))
        })
        .collect::<EngineResult<Vec<_>>>()?;
    if eliminated.get() {
        read.raise_completion_condition(GqlStatus::NULL_VALUE_ELIMINATED_IN_SET_FUNCTION);
    }
    Ok(projected)
}

/// One column a grouped projection makes: a bound element is carried from
/// the group's first row with its record, anything else is `value` with the
/// records of the elements it holds, so a later read of `n.name` finds them.
fn project_group_column(
    group: &[&BindingRow],
    item: &crate::PhysicalProjection,
    name: Symbol,
    value: impl FnOnce() -> EngineResult<QueryValue>,
    scope: &BindingRow,
    output: &mut BindingRow,
) -> EngineResult<()> {
    match (&item.expression, group.first()) {
        (PhysicalExpression::Binding(source), Some(first)) => carry_binding(first, source, name, output),
        (PhysicalExpression::Identity(source), Some(first)) if identity_is_binding(first, source) => {
            carry_binding(first, source, name, output)
        }
        _ => {
            let value = value()?;
            if let Some(records) = retain_relationship_records(group.iter().copied().chain(std::iter::once(scope)), &value) {
                output.relationship_lists.insert(name.clone(), records);
            }
            retain_vertex_records(
                group.iter().copied().chain(std::iter::once(scope)),
                &value,
                &mut output.vertex_records,
            );
            output.values.insert(name, value);
            Ok(())
        }
    }
}

/// Whether any sort key after a projection reads an input name the
/// projection does not give. Keys reading only projected names borrow the
/// projected row, without copying the input row per output row.
fn sort_needs_input(sorts: &[PhysicalSort], items: &[crate::PhysicalProjection]) -> bool {
    let projected = items.iter().filter_map(|item| projected_symbol(item).ok()).collect::<BTreeSet<_>>();
    sorts
        .iter()
        .any(|sort| row_reads(&sort.expression, &|name| !projected.contains(name)))
}

fn collect_grouping_expressions<'a>(expression: &'a PhysicalExpression, grouping: &mut Vec<&'a PhysicalExpression>) {
    if matches!(expression, PhysicalExpression::Aggregate { group: None, .. }) {
        return;
    }
    if !expression_contains_aggregate(expression) {
        if expression_requires_row(expression) && !grouping.contains(&expression) {
            grouping.push(expression);
        }
        return;
    }
    match expression {
        PhysicalExpression::Unary { expression, .. }
        | PhysicalExpression::TypeTest { expression, .. }
        | PhysicalExpression::Cast { expression, .. }
        | PhysicalExpression::Field { expression, .. } => {
            collect_grouping_expressions(expression, grouping);
        }
        PhysicalExpression::Record(fields) => {
            for (_, value) in fields {
                collect_grouping_expressions(value, grouping);
            }
        }
        PhysicalExpression::Let { bindings, body } => {
            for (_, value) in bindings {
                collect_grouping_expressions(value, grouping);
            }
            collect_grouping_expressions(body, grouping);
        }
        PhysicalExpression::Function { arguments, .. }
        | PhysicalExpression::List(arguments)
        | PhysicalExpression::PathConstructor(arguments) => {
            for argument in arguments {
                collect_grouping_expressions(argument, grouping);
            }
        }
        PhysicalExpression::Case { .. } => {
            for part in case_parts(expression) {
                collect_grouping_expressions(part, grouping);
            }
        }
        PhysicalExpression::Binary { left, right, .. } => {
            collect_grouping_expressions(left, grouping);
            collect_grouping_expressions(right, grouping);
        }
        // Only the probe can carry an aggregate; the candidate list is
        // constant and never contributes a grouping key.
        PhysicalExpression::InList { expression, .. } => {
            collect_grouping_expressions(expression, grouping);
        }
        PhysicalExpression::Binding(_)
        | PhysicalExpression::Identity(_)
        | PhysicalExpression::ElementTest { .. }
        | PhysicalExpression::PathNodesDistinct { .. }
        | PhysicalExpression::Property { .. }
        | PhysicalExpression::Value(_)
        | PhysicalExpression::Aggregate { .. } => {}
    }
}

fn expression_requires_row(expression: &PhysicalExpression) -> bool {
    row_reads(expression, &|_| true)
}

/// Whether `expression`, outside its aggregates, reads a name `read` accepts.
fn row_reads(expression: &PhysicalExpression, read: &dyn Fn(&Symbol) -> bool) -> bool {
    match expression {
        PhysicalExpression::Binding(binding) | PhysicalExpression::Identity(binding) | PhysicalExpression::Property { binding, .. } => {
            read(binding)
        }
        PhysicalExpression::ElementTest { binding, test } => read(binding) || test.edge().is_some_and(read),
        PhysicalExpression::PathNodesDistinct { parts, .. } => parts.iter().any(|part| read(part.name())),
        PhysicalExpression::Unary { expression, .. }
        | PhysicalExpression::TypeTest { expression, .. }
        | PhysicalExpression::Cast { expression, .. }
        | PhysicalExpression::Field { expression, .. } => row_reads(expression, read),
        PhysicalExpression::Record(fields) => fields.iter().any(|(_, value)| row_reads(value, read)),
        // The values read the row; the body reads it under the names they bind.
        PhysicalExpression::Let { bindings, body } => {
            bindings.iter().any(|(_, value)| row_reads(value, read))
                || row_reads(body, &|name| !bindings.iter().any(|(bound, _)| bound == name) && read(name))
        }
        PhysicalExpression::Function { arguments, .. }
        | PhysicalExpression::List(arguments)
        | PhysicalExpression::PathConstructor(arguments) => arguments.iter().any(|argument| row_reads(argument, read)),
        PhysicalExpression::Case { .. } => case_parts(expression).into_iter().any(|part| row_reads(part, read)),
        PhysicalExpression::Binary { left, right, .. } => row_reads(left, read) || row_reads(right, read),
        // The candidate list is constant, so only the tested expression can
        // make an `IN` row-dependent.
        PhysicalExpression::InList { expression, .. } => row_reads(expression, read),
        // A horizontal aggregate reads its group's list.
        PhysicalExpression::Aggregate { group, .. } => group.as_ref().is_some_and(read),
        PhysicalExpression::Value(_) => false,
    }
}

/// Scalar reads use the projected sort scope when present (§14.10).
/// Aggregate operands always read the original group, preserving multiplicity.
fn evaluate_group_expression_in_scope(
    expression: &PhysicalExpression,
    rows: &[&BindingRow],
    eliminated: &Cell<bool>,
    scope: Option<&BindingRow>,
) -> EngineResult<QueryValue> {
    match expression {
        PhysicalExpression::Aggregate {
            function,
            distinct,
            expression,
            fraction,
            group: None,
        } => evaluate_aggregate(*function, *distinct, expression.as_deref(), fraction.as_deref(), rows, eliminated),
        PhysicalExpression::Unary { operator, expression } => {
            let value = evaluate_group_expression_in_scope(expression, rows, eliminated, scope)?;
            evaluate_unary(*operator, query_value_to_evaluated(value)?).map(to_query_value)
        }
        PhysicalExpression::TypeTest {
            expression,
            value_type,
            negated,
        } => {
            let value = query_value_to_evaluated(evaluate_group_expression_in_scope(expression, rows, eliminated, scope)?)?;
            Ok(to_query_value(EvaluatedValue::Truth(Some(
                types::is_of_type(&value, value_type) != *negated,
            ))))
        }
        PhysicalExpression::Cast {
            expression,
            value_type,
            request,
        } => {
            let value = query_value_to_evaluated(evaluate_group_expression_in_scope(expression, rows, eliminated, scope)?)?;
            let request = request
                .as_deref()
                .map(|request| evaluate_expression(request, &BindingRow::default()))
                .transpose()?;
            types::cast(value, value_type, request.as_ref()).map(to_query_value)
        }
        PhysicalExpression::Record(fields) => Ok(QueryValue::Record(
            fields
                .iter()
                .map(|(name, value)| {
                    Ok((
                        Box::from(name.as_str()),
                        evaluate_group_expression_in_scope(value, rows, eliminated, scope)?,
                    ))
                })
                .collect::<EngineResult<Vec<_>>>()?,
        )),
        PhysicalExpression::Field { expression, field } => {
            let value = evaluate_group_expression_in_scope(expression, rows, eliminated, scope)?;
            // Elements can come from any group row or its fixed record.
            let mut records = BindingRow::default();
            retain_vertex_records(rows.iter().copied().chain(scope), &value, &mut records.vertex_records);
            if let Some(found) = retain_relationship_records(rows.iter().copied().chain(scope), &value) {
                records.relationship_lists.insert(Symbol::from("__hydradb_field_edges"), found);
            }
            record_field(query_value_to_evaluated(value)?, field, &records).map(to_query_value)
        }
        PhysicalExpression::Binary { left, operator, right } => {
            let left = evaluate_group_expression_in_scope(left, rows, eliminated, scope)?;
            let right = evaluate_group_expression_in_scope(right, rows, eliminated, scope)?;
            evaluate_binary(*operator, query_value_to_evaluated(left)?, query_value_to_evaluated(right)?).map(to_query_value)
        }
        PhysicalExpression::InList { expression, values } => {
            let value = query_value_to_evaluated(evaluate_group_expression_in_scope(expression, rows, eliminated, scope)?)?;
            Ok(to_query_value(evaluate_in_list(value, values)))
        }
        // Each argument is evaluated over the group the way it would be on its
        // own -- an aggregate over the group, a grouping key from its first
        // row, a constant without one -- and the call applies to the results.
        // So `coalesce(max(x), 0)` reads the group, and a constant call still
        // answers when the group is empty.
        PhysicalExpression::Function {
            function: ScalarFunction::IdProperty,
            arguments,
        } => {
            let value = evaluate_group_expression_in_scope(&arguments[0], rows, eliminated, scope)?;
            let mut records = BindingRow::default();
            retain_vertex_records(rows.iter().copied().chain(scope), &value, &mut records.vertex_records);
            if let Some(found) = retain_relationship_records(rows.iter().copied().chain(scope), &value) {
                records.relationship_lists.insert(Symbol::from("__hydradb_field_edges"), found);
            }
            stored_record_field(query_value_to_evaluated(value)?, &Symbol::from("id"), &records).map(to_query_value)
        }
        PhysicalExpression::Function { function, arguments } => {
            if *function == ScalarFunction::Coalesce {
                // A collected list (including an empty one) is a non-null value.
                // Do not force group results through scalar operand conversion.
                // Count is an internal aggregate tag; a scalar call exposes
                // its numeric value just like an integer literal or property.
                for argument in arguments {
                    let selected = evaluate_group_expression_in_scope(argument, rows, eliminated, scope)?;
                    if !selected.is_null() {
                        return Ok(match selected {
                            QueryValue::Count(count) => QueryValue::Scalar(ScalarValue::Integer(i128::from(count))),
                            value => value,
                        });
                    }
                }
                return Ok(QueryValue::Null);
            }
            let arguments = arguments
                .iter()
                .map(|argument| evaluate_group_expression_in_scope(argument, rows, eliminated, scope))
                .collect::<EngineResult<Vec<_>>>()?
                .into_iter()
                .map(query_value_to_evaluated)
                .collect::<EngineResult<Vec<_>>>()?;
            evaluate_function(*function, arguments).map(to_query_value)
        }
        PhysicalExpression::List(items) => Ok(QueryValue::List(
            items
                .iter()
                .map(|item| evaluate_group_expression_in_scope(item, rows, eliminated, scope))
                .collect::<EngineResult<Vec<_>>>()?,
        )),
        // A CASE wrapping an aggregate: each part is read over the group as
        // it would be alone, and the branch is chosen from those values.
        PhysicalExpression::Case { .. } if expression_contains_aggregate(expression) => {
            let mut evaluate = |part: &PhysicalExpression| {
                evaluate_group_expression_in_scope(part, rows, eliminated, scope).and_then(query_value_to_evaluated)
            };
            evaluate_case(expression, &mut evaluate).map(to_query_value)
        }
        // `PATH[min(a)]`: each element over the group, as an argument is, and
        // the path built from the records the group's rows hold for them.
        PhysicalExpression::PathConstructor(elements) if expression_contains_aggregate(expression) => {
            let values = elements
                .iter()
                .map(|element| evaluate_group_expression_in_scope(element, rows, eliminated, scope))
                .collect::<EngineResult<Vec<_>>>()?;
            let list = QueryValue::List(values.clone());
            let mut vertices = BTreeMap::new();
            retain_vertex_records(rows.iter().copied().chain(scope), &list, &mut vertices);
            let relationships = retain_relationship_records(rows.iter().copied().chain(scope), &list).unwrap_or_default();
            let elements = values.into_iter().map(query_value_to_evaluated).collect::<EngineResult<Vec<_>>>()?;
            Ok(QueryValue::Path(Box::new(construct_path(
                &elements,
                |id| path_vertex_record(vertices.values(), id),
                |id| path_relationship_record(relationships.iter(), id),
            )?)))
        }
        PhysicalExpression::Value(_) => evaluate_expression(expression, &BindingRow::default()).map(to_query_value),
        // Reading no row, it is answered for an empty group too.
        expression if !expression_contains_aggregate(expression) && !expression_requires_row(expression) => {
            evaluate_expression(expression, &BindingRow::default()).map(to_query_value)
        }
        _ => scope
            .or_else(|| rows.first().copied())
            .ok_or_else(|| EngineError::data(GqlStatus::DATA_EXCEPTION, "grouping expression has no input row"))
            .and_then(|row| evaluate_expression(expression, row))
            .map(to_query_value),
    }
}

fn evaluate_aggregate(
    function: AggregateFunction,
    distinct: bool,
    expression: Option<&PhysicalExpression>,
    fraction: Option<&PhysicalExpression>,
    rows: &[&BindingRow],
    eliminated: &Cell<bool>,
) -> EngineResult<QueryValue> {
    let values = match expression {
        Some(expression) => rows
            .iter()
            .map(|row| evaluate_expression(expression, row).map(to_query_value))
            .collect::<EngineResult<Vec<_>>>()?,
        None => Vec::new(),
    };
    aggregate_values(function, distinct, expression.is_some(), values, rows.len(), fraction, eliminated)
}

/// An aggregate over its argument's `values`, one per input row of the
/// `row_count` (`count(*)` has no argument and counts the rows).
fn aggregate_values(
    function: AggregateFunction,
    distinct: bool,
    has_argument: bool,
    mut values: Vec<QueryValue>,
    row_count: usize,
    fraction: Option<&PhysicalExpression>,
    eliminated: &Cell<bool>,
) -> EngineResult<QueryValue> {
    // GQL §20.9 General Rule 5: an input that is null is left out, and
    // leaving it out raises the warning 01G11.
    if values.iter().any(QueryValue::is_null) {
        eliminated.set(true);
    }
    if distinct {
        // Borrow inputs while identifying first occurrences: duplicate values
        // are never cloned and the hash table grows with unique values only.
        let mut seen = std::collections::HashSet::new();
        let keep: Vec<bool> = values
            .iter()
            .map(|value| contains_nan(value) || seen.insert(AggregateDistinctKey(value)))
            .collect();
        drop(seen);
        values = values
            .into_iter()
            .zip(keep)
            .filter_map(|(value, keep)| keep.then_some(value))
            .collect();
    }
    match function {
        AggregateFunction::Count => Ok(QueryValue::Count(match has_argument {
            false => row_count as u64,
            true => values.iter().filter(|value| !matches!(value, QueryValue::Null)).count() as u64,
        })),
        AggregateFunction::Collect => Ok(QueryValue::List(
            values.into_iter().filter(|value| !matches!(value, QueryValue::Null)).collect(),
        )),
        AggregateFunction::Sum | AggregateFunction::Average => aggregate_numeric(function, &values),
        // The extreme in the order ORDER BY sorts by. Values §19.3 cannot
        // compare, such as a number and a string, take the engine's order of
        // types (US007) rather than raising 22G04: deviation N6, as N1.
        AggregateFunction::Min | AggregateFunction::Max => {
            // Of equal values the first is kept, for MAX as for MIN:
            // `max_by` would keep the last, so `max` of `[1, 1.0]` was `1.0`.
            let wanted = if function == AggregateFunction::Min {
                Ordering::Less
            } else {
                Ordering::Greater
            };
            let extreme = values
                .into_iter()
                .filter(|value| !matches!(value, QueryValue::Null))
                .reduce(|extreme, value| {
                    if compare_query_values(&value, &extreme) == wanted {
                        value
                    } else {
                        extreme
                    }
                });
            Ok(extreme.unwrap_or(QueryValue::Null))
        }
        AggregateFunction::StddevSamp | AggregateFunction::StddevPop => standard_deviation(function, &values),
        AggregateFunction::PercentileCont | AggregateFunction::PercentileDisc => {
            // Lowering admits only a fraction that reads no row, so any row
            // evaluates it.
            let fraction = match fraction {
                Some(fraction) => evaluate_expression(fraction, &BindingRow::default())?,
                None => EvaluatedValue::Null,
            };
            percentile(function, &values, &fraction)
        }
    }
}

/// The non-null inputs of a numeric aggregate as floats; `22G03` for any
/// other value (§20.9 Syntax Rules 8 e) and 9 b)).
fn numeric_inputs(function: AggregateFunction, values: &[QueryValue]) -> EngineResult<Vec<f64>> {
    let wrong = || {
        EngineError::data(
            GqlStatus::INVALID_VALUE_TYPE,
            format!("{} requires numeric values", function.name()),
        )
    };
    values
        .iter()
        .filter(|value| !matches!(value, QueryValue::Null | QueryValue::Scalar(ScalarValue::Null)))
        .map(|value| match value {
            QueryValue::Scalar(ScalarValue::Integer(value)) => Ok(*value as f64),
            QueryValue::Count(value) => Ok(*value as f64),
            QueryValue::Scalar(ScalarValue::Float(value)) => value.parse::<f64>().map_err(|_| wrong()),
            // These results are approximate (§20.9 Syntax Rule 9 j)), so an
            // exact decimal enters as its nearest binary64 value.
            QueryValue::Scalar(ScalarValue::Decimal(value)) => Ok(value.to_f64()),
            _ => Err(wrong()),
        })
        .collect()
}

fn float_result(value: f64) -> EngineResult<QueryValue> {
    if value.is_finite() {
        Ok(QueryValue::Scalar(ScalarValue::Float(value.to_string().into())))
    } else {
        Err(EngineError::data(
            GqlStatus::NUMERIC_VALUE_OUT_OF_RANGE,
            "aggregate result is outside the FLOAT range",
        ))
    }
}

/// `STDDEV_POP` and `STDDEV_SAMP` (§20.9 General Rule 7 a) v)): null over no
/// values, and `STDDEV_SAMP` null over one. Computed about the mean, which is
/// the same quantity as the rule's `SXS - SX * SX / N` without its
/// cancellation.
fn standard_deviation(function: AggregateFunction, values: &[QueryValue]) -> EngineResult<QueryValue> {
    // A deviation does not move when every input does. Exact inputs are
    // measured from the first one before any conversion can lose a spread.
    let exact: Option<Vec<crate::decimal::ExactNumber>> = values
        .iter()
        .filter(|value| !value.is_null())
        .map(|value| match value {
            QueryValue::Scalar(ScalarValue::Integer(value)) => Some((*value).into()),
            QueryValue::Count(value) => Some(i128::from(*value).into()),
            QueryValue::Scalar(ScalarValue::Decimal(value)) => Some((*value).into()),
            _ => None,
        })
        .collect();
    let inputs = match exact.as_deref() {
        Some([base, rest @ ..]) => {
            let mut centered = Vec::with_capacity(rest.len() + 1);
            centered.push(0.0);
            for value in rest {
                centered.push(decimal_result(value.difference_to_f64(*base), || {
                    "an exact deviation exceeds the internal numeric range".into()
                })?);
            }
            centered
        }
        _ => numeric_inputs(function, values)?,
    };
    let count = inputs.len();
    let divisor = if function == AggregateFunction::StddevSamp {
        count.saturating_sub(1)
    } else {
        count
    };
    if divisor == 0 {
        return Ok(QueryValue::Null);
    }
    let integers: Option<Vec<i128>> = values
        .iter()
        .filter(|value| !value.is_null())
        .map(|value| match value {
            QueryValue::Scalar(ScalarValue::Integer(value)) => Some(*value),
            QueryValue::Count(value) => Some(i128::from(*value)),
            _ => None,
        })
        .collect();
    if let Some(integers) = &integers {
        if let Some(variance) = exact_integer_variance(integers, divisor) {
            return float_result(variance.sqrt());
        }
    }
    float_result(scaled_standard_deviation(&inputs, divisor as f64))
}

/// Exact shifted variance, or None when any intermediate exceeds i128.
fn exact_integer_variance(values: &[i128], divisor: usize) -> Option<f64> {
    let base = *values.first()?;
    let n = i128::try_from(values.len()).ok()?;
    let (sum, squares) = values.iter().try_fold((0_i128, 0_i128), |(sum, squares), value| {
        let deviation = value.checked_sub(base)?;
        Some((sum.checked_add(deviation)?, squares.checked_add(deviation.checked_mul(deviation)?)?))
    })?;
    let numerator = n.checked_mul(squares)?.checked_sub(sum.checked_mul(sum)?)?;
    let denominator = n.checked_mul(i128::try_from(divisor).ok()?)?;
    (denominator > 0).then(|| numerator as f64 / denominator as f64)
}

fn scaled_standard_deviation(inputs: &[f64], divisor: f64) -> f64 {
    // Scaled by the power of two at or below the largest magnitude, so
    // neither the sum behind the mean nor a square can overflow on inputs
    // whose deviation is finite (a thousand copies of 1e308 have a deviation
    // of zero). A power of two divides exactly, and measuring from the first
    // input keeps nearby inputs' separations exact, as for integers above.
    let largest = inputs.iter().fold(0.0_f64, |largest, value| largest.max(value.abs()));
    if largest == 0.0 {
        return 0.0;
    }
    // A subnormal input's power is the smallest one, 2^-1074.
    let scale = if largest < f64::MIN_POSITIVE {
        f64::from_bits(1)
    } else {
        f64::from_bits(largest.to_bits() & 0x7ff0_0000_0000_0000)
    };
    let first = inputs[0] / scale;
    let deviations = inputs.iter().map(|value| value / scale - first);
    let mean = deviations.clone().sum::<f64>() / inputs.len() as f64;
    let squares = deviations.map(|value| (value - mean) * (value - mean)).sum::<f64>();
    (squares / divisor).sqrt() * scale
}

/// `PERCENTILE_CONT` and `PERCENTILE_DISC` (§20.9 General Rule 7 b)): the
/// value at position `1 + p * (N - 1)` of the ordered inputs. A fraction
/// outside [0, 1] names no position, a case the standard leaves undefined, so
/// it raises a data exception with no subclass (§23.1).
fn percentile(function: AggregateFunction, values: &[QueryValue], fraction: &EvaluatedValue) -> EngineResult<QueryValue> {
    let mut inputs = numeric_inputs(function, values)?;
    // The range is checked on the exact value: 1.00000000000000000001M is
    // outside it, though its nearest float is 1.
    let (fraction, in_range) = match fraction {
        value if is_null(value) => return Ok(QueryValue::Null),
        EvaluatedValue::Scalar(ScalarValue::Integer(value)) => (*value as f64, (0..=1).contains(value)),
        EvaluatedValue::Scalar(ScalarValue::Float(value)) => {
            let value = value.parse::<f64>().unwrap_or(f64::NAN);
            (value, (0.0..=1.0).contains(&value))
        }
        EvaluatedValue::Scalar(ScalarValue::Decimal(value)) => (
            value.to_f64(),
            value.cmp_integer(0) != Ordering::Less && value.cmp_integer(1) != Ordering::Greater,
        ),
        _ => {
            return Err(EngineError::data(
                GqlStatus::INVALID_VALUE_TYPE,
                format!("the fraction of {} must be a number", function.name()),
            ))
        }
    };
    if !in_range {
        return Err(EngineError::data(
            GqlStatus::DATA_EXCEPTION,
            format!("the fraction of {} must be between 0 and 1", function.name()),
        ));
    }
    if inputs.is_empty() {
        return Ok(QueryValue::Null);
    }
    inputs.sort_by(f64::total_cmp);
    // Zero-based, so position `index` is the rule's `index + 1`.
    let index = fraction * (inputs.len() - 1) as f64;
    let (floor, ceiling) = (index.floor(), index.ceil());
    // Both answer the approximate numeric type §20.9 Syntax Rule 9 j) gives
    // binary set functions (ID099), binary64 here: PERCENTILE_DISC picks an
    // input and answers it as that type.
    let result = if function == AggregateFunction::PercentileDisc {
        // IA005: a fractional position rounds to the nearest, halves up.
        inputs[index.round() as usize]
    } else if floor == ceiling {
        inputs[index as usize]
    } else {
        // From the lower input by a share of the span: equal or tiny inputs
        // are not first scaled into rounding (5e-324 times a half is 0), and
        // only a span too wide for a float takes the weighted sum instead.
        let (lower, upper, share) = (inputs[floor as usize], inputs[ceiling as usize], index - floor);
        let span = upper - lower;
        if span.is_finite() {
            lower + span * share
        } else {
            (1.0 - share) * lower + share * upper
        }
    };
    float_result(result)
}

/// Materialized non-UNION composition. UNION keeps its streaming/dedup path.
struct ConjunctionFold<K> {
    rows: Option<Vec<K>>,
    ordinal: usize,
    conjunction: QueryConjunction,
    all: bool,
}

impl<K: Eq + std::hash::Hash> ConjunctionFold<K> {
    fn new(conjunction: QueryConjunction, all: bool) -> Self {
        Self {
            rows: None,
            ordinal: 0,
            conjunction,
            all,
        }
    }

    fn satisfied(&self) -> bool {
        self.conjunction == QueryConjunction::Otherwise && self.rows.as_ref().is_some_and(|rows| !rows.is_empty())
    }

    fn push<R>(
        &mut self,
        rows: Vec<R>,
        key: impl Fn(R, usize) -> K,
        check: &mut impl FnMut(&'static str) -> EngineResult<()>,
    ) -> EngineResult<()> {
        let right = rows
            .into_iter()
            .map(|row| {
                check("cypher_composite_materialize")?;
                self.ordinal += 1;
                Ok(key(row, self.ordinal))
            })
            .collect::<EngineResult<_>>()?;
        self.rows = Some(match self.rows.take() {
            None => right,
            Some(left) => compose(left, right, self.conjunction, self.all, check)?,
        });
        Ok(())
    }

    fn into_rows<R>(self, row: impl Fn(K) -> R, check: &mut impl FnMut(&'static str) -> EngineResult<()>) -> EngineResult<Vec<R>> {
        self.rows
            .unwrap_or_default()
            .into_iter()
            .map(|key| {
                check("cypher_composite_output")?;
                Ok(row(key))
            })
            .collect()
    }
}

/// Two tables under `EXCEPT`, `INTERSECT` or `OTHERWISE` (GQL §14.2 General
/// Rule 2), rows compared by `K`. With `ALL`, a row the right holds `N` times
/// and the left `M` times appears `max(M - N, 0)` times after `EXCEPT` and
/// `min(M, N)` after `INTERSECT`; without, once or not at all. The left's
/// order is kept.
fn compose<K: Eq + std::hash::Hash>(
    left: Vec<K>,
    right: Vec<K>,
    conjunction: QueryConjunction,
    all: bool,
    check: &mut impl FnMut(&'static str) -> EngineResult<()>,
) -> EngineResult<Vec<K>> {
    if conjunction == QueryConjunction::Otherwise {
        return Ok(if left.is_empty() { right } else { left });
    }
    let mut counts: std::collections::HashMap<K, usize> = std::collections::HashMap::new();
    for row in right {
        check("cypher_composite_counts")?;
        *counts.entry(row).or_default() += 1;
    }
    let left: Vec<K> = if all {
        left
    } else {
        let mut distinct = IndexSet::with_capacity(left.len());
        for row in left {
            check("cypher_composite_distinct")?;
            distinct.insert(row);
        }
        distinct
            .into_iter()
            .map(|row| {
                check("cypher_composite_distinct_rows")?;
                Ok(row)
            })
            .collect::<EngineResult<_>>()?
    };
    let intersect = conjunction == QueryConjunction::Intersect;
    let mut result = Vec::new();
    for row in left {
        check("cypher_composite_filter")?;
        let keep = match counts.get_mut(&row) {
            Some(count) if *count > 0 => {
                if all {
                    *count -= 1;
                }
                intersect
            }
            _ => !intersect,
        };
        if keep {
            result.push(row);
        }
    }
    Ok(result)
}

/// Two values are one for DISTINCT, grouping and the set operators when they
/// are not distinct (GQL §4.4.3): numbers by value (1 is 1.0), lists and
/// records element by element, zoned times by the instant they name.
#[derive(Eq)]
struct AggregateDistinctKey<'a>(&'a QueryValue);

impl PartialEq for AggregateDistinctKey<'_> {
    fn eq(&self, other: &Self) -> bool {
        DistinctKey::of(self.0) == DistinctKey::of(other.0)
    }
}

impl std::hash::Hash for AggregateDistinctKey<'_> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        DistinctKey::of(self.0).hash(state);
    }
}

/// The canonical form [`AggregateDistinctKey`] compares and hashes: two
/// values that are not distinct have one key, so equality and hashing
/// cannot disagree.
#[derive(Eq, Hash, PartialEq)]
enum DistinctKey<'a> {
    Null,
    /// An integer, or a float of integral value: exact.
    Exact(i128),
    /// A decimal, or a float whose exact value a decimal holds, that is not
    /// an integer: its digits without trailing zeros and their scale.
    Decimal(i128, u32),
    /// Any other float, by its bits; every NaN is one key (NaN rows are kept
    /// apart by `contains_nan`, not here).
    Float(u64),
    /// A float spelling that does not parse, kept as written.
    FloatText(&'a str),
    List(Vec<DistinctKey<'a>>),
    /// Fields ordered by name, as a record holds them.
    Record(Vec<(&'a str, DistinctKey<'a>)>),
    /// A zoned time (`false`) or datetime (`true`) by its UTC instant.
    Instant(bool, i128),
    /// Everything else is distinct exactly when it differs.
    Other(&'a QueryValue),
}

impl<'a> DistinctKey<'a> {
    fn of(value: &'a QueryValue) -> Self {
        match value {
            QueryValue::Null | QueryValue::Scalar(ScalarValue::Null) => Self::Null,
            QueryValue::Count(count) => Self::Exact(i128::from(*count)),
            QueryValue::Scalar(ScalarValue::Integer(value)) => Self::Exact(*value),
            QueryValue::Scalar(ScalarValue::Float(text)) => match text.parse::<f64>() {
                // Every integral f64 inside i128 converts exactly.
                Ok(number)
                    if number.is_finite()
                        && number.fract() == 0.0
                        && (-170141183460469231731687303715884105728.0..170141183460469231731687303715884105728.0).contains(&number) =>
                {
                    Self::Exact(number as i128)
                }
                // A float equals a decimal only when their values are one.
                Ok(number) => match crate::Decimal::from_f64_exact(number) {
                    Some(decimal) => {
                        let (digits, scale) = decimal.normalized();
                        Self::Decimal(digits, scale)
                    }
                    None => Self::Float(if number.is_nan() { f64::NAN.to_bits() } else { number.to_bits() }),
                },
                Err(_) => Self::FloatText(text),
            },
            QueryValue::Scalar(ScalarValue::Decimal(decimal)) => match decimal.normalized() {
                (digits, 0) => Self::Exact(digits),
                (digits, scale) => Self::Decimal(digits, scale),
            },
            QueryValue::List(values) => Self::List(values.iter().map(Self::of).collect()),
            QueryValue::Record(fields) => Self::Record(fields.iter().map(|(name, value)| (name.as_ref(), Self::of(value))).collect()),
            QueryValue::Temporal(temporal) => match crate::temporal::zoned_instant(temporal) {
                Some((datetime, instant)) => Self::Instant(datetime, instant),
                None => Self::Other(value),
            },
            other => Self::Other(other),
        }
    }
}

// NaN is unequal to itself in Cypher. Keep each occurrence in set/group
// operators while retaining reflexive equality for their Rust hash keys.
/// Whether a value holds a NaN anywhere, which keeps it distinct from every
/// other value. Every kind of value is listed, so a new one that can hold a
/// float has to say so.
fn contains_nan(value: &QueryValue) -> bool {
    match value {
        QueryValue::Scalar(ScalarValue::Float(value)) => value.parse::<f64>().is_ok_and(f64::is_nan),
        QueryValue::Scalar(_) => false,
        QueryValue::List(values) => values.iter().any(contains_nan),
        QueryValue::Record(fields) => fields.iter().any(|(_, value)| contains_nan(value)),
        QueryValue::Null
        | QueryValue::VertexId(_)
        | QueryValue::RelationshipId(_)
        | QueryValue::Count(_)
        | QueryValue::Path(_)
        | QueryValue::Temporal(_) => false,
    }
}
// Keying uses Cypher equality without changing QueryValue's typed result equality.
fn semantic_values_equal(left: &[QueryValue], right: &[QueryValue]) -> bool {
    left.len() == right.len()
        && left
            .iter()
            .zip(right)
            .all(|(a, b)| AggregateDistinctKey(a) == AggregateDistinctKey(b))
}
fn hash_semantic_values<H: std::hash::Hasher>(values: &[QueryValue], state: &mut H) {
    std::hash::Hash::hash(&values.len(), state);
    for value in values {
        std::hash::Hash::hash(&AggregateDistinctKey(value), state);
    }
}
#[derive(Eq)]
struct SemanticValues(Vec<QueryValue>, Option<usize>);
impl SemanticValues {
    fn new(values: Vec<QueryValue>, ordinal: usize) -> Self {
        let nonce = values.iter().any(contains_nan).then_some(ordinal);
        Self(values, nonce)
    }
}
impl PartialEq for SemanticValues {
    fn eq(&self, other: &Self) -> bool {
        self.1 == other.1 && semantic_values_equal(&self.0, &other.0)
    }
}
impl std::hash::Hash for SemanticValues {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        std::hash::Hash::hash(&self.1, state);
        hash_semantic_values(&self.0, state);
    }
}
#[derive(Eq)]
struct SemanticRow(QueryRow, Option<usize>);
impl SemanticRow {
    fn new(row: QueryRow, ordinal: usize) -> Self {
        let nonce = row.values.iter().any(contains_nan).then_some(ordinal);
        Self(row, nonce)
    }
}
impl PartialEq for SemanticRow {
    fn eq(&self, other: &Self) -> bool {
        self.1 == other.1 && semantic_values_equal(&self.0.values, &other.0.values)
    }
}
impl std::hash::Hash for SemanticRow {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        std::hash::Hash::hash(&self.1, state);
        hash_semantic_values(&self.0.values, state);
    }
}
#[derive(Eq)]
struct SemanticBindingRow(BindingRow, Option<usize>);
impl SemanticBindingRow {
    fn new(row: BindingRow, ordinal: usize) -> Self {
        let nonce = row.values.values().any(contains_nan).then_some(ordinal);
        Self(row, nonce)
    }
}
impl PartialEq for SemanticBindingRow {
    fn eq(&self, other: &Self) -> bool {
        self.1 == other.1
            && self.0.vertices == other.0.vertices
            && self.0.relationships == other.0.relationships
            && self.0.nulls == other.0.nulls
            && self.0.values.len() == other.0.values.len()
            && self.0.values.iter().all(|(name, value)| {
                other
                    .0
                    .values
                    .get(name)
                    .is_some_and(|other| AggregateDistinctKey(value) == AggregateDistinctKey(other))
            })
    }
}
impl std::hash::Hash for SemanticBindingRow {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        std::hash::Hash::hash(&self.1, state);
        std::hash::Hash::hash(&self.0.vertices, state);
        std::hash::Hash::hash(&self.0.relationships, state);
        std::hash::Hash::hash(&self.0.nulls, state);
        std::hash::Hash::hash(&self.0.values.len(), state);
        for (name, value) in &self.0.values {
            std::hash::Hash::hash(name, state);
            std::hash::Hash::hash(&AggregateDistinctKey(value), state);
        }
    }
}

fn decimal_result<T>(result: Result<T, crate::decimal::DecimalOverflow>, context: impl FnOnce() -> String) -> EngineResult<T> {
    result.map_err(|_| EngineError::data(GqlStatus::NUMERIC_VALUE_OUT_OF_RANGE, context()))
}

enum NumericSum {
    Integer(i128),
    Decimal(crate::decimal::DecimalSum),
    Float(f64),
}

impl NumericSum {
    fn add(self, value: &QueryValue) -> EngineResult<Self> {
        let overflow = |message: &str| EngineError::data(GqlStatus::NUMERIC_VALUE_OUT_OF_RANGE, message);
        Ok(match (self, value) {
            (Self::Integer(sum), QueryValue::Scalar(ScalarValue::Integer(value))) => Self::Integer(
                sum.checked_add(*value)
                    .ok_or_else(|| overflow("aggregate integer sum overflowed"))?,
            ),
            (Self::Integer(integers), QueryValue::Scalar(ScalarValue::Decimal(value))) => {
                let mut sum = crate::decimal::DecimalSum::default();
                if integers != 0 {
                    decimal_result(crate::Decimal::from_integer(integers).and_then(|value| sum.add(value)), || {
                        "aggregate decimal sum overflowed".into()
                    })?;
                }
                decimal_result(sum.add(*value), || "aggregate decimal sum overflowed".into())?;
                Self::Decimal(sum)
            }
            (Self::Decimal(mut sum), QueryValue::Scalar(ScalarValue::Integer(value))) => {
                decimal_result(crate::Decimal::from_integer(*value).and_then(|value| sum.add(value)), || {
                    "aggregate decimal sum overflowed".into()
                })?;
                Self::Decimal(sum)
            }
            (Self::Decimal(mut sum), QueryValue::Scalar(ScalarValue::Decimal(value))) => {
                decimal_result(sum.add(*value), || "aggregate decimal sum overflowed".into())?;
                Self::Decimal(sum)
            }
            (Self::Float(sum), QueryValue::Scalar(ScalarValue::Integer(value))) => Self::Float(sum + *value as f64),
            (Self::Float(sum), QueryValue::Scalar(ScalarValue::Decimal(value))) => Self::Float(sum + value.to_f64()),
            (Self::Float(sum), QueryValue::Scalar(ScalarValue::Float(value))) => Self::Float(
                sum + value
                    .parse::<f64>()
                    .map_err(|_| EngineError::data(GqlStatus::INVALID_VALUE_TYPE, "aggregate contains an invalid float"))?,
            ),
            _ => {
                return Err(EngineError::data(
                    GqlStatus::INVALID_VALUE_TYPE,
                    "sum and avg require numeric values",
                ))
            }
        })
    }

    fn finish(self, function: AggregateFunction, count: u64) -> EngineResult<QueryValue> {
        let overflow = |message: &str| EngineError::data(GqlStatus::NUMERIC_VALUE_OUT_OF_RANGE, message);
        Ok(QueryValue::Scalar(match self {
            Self::Decimal(sum) => ScalarValue::Decimal(if function == AggregateFunction::Average {
                // Divide the widened total before the only rounding.
                decimal_result(sum.average(count), || "aggregate decimal average overflowed".into())?
            } else {
                decimal_result(sum.total(), || "aggregate decimal sum overflowed".into())?
            }),
            Self::Integer(sum) if function == AggregateFunction::Average => {
                ScalarValue::Float(((sum as f64) / count as f64).to_string().into())
            }
            Self::Integer(sum) => {
                if i64::try_from(sum).is_err() {
                    return Err(overflow("integer sum is outside the 64-bit INTEGER range"));
                }
                ScalarValue::Integer(sum)
            }
            Self::Float(sum) => ScalarValue::Float(
                (if function == AggregateFunction::Average {
                    sum / count as f64
                } else {
                    sum
                })
                .to_string()
                .into(),
            ),
        }))
    }
}

fn aggregate_numeric(function: AggregateFunction, values: &[QueryValue]) -> EngineResult<QueryValue> {
    // §20.9 SR9 f): a float anywhere selects approximate accumulation before
    // the first row, so row order cannot cause an intermediate exact overflow.
    let mut sum = if values
        .iter()
        .any(|value| matches!(value, QueryValue::Scalar(ScalarValue::Float(_))))
    {
        NumericSum::Float(0.0)
    } else {
        NumericSum::Integer(0)
    };
    let mut count = 0_u64;
    for value in values {
        if matches!(value, QueryValue::Null) {
            continue;
        }
        sum = sum.add(value)?;
        count += 1;
    }
    if count == 0 {
        // N3: preserve Cypher's empty SUM = 0, not the normative null.
        return Ok(if function == AggregateFunction::Sum {
            QueryValue::Scalar(ScalarValue::Integer(0))
        } else {
            QueryValue::Null
        });
    }
    sum.finish(function, count)
}

fn compare_projected_sort_keys(left: &[QueryValue], right: &[QueryValue], items: &[PhysicalSort]) -> Ordering {
    let null = |value: &QueryValue| matches!(value, QueryValue::Null | QueryValue::Scalar(ScalarValue::Null));
    for ((left, right), item) in left.iter().zip(right).zip(items) {
        let ordering = directed(compare_query_values(left, right), null(left), null(right), item);
        if ordering != Ordering::Equal {
            return ordering;
        }
    }
    Ordering::Equal
}

fn compare_query_values(left: &QueryValue, right: &QueryValue) -> Ordering {
    match (left, right) {
        (QueryValue::Null, QueryValue::Null) => Ordering::Equal,
        (QueryValue::Null, _) => Ordering::Greater,
        (_, QueryValue::Null) => Ordering::Less,
        (QueryValue::VertexId(left), QueryValue::VertexId(right))
        | (QueryValue::RelationshipId(left), QueryValue::RelationshipId(right))
        | (QueryValue::Count(left), QueryValue::Count(right)) => left.cmp(right),
        (QueryValue::Scalar(left), QueryValue::Scalar(right)) => compare_property_values(left, right),
        (QueryValue::List(left), QueryValue::List(right)) => left
            .iter()
            .zip(right)
            .map(|(left, right)| compare_query_values(left, right))
            .find(|ordering| *ordering != Ordering::Equal)
            .unwrap_or_else(|| left.len().cmp(&right.len())),
        // As `compare_evaluated_values` orders them: by their elements.
        (QueryValue::Path(left), QueryValue::Path(right)) => path_identities(left).cmp(&path_identities(right)),
        (QueryValue::Temporal(left), QueryValue::Temporal(right)) => {
            crate::temporal::compare(left, right).unwrap_or_else(|| (left.kind() as u8).cmp(&(right.kind() as u8)))
        }
        // IV002: records by their field names, then their values, in name
        // order.
        (QueryValue::Record(left), QueryValue::Record(right)) => left
            .iter()
            .map(|(name, _)| name)
            .cmp(right.iter().map(|(name, _)| name))
            .then_with(|| {
                left.iter()
                    .zip(right)
                    .map(|((_, left), (_, right))| compare_query_values(left, right))
                    .find(|ordering| *ordering != Ordering::Equal)
                    .unwrap_or(Ordering::Equal)
            }),
        _ => query_value_rank(left).cmp(&query_value_rank(right)),
    }
}

fn query_value_rank(value: &QueryValue) -> u8 {
    match value {
        QueryValue::VertexId(_) => 0,
        QueryValue::RelationshipId(_) => 1,
        QueryValue::Count(_) => 2,
        QueryValue::Scalar(_) => 3,
        QueryValue::List(_) => 4,
        QueryValue::Path(_) => 5,
        QueryValue::Record(_) => 6,
        QueryValue::Temporal(_) => 7,
        QueryValue::Null => u8::MAX,
    }
}

/// Index entries a relationship range probe may read before it declines to
/// its fallback expansion.
const MAX_RANGE_PROBE_ENTRIES: usize = 256;

/// Deliberately the same budget as the range probe. Both answer "is this
/// index narrow enough to read whole", and a type that needs a larger
/// allowance than a property range is a type the label anchor should keep.
const MAX_TYPE_PROBE_ENTRIES: usize = 256;

fn execute_binding_plan<'a>(
    read: &'a mut ExecutionRead<'_>,
    plan: &'a GraphPhysicalPlan,
    row_limit: Option<usize>,
) -> Pin<Box<dyn Future<Output = EngineResult<Vec<BindingRow>>> + Send + 'a>> {
    execute_binding_plan_seeded(read, plan, row_limit, None)
}

fn execute_binding_plan_seeded<'a>(
    read: &'a mut ExecutionRead<'_>,
    plan: &'a GraphPhysicalPlan,
    row_limit: Option<usize>,
    seed: Option<&'a BindingRow>,
) -> Pin<Box<dyn Future<Output = EngineResult<Vec<BindingRow>>> + Send + 'a>> {
    Box::pin(async move {
        let fixed = FixedRecord::new(seed);
        execute_binding_plan_in_context(read, plan, row_limit, seed, &fixed).await
    })
}

fn execute_binding_plan_in_context<'a>(
    read: &'a mut ExecutionRead<'_>,
    plan: &'a GraphPhysicalPlan,
    row_limit: Option<usize>,
    seed: Option<&'a BindingRow>,
    fixed: &'a FixedRecord,
) -> Pin<Box<dyn Future<Output = EngineResult<Vec<BindingRow>>> + Send + 'a>> {
    Box::pin(async move {
        read.operator_started(plan);
        let result = dispatch_binding_plan(&mut *read, plan, row_limit, seed, fixed).await;
        read.operator_finished(plan, result.as_ref().map_or(0, Vec::len));
        result.map_err(|error| error.in_operation(plan.operation()))
    })
}

/// `None` means a data exception was retained on the mark, not an empty table.
/// Row extension stays with the scalar caller; joins still check compatibility.
async fn correlated_mark_rows(
    read: &mut ExecutionRead<'_>,
    subquery: &GraphPhysicalPlan,
    binding: &Symbol,
    row: &mut BindingRow,
) -> EngineResult<Option<Vec<BindingRow>>> {
    let outer = read.take_conditions();
    let found = execute_binding_plan_seeded(read, subquery, Some(1), Some(row)).await;
    if let Some(conditions) = read.defer_conditions(outer) {
        row.conditions.insert(binding.clone(), conditions);
    }
    match found {
        Ok(found) => Ok(Some(found)),
        Err(error) => match error.data_exception() {
            Some(exception) => {
                row.deferred.insert(binding.clone(), exception);
                Ok(None)
            }
            None => Err(error),
        },
    }
}

/// A zero-row demand can skip table operators, but not the record prologue.
/// Descendant procedure/query scopes initialize their own frames.
fn initialize_fixed_record<'a>(
    read: &'a mut ExecutionRead<'_>,
    plan: &'a GraphPhysicalPlan,
    seed: Option<&'a BindingRow>,
    fixed: &'a FixedRecord,
) -> Pin<Box<dyn Future<Output = EngineResult<()>> + Send + 'a>> {
    Box::pin(async move {
        match plan {
            GraphPhysicalPlan::Project { context, .. } if !context.defined.is_empty() => {
                if !fixed.contains(&context.defined) {
                    execute_binding_plan_in_context(read, plan, None, seed, fixed).await?;
                }
            }
            GraphPhysicalPlan::CallProcedure {
                input,
                subquery,
                imports,
                columns,
                outputs_to_record,
                ..
            } => {
                initialize_fixed_record(read, input, seed, fixed).await?;
                if *outputs_to_record && !fixed.contains(columns) {
                    let imported = fixed.readable(imports);
                    let rows = execute_binding_plan_seeded(read, subquery, None, Some(&imported)).await?;
                    if let Some(row) = rows.first() {
                        fixed.define(row, columns);
                    }
                }
            }
            GraphPhysicalPlan::Exists { input, .. }
            | GraphPhysicalPlan::ScalarSubquery { input, .. }
            | GraphPhysicalPlan::VariableExpand { input, .. } => initialize_fixed_record(read, input, seed, fixed).await?,
            _ => {
                for child in plan.children() {
                    initialize_fixed_record(read, child, seed, fixed).await?;
                }
            }
        }
        Ok(())
    })
}

fn dispatch_binding_plan<'a>(
    read: &'a mut ExecutionRead<'_>,
    plan: &'a GraphPhysicalPlan,
    row_limit: Option<usize>,
    seed: Option<&'a BindingRow>,
    fixed: &'a FixedRecord,
) -> Pin<Box<dyn Future<Output = EngineResult<Vec<BindingRow>>> + Send + 'a>> {
    Box::pin(async move {
        if row_limit == Some(0) {
            initialize_fixed_record(read, plan, seed, fixed).await?;
            return Ok(Vec::new());
        }
        read.checkpoint("cypher_execute_operator")?;
        match plan {
            // The one row that binds nothing, or the seed row a correlated
            // input already bound: either way exactly one.
            GraphPhysicalPlan::SingleRow => Ok(vec![seed.cloned().unwrap_or_default()]),
            GraphPhysicalPlan::Unwind {
                input,
                expression,
                binding,
                requires_list,
                position,
            } => {
                // Every element of a row's list may be filtered or joined away
                // later, so the input runs unbounded; the output stops at the
                // row limit.
                // A nonempty constant list produces exactly this many rows per
                // input. Propagate only that proven bound; dependent lists can
                // be empty and must not lose later input rows.
                let input_limit = match expression {
                    PhysicalExpression::List(items)
                        if !items.is_empty() && items.iter().all(|item| matches!(item, PhysicalExpression::Value(_))) =>
                    {
                        row_limit.map(|limit| limit.div_ceil(items.len()))
                    }
                    _ => None,
                };
                let rows = execute_binding_plan_in_context(read, input, input_limit, seed, fixed).await?;
                let mut unwound = Vec::new();
                for row in rows {
                    read.checkpoint("cypher_unwind_rows")?;
                    let elements = match evaluate_expression(expression, &row)? {
                        EvaluatedValue::List(elements) => elements,
                        // Unwinding null yields nothing; any other single
                        // value is a list of one, as Neo4j treats it. FOR
                        // takes a list only.
                        value if is_null(&value) => Vec::new(),
                        _ if *requires_list => {
                            return Err(EngineError::data(GqlStatus::INVALID_VALUE_TYPE, "FOR iterates over a list"));
                        }
                        value => vec![value],
                    };
                    for (index, element) in elements.into_iter().enumerate() {
                        let mut output = row.clone();
                        // §14.8 General Rule 2 c) ii): ORDINALITY is j, OFFSET j - 1.
                        if let Some((name, first)) = position {
                            output
                                .values
                                .insert(name.clone(), QueryValue::Scalar(ScalarValue::Integer(first + index as i128)));
                        }
                        match element {
                            EvaluatedValue::Relationship(id) => {
                                let record = row_relationship_records(&row).find(|record| record.id == id).ok_or_else(|| {
                                    unsupported(QueryFailureReason::Unwind, "UNWIND relationship has no record in this row")
                                })?;
                                output.relationships.insert(binding.clone(), record.clone());
                            }
                            EvaluatedValue::Vertex(id) => {
                                let record = row_vertex_records(&row)
                                    .find(|record| record.id == id)
                                    .ok_or_else(|| unsupported(QueryFailureReason::Unwind, "UNWIND vertex has no record in this row"))?;
                                output.vertices.insert(binding.clone(), record.clone());
                            }
                            value => {
                                output.values.insert(binding.clone(), to_query_value(value));
                            }
                        }
                        unwound.push(output);
                        read.check_intermediate_rows("cypher_unwind_intermediate_rows", unwound.len())?;
                        if row_limit.is_some_and(|limit| unwound.len() >= limit) {
                            return Ok(unwound);
                        }
                    }
                }
                Ok(unwound)
            }
            GraphPhysicalPlan::Union {
                arms,
                all,
                conjunction: conjunction @ (QueryConjunction::Except | QueryConjunction::Intersect | QueryConjunction::Otherwise),
            } => {
                let mut combined = ConjunctionFold::new(*conjunction, *all);
                for arm in arms {
                    if combined.satisfied() {
                        break;
                    }
                    let rows = execute_binding_plan_in_context(read, arm, None, seed, fixed).await?;
                    read.check_intermediate_rows("cypher_composite_intermediate_rows", rows.len())?;
                    combined.push(rows, SemanticBindingRow::new, &mut |operation| Ok(read.checkpoint(operation)?))?;
                }
                let mut rows = combined.into_rows(|row| row.0, &mut |operation| Ok(read.checkpoint(operation)?))?;
                if let Some(limit) = row_limit {
                    rows.truncate(limit);
                }
                Ok(rows)
            }
            GraphPhysicalPlan::Union { arms, all, .. } => {
                let mut rows = Vec::new();
                // Overlapped when the backend forks reads, folded in arm order
                // either way; see `execute_union_binding_arms_concurrently`.
                let mut concurrent = execute_union_binding_arms_concurrently(read, arms, seed, fixed).await;
                for arm in arms {
                    // No row limit into an arm. For UNION the dedupe below can
                    // shrink the result, and even for UNION ALL the limit
                    // belongs to the combined result, not to each arm's share
                    // of it.
                    let arm_rows = match concurrent.as_mut() {
                        Some(results) => results.next().expect("one result per UNION arm")?,
                        None => execute_binding_plan_in_context(read, arm, None, seed, fixed).await?,
                    };
                    rows.extend(arm_rows);
                    read.check_intermediate_rows("cypher_union_intermediate_rows", rows.len())?;
                }
                if !*all {
                    // Insertion-ordered hash set, not a scan over what has been
                    // kept. `unique.contains` made this quadratic: at the
                    // default intermediate-row ceiling a UNION of distinct rows
                    // would compare billions of times, inside a loop that never
                    // reached a checkpoint, so neither the runtime budget nor a
                    // cancellation could end it.
                    let mut unique: IndexSet<SemanticBindingRow> = IndexSet::with_capacity(rows.len());
                    for (ordinal, row) in rows.into_iter().enumerate() {
                        read.checkpoint("cypher_union_dedup")?;
                        unique.insert(SemanticBindingRow::new(row, ordinal));
                    }
                    rows = unique.into_iter().map(|row| row.0).collect();
                }
                Ok(rows)
            }
            GraphPhysicalPlan::VertexScan { scan } => vertex_scan::execute(&mut **read, scan, row_limit, seed).await,
            GraphPhysicalPlan::OrderedRelationshipPropertyScan {
                from,
                source_labels,
                relationship,
                relationship_type,
                direction,
                to,
                target_labels,
                property,
                sort_direction,
                prefix,
                lower,
                upper,
                required,
                items,
                residual,
            } => {
                // The planner places this operator at the root of a query
                // body's window. Under a seed (a CALL's body runs seeded with
                // what it imports, nothing for `CALL ()`) the window runs alone
                // when it binds and reads none of the seed's names, the seed
                // then joined to each row; reading or binding one would
                // correlate the window with it, which it cannot run.
                if let Some(seed) = seed {
                    let seeded = |name: &Symbol| seed.binds(name);
                    let correlated = [from, relationship, to].into_iter().any(&seeded)
                        || residual.as_ref().is_some_and(|residual| row_reads(residual, &seeded))
                        || items.iter().any(|item| row_reads(&item.expression, &seeded));
                    if correlated {
                        return Err(unsupported(
                            QueryFailureReason::OrderWindow,
                            "an ordered relationship window cannot run under a correlated input",
                        ));
                    }
                }
                let bound = |bound: &Option<(BoundValue, bool)>| {
                    bound
                        .as_ref()
                        .map(|(value, inclusive)| {
                            Ok::<_, EngineError>(PropertyBound {
                                value: seek_value(value)?.clone(),
                                inclusive: *inclusive,
                            })
                        })
                        .transpose()
                };
                let request = OrderedRelationshipPropertyScanRequest {
                    relationship_type: relationship_type.to_string(),
                    property: property.to_string(),
                    ascending: *sort_direction == SortDirection::Ascending,
                    prefix: prefix.as_ref().map(|value| bound_string(value, "STARTS WITH")).transpose()?,
                    lower: bound(lower)?,
                    upper: bound(upper)?,
                    required: row_limit.map_or(*required, |limit| limit.min(*required)),
                    after: None,
                };
                if request.required == 0 {
                    return Ok(Vec::new());
                }
                ordered_relationship_window_rows(
                    &mut **read,
                    request,
                    RelationshipWindow {
                        from,
                        source_labels,
                        relationship,
                        direction: *direction,
                        to,
                        target_labels,
                    },
                    items,
                    residual.as_ref(),
                )
                .await
                .and_then(|rows| merge_seed_rows(rows, seed))
            }
            GraphPhysicalPlan::RelationshipPropertySeek {
                from,
                source_labels,
                relationship,
                relationship_type,
                property,
                value,
                direction,
                to,
                target_labels,
            } => {
                let value = seek_value(value)?;
                let relationships = read
                    .seek_relationships_by_property(relationship_type.as_str(), property.as_str(), value)
                    .await?;
                relationship_seek_rows(
                    &mut **read,
                    relationships,
                    from,
                    source_labels,
                    relationship,
                    relationship_type,
                    *direction,
                    to,
                    target_labels,
                    row_limit,
                    seed,
                    &[],
                    SeekOrder::Identity,
                )
                .await
            }
            GraphPhysicalPlan::RelationshipPropertyMultiSeek {
                from,
                source_labels,
                relationship,
                relationship_type,
                property,
                values,
                direction,
                to,
                target_labels,
            } => {
                // Two parameters bound to one value seek it once.
                let unique_values = values
                    .iter()
                    .map(seek_value)
                    .collect::<EngineResult<BTreeSet<_>>>()?
                    .into_iter()
                    .cloned()
                    .collect::<Vec<_>>();
                // A relationship matched by two values -- `1` and `1.0`, or a
                // backend that reports it per value -- comes back twice;
                // `relationship_seek_rows` keys by relationship identity, so
                // it is one row.
                let relationships = read
                    .seek_relationships_by_property_values(relationship_type.as_str(), property.as_str(), &unique_values)
                    .await?;
                relationship_seek_rows(
                    &mut **read,
                    relationships,
                    from,
                    source_labels,
                    relationship,
                    relationship_type,
                    *direction,
                    to,
                    target_labels,
                    row_limit,
                    seed,
                    &[],
                    SeekOrder::Identity,
                )
                .await
            }
            GraphPhysicalPlan::RelationshipPropertyRangeProbe {
                from,
                source_labels,
                relationship,
                relationship_type,
                property,
                lower,
                inclusive,
                direction,
                to,
                target_labels,
                fallback,
            } => {
                let lower = seek_value(lower)?;
                match read
                    .probe_relationships_by_property_lower_bound(
                        relationship_type.as_str(),
                        property.as_str(),
                        lower,
                        *inclusive,
                        MAX_RANGE_PROBE_ENTRIES,
                    )
                    .await?
                {
                    Some(relationships) => {
                        relationship_seek_rows(
                            &mut **read,
                            relationships,
                            from,
                            source_labels,
                            relationship,
                            relationship_type,
                            *direction,
                            to,
                            target_labels,
                            row_limit,
                            seed,
                            &[],
                            SeekOrder::Expansion,
                        )
                        .await
                    }
                    None => execute_binding_plan_in_context(read, fallback, row_limit, seed, fixed).await,
                }
            }
            GraphPhysicalPlan::RelationshipTypeProbe {
                from,
                source_labels,
                relationship,
                relationship_type,
                direction,
                to,
                target_labels,
                fallback,
            } => {
                // A seed that already binds the anchor has narrowed it to one
                // vertex, which is the case the probe exists to avoid needing.
                // Reading a whole type to serve one vertex is strictly worse
                // than expanding from it, and on a correlated input it would
                // repeat per outer row. The planner cannot see this: whether a
                // seed arrives is a property of execution, not of the plan.
                if seed.is_some_and(|row| row.vertices.contains_key(from) || row.is_null(from)) {
                    return execute_binding_plan_in_context(read, fallback, row_limit, seed, fixed).await;
                }
                match read
                    .probe_relationships_by_type(relationship_type.as_str(), MAX_TYPE_PROBE_ENTRIES)
                    .await?
                {
                    Some(relationships) => {
                        relationship_seek_rows(
                            &mut **read,
                            relationships,
                            from,
                            source_labels,
                            relationship,
                            relationship_type,
                            *direction,
                            to,
                            target_labels,
                            row_limit,
                            seed,
                            &[],
                            SeekOrder::Expansion,
                        )
                        .await
                    }
                    None => execute_binding_plan_in_context(read, fallback, row_limit, seed, fixed).await,
                }
            }
            GraphPhysicalPlan::Expand {
                input,
                from,
                relationship,
                relationship_types,
                direction,
                to,
                target_labels,
            } => {
                // An input row can expand to zero rows, so limiting the input
                // would be semantically incorrect. Bound the expansion output
                // instead.
                let rows = execute_binding_plan_in_context(read, input, None, seed, fixed).await?;
                expand_rows(
                    &mut **read,
                    rows,
                    ExpandSpec {
                        from,
                        relationship: relationship.as_ref(),
                        relationship_types,
                        direction: *direction,
                        to,
                        target_labels,
                    },
                    row_limit,
                    &[],
                )
                .await
            }
            GraphPhysicalPlan::VariableExpand {
                input,
                from,
                relationship,
                relationship_types,
                direction,
                to,
                target_labels,
                min_hops,
                max_hops,
                trail,
                nodes,
                iteration,
            } => {
                let rows = execute_binding_plan_in_context(read, input, None, seed, fixed).await?;
                variable_expand_rows(
                    read,
                    rows,
                    from,
                    relationship.as_ref(),
                    relationship_types,
                    *direction,
                    to,
                    target_labels,
                    *min_hops,
                    *max_hops,
                    *trail,
                    *nodes,
                    iteration.as_ref(),
                    row_limit,
                    &[],
                )
                .await
            }
            // GQL §19.4: each input row once, marked with whether the
            // subquery, seeded with that row, yields any row. One row is all
            // the subquery is asked for.
            GraphPhysicalPlan::Exists { input, subquery, binding } => {
                let rows = execute_binding_plan_in_context(read, input, row_limit, seed, fixed).await?;
                let mut marked = Vec::with_capacity(rows.len());
                for mut row in rows {
                    read.checkpoint("cypher_exists_mark")?;
                    if let Some(found) = correlated_mark_rows(read, subquery, binding, &mut row).await? {
                        row.values
                            .insert(binding.clone(), QueryValue::Scalar(ScalarValue::Boolean(!found.is_empty())));
                    }
                    marked.push(row);
                }
                Ok(marked)
            }
            // §20.6 General Rule 2: the one field of the one row the subquery
            // yields for this row, or null when it yields none. Its Syntax
            // Rules have held it to one row, so one is all that is asked for.
            GraphPhysicalPlan::ScalarSubquery {
                input,
                subquery,
                column,
                binding,
            } => {
                let rows = execute_binding_plan_in_context(read, input, row_limit, seed, fixed).await?;
                let mut valued = Vec::with_capacity(rows.len());
                for mut row in rows {
                    read.checkpoint("cypher_scalar_subquery")?;
                    let Some(found) = correlated_mark_rows(read, subquery, binding, &mut row).await? else {
                        valued.push(row);
                        continue;
                    };
                    match found.first() {
                        Some(found) => {
                            let mut carried = BindingRow::default();
                            carry_binding(found, column, binding.clone(), &mut carried)?;
                            row.vertices.extend(carried.vertices);
                            row.relationships.extend(carried.relationships);
                            row.values.extend(carried.values);
                            row.relationship_lists.extend(carried.relationship_lists);
                            row.vertex_records.extend(carried.vertex_records);
                            if carried.nulls.contains(binding) {
                                row.values.insert(binding.clone(), QueryValue::Null);
                            }
                        }
                        None => {
                            row.values.insert(binding.clone(), QueryValue::Null);
                        }
                    }
                    valued.push(row);
                }
                Ok(valued)
            }
            // §15.1 General Rule 3: each row beside every row the procedure
            // yields for it, or, for an OPTIONAL call that yields none, beside
            // nulls.
            GraphPhysicalPlan::CallProcedure {
                input,
                subquery,
                imports,
                columns,
                outputs_to_record,
                optional,
            } => {
                let rows = execute_binding_plan_in_context(read, input, None, seed, fixed).await?;
                let mut called = Vec::new();
                for row in rows {
                    read.checkpoint("cypher_call_procedure")?;
                    let imported = row.restricted_to(imports);
                    // Under a window only the rows still wanted are asked for.
                    let wanted = row_limit.map(|limit| limit.saturating_sub(called.len()));
                    let results = execute_binding_plan_seeded(read, subquery, wanted, Some(&imported)).await?;
                    if results.is_empty() && *optional {
                        // §15.2: a miss binds every column to null, as an
                        // OPTIONAL MATCH's names are, so a later pattern
                        // cannot bind one to an unrelated element.
                        called.push(row.null_extended_call(columns));
                    }
                    for found in &results {
                        if *outputs_to_record {
                            fixed.define(found, columns);
                        }
                        called.push(row.joined_call_result(found, columns)?);
                    }
                    read.check_intermediate_rows("cypher_call_procedure_rows", called.len())?;
                    if let Some(limit) = row_limit {
                        if called.len() >= limit {
                            called.truncate(limit);
                            return Ok(called);
                        }
                    }
                }
                Ok(called)
            }
            GraphPhysicalPlan::Finish { .. } | GraphPhysicalPlan::Next { .. } => {
                Err(shape_error(RESULT_PLAN_ROOT, "statement results occur only at the query root"))
            }
            // §16.4, §22.6: the path value each row's bindings make. The
            // inner nodes of a variable-length hop are known by identity
            // only, so they are hydrated, once for all the rows.
            GraphPhysicalPlan::BindPath {
                input,
                names,
                parts,
                node_groups,
            } => {
                let mut rows = execute_binding_plan_in_context(read, input, row_limit, seed, fixed).await?;
                let mut known: BTreeMap<VertexId, VertexRecord> = rows
                    .iter()
                    .flat_map(|row| row.vertices.values())
                    .map(|vertex| (vertex.id, vertex.clone()))
                    .collect();
                let walks: Vec<Option<(Vec<VertexId>, Vec<RelationshipRecord>)>> = rows
                    .iter()
                    .map(|row| walk_path(row, parts).map(|(nodes, edges)| (nodes, edges.into_iter().cloned().collect())))
                    .collect();
                let missing: Vec<VertexId> = walks
                    .iter()
                    .flatten()
                    .flat_map(|(nodes, _)| nodes.iter().copied())
                    .filter(|id| !known.contains_key(id))
                    .collect::<BTreeSet<_>>()
                    .into_iter()
                    .collect();
                if !missing.is_empty() {
                    for vertex in read.hydrate_vertices(&missing).await? {
                        known.insert(vertex.id, vertex);
                    }
                }
                for (row, walk) in rows.iter_mut().zip(walks) {
                    read.checkpoint("cypher_bind_path")?;
                    let value = match walk {
                        None => QueryValue::Null,
                        Some((nodes, relationships)) => QueryValue::Path(Box::new(PathValue {
                            nodes: nodes
                                .iter()
                                .map(|id| {
                                    known
                                        .get(id)
                                        .cloned()
                                        .ok_or_else(|| unsupported(QueryFailureReason::Pattern, "a path node has no record"))
                                })
                                .collect::<EngineResult<Vec<_>>>()?,
                            relationships,
                        })),
                    };
                    // A group variable holds its nodes by identity; the row
                    // keeps their records, as a projection does a value's.
                    for (name, ends) in node_groups {
                        let nodes = match &value {
                            QueryValue::Path(path) => {
                                let count = path.nodes.len().saturating_sub(1);
                                let group = if *ends { path.nodes.get(1..) } else { path.nodes.get(..count) }.unwrap_or_default();
                                for node in group {
                                    row.vertex_records.entry(node.id).or_insert_with(|| node.clone());
                                }
                                QueryValue::List(group.iter().map(|node| QueryValue::VertexId(node.id)).collect())
                            }
                            _ => QueryValue::Null,
                        };
                        row.values.insert(name.clone(), nodes);
                    }
                    for name in names {
                        row.values.insert(name.clone(), value.clone());
                    }
                }
                Ok(rows)
            }
            // §22.4 General Rules 7, 12 and 13.
            GraphPhysicalPlan::SelectPaths {
                input,
                first,
                last,
                fixed_edges,
                lists,
                kind,
                count,
            } => {
                let count = match evaluate_expression(count, &BindingRow::default())? {
                    EvaluatedValue::Scalar(ScalarValue::Integer(count)) if count > 0 => usize::try_from(count).unwrap_or(usize::MAX),
                    _ => {
                        return Err(EngineError::data(
                            GqlStatus::INVALID_NUMBER_OF_PATHS_OR_GROUPS,
                            "a path search counts a positive integer",
                        ))
                    }
                };
                let rows = execute_binding_plan_in_context(read, input, None, seed, fixed).await?;
                let mut partitions: IndexMap<(VertexId, VertexId), Vec<(usize, BindingRow)>> = IndexMap::new();
                for row in rows {
                    read.checkpoint("cypher_select_paths")?;
                    let (Some(start), Some(end)) = (row.vertices.get(first), row.vertices.get(last)) else {
                        continue;
                    };
                    let length = fixed_edges
                        + lists
                            .iter()
                            .map(|list| row.relationship_lists.get(list).map_or(0, |records| records.len()))
                            .sum::<usize>();
                    partitions.entry((start.id, end.id)).or_default().push((length, row));
                }
                let mut selected = Vec::new();
                for (_, mut partition) in partitions {
                    // Ties keep the order the paths were found in (US005, UA005).
                    match kind {
                        PathSearchKind::Any => partition.truncate(count),
                        PathSearchKind::Shortest => {
                            partition.sort_by_key(|(length, _)| *length);
                            partition.truncate(count);
                        }
                        PathSearchKind::ShortestGroups => {
                            let lengths = partition.iter().map(|(length, _)| *length).collect::<BTreeSet<_>>();
                            let longest_length = lengths.last().copied();
                            if let Some(longest) = lengths.into_iter().nth(count.saturating_sub(1)).or(longest_length) {
                                partition.retain(|(length, _)| *length <= longest);
                            }
                            partition.sort_by_key(|(length, _)| *length);
                        }
                    }
                    selected.extend(partition.into_iter().map(|(_, row)| row));
                }
                if let Some(limit) = row_limit {
                    selected.truncate(limit);
                }
                Ok(selected)
            }
            // §16.7 G030 to G033: each operand's matches, the names only
            // another operand declares null on them; a union keeps a reduced
            // match two operands both find once (§16.4 General Rule 10).
            GraphPhysicalPlan::PathUnion { inputs, words } => {
                let names = inputs.iter().flat_map(crate::capability::produced_symbols).collect::<BTreeSet<_>>();
                let mut seen = std::collections::HashSet::new();
                let mut rows = Vec::new();
                for (index, input) in inputs.iter().enumerate() {
                    // A multiset alternation drops nothing, so under a window
                    // an operand need produce no more than the rows wanted.
                    let wanted = match words {
                        None => row_limit.map(|limit| limit.saturating_sub(rows.len())),
                        Some(_) => None,
                    };
                    for mut row in execute_binding_plan_in_context(read, input, wanted, seed, fixed).await? {
                        read.checkpoint("cypher_path_union")?;
                        if let Some(word) = words.as_ref().and_then(|words| words.get(index)) {
                            if !seen.insert(reduced_word(&row, word)) {
                                continue;
                            }
                        }
                        row.mark_null(&names);
                        rows.push(row);
                        if row_limit.is_some_and(|limit| rows.len() >= limit) {
                            return Ok(rows);
                        }
                    }
                    read.check_intermediate_rows("cypher_path_union_rows", rows.len())?;
                }
                Ok(rows)
            }
            GraphPhysicalPlan::NaturalJoin { left, right, optional } => {
                let left_rows = execute_binding_plan_in_context(read, left, None, seed, fixed).await?;
                if !optional && seed.is_none() && row_limit.is_none() && connected_match::accepts(left, right, &left_rows) {
                    let rows = connected_match::execute(read, right, left_rows).await?;
                    read.check_intermediate_rows("cypher_join_intermediate_rows", rows.len())?;
                    return Ok(rows);
                }
                let mut joined = Vec::new();
                // Only an optional join needs these, and only once.
                let mut right_bindings: Option<BTreeSet<Symbol>> = None;
                for left_row in left_rows {
                    read.checkpoint("cypher_natural_join_left")?;
                    // Under a window the right side need produce no more than
                    // the rows still missing: its rows are appended whole, in
                    // order, so a prefix of them is a prefix of the join. An
                    // optional side still tells empty from not, since at
                    // least one row is always asked for.
                    let right_limit = row_limit.map(|limit| limit.saturating_sub(joined.len()));
                    let right_rows = execute_binding_plan_seeded(read, right, right_limit, Some(&left_row)).await?;
                    if *optional && right_rows.is_empty() {
                        // Null-pad rather than pass through unchanged: the
                        // names this side would have bound are null from here
                        // on, and a later required clause has to be able to
                        // tell that from a name nothing has produced yet.
                        let mut missed = left_row;
                        missed.mark_null(right_bindings.get_or_insert_with(|| right.bindings()));
                        joined.push(missed);
                    } else {
                        joined.extend(right_rows);
                    }
                    read.check_intermediate_rows("cypher_join_intermediate_rows", joined.len())?;
                    if let Some(limit) = row_limit {
                        if joined.len() >= limit {
                            joined.truncate(limit);
                            return Ok(joined);
                        }
                    }
                }
                Ok(joined)
            }
            GraphPhysicalPlan::Filter {
                operation,
                input,
                predicate,
            } => {
                // A false predicate skips the table, not its fixed record.
                if matches!(
                    predicate,
                    PhysicalExpression::Value(BoundValue {
                        value: ScalarValue::Boolean(false),
                        ..
                    })
                ) {
                    initialize_fixed_record(read, input, seed, fixed).await?;
                    return Ok(Vec::new());
                }
                // A window over a Filter over an expansion: run the hop with
                // the window and the predicate, so it stops reading
                // relationships once enough rows survive instead of
                // expanding every input whole for the Filter to discard most
                // of. Stacked Filters fold in, innermost first.
                if let Some(limit) = row_limit {
                    let mut folded = Vec::new();
                    let mut residuals = vec![ResidualPredicate {
                        expression: predicate,
                        operation: *operation,
                    }];
                    let mut below: &GraphPhysicalPlan = input;
                    while let GraphPhysicalPlan::Filter {
                        operation,
                        input,
                        predicate,
                    } = below
                    {
                        folded.push(below);
                        residuals.push(ResidualPredicate {
                            expression: predicate,
                            operation: *operation,
                        });
                        below = input;
                    }
                    if is_windowed_hop(below) {
                        residuals.reverse();
                        // This invocation is the outermost Filter, already
                        // bracketed; the inner ones folded in still report to
                        // the profiler as operators that ran.
                        for operator in &folded {
                            read.operator_started(operator);
                        }
                        let rows = windowed_hop_rows(read, below, limit, seed, fixed, &residuals).await;
                        let rows_out = rows.as_ref().map_or(0, Vec::len);
                        for operator in folded.iter().rev() {
                            read.operator_finished(operator, rows_out);
                        }
                        return rows;
                    }
                }
                if let GraphPhysicalPlan::VertexScan { scan } = input.as_ref() {
                    read.operator_started(input);
                    let rows = vertex_scan::execute_for_filter(&mut **read, scan, predicate, row_limit, seed).await;
                    read.operator_finished(input, rows.as_ref().map_or(0, vertex_scan::FilterInput::len));
                    return rows?.filter(&mut **read, predicate, row_limit);
                }
                // A residual filter must read its child without a row limit.
                let rows = execute_binding_plan_in_context(read, input, None, seed, fixed).await?;
                filter_rows(&mut **read, rows, predicate, row_limit)
            }
            GraphPhysicalPlan::Sort { input, items, .. } => {
                // Sorting needs the complete input to identify the first row.
                let rows = execute_binding_plan_in_context(read, input, None, seed, fixed).await?;
                // The input is the largest collection this operator holds, and
                // it holds it whether or not a window narrows what it returns:
                // the child hands over a materialized `Vec`. Reported here, at
                // that peak, so the counter states the retention that actually
                // happens rather than the window it returns -- bounding this
                // needs a child that streams, not a narrower parent.
                read.record_retained_rows(rows.len());
                let rows = match row_limit {
                    // Under a window only the leading `SKIP + LIMIT` rows can
                    // reach the parent, so return no more than that, ranked by
                    // a heap that holds at most that many.
                    Some(limit) => top_k_rows(rows, items, limit)?,
                    None => sort_rows(rows, items)?,
                };
                read.check_intermediate_rows("cypher_sort_intermediate_rows", rows.len())?;
                Ok(rows)
            }
            GraphPhysicalPlan::Skip { input, count } => {
                let count = window_count(count, "SKIP")?;
                let child_limit = row_limit
                    .map(|limit| {
                        count.checked_add(limit).ok_or_else(|| {
                            unsupported(QueryFailureReason::OrderWindow, "SKIP plus LIMIT exceeds the supported usize range")
                        })
                    })
                    .transpose()?;
                let rows = execute_binding_plan_in_context(read, input, child_limit, seed, fixed).await?;
                let mut rows = rows.into_iter().skip(count).collect::<Vec<_>>();
                truncate_to_limit(&mut rows, row_limit);
                read.check_intermediate_rows("cypher_skip_intermediate_rows", rows.len())?;
                Ok(rows)
            }
            GraphPhysicalPlan::Limit { input, count } => {
                let count = window_count(count, "LIMIT")?;
                let effective_limit = Some(row_limit.map_or(count, |outer| outer.min(count)));
                let mut rows = execute_binding_plan_in_context(read, input, effective_limit, seed, fixed).await?;
                truncate_to_limit(&mut rows, effective_limit);
                read.check_intermediate_rows("cypher_limit_intermediate_rows", rows.len())?;
                Ok(rows)
            }
            GraphPhysicalPlan::Project {
                context,
                operation: _,
                input,
                items,
                distinct,
                post_sort,
                post_skip,
                post_limit,
            } => {
                // A projection below a result plan is a scope boundary: it
                // replaces the bindings in flight rather than adding to them,
                // which is what makes `WITH` a boundary rather than a comment.
                //
                // Without DISTINCT, grouping or a sort it maps rows one to
                // one, so a window over it -- its own or a parent's -- needs
                // only that many input rows past its own SKIP.
                let one_to_one =
                    !*distinct && post_sort.is_empty() && !items.iter().any(|item| expression_contains_aggregate(&item.expression));
                let input_limit = if one_to_one {
                    let own_limit = post_limit.as_ref().map(|count| window_count(count, "LIMIT")).transpose()?;
                    let window = match (row_limit, own_limit) {
                        (Some(outer), Some(own)) => Some(outer.min(own)),
                        (window, None) | (None, window) => window,
                    };
                    let skip = post_skip
                        .as_ref()
                        .map(|count| window_count(count, "SKIP"))
                        .transpose()?
                        .unwrap_or(0);
                    window.and_then(|window| window.checked_add(skip))
                } else {
                    None
                };
                let input_rows = execute_binding_plan_in_context(read, input, input_limit, seed, fixed).await?;
                let record = fixed.readable(&context.fixed);
                let mut output = project_binding_rows(&mut **read, input_rows, plan, &record)?;
                if !context.fixed.is_empty() {
                    for row in &mut output {
                        *row = record.overlaid(row);
                    }
                }
                if let Some(row) = output.first() {
                    fixed.define(row, &context.defined);
                }
                Ok(output)
            }
        }
    })
}

/// Whether a window over a Filter stack may be run inside `plan` by
/// [`windowed_hop_rows`] rather than over its whole output.
fn is_windowed_hop(plan: &GraphPhysicalPlan) -> bool {
    matches!(
        plan,
        GraphPhysicalPlan::Expand { .. }
            | GraphPhysicalPlan::VariableExpand { .. }
            | GraphPhysicalPlan::RelationshipTypeProbe { .. }
            | GraphPhysicalPlan::RelationshipPropertyRangeProbe { .. }
    )
}

/// A hop under a window and the predicates of the Filters above it
/// (innermost first; see [`residuals_hold`]): the leading `limit` rows of the
/// hop that pass them all, in the hop's own order, read without finishing the
/// hop once they are found.
///
/// A plain Filter must consume its input whole because it may discard any
/// prefix. Pushing its predicate into the hop instead lets the hop count only
/// surviving rows against the window. A probe that answers applies them to
/// what it read; one that declines hands them, with the window, to its
/// fallback expansion.
fn windowed_hop_rows<'a>(
    read: &'a mut ExecutionRead<'_>,
    plan: &'a GraphPhysicalPlan,
    limit: usize,
    seed: Option<&'a BindingRow>,
    fixed: &'a FixedRecord,
    residuals: &'a [ResidualPredicate<'a>],
) -> Pin<Box<dyn Future<Output = EngineResult<Vec<BindingRow>>> + Send + 'a>> {
    Box::pin(async move {
        if !is_windowed_hop(plan) {
            // A probe's fallback that is not itself a hop: run it whole, as
            // its own operator, and filter it as the Filter stack would have.
            let rows = execute_binding_plan_in_context(read, plan, None, seed, fixed).await?;
            let mut kept = Vec::new();
            for row in rows {
                read.checkpoint("cypher_filter_rows")?;
                if residuals_hold(&row, residuals)? {
                    kept.push(row);
                    if kept.len() >= limit {
                        break;
                    }
                }
            }
            return Ok(kept);
        }
        read.operator_started(plan);
        let rows = windowed_hop_dispatch(read, plan, limit, seed, fixed, residuals).await;
        read.operator_finished(plan, rows.as_ref().map_or(0, Vec::len));
        rows
    })
}

async fn windowed_hop_dispatch<'a>(
    read: &'a mut ExecutionRead<'_>,
    plan: &'a GraphPhysicalPlan,
    limit: usize,
    seed: Option<&'a BindingRow>,
    fixed: &'a FixedRecord,
    residuals: &'a [ResidualPredicate<'a>],
) -> EngineResult<Vec<BindingRow>> {
    read.checkpoint("cypher_execute_operator")?;
    match plan {
        GraphPhysicalPlan::Expand {
            input,
            from,
            relationship,
            relationship_types,
            direction,
            to,
            target_labels,
        } => {
            let rows = execute_binding_plan_in_context(read, input, None, seed, fixed).await?;
            let spec = ExpandSpec {
                from,
                relationship: relationship.as_ref(),
                relationship_types,
                direction: *direction,
                to,
                target_labels,
            };
            expand_rows(&mut **read, rows, spec, Some(limit), residuals).await
        }
        GraphPhysicalPlan::VariableExpand {
            input,
            from,
            relationship,
            relationship_types,
            direction,
            to,
            target_labels,
            min_hops,
            max_hops,
            trail,
            nodes,
            iteration,
        } => {
            let rows = execute_binding_plan_in_context(read, input, None, seed, fixed).await?;
            variable_expand_rows(
                read,
                rows,
                from,
                relationship.as_ref(),
                relationship_types,
                *direction,
                to,
                target_labels,
                *min_hops,
                *max_hops,
                *trail,
                *nodes,
                iteration.as_ref(),
                Some(limit),
                residuals,
            )
            .await
        }
        GraphPhysicalPlan::RelationshipTypeProbe {
            from,
            source_labels,
            relationship,
            relationship_type,
            direction,
            to,
            target_labels,
            fallback,
        } => {
            // As in the probe's own arm: a seed binding the anchor goes
            // straight to the expansion.
            if seed.is_some_and(|row| row.vertices.contains_key(from) || row.is_null(from)) {
                return windowed_hop_rows(read, fallback, limit, seed, fixed, residuals).await;
            }
            match read
                .probe_relationships_by_type(relationship_type.as_str(), MAX_TYPE_PROBE_ENTRIES)
                .await?
            {
                Some(relationships) => {
                    relationship_seek_rows(
                        &mut **read,
                        relationships,
                        from,
                        source_labels,
                        relationship,
                        relationship_type,
                        *direction,
                        to,
                        target_labels,
                        Some(limit),
                        seed,
                        residuals,
                        SeekOrder::Expansion,
                    )
                    .await
                }
                None => windowed_hop_rows(read, fallback, limit, seed, fixed, residuals).await,
            }
        }
        GraphPhysicalPlan::RelationshipPropertyRangeProbe {
            from,
            source_labels,
            relationship,
            relationship_type,
            property,
            lower,
            inclusive,
            direction,
            to,
            target_labels,
            fallback,
        } => {
            let lower = seek_value(lower)?;
            match read
                .probe_relationships_by_property_lower_bound(
                    relationship_type.as_str(),
                    property.as_str(),
                    lower,
                    *inclusive,
                    MAX_RANGE_PROBE_ENTRIES,
                )
                .await?
            {
                Some(relationships) => {
                    relationship_seek_rows(
                        &mut **read,
                        relationships,
                        from,
                        source_labels,
                        relationship,
                        relationship_type,
                        *direction,
                        to,
                        target_labels,
                        Some(limit),
                        seed,
                        residuals,
                        SeekOrder::Expansion,
                    )
                    .await
                }
                None => windowed_hop_rows(read, fallback, limit, seed, fixed, residuals).await,
            }
        }
        // `windowed_hop_rows` sends anything else down its filtered path.
        other => Err(unsupported(
            QueryFailureReason::Other,
            format!("{} is not a hop a window can run inside", other.operator_name()),
        )),
    }
}

/// How [`relationship_seek_rows`] orders what it read.
#[derive(Clone, Copy)]
enum SeekOrder {
    /// By relationship identity: an index seek's own order.
    Identity,
    /// As the expansion a probe stands in for emits them: by the `from`
    /// vertex, then the `to` vertex, then relationship identity. The probe's
    /// fallback expands a vertex scan, which arrives in vertex order, so a
    /// probe that answers and one that declines return one order -- which
    /// matters wherever arrival order survives, as it does for a tie between
    /// rows binding more than one relationship (see [`row_identity`]).
    Expansion,
}

/// Rows for the relationships an index or probe read, in `order`, keeping
/// only those that pass `residuals` (see [`residuals_hold`]) and stopping at
/// `row_limit` of them.
#[allow(clippy::too_many_arguments)]
async fn relationship_seek_rows(
    read: &mut dyn GraphRead,
    relationships: Vec<RelationshipRecord>,
    from: &Symbol,
    source_labels: &[Symbol],
    relationship_binding: &Symbol,
    relationship_type: &Symbol,
    direction: PatternDirection,
    to: &Symbol,
    target_labels: &[Symbol],
    row_limit: Option<usize>,
    seed: Option<&BindingRow>,
    residuals: &[ResidualPredicate<'_>],
    order: SeekOrder,
) -> EngineResult<Vec<BindingRow>> {
    let relationships = canonical_relationships(read, relationships)?;
    let vertex_ids = relationships
        .values()
        .flat_map(|relationship| [relationship.source, relationship.target])
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let vertices = read
        .hydrate_vertices(&vertex_ids)
        .await?
        .into_iter()
        .map(|vertex| (vertex.id, vertex))
        .collect::<BTreeMap<_, _>>();
    let mut candidates = Vec::new();
    for relationship in relationships.values() {
        read.checkpoint("cypher_relationship_property_seek")?;
        if relationship.relationship_type != relationship_type.as_str() {
            continue;
        }
        // Neither endpoint is bound on this path, so an undirected hop matches
        // the edge under both assignments and yields both rows. A self-loop is
        // the exception: the two assignments are the same row.
        let orientations: &[(VertexId, VertexId)] = match direction {
            PatternDirection::Outgoing => &[(relationship.source, relationship.target)],
            PatternDirection::Incoming => &[(relationship.target, relationship.source)],
            PatternDirection::Undirected if relationship.source == relationship.target => &[(relationship.source, relationship.target)],
            PatternDirection::Undirected => &[
                (relationship.source, relationship.target),
                (relationship.target, relationship.source),
            ],
        };
        for &(source_id, target_id) in orientations {
            candidates.push((source_id, target_id, relationship));
        }
    }
    if matches!(order, SeekOrder::Expansion) {
        candidates.sort_by_key(|(source, target, relationship)| (*source, *target, relationship.id));
    }
    let mut rows = Vec::new();
    for (source_id, target_id, relationship) in candidates {
        read.checkpoint("cypher_relationship_property_seek")?;
        let (Some(source), Some(target)) = (vertices.get(&source_id), vertices.get(&target_id)) else {
            continue;
        };
        if !source_labels.iter().all(|label| source.labels.contains(label.as_str()))
            || !target_labels.iter().all(|label| target.labels.contains(label.as_str()))
        {
            continue;
        }
        let row = BindingRow {
            vertices: BTreeMap::from([(from.clone(), source.clone()), (to.clone(), target.clone())]),
            relationships: BTreeMap::from([(relationship_binding.clone(), relationship.clone())]),
            nulls: BTreeSet::new(),
            values: BTreeMap::new(),
            relationship_lists: BTreeMap::new(),
            deferred: BTreeMap::new(),
            vertex_records: BTreeMap::new(),
            conditions: BTreeMap::new(),
        };
        let Some(row) = seed.map_or(Some(row.clone()), |seed| merge_compatible_rows(seed, &row)) else {
            continue;
        };
        if !residuals_hold(&row, residuals)? {
            continue;
        }
        rows.push(row);
        read.check_intermediate_rows("cypher_relationship_property_seek_rows", rows.len())?;
        if row_limit.is_some_and(|limit| rows.len() >= limit) {
            break;
        }
    }
    Ok(rows)
}

/// The best `capacity` rows seen so far under the complete requested order,
/// kept sorted best-first. Rows arrive in index order, so before the window
/// fills a new row lands inside the trailing tie, and afterwards only a tie on
/// the boundary value can still displace the worst member: insertion is a
/// binary search plus a shift across that tie, never across the window.
///
/// `T` is the implicit final tie-break: the vertex ID for vertex walks, and
/// `(source, target, relationship ID)` for relationship walks.
struct OrderedRowWindow<'a, T = VertexId> {
    items: &'a [PhysicalSort],
    id_ascending: bool,
    capacity: usize,
    rows: Vec<(Vec<EvaluatedValue>, T, BindingRow)>,
}

impl<'a, T: Ord + Copy> OrderedRowWindow<'a, T> {
    fn new(items: &'a [PhysicalSort], id_ascending: bool, capacity: usize) -> Self {
        Self {
            items,
            id_ascending,
            capacity,
            rows: Vec::new(),
        }
    }

    fn is_full(&self) -> bool {
        self.rows.len() >= self.capacity
    }

    fn compare(&self, left: (&[EvaluatedValue], T), right: (&[EvaluatedValue], T)) -> Ordering {
        compare_sort_keys(left.0, right.0, self.items).then_with(|| {
            let ordering = left.1.cmp(&right.1);
            if self.id_ascending {
                ordering
            } else {
                ordering.reverse()
            }
        })
    }

    /// Offer one row. Returns whether it was retained.
    fn offer(&mut self, keys: Vec<EvaluatedValue>, id: T, row: BindingRow) -> bool {
        let position = self
            .rows
            .partition_point(|(existing, existing_id, _)| self.compare((existing, *existing_id), (&keys, id)) != Ordering::Greater);
        if position >= self.capacity {
            return false;
        }
        self.rows.insert(position, (keys, id, row));
        if self.rows.len() > self.capacity {
            self.rows.pop();
        }
        true
    }

    fn into_rows(self) -> Vec<BindingRow> {
        self.rows.into_iter().map(|(_, _, row)| row).collect()
    }
}

/// Rows to ask for in the next page of an ordered walk. The backend hydrates
/// every row it returns, so a page sized to the whole window hydrates rows the
/// window no longer has room for. While the window fills, ask for the open
/// slots scaled by the residual's observed selectivity, never fewer than the
/// open slots (legacy's batch size) and never more than the window. Before any
/// row has been kept there is no estimate, and a full window only needs its
/// boundary tie, which the narrowed bound already stops; both take the window.
fn continuation_page_size(capacity: usize, retained: usize, walked: usize, kept: usize) -> usize {
    let open = capacity.saturating_sub(retained);
    if open == 0 || kept == 0 {
        return capacity;
    }
    open.saturating_mul(walked).div_ceil(kept).clamp(open, capacity)
}

/// Drive a paged ordered index walk into a bounded window.
///
/// Each page arrives in index order. Every row is re-checked against labels
/// and the request's sound restrictions, then against the residual predicate,
/// and offered to the window. Once the window is full only rows tying the
/// worst member's leading value can still enter, so the walk stops at the
/// first row past that value; when the index already walks ties in the
/// requested order - vertex ID order, or a second property the backend ranked
/// each run by - it stops as soon as the window fills. Continuation pages
/// after the window fills are narrowed to the boundary value, so the backend
/// stops before hydrating anything beyond the tie.
struct RelationshipWindow<'a> {
    from: &'a Symbol,
    source_labels: &'a [Symbol],
    relationship: &'a Symbol,
    direction: PatternDirection,
    to: &'a Symbol,
    target_labels: &'a [Symbol],
}

/// Drive a paged ordered relationship property walk into a bounded window.
///
/// Rows arrive in `(value, source, target)` order. Each is re-checked against
/// the request, bound to the pattern (endpoints hydrated once per page, in one
/// batch, and label-checked), filtered by the residual and offered to the
/// window, whose final tie-break is `(source, target, relationship ID)` in the
/// walk direction. Parallel relationships may arrive in any order, and later
/// sort keys are evaluated on the row, so a full window is settled only when
/// the walk passes the boundary value: continuation pages are narrowed to it,
/// and the walk stops at the first row beyond it.
async fn ordered_relationship_window_rows(
    read: &mut dyn GraphRead,
    mut request: OrderedRelationshipPropertyScanRequest,
    pattern: RelationshipWindow<'_>,
    items: &[PhysicalSort],
    residual: Option<&PhysicalExpression>,
) -> EngineResult<Vec<BindingRow>> {
    let capacity = request.required;
    let ascending = request.ascending;
    let mut window = OrderedRowWindow::<(VertexId, VertexId, RelationshipId)>::new(items, ascending, capacity);
    let boundary = |window: &OrderedRowWindow<'_, (VertexId, VertexId, RelationshipId)>| {
        window
            .rows
            .last()
            .and_then(|(_, _, row)| row.relationships.get(pattern.relationship))
            .and_then(|relationship| relationship.properties.get(request.property.as_str()))
            .cloned()
    };
    let mut previous: Option<(ScalarValue, VertexId, VertexId)> = None;
    let mut walked = 0_usize;
    let mut kept = 0_usize;
    loop {
        let page = read.scan_relationships_by_property_ordered(&request).await?;
        // Order check and index re-check first, so only rows that can still
        // matter have their endpoints hydrated.
        let mut candidates = Vec::with_capacity(page.relationships.len());
        for relationship in page.relationships {
            read.checkpoint("cypher_ordered_relationship_scan")?;
            let Some(value) = relationship
                .properties
                .get(request.property.as_str())
                .filter(|value| !matches!(value, ScalarValue::Null))
                .cloned()
            else {
                continue;
            };
            let position = (value, relationship.source, relationship.target);
            if let Some(previous) = &previous {
                let ordering =
                    compare_property_values(&previous.0, &position.0).then_with(|| (previous.1, previous.2).cmp(&(position.1, position.2)));
                let ordering = if ascending { ordering } else { ordering.reverse() };
                if ordering.is_gt() {
                    return Err(unsupported(
                        QueryFailureReason::OrderWindow,
                        "ordered relationship property scan returned rows out of index order",
                    ));
                }
            }
            previous = Some(position.clone());
            if relationship.relationship_type != request.relationship_type || !request.accepts(&position.0) {
                continue;
            }
            candidates.push((position.0, relationship));
        }
        let endpoints = candidates
            .iter()
            .flat_map(|(_, relationship)| [relationship.source, relationship.target])
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        let vertices = if endpoints.is_empty() {
            BTreeMap::new()
        } else {
            read.hydrate_vertices(&endpoints)
                .await?
                .into_iter()
                .map(|vertex| (vertex.id, vertex))
                .collect::<BTreeMap<_, _>>()
        };
        for (value, relationship) in candidates {
            read.checkpoint("cypher_ordered_relationship_scan_row")?;
            if window.is_full() && !boundary(&window).is_some_and(|boundary| compare_property_values(&boundary, &value).is_eq()) {
                return Ok(window.into_rows());
            }
            let (from_id, to_id) = match pattern.direction {
                PatternDirection::Incoming => (relationship.target, relationship.source),
                PatternDirection::Outgoing | PatternDirection::Undirected => (relationship.source, relationship.target),
            };
            let (Some(from), Some(to)) = (vertices.get(&from_id), vertices.get(&to_id)) else {
                continue;
            };
            if !pattern.source_labels.iter().all(|label| from.labels.contains(label.as_str()))
                || !pattern.target_labels.iter().all(|label| to.labels.contains(label.as_str()))
            {
                continue;
            }
            let tie = (relationship.source, relationship.target, relationship.id);
            let row = BindingRow {
                vertices: BTreeMap::from([(pattern.from.clone(), from.clone()), (pattern.to.clone(), to.clone())]),
                relationships: BTreeMap::from([(pattern.relationship.clone(), relationship)]),
                // This walk binds vertices and a relationship, never a
                // projected value; `values` only carries what a WITH boundary
                // put there.
                values: BTreeMap::new(),
                relationship_lists: BTreeMap::new(),
                deferred: BTreeMap::new(),
                vertex_records: BTreeMap::new(),
                conditions: BTreeMap::new(),
                // The walk only emits a hop it actually matched, so every name
                // this row carries is bound; none of them is a null an
                // OPTIONAL left behind.
                nulls: BTreeSet::new(),
            };
            walked += 1;
            if !residual_row_accepts(residual, &row)? {
                continue;
            }
            kept += 1;
            let keys = items
                .iter()
                .map(|item| evaluate_expression(&item.expression, &row))
                .collect::<EngineResult<Vec<_>>>()?;
            if window.offer(keys, tie, row) {
                read.check_intermediate_rows("cypher_ordered_relationship_scan_intermediate_rows", window.rows.len())?;
            }
        }
        if page.exhausted {
            return Ok(window.into_rows());
        }
        let Some(next) = page.last else {
            return Ok(window.into_rows());
        };
        request.required = continuation_page_size(capacity, window.rows.len(), walked, kept);
        if window.is_full() {
            if let Some(boundary) = boundary(&window) {
                let bound = Some(PropertyBound {
                    value: boundary,
                    inclusive: true,
                });
                if ascending {
                    request.upper = bound;
                } else {
                    request.lower = bound;
                }
            }
        }
        request.after = Some(next);
    }
}

fn bound_seed_row(seed: Option<&BindingRow>, binding: &Symbol, labels: &[Symbol]) -> Option<Vec<BindingRow>> {
    let seed = seed?;
    if seed.is_null(binding) {
        // An OPTIONAL bound this to null, so no vertex satisfies the pattern
        // and the row is eliminated. Returning `Some(no rows)` rather than
        // `None` is the whole point: `None` would fall through to a scan and
        // bind some unrelated vertex to a name Cypher says is null.
        return Some(Vec::new());
    }
    let vertex = seed.vertices.get(binding)?;
    Some(
        labels
            .iter()
            .all(|label| vertex.labels.contains(label.as_str()))
            .then(|| seed.clone())
            .into_iter()
            .collect(),
    )
}

fn merge_seed_rows(rows: Vec<BindingRow>, seed: Option<&BindingRow>) -> EngineResult<Vec<BindingRow>> {
    let Some(seed) = seed else {
        return Ok(rows);
    };
    Ok(rows.into_iter().filter_map(|row| merge_compatible_rows(seed, &row)).collect())
}

fn truncate_to_limit<T>(rows: &mut Vec<T>, row_limit: Option<usize>) {
    if let Some(limit) = row_limit {
        rows.truncate(limit);
    }
}

/// Insert `row` into `rows`, which holds at most `limit` rows ranked by
/// `items`, with `keys` holding their sort keys index for index.
///
/// Equal keys keep arrival order and a row that ties the worst held one is
/// dropped, so the result is exactly what a stable sort of every row followed
/// by truncation to `limit` would keep -- without ever holding more than
/// `limit` rows.
fn insert_ranked(
    rows: &mut Vec<BindingRow>,
    keys: &mut Vec<Vec<EvaluatedValue>>,
    row: BindingRow,
    items: &[PhysicalSort],
    limit: usize,
) -> EngineResult<()> {
    let key = items
        .iter()
        .map(|item| evaluate_expression(&item.expression, &row))
        .collect::<EngineResult<Vec<_>>>()?;
    let at = keys.partition_point(|held| compare_sort_keys(held, &key, items) != Ordering::Greater);
    if at >= limit {
        return Ok(());
    }
    if rows.len() == limit {
        rows.pop();
        keys.pop();
    }
    rows.insert(at, row);
    keys.insert(at, key);
    Ok(())
}

fn sort_rows(mut rows: Vec<BindingRow>, items: &[PhysicalSort]) -> EngineResult<Vec<BindingRow>> {
    let keys = rows
        .iter()
        .map(|row| {
            items
                .iter()
                .map(|item| evaluate_expression(&item.expression, row))
                .collect::<EngineResult<Vec<_>>>()
        })
        .collect::<EngineResult<Vec<_>>>()?;
    let ranked = ranks_by_identity(&rows);
    let mut keyed = rows
        .drain(..)
        .zip(keys)
        .map(|(row, keys)| {
            let identity = ranked.then(|| row_identity(&row)).flatten();
            (row, keys, identity)
        })
        .collect::<Vec<_>>();
    keyed.sort_by(|(_, left, left_id), (_, right, right_id)| compare_sort_keys(left, right, items).then_with(|| left_id.cmp(right_id)));
    Ok(keyed.into_iter().map(|(row, _, _)| row).collect())
}

/// The order a tie falls back to in a sort: the relationships a row binds, by
/// `(source, target, ID)`, in binding-name order, ascending.
///
/// This repairs one artifact and nothing else. Expansion groups its output by
/// the end the pattern binds first, so `(b)<-[r]-(a)` arrives in
/// `(target, source)` order where `(a)-[r]->(b)` arrives in
/// `(source, target)` order -- the same relationships, described from the
/// other end -- and a stable sort keeps whichever order it was handed, so a
/// tie ranked differently for the two spellings. Legacy ranks such a tie by
/// the relationship's own key either way, and it stays ascending whichever way
/// the sort keys run, because that is what a stable sort over an ascending
/// expansion does.
///
/// Whether this input is the shape [`row_identity`] was reasoned about: every
/// row binding exactly one relationship.
///
/// Decided once for the whole sort, and that is what makes the comparison a
/// total order. `None` sorts before `Some`, so a mixed input -- an
/// `OPTIONAL MATCH` hop bound in some rows and not others -- would rank the
/// two-relationship rows ahead of the rest whatever their arrival order, and
/// comparing identities only when both sides have one is worse than useless:
/// `Some(a)` and `Some(b)` would order while each compared `Equal` to a `None`
/// between them, which is not transitive and which `sort_by` is entitled to
/// answer with garbage.
fn ranks_by_identity(rows: &[BindingRow]) -> bool {
    rows.iter().all(|row| row.relationships.len() == 1)
}

/// The order a tie falls back to: the one relationship the row binds, by
/// `(source, target, ID)`, ascending. Only called when
/// [`ranks_by_identity`] holds.
///
/// Rows binding no relationship, or more than one, keep arrival order. Both
/// exclusions are deliberate.
///
/// A vertex scan has no grouping artifact to repair, and a sort can sit above
/// an ordered vertex walk that has already put a tie in the index's order --
/// re-ranking there would overwrite the answer the walk was asked for.
///
/// A multi-hop row binds several relationships, and the only order available
/// here is by binding name, which is not the order the hops ran in: with
/// `(a)-[z]->(b)-[k]->(c)` the second hop would lead. Expansion order is the
/// hop order, so leaving those rows to it is what keeps them matching the
/// plan this stands in for. The artifact being repaired is one expansion
/// grouping its output by the end its pattern binds first, which is a
/// single-hop shape.
fn row_identity(row: &BindingRow) -> Option<(VertexId, VertexId, RelationshipId)> {
    let mut relationships = row.relationships.values();
    match (relationships.next(), relationships.next()) {
        (Some(relationship), None) => Some((relationship.source, relationship.target, relationship.id)),
        _ => None,
    }
}

/// The first `limit` rows of [`sort_rows`], ranked by a heap that holds at most
/// `limit` rows rather than by sorting the whole input. The input itself
/// arrives materialized and is consumed as the heap fills, so this bounds the
/// ranking structure and the result, not the operator's peak retention.
///
/// `sort_rows` ranks equal keys by [`row_identity`] and only then by arrival.
/// Ranking by `(keys, identity, input position)` reproduces that exactly:
/// a later row that ties the worst retained row ranks below it and is dropped,
/// so the boundary tie never needs extra slots and the result is identical to
/// sorting everything and truncating. A max-heap on that rank keeps the worst
/// retained row on top, so each input row costs `O(log limit)`.
fn top_k_rows(rows: Vec<BindingRow>, items: &[PhysicalSort], limit: usize) -> EngineResult<Vec<BindingRow>> {
    if limit == 0 {
        return Ok(Vec::new());
    }
    let ranked = ranks_by_identity(&rows);
    let mut retained = BinaryHeap::with_capacity(limit.min(rows.len()));
    for (position, row) in rows.into_iter().enumerate() {
        let keys = items
            .iter()
            .map(|item| evaluate_expression(&item.expression, &row))
            .collect::<EngineResult<Vec<_>>>()?;
        let candidate = RankedRow {
            keys,
            identity: ranked.then(|| row_identity(&row)).flatten(),
            position,
            row,
            items,
        };
        if retained.len() < limit {
            retained.push(candidate);
        } else if let Some(mut worst) = retained.peek_mut() {
            if candidate < *worst {
                *worst = candidate;
            }
        }
    }
    Ok(retained.into_sorted_vec().into_iter().map(|ranked| ranked.row).collect())
}

/// A row ranked by its sort keys, then by [`row_identity`], then by input
/// position (see [`top_k_rows`]).
struct RankedRow<'a> {
    keys: Vec<EvaluatedValue>,
    identity: Option<(VertexId, VertexId, RelationshipId)>,
    position: usize,
    row: BindingRow,
    items: &'a [PhysicalSort],
}

impl Ord for RankedRow<'_> {
    fn cmp(&self, other: &Self) -> Ordering {
        compare_sort_keys(&self.keys, &other.keys, self.items)
            .then_with(|| self.identity.cmp(&other.identity))
            .then_with(|| self.position.cmp(&other.position))
    }
}

impl PartialOrd for RankedRow<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for RankedRow<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other).is_eq()
    }
}

impl Eq for RankedRow<'_> {}

/// One sort key's ordering under its item: the direction, and an explicit
/// null ordering, which places a null against a value whatever the direction
/// (GQL §16.17 General Rule 1 g) ii)). Without one, nulls compare greater
/// than every value, so they come last ascending and first descending.
fn directed(ordering: Ordering, left_null: bool, right_null: bool, item: &PhysicalSort) -> Ordering {
    if let (Some(nulls), true) = (item.nulls, left_null != right_null) {
        return match (nulls, left_null) {
            (NullOrdering::First, true) | (NullOrdering::Last, false) => Ordering::Less,
            (NullOrdering::First, false) | (NullOrdering::Last, true) => Ordering::Greater,
        };
    }
    match item.direction {
        crate::SortDirection::Ascending => ordering,
        crate::SortDirection::Descending => ordering.reverse(),
    }
}

fn compare_sort_keys(left: &[EvaluatedValue], right: &[EvaluatedValue], items: &[PhysicalSort]) -> Ordering {
    let null = |value: &EvaluatedValue| matches!(value, EvaluatedValue::Null | EvaluatedValue::Truth(None));
    for ((left, right), item) in left.iter().zip(right).zip(items) {
        let ordering = directed(compare_evaluated_values(left, right), null(left), null(right), item);
        if ordering != Ordering::Equal {
            return ordering;
        }
    }
    Ordering::Equal
}

fn compare_evaluated_values(left: &EvaluatedValue, right: &EvaluatedValue) -> Ordering {
    match (left, right) {
        (EvaluatedValue::Null | EvaluatedValue::Truth(None), EvaluatedValue::Null | EvaluatedValue::Truth(None)) => Ordering::Equal,
        (EvaluatedValue::Null | EvaluatedValue::Truth(None), _) => Ordering::Greater,
        (_, EvaluatedValue::Null | EvaluatedValue::Truth(None)) => Ordering::Less,
        (EvaluatedValue::Vertex(left), EvaluatedValue::Vertex(right))
        | (EvaluatedValue::Relationship(left), EvaluatedValue::Relationship(right)) => left.cmp(right),
        (EvaluatedValue::Scalar(left), EvaluatedValue::Scalar(right)) => compare_property_values(left, right),
        (EvaluatedValue::Truth(Some(left)), EvaluatedValue::Truth(Some(right))) => left.cmp(right),
        (EvaluatedValue::List(left), EvaluatedValue::List(right)) => left
            .iter()
            .zip(right)
            .map(|(left, right)| compare_evaluated_values(left, right))
            .find(|ordering| *ordering != Ordering::Equal)
            .unwrap_or_else(|| left.len().cmp(&right.len())),
        // Paths of different types never meet; among paths, by their
        // elements' identities in order (US007).
        (EvaluatedValue::Path(left), EvaluatedValue::Path(right)) => path_identities(left).cmp(&path_identities(right)),
        // US007: temporal values by type, then as they compare.
        (EvaluatedValue::Temporal(left), EvaluatedValue::Temporal(right)) => {
            crate::temporal::compare(left, right).unwrap_or_else(|| (left.kind() as u8).cmp(&(right.kind() as u8)))
        }
        // IV002: records by their field names, then their values.
        (EvaluatedValue::Record(left), EvaluatedValue::Record(right)) => left
            .iter()
            .map(|(name, _)| name)
            .cmp(right.iter().map(|(name, _)| name))
            .then_with(|| {
                left.iter()
                    .zip(right)
                    .map(|((_, left), (_, right))| compare_evaluated_values(left, right))
                    .find(|ordering| *ordering != Ordering::Equal)
                    .unwrap_or(Ordering::Equal)
            }),
        _ => evaluated_value_rank(left).cmp(&evaluated_value_rank(right)),
    }
}

/// A path's element identities in order, nodes and edges alternating.
fn path_identities(path: &PathValue) -> Vec<u64> {
    let mut identities = Vec::with_capacity(path.nodes.len() + path.relationships.len());
    for (index, node) in path.nodes.iter().enumerate() {
        identities.push(node.id);
        if let Some(relationship) = path.relationships.get(index) {
            identities.push(relationship.id);
        }
    }
    identities
}

fn evaluated_value_rank(value: &EvaluatedValue) -> u8 {
    match value {
        EvaluatedValue::Truth(Some(_)) => 1,
        EvaluatedValue::Vertex(_) => 2,
        EvaluatedValue::Relationship(_) => 3,
        EvaluatedValue::List(_) => 0,
        EvaluatedValue::Scalar(value) => 4 + scalar_value_rank(value),
        EvaluatedValue::Path(_) => 200,
        EvaluatedValue::Record(_) => 201,
        EvaluatedValue::Temporal(_) => 202,
        EvaluatedValue::Null | EvaluatedValue::Truth(None) => u8::MAX,
    }
}

/// Keep the rows `predicate` accepts, stopping once `row_limit` are kept.
fn filter_rows(
    read: &mut dyn GraphRead,
    rows: Vec<BindingRow>,
    predicate: &PhysicalExpression,
    row_limit: Option<usize>,
) -> EngineResult<Vec<BindingRow>> {
    let mut filtered = Vec::new();
    for row in rows {
        read.checkpoint("cypher_filter_rows")?;
        if predicate_holds(
            evaluate_expression(predicate, &row)?,
            "Filter predicate did not evaluate to a boolean",
        )? {
            filtered.push(row);
            read.check_intermediate_rows("cypher_filter_intermediate_rows", filtered.len())?;
            if row_limit.is_some_and(|limit| filtered.len() >= limit) {
                break;
            }
        }
    }
    Ok(filtered)
}

/// Whether a whole binding row passes an ordered scan's residual predicate.
///
/// The vertex walk builds a one-binding row for its residual; a
/// relationship walk already holds the source, relationship and target, so it
/// passes the row it built.
fn residual_row_accepts(residual: Option<&PhysicalExpression>, row: &BindingRow) -> EngineResult<bool> {
    let Some(residual) = residual else {
        return Ok(true);
    };
    predicate_holds(
        evaluate_expression(residual, row)?,
        "ordered scan residual did not evaluate to a boolean",
    )
}

/// Expand `rows` over one hop. `residuals` are the predicates of the Filters
/// a window was pushed through (see the `Filter` arm), innermost first; each
/// row the hop builds must pass them all to count against `row_limit`.
async fn expand_rows(
    read: &mut dyn GraphRead,
    rows: Vec<BindingRow>,
    spec: ExpandSpec<'_>,
    row_limit: Option<usize>,
    residuals: &[ResidualPredicate<'_>],
) -> EngineResult<Vec<BindingRow>> {
    if row_limit == Some(0) {
        return Ok(Vec::new());
    }
    // A hop touching a name an OPTIONAL bound to null yields nothing for that
    // row: it starts nowhere if `from` is null, and no vertex or relationship
    // equals null, so a null `to` or relationship name cannot be satisfied
    // either. Dropping the row here is what eliminates it, and it also keeps
    // the row away from `binding_vertex`, which cannot tell null from a name
    // no plan produced and reports the latter as a query error.
    let rows = rows
        .into_iter()
        .filter(|row| !row.is_null(spec.from) && !row.is_null(spec.to) && !spec.relationship.is_some_and(|binding| row.is_null(binding)))
        .collect::<Vec<_>>();
    let input_vertex_ids = rows
        .iter()
        .map(|row| {
            read.checkpoint("cypher_expand_inputs")?;
            binding_vertex(row, spec.from).map(|vertex| vertex.id)
        })
        .collect::<EngineResult<BTreeSet<_>>>()?;
    if input_vertex_ids.is_empty() {
        return Ok(Vec::new());
    }
    if let Some(limit) = row_limit {
        if let Some(expanded) = expand_rows_windowed(read, &rows, &spec, limit, residuals).await? {
            return Ok(expanded);
        }
    }

    let requested_types = spec.relationship_types.iter().map(Symbol::as_str).collect::<BTreeSet<_>>();
    let mut by_input = BTreeMap::<VertexId, Vec<(VertexId, RelationshipRecord)>>::new();
    let mut relationship_rows = 0_usize;
    // An undirected hop runs as both storage orientations and merges here,
    // inside pattern matching, rather than as two plan-level union arms. Union
    // arms complete independently, so ORDER BY, DISTINCT and LIMIT would each
    // apply per arm instead of once across the merged rows.
    let mut seen_per_input = BTreeMap::<VertexId, BTreeSet<RelationshipId>>::new();
    for storage_direction in spec.direction.storage_directions().iter().copied() {
        let request = ExpandRequest {
            input_vertex_ids: input_vertex_ids.iter().copied().collect(),
            direction: storage_direction,
            relationship_types: spec.relationship_types.iter().map(ToString::to_string).collect(),
        };
        let returned = read.expand_relationships(&request).await?;
        let relationships = canonical_relationships(read, returned)?;
        for relationship in relationships.into_values() {
            read.checkpoint("cypher_expand_relationships")?;
            if !requested_types.is_empty() && !requested_types.contains(relationship.relationship_type.as_str()) {
                continue;
            }
            let (input, target) = match storage_direction {
                ExpandDirection::Outgoing => (relationship.source, relationship.target),
                ExpandDirection::Incoming => (relationship.target, relationship.source),
            };
            if !input_vertex_ids.contains(&input) {
                continue;
            }
            // A self-loop is returned by both orientations. Keying on the
            // relationship identity reports it once while still reporting both
            // edges of a reciprocal pair, which are distinct relationships.
            if !seen_per_input.entry(input).or_default().insert(relationship.id) {
                continue;
            }
            by_input.entry(input).or_default().push((target, relationship));
            relationship_rows = relationship_rows.saturating_add(1);
            read.check_intermediate_rows("cypher_expand_relationship_rows", relationship_rows)?;
        }
    }
    for relationships in by_input.values_mut() {
        relationships.sort_by_key(|(target, relationship)| (*target, relationship.id));
    }

    let target_ids = by_input
        .values()
        .flatten()
        .map(|(target, _)| *target)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let mut hydrated = read.hydrate_vertices(&target_ids).await?;
    hydrated.sort_by_key(|vertex| vertex.id);
    hydrated.dedup_by_key(|vertex| vertex.id);
    let targets = hydrated.into_iter().map(|vertex| (vertex.id, vertex)).collect::<BTreeMap<_, _>>();

    let mut expanded = Vec::new();
    for row in rows {
        read.checkpoint("cypher_expand_output_rows")?;
        let Some(input) = row.vertices.get(spec.from).map(|vertex| vertex.id) else {
            continue;
        };
        for (target_id, relationship) in by_input.get(&input).into_iter().flatten() {
            read.checkpoint("cypher_expand_output_rows")?;
            let Some(target) = targets.get(target_id) else {
                continue;
            };
            let Some(result) = expanded_row(&row, &spec, target, relationship) else {
                continue;
            };
            if !residuals_hold(&result, residuals)? {
                continue;
            }
            expanded.push(result);
            read.check_intermediate_rows("cypher_expand_intermediate_rows", expanded.len())?;
            if row_limit.is_some_and(|limit| expanded.len() >= limit) {
                return Ok(expanded);
            }
        }
    }
    Ok(expanded)
}

/// `row` extended over `relationship` to `target`, or `None` when the hop's
/// target labels or an already-bound `to` or relationship name reject it.
fn expanded_row(row: &BindingRow, spec: &ExpandSpec<'_>, target: &VertexRecord, relationship: &RelationshipRecord) -> Option<BindingRow> {
    if !spec.target_labels.iter().all(|label| target.labels.contains(label.as_str())) {
        return None;
    }
    let mut result = row.clone();
    if let Some(existing) = result.vertices.get(spec.to) {
        if existing != target {
            return None;
        }
    } else {
        result.vertices.insert(spec.to.clone(), target.clone());
    }
    if let Some(binding) = spec.relationship {
        if let Some(existing) = result.relationships.get(binding) {
            if existing != relationship {
                return None;
            }
        } else {
            result.relationships.insert(binding.clone(), relationship.clone());
        }
    }
    Some(result)
}

/// Whether `row` passes every predicate of a stack of Filters, innermost
/// first. Evaluation stops at the first rejection, exactly as the stack did:
/// an outer predicate never sees a row an inner one discarded, so one that
/// would fail on such a row still does not.
fn residuals_hold(row: &BindingRow, residuals: &[ResidualPredicate<'_>]) -> EngineResult<bool> {
    for predicate in residuals {
        let holds = evaluate_expression(predicate.expression, row)
            .and_then(|value| predicate_holds(value, "Filter predicate did not evaluate to a boolean"))
            .map_err(|error| error.in_operation(predicate.operation))?;
        if !holds {
            return Ok(false);
        }
    }
    Ok(true)
}

/// The neighbours of one input that share a target, and the storage
/// orientations and types that reach it. A target is the unit a windowed
/// expansion reads: every relationship to it is read together, because the
/// hop orders an input's rows by target and then by relationship identity.
struct NeighborGroup {
    target: VertexId,
    /// `(index into storage_directions, relationship type)`.
    via: Vec<(usize, String)>,
}

/// Fewest inputs a windowed expansion lists neighbours for at once, so a hop
/// over many inputs that each reach little does not pay a round trip per
/// input.
const WINDOWED_EXPAND_MIN_LIST_INPUTS: usize = 64;

/// [`expand_rows`] under a `LIMIT`: the same rows in the same order, reading
/// only the relationships and targets the window needs.
///
/// The eager hop reads every relationship of every input and hydrates every
/// target before emitting its first row, so `LIMIT 50` from a vertex with
/// ten thousand relationships paid for all ten thousand. Here neighbours are
/// listed first -- the cheap adjacency read -- and relationship records are
/// read target by target in output order: by input row, then by target, which
/// is the order the eager hop sorts into. Each round reads the next `batch`
/// targets, emits what it can, and stops once `limit` rows passed `residuals`.
/// `batch` starts at the window and doubles every round, so a predicate that
/// rejects most rows costs a logarithmic number of rounds, not one per row.
///
/// `None` when the backend cannot list neighbours on their own; the caller
/// then runs the eager hop.
async fn expand_rows_windowed(
    read: &mut dyn GraphRead,
    rows: &[BindingRow],
    spec: &ExpandSpec<'_>,
    limit: usize,
    residuals: &[ResidualPredicate<'_>],
) -> EngineResult<Option<Vec<BindingRow>>> {
    let storage_directions = spec.direction.storage_directions();
    let requested_types = spec.relationship_types.iter().map(Symbol::as_str).collect::<BTreeSet<_>>();
    let row_inputs = rows
        .iter()
        .map(|row| binding_vertex(row, spec.from).map(|vertex| vertex.id))
        .collect::<EngineResult<Vec<_>>>()?;
    // Listed inputs, each with its targets in ascending order.
    let mut groups = BTreeMap::<VertexId, Vec<NeighborGroup>>::new();
    // Read groups, keyed by `(input, target)`, relationships by identity.
    let mut fetched = BTreeMap::<(VertexId, VertexId), Vec<RelationshipRecord>>::new();
    // A self-loop is returned by both orientations; see the eager hop.
    let mut seen = BTreeSet::<(VertexId, RelationshipId)>::new();
    let mut targets = BTreeMap::<VertexId, Option<VertexRecord>>::new();
    let mut relationship_rows = 0_usize;
    let mut output = Vec::new();
    let (mut row_index, mut group_index) = (0_usize, 0_usize);
    let mut batch = limit.max(1);
    // A single ordered stream can resume by target id. Multiple inputs, types
    // or directions still use the merged listing below.
    let mut paged =
        rows.len() == 1 && storage_directions == [ExpandDirection::Outgoing] && spec.relationship_types.len() == 1 && limit <= 64;
    let mut after = None;
    while row_index < rows.len() {
        read.checkpoint("cypher_expand_window")?;
        if paged && !groups.contains_key(&row_inputs[row_index]) {
            let input = row_inputs[row_index];
            let edge_type = spec.relationship_types[0].as_str();
            let size = batch.clamp(1, 64);
            match read.outgoing_neighbor_page(input, edge_type, after, size).await? {
                Some(ids) => {
                    if ids.len() > size
                        || ids.windows(2).any(|pair| pair[0] >= pair[1])
                        || after.zip(ids.first()).is_some_and(|(a, first)| *first <= a)
                    {
                        return Err(EngineError::InvalidStorageResponse(
                            "outgoing neighbor page violated cursor order or size".to_string(),
                        ));
                    }
                    after = if ids.len() == size { ids.last().copied() } else { None };
                    groups.insert(
                        input,
                        ids.into_iter()
                            .map(|target| NeighborGroup {
                                target,
                                via: vec![(0, edge_type.to_owned())],
                            })
                            .collect(),
                    );
                }
                None if after.is_none() => paged = false,
                None => {
                    return Err(EngineError::InvalidStorageResponse(
                        "backend declined a continuation of an outgoing neighbor page".to_string(),
                    ))
                }
            }
        }
        // List the next inputs, in row order, when the cursor reaches one
        // that is not listed yet.
        if !groups.contains_key(&row_inputs[row_index]) {
            let chunk_size = batch.max(WINDOWED_EXPAND_MIN_LIST_INPUTS);
            let mut chunk = BTreeSet::new();
            for input in &row_inputs[row_index..] {
                if chunk.len() >= chunk_size {
                    break;
                }
                if !groups.contains_key(input) {
                    chunk.insert(*input);
                }
            }
            let mut listed = BTreeMap::<VertexId, BTreeMap<VertexId, BTreeSet<(usize, String)>>>::new();
            for (direction_index, direction) in storage_directions.iter().copied().enumerate() {
                let request = ExpandRequest {
                    input_vertex_ids: chunk.iter().copied().collect(),
                    direction,
                    relationship_types: spec.relationship_types.iter().map(ToString::to_string).collect(),
                };
                let Some(neighbors) = read.expand_neighbors(&request).await? else {
                    if groups.is_empty() && direction_index == 0 {
                        return Ok(None);
                    }
                    return Err(EngineError::InvalidStorageResponse(
                        "the backend stopped listing neighbours partway through an expansion".to_string(),
                    ));
                };
                for neighbor in neighbors {
                    read.checkpoint("cypher_expand_neighbors")?;
                    if !chunk.contains(&neighbor.input)
                        || (!requested_types.is_empty() && !requested_types.contains(neighbor.relationship_type.as_str()))
                    {
                        continue;
                    }
                    listed
                        .entry(neighbor.input)
                        .or_default()
                        .entry(neighbor.neighbor)
                        .or_default()
                        .insert((direction_index, neighbor.relationship_type));
                }
            }
            for input in chunk {
                let input_groups = listed
                    .remove(&input)
                    .unwrap_or_default()
                    .into_iter()
                    .map(|(target, via)| NeighborGroup {
                        target,
                        via: via.into_iter().collect(),
                    })
                    .collect();
                groups.insert(input, input_groups);
            }
        }

        // The unread targets among the next `batch`, in output order. The
        // walk stops at an input not listed yet; the next round lists it.
        // `(input, target)` to the target's position among the input's groups.
        let mut planned = BTreeMap::<(VertexId, VertexId), usize>::new();
        let (mut walk_row, mut walk_group, mut walked) = (row_index, group_index, 0_usize);
        while walk_row < rows.len() && walked < batch {
            let input = row_inputs[walk_row];
            let Some(input_groups) = groups.get(&input) else {
                break;
            };
            let Some(group) = input_groups.get(walk_group) else {
                walk_row += 1;
                walk_group = 0;
                continue;
            };
            if !fetched.contains_key(&(input, group.target)) {
                planned.insert((input, group.target), walk_group);
            }
            walk_group += 1;
            walked += 1;
        }

        if !planned.is_empty() {
            let mut read_now = BTreeMap::<(VertexId, VertexId), Vec<RelationshipRecord>>::new();
            for (direction_index, direction) in storage_directions.iter().copied().enumerate() {
                let neighbors = planned
                    .iter()
                    .flat_map(|(&(input, target), &position)| {
                        groups[&input][position]
                            .via
                            .iter()
                            .filter(move |(index, _)| *index == direction_index)
                            .map(move |(_, relationship_type)| ExpandNeighbor {
                                input,
                                neighbor: target,
                                relationship_type: relationship_type.clone(),
                            })
                    })
                    .collect::<Vec<_>>();
                if neighbors.is_empty() {
                    continue;
                }
                let returned = read.expand_relationships_between(direction, &neighbors).await?;
                for relationship in canonical_relationships(read, returned)?.into_values() {
                    read.checkpoint("cypher_expand_relationships")?;
                    if !requested_types.is_empty() && !requested_types.contains(relationship.relationship_type.as_str()) {
                        continue;
                    }
                    let (input, target) = match direction {
                        ExpandDirection::Outgoing => (relationship.source, relationship.target),
                        ExpandDirection::Incoming => (relationship.target, relationship.source),
                    };
                    if !planned.contains_key(&(input, target)) || !seen.insert((input, relationship.id)) {
                        continue;
                    }
                    read_now.entry((input, target)).or_default().push(relationship);
                    relationship_rows = relationship_rows.saturating_add(1);
                    read.check_intermediate_rows("cypher_expand_relationship_rows", relationship_rows)?;
                }
            }
            let mut unhydrated = BTreeSet::new();
            for key in planned.into_keys() {
                let mut relationships = read_now.remove(&key).unwrap_or_default();
                relationships.sort_by_key(|relationship| relationship.id);
                if !relationships.is_empty() && !targets.contains_key(&key.1) {
                    unhydrated.insert(key.1);
                }
                fetched.insert(key, relationships);
            }
            if !unhydrated.is_empty() {
                let unhydrated = unhydrated.into_iter().collect::<Vec<_>>();
                for vertex in read.hydrate_vertices(&unhydrated).await? {
                    targets.entry(vertex.id).or_insert(Some(vertex));
                }
                for id in unhydrated {
                    targets.entry(id).or_insert(None);
                }
            }
        }

        // Emit from the cursor until a target or an input is not read yet.
        while let Some(&input) = row_inputs.get(row_index) {
            let Some(input_groups) = groups.get(&input) else {
                break;
            };
            let Some(group) = input_groups.get(group_index) else {
                if paged && after.is_some() {
                    groups.remove(&input);
                    group_index = 0;
                    break;
                }
                row_index += 1;
                group_index = 0;
                continue;
            };
            let Some(relationships) = fetched.get(&(input, group.target)) else {
                break;
            };
            if let Some(Some(target)) = targets.get(&group.target) {
                for relationship in relationships {
                    read.checkpoint("cypher_expand_output_rows")?;
                    let Some(result) = expanded_row(&rows[row_index], spec, target, relationship) else {
                        continue;
                    };
                    if !residuals_hold(&result, residuals)? {
                        continue;
                    }
                    output.push(result);
                    read.check_intermediate_rows("cypher_expand_intermediate_rows", output.len())?;
                    if output.len() >= limit {
                        return Ok(Some(output));
                    }
                }
            }
            group_index += 1;
        }
        batch = batch.saturating_mul(2);
    }
    Ok(Some(output))
}

/// Fewest frontier states a windowed variable-length walk reads
/// relationships for at once; see [`variable_expand_rows`].
const WINDOWED_WALK_MIN_STATES: usize = 64;

/// Walk `min_hops..=max_hops` hops breadth first: every row at one depth is
/// emitted, frontier state by frontier state, before any row of the next.
///
/// Under a window the frontier is read in chunks of states, in frontier
/// order, each chunk's inputs expanded and their targets hydrated only when
/// the window is still open; the first chunk is the rows still missing (at
/// least [`WINDOWED_WALK_MIN_STATES`]) and each next one doubles. Rows, the
/// next frontier and its order are what one whole-frontier read produces,
/// because a state's candidates do not depend on which chunk it is in. A
/// window that fills inside a depth therefore never reads the rest of that
/// depth, nor any deeper one.
///
/// The unit is a whole input, not a target as in [`expand_rows_windowed`]:
/// a walk orders an input's candidates by relationship identity, storage
/// orientation first, and identity is what an adjacency listing cannot
/// supply -- it lists targets, and a structural relationship's identity is
/// assigned only when its record is read. Reading one input's records in
/// target order would reorder its paths. So a single high-degree vertex at
/// the head of the frontier is still read whole.
#[allow(clippy::too_many_arguments)]
async fn variable_expand_rows(
    read: &mut ExecutionRead<'_>,
    rows: Vec<BindingRow>,
    from: &Symbol,
    path_binding: Option<&Symbol>,
    relationship_types: &[Symbol],
    direction: PatternDirection,
    to: &Symbol,
    target_labels: &[Symbol],
    min_hops: u8,
    max_hops: Option<u8>,
    trail: bool,
    nodes: WalkNodes,
    iteration: Option<&crate::IterationPlan<GraphPhysicalPlan>>,
    row_limit: Option<usize>,
    residuals: &[ResidualPredicate<'_>],
) -> EngineResult<Vec<BindingRow>> {
    let iteration = iteration
        .filter(|iteration| iteration.source == iteration.target || !matches!(iteration.plan.as_ref(), GraphPhysicalPlan::SingleRow));
    if row_limit == Some(0) {
        return Ok(Vec::new());
    }
    // As in `expand_rows`: a walk touching a null name yields nothing.
    let mut frontier = rows
        .into_iter()
        .filter(|row| !row.is_null(from) && !row.is_null(to))
        .map(|row| {
            let source = binding_vertex(&row, from)?;
            let current = source.id;
            let iteration_source = iteration.map(|_| source.clone());
            Ok(TraversalState {
                row,
                start: current,
                current,
                iteration_source,
                history: TraversalHistory {
                    path: path_binding.map(|_| std::sync::Arc::new(Vec::new())),
                    nodes: (nodes != WalkNodes::Any).then(|| std::sync::Arc::new(BTreeSet::from([current]))),
                    ..TraversalHistory::default()
                },
            })
        })
        .collect::<EngineResult<Vec<_>>>()?;
    let mut output = Vec::new();

    if min_hops == 0 {
        for state in &frontier {
            let source = binding_vertex(&state.row, from)?.clone();
            if target_labels.iter().all(|label| source.labels.contains(label.as_str())) {
                let mut row = state.row.clone();
                if row.vertices.get(to).is_none_or(|existing| existing == &source) {
                    row.vertices.insert(to.clone(), source);
                    if bind_path(&mut row, path_binding, state.history.records()) && residuals_hold(&row, residuals)? {
                        output.push(row);
                        read.check_intermediate_rows("cypher_variable_expand_output_rows", output.len())?;
                        if row_limit.is_some_and(|limit| output.len() >= limit) {
                            return Ok(output);
                        }
                    }
                }
            }
        }
    }

    // An undirected walk asks storage for both orientations at every hop and
    // merges them inside the walk -- not two directed walks, because the two
    // orientations compound: `a-b-a` over a reciprocal pair is a path either
    // run alone would miss. Trail semantics key on relationship identity, so
    // a self-loop, which both orientations return, is one step, and each edge
    // of a reciprocal pair is its own -- what Neo4j answers.
    let storage_directions = direction.storage_directions();
    let requested_types = relationship_types.iter().map(Symbol::as_str).collect::<BTreeSet<_>>();

    let mut depth: u32 = 0;
    loop {
        depth += 1;
        if frontier.is_empty() || max_hops.is_some_and(|max_hops| depth > u32::from(max_hops)) {
            break;
        }
        let mut by_input = BTreeMap::<VertexId, Vec<(VertexId, RelationshipRecord)>>::new();
        let mut targets = BTreeMap::<VertexId, VertexRecord>::new();
        let mut candidate_rows = 0_usize;
        let mut next = Vec::new();
        // Without a window the whole frontier is one chunk: one expansion
        // per orientation and one hydration per depth, as always.
        let mut chunk_size = match row_limit {
            Some(limit) => limit.saturating_sub(output.len()).max(WINDOWED_WALK_MIN_STATES),
            None => frontier.len(),
        };
        let mut states = frontier.into_iter();
        loop {
            let chunk = states.by_ref().take(chunk_size).collect::<Vec<_>>();
            if chunk.is_empty() {
                break;
            }
            chunk_size = chunk_size.saturating_mul(2);
            let input_vertex_ids = chunk
                .iter()
                .map(|state| state.current)
                .filter(|input| !by_input.contains_key(input))
                .collect::<BTreeSet<_>>();
            if !input_vertex_ids.is_empty() {
                let mut seen_per_input = BTreeMap::<VertexId, BTreeSet<RelationshipId>>::new();
                for storage_direction in storage_directions.iter().copied() {
                    let request = ExpandRequest {
                        input_vertex_ids: input_vertex_ids.iter().copied().collect(),
                        direction: storage_direction,
                        relationship_types: relationship_types.iter().map(ToString::to_string).collect(),
                    };
                    let returned = read.expand_relationships(&request).await?;
                    let relationships = canonical_relationships(&**read, returned)?;
                    for relationship in relationships.into_values() {
                        read.checkpoint("cypher_variable_expand_relationships")?;
                        if !requested_types.is_empty() && !requested_types.contains(relationship.relationship_type.as_str()) {
                            continue;
                        }
                        let (input, target) = match storage_direction {
                            ExpandDirection::Outgoing => (relationship.source, relationship.target),
                            ExpandDirection::Incoming => (relationship.target, relationship.source),
                        };
                        if input_vertex_ids.contains(&input) && seen_per_input.entry(input).or_default().insert(relationship.id) {
                            candidate_rows += 1;
                            read.check_intermediate_rows("cypher_variable_expand_candidates", candidate_rows)?;
                            by_input.entry(input).or_default().push((target, relationship));
                        }
                    }
                }
                // An input with no candidates is read all the same.
                for input in &input_vertex_ids {
                    by_input.entry(*input).or_default();
                }
                let target_ids = input_vertex_ids
                    .iter()
                    .flat_map(|input| by_input[input].iter().map(|(target, _)| *target))
                    .filter(|target| !targets.contains_key(target))
                    .collect::<BTreeSet<_>>()
                    .into_iter()
                    .collect::<Vec<_>>();
                for vertex in read.hydrate_vertices(&target_ids).await? {
                    targets.entry(vertex.id).or_insert(vertex);
                }
            }
            for mut state in chunk {
                let candidates = by_input.get(&state.current).map(Vec::as_slice).unwrap_or_default();
                for (index, (target_id, relationship)) in candidates.iter().enumerate() {
                    read.checkpoint("cypher_variable_expand_paths")?;
                    // A trail reuses no relationship; a WALK may (§22.2).
                    if trail && state.history.used.contains(&relationship.id) {
                        continue;
                    }
                    // SIMPLE and ACYCLIC repeat no node; a SIMPLE walk may
                    // close on its start, and then ends there.
                    let closes = state.history.nodes.as_ref().is_some_and(|visited| visited.contains(target_id));
                    if closes && !(nodes == WalkNodes::Simple && *target_id == state.start) {
                        continue;
                    }
                    let Some(target) = targets.get(target_id) else {
                        continue;
                    };
                    if let Some(iteration) = iteration {
                        let source = state
                            .iteration_source
                            .as_ref()
                            .ok_or_else(|| unsupported(QueryFailureReason::Pattern, "an iteration source has no record"))?;
                        if iteration.source == iteration.target && source.id != target.id {
                            continue;
                        }
                        let mut singleton = state.row.clone();
                        singleton.vertices.insert(iteration.source.clone(), source.clone());
                        singleton.vertices.insert(iteration.target.clone(), target.clone());
                        singleton.relationships.insert(iteration.relationship.clone(), relationship.clone());
                        for name in [&iteration.source, &iteration.target, &iteration.relationship] {
                            singleton.values.remove(name);
                            singleton.nulls.remove(name);
                        }
                        if execute_binding_plan_seeded(read, &iteration.plan, Some(1), Some(&singleton))
                            .await?
                            .is_empty()
                        {
                            continue;
                        }
                    }
                    // The final child takes ownership of its parent's history.
                    // A chain has one child, so both collections grow in place
                    // rather than copying every earlier hop at every depth.
                    // Siblings get copy-on-write histories and cannot mutate
                    // each other's trail.
                    let history = if index + 1 == candidates.len() {
                        std::mem::take(&mut state.history)
                    } else {
                        state.history.clone()
                    }
                    .extend(relationship, *target_id);
                    let next_state = TraversalState {
                        row: state.row.clone(),
                        start: state.start,
                        current: *target_id,
                        iteration_source: iteration.map(|_| target.clone()),
                        history,
                    };
                    if depth >= u32::from(min_hops)
                        && target_labels.iter().all(|label| target.labels.contains(label.as_str()))
                        && next_state.row.vertices.get(to).is_none_or(|existing| existing == target)
                    {
                        let mut row = next_state.row.clone();
                        row.vertices.insert(to.clone(), target.clone());
                        if bind_path(&mut row, path_binding, next_state.history.records()) && residuals_hold(&row, residuals)? {
                            output.push(row);
                            read.check_intermediate_rows("cypher_variable_expand_output_rows", output.len())?;
                            if row_limit.is_some_and(|limit| output.len() >= limit) {
                                return Ok(output);
                            }
                        }
                    }
                    if !closes {
                        next.push(next_state);
                        read.check_intermediate_rows("cypher_variable_expand_frontier", next.len())?;
                    }
                }
            }
        }
        frontier = next;
    }
    Ok(output)
}

/// Bind a variable-length relationship name to the path's relationships, in
/// walk order, as a list value. `size(rs)` is then the hop count.
fn bind_path(row: &mut BindingRow, binding: Option<&Symbol>, path: &[RelationshipRecord]) -> bool {
    if let Some(binding) = binding {
        let value = QueryValue::List(path.iter().map(|record| QueryValue::RelationshipId(record.id)).collect());
        if row.values.get(binding).is_some_and(|existing| existing != &value) {
            return false;
        }
        if row.is_null(binding) {
            return false;
        }
        row.values.insert(binding.clone(), value);
        row.relationship_lists.insert(binding.clone(), std::sync::Arc::new(path.to_vec()));
    }
    true
}

fn canonical_relationships(
    read: &dyn GraphRead,
    relationships: Vec<RelationshipRecord>,
) -> EngineResult<BTreeMap<RelationshipId, RelationshipRecord>> {
    let mut canonical = BTreeMap::new();
    for relationship in relationships {
        read.checkpoint("cypher_canonical_relationships")?;
        if let Some(previous) = canonical.insert(relationship.id, relationship.clone()) {
            if previous != relationship {
                return Err(EngineError::InvalidStorageResponse(format!(
                    "relationship {} was returned with conflicting records",
                    relationship.id
                )));
            }
        }
    }
    Ok(canonical)
}

fn merge_compatible_rows(left: &BindingRow, right: &BindingRow) -> Option<BindingRow> {
    if left.vertices.iter().any(|(binding, vertex)| {
        right
            .vertices
            .get(binding)
            .is_some_and(|other| other != vertex)
    }) || left.relationships.iter().any(|(binding, relationship)| {
        right
            .relationships
            .get(binding)
            .is_some_and(|other| other != relationship)
    }) || left
        .vertices
        .keys()
        .any(|binding| right.relationships.contains_key(binding))
        || left
            .relationships
            .keys()
            .any(|binding| right.vertices.contains_key(binding))
        // Null equals nothing, itself included, so a side that bound a name
        // the other side calls null cannot be reconciled with it.
        || left
            .nulls
            .iter()
            .any(|binding| right.vertices.contains_key(binding)
                || right.relationships.contains_key(binding))
        || right
            .nulls
            .iter()
            .any(|binding| left.vertices.contains_key(binding)
                || left.relationships.contains_key(binding))
    {
        return None;
    }
    if left.values.iter().any(|(name, value)| {
        right
            .values
            .get(name)
            .is_some_and(|other| AggregateDistinctKey(value) != AggregateDistinctKey(other))
    }) {
        return None;
    }
    let mut merged = left.clone();
    merged.vertices.extend(right.vertices.clone());
    merged.relationships.extend(right.relationships.clone());
    merged.nulls.extend(right.nulls.iter().cloned());
    merged.values.extend(right.values.clone());
    merged.relationship_lists.extend(right.relationship_lists.clone());
    merged.deferred.extend(right.deferred.clone());
    merged.vertex_records.extend(right.vertex_records.clone());
    merged.conditions.extend(right.conditions.clone());
    Some(merged)
}

/// Whether `source.id` projects as `source` itself: a node's identity is the
/// node, and a null or a subquery's mark is carried as it is (so the mark's
/// exception is raised). A relationship's `.id` is its stored property and a
/// record's its `id` field, values `evaluate_expression` reads.
fn identity_is_binding(row: &BindingRow, source: &Symbol) -> bool {
    row.vertices.contains_key(source)
        || matches!(row.values.get(source), Some(QueryValue::VertexId(_)))
        || row.is_null(source)
        || row.deferred.contains_key(source)
}

/// §20.14 General Rule 1: the path `PATH[...]` enumerates, which has to
/// alternate nodes and edges, start and end on a node, and have each edge
/// join the nodes either side of it.
fn construct_path<'a>(
    elements: &[EvaluatedValue],
    vertex: impl Fn(VertexId) -> EngineResult<&'a VertexRecord>,
    relationship: impl Fn(RelationshipId) -> EngineResult<&'a RelationshipRecord>,
) -> EngineResult<PathValue> {
    let malformed = || EngineError::data(GqlStatus::MALFORMED_PATH, "PATH[...] does not identify a path");
    if elements.len().is_multiple_of(2) {
        return Err(malformed());
    }
    let mut path = PathValue {
        nodes: Vec::with_capacity(elements.len() / 2 + 1),
        relationships: Vec::with_capacity(elements.len() / 2),
    };
    for (index, element) in elements.iter().enumerate() {
        match (index % 2, element) {
            (0, EvaluatedValue::Vertex(id)) => path.nodes.push(vertex(*id)?.clone()),
            (1, EvaluatedValue::Relationship(id)) => path.relationships.push(relationship(*id)?.clone()),
            _ => return Err(malformed()),
        }
    }
    let joined = path.relationships.iter().enumerate().all(|(index, relationship)| {
        let (from, to) = (path.nodes[index].id, path.nodes[index + 1].id);
        (relationship.source == from && relationship.target == to) || (relationship.source == to && relationship.target == from)
    });
    if joined {
        Ok(path)
    } else {
        Err(malformed())
    }
}

/// Run `visit` on a record that is `row` with the group variable `group`
/// bound to each of its elements alone, in turn (§22.7 General Rule 5), and
/// return how many there were. A null group has none. One record is amended
/// for every element rather than `row` copied once per element.
fn for_each_element(row: &BindingRow, group: &Symbol, visit: &mut dyn FnMut(&BindingRow) -> EngineResult<()>) -> EngineResult<usize> {
    let mut element = row.clone();
    element.values.remove(group);
    element.relationship_lists.remove(group);
    if let Some(records) = row.relationship_lists.get(group) {
        for record in records.iter() {
            element.relationships.insert(group.clone(), record.clone());
            visit(&element)?;
        }
        return Ok(records.len());
    }
    match row.values.get(group) {
        Some(QueryValue::List(elements)) => {
            for value in elements {
                let QueryValue::VertexId(id) = value else {
                    return Err(unsupported(
                        QueryFailureReason::Where,
                        "a horizontal aggregate reads a group of nodes or edges",
                    ));
                };
                let record = row_vertex_record(row, *id)?.clone();
                element.vertices.insert(group.clone(), record);
                visit(&element)?;
            }
            Ok(elements.len())
        }
        _ => Ok(0),
    }
}

/// The record of a node a row reaches; see [`row_vertex_records`].
fn row_vertex_record(row: &BindingRow, id: VertexId) -> EngineResult<&VertexRecord> {
    path_vertex_record(row_vertex_records(row), id)
}

fn path_vertex_record<'a>(mut records: impl Iterator<Item = &'a VertexRecord>, id: VertexId) -> EngineResult<&'a VertexRecord> {
    records
        .find(|record| record.id == id)
        .ok_or_else(|| EngineError::data(GqlStatus::FEATURE_NOT_SUPPORTED, "PATH[...] reads only nodes the row binds"))
}

/// The record of an edge a row reaches; see [`row_relationship_records`].
fn row_relationship_record(row: &BindingRow, id: RelationshipId) -> EngineResult<&RelationshipRecord> {
    path_relationship_record(row_relationship_records(row), id)
}

fn path_relationship_record<'a>(
    mut records: impl Iterator<Item = &'a RelationshipRecord>,
    id: RelationshipId,
) -> EngineResult<&'a RelationshipRecord> {
    records
        .find(|record| record.id == id)
        .ok_or_else(|| EngineError::data(GqlStatus::FEATURE_NOT_SUPPORTED, "PATH[...] reads only edges the row binds"))
}

/// The property a relationship's `.id` reads: the identity the client wrote.
const RELATIONSHIP_ID_PROPERTY: &str = "id";

/// The relationship record `binding` names in `row`, wherever it came from: a
/// pattern or UNWIND bound it, or a computed projection (`coalesce(r, ...)`,
/// CASE, `min(r)`) carried it as a value whose record the row retained.
fn bound_relationship<'a>(row: &'a BindingRow, binding: &Symbol) -> Option<&'a RelationshipRecord> {
    if let Some(relationship) = row.relationships.get(binding) {
        return Some(relationship);
    }
    let Some(QueryValue::RelationshipId(id)) = row.values.get(binding) else {
        return None;
    };
    row.relationship_lists
        .get(binding)
        .into_iter()
        .flat_map(|records| records.iter())
        .chain(row_relationship_records(row))
        .find(|record| record.id == *id)
}

fn evaluate_expression(expression: &PhysicalExpression, row: &BindingRow) -> EngineResult<EvaluatedValue> {
    Ok(match expression {
        PhysicalExpression::TypeTest {
            expression,
            value_type,
            negated,
        } => EvaluatedValue::Truth(Some(
            types::is_of_type(&evaluate_expression(expression, row)?, value_type) != *negated,
        )),
        PhysicalExpression::Cast {
            expression,
            value_type,
            request,
        } => {
            let request = request.as_deref().map(|request| evaluate_expression(request, row)).transpose()?;
            types::cast(evaluate_expression(expression, row)?, value_type, request.as_ref())?
        }
        // §20.18 General Rule 2.
        PhysicalExpression::Record(fields) => EvaluatedValue::Record(
            fields
                .iter()
                .map(|(name, value)| Ok((Box::from(name.as_str()), evaluate_expression(value, row)?)))
                .collect::<EngineResult<Vec<_>>>()?,
        ),
        PhysicalExpression::Field { expression, field } => record_field(evaluate_expression(expression, row)?, field, row)?,
        // §20.5 General Rules 1-2: every value is evaluated in the incoming
        // record, then the body runs in a child context amended with them.
        PhysicalExpression::Let { bindings, body } => {
            let mut scope = row.clone();
            for (name, value) in bindings {
                let value = to_query_value(evaluate_expression(value, row)?);
                // A node or edge the local name holds keeps the record the row
                // has for it. Lowering refuses rebinding a name in scope; the
                // name is cleared of any element all the same.
                scope.vertices.remove(name);
                scope.relationships.remove(name);
                scope.relationship_lists.remove(name);
                scope.nulls.remove(name);
                retain_vertex_records(std::iter::once(row), &value, &mut scope.vertex_records);
                if let Some(records) = retain_relationship_records(std::iter::once(row), &value) {
                    scope.relationship_lists.insert(name.clone(), records);
                }
                scope.values.insert(name.clone(), value);
            }
            evaluate_expression(body, &scope)?
        }
        PhysicalExpression::PathConstructor(elements) => {
            let elements = elements
                .iter()
                .map(|element| evaluate_expression(element, row))
                .collect::<EngineResult<Vec<_>>>()?;
            EvaluatedValue::Path(Box::new(construct_path(
                &elements,
                |id| row_vertex_record(row, id),
                |id| row_relationship_record(row, id),
            )?))
        }
        PhysicalExpression::Binding(binding) => {
            row.read_mark(binding)?;
            if let Some(vertex) = row.vertices.get(binding) {
                EvaluatedValue::Vertex(vertex.id)
            } else if let Some(relationship) = row.relationships.get(binding) {
                EvaluatedValue::Relationship(relationship.id)
            } else if let Some(value) = row.values.get(binding) {
                evaluated_from_query_value(value)
            } else {
                EvaluatedValue::Null
            }
        }
        // A vertex's identity is the client's `id`. A relationship's is an
        // internal counter, and the client's `id` is its stored property --
        // however the relationship reached this name.
        PhysicalExpression::Identity(binding) => {
            if let Some(vertex) = row_vertex(row, binding) {
                EvaluatedValue::Vertex(vertex.id)
            } else if let Some(relationship) = bound_relationship(row, binding) {
                match relationship.properties.get(RELATIONSHIP_ID_PROPERTY) {
                    None | Some(ScalarValue::Null) => EvaluatedValue::Null,
                    Some(value) => evaluated_from_scalar(value),
                }
            } else if let Some(record @ QueryValue::Record(_)) = row.values.get(binding) {
                record_field(evaluated_from_query_value(record), &Symbol::from("id"), row)?
            } else {
                EvaluatedValue::Null
            }
        }
        PhysicalExpression::Property { binding, property } => {
            let properties = if let Some(vertex) = row_vertex(row, binding) {
                &vertex.properties
            } else if let Some(relationship) = bound_relationship(row, binding) {
                &relationship.properties
            } else if let Some(record @ QueryValue::Record(_)) = row.values.get(binding) {
                // §20.11 General Rule 3 b): a record's field.
                return record_field(evaluated_from_query_value(record), property, row);
            } else {
                // Includes the case of a name bound to a scalar: a scalar has
                // no properties, and Cypher reads a missing property as null.
                return Ok(EvaluatedValue::Null);
            };
            match properties.get(property.as_str()) {
                None | Some(ScalarValue::Null) => EvaluatedValue::Null,
                Some(value) => evaluated_from_scalar(value),
            }
        }
        PhysicalExpression::Value(value) => match &value.value {
            ScalarValue::Null => EvaluatedValue::Null,
            value => evaluated_from_scalar(value),
        },
        PhysicalExpression::ElementTest { binding, test } => evaluate_element_test(row, binding, test)?,
        PhysicalExpression::PathNodesDistinct { simple, parts } => EvaluatedValue::Truth(path_nodes_distinct(row, *simple, parts)?),
        // §22.7: a horizontal aggregate reads its group variable's elements
        // one at a time, each in a record amended with that element alone.
        PhysicalExpression::Aggregate {
            function,
            distinct,
            expression,
            fraction,
            group: Some(group),
        } => {
            // Each element in one record, amended and read in turn: no copy
            // of the record per element.
            let mut values = Vec::new();
            let count = for_each_element(row, group, &mut |element| {
                if let Some(expression) = expression.as_deref() {
                    values.push(to_query_value(evaluate_expression(expression, element)?));
                }
                Ok(())
            })?;
            evaluated_from_query_value(&aggregate_values(
                *function,
                *distinct,
                expression.is_some(),
                values,
                count,
                fraction.as_deref(),
                &Cell::new(false),
            )?)
        }
        PhysicalExpression::Aggregate { group: None, .. } => {
            return Err(unsupported(
                QueryFailureReason::Return,
                "aggregate expressions must be evaluated by the projection stage",
            ));
        }
        PhysicalExpression::Unary { operator, expression } => evaluate_unary(*operator, evaluate_expression(expression, row)?)?,
        PhysicalExpression::Binary { left, operator, right } => {
            evaluate_binary(*operator, evaluate_expression(left, row)?, evaluate_expression(right, row)?)?
        }
        PhysicalExpression::InList { expression, values } => evaluate_in_list(evaluate_expression(expression, row)?, values),
        PhysicalExpression::Function {
            function: ScalarFunction::IdProperty,
            arguments,
        } => {
            let value = evaluate_expression(&arguments[0], row)?;
            stored_record_field(value, &Symbol::from("id"), row)?
        }
        PhysicalExpression::Function { function, arguments } => {
            if *function == ScalarFunction::Coalesce {
                for argument in arguments {
                    let value = evaluate_expression(argument, row)?;
                    if !is_null(&value) {
                        return Ok(value);
                    }
                }
                return Ok(EvaluatedValue::Null);
            }
            let arguments = arguments
                .iter()
                .map(|argument| evaluate_expression(argument, row))
                .collect::<EngineResult<Vec<_>>>()?;
            evaluate_function(*function, arguments)?
        }
        PhysicalExpression::List(items) => EvaluatedValue::List(
            items
                .iter()
                .map(|item| evaluate_expression(item, row))
                .collect::<EngineResult<Vec<_>>>()?,
        ),
        PhysicalExpression::Case { .. } => evaluate_case(expression, &mut |part| evaluate_expression(part, row))?,
    })
}

/// What a name in a row is bound to, for a predicate that reads the element
/// itself.
enum BoundElement<'a> {
    Node(&'a VertexRecord),
    Edge(&'a RelationshipRecord),
    Null,
}

fn bound_element<'a>(row: &'a BindingRow, binding: &Symbol) -> EngineResult<BoundElement<'a>> {
    if let Some(vertex) = row_vertex(row, binding) {
        return Ok(BoundElement::Node(vertex));
    }
    if let Some(relationship) = bound_relationship(row, binding) {
        return Ok(BoundElement::Edge(relationship));
    }
    match row.values.get(binding) {
        None => Ok(BoundElement::Null),
        Some(value) if value.is_null() => Ok(BoundElement::Null),
        Some(_) => Err(EngineError::data(
            GqlStatus::INVALID_VALUE_TYPE,
            format!("{binding} is not a node or an edge"),
        )),
    }
}

fn wrong_element(binding: &Symbol, wanted: &str) -> EngineError {
    EngineError::data(GqlStatus::INVALID_VALUE_TYPE, format!("{binding} is not {wanted}"))
}

/// GQL §19.8 to §19.13 on the element bound to `binding`: unknown
/// for a null element, and `22G03` for one of the wrong kind, which the
/// engine finds only as it evaluates (the static type check §19.8 Syntax
/// Rule 3 and §19.10 Syntax Rules 2 and 3 describe).
fn evaluate_element_test(row: &BindingRow, binding: &Symbol, test: &ElementTestKind) -> EngineResult<EvaluatedValue> {
    let element = bound_element(row, binding)?;
    let truth = match test {
        ElementTestKind::Labeled(expression) => match element {
            BoundElement::Null => None,
            BoundElement::Node(vertex) => Some(labels_satisfy(
                expression,
                &|label| vertex.labels.contains(label),
                !vertex.labels.is_empty(),
            )),
            BoundElement::Edge(edge) => Some(labels_satisfy(expression, &|label| edge.relationship_type == label, true)),
        },
        ElementTestKind::Directed => match element {
            BoundElement::Null => None,
            // HydraDB stores no undirected edges.
            BoundElement::Edge(_) => Some(true),
            BoundElement::Node(_) => return Err(wrong_element(binding, "an edge")),
        },
        ElementTestKind::SourceOf(edge) | ElementTestKind::DestinationOf(edge) => {
            let node = match element {
                BoundElement::Node(node) => Some(node),
                BoundElement::Null => None,
                BoundElement::Edge(_) => return Err(wrong_element(binding, "a node")),
            };
            let edge_record = match bound_element(row, edge)? {
                BoundElement::Edge(edge) => Some(edge),
                BoundElement::Null => None,
                BoundElement::Node(_) => return Err(wrong_element(edge, "an edge")),
            };
            match (node, edge_record) {
                (Some(node), Some(edge_record)) => Some(if matches!(test, ElementTestKind::SourceOf(_)) {
                    edge_record.source == node.id
                } else {
                    edge_record.target == node.id
                }),
                _ => None,
            }
        }
        ElementTestKind::PropertyExists(property) | ElementTestKind::StoredPropertyExists(property) => {
            let has = |properties: &BTreeMap<String, ScalarValue>| {
                properties
                    .get(property.as_str())
                    .is_some_and(|value| !matches!(value, ScalarValue::Null))
            };
            match element {
                BoundElement::Null => None,
                // Every node has `id`, its identity (the `n.id` data model).
                BoundElement::Node(node) => Some(
                    (matches!(test, ElementTestKind::PropertyExists(_)) && property.as_str() == NODE_ID_PROPERTY) || has(&node.properties),
                ),
                BoundElement::Edge(edge) => Some(has(&edge.properties)),
            }
        }
    };
    Ok(EvaluatedValue::Truth(truth))
}

const NODE_ID_PROPERTY: &str = "id";

/// The path values a row holds, alone or in lists or records, whose
/// elements' records a node or edge taken out of one reads.
fn row_paths(row: &BindingRow) -> Vec<&PathValue> {
    fn paths<'a>(value: &'a QueryValue, into: &mut Vec<&'a PathValue>) {
        match value {
            QueryValue::Path(path) => into.push(path),
            QueryValue::List(values) => values.iter().for_each(|value| paths(value, into)),
            QueryValue::Record(fields) => fields.iter().for_each(|(_, value)| paths(value, into)),
            _ => {}
        }
    }
    let mut found = Vec::new();
    row.values.values().for_each(|value| paths(value, &mut found));
    found
}

/// Every node record a row holds: bound by a pattern, kept for a value that
/// names the node, or inside a path value. A node held by identity anywhere
/// in the row is found here, however it reached its name.
fn row_vertex_records(row: &BindingRow) -> impl Iterator<Item = &VertexRecord> {
    row.vertices
        .values()
        .chain(row.vertex_records.values())
        .chain(row_paths(row).into_iter().flat_map(|path| path.nodes.iter()))
}

/// Every relationship record a row holds, as [`row_vertex_records`] for
/// nodes: bound, in a relationship list, or inside a path value.
fn row_relationship_records(row: &BindingRow) -> impl Iterator<Item = &RelationshipRecord> {
    row.relationships
        .values()
        .chain(row.relationship_lists.values().flat_map(|records| records.iter()))
        .chain(row_paths(row).into_iter().flat_map(|path| path.relationships.iter()))
}

/// The path a row binds through `parts`: its node identities and its edges,
/// in the path's order. A variable-length hop's list runs from whichever end
/// meets the node before it, so a list walked from the far end reads back.
/// `None` when any part is unbound or null.
fn walk_path<'a>(row: &'a BindingRow, parts: &[PathPart]) -> Option<(Vec<VertexId>, Vec<&'a RelationshipRecord>)> {
    let mut nodes: Vec<VertexId> = Vec::new();
    let mut edges: Vec<&RelationshipRecord> = Vec::new();
    // After a list of no relationships the next node is the one before it:
    // the two bindings are not separable, so the node appears once.
    let mut repeats_previous = false;
    // A node part right after another is the same node (§16.7 G046).
    let mut after_node = false;
    for part in parts {
        let beside = std::mem::replace(&mut after_node, matches!(part, PathPart::Node(_)));
        match part {
            PathPart::Node(name) => {
                let vertex = row.vertices.get(name)?;
                if !std::mem::take(&mut repeats_previous) && !beside {
                    nodes.push(vertex.id);
                }
            }
            PathPart::Edge(name) => edges.push(bound_relationship(row, name)?),
            PathPart::Edges(name) => {
                let records = row.relationship_lists.get(name)?;
                let mut current = *nodes.last()?;
                let forward = records
                    .first()
                    .is_none_or(|first| first.source == current || first.target == current);
                let ordered: Vec<&RelationshipRecord> = if forward {
                    records.iter().collect()
                } else {
                    records.iter().rev().collect()
                };
                // Every node the hop passes through but its last, which is
                // the next node part.
                for record in ordered.iter().take(ordered.len().saturating_sub(1)) {
                    current = if record.source == current { record.target } else { record.source };
                    nodes.push(current);
                }
                repeats_previous = ordered.is_empty();
                edges.extend(ordered);
            }
        }
    }
    Some((nodes, edges))
}

/// An element of a reduced word.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
enum WordElement {
    Node(VertexId),
    Edge(RelationshipId),
}

/// A word's element and the variable naming it, `None` for the anonymous
/// symbol.
type WordLetterBinding = (Option<Symbol>, WordElement);

/// A path pattern union operand's match as its reduced word (GQL §22.2
/// General Rule 14, §16.4 General Rule 10): the operand's subpath variables,
/// then each element in written order with the variable naming it. Of node
/// bindings next to each other, which bind one node, the anonymous ones go
/// beside a named one, and all but one go otherwise. Two operands' matches are
/// one match exactly when their words agree.
fn reduced_word(row: &BindingRow, word: &PathWord) -> Option<(Vec<Symbol>, Vec<WordLetterBinding>)> {
    let mut letters: Vec<WordLetterBinding> = Vec::new();
    let mut current = None;
    for letter in &word.letters {
        match &letter.part {
            PathPart::Node(binding) => {
                let id = row.vertices.get(binding)?.id;
                letters.push((letter.name.clone(), WordElement::Node(id)));
                current = Some(id);
            }
            PathPart::Edge(binding) => letters.push((letter.name.clone(), WordElement::Edge(bound_relationship(row, binding)?.id))),
            // §16.7 Syntax Rule 14 a): a quantified edge `-[e]->{m,n}` is
            // `(() -[e]-> ()){m,n}`; a quantified path names its iterations'
            // ends itself.
            PathPart::Edges(binding) => {
                let records = row.relationship_lists.get(binding)?;
                let mut node = current?;
                let forward = records.first().is_none_or(|first| first.source == node || first.target == node);
                let ordered: Vec<&RelationshipRecord> = if forward {
                    records.iter().collect()
                } else {
                    records.iter().rev().collect()
                };
                let (start, end) = letter.iteration.clone().unwrap_or_default();
                for record in ordered {
                    letters.push((start.clone(), WordElement::Node(node)));
                    letters.push((letter.name.clone(), WordElement::Edge(record.id)));
                    node = if record.source == node { record.target } else { record.source };
                    letters.push((end.clone(), WordElement::Node(node)));
                }
            }
        }
    }
    let mut reduced: Vec<WordLetterBinding> = Vec::new();
    let mut run: Vec<WordLetterBinding> = Vec::new();
    let close = |run: &mut Vec<WordLetterBinding>, reduced: &mut Vec<WordLetterBinding>| {
        if run.iter().any(|(name, _)| name.is_some()) {
            run.retain(|(name, _)| name.is_some());
        } else {
            run.truncate(1);
        }
        reduced.append(run);
    };
    for letter in letters {
        if matches!(letter.1, WordElement::Node(_)) {
            run.push(letter);
        } else {
            close(&mut run, &mut reduced);
            reduced.push(letter);
        }
    }
    close(&mut run, &mut reduced);
    Some((word.subpaths.clone(), reduced))
}

/// GQL's ACYCLIC and SIMPLE (§22.2 General Rule 16 d) ii)): no two separable
/// node bindings of the path bind one node, except, when `simple`, its first
/// and last. Unknown when any part is null.
fn path_nodes_distinct(row: &BindingRow, simple: bool, parts: &[PathPart]) -> EngineResult<Option<bool>> {
    let Some((nodes, _)) = walk_path(row, parts) else {
        return Ok(None);
    };
    let last = nodes.len().saturating_sub(1);
    let mut seen = BTreeSet::new();
    for (index, node) in nodes.iter().enumerate() {
        if index == last && simple && index > 0 && *node == nodes[0] {
            continue;
        }
        if !seen.insert(*node) {
            return Ok(Some(false));
        }
    }
    Ok(Some(true))
}

/// Whether a label set satisfies a label expression (GQL §22.5). `has`
/// answers for one label; `any` says whether the set is non-empty, which is
/// what the wildcard `%` asks.
fn labels_satisfy(expression: &hydradb_cypher_ast::LabelExpression, has: &dyn Fn(&str) -> bool, any: bool) -> bool {
    use hydradb_cypher_ast::LabelExpression;
    match expression {
        LabelExpression::Label(label) => has(label.as_str()),
        LabelExpression::Wildcard => any,
        LabelExpression::Not(term) => !labels_satisfy(term, has, any),
        LabelExpression::And(terms) => terms.iter().all(|term| labels_satisfy(term, has, any)),
        LabelExpression::Or(terms) => terms.iter().any(|term| labels_satisfy(term, has, any)),
    }
}

/// Every sub-expression of a CASE, in written order.
fn case_parts(expression: &PhysicalExpression) -> Vec<&PhysicalExpression> {
    let PhysicalExpression::Case {
        operand,
        branches,
        default,
    } = expression
    else {
        return Vec::new();
    };
    let mut parts: Vec<&PhysicalExpression> = operand.as_deref().into_iter().collect();
    for (when, then) in branches {
        parts.push(when);
        parts.push(then);
    }
    parts.extend(default.as_deref());
    parts
}

/// The value of a CASE, evaluating only what the chosen branch needs: the
/// operand, the `WHEN`s up to the first that holds, and its `THEN`. A simple
/// CASE compares with `=`, so a null operand matches no `WHEN`; a searched
/// CASE takes a `WHEN` only when it is true, not when it is null. With no
/// branch taken it is the `ELSE`, or null.
fn evaluate_case(
    expression: &PhysicalExpression,
    evaluate: &mut dyn FnMut(&PhysicalExpression) -> EngineResult<EvaluatedValue>,
) -> EngineResult<EvaluatedValue> {
    let PhysicalExpression::Case {
        operand,
        branches,
        default,
    } = expression
    else {
        unreachable!("evaluate_case is called on CASE only");
    };
    let operand = operand.as_deref().map(&mut *evaluate).transpose()?;
    for (when, then) in branches {
        let when = evaluate(when)?;
        let taken = match &operand {
            Some(operand) => evaluate_equal(operand.clone(), when) == Some(true),
            None => as_truth(when, "a CASE condition")? == Some(true),
        };
        if taken {
            return evaluate(then);
        }
    }
    default.as_deref().map_or(Ok(EvaluatedValue::Null), evaluate)
}

/// An element as `ALL_DIFFERENT` and `SAME` compare it: nodes and edges are
/// never comparable with each other (§19.11, §19.12 General Rule 2 b)).
#[derive(Clone, Copy, Eq, Ord, PartialEq, PartialOrd)]
enum ElementIdentity {
    Node(VertexId),
    Edge(RelationshipId),
}

fn element_identity(function: ScalarFunction, value: &EvaluatedValue) -> EngineResult<ElementIdentity> {
    match value {
        EvaluatedValue::Vertex(id) => Ok(ElementIdentity::Node(*id)),
        EvaluatedValue::Relationship(id) => Ok(ElementIdentity::Edge(*id)),
        value if is_null(value) => Err(EngineError::data(
            GqlStatus::NULL_VALUE_NOT_ALLOWED,
            format!("{} does not take a null element", function.name()),
        )),
        _ => Err(EngineError::data(
            GqlStatus::INVALID_VALUE_TYPE,
            format!("{} takes nodes or edges", function.name()),
        )),
    }
}

/// `ELEMENT_ID` (§20.10), `ALL_DIFFERENT` (§19.11), `SAME` (§19.12) and
/// `CARDINALITY` (§20.22 General Rule 4).
fn evaluate_element_function(function: ScalarFunction, arguments: Vec<EvaluatedValue>) -> EngineResult<EvaluatedValue> {
    match function {
        // A node's identity is its `id`; an edge's an internal counter, so the
        // two are told apart by a prefix.
        ScalarFunction::Id => Ok(match arguments.first() {
            Some(EvaluatedValue::Vertex(id) | EvaluatedValue::Relationship(id)) => {
                EvaluatedValue::Scalar(ScalarValue::Integer(i128::from(*id)))
            }
            Some(value) if !is_null(value) => {
                return Err(EngineError::data(
                    GqlStatus::INVALID_VALUE_TYPE,
                    "id takes a node or a relationship",
                ));
            }
            _ => EvaluatedValue::Null,
        }),
        ScalarFunction::ElementId => Ok(match arguments.first() {
            Some(EvaluatedValue::Vertex(id)) => EvaluatedValue::Scalar(ScalarValue::String(format!("node:{id}").into())),
            Some(EvaluatedValue::Relationship(id)) => EvaluatedValue::Scalar(ScalarValue::String(format!("edge:{id}").into())),
            Some(value) if !is_null(value) => {
                return Err(EngineError::data(
                    GqlStatus::INVALID_VALUE_TYPE,
                    "element_id takes a node or an edge",
                ))
            }
            _ => EvaluatedValue::Null,
        }),
        ScalarFunction::AllDifferent | ScalarFunction::Same => {
            let elements = arguments
                .iter()
                .map(|argument| element_identity(function, argument))
                .collect::<EngineResult<Vec<_>>>()?;
            let kinds_differ = elements.windows(2).any(|pair| {
                matches!(
                    pair,
                    [ElementIdentity::Node(_), ElementIdentity::Edge(_)] | [ElementIdentity::Edge(_), ElementIdentity::Node(_)]
                )
            });
            if kinds_differ {
                return Err(EngineError::data(
                    GqlStatus::VALUES_NOT_COMPARABLE,
                    format!("{} cannot compare a node with an edge", function.name()),
                ));
            }
            let distinct = elements.iter().collect::<BTreeSet<_>>().len();
            Ok(EvaluatedValue::Truth(Some(if function == ScalarFunction::Same {
                distinct == 1
            } else {
                distinct == elements.len()
            })))
        }
        ScalarFunction::Cardinality => Ok(match arguments.into_iter().next() {
            Some(EvaluatedValue::List(items)) => EvaluatedValue::Scalar(ScalarValue::Integer(items.len() as i128)),
            Some(EvaluatedValue::Record(fields)) => EvaluatedValue::Scalar(ScalarValue::Integer(fields.len() as i128)),
            // A path's cardinality is its element list's (§4.15.2).
            Some(EvaluatedValue::Path(path)) => {
                EvaluatedValue::Scalar(ScalarValue::Integer((path.nodes.len() + path.relationships.len()) as i128))
            }
            Some(value) if !is_null(&value) => {
                return Err(EngineError::data(
                    GqlStatus::INVALID_VALUE_TYPE,
                    "cardinality takes a list, a record or a path",
                ));
            }
            _ => EvaluatedValue::Null,
        }),
        ScalarFunction::PathLength | ScalarFunction::Elements => Ok(match arguments.into_iter().next() {
            Some(EvaluatedValue::Path(path)) if function == ScalarFunction::PathLength => {
                EvaluatedValue::Scalar(ScalarValue::Integer(path.relationships.len() as i128))
            }
            Some(EvaluatedValue::Path(path)) => {
                let mut elements = Vec::with_capacity(path.nodes.len() + path.relationships.len());
                for (index, node) in path.nodes.iter().enumerate() {
                    elements.push(EvaluatedValue::Vertex(node.id));
                    if let Some(relationship) = path.relationships.get(index) {
                        elements.push(EvaluatedValue::Relationship(relationship.id));
                    }
                }
                EvaluatedValue::List(elements)
            }
            Some(value) if !is_null(&value) => {
                return Err(EngineError::data(
                    GqlStatus::INVALID_VALUE_TYPE,
                    format!("{} takes a path", function.name()),
                ));
            }
            _ => EvaluatedValue::Null,
        }),
        other => unreachable!("{} is not an element function", other.name()),
    }
}

/// Lazy arguments are selected by the row/group evaluator before this dispatch.
fn evaluate_function(function: ScalarFunction, arguments: Vec<EvaluatedValue>) -> EngineResult<EvaluatedValue> {
    // §20.29: the absolute value of a duration.
    if function == ScalarFunction::Abs {
        if let Some(value) = arguments.first().and_then(time::duration_absolute) {
            return value;
        }
    }
    use crate::ScalarFunctionFamily;
    match function.family() {
        ScalarFunctionFamily::RelationshipTrail => evaluate_relationship_trail(arguments),
        ScalarFunctionFamily::ListElement => evaluate_list_element(arguments),
        ScalarFunctionFamily::Coalesce => Ok(arguments
            .into_iter()
            .find(|argument| !is_null(argument))
            .unwrap_or(EvaluatedValue::Null)),
        ScalarFunctionFamily::CypherConversion => evaluate_cypher_conversion(function, arguments),
        ScalarFunctionFamily::Element => evaluate_element_function(function, arguments),
        ScalarFunctionFamily::Temporal => time::evaluate_temporal_function(function, arguments),
        ScalarFunctionFamily::Gql => functions::evaluate_gql_function(function, arguments),
    }
}

/// `list[index]`, Cypher's list element access.
///
/// - A null list or a null index is null.
/// - The index counts from 0; a negative index counts back from the end, so
///   `[-1]` is the last element.
/// - An index past either end is null, not an error.
/// - Anything but a list on the left, or anything but an integer as the
///   index, is a type error (TCK `List1` scenarios 6 to 9).
fn evaluate_list_element(arguments: Vec<EvaluatedValue>) -> EngineResult<EvaluatedValue> {
    let [list, index]: [EvaluatedValue; 2] = arguments
        .try_into()
        .map_err(|_| EngineError::data(GqlStatus::INVALID_VALUE_TYPE, "list element access takes a list and an index"))?;
    if is_null(&list) || is_null(&index) {
        return Ok(EvaluatedValue::Null);
    }
    let EvaluatedValue::List(values) = list else {
        return Err(EngineError::data(
            GqlStatus::INVALID_VALUE_TYPE,
            "list element access requires a list",
        ));
    };
    let EvaluatedValue::Scalar(ScalarValue::Integer(index)) = index else {
        return Err(EngineError::data(GqlStatus::INVALID_VALUE_TYPE, "a list index must be an integer"));
    };
    let length = values.len() as i128;
    let position = if index < 0 { length + index } else { index };
    if !(0..length).contains(&position) {
        return Ok(EvaluatedValue::Null);
    }
    Ok(values.into_iter().nth(position as usize).unwrap_or(EvaluatedValue::Null))
}

fn evaluate_relationship_trail(arguments: Vec<EvaluatedValue>) -> EngineResult<EvaluatedValue> {
    let mut seen = BTreeSet::new();
    for argument in arguments {
        let relationships = match argument {
            EvaluatedValue::Relationship(id) => vec![EvaluatedValue::Relationship(id)],
            EvaluatedValue::List(values) => values,
            value if is_null(&value) => continue,
            _ => {
                return Err(unsupported(
                    QueryFailureReason::Pattern,
                    "relationship uniqueness requires relationships",
                ))
            }
        };
        for relationship in relationships {
            let EvaluatedValue::Relationship(id) = relationship else {
                return Err(unsupported(
                    QueryFailureReason::Pattern,
                    "relationship trail contains a non-relationship",
                ));
            };
            if !seen.insert(id) {
                return Ok(EvaluatedValue::Truth(Some(false)));
            }
        }
    }
    Ok(EvaluatedValue::Truth(Some(true)))
}

fn evaluate_cypher_conversion(function: ScalarFunction, arguments: Vec<EvaluatedValue>) -> EngineResult<EvaluatedValue> {
    // Every other function takes one argument, which lowering has checked,
    // and maps null to null.
    let argument = arguments
        .into_iter()
        .next()
        .ok_or_else(|| EngineError::data(GqlStatus::INVALID_VALUE_TYPE, format!("{} needs an argument", function.name())))?;
    if is_null(&argument) {
        return Ok(EvaluatedValue::Null);
    }
    if let (ScalarFunction::Size, EvaluatedValue::List(items)) = (function, &argument) {
        return Ok(EvaluatedValue::Scalar(ScalarValue::Integer(items.len() as i128)));
    }
    let string_argument = |argument: EvaluatedValue| match argument {
        EvaluatedValue::Scalar(ScalarValue::String(value)) => Ok(value),
        _ => Err(EngineError::data(
            GqlStatus::INVALID_VALUE_TYPE,
            format!("{} requires a string argument", function.name()),
        )),
    };
    let string = |value: String| EvaluatedValue::Scalar(ScalarValue::String(value.into()));
    match function {
        ScalarFunction::ToUpper => Ok(string(string_argument(argument)?.to_uppercase())),
        ScalarFunction::ToLower => Ok(string(string_argument(argument)?.to_lowercase())),
        // Characters, not bytes and not UTF-16 units: Neo4j counts `'a😀'` as 2.
        ScalarFunction::Size => Ok(EvaluatedValue::Scalar(ScalarValue::Integer(
            string_argument(argument)?.chars().count() as i128,
        ))),
        ScalarFunction::ToString => Ok(match argument {
            EvaluatedValue::Scalar(ScalarValue::String(value)) => EvaluatedValue::Scalar(ScalarValue::String(value)),
            EvaluatedValue::Scalar(ScalarValue::Integer(value)) => string(value.to_string()),
            EvaluatedValue::Scalar(ScalarValue::Boolean(value)) | EvaluatedValue::Truth(Some(value)) => string(value.to_string()),
            EvaluatedValue::Scalar(ScalarValue::Float(spelling)) => string(cypher_float_string(parse_float(&spelling)?)),
            // Its exact digits, at its scale: `1.50M` is "1.50".
            EvaluatedValue::Scalar(ScalarValue::Decimal(decimal)) => string(decimal.to_string()),
            _ => {
                return Err(EngineError::data(
                    GqlStatus::INVALID_VALUE_TYPE,
                    "toString requires a string, number or boolean",
                ))
            }
        }),
        ScalarFunction::ToInteger => match argument {
            EvaluatedValue::Scalar(ScalarValue::Integer(value)) => Ok(EvaluatedValue::Scalar(ScalarValue::Integer(value))),
            EvaluatedValue::Scalar(ScalarValue::Boolean(value)) | EvaluatedValue::Truth(Some(value)) => {
                Ok(EvaluatedValue::Scalar(ScalarValue::Integer(i128::from(value))))
            }
            EvaluatedValue::Scalar(ScalarValue::Float(spelling)) => truncate_float(parse_float(&spelling)?),
            // Toward zero, as a float is, but exactly; within INT64 too.
            EvaluatedValue::Scalar(ScalarValue::Decimal(decimal)) => {
                let truncated = decimal.truncate();
                if i64::try_from(truncated).is_err() {
                    return Err(EngineError::data(
                        GqlStatus::NUMERIC_VALUE_OUT_OF_RANGE,
                        "decimal is out of range for an integer",
                    ));
                }
                Ok(EvaluatedValue::Scalar(ScalarValue::Integer(truncated)))
            }
            // A string that spells no number is null rather than an error:
            // `toInteger('x')` is how a caller asks whether it is one.
            EvaluatedValue::Scalar(ScalarValue::String(value)) => {
                let value = value.trim();
                if let Ok(integer) = value.parse::<i64>() {
                    return Ok(EvaluatedValue::Scalar(ScalarValue::Integer(i128::from(integer))));
                }
                match value.parse::<f64>() {
                    Ok(float) if float.is_finite() => truncate_float(float),
                    _ => Ok(EvaluatedValue::Null),
                }
            }
            _ => Err(EngineError::data(
                GqlStatus::INVALID_VALUE_TYPE,
                "toInteger requires a string, number or boolean",
            )),
        },
        _ => unreachable!("function family dispatched before conversion"),
    }
}

fn parse_float(spelling: &str) -> EngineResult<f64> {
    spelling
        .parse::<f64>()
        .map_err(|_| EngineError::data(GqlStatus::INVALID_VALUE_TYPE, "invalid float value"))
}

/// A float toward zero as a 64-bit integer, the range Cypher integers have.
fn truncate_float(value: f64) -> EngineResult<EvaluatedValue> {
    const I64_EXCLUSIVE_UPPER: f64 = 9_223_372_036_854_775_808.0;
    let truncated = value.trunc();
    if !truncated.is_finite() || truncated < -I64_EXCLUSIVE_UPPER || truncated >= I64_EXCLUSIVE_UPPER {
        return Err(EngineError::data(
            GqlStatus::NUMERIC_VALUE_OUT_OF_RANGE,
            "float is out of range for an integer",
        ));
    }
    Ok(EvaluatedValue::Scalar(ScalarValue::Integer(truncated as i128)))
}

/// A float spelled the way Cypher spells one, which is Java's
/// `Double.toString`: plain decimal with at least one fractional digit for
/// magnitudes in `[1e-3, 1e7)`, otherwise `d.dddE±n`, both using the shortest
/// digits that round-trip. So `1.0`, `0.1` and `-0.0`, but `1.0E8` and
/// `1.0E21` where Rust would print `100000000` and `1e21`.
fn cypher_float_string(value: f64) -> String {
    if value.is_nan() {
        return "NaN".to_string();
    }
    if value.is_infinite() {
        return if value > 0.0 { "Infinity" } else { "-Infinity" }.to_string();
    }
    let magnitude = value.abs();
    if magnitude == 0.0 || (1e-3..1e7).contains(&magnitude) {
        // Rust's `Debug` is shortest round-trip and keeps a `.0`, and inside
        // this range it never switches to an exponent.
        return format!("{value:?}");
    }
    let scientific = format!("{value:e}");
    let (mantissa, exponent) = scientific.split_once('e').expect("`{:e}` always writes an exponent");
    if mantissa.contains('.') {
        format!("{mantissa}E{exponent}")
    } else {
        format!("{mantissa}.0E{exponent}")
    }
}

/// Cypher membership, which is three-valued.
///
/// An empty list is `false` even for a null probe: there is nothing it could
/// match. Otherwise a null probe is unknown, a hit is true, and a miss is
/// unknown when the list contains a null, because that null might have been
/// the match.
fn evaluate_in_list(probe: EvaluatedValue, values: &[BoundValue]) -> EvaluatedValue {
    if values.is_empty() {
        return EvaluatedValue::Truth(Some(false));
    }
    if is_null(&probe) {
        return EvaluatedValue::Truth(None);
    }
    let mut saw_null = false;
    for candidate in values {
        match evaluate_equal(probe.clone(), evaluated_from_scalar(&candidate.value)) {
            Some(true) => return EvaluatedValue::Truth(Some(true)),
            None => saw_null = true,
            Some(false) => {}
        }
    }
    EvaluatedValue::Truth(if saw_null { None } else { Some(false) })
}

fn evaluate_unary(operator: PhysicalUnaryOperator, value: EvaluatedValue) -> EngineResult<EvaluatedValue> {
    match (operator, value) {
        (PhysicalUnaryOperator::IsNull, value) => Ok(EvaluatedValue::Truth(Some(is_null(&value)))),
        (PhysicalUnaryOperator::IsNotNull, value) => Ok(EvaluatedValue::Truth(Some(!is_null(&value)))),
        (PhysicalUnaryOperator::Not, value) => Ok(EvaluatedValue::Truth(as_truth(value, "the NOT operand")?.map(|value| !value))),
        (operator @ (PhysicalUnaryOperator::Plus | PhysicalUnaryOperator::Minus), value) => match time::temporal_sign(operator, &value) {
            Some(result) => result,
            None => functions::evaluate_sign(operator, value),
        },
    }
}

fn evaluate_binary(operator: PhysicalBinaryOperator, left: EvaluatedValue, right: EvaluatedValue) -> EngineResult<EvaluatedValue> {
    Ok(match operator {
        PhysicalBinaryOperator::Equal => EvaluatedValue::Truth(evaluate_equal(left, right)),
        PhysicalBinaryOperator::NotEqual => EvaluatedValue::Truth(evaluate_equal(left, right).map(|value| !value)),
        PhysicalBinaryOperator::LessThan => EvaluatedValue::Truth(evaluate_ordering(left, right).map(Ordering::is_lt)),
        PhysicalBinaryOperator::LessThanOrEqual => EvaluatedValue::Truth(evaluate_ordering(left, right).map(Ordering::is_le)),
        PhysicalBinaryOperator::GreaterThan => EvaluatedValue::Truth(evaluate_ordering(left, right).map(Ordering::is_gt)),
        PhysicalBinaryOperator::GreaterThanOrEqual => EvaluatedValue::Truth(evaluate_ordering(left, right).map(Ordering::is_ge)),
        PhysicalBinaryOperator::And => {
            EvaluatedValue::Truth(evaluate_and(as_truth(left, "AND operands")?, as_truth(right, "AND operands")?))
        }
        PhysicalBinaryOperator::Or => EvaluatedValue::Truth(evaluate_or(as_truth(left, "OR operands")?, as_truth(right, "OR operands")?)),
        // §20.20 Syntax Rule 4's `(X OR Y) AND NOT (X AND Y)`, evaluated
        // once: null when either side is null.
        PhysicalBinaryOperator::Xor => EvaluatedValue::Truth(match (as_truth(left, "XOR operands")?, as_truth(right, "XOR operands")?) {
            (Some(left), Some(right)) => Some(left != right),
            _ => None,
        }),
        PhysicalBinaryOperator::Add
        | PhysicalBinaryOperator::Subtract
        | PhysicalBinaryOperator::Multiply
        | PhysicalBinaryOperator::Divide
        | PhysicalBinaryOperator::Modulo => match time::temporal_arithmetic(operator, &left, &right) {
            Some(result) => result?,
            None => functions::evaluate_arithmetic(operator, left, right)?,
        },
        PhysicalBinaryOperator::StartsWith | PhysicalBinaryOperator::EndsWith | PhysicalBinaryOperator::Contains => {
            evaluate_string_predicate(operator, left, right)?
        }
        PhysicalBinaryOperator::Concatenate => functions::evaluate_concatenate(left, right)?,
        PhysicalBinaryOperator::In => evaluate_list_membership(left, right)?,
    })
}

/// `probe IN list` for a list computed per row, with three-valued logic: an
/// empty list is false, a null list or probe is unknown, and a null element
/// that could have matched makes a miss unknown. Equality is Cypher equality,
/// the same `evaluate_in_list` uses for planned lists, so `1 IN [1.0]` holds.
///
/// A null list is null in every form the engine produces: a missing property,
/// a null scalar, or an unknown truth value such as `(NULL = 1)`. Only a
/// non-null value that is not a list is a type error.
fn evaluate_list_membership(probe: EvaluatedValue, list: EvaluatedValue) -> EngineResult<EvaluatedValue> {
    let values = match list {
        list if is_null(&list) => return Ok(EvaluatedValue::Truth(None)),
        EvaluatedValue::List(values) => values,
        _ => {
            return Err(EngineError::data(
                GqlStatus::INVALID_VALUE_TYPE,
                "the right side of IN must be a list".to_string(),
            ))
        }
    };
    if values.is_empty() {
        return Ok(EvaluatedValue::Truth(Some(false)));
    }
    if is_null(&probe) {
        return Ok(EvaluatedValue::Truth(None));
    }
    let mut saw_null = false;
    for candidate in values {
        match evaluate_equal(probe.clone(), candidate) {
            Some(true) => return Ok(EvaluatedValue::Truth(Some(true))),
            None => saw_null = true,
            Some(false) => {}
        }
    }
    Ok(EvaluatedValue::Truth(if saw_null { None } else { Some(false) }))
}

fn query_value_to_evaluated(value: QueryValue) -> EngineResult<EvaluatedValue> {
    match value {
        QueryValue::Null => Ok(EvaluatedValue::Null),
        QueryValue::VertexId(id) => Ok(EvaluatedValue::Vertex(id)),
        QueryValue::RelationshipId(id) => Ok(EvaluatedValue::Relationship(id)),
        QueryValue::Count(value) => Ok(EvaluatedValue::Scalar(ScalarValue::Integer(i128::from(value)))),
        QueryValue::Scalar(value) => Ok(EvaluatedValue::Scalar(value)),
        QueryValue::List(values) => Ok(EvaluatedValue::List(
            values.into_iter().map(query_value_to_evaluated).collect::<EngineResult<Vec<_>>>()?,
        )),
        QueryValue::Path(path) => Ok(EvaluatedValue::Path(path)),
        QueryValue::Record(fields) => Ok(EvaluatedValue::Record(
            fields
                .into_iter()
                .map(|(name, value)| Ok((name, query_value_to_evaluated(value)?)))
                .collect::<EngineResult<Vec<_>>>()?,
        )),
        QueryValue::Temporal(value) => Ok(EvaluatedValue::Temporal(value)),
    }
}

fn binding_vertex<'a>(row: &'a BindingRow, binding: &Symbol) -> EngineResult<&'a VertexRecord> {
    row.vertices
        .get(binding)
        .ok_or_else(|| unsupported(QueryFailureReason::Other, format!("unbound vertex variable {binding}")))
}

/// §20.11 General Rule 3: a record's field, null when it has none or the
/// record is null. Anything else has no fields.
fn record_field(value: EvaluatedValue, field: &Symbol, row: &BindingRow) -> EngineResult<EvaluatedValue> {
    // Legacy managed semantics for computed property access.
    if let EvaluatedValue::Vertex(id) = value {
        if field.as_str() == "id" {
            return Ok(EvaluatedValue::Vertex(id));
        }
    }
    stored_record_field(value, field, row)
}

fn stored_record_field(value: EvaluatedValue, field: &Symbol, row: &BindingRow) -> EngineResult<EvaluatedValue> {
    // A node or edge reached through a record (`{node: n}.node.name`) reads
    // its property from the record the row holds for it.
    let property =
        |properties: Option<&BTreeMap<String, ScalarValue>>| match properties.and_then(|properties| properties.get(field.as_str())) {
            None | Some(ScalarValue::Null) => EvaluatedValue::Null,
            Some(value) => evaluated_from_scalar(value),
        };
    match value {
        EvaluatedValue::Record(fields) => Ok(fields
            .into_iter()
            .find(|(name, _)| name.as_ref() == field.as_str())
            .map_or(EvaluatedValue::Null, |(_, value)| value)),
        EvaluatedValue::Vertex(id) => Ok(property(
            row_vertex_records(row)
                .find(|record| record.id == id)
                .map(|record| &record.properties),
        )),
        EvaluatedValue::Relationship(id) => Ok(property(
            row_relationship_records(row)
                .find(|record| record.id == id)
                .map(|record| &record.properties),
        )),
        value if is_null(&value) => Ok(EvaluatedValue::Null),
        _ => Err(EngineError::data(
            GqlStatus::INVALID_VALUE_TYPE,
            format!("only a node, an edge or a record has the field {field}"),
        )),
    }
}

/// §19.3's `=` of two values: `None` when it is unknown. Matched on the
/// left value's kind with no catch-all, so a new kind of value has to say
/// how it compares instead of comparing unequal unnoticed; values of two
/// kinds are unequal.
fn evaluate_equal(left: EvaluatedValue, right: EvaluatedValue) -> Option<bool> {
    use EvaluatedValue as V;
    if matches!(right, V::Null | V::Truth(None)) {
        return None;
    }
    match left {
        V::Null | V::Truth(None) => None,
        V::Vertex(left) => match right {
            V::Vertex(right) => Some(left == right),
            V::Scalar(ScalarValue::Integer(right)) => u64::try_from(right).ok().map(|right| left == right),
            _ => Some(false),
        },
        V::Relationship(left) => match right {
            V::Relationship(right) => Some(left == right),
            V::Scalar(ScalarValue::Integer(right)) => u64::try_from(right).ok().map(|right| left == right),
            _ => Some(false),
        },
        V::Scalar(left) => match (left, right) {
            (left, V::Scalar(right)) => Some(scalar_values_equal(&left, &right)),
            (ScalarValue::Integer(left), V::Vertex(right) | V::Relationship(right)) => u64::try_from(left).ok().map(|left| left == right),
            (ScalarValue::Boolean(left), V::Truth(Some(right))) => Some(left == right),
            _ => Some(false),
        },
        V::Truth(Some(left)) => match right {
            V::Truth(Some(right)) | V::Scalar(ScalarValue::Boolean(right)) => Some(left == right),
            _ => Some(false),
        },
        // Element-wise and three-valued: lists of different lengths differ,
        // a differing pair decides, and otherwise a null pair leaves it
        // unknown.
        V::List(left) => match right {
            V::List(right) => {
                if left.len() != right.len() {
                    return Some(false);
                }
                all_equal(left.into_iter().zip(right))
            }
            _ => Some(false),
        },
        // §19.3 General Rule 2 b) iv), GA09: equal path element lists.
        V::Path(left) => match right {
            V::Path(right) => Some(path_identities(&left) == path_identities(&right)),
            _ => Some(false),
        },
        // §19.3 General Rule 2 b) iii): records with the same field names,
        // field by field and three-valued.
        V::Record(left) => match right {
            V::Record(right) => {
                if !same_field_names(&left, &right) {
                    return Some(false);
                }
                all_equal(left.into_iter().zip(right).map(|((_, left), (_, right))| (left, right)))
            }
            _ => Some(false),
        },
        // §4.16.6: temporal values of one type, as the instants or amounts
        // they denote; of different types they are never equal.
        V::Temporal(left) => match right {
            V::Temporal(right) => Some(crate::temporal::compare(&left, &right) == Some(Ordering::Equal)),
            _ => Some(false),
        },
    }
}

/// Pairs equal one by one, three-valued: a differing pair decides, and
/// otherwise a null pair leaves it unknown.
fn all_equal(pairs: impl IntoIterator<Item = (EvaluatedValue, EvaluatedValue)>) -> Option<bool> {
    let mut unknown = false;
    for (left, right) in pairs {
        match evaluate_equal(left, right) {
            Some(false) => return Some(false),
            None => unknown = true,
            Some(true) => {}
        }
    }
    (!unknown).then_some(true)
}

fn same_field_names(left: &[(Box<str>, EvaluatedValue)], right: &[(Box<str>, EvaluatedValue)]) -> bool {
    left.len() == right.len() && left.iter().zip(right).all(|((left, _), (right, _))| left == right)
}

/// Pairs ordered one by one: the first that differs decides, an unknown
/// pair before it leaves it unknown, and `tie` when every pair is equal.
fn lexicographic(pairs: impl IntoIterator<Item = (EvaluatedValue, EvaluatedValue)>, tie: Ordering) -> Option<Ordering> {
    for (left, right) in pairs {
        match evaluate_ordering(left, right)? {
            Ordering::Equal => {}
            comparison => return Some(comparison),
        }
    }
    Some(tie)
}

/// §19.3's `<` of two values: `None` when it is unknown, or when the values
/// cannot be compared (deviation N1). Matched on the left value's kind with
/// no catch-all, so a new kind of value has to say how it orders instead of
/// comparing as unknown unnoticed.
fn evaluate_ordering(left: EvaluatedValue, right: EvaluatedValue) -> Option<Ordering> {
    use EvaluatedValue as V;
    if matches!(right, V::Null | V::Truth(None)) {
        return None;
    }
    match left {
        V::Null | V::Truth(None) => None,
        V::Vertex(left) => match right {
            V::Vertex(right) => Some(left.cmp(&right)),
            V::Scalar(ScalarValue::Integer(right)) => i128::from(left).partial_cmp(&right),
            _ => None,
        },
        V::Relationship(left) => match right {
            V::Relationship(right) => Some(left.cmp(&right)),
            V::Scalar(ScalarValue::Integer(right)) => i128::from(left).partial_cmp(&right),
            _ => None,
        },
        V::Scalar(left) => match (left, right) {
            (left, V::Scalar(right)) => Some(compare_property_values(&left, &right)),
            (ScalarValue::Integer(left), V::Vertex(right) | V::Relationship(right)) => left.partial_cmp(&i128::from(right)),
            (ScalarValue::Boolean(left), V::Truth(Some(right))) => Some(left.cmp(&right)),
            _ => None,
        },
        V::Truth(Some(left)) => match right {
            V::Truth(Some(right)) | V::Scalar(ScalarValue::Boolean(right)) => Some(left.cmp(&right)),
            _ => None,
        },
        V::List(left) => match right {
            V::List(right) => {
                let length = left.len().cmp(&right.len());
                lexicographic(left.into_iter().zip(right), length)
            }
            _ => None,
        },
        // §19.3 General Rule 2 b) iv): by their path element lists, as a
        // sort orders them.
        V::Path(left) => match right {
            V::Path(right) => Some(path_identities(&left).cmp(&path_identities(&right))),
            _ => None,
        },
        // §19.3 General Rule 2 b) iii) leaves `<` of records with the same
        // field names implementation-defined (IV002): their values in name
        // order, as a sort orders them. Other records cannot be compared.
        V::Record(left) => match right {
            V::Record(right) if same_field_names(&left, &right) => lexicographic(
                left.into_iter().zip(right).map(|((_, left), (_, right))| (left, right)),
                Ordering::Equal,
            ),
            _ => None,
        },
        V::Temporal(left) => match right {
            V::Temporal(right) => crate::temporal::compare(&left, &right),
            _ => None,
        },
    }
}

fn evaluate_string_predicate(
    operator: PhysicalBinaryOperator,
    left: EvaluatedValue,
    right: EvaluatedValue,
) -> EngineResult<EvaluatedValue> {
    if is_null(&left) || is_null(&right) {
        return Ok(EvaluatedValue::Truth(None));
    }
    match (left, right) {
        (EvaluatedValue::Scalar(ScalarValue::String(value)), EvaluatedValue::Scalar(ScalarValue::String(needle))) => {
            Ok(EvaluatedValue::Truth(Some(match operator {
                PhysicalBinaryOperator::StartsWith => value.starts_with(needle.as_ref()),
                PhysicalBinaryOperator::EndsWith => value.ends_with(needle.as_ref()),
                PhysicalBinaryOperator::Contains => value.contains(needle.as_ref()),
                _ => unreachable!("only string predicate operators call evaluate_string_predicate"),
            })))
        }
        _ => Err(EngineError::data(
            GqlStatus::INVALID_VALUE_TYPE,
            match operator {
                PhysicalBinaryOperator::StartsWith => "STARTS WITH operands must be strings",
                PhysicalBinaryOperator::EndsWith => "ENDS WITH operands must be strings",
                PhysicalBinaryOperator::Contains => "CONTAINS operands must be strings",
                _ => unreachable!("only string predicate operators call evaluate_string_predicate"),
            },
        )),
    }
}

/// Whether a predicate's value keeps a row: true keeps it, false and unknown
/// drop it. A boolean value counts the same as a truth value, so
/// `WHERE true` and a CASE choosing `false` filter as a comparison does.
fn predicate_holds(value: EvaluatedValue, refusal: &'static str) -> EngineResult<bool> {
    match value {
        EvaluatedValue::Truth(Some(holds)) | EvaluatedValue::Scalar(ScalarValue::Boolean(holds)) => Ok(holds),
        EvaluatedValue::Truth(None) | EvaluatedValue::Null | EvaluatedValue::Scalar(ScalarValue::Null) => Ok(false),
        _ => Err(EngineError::data(GqlStatus::INVALID_VALUE_TYPE, refusal)),
    }
}

fn is_null(value: &EvaluatedValue) -> bool {
    matches!(
        value,
        EvaluatedValue::Null | EvaluatedValue::Scalar(ScalarValue::Null) | EvaluatedValue::Truth(None)
    )
}

/// `value` as a truth value; `22G03` naming `operand` (what the value is to
/// its operator) for anything else.
fn as_truth(value: EvaluatedValue, operand: &str) -> EngineResult<Option<bool>> {
    match value {
        EvaluatedValue::Truth(value) => Ok(value),
        EvaluatedValue::Scalar(ScalarValue::Boolean(value)) => Ok(Some(value)),
        EvaluatedValue::Null => Ok(None),
        _ => Err(EngineError::data(
            GqlStatus::INVALID_VALUE_TYPE,
            format!("{operand} must be boolean"),
        )),
    }
}

fn evaluate_or(left: Option<bool>, right: Option<bool>) -> Option<bool> {
    match (left, right) {
        (Some(true), _) | (_, Some(true)) => Some(true),
        (Some(false), Some(false)) => Some(false),
        _ => None,
    }
}

fn evaluate_and(left: Option<bool>, right: Option<bool>) -> Option<bool> {
    match (left, right) {
        (Some(false), _) | (_, Some(false)) => Some(false),
        (Some(true), Some(true)) => Some(true),
        _ => None,
    }
}

/// A value a projection bound, read back as an evaluable value.
fn evaluated_from_query_value(value: &QueryValue) -> EvaluatedValue {
    match value {
        QueryValue::Null => EvaluatedValue::Null,
        QueryValue::VertexId(id) => EvaluatedValue::Vertex(*id),
        QueryValue::RelationshipId(id) => EvaluatedValue::Relationship(*id),
        QueryValue::Count(count) => EvaluatedValue::Scalar(ScalarValue::Integer(i128::from(*count))),
        QueryValue::Scalar(scalar) => evaluated_from_scalar(scalar),
        QueryValue::List(values) => EvaluatedValue::List(values.iter().map(evaluated_from_query_value).collect()),
        QueryValue::Path(path) => EvaluatedValue::Path(path.clone()),
        QueryValue::Record(fields) => EvaluatedValue::Record(
            fields
                .iter()
                .map(|(name, value)| (name.clone(), evaluated_from_query_value(value)))
                .collect(),
        ),
        QueryValue::Temporal(value) => EvaluatedValue::Temporal(*value),
    }
}

fn evaluated_from_scalar(value: &ScalarValue) -> EvaluatedValue {
    match value {
        ScalarValue::Null => EvaluatedValue::Null,
        ScalarValue::List(values) => EvaluatedValue::List(values.iter().map(evaluated_from_scalar).collect()),
        ScalarValue::Date(days_since_epoch) => {
            use chrono::{Days, NaiveDate};

            let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).expect("Unix epoch is valid");
            let date = if *days_since_epoch >= 0 {
                epoch.checked_add_days(Days::new(*days_since_epoch as u64))
            } else {
                epoch.checked_sub_days(Days::new(days_since_epoch.unsigned_abs() as u64))
            };
            date.map_or_else(
                || EvaluatedValue::Scalar(value.clone()),
                |date| EvaluatedValue::Temporal(TemporalValue::Date(date)),
            )
        }
        value => EvaluatedValue::Scalar(value.clone()),
    }
}

fn to_query_value(value: EvaluatedValue) -> QueryValue {
    match value {
        EvaluatedValue::Null | EvaluatedValue::Truth(None) => QueryValue::Null,
        EvaluatedValue::Vertex(id) => QueryValue::VertexId(id),
        EvaluatedValue::Relationship(id) => QueryValue::RelationshipId(id),
        EvaluatedValue::Scalar(value) => QueryValue::Scalar(value),
        EvaluatedValue::Truth(Some(value)) => QueryValue::Scalar(ScalarValue::Boolean(value)),
        EvaluatedValue::List(values) => QueryValue::List(values.into_iter().map(to_query_value).collect()),
        EvaluatedValue::Path(path) => QueryValue::Path(path),
        EvaluatedValue::Record(fields) => {
            QueryValue::Record(fields.into_iter().map(|(name, value)| (name, to_query_value(value))).collect())
        }
        EvaluatedValue::Temporal(value) => QueryValue::Temporal(value),
    }
}

fn bound_string(value: &BoundValue, role: &'static str) -> EngineResult<String> {
    match &value.value {
        ScalarValue::String(value) => Ok(value.to_string()),
        _ => Err(EngineError::data(
            GqlStatus::INVALID_VALUE_TYPE,
            format!("ordered scan {role} must be a string"),
        )),
    }
}

fn seek_value(value: &BoundValue) -> EngineResult<&ScalarValue> {
    if matches!(value.value, ScalarValue::Null) {
        return Err(EngineError::data(
            GqlStatus::NULL_VALUE_NOT_ALLOWED,
            "NULL cannot be a property seek key",
        ));
    }
    Ok(&value.value)
}

fn vertex_id_value(value: &BoundValue) -> EngineResult<VertexId> {
    let ScalarValue::Integer(value) = value.value else {
        return Err(EngineError::data(GqlStatus::INVALID_VALUE_TYPE, "vertex id must be an integer"));
    };
    VertexId::try_from(value).map_err(|_| EngineError::data(GqlStatus::INVALID_VALUE_TYPE, "vertex id must be non-negative and fit u64"))
}

fn window_count(value: &BoundValue, field: &'static str) -> EngineResult<usize> {
    let ScalarValue::Integer(value) = value.value else {
        return Err(unsupported(
            QueryFailureReason::OrderWindow,
            format!("{field} count is not an integer"),
        ));
    };
    usize::try_from(value).map_err(|_| unsupported(QueryFailureReason::OrderWindow, format!("{field} count is out of range")))
}

fn unsupported(bucket: QueryFailureReason, reason: impl Into<String>) -> EngineError {
    EngineError::UnsupportedPlan(bucket, reason.into())
}
