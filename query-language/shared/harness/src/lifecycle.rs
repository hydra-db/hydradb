//! Lifecycle validation and adapter boundary. Infrastructure errors are not query errors.
use crate::{steps::Kind, values::Value};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::future::Future;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Diagnostic {
    pub phase: String,
    pub error_type: String,
    pub detail: String,
    pub stage: String,
    pub native_code: Option<String>,
    pub native_message: String,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum QueryOutcome {
    Rows { columns: Vec<String>, rows: Vec<Vec<Value>> },
    Error(Diagnostic),
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct QueryObservation {
    pub engine: String,
    pub execution_path: String,
    pub source_line: usize,
    pub parser: String,
    pub semantics: String,
    pub outcome: QueryOutcome,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum AdapterFailure {
    Unsupported(String),
    Fixture(String),
    Setup(String),
    Control(String),
    Infrastructure(String),
    Timeout,
    Cancelled,
}

/// One adapter session is owned by one case and must not be shared between engines.
#[async_trait]
pub trait Adapter {
    type Session: Send;
    async fn create(&self) -> Result<Self::Session, AdapterFailure>;
    async fn fixture(&self, session: &mut Self::Session, fixture: &Kind) -> Result<(), AdapterFailure>;
    async fn setup(&self, session: &mut Self::Session, query: &str) -> Result<(), AdapterFailure>;
    async fn procedure(
        &self,
        _session: &mut Self::Session,
        _signature: &str,
        _columns: &[String],
        _rows: &[Vec<Value>],
    ) -> Result<(), AdapterFailure> {
        Err(AdapterFailure::Unsupported("procedure fixture".into()))
    }
    async fn parameters(&self, session: &mut Self::Session, values: &BTreeMap<String, Value>) -> Result<(), AdapterFailure>;
    async fn syntax(&self, session: &mut Self::Session, query: &str) -> Result<(), Diagnostic>;
    async fn prepare(&self, session: &mut Self::Session, query: &str) -> Result<(), Diagnostic>;
    /// Must consume all result rows and expose deferred errors before returning.
    async fn execute(&self, session: &mut Self::Session, query: &str, source_line: usize) -> Result<QueryObservation, AdapterFailure>;
    async fn observe_graph(&self, session: &mut Self::Session) -> Result<GraphState, AdapterFailure>;
    async fn close(&self, session: Self::Session) -> Result<(), AdapterFailure>;
}

/// Identities are scoped to a session; labels identify node-label assignments.
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct GraphState {
    pub nodes: std::collections::BTreeSet<String>,
    pub relationships: std::collections::BTreeSet<String>,
    pub labels: std::collections::BTreeSet<(String, String)>,
    pub properties: BTreeMap<(String, String), Value>,
}
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct Effects {
    pub nodes_added: usize,
    pub nodes_removed: usize,
    pub relationships_added: usize,
    pub relationships_removed: usize,
    pub labels_added: usize,
    pub labels_removed: usize,
    pub properties_added: usize,
    pub properties_removed: usize,
}
pub fn effects(before: &GraphState, after: &GraphState) -> Effects {
    Effects {
        nodes_added: after.nodes.difference(&before.nodes).count(),
        nodes_removed: before.nodes.difference(&after.nodes).count(),
        relationships_added: after.relationships.difference(&before.relationships).count(),
        relationships_removed: before.relationships.difference(&after.relationships).count(),
        labels_added: after.labels.difference(&before.labels).count(),
        labels_removed: before.labels.difference(&after.labels).count(),
        properties_added: after
            .properties
            .iter()
            .filter(|(k, v)| {
                before
                    .properties
                    .get(*k)
                    .is_none_or(|old| !crate::compare::value_eq(old, v, crate::compare::Order::Ordered))
            })
            .count(),
        properties_removed: before
            .properties
            .iter()
            .filter(|(k, v)| {
                after
                    .properties
                    .get(*k)
                    .is_none_or(|new| !crate::compare::value_eq(v, new, crate::compare::Order::Ordered))
            })
            .count(),
    }
}

/// Reject invalid sequencing before any adapter operation is attempted.
pub fn validate(steps: &[Kind]) -> Result<(), String> {
    let mut fixture = false;
    let mut executed = false;
    let mut pending = false;
    for step in steps {
        match step {
            Kind::EmptyGraph | Kind::AnyGraph | Kind::NamedGraph(_) if !fixture && !executed => fixture = true,
            Kind::SetupQuery | Kind::Parameters | Kind::Procedure(_) if fixture && !executed => {}
            Kind::Query | Kind::ControlQuery if fixture && !pending => {
                executed = true;
                pending = true;
            }
            Kind::Results { .. } | Kind::EmptyResult | Kind::Error { .. } if pending => {
                pending = false;
            }
            Kind::SideEffects | Kind::NoSideEffects if executed && !pending => {}
            _ => return Err(format!("invalid lifecycle transition at {step:?}")),
        }
    }
    if !fixture || !executed || pending {
        return Err("incomplete scenario lifecycle".into());
    }
    Ok(())
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExecutionRecord {
    pub source_line: usize,
    pub control: bool,
    pub syntax: Result<(), Diagnostic>,
    pub preparation: Option<Result<(), Diagnostic>>,
    pub observation: Option<QueryObservation>,
    pub before: GraphState,
    pub after: GraphState,
}
impl ExecutionRecord {
    /// Return the first diagnostic observed along the production query path.
    pub fn diagnostic(&self) -> Option<&Diagnostic> {
        self.syntax
            .as_ref()
            .err()
            .or_else(|| self.preparation.as_ref().and_then(|result| result.as_ref().err()))
            .or_else(|| {
                self.observation.as_ref().and_then(|observation| match &observation.outcome {
                    QueryOutcome::Error(error) => Some(error),
                    _ => None,
                })
            })
    }
}
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RunResult {
    pub executions: Vec<ExecutionRecord>,
    pub mismatches: Vec<String>,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RunFailure {
    pub failure: AdapterFailure,
    pub cleanup: Option<AdapterFailure>,
    pub partial: RunResult,
}

/// Every adapter operation has completed (including deferred row draining) before cleanup.
/// The caller must not cancel/drop this future while it is running: async Rust cannot
/// await `close` from Drop. Timeouts should be returned by the adapter itself.
pub async fn run<A: Adapter + Sync>(adapter: &A, steps: &[crate::steps::ExecutableStep]) -> Result<RunResult, RunFailure> {
    use crate::steps::Action;
    let kinds = steps
        .iter()
        .map(|s| match &s.action {
            Action::Fixture(k) => k.clone(),
            Action::Setup(_) => Kind::SetupQuery,
            Action::Parameters(_) => Kind::Parameters,
            Action::Procedure { .. } => Kind::Procedure(String::new()),
            Action::Execute { control: false, .. } => Kind::Query,
            Action::Execute { control: true, .. } => Kind::ControlQuery,
            Action::Rows {
                ordered,
                list_order_ignored,
                ..
            } => Kind::Results {
                ordered: *ordered,
                list_order_ignored: *list_order_ignored,
            },
            Action::Empty => Kind::EmptyResult,
            Action::Error { error_type, phase, detail } => Kind::Error {
                error_type: error_type.clone(),
                phase: phase.clone(),
                detail: detail.clone(),
            },
            Action::Effects(_) => Kind::SideEffects,
        })
        .collect::<Vec<_>>();
    let mut partial = RunResult::default();
    if let Err(e) = validate(&kinds) {
        return Err(RunFailure {
            failure: AdapterFailure::Infrastructure(e),
            cleanup: None,
            partial,
        });
    }
    let mut session = adapter.create().await.map_err(|failure| RunFailure {
        failure,
        cleanup: None,
        partial: partial.clone(),
    })?;
    let inner = run_steps(adapter, &mut session, steps, &mut partial);
    // Catch panics when polling the adapter so that close is still awaited.
    let mut inner = Box::pin(inner);
    let outcome = std::future::poll_fn(|cx| {
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| inner.as_mut().poll(cx)))
            .unwrap_or_else(|_| std::task::Poll::Ready(Err(AdapterFailure::Infrastructure("adapter panicked".into()))))
    })
    .await;
    drop(inner);
    let cleanup = adapter.close(session).await.err();
    match (outcome, cleanup) {
        (Ok(()), None) => Ok(partial),
        (Err(failure), cleanup) => Err(RunFailure { failure, cleanup, partial }),
        (Ok(()), Some(failure)) => Err(RunFailure {
            failure,
            cleanup: None,
            partial,
        }),
    }
}

async fn run_steps<A: Adapter + Sync>(
    adapter: &A,
    session: &mut A::Session,
    steps: &[crate::steps::ExecutableStep],
    output: &mut RunResult,
) -> Result<(), AdapterFailure> {
    use crate::{
        compare::{rows_equal, Order},
        steps::Action,
    };
    for step in steps {
        match &step.action {
            Action::Fixture(f) => adapter.fixture(session, f).await?,
            Action::Setup(query) => adapter.setup(session, query).await?,
            Action::Parameters(p) => adapter.parameters(session, p).await?,
            Action::Procedure { signature, columns, rows } => adapter.procedure(session, signature, columns, rows).await?,
            Action::Execute { query, control } => {
                let before = adapter.observe_graph(session).await?;
                let syntax = adapter.syntax(session, query).await;
                let preparation = if syntax.is_ok() {
                    Some(adapter.prepare(session, query).await)
                } else {
                    None
                };
                let observation = if matches!(preparation, Some(Ok(()))) {
                    Some(adapter.execute(session, query, step.line).await?)
                } else {
                    None
                };
                let after = adapter.observe_graph(session).await?;
                output.executions.push(ExecutionRecord {
                    source_line: step.line,
                    control: *control,
                    syntax,
                    preparation,
                    observation,
                    before,
                    after,
                });
            }
            Action::Rows {
                columns,
                rows,
                ordered,
                list_order_ignored,
            } => {
                let last = output
                    .executions
                    .last()
                    .ok_or_else(|| AdapterFailure::Infrastructure("assertion without query".into()))?;
                let equal = match &last.observation {
                    Some(QueryObservation {
                        outcome:
                            QueryOutcome::Rows {
                                columns: actual_columns,
                                rows: actual,
                            },
                        ..
                    }) => rows_equal(
                        columns,
                        rows,
                        actual_columns,
                        actual,
                        if *ordered { Order::Ordered } else { Order::Unordered },
                        if *list_order_ignored { Order::Unordered } else { Order::Ordered },
                    ),
                    _ => false,
                };
                if !equal {
                    output.mismatches.push(format!("line {}: rows differ or query failed", step.line));
                }
            }
            Action::Empty => {
                let last = output
                    .executions
                    .last()
                    .ok_or_else(|| AdapterFailure::Infrastructure("assertion without query".into()))?;
                if !matches!(&last.observation, Some(QueryObservation { outcome: QueryOutcome::Rows { rows, .. }, .. }) if rows.is_empty())
                {
                    output.mismatches.push(format!("line {}: expected empty result", step.line));
                }
            }
            Action::Error { error_type, phase, detail } => {
                let last = output
                    .executions
                    .last()
                    .ok_or_else(|| AdapterFailure::Infrastructure("assertion without query".into()))?;
                if !last.diagnostic().is_some_and(|e| {
                    (phase == "any time" || &e.phase == phase) && &e.error_type == error_type && (detail == "*" || &e.detail == detail)
                }) {
                    output.mismatches.push(format!("line {}: diagnostic differs", step.line));
                }
                if effects(&last.before, &last.after) != Effects::default() {
                    output.mismatches.push(format!("line {}: error committed graph changes", step.line));
                }
            }
            Action::Effects(expected) => {
                let last = output
                    .executions
                    .last()
                    .ok_or_else(|| AdapterFailure::Infrastructure("effects without query".into()))?;
                if &effects(&last.before, &last.after) != expected {
                    output.mismatches.push(format!("line {}: side effects differ", step.line));
                }
            }
        }
    }
    Ok(())
}
