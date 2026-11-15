//! Strict recognition of the step vocabulary at the pinned corpus revision.
use crate::importer::ExpandedStep;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Kind {
    EmptyGraph,
    AnyGraph,
    NamedGraph(String),
    SetupQuery,
    Query,
    ControlQuery,
    Parameters,
    Procedure(String),
    Results { ordered: bool, list_order_ignored: bool },
    EmptyResult,
    Error { error_type: String, phase: String, detail: String },
    SideEffects,
    NoSideEffects,
}

pub fn bind(step: &ExpandedStep) -> Result<Kind, String> {
    let s = step.text.trim();
    let kind = match s {
        "an empty graph" => Kind::EmptyGraph,
        "any graph" => Kind::AnyGraph,
        "having executed:" => Kind::SetupQuery,
        "executing query:" => Kind::Query,
        "executing control query:" => Kind::ControlQuery,
        "parameters are:" => Kind::Parameters,
        "the result should be empty" => Kind::EmptyResult,
        "the result should be, in any order:" => Kind::Results {
            ordered: false,
            list_order_ignored: false,
        },
        "the result should be, in order:" => Kind::Results {
            ordered: true,
            list_order_ignored: false,
        },
        "the result should be (ignoring element order for lists):" => Kind::Results {
            ordered: false,
            list_order_ignored: true,
        },
        "the result should be, in order (ignoring element order for lists):" => Kind::Results {
            ordered: true,
            list_order_ignored: true,
        },
        "the side effects should be:" => Kind::SideEffects,
        "no side effects" => Kind::NoSideEffects,
        _ if s.starts_with("the ") && s.ends_with(" graph") => {
            let name = &s[4..s.len() - 6];
            if name.is_empty() {
                return Err("empty graph name".into());
            }
            Kind::NamedGraph(name.into())
        }
        _ if s.starts_with("there exists a procedure ") && s.ends_with(':') => Kind::Procedure(s[25..s.len() - 1].trim().into()),
        _ if s.starts_with("a ") || s.starts_with("an ") => {
            let rest = s.strip_prefix("a ").or_else(|| s.strip_prefix("an ")).unwrap();
            let (error_type, rest) = rest
                .split_once(" should be raised at ")
                .ok_or_else(|| format!("unknown step: {s}"))?;
            let (phase, detail) = rest.split_once(": ").ok_or_else(|| format!("malformed error: {s}"))?;
            if error_type.is_empty() || detail.is_empty() || !matches!(phase, "compile time" | "runtime" | "any time") {
                return Err(format!("malformed error: {s}"));
            }
            Kind::Error {
                error_type: error_type.into(),
                phase: phase.into(),
                detail: detail.into(),
            }
        }
        _ => return Err(format!("unknown step: {s}")),
    };
    let requires_doc = matches!(kind, Kind::SetupQuery | Kind::Query | Kind::ControlQuery);
    let requires_table = matches!(
        kind,
        Kind::Parameters | Kind::Procedure(_) | Kind::Results { .. } | Kind::SideEffects
    );
    if requires_doc && (step.docstring.is_none() || step.table.is_some()) {
        return Err(format!("expected query doc string: {s}"));
    }
    if requires_table && (step.table.is_none() || step.docstring.is_some()) {
        return Err(format!("expected data table: {s}"));
    }
    if !requires_doc && !requires_table && (step.docstring.is_some() || step.table.is_some()) {
        return Err(format!("unexpected argument: {s}"));
    }
    Ok(kind)
}

use crate::{
    lifecycle::Effects,
    values::{self, Value},
};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExecutableStep {
    pub line: usize,
    pub action: Action,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Action {
    Fixture(Kind),
    Setup(String),
    Parameters(BTreeMap<String, Value>),
    Procedure {
        signature: String,
        columns: Vec<String>,
        rows: Vec<Vec<Value>>,
    },
    Execute {
        control: bool,
        query: String,
    },
    Rows {
        columns: Vec<String>,
        rows: Vec<Vec<Value>>,
        ordered: bool,
        list_order_ignored: bool,
    },
    Empty,
    Error {
        error_type: String,
        phase: String,
        detail: String,
    },
    Effects(Effects),
}
fn typed_rows(rows: &[Vec<String>]) -> Result<Vec<Vec<Value>>, String> {
    rows.iter()
        .map(|r| r.iter().map(|v| values::parse(v).map_err(|e| format!("{v:?}: {e}"))).collect())
        .collect()
}
pub fn bind_executable(step: &ExpandedStep) -> Result<ExecutableStep, String> {
    let kind = bind(step)?;
    let table = || step.table.as_ref().ok_or_else(|| "missing table".to_string());
    let action = match kind {
        k @ (Kind::EmptyGraph | Kind::AnyGraph | Kind::NamedGraph(_)) => Action::Fixture(k),
        Kind::SetupQuery => Action::Setup(step.docstring.clone().unwrap()),
        Kind::Query | Kind::ControlQuery => Action::Execute {
            control: matches!(kind, Kind::ControlQuery),
            query: step.docstring.clone().unwrap(),
        },
        Kind::Parameters => {
            let mut params = BTreeMap::new();
            for row in table()? {
                if row.len() != 2 || row[0].is_empty() {
                    return Err("invalid parameter row".into());
                }
                if params.insert(row[0].clone(), values::parse(&row[1])?).is_some() {
                    return Err(format!("duplicate parameter {}", row[0]));
                }
            }
            Action::Parameters(params)
        }
        Kind::Procedure(signature) => {
            let rows = table()?;
            let columns = rows.first().ok_or("missing procedure header")?.clone();
            Action::Procedure {
                signature,
                columns,
                rows: typed_rows(&rows[1..])?,
            }
        }
        Kind::Results {
            ordered,
            list_order_ignored,
        } => {
            let table = table()?;
            let columns = table.first().ok_or("missing result header")?.clone();
            if columns.iter().any(|c| c.is_empty())
                || columns.iter().collect::<BTreeSet<_>>().len() != columns.len()
                || table.iter().any(|r| r.len() != columns.len())
            {
                return Err("invalid result columns or row widths".into());
            }
            Action::Rows {
                columns,
                rows: typed_rows(&table[1..])?,
                ordered,
                list_order_ignored,
            }
        }
        Kind::EmptyResult => Action::Empty,
        Kind::Error { error_type, phase, detail } => Action::Error { error_type, phase, detail },
        Kind::NoSideEffects => Action::Effects(Effects::default()),
        Kind::SideEffects => {
            let mut effects = Effects::default();
            let mut seen = BTreeSet::new();
            for row in table()? {
                if row.len() != 2 || !seen.insert(row[0].clone()) {
                    return Err("invalid or duplicate side effect".into());
                }
                let value = row[1].parse::<usize>().map_err(|_| "invalid side effect count")?;
                let target = match row[0].as_str() {
                    "+nodes" => &mut effects.nodes_added,
                    "-nodes" => &mut effects.nodes_removed,
                    "+relationships" => &mut effects.relationships_added,
                    "-relationships" => &mut effects.relationships_removed,
                    "+labels" => &mut effects.labels_added,
                    "-labels" => &mut effects.labels_removed,
                    "+properties" => &mut effects.properties_added,
                    "-properties" => &mut effects.properties_removed,
                    _ => return Err(format!("unknown side effect {}", row[0])),
                };
                *target = value;
            }
            Action::Effects(effects)
        }
    };
    Ok(ExecutableStep { line: step.line, action })
}

/// Bind and validate a complete scenario before creating any engine state.
pub fn bind_scenario(case: &crate::importer::ExpandedCase) -> Result<Vec<ExecutableStep>, String> {
    let mut seen = BTreeSet::new();
    let mut kinds = Vec::new();
    let mut bound = Vec::new();
    for step in &case.steps {
        let kind = bind(step).map_err(|e| format!("line {}: {e}", step.line))?;
        let executable = bind_executable(step).map_err(|e| format!("line {}: {e}", step.line))?;
        if let Action::Parameters(params) = &executable.action {
            for name in params.keys() {
                if !seen.insert(name.clone()) {
                    return Err(format!("line {}: duplicate parameter {name}", step.line));
                }
            }
        }
        kinds.push(kind);
        bound.push(executable);
    }
    crate::lifecycle::validate(&kinds)?;
    Ok(bound)
}
