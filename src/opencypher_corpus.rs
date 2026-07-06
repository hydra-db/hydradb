use crate::{
    GraphError, QueryColumn, QueryFloat, QueryResultSet, QueryRow, QueryValue, Result,
    VertexPropertyValue,
};
use std::path::{Path, PathBuf};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CypherTckCorpus {
    pub cases: Vec<CypherTckCase>,
    pub skipped: Vec<String>,
    pub total_scenarios: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CypherTckCase {
    pub name: String,
    pub query: String,
    pub expected: QueryResultSet,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct CypherTckCompatibilityReport {
    pub total_scenarios: usize,
    pub runnable_scenarios: usize,
    pub skipped_scenarios: usize,
    pub skipped: Vec<String>,
}

impl CypherTckCorpus {
    pub fn compatibility_report(&self) -> CypherTckCompatibilityReport {
        CypherTckCompatibilityReport {
            total_scenarios: self.total_scenarios,
            runnable_scenarios: self.cases.len(),
            skipped_scenarios: self.skipped.len(),
            skipped: self.skipped.clone(),
        }
    }
}

pub fn parse_opencypher_tck_corpus(input: &str) -> Result<CypherTckCorpus> {
    let mut parser = TckCorpusParser::new(input);
    parser.parse()
}

pub fn parse_opencypher_tck_corpus_dir(root: impl AsRef<Path>) -> Result<CypherTckCorpus> {
    let root = root.as_ref();
    let mut files = Vec::new();
    collect_feature_files(root, &mut files)?;
    files.sort();

    let mut cases = Vec::new();
    let mut skipped = Vec::new();
    let mut total_scenarios = 0;
    for file in files {
        let input = std::fs::read_to_string(&file).map_err(|err| GraphError::CorruptValue {
            key: file.display().to_string(),
            reason: err.to_string(),
        })?;
        let corpus = parse_opencypher_tck_corpus(&input)?;
        total_scenarios += corpus.total_scenarios;
        let relative = file
            .strip_prefix(root)
            .unwrap_or(&file)
            .display()
            .to_string();
        cases.extend(corpus.cases.into_iter().map(|mut case| {
            case.name = format!("{relative}: {}", case.name);
            case
        }));
        skipped.extend(
            corpus
                .skipped
                .into_iter()
                .map(|reason| format!("{relative}: {reason}")),
        );
    }

    Ok(CypherTckCorpus {
        cases,
        skipped,
        total_scenarios,
    })
}

fn collect_feature_files(root: &Path, files: &mut Vec<PathBuf>) -> Result<()> {
    let entries = std::fs::read_dir(root).map_err(|err| GraphError::CorruptValue {
        key: root.display().to_string(),
        reason: err.to_string(),
    })?;
    for entry in entries {
        let entry = entry.map_err(|err| GraphError::CorruptValue {
            key: root.display().to_string(),
            reason: err.to_string(),
        })?;
        let path = entry.path();
        if path.is_dir() {
            collect_feature_files(&path, files)?;
        } else if path.extension().and_then(|ext| ext.to_str()) == Some("feature") {
            files.push(path);
        }
    }
    Ok(())
}

struct TckCorpusParser<'a> {
    lines: Vec<&'a str>,
    idx: usize,
    cases: Vec<CypherTckCase>,
    skipped: Vec<String>,
    total_scenarios: usize,
}

impl<'a> TckCorpusParser<'a> {
    fn new(input: &'a str) -> Self {
        Self {
            lines: input.lines().collect(),
            idx: 0,
            cases: Vec::new(),
            skipped: Vec::new(),
            total_scenarios: 0,
        }
    }

    fn parse(&mut self) -> Result<CypherTckCorpus> {
        while self.idx < self.lines.len() {
            let line = self.trimmed();
            if let Some(name) = scenario_name(line) {
                self.total_scenarios += 1;
                self.idx += 1;
                self.parse_scenario(name)?;
            } else {
                self.idx += 1;
            }
        }
        Ok(CypherTckCorpus {
            cases: std::mem::take(&mut self.cases),
            skipped: std::mem::take(&mut self.skipped),
            total_scenarios: self.total_scenarios,
        })
    }

    fn parse_scenario(&mut self, name: String) -> Result<()> {
        let mut query = None;
        let mut expected = None;
        let mut unsupported = None;

        while self.idx < self.lines.len() {
            let line = self.trimmed();
            if scenario_name(line).is_some() {
                break;
            }
            if line.starts_with("Given ")
                || line.starts_with("And having executed")
                || line.starts_with("And after")
                || line.starts_with("When executing control query")
            {
                unsupported = Some(format!(
                    "{name}: setup/control clauses require an external TCK fixture runner"
                ));
            }
            if line.starts_with("When executing query:") {
                self.idx += 1;
                query = Some(self.read_doc_string()?);
                continue;
            }
            if line.starts_with("Then the result should be") {
                self.idx += 1;
                expected = Some(self.read_result_table(&name)?);
                continue;
            }
            if line.starts_with("Then no side effects")
                || line.starts_with("Then the side effects should be")
            {
                unsupported = Some(format!(
                    "{name}: side-effect assertions are not row-query corpus cases"
                ));
            }
            self.idx += 1;
        }

        match (unsupported, query, expected) {
            (Some(reason), _, _) => self.skipped.push(reason),
            (None, Some(query), Some(expected)) => {
                self.cases.push(CypherTckCase {
                    name,
                    query,
                    expected,
                });
            }
            (None, None, _) => self.skipped.push(format!("{name}: missing query block")),
            (None, _, None) => self.skipped.push(format!("{name}: missing result table")),
        }
        Ok(())
    }

    fn read_doc_string(&mut self) -> Result<String> {
        self.skip_blank_lines();
        if self.idx >= self.lines.len() || self.trimmed() != "\"\"\"" {
            return Err(GraphError::QueryParse {
                dialect: "OpenCypherTCK",
                reason: "expected triple-quoted query block".to_string(),
            });
        }
        self.idx += 1;
        let mut query = String::new();
        while self.idx < self.lines.len() {
            let line = self.lines[self.idx];
            if line.trim() == "\"\"\"" {
                self.idx += 1;
                return Ok(query.trim().to_string());
            }
            if !query.is_empty() {
                query.push('\n');
            }
            query.push_str(line.trim());
            self.idx += 1;
        }
        Err(GraphError::QueryParse {
            dialect: "OpenCypherTCK",
            reason: "unterminated triple-quoted query block".to_string(),
        })
    }

    fn read_result_table(&mut self, scenario: &str) -> Result<QueryResultSet> {
        self.skip_blank_lines();
        let mut rows = Vec::new();
        while self.idx < self.lines.len() {
            let line = self.trimmed();
            if line.is_empty() {
                self.idx += 1;
                continue;
            }
            if !line.starts_with('|') {
                break;
            }
            rows.push(parse_table_row(line)?);
            self.idx += 1;
        }
        if rows.is_empty() {
            return Err(GraphError::QueryParse {
                dialect: "OpenCypherTCK",
                reason: format!("{scenario}: expected result table"),
            });
        }
        let columns = rows
            .remove(0)
            .into_iter()
            .map(QueryColumn::new)
            .collect::<Vec<_>>();
        let query_rows = rows
            .into_iter()
            .map(|row| {
                if row.len() != columns.len() {
                    return Err(GraphError::QueryParse {
                        dialect: "OpenCypherTCK",
                        reason: format!(
                            "{scenario}: result row has {} cells, expected {}",
                            row.len(),
                            columns.len()
                        ),
                    });
                }
                let values = row
                    .iter()
                    .zip(columns.iter())
                    .map(|(cell, column)| parse_expected_value(column, cell))
                    .collect::<Result<_>>()?;
                Ok(QueryRow::new(values))
            })
            .collect::<Result<_>>()?;
        Ok(QueryResultSet::new(columns, query_rows))
    }

    fn skip_blank_lines(&mut self) {
        while self.idx < self.lines.len() && self.lines[self.idx].trim().is_empty() {
            self.idx += 1;
        }
    }

    fn trimmed(&self) -> &'a str {
        self.lines[self.idx].trim()
    }
}

fn scenario_name(line: &str) -> Option<String> {
    for prefix in ["Scenario:", "Scenario Outline:"] {
        if let Some(name) = line.strip_prefix(prefix) {
            return Some(name.trim().to_string());
        }
    }
    None
}

fn parse_table_row(line: &str) -> Result<Vec<String>> {
    if !line.starts_with('|') || !line.ends_with('|') {
        return Err(GraphError::QueryParse {
            dialect: "OpenCypherTCK",
            reason: format!("invalid table row: {line}"),
        });
    }
    Ok(line
        .trim_matches('|')
        .split('|')
        .map(|cell| cell.trim().to_string())
        .collect())
}

fn parse_expected_value(column: &QueryColumn, cell: &str) -> Result<QueryValue> {
    let cell = cell.trim();
    if cell.eq_ignore_ascii_case("null") || cell == "<null>" {
        return Ok(QueryValue::Null);
    }
    if cell.eq_ignore_ascii_case("true") {
        return Ok(QueryValue::Property(VertexPropertyValue::Bool(true)));
    }
    if cell.eq_ignore_ascii_case("false") {
        return Ok(QueryValue::Property(VertexPropertyValue::Bool(false)));
    }
    if let Some(string) = quoted_string(cell) {
        return Ok(QueryValue::Property(VertexPropertyValue::String(
            string.to_string(),
        )));
    }
    if let Some(list) = parse_expected_list(column, cell)? {
        return Ok(QueryValue::List(list));
    }
    if let Ok(value) = cell.parse::<u64>() {
        if looks_like_vertex_id_column(&column.name) {
            return Ok(QueryValue::VertexId(value));
        }
        if column.name.starts_with("count(") || column.name == "count" || column.name == "total" {
            return Ok(QueryValue::Count(value));
        }
        return Ok(QueryValue::Property(VertexPropertyValue::Integer(value)));
    }
    if let Ok(value) = cell.parse::<f64>() {
        return Ok(QueryValue::Float(QueryFloat(value)));
    }
    Ok(QueryValue::Property(VertexPropertyValue::String(
        cell.to_string(),
    )))
}

fn looks_like_vertex_id_column(column: &str) -> bool {
    column.ends_with(".id")
        || column == "id"
        || column.ends_with("_id")
        || matches!(
            column,
            "vertex" | "node" | "src" | "dst" | "source" | "target" | "user" | "post" | "followed"
        )
}

fn quoted_string(value: &str) -> Option<&str> {
    value
        .strip_prefix('\'')
        .and_then(|value| value.strip_suffix('\''))
        .or_else(|| {
            value
                .strip_prefix('"')
                .and_then(|value| value.strip_suffix('"'))
        })
}

fn parse_expected_list(column: &QueryColumn, cell: &str) -> Result<Option<Vec<QueryValue>>> {
    let Some(inner) = cell
        .strip_prefix('[')
        .and_then(|cell| cell.strip_suffix(']'))
    else {
        return Ok(None);
    };
    if inner.trim().is_empty() {
        return Ok(Some(Vec::new()));
    }
    let mut values = Vec::new();
    for item in inner.split(',') {
        let pseudo_column = QueryColumn::new(if column.name.contains("id") {
            "item.id"
        } else {
            "item"
        });
        values.push(parse_expected_value(&pseudo_column, item.trim())?);
    }
    Ok(Some(values))
}
