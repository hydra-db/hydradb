//! Source parsing and deterministic outline expansion. Binding is a separate stage.
use gherkin::{Feature, GherkinEnv, Step, Table};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq, Ord, PartialOrd)]
pub struct CaseId {
    pub namespace: String,
    pub revision: String,
    pub path: String,
    pub scenario_line: usize,
    pub scenario_column: usize,
    pub example_line: Option<usize>,
    pub example_row: Option<usize>,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ExpandedStep {
    pub keyword: String,
    pub text: String,
    pub line: usize,
    pub column: usize,
    pub docstring: Option<String>,
    pub table: Option<Vec<Vec<String>>>,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ExpandedCase {
    pub id: CaseId,
    pub feature: String,
    pub scenario: String,
    pub tags: Vec<String>,
    pub bindings: BTreeMap<String, String>,
    pub steps: Vec<ExpandedStep>,
}

fn sanitize_tables(text: &str) -> String {
    let mut output = String::with_capacity(text.len());
    let mut doc_delimiter: Option<&str> = None;
    for line in text.split_inclusive('\n') {
        let trimmed = line.trim_start();
        if let Some(delimiter) = doc_delimiter {
            output.push_str(line);
            if trimmed.starts_with(delimiter) {
                doc_delimiter = None;
            }
            continue;
        }
        if trimmed.starts_with("\"\"\"") || trimmed.starts_with("```") {
            doc_delimiter = Some(if trimmed.starts_with("```") { "```" } else { "\"\"\"" });
            output.push_str(line);
            continue;
        }
        if !trimmed.starts_with('|') {
            output.push_str(line);
            continue;
        }
        let mut chars = line.chars();
        while let Some(c) = chars.next() {
            output.push(c);
            if c == '\\' {
                if let Some(next) = chars.next() {
                    output.push(if next == '\'' { '\\' } else { next });
                }
            }
        }
    }
    output
}

fn decode_row(line: &str) -> Result<Vec<String>, String> {
    let line = line.trim();
    if !line.starts_with('|') {
        return Err("table row missing opening delimiter".into());
    }
    if line == "|" {
        return Ok(Vec::new());
    }
    let mut cells = Vec::new();
    let mut cell = String::new();
    let mut chars = line[1..].chars();
    let mut closed = false;
    while let Some(c) = chars.next() {
        closed = false;
        match c {
            '|' => {
                cells.push(cell.trim().to_string());
                cell.clear();
                closed = true;
            }
            '\\' => match chars.next() {
                Some('|') => cell.push('|'),
                Some('\\') => cell.push('\\'),
                Some('n') => cell.push('\n'),
                Some(other) => {
                    cell.push('\\');
                    cell.push(other);
                }
                None => return Err("trailing table escape".into()),
            },
            _ => cell.push(c),
        }
    }
    if !closed && !cell.trim().is_empty() {
        return Err("table row missing closing delimiter".into());
    }
    if cells.is_empty() {
        return Err("empty table row".into());
    }
    Ok(cells)
}

fn restore_table(table: &mut Table, lines: &[&str]) -> Result<(), String> {
    let start = table.position.line.checked_sub(1).ok_or("invalid table location")?;
    let mut rows = Vec::new();
    for line in lines.iter().skip(start) {
        let trimmed = line.trim_start();
        if trimmed.starts_with('|') {
            rows.push(decode_row(line)?);
        } else if !trimmed.is_empty() && !trimmed.starts_with('#') {
            break;
        }
        if rows.len() == table.rows.len() {
            break;
        }
    }
    if rows.len() != table.rows.len() || rows.is_empty() || rows.iter().any(|r| r.len() != rows[0].len()) {
        return Err(format!("malformed table at line {}", table.position.line));
    }
    table.rows = rows;
    Ok(())
}
fn restore_steps(steps: &mut [Step], lines: &[&str]) -> Result<(), String> {
    for step in steps {
        if let Some(table) = &mut step.table {
            restore_table(table, lines)?;
        }
    }
    Ok(())
}

pub fn parse(text: &str) -> Result<Feature, String> {
    let sanitized = sanitize_tables(text);
    debug_assert_eq!(text.len(), sanitized.len());
    let mut feature = Feature::parse(&sanitized, GherkinEnv::default()).map_err(|e| e.to_string())?;
    if !feature.rules.is_empty() {
        return Err("Gherkin Rule is not supported".into());
    }
    let lines: Vec<_> = text.lines().collect();
    if let Some(background) = &mut feature.background {
        restore_steps(&mut background.steps, &lines)?;
    }
    for scenario in &mut feature.scenarios {
        restore_steps(&mut scenario.steps, &lines)?;
        for example in &mut scenario.examples {
            if let Some(table) = &mut example.table {
                restore_table(table, &lines)?;
            }
        }
    }
    Ok(feature)
}

fn substitute(text: &str, bindings: &BTreeMap<String, String>) -> String {
    // Replace in one pass: a value that happens to contain another placeholder is literal.
    let mut out = String::new();
    let mut remainder = text;
    while let Some(start) = remainder.find('<') {
        out.push_str(&remainder[..start]);
        let suffix = &remainder[start + 1..];
        if let Some(end) = suffix.find('>') {
            let key = &suffix[..end];
            if let Some(value) = bindings.get(key) {
                out.push_str(value);
            } else {
                out.push('<');
                out.push_str(key);
                out.push('>');
            }
            remainder = &suffix[end + 1..];
        } else {
            out.push_str(&remainder[start..]);
            return out;
        }
    }
    out.push_str(remainder);
    out
}
fn check_placeholders(text: &str, bindings: &BTreeMap<String, String>) -> Result<(), String> {
    let mut rest = text;
    while let Some(start) = rest.find('<') {
        let suffix = &rest[start + 1..];
        let Some(end) = suffix.find('>') else {
            break;
        };
        let key = &suffix[..end];
        if !key.is_empty()
            && key.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
            && key.chars().next().unwrap().is_ascii_alphabetic()
            && !bindings.contains_key(key)
        {
            return Err(format!("unresolved outline placeholder <{key}>"));
        }
        rest = &suffix[end + 1..];
    }
    Ok(())
}

fn expand_step(step: &Step, bindings: &BTreeMap<String, String>) -> ExpandedStep {
    ExpandedStep {
        keyword: step.keyword.clone(),
        text: substitute(&step.value, bindings),
        line: step.position.line,
        column: step.position.col,
        docstring: step.docstring.as_ref().map(|s| substitute(s, bindings)),
        table: step
            .table
            .as_ref()
            .map(|t| t.rows.iter().map(|r| r.iter().map(|s| substitute(s, bindings)).collect()).collect()),
    }
}

pub fn expand(feature: &Feature, namespace: &str, revision: &str, path: &str) -> Result<Vec<ExpandedCase>, String> {
    if namespace.is_empty() || revision.is_empty() || path.is_empty() {
        return Err("empty source identity".into());
    }
    if !feature.rules.is_empty() {
        return Err("Gherkin Rule is not supported".into());
    }
    let mut cases = Vec::new();
    for scenario in &feature.scenarios {
        let mut variants = Vec::new();
        if scenario.examples.is_empty() {
            if scenario.keyword.to_lowercase().contains("outline") || scenario.keyword.to_lowercase().contains("template") {
                return Err(format!("outline at {} has no examples", scenario.position.line));
            }
            variants.push((None, None, BTreeMap::new(), Vec::new()));
        } else {
            for example in &scenario.examples {
                let rows = &example.table.as_ref().ok_or("examples missing table")?.rows;
                let header = rows.first().ok_or("examples missing header")?;
                if header.is_empty() || rows.len() < 2 {
                    return Err("examples must contain header and data".into());
                }
                let unique: BTreeSet<_> = header.iter().collect();
                if unique.len() != header.len() || header.iter().any(|s| s.is_empty()) {
                    return Err("duplicate or empty examples header".into());
                }
                for (row_index, row) in rows.iter().skip(1).enumerate() {
                    if row.len() != header.len() {
                        return Err("examples row width mismatch".into());
                    }
                    variants.push((
                        Some(example.position.line),
                        Some(row_index + 1),
                        header.iter().cloned().zip(row.iter().cloned()).collect(),
                        example.tags.clone(),
                    ));
                }
            }
        }
        for (example_line, example_row, bindings, example_tags) in variants {
            if example_line.is_some() {
                check_placeholders(&scenario.name, &bindings)?;
                for step in feature.background.iter().flat_map(|b| b.steps.iter()).chain(scenario.steps.iter()) {
                    check_placeholders(&step.value, &bindings)?;
                    if let Some(doc) = &step.docstring {
                        check_placeholders(doc, &bindings)?;
                    }
                    if let Some(table) = &step.table {
                        for row in &table.rows {
                            for cell in row {
                                check_placeholders(cell, &bindings)?;
                            }
                        }
                    }
                }
            }
            let mut tags = feature.tags.clone();
            tags.extend(scenario.tags.clone());
            tags.extend(example_tags);
            let steps = feature
                .background
                .iter()
                .flat_map(|b| b.steps.iter())
                .chain(scenario.steps.iter())
                .map(|s| expand_step(s, &bindings))
                .collect();
            cases.push(ExpandedCase {
                id: CaseId {
                    namespace: namespace.into(),
                    revision: revision.into(),
                    path: path.into(),
                    scenario_line: scenario.position.line,
                    scenario_column: scenario.position.col,
                    example_line,
                    example_row,
                },
                feature: feature.name.clone(),
                scenario: substitute(&scenario.name, &bindings),
                tags,
                bindings,
                steps,
            });
        }
    }
    Ok(cases)
}
