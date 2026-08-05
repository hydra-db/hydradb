use std::collections::BTreeMap;

use crate::{GraphError, QueryColumn, QueryFloat, Result, VertexId, VertexPropertyValue};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum NativePathProcedureKind {
    SinglePair,
    SingleSource,
    MultiSource,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum NativePathDirection {
    Incoming,
    Outgoing,
    Both,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum NativePathProjection {
    Path,
    Weight,
    Cost,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct NativePathNodeSelector {
    pub(crate) label: String,
    pub(crate) property: String,
    pub(crate) values: Vec<String>,
}

impl NativePathProjection {
    pub(crate) fn column(self) -> QueryColumn {
        QueryColumn::new(match self {
            Self::Path => "path",
            Self::Weight => "pathWeight",
            Self::Cost => "pathCost",
        })
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct NativePathProcedure {
    pub(crate) kind: NativePathProcedureKind,
    pub(crate) source: VertexId,
    pub(crate) target: Option<VertexId>,
    pub(crate) source_selector: Option<NativePathNodeSelector>,
    pub(crate) target_selector: Option<NativePathNodeSelector>,
    pub(crate) pairwise: bool,
    pub(crate) fair_relationship_variants: bool,
    pub(crate) rel_types: Vec<String>,
    pub(crate) direction: NativePathDirection,
    pub(crate) max_len: u8,
    pub(crate) weight_property: Option<String>,
    pub(crate) cost_property: Option<String>,
    pub(crate) max_cost: Option<QueryFloat>,
    pub(crate) path_count: u64,
    pub(crate) result_limit: Option<u64>,
    pub(crate) projections: Vec<NativePathProjection>,
}

pub(crate) fn is_native_path_procedure(query: &str) -> bool {
    let query = query.trim_start();
    query
        .get(..4)
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("CALL"))
        && query[4..]
            .trim_start()
            .to_ascii_lowercase()
            .starts_with("algo.")
}

pub(crate) fn parse_native_path_procedure(
    query: &str,
    parameters: &BTreeMap<String, VertexPropertyValue>,
    max_traversal_hops: u8,
) -> Result<Option<NativePathProcedure>> {
    if !is_native_path_procedure(query) {
        return Ok(None);
    }
    let mut parser = Parser::new(query)?;
    parser.keyword("CALL")?;
    parser.identifier_eq("algo")?;
    parser.token(Token::Dot)?;
    let procedure = parser.identifier()?;
    let kind = if procedure.eq_ignore_ascii_case("SPpaths") {
        NativePathProcedureKind::SinglePair
    } else if procedure.eq_ignore_ascii_case("SSpaths") {
        NativePathProcedureKind::SingleSource
    } else if procedure.eq_ignore_ascii_case("MSpaths") {
        NativePathProcedureKind::MultiSource
    } else {
        return Ok(None);
    };
    parser.token(Token::LeftParen)?;
    let config = parser.config()?;
    parser.token(Token::RightParen)?;
    parser.keyword("YIELD")?;
    let yielded = parser.projections()?;
    parser.keyword("RETURN")?;
    let projections = parser.projections()?;
    parser.optional(Token::Semicolon);
    parser.end()?;
    for projection in &projections {
        if !yielded.contains(projection) {
            return path_parse_error("RETURN may only reference yielded path columns");
        }
    }

    let (source, target, source_selector, target_selector, pairwise) = match kind {
        NativePathProcedureKind::SinglePair => {
            let source = required_vertex(&config, "sourceNode", parameters)?;
            let target = optional_vertex(&config, "targetNode", parameters)?;
            if target.is_none() {
                return path_parse_error("algo.SPpaths requires targetNode");
            }
            (source, target, None, None, false)
        }
        NativePathProcedureKind::SingleSource => {
            let source = required_vertex(&config, "sourceNode", parameters)?;
            if optional_vertex(&config, "targetNode", parameters)?.is_some() {
                return path_parse_error("algo.SSpaths does not accept targetNode");
            }
            (source, None, None, None, false)
        }
        NativePathProcedureKind::MultiSource => {
            let source_selector = required_selector(&config, "source")?;
            let target_selector = optional_selector(&config, "target", &source_selector)?;
            let pairwise = config_bool(&config, "pairwise")?.unwrap_or(false);
            if pairwise && target_selector.is_none() {
                return path_parse_error("algo.MSpaths pairwise mode requires targetValues");
            }
            (0, None, Some(source_selector), target_selector, pairwise)
        }
    };
    let rel_types =
        config_string_list(&config, "relTypes")?.ok_or_else(|| GraphError::UnsupportedQuery {
            dialect: "OpenCypher",
            feature: "native path procedures require a non-empty relTypes list".to_string(),
        })?;
    if rel_types.is_empty() {
        return path_parse_error("native path procedures require a non-empty relTypes list");
    }
    for edge_type in &rel_types {
        crate::validate_component("edge_type", edge_type)?;
    }
    let direction_value =
        config_string(&config, "relDirection", parameters)?.map(|value| value.to_ascii_lowercase());
    let direction = match direction_value.as_deref() {
        None | Some("outgoing") => NativePathDirection::Outgoing,
        Some("incoming") => NativePathDirection::Incoming,
        Some("both") => NativePathDirection::Both,
        Some(_) => {
            return path_parse_error("relDirection must be 'incoming', 'outgoing', or 'both'")
        }
    };
    let max_len =
        config_u64(&config, "maxLen", parameters)?.unwrap_or(u64::from(max_traversal_hops));
    if max_len > u64::from(max_traversal_hops) {
        return Err(GraphError::AdmissionRejected {
            operation: "native_path_max_len",
            actual: max_len,
            limit: u64::from(max_traversal_hops),
        });
    }
    let path_count = config_u64(&config, "pathCount", parameters)?.unwrap_or(1);
    let result_limit = config_u64(&config, "resultLimit", parameters)?;
    if result_limit == Some(0) {
        return path_parse_error("resultLimit must be greater than zero");
    }
    let weight_property = config_string(&config, "weightProp", parameters)?;
    let cost_property = config_string(&config, "costProp", parameters)?;
    let max_cost = config_number(&config, "maxCost", parameters)?.map(QueryFloat);
    let fair_relationship_variants =
        config_bool(&config, "fairRelationshipVariants")?.unwrap_or(false);
    if fair_relationship_variants && (kind != NativePathProcedureKind::MultiSource || !pairwise) {
        return path_parse_error(
            "fairRelationshipVariants is only supported by pairwise algo.MSpaths",
        );
    }
    if fair_relationship_variants
        && (weight_property.is_some() || cost_property.is_some() || max_cost.is_some())
    {
        return path_parse_error(
            "fairRelationshipVariants requires an unweighted pairwise algo.MSpaths query",
        );
    }
    let known = [
        "sourceNode",
        "targetNode",
        "relTypes",
        "relDirection",
        "maxLen",
        "weightProp",
        "costProp",
        "maxCost",
        "pathCount",
        "resultLimit",
        "sourceLabel",
        "sourceProperty",
        "sourceValues",
        "targetLabel",
        "targetProperty",
        "targetValues",
        "pairwise",
        "fairRelationshipVariants",
    ];
    if let Some(unknown) = config.keys().find(|key| !known.contains(&key.as_str())) {
        return path_parse_error(format!("unknown native path option {unknown}"));
    }
    let multi_source_options = [
        "sourceLabel",
        "sourceProperty",
        "sourceValues",
        "targetLabel",
        "targetProperty",
        "targetValues",
        "pairwise",
        "fairRelationshipVariants",
    ];
    if kind != NativePathProcedureKind::MultiSource {
        if let Some(option) = multi_source_options
            .iter()
            .find(|option| config.contains_key(**option))
        {
            return path_parse_error(format!("{option} is only supported by algo.MSpaths"));
        }
    } else if let Some(option) = ["sourceNode", "targetNode"]
        .iter()
        .find(|option| config.contains_key(**option))
    {
        return path_parse_error(format!(
            "{option} is not supported by algo.MSpaths; use indexed selectors"
        ));
    }
    Ok(Some(NativePathProcedure {
        kind,
        source,
        target,
        source_selector,
        target_selector,
        pairwise,
        fair_relationship_variants,
        rel_types,
        direction,
        max_len: max_len as u8,
        weight_property,
        cost_property,
        max_cost,
        path_count,
        result_limit,
        projections,
    }))
}

fn required_selector(
    config: &BTreeMap<String, ConfigValue>,
    prefix: &str,
) -> Result<NativePathNodeSelector> {
    optional_selector_parts(config, prefix, None)?.ok_or_else(|| {
        GraphError::MissingQueryParameter {
            dialect: "OpenCypher",
            name: format!("{prefix}Values"),
        }
    })
}

fn optional_selector(
    config: &BTreeMap<String, ConfigValue>,
    prefix: &str,
    defaults: &NativePathNodeSelector,
) -> Result<Option<NativePathNodeSelector>> {
    optional_selector_parts(config, prefix, Some(defaults))
}

fn optional_selector_parts(
    config: &BTreeMap<String, ConfigValue>,
    prefix: &str,
    defaults: Option<&NativePathNodeSelector>,
) -> Result<Option<NativePathNodeSelector>> {
    let values_key = format!("{prefix}Values");
    let Some(values) = config_string_list(config, &values_key)? else {
        let label_key = format!("{prefix}Label");
        let property_key = format!("{prefix}Property");
        if config.contains_key(&label_key) || config.contains_key(&property_key) {
            return path_parse_error(format!(
                "{values_key} is required when {prefix}Label or {prefix}Property is set"
            ));
        }
        return Ok(None);
    };
    if values.is_empty() {
        return path_parse_error(format!("{values_key} must not be empty"));
    }
    if values.iter().any(|value| value.is_empty()) {
        return path_parse_error(format!("{values_key} must not contain empty strings"));
    }
    let label_key = format!("{prefix}Label");
    let property_key = format!("{prefix}Property");
    let label = config_literal_string(config, &label_key)?
        .or_else(|| defaults.map(|selector| selector.label.clone()))
        .ok_or_else(|| GraphError::MissingQueryParameter {
            dialect: "OpenCypher",
            name: label_key,
        })?;
    let property = config_literal_string(config, &property_key)?
        .or_else(|| defaults.map(|selector| selector.property.clone()))
        .ok_or_else(|| GraphError::MissingQueryParameter {
            dialect: "OpenCypher",
            name: property_key,
        })?;
    crate::validate_component("label", &label)?;
    crate::validate_component("property", &property)?;
    Ok(Some(NativePathNodeSelector {
        label,
        property,
        values,
    }))
}

fn config_literal_string(
    config: &BTreeMap<String, ConfigValue>,
    key: &str,
) -> Result<Option<String>> {
    match config.get(key) {
        None | Some(ConfigValue::Null) => Ok(None),
        Some(ConfigValue::String(value)) => Ok(Some(value.clone())),
        Some(_) => path_parse_error(format!("{key} must be a string literal")),
    }
}

fn config_bool(config: &BTreeMap<String, ConfigValue>, key: &str) -> Result<Option<bool>> {
    match config.get(key) {
        None | Some(ConfigValue::Null) => Ok(None),
        Some(ConfigValue::Identifier(value)) if value.eq_ignore_ascii_case("true") => {
            Ok(Some(true))
        }
        Some(ConfigValue::Identifier(value)) if value.eq_ignore_ascii_case("false") => {
            Ok(Some(false))
        }
        Some(_) => path_parse_error(format!("{key} must be true or false")),
    }
}

#[derive(Clone, Debug, PartialEq)]
enum ConfigValue {
    Null,
    Integer(i64),
    Float(f64),
    String(String),
    Parameter(String),
    List(Vec<ConfigValue>),
    Identifier(String),
}

fn required_vertex(
    config: &BTreeMap<String, ConfigValue>,
    key: &str,
    parameters: &BTreeMap<String, VertexPropertyValue>,
) -> Result<VertexId> {
    optional_vertex(config, key, parameters)?.ok_or_else(|| GraphError::MissingQueryParameter {
        dialect: "OpenCypher",
        name: key.to_string(),
    })
}

fn optional_vertex(
    config: &BTreeMap<String, ConfigValue>,
    key: &str,
    parameters: &BTreeMap<String, VertexPropertyValue>,
) -> Result<Option<VertexId>> {
    let Some(value) = config.get(key) else {
        return Ok(None);
    };
    let value = match value {
        ConfigValue::Null => return Ok(None),
        ConfigValue::Integer(value) => *value,
        ConfigValue::Parameter(parameter) => match parameters.get(parameter) {
            Some(VertexPropertyValue::Integer(value)) => {
                return Ok(Some(*value));
            }
            Some(VertexPropertyValue::SignedInteger(value)) => *value,
            Some(_) => {
                return path_parse_error(format!("{key} must resolve to an integer node id"))
            }
            None => {
                return Err(GraphError::MissingQueryParameter {
                    dialect: "OpenCypher",
                    name: parameter.clone(),
                })
            }
        },
        _ => return path_parse_error(format!("{key} must be an integer node id")),
    };
    u64::try_from(value)
        .map(Some)
        .map_err(|_| GraphError::UnsupportedQuery {
            dialect: "OpenCypher",
            feature: format!("{key} must be a non-negative integer node id"),
        })
}

fn config_string(
    config: &BTreeMap<String, ConfigValue>,
    key: &str,
    parameters: &BTreeMap<String, VertexPropertyValue>,
) -> Result<Option<String>> {
    match config.get(key) {
        None | Some(ConfigValue::Null) => Ok(None),
        Some(ConfigValue::String(value)) => Ok(Some(value.clone())),
        Some(ConfigValue::Parameter(parameter)) => match parameters.get(parameter) {
            Some(VertexPropertyValue::String(value)) => Ok(Some(value.clone())),
            Some(_) => path_parse_error(format!("{key} parameter must be a string")),
            None => Err(GraphError::MissingQueryParameter {
                dialect: "OpenCypher",
                name: parameter.clone(),
            }),
        },
        Some(_) => path_parse_error(format!("{key} must be a string")),
    }
}

fn config_string_list(
    config: &BTreeMap<String, ConfigValue>,
    key: &str,
) -> Result<Option<Vec<String>>> {
    match config.get(key) {
        None | Some(ConfigValue::Null) => Ok(None),
        Some(ConfigValue::List(values)) => values
            .iter()
            .map(|value| match value {
                ConfigValue::String(value) => Ok(value.clone()),
                _ => path_parse_error(format!("{key} must be a list of strings")),
            })
            .collect::<Result<Vec<_>>>()
            .map(Some),
        Some(_) => path_parse_error(format!("{key} must be a list of strings")),
    }
}

fn config_u64(
    config: &BTreeMap<String, ConfigValue>,
    key: &str,
    parameters: &BTreeMap<String, VertexPropertyValue>,
) -> Result<Option<u64>> {
    match config.get(key) {
        None | Some(ConfigValue::Null) => Ok(None),
        Some(ConfigValue::Integer(value)) => non_negative_u64(*value, key).map(Some),
        Some(ConfigValue::Parameter(parameter)) => match parameters.get(parameter) {
            Some(VertexPropertyValue::Integer(value)) => Ok(Some(*value)),
            Some(VertexPropertyValue::SignedInteger(value)) => {
                non_negative_u64(*value, key).map(Some)
            }
            Some(_) => path_parse_error(format!("{key} parameter must be an integer")),
            None => Err(GraphError::MissingQueryParameter {
                dialect: "OpenCypher",
                name: parameter.clone(),
            }),
        },
        Some(_) => path_parse_error(format!("{key} must be an integer")),
    }
}

fn non_negative_u64(value: i64, key: &str) -> Result<u64> {
    u64::try_from(value).map_err(|_| GraphError::UnsupportedQuery {
        dialect: "OpenCypher",
        feature: format!("{key} must be a non-negative integer"),
    })
}

fn config_number(
    config: &BTreeMap<String, ConfigValue>,
    key: &str,
    parameters: &BTreeMap<String, VertexPropertyValue>,
) -> Result<Option<f64>> {
    let value = match config.get(key) {
        None | Some(ConfigValue::Null) => return Ok(None),
        Some(ConfigValue::Integer(value)) => *value as f64,
        Some(ConfigValue::Float(value)) => *value,
        Some(ConfigValue::Parameter(parameter)) => match parameters.get(parameter) {
            Some(VertexPropertyValue::Integer(value)) => *value as f64,
            Some(VertexPropertyValue::SignedInteger(value)) => *value as f64,
            Some(VertexPropertyValue::Float(value)) => value.0,
            Some(_) => return path_parse_error(format!("{key} parameter must be numeric")),
            None => {
                return Err(GraphError::MissingQueryParameter {
                    dialect: "OpenCypher",
                    name: parameter.clone(),
                })
            }
        },
        Some(_) => return path_parse_error(format!("{key} must be numeric")),
    };
    if !value.is_finite() || value < 0.0 {
        return path_parse_error(format!("{key} must be finite and non-negative"));
    }
    Ok(Some(value))
}

#[derive(Clone, Debug, PartialEq)]
enum Token {
    Identifier(String),
    Parameter(String),
    String(String),
    Integer(i64),
    Float(f64),
    LeftParen,
    RightParen,
    LeftBrace,
    RightBrace,
    LeftBracket,
    RightBracket,
    Colon,
    Comma,
    Dot,
    Semicolon,
}

struct Parser {
    tokens: Vec<Token>,
    offset: usize,
}

impl Parser {
    fn new(query: &str) -> Result<Self> {
        Ok(Self {
            tokens: lex(query)?,
            offset: 0,
        })
    }

    fn config(&mut self) -> Result<BTreeMap<String, ConfigValue>> {
        self.token(Token::LeftBrace)?;
        let mut values = BTreeMap::new();
        if self.optional(Token::RightBrace) {
            return Ok(values);
        }
        loop {
            let key = self.identifier()?;
            self.token(Token::Colon)?;
            let value = self.value()?;
            if values.insert(key.clone(), value).is_some() {
                return path_parse_error(format!("duplicate native path option {key}"));
            }
            if self.optional(Token::RightBrace) {
                return Ok(values);
            }
            self.token(Token::Comma)?;
        }
    }

    fn value(&mut self) -> Result<ConfigValue> {
        match self.next()? {
            Token::Identifier(value) if value.eq_ignore_ascii_case("null") => Ok(ConfigValue::Null),
            Token::Identifier(value) => Ok(ConfigValue::Identifier(value)),
            Token::Parameter(value) => Ok(ConfigValue::Parameter(value)),
            Token::String(value) => Ok(ConfigValue::String(value)),
            Token::Integer(value) => Ok(ConfigValue::Integer(value)),
            Token::Float(value) => Ok(ConfigValue::Float(value)),
            Token::LeftBracket => {
                let mut values = Vec::new();
                if self.optional(Token::RightBracket) {
                    return Ok(ConfigValue::List(values));
                }
                loop {
                    values.push(self.value()?);
                    if self.optional(Token::RightBracket) {
                        return Ok(ConfigValue::List(values));
                    }
                    self.token(Token::Comma)?;
                }
            }
            token => path_parse_error(format!("unexpected native path value {token:?}")),
        }
    }

    fn projections(&mut self) -> Result<Vec<NativePathProjection>> {
        let mut projections = Vec::new();
        loop {
            let projection = match self.identifier()?.to_ascii_lowercase().as_str() {
                "path" => NativePathProjection::Path,
                "pathweight" => NativePathProjection::Weight,
                "pathcost" => NativePathProjection::Cost,
                other => return path_parse_error(format!("unknown path projection {other}")),
            };
            if projections.contains(&projection) {
                return path_parse_error("duplicate path projection");
            }
            projections.push(projection);
            if !self.optional(Token::Comma) {
                return Ok(projections);
            }
        }
    }

    fn keyword(&mut self, expected: &str) -> Result<()> {
        let actual = self.identifier()?;
        if actual.eq_ignore_ascii_case(expected) {
            Ok(())
        } else {
            path_parse_error(format!("expected {expected}, found {actual}"))
        }
    }

    fn identifier_eq(&mut self, expected: &str) -> Result<()> {
        self.keyword(expected)
    }

    fn identifier(&mut self) -> Result<String> {
        match self.next()? {
            Token::Identifier(value) => Ok(value),
            token => path_parse_error(format!("expected identifier, found {token:?}")),
        }
    }

    fn token(&mut self, expected: Token) -> Result<()> {
        let actual = self.next()?;
        if actual == expected {
            Ok(())
        } else {
            path_parse_error(format!("expected {expected:?}, found {actual:?}"))
        }
    }

    fn optional(&mut self, expected: Token) -> bool {
        if self.tokens.get(self.offset) == Some(&expected) {
            self.offset += 1;
            true
        } else {
            false
        }
    }

    fn next(&mut self) -> Result<Token> {
        let token =
            self.tokens
                .get(self.offset)
                .cloned()
                .ok_or_else(|| GraphError::QueryParse {
                    dialect: "OpenCypher",
                    reason: "unexpected end of query".to_string(),
                })?;
        self.offset += 1;
        Ok(token)
    }

    fn end(&self) -> Result<()> {
        if self.offset == self.tokens.len() {
            Ok(())
        } else {
            path_parse_error("unexpected tokens after native path procedure")
        }
    }
}

fn lex(query: &str) -> Result<Vec<Token>> {
    let chars = query.char_indices().collect::<Vec<_>>();
    let mut tokens = Vec::new();
    let mut cursor = 0;
    while cursor < chars.len() {
        let (byte, ch) = chars[cursor];
        if ch.is_ascii_whitespace() {
            cursor += 1;
            continue;
        }
        let punctuation = match ch {
            '(' => Some(Token::LeftParen),
            ')' => Some(Token::RightParen),
            '{' => Some(Token::LeftBrace),
            '}' => Some(Token::RightBrace),
            '[' => Some(Token::LeftBracket),
            ']' => Some(Token::RightBracket),
            ':' => Some(Token::Colon),
            ',' => Some(Token::Comma),
            '.' => Some(Token::Dot),
            ';' => Some(Token::Semicolon),
            _ => None,
        };
        if let Some(token) = punctuation {
            tokens.push(token);
            cursor += 1;
            continue;
        }
        if ch == '\'' || ch == '"' {
            let quote = ch;
            let mut value = String::new();
            cursor += 1;
            let mut closed = false;
            while cursor < chars.len() {
                let (_, current) = chars[cursor];
                cursor += 1;
                if current == quote {
                    closed = true;
                    break;
                }
                if current == '\\' {
                    let Some((_, escaped)) = chars.get(cursor).copied() else {
                        return path_parse_error("unterminated string escape");
                    };
                    cursor += 1;
                    value.push(escaped);
                } else {
                    value.push(current);
                }
            }
            if !closed {
                return path_parse_error("unterminated string literal");
            }
            tokens.push(Token::String(value));
            continue;
        }
        if ch == '$' {
            let start = byte + 1;
            cursor += 1;
            let end = consume_identifier(query, &chars, &mut cursor, start)?;
            tokens.push(Token::Parameter(query[start..end].to_string()));
            continue;
        }
        if ch.is_ascii_alphabetic() || ch == '_' {
            let start = byte;
            cursor += 1;
            let end = consume_identifier(query, &chars, &mut cursor, start)?;
            tokens.push(Token::Identifier(query[start..end].to_string()));
            continue;
        }
        if ch.is_ascii_digit() || ch == '-' {
            let start = byte;
            cursor += 1;
            let mut saw_dot = false;
            while cursor < chars.len() {
                let (_, current) = chars[cursor];
                if current == '.' && !saw_dot {
                    saw_dot = true;
                    cursor += 1;
                } else if current.is_ascii_digit() {
                    cursor += 1;
                } else {
                    break;
                }
            }
            let end = chars.get(cursor).map_or(query.len(), |(byte, _)| *byte);
            let raw = &query[start..end];
            if saw_dot {
                let value = raw.parse::<f64>().map_err(|_| GraphError::QueryParse {
                    dialect: "OpenCypher",
                    reason: format!("invalid number {raw}"),
                })?;
                tokens.push(Token::Float(value));
            } else {
                let value = raw.parse::<i64>().map_err(|_| GraphError::QueryParse {
                    dialect: "OpenCypher",
                    reason: format!("invalid integer {raw}"),
                })?;
                tokens.push(Token::Integer(value));
            }
            continue;
        }
        return path_parse_error(format!("unexpected character {ch:?}"));
    }
    Ok(tokens)
}

fn consume_identifier(
    query: &str,
    chars: &[(usize, char)],
    cursor: &mut usize,
    start: usize,
) -> Result<usize> {
    while *cursor < chars.len()
        && (chars[*cursor].1.is_ascii_alphanumeric() || chars[*cursor].1 == '_')
    {
        *cursor += 1;
    }
    let end = chars.get(*cursor).map_or(query.len(), |(byte, _)| *byte);
    if end == start {
        return path_parse_error("expected identifier");
    }
    Ok(end)
}

fn path_parse_error<T>(reason: impl Into<String>) -> Result<T> {
    Err(GraphError::QueryParse {
        dialect: "OpenCypher",
        reason: reason.into(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_single_pair_native_path_call() {
        let parameters = BTreeMap::from([
            ("source".to_string(), VertexPropertyValue::Integer(7)),
            ("target".to_string(), VertexPropertyValue::Integer(11)),
            ("count".to_string(), VertexPropertyValue::Integer(2)),
        ]);
        let parsed = parse_native_path_procedure(
            "CALL algo.SPpaths({sourceNode: $source, targetNode: $target, relTypes: ['RELATES'], maxLen: 3, relDirection: 'both', pathCount: $count}) YIELD path, pathWeight, pathCost RETURN path, pathWeight, pathCost",
            &parameters,
            10,
        )
        .unwrap()
        .unwrap();
        assert_eq!(parsed.kind, NativePathProcedureKind::SinglePair);
        assert_eq!(parsed.source, 7);
        assert_eq!(parsed.target, Some(11));
        assert!(parsed.source_selector.is_none());
        assert!(parsed.target_selector.is_none());
        assert_eq!(parsed.rel_types, vec!["RELATES"]);
        assert_eq!(parsed.direction, NativePathDirection::Both);
        assert_eq!(parsed.max_len, 3);
        assert_eq!(parsed.path_count, 2);
    }

    #[test]
    fn parses_multi_source_native_path_call() {
        let parsed = parse_native_path_procedure(
            "CALL algo.MSpaths({sourceLabel: 'Entity', sourceProperty: 'name', sourceValues: ['alpha', 'beta'], targetValues: ['alpha', 'beta'], pairwise: true, relTypes: ['RELATES'], maxLen: 3, relDirection: 'both', pathCount: 5, fairRelationshipVariants: true, resultLimit: 100}) YIELD path RETURN path",
            &BTreeMap::new(),
            10,
        )
        .unwrap()
        .unwrap();
        assert_eq!(parsed.kind, NativePathProcedureKind::MultiSource);
        assert!(parsed.pairwise);
        assert!(parsed.fair_relationship_variants);
        assert_eq!(parsed.path_count, 5);
        assert_eq!(parsed.result_limit, Some(100));
        assert_eq!(
            parsed.source_selector,
            Some(NativePathNodeSelector {
                label: "Entity".to_string(),
                property: "name".to_string(),
                values: vec!["alpha".to_string(), "beta".to_string()],
            })
        );
        assert_eq!(
            parsed.target_selector,
            Some(NativePathNodeSelector {
                label: "Entity".to_string(),
                property: "name".to_string(),
                values: vec!["alpha".to_string(), "beta".to_string()],
            })
        );
    }

    #[test]
    fn rejects_multi_source_options_on_scalar_procedures() {
        let error = parse_native_path_procedure(
            "CALL algo.SSpaths({sourceNode: 7, sourceValues: ['alpha'], relTypes: ['RELATES']}) YIELD path RETURN path",
            &BTreeMap::new(),
            10,
        )
        .unwrap_err();
        assert!(error.to_string().contains("only supported by algo.MSpaths"));
    }

    #[test]
    fn rejects_partial_multi_source_target_selector() {
        let error = parse_native_path_procedure(
            "CALL algo.MSpaths({sourceLabel: 'Entity', sourceProperty: 'name', sourceValues: ['alpha'], targetLabel: 'Entity', relTypes: ['RELATES']}) YIELD path RETURN path",
            &BTreeMap::new(),
            10,
        )
        .unwrap_err();
        assert!(error.to_string().contains("targetValues is required"));
    }

    #[test]
    fn rejects_path_depth_above_runtime_limit() {
        let error = parse_native_path_procedure(
            "CALL algo.SSpaths({sourceNode: 7, relTypes: ['RELATES'], maxLen: 11}) YIELD path RETURN path",
            &BTreeMap::new(),
            10,
        )
        .unwrap_err();
        assert!(matches!(error, GraphError::AdmissionRejected { .. }));
    }
}
