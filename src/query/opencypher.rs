use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::ffi::{CStr, CString};
use std::ptr::null_mut;

use libcypher_parser_sys as sys;

use crate::{
    validate_component, EdgeMetadata, GraphError, QueryColumn, QueryFloat, QueryWindow, Result,
    VertexId, VertexMetadata, VertexPropertyValue,
};

type AstNode = sys::cypher_astnode_t;

const PARSED_CYPHER_THREAD_CACHE_CAPACITY: usize = 32;
const MAX_TOP_LEVEL_UNION_ARMS: usize = 256;
#[cfg(feature = "client-api")]
const UPDATE_IF_NEWER_MARKER: &str = "__hydradb_update_if_newer_by";
#[cfg(feature = "client-api")]
const CREATE_ONLY_MARKER_PREFIX: &str = "__hydradb_create_only_";

thread_local! {
    static PARSED_CYPHER_THREAD_CACHE: RefCell<ParsedCypherThreadCache> =
        RefCell::new(ParsedCypherThreadCache::default());
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ParsedRowQuery {
    pub patterns: Vec<RowPattern>,
    pub pattern_groups: Vec<RowMatchGroup>,
    pub union_arms: Vec<ParsedRowQuery>,
    pub union_all: bool,
    pub predicate: Option<RowPredicate>,
    pub projections: Vec<RowProjection>,
    pub order_by: Vec<RowSort>,
    pub window: QueryWindow,
    pub columns: Vec<QueryColumn>,
    pub distinct: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ParsedMutationQuery {
    pub patterns: Vec<RowPattern>,
    pub predicate: Option<RowPredicate>,
    pub actions: Vec<RowMutationAction>,
}

#[cfg(feature = "client-api")]
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ParsedUnwindBatch {
    pub(crate) parameter: String,
    pub(crate) kind: ParsedUnwindBatchKind,
}

#[cfg(feature = "client-api")]
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ParsedUnwindBatchKind {
    OutNeighbors {
        edge_type: String,
        source_field: String,
        source_column: QueryColumn,
        destination_column: QueryColumn,
    },
    CreateEdges {
        edge_type: String,
        source_field: String,
        destination_field: String,
    },
    CreateEdgesBetweenLabeledVertices {
        edge_type: String,
        source_field: String,
        destination_field: String,
        source_label: String,
        destination_label: String,
    },
    DeleteEdges {
        edge_type: String,
        source_field: String,
        destination_field: String,
    },
    DeleteVertices {
        vertex_field: String,
        detach: bool,
    },
    DeleteRelationshipsByProperty {
        edge_type: String,
        property: String,
        value_field: String,
    },
    UpsertVertices {
        label: String,
        vertex_field: String,
        property_fields: BTreeMap<String, String>,
        update_if_newer_by: Option<String>,
        create_only_properties: BTreeSet<String>,
    },
    CreateRelationshipsBetweenLabeledVertices {
        edge_type: String,
        source_field: String,
        destination_field: String,
        relationship_id_field: String,
        property_fields: BTreeMap<String, String>,
        source_label: String,
        destination_label: String,
    },
    MergeRelationshipsBetweenLabeledVertices {
        edge_type: String,
        source_field: String,
        destination_field: String,
        relationship_id_field: String,
        property_fields: BTreeMap<String, String>,
        source_label: String,
        destination_label: String,
        update_if_newer_by: Option<String>,
        create_only_properties: BTreeSet<String>,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum OpenCypherQueryAccess {
    Read,
    Write,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RowPattern {
    Node(RowNodePattern),
    Edge(RowEdgePattern),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RowMatchGroup {
    pub patterns: Vec<RowPattern>,
    pub predicate: Option<RowPredicate>,
    pub optional: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RowEdgePattern {
    pub binding: Option<String>,
    pub edge_type: String,
    pub src: RowNodePattern,
    pub dst: RowNodePattern,
    pub properties: BTreeMap<String, VertexPropertyValue>,
    pub hop_range: Option<(u8, u8)>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RowNodePattern {
    pub binding: Option<String>,
    pub id: Option<VertexId>,
    pub labels: BTreeSet<String>,
    pub properties: BTreeMap<String, VertexPropertyValue>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RowProjection {
    NodeId {
        binding: String,
    },
    Property {
        binding: String,
        property: String,
    },
    CountAll,
    Aggregate {
        function: RowAggregateFunction,
        expression: RowExpression,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RowAggregateFunction {
    Count,
    Sum,
    Avg,
    Collect,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RowMutationAction {
    CreateEdge {
        edge_type: String,
        src: VertexId,
        dst: VertexId,
        src_metadata: VertexMetadata,
        dst_metadata: VertexMetadata,
        edge_metadata: EdgeMetadata,
    },
    DeleteBinding {
        binding: String,
        detach: bool,
    },
    DeleteRelationship {
        binding: String,
        detach: bool,
    },
    SetProperty {
        binding: String,
        property: String,
        value: VertexPropertyValue,
    },
    SetLabels {
        binding: String,
        labels: BTreeSet<String>,
    },
    RemoveProperty {
        binding: String,
        property: String,
    },
    RemoveLabels {
        binding: String,
        labels: BTreeSet<String>,
    },
    MergeEdge {
        edge_type: String,
        src: VertexId,
        dst: VertexId,
        src_metadata: VertexMetadata,
        dst_metadata: VertexMetadata,
        edge_metadata: EdgeMetadata,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RowSort {
    pub expression: RowSortExpression,
    pub ascending: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RowSortExpression {
    NodeId { binding: String },
    Property { binding: String, property: String },
    Column { name: String },
    CountAll,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RowPredicate {
    Compare {
        left: RowExpression,
        op: RowComparisonOp,
        right: RowExpression,
    },
    StartsWith {
        expression: RowExpression,
        prefix: String,
    },
    And(Box<RowPredicate>, Box<RowPredicate>),
    Or(Box<RowPredicate>, Box<RowPredicate>),
    Not(Box<RowPredicate>),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RowExpression {
    NodeId { binding: String },
    Property { binding: String, property: String },
    Literal(VertexPropertyValue),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RowComparisonOp {
    Eq,
    Ne,
    Lt,
    Gt,
    Lte,
    Gte,
}

pub fn parse_opencypher_row_query(query: &str) -> Result<ParsedRowQuery> {
    parse_opencypher_row_query_with_parameters(query, &BTreeMap::new())
}

pub fn parse_opencypher_row_query_with_parameters(
    query: &str,
    parameters: &BTreeMap<String, VertexPropertyValue>,
) -> Result<ParsedRowQuery> {
    if query_might_contain_union(query) {
        if let Some(union) = split_top_level_union(query)? {
            return with_parsed_cypher_union(query, &union, |arms, union_all| {
                lower_independently_parsed_union_arms(arms, union_all, parameters)
            });
        }
    }
    with_parsed_cypher(query, |parsed| parsed.lower_row_query(parameters))
}

pub fn parse_opencypher_mutation_query_with_parameters(
    query: &str,
    parameters: &BTreeMap<String, VertexPropertyValue>,
) -> Result<Option<ParsedMutationQuery>> {
    with_parsed_cypher(query, |parsed| parsed.lower_mutation_query(parameters))
}

#[cfg(feature = "client-api")]
pub(crate) fn parse_opencypher_unwind_batch(query: &str) -> Result<Option<ParsedUnwindBatch>> {
    if starts_with_non_unwind_clause(query) {
        return Ok(None);
    }
    with_parsed_cypher(query, ParsedCypher::lower_unwind_batch)
}

#[cfg(feature = "client-api")]
fn starts_with_non_unwind_clause(query: &str) -> bool {
    const NON_UNWIND_CLAUSES: &[&str] = &[
        "CALL", "CREATE", "MATCH", "MERGE", "OPTIONAL", "REMOVE", "RETURN", "SET", "WITH",
    ];
    let query = query.trim_start();
    NON_UNWIND_CLAUSES.iter().any(|keyword| {
        query
            .get(..keyword.len())
            .is_some_and(|prefix| prefix.eq_ignore_ascii_case(keyword))
            && query[keyword.len()..]
                .chars()
                .next()
                .is_none_or(|next| next.is_ascii_whitespace() || matches!(next, '(' | '[' | '{'))
    })
}

pub(crate) fn classify_opencypher_query_access(query: &str) -> Result<OpenCypherQueryAccess> {
    if super::path_procedure::is_native_path_procedure(query) {
        return Ok(OpenCypherQueryAccess::Read);
    }
    if query_might_contain_union(query) {
        if let Some(union) = split_top_level_union(query)? {
            return with_parsed_cypher_union(query, &union, |arms, _| {
                for arm in arms {
                    if arm.query_access()? != OpenCypherQueryAccess::Read {
                        return unsupported("UNION only supports read queries");
                    }
                }
                Ok(OpenCypherQueryAccess::Read)
            });
        }
    }
    with_parsed_cypher(query, ParsedCypher::query_access)
}

struct ParsedCypherEntry {
    query: String,
    parsed: ParsedCypher,
    /// Computed once, next to the parse it describes. Recomputing a shape hash
    /// on every span would be a second scan of the query text on the hot path
    /// for a value that cannot change.
    fingerprint: String,
}

struct ParsedCypherUnionEntry {
    query: String,
    arms: Vec<ParsedCypher>,
    union_all: bool,
    fingerprint: String,
}

#[derive(Default)]
struct ParsedCypherThreadCache {
    entries: VecDeque<ParsedCypherEntry>,
}

#[derive(Default)]
struct ParsedCypherUnionThreadCache {
    entries: VecDeque<ParsedCypherUnionEntry>,
}

thread_local! {
    static PARSED_CYPHER_UNION_THREAD_CACHE: RefCell<ParsedCypherUnionThreadCache> =
        RefCell::new(ParsedCypherUnionThreadCache::default());
}

#[derive(Debug, Eq, PartialEq)]
struct TopLevelUnion<'a> {
    arms: Vec<&'a str>,
    union_all: bool,
}

#[inline]
fn query_might_contain_union(query: &str) -> bool {
    query.as_bytes().windows(5).any(|window| {
        window[0].eq_ignore_ascii_case(&b'U')
            && window[1].eq_ignore_ascii_case(&b'N')
            && window[2].eq_ignore_ascii_case(&b'I')
            && window[3].eq_ignore_ascii_case(&b'O')
            && window[4].eq_ignore_ascii_case(&b'N')
    })
}

// libcypher-parser rejects otherwise valid statements once detailed path
// projections are repeated across enough UNION arms. Segment only lexical
// top-level separators, then let libcypher-parser validate every complete arm.
fn split_top_level_union(query: &str) -> Result<Option<TopLevelUnion<'_>>> {
    let bytes = query.as_bytes();
    let mut separators = Vec::new();
    let mut delimiters = Vec::new();
    let mut index = 0;

    while index < bytes.len() {
        match bytes[index] {
            b'\'' | b'"' | b'`' => {
                index = skip_quoted_cypher_token(bytes, index);
            }
            b'/' if bytes.get(index + 1) == Some(&b'/') => {
                index = skip_line_comment(bytes, index + 2);
            }
            b'/' if bytes.get(index + 1) == Some(&b'*') => {
                index = skip_block_comment(bytes, index + 2);
            }
            b'(' | b'[' | b'{' => {
                delimiters.push(bytes[index]);
                index += 1;
            }
            b')' | b']' | b'}' => {
                if delimiters.last().is_some_and(|open| {
                    matches!(
                        (*open, bytes[index]),
                        (b'(', b')') | (b'[', b']') | (b'{', b'}')
                    )
                }) {
                    delimiters.pop();
                }
                index += 1;
            }
            current if delimiters.is_empty() && is_cypher_identifier_start(current) => {
                let end = scan_cypher_identifier(bytes, index);
                if query[index..end].eq_ignore_ascii_case("UNION")
                    && is_union_separator_token(bytes, index, end)
                {
                    let mut separator_end = end;
                    let next = skip_cypher_trivia(bytes, end);
                    let union_all = if bytes
                        .get(next)
                        .is_some_and(|value| is_cypher_identifier_start(*value))
                    {
                        let next_end = scan_cypher_identifier(bytes, next);
                        if query[next..next_end].eq_ignore_ascii_case("ALL") {
                            separator_end = next_end;
                            true
                        } else {
                            false
                        }
                    } else {
                        false
                    };
                    separators.push((index, separator_end, union_all));
                    if separators.len() >= MAX_TOP_LEVEL_UNION_ARMS {
                        return unsupported(format!(
                            "UNION exceeds the {MAX_TOP_LEVEL_UNION_ARMS}-arm query limit"
                        ));
                    }
                    index = separator_end;
                } else {
                    index = end;
                }
            }
            _ => index += 1,
        }
    }

    if separators.is_empty() {
        return Ok(None);
    }
    let union_all = separators[0].2;
    if separators.iter().any(|separator| separator.2 != union_all) {
        return unsupported("mixing UNION and UNION ALL is not executable in Query engine");
    }

    let mut arms = Vec::with_capacity(separators.len() + 1);
    let mut start = 0;
    for (separator_start, separator_end, _) in separators {
        let arm = query[start..separator_start].trim();
        if arm.is_empty() {
            return Err(parse_error("UNION requires a query before it"));
        }
        arms.push(arm);
        start = separator_end;
    }
    let final_arm = query[start..].trim();
    if final_arm.is_empty() {
        return Err(parse_error("UNION requires a query after it"));
    }
    arms.push(final_arm);
    Ok(Some(TopLevelUnion { arms, union_all }))
}

fn is_union_separator_token(bytes: &[u8], start: usize, end: usize) -> bool {
    let previous = bytes[..start]
        .iter()
        .rev()
        .copied()
        .find(|value| !value.is_ascii_whitespace());
    let next = bytes[end..]
        .iter()
        .copied()
        .find(|value| !value.is_ascii_whitespace());
    !previous.is_some_and(|value| matches!(value, b'$' | b'.' | b':')) && next != Some(b'.')
}

fn is_cypher_identifier_start(value: u8) -> bool {
    value.is_ascii_alphabetic() || value == b'_'
}

fn is_cypher_identifier_continue(value: u8) -> bool {
    is_cypher_identifier_start(value) || value.is_ascii_digit()
}

fn scan_cypher_identifier(bytes: &[u8], mut index: usize) -> usize {
    while bytes
        .get(index)
        .is_some_and(|value| is_cypher_identifier_continue(*value))
    {
        index += 1;
    }
    index
}

fn skip_quoted_cypher_token(bytes: &[u8], start: usize) -> usize {
    let quote = bytes[start];
    let mut index = start + 1;
    while index < bytes.len() {
        if bytes[index] == b'\\' {
            index = (index + 2).min(bytes.len());
            continue;
        }
        if bytes[index] == quote {
            if bytes.get(index + 1) == Some(&quote) {
                index += 2;
                continue;
            }
            return index + 1;
        }
        index += 1;
    }
    bytes.len()
}

fn skip_line_comment(bytes: &[u8], mut index: usize) -> usize {
    while bytes.get(index).is_some_and(|value| *value != b'\n') {
        index += 1;
    }
    index
}

fn skip_block_comment(bytes: &[u8], mut index: usize) -> usize {
    while index + 1 < bytes.len() {
        if bytes[index] == b'*' && bytes[index + 1] == b'/' {
            return index + 2;
        }
        index += 1;
    }
    bytes.len()
}

fn skip_cypher_trivia(bytes: &[u8], mut index: usize) -> usize {
    loop {
        while bytes
            .get(index)
            .is_some_and(|value| value.is_ascii_whitespace())
        {
            index += 1;
        }
        if bytes.get(index) == Some(&b'/') && bytes.get(index + 1) == Some(&b'/') {
            index = skip_line_comment(bytes, index + 2);
            continue;
        }
        if bytes.get(index) == Some(&b'/') && bytes.get(index + 1) == Some(&b'*') {
            index = skip_block_comment(bytes, index + 2);
            continue;
        }
        return index;
    }
}

fn with_parsed_cypher<T>(
    query: &str,
    operation: impl FnOnce(&ParsedCypher) -> Result<T>,
) -> Result<T> {
    let span = tracing::info_span!(
        "query.parse",
        hydradb.query.fingerprint = tracing::field::Empty,
        parse_cache_hit = tracing::field::Empty,
        error.class = tracing::field::Empty,
        hydradb.sampling.tail_keep = tracing::field::Empty,
    );
    let _entered = span.enter();
    PARSED_CYPHER_THREAD_CACHE.with(|cache| {
        let mut cache = cache.borrow_mut();
        if let Some(index) = cache.entries.iter().position(|entry| entry.query == query) {
            let entry = cache
                .entries
                .remove(index)
                .expect("cached Cypher entry exists at the located index");
            cache.entries.push_front(entry);
            span.record("parse_cache_hit", true);
        } else {
            let parsed = match ParsedCypher::parse(query) {
                Ok(parsed) => parsed,
                Err(err) => {
                    // Innermost span that produced it. A parse failure seen at
                    // the client boundary says the request failed; seen here it
                    // says the statement is not Cypher we accept.
                    span.record("error.class", err.class());
                    // Marks the trace for the collector's tail sampler, which
                    // is the only thing that can act on a verdict reached after
                    // the span started. See `hydradb_telemetry::sampling`.
                    span.record("hydradb.sampling.tail_keep", "error");
                    return Err(err);
                }
            };
            cache.entries.push_front(ParsedCypherEntry {
                query: query.to_string(),
                parsed,
                fingerprint: query_shape_fingerprint(query),
            });
            cache.entries.truncate(PARSED_CYPHER_THREAD_CACHE_CAPACITY);
            span.record("parse_cache_hit", false);
        }
        let entry = cache
            .entries
            .front()
            .expect("parsed Cypher cache contains the requested query");
        span.record("hydradb.query.fingerprint", entry.fingerprint.as_str());
        operation(&entry.parsed)
    })
}

fn with_parsed_cypher_union<T>(
    query: &str,
    union: &TopLevelUnion<'_>,
    operation: impl FnOnce(&[ParsedCypher], bool) -> Result<T>,
) -> Result<T> {
    let span = tracing::info_span!(
        "query.parse",
        hydradb.query.fingerprint = tracing::field::Empty,
        parse_cache_hit = tracing::field::Empty,
        error.class = tracing::field::Empty,
        hydradb.sampling.tail_keep = tracing::field::Empty,
    );
    let _entered = span.enter();
    // Cache by the original statement so segmentation does not turn one hot
    // batched query into a parse-cache entry for every arm.
    PARSED_CYPHER_UNION_THREAD_CACHE.with(|cache| {
        let mut cache = cache.borrow_mut();
        if let Some(index) = cache.entries.iter().position(|entry| entry.query == query) {
            let entry = cache
                .entries
                .remove(index)
                .expect("cached Cypher UNION entry exists at the located index");
            cache.entries.push_front(entry);
            span.record("parse_cache_hit", true);
        } else {
            let mut parsed_arms = Vec::with_capacity(union.arms.len());
            for arm in &union.arms {
                match ParsedCypher::parse(arm) {
                    Ok(parsed) => parsed_arms.push(parsed),
                    Err(err) => {
                        span.record("error.class", err.class());
                        span.record("hydradb.sampling.tail_keep", "error");
                        return Err(err);
                    }
                }
            }
            cache.entries.push_front(ParsedCypherUnionEntry {
                query: query.to_string(),
                arms: parsed_arms,
                union_all: union.union_all,
                fingerprint: query_shape_fingerprint(query),
            });
            cache.entries.truncate(PARSED_CYPHER_THREAD_CACHE_CAPACITY);
            span.record("parse_cache_hit", false);
        }
        let entry = cache
            .entries
            .front()
            .expect("parsed Cypher UNION cache contains the requested query");
        span.record("hydradb.query.fingerprint", entry.fingerprint.as_str());
        operation(&entry.arms, entry.union_all)
    })
}

/// The stable identity of a query *shape*, for `hydradb.query.fingerprint`.
///
/// Nothing existing substitutes for this. `ClientQueryRequest::query_id` is
/// generated by the Bolt path as `{session_id}-query-{n}`, which makes it a
/// cancellation handle rather than an identity: it is not stable across
/// sessions, means nothing to a caller, and two runs of the same statement
/// never share one. Correlating on it looks like it works in a single-session
/// test and fails in production.
///
/// Two properties are load-bearing. It is stable across runs, so "this shape
/// got slower" is a question a backend can answer. And it contains no parameter
/// or literal values, so it is safe to log unredacted.
///
/// Consulted through the parsed-query cache where one exists, so the cost is
/// paid once per distinct statement per thread. A query that fails to parse
/// still has a fingerprint — it is computed from text, not from the AST — which
/// matters because the root span is opened before anything is parsed.
// The only caller is the client boundary; with `opencypher` alone the crate is
// a library with no service in front of it.
#[cfg_attr(not(feature = "client-api"), allow(dead_code))]
pub(crate) fn opencypher_query_fingerprint(query: &str) -> String {
    if query_might_contain_union(query) {
        if let Some(fingerprint) = PARSED_CYPHER_UNION_THREAD_CACHE.with(|cache| {
            cache
                .borrow()
                .entries
                .iter()
                .find(|entry| entry.query == query)
                .map(|entry| entry.fingerprint.clone())
        }) {
            return fingerprint;
        }
    }
    PARSED_CYPHER_THREAD_CACHE.with(|cache| {
        if let Some(entry) = cache
            .borrow()
            .entries
            .iter()
            .find(|entry| entry.query == query)
        {
            return entry.fingerprint.clone();
        }
        query_shape_fingerprint(query)
    })
}

/// Normalise a statement to its shape and hash it.
///
/// The normaliser elides every inline literal, drops comments and collapses
/// whitespace, then collapses runs of elided literals so that `IN [1, 2]` and
/// `IN [1, 2, 3]` are one shape. Parameter *names* survive — they are part of
/// the shape and are chosen by the application, never by the data — but no
/// value ever reaches the hash.
fn query_shape_fingerprint(query: &str) -> String {
    let shape = normalize_query_shape(query);
    // FNV-1a, not `DefaultHasher`: the whole point of this value is that it is
    // comparable across processes and across time, and `DefaultHasher`'s
    // algorithm is explicitly not guaranteed stable between Rust releases.
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in shape.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    format!("{hash:016x}")
}

fn normalize_query_shape(query: &str) -> String {
    let bytes = query.as_bytes();
    let mut shape = String::with_capacity(query.len());
    let mut index = 0;
    while index < bytes.len() {
        let byte = bytes[index];
        match byte {
            b'/' if bytes.get(index + 1) == Some(&b'/') => {
                while index < bytes.len() && bytes[index] != b'\n' {
                    index += 1;
                }
            }
            b'/' if bytes.get(index + 1) == Some(&b'*') => {
                index += 2;
                while index < bytes.len()
                    && !(bytes[index] == b'*' && bytes.get(index + 1) == Some(&b'/'))
                {
                    index += 1;
                }
                index = (index + 2).min(bytes.len());
            }
            b'\'' | b'"' | b'`' => {
                let quote = byte;
                index += 1;
                while index < bytes.len() && bytes[index] != quote {
                    // A backslash escape cannot end the literal, so skip the
                    // escaped byte outright rather than re-examining it.
                    index += if bytes[index] == b'\\' { 2 } else { 1 };
                }
                index = (index + 1).min(bytes.len());
                // Backticks quote identifiers, not values, but eliding them
                // costs only the ability to distinguish two shapes that differ
                // solely by a quoted label — and it guarantees no value leaks
                // through a dialect quirk.
                push_elided_literal(&mut shape);
            }
            b'0'..=b'9' => {
                while index < bytes.len()
                    && (bytes[index].is_ascii_digit()
                        || bytes[index] == b'.'
                        || bytes[index] == b'e'
                        || bytes[index] == b'E'
                        || ((bytes[index] == b'+' || bytes[index] == b'-')
                            && matches!(bytes[index - 1], b'e' | b'E')))
                {
                    index += 1;
                }
                push_elided_literal(&mut shape);
            }
            _ if byte.is_ascii_whitespace() => {
                if !shape.ends_with(' ') && !shape.is_empty() {
                    shape.push(' ');
                }
                index += 1;
            }
            _ => {
                shape.push(byte as char);
                index += 1;
            }
        }
    }
    // Numeric-looking words such as `x1` were split by the digit arm above and
    // are already stable; only the trailing separator needs trimming.
    shape.trim().to_string()
}

/// Append a `?` placeholder, collapsing `?, ?` runs to a single `?` so that a
/// literal list's *length* does not fork the fingerprint.
fn push_elided_literal(shape: &mut String) {
    let tail = shape.trim_end();
    if tail.ends_with("?,") || tail.ends_with("?, ") {
        shape.truncate(tail.len() - 1);
        return;
    }
    shape.push('?');
}

struct ParsedCypher {
    result: *mut sys::cypher_parse_result_t,
}

impl ParsedCypher {
    fn parse(query: &str) -> Result<Self> {
        let c_query =
            CString::new(query).map_err(|_| parse_error("query contains an embedded NUL byte"))?;

        unsafe {
            let result = sys::cypher_uparse(
                c_query.as_ptr(),
                query.len() as u64,
                null_mut(),
                null_mut(),
                // bindgen gives this constant the width the platform's headers
                // give it: `u64` where CI runs, already `u32` on macOS. The
                // conversion is therefore load-bearing on one platform and a
                // no-op on the other, and clippy only ever sees the platform it
                // is running on.
                #[allow(clippy::useless_conversion)]
                sys::CYPHER_PARSE_ONLY_STATEMENTS.into(),
            );
            if result.is_null() {
                return Err(parse_error("libcypher-parser returned a null parse result"));
            }

            let parsed = Self { result };
            parsed.ensure_no_parse_errors()?;
            Ok(parsed)
        }
    }

    fn query_access(&self) -> Result<OpenCypherQueryAccess> {
        unsafe {
            let directives = sys::cypher_parse_result_ndirectives(self.result);
            if directives != 1 {
                return unsupported("query transport requires exactly one Cypher statement");
            }

            let statement = checked_node(sys::cypher_parse_result_get_directive(self.result, 0))?;
            ensure_instance(statement, sys::CYPHER_AST_STATEMENT, "statement")?;
            let body = checked_node(sys::cypher_ast_statement_get_body(statement))?;
            ensure_instance(body, sys::CYPHER_AST_QUERY, "query")?;
            let clause_count = sys::cypher_ast_query_nclauses(body);
            if clause_count == 0 {
                return unsupported("query transport requires at least one Cypher clause");
            }

            let mut has_unknown_clause = false;
            let mut has_write_clause = false;
            let mut has_unwind = false;
            for index in 0..clause_count {
                let clause = checked_node(sys::cypher_ast_query_get_clause(body, index))?;
                if is_instance(clause, sys::CYPHER_AST_CREATE)
                    || is_instance(clause, sys::CYPHER_AST_MERGE)
                    || is_instance(clause, sys::CYPHER_AST_DELETE)
                    || is_instance(clause, sys::CYPHER_AST_SET)
                    || is_instance(clause, sys::CYPHER_AST_REMOVE)
                {
                    has_write_clause = true;
                    continue;
                }
                if is_instance(clause, sys::CYPHER_AST_UNWIND) {
                    has_unwind = true;
                    continue;
                }
                if !is_instance(clause, sys::CYPHER_AST_MATCH)
                    && !is_instance(clause, sys::CYPHER_AST_WITH)
                    && !is_instance(clause, sys::CYPHER_AST_RETURN)
                    && !is_instance(clause, sys::CYPHER_AST_UNION)
                {
                    has_unknown_clause = true;
                }
            }
            if has_unknown_clause {
                return unsupported(
                    "query transport cannot authorize an unsupported Cypher clause",
                );
            }
            #[cfg(feature = "client-api")]
            if has_unwind {
                let batch =
                    self.lower_unwind_batch()?
                        .ok_or_else(|| GraphError::UnsupportedQuery {
                            dialect: "OpenCypher",
                            feature: "query transport cannot authorize an unsupported UNWIND query"
                                .to_string(),
                        })?;
                let batch_is_write = matches!(
                    batch.kind,
                    ParsedUnwindBatchKind::CreateEdges { .. }
                        | ParsedUnwindBatchKind::CreateEdgesBetweenLabeledVertices { .. }
                        | ParsedUnwindBatchKind::DeleteEdges { .. }
                        | ParsedUnwindBatchKind::DeleteVertices { .. }
                        | ParsedUnwindBatchKind::DeleteRelationshipsByProperty { .. }
                        | ParsedUnwindBatchKind::UpsertVertices { .. }
                        | ParsedUnwindBatchKind::CreateRelationshipsBetweenLabeledVertices { .. }
                        | ParsedUnwindBatchKind::MergeRelationshipsBetweenLabeledVertices { .. }
                );
                if batch_is_write != has_write_clause {
                    return Err(GraphError::CorruptValue {
                        key: "opencypher/unwind_access".to_string(),
                        reason: "UNWIND access classification disagrees with its clauses"
                            .to_string(),
                    });
                }
                return Ok(if batch_is_write {
                    OpenCypherQueryAccess::Write
                } else {
                    OpenCypherQueryAccess::Read
                });
            }
            #[cfg(not(feature = "client-api"))]
            if has_unwind {
                return unsupported("UNWIND execution requires the client-api feature");
            }
            Ok(if has_write_clause {
                OpenCypherQueryAccess::Write
            } else {
                OpenCypherQueryAccess::Read
            })
        }
    }

    #[cfg(feature = "client-api")]
    fn lower_unwind_batch(&self) -> Result<Option<ParsedUnwindBatch>> {
        unsafe {
            let directives = sys::cypher_parse_result_ndirectives(self.result);
            if directives != 1 {
                return Ok(None);
            }
            let statement = checked_node(sys::cypher_parse_result_get_directive(self.result, 0))?;
            ensure_instance(statement, sys::CYPHER_AST_STATEMENT, "statement")?;
            let query = checked_node(sys::cypher_ast_statement_get_body(statement))?;
            ensure_instance(query, sys::CYPHER_AST_QUERY, "query")?;
            let clause_count = sys::cypher_ast_query_nclauses(query);
            if clause_count < 2 {
                return Ok(None);
            }
            let unwind = checked_node(sys::cypher_ast_query_get_clause(query, 0))?;
            if !is_instance(unwind, sys::CYPHER_AST_UNWIND) {
                return Ok(None);
            }
            let expression = checked_node(sys::cypher_ast_unwind_get_expression(unwind))?;
            if !is_instance(expression, sys::CYPHER_AST_PARAMETER) {
                // An inline list parses, so the rejection lands on a statement that reads
                // like a working batch. Naming the input the batch wants keeps the next
                // attempt from being the same literal list rearranged.
                return unsupported(
                    "UNWIND batch input must be a parameter holding a list of maps, \
                     not an inline list",
                );
            }
            let parameter = parameter_name(expression)?;
            let alias = identifier_name(checked_node(sys::cypher_ast_unwind_get_alias(unwind))?)?;
            let second = checked_node(sys::cypher_ast_query_get_clause(query, 1))?;

            if is_instance(second, sys::CYPHER_AST_MERGE) {
                if clause_count != 3 || sys::cypher_ast_merge_nactions(second) != 0 {
                    return unsupported(
                        "UNWIND vertex upsert requires MERGE by id followed by SET",
                    );
                }
                let set_clause = checked_node(sys::cypher_ast_query_get_clause(query, 2))?;
                if !is_instance(set_clause, sys::CYPHER_AST_SET) {
                    return unsupported(
                        "UNWIND vertex upsert requires MERGE by id followed by SET",
                    );
                }
                let template = unwind_vertex_upsert_template(second, set_clause, &alias)?;
                return Ok(Some(ParsedUnwindBatch {
                    parameter,
                    kind: ParsedUnwindBatchKind::UpsertVertices {
                        label: template.label,
                        vertex_field: template.vertex_field,
                        property_fields: template.property_fields,
                        update_if_newer_by: template.update_if_newer_by,
                        create_only_properties: template.create_only_properties,
                    },
                }));
            }

            if is_instance(second, sys::CYPHER_AST_CREATE) {
                if clause_count != 2 {
                    return unsupported("UNWIND CREATE cannot be followed by another clause");
                }
                let pattern = checked_node(sys::cypher_ast_create_get_pattern(second))?;
                let edge = unwind_edge_template(pattern, &alias, true)?;
                return Ok(Some(ParsedUnwindBatch {
                    parameter,
                    kind: ParsedUnwindBatchKind::CreateEdges {
                        edge_type: edge.edge_type,
                        source_field: edge.source_field,
                        destination_field: edge.destination_field.ok_or_else(|| {
                            unsupported_value("UNWIND CREATE requires destination id field")
                        })?,
                    },
                }));
            }

            if !is_instance(second, sys::CYPHER_AST_MATCH) || !(3..=4).contains(&clause_count) {
                return unsupported(
                    "UNWIND batches support CREATE or MATCH followed by RETURN, DELETE, CREATE, or MERGE",
                );
            }
            if sys::cypher_ast_match_is_optional(second)
                || sys::cypher_ast_match_nhints(second) != 0
                || !sys::cypher_ast_match_get_predicate(second).is_null()
            {
                return unsupported("UNWIND MATCH does not support OPTIONAL, hints, or WHERE");
            }
            let pattern = checked_node(sys::cypher_ast_match_get_pattern(second))?;
            let third = checked_node(sys::cypher_ast_query_get_clause(query, 2))?;
            if is_instance(third, sys::CYPHER_AST_CREATE) {
                if clause_count != 3 {
                    return unsupported("UNWIND MATCH CREATE cannot be followed by another clause");
                }
                let create_pattern = checked_node(sys::cypher_ast_create_get_pattern(third))?;
                let template = unwind_bound_edge_create_template(pattern, create_pattern, &alias)?;
                if let Some(relationship_id_field) = template.relationship_id_field {
                    return Ok(Some(ParsedUnwindBatch {
                        parameter,
                        kind: ParsedUnwindBatchKind::CreateRelationshipsBetweenLabeledVertices {
                            edge_type: template.edge_type,
                            source_field: template.source_field,
                            destination_field: template.destination_field,
                            relationship_id_field,
                            property_fields: template.property_fields,
                            source_label: template.source_label,
                            destination_label: template.destination_label,
                        },
                    }));
                }
                return Ok(Some(ParsedUnwindBatch {
                    parameter,
                    kind: ParsedUnwindBatchKind::CreateEdgesBetweenLabeledVertices {
                        edge_type: template.edge_type,
                        source_field: template.source_field,
                        destination_field: template.destination_field,
                        source_label: template.source_label,
                        destination_label: template.destination_label,
                    },
                }));
            }
            if is_instance(third, sys::CYPHER_AST_MERGE) {
                if sys::cypher_ast_merge_nactions(third) != 0 {
                    return unsupported(
                        "UNWIND relationship MERGE does not support ON CREATE or ON MATCH",
                    );
                }
                let set_clause = if clause_count == 4 {
                    let clause = checked_node(sys::cypher_ast_query_get_clause(query, 3))?;
                    if !is_instance(clause, sys::CYPHER_AST_SET) {
                        return unsupported(
                            "UNWIND relationship MERGE may only be followed by SET",
                        );
                    }
                    Some(clause)
                } else {
                    None
                };
                let template =
                    unwind_bound_edge_merge_template(pattern, third, set_clause, &alias)?;
                return Ok(Some(ParsedUnwindBatch {
                    parameter,
                    kind: ParsedUnwindBatchKind::MergeRelationshipsBetweenLabeledVertices {
                        edge_type: template.edge_type,
                        source_field: template.source_field,
                        destination_field: template.destination_field,
                        relationship_id_field: template.relationship_id_field.ok_or_else(|| {
                            unsupported_value("UNWIND relationship MERGE requires id: row.<field>")
                        })?,
                        property_fields: template.property_fields,
                        source_label: template.source_label,
                        destination_label: template.destination_label,
                        update_if_newer_by: template.update_if_newer_by,
                        create_only_properties: template.create_only_properties,
                    },
                }));
            }
            if is_instance(third, sys::CYPHER_AST_DELETE) {
                if clause_count != 3 {
                    return unsupported("UNWIND MATCH DELETE cannot be followed by another clause");
                }
                if let Some(template) =
                    unwind_relationship_property_delete_template(pattern, &alias)?
                {
                    if sys::cypher_ast_delete_has_detach(third)
                        || sys::cypher_ast_delete_nexpressions(third) != 1
                    {
                        return unsupported(
                            "UNWIND relationship property DELETE requires one relationship",
                        );
                    }
                    let deleted = checked_node(sys::cypher_ast_delete_get_expression(third, 0))?;
                    if identifier_name(deleted)? != template.binding {
                        return unsupported(
                            "UNWIND relationship property DELETE must delete the matched relationship",
                        );
                    }
                    return Ok(Some(ParsedUnwindBatch {
                        parameter,
                        kind: ParsedUnwindBatchKind::DeleteRelationshipsByProperty {
                            edge_type: template.edge_type,
                            property: template.property,
                            value_field: template.value_field,
                        },
                    }));
                }
                if let Some((binding, vertex_field)) =
                    unwind_vertex_delete_template(pattern, &alias)?
                {
                    if sys::cypher_ast_delete_nexpressions(third) != 1 {
                        return unsupported("UNWIND vertex DELETE requires one vertex variable");
                    }
                    let deleted = checked_node(sys::cypher_ast_delete_get_expression(third, 0))?;
                    if identifier_name(deleted)? != binding {
                        return unsupported("UNWIND vertex DELETE must delete the matched vertex");
                    }
                    return Ok(Some(ParsedUnwindBatch {
                        parameter,
                        kind: ParsedUnwindBatchKind::DeleteVertices {
                            vertex_field,
                            detach: sys::cypher_ast_delete_has_detach(third),
                        },
                    }));
                }
                let edge = unwind_edge_template(pattern, &alias, true)?;
                let relationship_binding = edge.relationship_binding.ok_or_else(|| {
                    unsupported_value("UNWIND DELETE requires a named relationship")
                })?;
                if sys::cypher_ast_delete_has_detach(third)
                    || sys::cypher_ast_delete_nexpressions(third) != 1
                {
                    return unsupported("UNWIND DELETE requires exactly one relationship variable");
                }
                let deleted = checked_node(sys::cypher_ast_delete_get_expression(third, 0))?;
                if identifier_name(deleted)? != relationship_binding {
                    return unsupported("UNWIND DELETE must delete the matched relationship");
                }
                return Ok(Some(ParsedUnwindBatch {
                    parameter,
                    kind: ParsedUnwindBatchKind::DeleteEdges {
                        edge_type: edge.edge_type,
                        source_field: edge.source_field,
                        destination_field: edge.destination_field.ok_or_else(|| {
                            unsupported_value("UNWIND DELETE requires destination id field")
                        })?,
                    },
                }));
            }
            if !is_instance(third, sys::CYPHER_AST_RETURN) {
                return unsupported("UNWIND MATCH must end in RETURN or DELETE");
            }
            if clause_count != 3 {
                return unsupported("UNWIND MATCH RETURN cannot be followed by another clause");
            }
            let edge = unwind_edge_template(pattern, &alias, false)?;
            let destination_binding = edge.destination_binding.ok_or_else(|| {
                unsupported_value("UNWIND batch read requires a named destination node")
            })?;
            if edge.destination_field.is_some()
                || sys::cypher_ast_return_is_distinct(third)
                || !sys::cypher_ast_return_get_order_by(third).is_null()
                || !sys::cypher_ast_return_get_skip(third).is_null()
                || !sys::cypher_ast_return_get_limit(third).is_null()
                || sys::cypher_ast_return_nprojections(third) != 2
            {
                return unsupported(
                    "UNWIND batch read requires two unsorted projections without a destination id constraint",
                );
            }
            let source_projection = checked_node(sys::cypher_ast_return_get_projection(third, 0))?;
            let source_expression =
                checked_node(sys::cypher_ast_projection_get_expression(source_projection))?;
            if property_expression_binding(source_expression)?
                != Some((alias.clone(), edge.source_field.clone()))
            {
                return unsupported("UNWIND batch read first projection must be the source field");
            }
            let destination_projection =
                checked_node(sys::cypher_ast_return_get_projection(third, 1))?;
            let destination_expression = checked_node(sys::cypher_ast_projection_get_expression(
                destination_projection,
            ))?;
            if node_id_expression_binding(destination_expression)?.as_deref()
                != Some(destination_binding.as_str())
            {
                return unsupported("UNWIND batch read second projection must be destination.id");
            }
            Ok(Some(ParsedUnwindBatch {
                parameter,
                kind: ParsedUnwindBatchKind::OutNeighbors {
                    edge_type: edge.edge_type,
                    source_field: edge.source_field.clone(),
                    source_column: QueryColumn::new(projection_column_name(
                        source_projection,
                        format!("{}.{}", alias, edge.source_field),
                    )?),
                    destination_column: QueryColumn::new(projection_column_name(
                        destination_projection,
                        format!("{destination_binding}.id"),
                    )?),
                },
            }))
        }
    }

    fn lower_row_query(
        &self,
        parameters: &BTreeMap<String, VertexPropertyValue>,
    ) -> Result<ParsedRowQuery> {
        unsafe {
            let directives = sys::cypher_parse_result_ndirectives(self.result);
            if directives != 1 {
                return unsupported("only a single Cypher statement is supported in Query engine");
            }

            let statement = checked_node(sys::cypher_parse_result_get_directive(self.result, 0))?;
            ensure_instance(statement, sys::CYPHER_AST_STATEMENT, "statement")?;
            let body = checked_node(sys::cypher_ast_statement_get_body(statement))?;
            ensure_instance(body, sys::CYPHER_AST_QUERY, "query")?;
            self.lower_row_query_body(body, parameters)
        }
    }

    fn lower_mutation_query(
        &self,
        parameters: &BTreeMap<String, VertexPropertyValue>,
    ) -> Result<Option<ParsedMutationQuery>> {
        unsafe {
            let directives = sys::cypher_parse_result_ndirectives(self.result);
            if directives != 1 {
                return unsupported("only a single Cypher statement is supported in Query engine");
            }

            let statement = checked_node(sys::cypher_parse_result_get_directive(self.result, 0))?;
            ensure_instance(statement, sys::CYPHER_AST_STATEMENT, "statement")?;
            let body = checked_node(sys::cypher_ast_statement_get_body(statement))?;
            ensure_instance(body, sys::CYPHER_AST_QUERY, "query")?;
            self.lower_mutation_query_body(body, parameters)
        }
    }

    fn lower_row_query_body(
        &self,
        query: *const AstNode,
        parameters: &BTreeMap<String, VertexPropertyValue>,
    ) -> Result<ParsedRowQuery> {
        unsafe {
            let clause_count = sys::cypher_ast_query_nclauses(query);
            if clause_count == 0 {
                return unsupported("row execution supports MATCH ... RETURN queries");
            }
            let mut clauses = Vec::with_capacity(clause_count as usize);
            let mut has_union = false;
            for idx in 0..clause_count {
                let clause = checked_node(sys::cypher_ast_query_get_clause(query, idx))?;
                if is_instance(clause, sys::CYPHER_AST_UNION) {
                    has_union = true;
                }
                clauses.push(clause);
            }
            if has_union {
                lower_row_union_query_clauses(&clauses, parameters)
            } else {
                lower_row_query_clauses(&clauses, parameters)
            }
        }
    }

    fn lower_mutation_query_body(
        &self,
        query: *const AstNode,
        parameters: &BTreeMap<String, VertexPropertyValue>,
    ) -> Result<Option<ParsedMutationQuery>> {
        unsafe {
            let clause_count = sys::cypher_ast_query_nclauses(query);
            if clause_count == 0 {
                return Ok(None);
            }

            let first_clause = checked_node(sys::cypher_ast_query_get_clause(query, 0))?;
            if is_instance(first_clause, sys::CYPHER_AST_CREATE) {
                if clause_count != 1 {
                    return unsupported(
                        "CREATE with following clauses is not executable in Query engine",
                    );
                }
                return Ok(Some(lower_create_mutations(first_clause, parameters)?));
            }
            if is_instance(first_clause, sys::CYPHER_AST_MERGE) {
                if clause_count != 1 {
                    return unsupported(
                        "MERGE with following clauses is not executable in Query engine",
                    );
                }
                return Ok(Some(lower_simple_merge(first_clause, parameters)?));
            }

            let mut match_clauses = Vec::new();
            let mut actions = Vec::new();
            let mut saw_mutation = false;
            for idx in 0..clause_count {
                let clause = checked_node(sys::cypher_ast_query_get_clause(query, idx))?;
                if !saw_mutation && is_instance(clause, sys::CYPHER_AST_MATCH) {
                    match_clauses.push(clause);
                    continue;
                }
                if is_instance(clause, sys::CYPHER_AST_DELETE) {
                    saw_mutation = true;
                    actions.extend(lower_delete_actions(clause)?);
                    continue;
                }
                if is_instance(clause, sys::CYPHER_AST_SET) {
                    saw_mutation = true;
                    actions.extend(lower_set_actions(clause, parameters)?);
                    continue;
                }
                if is_instance(clause, sys::CYPHER_AST_REMOVE) {
                    saw_mutation = true;
                    actions.extend(lower_remove_actions(clause)?);
                    continue;
                }
                if saw_mutation {
                    return unsupported(
                        "mutation queries cannot continue with MATCH, RETURN, or WITH after writes",
                    );
                }
                return Ok(None);
            }

            if !saw_mutation {
                return Ok(None);
            }
            if match_clauses.is_empty() {
                return unsupported("DELETE, SET, and REMOVE require a preceding MATCH");
            }
            if actions.is_empty() {
                return unsupported("mutation query has no executable actions");
            }

            let mut patterns = Vec::new();
            let mut predicate = None;
            for match_clause in match_clauses {
                if sys::cypher_ast_match_is_optional(match_clause) {
                    return unsupported(
                        "OPTIONAL MATCH mutations are not executable in Query engine",
                    );
                }
                if sys::cypher_ast_match_nhints(match_clause) != 0 {
                    return unsupported("MATCH hints are not executable in Query engine mutations");
                }
                let pattern = checked_node(sys::cypher_ast_match_get_pattern(match_clause))?;
                patterns.extend(lower_row_patterns(pattern, parameters)?);
                let match_predicate = sys::cypher_ast_match_get_predicate(match_clause);
                if !match_predicate.is_null() {
                    predicate = Some(and_row_predicates(
                        predicate,
                        lower_row_predicate(match_predicate, parameters)?,
                    ));
                }
            }

            Ok(Some(ParsedMutationQuery {
                patterns,
                predicate,
                actions,
            }))
        }
    }

    fn ensure_no_parse_errors(&self) -> Result<()> {
        unsafe {
            let error_count = sys::cypher_parse_result_nerrors(self.result);
            if error_count == 0 {
                return Ok(());
            }

            let error = sys::cypher_parse_result_get_error(self.result, 0);
            if error.is_null() {
                return Err(parse_error(format!(
                    "libcypher-parser reported {error_count} parse errors"
                )));
            }

            let message = sys::cypher_parse_error_message(error);
            if message.is_null() {
                return Err(parse_error(format!(
                    "libcypher-parser reported {error_count} parse errors"
                )));
            }

            Err(parse_error(c_string(message)))
        }
    }
}

#[cfg(feature = "client-api")]
struct UnwindBoundEdgeCreateTemplate {
    edge_type: String,
    source_field: String,
    destination_field: String,
    source_label: String,
    destination_label: String,
    relationship_id_field: Option<String>,
    relationship_binding: Option<String>,
    property_fields: BTreeMap<String, String>,
    update_if_newer_by: Option<String>,
    create_only_properties: BTreeSet<String>,
}

#[cfg(feature = "client-api")]
struct UnwindVertexUpsertTemplate {
    label: String,
    vertex_field: String,
    property_fields: BTreeMap<String, String>,
    update_if_newer_by: Option<String>,
    create_only_properties: BTreeSet<String>,
}

#[cfg(feature = "client-api")]
fn unwind_vertex_upsert_template(
    merge_clause: *const AstNode,
    set_clause: *const AstNode,
    unwind_alias: &str,
) -> Result<UnwindVertexUpsertTemplate> {
    unsafe {
        let path = checked_node(sys::cypher_ast_merge_get_pattern_path(merge_clause))?;
        ensure_instance(path, sys::CYPHER_AST_PATTERN_PATH, "UNWIND MERGE vertex")?;
        if sys::cypher_ast_pattern_path_nelements(path) != 1 {
            return unsupported("UNWIND vertex MERGE requires exactly one node");
        }
        let node = checked_node(sys::cypher_ast_pattern_path_get_element(path, 0))?;
        ensure_instance(node, sys::CYPHER_AST_NODE_PATTERN, "UNWIND MERGE vertex")?;
        if sys::cypher_ast_node_pattern_nlabels(node) != 0 {
            return unsupported(
                "UNWIND vertex upsert MERGE pattern matches only id; apply labels with SET",
            );
        }
        let node_binding = node_identifier(node)?.ok_or_else(|| {
            unsupported_value("UNWIND vertex upsert MERGE node requires a binding")
        })?;
        let properties = checked_node(sys::cypher_ast_node_pattern_get_properties(node))?;
        let mut fields = unwind_row_property_fields(properties, unwind_alias, "vertex MERGE")?;
        let vertex_field = fields
            .remove("id")
            .ok_or_else(|| unsupported_value("UNWIND vertex MERGE requires id: row.<field>"))?;
        if !fields.is_empty() {
            return unsupported(
                "UNWIND vertex upsert MERGE pattern matches only id; apply properties with SET",
            );
        }

        ensure_instance(set_clause, sys::CYPHER_AST_SET, "UNWIND vertex SET")?;
        let mut label = None;
        let mut property_fields = BTreeMap::new();
        let mut update_if_newer_by = None;
        let mut create_only_properties = BTreeSet::new();
        for index in 0..sys::cypher_ast_set_nitems(set_clause) {
            let item = checked_node(sys::cypher_ast_set_get_item(set_clause, index))?;
            if is_instance(item, sys::CYPHER_AST_SET_LABELS) {
                let binding = identifier_name(checked_node(
                    sys::cypher_ast_set_labels_get_identifier(item),
                )?)?;
                if binding != node_binding {
                    return unsupported("UNWIND vertex SET label references the wrong node");
                }
                if sys::cypher_ast_set_labels_nlabels(item) != 1 || label.is_some() {
                    return unsupported("UNWIND vertex upsert requires exactly one SET label");
                }
                let value =
                    label_name(checked_node(sys::cypher_ast_set_labels_get_label(item, 0))?)?;
                validate_component("label", &value)?;
                label = Some(value);
                continue;
            }
            if is_instance(item, sys::CYPHER_AST_SET_PROPERTY) {
                let property = checked_node(sys::cypher_ast_set_property_get_property(item))?;
                let Some((binding, property)) = property_expression_binding(property)? else {
                    return unsupported("UNWIND vertex SET requires <node>.<property>");
                };
                if binding != node_binding {
                    return unsupported("UNWIND vertex SET property references the wrong node");
                }
                if property.eq_ignore_ascii_case("id") {
                    return unsupported("UNWIND vertex SET cannot update node id");
                }
                validate_component("property", &property)?;
                let expression = checked_node(sys::cypher_ast_set_property_get_expression(item))?;
                let Some((binding, field)) = property_expression_binding(expression)? else {
                    return unsupported(
                        "UNWIND vertex SET values must read fields from the row map",
                    );
                };
                if binding != unwind_alias {
                    return unsupported("UNWIND vertex SET value references the wrong row alias");
                }
                if property == UPDATE_IF_NEWER_MARKER {
                    if update_if_newer_by.replace(field).is_some() {
                        return unsupported("UNWIND vertex SET repeats the update guard");
                    }
                    continue;
                }
                if let Some(create_only_property) = property.strip_prefix(CREATE_ONLY_MARKER_PREFIX)
                {
                    validate_component("property", create_only_property)?;
                    if field != create_only_property {
                        return unsupported(
                            "UNWIND vertex create-only marker must read the same row field",
                        );
                    }
                    create_only_properties.insert(create_only_property.to_string());
                    continue;
                }
                if property_fields.insert(property.clone(), field).is_some() {
                    return unsupported(format!("UNWIND vertex SET repeats property {property}"));
                }
                continue;
            }
            return unsupported("UNWIND vertex upsert supports SET labels and properties only");
        }
        validate_merge_policy(
            &property_fields,
            update_if_newer_by.as_deref(),
            &create_only_properties,
            "vertex",
        )?;
        Ok(UnwindVertexUpsertTemplate {
            label: label.ok_or_else(|| {
                unsupported_value("UNWIND vertex upsert requires exactly one SET label")
            })?,
            vertex_field,
            property_fields,
            update_if_newer_by,
            create_only_properties,
        })
    }
}

#[cfg(feature = "client-api")]
fn unwind_row_property_fields(
    properties: *const AstNode,
    unwind_alias: &str,
    operation: &str,
) -> Result<BTreeMap<String, String>> {
    unsafe {
        ensure_instance(properties, sys::CYPHER_AST_MAP, "UNWIND property map")?;
        let mut fields = BTreeMap::new();
        for index in 0..sys::cypher_ast_map_nentries(properties) {
            let property = prop_name(checked_node(sys::cypher_ast_map_get_key(
                properties, index,
            ))?)?;
            validate_component("property", &property)?;
            let value = checked_node(sys::cypher_ast_map_get_value(properties, index))?;
            let Some((binding, field)) = property_expression_binding(value)? else {
                return unsupported(format!(
                    "UNWIND {operation} properties must read from the row map"
                ));
            };
            if binding != unwind_alias {
                return unsupported(format!(
                    "UNWIND {operation} property references the wrong row alias"
                ));
            }
            if fields.insert(property.clone(), field).is_some() {
                return unsupported(format!("UNWIND {operation} repeats property {property}"));
            }
        }
        Ok(fields)
    }
}

#[cfg(feature = "client-api")]
fn unwind_bound_edge_create_template(
    match_pattern: *const AstNode,
    create_pattern: *const AstNode,
    unwind_alias: &str,
) -> Result<UnwindBoundEdgeCreateTemplate> {
    unsafe {
        ensure_instance(
            create_pattern,
            sys::CYPHER_AST_PATTERN,
            "UNWIND CREATE pattern",
        )?;
        if sys::cypher_ast_pattern_npaths(create_pattern) != 1 {
            return unsupported("UNWIND MATCH CREATE requires one relationship pattern");
        }
        let path = checked_node(sys::cypher_ast_pattern_get_path(create_pattern, 0))?;
        unwind_bound_edge_path_template(match_pattern, path, unwind_alias, "CREATE")
    }
}

#[cfg(feature = "client-api")]
fn unwind_bound_edge_merge_template(
    match_pattern: *const AstNode,
    merge_clause: *const AstNode,
    set_clause: Option<*const AstNode>,
    unwind_alias: &str,
) -> Result<UnwindBoundEdgeCreateTemplate> {
    unsafe {
        let path = checked_node(sys::cypher_ast_merge_get_pattern_path(merge_clause))?;
        let mut template =
            unwind_bound_edge_path_template(match_pattern, path, unwind_alias, "MERGE")?;
        if !template.property_fields.is_empty() {
            return unsupported(
                "UNWIND relationship MERGE pattern matches only id; apply properties with SET",
            );
        }
        if let Some(set_clause) = set_clause {
            let fields = unwind_relationship_set_fields(
                set_clause,
                template.relationship_binding.as_deref().ok_or_else(|| {
                    unsupported_value("UNWIND relationship MERGE SET requires a binding")
                })?,
                unwind_alias,
            )?;
            template.property_fields = fields.property_fields;
            template.update_if_newer_by = fields.update_if_newer_by;
            template.create_only_properties = fields.create_only_properties;
        }
        Ok(template)
    }
}

#[cfg(feature = "client-api")]
struct UnwindRelationshipSetFields {
    property_fields: BTreeMap<String, String>,
    update_if_newer_by: Option<String>,
    create_only_properties: BTreeSet<String>,
}

#[cfg(feature = "client-api")]
fn unwind_relationship_set_fields(
    set_clause: *const AstNode,
    relationship_binding: &str,
    unwind_alias: &str,
) -> Result<UnwindRelationshipSetFields> {
    unsafe {
        ensure_instance(set_clause, sys::CYPHER_AST_SET, "UNWIND relationship SET")?;
        let mut property_fields = BTreeMap::new();
        let mut update_if_newer_by = None;
        let mut create_only_properties = BTreeSet::new();
        for index in 0..sys::cypher_ast_set_nitems(set_clause) {
            let item = checked_node(sys::cypher_ast_set_get_item(set_clause, index))?;
            if !is_instance(item, sys::CYPHER_AST_SET_PROPERTY) {
                return unsupported("UNWIND relationship MERGE supports SET properties only");
            }
            let property = checked_node(sys::cypher_ast_set_property_get_property(item))?;
            let Some((binding, property)) = property_expression_binding(property)? else {
                return unsupported("UNWIND relationship SET requires <relationship>.<property>");
            };
            if binding != relationship_binding {
                return unsupported("UNWIND relationship SET references the wrong relationship");
            }
            if property.eq_ignore_ascii_case("id") {
                return unsupported("UNWIND relationship SET cannot update relationship id");
            }
            validate_component("property", &property)?;
            let expression = checked_node(sys::cypher_ast_set_property_get_expression(item))?;
            let Some((binding, field)) = property_expression_binding(expression)? else {
                return unsupported("UNWIND relationship SET values must read from the row map");
            };
            if binding != unwind_alias {
                return unsupported("UNWIND relationship SET references the wrong row alias");
            }
            if property == UPDATE_IF_NEWER_MARKER {
                if update_if_newer_by.replace(field).is_some() {
                    return unsupported("UNWIND relationship SET repeats the update guard");
                }
                continue;
            }
            if let Some(create_only_property) = property.strip_prefix(CREATE_ONLY_MARKER_PREFIX) {
                validate_component("property", create_only_property)?;
                if field != create_only_property {
                    return unsupported(
                        "UNWIND relationship create-only marker must read the same row field",
                    );
                }
                create_only_properties.insert(create_only_property.to_string());
                continue;
            }
            if property_fields.insert(property.clone(), field).is_some() {
                return unsupported(format!(
                    "UNWIND relationship SET repeats property {property}"
                ));
            }
        }
        validate_merge_policy(
            &property_fields,
            update_if_newer_by.as_deref(),
            &create_only_properties,
            "relationship",
        )?;
        Ok(UnwindRelationshipSetFields {
            property_fields,
            update_if_newer_by,
            create_only_properties,
        })
    }
}

#[cfg(feature = "client-api")]
fn validate_merge_policy(
    property_fields: &BTreeMap<String, String>,
    update_if_newer_by: Option<&str>,
    create_only_properties: &BTreeSet<String>,
    kind: &str,
) -> Result<()> {
    if update_if_newer_by.is_none() && !create_only_properties.is_empty() {
        return unsupported(format!(
            "UNWIND {kind} create-only markers require an update guard"
        ));
    }
    if let Some(guard) = update_if_newer_by {
        if create_only_properties.contains(guard) {
            return unsupported(format!(
                "UNWIND {kind} update guard cannot also be create-only"
            ));
        }
        if property_fields.get(guard).map(String::as_str) != Some(guard) {
            return unsupported(format!(
                "UNWIND {kind} update guard must name a property assigned from the same row field"
            ));
        }
    }
    for property in create_only_properties {
        if property_fields.get(property).map(String::as_str) != Some(property.as_str()) {
            return unsupported(format!(
                "UNWIND {kind} create-only marker must name a property assigned from the same row field"
            ));
        }
    }
    Ok(())
}

#[cfg(feature = "client-api")]
fn unwind_bound_edge_path_template(
    match_pattern: *const AstNode,
    path: *const AstNode,
    unwind_alias: &str,
    operation: &str,
) -> Result<UnwindBoundEdgeCreateTemplate> {
    unsafe {
        ensure_instance(
            match_pattern,
            sys::CYPHER_AST_PATTERN,
            "UNWIND MATCH pattern",
        )?;
        if sys::cypher_ast_pattern_npaths(match_pattern) != 2 {
            return unsupported(format!(
                "UNWIND MATCH {operation} requires exactly two endpoint nodes"
            ));
        }
        let mut endpoints = BTreeMap::<String, (String, String)>::new();
        for index in 0..2 {
            let path = checked_node(sys::cypher_ast_pattern_get_path(match_pattern, index))?;
            ensure_instance(path, sys::CYPHER_AST_PATTERN_PATH, "UNWIND MATCH endpoint")?;
            if sys::cypher_ast_pattern_path_nelements(path) != 1 {
                return unsupported(format!(
                    "UNWIND MATCH {operation} endpoints must be node patterns"
                ));
            }
            let node = checked_node(sys::cypher_ast_pattern_path_get_element(path, 0))?;
            ensure_instance(
                node,
                sys::CYPHER_AST_NODE_PATTERN,
                "UNWIND MATCH endpoint node",
            )?;
            let binding = node_identifier(node)?.ok_or_else(|| {
                unsupported_value(format!(
                    "UNWIND MATCH {operation} endpoints require bindings"
                ))
            })?;
            let (field, label) = unwind_labeled_node_id_field(node, unwind_alias)?;
            if endpoints.insert(binding.clone(), (field, label)).is_some() {
                return unsupported(format!(
                    "UNWIND MATCH {operation} repeats endpoint binding {binding}"
                ));
            }
        }
        ensure_instance(
            path,
            sys::CYPHER_AST_PATTERN_PATH,
            "UNWIND relationship path",
        )?;
        if sys::cypher_ast_pattern_path_nelements(path) != 3 {
            return unsupported(format!(
                "UNWIND MATCH {operation} supports one-hop relationships only"
            ));
        }
        let left = checked_node(sys::cypher_ast_pattern_path_get_element(path, 0))?;
        let relationship = checked_node(sys::cypher_ast_pattern_path_get_element(path, 1))?;
        let right = checked_node(sys::cypher_ast_pattern_path_get_element(path, 2))?;
        ensure_instance(
            left,
            sys::CYPHER_AST_NODE_PATTERN,
            "UNWIND relationship source",
        )?;
        ensure_instance(
            relationship,
            sys::CYPHER_AST_REL_PATTERN,
            "UNWIND relationship",
        )?;
        ensure_instance(
            right,
            sys::CYPHER_AST_NODE_PATTERN,
            "UNWIND relationship destination",
        )?;
        if !sys::cypher_ast_node_pattern_get_properties(left).is_null()
            || !sys::cypher_ast_node_pattern_get_properties(right).is_null()
            || sys::cypher_ast_node_pattern_nlabels(left) != 0
            || sys::cypher_ast_node_pattern_nlabels(right) != 0
        {
            return unsupported(format!(
                "UNWIND MATCH {operation} must reference bound endpoint variables"
            ));
        }
        if !sys::cypher_ast_rel_pattern_get_varlength(relationship).is_null()
            || sys::cypher_ast_rel_pattern_nreltypes(relationship) != 1
        {
            return unsupported(format!(
                "UNWIND MATCH {operation} requires one fixed relationship type"
            ));
        }
        let left_binding = node_identifier(left)?.ok_or_else(|| {
            unsupported_value(format!(
                "UNWIND MATCH {operation} source must reference a binding"
            ))
        })?;
        let right_binding = node_identifier(right)?.ok_or_else(|| {
            unsupported_value(format!(
                "UNWIND MATCH {operation} destination must reference a binding"
            ))
        })?;
        let left_endpoint = endpoints.get(&left_binding).ok_or_else(|| {
            unsupported_value(format!(
                "UNWIND {operation} source {left_binding} is not matched"
            ))
        })?;
        let right_endpoint = endpoints.get(&right_binding).ok_or_else(|| {
            unsupported_value(format!(
                "UNWIND {operation} destination {right_binding} is not matched"
            ))
        })?;
        let edge_type = reltype_name(checked_node(sys::cypher_ast_rel_pattern_get_reltype(
            relationship,
            0,
        ))?)?;
        let relationship_properties = sys::cypher_ast_rel_pattern_get_properties(relationship);
        let (relationship_id_field, property_fields) = if relationship_properties.is_null() {
            (None, BTreeMap::new())
        } else {
            let mut fields = unwind_row_property_fields(
                relationship_properties,
                unwind_alias,
                &format!("relationship {operation}"),
            )?;
            let relationship_id_field = fields.remove("id").ok_or_else(|| {
                unsupported_value(format!(
                    "UNWIND relationship {operation} properties require id: row.<field>"
                ))
            })?;
            (Some(relationship_id_field), fields)
        };
        let relationship_binding = rel_identifier(relationship)?;
        match sys::cypher_ast_rel_pattern_get_direction(relationship) {
            sys::cypher_rel_direction::CYPHER_REL_OUTBOUND => Ok(UnwindBoundEdgeCreateTemplate {
                edge_type,
                source_field: left_endpoint.0.clone(),
                destination_field: right_endpoint.0.clone(),
                source_label: left_endpoint.1.clone(),
                destination_label: right_endpoint.1.clone(),
                relationship_id_field,
                relationship_binding,
                property_fields,
                update_if_newer_by: None,
                create_only_properties: BTreeSet::new(),
            }),
            sys::cypher_rel_direction::CYPHER_REL_INBOUND => Ok(UnwindBoundEdgeCreateTemplate {
                edge_type,
                source_field: right_endpoint.0.clone(),
                destination_field: left_endpoint.0.clone(),
                source_label: right_endpoint.1.clone(),
                destination_label: left_endpoint.1.clone(),
                relationship_id_field,
                relationship_binding,
                property_fields,
                update_if_newer_by: None,
                create_only_properties: BTreeSet::new(),
            }),
            sys::cypher_rel_direction::CYPHER_REL_BIDIRECTIONAL => unsupported(format!(
                "UNWIND MATCH {operation} does not support undirected relationships"
            )),
        }
    }
}

#[cfg(feature = "client-api")]
fn unwind_labeled_node_id_field(
    node: *const AstNode,
    unwind_alias: &str,
) -> Result<(String, String)> {
    unsafe {
        if sys::cypher_ast_node_pattern_nlabels(node) != 1 {
            return unsupported("UNWIND MATCH CREATE endpoints require exactly one label");
        }
        let label = label_name(checked_node(sys::cypher_ast_node_pattern_get_label(
            node, 0,
        ))?)?;
        let properties = checked_node(sys::cypher_ast_node_pattern_get_properties(node))?;
        ensure_instance(
            properties,
            sys::CYPHER_AST_MAP,
            "UNWIND MATCH endpoint properties",
        )?;
        if sys::cypher_ast_map_nentries(properties) != 1 {
            return unsupported("UNWIND MATCH CREATE endpoints support only the id property");
        }
        let key = prop_name(checked_node(sys::cypher_ast_map_get_key(properties, 0))?)?;
        if key != "id" {
            return unsupported("UNWIND MATCH CREATE endpoint property must be id");
        }
        let value = checked_node(sys::cypher_ast_map_get_value(properties, 0))?;
        let Some((binding, field)) = property_expression_binding(value)? else {
            return unsupported("UNWIND MATCH CREATE endpoint id must read from the row map");
        };
        if binding != unwind_alias {
            return unsupported("UNWIND MATCH CREATE endpoint references the wrong row alias");
        }
        Ok((field, label))
    }
}

#[cfg(feature = "client-api")]
struct UnwindRelationshipPropertyDeleteTemplate {
    binding: String,
    edge_type: String,
    property: String,
    value_field: String,
}

#[cfg(feature = "client-api")]
fn unwind_relationship_property_delete_template(
    pattern: *const AstNode,
    unwind_alias: &str,
) -> Result<Option<UnwindRelationshipPropertyDeleteTemplate>> {
    unsafe {
        ensure_instance(
            pattern,
            sys::CYPHER_AST_PATTERN,
            "UNWIND relationship pattern",
        )?;
        if sys::cypher_ast_pattern_npaths(pattern) != 1 {
            return Ok(None);
        }
        let path = checked_node(sys::cypher_ast_pattern_get_path(pattern, 0))?;
        ensure_instance(
            path,
            sys::CYPHER_AST_PATTERN_PATH,
            "UNWIND relationship path",
        )?;
        if sys::cypher_ast_pattern_path_nelements(path) != 3 {
            return Ok(None);
        }
        let left = checked_node(sys::cypher_ast_pattern_path_get_element(path, 0))?;
        let relationship = checked_node(sys::cypher_ast_pattern_path_get_element(path, 1))?;
        let right = checked_node(sys::cypher_ast_pattern_path_get_element(path, 2))?;
        ensure_instance(
            left,
            sys::CYPHER_AST_NODE_PATTERN,
            "UNWIND relationship source",
        )?;
        ensure_instance(
            relationship,
            sys::CYPHER_AST_REL_PATTERN,
            "UNWIND relationship",
        )?;
        ensure_instance(
            right,
            sys::CYPHER_AST_NODE_PATTERN,
            "UNWIND relationship target",
        )?;
        let properties = sys::cypher_ast_rel_pattern_get_properties(relationship);
        if properties.is_null() {
            return Ok(None);
        }
        if !sys::cypher_ast_node_pattern_get_properties(left).is_null()
            || !sys::cypher_ast_node_pattern_get_properties(right).is_null()
            || sys::cypher_ast_node_pattern_nlabels(left) != 0
            || sys::cypher_ast_node_pattern_nlabels(right) != 0
            || !sys::cypher_ast_rel_pattern_get_varlength(relationship).is_null()
            || sys::cypher_ast_rel_pattern_nreltypes(relationship) != 1
        {
            return unsupported(
                "UNWIND relationship property DELETE requires anonymous endpoints and one edge type",
            );
        }
        ensure_instance(
            properties,
            sys::CYPHER_AST_MAP,
            "UNWIND relationship properties",
        )?;
        if sys::cypher_ast_map_nentries(properties) != 1 {
            return unsupported(
                "UNWIND relationship property DELETE requires exactly one property",
            );
        }
        let property = prop_name(checked_node(sys::cypher_ast_map_get_key(properties, 0))?)?;
        let value = checked_node(sys::cypher_ast_map_get_value(properties, 0))?;
        let Some((binding, value_field)) = property_expression_binding(value)? else {
            return unsupported("UNWIND relationship property value must read from the row map");
        };
        if binding != unwind_alias {
            return unsupported("UNWIND relationship property references the wrong row alias");
        }
        Ok(Some(UnwindRelationshipPropertyDeleteTemplate {
            binding: rel_identifier(relationship)?.ok_or_else(|| {
                unsupported_value("UNWIND relationship property DELETE requires a binding")
            })?,
            edge_type: reltype_name(checked_node(sys::cypher_ast_rel_pattern_get_reltype(
                relationship,
                0,
            ))?)?,
            property,
            value_field,
        }))
    }
}

#[cfg(feature = "client-api")]
fn unwind_vertex_delete_template(
    pattern: *const AstNode,
    unwind_alias: &str,
) -> Result<Option<(String, String)>> {
    unsafe {
        ensure_instance(pattern, sys::CYPHER_AST_PATTERN, "UNWIND vertex pattern")?;
        if sys::cypher_ast_pattern_npaths(pattern) != 1 {
            return Ok(None);
        }
        let path = checked_node(sys::cypher_ast_pattern_get_path(pattern, 0))?;
        ensure_instance(path, sys::CYPHER_AST_PATTERN_PATH, "UNWIND vertex path")?;
        if sys::cypher_ast_pattern_path_nelements(path) != 1 {
            return Ok(None);
        }
        let node = checked_node(sys::cypher_ast_pattern_path_get_element(path, 0))?;
        ensure_instance(node, sys::CYPHER_AST_NODE_PATTERN, "UNWIND vertex")?;
        let binding = node_identifier(node)?
            .ok_or_else(|| unsupported_value("UNWIND vertex DELETE requires a named vertex"))?;
        let field = unwind_node_id_field(node, unwind_alias, true)?
            .ok_or_else(|| unsupported_value("UNWIND vertex DELETE requires an id field"))?;
        Ok(Some((binding, field)))
    }
}

impl Drop for ParsedCypher {
    fn drop(&mut self) {
        unsafe {
            sys::cypher_parse_result_free(self.result);
        }
    }
}

fn lower_row_query_clauses(
    clauses: &[*const AstNode],
    parameters: &BTreeMap<String, VertexPropertyValue>,
) -> Result<ParsedRowQuery> {
    unsafe {
        if clauses.len() < 2 {
            return unsupported("row execution supports MATCH ... RETURN queries");
        }
        let Some(return_clause_ptr) = clauses.last().copied() else {
            return unsupported("row execution supports MATCH ... RETURN queries");
        };
        let return_clause = checked_node(return_clause_ptr)?;
        if !is_instance(return_clause, sys::CYPHER_AST_RETURN) {
            return unsupported("row execution supports MATCH ... RETURN queries ending in RETURN");
        }
        let mut match_clauses = Vec::with_capacity(clauses.len().saturating_sub(1));
        let mut scoped_bindings = BTreeSet::new();
        for clause in &clauses[..clauses.len() - 1] {
            if is_instance(*clause, sys::CYPHER_AST_MATCH) {
                collect_match_clause_bindings(*clause, parameters, &mut scoped_bindings)?;
                match_clauses.push(*clause);
                continue;
            }
            if is_instance(*clause, sys::CYPHER_AST_WITH) {
                lower_passthrough_with(*clause, &scoped_bindings)?;
                continue;
            }
            return unsupported(
                "row execution currently supports MATCH/WITH clauses followed by RETURN",
            );
        }
        lower_match_return_rows(&match_clauses, return_clause, parameters)
    }
}

fn lower_row_union_query_clauses(
    clauses: &[*const AstNode],
    parameters: &BTreeMap<String, VertexPropertyValue>,
) -> Result<ParsedRowQuery> {
    let mut arms = Vec::new();
    let mut start = 0usize;
    let mut union_all = None;
    for (idx, clause) in clauses.iter().enumerate() {
        if !unsafe { is_instance(*clause, sys::CYPHER_AST_UNION) } {
            continue;
        }
        if idx == start {
            return unsupported("UNION requires a query before it");
        }
        let all = unsafe { sys::cypher_ast_union_has_all(*clause) };
        if let Some(previous) = union_all {
            if previous != all {
                return unsupported("mixing UNION and UNION ALL is not executable in Query engine");
            }
        } else {
            union_all = Some(all);
        }
        arms.push(lower_row_query_clauses(&clauses[start..idx], parameters)?);
        start = idx + 1;
    }
    if start >= clauses.len() {
        return unsupported("UNION requires a query after it");
    }
    arms.push(lower_row_query_clauses(&clauses[start..], parameters)?);
    if arms.len() < 2 {
        return unsupported("UNION requires at least two query arms");
    }

    combine_parsed_union_arms(arms, union_all.unwrap_or(false))
}

fn lower_independently_parsed_union_arms(
    parsed_arms: &[ParsedCypher],
    union_all: bool,
    parameters: &BTreeMap<String, VertexPropertyValue>,
) -> Result<ParsedRowQuery> {
    let mut arms = Vec::with_capacity(parsed_arms.len());
    for parsed in parsed_arms {
        let arm = parsed.lower_row_query(parameters)?;
        if !arm.union_arms.is_empty() {
            return unsupported("nested UNION queries are not executable in Query engine");
        }
        arms.push(arm);
    }
    combine_parsed_union_arms(arms, union_all)
}

fn combine_parsed_union_arms(
    mut arms: Vec<ParsedRowQuery>,
    union_all: bool,
) -> Result<ParsedRowQuery> {
    if arms.len() < 2 {
        return unsupported("UNION requires at least two query arms");
    }
    let columns = arms[0].columns.clone();
    for arm in &arms {
        if arm.columns != columns {
            return unsupported("UNION arms must project the same column names");
        }
    }

    let mut first = arms.remove(0);
    first.union_all = union_all;
    first.union_arms = arms;
    Ok(first)
}

fn lower_match_return_rows(
    match_clauses: &[*const AstNode],
    return_clause: *const AstNode,
    parameters: &BTreeMap<String, VertexPropertyValue>,
) -> Result<ParsedRowQuery> {
    unsafe {
        let mut patterns = Vec::new();
        let mut pattern_groups = Vec::new();
        let mut predicate = None;
        for match_clause in match_clauses {
            let optional = sys::cypher_ast_match_is_optional(*match_clause);
            if sys::cypher_ast_match_nhints(*match_clause) != 0 {
                return unsupported("MATCH hints are not executable in Query engine");
            }

            let pattern = checked_node(sys::cypher_ast_match_get_pattern(*match_clause))?;
            let group_patterns = lower_row_patterns(pattern, parameters)?;
            patterns.extend(group_patterns.iter().cloned());
            let match_predicate = sys::cypher_ast_match_get_predicate(*match_clause);
            let group_predicate = if match_predicate.is_null() {
                None
            } else {
                Some(lower_row_predicate(match_predicate, parameters)?)
            };
            if let Some(group_predicate) = &group_predicate {
                predicate = Some(and_row_predicates(predicate, group_predicate.clone()));
            }
            pattern_groups.push(RowMatchGroup {
                patterns: group_patterns,
                predicate: group_predicate,
                optional,
            });
        }
        if patterns.is_empty() {
            return unsupported("MATCH requires at least one executable row pattern");
        }
        if sys::cypher_ast_return_has_include_existing(return_clause) {
            return unsupported("RETURN * is not executable in Query engine");
        }
        let distinct = sys::cypher_ast_return_is_distinct(return_clause);

        let projection_count = sys::cypher_ast_return_nprojections(return_clause);
        if projection_count == 0 {
            return unsupported("RETURN requires at least one projection");
        }

        let mut projections = Vec::with_capacity(projection_count as usize);
        let mut columns = Vec::with_capacity(projection_count as usize);
        for idx in 0..projection_count {
            let projection =
                checked_node(sys::cypher_ast_return_get_projection(return_clause, idx))?;
            let expression = checked_node(sys::cypher_ast_projection_get_expression(projection))?;
            if is_count_star(expression)? {
                projections.push(RowProjection::CountAll);
                columns.push(QueryColumn::new(projection_column_name(
                    projection, "count(*)",
                )?));
                continue;
            }
            if let Some((function, expression, fallback_name)) =
                lower_row_aggregate_expression(expression, parameters)?
            {
                columns.push(QueryColumn::new(projection_column_name(
                    projection,
                    fallback_name,
                )?));
                projections.push(RowProjection::Aggregate {
                    function,
                    expression,
                });
                continue;
            }

            if let Some(binding) = node_id_expression_binding(expression)? {
                columns.push(QueryColumn::new(projection_column_name(
                    projection,
                    format!("{binding}.id"),
                )?));
                projections.push(RowProjection::NodeId { binding });
                continue;
            }
            let Some((binding, property)) = property_expression_binding(expression)? else {
                return unsupported("RETURN currently supports <binding>.<property> or count(*)");
            };
            columns.push(QueryColumn::new(projection_column_name(
                projection,
                format!("{binding}.{property}"),
            )?));
            projections.push(RowProjection::Property { binding, property });
        }

        let order_by = lower_return_order_by(return_clause)?;
        if distinct {
            validate_distinct_order_by(&order_by, &projections, &columns)?;
        }
        let window = lower_return_window(return_clause, parameters)?;
        Ok(ParsedRowQuery {
            patterns,
            pattern_groups,
            union_arms: Vec::new(),
            union_all: false,
            predicate,
            projections,
            order_by,
            window,
            columns,
            distinct,
        })
    }
}

fn collect_match_clause_bindings(
    match_clause: *const AstNode,
    parameters: &BTreeMap<String, VertexPropertyValue>,
    bindings: &mut BTreeSet<String>,
) -> Result<()> {
    unsafe {
        let pattern = checked_node(sys::cypher_ast_match_get_pattern(match_clause))?;
        for pattern in lower_row_patterns(pattern, parameters)? {
            collect_row_pattern_bindings(&pattern, bindings);
        }
        Ok(())
    }
}

fn collect_row_pattern_bindings(pattern: &RowPattern, bindings: &mut BTreeSet<String>) {
    match pattern {
        RowPattern::Node(node) => collect_row_node_bindings(node, bindings),
        RowPattern::Edge(edge) => {
            if let Some(binding) = &edge.binding {
                bindings.insert(binding.clone());
            }
            collect_row_node_bindings(&edge.src, bindings);
            collect_row_node_bindings(&edge.dst, bindings);
        }
    }
}

fn collect_row_node_bindings(node: &RowNodePattern, bindings: &mut BTreeSet<String>) {
    if let Some(binding) = &node.binding {
        bindings.insert(binding.clone());
    }
}

fn lower_passthrough_with(
    with_clause: *const AstNode,
    scoped_bindings: &BTreeSet<String>,
) -> Result<()> {
    unsafe {
        if scoped_bindings.is_empty() {
            return unsupported("WITH requires preceding bindings in Query engine");
        }
        if sys::cypher_ast_with_is_distinct(with_clause)
            || sys::cypher_ast_with_has_include_existing(with_clause)
            || !sys::cypher_ast_with_get_order_by(with_clause).is_null()
            || !sys::cypher_ast_with_get_skip(with_clause).is_null()
            || !sys::cypher_ast_with_get_limit(with_clause).is_null()
            || !sys::cypher_ast_with_get_predicate(with_clause).is_null()
        {
            return unsupported(
                "WITH currently supports only pass-through identifiers without DISTINCT, WHERE, ORDER BY, SKIP, or LIMIT",
            );
        }

        let projection_count = sys::cypher_ast_with_nprojections(with_clause);
        if projection_count as usize != scoped_bindings.len() {
            return unsupported("WITH must pass through every in-scope binding in Query engine");
        }
        let mut projected = BTreeSet::new();
        for idx in 0..projection_count {
            let projection = checked_node(sys::cypher_ast_with_get_projection(with_clause, idx))?;
            let expression = checked_node(sys::cypher_ast_projection_get_expression(projection))?;
            if !is_instance(expression, sys::CYPHER_AST_IDENTIFIER) {
                return unsupported("WITH pass-through supports only bare identifiers");
            }
            let binding = identifier_name(expression)?;
            let alias = sys::cypher_ast_projection_get_alias(projection);
            if !alias.is_null() && identifier_name(alias)? != binding {
                return unsupported("WITH aliases are not executable in Query engine");
            }
            if !scoped_bindings.contains(&binding) {
                return unsupported(format!("WITH references out-of-scope binding {binding}"));
            }
            projected.insert(binding);
        }
        if &projected != scoped_bindings {
            return unsupported(
                "WITH must pass through each in-scope binding exactly once in Query engine",
            );
        }
        Ok(())
    }
}

fn lower_simple_merge(
    merge_clause: *const AstNode,
    parameters: &BTreeMap<String, VertexPropertyValue>,
) -> Result<ParsedMutationQuery> {
    unsafe {
        if sys::cypher_ast_merge_nactions(merge_clause) != 0 {
            return unsupported(
                "MERGE ON CREATE/ON MATCH actions are not executable in Query engine",
            );
        }
        let path = checked_node(sys::cypher_ast_merge_get_pattern_path(merge_clause))?;
        let edge = lower_create_edge_path(path, parameters, "MERGE")?;
        if edge.hop_range.is_some() {
            return unsupported(
                "MERGE does not support variable-length relationships in Query engine",
            );
        }
        let src = edge
            .src
            .id
            .ok_or_else(|| unsupported_value("MERGE requires source id"))?;
        let dst = edge
            .dst
            .id
            .ok_or_else(|| unsupported_value("MERGE requires destination id"))?;
        let edge_metadata = edge_metadata_from_edge_pattern(&edge);
        Ok(ParsedMutationQuery {
            patterns: Vec::new(),
            predicate: None,
            actions: vec![RowMutationAction::MergeEdge {
                edge_type: edge.edge_type,
                src,
                dst,
                src_metadata: vertex_metadata_from_node_pattern(&edge.src),
                dst_metadata: vertex_metadata_from_node_pattern(&edge.dst),
                edge_metadata,
            }],
        })
    }
}

fn lower_create_mutations(
    create_clause: *const AstNode,
    parameters: &BTreeMap<String, VertexPropertyValue>,
) -> Result<ParsedMutationQuery> {
    unsafe {
        if sys::cypher_ast_create_is_unique(create_clause) {
            return unsupported("CREATE UNIQUE is not executable in Query engine");
        }
        let pattern = checked_node(sys::cypher_ast_create_get_pattern(create_clause))?;
        let path_count = sys::cypher_ast_pattern_npaths(pattern);
        if path_count == 0 {
            return unsupported("CREATE requires at least one relationship path");
        }
        let mut actions = Vec::with_capacity(path_count as usize);
        for index in 0..path_count {
            let path = checked_node(sys::cypher_ast_pattern_get_path(pattern, index))?;
            let edge = lower_create_edge_path(path, parameters, "CREATE")?;
            if edge.hop_range.is_some() {
                return unsupported(
                    "CREATE does not support variable-length relationships in Query engine",
                );
            }
            let src = edge
                .src
                .id
                .ok_or_else(|| unsupported_value("CREATE requires source id"))?;
            let dst = edge
                .dst
                .id
                .ok_or_else(|| unsupported_value("CREATE requires destination id"))?;
            actions.push(RowMutationAction::CreateEdge {
                edge_type: edge.edge_type.clone(),
                src,
                dst,
                src_metadata: vertex_metadata_from_node_pattern(&edge.src),
                dst_metadata: vertex_metadata_from_node_pattern(&edge.dst),
                edge_metadata: edge_metadata_from_edge_pattern(&edge),
            });
        }
        Ok(ParsedMutationQuery {
            patterns: Vec::new(),
            predicate: None,
            actions,
        })
    }
}

fn lower_delete_actions(delete_clause: *const AstNode) -> Result<Vec<RowMutationAction>> {
    unsafe {
        let detach = sys::cypher_ast_delete_has_detach(delete_clause);
        let expression_count = sys::cypher_ast_delete_nexpressions(delete_clause);
        if expression_count == 0 {
            return unsupported("DELETE requires at least one expression");
        }
        let mut actions = Vec::with_capacity(expression_count as usize);
        for idx in 0..expression_count {
            let expression =
                checked_node(sys::cypher_ast_delete_get_expression(delete_clause, idx))?;
            if is_instance(expression, sys::CYPHER_AST_IDENTIFIER) {
                actions.push(RowMutationAction::DeleteBinding {
                    binding: identifier_name(expression)?,
                    detach,
                });
            } else {
                return unsupported("DELETE currently supports node or relationship variables");
            }
        }
        Ok(actions)
    }
}

fn lower_set_actions(
    set_clause: *const AstNode,
    parameters: &BTreeMap<String, VertexPropertyValue>,
) -> Result<Vec<RowMutationAction>> {
    unsafe {
        let item_count = sys::cypher_ast_set_nitems(set_clause);
        if item_count == 0 {
            return unsupported("SET requires at least one item");
        }
        let mut actions = Vec::with_capacity(item_count as usize);
        for idx in 0..item_count {
            let item = checked_node(sys::cypher_ast_set_get_item(set_clause, idx))?;
            if is_instance(item, sys::CYPHER_AST_SET_PROPERTY) {
                let property = checked_node(sys::cypher_ast_set_property_get_property(item))?;
                let Some((binding, property)) = property_expression_binding(property)? else {
                    return unsupported("SET property requires <node>.<property>");
                };
                if property.eq_ignore_ascii_case("id") {
                    return unsupported("SET cannot update node id");
                }
                validate_component("property", &property)?;
                let expression = checked_node(sys::cypher_ast_set_property_get_expression(item))?;
                actions.push(RowMutationAction::SetProperty {
                    binding,
                    property,
                    value: scalar_property_value(expression, parameters)?,
                });
                continue;
            }
            if is_instance(item, sys::CYPHER_AST_SET_LABELS) {
                let binding = identifier_name(checked_node(
                    sys::cypher_ast_set_labels_get_identifier(item),
                )?)?;
                let mut labels = BTreeSet::new();
                for label_idx in 0..sys::cypher_ast_set_labels_nlabels(item) {
                    let label = label_name(checked_node(sys::cypher_ast_set_labels_get_label(
                        item, label_idx,
                    ))?)?;
                    validate_component("label", &label)?;
                    labels.insert(label);
                }
                if labels.is_empty() {
                    return unsupported("SET label item has no labels");
                }
                actions.push(RowMutationAction::SetLabels { binding, labels });
                continue;
            }
            if is_instance(item, sys::CYPHER_AST_SET_ALL_PROPERTIES)
                || is_instance(item, sys::CYPHER_AST_MERGE_PROPERTIES)
            {
                return unsupported(
                    "SET property-map replacement is not executable in Query engine",
                );
            }
            return unsupported(format!("unsupported SET item {}", node_type_name(item)));
        }
        Ok(actions)
    }
}

fn lower_remove_actions(remove_clause: *const AstNode) -> Result<Vec<RowMutationAction>> {
    unsafe {
        let item_count = sys::cypher_ast_remove_nitems(remove_clause);
        if item_count == 0 {
            return unsupported("REMOVE requires at least one item");
        }
        let mut actions = Vec::with_capacity(item_count as usize);
        for idx in 0..item_count {
            let item = checked_node(sys::cypher_ast_remove_get_item(remove_clause, idx))?;
            if is_instance(item, sys::CYPHER_AST_REMOVE_PROPERTY) {
                let property = checked_node(sys::cypher_ast_remove_property_get_property(item))?;
                let Some((binding, property)) = property_expression_binding(property)? else {
                    return unsupported("REMOVE property requires <node>.<property>");
                };
                if property.eq_ignore_ascii_case("id") {
                    return unsupported("REMOVE cannot remove node id");
                }
                validate_component("property", &property)?;
                actions.push(RowMutationAction::RemoveProperty { binding, property });
                continue;
            }
            if is_instance(item, sys::CYPHER_AST_REMOVE_LABELS) {
                let binding = identifier_name(checked_node(
                    sys::cypher_ast_remove_labels_get_identifier(item),
                )?)?;
                let mut labels = BTreeSet::new();
                for label_idx in 0..sys::cypher_ast_remove_labels_nlabels(item) {
                    let label = label_name(checked_node(
                        sys::cypher_ast_remove_labels_get_label(item, label_idx),
                    )?)?;
                    validate_component("label", &label)?;
                    labels.insert(label);
                }
                if labels.is_empty() {
                    return unsupported("REMOVE label item has no labels");
                }
                actions.push(RowMutationAction::RemoveLabels { binding, labels });
                continue;
            }
            return unsupported(format!("unsupported REMOVE item {}", node_type_name(item)));
        }
        Ok(actions)
    }
}

fn lower_return_window(
    return_clause: *const AstNode,
    parameters: &BTreeMap<String, VertexPropertyValue>,
) -> Result<QueryWindow> {
    unsafe {
        let skip_node = sys::cypher_ast_return_get_skip(return_clause);
        let skip = if skip_node.is_null() {
            0
        } else {
            window_u64_expression(checked_node(skip_node)?, "SKIP", parameters)?
        };

        let limit_node = sys::cypher_ast_return_get_limit(return_clause);
        let limit = if limit_node.is_null() {
            None
        } else {
            Some(window_usize_expression(
                checked_node(limit_node)?,
                "LIMIT",
                parameters,
            )?)
        };

        Ok(QueryWindow { skip, limit })
    }
}

fn lower_return_order_by(return_clause: *const AstNode) -> Result<Vec<RowSort>> {
    unsafe {
        let order_by = sys::cypher_ast_return_get_order_by(return_clause);
        if order_by.is_null() {
            return Ok(Vec::new());
        }
        ensure_instance(order_by, sys::CYPHER_AST_ORDER_BY, "ORDER BY")?;
        let item_count = sys::cypher_ast_order_by_nitems(order_by);
        let mut items = Vec::with_capacity(item_count as usize);
        for idx in 0..item_count {
            let item = checked_node(sys::cypher_ast_order_by_get_item(order_by, idx))?;
            ensure_instance(item, sys::CYPHER_AST_SORT_ITEM, "sort item")?;
            let expression = checked_node(sys::cypher_ast_sort_item_get_expression(item))?;
            items.push(RowSort {
                expression: lower_sort_expression(expression)?,
                ascending: sys::cypher_ast_sort_item_is_ascending(item),
            });
        }
        Ok(items)
    }
}

fn validate_distinct_order_by(
    order_by: &[RowSort],
    projections: &[RowProjection],
    columns: &[QueryColumn],
) -> Result<()> {
    for sort in order_by {
        if distinct_order_expression_is_projected(&sort.expression, projections, columns) {
            continue;
        }
        return unsupported(
            "ORDER BY expressions with RETURN DISTINCT must be projected by the RETURN clause",
        );
    }
    Ok(())
}

fn distinct_order_expression_is_projected(
    expression: &RowSortExpression,
    projections: &[RowProjection],
    columns: &[QueryColumn],
) -> bool {
    if let RowSortExpression::Column { name } = expression {
        return columns.iter().any(|column| column.name == *name);
    }
    projections
        .iter()
        .any(|projection| row_projection_matches_sort_expression(projection, expression))
}

fn row_projection_matches_sort_expression(
    projection: &RowProjection,
    expression: &RowSortExpression,
) -> bool {
    match (projection, expression) {
        (RowProjection::NodeId { binding: left }, RowSortExpression::NodeId { binding: right }) => {
            left == right
        }
        (
            RowProjection::Property {
                binding: left_binding,
                property: left_property,
            },
            RowSortExpression::Property {
                binding: right_binding,
                property: right_property,
            },
        ) => left_binding == right_binding && left_property == right_property,
        (RowProjection::CountAll, RowSortExpression::CountAll) => true,
        _ => false,
    }
}

fn lower_sort_expression(expression: *const AstNode) -> Result<RowSortExpression> {
    if is_count_star(expression)? {
        return Ok(RowSortExpression::CountAll);
    }
    if let Some(binding) = node_id_expression_binding(expression)? {
        return Ok(RowSortExpression::NodeId { binding });
    }
    if let Some((binding, property)) = property_expression_binding(expression)? {
        return Ok(RowSortExpression::Property { binding, property });
    }
    unsafe {
        if is_instance(expression, sys::CYPHER_AST_IDENTIFIER) {
            return Ok(RowSortExpression::Column {
                name: identifier_name(expression)?,
            });
        }
    }
    unsupported("ORDER BY currently supports projected aliases, <binding>.id, or count(*)")
}

fn lower_row_predicate(
    predicate: *const AstNode,
    parameters: &BTreeMap<String, VertexPropertyValue>,
) -> Result<RowPredicate> {
    unsafe {
        if is_instance(predicate, sys::CYPHER_AST_COMPARISON) {
            let length = sys::cypher_ast_comparison_get_length(predicate);
            if length == 0 {
                return unsupported("empty WHERE comparison is not executable");
            }
            let mut combined = None;
            for idx in 0..length {
                let left = checked_node(sys::cypher_ast_comparison_get_argument(predicate, idx))?;
                let right =
                    checked_node(sys::cypher_ast_comparison_get_argument(predicate, idx + 1))?;
                let op =
                    row_comparison_op(sys::cypher_ast_comparison_get_operator(predicate, idx))?;
                let next = RowPredicate::Compare {
                    left: lower_row_expression(left, parameters)?,
                    op,
                    right: lower_row_expression(right, parameters)?,
                };
                combined = Some(match combined {
                    Some(prev) => RowPredicate::And(Box::new(prev), Box::new(next)),
                    None => next,
                });
            }
            return combined.ok_or_else(|| unsupported_value("empty WHERE comparison"));
        }

        if is_instance(predicate, sys::CYPHER_AST_BINARY_OPERATOR) {
            let op = sys::cypher_ast_binary_operator_get_operator(predicate);
            let left = checked_node(sys::cypher_ast_binary_operator_get_argument1(predicate))?;
            let right = checked_node(sys::cypher_ast_binary_operator_get_argument2(predicate))?;
            if op == sys::CYPHER_OP_AND {
                return Ok(RowPredicate::And(
                    Box::new(lower_row_predicate(left, parameters)?),
                    Box::new(lower_row_predicate(right, parameters)?),
                ));
            }
            if op == sys::CYPHER_OP_OR {
                return Ok(RowPredicate::Or(
                    Box::new(lower_row_predicate(left, parameters)?),
                    Box::new(lower_row_predicate(right, parameters)?),
                ));
            }
            if op == sys::CYPHER_OP_STARTS_WITH {
                let RowExpression::Literal(VertexPropertyValue::String(prefix)) =
                    lower_row_expression(right, parameters)?
                else {
                    return unsupported("STARTS WITH requires a string literal or parameter");
                };
                return Ok(RowPredicate::StartsWith {
                    expression: lower_row_expression(left, parameters)?,
                    prefix,
                });
            }
            if let Ok(op) = row_comparison_op(op) {
                return Ok(RowPredicate::Compare {
                    left: lower_row_expression(left, parameters)?,
                    op,
                    right: lower_row_expression(right, parameters)?,
                });
            }
        }

        if is_instance(predicate, sys::CYPHER_AST_UNARY_OPERATOR) {
            let op = sys::cypher_ast_unary_operator_get_operator(predicate);
            if op == sys::CYPHER_OP_NOT {
                let arg = checked_node(sys::cypher_ast_unary_operator_get_argument(predicate))?;
                return Ok(RowPredicate::Not(Box::new(lower_row_predicate(
                    arg, parameters,
                )?)));
            }
        }
    }
    unsupported("WHERE currently supports boolean combinations of property comparisons")
}

fn and_row_predicates(left: Option<RowPredicate>, right: RowPredicate) -> RowPredicate {
    match left {
        Some(left) => RowPredicate::And(Box::new(left), Box::new(right)),
        None => right,
    }
}

fn row_comparison_op(op: *const sys::cypher_operator_t) -> Result<RowComparisonOp> {
    unsafe {
        if op == sys::CYPHER_OP_EQUAL {
            Ok(RowComparisonOp::Eq)
        } else if op == sys::CYPHER_OP_NEQUAL {
            Ok(RowComparisonOp::Ne)
        } else if op == sys::CYPHER_OP_LT {
            Ok(RowComparisonOp::Lt)
        } else if op == sys::CYPHER_OP_GT {
            Ok(RowComparisonOp::Gt)
        } else if op == sys::CYPHER_OP_LTE {
            Ok(RowComparisonOp::Lte)
        } else if op == sys::CYPHER_OP_GTE {
            Ok(RowComparisonOp::Gte)
        } else {
            unsupported("comparison operator is not executable in Query engine")
        }
    }
}

fn lower_row_expression(
    expression: *const AstNode,
    parameters: &BTreeMap<String, VertexPropertyValue>,
) -> Result<RowExpression> {
    if let Some(binding) = node_id_expression_binding(expression)? {
        return Ok(RowExpression::NodeId { binding });
    }
    if let Some((binding, property)) = property_expression_binding(expression)? {
        return Ok(RowExpression::Property { binding, property });
    }
    Ok(RowExpression::Literal(scalar_property_value(
        expression, parameters,
    )?))
}

fn lower_row_aggregate_expression(
    expression: *const AstNode,
    parameters: &BTreeMap<String, VertexPropertyValue>,
) -> Result<Option<(RowAggregateFunction, RowExpression, String)>> {
    unsafe {
        if !is_instance(expression, sys::CYPHER_AST_APPLY_OPERATOR) {
            return Ok(None);
        }
        if sys::cypher_ast_apply_operator_get_distinct(expression) {
            return unsupported("DISTINCT aggregate arguments are not executable in Query engine");
        }
        let function_node = checked_node(sys::cypher_ast_apply_operator_get_func_name(expression))?;
        let function_name = function_name(function_node)?;
        let Some(function) = row_aggregate_function(&function_name) else {
            return Ok(None);
        };
        let argument_count = sys::cypher_ast_apply_operator_narguments(expression);
        if argument_count != 1 {
            return unsupported(format!(
                "{} aggregate expects exactly one argument",
                aggregate_function_name(function)
            ));
        }
        let argument = checked_node(sys::cypher_ast_apply_operator_get_argument(expression, 0))?;
        let expression = lower_row_expression(argument, parameters)?;
        let fallback_name = format!(
            "{}({})",
            aggregate_function_name(function),
            row_expression_name(&expression)
        );
        Ok(Some((function, expression, fallback_name)))
    }
}

fn row_aggregate_function(name: &str) -> Option<RowAggregateFunction> {
    if name.eq_ignore_ascii_case("count") {
        Some(RowAggregateFunction::Count)
    } else if name.eq_ignore_ascii_case("sum") {
        Some(RowAggregateFunction::Sum)
    } else if name.eq_ignore_ascii_case("avg") {
        Some(RowAggregateFunction::Avg)
    } else if name.eq_ignore_ascii_case("collect") {
        Some(RowAggregateFunction::Collect)
    } else {
        None
    }
}

fn aggregate_function_name(function: RowAggregateFunction) -> &'static str {
    match function {
        RowAggregateFunction::Count => "count",
        RowAggregateFunction::Sum => "sum",
        RowAggregateFunction::Avg => "avg",
        RowAggregateFunction::Collect => "collect",
    }
}

fn row_expression_name(expression: &RowExpression) -> String {
    match expression {
        RowExpression::NodeId { binding } => format!("{binding}.id"),
        RowExpression::Property { binding, property } => format!("{binding}.{property}"),
        RowExpression::Literal(VertexPropertyValue::Integer(value)) => value.to_string(),
        RowExpression::Literal(VertexPropertyValue::SignedInteger(value)) => value.to_string(),
        RowExpression::Literal(VertexPropertyValue::Bool(value)) => value.to_string(),
        RowExpression::Literal(VertexPropertyValue::Float(value)) => value.0.to_string(),
        RowExpression::Literal(VertexPropertyValue::String(value)) => format!("'{value}'"),
    }
}

#[cfg(feature = "client-api")]
struct UnwindEdgeTemplate {
    edge_type: String,
    source_field: String,
    destination_field: Option<String>,
    destination_binding: Option<String>,
    relationship_binding: Option<String>,
}

#[cfg(feature = "client-api")]
fn unwind_edge_template(
    pattern: *const AstNode,
    unwind_alias: &str,
    require_destination_field: bool,
) -> Result<UnwindEdgeTemplate> {
    unsafe {
        ensure_instance(pattern, sys::CYPHER_AST_PATTERN, "UNWIND edge pattern")?;
        if sys::cypher_ast_pattern_npaths(pattern) != 1 {
            return unsupported("UNWIND batch requires exactly one edge pattern");
        }
        let path = checked_node(sys::cypher_ast_pattern_get_path(pattern, 0))?;
        ensure_instance(path, sys::CYPHER_AST_PATTERN_PATH, "UNWIND edge path")?;
        if sys::cypher_ast_pattern_path_nelements(path) != 3 {
            return unsupported(
                "UNWIND batch supports one-hop relationships only; send one batch per hop",
            );
        }
        let left = checked_node(sys::cypher_ast_pattern_path_get_element(path, 0))?;
        let relationship = checked_node(sys::cypher_ast_pattern_path_get_element(path, 1))?;
        let right = checked_node(sys::cypher_ast_pattern_path_get_element(path, 2))?;
        ensure_instance(left, sys::CYPHER_AST_NODE_PATTERN, "UNWIND source node")?;
        ensure_instance(
            relationship,
            sys::CYPHER_AST_REL_PATTERN,
            "UNWIND relationship",
        )?;
        ensure_instance(
            right,
            sys::CYPHER_AST_NODE_PATTERN,
            "UNWIND destination node",
        )?;
        if !sys::cypher_ast_rel_pattern_get_varlength(relationship).is_null()
            || !sys::cypher_ast_rel_pattern_get_properties(relationship).is_null()
            || sys::cypher_ast_rel_pattern_nreltypes(relationship) != 1
        {
            return unsupported(
                "UNWIND batch requires one fixed relationship type without properties",
            );
        }
        let edge_type = reltype_name(checked_node(sys::cypher_ast_rel_pattern_get_reltype(
            relationship,
            0,
        ))?)?;
        let relationship_binding = rel_identifier(relationship)?;
        let left_field = unwind_node_id_field(left, unwind_alias, true)?;
        let right_field = unwind_node_id_field(right, unwind_alias, require_destination_field)?;
        let left_binding = node_identifier(left)?;
        let right_binding = node_identifier(right)?;
        match sys::cypher_ast_rel_pattern_get_direction(relationship) {
            sys::cypher_rel_direction::CYPHER_REL_OUTBOUND => Ok(UnwindEdgeTemplate {
                edge_type,
                source_field: left_field
                    .ok_or_else(|| unsupported_value("UNWIND source requires an id field"))?,
                destination_field: right_field,
                destination_binding: right_binding,
                relationship_binding,
            }),
            sys::cypher_rel_direction::CYPHER_REL_INBOUND => Ok(UnwindEdgeTemplate {
                edge_type,
                source_field: right_field
                    .ok_or_else(|| unsupported_value("UNWIND source requires an id field"))?,
                destination_field: left_field,
                destination_binding: left_binding,
                relationship_binding,
            }),
            sys::cypher_rel_direction::CYPHER_REL_BIDIRECTIONAL => {
                unsupported("UNWIND batch does not support undirected relationships")
            }
        }
    }
}

#[cfg(feature = "client-api")]
fn unwind_node_id_field(
    node: *const AstNode,
    unwind_alias: &str,
    required: bool,
) -> Result<Option<String>> {
    unsafe {
        if sys::cypher_ast_node_pattern_nlabels(node) != 0 {
            // Labelled batch writes do exist, just not in this shape. A message that only
            // says "no labels" reads as "no labelled batch path at all", and the way out
            // of that reading is one round trip per edge, so name the shapes that work.
            return unsupported(
                "UNWIND batch node patterns match by id alone; label a vertex in the SET \
                 of an UNWIND MERGE upsert, and label relationship endpoints in an \
                 UNWIND MATCH that binds them for CREATE or MERGE",
            );
        }
        let properties = sys::cypher_ast_node_pattern_get_properties(node);
        if properties.is_null() {
            if required {
                return unsupported("UNWIND batch node requires an id property");
            }
            return Ok(None);
        }
        ensure_instance(properties, sys::CYPHER_AST_MAP, "UNWIND node properties")?;
        if sys::cypher_ast_map_nentries(properties) != 1 {
            return unsupported("UNWIND batch node supports only the id property");
        }
        let key = prop_name(checked_node(sys::cypher_ast_map_get_key(properties, 0))?)?;
        if key != "id" {
            return unsupported("UNWIND batch node property must be id");
        }
        let value = checked_node(sys::cypher_ast_map_get_value(properties, 0))?;
        let Some((binding, field)) = property_expression_binding(value)? else {
            return unsupported("UNWIND batch node id must read a field from the row map");
        };
        if binding != unwind_alias {
            return unsupported("UNWIND batch node id references the wrong row alias");
        }
        Ok(Some(field))
    }
}

fn lower_create_edge_path(
    path: *const AstNode,
    parameters: &BTreeMap<String, VertexPropertyValue>,
    clause: &str,
) -> Result<RowEdgePattern> {
    unsafe {
        ensure_instance(path, sys::CYPHER_AST_PATTERN_PATH, "pattern path")?;
        if sys::cypher_ast_pattern_path_nelements(path) != 3 {
            return unsupported(format!(
                "only one-hop edge patterns are executable in Query engine {clause}"
            ));
        }

        let left = checked_node(sys::cypher_ast_pattern_path_get_element(path, 0))?;
        let rel = checked_node(sys::cypher_ast_pattern_path_get_element(path, 1))?;
        let right = checked_node(sys::cypher_ast_pattern_path_get_element(path, 2))?;
        ensure_instance(left, sys::CYPHER_AST_NODE_PATTERN, "left node pattern")?;
        ensure_instance(rel, sys::CYPHER_AST_REL_PATTERN, "relationship pattern")?;
        ensure_instance(right, sys::CYPHER_AST_NODE_PATTERN, "right node pattern")?;

        let varlength = sys::cypher_ast_rel_pattern_get_varlength(rel);
        let hop_range = if varlength.is_null() {
            None
        } else {
            Some(lower_hop_range(varlength)?)
        };
        let properties = relationship_properties(rel, parameters)?;
        if sys::cypher_ast_rel_pattern_nreltypes(rel) != 1 {
            return unsupported(format!(
                "relationship pattern must have exactly one type in Query engine {clause}"
            ));
        }

        let edge_type_node = checked_node(sys::cypher_ast_rel_pattern_get_reltype(rel, 0))?;
        let edge_type = reltype_name(edge_type_node)?;
        let binding = rel_identifier(rel)?;
        let left_node = lower_create_node_pattern(left, parameters)?;
        let right_node = lower_create_node_pattern(right, parameters)?;

        match sys::cypher_ast_rel_pattern_get_direction(rel) {
            sys::cypher_rel_direction::CYPHER_REL_OUTBOUND => Ok(RowEdgePattern {
                binding,
                edge_type,
                src: left_node,
                dst: right_node,
                properties,
                hop_range,
            }),
            sys::cypher_rel_direction::CYPHER_REL_INBOUND => Ok(RowEdgePattern {
                binding,
                edge_type,
                src: right_node,
                dst: left_node,
                properties,
                hop_range,
            }),
            sys::cypher_rel_direction::CYPHER_REL_BIDIRECTIONAL => unsupported(format!(
                "undirected relationships are not executable in Query engine {clause}"
            )),
        }
    }
}

fn lower_row_patterns(
    pattern: *const AstNode,
    parameters: &BTreeMap<String, VertexPropertyValue>,
) -> Result<Vec<RowPattern>> {
    unsafe {
        ensure_instance(pattern, sys::CYPHER_AST_PATTERN, "pattern")?;
        let path_count = sys::cypher_ast_pattern_npaths(pattern);
        if path_count == 0 {
            return unsupported("MATCH requires at least one path pattern");
        }

        let mut patterns = Vec::new();
        for path_idx in 0..path_count {
            let path = checked_node(sys::cypher_ast_pattern_get_path(pattern, path_idx))?;
            ensure_instance(path, sys::CYPHER_AST_PATTERN_PATH, "pattern path")?;
            let element_count = sys::cypher_ast_pattern_path_nelements(path);
            match element_count {
                1 => {
                    let node = checked_node(sys::cypher_ast_pattern_path_get_element(path, 0))?;
                    ensure_instance(node, sys::CYPHER_AST_NODE_PATTERN, "node pattern")?;
                    patterns.push(RowPattern::Node(lower_row_node_pattern(node, parameters)?));
                }
                count if count >= 3 && count % 2 == 1 => {
                    let edge_count = (count - 1) / 2;
                    for edge_idx in 0..edge_count {
                        patterns.push(RowPattern::Edge(lower_row_edge_path_segment(
                            path, edge_idx, parameters,
                        )?));
                    }
                }
                _ => {
                    return unsupported(
                        "MATCH paths must alternate node and relationship patterns in Query engine",
                    );
                }
            }
        }
        Ok(patterns)
    }
}

fn lower_create_node_pattern(
    node: *const AstNode,
    parameters: &BTreeMap<String, VertexPropertyValue>,
) -> Result<RowNodePattern> {
    unsafe {
        ensure_instance(node, sys::CYPHER_AST_NODE_PATTERN, "node pattern")?;
        let binding = node_identifier(node)?;
        let mut labels = BTreeSet::new();
        for idx in 0..sys::cypher_ast_node_pattern_nlabels(node) {
            let label = checked_node(sys::cypher_ast_node_pattern_get_label(node, idx))?;
            let label = label_name(label)?;
            validate_component("label", &label)?;
            labels.insert(label);
        }
        let properties = sys::cypher_ast_node_pattern_get_properties(node);
        let properties = if properties.is_null() {
            BTreeMap::new()
        } else {
            row_node_properties(properties, parameters)?
        };
        let id = match properties.get("id") {
            Some(VertexPropertyValue::Integer(id)) => Some(*id),
            Some(_) => return unsupported("node id property must be an integer"),
            None => None,
        };
        Ok(RowNodePattern {
            binding,
            id,
            labels,
            properties,
        })
    }
}

fn lower_row_edge_path_segment(
    path: *const AstNode,
    edge_idx: u32,
    parameters: &BTreeMap<String, VertexPropertyValue>,
) -> Result<RowEdgePattern> {
    unsafe {
        let left_idx = edge_idx.saturating_mul(2);
        let rel_idx = left_idx + 1;
        let right_idx = left_idx + 2;
        let left = checked_node(sys::cypher_ast_pattern_path_get_element(path, left_idx))?;
        let rel = checked_node(sys::cypher_ast_pattern_path_get_element(path, rel_idx))?;
        let right = checked_node(sys::cypher_ast_pattern_path_get_element(path, right_idx))?;
        ensure_instance(left, sys::CYPHER_AST_NODE_PATTERN, "left node pattern")?;
        ensure_instance(rel, sys::CYPHER_AST_REL_PATTERN, "relationship pattern")?;
        ensure_instance(right, sys::CYPHER_AST_NODE_PATTERN, "right node pattern")?;

        let varlength = sys::cypher_ast_rel_pattern_get_varlength(rel);
        let hop_range = if varlength.is_null() {
            None
        } else {
            Some(lower_hop_range(varlength)?)
        };
        let properties = relationship_properties(rel, parameters)?;
        if sys::cypher_ast_rel_pattern_nreltypes(rel) != 1 {
            return unsupported("relationship pattern must have exactly one type in Query engine");
        }

        let edge_type_node = checked_node(sys::cypher_ast_rel_pattern_get_reltype(rel, 0))?;
        let edge_type = reltype_name(edge_type_node)?;
        let binding = rel_identifier(rel)?;
        if binding.is_some() && hop_range.is_some() {
            return unsupported(
                "variable-length relationship bindings are not executable in Query engine",
            );
        }
        let left_node = lower_row_node_pattern(left, parameters)?;
        let right_node = lower_row_node_pattern(right, parameters)?;

        match sys::cypher_ast_rel_pattern_get_direction(rel) {
            sys::cypher_rel_direction::CYPHER_REL_OUTBOUND => Ok(RowEdgePattern {
                binding,
                edge_type,
                src: left_node,
                dst: right_node,
                properties,
                hop_range,
            }),
            sys::cypher_rel_direction::CYPHER_REL_INBOUND => Ok(RowEdgePattern {
                binding,
                edge_type,
                src: right_node,
                dst: left_node,
                properties,
                hop_range,
            }),
            sys::cypher_rel_direction::CYPHER_REL_BIDIRECTIONAL => {
                unsupported("undirected relationships are not executable in Query engine")
            }
        }
    }
}

fn vertex_metadata_from_node_pattern(node: &RowNodePattern) -> VertexMetadata {
    VertexMetadata {
        labels: node.labels.clone(),
        properties: node
            .properties
            .iter()
            .filter(|(property, _)| property.as_str() != "id")
            .map(|(property, value)| (property.clone(), value.clone()))
            .collect(),
    }
}

fn edge_metadata_from_edge_pattern(edge: &RowEdgePattern) -> EdgeMetadata {
    EdgeMetadata {
        properties: edge.properties.clone(),
    }
}

fn relationship_properties(
    rel: *const AstNode,
    parameters: &BTreeMap<String, VertexPropertyValue>,
) -> Result<BTreeMap<String, VertexPropertyValue>> {
    unsafe {
        let properties = sys::cypher_ast_rel_pattern_get_properties(rel);
        if properties.is_null() {
            Ok(BTreeMap::new())
        } else {
            property_map(properties, parameters, "relationship property map")
        }
    }
}

fn lower_hop_range(range: *const AstNode) -> Result<(u8, u8)> {
    unsafe {
        ensure_instance(range, sys::CYPHER_AST_RANGE, "variable-length range")?;
        let start = sys::cypher_ast_range_get_start(range);
        let end = sys::cypher_ast_range_get_end(range);
        let min_hops = if start.is_null() {
            1
        } else {
            integer_u8(start, "minimum hop count")?
        };
        if end.is_null() {
            return unsupported("unbounded variable-length MATCH requires an explicit max hop");
        }
        let max_hops = integer_u8(end, "maximum hop count")?;
        if min_hops > max_hops {
            return unsupported("invalid variable-length hop range");
        }
        Ok((min_hops, max_hops))
    }
}

fn lower_row_node_pattern(
    node: *const AstNode,
    parameters: &BTreeMap<String, VertexPropertyValue>,
) -> Result<RowNodePattern> {
    unsafe {
        ensure_instance(node, sys::CYPHER_AST_NODE_PATTERN, "node pattern")?;
        let binding = node_identifier(node)?;
        let mut labels = BTreeSet::new();
        for idx in 0..sys::cypher_ast_node_pattern_nlabels(node) {
            let label = checked_node(sys::cypher_ast_node_pattern_get_label(node, idx))?;
            let label = label_name(label)?;
            validate_component("label", &label)?;
            labels.insert(label);
        }
        let properties = sys::cypher_ast_node_pattern_get_properties(node);
        let properties = if properties.is_null() {
            BTreeMap::new()
        } else {
            row_node_properties(properties, parameters)?
        };
        let id = match properties.get("id") {
            Some(VertexPropertyValue::Integer(id)) => Some(*id),
            Some(_) => return unsupported("node id property must be an integer"),
            None => None,
        };
        if binding.is_none()
            && (!labels.is_empty() || properties.keys().any(|property| property != "id"))
        {
            return unsupported("node labels and non-id properties require a named node");
        }
        Ok(RowNodePattern {
            binding,
            id,
            labels,
            properties,
        })
    }
}

fn row_node_properties(
    properties: *const AstNode,
    parameters: &BTreeMap<String, VertexPropertyValue>,
) -> Result<BTreeMap<String, VertexPropertyValue>> {
    property_map(properties, parameters, "node property map")
}

fn property_map(
    properties: *const AstNode,
    parameters: &BTreeMap<String, VertexPropertyValue>,
    expected: &str,
) -> Result<BTreeMap<String, VertexPropertyValue>> {
    unsafe {
        ensure_instance(properties, sys::CYPHER_AST_MAP, expected)?;
        let mut result = BTreeMap::new();
        for idx in 0..sys::cypher_ast_map_nentries(properties) {
            let key = checked_node(sys::cypher_ast_map_get_key(properties, idx))?;
            let key = prop_name(key)?;
            validate_component("property", &key)?;
            let value = checked_node(sys::cypher_ast_map_get_value(properties, idx))?;
            if result
                .insert(key, scalar_property_value(value, parameters)?)
                .is_some()
            {
                return unsupported("duplicate property in map");
            }
        }
        Ok(result)
    }
}

fn scalar_property_value(
    node: *const AstNode,
    parameters: &BTreeMap<String, VertexPropertyValue>,
) -> Result<VertexPropertyValue> {
    unsafe {
        if is_instance(node, sys::CYPHER_AST_PARAMETER) {
            return parameter_value(node, parameters).cloned();
        }
        if is_instance(node, sys::CYPHER_AST_INTEGER) {
            return Ok(VertexPropertyValue::Integer(integer_vertex_id(
                node, parameters,
            )?));
        }
        if is_instance(node, sys::CYPHER_AST_FLOAT) {
            let value = c_string(sys::cypher_ast_float_get_valuestr(node));
            return value
                .parse::<f64>()
                .map(|value| VertexPropertyValue::Float(QueryFloat(value)))
                .map_err(|err| parse_error(format!("invalid float literal {value}: {err}")));
        }
        if is_instance(node, sys::CYPHER_AST_STRING) {
            return Ok(VertexPropertyValue::String(c_string(
                sys::cypher_ast_string_get_value(node),
            )));
        }
        if is_instance(node, sys::CYPHER_AST_TRUE) {
            return Ok(VertexPropertyValue::Bool(true));
        }
        if is_instance(node, sys::CYPHER_AST_FALSE) {
            return Ok(VertexPropertyValue::Bool(false));
        }
        if is_instance(node, sys::CYPHER_AST_UNARY_OPERATOR) {
            let op = sys::cypher_ast_unary_operator_get_operator(node);
            let arg = checked_node(sys::cypher_ast_unary_operator_get_argument(node))?;
            let value = scalar_property_value(arg, parameters)?;
            if op == sys::CYPHER_OP_UNARY_PLUS {
                return match value {
                    VertexPropertyValue::Integer(_)
                    | VertexPropertyValue::SignedInteger(_)
                    | VertexPropertyValue::Float(_) => Ok(value),
                    VertexPropertyValue::Bool(_) | VertexPropertyValue::String(_) => {
                        unsupported("unary plus requires a numeric property value")
                    }
                };
            }
            if op == sys::CYPHER_OP_UNARY_MINUS {
                return match value {
                    VertexPropertyValue::Float(value) => {
                        Ok(VertexPropertyValue::Float(QueryFloat(-value.0)))
                    }
                    VertexPropertyValue::Integer(0) => {
                        Ok(VertexPropertyValue::Float(QueryFloat(-0.0)))
                    }
                    VertexPropertyValue::Integer(value) => {
                        let value = if value == (1_u64 << 63) {
                            Some(i64::MIN)
                        } else {
                            i64::try_from(value).ok().and_then(i64::checked_neg)
                        };
                        value
                            .map(VertexPropertyValue::SignedInteger)
                            .ok_or_else(|| GraphError::UnsupportedQuery {
                                dialect: "OpenCypher",
                                feature: "integer literal exceeds the signed 64-bit range"
                                    .to_string(),
                            })
                    }
                    VertexPropertyValue::SignedInteger(value) => {
                        Ok(VertexPropertyValue::Integer(value.unsigned_abs()))
                    }
                    VertexPropertyValue::Bool(_) | VertexPropertyValue::String(_) => {
                        unsupported("unary minus requires a numeric property value")
                    }
                };
            }
            return unsupported("property value unary operator must be plus or minus");
        }
    }
    unsupported("property values support integer, float, boolean, and string literals")
}

fn integer_vertex_id(
    node: *const AstNode,
    parameters: &BTreeMap<String, VertexPropertyValue>,
) -> Result<VertexId> {
    unsafe {
        if is_instance(node, sys::CYPHER_AST_PARAMETER) {
            let name = parameter_name(node)?;
            return match parameter_value_by_name(&name, parameters)? {
                VertexPropertyValue::Integer(value) => Ok(*value),
                VertexPropertyValue::SignedInteger(_) => {
                    unsupported(format!("parameter ${name} cannot be a negative node id"))
                }
                _ => unsupported(format!("parameter ${name} must be an integer")),
            };
        }
        ensure_instance(node, sys::CYPHER_AST_INTEGER, "integer literal")?;
        let value = c_string(sys::cypher_ast_integer_get_valuestr(node));
        if value.starts_with('-') {
            return unsupported("node id cannot be negative");
        }
        value
            .parse::<VertexId>()
            .map_err(|err| parse_error(format!("invalid node id integer literal {value}: {err}")))
    }
}

fn window_u64_expression(
    node: *const AstNode,
    field: &str,
    parameters: &BTreeMap<String, VertexPropertyValue>,
) -> Result<u64> {
    let value = constant_integer_expression(node, field, parameters)?;
    u64::try_from(value).map_err(|_| unsupported_value(format!("{field} cannot be negative")))
}

fn window_usize_expression(
    node: *const AstNode,
    field: &str,
    parameters: &BTreeMap<String, VertexPropertyValue>,
) -> Result<usize> {
    let value = window_u64_expression(node, field, parameters)?;
    usize::try_from(value).map_err(|_| unsupported_value(format!("{field} exceeds platform usize")))
}

fn constant_integer_expression(
    node: *const AstNode,
    field: &str,
    parameters: &BTreeMap<String, VertexPropertyValue>,
) -> Result<i128> {
    unsafe {
        if is_instance(node, sys::CYPHER_AST_PARAMETER) {
            let name = parameter_name(node)?;
            return match parameter_value_by_name(&name, parameters)? {
                VertexPropertyValue::Integer(value) => Ok(i128::from(*value)),
                VertexPropertyValue::SignedInteger(value) => Ok(i128::from(*value)),
                _ => unsupported(format!("{field} parameter ${name} must be an integer")),
            };
        }
        if is_instance(node, sys::CYPHER_AST_INTEGER) {
            let value = c_string(sys::cypher_ast_integer_get_valuestr(node));
            return value.parse::<i128>().map_err(|err| {
                parse_error(format!("invalid {field} integer literal {value}: {err}"))
            });
        }

        if is_instance(node, sys::CYPHER_AST_UNARY_OPERATOR) {
            let op = sys::cypher_ast_unary_operator_get_operator(node);
            let arg = checked_node(sys::cypher_ast_unary_operator_get_argument(node))?;
            let value = constant_integer_expression(arg, field, parameters)?;
            if op == sys::CYPHER_OP_UNARY_PLUS {
                return Ok(value);
            }
            if op == sys::CYPHER_OP_UNARY_MINUS {
                return value.checked_neg().ok_or_else(|| {
                    unsupported_value(format!("{field} constant expression overflowed"))
                });
            }
            return unsupported(format!("{field} supports only constant integer arithmetic"));
        }

        if is_instance(node, sys::CYPHER_AST_BINARY_OPERATOR) {
            let op = sys::cypher_ast_binary_operator_get_operator(node);
            let left = checked_node(sys::cypher_ast_binary_operator_get_argument1(node))?;
            let right = checked_node(sys::cypher_ast_binary_operator_get_argument2(node))?;
            let left = constant_integer_expression(left, field, parameters)?;
            let right = constant_integer_expression(right, field, parameters)?;
            let value = if op == sys::CYPHER_OP_PLUS {
                left.checked_add(right)
            } else if op == sys::CYPHER_OP_MINUS {
                left.checked_sub(right)
            } else if op == sys::CYPHER_OP_MULT {
                left.checked_mul(right)
            } else if op == sys::CYPHER_OP_DIV {
                if right == 0 {
                    return unsupported(format!("{field} division by zero"));
                }
                left.checked_div(right)
            } else if op == sys::CYPHER_OP_MOD {
                if right == 0 {
                    return unsupported(format!("{field} modulo by zero"));
                }
                left.checked_rem(right)
            } else {
                return unsupported(format!("{field} supports only constant integer arithmetic"));
            };
            return value.ok_or_else(|| {
                unsupported_value(format!("{field} constant expression overflowed"))
            });
        }

        unsupported(format!("{field} supports only constant integer arithmetic"))
    }
}

fn integer_u8(node: *const AstNode, field: &str) -> Result<u8> {
    let value = integer_vertex_id(node, &BTreeMap::new())?;
    u8::try_from(value).map_err(|_| unsupported_value(format!("{field} exceeds 255")))
}

fn is_count_star(expression: *const AstNode) -> Result<bool> {
    unsafe {
        if !is_instance(expression, sys::CYPHER_AST_APPLY_ALL_OPERATOR) {
            return Ok(false);
        }
        if sys::cypher_ast_apply_all_operator_get_distinct(expression) {
            return unsupported("count(DISTINCT *) is not executable in the query engine");
        }

        let function = checked_node(sys::cypher_ast_apply_all_operator_get_func_name(expression))?;
        let function = function_name(function)?;
        Ok(function.eq_ignore_ascii_case("count"))
    }
}

fn node_id_expression_binding(expression: *const AstNode) -> Result<Option<String>> {
    match property_expression_binding(expression)? {
        Some((binding, property)) if property.eq_ignore_ascii_case("id") => Ok(Some(binding)),
        _ => Ok(None),
    }
}

fn property_expression_binding(expression: *const AstNode) -> Result<Option<(String, String)>> {
    unsafe {
        if !is_instance(expression, sys::CYPHER_AST_PROPERTY_OPERATOR) {
            return Ok(None);
        }

        let prop = checked_node(sys::cypher_ast_property_operator_get_prop_name(expression))?;
        let property = prop_name(prop)?;

        let base = checked_node(sys::cypher_ast_property_operator_get_expression(expression))?;
        if !is_instance(base, sys::CYPHER_AST_IDENTIFIER) {
            return Ok(None);
        }
        Ok(Some((identifier_name(base)?, property)))
    }
}

fn projection_column_name(
    projection: *const AstNode,
    fallback: impl Into<String>,
) -> Result<String> {
    unsafe {
        let alias = sys::cypher_ast_projection_get_alias(projection);
        if alias.is_null() {
            Ok(fallback.into())
        } else {
            identifier_name(alias)
        }
    }
}

fn node_identifier(node: *const AstNode) -> Result<Option<String>> {
    unsafe {
        let ident = sys::cypher_ast_node_pattern_get_identifier(node);
        if ident.is_null() {
            Ok(None)
        } else {
            Ok(Some(identifier_name(ident)?))
        }
    }
}

fn rel_identifier(rel: *const AstNode) -> Result<Option<String>> {
    unsafe {
        let ident = sys::cypher_ast_rel_pattern_get_identifier(rel);
        if ident.is_null() {
            Ok(None)
        } else {
            Ok(Some(identifier_name(ident)?))
        }
    }
}

fn identifier_name(node: *const AstNode) -> Result<String> {
    unsafe {
        ensure_instance(node, sys::CYPHER_AST_IDENTIFIER, "identifier")?;
        Ok(c_string(sys::cypher_ast_identifier_get_name(node)))
    }
}

fn prop_name(node: *const AstNode) -> Result<String> {
    unsafe {
        ensure_instance(node, sys::CYPHER_AST_PROP_NAME, "property name")?;
        Ok(c_string(sys::cypher_ast_prop_name_get_value(node)))
    }
}

fn label_name(node: *const AstNode) -> Result<String> {
    unsafe {
        ensure_instance(node, sys::CYPHER_AST_LABEL, "label")?;
        Ok(c_string(sys::cypher_ast_label_get_name(node)))
    }
}

fn reltype_name(node: *const AstNode) -> Result<String> {
    unsafe {
        ensure_instance(node, sys::CYPHER_AST_RELTYPE, "relationship type")?;
        Ok(c_string(sys::cypher_ast_reltype_get_name(node)))
    }
}

fn function_name(node: *const AstNode) -> Result<String> {
    unsafe {
        ensure_instance(node, sys::CYPHER_AST_FUNCTION_NAME, "function name")?;
        Ok(c_string(sys::cypher_ast_function_name_get_value(node)))
    }
}

fn parameter_name(node: *const AstNode) -> Result<String> {
    unsafe {
        ensure_instance(node, sys::CYPHER_AST_PARAMETER, "parameter")?;
        let name = c_string(sys::cypher_ast_parameter_get_name(node));
        Ok(name.trim_start_matches('$').to_string())
    }
}

fn parameter_value(
    node: *const AstNode,
    parameters: &BTreeMap<String, VertexPropertyValue>,
) -> Result<&VertexPropertyValue> {
    let name = parameter_name(node)?;
    parameter_value_by_name(&name, parameters)
}

fn parameter_value_by_name<'a>(
    name: &str,
    parameters: &'a BTreeMap<String, VertexPropertyValue>,
) -> Result<&'a VertexPropertyValue> {
    parameters
        .get(name)
        .or_else(|| parameters.get(&format!("${name}")))
        .ok_or_else(|| GraphError::MissingQueryParameter {
            dialect: "OpenCypher",
            name: name.to_string(),
        })
}

fn checked_node(node: *const AstNode) -> Result<*const AstNode> {
    if node.is_null() {
        Err(parse_error("libcypher-parser returned a null AST node"))
    } else {
        Ok(node)
    }
}

fn ensure_instance(
    node: *const AstNode,
    node_type: sys::cypher_astnode_type_t,
    expected: &str,
) -> Result<()> {
    if is_instance(node, node_type) {
        Ok(())
    } else {
        Err(parse_error(format!(
            "expected {expected}, got {}",
            node_type_name(node)
        )))
    }
}

fn is_instance(node: *const AstNode, node_type: sys::cypher_astnode_type_t) -> bool {
    if node.is_null() {
        false
    } else {
        unsafe { sys::cypher_astnode_instanceof(node, node_type) }
    }
}

fn node_type_name(node: *const AstNode) -> String {
    if node.is_null() {
        return "null".to_string();
    }
    unsafe {
        let node_type = sys::cypher_astnode_type(node);
        c_string(sys::cypher_astnode_typestr(node_type))
    }
}

fn c_string(value: *const std::os::raw::c_char) -> String {
    if value.is_null() {
        String::new()
    } else {
        unsafe { CStr::from_ptr(value).to_string_lossy().into_owned() }
    }
}

fn unsupported<T>(feature: impl Into<String>) -> Result<T> {
    Err(unsupported_value(feature))
}

fn unsupported_value(feature: impl Into<String>) -> GraphError {
    GraphError::UnsupportedQuery {
        dialect: "OpenCypher",
        feature: feature.into(),
    }
}

fn parse_error(reason: impl Into<String>) -> GraphError {
    GraphError::QueryParse {
        dialect: "OpenCypher",
        reason: reason.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lowers_row_query_with_multiple_projections_where_and_order() {
        let parsed = parse_opencypher_row_query(
            "MATCH (u {id: 1})-[:FOLLOWS]->(v) \
             WHERE v.id >= 10 AND v.id <> 12 \
             RETURN u.id AS src, v.id AS dst ORDER BY dst DESC SKIP 1 LIMIT 2",
        )
        .unwrap();
        assert_eq!(
            parsed.columns,
            vec![QueryColumn::new("src"), QueryColumn::new("dst")]
        );
        assert_eq!(
            parsed.projections,
            vec![
                RowProjection::NodeId {
                    binding: "u".to_string(),
                },
                RowProjection::NodeId {
                    binding: "v".to_string(),
                },
            ]
        );
        assert_eq!(
            parsed.order_by,
            vec![RowSort {
                expression: RowSortExpression::Column {
                    name: "dst".to_string(),
                },
                ascending: false,
            }]
        );
        assert_eq!(
            parsed.window,
            QueryWindow {
                skip: 1,
                limit: Some(2),
            }
        );
    }

    #[test]
    fn lowers_starts_with_string_predicate() {
        let parameters = BTreeMap::from([(
            "prefix".to_string(),
            VertexPropertyValue::String("thread-".to_string()),
        )]);
        let parsed = parse_opencypher_row_query_with_parameters(
            "MATCH (s:Source) WHERE s.thread_id STARTS WITH $prefix RETURN s.id",
            &parameters,
        )
        .unwrap();
        assert!(matches!(
            parsed.predicate,
            Some(RowPredicate::StartsWith {
                expression: RowExpression::Property { ref binding, ref property },
                ref prefix,
            }) if binding == "s" && property == "thread_id" && prefix == "thread-"
        ));
    }

    #[test]
    fn cached_cypher_ast_lowers_each_requests_parameters() {
        let query = "MATCH (s:Source) WHERE s.thread_id STARTS WITH $prefix RETURN s.id";
        for expected in ["first-", "second-"] {
            let parameters = BTreeMap::from([(
                "prefix".to_string(),
                VertexPropertyValue::String(expected.to_string()),
            )]);
            let parsed = parse_opencypher_row_query_with_parameters(query, &parameters).unwrap();
            assert!(matches!(
                parsed.predicate,
                Some(RowPredicate::StartsWith { ref prefix, .. }) if prefix == expected
            ));
        }
    }

    #[cfg(feature = "client-api")]
    #[test]
    fn lowers_unwind_detach_delete_vertex_batch() {
        let parsed = parse_opencypher_unwind_batch(
            "UNWIND $vertices AS row MATCH (n {id: row.vertex}) DETACH DELETE n",
        )
        .unwrap()
        .unwrap();
        assert_eq!(parsed.parameter, "vertices");
        assert_eq!(
            parsed.kind,
            ParsedUnwindBatchKind::DeleteVertices {
                vertex_field: "vertex".to_string(),
                detach: true,
            }
        );
    }

    #[cfg(feature = "client-api")]
    #[test]
    fn lowers_unwind_relationship_property_delete_batch() {
        let parsed = parse_opencypher_unwind_batch(
            "UNWIND $rows AS row MATCH ()-[r:RELATES {chunk_id: row.chunk_id}]->() DELETE r",
        )
        .unwrap()
        .unwrap();
        assert_eq!(parsed.parameter, "rows");
        assert_eq!(
            parsed.kind,
            ParsedUnwindBatchKind::DeleteRelationshipsByProperty {
                edge_type: "RELATES".to_string(),
                property: "chunk_id".to_string(),
                value_field: "chunk_id".to_string(),
            }
        );
    }

    #[cfg(feature = "client-api")]
    #[test]
    fn lowers_unwind_create_between_matched_labeled_vertices() {
        let parsed = parse_opencypher_unwind_batch(
            "UNWIND $rows AS row \
             MATCH (s:Source {id: row.source_vertex}), \
                   (r:Source {id: row.related_vertex}) \
             CREATE (s)-[:FORCEFUL_RELATION]->(r)",
        )
        .unwrap()
        .unwrap();
        assert_eq!(parsed.parameter, "rows");
        assert_eq!(
            parsed.kind,
            ParsedUnwindBatchKind::CreateEdgesBetweenLabeledVertices {
                edge_type: "FORCEFUL_RELATION".to_string(),
                source_field: "source_vertex".to_string(),
                destination_field: "related_vertex".to_string(),
                source_label: "Source".to_string(),
                destination_label: "Source".to_string(),
            }
        );
    }

    #[cfg(feature = "client-api")]
    #[test]
    fn labeled_unwind_create_names_the_labeled_batch_forms() {
        // The labelled forms this points at are the ones covered by
        // lowers_unwind_vertex_upsert_batch and
        // lowers_unwind_create_between_matched_labeled_vertices, so a caller who
        // follows the message lands on a batch that parses.
        let err = parse_opencypher_unwind_batch(
            "UNWIND $rows AS row \
             CREATE (s:Source {id: row.source_vertex})-[:RELATES]->(d:Source {id: row.related_vertex})",
        )
        .unwrap_err();
        let message = err.to_string();
        assert!(message.contains("match by id alone"), "{message}");
        assert!(message.contains("SET of an UNWIND MERGE upsert"), "{message}");
        assert!(message.contains("MATCH that binds them"), "{message}");
    }

    #[cfg(feature = "client-api")]
    #[test]
    fn inline_unwind_list_names_the_batch_input_it_wanted() {
        let err = parse_opencypher_unwind_batch("UNWIND [1, 2, 3] AS row CREATE (n {id: row})")
            .unwrap_err();
        let message = err.to_string();
        assert!(message.contains("parameter holding a list of maps"), "{message}");
        assert!(message.contains("not an inline list"), "{message}");
    }

    #[cfg(feature = "client-api")]
    #[test]
    fn multi_hop_unwind_batch_names_the_one_hop_rule() {
        let err = parse_opencypher_unwind_batch(
            "UNWIND $rows AS row \
             CREATE (a {id: row.a})-[:RELATES]->(b {id: row.b})-[:RELATES]->(c {id: row.c})",
        )
        .unwrap_err();
        let message = err.to_string();
        assert!(message.contains("one-hop relationships only"), "{message}");
        assert!(message.contains("one batch per hop"), "{message}");
    }

    #[cfg(feature = "client-api")]
    #[test]
    fn lowers_unwind_vertex_upsert_batch() {
        let parsed = parse_opencypher_unwind_batch(
            "UNWIND $rows AS row MERGE (n {id: row.vertex}) SET n:Source, n.source_id = row.source_id, n.active = row.active",
        )
        .unwrap()
        .unwrap();
        assert_eq!(parsed.parameter, "rows");
        assert_eq!(
            parsed.kind,
            ParsedUnwindBatchKind::UpsertVertices {
                label: "Source".to_string(),
                vertex_field: "vertex".to_string(),
                property_fields: BTreeMap::from([
                    ("active".to_string(), "active".to_string()),
                    ("source_id".to_string(), "source_id".to_string()),
                ]),
                update_if_newer_by: None,
                create_only_properties: BTreeSet::new(),
            }
        );
    }

    #[cfg(feature = "client-api")]
    #[test]
    fn lowers_guarded_unwind_vertex_upsert_batch() {
        let parsed = parse_opencypher_unwind_batch(
            "UNWIND $rows AS row MERGE (n {id: row.vertex}) SET n:Source, \
             n.created_at = row.created_at, n.updated_at = row.updated_at, \
             n.__hydradb_update_if_newer_by = row.updated_at, \
             n.__hydradb_create_only_created_at = row.created_at",
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            parsed.kind,
            ParsedUnwindBatchKind::UpsertVertices {
                label: "Source".to_string(),
                vertex_field: "vertex".to_string(),
                property_fields: BTreeMap::from([
                    ("created_at".to_string(), "created_at".to_string()),
                    ("updated_at".to_string(), "updated_at".to_string()),
                ]),
                update_if_newer_by: Some("updated_at".to_string()),
                create_only_properties: BTreeSet::from(["created_at".to_string()]),
            }
        );
    }

    #[cfg(feature = "client-api")]
    #[test]
    fn rejects_a_guard_that_is_also_create_only() {
        let error = parse_opencypher_unwind_batch(
            "UNWIND $rows AS row MERGE (n {id: row.vertex}) SET n:Source, \
             n.updated_at = row.updated_at, \
             n.__hydradb_update_if_newer_by = row.updated_at, \
             n.__hydradb_create_only_updated_at = row.updated_at",
        )
        .unwrap_err();

        assert!(error.to_string().contains("cannot also be create-only"));
    }

    #[cfg(feature = "client-api")]
    #[test]
    fn rejects_unwind_merge_that_would_rewrite_pattern_metadata() {
        let err = parse_opencypher_unwind_batch(
            "UNWIND $rows AS row MERGE (n:Source {id: row.vertex, source_id: row.source_id})",
        )
        .unwrap_err();
        assert!(err
            .to_string()
            .contains("requires MERGE by id followed by SET"));
    }

    #[cfg(feature = "client-api")]
    #[test]
    fn lowers_unwind_relationship_create_batch() {
        let parsed = parse_opencypher_unwind_batch(
            "UNWIND $rows AS row \
             MATCH (s:Entity {id: row.source_vertex}), \
                   (d:Entity {id: row.destination_vertex}) \
             CREATE (s)-[:RELATES {id: row.relationship_vertex, relationship_id: row.relationship_id, chunk_id: row.chunk_id}]->(d)",
        )
        .unwrap()
        .unwrap();
        assert_eq!(parsed.parameter, "rows");
        assert_eq!(
            parsed.kind,
            ParsedUnwindBatchKind::CreateRelationshipsBetweenLabeledVertices {
                edge_type: "RELATES".to_string(),
                source_field: "source_vertex".to_string(),
                destination_field: "destination_vertex".to_string(),
                relationship_id_field: "relationship_vertex".to_string(),
                property_fields: BTreeMap::from([
                    ("chunk_id".to_string(), "chunk_id".to_string()),
                    ("relationship_id".to_string(), "relationship_id".to_string(),),
                ]),
                source_label: "Entity".to_string(),
                destination_label: "Entity".to_string(),
            }
        );
    }

    #[cfg(feature = "client-api")]
    #[test]
    fn lowers_guarded_unwind_relationship_merge_batch() {
        let parsed = parse_opencypher_unwind_batch(
            "UNWIND $rows AS row \
             MATCH (s:Entity {id: row.source_vertex}), \
                   (d:Entity {id: row.destination_vertex}) \
             MERGE (s)-[r:RELATES {id: row.relationship_vertex}]->(d) \
             SET r.timestamp = row.timestamp, r.valid_from = row.valid_from, \
                 r.__hydradb_update_if_newer_by = row.timestamp, \
                 r.__hydradb_create_only_valid_from = row.valid_from",
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            parsed.kind,
            ParsedUnwindBatchKind::MergeRelationshipsBetweenLabeledVertices {
                edge_type: "RELATES".to_string(),
                source_field: "source_vertex".to_string(),
                destination_field: "destination_vertex".to_string(),
                relationship_id_field: "relationship_vertex".to_string(),
                property_fields: BTreeMap::from([
                    ("timestamp".to_string(), "timestamp".to_string()),
                    ("valid_from".to_string(), "valid_from".to_string()),
                ]),
                source_label: "Entity".to_string(),
                destination_label: "Entity".to_string(),
                update_if_newer_by: Some("timestamp".to_string()),
                create_only_properties: BTreeSet::from(["valid_from".to_string()]),
            }
        );
    }

    #[cfg(feature = "client-api")]
    #[test]
    fn lowers_unwind_relationship_merge_batch() {
        let parsed = parse_opencypher_unwind_batch(
            "UNWIND $rows AS row \
             MATCH (s:Entity {id: row.source_vertex}), \
                   (d:Entity {id: row.destination_vertex}) \
             MERGE (s)-[r:RELATES {id: row.relationship_vertex}]->(d) \
             SET r.relationship_id = row.relationship_id, r.chunk_id = row.chunk_id",
        )
        .unwrap()
        .unwrap();
        assert_eq!(parsed.parameter, "rows");
        assert_eq!(
            parsed.kind,
            ParsedUnwindBatchKind::MergeRelationshipsBetweenLabeledVertices {
                edge_type: "RELATES".to_string(),
                source_field: "source_vertex".to_string(),
                destination_field: "destination_vertex".to_string(),
                relationship_id_field: "relationship_vertex".to_string(),
                property_fields: BTreeMap::from([
                    ("chunk_id".to_string(), "chunk_id".to_string()),
                    ("relationship_id".to_string(), "relationship_id".to_string()),
                ]),
                source_label: "Entity".to_string(),
                destination_label: "Entity".to_string(),
                update_if_newer_by: None,
                create_only_properties: BTreeSet::new(),
            }
        );
    }

    #[cfg(feature = "client-api")]
    #[test]
    fn rejects_mutable_relationship_merge_keys() {
        let err = parse_opencypher_unwind_batch(
            "UNWIND $rows AS row \
             MATCH (s:Entity {id: row.source_vertex}), \
                   (d:Entity {id: row.destination_vertex}) \
             MERGE (s)-[r:RELATES {id: row.relationship_vertex, chunk_id: row.chunk_id}]->(d)",
        )
        .unwrap_err();
        assert!(err
            .to_string()
            .contains("MERGE pattern matches only id; apply properties with SET"));
    }

    #[test]
    fn lowers_distinct_row_query() {
        let parsed = parse_opencypher_row_query(
            "MATCH (u {id: 1})-[:FOLLOWS]->(v) RETURN DISTINCT u.id AS src",
        )
        .unwrap();
        assert!(parsed.distinct);
        assert_eq!(parsed.columns, vec![QueryColumn::new("src")]);

        let err = parse_opencypher_row_query(
            "MATCH (u {id: 1})-[:FOLLOWS]->(v) RETURN DISTINCT u.id ORDER BY v.id",
        )
        .unwrap_err();
        assert!(matches!(err, GraphError::UnsupportedQuery { .. }));
    }

    #[test]
    fn lowers_row_query_with_labels_properties_and_property_projection() {
        let parsed = parse_opencypher_row_query(
            "MATCH (u:User {active: true})-[:FOLLOWS]->(v:User {age: 42}) \
             RETURN u.name AS src, v.age AS age ORDER BY v.age DESC",
        )
        .unwrap();
        assert_eq!(parsed.patterns.len(), 1);
        let RowPattern::Edge(pattern) = &parsed.patterns[0] else {
            panic!("expected edge row pattern");
        };
        assert_eq!(pattern.src.labels, BTreeSet::from(["User".to_string()]));
        assert_eq!(
            pattern.src.properties.get("active"),
            Some(&VertexPropertyValue::Bool(true))
        );
        assert_eq!(
            pattern.dst.properties.get("age"),
            Some(&VertexPropertyValue::Integer(42))
        );
        assert_eq!(
            parsed.projections,
            vec![
                RowProjection::Property {
                    binding: "u".to_string(),
                    property: "name".to_string(),
                },
                RowProjection::Property {
                    binding: "v".to_string(),
                    property: "age".to_string(),
                },
            ]
        );
        assert_eq!(
            parsed.order_by,
            vec![RowSort {
                expression: RowSortExpression::Property {
                    binding: "v".to_string(),
                    property: "age".to_string(),
                },
                ascending: false,
            }]
        );
    }

    #[test]
    fn lowers_row_query_with_relationship_properties_and_projection() {
        let parsed = parse_opencypher_row_query(
            "MATCH (u {id: 1})-[r:FOLLOWS {since: 2020, close: true}]->(v) \
             RETURN r.since AS since, v.id AS dst ORDER BY r.since DESC",
        )
        .unwrap();
        assert_eq!(parsed.patterns.len(), 1);
        let RowPattern::Edge(pattern) = &parsed.patterns[0] else {
            panic!("expected edge row pattern");
        };
        assert_eq!(pattern.binding.as_deref(), Some("r"));
        assert_eq!(
            pattern.properties.get("since"),
            Some(&VertexPropertyValue::Integer(2020))
        );
        assert_eq!(
            pattern.properties.get("close"),
            Some(&VertexPropertyValue::Bool(true))
        );
        assert_eq!(
            parsed.projections,
            vec![
                RowProjection::Property {
                    binding: "r".to_string(),
                    property: "since".to_string(),
                },
                RowProjection::NodeId {
                    binding: "v".to_string(),
                },
            ]
        );
        assert_eq!(
            parsed.order_by,
            vec![RowSort {
                expression: RowSortExpression::Property {
                    binding: "r".to_string(),
                    property: "since".to_string(),
                },
                ascending: false,
            }]
        );
        assert_eq!(
            parsed.columns,
            vec![QueryColumn::new("since"), QueryColumn::new("dst")]
        );
    }

    #[test]
    fn lowers_node_only_row_query() {
        let parsed = parse_opencypher_row_query(
            "MATCH (u:User {active: true}) RETURN u.name AS name ORDER BY u.name",
        )
        .unwrap();
        assert_eq!(parsed.patterns.len(), 1);
        let RowPattern::Node(node) = &parsed.patterns[0] else {
            panic!("expected node row pattern");
        };
        assert_eq!(node.binding.as_deref(), Some("u"));
        assert_eq!(node.labels, BTreeSet::from(["User".to_string()]));
        assert_eq!(
            node.properties.get("active"),
            Some(&VertexPropertyValue::Bool(true))
        );
        assert_eq!(
            parsed.projections,
            vec![RowProjection::Property {
                binding: "u".to_string(),
                property: "name".to_string(),
            }]
        );
    }

    #[test]
    fn lowers_multi_match_row_query_as_pattern_pipeline() {
        let parsed = parse_opencypher_row_query(
            "MATCH (u:User {id: 1})-[:FOLLOWS]->(v) \
             MATCH (v)-[:POSTED]->(p:Post) \
             WHERE p.score >= 10 \
             RETURN u.id AS user, p.id AS post ORDER BY post",
        )
        .unwrap();
        assert_eq!(parsed.patterns.len(), 2);
        let RowPattern::Edge(first) = &parsed.patterns[0] else {
            panic!("expected first edge row pattern");
        };
        let RowPattern::Edge(second) = &parsed.patterns[1] else {
            panic!("expected second edge row pattern");
        };
        assert_eq!(first.src.binding.as_deref(), Some("u"));
        assert_eq!(first.dst.binding.as_deref(), Some("v"));
        assert_eq!(second.src.binding.as_deref(), Some("v"));
        assert_eq!(second.dst.binding.as_deref(), Some("p"));
        assert!(parsed.predicate.is_some());
        assert_eq!(
            parsed.columns,
            vec![QueryColumn::new("user"), QueryColumn::new("post")]
        );
    }

    #[test]
    fn lowers_passthrough_with_row_query() {
        let parsed = parse_opencypher_row_query(
            "MATCH (u {id: 1})-[r:FOLLOWS]->(v) WITH u, r, v \
             MATCH (v)-[:POSTED]->(p) RETURN p.id AS post",
        )
        .unwrap();
        assert_eq!(parsed.patterns.len(), 2);
        let RowPattern::Edge(first) = &parsed.patterns[0] else {
            panic!("expected first edge row pattern");
        };
        assert_eq!(first.binding.as_deref(), Some("r"));
        assert_eq!(parsed.columns, vec![QueryColumn::new("post")]);

        let err = parse_opencypher_row_query(
            "MATCH (u {id: 1})-[r:FOLLOWS]->(v) WITH v \
             MATCH (v)-[:POSTED]->(p) RETURN p.id",
        )
        .unwrap_err();
        assert!(matches!(err, GraphError::UnsupportedQuery { .. }));
    }

    #[test]
    fn lowers_optional_match_as_left_join_group() {
        let parsed = parse_opencypher_row_query(
            "MATCH (u:User) OPTIONAL MATCH (u)-[:FOLLOWS]->(v) WHERE v.id <> 99 \
             RETURN u.id AS user, v.id AS followed ORDER BY user",
        )
        .unwrap();
        assert_eq!(parsed.pattern_groups.len(), 2);
        assert!(!parsed.pattern_groups[0].optional);
        assert!(parsed.pattern_groups[1].optional);
        assert!(parsed.pattern_groups[1].predicate.is_some());
        assert_eq!(
            parsed.columns,
            vec![QueryColumn::new("user"), QueryColumn::new("followed")]
        );
    }

    #[test]
    fn lowers_union_row_query_arms() {
        let parsed = parse_opencypher_row_query(
            "MATCH (u:User) RETURN u.id AS id \
             UNION ALL MATCH (m:Moderator) RETURN m.id AS id",
        )
        .unwrap();
        assert!(parsed.union_all);
        assert_eq!(parsed.union_arms.len(), 1);
        assert_eq!(parsed.columns, vec![QueryColumn::new("id")]);
        assert_eq!(parsed.union_arms[0].columns, vec![QueryColumn::new("id")]);

        let windowed = parse_opencypher_row_query(
            "MATCH (u:User) RETURN u.id AS id ORDER BY id DESC LIMIT 2 \
			 UNION ALL MATCH (m:Moderator) RETURN m.id AS id ORDER BY id LIMIT 1",
        )
        .unwrap();
        assert_eq!(windowed.window.limit, Some(2));
        assert_eq!(windowed.union_arms[0].window.limit, Some(1));
        assert!(!windowed.order_by.is_empty());
        assert!(!windowed.union_arms[0].order_by.is_empty());

        let mismatch = parse_opencypher_row_query(
            "MATCH (u:User) RETURN u.id AS id \
             UNION MATCH (m:Moderator) RETURN m.id AS other",
        )
        .unwrap_err();
        assert!(matches!(mismatch, GraphError::UnsupportedQuery { .. }));
    }

    #[test]
    fn lowers_large_union_path_arms_independently() {
        fn path_arm(parameter: &str) -> String {
            let mut query =
                format!("MATCH (n0:Entity{{name:${parameter},tenant_id:$t,sub_tenant_id:$u}})");
            for hop in 0..3 {
                query.push_str(&format!(
                    "-[r{hop}:RELATES]->(n{}:Entity{{tenant_id:$t,sub_tenant_id:$u}})",
                    hop + 1
                ));
            }
            let mut projections = Vec::new();
            for node in 0..=3 {
                for (index, property) in ["name", "type", "identifier", "entity_id", "namespace"]
                    .iter()
                    .enumerate()
                {
                    if node == 0 && *property == "name" {
                        projections.push("n0.name".to_string());
                    } else {
                        projections.push(format!(
                            "n{node}.{property} AS n{node}{}",
                            char::from(b'a' + u8::try_from(index).unwrap())
                        ));
                    }
                }
            }
            for hop in 0..3 {
                for (index, property) in [
                    "canonical_relation",
                    "raw_relation",
                    "chunk_id",
                    "relationship_id",
                    "timestamp",
                    "metadata",
                ]
                .iter()
                .enumerate()
                {
                    projections.push(format!(
                        "r{hop}.{property} AS r{hop}{}",
                        char::from(b'a' + u8::try_from(index).unwrap())
                    ));
                }
            }
            query.push_str(" RETURN ");
            query.push_str(&projections.join(","));
            query.push_str(" LIMIT 50");
            query
        }

        let mut parameters = BTreeMap::from([
            (
                "t".to_string(),
                VertexPropertyValue::String("tenant".to_string()),
            ),
            (
                "u".to_string(),
                VertexPropertyValue::String("collection".to_string()),
            ),
        ]);
        let mut arms = Vec::new();
        for index in 0..10 {
            let parameter = format!("batch_value_{index}");
            parameters.insert(
                parameter.clone(),
                VertexPropertyValue::String(format!("entity-{index}")),
            );
            arms.push(path_arm(&parameter));
        }
        let query = arms.join(" UNION ALL ");

        let parsed = parse_opencypher_row_query_with_parameters(&query, &parameters).unwrap();
        assert!(parsed.union_all);
        assert_eq!(parsed.union_arms.len(), 9);
        assert_eq!(parsed.columns.len(), 38);
        assert_eq!(
            classify_opencypher_query_access(&query).unwrap(),
            OpenCypherQueryAccess::Read
        );
    }

    #[test]
    fn top_level_union_splitter_ignores_nested_and_quoted_tokens() {
        let query = "MATCH (n {text: 'UNION ALL', `UNION`: 1}) RETURN n.id AS id \
                     UNION /* separator */ ALL \
                     MATCH (m {text: \"UNION\"}) RETURN m.id AS id";
        let union = split_top_level_union(query).unwrap().unwrap();
        assert!(union.union_all);
        assert_eq!(union.arms.len(), 2);
        assert!(union.arms[0].contains("'UNION ALL'"));
        assert!(union.arms[1].contains("\"UNION\""));

        assert!(
            split_top_level_union("CALL { RETURN 1 UNION ALL RETURN 2 } RETURN 3")
                .unwrap()
                .is_none()
        );
        assert!(
            split_top_level_union("MATCH (n) WHERE n.union = $union RETURN n.union AS value")
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn lowers_multi_edge_path_as_joinable_edge_patterns() {
        let parsed = parse_opencypher_row_query(
            "MATCH (u {id: 1})-[:FOLLOWS]->(v)-[:POSTED]->(p) \
             RETURN u.id, v.id, p.id",
        )
        .unwrap();
        assert_eq!(parsed.patterns.len(), 2);
        let RowPattern::Edge(first) = &parsed.patterns[0] else {
            panic!("expected first edge row pattern");
        };
        let RowPattern::Edge(second) = &parsed.patterns[1] else {
            panic!("expected second edge row pattern");
        };
        assert_eq!(first.edge_type, "FOLLOWS");
        assert_eq!(first.src.id, Some(1));
        assert_eq!(first.dst.binding.as_deref(), Some("v"));
        assert_eq!(second.edge_type, "POSTED");
        assert_eq!(second.src.binding.as_deref(), Some("v"));
        assert_eq!(second.dst.binding.as_deref(), Some("p"));
    }

    #[test]
    fn lowers_grouped_aggregate_row_query() {
        let parsed = parse_opencypher_row_query(
            "MATCH (u)-[:FOLLOWS]->(v)-[:POSTED]->(p:Post) \
             RETURN u.id AS user, count(*) AS posts, sum(p.score) AS score, \
             avg(p.score) AS avg_score, collect(p.id) AS post_ids ORDER BY posts DESC",
        )
        .unwrap();
        assert_eq!(
            parsed.columns,
            vec![
                QueryColumn::new("user"),
                QueryColumn::new("posts"),
                QueryColumn::new("score"),
                QueryColumn::new("avg_score"),
                QueryColumn::new("post_ids"),
            ]
        );
        assert_eq!(
            parsed.projections,
            vec![
                RowProjection::NodeId {
                    binding: "u".to_string(),
                },
                RowProjection::CountAll,
                RowProjection::Aggregate {
                    function: RowAggregateFunction::Sum,
                    expression: RowExpression::Property {
                        binding: "p".to_string(),
                        property: "score".to_string(),
                    },
                },
                RowProjection::Aggregate {
                    function: RowAggregateFunction::Avg,
                    expression: RowExpression::Property {
                        binding: "p".to_string(),
                        property: "score".to_string(),
                    },
                },
                RowProjection::Aggregate {
                    function: RowAggregateFunction::Collect,
                    expression: RowExpression::NodeId {
                        binding: "p".to_string(),
                    },
                },
            ]
        );
    }

    #[test]
    fn lowers_mutation_queries() {
        let parsed = parse_opencypher_mutation_query_with_parameters(
            "MATCH (u {id: 1})-[r:FOLLOWS]->(v {id: 2}) \
             SET u.active = true, v:Moderator REMOVE v.name DELETE r",
            &BTreeMap::new(),
        )
        .unwrap()
        .unwrap();
        assert_eq!(parsed.patterns.len(), 1);
        let RowPattern::Edge(edge) = &parsed.patterns[0] else {
            panic!("expected edge pattern");
        };
        assert_eq!(edge.binding.as_deref(), Some("r"));
        assert_eq!(edge.edge_type, "FOLLOWS");
        assert_eq!(edge.src.binding.as_deref(), Some("u"));
        assert_eq!(edge.dst.binding.as_deref(), Some("v"));
        assert_eq!(
            parsed.actions,
            vec![
                RowMutationAction::SetProperty {
                    binding: "u".to_string(),
                    property: "active".to_string(),
                    value: VertexPropertyValue::Bool(true),
                },
                RowMutationAction::SetLabels {
                    binding: "v".to_string(),
                    labels: BTreeSet::from(["Moderator".to_string()]),
                },
                RowMutationAction::RemoveProperty {
                    binding: "v".to_string(),
                    property: "name".to_string(),
                },
                RowMutationAction::DeleteBinding {
                    binding: "r".to_string(),
                    detach: false,
                },
            ]
        );

        let merge = parse_opencypher_mutation_query_with_parameters(
            "MERGE (u:User {id: 1})-[:FOLLOWS]->(v {id: 2})",
            &BTreeMap::new(),
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            merge.actions,
            vec![RowMutationAction::MergeEdge {
                edge_type: "FOLLOWS".to_string(),
                src: 1,
                dst: 2,
                src_metadata: VertexMetadata::default().with_label("User"),
                dst_metadata: VertexMetadata::default(),
                edge_metadata: EdgeMetadata::default(),
            }]
        );
    }

    #[test]
    fn lowers_full_signed_integer_literal_range() {
        let parsed = parse_opencypher_row_query(
            "MATCH (n:Score {score: -9223372036854775808}) RETURN n.score",
        )
        .unwrap();
        let RowPattern::Node(node) = &parsed.patterns[0] else {
            panic!("expected node pattern");
        };
        assert_eq!(
            node.properties.get("score"),
            Some(&VertexPropertyValue::SignedInteger(i64::MIN))
        );
    }

    /// The fingerprint's two load-bearing properties, and nothing more: it is
    /// the same for two runs of the same shape, and it never contains a value.
    #[test]
    fn query_fingerprint_is_stable_across_values_and_formatting() {
        let a = query_shape_fingerprint("MATCH (n:Person) WHERE n.id = 42 RETURN n.name");
        let b = query_shape_fingerprint("MATCH (n:Person)\n  WHERE n.id = 7\n  RETURN n.name");
        assert_eq!(
            a, b,
            "literal values and whitespace must not fork the shape"
        );

        let quoted =
            query_shape_fingerprint("MATCH (n:Person) WHERE n.email = 'a@b.test' RETURN n");
        let other = query_shape_fingerprint("MATCH (n:Person) WHERE n.email = 'c@d.test' RETURN n");
        assert_eq!(quoted, other);

        let different = query_shape_fingerprint("MATCH (n:Company) WHERE n.id = 42 RETURN n.name");
        assert_ne!(a, different, "a different label is a different shape");

        // A literal list's length must not fork the shape either, or every
        // batch size becomes its own row in the dashboard.
        assert_eq!(
            query_shape_fingerprint("MATCH (n) WHERE n.id IN [1, 2] RETURN n"),
            query_shape_fingerprint("MATCH (n) WHERE n.id IN [1, 2, 3, 4, 5] RETURN n")
        );

        // Safe to log unredacted: the normalised shape carries no value.
        let shape =
            normalize_query_shape("MATCH (n) WHERE n.email = 'secret@example.test' RETURN n");
        assert!(!shape.contains("secret"), "shape leaked a literal: {shape}");
        assert_eq!(shape, "MATCH (n) WHERE n.email = ? RETURN n");
    }

    #[test]
    fn query_fingerprint_survives_an_unparseable_statement() {
        // The root span opens before anything is parsed, so the fingerprint has
        // to exist for text the parser will reject.
        assert_eq!(
            opencypher_query_fingerprint("MATCH ((("),
            query_shape_fingerprint("MATCH ((("),
        );
    }
}
