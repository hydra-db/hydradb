use std::collections::{BTreeMap, BTreeSet};
use std::ffi::{CStr, CString};
use std::ptr::null_mut;

use libcypher_parser_sys as sys;

use crate::{
    validate_component, GraphError, QueryColumn, QueryStatement, QueryWindow, Result, VertexId,
    VertexMetadata, VertexPropertyValue,
};

type AstNode = sys::cypher_astnode_t;

pub trait CypherFrontend {
    fn parse(&self, query: &str) -> Result<QueryStatement>;
}

#[derive(Clone, Copy, Debug, Default)]
pub struct LibCypherParserFrontend;

pub type DefaultCypherFrontend = LibCypherParserFrontend;

#[derive(Clone, Debug, Eq, PartialEq)]
struct EdgePattern {
    edge_type: String,
    src: Option<VertexId>,
    src_binding: Option<String>,
    dst: Option<VertexId>,
    dst_binding: Option<String>,
    hop_range: Option<(u8, u8)>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct NodePattern {
    binding: Option<String>,
    id: Option<VertexId>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ParsedQuery {
    pub statement: QueryStatement,
    pub window: QueryWindow,
    pub columns: Vec<QueryColumn>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ParsedRowQuery {
    pub pattern: RowPattern,
    pub predicate: Option<RowPredicate>,
    pub projections: Vec<RowProjection>,
    pub order_by: Vec<RowSort>,
    pub window: QueryWindow,
    pub columns: Vec<QueryColumn>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RowPattern {
    Node(RowNodePattern),
    Edge(RowEdgePattern),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RowEdgePattern {
    pub edge_type: String,
    pub src: RowNodePattern,
    pub dst: RowNodePattern,
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
    NodeId { binding: String },
    Property { binding: String, property: String },
    CountAll,
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

pub fn parse_cypher(query: &str) -> Result<QueryStatement> {
    LibCypherParserFrontend.parse(query)
}

pub fn parse_opencypher(query: &str) -> Result<QueryStatement> {
    parse_cypher(query)
}

pub fn parse_cypher_with_window(query: &str) -> Result<ParsedQuery> {
    let parsed = ParsedCypher::parse(query)?;
    parsed.lower_with_window()
}

pub fn parse_opencypher_with_window(query: &str) -> Result<ParsedQuery> {
    parse_cypher_with_window(query)
}

pub fn parse_opencypher_row_query(query: &str) -> Result<ParsedRowQuery> {
    let parsed = ParsedCypher::parse(query)?;
    parsed.lower_row_query()
}

impl CypherFrontend for LibCypherParserFrontend {
    fn parse(&self, query: &str) -> Result<QueryStatement> {
        let parsed = ParsedCypher::parse(query)?;
        parsed.lower()
    }
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

    fn lower(&self) -> Result<QueryStatement> {
        let lowered = self.lower_with_window()?;
        if !lowered.window.is_default() {
            return unsupported(
                "statement-only Cypher parsing cannot drop SKIP/LIMIT; use parse_cypher_with_window",
            );
        }
        Ok(lowered.statement)
    }

    fn lower_with_window(&self) -> Result<ParsedQuery> {
        unsafe {
            let directives = sys::cypher_parse_result_ndirectives(self.result);
            if directives != 1 {
                return unsupported("only a single Cypher statement is supported in Phase 0");
            }

            let statement = checked_node(sys::cypher_parse_result_get_directive(self.result, 0))?;
            ensure_instance(statement, sys::CYPHER_AST_STATEMENT, "statement")?;
            let body = checked_node(sys::cypher_ast_statement_get_body(statement))?;
            ensure_instance(body, sys::CYPHER_AST_QUERY, "query")?;
            self.lower_query(body)
        }
    }

    fn lower_query(&self, query: *const AstNode) -> Result<ParsedQuery> {
        unsafe {
            let clause_count = sys::cypher_ast_query_nclauses(query);
            if clause_count == 1 {
                let clause = checked_node(sys::cypher_ast_query_get_clause(query, 0))?;
                if is_instance(clause, sys::CYPHER_AST_CREATE) {
                    return Ok(ParsedQuery {
                        statement: lower_create(clause)?,
                        window: QueryWindow::default(),
                        columns: Vec::new(),
                    });
                }
            }

            if clause_count == 2 {
                let match_clause = checked_node(sys::cypher_ast_query_get_clause(query, 0))?;
                let return_clause = checked_node(sys::cypher_ast_query_get_clause(query, 1))?;
                if is_instance(match_clause, sys::CYPHER_AST_MATCH)
                    && is_instance(return_clause, sys::CYPHER_AST_RETURN)
                {
                    return lower_match_return(match_clause, return_clause);
                }
            }

            unsupported("only CREATE and MATCH edge patterns are supported in Phase 0")
        }
    }

    fn lower_row_query(&self) -> Result<ParsedRowQuery> {
        unsafe {
            let directives = sys::cypher_parse_result_ndirectives(self.result);
            if directives != 1 {
                return unsupported("only a single Cypher statement is supported in Phase 2");
            }

            let statement = checked_node(sys::cypher_parse_result_get_directive(self.result, 0))?;
            ensure_instance(statement, sys::CYPHER_AST_STATEMENT, "statement")?;
            let body = checked_node(sys::cypher_ast_statement_get_body(statement))?;
            ensure_instance(body, sys::CYPHER_AST_QUERY, "query")?;
            self.lower_row_query_body(body)
        }
    }

    fn lower_row_query_body(&self, query: *const AstNode) -> Result<ParsedRowQuery> {
        unsafe {
            if sys::cypher_ast_query_nclauses(query) != 2 {
                return unsupported("row execution supports MATCH ... RETURN queries");
            }
            let match_clause = checked_node(sys::cypher_ast_query_get_clause(query, 0))?;
            let return_clause = checked_node(sys::cypher_ast_query_get_clause(query, 1))?;
            if !is_instance(match_clause, sys::CYPHER_AST_MATCH)
                || !is_instance(return_clause, sys::CYPHER_AST_RETURN)
            {
                return unsupported("row execution supports MATCH ... RETURN queries");
            }
            lower_match_return_rows(match_clause, return_clause)
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

impl Drop for ParsedCypher {
    fn drop(&mut self) {
        unsafe {
            sys::cypher_parse_result_free(self.result);
        }
    }
}

fn lower_create(create: *const AstNode) -> Result<QueryStatement> {
    unsafe {
        if sys::cypher_ast_create_is_unique(create) {
            return unsupported("CREATE UNIQUE is not executable in Phase 0");
        }

        let pattern = checked_node(sys::cypher_ast_create_get_pattern(create))?;
        let edge = lower_create_edge_pattern(pattern)?;
        if edge.hop_range.is_some() {
            return unsupported("CREATE does not support variable-length relationships in Phase 2");
        }
        let src = edge
            .src
            .id
            .ok_or_else(|| unsupported_value("CREATE requires source id"))?;
        let dst = edge
            .dst
            .id
            .ok_or_else(|| unsupported_value("CREATE requires destination id"))?;
        let src_metadata = vertex_metadata_from_node_pattern(&edge.src);
        let dst_metadata = vertex_metadata_from_node_pattern(&edge.dst);
        if src_metadata.labels.is_empty()
            && src_metadata.properties.is_empty()
            && dst_metadata.labels.is_empty()
            && dst_metadata.properties.is_empty()
        {
            return Ok(QueryStatement::CreateEdge {
                edge_type: edge.edge_type,
                src,
                dst,
            });
        }
        Ok(QueryStatement::CreateEdgeWithMetadata {
            edge_type: edge.edge_type,
            src,
            dst,
            src_metadata,
            dst_metadata,
        })
    }
}

fn lower_match_return(
    match_clause: *const AstNode,
    return_clause: *const AstNode,
) -> Result<ParsedQuery> {
    unsafe {
        if sys::cypher_ast_match_is_optional(match_clause) {
            return unsupported("OPTIONAL MATCH is not executable in Phase 0");
        }
        if sys::cypher_ast_match_nhints(match_clause) != 0 {
            return unsupported("MATCH hints are not executable in Phase 0");
        }
        if sys::cypher_ast_return_is_distinct(return_clause)
            || sys::cypher_ast_return_has_include_existing(return_clause)
            || !sys::cypher_ast_return_get_order_by(return_clause).is_null()
            || sys::cypher_ast_return_nprojections(return_clause) != 1
        {
            return unsupported("MATCH edge currently supports a single RETURN projection only");
        }
        let window = lower_return_window(return_clause)?;

        let pattern = checked_node(sys::cypher_ast_match_get_pattern(match_clause))?;
        let edge = lower_single_edge_pattern(pattern)?;
        let predicate = sys::cypher_ast_match_get_predicate(match_clause);
        let where_dst = if predicate.is_null() {
            None
        } else {
            Some(lower_where_node_id(predicate, edge.dst_binding.as_deref())?)
        };
        let fixed_dst = resolve_node_id_constraint(edge.dst, where_dst)?;
        let src = edge
            .src
            .ok_or_else(|| unsupported_value("MATCH requires source id"))?;
        let projection = checked_node(sys::cypher_ast_return_get_projection(return_clause, 0))?;
        let expression = checked_node(sys::cypher_ast_projection_get_expression(projection))?;

        if is_count_star(expression)? {
            let columns = vec![QueryColumn::new(projection_column_name(
                projection, "count(*)",
            )?)];
            if let Some((min_hops, max_hops)) = edge.hop_range {
                if fixed_dst.is_some() {
                    return unsupported(
                        "variable-length MATCH with fixed destination is not executable in Phase 0",
                    );
                }
                return Ok(ParsedQuery {
                    statement: QueryStatement::MatchReachable {
                        edge_type: edge.edge_type,
                        src,
                        min_hops,
                        max_hops,
                        return_count: true,
                    },
                    window,
                    columns,
                });
            }
            if let Some(dst) = fixed_dst {
                return Ok(ParsedQuery {
                    statement: QueryStatement::MatchOutFiltered {
                        edge_type: edge.edge_type,
                        src,
                        dst,
                        return_count: true,
                    },
                    window,
                    columns,
                });
            }
            return Ok(ParsedQuery {
                statement: QueryStatement::MatchOut {
                    edge_type: edge.edge_type,
                    src,
                    return_count: true,
                },
                window,
                columns,
            });
        }

        if let Some((min_hops, max_hops)) = edge.hop_range {
            if fixed_dst.is_some() {
                return unsupported(
                    "variable-length MATCH with fixed destination is not executable in Phase 0",
                );
            }
            if !projects_node_id(expression, edge.dst_binding.as_deref())? {
                return unsupported("variable-length MATCH currently requires RETURN <dst>.id");
            }
            let columns = vec![QueryColumn::new(projection_column_name(
                projection,
                node_id_column_name(edge.dst_binding.as_deref())?,
            )?)];
            return Ok(ParsedQuery {
                statement: QueryStatement::MatchReachable {
                    edge_type: edge.edge_type,
                    src,
                    min_hops,
                    max_hops,
                    return_count: false,
                },
                window,
                columns,
            });
        }

        if let Some(dst) = fixed_dst {
            if !projects_node_id(expression, edge.dst_binding.as_deref())? {
                return unsupported(
                    "exact edge MATCH currently supports RETURN <dst>.id or count(*)",
                );
            }
            let columns = vec![QueryColumn::new(projection_column_name(
                projection,
                node_id_column_name(edge.dst_binding.as_deref())?,
            )?)];
            return Ok(ParsedQuery {
                statement: QueryStatement::MatchOutFiltered {
                    edge_type: edge.edge_type,
                    src,
                    dst,
                    return_count: false,
                },
                window,
                columns,
            });
        }
        if !projects_node_id(expression, edge.dst_binding.as_deref())? {
            return unsupported("open-ended MATCH currently requires RETURN <dst>.id");
        }
        let columns = vec![QueryColumn::new(projection_column_name(
            projection,
            node_id_column_name(edge.dst_binding.as_deref())?,
        )?)];

        Ok(ParsedQuery {
            statement: QueryStatement::MatchOut {
                edge_type: edge.edge_type,
                src,
                return_count: false,
            },
            window,
            columns,
        })
    }
}

fn lower_match_return_rows(
    match_clause: *const AstNode,
    return_clause: *const AstNode,
) -> Result<ParsedRowQuery> {
    unsafe {
        if sys::cypher_ast_match_is_optional(match_clause) {
            return unsupported("OPTIONAL MATCH is not executable in Phase 2");
        }
        if sys::cypher_ast_match_nhints(match_clause) != 0 {
            return unsupported("MATCH hints are not executable in Phase 2");
        }
        if sys::cypher_ast_return_is_distinct(return_clause)
            || sys::cypher_ast_return_has_include_existing(return_clause)
        {
            return unsupported("DISTINCT and RETURN * are not executable in Phase 2");
        }

        let pattern = checked_node(sys::cypher_ast_match_get_pattern(match_clause))?;
        let pattern = lower_row_pattern(pattern)?;
        let predicate = sys::cypher_ast_match_get_predicate(match_clause);
        let predicate = if predicate.is_null() {
            None
        } else {
            Some(lower_row_predicate(predicate)?)
        };

        let projection_count = sys::cypher_ast_return_nprojections(return_clause);
        if projection_count == 0 {
            return unsupported("RETURN requires at least one projection");
        }

        let mut projections = Vec::with_capacity(projection_count as usize);
        let mut columns = Vec::with_capacity(projection_count as usize);
        let mut has_count = false;
        for idx in 0..projection_count {
            let projection =
                checked_node(sys::cypher_ast_return_get_projection(return_clause, idx))?;
            let expression = checked_node(sys::cypher_ast_projection_get_expression(projection))?;
            if is_count_star(expression)? {
                has_count = true;
                projections.push(RowProjection::CountAll);
                columns.push(QueryColumn::new(projection_column_name(
                    projection, "count(*)",
                )?));
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
        if has_count && projection_count != 1 {
            return unsupported("count(*) cannot be mixed with row projections in Phase 2");
        }

        let order_by = lower_return_order_by(return_clause)?;
        let window = lower_return_window(return_clause)?;
        Ok(ParsedRowQuery {
            pattern,
            predicate,
            projections,
            order_by,
            window,
            columns,
        })
    }
}

fn lower_return_window(return_clause: *const AstNode) -> Result<QueryWindow> {
    unsafe {
        let skip_node = sys::cypher_ast_return_get_skip(return_clause);
        let skip = if skip_node.is_null() {
            0
        } else {
            window_u64_expression(checked_node(skip_node)?, "SKIP")?
        };

        let limit_node = sys::cypher_ast_return_get_limit(return_clause);
        let limit = if limit_node.is_null() {
            None
        } else {
            Some(window_usize_expression(checked_node(limit_node)?, "LIMIT")?)
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

fn lower_row_predicate(predicate: *const AstNode) -> Result<RowPredicate> {
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
                    left: lower_row_expression(left)?,
                    op,
                    right: lower_row_expression(right)?,
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
                    Box::new(lower_row_predicate(left)?),
                    Box::new(lower_row_predicate(right)?),
                ));
            }
            if op == sys::CYPHER_OP_OR {
                return Ok(RowPredicate::Or(
                    Box::new(lower_row_predicate(left)?),
                    Box::new(lower_row_predicate(right)?),
                ));
            }
            if let Ok(op) = row_comparison_op(op) {
                return Ok(RowPredicate::Compare {
                    left: lower_row_expression(left)?,
                    op,
                    right: lower_row_expression(right)?,
                });
            }
        }

        if is_instance(predicate, sys::CYPHER_AST_UNARY_OPERATOR) {
            let op = sys::cypher_ast_unary_operator_get_operator(predicate);
            if op == sys::CYPHER_OP_NOT {
                let arg = checked_node(sys::cypher_ast_unary_operator_get_argument(predicate))?;
                return Ok(RowPredicate::Not(Box::new(lower_row_predicate(arg)?)));
            }
        }
    }
    unsupported("WHERE currently supports boolean combinations of property comparisons")
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
            unsupported("comparison operator is not executable in Phase 2")
        }
    }
}

fn lower_row_expression(expression: *const AstNode) -> Result<RowExpression> {
    if let Some(binding) = node_id_expression_binding(expression)? {
        return Ok(RowExpression::NodeId { binding });
    }
    if let Some((binding, property)) = property_expression_binding(expression)? {
        return Ok(RowExpression::Property { binding, property });
    }
    Ok(RowExpression::Literal(scalar_property_value(expression)?))
}

fn lower_single_edge_pattern(pattern: *const AstNode) -> Result<EdgePattern> {
    unsafe {
        ensure_instance(pattern, sys::CYPHER_AST_PATTERN, "pattern")?;
        if sys::cypher_ast_pattern_npaths(pattern) != 1 {
            return unsupported("only one path pattern is executable in Phase 0");
        }

        let path = checked_node(sys::cypher_ast_pattern_get_path(pattern, 0))?;
        ensure_instance(path, sys::CYPHER_AST_PATTERN_PATH, "pattern path")?;
        if sys::cypher_ast_pattern_path_nelements(path) != 3 {
            return unsupported("only one-hop edge patterns are executable in Phase 0");
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
        if !sys::cypher_ast_rel_pattern_get_properties(rel).is_null() {
            return unsupported("relationship properties are not executable in Phase 0");
        }
        if sys::cypher_ast_rel_pattern_nreltypes(rel) != 1 {
            return unsupported("relationship pattern must have exactly one type in Phase 0");
        }

        let edge_type_node = checked_node(sys::cypher_ast_rel_pattern_get_reltype(rel, 0))?;
        let edge_type = reltype_name(edge_type_node)?;

        let left_node = lower_node_pattern(left)?;
        let right_node = lower_node_pattern(right)?;

        match sys::cypher_ast_rel_pattern_get_direction(rel) {
            sys::cypher_rel_direction::CYPHER_REL_OUTBOUND => Ok(EdgePattern {
                edge_type,
                src: left_node.id,
                src_binding: left_node.binding,
                dst: right_node.id,
                dst_binding: right_node.binding,
                hop_range,
            }),
            sys::cypher_rel_direction::CYPHER_REL_INBOUND => Ok(EdgePattern {
                edge_type,
                src: right_node.id,
                src_binding: right_node.binding,
                dst: left_node.id,
                dst_binding: left_node.binding,
                hop_range,
            }),
            sys::cypher_rel_direction::CYPHER_REL_BIDIRECTIONAL => {
                unsupported("undirected relationships are not executable in Phase 0")
            }
        }
    }
}

fn lower_create_edge_pattern(pattern: *const AstNode) -> Result<RowEdgePattern> {
    unsafe {
        ensure_instance(pattern, sys::CYPHER_AST_PATTERN, "pattern")?;
        if sys::cypher_ast_pattern_npaths(pattern) != 1 {
            return unsupported("only one path pattern is executable in Phase 2 CREATE");
        }

        let path = checked_node(sys::cypher_ast_pattern_get_path(pattern, 0))?;
        ensure_instance(path, sys::CYPHER_AST_PATTERN_PATH, "pattern path")?;
        if sys::cypher_ast_pattern_path_nelements(path) != 3 {
            return unsupported("only one-hop edge patterns are executable in Phase 2 CREATE");
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
        if !sys::cypher_ast_rel_pattern_get_properties(rel).is_null() {
            return unsupported("relationship properties are not executable in Phase 2 CREATE");
        }
        if sys::cypher_ast_rel_pattern_nreltypes(rel) != 1 {
            return unsupported(
                "relationship pattern must have exactly one type in Phase 2 CREATE",
            );
        }

        let edge_type_node = checked_node(sys::cypher_ast_rel_pattern_get_reltype(rel, 0))?;
        let edge_type = reltype_name(edge_type_node)?;
        let left_node = lower_create_node_pattern(left)?;
        let right_node = lower_create_node_pattern(right)?;

        match sys::cypher_ast_rel_pattern_get_direction(rel) {
            sys::cypher_rel_direction::CYPHER_REL_OUTBOUND => Ok(RowEdgePattern {
                edge_type,
                src: left_node,
                dst: right_node,
                hop_range,
            }),
            sys::cypher_rel_direction::CYPHER_REL_INBOUND => Ok(RowEdgePattern {
                edge_type,
                src: right_node,
                dst: left_node,
                hop_range,
            }),
            sys::cypher_rel_direction::CYPHER_REL_BIDIRECTIONAL => {
                unsupported("undirected relationships are not executable in Phase 2 CREATE")
            }
        }
    }
}

fn lower_row_pattern(pattern: *const AstNode) -> Result<RowPattern> {
    unsafe {
        ensure_instance(pattern, sys::CYPHER_AST_PATTERN, "pattern")?;
        if sys::cypher_ast_pattern_npaths(pattern) != 1 {
            return unsupported("only one path pattern is executable in Phase 2");
        }

        let path = checked_node(sys::cypher_ast_pattern_get_path(pattern, 0))?;
        ensure_instance(path, sys::CYPHER_AST_PATTERN_PATH, "pattern path")?;
        match sys::cypher_ast_pattern_path_nelements(path) {
            1 => {
                let node = checked_node(sys::cypher_ast_pattern_path_get_element(path, 0))?;
                ensure_instance(node, sys::CYPHER_AST_NODE_PATTERN, "node pattern")?;
                Ok(RowPattern::Node(lower_row_node_pattern(node)?))
            }
            3 => lower_row_single_edge_path(path).map(RowPattern::Edge),
            _ => unsupported("only node and one-hop edge patterns are executable in Phase 2"),
        }
    }
}

fn lower_create_node_pattern(node: *const AstNode) -> Result<RowNodePattern> {
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
            row_node_properties(properties)?
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

fn lower_row_single_edge_path(path: *const AstNode) -> Result<RowEdgePattern> {
    unsafe {
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
        if !sys::cypher_ast_rel_pattern_get_properties(rel).is_null() {
            return unsupported("relationship properties are not executable in Phase 2");
        }
        if sys::cypher_ast_rel_pattern_nreltypes(rel) != 1 {
            return unsupported("relationship pattern must have exactly one type in Phase 2");
        }

        let edge_type_node = checked_node(sys::cypher_ast_rel_pattern_get_reltype(rel, 0))?;
        let edge_type = reltype_name(edge_type_node)?;
        let left_node = lower_row_node_pattern(left)?;
        let right_node = lower_row_node_pattern(right)?;

        match sys::cypher_ast_rel_pattern_get_direction(rel) {
            sys::cypher_rel_direction::CYPHER_REL_OUTBOUND => Ok(RowEdgePattern {
                edge_type,
                src: left_node,
                dst: right_node,
                hop_range,
            }),
            sys::cypher_rel_direction::CYPHER_REL_INBOUND => Ok(RowEdgePattern {
                edge_type,
                src: right_node,
                dst: left_node,
                hop_range,
            }),
            sys::cypher_rel_direction::CYPHER_REL_BIDIRECTIONAL => {
                unsupported("undirected relationships are not executable in Phase 2")
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

fn lower_where_node_id(predicate: *const AstNode, binding: Option<&str>) -> Result<VertexId> {
    let Some(binding) = binding else {
        return unsupported("WHERE id filter requires a named destination binding");
    };
    unsafe {
        if is_instance(predicate, sys::CYPHER_AST_COMPARISON) {
            if sys::cypher_ast_comparison_get_length(predicate) != 1 {
                return unsupported("WHERE supports only one equality comparison in Phase 0");
            }
            let op = sys::cypher_ast_comparison_get_operator(predicate, 0);
            if op != sys::CYPHER_OP_EQUAL {
                return unsupported("WHERE supports only equality on node id in Phase 0");
            }
            let left = checked_node(sys::cypher_ast_comparison_get_argument(predicate, 0))?;
            let right = checked_node(sys::cypher_ast_comparison_get_argument(predicate, 1))?;
            return lower_node_id_equality(left, right, binding);
        }
        if is_instance(predicate, sys::CYPHER_AST_BINARY_OPERATOR) {
            let op = sys::cypher_ast_binary_operator_get_operator(predicate);
            if op != sys::CYPHER_OP_EQUAL {
                return unsupported("WHERE supports only equality on node id in Phase 0");
            }
            let left = checked_node(sys::cypher_ast_binary_operator_get_argument1(predicate))?;
            let right = checked_node(sys::cypher_ast_binary_operator_get_argument2(predicate))?;
            return lower_node_id_equality(left, right, binding);
        }
    }
    unsupported("WHERE supports only <dst>.id = <integer> in Phase 0")
}

fn lower_node_id_equality(
    left: *const AstNode,
    right: *const AstNode,
    binding: &str,
) -> Result<VertexId> {
    if projects_node_id(left, Some(binding))? {
        return integer_vertex_id(right);
    }
    if projects_node_id(right, Some(binding))? {
        return integer_vertex_id(left);
    }
    unsupported("WHERE supports only <dst>.id = <integer> in Phase 0")
}

fn resolve_node_id_constraint(
    pattern_id: Option<VertexId>,
    where_id: Option<VertexId>,
) -> Result<Option<VertexId>> {
    match (pattern_id, where_id) {
        (Some(left), Some(right)) if left != right => {
            unsupported("conflicting node id constraints are not executable in Phase 0")
        }
        (Some(value), _) | (_, Some(value)) => Ok(Some(value)),
        (None, None) => Ok(None),
    }
}

fn lower_node_pattern(node: *const AstNode) -> Result<NodePattern> {
    unsafe {
        ensure_instance(node, sys::CYPHER_AST_NODE_PATTERN, "node pattern")?;
        if sys::cypher_ast_node_pattern_nlabels(node) != 0 {
            return unsupported("node labels are not executable in Phase 0");
        }
        let binding = node_identifier(node)?;
        let properties = sys::cypher_ast_node_pattern_get_properties(node);
        let id = if properties.is_null() {
            None
        } else {
            Some(node_id_property(properties)?)
        };
        Ok(NodePattern { binding, id })
    }
}

fn lower_row_node_pattern(node: *const AstNode) -> Result<RowNodePattern> {
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
            row_node_properties(properties)?
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
) -> Result<BTreeMap<String, VertexPropertyValue>> {
    unsafe {
        ensure_instance(properties, sys::CYPHER_AST_MAP, "node property map")?;
        let mut result = BTreeMap::new();
        for idx in 0..sys::cypher_ast_map_nentries(properties) {
            let key = checked_node(sys::cypher_ast_map_get_key(properties, idx))?;
            let key = prop_name(key)?;
            validate_component("property", &key)?;
            let value = checked_node(sys::cypher_ast_map_get_value(properties, idx))?;
            if result.insert(key, scalar_property_value(value)?).is_some() {
                return unsupported("duplicate node property in pattern");
            }
        }
        Ok(result)
    }
}

fn node_id_property(properties: *const AstNode) -> Result<VertexId> {
    unsafe {
        ensure_instance(properties, sys::CYPHER_AST_MAP, "node property map")?;
        if sys::cypher_ast_map_nentries(properties) != 1 {
            return unsupported("node property map must contain only id in Phase 0");
        }

        let key = checked_node(sys::cypher_ast_map_get_key(properties, 0))?;
        let key_name = prop_name(key)?;
        if !key_name.eq_ignore_ascii_case("id") {
            return unsupported("node property map must contain id in Phase 0");
        }

        let value = checked_node(sys::cypher_ast_map_get_value(properties, 0))?;
        integer_vertex_id(value)
    }
}

fn scalar_property_value(node: *const AstNode) -> Result<VertexPropertyValue> {
    unsafe {
        if is_instance(node, sys::CYPHER_AST_INTEGER) {
            return Ok(VertexPropertyValue::Integer(integer_vertex_id(node)?));
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
    }
    unsupported("property values currently support integer, boolean, and string literals")
}

fn integer_vertex_id(node: *const AstNode) -> Result<VertexId> {
    unsafe {
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

fn window_u64_expression(node: *const AstNode, field: &str) -> Result<u64> {
    let value = constant_integer_expression(node, field)?;
    u64::try_from(value).map_err(|_| unsupported_value(format!("{field} cannot be negative")))
}

fn window_usize_expression(node: *const AstNode, field: &str) -> Result<usize> {
    let value = window_u64_expression(node, field)?;
    usize::try_from(value).map_err(|_| unsupported_value(format!("{field} exceeds platform usize")))
}

fn constant_integer_expression(node: *const AstNode, field: &str) -> Result<i128> {
    unsafe {
        if is_instance(node, sys::CYPHER_AST_INTEGER) {
            let value = c_string(sys::cypher_ast_integer_get_valuestr(node));
            return value.parse::<i128>().map_err(|err| {
                parse_error(format!("invalid {field} integer literal {value}: {err}"))
            });
        }

        if is_instance(node, sys::CYPHER_AST_UNARY_OPERATOR) {
            let op = sys::cypher_ast_unary_operator_get_operator(node);
            let arg = checked_node(sys::cypher_ast_unary_operator_get_argument(node))?;
            let value = constant_integer_expression(arg, field)?;
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
            let left = constant_integer_expression(left, field)?;
            let right = constant_integer_expression(right, field)?;
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
    let value = integer_vertex_id(node)?;
    u8::try_from(value).map_err(|_| unsupported_value(format!("{field} exceeds 255")))
}

fn is_count_star(expression: *const AstNode) -> Result<bool> {
    unsafe {
        if !is_instance(expression, sys::CYPHER_AST_APPLY_ALL_OPERATOR) {
            return Ok(false);
        }
        if sys::cypher_ast_apply_all_operator_get_distinct(expression) {
            return unsupported("count(DISTINCT *) is not executable in Phase 0");
        }

        let function = checked_node(sys::cypher_ast_apply_all_operator_get_func_name(expression))?;
        let function = function_name(function)?;
        Ok(function.eq_ignore_ascii_case("count"))
    }
}

fn projects_node_id(expression: *const AstNode, binding: Option<&str>) -> Result<bool> {
    let Some(binding) = binding else {
        return Ok(false);
    };
    Ok(node_id_expression_binding(expression)?.as_deref() == Some(binding))
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

fn node_id_column_name(binding: Option<&str>) -> Result<String> {
    binding
        .map(|binding| format!("{binding}.id"))
        .ok_or_else(|| unsupported_value("RETURN <dst>.id requires a named destination node"))
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
    fn lowers_create_edge_through_libcypher_parser() {
        assert_eq!(
            parse_opencypher("CREATE (u {id: 1})-[:USER_SUBSCRIBED_TO_SUBREDDIT]->(s {id: 2})")
                .unwrap(),
            QueryStatement::CreateEdge {
                edge_type: "USER_SUBSCRIBED_TO_SUBREDDIT".to_string(),
                src: 1,
                dst: 2
            }
        );
    }

    #[test]
    fn lowers_match_out_neighbors_through_libcypher_parser() {
        assert_eq!(
            parse_opencypher("MATCH (u {id: 1})-[:USER_SUBSCRIBED_TO_SUBREDDIT]->(s) RETURN s.id")
                .unwrap(),
            QueryStatement::MatchOut {
                edge_type: "USER_SUBSCRIBED_TO_SUBREDDIT".to_string(),
                src: 1,
                return_count: false
            }
        );
    }

    #[test]
    fn lowers_match_edge_count_through_libcypher_parser() {
        assert_eq!(
            parse_opencypher(
                "MATCH (u {id: 1})-[:USER_SUBSCRIBED_TO_SUBREDDIT]->(s {id: 2}) RETURN count(*)"
            )
            .unwrap(),
            QueryStatement::MatchOutFiltered {
                edge_type: "USER_SUBSCRIBED_TO_SUBREDDIT".to_string(),
                src: 1,
                dst: 2,
                return_count: true
            }
        );
    }

    #[test]
    fn frontend_trait_uses_libcypher_parser() {
        let frontend = LibCypherParserFrontend;
        assert_eq!(
            frontend
                .parse("MATCH (u {id: 1})-[:FOLLOWS]->(v) RETURN count(*)")
                .unwrap(),
            QueryStatement::MatchOut {
                edge_type: "FOLLOWS".to_string(),
                src: 1,
                return_count: true
            }
        );
    }

    #[test]
    fn rejects_cypher_syntax_errors_from_libcypher_parser() {
        assert!(matches!(
            parse_opencypher("MATCH (u {id: 1})-[:FOLLOWS]-> RETURN u.id"),
            Err(GraphError::QueryParse { .. })
        ));
    }

    #[test]
    fn rejects_labels_instead_of_silently_dropping_them() {
        assert!(matches!(
            parse_opencypher("MATCH (u {id: 1})-[:FOLLOWS]->(v:User) RETURN v.id"),
            Err(GraphError::UnsupportedQuery { .. })
        ));
    }

    #[test]
    fn rejects_unbounded_variable_length_paths() {
        assert!(matches!(
            parse_opencypher("MATCH (u {id: 1})-[:FOLLOWS*]->(v) RETURN v.id"),
            Err(GraphError::UnsupportedQuery { .. })
        ));
    }

    #[test]
    fn rejects_conflicting_pattern_and_where_ids() {
        assert!(matches!(
            parse_opencypher(
                "MATCH (u {id: 1})-[:FOLLOWS]->(v {id: 2}) WHERE v.id = 3 RETURN v.id"
            ),
            Err(GraphError::UnsupportedQuery { .. })
        ));
    }

    #[test]
    fn lowers_variable_length_paths_through_libcypher_parser() {
        assert_eq!(
            parse_opencypher("MATCH (u {id: 1})-[:FOLLOWS*1..3]->(v) RETURN v.id").unwrap(),
            QueryStatement::MatchReachable {
                edge_type: "FOLLOWS".to_string(),
                src: 1,
                min_hops: 1,
                max_hops: 3,
                return_count: false,
            }
        );
    }

    #[test]
    fn lowers_where_id_predicates_through_libcypher_parser() {
        assert_eq!(
            parse_opencypher("MATCH (u {id: 1})-[:FOLLOWS]->(v) WHERE v.id = 2 RETURN v.id")
                .unwrap(),
            QueryStatement::MatchOutFiltered {
                edge_type: "FOLLOWS".to_string(),
                src: 1,
                dst: 2,
                return_count: false,
            }
        );
    }

    #[test]
    fn preserves_return_skip_and_limit_for_planning() {
        let parsed = parse_opencypher_with_window(
            "MATCH (u {id: 1})-[:FOLLOWS]->(v) RETURN v.id SKIP 2 LIMIT 3",
        )
        .unwrap();
        assert_eq!(
            parsed.statement,
            QueryStatement::MatchOut {
                edge_type: "FOLLOWS".to_string(),
                src: 1,
                return_count: false,
            }
        );
        assert_eq!(
            parsed.window,
            QueryWindow {
                skip: 2,
                limit: Some(3)
            }
        );
        assert_eq!(parsed.columns, vec![QueryColumn::new("v.id")]);
    }

    #[test]
    fn folds_constant_return_skip_and_limit_expressions() {
        let parsed = parse_opencypher_with_window(
            "MATCH (u {id: 1})-[:FOLLOWS]->(v) RETURN v.id SKIP 1 + 1 LIMIT 6 / 2",
        )
        .unwrap();
        assert_eq!(
            parsed.window,
            QueryWindow {
                skip: 2,
                limit: Some(3)
            }
        );
        assert_eq!(parsed.columns, vec![QueryColumn::new("v.id")]);
    }

    #[test]
    fn preserves_count_projection_column_for_planning() {
        let parsed =
            parse_opencypher_with_window("MATCH (u {id: 1})-[:FOLLOWS]->(v) RETURN count(*)")
                .unwrap();
        assert_eq!(parsed.columns, vec![QueryColumn::new("count(*)")]);
    }

    #[test]
    fn preserves_aliased_projection_columns_for_planning() {
        let vertex = parse_opencypher_with_window(
            "MATCH (u {id: 1})-[:FOLLOWS]->(v) RETURN v.id AS vertex_id",
        )
        .unwrap();
        assert_eq!(vertex.columns, vec![QueryColumn::new("vertex_id")]);

        let count = parse_opencypher_with_window(
            "MATCH (u {id: 1})-[:FOLLOWS]->(v) RETURN count(*) AS total",
        )
        .unwrap();
        assert_eq!(count.columns, vec![QueryColumn::new("total")]);
    }

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
    fn lowers_row_query_with_labels_properties_and_property_projection() {
        let parsed = parse_opencypher_row_query(
            "MATCH (u:User {active: true})-[:FOLLOWS]->(v:User {age: 42}) \
             RETURN u.name AS src, v.age AS age ORDER BY v.age DESC",
        )
        .unwrap();
        let RowPattern::Edge(pattern) = &parsed.pattern else {
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
    fn lowers_node_only_row_query() {
        let parsed = parse_opencypher_row_query(
            "MATCH (u:User {active: true}) RETURN u.name AS name ORDER BY u.name",
        )
        .unwrap();
        let RowPattern::Node(node) = &parsed.pattern else {
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
    fn rejects_negative_return_window_expressions() {
        assert!(matches!(
            parse_opencypher_with_window(
                "MATCH (u {id: 1})-[:FOLLOWS]->(v) RETURN v.id SKIP 1 - 2 LIMIT 3"
            ),
            Err(GraphError::UnsupportedQuery { .. })
        ));
    }

    #[test]
    fn statement_only_parser_rejects_windowed_returns() {
        assert!(matches!(
            parse_opencypher("MATCH (u {id: 1})-[:FOLLOWS]->(v) RETURN v.id LIMIT 1"),
            Err(GraphError::UnsupportedQuery { .. })
        ));
    }
}
