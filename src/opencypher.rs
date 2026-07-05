use std::ffi::{CStr, CString};
use std::ptr::null_mut;

use libcypher_parser_sys as sys;

use crate::{GraphError, QueryStatement, QueryWindow, Result, VertexId};

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
        let edge = lower_single_edge_pattern(pattern)?;
        if edge.hop_range.is_some() {
            return unsupported("CREATE does not support variable-length relationships in Phase 0");
        }
        Ok(QueryStatement::CreateEdge {
            edge_type: edge.edge_type,
            src: edge
                .src
                .ok_or_else(|| unsupported_value("CREATE requires source id"))?,
            dst: edge
                .dst
                .ok_or_else(|| unsupported_value("CREATE requires destination id"))?,
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
                });
            }
            return Ok(ParsedQuery {
                statement: QueryStatement::MatchOut {
                    edge_type: edge.edge_type,
                    src,
                    return_count: true,
                },
                window,
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
            return Ok(ParsedQuery {
                statement: QueryStatement::MatchReachable {
                    edge_type: edge.edge_type,
                    src,
                    min_hops,
                    max_hops,
                    return_count: false,
                },
                window,
            });
        }

        if let Some(dst) = fixed_dst {
            if !projects_node_id(expression, edge.dst_binding.as_deref())? {
                return unsupported(
                    "exact edge MATCH currently supports RETURN <dst>.id or count(*)",
                );
            }
            return Ok(ParsedQuery {
                statement: QueryStatement::MatchOutFiltered {
                    edge_type: edge.edge_type,
                    src,
                    dst,
                    return_count: false,
                },
                window,
            });
        }
        if !projects_node_id(expression, edge.dst_binding.as_deref())? {
            return unsupported("open-ended MATCH currently requires RETURN <dst>.id");
        }

        Ok(ParsedQuery {
            statement: QueryStatement::MatchOut {
                edge_type: edge.edge_type,
                src,
                return_count: false,
            },
            window,
        })
    }
}

fn lower_return_window(return_clause: *const AstNode) -> Result<QueryWindow> {
    unsafe {
        let skip_node = sys::cypher_ast_return_get_skip(return_clause);
        let skip = if skip_node.is_null() {
            0
        } else {
            integer_vertex_id(checked_node(skip_node)?)?
        };

        let limit_node = sys::cypher_ast_return_get_limit(return_clause);
        let limit = if limit_node.is_null() {
            None
        } else {
            let limit = integer_vertex_id(checked_node(limit_node)?)?;
            Some(
                usize::try_from(limit)
                    .map_err(|_| unsupported_value("LIMIT exceeds platform usize"))?,
            )
        };

        Ok(QueryWindow { skip, limit })
    }
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
                dst: right_node.id,
                dst_binding: right_node.binding,
                hop_range,
            }),
            sys::cypher_rel_direction::CYPHER_REL_INBOUND => Ok(EdgePattern {
                edge_type,
                src: right_node.id,
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

    unsafe {
        if !is_instance(expression, sys::CYPHER_AST_PROPERTY_OPERATOR) {
            return Ok(false);
        }

        let prop = checked_node(sys::cypher_ast_property_operator_get_prop_name(expression))?;
        if !prop_name(prop)?.eq_ignore_ascii_case("id") {
            return Ok(false);
        }

        let base = checked_node(sys::cypher_ast_property_operator_get_expression(expression))?;
        if !is_instance(base, sys::CYPHER_AST_IDENTIFIER) {
            return Ok(false);
        }
        Ok(identifier_name(base)?.eq(binding))
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
    }

    #[test]
    fn statement_only_parser_rejects_windowed_returns() {
        assert!(matches!(
            parse_opencypher("MATCH (u {id: 1})-[:FOLLOWS]->(v) RETURN v.id LIMIT 1"),
            Err(GraphError::UnsupportedQuery { .. })
        ));
    }
}
