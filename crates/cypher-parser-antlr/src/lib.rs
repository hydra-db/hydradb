//! A pure-Rust ANTLR Cypher 25 frontend generated from Neo4j's current grammar.
//!
//! The crate deliberately stops at the syntax boundary:
//!
//! ```text
//! query text -> generated lexer -> generated parser -> validated syntax tree
//!                                                   ---------------------
//!                                                    `parse_cypher25_syntax`
//!                                                          |
//!                                                          v
//!                                                owned shared Cypher AST
//!                                                    `parse_cypher25`
//! ```
//!
//! Semantic lowering into HydraDB's future graph logical IR belongs after the
//! shared-AST boundary. Keeping those stages separate avoids leaking generated
//! parser details into planning or execution.

pub mod generated;

mod lower;

use generated::{
    cypher25_lexer::Cypher25Lexer,
    cypher25_parser::{self, Cypher25Parser},
};

#[derive(Clone, Debug)]
struct GqlLexicalPredicates;

impl antlr4_runtime::SemanticHooks for GqlLexicalPredicates {
    fn lexer_sempred<I: antlr4_runtime::CharStream>(
        &mut self,
        context: &mut antlr4_runtime::LexerSemCtx<'_, I>,
        rule: usize,
        predicate: usize,
    ) -> Option<bool> {
        match (generated::cypher25_lexer::rule_names().get(rule).copied(), predicate) {
            // §21.2 permits a trailing decimal period, but the first period
            // of a path range belongs to DOTDOT, not to the numeric token.
            (Some("DECIMAL_DOUBLE"), 0) => Some(context.la(1) != i32::from(b'.')),
            _ => None,
        }
    }
}

pub use generated::cypher25_parser::{Cypher25ValidatedTree, Cypher25ValidationError};

/// Parse one or more Cypher 25 statements into a syntax-clean, structurally
/// validated generated tree.
///
/// The returned tree provides typed contexts for every production in Neo4j's
/// Cypher 25 grammar. It is the source for the canonical AST builder that will
/// sit between this parser and HydraDB's common graph IR.
pub fn parse_cypher25_syntax(source: &str) -> Result<Cypher25ValidatedTree, Cypher25ValidationError> {
    cypher25_parser::parse_validated(
        source,
        |input| Cypher25Lexer::with_hooks(input, GqlLexicalPredicates),
        Cypher25Parser::statements,
    )
}

/// Parse Cypher 25 into the parser-independent, owned syntax model.
///
/// The generated syntax tree remains available from [`parse_cypher25_syntax`]
/// for consumers that need full grammar coverage. The shared AST currently
/// lowers the read-clause subset and typed graph mutations, including
/// `CREATE`, `MERGE`, `SET`, `REMOVE`, `DELETE`, `DETACH DELETE`, `FOREACH`,
/// and procedure/subquery `CALL` forms. Valid syntax outside that subset is
/// retained as an owned [`hydradb_cypher_ast::SyntaxFragment`].
pub fn parse_cypher25(source: &str) -> Result<hydradb_cypher_ast::Document, Cypher25ValidationError> {
    let syntax = parse_cypher25_syntax(source)?;
    Ok(lower::lower_cypher25(source, &syntax))
}

/// Compatibility spelling for the former generated-tree-only API.
pub fn parse(source: &str) -> Result<Cypher25ValidatedTree, Cypher25ValidationError> {
    parse_cypher25_syntax(source)
}
