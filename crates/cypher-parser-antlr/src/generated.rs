//! ANTLR recognizers generated at build time from `query-language/cypher/grammar`.
//!
//! These modules are public so early parser experiments can use the complete
//! typed parse-tree surface. Application code should prefer the stable facade
//! in this crate once it has the AST node it needs.

pub mod cypher25_lexer {
    include!(concat!(env!("OUT_DIR"), "/generated/cypher25_lexer.rs"));
}

pub mod cypher25_parser {
    include!(concat!(env!("OUT_DIR"), "/generated/cypher25_parser.rs"));
}
