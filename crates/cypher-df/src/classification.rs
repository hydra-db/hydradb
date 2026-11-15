use hydradb_cypher_ast::{Document, Statement, WriteClassification};

/// Parse and classify without binding, lowering, or opening graph storage.
/// Service boundaries use this before authorization and scoped preparation.
pub fn classify_query(query: &str) -> crate::DfResult<QueryEffect> {
    let document = hydradb_cypher_parser_antlr::parse_cypher25(query).map_err(|error| crate::DfError::Invalid(error.to_string()))?;
    Ok(classify_document(&document))
}

/// Conservative effect classification made before opening provider state.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum QueryEffect {
    ReadOnly,
    Writes,
    Unknown,
}

impl QueryEffect {
    pub const fn may_write(self) -> bool {
        !matches!(self, Self::ReadOnly)
    }
}

/// Classify every statement before planning or execution. Unknown syntax and
/// procedures stay on the potentially-writing route; this layer must never
/// infer read-only from the absence of a recognized write clause.
pub fn classify_document(document: &Document) -> QueryEffect {
    let mut classification = WriteClassification::ReadOnly;
    for statement in &document.statements {
        let effect = match statement {
            Statement::Query(query) => query.write_classification(),
            Statement::Syntax(_) => WriteClassification::Unknown,
        };
        classification = combine(classification, effect);
    }
    match classification {
        WriteClassification::ReadOnly => QueryEffect::ReadOnly,
        WriteClassification::Writes => QueryEffect::Writes,
        WriteClassification::Unknown => QueryEffect::Unknown,
    }
}

fn combine(left: WriteClassification, right: WriteClassification) -> WriteClassification {
    match (left, right) {
        (WriteClassification::Unknown, _) | (_, WriteClassification::Unknown) => WriteClassification::Unknown,
        (WriteClassification::Writes, _) | (_, WriteClassification::Writes) => WriteClassification::Writes,
        _ => WriteClassification::ReadOnly,
    }
}
