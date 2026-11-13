use std::sync::Arc;

use hydradb_cypher_ast::{CypherDialect, CypherVersion, Document};
use hydradb_graph_plan::{explain_graph_ir, GraphIr, MutationPolicy};

use crate::bind::bind_document;
use crate::cache::{ParameterType, TemplateKey};
use crate::classification::{classify_document, QueryEffect};
use crate::error::{DfError, DfResult};

pub(crate) fn dialect_key(dialect: &CypherDialect) -> String {
    match dialect {
        CypherDialect::OpenCypher => "open-cypher".to_owned(),
        CypherDialect::Neo4j(CypherVersion::V5) => "neo4j-cypher-5".to_owned(),
        CypherDialect::Neo4j(CypherVersion::V25) => "neo4j-cypher-25".to_owned(),
        CypherDialect::Other(name) => format!("other:{name}"),
    }
}

/// Immutable, request-independent frontend output. The document and graph IR
/// contain parameter slots only; a caller must supply fresh values, provider
/// state, snapshot, and mutation session when lowering/executing a request.
#[derive(Clone, Debug)]
pub struct PreparedTemplate {
    key: TemplateKey,
    document: Arc<Document>,
    graph_ir: Arc<GraphIr>,
    effect: QueryEffect,
}

impl PreparedTemplate {
    pub fn compile(
        query: &str,
        parameter_types: impl IntoIterator<Item = (String, ParameterType)>,
        schema_revision: u64,
        semantic_codec_version: u32,
        capability_fingerprint: [u8; 32],
        mutation_policy: MutationPolicy,
    ) -> DfResult<Self> {
        let document = hydradb_cypher_parser_antlr::parse_cypher25(query).map_err(|error| DfError::Invalid(error.to_string()))?;
        let effect = classify_document(&document);
        let key = TemplateKey::new(
            dialect_key(&document.dialect),
            query,
            parameter_types,
            schema_revision,
            semantic_codec_version,
            capability_fingerprint,
            mutation_policy,
        );
        let graph_ir = bind_document(&document, mutation_policy)?;
        Ok(Self {
            key,
            document: Arc::new(document),
            graph_ir: Arc::new(graph_ir),
            effect,
        })
    }

    pub fn key(&self) -> &TemplateKey {
        &self.key
    }

    pub fn document(&self) -> &Arc<Document> {
        &self.document
    }

    pub fn graph_ir(&self) -> &Arc<GraphIr> {
        &self.graph_ir
    }

    pub fn effect(&self) -> QueryEffect {
        self.effect
    }

    /// Stable, parameter-redacted graph IR explanation.
    pub fn explain(&self) -> String {
        explain_graph_ir(&self.graph_ir)
    }
}
