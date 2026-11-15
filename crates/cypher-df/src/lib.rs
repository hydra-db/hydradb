//! Cypher-to-DataFusion frontend over the backend-neutral graph IR.
//!
//! Prepared templates are immutable. Every execution lowers the template with
//! fresh parameters and a fresh graph execution context, so snapshots and
//! mutation sessions are never cached.

mod bind;
mod cache;
mod classification;
mod engine;
mod error;
mod lower;
mod template;

pub use bind::bind_document;
pub use cache::{ParameterType, TemplateCache, TemplateKey};
pub use classification::{classify_document, classify_query, QueryEffect};
pub use engine::CypherDfEngine;
pub use error::{DfError, DfResult};
pub use lower::{lower_graph_ir, LoweringState};
pub use template::PreparedTemplate;
