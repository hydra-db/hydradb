mod mutation;
mod read;
mod route;

use std::sync::Arc;

use hydradb_graph_provider::Result;

use crate::{GraphShard, SlateGraphStore};

pub(crate) use hydradb_graph_slate::property_to_cypher;
pub use read::SlateGraphReadView;

pub(crate) fn statement_receipt_lookup(
    context: &crate::QueryContext,
    query: &str,
) -> crate::Result<hydradb_graph_provider::StatementReceiptLookup> {
    route::statement_receipt_lookup(context, query)
}

pub(crate) async fn scope_legacy_statement_receipt<F>(lookup: hydradb_graph_provider::StatementReceiptLookup, future: F) -> F::Output
where
    F: std::future::Future,
{
    mutation::scope_legacy_statement_receipt(lookup, future).await
}

pub(crate) async fn complete_legacy_statement_receipt(
    shard: &GraphShard,
    lookup: hydradb_graph_provider::StatementReceiptLookup,
    result: &crate::QueryMutationResult,
) -> crate::Result<u64> {
    mutation::complete_legacy_statement_receipt(shard, lookup, result).await
}

pub fn new_slate_graph_provider(shard: Arc<GraphShard>) -> Result<SlateGraphStore> {
    Ok(shard.storage.clone())
}
