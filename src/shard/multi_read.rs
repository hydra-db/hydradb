use std::future::Future;
use std::pin::Pin;

use super::*;
use crate::query::opencypher::{classify_opencypher_query_access, OpenCypherQueryAccess};
use crate::query::path_procedure::{parse_native_multi_read_procedure, NativeMultiReadProcedure};
use crate::{QueryColumn, QueryResultSet, QueryRow, QueryValue, VertexPropertyValue};

type NativeMultiReadFuture<'a> = Pin<Box<dyn Future<Output = Result<QueryResultSet>> + Send + 'a>>;

impl GraphShard {
    pub(crate) fn native_multi_read_rows_if_present<'a>(
        &'a self,
        context: QueryContext,
        query: &str,
    ) -> Result<Option<NativeMultiReadFuture<'a>>> {
        let Some(procedure) = parse_native_multi_read_procedure(query)? else {
            return Ok(None);
        };
        Ok(Some(
            self.execute_native_multi_read_rows(context, procedure),
        ))
    }

    pub(crate) fn execute_native_multi_read_rows(
        &self,
        context: QueryContext,
        procedure: NativeMultiReadProcedure,
    ) -> NativeMultiReadFuture<'_> {
        Box::pin(async move {
            if context.read_epoch.is_some() && context.validated_read_epoch().is_none() {
                return Err(GraphError::UnsupportedQuery {
                    dialect: "OpenCypher",
                    feature: "historical graph epochs are not storage snapshots; execute against a current SlateDB snapshot"
                        .to_string(),
                });
            }
            if context.read_epoch.is_none() {
                let snapshot = if context.uses_refreshed_reader() {
                    self.db.reader_snapshot().await?
                } else {
                    self.db.snapshot().await?
                };
                let read_epoch = snapshot.seq();
                let context = context.with_validated_storage_read_epoch(read_epoch, read_epoch);
                return GraphStore::scope_snapshot(
                    snapshot,
                    self.execute_native_multi_read_rows_inner(context, procedure),
                )
                .await;
            }
            self.execute_native_multi_read_rows_inner(context, procedure)
                .await
        })
    }

    fn execute_native_multi_read_rows_inner(
        &self,
        context: QueryContext,
        procedure: NativeMultiReadProcedure,
    ) -> NativeMultiReadFuture<'_> {
        Box::pin(async move {
            let read_epoch =
                context
                    .validated_read_epoch()
                    .ok_or_else(|| GraphError::UnsupportedQuery {
                        dialect: "OpenCypher",
                        feature: "algo.MultiRead requires a validated storage snapshot".to_string(),
                    })?;
            let storage_sequence = context.validated_storage_sequence().unwrap_or(read_epoch);
            let result_limit = self.limits.max_query_result_vertices;
            let mut rows = Vec::new();

            for operation in procedure.operations {
                if parse_native_multi_read_procedure(&operation.query)?.is_some() {
                    return Err(GraphError::UnsupportedQuery {
                        dialect: "OpenCypher",
                        feature: "nested algo.MultiRead calls are not supported".to_string(),
                    });
                }
                if classify_opencypher_query_access(&operation.query)?
                    != OpenCypherQueryAccess::Read
                {
                    return Err(GraphError::UnsupportedQuery {
                        dialect: "OpenCypher",
                        feature: format!(
                            "algo.MultiRead operation {} must be read-only",
                            operation.name
                        ),
                    });
                }

                let result = if let Some(path) =
                    crate::query::path_procedure::parse_native_path_procedure(
                        &operation.query,
                        &context.parameters,
                        self.limits.max_traversal_hops,
                    )? {
                    self.execute_native_path_rows(context.clone(), path).await?
                } else {
                    let parsed = self
                        .parsed_opencypher_row_query(
                            &context.cell_id,
                            &operation.query,
                            &context.parameters,
                        )
                        .await?;
                    let operation_context = super::query::merge_opencypher_window(
                        context.clone(),
                        super::query::opencypher_outer_window(&parsed),
                    )?;
                    Box::pin(self.execute_parsed_opencypher_rows_inner(operation_context, parsed))
                        .await?
                };
                let columns = QueryValue::List(
                    result
                        .columns
                        .iter()
                        .map(|column| {
                            QueryValue::Property(VertexPropertyValue::String(column.name.clone()))
                        })
                        .collect(),
                );
                for row in result.rows {
                    if rows.len() >= result_limit {
                        return Err(GraphError::AdmissionRejected {
                            operation: "native_multi_read_rows",
                            actual: rows.len().saturating_add(1) as u64,
                            limit: result_limit as u64,
                        });
                    }
                    rows.push(QueryRow::new(vec![
                        QueryValue::Property(VertexPropertyValue::String(operation.name.clone())),
                        columns.clone(),
                        QueryValue::List(row.values),
                    ]));
                }
            }

            Ok(QueryResultSet::new(
                vec![
                    QueryColumn::new("operation"),
                    QueryColumn::new("columns"),
                    QueryColumn::new("values"),
                ],
                rows,
            )
            .with_read_epoch(read_epoch)
            .with_storage_sequence(storage_sequence))
        })
    }
}
