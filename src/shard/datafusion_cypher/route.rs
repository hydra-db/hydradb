use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use arrow_array::{Array, BooleanArray, ListArray, MapArray, RecordBatch, StringArray, StructArray, UInt64Array, UInt8Array};
use datafusion::common::{DataFusionError, ScalarValue};
use futures::StreamExt;
use hydradb_cypher_df::{CypherDfEngine, DfError, PreparedTemplate};
use hydradb_graph_datafusion::{context::GraphExecutionContext, ProviderGraphOperatorRuntime, ProviderMutationProgramRuntime};
use hydradb_graph_plan::{
    EntityKind, GraphExpression, GraphIr, GraphIrKind, GraphSeekPredicate, MutationOperation, MutationPolicy, Projection,
};
use hydradb_graph_provider::{
    BudgetLimits, CancellationToken, CommitToken, GraphProvider, GraphProviderError, GraphReadView, GraphScope as ProviderScope,
    IdempotencyIdentity, MissingEntityPolicy, MutationAtomicityPolicy as ProviderAtomicityPolicy, MutationFingerprint, MutationRequest,
    NodeFetchKey, NodeFetchRequest, PropertyProjection, ReadOperation, ReadRequest, ReadRequirements, RelationshipFetchKey,
    RelationshipFetchRequest, StatementReceiptLookup, StatementReceiptOrigin, StatementReceiptResolution, StatementSemanticsVersion,
    WriterAdmissionToken,
};
use hydradb_graph_types::{
    canonical_value_data_type, decode_value, encode_value, encode_value_stable, value_from_array, CypherValue, NodeEntity, NodeId, Path,
    PathRelationship, RelationshipEntity, RelationshipId, RelationshipIdentity, RelationshipRef, TraversalOrientation,
};
use sha2::{Digest, Sha256};

use crate::{
    GraphError, GraphShard, MutationAtomicityPolicy, PreparedCypherEffect, PreparedCypherQuery, QueryCancellationToken, QueryColumn,
    QueryContext, QueryFailureReason, QueryMutationResult, QueryOutput, QueryResultPage, QueryResultSet, QueryRow, QueryValue,
    StorageSequence,
};

use super::{new_slate_graph_provider, property_to_cypher};

const DATAFUSION_SCHEMA_REVISION: u64 = 1;
const DATAFUSION_SEMANTIC_CODEC_VERSION: u32 = 1;
const DEFAULT_RESULT_BYTES: u64 = 64 * 1024 * 1024;
const MAX_BATCH_ROWS: u64 = 4096;
const MAX_BATCH_BYTES: u64 = 8 * 1024 * 1024;
const MAX_IO_CONCURRENCY: usize = 16;

fn legacy_mutation_result_columns(context: &QueryContext, query: &str) -> Result<Vec<QueryColumn>, GraphError> {
    let mutation = crate::parse_opencypher_mutation_query_with_list_parameters(query, &context.parameters, &context.list_parameters)?;
    let Some(mutation) = mutation else {
        return Err(GraphError::CorruptValue {
            key: "query/legacy/receipt_columns".to_string(),
            reason: "legacy statement receipt does not match a parsed mutation".to_string(),
        });
    };
    Ok(mutation.returning.map(|returning| vec![returning.column]).unwrap_or_default())
}

fn statement_replay_rows(
    columns: &[QueryColumn],
    return_manifest: Vec<hydradb_graph_provider::ReturnManifestRow>,
    legacy_receipt: bool,
    max_rows: u64,
    max_result_bytes: u64,
) -> Result<Vec<QueryRow>, GraphError> {
    if columns.is_empty() {
        return Ok(Vec::new());
    }
    let mut rows = Vec::new();
    let mut result_bytes = 0_u64;
    for manifest_row in return_manifest {
        for manifest_values in manifest_row.rows {
            let next_rows = u64::try_from(rows.len()).unwrap_or(u64::MAX).saturating_add(1);
            if next_rows > max_rows {
                return Err(GraphError::AdmissionRejected {
                    operation: "datafusion_query_rows",
                    actual: next_rows,
                    limit: max_rows,
                });
            }
            if manifest_values.len() != columns.len() {
                return Err(GraphError::CorruptValue {
                    key: "query/datafusion/receipt_columns".to_string(),
                    reason: format!(
                        "statement receipt row has {} values but the public query has {} columns",
                        manifest_values.len(),
                        columns.len()
                    ),
                });
            }
            let values = manifest_values
                .iter()
                .map(|value| {
                    // Legacy mutation RETURN currently supports count(binding),
                    // whose canonical manifest value is an integer. Restore
                    // the historical public QueryValue variant on replay.
                    if legacy_receipt {
                        if let CypherValue::Integer(count) = value {
                            return u64::try_from(*count).map(QueryValue::Count).map_err(|_| GraphError::CorruptValue {
                                key: "query/legacy/receipt_value".to_string(),
                                reason: format!("legacy mutation count {count} is outside the public count range"),
                            });
                        }
                    }
                    encode_value(value)
                        .map(QueryValue::GraphValue)
                        .map_err(|error| GraphError::CorruptValue {
                            key: "query/datafusion/receipt_value".to_string(),
                            reason: error.to_string(),
                        })
                })
                .collect::<Result<Vec<_>, _>>()?;
            let row = QueryRow::new(values);
            let next_bytes = result_bytes.saturating_add(row.estimated_resident_bytes());
            if next_bytes > max_result_bytes {
                return Err(GraphError::AdmissionRejected {
                    operation: "datafusion_query_result_bytes",
                    actual: next_bytes,
                    limit: max_result_bytes,
                });
            }
            result_bytes = next_bytes;
            rows.push(row);
        }
    }
    Ok(rows)
}

impl GraphShard {
    pub(crate) fn register_datafusion_self(&self, shard: &Arc<Self>) {
        let _ = self.datafusion_self.set(Arc::downgrade(shard));
    }

    pub(crate) fn shared_datafusion_self(&self) -> Result<Arc<Self>, GraphError> {
        self.datafusion_self
            .get()
            .and_then(std::sync::Weak::upgrade)
            .ok_or_else(|| GraphError::UnsupportedQuery {
                reason: QueryFailureReason::Other,
                dialect: "DataFusionCypher",
                feature: "DataFusion execution requires a GraphCluster-owned shard".to_string(),
            })
    }

    pub(crate) async fn prepare_datafusion_cypher(&self, context: &QueryContext, query: &str) -> Result<PreparedCypherQuery, GraphError> {
        let parameters = cypher_parameters(context)?;
        let template = self.prepare_datafusion_template(context, query, &parameters).await?;
        let effect = match template.effect() {
            hydradb_cypher_df::QueryEffect::ReadOnly => PreparedCypherEffect::Read,
            hydradb_cypher_df::QueryEffect::Writes | hydradb_cypher_df::QueryEffect::Unknown => PreparedCypherEffect::Write,
        };
        Ok(PreparedCypherQuery {
            effect,
            columns: result_columns(template.graph_ir()),
        })
    }

    pub(crate) async fn resolve_datafusion_statement_receipt(
        &self,
        context: &QueryContext,
        query: &str,
    ) -> Result<Option<QueryResultSet>, GraphError> {
        // Receipt replay is a separate entry point and can precede preparation.
        self.ensure_managed_cypher_layout().await?;
        if context.idempotency_key.is_empty() {
            return Ok(None);
        }
        let parameters = cypher_parameters(context)?;
        let lookup = statement_receipt_lookup(context, query)?;
        let provider = new_slate_graph_provider(self.shared_datafusion_self()?).map_err(provider_error_to_graph)?;
        match provider.resolve_statement_receipt(lookup).await.map_err(provider_error_to_graph)? {
            StatementReceiptResolution::NotFound => Ok(None),
            StatementReceiptResolution::InProgress {
                origin, completed_actions, ..
            } => {
                ensure_receipt_origin_matches(context, origin, &completed_actions)?;
                Ok(None)
            }
            StatementReceiptResolution::Committed { origin, replay } => {
                if replay.receipt.durability != hydradb_graph_provider::CommitDurability::Durable {
                    return Err(provider_error_to_graph(GraphProviderError::DurabilityNotConfirmed));
                }
                let columns = match origin {
                    StatementReceiptOrigin::Legacy => legacy_mutation_result_columns(context, query)?,
                    StatementReceiptOrigin::DataFusion => {
                        let template = self.prepare_datafusion_template(context, query, &parameters).await?;
                        result_columns(template.graph_ir())
                    }
                };
                // Legacy checkpoint receipts can have one empty manifest row
                // per matched input even when the statement has no RETURN.
                // The public result of a no-RETURN statement is always empty.
                let rows = statement_replay_rows(
                    &columns,
                    replay.return_manifest,
                    origin == StatementReceiptOrigin::Legacy,
                    self.limits.max_query_result_vertices.max(1) as u64,
                    context.max_result_bytes.unwrap_or(DEFAULT_RESULT_BYTES),
                )?;
                let sequence =
                    u64::from_be_bytes(
                        replay
                            .receipt
                            .commit_token
                            .as_bytes()
                            .try_into()
                            .map_err(|_| GraphError::CorruptValue {
                                key: "query/datafusion/receipt_commit_token".to_string(),
                                reason: "SlateDB commit token must contain exactly eight sequence bytes".to_string(),
                            })?,
                    ) as StorageSequence;
                Ok(Some(QueryResultSet::new(columns, rows).with_storage_sequence(sequence)))
            }
        }
    }

    pub(crate) async fn execute_datafusion_cypher(self: &Arc<Self>, context: QueryContext, query: &str) -> Result<QueryOutput, GraphError> {
        let parameters = cypher_parameters(&context)?;
        let template = self.prepare_datafusion_template(&context, query, &parameters).await?;
        let columns = result_columns(template.graph_ir());
        let result_kinds = result_value_kinds(template.graph_ir());
        let query_effect = template.effect();
        let selected_policy = context.mutation_atomicity_policy;
        let engine_policy = engine_mutation_policy(selected_policy);
        let provider_scope = provider_scope(&context)?;
        let provider = Arc::new(new_slate_graph_provider(Arc::clone(self)).map_err(provider_error_to_graph)?);
        let capabilities = provider.capabilities(&provider_scope);
        let provider_id = provider.provider_id().clone();
        let request_context = context.clone();
        let query_cancel = context.cancellation_token.clone();
        let provider_cancel = CancellationToken::new();
        let budget = budget_limits(self, &context)?;
        let max_result_bytes = context.max_result_bytes.unwrap_or(DEFAULT_RESULT_BYTES);
        let max_rows = self.limits.max_query_result_vertices.max(1) as u64;
        let precommit_results = Arc::new(Mutex::new(PrecommitMutationResults::default()));
        let deadline = context
            .max_runtime_ms
            .or(self.limits.max_query_runtime_ms)
            .map(|limit| Instant::now() + Duration::from_millis(limit));
        let runtime_context = Arc::new(Mutex::new(None::<Arc<GraphExecutionContext>>));
        let runtime_context_for_factory = Arc::clone(&runtime_context);
        let provider_for_factory = Arc::clone(&provider);
        let shard_for_factory = Arc::clone(self);
        let provider_scope_for_factory = provider_scope.clone();
        let provider_id_for_factory = provider_id.clone();
        let provider_cancel_for_factory = provider_cancel.clone();
        let precommit_results_for_factory = Arc::clone(&precommit_results);
        let budget_for_factory = budget;
        let deadline_for_factory = deadline;
        let request_context_for_factory = request_context.clone();
        let result_kinds_for_factory = result_kinds.clone();
        let result_column_count = columns.len();
        let max_result_bytes_for_factory = max_result_bytes;
        let max_rows_for_factory = max_rows;
        let started = Instant::now();

        let cancellation_task = query_cancel.clone().map(|query_cancel| {
            let provider_cancel = provider_cancel.clone();
            tokio::spawn(async move {
                query_cancel.cancelled().await;
                provider_cancel.cancel();
            })
        });

        let result =
            self.datafusion_engine()?
                .execute(
                    template,
                    parameters,
                    engine_policy,
                    move |effect, graph_ir, bound_parameters| async move {
                        let runtime_parameters = runtime_parameter_map(bound_parameters.as_ref());
                        let operator_runtime = Arc::new(ProviderGraphOperatorRuntime::new());
                        let read_requirements = read_requirements(&graph_ir).map_err(provider_error_to_datafusion)?;
                        if effect == hydradb_cypher_df::QueryEffect::ReadOnly {
                            let read_request = ReadRequest {
                                scope: provider_scope_for_factory.clone(),
                                causal_lower_bound: request_context_for_factory
                                    .read_epoch
                                    .map(|epoch| {
                                        CommitToken::new(
                                            provider_id_for_factory.clone(),
                                            provider_scope_for_factory.clone(),
                                            epoch.to_be_bytes().to_vec(),
                                        )
                                    })
                                    .transpose()
                                    .map_err(provider_error_to_datafusion)?,
                                deadline: deadline_for_factory,
                                cancellation: provider_cancel_for_factory.clone(),
                                budget: budget_for_factory,
                                max_io_concurrency: MAX_IO_CONCURRENCY,
                                requirements: read_requirements,
                            };
                            capabilities.validate_read(&read_request).map_err(provider_error_to_datafusion)?;
                            let view = provider_for_factory
                                .open_read(read_request.clone())
                                .await
                                .map_err(provider_error_to_datafusion)?;
                            let graph_context = Arc::new(
                                GraphExecutionContext::for_read(
                                    view,
                                    capabilities.clone(),
                                    &read_request,
                                    runtime_parameters,
                                    operator_runtime,
                                )
                                .map_err(datafusion_error_to_datafusion)?,
                            );
                            *runtime_context_for_factory
                                .lock()
                                .map_err(|_| DataFusionError::Execution("DataFusion query context lock was poisoned".into()))? =
                                Some(Arc::clone(&graph_context));
                            Ok(graph_context)
                        } else {
                            validate_slate_mutation_program(&graph_ir).map_err(provider_error_to_datafusion)?;
                            let writer_epoch = shard_for_factory.storage.writer_epoch().ok_or_else(|| {
                                DataFusionError::Execution("DataFusion mutation requires an admitted SlateDB writer".into())
                            })?;
                            let mutation_request = MutationRequest {
                                scope: provider_scope_for_factory.clone(),
                                writer_admission: WriterAdmissionToken::new(writer_epoch.to_be_bytes().to_vec(), writer_epoch)
                                    .map_err(provider_error_to_datafusion)?,
                                idempotency: IdempotencyIdentity::new(
                                    if request_context_for_factory.idempotency_principal.is_empty() {
                                        "anonymous".to_string()
                                    } else {
                                        request_context_for_factory.idempotency_principal.clone()
                                    },
                                    request_context_for_factory.idempotency_key.clone(),
                                )
                                .map_err(provider_error_to_datafusion)?,
                                fingerprint: mutation_fingerprint(query, &bound_parameters).map_err(provider_error_to_datafusion)?,
                                atomicity: provider_atomicity_policy(selected_policy),
                                semantics_version: StatementSemanticsVersion(DATAFUSION_SEMANTIC_CODEC_VERSION),
                                deadline: deadline_for_factory,
                                cancellation: provider_cancel_for_factory.clone(),
                                budget: budget_for_factory,
                                max_io_concurrency: MAX_IO_CONCURRENCY,
                                requirements: Default::default(),
                            };
                            let mut validation_request = ReadRequest {
                                scope: provider_scope_for_factory.clone(),
                                causal_lower_bound: None,
                                deadline: deadline_for_factory,
                                cancellation: provider_cancel_for_factory.clone(),
                                budget: budget_for_factory,
                                max_io_concurrency: MAX_IO_CONCURRENCY,
                                requirements: read_requirements,
                            };
                            validation_request.requirements.read_own_writes = true;
                            capabilities
                                .validate_read(&validation_request)
                                .and_then(|()| capabilities.validate_mutation(&mutation_request))
                                .map_err(provider_error_to_datafusion)?;
                            let session = provider_for_factory
                                .begin_mutation(mutation_request.clone())
                                .await
                                .map_err(provider_error_to_datafusion)?;
                            let graph_context = Arc::new(
                                GraphExecutionContext::for_mutation(
                                    session,
                                    capabilities.clone(),
                                    &mutation_request,
                                    runtime_parameters,
                                    operator_runtime,
                                    Arc::new(ProviderMutationProgramRuntime::new()),
                                )
                                .map_err(datafusion_error_to_datafusion)?,
                            );
                            let validation_results = Arc::clone(&precommit_results_for_factory);
                            let validation_kinds = result_kinds_for_factory.clone();
                            graph_context
                                .set_mutation_result_validator(Arc::new(move |batch, materialized_rows| {
                                    if batch.num_columns() != result_column_count {
                                        return Err(graph_error_to_datafusion(GraphError::CorruptValue {
                                            key: "query/datafusion/result_columns".to_string(),
                                            reason: format!(
                                                "prepared template has {result_column_count} columns but mutation RETURN returned {}",
                                                batch.num_columns()
                                            ),
                                        }));
                                    }
                                    if !materialized_rows.is_empty() && materialized_rows.len() != batch.num_rows() {
                                        return Err(DataFusionError::Execution(
                                            "mutation result values do not match their Arrow row batch".into(),
                                        ));
                                    }
                                    let mut output = validation_results
                                        .lock()
                                        .map_err(|_| DataFusionError::Execution("precommit result buffer lock was poisoned".into()))?;
                                    let next_rows = u64::try_from(output.rows.len())
                                        .unwrap_or(u64::MAX)
                                        .saturating_add(u64::try_from(batch.num_rows()).unwrap_or(u64::MAX));
                                    if next_rows > max_rows_for_factory {
                                        return Err(graph_error_to_datafusion(GraphError::AdmissionRejected {
                                            operation: "datafusion_query_rows",
                                            actual: next_rows,
                                            limit: max_rows_for_factory,
                                        }));
                                    }
                                    for row_index in 0..batch.num_rows() {
                                        let row = if let Some(values) = materialized_rows.get(row_index) {
                                            materialize_result_values(values, &validation_kinds, true).map_err(graph_error_to_datafusion)?
                                        } else {
                                            materialize_result_row(batch, row_index, &validation_kinds, true)
                                                .map_err(graph_error_to_datafusion)?
                                        };
                                        let next_bytes = output.bytes.saturating_add(row.estimated_resident_bytes());
                                        if next_bytes > max_result_bytes_for_factory {
                                            return Err(graph_error_to_datafusion(GraphError::AdmissionRejected {
                                                operation: "datafusion_query_result_bytes",
                                                actual: next_bytes,
                                                limit: max_result_bytes_for_factory,
                                            }));
                                        }
                                        output.bytes = next_bytes;
                                        output.rows.push(row);
                                    }
                                    Ok(())
                                }))
                                .map_err(datafusion_error_to_datafusion)?;
                            *runtime_context_for_factory
                                .lock()
                                .map_err(|_| DataFusionError::Execution("DataFusion query context lock was poisoned".into()))? =
                                Some(Arc::clone(&graph_context));
                            Ok(graph_context)
                        }
                    },
                )
                .await;

        let mut stream = match result {
            Ok(stream) => stream,
            Err(error) => {
                provider_cancel.cancel();
                if let Some(task) = cancellation_task {
                    task.abort();
                }
                abort_mutation_context(&runtime_context).await;
                return Err(cypher_df_error_to_graph(error));
            }
        };

        let mut rows = Vec::new();
        let mut output_bytes = 0_u64;
        let is_mutation = query_effect != hydradb_cypher_df::QueryEffect::ReadOnly;
        while let Some(batch_result) = stream.next().await {
            if !is_mutation && query_cancel.as_ref().is_some_and(QueryCancellationToken::is_cancelled) {
                provider_cancel.cancel();
                abort_mutation_context(&runtime_context).await;
                if let Some(task) = cancellation_task {
                    task.abort();
                }
                return Err(cancelled_error());
            }
            if !is_mutation && deadline.is_some_and(|deadline| Instant::now() >= deadline) {
                provider_cancel.cancel();
                abort_mutation_context(&runtime_context).await;
                if let Some(task) = cancellation_task {
                    task.abort();
                }
                return Err(timeout_error(started, context.max_runtime_ms.or(self.limits.max_query_runtime_ms)));
            }
            let batch = match batch_result {
                Ok(batch) => batch,
                Err(error) => {
                    provider_cancel.cancel();
                    abort_mutation_context(&runtime_context).await;
                    if let Some(task) = cancellation_task {
                        task.abort();
                    }
                    return Err(datafusion_error_to_graph(error));
                }
            };
            if batch.num_columns() != columns.len() {
                provider_cancel.cancel();
                abort_mutation_context(&runtime_context).await;
                if let Some(task) = cancellation_task {
                    task.abort();
                }
                return Err(GraphError::CorruptValue {
                    key: "query/datafusion/result_columns".to_string(),
                    reason: format!(
                        "prepared template has {} columns but execution returned {}",
                        columns.len(),
                        batch.num_columns()
                    ),
                });
            }
            if is_mutation {
                // MutationSink retained the fully materialized public rows
                // after precommit validation. Do not decode the returned
                // Arrow batches a second time after the commit.
                continue;
            }
            let next_rows = (rows.len() as u64).saturating_add(batch.num_rows() as u64);
            if next_rows > max_rows {
                provider_cancel.cancel();
                abort_mutation_context(&runtime_context).await;
                if let Some(task) = cancellation_task {
                    task.abort();
                }
                return Err(GraphError::AdmissionRejected {
                    operation: "datafusion_query_rows",
                    actual: next_rows,
                    limit: max_rows,
                });
            }
            for row_index in 0..batch.num_rows() {
                let row = match materialize_result_row(&batch, row_index, &result_kinds, false) {
                    Ok(row) => row,
                    Err(error) => {
                        provider_cancel.cancel();
                        abort_mutation_context(&runtime_context).await;
                        if let Some(task) = cancellation_task {
                            task.abort();
                        }
                        return Err(error);
                    }
                };
                output_bytes = output_bytes.saturating_add(row.estimated_resident_bytes());
                if output_bytes > max_result_bytes {
                    provider_cancel.cancel();
                    abort_mutation_context(&runtime_context).await;
                    if let Some(task) = cancellation_task {
                        task.abort();
                    }
                    return Err(GraphError::AdmissionRejected {
                        operation: "datafusion_query_result_bytes",
                        actual: output_bytes,
                        limit: max_result_bytes,
                    });
                }
                rows.push(row);
            }
        }
        if let Some(task) = cancellation_task {
            task.abort();
        }
        if is_mutation {
            let mut output = precommit_results.lock().map_err(|_| GraphError::CorruptValue {
                key: "query/datafusion/result_buffer".to_string(),
                reason: "precommit result buffer lock was poisoned".to_string(),
            })?;
            rows = std::mem::take(&mut output.rows);
        }
        let graph_context = runtime_context
            .lock()
            .map_err(|_| GraphError::CorruptValue {
                key: "query/datafusion/context".to_string(),
                reason: "DataFusion query context lock was poisoned".to_string(),
            })?
            .clone()
            .ok_or_else(|| GraphError::CorruptValue {
                key: "query/datafusion/context".to_string(),
                reason: "DataFusion execution completed without a provider context".to_string(),
            })?;
        if matches!(query_effect, hydradb_cypher_df::QueryEffect::ReadOnly) {
            hydrate_public_result_rows(&mut rows, graph_context.read_view()).await?;
            output_bytes = rows.iter().map(QueryRow::estimated_resident_bytes).fold(0_u64, u64::saturating_add);
            if output_bytes > max_result_bytes {
                return Err(GraphError::AdmissionRejected {
                    operation: "datafusion_query_result_bytes",
                    actual: output_bytes,
                    limit: max_result_bytes,
                });
            }
        }
        let read_epoch = graph_context.snapshot_identity().version;
        let mut result_set = QueryResultSet::new(columns, rows);
        if let Some(epoch) = read_epoch {
            result_set = result_set.with_read_epoch(epoch as StorageSequence);
            if matches!(query_effect, hydradb_cypher_df::QueryEffect::ReadOnly) {
                result_set = result_set.with_storage_sequence(epoch as StorageSequence);
            }
        }
        if matches!(query_effect, hydradb_cypher_df::QueryEffect::ReadOnly) {
            return Ok(QueryOutput::Rows(result_set));
        }

        let counters = graph_context
            .mutation_counters()
            .map_err(datafusion_error_to_graph)?
            .unwrap_or_default();
        let receipt = graph_context
            .mutation_commit_receipt()
            .map_err(datafusion_error_to_graph)?
            .ok_or_else(|| GraphError::CorruptValue {
                key: "query/datafusion/commit_receipt".to_string(),
                reason: "DataFusion mutation completed without a validated commit receipt".to_string(),
            })?;
        let sequence_bytes: [u8; 8] = receipt.commit_token.as_bytes().try_into().map_err(|_| GraphError::CorruptValue {
            key: "query/datafusion/commit_token".to_string(),
            reason: "SlateDB commit token must contain exactly eight sequence bytes".to_string(),
        })?;
        let committed_sequence = u64::from_be_bytes(sequence_bytes) as StorageSequence;
        result_set = result_set.with_storage_sequence(committed_sequence);
        let mutation = QueryMutationResult {
            matched_rows: counters.matched,
            created_vertices: counters.nodes_created,
            created_edges: counters.relationships_created,
            created_relationships: counters.relationships_created,
            deleted_edges: counters.relationships_deleted,
            updated_vertices: counters.properties_set.saturating_add(counters.properties_removed),
            updated_relationships: 0,
            noops: 0,
            topology_sequence: Some(committed_sequence),
            returned_rows: Some(result_set),
        };
        Ok(QueryOutput::Mutation(mutation))
    }

    pub(crate) async fn execute_datafusion_cypher_rows(&self, context: QueryContext, query: &str) -> Result<QueryResultSet, GraphError> {
        match self.shared_datafusion_self()?.execute_datafusion_cypher(context, query).await? {
            QueryOutput::Rows(rows) => Ok(rows),
            QueryOutput::Mutation(mutation) => Ok(mutation.into_result_set()),
            output => Err(GraphError::CorruptValue {
                key: "query/datafusion/output".to_string(),
                reason: format!("DataFusion Cypher returned unexpected output {output:?}"),
            }),
        }
    }

    pub(crate) async fn execute_datafusion_cypher_rows_page(
        &self,
        context: QueryContext,
        query: &str,
        cursor: Option<crate::QueryCursorToken>,
        _page_size: usize,
    ) -> Result<QueryResultPage, GraphError> {
        if cursor.is_some() {
            return Err(GraphError::UnsupportedQuery {
                reason: QueryFailureReason::InvalidRequest,
                dialect: "ClientProtocol",
                feature: "DataFusion result pages use the service cursor and cannot resume a shard cursor".to_string(),
            });
        }
        let result = self.shared_datafusion_self()?.execute_datafusion_cypher(context, query).await?;
        let rows = match result {
            QueryOutput::Rows(rows) => rows,
            QueryOutput::Mutation(mutation) => mutation.into_result_set(),
            output => {
                return Err(GraphError::CorruptValue {
                    key: "query/datafusion/page_output".to_string(),
                    reason: format!("DataFusion Cypher returned unexpected page output {output:?}"),
                });
            }
        };
        Ok(QueryResultPage::new(rows.columns, rows.rows, None))
    }

    async fn prepare_datafusion_template(
        &self,
        context: &QueryContext,
        query: &str,
        parameters: &BTreeMap<String, CypherValue>,
    ) -> Result<Arc<PreparedTemplate>, GraphError> {
        // The importer-only release cannot execute DataFusion's managed ID
        // semantics on BYOG data. Guard the shared preparation path so routed
        // clients, direct execution and cached templates obey the same boundary.
        self.ensure_managed_cypher_layout().await?;
        self.datafusion_engine()?
            .prepare(
                query,
                parameters,
                DATAFUSION_SCHEMA_REVISION,
                DATAFUSION_SEMANTIC_CODEC_VERSION,
                [0; 32],
                engine_mutation_policy(context.mutation_atomicity_policy),
            )
            .map_err(|error| GraphError::QueryParse {
                dialect: "Cypher25",
                reason: error.to_string(),
            })
    }

    fn datafusion_engine(&self) -> Result<&CypherDfEngine, GraphError> {
        self.datafusion_cypher_engine
            .get_or_init(|| CypherDfEngine::production().map_err(|error| error.to_string()))
            .as_ref()
            .map_err(|error| GraphError::UnsupportedQuery {
                reason: QueryFailureReason::Other,
                dialect: "Cypher25",
                feature: format!("failed to initialize DataFusion Cypher engine: {error}"),
            })
    }
}

fn validate_slate_mutation_program(graph_ir: &GraphIr) -> Result<(), GraphProviderError> {
    let GraphIrKind::MutationProgram { program, .. } = &graph_ir.kind else {
        return Err(GraphProviderError::InvalidRequest(
            "a write route must contain a mutation program".into(),
        ));
    };
    for step in &program.steps {
        validate_slate_mutation_operation(&step.operation)?;
    }
    Ok(())
}

fn validate_slate_mutation_operation(operation: &MutationOperation) -> Result<(), GraphProviderError> {
    match operation {
        MutationOperation::Delete { detach: true, .. } => Err(GraphProviderError::UnsupportedCapability {
            capability: "DETACH DELETE",
        }),
        MutationOperation::Foreach { steps, .. } => {
            for operation in steps {
                validate_slate_mutation_operation(operation)?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ResultColumnKind {
    Scalar,
    Node,
    Relationship,
    Path,
}

fn result_value_kinds(graph_ir: &GraphIr) -> Vec<ResultColumnKind> {
    match &graph_ir.kind {
        GraphIrKind::Project { items, .. } => items.iter().map(|item| expression_result_kind(&item.expression)).collect(),
        GraphIrKind::MutationProgram { program, .. } => program
            .returning
            .iter()
            .map(|item| expression_result_kind(&item.expression))
            .collect(),
        GraphIrKind::Aggregation { group_by, aggregates, .. } => group_by
            .iter()
            .map(|item| expression_result_kind(&item.expression))
            .chain(std::iter::repeat_n(ResultColumnKind::Scalar, aggregates.len()))
            .collect(),
        GraphIrKind::Union { outputs, .. } => outputs.iter().map(|output| entity_result_kind(output.kind)).collect(),
        GraphIrKind::ResultWindow { input, .. } | GraphIrKind::Sort { input, .. } | GraphIrKind::Window { input, .. } => {
            result_value_kinds(input)
        }
        _ => Vec::new(),
    }
}

fn expression_result_kind(expression: &GraphExpression) -> ResultColumnKind {
    match expression {
        GraphExpression::Binding(binding) => entity_result_kind(binding.kind),
        _ => ResultColumnKind::Scalar,
    }
}

fn entity_result_kind(kind: EntityKind) -> ResultColumnKind {
    match kind {
        EntityKind::Node => ResultColumnKind::Node,
        EntityKind::Relationship => ResultColumnKind::Relationship,
        EntityKind::Path => ResultColumnKind::Path,
        EntityKind::Scalar | EntityKind::List | EntityKind::Map | EntityKind::Any => ResultColumnKind::Scalar,
    }
}

fn decimal_value_to_cypher(value: Option<i128>, scale: i8) -> Result<CypherValue, GraphError> {
    let Some(value) = value else {
        return Ok(CypherValue::Null);
    };
    if scale == 0 {
        return Ok(CypherValue::Integer(value));
    }

    let value = (value as f64) / 10_f64.powi(i32::from(scale));
    if !value.is_finite() {
        return Err(GraphError::UnsupportedQuery {
            reason: QueryFailureReason::Evaluation,
            dialect: "DataFusionCypher",
            feature: "decimal result exceeds the canonical Cypher float range".to_string(),
        });
    }
    Ok(CypherValue::Float(value))
}

fn cypher_value_from_array(array: &dyn Array, row: usize, kind: ResultColumnKind) -> Result<CypherValue, GraphError> {
    if array.is_null(row) {
        return Ok(CypherValue::Null);
    }
    if array.data_type() == &canonical_value_data_type() {
        return value_from_array(array, row).map_err(|error| GraphError::CorruptValue {
            key: "query/datafusion/arrow_value".to_string(),
            reason: error.to_string(),
        });
    }

    match kind {
        ResultColumnKind::Node => {
            let values = array
                .as_any()
                .downcast_ref::<UInt64Array>()
                .ok_or_else(|| GraphError::CorruptValue {
                    key: "query/datafusion/node_binding".to_string(),
                    reason: format!("node binding has unexpected Arrow type {}", array.data_type()),
                })?;
            return Ok(CypherValue::Node(NodeId(values.value(row))));
        }
        ResultColumnKind::Relationship => {
            let values = array
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| GraphError::CorruptValue {
                    key: "query/datafusion/relationship_binding".to_string(),
                    reason: format!("relationship binding has unexpected Arrow type {}", array.data_type()),
                })?;
            return relationship_value(values, row).map(CypherValue::RelationshipEntity);
        }
        ResultColumnKind::Path => {
            let values = array
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| GraphError::CorruptValue {
                    key: "query/datafusion/path_binding".to_string(),
                    reason: format!("path binding has unexpected Arrow type {}", array.data_type()),
                })?;
            return path_value(values, row).map(CypherValue::Path);
        }
        ResultColumnKind::Scalar => {}
    }

    let scalar = ScalarValue::try_from_array(array, row).map_err(|error| GraphError::UnsupportedQuery {
        reason: QueryFailureReason::Evaluation,
        dialect: "DataFusionCypher",
        feature: format!("result value type {} could not be decoded: {error}", array.data_type()),
    })?;
    match scalar {
        ScalarValue::Null => Ok(CypherValue::Null),
        ScalarValue::Boolean(value) => Ok(value.map(CypherValue::Boolean).unwrap_or(CypherValue::Null)),
        ScalarValue::Int8(value) => Ok(value
            .map(|value| CypherValue::Integer(i128::from(value)))
            .unwrap_or(CypherValue::Null)),
        ScalarValue::Int16(value) => Ok(value
            .map(|value| CypherValue::Integer(i128::from(value)))
            .unwrap_or(CypherValue::Null)),
        ScalarValue::Int32(value) => Ok(value
            .map(|value| CypherValue::Integer(i128::from(value)))
            .unwrap_or(CypherValue::Null)),
        ScalarValue::Int64(value) => Ok(value
            .map(|value| CypherValue::Integer(i128::from(value)))
            .unwrap_or(CypherValue::Null)),
        ScalarValue::Decimal32(value, _, scale) => decimal_value_to_cypher(value.map(i128::from), scale),
        ScalarValue::Decimal64(value, _, scale) => decimal_value_to_cypher(value.map(i128::from), scale),
        ScalarValue::Decimal128(value, _, scale) => decimal_value_to_cypher(value, scale),
        ScalarValue::Decimal256(value, _, 0) => value
            .map(|value| {
                value
                    .to_string()
                    .parse::<i128>()
                    .map(CypherValue::Integer)
                    .map_err(|_| GraphError::UnsupportedQuery {
                        reason: QueryFailureReason::Evaluation,
                        dialect: "DataFusionCypher",
                        feature: "256-bit decimal result exceeds the canonical Cypher integer range".to_string(),
                    })
            })
            .unwrap_or(Ok(CypherValue::Null)),
        ScalarValue::UInt8(value) => Ok(value
            .map(|value| CypherValue::Integer(i128::from(value)))
            .unwrap_or(CypherValue::Null)),
        ScalarValue::UInt16(value) => Ok(value
            .map(|value| CypherValue::Integer(i128::from(value)))
            .unwrap_or(CypherValue::Null)),
        ScalarValue::UInt32(value) => Ok(value
            .map(|value| CypherValue::Integer(i128::from(value)))
            .unwrap_or(CypherValue::Null)),
        ScalarValue::UInt64(value) => Ok(value
            .map(|value| CypherValue::Integer(i128::from(value)))
            .unwrap_or(CypherValue::Null)),
        ScalarValue::Float32(value) => Ok(value.map(|value| CypherValue::Float(f64::from(value))).unwrap_or(CypherValue::Null)),
        ScalarValue::Float64(value) => Ok(value.map(CypherValue::Float).unwrap_or(CypherValue::Null)),
        ScalarValue::Utf8(value) | ScalarValue::Utf8View(value) | ScalarValue::LargeUtf8(value) => {
            Ok(value.map(CypherValue::String).unwrap_or(CypherValue::Null))
        }
        ScalarValue::List(values) => {
            let values = values.value(0);
            (0..values.len())
                .map(|index| cypher_value_from_array(values.as_ref(), index, ResultColumnKind::Scalar))
                .collect::<Result<Vec<_>, _>>()
                .map(CypherValue::List)
        }
        ScalarValue::LargeList(values) => {
            let values = values.value(0);
            (0..values.len())
                .map(|index| cypher_value_from_array(values.as_ref(), index, ResultColumnKind::Scalar))
                .collect::<Result<Vec<_>, _>>()
                .map(CypherValue::List)
        }
        ScalarValue::Struct(values) => {
            let arrow_schema::DataType::Struct(fields) = values.data_type() else {
                unreachable!("a Struct scalar always has a struct data type");
            };
            fields
                .iter()
                .enumerate()
                .map(|(index, field)| {
                    Ok((
                        field.name().clone(),
                        cypher_value_from_array(values.column(index).as_ref(), 0, ResultColumnKind::Scalar)?,
                    ))
                })
                .collect::<Result<BTreeMap<_, _>, GraphError>>()
                .map(CypherValue::Record)
        }
        other => Err(GraphError::UnsupportedQuery {
            reason: QueryFailureReason::Evaluation,
            dialect: "DataFusionCypher",
            feature: format!("result value type {:?} has no canonical Cypher value conversion", other.data_type()),
        }),
    }
}

fn relationship_value(array: &StructArray, row: usize) -> Result<RelationshipEntity, GraphError> {
    let source = required_u64_field(array, row, "source")?;
    let destination = required_u64_field(array, row, "destination")?;
    let relationship_type = required_string_field(array, row, "relationship_type")?;
    let kind = required_string_field(array, row, "kind")?;
    let record_id = optional_u64_field(array, row, "record_id")?;
    let identity = match (kind.as_str(), record_id) {
        ("identified", Some(id)) => RelationshipRef::identified(RelationshipId(id), NodeId(source), NodeId(destination), relationship_type),
        ("structural", None) => RelationshipRef::structural(NodeId(source), NodeId(destination), relationship_type),
        _ => {
            return Err(GraphError::CorruptValue {
                key: "query/datafusion/relationship_binding".to_string(),
                reason: "relationship kind and record identity disagree".to_string(),
            });
        }
    };
    let properties = property_map_from_array(array.column(required_field_index(array, "properties")?).as_ref(), row)?;
    Ok(RelationshipEntity { identity, properties })
}

fn property_map_from_array(array: &dyn Array, row: usize) -> Result<BTreeMap<String, CypherValue>, GraphError> {
    if array.is_null(row) || array.data_type() == &arrow_schema::DataType::Null {
        return Ok(BTreeMap::new());
    }
    let map = array.as_any().downcast_ref::<MapArray>().ok_or_else(|| GraphError::CorruptValue {
        key: "query/datafusion/entity_properties".to_string(),
        reason: format!("entity properties have unexpected Arrow type {}", array.data_type()),
    })?;
    let entries = map.entries();
    let keys = entries
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| GraphError::CorruptValue {
            key: "query/datafusion/entity_properties".to_string(),
            reason: "entity property keys are not strings".to_string(),
        })?;
    let values = entries.column(1);
    let offsets = map.value_offsets();
    let mut properties = BTreeMap::new();
    for index in offsets[row] as usize..offsets[row + 1] as usize {
        let value = value_from_array(values.as_ref(), index).map_err(|error| GraphError::CorruptValue {
            key: "query/datafusion/entity_properties".to_string(),
            reason: error.to_string(),
        })?;
        properties.insert(keys.value(index).to_owned(), value);
    }
    Ok(properties)
}

async fn hydrate_public_result_rows(rows: &mut [QueryRow], read_view: Arc<dyn GraphReadView>) -> Result<(), GraphError> {
    let mut decoded_rows = Vec::with_capacity(rows.len());
    let mut node_ids = BTreeSet::new();
    let mut relationship_ids = BTreeSet::new();
    for row in rows.iter() {
        let decoded = row
            .values
            .iter()
            .map(|value| match value {
                QueryValue::GraphValue(bytes) => decode_value(bytes).map(Some).map_err(|error| GraphError::CorruptValue {
                    key: "query/datafusion/result_value".to_string(),
                    reason: error.to_string(),
                }),
                _ => Ok(None),
            })
            .collect::<Result<Vec<_>, _>>()?;
        for value in decoded.iter().flatten() {
            collect_result_entities(value, &mut node_ids, &mut relationship_ids);
        }
        decoded_rows.push(decoded);
    }

    let mut nodes = BTreeMap::new();
    if !node_ids.is_empty() {
        let requested: Vec<_> = node_ids.into_iter().collect();
        let ordinals: BTreeMap<_, _> = requested
            .iter()
            .enumerate()
            .map(|(index, id)| (u64::try_from(index).unwrap_or(u64::MAX), *id))
            .collect();
        let mut batches = read_view
            .fetch_nodes(NodeFetchRequest {
                entities: requested
                    .iter()
                    .enumerate()
                    .map(|(index, node_id)| NodeFetchKey {
                        input_ordinal: u64::try_from(index).unwrap_or(u64::MAX),
                        node_id: *node_id,
                    })
                    .collect(),
                projection: PropertyProjection {
                    include_labels: true,
                    ..PropertyProjection::all()
                },
                missing: MissingEntityPolicy::Error,
            })
            .map_err(provider_error_to_graph)?;
        while let Some(batch) = batches.next().await {
            let batch = batch.map_err(provider_error_to_graph)?;
            let ordinal_index = batch.schema().index_of("input_ordinal").map_err(|error| GraphError::CorruptValue {
                key: "query/datafusion/node_fetch".to_string(),
                reason: error.to_string(),
            })?;
            let id_index = batch.schema().index_of("node_id").map_err(|error| GraphError::CorruptValue {
                key: "query/datafusion/node_fetch".to_string(),
                reason: error.to_string(),
            })?;
            let exists_index = batch.schema().index_of("exists").map_err(|error| GraphError::CorruptValue {
                key: "query/datafusion/node_fetch".to_string(),
                reason: error.to_string(),
            })?;
            let labels_index = batch.schema().index_of("labels").map_err(|error| GraphError::CorruptValue {
                key: "query/datafusion/node_fetch".to_string(),
                reason: error.to_string(),
            })?;
            let properties_index = batch.schema().index_of("properties").map_err(|error| GraphError::CorruptValue {
                key: "query/datafusion/node_fetch".to_string(),
                reason: error.to_string(),
            })?;
            let ordinals_array =
                batch
                    .column(ordinal_index)
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .ok_or_else(|| GraphError::CorruptValue {
                        key: "query/datafusion/node_fetch".to_string(),
                        reason: "node fetch ordinals are not UInt64".to_string(),
                    })?;
            let ids_array = batch
                .column(id_index)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .ok_or_else(|| GraphError::CorruptValue {
                    key: "query/datafusion/node_fetch".to_string(),
                    reason: "fetched node identifiers are not UInt64".to_string(),
                })?;
            let exists_array =
                batch
                    .column(exists_index)
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .ok_or_else(|| GraphError::CorruptValue {
                        key: "query/datafusion/node_fetch".to_string(),
                        reason: "node fetch existence values are not Boolean".to_string(),
                    })?;
            let labels_array = batch
                .column(labels_index)
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(|| GraphError::CorruptValue {
                    key: "query/datafusion/node_fetch".to_string(),
                    reason: "node labels are not a list".to_string(),
                })?;
            for row_index in 0..batch.num_rows() {
                let ordinal = ordinals_array.value(row_index);
                let requested_id = ordinals.get(&ordinal).copied().ok_or_else(|| GraphError::CorruptValue {
                    key: "query/datafusion/node_fetch".to_string(),
                    reason: format!("provider returned unknown node fetch ordinal {ordinal}"),
                })?;
                if !exists_array.value(row_index) || ids_array.is_null(row_index) || NodeId(ids_array.value(row_index)) != requested_id {
                    return Err(GraphError::CorruptValue {
                        key: "query/datafusion/node_fetch".to_string(),
                        reason: format!("provider returned an invalid node binding for {}", requested_id.0),
                    });
                }
                let label_values = labels_array.value(row_index);
                let labels = label_values
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| GraphError::CorruptValue {
                        key: "query/datafusion/node_fetch".to_string(),
                        reason: "node label values are not strings".to_string(),
                    })?;
                let labels = (0..labels.len()).map(|index| labels.value(index).to_owned()).collect();
                let properties = property_map_from_array(batch.column(properties_index).as_ref(), row_index)?;
                nodes.insert(
                    requested_id,
                    NodeEntity {
                        id: requested_id,
                        labels,
                        properties,
                    },
                );
            }
        }
        if nodes.len() != ordinals.len() {
            return Err(GraphError::CorruptValue {
                key: "query/datafusion/node_fetch".to_string(),
                reason: "provider omitted one or more requested node bindings".to_string(),
            });
        }
    }

    let mut relationships = BTreeMap::new();
    if !relationship_ids.is_empty() {
        let requested: Vec<_> = relationship_ids.into_iter().collect();
        let ordinals: BTreeMap<_, _> = requested
            .iter()
            .enumerate()
            .map(|(index, id)| (u64::try_from(index).unwrap_or(u64::MAX), *id))
            .collect();
        let mut batches = read_view
            .fetch_relationships(RelationshipFetchRequest {
                entities: requested
                    .iter()
                    .enumerate()
                    .map(|(index, relationship_id)| RelationshipFetchKey {
                        input_ordinal: u64::try_from(index).unwrap_or(u64::MAX),
                        relationship_id: *relationship_id,
                    })
                    .collect(),
                projection: PropertyProjection::all(),
                missing: MissingEntityPolicy::Error,
            })
            .map_err(provider_error_to_graph)?;
        while let Some(batch) = batches.next().await {
            let batch = batch.map_err(provider_error_to_graph)?;
            let ordinal_index = batch.schema().index_of("input_ordinal").map_err(|error| GraphError::CorruptValue {
                key: "query/datafusion/relationship_fetch".to_string(),
                reason: error.to_string(),
            })?;
            let relationship_index = batch.schema().index_of("relationship").map_err(|error| GraphError::CorruptValue {
                key: "query/datafusion/relationship_fetch".to_string(),
                reason: error.to_string(),
            })?;
            let ordinals_array =
                batch
                    .column(ordinal_index)
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .ok_or_else(|| GraphError::CorruptValue {
                        key: "query/datafusion/relationship_fetch".to_string(),
                        reason: "relationship fetch ordinals are not UInt64".to_string(),
                    })?;
            let relationships_array = batch
                .column(relationship_index)
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| GraphError::CorruptValue {
                    key: "query/datafusion/relationship_fetch".to_string(),
                    reason: "fetched relationships are not structs".to_string(),
                })?;
            for row_index in 0..batch.num_rows() {
                let ordinal = ordinals_array.value(row_index);
                let requested_id = ordinals.get(&ordinal).copied().ok_or_else(|| GraphError::CorruptValue {
                    key: "query/datafusion/relationship_fetch".to_string(),
                    reason: format!("provider returned unknown relationship fetch ordinal {ordinal}"),
                })?;
                if relationships_array.is_null(row_index) {
                    return Err(GraphError::CorruptValue {
                        key: "query/datafusion/relationship_fetch".to_string(),
                        reason: format!("provider returned a missing relationship {}", requested_id.0),
                    });
                }
                let value = relationship_value(relationships_array, row_index)?;
                if value.identity.identity() != &RelationshipIdentity::Identified(requested_id) {
                    return Err(GraphError::CorruptValue {
                        key: "query/datafusion/relationship_fetch".to_string(),
                        reason: format!("provider returned a different relationship for {}", requested_id.0),
                    });
                }
                relationships.insert(requested_id, value);
            }
        }
        if relationships.len() != ordinals.len() {
            return Err(GraphError::CorruptValue {
                key: "query/datafusion/relationship_fetch".to_string(),
                reason: "provider omitted one or more requested relationship bindings".to_string(),
            });
        }
    }

    for (row_index, decoded) in decoded_rows.into_iter().enumerate() {
        for (column_index, value) in decoded.into_iter().enumerate() {
            let Some(value) = value else { continue };
            let value = hydrate_result_value(value, &nodes, &relationships)?;
            validate_transport_value(&value)?;
            let encoded = encode_value(&value).map_err(|error| GraphError::CorruptValue {
                key: "query/datafusion/canonical_value".to_string(),
                reason: error.to_string(),
            })?;
            rows[row_index].values[column_index] = QueryValue::GraphValue(encoded);
        }
    }
    Ok(())
}

fn collect_result_entities(value: &CypherValue, nodes: &mut BTreeSet<NodeId>, relationships: &mut BTreeSet<RelationshipId>) {
    match value {
        CypherValue::Node(id) => {
            nodes.insert(*id);
        }
        CypherValue::Relationship(relationship) => {
            if let RelationshipIdentity::Identified(id) = relationship.identity() {
                relationships.insert(*id);
            }
        }
        CypherValue::RelationshipEntity(relationship) => {
            if let RelationshipIdentity::Identified(id) = relationship.identity.identity() {
                relationships.insert(*id);
            }
        }
        CypherValue::List(values) => {
            for value in values {
                collect_result_entities(value, nodes, relationships);
            }
        }
        CypherValue::Map(values) => {
            for value in values.values() {
                collect_result_entities(value, nodes, relationships);
            }
        }
        CypherValue::Record(values) => {
            for value in values.values() {
                collect_result_entities(value, nodes, relationships);
            }
        }
        _ => {}
    }
}

fn hydrate_result_value(
    value: CypherValue,
    nodes: &BTreeMap<NodeId, NodeEntity>,
    relationships: &BTreeMap<RelationshipId, RelationshipEntity>,
) -> Result<CypherValue, GraphError> {
    Ok(match value {
        CypherValue::Node(id) => CypherValue::NodeEntity(nodes.get(&id).cloned().ok_or_else(|| GraphError::CorruptValue {
            key: "query/datafusion/node_fetch".to_string(),
            reason: format!("node {} was not materialized", id.0),
        })?),
        CypherValue::Relationship(identity) => match identity.identity() {
            RelationshipIdentity::Identified(id) => {
                CypherValue::RelationshipEntity(relationships.get(id).cloned().ok_or_else(|| GraphError::CorruptValue {
                    key: "query/datafusion/relationship_fetch".to_string(),
                    reason: format!("relationship {} was not materialized", id.0),
                })?)
            }
            RelationshipIdentity::Structural { .. } => CypherValue::RelationshipEntity(RelationshipEntity {
                identity,
                properties: BTreeMap::new(),
            }),
        },
        CypherValue::RelationshipEntity(entity) => match entity.identity.identity().clone() {
            RelationshipIdentity::Identified(id) => {
                CypherValue::RelationshipEntity(relationships.get(&id).cloned().ok_or_else(|| GraphError::CorruptValue {
                    key: "query/datafusion/relationship_fetch".to_string(),
                    reason: format!("relationship {} was not materialized", id.0),
                })?)
            }
            RelationshipIdentity::Structural { .. } => CypherValue::RelationshipEntity(entity),
        },
        CypherValue::List(values) => CypherValue::List(
            values
                .into_iter()
                .map(|value| hydrate_result_value(value, nodes, relationships))
                .collect::<Result<_, _>>()?,
        ),
        CypherValue::Map(values) => CypherValue::Map(
            values
                .into_iter()
                .map(|(key, value)| Ok((key, hydrate_result_value(value, nodes, relationships)?)))
                .collect::<Result<_, GraphError>>()?,
        ),
        CypherValue::Record(values) => CypherValue::Record(
            values
                .into_iter()
                .map(|(key, value)| Ok((key, hydrate_result_value(value, nodes, relationships)?)))
                .collect::<Result<_, GraphError>>()?,
        ),
        other => other,
    })
}

fn path_value(array: &StructArray, row: usize) -> Result<Path, GraphError> {
    let nodes_array = array
        .column(required_field_index(array, "nodes")?)
        .as_any()
        .downcast_ref::<ListArray>()
        .ok_or_else(|| GraphError::CorruptValue {
            key: "query/datafusion/path_binding".to_string(),
            reason: "path nodes are not a list".to_string(),
        })?;
    let nodes_values = nodes_array.value(row);
    let nodes = nodes_values
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| GraphError::CorruptValue {
            key: "query/datafusion/path_binding".to_string(),
            reason: "path node identifiers are not UInt64".to_string(),
        })?;
    let nodes = (0..nodes.len()).map(|index| NodeId(nodes.value(index))).collect();

    let relationships_array = array
        .column(required_field_index(array, "relationships")?)
        .as_any()
        .downcast_ref::<ListArray>()
        .ok_or_else(|| GraphError::CorruptValue {
            key: "query/datafusion/path_binding".to_string(),
            reason: "path relationships are not a list".to_string(),
        })?;
    let relationships_values = relationships_array.value(row);
    let relationships = relationships_values
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| GraphError::CorruptValue {
            key: "query/datafusion/path_binding".to_string(),
            reason: "path relationship entries are not structs".to_string(),
        })?;
    let mut path_relationships = Vec::with_capacity(relationships.len());
    for index in 0..relationships.len() {
        let identity = relationships
            .column(required_field_index(relationships, "identity")?)
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| GraphError::CorruptValue {
                key: "query/datafusion/path_binding".to_string(),
                reason: "path relationship identity is not a struct".to_string(),
            })?;
        let identity = relationship_identity(identity, index)?;
        let orientation_array = relationships
            .column(required_field_index(relationships, "orientation")?)
            .as_any()
            .downcast_ref::<UInt8Array>()
            .ok_or_else(|| GraphError::CorruptValue {
                key: "query/datafusion/path_binding".to_string(),
                reason: "path relationship orientation is not UInt8".to_string(),
            })?;
        let orientation = match orientation_array.value(index) {
            0 => TraversalOrientation::Forward,
            1 => TraversalOrientation::Reverse,
            other => {
                return Err(GraphError::CorruptValue {
                    key: "query/datafusion/path_binding".to_string(),
                    reason: format!("unknown path relationship orientation {other}"),
                });
            }
        };
        path_relationships.push(PathRelationship::new(identity, orientation));
    }
    Path::new(nodes, path_relationships).map_err(|error| GraphError::CorruptValue {
        key: "query/datafusion/path_binding".to_string(),
        reason: error.to_string(),
    })
}

fn relationship_identity(array: &StructArray, row: usize) -> Result<RelationshipIdentity, GraphError> {
    let kind = array
        .column(required_field_index(array, "kind")?)
        .as_any()
        .downcast_ref::<UInt8Array>()
        .ok_or_else(|| GraphError::CorruptValue {
            key: "query/datafusion/path_binding".to_string(),
            reason: "relationship identity kind is not UInt8".to_string(),
        })?
        .value(row);
    let source = optional_u64_field(array, row, "source")?;
    let destination = optional_u64_field(array, row, "destination")?;
    let relationship_type = optional_string_field(array, row, "relationship_type")?.unwrap_or_default();
    let record_id = optional_u64_field(array, row, "record_id")?;
    match kind {
        1 => record_id
            .map(RelationshipId)
            .map(RelationshipIdentity::Identified)
            .ok_or_else(|| GraphError::CorruptValue {
                key: "query/datafusion/path_binding".to_string(),
                reason: "identified path relationship has no record ID".to_string(),
            }),
        0 => Ok(RelationshipIdentity::Structural {
            source: NodeId(source.ok_or_else(|| GraphError::CorruptValue {
                key: "query/datafusion/path_binding".to_string(),
                reason: "structural path relationship has no source ID".to_string(),
            })?),
            destination: NodeId(destination.ok_or_else(|| GraphError::CorruptValue {
                key: "query/datafusion/path_binding".to_string(),
                reason: "structural path relationship has no destination ID".to_string(),
            })?),
            relationship_type,
        }),
        other => Err(GraphError::CorruptValue {
            key: "query/datafusion/path_binding".to_string(),
            reason: format!("unknown relationship identity kind {other}"),
        }),
    }
}

fn required_field_index(array: &StructArray, name: &str) -> Result<usize, GraphError> {
    array
        .fields()
        .iter()
        .position(|field| field.name() == name)
        .ok_or_else(|| GraphError::CorruptValue {
            key: "query/datafusion/graph_value".to_string(),
            reason: format!("graph value is missing field `{name}`"),
        })
}

fn required_u64_field(array: &StructArray, row: usize, name: &str) -> Result<u64, GraphError> {
    optional_u64_field(array, row, name)?.ok_or_else(|| GraphError::CorruptValue {
        key: "query/datafusion/graph_value".to_string(),
        reason: format!("graph value field `{name}` is null"),
    })
}

fn optional_u64_field(array: &StructArray, row: usize, name: &str) -> Result<Option<u64>, GraphError> {
    let child = array
        .column(required_field_index(array, name)?)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| GraphError::CorruptValue {
            key: "query/datafusion/graph_value".to_string(),
            reason: format!("graph value field `{name}` is not UInt64"),
        })?;
    Ok((!child.is_null(row)).then(|| child.value(row)))
}

fn required_string_field(array: &StructArray, row: usize, name: &str) -> Result<String, GraphError> {
    optional_string_field(array, row, name)?.ok_or_else(|| GraphError::CorruptValue {
        key: "query/datafusion/graph_value".to_string(),
        reason: format!("graph value field `{name}` is null"),
    })
}

fn optional_string_field(array: &StructArray, row: usize, name: &str) -> Result<Option<String>, GraphError> {
    let child = array
        .column(required_field_index(array, name)?)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| GraphError::CorruptValue {
            key: "query/datafusion/graph_value".to_string(),
            reason: format!("graph value field `{name}` is not Utf8"),
        })?;
    Ok((!child.is_null(row)).then(|| child.value(row).to_string()))
}

fn materialize_result_row(
    batch: &RecordBatch,
    row_index: usize,
    result_kinds: &[ResultColumnKind],
    validate_transport: bool,
) -> Result<QueryRow, GraphError> {
    let values = (0..batch.num_columns())
        .map(|column_index| {
            let kind = result_kinds.get(column_index).copied().unwrap_or(ResultColumnKind::Scalar);
            let value = cypher_value_from_array(batch.column(column_index).as_ref(), row_index, kind)?;
            if validate_transport {
                validate_transport_value(&value)?;
            }
            let encoded = encode_value(&value).map_err(|error| GraphError::CorruptValue {
                key: "query/datafusion/canonical_value".to_string(),
                reason: error.to_string(),
            })?;
            Ok(QueryValue::GraphValue(encoded))
        })
        .collect::<Result<Vec<_>, GraphError>>()?;
    Ok(QueryRow::new(values))
}

fn materialize_result_values(
    values: &[CypherValue],
    result_kinds: &[ResultColumnKind],
    validate_transport: bool,
) -> Result<QueryRow, GraphError> {
    if values.len() != result_kinds.len() {
        return Err(GraphError::CorruptValue {
            key: "query/datafusion/result_columns".to_string(),
            reason: format!(
                "mutation returned {} values for {} public columns",
                values.len(),
                result_kinds.len()
            ),
        });
    }
    let values = values
        .iter()
        .map(|value| {
            if validate_transport {
                validate_transport_value(value)?;
            }
            encode_value(value)
                .map(QueryValue::GraphValue)
                .map_err(|error| GraphError::CorruptValue {
                    key: "query/datafusion/canonical_value".to_string(),
                    reason: error.to_string(),
                })
        })
        .collect::<Result<Vec<_>, GraphError>>()?;
    Ok(QueryRow::new(values))
}

fn validate_transport_value(value: &CypherValue) -> Result<(), GraphError> {
    fn validate_graph_id(value: u64, kind: &'static str) -> Result<(), GraphError> {
        i64::try_from(value).map(|_| ()).map_err(|_| GraphError::UnsupportedQuery {
            reason: QueryFailureReason::Evaluation,
            dialect: "ClientProtocol",
            feature: format!("{kind} identifier {value} exceeds the signed 64-bit protocol range"),
        })
    }

    fn validate_relationship_identity(identity: &RelationshipIdentity) -> Result<(), GraphError> {
        match identity {
            RelationshipIdentity::Structural { source, destination, .. } => {
                validate_graph_id(source.0, "node")?;
                validate_graph_id(destination.0, "node")
            }
            RelationshipIdentity::Identified(id) => validate_graph_id(id.0, "relationship"),
        }
    }

    match value {
        CypherValue::Integer(value) if i64::try_from(*value).is_err() => Err(GraphError::UnsupportedQuery {
            reason: QueryFailureReason::Evaluation,
            dialect: "ClientProtocol",
            feature: format!("integer {value} exceeds the signed 64-bit protocol range"),
        }),
        CypherValue::Float(value) if !value.is_finite() => Err(GraphError::UnsupportedQuery {
            reason: QueryFailureReason::Evaluation,
            dialect: "ClientProtocol",
            feature: "non-finite floating-point values are not representable in the HTTP/Bolt response domain".to_string(),
        }),
        CypherValue::Node(node) => validate_graph_id(node.0, "node"),
        CypherValue::NodeEntity(node) => {
            validate_graph_id(node.id.0, "node")?;
            for value in node.properties.values() {
                validate_transport_value(value)?;
            }
            Ok(())
        }
        CypherValue::Relationship(relationship) => {
            validate_graph_id(relationship.source().0, "node")?;
            validate_graph_id(relationship.destination().0, "node")?;
            validate_relationship_identity(relationship.identity())
        }
        CypherValue::RelationshipEntity(relationship) => {
            validate_graph_id(relationship.identity.source().0, "node")?;
            validate_graph_id(relationship.identity.destination().0, "node")?;
            validate_relationship_identity(relationship.identity.identity())?;
            for value in relationship.properties.values() {
                validate_transport_value(value)?;
            }
            Ok(())
        }
        CypherValue::Path(path) => {
            for node in path.nodes() {
                validate_graph_id(node.0, "node")?;
            }
            for relationship in path.relationships() {
                validate_relationship_identity(relationship.identity())?;
            }
            Ok(())
        }
        CypherValue::List(values) => {
            for value in values {
                validate_transport_value(value)?;
            }
            Ok(())
        }
        CypherValue::Map(values) => {
            for value in values.values() {
                validate_transport_value(value)?;
            }
            Ok(())
        }
        CypherValue::Record(values) => {
            for value in values.values() {
                validate_transport_value(value)?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

fn result_columns(graph_ir: &GraphIr) -> Vec<QueryColumn> {
    match &graph_ir.kind {
        GraphIrKind::Project { items, .. } => projection_columns(items),
        GraphIrKind::MutationProgram { program, .. } => projection_columns(&program.returning),
        GraphIrKind::Aggregation { group_by, aggregates, .. } => group_by
            .iter()
            .enumerate()
            .map(|(index, projection)| projection_column(projection, index))
            .chain(
                aggregates
                    .iter()
                    .map(|aggregate| QueryColumn::new(aggregate.output.symbol.as_str())),
            )
            .collect(),
        GraphIrKind::Union { outputs, .. } => outputs.iter().map(|output| QueryColumn::new(output.symbol.as_str())).collect(),
        GraphIrKind::ResultWindow { input, .. } | GraphIrKind::Sort { input, .. } | GraphIrKind::Window { input, .. } => {
            result_columns(input)
        }
        _ => Vec::new(),
    }
}

fn projection_columns(projections: &[Projection]) -> Vec<QueryColumn> {
    projections
        .iter()
        .enumerate()
        .map(|(index, projection)| projection_column(projection, index))
        .collect()
}

fn projection_column(projection: &Projection, index: usize) -> QueryColumn {
    let name = projection
        .alias
        .as_ref()
        .map(|alias| alias.symbol.as_str().to_owned())
        .unwrap_or_else(|| public_expression_name(&projection.expression, index));
    QueryColumn::new(name)
}

fn public_expression_name(expression: &GraphExpression, index: usize) -> String {
    let display = match expression {
        GraphExpression::Binding(binding) => binding.symbol.as_str().to_owned(),
        GraphExpression::Property { entity, property } => format!("{}.{}", entity.symbol.as_str(), property.as_str()),
        GraphExpression::Parameter(name) => format!("${}", name.as_str()),
        GraphExpression::Literal(value) => match value {
            hydradb_graph_plan::ScalarValue::Null => "null".to_string(),
            hydradb_graph_plan::ScalarValue::Boolean(value) => value.to_string(),
            hydradb_graph_plan::ScalarValue::Integer(value) => value.to_string(),
            hydradb_graph_plan::ScalarValue::Float(value) => value.to_string(),
            hydradb_graph_plan::ScalarValue::String(value) => format!("'{}'", value),
        },
        GraphExpression::Binary { left, operator, right } => {
            let operator = match operator {
                hydradb_graph_plan::GraphBinaryOperator::Equal => "=",
                hydradb_graph_plan::GraphBinaryOperator::NotEqual => "<>",
                hydradb_graph_plan::GraphBinaryOperator::LessThan => "<",
                hydradb_graph_plan::GraphBinaryOperator::LessThanOrEqual => "<=",
                hydradb_graph_plan::GraphBinaryOperator::GreaterThan => ">",
                hydradb_graph_plan::GraphBinaryOperator::GreaterThanOrEqual => ">=",
                hydradb_graph_plan::GraphBinaryOperator::And => "AND",
                hydradb_graph_plan::GraphBinaryOperator::Or => "OR",
                hydradb_graph_plan::GraphBinaryOperator::Add => "+",
                hydradb_graph_plan::GraphBinaryOperator::Subtract => "-",
                hydradb_graph_plan::GraphBinaryOperator::Multiply => "*",
                hydradb_graph_plan::GraphBinaryOperator::Divide => "/",
                hydradb_graph_plan::GraphBinaryOperator::In => "IN",
                hydradb_graph_plan::GraphBinaryOperator::StartsWith => "STARTS WITH",
                hydradb_graph_plan::GraphBinaryOperator::EndsWith => "ENDS WITH",
                hydradb_graph_plan::GraphBinaryOperator::Contains => "CONTAINS",
            };
            format!(
                "{} {operator} {}",
                public_expression_name(left, index),
                public_expression_name(right, index)
            )
        }
        GraphExpression::Unary { operator, expression } => {
            let expression = public_expression_name(expression, index);
            match operator {
                hydradb_graph_plan::UnaryOperator::Not => format!("NOT {expression}"),
                hydradb_graph_plan::UnaryOperator::Negate => format!("-{expression}"),
                hydradb_graph_plan::UnaryOperator::IsNull => format!("{expression} IS NULL"),
                hydradb_graph_plan::UnaryOperator::IsNotNull => format!("{expression} IS NOT NULL"),
            }
        }
        GraphExpression::Function { name, arguments, distinct } => {
            let arguments = arguments
                .iter()
                .map(|argument| public_expression_name(argument, index))
                .collect::<Vec<_>>()
                .join(", ");
            let distinct = if *distinct { "DISTINCT " } else { "" };
            format!("{}({distinct}{arguments})", name.as_str())
        }
        GraphExpression::List(items) => format!(
            "[{}]",
            items
                .iter()
                .map(|item| public_expression_name(item, index))
                .collect::<Vec<_>>()
                .join(", ")
        ),
        GraphExpression::Map(items) => format!(
            "{{{}}}",
            items
                .iter()
                .map(|(key, value)| format!("{}: {}", key.as_str(), public_expression_name(value, index)))
                .collect::<Vec<_>>()
                .join(", ")
        ),
        GraphExpression::Case { branches, fallback } => {
            let mut clauses = String::from("CASE");
            for (condition, result) in branches {
                clauses.push_str(&format!(
                    " WHEN {} THEN {}",
                    public_expression_name(condition, index),
                    public_expression_name(result, index)
                ));
            }
            if let Some(fallback) = fallback {
                clauses.push_str(&format!(" ELSE {}", public_expression_name(fallback, index)));
            }
            clauses.push_str(" END");
            clauses
        }
    };
    if display.is_empty() {
        format!("column_{index}")
    } else {
        display
    }
}

fn cypher_parameters(context: &QueryContext) -> Result<BTreeMap<String, CypherValue>, GraphError> {
    let mut parameters = BTreeMap::new();
    for (name, value) in &context.parameters {
        parameters.insert(name.clone(), property_to_cypher(value));
    }
    for (name, values) in &context.list_parameters {
        if parameters.contains_key(name) {
            return Err(GraphError::CorruptValue {
                key: format!("query/datafusion/parameter/{name}"),
                reason: "parameter is present as both a scalar and a list".to_string(),
            });
        }
        parameters.insert(name.clone(), CypherValue::List(values.iter().map(property_to_cypher).collect()));
    }
    Ok(parameters)
}

fn runtime_parameter_map(parameters: &BTreeMap<String, CypherValue>) -> HashMap<Arc<str>, CypherValue> {
    parameters
        .iter()
        .map(|(name, value)| (Arc::<str>::from(name.as_str()), value.clone()))
        .collect()
}

fn provider_scope(context: &QueryContext) -> Result<ProviderScope, GraphError> {
    // The shard provider addresses canonical records by physical cell, not by
    // the logical graph name. Authorization of the logical graph has already
    // happened at the routing boundary. Using graph_id here silently creates
    // a second keyspace that legacy reads (and rollback) cannot see.
    ProviderScope::new(context.scope.namespace.to_string(), context.cell_id.clone()).map_err(provider_error_to_graph)
}

fn budget_limits(shard: &GraphShard, context: &QueryContext) -> Result<BudgetLimits, GraphError> {
    let max_total_rows = (shard.limits.max_query_result_vertices as u64).max(1);
    let max_total_bytes = context.max_result_bytes.unwrap_or(DEFAULT_RESULT_BYTES).max(1);
    let max_batch_rows = MAX_BATCH_ROWS.min(max_total_rows).max(1);
    let max_batch_bytes = MAX_BATCH_BYTES.min(max_total_bytes).max(1);
    // Providers emit singleton action batches and operators may emit short or
    // empty batches. Bound their count by work, not by ideal full result batches.
    let max_work_units = max_total_rows.saturating_mul(64).max(1);
    let max_batches = max_work_units;
    let limits = BudgetLimits::new(
        max_batch_rows,
        max_batch_bytes,
        max_total_rows,
        max_total_bytes,
        max_batches.max(1),
        max_work_units,
    )
    .map_err(provider_error_to_graph)?;
    Ok(limits)
}

fn provider_atomicity_policy(policy: MutationAtomicityPolicy) -> ProviderAtomicityPolicy {
    match policy {
        MutationAtomicityPolicy::Statement => ProviderAtomicityPolicy::Statement,
        MutationAtomicityPolicy::LegacyChunked => ProviderAtomicityPolicy::LegacyChunked,
    }
}

fn engine_mutation_policy(policy: MutationAtomicityPolicy) -> MutationPolicy {
    match policy {
        MutationAtomicityPolicy::Statement => MutationPolicy::Statement,
        MutationAtomicityPolicy::LegacyChunked => MutationPolicy::LegacyChunked,
    }
}

pub(crate) fn statement_receipt_lookup(context: &QueryContext, query: &str) -> Result<StatementReceiptLookup, GraphError> {
    let parameters = cypher_parameters(context)?;
    let idempotency = IdempotencyIdentity::new(
        if context.idempotency_principal.is_empty() {
            "anonymous".to_string()
        } else {
            context.idempotency_principal.clone()
        },
        context.idempotency_key.clone(),
    )
    .map_err(provider_error_to_graph)?;
    Ok(StatementReceiptLookup {
        scope: provider_scope(context)?,
        idempotency,
        fingerprint: mutation_fingerprint(query, &parameters).map_err(provider_error_to_graph)?,
        semantics_version: StatementSemanticsVersion(DATAFUSION_SEMANTIC_CODEC_VERSION),
    })
}

fn ensure_receipt_origin_matches(
    context: &QueryContext,
    origin: StatementReceiptOrigin,
    completed_actions: &[hydradb_graph_provider::CompletedActionRange],
) -> Result<(), GraphError> {
    let requested_origin = if context.cypher_engine == crate::CypherEngineMode::DataFusion {
        StatementReceiptOrigin::DataFusion
    } else {
        StatementReceiptOrigin::Legacy
    };
    if origin == requested_origin {
        return Ok(());
    }
    let completed_ranges = completed_actions.iter().map(|range| (range.0.first, range.0.last)).collect();
    Err(provider_error_to_graph(GraphProviderError::PartialCompletion {
        completed_ranges,
        cause: Box::new(GraphProviderError::InvalidRequest(
            "a durable statement is in progress; cross-engine redispatch is blocked".to_string(),
        )),
    }))
}

fn mutation_fingerprint(query: &str, parameters: &BTreeMap<String, CypherValue>) -> Result<MutationFingerprint, GraphProviderError> {
    let mut hasher = Sha256::new();
    hasher.update(query.as_bytes());
    hasher.update([0]);
    for (name, value) in parameters {
        hasher.update(name.as_bytes());
        hasher.update([0]);
        hasher.update(encode_value_stable(value)?);
        hasher.update([0]);
    }
    Ok(MutationFingerprint(hasher.finalize().into()))
}

fn read_requirements(graph_ir: &GraphIr) -> Result<ReadRequirements, GraphProviderError> {
    fn visit_expression(expression: &GraphExpression, operations: &mut BTreeSet<ReadOperation>) {
        match expression {
            GraphExpression::Binding(binding) => {
                operations.insert(if binding.kind == EntityKind::Relationship {
                    ReadOperation::FetchRelationships
                } else {
                    ReadOperation::FetchNodes
                });
            }
            GraphExpression::Binary { left, right, .. } => {
                visit_expression(left, operations);
                visit_expression(right, operations);
            }
            GraphExpression::Unary { expression, .. } => visit_expression(expression, operations),
            GraphExpression::Function { arguments, .. } | GraphExpression::List(arguments) => {
                for argument in arguments {
                    visit_expression(argument, operations);
                }
            }
            GraphExpression::Map(entries) => {
                for (_, value) in entries {
                    visit_expression(value, operations);
                }
            }
            GraphExpression::Case { branches, fallback } => {
                for (condition, value) in branches {
                    visit_expression(condition, operations);
                    visit_expression(value, operations);
                }
                if let Some(fallback) = fallback {
                    visit_expression(fallback, operations);
                }
            }
            GraphExpression::Property { .. } | GraphExpression::Parameter(_) | GraphExpression::Literal(_) => {}
        }
    }

    fn visit(ir: &GraphIr, operations: &mut BTreeSet<ReadOperation>) -> Result<(), GraphProviderError> {
        let mut add_binding_fetch = |kind| {
            operations.insert(match kind {
                hydradb_graph_plan::EntityKind::Relationship => ReadOperation::FetchRelationships,
                _ => ReadOperation::FetchNodes,
            });
        };
        match &ir.kind {
            GraphIrKind::SingleRow => {}
            GraphIrKind::NodeScan { input, .. } => {
                operations.insert(ReadOperation::NodeScan);
                if let Some(input) = input {
                    visit(input, operations)?;
                }
            }
            GraphIrKind::RelationshipScan { input, .. } => {
                operations.insert(ReadOperation::RelationshipScan);
                if let Some(input) = input {
                    visit(input, operations)?;
                }
            }
            GraphIrKind::GraphSeek { input, predicate, .. } => {
                operations.insert(if matches!(predicate, GraphSeekPredicate::Id(_)) {
                    ReadOperation::FetchNodes
                } else {
                    ReadOperation::Seek
                });
                if let Some(input) = input {
                    visit(input, operations)?;
                }
            }
            GraphIrKind::Project { input, items } => {
                for item in items {
                    visit_expression(&item.expression, operations);
                }
                visit(input, operations)?;
            }
            GraphIrKind::MutationProgram { input, program } => {
                for item in &program.returning {
                    visit_expression(&item.expression, operations);
                }
                visit(input, operations)?;
            }
            GraphIrKind::Aggregation { input, group_by, .. } => {
                for item in group_by {
                    visit_expression(&item.expression, operations);
                }
                visit(input, operations)?;
            }
            GraphIrKind::Filter { input, .. }
            | GraphIrKind::GraphHydrate { input, .. }
            | GraphIrKind::Unwind { input, .. }
            | GraphIrKind::Sort { input, .. }
            | GraphIrKind::Window { input, .. }
            | GraphIrKind::ResultWindow { input, .. } => {
                if let GraphIrKind::GraphHydrate { entity, .. } = &ir.kind {
                    add_binding_fetch(entity.kind);
                }
                visit(input, operations)?;
            }
            GraphIrKind::Expand { input, .. } => {
                operations.insert(ReadOperation::Expand);
                visit(input, operations)?;
            }
            GraphIrKind::Exists {
                input, subquery, output, ..
            } => {
                add_binding_fetch(output.kind);
                visit(input, operations)?;
                visit(subquery, operations)?;
            }
            GraphIrKind::OptionalApply { left, right, .. } | GraphIrKind::CorrelatedApply { left, right, .. } => {
                visit(left, operations)?;
                visit(right, operations)?;
            }
            GraphIrKind::Union { arms, .. } => {
                for arm in arms {
                    visit(arm, operations)?;
                }
            }
            GraphIrKind::VariableExpand { input, .. } => {
                operations.insert(ReadOperation::VariableExpand);
                visit(input, operations)?;
            }
            GraphIrKind::GraphProcedure { .. } => {
                return Err(GraphProviderError::UnsupportedCapability {
                    capability: "Cypher procedure execution",
                });
            }
        }
        Ok(())
    }

    let mut operations = BTreeSet::new();
    visit(graph_ir, &mut operations)?;
    Ok(ReadRequirements {
        operations,
        retained_snapshot: true,
        ..ReadRequirements::default()
    })
}

fn provider_error_to_datafusion(error: GraphProviderError) -> DataFusionError {
    DataFusionError::External(Box::new(error))
}

#[derive(Default)]
struct PrecommitMutationResults {
    rows: Vec<QueryRow>,
    bytes: u64,
}

fn graph_error_to_datafusion(error: GraphError) -> DataFusionError {
    DataFusionError::External(Box::new(error))
}

fn datafusion_error_to_datafusion(error: impl std::fmt::Display) -> DataFusionError {
    DataFusionError::Execution(error.to_string())
}

fn provider_error_to_graph(error: GraphProviderError) -> GraphError {
    GraphError::DataFusionProvider(error)
}

fn datafusion_error_to_graph(error: DataFusionError) -> GraphError {
    if let DataFusionError::External(error) = error {
        return match error.downcast::<GraphProviderError>() {
            Ok(provider_error) => provider_error_to_graph(*provider_error),
            Err(error) => match error.downcast::<GraphError>() {
                Ok(graph_error) => *graph_error,
                Err(error) => GraphError::UnsupportedQuery {
                    reason: QueryFailureReason::Evaluation,
                    dialect: "DataFusionCypher",
                    feature: error.to_string(),
                },
            },
        };
    }
    GraphError::UnsupportedQuery {
        reason: QueryFailureReason::Evaluation,
        dialect: "DataFusionCypher",
        feature: error.to_string(),
    }
}

fn cypher_df_error_to_graph(error: DfError) -> GraphError {
    match error {
        DfError::DataFusion(error) => datafusion_error_to_graph(error),
        DfError::GraphPlan(error) => GraphError::UnsupportedQuery {
            reason: QueryFailureReason::Other,
            dialect: "DataFusionCypher",
            feature: error.to_string(),
        },
        DfError::MissingParameter(name) => GraphError::MissingQueryParameter {
            dialect: "DataFusionCypher",
            name,
        },
        DfError::Unsupported(feature) => GraphError::UnsupportedQuery {
            reason: QueryFailureReason::Other,
            dialect: "DataFusionCypher",
            feature,
        },
        DfError::Invalid(reason) => GraphError::QueryParse {
            dialect: "DataFusionCypher",
            reason,
        },
    }
}

async fn abort_mutation_context(runtime_context: &Arc<Mutex<Option<Arc<GraphExecutionContext>>>>) {
    let context = runtime_context.lock().ok().and_then(|context| context.clone());
    if let Some(context) = context {
        let _ = context.abort_mutation().await;
    }
}

fn cancelled_error() -> GraphError {
    GraphError::UnsupportedQuery {
        reason: QueryFailureReason::InvalidRequest,
        dialect: "DataFusionCypher",
        feature: "query was cancelled".to_string(),
    }
}

fn timeout_error(started: Instant, limit_ms: Option<u64>) -> GraphError {
    let elapsed_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
    let limit_ms = limit_ms.unwrap_or(elapsed_ms);
    GraphError::QueryTimeout {
        operation: "datafusion_cypher_query",
        elapsed_ms,
        limit_ms,
    }
}
