#![cfg(feature = "query-telemetry")]
use std::{
    sync::atomic::{AtomicU64, Ordering},
    time::{Instant, SystemTime, UNIX_EPOCH},
};

use hydradb_query_telemetry::QueryFailureEvent;
use sha2::{Digest, Sha256};

use super::service::ClientQueryRequest;
use crate::{CypherEngineMode, GraphError, QueryFailureStage};

static NEXT_EVENT: AtomicU64 = AtomicU64::new(1);

pub(crate) struct FailureObservation {
    engine: CypherEngineMode,
    configured_engine: CypherEngineMode,
    timeout_ms: Option<u64>,
    page_size: Option<usize>,
    cursor_continuation: bool,
    started: Instant,
}

fn bounded(value: &str, max: usize) -> String {
    let mut end = value.len().min(max);
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    value[..end].to_string()
}

impl FailureObservation {
    pub fn retained_bytes(request: &ClientQueryRequest, node_id: &str) -> usize {
        let strings = [
            request.query.as_str(),
            &request.query_id,
            &request.target.cell_id,
            request.target.scope.graph_id.as_str(),
            node_id,
            request.correlation_id.as_deref().unwrap_or(""),
            request.caller_step.as_deref().unwrap_or(""),
        ];
        let vector_bytes = 32 * size_of::<String>() + hydradb_scope::MAX_NAMESPACE_DEPTH * size_of::<crate::NamespaceId>();
        strings
            .into_iter()
            .map(str::len)
            .chain(request.parameters.keys().take(32).map(String::len))
            .chain(request.target.scope.namespace.segments().iter().map(|name| name.as_str().len()))
            .fold(vector_bytes, usize::saturating_add)
    }

    pub fn new(
        engine: CypherEngineMode,
        configured_engine: CypherEngineMode,
        timeout_ms: Option<u64>,
        page_size: Option<usize>,
        cursor_continuation: bool,
    ) -> Self {
        Self {
            started: Instant::now(),
            engine,
            configured_engine,
            timeout_ms,
            page_size,
            cursor_continuation,
        }
    }

    pub fn failed(
        self,
        request: &ClientQueryRequest,
        node_id: &str,
        stage: QueryFailureStage,
        error: &GraphError,
    ) -> impl FnOnce() -> QueryFailureEvent + Send + 'static {
        // Copy only reportable fields, only after a failure. Never retain bound values or error display text.
        let query = request.query.clone();
        let query_id = request.query_id.clone();
        let protocol = request.protocol;
        let target = request.target.clone();
        let correlation_id = request.correlation_id.clone();
        let caller_step = request.caller_step.clone();
        let parameter_count = request.parameters.len();
        let parameter_names: Vec<_> = request.parameters.keys().take(32).cloned().collect();
        let consistency = request.consistency;
        let read_epoch = request.read_epoch;
        let node_id = node_id.to_string();
        let error_class = error.class();
        let failure_reason = error.failure_reason();
        let error_operation = error.limit_operation();
        let elapsed_us = self.started.elapsed().as_micros().try_into().unwrap_or(u64::MAX);
        let timestamp = SystemTime::now();
        move || {
            let now = timestamp.duration_since(UNIX_EPOCH).unwrap_or_default();
            QueryFailureEvent {
                schema_version: 1,
                event_id: format!(
                    "{}-{}-{}-{}",
                    node_id,
                    now.as_nanos(),
                    std::process::id(),
                    NEXT_EVENT.fetch_add(1, Ordering::Relaxed)
                ),
                timestamp_unix_ms: now.as_millis().try_into().unwrap_or(u64::MAX),
                node_id: bounded(&node_id, 256),
                server_version: env!("CARGO_PKG_VERSION").to_string(),
                query_id: bounded(&query_id, 256),
                protocol: protocol.to_string(),
                query: bounded(&query, 16 * 1024),
                query_truncated: query.len() > 16 * 1024,
                query_sha256: format!("{:x}", Sha256::digest(query.as_bytes())),
                namespace: bounded(&target.scope.namespace.to_string(), 1024),
                graph_id: bounded(target.scope.graph_id.as_str(), 256),
                cell_id: bounded(&target.cell_id, 256),
                correlation_id: correlation_id.as_ref().map(|s| bounded(s, 128)),
                caller_step: caller_step.as_ref().map(|s| bounded(s, 128)),
                parameter_count,
                parameter_names: parameter_names.iter().map(|name| bounded(name, 128)).collect(),
                configured_engine: self.configured_engine.as_str().to_string(),
                engine: self.engine.as_str().to_string(),
                stage: stage.as_str().to_string(),
                elapsed_us,
                timeout_ms: self.timeout_ms,
                consistency: match consistency {
                    super::service::ClientReadConsistency::Causal => "causal",
                    super::service::ClientReadConsistency::Strong => "strong",
                }
                .to_string(),
                page_size: self.page_size,
                cursor_continuation: self.cursor_continuation,
                read_epoch,
                error_class: error_class.to_string(),
                failure_reason: failure_reason.map(|reason| reason.as_str().to_string()),
                error_operation: error_operation.map(str::to_string),
                error: error_class.to_string(),
                error_truncated: false,
            }
        }
    }
}
